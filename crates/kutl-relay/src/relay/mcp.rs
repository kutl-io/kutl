//! MCP (Model Context Protocol) family of the relay actor: session
//! lifecycle and reaping, tool handlers, document/signal resolution, and
//! SSE notification fan-out.
//!
//! Child module of the relay actor (`super`) so the `impl Relay` block here
//! reaches the actor's private fields directly. `process_command` in relay.rs
//! routes MCP commands to these handlers; the agent-facing `McpError` hint
//! strings are an agent contract.

use std::sync::Arc;
use std::time::Duration;

use kutl_core::Boundary;
use kutl_proto::sync::{self};
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::auth::AuthError;
use crate::authorized_keys::name_path_matches;
use crate::mcp_tools;
use crate::observer::{EditContentMode, MergedEvent, ReactionEvent};
use crate::registry;
use crate::registry_store::MirrorMetadata;

use super::{
    AuthorIdentity, BlobData, DocContent, DocKey, DocSlot, EditedAtPending, NO_SKIP_CONN, Relay,
    build_and_relay_text_outbound, compute_blob_hash, persist_entry,
};

/// MCP sessions are reaped after 20 minutes of inactivity.
const MCP_SESSION_IDLE_TTL: Duration = Duration::from_mins(20);

/// How often the relay checks for expired MCP sessions.
pub(super) const MCP_SESSION_REAP_INTERVAL: Duration =
    Duration::from_secs((5 * kutl_core::SECONDS_PER_MINUTE).unsigned_abs());

/// Unique identifier for an MCP session.
pub type McpSessionId = String;

/// An MCP session tracked by the relay actor.
pub(super) struct McpSession {
    /// The DID of the authenticated agent.
    did: String,
    /// PAT context attached when this session was created via a PAT
    /// bearer token. `None` for DID-challenge-response sessions and
    /// device-flow / OAuth sessions. Carries both `pat_hash` (for
    /// scope checks) and `pat_id` (for per-PAT signal attribution).
    pat: Option<crate::auth::PatAuthContext>,
    /// SSE notification channel (None until GET /mcp registers it).
    notify_tx: Option<mpsc::Sender<String>>,
    /// When the session last handled a request (reset on each validate).
    last_active: tokio::time::Instant,
}

/// Default agent-facing hint when a read-style MCP tool resolves to no
/// document. The hint is load-bearing — it tells the agent how to
/// discover valid document IDs without re-guessing.
const DOC_NOT_FOUND_HINT_READ: &str =
    "use list_documents to discover document_id values for this space";

/// Agent-facing hint when `edit_document` is called against an unknown
/// document id or path. Directs the agent at `create_document` — edit
/// never auto-creates.
const DOC_NOT_FOUND_HINT_EDIT: &str = "use create_document to create new documents";

/// Agent-facing hint when `create_document` / `upload_blob` targets a
/// space the relay's space backend doesn't know about. Space creation
/// is intentionally NOT an MCP capability — direct the agent at the
/// human creation flow.
const SPACE_NOT_FOUND_HINT: &str = "space creation is not an MCP capability — humans create spaces via `kutl init`, the desktop app, the kutlhub web UI, or `POST /spaces/register`. Use `list_spaces` to discover destinations you're already authorised for";

/// Changes that may land under a writer before its base is refused.
///
/// Generous on purpose: a writer can legitimately wait on a peer for a minute
/// and compose for another, and the cost of refusing wrongly is a whole
/// discarded composition. Tighten against evidence, not intuition.
const MAX_CHANGES_BEHIND: usize = 25;

/// Longest excerpt, in characters, echoed back in a refusal sentence.
///
/// The excerpt is a line of the caller's own text and nothing constrains its
/// length. Past the width of a terminal line it stops helping the writer find
/// the region and starts burying the instruction that follows it.
const MAX_REFUSAL_EXCERPT_CHARS: usize = 80;

/// Agent-facing refusal when a version token did not come from this relay in a
/// shape it can read.
const BASE_MALFORMED: &str = "malformed version token; pass back the `version` field from \
                              read_document unchanged";

/// Agent-facing refusal when a token is well-formed but names a different
/// document.
const BASE_WRONG_DOCUMENT: &str = "that version token was issued for a different document; read \
                                   this one and use the version it returns";

/// Agent-facing refusal when a token names a position this document's history
/// does not hold, or holds text other than what the token was minted over.
/// Reachable by a token that travelled from another relay, whose frontier
/// indices mean something else here, and by a hand-built one.
const BASE_NOT_THIS_HISTORY: &str = "that version does not describe this document's history; read \
                                     the document again and use the version it returns";

/// Errors specific to MCP tool dispatch.
#[derive(Debug, thiserror::Error)]
pub enum McpError {
    /// The MCP session ID is not recognized.
    #[error("session not found")]
    SessionNotFound,
    /// The requested document does not exist. The `hint` field carries
    /// agent-facing recovery guidance (e.g. "use `create_document` to create
    /// new documents") — the message string is part of the agent contract.
    #[error("document not found: {space_id}/{document_id} — {hint}")]
    DocumentNotFound {
        /// Space that was searched.
        space_id: String,
        /// Document identifier that did not resolve.
        document_id: String,
        /// Recovery hint surfaced to the calling agent.
        hint: String,
    },
    /// The document contains binary blob data, not text.
    #[error("document is a binary blob, not text")]
    NotTextDocument,
    /// The caller is not a member of the requested space.
    #[error("not a member of space {space_id}")]
    NotAuthorized { space_id: String },
    /// The signal does not exist or belongs to a different space.
    #[error("signal not found: {signal_id}")]
    SignalNotFound { signal_id: String },
    /// An edit operation failed.
    #[error("edit failed: {0}")]
    EditFailed(String),
    /// The caller's `base_version` cannot be used, with the remedy in `reason`.
    #[error("{reason}")]
    UnusableBaseVersion {
        /// What is wrong and what to do about it, in the caller's terms.
        reason: String,
    },
    /// A client-pushed record failed the ingest gate (well-formedness,
    /// space-binding, or actor-binding). A client error: the caller
    /// sent an inadmissible record (e.g. one impersonating another `actor_did`),
    /// so it maps to HTTP 400, not a 500.
    #[error("record rejected: {reason}")]
    RecordRejected {
        /// Why the record was rejected (the admission-gate reason).
        reason: String,
    },
    /// This relay does not accept client-pushed signal history.
    ///
    /// A deployment policy, not a per-record verdict and not an authorization
    /// failure — every caller gets the same answer, and the batch's contents are
    /// never examined. Distinct from a rejected-record outcome so a client can
    /// tell "this relay will never take my history" from "these particular
    /// records were bad", and stop retrying.
    #[error("this relay does not accept re-seed")]
    ReSeedRefused,
    /// The requested path is already in use by another document.
    #[error("path already in use: {path} (existing document_id {existing_document_id})")]
    PathAlreadyInUse {
        /// The colliding within-space path.
        path: String,
        /// Existing document at that path.
        existing_document_id: String,
    },
    /// The text/blob distinction at this path was violated (e.g. uploading
    /// a blob at a path already held by a text document).
    #[error("path {path} is occupied by a text document — text and blob cannot share a path")]
    PathTypeConflict {
        /// The colliding within-space path.
        path: String,
    },
    /// The blob bytes exceed the configured cap.
    #[error(
        "blob exceeds configured cap: {actual} bytes > {max} bytes (KUTL_MAX_BLOB_BYTES) — \
         reduce the payload or upload in another way"
    )]
    BlobTooLarge {
        /// Size of the rejected payload.
        actual: usize,
        /// Configured cap.
        max: usize,
    },
    /// The requested space does not exist on this relay's space backend.
    /// Fired by `create_document` / `upload_blob` when the relay is
    /// configured with persistent space registration but the agent
    /// targets an unregistered slug/UUID. The `hint` directs the agent
    /// at the human creation flow (`kutl init` / desktop / web UI /
    /// `POST /spaces/register`) — space creation is intentionally not
    /// an MCP capability.
    #[error("space not found: {space_id} — {hint}")]
    SpaceNotFound {
        /// Space identifier the caller supplied.
        space_id: String,
        /// Recovery hint surfaced to the calling agent.
        hint: String,
    },
    /// An unexpected internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl McpError {
    /// Classify a backend error by whose fault it is.
    ///
    /// Exists rather than `map_err(|e| Internal(e.to_string()))` at each site
    /// because argument validation lives inward at the authored
    /// seam: a caller mistake arriving as `Internal` comes back to the user as a
    /// 500 for what is really a 400. `InvalidArgument` maps to
    /// [`McpError::RecordRejected`], already documented as the client-error
    /// variant. `NotFound` is not handled here — the client-error shape for it
    /// needs a signal id this conversion does not have, so those sites keep their
    /// own `match`.
    ///
    /// **An inherent method, deliberately, not `impl From`.** A `From` impl on an
    /// error type used with `?` becomes a second conversion candidate, making
    /// inference ambiguous at an unrelated `?` several hundred lines
    /// away (`E0283` at the `replace_content` closure). A named function converts
    /// exactly where it is written and nowhere else.
    pub(crate) fn from_change(e: crate::change_backend::ChangeError) -> Self {
        match e {
            crate::change_backend::ChangeError::InvalidArgument { reason } => {
                Self::RecordRejected { reason }
            }
            other => Self::Internal(other.to_string()),
        }
    }
}

/// Content and version of a document read via MCP.
#[derive(Debug, Serialize)]
pub struct McpDocumentContent {
    /// The full text content.
    pub content: String,
    /// Opaque token naming the read content and frontier, minted by
    /// [`crate::doc_version::mint`]. A caller echoes it back on write to
    /// name the base it edited against; nothing outside that module
    /// interprets the payload.
    pub version: String,
}

/// Summary of a document for listing.
#[derive(Debug, Serialize)]
pub struct McpDocumentSummary {
    /// Document identifier (UUID).
    pub document_id: String,
    /// Human-readable path (may equal `document_id` if none registered).
    pub path: String,
    /// Content type: "text", "blob", or "empty".
    pub content_type: String,
    /// Number of WS subscribers.
    pub subscriber_count: usize,
    /// `SourceKind` enum value (proto u32), when set by an import.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_kind: Option<u32>,
    /// Source-side stable identifier (e.g. Notion page id), when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    /// Canonical source URL for "view original" links, when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_url: Option<String>,
    /// Non-DID author display string captured from the source, when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_author_display: Option<String>,
    /// Source-side creation timestamp in Unix milliseconds, when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub originally_created_at: Option<i64>,
    /// Ingestion job UUID (set by the format-service worker on its
    /// built-in path; rarely populated by cross-MCP callers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingestion_job_id: Option<String>,
}

/// A log entry returned by `read_log`.
#[derive(Debug, Serialize)]
pub struct McpLogEntry {
    /// DID of the change author.
    pub author_did: String,
    /// Human-readable intent.
    pub intent: String,
    /// Timestamp in Unix milliseconds.
    pub timestamp: i64,
    /// Change identifier.
    pub id: String,
    /// Boundary type.
    pub boundary: String,
    /// Whether this change was a full rewrite (bulk path).
    pub full_rewrite: bool,
}

/// A participant connected to a space.
#[derive(Debug, Clone, Serialize)]
pub struct McpParticipant {
    /// DID of the participant.
    pub did: String,
    /// What participants call this DID, when the relay knows a name.
    ///
    /// `None` is legal and common — an entry that names nobody is still an
    /// actor, reachable by DID.
    pub name: Option<String>,
    /// Whether this actor is here right now, and over what: `"websocket"` (a
    /// connection listening to the space: a daemon, `kutl mcp serve`), `"mcp"`
    /// (a live MCP session), or `"offline"`.
    ///
    /// A status, not a filter. Everyone who may act in the space is listed
    /// whether or not they are here: you address someone precisely because
    /// they are away, and a roster that hid them would teach a caller they do
    /// not exist.
    pub connection_type: String,
}

/// A participant a typed name resolved to.
#[derive(Debug, Serialize)]
pub struct McpResolvedParticipant {
    /// The DID the name resolved to — what a record actually carries.
    pub did: String,
    /// The canonical stored name, not the query as typed — so an ambiguity
    /// error built from several of these lists fully-qualified candidates
    /// (`cfo/accountant`) rather than repeating the short query the caller
    /// already knows it sent.
    pub name: String,
}

/// Space status information.
#[derive(Debug, Serialize)]
pub struct McpSpaceStatus {
    /// Number of documents in the space.
    pub document_count: usize,
    /// Connections listening to the space right now: the parties present over
    /// websocket.
    pub listener_count: usize,
    /// Number of active MCP sessions.
    pub mcp_session_count: usize,
}

/// Result of an edit operation via MCP.
///
/// Carries no version token, deliberately. A merge leaves the document holding
/// the caller's content AND whatever a peer contributed meanwhile, so a token
/// minted here would honestly name text the caller has never read — and a
/// second edit based on it would express the peer's contribution as a
/// deletion, which is the defect merging exists to remove. Nothing detects
/// that, either: the digest legitimately matches. A caller that wants to edit
/// again reads again.
#[derive(Debug, Serialize)]
pub struct McpEditResult {
    /// The actual document UUID (may differ from the requested ID if
    /// a non-UUID string was provided and a UUID was auto-generated).
    pub document_id: String,
    /// Number of coalesced insert/delete actions applied — one per contiguous
    /// changed run within a placed region, so it scales with what the edit
    /// changed, not with how much text surrounds it. Not a character count and
    /// not a quota figure.
    pub ops_applied: usize,
    /// One sentence per region that could not be placed, so the caller learns
    /// which part of its edit did not land rather than assuming all of it did.
    /// A non-empty list is still a successful edit: everything else landed.
    pub hunks_refused: Vec<String>,
}

/// Result of `create_document`.
#[derive(Debug, Serialize)]
pub struct McpCreateDocumentResult {
    /// The newly minted document UUID.
    pub document_id: String,
}

/// Result of `upload_blob`.
#[derive(Debug, Serialize)]
pub struct McpUploadBlobResult {
    /// The document UUID (stable across replace).
    pub document_id: String,
    /// Relay-relative URL for markdown embedding — currently the same
    /// within-space path the caller supplied.
    pub content_url: String,
}

/// A space record surfaced through `list_spaces`.
///
/// Owner fields (`owner_account_id`, `owner_display_name`) are
/// populated when a `MembershipBackend` is configured (kutlhub
/// deployment); the OSS in-process path leaves them as `None`
/// because there is no `accounts` table to resolve through.
#[derive(Debug, Serialize)]
pub struct McpSpaceSummary {
    /// Internal space identifier (currently the slug on the OSS path;
    /// real UUID on the membership-backed path).
    pub space_id: String,
    /// Slug as used in URLs and tool calls.
    pub slug: String,
    /// Human-readable name. Defaults to the slug when no name is set.
    pub name: String,
    /// Account UUID of the space owner. `None` when no
    /// `MembershipBackend` is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_account_id: Option<String>,
    /// Display name of the space owner, used by agents to
    /// cross-reference `PatAttribution.owner_display_name`.
    /// `None` when no `MembershipBackend` is configured or when the
    /// owner has no display name set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_display_name: Option<String>,
}

/// A signal lookup's failure in MCP's error shape: unknown signal versus a
/// projection that could not answer, so a caller with a good id is not told
/// the id is wrong.
fn signal_lookup_error(e: crate::change_backend::ChangeError, signal_id: &str) -> McpError {
    match e {
        crate::change_backend::ChangeError::NotFound(_) => McpError::SignalNotFound {
            signal_id: signal_id.to_owned(),
        },
        other => McpError::Internal(other.to_string()),
    }
}

impl Relay {
    // -----------------------------------------------------------------------
    // MCP handlers
    // -----------------------------------------------------------------------

    pub(super) async fn handle_mcp_validate_token(
        &mut self,
        token: &str,
    ) -> Result<(String, Option<crate::auth::PatAuthContext>), AuthError> {
        let now = kutl_core::now_ms();
        self.auth
            .validate_token_with_backend(
                token,
                now,
                self.session_backend.as_deref(),
                self.pat_backend.as_deref(),
            )
            .await
    }

    pub(super) fn handle_mcp_create_session(
        &mut self,
        did: &str,
        pat: Option<crate::auth::PatAuthContext>,
    ) -> McpSessionId {
        let session_id = generate_session_id();
        info!(session_id = %session_id, %did, "MCP session created");
        self.mcp_sessions.insert(
            session_id.clone(),
            McpSession {
                did: did.to_owned(),
                pat,
                notify_tx: None,
                last_active: tokio::time::Instant::now(),
            },
        );
        session_id
    }

    pub(super) fn handle_mcp_destroy_session(&mut self, session_id: &str) {
        if self.mcp_sessions.remove(session_id).is_some() {
            info!(session_id, "MCP session destroyed");
        }
    }

    /// Validate that a session exists and belongs to the expected DID.
    ///
    /// The session's DID — bound from the validated bearer at initialize time —
    /// must match `expected_did`; otherwise the session is treated as not found.
    pub(super) fn handle_mcp_validate_session_did(
        &mut self,
        session_id: &str,
        expected_did: &str,
    ) -> Result<(), McpError> {
        let did = self.validate_mcp_session(session_id)?;
        if did != expected_did {
            return Err(McpError::SessionNotFound);
        }
        Ok(())
    }

    /// Validate that an MCP session exists (and is not idle-expired) and return
    /// the DID. Refreshes `last_active` on success.
    fn validate_mcp_session(&mut self, session_id: &str) -> Result<String, McpError> {
        let session = self
            .mcp_sessions
            .get_mut(session_id)
            .ok_or(McpError::SessionNotFound)?;

        if session.last_active.elapsed() > MCP_SESSION_IDLE_TTL {
            info!(session_id, did = %session.did, "MCP session expired (idle)");
            self.mcp_sessions.remove(session_id);
            return Err(McpError::SessionNotFound);
        }

        session.last_active = tokio::time::Instant::now();
        Ok(session.did.clone())
    }

    /// Validate the MCP session and authorize the caller against the space.
    ///
    /// Returns the caller's [`AuthorIdentity`] on success — the underlying
    /// DID plus, when PAT-authenticated, the `pat_id` for per-PAT signal
    /// attribution. This is the standard preamble shared by
    /// all MCP handler methods that operate on a specific space.
    async fn authorize_mcp_caller(
        &mut self,
        session_id: &str,
        space_id: &str,
    ) -> Result<AuthorIdentity, McpError> {
        let did = self.validate_mcp_session(session_id)?;
        let pat = self
            .mcp_sessions
            .get(session_id)
            .and_then(|s| s.pat.as_ref());
        let pat_id = pat.map(|p| p.pat_id.clone());
        let decision = self.authorize_space(&did, space_id, pat.map(|p| p.pat_hash.as_str()));
        let _authorized = decision.await.map_err(|_| McpError::NotAuthorized {
            space_id: space_id.to_owned(),
        })?;
        Ok(AuthorIdentity {
            did,
            via_pat_id: pat_id,
        })
    }

    /// Resolve a potentially non-UUID `document_id` to the internal UUID
    /// via the registry's path index. Returns the original value if it's
    /// already a UUID or has no registry mapping.
    fn resolve_doc_id(&self, space_id: &str, document_id: &str) -> String {
        if uuid::Uuid::try_parse(document_id).is_ok() {
            return document_id.to_owned();
        }
        if let Some(reg) = self.registries.get(space_id)
            && let Some(entry) = reg.get_by_path(document_id)
        {
            return entry.document_id.clone();
        }
        document_id.to_owned()
    }

    /// Check whether `space_id` is known to this relay before allowing
    /// an MCP write to create a document in it. Space creation is NOT an
    /// MCP capability;
    /// agents discover destinations via `list_spaces` and humans create
    /// spaces via `kutl init` / desktop / web UI / `POST /spaces/register`.
    ///
    /// Resolution rules:
    /// - **Persistent mode** (`space_backend` configured): the backend
    ///   is the source of truth. Check `resolve_by_id` first (UUID-shape
    ///   `space_id`s), then `resolve_by_name` (slug-shape). Either hit
    ///   means the space exists. A miss on both means error. A backend
    ///   ERROR (transient PG outage, pool timeout) is NOT a miss —
    ///   propagate as `McpError::Internal` so the caller surfaces a
    ///   retry-able message rather than misleading the agent into
    ///   thinking the space needs to be created.
    /// - **No space backend** (`space_backend` is None: the hosted relay,
    ///   whose UX server owns spaces, and the in-memory test relay): there
    ///   is no space-registry authority to resolve against, so return the raw
    ///   `space_id` and let the caller's membership check be the gate.
    ///   This is NOT implicit space creation: every MCP handler runs
    ///   `authorize_mcp_caller` (membership authz) immediately after, and
    ///   a caller cannot be a member of a space that does not exist — so
    ///   on the kutlhub host relay (the only production reacher of this
    ///   branch; `membership_backend` is Some, spaces live in UX
    ///   Postgres) an unknown space is rejected downstream, and this
    ///   branch only lazily materializes the in-memory registry for an
    ///   already-existing space on its first MCP touch. Genuine
    ///   implicit-create-on-first-touch survives only in backendless
    ///   tests (no membership backend either). The OSS binary always has
    ///   a space backend, so it never reaches this branch. The
    ///   implicit-create exception is entangled with the WS
    ///   unknown-space birth path — the same question resolved oppositely.
    ///
    /// **Dual-shape `space_id` (slug or UUID).** Accepts both input
    /// forms — `resolve_by_id` covers UUID-shape, `resolve_by_name`
    /// covers slug-shape. The returned `String` is the **canonical
    /// UUID**, so callers can rebind `space_id` and key every
    /// downstream backend / registry call on a single shape. This is
    /// the entry-point guard against the slug-vs-UUID
    /// parallel-registry-entry
    /// bug class — every MCP handler that takes a `space_id` MUST call
    /// this first and use the returned value from that point on.
    ///
    /// With **no `space_backend`** (the hosted relay, whose UX server owns
    /// spaces, or a standalone test actor) there is no canonical authority —
    /// return the raw caller-supplied string. Existence is gated by the
    /// downstream membership check (see the no-space-backend bullet above),
    /// not created here. The hosted relay and backendless test scenarios
    /// accept the consequence of slug-vs-UUID drift.
    fn mcp_check_space_registered(
        &self,
        space_id: &str,
    ) -> impl Future<Output = Result<String, McpError>> + Send + use<> {
        let backend = self.space_backend.clone();
        let space_id = space_id.to_owned();
        async move {
            if let Some(backend) = backend {
                match backend.resolve_by_id(&space_id).await {
                    Ok(Some(reg)) => return Ok(reg.space_id),
                    Ok(None) => {}
                    Err(e) => {
                        return Err(McpError::Internal(format!(
                            "space backend lookup failed for {space_id}: {e}"
                        )));
                    }
                }
                match backend.resolve_by_name(&space_id).await {
                    Ok(Some(reg)) => return Ok(reg.space_id),
                    Ok(None) => {}
                    Err(e) => {
                        return Err(McpError::Internal(format!(
                            "space backend lookup failed for {space_id}: {e}"
                        )));
                    }
                }
                return Err(McpError::SpaceNotFound {
                    space_id,
                    hint: SPACE_NOT_FOUND_HINT.to_owned(),
                });
            }
            // No space backend to resolve against. Return the raw id; the
            // caller's membership check (authorize_mcp_caller) is
            // the existence gate, so this is lazy in-memory materialization of
            // an already-existing space, not implicit create (except in
            // backendless tests). See doc-comment above.
            Ok(space_id)
        }
    }

    pub(super) async fn handle_mcp_read_document(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
    ) -> Result<McpDocumentContent, McpError> {
        // Rebind to the canonical UUID so every downstream registry /
        // backend / DocKey call keys on the same shape regardless of
        // whether the caller passed slug- or UUID-form.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        self.authorize_mcp_caller(session_id, space_id).await?;

        let resolved_id = self.resolve_doc_id(space_id, document_id);
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: resolved_id,
        };

        if !self.ensure_doc_loaded(&key).await {
            return Err(McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                hint: DOC_NOT_FOUND_HINT_READ.to_owned(),
            });
        }
        let slot = self
            .documents
            .get(&key)
            .expect("ensure_doc_loaded returned true");

        match &slot.content {
            DocContent::Empty => Ok(McpDocumentContent {
                content: String::new(),
                version: crate::doc_version::mint(space_id, &key.document_id, "", &[]),
            }),
            DocContent::Text(doc) => {
                let content = doc.content();
                let version = crate::doc_version::mint(
                    space_id,
                    &key.document_id,
                    &content,
                    &doc.local_version(),
                );
                Ok(McpDocumentContent { content, version })
            }
            DocContent::Blob(_) => Err(McpError::NotTextDocument),
        }
    }

    pub(super) async fn handle_read_document_text_by_did(
        &mut self,
        did: &str,
        space_id: &str,
        document_id: &str,
    ) -> Result<String, McpError> {
        let _authorized = self
            .authorize_space(did, space_id, None)
            .await
            .map_err(|_| McpError::NotAuthorized {
                space_id: space_id.to_owned(),
            })?;

        let resolved_id = self.resolve_doc_id(space_id, document_id);
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: resolved_id,
        };

        if !self.ensure_doc_loaded(&key).await {
            return Err(McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                hint: DOC_NOT_FOUND_HINT_READ.to_owned(),
            });
        }
        let slot = self
            .documents
            .get(&key)
            .expect("ensure_doc_loaded returned true");

        match &slot.content {
            DocContent::Empty => Err(McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                hint: DOC_NOT_FOUND_HINT_READ.to_owned(),
            }),
            DocContent::Text(doc) => Ok(doc.content()),
            DocContent::Blob(_) => Err(McpError::NotTextDocument),
        }
    }

    pub(super) async fn handle_mcp_list_documents(
        &mut self,
        session_id: &str,
        space_id: &str,
    ) -> Result<Vec<McpDocumentSummary>, McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        self.authorize_mcp_caller(session_id, space_id).await?;

        // Iterate the registry (source of truth), same as the WS
        // ListSpaceDocuments path. Augment with in-memory content_type
        // and subscriber_count when the doc is loaded.
        let docs: Vec<McpDocumentSummary> = self
            .registries
            .get(space_id)
            .map(|reg| {
                reg.active_entries()
                    .map(|(_, entry)| {
                        let key = DocKey {
                            space_id: space_id.to_owned(),
                            document_id: entry.document_id.clone(),
                        };
                        let (content_type, subscriber_count) = match self.documents.get(&key) {
                            Some(slot) => {
                                let ct = match &slot.content {
                                    DocContent::Empty => "empty",
                                    DocContent::Text(_) => "text",
                                    DocContent::Blob(_) => "blob",
                                };
                                (ct, slot.subscribers.active_count())
                            }
                            None => ("unknown", 0),
                        };
                        McpDocumentSummary {
                            document_id: entry.document_id.clone(),
                            path: entry.path.clone(),
                            content_type: content_type.into(),
                            subscriber_count,
                            source_kind: entry.source_kind,
                            source_id: entry.source_id.clone(),
                            source_url: entry.source_url.clone(),
                            source_author_display: entry.source_author_display.clone(),
                            originally_created_at: entry.originally_created_at,
                            ingestion_job_id: entry.ingestion_job_id.clone(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(docs)
    }

    pub(super) async fn handle_mcp_read_log(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<McpLogEntry>, McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        self.authorize_mcp_caller(session_id, space_id).await?;

        let resolved_id = self.resolve_doc_id(space_id, document_id);
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: resolved_id,
        };

        let slot = self
            .documents
            .get(&key)
            .ok_or_else(|| McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                hint: DOC_NOT_FOUND_HINT_READ.to_owned(),
            })?;

        let changes = match &slot.content {
            DocContent::Text(doc) => doc.changes(),
            _ => return Ok(Vec::new()),
        };

        let iter = changes.iter().rev();
        let entries: Vec<McpLogEntry> = if let Some(n) = limit {
            iter.take(n).map(change_to_log_entry).collect()
        } else {
            iter.map(change_to_log_entry).collect()
        };

        Ok(entries)
    }

    pub(super) async fn handle_mcp_list_participants(
        &mut self,
        session_id: &str,
        space_id: &str,
    ) -> Result<Vec<McpParticipant>, McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        self.authorize_mcp_caller(session_id, space_id).await?;

        // Who is here right now. Over websocket that is the space's listeners
        // (to be present is to be listening); MCP sessions are not scoped at
        // all, so a session may only MARK an actor of this space as connected
        // — it must never add one, or a session in another space would leak
        // its DID into this list.
        let mut live: std::collections::HashMap<String, &'static str> =
            std::collections::HashMap::new();
        if let Some(conns) = self.listeners.get(space_id) {
            for conn_id in conns.keys() {
                if let Some((did, _)) = self.authenticated.get(conn_id) {
                    live.insert(did.clone(), "websocket");
                }
            }
        }
        for session in self.mcp_sessions.values() {
            live.entry(session.did.clone()).or_insert("mcp");
        }

        // The actor set: everyone authorized to act here, present or not,
        // as (did, name) pairs from whichever roster this relay has.
        let roster: Vec<(String, Option<String>)> =
            if let Some(keys) = self.authorized_keys.as_ref() {
                keys.dids_for_space(space_id, kutl_core::now_ms())
                    .into_iter()
                    .map(|did| {
                        let name = keys.name_for(&did);
                        (did, name)
                    })
                    .collect()
            } else if let Some(membership) = self.membership_backend.as_ref().map(Arc::clone) {
                // A membership-backed relay has no authorized-keys file to
                // enumerate; the backend's roster IS the actor set here.
                membership
                    .list_space_participants(space_id)
                    .await
                    .map_err(|e| McpError::Internal(e.to_string()))?
                    .into_iter()
                    .map(|participant| (participant.did, Some(participant.name)))
                    .collect()
            } else {
                // Neither authorizer is configured (test shape): there is no
                // actor set to check a live DID against, so whoever is
                // connected is all this can report.
                live.keys().map(|did| (did.clone(), None)).collect()
            };

        // One pass over whichever roster answered. The outsider rule holds for
        // all of them: a live DID marks presence on an entry that is already
        // here, and never adds one, or a session in another space would leak
        // its DID into this roster.
        let mut participants = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (did, name) in roster {
            let connection_type = (*live.get(&did).unwrap_or(&"offline")).to_owned();
            if seen.insert(did.clone()) {
                participants.push(McpParticipant {
                    did,
                    name,
                    connection_type,
                });
            }
        }

        Ok(participants)
    }

    /// Resolve a typed name to the participants it names in this space.
    ///
    /// Answers over the AUTHORIZED set, not who is connected: a signal waits
    /// for its recipient, so a name has to resolve while its owner is away.
    ///
    /// Deliberately a lookup and not a listing. A caller must already hold the
    /// name it is asking about, so this confirms a guess rather than handing
    /// out a roster — which is what lets it read the authorization list at all.
    ///
    /// Returns every match. One is a resolution, several are an ambiguity for
    /// the caller to refuse, none means nobody here answers to that name.
    /// Choosing among them here would send someone else's mail.
    pub(super) async fn handle_mcp_resolve_participant(
        &mut self,
        session_id: &str,
        space_id: &str,
        name: &str,
    ) -> Result<Vec<McpResolvedParticipant>, McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        self.authorize_mcp_caller(session_id, space_id).await?;

        if let Some(keys) = self.authorized_keys.as_ref() {
            return Ok(keys
                .dids_named(name, space_id, kutl_core::now_ms())
                .into_iter()
                .map(|did| {
                    let canonical_name = keys.name_for(&did).unwrap_or_else(|| name.to_owned());
                    McpResolvedParticipant {
                        did,
                        name: canonical_name,
                    }
                })
                .collect());
        }

        if let Some(membership) = self.membership_backend.as_ref().map(Arc::clone) {
            let mut candidates = membership
                .list_space_participants(space_id)
                .await
                .map_err(|e| McpError::Internal(e.to_string()))?;
            candidates.retain(|p| name_path_matches(&p.name, name));
            return Ok(candidates
                .into_iter()
                .map(|p| McpResolvedParticipant {
                    did: p.did,
                    name: p.name,
                })
                .collect());
        }

        // Neither authorizer is configured (a test actor): there is
        // no name directory to resolve against, and the caller falls back to
        // a DID.
        Ok(Vec::new())
    }

    pub(super) async fn handle_mcp_status(
        &mut self,
        session_id: &str,
        space_id: &str,
    ) -> Result<McpSpaceStatus, McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        self.authorize_mcp_caller(session_id, space_id).await?;

        let document_count = self
            .documents
            .keys()
            .filter(|key| key.space_id == space_id)
            .count();
        let listener_count = self
            .listeners
            .get(space_id)
            .map_or(0, std::collections::HashMap::len);

        Ok(McpSpaceStatus {
            document_count,
            listener_count,
            mcp_session_count: self.mcp_sessions.len(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_mcp_edit_document(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
        base_version: &str,
        new_content: &str,
        intent: &str,
        _snippet: &str,
    ) -> Result<McpEditResult, McpError> {
        // FIXME: document-edit PAT attribution is deferred —
        // an agent editing a doc via PAT currently attributes to its
        // human principal's DID with no PAT distinction. The shape
        // would mirror how signal attribution works (a sibling column
        // on the documents table); it is undesigned, not "documents
        // are intentionally human-only".
        //
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id } =
            self.authorize_mcp_caller(session_id, space_id).await?;

        // Strict resolution: the document MUST already exist in the
        // registry. No auto-create — that's `create_document`'s job.
        // The hint string is part of the agent-facing
        // contract; do not drop it.
        let actual_doc_id = self
            .resolve_existing_doc_id(space_id, document_id)
            .ok_or_else(|| McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                hint: DOC_NOT_FOUND_HINT_EDIT.to_owned(),
            })?;

        // Hydrate before editing so the blob guard can fire — see
        // `hydrate_doc_for_edit` for the classification rationale.
        self.hydrate_doc_for_edit(space_id, &actual_doc_id)
            .await
            .map_err(McpError::Internal)?;

        self.apply_relay_text_edit(
            session_id,
            space_id,
            &actual_doc_id,
            &did,
            via_pat_id,
            intent,
            Some(base_version),
            new_content,
        )
        .await
    }

    /// Resolve a `document_id` that may be a UUID or a within-space path
    /// to the canonical UUID. Returns `None` when there is no registry
    /// entry — callers translate this into [`McpError::DocumentNotFound`]
    /// with the appropriate agent-facing hint.
    fn resolve_existing_doc_id(&self, space_id: &str, document_id: &str) -> Option<String> {
        if uuid::Uuid::try_parse(document_id).is_ok() {
            // Even a UUID must exist in the registry — otherwise an
            // agent guessing UUIDs would silently miss real documents.
            if let Some(reg) = self.registries.get(space_id)
                && reg.get(document_id).is_some()
            {
                return Some(document_id.to_owned());
            }
            return None;
        }
        self.registries
            .get(space_id)?
            .get_by_path(document_id)
            .map(|e| e.document_id.clone())
    }

    /// Apply a relay-side text edit to an already-resolved,
    /// already-registered document slot: the MCP edit and create paths, and
    /// the decision-marker flip the transition path performs. Does NOT create
    /// or register the document; the caller is responsible for that.
    ///
    /// `base_version` names the text the caller composed against, and only the
    /// difference between that text and `new_content` is applied — everything
    /// a peer added meanwhile is left alone, because the caller never expressed
    /// a change to it. `None` diffs against the CURRENT content (the create
    /// path's whole-write, and the flip's read-splice-write, which composed
    /// against the resident text a moment ago).
    ///
    /// `session_id` becomes the CRDT agent name (the DID can exceed
    /// diamond-types' 50-byte limit) and is excluded from the peer-session
    /// notification. An MCP session passes its own id; the flip passes a
    /// minted uuid, which keeps agent identity unique across relays and makes
    /// the exclusion a no-op — every session hears about the change.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn apply_relay_text_edit(
        &mut self,
        session_id: &str,
        space_id: &str,
        actual_doc_id: &str,
        did: &str,
        via_pat_id: Option<String>,
        intent: &str,
        base_version: Option<&str>,
        new_content: &str,
    ) -> Result<McpEditResult, McpError> {
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: actual_doc_id.to_owned(),
        };

        // A pending debounced edit by a DIFFERENT identity flushes BEFORE
        // this mutation, so its merge event fires against the content its
        // author actually produced — the same guard the WS merge path
        // applies. With the orders reversed, the pending author's event would
        // fire on the post-edit content and claim this edit's changes as
        // theirs (for the decision flip, that is the transition record itself
        // minted under the wrong author). Identity is the (did, pat) pair, as
        // on the WS path: a person and their agent share a DID.
        if self
            .documents
            .get(&key)
            .and_then(|slot| slot.pending_edit.as_ref())
            .is_some_and(|p| p.author_did != did || p.via_pat_id != via_pat_id)
        {
            self.flush_pending_edit_for(&key).await;
        }

        // Auto-create the in-memory slot if it doesn't exist — registry
        // entries can exist without a hot slot (post-restart, post-evict).
        let slot = self
            .documents
            .entry(key.clone())
            .or_insert_with(DocSlot::empty);

        // Ensure content is Text.
        if let DocContent::Blob(_) = &slot.content {
            return Err(McpError::NotTextDocument);
        }
        slot.ensure_text();

        // Notify before-merge observer (captures baseline for snippet/mention diffing).
        if let DocContent::Text(doc) = &slot.content {
            self.before_merge
                .before_text_merge(space_id, actual_doc_id, doc);
        }

        // Apply the edit. mutate_text marks the slot dirty on any Ok — a write
        // that placed nothing marks it too. Harmless: content is unchanged, so
        // the worst of it is a flush with nothing to say.
        // Use session_id (36 bytes) as CRDT agent name — the DID can
        // exceed diamond-types' 50-byte limit.
        let did_owned = did.to_owned();
        let session_owned = session_id.to_owned();
        let (ops_applied, hunks_refused) = slot.mutate_text(|doc| {
            let agent = doc
                .register_agent(&session_owned)
                .map_err(|e| McpError::EditFailed(e.to_string()))?;
            let write = match base_version {
                None => {
                    let outcome = doc
                        .replace_content(agent, &did_owned, intent, Boundary::Explicit, new_content)
                        .map_err(|e| McpError::EditFailed(e.to_string()))?;
                    (outcome.ops_applied, Vec::new())
                }
                Some(token) => {
                    let base = resolve_base(doc, token, space_id, actual_doc_id)?;
                    let outcome = doc
                        .merge_from_base(
                            agent,
                            &did_owned,
                            intent,
                            Boundary::Explicit,
                            &base,
                            new_content,
                        )
                        .map_err(|e| McpError::EditFailed(e.to_string()))?;
                    let refused = outcome
                        .refused
                        .iter()
                        .map(|(reason, excerpt)| describe_refusal(*reason, excerpt))
                        .collect();
                    (outcome.ops_applied, refused)
                }
            };
            Ok(write)
        })?;

        // Nothing moved — an unchanged payload, or an edit whose every region
        // was refused. There is no delta to relay and nothing to notify anyone
        // about, but the caller still gets its refusals.
        if ops_applied > 0 {
            self.broadcast_mcp_text_edit(&key, session_id, did, via_pat_id, intent, ops_applied)
                .await;
        }

        Ok(McpEditResult {
            document_id: actual_doc_id.to_owned(),
            ops_applied,
            hunks_refused,
        })
    }

    /// Post-mutation half of `apply_relay_text_edit`: stamp `edited_at`,
    /// relay deltas to WS subscribers, notify peer MCP sessions, flush
    /// the pending edit through the debounce window (MCP edits bypass
    /// it), and fire the after-merge observer.
    ///
    /// Split out of `apply_relay_text_edit` purely for readability — the
    /// two halves have different shapes (pure CRDT mutation vs. fan-out
    /// of side effects).
    async fn broadcast_mcp_text_edit(
        &mut self,
        key: &DocKey,
        session_id: &str,
        did: &str,
        via_pat_id: Option<String>,
        intent: &str,
        ops_applied: usize,
    ) {
        let slot = self
            .documents
            .get_mut(key)
            .expect("doc slot must exist after merge");
        slot.edited_at_pending = Some(EditedAtPending {
            timestamp: kutl_core::env::now_ms(),
            author_did: did.to_owned(),
        });

        build_and_relay_text_outbound(
            &self.connections,
            slot,
            key,
            None, // MCP edits have no WS sender — relay to all subscribers.
            &key.space_id,
            &key.document_id,
        );

        self.send_mcp_notification(session_id, &key.space_id, &key.document_id, did, intent);

        self.flush_pending_edit_for(key).await;
        // `invoke_after_merge` seeds the observer's baseline once per doc
        // (restart correctness) and fires only for text content.
        let event = MergedEvent {
            space_id: key.space_id.clone(),
            document_id: key.document_id.clone(),
            author_did: did.to_owned(),
            via_pat_id,
            op_count: ops_applied,
            intent: intent.to_owned(),
            content_mode: EditContentMode::Text,
            timestamp: kutl_core::env::now_ms(),
        };
        self.invoke_after_merge(key, event).await;
    }

    /// MCP `create_document` handler — mints a new UUID, registers with
    /// the supplied provenance, then applies initial content via
    /// [`apply_relay_text_edit`]. Path collisions surface as
    /// [`McpError::PathAlreadyInUse`]; unknown spaces surface as
    /// [`McpError::SpaceNotFound`].
    ///
    /// **Unknown-space behaviour:** space creation is NOT an MCP
    /// capability. On the OSS
    /// binary (`SQLite` `space_backend`, always present)
    /// the handler errors on an unknown `space_id` and routes the agent at
    /// the human creation flow. The kutlhub host relay runs with no
    /// `space_backend` (spaces live in UX Postgres); there, an unknown
    /// space is rejected by the downstream membership check rather than by
    /// this handler, and the no-space-backend branch only lazily materializes the
    /// in-memory registry for an already-existing space — it does not
    /// create spaces. See `mcp_check_space_registered` for the policy detail.
    pub(super) async fn handle_mcp_create_document(
        &mut self,
        session_id: &str,
        space_id: &str,
        path: &str,
        content: &str,
        provenance: mcp_tools::ProvenanceArgs,
    ) -> Result<McpCreateDocumentResult, McpError> {
        // FIXME: see `apply_relay_text_edit` — document-edit
        // PAT attribution is deferred pending a documents-side
        // design pass. The discard here is not "intentionally
        // human-only"; it's "not designed yet."
        //
        // Canonicalize at the boundary BEFORE authorize_mcp_caller so
        // PAT scope + membership checks key on the canonical UUID;
        // see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id } =
            self.authorize_mcp_caller(session_id, space_id).await?;

        // Reject path collisions early so the agent can disambiguate
        // rather than getting a confusing registry error after a partial
        // mutation.
        if let Some(reg) = self.registries.get(space_id)
            && let Some(existing) = reg.get_by_path(path)
        {
            return Err(McpError::PathAlreadyInUse {
                path: path.to_owned(),
                existing_document_id: existing.document_id.clone(),
            });
        }

        let actual_doc_id = uuid::Uuid::new_v4().to_string();
        let account_id = self.account_for_identity(&did).await.ok().flatten();

        let provenance = registry::SourceProvenance {
            originally_created_at_ms: provenance.originally_created_at_ms,
            source_kind: provenance.source_kind,
            source_id: provenance.source_id,
            source_url: provenance.source_url,
            ingestion_job_id: provenance.ingestion_job_id,
            source_author_display: provenance.source_author_display,
        };

        self.register_document_internal(
            space_id,
            &actual_doc_id,
            path,
            &did,
            account_id,
            provenance,
            // MCP `create_document` doesn't surface UX-only metadata
            // (title/content_type/convert provenance/size) — the
            // mirror's filename-derived title fallback applies. Web
            // `CreateDocument` is the path that supplies them.
            &MirrorMetadata::default(),
            kutl_core::env::now_ms(),
            None,
            NO_SKIP_CONN,
        )
        .await
        .map_err(McpError::EditFailed)?;

        if !content.is_empty() {
            let _ = self
                .apply_relay_text_edit(
                    session_id,
                    space_id,
                    &actual_doc_id,
                    &did,
                    via_pat_id,
                    "create_document",
                    // A document minted a moment ago has no base to name.
                    None,
                    content,
                )
                .await?;
        }

        Ok(McpCreateDocumentResult {
            document_id: actual_doc_id,
        })
    }

    /// MCP `upload_blob` handler — creates or replaces a blob document
    /// at `path`. Enforces the size cap, rejects path collisions with
    /// text documents, and preserves provenance fields on omit when
    /// replacing an existing blob.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_mcp_upload_blob(
        &mut self,
        session_id: &str,
        space_id: &str,
        path: &str,
        _content_type: &str,
        bytes: Vec<u8>,
        provenance: mcp_tools::ProvenanceArgs,
        max_bytes: usize,
    ) -> Result<McpUploadBlobResult, McpError> {
        // FIXME: see `apply_relay_text_edit` — blob-upload
        // PAT attribution is deferred pending the documents-side
        // design pass. Not intentionally human-only.
        //
        // Canonicalize at the boundary BEFORE authorize_mcp_caller so
        // PAT scope + membership checks key on the canonical UUID;
        // see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id } =
            self.authorize_mcp_caller(session_id, space_id).await?;

        if bytes.len() > max_bytes {
            return Err(McpError::BlobTooLarge {
                actual: bytes.len(),
                max: max_bytes,
            });
        }

        // Determine whether this is a create or replace by checking the
        // registry path index. Text documents at the same path are a
        // hard conflict.
        let (actual_doc_id, is_new) = match self
            .registries
            .get(space_id)
            .and_then(|reg| reg.get_by_path(path))
        {
            Some(existing) => {
                let key = DocKey {
                    space_id: space_id.to_owned(),
                    document_id: existing.document_id.clone(),
                };
                if let Some(slot) = self.documents.get(&key)
                    && matches!(slot.content, DocContent::Text(_))
                {
                    return Err(McpError::PathTypeConflict {
                        path: path.to_owned(),
                    });
                }
                (existing.document_id.clone(), false)
            }
            None => (uuid::Uuid::new_v4().to_string(), true),
        };

        if is_new {
            let account_id = self.account_for_identity(&did).await.ok().flatten();
            let provenance = registry::SourceProvenance {
                originally_created_at_ms: provenance.originally_created_at_ms,
                source_kind: provenance.source_kind,
                source_id: provenance.source_id,
                source_url: provenance.source_url,
                ingestion_job_id: provenance.ingestion_job_id,
                source_author_display: provenance.source_author_display,
            };
            self.register_document_internal(
                space_id,
                &actual_doc_id,
                path,
                &did,
                account_id,
                provenance,
                // MCP `upload_blob` doesn't surface UX-only metadata
                // here — the mirror's filename-derived title fallback
                // applies.
                &MirrorMetadata::default(),
                kutl_core::env::now_ms(),
                None,
                NO_SKIP_CONN,
            )
            .await
            .map_err(McpError::EditFailed)?;
        } else {
            // Replace path: update provenance leave-as-is-on-omit.
            self.merge_blob_provenance(space_id, &actual_doc_id, &provenance);
        }

        // Set the blob bytes in the in-memory slot. The flush task will
        // persist via `BlobBackend::save`; in-memory presence is enough
        // for immediate consistency on subsequent reads.
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: actual_doc_id.clone(),
        };
        let slot = self
            .documents
            .entry(key.clone())
            .or_insert_with(DocSlot::empty);

        // Compute content hash (sha256 of bytes) — used for dedup and
        // catch-up version checks on the WS path.
        let hash = compute_blob_hash(&bytes).to_vec();
        let timestamp = kutl_core::env::now_ms();
        slot.set_blob(BlobData {
            content: bytes,
            hash,
            timestamp,
        });
        slot.edited_at_pending = Some(EditedAtPending {
            timestamp,
            author_did: did.clone(),
        });

        // Flush edited_at immediately (blobs are atomic — no debounce).
        self.flush_pending_edit_for(&key).await;

        // Notify observers + other MCP sessions so the change is visible.
        let intent = if is_new {
            "upload_blob"
        } else {
            "replace_blob"
        };
        self.send_mcp_notification(session_id, space_id, &actual_doc_id, &did, intent);
        self.observer.on_blob_edited(MergedEvent {
            via_pat_id,
            space_id: space_id.to_owned(),
            document_id: actual_doc_id.clone(),
            author_did: did,
            op_count: 1,
            intent: "blob".into(),
            content_mode: EditContentMode::Blob,
            timestamp,
        });

        Ok(McpUploadBlobResult {
            document_id: actual_doc_id,
            content_url: path.to_owned(),
        })
    }

    /// Apply leave-as-is-on-omit provenance merge to an existing
    /// registry entry. Only mutates fields the caller supplied. The
    /// persistence backend is best-effort updated when configured; the
    /// in-memory registry is updated regardless so subsequent reads see
    /// the new values.
    fn merge_blob_provenance(
        &mut self,
        space_id: &str,
        document_id: &str,
        provenance: &mcp_tools::ProvenanceArgs,
    ) {
        let Some(reg) = self.registries.get_mut(space_id) else {
            return;
        };
        let Some(entry) = reg.get_mut_any(document_id) else {
            return;
        };
        if provenance.source_kind.is_some() {
            entry.source_kind = provenance.source_kind;
        }
        if provenance.source_id.is_some() {
            entry.source_id.clone_from(&provenance.source_id);
        }
        if provenance.source_url.is_some() {
            entry.source_url.clone_from(&provenance.source_url);
        }
        if provenance.source_author_display.is_some() {
            entry
                .source_author_display
                .clone_from(&provenance.source_author_display);
        }
        if provenance.originally_created_at_ms.is_some() {
            entry.originally_created_at = provenance.originally_created_at_ms;
        }
        if provenance.ingestion_job_id.is_some() {
            entry
                .ingestion_job_id
                .clone_from(&provenance.ingestion_job_id);
        }
        // Edit-time provenance update: no UX-only mirror metadata to
        // attach — title/content_type/convert provenance land via the
        // original register path.
        //
        // This is a leave-as-is provenance merge for a doc already
        // registered; failure here is recoverable (next edit will
        // re-persist) and there is no in-memory mutation to roll back
        // (the registry has the merged values, but they're idempotent
        // — a later successful persist establishes durability). Log
        // and continue.
        if let Err(e) = persist_entry(
            self.registry_backend.as_deref(),
            &self.registries,
            space_id,
            document_id,
            &MirrorMetadata::default(),
        ) {
            error!(
                error = %e,
                doc_id = document_id,
                "failed to persist blob provenance merge"
            );
        }
    }

    /// MCP `list_spaces` handler — returns the set of spaces the caller
    /// is authorised for. On the OSS relay, authorisation is gated by
    /// the `authorized_keys` file per-space: the caller sees only the
    /// spaces its DID is authorised for right now (honouring the entry's
    /// `scope` and `expiry`) — a space-scoped or expired DID never
    /// enumerates a space it can't reach. Extension hosts (kutlhub-relay)
    /// override this command before it reaches the relay actor — out of
    /// scope here.
    pub(super) async fn handle_mcp_list_spaces(
        &mut self,
        session_id: &str,
    ) -> Result<Vec<McpSpaceSummary>, McpError> {
        let did = self.validate_mcp_session(session_id)?;

        // Membership-backed path (kutlhub): route through
        // `list_spaces_for_account`. The in-memory `self.registries`
        // map is populated lazily on subscribe, so a freshly-connected
        // agent would otherwise see an empty list — contradicting the
        // tool description's promise that this returns destinations
        // the caller is already authorized for. Authorization here is
        // by membership; when an `authorized_keys` file is also
        // configured it acts as a coarse presence gate (an unlisted DID
        // sees nothing).
        if let Some(membership) = self.membership_backend.as_ref().map(Arc::clone) {
            if let Some(ref ak) = self.authorized_keys
                && !ak.is_authorized(&did)
            {
                return Ok(Vec::new());
            }
            // Resolve DID → account through the one decision. A backend
            // ERROR is not a miss: it propagates as `Internal` so the caller
            // sees a retry-able message rather than an empty list (the same
            // rule as `mcp_check_space_registered`).
            let account_id = self
                .account_for_identity(&did)
                .await
                .map_err(|e| McpError::Internal(e.to_string()))?;
            let Some(account_id) = account_id else {
                // Unknown DID — no spaces visible. Avoid leaking
                // whether the DID exists by returning an empty list
                // rather than an error.
                return Ok(Vec::new());
            };
            let memberships = membership
                .list_spaces_for_account(&account_id)
                .await
                .map_err(|e| McpError::Internal(e.to_string()))?;
            return Ok(memberships
                .into_iter()
                .map(|m| McpSpaceSummary {
                    space_id: m.space_id,
                    slug: m.space_slug,
                    name: m.space_name,
                    owner_account_id: Some(m.owner_account_id),
                    owner_display_name: m.owner_display_name,
                })
                .collect());
        }

        // OSS in-memory fallback: enumerate spaces from the in-memory
        // registries. Use slug for space_id, slug, and name when no
        // separate label is tracked (the OSS in-process registry
        // doesn't carry one). No owner fields — OSS has no accounts
        // table.
        //
        // Filter each candidate space through `authorize` (honouring the
        // caller's `scope` and `expiry`) so a space-scoped or expired DID
        // never enumerates a space it can't reach. On this OSS fallback the
        // relay authorizes via `authorized_keys` (mandatory — the boot assert
        // guarantees an authz source), so every connected DID is authenticated.
        let now_ms = kutl_core::now_ms();
        let mut summaries: Vec<McpSpaceSummary> = self
            .registries
            .keys()
            .filter(|slug| {
                self.authorized_keys
                    .as_ref()
                    .is_none_or(|ak| ak.authorize(&did, slug, now_ms))
            })
            .map(|slug| McpSpaceSummary {
                space_id: slug.clone(),
                slug: slug.clone(),
                name: slug.clone(),
                owner_account_id: None,
                owner_display_name: None,
            })
            .collect();
        summaries.sort_by(|a, b| a.slug.cmp(&b.slug));
        Ok(summaries)
    }

    /// Create a flag signal via MCP — a RELAY-MINTED (tier-2) record.
    ///
    /// This and its siblings (`handle_mcp_create_reply`,
    /// `handle_mcp_close_flag`, `handle_mcp_reopen_flag`) author + attest on
    /// the caller's behalf. Relay-mint is the ONLY authoring
    /// model: key-holding actors (the CLI, `kutl mcp serve` with its `--agent`
    /// keyfile) do not sign records client-side — their keys only
    /// authenticate; there is no signed-CREATE / signed-transition
    /// route. This path also serves callers with no local key
    /// (a remote MCP agent hitting `/mcp` directly), marker materialization,
    /// and backfill.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_mcp_create_flag(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
        kind: i32,
        message: &str,
        audience: i32,
        target_did: &str,
        supplied_signal_id: Option<&str>,
        anchor_text: Option<&str>,
    ) -> Result<String, McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let author = self.authorize_mcp_caller(session_id, space_id).await?;

        // Same converter the WS door and the CLI-create door use, so this
        // surface cannot have its own opinion about which audiences are legal.
        let audience = kutl_proto::vocab::audience_from_untyped_checked(audience, Some(target_did))
            .map_err(|reason| McpError::RecordRejected { reason })?;

        let signal_id = self
            .relay_flag_signal(
                supplied_signal_id,
                space_id,
                Some(document_id),
                &author.did,
                kind,
                audience,
                message,
                anchor_text,
                kutl_core::env::now_ms(),
                author.via_pat_id.as_deref(),
            )
            .await
            // `from_change`, not a blanket Internal: a caller mistake must come
            // back as a caller mistake. Three are reachable here — an empty
            // message (`require_string` accepts ""), one over MAX_BODY_CHARS,
            // and a caller-supplied signal-id collision, which `create_comment`
            // makes reachable because it supplies its own id. An agent reads
            // "internal error" as transient and retries the unretryable.
            .map_err(McpError::from_change)?;

        Ok(signal_id)
    }

    pub(super) async fn handle_mcp_get_changes(
        &mut self,
        session_id: &str,
        space_id: &str,
        checkpoint: Option<&str>,
    ) -> Result<crate::change_backend::ChangesResponse, McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id: _ } =
            self.authorize_mcp_caller(session_id, space_id).await?;

        let Some(backend) = self.record_log.reads() else {
            return Ok(crate::change_backend::ChangesResponse::default());
        };

        // The cursor must be unique per agent. Authentication is mandatory,
        // so the DID is always a real, unique did:key — use it
        // directly as the cursor key.
        let cursor_key = &did;

        let mut changes = backend
            .get_changes(cursor_key, space_id, checkpoint)
            .await
            .map_err(McpError::from_change)?;
        // The caller's authenticated DID is the audience filter. It is never a
        // tool argument: asking on another participant's behalf would be
        // reading their mail, and asking on your own is what this already does.
        changes.retain_addressed_to(&did);
        Ok(changes)
    }

    /// List a space's signals, narrowed by the same filters `kutl signal list`
    /// offers.
    ///
    /// **Reads the FOLD, not the projection.** The projection is derived;
    /// listing from it would be a second path to a fact the log already owns,
    /// and the two could then disagree. Folding also makes the answer identical
    /// on a self-hosted relay, on kutlhub, and on a client reading its own
    /// segments — which is the parity being claimed. Filtering and summarizing
    /// are `kutl_signals::summary`, shared verbatim with the CLI, so the two
    /// surfaces cannot drift.
    ///
    /// Costs one fold of the space per call, the same O(records) the CLI pays
    /// locally. Fine for a listing verb; if a space ever grows past that, the
    /// answer is a paged cursor over the log, not a projection query.
    pub(super) async fn handle_mcp_list_signals(
        &mut self,
        session_id: &str,
        space_id: &str,
        status: &str,
        kind: Option<&str>,
        document_id: Option<&str>,
        flag_kind: Option<&str>,
    ) -> Result<Vec<kutl_signals::summary::SignalSummary>, McpError> {
        use kutl_signals::summary::{SignalFilters, SignalKind, StatusFilter};

        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let _author = self.authorize_mcp_caller(session_id, space_id).await?;

        let status = match status {
            "open" => StatusFilter::Open,
            "closed" => StatusFilter::Closed,
            "all" => StatusFilter::All,
            other => {
                return Err(McpError::RecordRejected {
                    reason: format!("invalid status {other:?}: one of open, closed, all"),
                });
            }
        };
        let kind = match kind {
            None => None,
            Some(k) => Some(
                SignalKind::from_label(k).ok_or_else(|| McpError::RecordRejected {
                    reason: format!("invalid kind {k:?}: one of flag, chat, decision, reply"),
                })?,
            ),
        };
        let flag_kind = match flag_kind {
            None => None,
            Some(k) => Some(kutl_proto::vocab::flag_kind_from_str(k).ok_or_else(|| {
                McpError::RecordRejected {
                    reason: format!(
                        "invalid flag_kind {k:?}: one of {}",
                        kutl_proto::vocab::flag_kind_names(kutl_proto::vocab::FLAG_KINDS)
                            .join(", ")
                    ),
                }
            })?),
        };

        // A log-less relay (a standalone test or sim actor) has no history to
        // list. An empty list, not an error: the route is reachable and the
        // honest answer is "no signals".
        if !self.record_log.is_configured() {
            return Ok(Vec::new());
        }
        let space_uuid = uuid::Uuid::parse_str(space_id).map_err(|_| McpError::NotAuthorized {
            space_id: space_id.to_owned(),
        })?;
        let records = self
            .record_log
            .load_space(space_uuid)
            .await
            .map_err(|e| McpError::Internal(e.to_string()))?;

        let mut fold = kutl_signals::fold::SpaceSignalState::default();
        for record in records {
            fold.apply(record);
        }
        Ok(kutl_signals::summary::list(
            &fold,
            &SignalFilters {
                status,
                kind,
                document_id,
                flag_kind,
            },
        ))
    }

    /// Create a reply to a signal via MCP.
    ///
    /// Generates a UUID and hands off to the shared reply door, which appends,
    /// projects, replicates to every signal subscriber, and announces.
    pub(super) async fn handle_mcp_create_reply(
        &mut self,
        session_id: &str,
        space_id: &str,
        parent_signal_id: &str,
        parent_reply_id: Option<&str>,
        body: &str,
    ) -> Result<String, McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id } =
            self.authorize_mcp_caller(session_id, space_id).await?;

        // Verify the parent signal actually lives in this space. Without this,
        // a member of space-A could reply to a parent signal in space-B (the
        // caller is only authorized for space-A). Mirrors the react/close/
        // reopen handlers, which all gate on `signal_is_in_space`.
        self.signal_is_in_space(space_id, parent_signal_id)
            .await
            .map_err(|e| signal_lookup_error(e, parent_signal_id))?;

        self.relay_reply_signal(
            space_id,
            parent_signal_id,
            parent_reply_id,
            body,
            &did,
            via_pat_id.as_deref(),
        )
        .await
        .map_err(McpError::from_change)
    }

    /// The lifecycle audit trail for `signal_id`, or empty when this relay
    /// keeps no records or cannot read them.
    ///
    /// Degrading to empty rather than erroring is deliberate: this decorates a
    /// detail read that has already succeeded, so a log problem should cost the
    /// caller the history, not the signal.
    pub(in crate::relay) async fn signal_transition_history(
        &mut self,
        space_id: &str,
        signal_id: &str,
    ) -> Vec<kutl_signals::summary::TransitionEntry> {
        if !self.record_log.is_configured() {
            return Vec::new();
        }
        let Ok(space_uuid) = uuid::Uuid::parse_str(space_id) else {
            return Vec::new();
        };
        // One signal's records, not the whole space: the trail folds only its
        // own history, and a database-backed log answers this from an index.
        match self.record_log.load_signal(space_uuid, signal_id).await {
            Ok(records) => kutl_signals::summary::transition_history(signal_id, &records),
            Err(e) => {
                warn!(
                    error = %e,
                    %space_id,
                    %signal_id,
                    "signal detail: could not load records for the transition trail"
                );
                Vec::new()
            }
        }
    }

    /// Add or remove a reaction on a signal via MCP.
    ///
    /// Fires the observer event for stream propagation. No direct DB write.
    pub(super) async fn handle_mcp_react_to_signal(
        &mut self,
        session_id: &str,
        space_id: &str,
        signal_id: &str,
        emoji: &str,
        remove: bool,
    ) -> Result<(), McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id } =
            self.authorize_mcp_caller(session_id, space_id).await?;
        self.signal_is_in_space(space_id, signal_id)
            .await
            .map_err(|e| signal_lookup_error(e, signal_id))?;

        self.observer.on_reaction(ReactionEvent {
            space_id: space_id.to_owned(),
            signal_id: signal_id.to_owned(),
            actor_did: did,
            via_pat_id,
            emoji: emoji.to_owned(),
            remove,
            timestamp: kutl_core::env::now_ms(),
        });

        Ok(())
    }

    /// Close a flag signal via MCP.
    ///
    /// Funnels into the shared transition path: a CLOSED record is minted and
    /// broadcast for author-born signals, while a decision is closed as the
    /// `? → =` marker flip in its document (the record follows via the
    /// materializer). Fires the observer event for stream propagation
    /// (kutlhub consumes it). When `reason` is `None`, defaults to
    /// `"resolved"`.
    pub(super) async fn handle_mcp_close_flag(
        &mut self,
        session_id: &str,
        space_id: &str,
        signal_id: &str,
        reason: Option<&str>,
        close_note: Option<&str>,
    ) -> Result<(), McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id } =
            self.authorize_mcp_caller(session_id, space_id).await?;
        self.signal_is_in_space(space_id, signal_id)
            .await
            .map_err(|e| signal_lookup_error(e, signal_id))?;

        let resolved_reason = reason.map_or_else(|| "resolved".to_owned(), str::to_owned);

        // Map the validated caller reason into the durable record's
        // close_reason, and carry the note on the record as well
        // — the projection reads it from there rather than from the
        // observer event, so it survives a rebuild from segments.
        self.emit_transition_record(
            signal_id,
            space_id,
            crate::relay::SignalTransitionEvent::Closed,
            &crate::relay::space_ops::TransitionAuthor {
                actor_did: &did,
                close_reason: Some(kutl_signals::payloads::close_reason_from_wire(
                    &resolved_reason,
                )),
                note: close_note,
                via_pat_id: via_pat_id.as_deref(),
            },
        )
        .await
        .map_err(McpError::from_change)?;

        // The announcement fires inside the admission seam, so every door
        // gets it.

        Ok(())
    }

    /// Reopen a previously closed flag signal via MCP.
    ///
    /// Funnels into the shared transition path: a REOPENED record is minted
    /// and broadcast for author-born signals, while a decision reopens as the
    /// `= → ?` marker flip in its document (the record follows via the
    /// materializer). Fires the observer event for stream propagation
    /// (kutlhub consumes it).
    pub(super) async fn handle_mcp_reopen_flag(
        &mut self,
        session_id: &str,
        space_id: &str,
        signal_id: &str,
    ) -> Result<(), McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id } =
            self.authorize_mcp_caller(session_id, space_id).await?;
        self.signal_is_in_space(space_id, signal_id)
            .await
            .map_err(|e| signal_lookup_error(e, signal_id))?;

        // Reopen carries no close_reason (REOPENED clears close-state) and no
        // note — a note explains a closure.
        self.emit_transition_record(
            signal_id,
            space_id,
            crate::relay::SignalTransitionEvent::Reopened,
            &crate::relay::space_ops::TransitionAuthor {
                actor_did: &did,
                close_reason: None,
                note: None,
                via_pat_id: via_pat_id.as_deref(),
            },
        )
        .await
        .map_err(McpError::from_change)?;

        // The announcement fires inside the admission seam, carrying the
        // same `record_id` the projection gave the lifecycle row so a consumer
        // addressing that row names what the relay wrote.

        Ok(())
    }

    /// Fetch full detail for a single signal via MCP.
    ///
    /// Uses the synchronous read path via the change backend. Returns an error
    /// if the backend is not configured or the signal does not exist.
    pub(super) async fn handle_mcp_get_signal_detail(
        &mut self,
        session_id: &str,
        space_id: &str,
        signal_id: &str,
    ) -> Result<crate::change_backend::SignalDetail, McpError> {
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        self.authorize_mcp_caller(session_id, space_id).await?;

        let Some(backend) = self.record_log.reads() else {
            return Err(McpError::Internal(
                "signal detail not available on this relay".into(),
            ));
        };

        let mut detail = backend
            .get_signal_detail(space_id, signal_id)
            .await
            .map_err(|e| signal_lookup_error(e, signal_id))?;

        // The audit trail comes from the LOG, not the projection: the
        // projection holds current state, and "how did it get here" is a
        // question only the records answer. Derived by the same shared code
        // `kutl signal view` uses, so the two surfaces show one history.
        // A log-less relay reports an empty trail rather than
        // a wrong one — and a load failure degrades the same way, because the
        // core detail is already in hand and losing it over the trail would be
        // the worse answer.
        detail.transitions = self.signal_transition_history(space_id, signal_id).await;
        Ok(detail)
    }

    pub(super) fn handle_mcp_register_notifications(
        &mut self,
        session_id: &str,
        tx: mpsc::Sender<String>,
    ) {
        if let Some(session) = self.mcp_sessions.get_mut(session_id) {
            session.notify_tx = Some(tx);
            info!(session_id, "MCP SSE notifications registered");
        }
    }

    pub(super) fn handle_reap_mcp_sessions(&mut self) {
        let before = self.mcp_sessions.len();
        self.mcp_sessions.retain(|id, session| {
            if session.last_active.elapsed() > MCP_SESSION_IDLE_TTL {
                info!(session_id = %id, did = %session.did, "MCP session reaped (idle)");
                false
            } else {
                true
            }
        });
        let reaped = before - self.mcp_sessions.len();
        if reaped > 0 {
            info!(
                reaped,
                remaining = self.mcp_sessions.len(),
                "MCP session reap complete"
            );
        }
    }

    /// Send a JSON-RPC document-changed notification to MCP sessions.
    ///
    /// `exclude_session_id` is skipped (use `""` to notify all sessions).
    /// Dead notification channels are cleaned up automatically.
    ///
    /// The params are built from [`crate::mcp::DocumentChangedParams`], the same
    /// type the consumer deserializes into, so the field names cannot drift
    /// apart without a build failing.
    fn send_mcp_notification(
        &mut self,
        exclude_session_id: &str,
        space_id: &str,
        document_id: &str,
        author_did: &str,
        intent: &str,
    ) {
        let params = crate::mcp::DocumentChangedParams {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            author_did: author_did.to_owned(),
            intent: intent.to_owned(),
        };
        let notification = crate::mcp::JsonRpcNotification {
            jsonrpc: "2.0".to_owned(),
            method: crate::mcp::DOC_CHANGED_METHOD.to_owned(),
            params: serde_json::to_value(params)
                .expect("document-changed params are plain strings"),
        };
        let json = serde_json::to_string(&notification)
            .expect("a JSON-RPC notification of plain strings serializes");

        let mut dead_notify_tx = Vec::new();

        // Every session but the author's, without regard to space: a session
        // is request/response identity, not a subscription, so the relay holds
        // nothing that says which spaces a session cares about. The frame
        // carries `space_id` for that reason — a consumer that acts on one
        // space must compare it before treating the frame as its own activity,
        // or it wakes on traffic its next read cannot return.
        for (id, session) in &self.mcp_sessions {
            if id == exclude_session_id {
                continue;
            }
            if let Some(tx) = &session.notify_tx
                && tx.try_send(json.clone()).is_err()
            {
                dead_notify_tx.push(id.clone());
            }
        }

        // A dead notify_tx means the SSE stream went away (proxy timeout,
        // client closed the GET stream, transient network drop). The
        // session itself remains valid for request/response — the
        // session-id is the identity, not the SSE channel. Clear the
        // dead tx so future broadcasts skip it without re-detecting,
        // but keep the session alive. The client can re-establish SSE
        // on the same session-id later. Idle TTL (20m) is the canonical
        // session-cleanup mechanism, not SSE-channel state.
        for id in dead_notify_tx {
            debug!(
                session_id = %id,
                "MCP SSE notify channel closed; clearing notify_tx, session kept"
            );
            if let Some(s) = self.mcp_sessions.get_mut(&id) {
                s.notify_tx = None;
            }
        }
    }

    /// Notify MCP sessions about a WS-originated edit.
    ///
    /// `authoritative_did` is the relay-authenticated identity of the sender
    /// and overrides any client-supplied `change.author_did` for every
    /// notification, so a forged inbound `author_did` never reaches observers.
    pub(super) fn notify_mcp_sessions_from_ws(
        &mut self,
        space_id: &str,
        document_id: &str,
        metadata: &[sync::ChangeMetadata],
        authoritative_did: &str,
    ) {
        if self.mcp_sessions.is_empty() {
            return;
        }

        for change in metadata {
            self.send_mcp_notification(
                "",
                space_id,
                document_id,
                authoritative_did,
                &change.intent,
            );
        }
    }
}

/// Resolve the text a writer says it read, or say why the version it named
/// cannot serve as a base.
///
/// Each way this fails is fixed differently, so each gets its own sentence
/// rather than one rejection the caller cannot act on.
fn resolve_base(
    doc: &kutl_core::Document,
    base_version: &str,
    space_id: &str,
    document_id: &str,
) -> Result<String, McpError> {
    let refuse = |reason: &str| McpError::UnusableBaseVersion {
        reason: reason.to_owned(),
    };

    let frontier =
        crate::doc_version::parse(base_version, space_id, document_id).map_err(|e| match e {
            crate::doc_version::TokenError::WrongDocument => refuse(BASE_WRONG_DOCUMENT),
            crate::doc_version::TokenError::Malformed => refuse(BASE_MALFORMED),
        })?;

    // Check the frontier's shape before anything uses it. A token is a hash of
    // identifiers the caller already knows, so any caller can build one naming
    // an impossible position, and the engine's own preconditions — strictly
    // ascending, every position inside the oplog — hold only under debug
    // assertions. This guard is what enforces them in a release build, where
    // an unchecked frontier reconstructs whatever the unvalidated path yields:
    // a panic at best, at worst a base the document never held, which the
    // merge would then express as the caller's edit. All three ways of
    // breaking the shape are equally reachable: a position past the end, a
    // descending pair, a repeat.
    let inside_the_log = frontier.iter().all(|&time| time < doc.op_count());
    let strictly_ascending = frontier.is_sorted_by(|earlier, later| earlier < later);
    if !inside_the_log || !strictly_ascending {
        return Err(refuse(BASE_NOT_THIS_HISTORY));
    }

    // Refuse a base so far behind that a clean merge would still be arguing
    // with settled work. Counted in changes rather than elapsed time: a quiet
    // document imposes no bound, a contested one tightens on its own, and
    // neither depends on a clock.
    let landed = doc.changes_since(&frontier).len();
    if landed > MAX_CHANGES_BEHIND {
        return Err(McpError::UnusableBaseVersion {
            reason: format!(
                "{landed} edits have landed since that version; read the document again and \
                 reapply your change"
            ),
        });
    }

    let base = doc.content_at(&frontier);
    let digest =
        crate::doc_version::base_digest(base_version).ok_or_else(|| refuse(BASE_MALFORMED))?;
    if !crate::doc_version::verify_base(digest, &base) {
        return Err(refuse(BASE_NOT_THIS_HISTORY));
    }
    Ok(base)
}

/// One sentence for a region a merge could not place: which region, why, and
/// what to do.
///
/// `excerpt` is the first line of the region that carries text, as the writer
/// last saw it, clipped to [`MAX_REFUSAL_EXCERPT_CHARS`]. It is empty only
/// when the whole region is blank, which names nothing a writer could go
/// looking for — hence the wording for that case.
fn describe_refusal(reason: kutl_core::HunkRefusal, excerpt: &str) -> String {
    let region = if excerpt.is_empty() {
        "a region of your edit".to_owned()
    } else {
        let mut clipped: String = excerpt.chars().take(MAX_REFUSAL_EXCERPT_CHARS).collect();
        if excerpt.chars().nth(MAX_REFUSAL_EXCERPT_CHARS).is_some() {
            clipped.push('…');
        }
        format!("the region starting \"{clipped}\"")
    };
    match reason {
        kutl_core::HunkRefusal::RegionChanged => format!(
            "{region} changed since you read it and was not applied; read the document again \
             and reapply that part"
        ),
        kutl_core::HunkRefusal::RegionAmbiguous => format!(
            "{region} now appears in more than one place, so applying it there would be a \
             guess and it was not applied; read the document again and reapply that part"
        ),
    }
}

/// Convert a `ChangeMetadata` proto to an `McpLogEntry`.
fn change_to_log_entry(c: &sync::ChangeMetadata) -> McpLogEntry {
    McpLogEntry {
        author_did: c.author_did.clone(),
        intent: c.intent.clone(),
        timestamp: c.timestamp,
        id: c.id.clone(),
        boundary: match Boundary::try_from(c.boundary) {
            Ok(Boundary::Explicit) => "explicit",
            Ok(Boundary::Auto) => "auto",
            _ => "unspecified",
        }
        .into(),
        full_rewrite: c.full_rewrite,
    }
}

/// Generate a random MCP session ID.
fn generate_session_id() -> String {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    /// Session ID random bytes.
    const SESSION_ID_SIZE: usize = 24;

    let bytes: Vec<u8> = (0..SESSION_ID_SIZE).map(|_| rand::random::<u8>()).collect();
    format!("mcp_{}", URL_SAFE_NO_PAD.encode(&bytes))
}

#[cfg(test)]
pub(in crate::relay) mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use kutl_proto::sync::{CloseReason, SignalEventType};

    use crate::blob_backend::BlobBackend;
    use crate::config::RelayConfig;
    use crate::membership_backend::{
        AcceptInvitationResult, MembershipBackend, SpaceMembershipInfo, SpaceParticipant,
        SpaceRecord,
    };

    use super::super::{TEST_CONN_DID, connect_client, resident_blob, subscribe_doc, test_config};
    use super::*;

    /// Pins the SSE-channel-close handling in `send_mcp_notification`.
    ///
    /// A dead `notify_tx` (receiver dropped — happens when the SSE GET
    /// stream closes) must clear the channel on the session while the
    /// session itself stays alive — the session-id is request/response
    /// identity, independent of the SSE channel state. Removing the
    /// whole session instead would break re-establishment. The client
    /// can re-establish SSE on the same session-id at any time; idle
    /// TTL (20 minutes) is the only canonical session-cleanup mechanism.
    #[tokio::test]
    async fn test_send_mcp_notification_preserves_session_on_dead_notify_tx() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);

        // Set up a session whose notify_tx is registered but whose
        // receiver was dropped (simulates the SSE GET stream closing
        // before the relay's writer task could notice).
        let dead_session_id = "session-with-dead-sse".to_owned();
        let (dead_tx, dead_rx) = mpsc::channel::<String>(8);
        drop(dead_rx); // sever — try_send will return Err(Closed)

        relay.mcp_sessions.insert(
            dead_session_id.clone(),
            McpSession {
                did: "did:key:zDeadSse".to_owned(),
                pat: None,
                notify_tx: Some(dead_tx),
                last_active: tokio::time::Instant::now(),
            },
        );

        // Healthy session as a control: live receiver, broadcast should
        // land in its buffer; session is preserved with notify_tx intact.
        let live_session_id = "session-with-live-sse".to_owned();
        let (live_tx, mut live_rx) = mpsc::channel::<String>(8);
        relay.mcp_sessions.insert(
            live_session_id.clone(),
            McpSession {
                did: "did:key:zLiveSse".to_owned(),
                pat: None,
                notify_tx: Some(live_tx),
                last_active: tokio::time::Instant::now(),
            },
        );

        // Trigger a broadcast. The exclude_session_id is some other
        // session not in our map, so both sessions are eligible to
        // receive the notification.
        relay.send_mcp_notification("other-session", "space-x", "doc-x", "did:author", "edit");

        // Dead session: still present in mcp_sessions; notify_tx now None.
        let dead = relay
            .mcp_sessions
            .get(&dead_session_id)
            .expect("dead-SSE session must NOT be removed — the session-id is the identity");
        assert!(
            dead.notify_tx.is_none(),
            "dead notify_tx should be cleared so future broadcasts skip it"
        );
        assert_eq!(
            dead.did, "did:key:zDeadSse",
            "session fields should be intact"
        );

        // Live session: still present, notify_tx still set, message delivered.
        let live = relay
            .mcp_sessions
            .get(&live_session_id)
            .expect("live session must be preserved");
        assert!(
            live.notify_tx.is_some(),
            "live notify_tx should not be cleared"
        );
        let delivered = live_rx
            .try_recv()
            .expect("notification should be delivered");
        assert!(
            delivered.contains("\"space_id\":\"space-x\""),
            "delivered payload should be the JSON-RPC notification: {delivered}"
        );
    }

    /// Pins the wire shape of the document-change notification.
    ///
    /// Consumers parse this frame off the SSE stream by method name and by
    /// these param names. [`crate::mcp::DocumentChangedParams`] keeps the two
    /// ends spelling them the same way; this keeps both ends spelling them the
    /// way relays already deployed do, which a rename of the struct's fields
    /// would otherwise change on both sides at once and break nothing here.
    #[tokio::test]
    async fn test_send_mcp_notification_frame_names_are_the_wire_contract() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let (tx, mut rx) = mpsc::channel::<String>(8);
        relay.mcp_sessions.insert(
            "listener".to_owned(),
            McpSession {
                did: "did:key:zListener".to_owned(),
                pat: None,
                notify_tx: Some(tx),
                last_active: tokio::time::Instant::now(),
            },
        );

        relay.send_mcp_notification("author", "space-x", "doc-y", "did:key:zAuthor", "edit");

        let frame: serde_json::Value =
            serde_json::from_str(&rx.try_recv().expect("the listening session is notified"))
                .expect("the frame is JSON");

        assert_eq!(frame["method"], "notifications/document/changed");
        let params = &frame["params"];
        assert_eq!(params["space_id"], "space-x");
        assert_eq!(params["document_id"], "doc-y");
        assert_eq!(params["author_did"], "did:key:zAuthor");
        assert_eq!(params["intent"], "edit");
    }

    // ---- Test helpers for MCP-path tests ----

    /// Register an MCP session so `validate_mcp_session` / `authorize_mcp_caller`
    /// succeed in tests. The session DID must be one of `test_config`'s
    /// authorized keys so the space authorization passes under mandatory auth.
    pub(in crate::relay) fn register_mcp_session(relay: &mut Relay, session_id: &str, did: &str) {
        relay.mcp_sessions.insert(
            session_id.to_owned(),
            McpSession {
                did: did.to_owned(),
                pat: None,
                notify_tx: None,
                last_active: tokio::time::Instant::now(),
            },
        );
    }

    /// Register a document in the in-memory registry so MCP edit's strict
    /// resolution (`resolve_existing_doc_id`) finds it.
    fn register_doc(relay: &mut Relay, space: &str, doc_id: &str, path: &str) {
        relay
            .registries
            .entry(space.to_owned())
            .or_default()
            .register(
                doc_id,
                path,
                crate::registry::EntryMetadata {
                    author_did: "did:test".to_owned(),
                    timestamp: 0,
                    ..Default::default()
                },
            );
    }

    // ---- MCP edit must not silently overwrite an evicted blob ----

    /// Blob backend backed by a single in-memory record.
    struct InMemoryBlobBackend {
        store: Mutex<HashMap<(String, String), crate::blob_backend::BlobRecord>>,
    }

    impl InMemoryBlobBackend {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::blob_backend::BlobBackend for InMemoryBlobBackend {
        async fn load(
            &self,
            space_id: &str,
            doc_id: &str,
        ) -> anyhow::Result<Option<crate::blob_backend::BlobRecord>> {
            Ok(self
                .store
                .lock()
                .unwrap()
                .get(&(space_id.to_owned(), doc_id.to_owned()))
                .cloned())
        }

        async fn save(
            &self,
            space_id: &str,
            doc_id: &str,
            blob: &crate::blob_backend::BlobRecord,
        ) -> anyhow::Result<()> {
            self.store
                .lock()
                .unwrap()
                .insert((space_id.to_owned(), doc_id.to_owned()), blob.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_mcp_edit_evicted_blob_refuses_overwrite() {
        // A blob lives only in the backend (no resident slot — i.e. evicted).
        // An MCP text edit must hydrate the slot first so the `NotTextDocument`
        // guard fires, rather than `or_insert_with(DocSlot::empty)` making a
        // fresh Empty slot and silently overwriting the binary blob.
        let blob_backend = InMemoryBlobBackend::new();
        let doc_id = uuid::Uuid::new_v4().to_string();
        blob_backend
            .save(
                "5171e0a1-1111-4000-8000-000000000001",
                &doc_id,
                &crate::blob_backend::BlobRecord {
                    data: b"\x89PNG-binary".to_vec(),
                    hash: b"png-hash".to_vec(),
                    timestamp: 100,
                },
            )
            .await
            .unwrap();

        let config = test_config();
        let mut relay = Relay::new_standalone_with_backend(
            config,
            None,
            None,
            Some(Arc::new(blob_backend)),
            None,
        );

        // Registry entry exists (post-evict: registry persists, slot doesn't).
        register_doc(
            &mut relay,
            "5171e0a1-1111-4000-8000-000000000001",
            &doc_id,
            "image.png",
        );
        // No resident slot — simulates eviction.
        assert!(!relay.has_document("5171e0a1-1111-4000-8000-000000000001", &doc_id));

        register_mcp_session(&mut relay, "sess-1", "did:agent");

        let result = relay
            .handle_mcp_edit_document(
                "sess-1",
                "5171e0a1-1111-4000-8000-000000000001",
                &doc_id,
                // Unusable on purpose: the blob guard must fire before the
                // base is ever examined.
                "kv1.not-a-token",
                "this is text that must NOT clobber the blob",
                "edit",
                "",
            )
            .await;

        assert!(
            matches!(result, Err(McpError::NotTextDocument)),
            "editing an evicted blob via MCP must be refused, got {result:?}"
        );

        // The resident slot (now hydrated) must still be the blob, unmodified.
        assert_eq!(
            resident_blob(&relay, "5171e0a1-1111-4000-8000-000000000001", &doc_id).as_deref(),
            Some(b"\x89PNG-binary".as_slice()),
            "blob content must be untouched after a refused edit"
        );
    }

    // ---- MCP create_reply must verify the parent signal is in-space ----

    /// Minimal change backend that only answers `get_signal_detail`, keyed by
    /// `(space_id, signal_id)`. A miss yields `NotFound`, matching production.
    struct InMemorySignalBackend {
        signals: Mutex<HashMap<(String, String), crate::change_backend::SignalDetail>>,
        /// What `get_changes` hands back, verbatim and unscoped — the shape
        /// every real backend produces, since the audience filter is the
        /// handler's job rather than each substrate's.
        changes: Mutex<Vec<sync::Signal>>,
    }

    impl InMemorySignalBackend {
        fn new() -> Self {
            Self {
                signals: Mutex::new(HashMap::new()),
                changes: Mutex::new(Vec::new()),
            }
        }

        /// Seed the records `get_changes` will return.
        fn with_changes(self, records: Vec<sync::Signal>) -> Self {
            *self.changes.lock().unwrap() = records;
            self
        }

        fn insert_signal(&self, space: &str, id: &str) {
            self.signals.lock().unwrap().insert(
                (space.to_owned(), id.to_owned()),
                crate::change_backend::SignalDetail {
                    id: id.to_owned(),
                    space_id: space.to_owned(),
                    document_id: None,
                    author_did: "did:test".to_owned(),
                    signal_type: "flag".to_owned(),
                    timestamp: 0,
                    flag_kind: None,
                    audience: None,
                    target_did: None,
                    message: None,
                    anchor_text: None,
                    closed_at: None,
                    parent_signal_id: None,
                    parent_reply_id: None,
                    body: None,
                    replies: vec![],
                    reactions: vec![],
                    // Filled by the detail handler from the record log, not the
                    // projection — see `signal_transition_history`.
                    transitions: Vec::new(),
                },
            );
        }
    }

    #[async_trait::async_trait]
    impl crate::change_backend::ProjectionWriter for InMemorySignalBackend {
        async fn project_record(
            &self,
            space_id: &str,
            record: &sync::Signal,
            _via_pat_id: Option<&str>,
        ) -> Result<(), crate::change_backend::ChangeError> {
            // Projects for real: the authored
            // seam asks `signal_exists` on every create, so a mock that
            // never recorded one would report every signal absent and let a
            // genuine collision through in tests.
            if matches!(
                record.event(),
                sync::SignalEventType::Created | sync::SignalEventType::Unspecified
            ) {
                // Keyed to the ADMITTING space, matching the real backends.
                self.insert_signal(space_id, &record.id);
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl crate::change_backend::ChangeBackend for InMemorySignalBackend {
        /// No fold to re-derive from — this map IS the projection, maintained
        /// incrementally by `project_record`.
        fn rebuild(&self) -> Option<&dyn crate::change_backend::ProjectionRebuild> {
            None
        }

        async fn get_changes(
            &self,
            _did: &str,
            _space_id: &str,
            _checkpoint: Option<&str>,
        ) -> Result<crate::change_backend::ChangesResponse, crate::change_backend::ChangeError>
        {
            Ok(crate::change_backend::ChangesResponse {
                signals: self.changes.lock().unwrap().clone(),
                ..Default::default()
            })
        }

        async fn signal_exists(
            &self,
            space_id: &str,
            signal_id: &str,
        ) -> Result<bool, crate::change_backend::ChangeError> {
            Ok(self
                .signals
                .lock()
                .unwrap()
                .contains_key(&(space_id.to_owned(), signal_id.to_owned())))
        }

        async fn get_signal_detail(
            &self,
            space_id: &str,
            signal_id: &str,
        ) -> Result<crate::change_backend::SignalDetail, crate::change_backend::ChangeError>
        {
            self.signals
                .lock()
                .unwrap()
                .get(&(space_id.to_owned(), signal_id.to_owned()))
                .cloned()
                .ok_or_else(|| crate::change_backend::ChangeError::NotFound(signal_id.to_owned()))
        }

        async fn prune(&self, _now_ms: i64) -> Result<u64, crate::change_backend::ChangeError> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn test_mcp_create_reply_rejects_cross_space_parent() {
        // The parent signal lives in space-B. A caller authorized for space-A
        // must NOT be able to reply to it.
        let backend = InMemorySignalBackend::new();
        let parent_id = uuid::Uuid::new_v4().to_string();
        backend.insert_signal("5171e0a1-2222-4000-8000-00000000000b", &parent_id);

        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        relay.record_log =
            crate::relay::signal_log::SignalLogHandle::new(None, Some(Arc::new(backend)));
        register_mcp_session(&mut relay, "sess-1", "did:agent");

        // Reply in space-A referencing the space-B parent — must be rejected.
        let result = relay
            .handle_mcp_create_reply(
                "sess-1",
                "5171e0a1-2222-4000-8000-00000000000a",
                &parent_id,
                None,
                "body",
            )
            .await;
        assert!(
            matches!(result, Err(McpError::SignalNotFound { .. })),
            "cross-space reply parent must be rejected, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_mcp_create_reply_accepts_same_space_parent() {
        // Sanity check: a reply to a parent in the caller's own space succeeds.
        let backend = InMemorySignalBackend::new();
        let parent_id = uuid::Uuid::new_v4().to_string();
        backend.insert_signal("5171e0a1-2222-4000-8000-00000000000a", &parent_id);

        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        relay.record_log =
            crate::relay::signal_log::SignalLogHandle::new(None, Some(Arc::new(backend)));
        register_mcp_session(&mut relay, "sess-1", "did:agent");

        let result = relay
            .handle_mcp_create_reply(
                "sess-1",
                "5171e0a1-2222-4000-8000-00000000000a",
                &parent_id,
                None,
                "body",
            )
            .await;
        assert!(
            result.is_ok(),
            "same-space reply parent must succeed, got {result:?}"
        );
    }

    // ---- list_participants reports the actor set, not the attendance ----

    /// Everyone authorized is listed whether present or not, presence rides as
    /// a field, and a live session outside the actor set never joins it.
    ///
    /// The last one is the cross-space leak: MCP sessions are not space-scoped,
    /// so a session must only MARK an actor as present, never add one.
    #[tokio::test]
    async fn test_list_participants_lists_absent_actors_and_admits_no_outsider() {
        const SPACE: &str = "5171e0a1-3333-4000-8000-00000000000b";
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        register_mcp_session(&mut relay, "sess-1", TEST_CONN_DID);
        // A session for a DID the actor set does not contain — the shape a
        // foreign space's session arrives in.
        register_mcp_session(&mut relay, "sess-outsider", "did:key:zStranger");

        let listed = relay
            .handle_mcp_list_participants("sess-1", SPACE)
            .await
            .expect("an authorized caller may list");

        let did_of = |d: &str| listed.iter().find(|p| p.did == d).cloned();

        // The caller's own DID is an actor here and is connected over MCP.
        let me = did_of(TEST_CONN_DID).expect("the caller is an actor of this space");
        assert_eq!(me.connection_type, "mcp", "a live session marks presence");

        // Another authorized DID with no connection is still listed — the whole
        // point, since you address someone because they are away.
        let absent = did_of("did:key:zAlice").expect("an authorized DID is an actor when away");
        assert_eq!(absent.connection_type, "offline");

        // `did:key:zStranger` is deliberately absent from the authorized list,
        // so its live session must not put it in this space's roster.
        assert!(
            did_of("did:key:zStranger").is_none(),
            "a session outside the actor set must not be listed: {listed:?}"
        );

        // DIDs are unique across the response.
        let mut dids: Vec<&str> = listed.iter().map(|p| p.did.as_str()).collect();
        let before = dids.len();
        dids.sort_unstable();
        dids.dedup();
        assert_eq!(dids.len(), before, "DIDs must not repeat: {listed:?}");
    }

    /// Presence over websocket is listening: a connection that subscribed to
    /// the space's signals marks its DID `websocket` (outranking a live MCP
    /// session for the same DID), and a connection that only subscribed to a
    /// document is not here at all.
    #[tokio::test]
    async fn test_list_participants_marks_listeners_websocket_and_bare_subscribers_offline() {
        const SPACE: &str = "5171e0a1-3333-4000-8000-00000000000b";
        const DOC: &str = "0f0f0f0f-3333-4000-8000-000000000001";
        let mut relay = Relay::new_standalone(test_config());
        register_mcp_session(&mut relay, "sess-1", TEST_CONN_DID);
        // The caller also listens over websocket.
        let (_l_data, _l_ack, _l_ctrl) = connect_client(&mut relay, 1).await;
        relay
            .handle_subscribe_signals(
                1,
                &sync::SubscribeSignals {
                    space_id: SPACE.into(),
                    cursor: None,
                },
            )
            .await;
        // zAlice holds a bare document subscription and nothing else.
        let (_s_data, _s_ack, _s_ctrl) = connect_client(&mut relay, 2).await;
        relay.test_set_authenticated(2, "did:key:zAlice");
        subscribe_doc(&mut relay, 2, SPACE, DOC).await;

        let listed = relay
            .handle_mcp_list_participants("sess-1", SPACE)
            .await
            .expect("an authorized caller may list");
        let did_of = |d: &str| listed.iter().find(|p| p.did == d).cloned();

        assert_eq!(
            did_of(TEST_CONN_DID)
                .expect("the caller is an actor")
                .connection_type,
            "websocket",
            "a listening connection is presence over websocket, whatever else the DID holds"
        );
        assert_eq!(
            did_of("did:key:zAlice")
                .expect("zAlice is an actor")
                .connection_type,
            "offline",
            "a bare document subscription is not presence"
        );
    }

    // ---- the membership branch: same actor-set and canonical-name rules ----

    /// A membership backend whose roster is exactly the participants given
    /// at construction. Only the methods the membership branch of the
    /// handlers under test actually reaches (`check_membership`,
    /// `resolve_did_to_account` — both via `authorize_mcp_caller` —  and
    /// `list_space_participants`) are implemented; every other trait method
    /// is unreachable from these tests by construction.
    struct StubMembership {
        participants: Vec<(String, String)>,
    }

    #[async_trait]
    impl MembershipBackend for StubMembership {
        async fn check_membership(
            &self,
            _space_id: &str,
            _account_id: &str,
        ) -> anyhow::Result<Option<String>> {
            Ok(Some("member".to_owned()))
        }

        async fn resolve_did_to_account(&self, did: &str) -> anyhow::Result<Option<String>> {
            // Any authenticated DID is its own account for these tests —
            // `check_membership` above accepts every account unconditionally,
            // so the exact mapping doesn't matter, only that one exists.
            Ok(Some(did.to_owned()))
        }

        async fn resolve_account_to_did(
            &self,
            _account_id: &str,
        ) -> anyhow::Result<Option<String>> {
            unreachable!("not exercised")
        }

        async fn resolve_space_by_slugs(
            &self,
            _owner_slug: &str,
            _space_slug: &str,
        ) -> anyhow::Result<Option<SpaceRecord>> {
            unreachable!("not exercised")
        }

        async fn list_spaces_for_account(
            &self,
            _account_id: &str,
        ) -> anyhow::Result<Vec<SpaceMembershipInfo>> {
            unreachable!("not exercised")
        }

        async fn accept_invitation(
            &self,
            _code: &str,
            _account_id: &str,
        ) -> anyhow::Result<AcceptInvitationResult> {
            unreachable!("not exercised")
        }

        async fn list_space_participants(
            &self,
            _space_id: &str,
        ) -> anyhow::Result<Vec<SpaceParticipant>> {
            Ok(self
                .participants
                .iter()
                .map(|(did, name)| SpaceParticipant {
                    did: did.clone(),
                    name: name.clone(),
                })
                .collect())
        }
    }

    /// A standalone relay authorized by membership alone: no
    /// `authorized_keys` file, so the handlers under test must take their
    /// membership branch rather than falling back to the file-based one.
    fn membership_relay(participants: Vec<(&str, &str)>) -> Relay {
        let mut relay = Relay::new_standalone(RelayConfig {
            port: 0,
            ..Default::default()
        });
        relay.test_set_membership_backend(Arc::new(StubMembership {
            participants: participants
                .into_iter()
                .map(|(did, name)| (did.to_owned(), name.to_owned()))
                .collect(),
        }));
        relay
    }

    /// The membership branch reports the same actor set — present or not,
    /// no outsider — that the `authorized_keys` branch does; only the roster
    /// source changed.
    #[tokio::test]
    async fn test_membership_list_participants_is_the_actor_set() {
        const SPACE: &str = "5171e0a1-4444-4000-8000-00000000000c";
        let mut relay = membership_relay(vec![
            ("did:key:zAway", "boris"),
            ("did:key:zBot", "boris/helper"),
        ]);
        register_mcp_session(&mut relay, "sess-1", "did:key:zBot");
        // A live session for a DID the roster does not contain — the shape a
        // foreign space's session arrives in.
        register_mcp_session(&mut relay, "sess-outsider", "did:key:zStranger");

        let listed = relay
            .handle_mcp_list_participants("sess-1", SPACE)
            .await
            .expect("a membership-authorized caller may list");

        let did_of = |d: &str| listed.iter().find(|p| p.did == d).cloned();

        let bot = did_of("did:key:zBot").expect("zBot is in the roster");
        assert_eq!(bot.connection_type, "mcp", "a live session marks presence");
        assert_eq!(bot.name.as_deref(), Some("boris/helper"));

        let away = did_of("did:key:zAway").expect("zAway is in the roster though offline");
        assert_eq!(away.connection_type, "offline");

        assert!(
            did_of("did:key:zStranger").is_none(),
            "a live session outside the roster must not be listed: {listed:?}"
        );
    }

    /// A suffix of a `/`-joined path resolves to its participant, and the
    /// returned name is the full stored path, not the short suffix typed.
    #[tokio::test]
    async fn test_membership_resolve_participant_suffix_and_canonical_name() {
        const SPACE: &str = "5171e0a1-4444-4000-8000-00000000000c";
        let mut relay = membership_relay(vec![
            ("did:key:zBot", "boris/helper"),
            ("did:key:zCfo", "boris/cfo"),
        ]);
        register_mcp_session(&mut relay, "sess-1", "did:key:zBot");

        let resolved = relay
            .handle_mcp_resolve_participant("sess-1", SPACE, "helper")
            .await
            .expect("a membership-authorized caller may resolve");

        assert_eq!(
            resolved.iter().map(|p| p.did.as_str()).collect::<Vec<_>>(),
            vec!["did:key:zBot"],
            "the suffix names exactly the one participant: {resolved:?}"
        );
        assert_eq!(
            resolved[0].name, "boris/helper",
            "the resolved name is the full stored path, not the typed suffix"
        );
    }

    /// Deliberate behavior change on the `authorized_keys` branch: the
    /// returned name used to echo the caller's query; it now returns the
    /// canonical stored name, so an ambiguity error built from several of
    /// these lists fully-qualified candidates instead of repeating the short
    /// query the caller already knows it sent.
    #[tokio::test]
    async fn test_resolve_participant_authorized_keys_branch_returns_canonical_name() {
        const SPACE: &str = "5171e0a1-5555-4000-8000-00000000000d";
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        let unique = format!(
            "kutl-relay-test-canonical-name-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let keys_path = std::env::temp_dir().join(unique);
        std::fs::write(&keys_path, "did:key:zAlice name=alice\n")
            .expect("write test authorized_keys file");

        let mut relay = Relay::new_standalone(RelayConfig {
            port: 0,
            authorized_keys_file: Some(keys_path),
            ..Default::default()
        });
        register_mcp_session(&mut relay, "sess-1", "did:key:zAlice");

        // Typed with different case than stored — proves the match still
        // works AND that the echoed shape is gone: a pre-change caller would
        // see "Alice" (its own query) come back.
        let resolved = relay
            .handle_mcp_resolve_participant("sess-1", SPACE, "Alice")
            .await
            .expect("an authorized caller may resolve");

        assert_eq!(resolved.len(), 1, "exactly one candidate: {resolved:?}");
        assert_eq!(
            resolved[0].name, "alice",
            "returns the canonical stored name, not the query as typed"
        );
    }

    // ---- get_changes delivers only what the caller is addressed by ----

    /// The space every scoping test below runs in.
    const SCOPE_SPACE: &str = "5171e0a1-2222-4000-8000-00000000000a";

    /// A flag CREATED record carrying `audience`.
    fn scoped_flag(id: &str, audience: kutl_proto::sync::Audience) -> sync::Signal {
        let mut record = sync::Signal {
            id: id.to_owned(),
            space_id: SCOPE_SPACE.to_owned(),
            record_id: format!("rec-{id}"),
            author_did: "did:key:zAuthor".to_owned(),
            timestamp: 1,
            payload: Some(sync::signal::Payload::Flag(sync::FlagPayload {
                kind: i32::from(sync::FlagKind::Question),
                message: format!("message for {id}"),
                audience: Some(audience),
                ..Default::default()
            })),
            ..Default::default()
        };
        record.set_event(sync::SignalEventType::Created);
        record
    }

    /// A standalone relay whose change backend hands back `records` unscoped.
    fn relay_with_changes(records: Vec<sync::Signal>) -> Relay {
        let backend = InMemorySignalBackend::new().with_changes(records);
        let mut relay = Relay::new_standalone(test_config());
        relay.record_log =
            crate::relay::signal_log::SignalLogHandle::new(None, Some(Arc::new(backend)));
        relay
    }

    /// The signal ids `did` receives through the MCP door.
    async fn changes_delivered_to(did: &str, records: Vec<sync::Signal>) -> Vec<String> {
        let mut relay = relay_with_changes(records);
        register_mcp_session(&mut relay, "sess-1", did);

        signal_ids(
            relay
                .handle_mcp_get_changes("sess-1", SCOPE_SPACE, None)
                .await,
        )
    }

    /// The signal ids `did` receives through the HTTP door — the route behind
    /// `kutl space feed` and the CLI client's catch-up, which resolves a bearer
    /// token to a DID instead of carrying an MCP session.
    ///
    /// A separate door with its own authorization path, so it needs its own
    /// coverage: the MCP tests above pass whether or not this one filters.
    async fn feed_delivered_to(did: &str, records: Vec<sync::Signal>) -> Vec<String> {
        let mut relay = relay_with_changes(records);
        signal_ids(relay.handle_get_changes(did, SCOPE_SPACE, None).await)
    }

    fn signal_ids(result: Result<crate::change_backend::ChangesResponse, McpError>) -> Vec<String> {
        result
            .expect("get_changes succeeds")
            .signals
            .into_iter()
            .map(|s| s.id)
            .collect()
    }

    /// Two participants authorized in this relay's test keys.
    const ALICE: &str = "did:key:zAlice";
    const BOB: &str = "did:key:zBob";

    /// One flag for alice, one for bob, one for the space — the fixture that
    /// distinguishes "addressed to me", "addressed to someone else", and
    /// "addressed to everyone" in a single page.
    fn addressed_trio() -> Vec<sync::Signal> {
        use kutl_proto::vocab::{participant_audience, space_audience};

        vec![
            scoped_flag("for-alice", participant_audience(ALICE)),
            scoped_flag("for-bob", participant_audience(BOB)),
            scoped_flag("for-everyone", space_audience()),
        ]
    }

    /// A flag naming one participant reaches that participant and nobody else,
    /// and a space-addressed flag reaches everyone. The caller's authenticated
    /// DID is the whole filter — there is no argument for asking on another
    /// participant's behalf.
    #[tokio::test]
    async fn test_get_changes_delivers_only_signals_addressed_to_the_caller() {
        // Each block constructs and drops its own relay, so the two
        // scenarios share no state.
        {
            let ids = changes_delivered_to(ALICE, addressed_trio()).await;
            assert_eq!(
                ids,
                vec!["for-alice", "for-everyone"],
                "alice gets her own flag and the broadcast, never the one naming bob"
            );
        }
        {
            let ids = changes_delivered_to(BOB, addressed_trio()).await;
            assert_eq!(
                ids,
                vec!["for-bob", "for-everyone"],
                "bob gets his own flag and the broadcast, never the one naming alice"
            );
        }
    }

    /// The HTTP door scopes exactly as the MCP door does.
    ///
    /// Its own test rather than a note on the one above: the two doors reach
    /// the backend by different paths with different authorization, and
    /// deleting the filter from this one leaves every MCP test green.
    #[tokio::test]
    async fn test_feed_door_delivers_only_signals_addressed_to_the_caller() {
        // As in the MCP-door test above: one relay per block, no shared
        // state between scenarios.
        {
            let ids = feed_delivered_to(ALICE, addressed_trio()).await;
            assert_eq!(
                ids,
                vec!["for-alice", "for-everyone"],
                "the feed door must withhold the flag naming bob"
            );
        }
        {
            let ids = feed_delivered_to(BOB, addressed_trio()).await;
            assert_eq!(
                ids,
                vec!["for-bob", "for-everyone"],
                "and the flag naming alice"
            );
        }
    }

    /// A flag whose addressing is malformed reaches nobody. Widening it to
    /// space-wide is the repair a stored row needs and the wrong answer for
    /// delivery, where it would turn an unaddressed flag into one every
    /// participant is handed.
    #[tokio::test]
    async fn test_get_changes_drops_a_flag_addressed_to_nobody() {
        // A participant audience carrying an empty DID addresses no one. The
        // authoring seam refuses to mint this shape, so it arrives by
        // replication rather than from a local caller.
        let undeliverable = scoped_flag("malformed", kutl_proto::vocab::participant_audience(""));

        let ids = changes_delivered_to(ALICE, vec![undeliverable]).await;
        assert!(
            ids.is_empty(),
            "a flag addressed to nobody must not be delivered to anybody"
        );
    }

    /// Only a flag carries an audience. A reply, decision, or chat names no
    /// one, so the filter must pass it through rather than read its absent
    /// audience as "addressed to someone else" and drop every reply in the
    /// space.
    #[tokio::test]
    async fn test_get_changes_keeps_records_that_carry_no_audience() {
        let mut reply = scoped_flag("a-reply", kutl_proto::vocab::space_audience());
        reply.payload = Some(sync::signal::Payload::Reply(sync::ReplyPayload {
            parent_signal_id: "for-alice".to_owned(),
            body: "answering".to_owned(),
            ..Default::default()
        }));

        let ids = changes_delivered_to(ALICE, vec![reply]).await;
        assert_eq!(
            ids,
            vec!["a-reply"],
            "a reply carries no audience and must survive the filter"
        );
    }

    /// The reply's segment record carries its `ReplyPayload` so that when the
    /// projection is derived by folding segments, the parent linkage and body
    /// survive — a reply must be reconstructable from its segment record alone.
    #[tokio::test]
    async fn test_mcp_create_reply_segment_record_carries_reply_payload() {
        let space_uuid = uuid::Uuid::new_v4();
        let space_id = space_uuid.to_string();
        let parent_id = uuid::Uuid::new_v4().to_string();

        let backend = InMemorySignalBackend::new();
        backend.insert_signal(&space_id, &parent_id);

        let signals_dir = tempfile::TempDir::new().unwrap();
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        relay.test_set_record_log(
            Some(Arc::new(crate::record_log::SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(signals_dir.path().to_path_buf()),
            ))),
            Some(Arc::new(backend)),
        );
        register_mcp_session(&mut relay, "sess-1", "did:agent");

        relay
            .handle_mcp_create_reply("sess-1", &space_id, &parent_id, None, "hello body")
            .await
            .expect("reply must succeed");

        // Load the space's segment and inspect the appended record.
        let records = relay
            .record_log
            .load_space(space_uuid)
            .await
            .expect("record load");
        assert_eq!(records.len(), 1, "one reply record appended");
        let rec = &records[0];
        assert_eq!(rec.event(), SignalEventType::Created);
        assert_eq!(rec.author_did, "did:agent");

        let payload = rec
            .payload
            .as_ref()
            .expect("reply record must carry a payload");
        let sync::signal::Payload::Reply(reply) = payload else {
            panic!("expected a Reply payload, got {payload:?}");
        };
        assert_eq!(reply.parent_signal_id, parent_id);
        assert_eq!(reply.parent_reply_id, None);
        assert_eq!(reply.body, "hello body");
    }

    /// `get_signal_detail` returns the lifecycle trail alongside core, replies
    /// and reactions — the unified detail read. The trail comes
    /// from the LOG, so it shows every transition, not just the winning one the
    /// projection reflects.
    #[tokio::test]
    async fn test_mcp_get_signal_detail_carries_the_transition_trail() {
        let space_uuid = uuid::Uuid::new_v4();
        let space_id = space_uuid.to_string();

        let signals_dir = tempfile::TempDir::new().unwrap();
        let backend = Arc::new(InMemorySignalBackend::new());
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        relay.test_set_record_log(
            Some(Arc::new(crate::record_log::SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(signals_dir.path().to_path_buf()),
            ))),
            Some(backend.clone()),
        );
        register_mcp_session(&mut relay, "sess-1", "did:agent");

        let doc = uuid::Uuid::new_v4().to_string();
        let signal_id = relay
            .handle_mcp_create_flag(
                "sess-1",
                &space_id,
                &doc,
                i32::from(sync::FlagKind::Question),
                "needs an answer",
                i32::from(sync::AudienceType::Space),
                "",
                None,
                None,
            )
            .await
            .expect("create the flag");
        // The in-memory backend does not project, so seed the header the
        // detail read joins against; the TRAIL is what is under test.
        backend.insert_signal(&space_id, &signal_id);

        relay
            .handle_mcp_close_flag(
                "sess-1",
                &space_id,
                &signal_id,
                Some("declined"),
                Some("no"),
            )
            .await
            .expect("close it");
        relay
            .handle_mcp_reopen_flag("sess-1", &space_id, &signal_id)
            .await
            .expect("reopen it");

        let detail = relay
            .handle_mcp_get_signal_detail("sess-1", &space_id, &signal_id)
            .await
            .expect("detail read");

        assert_eq!(
            detail
                .transitions
                .iter()
                .map(|t| t.event.as_str())
                .collect::<Vec<_>>(),
            vec!["created", "closed", "reopened"],
            "the trail must show every transition, oldest first"
        );
        let closed = &detail.transitions[1];
        assert_eq!(closed.close_reason.as_deref(), Some("declined"));
        assert_eq!(closed.note.as_deref(), Some("no"));
    }

    /// `list_signals` reads the FOLD, so a closed signal leaves the default
    /// listing and reappears under `all` — and the filters narrow the way the
    /// CLI's do, because both call the same `kutl_signals::summary` code.
    #[tokio::test]
    async fn test_mcp_list_signals_reflects_the_fold_and_filters() {
        let space_uuid = uuid::Uuid::new_v4();
        let space_id = space_uuid.to_string();

        let signals_dir = tempfile::TempDir::new().unwrap();
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        relay.test_set_record_log(
            Some(Arc::new(crate::record_log::SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(signals_dir.path().to_path_buf()),
            ))),
            Some(Arc::new(InMemorySignalBackend::new())),
        );
        register_mcp_session(&mut relay, "sess-1", "did:agent");

        let doc = uuid::Uuid::new_v4().to_string();
        let question = relay
            .handle_mcp_create_flag(
                "sess-1",
                &space_id,
                &doc,
                i32::from(sync::FlagKind::Question),
                "needs an answer",
                i32::from(sync::AudienceType::Space),
                "",
                None,
                None,
            )
            .await
            .expect("create the question flag");
        let blocker = relay
            .handle_mcp_create_flag(
                "sess-1",
                &space_id,
                &doc,
                i32::from(sync::FlagKind::Blocked),
                "cannot proceed",
                i32::from(sync::AudienceType::Space),
                "",
                None,
                None,
            )
            .await
            .expect("create the blocked flag");

        let ids = |v: Vec<kutl_signals::summary::SignalSummary>| -> Vec<String> {
            v.into_iter().map(|s| s.id).collect()
        };

        // Both are open, and the flag-kind filter picks exactly one.
        let mut open = ids(relay
            .handle_mcp_list_signals("sess-1", &space_id, "open", None, None, None)
            .await
            .expect("list open"));
        open.sort();
        let mut expected = vec![question.clone(), blocker.clone()];
        expected.sort();
        assert_eq!(open, expected);

        assert_eq!(
            ids(relay
                .handle_mcp_list_signals("sess-1", &space_id, "open", None, None, Some("question"))
                .await
                .expect("list questions")),
            vec![question.clone()]
        );

        // Close one: it leaves the default listing and shows up under `all`.
        relay
            .handle_mcp_close_flag("sess-1", &space_id, &blocker, Some("resolved"), None)
            .await
            .expect("close the blocker");

        assert_eq!(
            ids(relay
                .handle_mcp_list_signals("sess-1", &space_id, "open", None, None, None)
                .await
                .expect("list open after close")),
            vec![question.clone()],
            "a closed signal must leave the default listing"
        );
        assert_eq!(
            ids(relay
                .handle_mcp_list_signals("sess-1", &space_id, "closed", None, None, None)
                .await
                .expect("list closed")),
            vec![blocker],
            "and be reachable under `closed`"
        );

        // An unknown filter value is a caller error, not a silent empty list.
        assert!(
            relay
                .handle_mcp_list_signals("sess-1", &space_id, "everything", None, None, None)
                .await
                .is_err(),
            "an invalid status must be rejected rather than coerced"
        );
    }

    /// close/reopen append CLOSED/REOPENED records; the fold reflects the
    /// transitions (OSS close-state persistence).
    ///
    /// Verifies two paths:
    /// 1. create → close → fold ⇒ Closed
    /// 2. create → close → reopen → fold ⇒ Open
    ///
    /// The monotonic HLC guarantees create < close < reopen even within one
    /// millisecond, so this test is not flaky.
    #[tokio::test]
    async fn test_close_reopen_persist_as_records() {
        use kutl_signals::fold::{SignalStatus, SpaceSignalState};

        let space_uuid = uuid::Uuid::new_v4();
        let space_id = space_uuid.to_string();
        let signal_id = uuid::Uuid::new_v4().to_string();
        let doc_id = uuid::Uuid::new_v4().to_string();

        // No pre-insert: the create below projects through the backend, which
        // is what `signal_is_in_space` then finds. Seeding the id first
        // would be a collision — the authored seam refuses a create naming
        // a signal that already exists.
        let backend = InMemorySignalBackend::new();

        let signals_dir = tempfile::TempDir::new().unwrap();
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        relay.test_set_record_log(
            Some(Arc::new(crate::record_log::SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(signals_dir.path().to_path_buf()),
            ))),
            Some(Arc::new(backend)),
        );
        register_mcp_session(&mut relay, "sess-closer", "did:closer");

        // Append the CREATED record via the shared flag-signal path.
        relay
            .relay_flag_signal(
                Some(&signal_id),
                &space_id,
                Some(&doc_id),
                "did:author",
                0,
                kutl_proto::vocab::space_audience(),
                "test message",
                None,
                0,
                None,
            )
            .await
            .expect("create flag must succeed");

        // --- path 1: create → close → Closed ---
        relay
            .handle_mcp_close_flag("sess-closer", &space_id, &signal_id, None, None)
            .await
            .expect("close must succeed");

        let loaded = relay
            .record_log
            .load_space(space_uuid)
            .await
            .expect("segment load");
        let mut fold = SpaceSignalState::default();
        for rec in loaded {
            fold.apply(rec);
        }
        assert_eq!(
            fold.get(&signal_id).expect("signal must be in fold").status,
            SignalStatus::Closed,
            "signal must be Closed after create → close"
        );

        // --- path 2: … → reopen → Open ---
        relay
            .handle_mcp_reopen_flag("sess-closer", &space_id, &signal_id)
            .await
            .expect("reopen must succeed");

        let loaded = relay
            .record_log
            .load_space(space_uuid)
            .await
            .expect("segment load");
        let mut fold = SpaceSignalState::default();
        for rec in loaded {
            fold.apply(rec);
        }
        assert_eq!(
            fold.get(&signal_id).expect("signal must be in fold").status,
            SignalStatus::Open,
            "signal must be Open after create → close → reopen"
        );
    }

    /// Guard: the caller-supplied `close` reason must be signed into the
    /// durable CLOSED record verbatim — a handler that hardcoded
    /// `CloseReason::Resolved` would permanently mis-record `declined` /
    /// `withdrawn` closes. Drives all three reasons
    /// on three flags and asserts the appended record's `close_reason()`.
    #[tokio::test]
    async fn test_close_reason_roundtrips_into_durable_record() {
        let space_uuid = uuid::Uuid::new_v4();
        let space_id = space_uuid.to_string();

        let backend = Arc::new(InMemorySignalBackend::new());
        let signals_dir = tempfile::TempDir::new().unwrap();
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        relay.test_set_record_log(
            Some(Arc::new(crate::record_log::SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(signals_dir.path().to_path_buf()),
            ))),
            Some(Arc::clone(&backend) as _),
        );
        register_mcp_session(&mut relay, "sess-closer", "did:closer");

        // Each row: caller reason string → expected durable CloseReason.
        let cases = [
            ("resolved", CloseReason::Resolved),
            ("declined", CloseReason::Declined),
            ("withdrawn", CloseReason::Withdrawn),
        ];

        for (reason_str, expected) in cases {
            let signal_id = uuid::Uuid::new_v4().to_string();
            let doc_id = uuid::Uuid::new_v4().to_string();
            relay
                .relay_flag_signal(
                    Some(&signal_id),
                    &space_id,
                    Some(&doc_id),
                    "did:author",
                    0,
                    kutl_proto::vocab::space_audience(),
                    "test message",
                    None,
                    0,
                    None,
                )
                .await
                .expect("create flag must succeed");

            relay
                .handle_mcp_close_flag("sess-closer", &space_id, &signal_id, Some(reason_str), None)
                .await
                .expect("close must succeed");

            // Read the CLOSED record back from the durable segment and assert
            // the reason survived the round-trip.
            let loaded = relay
                .record_log
                .load_space(space_uuid)
                .await
                .expect("record load");
            let closed = loaded
                .iter()
                .find(|r| r.id == signal_id && r.event() == SignalEventType::Closed)
                .expect("a CLOSED record must exist for this signal");
            assert_eq!(
                closed.close_reason(),
                expected,
                "close reason {reason_str:?} must be signed into the durable record"
            );
        }
    }

    #[test]
    fn test_describe_refusal_bounds_the_echoed_excerpt() {
        // The excerpt is a line of the caller's own text. A minified file or
        // a pasted blob is one line long enough to bury the instruction the
        // sentence exists to deliver.
        let long = describe_refusal(
            kutl_core::HunkRefusal::RegionChanged,
            &"x".repeat(MAX_REFUSAL_EXCERPT_CHARS * 4),
        );
        let longer = describe_refusal(
            kutl_core::HunkRefusal::RegionChanged,
            &"x".repeat(MAX_REFUSAL_EXCERPT_CHARS * 400),
        );
        assert_eq!(
            long, longer,
            "the sentence must not grow with the caller's line length"
        );
        assert!(
            long.contains("read the document again"),
            "the instruction survives the truncation: {long}"
        );
    }

    #[test]
    fn test_describe_refusal_truncates_on_a_character_boundary() {
        let wide = "é".repeat(MAX_REFUSAL_EXCERPT_CHARS + 10);
        let sentence = describe_refusal(kutl_core::HunkRefusal::RegionAmbiguous, &wide);
        assert!(
            sentence.contains(&"é".repeat(MAX_REFUSAL_EXCERPT_CHARS)),
            "truncation counts characters, not bytes: {sentence}"
        );
    }
}
