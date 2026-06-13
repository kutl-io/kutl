//! MCP (Model Context Protocol) family of the relay actor: session
//! lifecycle and reaping, tool handlers, document/signal resolution, and
//! SSE notification fan-out.
//!
//! Child module of the relay actor (`super`) so the `impl Relay` block here
//! reaches the actor's private fields directly. Pure relocation from
//! relay.rs — behavior, dispatch, and the agent-facing `McpError` hint
//! strings (an agent contract) are unchanged; `process_command` in relay.rs
//! still routes MCP commands to these handlers.

use std::sync::Arc;
use std::time::Duration;

use kutl_core::Boundary;
use kutl_proto::sync;
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::auth::AuthError;
use crate::mcp_tools;
use crate::observer::{
    EditContentMode, MergedEvent, ReactionEvent, SignalClosedEvent, SignalCreatedEvent,
    SignalReopenedEvent,
};
use crate::registry;
use crate::registry_store::MirrorMetadata;

use super::{
    AuthorIdentity, BlobData, DocContent, DocKey, DocSlot, EditedAtPending, LoadResult,
    NO_SKIP_CONN, Relay, build_and_relay_text_outbound, compute_blob_hash, persist_entry,
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
    /// scope checks) and `pat_id` (for per-PAT signal attribution per
    /// RFD 0044).
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
/// document id or path. Directs the agent at `create_document` rather
/// than the old auto-create surprise.
const DOC_NOT_FOUND_HINT_EDIT: &str = "use create_document to create new documents";

/// Agent-facing hint when `create_document` / `upload_blob` targets a
/// space the relay's space backend doesn't know about. Per RFD 0083
/// §4 space creation is intentionally NOT an MCP capability — direct
/// the agent at the human creation flow.
const SPACE_NOT_FOUND_HINT: &str = "space creation is not an MCP capability — humans create spaces via `kutl init`, the desktop app, the kutlhub web UI, or `POST /spaces/register`. Use `list_spaces` to discover destinations you're already authorised for";

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
    /// an MCP capability per RFD 0083 §4.
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

/// Content and version of a document read via MCP.
#[derive(Debug, Serialize)]
pub struct McpDocumentContent {
    /// The full text content.
    pub content: String,
    /// Current version identifier (stringified for JSON).
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
#[derive(Debug, Serialize)]
pub struct McpParticipant {
    /// DID of the participant.
    pub did: String,
    /// Connection type: "websocket" or "mcp".
    pub connection_type: String,
}

/// Space status information.
#[derive(Debug, Serialize)]
pub struct McpSpaceStatus {
    /// Number of documents in the space.
    pub document_count: usize,
    /// Total number of WS subscribers across all docs.
    pub subscriber_count: usize,
    /// Number of active MCP sessions.
    pub mcp_session_count: usize,
}

/// Result of an edit operation via MCP.
#[derive(Debug, Serialize)]
pub struct McpEditResult {
    /// The actual document UUID (may differ from the requested ID if
    /// a non-UUID string was provided and a UUID was auto-generated).
    pub document_id: String,
    /// Version string after the edit.
    pub new_version: String,
    /// Number of CRDT ops applied.
    pub ops_applied: usize,
    /// Whether the bulk (delete-all + insert-all) path was taken.
    pub was_bulk: bool,
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
    /// cross-reference `PatAttribution.owner_display_name` (RFD 0044).
    /// `None` when no `MembershipBackend` is configured or when the
    /// owner has no display name set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_display_name: Option<String>,
}

impl Relay {
    // -----------------------------------------------------------------------
    // MCP handlers
    // -----------------------------------------------------------------------

    pub(super) async fn handle_mcp_validate_token(
        &self,
        token: &str,
    ) -> Result<(String, Option<crate::auth::PatAuthContext>), AuthError> {
        let auth = self.auth.as_ref().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        auth.validate_token_with_backend(
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

    /// Validate that a session exists and optionally belongs to the expected DID.
    ///
    /// When `expected_did` is `Some`, both existence and DID ownership are
    /// checked (auth-on path). When `None`, only existence is verified
    /// (auth-off path where the DID was self-asserted at initialize time).
    pub(super) fn handle_mcp_validate_session_did(
        &mut self,
        session_id: &str,
        expected_did: Option<&str>,
    ) -> Result<(), McpError> {
        let did = self.validate_mcp_session(session_id)?;
        if let Some(expected) = expected_did
            && did != expected
        {
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
    /// attribution (RFD 0044). This is the standard preamble shared by
    /// all MCP handler methods that operate on a specific space.
    async fn authorize_mcp_caller(
        &mut self,
        session_id: &str,
        space_id: &str,
    ) -> Result<AuthorIdentity, McpError> {
        let did = self.validate_mcp_session(session_id)?;
        let (pat_hash, pat_id) = self
            .mcp_sessions
            .get(session_id)
            .and_then(|s| s.pat.as_ref())
            .map_or((None, None), |p| {
                (Some(p.pat_hash.clone()), Some(p.pat_id.clone()))
            });
        let _ = self
            .authorize_space(&did, space_id, pat_hash.as_deref())
            .await
            .map_err(|_| McpError::NotAuthorized {
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
    /// an MCP write to create a document in it. Per RFD 0083 §4 (audit
    /// amendment 2026-05-20) space creation is NOT an MCP capability;
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
    /// - **Pure ephemeral mode** (`space_backend` is None): there is no
    ///   persistent registry, so the relay relies on in-memory state.
    ///   Accept any `space_id` already present in `self.registries` (a
    ///   prior doc registered against it within this process lifetime).
    ///   Otherwise allow implicit-create-on-first-touch — ephemeral
    ///   mode is for tests and dev where there's no out-of-band
    ///   registration path. This is the ONLY remaining caller of the
    ///   legacy implicit-create behaviour, scoped to a deliberately
    ///   non-production mode. See parking-lot "Ephemeral-mode MCP
    ///   implicit-create exception" for the durable decision to revisit.
    ///
    /// **Dual-shape `space_id` (slug or UUID).** Accepts both input
    /// forms — `resolve_by_id` covers UUID-shape, `resolve_by_name`
    /// covers slug-shape. The returned `String` is the **canonical
    /// UUID**, so callers can rebind `space_id` and key every
    /// downstream backend / registry call on a single shape. This is
    /// the entry-point fix for the slug-vs-UUID parallel-registry-entry
    /// bug class — every MCP handler that takes a `space_id` MUST call
    /// this first and use the returned value from that point on. Per
    /// parking-lot "MCP `space_id` boundary normalization."
    ///
    /// In **pure ephemeral mode** (no `space_backend`) there is no
    /// canonical authority — return the raw caller-supplied string so
    /// the existing implicit-create-on-first-touch behaviour stays
    /// intact. Tests and dev scenarios that run without a backend
    /// already accept the consequence of slug-vs-UUID drift.
    async fn mcp_check_space_registered(&self, space_id: &str) -> Result<String, McpError> {
        if let Some(ref backend) = self.space_backend {
            match backend.resolve_by_id(space_id).await {
                Ok(Some(reg)) => return Ok(reg.space_id),
                Ok(None) => {}
                Err(e) => {
                    return Err(McpError::Internal(format!(
                        "space backend lookup failed for {space_id}: {e}"
                    )));
                }
            }
            match backend.resolve_by_name(space_id).await {
                Ok(Some(reg)) => return Ok(reg.space_id),
                Ok(None) => {}
                Err(e) => {
                    return Err(McpError::Internal(format!(
                        "space backend lookup failed for {space_id}: {e}"
                    )));
                }
            }
            return Err(McpError::SpaceNotFound {
                space_id: space_id.to_owned(),
                hint: SPACE_NOT_FOUND_HINT.to_owned(),
            });
        }
        // Ephemeral fallback: no backend, no source of truth. Accept
        // any space the in-process registry has seen; allow implicit-
        // create for genuinely-new slugs. See doc-comment above.
        Ok(space_id.to_owned())
    }

    pub(super) async fn handle_mcp_read_document(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
    ) -> Result<McpDocumentContent, McpError> {
        // Rebind to the canonical UUID so every downstream registry /
        // backend / DocKey call keys on the same shape regardless of
        // whether the caller passed slug- or UUID-form (parking-lot
        // "MCP `space_id` boundary normalization").
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
                version: "[]".into(),
            }),
            DocContent::Text(doc) => Ok(McpDocumentContent {
                content: doc.content(),
                version: format!("{:?}", doc.local_version()),
            }),
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

        let mut participants = Vec::new();

        // Collect WS-authenticated DIDs subscribed to any doc in this space.
        let mut seen_dids = std::collections::HashSet::new();
        for (key, slot) in &self.documents {
            if key.space_id != space_id {
                continue;
            }
            for (sub_conn_id, _) in slot.subscribers.active_entries() {
                if let Some((did, _)) = self.authenticated.get(&sub_conn_id)
                    && seen_dids.insert(did.clone())
                {
                    participants.push(McpParticipant {
                        did: did.clone(),
                        connection_type: "websocket".into(),
                    });
                }
            }
        }

        // Collect MCP sessions (all are space-agnostic for now).
        for session in self.mcp_sessions.values() {
            if seen_dids.insert(session.did.clone()) {
                participants.push(McpParticipant {
                    did: session.did.clone(),
                    connection_type: "mcp".into(),
                });
            }
        }

        Ok(participants)
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

        let mut document_count = 0;
        let mut subscriber_count = 0;

        for (key, slot) in &self.documents {
            if key.space_id == space_id {
                document_count += 1;
                subscriber_count += slot.subscribers.active_count();
            }
        }

        Ok(McpSpaceStatus {
            document_count,
            subscriber_count,
            mcp_session_count: self.mcp_sessions.len(),
        })
    }

    pub(super) async fn handle_mcp_edit_document(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
        new_content: &str,
        intent: &str,
        _snippet: &str,
    ) -> Result<McpEditResult, McpError> {
        // FIXME(rfd0044): document-edit PAT attribution is deferred —
        // an agent editing a doc via PAT currently attributes to its
        // human principal's DID with no PAT distinction. The shape
        // mirrors how signal attribution works (a sibling column on
        // the documents table), but landing it needs the same design
        // pass that decides the close-attribution lifecycle-kind
        // extension. Not "documents are intentionally human-only";
        // we just haven't decided yet.
        //
        // Canonicalize at the boundary; see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id: _ } =
            self.authorize_mcp_caller(session_id, space_id).await?;

        // Strict resolution: the document MUST already exist in the
        // registry. No auto-create — that's `create_document`'s job
        // (RFD 0083 §1). The hint string is part of the agent-facing
        // contract; do not drop it.
        let actual_doc_id = self
            .resolve_existing_doc_id(space_id, document_id)
            .ok_or_else(|| McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                hint: DOC_NOT_FOUND_HINT_EDIT.to_owned(),
            })?;

        // Hydrate the slot from the backends before editing. If the document
        // is an evicted blob, `apply_mcp_text_edit` would otherwise
        // `or_insert_with(DocSlot::empty)` a fresh Empty slot, miss the
        // `DocContent::Blob` guard, and silently overwrite the binary blob with
        // the agent's text. Loading first lets the guard fire.
        //
        // Classify the load rather than discarding it: a `Found` blob inserts a
        // slot the guard catches, and a genuinely `Empty` doc is safe to write
        // as text — but `Inconsistent`/`Unavailable` (backend unreachable, or
        // the witness says content was persisted yet none came back) means we
        // CANNOT rule out an evicted blob, so fail closed and refuse rather than
        // risk clobbering binary content.
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: actual_doc_id.clone(),
        };
        if !self.documents.contains_key(&key) {
            match self.load_from_backends(space_id, &actual_doc_id).await {
                LoadResult::Found(slot) => {
                    self.documents.insert(key.clone(), *slot);
                }
                LoadResult::Empty => {}
                LoadResult::Inconsistent { .. } | LoadResult::Unavailable { .. } => {
                    return Err(McpError::Internal(
                        "document could not be loaded for editing; storage is \
                         temporarily unavailable or its content is inconsistent — retry"
                            .to_owned(),
                    ));
                }
            }
        }

        self.apply_mcp_text_edit(
            session_id,
            space_id,
            &actual_doc_id,
            &did,
            intent,
            new_content,
        )
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

    /// Apply a text edit to an already-resolved, already-registered
    /// document slot. Shared by `handle_mcp_edit_document` and
    /// `handle_mcp_create_document`. Does NOT create or register the
    /// document; the caller is responsible for that.
    fn apply_mcp_text_edit(
        &mut self,
        session_id: &str,
        space_id: &str,
        actual_doc_id: &str,
        did: &str,
        intent: &str,
        new_content: &str,
    ) -> Result<McpEditResult, McpError> {
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: actual_doc_id.to_owned(),
        };

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

        // Apply the edit. mutate_text sets dirty only if the closure
        // returns Ok, so the zero-ops case (no mutation) stays clean.
        // Use session_id (36 bytes) as CRDT agent name — the DID can
        // exceed diamond-types' 50-byte limit.
        let did_owned = did.to_owned();
        let session_owned = session_id.to_owned();
        let (outcome, hub_version) = slot.mutate_text(|doc| {
            let agent = doc
                .register_agent(&session_owned)
                .map_err(|e| McpError::EditFailed(e.to_string()))?;
            let outcome = doc
                .replace_content(agent, &did_owned, intent, Boundary::Explicit, new_content)
                .map_err(|e| McpError::EditFailed(e.to_string()))?;
            Ok((outcome, doc.local_version()))
        })?;

        if outcome.ops_applied == 0 {
            return Ok(McpEditResult {
                document_id: actual_doc_id.to_owned(),
                new_version: format!("{hub_version:?}"),
                ops_applied: 0,
                was_bulk: false,
            });
        }

        self.broadcast_mcp_text_edit(&key, session_id, did, intent, &outcome);

        Ok(McpEditResult {
            document_id: actual_doc_id.to_owned(),
            new_version: format!("{hub_version:?}"),
            ops_applied: outcome.ops_applied,
            was_bulk: outcome.was_bulk,
        })
    }

    /// Post-mutation half of `apply_mcp_text_edit`: stamp `edited_at`,
    /// relay deltas to WS subscribers, notify peer MCP sessions, flush
    /// the pending edit through the debounce window (MCP edits bypass
    /// it), and fire the after-merge observer.
    ///
    /// Split out of `apply_mcp_text_edit` purely for readability — the
    /// two halves had different shapes (pure CRDT mutation vs. fan-out
    /// of side effects) and the combined function exceeded Boris's
    /// 100-line guideline.
    fn broadcast_mcp_text_edit(
        &mut self,
        key: &DocKey,
        session_id: &str,
        did: &str,
        intent: &str,
        outcome: &kutl_core::ReplaceOutcome,
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

        self.flush_pending_edit_for(key);
        if let DocContent::Text(doc) = &self.documents.get(key).expect("slot just used").content {
            self.after_merge.after_text_merge(
                MergedEvent {
                    space_id: key.space_id.clone(),
                    document_id: key.document_id.clone(),
                    author_did: did.to_owned(),
                    op_count: outcome.ops_applied,
                    intent: intent.to_owned(),
                    content_mode: EditContentMode::Text,
                    timestamp: kutl_core::env::now_ms(),
                },
                doc,
            );
        }
    }

    /// MCP `create_document` handler — mints a new UUID, registers with
    /// the supplied provenance, then applies initial content via
    /// [`apply_mcp_text_edit`]. Path collisions surface as
    /// [`McpError::PathAlreadyInUse`]; unknown spaces surface as
    /// [`McpError::SpaceNotFound`].
    ///
    /// **Unknown-space behaviour (RFD 0083 §4, audit-amended
    /// 2026-05-20):** space creation is NOT an MCP capability. With a
    /// `space_backend` configured (kutlhub Postgres, OSS `SQLite`),
    /// the handler errors on unknown `space_id` and routes the agent
    /// at the human creation flow. Pure ephemeral mode (no backend
    /// configured — test/dev) keeps implicit-create-on-first-touch
    /// because there is no out-of-band registration path in that mode;
    /// see `mcp_check_space_registered` for the policy detail.
    pub(super) async fn handle_mcp_create_document(
        &mut self,
        session_id: &str,
        space_id: &str,
        path: &str,
        content: &str,
        provenance: mcp_tools::ProvenanceArgs,
    ) -> Result<McpCreateDocumentResult, McpError> {
        // FIXME(rfd0044): see `apply_mcp_text_edit` — document-edit
        // PAT attribution is deferred pending the same design pass
        // that resolves the close-attribution lifecycle-kind
        // extension. The discard here is not "intentionally
        // human-only"; it's "not designed yet."
        //
        // Canonicalize at the boundary BEFORE authorize_mcp_caller so
        // PAT scope + membership checks key on the canonical UUID;
        // see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id: _ } =
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
        let account_id = if let Some(ref membership) = self.membership_backend {
            membership.resolve_did_to_account(&did).await.ok().flatten()
        } else {
            None
        };

        let rfd0081 = registry::RegisterRfd0081 {
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
            rfd0081,
            // MCP `create_document` doesn't surface UX-only metadata
            // (title/content_type/convert provenance/size) — the
            // mirror's filename-derived title fallback applies. Web
            // `CreateDocument` is the path that supplies them
            // (Phase 3 of RFD 0042 amendment).
            &MirrorMetadata::default(),
            kutl_core::env::now_ms(),
            None,
            NO_SKIP_CONN,
        )
        .map_err(McpError::EditFailed)?;

        if !content.is_empty() {
            let _ = self.apply_mcp_text_edit(
                session_id,
                space_id,
                &actual_doc_id,
                &did,
                "create_document",
                content,
            )?;
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
    #[expect(
        clippy::too_many_lines,
        reason = "linear MCP blob-upload flow (validate, register-or-rebind, persist, broadcast); \
                  splitting would scatter the single ack/error contract"
    )]
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
        // FIXME(rfd0044): see `apply_mcp_text_edit` — blob-upload
        // PAT attribution is deferred pending the documents-side
        // design pass. Not intentionally human-only.
        //
        // Canonicalize at the boundary BEFORE authorize_mcp_caller so
        // PAT scope + membership checks key on the canonical UUID;
        // see read_document for rationale.
        let canonical_space_id = self.mcp_check_space_registered(space_id).await?;
        let space_id: &str = &canonical_space_id;
        let AuthorIdentity { did, via_pat_id: _ } =
            self.authorize_mcp_caller(session_id, space_id).await?;

        if bytes.len() > max_bytes {
            return Err(McpError::BlobTooLarge {
                actual: bytes.len(),
                max: max_bytes,
            });
        }

        // Determine whether this is a create or replace by checking the
        // registry path index. Text documents at the same path are a
        // hard conflict (see RFD 0083 §3).
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
            let account_id = if let Some(ref membership) = self.membership_backend {
                membership.resolve_did_to_account(&did).await.ok().flatten()
            } else {
                None
            };
            let rfd0081 = registry::RegisterRfd0081 {
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
                rfd0081,
                // MCP `upload_blob` doesn't surface UX-only metadata
                // here — the mirror's filename-derived title fallback
                // applies.
                &MirrorMetadata::default(),
                kutl_core::env::now_ms(),
                None,
                NO_SKIP_CONN,
            )
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
        self.flush_pending_edit_for(&key);

        // Notify observers + other MCP sessions so the change is visible.
        let intent = if is_new {
            "upload_blob"
        } else {
            "replace_blob"
        };
        self.send_mcp_notification(session_id, space_id, &actual_doc_id, &did, intent);
        self.observer.on_blob_edited(MergedEvent {
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
    /// the `authorized_keys` file: a DID listed there sees every space
    /// the relay knows about; any other DID sees none. Extension hosts
    /// (kutlhub-relay) override this command before it reaches the
    /// relay actor — out of scope here.
    pub(super) async fn handle_mcp_list_spaces(
        &mut self,
        session_id: &str,
    ) -> Result<Vec<McpSpaceSummary>, McpError> {
        let did = self.validate_mcp_session(session_id)?;

        // If `authorized_keys` is configured and the caller isn't in it,
        // they see nothing. When it's not configured, the relay is
        // anonymous-by-default and every connected DID sees everything.
        if let Some(ref ak) = self.authorized_keys
            && !ak.is_authorized(&did)
        {
            return Ok(Vec::new());
        }

        // Membership-backed path (kutlhub): route through
        // `list_spaces_for_account`, the same call the WS
        // `ListMySpaces` handler uses. The in-memory `self.registries`
        // map is populated lazily on subscribe, so a freshly-connected
        // agent would otherwise see an empty list — contradicting the
        // tool description's promise that this returns destinations
        // the caller is already authorized for.
        if let Some(membership) = self.membership_backend.as_ref().map(Arc::clone) {
            // Resolve DID → account. PgPatBackend synthesizes
            // `account:<account_id>` for accounts without a custodied
            // DID (RFD 0044), so strip that prefix directly. Otherwise
            // hit the custodied_keys lookup.
            let account_id = if let Some(rest) = did.strip_prefix("account:") {
                Some(rest.to_owned())
            } else {
                membership
                    .resolve_did_to_account(&did)
                    .await
                    .map_err(|e| McpError::Internal(e.to_string()))?
            };
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
        let mut summaries: Vec<McpSpaceSummary> = self
            .registries
            .keys()
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

    /// Create a flag signal via MCP.
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

        let signal_id = self
            .relay_flag_signal(
                supplied_signal_id,
                space_id,
                document_id,
                &author.did,
                kind,
                audience,
                message,
                target_did,
                anchor_text,
                kutl_core::env::now_ms(),
                Some(&author.did),
                None,
                author.via_pat_id.as_deref(),
            )
            .await
            .map_err(|e| McpError::Internal(format!("failed to persist flag signal: {e}")))?;

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

        let Some(ref backend) = self.change_backend else {
            return Ok(crate::change_backend::ChangesResponse::default());
        };

        // The cursor must be unique per agent. When auth is enabled, the DID
        // is unique. When auth is disabled, all sessions get DID "anonymous" —
        // using that as the cursor key means agents share one cursor and steal
        // each other's signals. Use session_id as the cursor key instead.
        let cursor_key = if did == "anonymous" { session_id } else { &did };

        backend
            .get_changes(cursor_key, space_id, checkpoint)
            .await
            .map_err(|e| McpError::Internal(e.to_string()))
    }

    /// Create a reply to a signal via MCP.
    ///
    /// Generates a UUID, emits a [`SignalCreatedEvent`] (`signal_type`: "reply"),
    /// and persists via the change backend. No WebSocket broadcast.
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
        // reopen handlers, which all gate on `verify_signal_in_space`.
        self.verify_signal_in_space(space_id, parent_signal_id)
            .await?;

        let signal_id = uuid::Uuid::new_v4().to_string();
        let now = kutl_core::env::now_ms();

        // Persist FIRST. A persistence failure halts the whole emit so
        // the agent sees the actual error instead of a phantom success
        // (the broadcast/observer chain would otherwise have published
        // events for a signal that never reached the DB). Symmetric
        // with `relay_flag_signal`.
        if let Some(ref backend) = self.change_backend {
            backend
                .record_signal(
                    &signal_id,
                    space_id,
                    None,
                    &did,
                    "reply",
                    now,
                    None, // flag_kind
                    None, // audience
                    None, // message
                    None, // target_did
                    None, // anchor_text — replies have no anchor
                    Some(parent_signal_id),
                    parent_reply_id,
                    Some(body),
                    via_pat_id.as_deref(),
                )
                .await
                .inspect_err(|e| {
                    error!(signal_id = %signal_id, error = %e, "failed to persist reply signal — skipping observer notification");
                })
                .map_err(|e| McpError::Internal(format!("failed to persist reply signal: {e}")))?;
        }

        self.observer.on_signal_created(SignalCreatedEvent {
            id: signal_id.clone(),
            space_id: space_id.to_owned(),
            document_id: String::new(),
            author_did: did.clone(),
            signal_type: "reply".to_owned(),
            timestamp: now,
            flag_kind: None,
            audience: None,
            target_did: None,
            message: None,
            parent_signal_id: Some(parent_signal_id.to_owned()),
            parent_reply_id: parent_reply_id.map(str::to_owned),
            body: Some(body.to_owned()),
            via_pat_id: via_pat_id.clone(),
        });

        Ok(signal_id)
    }

    /// Add or remove a reaction on a signal via MCP.
    ///
    /// Fires the observer event for stream propagation. No direct DB write.
    /// Verify that a signal belongs to the claimed space. Prevents
    /// cross-space mutations where a caller authorized for space-A
    /// supplies a `signal_id` that actually lives in space-B.
    ///
    /// Skipped when no change backend is configured (OSS relay with
    /// ephemeral in-memory signals).
    async fn verify_signal_in_space(
        &self,
        space_id: &str,
        signal_id: &str,
    ) -> Result<(), McpError> {
        let Some(ref backend) = self.change_backend else {
            return Ok(());
        };
        match backend.get_signal_detail(space_id, signal_id).await {
            Ok(_) => Ok(()),
            Err(crate::change_backend::ChangeError::NotFound(_)) => Err(McpError::SignalNotFound {
                signal_id: signal_id.to_owned(),
            }),
            Err(e) => Err(McpError::Internal(e.to_string())),
        }
    }

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
        self.verify_signal_in_space(space_id, signal_id).await?;

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
    /// Fires the observer event for stream propagation. When `reason` is
    /// `None`, defaults to `"resolved"`.
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
        self.verify_signal_in_space(space_id, signal_id).await?;

        let resolved_reason = reason.map_or_else(|| "resolved".to_owned(), str::to_owned);

        self.observer.on_signal_closed(SignalClosedEvent {
            space_id: space_id.to_owned(),
            signal_id: signal_id.to_owned(),
            actor_did: did,
            via_pat_id,
            reason: resolved_reason,
            close_note: close_note.map(str::to_owned),
            timestamp: kutl_core::env::now_ms(),
        });

        Ok(())
    }

    /// Reopen a previously closed flag signal via MCP.
    ///
    /// Fires the observer event for stream propagation.
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
        self.verify_signal_in_space(space_id, signal_id).await?;

        self.observer.on_signal_reopened(SignalReopenedEvent {
            space_id: space_id.to_owned(),
            signal_id: signal_id.to_owned(),
            actor_did: did,
            via_pat_id,
            timestamp: kutl_core::env::now_ms(),
        });

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

        let Some(ref backend) = self.change_backend else {
            return Err(McpError::Internal(
                "signal detail not available on this relay".into(),
            ));
        };

        backend
            .get_signal_detail(space_id, signal_id)
            .await
            .map_err(|e| match e {
                crate::change_backend::ChangeError::NotFound(_) => McpError::SignalNotFound {
                    signal_id: signal_id.to_owned(),
                },
                other => McpError::Internal(other.to_string()),
            })
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
    fn send_mcp_notification(
        &mut self,
        exclude_session_id: &str,
        space_id: &str,
        document_id: &str,
        author_did: &str,
        intent: &str,
    ) {
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/document/changed",
            "params": {
                "space_id": space_id,
                "document_id": document_id,
                "author_did": author_did,
                "intent": intent
            }
        });
        let json = notification.to_string();

        let mut dead_notify_tx = Vec::new();

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
    /// `authoritative_did`, when `Some`, overrides `change.author_did` for
    /// every notification — this is the relay-authenticated identity of
    /// the sender.
    pub(super) fn notify_mcp_sessions_from_ws(
        &mut self,
        space_id: &str,
        document_id: &str,
        metadata: &[sync::ChangeMetadata],
        authoritative_did: Option<&str>,
    ) {
        if self.mcp_sessions.is_empty() {
            return;
        }

        for change in metadata {
            let author_did = authoritative_did.unwrap_or(&change.author_did);
            self.send_mcp_notification("", space_id, document_id, author_did, &change.intent);
        }
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
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use crate::blob_backend::BlobBackend;

    use super::super::{resident_blob, test_config};
    use super::*;

    /// Regression test for the SSE-channel-close handling in
    /// `send_mcp_notification`.
    ///
    /// Before the fix, a dead `notify_tx` (receiver dropped — happens
    /// when the SSE GET stream closes) caused the relay to remove the
    /// entire session from `mcp_sessions`. After the fix, a dead
    /// `notify_tx` is cleared on the session but the session itself
    /// stays alive — the session-id is request/response identity,
    /// independent of the SSE channel state. The client can re-establish
    /// SSE on the same session-id at any time; idle TTL (20 minutes) is
    /// the only canonical session-cleanup mechanism.
    #[tokio::test]
    async fn test_send_mcp_notification_preserves_session_on_dead_notify_tx() {
        let mut relay = Relay::new_standalone(test_config());

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

    // ---- Test helpers for MCP-path tests ----

    /// Register an MCP session so `validate_mcp_session` / `authorize_mcp_caller`
    /// succeed in tests (with `require_auth` false, the space authorization is
    /// a no-op).
    fn register_mcp_session(relay: &mut Relay, session_id: &str, did: &str) {
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

        async fn delete(&self, space_id: &str, doc_id: &str) -> anyhow::Result<()> {
            self.store
                .lock()
                .unwrap()
                .remove(&(space_id.to_owned(), doc_id.to_owned()));
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
                "space",
                &doc_id,
                &crate::blob_backend::BlobRecord {
                    data: b"\x89PNG-binary".to_vec(),
                    hash: b"png-hash".to_vec(),
                    timestamp: 100,
                },
            )
            .await
            .unwrap();

        let mut relay = Relay::new_standalone_with_backend(
            test_config(),
            None,
            None,
            Some(Arc::new(blob_backend)),
            None,
        );

        // Registry entry exists (post-evict: registry persists, slot doesn't).
        register_doc(&mut relay, "space", &doc_id, "image.png");
        // No resident slot — simulates eviction.
        assert!(!relay.has_document("space", &doc_id));

        register_mcp_session(&mut relay, "sess-1", "did:agent");

        let result = relay
            .handle_mcp_edit_document(
                "sess-1",
                "space",
                &doc_id,
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
            resident_blob(&relay, "space", &doc_id).as_deref(),
            Some(b"\x89PNG-binary".as_slice()),
            "blob content must be untouched after a refused edit"
        );
    }

    // ---- MCP create_reply must verify the parent signal is in-space ----

    /// Minimal change backend that only answers `get_signal_detail`, keyed by
    /// `(space_id, signal_id)`. A miss yields `NotFound`, matching production.
    struct InMemorySignalBackend {
        signals: Mutex<HashMap<(String, String), crate::change_backend::SignalDetail>>,
    }

    impl InMemorySignalBackend {
        fn new() -> Self {
            Self {
                signals: Mutex::new(HashMap::new()),
            }
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
                },
            );
        }
    }

    #[async_trait::async_trait]
    impl crate::change_backend::ChangeBackend for InMemorySignalBackend {
        async fn get_changes(
            &self,
            _did: &str,
            _space_id: &str,
            _checkpoint: Option<&str>,
        ) -> Result<crate::change_backend::ChangesResponse, crate::change_backend::ChangeError>
        {
            Ok(crate::change_backend::ChangesResponse::default())
        }

        #[allow(clippy::too_many_arguments)]
        async fn record_signal(
            &self,
            _id: &str,
            _space_id: &str,
            _document_id: Option<&str>,
            _author_did: &str,
            _signal_type: &str,
            _timestamp: i64,
            _flag_kind: Option<i32>,
            _audience: Option<i32>,
            _message: Option<&str>,
            _target_did: Option<&str>,
            _anchor_text: Option<&str>,
            _parent_signal_id: Option<&str>,
            _parent_reply_id: Option<&str>,
            _body: Option<&str>,
            _via_pat_id: Option<&str>,
        ) -> Result<(), crate::change_backend::ChangeError> {
            Ok(())
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
        backend.insert_signal("space-b", &parent_id);

        let mut relay = Relay::new_standalone(test_config());
        relay.change_backend = Some(Arc::new(backend));
        register_mcp_session(&mut relay, "sess-1", "did:agent");

        // Reply in space-A referencing the space-B parent — must be rejected.
        let result = relay
            .handle_mcp_create_reply("sess-1", "space-a", &parent_id, None, "body")
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
        backend.insert_signal("space-a", &parent_id);

        let mut relay = Relay::new_standalone(test_config());
        relay.change_backend = Some(Arc::new(backend));
        register_mcp_session(&mut relay, "sess-1", "did:agent");

        let result = relay
            .handle_mcp_create_reply("sess-1", "space-a", &parent_id, None, "body")
            .await;
        assert!(
            result.is_ok(),
            "same-space reply parent must succeed, got {result:?}"
        );
    }
}
