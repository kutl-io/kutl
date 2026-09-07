//! Space ops + signals family of the relay actor: space registration and
//! resolution, invitation joins, membership and document listings, and the
//! WebSocket flag-signal path with its shared persist-then-broadcast core.
//!
//! Child module of the relay actor (`super`) so the `impl Relay` block here
//! reaches the actor's private fields directly. `process_command` in
//! relay.rs routes space and signal commands to these handlers, and
//! `mcp.rs` reaches `relay_flag_signal` as a sibling (the MCP create-flag
//! path).

use prost::Message as _;

use kutl_proto::sync::{self, CloseReason, ErrorCode, Signal, SignalEventType};
use tracing::{debug, error, info, warn};

use crate::protocol::{encode_envelope, refusal};
use crate::spaces;

use super::signal_log::Announce;
use super::{ConnId, Relay, SignalTransitionEvent};

/// Largest re-seed batch a caller may push in one frame.
///
/// A memory bound on an untrusted, authenticated caller: the whole batch is
/// held while it is deduped against the space's existing records, so an
/// unbounded push is an unbounded allocation. Sized well above any realistic
/// single re-seed (a catch-up page is 100 records); a legitimate caller with
/// more history submits several batches.
const MAX_RESEED_BATCH: usize = 10_000;

/// How [`Relay::emit_transition_record`] performed a caller's transition.
///
/// The two arms ack differently, which is why the distinction reaches the
/// doors at all: a recorded transition can name the row the projection wrote,
/// a flipped one cannot name anything yet.
#[derive(Debug)]
pub(super) enum TransitionOutcome {
    /// A transition record was minted and appended through the admission
    /// seam. Carries its `record_id` — also the id the projection gives the
    /// lifecycle row, so a consumer fanning out a bell can name the row the
    /// relay wrote instead of minting an id of its own.
    Recorded(String),
    /// The target is a decision: the transition was performed as the marker
    /// flip in its document, and the record follows via the materializer on
    /// that merge — there is no synchronous `record_id` to hand back.
    MarkerFlipped,
}

/// What the marker flip needs to know about a decision signal: the document
/// that holds its heading, the title hash the heading currently carries (the
/// fold's content track, which follows renames), and the heading state the
/// FOLD believes — `None` when the fold says the heading is gone (withdrawn
/// or tombstoned).
struct DecisionFlipTarget {
    document_id: String,
    title_hash: u64,
    fold_state: Option<crate::markers::decisions::DecisionState>,
}

/// What [`Relay::flip_decision_marker`] did to the document.
enum FlipOutcome {
    /// The document was edited — marker flip, note insertion, or both — and
    /// the merge drives the materializer.
    Edited,
    /// The heading already reads the target marker and no note needed
    /// placing; the document was not touched.
    DocAlreadyAgrees,
}

/// Who performed one lifecycle transition, and what they supplied with it.
///
/// A parameter object rather than four more positional arguments: the two
/// transition paths (the single-signal close/reopen and the batched document
/// cascade) both thread the whole set through, and their argument lists had
/// already reached the point where the next one would need a
/// `too_many_arguments` allow.
pub(super) struct TransitionAuthor<'a> {
    /// The authenticated DID performing the transition. Becomes the record's
    /// `author_did` and `actor_did` both, matching `assemble_record`'s contract
    /// for a relay-minted transition.
    pub actor_did: &'a str,
    /// Meaningful on CLOSED only; `None` on reopen and on every cascade.
    pub close_reason: Option<CloseReason>,
    /// The closer's free-text note, which rides the record as a
    /// `TransitionPayload`.
    pub note: Option<&'a str>,
    /// The PAT the actor authenticated with, for per-PAT attribution on the
    /// projection's lifecycle row. `None` for DID auth.
    pub via_pat_id: Option<&'a str>,
}

impl<'a> TransitionAuthor<'a> {
    /// A transition the relay initiated rather than a caller: the deletion and
    /// revive cascades, which carry no reason, no note and no PAT.
    pub(super) fn relay_initiated(actor_did: &'a str) -> Self {
        Self {
            actor_did,
            close_reason: None,
            note: None,
            via_pat_id: None,
        }
    }
}

/// Counts from a re-seed ingest batch.
///
/// Every posted record lands in exactly one bucket:
/// - `appended`: a previously-unseen `record_id`, validated and appended.
/// - `duplicate`: a `record_id` already stored with byte-identical content
///   (idempotent replay — the safe, expected re-seed case; no-op).
/// - `rejected`: failed the well-formedness gate OR its `record_id` is already
///   stored with DIFFERENT bytes (a bijection violation — the stored record is
///   left untouched).
#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct ReSeedOutcome {
    /// New records appended to the space's segments.
    pub appended: usize,
    /// Records already stored byte-identically (idempotent no-op).
    pub duplicate: usize,
    /// Records rejected (invalid or a divergent-bytes `record_id` collision).
    pub rejected: usize,
}

/// The re-seed dedup key: `record` encoded with the RELAY ATTESTATION cleared
/// (the author signature is preserved). The relay attaches its own attestation
/// (`observed_at_ms` + relay signature) on ingest, so a stored
/// record carries an attestation the re-seeding client never sent. Excluding it
/// from the dedup key keeps an honest replay of the client's own record
/// idempotent across calls while still flagging any change to the CLIENT's own
/// bytes (including the author signature) as a divergent collision.
fn dedup_bytes(record: &Signal) -> Vec<u8> {
    if record.attestation.is_none() {
        return record.encode_to_vec();
    }
    let mut stripped = record.clone();
    stripped.attestation = None;
    stripped.encode_to_vec()
}

/// The verdict of checking an incoming record's `record_id` against the space's
/// stored records, enforcing the fold's bijection precondition
/// (`record_id` ↔ record-bytes, [`kutl_signals::fold`]).
#[derive(Debug, PartialEq, Eq)]
enum RecordIdCheck {
    /// `record_id` is unseen — safe to append.
    New,
    /// `record_id` is stored with byte-identical [`dedup_bytes`] — an honest,
    /// idempotent replay (accept, no-op; do not re-append).
    Duplicate,
    /// `record_id` is stored with DIFFERENT [`dedup_bytes`] — a bijection
    /// violation; reject and leave the stored record untouched.
    Collision,
}

/// Classify an incoming `record` against a `record_id` → [`dedup_bytes`] index
/// of the space's already-stored records.
///
/// The single source of truth for the fold-bijection check ([`fold`]'s
/// precondition): re-seed builds the index once and drives its ingest loop
/// through this. Comparison is on [`dedup_bytes`] (relay attestation stripped)
/// so an honest replay of a client's own record — which the relay re-attests on
/// ingest — is `Duplicate`, not `Collision`.
///
/// [`fold`]: kutl_signals::fold
fn classify_record_id(
    by_id: &std::collections::HashMap<String, Vec<u8>>,
    record: &Signal,
) -> RecordIdCheck {
    match by_id.get(&record.record_id) {
        None => RecordIdCheck::New,
        Some(stored_bytes) if *stored_bytes == dedup_bytes(record) => RecordIdCheck::Duplicate,
        Some(_) => RecordIdCheck::Collision,
    }
}

/// A document's folded signals split by lifecycle status for the deletion
/// ladder: `visible` are the currently non-tombstoned signals the
/// delete cascade tombstones; `tombstoned` are the ones the revive cascade
/// reopens. Both sets include the document's directly-attached signals AND the
/// replies whose parent chain reaches them (replies carry no `document_id`).
/// Produced by a single fold pass shared by delete and revive so the two
/// directions can never disagree on the document's signal set.
#[derive(Debug, Default)]
struct DocumentSignalIds {
    /// Non-tombstoned signal ids attached to the document (delete → tombstone).
    visible: Vec<String>,
    /// Tombstoned signal ids attached to the document (revive → reopen).
    tombstoned: Vec<String>,
}

impl Relay {
    // -----------------------------------------------------------------------
    // Space ops handlers (require a configured database)
    // -----------------------------------------------------------------------

    /// Handle `RegisterSpace` — create a new space via the space backend.
    pub(super) async fn handle_register_space(
        &mut self,
        name: &str,
    ) -> Result<crate::space_backend::RegisteredSpace, crate::space_backend::SpaceBackendError>
    {
        if let Err(msg) = spaces::validate_space_name(name) {
            return Err(crate::space_backend::SpaceBackendError::InvalidName(
                msg.to_owned(),
            ));
        }
        let backend = self.space_backend.as_ref().ok_or_else(|| {
            crate::space_backend::SpaceBackendError::Storage("space backend not configured".into())
        })?;
        let result = backend.register(name).await?;
        info!(space_id = %result.space_id, name = %result.name, "space registered");
        Ok(result)
    }

    /// Handle `ResolveSpace` — look up a space by name via the space backend.
    pub(super) async fn handle_resolve_space(
        &mut self,
        name: &str,
    ) -> Result<
        Option<crate::space_backend::RegisteredSpace>,
        crate::space_backend::SpaceBackendError,
    > {
        let backend = self.space_backend.as_ref().ok_or_else(|| {
            crate::space_backend::SpaceBackendError::Storage("space backend not configured".into())
        })?;
        let result = backend.resolve_by_name(name).await?;
        if let Some(ref space) = result {
            info!(space_id = %space.space_id, name = %space.name, "space resolved");
        }
        Ok(result)
    }

    /// The membership backend and the caller's account id, for operations
    /// that need both (invitations, slug lookups). On `None` the connection
    /// has already been told why: `InvalidMessage` when this relay has no
    /// membership backend, `AuthFailed` when its identity resolves to no
    /// account.
    async fn membership_and_caller(
        &mut self,
        conn_id: ConnId,
    ) -> Result<
        (
            std::sync::Arc<dyn crate::membership_backend::MembershipBackend>,
            String,
        ),
        sync::Error,
    > {
        let Some(membership) = self.membership_backend.clone() else {
            return Err(refusal(
                ErrorCode::InvalidMessage,
                "operation not supported on this relay",
            ));
        };
        let Some(account_id) = self.resolve_account_id(conn_id).await else {
            return Err(refusal(ErrorCode::AuthFailed, "not authenticated"));
        };
        Ok((membership, account_id))
    }

    /// Refuse a `JoinSpace`: the result carries the refusal and nothing else.
    fn send_join_refusal(&self, conn_id: ConnId, error: sync::Error) {
        self.send_payload(
            conn_id,
            sync::sync_envelope::Payload::JoinSpaceResult(sync::JoinSpaceResult {
                error: Some(error),
                ..Default::default()
            }),
        );
    }

    /// Refuse a `ResolveSpace`: the result carries the refusal and nothing else.
    fn send_resolve_refusal(&self, conn_id: ConnId, error: sync::Error) {
        self.send_payload(
            conn_id,
            sync::sync_envelope::Payload::ResolveSpaceResult(sync::ResolveSpaceResult {
                error: Some(error),
                ..Default::default()
            }),
        );
    }

    /// Handle `JoinSpace` — accept an invitation code and join the space.
    pub(super) async fn handle_join_space_op(&mut self, conn_id: ConnId, msg: &sync::JoinSpace) {
        let (membership, account_id) = match self.membership_and_caller(conn_id).await {
            Ok(caller) => caller,
            Err(e) => {
                self.send_join_refusal(conn_id, e);
                return;
            }
        };

        match membership.accept_invitation(&msg.code, &account_id).await {
            Ok(result) => {
                self.send_payload(
                    conn_id,
                    sync::sync_envelope::Payload::JoinSpaceResult(sync::JoinSpaceResult {
                        space_id: result.space_id,
                        space_name: result.space_name,
                        space_slug: result.space_slug,
                        owner_account_id: result.owner_account_id,
                        error: None,
                    }),
                );
            }
            Err(e) => {
                warn!(conn_id, error = %e, "join space failed");
                self.send_join_refusal(conn_id, refusal(ErrorCode::InvalidMessage, e.to_string()));
            }
        }
    }

    /// Handle `ResolveSpace` — look up a space by owner slug and space slug.
    pub(super) async fn handle_resolve_space_op(
        &mut self,
        conn_id: ConnId,
        msg: &sync::ResolveSpace,
    ) {
        let (membership, account_id) = match self.membership_and_caller(conn_id).await {
            Ok(caller) => caller,
            Err(e) => {
                self.send_resolve_refusal(conn_id, e);
                return;
            }
        };

        match membership
            .resolve_space_by_slugs(&msg.owner, &msg.slug)
            .await
        {
            Ok(Some(record)) => {
                // Verify caller is a member.
                match membership
                    .check_membership(&record.space_id, &account_id)
                    .await
                {
                    Ok(Some(_role)) => {
                        let relay_url = self.config.external_url.clone().unwrap_or_default();
                        self.send_payload(
                            conn_id,
                            sync::sync_envelope::Payload::ResolveSpaceResult(
                                sync::ResolveSpaceResult {
                                    space_id: record.space_id,
                                    space_name: record.space_name,
                                    relay_url,
                                    error: None,
                                },
                            ),
                        );
                    }
                    Ok(None) => {
                        // Anti-enumeration: a non-member must not be able to
                        // distinguish an existing private space from a
                        // non-existent one. Return the identical not-found error.
                        self.send_resolve_refusal(
                            conn_id,
                            refusal(ErrorCode::InvalidMessage, "space not found"),
                        );
                    }
                    Err(e) => {
                        // This arm is reachable only when the space EXISTS
                        // (resolve returned Some). Anti-enumeration:
                        // emit the byte-identical response the resolve-error arm
                        // sends, so an internal failure on an existing space is
                        // indistinguishable from one on a non-existent space.
                        // (AuthFailed would otherwise leak existence: the CLI
                        // maps it to a distinct "run kutl auth login" message.)
                        debug!(conn_id, error = %e, "membership check failed");
                        self.send_resolve_refusal(
                            conn_id,
                            refusal(ErrorCode::InvalidMessage, "internal error resolving space"),
                        );
                    }
                }
            }
            Ok(None) => {
                self.send_resolve_refusal(
                    conn_id,
                    refusal(ErrorCode::InvalidMessage, "space not found"),
                );
            }
            Err(e) => {
                warn!(conn_id, error = %e, "resolve space failed");
                self.send_resolve_refusal(
                    conn_id,
                    refusal(ErrorCode::InvalidMessage, "internal error resolving space"),
                );
            }
        }
    }

    /// Handle `ListSpaceDocuments` — return all active documents in the space
    /// registry. A pure query: it enrols the connection in nothing (listening
    /// to a space is `SubscribeSignals`).
    pub(super) async fn handle_list_space_documents(
        &mut self,
        conn_id: ConnId,
        msg: &sync::ListSpaceDocuments,
    ) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_payload(
                    conn_id,
                    sync::sync_envelope::Payload::ListSpaceDocumentsResult(
                        sync::ListSpaceDocumentsResult {
                            space_id: msg.space_id.clone(),
                            documents: Vec::new(),
                            error: Some(refusal(ErrorCode::AuthFailed, e.to_string())),
                        },
                    ),
                );
                return;
            }
        };

        let documents: Vec<sync::DocumentInfo> = self
            .registries
            .get(&msg.space_id)
            .map(|reg| {
                reg.active_entries()
                    .map(|(_, entry)| sync::DocumentInfo {
                        document_id: entry.document_id.clone(),
                        path: entry.path.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let has_conn = self.connections.contains_key(&conn_id);
        info!(
            conn_id,
            space_id = %msg.space_id,
            document_count = documents.len(),
            has_conn,
            "sending ListSpaceDocumentsResult"
        );

        self.send_payload(
            conn_id,
            sync::sync_envelope::Payload::ListSpaceDocumentsResult(
                sync::ListSpaceDocumentsResult {
                    space_id: msg.space_id.clone(),
                    documents,
                    error: None,
                },
            ),
        );
    }

    // -----------------------------------------------------------------------
    // Signal handler (flag payload accepted over WebSocket)
    // -----------------------------------------------------------------------

    /// Refuse a bare `Signal` frame on the sender's own-ack lane. The frame
    /// carries no `client_ref`, so the ack echoes the signal id it named
    /// instead; the code is the verdict, the prose the display text.
    fn refuse_signal(
        &self,
        conn_id: ConnId,
        signal_id: &str,
        code: ErrorCode,
        message: impl Into<String>,
    ) {
        self.send_signal_ack(conn_id, "", signal_id, "", Err(refusal(code, message)));
    }

    /// Handle an incoming signal from a WebSocket client.
    ///
    /// Only flag payloads are accepted over WebSocket. Reply and decision
    /// signals are created exclusively through MCP.
    pub(super) async fn handle_signal(&mut self, conn_id: ConnId, msg: &sync::Signal) {
        // Only flag payloads arrive via WebSocket.
        let Some(sync::signal::Payload::Flag(flag)) = &msg.payload else {
            self.refuse_signal(
                conn_id,
                &msg.id,
                ErrorCode::InvalidMessage,
                "only flag signals accepted over websocket",
            );
            return;
        };

        let space_id = &msg.space_id;
        let _authorized = match self.authorize_conn(conn_id, space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.refuse_signal(conn_id, &msg.id, ErrorCode::AuthFailed, e.to_string());
                return;
            }
        };

        let document_id = match &msg.document_id {
            Some(d) if !d.is_empty() => d.as_str(),
            _ => {
                self.refuse_signal(
                    conn_id,
                    &msg.id,
                    ErrorCode::InvalidMessage,
                    "flag signal requires document_id",
                );
                return;
            }
        };
        // Catch path-string document_ids before they reach the durable
        // backend's `::uuid` cast and surface as opaque DB errors. Signals
        // index documents by UUID; path-typed callers are bugs we want
        // to fail loudly.
        if let Err(reason) = crate::ids::check_uuid("document_id", document_id) {
            self.refuse_signal(conn_id, &msg.id, ErrorCode::InvalidMessage, reason);
            return;
        }

        // Resolve the audience through the typed-first precedence rule before
        // validating it: a client on a newer build sets only the typed
        // `Audience`, and reading the deprecated pair directly would reject
        // that flag as "audience type required".
        //
        // The three rejections — unspecified, participant-without-target,
        // space-with-target — live in `audience_from_untyped_checked`, shared
        // with the MCP door, so two doors onto one record log cannot disagree
        // about which shapes are legal.
        let (audience_type, target_did) = kutl_proto::vocab::flag_audience_untyped(flag);
        let audience =
            match kutl_proto::vocab::audience_from_untyped_checked(audience_type, target_did) {
                Ok(a) => a,
                Err(reason) => {
                    self.refuse_signal(conn_id, &msg.id, ErrorCode::InvalidMessage, reason);
                    return;
                }
            };

        // Author identity is the relay-authenticated identity of this
        // connection. Any client-supplied `author_did` would otherwise let an
        // authenticated member impersonate a different DID in signals
        // (including DM targets and persisted `signals.author_did`). The PAT
        // half rides along so an agent's signal is attributed to the agent.
        let author = self.authoritative_identity(conn_id);
        let author_did = author.did.clone();

        // Client-supplied signal id is honored when present and well-
        // formed. Used by the ingestion-worker's comment-materialization
        // path: the `[text]{.cmt #<uuid>}` marker in the
        // imported document content must reference the SAME UUID as the
        // FLAG_KIND_COMMENT signal so the relay's CommentTracker can tie
        // marker to signal on next merge. Without honoring the supplied
        // id the relay would mint a different one and the marker would
        // point at a non-existent signal. Empty id keeps the legacy
        // behavior (relay mints).
        let supplied_id: Option<&str> = if msg.id.is_empty() {
            None
        } else {
            if let Err(reason) = crate::ids::check_uuid("signal_id", &msg.id) {
                self.refuse_signal(conn_id, &msg.id, ErrorCode::InvalidMessage, reason);
                return;
            }
            Some(msg.id.as_str())
        };

        if let Err(e) = self
            .relay_flag_signal(
                supplied_id,
                space_id,
                Some(document_id),
                &author_did,
                flag.kind,
                audience,
                &flag.message,
                // The WS `FlagPayload` doesn't carry `anchor_text` —
                // comment-anchor posterity on the WS path comes from
                // the document body (CommentTracker on merge), not
                // from the WS signal envelope.
                None,
                msg.timestamp,
                author.via_pat_id.as_deref(),
            )
            .await
        {
            // Persistence failed → no broadcast happened, no observer
            // event fired. Tell the WS client honestly so it can retry
            // or surface the failure to its user.
            self.refuse_signal(
                conn_id,
                &msg.id,
                ErrorCode::InvalidMessage,
                format!("failed to persist flag signal: {e}"),
            );
        }
    }

    /// Stamp a caller-built `Signal` as a CREATED record and append it to the
    /// segment store — the create-path preamble shared by every
    /// signal-creation site.
    ///
    /// The caller supplies `record` with its identity + payload fields already
    /// set (`id`, `space_id`, `document_id`, `timestamp`, `payload`); this ticks
    /// the relay's strictly-monotonic HLC (so records emitted in sequence order
    /// by HLC even within one millisecond), assembles the
    /// [`SignalEventType::Created`] envelope via the SHARED
    /// [`assemble_record`](kutl_signals::authoring::assemble_record) the client
    /// uses (structural byte-parity), relay-attests it via
    /// [`attest_on_ingest`](crate::signal_record::attest_on_ingest), and appends
    /// via [`Self::append_authored_record`] — which also projects, replicates
    /// and announces, so there is nothing left for the caller to deliver.
    ///
    /// `author_did == actor_did` on a relay-mint CREATE: the authoritative
    /// caller DID is both the author and the actor, matching `assemble_record`.
    ///
    /// Shared by [`Self::relay_flag_signal`] and the MCP reply-create path so
    /// the two cannot drift on how a CREATED record is minted. `signal_id` is
    /// the resolved record id (equal to the intent's; passed separately because
    /// the caller already owns it for the persistence + broadcast that follow).
    ///
    /// **Takes a [`SignalIntent`](kutl_signals::authoring::SignalIntent), not a
    /// half-built `Signal`.** Taking a record would let a caller hand in one
    /// assembled any way at all, leaving the shape rules nowhere to live. The
    /// caller states its intent through a per-kind builder — where the shape
    /// rules do live — and this seam only stamps and appends.
    pub(super) async fn stamp_and_append_created(
        &mut self,
        intent: &kutl_signals::authoring::SignalIntent,
        signal_id: &str,
        space_id: &str,
        author_did: &str,
        // Threaded to the projection, which stores it on `signals.via_pat_id`
        // for per-PAT attribution. It describes the SESSION rather
        // than the signal, so it has no home on the record itself.
        via_pat_id: Option<&str>,
        // Threaded to the admission seam, which owns the announcement — see
        // [`Announce`]. Required here too, so a future batch producer of
        // CREATEDs (an import, say) has to declare its silence rather than
        // inherit a storm.
        announce: Announce,
    ) -> Result<(), crate::change_backend::ChangeError> {
        let now_ms = kutl_core::env::now_ms();
        let relay_hlc: kutl_proto::sync::Hlc = self
            .signal_clock
            .lock()
            .expect("signal clock lock poisoned")
            .tick(now_ms.cast_unsigned())
            .into();
        let mut record = kutl_signals::authoring::assemble_record(intent, author_did, relay_hlc);
        crate::signal_record::attest_on_ingest(
            &mut record,
            self.signing_identity.as_deref(),
            now_ms,
        );
        self.append_authored_record(&record, signal_id, space_id, via_pat_id, announce)
            .await
    }

    /// Rebuild a space's ENTIRE projection from its durable records.
    ///
    /// A repair path, not a write path. The per-record write happens inside
    /// `append_admitted_record` on every admission; this exists for the one
    /// caller that cannot use it — re-seed, which ingests a peer's records in
    /// arbitrary order, where an incremental apply has no way to reason about
    /// what it is overwriting and the fold's per-signal LWW is the correctness
    /// mechanism.
    ///
    /// Returns `Ok(())` when the projection has no rebuild
    /// ([`crate::change_backend::ChangeBackend::rebuild`] is `None`, which the
    /// Postgres backend says of itself: its rows are maintained incrementally
    /// and there is nothing to re-derive them from) or when no records are kept.
    ///
    /// Returns `Err` if segment load or the rebuild fails — the caller halts
    /// the emit so the store never diverges from peers.
    ///
    /// `context_id` is a log-only breadcrumb identifying what triggered the
    /// rebuild. It never scopes it — the whole space is always refolded.
    pub(super) async fn rebuild_space_projection(
        &mut self,
        context_id: &str,
        space_id: &str,
    ) -> Result<(), crate::change_backend::ChangeError> {
        let Some(rebuild) = self.record_log.rebuild() else {
            return Ok(());
        };
        let space_uuid = uuid::Uuid::parse_str(space_id).map_err(|_| {
            crate::change_backend::ChangeError::Internal(format!(
                "space_id must be a UUID; got {space_id:?}"
            ))
        })?;
        let records = self.record_log.load_space(space_uuid).await.map_err(|e| {
            error!(
                context_id = %context_id,
                error = %e,
                "failed to load segments for rebuild — halting emit"
            );
            crate::change_backend::ChangeError::Internal(e.to_string())
        })?;
        rebuild
            .rebuild_space(space_id, &records)
            .await
            .inspect_err(|e| {
                error!(
                    context_id = %context_id,
                    error = %e,
                    "failed to rebuild signal projection — halting emit"
                );
            })
    }

    /// Fetch the space activity feed (edits + signals) for the HTTP
    /// `GET /spaces/{space_id}/changes` route.
    ///
    /// The caller's DID must be a member of the space (checked via
    /// `authorize_space`). A non-member receives
    /// [`crate::relay::McpError::NotAuthorized`], surfaced by the HTTP handler
    /// as a 403 with a reason body.
    ///
    /// When no change backend is configured the route degrades gracefully: an
    /// empty [`crate::change_backend::ChangesResponse`] is returned (not an
    /// error) — the CLI can fall back to local signals-only display.
    pub(super) async fn handle_get_changes(
        &mut self,
        did: &str,
        space_id: &str,
        checkpoint: Option<&str>,
    ) -> Result<crate::change_backend::ChangesResponse, crate::relay::McpError> {
        let _authorized = self
            .authorize_space(did, space_id, None)
            .await
            .map_err(|_| crate::relay::McpError::NotAuthorized {
                space_id: space_id.to_owned(),
            })?;

        let Some(backend) = self.record_log.reads() else {
            return Ok(crate::change_backend::ChangesResponse::default());
        };

        let mut changes = backend
            .get_changes(did, space_id, checkpoint)
            .await
            .map_err(|e| crate::relay::McpError::Internal(e.to_string()))?;
        // Same audience narrowing the MCP door applies, for the same reason:
        // the bearer's DID is the filter, and the two doors carry the same
        // activity.
        changes.retain_addressed_to(did);
        Ok(changes)
    }

    /// Re-seed a space's signal records from a peer's `SignalCatchUpResult` —
    /// the "clients are the source of truth" property at the relay layer.
    ///
    /// The relay INGESTS the peer's records as-is: unlike the create path, it
    /// does NOT re-stamp `record_id`/`hlc`/`actor_did` (re-stamping would change
    /// the `record_id` and break idempotence). It only validates, checks for a
    /// `record_id` collision against stored bytes, appends the good, and skips
    /// the bad.
    ///
    /// Authorization mirrors [`Self::handle_signal`]: the caller's DID must be
    /// a member of the space (checked via `authorize_space`); non-members
    /// receive [`crate::relay::McpError::NotAuthorized`].
    ///
    /// **Per-record collision defense** (the correctness-critical piece:
    /// ingest well-formedness plus the `record_id` bijection
    /// precondition). For each incoming record:
    /// - fails [`validate_record`] → rejected (counted, not appended);
    /// - `record_id` UNSEEN → appended + projected;
    /// - SEEN with byte-identical stored content → duplicate no-op (idempotent);
    /// - SEEN with DIFFERENT bytes → rejected, NOT appended (a hostile/buggy
    ///   signer reusing one `record_id` for two byte-distinct records would
    ///   break set-union convergence; the stored record is left intact).
    ///
    /// The whole POST succeeds (200) even if some records were rejected — a
    /// re-seed is a recovery batch: append the good, skip the bad, report. One
    /// poison record must not deny a legitimate re-seed.
    ///
    /// Ephemeral relays (no durable `signal_store`) have nowhere to append: the
    /// route stays reachable and every record is counted `rejected`.
    /// Decide whether a re-seed batch may proceed at all, before a single
    /// caller-supplied record is examined.
    ///
    /// Four gates, and their ORDER is the design:
    ///
    /// 1. **Authorization** first, so nothing below can be probed by a caller
    ///    who is not a member.
    /// 2. **Deployment policy** (`accepts_reseed`) next — after authorization so
    ///    a refusal is not an unauthenticated oracle for which relays take
    ///    history, and before anything reads the batch so a refusing relay never
    ///    touches a pushed record.
    /// 3. **Space id well-formedness**, needed to key the log.
    /// 4. **A log to append to.**
    ///
    /// Returns `Ok(Some(space_uuid))` to proceed, `Ok(None)` for a log-less
    /// relay — a reachable route with nowhere to put anything, which is a
    /// per-record rejection rather than an error. `Err` means the batch was
    /// refused outright.
    async fn admit_reseed_batch(
        &mut self,
        did: &str,
        space_id: &str,
        record_count: usize,
    ) -> Result<Option<uuid::Uuid>, crate::relay::McpError> {
        let _authorized = self
            .authorize_space(did, space_id, None)
            .await
            .map_err(|_| crate::relay::McpError::NotAuthorized {
                space_id: space_id.to_owned(),
            })?;

        // Bound the batch, at the admission seam rather than at one
        // transport's door: ANY caller pushing history has to be bounded, and
        // there is more than one way in.
        if record_count > MAX_RESEED_BATCH {
            debug!(
                %space_id,
                count = record_count,
                cap = MAX_RESEED_BATCH,
                "re-seed refused: batch over cap"
            );
            return Err(crate::relay::McpError::RecordRejected {
                reason: format!(
                    "re-seed batch of {record_count} exceeds the {MAX_RESEED_BATCH}-record cap"
                ),
            });
        }

        if !self.config.accepts_reseed {
            debug!(
                %space_id,
                count = record_count,
                "re-seed refused: this relay does not accept client-pushed history"
            );
            return Err(crate::relay::McpError::ReSeedRefused);
        }

        let space_uuid =
            uuid::Uuid::parse_str(space_id).map_err(|_| crate::relay::McpError::NotAuthorized {
                space_id: space_id.to_owned(),
            })?;

        Ok(self.record_log.is_configured().then_some(space_uuid))
    }

    pub(super) async fn handle_signal_reseed(
        &mut self,
        did: &str,
        space_id: &str,
        records: Vec<Signal>,
    ) -> Result<ReSeedOutcome, crate::relay::McpError> {
        let Some(space_uuid) = self
            .admit_reseed_batch(did, space_id, records.len())
            .await?
        else {
            // Log-less relay: reachable route, nowhere to append.
            return Ok(ReSeedOutcome {
                rejected: records.len(),
                ..ReSeedOutcome::default()
            });
        };

        // Load the space's existing records once and index by `record_id` →
        // the record encoded with the RELAY ATTESTATION CLEARED (author
        // signature preserved). The relay attaches its own non-deterministic
        // attestation (`observed_at_ms` + relay signature) on ingest,
        // so a stored record carries an attestation the re-seeding CLIENT never
        // sent. Deduping on `dedup_bytes` (attestation stripped) keeps a replay
        // of the client's own record idempotent ACROSS calls — comparing full
        // bytes would flag every re-seed as a divergent collision. Both sides of
        // the comparison run through the same encoder, so byte-equality is
        // structural equality under prost's canonical field ordering.
        let stored = {
            self.record_log.load_space(space_uuid).await.map_err(|e| {
                error!(error = %e, %space_id, "failed to load records for re-seed");
                crate::relay::McpError::EditFailed(e.to_string())
            })?
        };
        let mut by_id: std::collections::HashMap<String, Vec<u8>> = stored
            .iter()
            .map(|r| (r.record_id.clone(), dedup_bytes(r)))
            .collect();

        // This relay's own identity is the only MATERIALIZER claim it recognizes:
        // a re-seeded record materialized under any other
        // relay's key is either a predecessor relay's or a leaked key's, and the
        // relay cannot tell those apart. Empty when the relay runs identity-less,
        // which trusts nothing rather than everything. Built once for the batch
        // — the verdict is per record, the trust set is not.
        let trusted_relays: Vec<String> = self
            .signing_identity
            .iter()
            .map(|id| id.did().to_owned())
            .collect();

        let now_ms = kutl_core::env::now_ms();
        let mut outcome = ReSeedOutcome::default();
        let mut appended_any = false;
        for mut record in records {
            // Ingest choke-point for REPLICATION: well-formedness + space-binding
            // (rejects a record whose `space_id` ≠ the authorized path space —
            // the cross-tenant injection defense) + advisory signature/
            // attestation logging. Re-seed deliberately does NOT actor-bind: a
            // recovering daemon relays records authored by EVERY member it caught
            // up, so binding `actor_did` to the pushing caller
            // would silently drop every foreign-authored record — permanent
            // multi-member data loss on recovery. Re-seed's integrity instead
            // rests on space-binding above + the `record_id`-collision check just
            // below (a divergent record can never overwrite a stored one) + the
            // advisory tier-1 author signature. Do NOT re-stamp; the record is
            // ingested as-is from the peer.
            if let Err(e) = crate::signal_record::admit_replicated_record(
                &record,
                space_id,
                kutl_signals::RelayTrust::pinned(&trusted_relays),
            ) {
                warn!(
                    record_id = %record.record_id,
                    signal_id = %record.id,
                    error = %e,
                    %space_id,
                    "re-seed: record failed ingest gate — rejected"
                );
                outcome.rejected += 1;
                continue;
            }
            // Dedup on the client-authored bytes (relay attestation stripped) so
            // the relay's own attestation, added just below, cannot make a
            // replay of the client's record look divergent.
            let incoming_bytes = dedup_bytes(&record);
            match classify_record_id(&by_id, &record) {
                // Seen, byte-identical: idempotent replay — no-op.
                RecordIdCheck::Duplicate => {
                    outcome.duplicate += 1;
                }
                // Seen, divergent bytes: bijection violation — REJECT, do NOT
                // append. Never let one poison record overwrite the stored one.
                RecordIdCheck::Collision => {
                    warn!(
                        record_id = %record.record_id,
                        signal_id = %record.id,
                        %space_id,
                        "re-seed: record_id collision with divergent bytes — rejected (not appended)"
                    );
                    outcome.rejected += 1;
                }
                // Unseen: attest (attach the server-observed anchor without
                // disturbing the author signature) then append now;
                // the whole space is ALSO rebuilt once after the loop. Each
                // admitted record is projected individually by the admission
                // seam (the log and its projection are fused so a caller
                // cannot do one and forget the other); the trailing
                // rebuild remains because re-seed ingests records in an order
                // the incremental write cannot reason about, and the fold is
                // order-insensitive and idempotent so re-running it is safe. `attest_reseeded_record` PRESERVES this relay's
                // own past `observed_at` anchor (a record already carrying our
                // own valid attestation is not re-minted — that would destroy the
                // durable anchor the backdated-HLC defense relies on); any other
                // attestation is re-minted fresh.
                RecordIdCheck::New => {
                    crate::signal_record::attest_reseeded_record(
                        &mut record,
                        self.signing_identity.as_deref(),
                        now_ms,
                    );
                    // Through the REPLICATED seam, not a direct `store.append`:
                    // this record was authored elsewhere, so it gets
                    // well-formedness and space binding and none of the authored
                    // seam's tightened checks.
                    let reseed_signal_id = record.id.clone();
                    if let Err(e) = self
                        .append_replicated_record(&record, &reseed_signal_id, space_id)
                        .await
                    {
                        error!(
                            error = %e,
                            record_id = %record.record_id,
                            signal_id = %reseed_signal_id,
                            %space_id,
                            "re-seed: failed to append record — rejected"
                        );
                        outcome.rejected += 1;
                        continue;
                    }
                    by_id.insert(record.record_id.clone(), incoming_bytes);
                    appended_any = true;
                    outcome.appended += 1;
                }
            }
        }

        // KNOWN LIMITATION:
        // re-seed appends + projects but does NOT broadcast the ingested records
        // to live subscribers (unlike `handle_materialized_records`). Combined
        // with the coarse-`physical_ms` GET cursor (see `catchup::records_after`),
        // a re-seeded record whose `physical_ms` is at or below an already-advanced
        // subscriber cursor is unreachable via live catch-up — it surfaces only on
        // a fresh from-zero catch-up (`since = None`). Acceptable: re-seed is a
        // recovery/backfill batch, not a live-edit path.
        //
        // Project the space ONCE after appending all good records. On a
        // projection error, log and still return the outcome counts — the
        // records are already durably appended, so `?`-aborting the POST
        // (an HTTP 500) would contradict the "append the good, report counts"
        // contract and hide a successful append behind a 500. Matches
        // `handle_materialized_records`' log-and-continue posture.
        if appended_any && let Err(e) = self.rebuild_space_projection(space_id, space_id).await {
            error!(
                error = %e,
                %space_id,
                appended = outcome.appended,
                "re-seed: appended records but projection failed (records durable; projection will heal on next fold)"
            );
        }

        Ok(outcome)
    }

    /// Serve a `SubscribeSignals` frame with one `SignalPage`.
    ///
    /// The frame form of catch-up, paging through the shared
    /// [`kutl_signals::catchup::page`], where the boundary rule lives — a page
    /// is `max` plus the trailing same-`physical_ms` group, and a resume
    /// filter of `physical_ms > cursor` therefore cannot split a millisecond.
    ///
    /// ONE page per subscribe: the client re-subscribes with the returned
    /// cursor for more. The relay keeps no cursor of its own; a retry
    /// re-sends the page the client asked for.
    ///
    /// The page is the reply to this connection's own request, so it rides
    /// the unbounded **own-ack** lane like every other reply and cannot
    /// overflow or pause anything; the bounded data lane is for live signals
    /// and document traffic. Backpressure on paging is the client's: it asks
    /// for the next page only after ingesting this one. Every subscribe is
    /// answered, with a page or with a `StaleSignalStream` — never silence.
    ///
    /// Listening is presence: the entry this makes in `listeners` is what
    /// puts the connection in the space (signal fan-out, lifecycle broadcasts,
    /// `list_participants`). Live delivery itself happens in the fan-out
    /// paths; this serves the backlog page.
    pub(super) async fn handle_subscribe_signals(
        &mut self,
        conn_id: ConnId,
        msg: &sync::SubscribeSignals,
    ) {
        /// Records per `SignalPage`, before the trailing same-`physical_ms`
        /// group the pager adds to keep a millisecond whole.
        const SIGNAL_PAGE_SIZE: usize = 100;

        // `authorize_conn`, the WS-path authorizer: it carries the connection's
        // PAT context, which `authorize_space` on a bare DID does not. The
        // token it returns is the parsed space id: authorization is the one
        // place a space id is checked, so nothing downstream re-parses it.
        let Ok(authorized) = self.authorize_conn(conn_id, &msg.space_id).await else {
            // A refusal ends any listening this connection was doing here:
            // the notice says it is not a listener, and the set must agree.
            if let Some(conns) = self.listeners.get_mut(&msg.space_id) {
                conns.remove(&conn_id);
                if conns.is_empty() {
                    self.listeners.remove(&msg.space_id);
                }
            }
            self.send_stale_signal_stream(
                conn_id,
                &msg.space_id,
                sync::StaleStreamReason::NotAuthorized,
                "not authorized for this space",
            );
            return;
        };

        // REGISTER FIRST, then backfill. The two answer different questions:
        // registration is "I am listening to this space from now on", which
        // needs nothing but an authorized connection; the page is "and here
        // is what you missed", which needs a record log and a space id the
        // log can key on. Gating registration on the page would mean a space
        // with no records — or a momentarily full data lane — silently left
        // the listener out of live delivery, which is the exact failure mode
        // this set exists to remove. A re-subscribe sets a paused stream
        // flowing again.
        self.listeners
            .entry(msg.space_id.clone())
            .or_default()
            .insert(conn_id, crate::relay::SignalStream::Flowing);

        // A PAGE's worth, not the whole space. `+ 1` because
        // `page` reports `more` as "the slice held records past the cut" and
        // cannot otherwise tell a full page from the last one.
        let records = match self
            .record_log
            .list(authorized.uuid(), msg.cursor.as_ref(), SIGNAL_PAGE_SIZE + 1)
            .await
        {
            Ok(records) => records,
            Err(e) => {
                error!(space_id = %msg.space_id, error = %e, "failed to load records for SubscribeSignals");
                self.send_stale_signal_stream(
                    conn_id,
                    &msg.space_id,
                    sync::StaleStreamReason::RecordsUnavailable,
                    "signal records could not be loaded; re-subscribe",
                );
                return;
            }
        };
        // NOTE: an empty log still gets a page. Silence would be
        // indistinguishable from a dropped frame, and would leave a subscriber
        // with no event to hang "I am caught up" on — against a fresh relay
        // that silence stalls the daemon's re-seed push entirely
        // (`test_signal_catch_up_converges_two_daemons` pins this). An empty
        // page with `more: false` says the same thing, out loud.
        let page = kutl_signals::catchup::page(&records, msg.cursor.as_ref(), SIGNAL_PAGE_SIZE);
        let cursor = page.high_water.clone();
        let frame = sync::SyncEnvelope {
            payload: Some(sync::sync_envelope::Payload::SignalPage(sync::SignalPage {
                records: page.records.into_iter().cloned().collect(),
                cursor,
                more: page.more,
            })),
        };
        // The page is the reply to this connection's own request, so it
        // rides the own-ack lane like every other reply: it lands whatever
        // the data lane holds. On the data lane it would be the one reply
        // that can fail, and the failure feeds itself: a page that does not
        // fit pauses the stream and tells the client, the client
        // re-subscribes at once, and the page does not fit again, a loop
        // that holds until the socket writer drains the lane, with a segment
        // read per turn. On the own-ack lane a re-subscribe cannot earn a
        // pause, so a pause takes a live signal on a congested lane, and
        // notices come no faster than signals do. The lane stays bounded by
        // the client: it asks for the next page only after ingesting this
        // one.
        self.send_ack(conn_id, &encode_envelope(&frame));
    }

    /// Tell a connection its signal stream is not going to deliver, and why:
    /// `cause` is the typed verdict a client branches on (re-subscribe at
    /// once for a pause, stop for a refusal, back off while records are
    /// unavailable); `reason` is prose for its log line.
    ///
    /// This notice is `SubscribeSignals`' typed reply, so a refused subscribe
    /// names the space it concerns instead of riding a connection-wide frame,
    /// and a connection carrying unrelated document sync keeps it.
    ///
    /// It rides the ACK lane — unbounded, cannot drop — never the lane that
    /// just overflowed. A notice storm down a congested bounded lane would
    /// leave the paused listeners stranded with nothing to retry.
    pub(super) fn send_stale_signal_stream(
        &self,
        conn_id: ConnId,
        space_id: &str,
        cause: sync::StaleStreamReason,
        reason: &str,
    ) {
        let notice = sync::SyncEnvelope {
            payload: Some(sync::sync_envelope::Payload::StaleSignalStream(
                sync::StaleSignalStream {
                    space_id: space_id.to_owned(),
                    reason: reason.to_owned(),
                    cause: cause.into(),
                },
            )),
        };
        self.send_ack(conn_id, &encode_envelope(&notice));
    }

    /// Ingest a batch of marker-materialized records built by the
    /// [`RecordMaterializingObserver`](crate::markers::materialize::RecordMaterializingObserver).
    ///
    /// The observer already built, HLC-stamped, and (with a relay identity)
    /// MATERIALIZER-signed each record on the merge path, then handed the batch
    /// over the dedicated UNBOUNDED materialize channel (never dropped under
    /// backpressure — see the observer's module docs); the actor drains it here
    /// on a later loop iteration. The actor is the SOLE segment writer, so it
    /// validates, appends, reprojects, and broadcasts each record — mirroring
    /// the re-seed ingest loop. Fire-and-forget from the observer's side (no
    /// reply): a record that fails the well-formedness gate or the append is
    /// dropped with an error log rather than halting the batch, since there is
    /// no caller to signal failure to and the merge itself already succeeded.
    ///
    /// A non-UUID `space_id` (never happens for real spaces — ids are UUIDs)
    /// or a relay with no record log configured drops the whole batch.
    pub(super) async fn handle_materialized_records(
        &mut self,
        space_id: &str,
        records: Vec<Signal>,
    ) {
        if !self.record_log.is_configured() {
            debug!(
                %space_id,
                count = records.len(),
                "no record log configured; dropping materialized records"
            );
            return;
        }
        // Checked once for the batch rather than per record: the seam parses it
        // too, but every record in a batch shares the space, so failing here
        // logs the cause once instead of N identical times.
        if uuid::Uuid::parse_str(space_id).is_err() {
            error!(%space_id, "materialized records: space_id is not a uuid; dropping batch");
            return;
        }
        for record in records {
            let signal_id = record.id.clone();
            let record_id = record.record_id.clone();
            // Through the materializer's seam, which runs the well-formedness
            // gate and then appends.
            //
            // KNOWN LIMITATION: the observer's tracker
            // already advanced past this record before the batch was sent, so a
            // hard failure here (a rejected record, or disk ENOSPC) loses THIS
            // record permanently — logged, but not re-derivable. This is an infra
            // failure, not silent backpressure (the unbounded materialize channel
            // closes that gap). A later refinement should advance tracker state
            // only after a confirmed append. Drop-and-continue is what keeps one
            // bad record from costing the rest of the batch.
            if let Err(e) = self
                .append_materialized_record(&record, &signal_id, space_id)
                .await
            {
                error!(error = %e, %space_id, signal_id = %signal_id, record_id = %record_id, "failed to append materialized record — dropping");
                continue;
            }
            // A marker-materialized CREATED can land AFTER the
            // document was already soft-deleted (materialization rides the
            // unbounded channel and drains on a later loop iteration, past the
            // delete cascade's fold). Projecting it Open would leave a visible
            // orphan signal on a deleted document, which the non-destructive
            // startup reconcile then perpetuates. Guard it: if the record's
            // document is soft-deleted, tombstone this signal instead of
            // projecting it Open. (A genuine later edit REVIVES the doc and the
            // edit-revive cascade reopens it, so this never wrongly hides a live
            // doc's signal.)
            if self.record_document_is_soft_deleted(space_id, &record)
                && let Err(e) = self
                    .tombstone_orphan_materialized_record(space_id, &record)
                    .await
            {
                error!(error = %e, %space_id, signal_id = %signal_id, record_id = %record_id, "failed to tombstone materialized record on a soft-deleted document");
            }
            // Nothing follows. `append_materialized_record` above appended,
            // projected AND replicated in one act; a step here could only
            // repeat one of them, and on a backend whose fold-backed rebuild
            // is `None` (Postgres) a projection without a preceding append
            // would leave marker-derived signals in no table at all.
        }
    }

    /// Whether the document a materialized `record` attaches to is soft-deleted
    /// in the registry. `false` when the record carries no `document_id`
    /// (space-level) or the document is absent/active — the record then projects
    /// normally. Used by [`Self::handle_materialized_records`] to divert an
    /// orphan CREATED that lands after its document's delete cascade.
    fn record_document_is_soft_deleted(&self, space_id: &str, record: &Signal) -> bool {
        let Some(document_id) = record.document_id.as_deref() else {
            return false;
        };
        self.registries
            .get(space_id)
            .and_then(|reg| reg.get_any(document_id))
            .and_then(|entry| entry.deleted_at)
            .is_some()
    }

    /// Tombstone the signal a just-appended orphan materialized `record`
    /// created: append a TOMBSTONED transition for it and reproject once, so the
    /// fold hides it rather than exposing an Open signal on a soft-deleted
    /// document. The transition's actor is the record's own author (the
    /// materializing author); there is no separate deleter identity on this
    /// path.
    async fn tombstone_orphan_materialized_record(
        &mut self,
        space_id: &str,
        record: &Signal,
    ) -> Result<(), crate::change_backend::ChangeError> {
        // `build_and_append_transition` ends in the admission seam, which
        // appends, projects and replicates — every replica gets the tombstone
        // like any other record, so a peer's fold hides the orphan too.
        // Silent: the signal is born already hidden, so there is nothing
        // current to tell anyone about.
        self.build_and_append_transition(
            &record.id,
            space_id,
            record.document_id.as_deref(),
            SignalEventType::Tombstoned,
            &TransitionAuthor::relay_initiated(&record.author_did),
            Announce::Silent,
        )
        .await
        .map(|_tombstone| ())
    }

    /// Shared core of flag signal creation.
    ///
    /// Uses `supplied_id` as the signal id when present (well-formed UUID
    /// pre-validated by the caller); otherwise mints a fresh v4 UUID. States
    /// the intent through a per-kind builder, then stamps and appends through
    /// the admission seam — which appends, projects, replicates to every
    /// signal subscriber, and announces, in that order, halting the whole
    /// emit on any failure so the caller sees the truth instead of a
    /// phantom-success broadcast that never landed. Returns the resolved
    /// signal id.
    ///
    /// Called from the WS path (`handle_signal`), the HTTP flag route, and
    /// the MCP path (`handle_mcp_create_flag`).
    ///
    /// `anchor_text` — the comment-kind posterity snapshot. `Some` for
    /// comment signals with an inline marker; ignored (and persisted as
    /// `None`) for other kinds. Carried on the record itself, so a rebuild
    /// from segments keeps it.
    ///
    /// `document_id` is `Some(id)` for a document-attached flag and `None` for a
    /// space-level flag (the CLI's default create). The record carries the value
    /// verbatim; readers that scope by document ignore a space-level flag.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn relay_flag_signal(
        &mut self,
        supplied_id: Option<&str>,
        space_id: &str,
        document_id: Option<&str>,
        author_did: &str,
        kind: i32,
        // The TYPED audience, not the deprecated `(audience_type, target_did)`
        // pair. Doors that still receive the pair convert at
        // their own boundary via `vocab::audience_from_untyped_checked`, which
        // refuses what the pair can express and the typed shape cannot. Taking
        // the pair here would make the doors that already HAVE a typed audience
        // flatten it on the way in and this function rebuild it on the way
        // out — through `audience_from_untyped`, whose whole job is to WIDEN an
        // unrecognized audience to space-wide. On a storage path that is a
        // repair; on this one it silently turns a caller's typo into a
        // notify-everybody broadcast.
        audience: sync::Audience,
        message: &str,
        anchor_text: Option<&str>,
        timestamp: i64,
        via_pat_id: Option<&str>,
    ) -> Result<String, crate::change_backend::ChangeError> {
        let signal_id = supplied_id.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);

        // --- State the intent through a per-kind builder, then
        // stamp + append. The builder owns the shape rules (audience
        // well-formedness, message length, and refusing the retired
        // FLAG_KIND_COMMENT), so this door cannot hand-roll a payload and cannot
        // set both the typed audience and the deprecated pair.
        //
        // Order: append to segments FIRST, then projection, then replication
        // and announcement. A failure at any stage halts the whole emit.
        let envelope = kutl_signals::authoring::RecordEnvelope {
            space_id: space_id.to_owned(),
            document_id: document_id.map(str::to_owned),
            signal_id: signal_id.clone(),
            timestamp,
        };
        // SPACE-with-a-target is unrepresentable here
        // rather than refused: the typed `Audience` has no arm for "space-wide
        // but addressed to someone", so a door cannot hand one
        // in. The rule lives at the boundary where the
        // pair still exists, in `audience_from_untyped_checked`, which is also
        // what keeps the WS and MCP doors agreeing about it.

        let kind_enum = sync::FlagKind::try_from(kind).map_err(|_| {
            // An unrecognized discriminant is an error, never coerced to a
            // default: `try_from(...).unwrap_or(Unspecified)` would silently
            // rewrite a bad kind into a valid flag.
            crate::change_backend::ChangeError::InvalidArgument {
                reason: format!("unknown flag kind discriminant {kind}"),
            }
        })?;
        // Dispatch on kind. A `comment` argument still arrives through this
        // door, but the RECORD it produces is a `CommentPayload`,
        // never a flag wearing the retired `FLAG_KIND_COMMENT`. The comment
        // record also carries `anchor_text` itself rather than leaving it
        // projection-only, so a rebuild carries it.
        let intent = if kind_enum == sync::FlagKind::Comment {
            kutl_signals::authoring::SignalIntent::comment(
                envelope,
                message.to_owned(),
                audience,
                anchor_text.map(str::to_owned),
            )
        } else {
            kutl_signals::authoring::SignalIntent::flag(
                envelope,
                kind_enum,
                message.to_owned(),
                audience,
            )
        }
        .map_err(|e| crate::change_backend::ChangeError::InvalidArgument {
            reason: e.to_string(),
        })?;
        self.stamp_and_append_created(
            &intent,
            &signal_id,
            space_id,
            author_did,
            via_pat_id,
            Announce::Feed,
        )
        .await?;
        // Nothing follows. The admission seam appended, projected, replicated
        // and announced in one act; a failure in any of them halted this
        // function above, so reaching this line means the flag is durable and
        // readable everywhere it should be.

        Ok(signal_id)
    }

    /// Emit a caller's transition for an existing signal, through one of two
    /// mechanisms the target's KIND selects:
    ///
    /// - **A decision** is marker-born — the document heading is the source
    ///   and the record is derived from it by the materializer — so the
    ///   transition is performed as the `?` ↔ `=` marker edit in the
    ///   document, attributed to the actor
    ///   ([`Self::flip_decision_marker`]). No record is written here; the
    ///   materializer mints CLOSED(RESOLVED)/REOPENED from the flip's own
    ///   merge, through the one lane that owns every document-born
    ///   transition. Writing the record directly instead would leave the
    ///   heading contradicting the record FOREVER: the tracker's per-document
    ///   baseline rebuilds from the document's own records, a single-signal
    ///   transition names no document, and the heading keeps its old marker —
    ///   so no later merge reconciles the two, and every surface that renders
    ///   the document shows open while the fold says closed.
    /// - **Every other kind** is author-born — the record is the source — so
    ///   the transition IS a record: build the payload-less transition
    ///   [`Signal`], stamp it with a strictly-monotonic HLC, and hand it to
    ///   the admission seam, which appends, projects, replicates and
    ///   announces it. On any failure the whole emit halts so the store never
    ///   diverges from what peers received.
    ///
    /// The single-transition path every caller-facing door funnels into (WS,
    /// HTTP, MCP close/reopen) — which is what makes the kind dispatch cover
    /// all of them at once. It takes [`SignalTransitionEvent`] — close or
    /// reopen, nothing else — because the record arm declares
    /// [`Announce::Feed`]: the two caller-initiated transitions are exactly
    /// the set that announces, so taking the wider event type would let a
    /// system-minted TOMBSTONED reach the feed by arriving through this door.
    /// The cascades and the orphan tombstone mint through
    /// [`Self::build_and_append_transition`] directly and declare their
    /// silence there.
    ///
    /// Recorded transitions carry no signal payload — a records-capable
    /// receiver folds them; legacy receivers ignore payload-less signals. The
    /// close note rides the record, so it survives a rebuild from segments;
    /// on a decision the note lands as body text under the heading instead.
    pub(super) async fn emit_transition_record(
        &mut self,
        signal_id: &str,
        space_id: &str,
        event: SignalTransitionEvent,
        author: &TransitionAuthor<'_>,
    ) -> Result<TransitionOutcome, crate::change_backend::ChangeError> {
        if let Some(target) = self.decision_flip_target(signal_id, space_id).await? {
            // A decision close is the resolve flip and nothing else: flipping
            // `?` to `=` under a non-resolved reason would durably record
            // RESOLVED against the caller's stated intent. Withdrawal is a
            // heading removal, which is an ordinary document edit.
            if matches!(event, SignalTransitionEvent::Closed)
                && author
                    .close_reason
                    .is_some_and(|r| r != CloseReason::Resolved)
            {
                return Err(crate::change_backend::ChangeError::InvalidArgument {
                    reason: "a decision is closed by editing its document: close supports \
                             only reason resolved (the ? to = marker flip); remove the \
                             heading to withdraw it"
                        .to_owned(),
                });
            }
            // The record path bounds the note inside intent assembly; the
            // flip path must not become the way around that bound.
            if let Some(note) = author.note
                && note.chars().count() > kutl_signals::authoring::MAX_BODY_CHARS
            {
                return Err(crate::change_backend::ChangeError::InvalidArgument {
                    reason: format!(
                        "note exceeds {} characters",
                        kutl_signals::authoring::MAX_BODY_CHARS
                    ),
                });
            }
            let to_state = match event {
                SignalTransitionEvent::Closed => crate::markers::decisions::DecisionState::Resolved,
                SignalTransitionEvent::Reopened => crate::markers::decisions::DecisionState::Open,
            };
            match self
                .flip_decision_marker(signal_id, space_id, &target, to_state, author)
                .await?
            {
                FlipOutcome::Edited => return Ok(TransitionOutcome::MarkerFlipped),
                FlipOutcome::DocAlreadyAgrees => {
                    if target.fold_state == Some(to_state) {
                        // Doc and fold both already read the target: the
                        // transition happened; acking it again is idempotent.
                        return Ok(TransitionOutcome::MarkerFlipped);
                    }
                    // The document already reads the target but the fold does
                    // not — the transition record was lost (e.g. a
                    // materialized append that failed mid-batch). The
                    // document is the source and already agrees with the
                    // caller's intent, so minting the record directly HEALS:
                    // doc and record agree after the write, which is the
                    // state the flip exists to preserve. Fall through to the
                    // record arm.
                }
            }
        }

        let event = match event {
            SignalTransitionEvent::Closed => SignalEventType::Closed,
            SignalTransitionEvent::Reopened => SignalEventType::Reopened,
        };
        let transition = self
            .build_and_append_transition(signal_id, space_id, None, event, author, Announce::Feed)
            .await?;
        Ok(TransitionOutcome::Recorded(transition.record_id))
    }

    /// Classify a transition target as a decision, returning what the marker
    /// flip needs when it is one.
    ///
    /// Reads the space's durable records and folds them — the same basis the
    /// tracker re-derives its known-sets from — so the answer does not depend
    /// on a projection being configured. `Ok(None)` routes the caller to the
    /// record path: the signal is not a decision, or this relay has no record
    /// log (nothing durable for a document to contradict), or the id is not
    /// in the fold (the record path keeps its existing unknown-id behavior).
    /// A load FAILURE is an error, not `None`: falling back to the record
    /// path on I/O trouble would write the derived copy against the source —
    /// the divergence the flip exists to prevent.
    async fn decision_flip_target(
        &mut self,
        signal_id: &str,
        space_id: &str,
    ) -> Result<Option<DecisionFlipTarget>, crate::change_backend::ChangeError> {
        if !self.record_log.is_configured() {
            return Ok(None);
        }
        let Ok(space_uuid) = uuid::Uuid::parse_str(space_id) else {
            return Ok(None);
        };
        // Fast path: the projection, when configured, answers "is this a
        // decision?" in one row read — sparing every flag and reply close the
        // whole-space fold below. Only a POSITIVE non-decision answer skips
        // the fold; a projection error or missing row falls through to it,
        // because concluding non-decision wrongly would take the record path
        // and write the derived copy against the source.
        if let Some(backend) = self.record_log.reads()
            && let Ok(detail) = backend.get_signal_detail(space_id, signal_id).await
            && detail.signal_type != "decision"
        {
            return Ok(None);
        }
        let records = self.record_log.load_space(space_uuid).await.map_err(|e| {
            crate::change_backend::ChangeError::Internal(format!(
                "cannot load records to classify signal {signal_id}: {e}"
            ))
        })?;
        let mut fold = kutl_signals::fold::SpaceSignalState::default();
        for record in records {
            fold.apply(record);
        }
        let Some((_, state)) = fold.iter().find(|(id, _)| id.as_str() == signal_id) else {
            return Ok(None);
        };
        // Kind is identity: classified by the BIRTH payload. The title hash
        // is read from the fold's CONTENT track, which follows renames — the
        // heading is addressed by the title it carries NOW.
        let Some(sync::signal::Payload::Decision(_)) = &state.created.payload else {
            return Ok(None);
        };
        let Some(sync::signal::Payload::Decision(payload)) = &state.content().payload else {
            return Ok(None);
        };
        // The signal IS a decision from here on, so a defect in its record is
        // an error, never a silent fall-through to the record path — that
        // would write the derived copy against the source, the divergence the
        // flip exists to prevent. Neither field can be malformed by an
        // in-tree producer; this is crash-residue or foreign-record handling.
        let Some(document_id) = state.created.document_id.clone() else {
            return Err(crate::change_backend::ChangeError::Internal(format!(
                "decision {signal_id} carries no document_id; refusing the transition \
                 rather than writing a record its document cannot agree with"
            )));
        };
        let Ok(title_hash) = payload.title_hash.parse::<u64>() else {
            return Err(crate::change_backend::ChangeError::Internal(format!(
                "decision {signal_id} carries unparseable title_hash {:?}; refusing the \
                 transition rather than writing a record its document cannot agree with",
                payload.title_hash
            )));
        };
        let fold_state = crate::markers::decisions::decision_state_from_fold(
            &state.status,
            state.close_reason(),
        );
        Ok(Some(DecisionFlipTarget {
            document_id,
            title_hash,
            fold_state,
        }))
    }

    /// Perform a decision transition as the marker edit it is: splice the
    /// heading's marker (`=` for resolve, `?` for reopen) in the decision's
    /// document, as an edit attributed to the actor, and let the flip's merge
    /// drive the materializer. The note, when given, lands blockquoted under
    /// the heading — never in the title, whose text anchors the decision's
    /// identity — and it lands even when the heading already reads the target
    /// marker, because a caller's rationale must not vanish behind a success
    /// ack.
    ///
    /// The heading is located by the fold's current title hash, so a rename
    /// merged but not yet folded (the materialized batch drains a loop
    /// iteration behind the merge) can transiently surface as not-found; a
    /// retry lands. One matcher, deliberately: a fallback matcher here would
    /// be a second opinion on decision identity.
    async fn flip_decision_marker(
        &mut self,
        signal_id: &str,
        space_id: &str,
        target: &DecisionFlipTarget,
        to_state: crate::markers::decisions::DecisionState,
        author: &TransitionAuthor<'_>,
    ) -> Result<FlipOutcome, crate::change_backend::ChangeError> {
        use crate::markers::decisions::{DecisionState, MarkerSplice, splice_decision_marker};

        self.hydrate_doc_for_edit(space_id, &target.document_id)
            .await
            .map_err(crate::change_backend::ChangeError::Internal)?;
        let key = super::doc_load::DocKey {
            space_id: space_id.to_owned(),
            document_id: target.document_id.clone(),
        };
        let content = match self.documents.get(&key).map(|slot| &slot.content) {
            Some(super::doc_load::DocContent::Text(doc)) => doc.content(),
            Some(super::doc_load::DocContent::Blob(_)) => {
                return Err(crate::change_backend::ChangeError::Internal(format!(
                    "decision {signal_id}'s document {} is not a text document",
                    target.document_id
                )));
            }
            // An absent or empty slot parses as a document with no headings,
            // which the splice reports as the heading being gone.
            Some(super::doc_load::DocContent::Empty) | None => String::new(),
        };

        // A whitespace-only note carries nothing to place; treat it as
        // absent, matching the record path's non-empty note requirement.
        let note = author.note.filter(|n| !n.trim().is_empty());
        let new_content = match splice_decision_marker(&content, target.title_hash, to_state, note)
        {
            MarkerSplice::Updated(new_content) => new_content,
            MarkerSplice::AlreadyInTargetState => return Ok(FlipOutcome::DocAlreadyAgrees),
            // InvalidArgument, not NotFound: the situation is
            // caller-addressable (edit the document), and the doors
            // surface InvalidArgument as a client error — NotFound would
            // come back as an internal error.
            MarkerSplice::NotFound => {
                return Err(crate::change_backend::ChangeError::InvalidArgument {
                    reason: format!(
                        "no heading for decision {signal_id} in document {}; it may \
                             have been removed (re-add the heading by editing the \
                             document), or renamed by a merge still settling (retry)",
                        target.document_id
                    ),
                });
            }
        };

        let intent = match to_state {
            DecisionState::Resolved => "resolve decision",
            DecisionState::Open => "reopen decision",
        };
        // A minted uuid as the CRDT agent name: unique across relays (unlike
        // a shared constant, whose (agent, seq) pairs would collide between
        // two relays flipping in one space) and within diamond-types' agent
        // name budget (unlike a DID).
        let agent_seed = uuid::Uuid::new_v4().to_string();
        self.apply_relay_text_edit(
            &agent_seed,
            space_id,
            &target.document_id,
            author.actor_did,
            author.via_pat_id.map(str::to_owned),
            intent,
            None,
            &new_content,
        )
        .await
        .map_err(|e| {
            crate::change_backend::ChangeError::Internal(format!(
                "marker flip edit failed for decision {signal_id}: {e}"
            ))
        })?;
        Ok(FlipOutcome::Edited)
    }

    /// Build a signal *transition* record (CLOSED / REOPENED /
    /// TOMBSTONED) via the shared
    /// [`assemble_record`](kutl_signals::authoring::assemble_record), stamp it
    /// with a strictly-monotonic HLC off the shared clock, relay-attest it, and
    /// append it to the space's durable segments.
    /// Returns the built record for the caller to broadcast.
    ///
    /// The `build + stamp + append` half of the transition emit, shared by the
    /// single-signal path ([`Self::emit_transition_record`]) and the batched
    /// document-cascade path ([`Self::cascade_document_signal_transition`]) so
    /// the two cannot drift on how a transition record is minted. The caller
    /// owns the ordering-sensitive rest: project ONCE and broadcast.
    ///
    /// `document_id` attaches the transition to its document for traceability
    /// (the fold derives status from the transition and identity from the
    /// CREATED record, so this field is advisory, not load-bearing). `None` for
    /// the single-signal close/reopen path, `Some(doc)` for the cascade.
    ///
    /// `announce` is where the single path and the cascades part ways — see
    /// [`Announce`]. The same record shape is minted either way, so nothing
    /// downstream can reconstruct the difference; the caller declares it.
    async fn build_and_append_transition(
        &mut self,
        signal_id: &str,
        space_id: &str,
        document_id: Option<&str>,
        event: SignalEventType,
        author: &TransitionAuthor<'_>,
        announce: Announce,
    ) -> Result<Signal, crate::change_backend::ChangeError> {
        // Strictly-monotonic HLC so records emitted in sequence order by HLC
        // (not a random `record_id` tiebreak) even within one millisecond.
        let now_ms = kutl_core::env::now_ms();
        let relay_hlc: kutl_proto::sync::Hlc = self
            .signal_clock
            .lock()
            .expect("signal clock lock poisoned")
            .tick(now_ms.cast_unsigned())
            .into();
        // Assemble via the SHARED `assemble_record` seam (structural
        // byte-parity), then relay-attest. `author_did == actor_did
        // == closer` here, matching the seam's contract, so relay-minted
        // transitions agree on canonical bytes with legacy client-authored
        // ones. `assemble_record` applies `close_reason` on CLOSED
        // only — matching the caller contract (a reason is passed for CLOSE only).
        // The transition builder rejects CREATED/UNSPECIFIED, so a transition
        // path cannot mint a create. A note-less transition keeps a `None`
        // payload, so its canonical bytes match a legacy note-less
        // transition; a note becomes a `TransitionPayload`,
        // which is what makes the note survive a rebuild from segments rather
        // than living only on the observer event.
        let intent = kutl_signals::authoring::SignalIntent::transition(
            kutl_signals::authoring::RecordEnvelope {
                space_id: space_id.to_owned(),
                document_id: document_id.map(str::to_owned),
                signal_id: signal_id.to_owned(),
                timestamp: now_ms,
            },
            event,
            author.close_reason,
            author.note.map(str::to_owned),
        )
        .map_err(|e| crate::change_backend::ChangeError::InvalidArgument {
            reason: e.to_string(),
        })?;
        let mut transition =
            kutl_signals::authoring::assemble_record(&intent, author.actor_did, relay_hlc);
        crate::signal_record::attest_on_ingest(
            &mut transition,
            self.signing_identity.as_deref(),
            now_ms,
        );
        self.append_authored_record(
            &transition,
            signal_id,
            space_id,
            author.via_pat_id,
            announce,
        )
        .await?;
        Ok(transition)
    }

    /// Cascade ONE lifecycle transition (`event`) across every signal id in
    /// `signal_ids`, all attached to `document_id` (the deletion ladder).
    ///
    /// The batched sibling of [`Self::emit_transition_record`]: each record
    /// goes through the admission seam as it is built — appended, projected
    /// incrementally (one row write per record, not a whole-space rebuild)
    /// and replicated — with silence declared per record. Used for both
    /// cascade directions:
    /// - [`SignalEventType::Tombstoned`] on document soft-delete (hide the doc's
    ///   visible signals), and
    /// - [`SignalEventType::Reopened`] on document revive (bring them back).
    ///
    /// A no-op (no append, no projection, no broadcast) when `signal_ids` is
    /// empty. On any append/projection failure the whole cascade halts with the
    /// error so the store never diverges from what peers received — records
    /// already appended stay durable and heal on the next fold.
    async fn cascade_document_signal_transition(
        &mut self,
        space_id: &str,
        document_id: &str,
        actor_did: &str,
        event: SignalEventType,
        signal_ids: &[String],
    ) -> Result<(), crate::change_backend::ChangeError> {
        if signal_ids.is_empty() {
            return Ok(());
        }
        // A cascade is relay-initiated: no reason, no note, no PAT — the actor
        // is whoever deleted or revived the document.
        let author = TransitionAuthor::relay_initiated(actor_did);
        for signal_id in signal_ids {
            // Silent: the document event announces the covering action, and a
            // per-signal announcement here would be a storm of N feed rows
            // for one delete or revive.
            self.build_and_append_transition(
                signal_id,
                space_id,
                Some(document_id),
                event,
                &author,
                Announce::Silent,
            )
            .await?;
        }
        // Appended, projected AND replicated by the admission seam as each was
        // built, so the cascade has no pass of its own for any of the three.
        Ok(())
    }

    /// Tombstone every currently-visible signal attached to `document_id` on
    /// document soft-delete (deletion-ladder cascade rung 1).
    ///
    /// Sources the target ids from the SEGMENT FOLD, not the projection:
    /// a signal whose create-time projection failed is absent from
    /// the projection but its CREATED record is durable in segments — the
    /// non-destructive startup reconcile would otherwise re-fold and resurrect it
    /// as an Open signal on a deleted document. The fold sees every visible
    /// signal directly attached to the document PLUS every reply whose parent
    /// chain lands on one of them (replies carry no `document_id`, so
    /// they are reachable only through their `parent_signal_id`). A no-op on a
    /// relay with no record log configured, or when the document has no
    /// visible signals.
    ///
    /// `actor_did` is the deleting caller's authenticated DID.
    pub(super) async fn cascade_document_delete_signals(
        &mut self,
        space_id: &str,
        document_id: &str,
        actor_did: &str,
    ) -> Result<(), crate::change_backend::ChangeError> {
        // Gated on the record log: the cascade appends TOMBSTONED records to
        // it, whatever substrate holds it.
        if !self.record_log.is_configured() {
            return Ok(());
        }
        let ids = self
            .document_signal_ids_by_status(space_id, document_id)
            .await?;
        self.cascade_document_signal_transition(
            space_id,
            document_id,
            actor_did,
            SignalEventType::Tombstoned,
            &ids.visible,
        )
        .await
    }

    /// [`Self::cascade_document_delete_signals`] with the caller-site
    /// log-and-continue posture: on failure, WARN and swallow. The document
    /// delete is already durable, so failing the whole unregister would strand a
    /// committed delete; the signal projection heals on the next fold.
    pub(super) async fn cascade_delete_signals_logged(
        &mut self,
        space_id: &str,
        document_id: &str,
        actor_did: &str,
    ) {
        if let Err(e) = self
            .cascade_document_delete_signals(space_id, document_id, actor_did)
            .await
        {
            warn!(
                error = %e,
                %space_id,
                %document_id,
                "document delete committed but signal tombstone cascade failed (heals on next fold)"
            );
        }
    }

    /// Tombstone EVERY currently-visible signal in a space on bulk space-delete
    /// (the deletion ladder). `handle_unregister_space`
    /// soft-deletes every document but never touches the `signals` projection,
    /// so without this a durable relay leaves every doc's signals Open. Since
    /// every document is being soft-deleted, the correct set is the whole space's
    /// visible signals (no per-document scoping needed) — folded ONCE from
    /// segments (not the projection, for the same reason as
    /// [`Self::cascade_document_delete_signals`]). A no-op with no durable
    /// `signal_store` or when the space has no visible signals.
    ///
    /// `actor_did` is the deleting caller's authenticated DID.
    async fn cascade_space_delete_signals(
        &mut self,
        space_id: &str,
        actor_did: &str,
    ) -> Result<(), crate::change_backend::ChangeError> {
        use kutl_signals::fold::{SignalStatus, SpaceSignalState};

        if !self.record_log.is_configured() {
            return Ok(());
        }
        let space_uuid = uuid::Uuid::parse_str(space_id).map_err(|_| {
            crate::change_backend::ChangeError::Internal(format!(
                "space_id must be a UUID; got {space_id:?}"
            ))
        })?;
        let records = self.record_log.load_space(space_uuid).await.map_err(|e| {
            error!(%space_id, error = %e, "failed to load records for space-delete cascade");
            crate::change_backend::ChangeError::Internal(e.to_string())
        })?;
        let mut state = SpaceSignalState::default();
        for record in records {
            state.apply(record);
        }
        let visible: Vec<String> = state
            .iter()
            .filter(|(_, s)| s.status != SignalStatus::Tombstoned)
            .map(|(id, _)| id.clone())
            .collect();
        if visible.is_empty() {
            return Ok(());
        }
        // The transition's advisory `document_id` is `None` — the batch spans
        // many documents and the fold keys status by signal id, not the
        // transition's document field.
        let author = TransitionAuthor::relay_initiated(actor_did);
        for signal_id in &visible {
            // Silent: the space event announces the covering delete.
            self.build_and_append_transition(
                signal_id,
                space_id,
                None,
                SignalEventType::Tombstoned,
                &author,
                Announce::Silent,
            )
            .await?;
        }
        // Appended, projected and replicated as each was built; see
        // `cascade_document_signal_transition`.
        Ok(())
    }

    /// [`Self::cascade_space_delete_signals`] with the caller-site
    /// log-and-continue posture: on failure, WARN and swallow. The bulk
    /// document delete is already durable, so failing the space unregister would
    /// strand it; the signal projection heals on the next fold.
    pub(super) async fn cascade_space_delete_signals_logged(
        &mut self,
        space_id: &str,
        actor_did: &str,
    ) {
        if let Err(e) = self.cascade_space_delete_signals(space_id, actor_did).await {
            warn!(
                error = %e,
                %space_id,
                "space delete committed but signal tombstone cascade failed (heals on next fold)"
            );
        }
    }

    /// Reopen every tombstoned signal attached to `document_id` on document
    /// revive (deletion-ladder cascade rung 1 undo).
    ///
    /// Tombstoned signals are absent from the projection (it deleted their
    /// rows), so this folds the space's segments and collects the signal ids
    /// whose folded status is `Tombstoned` AND whose CREATED record is attached
    /// to `document_id` — directly OR through a reply chain (replies
    /// carry no `document_id`, so the delete tombstoned them via their parent
    /// link, and the revive must reopen the same set) — then cascades a REOPENED
    /// transition over them. A no-op with no durable `signal_store` or when the
    /// document has no tombstoned signals.
    ///
    /// **Deliberate semantic:** a revive brings signals back as Open (a
    /// blanket REOPENED), NOT their exact pre-delete Open/Closed state — a
    /// signal closed before the doc-delete comes back Open.
    ///
    /// **Assumption:** reopening ALL of the doc's tombstoned signals is
    /// correct BECAUSE doc-delete is the ONLY tombstone source today. If
    /// another tombstone source arrives (e.g. daemon-authored
    /// tombstones), this must distinguish "tombstoned by doc-delete" from other
    /// tombstones (e.g. a reason on the tombstone record) to avoid over-reopening.
    ///
    /// `actor_did` is the reviving caller's authenticated DID.
    pub(super) async fn cascade_document_revive_signals(
        &mut self,
        space_id: &str,
        document_id: &str,
        actor_did: &str,
    ) -> Result<(), crate::change_backend::ChangeError> {
        let ids = self
            .document_signal_ids_by_status(space_id, document_id)
            .await?;
        self.cascade_document_signal_transition(
            space_id,
            document_id,
            actor_did,
            SignalEventType::Reopened,
            &ids.tombstoned,
        )
        .await
    }

    /// [`Self::cascade_document_revive_signals`] with the caller-site
    /// log-and-continue posture: on failure, WARN and swallow. The document is
    /// already live (the edit revived it), so failing the edit would lose the
    /// merge; the signal reopen heals on the next fold.
    pub(super) async fn cascade_revive_signals_logged(
        &mut self,
        space_id: &str,
        document_id: &str,
        actor_did: &str,
    ) {
        if let Err(e) = self
            .cascade_document_revive_signals(space_id, document_id, actor_did)
            .await
        {
            warn!(
                error = %e,
                %space_id,
                %document_id,
                "edit revived document but signal reopen cascade failed (heals on next fold)"
            );
        }
    }

    /// Fold the space's segments ONCE and split the document's signals into the
    /// two id sets the deletion ladder needs: `visible` (currently non-tombstoned
    /// — the delete cascade tombstones these) and `tombstoned` (the revive cascade
    /// reopens these). Delete and revive share this single fold pass so they can
    /// never disagree on which signals belong to the document.
    ///
    /// Membership is computed structurally, so a signal reaches a set whether it
    /// is directly attached (its CREATED's `document_id == Some(document_id)`) OR
    /// it is a reply whose `parent_signal_id` chain terminates on a
    /// directly-attached signal (replies carry no `document_id`).
    /// Reply parents are resolved transitively across the whole fold, so a
    /// reply-to-a-reply is covered; the walk is bounded by the number of folded
    /// signals (each is visited at most once) and cycle-safe via a visited set.
    ///
    /// Empty (and a no-load) when there is no durable `signal_store`.
    async fn document_signal_ids_by_status(
        &mut self,
        space_id: &str,
        document_id: &str,
    ) -> Result<DocumentSignalIds, crate::change_backend::ChangeError> {
        use std::collections::{HashMap, HashSet};

        use kutl_proto::sync::signal::Payload;
        use kutl_signals::fold::{SignalStatus, SpaceSignalState};

        if !self.record_log.is_configured() {
            return Ok(DocumentSignalIds::default());
        }
        let space_uuid = uuid::Uuid::parse_str(space_id).map_err(|_| {
            crate::change_backend::ChangeError::Internal(format!(
                "space_id must be a UUID; got {space_id:?}"
            ))
        })?;
        let records = self.record_log.load_space(space_uuid).await.map_err(|e| {
            error!(%space_id, error = %e, "failed to load records for document-signal cascade");
            crate::change_backend::ChangeError::Internal(e.to_string())
        })?;
        let mut state = SpaceSignalState::default();
        for record in records {
            state.apply(record);
        }

        // Signals directly attached to the document (the cascade roots).
        // `parent`: reply-child → parent signal, for the transitive reply walk.
        let mut roots: HashSet<String> = HashSet::new();
        let mut parent: HashMap<String, String> = HashMap::new();
        for (id, signal_state) in state.iter() {
            if signal_state.created.document_id.as_deref() == Some(document_id) {
                roots.insert(id.clone());
            }
            if let Some(Payload::Reply(reply)) = signal_state.created.payload.as_ref()
                && !reply.parent_signal_id.is_empty()
            {
                parent.insert(id.clone(), reply.parent_signal_id.clone());
            }
        }

        // A signal belongs to the document iff it is a root or its reply-parent
        // chain reaches a root. Walk each signal's chain once, memoizing the
        // outcome; the visited set makes a malformed cycle terminate safely.
        let mut belongs: HashMap<String, bool> = HashMap::new();
        for id in state.iter().map(|(id, _)| id) {
            resolve_belongs(id, &roots, &parent, &mut belongs);
        }

        let mut ids = DocumentSignalIds::default();
        for (id, signal_state) in state.iter() {
            if belongs.get(id).copied() != Some(true) {
                continue;
            }
            match signal_state.status {
                SignalStatus::Tombstoned => ids.tombstoned.push(id.clone()),
                SignalStatus::Open | SignalStatus::Closed => ids.visible.push(id.clone()),
            }
        }
        Ok(ids)
    }
}

/// Whether `id`'s reply-parent chain reaches a document root, memoized into
/// `belongs`. A signal belongs when it is itself a root, or when the signal its
/// `parent_signal_id` points at belongs.
///
/// Walks the parent chain ITERATIVELY (not recursively): a reply-to-reply chain
/// is agent-controlled and unbounded, so native recursion would grow the actor
/// stack one frame per link and a deep chain could overflow it. The walk records
/// its path, stops at the first terminal (a root, an already-memoized node, a
/// dangling parent, or a cycle), then propagates that outcome to every node on
/// the path — O(chain length) work, O(1) stack. The `on_path` set makes a
/// malformed parent cycle resolve to `false` and terminate; a parent id absent
/// from the fold (dangling link) also resolves to `false`.
fn resolve_belongs(
    id: &str,
    roots: &std::collections::HashSet<String>,
    parent: &std::collections::HashMap<String, String>,
    belongs: &mut std::collections::HashMap<String, bool>,
) -> bool {
    if let Some(&known) = belongs.get(id) {
        return known;
    }
    let mut path: Vec<String> = Vec::new();
    let mut on_path: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut cursor = id.to_owned();
    let result = loop {
        if roots.contains(&cursor) {
            break true;
        }
        if let Some(&known) = belongs.get(&cursor) {
            break known;
        }
        if !on_path.insert(cursor.clone()) {
            break false; // cycle in the parent chain
        }
        path.push(cursor.clone());
        match parent.get(&cursor) {
            Some(parent_id) => cursor.clone_from(parent_id),
            None => break false, // dangling parent link
        }
    };
    // Memoize the terminal node (`cursor` — a root breaks BEFORE being pushed to
    // `path`, so it must be recorded here; for a memoized/path terminal this
    // re-sets the same value, harmlessly) plus every node walked to reach it.
    belongs.insert(cursor, result);
    for node in path {
        belongs.insert(node, result);
    }
    result
}

#[cfg(test)]
pub(super) mod catch_up_tests {
    use kutl_proto::sync::{Hlc, Signal, SignalEventType};

    /// A [`RelayObserver`](crate::observer::RelayObserver) that records the
    /// signal-record announcements it was handed.
    ///
    /// A door can append the record, write the projection, replicate and ack
    /// success while announcing nothing at all — every other assertion still
    /// passes. Only something that watches the observer can tell "closed"
    /// from "closed, and nobody was told".
    #[derive(Default)]
    pub(in crate::relay) struct RecordingObserver {
        records: std::sync::Mutex<Vec<crate::observer::SignalRecordEvent>>,
    }

    impl RecordingObserver {
        /// The announced records carrying `event`, in arrival order.
        pub(in crate::relay) fn with_event(&self, want: SignalEventType) -> Vec<Signal> {
            self.records
                .lock()
                .expect("records lock poisoned")
                .iter()
                .map(|e| e.record.clone())
                .filter(|r| r.event() == want)
                .collect()
        }

        /// Every announcement so far, regardless of event — and announcements
        /// ONLY, which holds because every other trait arm below is a
        /// deliberate no-op. The silence pins are meaningful only while that
        /// stays true; a test that wants to record reactions or document
        /// events needs its own recorder, not a wider arm here.
        ///
        /// The count exists because a per-event check is not a silence proof:
        /// "no TOMBSTONED was announced" is vacuous if some other event
        /// leaked instead.
        pub(in crate::relay) fn announcement_count(&self) -> usize {
            self.records.lock().expect("records lock poisoned").len()
        }

        /// The `via_pat_id` each announcement carried, in arrival order — the
        /// credential the feed byline names an agent by.
        pub(in crate::relay) fn announced_via_pats(&self) -> Vec<Option<String>> {
            self.records
                .lock()
                .expect("records lock poisoned")
                .iter()
                .map(|e| e.via_pat_id.clone())
                .collect()
        }
    }

    impl crate::observer::RelayObserver for RecordingObserver {
        fn on_signal_record(&self, event: crate::observer::SignalRecordEvent) {
            self.records
                .lock()
                .expect("records lock poisoned")
                .push(event);
        }

        // Deliberate no-ops — see `announcement_count`.
        fn on_blob_edited(&self, _event: crate::observer::MergedEvent) {}
        fn on_document_registered(&self, _event: crate::observer::DocumentRegisteredEvent) {}
        fn on_document_renamed(&self, _event: crate::observer::DocumentRenamedEvent) {}
        fn on_document_unregistered(&self, _event: crate::observer::DocumentUnregisteredEvent) {}
        fn on_reaction(&self, _event: crate::observer::ReactionEvent) {}
    }
    use uuid::Uuid;

    use crate::record_log::SegmentRecordLog;
    use crate::relay::signal_log::Announce;
    use crate::relay::{McpError, Relay, test_config};
    use crate::signal_store::SignalStore;

    /// Build a durable-store relay and append `records` to `space` so the
    /// catch-up handler has real segments to page.
    fn relay_with_records(space: Uuid, records: &[Signal]) -> (Relay, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let mut store = SignalStore::new(dir.path().to_path_buf());
        for rec in records {
            store.append(space, rec).unwrap();
        }
        // Records only — these exercise catch-up paging, which reads the log
        // and never touches a projection.
        relay.test_set_record_log(
            Some(std::sync::Arc::new(SegmentRecordLog::new(store))),
            None,
        );
        (relay, dir)
    }

    /// A `Created` record stamped at `(physical_ms, logical)`.
    fn record(space: Uuid, physical_ms: u64, logical: u32, rec: &str) -> Signal {
        record_with_id(
            space,
            &Uuid::new_v4().to_string(),
            physical_ms,
            logical,
            rec,
        )
    }

    /// Set a record's `actor_did` so it matches the authenticated caller the
    /// re-seed test pushes it as. Returns the record for chaining at the call
    /// site.
    fn as_actor(mut s: Signal, did: &str) -> Signal {
        s.actor_did = did.to_owned();
        s
    }

    /// A `Created` record with an explicit signal `id` (so a test can spoof a
    /// chosen signal id) stamped at `(physical_ms, logical)`.
    pub(in crate::relay) fn record_with_id(
        space: Uuid,
        id: &str,
        physical_ms: u64,
        logical: u32,
        rec: &str,
    ) -> Signal {
        let mut s = Signal {
            id: id.to_owned(),
            space_id: space.to_string(),
            record_id: rec.into(),
            author_did: "did:key:zAuthor".into(),
            hlc: Some(Hlc {
                physical_ms,
                logical,
                actor: vec![0u8; 16],
            }),
            payload: Some(kutl_proto::sync::signal::Payload::Flag(
                // The deprecated audience pair is the shape under test: these fixtures
                // stand in for records already on disk.
                #[allow(deprecated)]
                kutl_proto::sync::FlagPayload {
                    kind: 1,
                    audience_type: 2,
                    target_did: None,
                    message: "please review".into(),
                    audience: None,
                    anchor_text: None,
                },
            )),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// Count records currently in the space's segments.
    async fn stored_count(relay: &Relay, space: Uuid) -> usize {
        relay
            .record_log
            .load_space(space)
            .await
            .expect("load records")
            .len()
    }

    /// A re-seed appends the records the relay lacks; replaying the SAME batch
    /// is a pure no-op (all duplicate, no growth in the stored record count).
    #[tokio::test]
    async fn test_reseed_appends_missing_records_idempotently() {
        let space = Uuid::new_v4();
        // Start with an empty durable space.
        let (mut relay, _dir) = relay_with_records(space, &[]);
        let space_id = space.to_string();
        assert_eq!(stored_count(&relay, space).await, 0, "empty to start");

        let seeder = "did:key:zSeeder";
        let batch = vec![
            as_actor(record(space, 1, 0, "r1"), seeder),
            as_actor(record(space, 2, 0, "r2"), seeder),
            as_actor(record(space, 3, 0, "r3"), seeder),
        ];

        // First POST: all three appended.
        let first = relay
            .handle_signal_reseed(seeder, &space_id, batch.clone())
            .await
            .expect("member re-seed succeeds");
        assert_eq!(first.appended, 3, "all missing records appended");
        assert_eq!(first.duplicate, 0);
        assert_eq!(first.rejected, 0);
        assert_eq!(stored_count(&relay, space).await, 3, "segment grew by 3");

        // Second POST of the SAME batch: all duplicate no-ops, no growth.
        let second = relay
            .handle_signal_reseed(seeder, &space_id, batch)
            .await
            .expect("idempotent replay succeeds");
        assert_eq!(second.appended, 0, "replay appends nothing");
        assert_eq!(
            second.duplicate, 3,
            "all three fold as byte-identical no-ops"
        );
        assert_eq!(second.rejected, 0);
        assert_eq!(
            stored_count(&relay, space).await,
            3,
            "idempotent replay does not grow the segment"
        );
    }

    /// ANTI-MASK: a re-seed pushed by ONE caller must admit records authored by
    /// OTHER members. A recovering daemon relays every author's records it
    /// caught up; if the ingest gate actor-bound to the pushing
    /// caller, every foreign-authored record would be silently dropped —
    /// permanent multi-member data loss. Here three records carry three GENUINELY
    /// DISTINCT `actor_did`s (NOT `as_actor`, which forces actor==caller and is
    /// exactly what masked this bug); a single seeder pushes all three and every
    /// one must be appended, none rejected.
    #[tokio::test]
    async fn test_reseed_admits_records_from_multiple_distinct_authors() {
        let space = Uuid::new_v4();
        let (mut relay, _dir) = relay_with_records(space, &[]);
        let space_id = space.to_string();
        assert_eq!(stored_count(&relay, space).await, 0, "empty to start");

        // Three records authored by three DIFFERENT members — none of which is
        // the pushing caller. Build them directly and set distinct actor_dids
        // WITHOUT the actor==caller `as_actor` helper.
        let mut alice = record(space, 1, 0, "rAlice");
        alice.actor_did = "did:key:zAlice".into();
        let mut bob = record(space, 2, 0, "rBob");
        bob.actor_did = "did:key:zBob".into();
        let mut carol = record(space, 3, 0, "rCarol");
        carol.actor_did = "did:key:zCarol".into();

        // ONE caller (a fourth DID) drives the re-seed for all three authors.
        let seeder = "did:key:zSeeder";
        assert_ne!(alice.actor_did, seeder);
        assert_ne!(bob.actor_did, seeder);
        assert_ne!(carol.actor_did, seeder);
        assert_ne!(alice.actor_did, bob.actor_did);
        assert_ne!(bob.actor_did, carol.actor_did);

        let outcome = relay
            .handle_signal_reseed(seeder, &space_id, vec![alice, bob, carol])
            .await
            .expect("member re-seed of foreign-authored records succeeds");
        assert_eq!(
            outcome.appended, 3,
            "all three foreign-authored records must be appended"
        );
        assert_eq!(outcome.duplicate, 0);
        assert_eq!(
            outcome.rejected, 0,
            "NO foreign-authored record may be rejected on re-seed (would be data loss)"
        );
        assert_eq!(
            stored_count(&relay, space).await,
            3,
            "all three authors' records landed in the segment"
        );
    }

    /// A `record_id` collision with DIVERGENT bytes is rejected — the incoming
    /// record is NOT appended and the stored record is left unchanged.
    #[tokio::test]
    async fn test_reseed_rejects_record_id_collision_with_divergent_bytes() {
        let space = Uuid::new_v4();
        // Seed one record via the re-seed path so it is the stored bytes.
        let original = record(space, 1, 0, "rDup");
        let (mut relay, _dir) = relay_with_records(space, std::slice::from_ref(&original));
        let space_id = space.to_string();
        assert_eq!(stored_count(&relay, space).await, 1);

        // Same record_id, DIFFERENT bytes (distinct HLC → distinct encoding).
        // Bind the actor to the caller so the record clears the actor-binding
        // gate and reaches the divergent-collision check this test exercises.
        let seeder = "did:key:zSeeder";
        let divergent = as_actor(record(space, 999, 7, "rDup"), seeder);
        assert_ne!(
            prost::Message::encode_to_vec(&original),
            prost::Message::encode_to_vec(&divergent),
            "test setup: the two records must encode to different bytes"
        );

        let outcome = relay
            .handle_signal_reseed(seeder, &space_id, vec![divergent])
            .await
            .expect("re-seed succeeds even with a poison record");
        assert_eq!(
            outcome.appended, 0,
            "divergent-collision record not appended"
        );
        assert_eq!(outcome.duplicate, 0);
        assert_eq!(outcome.rejected, 1, "the collision is counted as rejected");

        // Stored record is untouched: still exactly one, still the original bytes.
        assert_eq!(stored_count(&relay, space).await, 1, "no append happened");
        let loaded = relay.record_log.load_space(space).await.unwrap();
        assert_eq!(
            prost::Message::encode_to_vec(&loaded[0]),
            prost::Message::encode_to_vec(&original),
            "the stored record must be byte-for-byte the original"
        );
    }
    /// A re-seed batch mixing a good new record and an invalid one appends the
    /// good and rejects the bad — one poison record does not fail the batch.
    #[tokio::test]
    async fn test_reseed_appends_good_rejects_invalid_in_same_batch() {
        let space = Uuid::new_v4();
        let (mut relay, _dir) = relay_with_records(space, &[]);
        let space_id = space.to_string();

        let seeder = "did:key:zSeeder";
        let good = as_actor(record(space, 1, 0, "rGood"), seeder);
        // Missing record_id fails validate_record.
        let mut bad = as_actor(record(space, 2, 0, "rBad"), seeder);
        bad.record_id = String::new();

        let outcome = relay
            .handle_signal_reseed(seeder, &space_id, vec![good, bad])
            .await
            .expect("batch with one poison record still succeeds");
        assert_eq!(outcome.appended, 1, "the well-formed record is appended");
        assert_eq!(outcome.rejected, 1, "the invalid record is rejected");
        assert_eq!(outcome.duplicate, 0);
        assert_eq!(
            stored_count(&relay, space).await,
            1,
            "only the good record landed"
        );
    }

    /// A non-member DID is rejected on the re-seed path too — the same
    /// `NotAuthorized` class the catch-up + WS `handle_signal` paths give.
    #[tokio::test]
    async fn test_reseed_non_member_rejected() {
        let space = Uuid::new_v4();
        let dir = tempfile::TempDir::new().unwrap();

        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        relay.test_set_record_log(
            Some(std::sync::Arc::new(SegmentRecordLog::new(
                SignalStore::new(dir.path().to_path_buf()),
            ))),
            None,
        );

        let err = relay
            .handle_signal_reseed(
                "did:key:zStranger",
                &space.to_string(),
                vec![record(space, 1, 0, "r1")],
            )
            .await
            .expect_err("non-member re-seed must be rejected");
        assert!(
            matches!(err, McpError::NotAuthorized { .. }),
            "non-member gets NotAuthorized, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // CONFUSED DEPUTY: a member of space A must not be able to
    // cause ANY observable change in space B via the re-seed path.
    // -----------------------------------------------------------------------

    /// Build a durable-store relay whose `signal_store` writes to `dir` AND
    /// whose `change_backend` is a real `SqliteChangeBackend` on a shared
    /// in-memory pool, so `project_space_signals` actually writes projection
    /// rows the test can inspect for cross-space tamper.
    pub(in crate::relay) async fn relay_with_projection(
        dir: &tempfile::TempDir,
    ) -> (Relay, sqlx::sqlite::SqlitePool) {
        relay_with_projection_and_observer(dir, std::sync::Arc::new(crate::observer::NoopObserver))
            .await
    }

    /// [`relay_with_projection`] with a caller-supplied observer, for tests that
    /// assert a door FIRED an event rather than only that it wrote a row.
    pub(in crate::relay) async fn relay_with_projection_and_observer(
        dir: &tempfile::TempDir,
        observer: std::sync::Arc<dyn crate::observer::RelayObserver>,
    ) -> (Relay, sqlx::sqlite::SqlitePool) {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        // The change backend queries the `registry` table on some paths; the
        // projection path does not, but create it for parity with production.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS registry (
                space_id TEXT NOT NULL, document_id TEXT NOT NULL, path TEXT NOT NULL,
                created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
                renamed_by TEXT, renamed_at INTEGER, deleted_at INTEGER,
                edited_at INTEGER, originally_created_at INTEGER,
                rename_causal_floor_at INTEGER,
                PRIMARY KEY (space_id, document_id))",
        )
        .execute(&pool)
        .await
        .unwrap();
        let backend = crate::change_sqlite::SqliteChangeBackend::new(pool.clone())
            .await
            .expect("build sqlite change backend");
        let config = test_config();
        let mut relay = Relay::new_standalone_with_observer(
            config,
            None,
            None,
            None,
            None,
            observer,
            std::sync::Arc::new(crate::observer::NoopBeforeMergeObserver),
            std::sync::Arc::new(crate::observer::NoopAfterMergeObserver),
        );
        // Log and projection are wired together — there is no separate
        // `change_backend` field to set, which is the point: a test cannot
        // construct a relay that appends without projecting either.
        relay.test_set_record_log(
            Some(std::sync::Arc::new(SegmentRecordLog::new(
                SignalStore::new(dir.path().to_path_buf()),
            ))),
            Some(std::sync::Arc::new(backend)),
        );
        (relay, pool)
    }

    /// Read the `(space_id, message)` of a projected row by signal id, or `None`.
    async fn projected_row(
        pool: &sqlx::sqlite::SqlitePool,
        id: &str,
    ) -> Option<(String, Option<String>)> {
        sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT space_id, message FROM signals_active WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    /// CONFUSED DEPUTY: a member authorized for space A drives `handle_signal_reseed`
    /// with a batch that tries to slip records into space B —
    ///   1. a record whose `space_id = B` (spoofed foreign space), and
    ///   2. a record spoofed `space_id = A` but carrying signal `id = <B's own
    ///      existing signal id>` (a chosen-id collision attempt).
    /// Both must be rejected/neutralized: B's projection row must be
    /// byte-unchanged, B's segments must be unchanged, the foreign-`space_id`
    /// record counted `rejected`, and A's OWN legitimate record must still land.
    #[tokio::test]
    async fn test_reseed_cannot_tamper_other_space() {
        let space_a = Uuid::new_v4();
        let space_b = Uuid::new_v4();
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = relay_with_projection(&dir).await;

        // Seed space B with a legitimate signal (project it so B has a row).
        let b_member = "did:key:zBMember";
        let victim_signal_id = Uuid::new_v4().to_string();
        let victim = as_actor(
            record_with_id(space_b, &victim_signal_id, 10, 0, "b-victim"),
            b_member,
        );
        relay
            .handle_signal_reseed(b_member, &space_b.to_string(), vec![victim.clone()])
            .await
            .expect("seed space B succeeds");
        let (b_before_space, b_before_msg) = projected_row(&pool, &victim_signal_id)
            .await
            .expect("space B victim row must exist after seeding");
        assert_eq!(b_before_space, space_b.to_string());
        let b_segments_before = stored_count(&relay, space_b).await;

        // Attacker is a member of space A. Batch:
        //  - a_good: a legitimate A record (must land),
        //  - foreign: space_id = B (must be rejected at ingest — 1a),
        //  - chosen_id: space_id spoofed to A but signal id = B's victim id, and
        //    a divergent message (which must not overwrite B's row via the
        //    id-keyed projection PK).
        // All three are pushed by the same A member, so their `actor_did` binds
        // to that caller — `foreign` is then rejected specifically for its
        // foreign `space_id` (space-binding runs before actor-binding), keeping
        // this test's confused-deputy intent crisp.
        let a_member = "did:key:zAMember";
        let a_good = as_actor(
            record_with_id(space_a, &Uuid::new_v4().to_string(), 20, 0, "a-good"),
            a_member,
        );
        let mut foreign = as_actor(
            record_with_id(space_b, &Uuid::new_v4().to_string(), 21, 0, "foreign"),
            a_member,
        );
        if let Some(kutl_proto::sync::signal::Payload::Flag(f)) = foreign.payload.as_mut() {
            f.message = "INJECTED INTO B".into();
        }
        let mut chosen_id = as_actor(
            record_with_id(space_a, &victim_signal_id, 22, 0, "chosen-id"),
            a_member,
        );
        if let Some(kutl_proto::sync::signal::Payload::Flag(f)) = chosen_id.payload.as_mut() {
            f.message = "OVERWRITE B VICTIM".into();
        }

        let outcome = relay
            .handle_signal_reseed(
                a_member,
                &space_a.to_string(),
                vec![a_good, foreign, chosen_id],
            )
            .await
            .expect("re-seed as A member returns an outcome, not an error");

        // The foreign-space record is rejected; A's own good record lands. The
        // chosen-id record has space_id = A so it lands in A (its own space) —
        // the point is it must NOT reach into B.
        assert!(
            outcome.rejected >= 1,
            "the foreign-space record is rejected"
        );
        assert!(outcome.appended >= 1, "A's legitimate record still lands");

        // B's projection row is BYTE-UNCHANGED.
        let (b_after_space, b_after_msg) = projected_row(&pool, &victim_signal_id)
            .await
            .expect("space B victim row must still exist");
        assert_eq!(
            b_after_space,
            space_b.to_string(),
            "B's row space_id unchanged"
        );
        assert_eq!(b_after_msg, b_before_msg, "B's row message unchanged");
        assert_eq!(
            b_before_msg.as_deref(),
            Some("please review"),
            "B's row must be the original, not the injected message"
        );

        // B's segments are unchanged.
        assert_eq!(
            stored_count(&relay, space_b).await,
            b_segments_before,
            "no record must have been appended to space B's segments"
        );
    }

    /// The 200-always contract: a batch mixing good, duplicate, and rejectable
    /// records returns the right per-record counts as an `Ok` outcome — never an
    /// error — even when the projection runs.
    #[tokio::test]
    async fn test_reseed_mixed_batch_returns_counts_not_error() {
        let space = Uuid::new_v4();
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = space.to_string();

        // Pre-seed one record so a duplicate can be exercised.
        let member = "did:key:zM";
        let dup = as_actor(
            record_with_id(space, &Uuid::new_v4().to_string(), 1, 0, "rDup"),
            member,
        );
        relay
            .handle_signal_reseed(member, &space_id, vec![dup.clone()])
            .await
            .expect("seed succeeds");

        let good = as_actor(
            record_with_id(space, &Uuid::new_v4().to_string(), 2, 0, "rGood"),
            member,
        );
        let mut bad = as_actor(
            record_with_id(space, &Uuid::new_v4().to_string(), 3, 0, "rBad"),
            member,
        );
        bad.record_id = String::new(); // fails validate_record

        let outcome = relay
            .handle_signal_reseed(member, &space_id, vec![good, dup, bad])
            .await
            .expect("mixed batch returns Ok(outcome), never an error");
        assert_eq!(outcome.appended, 1, "one good record appended");
        assert_eq!(outcome.duplicate, 1, "the replayed record is a duplicate");
        assert_eq!(outcome.rejected, 1, "the malformed record is rejected");
    }

    /// A relay configured to refuse client-pushed history returns
    /// `ReSeedRefused` and stores nothing, while catch-up over the same log
    /// keeps working.
    ///
    /// Both halves in one test on purpose: the refusal is only correct if it is
    /// narrow. A change that accidentally gated the record log itself would
    /// still pass a refusal-only assertion while silently making kutlhub's
    /// history unreadable — which is the exact regression this deployment
    /// cannot afford, since serving history is why it got a log at all.
    #[tokio::test]
    async fn test_reseed_refused_still_serves_catch_up() {
        let space = Uuid::new_v4();
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = space.to_string();
        let member = "did:key:zM";

        // Seed one record through the accepting configuration, so catch-up has
        // something to serve after the refusal is switched on.
        let seeded = as_actor(
            record_with_id(space, &Uuid::new_v4().to_string(), 1, 0, "rSeeded"),
            member,
        );
        relay
            .handle_signal_reseed(member, &space_id, vec![seeded])
            .await
            .expect("the accepting configuration takes the record");

        relay.config.accepts_reseed = false;

        let pushed = as_actor(
            record_with_id(space, &Uuid::new_v4().to_string(), 2, 0, "rPushed"),
            member,
        );
        let err = relay
            .handle_signal_reseed(member, &space_id, vec![pushed])
            .await
            .expect_err("a refusing relay must not report a per-record outcome");
        assert!(
            matches!(err, crate::relay::McpError::ReSeedRefused),
            "the refusal must be its own error, not a rejected-record count: {err}"
        );

        // Nothing was stored, and the pre-existing record still pages.
        let stored = relay.record_log.load_space(space).await.expect("load");
        assert_eq!(stored.len(), 1, "a refused batch must store nothing");
        assert_eq!(stored[0].record_id, "rSeeded");

        // The serving side, transport-independent: the same log + pager pair
        // every catch-up door composes must still page the record.
        let records = relay
            .record_log
            .list(space, None, 10)
            .await
            .expect("catch-up reads must keep working on a refusing relay");
        let page = kutl_signals::catchup::page(&records, None, 10);
        assert_eq!(
            page.records.len(),
            1,
            "refusing to be written to must not stop the relay serving history"
        );
    }

    /// The seam asymmetry: the same
    /// malformed record is a CALLER MISTAKE on the authored path and HISTORY on
    /// the replicated one.
    ///
    /// This is the whole reason there are two seams rather than one function with
    /// a flag. Tightening the replicated path would retroactively reject records a
    /// peer already accepted and lose history; loosening the authored path would
    /// let a door mint the dead end the typed audience exists to remove.
    #[tokio::test]
    async fn test_malformed_audience_rejected_authored_admitted_replicated() {
        let space = Uuid::new_v4();
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = space.to_string();
        let member = "did:key:zM";
        let signal_id = Uuid::new_v4().to_string();

        // A participant audience naming nobody: `to` IS set, so a presence check
        // waves it through — which is why the seam checks well-formedness.
        let mut rec = as_actor(record_with_id(space, &signal_id, 7, 0, "rNoTarget"), member);
        if let Some(kutl_proto::sync::signal::Payload::Flag(flag)) = rec.payload.as_mut() {
            flag.audience = Some(kutl_proto::sync::Audience {
                to: Some(kutl_proto::sync::audience::To::Participant(
                    kutl_proto::sync::audience::Participant { did: String::new() },
                )),
            });
        }

        let err = relay
            .append_authored_record(&rec, &signal_id, &space_id, None, Announce::Silent)
            .await
            .expect_err("the authored seam rejects an audience that names nobody");
        assert!(
            matches!(
                err,
                crate::change_backend::ChangeError::InvalidArgument { .. }
            ),
            "a caller mistake must surface as InvalidArgument, not Internal —              otherwise a user error comes back as a 500; got {err:?}"
        );

        let outcome = relay
            .handle_signal_reseed(member, &space_id, vec![rec])
            .await
            .expect("re-seed returns an outcome");
        assert_eq!(
            outcome.appended, 1,
            "the replicated seam ADMITS the same record: it was legal when its              author wrote it, and rejecting it here would lose peer history"
        );
        assert_eq!(outcome.rejected, 0);
    }

    /// The supplied-id collision check, and the same seam
    /// asymmetry: a second CREATE naming a signal that already exists is a
    /// caller mistake on the authored path, and ordinary replication on the
    /// other — a peer re-seeding a space legitimately sends `CREATED`s for
    /// signals this relay already holds.
    #[tokio::test]
    async fn test_duplicate_create_rejected_authored_admitted_replicated() {
        let space = Uuid::new_v4();
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = space.to_string();
        let member = "did:key:zM";
        let signal_id = Uuid::new_v4().to_string();

        // `record_with_id` carries the DEPRECATED audience pair — it stands in
        // for records already on disk, which the replicated seam admits and the
        // authored one refuses. An authored record speaks the typed shape.
        let authored = |rec: &str| {
            let mut s = as_actor(record_with_id(space, &signal_id, 7, 2, rec), member);
            if let Some(kutl_proto::sync::signal::Payload::Flag(flag)) = s.payload.as_mut() {
                #[allow(deprecated)]
                {
                    flag.audience_type = 0;
                }
                flag.audience = Some(kutl_proto::sync::Audience {
                    to: Some(kutl_proto::sync::audience::To::Space(
                        kutl_proto::sync::audience::Space {},
                    )),
                });
            }
            s
        };

        // Create once, projecting the row the check reads.
        let first = authored("rFirst");
        relay
            .append_authored_record(&first, &signal_id, &space_id, None, Announce::Silent)
            .await
            .expect("the first create is admitted");
        relay
            .rebuild_space_projection(&signal_id, &space_id)
            .await
            .expect("project the create");

        // A second create under the SAME id — a different record, same signal.
        let second = authored("rSecond");
        let err = relay
            .append_authored_record(&second, &signal_id, &space_id, None, Announce::Silent)
            .await
            .expect_err("the authored seam refuses a create naming an existing signal");
        assert!(
            matches!(
                err,
                crate::change_backend::ChangeError::InvalidArgument { .. }
            ),
            "a caller mistake must surface as InvalidArgument, not Internal; got {err:?}"
        );

        // A TRANSITION for that same id is the opposite case — it is supposed to
        // name an existing signal, so the check must not touch it.
        let mut close = authored("rClose");
        close.set_event(SignalEventType::Closed);
        close.payload = None;
        relay
            .append_authored_record(&close, &signal_id, &space_id, None, Announce::Silent)
            .await
            .expect("a transition names an existing signal by design");

        // Replication still admits the duplicate create: re-seed exists to carry
        // history this relay may already hold.
        let outcome = relay
            .handle_signal_reseed(member, &space_id, vec![second])
            .await
            .expect("re-seed returns an outcome");
        assert_eq!(
            outcome.appended, 1,
            "the replicated seam admits a CREATE for a signal it already has"
        );
        assert_eq!(outcome.rejected, 0);
    }
}

#[cfg(test)]
mod create_broadcast_tests {
    use crate::record_log::SegmentRecordLog;
    use kutl_proto::sync::{self, Signal, SignalEventType};
    use uuid::Uuid;

    use super::super::{ConnId, Relay, connect_client, test_config};

    /// Connect a client and join the space's SIGNAL stream.
    ///
    /// A document subscription does not put a connection
    /// in the space's signal recipient set — that conflation would force
    /// `kutl watch` to subscribe to a sentinel document it has no interest in.
    async fn connect_signal_subscriber(
        relay: &mut Relay,
        conn_id: ConnId,
        space: &str,
    ) -> (
        tokio::sync::mpsc::Receiver<Vec<u8>>,
        tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) {
        let channels = connect_client(relay, conn_id).await;
        relay
            .handle_subscribe_signals(
                conn_id,
                &sync::SubscribeSignals {
                    space_id: space.to_owned(),
                    cursor: None,
                },
            )
            .await;
        channels
    }

    /// Decode a broadcast frame into its inner `Signal`, or `None` if the frame
    /// is some other envelope variant.
    fn decode_signal(bytes: &[u8]) -> Option<Signal> {
        match kutl_proto::protocol::decode_envelope(bytes).ok()?.payload {
            Some(sync::sync_envelope::Payload::Signal(sig)) => Some(sig),
            _ => None,
        }
    }

    /// A subscriber whose data lane fills is evicted from the SIGNAL STREAM and
    /// told so — it is not disconnected.
    ///
    /// Live signals ride the data lane, not `ctrl`: a full `ctrl` lane calls
    /// `handle_disconnect`, so the whole connection goes down. A client slow to
    /// drain one signal broadcast would then lose its DOCUMENT sync as
    /// collateral, which is a spectacular over-reaction to back-pressure on
    /// bulk traffic.
    ///
    /// Both halves are asserted, because either alone would pass for the wrong
    /// reason: the connection must SURVIVE, and the client must be TOLD, or it
    /// waits forever on a stream that stopped feeding it. The notice arrives on
    /// the ack lane rather than the data lane that just overflowed — a notice
    /// storm down the bounded lane that just overflowed would re-strand the
    /// subscribers it exists to save.
    #[tokio::test]
    async fn test_a_full_data_lane_evicts_the_signal_stream_not_the_connection() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let dir = tempfile::TempDir::new().unwrap();
        relay.test_set_record_log(
            Some(std::sync::Arc::new(SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(dir.path().to_path_buf()),
            ))),
            None,
        );
        let space = Uuid::new_v4().to_string();

        // A subscriber that never drains its data lane. `test_config` sizes the
        // lane at 16, so authoring past that overflows it.
        //
        // Its DID must DIFFER from the author's: `connect_client` authenticates
        // every connection as `TEST_CONN_DID`, and `collect_flag_recipients`
        // skips the author, so a same-DID subscriber receives nothing at all and
        // its lane never fills — the test would then pass by never exercising
        // anything.
        let subscriber: ConnId = 2;
        let (_data_never_drained, mut sub_ack, _ctrl) =
            connect_client(&mut relay, subscriber).await;
        relay.test_set_authenticated(subscriber, "did:b");
        relay
            .handle_subscribe_signals(
                subscriber,
                &sync::SubscribeSignals {
                    space_id: space.clone(),
                    cursor: None,
                },
            )
            .await;
        while sub_ack.try_recv().is_ok() {}

        let author: ConnId = 1;
        let (_a_data, _a_ack, _a_ctrl) = connect_client(&mut relay, author).await;
        for i in 0..24 {
            relay
                .handle_submit_flag(
                    author,
                    &sync::SubmitFlag {
                        client_ref: format!("ref-{i}"),
                        space_id: space.clone(),
                        document_id: None,
                        signal_id: None,
                        kind: i32::from(sync::FlagKind::Info),
                        message: format!("flood {i}"),
                        audience: Some(kutl_proto::vocab::space_audience()),
                    },
                )
                .await;
        }

        assert!(
            relay.connections.contains_key(&subscriber),
            "a full data lane must not tear down the connection — that is the \
             `handle_disconnect` behaviour this replaced, and it took document \
             sync down with it"
        );

        let mut notices = 0;
        while let Ok(bytes) = sub_ack.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && let Some(sync::sync_envelope::Payload::StaleSignalStream(notice)) = env.payload
            {
                assert_eq!(
                    notice.cause(),
                    sync::StaleStreamReason::PausedLaneFull,
                    "a pause names itself: the client re-subscribes at once on this cause alone"
                );
                notices += 1;
            }
        }
        assert!(
            notices > 0,
            "an evicted subscriber must be TOLD on the ack lane, or it waits \
             forever on a stream that quietly stopped feeding it"
        );
    }

    /// A refused subscribe is ANSWERED, with the cause a client stops on.
    ///
    /// Authorization is the one place a space id is checked, and an id that is
    /// not a UUID fails it; every exit after that is a page or a notice, so a
    /// connection is never left with nothing to hang "I am caught up" on. The
    /// typed cause is what keeps a client from re-subscribing into the same
    /// refusal at round-trip rate, and the listener set agrees with the notice.
    #[tokio::test]
    async fn test_a_refused_subscribe_is_answered_with_not_authorized() {
        let mut relay = Relay::new_standalone(test_config());
        let conn: ConnId = 1;
        let (_data, mut ack, _ctrl) = connect_client(&mut relay, conn).await;
        while ack.try_recv().is_ok() {}

        let space = "not-a-uuid";
        relay
            .handle_subscribe_signals(
                conn,
                &sync::SubscribeSignals {
                    space_id: space.to_owned(),
                    cursor: None,
                },
            )
            .await;

        let bytes = ack.try_recv().expect("a refused subscribe is answered");
        let Some(sync::sync_envelope::Payload::StaleSignalStream(notice)) =
            kutl_proto::protocol::decode_envelope(&bytes)
                .expect("valid envelope")
                .payload
        else {
            panic!("expected a StaleSignalStream notice");
        };
        assert_eq!(notice.space_id, space);
        assert_eq!(notice.cause(), sync::StaleStreamReason::NotAuthorized);
        assert!(
            ack.try_recv().is_err(),
            "one subscribe, one answer: no page follows a refusal"
        );
        assert!(
            !relay
                .listeners
                .get(space)
                .is_some_and(|conns| conns.contains_key(&conn)),
            "a refused connection is not a listener"
        );
    }

    /// The live CREATED broadcast for a flag must carry the FULL record
    /// envelope — a non-empty `record_id`, `event == CREATED`, and an `hlc`.
    /// A broadcast riding the legacy pre-record flag shape leaves
    /// every record-envelope field at its proto default
    /// (empty `record_id`, `UNSPECIFIED` event, no `hlc`), so a records-capable
    /// receiver could not fold it as a CREATED record.
    #[tokio::test]
    async fn test_created_broadcast_carries_full_record_envelope() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        // `relay_flag_signal` appends via the signal store, and the authored
        // seam's create branch requires the space, document and signal ids to be
        // UUIDs — so use real ones and a durable store.
        let dir = tempfile::TempDir::new().unwrap();
        relay.test_set_record_log(
            Some(std::sync::Arc::new(SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(dir.path().to_path_buf()),
            ))),
            None,
        );
        let space = Uuid::new_v4().to_string();
        let doc = Uuid::new_v4().to_string();

        // A subscriber (distinct from the author) receives the space broadcast.
        let subscriber: ConnId = 2;
        let (mut sub_data, _ack, mut sub_ctrl) =
            connect_signal_subscriber(&mut relay, subscriber, &space).await;
        // Drain any subscribe-time frames so the ctrl lane starts clean.
        while sub_ctrl.try_recv().is_ok() {}

        let signal_id = relay
            .relay_flag_signal(
                None,
                &space,
                Some(doc.as_str()),
                "did:key:zAuthor",
                i32::from(sync::FlagKind::ReviewRequested),
                // A space audience is a broadcast, and the typed shape has no arm
                // for one carrying a target — the combination is unrepresentable
                // here rather than merely rejected.
                kutl_proto::vocab::space_audience(),
                "please review",
                None,
                1_700_000_000_000,
                None,
            )
            .await
            .expect("flag emit succeeds");

        // Find the CREATED broadcast on the subscriber's DATA lane — live
        // signals ride the data lane, not `ctrl`.
        let mut created: Option<Signal> = None;
        while let Ok(bytes) = sub_data.try_recv() {
            if let Some(sig) = decode_signal(&bytes) {
                created = Some(sig);
            }
        }
        let created = created.expect("subscriber must receive the flag broadcast as a Signal");

        assert_eq!(created.id, signal_id, "broadcast is for the created signal");
        assert!(
            !created.record_id.is_empty(),
            "CREATED broadcast must carry the record_id (empty before the fix)"
        );
        assert_eq!(
            created.event(),
            SignalEventType::Created,
            "broadcast event must be CREATED (UNSPECIFIED before the fix)"
        );
        assert!(
            created.hlc.is_some(),
            "CREATED broadcast must carry the record hlc (absent before the fix)"
        );
    }

    /// SAFETY PIN: freeze the EXACT field-level output of the
    /// relay-mint flag CREATE path (`stamp_and_append_created` →
    /// `assemble_record` + `attest_on_ingest`) so any refactor of that
    /// pipeline is provably behavior-preserving. The golden
    /// canonical-bytes tests pin the byte LAYOUT; this pins the relay path's
    /// field VALUES — `id`/`space_id`/`document_id`, `author_did == actor_did`
    /// (the authoritative caller DID), a fresh v4 `record_id`, `event == CREATED`,
    /// the flag payload, the supplied `timestamp`, a monotonic HLC, and a
    /// present tier-2 attestation naming the relay's own DID.
    #[tokio::test]
    // The deprecated audience pair is the shape under test: these fixtures
    // stand in for records already on disk.
    #[allow(deprecated)]
    async fn test_relay_mint_flag_create_field_values_pinned() {
        /// The caller-supplied wall-clock timestamp for the created flag; it
        /// must ride into `record.timestamp` unchanged (the projection reads it).
        const SUPPLIED_TIMESTAMP: i64 = 1_700_000_000_000;
        /// The authoritative author == actor DID for a relay-mint flag CREATE.
        const AUTHOR_DID: &str = "did:key:zAuthor";

        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let dir = tempfile::TempDir::new().unwrap();
        relay.test_set_record_log(
            Some(std::sync::Arc::new(SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(dir.path().to_path_buf()),
            ))),
            None,
        );
        // Give the relay a signing identity so the minted record is tier-2
        // attested — this pins the attestation branch, not just the tier-3 one.
        let id_dir = tempfile::TempDir::new().unwrap();
        let identity = crate::identity::RelayIdentity::load_or_generate(id_dir.path()).unwrap();
        let relay_did = identity.did().to_owned();
        relay.signing_identity = Some(std::sync::Arc::new(identity));

        let space = Uuid::new_v4().to_string();
        let doc = Uuid::new_v4().to_string();

        // A subscriber (distinct from the author) receives the space broadcast,
        // carrying the same fully-stamped record just appended.
        let subscriber: ConnId = 2;
        let (mut sub_data, _ack, mut sub_ctrl) =
            connect_signal_subscriber(&mut relay, subscriber, &space).await;
        while sub_ctrl.try_recv().is_ok() {}

        let signal_id = relay
            .relay_flag_signal(
                None,
                &space,
                Some(doc.as_str()),
                AUTHOR_DID,
                i32::from(sync::FlagKind::ReviewRequested),
                // SPACE, which the typed shape can only express WITHOUT a target.
                // It stays a broadcast rather than becoming a participant flag
                // because this test asserts field pinning on the record the
                // SUBSCRIBER receives, and a participant audience delivers only to
                // the target and the author.
                kutl_proto::vocab::space_audience(),
                "please review",
                None,
                SUPPLIED_TIMESTAMP,
                None,
            )
            .await
            .expect("flag emit succeeds");

        // The DATA lane — live signals ride it, not `ctrl`.
        let mut created: Option<Signal> = None;
        while let Ok(bytes) = sub_data.try_recv() {
            if let Some(sig) = decode_signal(&bytes) {
                created = Some(sig);
            }
        }
        let created = created.expect("subscriber must receive the flag broadcast as a Signal");

        // Identity + envelope.
        assert_eq!(created.id, signal_id, "id is the created signal id");
        assert_eq!(created.space_id, space, "space_id is the target space");
        assert_eq!(
            created.document_id.as_deref(),
            Some(doc.as_str()),
            "document_id is the target document"
        );
        assert_eq!(created.actor_did, AUTHOR_DID, "actor_did is the author DID");
        assert_eq!(
            created.author_did, AUTHOR_DID,
            "author_did == actor_did on a relay-mint CREATE"
        );
        assert_eq!(
            created.event(),
            SignalEventType::Created,
            "event is CREATED"
        );
        assert_eq!(
            created.timestamp, SUPPLIED_TIMESTAMP,
            "the supplied timestamp rides through unchanged"
        );

        // Fresh v4 record_id.
        assert!(!created.record_id.is_empty(), "record_id must be minted");
        let parsed = Uuid::parse_str(&created.record_id).expect("record_id must be a valid UUID");
        assert_eq!(parsed.get_version_num(), 4, "record_id must be a v4 UUID");

        // Monotonic HLC carrying the wall-ms and a 16-byte actor.
        let hlc = created.hlc.as_ref().expect("hlc must be set");
        assert_eq!(hlc.actor.len(), 16, "hlc actor must be 16 bytes");

        // Flag payload matches the tool args.
        let Some(sync::signal::Payload::Flag(flag)) = created.payload.as_ref() else {
            panic!("the CREATED record must carry a flag payload");
        };
        assert_eq!(flag.kind, i32::from(sync::FlagKind::ReviewRequested));
        assert_eq!(flag.message, "please review");
        // The audience rides in the TYPED field, and the deprecated pair is left
        // unset — the builder emits one or the other, never both.
        assert_eq!(
            kutl_proto::vocab::audience_to_untyped(flag.audience.as_ref()),
            (i32::from(sync::AudienceType::Space), None),
            "the typed audience is the space-wide broadcast"
        );
        #[allow(deprecated)]
        {
            assert_eq!(
                flag.audience_type,
                i32::from(sync::AudienceType::Unspecified),
                "the deprecated audience_type must be unset on an authored record"
            );
            assert!(
                flag.target_did.is_none(),
                "the deprecated target_did must be unset on an authored record"
            );
        }

        // Tier-2 attestation present and naming this relay's own DID.
        let att = created
            .attestation
            .as_ref()
            .expect("a relay with a signing identity attests the record (tier-2)");
        assert_eq!(att.relay_did, relay_did, "attestation names the relay DID");
        assert!(!att.signature.is_empty(), "attestation is signed");
    }
}

/// The deletion ladder: deleting a document tombstones (hides) its
/// signals; reviving the document reopens them. These drive the cascade
/// helpers against a durable relay with a real projection so both the segment
/// fold and the `signals` projection can be inspected.
#[cfg(test)]
mod delete_cascade_tests {
    use kutl_proto::sync::{self, signal::Payload};
    use kutl_signals::fold::{SignalStatus, SpaceSignalState};
    use uuid::Uuid;

    use super::super::{Relay, TEST_CONN_DID, test_config};
    use crate::record_log::SegmentRecordLog;
    use crate::relay::signal_log::Announce;
    use crate::signal_store::SignalStore;

    /// Build a durable relay whose signal store writes to `dir` and whose change
    /// backend is a real sqlite projection on a shared in-memory pool, so both
    /// the fold and the projection are inspectable.
    async fn durable_relay(dir: &tempfile::TempDir) -> (Relay, sqlx::sqlite::SqlitePool) {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        let backend = crate::change_sqlite::SqliteChangeBackend::new(pool.clone())
            .await
            .expect("build sqlite change backend");
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        // Log and projection are wired together — there is no separate
        // `change_backend` field to set, which is the point: a test cannot
        // construct a relay that appends without projecting either.
        relay.test_set_record_log(
            Some(std::sync::Arc::new(SegmentRecordLog::new(
                SignalStore::new(dir.path().to_path_buf()),
            ))),
            Some(std::sync::Arc::new(backend)),
        );
        (relay, pool)
    }

    /// Create a projected flag signal attached to `doc` and return its id.
    async fn create_signal(relay: &mut Relay, space: &str, doc: &str) -> String {
        relay
            .relay_flag_signal(
                None,
                space,
                Some(doc),
                "did:key:zAuthor",
                i32::from(sync::FlagKind::ReviewRequested),
                kutl_proto::vocab::space_audience(),
                "please review",
                None,
                1_700_000_000_000,
                None,
            )
            .await
            .expect("flag emit succeeds")
    }

    /// Create + project a SPACE-LEVEL signal (no `document_id`) and return its id.
    async fn create_space_level_signal(relay: &mut Relay, space: &str) -> String {
        let id = Uuid::new_v4().to_string();
        let now = kutl_core::env::now_ms();
        let relay_hlc: kutl_proto::sync::Hlc = relay
            .signal_clock
            .lock()
            .unwrap()
            .tick(now.cast_unsigned())
            .into();
        // `rematerialize`, not the flag builder: the deprecated audience pair is
        // the shape under test — this fixture stands in for a record already on
        // disk, which the builders would refuse to produce.
        #[allow(deprecated)]
        let intent = kutl_signals::authoring::SignalIntent::rematerialize(
            kutl_signals::authoring::RecordEnvelope {
                space_id: space.to_owned(),
                document_id: None,
                signal_id: id.clone(),
                timestamp: now,
            },
            Some(Payload::Flag(sync::FlagPayload {
                kind: i32::from(sync::FlagKind::ReviewRequested),
                audience_type: i32::from(sync::AudienceType::Space),
                target_did: None,
                message: "space-level".into(),
                audience: None,
                anchor_text: None,
            })),
        );
        let mut rec =
            kutl_signals::authoring::assemble_record(&intent, "did:key:zAuthor", relay_hlc);
        crate::signal_record::attest_on_ingest(&mut rec, None, now);
        // Through the REPLICATED seam: this fixture seeds a legacy-shaped record
        // standing in for pre-existing history — the deprecated audience pair and
        // no typed audience — which the AUTHORED seam rightly refuses. Seeding
        // history and authoring a signal are different acts, which is why they
        // get different seams.
        relay
            .append_replicated_record(&rec, &id, space)
            .await
            .expect("append space-level signal");
        relay
            .rebuild_space_projection(&id, space)
            .await
            .expect("project space-level signal");
        id
    }

    /// Is this signal VISIBLE in the projection?
    ///
    /// Reads `signals_active`, not `signals`. A tombstone HIDES the row
    /// rather than deleting it (a delete would be unsurvivable — a REOPENED
    /// record carries no payload, so nothing incremental could bring the row
    /// back), so "is it projected" and "does the row exist" are not the same
    /// question. Every assertion here means the first one.
    async fn projected(pool: &sqlx::sqlite::SqlitePool, id: &str) -> bool {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signals_active WHERE id = ?")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap();
        n > 0
    }

    /// Fold the space's segments into `id → status`.
    async fn fold_status(
        relay: &Relay,
        space: Uuid,
    ) -> std::collections::BTreeMap<String, SignalStatus> {
        let mut state = SpaceSignalState::default();
        for record in relay
            .record_log
            .load_space(space)
            .await
            .expect("load records")
        {
            state.apply(record);
        }
        state
            .iter()
            .map(|(id, s)| (id.clone(), s.status.clone()))
            .collect()
    }

    /// The projection's visible signal ids for a space (from the `signals` table).
    async fn projected_ids(pool: &sqlx::sqlite::SqlitePool, space: &str) -> Vec<String> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT id FROM signals_active WHERE space_id = ? ORDER BY id")
                .bind(space)
                .fetch_all(pool)
                .await
                .unwrap();
        rows.into_iter().map(|(id,)| id).collect()
    }

    /// The projection must equal the fold's VISIBLE (non-tombstoned) signal set.
    async fn assert_fold_matches_projection(
        relay: &Relay,
        pool: &sqlx::sqlite::SqlitePool,
        space: Uuid,
    ) {
        let mut folded_visible: Vec<String> = fold_status(relay, space)
            .await
            .into_iter()
            .filter(|(_, s)| *s != SignalStatus::Tombstoned)
            .map(|(id, _)| id)
            .collect();
        folded_visible.sort();
        let projected = projected_ids(pool, &space.to_string()).await;
        assert_eq!(
            projected, folded_visible,
            "projection must equal the fold's visible set"
        );
    }

    /// Guard test 1 — DELETE HIDES: a doc's ≥2 signals are visible; after the
    /// delete cascade they are TOMBSTONED in the fold and GONE from the
    /// projection.
    #[tokio::test]
    async fn test_delete_cascade_tombstones_and_hides_doc_signals() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = durable_relay(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        let s1 = create_signal(&mut relay, &space_id, &doc).await;
        let s2 = create_signal(&mut relay, &space_id, &doc).await;

        // Precondition: both visible in the projection and Open in the fold.
        assert!(projected(&pool, &s1).await, "s1 visible before delete");
        assert!(projected(&pool, &s2).await, "s2 visible before delete");
        assert_fold_matches_projection(&relay, &pool, space).await;

        relay
            .cascade_document_delete_signals(&space_id, &doc, "did:key:zDeleter")
            .await
            .expect("delete cascade succeeds");

        // (a) TOMBSTONED in the fold.
        let folded = fold_status(&relay, space).await;
        assert_eq!(
            folded.get(&s1),
            Some(&SignalStatus::Tombstoned),
            "s1 must fold to Tombstoned after delete"
        );
        assert_eq!(
            folded.get(&s2),
            Some(&SignalStatus::Tombstoned),
            "s2 must fold to Tombstoned after delete"
        );
        // (b) GONE from the projection.
        assert!(!projected(&pool, &s1).await, "s1 hidden after delete");
        assert!(!projected(&pool, &s2).await, "s2 hidden after delete");
        assert_fold_matches_projection(&relay, &pool, space).await;
    }

    /// Guard test 2 — UNDELETE RESTORES: after a delete, reviving the doc
    /// reopens its tombstoned signals — Open in the fold and visible again in
    /// the projection.
    #[tokio::test]
    async fn test_revive_cascade_reopens_doc_signals() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = durable_relay(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        let s1 = create_signal(&mut relay, &space_id, &doc).await;
        let s2 = create_signal(&mut relay, &space_id, &doc).await;

        relay
            .cascade_document_delete_signals(&space_id, &doc, "did:key:zDeleter")
            .await
            .expect("delete cascade succeeds");
        assert!(!projected(&pool, &s1).await, "s1 hidden after delete");

        relay
            .cascade_document_revive_signals(&space_id, &doc, "did:key:zReviver")
            .await
            .expect("revive cascade succeeds");

        let folded = fold_status(&relay, space).await;
        assert_eq!(
            folded.get(&s1),
            Some(&SignalStatus::Open),
            "s1 must fold back to Open after revive"
        );
        assert_eq!(
            folded.get(&s2),
            Some(&SignalStatus::Open),
            "s2 must fold back to Open after revive"
        );
        assert!(projected(&pool, &s1).await, "s1 visible again after revive");
        assert!(projected(&pool, &s2).await, "s2 visible again after revive");
        assert_fold_matches_projection(&relay, &pool, space).await;
    }

    /// Guard test 3 — ISOLATION: deleting doc A leaves a DIFFERENT document's
    /// signal and a space-level signal (no `document_id`) untouched. The revive
    /// of A must likewise not disturb them.
    #[tokio::test]
    async fn test_cascade_is_scoped_to_the_document() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = durable_relay(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc_a = Uuid::new_v4().to_string();
        let doc_b = Uuid::new_v4().to_string();

        let a1 = create_signal(&mut relay, &space_id, &doc_a).await;
        let b1 = create_signal(&mut relay, &space_id, &doc_b).await;
        // A space-level signal with no document_id.
        let space_sig = create_space_level_signal(&mut relay, &space_id).await;

        relay
            .cascade_document_delete_signals(&space_id, &doc_a, "did:key:zDeleter")
            .await
            .expect("delete cascade succeeds");

        let folded = fold_status(&relay, space).await;
        assert_eq!(
            folded.get(&a1),
            Some(&SignalStatus::Tombstoned),
            "A's signal is tombstoned"
        );
        assert_eq!(
            folded.get(&b1),
            Some(&SignalStatus::Open),
            "B's signal is untouched by A's delete"
        );
        assert_eq!(
            folded.get(&space_sig),
            Some(&SignalStatus::Open),
            "the space-level signal is untouched by A's delete"
        );
        assert!(projected(&pool, &b1).await, "B's signal stays visible");
        assert!(
            projected(&pool, &space_sig).await,
            "space-level signal stays visible"
        );

        // Reviving A must not disturb B or the space-level signal either.
        relay
            .cascade_document_revive_signals(&space_id, &doc_a, "did:key:zReviver")
            .await
            .expect("revive cascade succeeds");
        let folded = fold_status(&relay, space).await;
        assert_eq!(folded.get(&a1), Some(&SignalStatus::Open), "A reopened");
        assert_eq!(folded.get(&b1), Some(&SignalStatus::Open), "B still open");
        assert_eq!(
            folded.get(&space_sig),
            Some(&SignalStatus::Open),
            "space-level still open"
        );
    }

    /// A delete cascade over a doc with NO signals is a pure no-op: no record is
    /// appended to the segments.
    #[tokio::test]
    async fn test_delete_cascade_no_signals_is_noop() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = durable_relay(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        let before = fold_status(&relay, space).await.len();
        relay
            .cascade_document_delete_signals(&space_id, &doc, "did:key:zDeleter")
            .await
            .expect("no-op cascade succeeds");
        assert_eq!(
            fold_status(&relay, space).await.len(),
            before,
            "an empty-doc delete appends nothing"
        );
    }

    /// Create a projected REPLY signal (`document_id = None`, carrying a
    /// `ReplyPayload` that points at `parent_signal_id`) and return its id. This
    /// mirrors `handle_mcp_create_reply`: replies are minted with no
    /// `document_id`, so the cascade must reach them through the parent link.
    async fn create_reply(relay: &mut Relay, space_id: &str, parent_signal_id: &str) -> String {
        let signal_id = Uuid::new_v4().to_string();
        let now = kutl_core::env::now_ms();
        let relay_hlc: kutl_proto::sync::Hlc = relay
            .signal_clock
            .lock()
            .unwrap()
            .tick(now.cast_unsigned())
            .into();
        // The real reply builder — this fixture is a well-formed reply, so it
        // exercises the production path rather than bypassing it.
        let intent = kutl_signals::authoring::SignalIntent::reply(
            kutl_signals::authoring::RecordEnvelope {
                space_id: space_id.to_owned(),
                document_id: None,
                signal_id: signal_id.clone(),
                timestamp: now,
            },
            parent_signal_id.to_owned(),
            None,
            "a reply".into(),
        )
        .expect("a well-formed reply intent");
        let mut rec =
            kutl_signals::authoring::assemble_record(&intent, "did:key:zReplier", relay_hlc);
        crate::signal_record::attest_on_ingest(&mut rec, None, now);
        relay
            .append_authored_record(&rec, &signal_id, space_id, None, Announce::Silent)
            .await
            .expect("append reply signal");
        relay
            .rebuild_space_projection(&signal_id, space_id)
            .await
            .expect("project reply signal");
        signal_id
    }

    /// Guard test — REPLIES HIDDEN ON DELETE: a flag on a doc
    /// plus a REPLY to that flag. Deleting the doc must tombstone BOTH the flag
    /// AND the reply (the reply carries no `document_id`, so it is reachable only
    /// via its parent link).
    #[tokio::test]
    async fn test_delete_cascade_tombstones_replies() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = durable_relay(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        let flag = create_signal(&mut relay, &space_id, &doc).await;
        let reply = create_reply(&mut relay, &space_id, &flag).await;
        // A second-level reply (reply to the reply) exercises the chain follow.
        let reply2 = create_reply(&mut relay, &space_id, &reply).await;

        assert!(projected(&pool, &flag).await, "flag visible before delete");
        assert!(
            projected(&pool, &reply).await,
            "reply visible before delete"
        );
        assert!(
            projected(&pool, &reply2).await,
            "reply2 visible before delete"
        );

        relay
            .cascade_document_delete_signals(&space_id, &doc, "did:key:zDeleter")
            .await
            .expect("delete cascade succeeds");

        let folded = fold_status(&relay, space).await;
        assert_eq!(
            folded.get(&flag),
            Some(&SignalStatus::Tombstoned),
            "flag tombstoned"
        );
        assert_eq!(
            folded.get(&reply),
            Some(&SignalStatus::Tombstoned),
            "direct reply tombstoned"
        );
        assert_eq!(
            folded.get(&reply2),
            Some(&SignalStatus::Tombstoned),
            "second-level reply tombstoned"
        );
        assert!(!projected(&pool, &flag).await, "flag hidden after delete");
        assert!(!projected(&pool, &reply).await, "reply hidden after delete");
        assert!(
            !projected(&pool, &reply2).await,
            "reply2 hidden after delete"
        );
        assert_fold_matches_projection(&relay, &pool, space).await;
    }

    /// Guard test — PROJECTION-ABSENT SIGNAL STILL TOMBSTONED:
    /// a signal whose CREATED is in segments but whose projection row is
    /// ABSENT (a create-time projection failure that "heals on next fold"). The
    /// cascade must still tombstone it — sourcing target ids from the FOLD, not
    /// the projection, which would skip it and leave the fold holding it Open.
    #[tokio::test]
    async fn test_delete_cascade_tombstones_projection_absent_signal() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = durable_relay(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        // A normal projected signal, plus one whose projection row we then
        // delete to simulate the accepted create-projection-failure posture
        // (record durable in segments; projection row absent, heals on fold).
        let projected_sig = create_signal(&mut relay, &space_id, &doc).await;
        let absent_sig = create_signal(&mut relay, &space_id, &doc).await;
        sqlx::query("DELETE FROM signals WHERE id = ?")
            .bind(&absent_sig)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            !projected(&pool, &absent_sig).await,
            "absent_sig row removed (simulated projection miss)"
        );

        relay
            .cascade_document_delete_signals(&space_id, &doc, "did:key:zDeleter")
            .await
            .expect("delete cascade succeeds");

        let folded = fold_status(&relay, space).await;
        assert_eq!(
            folded.get(&projected_sig),
            Some(&SignalStatus::Tombstoned),
            "projected signal tombstoned"
        );
        assert_eq!(
            folded.get(&absent_sig),
            Some(&SignalStatus::Tombstoned),
            "projection-absent signal must ALSO be tombstoned (fold-sourced)"
        );
    }

    /// Guard test — MATERIALIZE-AFTER-FOLD NOT ORPHANED:
    /// a document is soft-deleted, then a marker-materialized CREATED for that
    /// doc is drained. `handle_materialized_records` must NOT project it Open as
    /// an orphan on a soft-deleted document — it must tombstone it.
    #[tokio::test]
    async fn test_materialized_record_on_deleted_doc_is_tombstoned() {
        use crate::registry;

        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = durable_relay(&dir).await;
        // Watch the observer too: the orphan tombstone declares silence, and
        // this is the only test on its path, so the silence pin lives here.
        let observer = std::sync::Arc::new(super::catch_up_tests::RecordingObserver::default());
        relay.observer = observer.clone();
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        // One door-authored flag proves the observer records when a path DOES
        // announce, so the zero-delta silence assertion below is not vacuous.
        let live = create_signal(&mut relay, &space_id, &Uuid::new_v4().to_string()).await;
        assert!(!live.is_empty(), "setup: the liveness flag must land");
        assert_eq!(
            observer.announcement_count(),
            1,
            "setup: the observer is live"
        );

        // Soft-delete the document in the registry so the guard can see it.
        // The delete carries a strictly-later timestamp so it wins arbitration
        // over the register (both otherwise fold to the zero legacy HLC).
        let reg = relay.registries.entry(space_id.clone()).or_default();
        reg.register(
            &doc,
            "notes.md",
            registry::EntryMetadata {
                timestamp: 1_000,
                ..registry::EntryMetadata::default()
            },
        );
        reg.unregister(
            &doc,
            &registry::EntryMetadata {
                timestamp: 2_000,
                ..registry::EntryMetadata::default()
            },
        );
        assert!(
            reg.get_any(&doc).and_then(|e| e.deleted_at).is_some(),
            "doc soft-deleted"
        );

        // A materialized CREATED lands AFTER the doc is deleted (the orphan race).
        let late = Uuid::new_v4().to_string();
        let now = kutl_core::env::now_ms();
        let relay_hlc: kutl_proto::sync::Hlc = relay
            .signal_clock
            .lock()
            .unwrap()
            .tick(now.cast_unsigned())
            .into();
        // `rematerialize`: the deprecated audience pair is the shape under test —
        // a record already on disk, which no builder would produce.
        #[allow(deprecated)]
        let intent = kutl_signals::authoring::SignalIntent::rematerialize(
            kutl_signals::authoring::RecordEnvelope {
                space_id: space_id.clone(),
                document_id: Some(doc.clone()),
                signal_id: late.clone(),
                timestamp: now,
            },
            Some(Payload::Flag(sync::FlagPayload {
                kind: i32::from(sync::FlagKind::ReviewRequested),
                audience_type: i32::from(sync::AudienceType::Space),
                target_did: None,
                message: "late marker".into(),
                audience: None,
                anchor_text: None,
            })),
        );
        let mut rec =
            kutl_signals::authoring::assemble_record(&intent, "did:key:zAuthor", relay_hlc);
        crate::signal_record::attest_on_ingest(&mut rec, None, now);

        relay
            .handle_materialized_records(&space_id, vec![rec])
            .await;

        let folded = fold_status(&relay, space).await;
        assert_eq!(
            folded.get(&late),
            Some(&SignalStatus::Tombstoned),
            "a materialized signal on a soft-deleted doc must be tombstoned, not an Open orphan"
        );
        assert!(
            !projected(&pool, &late).await,
            "the late signal must not be a visible orphan in the projection"
        );
        // Neither the materialized ingest nor its orphan tombstone announces:
        // the signal is born already hidden, so there is nothing current to
        // tell anyone about.
        assert_eq!(
            observer.announcement_count(),
            1,
            "the orphan path must announce nothing beyond the liveness flag"
        );
    }

    /// Guard test — REVIVE REOPENS REPLIES TOO: after a delete that
    /// tombstoned a flag AND its reply, reviving the doc must reopen BOTH.
    #[tokio::test]
    async fn test_revive_cascade_reopens_replies() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = durable_relay(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        let flag = create_signal(&mut relay, &space_id, &doc).await;
        let reply = create_reply(&mut relay, &space_id, &flag).await;

        relay
            .cascade_document_delete_signals(&space_id, &doc, "did:key:zDeleter")
            .await
            .expect("delete cascade succeeds");
        assert!(!projected(&pool, &reply).await, "reply hidden after delete");

        relay
            .cascade_document_revive_signals(&space_id, &doc, "did:key:zReviver")
            .await
            .expect("revive cascade succeeds");

        let folded = fold_status(&relay, space).await;
        assert_eq!(
            folded.get(&flag),
            Some(&SignalStatus::Open),
            "flag reopened"
        );
        assert_eq!(
            folded.get(&reply),
            Some(&SignalStatus::Open),
            "reply reopened too"
        );
        assert!(projected(&pool, &flag).await, "flag visible again");
        assert!(projected(&pool, &reply).await, "reply visible again");
        assert_fold_matches_projection(&relay, &pool, space).await;
    }

    /// Guard test — BULK SPACE-DELETE CASCADES: after
    /// `handle_unregister_space` soft-deletes every document, the space's signals
    /// must be tombstoned + hidden.
    #[tokio::test]
    async fn test_bulk_space_delete_cascades_signals() {
        use crate::registry;

        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = durable_relay(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc_a = Uuid::new_v4().to_string();
        let doc_b = Uuid::new_v4().to_string();

        // Register both docs so `handle_unregister_space` has active entries.
        {
            let reg = relay.registries.entry(space_id.clone()).or_default();
            reg.register(&doc_a, "a.md", registry::EntryMetadata::default());
            reg.register(&doc_b, "b.md", registry::EntryMetadata::default());
        }

        let a1 = create_signal(&mut relay, &space_id, &doc_a).await;
        let b1 = create_signal(&mut relay, &space_id, &doc_b).await;
        let b_reply = create_reply(&mut relay, &space_id, &b1).await;

        // `handle_unregister_space` is driven for its projection side effects,
        // not its broadcast (the send helpers no-op on an unregistered conn).
        // Authentication is mandatory, so the driving conn must carry
        // an authorized identity or `authorize_conn` rejects before the cascade.
        relay.test_set_authenticated(1, TEST_CONN_DID);
        relay
            .handle_unregister_space(
                1,
                &sync::UnregisterSpace {
                    space_id: space_id.clone(),
                    metadata: None,
                },
            )
            .await;

        let folded = fold_status(&relay, space).await;
        assert_eq!(
            folded.get(&a1),
            Some(&SignalStatus::Tombstoned),
            "doc A's signal tombstoned by bulk delete"
        );
        assert_eq!(
            folded.get(&b1),
            Some(&SignalStatus::Tombstoned),
            "doc B's signal tombstoned by bulk delete"
        );
        assert_eq!(
            folded.get(&b_reply),
            Some(&SignalStatus::Tombstoned),
            "doc B's reply tombstoned by bulk delete"
        );
        assert!(!projected(&pool, &a1).await, "A's signal hidden");
        assert!(!projected(&pool, &b1).await, "B's signal hidden");
        assert!(!projected(&pool, &b_reply).await, "B's reply hidden");
        assert_fold_matches_projection(&relay, &pool, space).await;
    }

    /// A bulk space-delete of a space with NO active documents but WITH a
    /// space-level signal must still tombstone that signal — an empty-active-ids
    /// early return would skip the cascade entirely.
    #[tokio::test]
    async fn test_bulk_space_delete_with_no_active_docs_tombstones_space_level_signal() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, pool) = durable_relay(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();

        // A space-level signal, and NO registered documents (active_ids empty).
        let sig = create_space_level_signal(&mut relay, &space_id).await;
        assert!(
            projected(&pool, &sig).await,
            "space-level signal starts visible"
        );

        // Authentication is mandatory: the driving conn must carry an
        // authorized identity or `authorize_conn` rejects before the cascade.
        relay.test_set_authenticated(1, TEST_CONN_DID);
        relay
            .handle_unregister_space(
                1,
                &sync::UnregisterSpace {
                    space_id: space_id.clone(),
                    metadata: None,
                },
            )
            .await;

        assert_eq!(
            fold_status(&relay, space).await.get(&sig),
            Some(&SignalStatus::Tombstoned),
            "the space-level signal must be tombstoned even with no active docs"
        );
        assert!(!projected(&pool, &sig).await, "space-level signal hidden");
    }

    /// `resolve_belongs` must walk a very deep reply-parent chain without
    /// overflowing the stack (iterative, not recursive) and stay correct.
    #[test]
    fn test_resolve_belongs_deep_chain_and_cycle() {
        use std::collections::{HashMap, HashSet};

        // A chain sN..s0, deep enough that native recursion would overflow.
        const DEPTH: usize = 200_000;
        let mut parent: HashMap<String, String> = HashMap::new();
        for i in 0..DEPTH {
            parent.insert(format!("s{i}"), format!("s{}", i + 1));
        }

        // Chain terminates at a root → belongs.
        let mut roots: HashSet<String> = HashSet::new();
        roots.insert(format!("s{DEPTH}"));
        let mut belongs: HashMap<String, bool> = HashMap::new();
        assert!(
            super::resolve_belongs("s0", &roots, &parent, &mut belongs),
            "a deep chain reaching a root resolves true without overflow"
        );

        // Same deep chain with no root (dangling) → does not belong.
        let mut belongs2: HashMap<String, bool> = HashMap::new();
        assert!(
            !super::resolve_belongs("s0", &HashSet::new(), &parent, &mut belongs2),
            "a deep dangling chain resolves false without overflow"
        );

        // A cycle terminates (does not loop) and resolves false.
        let mut cyclic: HashMap<String, String> = HashMap::new();
        cyclic.insert("a".into(), "b".into());
        cyclic.insert("b".into(), "a".into());
        let mut belongs3: HashMap<String, bool> = HashMap::new();
        assert!(
            !super::resolve_belongs("a", &HashSet::new(), &cyclic, &mut belongs3),
            "a reply-parent cycle resolves false and terminates"
        );
    }
}
