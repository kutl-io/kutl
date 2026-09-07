//! Sync-ops ingest family of the relay actor: inbound text/blob `SyncOps`
//! dispatch, the text CRDT merge + per-subscriber outbound relay path, blob
//! LWW (server-authoritative timestamp + hash tiebreak), inbound quota
//! enforcement with the retry-bounded storage-delta ladder
//! ([`apply_delta_with_retry`]), and presence relay.
//!
//! Child module of the relay actor (`super`) so the `impl Relay` block here
//! reaches the actor's private fields directly. Pure relocation from
//! relay.rs — behavior and dispatch are unchanged; `process_command` in
//! relay.rs still routes inbound sync ops and presence updates to these
//! handlers, and the flush task keeps reaching [`apply_delta_with_retry`]
//! through its pre-split `crate::relay::` path.

use std::collections::HashMap;
use std::time::Duration;

use kutl_proto::sync::{self, ErrorCode};
use tracing::{debug, error, warn};

use crate::observer::{EditContentMode, MergedEvent};
use crate::protocol::{
    ABSOLUTE_BLOB_MAX, CapKind, blob_ops_envelope, encode_envelope, is_blob_mode,
    presence_update_envelope, quota_exceeded_refusal, refusal, sync_ops_envelope,
};
use crate::quota_backend::QuotaBackend;

use super::{
    BlobData, ConnChannels, ConnId, DocContent, DocKey, DocSlot, EditedAtPending, PendingEdit,
    Relay, relay_and_evict,
};

/// Sentinel `known_version` indicating the subscriber has the current blob.
pub(super) const BLOB_KNOWN: &[usize] = &[1];

/// Maximum `apply_storage_delta` attempts (1 initial + 2 retries).
const QUOTA_DELTA_MAX_ATTEMPTS: usize = 3;

/// Back-off (ms) slept between consecutive failed attempts. Length is
/// `QUOTA_DELTA_MAX_ATTEMPTS - 1`: no sleep after the final attempt.
/// Total sleep budget: 250 ms; typical call finishes in one DB round-trip.
const QUOTA_DELTA_BACKOFF_MS: [u64; QUOTA_DELTA_MAX_ATTEMPTS - 1] = [50, 200];

/// Apply a storage delta with bounded retries on transient failure.
///
/// Success returns `Ok(())` after the first successful apply. Failure on the
/// last attempt returns the underlying error — callers should log and
/// continue, because the blob is already in memory and the monthly
/// reconcile job will correct counter drift.
///
/// `pub(crate)` so the background flush task (`flush.rs`) can reuse the same
/// retry ladder without duplicating logic.
pub(crate) async fn apply_delta_with_retry(
    qb: &dyn QuotaBackend,
    space_id: &str,
    doc_id: &str,
    new_size: i64,
) -> anyhow::Result<()> {
    // Iterate the backoff table: each entry represents a sleep *before* the
    // next retry. If the attempt before the first retry succeeds we return
    // early; otherwise we sleep the prescribed interval and try again.
    for (attempt, &backoff_ms) in QUOTA_DELTA_BACKOFF_MS.iter().enumerate() {
        match qb.apply_storage_delta(space_id, doc_id, new_size).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                warn!(attempt, error = %e, "quota delta failed, retrying");
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }
    // Final attempt after all back-off sleeps are exhausted.
    qb.apply_storage_delta(space_id, doc_id, new_size)
        .await
        .map(|_| ())
}

impl Relay {
    pub(super) async fn handle_sync_ops(&mut self, conn_id: ConnId, msg: &sync::SyncOps) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_sync_ops_rejected(
                    conn_id,
                    &msg.space_id,
                    &msg.document_id,
                    refusal(ErrorCode::AuthFailed, e.to_string()),
                );
                return;
            }
        };

        let key = DocKey {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
        };

        if !self.documents.contains_key(&key) {
            self.send_sync_ops_rejected(
                conn_id,
                &msg.space_id,
                &msg.document_id,
                refusal(
                    ErrorCode::NotFound,
                    format!("no document {}/{}", msg.space_id, msg.document_id),
                ),
            );
            return;
        }

        if is_blob_mode(msg) {
            self.handle_blob_sync_ops(conn_id, msg, &key).await;
        } else {
            self.handle_text_sync_ops(conn_id, msg, &key).await;
        }
    }

    /// Answer a rejected text merge: the sender gets the classified error,
    /// and an engine panic additionally retires the document, whose engine
    /// the contained panic may have left half-applied.
    pub(super) fn reject_text_merge(
        &mut self,
        conn_id: ConnId,
        msg: &sync::SyncOps,
        key: &DocKey,
        e: &kutl_core::Error,
    ) {
        let err = classify_merge_reject(conn_id, msg, e);
        self.send_sync_ops_rejected(conn_id, &msg.space_id, &msg.document_id, err);
        if matches!(e, kutl_core::Error::EnginePanicked { .. }) {
            self.discard_poisoned_document(key);
        }
    }

    /// Handle inbound text CRDT sync ops.
    async fn handle_text_sync_ops(&mut self, conn_id: ConnId, msg: &sync::SyncOps, key: &DocKey) {
        // Quota pre-check: even without an inline delta, gate text inbound
        // on `storage_bytes_used >= storage_bytes_total` so an account at
        // cap cannot keep accumulating durable CRDT bytes. The flush task
        // updates the counter post-write. `actual_bytes` is unused by the
        // text path (per_blob check is disabled) — pass 0.
        if let Err(err) = self.check_inbound_quota(&msg.space_id, 0, false).await {
            self.send_sync_ops_rejected(conn_id, &msg.space_id, &msg.document_id, err);
            return;
        }

        // Pin observer / event `author_did` to the connection's authenticated
        // identity so a peer cannot attribute its edits to another user in
        // after-merge events, MCP notifications, or the edited_at_pending
        // flush. Authentication is mandatory, so the authenticated
        // DID always wins over any client-supplied `author_did` — and the
        // PAT half comes with it, because it says which of the person's
        // forms acted and the client does not get to claim one.
        let identity = self.authoritative_identity(conn_id);
        let authoritative = identity.did;
        let authoritative_pat = identity.via_pat_id;

        let slot = self.documents.get_mut(key).expect("slot checked by caller");

        // Ensure content is Text (transition from Empty or Blob if needed).
        if slot.ensure_text() {
            // Was Blob — reset subscriber versions since content type changed.
            slot.subscribers.reset_all_versions();
        }

        let mut meta = extract_sync_edit_meta(msg, slot, self.config.snippet_max_doc_chars);
        meta.author_did.clone_from(&authoritative);

        // If a different author has a pending edit, flush it before merge so
        // the observer sees the pre-merge content. This must happen
        // unconditionally (not gated on snippet_eligible) because the new
        // edit may be non-eligible (client snippet, doc too large) but the
        // *pending* edit still needs to be flushed correctly.
        // The comparison is on the pair, not the DID: a token binds to its
        // owner's key, so a person and their agent arrive under one DID. Keying
        // the burst on the DID alone lets human-typed and agent-typed edits
        // accumulate together, and whichever credential the flush then stamped
        // would be wrong for the other half.
        if let Some(ref pending) = slot.pending_edit
            && (pending.author_did != meta.author_did || pending.via_pat_id != authoritative_pat)
        {
            self.flush_pending_edit_for(key).await;
        }

        // Notify before-merge observer (captures baseline for snippet/mention diffing).
        {
            let slot = self.documents.get(key).expect("slot checked by caller");
            if let DocContent::Text(doc) = &slot.content {
                self.before_merge
                    .before_text_merge(&msg.space_id, &msg.document_id, doc);
            }
        }

        // Merge ops. Distinguish the per-document op-count cap and per-patch
        // size cap — both surface to the client as
        // `QUOTA_EXCEEDED` with a typed `cap_kind` so the UI can pick the
        // right copy. Other merge errors remain `InvalidMessage`.
        let slot = self.documents.get_mut(key).expect("slot checked by caller");
        // `msg` is a separate borrow from `self.documents`, so the merge can
        // read `msg.ops`/`msg.metadata` directly — no need to clone the op
        // payload (KBs) + metadata Vec on this hottest relay path.
        if let Err(e) = slot.mutate_text(|doc| doc.merge(&msg.ops, &msg.metadata)) {
            self.reject_text_merge(conn_id, msg, key, &e);
            return;
        }

        // Mark the doc as having a pending edited_at flush. The write is
        // performed by flush_pending_edit_for, which fires at most once per
        // snippet_debounce_ms per doc (or immediately for snippet-ineligible
        // edits via emit_text_edit_event).
        slot.edited_at_pending = Some(EditedAtPending {
            timestamp: kutl_core::env::now_ms(),
            author_did: meta.author_did.clone(),
        });

        build_and_relay_text_outbound(
            &self.connections,
            slot,
            key,
            Some(conn_id),
            &msg.space_id,
            &msg.document_id,
        );

        // Edit-revives lifecycle TOUCH + revive detection, deferred to here
        // (past `build_and_relay_text_outbound`) so the `slot` borrow is
        // released before this `&mut self` call. The touch is independent of the
        // outbound relay (it raises registry liveness, not slot content), so the
        // order does not affect what peers receive.
        let edit_revived =
            self.touch_and_detect_revive(&msg.space_id, &msg.document_id, &msg.metadata);

        // Deletion ladder: when the touch above REVIVED a soft-deleted
        // doc (its `deleted_at` transitioned set → cleared), reopen the doc's
        // tombstoned signals so they become visible again. This is the ONLY
        // set→cleared site in the relay — `register_or_reregister` REJECTS a
        // re-register of a soft-deleted id (it never un-deletes), so the
        // edit-touch is the sole revive path; funnel every revive through the
        // one cascade helper so revive entry points can't drift. Then
        // re-assert the document to the space: the delete it beat was already
        // broadcast and honored, so every daemon (the editor included) has
        // dropped the file and must re-materialize it.
        if edit_revived {
            self.cascade_revive_signals_logged(&msg.space_id, &msg.document_id, &authoritative)
                .await;
            self.broadcast_revival_register(&msg.space_id, &msg.document_id, conn_id)
                .await;
        }

        // Notify MCP sessions of the inbound edit. Pass the authenticated
        // DID so the notification reports the real editor even when the
        // inbound metadata was forged.
        self.notify_mcp_sessions_from_ws(
            &msg.space_id,
            &msg.document_id,
            &msg.metadata,
            &authoritative,
        );

        // Emit the edit event (immediate or deferred depending on snippet eligibility).
        self.emit_text_edit_event(
            key,
            &msg.space_id,
            &msg.document_id,
            meta.author_did,
            authoritative_pat,
            meta.intent,
            meta.op_count,
            meta.client_snippet.as_deref(),
            meta.snippet_eligible,
        )
        .await;
    }

    /// Raise the document's edit-revives lifecycle TOUCH to the edit's ORIGIN
    /// time and report whether that touch actually REVIVED a soft-deleted doc.
    ///
    /// The touch raises the doc's `touched_hlc` to the max origin HLC across the
    /// ops' metadata, so an edit concurrent with a delete keeps the document
    /// alive. Content ops carry only a physical timestamp, so the touch is
    /// physical-ms dominant (see `Hlc::physical_touch`): a stale offline edit
    /// carries its old time and cannot revive past a causally-later delete,
    /// while a concurrent edit beats it.
    ///
    /// Returns `true` when the doc's `deleted_at` transitioned set → cleared
    /// (soft-deleted before the touch, alive after) — the revive
    /// signal. Detected by sampling liveness across the touch. `false` when the
    /// metadata carries no origin HLC or the space is unknown.
    fn touch_and_detect_revive(
        &mut self,
        space_id: &str,
        document_id: &str,
        metadata: &[sync::ChangeMetadata],
    ) -> bool {
        let Some(edit_hlc) = max_origin_hlc(metadata) else {
            return false;
        };
        let Some(reg) = self.registries.get_mut(space_id) else {
            return false;
        };
        let was_soft_deleted = reg
            .get_any(document_id)
            .is_some_and(|e| e.deleted_at.is_some());
        reg.touch(document_id, edit_hlc);
        was_soft_deleted && reg.get(document_id).is_some()
    }

    /// Emit or defer the edit event depending on snippet eligibility.
    ///
    /// - Client snippet present OR not snippet-eligible: flush any pending
    ///   edit, then call `after_text_merge` immediately.
    /// - Snippet-eligible (no client snippet): accumulate into `PendingEdit`
    ///   or create a new one, reset the debounce timer.
    #[allow(clippy::too_many_arguments)]
    async fn emit_text_edit_event(
        &mut self,
        key: &DocKey,
        space_id: &str,
        document_id: &str,
        author_did: String,
        via_pat_id: Option<String>,
        intent: String,
        op_count: usize,
        client_snippet: Option<&str>,
        snippet_eligible: bool,
    ) {
        if client_snippet.is_some() || !snippet_eligible {
            self.flush_pending_edit_for(key).await;
            // `invoke_after_merge` seeds the observer's baseline once per doc
            // (restart correctness) and fires only for text content.
            let event = MergedEvent {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                author_did,
                via_pat_id,
                op_count,
                intent,
                content_mode: EditContentMode::Text,
                timestamp: kutl_core::env::now_ms(),
            };
            self.invoke_after_merge(key, event).await;
        } else {
            let slot = self.documents.get_mut(key).expect("slot checked by caller");
            if let Some(ref mut pending) = slot.pending_edit {
                // Same author (different author was already flushed) — accumulate.
                pending.op_count += op_count;
                pending.intent.clone_from(&intent);
            } else {
                slot.pending_edit = Some(PendingEdit {
                    author_did,
                    via_pat_id,
                    intent,
                    op_count,
                    timestamp: kutl_core::env::now_ms(),
                });
            }
            self.start_snippet_timer(space_id, document_id);
        }
    }

    /// Load and validate the storage quota for a space on the inbound path.
    ///
    /// Resolves to `Ok(())` when the write may proceed: no quota backend is
    /// configured (the OSS relay), or every cap passes given `actual_bytes`.
    /// Resolves to `Err(refusal)` when a cap is hit, the space is not found,
    /// or the load itself fails; the caller sends it as the frame's
    /// `SyncOpsRejected` and returns.
    fn check_inbound_quota(
        &self,
        space_id: &str,
        actual_bytes: i64,
        check_per_blob: bool,
    ) -> impl Future<Output = Result<(), sync::Error>> + Send + use<> {
        // Nothing is copied unless there is a backend to ask.
        let job = self
            .quota_backend
            .clone()
            .map(|qb| (qb, space_id.to_owned()));
        async move {
            let Some((qb, space_id)) = job else {
                return Ok(());
            };
            match qb.load_space_quota(&space_id).await {
                Ok(Some(q)) => {
                    if check_per_blob && actual_bytes > q.per_blob_size {
                        return Err(quota_exceeded_refusal(
                            CapKind::BlobSize,
                            q.per_blob_size,
                            None,
                        ));
                    }
                    if q.storage_bytes_used >= q.storage_bytes_total {
                        return Err(quota_exceeded_refusal(
                            CapKind::StorageTotal,
                            q.storage_bytes_total,
                            Some(q.storage_bytes_used),
                        ));
                    }
                    Ok(())
                }
                Ok(None) => Err(refusal(ErrorCode::NotFound, "space not found")),
                Err(e) => {
                    error!(%space_id, error = %e, "quota load failed; denying write");
                    Err(quota_exceeded_refusal(CapKind::StorageTotal, 0, None))
                }
            }
        }
    }

    /// Handle inbound binary blob sync ops (LWW, no merging).
    #[expect(
        clippy::too_many_lines,
        reason = "linear blob-upload pipeline: pre-check, quota check, LWW guard, in-memory \
                  slot mutation, edited_at + author DID capture, inline retry-bounded \
                  storage delta + observer notification + flush — one line over the \
                  100-line guideline"
    )]
    async fn handle_blob_sync_ops(&mut self, conn_id: ConnId, msg: &sync::SyncOps, key: &DocKey) {
        // Flush any pending text edit — blob ops are not snippet-eligible.
        self.flush_pending_edit_for(key).await;

        // Reject BLOB_REF — not implemented.
        if msg.content_mode == i32::from(sync::ContentMode::BlobRef) {
            self.send_sync_ops_rejected(
                conn_id,
                &msg.space_id,
                &msg.document_id,
                refusal(ErrorCode::InvalidMessage, "BLOB_REF is not implemented"),
            );
            return;
        }

        // msg.blob_size is client-advertised and untrusted. All
        // enforcement below uses msg.ops.len() (server-measured). We keep a
        // debug log to spot clients whose advertised size diverges from
        // actual — purely observational.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        if msg.blob_size as usize != msg.ops.len() {
            debug!(
                advertised = msg.blob_size,
                actual = msg.ops.len(),
                "client-advertised blob_size differs from actual payload size"
            );
        }

        // The WS codec caps the FRAME at `WS_MESSAGE_MAX` (the blob cap
        // plus envelope headroom), so a blob between the policy cap and the
        // frame cap arrives intact; the policy cap on the actual bytes is
        // enforced here and nowhere earlier.
        if msg.ops.len() > ABSOLUTE_BLOB_MAX {
            #[allow(clippy::cast_possible_wrap)]
            let limit = ABSOLUTE_BLOB_MAX as i64;
            self.send_sync_ops_rejected(
                conn_id,
                &msg.space_id,
                &msg.document_id,
                quota_exceeded_refusal(CapKind::BlobSize, limit, None),
            );
            return;
        }

        // Quota-backend pre-check (skipped when no backend is configured).
        // Reject based on server-measured `msg.ops.len()` — never on the
        // advertised `msg.blob_size`.
        #[allow(clippy::cast_possible_wrap)]
        let actual_len = msg.ops.len() as i64;
        if let Err(err) = self
            .check_inbound_quota(&msg.space_id, actual_len, true)
            .await
        {
            self.send_sync_ops_rejected(conn_id, &msg.space_id, &msg.document_id, err);
            return;
        }

        // Server-authoritative LWW key: never trust the client-supplied
        // metadata timestamp. A peer sending `timestamp = i64::MAX` would
        // otherwise permanently poison the blob (all later real writes carry a
        // smaller `now_ms()` and would be dropped). Capture once and use the
        // same value as the LWW comparison key and the stored timestamp — the
        // text path stamps `now_ms()` the same way.
        let timestamp = kutl_core::env::now_ms();

        // Authoritative content hash: hash the actual ops bytes rather than
        // trusting the client-supplied `content_hash`. Used for the LWW
        // tiebreak, the stored hash, and the hash forwarded to peers, so the
        // relay and every receiving daemon (which recompute the hash locally)
        // order an equal-timestamp tie the same way — a forged `content_hash`
        // otherwise lets them pick different winners. Matches the load path's
        // `compute_blob_hash(&bytes)`.
        let content_hash = compute_blob_hash(&msg.ops).to_vec();

        // Pin `author_did` to the connection's authenticated identity, exactly
        // like the text path and the MCP write paths — a peer must not be able
        // to attribute a blob write to another user in the `edited_at` flush
        // or observer fan-out. Captured before the slot borrow.
        let author_did = self.authoritative_author_did(conn_id).to_owned();
        let via_pat_id = self.authoritative_via_pat_id(conn_id);

        let slot = self.documents.get_mut(key).expect("slot checked by caller");

        // LWW: only accept if newer than current blob. On an exact timestamp
        // tie (two writes in the same millisecond), break deterministically on
        // the content hash — accept only when the incoming hash is strictly
        // greater than the resident one. This guarantees all replicas converge
        // on the same blob regardless of arrival order.
        if let DocContent::Blob(existing) = &slot.content {
            if timestamp < existing.timestamp {
                return;
            }
            if timestamp == existing.timestamp && content_hash <= existing.hash {
                return;
            }
        }

        // Store the blob (transition from any previous content type).
        slot.set_blob(BlobData {
            content: msg.ops.clone(),
            hash: content_hash.clone(),
            timestamp,
        });

        // Advance edited_at so get_changes consumers see the blob edit.
        // Uses the same edited_at_pending mechanism as the text path.
        slot.edited_at_pending = Some(EditedAtPending {
            timestamp: kutl_core::env::now_ms(),
            author_did: author_did.clone(),
        });

        // Apply the storage delta INLINE (not spawned) before
        // we fan out. Spawning would let back-to-back PUTs each pass the
        // pre-check in parallel and overshoot by N × per_blob_size. Awaiting
        // inline serializes through the actor's single-task loop and bounds
        // overshoot to `in_flight × per_blob_size` per connection.
        //
        // On retry-exhaustion we log + continue — the blob is in memory and
        // will flush; the monthly reconcile corrects any counter drift.
        if let Some(ref qb) = self.quota_backend {
            #[allow(clippy::cast_possible_wrap)]
            let new_size = msg.ops.len() as i64;
            if let Err(e) =
                apply_delta_with_retry(qb.as_ref(), &msg.space_id, &msg.document_id, new_size).await
            {
                error!(
                    space_id = %msg.space_id,
                    doc_id = %msg.document_id,
                    error = %e,
                    "quota delta failed after retries — reconcile will correct",
                );
            }
        }

        // Build the relay envelope for other subscribers. Forward the
        // server-authoritative timestamp and computed hash, NOT the client's:
        // peer daemons key their blob LWW on the metadata timestamp, so
        // forwarding a forged client timestamp (e.g. i64::MAX) would poison
        // every peer even though the relay's own state is protected. This
        // matches the catch-up path, which sends the stored server timestamp.
        let forwarded_metadata = msg.metadata.first().map(|m| {
            let mut m = m.clone();
            m.timestamp = timestamp;
            m
        });
        let relay_envelope = blob_ops_envelope(
            &msg.space_id,
            &msg.document_id,
            msg.ops.clone(),
            content_hash.clone(),
            forwarded_metadata,
        );
        let relay_bytes = encode_envelope(&relay_envelope);

        // Mark sender as having the current blob.
        if let Some(entry) = slot.subscribers.get_mut(&conn_id) {
            entry.known_version = BLOB_KNOWN.to_vec();
        }

        // Build outbound for other subscribers.
        let outbound: Vec<(ConnId, Vec<u8>)> = slot
            .subscribers
            .active_entries()
            .filter(|&(id, _)| id != conn_id)
            .map(|(id, _)| (id, relay_bytes.clone()))
            .collect();

        relay_and_evict(&self.connections, slot, key, outbound, BLOB_KNOWN, 0);

        // Notify observer about the blob edit, with the same pinned identity.
        self.observer.on_blob_edited(MergedEvent {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
            author_did,
            via_pat_id,
            op_count: 1,
            intent: "blob".into(),
            content_mode: EditContentMode::Blob,
            timestamp: kutl_core::env::now_ms(),
        });

        // Flush the edited_at write immediately (blob uploads are
        // complete on arrival — no debounce window like text edits).
        self.flush_pending_edit_for(key).await;
    }

    // -----------------------------------------------------------------------
    // Presence relay
    // -----------------------------------------------------------------------

    /// Relay a presence update to all other subscribers of the same document.
    ///
    /// The relay performs no presence logic — it is a pure relay. The sender's
    /// presence is forwarded as-is to every other subscriber of the document.
    pub(super) async fn handle_presence_update(
        &mut self,
        conn_id: ConnId,
        msg: &sync::PresenceUpdate,
    ) {
        if self.authorize_conn(conn_id, &msg.space_id).await.is_err() {
            return;
        }
        let key = DocKey {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
        };

        let Some(slot) = self.documents.get(&key) else {
            return; // not subscribed to this document
        };

        // Pin `participant_did` to the authenticated identity so an
        // authenticated peer cannot broadcast cursor positions labelled as
        // another user.
        let participant_did = self.authoritative_author_did(conn_id);

        let envelope = presence_update_envelope(
            &msg.space_id,
            &msg.document_id,
            participant_did,
            msg.cursor_pos,
            msg.custom_data.clone(),
        );
        let bytes = encode_envelope(&envelope);

        for (sub_conn_id, _) in slot.subscribers.active_entries() {
            if sub_conn_id == conn_id {
                continue; // don't echo back to sender
            }
            if let Some(channels) = self.connections.get(&sub_conn_id) {
                let _ = channels.data.try_send(bytes.clone());
            }
        }
    }
}

/// Log a failed inbound text merge and build the refusal the sender gets
/// back in its `SyncOpsRejected`.
///
/// The per-document op-count cap and per-patch size cap surface as
/// `QUOTA_EXCEEDED` with a typed `cap_kind` so the UI can pick the right
/// copy. Everything else is `InvalidMessage`. The `SyncOpsRejected` that
/// carries it names the document, so the client acts on the structured
/// fields; an unscoped reject would degrade into a log line while the
/// sender keeps re-submitting the same unmergeable history.
fn classify_merge_reject(
    conn_id: ConnId,
    msg: &sync::SyncOps,
    e: &kutl_core::Error,
) -> sync::Error {
    match e {
        // ERROR, not warn: an at-cap document is unrecoverable data
        // divergence — the sender keeps the edit, no peer ever sees it.
        kutl_core::Error::OpCountExceeded { .. } => error!(
            conn_id,
            space_id = %msg.space_id,
            document_id = %msg.document_id,
            error = %e,
            "rejecting inbound ops: document at the op cap"
        ),
        // ERROR: no well-formed peer produces bytes that crash the engine,
        // and the document is being retired with its unflushed edits.
        kutl_core::Error::EnginePanicked { .. } => error!(
            conn_id,
            space_id = %msg.space_id,
            document_id = %msg.document_id,
            error = %e,
            "rejecting inbound ops: engine panicked; retiring the document"
        ),
        // Doc identity on the reject, not just the connection: a client
        // stuck re-sending an unmergeable batch produces hundreds of these,
        // and an operator triaging them needs "which document" without
        // correlating conn_ids by hand.
        _ => warn!(
            conn_id,
            space_id = %msg.space_id,
            document_id = %msg.document_id,
            error = %e,
            "failed to merge inbound ops"
        ),
    }
    match e {
        kutl_core::Error::OpCountExceeded { cap, .. } => {
            #[allow(clippy::cast_possible_wrap)]
            let limit = *cap as i64;
            quota_exceeded_refusal(CapKind::OpCount, limit, None)
        }
        kutl_core::Error::PatchTooLarge { cap, .. } => {
            #[allow(clippy::cast_possible_wrap)]
            let limit = *cap as i64;
            quota_exceeded_refusal(CapKind::BlobSize, limit, None)
        }
        _ => refusal(ErrorCode::InvalidMessage, e.to_string()),
    }
}

/// Fields extracted from inbound `SyncOps` metadata, used for event emission
/// and snippet eligibility decisions.
struct SyncEditMeta {
    author_did: String,
    intent: String,
    /// Client-supplied snippet, if non-empty.
    client_snippet: Option<String>,
    /// Number of metadata entries (one per op group).
    op_count: usize,
    /// Whether this edit qualifies for relay-side snippet computation.
    snippet_eligible: bool,
}

/// Extract edit metadata from a `SyncOps` message and the current slot state.
///
/// `snippet_max_doc_chars` comes from `RelayConfig`. Zero disables relay-side
/// snippet computation.
fn extract_sync_edit_meta(
    msg: &sync::SyncOps,
    slot: &DocSlot,
    snippet_max_doc_chars: usize,
) -> SyncEditMeta {
    let first = msg.metadata.first();
    let author_did = first.map_or_else(String::new, |m| m.author_did.clone());
    let intent = first.map_or_else(String::new, |m| m.intent.clone());
    let client_snippet = first
        .map(|m| &m.change_snippet)
        .filter(|s| !s.is_empty())
        .cloned();
    let op_count = msg.metadata.len();
    let snippet_eligible = snippet_max_doc_chars > 0
        && client_snippet.is_none()
        && matches!(&slot.content, DocContent::Text(doc) if doc.len() <= snippet_max_doc_chars);
    SyncEditMeta {
        author_did,
        intent,
        client_snippet,
        op_count,
        snippet_eligible,
    }
}

/// Build per-subscriber outbound deltas from the current text document and
/// relay them, evicting stale subscribers.
///
/// Also advances the sending connection's `known_version` to the relay's
/// current version (the sender does not receive its own ops back).
///
/// `exclude_conn_id` is the WebSocket sender whose connection should be
/// skipped (it receives an ack via the control channel, not a relay copy).
/// Pass `None` for MCP edits which have no WS sender.
///
/// Free function to avoid borrow conflicts between `self.documents` and
/// `self.connections`.
pub(super) fn build_and_relay_text_outbound(
    connections: &HashMap<ConnId, ConnChannels>,
    slot: &mut DocSlot,
    key: &DocKey,
    exclude_conn_id: Option<ConnId>,
    space_id: &str,
    document_id: &str,
) {
    let DocContent::Text(doc) = &slot.content else {
        unreachable!();
    };
    let hub_version = doc.local_version();

    // Build per-subscriber outbound deltas, skipping the sender if any.
    let mut outbound: Vec<(ConnId, Vec<u8>)> = Vec::new();
    for (sub_conn_id, entry) in slot.subscribers.active_entries() {
        if Some(sub_conn_id) == exclude_conn_id {
            continue;
        }
        let delta = doc.encode_since(&entry.known_version);
        if delta.is_empty() {
            continue;
        }
        let metadata = doc.changes_since(&entry.known_version);
        // Incremental broadcast delta: the per-change `metadata` carries the
        // bindings the receiver needs, so no author snapshot rides here (only
        // catch-up carries the uncapped map).
        let envelope = sync_ops_envelope(
            space_id,
            document_id,
            delta,
            metadata,
            std::collections::HashMap::new(),
        );
        outbound.push((sub_conn_id, encode_envelope(&envelope)));
    }

    // Advance sender's known_version so it won't receive its own ops back.
    if let Some(sender) = exclude_conn_id
        && let Some(entry) = slot.subscribers.get_mut(&sender)
    {
        entry.known_version.clone_from(&hub_version);
    }

    let relay_ops_count = doc.changes().len() as u64;
    relay_and_evict(
        connections,
        slot,
        key,
        outbound,
        &hub_version,
        relay_ops_count,
    );
}

/// SHA-256 hash of blob bytes, matching the WS blob path's
/// `content_hash` field.
///
/// Returned as a fixed-size `[u8; 32]` — SHA-256 is always 32 bytes,
/// so a heap-allocated `Vec<u8>` would lose type information at no
/// runtime benefit. Persistence types (`BlobRecord`, `BlobData`) store
/// `Vec<u8>` because they're algorithm-agnostic at the storage layer;
/// callers convert with `.to_vec()` at the boundary.
pub(super) fn compute_blob_hash(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

/// The origin HLC of one op's metadata: its real HLC if it carries one,
/// otherwise an edit-revives liveness touch derived from its physical
/// `timestamp` (content ops carry only a wall-clock timestamp — see
/// [`kutl_core::Hlc::physical_touch`]). `None` only when neither is usable.
///
/// A negative `timestamp` (never a valid Unix-ms wall clock) is dropped rather
/// than fabricating a touch — unlike a lifecycle op, an edit may simply not
/// touch the lattice, so there is nothing to default. (Lifecycle ops instead
/// floor a bad timestamp to 0 in `registry::hlc_of`, because a delete/rename
/// must always carry a stamp.)
fn origin_hlc(m: &sync::ChangeMetadata) -> Option<kutl_core::Hlc> {
    if let Some(h) = m.hlc.clone().and_then(|h| kutl_core::Hlc::try_from(h).ok()) {
        return Some(h);
    }
    u64::try_from(m.timestamp)
        .ok()
        .map(kutl_core::Hlc::physical_touch)
}

/// The maximum origin HLC across a set of op metadata, or `None` if none carry
/// a usable stamp. Pure function. Used to stamp the lifecycle touch for a
/// content edit (edit-revives) with the edit's own origin time — so the
/// touch orders in the lattice exactly like the edit that produced it.
fn max_origin_hlc(metadata: &[sync::ChangeMetadata]) -> Option<kutl_core::Hlc> {
    metadata.iter().filter_map(origin_hlc).max()
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use kutl_proto::sync::sync_envelope::Payload;

    use super::super::{
        RelayCommand, TEST_CONN_DID, connect_client, resident_blob, subscribe_doc, test_config,
    };
    use super::*;

    /// Every payload waiting on an own-ack lane, decoded.
    fn ack_payloads(rx: &mut mpsc::UnboundedReceiver<Vec<u8>>) -> Vec<Payload> {
        let mut out = Vec::new();
        while let Ok(bytes) = rx.try_recv() {
            if let Some(payload) = kutl_proto::protocol::decode_envelope(&bytes)
                .ok()
                .and_then(|env| env.payload)
            {
                out.push(payload);
            }
        }
        out
    }

    fn stale_reasons(payloads: &[Payload]) -> Vec<sync::StaleReason> {
        payloads
            .iter()
            .filter_map(|p| match p {
                Payload::StaleSubscriber(s) => Some(s.reason()),
                _ => None,
            })
            .collect()
    }

    /// The engine-panic contract: the sender is told the message was
    /// invalid, the document is retired, and every subscriber is told to
    /// re-subscribe with the `DocumentReloaded` reason. An ordinary decode
    /// error (the control) rejects without retiring anything.
    #[tokio::test]
    async fn test_engine_panic_reject_retires_the_document_and_notifies_subscribers() {
        const SPACE: &str = "5171e0a1-1111-4000-8000-000000000001";
        let mut relay = Relay::new_standalone(test_config());
        let (_sender_data, mut sender_ack, _sender_ctrl) = connect_client(&mut relay, 1).await;
        let (_peer_data, mut peer_ack, _peer_ctrl) = connect_client(&mut relay, 2).await;
        subscribe_doc(&mut relay, 1, SPACE, "doc").await;
        subscribe_doc(&mut relay, 2, SPACE, "doc").await;
        let _ = ack_payloads(&mut sender_ack);
        let _ = ack_payloads(&mut peer_ack);
        let msg = sync::SyncOps {
            space_id: SPACE.into(),
            document_id: "doc".into(),
            ..Default::default()
        };
        let key = DocKey {
            space_id: SPACE.into(),
            document_id: "doc".into(),
        };

        let decode = kutl_core::Error::Decode("bad magic".into());
        relay.reject_text_merge(1, &msg, &key, &decode);
        assert!(
            relay.has_document(SPACE, "doc"),
            "a decode error keeps the document"
        );
        assert!(stale_reasons(&ack_payloads(&mut peer_ack)).is_empty());
        let sender_seen = ack_payloads(&mut sender_ack);
        assert!(
            sender_seen.iter().any(|p| matches!(
                p,
                Payload::SyncOpsRejected(r)
                    if r.error.as_ref().is_some_and(|e| e.code() == ErrorCode::InvalidMessage)
            )),
            "the sender is told the message was invalid: {sender_seen:?}"
        );

        let panicked = kutl_core::Error::EnginePanicked {
            message: "index out of bounds".into(),
        };
        relay.reject_text_merge(1, &msg, &key, &panicked);
        assert!(
            !relay.has_document(SPACE, "doc"),
            "an engine panic retires the document"
        );
        assert_eq!(
            stale_reasons(&ack_payloads(&mut peer_ack)),
            vec![sync::StaleReason::DocumentReloaded]
        );
        let sender_seen = ack_payloads(&mut sender_ack);
        assert!(
            sender_seen.iter().any(|p| matches!(
                p,
                Payload::SyncOpsRejected(r)
                    if r.error.as_ref().is_some_and(|e| e.code() == ErrorCode::InvalidMessage)
            )),
            "the sender is told the message was invalid: {sender_seen:?}"
        );
        assert_eq!(
            stale_reasons(&sender_seen),
            vec![sync::StaleReason::DocumentReloaded],
            "the sender is a subscriber too and reloads like the rest"
        );
    }

    /// An edit whose touch revives a soft-deleted document re-asserts it to
    /// the space, the editor included: the delete it beat was broadcast and
    /// honored before the edit arrived, so every daemon dropped its file.
    #[tokio::test]
    async fn test_edit_that_revives_a_deleted_document_is_broadcast_as_a_register() {
        const SPACE: &str = "5171e0a1-1111-4000-8000-000000000001";
        const DOC: &str = "0f0f0f0f-0000-4000-8000-000000000001";
        let mut relay = Relay::new_standalone(test_config());
        let (_editor_data, mut editor_ack, _editor_ctrl) = connect_client(&mut relay, 1).await;
        let (_deleter_data, _deleter_ack, mut deleter_ctrl) = connect_client(&mut relay, 2).await;
        let stamp = |ms: u64| {
            Some(sync::ChangeMetadata {
                author_did: "did:test".into(),
                timestamp: i64::try_from(ms).expect("test stamp fits i64"),
                hlc: Some(kutl_proto::sync::Hlc::from(kutl_core::Hlc::physical_touch(
                    ms,
                ))),
                ..Default::default()
            })
        };
        relay
            .process_command(RelayCommand::RegisterDocument {
                conn_id: 1,
                msg: sync::RegisterDocument {
                    space_id: SPACE.into(),
                    document_id: DOC.into(),
                    path: "doc.md".into(),
                    metadata: stamp(1_000),
                    ..Default::default()
                },
            })
            .await;
        subscribe_doc(&mut relay, 1, SPACE, DOC).await;
        subscribe_doc(&mut relay, 2, SPACE, DOC).await;
        // The deleter listens to the space, as a daemon does at startup: the
        // revival register reaches it as a lifecycle broadcast, which only
        // listeners receive.
        relay
            .process_command(RelayCommand::SubscribeSignals {
                conn_id: 2,
                msg: sync::SubscribeSignals {
                    space_id: SPACE.into(),
                    cursor: None,
                },
            })
            .await;
        relay
            .process_command(RelayCommand::UnregisterDocument {
                conn_id: 2,
                msg: sync::UnregisterDocument {
                    space_id: SPACE.into(),
                    document_id: DOC.into(),
                    metadata: stamp(2_000),
                },
            })
            .await;
        assert!(
            relay
                .registries
                .get(SPACE)
                .and_then(|r| r.get(DOC))
                .is_none(),
            "premise: the delete landed first and soft-deleted the document"
        );
        let _ = ack_payloads(&mut editor_ack);
        while deleter_ctrl.try_recv().is_ok() {}

        // The editor's ops carry a content timestamp far above the delete
        // (the local document's wall clock), so the touch revives the doc.
        let (ops, metadata) = super::super::make_text_ops("hello");
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id: 1,
                msg: Box::new(sync::SyncOps {
                    space_id: SPACE.into(),
                    document_id: DOC.into(),
                    ops,
                    metadata,
                    ..Default::default()
                }),
            })
            .await;
        assert!(
            relay
                .registries
                .get(SPACE)
                .and_then(|r| r.get(DOC))
                .is_some(),
            "the edit revived the document"
        );
        assert!(
            ack_payloads(&mut editor_ack)
                .iter()
                .any(|p| matches!(p, Payload::RegisterDocument(r) if r.document_id == DOC)),
            "the editor is told to re-materialize the document it honored the delete of"
        );
        let mut deleter_told = false;
        while let Ok(bytes) = deleter_ctrl.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && let Some(Payload::RegisterDocument(r)) = env.payload
                && r.document_id == DOC
            {
                deleter_told = true;
            }
        }
        assert!(
            deleter_told,
            "the rest of the space is told to re-materialize"
        );
    }

    #[tokio::test]
    async fn test_dirty_flag_set_on_text_mutation() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);

        // Connect + handshake.
        let conn_id = 1;
        let (data_tx, _data_rx) = mpsc::channel(16);
        let (ctrl_tx, _ctrl_rx) = mpsc::channel(16);
        let (ack_tx, _ack_rx) = mpsc::unbounded_channel();
        relay
            .process_command(RelayCommand::Connect {
                conn_id,
                tx: data_tx,
                ctrl_tx,
                ack_tx,
            })
            .await;
        // Authentication is mandatory; attach the identity directly
        // (the in-process actor tests cannot run the real HTTP handshake).
        relay.test_set_authenticated(conn_id, TEST_CONN_DID);

        // Subscribe to a document.
        relay
            .process_command(RelayCommand::Subscribe {
                conn_id,
                msg: sync::Subscribe {
                    space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                    document_id: "doc".into(),
                },
            })
            .await;

        // Should not be dirty yet (no mutations).
        assert!(!relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "doc"));

        // Create valid CRDT ops using a local document.
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(
            agent,
            "test",
            "insert",
            kutl_core::Boundary::Explicit,
            |ctx| ctx.insert(0, "hello"),
        )
        .unwrap();
        let ops = doc.encode_since(&[]);
        let metadata = doc.changes_since(&[]);

        // Send ops to the relay.
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(sync::SyncOps {
                    space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                    document_id: "doc".into(),
                    ops,
                    metadata,
                    ..Default::default()
                }),
            })
            .await;

        // Document should be dirty after mutation.
        assert!(relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "doc"));
    }

    #[tokio::test]
    async fn test_dirty_flag_set_on_blob_mutation() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);

        // Connect + handshake.
        let conn_id = 1;
        let (data_tx, _data_rx) = mpsc::channel(16);
        let (ctrl_tx, _ctrl_rx) = mpsc::channel(16);
        let (ack_tx, _ack_rx) = mpsc::unbounded_channel();
        relay
            .process_command(RelayCommand::Connect {
                conn_id,
                tx: data_tx,
                ctrl_tx,
                ack_tx,
            })
            .await;
        // Authentication is mandatory; attach the identity directly
        // (the in-process actor tests cannot run the real HTTP handshake).
        relay.test_set_authenticated(conn_id, TEST_CONN_DID);

        // Subscribe.
        relay
            .process_command(RelayCommand::Subscribe {
                conn_id,
                msg: sync::Subscribe {
                    space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                    document_id: "blob-doc".into(),
                },
            })
            .await;

        assert!(!relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "blob-doc"));

        // Send blob ops (content_mode = BLOB_INLINE).
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(sync::SyncOps {
                    space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                    document_id: "blob-doc".into(),
                    ops: b"binary-content".to_vec(),
                    content_hash: b"hash123".to_vec(),
                    content_mode: i32::from(sync::ContentMode::Blob),
                    blob_size: 14,
                    metadata: vec![sync::ChangeMetadata {
                        timestamp: 1000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            })
            .await;

        // Document should be dirty after blob store.
        assert!(relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "blob-doc"));
    }

    // ---- blob LWW uses server-authoritative timestamp + hash tiebreak ----

    /// Send an inline blob through the relay with a client-supplied metadata
    /// timestamp (which the server must NOT trust as the LWW key).
    async fn send_blob(
        relay: &mut Relay,
        conn_id: ConnId,
        space: &str,
        doc: &str,
        content: &[u8],
        hash: &[u8],
        client_ts: i64,
    ) {
        #[allow(clippy::cast_possible_wrap)]
        let blob_size = content.len() as i64;
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(sync::SyncOps {
                    space_id: space.into(),
                    document_id: doc.into(),
                    ops: content.to_vec(),
                    content_hash: hash.to_vec(),
                    content_mode: i32::from(sync::ContentMode::Blob),
                    blob_size,
                    metadata: vec![sync::ChangeMetadata {
                        timestamp: client_ts,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            })
            .await;
    }

    #[tokio::test]
    async fn test_blob_lww_ignores_client_timestamp_poison() {
        // A peer sends a blob claiming `timestamp = i64::MAX`. If the relay
        // trusted that as the LWW key, every subsequent real write (carrying a
        // smaller `now_ms()`) would be dropped — permanently poisoning the
        // blob. The server must stamp `now_ms()` itself, so the later write
        // (different content, strictly later wall-clock) wins.
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "blob",
        )
        .await;

        // First write claims the maximum possible client timestamp.
        send_blob(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "blob",
            b"poison",
            b"hash-poison",
            i64::MAX,
        )
        .await;
        assert_eq!(
            resident_blob(&relay, "5171e0a1-1111-4000-8000-000000000001", "blob").as_deref(),
            Some(b"poison".as_slice())
        );

        // A genuine later write with an honest (smaller) client timestamp.
        send_blob(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "blob",
            b"real-update",
            b"hash-real",
            1000,
        )
        .await;

        // The later write must win — the poison timestamp is ignored.
        assert_eq!(
            resident_blob(&relay, "5171e0a1-1111-4000-8000-000000000001", "blob").as_deref(),
            Some(b"real-update".as_slice()),
            "server-authoritative LWW must let the later write win over a client i64::MAX poison"
        );
    }

    #[test]
    fn test_blob_lww_equal_timestamp_hash_tiebreak_converges() {
        // Two distinct blobs written in the same millisecond must converge on
        // a single deterministic winner regardless of arrival order: the one
        // with the lexicographically greater content hash. We drive
        // `set_blob` + the tiebreak logic directly because two real writes
        // would land in different milliseconds.
        let lo_hash = b"hash-aaa".to_vec();
        let hi_hash = b"hash-zzz".to_vec();
        let ts = 5000_i64;

        // Replica A: applies the lower-hash blob first, then the higher-hash.
        let mut slot_a = DocSlot::empty();
        slot_a.set_blob(BlobData {
            content: b"lo".to_vec(),
            hash: lo_hash.clone(),
            timestamp: ts,
        });
        // Higher hash on equal ts wins.
        if let DocContent::Blob(existing) = &slot_a.content {
            assert!(ts == existing.timestamp && hi_hash > existing.hash);
        }
        slot_a.set_blob(BlobData {
            content: b"hi".to_vec(),
            hash: hi_hash.clone(),
            timestamp: ts,
        });

        // Replica B: applies the higher-hash blob first; the lower-hash blob
        // arriving second on equal ts must be REJECTED (hash not strictly
        // greater), so B keeps the higher-hash winner.
        let mut slot_b = DocSlot::empty();
        slot_b.set_blob(BlobData {
            content: b"hi".to_vec(),
            hash: hi_hash.clone(),
            timestamp: ts,
        });
        let reject = if let DocContent::Blob(existing) = &slot_b.content {
            ts == existing.timestamp && lo_hash <= existing.hash
        } else {
            false
        };
        assert!(reject, "lower-hash blob on equal ts must be rejected");

        // Both replicas converge on the higher-hash content.
        let winner_a = match &slot_a.content {
            DocContent::Blob(b) => (b.content.clone(), b.hash.clone()),
            _ => panic!("expected blob"),
        };
        let winner_b = match &slot_b.content {
            DocContent::Blob(b) => (b.content.clone(), b.hash.clone()),
            _ => panic!("expected blob"),
        };
        assert_eq!(
            winner_a, winner_b,
            "replicas must converge on the same blob"
        );
        assert_eq!(winner_a.1, hi_hash);
    }
}
