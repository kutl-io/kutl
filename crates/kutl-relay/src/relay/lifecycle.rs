//! Registry-lifecycle handler family of the relay actor: document
//! register/rename/unregister (single and space-bulk) and space ownership
//! transfer, with their atomic persist-or-rollback steps, the lifecycle and
//! displacement broadcasts, the revival correction for a lost delete, and the
//! startup path-based `DocKey` migration — plus the free helpers (atomic-batch
//! displaced persist, idempotent re-register, reserved-conflict-path
//! rejection, causal-floor extraction, forwarded-rename sanitization).
//!
//! Child module of the relay actor (`super`) so the `impl Relay` block here
//! reaches the actor's private fields directly. `process_command` in
//! relay.rs routes lifecycle envelopes to these handlers, and the MCP
//! family reaches `register_document_internal` and `persist_entry`
//! through `super::`.

use std::collections::{HashMap, HashSet};

use kutl_proto::sync::{self, ErrorCode};
use tracing::{info, warn};

use crate::observer::{DocumentRegisteredEvent, DocumentRenamedEvent, DocumentUnregisteredEvent};
use crate::protocol::{
    encode_envelope, register_document_ack_envelope, rename_document_ack_envelope,
    transfer_space_ownership_ack_envelope, unregister_document_ack_envelope,
    unregister_space_ack_envelope,
};
use crate::registry;
use crate::registry_store::{MirrorMetadata, RegistryBackend, RegistryStoreError};

use super::{ConnId, DocKey, DocSlot, NO_SKIP_CONN, Relay};

impl Relay {
    // -----------------------------------------------------------------------
    // Document lifecycle handlers
    // -----------------------------------------------------------------------

    /// Register a document in the space registry, persist the entry,
    /// broadcast the registration to all WebSocket subscribers, and
    /// notify the observer. Called by both WebSocket and MCP paths.
    ///
    /// `skip_conn` is the WebSocket connection to exclude from the
    /// broadcast (the sender). Pass [`NO_SKIP_CONN`] when there is no
    /// WebSocket sender (e.g. MCP).
    #[expect(
        clippy::too_many_arguments,
        reason = "source provenance is already bundled in `provenance` and UX-only \
                  mirror metadata is bundled in `mirror`; further bundling \
                  (e.g. space_id/document_id/path into a tuple) would obscure call sites"
    )]
    pub(super) async fn register_document_internal(
        &mut self,
        space_id: &str,
        document_id: &str,
        path: &str,
        author_did: &str,
        account_id: Option<String>,
        provenance: registry::SourceProvenance,
        // UX-only metadata bundle mirrored to the kutlhub-relay's
        // `documents` table (`title`, `content_type`,
        // `converted_from_id`, `converted_from_filename`, `size_bytes`).
        // Not stored on `RegistryEntry`; threaded through `persist_entry`
        // to the mirror INSERT. Daemon-path callers pass
        // `&MirrorMetadata::default()`.
        mirror: &MirrorMetadata<'_>,
        // Creation time + origin HLC. The daemon/WS path passes the op's origin
        // stamp so register orders consistently with rename/delete (all
        // origin-stamped); relay-originated callers (MCP) pass `now_ms`/`None`.
        created_at_ms: i64,
        origin_hlc: Option<kutl_core::Hlc>,
        skip_conn: ConnId,
    ) -> Result<(), String> {
        reject_reserved_conflict_path("register", path)?;
        let registry::SourceProvenance {
            originally_created_at_ms,
            source_kind,
            source_id,
            source_url,
            ingestion_job_id,
            source_author_display,
        } = provenance;

        let meta = registry::EntryMetadata {
            author_did: author_did.to_owned(),
            timestamp: created_at_ms,
            hlc: origin_hlc,
            account_id,
            originally_created_at: originally_created_at_ms,
            source_kind,
            source_id: source_id.clone(),
            source_url: source_url.clone(),
            ingestion_job_id: ingestion_job_id.clone(),
            source_author_display: source_author_display.clone(),
        };

        let reg = self.registries.entry(space_id.to_owned()).or_default();
        let (is_reregister, token, displaced) =
            register_or_reregister(reg, document_id, path, meta)?;

        // Propagate persistence errors and roll
        // back the in-memory insert on failure so the registry never diverges
        // from the mirror (capture-restore — the lattice merge is not
        // invertible). On a re-register, there's no mutation to undo. Any
        // documents this register displaced to conflict paths are persisted off
        // the contested path first (see `persist_with_displaced`).
        if let Err(e) = persist_with_displaced(
            self.registry_backend.as_deref(),
            &self.registries,
            space_id,
            document_id,
            mirror,
            &displaced,
        ) {
            if !is_reregister && let Some(reg) = self.registries.get_mut(space_id) {
                reg.restore(token);
            }
            return Err(format!("persist register failed: {e}"));
        }

        // Materialize an in-memory DocSlot so an immediately-following
        // SyncOps from the same client lands. Without this, the
        // "register → send_ops" pattern (used by any client that writes
        // content without first subscribing) silently drops the ops in
        // `handle_sync_ops`, because that handler errors when
        // `self.documents` has no entry for the key. The slot represents
        // in-memory presence; the registry is the persistent source of
        // truth. `or_insert_with` keeps this idempotent if a prior
        // subscribe already populated the slot. Empty slots remain
        // eligible for eviction under the normal eviction rules.
        let slot_key = DocKey {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
        };
        self.documents
            .entry(slot_key)
            .or_insert_with(DocSlot::empty);

        // Broadcast the POST-arbitration effective path, not the requested one.
        // When this register lost a path it contended for, the operated document
        // is displaced to its conflict path; replicas must materialize it there,
        // not at the path the registrant asked for. Without a collision these are
        // identical, so steady-state behaviour is unchanged.
        let effective_path = self
            .registries
            .get(space_id)
            .and_then(|reg| reg.get_any(document_id))
            .map_or_else(|| path.to_owned(), |entry| entry.path.clone());

        let proto_msg = sync::RegisterDocument {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            path: effective_path,
            metadata: Some(sync::ChangeMetadata {
                author_did: author_did.to_owned(),
                timestamp: kutl_core::env::now_ms(),
                // Forward the origin HLC so the register arrives HLC-bearing like
                // rename/unregister/revival do. The daemon folds it into the
                // per-doc lifecycle watermark (so a later stale rename/delete is
                // dropped) but does NOT gate the register on it — see
                // `handle_remote_register`.
                hlc: origin_hlc.map(kutl_proto::sync::Hlc::from),
                ..Default::default()
            }),
            originally_created_at_ms,
            source_kind,
            source_id,
            source_url,
            ingestion_job_id,
            source_author_display,
            // Forward UX-only metadata
            // on the broadcast. Other subscribers (e.g. the UX server
            // mirroring its own writes via observers) see the same
            // values that landed in the documents mirror.
            title: mirror.title.map(str::to_owned),
            content_type: mirror.content_type.map(str::to_owned),
            // Convert-path
            // metadata is forwarded on the broadcast too, so the UX
            // server's registry-handler can pick up convert provenance
            // from the broadcast it doesn't otherwise need to issue.
            converted_from_id: mirror.converted_from_id.map(str::to_owned),
            converted_from_filename: mirror.converted_from_filename.map(str::to_owned),
            size_bytes: mirror.size_bytes,
        };
        self.broadcast_lifecycle_to_space(
            space_id,
            skip_conn,
            sync::sync_envelope::Payload::RegisterDocument(proto_msg),
        )
        .await;

        // Broadcast any documents this register displaced off the contested path
        // to their conflict paths (to everyone). Inert when `displaced` is empty
        // (the common, no-collision case). The registrant's OWN document, if it
        // lost the path, is corrected via the `RegisterDocumentAck` (whose
        // `entry.path` is the effective path), not here.
        self.broadcast_displaced(space_id, &displaced).await;

        self.observer
            .on_document_registered(DocumentRegisteredEvent {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                author_did: author_did.to_owned(),
                path: path.to_owned(),
                timestamp: kutl_core::env::now_ms(),
            });

        Ok(())
    }

    /// Auto-register the space on its first appearance so the in-memory registry
    /// can't diverge from `space_backend` (persistent mode only). Ensures the row
    /// BEFORE `register_document_internal` creates the registry, so a persist
    /// failure aborts the register with nothing created. The implicit sync path
    /// carries no human name, so the name defaults to the `space_id`; `ensure` is
    /// idempotent and never overwrites a name set via explicit registration.
    ///
    /// Returns `true` if the register should abort (the failure ack has already
    /// been sent), `false` to proceed.
    ///
    /// Only the WS register path runs this auto-registration.
    async fn ensure_space_or_abort(
        &mut self,
        conn_id: ConnId,
        msg: &sync::RegisterDocument,
    ) -> bool {
        // Confine the `space_backend` borrow to the RHS so the later `&mut self`
        // ack calls don't conflict.
        let ensure_err = if self.registries.contains_key(&msg.space_id) {
            None
        } else if let Some(backend) = self.space_backend.as_ref() {
            backend.ensure(&msg.space_id, &msg.space_id).await.err()
        } else {
            None
        };
        let Some(e) = ensure_err else {
            return false;
        };
        warn!(error = %e, space_id = %msg.space_id, "auto-register space failed");
        self.send_error(
            conn_id,
            ErrorCode::InvalidMessage,
            &format!("register failed: {e}"),
        );
        let ack = register_document_ack_envelope(
            &msg.space_id,
            &msg.document_id,
            false,
            None,
            Some(lifecycle_ack_error(
                ErrorCode::InvalidMessage,
                format!("register failed: {e}"),
            )),
            None,
        );
        self.send_ack(conn_id, &encode_envelope(&ack));
        true
    }

    pub(super) async fn handle_register_document(
        &mut self,
        conn_id: ConnId,
        msg: &sync::RegisterDocument,
    ) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                // Auth failures pre-date the persist step — emit the
                // failure ack on the same channel as the error frame so
                // senders can correlate the result.
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                let ack = register_document_ack_envelope(
                    &msg.space_id,
                    &msg.document_id,
                    false,
                    None,
                    Some(lifecycle_ack_error(ErrorCode::AuthFailed, e.to_string())),
                    None,
                );
                self.send_ack(conn_id, &encode_envelope(&ack));
                return;
            }
        };

        if self.ensure_space_or_abort(conn_id, msg).await {
            return;
        }

        let mut meta = extract_meta(msg.metadata.as_ref());
        // Overwrite the client-supplied `author_did` with the connection's
        // authenticated identity so `registry.created_by` and the mirrored
        // `documents.created_by` column attribute the registration to the
        // real caller rather than any spoofed DID in the envelope.
        meta.author_did = self.authoritative_author_did(conn_id).to_owned();

        // Resolve the connection's account_id (DB-backed mode).
        if let Some(ref membership) = self.membership_backend {
            meta.account_id = self.resolve_account_id(conn_id, membership.as_ref()).await;
        }

        let provenance = registry::SourceProvenance {
            originally_created_at_ms: msg.originally_created_at_ms,
            source_kind: msg.source_kind,
            source_id: msg.source_id.clone(),
            source_url: msg.source_url.clone(),
            ingestion_job_id: msg.ingestion_job_id.clone(),
            source_author_display: msg.source_author_display.clone(),
        };

        let mirror = MirrorMetadata {
            title: msg.title.as_deref(),
            content_type: msg.content_type.as_deref(),
            converted_from_id: msg.converted_from_id.as_deref(),
            converted_from_filename: msg.converted_from_filename.as_deref(),
            size_bytes: msg.size_bytes,
        };
        let result = self
            .register_document_internal(
                &msg.space_id,
                &msg.document_id,
                &msg.path,
                &meta.author_did,
                meta.account_id,
                provenance,
                &mirror,
                meta.timestamp,
                meta.hlc,
                conn_id,
            )
            .await;

        // Emit the ack AFTER persistence
        // commits. `register_document_internal` returning Ok means both
        // the in-memory registry insert AND the `save_entry` mirror
        // INSERT have committed (the SQLite backend's `block_on` and
        // the PG backend's transactional `block_on` both return
        // synchronously after commit).
        let ack = match result {
            Ok(()) => {
                info!(conn_id, doc_id = %msg.document_id, "document registered");
                let (entry, hlc) = self.ack_entry_and_hlc(&msg.space_id, &msg.document_id);
                register_document_ack_envelope(
                    &msg.space_id,
                    &msg.document_id,
                    true,
                    entry,
                    None,
                    hlc,
                )
            }
            Err(e) => {
                warn!(error = %e, doc_id = %msg.document_id, "register document failed");
                // Preserve the existing legacy error frame for callers
                // that haven't migrated to the ack envelope yet.
                self.send_error(
                    conn_id,
                    ErrorCode::InvalidMessage,
                    &format!("register failed: {e}"),
                );
                register_document_ack_envelope(
                    &msg.space_id,
                    &msg.document_id,
                    false,
                    None,
                    Some(lifecycle_ack_error(
                        ErrorCode::InvalidMessage,
                        format!("register failed: {e}"),
                    )),
                    None,
                )
            }
        };
        self.send_ack(conn_id, &encode_envelope(&ack));
    }

    /// The persisted entry proto + liveness HLC of a document, from ONE registry
    /// lookup (a consistent post-persist snapshot); both `None` if the space or
    /// document is absent. Used to build a success register/rename ack — the HLC
    /// lets the operand's daemon freshness-gate the ack's self-correction.
    fn ack_entry_and_hlc(
        &self,
        space_id: &str,
        document_id: &str,
    ) -> (Option<sync::RegistryEntry>, Option<sync::Hlc>) {
        self.registries.get(space_id).map_or((None, None), |reg| {
            (
                reg.get(document_id).map(registry_entry_to_proto),
                reg.liveness_hlc(document_id)
                    .map(kutl_proto::sync::Hlc::from),
            )
        })
    }

    /// Send a failure `RenameDocumentAck` (no persisted entry) carrying `code` +
    /// `reason` to `conn_id`.
    fn send_rename_failure(
        &self,
        conn_id: ConnId,
        space_id: &str,
        document_id: &str,
        code: ErrorCode,
        reason: String,
    ) {
        let ack = rename_document_ack_envelope(
            space_id,
            document_id,
            false,
            None,
            Some(lifecycle_ack_error(code, reason)),
            None,
        );
        self.send_ack(conn_id, &encode_envelope(&ack));
    }

    pub(super) async fn handle_rename_document(
        &mut self,
        conn_id: ConnId,
        msg: &sync::RenameDocument,
    ) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                let ack = rename_document_ack_envelope(
                    &msg.space_id,
                    &msg.document_id,
                    false,
                    None,
                    Some(lifecycle_ack_error(ErrorCode::AuthFailed, e.to_string())),
                    None,
                );
                self.send_ack(conn_id, &encode_envelope(&ack));
                return;
            }
        };

        if let Err(reason) = reject_reserved_conflict_path("rename", &msg.new_path) {
            self.send_rename_failure(
                conn_id,
                &msg.space_id,
                &msg.document_id,
                ErrorCode::InvalidMessage,
                reason,
            );
            return;
        }

        let authoritative = self.authoritative_author_did(conn_id).to_owned();
        let mut meta = extract_meta(msg.metadata.as_ref());
        // Pin `renamed_by` to the authenticated identity so the registry
        // entry reflects the real caller.
        meta.author_did.clone_from(&authoritative);

        let reg = self.registries.entry(msg.space_id.clone()).or_default();

        // Causal floor the rename envelope carries (the register HLC the renamer
        // observed), so it supersedes a clock-skewed registration.
        let rename_causal_floor = extract_rename_causal_floor(msg);

        // Apply the rename through the lattice — total: concurrent renames of
        // the same document resolve by HLC (last writer wins), and a rename onto
        // a path held by a different document conflict-copies the loser. The
        // returned token rolls back on a persist failure
        // (capture-restore, since the merge is not invertible).
        let (token, displaced) = reg.rename(
            &msg.document_id,
            &msg.old_path,
            &msg.new_path,
            meta,
            rename_causal_floor,
        );

        // The authoritative current path after resolution — the winner of any
        // concurrent rename. Broadcast THIS (not the sender's requested path) so
        // a superseded rename still drives every replica to the winning path;
        // replicas reconcile their local copy by document id.
        let authoritative_path = reg
            .get_any(&msg.document_id)
            .map_or_else(|| msg.new_path.clone(), |e| e.path.clone());

        // Rename doesn't carry UX-only mirror metadata — those land via the
        // original register. COALESCE-on-conflict in the mirror keeps the
        // previously-stored values. A rename onto a path held by a different
        // document displaces that loser to its conflict path; persist the loser
        // off the contested path first (see `persist_with_displaced`).
        if let Err(e) = persist_with_displaced(
            self.registry_backend.as_deref(),
            &self.registries,
            &msg.space_id,
            &msg.document_id,
            &MirrorMetadata::default(),
            &displaced,
        ) {
            warn!(error = %e, doc_id = %msg.document_id, "rename persist failed");
            // Roll back the in-memory rename so the registry matches the
            // (unchanged) mirror state.
            if let Some(reg) = self.registries.get_mut(&msg.space_id) {
                reg.restore(token);
            }
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                &format!("rename persist failed: {e}"),
            );
            let ack = rename_document_ack_envelope(
                &msg.space_id,
                &msg.document_id,
                false,
                None,
                Some(lifecycle_ack_error(
                    ErrorCode::InvalidMessage,
                    format!("rename persist failed: {e}"),
                )),
                None,
            );
            self.send_ack(conn_id, &encode_envelope(&ack));
            return;
        }

        // Sanitize the payload forwarded to subscribers + the observer (pin the
        // authoritative DID, drive replicas to the LWW-winning path).
        let (forwarded, observed_author_did) =
            sanitize_forwarded_rename(msg, &authoritative_path, &authoritative);

        self.broadcast_lifecycle_to_space(
            &msg.space_id,
            conn_id,
            sync::sync_envelope::Payload::RenameDocument(forwarded),
        )
        .await;

        // Broadcast any documents this rename displaced off the contested path
        // to their conflict paths (to everyone). The renamer's OWN document, if
        // it lost the path, is corrected via the ack below — not here.
        self.broadcast_displaced(&msg.space_id, &displaced).await;

        self.observer.on_document_renamed(DocumentRenamedEvent {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
            author_did: observed_author_did,
            old_path: msg.old_path.clone(),
            new_path: msg.new_path.clone(),
            timestamp: kutl_core::env::now_ms(),
        });

        // Emit the ack AFTER persistence commits
        // across both the registry and the `documents` mirror. `entry.path` is
        // the post-arbitration effective path; when the rename lost a collision
        // it is the conflict path, and the renamer reconciles its local file to
        // it (the typed
        // ack already carries the operand's authoritative result to its sender).
        let (entry, hlc) = self.ack_entry_and_hlc(&msg.space_id, &msg.document_id);
        let ack =
            rename_document_ack_envelope(&msg.space_id, &msg.document_id, true, entry, None, hlc);
        self.send_ack(conn_id, &encode_envelope(&ack));
    }

    pub(super) async fn handle_unregister_document(
        &mut self,
        conn_id: ConnId,
        msg: &sync::UnregisterDocument,
    ) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                let ack = unregister_document_ack_envelope(
                    &msg.space_id,
                    &msg.document_id,
                    false,
                    Some(lifecycle_ack_error(ErrorCode::AuthFailed, e.to_string())),
                );
                self.send_ack(conn_id, &encode_envelope(&ack));
                return;
            }
        };

        let authoritative = self.authoritative_author_did(conn_id).to_owned();
        let mut meta = extract_meta(msg.metadata.as_ref());
        meta.author_did.clone_from(&authoritative);

        let reg = self.registries.entry(msg.space_id.clone()).or_default();

        // Soft-delete through the lattice — total: re-deleting is an idempotent
        // merge. The token rolls back on a persist failure.
        let token = reg.unregister(&msg.document_id, &meta);

        // Persist the soft-delete. On failure, roll the in-memory
        // soft-delete back so the registry doesn't diverge from the
        // mirror (capture-restore).
        let persist_result = match self.registry_backend.as_deref() {
            Some(backend) => {
                let deleted_at = self
                    .registries
                    .get(&msg.space_id)
                    .and_then(|r| r.get_any(&msg.document_id))
                    .and_then(|e| e.deleted_at);
                match deleted_at {
                    Some(ts) => backend.delete_entry(&msg.space_id, &msg.document_id, ts),
                    None => Ok(()),
                }
            }
            None => Ok(()),
        };
        if let Err(e) = persist_result {
            warn!(error = %e, doc_id = %msg.document_id, "unregister persist failed");
            if let Some(reg) = self.registries.get_mut(&msg.space_id) {
                reg.restore(token);
            }
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                &format!("unregister persist failed: {e}"),
            );
            let ack = unregister_document_ack_envelope(
                &msg.space_id,
                &msg.document_id,
                false,
                Some(lifecycle_ack_error(
                    ErrorCode::InvalidMessage,
                    format!("unregister persist failed: {e}"),
                )),
            );
            self.send_ack(conn_id, &encode_envelope(&ack));
            return;
        }

        // The delete may have LOST to a concurrent edit (edit-revives): a
        // content edit raised the doc's `touched_hlc` above this deletion's, so
        // the lattice keeps it alive. `get` returns `Some` only for an
        // active (non-deleted) doc, so its presence is the authoritative
        // "delete lost" signal.
        let revived = self
            .registries
            .get(&msg.space_id)
            .and_then(|r| r.get(&msg.document_id))
            .is_some();

        if revived {
            // Don't propagate a delete for a doc that is still alive — that would
            // wrongly destroy it on every other client. Instead correct the
            // sender (whose offline delete lost): re-assert the live document so
            // its daemon re-subscribes and re-materializes the surviving content.
            self.send_revival_register(conn_id, &msg.space_id, &msg.document_id);
            let ack = unregister_document_ack_envelope(&msg.space_id, &msg.document_id, true, None);
            self.send_ack(conn_id, &encode_envelope(&ack));
            return;
        }

        // Deletion cascade: the soft-delete committed
        // and did NOT lose to a concurrent edit, so tombstone every currently-
        // visible signal attached to this document.
        self.cascade_delete_signals_logged(&msg.space_id, &msg.document_id, &authoritative)
            .await;

        // Sanitize the broadcast payload so subscribers never see a spoofed
        // `author_did` on the lifecycle notification.
        let mut forwarded = msg.clone();
        {
            let mut new_meta = forwarded.metadata.unwrap_or_default();
            new_meta.author_did.clone_from(&authoritative);
            forwarded.metadata = Some(new_meta);
        }
        let observed_author_did = forwarded
            .metadata
            .as_ref()
            .map_or_else(String::new, |m| m.author_did.clone());

        self.broadcast_lifecycle_to_space(
            &msg.space_id,
            conn_id,
            sync::sync_envelope::Payload::UnregisterDocument(forwarded),
        )
        .await;

        self.observer
            .on_document_unregistered(DocumentUnregisteredEvent {
                space_id: msg.space_id.clone(),
                document_id: msg.document_id.clone(),
                author_did: observed_author_did,
                timestamp: kutl_core::env::now_ms(),
            });

        // Emit the ack AFTER persistence
        // commits across both the in-memory registry soft-delete AND
        // the `documents` mirror's `deleted_at` update (the latter is
        // synchronous inside `delete_entry`'s `block_on`).
        let ack = unregister_document_ack_envelope(&msg.space_id, &msg.document_id, true, None);
        self.send_ack(conn_id, &encode_envelope(&ack));
    }

    /// Correct a sender whose unregister lost to a concurrent edit (revival):
    /// send it a `RegisterDocument` for the doc's current active path, stamped
    /// with the doc's liveness HLC so the daemon's freshness gate accepts it and
    /// re-subscribes. Targeted at the sender only — other clients still hold the
    /// live doc and must not be disturbed. No-op if the doc isn't active (a
    /// genuine delete races this path).
    fn send_revival_register(&self, conn_id: ConnId, space_id: &str, document_id: &str) {
        let Some(reg) = self.registries.get(space_id) else {
            return;
        };
        let Some(entry) = reg.get(document_id) else {
            return;
        };
        let hlc = reg
            .liveness_hlc(document_id)
            .map(kutl_proto::sync::Hlc::from);
        let payload = sync::RegisterDocument {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            path: entry.path.clone(),
            metadata: Some(sync::ChangeMetadata {
                // The relay re-asserts liveness; there is no single authoring DID
                // for a revival (the surviving content's authors are on the CRDT
                // ops the daemon re-subscribes to).
                author_did: String::new(),
                timestamp: kutl_core::env::now_ms(),
                hlc,
                ..Default::default()
            }),
            ..Default::default()
        };
        let envelope = sync::SyncEnvelope {
            payload: Some(sync::sync_envelope::Payload::RegisterDocument(payload)),
        };
        self.send_ack(conn_id, &encode_envelope(&envelope));
    }

    /// Evict a space's cached signal-segment writer (if any), releasing its
    /// two open fds (the active segment + the flock'd LOCK) and the flock.
    ///
    /// Called on space unregister so a long-lived relay does not hold a
    /// `SegmentWriter` FOREVER for every space it ever appended to (an fd
    /// leak, and a latent stale-fd hazard if the space's data dir is later
    /// removed). The segments persist on disk; a later append re-creates the
    /// writer with no data loss. A no-op on a storeless relay (no
    /// record log) or a non-UUID space id (real spaces are UUIDs).
    async fn evict_signal_writer(&mut self, space_id: &str) {
        if let Ok(space_uuid) = uuid::Uuid::parse_str(space_id) {
            self.record_log.evict(space_uuid).await;
        }
    }

    /// Bulk unregister every active document in a space. Routes per-doc
    /// through the same `unregister` + `delete_entry` code path
    /// `handle_unregister_document` uses, then sends a single
    /// `UnregisterSpaceAck` carrying the IDs.
    ///
    /// Atomicity is per-doc, not bulk: a backend failure mid-pass
    /// fails the ack with the list of docs successfully soft-deleted
    /// before the failure. The caller should treat partial success
    /// as a real possibility and reconcile.
    pub(super) async fn handle_unregister_space(
        &mut self,
        conn_id: ConnId,
        msg: &sync::UnregisterSpace,
    ) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                let ack = unregister_space_ack_envelope(
                    &msg.space_id,
                    Vec::new(),
                    false,
                    Some(lifecycle_ack_error(ErrorCode::AuthFailed, e.to_string())),
                );
                self.send_ack(conn_id, &encode_envelope(&ack));
                return;
            }
        };

        let authoritative = self.authoritative_author_did(conn_id).to_owned();
        let mut meta = extract_meta(msg.metadata.as_ref());
        meta.author_did.clone_from(&authoritative);

        // Snapshot the active document IDs so we don't iterate while
        // mutating `self.registries`.
        let active_ids: Vec<String> = self
            .registries
            .get(&msg.space_id)
            .map(|reg| reg.active_entries().map(|(id, _)| id.to_owned()).collect())
            .unwrap_or_default();

        if active_ids.is_empty() {
            // No documents to delete, but the space may still hold SPACE-LEVEL
            // signals (document_id = None) — cascade-tombstone them, then release
            // the cached writer's fds + flock (the cascade needs the writer, so
            // eviction is last). Both are no-ops without a durable signal store.
            self.cascade_space_delete_signals_logged(&msg.space_id, &authoritative)
                .await;
            self.evict_signal_writer(&msg.space_id).await;
            let ack = unregister_space_ack_envelope(&msg.space_id, Vec::new(), true, None);
            self.send_ack(conn_id, &encode_envelope(&ack));
            return;
        }

        // Delete the whole batch in memory in ONE arbitrate + reproject pass
        // (O(documents), not the O(documents²) of a per-doc `unregister` loop),
        // capturing a single combined rollback token. Then gather each
        // tombstone's `(id, deleted_at)` for the atomic persist.
        let (token, deletes) = {
            let reg = self.registries.entry(msg.space_id.clone()).or_default();
            let token = reg.unregister_many(&active_ids, &meta);
            let deletes: Vec<(String, i64)> = active_ids
                .iter()
                .filter_map(|id| {
                    reg.get_any(id)
                        .and_then(|e| e.deleted_at)
                        .map(|ts| (id.clone(), ts))
                })
                .collect();
            (token, deletes)
        };

        // Persist the whole batch as ONE atomic unit. On failure, roll the entire
        // in-memory batch back so the registry never diverges from the mirror, and
        // abort. No lifecycle is broadcast in the failure case — broadcasts run
        // only after a successful persist below, so an aborted delete is fully
        // invisible to peers (a per-doc loop could leak
        // a committed prefix of broadcasts before failing).
        let persist_result = match self.registry_backend.as_deref() {
            Some(backend) => {
                let pairs: Vec<(&str, i64)> =
                    deletes.iter().map(|(id, ts)| (id.as_str(), *ts)).collect();
                backend.delete_batch(&msg.space_id, &pairs)
            }
            None => Ok(()),
        };
        if let Err(e) = persist_result {
            warn!(
                error = %e,
                space_id = %msg.space_id,
                "bulk unregister: persist failed — rolling back the whole batch and aborting"
            );
            if let Some(reg) = self.registries.get_mut(&msg.space_id) {
                reg.restore(token);
            }
            let ack = unregister_space_ack_envelope(
                &msg.space_id,
                Vec::new(),
                false,
                Some(lifecycle_ack_error(
                    ErrorCode::InvalidMessage,
                    format!("bulk unregister failed: {e}"),
                )),
            );
            self.send_ack(conn_id, &encode_envelope(&ack));
            return;
        }

        // Deletion cascade: the bulk soft-delete committed,
        // so tombstone every currently-visible signal in the space. Every
        // document is being deleted, so the whole space's visible signal set is
        // the correct target; `handle_unregister_space` otherwise never touches
        // the `signals` projection, leaving them Open on a durable relay. Logged-
        // and-swallowed: the document deletes are already durable, and the signal
        // projection heals on the next fold.
        self.cascade_space_delete_signals_logged(&msg.space_id, &authoritative)
            .await;

        // Release the space's cached signal writer (its fds + flock) AFTER the
        // cascade above has finished appending its tombstones — the cascade needs
        // the writer, and eviction is the last thing to touch the store. A later
        // append re-creates the writer with no data loss (segments persist).
        self.evict_signal_writer(&msg.space_id).await;

        // Broadcast per-doc lifecycle so feed observers see each delete attributed
        // correctly, and notify the after-merge observer.
        self.broadcast_bulk_unregister(&msg.space_id, conn_id, &active_ids, &meta.author_did)
            .await;

        let ack = unregister_space_ack_envelope(&msg.space_id, active_ids, true, None);
        self.send_ack(conn_id, &encode_envelope(&ack));
    }

    /// Fan out one `UnregisterDocument` lifecycle broadcast + after-merge
    /// observer event per document soft-deleted by a bulk space unregister, all
    /// stamped with one shared wall-clock `timestamp` and attributed to
    /// `author_did`. Split out of [`Self::handle_unregister_space`] to keep that
    /// handler under the function-length lint.
    async fn broadcast_bulk_unregister(
        &mut self,
        space_id: &str,
        conn_id: ConnId,
        document_ids: &[String],
        author_did: &str,
    ) {
        let timestamp = kutl_core::env::now_ms();
        for document_id in document_ids {
            let forwarded = sync::UnregisterDocument {
                space_id: space_id.to_owned(),
                document_id: document_id.clone(),
                metadata: Some(sync::ChangeMetadata {
                    author_did: author_did.to_owned(),
                    timestamp,
                    ..Default::default()
                }),
            };
            self.broadcast_lifecycle_to_space(
                space_id,
                conn_id,
                sync::sync_envelope::Payload::UnregisterDocument(forwarded),
            )
            .await;
            self.observer
                .on_document_unregistered(DocumentUnregisteredEvent {
                    space_id: space_id.to_owned(),
                    document_id: document_id.clone(),
                    author_did: author_did.to_owned(),
                    timestamp,
                });
        }
    }

    /// Transfer ownership of every document in a space. Used by the
    /// ingestion accept-time re-attribution path.
    ///
    /// Updates `RegistryEntry.account_id` in memory AND in the mirror
    /// (`relay_registry.account_id` + `documents_table.created_by` /
    /// `updated_by`). The mirror update is the
    /// [`RegistryBackend::transfer_space_ownership`] method — a
    /// dedicated trait method, not piggybacked on `save_entry`,
    /// because `save_entry`'s `ON CONFLICT` clause deliberately does
    /// NOT update `account_id` (creator-is-immutable for normal
    /// re-registers). This envelope is the explicit "bulk transfer"
    /// path that bypasses the immutability guard.
    ///
    /// DESTRUCTIVE — rewrites `created_by` / `updated_by` on
    /// soft-deleted rows too. The registry UPDATE in
    /// `PostgresRegistryBackend::transfer_space_ownership` is
    /// unfiltered by `deleted_at`, the mirror UPDATE likewise, and
    /// the in-memory `DocumentRegistry::transfer_ownership` iterates
    /// every entry including tombstones. For the only current caller
    /// (ingestion accept of a freshly-imported staging space) this is
    /// unobservable, but a future caller against a long-lived space
    /// would rewrite history. See the same caveat on the proto
    /// message — only safe for fresh-imported staging spaces.
    ///
    /// Also does NOT broadcast a lifecycle event per document — see
    /// the proto-message docstring for the rationale (ingestion accept
    /// tears the staging space down immediately after transfer).
    pub(super) async fn handle_transfer_space_ownership(
        &mut self,
        conn_id: ConnId,
        msg: &sync::TransferSpaceOwnership,
    ) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                let ack = transfer_space_ownership_ack_envelope(
                    &msg.space_id,
                    0,
                    false,
                    Some(lifecycle_ack_error(ErrorCode::AuthFailed, e.to_string())),
                );
                self.send_ack(conn_id, &encode_envelope(&ack));
                return;
            }
        };

        // Mirror UPDATE first. If it fails the registry stays
        // consistent with the (unchanged) mirror.
        let backend_count = match self.registry_backend.as_deref() {
            Some(backend) => {
                match backend.transfer_space_ownership(&msg.space_id, &msg.new_account_id) {
                    Ok(n) => n,
                    Err(e) => {
                        warn!(
                            error = %e,
                            space_id = %msg.space_id,
                            "transfer-ownership: backend update failed"
                        );
                        let ack = transfer_space_ownership_ack_envelope(
                            &msg.space_id,
                            0,
                            false,
                            Some(lifecycle_ack_error(
                                ErrorCode::InvalidMessage,
                                format!("transfer ownership persist failed: {e}"),
                            )),
                        );
                        self.send_ack(conn_id, &encode_envelope(&ack));
                        return;
                    }
                }
            }
            None => 0,
        };

        // In-memory mutation only after backend committed.
        if let Some(reg) = self.registries.get_mut(&msg.space_id) {
            reg.transfer_ownership(&msg.new_account_id);
        }

        let ack = transfer_space_ownership_ack_envelope(&msg.space_id, backend_count, true, None);
        self.send_ack(conn_id, &encode_envelope(&ack));
    }

    /// Broadcast a lifecycle message to all subscribers in a space (except
    /// sender). Recipients are collected first, then fanned out — so the
    /// per-recipient `send_broadcast` (which may evict on a full lane) doesn't
    /// run while `space_watchers` / `documents` are borrowed.
    async fn broadcast_lifecycle_to_space(
        &mut self,
        space_id: &str,
        sender: ConnId,
        payload: sync::sync_envelope::Payload,
    ) {
        let envelope = sync::SyncEnvelope {
            payload: Some(payload),
        };
        let bytes = encode_envelope(&envelope);

        let mut seen = HashSet::new();
        let mut recipients: Vec<ConnId> = Vec::new();

        // Include space-level watchers (connections that called ListSpaceDocuments).
        if let Some(watchers) = self.space_watchers.get(space_id) {
            for &watcher_id in watchers {
                if watcher_id != sender && seen.insert(watcher_id) {
                    recipients.push(watcher_id);
                }
            }
        }

        // Include per-document subscribers.
        for (key, slot) in &self.documents {
            if key.space_id != space_id {
                continue;
            }
            for (sub_conn_id, _) in slot.subscribers.active_entries() {
                if sub_conn_id != sender && seen.insert(sub_conn_id) {
                    recipients.push(sub_conn_id);
                }
            }
        }

        for conn_id in recipients {
            self.send_broadcast(conn_id, &bytes).await;
        }
    }

    /// Broadcast each document this register/rename displaced off the contested
    /// path to its conflict path, to *everyone* (`NO_SKIP_CONN`) — even the
    /// operation's sender may hold a now-stale view of a loser it learned about
    /// earlier. Inert when `displaced` is empty (the common, no-collision case).
    ///
    /// The operated document itself, when IT lost the path, is NOT corrected
    /// here — its sender learns the post-arbitration effective path from the
    /// `RegisterDocumentAck` / `RenameDocumentAck` (`entry.path`), the typed-ack
    /// rail that already carries an operand's authoritative result to its sender.
    ///
    /// Each loser's move is stamped with a logical-`u32::MAX` liveness touch
    /// ([`kutl_core::Hlc::physical_touch`]): a displacement carries no fresh
    /// rename HLC of its own (the loser's `renamed_hlc` is unchanged), so a naive
    /// re-broadcast would tie the receiver's already-applied stamp and be dropped
    /// by the daemon's `lifecycle_event_is_fresh` gate. The same-millisecond
    /// logical ceiling clears that tie while a genuinely later rename still wins.
    async fn broadcast_displaced(&mut self, space_id: &str, displaced: &[String]) {
        for loser in displaced {
            if let Some(payload) = self.displacement_payload(space_id, loser) {
                self.broadcast_lifecycle_to_space(space_id, NO_SKIP_CONN, payload)
                    .await;
            }
        }
    }

    /// A `RenameDocument` payload that moves `document_id` to its current
    /// effective (post-arbitration) path, stamped to clear the daemon's
    /// freshness gate. `None` if the document is not active in the space.
    ///
    /// `old_path` is set to the effective path too — it is advisory (the daemon
    /// reconciles by document id) and must still be a valid relative path so the
    /// receiver's `SafeRelayPath` check accepts the message.
    fn displacement_payload(
        &self,
        space_id: &str,
        document_id: &str,
    ) -> Option<sync::sync_envelope::Payload> {
        let reg = self.registries.get(space_id)?;
        let entry = reg.get(document_id)?;
        let hlc = reg
            .liveness_hlc(document_id)
            .map(|h| kutl_core::Hlc::physical_touch(h.physical_ms))
            .map(kutl_proto::sync::Hlc::from);
        Some(sync::sync_envelope::Payload::RenameDocument(
            sync::RenameDocument {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                old_path: entry.path.clone(),
                new_path: entry.path.clone(),
                metadata: Some(sync::ChangeMetadata {
                    author_did: String::new(),
                    timestamp: kutl_core::env::now_ms(),
                    hlc,
                    ..Default::default()
                }),
                // A displacement re-assert (path == path); the relay already
                // resolved arbitration, so it carries no causal floor.
                rename_causal_floor: None,
            },
        ))
    }

    /// Migrate path-based `DocKey`s to UUID-based using the registry.
    ///
    /// Scans all document keys for entries that look like file paths
    /// (containing `/` or `.`), looks up the matching registry entry by path,
    /// and re-keys the document slot from the old path-based key to the new
    /// UUID-based key. Called once at startup and can be triggered after
    /// bulk registration.
    pub fn migrate_path_based_keys(&mut self) {
        let mut total_migrated = 0u32;

        for (space_id, reg) in &self.registries {
            let mut migrations: Vec<(DocKey, String)> = Vec::new();

            for key in self.documents.keys() {
                if key.space_id != *space_id {
                    continue;
                }
                // Heuristic: path-based IDs contain '/' or '.' — UUIDs contain
                // only hex digits and hyphens (no '/' or '.').
                if (key.document_id.contains('/') || key.document_id.contains('.'))
                    && let Some(entry) = reg.get_by_path(&key.document_id)
                {
                    migrations.push((key.clone(), entry.document_id.clone()));
                }
            }

            for (old_key, new_id) in migrations {
                if let Some(slot) = self.documents.remove(&old_key) {
                    let new_key = DocKey {
                        space_id: old_key.space_id,
                        document_id: new_id.clone(),
                    };
                    info!(
                        old_id = %old_key.document_id,
                        new_id = %new_id,
                        "migrated path-based `DocKey` to UUID"
                    );
                    self.documents.insert(new_key, slot);
                    total_migrated += 1;
                }
            }
        }

        if total_migrated > 0 {
            info!(
                count = total_migrated,
                "path-to-UUID DocKey migration complete"
            );
        }
    }
}

/// Free function to avoid borrow conflicts between `self.registries` and
/// `self.registry_backend`.
///
/// `mirror` carries mirror-only metadata
/// (`title`, `content_type`, convert provenance, `size_bytes`)
/// threaded through to backends that maintain a UX-facing `documents`
/// mirror alongside the registry. The registry itself does not store
/// any of these. Callers that aren't writing UX-supplied metadata
/// (rename, edit-time rewrite) pass `&MirrorMetadata::default()`.
///
/// Returns:
/// - `Ok(())` when no backend is configured, when the registry has no
///   active entry for `document_id` (the no-op cases that mirror the
///   prior side-effect-free returns), or when `save_entry` succeeded.
/// - `Err(RegistryStoreError)` when the backend is configured and
///   `save_entry` failed. Lifecycle
///   handlers MUST propagate this error so the caller's ack carries a
///   failure and the in-memory registry mutation is rolled back —
///   otherwise the registry diverges from the mirror.
pub(super) fn persist_entry(
    backend: Option<&dyn RegistryBackend>,
    registries: &HashMap<String, registry::DocumentRegistry>,
    space_id: &str,
    document_id: &str,
    mirror: &MirrorMetadata<'_>,
) -> Result<(), RegistryStoreError> {
    let Some(backend) = backend else {
        return Ok(());
    };
    let Some(reg) = registries.get(space_id) else {
        return Ok(());
    };
    let Some(entry) = reg.get(document_id) else {
        return Ok(());
    };
    backend.save_entry(space_id, entry, mirror)
}

/// Persist a lifecycle operation that may have displaced other documents to
/// their conflict paths (path arbitration). The displaced losers are written
/// **first** — off the contested path — so the operated document's claim does
/// not collide with a loser's stale row under the mirror's
/// `UNIQUE(lower(path))`. This is what makes the OSS (in-memory) and kutlhub
/// (Postgres) backends agree: both conflict-copy; the constraint stays a
/// never-fire backstop rather than the thing that rejects the write. On any
/// failure the caller rolls back the whole in-memory operation via its token.
fn persist_with_displaced(
    backend: Option<&dyn RegistryBackend>,
    registries: &HashMap<String, registry::DocumentRegistry>,
    space_id: &str,
    document_id: &str,
    mirror: &MirrorMetadata<'_>,
    displaced: &[String],
) -> Result<(), RegistryStoreError> {
    let Some(backend) = backend else {
        return Ok(());
    };
    let Some(reg) = registries.get(space_id) else {
        return Ok(());
    };
    // Resolve each id's active entry (a missing/deleted id is a silent skip, the
    // same no-op `persist_entry` gave). Losers first, operated document last, so
    // the mirror's `UNIQUE(lower(path))` never momentarily collides — and the
    // whole set is written in ONE atomic batch, so a mid-batch failure can't
    // leave a committed prefix that diverges from the rolled-back in-memory
    // registry (the caller rolls back via its token on any error).
    let default_mirror = MirrorMetadata::default();
    let mut items = Vec::with_capacity(displaced.len() + 1);
    for loser in displaced {
        if let Some(entry) = reg.get(loser) {
            items.push((entry, &default_mirror));
        }
    }
    if let Some(entry) = reg.get(document_id) {
        items.push((entry, mirror));
    }
    if items.is_empty() {
        return Ok(());
    }
    backend.save_batch(space_id, &items)
}

/// Whether a user-supplied *intended* path embeds the conflict-copy infix. That
/// namespace is reserved for relay-generated displacement paths; a user path that
/// shadows a generated conflict path would let two alive documents resolve to one
/// effective path (the effective-path index keeps only one — a document silently
/// vanishes) and, in debug builds, can panic [`kutl_core::lattice::conflict_path`]
/// when an already-conflicted path itself loses arbitration. Rejected at the
/// register/rename boundary; relay-derived effective conflict paths never pass
/// through here.
fn is_reserved_conflict_path(path: &str) -> bool {
    path.contains(kutl_core::lattice::CONFLICT_INFIX)
}

/// Apply a register as either an idempotent re-register or a fresh insert,
/// returning `(is_reregister, rollback_token, displaced_loser_ids)`.
///
/// Idempotent register: if the
/// document already exists at the same path, treat the call as a mirror-field
/// patch (the relay's `save_entry` ON CONFLICT clause COALESCEs `title` /
/// `content_type` / convert provenance / `size_bytes`). Re-registers from the
/// UX-server migration paths (content-type sniffer, convert orphan re-emit,
/// ingestion re-pickup) flow through this branch so the mirror gets patched
/// without a separate "patch metadata" envelope. The in-memory `RegistryEntry`
/// is NOT mutated by a re-register (immutable fields stay; no path change) — the
/// caller's `save_entry` is the entire write. A fresh insert returns any
/// documents this register displaced to their conflict paths (path arbitration).
///
/// # Errors
/// Rejects a re-register that names a different path than the existing entry, or
/// one for a soft-deleted id (which must use a new id).
fn register_or_reregister(
    reg: &mut registry::DocumentRegistry,
    document_id: &str,
    path: &str,
    meta: registry::EntryMetadata,
) -> Result<(bool, registry::RollbackToken, Vec<String>), String> {
    if let Some(existing) = reg.get_any(document_id) {
        // Compare the requested path against the document's INTENDED path, not
        // its effective entry path: a conflict-copied (displaced) document's
        // entry path is the `.kutl-conflict-` path, but an idempotent re-register
        // legitimately names the original intended path. Comparing the effective
        // path would spuriously reject the re-register (and skip its mirror patch).
        let already_deleted = existing.deleted_at.is_some();
        let intended = reg.intended_path(document_id);
        if intended.as_deref() != Some(path) {
            return Err(format!(
                "register: document {document_id} already exists at \
                 {} but caller specified {path}",
                intended.as_deref().unwrap_or(path)
            ));
        }
        if already_deleted {
            return Err(format!(
                "register: document {document_id} is soft-deleted (use a new id)"
            ));
        }
        return Ok((true, registry::RollbackToken::default(), Vec::new()));
    }
    let (token, displaced) = reg.register(document_id, path, meta);
    Ok((false, token, displaced))
}

/// Error out a register/rename whose user-intended `path` embeds the reserved
/// conflict-copy infix. `intent` names the operation for the message.
fn reject_reserved_conflict_path(intent: &str, path: &str) -> Result<(), String> {
    if is_reserved_conflict_path(path) {
        return Err(format!(
            "{intent}: path {path} uses the reserved conflict-copy namespace"
        ));
    }
    Ok(())
}

/// Build a `sync::RegistryEntry` proto from the in-memory registry entry.
///
/// Used by `RegisterDocumentAck` to carry
/// the persisted snapshot back to the caller. Mirrors the 9 lifecycle +
/// 6 provenance fields of [`registry::RegistryEntry`]. The UX-only
/// `title` / `content_type` columns from the `documents` mirror are
/// intentionally NOT carried here — they're a mirror concern, not part
/// of the registry's state.
fn registry_entry_to_proto(entry: &registry::RegistryEntry) -> sync::RegistryEntry {
    sync::RegistryEntry {
        document_id: entry.document_id.clone(),
        path: entry.path.clone(),
        created_by: entry.created_by.clone(),
        created_at: entry.created_at,
        renamed_by: entry.renamed_by.clone(),
        renamed_at: entry.renamed_at,
        deleted_at: entry.deleted_at,
        account_id: entry.account_id.clone(),
        edited_at: entry.edited_at,
        originally_created_at: entry.originally_created_at,
        source_kind: entry.source_kind,
        source_id: entry.source_id.clone(),
        source_url: entry.source_url.clone(),
        ingestion_job_id: entry.ingestion_job_id.clone(),
        source_author_display: entry.source_author_display.clone(),
    }
}

/// Build a `sync::Error` for the failure branch of a lifecycle ack.
///
/// Mirrors the shape produced by `wrap_error` / `send_error` so callers
/// downstream can dispatch on the same `ErrorCode` they already handle.
fn lifecycle_ack_error(code: ErrorCode, message: impl Into<String>) -> sync::Error {
    sync::Error {
        code: code.into(),
        message: message.into(),
        ..Default::default()
    }
}

/// Extract registry metadata from a proto `ChangeMetadata`, or default.
///
/// `originally_created_at` is sourced from `RegisterDocument`'s
/// top-level field, not `ChangeMetadata`, so callers that want to
/// attach it must set it after this returns.
/// Decode the causal floor an inbound `RenameDocument` carries (the
/// `registered_hlc` the renamer observed). `None` from a pre-floor client or on
/// a malformed HLC. See [`registry::DocumentRegistry::rename`].
fn extract_rename_causal_floor(msg: &sync::RenameDocument) -> Option<kutl_core::Hlc> {
    msg.rename_causal_floor
        .clone()
        .and_then(|h| kutl_core::Hlc::try_from(h).ok())
}

/// Build the sanitized `RenameDocument` forwarded to other subscribers and the
/// observer: pin `metadata.author_did` to the authenticated identity (so no peer
/// sees a spoofed DID) and drive `new_path` to the LWW-winning `authoritative_path`
/// (not the sender's requested path), so a superseded rename still moves every
/// replica to the winning path. Returns the payload + the author DID the observer
/// records.
fn sanitize_forwarded_rename(
    msg: &sync::RenameDocument,
    authoritative_path: &str,
    authoritative: &str,
) -> (sync::RenameDocument, String) {
    let mut forwarded = msg.clone();
    forwarded.new_path.clear();
    forwarded.new_path.push_str(authoritative_path);
    let mut new_meta = forwarded.metadata.unwrap_or_default();
    new_meta.author_did.clear();
    new_meta.author_did.push_str(authoritative);
    forwarded.metadata = Some(new_meta);
    let observed_author_did = forwarded
        .metadata
        .as_ref()
        .map_or_else(String::new, |m| m.author_did.clone());
    (forwarded, observed_author_did)
}

fn extract_meta(proto: Option<&sync::ChangeMetadata>) -> registry::EntryMetadata {
    // Source-provenance fields (source_kind, source_id, source_url,
    // ingestion_job_id, source_author_display) live on
    // `sync::RegisterDocument` itself, not on `ChangeMetadata`.
    // Callers attach them via the bundle passed to
    // `register_document_internal`.
    proto.map_or_else(
        || registry::EntryMetadata {
            author_did: String::new(),
            timestamp: 0,
            ..Default::default()
        },
        |m| registry::EntryMetadata {
            author_did: m.author_did.clone(),
            timestamp: m.timestamp,
            // Origin HLC — the authoritative lifecycle order for rename/delete.
            // Absent on pre-HLC clients; the registry then falls back to the
            // timestamp (wall-clock) for ordering.
            hlc: m.hlc.clone().and_then(|h| kutl_core::Hlc::try_from(h).ok()),
            ..Default::default()
        },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use crate::record_log::SegmentRecordLog;

    use super::super::{RelayCommand, TEST_CONN_DID, connect_client, test_config};
    use super::*;

    /// Records how a persist resolved: one atomic `save_batch`, or N `save_entry`.
    struct CountingBackend {
        save_entry_calls: std::sync::atomic::AtomicUsize,
        save_batch_calls: std::sync::atomic::AtomicUsize,
        last_batch_paths: std::sync::Mutex<Vec<String>>,
        delete_entry_calls: std::sync::atomic::AtomicUsize,
        delete_batch_calls: std::sync::atomic::AtomicUsize,
        last_deleted: std::sync::Mutex<Vec<(String, i64)>>,
        /// When set, `delete_batch` records the call then returns an error so a
        /// test can drive the whole-batch rollback path.
        fail_delete: std::sync::atomic::AtomicBool,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                save_entry_calls: std::sync::atomic::AtomicUsize::new(0),
                save_batch_calls: std::sync::atomic::AtomicUsize::new(0),
                last_batch_paths: std::sync::Mutex::new(vec![]),
                delete_entry_calls: std::sync::atomic::AtomicUsize::new(0),
                delete_batch_calls: std::sync::atomic::AtomicUsize::new(0),
                last_deleted: std::sync::Mutex::new(vec![]),
                fail_delete: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    impl RegistryBackend for CountingBackend {
        fn load_all_including_deleted(
            &self,
            _: &str,
        ) -> Result<Vec<registry::RegistryEntry>, RegistryStoreError> {
            Ok(vec![])
        }
        fn load_spaces_including_deleted(&self) -> Result<Vec<String>, RegistryStoreError> {
            Ok(vec![])
        }
        fn save_entry(
            &self,
            _: &str,
            _: &registry::RegistryEntry,
            _: &MirrorMetadata<'_>,
        ) -> Result<(), RegistryStoreError> {
            self.save_entry_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn save_batch(
            &self,
            _: &str,
            items: &[(&registry::RegistryEntry, &MirrorMetadata<'_>)],
        ) -> Result<(), RegistryStoreError> {
            self.save_batch_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            *self.last_batch_paths.lock().unwrap() =
                items.iter().map(|(e, _)| e.path.clone()).collect();
            Ok(())
        }
        fn delete_entry(&self, _: &str, _: &str, _: i64) -> Result<(), RegistryStoreError> {
            self.delete_entry_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn delete_batch(&self, _: &str, deletes: &[(&str, i64)]) -> Result<(), RegistryStoreError> {
            use std::sync::atomic::Ordering::SeqCst;
            self.delete_batch_calls.fetch_add(1, SeqCst);
            *self.last_deleted.lock().unwrap() = deletes
                .iter()
                .map(|(id, ts)| ((*id).to_string(), *ts))
                .collect();
            if self.fail_delete.load(SeqCst) {
                // Any error type drives the rollback path; the variant is incidental.
                return Err(RegistryStoreError::MissingAccountIdForMirror {
                    space_id: "5171e0a1-1111-4000-8000-000000000006".into(),
                    document_id: "simulated-bulk-delete-failure".into(),
                });
            }
            Ok(())
        }
        fn update_edited_at(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: Option<&str>,
        ) -> Result<(), RegistryStoreError> {
            Ok(())
        }
    }

    /// A conflict-copy persist must issue exactly ONE atomic `save_batch`
    /// (losers first, operated doc last), not N independent `save_entry` writes —
    /// otherwise a mid-batch failure could leave a committed prefix.
    #[test]
    fn test_persist_with_displaced_uses_one_atomic_batch() {
        use registry::{DocumentRegistry, EntryMetadata};
        let meta = |did: &str, ts: i64| EntryMetadata {
            author_did: did.to_string(),
            timestamp: ts,
            ..Default::default()
        };
        let b = "00000000-0000-0000-0000-0000000000b0";
        let a = "00000000-0000-0000-0000-0000000000a0";
        let mut reg = DocumentRegistry::new();
        reg.register(b, "P.md", meta("did:b", 1000));
        // `a` wins the contested path; `b` is displaced to its conflict path.
        let (_token, displaced) = reg.register(a, "P.md", meta("did:a", 2000));
        assert_eq!(displaced.len(), 1, "exactly one doc displaced");

        let mut registries = HashMap::new();
        registries.insert("space-a".to_string(), reg);

        let backend = CountingBackend::new();
        let mirror = MirrorMetadata::default();
        persist_with_displaced(
            Some(&backend),
            &registries,
            "space-a",
            a,
            &mirror,
            &displaced,
        )
        .unwrap();

        let ord = std::sync::atomic::Ordering::SeqCst;
        assert_eq!(
            backend.save_batch_calls.load(ord),
            1,
            "exactly one atomic batch"
        );
        assert_eq!(backend.save_entry_calls.load(ord), 0, "no per-entry writes");
        let paths = backend.last_batch_paths.lock().unwrap();
        assert_eq!(paths.len(), 2, "loser + winner in the batch: {paths:?}");
        assert!(
            paths[0].contains(".kutl-conflict-"),
            "the displaced loser (conflict path) is written first: {paths:?}"
        );
        assert_eq!(
            paths[1], "P.md",
            "the operated doc (canonical) is written last: {paths:?}"
        );
    }

    // ---- path-arbitration conflict-copy broadcasts ----

    /// Register `document_id` at `path` from `conn_id`, stamped with a
    /// physical-time HLC of `physical_ms` so arbitration order is deterministic.
    async fn register_via_command(
        relay: &mut Relay,
        conn_id: ConnId,
        space: &str,
        document_id: &str,
        path: &str,
        physical_ms: u64,
    ) {
        let hlc = kutl_proto::sync::Hlc::from(kutl_core::Hlc::physical_touch(physical_ms));
        relay
            .process_command(RelayCommand::RegisterDocument {
                conn_id,
                msg: sync::RegisterDocument {
                    space_id: space.into(),
                    document_id: document_id.into(),
                    path: path.into(),
                    metadata: Some(sync::ChangeMetadata {
                        author_did: "did:test".into(),
                        timestamp: i64::try_from(physical_ms).expect("test stamp fits i64"),
                        hlc: Some(hlc),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            })
            .await;
    }

    /// Whether any control frame buffered on `ctrl_rx` is a `RenameDocument`
    /// moving `document_id` to `new_path` (drains the receiver).
    fn drained_rename_to(
        ctrl_rx: &mut mpsc::Receiver<Vec<u8>>,
        document_id: &str,
        new_path: &str,
    ) -> bool {
        let mut found = false;
        while let Ok(bytes) = ctrl_rx.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && let Some(sync::sync_envelope::Payload::RenameDocument(r)) = env.payload
                && r.document_id == document_id
                && r.new_path == new_path
            {
                found = true;
            }
        }
        found
    }

    #[tokio::test]
    async fn test_register_collision_displaces_prior_holder_and_broadcasts() {
        // Two distinct documents register at one path. The later (higher-HLC)
        // registration wins the canonical path; the prior holder is displaced to
        // its conflict path — and that displacement is broadcast to the space's
        // watchers (to everyone, so even the registrant learns the loser moved).
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let space = "5171e0a1-1111-4000-8000-000000000001";
        let doc_a = "11111111-1111-1111-1111-111111111111";
        let doc_b = "22222222-2222-2222-2222-222222222222";
        let path = "target.md";

        let (_a_data, _a_ack, mut a_ctrl) = connect_client(&mut relay, 1).await;
        let (_b_data, _b_ack, mut b_ctrl) = connect_client(&mut relay, 2).await;
        // Both watch the space so they receive lifecycle broadcasts.
        for conn_id in [1, 2] {
            relay
                .process_command(RelayCommand::ListSpaceDocuments {
                    conn_id,
                    msg: sync::ListSpaceDocuments {
                        space_id: space.into(),
                    },
                })
                .await;
        }
        while a_ctrl.try_recv().is_ok() {}
        while b_ctrl.try_recv().is_ok() {}

        // A registers first (lower HLC), B second (higher HLC → B wins the path).
        register_via_command(&mut relay, 1, space, doc_a, path, 1000).await;
        register_via_command(&mut relay, 2, space, doc_b, path, 2000).await;

        // Registry: B canonical at target, A displaced to its own conflict path.
        let reg = relay.registry(space).expect("space registry");
        assert_eq!(
            reg.get(doc_b).expect("doc_b active").path,
            path,
            "higher-HLC registrant keeps the canonical path"
        );
        let a_path = reg
            .get(doc_a)
            .expect("prior holder survives (conflict-copied, not destroyed)")
            .path
            .clone();
        assert!(
            a_path.contains(".kutl-conflict-") && a_path.contains(doc_a),
            "prior holder is displaced to its own conflict path: {a_path}"
        );

        // The displacement is broadcast to the space — A's client (a watcher and
        // the prior holder) must receive a RenameDocument moving doc_a there.
        assert!(
            drained_rename_to(&mut a_ctrl, doc_a, &a_path),
            "displaced prior holder's move to its conflict path is broadcast"
        );
    }

    #[tokio::test]
    async fn test_bulk_unregister_uses_one_delete_batch() {
        use std::sync::atomic::Ordering::SeqCst;
        let backend = Arc::new(CountingBackend::new());
        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, Some(backend.clone()), None, None, None);
        let space = "5171e0a1-1111-4000-8000-000000000001";
        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;

        // Three docs, including a conflict-copy pair, so the batch delete spans an
        // arbitrated (displaced) set, not just trivial standalone rows.
        let doc_a = "11111111-1111-1111-1111-111111111111";
        let doc_b = "22222222-2222-2222-2222-222222222222";
        let doc_c = "33333333-3333-3333-3333-333333333333";
        register_via_command(&mut relay, 1, space, doc_a, "notes.md", 1000).await;
        register_via_command(&mut relay, 1, space, doc_b, "notes.md", 2000).await; // displaced
        register_via_command(&mut relay, 1, space, doc_c, "other.md", 3000).await;
        while ack.try_recv().is_ok() {}

        relay
            .process_command(RelayCommand::UnregisterSpaceOp {
                conn_id: 1,
                msg: sync::UnregisterSpace {
                    space_id: space.into(),
                    // Stamp the delete above every registration HLC so it wins
                    // the lifecycle LWW (a delete below them would correctly lose).
                    metadata: Some(sync::ChangeMetadata {
                        author_did: "did:test".into(),
                        timestamp: 9000,
                        ..Default::default()
                    }),
                },
            })
            .await;

        assert_eq!(
            backend.delete_batch_calls.load(SeqCst),
            1,
            "bulk unregister persists in exactly one atomic delete_batch"
        );
        assert_eq!(
            backend.delete_entry_calls.load(SeqCst),
            0,
            "no per-doc delete_entry writes (that was the O(N) round-trip cost)"
        );
        assert_eq!(
            backend.last_deleted.lock().unwrap().len(),
            3,
            "all three docs ride in the one batch"
        );
        let reg = relay.registry(space).expect("space registry");
        assert!(
            reg.get(doc_a).is_none() && reg.get(doc_b).is_none() && reg.get(doc_c).is_none(),
            "every doc is soft-deleted"
        );
    }

    #[tokio::test]
    async fn test_bulk_unregister_rolls_back_whole_batch_on_persist_failure() {
        use std::sync::atomic::Ordering::SeqCst;
        let backend = Arc::new(CountingBackend::new());
        backend.fail_delete.store(true, SeqCst);
        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, Some(backend.clone()), None, None, None);
        let space = "5171e0a1-1111-4000-8000-000000000001";
        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;

        let doc_a = "11111111-1111-1111-1111-111111111111";
        let doc_b = "22222222-2222-2222-2222-222222222222";
        register_via_command(&mut relay, 1, space, doc_a, "a.md", 1000).await;
        register_via_command(&mut relay, 1, space, doc_b, "b.md", 2000).await;
        while ack.try_recv().is_ok() {}

        relay
            .process_command(RelayCommand::UnregisterSpaceOp {
                conn_id: 1,
                msg: sync::UnregisterSpace {
                    space_id: space.into(),
                    // Stamp the delete above every registration HLC so it wins
                    // the lifecycle LWW (a delete below them would correctly lose).
                    metadata: Some(sync::ChangeMetadata {
                        author_did: "did:test".into(),
                        timestamp: 9000,
                        ..Default::default()
                    }),
                },
            })
            .await;

        assert_eq!(
            backend.delete_batch_calls.load(SeqCst),
            1,
            "one atomic batch attempted"
        );
        // The failed persist rolled the WHOLE in-memory batch back — both docs are
        // alive at their original paths, never half-deleted.
        let reg = relay.registry(space).expect("space registry");
        assert_eq!(
            reg.get(doc_a).map(|e| e.path.as_str()),
            Some("a.md"),
            "doc_a restored alive"
        );
        assert_eq!(
            reg.get(doc_b).map(|e| e.path.as_str()),
            Some("b.md"),
            "doc_b restored alive"
        );
    }

    /// Evicting a space's writer on unregister: a space whose signals
    /// were appended holds a cached `SegmentWriter` (two open fds + a flock).
    /// `handle_unregister_space` must evict it so a long-lived relay does not
    /// leak an fd per space forever. After unregister the writer entry is GONE,
    /// and a later append re-creates it and still reads back all prior records
    /// (segments persist; only the in-memory writer was dropped).
    ///
    /// The bulk delete also cascade-tombstones the space's visible signals,
    /// so the appended `r1` CREATED gains a TOMBSTONED transition
    /// record; this test asserts no DATA LOSS (both `r1` and `r2` survive the
    /// unregister), not an exact record set.
    #[tokio::test]
    async fn test_unregister_space_evicts_signal_writer_no_data_loss() {
        use crate::signal_store::SignalStore;
        use kutl_proto::sync::{Hlc, Signal, SignalEventType};

        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        relay.record_log = crate::relay::signal_log::SignalLogHandle::new(
            Some(std::sync::Arc::new(SegmentRecordLog::new(
                SignalStore::new(dir.path().to_path_buf()),
            ))),
            None,
        );

        // A UUID space id (real spaces are UUIDs; the eviction keys off a
        // parseable UUID).
        let space = uuid::Uuid::from_u128(0x42);
        let space_id = space.to_string();

        let record = |rec: &str| -> Signal {
            let mut s = Signal {
                id: format!("sig-{rec}"),
                space_id: space_id.clone(),
                record_id: rec.into(),
                hlc: Some(Hlc {
                    physical_ms: 1,
                    logical: 0,
                    actor: vec![0u8; 16],
                }),
                ..Default::default()
            };
            s.set_event(SignalEventType::Created);
            s
        };

        // Append a record so a writer is cached for the space.
        relay
            .append_materialized_record(&record("r1"), "sig-r1", &space_id)
            .await
            .unwrap();
        assert!(
            relay.record_log.has_open_writer(space),
            "store reports the writer present after append"
        );

        // Register a doc so the space has active documents to unregister
        // (the eviction fires regardless, but this exercises the full path).
        let doc = "11111111-1111-1111-1111-111111111111";
        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        register_via_command(&mut relay, 1, &space_id, doc, "notes.md", 1000).await;
        while ack.try_recv().is_ok() {}

        relay
            .process_command(RelayCommand::UnregisterSpaceOp {
                conn_id: 1,
                msg: sync::UnregisterSpace {
                    space_id: space_id.clone(),
                    metadata: Some(sync::ChangeMetadata {
                        author_did: "did:test".into(),
                        timestamp: 9000,
                        ..Default::default()
                    }),
                },
            })
            .await;

        // The cached writer entry is GONE (its fds + flock released).
        assert!(
            !relay.record_log.has_open_writer(space),
            "unregister must evict the space's cached signal writer"
        );

        // A subsequent append re-creates the writer and still reads back all
        // prior records — the segments persisted on disk.
        relay
            .append_materialized_record(&record("r2"), "sig-r2", &space_id)
            .await
            .unwrap();
        assert!(
            relay.record_log.has_open_writer(space),
            "append after unregister re-creates the writer"
        );
        relay.record_log.evict(space).await;
        let records = relay.record_log.load_space(space).await.unwrap();
        let ids: Vec<_> = records.iter().map(|r| r.record_id.clone()).collect();
        // No data loss: both the pre-unregister and post-unregister CREATED
        // records survive. (The set also contains the delete-cascade's TOMBSTONED
        // transition for `sig-r1`, which carries a fresh UUID record_id.)
        assert!(
            ids.contains(&"r1".to_string()),
            "r1 survives unregister, got {ids:?}"
        );
        assert!(
            ids.contains(&"r2".to_string()),
            "r2 survives unregister, got {ids:?}"
        );
    }

    /// The `entry.path` of any `RegisterDocumentAck` for `document_id` buffered
    /// on `ack_rx` (drains the receiver), if one carries a persisted entry.
    /// Register acks ride the own-ack lane, so the registrant reads them here.
    fn drained_register_ack_path(
        ack_rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
        document_id: &str,
    ) -> Option<String> {
        let mut path = None;
        while let Ok(bytes) = ack_rx.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && let Some(sync::sync_envelope::Payload::RegisterDocumentAck(ack)) = env.payload
                && ack.document_id == document_id
            {
                path = ack.entry.map(|e| e.path);
            }
        }
        path
    }

    #[tokio::test]
    async fn test_register_loser_ack_carries_conflict_path() {
        // The operated document that LOST a path collision is corrected via its
        // own typed ack (not a separate broadcast): `RegisterDocumentAck.entry`
        // carries the post-arbitration effective (conflict) path, which the
        // registrant's daemon reconciles its local file to.
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let space = "5171e0a1-1111-4000-8000-000000000001";
        let winner = "11111111-1111-1111-1111-111111111111";
        let loser = "22222222-2222-2222-2222-222222222222";
        let path = "target.md";

        let (_w_data, mut w_ack, _w_ctrl) = connect_client(&mut relay, 1).await;
        let (_l_data, mut l_ack, _l_ctrl) = connect_client(&mut relay, 2).await;

        // Winner registers first (higher HLC); loser second (lower HLC → loses).
        register_via_command(&mut relay, 1, space, winner, path, 2000).await;
        register_via_command(&mut relay, 2, space, loser, path, 1000).await;

        let loser_path = drained_register_ack_path(&mut l_ack, loser)
            .expect("loser receives a RegisterDocumentAck carrying its persisted entry");
        assert!(
            loser_path.contains(".kutl-conflict-") && loser_path.contains(loser),
            "loser's own ack carries its conflict path, not the contested one: {loser_path}"
        );
        assert_eq!(
            drained_register_ack_path(&mut w_ack, winner).as_deref(),
            Some(path),
            "winner's ack carries the canonical path"
        );
    }

    /// Collect the `document_id` of every successful `RegisterDocumentAck`
    /// buffered on `ack_rx` (drains the receiver). Mirrors
    /// `drained_register_ack_path` but keys on success, not the entry path, so a
    /// bulk burst can assert no ack was dropped. Register acks ride the
    /// unbounded own-ack lane, which is exactly what makes the drop impossible.
    fn drained_register_ack_success_ids(
        ack_rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
    ) -> std::collections::HashSet<String> {
        let mut ids = std::collections::HashSet::new();
        while let Ok(bytes) = ack_rx.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && let Some(sync::sync_envelope::Payload::RegisterDocumentAck(ack)) = env.payload
                && ack.success
            {
                ids.insert(ack.document_id);
            }
        }
        ids
    }

    #[tokio::test]
    async fn test_bulk_register_acks_never_dropped() {
        // A bulk register of more docs than the cap-16 ctrl
        // channel holds (`conn.rs:14`) must still deliver a success ack for
        // EVERY doc. If acks rode the bounded ctrl lane, acks past slot 16
        // would vanish and those docs would strand at
        // `confirmed:false` forever. Register
        // acks ride the unbounded own-ack lane, so the burst is loss-free.

        /// Docs registered in one burst — well over the cap-16 ctrl channel so
        /// the overflow path is exercised, but small enough to stay a fast unit
        /// test.
        const BULK_DOC_COUNT: usize = 40;

        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let space = "5171e0a1-1111-4000-8000-000000000001";
        // Single client; do NOT drain its ack channel mid-burst. Register acks
        // ride the unbounded own-ack lane, so a >16 burst must still deliver
        // every ack — the cap-16 ctrl channel is not in the ack path.
        let (_data, mut ack_rx, _ctrl_rx) = connect_client(&mut relay, 1).await;

        let doc_ids: Vec<String> = (0..BULK_DOC_COUNT)
            .map(|i| format!("00000000-0000-0000-0000-{i:012}"))
            .collect();
        for (i, id) in doc_ids.iter().enumerate() {
            // Distinct paths so no registration is a path collision (collisions
            // would change the ack's entry but not its success/presence).
            let path = format!("doc-{i}.md");
            // Monotonic physical HLC keeps arbitration deterministic.
            register_via_command(&mut relay, 1, space, id, &path, 1000 + i as u64).await;
        }

        // Drain after the burst: every doc must have a success ack.
        let acked = drained_register_ack_success_ids(&mut ack_rx);
        let missing: Vec<&String> = doc_ids.iter().filter(|id| !acked.contains(*id)).collect();
        assert!(
            missing.is_empty(),
            "every bulk-registered doc must receive a success ack; {} dropped: {missing:?}",
            missing.len()
        );
    }

    #[tokio::test]
    async fn test_reregister_displaced_doc_at_intended_path_succeeds() {
        // A conflict-copied (displaced) document re-registered at its INTENDED
        // path (the UX-server content-type sniffer / ingestion re-pickup paths)
        // must be accepted as an idempotent re-register — NOT rejected because its
        // effective entry path is the `.kutl-conflict-` path.
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let space = "5171e0a1-1111-4000-8000-000000000001";
        let winner = "11111111-1111-1111-1111-111111111111";
        let loser = "22222222-2222-2222-2222-222222222222";
        let path = "target.md";
        let (_w, mut w_ack, mut w_ctrl) = connect_client(&mut relay, 1).await;
        let (_l, mut l_ack, mut l_ctrl) = connect_client(&mut relay, 2).await;

        register_via_command(&mut relay, 1, space, winner, path, 2000).await;
        register_via_command(&mut relay, 2, space, loser, path, 1000).await;
        let displaced = relay
            .registry(space)
            .unwrap()
            .get(loser)
            .unwrap()
            .path
            .clone();
        assert!(
            displaced.contains(".kutl-conflict-"),
            "loser is displaced: {displaced}"
        );
        while w_ack.try_recv().is_ok() {}
        while l_ack.try_recv().is_ok() {}
        while w_ctrl.try_recv().is_ok() {}
        while l_ctrl.try_recv().is_ok() {}

        // Re-register the displaced loser at its intended path — idempotent. The
        // loser's own RegisterDocumentAck rides the own-ack lane.
        register_via_command(&mut relay, 2, space, loser, path, 1000).await;
        let mut success = None;
        while let Ok(bytes) = l_ack.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && let Some(sync::sync_envelope::Payload::RegisterDocumentAck(a)) = env.payload
                && a.document_id == loser
            {
                success = Some(a.success);
            }
        }
        assert_eq!(
            success,
            Some(true),
            "re-register of a displaced doc at its intended path is idempotent (succeeds)"
        );
        assert!(
            relay.registry(space).unwrap().get(loser).is_some(),
            "the displaced loser survives the idempotent re-register"
        );
    }

    #[tokio::test]
    async fn test_register_rejects_reserved_conflict_path() {
        // A user-intended path embedding the conflict-copy infix is reserved for
        // relay-generated displacement paths; registering one must be rejected, not
        // stored (else a user file could shadow a generated conflict path and
        // double-book the effective-path index).
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let space = "5171e0a1-1111-4000-8000-000000000001";
        let doc = "11111111-1111-1111-1111-111111111111";
        let (_data, _ack, _ctrl) = connect_client(&mut relay, 1).await;

        register_via_command(
            &mut relay,
            1,
            space,
            doc,
            "notes.kutl-conflict-deadbeef.md",
            1000,
        )
        .await;

        assert!(
            relay.registry(space).and_then(|r| r.get(doc)).is_none(),
            "a register at a reserved conflict-copy path must be rejected, not stored"
        );
    }

    #[tokio::test]
    async fn test_rename_rejects_reserved_conflict_path() {
        // Likewise a rename onto a reserved conflict-copy path must be rejected,
        // leaving the document at its current path.
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let space = "5171e0a1-1111-4000-8000-000000000001";
        let doc = "11111111-1111-1111-1111-111111111111";
        let (_data, _ack, _ctrl) = connect_client(&mut relay, 1).await;

        register_via_command(&mut relay, 1, space, doc, "notes.md", 1000).await;
        relay
            .process_command(RelayCommand::RenameDocument {
                conn_id: 1,
                msg: sync::RenameDocument {
                    space_id: space.into(),
                    document_id: doc.into(),
                    old_path: "notes.md".into(),
                    new_path: "notes.kutl-conflict-deadbeef.md".into(),
                    metadata: Some(sync::ChangeMetadata {
                        author_did: "did:test".into(),
                        timestamp: 2000,
                        ..Default::default()
                    }),
                    rename_causal_floor: None,
                },
            })
            .await;

        assert_eq!(
            relay
                .registry(space)
                .and_then(|r| r.get(doc))
                .map(|e| e.path.clone())
                .as_deref(),
            Some("notes.md"),
            "a rename onto a reserved conflict-copy path must be rejected, leaving the doc in place"
        );
    }

    #[tokio::test]
    async fn test_full_broadcast_lane_evicts_subscriber() {
        // Invariant: a lifecycle broadcast to a subscriber whose
        // bounded ctrl lane is FULL must EVICT that subscriber (not silently
        // drop the frame in place). The evicted subscriber re-syncs on
        // reconnect, so eviction is the recoverable, OOM-bounded policy.

        /// One-slot ctrl lane so a single undrained broadcast fills it and the
        /// next broadcast hits the full-lane eviction path.
        const ONE_SLOT_CTRL: usize = 1;

        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let space = "5171e0a1-1111-4000-8000-000000000001";

        // A registrant whose acks/broadcasts we don't care about.
        let (_w_data, _w_ack, _w_ctrl) = connect_client(&mut relay, 1).await;

        // The victim: a space watcher with a 1-slot ctrl that we never drain.
        // The receivers stay bound (undrained) so the relay-side senders remain
        // alive and the single ctrl slot fills.
        let watcher = 2;
        let (v_data_tx, _v_data_rx) = mpsc::channel(16);
        let (v_ctrl_tx, _v_ctrl_rx) = mpsc::channel(ONE_SLOT_CTRL);
        let (v_ack_tx, _v_ack_rx) = mpsc::unbounded_channel();
        relay
            .process_command(RelayCommand::Connect {
                conn_id: watcher,
                tx: v_data_tx,
                ctrl_tx: v_ctrl_tx,
                ack_tx: v_ack_tx,
            })
            .await;
        // Authentication is mandatory; the watcher must be
        // authorized or `ListSpaceDocuments` is rejected before it registers.
        relay.test_set_authenticated(watcher, TEST_CONN_DID);
        relay
            .process_command(RelayCommand::ListSpaceDocuments {
                conn_id: watcher,
                msg: sync::ListSpaceDocuments {
                    space_id: space.into(),
                },
            })
            .await;
        assert!(
            relay.connections.contains_key(&watcher),
            "watcher is connected before the broadcast burst"
        );

        // First register: its broadcast fills the watcher's single ctrl slot
        // (undrained) but does NOT evict — the lane had room.
        register_via_command(&mut relay, 1, space, "doc-a", "a.md", 1000).await;
        assert!(
            relay.connections.contains_key(&watcher),
            "the first broadcast fits the empty slot — no eviction yet"
        );

        // Second register: the broadcast finds the lane full and must evict.
        register_via_command(&mut relay, 1, space, "doc-b", "b.md", 2000).await;
        assert!(
            !relay.connections.contains_key(&watcher),
            "a broadcast to a full ctrl lane evicts the subscriber"
        );
        assert!(
            relay
                .space_watchers
                .get(space)
                .is_none_or(|w| !w.contains(&watcher)),
            "the evicted subscriber is dropped from the space watcher set"
        );
    }
}
