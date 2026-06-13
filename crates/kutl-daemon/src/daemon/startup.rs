//! Startup reconciliation for [`SpaceWorker`]: the session-start sequence that
//! reconciles local state against the relay's authoritative document list —
//! the offline-rename pre-pass, the reconcile truth-table execution, the
//! initial file scan, and the shared `register_and_subscribe` emitter.
//!
//! Ordering contract: the CALLER (`run_session`, daemon.rs) runs
//! `startup_reconciliation` BEFORE the watcher starts (no echo suppressions are
//! registered here) and `initial_file_scan` AFTER it (A4f: the watcher must be
//! live before the scan so mid-scan creates are not silently invisible).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use kutl_core::Hlc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::SafeRelayPath;
use crate::client::{SyncCommand, SyncEvent};
use crate::core::rel_path_to_string;
use crate::reconcile::{self, ReconcileInputs, StartupAction};
use crate::watcher::{self, FileEvent, Suppression};

use super::SpaceWorker;
use super::effects::{encode_delta, remove_doc, rename_doc};

impl SpaceWorker {
    /// Register a document with the relay and subscribe to it.
    ///
    /// Best-effort stats the filesystem birthtime so the relay can
    /// populate `documents.originally_created_at`. A
    /// missing/unsupported birthtime sends `None`; the relay leaves
    /// the column NULL in that case. Never falls back to mtime — that
    /// would produce a wrong "originally created" claim.
    pub(super) fn register_and_subscribe(
        &mut self,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        document_id: &str,
        path: &str,
        intent: &str,
    ) -> Result<()> {
        let meta = self.make_metadata(intent);
        // Record the local register HLC as the causal-floor source for a later
        // local rename of this doc (the precise register stamp, NOT the watermark).
        if let Some(hlc) = meta.hlc.clone().and_then(|w| Hlc::try_from(w).ok()) {
            self.record_register_hlc(document_id, hlc);
            // SYMMETRY with the core mint (`register_document_effect`): seed
            // the registrant's own lifecycle watermark with its register stamp
            // — fold-only, never gated on at registration, never recv'd —
            // keeping the freshness gate's inputs uniform across registrant
            // and observers.
            self.record_lifecycle_hlc(document_id, hlc);
        }
        // Fold our own imperative register into the gamma placement lattice so
        // same-path collisions arbitrate against the FULL record set (see
        // `fold_local_register_record`).
        self.fold_local_register_record(document_id, path, &meta);
        let abs_path = self.config.space_root.join(path);
        let originally_created_at_ms = crate::birthtime::get_birthtime_ms(&abs_path);
        let register = SyncCommand::RegisterDocument {
            space_id: self.config.space_id.clone(),
            document_id: document_id.to_owned(),
            path: path.to_owned(),
            metadata: Some(meta),
            originally_created_at_ms,
        };
        self.send_cmd(sync_cmd_tx, register)?;
        self.send_cmd(
            sync_cmd_tx,
            SyncCommand::Subscribe {
                document_id: document_id.to_owned(),
            },
        )?;
        Ok(())
    }

    /// Gather inputs, run reconciliation, and execute the resulting actions.
    ///
    /// Replaces the previous scattered approach (inline `ListSpaceDocuments`
    /// handling, `reconcile_remotely_deleted`, local doc registration,
    /// `reconcile_missing_files`) with a single flow:
    ///
    /// 1. Fetch the relay's active document list
    /// 2. Build reconciliation inputs from three sources of truth
    /// 3. Call [`reconcile::reconcile_startup`] to produce actions
    /// 4. Execute each action in order
    pub(super) async fn startup_reconciliation(
        &mut self,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
    ) -> Result<()> {
        // Step 1: Fetch the relay's active document list.
        debug!(space_id = %self.config.space_id, "sending ListSpaceDocuments request");
        self.send_cmd(
            sync_cmd_tx,
            SyncCommand::ListSpaceDocuments {
                space_id: self.config.space_id.clone(),
            },
        )?;

        let remote_active = self.wait_for_document_list(sync_event_rx).await?;
        // The `SpaceDocuments` response is this command's ack (it does not flow
        // through `handle_lifecycle_ack`), so clear its backlog entry here. This
        // runs entirely within startup, before the live event loop's intake gate
        // is active, but keeps the counter honest across the session.
        self.intake.ack();

        // Step 1.5: Resolve documents renamed on disk while offline, BEFORE the
        // truth table. Each is a tracked doc whose recorded path is gone but
        // whose inode locates the file at a new path; the truth table would read
        // the gone recorded path as a local delete and destroy the identity, and
        // the new-file scan would then mint a spurious second UUID for the moved
        // file. Resolving here against the relay's authoritative state keeps one
        // document one identity. The watcher is not yet running, so the file
        // moves/removes below generate no events to suppress.
        let handled = self.reconcile_offline_renames(&remote_active, sync_cmd_tx)?;

        // Step 2: Build reconciliation inputs. `previously_remote` (the
        // `was_remote` axis) is derived from each document's persisted
        // `confirmed` flag — no separate snapshot to drift out of sync.
        // Documents already resolved by the offline-rename pre-pass are excluded
        // FROM BOTH AXES so the truth table does not re-process (and undo) them:
        // the relay's list still has each handled doc at its pre-rename path
        // (fetched before the pre-pass pushed the rename), so leaving it in
        // `remote_active` would re-classify the doc as remote-only and act on
        // the stale path. The full list still feeds `confirm_remote_documents`
        // below — a handled doc remains a live relay doc.
        let state_entries: HashMap<PathBuf, String> = self
            .state
            .state
            .documents
            .iter()
            .filter(|(_, v)| !handled.contains(&v.id))
            .map(|(k, v)| (PathBuf::from(k), v.id.clone()))
            .collect();
        let remote_unhandled: HashMap<String, SafeRelayPath> = remote_active
            .iter()
            .filter(|(id, _)| !handled.contains(*id))
            .map(|(id, path)| (id.clone(), path.clone()))
            .collect();
        let previously_remote = self.state.state.confirmed_ids();
        // The `materialized` axis: docs whose persisted entry records an inode
        // — i.e. whose file bytes have actually landed on this disk. Separates
        // a genuine offline delete (SendUnregister) from the crash window
        // where an identity was claimed + persisted but the content never
        // arrived (re-subscribe instead; see the truth-table note).
        let materialized: HashSet<String> = self
            .state
            .state
            .documents
            .values()
            .filter(|e| e.inode.is_some())
            .map(|e| e.id.clone())
            .collect();

        let inputs = ReconcileInputs {
            state_entries: &state_entries,
            previously_remote: &previously_remote,
            remote_active: &remote_unhandled,
            materialized: &materialized,
            space_root: &self.config.space_root,
        };

        // Step 3: Produce actions.
        let actions = reconcile::reconcile_startup(&inputs);

        // Step 4: Execute actions.
        self.execute_reconcile_actions(&actions, sync_cmd_tx, suppress_tx)?;

        // Step 5: Every document the relay currently lists is confirmed, so a
        // later removal while this daemon is offline classifies as a remote
        // deletion (DeleteLocal) — not a never-synced local file (SyncLocal) —
        // on the next start. Documents learned live during the session are
        // confirmed by the `DocumentRegistered` handler.
        self.confirm_remote_documents(&remote_active);
        Ok(())
    }

    /// Resolve every document renamed on disk while the daemon was offline,
    /// returning the set of document ids handled (excluded from the subsequent
    /// truth table). Three outcomes per moved document, by the relay's
    /// authoritative state:
    ///
    /// - **relay still at the recorded path** — the cluster did not touch D, so
    ///   the offline rename is the only change: acknowledge it locally and push
    ///   `RenameDocument` so the cluster learns the new path.
    /// - **relay moved D to a different path** — the cluster renamed D
    ///   concurrently. An offline rename carries no durable origin stamp, so it
    ///   cannot prove it is causally newer and **loses to the cluster's path**
    ///   (no last-to-rejoin clobber): conform the local file to the
    ///   relay path.
    /// - **relay no longer has D (and D was confirmed)** — the cluster deleted D
    ///   while we were offline. The offline rename is concurrent with that
    ///   delete, not causally after it, so it does **not** revive D:
    ///   remove the moved file and drop the identity.
    ///
    /// A moved file for a never-confirmed document (the relay never knew it) is
    /// left for the normal new-file scan to register fresh.
    fn reconcile_offline_renames(
        &mut self,
        remote_active: &HashMap<String, SafeRelayPath>,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) -> Result<HashSet<String>> {
        let mut handled = HashSet::new();
        let confirmed = self.state.state.confirmed_ids();
        // The clock has not ticked yet this session (remote ops are processed
        // only later, in the event loop), so the floor is the persisted
        // pre-offline stamp. Stamping a re-emitted offline rename with it makes
        // the rename lose to any cluster op that happened after we went offline,
        // and sets our per-doc watermark low enough that we still accept (and
        // conform to) that cluster op when it arrives.
        let offline_floor = self.offline_floor();
        for (document_id, old_path, new_local) in self.detect_offline_renames() {
            match remote_active.get(&document_id) {
                Some(relay_path) if relay_path.as_path() == old_path => {
                    info!(%document_id, old = %old_path.display(), new = %new_local.display(), "offline rename: propagating to cluster (stale-stamped)");
                    self.move_identity(&old_path, new_local.clone(), &document_id);
                    // Read the causal floor BEFORE the metadata stamp folds this
                    // rename's HLC, so it reflects the prior registration.
                    let rename_causal_floor = self.rename_causal_floor(&document_id);
                    let meta = self.make_lifecycle_metadata_with_hlc(
                        &document_id,
                        "offline rename",
                        offline_floor,
                    );
                    // Fold OUR OWN offline rename into the gamma placement
                    // lattice. Without this, each rejoiner's `known_records`
                    // holds only the PEER's concurrently-broadcast rename, so
                    // two devices that both renamed the same doc offline each
                    // "converge" to the OTHER's name — the deterministic
                    // equal-stamp tiebreak (`DocRecord::merge`: lexicographically
                    // larger path) never sees both candidates on either node
                    // (the double-rejoin winner swap). Same missing-fold class
                    // as the inode detectors (`fold_local_rename_record`).
                    self.fold_local_rename_record(
                        &document_id,
                        &new_local,
                        &meta,
                        rename_causal_floor,
                    );
                    let rename = SyncCommand::RenameDocument {
                        space_id: self.config.space_id.clone(),
                        document_id: document_id.clone(),
                        old_path: rel_path_to_string(&old_path),
                        new_path: rel_path_to_string(&new_local),
                        metadata: Some(meta),
                        rename_causal_floor,
                    };
                    self.send_cmd(sync_cmd_tx, rename)?;
                    self.send_cmd(
                        sync_cmd_tx,
                        SyncCommand::Subscribe {
                            document_id: document_id.clone(),
                        },
                    )?;
                    handled.insert(document_id);
                }
                Some(relay_path) => {
                    info!(%document_id, offline = %new_local.display(), authoritative = %relay_path, "offline rename loses to cluster rename; conforming to relay path");
                    let from_abs = self.config.space_root.join(&new_local);
                    let to_abs = relay_path.under(&self.config.space_root);
                    if let Some(parent) = to_abs.parent()
                        && let Err(e) = std::fs::create_dir_all(parent)
                    {
                        // Logged, not fatal: the rename below fails loudly on
                        // a genuinely missing parent.
                        warn!(parent = %parent.display(), error = %e, "failed to create parent dir for conform");
                    }
                    if let Err(e) = std::fs::rename(&from_abs, &to_abs) {
                        error!(from = %from_abs.display(), to = %to_abs.display(), error = %e, "failed to conform offline-renamed file to relay path");
                    }
                    let relay_buf = relay_path.as_path().to_path_buf();
                    self.move_identity(&old_path, relay_buf, &document_id);
                    self.send_cmd(
                        sync_cmd_tx,
                        SyncCommand::Subscribe {
                            document_id: document_id.clone(),
                        },
                    )?;
                    handled.insert(document_id);
                }
                None if confirmed.contains(&document_id) => {
                    info!(%document_id, offline = %new_local.display(), "offline rename of a doc the cluster deleted; honoring delete (no revival)");
                    let abs = self.config.space_root.join(&new_local);
                    if let Err(e) = std::fs::remove_file(&abs) {
                        error!(path = %abs.display(), error = %e, "failed to remove offline-renamed file the cluster deleted");
                    }
                    self.cleanup_document_state(&old_path);
                    self.unregister_identity(&document_id);
                    handled.insert(document_id);
                }
                None => {
                    // Relay never knew this document — leave the moved file for
                    // the new-file scan to register fresh.
                }
            }
        }
        Ok(handled)
    }

    /// Mark every document the relay currently lists as confirmed, persisting
    /// once if anything changed. Idempotent (monotone join).
    fn confirm_remote_documents(&mut self, remote_active: &HashMap<String, SafeRelayPath>) {
        let paths: Vec<String> = remote_active
            .keys()
            .filter_map(|uuid| self.state.uuid_to_path.get(uuid))
            .map(|p| rel_path_to_string(p))
            .collect();
        let mut changed = false;
        for path in paths {
            changed |= self.state.state.confirm(&path);
        }
        if changed {
            self.save_state();
        }
    }

    /// Execute a list of reconciliation actions produced by the truth table.
    fn execute_reconcile_actions(
        &mut self,
        actions: &[StartupAction],
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        // Collect IDs to unregister after the loop (avoids borrow conflicts).
        let mut to_unregister = Vec::new();

        for action in actions {
            match action {
                StartupAction::SubscribeRemote { document_id, path } => {
                    let rel = path.as_path().to_path_buf();
                    // Path-arbitration conflict-copy at rejoin: if
                    // an offline-created local file already occupies this path (on
                    // disk, untracked — our state never mapped it to THIS doc), it
                    // is a DISTINCT document that collides. Defer the remote doc
                    // instead of adopting the path for it: the new-file scan claims
                    // the local file (a fresh uuid), the relay arbitrates, and the
                    // runtime displacement/drain materializes both as conflict
                    // copies. Adopting here would instead merge the offline content
                    // into the remote doc via incorporate_pending_edits.
                    if !self.state.uuid_to_path.contains_key(document_id)
                        && self.uuid_at(&rel).is_none()
                        && self.config.space_root.join(&rel).exists()
                    {
                        debug!(%document_id, path = %path, "deferring remote subscribe: path held by an offline-created file");
                        self.state
                            .deferred
                            .insert(document_id.clone(), path.clone());
                        continue;
                    }
                    info!(%document_id, path = %path, "subscribing to remote document");
                    if !self.state.uuid_to_path.contains_key(document_id) {
                        self.register_identity(path.clone(), document_id.clone(), true);
                    }
                    self.send_cmd(
                        sync_cmd_tx,
                        SyncCommand::Subscribe {
                            document_id: document_id.clone(),
                        },
                    )?;
                }
                StartupAction::SyncLocal { document_id, path } => {
                    self.sync_local_document(document_id, path, sync_cmd_tx)?;
                }
                StartupAction::SendUnregister { document_id, path } => {
                    // A delete detected at startup happened while we were offline.
                    // Stamp it one ms above the document's observed content liveness
                    // (its latest CRDT change timestamp) so it wins over edits we
                    // have seen but loses to a concurrent peer edit we have not —
                    // see `offline_delete_stamp`. Read the stamp BEFORE
                    // `cleanup_document_state`, which drops the CRDT we read from.
                    info!(path = %path.display(), %document_id, "file deleted locally while offline, unregistering");
                    let stamp = self.offline_delete_stamp(document_id);
                    self.cleanup_document_state(path);
                    let meta = self.make_lifecycle_metadata_with_hlc(
                        document_id,
                        "offline file delete",
                        stamp,
                    );
                    self.send_cmd(
                        sync_cmd_tx,
                        SyncCommand::UnregisterDocument {
                            space_id: self.config.space_id.clone(),
                            document_id: document_id.clone(),
                            metadata: Some(meta),
                        },
                    )?;
                    to_unregister.push(document_id.clone());
                }
                StartupAction::RenameLocal {
                    document_id,
                    old_path,
                    new_path,
                } => {
                    let new_path_buf = new_path.as_path().to_path_buf();
                    info!(
                        %document_id,
                        old = %old_path.display(),
                        new = %new_path,
                        "document renamed remotely, renaming locally"
                    );
                    let old_abs = self.config.space_root.join(old_path);
                    let new_abs = new_path.under(&self.config.space_root);
                    rename_doc(
                        &mut self.state.file_identity,
                        old_path,
                        &old_abs,
                        &new_path_buf,
                        &new_abs,
                        suppress_tx,
                    )?;

                    self.move_identity(old_path, new_path_buf.clone(), document_id);

                    // Sync at the new path.
                    self.sync_local_document(document_id, &new_path_buf, sync_cmd_tx)?;
                }
                StartupAction::DeleteLocal { document_id, path } => {
                    info!(path = %path.display(), %document_id, "document unregistered remotely, deleting locally");
                    let abs_path = self.config.space_root.join(path);
                    if abs_path.exists()
                        && let Err(e) = remove_doc(path, &abs_path, suppress_tx)
                    {
                        error!(path = %abs_path.display(), error = %e, "failed to delete file");
                    }
                    self.cleanup_document_state(path);
                    to_unregister.push(document_id.clone());
                }
                StartupAction::CleanupState { document_id, path } => {
                    info!(path = %path.display(), %document_id, "cleaning up stale state entry");
                    self.cleanup_document_state(path);
                    to_unregister.push(document_id.clone());
                }
            }
        }

        for document_id in &to_unregister {
            self.unregister_identity(document_id);
        }

        Ok(())
    }

    /// Wait for the `SpaceDocuments` response, returning UUID → validated path map.
    ///
    /// Validates each relay-supplied path through [`SafeRelayPath`], skipping
    /// documents with invalid paths (traversal, absolute, `.kutl` prefix).
    async fn wait_for_document_list(
        &self,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
    ) -> Result<HashMap<String, SafeRelayPath>> {
        loop {
            match sync_event_rx.recv().await {
                Some(SyncEvent::SpaceDocuments { documents, .. }) => {
                    let mut map = HashMap::with_capacity(documents.len());
                    for (doc_id, path) in documents {
                        match SafeRelayPath::new(&path) {
                            Ok(safe) => {
                                map.insert(doc_id, safe);
                            }
                            Err(e) => {
                                error!(%doc_id, "skipping document with invalid path: {e}");
                            }
                        }
                    }
                    return Ok(map);
                }
                Some(SyncEvent::Error(msg)) => {
                    anyhow::bail!("relay rejected document discovery: {msg}");
                }
                Some(SyncEvent::Disconnected) | None => {
                    anyhow::bail!("disconnected during document discovery");
                }
                Some(other) => {
                    warn!(?other, "unexpected event during document discovery");
                }
            }
        }
    }

    /// Register a local document with the relay, subscribe, and push CRDT ops.
    ///
    /// Sends all ops since the beginning — the relay deduplicates anything
    /// it already has. This ensures the relay has the full CRDT state for
    /// forwarding to other subscribers.
    fn sync_local_document(
        &mut self,
        document_id: &str,
        path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) -> Result<()> {
        self.register_and_subscribe(
            sync_cmd_tx,
            document_id,
            &rel_path_to_string(path),
            "startup sync",
        )?;

        let pending = self.state.get_doc(document_id).and_then(|doc| {
            (!doc.local_version().is_empty()).then(|| (encode_delta(doc, &[]), doc.local_version()))
        });
        if let Some(((ops, metadata), version)) = pending {
            self.send_cmd(
                sync_cmd_tx,
                SyncCommand::SendOps {
                    document_id: document_id.to_string(),
                    ops,
                    metadata,
                    content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                    content_hash: Vec::new(),
                },
            )?;
            self.state.last_synced.insert(path.to_owned(), version);
        }

        Ok(())
    }

    /// Walk the space directory and process all files. For files already
    /// tracked by reconciliation, diffs content against the CRDT. For new
    /// files, registers and subscribes.
    pub(super) fn initial_file_scan(
        &mut self,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) {
        let space_root = self.config.space_root.clone();

        let mut count = 0u32;

        // Collect paths first to avoid borrow conflicts with self.
        let paths: Vec<PathBuf> = walkdir::WalkDir::new(&space_root)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.file_type().is_file())
            .filter_map(|e| {
                let rel = e.path().strip_prefix(&space_root).ok()?.to_path_buf();
                if watcher::should_ignore(&rel) {
                    return None;
                }
                Some(rel)
            })
            .collect();

        for rel_path in paths {
            // Skip files that already have a CRDT document loaded AND whose
            // content matches the file on disk. Process files with offline
            // edits (content differs from CRDT).
            // Chunk-wise equality against the rope — no per-file O(doc-size)
            // String materialization during the scan (perf §3.3).
            let doc_id = self.uuid_at(&rel_path);
            if let Some(doc) = doc_id.as_deref().and_then(|id| self.state.get_doc(id)) {
                let abs_path = self.state.file_path(&rel_path);
                match std::fs::read_to_string(&abs_path) {
                    Ok(file_content) if doc.content_eq(&file_content) => continue,
                    _ => {} // File differs or can't be read — process it
                }
            }

            let event = FileEvent::Modified {
                rel_path: rel_path.clone(),
            };
            if let Err(e) = self.handle_file_event(event, sync_cmd_tx, suppress_tx) {
                error!(path = %rel_path.display(), error = %e, "initial scan: failed to process file");
            } else {
                count += 1;
            }
        }

        if count > 0 {
            info!(count, "initial scan: processed existing files");
        }
    }
}
