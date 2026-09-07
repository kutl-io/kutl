//! Startup reconciliation for [`SpaceWorker`]: the session-start sequence that
//! reconciles local state against the relay's authoritative document list —
//! the offline-rename pre-pass, the reconcile truth-table execution, and the
//! initial file scan.
//!
//! Ordering contract: the CALLER (`run_session`, daemon.rs) runs
//! `startup_reconciliation` BEFORE the watcher starts and `initial_file_scan`
//! AFTER it (the watcher must be live before the scan so mid-scan creates are
//! not silently invisible). The offline-rename pre-pass mutates disk with
//! bare fs calls and depends on no watcher running; the truth-table actions
//! register their echo suppressions normally, and the watcher consumes them
//! once it starts.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::SafeRelayPath;
use crate::client::{SyncCommand, SyncEvent};
use crate::core::rel_path_to_string;
use crate::reconcile::{self, ReconcileInputs, StartupAction};
use crate::watcher::{self, FileEvent, Suppression};

use super::SpaceWorker;
use super::effects::{remove_doc, rename_doc};

impl SpaceWorker {
    /// Gather inputs, run reconciliation, and execute the resulting actions.
    ///
    /// A single flow:
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

    /// Subscribe to a document the relay lists. A path held by an
    /// offline-created local file the state never mapped to THIS doc is a
    /// DISTINCT document that collides, so the remote doc is deferred rather
    /// than adopted: the new-file scan claims the local file (a fresh uuid),
    /// the relay arbitrates, and the runtime displacement/drain materializes
    /// both as conflict copies. Adopting here would instead merge the
    /// offline content into the remote doc via `incorporate_pending_edits`.
    fn subscribe_remote_document(
        &mut self,
        document_id: &str,
        path: &SafeRelayPath,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        let rel = path.as_path().to_path_buf();
        let tracked = self.state.uuid_to_path.contains_key(document_id);
        let file_present = self.config.space_root.join(&rel).exists();
        if !tracked && self.uuid_at(&rel).is_none() && file_present {
            debug!(%document_id, path = %path, "deferring remote subscribe: path held by an offline-created file");
            self.state
                .deferred
                .insert(document_id.to_owned(), path.clone());
            return Ok(());
        }
        info!(%document_id, path = %path, "subscribing to remote document");
        if !tracked {
            self.register_identity(path.clone(), document_id.to_owned(), true);
        } else if let Some(local_rel) = self.state.uuid_to_path.get(document_id).cloned()
            && !self.config.space_root.join(&local_rel).exists()
        {
            // A tracked document with no file: its bytes never landed (or the
            // kill came before they did), and the subscription alone would
            // not recreate them — a sidecar already caught up with the relay
            // streams nothing. Write what the local copy holds now, at the
            // path the identity holds for THIS id (the relay's path may be a
            // rename the reconcile has yet to apply, and `restore_from_crdt`
            // resolves the document by path); the subscription brings
            // anything newer.
            match self.restore_from_crdt(&local_rel, suppress_tx) {
                Ok(true) => info!(
                    %document_id, path = %local_rel.display(),
                    "tracked document with no file materialized from its local copy"
                ),
                Ok(false) => {}
                Err(e) => error!(
                    %document_id, path = %local_rel.display(), error = %e,
                    "failed to materialize a tracked document from its local copy"
                ),
            }
        }
        self.send_cmd(
            sync_cmd_tx,
            SyncCommand::Subscribe {
                document_id: document_id.to_owned(),
            },
        )
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
                    self.subscribe_remote_document(document_id, path, sync_cmd_tx, suppress_tx)?;
                }
                StartupAction::SyncLocal { document_id, path } => {
                    self.subscribe_and_push(document_id, path, sync_cmd_tx)?;
                }
                StartupAction::RegisterLocal { document_id, path } => {
                    self.register_local_document(document_id, path, sync_cmd_tx, suppress_tx)?;
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
                        &mut self.state,
                        old_path,
                        &old_abs,
                        &new_path_buf,
                        &new_abs,
                        suppress_tx,
                    )?;

                    self.move_identity(old_path, new_path_buf.clone(), document_id);

                    // Sync at the new path. The doc is relay-listed by
                    // definition of this row — subscribe, never re-register.
                    self.subscribe_and_push(document_id, &new_path_buf, sync_cmd_tx)?;
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
    ///
    /// Every other event is parked in `startup_buffer` for the loop, in
    /// arrival order: the signal stream's first page or stale notice (the
    /// daemon subscribed before listing), or a peer's lifecycle broadcast. A
    /// broadcast parked here may predate the snapshot, because the ack lane
    /// the snapshot rides outruns the ctrl lane; applying it afterwards is
    /// safe because every later change to the same document is also in the
    /// ctrl queue behind it, so the last frame applied is the snapshot's
    /// state or newer.
    async fn wait_for_document_list(
        &mut self,
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
                Some(SyncEvent::Error {
                    message,
                    auth_failed,
                }) => {
                    // An authz rejection during discovery is the same
                    // provisioning problem as a subscribe rejection — surface the
                    // operator remedy rather than a bare message.
                    if auth_failed {
                        self.record_relay_error(&message, true);
                    }
                    anyhow::bail!("relay rejected document discovery: {message}");
                }
                Some(SyncEvent::Disconnected) | None => {
                    anyhow::bail!("disconnected during document discovery");
                }
                Some(other) => {
                    self.startup_buffer.push_back(other);
                }
            }
        }
    }

    /// Register a relay-unknown local document, subscribe, and push CRDT ops.
    ///
    /// ONLY for documents the relay does not already list (`RegisterLocal`):
    /// registration mints a fresh origin stamp and folds it into the placement
    /// lattice, which is correct exactly once — when the document is new to
    /// the cluster. Re-registering an already-listed document re-runs settled
    /// path arbitration with post-restart stamps and diverges the cluster
    /// (see the truth-table note on [`crate::reconcile::StartupAction`]);
    /// those documents go through [`Self::subscribe_and_push`] instead.
    fn register_local_document(
        &mut self,
        document_id: &str,
        path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        // Registers carry INTENDED paths; conflict-copy paths are per-node
        // derivations the relay refuses outright. A displaced-before-confirmed
        // local doc reaches this row keyed by its conflict DISK path (that is
        // all the persisted state maps), so recover the intended path from the
        // derivation and register there — arbitration then displaces it
        // identically on every node while the content still syncs. A
        // lookalike the inverse rejects (or whose embedded id is not this
        // document) is a user-manufactured name in the reserved namespace:
        // leave it local, exactly as the relay would force anyway.
        let disk_rel = rel_path_to_string(path);
        let register_path = if disk_rel.contains(kutl_core::lattice::CONFLICT_INFIX) {
            match kutl_core::lattice::intended_from_conflict_path(&disk_rel) {
                Some((intended, id)) if id.to_string() == document_id => intended,
                _ => {
                    warn!(
                        path = %path.display(),
                        %document_id,
                        "conflict-namespace path without a matching derivation; leaving it unsynced"
                    );
                    return Ok(());
                }
            }
        } else {
            disk_rel
        };
        // The core's one mint (metadata, the two HLC watermarks, the lattice
        // record), applied at the driver edge (birthtime, inode) like a live
        // mint; the Subscribe is explicit for the same reason it is on the
        // live door — a document already at its path emits no placement.
        let stamp = self.stamp(None);
        let effect = crate::core::handle::helpers::register_document_effect(
            &mut self.state,
            document_id,
            Path::new(&register_path),
            stamp,
            "startup sync",
        );
        self.apply_effect(effect, sync_cmd_tx, suppress_tx)?;
        self.send_cmd(
            sync_cmd_tx,
            SyncCommand::Subscribe {
                document_id: document_id.to_owned(),
            },
        )?;
        self.push_pending_ops(document_id, path, sync_cmd_tx)
    }

    /// Subscribe to a relay-listed document and push local CRDT ops, WITHOUT
    /// re-registering it. The relay already holds the entry (its list is the
    /// arbitrated truth this daemon just reconciled against), so the only
    /// startup work is re-attaching the subscription and shipping anything
    /// authored offline.
    fn subscribe_and_push(
        &mut self,
        document_id: &str,
        path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) -> Result<()> {
        self.send_cmd(
            sync_cmd_tx,
            SyncCommand::Subscribe {
                document_id: document_id.to_owned(),
            },
        )?;
        self.push_pending_ops(document_id, path, sync_cmd_tx)
    }

    /// Push the document's full CRDT delta to the relay. Sends all ops since
    /// the beginning — the relay deduplicates anything it already has. This
    /// ensures the relay has the full CRDT state for forwarding to other
    /// subscribers.
    fn push_pending_ops(
        &mut self,
        document_id: &str,
        path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) -> Result<()> {
        let pending = self.state.get_doc(document_id).and_then(|doc| {
            (!doc.local_version().is_empty()).then(|| (doc.delta_since(&[]), doc.local_version()))
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

    /// The scan's verdict on one file that is tracked, or that carries a
    /// journaled write intent: `true` when nothing more needs doing (the file
    /// matches its CRDT, was restored from it, or was adopted as this
    /// daemon's own interrupted materialization), `false` when the file must
    /// be processed as a modification.
    ///
    /// Two shapes of interrupted materialization are adopted rather than
    /// diffed. A journaled write intent whose bytes are on disk: the write
    /// landed and nothing after it ran. And a tracked path with no engine
    /// whose bytes match the identity's last-written hash: the write landed
    /// and its identity was journaled, but the sidecar save that follows
    /// never ran, so the engine to diff against is absent. Either file is
    /// content the relay already holds; diffing it into an empty engine
    /// would re-insert it, and every replica would converge on the text
    /// twice.
    fn scan_settles_file(
        &mut self,
        rel_path: &Path,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> bool {
        // Only a tracked path can be settled; an untracked file is new.
        let Some(doc_id) = self.uuid_at(rel_path) else {
            return false;
        };
        // A write intent journaled before a rename that may or may not have
        // landed (see `pending_writes`); the map is empty on every clean start.
        let intent = if self.state.state.pending_writes.is_empty() {
            None
        } else {
            self.state
                .state
                .pending_writes
                .get(&rel_path_to_string(rel_path))
                .cloned()
        };
        let doc = self.state.get_doc(&doc_id);
        let has_doc = doc.is_some();
        if !has_doc && intent.is_none() && self.state.awaiting_content.contains(&doc_id) {
            // A document whose sidecar was retired at this start is awaiting
            // the relay's refill: the file is settled by the merge that
            // refills, which diffs it against real content, and must not
            // enter the modify door as a brand-new document. Independent of
            // the last-written hash: a retired document keeps its identity,
            // hash included.
            info!(
                path = %rel_path.display(),
                "initial scan: document sidecar retired; the file settles against the relay's refill"
            );
            return true;
        }
        // With no engine and no intent, the identity's last-written hash is
        // the only record of what this daemon put at the path.
        let landed = if has_doc || intent.is_some() {
            None
        } else {
            self.state
                .file_identity
                .get(rel_path)
                .and_then(|fi| fi.last_written_hash.clone())
        };
        if !has_doc && intent.is_none() && landed.is_none() {
            return false;
        }
        // A blob is never adopted through the text branch below: adopting
        // would mint a text engine for bytes the last-writer-wins door owns,
        // and the read would pull the whole file into memory only to discard
        // the UTF-8 error before that door reads the same bytes again.
        if !has_doc && intent.is_none() && self.state.blob_state.get(rel_path).is_some() {
            return false;
        }
        let abs_path = self.state.file_path(rel_path);
        let file_content = std::fs::read_to_string(&abs_path);
        // Chunk-wise equality against the rope: no per-file O(doc-size) String
        // materialization for the common converged file.
        if let (Some(doc), Ok(text)) = (doc, &file_content)
            && doc.content_eq(text)
        {
            return true;
        }
        // Hash only files that FAILED the cheap content_eq above.
        let disk_hash = file_content
            .as_ref()
            .ok()
            .map(|text| crate::blob_state::sha256_hex(text.as_bytes()));
        // The bytes on disk are EXACTLY the ones this daemon's own funnel
        // announced it was about to place: the write landed, whatever followed
        // it did not, so the CRDT, loaded or not, is behind the file. Nothing
        // local to incorporate and nothing to restore: adopt the file as the
        // document's (inode + hash) and let the relay's ops bring the CRDT up;
        // the landing write then replaces the file with the same bytes.
        if let Some(hash) = intent.filter(|h| disk_hash.as_deref() == Some(h.as_str())) {
            self.adopt_own_materialization(rel_path, &abs_path, &doc_id, hash, "write intent");
            return true;
        }
        if let Some(hash) = landed.filter(|h| disk_hash.as_deref() == Some(h.as_str())) {
            self.adopt_own_materialization(rel_path, &abs_path, &doc_id, hash, "landed write");
            return true;
        }
        if !has_doc {
            return false;
        }
        let is_unchanged_since_we_looked = disk_hash.as_deref().is_some_and(|h| {
            self.state
                .file_identity
                .get(rel_path)
                .and_then(|fi| fi.last_written_hash.as_deref())
                == Some(h)
        });
        // Interrupted-materialization guard (the crash-window race): the
        // on-disk bytes are EXACTLY the last content this daemon wrote or
        // observed (journaled hash), so the difference cannot be a user edit;
        // it is content that merged into the CRDT but never finished
        // materializing before a kill. Disk is NOT truth here: restore the
        // file from the CRDT instead of incorporating, which would re-insert
        // ops the CRDT already carries (content duplicated cluster-wide) or
        // read the placed-but-still-empty file as a truncation (content
        // erased cluster-wide).
        if is_unchanged_since_we_looked {
            match self.restore_from_crdt(rel_path, suppress_tx) {
                Ok(true) => {
                    info!(
                        path = %rel_path.display(),
                        "initial scan: on-disk content is this daemon's own interrupted \
                         write; restored from the CRDT"
                    );
                    return true;
                }
                // An empty CRDT has nothing to restore. The modification
                // handler decides what the bytes are: a loaded-but-empty CRDT
                // is behind known bytes and waits for the relay.
                Ok(false) => {}
                Err(e) => {
                    error!(
                        path = %rel_path.display(), error = %e,
                        "initial scan: failed to restore an interrupted materialization"
                    );
                    return true;
                }
            }
        }
        // File differs or can't be read: process it. Logged because this is
        // the door that turns disk bytes into new CRDT ops for an
        // ALREADY-TRACKED doc: if the difference is not a genuine offline edit
        // (e.g. the CRDT is behind because remote catch-up for this doc is
        // still in flight), the diff re-inserts content the incoming ops also
        // carry, and every replica converges on the duplicated result.
        debug!(
            path = %rel_path.display(),
            "initial scan: tracked file differs from CRDT; applying as offline edit"
        );
        false
    }

    /// Adopt the file at `rel_path` as this daemon's own interrupted
    /// materialization (`how` names the evidence: a journaled write intent
    /// whose bytes landed, or a landed write whose sidecar never saved): the
    /// inode and hash are recorded, and an empty document where none was
    /// loaded keeps every door reading known bytes ahead of a loaded CRDT
    /// until the relay's catch-up fills it.
    fn adopt_own_materialization(
        &mut self,
        rel_path: &Path,
        abs_path: &Path,
        doc_id: &str,
        hash: String,
        how: &str,
    ) {
        if let Some(inode) = crate::inode::get_inode(abs_path) {
            self.state.identity_set_inode(rel_path, Some(inode));
        }
        self.state.identity_set_written_hash(rel_path, hash);
        self.state.load_or_create_doc(doc_id);
        info!(
            path = %rel_path.display(),
            how,
            "initial scan: on-disk content is this daemon's own interrupted \
             materialization; adopted, content streams from the relay"
        );
    }

    /// Walk the space directory and process all files. For files already
    /// tracked by reconciliation, diffs content against the CRDT. For new
    /// files, registers and subscribes.
    pub(super) fn initial_file_scan(
        &mut self,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
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

        // The scan is a burst by definition: every new file the core mints
        // would otherwise run a full placement pass, O(docs) each, the O(N²)
        // the live loop's intake gate exists to prevent. Mark the intake
        // backlogged for the scan's duration so each mint arms the placement
        // debt instead; the loop's first metrics tick pays it in one pass.
        let was_backlogged = std::mem::replace(&mut self.state.intake_backlogged, true);
        for rel_path in paths {
            // Keep the bounded `sync_event` channel drained as we scan so the WS
            // read loop never blocks on a full channel mid-startup and stops
            // answering keepalive pings (see `startup_buffer`).
            self.drain_startup_sync_events(sync_event_rx);

            // Settle tracked files (and files carrying a write intent) against
            // their CRDT first; only what remains is processed as a modification.
            if self.scan_settles_file(&rel_path, suppress_tx) {
                continue;
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
        self.state.intake_backlogged = was_backlogged;
        // Every intent had its one chance to match a file above; what is left
        // announced bytes that never landed and must not outlive this scan.
        self.state.state.pending_writes.clear();

        // Final drain: catch anything the relay flushed during the last file.
        self.drain_startup_sync_events(sync_event_rx);

        if count > 0 {
            info!(count, "initial scan: processed existing files");
        }
    }

    /// Move any sync events currently queued on the bounded `sync_event` channel
    /// into [`Self::startup_buffer`] (non-blocking). Called during
    /// [`Self::initial_file_scan`]; the event loop processes the buffer before
    /// live events. See `startup_buffer` for why this is load-bearing.
    fn drain_startup_sync_events(&mut self, sync_event_rx: &mut mpsc::Receiver<SyncEvent>) {
        while let Ok(event) = sync_event_rx.try_recv() {
            self.startup_buffer.push_back(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::tests::test_worker;

    /// Drive the startup scan with fresh channels; the returned receiver
    /// holds whatever the scan sent to the relay.
    fn run_scan(worker: &mut SpaceWorker) -> mpsc::UnboundedReceiver<SyncCommand> {
        let (sync_cmd_tx, sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();
        let (_se_tx, mut se_rx) = mpsc::channel::<SyncEvent>(16);
        worker.initial_file_scan(&sync_cmd_tx, &suppress_tx, &mut se_rx);
        sync_cmd_rx
    }

    /// The crash artifact behind the doubled-file race: the funnel's intent
    /// reached the journal, the rename landed, and the kill came before the
    /// sidecar save, so no CRDT is loaded for the file. The scan must NOT
    /// diff the file into a fresh document (the relay already holds these
    /// ops); it adopts the file and leaves the content to catch-up.
    #[test]
    fn test_scan_adopts_own_interrupted_materialization_without_sidecar() {
        const DOC: &str = "55555555-5555-4555-8555-555555555555";
        const CONTENT: &[u8] = b"remote content\n";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        // Registered before the file existed (inode unknown), then the write
        // landed and the kill came before the sidecar save.
        worker.register_identity(SafeRelayPath::new("doc.md").unwrap(), DOC.to_owned(), true);
        std::fs::write(dir.path().join("doc.md"), CONTENT).unwrap();
        worker
            .state
            .state
            .pending_writes
            .insert("doc.md".to_owned(), crate::blob_state::sha256_hex(CONTENT));

        let mut sync_cmd_rx = run_scan(&mut worker);

        assert_eq!(
            worker
                .state
                .get_doc(DOC)
                .map(kutl_core::Document::content)
                .as_deref(),
            Some(""),
            "the file is not diffed in: the document exists, empty, for catch-up to fill"
        );
        assert!(
            sync_cmd_rx.try_recv().is_err(),
            "nothing is sent to the relay for an adopted file"
        );
        let identity = &worker.state.file_identity[Path::new("doc.md")];
        assert!(
            identity.inode.is_some(),
            "the adopted file's inode is recorded"
        );
        assert_eq!(
            identity.last_written_hash.as_deref(),
            Some(crate::blob_state::sha256_hex(CONTENT).as_str()),
            "the adopted bytes become the last-written hash"
        );
        assert!(
            worker.state.state.pending_writes.is_empty(),
            "the scan consumes every intent"
        );
        assert_eq!(
            std::fs::read(dir.path().join("doc.md")).unwrap(),
            CONTENT,
            "the file is left in place for the landing write to replace"
        );
    }

    /// An intent whose bytes are not what is on disk proves nothing about the
    /// file: it incorporates as an offline edit like any other differing
    /// file, and the stale intent does not outlive the scan.
    #[test]
    fn test_scan_ignores_an_intent_the_disk_does_not_match() {
        const DOC: &str = "66666666-6666-4666-8666-666666666666";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        std::fs::write(dir.path().join("doc.md"), "user edit\n").unwrap();
        worker.register_identity(SafeRelayPath::new("doc.md").unwrap(), DOC.to_owned(), true);
        worker.state.state.pending_writes.insert(
            "doc.md".to_owned(),
            crate::blob_state::sha256_hex(b"remote content\n"),
        );

        let mut sync_cmd_rx = run_scan(&mut worker);

        assert_eq!(
            worker
                .state
                .get_doc(DOC)
                .map(kutl_core::Document::content)
                .as_deref(),
            Some("user edit\n"),
            "the file incorporates as an offline edit into the tracked document"
        );
        assert!(
            sync_cmd_rx.try_recv().is_ok(),
            "and the edit reaches the relay"
        );
        assert!(
            worker.state.state.pending_writes.is_empty(),
            "a stale intent is dropped by the scan"
        );
    }

    /// An intent match adopts even when a CRDT is loaded: an intent that
    /// survived replay means the loop tail never ran after the rename, and the
    /// sidecar save runs before that tail, so a loaded CRDT is by construction
    /// BEHIND the landed bytes. Restoring from it would regress the file (and
    /// drop a local edit the merge had folded in); adopting leaves the newer
    /// bytes in place for catch-up to confirm.
    #[test]
    fn test_scan_adopts_intent_bytes_over_a_crdt_behind_them() {
        const DOC: &str = "77777777-7777-4777-8777-777777777777";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        {
            let doc = worker.state.load_or_create_doc(DOC);
            let agent = doc.register_agent("peer").unwrap();
            doc.edit(agent, "peer", "seed", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(0, "older content\n")
            })
            .unwrap();
        }
        worker.register_identity(SafeRelayPath::new("doc.md").unwrap(), DOC.to_owned(), true);
        std::fs::write(dir.path().join("doc.md"), "newer content\n").unwrap();
        worker.state.state.pending_writes.insert(
            "doc.md".to_owned(),
            crate::blob_state::sha256_hex(b"newer content\n"),
        );

        let mut sync_cmd_rx = run_scan(&mut worker);

        assert_eq!(
            std::fs::read_to_string(dir.path().join("doc.md")).unwrap(),
            "newer content\n",
            "the landed bytes stay; nothing is restored over them"
        );
        assert_eq!(
            worker.state.get_doc(DOC).unwrap().content(),
            "older content\n",
            "the CRDT is left for catch-up, not diffed"
        );
        assert!(sync_cmd_rx.try_recv().is_err(), "nothing is shipped");
        assert_eq!(
            worker.state.file_identity[Path::new("doc.md")]
                .last_written_hash
                .as_deref(),
            Some(crate::blob_state::sha256_hex(b"newer content\n").as_str())
        );
        assert!(worker.state.state.pending_writes.is_empty());
    }

    /// The watcher door of the same race: a modified event for a tracked
    /// file whose bytes are this daemon's own last write, while the CRDT
    /// loaded for it is empty (the adopted state), must not diff the file in
    /// or mint a registration; the content is on the relay.
    #[test]
    fn test_modified_event_for_known_bytes_incorporates_nothing() {
        const DOC: &str = "88888888-8888-4888-8888-888888888888";
        const CONTENT: &[u8] = b"remote content\n";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        std::fs::write(dir.path().join("doc.md"), CONTENT).unwrap();
        worker.register_identity(SafeRelayPath::new("doc.md").unwrap(), DOC.to_owned(), true);
        worker.state.load_or_create_doc(DOC);
        worker
            .state
            .identity_set_written_hash(Path::new("doc.md"), crate::blob_state::sha256_hex(CONTENT));

        let (sync_cmd_tx, mut sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();
        worker
            .handle_file_event(
                FileEvent::Modified {
                    rel_path: PathBuf::from("doc.md"),
                },
                &sync_cmd_tx,
                &suppress_tx,
            )
            .unwrap();

        assert_eq!(
            worker
                .state
                .get_doc(DOC)
                .map(kutl_core::Document::content)
                .as_deref(),
            Some(""),
            "known bytes are not diffed into the empty document"
        );
        assert!(
            sync_cmd_rx.try_recv().is_err(),
            "nothing is registered or shipped for known bytes"
        );
    }

    /// The interrupted-materialization guard: a tracked file whose on-disk
    /// bytes are EXACTLY the daemon's own last funnel write (here the empty
    /// placeholder) while the CRDT holds newer content is RESTORED from the
    /// CRDT at startup — never incorporated, which would broadcast
    /// delete-everything and erase the document cluster-wide.
    #[test]
    fn test_scan_restores_interrupted_materialization() {
        const DOC: &str = "44444444-4444-4444-8444-444444444444";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        {
            let doc = worker.state.load_or_create_doc(DOC);
            let agent = doc.register_agent("peer").unwrap();
            doc.edit(agent, "peer", "seed", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(0, "remote content\n")
            })
            .unwrap();
        }
        // The crash artifact: registered identity, an EMPTY placed file, and
        // the journaled hash recording the empty bytes as OUR OWN write.
        std::fs::write(dir.path().join("doc.md"), "").unwrap();
        worker.register_identity(SafeRelayPath::new("doc.md").unwrap(), DOC.to_owned(), true);
        worker
            .state
            .identity_set_written_hash(Path::new("doc.md"), crate::blob_state::sha256_hex(b""));

        let _sync_cmd_rx = run_scan(&mut worker);

        assert_eq!(
            std::fs::read_to_string(dir.path().join("doc.md")).unwrap(),
            "remote content\n",
            "the interrupted write is restored from the CRDT"
        );
        assert_eq!(
            worker.state.get_doc(DOC).unwrap().content(),
            "remote content\n",
            "the CRDT is untouched — no delete-everything incorporation"
        );
    }

    /// The guard must NOT fire for a genuine offline edit: on-disk bytes that
    /// differ from the last funnel write incorporate as the user's edit,
    /// exactly as before the guard existed.
    #[test]
    fn test_scan_incorporates_offline_edit_despite_recorded_hash() {
        const DOC: &str = "55555555-5555-4555-8555-555555555555";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        {
            let doc = worker.state.load_or_create_doc(DOC);
            let agent = doc.register_agent("peer").unwrap();
            doc.edit(agent, "peer", "seed", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(0, "synced content\n")
            })
            .unwrap();
        }
        // The user edited the file while the daemon was down: bytes match
        // neither the CRDT nor the recorded last-written hash.
        std::fs::write(dir.path().join("doc.md"), "user offline edit\n").unwrap();
        worker.register_identity(SafeRelayPath::new("doc.md").unwrap(), DOC.to_owned(), true);
        worker.state.identity_set_written_hash(
            Path::new("doc.md"),
            crate::blob_state::sha256_hex(b"synced content\n"),
        );

        let _sync_cmd_rx = run_scan(&mut worker);

        assert_eq!(
            std::fs::read_to_string(dir.path().join("doc.md")).unwrap(),
            "user offline edit\n",
            "the user's file is left as the user wrote it"
        );
        assert_eq!(
            worker.state.get_doc(DOC).unwrap().content(),
            "user offline edit\n",
            "the offline edit incorporates into the CRDT as before"
        );
    }

    /// The revert hole: the recorded hash tracks the last content the daemon
    /// KNEW (wrote or observed), not merely the last funnel write. A live
    /// local edit is observed (hash advances with it); the user then
    /// offline-reverts the file to EXACTLY the old funnel-written bytes.
    /// With a hash frozen at the funnel write, the scan would read the
    /// revert as "our own interrupted write" and restore the CRDT's newer
    /// content — silently undoing the user's revert. With observation
    /// updates, the mismatch routes it to incorporate-as-edit.
    #[test]
    fn test_scan_incorporates_offline_revert_to_last_written_bytes() {
        const DOC: &str = "66666666-6666-4666-8666-666666666666";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        {
            let doc = worker.state.load_or_create_doc(DOC);
            let agent = doc.register_agent("peer").unwrap();
            doc.edit(agent, "peer", "seed", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(0, "v1\n")
            })
            .unwrap();
        }
        worker.register_identity(SafeRelayPath::new("doc.md").unwrap(), DOC.to_owned(), true);
        // The funnel wrote "v1" (recorded hash), then the user edited to "v2"
        // and the LIVE daemon incorporated it through the real handler — the
        // observation must advance the recorded hash past the funnel write.
        worker
            .state
            .identity_set_written_hash(Path::new("doc.md"), crate::blob_state::sha256_hex(b"v1\n"));
        std::fs::write(dir.path().join("doc.md"), "v2\n").unwrap();
        let _effects = crate::core::DaemonCore::handle(
            &mut worker.state,
            crate::core::Event::FileModified {
                rel: PathBuf::from("doc.md"),
                content: Some(b"v2\n".to_vec()),
                stamp: crate::core::EventStamp {
                    wall_ms: 1_000,
                    origin_hlc: None,
                },
            },
        );
        assert_eq!(
            worker
                .state
                .file_identity
                .get(Path::new("doc.md"))
                .and_then(|fi| fi.last_written_hash.as_deref()),
            Some(crate::blob_state::sha256_hex(b"v2\n").as_str()),
            "incorporating an observed edit advances the recorded hash"
        );

        // Crash; while the daemon is down the user reverts the file to the
        // exact old funnel bytes.
        std::fs::write(dir.path().join("doc.md"), "v1\n").unwrap();
        let _sync_cmd_rx = run_scan(&mut worker);

        assert_eq!(
            std::fs::read_to_string(dir.path().join("doc.md")).unwrap(),
            "v1\n",
            "the user's revert is left as the user wrote it"
        );
        assert_eq!(
            worker.state.get_doc(DOC).unwrap().content(),
            "v1\n",
            "the revert incorporates as an edit — never undone by a restore"
        );
    }

    /// A relay-listed document this daemon tracks whose file is absent
    /// re-subscribes, and when the local CRDT already holds its content the
    /// file is written from that copy at once: a sidecar caught up with the
    /// relay streams nothing, so nothing else would ever recreate the file.
    #[test]
    fn test_subscribe_remote_of_a_tracked_doc_with_no_file_materializes_the_local_copy() {
        const DOC: &str = "66666666-6666-4666-8666-666666666666";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let path = crate::SafeRelayPath::new("doc.md").unwrap();
        worker.register_identity(path.clone(), DOC.to_owned(), true);
        {
            let doc = worker.state.load_or_create_doc(DOC);
            let agent = doc.register_agent("t").unwrap();
            doc.edit(
                agent,
                "did:t",
                "seed",
                kutl_core::Boundary::Explicit,
                |ctx| ctx.insert(0, "local copy\n"),
            )
            .unwrap();
        }
        let (sync_cmd_tx, mut sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();
        worker
            .execute_reconcile_actions(
                &[crate::reconcile::StartupAction::SubscribeRemote {
                    document_id: DOC.to_owned(),
                    path,
                }],
                &sync_cmd_tx,
                &suppress_tx,
            )
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("doc.md")).unwrap(),
            "local copy\n",
            "the local copy is on disk before any op streams"
        );
        let mut subscribed = false;
        while let Ok(cmd) = sync_cmd_rx.try_recv() {
            if matches!(cmd, SyncCommand::Subscribe { ref document_id } if document_id == DOC) {
                subscribed = true;
            }
        }
        assert!(subscribed, "the subscription still brings anything newer");
    }

    /// A tracked file whose bytes match the identity's last-written hash but
    /// whose engine never reached disk (the kill between the write's
    /// journaled identity and the sidecar save) is adopted as this daemon's
    /// own materialization: an empty engine is loaded so the door that
    /// follows reads known bytes ahead of the CRDT, and the file never
    /// re-enters as a new local document.
    #[test]
    fn test_scan_adopts_a_landed_write_whose_sidecar_never_saved() {
        const DOC: &str = "77777777-7777-4777-8777-777777777777";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let rel = Path::new("landed.md");
        let content = "burst file 41\n";
        std::fs::write(dir.path().join(rel), content).unwrap();
        worker.register_identity(
            crate::SafeRelayPath::new("landed.md").unwrap(),
            DOC.to_owned(),
            true,
        );
        worker
            .state
            .identity_set_written_hash(rel, crate::blob_state::sha256_hex(content.as_bytes()));
        assert!(
            worker.state.get_doc(DOC).is_none(),
            "premise: no engine on disk"
        );
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();

        assert!(
            worker.scan_settles_file(rel, &suppress_tx),
            "the landed write is adopted, not diffed as a new document"
        );
        assert!(
            worker.state.get_doc(DOC).is_some(),
            "an empty engine was loaded"
        );
        assert!(
            worker.state.known_bytes_ahead_of_crdt(rel, content),
            "every door reads the bytes as known and the CRDT as behind"
        );
    }

    /// The event loop is the only drainer of the bounded `sync_event` channel and
    /// it starts AFTER the scan, so the scan must drain any events the relay has
    /// already flushed (e.g. a large re-subscribe burst) into the startup buffer.
    /// Otherwise the WS read loop blocks on a full channel mid-startup and stops
    /// answering pings until a keepalive reaper closes the connection. Regression
    /// for the startup stall under a many-document re-subscribe.
    #[test]
    fn test_initial_file_scan_drains_sync_events_to_startup_buffer() {
        // A relay flush queued on the bounded channel before the loop runs.
        const FLUSHED: usize = 5;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        // A couple of files so the scan loop iterates.
        std::fs::write(root.join("a.md"), "alpha").unwrap();
        std::fs::write(root.join("b.md"), "bravo").unwrap();

        let mut worker = test_worker(root);
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel();
        let (event_tx, mut event_rx) = mpsc::channel::<SyncEvent>(8);

        for _ in 0..FLUSHED {
            event_tx
                .try_send(SyncEvent::Connected {
                    relay_did: String::new(),
                    accepts_reseed: false,
                })
                .unwrap();
        }

        worker.initial_file_scan(&cmd_tx, &suppress_tx, &mut event_rx);

        assert_eq!(
            worker.startup_buffer.len(),
            FLUSHED,
            "scan must drain queued sync events into the startup buffer"
        );
        assert!(
            event_rx.try_recv().is_err(),
            "the bounded sync_event channel must be left empty by the scan"
        );
    }

    /// Discovery parks everything that is not its reply: the daemon
    /// subscribes to the space's signals before listing, so the stream's
    /// first answer (or a peer's lifecycle broadcast) can arrive during the
    /// wait, and dropping it would lose the catch-up page or a registration.
    #[tokio::test]
    async fn test_wait_for_document_list_parks_other_events_for_the_loop() {
        let tmp = tempfile::tempdir().unwrap();
        let mut worker = test_worker(tmp.path().canonicalize().unwrap());
        let (event_tx, mut event_rx) = mpsc::channel::<SyncEvent>(8);

        event_tx
            .try_send(SyncEvent::StaleSignalStream {
                space_id: worker.config.space_id.clone(),
                cause: kutl_proto::sync::StaleStreamReason::PausedLaneFull,
                reason: "signal broadcast did not fit the data lane; re-subscribe".into(),
            })
            .unwrap();
        event_tx
            .try_send(SyncEvent::SpaceDocuments {
                space_id: worker.config.space_id.clone(),
                documents: vec![("00000000-0000-0000-0000-000000000001".into(), "a.md".into())],
            })
            .unwrap();

        let listed = worker.wait_for_document_list(&mut event_rx).await.unwrap();
        assert_eq!(listed.len(), 1, "the reply is consumed");
        assert!(
            matches!(
                worker.startup_buffer.front(),
                Some(SyncEvent::StaleSignalStream { .. })
            ),
            "the event that arrived first is parked for the loop, in order"
        );
        assert_eq!(worker.startup_buffer.len(), 1);
    }

    /// The scan takes the core's mint rules, one of which the imperative
    /// path lacked: a `.kutl-conflict-` path is a materialization artifact,
    /// never an authored file, and the relay refuses to register the
    /// namespace, so the scan leaves it untracked instead of minting a
    /// register the relay would reject.
    #[test]
    fn test_scan_does_not_mint_a_conflict_copy_path() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let conflict = format!("note{}abcd.md", kutl_core::lattice::CONFLICT_INFIX);
        std::fs::write(root.join(&conflict), "artifact").unwrap();
        std::fs::write(root.join("note.md"), "authored").unwrap();

        let mut worker = test_worker(root);
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel();
        let (_event_tx, mut event_rx) = mpsc::channel::<SyncEvent>(8);
        worker.initial_file_scan(&cmd_tx, &suppress_tx, &mut event_rx);

        let mut registered = Vec::new();
        while let Ok(cmd) = cmd_rx.try_recv() {
            if let SyncCommand::RegisterDocument { path, .. } = cmd {
                registered.push(path);
            }
        }
        assert_eq!(
            registered,
            vec!["note.md".to_owned()],
            "only the authored file is minted"
        );
        assert!(
            !worker
                .state
                .file_identity
                .contains_key(std::path::Path::new(&conflict)),
            "the conflict-copy path stays untracked"
        );
    }

    /// New files found by the scan mint through the core, whose mint ends in
    /// a placement pass gated on the intake. The scan marks the intake
    /// backlogged so N mints arm one placement debt instead of running N
    /// passes, and restores the flag for the loop that pays it.
    #[test]
    fn test_scan_arms_one_placement_pass_for_its_mints() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        for name in ["a.md", "b.md", "c.md"] {
            std::fs::write(root.join(name), name).unwrap();
        }

        let mut worker = test_worker(root);
        assert!(!worker.state.intake_backlogged);
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel();
        let (_event_tx, mut event_rx) = mpsc::channel::<SyncEvent>(8);
        worker.initial_file_scan(&cmd_tx, &suppress_tx, &mut event_rx);

        assert_eq!(worker.state.file_identity.len(), 3, "every file was minted");
        assert!(
            worker.state.placement_dirty,
            "the mints armed the placement debt for the loop's first pass"
        );
        assert!(
            !worker.state.intake_backlogged,
            "the scan restores the intake flag it set"
        );
    }

    /// The retired-sidecar guard is independent of the identity's recorded
    /// hash: a retired document keeps its identity, hash included, and edited
    /// bytes on disk still settle against the relay's refill rather than
    /// entering the modify door as a new document.
    #[test]
    fn test_scan_settles_a_retired_document_whose_hash_is_recorded() {
        const DOC: &str = "77777777-7777-4777-8777-777777777777";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let rel = Path::new("retired.md");
        std::fs::write(
            dir.path().join(rel),
            "edited while the sidecar was unreadable\n",
        )
        .unwrap();
        worker.register_identity(
            crate::SafeRelayPath::new("retired.md").unwrap(),
            DOC.to_owned(),
            true,
        );
        worker.state.identity_set_written_hash(
            rel,
            crate::blob_state::sha256_hex(b"what the daemon last wrote\n"),
        );
        worker.state.awaiting_content.insert(DOC.to_owned());
        assert!(
            worker.state.get_doc(DOC).is_none(),
            "premise: the engine was retired"
        );
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();

        assert!(
            worker.scan_settles_file(rel, &suppress_tx),
            "the file settles against the refill, not as a new document"
        );
        assert!(
            worker.state.get_doc(DOC).is_none(),
            "no engine is created ahead of the refill"
        );
        assert!(
            worker.state.awaiting_content.contains(DOC),
            "the mark stays until the refill merge"
        );
    }

    /// A tracked document with no file is materialized at the path the
    /// identity holds for its id, not at the relay's path for it: the two
    /// differ when a remote rename has yet to be applied, and the local copy
    /// resolves by path.
    #[test]
    fn test_subscribe_remote_materializes_at_the_identity_path() {
        const DOC: &str = "88888888-8888-4888-8888-888888888888";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        worker.register_identity(
            crate::SafeRelayPath::new("old.md").unwrap(),
            DOC.to_owned(),
            true,
        );
        {
            let doc = worker.state.load_or_create_doc(DOC);
            let agent = doc.register_agent("t").unwrap();
            doc.edit(
                agent,
                "did:t",
                "seed",
                kutl_core::Boundary::Explicit,
                |ctx| ctx.insert(0, "local copy\n"),
            )
            .unwrap();
        }
        let (sync_cmd_tx, _sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();
        worker
            .execute_reconcile_actions(
                &[crate::reconcile::StartupAction::SubscribeRemote {
                    document_id: DOC.to_owned(),
                    path: crate::SafeRelayPath::new("new.md").unwrap(),
                }],
                &sync_cmd_tx,
                &suppress_tx,
            )
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("old.md")).unwrap(),
            "local copy\n",
            "the local copy lands at the identity's path"
        );
        assert!(
            !dir.path().join("new.md").exists(),
            "nothing is written at the relay's path ahead of the reconcile"
        );
    }
}
