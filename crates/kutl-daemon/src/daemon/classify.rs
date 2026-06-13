//! Sans-IO ingress classification for [`SpaceWorker`]: the edge that turns
//! watcher [`FileEvent`]s and relay [`SyncEvent`]s into [`LoopInput`]s for the
//! pure core — the edge bytes read, the blob size cap, the live-inode rename
//! candidacy checks, and the startup `DiskShadow` seed.

use std::path::{Path, PathBuf};

use kutl_core::Hlc;
use kutl_proto::protocol::ABSOLUTE_BLOB_MAX;
use tracing::{debug, error};

use crate::client::SyncEvent;
use crate::core::{Event, EventStamp};
use crate::watcher::FileEvent;

use super::SpaceWorker;
use super::identity::{find_rename_source, refresh_inode};
use super::session::LoopInput;

impl SpaceWorker {
    /// Seed the in-memory [`DiskShadow`] from the tracked identities + live disk,
    /// just before the event loop starts.
    ///
    /// The pure core answers "is this file on disk?" / "what inode does it carry?"
    /// from the shadow, NOT a live `exists()`/stat (the imperative path's
    /// `abs_path.exists()`, `daemon.rs:2721`). The shadow is otherwise only grown
    /// on shell-ACK of a write/rename this daemon performs, so without this seed a
    /// freshly-started daemon believes every tracked file is ABSENT — and the
    /// empty-ops "ensure the file exists" path would clobber an existing file with
    /// empty content. Reflect the startup truth: for each tracked path present on
    /// disk, record it `Tracked` by its doc with its live inode.
    pub(super) fn seed_shadow(&mut self) {
        let entries: Vec<(PathBuf, String, Option<u64>)> = self
            .state
            .file_identity
            .iter()
            .map(|(rel, id)| (rel.clone(), id.document_uuid.clone(), id.inode))
            .collect();
        for (rel, uuid, recorded_inode) in entries {
            let abs_path = self.config.space_root.join(&rel);
            if !abs_path.exists() {
                continue; // tracked but gone locally (a pending remote materialize)
            }
            // Mark the path occupied. The occupant's `Uuid` keys `shadow_path`; a
            // legacy non-UUID id (pre-UUID document) falls back to `Uuid::nil()` so
            // the PATH-keyed occupant entry — the "is this file on disk?" answer the
            // empty-ops/skip-write checks read — is still set.
            let id = uuid::Uuid::parse_str(&uuid).unwrap_or_else(|_| uuid::Uuid::nil());
            self.state.shadow.set_tracked(&rel, id);
            if let Some(inode) = recorded_inode.or_else(|| crate::inode::get_inode(&abs_path)) {
                self.state.shadow.set_inode(inode, &rel);
            }
        }
    }

    /// Build an [`EventStamp`] for an event entering the core: the skewed wall
    /// reading plus an optional origin HLC (a remote lifecycle/edit op's stamp).
    pub(super) fn stamp(&self, origin_hlc: Option<Hlc>) -> EventStamp {
        EventStamp {
            wall_ms: self.skewed_now_ms_u64(),
            origin_hlc,
        }
    }

    /// Translate a watcher [`FileEvent`] into a [`LoopInput`], doing the edge bytes
    /// read for a modify.
    ///
    /// Everything content-shaped routes [`LoopInput::Core`]: tracked text edits,
    /// untracked creates, removals, paired renames, and (since Task 6b) binary
    /// content — the size cap and the UTF-8 read happen here at the edge, the
    /// content-mode fork in the core. The ONLY imperative routes left are the
    /// ones that must read LIVE inodes or stat the live tree, which the pure
    /// core cannot: the inode/overwrite local-rename detections (a `Modified`
    /// that is really a move) and the directory-shaped removal/create
    /// expansions. This mirrors [`Self::classify_sync_event`], where every
    /// remote content/lifecycle event is core-routed.
    pub(super) fn classify_file_event(&mut self, event: FileEvent) -> LoopInput {
        match event {
            FileEvent::Modified { rel_path } => {
                let abs_path = self.state.file_path(&rel_path);
                // A new file may actually be a local rename (inode match) or an
                // overwrite-rename — both read live inodes the core can't, so
                // detect at the edge and route imperatively when matched. This
                // check is CONTENT-AGNOSTIC and must run BEFORE the UTF-8 fork:
                // a moved BINARY arrives as the same unpaired Modified at its
                // new path (the FSEvents pair does not form across a fresh
                // `mkdir` + `mv`), and routing it straight to the blob handler
                // minted it as a brand-new document while the old doc stayed
                // alive — one file became two, permanently divergent (the
                // mixed-blob rename test pins this).
                if self.is_local_rename_candidate(&rel_path, &abs_path) {
                    return LoopInput::ImperativeFile(FileEvent::Modified { rel_path });
                }
                match std::fs::read(&abs_path) {
                    Ok(bytes) => match String::from_utf8(bytes) {
                        Ok(text) => {
                            // ONE Core route for tracked and untracked text —
                            // the core's handle_file_modified forks edit vs
                            // mint itself. The only edge work is the TRACKED
                            // doc's inode refresh: editors save via an atomic
                            // tmp-rename that gives the file a fresh inode
                            // every write; refresh the recorded inode here
                            // (the IO the core can't do) so a genuine later
                            // rename is still detected and the freed inode is
                            // not left as false-rename bait. An UNTRACKED
                            // path needs nothing here — the mint claims
                            // identity in the core and the driver records its
                            // inode when the RegisterDocument effect lands.
                            if self.state.file_identity.contains_key(&rel_path) {
                                let inode = crate::inode::get_inode(&abs_path);
                                refresh_inode(&mut self.state.file_identity, &rel_path, inode);
                                if let Some(inode) = inode {
                                    self.state.shadow.set_inode(inode, &rel_path);
                                }
                            }
                            LoopInput::Core {
                                event: Event::FileModified {
                                    rel: rel_path,
                                    content: Some(text.into_bytes()),
                                    stamp: self.stamp(None),
                                },
                                activity: true,
                            }
                        }
                        // Non-UTF-8 → the core's blob path. The size cap is
                        // the classify-edge residue (the bytes are read here);
                        // an oversized blob never reaches the core.
                        Err(not_text) => {
                            self.classify_blob_bytes(rel_path, not_text.into_bytes(), &abs_path)
                        }
                    },
                    // Gone before we read it. A rename's old half or a transient
                    // file is a no-op (the imperative path returns Ok on
                    // NotFound) — but a vanished DIRECTORY that still PREFIXES
                    // tracked documents is a tree departure (a move-to-trash
                    // arriving as an uncorrelated Name(Any) single): route it to
                    // the per-child removal expansion, whose per-child gone
                    // check keeps a racing in-space dir rename safe (its pair
                    // moved the identities first; no tracked children remain).
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        if self.has_tracked_children(&rel_path) {
                            LoopInput::ImperativeFile(FileEvent::Removed { rel_path })
                        } else {
                            LoopInput::Activity
                        }
                    }
                    // InvalidData (binary) or any other read error → imperative
                    // (blob handler classifies InvalidData; others log there).
                    Err(_) => LoopInput::ImperativeFile(FileEvent::Modified { rel_path }),
                }
            }
            // A removal frees the path it held, which can let a deferred document
            // drain into it — part of the placement cascade. With gamma ON the
            // core's `handle_file_removed` + `reconcile_placement` re-derive the
            // freed path; with it OFF the imperative `drain_pending` does.
            FileEvent::Removed { rel_path } => {
                // A DIRECTORY-shaped removal (a dir-level Remove from `rm -rf`,
                // or a move-to-trash rename whose target left the space) names
                // an untracked path that PREFIXES tracked documents. Route it
                // imperatively: the expansion stats live disk
                // (the per-child gone check) which the pure core cannot, then
                // dispatches each child per config — see `expand_removed_dir`.
                if !self.state.file_identity.contains_key(&rel_path)
                    && self.has_tracked_children(&rel_path)
                {
                    return LoopInput::ImperativeFile(FileEvent::Removed { rel_path });
                }
                {
                    LoopInput::Core {
                        event: Event::FileRemoved {
                            rel: rel_path,
                            stamp: self.stamp(None),
                        },
                        activity: true,
                    }
                }
            }
            // A rename can land on an occupied path. With gamma ON the core's
            // `handle_file_renamed` + `reconcile_placement` resolve the collision
            // into a conflict-copy (its `(path_hlc, id)`-ordered `GuardedPlace(Rename)`);
            // with it OFF the imperative cascade (place_rename / defer_if_occupied /
            // materialize_at / drain_pending) owns it.
            FileEvent::Renamed { old_path, new_path } => LoopInput::Core {
                event: Event::FileRenamed {
                    old: old_path,
                    new: new_path,
                    stamp: self.stamp(None),
                },
                activity: true,
            },
        }
    }

    /// Whether a `Modified` on `rel_path` is actually a LOCAL rename (inode match
    /// to a tracked doc whose old path is gone) or an overwrite-rename (the on-disk
    /// inode now belongs to another tracked doc). Both need the imperative detection
    /// handlers (they read live inodes + walk the tree). A pure detection cost
    /// asymmetry the core does not yet model (Phase 4 folds inodes into the shadow).
    fn is_local_rename_candidate(&self, rel_path: &Path, abs_path: &Path) -> bool {
        let Some(inode) = crate::inode::get_inode(abs_path) else {
            return false;
        };
        let tracked = self.state.file_identity.contains_key(rel_path);
        if tracked {
            // Overwrite-rename: the on-disk inode differs from the recorded one and
            // belongs to another tracked doc whose old path is gone (or is this path).
            if Some(inode) == self.recorded_inode(rel_path) {
                return false;
            }
            let space_root = self.config.space_root.clone();
            find_rename_source(&self.state.file_identity, inode, |old| {
                old == rel_path || space_root.join(old).exists()
            })
            .is_some_and(|(_, mover_id)| self.uuid_at(rel_path) != Some(mover_id))
        } else {
            // Inode-rename: an untracked path whose inode matches a tracked doc
            // whose old path is gone.
            let space_root = self.config.space_root.clone();
            find_rename_source(&self.state.file_identity, inode, |old| {
                space_root.join(old).exists()
            })
            .is_some()
        }
    }

    /// Translate a relay [`SyncEvent`] into a [`LoopInput`]. Remote TEXT ops route
    /// through the core (with the FS-5 live-content edge read); remote BLOB ops and
    /// the lifecycle events (register/rename/unregister/ack) stay on the imperative
    /// cascade until gamma's `reconcile_placement` is wired as the live placement
    /// authority — its `GuardedPlace`/`RemoveFile` reconciliation is exercised only by
    /// the `DaemonSim` for now, so the imperative handlers still own the live disk-
    /// reconcile (conform/materialize/drain).
    /// Route binary bytes the classify edge just read into the core's blob
    /// path, enforcing the size cap here (the allowed classify-edge residue —
    /// the core never sees an oversized blob). Mirrors the tracked-text arm's
    /// inode refresh: editors churn inodes for binaries too.
    pub(super) fn classify_blob_bytes(
        &mut self,
        rel_path: PathBuf,
        bytes: Vec<u8>,
        abs_path: &Path,
    ) -> LoopInput {
        if bytes.len() > ABSOLUTE_BLOB_MAX {
            // ERROR, not warn: the file will NEVER sync (and a previously-
            // synced blob that grew past the cap silently stops updating) —
            // the user's data is unprotected until they shrink or remove it.
            // Fires once per content change of the oversized file.
            error!(
                path = %abs_path.display(),
                size = bytes.len(),
                limit = ABSOLUTE_BLOB_MAX,
                "blob exceeds the size cap and is not synced"
            );
            crate::metrics_calls::record_error(
                crate::metrics_calls::error_category::BLOB_TOO_LARGE,
            );
            return LoopInput::Activity;
        }
        if self.state.file_identity.contains_key(&rel_path) {
            let inode = crate::inode::get_inode(abs_path);
            refresh_inode(&mut self.state.file_identity, &rel_path, inode);
            if let Some(inode) = inode {
                self.state.shadow.set_inode(inode, &rel_path);
            }
        }
        LoopInput::Core {
            event: Event::FileModified {
                rel: rel_path,
                content: Some(bytes),
                stamp: self.stamp(None),
            },
            activity: true,
        }
    }

    pub(super) fn classify_sync_event(&mut self, event: SyncEvent) -> LoopInput {
        match event {
            SyncEvent::Disconnected => LoopInput::Disconnected,
            SyncEvent::RemoteOps {
                document_id,
                ops,
                metadata,
                content_mode,
                // The wire carries a blob content hash, but the core's blob
                // merge re-hashes the ops itself — nothing reads it.
                content_hash: _,
            } => {
                // FS-5: read the current on-disk text for a tracked doc so the core
                // can incorporate a pending local edit before the remote merge.
                // Prefer-shadow (`doc_disk_path`): for a DISPLACED doc the bytes
                // live at its conflict path — reading the `uuid_to_path`
                // original would incorporate the WINNER's content into THIS
                // doc's CRDT (a wrong-content merge, carried concern 2). A
                // binary file fails the UTF-8 read and carries `None` — the
                // blob LWW merge has no pending-edit incorporation.
                let local_content = self
                    .state
                    .doc_disk_path(&document_id)
                    .map(|rel| self.state.file_path(&rel))
                    .filter(|abs| abs.exists())
                    .and_then(|abs| std::fs::read_to_string(&abs).ok());
                let origin_hlc = metadata
                    .first()
                    .and_then(|m| m.hlc.clone())
                    .and_then(|w| Hlc::try_from(w).ok());
                LoopInput::Core {
                    event: Event::RemoteOps {
                        document_id,
                        ops,
                        metadata,
                        content_mode,
                        local_content,
                        stamp: self.stamp(origin_hlc),
                    },
                    activity: true,
                }
            }
            // Lifecycle (register/rename/unregister/ack): routing is feature-gated
            // (see [`Self::classify_lifecycle_event`]).
            ev @ (SyncEvent::DocumentRegistered { .. }
            | SyncEvent::DocumentRenamed { .. }
            | SyncEvent::DocumentUnregistered { .. }
            | SyncEvent::LifecycleAck { .. }) => self.classify_lifecycle_event(ev),
            // Backpressure eviction recovery (RFD bug 3). Config-agnostic: the
            // re-subscribe is a plain imperative side effect (no core placement
            // state changes), so route it through the imperative handler under
            // both procedural and gamma cascades.
            ev @ SyncEvent::StaleSubscriber { .. } => LoopInput::ImperativeSync(ev),
            SyncEvent::SpaceDocuments { .. } | SyncEvent::Connected => LoopInput::Activity,
            SyncEvent::AuthRejected(msg) | SyncEvent::Error(msg) => {
                error!(msg, "relay error");
                LoopInput::Activity
            }
        }
    }

    /// Translate a relay lifecycle event (register/rename/unregister/ack) into
    /// a [`LoopInput::Core`] event — `reconcile_placement` is the placement
    /// authority, unconditionally (the procedural cascade was sunset in
    /// Phase C). The ack arm also clears its intake-gate entry at this edge.
    ///
    /// The caller passes only the four lifecycle variants; any other is a
    /// programming error and is routed as activity-only.
    fn classify_lifecycle_event(&mut self, event: SyncEvent) -> LoopInput {
        match event {
            SyncEvent::DocumentRegistered {
                document_id,
                path,
                hlc,
            } => LoopInput::Core {
                event: Event::RemoteRegister {
                    document_id,
                    path,
                    stamp: self.stamp(hlc),
                },
                activity: true,
            },
            SyncEvent::DocumentRenamed {
                document_id,
                old_path,
                new_path,
                hlc,
                rename_causal_floor,
            } => LoopInput::Core {
                event: Event::RemoteRename {
                    document_id,
                    old_path,
                    new_path,
                    rename_causal_floor,
                    stamp: self.stamp(hlc),
                },
                activity: true,
            },
            SyncEvent::DocumentUnregistered { document_id, hlc } => LoopInput::Core {
                event: Event::RemoteUnregister {
                    document_id,
                    stamp: self.stamp(hlc),
                },
                activity: true,
            },
            SyncEvent::LifecycleAck {
                document_id,
                effective_path,
                hlc,
            } => {
                // Clear one in-flight ack-bearing command from the intake-gate
                // backlog. This decrement is a DRIVER concern (the intake
                // gate), separate from the ack's semantic handling: the ack
                // routes to the pure core, which knows nothing of the gate,
                // so the edge clears it here. Without it the depth
                // only ever climbs — a register/rename burst pins it at
                // `SYNC_BACKLOG_HIGH_WATER`, the `file_event` intake gate latches
                // shut, and the watcher is starved (the gamma bulk-move stall).
                self.intake.ack();
                LoopInput::Core {
                    event: Event::LifecycleAck {
                        document_id,
                        effective_path,
                        stamp: self.stamp(hlc),
                    },
                    activity: true,
                }
            }
            other => {
                debug!(
                    ?other,
                    "non-lifecycle event routed to classify_lifecycle_event"
                );
                LoopInput::Activity
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SyncCommand;
    use crate::core::DaemonCore;
    use crate::daemon::tests::test_worker;
    use crate::watcher::Suppression;
    use tokio::sync::mpsc;

    // --- blob size-cap boundary tests (the silent-skip gap, A4a) ---

    /// Drain every queued command from a test channel.
    fn drain_cmds(rx: &mut mpsc::UnboundedReceiver<SyncCommand>) -> Vec<SyncCommand> {
        let mut cmds = Vec::new();
        while let Ok(cmd) = rx.try_recv() {
            cmds.push(cmd);
        }
        cmds
    }

    /// Drive binary bytes at `rel` through the PRODUCTION blob route: the
    /// classify edge (where the size cap lives), the core's blob path, and
    /// `apply_effect`.
    fn run_blob_bytes(
        worker: &mut SpaceWorker,
        rel: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) {
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();
        let abs = worker.state.file_path(rel);
        let bytes = std::fs::read(&abs).unwrap();
        if let LoopInput::Core { event, .. } =
            worker.classify_blob_bytes(rel.to_path_buf(), bytes, &abs)
        {
            let effects = DaemonCore::handle(&mut worker.state, event);
            for eff in effects {
                worker.apply_effect(eff, sync_cmd_tx, &sup_tx).unwrap();
            }
        }
    }

    /// A binary file exactly AT `ABSOLUTE_BLOB_MAX` syncs: register + subscribe
    /// + the blob send. One byte OVER is skipped entirely — no identity minted,
    ///   no command sent (the cap is exclusive, the boundary itself is in).
    #[test]
    fn test_blob_cap_boundary_at_syncs_over_skips() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (sync_cmd_tx, mut sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();

        // PNG magic makes the content binary (non-UTF-8) without ambiguity.
        let mut at_cap = vec![0x89, b'P', b'N', b'G'];
        at_cap.resize(ABSOLUTE_BLOB_MAX, 0xAA);
        std::fs::write(dir.path().join("at-cap.bin"), &at_cap).unwrap();
        run_blob_bytes(&mut worker, Path::new("at-cap.bin"), &sync_cmd_tx);
        let cmds = drain_cmds(&mut sync_cmd_rx);
        assert!(
            matches!(cmds.first(), Some(SyncCommand::RegisterDocument { path, .. }) if path == "at-cap.bin"),
            "a blob exactly at the cap registers, got {cmds:?}"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c, SyncCommand::SendOps { .. })),
            "a blob exactly at the cap sends its bytes"
        );

        let mut over_cap = at_cap;
        over_cap.push(0xBB);
        std::fs::write(dir.path().join("over-cap.bin"), &over_cap).unwrap();
        run_blob_bytes(&mut worker, Path::new("over-cap.bin"), &sync_cmd_tx);
        assert!(
            drain_cmds(&mut sync_cmd_rx).is_empty(),
            "an over-cap blob sends nothing"
        );
        assert!(
            !worker
                .state
                .file_identity
                .contains_key(Path::new("over-cap.bin")),
            "an over-cap blob mints no identity"
        );
        assert!(
            worker
                .state
                .blob_state
                .get(Path::new("over-cap.bin"))
                .is_none(),
            "an over-cap blob records no blob state"
        );
    }

    /// A previously-synced blob that GROWS past the cap stops updating: the
    /// stale synced hash stays recorded (peers keep the old bytes) and no new
    /// command is sent. Pins the documented degradation so a behavior change
    /// is a conscious decision, not an accident.
    #[test]
    fn test_blob_grown_past_cap_stops_updating() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (sync_cmd_tx, mut sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();

        let small = [0x89, b'P', b'N', b'G', 1, 2, 3];
        std::fs::write(dir.path().join("grew.bin"), small).unwrap();
        run_blob_bytes(&mut worker, Path::new("grew.bin"), &sync_cmd_tx);
        assert!(!drain_cmds(&mut sync_cmd_rx).is_empty());
        let synced_hash = worker
            .state
            .blob_state
            .get(Path::new("grew.bin"))
            .expect("blob state recorded")
            .hash
            .clone();

        let mut grown = vec![0x89, b'P', b'N', b'G'];
        grown.resize(ABSOLUTE_BLOB_MAX + 1, 0xCC);
        std::fs::write(dir.path().join("grew.bin"), &grown).unwrap();
        run_blob_bytes(&mut worker, Path::new("grew.bin"), &sync_cmd_tx);
        assert!(
            drain_cmds(&mut sync_cmd_rx).is_empty(),
            "a blob grown past the cap sends nothing"
        );
        assert_eq!(
            worker
                .state
                .blob_state
                .get(Path::new("grew.bin"))
                .expect("blob state kept")
                .hash,
            synced_hash,
            "the recorded hash stays the last SYNCED content (peers keep it)"
        );
    }
}
