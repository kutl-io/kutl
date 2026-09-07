//! Effect application for [`SpaceWorker`]: the driver edge that executes the
//! pure core's [`Effect`]s — relay commands onto the unbounded `sync_cmd`
//! channel, disk writes/renames/removals through the suppression-pairing
//! funnels, the gamma `GuardedPlace` atomic stat-and-place critical section,
//! and the periodic metrics/watchdog emission — plus the free-standing
//! disk-mutation funnel (`write_doc`/`rename_doc`/`remove_doc`).

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use kutl_proto::protocol::is_blob_mode;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::SafeRelayPath;
use crate::blob_state::{HashedContent, sha256_bytes, sha256_hex};
use crate::client::SyncCommand;
use crate::core::{DaemonCore, Effect, EffectResult, Event, SpaceState, rel_path_to_string};
use crate::watcher::Suppression;

use super::SpaceWorker;
use super::session::{BacklogHealth, classify_backlog};

impl SpaceWorker {
    /// Execute one [`Effect`] — the ONLY place IO and channels happen for the pure
    /// core's events. Relay-command effects are handed to the unbounded `sync_cmd`
    /// channel via [`Self::send_cmd`] (never blocks, never dropped — a closed
    /// receiver means the session is ending); disk effects funnel through
    /// [`Self::apply_write`] / [`Self::apply_rename`] / [`Self::apply_remove`]
    /// (suppress routed through the watcher's `Suppression` channel, preserving the
    /// funnel's pairing/order); a landed disk effect folds its real post-op inode
    /// into the shadow via [`DaemonCore::apply_effect_result`] (success
    /// only). `GuardedPlace` is the placement cascade's effect: `reconcile_placement`
    /// emits it and this method executes its atomic stat-and-place critical
    /// section.
    pub(super) fn apply_effect(
        &mut self,
        effect: Effect,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        debug!(effect = effect.name(), "applying effect");
        match effect {
            // A local create's register: the pure core recorded its identity with
            // `inode: None` (it cannot stat the file), and a doc already at its final
            // path is NOT a mover, so no `GuardedPlace(Register)` fires to fold the
            // inode (unlike a placed/moved doc, whose inode lands via
            // `apply_effect_result`). Record the live inode here at the driver edge —
            // the same place the birthtime is read in `to_sync_command` — so a later
            // local move of this file is detected by `SpaceState::rename_source`
            // (which matches on the recorded inode) instead of being minted as a
            // spurious new document at the move target. Mirrors `register_identity`
            // (`daemon/identity.rs`) reading + recording the inode synchronously.
            Effect::RegisterDocument { ref path, .. } => {
                self.record_local_register_inode(path);
                let cmd = self.to_sync_command(effect);
                self.send_cmd(sync_cmd_tx, cmd)?;
            }
            Effect::Subscribe { .. }
            | Effect::SendOps { .. }
            | Effect::RenameDocument { .. }
            | Effect::UnregisterDocument { .. } => {
                // A blob upload is accounted as backlogged until the client
                // task drains it from the channel (the `blob_upload_backlog`
                // gauge — the former `handle_blob_change` increment).
                if let Effect::SendOps { content_mode, .. } = &effect {
                    let check = kutl_proto::sync::SyncOps {
                        content_mode: *content_mode,
                        ..Default::default()
                    };
                    if is_blob_mode(&check) {
                        self.blob_backlog.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let cmd = self.to_sync_command(effect);
                self.send_cmd(sync_cmd_tx, cmd)?;
            }
            Effect::WriteFile { rel, content } => {
                if let Some(res) = self.apply_write(&rel, &content, suppress_tx) {
                    DaemonCore::apply_effect_result(&mut self.state, res);
                }
            }
            Effect::RemoveFile { rel } => {
                let res = self.apply_remove(&rel, suppress_tx);
                DaemonCore::apply_effect_result(&mut self.state, res);
            }
            Effect::ReloadDoc { document_id } => self.reload_doc(&document_id),
            Effect::SaveDoc { document_id } => {
                if let Err(e) = self.save_doc(&document_id) {
                    error!(error = %e, %document_id, "failed to save document sidecar");
                    // The on-disk sidecar is now BEHIND the in-memory CRDT. If
                    // a restart loads that stale sidecar while the disk file
                    // still matches the recorded hash, the startup guard's
                    // restore branch would overwrite the file from the stale
                    // CRDT — erasing the un-persisted ops' content. Clearing
                    // the hash degrades that doc to incorporate-as-edit:
                    // duplication beats deletion. The write intent for these
                    // bytes is already on disk, so the cleared hash is
                    // journaled now, not at the loop tail: the snapshot
                    // retires the intent before a kill can pair it with the
                    // stale sidecar.
                    if let Some(rel) = self.state.uuid_to_path.get(&document_id).cloned() {
                        self.state.identity_clear_written_hash(&rel);
                        self.journal_identity_now(&rel);
                    }
                }
            }
            // Coalesced: mark a persist pending rather than rewriting the whole
            // state snapshot inline. The loop flushes it once the intake is drained
            // (see `run_session`), so a bulk materialization does one save per burst
            // instead of one O(docs) rewrite per doc.
            Effect::SaveState => self.state_dirty = true,
            // The blob LWW map persists inline (the former handle_blob_change/
            // handle_remote_blob saves) — small file, low frequency.
            Effect::SaveBlobState => {
                if let Err(e) = self.state.blob_state.save(&self.config.space_root) {
                    error!(error = %e, "failed to persist blob state");
                }
            }
            Effect::EmitMetrics => self.emit_periodic_metrics(),
            // The cascade's placement effect: `reconcile_placement` is the
            // placement authority, and the driver executes its atomic
            // stat-and-place critical section + the inode probe here.
            Effect::GuardedPlace {
                id,
                target,
                expected_free,
                place_kind,
            } => {
                self.apply_guarded_place(
                    &id,
                    &target,
                    expected_free,
                    place_kind,
                    sync_cmd_tx,
                    suppress_tx,
                )?;
            }
        }
        Ok(())
    }

    /// Record the live inode for a just-registered local document at `rel`, the IO
    /// the pure core could not do.
    ///
    /// The core registers a local create's identity with `inode: None` (it cannot
    /// stat the filesystem). For a doc placed/moved by the cascade the inode lands
    /// via `apply_effect_result`, but a local create already sits at its final path,
    /// so it is NOT a placement mover and emits no `GuardedPlace` to fold an inode.
    /// Without this, `SpaceState::rename_source` (which matches a moved file to its
    /// origin via the recorded inode) could never match the document on a later
    /// local move, and the relocated file would be minted as a spurious new document
    /// at the move target. Reading + recording the inode here — at the same driver
    /// edge that reads the birthtime in [`Self::to_sync_command`] — mirrors
    /// `register_identity` (`daemon/identity.rs`), which records it synchronously.
    ///
    /// No-op when the file is not on disk (a register whose content has not landed)
    /// or the inode cannot be read; the tracked-edit refresh in
    /// `handle_file_modified` remains a backstop for any path that gets a later
    /// edit first.
    fn record_local_register_inode(&mut self, rel: &str) {
        let rel_path = PathBuf::from(rel);
        let abs_path = self.config.space_root.join(&rel_path);
        let Some(inode) = crate::inode::get_inode(&abs_path) else {
            return;
        };
        self.state.identity_set_inode(&rel_path, Some(inode));
    }

    /// Attach the real filesystem birthtime to a `RegisterDocument` effect (the IO
    /// the pure core could not do), then
    /// fold the `Effect` variant into the wire [`SyncCommand`].
    fn to_sync_command(&self, effect: Effect) -> SyncCommand {
        match effect {
            Effect::Subscribe { document_id } => SyncCommand::Subscribe { document_id },
            Effect::SendOps {
                document_id,
                ops,
                metadata,
                content_mode,
                content_hash,
            } => SyncCommand::SendOps {
                document_id,
                ops,
                metadata,
                content_mode,
                content_hash,
            },
            Effect::RegisterDocument {
                space_id,
                document_id,
                path,
                metadata,
            } => {
                // The core can't stat the filesystem, so the birthtime is read
                // here from the real on-disk file at the apply edge.
                let abs_path = self.config.space_root.join(&path);
                let originally_created_at_ms = crate::birthtime::get_birthtime_ms(&abs_path);
                SyncCommand::RegisterDocument {
                    space_id,
                    document_id,
                    path,
                    metadata,
                    originally_created_at_ms,
                }
            }
            Effect::RenameDocument {
                space_id,
                document_id,
                old_path,
                new_path,
                metadata,
                rename_causal_floor,
            } => SyncCommand::RenameDocument {
                space_id,
                document_id,
                old_path,
                new_path,
                metadata,
                rename_causal_floor,
            },
            Effect::UnregisterDocument {
                space_id,
                document_id,
                metadata,
            } => SyncCommand::UnregisterDocument {
                space_id,
                document_id,
                metadata,
            },
            // The match in `apply_effect` only routes the relay-command effects
            // here, so the disk/housekeeping/cascade variants are unreachable.
            other => unreachable!("non-relay effect routed to to_sync_command: {other:?}"),
        }
    }

    /// Funnel: write `content` to `rel` (the [`Effect::WriteFile`] arm and the
    /// gamma cascade's materialization writes), delegating to [`write_doc`] so
    /// the write suppression is registered from the content hash by
    /// construction — a caller cannot register a removal suppression for a
    /// write. Returns the post-op [`EffectResult`] (with the real inode) on
    /// success; `None` on failure (a failed write leaves the shadow
    /// unchanged for the next reconcile).
    fn apply_write(
        &mut self,
        rel: &Path,
        content: &HashedContent,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Option<EffectResult> {
        let abs_path = self.config.space_root.join(rel);
        match self.write_doc(rel, &abs_path, content, suppress_tx) {
            Ok(_) => Some(EffectResult::FileWritten {
                rel: rel.to_path_buf(),
            }),
            Err(e) => {
                error!(path = %abs_path.display(), error = %e, "failed to write document");
                None
            }
        }
    }

    /// Funnel: move `old`→`new` (the `GuardedPlace(Rename)` arms), delegating to
    /// [`rename_doc`], which registers ONE pair-matched suppression (removal
    /// half at `old`, write half at `new` keyed by the moved bytes — the hash
    /// is computed from disk because the content lives at `old`, not in
    /// `SpaceState`). Returns the post-op [`EffectResult`] on success.
    fn apply_rename(
        &mut self,
        old: &Path,
        new: &Path,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Option<EffectResult> {
        let old_abs = self.config.space_root.join(old);
        let new_abs = self.config.space_root.join(new);
        match rename_doc(&mut self.state, old, &old_abs, new, &new_abs, suppress_tx) {
            Ok(()) => Some(EffectResult::RenameApplied {
                old: old.to_path_buf(),
                new: new.to_path_buf(),
            }),
            Err(e) => {
                error!(
                    from = %old_abs.display(), to = %new_abs.display(), error = %e,
                    "failed to apply rename to disk"
                );
                None
            }
        }
    }

    /// Funnel: delete `rel` from an [`Effect::RemoveFile`], suppressing the removal
    /// echo `(rel, None)`. Best-effort: a missing file is fine
    /// (the goal state is already met); other errors are logged, not propagated.
    ///
    /// Always returns `EffectResult::FileRemoved` — the goal state ("no file at
    /// `rel`") is met both when the unlink succeeds and when the file was
    /// already gone, and the shadow must vacate the occupant either way. (A
    /// failed unlink with the file still present is the one case the fold
    /// over-vacates; the next reconcile re-derives from the lattice and the
    /// stat re-observes the file, the same stateless self-heal every other
    /// funnel failure relies on.)
    pub(super) fn apply_remove(
        &self,
        rel: &Path,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> EffectResult {
        let abs_path = self.config.space_root.join(rel);
        let _ = suppress_tx.send(Suppression::Single(rel.to_path_buf(), None));
        if abs_path.exists()
            && let Err(e) = std::fs::remove_file(&abs_path)
        {
            error!(path = %abs_path.display(), error = %e, "failed to remove document");
        }
        EffectResult::FileRemoved {
            rel: rel.to_path_buf(),
        }
    }

    /// Execute a gamma [`Effect::GuardedPlace`] — the atomic stat-and-place critical
    /// section. Real-driver twin of `DaemonSim::apply_guarded_place`.
    ///
    /// The stat (`stat_untracked`) and the place run ADJACENTLY in this one
    /// non-suspending method — the same shape the former `reconcile_disk_for_rename`
    /// used (`exists()` then rename) — so a file appearing in the TOCTOU window IS
    /// seen by the stat:
    /// - DISAGREEMENT (`expected_free` but the live stat found an untracked
    ///   occupant): the TOCTOU race. Do NOT place — inject `Event::UntrackedFileObserved`
    ///   and re-run `handle` so the core marks the occupant and the doc DEFERS, then
    ///   drain the re-run's effects. The on-disk bytes stay put — no clobber.
    /// - AGREEMENT: place. A `Register` claims identity + subscribes + marks the
    ///   shadow `Tracked` (so the recompute is idempotent at the fixpoint — the next
    ///   reconcile sees the doc placed and re-emits nothing). A `Rename` moves the
    ///   held file via the shared `apply_rename` funnel and folds the post-op inode.
    fn apply_guarded_place(
        &mut self,
        id: &str,
        target: &Path,
        expected_free: bool,
        place_kind: crate::core::PlaceKind,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        let Ok(uid) = uuid::Uuid::parse_str(id) else {
            return Ok(());
        };

        // The atomic stat against LIVE disk: the target holds an untracked file iff
        // bytes sit there that no tracked id owns in the shadow. Reading disk (not
        // only the shadow) is the point — the racing create is on disk but the
        // core's shadow never saw it (that is the TOCTOU window).
        let occupied_by_untracked = self.stat_untracked(target);

        // DISAGREEMENT: the core believed the path free but the stat found an
        // untracked occupant — the TOCTOU race. Do NOT place; inject
        // `UntrackedFileObserved` and re-run `handle` IN THIS SAME critical section
        // (no intervening intake) so the core marks the occupant and defers, then
        // drain the resulting effects. The disk bytes stay put — not clobbered.
        if occupied_by_untracked && expected_free {
            let event = Event::UntrackedFileObserved {
                rel: target.to_path_buf(),
                stamp: self.stamp(None),
            };
            let effects = DaemonCore::handle(&mut self.state, event);
            for eff in effects {
                self.apply_effect(eff, sync_cmd_tx, suppress_tx)?;
            }
            return Ok(());
        }

        // AGREEMENT: place. The counter means placements landed, so a place
        // that moved nothing (an occupied target, a failed funnel) does not
        // count: a document re-emitted every pass behind an occupied target
        // would otherwise climb it without a byte moving.
        if self.place_now(uid, id, target, place_kind, sync_cmd_tx, suppress_tx)? {
            crate::metrics_calls::record_placement(&self.config.space_id);
        }
        Ok(())
    }

    /// The live-disk atomic stat for [`Self::apply_guarded_place`]: `target` holds an
    /// UNTRACKED occupant iff a file exists there that NO document this daemon knows
    /// owns. Replaces the shadow-only `shadow_occupant` read with a real `exists()`
    /// so a create that slipped into the TOCTOU window (on disk but never seen by the
    /// shadow) is caught.
    ///
    /// "Owns" is checked against BOTH `file_identity` (the path → id index, which a
    /// LOCAL create claims at `get_or_create_uuid` time — its own just-written file is
    /// NOT a foreign untracked occupant racing its registration) AND the shadow's
    /// `Tracked` occupant. Only a file on disk that neither index claims is the
    /// genuine TOCTOU race — a foreign create no document is arbitrated onto.
    ///
    /// TWIN: the core's deferral predicate (`reconcile_placement`'s
    /// `held_by_untracked`, `core/reconcile.rs`) applies the SAME
    /// identity-overrides-marker precedence; keep the two in sync — a divergence
    /// strands a minted doc deferred on its own file (unplaced, unsubscribed).
    fn stat_untracked(&self, target: &Path) -> bool {
        // The SHARED TWIN predicate (`held_by_foreign_untracked`) with the
        // shell's live disk evidence: the atomic stat sees occupants the
        // shadow has not folded yet (the TOCTOU window the core cannot see).
        let abs = self.config.space_root.join(target);
        crate::core::shadow::held_by_foreign_untracked(
            self.state
                .shadow
                .shadow_occupant
                .get(&crate::core::casefold(target)),
            abs.exists(),
            self.state.file_identity.contains_key(target),
        )
    }

    /// The `Register` arm of [`Self::place_now`]: claim identity + the shadow at
    /// `target`, place any bytes this daemon already holds, and subscribe.
    fn place_by_register(
        &mut self,
        uid: uuid::Uuid,
        id: &str,
        target: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<bool> {
        // Claim identity at the target ONLY when the doc is not already
        // tracked — a deferred remote register that never claimed a path.
        // A doc ALREADY in `uuid_to_path` is a
        // LOCAL create (confirmed:false, awaiting its own ack) or an eagerly-
        // placing remote register (confirmed:true); re-registering it would
        // OVERWRITE its `confirmed` flag — prematurely confirming a local
        // create and breaking the offline-delete floor. So preserve it.
        if !self.state.uuid_to_path.contains_key(id) {
            let Ok(safe) = SafeRelayPath::new(&rel_path_to_string(target)) else {
                return Ok(false);
            };
            self.register_identity(safe, id.to_owned(), /* confirmed */ true);
        }
        // Mark the shadow `Tracked` so the next reconcile sees the doc placed
        // and re-emits nothing (idempotent fixpoint). `register_identity` only
        // sets the shadow when the file already exists on disk; a register
        // places no bytes, so set it explicitly here.
        self.state.shadow.set_tracked(target, uid);
        let abs = self.config.space_root.join(target);
        if crate::inode::get_inode(&abs).is_none() {
            // No file at the claimed path. Usually fine — the subscribe below
            // streams the content in and the content path writes it. But a doc
            // REVIVED after a local delete already carries its content: the
            // delete tore down placement state while deliberately keeping the
            // CRDT sidecar, so the ops that arrive are ops this daemon already
            // holds, the merge advances nothing, and the content path returns
            // without writing. Nothing later recovers it either — the
            // `set_tracked` above just claimed the path, so the doc is no
            // longer a mover and every reconcile is a silent fixpoint.
            //
            // The invariant: never claim a path without the bytes being there
            // or guaranteed to arrive. When the sidecar already holds content,
            // that guarantee does not exist, so place it now.
            let held = self
                .state
                .get_doc(id)
                .map(|doc| doc.content().into_bytes())
                .filter(|content| !content.is_empty());
            if let Some(content) = held {
                debug!(
                    %id,
                    target = %target.display(),
                    bytes = content.len(),
                    "materializing revived document from its own sidecar"
                );
                if let Some(res) =
                    self.apply_write(target, &HashedContent::new(content), suppress_tx)
                {
                    DaemonCore::apply_effect_result(&mut self.state, res);
                }
            }
        }
        // Subscribe so the doc's content streams in — for BOTH a remote
        // register (its content arrives via the stream) and a local create
        // (every register is followed by its own subscribe; a self-subscribe
        // is harmless).
        self.send_cmd(
            sync_cmd_tx,
            SyncCommand::Subscribe {
                document_id: id.to_owned(),
            },
        )
        .context("failed to subscribe to gamma-placed document")?;
        Ok(true)
    }

    /// Place `id` at `target` by `place_kind`. Returns whether the placement
    /// landed — a register claim, or a write or move that reached disk — so
    /// the caller's placements counter counts only those; a skipped or failed
    /// place reports `false` and the doc stays a mover for the next reconcile.
    fn place_now(
        &mut self,
        uid: uuid::Uuid,
        id: &str,
        target: &Path,
        place_kind: crate::core::PlaceKind,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<bool> {
        match place_kind {
            crate::core::PlaceKind::Register => {
                self.place_by_register(uid, id, target, sync_cmd_tx, suppress_tx)
            }
            crate::core::PlaceKind::Rename { old_rel } => {
                // A placement onto a CONFLICT path is a DISPLACEMENT: this document
                // LOST a path collision and the lattice resolves it to its immutable
                // conflict sibling. Materialize it from the (uuid-keyed, intact) CRDT
                // sidecar rather than `apply_rename`-ing whatever sits at `old_rel`.
                //
                // The blind move is unsound for a displacement: the loser's file at
                // the contested path is frequently CLOBBERED by the winner before this
                // runs (a concurrent rename/create landed the winner's bytes there),
                // so moving it would carry the WRONG content to the conflict path; and
                // when a concurrent remote rename moved the loser's IDENTITY onto the
                // contested path while the occupied-target guard skipped its disk move,
                // the loser's real file is stranded at its register path (an orphan the
                // move never reaches). Writing from the CRDT gives the correct bytes
                // unconditionally and the explicit `old_rel` remove vacates the orphan.
                // (Mirrors the imperative overwrite handler's "recover the occupant
                // from its sidecar at the conflict path", daemon.rs `handle_overwrite_rename`.)
                let is_displacement =
                    rel_path_to_string(target).contains(kutl_core::lattice::CONFLICT_INFIX);
                if is_displacement {
                    // BLOB displacement: there is NO text CRDT to materialize
                    // from — `load_or_create_doc(id).content()` is EMPTY for a
                    // blob, so the text path below would write a zero-byte
                    // conflict file and then REMOVE the real bytes (data loss).
                    // When this doc still owns `old_rel` and the bytes there
                    // hash-match its last-synced blob state, MOVE the real
                    // bytes. Otherwise (clobbered by the winner, or the path
                    // belongs to another doc on this worker) leave the disk
                    // alone and re-subscribe: the relay's catch-up redelivers
                    // the blob, and `handle_remote_blob` writes it at the
                    // doc's (now conflict) path.
                    if let Some(blob) = self.state.blob_state.get(&old_rel) {
                        let owns = self.uuid_at(&old_rel).as_deref() == Some(id);
                        let abs_old = self.config.space_root.join(&old_rel);
                        let intact = owns
                            && std::fs::read(&abs_old).is_ok_and(|b| sha256_hex(&b) == blob.hash);
                        if intact {
                            let Some(res) = self.apply_rename(&old_rel, target, suppress_tx) else {
                                return Ok(false);
                            };
                            // Claim identity BEFORE folding so `RenameApplied`
                            // resolves the id (re-keys blob_state too).
                            self.move_identity(&old_rel, target.to_path_buf(), id);
                            DaemonCore::apply_effect_result(&mut self.state, res);
                            return Ok(true);
                        }
                        warn!(
                            %id, old = %old_rel.display(), to = %target.display(),
                            "displaced blob not recoverable locally; re-subscribing for redelivery"
                        );
                        if self.state.uuid_to_path.contains_key(id) {
                            let current = self.state.uuid_to_path[id].clone();
                            self.move_identity(&current, target.to_path_buf(), id);
                        } else if let Ok(safe) = SafeRelayPath::new(&rel_path_to_string(target)) {
                            self.register_identity(safe, id.to_owned(), true);
                        }
                        self.state.shadow.set_tracked(target, uid);
                        self.send_cmd(
                            sync_cmd_tx,
                            SyncCommand::Subscribe {
                                document_id: id.to_owned(),
                            },
                        )?;
                        return Ok(false);
                    }

                    // Materialize the conflict copy from the CRDT and claim the
                    // path, then vacate the stale source so it leaves no orphan.
                    let landed = self.materialize_and_claim(id, &old_rel, target, suppress_tx);
                    if landed {
                        let abs_old = self.config.space_root.join(&old_rel);
                        if old_rel.as_path() != target && abs_old.exists() {
                            self.apply_remove(&old_rel, suppress_tx);
                        }
                    }
                    return Ok(landed);
                }

                // Occupied-target guard (the former `reconcile_disk_for_rename`'s
                // shape: `abs_old.exists() && !abs_new.exists()`): move a WINNER
                // onto a clean target ONLY when the target is free on LIVE disk.
                //
                // A concurrent LOCAL rename onto the SAME target (the rename/rename
                // collision) can put a DIFFERENT live file at `target`
                // before this cascade-derived placement runs, and the cascade derived
                // this `Rename` from a lattice that has not yet folded that local move
                // (its watcher event is still in flight). Blindly `apply_rename`-ing
                // here would (a) clobber the occupant's bytes and orphan that document,
                // and (b) register a write-echo suppression at `target` that MASKS the
                // occupant's own pending local-rename watcher event — so the lattice
                // never learns about the second mover, never arbitrates the collision,
                // and the loser is silently destroyed. Skipping is self-correcting: the
                // occupant's local-rename event then surfaces unmasked → emits its
                // `RenameDocument` → the relay arbitrates → its displacement broadcast
                // re-runs this reconcile with both movers known, and the `(path_hlc,
                // id)` order vacates the loser to its (now free) conflict path before
                // the winner takes `target`. The `stat_untracked` arm above already
                // handles a foreign UNTRACKED occupant; this also covers a target
                // held by another live file the shadow has not yet caught up to.
                //
                // EXCEPTION: an "occupant" that is the held source ITSELF — both
                // names resolve to one file, the CASE-ONLY rename on a
                // case-insensitive filesystem — is not an occupant. Skipping there
                // strands the rename forever: there is no second mover whose
                // watcher event would self-correct it, and every reconcile re-skips.
                let abs_old = self.config.space_root.join(&old_rel);
                let abs_target = self.config.space_root.join(target);
                if abs_target.exists() && !crate::inode::same_file(&abs_old, &abs_target) {
                    return Ok(false);
                }
                // Missing held source: the recorded source path holds no file because a
                // concurrent LOCAL rename relocated this document's file out-of-band (its
                // watcher event has not drained yet) or it was deleted locally while the
                // relay keeps the doc alive. CONFORM the relocated file to the
                // authoritative target by inode (or MATERIALIZE from the CRDT) — the
                // former `conform_or_materialize_at`'s contract. Without
                // this the file stays at its local name and the doc's not-yet-drained
                // watcher event re-emits a `RenameDocument` whose HLC, recv'd past a
                // peer's injected clock skew, beats the authoritative winner (the
                // skew wrong-winner: the +5s node should win, but the loser's late local
                // rename ties its physical_ms and wins on the logical counter).
                if !abs_old.exists() {
                    return Ok(self.conform_relocated_or_materialize(
                        id,
                        &old_rel,
                        target,
                        suppress_tx,
                    ));
                }
                // Move the held file onto the target through the shared funnel
                // (suppress pairing/order preserved) and fold the post-op inode.
                let Some(res) = self.apply_rename(&old_rel, target, suppress_tx) else {
                    return Ok(false);
                };
                // Claim the new path's identity, as the former
                // `place_rename` did (`move_identity` before the disk
                // reconcile). The cascade derives a DISPLACEMENT move (a loser to
                // its conflict path) purely from `arbitrate` — no lifecycle handler
                // ever ran `move_identity` for it (only the explicitly-renamed doc
                // gets one). Without claiming identity here, `file_identity[target]`
                // stays empty, so `apply_effect_result`'s `RenameApplied` fold reads
                // `file_identity[new] == None`, calls `rename_fold(.., id: None ..)`,
                // and so NEVER re-keys `shadow_path[id]` onto `target` (it only
                // vacates the old key). The doc then stays a mover on every
                // subsequent reconcile — re-running the move and corrupting the
                // target with whatever later landed at `old_rel`. Claiming identity
                // first makes the fold resolve the id, so the placement is
                // idempotent at the fixpoint.
                self.move_identity(&old_rel, target.to_path_buf(), id);
                DaemonCore::apply_effect_result(&mut self.state, res);
                Ok(true)
            }
        }
    }

    /// Write `id`'s CRDT content at `target`, a path no identity claims yet,
    /// then claim it by moving the identity from `old_rel`. The funnel cannot
    /// record the landed hash against a path that has no identity, and the
    /// move carries the OLD path's hash forward on the premise that a move
    /// carries its bytes — false here, where the bytes were written: left
    /// alone, the identity would describe whatever sat at `old_rel`, and a
    /// restart with the CRDT ahead of these bytes would read the file as an
    /// offline edit and re-insert content the CRDT already carries. So the
    /// landed hash is recorded once the move has claimed the path, and
    /// journaled at once for the same kill window the funnel closes (the
    /// move itself refreshes the inode from the file now at `target`).
    /// Returns whether the bytes landed; a refused write is logged and leaves
    /// identity and shadow unchanged for the next reconcile.
    fn materialize_and_claim(
        &mut self,
        id: &str,
        old_rel: &Path,
        target: &Path,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> bool {
        let content = HashedContent::new(self.state.load_or_create_doc(id).content().into_bytes());
        let Some(res) = self.apply_write(target, &content, suppress_tx) else {
            return false;
        };
        // Claim identity at the target BEFORE folding so the `FileWritten`
        // fold resolves the id and marks the shadow `Tracked(id)` there
        // (idempotent fixpoint).
        self.move_identity(old_rel, target.to_path_buf(), id);
        self.state.identity_set_written_hash(target, content.hex());
        self.journal_identity_now(target);
        DaemonCore::apply_effect_result(&mut self.state, res);
        true
    }

    /// Conform a concurrently-relocated file to the authoritative `target`, or
    /// materialize it from the CRDT — the surviving port of the former
    /// imperative `conform_or_materialize_at`, reached from [`Self::place_now`]'s
    /// `Rename` arm when the held source path (`old_rel`, the shadow's `shadow_path`
    /// for the doc) holds no file on LIVE disk. Returns whether bytes moved.
    ///
    /// The source is empty because a concurrent LOCAL rename moved the file
    /// out-of-band (its watcher event has not drained yet) or it was deleted while
    /// the relay keeps the doc alive. Locate it by the doc's recorded inode:
    /// - found elsewhere → `apply_rename` it onto `target` (suppressed), then re-key
    ///   identity off the stale `old_rel` and fold the shadow. This is what stops the
    ///   stale local-rename echo from re-winning the LWW: with the file already at the
    ///   authoritative path, the doc's not-yet-drained watcher event finds nothing at
    ///   its local name and emits no competing `RenameDocument` (the skew
    ///   wrong-winner). A GENUINE concurrent
    ///   local rename does not reach this branch anymore: the driver drains it
    ///   (emitting its `RenameDocument` with a pre-recv stamp) via
    ///   `drain_relocated_local_rename` BEFORE dispatching the remote rename
    ///   into the core, so the silence here only ever swallows echoes of
    ///   an already-emitted rename.
    /// - not found (genuinely deleted) → write the CRDT content at `target` and
    ///   claim it ([`Self::materialize_and_claim`]).
    ///
    /// Uses the shared funnels (`apply_rename`/`write_doc` + `apply_effect_result`)
    /// and `move_identity`, rather than the driver-local rename bookkeeping helpers
    /// (`rename_doc`/`fold_shadow_rename`, which the local-rename detector owns), so
    /// this placement path keeps its own identity-follows-placement shadow
    /// bookkeeping. The inode `WalkDir` is O(files) but runs only on this rare
    /// missing-source path (never per content op). Best-effort like the former
    /// `conform_or_materialize_at` was: the funnels log their own IO errors.
    fn conform_relocated_or_materialize(
        &mut self,
        id: &str,
        old_rel: &Path,
        target: &Path,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> bool {
        match self.space_file_with_inode(self.recorded_inode(old_rel)) {
            // A live file carries the doc's inode at a different path: CONFORM it onto
            // the authoritative target (`relocated == target` is a no-op the `!=` skips).
            Some(relocated) if relocated.as_path() != target => {
                let Some(res) = self.apply_rename(&relocated, target, suppress_tx) else {
                    return false;
                };
                self.move_identity(old_rel, target.to_path_buf(), id);
                DaemonCore::apply_effect_result(&mut self.state, res);
                true
            }
            // Already at the authoritative path — nothing to do.
            Some(_) => false,
            // No file carries the inode: the doc was deleted locally yet the relay
            // holds it alive (a delete-superseding rename passed the gate).
            // MATERIALIZE from the (uuid-keyed, intact) CRDT sidecar at `target`.
            None => self.materialize_and_claim(id, old_rel, target, suppress_tx),
        }
    }

    /// Send a relay command on the unbounded `sync_cmd` channel. Non-blocking:
    /// the only failure is a closed receiver, which means the WS write task is
    /// gone and the session is ending — surfaced as an error so the loop tears the
    /// session down rather than silently dropping a command (a dropped command
    /// could leave a doc stuck `confirmed:false` or an op unsynced forever).
    ///
    /// Ack-bearing commands (`RegisterDocument`/`RenameDocument`/
    /// `UnregisterDocument`/`ListSpaceDocuments`) bump the [`IntakeGate`] so the
    /// intake gate (see [`Self::next_event`]) bounds how far the loop races ahead
    /// of a slow relay; the matching ack decrements it.
    pub(super) fn send_cmd(
        &mut self,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        cmd: SyncCommand,
    ) -> Result<()> {
        self.intake.track(&cmd);
        sync_cmd_tx
            .send(cmd)
            .map_err(|_| anyhow::anyhow!("relay command channel closed"))
    }

    /// Refresh this space's live `/metrics` gauges and run the non-silent
    /// watchdog classification. Called on the 10s metrics tick while a session
    /// is up — the only window where these counters are meaningful.
    ///
    /// The watchdog converts a silent `confirmed:false` strand into a loud
    /// `error!` by classifying the combination of the intake-gate depth (in-flight
    /// unacked lifecycle commands) and `last_progress.elapsed()`:
    /// - backlog > 0 and stale ≥ [`STALE_PROGRESS_THRESHOLD`] → ERROR
    /// - backlog ≥ [`BACKLOG_WARN_DEPTH`] but still moving → WARN
    /// - otherwise → no log
    fn emit_periodic_metrics(&self) {
        let space = &self.config.space_id;
        let backlog = self.intake.depth();

        crate::metrics_calls::record_sync_backlog(space, backlog);

        let blob_backlog =
            u64::try_from(self.blob_backlog.load(Ordering::Relaxed).max(0)).unwrap_or(0);
        crate::metrics_calls::record_blob_upload_backlog(space, blob_backlog);

        let stale_for = self.last_progress.elapsed();
        crate::metrics_calls::record_seconds_since_last_progress(space, stale_for.as_secs());
        crate::metrics_calls::record_files_quarantined(kutl_core::envelope::quarantine_count());
        crate::metrics_calls::record_files_migrated(kutl_core::envelope::migration_count());

        // Backstop: surface a wedged loop loudly instead of stranding a doc at
        // `confirmed:false` silently. ERROR on a stalled backlog, WARN on a
        // deep but still-moving one.
        match classify_backlog(backlog, stale_for) {
            BacklogHealth::Stalled => error!(
                space = %space,
                backlog,
                stale_secs = stale_for.as_secs(),
                "sync backlog has made no progress past the stale threshold — \
                 likely a stranded lifecycle ack; the loop may be wedged"
            ),
            BacklogHealth::DeepButMoving => warn!(
                space = %space,
                backlog,
                "sync backlog is deep but still draining"
            ),
            BacklogHealth::Healthy => {}
        }
    }
}

// ── Disk-mutation funnel ────────────────────────────────────────────────────
//
// Every daemon-originated filesystem mutation goes through one of `write_doc`,
// `rename_doc` or `remove_doc` — the `SpaceWorker::apply_*` effect funnels
// delegate here. Each bundles the echo suppression *with* the mutation, so the
// two cannot drift apart: a write registers `(path, Some(content_hash))` with
// the hash of the bytes being written, carried with them in `HashedContent`
// so it cannot be of anything else (the watcher then recognizes its own
// echo, while a genuine concurrent edit, different bytes, is never
// swallowed); a removal registers `(path, None)`. No caller passes that
// option itself: the funnel wraps the digest, so a write cannot register a
// removal suppression, which would both poison a later real removal echo at
// that path and let the write echo surface as a spurious local edit. Each
// write/rename also refreshes the recorded inode, keeping
// the invariant *"a tracked file's recorded inode reflects its on-disk
// identity whenever the file exists"* that rename detection relies on. There
// is no place to forget the suppression, the hash, or the refresh, because
// there is no other way to touch the filesystem.

impl SpaceWorker {
    /// Funnel: write `content` to a document path, suppressing the resulting
    /// echo by its content hash and recording the written hash + inode.
    /// Returns the recorded inode so the effect path can fold it into the
    /// shadow.
    ///
    /// Two journal records bracket the rename. A write intent, the hash of
    /// the bytes about to land, goes to the journal on its own line BEFORE
    /// the rename; the post-write snapshot — the refreshed inode and the
    /// landed hash — is appended as soon as the rename lands, not at the loop
    /// tail, so a kill in between cannot leave a materialized file whose
    /// persisted entry has no inode (a failed append leaves the path pending
    /// for the next drain or full save). A restart that finds the intent's
    /// bytes on disk knows they are this daemon's own materialization whether
    /// or not the sidecar save that follows the write ever ran. The intent
    /// never replaces the last-written hash and is acted on only when the
    /// bytes on disk match it, so an intent for bytes that never landed is
    /// inert.
    pub(super) fn write_doc(
        &mut self,
        rel_path: &Path,
        abs_path: &Path,
        content: &HashedContent,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<Option<u64>> {
        if let Some(parent) = abs_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // A destination this process may not write must NOT be replaced:
        // the user protected it deliberately (a vendored file, a mode or
        // ACL that denies us), and a remote edit that cannot land stays
        // scoped to this one doc via the funnel's log-and-continue, never
        // wedging the space. The atomic replace below would silently
        // bypass that protection: POSIX rename checks the PARENT
        // DIRECTORY's write bit, not the destination's. So ask the OS the
        // real question first, by opening the destination for writing
        // (without truncating it): the same check an in-place write would
        // have faced, covering mode bits, ownership and ACLs alike. Refuse
        // before any work: no temp, no suppression, no recorded hash for a
        // write that will not land.
        ensure_writable(abs_path)?;
        let hex = content.hex();
        let _ = suppress_tx.send(Suppression::Single(
            rel_path.to_path_buf(),
            Some(content.digest().to_vec()),
        ));
        // Text only: the scan reads an intent's bytes as text, and a blob's
        // write is last-writer-wins, never diffed in.
        if std::str::from_utf8(content.bytes()).is_ok() {
            self.journal_pending_write(rel_path, &hex);
        }
        kutl_core::fs::write_atomic(abs_path, content.bytes())
            .with_context(|| format!("failed to place {}", abs_path.display()))?;
        let inode = crate::inode::get_inode(abs_path);
        self.state.identity_set_inode(rel_path, inode);
        self.state.identity_set_written_hash(rel_path, hex);
        // Journaled now, not at the loop tail — see the funnel doc above.
        self.journal_identity_now(rel_path);
        Ok(inode)
    }
}

/// Refuse to replace a destination this process may not write. A missing
/// destination is writable (the replace creates it); an existing one must
/// open for writing, which is the OS's own answer covering mode bits,
/// ownership and ACLs.
fn ensure_writable(abs_path: &Path) -> Result<()> {
    // Only a regular file (or nothing) is a document: opening a FIFO or a
    // device for writing could block, and replacing a directory or a
    // socket is never right. The stat follows a symlink, so the check is
    // of what the open below would reach; a dangling link reads as nothing,
    // and the replace overwrites the link itself.
    match std::fs::metadata(abs_path) {
        Ok(meta) if !meta.file_type().is_file() => {
            return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)).with_context(
                || {
                    format!(
                        "refusing to replace a non-regular file {}",
                        abs_path.display()
                    )
                },
            );
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("cannot stat {}", abs_path.display())),
    }
    match std::fs::OpenOptions::new().write(true).open(abs_path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| {
            format!(
                "refusing to overwrite a protected file {}",
                abs_path.display()
            )
        }),
    }
}

/// Funnel: rename a document on disk, suppressing both echo halves (old-path
/// removal, new-path write keyed by the renamed content) and refreshing the
/// recorded inode at the new path. Returns the recorded inode so the effect
/// path can fold it into the shadow.
pub(super) fn rename_doc(
    state: &mut SpaceState,
    old_rel: &Path,
    old_abs: &Path,
    new_rel: &Path,
    new_abs: &Path,
    suppress_tx: &mpsc::UnboundedSender<Suppression>,
) -> Result<()> {
    if let Some(parent) = new_abs.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let renamed_hash = std::fs::read(old_abs).ok().map(|b| sha256_bytes(&b));
    // ONE pair-matched suppression (see `Suppression::RenamePair`).
    let _ = suppress_tx.send(Suppression::RenamePair {
        old: old_rel.to_path_buf(),
        new: new_rel.to_path_buf(),
        hash: renamed_hash,
    });
    std::fs::rename(old_abs, new_abs).with_context(|| {
        format!(
            "failed to rename {} to {}",
            old_abs.display(),
            new_abs.display()
        )
    })?;
    let inode = crate::inode::get_inode(new_abs);
    state.identity_set_inode(new_rel, inode);
    Ok(())
}

/// Funnel: remove a document from disk, suppressing the resulting removal echo.
pub(super) fn remove_doc(
    rel_path: &Path,
    abs_path: &Path,
    suppress_tx: &mpsc::UnboundedSender<Suppression>,
) -> Result<()> {
    let _ = suppress_tx.send(Suppression::Single(rel_path.to_path_buf(), None));
    std::fs::remove_file(abs_path)
        .with_context(|| format!("failed to remove {}", abs_path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use kutl_core::Hlc;

    use crate::core::{EventStamp, FileIdentity};
    use crate::daemon::tests::test_worker;

    /// Put `content` into the in-memory CRDT for `doc`, touching no placement
    /// state: for the revive tests this is what a LOCAL DELETE leaves behind
    /// (the CRDT held, the path gone); for the reload tests it is simply a
    /// document with content.
    fn seed_doc_content(worker: &mut SpaceWorker, doc: &str, content: &str) {
        let d = worker.state.load_or_create_doc(doc);
        let agent = d.register_agent("peer").unwrap();
        d.edit(agent, "peer", "seed", kutl_core::Boundary::Auto, |ctx| {
            ctx.insert(0, content)
        })
        .unwrap();
    }

    /// A document revived after a LOCAL DELETE must be materialized from its own
    /// sidecar at the moment the placement claims the path.
    ///
    /// Reached by concurrent rename-vs-delete: the peer's rename outlives this
    /// daemon's delete, so the relay keeps the document alive at the new name and
    /// this daemon has to hold the file there too. The delete tore down placement
    /// state but deliberately kept the CRDT, so the ops arriving after the
    /// placement's subscribe are ops already held — the merge advances nothing and
    /// the content path returns without writing. The placement has meanwhile
    /// claimed the path, so the document is no longer a mover and every later
    /// reconcile is a silent fixpoint: without this write the file never appears
    /// and the two peers stay permanently, stably different.
    #[tokio::test]
    async fn test_register_place_materializes_revived_doc_from_its_sidecar() {
        const DOC: &str = "d1357ddb-ad03-495b-826e-46763f20191d";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let uid = uuid::Uuid::parse_str(DOC).unwrap();
        let target = PathBuf::from("bar.md");

        seed_doc_content(&mut worker, DOC, "payload\n");
        assert!(
            !worker.state.shadow.shadow_path.contains_key(&uid),
            "precondition: the local delete dropped the placement entry"
        );
        assert!(
            !dir.path().join(&target).exists(),
            "precondition: no file at the revived path"
        );

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();
        worker
            .place_now(
                uid,
                DOC,
                &target,
                crate::core::PlaceKind::Register,
                &cmd_tx,
                &sup_tx,
            )
            .expect("register placement");

        assert_eq!(
            std::fs::read_to_string(dir.path().join(&target)).ok(),
            Some("payload\n".to_owned()),
            "the revived document is written from its own sidecar"
        );
        // Still subscribes: the placement must keep receiving later ops.
        assert!(
            std::iter::from_fn(|| cmd_rx.try_recv().ok()).any(
                |c| matches!(c, SyncCommand::Subscribe { ref document_id } if document_id == DOC)
            ),
            "a register placement still subscribes"
        );
    }

    /// The ordinary register — a remote document whose content has NOT arrived
    /// yet — must place no bytes. The materialization above keys on content this
    /// daemon already holds; an empty sidecar means the content is still coming on
    /// the stream, and writing an empty file here would clobber the real bytes'
    /// arrival with a spurious local edit.
    #[tokio::test]
    async fn test_register_place_without_held_content_writes_nothing() {
        const DOC: &str = "f1b5c944-99dc-4ceb-b711-fc84dff3edd4";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let uid = uuid::Uuid::parse_str(DOC).unwrap();
        let target = PathBuf::from("incoming.md");

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();
        worker
            .place_now(
                uid,
                DOC,
                &target,
                crate::core::PlaceKind::Register,
                &cmd_tx,
                &sup_tx,
            )
            .expect("register placement");

        assert!(
            !dir.path().join(&target).exists(),
            "a register with no held content places no bytes"
        );
        assert!(
            std::iter::from_fn(|| cmd_rx.try_recv().ok()).any(
                |c| matches!(c, SyncCommand::Subscribe { ref document_id } if document_id == DOC)
            ),
            "and still subscribes so the content can arrive"
        );
    }

    /// The write funnel records the on-disk inode (so a later rename of a
    /// received file is detected — not mis-emitted as a brand-new document) AND
    /// emits a content-hash suppression (so the resulting watcher event is
    /// recognized as the daemon's own echo, while a genuine concurrent edit is
    /// not). Both invariants, enforced in one place.
    #[tokio::test]
    async fn test_write_doc_records_inode_and_suppresses_by_hash() {
        let dir = tempfile::tempdir().unwrap();
        let rel = PathBuf::from("foo.md");
        let abs = dir.path().join(&rel);

        // A received doc: tracked identity, but inode is None (registered before
        // the file existed on disk) — exactly the bug's precondition.
        let mut worker = test_worker(dir.path().to_path_buf());
        worker.state.identity_insert(
            rel.clone(),
            FileIdentity {
                document_uuid: "uuid-foo".to_owned(),
                inode: None,
                last_written_hash: None,
            },
        );

        let (tx, mut rx) = mpsc::unbounded_channel::<Suppression>();
        worker
            .write_doc(&rel, &abs, &HashedContent::new(b"hello".to_vec()), &tx)
            .unwrap();
        let state = &worker.state;

        // The funnel emitted a suppression keyed by the content hash it wrote —
        // the watcher will treat the echo as ours, but a different-content edit
        // would not match.
        match rx.try_recv().expect("write_doc emits a suppression") {
            Suppression::Single(sup_path, sup_hash) => {
                assert_eq!(sup_path, rel);
                assert_eq!(
                    sup_hash,
                    Some(sha256_bytes(b"hello")),
                    "suppression carries the written content's hash"
                );
            }
            other @ Suppression::RenamePair { .. } => {
                panic!("write_doc emits a Single suppression, got {other:?}")
            }
        }

        // The funnel records the written hash — the startup scan's evidence
        // for the interrupted-materialization guard.
        assert_eq!(
            state.file_identity[&rel].last_written_hash.as_deref(),
            Some(sha256_hex(b"hello").as_str()),
            "the funnel records the written content hash"
        );

        // The file now exists and its real inode is recorded.
        let recorded = state.file_identity[&rel].inode;
        assert_eq!(
            recorded,
            crate::inode::get_inode(&abs),
            "the on-disk inode is recorded after a funnelled write"
        );

        // A later rename (file moved away, old path gone) is now detectable as a
        // rename of uuid-foo. (Skipped on platforms without inodes, where
        // recorded is None and rename detection is inode-independent.)
        if let Some(ino) = recorded {
            assert_eq!(
                state.rename_source(ino, |_| false),
                Some((rel.clone(), "uuid-foo".to_owned())),
                "rename of a received file is detected once the funnel records its inode"
            );
        }
    }

    /// A read-only destination is refused, not clobbered: the funnel returns
    /// an error (the loop's log-and-continue scopes it to the one doc) and
    /// leaves the protected file byte-intact. `rename` alone would silently
    /// replace it — POSIX rename checks the parent dir's write bit, not the
    /// file's — so the atomic write must guard the destination explicitly.
    #[test]
    fn test_write_doc_refuses_read_only_destination() {
        let dir = tempfile::tempdir().unwrap();
        let rel = PathBuf::from("ro.md");
        let abs = dir.path().join(&rel);
        std::fs::write(&abs, "protected\n").unwrap();
        let mut perms = std::fs::metadata(&abs).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&abs, perms).unwrap();

        let mut worker = test_worker(dir.path().to_path_buf());
        let (tx, mut rx) = mpsc::unbounded_channel::<Suppression>();
        let result = worker.write_doc(
            &rel,
            &abs,
            &HashedContent::new(b"remote edit it cannot take".to_vec()),
            &tx,
        );

        assert!(
            result.is_err(),
            "a read-only destination must refuse the write"
        );
        assert_eq!(
            std::fs::read_to_string(&abs).unwrap(),
            "protected\n",
            "the read-only file is byte-intact — never clobbered by the rename"
        );
        // No stranded hidden temp left beside it.
        let temps: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".kutl-tmp-"))
            .collect();
        assert!(
            temps.is_empty(),
            "a refused write leaves no temp sibling: {temps:?}"
        );
        // The refusal precedes all work: no suppression for an echo that will
        // never fire.
        assert!(
            rx.try_recv().is_err(),
            "a refused write emits no suppression"
        );
        assert!(
            !crate::state::identity_journal_path(&worker.kutl_dir()).exists(),
            "a refused write journals no intent"
        );
    }

    /// The funnel journals its write intent on its own line BEFORE the
    /// rename, so a restart can tell the landed bytes from a user edit even
    /// when the sidecar save that follows never ran; then the post-write
    /// snapshot with the inode and landed hash, appended as soon as the
    /// rename lands rather than at the loop tail, so a kill in between
    /// cannot lose the inode.
    #[tokio::test]
    async fn test_write_doc_journals_intent_then_post_write_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let rel = PathBuf::from("foo.md");
        let abs = dir.path().join(&rel);
        let mut worker = test_worker(dir.path().to_path_buf());
        worker.state.identity_insert(
            rel.clone(),
            FileIdentity {
                document_uuid: "uuid-foo".to_owned(),
                inode: None,
                last_written_hash: None,
            },
        );
        let (tx, _rx) = mpsc::unbounded_channel::<Suppression>();
        worker
            .write_doc(&rel, &abs, &HashedContent::new(b"hello".to_vec()), &tx)
            .unwrap();

        let lines = crate::state::read_journal_records(&worker.kutl_dir());
        assert_eq!(
            lines.len(),
            2,
            "the intent before the write, then the post-write snapshot — nothing waits for the loop tail"
        );
        assert_eq!(lines[0].path, "foo.md");
        assert_eq!(
            lines[0].pending_write(),
            Some(sha256_hex(b"hello").as_str()),
            "the intent carries the hash of the bytes about to land"
        );
        assert!(lines[0].entry().is_none(), "an intent is not a snapshot");
        let landed = lines[1]
            .entry()
            .expect("the post-write snapshot carries the entry");
        assert_eq!(lines[1].path, "foo.md");
        assert!(
            landed.inode.is_some(),
            "the snapshot carries the inode the write produced"
        );
        assert_eq!(
            landed.last_written_hash.as_deref(),
            Some(sha256_hex(b"hello").as_str()),
            "the snapshot carries the landed hash and retires the intent on replay"
        );
    }

    /// The intent precedes the write: a write that fails after the intent is
    /// journaled (here a destination whose directory refuses new files)
    /// leaves the intent line behind, which is inert on restart because no
    /// bytes ever matched it. Were the intent appended after the rename, this
    /// journal would be empty.
    #[tokio::test]
    async fn test_write_doc_journals_the_intent_before_the_write_lands() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let ro = dir.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        let rel = PathBuf::from("ro/new.md");
        let abs = dir.path().join(&rel);
        let mut worker = test_worker(dir.path().to_path_buf());
        worker.state.identity_insert(
            rel.clone(),
            FileIdentity {
                document_uuid: "uuid-new".to_owned(),
                inode: None,
                last_written_hash: None,
            },
        );
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o555)).unwrap();

        let (tx, _rx) = mpsc::unbounded_channel::<Suppression>();
        let result = worker.write_doc(&rel, &abs, &HashedContent::new(b"hello".to_vec()), &tx);

        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(result.is_err(), "the directory refuses the temp file");
        assert!(!abs.exists(), "nothing landed");
        let lines = crate::state::read_journal_records(&worker.kutl_dir());
        assert_eq!(
            lines.len(),
            1,
            "the intent was journaled before the write was attempted"
        );
        assert_eq!(
            lines[0].pending_write(),
            Some(sha256_hex(b"hello").as_str())
        );
        assert!(
            worker.state.file_identity[&rel].last_written_hash.is_none(),
            "no landed hash for bytes that never landed"
        );
    }

    /// A CASE-ONLY remote rename (`readme.md` → `README.md`) must move the
    /// file on a case-insensitive filesystem. The occupied-target guard
    /// (`place_now`) tests the target with `exists()`, which resolves
    /// case-insensitively to the SAME
    /// file still at its old casing — reading "occupied" and skipping the move
    /// forever (no second mover exists to self-correct). The fix: an
    /// "occupant" that is the held source itself (same inode) is not an
    /// occupant. Asserted byte-exact via the parent directory listing — a
    /// case-insensitive path probe would vacuously pass.
    #[test]
    fn test_case_only_remote_rename_moves_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let doc = "33333333-3333-4333-8333-333333333333";
        let mut worker = test_worker(dir.path().to_path_buf());
        let (sync_cmd_tx, _sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();

        std::fs::write(dir.path().join("readme.md"), "hello case\n").unwrap();
        worker.register_identity(
            SafeRelayPath::new("readme.md").unwrap(),
            doc.to_owned(),
            true,
        );

        {
            let effects = DaemonCore::handle(
                &mut worker.state,
                Event::RemoteRename {
                    document_id: doc.to_owned(),
                    old_path: "readme.md".to_owned(),
                    new_path: "README.md".to_owned(),
                    rename_causal_floor: None,
                    stamp: EventStamp {
                        wall_ms: 9_000,
                        origin_hlc: Some(Hlc {
                            physical_ms: 9_000,
                            logical: 0,
                            actor: kutl_core::ActorId(uuid::Uuid::from_u128(9)),
                        }),
                    },
                },
            );
            for eff in effects {
                worker
                    .apply_effect(eff, &sync_cmd_tx, &suppress_tx)
                    .expect("apply remote-rename effect");
            }
        }

        // Byte-exact directory listing: exactly README.md, no old casing.
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| !n.starts_with('.'))
            .collect();
        assert_eq!(
            names,
            vec!["README.md"],
            "the file must carry the new casing byte-exact"
        );
        assert_eq!(
            worker
                .state
                .uuid_to_path
                .get(doc)
                .map(std::path::PathBuf::as_path),
            Some(Path::new("README.md")),
            "identity re-keys to the new casing"
        );
    }

    /// The non-regular-file refusal applies to what the write would reach
    /// through a symlink, not to the link itself: a document path linked to a
    /// directory (or to a FIFO, which an open for writing would block on) is
    /// refused by the type check, before any open.
    #[test]
    fn test_write_doc_refuses_symlink_to_non_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let rel = PathBuf::from("linked.md");
        let abs = dir.path().join(&rel);
        let target_dir = dir.path().join("a-directory");
        std::fs::create_dir(&target_dir).unwrap();
        std::os::unix::fs::symlink(&target_dir, &abs).unwrap();

        let mut worker = test_worker(dir.path().to_path_buf());
        let (tx, mut rx) = mpsc::unbounded_channel::<Suppression>();
        let err = worker
            .write_doc(&rel, &abs, &HashedContent::new(b"x".to_vec()), &tx)
            .expect_err("a symlink to a directory is refused");

        assert!(
            format!("{err:#}").contains("non-regular"),
            "refused by the type check, not by the open: {err:#}"
        );
        assert!(
            rx.try_recv().is_err(),
            "a refused write emits no suppression"
        );
        assert!(target_dir.is_dir(), "the linked directory is untouched");
    }

    /// `place_now` reports whether it moved bytes, so the placements counter
    /// counts landed placements only: a rename whose target is held by a
    /// different live file is skipped for the next reconcile and reports
    /// `false`.
    #[test]
    fn test_place_now_reports_false_when_the_target_is_occupied() {
        const DOC: &str = "44444444-4444-4444-8444-444444444444";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        std::fs::write(dir.path().join("held.md"), "held\n").unwrap();
        std::fs::write(dir.path().join("taken.md"), "someone else\n").unwrap();
        worker.register_identity(SafeRelayPath::new("held.md").unwrap(), DOC.to_owned(), true);

        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();
        let placed = worker
            .place_now(
                uuid::Uuid::parse_str(DOC).unwrap(),
                DOC,
                Path::new("taken.md"),
                crate::core::PlaceKind::Rename {
                    old_rel: PathBuf::from("held.md"),
                },
                &cmd_tx,
                &sup_tx,
            )
            .expect("an occupied target is skipped, not an error");

        assert!(!placed, "no bytes moved, so nothing landed");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("taken.md")).unwrap(),
            "someone else\n",
            "the occupant is untouched"
        );
    }

    /// A displacement writes the loser's CRDT content at its conflict path and
    /// then claims the path by moving the identity from the contested one. The
    /// identity must describe the bytes just written, not the bytes that were
    /// at the old path: otherwise a restart with the CRDT ahead reads the
    /// conflict copy as an offline edit and re-inserts content the CRDT
    /// already carries. The landed hash reaches the journal at once.
    #[test]
    fn test_displacement_records_the_landed_hash_at_the_conflict_path() {
        const DOC: &str = "55555555-5555-4555-8555-555555555555";
        let dir = tempfile::tempdir().unwrap();
        let uid = uuid::Uuid::parse_str(DOC).unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        // The contested path holds the winner's bytes, recorded against the
        // loser's identity by the winner's funnel write.
        std::fs::write(dir.path().join("a.md"), "clobbered by the winner\n").unwrap();
        worker.register_identity(SafeRelayPath::new("a.md").unwrap(), DOC.to_owned(), true);
        worker
            .state
            .identity_set_written_hash(Path::new("a.md"), sha256_hex(b"clobbered by the winner\n"));
        seed_doc_content(&mut worker, DOC, "loser\n");
        let conflict = PathBuf::from(kutl_core::lattice::conflict_path("a.md", &uid));

        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();
        let placed = worker
            .place_now(
                uid,
                DOC,
                &conflict,
                crate::core::PlaceKind::Rename {
                    old_rel: PathBuf::from("a.md"),
                },
                &cmd_tx,
                &sup_tx,
            )
            .expect("displacement");
        assert!(placed, "the conflict copy landed");

        let abs = dir.path().join(&conflict);
        assert_eq!(std::fs::read_to_string(&abs).unwrap(), "loser\n");
        let identity = &worker.state.file_identity[&conflict];
        assert_eq!(
            identity.last_written_hash.as_deref(),
            Some(sha256_hex(b"loser\n").as_str()),
            "the identity describes the bytes written, not the old path's"
        );
        assert_eq!(
            identity.inode,
            crate::inode::get_inode(&abs),
            "the identity records the conflict file's own inode"
        );
        assert!(
            !worker.state.file_identity.contains_key(Path::new("a.md")),
            "the contested path is vacated"
        );
        assert_eq!(
            worker.state.shadow.shadow_path.get(&uid),
            Some(&conflict),
            "the shadow places the doc at its conflict path"
        );
        let last = crate::state::read_journal_records(&worker.kutl_dir())
            .pop()
            .expect("the landed hash reaches the journal at once");
        assert_eq!(last.path, crate::core::rel_path_to_string(&conflict));
        assert!(
            last.entry().is_some_and(
                |e| e.last_written_hash.as_deref() == Some(sha256_hex(b"loser\n").as_str())
            ),
            "the snapshot carries the landed hash and retires the write intent"
        );
    }

    // ── TWIN deferral predicate: equivalence pin ──

    /// The shadow occupant configured for one row of the TWIN-predicate matrix.
    #[derive(Debug, Clone, Copy)]
    enum RowShadow {
        /// No occupant marker for the target (and, in the driver row, no file
        /// on disk — the matrix keeps disk consistent with the shadow).
        Absent,
        /// The shadow marks the target `Occupant::Untracked` (a foreign file).
        Untracked,
        /// The shadow marks the target `Occupant::Tracked` by ANOTHER document.
        TrackedByOther,
    }

    /// The matrix's single mover document.
    fn twin_doc() -> uuid::Uuid {
        uuid::Uuid::from_u128(0x7717)
    }

    /// Evaluate the CORE half of the twin predicate — "is the desired target
    /// held by an untracked occupant?" — through the REAL `reconcile_placement`
    /// path, not a copied expression: with one alive record as the only mover
    /// and no revival exemption, the doc lands in `state.deferred` iff the
    /// predicate held.
    fn core_held_by_untracked(shadow: RowShadow, identity_claimed: bool) -> bool {
        let doc = twin_doc();
        let mut s = crate::core::SpaceState::new_for_test(
            "twin-test".into(),
            PathBuf::from("/tmp/twin"),
            "did:test".into(),
        );
        s.known_records
            .observe(kutl_core::lattice::DocRecord::register(
                doc,
                "p.md",
                Some(kutl_core::Hlc {
                    physical_ms: 1,
                    logical: 0,
                    actor: kutl_core::ActorId(uuid::Uuid::nil()),
                }),
            ));
        match shadow {
            RowShadow::Absent => {}
            RowShadow::Untracked => {
                s.shadow
                    .shadow_occupant
                    .insert("p.md".into(), crate::core::Occupant::Untracked);
            }
            RowShadow::TrackedByOther => {
                s.shadow.shadow_occupant.insert(
                    "p.md".into(),
                    crate::core::Occupant::Tracked(uuid::Uuid::from_u128(0xface)),
                );
            }
        }
        if identity_claimed {
            s.file_identity.insert(
                PathBuf::from("p.md"),
                crate::core::FileIdentity {
                    document_uuid: doc.to_string(),
                    inode: None,
                    last_written_hash: None,
                },
            );
        }
        s.rebuild_identity_indexes();
        let effects = crate::core::reconcile_placement(&mut s);
        let deferred = s.deferred.contains_key(&doc.to_string());
        let placed = effects
            .iter()
            .any(|e| matches!(e, Effect::GuardedPlace { id, .. } if *id == doc.to_string()));
        assert_ne!(
            deferred, placed,
            "sanity: the matrix's sole mover is either deferred or placed \
             (shadow {shadow:?}, identity {identity_claimed})"
        );
        deferred
    }

    /// Evaluate the DRIVER half — `stat_untracked` over a real tempdir — with
    /// disk materialized consistently with the shadow marker.
    fn driver_stat_untracked(shadow: RowShadow, identity_claimed: bool) -> bool {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        if !matches!(shadow, RowShadow::Absent) {
            std::fs::write(dir.path().join("p.md"), "occupant").unwrap();
        }
        match shadow {
            RowShadow::Absent => {}
            RowShadow::Untracked => {
                worker
                    .state
                    .shadow
                    .shadow_occupant
                    .insert("p.md".into(), crate::core::Occupant::Untracked);
            }
            RowShadow::TrackedByOther => {
                worker.state.shadow.shadow_occupant.insert(
                    "p.md".into(),
                    crate::core::Occupant::Tracked(uuid::Uuid::from_u128(0xface)),
                );
            }
        }
        if identity_claimed {
            worker.state.file_identity.insert(
                PathBuf::from("p.md"),
                crate::core::FileIdentity {
                    document_uuid: twin_doc().to_string(),
                    inode: None,
                    last_written_hash: None,
                },
            );
        }
        worker.state.rebuild_identity_indexes();
        worker.stat_untracked(Path::new("p.md"))
    }

    /// Pin of the TWIN deferral predicate across the IO boundary — the
    /// shell/core agreement point. The driver's `stat_untracked` (live disk +
    /// shadow + identity) and the core's held-by-untracked defer arm in
    /// `reconcile_placement` (shadow + identity) must encode IDENTICAL
    /// identity-overrides-marker precedence: a `file_identity` claim at the
    /// target beats the shadow's `Untracked` marker, in BOTH. The precedence
    /// lives once in `held_by_foreign_untracked`; this test is the regression
    /// sensor for a caller that stops feeding it the evidence it should.
    #[test]
    fn test_twin_deferral_predicate_equivalence() {
        for shadow in [
            RowShadow::Absent,
            RowShadow::Untracked,
            RowShadow::TrackedByOther,
        ] {
            for identity_claimed in [false, true] {
                // The contract: untracked-held iff the shadow says Untracked
                // AND no identity claims the target.
                let expected = matches!(shadow, RowShadow::Untracked) && !identity_claimed;
                assert_eq!(
                    core_held_by_untracked(shadow, identity_claimed),
                    expected,
                    "core defer arm at shadow {shadow:?}, identity {identity_claimed}"
                );
                assert_eq!(
                    driver_stat_untracked(shadow, identity_claimed),
                    expected,
                    "driver stat_untracked at shadow {shadow:?}, identity {identity_claimed}"
                );
            }
        }

        // The DELIBERATE asymmetry, outside the agreement matrix: a file on
        // disk the shadow has not yet seen (the TOCTOU window) is visible only
        // to the driver's live stat — the guard exists precisely because the
        // core cannot see it. Driver says occupied; the core (shadow-blind to
        // the file) would not defer. Pinned so a future "simplification" that
        // makes the driver trust the shadow alone fails here.
        let dir = tempfile::tempdir().unwrap();
        let worker = test_worker(dir.path().to_path_buf());
        std::fs::write(dir.path().join("p.md"), "raced onto disk").unwrap();
        assert!(
            worker.stat_untracked(Path::new("p.md")),
            "the driver's live stat sees a TOCTOU occupant the shadow has not folded"
        );
        assert!(
            !core_held_by_untracked(RowShadow::Absent, false),
            "the shadow-blind core does not defer on an occupant it has not observed"
        );
    }

    /// `ReloadDoc` swaps the in-memory CRDT for the sidecar copy: edits made
    /// since the last save are gone and the saved content is back.
    #[test]
    fn test_reload_doc_restores_the_sidecar_copy() {
        const DOC: &str = "88888888-8888-4888-8888-888888888888";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        std::fs::create_dir_all(dir.path().join(".kutl").join("docs")).unwrap();
        seed_doc_content(&mut worker, DOC, "saved\n");
        worker.save_doc(DOC).unwrap();
        seed_doc_content(&mut worker, DOC, "unsaved\n");
        assert_eq!(
            worker.state.get_doc(DOC).unwrap().content(),
            "unsaved\nsaved\n"
        );

        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel();
        worker
            .apply_effect(
                Effect::ReloadDoc {
                    document_id: DOC.to_owned(),
                },
                &cmd_tx,
                &sup_tx,
            )
            .unwrap();

        assert_eq!(
            worker.state.get_doc(DOC).map(kutl_core::Document::content),
            Some("saved\n".to_owned()),
            "the sidecar copy replaces the untrusted engine"
        );
    }

    /// A document that was never saved has no trustworthy copy anywhere:
    /// `ReloadDoc` drops it rather than keeping the untrusted engine.
    #[test]
    fn test_reload_doc_without_a_sidecar_drops_the_document() {
        const DOC: &str = "99999999-9999-4999-8999-999999999999";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        seed_doc_content(&mut worker, DOC, "never saved\n");

        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel();
        worker
            .apply_effect(
                Effect::ReloadDoc {
                    document_id: DOC.to_owned(),
                },
                &cmd_tx,
                &sup_tx,
            )
            .unwrap();

        assert!(worker.state.get_doc(DOC).is_none());
    }

    /// A failed sidecar save clears the doc's recorded disk hash. The sidecar
    /// on disk is now behind the in-memory CRDT; a restart loads that stale
    /// sidecar, and if the disk file still matched the recorded hash the
    /// startup guard's restore branch would overwrite it with the stale
    /// content — erasing the un-persisted ops. Clearing the hash degrades
    /// exactly that doc to incorporate-as-edit (duplication beats deletion),
    /// and the change is journaled like every identity delta.
    #[test]
    fn test_save_doc_failure_clears_written_hash() {
        const DOC: &str = "77777777-7777-4777-8777-777777777777";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let rel = PathBuf::from("doc.md");
        worker.state.identity_insert(
            rel.clone(),
            FileIdentity {
                document_uuid: DOC.to_owned(),
                inode: None,
                last_written_hash: Some(sha256_hex(b"v1\n")),
            },
        );
        worker
            .state
            .uuid_to_path
            .insert(DOC.to_owned(), rel.clone());
        {
            let doc = worker.state.load_or_create_doc(DOC);
            let agent = doc.register_agent("peer").unwrap();
            doc.edit(agent, "peer", "seed", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(0, "v2\n")
            })
            .unwrap();
        }
        // A FILE at `.kutl/docs` makes the sidecar save fail.
        let docs_dir = dir.path().join(".kutl").join("docs");
        let _ = std::fs::remove_dir_all(&docs_dir);
        std::fs::create_dir_all(dir.path().join(".kutl")).unwrap();
        std::fs::write(&docs_dir, "not a directory").unwrap();

        worker.state.journal_pending.clear();
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel();
        worker
            .apply_effect(
                Effect::SaveDoc {
                    document_id: DOC.to_owned(),
                },
                &cmd_tx,
                &sup_tx,
            )
            .expect("a SaveDoc failure is contained, not propagated");

        assert_eq!(
            worker.state.file_identity[&rel].last_written_hash, None,
            "a failed sidecar save must clear the recorded disk hash"
        );
        // Journaled NOW, not at the loop tail: the write intent for these bytes
        // is already on disk, and only a snapshot that follows it retires it.
        let last = crate::state::read_journal_records(&worker.kutl_dir())
            .pop()
            .expect("the cleared hash reaches the journal immediately");
        assert_eq!(last.path, crate::core::rel_path_to_string(&rel));
        assert!(
            last.entry().is_some_and(|e| e.last_written_hash.is_none()),
            "the snapshot carries the cleared hash and supersedes any intent"
        );
    }
}
