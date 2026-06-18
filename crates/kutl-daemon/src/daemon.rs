//! Per-space sync worker: event loop orchestrating watcher, sync client, and documents.
//!
//! Each [`SpaceWorker`] bridges files on disk with the relay for a single space:
//! - Watching for local file changes → diffing into CRDT ops → sending to relay
//! - Receiving remote ops from relay → merging into CRDT → writing to disk

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Instant;

use anyhow::{Context, Result};
use kutl_core::{Hlc, HlcClock};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::blob_state::BlobStateMap;
use crate::bridge;
use crate::client::{SyncCommand, SyncEvent};
use crate::core::{DaemonCore, DiskShadow, Event, FileIdentity, SpaceState, rel_path_to_string};
use crate::state::DaemonState;
use crate::watcher::{FileEvent, Suppression};

mod causal;
mod classify;
mod effects;
mod identity;
mod session;
mod startup;

use causal::read_clock_skew_ms;
use effects::{encode_delta, write_doc};
use identity::{find_rename_source, refresh_inode};
use session::{IntakeGate, LoopInput};

/// Number of hex characters used for the CRDT agent name (48 bits of entropy).
const AGENT_NAME_HEX_LEN: usize = 12;

/// Configuration for a per-space sync worker.
pub struct SpaceWorkerConfig {
    /// Path to the space root directory (should be canonicalized).
    pub space_root: PathBuf,
    /// DID identity of this peer.
    pub author_did: String,
    /// Relay WebSocket URL.
    pub relay_url: String,
    /// Space ID.
    pub space_id: String,
    /// Ed25519 signing key for relay authentication. `None` skips auth.
    pub signing_key: Option<ed25519_dalek::SigningKey>,
    /// If true, sync once and exit instead of running persistently.
    pub one_shot: bool,
    /// Self-declared display name sent in the handshake.
    pub display_name: String,
    /// Fired once the space worker is fully ready (connected, subscribed,
    /// watcher running). `None` in production; used by tests to avoid
    /// sleep-based waits.
    pub ready: Option<Arc<Notify>>,
    /// Cancellation token. When triggered, the worker shuts down gracefully.
    pub cancel: CancellationToken,
}

/// A per-space sync worker that orchestrates file watching, sync client, and
/// document management for a single space directory.
pub struct SpaceWorker {
    config: SpaceWorkerConfig,
    /// The pure per-space sync state: the lifecycle/sync fields the core
    /// mutates (clock, registry view, identities, deferred placements). Carved
    /// off `SpaceWorker` so `DaemonCore::handle` can drive it without IO.
    state: SpaceState,
    /// Instant of the last successful sync progress (a file or sync event
    /// processed without error). Drives the `seconds_since_last_progress` gauge.
    /// Reset to "now" at the start of each session (a fresh connection is itself
    /// activity); only the serial event loop touches it.
    last_progress: Instant,
    /// Count of blob uploads enqueued to the relay but not yet drained from the
    /// outbound channel — the `blob_upload_backlog` gauge. The producer
    /// (`handle_blob_change`) increments; the relay client task decrements as it
    /// drains each blob send, so it is shared via `Arc` across that task.
    blob_backlog: Arc<AtomicI64>,
    /// Outstanding ack-bearing relay commands: incremented when a
    /// register/rename/unregister/list command is sent (via
    /// [`Self::send_cmd`]), decremented on its matching lifecycle ack. Bounds the
    /// now-unbounded outbound channels by gating the `file_event` intake at
    /// [`SYNC_BACKLOG_HIGH_WATER`] (see [`Self::next_event`]). Also the Phase-5
    /// watchdog counter. Only the serial loop task touches it — a plain field,
    /// owned by [`IntakeGate`] so both halves of the arithmetic live in one
    /// place.
    intake: IntakeGate,
    /// A persist is pending: an [`Effect::SaveState`] was emitted (e.g. a remote
    /// doc's first materialization recorded a new inode) but not yet written to
    /// disk. Coalesced rather than written inline, because `save_state` rewrites
    /// the WHOLE `state.json` (O(docs)); doing that per materialization is O(docs²)
    /// under a bulk add (the N=1000 stage-1 regression). Flushed once the loop is
    /// caught up — both intake channels drained, see [`Self::flush_state_if_caught_up`]
    /// — so a burst does ONE save when it drains instead of one per doc. Set by
    /// [`Self::apply_effect`], cleared by [`Self::save_state`].
    state_dirty: bool,
    /// Sync events drained off the bounded `sync_event` channel DURING startup
    /// (`initial_file_scan`), before the event loop runs. The loop is the only
    /// drainer, and it starts after the scan — so a relay flood (e.g. a large
    /// re-subscribe burst) during a slow scan would fill the bounded channel and
    /// block the WS read loop's `event_tx.send().await`, stalling pings until an
    /// upstream keepalive reaper closes the connection. Draining into this buffer
    /// keeps the read loop responsive; [`Self::next_event`] processes the buffer
    /// FIRST (in arrival order), so the scan's local edits are still applied
    /// before these (mostly remote) events — local-before-remote is preserved.
    startup_buffer: VecDeque<SyncEvent>,
}

impl SpaceWorker {
    /// Create a new space worker from configuration.
    pub fn new(config: SpaceWorkerConfig) -> Result<Self> {
        let agent_name = generate_agent_name();
        // Ensure the sidecar directory exists; the in-memory CRDT store starts
        // empty and is populated by `scan_docs` below (the startup `.dt` scan).
        let docs_dir = config.space_root.join(".kutl").join("docs");
        std::fs::create_dir_all(&docs_dir)
            .with_context(|| format!("failed to create {}", docs_dir.display()))?;

        let blob_state = BlobStateMap::load(&config.space_root).unwrap_or_else(|e| {
            error!(error = %e, "failed to load blob state, starting fresh");
            BlobStateMap::default()
        });

        let kutl_dir = config.space_root.join(".kutl");
        let mut state = DaemonState::load(&kutl_dir);

        // Seed the origin HLC clock: the per-install device actor + the persisted
        // floor, so a restart never emits a stamp at or below one already
        // emitted. Persist immediately to capture a freshly-generated device id.
        let actor = state.ensure_device_actor();
        let clock = match state.hlc_floor {
            Some(floor) => HlcClock::restore(
                actor,
                Hlc {
                    physical_ms: floor.physical_ms,
                    logical: floor.logical,
                    actor,
                },
            ),
            None => HlcClock::new(actor),
        };
        if let Err(e) = state.save(&kutl_dir) {
            error!(error = %e, "failed to persist daemon state at startup");
        }

        // Populate file_identity and uuid_to_path from cached state.
        let mut file_identity = HashMap::new();
        let mut uuid_to_path = HashMap::new();
        for (path_str, uuid) in &state.validated_documents() {
            let rel_path = PathBuf::from(path_str);
            let abs_path = config.space_root.join(&rel_path);
            // Prefer the persisted inode: a file renamed while the daemon was
            // offline left its recorded path empty, so the inode can no longer
            // be read from disk there, yet the persisted value still identifies
            // the moved file. Fall back to a fresh read for legacy state files
            // (no persisted inode) and the normal present-file case.
            let inode = state
                .documents
                .get(path_str)
                .and_then(|e| e.inode)
                .or_else(|| crate::inode::get_inode(&abs_path));
            file_identity.insert(
                rel_path.clone(),
                FileIdentity {
                    document_uuid: uuid.clone(),
                    inode,
                },
            );
            uuid_to_path.insert(uuid.clone(), rel_path);
        }

        // Restore each doc's observed REGISTER stamp — the causal floor an
        // offline rename re-emitted at THIS startup must carry to supersede the
        // registration (see `rename_causal_floor`). A corrupt entry is skipped
        // (floor-absent degrades to the original lost-rename behavior, never a
        // wrong floor).
        let register_hlc: HashMap<String, Hlc> = state
            .register_hlc
            .iter()
            .filter_map(|(id, reg)| reg.to_hlc().map(|hlc| (id.clone(), hlc)))
            .collect();

        let space_state = SpaceState {
            space_id: config.space_id.clone(),
            space_root: config.space_root.clone(),
            author_did: config.author_did.clone(),
            agent_name,
            last_synced: HashMap::new(),
            blob_state,
            file_identity,
            uuid_to_path,
            state,
            hlc: clock,
            clock_skew_ms: read_clock_skew_ms(),
            lifecycle_hlc: HashMap::new(),
            register_hlc,
            documents: HashMap::new(),
            deferred: HashMap::new(),
            exempt_revival: std::collections::HashSet::new(),
            known_records: kutl_core::lattice::RegistryLattice::new(),
            shadow: DiskShadow::default(),
        };

        let mut worker = Self {
            config,
            state: space_state,
            last_progress: Instant::now(),
            blob_backlog: Arc::new(AtomicI64::new(0)),
            intake: IntakeGate::default(),
            state_dirty: false,
            startup_buffer: VecDeque::new(),
        };
        // Load every existing `.dt` sidecar into the in-memory CRDT store (the
        // startup scan, formerly `DocumentManager::scan_existing`).
        worker.scan_docs();
        Ok(worker)
    }

    /// Path to the `.kutl` directory for this space.
    fn kutl_dir(&self) -> PathBuf {
        self.config.space_root.join(".kutl")
    }

    /// Handle a local file change: diff → CRDT ops → send to relay.
    fn handle_file_event(
        &mut self,
        event: FileEvent,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        match event {
            FileEvent::Modified { rel_path } => {
                self.handle_file_modified(&rel_path, sync_cmd_tx, suppress_tx)
            }
            FileEvent::Removed { rel_path } => {
                // A directory-shaped removal expands to its tracked children
                // (see `expand_removed_dir`); a plain file removal unregisters.
                if !self.state.file_identity.contains_key(&rel_path)
                    && self.has_tracked_children(&rel_path)
                {
                    self.expand_removed_dir(&rel_path, sync_cmd_tx, suppress_tx)
                } else {
                    self.handle_file_removed(&rel_path, sync_cmd_tx)
                }
            }
            FileEvent::Renamed { old_path, new_path } => {
                self.handle_file_renamed(&old_path, &new_path, sync_cmd_tx, suppress_tx)
            }
        }
    }

    /// Handle a modified or newly created file.
    /// Inode-based rename detection: if `rel_path` is an untracked file whose
    /// inode matches a tracked document whose old path is gone, treat it as a
    /// local rename and propagate it. Handles platforms (macOS `FSEvents`) that
    /// don't emit paired rename events; the old-path-gone check in
    /// [`find_rename_source`] guards against inode reuse. Returns `true` when a
    /// rename was detected and handled.
    /// Apply a locally-DETECTED rename of `document_id` from `old` to `new` as
    /// ONE transition: all three stores (identity maps, `DiskShadow`, the gamma
    /// placement lattice) plus the `last_synced` re-key, the
    /// floor-read-BEFORE-stamp ordering, and the `RenameDocument` emit. This
    /// trio is the most failure-prone seam left from the cutover: a stale
    /// shadow orphans the doc (a `GuardedPlace` derived from a path the file no
    /// longer occupies) and a stale lattice record makes `reconcile_placement`
    /// move the file BACK, undoing the user's rename. One function makes
    /// partial application impossible at the detector sites.
    ///
    /// Used by the live inode detectors (`try_local_inode_rename`, the
    /// overwrite-rename mover half). Two deliberate NON-users, which differ in
    /// load-bearing ways: the startup offline-rename pre-pass stamps with the
    /// persisted OFFLINE floor (not a fresh tick) and runs before any shadow
    /// exists; the overwrite-rename OCCUPANT half re-writes bytes rather than
    /// moving them and deliberately folds NO lattice record (arbitration
    /// derives its conflict placement from the mover's seed).
    fn apply_local_rename_detected(
        &mut self,
        document_id: &str,
        old: &Path,
        new: &Path,
        abs_new: &Path,
        intent: &str,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) -> Result<()> {
        self.move_identity(old, new.to_path_buf(), document_id);
        self.fold_shadow_rename(document_id, old, new, abs_new);
        if let Some(version) = self.state.last_synced.remove(old) {
            self.state.last_synced.insert(new.to_path_buf(), version);
        }
        // The causal floor (the recorded registration) is read BEFORE the
        // metadata stamp / lattice fold advance this rename's HLC.
        let rename_causal_floor = self.rename_causal_floor(document_id);
        let meta = self.make_lifecycle_metadata(document_id, intent);
        self.fold_local_rename_record(document_id, new, &meta, rename_causal_floor);
        self.send_cmd(
            sync_cmd_tx,
            SyncCommand::RenameDocument {
                space_id: self.config.space_id.clone(),
                document_id: document_id.to_owned(),
                old_path: rel_path_to_string(old),
                new_path: rel_path_to_string(new),
                metadata: Some(meta),
                rename_causal_floor,
            },
        )
    }

    fn try_local_inode_rename(
        &mut self,
        rel_path: &Path,
        abs_path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<bool> {
        if self.state.file_identity.contains_key(rel_path) {
            return Ok(false);
        }
        let Some(new_inode) = crate::inode::get_inode(abs_path) else {
            return Ok(false);
        };
        let space_root = self.config.space_root.clone();
        let Some((old_path, document_id)) =
            find_rename_source(&self.state.file_identity, new_inode, |old| {
                space_root.join(old).exists()
            })
        else {
            return Ok(false);
        };

        info!(?old_path, ?rel_path, "detected rename via inode match");
        // The single three-store fold + emit (identity, shadow, lattice; see
        // `apply_local_rename_detected` for why this must be atomic).
        // Sidecar is keyed by stable document id — a rename moves no sidecar.
        self.apply_local_rename_detected(
            &document_id,
            &old_path,
            rel_path,
            abs_path,
            "file rename",
            sync_cmd_tx,
        )?;

        // Flush CRDT content if remote ops were merged mid-rename.
        self.flush_crdt_if_stale(rel_path, suppress_tx)?;
        Ok(true)
    }

    /// §1.3 — drain a not-yet-flushed concurrent LOCAL rename of `document_id`
    /// before applying a remote lifecycle op for it.
    ///
    /// The race: the user renames a file and, within the watcher's debounce
    /// window, a remote rename for the same document arrives. Applying the
    /// remote op first conforms the relocated file to the authoritative path
    /// (the former `conform_or_materialize_at` / gamma
    /// `conform_relocated_or_materialize`), destroying the only evidence of
    /// the local rename — its watcher event then finds nothing on disk (or is
    /// suppressed as a conform echo) and the rename NEVER reaches the relay.
    /// One of two concurrent renames is silently lost cluster-wide, and under
    /// clock skew the §7.1 winner flips with the interleaving (probe evidence:
    /// strict XOR — exactly one of the two renames arrived per run).
    ///
    /// Detection is the same inode evidence the conform itself uses: the doc's
    /// recorded path holds no file while a non-hidden file elsewhere in the
    /// space carries its recorded inode. Processing reuses
    /// [`Self::try_local_inode_rename`] — the watcher's own re-attribution
    /// path (identity + shadow + gamma lattice fold + emit) — so a drained
    /// rename is indistinguishable from one the watcher flushed in time.
    ///
    /// MUST run BEFORE the freshness gate `recv`s the incoming stamp: the
    /// local rename happened concurrently (the user never observed the remote
    /// op), so it must carry this daemon's pre-observation origin stamp.
    /// Stamping it after the `recv` would lift it past a skewed peer's stamp
    /// and flip the §7.1 arbitration — the catastrophic skew-defeating
    /// re-emit this area regressed on before (see the §7.1 doc comment in
    /// `fs_converge_scale_repro.rs`).
    ///
    /// Best-effort: errors are logged and the remote apply proceeds (the
    /// conform backstop still reconciles the disk).
    /// §1.3 pre-dispatch drain: surface an undrained concurrent LOCAL rename
    /// BEFORE the core gates/`recv`s an incoming event that would mutate the
    /// same document's placement — so the local rename reaches the relay with
    /// its honest origin stamp and the lattice arbitrates BOTH, instead of the
    /// conform destroying the rename's only evidence (the §1.3 swallow).
    ///
    /// Two triggers:
    /// - a remote RENAME of the doc (the original §1.3 case), and
    /// - a CONFLICT-PATH self-correction ack (residual (a) the §1.3 fix left
    ///   open): a conflict-infix effective path makes `handle_lifecycle_ack`
    ///   re-apply the same rename fold (`move_identity` onto the arbitrated
    ///   conflict path), which races a pending unflushed local rename exactly
    ///   like the remote-rename case. A plain success ack only confirms — no
    ///   mutating apply, no drain needed.
    fn drain_before_core_event(
        &mut self,
        event: &Event,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) {
        match event {
            Event::RemoteRename { document_id, .. } => {
                let id = document_id.clone();
                self.drain_relocated_local_rename(&id, sync_cmd_tx, suppress_tx);
            }
            Event::LifecycleAck {
                document_id,
                effective_path: Some(effective),
                ..
            } if effective.contains(kutl_core::lattice::CONFLICT_INFIX) => {
                let id = document_id.clone();
                self.drain_relocated_local_rename(&id, sync_cmd_tx, suppress_tx);
            }
            _ => {}
        }
    }

    fn drain_relocated_local_rename(
        &mut self,
        document_id: &str,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) {
        let Some(state_path) = self.state.uuid_to_path.get(document_id).cloned() else {
            return; // untracked: no local file to have renamed
        };
        if self.state.file_path(&state_path).exists() {
            return; // the file is where we track it: nothing pending
        }
        let Some(relocated) = self.space_file_with_inode(self.recorded_inode(&state_path)) else {
            return; // no relocation evidence (deleted, or never materialized)
        };
        if relocated == state_path || self.state.file_identity.contains_key(&relocated) {
            return; // not a relocation, or the path belongs to another tracked doc
        }
        let abs_relocated = self.state.file_path(&relocated);
        match self.try_local_inode_rename(&relocated, &abs_relocated, sync_cmd_tx, suppress_tx) {
            Ok(true) => debug!(
                %document_id,
                from = %state_path.display(),
                to = %relocated.display(),
                "drained pending local rename before remote apply"
            ),
            Ok(false) => {}
            Err(e) => warn!(
                error = %e,
                %document_id,
                "failed to drain pending local rename before remote apply"
            ),
        }
    }

    fn handle_file_modified(
        &mut self,
        rel_path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        let abs_path = self.state.file_path(rel_path);

        // Rename detection is CONTENT-AGNOSTIC (live inode reads; both fns
        // no-op on a missing file) and must run BEFORE the UTF-8 fork below: a
        // moved BINARY arrives as the same unpaired Modified at its new path,
        // and forking to `handle_blob_change` first minted it as a brand-new
        // document while the old doc stayed alive — one file became two,
        // permanently divergent (the mixed-blob rename test pins this).
        //
        // Inode-based rename detection (platforms without paired rename events).
        if self.try_local_inode_rename(rel_path, &abs_path, sync_cmd_tx, suppress_tx)? {
            return Ok(());
        }
        // Overwrite-rename detection: a `mv` onto an
        // already-tracked path. Handled in its own pass to keep this one small.
        if self.try_overwrite_rename(rel_path, &abs_path, sync_cmd_tx, suppress_tx)? {
            return Ok(());
        }

        let content = match std::fs::read_to_string(&abs_path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                // Binary content after the rename detectors declined: route to
                // the core's blob path exactly as `classify_file_event` does
                // (size cap + inode refresh at this edge, then the core).
                let bytes = match std::fs::read(&abs_path) {
                    Ok(b) => b,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(e) => {
                        return Err(e)
                            .with_context(|| format!("failed to read {}", abs_path.display()));
                    }
                };
                if let LoopInput::Core { event, .. } =
                    self.classify_blob_bytes(rel_path.to_path_buf(), bytes, &abs_path)
                {
                    let effects = DaemonCore::handle(&mut self.state, event);
                    for effect in effects {
                        self.apply_effect(effect, sync_cmd_tx, suppress_tx)?;
                    }
                }
                return Ok(());
            }
            Err(e) => {
                return Err(e).with_context(|| format!("failed to read {}", abs_path.display()));
            }
        };

        // A path the daemon has never tracked is a new document. `file_identity`
        // is the path → id index (the sidecar is keyed by id); use its keys for
        // the case-collision check, gated on the path being new.
        let was_tracked = self.state.file_identity.contains_key(rel_path);
        if !was_tracked {
            // Borrow the tracked paths directly — cloning the whole index per
            // untracked-file event was pure allocation (perf §3.1).
            if let Some(existing) = crate::case_collision::find_case_variant(
                rel_path,
                self.state.file_identity.keys().map(PathBuf::as_path),
            ) {
                error!(
                    new_path = %rel_path.display(),
                    existing_path = %existing.display(),
                    "case_collision_rejected: new file would collide with tracked document, skipping"
                );
                return Ok(());
            }
        }
        let document_id = self.get_or_create_uuid(rel_path);
        let is_new = self.state.get_doc(&document_id).is_none();

        // Editors save via the atomic tmp-rename dance, which gives the file a
        // fresh inode on every write. Refresh the recorded inode for an
        // already-tracked path so a genuine later rename of this file is still
        // detected (its current inode keeps matching) and the previous,
        // now-freed inode is not left behind as bait for a false rename match.
        // Newly registered paths already recorded their current inode, so only
        // refresh paths that existed before this event. See `refresh_inode`.
        if was_tracked {
            refresh_inode(
                &mut self.state.file_identity,
                rel_path,
                crate::inode::get_inode(&abs_path),
            );
        }

        // Register and subscribe if this is a new file.
        if is_new {
            self.register_and_subscribe(
                sync_cmd_tx,
                &document_id,
                &rel_path_to_string(rel_path),
                "file change",
            )?;
        }

        let agent_name = self.state.agent_name.clone();
        let author_did = self.config.author_did.clone();
        // Snapshot `last_synced[rel_path]` before the `doc` borrow: `doc` now
        // borrows `self.state`, so the two can no longer be read at once.
        let since = self
            .state
            .last_synced
            .get(rel_path)
            .map_or_else(Vec::new, Clone::clone);
        let doc = self.state.load_or_create_doc(&document_id);
        let agent = doc.register_agent(&agent_name)?;

        let version_before = doc.local_version();

        bridge::apply_file_change(doc, agent, &author_did, &content)?;

        let version_after = doc.local_version();
        if version_before == version_after {
            return Ok(());
        }

        let (ops, metadata) = encode_delta(doc, &since);

        self.send_cmd(
            sync_cmd_tx,
            SyncCommand::SendOps {
                document_id: document_id.clone(),
                ops,
                metadata,
                content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                content_hash: Vec::new(),
            },
        )?;

        self.state
            .last_synced
            .insert(rel_path.to_owned(), version_after);
        self.save_doc(&document_id)?;

        Ok(())
    }

    /// Detect a local overwrite-rename: the user `mv`'d a DIFFERENT tracked
    /// document onto this already-tracked path (e.g. `mv src_b target` where
    /// target held `doc_A`). macOS reports it as a Modified on the target whose
    /// on-disk inode is now the mover's, plus a Modified on the now-gone source —
    /// `try_local_inode_rename` skips it (this path is tracked). Detect it here: a
    /// tracked path whose on-disk inode now belongs to another tracked document
    /// whose old path is gone. Returns `true` (and applies it) when matched. (An
    /// editor's atomic-save gives a fresh inode that matches no tracked document,
    /// so it never fires here.)
    fn try_overwrite_rename(
        &mut self,
        rel_path: &Path,
        abs_path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<bool> {
        let Some(occupant_id) = self.uuid_at(rel_path) else {
            return Ok(false);
        };
        let Some(inode) = crate::inode::get_inode(abs_path) else {
            return Ok(false);
        };
        if Some(inode) == self.recorded_inode(rel_path) {
            return Ok(false);
        }
        let space_root = self.config.space_root.clone();
        let Some((mover_old, mover_id)) =
            find_rename_source(&self.state.file_identity, inode, |old| {
                old == rel_path || space_root.join(old).exists()
            })
        else {
            return Ok(false);
        };
        if mover_id == occupant_id {
            return Ok(false);
        }
        self.handle_overwrite_rename(
            &mover_old,
            &mover_id,
            rel_path,
            &occupant_id,
            sync_cmd_tx,
            suppress_tx,
        )?;
        Ok(true)
    }

    /// Apply a local overwrite-rename: the user `mv`'d `mover_id` (from
    /// `mover_old`, now gone) onto `target`, destroying the occupant
    /// `occupant_id`'s file there.
    ///
    /// Deterministically give the mover the canonical path and displace the
    /// occupant to its conflict path — the user's `mv` expresses "the mover takes
    /// this path". Both moves are emitted as renames to DISTINCT paths (the
    /// occupant vacates `target` before the mover claims it), so the relay and
    /// peers converge to the same conflict-copy with no path-arbitration race.
    /// The occupant's on-disk file is gone, so its content is recovered from its
    /// (uuid-keyed, untouched) CRDT sidecar and written at the conflict path.
    fn handle_overwrite_rename(
        &mut self,
        mover_old: &Path,
        mover_id: &str,
        target: &Path,
        occupant_id: &str,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        let target_str = rel_path_to_string(target);
        let Some(occ_uuid) = uuid::Uuid::parse_str(occupant_id).ok() else {
            error!(%occupant_id, "overwrite occupant id is not a uuid; cannot recover");
            return Ok(());
        };
        let conflict = kutl_core::lattice::conflict_path(&target_str, &occ_uuid);
        let conflict_rel = PathBuf::from(&conflict);
        info!(
            %mover_id, %occupant_id, target = %target.display(), %conflict,
            "overwrite-rename: mover takes the path, occupant displaced to its conflict path"
        );

        // Under gamma the occupant's file may NOT have been at `target` when the
        // overwrite landed: a concurrent remote rename can move the occupant's
        // IDENTITY onto `target` (`handle_remote_rename`'s eager `move_identity`)
        // while the cascade's occupied-target guard leaves its FILE at the prior
        // register path (the shadow still records that path). Recovering the occupant
        // to its conflict path from the sidecar then leaves that prior file as an
        // untracked ORPHAN. Capture the shadow's recorded path so we can remove it
        // below if it is a distinct live file (its bytes are preserved at `conflict`).
        let occ_stale_path = uuid::Uuid::parse_str(occupant_id)
            .ok()
            .and_then(|id| self.state.shadow.shadow_path.get(&id).cloned());

        // 1. Recover the occupant to its conflict path from its intact sidecar
        //    (its on-disk file was just destroyed by the overwrite), then emit the
        //    displacement rename. Vacates `target` for the mover.
        let occ_content = self.state.load_or_create_doc(occupant_id).content();
        let conflict_abs = self.config.space_root.join(&conflict_rel);
        write_doc(
            &mut self.state.file_identity,
            &conflict_rel,
            &conflict_abs,
            occ_content.as_bytes(),
            suppress_tx,
        )?;
        self.move_identity(target, conflict_rel.clone(), occupant_id);
        // gamma: delete the occupant's stale prior file (its bytes are now safe at
        // `conflict`) so it does not survive as an untracked orphan that the two
        // peers then disagree on. Skips `target` (the mover owns it) and `conflict`
        // (we just wrote it). Inert in the procedural overwrite case, where the
        // occupant's file genuinely WAS destroyed at `target` (shadow path == target).
        if let Some(stale) = occ_stale_path
            && stale.as_path() != target
            && stale != conflict_rel
        {
            self.apply_remove(&stale, suppress_tx);
        }
        // Fold the shadow as a rename `target`→`conflict`: the occupant's bytes now
        // live (freshly written) at the conflict path and `target` is vacated.
        // Without this gamma's `reconcile_placement` keeps seeing the occupant's
        // `shadow_path` at `target` and re-derives a doomed move from the path the
        // mover now holds. (The bytes were re-written, not moved, but the shadow
        // transition is identical: vacate old, occupy new with the post-write inode.)
        self.fold_shadow_rename(occupant_id, target, &conflict_rel, &conflict_abs);
        if let Some(v) = self.state.last_synced.remove(target) {
            self.state.last_synced.insert(conflict_rel.clone(), v);
        }
        let occ_floor = self.rename_causal_floor(occupant_id);
        let occ_meta = self.make_lifecycle_metadata(occupant_id, "overwrite displace");
        let displace = SyncCommand::RenameDocument {
            space_id: self.config.space_id.clone(),
            document_id: occupant_id.to_owned(),
            old_path: target_str.clone(),
            new_path: conflict,
            metadata: Some(occ_meta),
            rename_causal_floor: occ_floor,
        };
        self.send_cmd(sync_cmd_tx, displace)?;

        // 2. The mover takes the now-vacated target (its content is already on
        //    disk there from the `mv`): the single three-store fold + emit.
        //    The mover's rename HLC is newer than the occupant's prior
        //    placement, so `arbitrate` displaces the occupant (lower
        //    `path_hlc`) to its conflict path — agreeing with this handler's
        //    deterministic "mover takes the path" disk reconcile, so
        //    `reconcile_placement` sees both placed and emits nothing. The
        //    occupant's record already intends `target`, so seeding the mover
        //    is enough for the lattice to arbitrate the same conflict-copy.
        let target_abs = self.config.space_root.join(target);
        self.apply_local_rename_detected(
            mover_id,
            mover_old,
            target,
            &target_abs,
            "overwrite rename",
            sync_cmd_tx,
        )?;

        Ok(())
    }

    /// Handle a file rename detected by the watcher.
    fn handle_file_renamed(
        &mut self,
        old_path: &Path,
        new_path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        info!(?old_path, ?new_path, "file renamed");

        let old_path_str = rel_path_to_string(old_path);
        let new_path_str = rel_path_to_string(new_path);

        // Look up the UUID for the old path; if unknown, treat as a new file.
        let Some(identity) = self.state.file_identity.get(old_path).cloned() else {
            return self.handle_file_modified(new_path, sync_cmd_tx, suppress_tx);
        };
        let document_id = identity.document_uuid;

        // Reject rename-to-collide: if new_path is a case-variant of another
        // tracked document (not the source), refuse to propagate. The
        // filesystem holds both files; we will not sync the rename target
        // until the user resolves the collision.
        // Borrow the tracked paths directly — cloning the whole index per
        // rename event was pure allocation (perf §3.1).
        let tracked_iter = self
            .state
            .file_identity
            .keys()
            .filter(|p| p.as_path() != old_path)
            .map(PathBuf::as_path);
        if let Some(existing) = crate::case_collision::find_case_variant(new_path, tracked_iter) {
            error!(
                old_path = %old_path.display(),
                new_path = %new_path.display(),
                existing_path = %existing.display(),
                "case_collision_rejected: rename target would collide with tracked document, ignoring event"
            );
            return Ok(());
        }

        self.move_identity(old_path, new_path.to_owned(), &document_id);

        // Move last_synced version from old to new path.
        if let Some(version) = self.state.last_synced.remove(old_path) {
            self.state.last_synced.insert(new_path.to_owned(), version);
        }

        // The CRDT sidecar is keyed by document id, which is stable across a
        // rename — nothing to move on disk.

        // Send RenameDocument to relay. Read the causal floor (the recorded
        // registration) before the metadata stamp folds this rename's HLC.
        let rename_causal_floor = self.rename_causal_floor(&document_id);
        let meta = self.make_lifecycle_metadata(&document_id, "file rename");
        let rename = SyncCommand::RenameDocument {
            space_id: self.config.space_id.clone(),
            document_id,
            old_path: old_path_str,
            new_path: new_path_str,
            metadata: Some(meta),
            rename_causal_floor,
        };
        self.send_cmd(sync_cmd_tx, rename)?;

        // Flush CRDT content if remote ops were merged mid-rename.
        self.flush_crdt_if_stale(new_path, suppress_tx)?;

        Ok(())
    }

    /// Handle a file removal.
    /// Whether any tracked document lives UNDER `rel_path/` — the signature of
    /// a directory-shaped removal event (the dir path itself is never tracked).
    fn has_tracked_children(&self, rel_path: &Path) -> bool {
        self.state
            .file_identity
            .keys()
            .any(|tracked| tracked.starts_with(rel_path) && tracked != rel_path)
    }

    /// Expand a DIRECTORY-shaped removal (`rm -rf docs/` delivering a dir-level
    /// Remove, or a move-to-trash rename whose target left the space) into
    /// per-child removals: every tracked doc under `rel_path/` whose own file
    /// has truly LEFT the space is unregistered through the per-config removal
    /// path. Two per-child guards make the expansion safe:
    /// - GONE from live disk at its tracked path — a spurious dir event while
    ///   the tree still exists is a no-op;
    /// - its recorded INODE is not alive anywhere else in the space — a dir
    ///   RENAMED WITHIN the space also empties the old paths, and whether its
    ///   Remove half or its Create half drains first is map-iteration order:
    ///   expanding on the Remove half unregistered and re-minted the ENTIRE
    ///   tree (every identity lost, ~50% of shape-2 arrivals). The inode scan
    ///   is the same evidence the conform path trusts; a child found
    ///   alive elsewhere is left for the inode-rename detection to re-key.
    fn expand_removed_dir(
        &mut self,
        rel_path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        let children: Vec<PathBuf> = self
            .state
            .file_identity
            .iter()
            .filter(|(tracked, _)| tracked.starts_with(rel_path) && tracked.as_path() != rel_path)
            .filter(|(tracked, identity)| {
                !self.config.space_root.join(tracked).exists()
                    && self.space_file_with_inode(identity.inode).is_none()
            })
            .map(|(tracked, _)| tracked.clone())
            .collect();
        info!(
            dir = %rel_path.display(),
            children = children.len(),
            "directory removal: expanding to tracked children"
        );
        for child in children {
            {
                let stamp = self.stamp(None);
                let effects =
                    DaemonCore::handle(&mut self.state, Event::FileRemoved { rel: child, stamp });
                for eff in effects {
                    self.apply_effect(eff, sync_cmd_tx, suppress_tx)?;
                }
            }
        }
        Ok(())
    }

    fn handle_file_removed(
        &mut self,
        rel_path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) -> Result<()> {
        // If we have an identity, send UnregisterDocument.
        if let Some(identity) = self.state.file_identity.get(rel_path).cloned() {
            let document_id = identity.document_uuid;
            // Clear per-path sync state (last-synced version, blob state, CRDT
            // sidecar) BEFORE dropping the identity — `cleanup_document_state`
            // resolves the sidecar via `file_identity`. Parity with the remote
            // unregister path: without this, a later create at the same path
            // computes its delta against the DELETED document's stale
            // `last_synced` version vector, producing ops the relay cannot decode
            // (`BaseVersionUnknown`) — the recreate never syncs.
            self.cleanup_document_state(rel_path);
            self.unregister_identity(&document_id);

            // The delete's HLC (recorded by `make_lifecycle_metadata`) lets a
            // concurrent remote rename that is causally *earlier* be dropped (our
            // delete wins), while a causally *later* one still supersedes it (the
            // rename wins and we re-materialize). The watermark survives
            // `unregister_identity`.
            let meta = self.make_lifecycle_metadata(&document_id, "file delete");
            let unregister = SyncCommand::UnregisterDocument {
                space_id: self.config.space_id.clone(),
                document_id,
                metadata: Some(meta),
            };
            self.send_cmd(sync_cmd_tx, unregister)?;
        }

        Ok(())
    }

    /// Handle a sync event from the relay. Since the blob path moved into the
    /// core, only `StaleSubscriber` recovery is served here; every content and
    /// lifecycle event is core-routed and reaching the other arms is a bug.
    fn handle_sync_event(
        &mut self,
        event: SyncEvent,
        _suppress_tx: &mpsc::UnboundedSender<Suppression>,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) -> Result<()> {
        match event {
            SyncEvent::RemoteOps {
                document_id,
                ops,
                metadata,
                content_mode,
                content_hash,
            } => {
                // Advance the origin clock past every observed remote stamp, so a
                // lifecycle/edit op this daemon emits afterward is causally after
                // them. (Lifecycle-event HLCs arrive once those events carry the
                // authoritative DocRecord — the reconcile re-type step.)
                for m in &metadata {
                    self.observe_remote_hlc(Some(m));
                }
                // BOTH content modes are core-routed now (text via the CRDT
                // merge, blobs via the pure LWW merge) — reaching this handler
                // is a routing bug.
                let _ = (ops, content_hash, content_mode);
                error!(%document_id, "remote ops reached the imperative handler (routing bug)");
                debug_assert!(false, "remote ops are core-routed");
            }
            // Lifecycle events route through the pure core (classify_lifecycle_event
            // returns LoopInput::Core unconditionally since the procedural cascade
            // was sunset) — reaching this handler is a routing bug.
            SyncEvent::DocumentRegistered { document_id, .. }
            | SyncEvent::DocumentRenamed { document_id, .. }
            | SyncEvent::DocumentUnregistered { document_id, .. }
            | SyncEvent::LifecycleAck { document_id, .. } => {
                error!(%document_id, "lifecycle event reached the imperative handler (routing bug)");
                debug_assert!(false, "lifecycle events are core-routed");
            }
            SyncEvent::StaleSubscriber { document_id } => {
                self.handle_stale_subscriber(document_id, sync_cmd_tx)?;
            }
            SyncEvent::SpaceDocuments { .. } | SyncEvent::Connected | SyncEvent::Disconnected => {
                // Only expected during session setup; ignore if received later.
            }
            SyncEvent::AuthRejected(msg) | SyncEvent::Error(msg) => {
                error!(msg, "relay error");
            }
        }

        Ok(())
    }

    /// Recover from a relay backpressure eviction (RFD bug 3). The relay evicted
    /// us from `document_id` because our bounded outbound `data` lane overflowed
    /// — we fell behind a broadcast flood (the bulk-move N≈1000 stage-2 strand).
    /// Re-subscribe to recover: the relay's `handle_subscribe` replays the full
    /// current doc state via catch-up, so every broadcast missed between the
    /// eviction and the re-subscribe is reapplied. Degraded-but-recovering →
    /// WARN per the logging classification.
    ///
    /// STORM-AVOIDANCE — a bare re-subscribe is self-pacing, so no
    /// backoff/max-attempt machinery is needed: this event is read off the same
    /// multiplexed socket as the data broadcasts and pushed onto the bounded
    /// `sync_event` channel ([`CHANNEL_CAPACITY`] = 64). When the loop is slow
    /// enough to be evicted, that channel is full, so the read task blocks on
    /// `event_tx.send().await` until the loop drains its backlog — by which point
    /// the daemon is caught up and the re-subscribe's catch-up lands without
    /// immediately re-evicting. `Subscribe` is not ack-bearing, so [`Self::send_cmd`]
    /// does not bump the [`IntakeGate`] (no intake-gate churn).
    fn handle_stale_subscriber(
        &mut self,
        document_id: String,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) -> Result<()> {
        warn!(
            %document_id,
            "relay evicted stale subscriber (outbound backpressure); re-subscribing to recover"
        );
        self.send_cmd(sync_cmd_tx, SyncCommand::Subscribe { document_id })
    }

    /// Fold a successful imperative disk rename into the [`DiskShadow`], mirroring
    /// what the pure core's `apply_effect_result` does on `RenameApplied`. The
    /// post-move inode is read from the live file (`rename_doc` refreshed
    /// `file_identity` after the move, but the inode the shadow needs is the one
    /// the file actually carries at `abs_new`).
    fn fold_shadow_rename(
        &mut self,
        document_id: &str,
        old_rel: &Path,
        new_rel: &Path,
        abs_new: &Path,
    ) {
        let id = uuid::Uuid::parse_str(document_id).ok();
        let inode = self
            .state
            .file_identity
            .get(new_rel)
            .and_then(|fi| fi.inode)
            .or_else(|| crate::inode::get_inode(abs_new));
        self.state.shadow.rename_fold(old_rel, new_rel, id, inode);
    }
}

/// Generate a short random agent name for CRDT sessions.
///
/// Uses the first 12 hex characters of a v4 UUID (48 bits of entropy)
/// — more than enough to avoid collisions across concurrent daemon
/// sessions.
fn generate_agent_name() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..AGENT_NAME_HEX_LEN].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::EventStamp;

    /// Build a minimal, network-free `SpaceWorker` over `space_root` for unit
    /// tests of the in-memory placement/identity handlers. Shared with the
    /// child modules' test mods (`identity`), hence `pub(super)`.
    pub(super) fn test_worker(space_root: PathBuf) -> SpaceWorker {
        SpaceWorker::new(SpaceWorkerConfig {
            space_root,
            author_did: "did:test".to_owned(),
            relay_url: "ws://127.0.0.1:1/none".to_owned(),
            space_id: "test-space".to_owned(),
            signing_key: None,
            one_shot: false,
            display_name: "test".to_owned(),
            ready: None,
            cancel: CancellationToken::new(),
        })
        .expect("build test worker")
    }

    /// Build the §1.3 race precondition: `doc` is tracked at `foo.md`, but the
    /// user already renamed the file to `bar_b.md` on disk and the watcher has
    /// not drained the event yet (so identity still points at `foo.md` while the
    /// file — carrying the doc's recorded inode — sits at `bar_b.md`).
    fn seed_pending_local_rename(worker: &mut SpaceWorker, dir: &Path, doc: &str) {
        std::fs::write(dir.join("bar_b.md"), b"payload\n").unwrap();
        let inode = crate::inode::get_inode(&dir.join("bar_b.md"));
        assert!(inode.is_some(), "test platform must expose inodes");
        worker
            .state
            .uuid_to_path
            .insert(doc.to_owned(), PathBuf::from("foo.md"));
        worker.state.file_identity.insert(
            PathBuf::from("foo.md"),
            FileIdentity {
                document_uuid: doc.to_owned(),
                inode,
            },
        );
    }

    /// §1.3 residual (a): a CONFLICT-PATH self-correction ack drains a pending
    /// unflushed local rename of the same doc BEFORE the core applies the
    /// correction — the correction's `move_identity` onto the arbitrated
    /// conflict path would otherwise destroy the rename's only evidence (the
    /// old rename-swallow, the one path the §1.3 fix left open). A PLAIN
    /// success ack (no conflict infix) only confirms — it must NOT trigger
    /// the drain.
    #[tokio::test]
    async fn test_conflict_ack_drains_pending_local_rename_plain_ack_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let doc = "55555555-5555-4555-8555-555555555555";
        seed_pending_local_rename(&mut worker, dir.path(), doc);

        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();

        // A PLAIN ack: no conflict infix → no mutating apply → no drain.
        let plain = Event::LifecycleAck {
            document_id: doc.to_owned(),
            effective_path: Some("foo.md".to_owned()),
            stamp: worker.stamp(None),
        };
        worker.drain_before_core_event(&plain, &cmd_tx, &sup_tx);
        assert!(
            drain_rename_cmds(&mut cmd_rx).is_empty(),
            "a plain success ack must not trigger the §1.3 drain"
        );

        // The conflict-infix self-correction ack: the drain fires and the
        // pending local rename reaches the relay FIRST, with its honest stamp.
        let uid = uuid::Uuid::parse_str(doc).unwrap();
        let conflict = kutl_core::lattice::conflict_path("foo.md", &uid);
        let ack = Event::LifecycleAck {
            document_id: doc.to_owned(),
            effective_path: Some(conflict),
            stamp: worker.stamp(None),
        };
        worker.drain_before_core_event(&ack, &cmd_tx, &sup_tx);
        let renames = drain_rename_cmds(&mut cmd_rx);
        assert!(
            renames.iter().any(|(p, _)| p == "bar_b.md"),
            "the conflict-path ack must drain the pending local rename first, got {renames:?}"
        );
    }

    /// Collect every `RenameDocument` currently queued on the sync channel,
    /// as `(new_path, origin physical_ms)`.
    fn drain_rename_cmds(cmd_rx: &mut mpsc::UnboundedReceiver<SyncCommand>) -> Vec<(String, u64)> {
        let mut renames = Vec::new();
        while let Ok(cmd) = cmd_rx.try_recv() {
            if let SyncCommand::RenameDocument {
                new_path, metadata, ..
            } = cmd
            {
                let ms = metadata.and_then(|m| m.hlc).map_or(0, |h| h.physical_ms);
                renames.push((new_path, ms));
            }
        }
        renames
    }

    /// §1.3 — an incoming remote rename must not destroy the evidence of a
    /// concurrent local rename the watcher has not drained yet. The daemon
    /// drains the local rename FIRST — emitting its `RenameDocument` with a
    /// stamp taken BEFORE the incoming stamp is `recv`'d into the clock — and
    /// only then applies the remote op (which, being fresher here, conforms
    /// the file to the authoritative path). Without the drain, the conform
    /// relocates the file and the local rename never reaches the relay: one of
    /// two concurrent renames is silently lost cluster-wide, and under clock
    /// skew the §7.1 winner flips with the interleaving.
    #[tokio::test]
    async fn test_remote_rename_drains_pending_local_rename_first() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let doc = "33333333-3333-4333-8333-333333333333";
        seed_pending_local_rename(&mut worker, dir.path(), doc);

        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();

        // The remote rename arrives first (the race), stamped by a +5s-skewed
        // peer — strictly above anything this daemon's clock would mint.
        let incoming = Hlc {
            physical_ms: kutl_core::now_ms_u64() + 5_000,
            logical: 0,
            actor: kutl_core::ActorId(uuid::Uuid::nil()),
        };
        // Mirror the production dispatch (the event loop's Core arm): the §1.3
        // drain peek runs BEFORE the core recv's the incoming stamp, then the
        // pure core handles the rename and the driver applies its effects.
        worker.drain_relocated_local_rename(doc, &cmd_tx, &sup_tx);
        let stamp = worker.stamp(Some(incoming));
        let effects = DaemonCore::handle(
            &mut worker.state,
            Event::RemoteRename {
                document_id: doc.to_owned(),
                old_path: "foo.md".to_owned(),
                new_path: "bar_a.md".to_owned(),
                rename_causal_floor: None,
                stamp,
            },
        );
        for eff in effects {
            worker
                .apply_effect(eff, &cmd_tx, &sup_tx)
                .expect("apply remote-rename effect");
        }

        // The pending local rename was drained and emitted to the relay…
        let renames = drain_rename_cmds(&mut cmd_rx);
        let drained = renames
            .iter()
            .find(|(path, _)| path == "bar_b.md")
            .unwrap_or_else(|| {
                panic!("local rename must reach the relay; sent renames: {renames:?}")
            });
        // …stamped BEFORE the incoming stamp was recv'd: a concurrent rename
        // must carry its honest origin stamp, not a skew-defeating lift (the
        // catastrophic re-emit this area regressed on before).
        assert!(
            drained.1 < incoming.physical_ms,
            "drained rename stamp {} must predate the incoming skewed stamp {}",
            drained.1,
            incoming.physical_ms
        );

        // The fresher remote rename then applied: file conformed to bar_a.md.
        assert!(
            dir.path().join("bar_a.md").exists(),
            "remote winner must be materialized at the authoritative path"
        );
        assert!(
            !dir.path().join("bar_b.md").exists(),
            "the relocated file was conformed away"
        );
        assert_eq!(
            worker.state.uuid_to_path.get(doc),
            Some(&PathBuf::from("bar_a.md"))
        );
    }

    /// §1.3, loser direction: when the drained local rename is FRESHER than the
    /// incoming remote rename (no skew — the local stamp post-dates the remote
    /// origin), the freshness gate must drop the remote one and the file stays
    /// at the user's path; the local rename still reaches the relay, which
    /// arbitrates the cluster-wide winner.
    #[tokio::test]
    async fn test_remote_rename_loses_to_fresher_drained_local_rename() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let doc = "44444444-4444-4444-8444-444444444444";
        seed_pending_local_rename(&mut worker, dir.path(), doc);

        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();

        // The remote rename's origin stamp is well in the past, so the drained
        // local rename (stamped now) supersedes it at the gate.
        let incoming = Hlc {
            physical_ms: kutl_core::now_ms_u64() - 60_000,
            logical: 0,
            actor: kutl_core::ActorId(uuid::Uuid::nil()),
        };
        // Mirror the production dispatch (the event loop's Core arm): the §1.3
        // drain peek runs BEFORE the core recv's the incoming stamp, then the
        // pure core handles the rename and the driver applies its effects.
        worker.drain_relocated_local_rename(doc, &cmd_tx, &sup_tx);
        let stamp = worker.stamp(Some(incoming));
        let effects = DaemonCore::handle(
            &mut worker.state,
            Event::RemoteRename {
                document_id: doc.to_owned(),
                old_path: "foo.md".to_owned(),
                new_path: "bar_a.md".to_owned(),
                rename_causal_floor: None,
                stamp,
            },
        );
        for eff in effects {
            worker
                .apply_effect(eff, &cmd_tx, &sup_tx)
                .expect("apply remote-rename effect");
        }

        let renames = drain_rename_cmds(&mut cmd_rx);
        assert!(
            renames.iter().any(|(path, _)| path == "bar_b.md"),
            "local rename must reach the relay; sent renames: {renames:?}"
        );
        assert!(
            dir.path().join("bar_b.md").exists(),
            "the stale remote rename must not move the locally-renamed file"
        );
        assert!(!dir.path().join("bar_a.md").exists());
        assert_eq!(
            worker.state.uuid_to_path.get(doc),
            Some(&PathBuf::from("bar_b.md"))
        );
    }

    #[tokio::test]
    async fn test_untracked_uuid_blob_op_writes_no_garbage_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();

        // Untracked UUID + non-empty blob ops: must be skipped (the §1.5 hazard
        // on the blob path), no `<uuid>` file may appear. Routed through the
        // core's blob LWW merge (the production path since Task 6b).
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let untracked = "33333333-3333-3333-3333-333333333333";
        let effects = DaemonCore::handle(
            &mut worker.state,
            Event::RemoteOps {
                document_id: untracked.to_owned(),
                ops: b"binary".to_vec(),
                metadata: vec![],
                content_mode: i32::from(kutl_proto::sync::ContentMode::Blob),
                local_content: None,
                stamp: EventStamp {
                    wall_ms: 1,
                    origin_hlc: None,
                },
            },
        );
        for eff in effects {
            worker
                .apply_effect(eff, &cmd_tx, &sup_tx)
                .expect("untracked blob op is dropped, not an error");
        }
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !names.iter().any(|n| n.contains(untracked)),
            "no garbage <uuid> file materialized for an untracked blob doc: {names:?}"
        );
    }

    /// Bug 3 / backpressure recovery: a `StaleSubscriber` eviction notice must
    /// drive `handle_sync_event` to emit a `SyncCommand::Subscribe` for that
    /// document (the re-subscribe that recovers the missed broadcasts). Routed
    /// config-agnostically — verified here on the imperative handler that both
    /// cascades dispatch to.
    #[test]
    fn test_stale_subscriber_event_triggers_resubscribe() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());

        let (sync_cmd_tx, mut sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();

        let backlog_before = worker.intake.depth();
        worker
            .handle_sync_event(
                SyncEvent::StaleSubscriber {
                    document_id: "doc-evicted".to_owned(),
                },
                &suppress_tx,
                &sync_cmd_tx,
            )
            .expect("handle_sync_event must not error on a stale-subscriber notice");

        match sync_cmd_rx.try_recv() {
            Ok(SyncCommand::Subscribe { document_id }) => {
                assert_eq!(document_id, "doc-evicted");
            }
            other => panic!("expected a Subscribe re-subscribe command, got {other:?}"),
        }
        assert!(
            sync_cmd_rx.try_recv().is_err(),
            "a single eviction must emit exactly one re-subscribe"
        );
        // `Subscribe` is not ack-bearing, so the intake-gate backlog is untouched.
        assert_eq!(
            worker.intake.depth(),
            backlog_before,
            "re-subscribe must not bump the ack-bearing intake gate"
        );
    }
}
