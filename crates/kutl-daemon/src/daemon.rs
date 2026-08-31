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
mod signal_catchup;
mod signal_ingest;
mod startup;

use causal::read_clock_skew_ms;
use effects::{encode_delta, write_doc};
use identity::{find_rename_source, refresh_inode};
use session::{IntakeGate, LoopInput};

/// Number of hex characters used for the CRDT agent name (48 bits of entropy).
const AGENT_NAME_HEX_LEN: usize = 12;

/// How long a deferred empty observation waits before the periodic revisit
/// manufactures its second look (see [`SpaceWorker::revisit_pending_empties`]).
/// Comfortably past the watcher debounce, so a rewrite's content half gets
/// every chance to arrive as its own event first; short enough that a real
/// emptying still syncs promptly once the revisit fires.
const EMPTY_CONFIRM_DELAY: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(2));

/// The imperative path's gated edge read: `Ok(None)` when the snapshot's
/// (length, mtime) signature moved under the read — a writer is mid-rewrite
/// and the bytes must not become ops (same gate as the classify edge).
fn read_stable_to_string(abs_path: &Path) -> std::io::Result<Option<String>> {
    let sig_before = classify::file_sig(abs_path);
    let content = std::fs::read_to_string(abs_path)?;
    if let (Some(before), Some(after)) = (sig_before, classify::file_sig(abs_path))
        && before != after
    {
        return Ok(None);
    }
    Ok(Some(content))
}

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
    /// Where this worker records the relay identities it has seen.
    /// `None` resolves to `$KUTL_HOME/known_relays.json`,
    /// which is what production wants — one record per install, shared by every
    /// space worker on it.
    ///
    /// Explicit here rather than read from the environment at the point of use
    /// so a test can give each worker its own file. The alternative — having
    /// tests `set_var("KUTL_HOME")` — mutates process-global state from threads
    /// that are already running, which is unsound in a multithreaded binary,
    /// and would still leave every in-process worker contending on one path.
    pub known_relays_path: Option<PathBuf>,
    /// Use notify's polling backend instead of the platform watcher
    /// (FSEvents/inotify). **A test-suite knob, not a production mode.**
    ///
    /// Exists because the integration suite's own burst load starves macOS
    /// `FSEvents` stream REGISTRATION system-wide — a first `.watch()` measured
    /// at 98–174 s against a ~1 ms baseline, which is what every "timed out
    /// waiting for the watcher" in-suite failure actually was.
    /// The poll backend never talks to
    /// fseventsd, so the suite cannot starve itself. Config-plumbed rather
    /// than env-read for the same soundness reason as `known_relays_path`
    /// above.
    pub poll_watcher: bool,
}

/// A per-space sync worker that orchestrates file watching, sync client, and
/// document management for a single space directory.
pub struct SpaceWorker {
    config: SpaceWorkerConfig,
    /// The pure per-space sync state: the lifecycle/sync fields the core
    /// mutates (clock, registry view, identities, deferred placements). Carved
    /// off `SpaceWorker` so `DaemonCore::handle` can drive it without IO.
    state: SpaceState,
    /// The space's producer flock, held for the worker's whole life (see
    /// the acquisition in [`Self::new`]). Never read — the handle existing
    /// IS the exclusion.
    _producer_lock: std::fs::File,
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
    /// [`SYNC_BACKLOG_HIGH_WATER`] (see [`Self::next_event`]). Also the
    /// watchdog counter. Only the serial loop task touches it — a plain field,
    /// owned by [`IntakeGate`] so both halves of the arithmetic live in one
    /// place.
    intake: IntakeGate,
    /// Relay-reported error frames observed this session — rejections the relay
    /// sent back instead of applying an operation (e.g. a quota refusal). Reset
    /// per session. There is no in-session resend for a rejected operation, so
    /// in one-shot mode a nonzero count means work was NOT delivered and the
    /// run must not report success (see `one_shot_incomplete`).
    relay_errors: u64,
    /// The most recent relay error message, named in the one-shot failure so
    /// the operator sees the cause (e.g. the quota kind and limit), not just a
    /// count.
    last_relay_error: Option<String>,
    /// Times the relay evicted this worker from its space's signal stream for
    /// outbound backpressure, counted for the life of the worker rather than
    /// per session.
    ///
    /// Deliberately NOT a relay error: the eviction is recovered by
    /// re-subscribing, no work is lost, and a one-shot pass that hit one still
    /// completed. That recovery is exactly why the tally has to exist — a
    /// recovered eviction leaves no mark on the outcome, so a space that keeps
    /// outrunning the relay's outbound lane would otherwise look identical to a
    /// healthy one, and the cost would be paid in latency nobody can see.
    signal_stream_evictions: u64,
    /// A persist is pending: an [`Effect::SaveState`] was emitted (e.g. a remote
    /// doc's first materialization recorded a new inode) but not yet written to
    /// disk. Coalesced rather than written inline, because `save_state` rewrites
    /// the WHOLE `state.json` (O(docs)); doing that per materialization is O(docs²)
    /// under a bulk add. Flushed once the loop is
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
    /// Tracked documents observed empty on disk exactly once, awaiting a
    /// second observation before the emptying is committed as ops.
    ///
    /// An in-place rewriter (an editor or agent Write) truncates the file
    /// and then writes the new content through the same inode. A snapshot
    /// read landing inside that window sees a genuinely stable EMPTY file,
    /// and committing it broadcasts delete-everything: every marker-tracked
    /// decision in the document withdraws, and when the content half lands
    /// milliseconds later its headings are new insertions under new
    /// identities — nothing can re-bind them. So the first empty observation
    /// of a document whose known content is non-empty is DEFERRED: the
    /// content write's own event (or the periodic revisit, for a file that
    /// really was emptied) makes the second observation, and only that one
    /// commits. Session-lifetime state — a restart's scan re-reads the real
    /// file and converges without it.
    pending_empty: HashMap<PathBuf, Instant>,
    /// The relay's advertised signing identity, pinned from the current
    /// session's `HandshakeAck.relay_did`. Empty until connected, or
    /// when the relay advertises none. On signal ingest the record's relay
    /// attestation is checked against this DID (advisory — never rejects).
    pinned_relay_did: String,
    /// Every relay signing identity this client has recorded for the configured
    /// relay URL, oldest first (`$KUTL_HOME/known_relays.json`).
    ///
    /// Distinct from [`Self::pinned_relay_did`], and both are needed: the pin is
    /// "who am I talking to *right now*", which is what an incoming
    /// attestation's `relay_did` must match; this is "whose MATERIALIZER
    /// signatures do I honour", which must include earlier keys or every record
    /// signed before a rotation would stop verifying. Loaded at handshake and
    /// empty until then.
    known_relay_dids: Vec<String>,
    /// Whether the connected relay advertises `signal-reseed` — i.e. accepts
    /// client-pushed history. `false` until connected, and
    /// permanently `false` against a relay that serves history but refuses to
    /// store any (kutlhub). Gates only the PUSH half of catch-up; the pull half
    /// is gated on `signal-records`.
    relay_accepts_reseed: bool,
    /// Per-space signal segment store, lazily opened on first
    /// [`SyncEvent::SignalRecord`] ingest and held for the worker's lifetime so
    /// its single-writer `flock` guards this working tree's segments. `None`
    /// until the first record arrives (a space with no signals never opens it).
    signal_store: Option<crate::signal_store::DaemonSignalStore>,
    /// `record_id`s already persisted to the space's signal segments — the
    /// ingest-side dedup set (set-union semantics, mirroring the fold's `seen`
    /// and the relay's re-seed collision check). Seeded from the segments the
    /// first time [`Self::signal_store`] opens, then a duplicate broadcast is an
    /// O(1) no-op instead of a redundant segment append. Empty until the store
    /// opens.
    seen_record_ids: std::collections::HashSet<String>,
    /// The re-seed push in progress, if any (≤100 records per
    /// `SignalReseed`). `None` when nothing is being pushed.
    reseed: Option<ReseedPush>,
}

/// A re-seed push, walked one acked chunk at a time.
///
/// Sending the ENTIRE surplus in one frame would have two
/// consequences, and the second is the serious one: a large backlog is a large
/// WS message; and the relay refuses any batch over `MAX_RESEED_BATCH`
/// (10,000), so a daemon offline long enough would have its whole re-seed
/// rejected wholesale, every time, with no path to recovery — for exactly the
/// deployments re-seed exists to protect.
///
/// Chunked and ACK-GATED, rather than chunked and fired in a burst: one chunk
/// is in flight at a time, and the next goes out only when the relay confirms
/// the last. Firing them all would replace one oversized frame with a hundred
/// small ones arriving faster than the relay admits them, which is the same
/// pressure wearing a different shape.
pub(crate) struct ReseedPush {
    /// `client_ref` of the chunk awaiting its ack. Correlating on it means an
    /// ack for something else — an authored submit, a stale re-seed from a
    /// previous connection — cannot advance this walk.
    in_flight: String,
    /// Chunks not yet sent, in order.
    remaining: std::collections::VecDeque<Vec<kutl_proto::sync::Signal>>,
    /// Total chunks in this push, for the completion log.
    total: usize,
}

impl ReseedPush {
    /// Start a push over `chunks`. Nothing is in flight until the first send.
    pub(crate) fn new(
        remaining: std::collections::VecDeque<Vec<kutl_proto::sync::Signal>>,
        total: usize,
    ) -> Self {
        Self {
            in_flight: String::new(),
            remaining,
            total,
        }
    }

    /// Pop the next chunk, or `None` when the push is drained.
    pub(crate) fn take_next(&mut self) -> Option<Vec<kutl_proto::sync::Signal>> {
        self.remaining.pop_front()
    }

    /// Record the `client_ref` whose ack will release the next chunk.
    pub(crate) fn mark_in_flight(&mut self, client_ref: String) {
        self.in_flight = client_ref;
    }

    /// Is this ack the one this push is waiting on?
    ///
    /// An empty `in_flight` never matches, so an ack arriving before the first
    /// chunk was sent cannot advance the walk.
    pub(crate) fn is_in_flight(&self, client_ref: &str) -> bool {
        !self.in_flight.is_empty() && self.in_flight == client_ref
    }

    /// How many chunks this push was created with.
    pub(crate) fn total_chunks(&self) -> usize {
        self.total
    }
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

        // ONE CRDT producer per space, taken at the door. Every producer —
        // the persistent daemon's worker and the one-shot sync alike —
        // funnels through this constructor, so the lock here is the
        // invariant, not a convention each verb remembers. Two producers
        // with independent in-memory state re-mint each other's
        // materializations as fresh edits, geometrically (measured live:
        // 14 lines → 12M ops inside a minute). The handle is the lock:
        // held for the worker's lifetime, released by the OS if the
        // process dies — no stale-lock state exists to clean, unlike a
        // pidfile.
        let lock_path = config.space_root.join(".kutl").join("producer");
        let Some(producer_lock) = kutl_client::try_lock_exclusive(&lock_path)? else {
            anyhow::bail!(
                "another kutl process is already syncing {} — one producer per space; \
                 stop the running daemon or wait for the sync to finish",
                config.space_root.display()
            );
        };

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
            relay_errors: 0,
            last_relay_error: None,
            signal_stream_evictions: 0,
            state_dirty: false,
            startup_buffer: VecDeque::new(),
            pending_empty: HashMap::new(),
            _producer_lock: producer_lock,
            pinned_relay_did: String::new(),
            known_relay_dids: Vec::new(),
            relay_accepts_reseed: false,
            signal_store: None,
            seen_record_ids: std::collections::HashSet::new(),
            reseed: None,
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

    /// Confirm and commit deferred empty observations whose second look is
    /// due. Called from the session loop's periodic tick.
    ///
    /// The common case never reaches here: a deferral's content half raises
    /// its own event within a debounce tick and commits the full rewrite.
    /// This revisit exists for the file that REALLY was emptied — that write
    /// was its last event, so the second observation must be manufactured or
    /// the emptying would never sync.
    fn revisit_pending_empties(
        &mut self,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) {
        let due: Vec<PathBuf> = self
            .pending_empty
            .iter()
            .filter(|(_, deferred_at)| deferred_at.elapsed() >= EMPTY_CONFIRM_DELAY)
            .map(|(rel, _)| rel.clone())
            .collect();
        for rel_path in due {
            // A path that left the tree resolves its marker without a
            // commit: the removal took its own event path, and a later
            // file at this path is a fresh create that must not inherit
            // the second-observation shortcut.
            if !self.state.file_path(&rel_path).exists() {
                self.pending_empty.remove(&rel_path);
                continue;
            }
            // Re-observe through the normal classify path: still empty →
            // this IS the second observation and the emptying commits;
            // content appeared without its event reaching us → the content
            // commits. Either way the marker resolves.
            match self.classify_file_event(FileEvent::Modified { rel_path }) {
                LoopInput::Core { event, activity } => {
                    self.apply_core_input(event, activity, sync_cmd_tx, suppress_tx);
                }
                LoopInput::ImperativeFile(file_event) => {
                    if let Err(e) = self.handle_file_event(file_event, sync_cmd_tx, suppress_tx) {
                        error!(error = %e, "error revisiting deferred empty");
                    }
                }
                _ => {}
            }
        }
    }

    /// Binary content after the rename detectors declined: route to the
    /// core's blob path exactly as `classify_file_event` does (size cap +
    /// inode refresh at this edge, then the core).
    fn handle_binary_modified(
        &mut self,
        rel_path: &Path,
        abs_path: &Path,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        let bytes = match std::fs::read(abs_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(e).with_context(|| format!("failed to read {}", abs_path.display()));
            }
        };
        if let LoopInput::Core { event, .. } =
            self.classify_blob_bytes(rel_path.to_path_buf(), bytes, abs_path)
        {
            let effects = DaemonCore::handle(&mut self.state, event);
            for effect in effects {
                self.apply_effect(effect, sync_cmd_tx, suppress_tx)?;
            }
        }
        Ok(())
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
                // Only the directory-shaped expansion routes here
                // (`classify_file_event` requires an untracked path with
                // tracked children); a plain file removal is core-routed —
                // reaching the else is a routing bug.
                if !self.state.file_identity.contains_key(&rel_path)
                    && self.has_tracked_children(&rel_path)
                {
                    self.expand_removed_dir(&rel_path, sync_cmd_tx, suppress_tx)
                } else {
                    error!(
                        rel_path = %rel_path.display(),
                        "plain file removal reached the imperative handler (routing bug)"
                    );
                    debug_assert!(false, "plain file removals are core-routed");
                    Ok(())
                }
            }
            // Renames are core-routed unconditionally (`classify_file_event`
            // maps `FileEvent::Renamed` to `LoopInput::Core`) — reaching this
            // handler is a routing bug.
            FileEvent::Renamed { old_path, new_path } => {
                error!(
                    old_path = %old_path.display(),
                    new_path = %new_path.display(),
                    "rename reached the imperative handler (routing bug)"
                );
                debug_assert!(false, "renames are core-routed");
                Ok(())
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
    /// ONE transition: all three stores (identity maps — `move_identity`
    /// carries `last_synced` along — `DiskShadow`, the gamma placement
    /// lattice), the floor-read-BEFORE-stamp ordering, and the
    /// `RenameDocument` emit. This
    /// trio is the most failure-prone seam here: a stale
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

        // Reject an adoption whose target is a case-variant of ANOTHER tracked
        // document — the same rule the paired-rename handler and the untracked
        // new-file path enforce. Without this guard, a rename delivered as a
        // bare create (no event pairing) adopts exactly the collision both
        // siblings reject, and the colliding mapping propagates cluster-wide.
        if let Some(existing) = crate::case_collision::find_case_variant(
            rel_path,
            self.state
                .file_identity
                .keys()
                .filter(|p| p.as_path() != old_path)
                .map(PathBuf::as_path),
        ) {
            error!(
                old_path = %old_path.display(),
                new_path = %rel_path.display(),
                existing_path = %existing.display(),
                "case_collision_rejected: rename target would collide with tracked document, ignoring event"
            );
            return Ok(true);
        }

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

    /// Drain a not-yet-flushed concurrent LOCAL rename of `document_id`
    /// before applying a remote lifecycle op for it.
    ///
    /// The race: the user renames a file and, within the watcher's debounce
    /// window, a remote rename for the same document arrives. Applying the
    /// remote op first conforms the relocated file to the authoritative path
    /// (`conform_relocated_or_materialize`), destroying the only evidence of
    /// the local rename — its watcher event then finds nothing on disk (or is
    /// suppressed as a conform echo) and the rename NEVER reaches the relay.
    /// One of two concurrent renames is silently lost cluster-wide, and under
    /// clock skew the arbitration winner flips with the interleaving.
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
    /// and flip the arbitration — a skew-defeating re-emit that silently
    /// changes the cluster-wide winner.
    ///
    /// Best-effort: errors are logged and the remote apply proceeds (the
    /// conform backstop still reconciles the disk).
    /// Pre-dispatch drain: surface an undrained concurrent LOCAL rename
    /// BEFORE the core gates/`recv`s an incoming event that would mutate the
    /// same document's placement — so the local rename reaches the relay with
    /// its honest origin stamp and the lattice arbitrates BOTH, instead of the
    /// conform destroying the rename's only evidence (the rename swallow).
    ///
    /// Two triggers:
    /// - a remote RENAME of the doc, and
    /// - a CONFLICT-PATH self-correction ack:
    ///   a conflict-infix effective path makes `handle_lifecycle_ack`
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

        let content = match read_stable_to_string(&abs_path) {
            Ok(Some(c)) => c,
            // Snapshot moved under the read — mid-rewrite. Dropping is
            // self-correcting: the mutating write raises its own event (see
            // the classify edge's stability gate).
            Ok(None) => {
                debug!(
                    rel_path = %rel_path.display(),
                    "snapshot changed under the read; dropping torn event"
                );
                return Ok(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                return self.handle_binary_modified(rel_path, &abs_path, sync_cmd_tx, suppress_tx);
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
            // untracked-file event would be pure allocation.
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
        // (we just wrote it). Inert when the occupant's file genuinely was
        // destroyed at `target` (the overwrite case, shadow path == target).
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
    /// has truly LEFT the space is unregistered through the removal path. Two
    /// per-child guards make the expansion safe:
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

    /// Handle a sync event from the relay. Only `StaleSubscriber` recovery is
    /// served here; every content and
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
                // Routing-bug arm (all content is core-routed): the catch-up
                // author snapshot is threaded via `classify_sync_event`, not here.
                author_by_agent_snapshot: _,
            } => {
                // Advance the origin clock past every observed remote stamp, so a
                // lifecycle/edit op this daemon emits afterward is causally after
                // them.
                for m in &metadata {
                    self.observe_remote_hlc(Some(m));
                }
                // BOTH content modes are core-routed (text via the CRDT
                // merge, blobs via the pure LWW merge) — reaching this handler
                // is a routing bug.
                let _ = (ops, content_hash, content_mode);
                error!(%document_id, "remote ops reached the imperative handler (routing bug)");
                debug_assert!(false, "remote ops are core-routed");
            }
            // Lifecycle events route through the pure core (classify_lifecycle_event
            // returns LoopInput::Core unconditionally) — reaching this handler
            // is a routing bug.
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
            SyncEvent::StaleSignalStream { space_id, reason } => {
                self.handle_stale_signal_stream(&space_id, &reason, sync_cmd_tx)?;
            }
            SyncEvent::SpaceDocuments { .. }
            | SyncEvent::Connected { .. }
            | SyncEvent::Disconnected => {
                // Only expected during session setup; ignore if received later.
            }
            SyncEvent::SignalRecord(_) | SyncEvent::SignalPage(_) | SyncEvent::SignalAck { .. } => {
                // Signal records, backlog pages and acks are routed via
                // dedicated LoopInput arms (see `classify_sync_event`), never
                // the imperative handler — reaching this is a routing bug.
                error!("signal event reached the imperative handler (routing bug)");
                debug_assert!(false, "signal events are ingest-routed");
            }
            SyncEvent::HandshakeRejected {
                message,
                auth_failed,
            } => {
                error!(
                    detail = self.handshake_rejection_detail(&message, auth_failed),
                    "relay refused the handshake"
                );
            }
            SyncEvent::Error {
                message,
                auth_failed,
            } => {
                self.record_relay_error(&message, auth_failed);
            }
        }

        Ok(())
    }

    /// Recover from a relay backpressure eviction. The relay evicted
    /// us from `document_id` because our bounded outbound `data` lane overflowed
    /// — we fell behind a broadcast flood.
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

    /// Backpressure recovery for the signal stream — the same move
    /// [`Self::handle_stale_subscriber`] makes for a document, and it is needed
    /// for the same reason: the relay has already removed the subscription, so
    /// the notice is the only cue that this daemon has stopped receiving.
    ///
    /// Left unhandled the loss is total and silent — no broadcast of any record
    /// kind arrives for the rest of the session, while document sync carries on
    /// normally, so the space looks healthy and merely quiet.
    ///
    /// The re-subscribe resumes from the persisted cursor, so recovery is a
    /// backlog page rather than a full re-walk. Self-pacing needs no backoff:
    /// this notice shares the bounded event channel with the broadcasts, so a
    /// loop far enough behind to be evicted has a full channel, and the read
    /// task blocks until the backlog drains — by which point the re-subscribe's
    /// page lands without immediately earning a second eviction.
    ///
    /// # Errors
    ///
    /// Returns an error if the space's signal store cannot be opened or its
    /// cursor cannot be read.
    fn handle_stale_signal_stream(
        &mut self,
        space_id: &str,
        reason: &str,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) -> Result<()> {
        self.signal_stream_evictions += 1;
        crate::metrics_calls::record_signal_stream_eviction(space_id);
        warn!(
            %space_id,
            reason,
            evictions = self.signal_stream_evictions,
            "relay evicted this daemon's signal stream (outbound backpressure); re-subscribing to recover"
        );
        self.start_signal_catch_up(sync_cmd_tx)
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

    /// The config for a minimal, network-free test worker over `space_root` —
    /// the single literal shared by [`test_worker`] and the second-producer
    /// refusal (which needs the raw `SpaceWorker::new` error, not a built
    /// worker).
    pub(super) fn test_worker_config(space_root: PathBuf) -> SpaceWorkerConfig {
        SpaceWorkerConfig {
            poll_watcher: true,
            space_root,
            author_did: "did:test".to_owned(),
            relay_url: "ws://127.0.0.1:1/none".to_owned(),
            space_id: "3314f713-09a4-40c6-8910-0a2ea70c5c53".to_owned(),
            signing_key: None,
            one_shot: false,
            display_name: "test".to_owned(),
            ready: None,
            cancel: CancellationToken::new(),
            known_relays_path: None,
        }
    }

    /// Build a minimal, network-free `SpaceWorker` over `space_root` for unit
    /// tests of the in-memory placement/identity handlers. Shared with the
    /// child modules' test mods (`identity`), hence `pub(super)`.
    pub(super) fn test_worker(space_root: PathBuf) -> SpaceWorker {
        SpaceWorker::new(test_worker_config(space_root)).expect("build test worker")
    }

    /// Drive the real mint path: write `name` with `content`, classify the
    /// Modified, and apply the resulting core event, so the worker tracks a
    /// document whose core content matches the disk file.
    fn seed_doc_via_classify(worker: &mut SpaceWorker, dir: &Path, name: &str, content: &str) {
        std::fs::write(dir.join(name), content).unwrap();
        let input = worker.classify_file_event(FileEvent::Modified {
            rel_path: PathBuf::from(name),
        });
        let LoopInput::Core { event, .. } = input else {
            panic!("seeding {name} did not route Core");
        };
        let _effects = DaemonCore::handle(&mut worker.state, event);
    }

    /// The in-place rewrite tear: an editor or agent Write truncates the file
    /// and writes the content through the same inode, and a snapshot read in
    /// the window sees a stable EMPTY file. Committing that broadcasts
    /// delete-everything (measured live: every marker-tracked decision
    /// withdrew and the reborn headings minted fresh identities). The first
    /// empty observation of a tracked, non-empty document must defer; the
    /// second — the file really is empty — commits.
    #[test]
    fn test_empty_rewrite_defers_once_then_commits() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        seed_doc_via_classify(
            &mut worker,
            dir.path(),
            "doc.md",
            "# Dinner\n\n## ? Appetizer\n",
        );

        std::fs::write(dir.path().join("doc.md"), "").unwrap();
        let first = worker.classify_file_event(FileEvent::Modified {
            rel_path: PathBuf::from("doc.md"),
        });
        assert!(
            matches!(first, LoopInput::Activity),
            "first empty observation must defer, not commit"
        );
        assert!(worker.pending_empty.contains_key(Path::new("doc.md")));

        let second = worker.classify_file_event(FileEvent::Modified {
            rel_path: PathBuf::from("doc.md"),
        });
        let LoopInput::Core {
            event: Event::FileModified { content, .. },
            ..
        } = second
        else {
            panic!("second empty observation must commit the emptying");
        };
        assert_eq!(content, Some(Vec::new()));
        assert!(worker.pending_empty.is_empty());
    }

    /// The common resolution of a deferral: the rewrite's content half lands,
    /// its own event fires, and the full new content commits as ONE diff —
    /// the marker must clear so a later real emptying defers afresh.
    #[test]
    fn test_content_after_deferred_empty_commits_and_clears_marker() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        seed_doc_via_classify(
            &mut worker,
            dir.path(),
            "doc.md",
            "# Dinner\n\n## ? Appetizer\n",
        );

        std::fs::write(dir.path().join("doc.md"), "").unwrap();
        let deferred = worker.classify_file_event(FileEvent::Modified {
            rel_path: PathBuf::from("doc.md"),
        });
        assert!(matches!(deferred, LoopInput::Activity));

        std::fs::write(dir.path().join("doc.md"), "# Dinner v2\n\n## ? Appetizer\n").unwrap();
        let input = worker.classify_file_event(FileEvent::Modified {
            rel_path: PathBuf::from("doc.md"),
        });
        let LoopInput::Core {
            event: Event::FileModified { content, .. },
            ..
        } = input
        else {
            panic!("content after a deferred empty must commit");
        };
        assert_eq!(content, Some(b"# Dinner v2\n\n## ? Appetizer\n".to_vec()));
        assert!(worker.pending_empty.is_empty());
    }

    /// Claude's Write tool and atomic-save editors replace a file by
    /// writing a VISIBLE sibling temp and renaming it onto the target
    /// (measured by inotify: `CREATE` tmp → `MODIFY` → `CLOSE_WRITE` →
    /// `MOVED_FROM` tmp → `MOVED_TO` target). Routed as a rename, that pair
    /// hits the occupied-path collision machinery and DISPLACES the
    /// incumbent document — measured live as delete-everything ops, every
    /// marker-tracked decision withdrawn. An untracked source landing on a
    /// tracked target is an EDIT of the incumbent and must classify as one.
    #[test]
    fn test_atomic_save_rename_classifies_as_edit_of_the_incumbent() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        seed_doc_via_classify(
            &mut worker,
            dir.path(),
            "doc.md",
            "# Dinner\n\n## ? Appetizer\n",
        );

        // The rename's disk half already happened when the event arrives:
        // the temp is gone, the target carries the new content.
        std::fs::write(dir.path().join("doc.md"), "# Dinner v2\n\n## ? Appetizer\n").unwrap();
        let input = worker.classify_file_event(FileEvent::Renamed {
            old_path: PathBuf::from("doc.md.tmp.77.abcdef"),
            new_path: PathBuf::from("doc.md"),
        });
        let LoopInput::Core {
            event: Event::FileModified { rel, content, .. },
            ..
        } = input
        else {
            panic!("atomic-save rename must classify as an edit, not a rename");
        };
        assert_eq!(rel, PathBuf::from("doc.md"));
        assert_eq!(content, Some(b"# Dinner v2\n\n## ? Appetizer\n".to_vec()));
    }

    /// A rename whose SOURCE is tracked is a real move and must keep the
    /// rename semantics — only the untracked-source atomic-save shape folds
    /// into an edit.
    #[test]
    fn test_tracked_source_rename_stays_a_rename() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        seed_doc_via_classify(&mut worker, dir.path(), "doc.md", "# Dinner\n");

        let input = worker.classify_file_event(FileEvent::Renamed {
            old_path: PathBuf::from("doc.md"),
            new_path: PathBuf::from("renamed.md"),
        });
        assert!(
            matches!(
                input,
                LoopInput::Core {
                    event: Event::FileRenamed { .. },
                    ..
                }
            ),
            "a tracked-source rename must stay a rename"
        );
    }

    /// A brand-new empty file is a legitimate create, not a torn rewrite —
    /// it mints immediately.
    #[test]
    fn test_new_empty_file_mints_without_deferral() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        std::fs::write(dir.path().join("fresh.md"), "").unwrap();
        let input = worker.classify_file_event(FileEvent::Modified {
            rel_path: PathBuf::from("fresh.md"),
        });
        assert!(
            matches!(input, LoopInput::Core { .. }),
            "an untracked empty file must mint, not defer"
        );
        assert!(worker.pending_empty.is_empty());
    }

    /// A file that REALLY was emptied never raises a second event, so the
    /// periodic revisit makes the second observation and commits it.
    #[test]
    fn test_revisit_commits_a_still_empty_deferral() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        seed_doc_via_classify(
            &mut worker,
            dir.path(),
            "doc.md",
            "# Dinner\n\n## ? Appetizer\n",
        );

        std::fs::write(dir.path().join("doc.md"), "").unwrap();
        let deferred = worker.classify_file_event(FileEvent::Modified {
            rel_path: PathBuf::from("doc.md"),
        });
        assert!(matches!(deferred, LoopInput::Activity));

        // Not yet due: nothing happens.
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        worker.revisit_pending_empties(&cmd_tx, &sup_tx);
        assert!(worker.pending_empty.contains_key(Path::new("doc.md")));

        // Age the marker past the confirm delay, then revisit: the emptying
        // commits and the marker clears.
        *worker
            .pending_empty
            .get_mut(Path::new("doc.md"))
            .expect("marker present") = Instant::now()
            .checked_sub(2 * EMPTY_CONFIRM_DELAY)
            .expect("test clock predates the confirm delay");
        worker.revisit_pending_empties(&cmd_tx, &sup_tx);
        assert!(worker.pending_empty.is_empty());
        let doc_id = worker
            .state
            .file_identity
            .get(Path::new("doc.md"))
            .expect("doc tracked")
            .document_uuid
            .clone();
        let doc = worker.state.get_doc(&doc_id).expect("doc in store");
        assert_eq!(
            doc.content(),
            "",
            "revisit must have committed the emptying"
        );
    }

    /// The single-producer invariant, enforced at the door rather than
    /// discovered by pidfile heuristics: a space (one root, one `.kutl`)
    /// admits ONE CRDT producer at a time. A second daemon or a one-shot
    /// sync landing on a producer-held space must refuse loudly — two
    /// producers with independent in-memory state re-mint each other's
    /// materializations geometrically (measured live: 14 lines → 12M ops
    /// inside a minute).
    #[test]
    fn test_second_producer_on_a_space_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let _holder = test_worker(dir.path().to_path_buf());

        let err = SpaceWorker::new(test_worker_config(dir.path().to_path_buf()));
        let msg = match err {
            Ok(_) => panic!("a second producer on a held space must be refused"),
            Err(e) => e.to_string(),
        };
        assert!(msg.contains("one producer per space"), "got: {msg}");
    }

    /// The lock is the worker's lifetime, not a breadcrumb: dropping the
    /// holder releases it, and the OS releases it on process death — there
    /// is no stale-lock state to clean.
    #[test]
    fn test_producer_lock_releases_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let holder = test_worker(dir.path().to_path_buf());
        drop(holder);
        // A successor acquires cleanly.
        let _successor = test_worker(dir.path().to_path_buf());
    }

    /// Build the concurrent-rename race precondition: `doc` is tracked at `foo.md`, but the
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

    /// A CONFLICT-PATH self-correction ack drains a pending
    /// unflushed local rename of the same doc BEFORE the core applies the
    /// correction — the correction's `move_identity` onto the arbitrated
    /// conflict path would otherwise destroy the rename's only evidence (the
    /// rename swallow). A PLAIN
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
            "a plain success ack must not trigger the drain"
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

    /// An incoming remote rename must not destroy the evidence of a
    /// concurrent local rename the watcher has not drained yet. The daemon
    /// drains the local rename FIRST — emitting its `RenameDocument` with a
    /// stamp taken BEFORE the incoming stamp is `recv`'d into the clock — and
    /// only then applies the remote op (which, being fresher here, conforms
    /// the file to the authoritative path). Without the drain, the conform
    /// relocates the file and the local rename never reaches the relay: one of
    /// two concurrent renames is silently lost cluster-wide, and under clock
    /// skew the arbitration winner flips with the interleaving.
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
        // Mirror the production dispatch (the event loop's Core arm): the
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
        // must carry its honest origin stamp, not a skew-defeating lift.
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

    /// Loser direction: when the drained local rename is FRESHER than the
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
        // Mirror the production dispatch (the event loop's Core arm): the
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

        // Untracked UUID + non-empty blob ops: must be skipped — no `<uuid>`
        // file may appear. Routed through the
        // core's blob LWW merge (the production path).
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
                author_by_agent_snapshot: std::collections::HashMap::new(),
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

    /// Backpressure recovery: a `StaleSubscriber` eviction notice must
    /// drive `handle_sync_event` to emit a `SyncCommand::Subscribe` for that
    /// document (the re-subscribe that recovers the missed broadcasts). Verified
    /// here on the imperative handler `handle_sync_event`.
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

    /// The same backpressure recovery on the SIGNAL lane. The relay REMOVES an
    /// evicted subscriber, and nothing else re-adds it: a session that treats
    /// the notice as a log line receives no further broadcast of any signal
    /// kind for as long as it stays connected. Since the relay yields whenever
    /// the shared outbound lane is merely near full, ordinary document traffic
    /// is enough to trigger it.
    ///
    /// The re-subscribe resumes from the persisted cursor, so recovery costs a
    /// backlog page rather than a full re-walk.
    #[test]
    fn test_stale_signal_stream_event_triggers_resubscribe() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let space_id = worker.config.space_id.clone();

        let (sync_cmd_tx, mut sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();

        worker
            .handle_sync_event(
                SyncEvent::StaleSignalStream {
                    space_id: space_id.clone(),
                    reason: "outbound lane full".to_owned(),
                },
                &suppress_tx,
                &sync_cmd_tx,
            )
            .expect("handle_sync_event must not error on a stale-signal-stream notice");

        match sync_cmd_rx.try_recv() {
            Ok(SyncCommand::SubscribeSignals { space_id: got, .. }) => {
                assert_eq!(got, space_id);
            }
            other => panic!("expected a SubscribeSignals re-subscribe command, got {other:?}"),
        }
        assert!(
            sync_cmd_rx.try_recv().is_err(),
            "a single eviction must emit exactly one re-subscribe"
        );
    }

    /// GUARD (mid-group resume): the re-subscribe floor is one millisecond
    /// BEFORE the persisted cursor. Live ingest advances the cursor to each
    /// broadcast record's HLC, so an eviction or restart can leave it
    /// mid-`physical_ms`-group — and the relay's serve filter is coarse, so
    /// resuming from the raw cursor would exclude the unseen rest of that
    /// group forever. Re-serving the group is idempotent; losing a sibling
    /// is permanent.
    #[test]
    fn test_resubscribe_floor_rewinds_below_the_persisted_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        // ensure_signal_store keys segments by UUID.
        worker.config.space_id = uuid::Uuid::from_u128(0xF00D).to_string();

        let (sync_cmd_tx, mut sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();

        // Persist a MID-GROUP cursor, as ingesting a live record does: a
        // nonzero logical component inside millisecond 500.
        worker.ensure_signal_store().expect("open the signal store");
        let seg_dir = worker
            .signal_store
            .as_ref()
            .expect("store open")
            .dir()
            .to_path_buf();
        // The store creates its directory lazily on first append; the cursor
        // write needs it now.
        std::fs::create_dir_all(&seg_dir).expect("create segment dir");
        kutl_signals::catchup::save_cursor(
            &seg_dir,
            &kutl_proto::sync::Hlc {
                physical_ms: 500,
                logical: 3,
                actor: vec![0u8; 16],
            },
        )
        .expect("persist the cursor");

        worker
            .start_signal_catch_up(&sync_cmd_tx)
            .expect("start catch-up");

        match sync_cmd_rx.try_recv() {
            Ok(SyncCommand::SubscribeSignals { cursor, .. }) => {
                let floor = cursor.expect("a persisted cursor yields a floor");
                assert_eq!(
                    floor.physical_ms, 499,
                    "the floor must sit one ms below the cursor's group"
                );
                assert_eq!(floor.logical, 0, "the floor is synthetic");
            }
            other => panic!("expected SubscribeSignals, got {other:?}"),
        }
    }

    /// Evictions are tallied for the life of the worker, not the session.
    ///
    /// Recovery is what makes the tally necessary: once the re-subscribe works,
    /// an eviction leaves no trace in the outcome — the records arrive, the
    /// space converges, the sync reports success. A space repeatedly outrunning
    /// the relay's outbound lane would then be indistinguishable from a healthy
    /// one, and the fix would have turned a loud failure into a silent cost.
    #[test]
    fn test_signal_stream_evictions_are_tallied() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let space_id = worker.config.space_id.clone();

        let (sync_cmd_tx, _sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();

        assert_eq!(
            worker.signal_stream_evictions, 0,
            "a fresh worker has been evicted from nothing"
        );

        for _ in 0..2 {
            worker
                .handle_sync_event(
                    SyncEvent::StaleSignalStream {
                        space_id: space_id.clone(),
                        reason: "outbound lane full".to_owned(),
                    },
                    &suppress_tx,
                    &sync_cmd_tx,
                )
                .expect("handle_sync_event must not error on a stale-signal-stream notice");
        }

        assert_eq!(
            worker.signal_stream_evictions, 2,
            "every eviction counts, including ones the re-subscribe recovered from"
        );
    }
}

#[cfg(test)]
mod reseed_push_tests {
    use super::ReseedPush;

    fn chunks(sizes: &[usize]) -> std::collections::VecDeque<Vec<kutl_proto::sync::Signal>> {
        sizes
            .iter()
            .map(|n| vec![kutl_proto::sync::Signal::default(); *n])
            .collect()
    }

    /// The walk drains IN ORDER and reports exhaustion, so the caller knows when
    /// to stop rather than inferring it.
    #[test]
    fn test_reseed_push_drains_in_order_then_reports_empty() {
        let mut push = ReseedPush::new(chunks(&[100, 100, 7]), 3);
        assert_eq!(push.total_chunks(), 3);
        assert_eq!(push.take_next().map(|c| c.len()), Some(100));
        assert_eq!(push.take_next().map(|c| c.len()), Some(100));
        assert_eq!(push.take_next().map(|c| c.len()), Some(7));
        assert!(
            push.take_next().is_none(),
            "a drained push must say so — the caller ends the walk on this"
        );
    }

    /// **Only the ack for the chunk IN FLIGHT advances the walk.**
    ///
    /// The daemon receives acks for everything it submits, not just re-seeds,
    /// and a stale chunk's ack can arrive after a reconnect. Advancing on any
    /// ack would send the next chunk before the relay confirmed the last, which
    /// is the burst this design exists to avoid — and, worse, would let an
    /// unrelated failure look like a re-seed refusal.
    #[test]
    fn test_only_the_in_flight_ack_matches() {
        let mut push = ReseedPush::new(chunks(&[1, 1]), 2);
        assert!(
            !push.is_in_flight("anything"),
            "nothing is in flight before the first chunk is sent, so no ack may \
             advance the walk"
        );

        let _ = push.take_next();
        push.mark_in_flight("ref-a".to_owned());
        assert!(push.is_in_flight("ref-a"));
        assert!(
            !push.is_in_flight("ref-b"),
            "another submit's ack must not release the next chunk"
        );

        let _ = push.take_next();
        push.mark_in_flight("ref-b".to_owned());
        assert!(
            !push.is_in_flight("ref-a"),
            "the PREVIOUS chunk's ack must not match once it has been superseded \
             — a duplicate would otherwise skip a chunk"
        );
        assert!(push.is_in_flight("ref-b"));
    }
}
