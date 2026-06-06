//! Per-space sync worker: event loop orchestrating watcher, sync client, and documents.
//!
//! Each [`SpaceWorker`] bridges files on disk with the relay for a single space:
//! - Watching for local file changes → diffing into CRDT ops → sending to relay
//! - Receiving remote ops from relay → merging into CRDT → writing to disk

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use kutl_core::{Hlc, HlcClock};
use kutl_proto::protocol::{ABSOLUTE_BLOB_MAX, is_blob_mode};
use kutl_proto::sync::ChangeMetadata;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::SafeRelayPath;
use crate::blob_state::{BlobState, BlobStateMap, sha256_bytes, sha256_hex};
use crate::bridge;
use crate::client::{self, SyncCommand, SyncEvent};
use crate::documents::DocumentManager;
use crate::reconcile::{self, ReconcileInputs, StartupAction};
use crate::state::DaemonState;
use crate::watcher::{self, FileEvent, FileWatcher, Suppression};

/// Outcome of a single session, used to decide whether to reconnect or exit.
enum SessionOutcome {
    /// Relay disconnected or channels closed — reconnect.
    Disconnected,
    /// Cancellation requested — shut down gracefully.
    Shutdown,
    /// Relay explicitly rejected authentication — do not retry.
    AuthRejected(String),
}

/// Maximum reconnect backoff delay.
const MAX_BACKOFF: Duration = kutl_core::std_duration(kutl_core::SignedDuration::from_secs(30));
/// Initial reconnect delay.
const INITIAL_BACKOFF: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_millis(500));
/// Multiplier applied to the reconnect backoff after each failed attempt
/// (exponential growth, capped at [`MAX_BACKOFF`]).
const BACKOFF_MULTIPLIER: u32 = 2;
/// Channel capacity for sync events, commands, and file events.
const CHANNEL_CAPACITY: usize = 64;

/// Number of hex characters used for the CRDT agent name (48 bits of entropy).
const AGENT_NAME_HEX_LEN: usize = 12;

/// Idle timeout for one-shot sync mode: exit after this much inactivity.
const ONE_SHOT_IDLE_TIMEOUT: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(2));

/// How often the event loop refreshes the per-space `/metrics` gauges (queue
/// depth, blob backlog, progress staleness). `tokio::time::interval` fires its
/// first tick immediately, so the gauges register as soon as a session starts;
/// thereafter they refresh on this cadence so the staleness gauge keeps
/// climbing while the daemon is idle.
const METRICS_EMIT_INTERVAL: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(10));

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
    doc_manager: DocumentManager,
    /// Session-scoped CRDT agent name (short random alphanumeric string).
    ///
    /// Each daemon instance gets a unique agent name so that concurrent
    /// writers (even with the same DID) never collide on CRDT sequence
    /// numbers. The raw DID is preserved in `config.author_did` for
    /// metadata attribution.
    ///
    /// diamond-types limits agent names to 50 UTF-8 bytes.
    agent_name: String,
    /// Last synced version per document, for computing deltas.
    last_synced: HashMap<PathBuf, Vec<usize>>,
    /// Last-synced state for binary files (LWW).
    blob_state: BlobStateMap,
    /// Maps watched file paths to their (`document_uuid`, inode) for rename detection.
    /// When a file disappears and a new file appears with the same inode, the daemon
    /// recognizes it as a rename rather than a delete + create.
    file_identity: HashMap<PathBuf, FileIdentity>,
    /// Reverse mapping from UUID to relative path.
    uuid_to_path: HashMap<String, PathBuf>,
    /// Local daemon state cache persisted to `.kutl/state.json`.
    state: DaemonState,
    /// Origin hybrid-logical clock for lifecycle/edit stamps this daemon
    /// produces. A `Mutex` (not `RefCell`) so the spawned daemon future stays
    /// `Send` — it holds `&self` across awaits, which requires `SpaceWorker:
    /// Sync`. `make_metadata` is `&self` (called from `&self` contexts like
    /// `register_and_subscribe`), so the clock can't live behind `&mut self`
    /// without a cascade; the lock is uncontended (the event loop serializes
    /// access) and the critical sections are panic-free. The persisted floor is
    /// synced from the clock by [`Self::save_state`].
    hlc: Mutex<HlcClock>,
    /// Wall-clock skew in milliseconds applied to every HLC physical-time
    /// reading this daemon takes, from `KUTL_CLOCK_SKEW_MS` (default 0). A test
    /// seam for reproducibility under clock skew: two daemons
    /// given opposing skews disagree on physical time, so a passing convergence
    /// proves origin-HLC ordering does not depend on whose wall clock is ahead.
    /// Outside tests the env var is unset and this is 0 (no effect).
    clock_skew_ms: i64,
    /// Per-document HLC of the most recent lifecycle op (register/rename/
    /// unregister) this daemon has *applied* — whether produced locally or
    /// received from the relay. A remote lifecycle event is applied only when
    /// its HLC is causally newer than this; an older one is a stale echo of a
    /// superseded op and is dropped. This is what converges concurrent
    /// rename/rename and rename/delete: the loser of a race receives the
    /// winner's higher-HLC broadcast and applies it, while a local delete's HLC
    /// blocks a stale registration from resurrecting the file. In-memory and
    /// session-scoped; the startup reconcile is the cross-restart backstop.
    lifecycle_hlc: HashMap<String, Hlc>,
    /// Documents the relay has placed at a path currently held by a *different*
    /// live document, deferred until that path frees (path-arbitration
    /// conflict-copy). The daemon never displaces an occupant on its own — the
    /// relay always issues the occupant's move (its own rename, a displacement
    /// broadcast, or a loser-correction), and [`Self::drain_pending`] materializes
    /// the waiting document once its target is vacated. Maps document id → the
    /// effective path it is waiting to occupy. In-memory and session-scoped.
    ///
    /// Stores the validated [`SafeRelayPath`] (not a bare `PathBuf`) so `drain`
    /// can place the document directly without re-parsing — the validity proof is
    /// carried, not discarded and re-derived.
    pending_placements: HashMap<String, SafeRelayPath>,
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
}

/// Tracks the UUID and inode assigned to a file path in the relay registry.
///
/// The inode enables rename detection on platforms (e.g. macOS `FSEvents`)
/// that don't emit paired rename events. When a "new" file appears whose
/// inode matches an existing identity, the daemon treats it as a rename
/// rather than a delete + create.
#[derive(Debug, Clone)]
struct FileIdentity {
    /// UUID assigned to this document in the relay registry.
    document_uuid: String,
    /// Inode at registration time (None on non-Unix or if stat failed).
    inode: Option<u64>,
}

impl SpaceWorker {
    /// Create a new space worker from configuration.
    pub fn new(config: SpaceWorkerConfig) -> Result<Self> {
        let agent_name = generate_agent_name();
        let mut doc_manager = DocumentManager::new(config.space_root.clone())?;
        doc_manager.scan_existing()?;

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

        Ok(Self {
            config,
            doc_manager,
            agent_name,
            last_synced: HashMap::new(),
            blob_state,
            file_identity,
            uuid_to_path,
            state,
            hlc: Mutex::new(clock),
            clock_skew_ms: read_clock_skew_ms(),
            lifecycle_hlc: HashMap::new(),
            pending_placements: HashMap::new(),
            last_progress: Instant::now(),
            blob_backlog: Arc::new(AtomicI64::new(0)),
        })
    }

    /// Current wall-clock millis with this daemon's test skew applied. The single
    /// physical-time source for HLC stamping, so an injected skew shifts every
    /// stamp consistently. Saturates rather than wrapping at the u64 bounds.
    fn skewed_now_ms_u64(&self) -> u64 {
        kutl_core::now_ms_u64().saturating_add_signed(self.clock_skew_ms)
    }

    /// Path to the `.kutl` directory for this space.
    fn kutl_dir(&self) -> PathBuf {
        self.config.space_root.join(".kutl")
    }

    /// The inode currently recorded for a tracked path, if any.
    fn recorded_inode(&self, rel_path: &Path) -> Option<u64> {
        self.file_identity.get(rel_path).and_then(|id| id.inode)
    }

    /// Find a non-hidden file under the space root carrying `inode`, if any.
    ///
    /// Used to tell a genuinely-deleted document from one that was locally
    /// *relocated* (a concurrent rename), where the file still exists at a new
    /// path carrying the same inode. `None` inode never matches.
    fn space_file_with_inode(&self, inode: Option<u64>) -> Option<PathBuf> {
        let target = inode?;
        let root = &self.config.space_root;
        walkdir::WalkDir::new(root)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.file_type().is_file())
            .find_map(|e| {
                let rel = e.path().strip_prefix(root).ok()?.to_path_buf();
                if watcher::should_ignore(&rel) {
                    return None;
                }
                (crate::inode::get_inode(e.path()) == Some(target)).then_some(rel)
            })
    }

    /// Get the UUID for a file, or generate and register a new one.
    ///
    /// Records the inode for rename detection and persists the mapping.
    /// Local paths from the file watcher are validated through `SafeRelayPath`
    /// before storage — they should always pass since they're relative paths
    /// within the space root.
    fn get_or_create_uuid(&mut self, rel_path: &Path) -> String {
        if let Some(identity) = self.file_identity.get(rel_path) {
            return identity.document_uuid.clone();
        }

        let uuid = uuid::Uuid::new_v4().to_string();
        let safe_path = SafeRelayPath::new(&rel_path_to_string(rel_path))
            .expect("local file paths within space root must be valid");
        // A locally-created file is not yet relay-confirmed; it becomes
        // confirmed when the relay acknowledges its registration.
        self.register_identity(safe_path, uuid.clone(), false);
        uuid
    }

    /// Look up the relative path for a document UUID.
    fn path_for_uuid(&self, uuid: &str) -> Option<&PathBuf> {
        self.uuid_to_path.get(uuid)
    }

    /// Resolve the relative path for a document UUID, falling back to
    /// treating the document ID as a relay-supplied path (validated via
    /// [`SafeRelayPath`]) for backwards compatibility with pre-UUID documents.
    fn resolve_path(&self, document_id: &str) -> Result<PathBuf> {
        match self.path_for_uuid(document_id) {
            Some(path) => Ok(path.clone()),
            None => Self::resolve_legacy_path(document_id),
        }
    }

    /// Pre-UUID backwards-compat: treat the `document_id` itself as a relay path.
    /// Reached only for an UNTRACKED id — and the content/blob callers guard out
    /// real UUIDs first via [`Self::is_untracked_uuid`], so this coerces only a
    /// genuine non-UUID legacy path, never a UUID into a garbage `<uuid>` file.
    /// Kept until pre-UUID producers are confirmed extinct (then the whole
    /// fallback is deletable — YAGNI, tracked separately).
    fn resolve_legacy_path(document_id: &str) -> Result<PathBuf> {
        SafeRelayPath::new(document_id).map(SafeRelayPath::into_path_buf)
    }

    /// A remote content/blob op for a document we hold no path for whose id is a
    /// real UUID — locally deleted (its `uuid_to_path` cleared) or not yet
    /// registered. Such an op MUST be skipped, not materialized via
    /// [`Self::resolve_legacy_path`]'s coercion: that writes a garbage `<uuid>`
    /// file and diverges from peers that still resolve the real path (the §1.5
    /// edit-vs-delete race). The content is re-delivered by the catch-up after
    /// the (re-)register + subscribe establishes the path. A non-UUID legacy path
    /// is NOT skipped (only real UUIDs).
    fn is_untracked_uuid(&self, document_id: &str) -> bool {
        self.path_for_uuid(document_id).is_none() && uuid::Uuid::parse_str(document_id).is_ok()
    }

    /// Register identity for a document (path ↔ UUID) and persist.
    ///
    /// Accepts a [`SafeRelayPath`] to ensure relay-supplied paths have been
    /// validated before entering the identity map.
    fn register_identity(
        &mut self,
        rel_path: SafeRelayPath,
        document_uuid: String,
        confirmed: bool,
    ) {
        let rel_path = rel_path.into_path_buf();
        let path_str = rel_path_to_string(&rel_path);
        let abs_path = self.config.space_root.join(&rel_path);
        let inode = crate::inode::get_inode(&abs_path);
        self.file_identity.insert(
            rel_path.clone(),
            FileIdentity {
                document_uuid: document_uuid.clone(),
                inode,
            },
        );
        self.state.set(path_str, document_uuid.clone(), confirmed);
        self.uuid_to_path.insert(document_uuid, rel_path);
        self.save_state();
    }

    /// Mark a tracked document as confirmed by the relay (monotone) and persist.
    ///
    /// Called when the relay acknowledges a document the daemon already knows —
    /// e.g. our own create echoed back, or a doc present in the space's document
    /// list at startup — so its `was_remote` classification survives a restart.
    fn confirm_document(&mut self, document_uuid: &str) {
        let Some(rel_path) = self.uuid_to_path.get(document_uuid) else {
            return;
        };
        let path_str = rel_path_to_string(rel_path);
        if self.state.confirm(&path_str) {
            self.save_state();
        }
    }

    /// Unregister identity for a document and persist.
    fn unregister_identity(&mut self, document_uuid: &str) {
        if let Some(rel_path) = self.uuid_to_path.remove(document_uuid) {
            self.file_identity.remove(&rel_path);
            self.state.documents.remove(&rel_path_to_string(&rel_path));
            self.save_state();
        }
    }

    /// Move identity from one path to another and persist.
    fn move_identity(&mut self, old_path: &Path, new_path: PathBuf, document_uuid: &str) {
        let new_path_str = rel_path_to_string(&new_path);
        let old_path_str = rel_path_to_string(old_path);
        let abs_new = self.config.space_root.join(&new_path);
        let old_inode = self.file_identity.get(old_path).and_then(|id| id.inode);
        let inode = moved_inode(crate::inode::get_inode(&abs_new), old_inode);
        self.file_identity.remove(old_path);
        self.file_identity.insert(
            new_path.clone(),
            FileIdentity {
                document_uuid: document_uuid.to_string(),
                inode,
            },
        );
        // A rename preserves the document's confirmed status — it is the same
        // relay document at a new path.
        let confirmed = self
            .state
            .documents
            .get(&old_path_str)
            .is_some_and(|e| e.confirmed);
        self.state.documents.remove(&old_path_str);
        self.state
            .set(new_path_str, document_uuid.to_string(), confirmed);
        self.uuid_to_path
            .insert(document_uuid.to_string(), new_path);
        self.save_state();
    }

    /// Remove all local state for a document (last-synced, blob state, CRDT
    /// sidecar). The sidecar is keyed by document id, so resolve it from
    /// `file_identity` here — before any caller drops the identity.
    fn cleanup_document_state(&mut self, rel_path: &Path) {
        self.last_synced.remove(rel_path);
        self.blob_state.remove(rel_path);
        if let Some(id) = self.uuid_at(rel_path) {
            self.doc_manager.remove(&id);
        }
    }

    /// The document id tracked at `rel_path`, if any (path → id resolver).
    fn uuid_at(&self, rel_path: &Path) -> Option<String> {
        self.file_identity
            .get(rel_path)
            .map(|id| id.document_uuid.clone())
    }

    /// Whether `rel_path` is held by a document *other* than `document_id`, so a
    /// remote placement of `document_id` there must be deferred (path-arbitration
    /// conflict-copy) rather than clobber or CRDT-merge the occupant.
    ///
    /// Checks the on-disk file as well as `file_identity`: a local create or
    /// rename lands the file on disk *before* its watcher event updates
    /// `file_identity`, so a remote placement racing that watcher would otherwise
    /// see the path as free and overwrite the just-written local content. An
    /// untracked file on disk is treated as occupied — the relay never directs a
    /// document onto a path another document holds without also moving that
    /// occupant off it, so the deferral always drains.
    fn path_occupied_by_other(&self, rel_path: &Path, document_id: &str) -> bool {
        match self.uuid_at(rel_path) {
            Some(occupant) => occupant != document_id,
            None => self.config.space_root.join(rel_path).exists(),
        }
    }

    /// Clear `document_id` from `pending_placements`.
    ///
    /// An id is parked in `pending_placements` IFF it is currently deferred
    /// behind a path occupant and has not since been placed, deleted, or
    /// superseded. Every terminal transition funnels through this one owner so a
    /// stale deferral can't be left behind for a later [`Self::drain_pending`] to
    /// resurrect (the resurrection bug, commit `37093896`).
    fn resolve_placement(&mut self, document_id: &str) {
        self.pending_placements.remove(document_id);
    }

    /// The defer-or-place decision shared by remote register and rename.
    ///
    /// If `target` is held by a *different* live document, park `document_id` in
    /// `pending_placements` and return `true` — the caller returns; the relay
    /// will vacate the occupant (its own rename, a displacement broadcast, or a
    /// loser-correction) and [`Self::drain_pending`] materializes this document
    /// once the path frees. We never displace the occupant ourselves — only the
    /// relay decides who is displaced.
    ///
    /// Otherwise clear any earlier deferral ([`Self::resolve_placement`]) and
    /// return `false` — the caller places.
    ///
    /// `exempt_revival` bypasses the deferral: a register for a document this
    /// daemon has seen before is a REVIVAL returning to its OWN path (any file
    /// there is its own orphaned content, not a different document's), so
    /// deferring it would be permanent — no occupant ever moves off — stranding
    /// it untracked while content ops fall back to a uuid-named file. Rename
    /// passes `false` (a rename is never a revival of this kind). Routing both
    /// call sites through here keeps the exemption from drifting between them.
    fn defer_if_occupied(
        &mut self,
        target: &SafeRelayPath,
        document_id: &str,
        exempt_revival: bool,
    ) -> bool {
        if !exempt_revival && self.path_occupied_by_other(target.as_path(), document_id) {
            debug!(
                %document_id,
                path = %target,
                "deferring placement: path held by another document"
            );
            self.pending_placements
                .insert(document_id.to_owned(), target.clone());
            true
        } else {
            self.resolve_placement(document_id);
            false
        }
    }

    /// Documents renamed on disk while this daemon was offline: a tracked
    /// document whose recorded path is now absent, but whose recorded inode
    /// resolves to a file at a *different* path under the space root. Returns
    /// `(document_id, recorded_old_path, current_local_path)`.
    ///
    /// Pure detection (stat + inode walk, no mutation). The caller resolves each
    /// against the relay's authoritative state in [`Self::startup_reconciliation`]
    /// *before* the reconcile truth table runs, so a recorded-path-gone document
    /// is not misread as a local delete that destroys the identity before the
    /// new-file scan could re-bind the moved file (the split that mints a
    /// spurious second UUID for one document).
    fn detect_offline_renames(&self) -> Vec<(String, PathBuf, PathBuf)> {
        let mut out = Vec::new();
        for (old_path, identity) in &self.file_identity {
            if self.config.space_root.join(old_path).exists() {
                continue; // recorded path still present — not renamed away
            }
            if let Some(new_local) = self.space_file_with_inode(identity.inode)
                && &new_local != old_path
            {
                out.push((identity.document_uuid.clone(), old_path.clone(), new_local));
            }
        }
        out
    }

    /// Write CRDT content to disk if it differs from the file.
    ///
    /// Used after rename operations to flush remote ops that were merged
    /// into the CRDT while the rename was in flight.
    async fn flush_crdt_if_stale(
        &mut self,
        rel_path: &Path,
        suppress_tx: &mpsc::Sender<Suppression>,
    ) -> Result<()> {
        let Some(id) = self.uuid_at(rel_path) else {
            return Ok(());
        };
        let Some(doc) = self.doc_manager.get(&id) else {
            return Ok(());
        };
        let crdt_content = doc.content();
        if crdt_content.is_empty() {
            return Ok(());
        }
        let abs_path = self.config.space_root.join(rel_path);
        if let Ok(file_content) = std::fs::read_to_string(&abs_path)
            && file_content != crdt_content
        {
            debug!(
                path = %rel_path.display(),
                "flushing CRDT content after rename"
            );
            write_doc(
                &mut self.file_identity,
                rel_path,
                &abs_path,
                crdt_content.as_bytes(),
                suppress_tx,
            )
            .await?;
        }
        Ok(())
    }

    /// Build a `ChangeMetadata` with the daemon's author DID and a fresh origin
    /// HLC stamp. The stamp orders this op causally in the lifecycle lattice;
    /// `timestamp` (millis) is kept for display/mirror and equals `physical_ms`.
    fn make_metadata(&self, intent: &str) -> ChangeMetadata {
        let wall = self.skewed_now_ms_u64();
        let stamp = self.hlc.lock().expect("hlc mutex poisoned").tick(wall);
        ChangeMetadata {
            // Mirror the skewed physical time the stamp was taken at, so the
            // display/mirror `timestamp` stays equal to the HLC `physical_ms`.
            timestamp: kutl_core::ms_u64_to_i64_saturating(wall),
            author_did: self.config.author_did.clone(),
            intent: intent.into(),
            hlc: Some(stamp.into()),
            ..Default::default()
        }
    }

    /// Metadata stamped with an explicit (non-fresh) HLC. Used for a lifecycle
    /// op whose true origin time precedes "now" — notably an offline rename
    /// re-emitted on rejoin: stamping it with the daemon's pre-offline clock
    /// floor (rather than a fresh `now`) makes it lose to any cluster op that
    /// happened after the daemon went offline (no last-to-rejoin clobber), per
    /// the lattice-lifecycle design (MEDIUM-4: a stale offline op carries its
    /// old time, so it loses to recent cluster ops).
    fn make_metadata_with_hlc(&self, intent: &str, stamp: Hlc) -> ChangeMetadata {
        ChangeMetadata {
            timestamp: kutl_core::ms_u64_to_i64_saturating(stamp.physical_ms),
            author_did: self.config.author_did.clone(),
            intent: intent.into(),
            hlc: Some(stamp.into()),
            ..Default::default()
        }
    }

    /// This daemon's pre-offline clock floor: the high-water mark of stamps it
    /// emitted before the current session. Meaningful only during startup
    /// reconciliation — before the event loop processes any remote op (which
    /// would tick the clock past it). A lifecycle op re-emitted on rejoin (an
    /// offline rename or an offline delete) is stamped with this floor so it
    /// loses to any cluster op that happened after the daemon went offline (no
    /// last-to-rejoin clobber): the stale offline op carries its
    /// old time and cannot beat a causally-later concurrent edit/rename.
    fn offline_floor(&self) -> Hlc {
        self.hlc.lock().expect("hlc mutex poisoned").last()
    }

    /// HLC for an offline (startup-detected) delete of `document_id`.
    ///
    /// A delete only supersedes a document when its HLC dominates the document's
    /// liveness in the relay lattice (`DocRecord::is_alive`). The binding term is
    /// the content touch: the relay raises `touched_hlc` to
    /// `Hlc::physical_touch(content_ms)` = `{content_ms, logical: u32::MAX}`, where
    /// `content_ms` is the latest content-edit timestamp — and nothing at the same
    /// `physical_ms` outranks `u32::MAX`. So stamp the delete one millisecond above
    /// the latest content timestamp *in our own CRDT*: it dominates every edit we
    /// have OBSERVED (a genuine self-delete wins), yet still loses to a peer edit
    /// we have NOT observed — whose timestamp is newer and is absent from our CRDT
    /// — so that concurrent edit revives the document (§3.4 edit-revives, see
    /// `2026-06-05-offline-delete-rename-floor-design.md`). Reading our own CRDT
    /// (not the relay's current state) is what excludes the unobserved peer edit.
    ///
    /// A document with no content edit has no content touch; fall back to the
    /// pre-offline floor, which covers its registration stamp.
    fn offline_delete_stamp(&self, document_id: &str) -> Hlc {
        /// One millisecond: the smallest step that lifts the delete above a content
        /// touch at the same physical millisecond (whose `logical` is `u32::MAX`
        /// and so cannot be beaten at an equal `physical_ms`).
        const OVER_CONTENT_TOUCH_MS: u64 = 1;

        let floor = self.offline_floor();
        let observed_content_ms = self
            .doc_manager
            .get(document_id)
            .and_then(|doc| doc.changes().iter().map(|c| c.timestamp).max())
            .and_then(|ts| u64::try_from(ts).ok());
        match observed_content_ms {
            Some(ms) => Hlc {
                physical_ms: ms.saturating_add(OVER_CONTENT_TOUCH_MS),
                logical: 0,
                actor: floor.actor,
            },
            None => floor,
        }
    }

    /// Advance the origin clock past an observed remote stamp, so any op this
    /// daemon emits afterward is causally ordered after it.
    fn observe_remote_hlc(&self, meta: Option<&ChangeMetadata>) {
        if let Some(wire) = meta.and_then(|m| m.hlc.clone()) {
            match Hlc::try_from(wire) {
                Ok(remote) => {
                    let wall = self.skewed_now_ms_u64();
                    self.hlc
                        .lock()
                        .expect("hlc mutex poisoned")
                        .recv(remote, wall);
                }
                Err(e) => warn!(error = %e, "ignoring malformed remote hlc"),
            }
        }
    }

    /// Has this daemon already applied a lifecycle op for `document_id`?
    ///
    /// A register for a known id is a REVIVAL (its delete lost to a concurrent
    /// edit; the relay re-asserts it at its OWN path), which is exempt from
    /// path-collision deferral — see [`Self::defer_if_occupied`]. MUST be read
    /// BEFORE [`Self::record_lifecycle_hlc`] folds the current event's stamp,
    /// which would otherwise make every register look already-known.
    fn has_applied_lifecycle(&self, document_id: &str) -> bool {
        self.lifecycle_hlc.contains_key(document_id)
    }

    /// Advance the per-document lifecycle watermark to `hlc` (monotonic max).
    fn record_lifecycle_hlc(&mut self, document_id: &str, hlc: Hlc) {
        let slot = self
            .lifecycle_hlc
            .entry(document_id.to_owned())
            .or_insert(hlc);
        if hlc > *slot {
            *slot = hlc;
        }
    }

    /// Record the HLC of a lifecycle op this daemon just produced locally, so a
    /// stale remote echo of a now-superseded op for the same document is later
    /// dropped by [`Self::lifecycle_event_is_fresh`].
    fn note_local_lifecycle_hlc(&mut self, document_id: &str, meta: &ChangeMetadata) {
        if let Some(hlc) = meta.hlc.clone().and_then(|w| Hlc::try_from(w).ok()) {
            self.record_lifecycle_hlc(document_id, hlc);
        }
    }

    /// Build metadata for a LOCAL lifecycle op (rename/delete/displace) and record
    /// its HLC as this document's watermark. Stamping a lifecycle op and recording
    /// its watermark always go together — the watermark is what drops the stale
    /// echo the op would otherwise re-apply — so they live in one call that can't
    /// be half-used. (The local-register emit is intentionally NOT a lifecycle
    /// metadata: it is `&self` and the first op for a document, with no prior
    /// watermark to advance.)
    fn make_lifecycle_metadata(&mut self, document_id: &str, intent: &str) -> ChangeMetadata {
        let meta = self.make_metadata(intent);
        self.note_local_lifecycle_hlc(document_id, &meta);
        meta
    }

    /// As [`Self::make_lifecycle_metadata`] but with an explicit `stamp` — the
    /// offline-floor cases (a rename/delete that happened while offline must carry
    /// a pre-offline floor so it loses to a concurrent online op).
    fn make_lifecycle_metadata_with_hlc(
        &mut self,
        document_id: &str,
        intent: &str,
        stamp: Hlc,
    ) -> ChangeMetadata {
        let meta = self.make_metadata_with_hlc(intent, stamp);
        self.note_local_lifecycle_hlc(document_id, &meta);
        meta
    }

    /// Whether a remote lifecycle event should be applied: `true` when it is
    /// causally newer than the last lifecycle op applied for the document (or
    /// when it carries no HLC — the pre-HLC fallback applies unconditionally).
    ///
    /// On a `true` result for an HLC-bearing event, the watermark advances, so a
    /// later-arriving but causally-earlier event (a superseded rename, a lost
    /// delete) is then dropped. This is the gate that converges concurrent
    /// rename/rename and rename/delete.
    ///
    /// # Two freshness mechanisms — pick by call site
    ///
    /// This is the GATE-AND-APPLY mechanism (rename, unregister, ack). It does
    /// two load-bearing things a FOLD-ONLY fold does not: it `recv`s the stamp
    /// into the local clock — advancing it even when the event is DROPPED, so the
    /// daemon's NEXT op is causally after it (required for skew-reproducible
    /// arbitration) — and it then drops a causally-older event. Register uses the
    /// OTHER mechanism, [`Self::record_lifecycle_hlc`] (fold-only): it advances
    /// the per-doc watermark but does NOT gate (a revival must always apply) and
    /// does NOT `recv` (a register stamp can carry a peer's wall-clock skew, and
    /// recv'ing it would leak that skew into this daemon's next stamp). The
    /// asymmetry is the whole point — folding a gated event, or gating/recv'ing a
    /// register, would break convergence. A unified "policy" wrapper was declined
    /// as over-engineering: it would only relocate this match, not remove it.
    fn lifecycle_event_is_fresh(&mut self, document_id: &str, incoming: Option<Hlc>) -> bool {
        let Some(hlc) = incoming else {
            return true;
        };
        // Advance the origin clock past every observed lifecycle stamp — even a
        // stale one we drop — so any op this daemon emits next (e.g. the watcher
        // re-attributing a concurrently-relocated file) is causally after it and
        // wins the relay's HLC arbitration deterministically.
        let wall = self.skewed_now_ms_u64();
        self.hlc.lock().expect("hlc mutex poisoned").recv(hlc, wall);
        match self.lifecycle_hlc.get(document_id) {
            Some(applied) if hlc <= *applied => false,
            _ => {
                self.record_lifecycle_hlc(document_id, hlc);
                true
            }
        }
    }

    /// Sync the HLC floor from the live clock, then persist daemon state. The
    /// single funnel for state persistence so the floor (the monotonic-restart
    /// seed) is never left behind a stamp the clock has already emitted.
    fn save_state(&mut self) {
        // Sync live inodes into the persisted entries so a file renamed while
        // the daemon is later offline can still be located by inode on restart
        // (its recorded path will be gone, so the inode is unreadable there).
        let inodes: Vec<(String, Option<u64>)> = self
            .file_identity
            .iter()
            .map(|(p, id)| (rel_path_to_string(p), id.inode))
            .collect();
        for (path, inode) in inodes {
            self.state.set_inode(&path, inode);
        }
        let last = self.hlc.lock().expect("hlc mutex poisoned").last();
        self.state.record_emitted_hlc(last);
        if let Err(e) = self.state.save(&self.kutl_dir()) {
            error!(error = %e, "failed to persist daemon state");
        }
    }

    /// Register a document with the relay and subscribe to it.
    ///
    /// Best-effort stats the filesystem birthtime so the relay can
    /// populate `documents.originally_created_at`. A
    /// missing/unsupported birthtime sends `None`; the relay leaves
    /// the column NULL in that case. Never falls back to mtime — that
    /// would produce a wrong "originally created" claim.
    async fn register_and_subscribe(
        &self,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        document_id: &str,
        path: &str,
        intent: &str,
    ) -> Result<()> {
        let meta = self.make_metadata(intent);
        let abs_path = self.config.space_root.join(path);
        let originally_created_at_ms = crate::birthtime::get_birthtime_ms(&abs_path);
        sync_cmd_tx
            .send(SyncCommand::RegisterDocument {
                space_id: self.config.space_id.clone(),
                document_id: document_id.to_owned(),
                path: path.to_owned(),
                metadata: Some(meta),
                originally_created_at_ms,
            })
            .await?;
        sync_cmd_tx
            .send(SyncCommand::Subscribe {
                document_id: document_id.to_owned(),
            })
            .await?;
        Ok(())
    }

    /// Run the daemon event loop with automatic reconnection.
    ///
    /// Returns `Ok(())` on graceful shutdown (cancellation).
    pub async fn run(mut self) -> Result<()> {
        // Pre-flight: case-variant duplicates are bad state, not transient.
        // Run once before any session so we never enter the retry loop.
        if let Err(err) = crate::case_collision::detect_case_collisions(&self.config.space_root) {
            error!(
                space_id = %self.config.space_id,
                "{}",
                err.format_user_message()
            );
            return Err(err.into());
        }

        if self.config.one_shot {
            return self.run_once().await;
        }

        let mut backoff = INITIAL_BACKOFF;
        let mut is_reconnect = false;

        loop {
            if is_reconnect {
                crate::metrics_calls::record_relay_reconnect(&self.config.relay_url);
            }
            // Track whether this session ever reached the Connected state.
            // We only emit `connected=false` after a previous `true` so the
            // gauge never publishes "disconnected" for a connection that
            // never happened.
            let mut was_connected = false;
            let outcome = self.run_session(false, &mut was_connected).await;
            if was_connected {
                crate::metrics_calls::record_relay_connected(&self.config.relay_url, false);
            }
            is_reconnect = true;
            match outcome {
                Ok(SessionOutcome::Disconnected) => {
                    info!("session ended cleanly, reconnecting");
                    backoff = INITIAL_BACKOFF;
                    tokio::select! {
                        () = tokio::time::sleep(INITIAL_BACKOFF) => {}
                        () = self.config.cancel.cancelled() => {
                            info!("cancelled during reconnect backoff");
                            return Ok(());
                        }
                    }
                }
                Ok(SessionOutcome::Shutdown) => {
                    info!("received shutdown signal");
                    return Ok(());
                }
                Ok(SessionOutcome::AuthRejected(msg)) => {
                    return Err(anyhow::anyhow!("authentication rejected by relay: {msg}"));
                }
                Err(e) => {
                    warn!(error = %e, ?backoff, "session failed, reconnecting");
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = self.config.cancel.cancelled() => {
                            info!("cancelled during reconnect backoff");
                            return Ok(());
                        }
                    }
                    backoff = (backoff * BACKOFF_MULTIPLIER).min(MAX_BACKOFF);
                }
            }
        }
    }

    /// Run a single sync pass: connect, push local state, pull remote state, exit.
    async fn run_once(&mut self) -> Result<()> {
        // One-shot mode never reconnects, so the gauge transition isn't
        // observable; we still pass a sink to keep the signature uniform.
        let mut was_connected = false;
        match self.run_session(true, &mut was_connected).await? {
            SessionOutcome::Shutdown => info!("sync complete"),
            SessionOutcome::Disconnected => warn!("relay disconnected before sync completed"),
            SessionOutcome::AuthRejected(msg) => {
                anyhow::bail!("authentication rejected by relay: {msg}");
            }
        }
        Ok(())
    }

    /// Resolve a bearer token from stored credentials or DID challenge-response.
    ///
    /// Stored file credentials are only used if their `relay_url` matches
    /// the current relay. This prevents stale tokens from a different relay
    /// causing infinite auth-rejection retry loops.
    async fn resolve_auth_token(&self) -> String {
        // Priority 1: KUTL_TOKEN env var (explicit override, always trusted).
        if let Ok(token) = std::env::var(kutl_client::credentials::TOKEN_ENV_VAR)
            && !token.is_empty()
        {
            info!("using token from environment");
            return token;
        }

        // Priority 2: stored credentials, only if relay URL matches.
        let creds_path = kutl_client::credentials::default_credentials_path().ok();
        if let Some(path) = creds_path.as_deref()
            && let Ok(Some(creds)) = kutl_client::credentials::StoredCredentials::load(path)
            && creds.relay_url == self.config.relay_url
        {
            info!("using stored token");
            return creds.token;
        }
        if let Some(ref signing_key) = self.config.signing_key {
            return kutl_client::authenticate(
                &self.config.relay_url,
                &self.config.author_did,
                signing_key,
            )
            .await
            .unwrap_or_else(|e| {
                error!(error = %e, "authentication failed, proceeding without token");
                String::new()
            });
        }
        String::new()
    }

    /// Run a single session: authenticate, connect, relay, return on disconnect.
    ///
    /// `was_connected` is set to `true` when the relay reports Connected,
    /// letting the caller decide whether to emit the matching `false`
    /// transition for the connection-state gauge.
    async fn run_session(
        &mut self,
        one_shot: bool,
        was_connected: &mut bool,
    ) -> Result<SessionOutcome> {
        let auth_token = self.resolve_auth_token().await;

        // A fresh session is itself activity, and the previous session's
        // in-flight blob sends went away with its dropped channel — reset both
        // metric sources so the gauges reflect this session, not a stale one.
        self.last_progress = Instant::now();
        self.blob_backlog.store(0, Ordering::Relaxed);

        let (sync_cmd_tx, sync_cmd_rx) = mpsc::channel::<SyncCommand>(CHANNEL_CAPACITY);
        let (sync_event_tx, mut sync_event_rx) = mpsc::channel::<SyncEvent>(CHANNEL_CAPACITY);
        let (file_event_tx, mut file_event_rx) = mpsc::channel::<FileEvent>(CHANNEL_CAPACITY);
        let (suppress_tx, suppress_rx) = mpsc::channel::<Suppression>(CHANNEL_CAPACITY);

        // Spawn the WS client.
        let relay_url = self.config.relay_url.clone();
        let space_id = self.config.space_id.clone();
        let client_name = self.config.author_did.clone();
        let display_name = self.config.display_name.clone();
        let blob_backlog = self.blob_backlog.clone();
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client::run_client(
                &relay_url,
                &space_id,
                &client_name,
                &auth_token,
                &display_name,
                sync_cmd_rx,
                sync_event_tx,
                blob_backlog,
            )
            .await
            {
                error!(error = %e, "sync client error");
                crate::metrics_calls::record_error(crate::metrics_calls::error_category::RELAY);
            }
        });

        // Wait for connection before subscribing.
        match sync_event_rx.recv().await {
            Some(SyncEvent::Connected) => {
                info!("connected to relay");
                *was_connected = true;
                crate::metrics_calls::record_relay_connected(&self.config.relay_url, true);
            }
            Some(SyncEvent::AuthRejected(msg)) => {
                return Ok(SessionOutcome::AuthRejected(msg));
            }
            other => {
                anyhow::bail!("expected Connected event, got: {other:?}");
            }
        }

        // Discover remote documents, reconcile with local state, and execute.
        self.startup_reconciliation(&sync_cmd_tx, &suppress_tx, &mut sync_event_rx)
            .await?;

        // Scan all files on disk. For files already tracked by reconciliation,
        // this diffs file content against the CRDT and sends ops for any
        // offline edits. For new files, this registers and subscribes.
        //
        // This must run BEFORE processing remote catch-up so that local file
        // edits are applied to the CRDT against the pre-sync baseline. The
        // subsequent event loop merges remote ops, and diamond-types handles
        // the concurrent position transforms correctly.
        self.initial_file_scan(&sync_cmd_tx, &suppress_tx).await?;

        // Start file watcher (space_root is already canonical from caller).
        let mut watcher = FileWatcher::new(&self.config.space_root, file_event_tx, suppress_rx)?;
        let watcher_handle = tokio::spawn(async move {
            watcher.run().await;
        });

        if let Some(ref notify) = self.config.ready {
            notify.notify_one();
        }

        // Main event loop.
        let result = self
            .event_loop(
                &mut file_event_rx,
                &mut sync_event_rx,
                &sync_cmd_tx,
                &suppress_tx,
                one_shot,
            )
            .await;

        watcher_handle.abort();
        client_handle.abort();

        // Persist the HLC floor advanced by the stamps this session emitted (and
        // remote stamps it observed), so the next process seeds its clock above
        // them. The other `save_state` call sites fire from `register_identity` /
        // `confirm` / `move_identity`, which all run BEFORE `make_metadata` ticks
        // the clock — so without this, a one-shot `kutl sync` left the persisted
        // floor at its pre-session value (`{0,0}` on first run), and the next
        // sync stamped its offline delete/rename at zero and lost arbitration at
        // the relay. See `2026-06-05-offline-delete-rename-floor-design.md`.
        self.save_state();

        result
    }

    /// Core select loop processing file and sync events.
    ///
    /// In one-shot mode, exits after [`ONE_SHOT_IDLE_TIMEOUT`] of inactivity
    /// (no sync events received), indicating the initial sync exchange is complete.
    async fn event_loop(
        &mut self,
        file_event_rx: &mut mpsc::Receiver<FileEvent>,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
        one_shot: bool,
    ) -> Result<SessionOutcome> {
        // The idle timer is only polled when `one_shot` is true (via the select
        // guard). In persistent mode it's never polled, so no system timer is
        // registered and the cost is just the struct on the stack.
        let idle_timeout = tokio::time::sleep(ONE_SHOT_IDLE_TIMEOUT);
        tokio::pin!(idle_timeout);

        // Refreshes the per-space gauges; the first tick fires immediately so
        // they register as soon as the session is up. Independent of the idle
        // timer — a metrics tick is not user activity and must not keep a
        // one-shot session alive.
        let mut metrics_tick = tokio::time::interval(METRICS_EMIT_INTERVAL);

        loop {
            tokio::select! {
                Some(file_event) = file_event_rx.recv() => {
                    if one_shot {
                        idle_timeout.as_mut().reset(tokio::time::Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
                    }
                    if let Err(e) = self.handle_file_event(file_event, sync_cmd_tx, suppress_tx).await {
                        error!(error = %e, "error handling file event");
                        crate::metrics_calls::record_error(crate::metrics_calls::error_category::FILE_EVENT);
                    } else {
                        self.last_progress = Instant::now();
                    }
                }
                Some(sync_event) = sync_event_rx.recv() => {
                    // Reset idle timer on each sync event.
                    if one_shot {
                        idle_timeout.as_mut().reset(tokio::time::Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
                    }
                    match sync_event {
                        SyncEvent::Disconnected => {
                            info!("disconnected from relay");
                            return Ok(SessionOutcome::Disconnected);
                        }
                        other => {
                            if let Err(e) = self.handle_sync_event(other, suppress_tx, sync_cmd_tx).await {
                                error!(error = %e, "error handling sync event");
                                crate::metrics_calls::record_error(crate::metrics_calls::error_category::SYNC_EVENT);
                            } else {
                                self.last_progress = Instant::now();
                            }
                        }
                    }
                }
                _ = metrics_tick.tick() => {
                    self.emit_periodic_metrics(sync_cmd_tx);
                }
                () = &mut idle_timeout, if one_shot => {
                    info!("sync idle timeout, exiting");
                    return Ok(SessionOutcome::Shutdown);
                }
                () = self.config.cancel.cancelled() => {
                    return Ok(SessionOutcome::Shutdown);
                }
                else => break,
            }
        }
        Ok(SessionOutcome::Disconnected)
    }

    /// Refresh this space's live `/metrics` gauges from the current session
    /// state. Called on the metrics tick (and so only while a session is up,
    /// which is exactly when these counters are meaningful).
    fn emit_periodic_metrics(&self, sync_cmd_tx: &mpsc::Sender<SyncCommand>) {
        let space = &self.config.space_id;

        // Queue depth = buffered (not-yet-received) commands = capacity used.
        let depth = sync_cmd_tx
            .max_capacity()
            .saturating_sub(sync_cmd_tx.capacity());
        crate::metrics_calls::record_sync_queue_depth(space, depth as u64);

        let backlog = u64::try_from(self.blob_backlog.load(Ordering::Relaxed).max(0)).unwrap_or(0);
        crate::metrics_calls::record_blob_upload_backlog(space, backlog);

        crate::metrics_calls::record_seconds_since_last_progress(
            space,
            self.last_progress.elapsed().as_secs(),
        );
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
    async fn startup_reconciliation(
        &mut self,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
    ) -> Result<()> {
        // Step 1: Fetch the relay's active document list.
        debug!(space_id = %self.config.space_id, "sending ListSpaceDocuments request");
        sync_cmd_tx
            .send(SyncCommand::ListSpaceDocuments {
                space_id: self.config.space_id.clone(),
            })
            .await?;

        let remote_active = self.wait_for_document_list(sync_event_rx).await?;

        // Step 1.5: Resolve documents renamed on disk while offline, BEFORE the
        // truth table. Each is a tracked doc whose recorded path is gone but
        // whose inode locates the file at a new path; the truth table would read
        // the gone recorded path as a local delete and destroy the identity, and
        // the new-file scan would then mint a spurious second UUID for the moved
        // file. Resolving here against the relay's authoritative state keeps one
        // document one identity. The watcher is not yet running, so the file
        // moves/removes below generate no events to suppress.
        let handled = self
            .reconcile_offline_renames(&remote_active, sync_cmd_tx)
            .await?;

        // Step 2: Build reconciliation inputs. `previously_remote` (the
        // `was_remote` axis) is derived from each document's persisted
        // `confirmed` flag — no separate snapshot to drift out of sync.
        // Documents already resolved by the offline-rename pre-pass are excluded
        // so the truth table does not re-process (and undo) them.
        let state_entries: HashMap<PathBuf, String> = self
            .state
            .documents
            .iter()
            .filter(|(_, v)| !handled.contains(&v.id))
            .map(|(k, v)| (PathBuf::from(k), v.id.clone()))
            .collect();
        let previously_remote = self.state.confirmed_ids();

        let inputs = ReconcileInputs {
            state_entries: &state_entries,
            previously_remote: &previously_remote,
            remote_active: &remote_active,
            space_root: &self.config.space_root,
        };

        // Step 3: Produce actions.
        let actions = reconcile::reconcile_startup(&inputs);

        // Step 4: Execute actions.
        self.execute_reconcile_actions(&actions, sync_cmd_tx, suppress_tx)
            .await?;

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
    async fn reconcile_offline_renames(
        &mut self,
        remote_active: &HashMap<String, SafeRelayPath>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<HashSet<String>> {
        let mut handled = HashSet::new();
        let confirmed = self.state.confirmed_ids();
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
                    let meta = self.make_lifecycle_metadata_with_hlc(
                        &document_id,
                        "offline rename",
                        offline_floor,
                    );
                    sync_cmd_tx
                        .send(SyncCommand::RenameDocument {
                            space_id: self.config.space_id.clone(),
                            document_id: document_id.clone(),
                            old_path: rel_path_to_string(&old_path),
                            new_path: rel_path_to_string(&new_local),
                            metadata: Some(meta),
                        })
                        .await?;
                    sync_cmd_tx
                        .send(SyncCommand::Subscribe {
                            document_id: document_id.clone(),
                        })
                        .await?;
                    handled.insert(document_id);
                }
                Some(relay_path) => {
                    info!(%document_id, offline = %new_local.display(), authoritative = %relay_path, "offline rename loses to cluster rename; conforming to relay path");
                    let from_abs = self.config.space_root.join(&new_local);
                    let to_abs = relay_path.under(&self.config.space_root);
                    if let Some(parent) = to_abs.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    if let Err(e) = std::fs::rename(&from_abs, &to_abs) {
                        error!(from = %from_abs.display(), to = %to_abs.display(), error = %e, "failed to conform offline-renamed file to relay path");
                    }
                    let relay_buf = relay_path.as_path().to_path_buf();
                    self.move_identity(&old_path, relay_buf, &document_id);
                    sync_cmd_tx
                        .send(SyncCommand::Subscribe {
                            document_id: document_id.clone(),
                        })
                        .await?;
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
            .filter_map(|uuid| self.uuid_to_path.get(uuid))
            .map(|p| rel_path_to_string(p))
            .collect();
        let mut changed = false;
        for path in paths {
            changed |= self.state.confirm(&path);
        }
        if changed {
            self.save_state();
        }
    }

    /// Execute a list of reconciliation actions produced by the truth table.
    async fn execute_reconcile_actions(
        &mut self,
        actions: &[StartupAction],
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
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
                    if !self.uuid_to_path.contains_key(document_id)
                        && self.uuid_at(&rel).is_none()
                        && self.config.space_root.join(&rel).exists()
                    {
                        debug!(%document_id, path = %path, "deferring remote subscribe: path held by an offline-created file");
                        self.pending_placements
                            .insert(document_id.clone(), path.clone());
                        continue;
                    }
                    info!(%document_id, path = %path, "subscribing to remote document");
                    if !self.uuid_to_path.contains_key(document_id) {
                        self.register_identity(path.clone(), document_id.clone(), true);
                    }
                    sync_cmd_tx
                        .send(SyncCommand::Subscribe {
                            document_id: document_id.clone(),
                        })
                        .await?;
                }
                StartupAction::SyncLocal { document_id, path } => {
                    self.sync_local_document(document_id, path, sync_cmd_tx)
                        .await?;
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
                    sync_cmd_tx
                        .send(SyncCommand::UnregisterDocument {
                            space_id: self.config.space_id.clone(),
                            document_id: document_id.clone(),
                            metadata: Some(meta),
                        })
                        .await?;
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
                        &mut self.file_identity,
                        old_path,
                        &old_abs,
                        &new_path_buf,
                        &new_abs,
                        suppress_tx,
                    )
                    .await?;

                    self.move_identity(old_path, new_path_buf.clone(), document_id);

                    // Sync at the new path.
                    self.sync_local_document(document_id, &new_path_buf, sync_cmd_tx)
                        .await?;
                }
                StartupAction::DeleteLocal { document_id, path } => {
                    info!(path = %path.display(), %document_id, "document unregistered remotely, deleting locally");
                    let abs_path = self.config.space_root.join(path);
                    if abs_path.exists()
                        && let Err(e) = remove_doc(path, &abs_path, suppress_tx).await
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
    async fn sync_local_document(
        &mut self,
        document_id: &str,
        path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        self.register_and_subscribe(
            sync_cmd_tx,
            document_id,
            &rel_path_to_string(path),
            "startup sync",
        )
        .await?;

        if let Some(doc) = self.doc_manager.get(document_id)
            && !doc.local_version().is_empty()
        {
            let (ops, metadata) = encode_delta(doc, &[]);
            sync_cmd_tx
                .send(SyncCommand::SendOps {
                    document_id: document_id.to_string(),
                    ops,
                    metadata,
                    content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                    content_hash: Vec::new(),
                })
                .await?;
            self.last_synced
                .insert(path.to_owned(), doc.local_version());
        }

        Ok(())
    }

    /// Walk the space directory and process all files. For files already
    /// tracked by reconciliation, diffs content against the CRDT. For new
    /// files, registers and subscribes.
    async fn initial_file_scan(
        &mut self,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
    ) -> Result<()> {
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
            let crdt_content = self
                .uuid_at(&rel_path)
                .and_then(|id| self.doc_manager.get(&id).map(kutl_core::Document::content));
            if let Some(crdt_content) = crdt_content {
                let abs_path = self.doc_manager.file_path(&rel_path);
                match std::fs::read_to_string(&abs_path) {
                    Ok(file_content) if file_content == crdt_content => continue,
                    _ => {} // File differs or can't be read — process it
                }
            }

            let event = FileEvent::Modified {
                rel_path: rel_path.clone(),
            };
            if let Err(e) = self
                .handle_file_event(event, sync_cmd_tx, suppress_tx)
                .await
            {
                error!(path = %rel_path.display(), error = %e, "initial scan: failed to process file");
            } else {
                count += 1;
            }
        }

        if count > 0 {
            info!(count, "initial scan: processed existing files");
        }

        Ok(())
    }

    /// Handle a local file change: diff → CRDT ops → send to relay.
    async fn handle_file_event(
        &mut self,
        event: FileEvent,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
    ) -> Result<()> {
        match event {
            FileEvent::Modified { rel_path } => {
                self.handle_file_modified(&rel_path, sync_cmd_tx, suppress_tx)
                    .await
            }
            FileEvent::Removed { rel_path } => {
                self.handle_file_removed(&rel_path, sync_cmd_tx).await
            }
            FileEvent::Renamed { old_path, new_path } => {
                self.handle_file_renamed(&old_path, &new_path, sync_cmd_tx, suppress_tx)
                    .await
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
    async fn try_local_inode_rename(
        &mut self,
        rel_path: &Path,
        abs_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
    ) -> Result<bool> {
        if self.file_identity.contains_key(rel_path) {
            return Ok(false);
        }
        let Some(new_inode) = crate::inode::get_inode(abs_path) else {
            return Ok(false);
        };
        let space_root = self.config.space_root.clone();
        let Some((old_path, document_id)) =
            find_rename_source(&self.file_identity, new_inode, |old| {
                space_root.join(old).exists()
            })
        else {
            return Ok(false);
        };

        info!(?old_path, ?rel_path, "detected rename via inode match");
        self.move_identity(&old_path, rel_path.to_owned(), &document_id);
        if let Some(version) = self.last_synced.remove(&old_path) {
            self.last_synced.insert(rel_path.to_owned(), version);
        }
        // Sidecar is keyed by stable document id — a rename moves no sidecar.

        let meta = self.make_lifecycle_metadata(&document_id, "file rename");
        sync_cmd_tx
            .send(SyncCommand::RenameDocument {
                space_id: self.config.space_id.clone(),
                document_id,
                old_path: rel_path_to_string(&old_path),
                new_path: rel_path_to_string(rel_path),
                metadata: Some(meta),
            })
            .await?;

        // Flush CRDT content if remote ops were merged mid-rename.
        self.flush_crdt_if_stale(rel_path, suppress_tx).await?;
        Ok(true)
    }

    async fn handle_file_modified(
        &mut self,
        rel_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
    ) -> Result<()> {
        let abs_path = self.doc_manager.file_path(rel_path);
        let content = match std::fs::read_to_string(&abs_path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                return self.handle_blob_change(rel_path, sync_cmd_tx).await;
            }
            Err(e) => {
                return Err(e).with_context(|| format!("failed to read {}", abs_path.display()));
            }
        };

        // Inode-based rename detection (platforms without paired rename events).
        if self
            .try_local_inode_rename(rel_path, &abs_path, sync_cmd_tx, suppress_tx)
            .await?
        {
            return Ok(());
        }

        // Overwrite-rename detection: a `mv` onto an
        // already-tracked path. Handled in its own pass to keep this one small.
        if self
            .try_overwrite_rename(rel_path, &abs_path, sync_cmd_tx, suppress_tx)
            .await?
        {
            return Ok(());
        }

        // A path the daemon has never tracked is a new document. `file_identity`
        // is the path → id index (the sidecar is keyed by id); use its keys for
        // the case-collision check, gated on the path being new.
        let was_tracked = self.file_identity.contains_key(rel_path);
        if !was_tracked {
            let tracked: Vec<PathBuf> = self.file_identity.keys().cloned().collect();
            if let Some(existing) = crate::case_collision::find_case_variant(
                rel_path,
                tracked.iter().map(PathBuf::as_path),
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
        let is_new = self.doc_manager.get(&document_id).is_none();

        // Editors save via the atomic tmp-rename dance, which gives the file a
        // fresh inode on every write. Refresh the recorded inode for an
        // already-tracked path so a genuine later rename of this file is still
        // detected (its current inode keeps matching) and the previous,
        // now-freed inode is not left behind as bait for a false rename match.
        // Newly registered paths already recorded their current inode, so only
        // refresh paths that existed before this event. See `refresh_inode`.
        if was_tracked {
            refresh_inode(
                &mut self.file_identity,
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
            )
            .await?;
        }

        let doc = self.doc_manager.load_or_create(&document_id)?;
        let agent = doc.register_agent(&self.agent_name)?;

        let version_before = doc.local_version();

        let author_did = self.config.author_did.clone();
        bridge::apply_file_change(doc, agent, &author_did, &content)?;

        let version_after = doc.local_version();
        if version_before == version_after {
            return Ok(());
        }

        let since = self
            .last_synced
            .get(rel_path)
            .map_or_else(Vec::new, Clone::clone);
        let (ops, metadata) = encode_delta(doc, &since);

        sync_cmd_tx
            .send(SyncCommand::SendOps {
                document_id: document_id.clone(),
                ops,
                metadata,
                content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                content_hash: Vec::new(),
            })
            .await?;

        self.last_synced.insert(rel_path.to_owned(), version_after);
        self.doc_manager.save(&document_id)?;

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
    async fn try_overwrite_rename(
        &mut self,
        rel_path: &Path,
        abs_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
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
        let Some((mover_old, mover_id)) = find_rename_source(&self.file_identity, inode, |old| {
            old == rel_path || space_root.join(old).exists()
        }) else {
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
        )
        .await?;
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
    async fn handle_overwrite_rename(
        &mut self,
        mover_old: &Path,
        mover_id: &str,
        target: &Path,
        occupant_id: &str,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
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

        // 1. Recover the occupant to its conflict path from its intact sidecar
        //    (its on-disk file was just destroyed by the overwrite), then emit the
        //    displacement rename. Vacates `target` for the mover.
        let occ_content = self.doc_manager.load_or_create(occupant_id)?.content();
        let conflict_abs = self.config.space_root.join(&conflict_rel);
        write_doc(
            &mut self.file_identity,
            &conflict_rel,
            &conflict_abs,
            occ_content.as_bytes(),
            suppress_tx,
        )
        .await?;
        self.move_identity(target, conflict_rel.clone(), occupant_id);
        if let Some(v) = self.last_synced.remove(target) {
            self.last_synced.insert(conflict_rel.clone(), v);
        }
        let occ_meta = self.make_lifecycle_metadata(occupant_id, "overwrite displace");
        sync_cmd_tx
            .send(SyncCommand::RenameDocument {
                space_id: self.config.space_id.clone(),
                document_id: occupant_id.to_owned(),
                old_path: target_str.clone(),
                new_path: conflict,
                metadata: Some(occ_meta),
            })
            .await?;

        // 2. The mover takes the now-vacated target (its content is already on
        //    disk there from the `mv`). Move its identity and emit the rename.
        self.move_identity(mover_old, target.to_path_buf(), mover_id);
        if let Some(v) = self.last_synced.remove(mover_old) {
            self.last_synced.insert(target.to_path_buf(), v);
        }
        let mover_meta = self.make_lifecycle_metadata(mover_id, "overwrite rename");
        sync_cmd_tx
            .send(SyncCommand::RenameDocument {
                space_id: self.config.space_id.clone(),
                document_id: mover_id.to_owned(),
                old_path: rel_path_to_string(mover_old),
                new_path: target_str,
                metadata: Some(mover_meta),
            })
            .await?;

        Ok(())
    }

    /// Handle a file rename detected by the watcher.
    async fn handle_file_renamed(
        &mut self,
        old_path: &Path,
        new_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<Suppression>,
    ) -> Result<()> {
        info!(?old_path, ?new_path, "file renamed");

        let old_path_str = rel_path_to_string(old_path);
        let new_path_str = rel_path_to_string(new_path);

        // Look up the UUID for the old path; if unknown, treat as a new file.
        let Some(identity) = self.file_identity.get(old_path).cloned() else {
            return self
                .handle_file_modified(new_path, sync_cmd_tx, suppress_tx)
                .await;
        };
        let document_id = identity.document_uuid;

        // Reject rename-to-collide: if new_path is a case-variant of another
        // tracked document (not the source), refuse to propagate. The
        // filesystem holds both files; we will not sync the rename target
        // until the user resolves the collision.
        let tracked: Vec<PathBuf> = self.file_identity.keys().cloned().collect();
        let tracked_iter = tracked
            .iter()
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
        if let Some(version) = self.last_synced.remove(old_path) {
            self.last_synced.insert(new_path.to_owned(), version);
        }

        // The CRDT sidecar is keyed by document id, which is stable across a
        // rename — nothing to move on disk.

        // Send RenameDocument to relay.
        let meta = self.make_lifecycle_metadata(&document_id, "file rename");
        sync_cmd_tx
            .send(SyncCommand::RenameDocument {
                space_id: self.config.space_id.clone(),
                document_id,
                old_path: old_path_str,
                new_path: new_path_str,
                metadata: Some(meta),
            })
            .await?;

        // Flush CRDT content if remote ops were merged mid-rename.
        self.flush_crdt_if_stale(new_path, suppress_tx).await?;

        Ok(())
    }

    /// Handle a file removal.
    async fn handle_file_removed(
        &mut self,
        rel_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        // If we have an identity, send UnregisterDocument.
        if let Some(identity) = self.file_identity.get(rel_path).cloned() {
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
            sync_cmd_tx
                .send(SyncCommand::UnregisterDocument {
                    space_id: self.config.space_id.clone(),
                    document_id,
                    metadata: Some(meta),
                })
                .await?;
        }

        Ok(())
    }

    /// Handle a sync event from the relay: merge remote ops → write to disk.
    async fn handle_sync_event(
        &mut self,
        event: SyncEvent,
        suppress_tx: &mpsc::Sender<Suppression>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
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
                // Build a temporary SyncOps to check blob mode.
                let check = kutl_proto::sync::SyncOps {
                    content_mode,
                    ..Default::default()
                };
                if is_blob_mode(&check) {
                    self.handle_remote_blob(&document_id, ops, metadata, content_hash, suppress_tx)
                        .await?;
                } else {
                    self.handle_remote_text(&document_id, ops, metadata, suppress_tx, sync_cmd_tx)
                        .await?;
                }
            }
            SyncEvent::DocumentRegistered {
                document_id,
                path,
                hlc,
            } => {
                self.handle_remote_register(&document_id, &path, hlc, sync_cmd_tx)
                    .await?;
            }
            SyncEvent::DocumentRenamed {
                document_id,
                old_path,
                new_path,
                hlc,
            } => {
                let safe_old = match SafeRelayPath::new(&old_path) {
                    Ok(p) => p,
                    Err(e) => {
                        error!(%document_id, "ignoring document rename with invalid old path: {e}");
                        return Ok(());
                    }
                };
                let safe_new = match SafeRelayPath::new(&new_path) {
                    Ok(p) => p,
                    Err(e) => {
                        error!(%document_id, "ignoring document rename with invalid new path: {e}");
                        return Ok(());
                    }
                };
                // Drop a rename superseded by a newer lifecycle op (the loser of
                // a concurrent rename keeps the winner it already applied).
                if !self.lifecycle_event_is_fresh(&document_id, hlc) {
                    debug!(%document_id, "dropping stale remote rename");
                    return Ok(());
                }
                self.handle_remote_rename(
                    &document_id,
                    &safe_old,
                    &safe_new,
                    suppress_tx,
                    sync_cmd_tx,
                )
                .await?;
            }
            SyncEvent::DocumentUnregistered { document_id, hlc } => {
                self.handle_remote_unregister(&document_id, hlc, suppress_tx, sync_cmd_tx)
                    .await?;
            }
            SyncEvent::LifecycleAck {
                document_id,
                effective_path,
                hlc,
            } => {
                self.handle_lifecycle_ack(
                    &document_id,
                    effective_path,
                    hlc,
                    suppress_tx,
                    sync_cmd_tx,
                )
                .await?;
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

    /// Handle a remote document registration: subscribe to a newly-seen
    /// document, or confirm one we already track. Drops a stale registration
    /// superseded by a newer lifecycle op we already applied (e.g. our own
    /// local delete of this document).
    async fn handle_remote_register(
        &mut self,
        document_id: &str,
        path: &str,
        hlc: Option<Hlc>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        let safe_path = match SafeRelayPath::new(path) {
            Ok(p) => p,
            Err(e) => {
                error!(%document_id, "ignoring document registration with invalid path: {e}");
                return Ok(());
            }
        };
        // A register for a doc we've already applied a lifecycle op for is a
        // REVIVAL (exempt from path-collision deferral, see `defer_if_occupied`).
        // Captured BEFORE the HLC fold below, which would otherwise make every
        // register look already-known.
        let is_revival = self.has_applied_lifecycle(document_id);
        // A register is a creation/revival, never a competitor to drop: it either
        // mints a brand-new id (no prior watermark) or re-asserts a known one (a
        // revival, which MUST apply). So unlike rename/unregister it is NOT gated
        // on freshness — gating risks dropping a revival whose origin stamp is
        // older than the loser's recorded delete watermark. We still fold its HLC
        // into the per-doc watermark (monotonic max) so a later genuinely-stale
        // rename/delete is dropped, but we do NOT advance the local clock from it:
        // a register stamp can carry a peer's wall-clock skew, and recv'ing it
        // would let that skew leak into ops this daemon stamps next (breaking
        // skew-reproducible rename arbitration).
        if let Some(hlc) = hlc {
            self.record_lifecycle_hlc(document_id, hlc);
        }
        info!(%document_id, %path, "remote document registered");
        if self.uuid_to_path.contains_key(document_id) {
            // Already tracked (e.g. our own create echoed back) — record that the
            // relay has now acknowledged it.
            self.confirm_document(document_id);
            return Ok(());
        }

        // Path-arbitration conflict-copy: if this brand-new document's path is
        // held by a *different* live document, defer materializing it (the relay
        // will move that occupant off the path; the drain in the occupant's move
        // handler materializes this one then). A REVIVAL (`is_revival`) is
        // exempt — see `defer_if_occupied`. The not-deferred branch clears any
        // earlier deferral and places.
        if self.defer_if_occupied(&safe_path, document_id, is_revival) {
            return Ok(());
        }
        self.place_register(safe_path, document_id, sync_cmd_tx)
            .await
    }

    /// Materialize a newly-registered remote document: track its identity and
    /// subscribe so its content streams in. The placement half of
    /// [`Self::handle_remote_register`], also invoked by [`Self::drain_pending`]
    /// once a deferred document's contested path frees.
    async fn place_register(
        &mut self,
        safe_path: SafeRelayPath,
        document_id: &str,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        self.register_identity(safe_path, document_id.to_owned(), true);
        sync_cmd_tx
            .send(SyncCommand::Subscribe {
                document_id: document_id.to_owned(),
            })
            .await
            .context("failed to subscribe to newly registered document")?;
        Ok(())
    }

    /// Handle a remote document unregister: delete the file and clean up local
    /// state. Drops a delete superseded by a newer lifecycle op (a concurrent
    /// rename that won keeps the document alive).
    async fn handle_remote_unregister(
        &mut self,
        document_id: &str,
        hlc: Option<Hlc>,
        suppress_tx: &mpsc::Sender<Suppression>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        if !self.lifecycle_event_is_fresh(document_id, hlc) {
            debug!(%document_id, "dropping stale remote unregister");
            return Ok(());
        }
        info!(%document_id, "remote document unregistered");

        // Drop any deferred placement for this id (mirrors the register/rename
        // handlers). A document deleted while still parked in pending_placements
        // — deferred behind a path occupant, never placed into uuid_to_path —
        // would otherwise survive here (freed is None below, so neither the
        // cleanup nor the drain runs) and be resurrected when a later
        // drain_pending materializes the still-waiting, now-deleted id.
        self.resolve_placement(document_id);

        // Delete the file from disk and clean up CRDT state.
        let freed = self.uuid_to_path.get(document_id).cloned();
        if let Some(ref rel_path) = freed {
            let abs_path = self.config.space_root.join(rel_path);
            if abs_path.exists()
                && let Err(e) = remove_doc(rel_path, &abs_path, suppress_tx).await
            {
                error!(path = %abs_path.display(), error = %e, "failed to delete unregistered file");
            }
            self.cleanup_document_state(rel_path);
        }

        self.unregister_identity(document_id);

        // The deleted document vacated its path; a conflict-copy deferred on it
        // (a rare delete-races-collision) can now materialize.
        if let Some(rel_path) = freed {
            self.drain_pending(rel_path, suppress_tx, sync_cmd_tx)
                .await?;
        }
        Ok(())
    }

    /// Reconcile this daemon's own document to the post-arbitration effective
    /// path the relay confirmed in a register/rename ack (RFD 0042 typed-ack
    /// rail). When our own op lost a path collision, the relay persisted the
    /// document at its conflict path; `effective_path` differs from where we
    /// hold it locally, so move our file there. This is the sender-side half of
    /// path arbitration — the displaced *other* documents are corrected by the
    /// relay's `broadcast_displaced` instead.
    ///
    /// Reuses [`Self::handle_remote_rename`] so the vacated requested path drains
    /// any placement deferred on it (the collision winner). No-op when the
    /// document kept the path it requested (the common case) or isn't tracked.
    /// Classify a register/rename ack: the `(old, new)` safe paths to move our
    /// OWN document to, but ONLY when the ack signals we LOST a path collision
    /// (the effective path is a conflict path) AND the document is tracked here
    /// AND not already at that path. `None` for every no-op case — won op,
    /// untracked, already reconciled, or an unparseable path. Pure (reads +
    /// constructors); the caller applies the freshness gate and the move.
    ///
    /// The conflict-infix check is what makes self-correction immune to a lagging
    /// local create/rename watcher: a won op's effective path is the clean
    /// requested path, so a transiently-stale `uuid_to_path` can't spuriously
    /// "correct" a document that actually won.
    fn ack_requires_self_correction(
        &self,
        document_id: &str,
        effective: &str,
    ) -> Option<(SafeRelayPath, SafeRelayPath)> {
        if !effective.contains(kutl_core::lattice::CONFLICT_INFIX) {
            return None;
        }
        let current = self.uuid_to_path.get(document_id)?; // not tracked → nothing of ours
        let current_str = rel_path_to_string(current);
        if current_str == effective {
            return None; // already reconciled to the conflict path
        }
        let old = SafeRelayPath::new(&current_str)
            .map_err(|e| error!(%document_id, "tracked path is not relay-safe: {e}"))
            .ok()?;
        let new = SafeRelayPath::new(effective)
            .map_err(|e| error!(%document_id, "ack effective path invalid: {e}"))
            .ok()?;
        Some((old, new))
    }

    async fn handle_lifecycle_ack(
        &mut self,
        document_id: &str,
        effective_path: Option<String>,
        hlc: Option<Hlc>,
        suppress_tx: &mpsc::Sender<Suppression>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        let Some(effective) = effective_path else {
            return Ok(());
        };
        // A successful register/rename ack is the relay acknowledging this
        // document (`effective_path` is `None` on failure). The lifecycle
        // broadcast that drives `confirm_document` excludes the sender, so a
        // registrant never sees its own `DocumentRegistered` echo — this typed ack
        // is the ONLY signal that confirms its own create. Without it the doc stays
        // unconfirmed (`was_remote = false`), so a later local delete is
        // misclassified as SubscribeRemote (re-download) instead of SendUnregister
        // (push the delete). Confirm is monotone, so a rename ack re-confirming is a
        // no-op. See `2026-06-05-offline-delete-rename-floor-design.md`.
        self.confirm_document(document_id);
        let Some((old, new)) = self.ack_requires_self_correction(document_id, &effective) else {
            return Ok(());
        };
        // Freshness gate (after classification, before the mutating apply): a
        // late/reordered ack must not move the document against a causally-newer
        // rename already applied. handle_remote_rename itself advances no
        // watermark, so without this an out-of-order ack would re-apply a
        // superseded conflict-path move. Records the watermark on accept; drops a
        // stale (or duplicate same-HLC) ack. Kept OUTSIDE the predicate because it
        // is `&mut self` and advances the HLC clock even when it drops the event.
        if !self.lifecycle_event_is_fresh(document_id, hlc) {
            debug!(%document_id, "dropping stale lifecycle ack reconciliation");
            return Ok(());
        }
        info!(%document_id, from = %old, to = %new, "reconciling own document to arbitrated conflict path");
        self.handle_remote_rename(document_id, &old, &new, suppress_tx, sync_cmd_tx)
            .await
    }

    /// Handle a remote document rename: incorporate pending edits, move
    /// identity + CRDT sidecar, rename file on disk.
    ///
    /// Both paths are pre-validated [`SafeRelayPath`] references, ensuring
    /// the relay cannot trick the daemon into writing outside the space root.
    async fn handle_remote_rename(
        &mut self,
        document_id: &str,
        old_path: &SafeRelayPath,
        new_path: &SafeRelayPath,
        suppress_tx: &mpsc::Sender<Suppression>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        // `old_path` is the sender's advisory old name — logged for tracing only;
        // the move resolves the real local source by id inside `place_rename`.
        info!(%document_id, %old_path, %new_path, "remote document renamed");

        // Path-arbitration conflict-copy: if the authoritative path is held by a
        // *different* live document, defer this move until the relay vacates that
        // occupant. A rename is never a revival of this kind, so it is never
        // exempt. The not-deferred branch clears any earlier deferral (e.g. a
        // stale "wants the contested path" superseded by this displacement to its
        // conflict path) — see `defer_if_occupied`.
        if self.defer_if_occupied(new_path, document_id, false) {
            return Ok(());
        }

        // Place the document at the authoritative path (move if tracked, else
        // register + subscribe) and cascade the drain onto any path it vacated.
        if let Some(freed) = self
            .materialize_at(document_id, new_path, suppress_tx, sync_cmd_tx)
            .await?
        {
            self.drain_pending(freed, suppress_tx, sync_cmd_tx).await?;
        }
        Ok(())
    }

    /// Move the document's file/identity to the authoritative `new_rel`,
    /// reconciling by document id. Returns the path it vacated (so the caller can
    /// materialize any placement deferred on it), or `None` if nothing moved.
    /// Called only via [`Self::materialize_at`], and only for a TRACKED document.
    async fn place_rename(
        &mut self,
        document_id: &str,
        new_rel: &Path,
        suppress_tx: &mpsc::Sender<Suppression>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<Option<PathBuf>> {
        // Reconcile by document id: rename whatever path we currently hold to the
        // authoritative `new_rel`. The local source is resolved from
        // `uuid_to_path` (NOT a broadcast advisory old path), so a concurrent
        // rename/rename can't strand the file at its old local name (the §1.3
        // fix). `materialize_at` only calls here for a tracked document, so the
        // lookup succeeds; the fallback to `new_rel` is defensive (a self-rename,
        // a no-op the disk-reconcile guard already handles).
        let old_rel = self
            .uuid_to_path
            .get(document_id)
            .cloned()
            .unwrap_or_else(|| new_rel.to_path_buf());

        // Incorporate any pending local edits at the old path before
        // renaming — once we rename, the old-path watcher event would
        // find nothing and the edits would be lost.
        //
        // Derive the absolute source from `old_rel` (where the file actually is
        // locally), NOT the broadcast `old_path`. Under a concurrent rename/rename
        // our local path has already diverged from the sender's `old_path` (e.g.
        // the sender renamed foo→bar_b while we renamed foo→bar_a, so we hold
        // bar_a). Using the advisory `old_path` here strands the file: the
        // disk-move guard below (`abs_old.exists()`) would test the sender's old
        // name, find it absent, skip the move, and leave the file at our local
        // name forever (the rename/rename split). `old_rel` already
        // falls back to `old_path` when we don't track the document, so the
        // untracked case is unchanged.
        let abs_old = self.config.space_root.join(&old_rel);
        if let Ok(doc) = self.doc_manager.load_or_create(document_id)
            && let Some((ops, meta)) = incorporate_pending_edits(
                &abs_old,
                &old_rel,
                doc,
                &self.agent_name,
                &self.config.author_did,
            )?
        {
            sync_cmd_tx
                .send(SyncCommand::SendOps {
                    document_id: document_id.to_owned(),
                    ops,
                    metadata: meta,
                    content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                    content_hash: Vec::new(),
                })
                .await
                .context("failed to push pending edit ops before rename")?;
            self.last_synced
                .insert(old_rel.clone(), doc.local_version());
            self.doc_manager.save(document_id)?;
        }

        self.move_identity(&old_rel, new_rel.to_path_buf(), document_id);

        // The CRDT sidecar is keyed by the stable document id — nothing to move.

        // Move last_synced.
        if let Some(v) = self.last_synced.remove(&old_rel) {
            self.last_synced.insert(new_rel.to_path_buf(), v);
        }

        // Reconcile the rename on disk (move if still at the old path, else
        // conform/materialize at the new path).
        self.reconcile_disk_for_rename(document_id, &old_rel, &abs_old, new_rel, suppress_tx)
            .await;

        // Report the vacated path so the caller can materialize any placement
        // deferred on it. Equal old/new (an idempotent re-rename) frees nothing.
        let moved = old_rel.as_path() != new_rel;
        Ok(moved.then_some(old_rel))
    }

    /// Apply a rename to disk: move the file from `abs_old` to `new_rel` if it is
    /// still at the old path, otherwise conform a concurrently-relocated file (or
    /// materialize from the CRDT) at the new path.
    ///
    /// Two load-bearing invariants, both guaranteed by the sole caller
    /// [`Self::place_rename`] (and the reason this stays a private helper, not a
    /// general utility): `move_identity` MUST have already run — it records inode
    /// `None` because the file doesn't exist at the new path yet, and `rename_doc`
    /// refreshes the inode AFTER the move so a later local rename of the
    /// just-renamed file is detected; and `abs_old` MUST derive from the local
    /// `old_rel`, NOT the advisory broadcast `old_path` — testing the sender's
    /// old name would skip the move and strand the file (§1.3 rename/rename split).
    async fn reconcile_disk_for_rename(
        &mut self,
        document_id: &str,
        old_rel: &Path,
        abs_old: &Path,
        new_rel: &Path,
        suppress_tx: &mpsc::Sender<Suppression>,
    ) {
        let abs_new = self.config.space_root.join(new_rel);
        if abs_old.exists() && !abs_new.exists() {
            if let Err(e) = rename_doc(
                &mut self.file_identity,
                old_rel,
                abs_old,
                new_rel,
                &abs_new,
                suppress_tx,
            )
            .await
            {
                error!(error = %e, "failed to apply remote rename to disk");
            }
        } else if !abs_new.exists() {
            self.conform_or_materialize_at(document_id, old_rel, new_rel, &abs_new, suppress_tx)
                .await;
        }
    }

    /// Materialize `document_id` at `target` — the one place the
    /// "tracked → move it; untracked → register + subscribe" fork lives.
    ///
    /// Tracked elsewhere on disk → [`Self::place_rename`] moves whatever we hold
    /// onto `target` (returning the vacated path so the caller cascades drains).
    /// Untracked → we hold no file or CRDT to move, so register + subscribe and
    /// the content streams in to land at `target` (`place_rename`'s
    /// materialize-from-CRDT branch would otherwise write an empty file — there is
    /// no sidecar to read yet); nothing is vacated, so `Ok(None)`.
    ///
    /// Used by both the genuine remote rename ([`Self::handle_remote_rename`],
    /// `target` = the new path) and the drain ([`Self::drain_pending`], `target`
    /// = the freed path) — replacing the former `place_rename(id, &path, &path)`
    /// `old==new` sentinel with an explicit single-argument target.
    async fn materialize_at(
        &mut self,
        document_id: &str,
        target: &SafeRelayPath,
        suppress_tx: &mpsc::Sender<Suppression>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<Option<PathBuf>> {
        if self.uuid_to_path.contains_key(document_id) {
            self.place_rename(document_id, target.as_path(), suppress_tx, sync_cmd_tx)
                .await
        } else {
            self.place_register(target.clone(), document_id, sync_cmd_tx)
                .await?;
            Ok(None)
        }
    }

    /// The authoritative rename target has no file on disk: either a concurrent
    /// LOCAL rename relocated this document's file elsewhere (carrying its content
    /// inode), or no file carries the inode. CONFORM the relocated file to the
    /// authoritative path (so a stale local-rename echo can't re-win the LWW),
    /// or MATERIALIZE from the CRDT when the file was deleted
    /// locally while the relay holds the document alive. Errors are logged, not
    /// propagated (best-effort disk reconciliation; the next reconcile is the
    /// backstop).
    async fn conform_or_materialize_at(
        &mut self,
        document_id: &str,
        old_rel: &Path,
        new_rel: &Path,
        abs_new: &Path,
        suppress_tx: &mpsc::Sender<Suppression>,
    ) {
        match self.space_file_with_inode(self.recorded_inode(new_rel)) {
            // Only conform for a GENUINE rename (`new_rel != old_rel`). A no-op
            // rename (old == new — a stale rebroadcast of the pre-rename path that
            // beat our own not-yet-processed local rename through the freshness
            // gate) has no distinct target, so conforming would pull the relocated
            // file BACK, undoing the local rename.
            Some(relocated) if relocated.as_path() != new_rel && new_rel != old_rel => {
                let abs_relocated = self.config.space_root.join(&relocated);
                if let Err(e) = rename_doc(
                    &mut self.file_identity,
                    &relocated,
                    &abs_relocated,
                    new_rel,
                    abs_new,
                    suppress_tx,
                )
                .await
                {
                    error!(error = %e, "failed to conform relocated file to authoritative path");
                }
            }
            Some(_) => {} // already at the authoritative path — nothing to do
            None => {
                // No file carries the inode: the document was deleted locally, yet
                // this rename means the relay holds it alive (a delete-superseding
                // rename passed the gate). Materialize at the authoritative path.
                let content = self
                    .doc_manager
                    .get(document_id)
                    .map_or_else(String::new, kutl_core::Document::content);
                if let Err(e) = write_doc(
                    &mut self.file_identity,
                    new_rel,
                    abs_new,
                    content.as_bytes(),
                    suppress_tx,
                )
                .await
                {
                    error!(error = %e, "failed to materialize remote-alive document absent on disk");
                }
            }
        }
    }

    /// Materialize documents whose placement was deferred because their effective
    /// path was held by another document, now that `freed_path` has been vacated.
    ///
    /// Cascades through a worklist: a drained move may itself free a path another
    /// deferred document was waiting on. The relay guarantees the deferred
    /// targets eventually form a collision-free assignment (one alive document
    /// per effective path), so the worklist drains to empty.
    async fn drain_pending(
        &mut self,
        freed_path: PathBuf,
        suppress_tx: &mpsc::Sender<Suppression>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        let mut work = vec![freed_path];
        while let Some(path) = work.pop() {
            // Documents waiting for exactly this path. Snapshot the ids: the body
            // mutates `pending_placements`.
            let waiting: Vec<String> = self
                .pending_placements
                .iter()
                .filter(|(_, target)| target.as_path() == path.as_path())
                .map(|(id, _)| id.clone())
                .collect();
            for document_id in waiting {
                // Re-check: an earlier drain in this loop may have taken the path,
                // or a newer relay message may have re-targeted this waiter.
                if self.path_occupied_by_other(&path, &document_id) {
                    continue;
                }
                let Some(target) = self.pending_placements.get(&document_id).cloned() else {
                    continue;
                };
                if target.as_path() != path.as_path() {
                    continue;
                }
                self.resolve_placement(&document_id);

                // Place the waiter at the freed path (move if tracked, else
                // register + subscribe). `target` is the carried, already-
                // validated `SafeRelayPath` (== `path`, the re-check above), so
                // there is no re-parse. A move may itself free another path —
                // cascade it onto the worklist.
                if let Some(freed) = self
                    .materialize_at(&document_id, &target, suppress_tx, sync_cmd_tx)
                    .await?
                {
                    work.push(freed);
                }
            }
        }
        Ok(())
    }

    /// Handle remote text CRDT ops: merge → write to disk.
    ///
    /// Before merging remote ops, checks whether the file on disk has
    /// pending local edits (written by the user but not yet processed
    /// by the watcher). If so, applies them to the CRDT *first* — this
    /// ensures the local edit positions are computed against the base
    /// state, and the subsequent remote merge handles concurrent position
    /// transforms correctly via the CRDT engine.
    async fn handle_remote_text(
        &mut self,
        document_id: &str,
        ops: Vec<u8>,
        metadata: Vec<ChangeMetadata>,
        suppress_tx: &mpsc::Sender<Suppression>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        // Skip a content op for an untracked UUID (locally deleted or not yet
        // registered) — see `is_untracked_uuid` for why materializing it would
        // diverge (the §1.5 edit-vs-delete race).
        if self.is_untracked_uuid(document_id) {
            debug!(%document_id, "skipping remote content op for untracked document");
            return Ok(());
        }

        // Empty ops = document exists but has no content yet. Ensure the
        // file exists on disk (may be a zero-byte file).
        if ops.is_empty() {
            let rel_path = self.resolve_path(document_id)?;
            let abs_path = self.doc_manager.file_path(&rel_path);
            if !abs_path.exists() {
                write_doc(
                    &mut self.file_identity,
                    &rel_path,
                    &abs_path,
                    b"",
                    suppress_tx,
                )
                .await?;
            }
            return Ok(());
        }

        // Look up the UUID→path mapping, falling back to treating the ID as a path
        // for backwards compatibility with pre-UUID documents.
        let rel_path = self.resolve_path(document_id)?;
        let abs_path = self.doc_manager.file_path(&rel_path);

        let doc = self.doc_manager.load_or_create(document_id)?;

        // Incorporate pending local edits BEFORE merging remote ops so
        // the CRDT engine handles concurrent position transforms.
        let local_ops_to_push = incorporate_pending_edits(
            &abs_path,
            &rel_path,
            doc,
            &self.agent_name,
            &self.config.author_did,
        )?;

        // Merge remote ops.
        let version_before_merge = doc.local_version();
        doc.merge(&ops, &metadata)?;
        let version_after_merge = doc.local_version();

        let state_changed =
            local_ops_to_push.is_some() || version_after_merge != version_before_merge;

        // Push local edit ops to the relay so other clients see them.
        if let Some((local_ops, local_meta)) = local_ops_to_push {
            sync_cmd_tx
                .send(SyncCommand::SendOps {
                    document_id: document_id.to_owned(),
                    ops: local_ops,
                    metadata: local_meta,
                    content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                    content_hash: Vec::new(),
                })
                .await?;
        }

        // Only write the file if the state actually changed.
        // Redundant merges (e.g. receiving our own ops back via catch-up)
        // must NOT write + suppress, or we risk swallowing a concurrent
        // local file edit's watcher event.
        if state_changed {
            let content = doc.content();

            // If the file is gone from its expected path but we had a known
            // inode, the user probably renamed or deleted it locally. Don't
            // recreate the file — the watcher event will handle the new path.
            // We still merge ops into the CRDT above so the content is current.
            //
            // The `inode.is_some()` check is load-bearing: it distinguishes a
            // file that existed locally and is now gone (renamed/deleted — has
            // an inode recorded at registration) from a brand-new remote
            // document that is tracked from the space registry but has never
            // been written here (no inode — must be written). Gating on
            // tracked-path presence alone would skip writing every new remote
            // file, breaking initial sync of a peer's documents.
            if should_skip_remote_write(abs_path.exists(), self.file_identity.get(&rel_path)) {
                debug!(
                    path = %rel_path.display(),
                    "skipping write: file gone but has known inode (likely renamed)"
                );
                self.last_synced
                    .insert(rel_path.clone(), version_after_merge.clone());
                self.doc_manager.save(document_id)?;
                return Ok(());
            }

            let was_absent = !abs_path.exists();
            write_doc(
                &mut self.file_identity,
                &rel_path,
                &abs_path,
                content.as_bytes(),
                suppress_tx,
            )
            .await?;
            // First materialization of a remote document: `register_identity`
            // persisted a null inode (the file did not exist at SubscribeRemote
            // time) and `write_doc` just refreshed the live inode. Persist it now
            // so a rename of this file *while the daemon is offline* stays
            // locatable by inode on restart (its recorded path will be gone, so
            // the inode cannot be re-read from disk). Only on first create — the
            // inode is stable across later content edits, so this is not hot.
            if was_absent {
                self.save_state();
            }
        }

        self.last_synced
            .insert(rel_path.clone(), version_after_merge);
        self.doc_manager.save(document_id)?;

        Ok(())
    }

    /// Handle a local binary file change: hash, compare, send to relay.
    async fn handle_blob_change(
        &mut self,
        rel_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        let abs_path = self.doc_manager.file_path(rel_path);
        let bytes = match std::fs::read(&abs_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(e).with_context(|| format!("failed to read {}", abs_path.display()));
            }
        };

        if bytes.len() > ABSOLUTE_BLOB_MAX {
            warn!(
                path = %abs_path.display(),
                size = bytes.len(),
                limit = ABSOLUTE_BLOB_MAX,
                "blob too large, skipping"
            );
            return Ok(());
        }

        let hash_hex = sha256_hex(&bytes);

        // Skip if hash unchanged.
        if let Some(existing) = self.blob_state.get(rel_path)
            && existing.hash == hash_hex
        {
            return Ok(());
        }

        let is_new = self.blob_state.get(rel_path).is_none();
        let document_id = self.get_or_create_uuid(rel_path);

        // Register and subscribe if new document.
        if is_new {
            self.register_and_subscribe(
                sync_cmd_tx,
                &document_id,
                &rel_path_to_string(rel_path),
                "file change",
            )
            .await?;
        }

        let hash_bytes = sha256_bytes(&bytes);
        let metadata = self.make_metadata("file change");
        let timestamp = metadata.timestamp;

        #[allow(clippy::cast_possible_wrap)]
        let size = bytes.len() as i64;

        // Account this blob as backlogged until the client task drains it from
        // the channel (`blob_upload_backlog` gauge).
        self.blob_backlog.fetch_add(1, Ordering::Relaxed);
        sync_cmd_tx
            .send(SyncCommand::SendOps {
                document_id,
                ops: bytes,
                metadata: vec![metadata],
                content_mode: i32::from(kutl_proto::sync::ContentMode::Blob),
                content_hash: hash_bytes,
            })
            .await?;

        self.blob_state.insert(
            rel_path.to_owned(),
            BlobState {
                hash: hash_hex,
                timestamp,
                size,
            },
        );
        self.blob_state.save(&self.config.space_root)?;

        Ok(())
    }

    /// Handle a remote binary blob: LWW by timestamp, write to disk.
    async fn handle_remote_blob(
        &mut self,
        document_id: &str,
        ops: Vec<u8>,
        metadata: Vec<ChangeMetadata>,
        _content_hash: Vec<u8>,
        suppress_tx: &mpsc::Sender<Suppression>,
    ) -> Result<()> {
        // Empty ops are a catch-up signal for documents with no content yet.
        if ops.is_empty() {
            return Ok(());
        }

        // Skip a blob op for an untracked UUID — same §1.5 hazard as the text
        // path: `resolve_legacy_path` would otherwise coerce the id into a
        // garbage `<uuid>` file. Content is re-delivered by the post-register
        // catch-up.
        if self.is_untracked_uuid(document_id) {
            debug!(%document_id, "skipping remote blob op for untracked document");
            return Ok(());
        }

        let rel_path = self.resolve_path(document_id)?;
        let abs_path = self.doc_manager.file_path(&rel_path);

        let remote_timestamp = metadata.first().map_or(0, |m| m.timestamp);
        let remote_hash = sha256_hex(&ops);

        // LWW with a deterministic content-hash tiebreak on equal timestamps:
        // without it, two daemons writing different blobs in the same
        // millisecond reject each other and stay divergent. Comparing hashes
        // (keep the lexicographically greater) makes all replicas converge.
        if let Some(existing) = self.blob_state.get(&rel_path)
            && (remote_timestamp < existing.timestamp
                || (remote_timestamp == existing.timestamp && remote_hash <= existing.hash))
        {
            return Ok(());
        }

        write_doc(
            &mut self.file_identity,
            &rel_path,
            &abs_path,
            &ops,
            suppress_tx,
        )
        .await?;

        #[allow(clippy::cast_possible_wrap)]
        let size = ops.len() as i64;

        self.blob_state.insert(
            rel_path,
            BlobState {
                hash: remote_hash,
                timestamp: remote_timestamp,
                size,
            },
        );
        self.blob_state.save(&self.config.space_root)?;

        Ok(())
    }
}

/// Read the `KUTL_CLOCK_SKEW_MS` test seam: a signed millisecond offset added to
/// every HLC physical-time reading this daemon takes. Unset or unparseable → 0
/// (no skew). Used only by the reproducibility-under-skew acceptance test;
/// production never sets it.
fn read_clock_skew_ms() -> i64 {
    std::env::var("KUTL_CLOCK_SKEW_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Convert a relative path to a forward-slash string for use in wire messages.
///
/// This is the same logic as the deprecated `path_to_document_id`, but is used
/// internally for path→string in lifecycle message payloads (not as document identity).
fn rel_path_to_string(rel_path: &Path) -> String {
    rel_path
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Find the previously-tracked document that a newly-observed file was renamed
/// from, based on a matching inode.
///
/// A matching inode alone is **not** sufficient evidence of a rename: the
/// kernel reuses inode numbers after a file is unlinked, so a freshly created
/// file can be handed a freed inode that a still-tracked document recorded
/// earlier (the daemon does not refresh a document's stored inode on every
/// atomic content rewrite, so the recorded value is frequently a now-freed
/// inode). We therefore only treat the match as a rename when the candidate
/// old path no longer exists on disk: a genuine rename moves the file away from
/// its old path, whereas inode reuse leaves the old file untouched in place.
///
/// `old_path_exists` reports whether a candidate old path is still present on
/// disk; it is injected so the decision can be unit-tested without provoking a
/// real kernel inode reuse.
fn find_rename_source(
    file_identity: &HashMap<PathBuf, FileIdentity>,
    new_inode: u64,
    old_path_exists: impl Fn(&Path) -> bool,
) -> Option<(PathBuf, String)> {
    file_identity
        .iter()
        .find(|(old_path, id)| id.inode == Some(new_inode) && !old_path_exists(old_path))
        .map(|(old_path, id)| (old_path.clone(), id.document_uuid.clone()))
}

/// The inode to record for a document after moving its identity to a new path.
///
/// Prefer the file actually on disk at the new path. When the new path has no
/// file yet, fall back to the document's previously-recorded inode rather than
/// recording `None`.
///
/// The fallback is load-bearing for concurrent-rename convergence. When a remote
/// rename for document `D` arrives before the local file has reached the
/// authoritative path — e.g. the user concurrently renamed `D` to a *different*
/// local name, so the file currently sits there carrying `D`'s inode while the
/// authoritative path is not yet on disk — recording `None` would discard the
/// inode that links `D` to its on-disk file. The relocated file would then look
/// untracked to [`find_rename_source`] and be minted as a spurious new document,
/// splitting `D` in two. Preserving the inode keeps the relocated file matchable
/// as `D`, so the local rename is re-attributed to `D` and the relay's lattice
/// arbitrates a single winning path. (A genuinely deleted document is removed
/// from `file_identity` entirely, so it can never be matched here.)
fn moved_inode(new_path_on_disk: Option<u64>, previously_recorded: Option<u64>) -> Option<u64> {
    new_path_on_disk.or(previously_recorded)
}

/// Update the recorded inode for an already-tracked path to its current
/// on-disk value.
///
/// Editors rewrite files via the atomic tmp-rename dance, which gives the file
/// a fresh inode on every save. If the recorded inode is never refreshed it
/// goes stale, which causes two distinct failures: a genuine later rename of
/// the file is *missed* (its current inode no longer matches the recorded one,
/// see [`find_rename_source`]), and the stale value is a now-freed inode that a
/// newly created file may be assigned, inviting a false rename match. Keeping
/// the recorded inode current on every observed change avoids both.
///
/// No-op if the path is not tracked or the inode could not be read.
fn refresh_inode(
    file_identity: &mut HashMap<PathBuf, FileIdentity>,
    rel_path: &Path,
    current_inode: Option<u64>,
) {
    if let Some(identity) = file_identity.get_mut(rel_path) {
        identity.inode = current_inode;
    }
}

/// Decide whether to skip writing incoming remote content for a tracked path.
///
/// Returns true only when the file is absent from disk AND it was previously
/// present locally — a tracked identity whose inode was recorded at
/// registration. That means the user renamed or deleted it locally, so we must
/// not recreate it (the watcher event will handle the new path).
///
/// A brand-new remote document is tracked from the space registry but has no
/// recorded inode (nothing was ever stat'd on disk for it), so this returns
/// false and the content is written — initial sync of a peer's documents
/// depends on this. The inode presence is the load-bearing distinction:
/// gating on tracked-path presence alone would skip writing every new remote
/// file.
///
/// Related but deliberately SEPARATE from [`Self::conform_or_materialize_at`],
/// which reasons about the same "tracked file gone from its recorded path" fact
/// on the rename path. They are kept apart on purpose by COST: this is a cheap
/// two-state predicate (one stat + a map lookup, can't tell "relocated" from
/// "gone") run on every content op; `conform_or_materialize_at` is the finer
/// three-state check that walks the tree by inode (O(files)) and so must NOT run
/// per content op. Do not unify them behind one classifier without preserving
/// that asymmetry.
fn should_skip_remote_write(file_on_disk: bool, identity: Option<&FileIdentity>) -> bool {
    !file_on_disk && identity.is_some_and(|id| id.inode.is_some())
}

// ── Disk-mutation funnel ────────────────────────────────────────────────────
//
// Every daemon-originated filesystem mutation goes through one of `write_doc`,
// `rename_doc` or `remove_doc`. Each bundles the echo suppression *with* the
// mutation, so the two cannot drift apart: a write registers `(path,
// Some(content_hash))` (the watcher then recognizes its own echo, while a
// genuine concurrent edit — different bytes — is never swallowed); a removal
// registers `(path, None)`. Each write/rename also refreshes the recorded inode,
// keeping the invariant *"a tracked file's recorded inode reflects its on-disk
// identity whenever the file exists"* that rename detection relies on. There is
// no place to forget the suppression or the refresh, because there is no other
// way to touch the filesystem.

/// Funnel: write `content` to a document path, suppressing the resulting echo by
/// its content hash and refreshing the recorded inode.
async fn write_doc(
    file_identity: &mut HashMap<PathBuf, FileIdentity>,
    rel_path: &Path,
    abs_path: &Path,
    content: &[u8],
    suppress_tx: &mpsc::Sender<Suppression>,
) -> Result<()> {
    if let Some(parent) = abs_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = suppress_tx
        .send((rel_path.to_path_buf(), Some(sha256_bytes(content))))
        .await;
    std::fs::write(abs_path, content)
        .with_context(|| format!("failed to write {}", abs_path.display()))?;
    refresh_inode(file_identity, rel_path, crate::inode::get_inode(abs_path));
    Ok(())
}

/// Funnel: rename a document on disk, suppressing both echo halves (old-path
/// removal, new-path write keyed by the renamed content) and refreshing the
/// recorded inode at the new path.
async fn rename_doc(
    file_identity: &mut HashMap<PathBuf, FileIdentity>,
    old_rel: &Path,
    old_abs: &Path,
    new_rel: &Path,
    new_abs: &Path,
    suppress_tx: &mpsc::Sender<Suppression>,
) -> Result<()> {
    if let Some(parent) = new_abs.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let renamed_hash = std::fs::read(old_abs).ok().map(|b| sha256_bytes(&b));
    let _ = suppress_tx.send((old_rel.to_path_buf(), None)).await;
    let _ = suppress_tx
        .send((new_rel.to_path_buf(), renamed_hash))
        .await;
    std::fs::rename(old_abs, new_abs).with_context(|| {
        format!(
            "failed to rename {} to {}",
            old_abs.display(),
            new_abs.display()
        )
    })?;
    refresh_inode(file_identity, new_rel, crate::inode::get_inode(new_abs));
    Ok(())
}

/// Funnel: remove a document from disk, suppressing the resulting removal echo.
async fn remove_doc(
    rel_path: &Path,
    abs_path: &Path,
    suppress_tx: &mpsc::Sender<Suppression>,
) -> Result<()> {
    let _ = suppress_tx.send((rel_path.to_path_buf(), None)).await;
    std::fs::remove_file(abs_path)
        .with_context(|| format!("failed to remove {}", abs_path.display()))?;
    Ok(())
}

/// Encode ops and metadata as a delta since the given version.
fn encode_delta(doc: &kutl_core::Document, since: &[usize]) -> (Vec<u8>, Vec<ChangeMetadata>) {
    let ops = doc.encode_since(since);
    let metadata = doc.changes_since(since);
    (ops, metadata)
}

/// Incorporate pending local file edits into the CRDT.
///
/// Reads the file at `abs_path`, diffs against the CRDT content, and
/// applies any changes. Returns `(ops, metadata)` for the delta, or
/// `None` if the file matches the CRDT or doesn't exist on disk.
fn incorporate_pending_edits(
    abs_path: &Path,
    rel_path: &Path,
    doc: &mut kutl_core::Document,
    agent_name: &str,
    author_did: &str,
) -> Result<Option<(Vec<u8>, Vec<ChangeMetadata>)>> {
    if !abs_path.exists() {
        return Ok(None);
    }
    let Ok(file_content) = std::fs::read_to_string(abs_path) else {
        return Ok(None);
    };
    if file_content == doc.content() {
        return Ok(None);
    }

    debug!(path = %rel_path.display(), "incorporating pending local edit");
    let version_before = doc.local_version();
    let agent = doc.register_agent(agent_name)?;
    bridge::apply_file_change(doc, agent, author_did, &file_content)?;

    let (ops, meta) = encode_delta(doc, &version_before);
    if ops.is_empty() {
        Ok(None)
    } else {
        Ok(Some((ops, meta)))
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

    fn identity(uuid: &str, inode: u64) -> FileIdentity {
        FileIdentity {
            document_uuid: uuid.to_owned(),
            inode: Some(inode),
        }
    }

    /// A new file whose inode matches a tracked document whose old path is gone
    /// is a genuine rename — the source path and UUID are reported.
    #[test]
    fn test_find_rename_source_matches_when_old_path_gone() {
        let mut map = HashMap::new();
        map.insert(PathBuf::from("notes.md"), identity("uuid-notes", 42));

        let found = find_rename_source(&map, 42, |_| false);

        assert_eq!(
            found,
            Some((PathBuf::from("notes.md"), "uuid-notes".to_owned()))
        );
    }

    /// Inode-reuse guard: when the matched document's old path still exists on
    /// disk, the inode match is a coincidence — the kernel reused a freed inode
    /// for an unrelated new file — so it must NOT be reported as a rename.
    ///
    /// Reproduces the false-rename bug: `resumable.md` is still tracked and
    /// still on disk, but its recorded inode (freed by an earlier atomic
    /// rewrite) was reused by a newly created file at a different path.
    #[test]
    fn test_find_rename_source_rejects_when_old_path_still_exists() {
        let mut map = HashMap::new();
        map.insert(
            PathBuf::from("resumable.md"),
            identity("uuid-resumable", 99),
        );

        let found = find_rename_source(&map, 99, |_| true);

        assert_eq!(found, None);
    }

    /// No tracked document has the observed inode — nothing is reported.
    #[test]
    fn test_find_rename_source_no_inode_match() {
        let mut map = HashMap::new();
        map.insert(PathBuf::from("a.md"), identity("uuid-a", 1));

        assert_eq!(find_rename_source(&map, 2, |_| false), None);
    }

    /// A recorded inode goes stale when an editor rewrites the file (atomic
    /// tmp-rename changes the inode). A stale inode masks a genuine later
    /// rename — `find_rename_source` can't match the current inode — until the
    /// recorded value is refreshed. Reproduces the missed-rename bug.
    #[test]
    fn test_stale_inode_masks_rename_until_refreshed() {
        let mut map = HashMap::new();
        // foo.md was registered at inode 7; an atomic rewrite later moved its
        // content to inode 8, but the recorded value is still the freed 7.
        map.insert(PathBuf::from("foo.md"), identity("uuid-foo", 7));

        // foo.md is renamed to bar.md; rename preserves the current inode (8),
        // and foo.md no longer exists. The stale recorded inode (7) masks it:
        assert_eq!(find_rename_source(&map, 8, |_| false), None);

        // Refreshing the recorded inode to the file's current value restores
        // detection of the rename.
        refresh_inode(&mut map, Path::new("foo.md"), Some(8));
        assert_eq!(
            find_rename_source(&map, 8, |_| false),
            Some((PathBuf::from("foo.md"), "uuid-foo".to_owned()))
        );
    }

    /// Concurrent-rename convergence: a remote rename of `D` (`foo`→`bar_a`)
    /// arrives on a worker that has *locally* renamed the same file to a
    /// different name (`foo`→`bar_b`). The local file sits at `bar_b` carrying
    /// `D`'s inode; `bar_a` is not yet on disk. Moving `D`'s identity to `bar_a`
    /// must preserve its inode so the watcher re-attributes `bar_b` to `D`
    /// instead of minting a new document. Recording `bar_a`'s (absent) inode as
    /// `None` is the bug that splits `D`.
    #[test]
    fn test_moved_inode_preserves_identity_when_new_path_absent() {
        const D_INODE: u64 = 7;
        let mut map = HashMap::new();
        map.insert(PathBuf::from("foo.md"), identity("D", D_INODE));

        // move_identity(foo→bar_a) with bar_a absent on disk (its inode is None).
        let old_inode = map.get(Path::new("foo.md")).and_then(|id| id.inode);
        map.remove(Path::new("foo.md"));
        map.insert(
            PathBuf::from("bar_a.md"),
            FileIdentity {
                document_uuid: "D".to_owned(),
                inode: moved_inode(None, old_inode),
            },
        );

        // The local relocation (bar_b carries D's inode; bar_a is not on disk) is
        // recognized as a rename of D — not an untracked file to mint anew.
        assert_eq!(
            find_rename_source(&map, D_INODE, |_| false),
            Some((PathBuf::from("bar_a.md"), "D".to_owned())),
            "relocated file must be matched as D, not minted as a new document"
        );
    }

    /// When the new path *does* have a file on disk (the ordinary local-rename
    /// case), its on-disk inode is recorded — the fallback never masks reality.
    #[test]
    fn test_moved_inode_prefers_on_disk_inode() {
        assert_eq!(moved_inode(Some(9), Some(7)), Some(9));
        assert_eq!(moved_inode(None, Some(7)), Some(7));
        assert_eq!(moved_inode(None, None), None);
    }

    /// Refreshing an untracked path is a no-op (no spurious entry created).
    #[test]
    fn test_refresh_inode_untracked_path_is_noop() {
        let mut map = HashMap::new();
        refresh_inode(&mut map, Path::new("ghost.md"), Some(5));
        assert!(map.is_empty());
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
        let mut map = HashMap::new();
        map.insert(
            rel.clone(),
            FileIdentity {
                document_uuid: "uuid-foo".to_owned(),
                inode: None,
            },
        );

        let (tx, mut rx) = mpsc::channel::<Suppression>(8);
        write_doc(&mut map, &rel, &abs, b"hello", &tx)
            .await
            .unwrap();

        // The funnel emitted a suppression keyed by the content hash it wrote —
        // the watcher will treat the echo as ours, but a different-content edit
        // would not match.
        let (sup_path, sup_hash) = rx.try_recv().expect("write_doc emits a suppression");
        assert_eq!(sup_path, rel);
        assert_eq!(
            sup_hash,
            Some(sha256_bytes(b"hello")),
            "suppression carries the written content's hash"
        );

        // The file now exists and its real inode is recorded.
        let recorded = map[&rel].inode;
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
                find_rename_source(&map, ino, |_| false),
                Some((rel.clone(), "uuid-foo".to_owned())),
                "rename of a received file is detected once the funnel records its inode"
            );
        }
    }

    /// A new remote document is tracked from the registry but has no recorded
    /// inode, so incoming content MUST be written — not skipped. Gating on
    /// tracked-path presence alone (ignoring the inode) would skip it and break
    /// initial peer sync.
    #[test]
    fn test_new_remote_doc_is_written_not_skipped() {
        let tracked_without_inode = FileIdentity {
            document_uuid: "uuid-new".to_owned(),
            inode: None,
        };
        // Absent on disk + tracked-but-never-written (inode None) => write it.
        assert!(!should_skip_remote_write(
            false,
            Some(&tracked_without_inode)
        ));
    }

    /// A file that existed locally (inode recorded) and is now gone was
    /// renamed/deleted locally — skip recreating it.
    #[test]
    fn test_locally_removed_file_is_not_recreated() {
        let tracked_with_inode = identity("uuid-gone", 42);
        assert!(should_skip_remote_write(false, Some(&tracked_with_inode)));
    }

    /// A present file is always written (normal remote update); an untracked
    /// absent path is a new document and is written too.
    #[test]
    fn test_present_or_untracked_paths_are_written() {
        let tracked_with_inode = identity("uuid-x", 7);
        assert!(!should_skip_remote_write(true, Some(&tracked_with_inode)));
        assert!(!should_skip_remote_write(true, None));
        assert!(!should_skip_remote_write(false, None));
    }

    /// Build a minimal, network-free `SpaceWorker` over `space_root` for unit
    /// tests of the in-memory placement/identity handlers.
    fn test_worker(space_root: PathBuf) -> SpaceWorker {
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

    /// `offline_delete_stamp` must lift an offline delete one ms above the latest
    /// content edit it has OBSERVED, so the delete dominates the relay's content
    /// touch (`physical_touch(content_ms)`, logical = `u32::MAX`) and a genuine
    /// self-delete wins. A peer edit it has NOT observed is absent from the CRDT,
    /// so it stays above the delete's basis and revives the document (§3.4).
    #[test]
    fn test_offline_delete_stamp_is_one_ms_above_observed_content() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let doc_id = "11111111-1111-1111-1111-111111111111";

        let content_ms = {
            let doc = worker.doc_manager.load_or_create(doc_id).unwrap();
            let agent = doc.register_agent("did:test").unwrap();
            bridge::apply_file_change(doc, agent, "did:test", "hello").unwrap();
            doc.changes()
                .iter()
                .map(|c| c.timestamp)
                .max()
                .expect("a content edit recorded a change")
        };

        let stamp = worker.offline_delete_stamp(doc_id);
        assert_eq!(
            stamp.physical_ms,
            u64::try_from(content_ms).unwrap() + 1,
            "offline delete must be stamped one ms above the latest observed content edit"
        );
        assert_eq!(stamp.logical, 0);
    }

    /// A document with no observed content (no CRDT changes) has no content touch
    /// to beat, so the stamp falls back to the pre-offline floor (which covers the
    /// document's registration stamp).
    #[test]
    fn test_offline_delete_stamp_falls_back_to_floor_without_content() {
        let dir = tempfile::tempdir().unwrap();
        let worker = test_worker(dir.path().to_path_buf());
        let stamp = worker.offline_delete_stamp("22222222-2222-2222-2222-222222222222");
        assert_eq!(stamp, worker.offline_floor());
    }

    /// Regression: a remote unregister for a document still parked in
    /// `pending_placements` (deferred behind a path occupant, never placed into
    /// `uuid_to_path`) must clear the pending entry — otherwise a later
    /// `drain_pending` finds the deleted id still waiting and re-materializes it
    /// when the contested path frees, silently resurrecting a deleted document.
    #[tokio::test]
    async fn test_unregister_clears_deferred_pending_placement() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());

        // A document deferred behind an occupant of `notes.md`: it sits in
        // `pending_placements` and was never placed into `uuid_to_path`.
        worker.pending_placements.insert(
            "uuid-deferred".to_owned(),
            SafeRelayPath::new("notes.md").unwrap(),
        );

        let (sup_tx, _sup_rx) = mpsc::channel::<Suppression>(8);
        let (cmd_tx, _cmd_rx) = mpsc::channel::<SyncCommand>(8);
        worker
            .handle_remote_unregister("uuid-deferred", None, &sup_tx, &cmd_tx)
            .await
            .expect("handle remote unregister");

        assert!(
            !worker.pending_placements.contains_key("uuid-deferred"),
            "a deferred document deleted before its path frees must be removed from \
             pending_placements, else a later drain resurrects it"
        );
    }

    /// `defer_if_occupied` is the one decision register and rename both funnel
    /// through. This locks its two load-bearing behaviors against drift: a
    /// brand-new doc DEFERS behind an occupant, but a REVIVAL (exempt) PLACES
    /// onto its own contested path rather than deferring permanently into a
    /// uuid-named-file strand.
    #[test]
    fn test_defer_if_occupied_revival_exemption() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());

        // `notes.md` is held by a DIFFERENT live document.
        let occupant = "00000000-0000-0000-0000-0000000000aa";
        worker
            .uuid_to_path
            .insert(occupant.to_owned(), PathBuf::from("notes.md"));
        worker.file_identity.insert(
            PathBuf::from("notes.md"),
            FileIdentity {
                document_uuid: occupant.to_owned(),
                inode: None,
            },
        );
        let newcomer = "00000000-0000-0000-0000-0000000000bb";
        let notes = SafeRelayPath::new("notes.md").unwrap();
        let free = SafeRelayPath::new("free.md").unwrap();

        // Non-revival onto the occupied path → defer (park it).
        assert!(
            worker.defer_if_occupied(&notes, newcomer, false),
            "a brand-new doc must defer behind a path occupant"
        );
        assert_eq!(
            worker
                .pending_placements
                .get(newcomer)
                .map(SafeRelayPath::as_path),
            Some(Path::new("notes.md")),
            "the deferred doc is parked at the contested path"
        );

        // A REVIVAL onto the SAME occupied path → place, never defer (else it
        // strands permanently as a uuid-named file). Also clears the deferral.
        assert!(
            !worker.defer_if_occupied(&notes, newcomer, true),
            "a revival returns to its own path and must NOT defer"
        );
        assert!(
            !worker.pending_placements.contains_key(newcomer),
            "the revival cleared the earlier deferral"
        );

        // A free path → place, not defer.
        assert!(
            !worker.defer_if_occupied(&free, newcomer, false),
            "a free path places immediately"
        );
        assert!(!worker.pending_placements.contains_key(newcomer));
    }

    #[test]
    fn test_ack_requires_self_correction_classifies() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let doc = "11111111-1111-1111-1111-111111111111";
        worker
            .uuid_to_path
            .insert(doc.to_owned(), PathBuf::from("notes.md"));
        let conflict = format!("notes{}{doc}.md", kutl_core::lattice::CONFLICT_INFIX);

        // Won (clean effective path) → no correction.
        assert!(
            worker
                .ack_requires_self_correction(doc, "notes.md")
                .is_none()
        );
        // Untracked → no correction.
        assert!(
            worker
                .ack_requires_self_correction("22222222-2222-2222-2222-222222222222", &conflict)
                .is_none()
        );
        // Lost (conflict effective path), tracked, not yet reconciled → the move.
        let (old, new) = worker
            .ack_requires_self_correction(doc, &conflict)
            .expect("a tracked doc with a conflict effective path needs correction");
        assert_eq!(old.as_path(), Path::new("notes.md"));
        assert_eq!(new.as_path(), Path::new(&conflict));
        // Already at the conflict path → no correction.
        worker
            .uuid_to_path
            .insert(doc.to_owned(), PathBuf::from(&conflict));
        assert!(
            worker
                .ack_requires_self_correction(doc, &conflict)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_lifecycle_ack_freshness_gate_drops_stale() {
        // The typed-ack self-correction (reconcile our doc to an arbitrated
        // conflict path) must be freshness-gated: a late/reordered ack whose HLC
        // is older than a newer lifecycle op already applied for the document is
        // dropped, not re-applied against the newer state.
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let doc = "11111111-1111-1111-1111-111111111111";

        // The doc is tracked at its clean path, and a NEWER lifecycle op has
        // already advanced its watermark.
        worker
            .uuid_to_path
            .insert(doc.to_owned(), PathBuf::from("doc.md"));
        worker.file_identity.insert(
            PathBuf::from("doc.md"),
            FileIdentity {
                document_uuid: doc.to_owned(),
                inode: None,
            },
        );
        worker.record_lifecycle_hlc(doc, Hlc::physical_touch(5000));

        let (sup_tx, _sup_rx) = mpsc::channel::<Suppression>(8);
        let (cmd_tx, _cmd_rx) = mpsc::channel::<SyncCommand>(8);
        // A STALE ack (lower HLC) telling us we lost the path — must be dropped.
        let conflict = format!("doc.kutl-conflict-{doc}.md");
        worker
            .handle_lifecycle_ack(
                doc,
                Some(conflict),
                Some(Hlc::physical_touch(2000)),
                &sup_tx,
                &cmd_tx,
            )
            .await
            .expect("handle lifecycle ack");

        assert_eq!(
            worker.uuid_to_path.get(doc),
            Some(&PathBuf::from("doc.md")),
            "a stale lifecycle ack must not move the document off its current path"
        );
    }

    /// A remote content op for an UNTRACKED UUID (no `uuid_to_path` entry — e.g.
    /// locally deleted) must not materialize a garbage `<uuid>` file via the
    /// UUID-as-path fallback (the §1.5 edit-vs-delete divergence). A TRACKED doc
    /// still gets its real file ensured.
    #[tokio::test]
    async fn test_untracked_uuid_content_op_writes_no_garbage_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (sup_tx, _sup_rx) = mpsc::channel::<Suppression>(8);
        let (cmd_tx, _cmd_rx) = mpsc::channel::<SyncCommand>(8);

        // Untracked UUID (no path mapping): empty-ops "ensure file exists" must
        // be skipped — no `<uuid>` file may appear.
        let untracked = "33333333-3333-3333-3333-333333333333";
        worker
            .handle_remote_text(untracked, vec![], vec![], &sup_tx, &cmd_tx)
            .await
            .expect("untracked content op is dropped, not an error");
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !names.iter().any(|n| n.contains(untracked)),
            "no garbage <uuid> file materialized for an untracked doc: {names:?}"
        );

        // A TRACKED doc still gets its file ensured (the guard must not over-skip).
        let tracked = "44444444-4444-4444-4444-444444444444";
        worker
            .uuid_to_path
            .insert(tracked.to_owned(), PathBuf::from("real.md"));
        worker
            .handle_remote_text(tracked, vec![], vec![], &sup_tx, &cmd_tx)
            .await
            .expect("tracked content op handled");
        assert!(
            dir.path().join("real.md").exists(),
            "a tracked doc's empty-ops still ensures its real file exists"
        );
    }

    #[tokio::test]
    async fn test_untracked_uuid_blob_op_writes_no_garbage_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (sup_tx, _sup_rx) = mpsc::channel::<Suppression>(8);

        // Untracked UUID + non-empty blob ops: must be skipped (the §1.5 hazard
        // on the blob path), no `<uuid>` file may appear.
        let untracked = "33333333-3333-3333-3333-333333333333";
        worker
            .handle_remote_blob(untracked, b"binary".to_vec(), vec![], Vec::new(), &sup_tx)
            .await
            .expect("untracked blob op is dropped, not an error");
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

    /// §1.6 edit-during-rename: one daemon processing a remote edit and a remote
    /// rename of the same doc — in EITHER order — must converge with the edit
    /// present at the renamed path. Deterministic single-daemon reduction of the
    /// wall-clock multi-daemon e2e (`test_edit_during_concurrent_rename`):
    /// rename-then-edit resolves the edit to the new path; edit-then-rename
    /// writes the edit to the old path and the rename carries the content across.
    #[tokio::test]
    async fn test_edit_survives_concurrent_rename_either_order() {
        let doc = "55555555-5555-5555-5555-555555555555";

        // Build a v0 snapshot + a v0→v1 delta from ONE oplog history so the delta
        // merges cleanly into the daemon's doc (seeded from the same snapshot).
        let mut base = kutl_core::Document::new();
        let agent = base.register_agent("editor").expect("agent");
        base.replace_content(
            agent,
            "did:editor",
            "seed",
            kutl_core::Boundary::Auto,
            "v0\n",
        )
        .unwrap();
        let (seed_ops, seed_meta) = encode_delta(&base, &[]);
        let v0 = base.local_version();
        base.replace_content(
            agent,
            "did:editor",
            "edit",
            kutl_core::Boundary::Auto,
            "v0\nv1\n",
        )
        .unwrap();
        let (edit_ops, edit_meta) = encode_delta(&base, &v0);

        for rename_first in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut w = test_worker(dir.path().to_path_buf());
            let (sup_tx, _sup_rx) = mpsc::channel::<Suppression>(16);
            let (cmd_tx, _cmd_rx) = mpsc::channel::<SyncCommand>(16);

            // Track + seed the doc at foo.md (v0).
            w.register_identity(SafeRelayPath::new("foo.md").unwrap(), doc.to_owned(), true);
            w.handle_remote_text(doc, seed_ops.clone(), seed_meta.clone(), &sup_tx, &cmd_tx)
                .await
                .expect("seed v0");
            assert_eq!(
                std::fs::read_to_string(dir.path().join("foo.md")).unwrap(),
                "v0\n",
                "seeded at foo.md (rename_first={rename_first})"
            );

            let foo = SafeRelayPath::new("foo.md").unwrap();
            let bar = SafeRelayPath::new("bar.md").unwrap();
            if rename_first {
                w.handle_remote_rename(doc, &foo, &bar, &sup_tx, &cmd_tx)
                    .await
                    .expect("rename");
                w.handle_remote_text(doc, edit_ops.clone(), edit_meta.clone(), &sup_tx, &cmd_tx)
                    .await
                    .expect("edit");
            } else {
                w.handle_remote_text(doc, edit_ops.clone(), edit_meta.clone(), &sup_tx, &cmd_tx)
                    .await
                    .expect("edit");
                w.handle_remote_rename(doc, &foo, &bar, &sup_tx, &cmd_tx)
                    .await
                    .expect("rename");
            }

            assert!(
                !dir.path().join("foo.md").exists(),
                "old path is gone (rename_first={rename_first})"
            );
            assert_eq!(
                std::fs::read_to_string(dir.path().join("bar.md")).unwrap(),
                "v0\nv1\n",
                "the edit survives at the renamed path (rename_first={rename_first})"
            );
        }
    }

    /// §7.1 (partial, deterministic): the `place_rename` conform-by-inode
    /// mechanism. When the authoritative rename target has no file on disk but
    /// the document's content file is relocated elsewhere (a concurrent local
    /// rename, identified by inode), CONFORM (move) it to the authoritative path
    /// — do NOT materialize a duplicate from the CRDT (the duplicate is what let
    /// a stale local-rename echo re-win the LWW under clock skew). And the guard
    /// that fixed the residual: a NO-OP rename (`new == old`) must not pull the
    /// relocated file back. (Full run1==run2 reproducibility under two skewed
    /// event loops remains e2e — see lifecycle-test-determinism note.)
    #[tokio::test]
    async fn test_conform_relocated_file_by_inode_not_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = test_worker(dir.path().to_path_buf());
        let (sup_tx, _sup_rx) = mpsc::channel::<Suppression>(16);
        let doc = "77777777-7777-7777-7777-777777777777";

        // GENUINE rename: the file is relocated at `bar_a.md`; the authoritative
        // target is `bar_b.md` (empty on disk) but its recorded inode points at
        // the relocated file. Must conform (move) it, not duplicate.
        std::fs::write(dir.path().join("bar_a.md"), b"skewed\n").unwrap();
        let Some(inode) = crate::inode::get_inode(&dir.path().join("bar_a.md")) else {
            eprintln!(
                "skipping test_conform_relocated_file_by_inode_not_duplicate: no inode support"
            );
            return;
        };
        w.file_identity.insert(
            PathBuf::from("bar_b.md"),
            FileIdentity {
                document_uuid: doc.to_owned(),
                inode: Some(inode),
            },
        );
        let abs_new = dir.path().join("bar_b.md");
        w.conform_or_materialize_at(
            doc,
            &PathBuf::from("seed.md"),
            &PathBuf::from("bar_b.md"),
            &abs_new,
            &sup_tx,
        )
        .await;
        assert_eq!(
            std::fs::read_to_string(&abs_new).unwrap(),
            "skewed\n",
            "the relocated file is conformed to the authoritative path"
        );
        assert!(
            !dir.path().join("bar_a.md").exists(),
            "conform MOVES the file (no duplicate left at the relocated path)"
        );

        // NO-OP rename (new == old): the relocated file must NOT be pulled back to
        // `new_rel` — that was the §7.1 residual (a stale rebroadcast of the
        // pre-rename path beating the local rename through the freshness gate).
        std::fs::write(dir.path().join("kept_elsewhere.md"), b"stay\n").unwrap();
        let Some(inode2) = crate::inode::get_inode(&dir.path().join("kept_elsewhere.md")) else {
            return;
        };
        w.file_identity.insert(
            PathBuf::from("same.md"),
            FileIdentity {
                document_uuid: doc.to_owned(),
                inode: Some(inode2),
            },
        );
        let abs_same = dir.path().join("same.md");
        w.conform_or_materialize_at(
            doc,
            &PathBuf::from("same.md"),
            &PathBuf::from("same.md"),
            &abs_same,
            &sup_tx,
        )
        .await;
        assert!(
            !abs_same.exists(),
            "a no-op rename must not pull the relocated file back to new_rel"
        );
        assert!(
            dir.path().join("kept_elsewhere.md").exists(),
            "the relocated file stays put under a no-op rename"
        );
    }
}
