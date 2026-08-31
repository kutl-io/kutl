//! `SpaceState`: the pure per-space sync state carved off `SpaceWorker`.
//!
//! Holds the lifecycle/sync fields the daemon mutates while reconciling files
//! with the relay (the former inline `SpaceWorker` field block), plus the
//! in-memory `DiskShadow` model
//! and the client-side `known_records` lattice the placement cascade projects.
//! Nothing here touches the filesystem, a channel, or a clock: the clock is a
//! plain [`HlcClock`] ticked with an injected wall reading, never `Hlc::now()`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use kutl_core::lattice::RegistryLattice;
use kutl_core::{Document, Hlc, HlcClock};
use uuid::Uuid;

use crate::SafeRelayPath;
use crate::blob_state::BlobStateMap;
use crate::core::DiskShadow;
use crate::state::DaemonState;

/// Tracks the UUID and inode assigned to a file path in the relay registry.
///
/// The inode enables rename detection on platforms (e.g. macOS `FSEvents`)
/// that don't emit paired rename events. When a "new" file appears whose
/// inode matches an existing identity, the daemon treats it as a rename
/// rather than a delete + create.
#[derive(Debug, Clone)]
pub struct FileIdentity {
    /// UUID assigned to this document in the relay registry.
    pub document_uuid: String,
    /// Inode at registration time (None on non-Unix or if stat failed).
    pub inode: Option<u64>,
}

/// The pure per-space sync state. Mutated only by the driver (`SpaceWorker`)
/// and, from Phase 3 Task 5 onward, by `DaemonCore::handle` /
/// `reconcile_placement`; never touches IO or reads a clock.
pub struct SpaceState {
    // ── migrated verbatim from SpaceWorker ──
    /// Space ID (mirrors `SpaceWorkerConfig::space_id`).
    pub space_id: String,
    /// Path to the space root directory, used only for path joins — no IO in
    /// the core (mirrors `SpaceWorkerConfig::space_root`).
    pub space_root: PathBuf,
    /// DID identity of this peer (mirrors `SpaceWorkerConfig::author_did`).
    pub author_did: String,
    /// Session-scoped CRDT agent name (short random alphanumeric string).
    ///
    /// Each daemon instance gets a unique agent name so that concurrent
    /// writers (even with the same DID) never collide on CRDT sequence
    /// numbers. The raw DID is preserved in `author_did` for metadata
    /// attribution. diamond-types limits agent names to 50 UTF-8 bytes.
    pub agent_name: String,
    /// Last synced version per document, for computing deltas.
    pub last_synced: HashMap<PathBuf, Vec<usize>>,
    /// Last-synced state for binary files (LWW).
    pub blob_state: BlobStateMap,
    /// Maps watched file paths to their (`document_uuid`, inode) for rename
    /// detection. When a file disappears and a new file appears with the same
    /// inode, the daemon recognizes it as a rename rather than a delete +
    /// create.
    pub file_identity: HashMap<PathBuf, FileIdentity>,
    /// Reverse mapping from UUID to relative path.
    pub uuid_to_path: HashMap<String, PathBuf>,
    /// Local daemon state cache persisted to `.kutl/state.json`.
    pub state: DaemonState,
    /// Origin hybrid-logical clock for lifecycle/edit stamps this daemon
    /// produces. A plain [`HlcClock`] (no `Mutex`): the core is single-threaded
    /// by construction, so the `Send`-across-await reason the old `Mutex`
    /// guarded is gone. The persisted floor is synced from the clock by
    /// `SpaceWorker::save_state`.
    pub hlc: HlcClock,
    /// Wall-clock skew in milliseconds applied to every HLC physical-time
    /// reading this daemon takes, from `KUTL_CLOCK_SKEW_MS` (default 0). A test
    /// seam for reproducibility under clock skew: two daemons given opposing
    /// skews disagree on physical time, so a passing convergence proves
    /// origin-HLC ordering does not depend on whose wall clock is ahead. Outside
    /// tests the env var is unset and this is 0 (no effect). Read once at driver
    /// init; the driver applies it when taking the real clock.
    pub clock_skew_ms: i64,
    /// Per-document HLC of the most recent lifecycle op (register/rename/
    /// unregister) this daemon has *applied* — whether produced locally or
    /// received from the relay. A remote lifecycle event is applied only when
    /// its HLC is causally newer than this; an older one is a stale echo of a
    /// superseded op and is dropped. This is what converges concurrent
    /// rename/rename and rename/delete: the loser of a race receives the
    /// winner's higher-HLC broadcast and applies it, while a local delete's HLC
    /// blocks a stale registration from resurrecting the file. In-memory and
    /// session-scoped; the startup reconcile is the cross-restart backstop.
    pub lifecycle_hlc: HashMap<String, Hlc>,
    /// Per-document HLC of the REGISTER this daemon recorded for the doc (local
    /// register, or the first remote register it observed). Distinct from
    /// `lifecycle_hlc` (which absorbs renames/deletes too): a rename's CAUSAL
    /// FLOOR must be the *register* it observed, not the watermark — folding a
    /// concurrently-observed peer rename into the floor would let the floor reach
    /// that peer's rename HLC and TIE it (lexicographic-path tiebreak then flips
    /// the §7.1 skew winner). Monotone max. PERSISTED via `state.json`
    /// (seeded at startup, synced on every `save_state`, pruned on unregister):
    /// an OFFLINE rename is re-emitted at the NEXT startup stamped at the
    /// pre-offline floor, so the recorded register is its only causal proof over
    /// the registration — without persistence the rejoin re-emit carries
    /// `floor: None` and the relay drops it as a stale pre-registration echo
    /// (the offline-ingest rejoin bug).
    pub register_hlc: HashMap<String, Hlc>,

    // ── the in-memory CRDT store (was DocumentManager.documents) ──
    /// Per-document `Document` instances, keyed by document id (a UUID). The
    /// pure content-merge state the core mutates: a remote-ops merge, a local
    /// edit, and a revival all read/write `Document`s here. The on-disk `.dt`
    /// sidecar (`.kutl/docs/<document-id>.dt`) is EDGE IO — the startup scan
    /// loads every sidecar into this map (`SpaceWorker::scan_docs`), and
    /// `Effect::SaveDoc` (via `SpaceWorker::save_doc`) persists mutations. The
    /// path ↔ id mapping lives in `file_identity` / `uuid_to_path`, not here.
    pub documents: HashMap<String, Document>,

    // ── renamed: was `SpaceWorker`'s `pending_placements` ──
    /// Documents the relay has placed at a path currently held by a *different*
    /// live document, deferred until that path frees (path-arbitration
    /// conflict-copy). The daemon never displaces an occupant on its own — the
    /// relay always issues the occupant's move (its own rename, a displacement
    /// broadcast, or a loser-correction), and the drain materializes the waiting
    /// document once its target is vacated. Maps document id → the effective
    /// path it is waiting to occupy. In-memory and session-scoped.
    ///
    /// Stores the validated [`SafeRelayPath`] (not a bare `PathBuf`) so a drain
    /// can place the document directly without re-parsing — the validity proof
    /// is carried, not discarded and re-derived.
    ///
    /// Renamed from the former `pending_placements` field: the placement
    /// cascade derives this each reconcile from `known_records` rather than
    /// maintaining a procedural worklist. [`reconcile_placement`] CLEARS and
    /// rebuilds it every pass, so a doc that left `known_records` (a tombstone)
    /// is no longer a mover and so cannot leave a stale entry behind — the
    /// resurrection bug is structural, not a guard to remember.
    ///
    /// [`reconcile_placement`]: crate::core::reconcile::reconcile_placement
    pub deferred: HashMap<String, SafeRelayPath>,

    // ── new (graft 2): per-pass revival exemption set ──
    /// Document ids exempt from path-collision deferral in the CURRENT reconcile
    /// pass because their entry event was a REVIVAL (`was_seen_before`, read from
    /// [`Self::has_applied_lifecycle`] BEFORE the HLC fold). A revival re-asserts a
    /// doc at its OWN path (its delete lost to a concurrent edit), so it must place
    /// onto its own orphan rather than defer behind it.
    ///
    /// Cleared at the start of every [`DaemonCore::handle`] call, mirroring the
    /// per-call scope of the former imperative `is_revival` capture — a doc is exempt only for
    /// the reconcile its OWN register triggers, never for an unrelated later pass.
    /// Never persisted (purely a within-pass signal).
    ///
    /// [`DaemonCore::handle`]: crate::core::DaemonCore::handle
    pub exempt_revival: HashSet<Uuid>,

    // ── new: the client-side registry the placement cascade projects ──
    /// The daemon's client-side view of the registry: every doc's intended
    /// path, `path_hlc`, and liveness as this daemon last applied them. The
    /// placement cascade recomputes the desired `id → effective_path` from this
    /// by the SAME algorithm the relay runs (`RegistryLattice::arbitrate`,
    /// `registry.rs:88`), so daemon and relay agree by construction. Fed by
    /// every register/rename/unregister/ack the core applies. Empty at init.
    pub known_records: RegistryLattice,

    // ── new: the in-memory disk model (graft 3 truth) ──
    /// In-memory mirror of on-disk identity, updated only on shell-ACK of a
    /// successful disk effect.
    pub shadow: DiskShadow,
}

impl SpaceState {
    /// Build a minimal, IO-free `SpaceState` for unit and simulation tests.
    ///
    /// Seeds an empty registry/shadow and a fresh [`HlcClock`] over a freshly
    /// generated device actor — the same device-actor/floor seed shape as
    /// `SpaceWorker::new`, simplified for tests (no
    /// persisted floor to restore, no cached documents to populate). `pub`
    /// because the `kutl-sim` `DaemonSim` driver (a separate crate) constructs
    /// `SpaceState` directly to exercise the liveness harness.
    pub fn new_for_test(space_id: String, space_root: PathBuf, author_did: String) -> Self {
        let actor = kutl_core::ActorId(uuid::Uuid::new_v4());
        Self {
            space_id,
            space_root,
            author_did,
            agent_name: String::new(),
            last_synced: HashMap::new(),
            blob_state: BlobStateMap::default(),
            file_identity: HashMap::new(),
            uuid_to_path: HashMap::new(),
            state: DaemonState::default(),
            hlc: HlcClock::new(actor),
            clock_skew_ms: 0,
            lifecycle_hlc: HashMap::new(),
            register_hlc: HashMap::new(),
            documents: HashMap::new(),
            deferred: HashMap::new(),
            exempt_revival: HashSet::new(),
            known_records: RegistryLattice::new(),
            shadow: DiskShadow::default(),
        }
    }

    /// Whether a lifecycle op has ever been applied for `document_id` (verbatim
    /// port of the former `SpaceWorker::has_applied_lifecycle`).
    ///
    /// A register for a known id is a REVIVAL (its delete lost to a concurrent
    /// edit; the relay re-asserts it at its OWN path), which is exempt from
    /// path-collision deferral. MUST be read BEFORE
    /// [`Self::record_lifecycle_hlc`] folds the current event's stamp, which would
    /// otherwise make every register look already-known.
    #[must_use]
    pub fn has_applied_lifecycle(&self, document_id: &str) -> bool {
        self.lifecycle_hlc.contains_key(document_id)
    }

    /// Advance the per-document lifecycle watermark to `hlc` (monotonic max).
    /// Verbatim `SpaceWorker::record_lifecycle_hlc` (daemon.rs).
    pub fn record_lifecycle_hlc(&mut self, document_id: &str, hlc: Hlc) {
        let slot = self
            .lifecycle_hlc
            .entry(document_id.to_owned())
            .or_insert(hlc);
        if hlc > *slot {
            *slot = hlc;
        }
    }

    /// Record the REGISTER HLC for a document (monotonic max) — the precise
    /// source for a later rename's causal floor (see [`Self::register_hlc`]).
    /// Called for a local register/mint and every remote register observed;
    /// a revival's higher register stamp advances the floor.
    pub fn record_register_hlc(&mut self, document_id: &str, hlc: Hlc) {
        let slot = self
            .register_hlc
            .entry(document_id.to_owned())
            .or_insert(hlc);
        if hlc > *slot {
            *slot = hlc;
        }
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
    ///
    /// Port of the former imperative `SpaceWorker::lifecycle_event_is_fresh`,
    /// reading the injected `wall_ms` rather than `skewed_now_ms_u64` and the
    /// plain `HlcClock` (no `Mutex`).
    pub fn lifecycle_event_is_fresh(
        &mut self,
        document_id: &str,
        incoming: Option<Hlc>,
        wall_ms: u64,
    ) -> bool {
        let Some(hlc) = incoming else {
            return true;
        };
        // Advance the origin clock past every observed lifecycle stamp — even a
        // stale one we drop — so a DELIBERATE later local op (e.g. the user
        // renaming the doc after this event lands) is causally after it and
        // wins the relay's HLC arbitration deterministically. A CONCURRENT
        // local rename must NOT get this lift: the driver drains it (via
        // `drain_relocated_local_rename`, §1.3) BEFORE dispatching the remote
        // event into this gate, so it keeps its honest pre-observation stamp
        // under peer clock skew.
        self.hlc.recv(hlc, wall_ms);
        match self.lifecycle_hlc.get(document_id) {
            Some(applied) if hlc <= *applied => false,
            _ => {
                self.record_lifecycle_hlc(document_id, hlc);
                true
            }
        }
    }

    /// The RENAME variant of [`Self::lifecycle_event_is_fresh`]: additionally
    /// accepts a rename whose own stamp is below the watermark when its
    /// `rename_causal_floor` (the register HLC the renamer observed) is at or
    /// above it — the gate-level twin of `DocRecord::path_priority`'s
    /// supersession rule (`renamed >= registered OR floor >= registered`).
    ///
    /// Without the floor arm, a THIRD observer silently drops the very rename
    /// the relay's lattice accepts: a remote REGISTER folds its (possibly
    /// clock-skewed, future) stamp into the observer's watermark, so a peer's
    /// honest rename of that doc — stamped below the skewed register but
    /// causally after it — gates out. The registrant itself never hits this
    /// (a local mint records no lifecycle watermark) and the renamer applied
    /// locally, so exactly the OTHER workers keep the old path: the
    /// combined-stressors cross-worker lost rename. A 2-worker guard cannot
    /// see this hole — it needs a third, pure-observer worker.
    ///
    /// A stale rename from before a REVIVAL still drops: the revival's newer
    /// register watermark exceeds the old floor. The clock `recv` and the
    /// watermark fold happen in the delegated call; the floor arm records
    /// nothing new — the watermark already sits at or above
    /// `max(renamed, floor)`, so a genuinely newer lifecycle op keeps
    /// dominating, and a re-delivered echo of this rename re-applies
    /// idempotently (the record fold is a max-merge).
    pub fn rename_event_is_fresh(
        &mut self,
        document_id: &str,
        incoming: Option<Hlc>,
        rename_causal_floor: Option<Hlc>,
        wall_ms: u64,
    ) -> bool {
        if self.lifecycle_event_is_fresh(document_id, incoming, wall_ms) {
            return true;
        }
        match (rename_causal_floor, self.lifecycle_hlc.get(document_id)) {
            (Some(floor), Some(applied)) => floor >= *applied,
            _ => false,
        }
    }

    /// Push the live in-memory views into the persisted [`crate::state::DaemonState`]
    /// ahead of a write: identity inodes (offline-rename recovery), the
    /// emitted-HLC floor, and the per-doc register stamps (a wholesale
    /// replace, so prunes carry too). The single pre-serialize sync, shared by
    /// the driver's `save_state` and the sim's modeled coalesced flush —
    /// split copies of this block would silently drift.
    pub fn sync_persisted(&mut self) {
        let inodes: Vec<(String, Option<u64>)> = self
            .file_identity
            .iter()
            .map(|(p, id)| (super::rel_path_to_string(p), id.inode))
            .collect();
        for (path, inode) in inodes {
            self.state.set_inode(&path, inode);
        }
        let last = self.hlc.last();
        self.state.record_emitted_hlc(last);
        self.state.register_hlc = self
            .register_hlc
            .iter()
            .map(|(id, hlc)| (id.clone(), crate::state::RegisterHlc::from(*hlc)))
            .collect();
    }

    /// The document's CURRENT on-disk location: prefer the shadow's live path,
    /// falling back to `uuid_to_path` for a doc never placed here (no shadow
    /// entry). For a DISPLACED doc the cascade moved the file to its conflict
    /// path via `GuardedPlace(Rename)` and `shadow_path` tracks that, while
    /// `uuid_to_path` still records the original path (the pure cascade is a
    /// projection — it never `move_identity`s on a displacement). Reading
    /// `uuid_to_path` directly for a DISK access therefore reads (or writes!)
    /// the WINNER's file at the contested path. Every
    /// consumer that means "where are this doc's bytes on disk" goes through
    /// here; `uuid_to_path` remains the IDENTITY map (registered path) for
    /// tracking predicates and identity moves.
    #[must_use]
    pub fn doc_disk_path(&self, document_id: &str) -> Option<PathBuf> {
        Uuid::parse_str(document_id)
            .ok()
            .and_then(|id| self.shadow.shadow_path.get(&id).cloned())
            .or_else(|| self.uuid_to_path.get(document_id).cloned())
    }

    /// The `.dt` sidecar path for a document id: `.kutl/docs/<document-id>.dt`.
    /// Flat (a document id is a UUID — no path separators), so no nesting.
    /// Pure: a `space_root` join, no IO. The driver writes/reads at this path.
    #[must_use]
    pub fn dt_path(&self, document_id: &str) -> PathBuf {
        self.space_root
            .join(".kutl")
            .join("docs")
            .join(format!("{document_id}.dt"))
    }

    /// The absolute on-disk path for a relative space path (the user's file).
    /// Unrelated to sidecar storage — purely `space_root` + `rel_path`. Pure.
    #[must_use]
    pub fn file_path(&self, rel_path: &Path) -> PathBuf {
        self.space_root.join(rel_path)
    }

    /// Get a reference to a loaded CRDT document by id (in-memory only).
    #[must_use]
    pub fn get_doc(&self, document_id: &str) -> Option<&Document> {
        self.documents.get(document_id)
    }

    /// Get-or-create the in-memory CRDT document for `document_id`, returning a
    /// mutable reference.
    ///
    /// IN-MEMORY ONLY: never loads from disk. The startup scan
    /// (`SpaceWorker::scan_docs`) populates `documents` with every existing
    /// sidecar before the event loop runs, so a miss here is genuinely a new
    /// document (matching the old `DocumentManager::load_or_create`, whose disk
    /// load only ever fired for ids the startup scan had not already cached).
    pub fn load_or_create_doc(&mut self, document_id: &str) -> &mut Document {
        self.documents.entry(document_id.to_owned()).or_default()
    }

    /// Drop the in-memory CRDT document for `document_id`. The `.dt` sidecar is
    /// deleted separately at the edge (`SpaceWorker::remove_doc_sidecar`).
    pub fn remove_doc_in_memory(&mut self, document_id: &str) {
        self.documents.remove(document_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn empty_state() -> SpaceState {
        SpaceState::new_for_test("space-1".into(), PathBuf::from("/tmp/x"), "did:test".into())
    }

    #[test]
    fn test_clock_ticks_without_a_mutex() {
        // The core is single-threaded, so the clock is a plain HlcClock (no Mutex):
        // tick(wall) takes &mut self and returns a monotonic stamp (the same
        // tick `SpaceWorker::make_metadata` performs).
        let mut s = empty_state();
        let a = s.hlc.tick(100);
        let b = s.hlc.tick(100);
        assert!(b > a, "stamps must be monotonic at equal wall time");
    }

    #[test]
    fn test_deferred_is_the_renamed_pending_placements() {
        // `deferred` replaces the former `pending_placements`; empty at init.
        let s = empty_state();
        assert!(s.deferred.is_empty());
        assert!(s.known_records.records().next().is_none());
    }

    /// Migrated from `documents.rs::test_dt_path_mapping`: the sidecar path is a
    /// pure `space_root` join keyed by id, flat under `.kutl/docs/`.
    #[test]
    fn test_dt_path_mapping() {
        const DOC_A: &str = "11111111-1111-4111-8111-111111111111";
        let s = SpaceState::new_for_test(
            "space-1".into(),
            PathBuf::from("/tmp/space"),
            "did:x".into(),
        );
        assert_eq!(
            s.dt_path(DOC_A),
            PathBuf::from(format!("/tmp/space/.kutl/docs/{DOC_A}.dt"))
        );
    }

    /// Migrated from `documents.rs::test_load_or_create_new`: a miss yields a
    /// fresh empty document, in memory only (no disk load).
    #[test]
    fn test_load_or_create_doc_new() {
        const DOC_A: &str = "11111111-1111-4111-8111-111111111111";
        let mut s = empty_state();
        let doc = s.load_or_create_doc(DOC_A);
        assert!(doc.is_empty());
        assert!(s.get_doc(DOC_A).is_some());
    }

    /// `remove_doc_in_memory` drops only the in-memory entry (the `.dt` is an
    /// edge concern).
    #[test]
    fn test_remove_doc_in_memory() {
        const DOC_A: &str = "11111111-1111-4111-8111-111111111111";
        let mut s = empty_state();
        s.load_or_create_doc(DOC_A);
        assert!(s.get_doc(DOC_A).is_some());
        s.remove_doc_in_memory(DOC_A);
        assert!(s.get_doc(DOC_A).is_none());
    }
}
