//! `SpaceState`: the pure per-space sync state carved off `SpaceWorker`.
//!
//! Holds the lifecycle/sync fields the daemon mutates while reconciling files
//! with the relay (the former inline `SpaceWorker` field block), plus the
//! in-memory `DiskShadow` model
//! and the client-side `known_records` lattice the placement cascade projects.
//! Nothing here touches the filesystem, a channel, or a clock: the clock is a
//! plain [`HlcClock`] ticked with an injected wall reading, never `Hlc::now()`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use kutl_core::lattice::RegistryLattice;
use kutl_core::{Document, Hlc, HlcClock};
use uuid::Uuid;

use crate::SafeRelayPath;
use crate::blob_state::{BlobState, BlobStateMap};
use crate::core::{DiskShadow, casefold, rel_path_to_string};
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
    /// Hex SHA-256 of the last disk content this daemon KNEW — written by its
    /// own funnel or observed from a local modification it incorporated —
    /// the startup scan's evidence for telling an interrupted
    /// materialization from a genuine offline edit after a crash: disk
    /// matching this hash is unchanged since the daemon last saw it, so a
    /// CRDT-ahead delta is unmaterialized daemon-side content to restore,
    /// never a user edit to incorporate. Observations count, not just
    /// writes: a hash frozen at the last funnel write would still match a
    /// user's offline REVERT to that content, and the restore would undo
    /// the revert. `None` until the first write/observation (or on legacy
    /// state), which degrades to the incorporate-as-edit behavior; also
    /// cleared when a sidecar save fails, because a stale sidecar reloaded
    /// after a restart makes "restore from the CRDT" an erasure.
    pub last_written_hash: Option<String>,
}

/// Derived indexes over `SpaceState::file_identity`: O(log N)/O(1) forms of
/// the three per-event queries (tracked children of a directory, the holder
/// of an inode, the case variant of a path) that would otherwise be
/// O(tracked) scans, which a bulk burst of N events turns into O(N²). Kept
/// consistent by the `identity_*` choke-point methods; every index is a pure
/// function of `file_identity`, so [`SpaceState::rebuild_identity_indexes`]
/// can always re-derive it.
#[derive(Debug, Default, Clone)]
pub struct IdentityIndexes {
    /// Ordered tracked paths: "any tracked doc under `dir/`?" is one
    /// `range((Excluded(dir), Unbounded)).next()` probe + a `starts_with`
    /// (`PathBuf`'s component-wise order keeps a directory's descendants
    /// contiguous immediately after it).
    pub tracked: BTreeSet<PathBuf>,
    /// Recorded inode → every tracked path recording it, for rename-source
    /// detection. Live inodes are unique, but a RECORDED inode can be shared
    /// transiently: the kernel recycles a freed number, and an editor's
    /// atomic-save dance hands a tracked path a fresh inode before its old
    /// one is refreshed away. Holding every claimant keeps the probe as
    /// exhaustive as a scan of `file_identity`; the caller's on-disk filter
    /// picks the vacated one, newest claim first (claims are kept in claim
    /// order, so the freshest identity wins a tie). A bulk rebuild has no
    /// claim history to restore and lays the holders out in path order, so
    /// two processes rebuilt from the same map break a tie the same way.
    pub by_inode: HashMap<u64, Vec<PathBuf>>,
    /// Casefolded path string → every tracked path folding to it, for
    /// case-collision checks (keyed by the crate-wide `casefold`, the shared
    /// arbitration case rule). The relay refuses case-variant duplicates, so
    /// a fold normally has one holder; a transient second (a rename passing
    /// through its own case variant, a corrupt persisted map) must not evict
    /// the first.
    pub by_casefold: HashMap<String, Vec<PathBuf>>,
}

/// Add `rel` to the holders under `key`, newest last; a path already
/// holding the key is not added twice.
fn claim<K: Eq + std::hash::Hash>(index: &mut HashMap<K, Vec<PathBuf>>, key: K, rel: &Path) {
    let holders = index.entry(key).or_default();
    if !holders.iter().any(|p| p == rel) {
        holders.push(rel.to_path_buf());
    }
}

/// Drop `rel` from the holders under `key`, dropping the key once no holder
/// remains. Removing one claimant never touches another's claim.
fn release<K: Eq + std::hash::Hash>(index: &mut HashMap<K, Vec<PathBuf>>, key: &K, rel: &Path) {
    if let Some(holders) = index.get_mut(key) {
        holders.retain(|p| p != rel);
        if holders.is_empty() {
            index.remove(key);
        }
    }
}

impl IdentityIndexes {
    /// Record `rel` as a holder of `inode` (a `None` inode records nothing).
    fn claim_inode(&mut self, inode: Option<u64>, rel: &Path) {
        if let Some(ino) = inode {
            claim(&mut self.by_inode, ino, rel);
        }
    }

    /// Drop `rel`'s claim on `inode`, leaving every other claimant intact.
    fn release_inode(&mut self, inode: Option<u64>, rel: &Path) {
        if let Some(ino) = inode {
            release(&mut self.by_inode, &ino, rel);
        }
    }
}

/// The pure per-space sync state. Mutated only by the driver (`SpaceWorker`),
/// `DaemonCore::handle` and `reconcile_placement`; never touches IO or reads a
/// clock.
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
    /// Blob LWW records accepted from the relay whose `WriteFile` has not
    /// landed yet, keyed by the path the write targets. A record moves into
    /// `blob_state` only from the `FileWritten` fold, so a refused write (a
    /// read-only file, a full disk) leaves the committed record on the
    /// previous bytes and the relay's next redelivery of the same bytes
    /// passes the LWW gate and retries; committed at accept time, that
    /// redelivery would be gated out as redundant and the stale file kept
    /// forever. A later accept for the same path overwrites the entry; a
    /// path's teardown or move drops it. Never persisted.
    pub pending_blob_state: HashMap<PathBuf, BlobState>,
    /// Maps watched file paths to their (`document_uuid`, inode) for rename
    /// detection. When a file disappears and a new file appears with the same
    /// inode, the daemon recognizes it as a rename rather than a delete +
    /// create.
    pub file_identity: HashMap<PathBuf, FileIdentity>,
    /// Derived query indexes over [`Self::file_identity`] (see
    /// [`IdentityIndexes`]). Mutate `file_identity` ONLY through
    /// [`Self::identity_insert`] / [`Self::identity_remove`] /
    /// [`Self::identity_set_inode`], or call
    /// [`Self::rebuild_identity_indexes`] after a bulk build, so these stay
    /// consistent; the reconcile pass's debug-build tripwire fails loudly on
    /// drift.
    pub identity_idx: IdentityIndexes,
    /// Reverse mapping from UUID to relative path.
    pub uuid_to_path: HashMap<String, PathBuf>,
    /// Local daemon state cache persisted to `.kutl/state.ksnap`.
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
    /// which side wins under clock skew, so two skewed daemons stop converging
    /// identically). Monotone max. PERSISTED via the state snapshot
    /// (seeded at startup, synced on every `save_state`, pruned on unregister):
    /// an OFFLINE rename is re-emitted at the NEXT startup stamped at the
    /// pre-offline floor, so the recorded register is its only causal proof over
    /// the registration — without persistence the rejoin re-emit carries
    /// `floor: None` and the relay drops it as a stale pre-registration echo
    /// (the offline-ingest rejoin bug).
    pub register_hlc: HashMap<String, Hlc>,

    // ── the in-memory CRDT store ──
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

    // ── intake-burst signal (set by the driver each loop iteration) ──
    /// True while the driver's intake channels still hold undispatched events.
    /// [`reconcile_unless_backlogged`] returns empty while this is set, so a
    /// bulk burst runs ONE placement recompute when the intake drains instead
    /// of one O(docs) recompute per event — the same coalescing shape as the
    /// driver's `SaveState` flush. The `MetricsTick` reconcile ignores this flag
    /// (the tick is the guaranteed cascade driver), and the sim/tests that
    /// construct state directly default it to `false` = per-event semantics
    /// unchanged.
    ///
    /// [`reconcile_unless_backlogged`]: crate::core::reconcile::reconcile_unless_backlogged
    pub intake_backlogged: bool,

    /// A gated reconcile was SKIPPED and no pass has run since: the driver's
    /// drained-intake probe (`reconcile_if_caught_up`) owes the cascade one
    /// placement pass. Armed by [`reconcile_unless_backlogged`]'s skip branch,
    /// cleared by every [`reconcile_placement`] run. Without it, a burst whose
    /// trailing loop inputs carry no reconcile tail (suppression echoes,
    /// imperative acks) strands its deferred placements until the metrics
    /// tick — measured as the final ~2% of a bulk add waiting ~7s.
    ///
    /// [`reconcile_unless_backlogged`]: crate::core::reconcile::reconcile_unless_backlogged
    /// [`reconcile_placement`]: crate::core::reconcile::reconcile_placement
    pub placement_dirty: bool,
    /// The last placement pass stopped at its emission cap with movers still
    /// unemitted. Set and cleared by every [`reconcile_placement`] run beside
    /// `placement_dirty`. The driver reads it as burst-class debt: the
    /// remainder of a capped pass is bulk by definition, so it bypasses the
    /// trickle pacing floor the way a genuinely backlogged intake does.
    ///
    /// [`reconcile_placement`]: crate::core::reconcile::reconcile_placement
    pub placement_truncated: bool,

    /// Paths whose persisted identity view changed since the last identity-
    /// journal drain: registered, moved, unregistered, inode-refreshed, or
    /// relay-confirmed. The pure core only RECORDS the paths (no IO here);
    /// the driver's `drain_identity_journal` snapshots each one's current
    /// entry into `.kutl/identity.klog` so a SIGKILL between coalesced
    /// snapshot saves cannot forget an identity the daemon already acted
    /// on (restart would re-register its own files and mint conflict
    /// copies). Cleared by the drain and by every full save.
    pub journal_pending: HashSet<PathBuf>,
    /// Documents whose engine was retired because their `.dt` sidecar could
    /// not be read (the shell's one action for that, at the startup scan and
    /// the runtime reload alike). The relay's catch-up refills a fresh
    /// engine, and the file on disk is diffed against that content only
    /// AFTER the refill, so an edit made while the sidecar was unreadable
    /// folds in as the edit it is and the file is never read as a brand-new
    /// document. Cleared by the merge that refills.
    pub awaiting_content: HashSet<String>,

    // ── revival exemptions ──
    /// Document ids exempt from path-collision deferral because their entry
    /// event was a REVIVAL (`was_seen_before`, read from
    /// [`Self::has_applied_lifecycle`] BEFORE the HLC fold). A revival re-asserts a
    /// doc at its OWN path (its delete lost to a concurrent edit), so it must place
    /// onto its own orphan rather than defer behind it. Never persisted.
    ///
    /// Revival exemptions: document id → the wire path its register asked
    /// for. A previously-seen doc re-registered at its OWN path may place
    /// onto its own untracked orphan there, exempt from foreign-occupant
    /// deferral. Keyed by the path so the exemption never applies to a
    /// different path the doc may resolve to later; spent by the first
    /// ungated placement pass, whether or not that pass had a place to emit
    /// for the doc, unless the emission cap left the doc unemitted. It
    /// survives a gated pass (no pass ran) and is pruned with the document.
    pub exempt_revival: HashMap<Uuid, String>,

    // ── new: the client-side registry the placement cascade projects ──
    /// The daemon's client-side view of the registry: every doc's intended
    /// path, `path_hlc`, and liveness as this daemon last applied them. The
    /// placement cascade recomputes the desired `id → effective_path` from this
    /// by the SAME algorithm the relay runs (`RegistryLattice::arbitrate`),
    /// so daemon and relay agree by construction. Fed by
    /// every register/rename/unregister/ack the core applies. Empty at init.
    pub known_records: RegistryLattice,

    // ── the in-memory disk model ──
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
            pending_blob_state: HashMap::new(),
            file_identity: HashMap::new(),
            uuid_to_path: HashMap::new(),
            state: DaemonState::default(),
            hlc: HlcClock::new(actor),
            clock_skew_ms: 0,
            lifecycle_hlc: HashMap::new(),
            register_hlc: HashMap::new(),
            documents: HashMap::new(),
            deferred: HashMap::new(),
            intake_backlogged: false,
            placement_dirty: false,
            placement_truncated: false,
            journal_pending: HashSet::new(),
            awaiting_content: HashSet::new(),
            exempt_revival: HashMap::new(),
            known_records: RegistryLattice::new(),
            shadow: DiskShadow::default(),
            identity_idx: IdentityIndexes::default(),
        }
    }

    /// Insert (or overwrite) a tracked identity, keeping the derived indexes
    /// in step. The one write path for new/rebinding identities.
    pub fn identity_insert(&mut self, rel: PathBuf, id: FileIdentity) {
        let old_inode = self.file_identity.get(&rel).and_then(|old| old.inode);
        self.identity_idx.release_inode(old_inode, &rel);
        self.identity_idx.claim_inode(id.inode, &rel);
        claim(&mut self.identity_idx.by_casefold, casefold(&rel), &rel);
        self.identity_idx.tracked.insert(rel.clone());
        self.journal_pending.insert(rel.clone());
        self.file_identity.insert(rel, id);
    }

    /// Remove a tracked identity if present, keeping the indexes in step.
    /// Only `rel`'s own claims come out; another path sharing an inode or a
    /// casefold keeps its entry. An untracked `rel` is a no-op: nothing
    /// changes, so nothing is journaled.
    pub fn identity_remove(&mut self, rel: &Path) -> Option<FileIdentity> {
        let removed = self.file_identity.remove(rel)?;
        self.journal_pending.insert(rel.to_path_buf());
        self.identity_idx.tracked.remove(rel);
        release(&mut self.identity_idx.by_casefold, &casefold(rel), rel);
        self.identity_idx.release_inode(removed.inode, rel);
        Some(removed)
    }

    /// Update the recorded inode for `rel` in place (the shell's ACK-time and
    /// tracked-edit refresh), keeping `by_inode` in step. No-op when `rel` is
    /// untracked.
    ///
    /// The refresh itself is load-bearing: editors rewrite files via the atomic
    /// tmp-rename dance, which gives the file a fresh inode on every save. A
    /// stale recorded inode causes two distinct failures — a genuine later
    /// rename of the file is *missed* (its current inode no longer matches, see
    /// [`Self::rename_source`]), and the stale value is a now-freed inode that
    /// a newly created file may be assigned, inviting a false rename match.
    ///
    /// A refresh that changes nothing is not a mutation: it journals no line
    /// (a journal line per unchanged ACK would grow the journal, and the
    /// force-save budget behind it, for no durability).
    pub fn identity_set_inode(&mut self, rel: &Path, inode: Option<u64>) {
        let Some(entry) = self.file_identity.get_mut(rel) else {
            return;
        };
        if entry.inode == inode {
            return;
        }
        self.journal_pending.insert(rel.to_path_buf());
        let old_inode = entry.inode;
        entry.inode = inode;
        self.identity_idx.release_inode(old_inode, rel);
        self.identity_idx.claim_inode(inode, rel);
    }

    /// Record the hex SHA-256 of `rel`'s current disk content — set by the
    /// write funnel after a materialization AND by the local-modification
    /// handler when it incorporates observed bytes (see
    /// [`FileIdentity::last_written_hash`] for why observations must count).
    /// Journaled like every identity change; an unchanged hash journals
    /// nothing. No-op when `rel` is untracked.
    pub fn identity_set_written_hash(&mut self, rel: &Path, hash: String) {
        let Some(entry) = self.file_identity.get_mut(rel) else {
            return;
        };
        if entry.last_written_hash.as_deref() == Some(hash.as_str()) {
            return;
        }
        entry.last_written_hash = Some(hash);
        self.journal_pending.insert(rel.to_path_buf());
    }

    /// Snapshot one pending path as its identity-journal line: the entry now
    /// at `rel` (or `None` recording its removal), the document's register
    /// stamp, and the live clock floor — all read from CURRENT in-memory
    /// state, so replay is a last-line-wins fold, idempotent under
    /// re-append. The ONE implementation behind the driver's journal drain
    /// and the sim's modeled drain, so the two persist contracts cannot
    /// drift (the same single-implementation rule as
    /// [`Self::sync_persisted`]).
    #[must_use]
    pub fn journal_line_for(&self, rel: &Path) -> crate::state::IdentityJournalLine {
        use crate::state::JournalLineKind;
        let path = rel_path_to_string(rel);
        let hlc_floor = Some(crate::state::HlcFloor::from(self.hlc.last()));
        let kind = match self.file_identity.get(rel) {
            Some(fi) => {
                let entry = crate::state::DocEntry {
                    id: fi.document_uuid.clone(),
                    confirmed: self.state.documents.get(&path).is_some_and(|e| e.confirmed),
                    inode: fi.inode,
                    last_written_hash: fi.last_written_hash.clone(),
                };
                let register_hlc = self
                    .register_hlc
                    .get(&entry.id)
                    .map(|hlc| crate::state::RegisterHlc::from(*hlc));
                JournalLineKind::Snapshot {
                    entry,
                    register_hlc,
                }
            }
            None => JournalLineKind::Removal,
        };
        crate::state::IdentityJournalLine {
            path,
            kind,
            hlc_floor,
        }
    }

    /// Every pending path's snapshot line, in the order the journal must
    /// hold them: present-entry lines first, removals last, each group
    /// sorted by path. Replay stops at the first torn line, so every prefix
    /// of a batch must leave state no worse than its base: a torn tail that
    /// kept an insert but lost a removal leaves a stale extra entry the
    /// offline-rename pre-pass heals, while the reverse order could replay a
    /// move as remove-without-insert, the document forgotten entirely and
    /// re-registered into a conflict copy on restart. The path order makes a
    /// batch reproducible. The ONE batch builder behind the driver's journal
    /// drain and the sim's modeled drain, so the two persist contracts
    /// cannot drift; the caller clears `journal_pending` once the batch is
    /// safely appended.
    #[must_use]
    pub fn journal_batch(&self) -> Vec<crate::state::IdentityJournalLine> {
        let mut lines: Vec<_> = self
            .journal_pending
            .iter()
            .map(|rel| self.journal_line_for(rel))
            .collect();
        lines.sort_by(|a, b| {
            a.is_removal()
                .cmp(&b.is_removal())
                .then_with(|| a.path.cmp(&b.path))
        });
        lines
    }

    /// Whether `content`, the bytes now on disk at `rel`, is exactly what this
    /// daemon last wrote or observed there while the LOADED document is not at
    /// that content: known bytes with a CRDT behind them. Such bytes are never
    /// a local edit to incorporate, whichever door is looking (startup scan,
    /// remote merge, watcher event): the relay's ops are what brings the CRDT
    /// up, and diffing the bytes in would re-insert what those ops carry.
    /// With no document loaded nothing is known to be behind, and a document
    /// whose sidecar is gone must still be able to take its file back, so the
    /// ordinary path decides. Hashes the content only after the cheap checks.
    #[must_use]
    pub fn known_bytes_ahead_of_crdt(&self, rel: &Path, content: &str) -> bool {
        let Some(identity) = self.file_identity.get(rel) else {
            return false;
        };
        let Some(known) = identity.last_written_hash.as_deref() else {
            return false;
        };
        let Some(doc) = self.get_doc(&identity.document_uuid) else {
            return false;
        };
        if doc.content_eq(content) {
            return false;
        }
        known == crate::blob_state::sha256_hex(content.as_bytes())
    }

    /// Drop `rel`'s recorded disk hash, degrading the startup guard to the
    /// safe incorporate-as-edit behavior for this doc. Called when a sidecar
    /// save fails: a restart then loads a CRDT BEHIND the hash-matched disk,
    /// and the guard's restore branch would overwrite disk with the stale
    /// sidecar — erasing the un-persisted ops' content. Duplication beats
    /// deletion. Journaled like every identity change; no-op when untracked
    /// or already clear.
    pub fn identity_clear_written_hash(&mut self, rel: &Path) {
        let Some(entry) = self.file_identity.get_mut(rel) else {
            return;
        };
        if entry.last_written_hash.take().is_some() {
            self.journal_pending.insert(rel.to_path_buf());
        }
    }

    /// Case-variant probe over the tracked set: a tracked path whose
    /// casefolded form equals `rel`'s, other than `rel` itself (an ordinary
    /// re-tracking, never a collision) and the `exclude`d path (a rename's
    /// own source). Every holder of the fold is considered, so a transient
    /// second holder cannot hide behind the first.
    pub fn tracked_case_variant(&self, rel: &Path, exclude: Option<&Path>) -> Option<&PathBuf> {
        self.identity_idx
            .by_casefold
            .get(&casefold(rel))
            .into_iter()
            .flatten()
            .rev()
            .find(|p| p.as_path() != rel && exclude.is_none_or(|ex| p.as_path() != ex))
    }

    /// Find the tracked document a newly-observed file was renamed FROM, by
    /// its live inode: a probe of [`IdentityIndexes::by_inode`] over every
    /// path that recorded the inode. Returns the candidate's old path and
    /// document UUID.
    ///
    /// A matching inode alone is **not** sufficient evidence of a rename: the
    /// kernel reuses inode numbers after a file is unlinked, so a freshly
    /// created file can be handed a freed inode that a still-tracked document
    /// recorded earlier. The match is only a rename when the candidate old
    /// path no longer exists on disk: a genuine rename moves the file away
    /// from its old path, whereas inode reuse leaves the old file untouched
    /// in place. With several recorded holders the newest vacated one is
    /// the source; holders still present on disk are stale records.
    ///
    /// `old_path_exists` reports whether a candidate old path is still present
    /// on disk; it is injected so the decision can be unit-tested without
    /// provoking a real kernel inode reuse.
    pub fn rename_source(
        &self,
        new_inode: u64,
        old_path_exists: impl Fn(&Path) -> bool,
    ) -> Option<(PathBuf, String)> {
        self.identity_idx
            .by_inode
            .get(&new_inode)
            .into_iter()
            .flatten()
            .rev()
            .find(|old| !old_path_exists(old.as_path()))
            .and_then(|old| {
                self.file_identity
                    .get(old.as_path())
                    .map(|id| (old.clone(), id.document_uuid.clone()))
            })
    }

    /// Rebuild every derived index from `file_identity` — for the bulk
    /// builders (startup load, sim restart, tests that construct the map
    /// directly) where per-entry maintenance would be noise.
    ///
    /// Entries are laid down in path order, not `HashMap` iteration order:
    /// the holder vectors in `by_inode` and `by_casefold` break ties by
    /// position, and a persisted map that records one inode against two
    /// paths must resolve the same way on every restart.
    pub fn rebuild_identity_indexes(&mut self) {
        let mut entries: Vec<(&PathBuf, &FileIdentity)> = self.file_identity.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let idx = &mut self.identity_idx;
        *idx = IdentityIndexes::default();
        for (rel, id) in entries {
            idx.tracked.insert(rel.clone());
            claim(&mut idx.by_casefold, casefold(rel), rel);
            idx.claim_inode(id.inode, rel);
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
    ///
    /// An advance is identity-bearing state: the doc's journal line snapshots
    /// this stamp, so the advance re-pends the line — otherwise a stamp-only
    /// refresh (a revival with no identity mutation) survives no crash and a
    /// post-restart rename computes its floor from the stale stamp. Skipped
    /// when the doc has no tracked path yet: the journal is keyed by path, and
    /// the register choke point pends the line once the placement lands.
    pub fn record_register_hlc(&mut self, document_id: &str, hlc: Hlc) {
        let advanced = if let Some(slot) = self.register_hlc.get_mut(document_id) {
            if hlc > *slot {
                *slot = hlc;
                true
            } else {
                false
            }
        } else {
            self.register_hlc.insert(document_id.to_owned(), hlc);
            true
        };
        if advanced && let Some(rel) = self.uuid_to_path.get(document_id) {
            self.journal_pending.insert(rel.clone());
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
    /// Reads the injected `wall_ms`, never a live clock.
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
        // `drain_relocated_local_rename`) BEFORE dispatching the remote event
        // into this gate, so it keeps its honest pre-observation stamp under
        // peer clock skew.
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
        for (p, id) in &self.file_identity {
            let path = super::rel_path_to_string(p);
            self.state.set_inode(&path, id.inode);
            self.state
                .set_written_hash(&path, id.last_written_hash.clone());
        }
        let last = self.hlc.last();
        self.state.record_emitted_hlc(last);
        self.state.register_hlc = self
            .register_hlc
            .iter()
            .map(|(id, hlc)| (id.clone(), crate::state::RegisterHlc::from(*hlc)))
            .collect();
    }

    /// The latest content this daemon holds for `document_id`, as the
    /// physical touch the relay's edit-revives rule stamps an edit with
    /// (`Hlc::physical_touch` of the change's timestamp): the stamp a delete
    /// must dominate to remove the document, read from this daemon's own
    /// CRDT so it covers exactly the edits this daemon has observed. `None`
    /// for an unloaded document or one with no recorded change.
    #[must_use]
    pub fn content_touch(&self, document_id: &str) -> Option<Hlc> {
        self.get_doc(document_id)?
            .changes()
            .iter()
            .map(|c| c.timestamp)
            .max()
            .and_then(|ts| u64::try_from(ts).ok())
            .map(Hlc::physical_touch)
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
    /// document and a disk load could only ever re-read a sidecar already
    /// cached.
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

    /// The sidecar path is a pure `space_root` join keyed by id, flat under
    /// `.kutl/docs/`.
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

    /// A miss yields a fresh empty document, in memory only (no disk load).
    #[test]
    fn test_load_or_create_doc_new() {
        const DOC_A: &str = "11111111-1111-4111-8111-111111111111";
        let mut s = empty_state();
        let doc = s.load_or_create_doc(DOC_A);
        assert!(doc.is_empty());
        assert!(s.get_doc(DOC_A).is_some());
    }

    /// A register-stamp advance is identity-bearing: it must pend the doc's
    /// journal line (the line snapshots the stamp), or a stamp-only refresh —
    /// a revival with no identity mutation — survives no crash and a
    /// post-restart rename computes its causal floor from the stale stamp.
    /// A non-advance pends nothing; an untracked doc pends nothing (the
    /// journal is keyed by path).
    #[test]
    fn test_record_register_hlc_advance_pends_journal() {
        const DOC: &str = "11111111-1111-4111-8111-111111111111";
        let mut s = empty_state();
        let older = s.hlc.tick(100);
        let newer = s.hlc.tick(200);

        // Untracked doc: an advance has no path to pend a line for.
        s.record_register_hlc(DOC, older);
        assert!(s.journal_pending.is_empty());

        s.uuid_to_path.insert(DOC.to_owned(), PathBuf::from("a.md"));

        // Re-observing the same stamp is not an advance.
        s.record_register_hlc(DOC, older);
        assert!(s.journal_pending.is_empty());

        // A genuine advance (the revival case) pends the doc's line.
        s.record_register_hlc(DOC, newer);
        assert!(s.journal_pending.contains(Path::new("a.md")));
        assert_eq!(s.register_hlc.get(DOC), Some(&newer));

        // A stale stamp neither regresses the slot nor pends.
        s.journal_pending.clear();
        s.record_register_hlc(DOC, older);
        assert!(s.journal_pending.is_empty());
        assert_eq!(s.register_hlc.get(DOC), Some(&newer));
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

    /// A bulk rebuild lays the holders of a shared inode out in path order,
    /// whatever order the `HashMap` yields them in, so the newest-first probe
    /// in `rename_source` resolves a tie the same way on every restart.
    #[test]
    fn test_rebuild_identity_indexes_orders_shared_inode_holders_by_path() {
        /// A recorded inode two paths share (a recycled number).
        const SHARED_INODE: u64 = 42;
        let paths = ["d/z.md", "a.md", "m/n.md", "b.md"];
        let mut s = empty_state();
        for (i, p) in paths.iter().enumerate() {
            s.file_identity.insert(
                PathBuf::from(p),
                FileIdentity {
                    document_uuid: format!("uuid-{i}"),
                    inode: Some(SHARED_INODE),
                    last_written_hash: None,
                },
            );
        }
        s.rebuild_identity_indexes();
        let mut expected: Vec<PathBuf> = paths.iter().map(PathBuf::from).collect();
        expected.sort();
        assert_eq!(
            s.identity_idx.by_inode.get(&SHARED_INODE),
            Some(&expected),
            "holders are laid out in path order after a rebuild"
        );
        // The last holder in path order is what a tie resolves to.
        assert_eq!(
            s.rename_source(SHARED_INODE, |_| false).map(|(rel, _)| rel),
            expected.last().cloned()
        );
    }

    /// The recorded disk hash survives persistence: `sync_persisted` carries
    /// it into the snapshot entry, which is the only way a restart can tell
    /// its own interrupted write from a user edit.
    #[test]
    fn test_sync_persisted_carries_the_written_hash() {
        let mut s =
            SpaceState::new_for_test("space-1".into(), PathBuf::from("/tmp/x"), "did:test".into());
        let rel = PathBuf::from("a.md");
        s.identity_insert(
            rel.clone(),
            FileIdentity {
                document_uuid: "uuid-a".into(),
                inode: Some(7),
                last_written_hash: None,
            },
        );
        s.state.set("a.md".into(), "uuid-a".into(), true);
        s.identity_set_written_hash(&rel, "abc123".into());
        s.sync_persisted();
        let entry = s.state.documents.get("a.md").expect("persisted entry");
        assert_eq!(entry.last_written_hash.as_deref(), Some("abc123"));
        assert_eq!(entry.inode, Some(7));
    }
}
