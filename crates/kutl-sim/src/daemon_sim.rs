//! `DaemonSim`: the synchronous, deterministic liveness harness for the pure
//! [`DaemonCore`].
//!
//! This drives `DaemonCore::handle` against the daemon's four bounded channels —
//! modeled as capacity-[`VecDeque`]s — under a seeded scheduler, with no tokio
//! runtime and no wall clock. It is the runtime analogue of the lattice's
//! confluence proptest: where that proves arbitration is order-independent, this
//! proves the event loop is **deadlock-free**.
//!
//! Two channel topologies are modeled:
//! - [`DaemonSim::new`] — the SHIPPED shape: unbounded outbound (`sync_cmd`) plus
//!   an intake high-water gate. The loop never blocks on an outbound send, so it
//!   always drains `inbound`, processes acks, and quiesces.
//! - [`DaemonSim::new_blocking`] — the OLD shape: a bounded `sync_cmd` (cap 64)
//!   where a full queue STALLS the inbound drain (modeling the loop blocked on a
//!   bounded `.send().await`). A full `sync_cmd` plus a non-empty `inbound` is a
//!   **fixpoint** — the reproduced bulk-load deadlock.
//!
//! The relay is modeled by a [`RegistryLattice`]: register/rename/unregister
//! commands `observe` a `DocRecord`, and the relay then produces a
//! [`Event::LifecycleAck`] back into `inbound` (through the cap-16 control
//! channel) so the daemon's own docs confirm — exactly the Mode-2 typed-ack rail.
//!
//! No `std::fs`, no tokio, no `Instant::now` — all time is the [`VirtualClock`]
//! and all scheduling is a seeded [`ChaChaRng`].

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use kutl_core::lattice::{DocRecord, RegistryLattice};
use kutl_core::{ActorId, Hlc};
use kutl_daemon::core::{DaemonCore, Effect, Event, EventStamp, SpaceState};
use kutl_daemon::daemon::JOURNAL_LINES_FORCE_SAVE;
use kutl_daemon::state::{IdentityJournalLine, JournalLineKind};
use kutl_daemon::{HashedContent, SuppressionSet};
use kutl_proto::sync::ChangeMetadata;
use rand::RngExt;
use rand_chacha::ChaChaRng;
use uuid::Uuid;

use crate::clock::VirtualClock;
use crate::prng::peer_rng;

/// The daemon's inbound channel capacity (`CHANNEL_CAPACITY`,
/// daemon/session.rs). The sim sizes `sync_cmd` to it as well: the shipped
/// outbound lanes are unbounded, and the bounded model is kept because the
/// blocking-model fixpoint tests reproduce the circular wait that bounding
/// them once caused.
const DAEMON_CHANNEL_CAP: usize = 64;

/// The relay actor's command channel capacity (`RELAY_CHANNEL_CAPACITY`,
/// kutl-relay lib.rs), modeled per connection: the sim's single daemon is
/// that channel's only producer.
const RELAY_INBOUND_CAP: usize = 256;

/// The ack rail from the relay to the daemon: a deliberately narrow 16-slot
/// bound. The shipped relay carries own-acks on an unbounded lane and its
/// bounded ctrl lane (`CTRL_CAPACITY`, conn.rs, 256 slots) carries only
/// broadcasts; this rail must stay this narrow so the blocking-model
/// fixpoint tests keep reproducing the circular wait a bounded ack rail
/// causes.
const MODELED_CTRL_CAP: usize = 16;

/// Consecutive non-progress steps that prove a true fixpoint (the deadlock),
/// rather than an RNG run that happened to pick empty queues. There are four
/// modeled queues; this multiple guarantees every queue was offered and rejected
/// before declaring the system wedged — conclusive in the synchronous model.
const FIXPOINT_STREAK: usize = 16;

/// Step budget for the seeding helpers' drain-to-quiescence: generous enough
/// that any healthy seed converges, finite so a livelocked model fails the
/// test instead of hanging it.
const SEED_DRAIN_STEP_BUDGET: usize = 10_000;

/// Fixed second half of the relay's synthetic [`ActorId`] UUID
/// (`from_u64_pair(seed, RELAY_ACTOR_TAG)`): tags relay-minted actors so they
/// are recognizable in dumps and never collide with the per-peer actors
/// derived from the seed alone.
const RELAY_ACTOR_TAG: u64 = 0xACE5;

/// A bounded FIFO modeling one of the daemon's tokio `mpsc` channels.
///
/// [`try_send`](Self::try_send) returns `Err(item)` when the queue is at
/// capacity (mirroring `mpsc::Sender::try_send`'s `Full` rejection — the
/// `CHANNEL_CAPACITY` cap), handing the item back so the caller can decide whether to
/// retry or block. [`try_recv`](Self::try_recv) pops the front (FIFO order).
///
/// The cap mapping the four daemon channels use: inbound = `sync_cmd` = 64
/// (`CHANNEL_CAPACITY`, daemon/session.rs), relay inbound = 256 (`RELAY_INBOUND_CAP`), relay ctrl = 16
/// (`MODELED_CTRL_CAP`, narrower than the shipped lanes).
#[derive(Debug)]
pub struct BoundedQueue<T> {
    cap: usize,
    q: VecDeque<T>,
}

impl<T> BoundedQueue<T> {
    /// A new queue holding at most `cap` items.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            q: VecDeque::new(),
        }
    }

    /// Enqueue `item` at the back, returning `Err(item)` when the queue is full.
    ///
    /// # Errors
    ///
    /// Returns the rejected `item` when `len == cap` (the channel is full), so a
    /// blocking caller can re-offer it on the next step.
    pub fn try_send(&mut self, item: T) -> Result<(), T> {
        if self.q.len() >= self.cap {
            return Err(item);
        }
        self.q.push_back(item);
        Ok(())
    }

    /// Pop the front item (FIFO), or `None` when empty.
    pub fn try_recv(&mut self) -> Option<T> {
        self.q.pop_front()
    }

    /// Whether the queue is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.q.len() >= self.cap
    }

    /// Whether the queue holds no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }

    /// The number of queued items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.q.len()
    }
}

/// The outcome of one [`DaemonSim::step`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepOutcome {
    /// One item was consumed from some queue and its work applied — the system
    /// made progress this step.
    Progressed,
    /// Every queue was empty — nothing to do.
    Idle,
    /// A queue had work but the topology could not advance it (the blocking
    /// `sync_cmd` is full and the loop is stalled on it). A `Blocked` step with
    /// non-empty queues is the reproduced deadlock fixpoint.
    Blocked,
}

/// The synchronous liveness driver: owns a [`SpaceState`], the four modeled
/// channels, an in-memory disk, and a [`RegistryLattice`] relay model, and runs
/// `DaemonCore::handle` under a seeded schedule.
#[expect(
    clippy::struct_excessive_bools,
    reason = "each bool models an independent daemon/shell mode (blocking shape, \
              armed fs failure, suppression edge, dirty persist) — not a state machine"
)]
pub struct DaemonSim {
    /// The pure per-space state the core mutates.
    state: SpaceState,
    /// Virtual time source for event stamps — never the wall clock.
    clock: Arc<VirtualClock>,
    /// Seeded scheduler RNG — picks which non-empty queue to service each step.
    rng: ChaChaRng,
    /// Unbounded producer-side buffer feeding `inbound`. The watcher/relay-reader
    /// is the producer; it back-pressures (parks) when the bounded `inbound` is
    /// full rather than dropping events. `refill_inbound` drains this into
    /// `inbound` as space frees.
    pending_in: VecDeque<Event>,
    /// The daemon's inbound event queue (cap 64, `CHANNEL_CAPACITY`).
    inbound: BoundedQueue<Event>,
    /// The current event's outbound commands awaiting a slot in `sync_cmd`. The
    /// loop stages one event's commands here and flushes them to `sync_cmd`; in
    /// the BLOCKING model a flush that can't complete (full `sync_cmd`) leaves the
    /// remainder here and the loop CANNOT pop the next inbound event — this is the
    /// in-flight `.send().await` that wedges the single-task loop. Empty between
    /// events in the shipped (unbounded) model.
    loop_out: VecDeque<OutCommand>,
    /// The daemon's outbound command queue to the relay (cap 64 shipped via the
    /// unbounded override; the blocking model treats it as a hard cap).
    sync_cmd: BoundedQueue<OutCommand>,
    /// The relay's inbound queue (cap 256, `RELAY_INBOUND_CAP`).
    relay_inbound: BoundedQueue<OutCommand>,
    /// The relay's control/ack queue back to the daemon (cap 16, `MODELED_CTRL_CAP`).
    relay_ctrl: BoundedQueue<Event>,
    /// In-memory disk: relative path → (content, version counter). The version
    /// counter is unused by the liveness assertions but records write ordering.
    disk: HashMap<PathBuf, (Vec<u8>, u64)>,
    /// The stat-and-place race window model: relative path → bytes for an
    /// untracked file armed to land on disk INSIDE the shell's
    /// `apply_guarded_place` critical section — i.e. BETWEEN the core's decision
    /// (effect emit) and the shell's atomic stat. This is the sim's model of the
    /// window the guarded place exists to close: a local create the watcher
    /// hasn't yet delivered to the core lands while the
    /// shell is mid-place, so the core believed the path free (`expected_free`) but
    /// the atomic stat must see the occupant. `put_untracked_file_after_decide` /
    /// `arm_disk_write_in_guarded_window` arm it; the place's stat applies it.
    armed_window_writes: HashMap<PathBuf, Vec<u8>>,
    /// The relay's registry lattice model — the same arbitration the real relay
    /// runs, so acks carry the arbitrated effective path.
    relay: RegistryLattice,
    /// `true` for the OLD blocking topology: a full `sync_cmd` stalls the inbound
    /// drain (the reproduced deadlock). `false` for the shipped unbounded shape.
    blocking: bool,
    /// Monotonic write counter for the disk version field.
    write_seq: u64,
    /// A synthetic origin actor for relay-side lattice stamps when an effect
    /// carries no HLC (the sim feeds events with `origin_hlc: None`).
    relay_actor: ActorId,
    /// One-shot failure flag for the next fs effect (the failed-write self-heal model).
    ///
    /// When armed, the next `WriteFile` or `RenameFile` effect in `route_effect`
    /// (or the AGREEMENT branch of `apply_guarded_place`) skips BOTH the disk
    /// mutation AND the shadow fold — modelling the funnel's log-and-swallow
    /// (`apply_write`/`apply_rename` returning `None`) that leaves the shadow
    /// unchanged on failure. The flag
    /// is cleared immediately after the skip. NO retry is scheduled — the real
    /// driver has none: the next `reconcile_placement` (whenever a later event
    /// or the periodic tick runs `DaemonCore::handle`) is what re-derives the
    /// mover and retries. Modelling an automatic next-step retry here broke
    /// the idle-heal rule: it hid the fact that an IDLE space never heals.
    fail_next_fs_effect: bool,
    /// Live-disk inode assignments: path → inode number.
    ///
    /// Models the OS-level inode the write funnel stats after a write and
    /// records in `FileIdentity::inode` (`write_doc`), and the place-time
    /// inode the identity claim in `apply_guarded_place` reads. Minted by
    /// `next_inode` on every modeled write (the atomic replace gives the
    /// file a fresh inode each time) and at seeding; survives out-of-band
    /// renames (the same inode follows the file to its new path, matching
    /// POSIX rename semantics). Also how the missing-source conform locates
    /// a relocated file by inode.
    disk_inodes: HashMap<PathBuf, u64>,
    /// Monotonically increasing inode counter for the in-memory disk. Seeded at 1
    /// so inode 0 is never assigned (0 is "no inode" in the shadow's `Option<u64>`).
    inode_counter: u64,
    /// The watcher's echo-suppression set, the REAL `SuppressionSet` the
    /// shell's watcher holds, fed the same way the funnel feeds it: a write
    /// registers its content hash, a removal registers `None`, a rename
    /// registers one pair-matched entry. A local create consults it through
    /// `consume_write_echo`: a Create/Modify whose bytes hash matches the
    /// registered write is the daemon's OWN echo (suppressed); a
    /// different-bytes create, or one at a removal half, is a GENUINE edit
    /// and is never swallowed. The set is never ticked here, so an entry
    /// lives until its echo.
    suppressions: SuppressionSet,
    /// Whether the most recent [`Self::feed_local_create`] was swallowed by an
    /// echo suppression (`true`) or surfaced to the core as a genuine edit
    /// (`false`). Lets the rename-suppression pairing test assert
    /// own-write-suppressed vs peer-recreate-not-swallowed without inspecting
    /// the suppression set itself.
    last_local_create_suppressed: bool,
    /// Every `RenameDocument` the relay model received, in arrival order, as
    /// `(document_id, new_path)`. The dropped-rename observability rail: a concurrent
    /// local rename must REACH the relay (not be silently dropped daemon-side),
    /// even when it loses the arbitration — the lattice's current winner alone
    /// cannot show a superseded arrival.
    renames_relayed: Vec<(String, String)>,
    /// Every `Subscribe` the relay model received, in arrival order. The
    /// delivery rail: the real relay broadcasts a doc's ops ONLY to its
    /// subscribers, so a doc this daemon tracks but never subscribed to never
    /// receives peer edits — the `f_conflicts` create/create divergence (the
    /// observer held the winning doc unsubscribed and silently kept pre-edit
    /// content). A scenario that expects remote content to flow for a doc must
    /// find it here.
    /// The persisted `state.ksnap` MIRROR: a snapshot taken only when the
    /// modeled coalesced flush fires (`flush_state_if_caught_up`, mirroring
    /// the driver's save-coalescing rule: `Effect::SaveState` marks dirty, the
    /// write happens once the intake drains). `restart()` reloads from THIS,
    /// not the live map — the gap between them IS the coalesced-persist crash
    /// window (the crash-mid-bulk class).
    persisted_state: kutl_daemon::state::DaemonState,
    /// The persisted `.dt` sidecar mirror: full-oplog snapshots taken when an
    /// `Effect::SaveDoc` is APPLIED (the real `save_doc` writes inline).
    /// `restart()` reloads each as a fresh CRDT carrying exactly the
    /// persisted oplog.
    persisted_docs: HashMap<String, (Vec<u8>, Vec<ChangeMetadata>)>,
    /// The persisted identity-journal MIRROR (`.kutl/identity.klog`): one line
    /// per drained pending path, built by the REAL
    /// `SpaceState::journal_line_for` and replayed on `restart()` through the
    /// REAL `DaemonState::apply_journal_line` fold — the same
    /// single-implementation rule as `sync_persisted`, so the modeled and
    /// real persist contracts cannot drift. Every modeled save truncates it
    /// (the real save's truncate). It is what a crash between coalesced
    /// saves restarts from: the identity the snapshot has not caught up to
    /// survives only here.
    persisted_journal: Vec<kutl_daemon::state::IdentityJournalLine>,
    /// A persist is pending — the modeled `SpaceWorker::state_dirty`.
    state_dirty: bool,
    /// Journal lines appended since the last modeled save — the drain's
    /// growth-bound counter (`SpaceWorker::journal_lines_since_save`): past
    /// [`JOURNAL_LINES_FORCE_SAVE`] the drain marks state dirty so the next
    /// flush compacts the journal into one snapshot.
    journal_lines_since_save: u32,
    subscriptions: Vec<String>,
}

/// An outbound command the daemon sent toward the relay. Models the relevant
/// payload of `SyncCommand` (`client.rs:95`) the relay must act on; non-lifecycle
/// commands (subscribe, send-ops, housekeeping) carry no relay-model state and
/// are modeled as `Other`, which the relay simply consumes.
#[derive(Debug, Clone)]
enum OutCommand {
    /// A `RegisterDocument` the relay folds into its lattice and acks.
    Register {
        document_id: String,
        path: String,
        hlc: Option<Hlc>,
    },
    /// A `RenameDocument` the relay folds into its lattice and acks.
    Rename {
        document_id: String,
        new_path: String,
        hlc: Option<Hlc>,
    },
    /// An `UnregisterDocument` the relay folds into its lattice as a tombstone.
    Unregister {
        document_id: String,
        hlc: Option<Hlc>,
    },
    /// A `Subscribe` the relay records into its subscriber set (no lattice
    /// effect). Modeled explicitly — not as `Other` — because delivery of a
    /// doc's remote ops is gated on it (the `subscriptions` rail).
    Subscribe { document_id: String },
    /// Any other outbound command (send-ops, save-state, metrics): the
    /// relay consumes it with no lattice effect.
    Other,
}

impl DaemonSim {
    /// Build the SHIPPED-shape sim (unbounded outbound + intake gate) for `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self::build(seed, /* blocking */ false)
    }

    /// Build the OLD blocking-shape sim for `seed`: a bounded `sync_cmd` that, when
    /// full, stalls the inbound drain — so a full `sync_cmd` plus pending inbound
    /// is a fixpoint (the reproduced bulk-load deadlock).
    #[must_use]
    pub fn new_blocking(seed: u64) -> Self {
        Self::build(seed, /* blocking */ true)
    }

    fn build(seed: u64, blocking: bool) -> Self {
        let relay_actor = ActorId(Uuid::from_u64_pair(seed, RELAY_ACTOR_TAG));
        Self {
            state: SpaceState::new_for_test(
                "sim-space".to_owned(),
                PathBuf::from("/sim"),
                "did:sim:daemon".to_owned(),
            ),
            clock: VirtualClock::new(0),
            rng: peer_rng(seed, 0),
            pending_in: VecDeque::new(),
            inbound: BoundedQueue::new(DAEMON_CHANNEL_CAP),
            loop_out: VecDeque::new(),
            sync_cmd: BoundedQueue::new(DAEMON_CHANNEL_CAP),
            relay_inbound: BoundedQueue::new(RELAY_INBOUND_CAP),
            relay_ctrl: BoundedQueue::new(MODELED_CTRL_CAP),
            disk: HashMap::new(),
            armed_window_writes: HashMap::new(),
            relay: RegistryLattice::new(),
            blocking,
            write_seq: 0,
            relay_actor,
            fail_next_fs_effect: false,
            disk_inodes: HashMap::new(),
            // Inode 0 is "no inode" (Option<u64> None); start at 1.
            inode_counter: 1,
            suppressions: SuppressionSet::default(),
            last_local_create_suppressed: false,
            renames_relayed: Vec::new(),
            subscriptions: Vec::new(),
            persisted_state: kutl_daemon::state::DaemonState::default(),
            persisted_docs: HashMap::new(),
            persisted_journal: Vec::new(),
            state_dirty: false,
            journal_lines_since_save: 0,
        }
    }

    /// Push `event` onto a holding buffer for the daemon's inbound queue.
    ///
    /// The producer (watcher/relay-reader) feeds a bounded `inbound` channel; a
    /// bulk seed of >64 events can't all sit in the cap-64 queue at once. So
    /// `feed` parks events in an unbounded **pending** buffer and `refill_inbound`
    /// moves them into the bounded `inbound` queue as space frees — exactly the
    /// producer's back-pressure (a full `inbound` blocks the producer, it does not
    /// drop events). The liveness assertion still requires the whole buffer to
    /// drain to empty.
    pub fn feed(&mut self, event: Event) {
        // Advance virtual time so successive fed events are ordered in stamps.
        self.clock.tick();
        self.pending_in.push_back(event);
    }

    /// Move parked events into the bounded `inbound` queue while it has room. This
    /// is the producer side: it never drops, only back-pressures (stops when
    /// `inbound` is full).
    fn refill_inbound(&mut self) {
        while !self.inbound.is_full() {
            let Some(ev) = self.pending_in.pop_front() else {
                break;
            };
            self.inbound.q.push_back(ev);
        }
    }

    /// Service one queue chosen by the seeded RNG. See [`StepOutcome`].
    pub fn step(&mut self) -> StepOutcome {
        self.refill_inbound();
        let choices = self.nonempty_queues();
        if choices.is_empty() {
            return StepOutcome::Idle;
        }
        let pick = choices[self.rng.random_range(0..choices.len())];
        self.service(pick)
    }

    /// Service one queue chosen by a caller-supplied index, so a property test can
    /// control the schedule (rather than the internal RNG).
    ///
    /// `raw` is reduced modulo the number of currently non-empty, schedulable
    /// queues; an empty (or fully stalled) system is [`StepOutcome::Idle`]. The
    /// proptest harness passes `idx.index(usize::MAX)` — any `usize` works, since
    /// the modulo maps it into the live set.
    pub fn step_scheduled(&mut self, raw: usize) -> StepOutcome {
        self.refill_inbound();
        let choices = self.nonempty_queues();
        if choices.is_empty() {
            return StepOutcome::Idle;
        }
        let pick = choices[raw % choices.len()];
        self.service(pick)
    }

    /// Drive `step` until quiescent, panicking if `max_steps` is exhausted first.
    ///
    /// A fixpoint reached before quiescence (every `step` returns `Idle`/`Blocked`
    /// with work still pending) trips the step budget — the reproduced deadlock.
    ///
    /// # Panics
    ///
    /// Panics if quiescence is not reached within `max_steps`.
    pub fn drain_to_quiescence_or_fail(&mut self, max_steps: usize) {
        assert!(
            self.try_drain_to_quiescence(max_steps),
            "daemon sim did not reach quiescence within {max_steps} steps (deadlock): \
             inbound={} sync_cmd={} relay_inbound={} relay_ctrl={} unconfirmed={}",
            self.inbound.len(),
            self.sync_cmd.len(),
            self.relay_inbound.len(),
            self.relay_ctrl.len(),
            self.unconfirmed_count(),
        );
    }

    /// Drive `step` until quiescent, returning `false` on fixpoint/exhaustion
    /// instead of panicking — the assertion seam the blocking-model proof uses.
    pub fn try_drain_to_quiescence(&mut self, max_steps: usize) -> bool {
        let mut blocked_streak = 0usize;
        for _ in 0..max_steps {
            if self.is_quiescent() {
                return true;
            }
            match self.step() {
                StepOutcome::Progressed => blocked_streak = 0,
                StepOutcome::Idle => {
                    // Idle with work pending elsewhere (e.g. a stalled outbound)
                    // means the schedulable set can't make progress.
                    blocked_streak += 1;
                }
                StepOutcome::Blocked => blocked_streak += 1,
            }
            // A fixpoint: enough consecutive non-progress steps that no schedule
            // can advance (the blocking `sync_cmd` is full and stalls the drain).
            if blocked_streak > FIXPOINT_STREAK {
                return false;
            }
        }
        self.is_quiescent()
    }

    /// Whether the system is at rest: all queues empty, every relay-known doc
    /// confirmed, and the disk's tracked files agree with the relay's arbitrated
    /// effective-path index.
    #[must_use]
    pub fn is_quiescent(&self) -> bool {
        self.intake_drained()
            && self.sync_cmd.is_empty()
            && self.relay_inbound.is_empty()
            && self.relay_ctrl.is_empty()
            && self.unconfirmed_count() == 0
            && self.disk_is_orphan_free()
    }

    /// Whether the in-memory disk is free of ORPHAN files: every file the core
    /// materialized sits at a path some alive doc in the daemon's client-side
    /// registry (`known_records`) currently claims.
    ///
    /// This is the convergence half of quiescence. It is checked against the
    /// daemon's OWN registry (folded from both local and remote lifecycle), not
    /// the relay model — the relay model only learns of docs the daemon registers
    /// OUTBOUND, so remote-originated docs (the proptest domain) are invisible to
    /// it. A lingering file for a path no live record claims is a genuine
    /// liveness/consistency bug (a delete that never reached disk); a doc whose
    /// content never arrived (a pure register with no `WriteFile`) writes no file,
    /// so the orphan-free check never false-flags it.
    fn disk_is_orphan_free(&self) -> bool {
        let claimed: std::collections::HashSet<String> = self
            .state
            .known_records
            .records()
            .filter(|(_, r)| r.is_alive())
            .flat_map(|(_, r)| [r.effective_path(), r.path.clone()])
            .collect();
        self.disk
            .keys()
            .all(|rel| claimed.contains(&kutl_daemon::core::rel_path_to_string(rel)))
    }

    /// The number of files the core has materialized on the in-memory disk. A
    /// register-only place writes NO bytes (the content streams in via a later
    /// `RemoteOps`), so a bulk seed of pure remote registers leaves this at 0.
    #[must_use]
    pub fn disk_path_count(&self) -> usize {
        self.disk.len()
    }

    /// Whether the core has placed `document_id` on disk — i.e. the shadow holds a
    /// path for it (set by [`Self::apply_guarded_place`] on a landed place). The
    /// sim's read of "did the gamma cascade track this doc?".
    #[must_use]
    pub fn is_tracked(&self, document_id: &str) -> bool {
        Uuid::parse_str(document_id)
            .ok()
            .is_some_and(|uid| self.state.shadow.shadow_path.contains_key(&uid))
    }

    /// Whether the cascade has DEFERRED `document_id`: its desired path is held by
    /// an untracked occupant and it was not revival-exempt, so the core recorded it
    /// in `state.deferred`. The sim's read of "the doc is parked waiting
    /// for the occupant to vacate, not placed".
    #[must_use]
    pub fn is_deferred(&self, document_id: &str) -> bool {
        self.state.deferred.contains_key(document_id)
    }

    /// Whether the daemon's client-side registry (`known_records`) holds `document_id`
    /// as an ALIVE record — the structural source of truth for "the cascade will
    /// consider this doc a placement candidate." An unregistered doc is a tombstone:
    /// not alive, so `desired_assignment` excludes it and it can never be a mover
    /// (ghost resurrection is ruled out structurally, not by a guard).
    #[must_use]
    pub fn is_alive_record(&self, document_id: &str) -> bool {
        Uuid::parse_str(document_id).ok().is_some_and(|uid| {
            self.state
                .known_records
                .get(&uid)
                .is_some_and(kutl_core::lattice::DocRecord::is_alive)
        })
    }

    /// Place an UNTRACKED file on the in-memory disk and mark the shadow occupant
    /// `Untracked` — the FS-1/2 occupant a non-revival mover defers onto.
    ///
    /// This is the sim's stand-in for the shell's atomic stat seeing a file no
    /// document is arbitrated onto (the `Occupant::Untracked` branch): the bytes
    /// land on disk and `shadow_occupant[casefold(rel)] = Untracked`, so the next
    /// `reconcile_placement` sees the path held and defers a non-revival mover
    /// onto it. The real shell reaches the same state by feeding the core an
    /// `UntrackedFileObserved` event; the harness seeds the occupant directly.
    pub fn put_untracked_file(&mut self, rel: &str, content: &[u8]) {
        let path = PathBuf::from(rel);
        self.write_seq += 1;
        self.disk
            .insert(path.clone(), (content.to_vec(), self.write_seq));
        self.state.shadow.shadow_occupant.insert(
            kutl_daemon::core::casefold(&path),
            kutl_daemon::core::Occupant::Untracked,
        );
    }

    /// Arm an untracked file to land on disk INSIDE the shell's `GuardedPlace`
    /// critical section for `rel` — the stat-and-place race window model (FS-1).
    ///
    /// Unlike [`Self::put_untracked_file`] (which seeds the disk AND the shadow
    /// occupant immediately, so the CORE's `reconcile_placement` sees the occupant
    /// and defers before emitting any place), this seeds NEITHER until the shell
    /// goes to place. The core therefore decides with the path believed FREE
    /// (`expected_free == true`); the bytes land only when `apply_guarded_place`
    /// stats `rel`, modeling a local create the watcher has not yet delivered that
    /// slips in between the core's decision and the shell's place. The atomic stat
    /// must catch it → the doc defers, the local bytes are never clobbered.
    pub fn put_untracked_file_after_decide(&mut self, rel: &str, content: &[u8]) {
        self.armed_window_writes
            .insert(PathBuf::from(rel), content.to_vec());
    }

    /// Alias of [`Self::put_untracked_file_after_decide`] for the atomicity
    /// cases: a local create injected between the core's decision and the
    /// shell's mutation. Same window model; named to read as the atomicity probe.
    pub fn arm_disk_write_in_guarded_window(&mut self, rel: &str, content: &[u8]) {
        self.put_untracked_file_after_decide(rel, content);
    }

    /// The current on-disk bytes at `rel`, if any. Lets a race-window case assert the
    /// local bytes survived (were not clobbered by a place the atomic stat caught).
    #[must_use]
    pub fn disk_content(&self, rel: &str) -> Option<Vec<u8>> {
        self.disk.get(&PathBuf::from(rel)).map(|(c, _)| c.clone())
    }

    /// Model the startup file scan minting a FRESH, distinct uuid for an
    /// offline-created file at `rel` and ADOPTING it (`initial_file_scan` after
    /// `startup_reconciliation`, daemon/startup.rs).
    ///
    /// Used by the FS-2 startup case: the offline file the remote register DEFERRED
    /// onto is adopted by the scan as its OWN, distinct document — the two authors
    /// are NOT conflated (no merge). The real `initial_file_scan` mints a fresh id
    /// here because identity follows placement: a deferred remote register never
    /// claims `file_identity`/`uuid_to_path` at `rel`, so the scan's local create
    /// resolves to no existing document and mints a genuinely fresh id rather than
    /// resolving the path back to the deferred remote. This helper asserts that
    /// invariant FIRST: a stale claim at `rel` would conflate the two authors, and
    /// silently clearing it here would hide exactly that regression.
    ///
    /// Then it drives a local `FileModified` (the scan's create) so the core mints
    /// the fresh id, seeds its record, emits an outbound `RegisterDocument`, and
    /// folds the file into the shadow as that id (`Tracked`). The resulting
    /// two-records-at-one-path collision conflict-copies on the next reconcile — both
    /// files survive. Returns the fresh id (empty string when there are no bytes).
    pub fn scan_mints_uuid_for(&mut self, rel: &str) -> String {
        let path = PathBuf::from(rel);
        let content = self
            .disk
            .get(&path)
            .map(|(c, _)| c.clone())
            .unwrap_or_default();
        // ASSERT, never clear: identity follows placement, so a DEFERRED
        // remote register must not have claimed this path. Clearing a stale
        // claim here would let the case pass while eager claiming conflates
        // the two authors; asserting makes that regression fail loudly.
        assert!(
            !self.state.file_identity.contains_key(&path),
            "a deferred remote register must not claim identity at {} (identity follows placement)",
            path.display()
        );
        self.feed(Event::FileModified {
            rel: path.clone(),
            content: Some(content),
            stamp: EventStamp {
                wall_ms: self.now_ms(),
                origin_hlc: None,
            },
        });
        // Process the create so its fresh identity is minted and its record seeded.
        self.run_one_handle();
        let id = self
            .state
            .file_identity
            .get(&path)
            .map(|fi| fi.document_uuid.clone())
            .unwrap_or_default();
        // No manual shadow fold: the core's mint folds `shadow.set_tracked`
        // itself (every local rm/mv/create folds the shadow inside the core),
        // so the modeled `FileModified` above already adopted the file. Folding
        // it here as well would fake state the core never produced; the
        // assert keeps the model honest.
        assert!(
            self.state
                .shadow
                .shadow_occupant
                .contains_key(&kutl_daemon::core::casefold(&path)),
            "the modeled mint folds the shadow at {}",
            path.display()
        );
        id
    }

    /// Seed a prior applied-lifecycle watermark for `document_id` so the next
    /// `RemoteRegister` for it reads `was_seen_before == true` (a REVIVAL, exempt
    /// from deferral). The sim's stand-in for a doc this daemon had previously
    /// registered/renamed/deleted before its current revival register arrives.
    pub fn seed_prior_lifecycle(&mut self, document_id: &str, hlc: Hlc) {
        self.state.record_lifecycle_hlc(document_id, hlc);
    }

    /// Register a rename's echo suppression exactly as `rename_doc` does:
    /// ONE pair-matched entry linking the removal half at `old` and the
    /// write half at `new`, so an unrelated single entry at either path can
    /// never swallow a user rename. Every rename-shaped disk effect routes
    /// through here.
    fn push_rename_suppression_pair(
        &mut self,
        old: &std::path::Path,
        new: &std::path::Path,
        write_hash: Option<Vec<u8>>,
    ) {
        self.suppressions
            .insert_rename_pair(old.to_path_buf(), new.to_path_buf(), write_hash);
    }

    /// Whether the daemon's OWN rename pair `old` -> `new` is registered for
    /// echo suppression: the pair-matched consumer fires only for that exact
    /// pair, so a test can tell a pair from two unrelated single entries.
    pub fn consume_rename_pair_echo(
        &mut self,
        old: &std::path::Path,
        new: &std::path::Path,
    ) -> bool {
        self.suppressions.consume_rename_pair_echo(old, new)
    }

    /// Arm a one-shot failure for the NEXT disk-effect (`WriteFile` or
    /// `RenameFile`) that arrives in `route_effect`.
    ///
    /// When fired, the effect skips BOTH the disk mutation AND
    /// `apply_effect_result` — modelling the funnel's log-and-swallow
    /// (`apply_write`/`apply_rename` returning `None`) that leaves the shadow
    /// unchanged on a failed fs op. The
    /// flag clears immediately after the first skip, so subsequent effects are
    /// unaffected (the self-heal path: the next reconcile re-derives and retries).
    pub fn fail_next_fs_effect(&mut self) {
        self.fail_next_fs_effect = true;
    }

    /// Establish a doc as TRACKED at `path` with `content` on disk.
    ///
    /// Feeds a `RemoteRegister` (so the relay folds the record and acks back),
    /// writes the content bytes directly to the in-memory disk (the subscribe
    /// content-stream step that would normally write them), and drains to
    /// quiescence so the ack confirms the doc and the shadow records the
    /// placement. After this call `shadow_path(doc) == Some(path)`.
    ///
    /// Assigns a fresh inode to the placed file and records it in
    /// `file_identity` so the missing-source conform can recover the doc's
    /// inode after an out-of-band relocation.
    ///
    /// Suitable for seeding a "doc already placed" baseline before a test probes
    /// a subsequent operation (rename, conflict, etc.).
    pub fn register_and_place(&mut self, doc: &str, path: &str, content: &[u8]) {
        let hlc = self.hlc_for("r", self.now_ms());
        // Seed the relay so the ack carries the correct arbitrated path.
        self.feed(Event::RemoteRegister {
            document_id: doc.into(),
            path: path.into(),
            stamp: EventStamp {
                wall_ms: self.now_ms(),
                origin_hlc: Some(hlc),
            },
        });
        self.drain_to_quiescence_or_fail(SEED_DRAIN_STEP_BUDGET);
        // Write the content bytes to disk (models the subscribe content-stream).
        // The shadow is already updated by apply_effect_result via GuardedPlace;
        // seed the raw disk bytes so later disk assertions can inspect them.
        let path_buf = std::path::PathBuf::from(path);
        self.write_seq += 1;
        self.disk
            .insert(path_buf.clone(), (content.to_vec(), self.write_seq));
        // Assign a fresh inode and record it on disk and in `file_identity`,
        // the daemon's recorded view that the relocated-rename drain,
        // `SpaceState::rename_source` and the missing-source conform read (the
        // GuardedPlace ran before these content bytes landed, so its
        // place-time inode fold saw none).
        let inode = self.next_inode();
        self.disk_inodes.insert(path_buf.clone(), inode);
        self.state.identity_set_inode(&path_buf, Some(inode));
    }

    /// Whether the in-memory disk has a file at `rel`.
    ///
    /// The direct "does a path exist on disk" check, equivalent to
    /// `abs_path.exists()` in the real daemon. Used by the conform cases to assert
    /// that a conformed file lands at the authoritative path and the relocated
    /// source is vacated.
    #[must_use]
    pub fn disk_path_exists(&self, rel: &str) -> bool {
        self.disk.contains_key(&PathBuf::from(rel))
    }

    /// Every `RenameDocument` the relay model received, in arrival order, as
    /// `(document_id, new_path)` — the dropped-rename observability rail (see
    /// the field doc).
    #[must_use]
    pub fn renames_relayed(&self) -> &[(String, String)] {
        &self.renames_relayed
    }

    /// Every `Subscribe` the relay model received, in arrival order — the
    /// delivery rail (see the field doc): a doc absent here never receives peer
    /// edits, however correct its registration and placement look.
    #[must_use]
    pub fn subscribed_docs(&self) -> &[String] {
        &self.subscriptions
    }

    /// The document id whose identity (`file_identity`) claims `rel`, if any —
    /// the sim's read of "which doc the mint/place put at this path".
    #[must_use]
    pub fn doc_id_at(&self, rel: &str) -> Option<String> {
        self.state
            .file_identity
            .get(&PathBuf::from(rel))
            .map(|fi| fi.document_uuid.clone())
    }

    /// The live-disk path currently carrying `inode`, if any — the sim twin of the
    /// driver's `space_file_with_inode` (`daemon/identity.rs`) used by the missing-source
    /// conform in [`Self::apply_guarded_place`].
    fn disk_path_with_inode(&self, inode: u64) -> Option<PathBuf> {
        self.disk_inodes
            .iter()
            .find(|(_, i)| **i == inode)
            .map(|(path, _)| path.clone())
    }

    /// Feed a LOCAL file create/modify at `rel` with `content`, applying the
    /// watcher's echo-suppression dispatch (`classify_flush_event`'s Create/Modify
    /// arm, `watcher.rs`) BEFORE the event reaches the core.
    ///
    /// The shell's `consume_write_echo` rule (watcher/suppression.rs): a create whose
    /// `sha256(content)` matches a `Some(hash)` write-half suppression at `rel` is
    /// the daemon's OWN write echo — it is consumed (swallowed), never re-ingested
    /// as a fresh edit (op amplification / phantom conflict-copy). A create with
    /// DIFFERENT bytes — or one at a `None` removal half — is a GENUINE edit and
    /// is delivered to the core as `Event::FileModified`, which tracks it. This is
    /// the catastrophic invariant the pairing locks: own write IS suppressed; a
    /// peer recreate in the same window is NOT swallowed.
    ///
    /// [`Self::last_local_create_was_suppressed`] reports which branch fired.
    pub fn feed_local_create(&mut self, rel: &str, content: &[u8]) {
        let path = PathBuf::from(rel);
        // The watcher classifies against the bytes it read (carried on the event,
        // not re-read from disk in the core). A daemon-own write echo is swallowed
        // here at the shell edge and never reaches the core — its bytes are already
        // the placed file's, so there is nothing more to materialize.
        if self.consume_write_echo(&path, content) {
            self.last_local_create_suppressed = true;
            return;
        }
        // Genuine edit: deliver it to the core as a local modify. The placement
        // cascade mints/places it (a new doc onto a free path) — its bytes land via
        // the place / content stream, the same convention the bulk-seed local
        // creates use (no eager disk write before the place).
        self.last_local_create_suppressed = false;
        self.feed(Event::FileModified {
            rel: path,
            content: Some(content.to_vec()),
            stamp: EventStamp {
                wall_ms: self.now_ms(),
                origin_hlc: None,
            },
        });
    }

    /// Whether the most recent [`Self::feed_local_create`] was swallowed as the
    /// daemon's own write echo (`true`) or surfaced to the core as a genuine edit
    /// (`false`).
    #[must_use]
    pub fn last_local_create_was_suppressed(&self) -> bool {
        self.last_local_create_suppressed
    }

    /// Whether the disk path `rel` is currently TRACKED by some document — i.e.
    /// the shadow holds an `Occupant::Tracked` for it. The sim's read of "a file
    /// the core adopted at this path", used to prove a genuine recreate was
    /// materialized (not swallowed by the removal echo).
    #[must_use]
    pub fn is_tracked_at(&self, rel: &str) -> bool {
        self.tracked_at(&PathBuf::from(rel)).is_some()
    }

    /// Simulate a user out-of-band rename: move `old` → `new` on the live disk
    /// WITHOUT delivering a watcher event to the core, modelling `mv old.md
    /// moved.md` by the user while the daemon's watcher debounce window hasn't
    /// fired yet — the file is physically at `new`, the daemon hasn't observed it.
    ///
    /// FIDELITY: the daemon's in-memory shadow is updated ONLY by the daemon's own
    /// effects (`apply_effect_result`); it is BLIND to a raw user rename. So this
    /// moves the live disk + the OS-level inode (`disk_inodes`, which the file
    /// carries), but leaves the daemon shadow entirely stale — `shadow_path[id]` and
    /// the occupant still point at `old`.
    ///
    /// This is the realistic state the `f_conflicts` rename races hit: with the shadow stale,
    /// the cascade emits a blind `GuardedPlace(Rename old→target)`, and the SHELL must
    /// conform the relocated file by inode (`apply_guarded_place`, the twin of
    /// `place_now`'s missing-source conform, `daemon/effects.rs`).
    pub fn relocate_out_of_band(&mut self, old: &str, new: &str) {
        let old_buf = PathBuf::from(old);
        let new_buf = PathBuf::from(new);
        // Move the file bytes on the live disk.
        if let Some(entry) = self.disk.remove(&old_buf) {
            self.write_seq += 1;
            self.disk.insert(new_buf.clone(), entry);
        }
        // Move the disk inode tracking: the same OS inode follows the file. The
        // daemon shadow is NOT updated — it never saw this rename.
        if let Some(inode) = self.disk_inodes.remove(&old_buf) {
            self.disk_inodes.insert(new_buf.clone(), inode);
        }
        // The file is gone from `old` on disk, so a live stat of `old` finds
        // nothing. (The shadow's stale `shadow_path`/occupant are
        // the point — they still claim `old`, which is exactly the divergence the
        // shell conform must reconcile against live disk.)
    }

    /// The current shadow path for `document_id` as a forward-slash string, or
    /// `None` if the doc is not tracked in the shadow.
    ///
    /// Mirrors the driver's `file_identity` map (daemon.rs) for the shadow side: the value
    /// `shadow_path[id]` after a successful place, folded via
    /// `apply_effect_result`. Used by the failed-disk-effect cases to assert the shadow is
    /// (or is NOT) advanced after a failed disk effect.
    #[must_use]
    pub fn shadow_path(&self, document_id: &str) -> Option<String> {
        Uuid::parse_str(document_id).ok().and_then(|uid| {
            self.state
                .shadow
                .shadow_path
                .get(&uid)
                .map(|p| kutl_daemon::core::rel_path_to_string(p))
        })
    }

    /// Dispatch exactly ONE event through the modeled loop iteration — the
    /// same unit the scheduled loop runs — WITHOUT flushing the staged
    /// outbound commands into `sync_cmd`, so a test can inspect the state a
    /// single event derives (`deferred`, placement, identity) before the
    /// relay model sees or acks anything.
    ///
    /// Refills `inbound` from the pending buffer, pops one event, and runs
    /// [`Self::dispatch_one`] over it. A no-op when there is no event to
    /// process.
    pub fn run_one_handle(&mut self) {
        self.refill_inbound();
        let Some(event) = self.inbound.try_recv() else {
            return;
        };
        self.dispatch_one(event);
    }

    /// Every queued-but-undispatched input drained — the loop is caught up.
    /// The ONE definition of "drained" behind every burst-coalescing decision
    /// in the model (the `intake_backlogged` flag, the drain-edge reconcile,
    /// the coalesced flush, quiescence), mirroring the driver's
    /// `SpaceWorker::intake_drained`. The pending buffer counts: it is the
    /// producer's parked intake, so a non-empty buffer means the loop is
    /// still mid-burst even while the bounded queue sits empty.
    fn intake_drained(&self) -> bool {
        self.inbound.is_empty() && self.pending_in.is_empty()
    }

    /// One modeled loop iteration over a popped `event`, in the driver's
    /// order (`event_loop`, daemon/session.rs): the loop-head gate flag, the
    /// shell's pre-core disk edge, the pure core, its effects, then the loop
    /// tail. The ONE dispatch unit behind both the scheduled loop
    /// (`service_inbound`) and the single-step probe (`run_one_handle`).
    fn dispatch_one(&mut self, event: Event) {
        // The loop-head gate flag: backlogged while more inputs are queued.
        // (The pacing floor is not modeled; see `loop_tail`.)
        self.state.intake_backlogged = !self.intake_drained();
        self.model_disk_edge_for(&event);
        let effects = DaemonCore::handle(&mut self.state, event);
        for effect in effects {
            self.route_effect(effect);
        }
        self.loop_tail();
    }

    /// The modeled loop tail, in the driver's order (`event_loop`,
    /// daemon/session.rs): drain-edge reconcile probe, then the
    /// identity-journal drain, then the coalesced flush. The pacing floor is
    /// deliberately NOT modeled: it is a wall-clock rate limit, and with it
    /// off the sim runs strictly MORE passes — by confluence the same
    /// fixpoint — so the lattice properties under test are unaffected while
    /// every scenario still exercises the skip-and-arm/drain-edge machinery.
    fn loop_tail(&mut self) {
        if self.state.placement_dirty && self.intake_drained() {
            self.state.intake_backlogged = false;
            let effects = kutl_daemon::core::reconcile_placement(&mut self.state);
            for effect in effects {
                self.route_effect(effect);
            }
        }
        self.drain_identity_journal_model();
        self.flush_state_if_caught_up();
    }

    /// The modeled journal append (`SpaceWorker::append_identity_journal`):
    /// the lines land on the journal mirror and are charged to the force-save
    /// budget — past [`JOURNAL_LINES_FORCE_SAVE`] lines since the last save,
    /// state is marked dirty so the next flush compacts the journal into one
    /// snapshot (an edit-only session never dirties state any other way).
    /// The modeled append never fails, so torn tails are not modeled — the
    /// mirror holds only whole well-formed lines.
    fn append_journal_lines_model(&mut self, lines: Vec<IdentityJournalLine>) {
        let appended = u32::try_from(lines.len()).unwrap_or(u32::MAX);
        self.persisted_journal.extend(lines);
        self.journal_lines_since_save = self.journal_lines_since_save.saturating_add(appended);
        if self.journal_lines_since_save > JOURNAL_LINES_FORCE_SAVE {
            self.state_dirty = true;
        }
    }

    /// The modeled journal drain (`SpaceWorker::drain_identity_journal`):
    /// append the REAL shared batch (`SpaceState::journal_batch`, whose
    /// order is the drain's own) to the journal mirror under the real
    /// growth bound.
    fn drain_identity_journal_model(&mut self) {
        let lines = self.state.journal_batch();
        self.state.journal_pending.clear();
        self.append_journal_lines_model(lines);
    }

    /// The modeled coalesced persist (`SpaceWorker::flush_state_if_caught_up`,
    /// the save-coalescing rule): the snapshot is written only when a save is
    /// pending AND the intake is drained, so a bulk burst does one save at its
    /// end — and a crash mid-burst restarts from a STALE snapshot (the window
    /// the crash-mid-bulk e2e probes) repaired only by the journal mirror.
    ///
    /// The real flush also holds a minimum interval between saves
    /// (`MIN_STATE_SAVE_INTERVAL`); the sim deliberately does not model it.
    /// It is a wall-clock rate limit, and without it the sim runs strictly
    /// MORE flushes — by confluence the same persisted fixpoint.
    fn flush_state_if_caught_up(&mut self) {
        if self.state_dirty && self.intake_drained() {
            // The REAL pre-serialize sync (inodes, HLC floor, register
            // stamps) — shared with the driver's `save_state`.
            self.state.sync_persisted();
            self.persisted_state = self.state.state.clone();
            // The snapshot subsumes every journaled line: the real save
            // truncates the journal and resets the growth-bound counter. The
            // pending set needs no clearing here — the modeled drain that
            // runs before every flush never fails, so it is already empty.
            self.persisted_journal.clear();
            self.journal_lines_since_save = 0;
            self.state_dirty = false;
        }
    }

    /// The `WriteFile` arm of `route_effect` (extracted for length): the
    /// one-shot failure model (an armed failure skips BOTH the disk
    /// mutation AND the shadow fold — the funnel's log-and-swallow, so the
    /// next reconcile re-derives and retries), then the real funnel's
    /// sequence (`write_doc`, daemon/effects.rs) step for step: the write
    /// echo-suppression keyed by the content hash, the write intent journaled
    /// for text bytes, the disk mutation, the post-write identity (a fresh
    /// inode — the atomic replace gives the file a new one every time — and
    /// the landed hash), the immediate identity snapshot, and finally the
    /// landed-write shadow fold (the shell ACK). The modeled inode in
    /// `disk_inodes` stands in for the stat the funnel runs on the placed
    /// file.
    fn apply_write_file(&mut self, rel: PathBuf, content: HashedContent) {
        if self.fail_next_fs_effect {
            self.fail_next_fs_effect = false;
            return;
        }
        // Write suppression keyed by the content hash, read from the effect
        // exactly as the real driver's funnel does (`write_doc`): the digest
        // travels with the bytes and was computed from them.
        let hex = content.hex();
        self.suppressions
            .insert(rel.clone(), Some(content.digest().to_vec()));
        // The write intent, journaled ahead of the bytes for text only (as
        // the funnel does): the startup scan reads an intent's bytes as
        // text, and a blob's write is last-writer-wins, never diffed in.
        if std::str::from_utf8(content.bytes()).is_ok() {
            self.append_journal_lines_model(vec![IdentityJournalLine {
                path: kutl_daemon::core::rel_path_to_string(&rel),
                kind: JournalLineKind::WriteIntent { hash: hex.clone() },
                hlc_floor: None,
            }]);
        }
        self.write_seq += 1;
        self.disk
            .insert(rel.clone(), (content.into_bytes(), self.write_seq));
        let inode = self.next_inode();
        self.disk_inodes.insert(rel.clone(), inode);
        self.state.identity_set_inode(&rel, Some(inode));
        self.state.identity_set_written_hash(&rel, hex);
        // The post-write snapshot is journaled now, not at the loop tail
        // (`SpaceWorker::journal_identity_now`), so a kill between the two
        // cannot leave a materialized file whose persisted entry has no
        // inode. A path with no identity has nothing to publish: a removal
        // is only ever journaled by the drain.
        if self.state.file_identity.contains_key(&rel) {
            let line = self.state.journal_line_for(&rel);
            self.append_journal_lines_model(vec![line]);
            self.state.journal_pending.remove(&rel);
        }
        DaemonCore::apply_effect_result(
            &mut self.state,
            kutl_daemon::core::EffectResult::FileWritten { rel },
        );
    }

    /// Snapshot a doc's full oplog as its persisted `.dt` sidecar (the real
    /// `save_doc` is an inline disk write).
    fn snapshot_doc_sidecar(&mut self, document_id: &str) {
        if let Some(doc) = self.state.get_doc(document_id) {
            self.persisted_docs.insert(
                document_id.to_owned(),
                (doc.encode_full(), doc.changes_since(&[])),
            );
        }
    }

    /// The modeled `SpaceWorker::reload_doc`: the in-memory CRDT is replaced
    /// by the persisted sidecar mirror, or dropped when none was ever taken.
    /// A mirror that no longer decodes takes the real driver's retire action
    /// (`retire_unreadable_sidecar`): the mirror is discarded, as the file is
    /// quarantined, and the document is marked as awaiting the relay's
    /// refill, so the next merge diffs the disk against real content.
    fn reload_doc_sidecar(&mut self, document_id: &str) {
        self.state.remove_doc_in_memory(document_id);
        if let Some(sidecar) = self.persisted_docs.get(document_id) {
            match load_doc_sidecar(sidecar) {
                Some(doc) => {
                    self.state.documents.insert(document_id.to_owned(), doc);
                }
                None => self.retire_doc_sidecar(document_id),
            }
        }
    }

    /// The modeled `retire_unreadable_sidecar`: the persisted mirror goes
    /// (the real file moves aside as `.corrupt`) and the id awaits content.
    fn retire_doc_sidecar(&mut self, document_id: &str) {
        self.persisted_docs.remove(document_id);
        self.state.awaiting_content.insert(document_id.to_owned());
    }

    /// Fault injection: make the persisted `.dt` mirror for `document_id`
    /// undecodable, so the next reload (a restart, or a `ReloadDoc` effect)
    /// finds a sidecar the engine cannot read and takes the retire path. A
    /// no-op for a document that was never snapshotted.
    pub fn corrupt_doc_sidecar(&mut self, document_id: &str) {
        if let Some((ops, metadata)) = self.persisted_docs.get_mut(document_id) {
            *ops = UNDECODABLE_SIDECAR.to_vec();
            metadata.clear();
        }
    }

    /// Model a daemon RESTART — process death, then `SpaceWorker::new` over the
    /// same `.kutl` dir. Every in-memory structure is dropped; the new state is
    /// rebuilt ONLY from what the daemon persists, through the REAL load and
    /// projection code:
    ///
    /// - the persisted daemon state — the `state.ksnap` snapshot with the
    ///   identity journal replayed over it (`DaemonState::load`'s fold) —
    ///   projects `file_identity`/`uuid_to_path` (`identity_maps`, keeping
    ///   the persisted inode: at a gone path nothing is readable from disk —
    ///   the offline-rename case), the register stamps (`register_hlc_map`,
    ///   the causal floor an offline rename must carry), and the HLC clock
    ///   (`restore_clock`: persisted floor + device actor). The constructor's
    ///   immediate startup save is mirrored, so the persisted snapshot is
    ///   current after a restart and the journal is truncated.
    /// - `persisted_docs` (the `.dt` snapshots): each reloads as a fresh CRDT
    ///   merging the persisted oplog (the catch-up decode path).
    ///
    /// `last_synced`, the `DiskShadow`, `known_records`, the lifecycle
    /// watermarks, `deferred`, and `exempt_revival` are all LOST — exactly as
    /// in the real restart. Daemon queues die with the process; relay-side
    /// queues die with the connection. The driver's startup machinery (initial
    /// scan, startup reconciliation, relay catch-up) is NOT run here — tests
    /// drive those halves explicitly. `blob_state` carries over (the real
    /// daemon persists it inline, not coalesced). No state-faking: anything
    /// not persisted does not survive (the idle-heal rule).
    pub fn restart(&mut self) {
        let mut persisted = self.persisted_state.clone();
        // Replay the journal mirror over the snapshot through the REAL fold
        // (`DaemonState::load` does exactly this), so a crash between
        // coalesced saves restores what the drain journaled — the repair the
        // journal exists for (idempotent — last line per path wins).
        for line in self.persisted_journal.clone() {
            persisted.apply_journal_line(line);
        }
        let hlc = persisted.restore_clock();
        // The constructor's startup save: it persists the replayed state
        // (and a freshly generated device actor) at once, and like every
        // full save it truncates the journal and resets the line budget.
        self.persisted_state = persisted.clone();
        self.persisted_journal.clear();
        self.journal_lines_since_save = 0;

        // The REAL persisted→live projection (`DaemonState::identity_maps`).
        // The model's inodes are the recorded ones; a fallback probe has
        // nothing to read.
        let (file_identity, uuid_to_path) =
            persisted.identity_maps(&self.state.space_root, |_| None);
        let register_hlc = persisted.register_hlc_map();
        // The startup scan's action for a mirror that no longer decodes is
        // the runtime one (`retire_doc_sidecar`), applied after the state
        // swap below so the awaiting mark lands in the NEW state.
        let mut documents = HashMap::new();
        let mut unreadable = Vec::new();
        for (id, sidecar) in &self.persisted_docs {
            match load_doc_sidecar(sidecar) {
                Some(doc) => {
                    documents.insert(id.clone(), doc);
                }
                None => unreadable.push(id.clone()),
            }
        }

        let old = std::mem::replace(
            &mut self.state,
            SpaceState::new_for_test(String::new(), PathBuf::new(), String::new()),
        );
        // Full literal, no `..Default::default()`, DELIBERATELY: adding a
        // field to `SpaceState` must break this build and force a decision —
        // does the field survive a restart (copy from `old`), reload from
        // persisted state, or reset? A spread would silently default it and
        // the sim's restart would quietly stop modeling the real one.
        self.state = SpaceState {
            space_id: old.space_id,
            space_root: old.space_root,
            author_did: old.author_did,
            agent_name: old.agent_name,
            last_synced: HashMap::new(),
            blob_state: old.blob_state,
            file_identity,
            uuid_to_path,
            state: persisted,
            hlc,
            clock_skew_ms: old.clock_skew_ms,
            lifecycle_hlc: HashMap::new(),
            register_hlc,
            documents,
            deferred: HashMap::new(),
            intake_backlogged: false,
            placement_dirty: false,
            placement_truncated: false,
            journal_pending: std::collections::HashSet::new(),
            awaiting_content: std::collections::HashSet::new(),
            exempt_revival: std::collections::HashMap::new(),
            pending_blob_state: std::collections::HashMap::new(),
            known_records: RegistryLattice::new(),
            shadow: kutl_daemon::core::DiskShadow::default(),
            identity_idx: kutl_daemon::core::IdentityIndexes::default(),
        };
        // Restart rebuilt the identity map wholesale; re-derive its indexes.
        self.state.rebuild_identity_indexes();
        for id in unreadable {
            self.retire_doc_sidecar(&id);
        }
        self.state_dirty = false;
        self.pending_in.clear();
        self.inbound.q.clear();
        self.loop_out.clear();
        self.sync_cmd.q.clear();
        self.relay_inbound.q.clear();
        self.relay_ctrl.q.clear();
        self.suppressions = SuppressionSet::default();
        self.last_local_create_suppressed = false;
    }

    /// Model the disk-side effect of an event the shell observes BEFORE the core
    /// runs (the watcher/shell edge the pure core never touches).
    ///
    /// Covers:
    /// - `UntrackedFileRemoved`: a local `rm` of an untracked occupant vacated
    ///   `rel`, so the shell removes the bytes and clears the
    ///   `Occupant::Untracked` mark before the core re-derives placement.
    ///   (The deferral cases seed the `Untracked` occupant directly through
    ///   `put_untracked_file` rather than through `UntrackedFileObserved`.)
    /// - `RemoteRename`: the local-rename drain twin — the model of the driver's
    ///   `drain_relocated_local_rename` (`daemon.rs`). An incoming remote rename
    ///   for a doc whose recorded path holds no live file, while its recorded
    ///   inode sits at an UNCLAIMED path, is racing a local rename the watcher
    ///   has not drained yet. The real driver drains it through
    ///   `try_local_inode_rename` BEFORE the freshness gate `recv`s the incoming
    ///   stamp; the sim runs the core's `FileRenamed` first (identical fold —
    ///   identity + shadow + lattice + emit — since the core path folds the
    ///   shadow too), at the SAME wall reading, so the drained rename carries
    ///   its honest pre-observation stamp. Without this edge the conform in
    ///   `apply_guarded_place` destroys the relocation evidence and the local
    ///   rename never reaches the relay model — the silent one-of-two-renames
    ///   drop the drain exists to prevent.
    ///
    /// Every other event is a no-op here — the core + `route_effect` model its
    /// disk effects.
    fn model_disk_edge_for(&mut self, event: &Event) {
        match event {
            Event::UntrackedFileRemoved { rel, .. } => {
                self.disk.remove(rel);
                self.state
                    .shadow
                    .shadow_occupant
                    .remove(&kutl_daemon::core::casefold(rel));
            }
            Event::FileRemoved { rel, .. } => {
                // The user's `rm` already happened on the live disk — this
                // event IS the watcher observing it. The shadow learns via the
                // core's fold in `handle_file_removed`, not here.
                self.disk.remove(rel);
                self.disk_inodes.remove(rel);
            }
            Event::RemoteRename {
                document_id, stamp, ..
            } => {
                let Some(state_path) = self.state.uuid_to_path.get(document_id).cloned() else {
                    return; // untracked: no local file to have renamed
                };
                if self.disk.contains_key(&state_path) {
                    return; // the file is where we track it: nothing pending
                }
                // The recorded-inode view the real drain reads
                // (`recorded_inode` = `file_identity[..].inode`).
                let Some(relocated) = self
                    .state
                    .file_identity
                    .get(&state_path)
                    .and_then(|id| id.inode)
                    .and_then(|inode| self.disk_path_with_inode(inode))
                else {
                    return; // no relocation evidence (deleted, or never materialized)
                };
                if relocated == state_path || self.state.file_identity.contains_key(&relocated) {
                    return; // not a relocation, or the path belongs to another doc
                }
                let drained = Event::FileRenamed {
                    old: state_path,
                    new: relocated,
                    stamp: EventStamp {
                        wall_ms: stamp.wall_ms,
                        origin_hlc: None,
                    },
                };
                let effects = DaemonCore::handle(&mut self.state, drained);
                for eff in effects {
                    self.route_effect(eff);
                }
            }
            _ => {}
        }
    }

    /// A deterministic origin [`Hlc`] for an event from logical peer `key` at
    /// virtual wall time `wall_ms` — the sim's stand-in for the daemon's
    /// `make_metadata` stamp (daemon/causal.rs), so a fed event carries the same
    /// shape of origin HLC the relay would have arbitrated over. The actor is seeded
    /// from `key`'s first byte so distinct peers get distinct, stable actors.
    #[must_use]
    pub fn hlc_for(&self, key: &str, wall_ms: u64) -> Hlc {
        let mut b = [0u8; 16];
        b[15] = key.bytes().next().unwrap_or(0);
        Hlc {
            physical_ms: wall_ms,
            logical: 0,
            actor: ActorId(Uuid::from_bytes(b)),
        }
    }

    /// The number of documents the relay model registered (alive) for which this
    /// daemon has no confirmed local entry — a doc stranded `confirmed:false`.
    #[must_use]
    pub fn unconfirmed_count(&self) -> usize {
        let confirmed = self.state.state.confirmed_ids();
        self.relay
            .records()
            .filter(|(_, r)| r.is_alive())
            .filter(|(id, _)| !confirmed.contains(&id.to_string()))
            .count()
    }

    /// The current depth of each modeled channel, in the order
    /// `(pending_in, inbound, sync_cmd, relay_inbound, relay_ctrl)`. Used by the
    /// liveness tests to assert that a non-quiescent blocking model is genuinely
    /// WEDGED (full `sync_cmd` with pending work), not merely slow.
    #[must_use]
    pub fn queue_depths(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.pending_in.len() + self.loop_out.len(),
            self.inbound.len(),
            self.sync_cmd.len(),
            self.relay_inbound.len(),
            self.relay_ctrl.len(),
        )
    }

    // ── internals ────────────────────────────────────────────────────────────

    /// The set of queues that currently hold an item AND are schedulable.
    ///
    /// The loop (`Inbound`) is the single task that produces to `sync_cmd` and
    /// consumes `inbound`. In the BLOCKING topology, while it has a half-flushed
    /// event (`loop_out` non-empty) it is mid-`.send().await`: it cannot pop a new
    /// inbound event, and it can advance ONLY by flushing `loop_out` into a
    /// `sync_cmd` slot. So `Inbound` is offered whenever there is loop work, but
    /// `service_inbound` makes progress only if `loop_out` can flush (a slot is
    /// free) or there is a new event to pop with `loop_out` empty. When `sync_cmd`
    /// is full and `loop_out` is non-empty, `Inbound` is offered but `Blocked` —
    /// that is the wedge.
    fn nonempty_queues(&self) -> Vec<QueueKind> {
        let mut v = Vec::with_capacity(4);
        let loop_has_work = !self.loop_out.is_empty() || !self.inbound.is_empty();
        if loop_has_work {
            v.push(QueueKind::Inbound);
        }
        if !self.sync_cmd.is_empty() {
            v.push(QueueKind::SyncCmd);
        }
        if !self.relay_inbound.is_empty() {
            v.push(QueueKind::RelayInbound);
        }
        if !self.relay_ctrl.is_empty() {
            v.push(QueueKind::RelayCtrl);
        }
        v
    }

    fn service(&mut self, kind: QueueKind) -> StepOutcome {
        match kind {
            QueueKind::Inbound => self.service_inbound(),
            QueueKind::SyncCmd => self.service_sync_cmd(),
            QueueKind::RelayInbound => self.service_relay_inbound(),
            QueueKind::RelayCtrl => self.service_relay_ctrl(),
        }
    }

    /// Advance the loop: first finish flushing the current event's staged outbound
    /// commands; only when `loop_out` is empty pop and process the next inbound
    /// event. This models the single task that is mid-`.send().await` between
    /// events.
    fn service_inbound(&mut self) -> StepOutcome {
        // 1. Finish flushing a half-sent event's commands (the in-flight send).
        if !self.loop_out.is_empty() {
            return self.flush_loop_out();
        }
        // 2. No in-flight send: pop the next event, run the pure core, stage its
        //    outbound commands, apply its disk effects, then flush.
        let Some(event) = self.inbound.try_recv() else {
            return StepOutcome::Idle;
        };
        self.dispatch_one(event);
        self.flush_loop_out()
    }

    /// Flush staged outbound commands into `sync_cmd`.
    ///
    /// SHIPPED (unbounded): every command lands; the loop is never wedged, so this
    /// always drains `loop_out` to empty and the next inbound event can be popped.
    /// BLOCKING: `sync_cmd` is the bounded cap-64 channel; when it fills, the
    /// remaining staged commands STAY in `loop_out` and the loop reports `Blocked`
    /// — it cannot pop the next event until a `sync_cmd` slot frees. If nothing
    /// ever frees that slot (the ack rail is itself wedged), this is the fixpoint.
    fn flush_loop_out(&mut self) -> StepOutcome {
        let mut flushed_any = false;
        while let Some(cmd) = self.loop_out.front().cloned() {
            if self.blocking {
                if self.sync_cmd.try_send(cmd).is_err() {
                    // The bounded outbound is full: the loop blocks here, holding
                    // the rest of this event's commands in `loop_out`.
                    return if flushed_any {
                        StepOutcome::Progressed
                    } else {
                        StepOutcome::Blocked
                    };
                }
            } else {
                // Shipped: outbound is unbounded — push past the modeled cap so a
                // full queue never stalls the loop (the cycle-closing change).
                self.sync_cmd.q.push_back(cmd);
            }
            self.loop_out.pop_front();
            flushed_any = true;
        }
        StepOutcome::Progressed
    }

    /// Stage one effect: outbound relay commands go to `loop_out` (flushed to
    /// `sync_cmd`); disk effects apply immediately to the in-memory disk.
    fn route_effect(&mut self, effect: Effect) {
        match effect {
            Effect::RegisterDocument {
                document_id,
                path,
                metadata,
                ..
            } => self.loop_out.push_back(OutCommand::Register {
                document_id,
                path,
                hlc: hlc_of(metadata.as_ref()),
            }),
            Effect::RenameDocument {
                document_id,
                new_path,
                metadata,
                ..
            } => self.loop_out.push_back(OutCommand::Rename {
                document_id,
                new_path,
                hlc: hlc_of(metadata.as_ref()),
            }),
            Effect::UnregisterDocument {
                document_id,
                metadata,
                ..
            } => self.loop_out.push_back(OutCommand::Unregister {
                document_id,
                hlc: hlc_of(metadata.as_ref()),
            }),
            Effect::Subscribe { document_id } => self
                .loop_out
                .push_back(OutCommand::Subscribe { document_id }),
            // SaveBlobState: the blob LWW map persists inline in the real
            // driver; the sim's restart carries `blob_state` live (documented
            // on `restart`), so it only occupies the Other slot here.
            Effect::SendOps { .. } | Effect::SaveBlobState | Effect::EmitMetrics => {
                self.loop_out.push_back(OutCommand::Other);
            }

            // Persistence effects, modeled with the driver's exact semantics:
            // SaveState only marks dirty (the write is the COALESCED flush,
            // `flush_state_if_caught_up`); SaveDoc writes the sidecar mirror
            // inline (the real `save_doc` is an inline disk write). Each still
            // occupies an Other slot so queue scheduling is unchanged.
            Effect::SaveState => {
                self.state_dirty = true;
                self.loop_out.push_back(OutCommand::Other);
            }

            Effect::SaveDoc { document_id } => {
                self.snapshot_doc_sidecar(&document_id);
                self.loop_out.push_back(OutCommand::Other);
            }
            Effect::ReloadDoc { document_id } => {
                self.reload_doc_sidecar(&document_id);
                self.loop_out.push_back(OutCommand::Other);
            }
            Effect::WriteFile { rel, content } => self.apply_write_file(rel, content),
            Effect::RemoveFile { rel } => {
                // Register the removal echo-suppression the funnel sends
                // (`remove_doc`, daemon/effects.rs): `(rel, None)`.
                self.suppressions.insert(rel.clone(), None);
                self.disk.remove(&rel);
                self.disk_inodes.remove(&rel);
                // Fold the landed removal into the shadow (the shell ACK),
                // vacating the occupant — else a delete leaves a stale
                // `Tracked` entry that makes the next register's stat mistake a
                // foreign recreate for the tracked incumbent, merging a peer's
                // concurrent recreate into the deleted doc. Mirrors the driver
                // (`apply_remove` → `FileRemoved`).
                DaemonCore::apply_effect_result(
                    &mut self.state,
                    kutl_daemon::core::EffectResult::FileRemoved { rel },
                );
            }

            // The gamma cascade's atomic shell-guarded place.
            Effect::GuardedPlace {
                id,
                target,
                expected_free,
                place_kind,
            } => self.apply_guarded_place(&id, &target, expected_free, &place_kind),
        }
    }

    /// Apply a [`Effect::GuardedPlace`] (the atomic stat-and-place, modeled as
    /// ONE non-suspending step). The shell stats `target` for an untracked
    /// occupant and places without any event interleaving in between — mirroring
    /// the real driver's `apply_guarded_place`, which runs the stat and the place
    /// adjacently with nothing scheduled between them. The whole method runs to completion
    /// before any other queue is serviced, so the stat and the place (or defer) are
    /// atomic — a file appearing in the window IS seen by the stat.
    ///
    /// THE CRITICAL SECTION, in order, with no suspension:
    /// 1. Apply any armed TOCTOU-window write (`armed_window_writes`) for `target`
    ///    — the model of a local create that lands on disk AFTER the core decided
    ///    (it believed the path free, `expected_free == true`) but BEFORE this stat.
    /// 2. The atomic stat: `target` holds an UNTRACKED file iff the disk has bytes
    ///    at `target` that no tracked id owns (no `Occupant::Tracked` in the shadow).
    /// 3. Decide:
    ///    - DISAGREEMENT (`expected_free == true` but the stat found an untracked
    ///      occupant): the TOCTOU race (FS-1). Do NOT place — inject
    ///      `UntrackedFileObserved` and re-run `handle` so the core marks the path
    ///      `Occupant::Untracked` and the doc DEFERS (the non-revival deferral predicate).
    ///      The local bytes on disk are untouched — no clobber.
    ///    - AGREEMENT: place. `expected_free == true` with a free path is a normal
    ///      place; `expected_free == false` with an untracked occupant is a revival
    ///      ADOPTING its own orphan — the core knows the occupant is there
    ///      and claims it.
    ///
    /// A `Register` "tracks + subscribes": it records the id at `target` in the
    /// shadow (so the next `reconcile_placement` sees the doc placed and re-emits
    /// nothing — the recompute is idempotent at the fixpoint) and enqueues the
    /// `Subscribe` whose content stream the real shell starts on the place ACK
    /// (`place_now`'s Register arm). A register places NO bytes — the disk stays empty until
    /// content streams in via `RemoteOps`. A `Rename` moves the held file: it shifts
    /// the bytes (if any) and the shadow occupant from `old_rel` onto `target`.
    fn apply_guarded_place(
        &mut self,
        id: &str,
        target: &std::path::Path,
        expected_free: bool,
        place_kind: &kutl_daemon::core::PlaceKind,
    ) {
        let Ok(uid) = Uuid::parse_str(id) else {
            return;
        };

        // (1) An armed in-window write lands on disk NOW — inside the critical
        // section, before the stat. This is the local create that slipped in
        // between the core's free-path decision and the shell's place.
        if let Some(content) = self.armed_window_writes.remove(&target.to_path_buf()) {
            self.write_seq += 1;
            self.disk
                .insert(target.to_path_buf(), (content, self.write_seq));
        }

        // (2) The atomic stat against LIVE disk (not the shadow): the target holds
        // an untracked file iff bytes sit there that no tracked id owns. Reading
        // disk (not the shadow occupant) is the point — the racing create is on
        // disk but the core's shadow never saw it (that is the TOCTOU window).
        // PRECEDENCE mirrors the real `stat_untracked` (daemon/effects.rs) exactly:
        // a path `file_identity` claims — a local create's own mint, or a doc this
        // daemon placed — is NOT a foreign untracked occupant, even when the
        // shadow occupant is a stale `Untracked` marker from an earlier
        // disagreement that raced the mint.
        let occupied_by_untracked = self.disk.contains_key(target)
            && !self.state.file_identity.contains_key(target)
            && self.tracked_at(target).is_none();

        // (3a) DISAGREEMENT: the core believed the path free but the stat found an
        // untracked occupant — the FS-1 race. Do NOT place; inject
        // `UntrackedFileObserved` and re-run `handle` IN THIS SAME STEP (no queue,
        // no interleaving) so the core marks the occupant and defers. The disk bytes
        // stay put — the local content is not clobbered.
        if occupied_by_untracked && expected_free {
            let ev = Event::UntrackedFileObserved {
                rel: target.to_path_buf(),
                stamp: self.now_stamp(),
            };
            let effects = DaemonCore::handle(&mut self.state, ev);
            for effect in effects {
                self.route_effect(effect);
            }
            return;
        }

        // (3a') The target is held by a DIFFERENT tracked document on live
        // disk: do NOT place over it. The shell twin of `place_now`'s
        // occupied-target guard: skipping is self-correcting
        // — the occupant's own move (ordered before this mover, or re-derived
        // by a later reconcile) vacates the path first. Normally unreachable
        // (the `(path_hlc, id)` order vacates losers first); reached when that
        // vacate FAILED (the ENOSPC model), where placing anyway would adopt
        // the loser's path while its bytes still sit there.
        if self.disk.contains_key(target)
            && self.tracked_at(target).is_some_and(|other| other != uid)
        {
            return;
        }

        // (3b) AGREEMENT: place.
        //
        // One-shot failure model: an armed failure aborts the whole
        // place — no disk mutation and no shadow update — modelling the funnel's
        // log-and-swallow (`apply_write`/`apply_rename` returning `None`). The
        // shadow is left unchanged so a
        // LATER reconcile re-derives and retries; nothing is scheduled here,
        // because the real driver schedules nothing (the retry driver is the
        // next event/tick that runs `DaemonCore::handle`). The flag fires only
        // on the AGREEMENT path; a DISAGREEMENT-induced defer is not a disk
        // effect (it has no bytes to fail) so it is not intercepted.
        if self.fail_next_fs_effect {
            self.fail_next_fs_effect = false;
            return;
        }
        // A Rename moves the held file's bytes first.
        if let kutl_daemon::core::PlaceKind::Rename { old_rel } = place_kind {
            // Resolve the held file's REAL location on live disk. Normally it sits
            // at `old_rel` (the shadow's recorded path), but a concurrent out-of-band
            // user rename may have relocated it — its watcher event hasn't drained,
            // so `shadow_path` is stale and `old_rel` is empty on disk. When that
            // happens, CONFORM by the doc's recorded inode (the sim twin of
            // `place_now`'s `conform_relocated_or_materialize`, `daemon/effects.rs`):
            // find the live file carrying the inode `file_identity` recorded for
            // `old_rel` (the recorded view the real conform reads) and move THAT
            // onto the target. Without this the blind move from `old_rel`
            // finds nothing and the file is stranded as an orphan.
            let source = if self.disk.contains_key(old_rel) {
                old_rel.clone()
            } else {
                self.state
                    .file_identity
                    .get(old_rel)
                    .and_then(|fi| fi.inode)
                    .and_then(|inode| self.disk_path_with_inode(inode))
                    .unwrap_or_else(|| old_rel.clone())
            };
            // Register the rename's pair-matched echo suppression as the real
            // `rename_doc` does (daemon/effects.rs), the write half carrying
            // sha256 of the moved bytes (the file's content, read at the
            // resolved source, becomes the new path's content).
            let renamed_hash = self
                .disk
                .get(&source)
                .map(|(content, _)| kutl_daemon::sha256_bytes(content));
            self.push_rename_suppression_pair(&source, target, renamed_hash);
            // Move the held file's bytes from the resolved source onto the target.
            self.write_seq += 1;
            if let Some((content, _)) = self.disk.remove(&source) {
                self.disk
                    .insert(target.to_path_buf(), (content, self.write_seq));
            }
            // Move the disk inode tracking: the inode follows the file to target.
            if let Some(inode) = self.disk_inodes.remove(&source) {
                self.disk_inodes.insert(target.to_path_buf(), inode);
            }
            // Vacate the source occupant AND, if we conformed from a relocated path,
            // the stale `old_rel` shadow records the move left behind.
            self.state
                .shadow
                .shadow_occupant
                .remove(&kutl_daemon::core::casefold(&source));
            if source.as_path() != old_rel.as_path() {
                self.state
                    .shadow
                    .shadow_occupant
                    .remove(&kutl_daemon::core::casefold(old_rel));
            }
        }
        // Track the id at the target in the shadow so the next reconcile sees it
        // placed (Register and Rename both end here). A Register additionally
        // subscribes so the doc's content streams in.
        // The placed file's inode is now at `target`; the identity claim below
        // records it so the core can locate the file later by inode.
        let inode = self.disk_inodes.get(target).copied();
        self.state.shadow.set_tracked(target, uid);
        self.claim_identity_at_place(id, target, place_kind, inode);
        if matches!(place_kind, kutl_daemon::core::PlaceKind::Register) {
            // Models the shell's place-ACK Subscribe (`place_now`'s Register
            // arm) — the ONLY site that subscribes a placed doc, so it
            // must reach the relay model's `subscriptions` rail.
            self.loop_out.push_back(OutCommand::Subscribe {
                document_id: id.to_owned(),
            });
        }
    }

    /// Claim identity at place time, mirroring the real driver: `place_now`'s
    /// Register arm claims `file_identity`/`uuid_to_path` for the placed doc and
    /// its Rename arm re-keys them via `move_identity` (daemon/identity.rs).
    /// Identity-reading driver glue — the relocated-rename drain, the
    /// inode-rename detector — consults these maps, so the model must hold
    /// what the driver holds. The placed file's inode is recorded into
    /// `file_identity` under the driver's own rules: a fresh registration
    /// records the live inode (`register_identity`); a move records the live
    /// inode at the destination, falling back to the inode carried from the
    /// source when the destination has none (`move_identity`, the inode that
    /// keeps a relocated file matchable as its document); a Register onto a
    /// doc already in `uuid_to_path` touches no inode at all.
    fn claim_identity_at_place(
        &mut self,
        id: &str,
        target: &std::path::Path,
        place_kind: &kutl_daemon::core::PlaceKind,
        inode: Option<u64>,
    ) {
        match place_kind {
            kutl_daemon::core::PlaceKind::Rename { old_rel } => {
                // Mirror the real driver's `place_now`: the move vacates the
                // LATTICE-recorded source (`old_rel`), not wherever
                // `uuid_to_path` currently points. The two diverge in the
                // eager-move/occupied-target interleavings, and it is the
                // lattice path whose per-path state (the `last_synced`
                // frontier above all) must follow the document — resolving by
                // id here would leave the sim blind to exactly the
                // stranded-frontier class the driver can hit.
                kutl_daemon::core::handle::move_identity(
                    &mut self.state,
                    old_rel,
                    target.to_path_buf(),
                    id,
                );
                let carried = self.state.file_identity.get(target).and_then(|fi| fi.inode);
                self.state.identity_set_inode(target, inode.or(carried));
            }
            kutl_daemon::core::PlaceKind::Register => {
                if !self.state.uuid_to_path.contains_key(id) {
                    kutl_daemon::core::handle::register_identity(
                        &mut self.state,
                        target,
                        id.to_owned(),
                        /* confirmed */ true,
                    );
                    self.state.identity_set_inode(target, inode);
                }
            }
        }
    }

    /// Drain one outbound command into the relay model — the WS writer task.
    /// Moves a `sync_cmd` command to the relay's inbound queue.
    fn service_sync_cmd(&mut self) -> StepOutcome {
        if self.blocking && self.relay_inbound.is_full() {
            // The relay's bounded inbound is full: the writer blocks here.
            return StepOutcome::Blocked;
        }
        let Some(cmd) = self.sync_cmd.try_recv() else {
            return StepOutcome::Idle;
        };
        // Shipped: relay inbound modeled unbounded (the relay never back-pressured
        // the daemon in the repro — zero evictions, all 100 commands accepted).
        self.relay_inbound.q.push_back(cmd);
        StepOutcome::Progressed
    }

    /// The relay accepts one inbound command: fold it into the lattice and, for a
    /// lifecycle command, enqueue the daemon's own ack onto the control rail.
    ///
    /// In the BLOCKING model the ack rail (`relay_ctrl`, cap 16) is bounded: if it
    /// is full the relay cannot place the ack, so the command is re-queued and the
    /// relay stalls (the relay-writer block of cycle 1). In the SHIPPED model the
    /// ack rail is unbounded (Fix A: never drop/back-pressure the response path).
    fn service_relay_inbound(&mut self) -> StepOutcome {
        // Peek first: a lifecycle command must be re-queued whole if its ack can't
        // be placed on a full bounded ctrl rail (blocking model).
        if self.blocking && self.ack_rail_full_for_next() {
            return StepOutcome::Blocked;
        }
        let Some(cmd) = self.relay_inbound.try_recv() else {
            return StepOutcome::Idle;
        };
        match cmd {
            OutCommand::Register {
                document_id,
                path,
                hlc,
            } => self.relay_apply_register(&document_id, &path, hlc),
            OutCommand::Rename {
                document_id,
                new_path,
                hlc,
            } => self.relay_apply_rename(&document_id, &new_path, hlc),
            OutCommand::Unregister { document_id, hlc } => {
                self.relay_apply_unregister(&document_id, hlc);
            }
            OutCommand::Subscribe { document_id } => {
                self.subscriptions.push(document_id);
            }
            OutCommand::Other => {}
        }
        StepOutcome::Progressed
    }

    /// Whether the next `relay_inbound` command would produce an ack the bounded
    /// `relay_ctrl` rail cannot currently hold (blocking model only). A register
    /// or rename produces an ack; an unregister / other does not.
    fn ack_rail_full_for_next(&self) -> bool {
        let produces_ack = matches!(
            self.relay_inbound.q.front(),
            Some(OutCommand::Register { .. } | OutCommand::Rename { .. })
        );
        produces_ack && self.relay_ctrl.is_full()
    }

    /// Deliver one relay control message (a lifecycle ack) into the daemon's
    /// inbound queue. Models the bounded cap-16 ack rail (`MODELED_CTRL_CAP`) feeding the loop.
    ///
    /// In the BLOCKING model `inbound` is bounded (cap 64): a full `inbound` blocks
    /// the ack delivery, so the ack stays on `relay_ctrl` — this is the cycle-1
    /// circular wait (the loop can't drain `inbound` because it is wedged pushing
    /// to a full `sync_cmd`, and the acks that would free `sync_cmd` can't enter a
    /// full `inbound`). In the SHIPPED model acks ride the unbounded response path
    /// (Fix A), so they always land and the loop always confirms.
    fn service_relay_ctrl(&mut self) -> StepOutcome {
        if self.blocking && self.inbound.is_full() {
            // The bounded inbound cannot accept the ack: blocked (cycle 1).
            return StepOutcome::Blocked;
        }
        let Some(ack) = self.relay_ctrl.try_recv() else {
            return StepOutcome::Idle;
        };
        // The ack rejoins the daemon's inbound queue so the loop confirms the doc.
        self.inbound.q.push_back(ack);
        StepOutcome::Progressed
    }

    /// Relay-side register: observe a `DocRecord`, then ack the registrant.
    fn relay_apply_register(&mut self, document_id: &str, path: &str, hlc: Option<Hlc>) {
        let Ok(id) = Uuid::parse_str(document_id) else {
            return;
        };
        let stamp = hlc.unwrap_or_else(|| self.next_relay_hlc());
        self.relay
            .observe(DocRecord::register(id, path, Some(stamp)));
        self.enqueue_ack(document_id, &id);
    }

    /// Relay-side rename: observe the rename, then ack the renamer.
    fn relay_apply_rename(&mut self, document_id: &str, new_path: &str, hlc: Option<Hlc>) {
        self.renames_relayed
            .push((document_id.to_owned(), new_path.to_owned()));
        let Ok(id) = Uuid::parse_str(document_id) else {
            return;
        };
        let stamp = hlc.unwrap_or_else(|| self.next_relay_hlc());
        // Floor is None: the sim does not yet model a clock-skewed register the
        // rename observes; a faithful skew-floor model is the flagged kutl-sim
        // driver-glue extension, tracked separately.
        self.relay
            .observe(DocRecord::rename(id, new_path, Some(stamp), None));
        self.enqueue_ack(document_id, &id);
    }

    /// Relay-side unregister: observe the tombstone. The relay does not ack a
    /// delete on the typed-ack rail (`handle_lifecycle_ack` only fires for
    /// register/rename), so no ack is enqueued.
    fn relay_apply_unregister(&mut self, document_id: &str, hlc: Option<Hlc>) {
        let Ok(id) = Uuid::parse_str(document_id) else {
            return;
        };
        let stamp = hlc.unwrap_or_else(|| self.next_relay_hlc());
        // A delete must carry the current path forward so the tombstone keeps its
        // group membership; read the path the relay already holds.
        let path = self
            .relay
            .get(&id)
            .map_or_else(String::new, |r| r.path.clone());
        self.relay.observe(DocRecord::delete(id, path, Some(stamp)));
    }

    /// Enqueue the daemon's own [`Event::LifecycleAck`] for a register/rename it
    /// just made — carrying the relay's ARBITRATED effective path (the conflict
    /// path when this doc lost a collision), exactly as the typed-ack rail does.
    fn enqueue_ack(&mut self, document_id: &str, id: &Uuid) {
        let effective_path = self
            .relay
            .get(id)
            .map(kutl_core::lattice::DocRecord::effective_path);
        let ack = Event::LifecycleAck {
            document_id: document_id.to_owned(),
            effective_path,
            stamp: EventStamp {
                wall_ms: self.now_ms(),
                origin_hlc: None,
            },
        };
        if self.blocking {
            // The caller (`service_relay_inbound`) only consumed the command after
            // `ack_rail_full_for_next` confirmed the bounded cap-16 ctrl rail has
            // room, so this send cannot fail.
            self.relay_ctrl
                .try_send(ack)
                .expect("ack rail room was checked before consuming the command");
        } else {
            // Shipped: the ack response path is unbounded (Fix A) — never dropped,
            // never back-pressured (a lost ack = a doc stuck confirmed:false).
            self.relay_ctrl.q.push_back(ack);
        }
    }

    /// Allocate a fresh, monotonically-increasing inode number.
    ///
    /// Inode 0 is never assigned (`Option<u64>` uses `None` for "no inode").
    fn next_inode(&mut self) -> u64 {
        let inode = self.inode_counter;
        self.inode_counter += 1;
        inode
    }

    /// The current virtual time in millis (never the wall clock).
    fn now_ms(&self) -> u64 {
        u64::try_from(self.clock.now()).unwrap_or(0)
    }

    /// An `EventStamp` at the current virtual time with no origin HLC — the shape
    /// of a shell-injected feedback event (`UntrackedFileObserved`), which carries
    /// no peer HLC.
    fn now_stamp(&self) -> EventStamp {
        EventStamp {
            wall_ms: self.now_ms(),
            origin_hlc: None,
        }
    }

    /// The document id (if any) that TRACKS the disk path `target` — i.e. an
    /// `Occupant::Tracked` holds the case-folded path in the shadow. A disk path
    /// with no `Tracked` occupant is an UNTRACKED file (the FS-1 race occupant the
    /// atomic stat in [`Self::apply_guarded_place`] catches).
    fn tracked_at(&self, target: &std::path::Path) -> Option<Uuid> {
        match self
            .state
            .shadow
            .shadow_occupant
            .get(&kutl_daemon::core::casefold(target))
        {
            Some(kutl_daemon::core::Occupant::Tracked(uid)) => Some(*uid),
            _ => None,
        }
    }

    /// Consume a write echo for a local create at `path` through the real
    /// `SuppressionSet::consume_write_echo`, so the sim's match IS the
    /// watcher's.
    fn consume_write_echo(&mut self, path: &std::path::Path, content: &[u8]) -> bool {
        self.suppressions.consume_write_echo(path, content)
    }

    /// A fresh, strictly-increasing relay-side HLC for an effect that carried no
    /// origin HLC (the sim feeds `origin_hlc: None`). Uses virtual time so the
    /// stamps stay deterministic and monotonic.
    fn next_relay_hlc(&mut self) -> Hlc {
        let physical = self.now_ms();
        // Advance virtual time so successive relay stamps differ.
        self.clock.tick();
        Hlc {
            physical_ms: physical,
            logical: 0,
            actor: self.relay_actor,
        }
    }
}

/// Which modeled queue a step services.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueKind {
    Inbound,
    SyncCmd,
    RelayInbound,
    RelayCtrl,
}

/// Extract the HLC from a lifecycle effect's metadata, if present and valid.
fn hlc_of(metadata: Option<&ChangeMetadata>) -> Option<Hlc> {
    metadata
        .and_then(|m| m.hlc.clone())
        .and_then(|wire| Hlc::try_from(wire).ok())
}

/// A fresh CRDT carrying exactly a persisted sidecar mirror, or `None` when
/// the mirror no longer decodes.
/// Bytes no engine decodes: what `corrupt_doc_sidecar` leaves in a mirror.
const UNDECODABLE_SIDECAR: [u8; 16] = [0xff; 16];

fn load_doc_sidecar(
    (ops, metadata): &(Vec<u8>, Vec<ChangeMetadata>),
) -> Option<kutl_core::Document> {
    let mut doc = kutl_core::Document::new();
    doc.merge(ops, metadata).ok().map(|()| doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_daemon::core::{Event, EventStamp};
    use std::path::PathBuf;

    /// `restart()` reloads ONLY what the daemon persists: identity (with
    /// inode), the register stamps, the HLC floor, and the `.dt` CRDT oplogs —
    /// while everything in-memory (shadow, lattice, watermarks, `last_synced`)
    /// is lost, exactly as `SpaceWorker::new` rebuilds. The mint here flushes
    /// because the intake is drained (the coalesced-persist happy path).
    #[test]
    fn test_restart_reloads_only_persisted_state() {
        let mut sim = DaemonSim::new(0xD0_5EED);
        sim.feed(Event::FileModified {
            rel: PathBuf::from("a.md"),
            content: Some(b"hello".to_vec()),
            stamp: EventStamp {
                wall_ms: 10,
                origin_hlc: None,
            },
        });
        sim.run_one_handle();

        let id = sim
            .state
            .file_identity
            .get(&PathBuf::from("a.md"))
            .map(|fi| fi.document_uuid.clone())
            .expect("mint claimed identity");
        assert!(
            sim.state
                .known_records
                .get(&Uuid::parse_str(&id).unwrap())
                .is_some(),
            "precondition: the lattice holds the mint's record"
        );

        sim.restart();

        assert_eq!(
            sim.state
                .file_identity
                .get(&PathBuf::from("a.md"))
                .map(|fi| fi.document_uuid.clone()),
            Some(id.clone()),
            "persisted identity survives the restart"
        );
        assert!(
            sim.state.register_hlc.contains_key(&id),
            "the persisted register stamp (the A1 causal floor) survives"
        );
        assert_eq!(
            sim.state.get_doc(&id).map(kutl_core::Document::content),
            Some("hello".to_owned()),
            "the .dt sidecar snapshot reloads the CRDT oplog"
        );
        assert!(
            sim.state
                .known_records
                .get(&Uuid::parse_str(&id).unwrap())
                .is_none(),
            "the placement lattice is in-memory and must NOT survive"
        );
        assert!(
            sim.state.shadow.shadow_occupant.is_empty(),
            "the DiskShadow is in-memory and must NOT survive"
        );
        assert!(
            sim.state.lifecycle_hlc.is_empty() && sim.state.last_synced.is_empty(),
            "watermarks and last_synced are in-memory and must NOT survive"
        );
    }

    /// The coalesced-persist CRASH WINDOW and its journal repair, both
    /// visible in the sim: while the intake is non-empty the `SaveState`
    /// stays a dirty flag (the save-coalescing rule), so the SNAPSHOT is
    /// stale at crash time — but the loop tail journaled the minted identity
    /// (O(1) per event), and the restart replays the journal over the stale
    /// snapshot. The invariant pinned: an identity the daemon acted on
    /// survives a crash inside the window, so the restart cannot re-register
    /// its own file into a conflict copy.
    #[test]
    fn test_restart_inside_coalesced_window_replays_journal() {
        let mut sim = DaemonSim::new(0xD0_5EED);
        sim.feed(Event::FileModified {
            rel: PathBuf::from("a.md"),
            content: Some(b"hello".to_vec()),
            stamp: EventStamp {
                wall_ms: 10,
                origin_hlc: None,
            },
        });
        // A second parked event keeps the intake NON-EMPTY at flush time, so
        // the coalesced save cannot fire after the first event is serviced.
        sim.feed(Event::FileModified {
            rel: PathBuf::from("b.md"),
            content: Some(b"later".to_vec()),
            stamp: EventStamp {
                wall_ms: 11,
                origin_hlc: None,
            },
        });
        sim.run_one_handle();
        assert!(
            sim.state.file_identity.contains_key(&PathBuf::from("a.md")),
            "precondition: the mint claimed identity in memory"
        );
        assert!(
            !sim.persisted_state.documents.contains_key("a.md"),
            "the crash window is real: the coalesced SNAPSHOT is stale"
        );
        assert!(
            sim.persisted_journal.iter().any(|l| l.path == "a.md"),
            "the loop tail journaled the mint before any snapshot fired"
        );

        sim.restart();

        assert!(
            sim.state.file_identity.contains_key(&PathBuf::from("a.md")),
            "the restart replays the journal over the stale snapshot — the \
             minted identity survives the coalesced window"
        );
        assert!(
            sim.persisted_state.documents.contains_key("a.md") && sim.persisted_journal.is_empty(),
            "the constructor's startup save persists the replayed state and truncates the journal"
        );
    }

    /// The write funnel's journal records, modeled step for step: a placed
    /// write journals its intent (the hash about to land) ahead of the bytes,
    /// then the path's post-write identity — the fresh inode and the landed
    /// hash — the moment the bytes land, not at the loop tail, so a restart
    /// inside the coalesced window finds the identity even though no drain
    /// or snapshot ran after the write.
    #[test]
    fn test_write_file_journals_identity_immediately() {
        let mut sim = DaemonSim::new(0x11D);
        let rel = PathBuf::from("w.md");
        sim.register_and_place("99999999-9999-9999-9999-999999999999", "w.md", b"w");
        let seeded_inode = sim.disk_inodes[&rel];
        // A stale snapshot and an empty journal: the only durable record of
        // the write below must come from the funnel itself.
        sim.persisted_state.documents.remove("w.md");
        sim.persisted_journal.clear();
        sim.journal_lines_since_save = 0;
        sim.state.journal_pending.insert(rel.clone());

        let content = HashedContent::new(b"w2".to_vec());
        let hex = content.hex();
        sim.apply_write_file(rel.clone(), content);

        let lines: Vec<&IdentityJournalLine> = sim
            .persisted_journal
            .iter()
            .filter(|l| l.path == "w.md")
            .collect();
        assert_eq!(
            lines.len(),
            2,
            "the funnel's two records: the intent, then the snapshot"
        );
        assert_eq!(
            lines[0].pending_write(),
            Some(hex.as_str()),
            "the intent names the hash about to land"
        );
        let entry = lines[1]
            .entry()
            .expect("the post-write line is a present entry, never a removal");
        let inode = entry
            .inode
            .expect("the snapshot carries the post-write inode");
        assert_ne!(
            inode, seeded_inode,
            "the atomic replace gives the file a fresh inode"
        );
        assert_eq!(
            entry.last_written_hash.as_deref(),
            Some(hex.as_str()),
            "the snapshot carries the landed hash"
        );
        assert_eq!(
            sim.journal_lines_since_save, 2,
            "a materialization charges two records to the force-save budget"
        );
        assert!(
            !sim.state.journal_pending.contains(&rel),
            "the immediate line retires the pending mark (the drain would only repeat it)"
        );

        sim.restart();
        let replayed = sim
            .state
            .file_identity
            .get(&rel)
            .expect("the restart replays the funnel's line over the stale snapshot");
        assert_eq!(
            (replayed.inode, replayed.last_written_hash.as_deref()),
            (Some(inode), Some(hex.as_str())),
            "the replayed identity carries the inode and hash the funnel recorded"
        );
    }

    /// A remote document placed by `GuardedPlace(Register)` before its file
    /// exists claims identity with no inode and no hash; the modeled write
    /// then records both exactly as the funnel does, so the identity the
    /// journal carries — and a restart replays — is never the placeholder
    /// the place-time claim left.
    #[test]
    fn test_apply_write_file_records_inode_and_hash_after_remote_place() {
        let mut sim = DaemonSim::new(0x1D0);
        let rel = PathBuf::from("r.md");
        let hlc = sim.hlc_for("r", sim.now_ms());
        sim.feed(Event::RemoteRegister {
            document_id: "55555555-5555-5555-5555-555555555555".into(),
            path: "r.md".into(),
            stamp: EventStamp {
                wall_ms: sim.now_ms(),
                origin_hlc: Some(hlc),
            },
        });
        sim.drain_to_quiescence_or_fail(SEED_DRAIN_STEP_BUDGET);
        let placed = sim
            .state
            .file_identity
            .get(&rel)
            .expect("precondition: the place claimed identity");
        assert_eq!(
            (placed.inode, placed.last_written_hash.as_deref()),
            (None, None),
            "precondition: the place-time claim saw no file"
        );
        sim.persisted_journal.clear();
        sim.journal_lines_since_save = 0;

        let content = HashedContent::new(b"remote".to_vec());
        let hex = content.hex();
        sim.apply_write_file(rel.clone(), content);

        let written = sim
            .state
            .file_identity
            .get(&rel)
            .expect("the write keeps the identity");
        let inode = written
            .inode
            .expect("the write recorded the post-write inode");
        assert_eq!(
            sim.disk_inodes.get(&rel),
            Some(&inode),
            "the recorded inode is the one the file carries on disk"
        );
        assert_eq!(
            written.last_written_hash.as_deref(),
            Some(hex.as_str()),
            "the write recorded the landed hash"
        );
        let snapshot = sim
            .persisted_journal
            .iter()
            .rev()
            .find_map(|l| (l.path == "r.md").then(|| l.entry()).flatten())
            .expect("the write journaled the post-write snapshot");
        assert_eq!(
            (snapshot.inode, snapshot.last_written_hash.as_deref()),
            (Some(inode), Some(hex.as_str())),
            "the journaled snapshot is the updated identity, not the placeholder"
        );

        sim.restart();
        let replayed = sim
            .state
            .file_identity
            .get(&rel)
            .expect("the restart replays the identity");
        assert_eq!(
            (replayed.inode, replayed.last_written_hash.as_deref()),
            (Some(inode), Some(hex.as_str())),
            "the restart carries the inode and hash the write recorded"
        );
    }

    /// The funnel's two records are each conditional the way the real ones
    /// are: the intent is journaled for text bytes only (a blob's write is
    /// last-writer-wins, never read back as an intent), and the snapshot only
    /// for a path with an identity (a removal is the drain's to journal,
    /// never the write's).
    #[test]
    fn test_apply_write_file_intent_is_text_only_and_snapshot_needs_identity() {
        let mut sim = DaemonSim::new(0x1D1);
        let kinds_at = |sim: &DaemonSim, path: &str| -> Vec<(bool, bool)> {
            sim.persisted_journal
                .iter()
                .filter(|l| l.path == path)
                .map(|l| (l.pending_write().is_some(), l.is_removal()))
                .collect()
        };

        // A tracked blob: the snapshot lands, and no intent.
        sim.register_and_place("66666666-6666-6666-6666-666666666666", "img.png", b"seed");
        sim.persisted_journal.clear();
        sim.apply_write_file(
            PathBuf::from("img.png"),
            HashedContent::new(vec![0xFF, 0xFE, 0x00]),
        );
        assert_eq!(
            kinds_at(&sim, "img.png"),
            vec![(false, false)],
            "a blob write journals its snapshot and no intent"
        );

        // An untracked text path: the intent lands, and no removal line. (The
        // file it leaves is an orphan no record claims, so it comes last:
        // the seeding above drains to an orphan-free quiescence.)
        sim.apply_write_file(
            PathBuf::from("loose.md"),
            HashedContent::new(b"text".to_vec()),
        );
        assert_eq!(
            kinds_at(&sim, "loose.md"),
            vec![(true, false)],
            "an untracked text write journals its intent and nothing else"
        );
    }

    /// The modeled journal drain writes a reproducible mirror — present
    /// entries first, removals last, each group in path order — and applies
    /// the driver's growth bound: past `JOURNAL_LINES_FORCE_SAVE` lines since
    /// the last save it marks state dirty so the next flush compacts.
    #[test]
    fn test_drain_identity_journal_model_orders_lines_and_forces_save() {
        let mut sim = DaemonSim::new(0x10C);
        // Two tracked docs (present-entry lines) and two untracked paths
        // (removal lines), inserted in reverse so the order must come from
        // the drain, not from insertion.
        sim.register_and_place("77777777-7777-7777-7777-777777777777", "z.md", b"z");
        sim.register_and_place("88888888-8888-8888-8888-888888888888", "b.md", b"b");
        // Start the probe from a clean journal baseline (the seeding above
        // drained and flushed on its own schedule).
        sim.persisted_journal.clear();
        sim.state.journal_pending.clear();
        sim.journal_lines_since_save = 0;
        sim.state_dirty = false;
        for rel in ["gone-b.md", "z.md", "gone-a.md", "b.md"] {
            sim.state.journal_pending.insert(PathBuf::from(rel));
        }
        sim.drain_identity_journal_model();
        let order: Vec<(&str, bool)> = sim
            .persisted_journal
            .iter()
            .map(|l| (l.path.as_str(), l.entry().is_some()))
            .collect();
        assert_eq!(
            order,
            vec![
                ("b.md", true),
                ("z.md", true),
                ("gone-a.md", false),
                ("gone-b.md", false)
            ],
            "inserts before removals, each group sorted by path"
        );
        assert!(
            !sim.state_dirty,
            "four lines are far inside the budget: no forced save"
        );

        // Cross the budget in one drain: a removal line per synthetic path.
        for i in 0..=JOURNAL_LINES_FORCE_SAVE {
            sim.state
                .journal_pending
                .insert(PathBuf::from(format!("edit-{i}.md")));
        }
        sim.drain_identity_journal_model();
        assert!(
            sim.journal_lines_since_save > JOURNAL_LINES_FORCE_SAVE && sim.state_dirty,
            "past the line budget the drain marks state dirty for a compacting save"
        );
        sim.flush_state_if_caught_up();
        assert!(
            sim.persisted_journal.is_empty()
                && sim.journal_lines_since_save == 0
                && !sim.state_dirty,
            "the compacting save truncates the journal and resets the budget"
        );
    }

    /// Blob LWW through the modeled core: a newer blob lands on the
    /// modeled disk via the shared `WriteFile` route; a stale one arriving later
    /// is gated out, so the disk keeps the LWW winner.
    #[test]
    fn test_remote_blob_lww_converges_on_disk() {
        let mut sim = DaemonSim::new(0xB10B);
        let id = "44444444-4444-4444-4444-444444444444";
        let rel = PathBuf::from("img.png");
        // Track the doc (a placed remote register's identity claim).
        kutl_daemon::core::handle::register_identity(&mut sim.state, &rel, id.to_owned(), true);

        let blob_event = |bytes: &[u8], ts: i64, wall: u64| Event::RemoteOps {
            document_id: id.to_owned(),
            ops: bytes.to_vec(),
            metadata: vec![ChangeMetadata {
                timestamp: ts,
                ..Default::default()
            }],
            content_mode: i32::from(kutl_proto::sync::ContentMode::Blob),
            local_content: None,
            author_by_agent_snapshot: std::collections::HashMap::new(),
            stamp: EventStamp {
                wall_ms: wall,
                origin_hlc: None,
            },
        };

        sim.feed(blob_event(&[0x89, 1], 100, 10));
        sim.run_one_handle();
        sim.feed(blob_event(&[0x89, 2], 200, 20)); // newer: replaces
        sim.run_one_handle();
        sim.feed(blob_event(&[0x89, 3], 150, 30)); // stale: gated out
        sim.run_one_handle();

        assert_eq!(
            sim.disk.get(&rel).map(|(c, _)| c.clone()),
            Some(vec![0x89, 2]),
            "the modeled disk holds the LWW winner (newest timestamp)"
        );
    }

    #[test]
    fn test_bounded_queue_rejects_when_full() {
        // Models the daemon's CHANNEL_CAPACITY=64 (daemon/session.rs): try_send Errs at cap.
        let mut q: BoundedQueue<u32> = BoundedQueue::new(2);
        assert!(q.try_send(1).is_ok());
        assert!(q.try_send(2).is_ok());
        assert_eq!(q.try_send(3), Err(3)); // full
        assert_eq!(q.try_recv(), Some(1));
        assert!(q.try_send(3).is_ok()); // space freed
    }

    #[test]
    fn test_feed_then_step_drains_one_event() {
        let mut sim = DaemonSim::new(0x00C0_FFEE);
        sim.feed(Event::FileModified {
            rel: PathBuf::from("a.md"),
            content: Some(b"x".to_vec()),
            stamp: EventStamp {
                wall_ms: 1,
                origin_hlc: None,
            },
        });
        assert!(
            !sim.is_quiescent(),
            "a fed-but-unprocessed event is not quiescent"
        );
        let _ = sim.step();
        // After processing the only event and draining its effects, quiesce.
        sim.drain_to_quiescence_or_fail(1_000);
        assert!(sim.is_quiescent());
    }

    /// A local file creation event for doc index `i`, the `cp -R` repro path.
    fn local_create(i: usize) -> Event {
        Event::FileModified {
            rel: PathBuf::from(format!("file-{i}.md")),
            content: Some(format!("doc {i}\n").into_bytes()),
            stamp: EventStamp {
                wall_ms: i as u64,
                origin_hlc: None,
            },
        }
    }

    /// The shipped (unbounded-outbound) model drains a bulk seed of 100 local
    /// creates to quiescence with zero docs stranded.
    #[test]
    fn test_shipped_model_drains_bulk_seed() {
        const DOCS: usize = 100;
        let mut sim = DaemonSim::new(0xD0_0D);
        for i in 0..DOCS {
            sim.feed(local_create(i));
        }
        assert!(sim.try_drain_to_quiescence(50_000), "shipped model wedged");
        assert!(sim.is_quiescent());
        assert_eq!(sim.unconfirmed_count(), 0);
        // Every queue drained to empty.
        assert_eq!(sim.queue_depths(), (0, 0, 0, 0, 0));
    }

    /// The blocking (bounded-`sync_cmd`) model FIXPOINTS on a bulk seed — and the
    /// fixpoint is a genuine WEDGE: every channel is saturated and work is still
    /// pending. This is the reproduced bulk-load deadlock; the assertion proves it
    /// is a real circular wait, not a step-budget artifact.
    ///
    /// The seed must register enough docs to SATURATE the relay's inbound queue
    /// (cap 256, `RELAY_INBOUND_CAP`): the circular wait closes only when
    /// `relay_inbound` is also full, so the WS writer can't drain `sync_cmd`. gamma
    /// emits O(1) outbound commands per doc (one Register + one Subscribe), so the
    /// seed must exceed ~128 docs to cross that 256 threshold. A cascade that
    /// re-emitted every Subscribe on each reconcile would cross it at fewer docs,
    /// but that is a volume artifact, not the real deadlock, which is a saturation
    /// circular wait independent of volume.
    #[test]
    fn test_blocking_model_fixpoints_with_full_sync_cmd() {
        /// Above the relay-inbound saturation point (256 / ~2 commands per doc) so
        /// the circular wait actually closes under gamma's O(1)-per-doc volume.
        const DOCS: usize = 200;
        let mut sim = DaemonSim::new_blocking(0xD0_0D);
        for i in 0..DOCS {
            sim.feed(local_create(i));
        }
        let reached = sim.try_drain_to_quiescence(200_000);
        assert!(
            !reached,
            "blocking model must fixpoint (the reproduced deadlock)"
        );
        let (pending, inbound, sync_cmd, relay_in, _relay_ctrl) = sim.queue_depths();
        assert_eq!(
            sync_cmd, DAEMON_CHANNEL_CAP,
            "the wedge holds `sync_cmd` FULL (the loop is stalled on a bounded send)"
        );
        assert_eq!(
            relay_in, RELAY_INBOUND_CAP,
            "the wedge holds `relay_inbound` FULL (the WS writer can't drain sync_cmd)"
        );
        assert!(
            pending + inbound > 0,
            "work is still pending at the fixpoint (the loop made no progress)"
        );
        assert!(
            sim.unconfirmed_count() > 0,
            "docs remain stranded confirmed:false at the deadlock"
        );
    }

    /// A snapshotted document whose mirror no longer decodes is retired on
    /// restart the way the daemon retires an unreadable sidecar: no engine,
    /// the document marked as awaiting the relay's refill.
    #[test]
    fn test_restart_retires_an_unreadable_sidecar_mirror() {
        let mut sim = DaemonSim::new(0xC0_4B1D);
        sim.feed(Event::FileModified {
            rel: PathBuf::from("a.md"),
            content: Some(b"hello".to_vec()),
            stamp: EventStamp {
                wall_ms: 10,
                origin_hlc: None,
            },
        });
        sim.run_one_handle();
        let id = sim
            .state
            .file_identity
            .get(&PathBuf::from("a.md"))
            .map(|fi| fi.document_uuid.clone())
            .expect("mint claimed identity");
        assert!(
            sim.persisted_docs.contains_key(&id),
            "precondition: the mint's SaveDoc snapshotted the mirror"
        );

        sim.corrupt_doc_sidecar(&id);
        sim.restart();

        assert!(
            sim.state.get_doc(&id).is_none(),
            "an undecodable mirror loads no engine"
        );
        assert!(
            sim.state.awaiting_content.contains(&id),
            "the document awaits the relay's refill"
        );
        assert!(
            !sim.persisted_docs.contains_key(&id),
            "the retired mirror is gone, as the real sidecar moves aside"
        );
    }
}
