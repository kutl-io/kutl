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
use kutl_proto::sync::ChangeMetadata;
use rand::RngExt;
use rand_chacha::ChaChaRng;
use uuid::Uuid;

use crate::clock::VirtualClock;
use crate::prng::peer_rng;

/// The daemon's inbound + outbound channel capacity (`CHANNEL_CAPACITY`,
/// daemon/session.rs). Both `inbound` and `sync_cmd` are sized to this in the real
/// daemon; the sim models them as separate `BoundedQueue`s of this cap.
const DAEMON_CHANNEL_CAP: usize = 64;

/// The relay connection's inbound channel capacity (`conn.rs:143`). The relay
/// accepts up to this many in-flight commands from one connection before
/// back-pressuring.
const RELAY_INBOUND_CAP: usize = 256;

/// The relay control (ack) channel capacity (`conn.rs:14`). Lifecycle acks ride
/// this cap-16 rail back to the daemon (Mode-2 ack delivery).
const RELAY_CTRL_CAP: usize = 16;

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
/// (`CHANNEL_CAPACITY`, daemon/session.rs), relay inbound = 256 (`conn.rs:143`), relay ctrl = 16
/// (`conn.rs:14`).
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
    /// The relay's inbound queue (cap 256, `conn.rs:143`).
    relay_inbound: BoundedQueue<OutCommand>,
    /// The relay's control/ack queue back to the daemon (cap 16, `conn.rs:14`).
    relay_ctrl: BoundedQueue<Event>,
    /// In-memory disk: relative path → (content, version counter). The version
    /// counter is unused by the liveness assertions but records write ordering.
    disk: HashMap<PathBuf, (Vec<u8>, u64)>,
    /// graft 1 TOCTOU-window model: relative path → bytes for an untracked file
    /// armed to land on disk INSIDE the shell's `apply_guarded_place` critical
    /// section — i.e. BETWEEN the core's decision (effect emit) and the shell's
    /// atomic stat. This is the sim's model of the race window the design flags:
    /// a local create the watcher hasn't yet delivered to the core lands while the
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
    /// One-shot failure flag for the next fs effect (graft 3 self-heal model).
    ///
    /// When armed, the next `WriteFile` or `RenameFile` effect in `route_effect`
    /// (or the AGREEMENT branch of `apply_guarded_place`) skips BOTH the disk
    /// mutation AND the shadow fold — modelling the funnel's log-and-swallow
    /// (`apply_write`/`apply_rename` returning `None`) that leaves the shadow
    /// unchanged on failure. The flag
    /// is cleared immediately after the skip. NO retry is scheduled — the real
    /// driver has none: the next `reconcile_placement` (whenever a later event
    /// or the periodic tick runs `DaemonCore::handle`) is what re-derives the
    /// mover and retries. Modelling an automatic next-step retry here was the
    /// `ba966512` faking class: it hid the fact that an IDLE space never heals.
    fail_next_fs_effect: bool,
    /// Live-disk inode assignments: path → inode number.
    ///
    /// Models the OS-level inode that the shell reports back via
    /// `EffectResult::FileWritten { inode }` / `EffectResult::RenameApplied { inode }`.
    /// Monotonically assigned by `next_inode`; survives out-of-band renames (the
    /// same inode follows the file to its new path, matching POSIX rename semantics).
    /// Used by `apply_guarded_place` (to fold the inode ACK and locate a relocated
    /// file by inode for the missing-source conform).
    disk_inodes: HashMap<PathBuf, u64>,
    /// Monotonically increasing inode counter for the in-memory disk. Seeded at 1
    /// so inode 0 is never assigned (0 is "no inode" in the shadow's `Option<u64>`).
    inode_counter: u64,
    /// The watcher's echo-suppression set (`watcher::SuppressionSet`), modeled as
    /// an ordered list of `(rel, expected_hash)` entries in the order the shell
    /// registered them.
    ///
    /// The order is load-bearing (the `5dd87257` class, TDD case 11): a rename
    /// registers the removal half `(old, None)` BEFORE the write half
    /// `(new, Some(hash))` — the `rename_doc` funnel order (daemon/effects.rs). A local create consults this
    /// set via `consume_write_echo` semantics: a Create/Modify whose bytes hash
    /// matches a `Some(hash)` entry at its path is the daemon's OWN write echo
    /// (suppressed); a different-bytes create — or a create at a `None` removal
    /// half — is a GENUINE edit and is never swallowed (`consume_write_echo`,
    /// watcher/suppression.rs).
    suppressions: Vec<(PathBuf, Option<Vec<u8>>)>,
    /// Whether the most recent [`Self::feed_local_create`] was swallowed by an
    /// echo suppression (`true`) or surfaced to the core as a genuine edit
    /// (`false`). Lets TDD case 11 assert own-write-suppressed vs
    /// peer-recreate-not-swallowed without inspecting the suppression set itself.
    last_local_create_suppressed: bool,
    /// Every `RenameDocument` the relay model received, in arrival order, as
    /// `(document_id, new_path)`. The §1.3 observability rail: a concurrent
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
    /// The persisted `state.json` MIRROR: a snapshot taken only when the
    /// modeled coalesced flush fires (`flush_state_if_caught_up`, mirroring
    /// the driver's `4d67eb7b` semantics: `Effect::SaveState` marks dirty, the
    /// write happens once the intake drains). `restart()` reloads from THIS,
    /// not the live map — the gap between them IS the coalesced-persist crash
    /// window (the crash-mid-bulk class).
    persisted_state: kutl_daemon::state::DaemonState,
    /// The persisted `.dt` sidecar mirror: full-oplog snapshots taken when an
    /// `Effect::SaveDoc` is APPLIED (the real `save_doc` writes inline);
    /// removed on `Effect::RemoveDoc`. `restart()` reloads each as a fresh
    /// CRDT carrying exactly the persisted oplog.
    persisted_docs: HashMap<String, (Vec<u8>, Vec<ChangeMetadata>)>,
    /// A persist is pending — the modeled `SpaceWorker::state_dirty`.
    state_dirty: bool,
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
            relay_ctrl: BoundedQueue::new(RELAY_CTRL_CAP),
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
            suppressions: Vec::new(),
            last_local_create_suppressed: false,
            renames_relayed: Vec::new(),
            subscriptions: Vec::new(),
            persisted_state: kutl_daemon::state::DaemonState::default(),
            persisted_docs: HashMap::new(),
            state_dirty: false,
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
        self.pending_in.is_empty()
            && self.inbound.is_empty()
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
            .all(|rel| claimed.contains(&rel_path_string(rel)))
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
    /// in `state.deferred` (graft 2). The sim's read of "the doc is parked waiting
    /// for the occupant to vacate, not placed".
    #[must_use]
    pub fn is_deferred(&self, document_id: &str) -> bool {
        self.state.deferred.contains_key(document_id)
    }

    /// Whether the daemon's client-side registry (`known_records`) holds `document_id`
    /// as an ALIVE record — the structural source of truth for "the cascade will
    /// consider this doc a placement candidate." An unregistered doc is a tombstone:
    /// not alive, so `desired_assignment` excludes it and it can never be a mover
    /// (the `37093896` resurrection class is structural, graft 2).
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
    /// `Untracked` — the FS-1/2 occupant graft 2 defers onto.
    ///
    /// This is the sim's stand-in for the shell's atomic stat seeing a file no
    /// document is arbitrated onto (the `Occupant::Untracked` branch, graft 1):
    /// the bytes land on disk and `shadow_occupant[casefold(rel)] = Untracked`, so
    /// the next `reconcile_placement` sees the path held and defers a non-revival
    /// mover onto it. Graft 1 (Task 4) wires the real `UntrackedFileObserved`
    /// path; until then the harness seeds the occupant directly.
    pub fn put_untracked_file(&mut self, rel: &str, content: &[u8]) {
        let path = PathBuf::from(rel);
        self.write_seq += 1;
        self.disk
            .insert(path.clone(), (content.to_vec(), self.write_seq));
        self.state
            .shadow
            .shadow_occupant
            .insert(sim_casefold(&path), kutl_daemon::core::Occupant::Untracked);
    }

    /// Arm an untracked file to land on disk INSIDE the shell's `GuardedPlace`
    /// critical section for `rel` — the graft-1 TOCTOU-window model (FS-1).
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

    /// Alias of [`Self::put_untracked_file_after_decide`] for the atomicity TDD
    /// case (case 10): a local create injected between the core's decision and the
    /// shell's mutation. Same window model; named to read as the atomicity probe.
    pub fn arm_disk_write_in_guarded_window(&mut self, rel: &str, content: &[u8]) {
        self.put_untracked_file_after_decide(rel, content);
    }

    /// The current on-disk bytes at `rel`, if any. Lets a graft-1 case assert the
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
    /// here because a deferred remote register never claimed the path: in the
    /// former imperative daemon `handle_remote_register` only called
    /// `register_identity` from `place_register`, the NOT-deferred branch. The pure
    /// `handle.rs` port folds identity eagerly (before the cascade decides), so the
    /// deferred remote left a stale `file_identity`/`uuid_to_path` claim at `rel`;
    /// this helper drops that claim FIRST — modeling the entry the real daemon never
    /// made — so the scan's local create mints a genuinely fresh id rather than
    /// resolving the path back to the deferred remote (the FS-2 conflation graft 1
    /// exists to close).
    ///
    /// Then it drives a local `FileModified` (the scan's create) so the core mints
    /// the fresh id, seeds its record, and emits an outbound `RegisterDocument`, and
    /// finally claims the file in the shadow as that id (`Tracked`). The resulting
    /// two-records-at-one-path collision conflict-copies on the next reconcile — both
    /// files survive. Returns the fresh id (empty string when there are no bytes).
    pub fn scan_mints_uuid_for(&mut self, rel: &str) -> String {
        let path = PathBuf::from(rel);
        let content = self
            .disk
            .get(&path)
            .map(|(c, _)| c.clone())
            .unwrap_or_default();
        // ASSERT (not remove): identity-follows-placement means a DEFERRED
        // remote register must never have claimed this path — the old
        // claim-REMOVAL here was state-faking that would have papered over a
        // regression (if eager claiming ever returns, FS-2 keeps passing
        // because the workaround deletes the claim before asserting). The
        // assert turns the workaround into a live regression sensor.
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
        // No manual shadow fold: the core's mint now folds
        // `shadow.set_tracked` itself (the rm/mv/create local-fold trilogy),
        // so the modeled `FileModified` above already adopted the file — the
        // old direct `set_tracked` here was exactly the state-faking the
        // `ba966512` rule forbids. The assert keeps the model honest.
        assert!(
            self.state
                .shadow
                .shadow_occupant
                .contains_key(&sim_casefold(&path)),
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

    /// Register the rename echo-suppression PAIR in the funnel's load-bearing
    /// order: the removal half `(old, None)` FIRST, then the write half
    /// `(new, Some(hash))` — the `rename_doc` ordering (TDD case 11 / the
    /// `5dd87257` §1.3/§4.2 class). The single owner of that fact: every
    /// rename-shaped disk effect routes through here so the two halves cannot
    /// drift apart at one site while the case-11 pin keeps passing against
    /// the other.
    fn push_rename_suppression_pair(
        &mut self,
        old: &std::path::Path,
        new: &std::path::Path,
        write_hash: Option<Vec<u8>>,
    ) {
        self.suppressions.push((old.to_path_buf(), None));
        self.suppressions.push((new.to_path_buf(), write_hash));
    }

    /// Arm a one-shot failure for the NEXT disk-effect (`WriteFile` or
    /// `RenameFile`) that arrives in `route_effect`.
    ///
    /// When fired, the effect skips BOTH the disk mutation AND
    /// `apply_effect_result` — modelling the funnel's log-and-swallow
    /// (`apply_write`/`apply_rename` returning `None`) that leaves the shadow
    /// unchanged on a failed fs op. The
    /// flag clears immediately after the first skip, so subsequent effects are
    /// unaffected (graft 3 self-heal: the next reconcile re-derives and retries).
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
    /// Assigns a fresh inode to the placed file and records it in the shadow's
    /// `shadow_path_inode` map so the missing-source conform can recover the doc's
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
        // Assign a fresh inode and record it on disk + in the shadow's path→inode
        // map (`shadow_path_inode[path] = inode`) so the missing-source conform can
        // recover the doc's inode after an out-of-band relocation.
        let inode = self.next_inode();
        self.disk_inodes.insert(path_buf.clone(), inode);
        self.state.shadow.set_inode(inode, &path_buf);
        // Record it in `file_identity` too — the daemon's recorded view the §1.3
        // drain and `find_rename_source` read (the GuardedPlace ran before these
        // content bytes landed, so its place-time inode fold saw none).
        if let Some(entry) = self.state.file_identity.get_mut(&path_buf) {
            entry.inode = Some(inode);
        }
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
    /// `(document_id, new_path)` — the §1.3 silently-dropped-rename rail (see the
    /// field doc).
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

    /// The echo-suppression entries the shell has registered, in registration
    /// order. Each is `(rel, expected_hash)`: a `None` hash is a removal half, a
    /// `Some(hash)` is a write half.
    ///
    /// TDD case 11 asserts the rename pairing/order: a `GuardedPlace(Rename)`
    /// records the removal half `(old, None)` BEFORE the write half
    /// `(new, Some(hash))` — the `rename_doc` funnel order that the
    /// `5dd87257` §1.3/§4.2 class depends on.
    #[must_use]
    pub fn suppressions_in_order(&self) -> &[(PathBuf, Option<Vec<u8>>)] {
        &self.suppressions
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
    /// the catastrophic invariant TDD case 11 locks: own write IS suppressed; a
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
    /// the core adopted at this path", used by TDD case 11 to prove a genuine
    /// recreate was materialized (not swallowed by the removal echo).
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
    /// `shadow_path_inode` still point at `old`.
    ///
    /// This is the realistic state §7.1 / `f_conflicts` hit: with the shadow stale,
    /// the cascade emits a blind `GuardedPlace(Rename old→target)`, and the SHELL must
    /// conform the relocated file by inode (`apply_guarded_place`, the twin of
    /// `place_now`'s missing-source conform, `daemon/effects.rs`). An earlier version of this
    /// helper FAKED an inode-map entry at `new`, which artificially tripped the (now-
    /// removed) graft-4 inode-discrepancy probe and hid the real shell-conform path —
    /// the precise reason the sim never caught §7.1. Modelling it faithfully is what
    /// proved graft 4 dead.
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
        // nothing. (The shadow's stale `shadow_path`/`shadow_path_inode`/occupant are
        // the point — they still claim `old`, which is exactly the divergence the
        // shell conform must reconcile against live disk.)
    }

    /// The current shadow path for `document_id` as a forward-slash string, or
    /// `None` if the doc is not tracked in the shadow.
    ///
    /// Mirrors the driver's `file_identity` map (daemon.rs) for the shadow side: the value
    /// `shadow_path[id]` after a successful place, folded via
    /// `apply_effect_result`. Used by graft-3 cases to assert the shadow is
    /// (or is NOT) advanced after a failed disk effect.
    #[must_use]
    pub fn shadow_path(&self, document_id: &str) -> Option<String> {
        Uuid::parse_str(document_id).ok().and_then(|uid| {
            self.state
                .shadow
                .shadow_path
                .get(&uid)
                .map(|p| rel_path_string(p))
        })
    }

    /// Drive exactly ONE `DaemonCore::handle` over the next pending/inbound event,
    /// applying its effects, WITHOUT flushing the outbound→ack round-trip. Used by
    /// the graft-2 cases to inspect the derived `deferred`/placement state produced
    /// by a single event before the relay model acks anything.
    ///
    /// Refills `inbound` from the pending buffer, pops one event, runs the core,
    /// and routes its effects (placing/deferring synchronously). A no-op when there
    /// is no event to process.
    pub fn run_one_handle(&mut self) {
        self.refill_inbound();
        let Some(event) = self.inbound.try_recv() else {
            return;
        };
        self.model_disk_edge_for(&event);
        let effects = DaemonCore::handle(&mut self.state, event);
        for effect in effects {
            self.route_effect(effect);
        }
        self.flush_state_if_caught_up();
    }

    /// The modeled coalesced persist (`SpaceWorker::flush_state_if_caught_up`,
    /// `4d67eb7b`): the snapshot is written only when a save is pending AND the
    /// intake is drained, so a bulk burst does one save at its end — and a
    /// crash mid-burst restarts from a STALE snapshot (the window the
    /// crash-mid-bulk e2e probes).
    fn flush_state_if_caught_up(&mut self) {
        if self.state_dirty && self.inbound.is_empty() && self.pending_in.is_empty() {
            // The REAL pre-serialize sync (inodes, HLC floor, register
            // stamps) — shared with the driver's `save_state`.
            self.state.sync_persisted();
            self.persisted_state = self.state.state.clone();
            self.state_dirty = false;
        }
    }

    /// The `WriteFile` arm of `route_effect` (extracted for length): the
    /// graft-3 failure model (an armed one-shot failure skips BOTH the disk
    /// mutation AND the shadow fold — the funnel's log-and-swallow, so the
    /// next reconcile re-derives and retries), the inline write
    /// echo-suppression the funnel carries (`write_doc`), the disk mutation,
    /// and the landed-write shadow fold (the shell ACK; the sim has no real
    /// inodes, so it reports `None` exactly like the driver's fold when a
    /// stat fails).
    fn apply_write_file(&mut self, rel: PathBuf, content: Vec<u8>, suppress_hash: Option<Vec<u8>>) {
        if self.fail_next_fs_effect {
            self.fail_next_fs_effect = false;
            return;
        }
        self.suppressions.push((rel.clone(), suppress_hash));
        self.write_seq += 1;
        self.disk.insert(rel.clone(), (content, self.write_seq));
        DaemonCore::apply_effect_result(
            &mut self.state,
            kutl_daemon::core::EffectResult::FileWritten { rel, inode: None },
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

    /// Model a daemon RESTART — process death, then `SpaceWorker::new` over the
    /// same `.kutl` dir. Every in-memory structure is dropped; the new state is
    /// rebuilt ONLY from the persisted mirrors:
    ///
    /// - `persisted_state` (the `state.json` snapshot): the document map
    ///   rebuilds `file_identity`/`uuid_to_path` with the PERSISTED inode (at a
    ///   gone path the inode is unreadable from disk — the offline-rename
    ///   case), `register_hlc` reloads per entry (the `4bfaa9c9`/A1
    ///   causal-floor class), and the HLC restores from the persisted floor +
    ///   device actor, exactly as `SpaceWorker::new` does.
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
    /// not persisted does not survive (`ba966512`).
    pub fn restart(&mut self) {
        let mut persisted = self.persisted_state.clone();

        let mut file_identity = HashMap::new();
        let mut uuid_to_path = HashMap::new();
        for (path_str, uuid) in &persisted.validated_documents() {
            let rel = PathBuf::from(path_str);
            let inode = persisted.documents.get(path_str).and_then(|e| e.inode);
            file_identity.insert(
                rel.clone(),
                kutl_daemon::core::FileIdentity {
                    document_uuid: uuid.clone(),
                    inode,
                },
            );
            uuid_to_path.insert(uuid.clone(), rel);
        }
        let register_hlc: HashMap<String, Hlc> = persisted
            .register_hlc
            .iter()
            .filter_map(|(id, reg)| reg.to_hlc().map(|hlc| (id.clone(), hlc)))
            .collect();
        let actor = persisted.ensure_device_actor();
        let hlc = match persisted.hlc_floor {
            Some(floor) => kutl_core::HlcClock::restore(
                actor,
                Hlc {
                    physical_ms: floor.physical_ms,
                    logical: floor.logical,
                    actor,
                },
            ),
            None => kutl_core::HlcClock::new(actor),
        };
        let mut documents = HashMap::new();
        for (id, (ops, metadata)) in &self.persisted_docs {
            let mut doc = kutl_core::Document::new();
            if doc.merge(ops, metadata).is_ok() {
                documents.insert(id.clone(), doc);
            }
        }

        let old = std::mem::replace(
            &mut self.state,
            SpaceState::new_for_test(String::new(), PathBuf::new(), String::new()),
        );
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
            exempt_revival: std::collections::HashSet::new(),
            known_records: RegistryLattice::new(),
            shadow: kutl_daemon::core::DiskShadow::default(),
        };
        self.state_dirty = false;
        self.pending_in.clear();
        self.inbound.q.clear();
        self.loop_out.clear();
        self.sync_cmd.q.clear();
        self.relay_inbound.q.clear();
        self.relay_ctrl.q.clear();
        self.suppressions.clear();
        self.last_local_create_suppressed = false;
    }

    /// Model the disk-side effect of an event the shell observes BEFORE the core
    /// runs (the watcher/shell edge the pure core never touches).
    ///
    /// Covers:
    /// - `UntrackedFileRemoved`: a local `rm` of an untracked occupant vacated
    ///   `rel`, so the shell removes the bytes and clears the
    ///   `Occupant::Untracked` mark before the core re-derives placement.
    ///   (Graft 1's `UntrackedFileObserved` is seeded directly by
    ///   `put_untracked_file` in the graft-2 cases.)
    /// - `RemoteRename`: the §1.3 drain twin — the model of the driver's
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
                self.state.shadow.shadow_occupant.remove(&sim_casefold(rel));
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
        self.model_disk_edge_for(&event);
        let effects = DaemonCore::handle(&mut self.state, event);
        for effect in effects {
            self.route_effect(effect);
        }
        self.flush_state_if_caught_up();
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
            // `flush_state_if_caught_up`); SaveDoc/RemoveDoc write the sidecar
            // mirror inline (the real `save_doc`/`remove_doc_sidecar` are
            // inline disk writes). Each still occupies an Other slot so queue
            // scheduling is unchanged.
            Effect::SaveState => {
                self.state_dirty = true;
                self.loop_out.push_back(OutCommand::Other);
            }

            Effect::SaveDoc { document_id } => {
                self.snapshot_doc_sidecar(&document_id);
                self.loop_out.push_back(OutCommand::Other);
            }
            Effect::RemoveDoc { document_id } => {
                self.persisted_docs.remove(&document_id);
                self.state.remove_doc_in_memory(&document_id);
                self.loop_out.push_back(OutCommand::Other);
            }

            Effect::WriteFile {
                rel,
                content,
                suppress_hash,
            } => self.apply_write_file(rel, content, suppress_hash),
            Effect::RenameFile {
                old,
                new,
                suppress_hash,
            } => {
                // graft 3 failure model: an armed one-shot failure skips BOTH the
                // disk mutation AND the shadow fold — modelling the funnel's
                // log-and-swallow (`apply_rename` returning `None`). The shadow is
                // unchanged so the next reconcile re-derives and retries (self-heal).
                if self.fail_next_fs_effect {
                    self.fail_next_fs_effect = false;
                    return;
                }
                // Register both echo-suppression halves in the funnel ORDER
                // (`rename_doc`, daemon/effects.rs): removal half `(old, None)`
                // FIRST, then write half `(new, suppress_hash)` — the load-bearing
                // `5dd87257` ordering (also exercised via `GuardedPlace(Rename)`).
                self.push_rename_suppression_pair(&old, &new, suppress_hash);
                self.write_seq += 1;
                let moved = self.disk.remove(&old).map(|(c, _)| c).unwrap_or_default();
                self.disk.insert(new.clone(), (moved, self.write_seq));
                // Move the disk inode: the inode follows the file to the new path.
                let inode = self.disk_inodes.remove(&old);
                if let Some(inode_val) = inode {
                    self.disk_inodes.insert(new.clone(), inode_val);
                }
                // graft 3: fold the landed rename into the shadow (vacate old, track
                // new), matching the driver (`apply_effect`'s `RenameFile` arm). Report the inode so
                // the shadow's inode maps stay current after a conform rename.
                DaemonCore::apply_effect_result(
                    &mut self.state,
                    kutl_daemon::core::EffectResult::RenameApplied { old, new, inode },
                );
            }
            Effect::RemoveFile { rel } => {
                // Register the removal echo-suppression the funnel sends
                // (`remove_doc`, daemon/effects.rs): `(rel, None)`.
                self.suppressions.push((rel.clone(), None));
                self.disk.remove(&rel);
                self.disk_inodes.remove(&rel);
                // graft 3: fold the landed removal into the shadow (the shell
                // ACK), vacating the occupant — else a delete leaves a stale
                // `Tracked` entry that makes the next register's stat mistake a
                // foreign recreate for the tracked incumbent (the §4.2
                // concurrent-recreate merge corruption). Mirrors the driver
                // (`apply_remove` → `FileRemoved`).
                DaemonCore::apply_effect_result(
                    &mut self.state,
                    kutl_daemon::core::EffectResult::FileRemoved { rel },
                );
            }

            // Phase 4 gamma cascade: the atomic shell-guarded place.
            Effect::GuardedPlace {
                id,
                target,
                expected_free,
                place_kind,
            } => self.apply_guarded_place(&id, &target, expected_free, &place_kind),
        }
    }

    /// Apply a [`Effect::GuardedPlace`] (graft 1's atomic stat-and-place, modeled
    /// as ONE non-suspending step). The shell stats `target` for an untracked
    /// occupant and places without any event interleaving in between — mirroring
    /// the real driver's `apply_guarded_place`, which runs the stat and the place
    /// adjacently (the `exists()`-then-rename shape the former imperative
    /// `reconcile_disk_for_rename` used). The whole method runs to completion
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
    ///      `Occupant::Untracked` and the doc DEFERS (graft 1 + graft 2 predicate).
    ///      The local bytes on disk are untouched — no clobber.
    ///    - AGREEMENT: place. `expected_free == true` with a free path is a normal
    ///      place; `expected_free == false` with an untracked occupant is a revival
    ///      ADOPTING its own orphan (graft 2) — the core knows the occupant is there
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
        // graft 3 failure model: an armed one-shot failure aborts the whole
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
            // `place_now`'s `conform_relocated_or_materialize`, `daemon/effects.rs` / 41add2a5):
            // find the live file carrying the inode the shadow recorded for `old_rel`
            // and move THAT onto the target. Without this the blind move from `old_rel`
            // finds nothing and the file is stranded as an orphan — the §7.1 /
            // `f_conflicts` wrong-winner the e2e caught but the sim (which used to FAKE
            // an inode-map entry in `relocate_out_of_band`, routing through the since-
            // removed graft-4 ProbePath) silently missed.
            let source = if self.disk.contains_key(old_rel) {
                old_rel.clone()
            } else {
                self.state
                    .shadow
                    .shadow_path_inode
                    .get(old_rel)
                    .copied()
                    .and_then(|inode| self.disk_path_with_inode(inode))
                    .unwrap_or_else(|| old_rel.clone())
            };
            // Register the echo-suppression pair through the funnel ORDER the real
            // `rename_doc` uses (daemon/effects.rs): the removal half
            // `(source, None)` FIRST, then the write half `(new, Some(hash))`, where
            // the hash is sha256 of the moved bytes (the file's content, read at
            // the resolved source, becomes the new path's content). The order is
            // load-bearing — the `5dd87257` §1.3/§4.2 class (TDD case 11).
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
                .remove(&sim_casefold(&source));
            if source.as_path() != old_rel.as_path() {
                self.state
                    .shadow
                    .shadow_occupant
                    .remove(&sim_casefold(old_rel));
                self.state.shadow.shadow_path_inode.remove(old_rel);
            }
        }
        // Track the id at the target in the shadow so the next reconcile sees it
        // placed (Register and Rename both end here). A Register additionally
        // subscribes so the doc's content streams in.
        // Fold the post-op inode into the shadow (graft 3 ACK): the placed file's
        // inode is now at `target`, so the core can locate it later by inode.
        let inode = self.disk_inodes.get(target).copied();
        self.state.shadow.set_tracked(target, uid);
        if let Some(inode_val) = inode {
            self.state.shadow.set_inode(inode_val, target);
        }
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
    /// Identity-reading driver glue — the §1.3 remote-rename drain, the
    /// inode-rename detector — consults these maps; before this claim the sim
    /// modeled a daemon that forgot every doc it placed, so that glue had
    /// nothing to read and its bugs were invisible here. The placed file's inode
    /// is recorded into `file_identity` too (the real driver's claim reads the
    /// live inode; `record_local_register_inode`, 31aa2146) — the recorded view
    /// the §1.3 drain and `find_rename_source` match relocated files against.
    fn claim_identity_at_place(
        &mut self,
        id: &str,
        target: &std::path::Path,
        place_kind: &kutl_daemon::core::PlaceKind,
        inode: Option<u64>,
    ) {
        match place_kind {
            kutl_daemon::core::PlaceKind::Rename { .. } => {
                let prior = self.state.uuid_to_path.get(id).cloned();
                if let Some(prior_rel) = prior {
                    if prior_rel.as_path() != target {
                        kutl_daemon::core::handle::move_identity(
                            &mut self.state,
                            &prior_rel,
                            target.to_path_buf(),
                            id,
                        );
                    }
                } else {
                    kutl_daemon::core::handle::register_identity(
                        &mut self.state,
                        target,
                        id.to_owned(),
                        /* confirmed */ true,
                    );
                }
            }
            kutl_daemon::core::PlaceKind::Register => {
                if !self.state.uuid_to_path.contains_key(id) {
                    kutl_daemon::core::handle::register_identity(
                        &mut self.state,
                        target,
                        id.to_owned(),
                        /* confirmed */ true,
                    );
                }
            }
        }
        if let Some(entry) = self.state.file_identity.get_mut(&target.to_path_buf()) {
            entry.inode = inode;
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
    /// inbound queue. Models the cap-16 ack rail (`conn.rs:14`) feeding the loop.
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
        self.relay.observe(DocRecord {
            document_id: id,
            path: path.to_owned(),
            registered_hlc: Some(stamp),
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: None,
            displaced: false,
        });
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
        self.relay.observe(DocRecord {
            document_id: id,
            path: new_path.to_owned(),
            registered_hlc: None,
            renamed_hlc: Some(stamp),
            // The sim does not yet model a clock-skewed register the rename
            // observes; a faithful skew-floor model is the flagged kutl-sim
            // driver-glue extension, tracked separately.
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: None,
            displaced: false,
        });
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
        self.relay.observe(DocRecord {
            document_id: id,
            path,
            registered_hlc: None,
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: Some(stamp),
            displaced: false,
        });
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
        match self.state.shadow.shadow_occupant.get(&sim_casefold(target)) {
            Some(kutl_daemon::core::Occupant::Tracked(uid)) => Some(*uid),
            _ => None,
        }
    }

    /// Consume a write echo for a local create at `path`, mirroring
    /// `SuppressionSet::consume_write_echo` (watcher/suppression.rs): a Create/Modify
    /// whose `sha256(content)` matches a `Some(expected)` write-half suppression
    /// at `path` is the daemon's OWN write echo — consume that entry and return
    /// `true`. A different-bytes create (hash mismatch) or one at a `None` removal
    /// half is NOT a write echo: return `false` (it surfaces as a genuine edit).
    ///
    /// Hashing via `kutl_daemon::sha256_bytes` keeps the comparison byte-identical
    /// to the funnel's stored hash, so the sim's match agrees with the real
    /// watcher by construction.
    fn consume_write_echo(&mut self, path: &std::path::Path, content: &[u8]) -> bool {
        let current_hash = kutl_daemon::sha256_bytes(content);
        if let Some(pos) = self.suppressions.iter().position(|(p, expected)| {
            p == path && expected.as_deref() == Some(current_hash.as_slice())
        }) {
            self.suppressions.remove(pos);
            return true;
        }
        false
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

/// Render a relative path as a forward-slash string, matching the daemon's
/// `rel_path_to_string` (`core/mod.rs`) and the relay's lattice path keys so disk
/// paths and effective-path-index keys compare equal.
fn rel_path_string(rel: &std::path::Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Case-fold a path to the `DiskShadow::shadow_occupant` key form, mirroring the
/// daemon's `shadow::casefold` (`core/shadow.rs:97`, `pub(crate)` so unreachable
/// across the crate boundary): the forward-slash string, lowercased. Keeps the
/// sim's occupant lookups/removals keyed identically to the shadow's own inserts.
fn sim_casefold(rel: &std::path::Path) -> String {
    rel_path_string(rel).to_lowercase()
}

/// Extract the HLC from a lifecycle effect's metadata, if present and valid.
fn hlc_of(metadata: Option<&ChangeMetadata>) -> Option<Hlc> {
    metadata
        .and_then(|m| m.hlc.clone())
        .and_then(|wire| Hlc::try_from(wire).ok())
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

    /// The coalesced-persist CRASH WINDOW, visible in the sim: while the
    /// intake is non-empty the `SaveState` stays a dirty flag (`4d67eb7b`), so
    /// a crash before the flush restarts from the STALE snapshot and the
    /// just-minted identity is gone — the exact precondition of the
    /// crash-mid-bulk cluster-delete bug (`a5dd00a7`'s truth-table row).
    #[test]
    fn test_restart_inside_coalesced_window_loses_unflushed_identity() {
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

        sim.restart();

        assert!(
            !sim.state.file_identity.contains_key(&PathBuf::from("a.md")),
            "an identity minted inside the coalesced window is NOT persisted — \
             the restart faithfully loses it (the crash-window class)"
        );
    }

    /// Blob LWW through the modeled core (Task 6b): a newer blob lands on the
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
    /// seed must exceed ~128 docs to cross that 256 threshold (the Phase-3 stub's
    /// O(N) re-emitted Subscribes crossed it at fewer docs — an artifact, not the
    /// real deadlock, which is a saturation circular wait independent of volume).
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
}
