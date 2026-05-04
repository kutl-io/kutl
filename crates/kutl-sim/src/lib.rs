//! kutl-sim — simulation testing framework.
//!
//! Runs multiple CRDT peers in a single process with virtual time and
//! a virtual network, enabling deterministic, reproducible scenario tests.

pub mod clock;
pub mod fuzz;
pub mod fuzz_util;
pub mod link;
pub mod network;
pub mod peer;
pub mod prng;
pub mod relay_fuzz;
pub mod relay_sim;
pub mod sim_env;
pub mod store;
pub mod trace;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use kutl_core::{Boundary, EditContext, Result};

use clock::VirtualClock;
use link::LinkConfig;
use network::{Message, SendResult, VirtualNetwork};
use peer::Peer;
use prng::{net_rng, peer_rng};
use sim_env::SimEnv;
use store::MemoryDocStore;
use trace::{DropReason, TraceEvent, TraceLog};

/// Return the number of fuzz seeds to run.
///
/// Reads `KUTL_SIM_FUZZ_SEEDS` from the environment, falling back to 10.
pub fn seed_count() -> u64 {
    std::env::var("KUTL_SIM_FUZZ_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

/// Maximum ticks `run_until_quiescent` will iterate before panicking.
pub(crate) const MAX_QUIESCENT_TICKS: usize = 10_000;

/// Stride applied to the restart count when deriving a per-restart RNG index.
///
/// Multiplying the restart count by this value before adding the peer index
/// ensures that (`restart_count`, `peer_id`) pairs map to distinct `rng_index`
/// values for all realistic peer counts and restart counts. Without the stride,
/// peer 2 restarting once would collide with peer 2's initial seed if the
/// restart count were used directly.
const RNG_RESTART_STRIDE: usize = 100_000;

/// Index into the simulation's peer list.
pub type PeerId = usize;

/// Orchestrates a deterministic simulation of multiple CRDT peers.
#[derive(Debug)]
pub struct Simulation {
    seed: u64,
    clock: Arc<VirtualClock>,
    network: VirtualNetwork,
    peers: Vec<Peer>,
    /// Tracks the sender's local version at the time of last successful sync
    /// to each receiver. Used as the "since" point for delta encoding.
    last_synced: HashMap<(PeerId, PeerId), Vec<usize>>,
    /// Persisted snapshots for crash/recovery.
    store: MemoryDocStore,
    /// Which peers are currently crashed (not participating in sync/delivery).
    crashed: HashSet<PeerId>,
    /// Number of times each peer has been restarted (for deriving distinct PRNGs).
    restart_count: HashMap<PeerId, usize>,
    /// Optional trace log for debugging.
    trace: Option<TraceLog>,
}

impl Simulation {
    /// Create a new simulation with the given PRNG seed.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            clock: VirtualClock::new(0),
            network: VirtualNetwork::new(net_rng(seed)),
            peers: Vec::new(),
            last_synced: HashMap::new(),
            store: MemoryDocStore::default(),
            crashed: HashSet::new(),
            restart_count: HashMap::new(),
            trace: None,
        }
    }

    /// Enable tracing for this simulation (builder-style).
    #[must_use]
    pub fn with_tracing(mut self) -> Self {
        self.trace = Some(TraceLog::default());
        self
    }

    /// Enable tracing on an existing simulation.
    pub fn enable_tracing(&mut self) {
        if self.trace.is_none() {
            self.trace = Some(TraceLog::default());
        }
    }

    /// Return a reference to the trace log, if tracing is enabled.
    pub fn trace(&self) -> Option<&TraceLog> {
        self.trace.as_ref()
    }

    /// Record a trace event if tracing is enabled.
    fn trace_event(&mut self, event: TraceEvent) {
        if let Some(log) = &mut self.trace {
            log.record(event);
        }
    }

    /// Add a named peer and return its id.
    pub fn add_peer(&mut self, name: &str) -> Result<PeerId> {
        let id = self.peers.len();
        let rng = peer_rng(self.seed, id);
        let env = SimEnv::new(Arc::clone(&self.clock), rng);
        self.peers.push(Peer::new(name, env)?);
        Ok(id)
    }

    /// Set the link configuration for a directed link.
    pub fn configure_link(&mut self, from: PeerId, to: PeerId, config: LinkConfig) {
        self.network.configure_link(from, to, config);
    }

    /// Edit a peer's document.
    ///
    /// # Panics
    ///
    /// Panics if the peer is crashed.
    pub fn edit(
        &mut self,
        peer: PeerId,
        intent: &str,
        boundary: Boundary,
        f: impl FnOnce(&mut EditContext<'_>) -> Result<()>,
    ) -> Result<()> {
        assert!(
            !self.crashed.contains(&peer),
            "cannot edit: peer {peer} is crashed"
        );
        let result = self.peers[peer].edit(intent, boundary, f);
        if result.is_ok() {
            self.persist(peer);
            let tick = self.clock.now();
            self.trace_event(TraceEvent::Edit {
                tick,
                peer,
                intent: intent.to_owned(),
            });
        }
        result
    }

    /// Queue a sync from one peer to another.
    ///
    /// Skips if either peer is crashed.
    /// Returns the [`SendResult`] from the network.
    pub fn sync(&mut self, from: PeerId, to: PeerId) -> SendResult {
        if self.crashed.contains(&from) || self.crashed.contains(&to) {
            return SendResult::DroppedPartition;
        }

        // Check if there's anything new since the last sync.
        let since = self
            .last_synced
            .get(&(from, to))
            .map_or([].as_slice(), Vec::as_slice);

        if self.peers[from].local_version() == since {
            return SendResult::Enqueued;
        }

        let dt_bytes = self.peers[from].encode_since(since);
        let changes = self.peers[from].changes_since(since);

        let byte_count = dt_bytes.len();
        let tick = self.clock.now();

        let result = self.network.send(Message {
            from,
            to,
            dt_bytes,
            changes,
            delivery_tick: tick,
        });

        match result {
            SendResult::Enqueued => {
                self.last_synced
                    .insert((from, to), self.peers[from].local_version());
                self.trace_event(TraceEvent::MessageSent {
                    tick,
                    from,
                    to,
                    bytes: byte_count,
                });
            }
            SendResult::DroppedPartition => {
                self.trace_event(TraceEvent::MessageDropped {
                    tick,
                    from,
                    to,
                    reason: DropReason::Partition,
                });
            }
            SendResult::DroppedLoss => {
                self.trace_event(TraceEvent::MessageDropped {
                    tick,
                    from,
                    to,
                    reason: DropReason::Loss,
                });
            }
        }

        result
    }

    /// Sync every peer pair in both directions, skipping crashed peers.
    pub fn sync_all(&mut self) {
        let n = self.peers.len();
        for i in 0..n {
            for j in 0..n {
                if i != j {
                    self.sync(i, j);
                }
            }
        }
    }

    /// Advance the clock by one millisecond and deliver ready messages.
    pub fn tick(&mut self) {
        self.clock.tick();
        self.deliver();
    }

    /// Deliver all messages that are ready at the current clock time.
    fn deliver(&mut self) {
        let now = self.clock.now();
        let ready = self.network.deliver_ready(now);
        for (peer_id, messages) in ready {
            if self.crashed.contains(&peer_id) {
                continue;
            }
            for msg in messages {
                self.trace_event(TraceEvent::MessageDelivered {
                    tick: now,
                    from: msg.from,
                    to: msg.to,
                });
                if let Err(e) = self.peers[peer_id].merge(&msg.dt_bytes, &msg.changes) {
                    eprintln!(
                        "MERGE ERROR: tick={now} from={} to={peer_id} bytes={} err={e}",
                        msg.from,
                        msg.dt_bytes.len(),
                    );
                }
            }
        }
    }

    /// Tick until the network is quiescent (no pending messages).
    pub fn run_until_quiescent(&mut self) {
        // Deliver anything already ready at the current tick.
        self.deliver();
        let mut guard = 0;
        while !self.network.is_quiescent() {
            self.tick();
            guard += 1;
            assert!(
                guard < MAX_QUIESCENT_TICKS,
                "run_until_quiescent exceeded {MAX_QUIESCENT_TICKS} ticks"
            );
        }
    }

    /// Create a network partition between two peers.
    pub fn partition(&mut self, a: PeerId, b: PeerId) {
        self.network.partition(a, b);
        let tick = self.clock.now();
        self.trace_event(TraceEvent::Partition { tick, a, b });
    }

    /// Heal a network partition between two peers.
    pub fn heal(&mut self, a: PeerId, b: PeerId) {
        self.network.heal(a, b);
        let tick = self.clock.now();
        self.trace_event(TraceEvent::Heal { tick, a, b });
    }

    /// Clear all network partitions.
    pub fn heal_all(&mut self) {
        self.network.heal_all();
    }

    /// Remove all per-link adversity configurations.
    pub fn clear_link_configs(&mut self) {
        self.network.clear_link_configs();
    }

    /// Persist a peer's document state to the in-memory store.
    fn persist(&mut self, peer: PeerId) {
        let snapshot = self.peers[peer].snapshot();
        self.store.save(peer, snapshot);
    }

    /// Crash a peer: persist state, drop in-flight messages, clear sync cursors.
    ///
    /// # Panics
    ///
    /// Panics if the peer is already crashed.
    pub fn crash(&mut self, peer: PeerId) {
        assert!(
            !self.crashed.contains(&peer),
            "peer {peer} is already crashed"
        );
        self.persist(peer);
        self.network.drop_messages_for(peer);
        self.last_synced
            .retain(|&(from, to), _| from != peer && to != peer);
        self.crashed.insert(peer);
        let tick = self.clock.now();
        self.trace_event(TraceEvent::Crash { tick, peer });
    }

    /// Restart a crashed peer from its last persisted snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the peer is not crashed or has no snapshot.
    pub fn restart(&mut self, peer: PeerId) {
        assert!(self.crashed.contains(&peer), "peer {peer} is not crashed");
        let snapshot = self
            .store
            .get(peer)
            .expect("no snapshot for crashed peer")
            .clone();
        let name = self.peers[peer].name.clone();

        // Increment restart count and derive a fresh RNG.
        let count = self.restart_count.entry(peer).or_insert(0);
        *count += 1;
        let rng_index = *count * RNG_RESTART_STRIDE + peer;
        let rng = peer_rng(self.seed, rng_index);
        let env = SimEnv::new(Arc::clone(&self.clock), rng);

        let restored =
            Peer::restore(&name, env, &snapshot).expect("failed to restore peer from snapshot");
        self.peers[peer] = restored;
        self.crashed.remove(&peer);
        let tick = self.clock.now();
        self.trace_event(TraceEvent::Restart { tick, peer });
    }

    /// Return whether a peer is currently crashed.
    pub fn is_crashed(&self, peer: PeerId) -> bool {
        self.crashed.contains(&peer)
    }

    /// Restart all currently crashed peers.
    pub fn restart_all_crashed(&mut self) {
        let crashed: Vec<PeerId> = self.crashed.iter().copied().collect();
        for peer in crashed {
            self.restart(peer);
        }
    }

    /// Return the number of peers in the simulation.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Return all peer ids.
    pub fn peer_ids(&self) -> Vec<PeerId> {
        (0..self.peers.len()).collect()
    }

    /// Return the content of a peer's document.
    pub fn content(&self, peer: PeerId) -> String {
        self.peers[peer].content()
    }

    /// Return the document size in characters for a peer.
    pub fn document_size(&self, peer: PeerId) -> usize {
        self.peers[peer].doc.len()
    }

    /// Collect the current state of all non-crashed peers as `(id, content)` pairs.
    fn peer_states(&self) -> Vec<(PeerId, String)> {
        (0..self.peers.len())
            .filter(|id| !self.crashed.contains(id))
            .map(|id| (id, self.peers[id].content()))
            .collect()
    }

    /// Assert that all non-crashed peers have converged to the same content.
    ///
    /// If tracing is enabled and convergence fails, dumps trace info and
    /// writes a trace file before panicking.
    ///
    /// # Panics
    ///
    /// Panics if any two non-crashed peers have different content.
    pub fn assert_converged(&self) {
        let active: Vec<PeerId> = (0..self.peers.len())
            .filter(|id| !self.crashed.contains(id))
            .collect();
        if active.len() < 2 {
            return;
        }
        let reference = self.peers[active[0]].content();
        for &id in &active[1..] {
            let content = self.peers[id].content();
            if reference != content {
                self.dump_trace_on_failure(&format!(
                    "peer '{}' diverged from peer '{}'",
                    self.peers[id].name, self.peers[active[0]].name,
                ));
                panic!(
                    "peer '{}' diverged from peer '{}'\n  '{}': {:?}\n  '{}': {:?}",
                    self.peers[id].name,
                    self.peers[active[0]].name,
                    self.peers[active[0]].name,
                    reference,
                    self.peers[id].name,
                    content,
                );
            }
        }
    }

    /// Assert that a peer's document has the expected content.
    ///
    /// # Panics
    ///
    /// Panics if the content doesn't match.
    pub fn assert_content(&self, peer: PeerId, expected: &str) {
        let actual = self.content(peer);
        if actual != expected {
            self.dump_trace_on_failure(&format!(
                "peer '{}' content mismatch: expected {:?}, got {:?}",
                self.peers[peer].name, expected, actual,
            ));
            panic!(
                "peer '{}' content mismatch: expected {:?}, got {:?}",
                self.peers[peer].name, expected, actual,
            );
        }
    }

    /// Return a reference to the peer's changes.
    pub fn changes(&self, peer: PeerId) -> &[kutl_core::Change] {
        self.peers[peer].changes()
    }

    /// If tracing is enabled, print failure info and write a trace file.
    fn dump_trace_on_failure(&self, message: &str) {
        if let Some(log) = &self.trace {
            let states = self.peer_states();
            eprintln!("{}", log.format_failure(self.seed, &states, message));
            if let Ok(path) = log.write_to_file(self.seed) {
                eprintln!("Trace written to: {}", path.display());
            }
        }
    }
}
