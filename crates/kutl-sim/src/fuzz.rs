//! Randomized fuzz testing for the simulation framework.
//!
//! Generates random sequences of edits, syncs, partitions, crashes, and
//! restarts to explore the state space that hand-written tests cannot.

use rand::RngExt;
use rand_chacha::ChaChaRng;

use kutl_core::Boundary;

use crate::fuzz_util::{
    CONVERGENCE_ROUNDS, FUZZ_RNG_INDEX, MAX_DELETE_LEN, rand_bool, rand_usize, random_text,
};
use crate::link::LinkConfig;
use crate::prng::peer_rng;
use crate::{PeerId, Simulation};

// --- Action selection weights (cumulative upper bounds out of 100) ---
// Edit: 40%, Sync: 30%, Partition: 10%, Heal: 10%, Crash: 5%, Restart: 5%.

const ACTION_WEIGHT_TOTAL: u64 = 100;
const ACTION_EDIT_UPPER: u64 = 40;
const ACTION_SYNC_UPPER: u64 = 70;
const ACTION_PARTITION_UPPER: u64 = 80;
const ACTION_HEAL_UPPER: u64 = 90;
const ACTION_CRASH_UPPER: u64 = 95;

// --- Adversity setup ---

/// Probability that a given directed link gets adversity configured.
const ADVERSITY_LINK_PROBABILITY: f64 = 0.3;
/// Maximum latency start value (ticks).
const ADVERSITY_MAX_LATENCY_START: u64 = 10;
/// Maximum additional latency range (ticks).
const ADVERSITY_MAX_LATENCY_RANGE: u64 = 20;
/// Maximum loss percentage (values 0..N map to 0%..N%).
const ADVERSITY_MAX_LOSS_PCT: u64 = 15;
/// Maximum reorder percentage (values 0..N map to 0%..N%).
const ADVERSITY_MAX_REORDER_PCT: u64 = 20;

/// An action the fuzzer can take on each tick.
#[derive(Debug, Clone)]
enum FuzzAction {
    Edit { peer: PeerId },
    Sync { from: PeerId, to: PeerId },
    Partition { a: PeerId, b: PeerId },
    Heal { a: PeerId, b: PeerId },
    Crash { peer: PeerId },
    Restart { peer: PeerId },
}

/// Configuration for a fuzz run.
#[derive(Debug, Clone)]
pub struct FuzzConfig {
    /// Number of ticks to simulate.
    pub ticks: u64,
    /// Minimum number of peers.
    pub min_peers: usize,
    /// Maximum number of peers.
    pub max_peers: usize,
    /// Whether to configure random link adversity.
    pub adversity: bool,
    /// Whether to allow crash/restart actions.
    pub crashes: bool,
}

impl Default for FuzzConfig {
    fn default() -> Self {
        Self {
            ticks: 200,
            min_peers: 3,
            max_peers: 8,
            adversity: true,
            crashes: true,
        }
    }
}

/// Result of a fuzz run.
#[derive(Debug)]
pub struct FuzzResult {
    /// The seed used for this run.
    pub seed: u64,
    /// Number of peers created.
    pub peer_count: usize,
    /// Number of edits performed.
    pub edit_count: usize,
    /// Final converged content.
    pub final_content: String,
}

/// Helper: random small integer as f64 (values are small enough that precision is fine).
#[allow(clippy::cast_precision_loss)]
fn rand_small_f64(rng: &mut ChaChaRng, modulus: u64) -> f64 {
    (rng.random::<u64>() % modulus) as f64
}

/// Run a single fuzz iteration with the given seed and config.
///
/// # Panics
///
/// Panics if convergence fails after the run completes.
pub fn run_fuzz(seed: u64, config: &FuzzConfig) -> FuzzResult {
    let mut rng = peer_rng(seed, FUZZ_RNG_INDEX);
    let mut sim = Simulation::new(seed).with_tracing();

    let (peers, n) = setup_peers(&mut rng, &mut sim, config);
    setup_adversity(&mut rng, &mut sim, &peers, config);

    // Seed initial content: first peer writes, sync to all.
    sim.edit(peers[0], "fuzz seed", Boundary::Auto, |ctx| {
        ctx.insert(0, "init")
    })
    .expect("seed edit on empty document should succeed");
    sim.sync_all();
    sim.run_until_quiescent();

    let edit_count = run_fuzz_loop(&mut rng, &mut sim, &peers, config);

    // Convergence phase.
    converge(&mut sim);

    let final_content = sim.content(peers[0]);

    FuzzResult {
        seed,
        peer_count: n,
        edit_count: edit_count + 1, // +1 for the seed edit
        final_content,
    }
}

/// Create peers for the simulation.
fn setup_peers(
    rng: &mut ChaChaRng,
    sim: &mut Simulation,
    config: &FuzzConfig,
) -> (Vec<PeerId>, usize) {
    let range = config.max_peers - config.min_peers;
    let n = if range > 0 {
        config.min_peers + (rand_usize(rng) % (range + 1))
    } else {
        config.min_peers
    };
    for i in 0..n {
        sim.add_peer(&format!("peer-{i}")).unwrap();
    }
    (sim.peer_ids(), n)
}

/// Optionally configure random link adversity.
fn setup_adversity(
    rng: &mut ChaChaRng,
    sim: &mut Simulation,
    peers: &[PeerId],
    config: &FuzzConfig,
) {
    if !config.adversity {
        return;
    }
    for &from in peers {
        for &to in peers {
            if from != to {
                let roll: f64 = rng.random();
                if roll < ADVERSITY_LINK_PROBABILITY {
                    let latency_start =
                        (rng.random::<u64>() % ADVERSITY_MAX_LATENCY_START).cast_signed();
                    let latency_end = latency_start
                        + (rng.random::<u64>() % ADVERSITY_MAX_LATENCY_RANGE).cast_signed();
                    let loss = rand_small_f64(rng, ADVERSITY_MAX_LOSS_PCT) / 100.0;
                    let reorder = rand_small_f64(rng, ADVERSITY_MAX_REORDER_PCT) / 100.0;
                    sim.configure_link(
                        from,
                        to,
                        LinkConfig {
                            latency: latency_start..latency_end,
                            loss,
                            reorder,
                        },
                    );
                }
            }
        }
    }
}

/// Execute the main fuzz loop, returning the number of successful edits.
fn run_fuzz_loop(
    rng: &mut ChaChaRng,
    sim: &mut Simulation,
    peers: &[PeerId],
    config: &FuzzConfig,
) -> usize {
    let mut edit_count = 0;

    for tick in 0..config.ticks {
        let action = pick_action(rng, peers, sim, config);

        match action {
            FuzzAction::Edit { peer } => {
                if !sim.is_crashed(peer) && apply_edit(rng, sim, peer, tick) {
                    edit_count += 1;
                }
            }
            FuzzAction::Sync { from, to } => {
                sim.sync(from, to);
            }
            FuzzAction::Partition { a, b } => sim.partition(a, b),
            FuzzAction::Heal { a, b } => sim.heal(a, b),
            FuzzAction::Crash { peer } => {
                if !sim.is_crashed(peer) {
                    sim.crash(peer);
                }
            }
            FuzzAction::Restart { peer } => {
                if sim.is_crashed(peer) {
                    sim.restart(peer);
                }
            }
        }

        sim.tick();
    }

    edit_count
}

/// Apply a random edit (insert or delete) to a peer. Returns true if successful.
fn apply_edit(rng: &mut ChaChaRng, sim: &mut Simulation, peer: PeerId, tick: u64) -> bool {
    let doc_len = sim.document_size(peer);
    let intent = format!("fuzz-edit-{tick}");
    let text = random_text(rng);

    let result = if doc_len > 0 && rand_bool(rng) {
        let start = rand_usize(rng) % doc_len;
        let max_del = (doc_len - start).min(MAX_DELETE_LEN);
        let del_len = if max_del > 0 {
            1 + rand_usize(rng) % max_del
        } else {
            1
        };
        let end = (start + del_len).min(doc_len);
        if start < end {
            sim.edit(peer, &intent, Boundary::Auto, |ctx| ctx.delete(start..end))
        } else {
            Ok(())
        }
    } else {
        let pos = if doc_len > 0 {
            rand_usize(rng) % (doc_len + 1)
        } else {
            0
        };
        sim.edit(peer, &intent, Boundary::Auto, |ctx| ctx.insert(pos, &text))
    };

    result.is_ok()
}

/// Heal all partitions, restart all crashed peers, reset link adversity, and sync until convergence.
fn converge(sim: &mut Simulation) {
    sim.heal_all();
    sim.restart_all_crashed();
    sim.clear_link_configs();
    for _ in 0..CONVERGENCE_ROUNDS {
        sim.sync_all();
        sim.run_until_quiescent();
    }
    sim.assert_converged();
}

/// Pick a random action weighted toward edits and syncs.
fn pick_action(
    rng: &mut ChaChaRng,
    peers: &[PeerId],
    sim: &Simulation,
    config: &FuzzConfig,
) -> FuzzAction {
    let n = peers.len();
    let roll = rng.random::<u64>() % ACTION_WEIGHT_TOTAL;

    match roll {
        0..ACTION_EDIT_UPPER => FuzzAction::Edit {
            peer: peers[rand_usize(rng) % n],
        },
        ACTION_EDIT_UPPER..ACTION_SYNC_UPPER => {
            let from = peers[rand_usize(rng) % n];
            let to = peers[rand_usize(rng) % n];
            let to = if from == to {
                peers[(from + 1) % n]
            } else {
                to
            };
            FuzzAction::Sync { from, to }
        }
        ACTION_SYNC_UPPER..ACTION_PARTITION_UPPER => {
            let a = peers[rand_usize(rng) % n];
            let b = peers[(a + 1 + rand_usize(rng) % (n - 1)) % n];
            FuzzAction::Partition { a, b }
        }
        ACTION_PARTITION_UPPER..ACTION_HEAL_UPPER => {
            let a = peers[rand_usize(rng) % n];
            let b = peers[(a + 1 + rand_usize(rng) % (n - 1)) % n];
            FuzzAction::Heal { a, b }
        }
        ACTION_HEAL_UPPER..ACTION_CRASH_UPPER => pick_crash_action(rng, peers, sim, config),
        _ => pick_restart_action(rng, peers, sim, config),
    }
}

/// Pick a crash action or fall back to edit if crash not possible.
fn pick_crash_action(
    rng: &mut ChaChaRng,
    peers: &[PeerId],
    sim: &Simulation,
    config: &FuzzConfig,
) -> FuzzAction {
    if !config.crashes {
        return FuzzAction::Edit {
            peer: peers[rand_usize(rng) % peers.len()],
        };
    }
    let peer = peers[rand_usize(rng) % peers.len()];
    let active_count = peers.iter().filter(|&&p| !sim.is_crashed(p)).count();
    if !sim.is_crashed(peer) && active_count > 1 {
        FuzzAction::Crash { peer }
    } else {
        FuzzAction::Edit {
            peer: *peers
                .iter()
                .find(|&&p| !sim.is_crashed(p))
                .expect("at least one active peer must exist"),
        }
    }
}

/// Pick a restart action or fall back to sync if restart not possible.
fn pick_restart_action(
    rng: &mut ChaChaRng,
    peers: &[PeerId],
    sim: &Simulation,
    config: &FuzzConfig,
) -> FuzzAction {
    let n = peers.len();
    if !config.crashes {
        return FuzzAction::Sync {
            from: peers[rand_usize(rng) % n],
            to: peers[rand_usize(rng) % n],
        };
    }
    let peer = peers[rand_usize(rng) % n];
    if sim.is_crashed(peer) {
        FuzzAction::Restart { peer }
    } else {
        FuzzAction::Sync {
            from: peer,
            to: peers[(peer + 1) % n],
        }
    }
}
