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
    /// Whether to allow crash/restart actions.
    pub crashes: bool,
}

/// Run a single fuzz iteration with the given seed and config.
///
/// # Panics
///
/// Panics if convergence fails after the run completes.
pub fn run_fuzz(seed: u64, config: &FuzzConfig) {
    let mut rng = peer_rng(seed, FUZZ_RNG_INDEX);
    let mut sim = Simulation::new(seed).with_tracing();

    let peers = setup_peers(&mut rng, &mut sim, config);

    // Seed initial content: first peer writes, sync to all.
    sim.edit(peers[0], "fuzz seed", Boundary::Auto, |ctx| {
        ctx.insert(0, "init")
    })
    .expect("seed edit on empty document should succeed");
    sim.sync_all();
    sim.run_until_quiescent();

    run_fuzz_loop(&mut rng, &mut sim, &peers, config);

    // Convergence phase.
    converge(&mut sim);
}

/// Create peers for the simulation.
fn setup_peers(rng: &mut ChaChaRng, sim: &mut Simulation, config: &FuzzConfig) -> Vec<PeerId> {
    let range = config.max_peers - config.min_peers;
    let n = if range > 0 {
        config.min_peers + (rand_usize(rng) % (range + 1))
    } else {
        config.min_peers
    };
    for i in 0..n {
        sim.add_peer(&format!("peer-{i}")).unwrap();
    }
    sim.peer_ids()
}

/// Execute the main fuzz loop.
fn run_fuzz_loop(rng: &mut ChaChaRng, sim: &mut Simulation, peers: &[PeerId], config: &FuzzConfig) {
    for tick in 0..config.ticks {
        let action = pick_action(rng, peers, sim, config);

        match action {
            FuzzAction::Edit { peer } => {
                if !sim.is_crashed(peer) {
                    apply_edit(rng, sim, peer, tick);
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
}

/// Apply a random edit (insert or delete) to a peer. Fuzz edits are
/// best-effort: an edit the engine rejects is intentionally dropped (the
/// convergence oracle at the end of the run is the correctness check).
fn apply_edit(rng: &mut ChaChaRng, sim: &mut Simulation, peer: PeerId, tick: u64) {
    let doc_len = sim.document_size(peer);
    let intent = format!("fuzz-edit-{tick}");
    let text = random_text(rng);

    if doc_len > 0 && rand_bool(rng) {
        let start = rand_usize(rng) % doc_len;
        let max_del = (doc_len - start).min(MAX_DELETE_LEN);
        let del_len = if max_del > 0 {
            1 + rand_usize(rng) % max_del
        } else {
            1
        };
        let end = (start + del_len).min(doc_len);
        if start < end {
            let _ = sim.edit(peer, &intent, Boundary::Auto, |ctx| ctx.delete(start..end));
        }
    } else {
        let pos = if doc_len > 0 {
            rand_usize(rng) % (doc_len + 1)
        } else {
            0
        };
        let _ = sim.edit(peer, &intent, Boundary::Auto, |ctx| ctx.insert(pos, &text));
    }
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
