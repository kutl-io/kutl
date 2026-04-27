//! Randomized fuzz testing for the relay-mediated simulation.
//!
//! Mirrors [`fuzz`](crate::fuzz) but operates on [`RelaySimulation`] instead of
//! peer-to-peer [`Simulation`](crate::Simulation). All communication goes
//! through the relay, and actions include disconnect/reconnect to test
//! session lifecycle.
//!
//! Network-level partitions are deliberately not used here: they break the
//! relay's `known_version` tracking (the relay advances it on successful
//! `try_send` to the mpsc channel, but the virtual network may then drop
//! the message). Disconnect/reconnect correctly handles this by cleaning up
//! the subscription at the relay level.

use rand::RngExt;
use rand_chacha::ChaChaRng;

use crate::fuzz_util::{
    CONVERGENCE_ROUNDS, FUZZ_RNG_INDEX, MAX_DELETE_LEN, rand_bool, rand_usize, random_text,
};
use crate::prng::peer_rng;
use crate::relay_sim::{DaemonId, RelaySimulation};

// --- Action selection weights (cumulative upper bounds out of 100) ---
// Edit: 40%, SyncDaemon: 20%, SyncAll: 10%, Disconnect: 15%, Reconnect: 15%.

const ACTION_WEIGHT_TOTAL: u64 = 100;
const ACTION_EDIT_UPPER: u64 = 40;
const ACTION_SYNC_DAEMON_UPPER: u64 = 60;
const ACTION_SYNC_ALL_UPPER: u64 = 70;
const ACTION_DISCONNECT_UPPER: u64 = 85;

/// An action the fuzzer can take on each tick.
#[derive(Debug, Clone)]
enum FuzzAction {
    Edit { daemon: DaemonId },
    SyncDaemon { daemon: DaemonId },
    SyncAll,
    Disconnect { daemon: DaemonId },
    Reconnect { daemon: DaemonId },
}

/// Configuration for a relay fuzz run.
///
/// Unlike the p2p [`FuzzConfig`](crate::fuzz::FuzzConfig), there is no
/// `adversity` option. Link-level adversity (latency, loss, reorder) is
/// incompatible with the relay's `known_version` tracking and the simulation's
/// `last_sent` cursor: message reordering causes later messages to reference
/// base versions from earlier messages that the relay hasn't received yet.
/// Use `disconnects` for disruption testing instead.
#[derive(Debug, Clone)]
pub struct RelayFuzzConfig {
    /// Number of ticks to simulate.
    pub ticks: u64,
    /// Minimum number of daemons.
    pub min_daemons: usize,
    /// Maximum number of daemons.
    pub max_daemons: usize,
    /// Whether to allow disconnect/reconnect actions.
    pub disconnects: bool,
}

impl Default for RelayFuzzConfig {
    fn default() -> Self {
        Self {
            ticks: 200,
            min_daemons: 3,
            max_daemons: 8,
            disconnects: true,
        }
    }
}

/// Result of a relay fuzz run.
#[derive(Debug)]
pub struct RelayFuzzResult {
    /// The seed used for this run.
    pub seed: u64,
    /// Number of daemons created.
    pub daemon_count: usize,
    /// Number of edits performed.
    pub edit_count: usize,
    /// Final converged content.
    pub final_content: String,
}

/// Run a single relay fuzz iteration with the given seed and config.
///
/// # Panics
///
/// Panics if convergence fails after the run completes.
pub fn run_relay_fuzz(seed: u64, config: &RelayFuzzConfig) -> RelayFuzzResult {
    let mut rng = peer_rng(seed, FUZZ_RNG_INDEX);
    let mut sim = RelaySimulation::new(seed);

    let (daemons, n) = setup_daemons(&mut rng, &mut sim, config);

    // Seed initial content: first daemon writes, sync to all.
    sim.insert(daemons[0], 0, "init");
    sim.sync_all_daemons();
    sim.run_until_quiescent();

    let edit_count = run_fuzz_loop(&mut rng, &mut sim, &daemons, config);

    // Convergence phase.
    converge(&mut sim);

    let final_content = sim.content(daemons[0]);

    RelayFuzzResult {
        seed,
        daemon_count: n,
        edit_count: edit_count + 1, // +1 for the seed edit
        final_content,
    }
}

/// Create daemons for the simulation.
fn setup_daemons(
    rng: &mut ChaChaRng,
    sim: &mut RelaySimulation,
    config: &RelayFuzzConfig,
) -> (Vec<DaemonId>, usize) {
    let range = config.max_daemons - config.min_daemons;
    let n = if range > 0 {
        config.min_daemons + (rand_usize(rng) % (range + 1))
    } else {
        config.min_daemons
    };
    for i in 0..n {
        sim.add_daemon(&format!("daemon-{i}"), "fuzz-space", "fuzz-doc")
            .unwrap();
    }
    (sim.daemon_ids(), n)
}

/// Execute the main fuzz loop, returning the number of successful edits.
fn run_fuzz_loop(
    rng: &mut ChaChaRng,
    sim: &mut RelaySimulation,
    daemons: &[DaemonId],
    config: &RelayFuzzConfig,
) -> usize {
    let mut edit_count = 0;

    for _tick in 0..config.ticks {
        let action = pick_action(rng, daemons, sim, config);

        match action {
            FuzzAction::Edit { daemon } => {
                if sim.is_connected(daemon) && apply_edit(rng, sim, daemon) {
                    edit_count += 1;
                }
            }
            FuzzAction::SyncDaemon { daemon } => {
                sim.sync_daemon(daemon);
            }
            FuzzAction::SyncAll => {
                sim.sync_all_daemons();
            }
            FuzzAction::Disconnect { daemon } => {
                if sim.is_connected(daemon) {
                    let connected = daemons.iter().filter(|&&d| sim.is_connected(d)).count();
                    if connected > 1 {
                        sim.disconnect_daemon(daemon);
                    }
                }
            }
            FuzzAction::Reconnect { daemon } => {
                if !sim.is_connected(daemon) {
                    sim.reconnect_daemon(daemon);
                }
            }
        }

        sim.tick();
    }

    edit_count
}

/// Apply a random edit (insert or delete) to a daemon. Returns true if successful.
fn apply_edit(rng: &mut ChaChaRng, sim: &mut RelaySimulation, daemon: DaemonId) -> bool {
    let doc_len = sim.document_size(daemon);
    let text = random_text(rng);

    if doc_len > 0 && rand_bool(rng) {
        // Delete.
        let start = rand_usize(rng) % doc_len;
        let max_del = (doc_len - start).min(MAX_DELETE_LEN);
        let del_len = if max_del > 0 {
            1 + rand_usize(rng) % max_del
        } else {
            1
        };
        let end = (start + del_len).min(doc_len);
        if start < end {
            sim.delete(daemon, start, end - start);
            true
        } else {
            false
        }
    } else {
        // Insert.
        let pos = if doc_len > 0 {
            rand_usize(rng) % (doc_len + 1)
        } else {
            0
        };
        sim.insert(daemon, pos, &text);
        true
    }
}

/// Reconnect all disconnected daemons and sync until convergence.
fn converge(sim: &mut RelaySimulation) {
    // Reconnect any disconnected daemons.
    let ids = sim.daemon_ids();
    for &id in &ids {
        if !sim.is_connected(id) {
            sim.reconnect_daemon(id);
        }
    }

    for _ in 0..CONVERGENCE_ROUNDS {
        sim.sync_all_daemons();
        sim.run_until_quiescent();
    }
    sim.assert_converged();
}

/// Pick a random action weighted toward edits.
fn pick_action(
    rng: &mut ChaChaRng,
    daemons: &[DaemonId],
    sim: &RelaySimulation,
    config: &RelayFuzzConfig,
) -> FuzzAction {
    let n = daemons.len();
    let roll = rng.random::<u64>() % ACTION_WEIGHT_TOTAL;

    match roll {
        0..ACTION_EDIT_UPPER => FuzzAction::Edit {
            daemon: daemons[rand_usize(rng) % n],
        },
        ACTION_EDIT_UPPER..ACTION_SYNC_DAEMON_UPPER => FuzzAction::SyncDaemon {
            daemon: daemons[rand_usize(rng) % n],
        },
        ACTION_SYNC_DAEMON_UPPER..ACTION_SYNC_ALL_UPPER => FuzzAction::SyncAll,
        ACTION_SYNC_ALL_UPPER..ACTION_DISCONNECT_UPPER => {
            pick_disconnect_action(rng, daemons, sim, config)
        }
        _ => pick_reconnect_action(rng, daemons, sim, config),
    }
}

/// Pick a disconnect action or fall back to edit if not possible.
fn pick_disconnect_action(
    rng: &mut ChaChaRng,
    daemons: &[DaemonId],
    sim: &RelaySimulation,
    config: &RelayFuzzConfig,
) -> FuzzAction {
    let n = daemons.len();
    if !config.disconnects {
        return FuzzAction::Edit {
            daemon: daemons[rand_usize(rng) % n],
        };
    }
    let daemon = daemons[rand_usize(rng) % n];
    let connected = daemons.iter().filter(|&&d| sim.is_connected(d)).count();
    if sim.is_connected(daemon) && connected > 1 {
        FuzzAction::Disconnect { daemon }
    } else {
        FuzzAction::Edit {
            daemon: *daemons
                .iter()
                .find(|&&d| sim.is_connected(d))
                .expect("at least one connected daemon must exist"),
        }
    }
}

/// Pick a reconnect action or fall back to sync if not possible.
fn pick_reconnect_action(
    rng: &mut ChaChaRng,
    daemons: &[DaemonId],
    sim: &RelaySimulation,
    config: &RelayFuzzConfig,
) -> FuzzAction {
    let n = daemons.len();
    if !config.disconnects {
        return FuzzAction::SyncDaemon {
            daemon: daemons[rand_usize(rng) % n],
        };
    }
    let daemon = daemons[rand_usize(rng) % n];
    if sim.is_connected(daemon) {
        FuzzAction::SyncDaemon { daemon }
    } else {
        FuzzAction::Reconnect { daemon }
    }
}
