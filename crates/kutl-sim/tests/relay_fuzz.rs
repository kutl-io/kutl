//! Randomized fuzz tests for relay-mediated simulation.

use kutl_sim::relay_fuzz::{RelayFuzzConfig, run_relay_fuzz};
use kutl_sim::seed_count;

#[test]
fn relay_fuzz_default() {
    let config = RelayFuzzConfig {
        ticks: 100,
        min_daemons: 3,
        max_daemons: 5,
        disconnects: true,
    };
    let count = seed_count();
    for seed in 0..count {
        let result = run_relay_fuzz(seed, &config);
        eprintln!(
            "relay fuzz seed={seed}: daemons={}, edits={}, content_len={}",
            result.daemon_count,
            result.edit_count,
            result.final_content.len(),
        );
    }
}

#[test]
fn relay_fuzz_no_disconnects() {
    let config = RelayFuzzConfig {
        ticks: 100,
        min_daemons: 3,
        max_daemons: 5,
        disconnects: false,
    };
    let count = seed_count();
    for seed in 0..count {
        run_relay_fuzz(seed, &config);
    }
}

#[test]
fn relay_fuzz_many_daemons() {
    let config = RelayFuzzConfig {
        ticks: 100,
        min_daemons: 5,
        max_daemons: 8,
        disconnects: true,
    };
    let count = seed_count();
    for seed in 0..count {
        run_relay_fuzz(seed, &config);
    }
}
