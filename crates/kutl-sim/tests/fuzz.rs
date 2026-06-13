//! Randomized fuzz tests for the simulation framework.

use kutl_sim::fuzz::{FuzzConfig, run_fuzz};
use kutl_sim::seed_count;

#[test]
fn fuzz_no_adversity() {
    let config = FuzzConfig {
        ticks: 100,
        min_peers: 3,
        max_peers: 5,
        adversity: false,
        crashes: false,
    };
    let count = seed_count();
    for seed in 0..count {
        run_fuzz(seed, &config);
    }
}

#[test]
fn fuzz_with_crashes_only() {
    let config = FuzzConfig {
        ticks: 100,
        min_peers: 3,
        max_peers: 5,
        adversity: false,
        crashes: true,
    };
    let count = seed_count();
    for seed in 0..count {
        run_fuzz(seed, &config);
    }
}
