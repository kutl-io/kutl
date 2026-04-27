//! Seed handling for deterministic simulation runs.

use rand::SeedableRng;
use rand_chacha::ChaChaRng;

/// Read the simulation seed from `KUTL_SIM_SEED` or generate a random one.
///
/// When a random seed is chosen, it is printed to stderr so that a failing
/// run can be reproduced.
pub fn sim_seed() -> u64 {
    if let Ok(val) = std::env::var("KUTL_SIM_SEED") {
        val.parse().expect("KUTL_SIM_SEED must be a u64")
    } else {
        let seed = rand::random::<u64>();
        eprintln!("kutl-sim: using random seed {seed}");
        seed
    }
}

/// Derive a per-peer PRNG from a base seed and peer index.
///
/// Each peer gets a distinct but deterministic stream by mixing the
/// peer index into the seed bytes.
pub fn peer_rng(base_seed: u64, peer_index: usize) -> ChaChaRng {
    let mut seed_bytes = [0u8; 32];
    seed_bytes[..8].copy_from_slice(&base_seed.to_le_bytes());
    seed_bytes[8..16].copy_from_slice(&(peer_index as u64).to_le_bytes());
    ChaChaRng::from_seed(seed_bytes)
}

/// Derive a PRNG for the virtual network, distinct from all peer PRNGs.
///
/// Uses a fixed marker in byte range `[16..24]` to ensure the network
/// RNG never collides with peer RNGs (which use bytes `[8..16]`).
pub fn net_rng(base_seed: u64) -> ChaChaRng {
    let mut seed_bytes = [0u8; 32];
    seed_bytes[..8].copy_from_slice(&base_seed.to_le_bytes());
    // Marker: "net\0" in the peer-index slot ensures no collision with peer_rng.
    seed_bytes[16..20].copy_from_slice(b"net\0");
    ChaChaRng::from_seed(seed_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_rng_deterministic() {
        use rand::RngExt;
        let mut a = peer_rng(42, 0);
        let mut b = peer_rng(42, 0);
        // Same seed + index → same output.
        let va: u64 = a.random();
        let vb: u64 = b.random();
        assert_eq!(va, vb);
    }

    #[test]
    fn test_peer_rng_distinct_per_index() {
        use rand::RngExt;
        let mut a = peer_rng(42, 0);
        let mut b = peer_rng(42, 1);
        let va: u64 = a.random();
        let vb: u64 = b.random();
        assert_ne!(va, vb);
    }
}
