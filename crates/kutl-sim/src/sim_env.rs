//! Simulation [`Env`] implementation with virtual clock and deterministic IDs.

use std::sync::{Arc, Mutex};

use rand::RngExt;
use rand_chacha::ChaChaRng;
use uuid::Uuid;

use kutl_core::Env;

use crate::clock::VirtualClock;

/// A deterministic [`Env`] for simulation testing.
///
/// Uses a shared [`VirtualClock`] for time and a seeded PRNG for ID generation.
#[derive(Debug)]
pub struct SimEnv {
    clock: Arc<VirtualClock>,
    rng: Mutex<ChaChaRng>,
}

impl SimEnv {
    /// Create a new `SimEnv` with the given clock and PRNG.
    pub fn new(clock: Arc<VirtualClock>, rng: ChaChaRng) -> Arc<Self> {
        Arc::new(Self {
            clock,
            rng: Mutex::new(rng),
        })
    }
}

impl Env for SimEnv {
    fn now_millis(&self) -> i64 {
        self.clock.now()
    }

    fn gen_id(&self) -> Uuid {
        let mut rng = self.rng.lock().expect("SimEnv rng lock poisoned");
        let mut bytes = [0u8; 16];
        rng.fill(&mut bytes);

        // Set UUID v4 version and RFC 4122 variant bits.
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;

        Uuid::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prng::peer_rng;

    #[test]
    fn test_sim_env_uses_virtual_clock() {
        let clock = VirtualClock::new(5000);
        let env = SimEnv::new(Arc::clone(&clock), peer_rng(1, 0));
        assert_eq!(env.now_millis(), 5000);
        clock.tick();
        assert_eq!(env.now_millis(), 5001);
    }

    #[test]
    fn test_sim_env_deterministic_ids() {
        let clock = VirtualClock::new(0);
        let env_a = SimEnv::new(Arc::clone(&clock), peer_rng(42, 0));
        let env_b = SimEnv::new(Arc::clone(&clock), peer_rng(42, 0));

        let id_a = env_a.gen_id();
        let id_b = env_b.gen_id();
        assert_eq!(id_a, id_b);
    }
}
