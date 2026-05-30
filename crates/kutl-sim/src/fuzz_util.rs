//! Shared utilities for fuzz modules.
//!
//! Both [`fuzz`](crate::fuzz) (p2p) and [`relay_fuzz`](crate::relay_fuzz) share
//! random text generation and helper functions. This module avoids duplication.

use rand::RngExt;
use rand_chacha::ChaChaRng;

/// RNG index for the fuzz coordinator (distinct from any peer/daemon index).
pub const FUZZ_RNG_INDEX: usize = 9999;

/// Number of sync-all + run-to-quiescence rounds during the convergence phase.
pub const CONVERGENCE_ROUNDS: usize = 10;

/// Maximum number of characters to delete in a single fuzz edit.
pub const MAX_DELETE_LEN: usize = 5;

/// Maximum length of randomly generated text (in characters).
pub const MAX_TEXT_LEN: usize = 20;

/// Size of the random text alphabet: a–z (26) + space + newline.
const ALPHABET_SIZE: u64 = 28;

/// Helper: random usize from RNG (uses u64 truncation, acceptable for index math).
#[allow(clippy::cast_possible_truncation)]
pub fn rand_usize(rng: &mut ChaChaRng) -> usize {
    rng.random::<u64>() as usize
}

/// Helper: random bool from RNG.
pub fn rand_bool(rng: &mut ChaChaRng) -> bool {
    rng.random::<u64>() & 1 == 0
}

/// Generate a random string of 1–`MAX_TEXT_LEN` characters from `[a-z, space, newline]`.
pub fn random_text(rng: &mut ChaChaRng) -> String {
    let len = 1 + rand_usize(rng) % MAX_TEXT_LEN;
    (0..len)
        .map(|_| {
            let roll = (rng.random::<u64>() % ALPHABET_SIZE) as u8;
            match roll {
                0..26 => (b'a' + roll) as char,
                26 => ' ',
                _ => '\n',
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prng::peer_rng;

    #[test]
    fn test_random_text_length() {
        let mut rng = peer_rng(42, 0);
        for _ in 0..100 {
            let text = random_text(&mut rng);
            let char_count = text.chars().count();
            assert!(char_count >= 1);
            assert!(char_count <= MAX_TEXT_LEN);
        }
    }

    #[test]
    fn test_random_text_alphabet() {
        let mut rng = peer_rng(99, 0);
        for _ in 0..200 {
            let text = random_text(&mut rng);
            for ch in text.chars() {
                assert!(
                    ch.is_ascii_lowercase() || ch == ' ' || ch == '\n',
                    "unexpected character: {ch:?}"
                );
            }
        }
    }
}
