//! Deterministic workload generators for CRDT benchmarks.

use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;

/// Threshold out of 100 for insert vs delete in mixed workloads.
const INSERT_THRESHOLD: u32 = 70;

/// Alphabet used for generated text in all workloads.
const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz ";

/// Returns the alphabet as a `Vec<char>`.
fn alphabet() -> Vec<char> {
    ALPHABET.chars().collect()
}

/// A single editing operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EditOp {
    /// Insert text at the given position.
    Insert { pos: usize, text: String },
    /// Delete `len` characters starting at `pos`.
    Delete { pos: usize, len: usize },
}

/// Apply a sequence of `EditOp`s to any `BenchEngine`.
pub fn apply_ops<E: super::adapters::BenchEngine>(ops: &[EditOp]) -> E {
    let mut engine = E::new();
    for op in ops {
        match op {
            EditOp::Insert { pos, text } => engine.insert(*pos, text),
            EditOp::Delete { pos, len } => engine.delete(*pos, *len),
        }
    }
    engine
}

/// Sequential single-character appends at the end of the document.
/// Simulates a user typing linearly.
pub fn sequential_chars(n: usize) -> Vec<EditOp> {
    let chars = alphabet();
    (0..n)
        .map(|i| EditOp::Insert {
            pos: i,
            text: chars[i % chars.len()].to_string(),
        })
        .collect()
}

/// Single-character inserts at random valid positions.
pub fn random_inserts(n: usize, seed: u64) -> Vec<EditOp> {
    let mut rng = StdRng::seed_from_u64(seed);
    let chars = alphabet();
    let mut doc_len: usize = 0;

    (0..n)
        .map(|_| {
            let pos = if doc_len == 0 {
                0
            } else {
                rng.random_range(0..=doc_len)
            };
            let ch = chars[rng.random_range(0..chars.len())];
            doc_len += 1;
            EditOp::Insert {
                pos,
                text: ch.to_string(),
            }
        })
        .collect()
}

/// Insert at position 0 repeatedly. Worst case for position lookups in
/// array-backed data structures.
pub fn front_insert(n: usize) -> Vec<EditOp> {
    let chars = alphabet();
    (0..n)
        .map(|i| EditOp::Insert {
            pos: 0,
            text: chars[i % chars.len()].to_string(),
        })
        .collect()
}

/// Mixed inserts (70%) and deletes (30%) at random positions.
pub fn mixed_edit(n: usize, seed: u64) -> Vec<EditOp> {
    let mut rng = StdRng::seed_from_u64(seed);
    let chars = alphabet();
    let mut doc_len: usize = 0;

    (0..n)
        .map(|_| {
            let do_insert = doc_len == 0 || rng.random_range(0..100) < INSERT_THRESHOLD;
            if do_insert {
                let pos = if doc_len == 0 {
                    0
                } else {
                    rng.random_range(0..=doc_len)
                };
                let ch = chars[rng.random_range(0..chars.len())];
                doc_len += 1;
                EditOp::Insert {
                    pos,
                    text: ch.to_string(),
                }
            } else {
                let pos = rng.random_range(0..doc_len);
                doc_len -= 1;
                EditOp::Delete { pos, len: 1 }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_chars_length() {
        let ops = sequential_chars(100);
        assert_eq!(ops.len(), 100);
    }

    #[test]
    fn test_random_inserts_deterministic() {
        let a = random_inserts(50, 42);
        let b = random_inserts(50, 42);
        assert_eq!(a, b);
    }

    #[test]
    fn test_mixed_edit_produces_valid_ops() {
        use crate::adapters::BenchEngine;

        // Apply the ops to a DtEngine and verify it doesn't panic.
        let ops = mixed_edit(200, 99);
        let engine = apply_ops::<crate::adapters::DtEngine>(&ops);
        // After 200 mixed ops we should have some content.
        assert!(!engine.is_empty());
    }
}
