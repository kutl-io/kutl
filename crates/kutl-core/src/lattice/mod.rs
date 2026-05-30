//! State-based CRDT types — join semilattices for lifecycle state.
//!
//! Every type implements [`Lattice`], guaranteeing commutative, associative,
//! and idempotent merge. Property-based tests verify the semilattice laws.

mod content_state;
mod doc_record;
mod eviction_state;
mod subscriber_map;

pub use content_state::{ContentKind, ContentState};
pub use doc_record::DocRecord;
pub use eviction_state::{DEFAULT_EVICTION_GRACE_MS, EvictionState};
pub use subscriber_map::{SubscriberEntry, SubscriberMap};

/// Unique identifier for a WebSocket connection.
pub type ConnId = u64;

/// State-based CRDT: join semilattice.
///
/// `merge()` computes the least upper bound (⊔). Must be commutative,
/// associative, and idempotent. `lattice_le()` provides the lattice
/// partial order (⊑).
///
/// Uses a dedicated `lattice_le` method instead of `PartialOrd` because
/// lattice ordering compares only timestamps/counters, while `PartialEq`
/// (derived) compares all fields including associated data like paths and
/// versions. Rust requires `PartialOrd`/`PartialEq` consistency, so a
/// separate method avoids that conflict.
pub trait Lattice: Clone + PartialEq + std::fmt::Debug {
    /// Least upper bound — merge `other` into `self`.
    fn merge(&mut self, other: &Self);

    /// Returns true if `self ⊑ other` in the lattice partial order.
    fn lattice_le(&self, other: &Self) -> bool;
}
