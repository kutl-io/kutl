//! State-based CRDT types for lifecycle state.
//!
//! [`DocRecord`] implements [`Lattice`], guaranteeing commutative,
//! associative, and idempotent merge. Property-based tests verify the
//! semilattice laws. The other types here are plain relay-local state.

mod content_state;
mod doc_record;
mod eviction_state;
mod registry;
mod subscriber_map;

pub use content_state::{ContentKind, ContentState};
pub use doc_record::{
    CONFLICT_INFIX, DocRecord, LifecycleProjection, conflict_path, intended_from_conflict_path,
};
pub use eviction_state::{DEFAULT_EVICTION_GRACE_MS, EvictionState};
pub use registry::RegistryLattice;
pub use subscriber_map::{SubscriberEntry, SubscriberMap};

/// Unique identifier for a WebSocket connection.
pub type ConnId = u64;

/// State-based CRDT: join semilattice.
///
/// `merge()` computes the least upper bound (⊔). Must be commutative,
/// associative, and idempotent.
pub trait Lattice: Clone + PartialEq + std::fmt::Debug {
    /// Least upper bound — merge `other` into `self`.
    fn merge(&mut self, other: &Self);
}
