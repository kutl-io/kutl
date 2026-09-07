//! Pure, sans-IO daemon core: `(SpaceState, Event) -> Vec<Effect>`.
//!
//! Nothing in this module reads a clock, touches the filesystem, or holds a
//! channel handle. Those operations belong in the driver (`daemon.rs`) and at
//! the simulation edge (`kutl-sim`).

pub mod effect;
pub mod event;
pub mod handle;
pub mod reconcile;
pub mod shadow;
pub mod state;

pub use effect::{Effect, EffectResult, PlaceKind};
pub use event::{Event, EventStamp};
pub use handle::DaemonCore;
pub use reconcile::desired_assignment;
// `reconcile_placement` is public for the sim's modeled drain-edge probe —
// the same single-implementation rule as `sync_persisted`.
pub use reconcile::reconcile_placement;
pub(crate) use reconcile::reconcile_unless_backlogged;
pub use shadow::{DiskShadow, Occupant};
pub use state::{FileIdentity, IdentityIndexes, SpaceState};

/// Convert a relative path to a forward-slash string for use in wire messages
/// and case-folded map keys.
///
/// Uses the component-by-component approach so the result is portable across
/// platforms regardless of the OS path separator. Replaces the former inline
/// conversion in `daemon.rs` (shared here so `shadow.rs` and `daemon.rs` use
/// the same canonical form — the driver imports this helper).
pub fn rel_path_to_string(rel_path: &std::path::Path) -> String {
    rel_path
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Case-fold a relative path to a map key: the canonical wire form
/// ([`rel_path_to_string`], so a path reaches the same key whichever
/// separator form its `PathBuf` carries), then the shared
/// `kutl_core::lattice::fold_path` case rule. THE one path fold in this
/// crate — the identity index, the shadow occupant map, and the
/// case-collision scan all key by it, so no two surfaces can disagree with
/// each other or with the relay's arbitration about which paths collide.
/// Public for the simulator, which keys the same maps.
pub fn casefold(p: &std::path::Path) -> String {
    kutl_core::lattice::fold_path(&rel_path_to_string(p))
}
