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
pub(crate) use reconcile::reconcile_placement;
pub use shadow::{DiskShadow, Occupant};
pub use state::{FileIdentity, SpaceState};

/// Convert a relative path to a forward-slash string for use in wire messages
/// and case-folded map keys.
///
/// Uses the component-by-component approach so the result is portable across
/// platforms regardless of the OS path separator. Replaces the former inline
/// conversion in `daemon.rs` (shared here so `shadow.rs` and `daemon.rs` use
/// the same canonical form — the driver imports this helper).
pub(crate) fn rel_path_to_string(rel_path: &std::path::Path) -> String {
    rel_path
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}
