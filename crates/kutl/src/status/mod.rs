//! `kutl status` — collect and render client diagnostic state.
//!
//! Per RFD 0074: works whether the daemon is up or down. All collection
//! is file-or-network from the CLI process; no IPC to the daemon.
//!
//! Submodules:
//! - [`schema`] — stable JSON-serializable types.
//! - [`collect`] — file-system collection (registry, identity, PIDs).
//! - [`probe`] — relay reachability probe (network).
//! - [`format`] — human-readable renderer.

mod collect;
mod format;
mod probe;
mod schema;

pub use collect::collect_static;
pub use format::render_human;
pub use probe::probe_relays;
