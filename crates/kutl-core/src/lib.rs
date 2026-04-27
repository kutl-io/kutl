//! kutl-core — sync engine, traits, and shared types.

mod change;
mod document;
mod engine;
pub mod env;
mod error;
pub mod lattice;

/// Maximum CRDT operations per document. Post-merge enforcement — bounded
/// overshoot per merge by [`MAX_PATCH_BYTES`]. See RFD 0063 §3.
pub const MAX_OPS_PER_DOC: usize = 100_000;

/// Maximum bytes per merge patch. At ≥10 bytes/op (post RLE-break),
/// bounds ops-per-merge to ~100 000 so worst-case cap overshoot is ≤2×.
/// Legitimate text edits are sub-KB; bulk CRDT ingest fits comfortably.
pub const MAX_PATCH_BYTES: usize = 1_000_000;

pub use change::{Boundary, Change, ChangeList, VersionSpan, new_change, span_start};
pub use diamond_types::AgentId;
pub use document::{Document, EditContext, MAX_AGENT_NAME_BYTES, ReplaceOutcome};
pub use engine::Engine;
pub use env::{
    Env, MS_PER_DAY, MS_PER_HOUR, MS_PER_MINUTE, MS_PER_SECOND, MS_PER_WEEK, SECONDS_PER_DAY,
    SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK, SharedEnv, SystemEnv, duration_ms,
    elapsed_ms, now_ms, now_ms_u64, std_duration, system_env,
};
pub use error::{Error, Result};
pub use jiff::SignedDuration;
pub use uuid::Uuid;
