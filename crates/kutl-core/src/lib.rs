//! kutl-core — sync engine, traits, and shared types.

mod change;
mod document;
mod engine;
pub mod env;
mod error;
pub mod hlc;
pub mod lattice;

/// Maximum CRDT operations per document. Post-merge enforcement — bounded
/// overshoot per merge by [`MAX_PATCH_BYTES`].
///
/// Ops are per-character (one per inserted/deleted char), so this is also a
/// bound on cumulative inserted text history: ~1 MB before edits stop
/// merging. The cap exists to bound the type/delete attack's blast radius
/// pre-compaction: adversarial RLE-broken history encodes at ~4 bytes/op
/// (~4 MB durable per doc at this cap) and costs ~110 bytes/op of CRDT
/// memory on the relay and every subscriber (~110 MB worst case per doc);
/// legitimate text measures ~3 bytes/op in memory. Account-level damage is
/// gated separately by `documents_per_account` (RFD 0063).
pub const MAX_OPS_PER_DOC: usize = 1_000_000;

/// Maximum bytes per merge patch. Sized so a near-cap legitimate document's
/// `encode_full()` catch-up frame (~1 byte/op incompressible) still merges
/// at the receiver. At the measured ~4 bytes/op encoded floor for RLE-broken
/// ops, bounds ops-per-merge to ~1 000 000 so worst-case cap overshoot is
/// ≤2×. Legitimate text edits are sub-KB; bulk CRDT ingest fits comfortably.
pub const MAX_PATCH_BYTES: usize = 4_000_000;

pub use change::{Boundary, Change, ChangeList, VersionSpan, new_change, span_start};
pub use diamond_types::AgentId;
pub use document::{Document, EditContext, MAX_AGENT_NAME_BYTES, ReplaceOutcome};
pub use engine::Engine;
pub use env::{
    Env, MS_PER_DAY, MS_PER_HOUR, MS_PER_MINUTE, MS_PER_SECOND, MS_PER_WEEK, SECONDS_PER_DAY,
    SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK, SharedEnv, SystemEnv, duration_ms,
    elapsed_ms, ms_u64_to_i64_saturating, now_ms, now_ms_u64, std_duration, system_env,
};
pub use error::{Error, Result};
pub use hlc::{ActorId, Hlc, HlcClock};
pub use jiff::SignedDuration;
pub use uuid::Uuid;
