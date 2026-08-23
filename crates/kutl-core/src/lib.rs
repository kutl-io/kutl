//! kutl-core — sync engine, traits, and shared types.

mod change;
pub mod content_envelope;
mod document;
mod engine;
pub mod env;
mod error;
pub mod hlc;
pub mod lattice;
pub mod merge;

/// Maximum CRDT operations per document. Post-merge enforcement — bounded
/// overshoot per merge by [`MAX_PATCH_BYTES`].
///
/// Ops are per-character (one per inserted/deleted char), so this is also a
/// bound on cumulative inserted+deleted text history: ~10 MB of churn before
/// edits stop merging. Legitimate costs scale LINEARLY with history with no
/// knee through 16M ops (measured, `tests/op_cap_probe.rs`: `encode_full`
/// ~7 ms and cold-load ~45 ms per million ops, ~0.12–0.3 encoded bytes/op
/// for churn-shaped docs; the steady-state edit path is flat), so this cap
/// is not a performance cliff — it bounds the type/delete attack's blast
/// radius pre-compaction: adversarial RLE-broken history encodes at
/// ~4 bytes/op (~40 MB durable per doc at this cap) and costs
/// ~110 bytes/op of CRDT memory on the relay and every subscriber
/// (~1.1 GB worst case per doc — the tradeoff accepted for the 10×
/// headroom); legitimate text measures ~3 bytes/op in memory (~30 MB).
/// Account-level damage is gated separately by `documents_per_account`.
pub const MAX_OPS_PER_DOC: usize = 10_000_000;

/// Op count at which a document is APPROACHING [`MAX_OPS_PER_DOC`] (80%) —
/// the early-warning threshold. At the cap edits stop syncing outright, so
/// surfacing the approach (in `kutl status` and operator logs) is what gives
/// the owner time to compact or split the document before the freeze.
pub const OP_CAP_WARN_THRESHOLD: usize = MAX_OPS_PER_DOC / 5 * 4;

/// Maximum bytes per merge patch. Sized so a near-cap legitimate document's
/// `encode_full()` catch-up frame still merges at the receiver: measured
/// churn shapes encode at ~0.12–0.3 bytes/op (`tests/op_cap_probe.rs` and
/// the production doc that hit the cap), so a full frame at
/// [`MAX_OPS_PER_DOC`] is ~1–3 MB — this gate admits it with ~5× margin. At
/// the measured ~4 bytes/op encoded floor for RLE-broken ops, bounds
/// ops-per-merge to ~4 000 000 so worst-case cap overshoot is ≤1.4×.
/// Legitimate text edits are sub-KB; bulk CRDT ingest fits comfortably.
/// Stays under the relay's 25 MB flush-envelope ceiling and the WebSocket
/// frame-size limit.
pub const MAX_PATCH_BYTES: usize = 16_000_000;

pub use change::{Boundary, Change, ChangeList, VersionSpan, new_change, span_end, span_start};
pub use content_envelope::{
    DecodedEnvelope, ENVELOPE_MAGIC, decode_content_envelope, encode_content_envelope,
};
pub use diamond_types::AgentId;
pub use document::{Document, EditContext, MAX_AGENT_NAME_BYTES, MergeOutcome, ReplaceOutcome};
pub use engine::Engine;
pub use env::{
    Env, MS_PER_DAY, MS_PER_HOUR, MS_PER_SECOND, SECONDS_PER_DAY, SECONDS_PER_HOUR,
    SECONDS_PER_MINUTE, SharedEnv, SystemEnv, duration_ms, elapsed_ms, ms_u64_to_i64_saturating,
    now_ms, now_ms_u64, std_duration, system_env,
};
pub use error::{Error, Result};
pub use hlc::{ActorId, Hlc, HlcClock};
pub use jiff::SignedDuration;
pub use merge::{Hunk, HunkRefusal};
pub use uuid::Uuid;
