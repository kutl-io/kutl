//! Shared signal-record foundation.
//!
//! A signal's canonical form is an append-only event record — the wire
//! `Signal` message with its record-envelope fields populated. This crate
//! owns the four pieces every consumer (relay, daemon, CLI) must agree on:
//!
//! - [`record`]: canonical bytes, Ed25519 signing, advisory verification,
//!   and the bounded did:key encode/decode pair (`did_key_encode`,
//!   `did_key_verifying_key`).
//! - [`segment`]: append-only per-space binary segments (rotate, seal,
//!   zstd, crash recovery, single-writer lock).
//! - [`fold`]: the deterministic fold from records to signal state.
//! - [`catchup`]: HLC cursor bookkeeping for the HTTP catch-up exchange.
//!
//! Storage discipline is normative: segments never serve online queries —
//! projections do. This crate is the plumbing.

pub mod authoring;
pub mod catchup;
pub mod content;
pub mod error;
pub mod fold;
pub mod payloads;
pub mod record;
pub mod segment;

/// Filesystem-level storage-fault injectors for tests (behind the `testing`
/// feature). A faithful external injector over real on-disk segment state.
/// Off by default; production code never depends on it.
pub mod summary;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use error::{Error, Result};
pub use record::{
    AttestationVerification, RecordVerification, RelayTrust, did_key_encode, did_key_verifying_key,
    sign_attestation, sign_record, verify_attestation, verify_record,
};
