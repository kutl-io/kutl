//! Re-exports of the shared classification logic from
//! [`kutl_sync_protocol`] (RFD 0066). The pure rules live in that
//! crate so the daemon, the relay, and the browser all agree on the
//! same invariants by definition (not by convention).
//!
//! This module exists as a stable internal import surface for relay
//! code: `use crate::load_class::{LoadOutcome, LoadClass, classify_load}`
//! keeps compiling whether the shared crate is in this workspace or
//! (someday) a separate published crate.

pub(crate) use kutl_sync_protocol::{LoadClass, LoadOutcome, classify_load};
