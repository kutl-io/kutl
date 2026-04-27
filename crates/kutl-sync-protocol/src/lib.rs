//! Pure classification rules for the kutl sync protocol (RFD 0066).
//!
//! This crate owns the classification invariants that every sync client
//! and the relay must agree on:
//!
//! - [`classify_load`]: server-side — given a backend-load outcome and
//!   the durability witness, which read class applies?
//! - [`resolve_client_classification`]: client-side — given the server's
//!   classification and whether the client already has local content,
//!   should the client re-classify as `Inconsistent` to protect its own
//!   in-memory state?
//!
//! Both rules are pure functions with property-tested semilattice-style
//! invariants. The crate depends on nothing beyond `core` + `alloc` so it
//! builds for native and `wasm32-unknown-unknown` identically. The
//! browser consumes it via the sibling `kutl-sync-protocol-wasm` crate.

#![no_std]

/// Outcome of a single backend load call, stripped of data.
///
/// This is the minimum information [`classify_load`] needs to decide
/// which read class applies. The caller retains the actual bytes
/// (if any) and uses them to build the final response on `FoundContent`
/// or `FoundBlob` outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LoadOutcome {
    /// Backend returned `Ok(Some(..))`.
    Present,
    /// Backend returned `Ok(None)` — no row for this document.
    Absent,
    /// Backend returned `Err(..)` — we could not determine the state.
    Errored,
}

/// Classification of a subscribe cold-load (RFD 0066 Decision 2).
///
/// This enum is the wire-level shape the relay communicates to
/// clients via the `SubscribeStatus` envelope. Clients gate editor
/// writability on the value — see [`should_allow_editing`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LoadClass {
    /// Content backend returned bytes for this doc. Safe to edit.
    FoundContent,
    /// Blob backend returned bytes for this doc. Safe to view; editing
    /// semantics are blob-specific (the blob is replaced, not edited).
    FoundBlob,
    /// Both backends returned `Ok(None)` and the witness was never
    /// advanced (`persisted_version == 0`). Legitimate first-time
    /// document — editor may open as writable.
    Empty,
    /// Both backends returned `Ok(None)` but the witness says content
    /// was previously persisted. The Elasticsearch failure mode —
    /// server lost bytes it claims are durable. Editor MUST refuse
    /// writes.
    Inconsistent {
        /// The `persisted_version` the witness claims exists.
        expected_version: u64,
    },
    /// At least one backend errored. State is undetermined; refuse
    /// to claim either Empty or Inconsistent.
    Unavailable,
}

/// Server-side classification of a cold-load outcome (RFD 0066).
///
/// Rules, in priority order:
///
/// 1. Any `Errored` outcome → `Unavailable`. We cannot be sure
///    whether the doc exists, so we must not claim either.
/// 2. `content = Present` → `FoundContent`. Content takes precedence
///    over blob.
/// 3. `blob = Present` → `FoundBlob`.
/// 4. Both `Absent` + `witness_version == 0` → `Empty`.
/// 5. Both `Absent` + `witness_version > 0` → `Inconsistent`.
///
/// Pure: commutative over backend choice (content vs. blob is a
/// deterministic priority, not a race), deterministic for any
/// `witness_version`, and idempotent.
#[must_use]
pub fn classify_load(content: LoadOutcome, blob: LoadOutcome, witness_version: u64) -> LoadClass {
    if matches!(content, LoadOutcome::Errored) || matches!(blob, LoadOutcome::Errored) {
        return LoadClass::Unavailable;
    }
    if matches!(content, LoadOutcome::Present) {
        return LoadClass::FoundContent;
    }
    if matches!(blob, LoadOutcome::Present) {
        return LoadClass::FoundBlob;
    }
    if witness_version > 0 {
        LoadClass::Inconsistent {
            expected_version: witness_version,
        }
    } else {
        LoadClass::Empty
    }
}

/// Client-side symmetric fail-closed rule (RFD 0066 Decision 4).
///
/// Given the relay's classification and whether the client already has
/// local content for this doc, decide whether to re-classify on the
/// client side.
///
/// The narrow rule: if the server classifies as `Empty` but the client
/// has an in-memory adapter with non-empty state, the two views
/// disagree on whether this doc has ever had content. That's precisely
/// the "momentarily-degraded relay could cause silent loss" case. The
/// client refuses the server's claim and surfaces it as `Inconsistent`
/// from its own side.
///
/// Fallthrough: every other combination passes through unchanged.
///
/// - `FoundContent` / `FoundBlob` with or without local content: the
///   CRDT merge is union-safe; no re-classification is needed.
/// - `Inconsistent` / `Unavailable`: already fail-closed server-side.
/// - `Empty` without local content: genuine first-time doc.
///
/// The rule is narrow on purpose. Broader "client's version vector
/// strictly supersedes server's" checks reduce to the CRDT merge
/// itself being union-safe (local state is never discarded on the
/// data plane). The case this catches is the one that CRDT merge
/// alone cannot surface as a UX-level error: the disagreement is
/// purely at the classification layer.
#[must_use]
pub fn resolve_client_classification(
    server_class: LoadClass,
    has_local_content: bool,
) -> LoadClass {
    match (server_class, has_local_content) {
        (LoadClass::Empty, true) => LoadClass::Inconsistent {
            expected_version: 0,
        },
        (class, _) => class,
    }
}

/// The hard edit guard (RFD 0066 Decision 3): is the editor allowed
/// to be writable, given the classification?
///
/// Only `FoundContent` and `Empty` are writable. Everything else fails
/// closed. `FoundBlob` is explicitly NOT writable from the editor's
/// perspective — the blob is replaced whole via upload, not edited in
/// place; the editor should render a viewer instead.
#[must_use]
pub fn should_allow_editing(class: LoadClass) -> bool {
    matches!(class, LoadClass::FoundContent | LoadClass::Empty)
}

#[cfg(test)]
mod tests {
    extern crate alloc;
    use super::*;
    use proptest::prelude::*;

    // ------------------------------------------------------------------
    // classify_load: server-side classifier (RFD tests 3–6, 10)
    // ------------------------------------------------------------------

    /// Test 3: content `Present` → `FoundContent` regardless of witness.
    #[test]
    fn classify_load_present_content_is_found_any_witness() {
        assert_eq!(
            classify_load(LoadOutcome::Present, LoadOutcome::Absent, 0),
            LoadClass::FoundContent
        );
        assert_eq!(
            classify_load(LoadOutcome::Present, LoadOutcome::Absent, 99),
            LoadClass::FoundContent
        );
    }

    /// Blob path: content absent, blob present → `FoundBlob`.
    #[test]
    fn classify_load_blob_present_when_content_absent() {
        assert_eq!(
            classify_load(LoadOutcome::Absent, LoadOutcome::Present, 5),
            LoadClass::FoundBlob
        );
    }

    /// Test 10: both absent + witness == 0 → Empty (first-time doc).
    #[test]
    fn classify_load_absent_with_zero_witness_is_empty() {
        assert_eq!(
            classify_load(LoadOutcome::Absent, LoadOutcome::Absent, 0),
            LoadClass::Empty
        );
    }

    /// Test 5 (headline incident): both absent + witness > 0 →
    /// Inconsistent with the witness version propagated.
    #[test]
    fn classify_load_absent_with_positive_witness_is_inconsistent() {
        assert_eq!(
            classify_load(LoadOutcome::Absent, LoadOutcome::Absent, 42),
            LoadClass::Inconsistent {
                expected_version: 42
            }
        );
    }

    /// Test 6: backend Err → Unavailable regardless of witness. The
    /// critical invariant — an error must NEVER silently collapse
    /// into Empty.
    #[test]
    fn classify_load_errored_content_is_unavailable_regardless_of_witness() {
        assert_eq!(
            classify_load(LoadOutcome::Errored, LoadOutcome::Absent, 0),
            LoadClass::Unavailable
        );
        assert_eq!(
            classify_load(LoadOutcome::Errored, LoadOutcome::Absent, 100),
            LoadClass::Unavailable
        );
    }

    #[test]
    fn classify_load_errored_blob_is_unavailable() {
        assert_eq!(
            classify_load(LoadOutcome::Absent, LoadOutcome::Errored, 0),
            LoadClass::Unavailable
        );
    }

    /// Any backend Err is Unavailable, even when the other backend
    /// returned bytes. Conservative: refuse ambiguous success rather
    /// than silently suppressing a backend-error signal.
    #[test]
    fn classify_load_any_errored_wins_over_present() {
        assert_eq!(
            classify_load(LoadOutcome::Present, LoadOutcome::Errored, 5),
            LoadClass::Unavailable
        );
        assert_eq!(
            classify_load(LoadOutcome::Errored, LoadOutcome::Present, 5),
            LoadClass::Unavailable
        );
    }

    #[test]
    fn classify_load_both_errored_is_unavailable() {
        assert_eq!(
            classify_load(LoadOutcome::Errored, LoadOutcome::Errored, 42),
            LoadClass::Unavailable
        );
    }

    /// Priority: content wins over blob when both present (which
    /// shouldn't happen in practice — a doc is one or the other —
    /// but the rule is deterministic).
    #[test]
    fn classify_load_both_present_prefers_content() {
        assert_eq!(
            classify_load(LoadOutcome::Present, LoadOutcome::Present, 7),
            LoadClass::FoundContent
        );
    }

    // ------------------------------------------------------------------
    // resolve_client_classification: symmetric rule (RFD tests 19, 20)
    // ------------------------------------------------------------------

    /// Test 19: Found passes through whether or not the client has
    /// local content. The CRDT merge is union-safe.
    #[test]
    fn resolve_found_passes_through() {
        assert_eq!(
            resolve_client_classification(LoadClass::FoundContent, false),
            LoadClass::FoundContent
        );
        assert_eq!(
            resolve_client_classification(LoadClass::FoundContent, true),
            LoadClass::FoundContent
        );
        assert_eq!(
            resolve_client_classification(LoadClass::FoundBlob, true),
            LoadClass::FoundBlob
        );
    }

    /// Test 20 (symmetric guard headline): server says Empty, client
    /// has local content — flip to Inconsistent. This is the case
    /// where the "mixed clients during a momentarily-degraded relay"
    /// scenario produces silent loss without this guard.
    #[test]
    fn resolve_empty_with_local_content_becomes_inconsistent() {
        assert_eq!(
            resolve_client_classification(LoadClass::Empty, true),
            LoadClass::Inconsistent {
                expected_version: 0
            }
        );
    }

    /// The legitimate first-time subscribe: server and client agree
    /// there's nothing, editor opens fresh. Must NOT be re-classified.
    #[test]
    fn resolve_empty_without_local_content_stays_empty() {
        assert_eq!(
            resolve_client_classification(LoadClass::Empty, false),
            LoadClass::Empty
        );
    }

    /// Server-side fail-closed classes are stricter than `Empty`;
    /// the client trusts them regardless of local state.
    #[test]
    fn resolve_inconsistent_and_unavailable_pass_through() {
        let inconsistent = LoadClass::Inconsistent {
            expected_version: 7,
        };
        assert_eq!(
            resolve_client_classification(inconsistent, false),
            inconsistent
        );
        assert_eq!(
            resolve_client_classification(inconsistent, true),
            inconsistent
        );
        assert_eq!(
            resolve_client_classification(LoadClass::Unavailable, false),
            LoadClass::Unavailable
        );
        assert_eq!(
            resolve_client_classification(LoadClass::Unavailable, true),
            LoadClass::Unavailable
        );
    }

    // ------------------------------------------------------------------
    // should_allow_editing: hard edit guard (RFD tests 15–18)
    // ------------------------------------------------------------------

    #[test]
    fn editing_allowed_on_found_content() {
        assert!(should_allow_editing(LoadClass::FoundContent));
    }

    #[test]
    fn editing_allowed_on_empty() {
        assert!(should_allow_editing(LoadClass::Empty));
    }

    #[test]
    fn editing_refused_on_inconsistent() {
        assert!(!should_allow_editing(LoadClass::Inconsistent {
            expected_version: 42
        }));
    }

    #[test]
    fn editing_refused_on_unavailable() {
        assert!(!should_allow_editing(LoadClass::Unavailable));
    }

    /// Blobs are not writable through the text editor surface —
    /// they're replaced whole via upload. A blob doc renders a
    /// viewer component instead.
    #[test]
    fn editing_refused_on_found_blob() {
        assert!(!should_allow_editing(LoadClass::FoundBlob));
    }

    // ------------------------------------------------------------------
    // Property tests: determinism and priority correctness
    // ------------------------------------------------------------------

    fn arb_outcome() -> impl Strategy<Value = LoadOutcome> {
        prop_oneof![
            Just(LoadOutcome::Present),
            Just(LoadOutcome::Absent),
            Just(LoadOutcome::Errored),
        ]
    }

    proptest! {
        /// Determinism: classify_load is a pure function — same input,
        /// same output, always.
        #[test]
        fn classify_load_is_deterministic(
            content in arb_outcome(),
            blob in arb_outcome(),
            witness in 0u64..100,
        ) {
            prop_assert_eq!(
                classify_load(content, blob, witness),
                classify_load(content, blob, witness)
            );
        }

        /// Invariant: witness value never matters when any backend is
        /// Errored. Unavailable must stay Unavailable across all
        /// possible witness values.
        #[test]
        fn errored_is_unavailable_for_every_witness(
            witness in 0u64..u64::MAX,
            blob in arb_outcome(),
        ) {
            prop_assert_eq!(
                classify_load(LoadOutcome::Errored, blob, witness),
                LoadClass::Unavailable
            );
        }

        /// Invariant: resolve_client_classification never weakens the
        /// server's classification. It can only elevate Empty →
        /// Inconsistent; everything else is a pass-through. This
        /// property ensures the client cannot accidentally allow
        /// edits by mis-routing.
        #[test]
        fn resolve_never_downgrades_to_editable(
            server in prop_oneof![
                Just(LoadClass::Inconsistent { expected_version: 1 }),
                Just(LoadClass::Unavailable),
            ],
            has_local in prop::bool::ANY,
        ) {
            let resolved = resolve_client_classification(server, has_local);
            prop_assert!(!should_allow_editing(resolved));
        }
    }
}
