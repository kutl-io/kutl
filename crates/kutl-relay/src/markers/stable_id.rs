//! Spec-pinned stable identifiers for marker-derived signal records.
//!
//! Marker-derived signal ids and decision `title_hash`es are DURABLE: a
//! CLOSE/REOPEN record materialized long after (and possibly on a different
//! relay toolchain than) its CREATED must compute the SAME id, or the fold
//! orphans the original Open forever and duplicates appear. `title_hash` is
//! likewise persisted in `DecisionPayload` and re-parsed on tracker
//! re-derivation.
//!
//! These derivations therefore MUST be reproducible across Rust releases and
//! across relays. `std::collections::hash_map::DefaultHasher` is explicitly
//! NOT guaranteed stable across std versions, so it cannot back durable
//! identity. This module uses RFC-4122 v5 UUIDs instead: v5 is SHA-1 over a
//! fixed namespace plus a name, spec-stable by construction. The `uuid` crate
//! is already a relay dependency, so no hashing crate is added.

use uuid::Uuid;

/// Fixed UUID namespace for every marker-derived stable id in this relay.
///
/// This value is a load-bearing constant of the ON-DISK / ON-THE-WIRE format:
/// changing it re-derives EVERY marker signal id and every decision
/// `title_hash`, orphaning all open marker-derived signals across the fleet.
/// It MUST NEVER change. A deliberate format bump would go through an explicit,
/// versioned migration — not an edit here.
const STABLE_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x7e, 0x52, 0xcb, 0x6b, 0xb6, 0xfc, 0x42, 0x80, 0x99, 0x6d, 0xc1, 0x53, 0xc4, 0x2e, 0xe0, 0x55,
]);

/// Component tag for decision-heading signal ids. Private: callers go through
/// [`decision_signal_id`] so the derivation has exactly one definition.
const SIGNAL_ID_TAG_DECISION: &str = "decision";

/// Component tag for mention (in-doc flag) signal ids. Private: callers go
/// through [`mention_signal_id`].
const SIGNAL_ID_TAG_MENTION: &str = "mention";

/// Component tag for decision `title_hash` derivation. Callers go through
/// [`crate::markers::decisions::hash_title`].
pub(crate) const TITLE_HASH_TAG: &str = "decision-title";

/// The signal id a decision heading is BORN with, derived from the document it
/// lives in and the hash of the title it was first seen under.
///
/// Mints identity; it does not look it up. A decision's id is fixed at birth
/// and then CARRIED by the tracker (see
/// [`crate::markers::decisions::DecisionEvent::signal_id`]), because a title
/// rewrite changes `title_hash` while the decision stays the same decision.
/// Re-deriving from the *current* title on a later transition would address a
/// signal that was never created — an orphan CLOSE the fold parks forever.
#[must_use]
pub fn decision_signal_id(document_id: &str, title_hash: u64) -> String {
    stable_v5(
        SIGNAL_ID_TAG_DECISION,
        &[document_id, &title_hash.to_string()],
    )
    .to_string()
}

/// The signal id an in-doc mention marker (`@[Name](kind:id)`) maps to.
///
/// Unlike a decision, a mention has no rename: `(kind, target_id)` IS its
/// identity, so the id is safely re-derivable wherever the marker is in hand.
/// Both the materializer (which mints the record) and a host publisher (which
/// names it in a feed event) call this, so the two can never disagree.
#[must_use]
pub fn mention_signal_id(document_id: &str, kind: &str, target_id: &str) -> String {
    stable_v5(SIGNAL_ID_TAG_MENTION, &[document_id, kind, target_id]).to_string()
}

/// Derive a spec-pinned v5 UUID from a `tag` and ordered `components`.
///
/// The name fed to v5 is an UNAMBIGUOUS, word-size-independent, length-prefixed
/// concatenation: the tag then each component is written as its UTF-8 byte
/// length (a fixed-width `u64`, little-endian) followed by its bytes. Fixed
/// widths mean the derivation never depends on the host's `usize` size;
/// length-prefixing means `("a", "bc")` and `("ab", "c")` can never collide.
///
/// The same `(tag, components)` always yields the same UUID, so a CLOSE/REOPEN
/// record references the same `Signal.id` as its CREATED — the whole point.
#[must_use]
pub(crate) fn stable_v5(tag: &str, components: &[&str]) -> Uuid {
    let mut name = Vec::new();
    push_length_prefixed(&mut name, tag);
    for component in components {
        push_length_prefixed(&mut name, component);
    }
    Uuid::new_v5(&STABLE_ID_NAMESPACE, &name)
}

/// Append `s` to `buf` as a fixed-width `u64`-little-endian length prefix
/// followed by `s`'s UTF-8 bytes.
fn push_length_prefixed(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    // Fixed-width so the byte layout never depends on the host's `usize` size.
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GOLDEN (archetype F): the derivation is pinned to HARDCODED expected
    /// UUIDs for fixed inputs. If a future change to the namespace or the
    /// name-byte layout alters these, THIS test fails loudly and forces a
    /// conscious, versioned format decision — the derivation is durable
    /// on-disk / on-the-wire identity and must never drift silently.
    #[test]
    fn test_stable_v5_golden_values() {
        assert_eq!(
            decision_signal_id("doc-1", 42),
            "26191cbd-8484-5d93-b324-2d3ab6fcf0a4",
            "decision signal id derivation changed — durable identity would break"
        );
        assert_eq!(
            mention_signal_id("doc-1", "review_requested", "acc-1"),
            "ebe4db05-8c20-5046-95e9-60514ead4361",
            "mention signal id derivation changed — durable identity would break"
        );
    }

    /// The same inputs always yield the same UUID (within and across calls),
    /// and the tag namespaces the id (a decision and a mention with identical
    /// components differ).
    #[test]
    fn test_stable_v5_deterministic_and_tagged() {
        let a = stable_v5(SIGNAL_ID_TAG_DECISION, &["doc-1", "42"]);
        let b = stable_v5(SIGNAL_ID_TAG_DECISION, &["doc-1", "42"]);
        assert_eq!(a, b, "same inputs → same id");
        let mention = stable_v5(SIGNAL_ID_TAG_MENTION, &["doc-1", "42"]);
        assert_ne!(a, mention, "tag namespaces the id");
    }

    /// Length-prefixing prevents component-boundary collisions:
    /// `("a", "bc")` and `("ab", "c")` must differ.
    #[test]
    fn test_stable_v5_length_prefixing_avoids_boundary_collision() {
        let ab = stable_v5(SIGNAL_ID_TAG_DECISION, &["a", "bc"]);
        let cd = stable_v5(SIGNAL_ID_TAG_DECISION, &["ab", "c"]);
        assert_ne!(ab, cd, "component boundaries must not collide");
    }
}
