//! Shared close-reason wire vocabulary: the lowercase labels
//! (`resolved` | `declined` | `withdrawn`) mapped to and from the durable
//! [`CloseReason`] enum, so every surface that accepts or emits a close
//! reason (the WS transition frame, the MCP close tool) shares one mapping.
//!
//! No payload builders live here: each `FlagPayload`/`ReplyPayload` is built
//! inline at its single relay call site, so there is no client/relay
//! constructor pair to drift.

use kutl_proto::sync::CloseReason;

/// Map a wire close-reason label to its durable [`CloseReason`] enum variant.
/// The transition wire vocabulary is `resolved` | `declined` | `withdrawn`;
/// any unrecognized value falls back to `Resolved` so a malformed reason never
/// silences the record. The inverse of [`close_reason_to_wire`].
#[must_use]
pub fn close_reason_from_wire(reason: &str) -> CloseReason {
    match reason {
        "declined" => CloseReason::Declined,
        "withdrawn" => CloseReason::Withdrawn,
        _ => CloseReason::Resolved,
    }
}

/// The stable lowercase wire label for a [`CloseReason`], surfaced wherever a
/// close reason renders. The inverse of [`close_reason_from_wire`];
/// `Unspecified` folds to `resolved` so a malformed reason never surfaces an
/// empty string.
#[must_use]
pub fn close_reason_to_wire(reason: CloseReason) -> &'static str {
    match reason {
        CloseReason::Declined => "declined",
        CloseReason::Withdrawn => "withdrawn",
        // Projection-only: `Superseded` has no authoring
        // path, so it is absent from `close_reason_from_wire`; this label
        // exists only to give a stable rendering if the value ever appears.
        CloseReason::Superseded => "superseded",
        CloseReason::Resolved | CloseReason::Unspecified => "resolved",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_proto::sync::CloseReason;

    /// Every wire label maps to its enum variant, and an unknown value falls
    /// back to `Resolved` (so a malformed reason never silences the record).
    #[test]
    fn test_close_reason_from_wire_all_arms() {
        assert_eq!(close_reason_from_wire("resolved"), CloseReason::Resolved);
        assert_eq!(close_reason_from_wire("declined"), CloseReason::Declined);
        assert_eq!(close_reason_from_wire("withdrawn"), CloseReason::Withdrawn);
        assert_eq!(close_reason_from_wire("nonsense"), CloseReason::Resolved);
        assert_eq!(close_reason_from_wire(""), CloseReason::Resolved);
    }

    /// The inverse used by the relay transition JSON path: every variant (incl.
    /// `Unspecified`) yields a non-empty lowercase label.
    #[test]
    fn test_close_reason_to_wire_all_variants() {
        assert_eq!(close_reason_to_wire(CloseReason::Resolved), "resolved");
        assert_eq!(close_reason_to_wire(CloseReason::Declined), "declined");
        assert_eq!(close_reason_to_wire(CloseReason::Withdrawn), "withdrawn");
        assert_eq!(close_reason_to_wire(CloseReason::Superseded), "superseded");
        assert_eq!(close_reason_to_wire(CloseReason::Unspecified), "resolved");
    }

    /// `from_wire` then `to_wire` round-trips the three canonical labels.
    #[test]
    fn test_close_reason_wire_round_trip() {
        for label in ["resolved", "declined", "withdrawn"] {
            assert_eq!(close_reason_to_wire(close_reason_from_wire(label)), label);
        }
    }
}
