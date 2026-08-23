//! The one rule for "this id field must be a UUID", and the one wording for
//! rejecting it.
//!
//! Document ids and signal ids are UUIDs everywhere they are durable: migration
//! `0028_signals_document_id_uuid` types `signals.document_id` as `uuid`, and
//! every minting site is `Uuid::new_v4()`. What was missing is a boundary that
//! says so. Without one, a caller passing a path (`daily-digest.md`) or a slug
//! reached the database and got back an opaque cast error, and each door that
//! did defend invented its own sentence for it — three wordings across six
//! sites, which is three chances for one of them to drift into meaning
//! something subtly different.
//!
//! # Why a function and not a newtype
//!
//! [`SpaceId`](crate::acl::SpaceId) makes the same guarantee by construction,
//! which is strictly better: a `SpaceId` cannot be malformed, so no consumer
//! re-checks. The equivalent for document and signal ids is the right end
//! state, but it is a wide migration through the proto-facing signatures rather
//! than something to land inside an admission-seam change. This helper is the
//! interim: it does not make the malformed value unrepresentable, but it does
//! make every rejection say the same thing, which is the property that was
//! actually being lost.
//!
//! **Space ids do not come here.** They have the newtype; routing them through
//! a string check would be a step backwards.

/// Check that an id field parses as a UUID, returning the shared rejection
/// message on failure.
///
/// Returns the message rather than a typed error because the doors deliver it
/// differently — an MCP `ToolCallResult`, a WS `ErrorCode::InvalidMessage`
/// frame, a `ChangeError::InvalidArgument` on the record seam. The wording is
/// the part that must not diverge; the envelope is each door's own business.
///
/// The raw value is echoed back deliberately: a caller who passed a path wants
/// to see the path, not a bare "invalid id".
pub(crate) fn check_uuid(field: &str, value: &str) -> Result<(), String> {
    if uuid::Uuid::parse_str(value).is_err() {
        return Err(format!("{field} must be a UUID; got {value:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::check_uuid;

    #[test]
    fn test_check_uuid_accepts_a_uuid() {
        assert!(check_uuid("document_id", &uuid::Uuid::new_v4().to_string()).is_ok());
    }

    #[test]
    fn test_check_uuid_echoes_the_offending_value() {
        let err = check_uuid("document_id", "daily-digest.md")
            .expect_err("a path is not a UUID and must be rejected");
        assert!(
            err.contains("daily-digest.md"),
            "the rejection must show what was passed, got: {err}"
        );
        assert!(err.contains("document_id"), "and which field: {err}");
    }

    #[test]
    fn test_check_uuid_rejects_the_empty_string() {
        // Callers decide whether absent is legal; this only answers "is it a
        // UUID". An empty id reaching here is a caller that forgot to filter.
        assert!(check_uuid("signal_id", "").is_err());
    }
}
