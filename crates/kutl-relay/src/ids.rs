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

/// Longest within-space path the relay accepts, in bytes.
pub(crate) const MAX_PATH_BYTES: usize = 1024;

/// The one rule for "this is a within-space document path", and the one
/// wording for rejecting it. Every door that registers or renames a
/// document goes through it (the WebSocket lifecycle handlers and the MCP
/// tools alike), so a malformed path is refused at the boundary with the
/// same sentence everywhere instead of reaching the registry, the mirror,
/// or a daemon's filesystem.
///
/// Refused: an empty path; a leading `/` (paths are within-space relative,
/// not absolute); a backslash (within-space paths are POSIX-relative, and a
/// backslash would let `dir\..\..\escape` slip past a `/`-only `..`
/// split); a `..` component (traversal out of the space); a NUL byte; and
/// a path longer than [`MAX_PATH_BYTES`].
pub(crate) fn check_within_space_path(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("path must not be empty".to_owned());
    }
    if path.starts_with('/') {
        return Err("path must not start with `/` — paths are within-space relative".to_owned());
    }
    if path.len() > MAX_PATH_BYTES {
        return Err(format!("path exceeds {MAX_PATH_BYTES}-byte limit"));
    }
    if path.contains('\0') {
        return Err("path must not contain NUL bytes".to_owned());
    }
    if path.contains('\\') {
        return Err(
            "path must not contain backslashes — within-space paths are POSIX-relative".to_owned(),
        );
    }
    if path.split('/').any(|seg| seg == "..") {
        return Err("path must not contain `..` components — traversal is not allowed".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MAX_PATH_BYTES, check_uuid, check_within_space_path};

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

    #[test]
    fn test_check_within_space_path_accepts_nested_relative_paths() {
        assert!(check_within_space_path("notes/a.md").is_ok());
        assert!(check_within_space_path("a").is_ok());
        assert!(check_within_space_path("deep/er/still.txt").is_ok());
    }

    #[test]
    fn test_check_within_space_path_refuses_every_escape_shape() {
        for bad in [
            "",
            "/abs.md",
            "../escape.md",
            "dir/../../x",
            "a\\..\\b",
            "nul\0.md",
        ] {
            assert!(
                check_within_space_path(bad).is_err(),
                "{bad:?} must be refused"
            );
        }
        let long = "x".repeat(MAX_PATH_BYTES + 1);
        assert!(check_within_space_path(&long).is_err());
    }
}
