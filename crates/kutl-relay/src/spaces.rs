//! Space name validation and membership type re-exports.

/// Minimum space name length.
const SPACE_NAME_MIN_LEN: usize = 3;

/// Maximum space name length.
const SPACE_NAME_MAX_LEN: usize = 50;

/// Reserved slugs that conflict with application routes.
const RESERVED_SLUGS: &[&str] = &[
    "settings", "new", "admin", "api", "auth", "health", "metrics", "_",
];

/// Extract the account ID from an authenticated identity string.
///
/// Handles both `account:<uuid>` format (from PAT auth) and DID format
/// (from challenge-response auth). Returns `Some(uuid_string)` for
/// account-based identities, `None` for DID-based identities.
pub fn extract_account_id(identity: &str) -> Option<&str> {
    identity.strip_prefix("account:")
}

pub use crate::membership_backend::{AcceptInvitationResult, SpaceMembershipInfo, SpaceRecord};

/// Validate a space name (slug rules).
///
/// Canonical rules shared with the TS server and client:
/// - Pattern: `^[a-z0-9]+(-[a-z0-9]+)*$`
/// - Length: 3..=50
/// - No reserved words
pub fn validate_space_name(name: &str) -> Result<(), &'static str> {
    if name.len() < SPACE_NAME_MIN_LEN {
        return Err("space name must be at least 3 characters");
    }
    if name.len() > SPACE_NAME_MAX_LEN {
        return Err("space name must be at most 50 characters");
    }
    // Pattern: ^[a-z0-9]+(-[a-z0-9]+)*$
    // Each segment between hyphens must be non-empty and consist of [a-z0-9].
    for segment in name.split('-') {
        if segment.is_empty() {
            // Leading/trailing hyphen or consecutive hyphens.
            return Err(
                "space name must match pattern: lowercase alphanumeric segments separated by single hyphens",
            );
        }
        if !segment
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err("space name must contain only lowercase letters, digits, and hyphens");
        }
    }
    if RESERVED_SLUGS.contains(&name) {
        return Err("space name is reserved");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct SlugTestVectors {
        valid: Vec<String>,
        invalid: Vec<InvalidEntry>,
    }

    #[derive(serde::Deserialize)]
    struct InvalidEntry {
        input: String,
        reason: String,
    }

    #[test]
    fn test_validate_space_name_shared_vectors() {
        let json = include_str!("../../../test-vectors/slug-validation.json");
        let vectors: SlugTestVectors =
            serde_json::from_str(json).expect("failed to parse test vectors");

        for name in &vectors.valid {
            assert!(
                validate_space_name(name).is_ok(),
                "expected valid but got error for {name:?}",
            );
        }

        for entry in &vectors.invalid {
            assert!(
                validate_space_name(&entry.input).is_err(),
                "expected error for {:?} (reason: {})",
                entry.input,
                entry.reason,
            );
        }
    }
}
