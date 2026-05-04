//! Space membership backend trait.
//!
//! Covers ACL checks, DID-to-account resolution, space queries, and
//! invitation acceptance. The OSS relay passes `None` — ACL falls
//! back to `authorized_keys` or allows any authenticated identity.

use anyhow::Result;
use async_trait::async_trait;

/// A space resolved by owner/slug.
pub struct SpaceRecord {
    /// Space UUID as a string.
    pub space_id: String,
    /// Display name of the space.
    pub space_name: String,
}

/// A space membership with full space details.
pub struct SpaceMembershipInfo {
    /// Space UUID as a string.
    pub space_id: String,
    /// Display name of the space.
    pub space_name: String,
    /// URL-safe slug.
    pub space_slug: String,
    /// Account UUID of the space owner.
    pub owner_account_id: String,
    /// Caller's role in this space.
    pub role: String,
}

/// Result of accepting an invitation.
pub struct AcceptInvitationResult {
    /// Space UUID as a string.
    pub space_id: String,
    /// Display name of the space.
    pub space_name: String,
    /// URL-safe slug.
    pub space_slug: String,
    /// Account UUID of the space owner.
    pub owner_account_id: String,
}

/// Backend for space membership checks and operations.
#[async_trait]
pub trait MembershipBackend: Send + Sync + 'static {
    /// Check if an account has membership in a space.
    /// Returns the role string if found, `None` otherwise.
    async fn check_membership(&self, space_id: &str, account_id: &str) -> Result<Option<String>>;

    /// Resolve a DID to an account ID via custodied keys.
    /// Returns the account UUID string if found.
    async fn resolve_did_to_account(&self, did: &str) -> Result<Option<String>>;

    /// Look up a space by owner account slug and space slug.
    async fn resolve_space_by_slugs(
        &self,
        owner_slug: &str,
        space_slug: &str,
    ) -> Result<Option<SpaceRecord>>;

    /// List all spaces a given account is a member of.
    async fn list_spaces_for_account(&self, account_id: &str) -> Result<Vec<SpaceMembershipInfo>>;

    /// Accept an invitation: validate code, insert membership, increment use count.
    async fn accept_invitation(
        &self,
        code: &str,
        account_id: &str,
    ) -> Result<AcceptInvitationResult>;
}
