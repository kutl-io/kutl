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
    /// Display name of the space owner. `None` when the owner has no
    /// display name set (e.g., bootstrapping accounts).
    pub owner_display_name: Option<String>,
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

/// A member of a space, as the backend names it for addressing.
pub struct SpaceParticipant {
    /// The DID this participant syncs and is addressed as.
    pub did: String,
    /// Addressing name: the account's handle, or for an account operated
    /// by another, the `/`-joined chain of handles from its root operator
    /// down to it (e.g. `boris/cfo/accountant`).
    pub name: String,
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

    /// Resolve an account ID to its DID via custodied keys — the reverse of
    /// [`Self::resolve_did_to_account`], and the relay's account directory.
    ///
    /// In-document mention markers carry an **account id** while the record
    /// field is `target_did`, because the editor's picker knows accounts and
    /// the protocol knows DIDs. Mapping between them belongs here rather than
    /// in a converter on the read side: a deployment that has accounts is
    /// exactly a deployment that has this backend.
    ///
    /// **Resolve only — never provision.** A caller that finds `None` must
    /// degrade, not mint. Minting would mean a document edit creates a signing
    /// identity for a third party (the person mentioned, who has not acted),
    /// synchronously, on the merge path — and would require the relay to hold
    /// the custodied-key encryption secret. Accounts get their DID at signup;
    /// one without a DID is a provenance-or-config problem to surface, not to
    /// paper over.
    ///
    /// Returns `None` when the account has no custodied key, or has no
    /// existence on this deployment at all. Callers cannot distinguish those,
    /// and should not need to.
    async fn resolve_account_to_did(&self, account_id: &str) -> Result<Option<String>>;

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

    /// Every member of the space that has a resolvable DID, named for
    /// addressing. The actor set, not a presence list: a name must resolve
    /// while its owner is away, because a signal's whole purpose is to wait
    /// for someone. Members without a key are omitted — they cannot be
    /// addressed, and a name that resolves to nothing is worse than absence.
    async fn list_space_participants(&self, space_id: &str) -> Result<Vec<SpaceParticipant>>;
}
