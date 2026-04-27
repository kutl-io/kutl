//! Invite backend trait for OSS relay.
//!
//! The OSS relay uses [`SqliteInviteBackend`] (Task 2) backed by the same
//! `registry.db` as [`crate::space_backend::SqliteSpaceBackend`].
//! kutlhub-relay provides its own implementation backed by the UX server's
//! Postgres `invitations` table.

use thiserror::Error;

/// Info returned when validating an invite code.
#[derive(Debug, Clone)]
pub struct InviteInfo {
    /// Relay-assigned UUID for the space.
    pub space_id: String,
    /// Human-readable name of the space.
    pub space_name: String,
}

/// Full invite record returned by `create_invite` and `list_invites`.
#[derive(Debug, Clone)]
pub struct InviteRecord {
    /// Opaque invite code (URL-safe random string).
    pub code: String,
    /// Relay-assigned UUID for the space.
    pub space_id: String,
    /// Human-readable name of the space.
    pub space_name: String,
    /// Expiry timestamp in milliseconds since epoch, or `None` for no expiry.
    pub expires_at: Option<i64>,
    /// Creation timestamp in milliseconds since epoch.
    pub created_at: i64,
}

/// Errors from invite backend operations.
#[derive(Debug, Error)]
pub enum InviteBackendError {
    /// The requested space does not exist.
    #[error("space not found: {0}")]
    SpaceNotFound(String),
    /// The invite code is not recognised.
    #[error("invalid invite code: {0}")]
    InvalidCode(String),
    /// The invite code exists but has expired.
    #[error("invite expired: {0}")]
    Expired(String),
    /// Storage-level error.
    #[error("invite storage error: {0}")]
    Storage(String),
}

/// Backend for invite creation, validation, listing, and revocation.
///
/// Synchronous trait matching [`crate::space_backend::SpaceBackend`]'s
/// pattern — both are `SQLite`-backed in the OSS relay and use
/// `block_in_place` for DB access.
pub trait InviteBackend: Send + Sync {
    /// Create a new invite for `space_id`, optionally expiring at `expires_at`
    /// (milliseconds since epoch).
    ///
    /// Returns `SpaceNotFound` if `space_id` is not registered.
    fn create_invite(
        &self,
        space_id: &str,
        expires_at: Option<i64>,
    ) -> Result<InviteRecord, InviteBackendError>;

    /// Validate an invite code.
    ///
    /// Returns `Ok(Some(InviteInfo))` for a valid, non-expired invite,
    /// `Ok(None)` if the code does not exist, or `Err(Expired)` if it has
    /// expired.
    fn validate_invite(&self, code: &str) -> Result<Option<InviteInfo>, InviteBackendError>;

    /// List all invites for `space_id`, including expired ones.
    fn list_invites(&self, space_id: &str) -> Result<Vec<InviteRecord>, InviteBackendError>;

    /// Revoke an invite by code.
    ///
    /// Returns `true` if the invite was found and deleted, `false` if it did
    /// not exist.
    fn revoke_invite(&self, code: &str) -> Result<bool, InviteBackendError>;
}
