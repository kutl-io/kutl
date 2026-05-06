//! Personal Access Token validation backend trait.
//!
//! PATs are managed by the UX server and stored in the `api_tokens` table.
//! The OSS relay has no accounts or PATs — passes `None`.

use anyhow::Result;
use async_trait::async_trait;

/// Backend for validating Personal Access Tokens.
#[async_trait]
pub trait PatBackend: Send + Sync + 'static {
    /// Validate a PAT by its hash.
    /// Returns an identity string (DID or `account:<id>` fallback) if valid.
    async fn validate(&self, token_hash: &str) -> Result<Option<String>>;

    /// Check whether a validated PAT is scoped for a specific space.
    ///
    /// Returns `true` when the token either has no space restriction
    /// (`space_ids IS NULL`) or the given space is in its scope list.
    /// The default implementation returns `true` (unrestricted) — the
    /// OSS relay has no scoped tokens. kutlhub-relay overrides this
    /// with a Postgres query against `api_tokens.space_ids`.
    async fn check_scope(&self, _token_hash: &str, _space_id: &str) -> Result<bool> {
        Ok(true)
    }
}
