//! Personal Access Token validation backend trait.
//!
//! PATs are managed by the UX server and stored in the `api_tokens` table.
//! The OSS relay has no accounts or PATs — passes `None`.

use anyhow::Result;
use async_trait::async_trait;

/// Resolved identity from a validated PAT.
///
/// Carries both the underlying identity (`did` — a `did:key:...` for
/// accounts with a custodied DID, or `account:<account_id>` for the
/// synthesized fallback used by web-only accounts) AND the specific
/// `api_tokens.id` (UUID-as-string) of the PAT used. The
/// latter is what lets the activity feed distinguish "alice's
/// coding-agent" from "alice's doc-writer" — multiple PATs for the same
/// account collapse to the same `did` but differ on `pat_id`.
#[derive(Debug, Clone)]
pub struct PatIdentity {
    /// Underlying identity. `did:key:...` when the account has a
    /// custodied DID; `account:<account_id>` synthesized otherwise.
    pub did: String,
    /// `api_tokens.id` UUID for the PAT that was validated. Used as
    /// `signals.via_pat_id` to keep per-PAT attribution discoverable
    /// from the signals table.
    pub pat_id: String,
}

/// Backend for validating Personal Access Tokens.
#[async_trait]
pub trait PatBackend: Send + Sync + 'static {
    /// Validate a PAT by its hash.
    ///
    /// Returns a [`PatIdentity`] when the token resolves to an
    /// authorized account. `None` for revoked or unknown tokens.
    async fn validate(&self, token_hash: &str) -> Result<Option<PatIdentity>>;

    /// Check whether a validated PAT is scoped for a specific space.
    ///
    /// Returns `true` when the token either has no space restriction
    /// (`space_ids IS NULL`) or the given space is in its scope list.
    /// The default implementation returns `true` (unrestricted) — the
    /// default relay has no scoped tokens. DB-backed deployments
    /// override this with a query against `api_tokens.space_ids`.
    async fn check_scope(&self, _token_hash: &str, _space_id: &str) -> Result<bool> {
        Ok(true)
    }
}
