//! Session persistence backend trait.
//!
//! The OSS relay passes `None` (in-memory-only sessions via [`AuthStore`]).
//! kutlhub-relay provides a Postgres-backed implementation.

use anyhow::Result;
use async_trait::async_trait;

/// Backend for persisting relay sessions across restarts.
#[async_trait]
pub trait SessionBackend: Send + Sync + 'static {
    /// Persist a new session.
    async fn create(&self, token_hash: &str, did: &str, expires_at_ms: i64) -> Result<()>;

    /// Validate a session by token hash. Returns the DID if valid and not expired.
    async fn validate(&self, token_hash: &str, now_ms: i64) -> Result<Option<String>>;

    /// Delete expired sessions. Returns the number removed.
    async fn purge_expired(&self, now_ms: i64) -> Result<u64>;
}
