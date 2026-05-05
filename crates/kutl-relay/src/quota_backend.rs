//! Storage quota backend trait (RFD 0063).
//!
//! The OSS relay does not enforce quotas on its own — it exposes this trait
//! for kutlhub-relay (and future hosts) to plug in a Postgres-backed
//! implementation. When the trait object is `None`, `handle_blob_sync_ops`
//! and the flush task skip all quota checks.

use anyhow::Result;
use async_trait::async_trait;

/// Quota snapshot for the space owner, read per durable-write attempt.
#[derive(Debug, Clone)]
pub struct SpaceQuota {
    /// Account UUID of the space owner (as string).
    pub owner_account_id: String,
    /// Current total durable bytes billed to the owner.
    pub storage_bytes_used: i64,
    /// Tier cap for the owner.
    pub storage_bytes_total: i64,
    /// Tier cap for a single blob upload.
    pub per_blob_size: i64,
}

/// Outcome of applying a storage delta.
#[derive(Debug, Clone, Copy)]
pub struct StorageDeltaResult {
    /// The account's new `storage_bytes_used` value post-update.
    pub new_used: i64,
    /// The signed byte change that was applied (`new_size - prior_size`).
    pub delta: i64,
}

/// Per-tenant storage quota backend.
#[async_trait]
pub trait QuotaBackend: Send + Sync + 'static {
    /// Fetch the space owner's current quota state.
    ///
    /// Returns `None` if the space does not exist or was deleted (callers
    /// should treat as `NOT_FOUND` and reject the write).
    async fn load_space_quota(&self, space_id: &str) -> Result<Option<SpaceQuota>>;

    /// Update `documents.size_bytes` and apply the delta to the owner's
    /// `accounts.storage_bytes_used` atomically. Reads the prior size under
    /// `FOR UPDATE` to keep the delta correct across concurrent writers.
    ///
    /// Idempotent at the logical level: setting `size_bytes` to the same
    /// value as the current row yields `delta = 0`. If the document or
    /// space has been soft-deleted, returns `delta = 0` without touching
    /// the counter.
    async fn apply_storage_delta(
        &self,
        space_id: &str,
        doc_id: &str,
        new_size: i64,
    ) -> Result<StorageDeltaResult>;
}
