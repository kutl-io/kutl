//! Persistent storage trait for CRDT document content.

use async_trait::async_trait;

/// Persistent storage for CRDT document content (`.dt` encoded bytes).
///
/// Implementations receive the output of `Document::encode_full()` and must
/// return it verbatim on load. The relay treats this as an opaque blob.
#[async_trait]
pub trait ContentBackend: Send + Sync + 'static {
    /// Load persisted document content. Returns `None` if no content has been saved.
    async fn load(&self, space_id: &str, doc_id: &str) -> anyhow::Result<Option<Vec<u8>>>;

    /// Persist document content (full `encode_full()` snapshot).
    async fn save(&self, space_id: &str, doc_id: &str, data: &[u8]) -> anyhow::Result<()>;

    /// Delete persisted document content.
    async fn delete(&self, space_id: &str, doc_id: &str) -> anyhow::Result<()>;
}
