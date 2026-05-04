//! Persistent storage trait for binary blob content.

use async_trait::async_trait;

/// A stored binary blob.
///
/// The relay is format-agnostic — MIME content type is not part of the sync
/// protocol and is tracked by the UX server (Postgres) via sideband detection.
#[derive(Debug, Clone)]
pub struct BlobRecord {
    /// Raw blob bytes.
    pub data: Vec<u8>,
    /// Content hash for deduplication / integrity.
    pub hash: Vec<u8>,
    /// LWW timestamp (milliseconds since epoch).
    pub timestamp: i64,
}

/// Persistent storage for binary blobs (imported files: PDFs, images, etc.).
///
/// Blobs are write-once, last-writer-wins. The relay stores the full blob
/// bytes along with metadata for catch-up on subscribe.
#[async_trait]
pub trait BlobBackend: Send + Sync + 'static {
    /// Load a persisted blob. Returns `None` if no blob has been saved.
    async fn load(&self, space_id: &str, doc_id: &str) -> anyhow::Result<Option<BlobRecord>>;

    /// Persist a blob.
    async fn save(&self, space_id: &str, doc_id: &str, blob: &BlobRecord) -> anyhow::Result<()>;

    /// Delete a persisted blob.
    async fn delete(&self, space_id: &str, doc_id: &str) -> anyhow::Result<()>;
}
