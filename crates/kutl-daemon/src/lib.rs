//! kutl-daemon — per-space file watcher and sync client for kutl.

mod blob_state;
pub mod bridge;
pub mod case_collision;
pub mod client;
pub mod daemon;
mod documents;
pub mod inode;
mod reconcile;
mod safe_path;
pub mod state;
pub mod telemetry;
mod watcher;

// Re-exports for integration testing and external consumers.
pub use blob_state::sha256_bytes;
pub use daemon::{SpaceWorker, SpaceWorkerConfig};
pub use documents::DocumentManager;
pub use safe_path::SafeRelayPath;
pub use watcher::{FileEvent, FileWatcher};

/// Run a space worker with the given configuration.
///
/// Handles persistent mode (reconnect loop) or one-shot mode based on
/// `config.one_shot`. Returns `Ok` on graceful shutdown, `Err` on
/// unrecoverable errors.
pub async fn run(config: SpaceWorkerConfig) -> anyhow::Result<()> {
    SpaceWorker::new(config)?.run().await
}
