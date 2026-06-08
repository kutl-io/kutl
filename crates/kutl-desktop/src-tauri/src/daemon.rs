//! In-process daemon supervisor for the kutl desktop application.
//!
//! Manages per-space sync workers directly in the Tauri process, reading
//! `~/.kutl/spaces.json` to discover registered spaces and spawning a
//! [`kutl_daemon::SpaceWorker`] for each one. Provides PID-based mutual
//! exclusion with the CLI daemon (`daemon.pid` / `desktop.pid`).

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

// ── Constants ───────────────────────────────────────────────────────

/// PID file name for the desktop app within `~/.kutl/`.
const DESKTOP_PID_FILENAME: &str = "desktop.pid";

/// PID file name for the CLI daemon within `~/.kutl/`.
const CLI_DAEMON_PID_FILENAME: &str = "daemon.pid";

/// Delay after spawning a worker before promoting its state to `Synced`.
///
/// If the worker hasn't exited after this duration, we assume it connected
/// successfully. This is a v1 heuristic — a proper status channel is a
/// future improvement.
const SYNC_HEURISTIC_DELAY: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(5));

// ── Public types ────────────────────────────────────────────────────

/// Current sync state of a space worker.
#[derive(Debug, Clone)]
pub enum SyncState {
    /// Worker spawned, waiting for relay connection.
    Connecting,
    /// Worker has been running long enough to assume connectivity.
    Synced,
    /// Worker exited with an error (message available for future UI display).
    Error(String),
}

/// Snapshot of a single space's status.
#[derive(Debug, Clone)]
pub struct SpaceStatus {
    /// Space identifier from `.kutl/space.json`.
    pub space_id: String,
    /// Human-readable space name.
    pub space_name: String,
    /// Absolute path to the space root directory.
    pub space_root: PathBuf,
    /// Relay WebSocket URL.
    pub relay_url: String,
    /// Current sync state.
    pub state: SyncState,
}

/// Per-space worker handle with metadata for status reporting.
struct WorkerHandle {
    space_name: String,
    space_root: PathBuf,
    relay_url: String,
    task: JoinHandle<()>,
    cancel: CancellationToken,
    state: SyncState,
}

/// In-process daemon supervisor that manages sync workers for all registered spaces.
///
/// Stored as `Arc<tokio::sync::Mutex<DaemonSupervisor>>` in Tauri state so that
/// async methods can be called from Tauri commands and the setup hook.
pub struct DaemonSupervisor {
    /// Running workers keyed by space ID.
    workers: HashMap<String, WorkerHandle>,
    /// Global cancellation token — cancelling this stops all workers.
    cancel: CancellationToken,
}

// ── Identity helpers ────────────────────────────────────────────────

/// Loaded identity: DID string, optional signing key, and display name.
pub struct LoadedIdentity {
    /// `did:key:z6Mk...` identifier.
    pub did: String,
    /// Ed25519 signing key decoded from the identity file.
    pub signing_key: Option<ed25519_dalek::SigningKey>,
    /// Human-readable display name from the identity file.
    pub display_name: String,
}

/// Load the identity from `~/.kutl/identity.json`.
///
/// Returns the DID, decoded signing key, and display name. If the
/// identity file doesn't exist, returns an error — the user needs to
/// run `kutl init` first (or the desktop app should guide them through
/// onboarding in a later task).
pub fn load_identity() -> Result<LoadedIdentity> {
    let path = kutl_client::default_identity_path()?;

    let id = kutl_client::Identity::load(&path)
        .with_context(|| format!("failed to read identity from {}", path.display()))?;

    let signing_key = match id.decode_signing_key() {
        Ok(key) => Some(key),
        Err(e) => {
            warn!(error = %e, "failed to decode signing key, continuing without auth");
            None
        }
    };

    Ok(LoadedIdentity {
        did: id.did,
        signing_key,
        display_name: id.display_name.unwrap_or_default(),
    })
}

// ── Path helpers ────────────────────────────────────────────────────

/// Path to the global space registry (`$KUTL_HOME/spaces.json`).
fn registry_path() -> Result<PathBuf> {
    kutl_client::registry_path()
}

// ── PID file helpers ────────────────────────────────────────────────

/// Check if the CLI daemon is currently running by reading `daemon.pid`.
///
/// Returns `Some(pid)` if the process is alive, `None` otherwise.
/// Removes stale PID files automatically.
pub fn cli_daemon_running() -> Result<Option<u32>> {
    let path = kutl_client::kutl_home()?.join(CLI_DAEMON_PID_FILENAME);
    kutl_client::read_pid_file_alive(&path)
}

/// Write the desktop app's PID to `~/.kutl/desktop.pid`.
pub fn write_desktop_pid() -> Result<()> {
    let path = kutl_client::kutl_home()?.join(DESKTOP_PID_FILENAME);
    kutl_client::write_pid_file(&path, std::process::id())
}

/// Remove the desktop app's PID file (best-effort).
pub fn remove_desktop_pid() {
    match kutl_client::kutl_home() {
        Ok(home) => kutl_client::remove_pid_file(&home.join(DESKTOP_PID_FILENAME)),
        Err(e) => {
            warn!(error = %e, "could not determine PID file path for removal");
        }
    }
}

// ── Worker config builder ───────────────────────────────────────────

/// Build a [`SpaceWorkerConfig`](kutl_daemon::SpaceWorkerConfig) for a space
/// directory.
///
/// Canonicalizes the path, loads `.kutl/space.json`, and assembles the
/// worker config. Returns the config and the human-readable space name.
/// Returns `None` with a warning if the path is invalid or the config
/// cannot be loaded.
pub(crate) fn build_worker_config(
    space_path: &str,
    identity: &LoadedIdentity,
) -> Option<(kutl_daemon::SpaceWorkerConfig, String)> {
    let path = PathBuf::from(space_path);
    let canonical = match path.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            warn!(path = %space_path, error = %e, "skipping space: cannot resolve path");
            return None;
        }
    };

    let config = match kutl_client::SpaceConfig::load(&canonical) {
        Ok(c) => c,
        Err(e) => {
            warn!(
                path = %canonical.display(),
                error = %e,
                "skipping space: failed to load space config"
            );
            return None;
        }
    };

    let space_name = kutl_client::KutlspaceConfig::display_name(&canonical, &config.space_id);

    let worker_config = kutl_daemon::SpaceWorkerConfig {
        space_root: canonical,
        author_did: identity.did.clone(),
        relay_url: config.relay_url,
        space_id: config.space_id,
        signing_key: identity.signing_key.clone(),
        one_shot: false,
        display_name: identity.display_name.clone(),
        ready: None,
        cancel: CancellationToken::new(),
    };

    Some((worker_config, space_name))
}

// ── DaemonSupervisor ────────────────────────────────────────────────

impl DaemonSupervisor {
    /// Create a new supervisor with no workers.
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
            cancel: CancellationToken::new(),
        }
    }

    /// Start a sync worker for a space.
    ///
    /// Spawns the worker in a tokio task and tracks it. `space_name` is
    /// the human-readable name from `space.json` (distinct from the
    /// worker's `display_name`, which is the user's identity name).
    /// If a worker for this space is already running, it is stopped first.
    pub async fn start_space(
        &mut self,
        config: kutl_daemon::SpaceWorkerConfig,
        space_name: String,
    ) -> Result<()> {
        let space_id = config.space_id.clone();

        // Stop existing worker for this space if present.
        if self.workers.contains_key(&space_id) {
            self.stop_space(&space_id).await?;
        }

        let cancel = config.cancel.clone();
        let space_root = config.space_root.clone();
        let relay_url = config.relay_url.clone();
        let worker_space_id = space_id.clone();
        let log_path = space_root.clone();

        let task = tokio::spawn(async move {
            info!(space_id = %worker_space_id, path = %log_path.display(), "starting space worker");
            match kutl_daemon::run(config).await {
                Ok(()) => info!(space_id = %worker_space_id, "space worker stopped"),
                Err(e) => error!(space_id = %worker_space_id, error = %e, "space worker failed"),
            }
        });

        self.workers.insert(
            space_id,
            WorkerHandle {
                space_name,
                space_root,
                relay_url,
                task,
                cancel,
                state: SyncState::Connecting,
            },
        );

        Ok(())
    }

    /// Stop and remove a space worker.
    pub async fn stop_space(&mut self, space_id: &str) -> Result<()> {
        if let Some(handle) = self.workers.remove(space_id) {
            handle.cancel.cancel();
            if let Err(e) = handle.task.await {
                error!(space_id = %space_id, error = %e, "worker task panicked during stop");
            }
        }
        Ok(())
    }

    /// Return the current status of all spaces.
    pub fn space_statuses(&self) -> Vec<SpaceStatus> {
        self.workers
            .iter()
            .map(|(space_id, handle)| {
                // Check if the task has finished — if so, it's an error.
                let state = if handle.task.is_finished() {
                    SyncState::Error("worker exited unexpectedly".into())
                } else {
                    handle.state.clone()
                };

                SpaceStatus {
                    space_id: space_id.clone(),
                    space_name: handle.space_name.clone(),
                    space_root: handle.space_root.clone(),
                    relay_url: handle.relay_url.clone(),
                    state,
                }
            })
            .collect()
    }

    /// Reload the space list from `~/.kutl/spaces.json` and reconcile workers.
    ///
    /// - New spaces get workers spawned.
    /// - Removed spaces get workers stopped.
    /// - Existing spaces are left untouched.
    pub async fn reload_from_registry(&mut self, identity: &LoadedIdentity) -> Result<()> {
        let path = registry_path()?;
        let registry = kutl_client::SpaceRegistry::load(&path)?;

        let new_paths: std::collections::HashSet<&str> =
            registry.spaces.iter().map(String::as_str).collect();

        // Collect space IDs to remove (spaces no longer in registry).
        // We need to map from space_path -> space_id, but our workers are keyed
        // by space_id. Build a reverse map from space_root -> space_id.
        let root_to_id: HashMap<String, String> = self
            .workers
            .iter()
            .map(|(id, h)| (h.space_root.display().to_string(), id.clone()))
            .collect();

        // Find workers whose space_root is no longer in the registry.
        let mut to_remove = Vec::new();
        for (root_str, space_id) in &root_to_id {
            // The registry stores the original (pre-canonicalized) path. We need
            // to check both the raw path and the canonical path.
            let in_registry = new_paths.iter().any(|p| {
                *p == root_str.as_str()
                    || PathBuf::from(p)
                        .canonicalize()
                        .ok()
                        .is_some_and(|c| c.display().to_string() == *root_str)
            });
            if !in_registry {
                to_remove.push(space_id.clone());
            }
        }

        // Stop removed workers.
        for space_id in &to_remove {
            info!(space_id = %space_id, "space removed from registry, stopping worker");
            self.stop_space(space_id).await?;
        }

        // Build a set of canonical paths for currently running workers.
        let running_roots: std::collections::HashSet<String> = self
            .workers
            .values()
            .map(|h| h.space_root.display().to_string())
            .collect();

        // Start workers for new spaces.
        for space_path in &registry.spaces {
            // Check if we already have a worker for this path (by canonical path).
            let canonical = PathBuf::from(space_path)
                .canonicalize()
                .ok()
                .map(|p| p.display().to_string());

            let already_running = canonical
                .as_ref()
                .is_some_and(|c| running_roots.contains(c.as_str()));

            if already_running {
                continue;
            }

            if let Some((config, space_name)) = build_worker_config(space_path, identity) {
                info!(
                    path = %space_path,
                    space_id = %config.space_id,
                    space_name = %space_name,
                    "new space registered, starting worker"
                );
                if let Err(e) = self.start_space(config, space_name).await {
                    error!(path = %space_path, error = %e, "failed to start worker for new space");
                }
            }
        }

        info!(
            active_workers = self.workers.len(),
            "registry reload complete"
        );
        Ok(())
    }

    /// Promote workers that have been running long enough from `Connecting` to `Synced`.
    ///
    /// Called after a delay to implement the v1 heuristic: if a worker
    /// hasn't exited after [`SYNC_HEURISTIC_DELAY`], assume it's connected.
    pub fn promote_connecting_workers(&mut self) {
        for handle in self.workers.values_mut() {
            if matches!(handle.state, SyncState::Connecting) {
                if handle.task.is_finished() {
                    handle.state = SyncState::Error("worker exited during startup".into());
                } else {
                    handle.state = SyncState::Synced;
                }
            }
        }
    }

    /// Gracefully shut down all workers, awaiting each task's completion.
    pub async fn shutdown(&mut self) {
        self.cancel.cancel();
        let space_ids: Vec<String> = self.workers.keys().cloned().collect();
        for space_id in space_ids {
            if let Err(e) = self.stop_space(&space_id).await {
                error!(space_id = %space_id, error = %e, "error stopping worker during shutdown");
            }
        }
    }
}

// ── Tray status summary ────────────────────────────────────────────

/// Overall status for the tray status line.
#[derive(Debug, Clone)]
pub enum OverallStatus {
    /// All spaces are synced.
    Synced,
    /// At least one space is still connecting.
    Syncing,
    /// At least one space has an error.
    Error,
    /// No spaces registered.
    NoSpaces,
    /// CLI daemon is running — desktop supervisor is inactive.
    CliDaemonRunning,
}

/// Compute the overall status from individual space statuses.
pub fn overall_status(statuses: &[SpaceStatus]) -> OverallStatus {
    if statuses.is_empty() {
        return OverallStatus::NoSpaces;
    }

    let mut has_error = false;
    let mut has_connecting = false;

    for status in statuses {
        match &status.state {
            SyncState::Error(_) => has_error = true,
            SyncState::Connecting => has_connecting = true,
            SyncState::Synced => {}
        }
    }

    if has_error {
        OverallStatus::Error
    } else if has_connecting {
        OverallStatus::Syncing
    } else {
        OverallStatus::Synced
    }
}

/// Status line text for the tray menu.
pub fn status_line(status: &OverallStatus) -> &'static str {
    match status {
        OverallStatus::Synced => "kutl \u{2014} Synced",
        OverallStatus::Syncing => "kutl \u{2014} Syncing...",
        OverallStatus::Error => "kutl \u{2014} Error",
        OverallStatus::NoSpaces => "kutl \u{2014} No spaces",
        OverallStatus::CliDaemonRunning => "kutl \u{2014} CLI daemon running",
    }
}

/// Format a space menu item label with sync state indicator.
pub fn space_menu_label(name: &str, state: &SyncState) -> String {
    match state {
        SyncState::Synced => format!("\u{1F4C1} {name}    \u{2713}"),
        SyncState::Connecting => format!("\u{1F4C1} {name}    \u{27F3}"),
        SyncState::Error(msg) => format!("\u{1F4C1} {name}    \u{2717} {msg}"),
    }
}

/// Return the heuristic delay before promoting connecting workers.
pub fn sync_heuristic_delay() -> std::time::Duration {
    SYNC_HEURISTIC_DELAY
}

/// Return the kutl home directory (`$KUTL_HOME` or `~/.kutl`).
///
/// Convenience re-export for use by `main.rs` autostart logic.
pub fn kutl_home() -> Result<PathBuf> {
    kutl_client::kutl_home()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overall_status_empty() {
        assert!(matches!(overall_status(&[]), OverallStatus::NoSpaces));
    }

    #[test]
    fn test_overall_status_all_synced() {
        let statuses = vec![SpaceStatus {
            space_id: "a".into(),
            space_name: "alpha".into(),
            space_root: PathBuf::from("/tmp/a"),
            relay_url: "ws://localhost:9100/ws".into(),
            state: SyncState::Synced,
        }];
        assert!(matches!(overall_status(&statuses), OverallStatus::Synced));
    }

    #[test]
    fn test_overall_status_connecting() {
        let statuses = vec![
            SpaceStatus {
                space_id: "a".into(),
                space_name: "alpha".into(),
                space_root: PathBuf::from("/tmp/a"),
                relay_url: "ws://localhost:9100/ws".into(),
                state: SyncState::Synced,
            },
            SpaceStatus {
                space_id: "b".into(),
                space_name: "beta".into(),
                space_root: PathBuf::from("/tmp/b"),
                relay_url: "ws://localhost:9100/ws".into(),
                state: SyncState::Connecting,
            },
        ];
        assert!(matches!(overall_status(&statuses), OverallStatus::Syncing));
    }

    #[test]
    fn test_overall_status_error_takes_priority() {
        let statuses = vec![
            SpaceStatus {
                space_id: "a".into(),
                space_name: "alpha".into(),
                space_root: PathBuf::from("/tmp/a"),
                relay_url: "ws://localhost:9100/ws".into(),
                state: SyncState::Connecting,
            },
            SpaceStatus {
                space_id: "b".into(),
                space_name: "beta".into(),
                space_root: PathBuf::from("/tmp/b"),
                relay_url: "ws://localhost:9100/ws".into(),
                state: SyncState::Error("test error".into()),
            },
        ];
        assert!(matches!(overall_status(&statuses), OverallStatus::Error));
    }

    #[test]
    fn test_status_line_text() {
        assert_eq!(status_line(&OverallStatus::Synced), "kutl \u{2014} Synced");
        assert_eq!(
            status_line(&OverallStatus::Syncing),
            "kutl \u{2014} Syncing..."
        );
        assert_eq!(status_line(&OverallStatus::Error), "kutl \u{2014} Error");
        assert_eq!(
            status_line(&OverallStatus::NoSpaces),
            "kutl \u{2014} No spaces"
        );
        assert_eq!(
            status_line(&OverallStatus::CliDaemonRunning),
            "kutl \u{2014} CLI daemon running"
        );
    }

    #[test]
    fn test_space_menu_label_synced() {
        let label = space_menu_label("my-project", &SyncState::Synced);
        assert!(label.contains("my-project"));
        assert!(label.contains('\u{2713}'));
    }

    #[test]
    fn test_space_menu_label_connecting() {
        let label = space_menu_label("my-project", &SyncState::Connecting);
        assert!(label.contains('\u{27F3}'));
    }

    #[test]
    fn test_space_menu_label_error() {
        let label = space_menu_label("my-project", &SyncState::Error("oops".into()));
        assert!(label.contains('\u{2717}'));
    }

    #[test]
    fn test_supervisor_new_empty() {
        let supervisor = DaemonSupervisor::new();
        assert!(supervisor.workers.is_empty());
        let statuses = supervisor.space_statuses();
        assert!(statuses.is_empty());
    }

    #[test]
    fn test_space_menu_label_error_includes_message() {
        let label = space_menu_label("my-project", &SyncState::Error("connection refused".into()));
        assert!(label.contains("connection refused"));
    }

    #[tokio::test]
    async fn test_supervisor_shutdown_empty() {
        let mut supervisor = DaemonSupervisor::new();
        // Shutdown on an empty supervisor should complete without error.
        supervisor.shutdown().await;
        assert!(supervisor.workers.is_empty());
    }
}
