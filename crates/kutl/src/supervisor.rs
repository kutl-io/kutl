//! Global daemon supervisor.
//!
//! Reads `$KUTL_HOME/spaces.json`, spawns a [`SpaceWorker`](kutl_daemon::SpaceWorker)
//! task per registered space, handles SIGHUP to reload the space list, and
//! SIGTERM/ctrl-c to shut down all workers. If a worker exits unexpectedly
//! (crash or error), it is respawned automatically.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::space::{SpaceConfig, SpaceRegistry};

/// Maximum consecutive respawn failures before giving up on a space.
const MAX_RESPAWN_FAILURES: u32 = 5;

/// Tracks a running space worker task.
struct WorkerHandle {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

/// Build a [`SpaceWorkerConfig`](kutl_daemon::SpaceWorkerConfig) for a space
/// directory.
///
/// Canonicalizes the path, loads the space config from `.kutl/space.json`,
/// and assembles the worker config. Returns `None` with a warning log if
/// the path is invalid or the space config cannot be loaded.
fn build_worker_config(
    space_path: &str,
    did: &str,
    signing_key: Option<&ed25519_dalek::SigningKey>,
    display_name: &str,
) -> Option<kutl_daemon::SpaceWorkerConfig> {
    let path = PathBuf::from(space_path);
    let canonical = match path.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            warn!(path = %space_path, error = %e, "skipping space: cannot resolve path");
            return None;
        }
    };

    let config = match SpaceConfig::load(&canonical) {
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

    Some(kutl_daemon::SpaceWorkerConfig {
        space_root: canonical,
        author_did: did.to_owned(),
        relay_url: config.relay_url,
        space_id: config.space_id,
        signing_key: signing_key.cloned(),
        one_shot: false,
        display_name: display_name.to_owned(),
        ready: None,
        cancel: CancellationToken::new(),
    })
}

/// Build a per-space tracing subscriber that writes to `$KUTL_HOME/logs/<space_id>.log`.
///
/// Falls back to the global subscriber (stdout) if the log file cannot be opened.
fn build_space_subscriber(space_id: &str) -> Option<tracing::Dispatch> {
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    let log_path = match kutl_daemon::telemetry::space_log_path(space_id) {
        Ok(p) => p,
        Err(e) => {
            warn!(space_id = %space_id, error = %e, "cannot create per-space log path, using global log");
            return None;
        }
    };
    let file = match kutl_daemon::telemetry::open_log_file(&log_path) {
        Ok(f) => f,
        Err(e) => {
            warn!(space_id = %space_id, error = %e, "cannot open per-space log file, using global log");
            return None;
        }
    };

    let filter = kutl_relay::telemetry::build_env_filter();

    let subscriber = tracing_subscriber::registry().with(
        fmt::layer()
            .json()
            .with_ansi(false)
            .with_target(true)
            .with_writer(file)
            .with_filter(filter),
    );

    Some(tracing::Dispatch::new(subscriber))
}

/// Spawn a tokio task for a space worker.
///
/// Returns a [`WorkerHandle`] that can be used to cancel and await the task.
/// When the worker exits (for any reason), it sends its path on `exit_tx`
/// so the supervisor can detect unexpected exits and respawn.
///
/// Each worker gets its own per-space log file at `$KUTL_HOME/logs/<space_id>.log`.
fn spawn_worker(
    config: kutl_daemon::SpaceWorkerConfig,
    exit_tx: mpsc::UnboundedSender<String>,
) -> WorkerHandle {
    let cancel = config.cancel.clone();
    let space_id = config.space_id.clone();
    let path = config.space_root.display().to_string();
    let path_for_exit = path.clone();
    let dispatch = build_space_subscriber(&space_id);

    let task = tokio::spawn(async move {
        info!(space_id = %space_id, path = %path, "starting space worker");

        let result = if let Some(dispatch) = dispatch {
            // Run with per-space log subscriber.
            tracing::dispatcher::with_default(&dispatch, || {
                info!(space_id = %space_id, path = %path, "space worker log started");
            });
            // The worker is async, so we set the default for the current task.
            let _guard = tracing::dispatcher::set_default(&dispatch);
            kutl_daemon::run(config).await
        } else {
            kutl_daemon::run(config).await
        };

        match result {
            Ok(()) => info!(space_id = %space_id, path = %path, "space worker stopped"),
            Err(e) => error!(space_id = %space_id, path = %path, error = %e, "space worker failed"),
        }
        // Notify supervisor that this worker exited.
        let _ = exit_tx.send(path_for_exit);
    });

    WorkerHandle { cancel, task }
}

/// Load the global space registry.
///
/// Returns an empty registry if the file does not exist.
fn load_registry() -> Result<SpaceRegistry> {
    let path = crate::space::registry_path()?;
    SpaceRegistry::load(&path)
}

/// Reload the space list and reconcile running workers.
///
/// - New paths in the registry get a worker spawned.
/// - Paths removed from the registry get their worker cancelled and awaited.
/// - Paths already running are left untouched.
async fn reload(
    workers: &mut HashMap<String, WorkerHandle>,
    did: &str,
    signing_key: Option<&ed25519_dalek::SigningKey>,
    display_name: &str,
    exit_tx: &mpsc::UnboundedSender<String>,
    crash_counts: &mut HashMap<String, u32>,
) {
    let registry = match load_registry() {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "failed to reload space registry");
            return;
        }
    };

    let new_paths: std::collections::HashSet<&str> =
        registry.spaces.iter().map(String::as_str).collect();
    let current_paths: Vec<String> = workers.keys().cloned().collect();

    // Stop workers for removed spaces.
    for path in &current_paths {
        if !new_paths.contains(path.as_str()) {
            info!(path = %path, "space removed, stopping worker");
            if let Some(handle) = workers.remove(path) {
                handle.cancel.cancel();
                if let Err(e) = handle.task.await {
                    error!(path = %path, error = %e, "worker task panicked during removal");
                }
            }
            crash_counts.remove(path);
        }
    }

    // Start workers for new spaces.
    for space_path in &registry.spaces {
        if workers.contains_key(space_path) {
            continue;
        }
        if let Some(config) = build_worker_config(space_path, did, signing_key, display_name) {
            info!(path = %space_path, space_id = %config.space_id, "new space registered, starting worker");
            let handle = spawn_worker(config, exit_tx.clone());
            workers.insert(space_path.clone(), handle);
            crash_counts.remove(space_path);
        }
    }

    info!(active_workers = workers.len(), "reload complete");
    kutl_daemon::metrics_calls::record_active_spaces(workers.len() as u64);
}

/// Run the global daemon supervisor.
///
/// Loads the space registry, spawns a worker per space, then enters a
/// signal loop: SIGHUP reloads the space list, SIGTERM/ctrl-c shuts
/// everything down. If a worker exits unexpectedly (not via cancellation),
/// it is respawned automatically up to [`MAX_RESPAWN_FAILURES`] times.
pub async fn run(
    did: String,
    signing_key: Option<ed25519_dalek::SigningKey>,
    display_name: String,
) -> Result<()> {
    let mut workers: HashMap<String, WorkerHandle> = HashMap::new();
    let mut crash_counts: HashMap<String, u32> = HashMap::new();

    // Channel for worker exit notifications.
    let (exit_tx, mut exit_rx) = mpsc::unbounded_channel::<String>();

    // Initial load and spawn.
    let registry = load_registry()?;
    for space_path in &registry.spaces {
        if let Some(config) =
            build_worker_config(space_path, &did, signing_key.as_ref(), &display_name)
        {
            info!(path = %space_path, space_id = %config.space_id, "starting worker");
            let handle = spawn_worker(config, exit_tx.clone());
            workers.insert(space_path.clone(), handle);
        }
    }

    info!(active_workers = workers.len(), "supervisor started");
    kutl_daemon::metrics_calls::record_active_spaces(workers.len() as u64);

    // Signal loop.
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .context("failed to register SIGHUP handler")?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to register SIGTERM handler")?;

    loop {
        tokio::select! {
            _ = sighup.recv() => {
                info!("received SIGHUP, reloading space registry");
                reload(&mut workers, &did, signing_key.as_ref(), &display_name, &exit_tx, &mut crash_counts).await;
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("received ctrl-c, shutting down");
                break;
            }
            Some(path) = exit_rx.recv() => {
                if let Some(handle) = workers.remove(&path) {
                    // Wait for the task to fully complete.
                    let panicked = handle.task.await.is_err();

                    // Only respawn if the worker wasn't deliberately cancelled.
                    if handle.cancel.is_cancelled() {
                        continue;
                    }

                    let count = crash_counts.entry(path.clone()).or_insert(0);
                    *count += 1;

                    if *count > MAX_RESPAWN_FAILURES {
                        error!(
                            path = %path,
                            failures = *count,
                            "worker exceeded max respawn attempts, giving up"
                        );
                        continue;
                    }

                    if panicked {
                        error!(path = %path, attempt = *count, "worker panicked, respawning");
                    } else {
                        warn!(path = %path, attempt = *count, "worker exited unexpectedly, respawning");
                    }

                    if let Some(config) = build_worker_config(&path, &did, signing_key.as_ref(), &display_name) {
                        let new_handle = spawn_worker(config, exit_tx.clone());
                        workers.insert(path, new_handle);
                    }
                }
            }
        }
    }

    // Cancel all workers, then await their tasks.
    for handle in workers.values() {
        handle.cancel.cancel();
    }
    for (path, handle) in workers.drain() {
        info!(path = %path, "waiting for worker to stop");
        if let Err(e) = handle.task.await {
            error!(path = %path, error = %e, "worker task panicked during shutdown");
        }
    }

    info!("supervisor stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::space::SpaceConfig;

    #[test]
    fn test_build_worker_config_valid_space() {
        let dir = tempfile::tempdir().unwrap();
        let config = SpaceConfig {
            space_id: "test-space-001".into(),
            relay_url: "ws://127.0.0.1:9100/ws".into(),
        };
        config.save(dir.path()).unwrap();

        let result =
            build_worker_config(dir.path().to_str().unwrap(), "did:key:test", None, "tester");

        let worker_config = result.expect("should return Some for valid space");
        assert_eq!(worker_config.space_id, "test-space-001");
        assert_eq!(worker_config.relay_url, "ws://127.0.0.1:9100/ws");
        assert_eq!(worker_config.author_did, "did:key:test");
        assert_eq!(worker_config.display_name, "tester");
        assert!(!worker_config.one_shot);
    }

    #[test]
    fn test_build_worker_config_missing_path() {
        let result = build_worker_config(
            "/nonexistent/path/that/does/not/exist",
            "did:key:test",
            None,
            "tester",
        );
        assert!(result.is_none(), "should return None for nonexistent path");
    }

    #[test]
    fn test_build_worker_config_missing_space_json() {
        let dir = tempfile::tempdir().unwrap();
        // Dir exists but has no .kutl/space.json
        let result =
            build_worker_config(dir.path().to_str().unwrap(), "did:key:test", None, "tester");
        assert!(
            result.is_none(),
            "should return None when space.json is missing"
        );
    }

    #[tokio::test]
    async fn test_cancel_stops_worker() {
        let dir = tempfile::tempdir().unwrap();
        let config = SpaceConfig {
            space_id: "cancel-test".into(),
            relay_url: "ws://127.0.0.1:1/bogus".into(),
        };
        config.save(dir.path()).unwrap();

        let worker_config =
            build_worker_config(dir.path().to_str().unwrap(), "did:key:test", None, "tester")
                .expect("valid space config");

        let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
        let handle = spawn_worker(worker_config, exit_tx);

        // Give the task a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!handle.task.is_finished(), "worker should still be running");

        // Cancel the worker. The reconnect loop checks the cancel token
        // during backoff sleeps, so cancellation works even with a bogus relay.
        handle.cancel.cancel();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle.task).await;
        assert!(
            result.is_ok(),
            "worker should stop within 5 seconds after cancel"
        );
        // Cancel triggers graceful shutdown, so the task returns Ok(()).
        result.unwrap().unwrap();
    }
}
