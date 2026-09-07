//! Global daemon supervisor.
//!
//! Reads `$KUTL_HOME/spaces.toml`, spawns a [`SpaceWorker`](kutl_daemon::SpaceWorker)
//! task per registered space, handles SIGHUP to reload the space list, and
//! SIGTERM/ctrl-c to shut down all workers. If a worker exits unexpectedly
//! (crash or error), it is respawned automatically.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use kutl_daemon::SpaceWorkerConfig;

use crate::space::SpaceRegistry;

/// Maximum consecutive respawn failures before giving up on a space.
const MAX_RESPAWN_FAILURES: u32 = 5;

/// Base pause before the first respawn attempt; doubles per consecutive
/// failure (2s, 4s, … 32s). A failed worker CONSTRUCTION is near-instant —
/// notably the producer-flock refusal when a one-shot `kutl sync` holds the
/// space's lock — so without a pause the whole respawn budget burns in
/// milliseconds while the transient holder still has seconds to run. The
/// doubling schedule spans over a minute across the budget, which outlives
/// any one-shot sync.
const RESPAWN_BACKOFF_BASE: std::time::Duration = std::time::Duration::from_secs(2);

/// A worker that stayed up at least this long was healthy: its eventual exit
/// starts a FRESH failure streak instead of extending an old one. Without
/// this reset the budget is cumulative over the supervisor's lifetime, and
/// [`MAX_RESPAWN_FAILURES`] crashes EVER — even weeks apart — would
/// permanently drop the space.
const HEALTHY_UPTIME: std::time::Duration = std::time::Duration::from_mins(1);

/// Pause before respawn attempt `count` (1-based): exponential doubling from
/// [`RESPAWN_BACKOFF_BASE`].
fn respawn_delay(count: u32) -> std::time::Duration {
    RESPAWN_BACKOFF_BASE * 2u32.saturating_pow(count.saturating_sub(1))
}

/// Tracks a running space worker task.
struct WorkerHandle {
    cancel: CancellationToken,
    /// Identifies this worker instance among the supervisor's tasks. A path
    /// can be removed and re-added across reloads, so an arriving exit counts
    /// only when its task id still matches the worker mapped for that path —
    /// otherwise it belongs to an instance already replaced or torn down.
    abort: tokio::task::AbortHandle,
    /// When this instance was spawned — an exit after [`HEALTHY_UPTIME`]
    /// resets the failure streak instead of extending it.
    spawned_at: std::time::Instant,
}

/// The space path whose CURRENT worker is the task that just finished.
///
/// `None` means the exit is stale — the path was unregistered, or a newer
/// worker already replaced this instance — and acting on it would evict the
/// live worker. Matching on the task id is what makes that decision: ids are
/// unique per spawn, so a replaced instance can never be mistaken for its
/// successor.
fn path_for_task(workers: &HashMap<String, WorkerHandle>, id: tokio::task::Id) -> Option<String> {
    workers
        .iter()
        .find(|(_, handle)| handle.abort.id() == id)
        .map(|(path, _)| path.clone())
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

/// Spawn a space worker into the supervisor's task set.
///
/// The set is what detects exits: every termination — a clean return, an
/// error, a panic, a cancellation — surfaces from `join_next_with_id`, and by
/// the time it does the task has fully finished, so its producer lock is
/// already released and a respawn cannot race it.
///
/// Each worker gets its own per-space log file at `$KUTL_HOME/logs/<space_id>.log`.
fn spawn_worker(
    tasks: &mut tokio::task::JoinSet<()>,
    config: kutl_daemon::SpaceWorkerConfig,
) -> WorkerHandle {
    let cancel = config.cancel.clone();
    let space_id = config.space_id.clone();
    let path = config.space_root.display().to_string();
    let dispatch = build_space_subscriber(&space_id);

    // The dispatch difference is confined to the worker FUTURE; box it so the
    // spawn + notify plumbing appears once.
    let worker: std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> =
        if let Some(dispatch) = dispatch {
            use tracing::instrument::WithSubscriber as _;
            // The worker is async. A thread-local default (`set_default`) does NOT
            // follow a future across its `.await` points — the executor can resume
            // the task on another thread, and even on the same thread the default
            // is only active between polls of *this* stack frame, not inside the
            // awaited worker future. So `set_default` captured only the worker's
            // synchronous startup and lost every event-loop log. `with_subscriber`
            // attaches the dispatch to the future itself, entering it around every
            // poll, so all of the worker's logs (incl. across awaits) reach the
            // per-space file.
            tracing::dispatcher::with_default(&dispatch, || {
                info!(space_id = %space_id, path = %path, "space worker log started");
            });
            Box::pin(kutl_daemon::run(config).with_subscriber(dispatch))
        } else {
            Box::pin(kutl_daemon::run(config))
        };
    let abort = tasks.spawn(run_worker(worker, space_id, path));

    WorkerHandle {
        cancel,
        abort,
        spawned_at: std::time::Instant::now(),
    }
}

/// Run a worker future to completion, logging how it ended.
async fn run_worker<F>(worker: F, space_id: String, path: String)
where
    F: std::future::Future<Output = Result<()>>,
{
    info!(space_id = %space_id, path = %path, "starting space worker");
    match worker.await {
        Ok(()) => info!(space_id = %space_id, path = %path, "space worker stopped"),
        Err(e) => error!(space_id = %space_id, path = %path, error = %e, "space worker failed"),
    }
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
fn reload(
    workers: &mut HashMap<String, WorkerHandle>,
    tasks: &mut tokio::task::JoinSet<()>,
    did: &str,
    signing_key: Option<&ed25519_dalek::SigningKey>,
    display_name: &str,
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

    // Stop workers for removed spaces. Dropping the handle unmaps the path,
    // so the cancelled task's eventual exit reads as stale and is ignored —
    // no await needed here, and the loop keeps serving signals meanwhile.
    for path in &current_paths {
        if !new_paths.contains(path.as_str()) {
            info!(path = %path, "space removed, stopping worker");
            if let Some(handle) = workers.remove(path) {
                handle.cancel.cancel();
            }
            crash_counts.remove(path);
        }
    }

    // Start workers for new spaces.
    for space_path in &registry.spaces {
        if workers.contains_key(space_path) {
            continue;
        }
        if let Some(config) = worker_config(space_path, did, signing_key, display_name) {
            info!(path = %space_path, space_id = %config.space_id, "new space registered, starting worker");
            let handle = spawn_worker(tasks, config);
            workers.insert(space_path.clone(), handle);
            crash_counts.remove(space_path);
        }
    }

    info!(active_workers = workers.len(), "reload complete");
    kutl_daemon::metrics_calls::record_active_spaces(workers.len() as u64);
}

/// [`SpaceWorkerConfig::for_space`] under the supervisor's policy for a
/// space that cannot be configured: warn and skip it rather than fail the
/// others.
fn worker_config(
    space_path: &str,
    did: &str,
    signing_key: Option<&ed25519_dalek::SigningKey>,
    display_name: &str,
) -> Option<SpaceWorkerConfig> {
    SpaceWorkerConfig::for_space(Path::new(space_path), did, signing_key, display_name, false)
        .inspect_err(|e| warn!(path = %space_path, error = %format!("{e:#}"), "skipping space"))
        .ok()
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
    // Owns every worker task; joining it IS the exit notification.
    let mut tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();

    // Initial load and spawn.
    let registry = load_registry()?;
    for space_path in &registry.spaces {
        if let Some(config) = worker_config(space_path, &did, signing_key.as_ref(), &display_name) {
            info!(path = %space_path, space_id = %config.space_id, "starting worker");
            let handle = spawn_worker(&mut tasks, config);
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
                reload(&mut workers, &mut tasks, &did, signing_key.as_ref(), &display_name, &mut crash_counts);
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("received ctrl-c, shutting down");
                break;
            }
            Some(joined) = tasks.join_next_with_id() => {
                // The task has fully finished by the time it joins — including
                // a panicking one, which arrives as Err carrying its id.
                let (id, panicked) = match joined {
                    Ok((id, ())) => (id, false),
                    Err(e) => (e.id(), true),
                };

                // An exit from an instance no longer mapped for its path was
                // cancelled or replaced; acting on it would evict the live
                // worker that took its place.
                let Some(path) = path_for_task(&workers, id) else {
                    info!(panicked, "ignoring exit from a replaced or removed worker");
                    continue;
                };

                if let Some(handle) = workers.remove(&path) {
                    // Only respawn if the worker wasn't deliberately cancelled.
                    if handle.cancel.is_cancelled() {
                        continue;
                    }

                    let count = crash_counts.entry(path.clone()).or_insert(0);
                    // A healthy stretch of uptime ends the failure streak:
                    // the budget bounds CONSECUTIVE rapid failures, not
                    // crashes accumulated over the supervisor's lifetime.
                    if handle.spawned_at.elapsed() >= HEALTHY_UPTIME {
                        *count = 0;
                    }
                    *count += 1;
                    let count = *count;

                    if count > MAX_RESPAWN_FAILURES {
                        error!(
                            path = %path,
                            failures = count,
                            "worker exceeded max respawn attempts, giving up"
                        );
                        // The space is now permanently unwatched (removed above,
                        // not replaced): reflect the shrunk fleet so the gauge
                        // does not keep counting a dead space as active.
                        kutl_daemon::metrics_calls::record_active_spaces(workers.len() as u64);
                        continue;
                    }

                    if panicked {
                        error!(path = %path, attempt = count, "worker panicked, respawning");
                    } else {
                        warn!(path = %path, attempt = count, "worker exited unexpectedly, respawning");
                    }

                    // Pause before the attempt so a transiently held producer
                    // flock (a one-shot sync on this space) clears instead of
                    // the whole budget burning in milliseconds on instant
                    // construction refusals. Shutdown stays prompt: the pause
                    // races the shutdown signals.
                    tokio::select! {
                        () = tokio::time::sleep(respawn_delay(count)) => {}
                        _ = sigterm.recv() => {
                            info!("received SIGTERM during respawn pause, shutting down");
                            break;
                        }
                        _ = tokio::signal::ctrl_c() => {
                            info!("received ctrl-c during respawn pause, shutting down");
                            break;
                        }
                    }

                    if let Some(config) = worker_config(&path, &did, signing_key.as_ref(), &display_name) {
                        let new_handle = spawn_worker(&mut tasks, config);
                        workers.insert(path, new_handle);
                    }
                }
            }
        }
    }

    // Cancel every worker, then drain the set so each has finished — and its
    // producer lock is released — before the process exits.
    for handle in workers.values() {
        handle.cancel.cancel();
    }
    info!(workers = workers.len(), "waiting for workers to stop");
    while let Some(joined) = tasks.join_next().await {
        if let Err(e) = joined {
            error!(error = %e, "worker task panicked during shutdown");
        }
    }

    info!("supervisor stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::space::SpaceConfig;

    /// Map one path to a trivial task in `tasks`, for testing the
    /// stale-exit decision without a real space worker.
    fn dummy_worker(
        workers: &mut HashMap<String, WorkerHandle>,
        tasks: &mut tokio::task::JoinSet<()>,
        path: &str,
    ) -> tokio::task::Id {
        let abort = tasks.spawn(std::future::pending());
        let id = abort.id();
        workers.insert(
            path.to_owned(),
            WorkerHandle {
                cancel: CancellationToken::new(),
                abort,
                spawned_at: std::time::Instant::now(),
            },
        );
        id
    }

    #[test]
    fn test_respawn_delay_doubles_per_attempt() {
        // The pause is what lets a transiently held producer flock (a
        // one-shot sync on the same space) clear before an attempt burns
        // respawn budget: the budget's attempts must span tens of seconds,
        // not milliseconds.
        assert_eq!(respawn_delay(1), std::time::Duration::from_secs(2));
        assert_eq!(respawn_delay(2), std::time::Duration::from_secs(4));
        assert_eq!(respawn_delay(5), std::time::Duration::from_secs(32));
        // Total span across the whole budget outlives any one-shot sync.
        let total: std::time::Duration = (1..=MAX_RESPAWN_FAILURES).map(respawn_delay).sum();
        assert!(
            total >= std::time::Duration::from_mins(1),
            "budget span must outlive a one-shot sync, got {total:?}"
        );
    }

    #[tokio::test]
    async fn test_path_for_task_resolves_the_mapped_worker() {
        let mut workers = HashMap::new();
        let mut tasks = tokio::task::JoinSet::new();
        let id = dummy_worker(&mut workers, &mut tasks, "/spaces/a");

        assert_eq!(path_for_task(&workers, id).as_deref(), Some("/spaces/a"));
    }

    #[tokio::test]
    async fn test_path_for_task_rejects_a_replaced_instance() {
        // A path re-spawned under the same key: the OLD instance's exit must
        // not resolve, or it would evict the live worker that replaced it.
        let mut workers = HashMap::new();
        let mut tasks = tokio::task::JoinSet::new();
        let old = dummy_worker(&mut workers, &mut tasks, "/spaces/a");
        let new = dummy_worker(&mut workers, &mut tasks, "/spaces/a");

        assert_ne!(old, new, "each spawn gets a distinct task id");
        assert_eq!(
            path_for_task(&workers, old),
            None,
            "the replaced instance's exit must read as stale"
        );
        assert_eq!(path_for_task(&workers, new).as_deref(), Some("/spaces/a"));
    }

    #[tokio::test]
    async fn test_path_for_task_rejects_an_unmapped_task() {
        // A cancelled-and-unmapped worker (space unregistered on reload).
        let mut workers = HashMap::new();
        let mut tasks = tokio::task::JoinSet::new();
        let id = dummy_worker(&mut workers, &mut tasks, "/spaces/a");
        workers.remove("/spaces/a");

        assert_eq!(path_for_task(&workers, id), None);
    }

    #[tokio::test]
    async fn test_cancel_stops_worker() {
        let dir = tempfile::tempdir().unwrap();
        let config = SpaceConfig {
            space_id: "58ff98f8-b664-4ec6-8a99-9ba62261365a".into(),
            relay_url: "ws://127.0.0.1:1/bogus".into(),
        };
        config.save(dir.path()).unwrap();

        let worker_config =
            SpaceWorkerConfig::for_space(dir.path(), "did:key:test", None, "tester", false)
                .expect("valid space config");

        let mut tasks = tokio::task::JoinSet::new();
        let handle = spawn_worker(&mut tasks, worker_config);

        // Give the task a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(tasks.len(), 1, "worker should still be running");

        // Cancel the worker. The reconnect loop checks the cancel token
        // during backoff sleeps, so cancellation works even with a bogus relay.
        handle.cancel.cancel();

        let joined = tokio::time::timeout(std::time::Duration::from_secs(5), tasks.join_next())
            .await
            .expect("worker should stop within 5 seconds after cancel")
            .expect("the set holds exactly one task");
        // Cancel triggers graceful shutdown, so the task ends without panicking.
        joined.expect("cancelled worker must not panic");
    }

    #[tokio::test]
    async fn test_worker_panic_surfaces_with_its_path() {
        // A worker that PANICS (a diamond-types unwind in the wild) must still
        // reach the supervisor as a resolvable exit — otherwise it is a zombie:
        // the task is dead, the path stays mapped, and the space silently stops
        // syncing with no respawn. A panicking task joins as Err, which carries
        // only the task id, so resolving the path from that id is the whole
        // path back to the restart-or-give-up policy.
        //
        // Silence the intentional panic's default backtrace so the output stays
        // pristine. Restore via a drop guard: a plain set_hook(prev) after the
        // asserts would be skipped if an assert failed, leaving the
        // message-swallowing hook installed for the rest of the test binary.
        type PanicHook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send>;
        struct HookGuard(Option<PanicHook>);
        impl Drop for HookGuard {
            fn drop(&mut self) {
                if let Some(prev) = self.0.take() {
                    std::panic::set_hook(prev);
                }
            }
        }
        let _hook = HookGuard(Some(std::panic::take_hook()));
        std::panic::set_hook(Box::new(|_| {}));

        let mut workers = HashMap::new();
        let mut tasks = tokio::task::JoinSet::new();
        let abort = tasks.spawn(run_worker(
            async { panic!("worker boom") },
            "space-id".to_owned(),
            "/spaces/a".to_owned(),
        ));
        workers.insert(
            "/spaces/a".to_owned(),
            WorkerHandle {
                cancel: CancellationToken::new(),
                abort,
                spawned_at: std::time::Instant::now(),
            },
        );

        let joined =
            tokio::time::timeout(std::time::Duration::from_secs(1), tasks.join_next_with_id())
                .await
                .expect("the exit must surface, not hang forever")
                .expect("the set holds exactly one task");

        let err = joined.expect_err("the worker panicked, so it joins as Err");
        assert_eq!(
            path_for_task(&workers, err.id()).as_deref(),
            Some("/spaces/a"),
            "a panicked worker's exit must still resolve to its path"
        );
    }
}
