//! Daemon lifecycle management: PID file operations and process control.
//!
//! Uses the standard Unix PID-file pattern to track the daemon process.
//! The global PID file lives at `$KUTL_HOME/daemon.pid`, logs at
//! `$KUTL_HOME/logs/daemon.log`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use kutl_client::{is_process_alive, read_pid_file, remove_pid_file, send_signal, write_pid_file};

/// PID file name within the `$KUTL_HOME` directory.
const PID_FILENAME: &str = "daemon.pid";

/// Process-identity sidecar file name within the `$KUTL_HOME` directory.
///
/// Stores a stable identity token (process start time) for the PID recorded in
/// [`PID_FILENAME`]. The kernel recycles PIDs, so a bare `kill(pid, 0)` cannot
/// distinguish the original daemon from an unrelated process that inherited its
/// PID after it died. We verify this sidecar before sending any signal.
const PID_ID_FILENAME: &str = "daemon.pid.id";

/// Log file name within the `$KUTL_HOME/logs` directory.
const LOG_FILENAME: &str = "daemon.log";

/// Polling interval when waiting for a process to exit.
const POLL_INTERVAL: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_millis(250));

/// How long to wait for graceful shutdown before sending SIGKILL.
const GRACEFUL_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(5));

/// Extra time to wait after SIGKILL before giving up.
const KILL_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(1));

/// Path to the global daemon PID file (`$KUTL_HOME/daemon.pid`).
pub fn pid_path() -> Result<PathBuf> {
    Ok(crate::dirs::kutl_home()?.join(PID_FILENAME))
}

/// Path to the process-identity sidecar file (`$KUTL_HOME/daemon.pid.id`).
fn id_path() -> Result<PathBuf> {
    Ok(crate::dirs::kutl_home()?.join(PID_ID_FILENAME))
}

/// Read a stable process-identity token for `pid`.
///
/// The token is the process start time (seconds since the Unix epoch), which is
/// stable for the lifetime of a process and differs across PID reuse — a
/// recycled PID belongs to a process that started later. Returns `None` when
/// identity cannot be determined (process gone, or the platform doesn't report
/// a start time); callers must treat `None` as "cannot verify" and fall back to
/// liveness-only checks.
///
/// Backed by `sysinfo` rather than hand-rolled `/proc/<pid>/stat` field parsing
/// or a `ps` subprocess: the field-index parsing was fragile (a wrong index
/// either leaks the daemon — a false mismatch refusing to signal — or signals a
/// recycled PID), and it can't be exercised cross-platform from a dev machine.
fn process_identity(pid: u32) -> Option<String> {
    use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
    let pid = Pid::from_u32(pid);
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    let start_time = system.process(pid)?.start_time();
    Some(format!("starttime={start_time}"))
}

/// Decide whether it is safe to signal `pid` given the stored identity token.
///
/// - `stored == None`: no recorded identity (e.g. legacy PID file, or identity
///   was unavailable at write time) — fall back to "safe to signal" so behavior
///   matches the pre-identity liveness-only check.
/// - `current == None`: identity could not be read now — fall back to safe.
/// - both present: signal only if they match (same process instance).
///
/// Returning `false` means the PID was almost certainly recycled to an
/// unrelated process and must NOT be signalled.
fn identity_permits_signal(stored: Option<&str>, current: Option<&str>) -> bool {
    match (stored, current) {
        (Some(stored), Some(current)) => stored == current,
        _ => true,
    }
}

/// Read the stored process-identity token, if any.
fn read_stored_identity() -> Option<String> {
    let path = id_path().ok()?;
    let content = std::fs::read_to_string(path).ok()?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// Verify that `pid` is the same process instance recorded in the PID file.
///
/// Returns `true` if signalling `pid` is safe (identities match, or identity
/// cannot be determined on either side). Returns `false` when the stored and
/// current identities disagree, indicating PID reuse.
fn verify_daemon_identity(pid: u32) -> bool {
    let stored = read_stored_identity();
    let current = process_identity(pid);
    if stored.is_some() && current.is_some() && stored != current {
        tracing::warn!(
            pid,
            "PID file process identity mismatch — PID appears recycled, refusing to signal"
        );
    }
    identity_permits_signal(stored.as_deref(), current.as_deref())
}

/// Remove the process-identity sidecar file (best-effort).
fn remove_id_file() {
    if let Ok(path) = id_path()
        && let Err(e) = std::fs::remove_file(&path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(error = %e, path = %path.display(), "failed to remove PID identity sidecar");
    }
}

/// Path to the global daemon log file (`$KUTL_HOME/logs/daemon.log`).
pub fn log_path() -> Result<PathBuf> {
    let logs = crate::dirs::kutl_home()?.join("logs");
    std::fs::create_dir_all(&logs)
        .with_context(|| format!("failed to create {}", logs.display()))?;
    Ok(logs.join(LOG_FILENAME))
}

/// Read and parse the PID from the global PID file.
///
/// Returns `Ok(None)` if the file doesn't exist or contains invalid content.
pub fn read_pid() -> Result<Option<u32>> {
    read_pid_file(&pid_path()?)
}

/// Write a PID to the global PID file, plus a process-identity sidecar.
///
/// The sidecar records the target process's start time so later `stop`/`reload`
/// calls can detect PID reuse (the OS recycling the daemon's PID for an
/// unrelated process). Writing the sidecar is best-effort: if identity is
/// unavailable, no sidecar is written and verification falls back to a
/// liveness-only check (preserving prior behavior).
pub fn write_pid(pid: u32) -> Result<()> {
    write_pid_file(&pid_path()?, pid)?;

    let id_path = id_path()?;
    if let Some(identity) = process_identity(pid) {
        if let Err(e) = std::fs::write(&id_path, &identity) {
            tracing::warn!(pid, error = %e, "failed to write PID identity sidecar");
        }
    } else {
        // Identity unavailable — remove any stale sidecar so verification falls
        // back to liveness-only rather than comparing against a previous
        // daemon's identity.
        remove_id_file();
        tracing::debug!(
            pid,
            "process identity unavailable; PID reuse protection disabled for this daemon"
        );
    }
    Ok(())
}

/// Remove the global PID file and its identity sidecar (best-effort).
pub fn remove_pid() {
    match pid_path() {
        Ok(path) => remove_pid_file(&path),
        Err(e) => {
            tracing::warn!(error = %e, "could not determine PID file path for removal");
        }
    }
    remove_id_file();
}

/// Read the global PID file and check if the recorded daemon is running.
///
/// Returns `Ok(Some(pid))` only if the process is alive AND its identity matches
/// the recorded sidecar (so a recycled PID belonging to an unrelated process is
/// not reported as a running daemon). Removes a stale PID file (and sidecar) and
/// returns `Ok(None)` when the process is dead, the PID was recycled, or no PID
/// file exists.
pub fn stale_pid_check() -> Result<Option<u32>> {
    let Some(pid) = read_pid()? else {
        return Ok(None);
    };
    if is_process_alive(pid) && verify_daemon_identity(pid) {
        Ok(Some(pid))
    } else {
        // Dead process or recycled PID — clear the stale state.
        remove_pid();
        Ok(None)
    }
}

/// Claim the daemon PID file for THIS process — the foreground
/// (`kutl daemon run`) counterpart of the `daemon start` flow's
/// check-then-write.
///
/// One daemon per home is a sync invariant, not a tidiness rule: two
/// daemons watching the same tree each read the other's materializations
/// as fresh user edits (own-write suppression is in-process state) and
/// re-mint them, growing the document geometrically within seconds. A
/// foreground daemon that skipped this claim was invisible to
/// `daemon start`'s liveness check, which is exactly how that second
/// daemon came to exist.
pub fn claim_foreground() -> Result<()> {
    if let Some(pid) = stale_pid_check()? {
        anyhow::bail!(
            "a daemon is already running for this home (pid {pid}); stop it with `kutl daemon stop` first"
        );
    }
    write_pid(std::process::id())
}

/// Refuse a one-shot sync of `space_root` while a live daemon watches it.
///
/// A one-shot sync and the daemon are two sync actors sharing the same
/// on-disk CRDT state with independent in-memory copies — measured: one
/// `kutl sync` beside a live daemon re-minted a 14-line document into
/// millions of ops on first contact. The daemon already syncs its
/// registered spaces continuously, so the one-shot adds nothing there. A
/// tree the daemon does NOT watch (a `--dir` sync of an unregistered
/// space) stays allowed — that is the one-shot's legitimate territory.
pub fn refuse_if_daemon_watches(space_root: &Path) -> Result<()> {
    let Some(pid) = stale_pid_check()? else {
        return Ok(());
    };
    let registry_path = kutl_client::space_registry::registry_path()?;
    let registry = kutl_client::space_registry::SpaceRegistry::load(&registry_path)?;
    let root = space_root
        .canonicalize()
        .unwrap_or_else(|_| space_root.to_path_buf());
    let watched = registry.spaces.iter().any(|p| {
        let p = Path::new(p);
        p.canonicalize().map_or(p == root, |c| c == root)
    });
    if watched {
        anyhow::bail!(
            "the daemon (pid {pid}) is already syncing this space; edits sync on their own, or stop it with `kutl daemon stop` for a manual pass"
        );
    }
    Ok(())
}

/// Locate the current `kutl` binary for re-spawning as daemon.
pub fn find_self_binary() -> Result<PathBuf> {
    std::env::current_exe().context("failed to determine current executable")
}

/// Brief delay after spawn to catch immediate startup failures.
const SPAWN_CHECK_DELAY: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_millis(200));

/// Spawn the global daemon as a background process, returning its PID.
///
/// stdout and stderr are redirected to the log file. After spawning, waits
/// briefly and verifies the process is still alive to catch immediate failures.
pub fn spawn_daemon() -> Result<u32> {
    let kutl_bin = find_self_binary()?;
    let log = log_path()?;

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .with_context(|| format!("failed to open log file {}", log.display()))?;
    let stderr_file = log_file
        .try_clone()
        .context("failed to clone log file handle")?;

    let child = std::process::Command::new(&kutl_bin)
        .args(["daemon", "run"])
        .stdout(log_file)
        .stderr(stderr_file)
        .spawn()
        .with_context(|| format!("failed to spawn {}", kutl_bin.display()))?;

    let pid = child.id();

    // Brief delay to catch immediate startup failures.
    std::thread::sleep(SPAWN_CHECK_DELAY);
    if !is_process_alive(pid) {
        anyhow::bail!(
            "daemon exited immediately after start — check {}",
            log.display()
        );
    }

    Ok(pid)
}

/// Stop the global daemon: SIGTERM, wait, SIGKILL if needed, remove PID file.
///
/// Returns a message describing what happened.
pub fn stop_daemon() -> Result<String> {
    let Some(pid) = read_pid()? else {
        return Ok("daemon is not running".into());
    };

    if !is_process_alive(pid) {
        remove_pid();
        return Ok("daemon is not running (stale PID file removed)".into());
    }

    // Guard against PID reuse: if the live process is not the daemon we
    // recorded (identity mismatch), do not signal an unrelated process.
    if !verify_daemon_identity(pid) {
        remove_pid();
        return Ok(format!(
            "daemon is not running (PID {pid} was recycled; stale PID file removed)"
        ));
    }

    // Send SIGTERM for graceful shutdown.
    send_signal(pid, libc::SIGTERM)?;

    // Poll until the process exits or we hit the timeout.
    let deadline = std::time::Instant::now() + GRACEFUL_TIMEOUT;
    while std::time::Instant::now() < deadline {
        if !is_process_alive(pid) {
            remove_pid();
            return Ok(format!("daemon stopped (PID {pid})"));
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    // Escalate to SIGKILL.
    let _ = send_signal(pid, libc::SIGKILL);
    let kill_deadline = std::time::Instant::now() + KILL_TIMEOUT;
    while std::time::Instant::now() < kill_deadline {
        if !is_process_alive(pid) {
            remove_pid();
            return Ok(format!("daemon killed (PID {pid})"));
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    remove_pid();
    Ok(format!("daemon may still be running (PID {pid})"))
}

/// Send SIGHUP to the running daemon to trigger a registry reload.
///
/// No-op if no daemon is running.
pub fn signal_reload() -> Result<()> {
    let Some(pid) = read_pid()? else {
        return Ok(());
    };
    if !is_process_alive(pid) {
        return Ok(());
    }
    // Guard against PID reuse before signalling — `signal_reload` fires
    // automatically on every `kutl init`/`join`, so an unverified SIGHUP could
    // hit an unrelated process that inherited the dead daemon's PID.
    if !verify_daemon_identity(pid) {
        tracing::warn!(
            pid,
            "skipping SIGHUP: PID appears recycled (identity mismatch)"
        );
        return Ok(());
    }
    if let Err(e) = send_signal(pid, libc::SIGHUP) {
        tracing::warn!(pid, error = %e, "failed to send SIGHUP to daemon");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    /// Set `HOME` to a temporary directory so that `pid_path()` / `read_pid()`
    /// etc. operate in an isolated location. Returns the guard that keeps the
    /// temp dir alive. Must be used with `#[serial]` since `HOME` is
    /// process-global.
    ///
    /// Also clears `KUTL_HOME` defensively — other tests in the binary (e.g.
    /// `save_space_config_tests`) set `KUTL_HOME` to their own tempdirs and
    /// don't always clean up. Since `kutl_client::dirs::kutl_home` honors
    /// `KUTL_HOME` over `HOME`, leaving it set would cause `pid_path()` to
    /// look in the wrong directory and read stale state from a previous test.
    fn fake_home() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::TempDir::new().unwrap();
        // SAFETY: these tests are serialized via `#[serial]`, so no concurrent
        // threads observe the env var mutation.
        unsafe {
            std::env::set_var("HOME", dir.path());
            std::env::remove_var("KUTL_HOME");
        }
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        (dir, kutl_dir)
    }

    #[test]
    #[serial]
    fn test_pid_write_read_roundtrip() {
        let (_guard, _) = fake_home();

        write_pid(12345).unwrap();
        assert_eq!(read_pid().unwrap(), Some(12345));
    }

    #[test]
    #[serial]
    fn test_read_pid_missing_file() {
        let (_guard, _) = fake_home();
        assert_eq!(read_pid().unwrap(), None);
    }

    #[test]
    #[serial]
    fn test_read_pid_invalid_content() {
        let (_guard, kutl_dir) = fake_home();
        std::fs::write(kutl_dir.join(PID_FILENAME), "not-a-number").unwrap();
        assert_eq!(read_pid().unwrap(), None);
    }

    #[test]
    #[serial]
    fn test_stale_pid_removes_dead() {
        let (_guard, _) = fake_home();

        // Write a PID that doesn't exist.
        write_pid(4_000_000).unwrap();
        assert!(pid_path().unwrap().exists());

        // Stale check should remove it and return None.
        assert_eq!(stale_pid_check().unwrap(), None);
        assert!(!pid_path().unwrap().exists());
    }

    #[test]
    #[serial]
    fn test_remove_pid_nonexistent() {
        let (_guard, _) = fake_home();
        // Should not panic on missing file.
        remove_pid();
    }

    #[test]
    #[serial]
    fn test_claim_foreground_records_this_process() {
        let (_guard, _) = fake_home();

        claim_foreground().unwrap();
        assert_eq!(read_pid().unwrap(), Some(std::process::id()));
    }

    #[test]
    #[serial]
    fn test_claim_foreground_refuses_live_daemon() {
        let (_guard, _) = fake_home();

        // The test process itself stands in for the running daemon: a PID
        // that is genuinely alive with a matching identity sidecar.
        write_pid(std::process::id()).unwrap();

        let err = claim_foreground().unwrap_err().to_string();
        assert!(err.contains("already running"), "got: {err}");
        // The incumbent's claim must survive the refusal.
        assert_eq!(read_pid().unwrap(), Some(std::process::id()));
    }

    #[test]
    #[serial]
    fn test_claim_foreground_reclaims_stale() {
        let (_guard, _) = fake_home();

        // A dead PID must not block the claim.
        write_pid(4_000_000).unwrap();
        claim_foreground().unwrap();
        assert_eq!(read_pid().unwrap(), Some(std::process::id()));
    }

    /// Register `workspace` in the fake home's spaces.json.
    fn register_space(kutl_dir: &Path, workspace: &Path) {
        std::fs::write(
            kutl_dir.join("spaces.json"),
            format!("{{\"spaces\":[\"{}\"]}}", workspace.display()),
        )
        .unwrap();
    }

    #[test]
    #[serial]
    fn test_sync_refused_while_daemon_watches_the_space() {
        let (guard, kutl_dir) = fake_home();
        let workspace = guard.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        register_space(&kutl_dir, &workspace);
        // The test process stands in for the live daemon.
        write_pid(std::process::id()).unwrap();

        let err = refuse_if_daemon_watches(&workspace)
            .unwrap_err()
            .to_string();
        assert!(err.contains("already syncing"), "got: {err}");
    }

    #[test]
    #[serial]
    fn test_sync_allowed_with_no_daemon() {
        let (guard, kutl_dir) = fake_home();
        let workspace = guard.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        register_space(&kutl_dir, &workspace);

        refuse_if_daemon_watches(&workspace).unwrap();
    }

    #[test]
    #[serial]
    fn test_sync_allowed_for_a_tree_the_daemon_does_not_watch() {
        let (guard, kutl_dir) = fake_home();
        let watched = guard.path().join("workspace");
        let unwatched = guard.path().join("elsewhere");
        std::fs::create_dir_all(&watched).unwrap();
        std::fs::create_dir_all(&unwatched).unwrap();
        register_space(&kutl_dir, &watched);
        write_pid(std::process::id()).unwrap();

        refuse_if_daemon_watches(&unwatched).unwrap();
    }

    // --- Identity verification tests ---

    #[test]
    fn test_identity_permits_signal_match() {
        assert!(identity_permits_signal(
            Some("starttime=42"),
            Some("starttime=42")
        ));
    }

    #[test]
    fn test_identity_permits_signal_mismatch_blocks() {
        // Stored and current identities differ → PID was recycled → must NOT signal.
        assert!(!identity_permits_signal(
            Some("starttime=42"),
            Some("starttime=99")
        ));
    }

    #[test]
    fn test_identity_permits_signal_missing_falls_back_to_safe() {
        // Missing identity on either side falls back to liveness-only behavior.
        assert!(identity_permits_signal(None, Some("starttime=1")));
        assert!(identity_permits_signal(Some("starttime=1"), None));
        assert!(identity_permits_signal(None, None));
    }

    #[test]
    fn test_process_identity_is_stable_and_meaningful_for_self() {
        // Our own process is determinable and stable across reads, AND the token
        // carries a real (non-zero) start time. The non-zero check guards
        // against a backend that silently reports 0 — which would make every
        // identity compare equal and defeat the PID-reuse guard.
        let pid = std::process::id();
        let a = process_identity(pid);
        let b = process_identity(pid);
        assert_eq!(a, b, "process identity should be stable across reads");
        let token = a.expect("our own process identity should be determinable");
        let secs: u64 = token
            .strip_prefix("starttime=")
            .expect("token carries the starttime= prefix")
            .parse()
            .expect("start time is numeric");
        assert!(
            secs > 0,
            "start time should be a real epoch value, got {secs}"
        );
    }

    #[test]
    fn test_process_identity_none_for_dead_pid() {
        // Spawn a trivial process, reap it, then confirm its now-dead PID has no
        // identity — so callers fall back to liveness-only checks instead of
        // matching a phantom. The just-freed PID is not instantly reused.
        let mut child = std::process::Command::new("true")
            .spawn()
            .expect("spawn a trivial child");
        let pid = child.id();
        child.wait().expect("reap the child");
        assert_eq!(
            process_identity(pid),
            None,
            "a reaped PID should have no identity"
        );
    }

    #[test]
    #[serial]
    fn test_write_pid_records_identity_for_live_process() {
        let (_guard, _) = fake_home();

        // Use our own (live) PID so identity is readable on supported platforms.
        let pid = std::process::id();
        write_pid(pid).unwrap();

        // The sidecar should match what process_identity reports now (or be
        // absent on unsupported platforms, where process_identity is None).
        let stored = read_stored_identity();
        let current = process_identity(pid);
        assert_eq!(stored, current);
        // Identity must permit signalling our own live process.
        assert!(verify_daemon_identity(pid));
    }

    #[test]
    #[serial]
    fn test_verify_identity_blocks_mismatched_sidecar() {
        let (_guard, _) = fake_home();

        let pid = std::process::id();
        write_pid(pid).unwrap();

        // Only meaningful where identity is determinable; on unsupported
        // platforms there is no sidecar to corrupt and the fallback is safe.
        if process_identity(pid).is_some() {
            // Corrupt the sidecar to simulate PID reuse (different process).
            std::fs::write(id_path().unwrap(), "starttime=does-not-match").unwrap();
            assert!(
                !verify_daemon_identity(pid),
                "mismatched identity must block signalling"
            );
        }
    }

    #[test]
    #[serial]
    fn test_remove_pid_clears_identity_sidecar() {
        let (_guard, _) = fake_home();

        let pid = std::process::id();
        write_pid(pid).unwrap();
        remove_pid();

        assert!(!pid_path().unwrap().exists());
        assert!(!id_path().unwrap().exists());
    }
}
