//! Daemon lifecycle management: PID file operations and process control.
//!
//! Uses the standard Unix PID-file pattern to track the daemon process.
//! The global PID file lives at `$KUTL_HOME/daemon.pid`, logs at
//! `$KUTL_HOME/logs/daemon.log`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use kutl_client::{
    is_process_alive, read_pid_file, read_pid_file_alive, remove_pid_file, send_signal,
    write_pid_file,
};

/// PID file name within the `$KUTL_HOME` directory.
const PID_FILENAME: &str = "daemon.pid";

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

/// Write a PID to the global PID file.
pub fn write_pid(pid: u32) -> Result<()> {
    write_pid_file(&pid_path()?, pid)
}

/// Remove the global PID file (best-effort, logs errors).
pub fn remove_pid() {
    match pid_path() {
        Ok(path) => remove_pid_file(&path),
        Err(e) => {
            tracing::warn!(error = %e, "could not determine PID file path for removal");
        }
    }
}

/// Read the global PID file and check if the process is alive.
///
/// Returns `Ok(Some(pid))` if the process is running.
/// Removes a stale PID file and returns `Ok(None)` if the process is dead.
/// Returns `Ok(None)` if no PID file exists.
pub fn stale_pid_check() -> Result<Option<u32>> {
    read_pid_file_alive(&pid_path()?)
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
}
