//! PID file utilities and process signal helpers.
//!
//! Shared by the CLI (`kutl`) and the desktop app (`kutl-desktop`) for
//! tracking daemon processes via the standard Unix PID-file pattern.

use std::path::Path;

use anyhow::{Context, Result};

// ── Process liveness ────────────────────────────────────────────────

/// Check whether a process with the given PID is alive.
///
/// Uses `kill(pid, 0)` via libc, which checks for process existence without
/// delivering a signal. Returns `false` if the process does not exist or if
/// we lack permission to signal it.
#[cfg(unix)]
pub fn is_process_alive(pid: u32) -> bool {
    // SAFETY: signal 0 does not deliver a signal — it only checks whether
    // the process exists and we have permission to signal it.
    // PIDs never exceed i32::MAX on any Unix system, so the cast is safe.
    unsafe { libc::kill(pid.cast_signed(), 0) == 0 }
}

/// Check whether a process with the given PID is alive.
///
/// Non-Unix stub — always returns `false`.
#[cfg(not(unix))]
pub fn is_process_alive(_pid: u32) -> bool {
    false
}

// ── Signal delivery ─────────────────────────────────────────────────

/// Send a signal to a process.
///
/// `signal` should be one of `libc::SIGTERM`, `libc::SIGKILL`,
/// `libc::SIGHUP`, etc.
///
/// Returns an error if the process does not exist or we lack permission.
#[cfg(unix)]
pub fn send_signal(pid: u32, signal: i32) -> Result<()> {
    // SAFETY: we pass a valid signal number and a valid PID.
    let rc = unsafe { libc::kill(pid.cast_signed(), signal) };
    if rc == 0 {
        Ok(())
    } else {
        let err = std::io::Error::last_os_error();
        Err(anyhow::anyhow!("kill({pid}, {signal}) failed: {err}"))
    }
}

/// Send a signal to a process.
///
/// Non-Unix stub — always returns an error.
#[cfg(not(unix))]
pub fn send_signal(_pid: u32, _signal: i32) -> Result<()> {
    anyhow::bail!("signal delivery is not supported on this platform")
}

// ── PID file operations ─────────────────────────────────────────────

/// Read and parse a PID from a file.
///
/// Returns `Ok(None)` if the file does not exist or contains invalid content.
pub fn read_pid_file(path: &Path) -> Result<Option<u32>> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read PID file {}", path.display()));
        }
    };
    Ok(content
        .trim()
        .parse()
        .map_err(|e| {
            tracing::error!(
                path = %path.display(),
                content = content.trim(),
                error = %e,
                "PID file contains invalid content"
            );
        })
        .ok())
}

/// Write a PID to a file, creating parent directories as needed.
pub fn write_pid_file(path: &Path, pid: u32) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, pid.to_string())
        .with_context(|| format!("failed to write PID file {}", path.display()))
}

/// Remove a PID file (best-effort).
///
/// Silently ignores the case where the file does not exist. Logs a warning
/// for any other error.
pub fn remove_pid_file(path: &Path) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(error = %e, path = %path.display(), "failed to remove PID file");
    }
}

/// Read a PID file and check whether the process is still alive.
///
/// Returns `Ok(Some(pid))` if the process is running.
/// Removes a stale PID file and returns `Ok(None)` if the process is dead,
/// the file is missing, or the content is invalid.
pub fn read_pid_file_alive(path: &Path) -> Result<Option<u32>> {
    let Some(pid) = read_pid_file(path)? else {
        return Ok(None);
    };

    if is_process_alive(pid) {
        Ok(Some(pid))
    } else {
        // Remove the stale file best-effort; ignore errors.
        let _ = std::fs::remove_file(path);
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_process_alive_self() {
        assert!(is_process_alive(std::process::id()));
    }

    #[test]
    fn test_is_process_alive_nonexistent() {
        assert!(!is_process_alive(4_000_000));
    }

    #[test]
    fn test_pid_file_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.pid");

        write_pid_file(&path, 12345).unwrap();
        assert_eq!(read_pid_file(&path).unwrap(), Some(12345));
    }

    #[test]
    fn test_read_pid_file_missing() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("missing.pid");
        assert_eq!(read_pid_file(&path).unwrap(), None);
    }

    #[test]
    fn test_read_pid_file_invalid_content() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bad.pid");
        std::fs::write(&path, "not-a-number").unwrap();
        assert_eq!(read_pid_file(&path).unwrap(), None);
    }

    #[test]
    fn test_remove_pid_file_nonexistent() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("gone.pid");
        // Must not panic.
        remove_pid_file(&path);
    }

    #[test]
    fn test_read_pid_file_alive_removes_stale() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("stale.pid");
        write_pid_file(&path, 4_000_000).unwrap();
        assert!(path.exists());
        assert_eq!(read_pid_file_alive(&path).unwrap(), None);
        assert!(!path.exists());
    }

    #[test]
    fn test_read_pid_file_alive_live_process() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("live.pid");
        let pid = std::process::id();
        write_pid_file(&path, pid).unwrap();
        assert_eq!(read_pid_file_alive(&path).unwrap(), Some(pid));
        assert!(path.exists());
    }
}
