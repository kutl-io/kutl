//! Cross-process file locking and atomic replacement for `$KUTL_HOME` state.
//!
//! Several files under `$KUTL_HOME` are read-modify-written by more than one
//! process at once — the CLI, the desktop app, and one daemon per space all
//! touch them. Two mechanisms make that safe, and they are the same wherever
//! the pattern appears:
//!
//! 1. an exclusive OS file lock (`flock` on Unix, `LockFileEx` on Windows,
//!    via std's [`File::lock`](std::fs::File::lock) family) held across the
//!    WHOLE read-modify-write, so a concurrent writer cannot interleave and
//!    lose an update;
//! 2. write-to-temp-then-rename, so a crash mid-write leaves the previous
//!    contents rather than a truncated file.
//!
//! Loading does not live here: every text-tier file is read through the one
//! door in [`crate::text_file`]. This module owns only the locking and the
//! atomic / owner-only placement of what gets written back.

use std::path::Path;

use anyhow::{Context, Result};

/// Acquire an exclusive OS lock on a `.lock` sibling of `path`, blocking
/// until it is granted.
///
/// Returns the lock file handle — the lock is held until the handle is
/// dropped, and the OS releases it if the holding process dies, so there is
/// no stale-lock state to clean. Locking a sibling rather than the file
/// itself keeps the lock valid across the rename in [`write_atomic`], which
/// would otherwise swap the locked inode out from under the holder.
///
/// # Errors
///
/// Returns an error if the lock file cannot be opened or the lock call fails.
pub(crate) fn lock_exclusive(path: &Path) -> Result<std::fs::File> {
    let file = open_lock_file(path)?;
    file.lock()
        .with_context(|| format!("failed to acquire lock on {}", path.display()))?;
    Ok(file)
}

/// Non-blocking twin of [`lock_exclusive`]: attempt the lock and report
/// `Ok(None)` when another process already holds it, instead of queueing
/// behind them. The caller owns the loud refusal — it knows what the
/// holder means on its surface. Like its twin, the returned handle IS the
/// lock: held until dropped, released by the OS if the holder dies.
///
/// # Errors
///
/// Returns an error if the lock file cannot be opened or the lock call fails
/// for any reason other than the lock being held.
pub fn try_lock_exclusive(path: &Path) -> Result<Option<std::fs::File>> {
    let file = open_lock_file(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(e).with_context(|| format!("failed to acquire lock on {}", path.display()))
        }
    }
}

/// Open (creating if needed) the `.lock` sibling of `path`.
fn open_lock_file(path: &Path) -> Result<std::fs::File> {
    let lock_path = path.with_extension("lock");
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open lock file {}", lock_path.display()))
}

/// Replace `path`'s contents with `data`, atomically
/// ([`kutl_core::fs::write_atomic`]): a reader sees either the old contents
/// or the new ones and never a partial write.
///
/// # Errors
///
/// Returns an error if the temp file cannot be written or the rename fails.
pub(crate) fn write_atomic(path: &Path, data: &str) -> Result<()> {
    kutl_core::fs::write_atomic(path, data.as_bytes())
        .with_context(|| format!("failed to write {}", path.display()))
}

/// [`write_atomic`] for a secret (a private key, a bearer token): the file
/// is created owner-only (0600 on unix) from the first byte, so the secret
/// is never world-readable even transiently, and placed by rename.
///
/// # Errors
///
/// Returns an error if the temp file cannot be written or the rename fails.
pub(crate) fn write_atomic_secret(path: &Path, data: &str) -> Result<()> {
    kutl_core::fs::write_atomic_secret(path, data.as_bytes())
        .with_context(|| format!("failed to write secret file {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_atomic_secret_replaces_content_owner_only() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, "old longer content").unwrap();
        write_atomic_secret(&path, "new").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "secret file must be created mode 0600");
        }
    }
}
