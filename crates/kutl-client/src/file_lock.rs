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
//! What deliberately does NOT live here is *loading*. Each file decides for
//! itself what an absent or legacy-shaped file means, and folding those into a
//! generic helper would force every caller through one policy — the space
//! registry migrates an older on-disk shape, for instance, while
//! [`crate::known_relays`] treats anything unparseable as fatal.

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

/// Replace `path`'s contents with `data`, atomically.
///
/// Writes a `.tmp` sibling and renames it over `path` — a rename within a
/// directory is atomic, so a reader sees either the old contents or the new
/// ones and never a partial write.
///
/// # Errors
///
/// Returns an error if the temp file cannot be written or the rename fails.
pub(crate) fn write_atomic(path: &Path, data: &str) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data).with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to rename {} to {}", tmp.display(), path.display()))
}
