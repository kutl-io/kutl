//! Filesystem primitives shared by every kutl process that writes files a
//! user or another process may be reading: one atomic-replace rule, so no
//! two crates disagree about what a crash can leave behind.

use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

/// Replace `path`'s contents with `bytes` atomically: write a hidden sibling
/// temp file, then rename it over `path`. The temp lives in the same
/// directory (same filesystem, so the rename is atomic), and a PROCESS crash
/// at any point leaves either the previous complete file or the new complete
/// file, never a truncation.
///
/// The destination's permission bits are carried onto the replacement when
/// it exists: a rename replaces the inode, so without this every write
/// would reset the file's mode to the process default. Ownership beyond
/// the mode, extended attributes, and hard links are NOT carried: the
/// replacement is a fresh inode, and a path that was a symlink becomes a
/// regular file. Callers that must refuse to replace a file the user
/// protected check writability before calling.
///
/// The guarantee is process-crash atomicity, not power-loss durability: no
/// fsync is issued, so an OS crash or power cut can reorder the rename
/// ahead of the data blocks and surface an empty or partial file. Every
/// caller of this spelling sits on a hot path (a sidecar per content event,
/// a materialized file per remote op) and every consumer of these files
/// already treats a torn one as recoverable, so the per-write fsync is not
/// paid; [`write_atomic_secret_durable`] is the cold-path spelling that pays
/// it.
///
/// The temp is `.kutl-tmp-<hash of the destination's name>-<pid>-<seq>`:
/// dot-prefixed so a file watcher's hidden-path rule ignores it, fixed
/// length so a long document name cannot push it past the filesystem's
/// name limit, and per writer (this process and a per-process counter) so
/// two writers of one destination (a daemon and a CLI both rewriting a file
/// on upgrade day) never share a scratch file and rename each other's
/// half-written bytes into place. A temp stranded by a crash between write
/// and rename is inert and removed on any failure; a crash mid-write can
/// leave one behind, at one file per crash.
///
/// # Errors
///
/// Returns the bare I/O error of the write, the permission copy, or the
/// rename; the caller names the destination in its context.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    replace_atomic(path, bytes, None, false)
}

/// Owner-only mode for a secret file (0600).
const SECRET_FILE_MODE: u32 = 0o600;
/// The mode a secret file is created with, where the platform enforces one.
const SECRET_MODE: Option<u32> = if cfg!(unix) {
    Some(SECRET_FILE_MODE)
} else {
    None
};

/// [`write_atomic`] for a secret (a private key, a bearer token): the temp is
/// created owner-only from the first byte, so the secret is never
/// world-readable even transiently, and that mode is kept on the
/// replacement rather than inherited from a looser previous file. On
/// non-unix platforms the permission model is not enforced here and the
/// write is a plain atomic replace.
///
/// # Errors
///
/// As [`write_atomic`].
pub fn write_atomic_secret(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    replace_atomic(path, bytes, SECRET_MODE, false)
}

/// [`write_atomic_secret`] with the temp fsynced before the rename, for a
/// secret minted once at first start (a relay's signing identity): a torn
/// first write there would be read as a valid identity by the next boot and
/// silently flip the key. The directory entry is not fsynced, so a power
/// cut can still lose the rename itself, which leaves the previous file or
/// none, never a torn one.
///
/// # Errors
///
/// As [`write_atomic`], plus the fsync.
pub fn write_atomic_secret_durable(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    replace_atomic(path, bytes, SECRET_MODE, true)
}

fn replace_atomic(
    path: &Path,
    bytes: &[u8],
    mode: Option<u32>,
    durable: bool,
) -> std::io::Result<()> {
    let tmp = temp_sibling(path);
    let result = write_then_rename(path, &tmp, bytes, mode, durable);
    if result.is_err() {
        // Best effort: a stranded temp is inert, but a failed place should
        // still leave the directory as it found it.
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn write_then_rename(
    path: &Path,
    tmp: &Path,
    bytes: &[u8],
    mode: Option<u32>,
    durable: bool,
) -> std::io::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(mode);
    }
    let mut file = opts.open(tmp)?;
    file.write_all(bytes)?;
    if durable {
        file.sync_all()?;
    }
    drop(file);
    if mode.is_none()
        && let Ok(meta) = std::fs::metadata(path)
    {
        std::fs::set_permissions(tmp, meta.permissions())?;
    }
    std::fs::rename(tmp, path)
}

/// Distinguishes this process's temps from another process's for the same
/// destination, together with the process id.
static TEMP_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The hidden sibling temp for an atomic write of `path`:
/// `.kutl-tmp-<hash of the destination's name>-<pid>-<sequence>` in the
/// destination's directory. Unique per writer, fixed length whatever the
/// destination's name length.
fn temp_sibling(path: &Path) -> PathBuf {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.file_name().hash(&mut hasher);
    let sequence = TEMP_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.with_file_name(format!(
        ".kutl-tmp-{:016x}-{}-{sequence}",
        hasher.finish(),
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_atomic_replaces_content_and_leaves_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("doc.md");
        std::fs::write(&target, b"old").unwrap();
        write_atomic(&target, b"new").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".kutl-tmp-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no temp survives a successful write: {leftovers:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_durable_secret_write_is_owner_only_and_leaves_no_temp() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("identity.json");
        write_atomic_secret_durable(&target, b"secret").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"secret");
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["identity.json"],
            "no temp survives the fsynced write"
        );
    }

    #[test]
    fn test_write_atomic_creates_a_missing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("fresh.md");
        write_atomic(&target, b"hello").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"hello");
    }

    #[cfg(unix)]
    #[test]
    fn test_write_atomic_carries_the_destination_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("exec.sh");
        std::fs::write(&target, b"#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
        write_atomic(&target, b"#!/bin/sh\necho hi\n").unwrap();
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755, "the replacement keeps the destination's mode");
    }

    #[test]
    fn test_write_atomic_failure_removes_the_temp() {
        let dir = tempfile::tempdir().unwrap();
        // The destination is a DIRECTORY: the temp is written beside it,
        // the rename over a non-empty directory fails, and the temp must
        // be gone afterwards.
        let target = dir.path().join("doc.md");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("inner"), b"x").unwrap();
        assert!(write_atomic(&target, b"new").is_err());
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".kutl-tmp-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "a failed write leaves no temp: {leftovers:?}"
        );
        assert!(target.is_dir(), "the destination is untouched");
    }
}
