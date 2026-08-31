//! Platform-portable filesystem birthtime stat.
//!
//! When the daemon registers a document, it carries the filesystem
//! creation time as `originally_created_at` so the relay can preserve
//! "when did this content first exist" for files dragged into the
//! `kutl/` folder. Failure modes (unsupported kernel, unsupported
//! filesystem, missing path) collapse to `None` — never fall back to
//! mtime, which would produce a wrong "originally created" claim.
//!
//! Implementation note: `std::fs::Metadata::created()` is the
//! cross-platform stdlib entry point and uses `statx(STATX_BTIME)` on
//! Linux ≥ 4.11, `stat`'s `st_birthtimespec` on macOS/BSD, and
//! `GetFileInformationByHandle`'s `ftCreationTime` on Windows. Older
//! Linux kernels and filesystems without birthtime support surface as
//! `Err`, which we map to `None`.
//!
//! Never fall back to mtime — that would produce a wrong "originally
//! created" claim.
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the filesystem creation time for a path.
///
/// Returns `None` if the file doesn't exist, metadata can't be read,
/// or the platform / filesystem doesn't expose birthtime.
pub fn get_birthtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).ok()?.created().ok()
}

/// Returns the filesystem creation time as Unix milliseconds.
///
/// Returns `None` for the same reasons as [`get_birthtime`], plus the
/// pre-1970 case where the birthtime is before the Unix epoch (the
/// `duration_since` would error). The proto field on
/// `RegisterDocument` expects `i64` millis.
pub fn get_birthtime_ms(path: &Path) -> Option<i64> {
    let bt = get_birthtime(path)?;
    let ms = bt.duration_since(UNIX_EPOCH).ok()?.as_millis();
    i64::try_from(ms).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// On macOS APFS/HFS+ and Windows NTFS, every newly-written file has
    /// a creation time. Linux is filesystem-dependent (`ext4` ≥ 4.11 +
    /// `statx` supports it; older kernels or `tmpfs` may not), so we
    /// only assert the file-exists branch on the platforms where it's
    /// guaranteed by the OS contract.
    #[test]
    fn test_get_birthtime_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.txt");
        std::fs::write(&path, "hello").unwrap();

        let bt = get_birthtime(&path);
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        assert!(bt.is_some(), "should return birthtime on macOS / Windows");
        // Linux: don't assert — depends on kernel + filesystem.
        let _ = bt;
    }

    #[test]
    fn test_get_birthtime_nonexistent_file() {
        let bt = get_birthtime(Path::new("/nonexistent/file.txt"));
        assert!(bt.is_none());
    }

    /// Renames preserve birthtime: on the supported platforms a rename
    /// is an inode-stable operation and the creation timestamp travels
    /// with the inode. Mirror the inode-stability test in `inode.rs`.
    #[test]
    fn test_birthtime_stable_after_rename() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old.txt");
        let new = dir.path().join("new.txt");
        std::fs::write(&old, "content").unwrap();

        let bt_before = get_birthtime(&old);
        std::fs::rename(&old, &new).unwrap();
        let bt_after = get_birthtime(&new);

        #[cfg(any(target_os = "macos", target_os = "windows"))]
        assert_eq!(
            bt_before, bt_after,
            "birthtime should be stable across rename"
        );
        // Linux: only compare when both probes succeeded — see the
        // platform note on `test_get_birthtime_existing_file`.
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        if bt_before.is_some() && bt_after.is_some() {
            assert_eq!(bt_before, bt_after);
        }
    }
}
