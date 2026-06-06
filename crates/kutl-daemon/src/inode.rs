//! Platform-specific inode retrieval for rename detection.
//!
//! When a file is renamed, its inode stays the same. By tracking the
//! inode→UUID mapping, the daemon can distinguish a rename (same inode,
//! different path) from a delete+create (different inode, same path).

use std::path::Path;

/// Get the inode number for a file.
///
/// Returns `None` if the file doesn't exist or metadata can't be read.
#[cfg(unix)]
pub fn get_inode(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.ino())
}

/// Stub for non-Unix platforms — always returns `None`.
///
/// Rename detection falls back to delete+create on these platforms.
#[cfg(not(unix))]
pub fn get_inode(path: &Path) -> Option<u64> {
    let _ = path;
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_inode_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.txt");
        std::fs::write(&path, "hello").unwrap();

        let inode = get_inode(&path);
        #[cfg(unix)]
        assert!(inode.is_some(), "should return inode on unix");
        #[cfg(not(unix))]
        assert!(inode.is_none(), "stub returns None on non-unix");
    }

    #[test]
    fn test_get_inode_nonexistent_file() {
        let inode = get_inode(Path::new("/nonexistent/file.txt"));
        assert!(inode.is_none());
    }

    #[test]
    fn test_inode_stable_after_rename() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old.txt");
        let new = dir.path().join("new.txt");
        std::fs::write(&old, "content").unwrap();

        let inode_before = get_inode(&old);
        std::fs::rename(&old, &new).unwrap();
        let inode_after = get_inode(&new);

        #[cfg(unix)]
        assert_eq!(
            inode_before, inode_after,
            "inode should be stable across rename"
        );
    }

    #[test]
    fn test_inode_differs_for_different_files() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.txt");
        let b = dir.path().join("b.txt");
        std::fs::write(&a, "aaa").unwrap();
        std::fs::write(&b, "bbb").unwrap();

        #[cfg(unix)]
        {
            let ia = get_inode(&a).unwrap();
            let ib = get_inode(&b).unwrap();
            assert_ne!(ia, ib, "different files should have different inodes");
        }
    }
}
