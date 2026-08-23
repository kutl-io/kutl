//! Validated relay-supplied paths that are safe for filesystem operations.

use std::path::{Component, Path, PathBuf};

use anyhow::{Result, bail, ensure};

/// A validated relative path received from the relay.
///
/// Can only be constructed via [`SafeRelayPath::new`], which rejects:
/// - empty paths
/// - `..` components (path traversal)
/// - absolute paths
/// - paths starting with `.kutl` (metadata directory)
#[derive(Clone, PartialEq, Eq)]
pub struct SafeRelayPath(PathBuf);

impl SafeRelayPath {
    /// Validate a relay-supplied path string.
    pub fn new(raw: &str) -> Result<Self> {
        ensure!(!raw.is_empty(), "relay path is empty");
        let p = PathBuf::from(raw);
        for component in p.components() {
            match component {
                Component::ParentDir => bail!("relay path contains '..' component: {raw}"),
                Component::RootDir | Component::Prefix(_) => {
                    bail!("relay path is absolute: {raw}")
                }
                _ => {}
            }
        }
        ensure!(
            !p.starts_with(".kutl"),
            "relay path conflicts with metadata directory: {raw}"
        );
        Ok(Self(p))
    }

    /// The validated path.
    pub fn as_path(&self) -> &Path {
        &self.0
    }

    /// Join this validated path onto a space root.
    ///
    /// Safe because `self` has been validated to be relative and non-escaping.
    pub fn under(&self, space_root: &Path) -> PathBuf {
        space_root.join(&self.0)
    }

    /// Consume and return the inner `PathBuf`.
    pub fn into_path_buf(self) -> PathBuf {
        self.0
    }
}

impl AsRef<Path> for SafeRelayPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl std::fmt::Display for SafeRelayPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.display().fmt(f)
    }
}

impl std::fmt::Debug for SafeRelayPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SafeRelayPath({:?})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rejects_parent_traversal() {
        assert!(SafeRelayPath::new("../../etc/passwd").is_err());
        assert!(SafeRelayPath::new("docs/../../../etc/shadow").is_err());
        assert!(SafeRelayPath::new("a/b/../../../escape").is_err());
    }

    #[test]
    fn test_rejects_absolute_paths() {
        assert!(SafeRelayPath::new("/etc/passwd").is_err());
        assert!(SafeRelayPath::new("/tmp/attack").is_err());
    }

    #[test]
    fn test_rejects_metadata_directory() {
        assert!(SafeRelayPath::new(".kutl/state.json").is_err());
        assert!(SafeRelayPath::new(".kutl").is_err());
    }

    #[test]
    fn test_rejects_empty() {
        assert!(SafeRelayPath::new("").is_err());
    }

    #[test]
    fn test_accepts_valid_relative_paths() {
        assert!(SafeRelayPath::new("notes/todo.md").is_ok());
        assert!(SafeRelayPath::new("deep/nested/path/file.txt").is_ok());
        assert!(SafeRelayPath::new("file.md").is_ok());
        assert!(SafeRelayPath::new(".hidden-file").is_ok());
        assert!(SafeRelayPath::new("dir/.hidden/file.md").is_ok());
    }

    #[test]
    fn test_under_joins_safely() {
        let root = Path::new("/home/user/space");
        let p = SafeRelayPath::new("docs/readme.md").unwrap();
        assert_eq!(
            p.under(root),
            PathBuf::from("/home/user/space/docs/readme.md")
        );
    }

    #[test]
    fn test_display() {
        let p = SafeRelayPath::new("notes/todo.md").unwrap();
        assert_eq!(p.to_string(), "notes/todo.md");
    }
}
