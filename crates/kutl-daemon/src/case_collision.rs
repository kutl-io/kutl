//! Case-collision detection for kutl spaces.
//!
//! On case-sensitive filesystems (Linux, and macOS with case-sensitive APFS),
//! a user can legitimately hold `foo.md` and `Foo.md` side by side. The relay
//! rejects case-variant duplicates via a unique index on `(space_id, lower(path))`,
//! so these files cannot coexist in a synced space. This module lets the daemon
//! and CLI detect such pairs at workspace-entry time and refuse to proceed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use thiserror::Error;
use walkdir::WalkDir;

use crate::watcher;

/// A set of relative paths whose lowercased form collides.
///
/// All paths in this group are siblings in the same lowercased namespace
/// and therefore cannot coexist in a synced kutl space.
#[derive(Debug, Clone)]
pub struct CaseCollisionGroup {
    /// The shared lowercased relative path.
    pub lowercased: String,
    /// The colliding relative paths, in lexicographic order for stable output.
    pub paths: Vec<PathBuf>,
}

/// Error returned when case-variant duplicates are detected in a workspace.
///
/// Contains every collision group found. Format with
/// [`CaseCollisionError::format_user_message`] for user-facing output.
#[derive(Debug, Error)]
#[error("case-variant paths cannot coexist in a kutl space")]
pub struct CaseCollisionError {
    pub groups: Vec<CaseCollisionGroup>,
}

impl CaseCollisionError {
    /// Render a multi-line message suitable for stderr.
    pub fn format_user_message(&self) -> String {
        let mut out = String::from("case-variant paths cannot coexist in a kutl space:\n");
        for group in &self.groups {
            let names: Vec<String> = group
                .paths
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect();
            out.push_str("  ");
            out.push_str(&names.join(", "));
            out.push('\n');
        }
        out.push_str("rename or remove one of each pair before syncing.");
        out
    }
}

/// Walk `root` and return `Err` if any two relative paths share the same
/// lowercased form. Ignores the same paths the file watcher ignores
/// (hidden paths, `.kutl/`, `.git/`, etc.).
///
/// Lowercasing uses [`str::to_lowercase`] — sufficient for the ASCII paths
/// that dominate real workspaces. Unicode normalization (NFC vs NFD) is
/// explicitly out of scope; see the design doc.
pub fn detect_case_collisions(root: &Path) -> Result<(), CaseCollisionError> {
    let mut groups: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();

    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let Ok(rel) = entry.path().strip_prefix(root) else {
            continue;
        };
        if watcher::should_ignore(rel) {
            continue;
        }
        let key = rel.to_string_lossy().to_lowercase();
        groups.entry(key).or_default().push(rel.to_path_buf());
    }

    let mut collisions: Vec<CaseCollisionGroup> = groups
        .into_iter()
        .filter(|(_, paths)| paths.len() >= 2)
        .map(|(lowercased, mut paths)| {
            paths.sort();
            CaseCollisionGroup { lowercased, paths }
        })
        .collect();

    if collisions.is_empty() {
        Ok(())
    } else {
        collisions.sort_by(|a, b| a.lowercased.cmp(&b.lowercased));
        Err(CaseCollisionError { groups: collisions })
    }
}

/// Check whether `candidate` would collide (case-insensitively) with any
/// path in `tracked`. Returns the existing tracked path on hit, `None` on miss.
///
/// An exact path match is **not** a collision — only a case-variant of a
/// different path counts. This makes the function safe to call on the
/// "is this an already-tracked document?" path in the watcher.
pub fn find_case_variant<'a>(
    candidate: &Path,
    tracked: impl IntoIterator<Item = &'a Path>,
) -> Option<PathBuf> {
    let candidate_lower = candidate.to_string_lossy().to_lowercase();
    for existing in tracked {
        if existing == candidate {
            continue;
        }
        if existing.to_string_lossy().to_lowercase() == candidate_lower {
            return Some(existing.to_path_buf());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// Returns `true` if `dir` lives on a case-sensitive filesystem.
    ///
    /// Probes by writing a file and checking whether its case-variant is a
    /// distinct path. Used to skip collision tests on macOS case-insensitive
    /// APFS/HFS+.
    fn is_case_sensitive_fs(dir: &std::path::Path) -> bool {
        let probe = dir.join("__kutl_cs_probe__.md");
        let probe_upper = dir.join("__KUTL_CS_PROBE__.md");
        fs::write(&probe, "probe").unwrap();
        let sensitive = !probe_upper.exists();
        fs::remove_file(&probe).unwrap();
        sensitive
    }

    #[test]
    fn test_detect_case_collisions_empty_dir() {
        let dir = tempdir().unwrap();
        assert!(detect_case_collisions(dir.path()).is_ok());
    }

    #[test]
    fn test_detect_case_collisions_no_collision() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.md"), "a").unwrap();
        fs::write(dir.path().join("b.md"), "b").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/c.md"), "c").unwrap();
        assert!(detect_case_collisions(dir.path()).is_ok());
    }

    #[test]
    fn test_detect_case_collisions_single_pair() {
        let dir = tempdir().unwrap();
        if !is_case_sensitive_fs(dir.path()) {
            eprintln!(
                "skipping test_detect_case_collisions_single_pair: filesystem is case-insensitive"
            );
            return; // case-insensitive FS can't hold two case-variant files
        }
        fs::write(dir.path().join("foo.md"), "a").unwrap();
        fs::write(dir.path().join("Foo.md"), "b").unwrap();
        let err = detect_case_collisions(dir.path()).unwrap_err();
        assert_eq!(err.groups.len(), 1);
        let group = &err.groups[0];
        assert_eq!(group.lowercased, "foo.md");
        assert_eq!(group.paths.len(), 2);
        let names: Vec<&str> = group.paths.iter().map(|p| p.to_str().unwrap()).collect();
        assert!(names.contains(&"foo.md"));
        assert!(names.contains(&"Foo.md"));
    }

    #[test]
    fn test_detect_case_collisions_multiple_groups() {
        let dir = tempdir().unwrap();
        if !is_case_sensitive_fs(dir.path()) {
            eprintln!(
                "skipping test_detect_case_collisions_multiple_groups: filesystem is case-insensitive"
            );
            return; // case-insensitive FS can't hold two case-variant files
        }
        fs::write(dir.path().join("foo.md"), "a").unwrap();
        fs::write(dir.path().join("FOO.md"), "b").unwrap();
        fs::create_dir(dir.path().join("notes")).unwrap();
        fs::write(dir.path().join("notes/todo.md"), "c").unwrap();
        fs::write(dir.path().join("notes/TODO.md"), "d").unwrap();
        let err = detect_case_collisions(dir.path()).unwrap_err();
        assert_eq!(err.groups.len(), 2);
        let lowered: Vec<&str> = err.groups.iter().map(|g| g.lowercased.as_str()).collect();
        assert!(lowered.contains(&"foo.md"));
        assert!(lowered.contains(&"notes/todo.md"));
    }

    #[test]
    fn test_detect_case_collisions_three_way_group() {
        let dir = tempdir().unwrap();
        if !is_case_sensitive_fs(dir.path()) {
            eprintln!(
                "skipping test_detect_case_collisions_three_way_group: filesystem is case-insensitive"
            );
            return; // case-insensitive FS can't hold three case-variant files
        }
        fs::write(dir.path().join("readme.md"), "a").unwrap();
        fs::write(dir.path().join("README.md"), "b").unwrap();
        fs::write(dir.path().join("ReadMe.md"), "c").unwrap();
        let err = detect_case_collisions(dir.path()).unwrap_err();
        assert_eq!(err.groups.len(), 1);
        assert_eq!(err.groups[0].paths.len(), 3);
    }

    #[test]
    fn test_detect_case_collisions_skips_hidden() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join(".kutl")).unwrap();
        fs::write(dir.path().join(".kutl/a.md"), "a").unwrap();
        fs::write(dir.path().join(".kutl/A.md"), "b").unwrap();
        // Visible, no collision.
        fs::write(dir.path().join("c.md"), "c").unwrap();
        assert!(detect_case_collisions(dir.path()).is_ok());
    }

    #[test]
    fn test_format_user_message() {
        let err = CaseCollisionError {
            groups: vec![
                CaseCollisionGroup {
                    lowercased: "foo.md".to_string(),
                    paths: vec![PathBuf::from("foo.md"), PathBuf::from("Foo.md")],
                },
                CaseCollisionGroup {
                    lowercased: "notes/todo.md".to_string(),
                    paths: vec![
                        PathBuf::from("notes/todo.md"),
                        PathBuf::from("notes/TODO.md"),
                    ],
                },
            ],
        };
        let msg = err.format_user_message();
        assert!(msg.contains("case-variant paths cannot coexist"));
        assert!(msg.contains("foo.md, Foo.md") || msg.contains("Foo.md, foo.md"));
        assert!(
            msg.contains("notes/todo.md, notes/TODO.md")
                || msg.contains("notes/TODO.md, notes/todo.md")
        );
        assert!(msg.contains("rename or remove"));
    }

    #[test]
    fn test_find_case_variant_hit() {
        let tracked = [PathBuf::from("foo.md"), PathBuf::from("bar.md")];
        let hit = find_case_variant(Path::new("Foo.md"), tracked.iter().map(PathBuf::as_path));
        assert_eq!(hit, Some(PathBuf::from("foo.md")));
    }

    #[test]
    fn test_find_case_variant_miss() {
        let tracked = [PathBuf::from("foo.md"), PathBuf::from("bar.md")];
        let hit = find_case_variant(Path::new("baz.md"), tracked.iter().map(PathBuf::as_path));
        assert!(hit.is_none());
    }

    #[test]
    fn test_find_case_variant_same_path_is_not_a_collision() {
        // The candidate path being identical to a tracked path is not a
        // collision — it's an ordinary re-tracking.
        let tracked = [PathBuf::from("foo.md")];
        let hit = find_case_variant(Path::new("foo.md"), tracked.iter().map(PathBuf::as_path));
        assert!(hit.is_none());
    }
}
