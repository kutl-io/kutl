//! Git-repo detection used by `kutl init` and `kutl join`.
//!
//! Per RFD 0060, when these commands run inside a git repo, kutl prompts for
//! a dedicated subfolder so the kutl space and the git working tree do not
//! share files. The detection rule is simple: walk up from the given path
//! until a `.git` entry is found or we reach the filesystem root.

use std::path::{Path, PathBuf};

/// Find the nearest ancestor directory containing a `.git` entry.
///
/// Returns the path to the directory that contains `.git` (i.e. the git repo
/// root), or `None` if no ancestor is a git repo. Walks up from `start`,
/// checking `start` itself first.
///
/// Both regular `.git` directories and `.git` files (used by submodules and
/// worktrees) count as a match.
pub fn find_git_repo_root(start: &Path) -> Option<PathBuf> {
    let mut current = Some(start);
    while let Some(dir) = current {
        if dir.join(".git").exists() {
            return Some(dir.to_path_buf());
        }
        current = dir.parent();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_find_git_repo_root_in_root() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();

        let found = find_git_repo_root(dir.path()).unwrap();
        assert_eq!(found, dir.path());
    }

    #[test]
    fn test_find_git_repo_root_in_ancestor() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        let nested = dir.path().join("a").join("b");
        std::fs::create_dir_all(&nested).unwrap();

        let found = find_git_repo_root(&nested).unwrap();
        assert_eq!(found, dir.path());
    }

    #[test]
    fn test_find_git_repo_root_none() {
        let dir = TempDir::new().unwrap();
        // No .git anywhere.
        assert!(find_git_repo_root(dir.path()).is_none());
    }

    #[test]
    fn test_find_git_repo_root_dotgit_file() {
        // Submodules have a `.git` file pointing at the parent gitdir, not a
        // directory. The helper should still detect it.
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(".git"), "gitdir: ../.git/modules/foo").unwrap();

        let found = find_git_repo_root(dir.path()).unwrap();
        assert_eq!(found, dir.path());
    }
}
