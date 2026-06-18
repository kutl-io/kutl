//! Git-repo detection used by `kutl init`, `kutl join`, and the
//! AGENTS.md anchor resolver in `kutl watch`.
//!
//! The detection rule is bounded by two invariants:
//!
//! 1. **At most one hop.** Check the starting directory and exactly one
//!    parent — no further. This encodes the RFD 0060 kutl-aware-repo
//!    layout: the kutl folder is a direct child of the project root.
//!    Deeper layouts (`docs/kutl/`, etc.) are intentionally not
//!    auto-detected; callers must supply the repo root explicitly.
//!    The 1-hop bound stops an incidental `.git` ancestor (a kutl
//!    workspace nested inside an unrelated project's repo) from being
//!    mistaken for the kutl-aware project root.
//!
//! 2. **`$KUTL_HOME` is a hard ceiling.** When the env var is set, the
//!    walk never crosses above it, even within the 1-hop bound.
//!    `KUTL_HOME` is the agent's declared project boundary; crossing
//!    it would bind the caller to an unrelated repo's `.git`. This
//!    mirrors the equivalent walk-up removal in `discover_space`
//!    (commit `504af1d7`, April 2026), which fixed the same class of
//!    bug from the other direction.

use std::path::{Path, PathBuf};

/// Find the kutl-aware project root, anchored on a `.git` entry.
///
/// Walk semantics:
/// - Check `start` itself.
/// - Check `start.parent()`.
/// - Stop. Do not ascend further.
///
/// `$KUTL_HOME` (when set) bounds the walk: candidates outside that
/// subtree are skipped. The module-level docs explain why.
///
/// Both regular `.git` directories and `.git` files (used by submodules
/// and worktrees) count as a match.
#[must_use]
pub fn find_git_repo_root(start: &Path) -> Option<PathBuf> {
    let ceiling = std::env::var_os("KUTL_HOME").map(PathBuf::from);
    find_git_repo_root_bounded(start, ceiling.as_deref())
}

/// Variant of [`find_git_repo_root`] that takes an explicit ceiling
/// instead of resolving it from `$KUTL_HOME`. Tests use this directly
/// for deterministic isolation from the ambient env; programmatic
/// callers can pass an explicit boundary when they want one.
#[must_use]
pub fn find_git_repo_root_bounded(start: &Path, ceiling: Option<&Path>) -> Option<PathBuf> {
    let within_ceiling = |p: &Path| -> bool {
        match ceiling {
            Some(c) => p.starts_with(c),
            None => true,
        }
    };
    if within_ceiling(start) && start.join(".git").exists() {
        return Some(start.to_path_buf());
    }
    let parent = start.parent()?;
    if within_ceiling(parent) && parent.join(".git").exists() {
        return Some(parent.to_path_buf());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_find_git_repo_root_at_start() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        let found = find_git_repo_root_bounded(dir.path(), None).unwrap();
        assert_eq!(found, dir.path());
    }

    #[test]
    fn test_find_git_repo_root_one_hop_up() {
        // Standard `repo-root/kutl/` layout: start one directory below
        // the .git anchor.
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        let kutl_dir = dir.path().join("kutl");
        std::fs::create_dir(&kutl_dir).unwrap();
        let found = find_git_repo_root_bounded(&kutl_dir, None).unwrap();
        assert_eq!(found, dir.path());
    }

    #[test]
    fn test_find_git_repo_root_two_hops_returns_none() {
        // `docs/kutl/` layout (or any 2+ levels deep) is intentionally
        // not auto-detected — strict-1 bound forces explicit setup.
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        let nested = dir.path().join("docs").join("kutl");
        std::fs::create_dir_all(&nested).unwrap();
        assert!(find_git_repo_root_bounded(&nested, None).is_none());
    }

    #[test]
    fn test_find_git_repo_root_deep_nesting_returns_none() {
        // The bug this bound exists to prevent: an agent workspace
        // nested deep inside an unrelated git repo must not bind to
        // that repo's `.git`.
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        let workspace = dir.path().join("a").join("b").join("c").join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        assert!(find_git_repo_root_bounded(&workspace, None).is_none());
    }

    #[test]
    fn test_find_git_repo_root_no_git_anywhere() {
        let dir = TempDir::new().unwrap();
        assert!(find_git_repo_root_bounded(dir.path(), None).is_none());
    }

    #[test]
    fn test_find_git_repo_root_dotgit_file() {
        // Submodules / worktrees use a `.git` file pointing at the
        // parent gitdir, not a directory. Still counts as a match.
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(".git"), "gitdir: ../.git/modules/foo").unwrap();
        let found = find_git_repo_root_bounded(dir.path(), None).unwrap();
        assert_eq!(found, dir.path());
    }

    #[test]
    fn test_ceiling_blocks_parent_walk() {
        // .git is in the parent, but ceiling = start dir. Walk must
        // not ascend above the ceiling.
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        let kutl_dir = dir.path().join("kutl");
        std::fs::create_dir(&kutl_dir).unwrap();
        let found = find_git_repo_root_bounded(&kutl_dir, Some(&kutl_dir));
        assert!(found.is_none());
    }

    #[test]
    fn test_ceiling_above_start_allows_parent_walk() {
        // Ceiling at the parent (= the candidate). Walk reaches it
        // because `parent.starts_with(parent) == true`.
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        let kutl_dir = dir.path().join("kutl");
        std::fs::create_dir(&kutl_dir).unwrap();
        let found = find_git_repo_root_bounded(&kutl_dir, Some(dir.path())).unwrap();
        assert_eq!(found, dir.path());
    }

    #[test]
    fn test_ceiling_at_start_still_allows_start_match() {
        // .git is at start, ceiling at start. Self-match works
        // because `start.starts_with(start) == true`.
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        let found = find_git_repo_root_bounded(dir.path(), Some(dir.path())).unwrap();
        assert_eq!(found, dir.path());
    }
}
