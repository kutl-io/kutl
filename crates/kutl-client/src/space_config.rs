//! Per-space configuration stored at `.kutl/space.json`.
//!
//! Each kutl space has a `.kutl/space.json` file containing per-installation
//! runtime state: the space ID and the relay WebSocket URL. The team-wide
//! canonical `space_name` is held separately in `.kutlspace`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Default relay URL for local development.
pub const DEFAULT_RELAY_URL: &str = "ws://127.0.0.1:9100/ws";

/// Per-space config stored at `.kutl/space.json`.
///
/// Holds per-installation runtime state. The team-wide canonical
/// `space_name` lives in `.kutlspace` (see [`crate::KutlspaceConfig`]).
#[derive(Debug, Serialize, Deserialize)]
pub struct SpaceConfig {
    /// Unique space identifier (UUID from the relay, or local hex fallback).
    pub space_id: String,
    /// WebSocket URL of the sync relay.
    pub relay_url: String,
}

impl SpaceConfig {
    /// Load the space config from `.kutl/space.json` under `space_root`.
    ///
    /// Normalizes the relay URL to ensure it ends with `/ws`.
    pub fn load(space_root: &Path) -> Result<Self> {
        let path = space_root.join(".kutl").join("space.json");
        let data = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read space config from {}", path.display()))?;
        let mut config: Self = serde_json::from_str(&data)
            .with_context(|| format!("failed to parse space config from {}", path.display()))?;
        config.relay_url = crate::normalize_relay_url(&config.relay_url);
        Ok(config)
    }

    /// Save the space config to `.kutl/space.json` under `space_root`.
    ///
    /// Normalizes the relay URL to ensure it ends with `/ws`.
    /// Creates `.kutl/` and `.kutl/docs/` directories if they don't exist.
    pub fn save(&self, space_root: &Path) -> Result<()> {
        let dotdir = space_root.join(".kutl");
        std::fs::create_dir_all(&dotdir)
            .with_context(|| format!("failed to create {}", dotdir.display()))?;

        let docs_dir = dotdir.join("docs");
        std::fs::create_dir_all(&docs_dir)
            .with_context(|| format!("failed to create {}", docs_dir.display()))?;

        let normalized = Self {
            space_id: self.space_id.clone(),
            relay_url: crate::normalize_relay_url(&self.relay_url),
        };

        let path = dotdir.join("space.json");
        let json = serde_json::to_string_pretty(&normalized)?;
        std::fs::write(&path, json)
            .with_context(|| format!("failed to write {}", path.display()))?;

        Ok(())
    }
}

/// Find the innermost kutl space root at or above `start`.
///
/// Walks from `start` toward the filesystem root, returning the FIRST
/// directory holding a space marker — `.kutl/space.json` (a joined,
/// operable space) or `.kutlspace` (a kutl-marked folder not yet joined
/// on this machine). Matching either marker is deliberate: standing
/// inside an unjoined marked folder must surface "join this one", never
/// silently resolve to an ENCLOSING joined space. Innermost wins when
/// spaces nest.
///
/// `$KUTL_HOME` (when set) is a hard boundary, exactly as for
/// [`crate::find_git_repo_root`]: NOTHING outside it resolves — not an
/// ancestor, not even the start directory itself. Fail closed. A set
/// `KUTL_HOME` declares the process's workspace (the agent case), and a
/// process roaming outside its declared workspace must never bind to —
/// and then sync or author into — whatever space its cwd happens to sit
/// inside. Unset (a human's shell), the walk is unbounded — cwd-first
/// resolution behaves like `git`'s.
#[must_use]
pub fn find_space_root_upward(start: &Path) -> Option<PathBuf> {
    find_space_root_upward_ceiling(start, &crate::bounds::Ceiling::from_env())
}

/// Variant of [`find_space_root_upward`] that takes an explicit ceiling
/// instead of resolving it from `$KUTL_HOME`. Tests use this directly
/// for deterministic isolation from the ambient env.
#[must_use]
pub fn find_space_root_upward_bounded(start: &Path, ceiling: Option<&Path>) -> Option<PathBuf> {
    find_space_root_upward_ceiling(start, &crate::bounds::Ceiling::explicit(ceiling))
}

/// The walk itself, against a resolved [`crate::bounds::Ceiling`] — the
/// shared boundary type that owns env resolution and the symlink-proof
/// containment comparison for every bounded walk.
fn find_space_root_upward_ceiling(
    start: &Path,
    ceiling: &crate::bounds::Ceiling,
) -> Option<PathBuf> {
    // Canonicalize the start so the ceiling comparison cannot be defeated
    // by symlinks (macOS `/tmp` and `/var` are symlinks into `/private`,
    // so an env-var ceiling and a resolved cwd can name the same directory
    // through different paths). The ceiling side is canonicalized by
    // `Ceiling` itself.
    let start = start.canonicalize().ok()?;
    let mut current: Option<&Path> = Some(&start);
    while let Some(dir) = current {
        if !ceiling.contains(dir) {
            // Outside the boundary — fail closed (see above). This also
            // rejects a START outside the ceiling, not just ancestors.
            return None;
        }
        if dir.join(".kutl").join("space.json").exists() || dir.join(".kutlspace").exists() {
            return Some(dir.to_path_buf());
        }
        current = dir.parent();
    }
    None
}

/// Discover a space configuration.
///
/// Two strategies, tried in order:
/// 1. Check `search_root` directly for `.kutl/space.json` (works when
///    the search root IS the workspace directory).
/// 2. Read `$KUTL_HOME/spaces.json` and check each listed workspace for
///    `.kutl/space.json` (works when the search root is a separate config
///    directory).
///
/// Does NOT walk up to ancestor directories — an unbounded walk would
/// find unrelated spaces when `KUTL_HOME` is nested inside a workspace
/// tree (e.g., the repo root). [`find_space_root_upward`] is the walking
/// variant; it solves that concern with `$KUTL_HOME` as a hard ceiling.
///
/// Returns the config and the workspace root path, or `None` if no space
/// is found.
pub fn discover_space(search_root: &Path) -> Option<(SpaceConfig, PathBuf)> {
    discover_space_with_registry(search_root, None)
}

/// Test-friendly variant of [`discover_space`] that accepts an explicit
/// `spaces.json` path instead of resolving it from `$KUTL_HOME`.
///
/// Production callers use [`discover_space`] (which passes `None` and
/// resolves the registry via `$KUTL_HOME` exactly as before). Tests
/// pass `Some(&isolated_path)` to a path that does not exist, which
/// makes Strategy 2 fail deterministically without depending on the
/// global env-var or any sibling test's state. Both prevent the
/// cross-test pollution that previously made `test_discover_space_*`
/// flaky under `cargo test --workspace`.
pub fn discover_space_with_registry(
    search_root: &Path,
    registry_override: Option<&Path>,
) -> Option<(SpaceConfig, PathBuf)> {
    // Strategy 1: check search_root directly.
    if search_root.join(".kutl").join("space.json").exists() {
        return SpaceConfig::load(search_root)
            .map_err(|e| {
                tracing::warn!(
                    path = %search_root.display(),
                    error = %e,
                    "failed to load space config"
                );
            })
            .ok()
            .map(|c| (c, search_root.to_path_buf()));
    }

    // Strategy 2: read the spaces.json registry. Use the override when
    // provided (tests); otherwise resolve from `$KUTL_HOME`.
    let registry_path: PathBuf = match registry_override {
        Some(p) => p.to_path_buf(),
        None => crate::space_registry::registry_path()
            .map_err(|e| {
                tracing::warn!(error = %e, "failed to resolve spaces.json path");
            })
            .ok()?,
    };
    let registry = crate::space_registry::SpaceRegistry::load(&registry_path)
        .map_err(|e| {
            tracing::warn!(error = %e, "failed to load spaces.json");
        })
        .ok()?;
    for space_path in &registry.spaces {
        let root = PathBuf::from(space_path);
        if root.join(".kutl").join("space.json").exists() {
            return SpaceConfig::load(&root)
                .map_err(|e| {
                    tracing::warn!(
                        path = %root.display(),
                        error = %e,
                        "failed to load space config"
                    );
                })
                .ok()
                .map(|c| (c, root));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_space_config_save_load() {
        let dir = TempDir::new().unwrap();
        let config = SpaceConfig {
            space_id: "6ca13d52-ca70-4883-80f0-bb101e425a89".into(),
            relay_url: "ws://localhost:9100/ws".into(),
        };
        config.save(dir.path()).unwrap();

        let loaded = SpaceConfig::load(dir.path()).unwrap();
        assert_eq!(loaded.space_id, "6ca13d52-ca70-4883-80f0-bb101e425a89");
        assert_eq!(loaded.relay_url, "ws://localhost:9100/ws");

        // Verify .kutl/docs/ was created
        assert!(dir.path().join(".kutl").join("docs").is_dir());
    }

    /// Create a joined-space marker (`.kutl/space.json`) under `root`.
    fn mark_joined(root: &Path) {
        std::fs::create_dir_all(root.join(".kutl")).unwrap();
        std::fs::write(root.join(".kutl").join("space.json"), "{}").unwrap();
    }

    #[test]
    fn test_find_space_root_upward_at_start() {
        let dir = TempDir::new().unwrap();
        mark_joined(dir.path());
        let found = find_space_root_upward_bounded(dir.path(), None).unwrap();
        assert_eq!(found, dir.path().canonicalize().unwrap());
    }

    #[test]
    fn test_find_space_root_upward_deep_subdir() {
        // Unlike git detection's 1-hop bound, the space walk ascends any
        // depth: `kutl sync` from `space/docs/notes/` must find the space.
        let dir = TempDir::new().unwrap();
        mark_joined(dir.path());
        let nested = dir.path().join("docs").join("notes").join("deep");
        std::fs::create_dir_all(&nested).unwrap();
        let found = find_space_root_upward_bounded(&nested, None).unwrap();
        assert_eq!(found, dir.path().canonicalize().unwrap());
    }

    #[test]
    fn test_find_space_root_upward_innermost_wins() {
        let dir = TempDir::new().unwrap();
        mark_joined(dir.path());
        let inner = dir.path().join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        mark_joined(&inner);
        let below = inner.join("sub");
        std::fs::create_dir_all(&below).unwrap();
        let found = find_space_root_upward_bounded(&below, None).unwrap();
        assert_eq!(found, inner.canonicalize().unwrap());
    }

    #[test]
    fn test_find_space_root_upward_kutlspace_marker_matches() {
        // A `.kutlspace`-only folder (cloned, not joined) still anchors the
        // walk — callers surface the join hint instead of resolving past it
        // to an enclosing joined space.
        let dir = TempDir::new().unwrap();
        mark_joined(dir.path());
        let unjoined = dir.path().join("clone");
        std::fs::create_dir_all(&unjoined).unwrap();
        std::fs::write(unjoined.join(".kutlspace"), "space_name = \"x\"\n").unwrap();
        let below = unjoined.join("sub");
        std::fs::create_dir_all(&below).unwrap();
        let found = find_space_root_upward_bounded(&below, None).unwrap();
        assert_eq!(found, unjoined.canonicalize().unwrap());
    }

    #[test]
    fn test_find_space_root_upward_ceiling_blocks_ascent() {
        // Space marker above the ceiling: the walk must not bind to it.
        let dir = TempDir::new().unwrap();
        mark_joined(dir.path());
        let workspace = dir.path().join("agent-home").join("work");
        std::fs::create_dir_all(&workspace).unwrap();
        let ceiling = dir.path().join("agent-home");
        assert!(find_space_root_upward_bounded(&workspace, Some(&ceiling)).is_none());
    }

    #[test]
    fn test_find_space_root_upward_ceiling_allows_within() {
        let dir = TempDir::new().unwrap();
        let space = dir.path().join("home").join("space");
        let below = space.join("docs");
        std::fs::create_dir_all(&below).unwrap();
        mark_joined(&space);
        let ceiling = dir.path().join("home");
        let found = find_space_root_upward_bounded(&below, Some(&ceiling)).unwrap();
        assert_eq!(found, space.canonicalize().unwrap());
    }

    #[test]
    fn test_find_space_root_upward_start_outside_ceiling_fails_closed() {
        // A set ceiling declares the process's workspace: a start OUTSIDE it
        // must resolve NOTHING — even when the start itself is a real,
        // joined space. This is the guard against a boundary-declaring
        // process (an agent) roaming into someone else's space tree and
        // binding to it.
        let dir = TempDir::new().unwrap();
        let space = dir.path().join("space");
        std::fs::create_dir_all(&space).unwrap();
        mark_joined(&space);
        let declared_home = dir.path().join("kutl-home");
        std::fs::create_dir_all(&declared_home).unwrap();
        assert!(find_space_root_upward_bounded(&space, Some(&declared_home)).is_none());
    }

    #[test]
    fn test_find_space_root_upward_no_marker_anywhere() {
        let dir = TempDir::new().unwrap();
        let below = dir.path().join("plain");
        std::fs::create_dir_all(&below).unwrap();
        // Ceiling at the temp dir so the walk cannot escape into the real
        // filesystem (where an ancestor might genuinely hold a marker).
        assert!(find_space_root_upward_bounded(&below, Some(dir.path())).is_none());
    }
}
