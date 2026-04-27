//! Per-space configuration stored at `.kutl/space.json`.
//!
//! Each kutl space has a `.kutl/space.json` file containing per-installation
//! runtime state: the space ID and the relay WebSocket URL. The team-wide
//! canonical `space_name` is held separately in `.kutlspace` (RFD 0060).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Default relay URL for local development.
pub const DEFAULT_RELAY_URL: &str = "ws://127.0.0.1:9100/ws";

/// Per-space config stored at `.kutl/space.json`.
///
/// Holds per-installation runtime state. The team-wide canonical
/// `space_name` lives in `.kutlspace` (see [`crate::KutlspaceConfig`]) per
/// RFD 0060.
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

/// Discover a space configuration.
///
/// Two strategies, tried in order:
/// 1. Check `search_root` directly for `.kutl/space.json` (works when
///    the search root IS the workspace directory).
/// 2. Read `$KUTL_HOME/spaces.json` and check each listed workspace for
///    `.kutl/space.json` (works when the search root is a separate config
///    directory).
///
/// Does NOT walk up to ancestor directories — that would find unrelated
/// spaces when `KUTL_HOME` is nested inside a workspace tree (e.g., the
/// repo root). Use `kutl init` for ancestor-based discovery.
///
/// Returns the config and the workspace root path, or `None` if no space
/// is found.
pub fn discover_space(search_root: &Path) -> Option<(SpaceConfig, PathBuf)> {
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

    // Strategy 2: read the spaces.json registry.
    let registry_path = crate::space_registry::registry_path()
        .map_err(|e| {
            tracing::warn!(error = %e, "failed to resolve spaces.json path");
        })
        .ok()?;
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
            space_id: "abc123".into(),
            relay_url: "ws://localhost:9100/ws".into(),
        };
        config.save(dir.path()).unwrap();

        let loaded = SpaceConfig::load(dir.path()).unwrap();
        assert_eq!(loaded.space_id, "abc123");
        assert_eq!(loaded.relay_url, "ws://localhost:9100/ws");

        // Verify .kutl/docs/ was created
        assert!(dir.path().join(".kutl").join("docs").is_dir());
    }
}
