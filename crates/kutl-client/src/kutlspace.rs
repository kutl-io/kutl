//! Team-wide kutl space configuration (`.kutlspace`).
//!
//! Per RFD 0060, every kutl space has a `.kutlspace` TOML file at its root
//! containing team-wide config: the canonical `space_name` and an optional
//! `[surface]` section for `kutl surface`. The file is git-tracked but NOT
//! synced via the relay — the daemon ignores it like any other dot-file.
//!
//! Per-installation runtime state (`space_id`, `relay_url`) lives in
//! `.kutl/space.json` instead and is owned by the daemon.

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// File name for the team-wide kutl space config.
pub const KUTLSPACE_FILENAME: &str = ".kutlspace";

/// Contents of `.kutlspace`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KutlspaceConfig {
    /// Canonical human-readable space name. Used to resolve the space at
    /// the relay (e.g. by `kutl join` with no arguments).
    pub space_name: String,
    /// Optional `[surface]` section. Present only when surfacing has been
    /// configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<SurfaceConfig>,
}

/// `[surface]` section of `.kutlspace`. Configures `kutl surface`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceConfig {
    /// Destination path for `kutl surface`, relative to the space root.
    /// The default `"../"` mirrors content into the parent directory.
    pub target: String,
}

impl KutlspaceConfig {
    /// Resolve the human-readable space name for display purposes.
    ///
    /// Loads `.kutlspace` from `space_root` and returns its `space_name` if
    /// present. Falls back to `fallback` (typically the `space_id`) when
    /// `.kutlspace` is missing or unparseable.
    ///
    /// This is the standard pattern for any UI/log site that wants to show
    /// a human-friendly name without forcing the caller to handle the
    /// `Result<Option<Self>>` shape.
    pub fn display_name(space_root: &Path, fallback: &str) -> String {
        Self::load(space_root)
            .ok()
            .flatten()
            .map_or_else(|| fallback.to_owned(), |k| k.space_name)
    }

    /// Load the kutlspace config from `<space_root>/.kutlspace`.
    ///
    /// Returns `Ok(None)` if the file does not exist (the space is not
    /// kutl-marked). Returns `Err` for I/O or parse failures.
    pub fn load(space_root: &Path) -> Result<Option<Self>> {
        let path = space_root.join(KUTLSPACE_FILENAME);
        let data = match std::fs::read_to_string(&path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(err).with_context(|| format!("failed to read {}", path.display()));
            }
        };
        let config: Self =
            toml::from_str(&data).with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(Some(config))
    }

    /// Save the kutlspace config to `<space_root>/.kutlspace`.
    ///
    /// Overwrites any existing file. The destination directory must already
    /// exist (callers typically create the space root first).
    pub fn save(&self, space_root: &Path) -> Result<()> {
        let path = space_root.join(KUTLSPACE_FILENAME);
        let serialized =
            toml::to_string_pretty(self).context("failed to serialize kutlspace config")?;
        std::fs::write(&path, serialized)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_load_minimal() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join(".kutlspace"),
            "space_name = \"my-project-docs\"\n",
        )
        .unwrap();

        let config = KutlspaceConfig::load(dir.path()).unwrap().unwrap();
        assert_eq!(config.space_name, "my-project-docs");
        assert!(config.surface.is_none());
    }

    #[test]
    fn test_load_with_surface() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join(".kutlspace"),
            "space_name = \"my-project-docs\"\n\n[surface]\ntarget = \"../\"\n",
        )
        .unwrap();

        let config = KutlspaceConfig::load(dir.path()).unwrap().unwrap();
        assert_eq!(config.space_name, "my-project-docs");
        assert_eq!(config.surface.unwrap().target, "../");
    }

    #[test]
    fn test_load_missing_returns_none() {
        let dir = TempDir::new().unwrap();
        let result = KutlspaceConfig::load(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_load_invalid_toml_returns_err() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(".kutlspace"), "this is not = valid = toml").unwrap();

        let result = KutlspaceConfig::load(dir.path());
        let err = result.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(".kutlspace"),
            "error should mention the file name, got: {msg}"
        );
    }

    #[test]
    fn test_save_roundtrip() {
        let dir = TempDir::new().unwrap();
        let original = KutlspaceConfig {
            space_name: "calm-eagle-0f1a".into(),
            surface: Some(SurfaceConfig {
                target: "../".into(),
            }),
        };
        original.save(dir.path()).unwrap();

        let loaded = KutlspaceConfig::load(dir.path()).unwrap().unwrap();
        assert_eq!(loaded, original);
    }

    #[test]
    fn test_save_minimal_omits_surface() {
        let dir = TempDir::new().unwrap();
        let config = KutlspaceConfig {
            space_name: "doc-space".into(),
            surface: None,
        };
        config.save(dir.path()).unwrap();

        let written = std::fs::read_to_string(dir.path().join(".kutlspace")).unwrap();
        assert!(written.contains("space_name = \"doc-space\""));
        assert!(!written.contains("[surface]"));
    }

    #[test]
    fn test_display_name_present() {
        let dir = TempDir::new().unwrap();
        KutlspaceConfig {
            space_name: "my-space".into(),
            surface: None,
        }
        .save(dir.path())
        .unwrap();

        assert_eq!(
            KutlspaceConfig::display_name(dir.path(), "fallback"),
            "my-space"
        );
    }

    #[test]
    fn test_display_name_missing_uses_fallback() {
        let dir = TempDir::new().unwrap();
        assert_eq!(
            KutlspaceConfig::display_name(dir.path(), "fallback"),
            "fallback"
        );
    }
}
