//! Global space registry stored at `$KUTL_HOME/spaces.toml`.
//!
//! Each entry is an absolute path to a space root directory. The per-space
//! `.kutl/space.toml` is the sole source of truth for space configuration
//! (`space_id`, `relay_url`, etc.). A `spaces.json` written before the TOML
//! move still loads, in either of its historical shapes, and is rewritten on
//! first read (see [`crate::text_file`]).
//!
//! The registry supports flock-based locking for concurrent access from
//! multiple processes (CLI, daemon, desktop app).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Registry of all known spaces, stored at `$KUTL_HOME/spaces.toml`.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SpaceRegistry {
    /// Absolute paths to space root directories.
    #[serde(default)]
    pub spaces: Vec<String>,
}

impl SpaceRegistry {
    /// Atomically update the global space registry.
    ///
    /// Acquires an exclusive `flock` on `$KUTL_HOME/spaces.lock`, loads the
    /// registry, calls `f` to modify it, prunes stale entries, then saves.
    /// The lock is held across the entire read-modify-write to prevent lost
    /// updates from concurrent processes. Released automatically when the
    /// lock file descriptor is dropped.
    pub fn update(f: impl FnOnce(&mut Self)) -> Result<()> {
        let path = registry_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let _lock = crate::file_lock::lock_exclusive(&path)?;
        let mut registry = Self::load_unlocked(&path)?;
        f(&mut registry);
        registry.prune();
        registry.save(&path)?;
        Ok(())
    }

    /// Load the registry from `path` (a `.toml` path; its `.json` sibling is
    /// the never-expiring fallback, in either shape it was ever written), or
    /// return an empty one if neither file exists.
    ///
    /// A read that finds the JSON sibling rewrites the file, so it takes
    /// the same lock every write holds: an unlocked rewrite could land on
    /// top of an update another process just committed under the lock. The
    /// lock file is a sibling of a JSON file that was just observed to
    /// exist, so its directory is already there.
    pub fn load(path: &Path) -> Result<Self> {
        if !crate::text_file::json_sibling(path).exists() {
            return Self::load_unlocked(path);
        }
        let _lock = crate::file_lock::lock_exclusive(path)?;
        Self::load_unlocked(path)
    }

    /// [`Self::load`] for a caller that already holds the registry lock
    /// (a second lock in the same process would wait on the first).
    fn load_unlocked(path: &Path) -> Result<Self> {
        Ok(
            crate::text_file::load_with_legacy::<Self, crate::legacy::SpaceRegistryJsonV0>(
                path,
                crate::file_lock::write_atomic,
            )?
            .unwrap_or_default(),
        )
    }

    /// Save the registry to `path` using atomic write (write-to-temp, rename).
    /// Production writes go through [`Self::update`], which holds the lock;
    /// test harnesses that provision a private `$KUTL_HOME` call this directly.
    pub fn save(&self, path: &Path) -> Result<()> {
        crate::text_file::save(path, self, crate::file_lock::write_atomic)
            .with_context(|| format!("failed to write the space registry to {}", path.display()))
    }

    /// Add a space path if not already present.
    pub fn add(&mut self, path: &str) {
        if !self.spaces.iter().any(|p| p == path) {
            self.spaces.push(path.to_owned());
        }
    }

    /// Remove entries whose directories are no longer joined spaces.
    pub fn prune(&mut self) {
        self.spaces
            .retain(|p| crate::space_config::SpaceConfig::is_joined(std::path::Path::new(p)));
    }
}

/// The registry under an explicit kutl home (`<home>/spaces.toml`).
#[must_use]
pub fn registry_path_in(home: &Path) -> PathBuf {
    home.join("spaces.toml")
}

/// Default registry path: `$KUTL_HOME/spaces.toml`.
pub fn registry_path() -> Result<PathBuf> {
    Ok(registry_path_in(&crate::dirs::kutl_home()?))
}

/// Number of random bytes used to generate a fallback space ID.
const SPACE_ID_BYTES: usize = 16;

/// Generate a random space ID (32-char hex string from 16 random bytes).
///
/// Used as a fallback when the relay is unreachable and cannot assign a UUID.
pub fn generate_space_id() -> String {
    let mut buf = [0u8; SPACE_ID_BYTES];
    fill_random(&mut buf);
    kutl_proto::protocol::hex_encode(&buf)
}

/// Generate a random human-readable space name.
///
/// Format: `{adjective}-{noun}-{4 hex}`. E.g., `bright-falcon-a3f2`.
pub fn generate_space_name() -> String {
    use rand::RngExt;

    const ADJECTIVES: &[&str] = &[
        "amber", "bold", "bright", "calm", "clear", "cool", "crisp", "dark", "deep", "fair",
        "fast", "fresh", "glad", "gold", "green", "keen", "kind", "light", "lucky", "neat",
        "noble", "plain", "proud", "quiet", "sharp", "smart", "still", "swift", "warm", "wise",
    ];

    const NOUNS: &[&str] = &[
        "anchor", "badge", "bear", "bell", "birch", "bridge", "brook", "cedar", "cliff", "cloud",
        "crane", "crown", "delta", "eagle", "ember", "falcon", "fern", "finch", "flame", "frost",
        "grove", "heron", "iris", "lake", "maple", "oak", "pearl", "pine", "river", "stone",
    ];

    let mut rng = rand::rng();
    let adj = ADJECTIVES[rng.random_range(0..ADJECTIVES.len())];
    let noun = NOUNS[rng.random_range(0..NOUNS.len())];
    let hex: u16 = rng.random();
    format!("{adj}-{noun}-{hex:04x}")
}

/// Fill a buffer with cryptographically secure random bytes.
fn fill_random(buf: &mut [u8]) {
    use rand_core::RngCore;
    rand_core::OsRng.fill_bytes(buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::space_config::SpaceConfig;

    #[test]
    fn test_generate_space_id_format() {
        let id = generate_space_id();
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_generate_space_name_format() {
        let name = generate_space_name();
        let parts: Vec<&str> = name.split('-').collect();
        // Format: adjective-noun-hex (3 parts)
        assert_eq!(parts.len(), 3, "expected 3 parts, got: {name}");
        // Last part should be exactly 4 hex characters
        assert_eq!(parts[2].len(), 4, "hex suffix should be 4 chars: {name}");
        assert!(
            parts[2].chars().all(|c| c.is_ascii_hexdigit()),
            "hex suffix should be hex: {name}"
        );
    }

    #[test]
    fn test_registry_stores_paths_only() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("spaces.toml");

        let mut reg = SpaceRegistry::default();
        reg.add("/projects/alpha");
        reg.add("/projects/beta");
        reg.save(&path).unwrap();

        let loaded = SpaceRegistry::load(&path).unwrap();
        assert_eq!(loaded.spaces, vec!["/projects/alpha", "/projects/beta"]);
    }

    #[test]
    fn test_registry_add_deduplicates() {
        let mut reg = SpaceRegistry::default();
        reg.add("/foo");
        reg.add("/foo");
        assert_eq!(reg.spaces.len(), 1);
    }

    #[test]
    fn test_registry_prune_removes_stale() {
        let live = TempDir::new().unwrap();
        let config = SpaceConfig {
            space_id: "247610f4-dedd-4ab7-847d-07dbda19c81c".into(),
            relay_url: "ws://localhost:9100/ws".into(),
        };
        config.save(live.path()).unwrap();

        let mut reg = SpaceRegistry::default();
        reg.add(live.path().to_str().unwrap());
        reg.add("/nonexistent/dead/path");
        assert_eq!(reg.spaces.len(), 2);

        reg.prune();
        assert_eq!(reg.spaces.len(), 1);
        assert_eq!(reg.spaces[0], live.path().to_str().unwrap());
    }

    #[test]
    fn test_registry_migrates_legacy_format() {
        // The original `{path, relay_url}` shape, in a pre-move spaces.json:
        // loads through the fallback and is rewritten as TOML paths.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("spaces.toml");
        let json_path = dir.path().join("spaces.json");
        let legacy = r#"{"spaces":[{"path":"/old/project","relay_url":"ws://localhost:9100/ws"}]}"#;
        std::fs::write(&json_path, legacy).unwrap();

        let reg = SpaceRegistry::load(&path).unwrap();
        assert_eq!(reg.spaces, vec!["/old/project"]);
        assert!(path.exists() && !json_path.exists(), "rewritten as toml");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "spaces = [\"/old/project\"]\n"
        );
    }

    #[test]
    fn test_registry_reads_plain_json_paths() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("spaces.toml");
        std::fs::write(dir.path().join("spaces.json"), r#"{"spaces":["/a","/b"]}"#).unwrap();
        assert_eq!(SpaceRegistry::load(&path).unwrap().spaces, vec!["/a", "/b"]);
    }
}
