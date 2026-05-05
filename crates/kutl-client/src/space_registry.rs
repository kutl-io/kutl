//! Global space registry stored at `$KUTL_HOME/spaces.json`.
//!
//! Each entry is an absolute path to a space root directory. The per-space
//! `.kutl/space.json` is the sole source of truth for space configuration
//! (`space_id`, `relay_url`, etc.).
//!
//! The registry supports flock-based locking for concurrent access from
//! multiple processes (CLI, daemon, desktop app).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Registry of all known spaces, stored at `$KUTL_HOME/spaces.json`.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SpaceRegistry {
    /// Absolute paths to space root directories.
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

        let _lock = lock_exclusive(&path)?;
        let mut registry = Self::load(&path)?;
        f(&mut registry);
        registry.prune();
        registry.save(&path)?;
        Ok(())
    }

    /// Load the registry from disk, or return an empty one if the file
    /// does not exist.
    ///
    /// Migrates from the legacy format (objects with `path` + `relay_url` fields)
    /// to the current format (plain path strings) transparently.
    pub fn load(path: &Path) -> Result<Self> {
        let data = match std::fs::read_to_string(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => {
                return Err(e).with_context(|| format!("failed to read {}", path.display()));
            }
        };

        // Try the current format first (Vec<String>).
        if let Ok(registry) = serde_json::from_str::<Self>(&data) {
            return Ok(registry);
        }

        // Fall back to the legacy format (Vec<{path, relay_url}>).
        let legacy: LegacyRegistry = serde_json::from_str(&data)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        let spaces = legacy.spaces.into_iter().map(|e| e.path).collect();
        Ok(Self { spaces })
    }

    /// Save the registry to disk using atomic write (write-to-temp, rename).
    fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &json)
            .with_context(|| format!("failed to write {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("failed to rename {} to {}", tmp.display(), path.display()))?;
        Ok(())
    }

    /// Add a space path if not already present.
    pub fn add(&mut self, path: &str) {
        if !self.spaces.iter().any(|p| p == path) {
            self.spaces.push(path.to_owned());
        }
    }

    /// Remove entries whose directories no longer exist.
    pub fn prune(&mut self) {
        self.spaces.retain(|p| {
            std::path::Path::new(p)
                .join(".kutl")
                .join("space.json")
                .exists()
        });
    }
}

/// Legacy registry format for backward-compatible migration.
#[derive(Deserialize)]
struct LegacyRegistry {
    spaces: Vec<LegacyEntry>,
}

/// Legacy entry format: `{path, relay_url}`.
#[derive(Deserialize)]
struct LegacyEntry {
    path: String,
}

/// Acquire an exclusive `flock` on a `.lock` sibling of the given path.
///
/// Returns the lock file handle — the lock is held until the handle is dropped.
/// If the locking process dies, the OS automatically releases the lock.
///
/// File locking is Unix-only. On non-Unix platforms this is a no-op and callers
/// get advisory (best-effort) locking only.
#[cfg(unix)]
fn lock_exclusive(path: &Path) -> Result<std::fs::File> {
    use std::os::unix::io::AsRawFd;

    let lock_path = path.with_extension("lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open lock file {}", lock_path.display()))?;

    // SAFETY: `flock` is safe with a valid fd and a known flag constant.
    let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        anyhow::bail!("failed to acquire registry lock: {err}");
    }

    Ok(file)
}

/// No-op lock for non-Unix platforms.
///
/// File locking via `flock` is Unix-only. On Windows, callers receive advisory
/// (no-op) locking — concurrent writes are not prevented at the OS level.
#[cfg(not(unix))]
fn lock_exclusive(path: &Path) -> Result<std::fs::File> {
    let lock_path = path.with_extension("lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open lock file {}", lock_path.display()))?;
    Ok(file)
}

/// Default registry path: `$KUTL_HOME/spaces.json`.
pub fn registry_path() -> Result<PathBuf> {
    Ok(crate::dirs::kutl_home()?.join("spaces.json"))
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
        let path = dir.path().join("spaces.json");

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
            space_id: "live".into(),
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
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("spaces.json");
        let legacy = r#"{"spaces":[{"path":"/old/project","relay_url":"ws://localhost:9100/ws"}]}"#;
        std::fs::write(&path, legacy).unwrap();

        let reg = SpaceRegistry::load(&path).unwrap();
        assert_eq!(reg.spaces, vec!["/old/project"]);
    }
}
