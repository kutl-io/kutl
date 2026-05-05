//! Tracks last-synced state for binary files.
//!
//! Persists to `.kutl/blob-state.json` so the daemon can skip re-sending
//! unchanged blobs after restart.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Filename for the persisted blob state.
const BLOB_STATE_FILE: &str = "blob-state.json";

/// Per-file last-synced blob state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobState {
    /// Hex-encoded SHA-256.
    pub hash: String,
    /// Unix millis.
    pub timestamp: i64,
    /// File size in bytes.
    pub size: i64,
}

/// Map of relative paths to their last-synced blob state.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BlobStateMap {
    entries: HashMap<PathBuf, BlobState>,
}

impl BlobStateMap {
    /// Load from `.kutl/blob-state.json`, or return empty if missing.
    pub fn load(space_root: &Path) -> Result<Self> {
        let path = space_root.join(".kutl").join(BLOB_STATE_FILE);
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let map: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(map)
    }

    /// Save to `.kutl/blob-state.json`.
    ///
    /// Uses atomic write (temp file + rename) to prevent corruption on crash.
    pub fn save(&self, space_root: &Path) -> Result<()> {
        let dir = space_root.join(".kutl");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(BLOB_STATE_FILE);
        let json = serde_json::to_string_pretty(self)?;
        let tmp = tempfile::NamedTempFile::new_in(&dir)
            .with_context(|| format!("failed to create temp file in {}", dir.display()))?;
        std::fs::write(tmp.path(), &json)
            .with_context(|| format!("failed to write temp file for {}", path.display()))?;
        tmp.persist(&path)
            .map_err(|e| anyhow::anyhow!("failed to persist {}: {}", path.display(), e.error))?;
        Ok(())
    }

    /// Get the blob state for a relative path.
    pub fn get(&self, rel_path: &Path) -> Option<&BlobState> {
        self.entries.get(rel_path)
    }

    /// Insert or update the blob state for a relative path.
    pub fn insert(&mut self, rel_path: PathBuf, state: BlobState) {
        self.entries.insert(rel_path, state);
    }

    /// Remove the blob state for a relative path.
    pub fn remove(&mut self, rel_path: &Path) {
        self.entries.remove(rel_path);
    }
}

/// Compute SHA-256 and return the hex-encoded string.
pub fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    kutl_proto::protocol::hex_encode(&hash)
}

/// Compute SHA-256 and return the raw 32-byte digest.
pub fn sha256_bytes(data: &[u8]) -> Vec<u8> {
    Sha256::digest(data).to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_hex() {
        let hash = sha256_hex(b"hello");
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_sha256_bytes() {
        let bytes = sha256_bytes(b"hello");
        assert_eq!(bytes.len(), 32);
        assert_eq!(
            sha256_hex(b"hello"),
            kutl_proto::protocol::hex_encode(&bytes)
        );
    }

    #[test]
    fn test_blob_state_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join(".kutl")).unwrap();

        let mut map = BlobStateMap::default();
        map.insert(
            PathBuf::from("image.png"),
            BlobState {
                hash: sha256_hex(b"png data"),
                timestamp: 12345,
                size: 100,
            },
        );
        map.save(root).unwrap();

        let loaded = BlobStateMap::load(root).unwrap();
        let state = loaded.get(Path::new("image.png")).unwrap();
        assert_eq!(state.hash, sha256_hex(b"png data"));
        assert_eq!(state.timestamp, 12345);
        assert_eq!(state.size, 100);
    }

    #[test]
    fn test_load_missing_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let map = BlobStateMap::load(dir.path()).unwrap();
        assert!(map.get(Path::new("anything")).is_none());
    }
}
