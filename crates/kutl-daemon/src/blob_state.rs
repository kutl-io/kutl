//! Tracks last-synced state for binary files.
//!
//! Persists to `.kutl/blob-state.ksnap`, a kutl envelope snapshot of
//! `kutl.daemon.v1.BlobState`, so the daemon can skip re-sending unchanged
//! blobs after restart. A `blob-state.json` written before the envelope loads
//! through its legacy decoder and is replaced by the next save. A file that
//! is neither shape is quarantined beside its replacement and the map starts
//! empty: every blob is re-hashed and re-sent once, which costs bandwidth
//! and never content.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use kutl_core::envelope::{self, Kind, Loaded};
use kutl_proto::daemon::{BlobEntry, BlobState as BlobStateProto};
use sha2::{Digest, Sha256};

/// Filename for the persisted blob state.
const BLOB_STATE_FILE: &str = "blob-state.ksnap";

/// Per-file last-synced blob state.
#[derive(Clone, Debug)]
pub struct BlobState {
    /// Hex-encoded SHA-256.
    pub hash: String,
    /// Unix millis.
    pub timestamp: i64,
}

/// Map of relative paths to their last-synced blob state.
#[derive(Debug, Default)]
pub struct BlobStateMap {
    entries: HashMap<PathBuf, BlobState>,
    /// Set when the map was loaded from the pre-envelope file, so the
    /// caller's first save replaces it with the envelope and removes the
    /// old file.
    needs_rewrite: bool,
}

impl BlobStateMap {
    /// The snapshot's path under `space_root`.
    fn path(space_root: &Path) -> PathBuf {
        space_root.join(".kutl").join(BLOB_STATE_FILE)
    }

    /// Load from `.kutl/blob-state.ksnap` (or the pre-envelope
    /// `blob-state.json`), or return empty if neither exists. A present file
    /// that is neither shape is quarantined and the map starts empty; only an
    /// I/O failure is an error.
    pub fn load(space_root: &Path) -> Result<Self> {
        let path = Self::path(space_root);
        match envelope::load_or_recover::<BlobStateProto>(
            Kind::BlobState,
            &path,
            Some(&crate::legacy::BLOB_STATE_V0),
            envelope::Recovery::Act,
        ) {
            Ok(None) => Ok(Self::default()),
            Ok(Some(Loaded::Envelope(proto))) => Ok(Self::from_proto(proto, false)),
            Ok(Some(Loaded::Legacy(proto))) => Ok(Self::from_proto(proto, true)),
            Err(source) => {
                Err(source).with_context(|| format!("failed to read {}", path.display()))
            }
        }
    }

    /// Whether the map came from the pre-envelope file and should be saved
    /// now so the space is migrated on this start rather than on its next
    /// blob change.
    #[must_use]
    pub fn needs_rewrite(&self) -> bool {
        self.needs_rewrite
    }

    /// Save to `.kutl/blob-state.ksnap`.
    ///
    /// Replaces the file through [`kutl_core::fs::write_atomic`], the one
    /// atomic-replace rule, so a crash mid-save leaves the previous complete
    /// map. A pre-envelope `blob-state.json` is removed once the envelope is
    /// in place, so the file never has two sources.
    pub fn save(&mut self, space_root: &Path) -> Result<()> {
        let dir = space_root.join(".kutl");
        std::fs::create_dir_all(&dir)?;
        let path = Self::path(space_root);
        envelope::write_snapshot_verified(
            Kind::BlobState,
            &path,
            kutl_core::env::now_ms(),
            &self.to_proto(),
        )
        .with_context(|| format!("failed to persist {}", path.display()))?;
        envelope::retire_legacy(Kind::BlobState, &dir, &path);
        self.needs_rewrite = false;
        Ok(())
    }

    fn to_proto(&self) -> BlobStateProto {
        BlobStateProto {
            entries: self
                .entries
                .iter()
                .map(|(path, state)| {
                    (
                        crate::core::rel_path_to_string(path),
                        BlobEntry {
                            hash: state.hash.clone(),
                            timestamp: state.timestamp,
                        },
                    )
                })
                .collect(),
        }
    }

    fn from_proto(proto: BlobStateProto, needs_rewrite: bool) -> Self {
        Self {
            entries: proto
                .entries
                .into_iter()
                .map(|(path, e)| {
                    (
                        PathBuf::from(path),
                        BlobState {
                            hash: e.hash,
                            timestamp: e.timestamp,
                        },
                    )
                })
                .collect(),
            needs_rewrite,
        }
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

    /// Re-key a blob's state on a document rename, `old` → `new`. A no-op for
    /// a path with no entry (text docs). Without this, the LWW newer-wins
    /// guard (`handle_remote_blob`) looks up the doc's CURRENT path after a
    /// rename, finds nothing, and an older replayed blob silently overwrites
    /// newer local bytes — while the stale old-path entry leaks.
    pub fn rename(&mut self, old: &Path, new: &Path) {
        if let Some(state) = self.entries.remove(old) {
            self.entries.insert(new.to_path_buf(), state);
        }
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

/// Bytes paired with their SHA-256 digest, hashed once at construction.
///
/// The digest keys the write funnel's echo suppression and the identity's
/// last-written hash, so it must be of exactly these bytes: the only
/// constructor hashes, and nothing can pair a digest with other bytes. A
/// producer that hashed for its own decision (the blob merge's LWW compare)
/// hands the pair on rather than paying a second pass in the funnel.
#[derive(Clone, PartialEq, Eq)]
pub struct HashedContent {
    bytes: Vec<u8>,
    digest: Vec<u8>,
}

impl HashedContent {
    /// Hash `bytes` once and carry both.
    pub fn new(bytes: Vec<u8>) -> Self {
        let digest = sha256_bytes(&bytes);
        Self { bytes, digest }
    }

    /// The bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The raw SHA-256 digest of the bytes.
    pub fn digest(&self) -> &[u8] {
        &self.digest
    }

    /// The bytes, giving up the digest.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// The digest as hex, the form the identity and blob records store.
    pub fn hex(&self) -> String {
        kutl_proto::protocol::hex_encode(&self.digest)
    }
}

/// Length and digest only: a payload can be tens of MiB, and an effect that
/// carries one is rendered on failure paths and at debug level.
impl std::fmt::Debug for HashedContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashedContent")
            .field("len", &self.bytes.len())
            .field("digest", &self.hex())
            .finish()
    }
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
            },
        );
        map.save(root).unwrap();

        let loaded = BlobStateMap::load(root).unwrap();
        let state = loaded.get(Path::new("image.png")).unwrap();
        assert_eq!(state.hash, sha256_hex(b"png data"));
        assert_eq!(state.timestamp, 12345);
    }

    /// A pre-envelope `blob-state.json` loads through the legacy decoder and
    /// asks to be rewritten; the next save writes the envelope and removes
    /// the JSON, so the file never has two sources.
    #[test]
    fn test_legacy_json_loads_and_the_next_save_replaces_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let kutl = root.join(".kutl");
        std::fs::create_dir_all(&kutl).unwrap();
        std::fs::write(
            kutl.join("blob-state.json"),
            include_bytes!("../legacy-fixtures/v0/blob-state.json"),
        )
        .unwrap();

        let mut map = BlobStateMap::load(root).unwrap();
        assert!(map.needs_rewrite(), "loaded from the pre-envelope file");
        assert_eq!(map.get(Path::new("image.png")).unwrap().timestamp, 12345);

        map.save(root).unwrap();
        assert!(!map.needs_rewrite());
        assert!(
            !kutl.join("blob-state.json").exists(),
            "json removed after the envelope landed"
        );
        let bytes = std::fs::read(kutl.join("blob-state.ksnap")).unwrap();
        assert!(kutl_core::envelope::has_magic(Kind::BlobState, &bytes));
        let again = BlobStateMap::load(root).unwrap();
        assert!(!again.needs_rewrite());
        assert_eq!(again.get(Path::new("image.png")).unwrap().timestamp, 12345);
    }

    /// A file that is neither shape is quarantined and the map starts
    /// empty: every blob is re-hashed and re-sent once, content untouched.
    #[test]
    fn test_corrupt_snapshot_is_quarantined_and_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let kutl = root.join(".kutl");
        std::fs::create_dir_all(&kutl).unwrap();
        let path = kutl.join("blob-state.ksnap");
        std::fs::write(&path, b"KBST\x01\x00junk that is not a frame").unwrap();

        let map = BlobStateMap::load(root).unwrap();
        assert!(map.get(Path::new("image.png")).is_none());
        assert!(!path.exists(), "moved aside");
        assert!(kutl_core::envelope::corrupt_path_for(&path).exists());
    }

    #[test]
    fn test_load_missing_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let map = BlobStateMap::load(dir.path()).unwrap();
        assert!(map.get(Path::new("anything")).is_none());
    }
}
