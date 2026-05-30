//! File-based DID authorization for the open-source relay.
//!
//! Reads a plain-text file listing allowed DIDs (one per line), similar to
//! SSH's `authorized_keys`. Comments (`#`) and blank lines are ignored.
//! The file is re-read on every check, so changes take effect immediately
//! without a relay restart.

use std::collections::HashSet;
use std::io;
use std::path::PathBuf;

use tracing::warn;

/// File-based DID authorization list.
///
/// Each call to [`is_authorized`](AuthorizedKeys::is_authorized) re-reads the
/// file from disk, allowing live updates without restarting the relay.
pub struct AuthorizedKeys {
    path: PathBuf,
}

impl AuthorizedKeys {
    /// Create a new `AuthorizedKeys` backed by the given file path.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Check whether `did` appears in the authorized keys file.
    ///
    /// Returns `true` if the DID is listed. If the file does not exist or
    /// cannot be read, logs a warning and returns `false`.
    pub fn is_authorized(&self, did: &str) -> bool {
        match self.load() {
            Ok(keys) => keys.contains(did),
            Err(e) => {
                warn!(path = %self.path.display(), error = %e, "failed to read authorized keys file");
                false
            }
        }
    }

    /// Read the authorized keys file, filtering out comments and blank lines.
    ///
    /// Each non-empty, non-comment line is treated as a DID string.
    fn load(&self) -> io::Result<HashSet<String>> {
        let content = std::fs::read_to_string(&self.path)?;
        let keys = content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(String::from)
            .collect();
        Ok(keys)
    }
}
