//! Atomic creation of secret files with restricted permissions.
//!
//! Both [`crate::identity`] (Ed25519 private key) and [`crate::credentials`]
//! (bearer token) persist secrets to disk. Creating a file under the umask
//! (typically mode 0644) and *then* tightening it with `set_permissions`
//! leaves a window where the secret is world-readable. This module creates
//! the file with the restricted mode atomically so the secret is never
//! exposed.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};

/// File mode for secret files: owner read/write only (0600).
#[cfg(unix)]
const SECRET_FILE_MODE: u32 = 0o600;

/// Write `bytes` to `path`, creating or truncating the file.
///
/// On unix the file is created atomically with mode 0600 so the secret is
/// never world-readable, even transiently. On non-unix platforms it falls
/// back to a plain truncating write (permission model is platform-specific
/// and not enforced here).
pub fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut opts = OpenOptions::new();
    opts.write(true).create(true).truncate(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(SECRET_FILE_MODE);
    }

    let mut file = opts
        .open(path)
        .with_context(|| format!("failed to create secret file {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to write secret file {}", path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_write_secret_file_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("secret");
        write_secret_file(&path, b"top secret").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"top secret");
    }

    #[test]
    fn test_write_secret_file_truncates_existing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, b"old longer content").unwrap();
        write_secret_file(&path, b"new").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }

    #[cfg(unix)]
    #[test]
    fn test_write_secret_file_creates_with_restricted_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("secret");
        write_secret_file(&path, b"data").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "secret file must be created mode 0600");
    }
}
