//! Credential storage and resolution.
//!
//! Manages `$KUTL_HOME/auth.json` for stored authentication tokens
//! and resolves credentials from environment or file.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Environment variable for token override.
pub const TOKEN_ENV_VAR: &str = "KUTL_TOKEN";

/// Stored credentials from `$KUTL_HOME/auth.json`.
#[derive(Debug, Serialize, Deserialize)]
pub struct StoredCredentials {
    /// Bearer token (`kutl_` prefix).
    pub token: String,
    /// Relay WebSocket URL.
    pub relay_url: String,
    /// Account ID on kutlhub.
    pub account_id: String,
    /// Human-readable display name.
    pub display_name: String,
}

impl StoredCredentials {
    /// Load credentials from a JSON file. Returns `None` if file does not exist.
    pub fn load(path: &Path) -> Result<Option<Self>> {
        let data = match std::fs::read_to_string(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("failed to read credentials from {}", path.display())
                });
            }
        };
        let creds: Self = serde_json::from_str(&data)
            .with_context(|| format!("failed to parse credentials from {}", path.display()))?;
        Ok(Some(creds))
    }

    /// Save credentials to a JSON file with restricted permissions (mode 0600).
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, &json)
            .with_context(|| format!("failed to write credentials to {}", path.display()))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(path, perms)?;
        }

        Ok(())
    }
}

/// Resolve a bearer token using the priority chain:
/// 1. `KUTL_TOKEN` environment variable
/// 2. Token from credentials file at `path`
/// 3. `None` (caller should fall back to DID auth)
pub fn resolve_token(path: Option<&Path>) -> Option<String> {
    // Priority 1: env var
    if let Ok(token) = std::env::var(TOKEN_ENV_VAR)
        && !token.is_empty()
    {
        return Some(token);
    }

    // Priority 2: credentials file
    if let Some(p) = path
        && let Ok(Some(creds)) = StoredCredentials::load(p)
    {
        return Some(creds.token);
    }

    None
}

/// Delete the credentials file.
pub fn delete_credentials(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("failed to delete {}", path.display())),
    }
}

/// Default credentials file path: `$KUTL_HOME/auth.json`.
pub fn default_credentials_path() -> Result<PathBuf> {
    Ok(crate::dirs::kutl_home()?.join("auth.json"))
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_save_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.json");

        let creds = StoredCredentials {
            token: "kutl_test123".into(),
            relay_url: "wss://relay.kutlhub.com/ws".into(),
            account_id: "acc_abc".into(),
            display_name: "Alice".into(),
        };
        creds.save(&path).unwrap();

        let loaded = StoredCredentials::load(&path).unwrap().unwrap();
        assert_eq!(loaded.token, "kutl_test123");
        assert_eq!(loaded.relay_url, "wss://relay.kutlhub.com/ws");
    }

    #[test]
    fn test_load_missing_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.json");
        assert!(StoredCredentials::load(&path).unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_resolve_env_var_overrides_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.json");

        let creds = StoredCredentials {
            token: "kutl_from_file".into(),
            relay_url: "wss://relay.kutlhub.com/ws".into(),
            account_id: "acc_abc".into(),
            display_name: "Alice".into(),
        };
        creds.save(&path).unwrap();

        // env var should win
        // SAFETY: test runs are single-threaded for env var mutation tests.
        unsafe { std::env::set_var("KUTL_TOKEN", "kutl_from_env") };
        let resolved = resolve_token(Some(&path));
        unsafe { std::env::remove_var("KUTL_TOKEN") };

        assert_eq!(resolved, Some("kutl_from_env".to_owned()));
    }

    #[test]
    #[serial]
    fn test_resolve_falls_back_to_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.json");

        let creds = StoredCredentials {
            token: "kutl_from_file".into(),
            relay_url: "wss://relay.kutlhub.com/ws".into(),
            account_id: "acc_abc".into(),
            display_name: "Alice".into(),
        };
        creds.save(&path).unwrap();

        // Make sure env var is not set
        // SAFETY: test runs are single-threaded for env var mutation tests.
        unsafe { std::env::remove_var("KUTL_TOKEN") };
        let resolved = resolve_token(Some(&path));
        assert_eq!(resolved, Some("kutl_from_file".to_owned()));
    }

    #[test]
    #[serial]
    fn test_resolve_returns_none_when_nothing() {
        // SAFETY: test runs are single-threaded for env var mutation tests.
        unsafe { std::env::remove_var("KUTL_TOKEN") };
        let resolved = resolve_token(None);
        assert!(resolved.is_none());
    }

    #[test]
    fn test_delete_credentials() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.json");

        let creds = StoredCredentials {
            token: "kutl_test".into(),
            relay_url: "wss://relay.kutlhub.com/ws".into(),
            account_id: "acc_abc".into(),
            display_name: "Alice".into(),
        };
        creds.save(&path).unwrap();
        assert!(path.exists());

        delete_credentials(&path).unwrap();
        assert!(!path.exists());
    }
}
