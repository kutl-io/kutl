//! Kutl configuration directory resolution.
//!
//! Checks `KUTL_HOME` first, then falls back to `~/.kutl`. This allows
//! running multiple daemon instances with isolated config directories
//! (e.g., for the demo simulation suite) without overriding `HOME`.

use std::path::PathBuf;

use anyhow::{Context, Result};

/// Environment variable to override the kutl config directory.
const KUTL_HOME_ENV: &str = "KUTL_HOME";

/// Return the kutl config directory.
///
/// Precedence: `$KUTL_HOME` > `~/.kutl`.
pub fn kutl_home() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var(KUTL_HOME_ENV) {
        return Ok(PathBuf::from(dir));
    }
    let home = dirs::home_dir().context("could not determine home directory")?;
    Ok(home.join(".kutl"))
}

/// Return the keyfile path for a tool-held agent identity
/// (`$KUTL_HOME/agents/<name>.json`).
///
/// The `name` is validated to prevent path traversal: it must be non-empty and
/// contain only ASCII alphanumerics, `-`, or `_`. This rules out path
/// separators and `..`, so the returned path is always confined to the
/// `agents/` subdirectory.
pub fn agent_identity_path(name: &str) -> Result<PathBuf> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        anyhow::bail!("agent name must be alphanumeric/-/_");
    }
    Ok(kutl_home()?.join("agents").join(format!("{name}.json")))
}

#[cfg(test)]
mod tests {
    use crate::Identity;
    use serial_test::serial;
    use tempfile::TempDir;

    use super::*;

    #[test]
    #[serial]
    fn test_agent_identity_path_shape() {
        let dir = TempDir::new().unwrap();
        // SAFETY: env-mutating tests are serialized via `#[serial]`.
        unsafe { std::env::set_var(KUTL_HOME_ENV, dir.path()) };
        let path = agent_identity_path("claude-laptop").unwrap();
        unsafe { std::env::remove_var(KUTL_HOME_ENV) };

        assert_eq!(path, dir.path().join("agents").join("claude-laptop.json"));
    }

    #[test]
    #[serial]
    fn test_agent_keyfile_generate_save_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        // SAFETY: env-mutating tests are serialized via `#[serial]`.
        unsafe { std::env::set_var(KUTL_HOME_ENV, dir.path()) };
        let path = agent_identity_path("claude-laptop").unwrap();

        let id = Identity::generate();
        // The `agents/` directory does not exist yet; save must create it.
        id.save(&path).unwrap();
        let loaded = Identity::load(&path).unwrap();
        unsafe { std::env::remove_var(KUTL_HOME_ENV) };

        assert_eq!(id.did, loaded.did);
        assert_eq!(id.private_key, loaded.private_key);
        assert_eq!(
            id.decode_signing_key().unwrap().to_bytes(),
            loaded.decode_signing_key().unwrap().to_bytes(),
        );
    }

    #[test]
    fn test_agent_identity_path_rejects_traversal() {
        assert!(agent_identity_path("../evil").is_err());
        assert!(agent_identity_path("foo/bar").is_err());
        assert!(agent_identity_path("").is_err());
        assert!(agent_identity_path("ok_name-1").is_ok());
    }
}
