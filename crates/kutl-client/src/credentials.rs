//! Credential storage and resolution.
//!
//! Manages `$KUTL_HOME/auth.toml` for stored authentication tokens
//! and resolves credentials from environment or file. An `auth.json`
//! written before the TOML move still loads and is rewritten on first read
//! (see [`crate::text_file`]).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Environment variable for token override.
pub const TOKEN_ENV_VAR: &str = "KUTL_TOKEN";

/// Stored credentials from `$KUTL_HOME/auth.toml`.
#[derive(Debug, Serialize, Deserialize)]
pub struct StoredCredentials {
    /// Bearer token (`kutl_` prefix).
    pub token: String,
    /// Relay WebSocket URL.
    pub relay_url: String,
    /// Account ID on the relay.
    pub account_id: String,
    /// Human-readable display name.
    pub display_name: String,
}

impl StoredCredentials {
    /// Load credentials from `path` (a `.toml` path; its `.json` sibling is
    /// the never-expiring fallback). Returns `None` if neither file exists.
    pub fn load(path: &Path) -> Result<Option<Self>> {
        crate::text_file::load(path, crate::file_lock::write_atomic_secret)
            .with_context(|| format!("failed to read credentials from {}", path.display()))
    }

    /// Save credentials to a TOML file with restricted permissions (mode 0600).
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        crate::text_file::save(path, self, crate::file_lock::write_atomic_secret)
            .with_context(|| format!("failed to write credentials to {}", path.display()))
    }
}

/// Which slot supplied a resolved bearer token, so a relay that refuses one can
/// name the thing that has to change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenSource {
    /// The `KUTL_TOKEN` environment variable.
    Env,
    /// The credentials file at this path.
    File(PathBuf),
}

/// Walk the credential chain once: `KUTL_TOKEN`, then the credentials file at
/// `path`, then nothing.
///
/// `relay_url`, when supplied, additionally requires a stored file token to have
/// been minted by that relay. A token is only meaningful to the relay that
/// issued it, so presenting a stale one elsewhere converts a working did:key
/// identity into a refused handshake; a mismatch resolves to `None` and the
/// caller falls through to the challenge flow. The environment variable is an
/// explicit override and is always trusted, matched or not.
///
/// Every token decision goes through here — the relay-matched and
/// presence-only views below are the same walk read two ways.
fn resolve(relay_url: Option<&str>, path: Option<&Path>) -> Option<(String, TokenSource)> {
    if let Ok(token) = std::env::var(TOKEN_ENV_VAR)
        && !token.is_empty()
    {
        return Some((token, TokenSource::Env));
    }

    if let Some(p) = path
        && let Ok(Some(creds)) = StoredCredentials::load(p)
        && relay_url.is_none_or(|url| creds.relay_url == url)
    {
        return Some((creds.token, TokenSource::File(p.to_path_buf())));
    }

    None
}

/// Resolve a bearer token for `relay_ws_url` and report which slot supplied it.
///
/// The form every connecting caller wants: a stored token minted by a different
/// relay is skipped rather than presented, and the source rides along so a
/// refusal can be reported against the slot that actually holds the token.
pub fn resolve_token_for(relay_ws_url: &str, path: Option<&Path>) -> Option<(String, TokenSource)> {
    resolve(Some(relay_ws_url), path)
}

/// Resolve a bearer token without regard to which relay minted it.
///
/// For callers asking only whether the user holds credentials at all — the join
/// gate decides between prompting for sign-in and proceeding before any relay
/// endpoint is settled. Anything that is about to CONNECT wants
/// [`resolve_token_for`] instead.
pub fn resolve_token(path: Option<&Path>) -> Option<String> {
    resolve(None, path).map(|(token, _)| token)
}

/// Explain a relay's refusal of the presented bearer token, naming the slot the
/// token came from and the one command that clears it.
///
/// The slot is re-derived here rather than threaded down from whoever resolved
/// the token: this is an error path, the derivation is the same shared walk, and
/// carrying the answer through every connecting call site would put a second
/// copy of "where do tokens come from" in the codebase — the drift this module
/// exists to prevent.
///
/// The slot matters because the REMEDY differs by slot, not for precision's own
/// sake: `kutl auth logout` clears the credentials file and does nothing at all
/// to `KUTL_TOKEN`, so a single generic line sends half of its readers in a
/// circle. The suggested `--relay` value is the URL that was just refused, which
/// is stored verbatim as the credential's `relay_url` — so a token stored by
/// following this line is one the next resolve will match rather than skip.
pub fn refused_token_remedy(relay_ws_url: &str) -> String {
    let source = default_credentials_path()
        .ok()
        .and_then(|path| resolve_token_for(relay_ws_url, Some(&path)))
        .map(|(_, source)| source);
    match source {
        Some(TokenSource::Env) => format!(
            "{relay_ws_url} refused the token in {TOKEN_ENV_VAR} — unset it to fall back to \
             this machine's did:key identity, or replace it with one this relay minted"
        ),
        Some(TokenSource::File(_)) => format!(
            "{relay_ws_url} refused the stored token — run `kutl auth logout` to fall back to \
             this machine's did:key identity, or `kutl auth login --relay {relay_ws_url}` to \
             store one this relay minted"
        ),
        // Nothing in either slot applies to this relay, so the refused bearer was
        // minted from the machine's own identity.
        None => format!(
            "{relay_ws_url} refused the bearer minted for this machine's did:key identity — \
             the identity must be authorized on the relay before this space can sync"
        ),
    }
}

/// Delete the credentials file, in both spellings, so a logout never leaves
/// a pre-move `auth.json` behind to be read back as the fallback.
pub fn delete_credentials(path: &Path) -> Result<()> {
    for p in [path.to_path_buf(), crate::text_file::json_sibling(path)] {
        match std::fs::remove_file(&p) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("failed to delete {}", p.display())),
        }
    }
    Ok(())
}

/// The credentials file under an explicit kutl home (`<home>/auth.toml`).
#[must_use]
pub fn default_credentials_path_in(home: &Path) -> PathBuf {
    home.join("auth.toml")
}

/// Default credentials file path: `$KUTL_HOME/auth.toml`.
pub fn default_credentials_path() -> Result<PathBuf> {
    Ok(default_credentials_path_in(&crate::dirs::kutl_home()?))
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_save_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.toml");

        let creds = StoredCredentials {
            token: "kutl_test123".into(),
            relay_url: "wss://relay.example.com/ws".into(),
            account_id: "acc_abc".into(),
            display_name: "Alice".into(),
        };
        creds.save(&path).unwrap();

        let loaded = StoredCredentials::load(&path).unwrap().unwrap();
        assert_eq!(loaded.token, "kutl_test123");
        assert_eq!(loaded.relay_url, "wss://relay.example.com/ws");
    }

    #[cfg(unix)]
    #[test]
    fn test_save_sets_restricted_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.toml");

        let creds = StoredCredentials {
            token: "kutl_secret".into(),
            relay_url: "wss://relay.example.com/ws".into(),
            account_id: "acc_abc".into(),
            display_name: "Alice".into(),
        };
        creds.save(&path).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "credentials file must be mode 0600");
    }

    /// A pre-move `auth.json` loads through the fallback, is rewritten as
    /// owner-only TOML, and the JSON is gone; the next load reads the TOML.
    #[test]
    fn test_json_credentials_load_and_are_rewritten() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.toml");
        let json_path = dir.path().join("auth.json");
        let creds = StoredCredentials {
            token: "kutl_json".into(),
            relay_url: "ws://relay.example/ws".into(),
            account_id: "acct-1".into(),
            display_name: "Json User".into(),
        };
        std::fs::write(&json_path, serde_json::to_string(&creds).unwrap()).unwrap();

        let loaded = StoredCredentials::load(&path).unwrap().unwrap();
        assert_eq!(loaded.token, "kutl_json");
        assert_eq!(loaded.account_id, "acct-1");
        assert!(path.exists(), "rewritten as toml");
        assert!(!json_path.exists(), "json original removed");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "the rewrite keeps the token owner-only");
        }
        let again = StoredCredentials::load(&path).unwrap().unwrap();
        assert_eq!(again.display_name, "Json User");
    }

    #[test]
    fn test_load_missing_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.toml");
        assert!(StoredCredentials::load(&path).unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_resolve_env_var_overrides_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.toml");

        let creds = StoredCredentials {
            token: "kutl_from_file".into(),
            relay_url: "wss://relay.example.com/ws".into(),
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
        let path = dir.path().join("auth.toml");

        let creds = StoredCredentials {
            token: "kutl_from_file".into(),
            relay_url: "wss://relay.example.com/ws".into(),
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

    /// Write a credentials file holding `token`, minted by `relay_url`.
    fn stored_at(dir: &TempDir, relay_url: &str, token: &str) -> PathBuf {
        let path = dir.path().join("auth.toml");
        StoredCredentials {
            token: token.into(),
            relay_url: relay_url.into(),
            account_id: "acc_abc".into(),
            display_name: "Alice".into(),
        }
        .save(&path)
        .unwrap();
        path
    }

    #[test]
    #[serial]
    fn test_resolve_for_relay_skips_token_minted_elsewhere() {
        let dir = TempDir::new().unwrap();
        let path = stored_at(&dir, "wss://hosted.example.com/ws", "kutl_hosted");

        // SAFETY: test runs are single-threaded for env var mutation tests.
        unsafe { std::env::remove_var("KUTL_TOKEN") };
        assert_eq!(
            resolve_token_for("wss://self-hosted.internal/ws", Some(&path)),
            None,
            "a token minted by another relay must not be presented"
        );
        // The relay-agnostic presence view still sees it.
        assert_eq!(resolve_token(Some(&path)), Some("kutl_hosted".to_owned()));
    }

    #[test]
    #[serial]
    fn test_resolve_for_relay_returns_matching_token_and_source() {
        let dir = TempDir::new().unwrap();
        let path = stored_at(&dir, "wss://relay.example.com/ws", "kutl_from_file");

        // SAFETY: test runs are single-threaded for env var mutation tests.
        unsafe { std::env::remove_var("KUTL_TOKEN") };
        assert_eq!(
            resolve_token_for("wss://relay.example.com/ws", Some(&path)),
            Some(("kutl_from_file".to_owned(), TokenSource::File(path.clone())))
        );
    }

    #[test]
    #[serial]
    fn test_resolve_for_relay_trusts_env_across_relays() {
        let dir = TempDir::new().unwrap();
        let path = stored_at(&dir, "wss://relay.example.com/ws", "kutl_from_file");

        // SAFETY: test runs are single-threaded for env var mutation tests.
        unsafe { std::env::set_var("KUTL_TOKEN", "kutl_from_env") };
        let resolved = resolve_token_for("wss://elsewhere.invalid/ws", Some(&path));
        unsafe { std::env::remove_var("KUTL_TOKEN") };

        assert_eq!(
            resolved,
            Some(("kutl_from_env".to_owned(), TokenSource::Env)),
            "an explicit override is trusted whichever relay is being contacted"
        );
    }

    #[test]
    #[serial]
    fn test_refused_token_remedy_names_the_slot_that_holds_the_token() {
        // An environment override: `logout` would not clear it, so the line must
        // not send the reader there.
        // SAFETY: env-var mutation tests run single-threaded (`#[serial]`).
        unsafe { std::env::set_var(TOKEN_ENV_VAR, "kutl_from_env") };
        let msg = refused_token_remedy("wss://relay.example.com/ws");
        unsafe { std::env::remove_var(TOKEN_ENV_VAR) };
        assert!(msg.contains(TOKEN_ENV_VAR), "{msg}");
        assert!(
            !msg.contains("kutl auth logout"),
            "logout does not clear an environment override: {msg}"
        );

        // Neither slot holds a token for this relay: the refused bearer was minted
        // from the local identity, and no credential command helps.
        let msg = refused_token_remedy("wss://relay.example.com/ws");
        assert!(msg.contains("did:key"), "{msg}");
        assert!(!msg.contains("kutl auth logout"), "{msg}");
    }

    #[test]
    fn test_delete_credentials() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("auth.toml");

        let creds = StoredCredentials {
            token: "kutl_test".into(),
            relay_url: "wss://relay.example.com/ws".into(),
            account_id: "acc_abc".into(),
            display_name: "Alice".into(),
        };
        creds.save(&path).unwrap();
        assert!(path.exists());

        delete_credentials(&path).unwrap();
        assert!(!path.exists());
    }
}
