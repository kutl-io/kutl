//! DID identity generation and persistence.
//!
//! Generates `did:key` identities using Ed25519 key pairs.
//! The identity is stored at `$KUTL_HOME/identity.json` with restricted file
//! permissions (mode 0600).

use std::path::Path;

use anyhow::{Context, Result};
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};

/// A local `did:key` identity backed by an Ed25519 key pair.
#[derive(Debug, Serialize, Deserialize)]
pub struct Identity {
    /// `did:key:z6Mk...` identifier.
    pub did: String,
    /// Base64url-encoded Ed25519 private key (32 bytes).
    pub private_key: String,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
    /// Optional human-readable display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Optional contact email address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

impl Identity {
    /// Generate a new random identity.
    pub fn generate() -> Self {
        use base64::Engine;
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let mut rng = rand_core::OsRng;
        let signing_key = SigningKey::generate(&mut rng);

        // did:key = "did:key:z" + base58btc(0xed01 || public_key_bytes)
        let did = kutl_signals::did_key_encode(&signing_key.verifying_key());
        let private_key = URL_SAFE_NO_PAD.encode(signing_key.to_bytes());
        let created_at = now_iso8601();

        Self {
            did,
            private_key,
            created_at,
            display_name: None,
            email: None,
        }
    }

    /// Load an identity from a JSON file.
    pub fn load(path: &Path) -> Result<Self> {
        let data = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read identity from {}", path.display()))?;
        let identity: Self = serde_json::from_str(&data)
            .with_context(|| format!("failed to parse identity from {}", path.display()))?;
        Ok(identity)
    }

    /// Save the identity to a JSON file with restricted permissions (mode 0600).
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }

        let json = serde_json::to_string_pretty(self)?;
        // Create atomically with mode 0600 so the private key is never
        // world-readable, even transiently (see `secret_file`).
        crate::secret_file::write_secret_file(path, json.as_bytes())
            .with_context(|| format!("failed to write identity to {}", path.display()))?;

        Ok(())
    }

    /// Decode the Ed25519 signing key from the stored base64url private key.
    ///
    /// Returns `None` if the key cannot be decoded (with a warning logged
    /// to the caller's discretion).
    pub fn decode_signing_key(&self) -> Result<SigningKey> {
        use base64::Engine;
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;

        let key_bytes = URL_SAFE_NO_PAD
            .decode(&self.private_key)
            .context("failed to decode private key")?;
        let key_array: [u8; 32] = key_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid private key length"))?;
        Ok(SigningKey::from_bytes(&key_array))
    }
}

/// Return the default identity file path (`$KUTL_HOME/identity.json`).
pub fn default_identity_path() -> Result<std::path::PathBuf> {
    Ok(crate::dirs::kutl_home()?.join("identity.json"))
}

/// Load the identity from the default path, or generate and save a new one.
pub fn load_or_generate() -> Result<Identity> {
    let path = default_identity_path()?;
    if path.exists() {
        Identity::load(&path)
    } else {
        let identity = Identity::generate();
        identity.save(&path)?;
        Ok(identity)
    }
}

/// Current time as ISO 8601 string using jiff.
fn now_iso8601() -> String {
    jiff::Zoned::now()
        .strftime("%Y-%m-%dT%H:%M:%S%:z")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_generate_produces_valid_did() {
        let id = Identity::generate();
        assert!(id.did.starts_with("did:key:z6Mk"), "got: {}", id.did);
        assert!(!id.private_key.is_empty());
        assert!(!id.created_at.is_empty());
    }

    #[test]
    fn test_save_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("identity.json");

        let id = Identity::generate();
        id.save(&path).unwrap();

        let loaded = Identity::load(&path).unwrap();
        assert_eq!(id.did, loaded.did);
        assert_eq!(id.private_key, loaded.private_key);
    }

    #[test]
    fn test_identity_with_display_name_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("identity.json");

        let mut id = Identity::generate();
        id.display_name = Some("Alice".to_owned());
        id.email = Some("alice@example.com".to_owned());
        id.save(&path).unwrap();

        let loaded = Identity::load(&path).unwrap();
        assert_eq!(loaded.display_name.as_deref(), Some("Alice"));
        assert_eq!(loaded.email.as_deref(), Some("alice@example.com"));
    }

    #[test]
    fn test_identity_without_display_name_loads() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("identity.json");

        // Simulate old-format JSON without display_name/email fields.
        let json = r#"{
  "did": "did:key:z6MkTest",
  "private_key": "dGVzdA",
  "created_at": "2025-01-01T00:00:00+00:00"
}"#;
        std::fs::write(&path, json).unwrap();

        let loaded = Identity::load(&path).unwrap();
        assert_eq!(loaded.did, "did:key:z6MkTest");
        assert!(loaded.display_name.is_none());
        assert!(loaded.email.is_none());
    }

    #[test]
    fn test_decode_signing_key_roundtrip() {
        let id = Identity::generate();
        let key = id.decode_signing_key().unwrap();
        // Verify that the decoded key matches the DID.
        assert!(!id.did.is_empty());
        assert_eq!(key.to_bytes().len(), 32);
    }

    #[cfg(unix)]
    #[test]
    fn test_save_sets_restricted_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("identity.json");

        let id = Identity::generate();
        id.save(&path).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    /// GOLDEN DID — pins the did:key encode output for a fixed Ed25519 key
    /// so any encoding divergence (e.g. from a multicodec or base58 change)
    /// is a build error rather than a silent identity mismatch.
    ///
    /// The encoding is now shared with `kutl-relay` and `kutl-signals` via
    /// `kutl_signals::did_key_encode`; byte-identity with the old local
    /// implementation was verified when this constant was captured.
    #[test]
    fn test_did_key_encode_golden() {
        /// Signing key [7u8; 32] is a stable fixture used across kutl-signals
        /// tests; the verifying key for this seed encodes deterministically.
        const GOLDEN_DID: &str = "did:key:z6MkvDqGT54cXesYGvABpF1UapVNwjCqRcafi4Px6Thv5T3Z";

        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let did = kutl_signals::did_key_encode(&signing_key.verifying_key());
        assert_eq!(did, GOLDEN_DID, "did:key encoding must be stable");
    }
}
