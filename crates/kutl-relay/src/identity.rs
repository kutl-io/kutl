//! The relay's own `did:key` signing identity.
//!
//! Persisted as `identity.json` (mode 0600) in either the relay's `data_dir` or
//! a dedicated `identity_dir`. Used to attest records the relay ingests and to
//! sign marker-materialized records as `MATERIALIZER`.
//!
//! **Two loaders, and the choice is not stylistic.** [`RelayIdentity::load`] is
//! strict (absent is fatal) and backs `identity_dir`; `load_or_generate` is
//! permissive and backs `data_dir`. The split lets a host
//! relay hold an identity without also acquiring a segment store, and keeps
//! a fleet sharing one provisioned key from silently fanning out into one
//! DID per replica. `resolve_relay_identity` ranks them.
//!
//! Still optional by design: standalone test and sim actors run identity-less,
//! as does a relay whose operator configured neither key — every attestation
//! site is `Option`-gated. The OSS binary always has a data dir.

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};

use crate::identity_error::{IdentityError, Result};

/// Filename for the relay identity inside the data dir.
pub const IDENTITY_FILE_NAME: &str = "identity.json";

/// Mode for the identity file: owner read/write only.
#[cfg(unix)]
const IDENTITY_FILE_MODE: u32 = 0o600;

/// On-disk representation of the relay signing identity (mirrors
/// `kutl-client`'s `Identity` format so the two are conceptually identical).
///
/// Deliberately does not derive `Debug`: it holds the base64url private key,
/// and a `Debug` impl is a latent secret-exposure risk if its scope widens.
#[derive(Serialize, Deserialize)]
struct PersistedIdentity {
    /// `did:key:z6Mk…` identifier.
    did: String,
    /// Base64url-encoded Ed25519 private key (32 bytes).
    private_key: String,
    /// RFC 3339 creation timestamp.
    created_at: String,
}

/// A loaded relay signing identity.
///
/// Holds the `did:key` identifier and the corresponding Ed25519 signing key.
/// The private key is never logged; only the DID is.
pub struct RelayIdentity {
    did: String,
    signing_key: SigningKey,
}

impl RelayIdentity {
    /// The relay's `did:key` identifier.
    pub fn did(&self) -> &str {
        &self.did
    }

    /// The Ed25519 signing key (private — never logged).
    pub fn signing_key(&self) -> &SigningKey {
        &self.signing_key
    }

    /// Load the identity from `<data_dir>/identity.json`, generating and
    /// persisting a fresh one (mode 0600) on first start.
    ///
    /// Three cases, kept crisply distinct:
    /// - **absent** (`identity.json` does not exist): the normal first-run path
    ///   — generate a fresh identity and persist it atomically. NOT an error.
    /// - **present and valid**: load and return it.
    /// - **present but malformed** (unreadable, unparseable, undecodable key, or
    ///   a stored-DID/key mismatch): corruption — return an error. The caller
    ///   MUST abort startup, NOT regenerate (a fresh identity would silently
    ///   flip the relay DID, breaking verification of prior-run signatures) and
    ///   NOT degrade to a storeless/tier-3 relay silently.
    ///
    /// Uses `open`-then-read (not `.exists()` then read) to close the TOCTOU
    /// window and so that a genuinely-absent file is distinguished from a
    /// present-but-unreadable one at the OS level.
    ///
    /// # Errors
    ///
    /// Returns an error only when the file is PRESENT but malformed, or when a
    /// fresh identity cannot be persisted. An absent file is not an error.
    pub fn load_or_generate(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(IDENTITY_FILE_NAME);
        match std::fs::read_to_string(&path) {
            Ok(data) => Self::decode(&path, &data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Absent: normal first start — generate and persist.
                let identity = Self::generate();
                identity.save(data_dir)?;
                Ok(identity)
            }
            // Present but unreadable (permissions, IO error): corruption from the
            // caller's point of view — abort rather than silently regenerate.
            Err(e) => Err(IdentityError::Read(path.display().to_string(), e)),
        }
    }

    /// Load the identity from `<identity_dir>/identity.json`, treating an
    /// absent file as a **fatal error** rather than a first start.
    ///
    /// The counterpart to [`Self::load_or_generate`], and the difference is
    /// deliberate: it turns on whether the operator asked for a *specific*
    /// identity or merely for *an* identity.
    ///
    /// - `load_or_generate` backs `data_dir` — a self-hoster's own disk, one
    ///   process, where "no identity yet" means first start and minting one is
    ///   exactly right.
    /// - `load` backs an explicitly configured identity directory — which an
    ///   operator sets only when they have provisioned a key and want *that*
    ///   one. There, an absent file means the secret failed to mount, and
    ///   generating would be the worst available answer: every replica would
    ///   mint its **own** DID, so one relay URL would present N identities to a
    ///   single-valued client pin, and clients would see the
    ///   relay's identity change on every rollout. Failing to boot is loud and
    ///   recoverable; silently diverging replicas is neither.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::Absent`] when the file does not exist, and the
    /// same corruption errors as [`Self::load_or_generate`] when it does but is
    /// malformed. Every case is fatal to the caller.
    pub fn load(identity_dir: &Path) -> Result<Self> {
        let path = identity_dir.join(IDENTITY_FILE_NAME);
        match std::fs::read_to_string(&path) {
            Ok(data) => Self::decode(&path, &data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(IdentityError::Absent(path.display().to_string()))
            }
            Err(e) => Err(IdentityError::Read(path.display().to_string(), e)),
        }
    }

    /// Generate a fresh identity using OS-sourced randomness.
    ///
    /// Fills 32 random bytes via `getrandom` and constructs the signing key,
    /// avoiding a `rand_core` version mismatch between `ed25519-dalek` (which
    /// uses `rand_core 0.6`) and the relay's `rand 0.10` dependency.
    fn generate() -> Self {
        let mut key_bytes = [0u8; 32];
        getrandom::fill(&mut key_bytes).expect("os rng must be available");
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let did = kutl_signals::did_key_encode(&signing_key.verifying_key());
        Self { did, signing_key }
    }

    /// Decode an identity from the already-read file contents `data`.
    ///
    /// `path` is used only for error context. Any failure here means the
    /// present file is malformed (parse error, undecodable key, or a
    /// stored-DID/key mismatch) — a corruption signal the caller must treat as
    /// fatal, never as "regenerate".
    fn decode(path: &Path, data: &str) -> Result<Self> {
        let persisted: PersistedIdentity = serde_json::from_str(data)
            .map_err(|e| IdentityError::Parse(path.display().to_string(), e))?;
        let key_bytes = URL_SAFE_NO_PAD
            .decode(&persisted.private_key)
            .map_err(|e| IdentityError::DecodeKey(e.to_string()))?;
        let key_array: [u8; 32] = key_bytes
            .try_into()
            .map_err(|_| IdentityError::DecodeKey("private key is not 32 bytes".to_owned()))?;
        let signing_key = SigningKey::from_bytes(&key_array);
        // Verify the stored DID matches the key (detects file corruption).
        let derived_did = kutl_signals::did_key_encode(&signing_key.verifying_key());
        if derived_did != persisted.did {
            return Err(IdentityError::DidMismatch {
                stored: persisted.did,
                derived: derived_did,
            });
        }
        Ok(Self {
            did: persisted.did,
            signing_key,
        })
    }

    /// Persist the identity to `<data_dir>/identity.json` with mode 0600.
    fn save(&self, data_dir: &Path) -> Result<()> {
        std::fs::create_dir_all(data_dir)
            .map_err(|e| IdentityError::CreateDir(data_dir.display().to_string(), e))?;
        let path = data_dir.join(IDENTITY_FILE_NAME);
        let persisted = PersistedIdentity {
            did: self.did.clone(),
            private_key: URL_SAFE_NO_PAD.encode(self.signing_key.to_bytes()),
            created_at: jiff::Timestamp::now().to_string(),
        };
        let json = serde_json::to_vec_pretty(&persisted)
            .map_err(|e| IdentityError::Serialize(e.to_string()))?;
        write_secret_file(&path, &json)?;
        Ok(())
    }
}

/// Atomically write `bytes` to `path` with mode 0600 on Unix (owner
/// read/write only).
///
/// Mirrors the atomic `save_cursor` in `kutl-signals`: write to a sibling
/// `<path>.tmp` (created 0600 so the secret is never world-readable, even
/// transiently), `fsync` the tmp file so its contents reach disk, then
/// `rename` it over `path`. A crash or `ENOSPC` mid-write therefore leaves at
/// most a stale `.tmp` — never a truncated `identity.json` that a later boot
/// would mistake for a valid identity and silently degrade over. Unlike the
/// cursor (bookkeeping, no-fsync is fine), the identity is fsynced: a torn
/// first-start write is the exact silent DID-flip the design forbids.
///
/// The rename is atomic on the same filesystem; the tmp sits in the same
/// directory as `path` so the rename never crosses a filesystem boundary.
fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = tmp_sibling(path);

    let mut opts = OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(IDENTITY_FILE_MODE);
    }
    let mut file = opts
        .open(&tmp)
        .map_err(|e| IdentityError::Write(tmp.display().to_string(), e))?;
    file.write_all(bytes)
        .map_err(|e| IdentityError::Write(tmp.display().to_string(), e))?;
    // fsync the tmp file so its bytes are durable before the rename — the
    // rename must not publish a name that points at unwritten data.
    file.sync_all()
        .map_err(|e| IdentityError::Write(tmp.display().to_string(), e))?;
    drop(file);

    std::fs::rename(&tmp, path).map_err(|e| IdentityError::Write(path.display().to_string(), e))?;
    Ok(())
}

/// The staging path for the atomic write: `<path>.tmp` alongside `path` so the
/// rename stays within one filesystem.
fn tmp_sibling(path: &Path) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    std::path::PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// First load generates + persists; second load reads the same DID.
    #[test]
    fn test_load_or_generate_is_stable() {
        let dir = TempDir::new().unwrap();
        let a = RelayIdentity::load_or_generate(dir.path()).unwrap();
        let b = RelayIdentity::load_or_generate(dir.path()).unwrap();
        assert_eq!(a.did(), b.did());
        assert!(a.did().starts_with("did:key:z"));
    }

    /// The persisted key round-trips into a usable signing key whose DID
    /// matches the stored DID (via [`kutl_signals::did_key_encode`]).
    #[test]
    fn test_signing_key_matches_did() {
        let dir = TempDir::new().unwrap();
        let id = RelayIdentity::load_or_generate(dir.path()).unwrap();
        let derived = kutl_signals::did_key_encode(&id.signing_key().verifying_key());
        assert_eq!(derived, id.did());
    }

    #[cfg(unix)]
    #[test]
    fn test_identity_file_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        RelayIdentity::load_or_generate(dir.path()).unwrap();
        let mode = std::fs::metadata(dir.path().join(IDENTITY_FILE_NAME))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o600,
            "the relay private key must not be world-readable"
        );
    }

    /// An ABSENT identity file is the normal first-run path: generate a fresh,
    /// valid identity. This must NOT be treated as corruption.
    #[test]
    fn test_load_or_generate_absent_generates_valid_identity() {
        let dir = TempDir::new().unwrap();
        assert!(!dir.path().join(IDENTITY_FILE_NAME).exists());
        let id = RelayIdentity::load_or_generate(dir.path())
            .expect("absent identity must generate, not error");
        assert!(id.did().starts_with("did:key:z"));
        // The file must now exist and be a valid, re-loadable identity.
        assert!(dir.path().join(IDENTITY_FILE_NAME).exists());
    }

    /// The strict loader refuses to mint. This is the property that keeps a
    /// multi-replica deployment sharing ONE provisioned key: if an absent file
    /// generated here, each replica would boot with its own DID and one relay
    /// URL would present N identities to a single-valued client pin.
    #[test]
    fn test_load_absent_is_fatal_and_writes_nothing() {
        let dir = TempDir::new().unwrap();
        let Err(err) = RelayIdentity::load(dir.path()) else {
            panic!("an explicitly configured identity must never be generated")
        };
        assert!(
            !dir.path().join(IDENTITY_FILE_NAME).exists(),
            "the strict loader must not leave an identity behind"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("absent"),
            "error should say the file is absent, got: {msg}"
        );
    }

    /// The two loaders agree on a present, valid file — they differ only on
    /// absence. Written as a round trip so a future divergence in `decode`
    /// cannot pass unnoticed.
    #[test]
    fn test_load_and_load_or_generate_agree_on_a_present_identity() {
        let dir = TempDir::new().unwrap();
        let generated = RelayIdentity::load_or_generate(dir.path()).unwrap();
        let strict =
            RelayIdentity::load(dir.path()).expect("a present identity must load strictly");
        assert_eq!(generated.did(), strict.did());
    }

    /// Corruption is fatal to the strict loader too — absence is the only case
    /// the two loaders treat differently.
    #[test]
    fn test_load_malformed_file_aborts() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(IDENTITY_FILE_NAME), b"not json at all").unwrap();
        assert!(
            RelayIdentity::load(dir.path()).is_err(),
            "a malformed identity must abort the strict loader as well"
        );
    }

    /// A PRESENT-BUT-MALFORMED identity file (truncated / garbage JSON) is
    /// corruption: `load_or_generate` MUST return an error that aborts startup.
    /// It must NOT silently regenerate a new identity (that would flip the
    /// relay DID) and must NOT degrade to `None`.
    #[test]
    fn test_load_or_generate_malformed_file_aborts() {
        let dir = TempDir::new().unwrap();
        // A truncated JSON blob — the exact torn-write shape a crash mid-write
        // leaves behind.
        std::fs::write(
            dir.path().join(IDENTITY_FILE_NAME),
            b"{\"did\":\"did:key:zBroken\",\"private_key\":\"AAAA",
        )
        .unwrap();
        // `RelayIdentity` intentionally does not derive `Debug` (it holds the
        // private key), so match rather than `.expect_err()`.
        let Err(err) = RelayIdentity::load_or_generate(dir.path()) else {
            panic!("a malformed identity file must abort, not regenerate or degrade")
        };
        // The file must be left untouched — no silent regeneration over it.
        let after = std::fs::read(dir.path().join(IDENTITY_FILE_NAME)).unwrap();
        assert_eq!(
            after, b"{\"did\":\"did:key:zBroken\",\"private_key\":\"AAAA",
            "a malformed identity must NOT be overwritten with a fresh one"
        );
        // Sanity: the error mentions parsing/corruption context.
        let msg = err.to_string();
        assert!(
            msg.contains("parse") || msg.contains("corrupt") || msg.contains("did"),
            "error should describe the corruption, got: {msg}"
        );
    }

    /// A PRESENT file whose stored DID does not match the key (bit-rot within a
    /// syntactically-valid JSON) is also corruption: abort, do not regenerate.
    #[test]
    fn test_load_or_generate_did_mismatch_aborts() {
        let dir = TempDir::new().unwrap();
        // Syntactically valid JSON, but the DID does not derive from the key.
        let bogus = format!(
            "{{\"did\":\"did:key:zWrong\",\"private_key\":\"{}\",\"created_at\":\"2026-01-01T00:00:00Z\"}}",
            URL_SAFE_NO_PAD.encode([7u8; 32])
        );
        std::fs::write(dir.path().join(IDENTITY_FILE_NAME), bogus.as_bytes()).unwrap();
        assert!(
            RelayIdentity::load_or_generate(dir.path()).is_err(),
            "a DID/key mismatch must abort, not regenerate"
        );
    }

    /// After a successful write, no `identity.json.tmp` remains — the atomic
    /// write cleans up its staging file via rename.
    #[test]
    fn test_write_is_atomic_no_tmp_left_behind() {
        let dir = TempDir::new().unwrap();
        RelayIdentity::load_or_generate(dir.path()).unwrap();
        let tmp = dir.path().join(format!("{IDENTITY_FILE_NAME}.tmp"));
        assert!(
            !tmp.exists(),
            "atomic write must rename the tmp file away; none should remain"
        );
    }
}
