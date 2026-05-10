//! DID challenge-response authentication.
//!
//! Implements the MVP auth flow from RFD 0013:
//! 1. Client requests a challenge for its DID
//! 2. Client signs the nonce with its Ed25519 private key
//! 3. Relay verifies the signature and issues a bearer token
//! 4. Client includes the token in the WebSocket handshake

use std::collections::HashMap;
use std::fmt::Write;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};
use tracing::{error, warn};

/// Token prefix for kutl bearer tokens.
const TOKEN_PREFIX: &str = "kutl_";

/// Challenge nonce size in bytes.
const NONCE_SIZE: usize = 32;

/// Token random part size in bytes.
const TOKEN_RANDOM_SIZE: usize = 32;

/// Challenge lifetime (5 minutes).
const CHALLENGE_LIFETIME_MS: i64 = kutl_core::duration_ms(kutl_core::SignedDuration::from_mins(5));

/// Token lifetime (30 days).
const TOKEN_LIFETIME_MS: i64 =
    kutl_core::duration_ms(kutl_core::SignedDuration::from_hours(30 * 24));

/// Multicodec prefix for Ed25519 public keys (varint 0xed = 237, type 0x01).
///
/// See <https://github.com/multiformats/multicodec/blob/master/table.csv>.
const ED25519_MULTICODEC: [u8; 2] = [0xed, 0x01];

/// Ed25519 public key size in bytes.
const ED25519_PUB_KEY_LEN: usize = 32;

/// Device authorization request lifetime (15 minutes).
const DEVICE_REQUEST_LIFETIME_MS: i64 =
    kutl_core::duration_ms(kutl_core::SignedDuration::from_mins(15));

/// Number of characters in the user-facing device code (before hyphen formatting).
const USER_CODE_LENGTH: usize = 8;

/// Alphabet for generating user codes (uppercase alphanumeric, no ambiguous chars).
const USER_CODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

/// Compute the SHA-256 hex digest of a raw token string.
pub fn token_hash(token: &str) -> String {
    let hash = Sha256::digest(token.as_bytes());
    let mut hex = String::with_capacity(hash.len() * 2);
    for byte in hash {
        // write! to a String is infallible.
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// In-memory auth state, owned by the Relay actor.
#[derive(Default)]
pub struct AuthStore {
    /// Pending challenges: nonce (base64url) → challenge data.
    challenges: HashMap<String, PendingChallenge>,
    /// Active tokens: token string → session data.
    tokens: HashMap<String, TokenRecord>,
    /// Pending device authorization requests, keyed by `device_code`.
    device_requests: HashMap<String, DeviceRequest>,
}

struct PendingChallenge {
    did: String,
    nonce_bytes: Vec<u8>,
    expires_at: i64,
}

struct TokenRecord {
    did: String,
    expires_at: i64,
}

/// Pending device authorization request.
struct DeviceRequest {
    /// Code displayed to user (e.g. "ABCD-1234").
    user_code: String,
    /// Expiry timestamp (ms).
    expires_at: i64,
    /// Set once user authorizes: the bearer token.
    authorized_token: Option<String>,
    /// Set once user authorizes: account info.
    authorized_account_id: Option<String>,
    /// Set once user authorizes: display name.
    authorized_display_name: Option<String>,
}

/// Successful response from polling a device authorization request.
pub struct DeviceTokenResponse {
    /// Bearer token to use for relay authentication.
    pub token: String,
    /// Account ID of the authorized user.
    pub account_id: String,
    /// Display name of the authorized user.
    pub display_name: String,
}

/// Auth operation errors.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// The relay does not require authentication.
    #[error("authentication not required")]
    AuthNotRequired,
    /// The DID string could not be parsed as a `did:key`.
    #[error("invalid DID: {0}")]
    InvalidDid(String),
    /// No pending challenge found for this nonce.
    #[error("challenge not found")]
    ChallengeNotFound,
    /// The challenge has expired.
    #[error("challenge expired")]
    ChallengeExpired,
    /// The signature did not verify against the DID's public key.
    #[error("invalid signature")]
    InvalidSignature,
    /// The signature bytes could not be decoded.
    #[error("malformed signature: {0}")]
    MalformedSignature(String),
    /// The nonce bytes could not be decoded.
    #[error("malformed nonce: {0}")]
    MalformedNonce(String),
    /// The bearer token is not recognized.
    #[error("token not found")]
    TokenNotFound,
    /// The bearer token has expired.
    #[error("token expired")]
    TokenExpired,
    /// The device authorization is still pending user approval.
    #[error("authorization pending")]
    AuthorizationPending,
    /// The device code has expired.
    #[error("device code expired")]
    DeviceCodeExpired,
    /// The device code is not recognized.
    #[error("invalid device code")]
    InvalidDeviceCode,
    /// The user code is not recognized.
    #[error("invalid user code")]
    InvalidUserCode,
}

impl AuthStore {
    /// Create a new empty auth store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a challenge for the given DID.
    ///
    /// Returns `(nonce_base64url, expires_at_millis)`.
    /// Fails if the DID is not a valid `did:key`.
    pub fn create_challenge(&mut self, did: &str, now_ms: i64) -> Result<(String, i64), AuthError> {
        // Validate DID format early to reject garbage before allocating state.
        parse_did_key(did)?;

        // Purge expired entries on each request to prevent unbounded growth.
        self.purge_expired(now_ms);

        let nonce_bytes: Vec<u8> = (0..NONCE_SIZE).map(|_| rand::random::<u8>()).collect();
        let nonce_b64 = URL_SAFE_NO_PAD.encode(&nonce_bytes);
        let expires_at = now_ms + CHALLENGE_LIFETIME_MS;

        self.challenges.insert(
            nonce_b64.clone(),
            PendingChallenge {
                did: did.to_owned(),
                nonce_bytes,
                expires_at,
            },
        );

        Ok((nonce_b64, expires_at))
    }

    /// Verify a challenge-response signature.
    ///
    /// On success, consumes the challenge and returns `(token, expires_at_millis)`.
    pub fn verify_challenge(
        &mut self,
        did: &str,
        nonce_b64: &str,
        signature_b64: &str,
        now_ms: i64,
    ) -> Result<(String, i64), AuthError> {
        // Look up the challenge WITHOUT removing it yet. Verify the DID
        // matches before consuming the nonce — otherwise an attacker who
        // observes a nonce can race the legitimate client and burn it
        // (targeted DoS). The challenge is removed only after the DID
        // and expiry checks pass.
        let challenge = self
            .challenges
            .get(nonce_b64)
            .ok_or(AuthError::ChallengeNotFound)?;

        if now_ms > challenge.expires_at {
            self.challenges.remove(nonce_b64);
            return Err(AuthError::ChallengeExpired);
        }

        if challenge.did != did {
            return Err(AuthError::ChallengeNotFound);
        }

        // DID + expiry verified — now consume the nonce (single-use).
        let challenge = self
            .challenges
            .remove(nonce_b64)
            .expect("challenge verified above");

        // Parse the DID to get the public key.
        let verifying_key = parse_did_key(did)?;

        // Decode the signature.
        let sig_bytes = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|e| AuthError::MalformedSignature(e.to_string()))?;
        let signature = Signature::from_slice(&sig_bytes)
            .map_err(|e| AuthError::MalformedSignature(e.to_string()))?;

        // Verify: the client signs the raw nonce bytes.
        verifying_key
            .verify(&challenge.nonce_bytes, &signature)
            .map_err(|_| AuthError::InvalidSignature)?;

        // Issue a bearer token.
        let token = generate_token();
        let expires_at = now_ms + TOKEN_LIFETIME_MS;

        self.tokens.insert(
            token.clone(),
            TokenRecord {
                did: did.to_owned(),
                expires_at,
            },
        );

        Ok((token, expires_at))
    }

    /// Validate a bearer token.
    ///
    /// Returns the authenticated DID on success.
    pub fn validate_token(&self, token: &str, now_ms: i64) -> Result<String, AuthError> {
        let record = self.tokens.get(token).ok_or(AuthError::TokenNotFound)?;

        if now_ms > record.expires_at {
            return Err(AuthError::TokenExpired);
        }

        Ok(record.did.clone())
    }

    /// Create a device authorization request.
    ///
    /// Returns `(device_code, user_code, expires_at)`. The `device_code` is an
    /// opaque UUID for CLI polling; the `user_code` is a human-readable code
    /// (formatted as `XXXX-XXXX`) displayed to the user.
    pub fn create_device_request(&mut self, now_ms: i64) -> (String, String, i64) {
        self.purge_expired(now_ms);

        let device_code = uuid::Uuid::new_v4().to_string();
        let user_code = generate_user_code();
        let expires_at = now_ms + DEVICE_REQUEST_LIFETIME_MS;

        self.device_requests.insert(
            device_code.clone(),
            DeviceRequest {
                user_code: user_code.clone(),
                expires_at,
                authorized_token: None,
                authorized_account_id: None,
                authorized_display_name: None,
            },
        );

        (device_code, user_code, expires_at)
    }

    /// Authorize a pending device request by user code.
    ///
    /// Called by the UX server after the user approves the device login.
    /// Sets the authorized token and account info on the matching request.
    pub fn authorize_device(
        &mut self,
        user_code: &str,
        token: String,
        account_id: String,
        display_name: String,
        now_ms: i64,
    ) -> Result<(), AuthError> {
        let request = self
            .device_requests
            .values_mut()
            .find(|r| r.user_code == user_code)
            .ok_or(AuthError::InvalidUserCode)?;

        if now_ms > request.expires_at {
            return Err(AuthError::DeviceCodeExpired);
        }

        request.authorized_token = Some(token);
        request.authorized_account_id = Some(account_id);
        request.authorized_display_name = Some(display_name);

        Ok(())
    }

    /// Poll a device authorization request.
    ///
    /// Returns the token and account info if the user has authorized,
    /// `AuthorizationPending` if not yet approved, or `DeviceCodeExpired`
    /// if the request has expired.
    pub fn poll_device(
        &mut self,
        device_code: &str,
        now_ms: i64,
    ) -> Result<DeviceTokenResponse, AuthError> {
        let request = self
            .device_requests
            .get(device_code)
            .ok_or(AuthError::InvalidDeviceCode)?;

        if now_ms > request.expires_at {
            self.device_requests.remove(device_code);
            return Err(AuthError::DeviceCodeExpired);
        }

        if let (Some(token), Some(account_id), Some(display_name)) = (
            &request.authorized_token,
            &request.authorized_account_id,
            &request.authorized_display_name,
        ) {
            let response = DeviceTokenResponse {
                token: token.clone(),
                account_id: account_id.clone(),
                display_name: display_name.clone(),
            };
            // Consume the request (single-use).
            self.device_requests.remove(device_code);
            Ok(response)
        } else {
            Err(AuthError::AuthorizationPending)
        }
    }

    /// Remove expired challenges, tokens, and device requests.
    pub fn purge_expired(&mut self, now_ms: i64) {
        self.challenges.retain(|_, c| c.expires_at > now_ms);
        self.tokens.retain(|_, t| t.expires_at > now_ms);
        self.device_requests.retain(|_, d| d.expires_at > now_ms);
    }

    /// Verify a challenge-response and persist the session via backend if available.
    ///
    /// Delegates to [`verify_challenge`](Self::verify_challenge) for the core logic,
    /// then writes the token hash via the session backend when `sessions` is `Some`.
    pub async fn verify_challenge_with_backend(
        &mut self,
        did: &str,
        nonce_b64: &str,
        signature_b64: &str,
        now_ms: i64,
        sessions: Option<&dyn crate::session_backend::SessionBackend>,
    ) -> Result<(String, i64), AuthError> {
        let (token, expires_at) = self.verify_challenge(did, nonce_b64, signature_b64, now_ms)?;

        if let Some(sessions) = sessions {
            let hash = token_hash(&token);
            if let Err(e) = sessions.create(&hash, did, expires_at).await {
                error!(error = %e, "failed to persist session to database");
            }
        }

        Ok((token, expires_at))
    }

    /// Validate a bearer token, falling back to backends if not found in memory.
    ///
    /// Returns `(identity, pat_hash)` where `pat_hash` is `Some` when the
    /// token was validated via the PAT backend (so callers can enforce
    /// per-token space scoping via `PatBackend::check_scope`).
    pub async fn validate_token_with_backend(
        &self,
        token: &str,
        now_ms: i64,
        sessions: Option<&dyn crate::session_backend::SessionBackend>,
        pats: Option<&dyn crate::pat_backend::PatBackend>,
    ) -> Result<(String, Option<String>), AuthError> {
        // Fast path: in-memory lookup (DID challenge tokens).
        match self.validate_token(token, now_ms) {
            Ok(did) => return Ok((did, None)),
            Err(AuthError::TokenExpired) => return Err(AuthError::TokenExpired),
            Err(AuthError::TokenNotFound) => {}
            Err(e) => return Err(e),
        }

        let hash = token_hash(token);

        if let Some(sessions) = sessions {
            match sessions.validate(&hash, now_ms).await {
                Ok(Some(did)) => return Ok((did, None)),
                Ok(None) => {}
                Err(e) => {
                    warn!(error = %e, "session backend lookup failed");
                }
            }
        }

        if let Some(pats) = pats {
            match pats.validate(&hash).await {
                Ok(Some(identity)) => return Ok((identity, Some(hash))),
                Ok(None) => {}
                Err(e) => {
                    warn!(error = %e, "PAT backend lookup failed");
                }
            }
        }

        Err(AuthError::TokenNotFound)
    }
}

/// Parse a `did:key:z6Mk...` string into an Ed25519 `VerifyingKey`.
pub fn parse_did_key(did: &str) -> Result<VerifyingKey, AuthError> {
    let multibase = did
        .strip_prefix("did:key:z")
        .ok_or_else(|| AuthError::InvalidDid("expected did:key:z prefix".into()))?;

    let decoded = bs58::decode(multibase)
        .into_vec()
        .map_err(|e| AuthError::InvalidDid(format!("base58 decode failed: {e}")))?;

    if decoded.len() != ED25519_MULTICODEC.len() + ED25519_PUB_KEY_LEN {
        return Err(AuthError::InvalidDid(format!(
            "expected {} bytes, got {}",
            ED25519_MULTICODEC.len() + ED25519_PUB_KEY_LEN,
            decoded.len()
        )));
    }

    if decoded[..2] != ED25519_MULTICODEC {
        return Err(AuthError::InvalidDid(
            "unsupported multicodec prefix".into(),
        ));
    }

    let key_bytes: [u8; ED25519_PUB_KEY_LEN] = decoded[2..]
        .try_into()
        .map_err(|_| AuthError::InvalidDid("invalid key length".into()))?;

    VerifyingKey::from_bytes(&key_bytes)
        .map_err(|e| AuthError::InvalidDid(format!("invalid Ed25519 key: {e}")))
}

/// Generate a bearer token: `kutl_` + 32 random bytes base64url.
fn generate_token() -> String {
    let random_bytes: Vec<u8> = (0..TOKEN_RANDOM_SIZE)
        .map(|_| rand::random::<u8>())
        .collect();
    format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(&random_bytes))
}

/// Generate a user-facing device code formatted as `XXXX-XXXX`.
fn generate_user_code() -> String {
    let half = USER_CODE_LENGTH / 2;
    let chars: Vec<u8> = (0..USER_CODE_LENGTH)
        .map(|_| {
            let idx = rand::random_range(0..USER_CODE_ALPHABET.len());
            USER_CODE_ALPHABET[idx]
        })
        .collect();
    format!(
        "{}-{}",
        std::str::from_utf8(&chars[..half]).expect("user code alphabet is ASCII"),
        std::str::from_utf8(&chars[half..]).expect("user code alphabet is ASCII"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    /// Generate a test keypair and return `(did, signing_key)`.
    fn test_keypair() -> (String, SigningKey) {
        let secret: [u8; 32] = std::array::from_fn(|_| rand::random::<u8>());
        let signing_key = SigningKey::from_bytes(&secret);
        let public_key = signing_key.verifying_key();

        let mut multicodec_key = Vec::with_capacity(ED25519_MULTICODEC.len() + ED25519_PUB_KEY_LEN);
        multicodec_key.extend_from_slice(&ED25519_MULTICODEC);
        multicodec_key.extend_from_slice(public_key.as_bytes());
        let did = format!("did:key:z{}", bs58::encode(&multicodec_key).into_string());

        (did, signing_key)
    }

    #[test]
    fn test_parse_did_key_valid() {
        let (did, signing_key) = test_keypair();
        let verifying_key = parse_did_key(&did).unwrap();
        assert_eq!(verifying_key, signing_key.verifying_key());
    }

    #[test]
    fn test_parse_did_key_invalid_prefix() {
        assert!(parse_did_key("not-a-did").is_err());
        assert!(parse_did_key("did:key:z111").is_err());
    }

    #[test]
    fn test_create_challenge_returns_valid_nonce() {
        let (did, _) = test_keypair();
        let mut store = AuthStore::new();
        let (nonce, expires_at) = store.create_challenge(&did, 1000).unwrap();

        let decoded = URL_SAFE_NO_PAD.decode(&nonce).unwrap();
        assert_eq!(decoded.len(), NONCE_SIZE);
        assert_eq!(expires_at, 1000 + CHALLENGE_LIFETIME_MS);
    }

    #[test]
    fn test_create_challenge_rejects_invalid_did() {
        let mut store = AuthStore::new();
        let result = store.create_challenge("not-a-did", 1000);
        assert!(matches!(result, Err(AuthError::InvalidDid(_))));
    }

    #[test]
    fn test_verify_challenge_valid_signature() {
        let (did, signing_key) = test_keypair();
        let mut store = AuthStore::new();
        let now = 1000;

        let (nonce_b64, _) = store.create_challenge(&did, now).unwrap();

        let nonce_bytes = URL_SAFE_NO_PAD.decode(&nonce_b64).unwrap();
        let signature = signing_key.sign(&nonce_bytes);
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        let (token, expires_at) = store
            .verify_challenge(&did, &nonce_b64, &sig_b64, now)
            .unwrap();
        assert!(token.starts_with(TOKEN_PREFIX));
        assert_eq!(expires_at, now + TOKEN_LIFETIME_MS);
    }

    #[test]
    fn test_verify_challenge_expired() {
        let (did, signing_key) = test_keypair();
        let mut store = AuthStore::new();
        let now = 1000;

        let (nonce_b64, _) = store.create_challenge(&did, now).unwrap();

        let nonce_bytes = URL_SAFE_NO_PAD.decode(&nonce_b64).unwrap();
        let signature = signing_key.sign(&nonce_bytes);
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        // Verify after expiry.
        let late = now + CHALLENGE_LIFETIME_MS + 1;
        let result = store.verify_challenge(&did, &nonce_b64, &sig_b64, late);
        assert!(matches!(result, Err(AuthError::ChallengeExpired)));
    }

    #[test]
    fn test_verify_challenge_wrong_signature() {
        let (did, _) = test_keypair();
        let (_, wrong_key) = test_keypair();
        let mut store = AuthStore::new();
        let now = 1000;

        let (nonce_b64, _) = store.create_challenge(&did, now).unwrap();

        let nonce_bytes = URL_SAFE_NO_PAD.decode(&nonce_b64).unwrap();
        let signature = wrong_key.sign(&nonce_bytes);
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        let result = store.verify_challenge(&did, &nonce_b64, &sig_b64, now);
        assert!(matches!(result, Err(AuthError::InvalidSignature)));
    }

    #[test]
    fn test_verify_challenge_wrong_nonce() {
        let (did, _) = test_keypair();
        let mut store = AuthStore::new();
        let now = 1000;
        let _ = store.create_challenge(&did, now).unwrap();

        let result = store.verify_challenge(&did, "nonexistent_nonce", "sig", now);
        assert!(matches!(result, Err(AuthError::ChallengeNotFound)));
    }

    #[test]
    fn test_validate_token_valid() {
        let (did, signing_key) = test_keypair();
        let mut store = AuthStore::new();
        let now = 1000;

        let (nonce_b64, _) = store.create_challenge(&did, now).unwrap();
        let nonce_bytes = URL_SAFE_NO_PAD.decode(&nonce_b64).unwrap();
        let signature = signing_key.sign(&nonce_bytes);
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        let (token, _) = store
            .verify_challenge(&did, &nonce_b64, &sig_b64, now)
            .unwrap();

        let authenticated_did = store.validate_token(&token, now).unwrap();
        assert_eq!(authenticated_did, did);
    }

    #[test]
    fn test_validate_token_expired() {
        let (did, signing_key) = test_keypair();
        let mut store = AuthStore::new();
        let now = 1000;

        let (nonce_b64, _) = store.create_challenge(&did, now).unwrap();
        let nonce_bytes = URL_SAFE_NO_PAD.decode(&nonce_b64).unwrap();
        let signature = signing_key.sign(&nonce_bytes);
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        let (token, _) = store
            .verify_challenge(&did, &nonce_b64, &sig_b64, now)
            .unwrap();

        let late = now + TOKEN_LIFETIME_MS + 1;
        let result = store.validate_token(&token, late);
        assert!(matches!(result, Err(AuthError::TokenExpired)));
    }

    #[test]
    fn test_validate_token_unknown() {
        let store = AuthStore::new();
        let result = store.validate_token("kutl_unknown", 1000);
        assert!(matches!(result, Err(AuthError::TokenNotFound)));
    }

    #[test]
    fn test_purge_expired_removes_old_entries() {
        let (did_a, _) = test_keypair();
        let (did_b, _) = test_keypair();
        let mut store = AuthStore::new();

        // Create two challenges at different times.
        let _ = store.create_challenge(&did_a, 1000).unwrap();
        let _ = store.create_challenge(&did_b, 2000).unwrap();

        assert_eq!(store.challenges.len(), 2);

        // Purge at a time that expires the first but not the second.
        store.purge_expired(1000 + CHALLENGE_LIFETIME_MS + 1);
        assert_eq!(store.challenges.len(), 1);
    }

    #[test]
    fn test_challenge_is_single_use() {
        let (did, signing_key) = test_keypair();
        let mut store = AuthStore::new();
        let now = 1000;

        let (nonce_b64, _) = store.create_challenge(&did, now).unwrap();
        let nonce_bytes = URL_SAFE_NO_PAD.decode(&nonce_b64).unwrap();
        let signature = signing_key.sign(&nonce_bytes);
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        // First verify succeeds.
        store
            .verify_challenge(&did, &nonce_b64, &sig_b64, now)
            .unwrap();

        // Second verify fails (challenge consumed).
        let result = store.verify_challenge(&did, &nonce_b64, &sig_b64, now);
        assert!(matches!(result, Err(AuthError::ChallengeNotFound)));
    }

    #[test]
    fn test_device_flow_create_poll_authorize() {
        let mut store = AuthStore::new();
        let now = 1000;
        let (device_code, user_code, _) = store.create_device_request(now);

        // User code should be formatted as XXXX-XXXX.
        assert_eq!(user_code.len(), 9);
        assert_eq!(user_code.as_bytes()[4], b'-');

        // Poll before authorization — should be pending.
        let result = store.poll_device(&device_code, now);
        assert!(matches!(result, Err(AuthError::AuthorizationPending)));

        // Authorize.
        store
            .authorize_device(
                &user_code,
                "kutl_token123".into(),
                "acc1".into(),
                "Alice".into(),
                now,
            )
            .unwrap();

        // Poll after authorization — should return token.
        let result = store.poll_device(&device_code, now).unwrap();
        assert_eq!(result.token, "kutl_token123");
        assert_eq!(result.account_id, "acc1");
        assert_eq!(result.display_name, "Alice");
    }

    #[test]
    fn test_device_flow_expired() {
        let mut store = AuthStore::new();
        let now = 1000;
        let (device_code, _, _) = store.create_device_request(now);

        // Poll after expiry.
        let far_future = now + DEVICE_REQUEST_LIFETIME_MS + 1;
        let result = store.poll_device(&device_code, far_future);
        assert!(matches!(result, Err(AuthError::DeviceCodeExpired)));
    }

    #[test]
    fn test_device_flow_invalid_device_code() {
        let mut store = AuthStore::new();
        let result = store.poll_device("nonexistent", 1000);
        assert!(matches!(result, Err(AuthError::InvalidDeviceCode)));
    }

    #[test]
    fn test_device_flow_invalid_user_code() {
        let mut store = AuthStore::new();
        let result = store.authorize_device("ZZZZ-ZZZZ", "t".into(), "a".into(), "n".into(), 1000);
        assert!(matches!(result, Err(AuthError::InvalidUserCode)));
    }

    #[test]
    fn test_device_flow_poll_consumes_request() {
        let mut store = AuthStore::new();
        let now = 1000;
        let (device_code, user_code, _) = store.create_device_request(now);

        store
            .authorize_device(
                &user_code,
                "kutl_tok".into(),
                "acc".into(),
                "Bob".into(),
                now,
            )
            .unwrap();

        // First poll succeeds and consumes.
        store.poll_device(&device_code, now).unwrap();

        // Second poll fails — request consumed.
        let result = store.poll_device(&device_code, now);
        assert!(matches!(result, Err(AuthError::InvalidDeviceCode)));
    }

    #[test]
    fn test_device_flow_purge_expired() {
        let mut store = AuthStore::new();
        let _ = store.create_device_request(1000);
        let _ = store.create_device_request(2000);
        assert_eq!(store.device_requests.len(), 2);

        // Purge at a time that expires the first but not the second.
        store.purge_expired(1000 + DEVICE_REQUEST_LIFETIME_MS + 1);
        assert_eq!(store.device_requests.len(), 1);
    }
}
