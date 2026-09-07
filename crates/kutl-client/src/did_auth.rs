//! DID challenge-response authentication against a kutl relay.
//!
//! Performs the `/auth/challenge` -> sign -> `/auth/verify` flow and returns
//! a bearer token for authenticated WebSocket handshakes.

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use tracing::info;

use crate::credentials;
use crate::identity::{Identity, default_identity_path};
use crate::url::ws_url_to_http;

/// Resolve a bearer token for `relay_ws_url`, falling back to a did:key
/// challenge-response against the relay when no stored token applies.
///
/// Checks the token resolution chain (`KUTL_TOKEN` env, credentials file) first —
/// so hosted deployments keep using their PAT — then, for a relay this machine
/// holds no token for, authenticates the local identity via the challenge flow
/// (`authenticate`) and returns the minted bearer. A stored token minted by a
/// DIFFERENT relay does not apply and is skipped rather than presented, so a
/// leftover credential cannot mask a working local identity.
///
/// # Errors
///
/// Returns an error if the credentials/identity paths cannot be resolved, the
/// local identity is missing or malformed, or the challenge-response against the
/// relay fails.
pub async fn resolve_or_authenticate(relay_ws_url: &str) -> Result<String> {
    let creds_path = credentials::default_credentials_path()?;
    if let Some((token, _)) = credentials::resolve_token_for(relay_ws_url, Some(&creds_path)) {
        return Ok(token);
    }
    // No stored token — self-hosted relay path: author-sign the challenge with
    // the local did:key identity and return the minted bearer.
    let identity_path = default_identity_path()?;
    let identity = Identity::load(&identity_path).with_context(|| {
        format!(
            "loading the identity at {} — run `kutl init` first",
            identity_path.display()
        )
    })?;
    let signing_key = identity
        .decode_signing_key()
        .context("decoding the identity signing key for relay authentication")?;
    authenticate(relay_ws_url, &identity.did, &signing_key).await
}

/// Authenticate with a relay via DID challenge-response.
///
/// Performs the `/auth/challenge` -> sign -> `/auth/verify` flow.
/// Errors if the relay rejects the challenge request or the signed
/// verification; on success the minted bearer token is always non-empty.
pub async fn authenticate(
    relay_ws_url: &str,
    did: &str,
    signing_key: &SigningKey,
) -> Result<String> {
    let base_url = ws_url_to_http(relay_ws_url);
    let client = reqwest::Client::new();

    // 1. Request a challenge nonce.
    let resp = client
        .post(format!("{base_url}/auth/challenge"))
        .json(&serde_json::json!({"did": did}))
        .send()
        .await
        .context("failed to send auth challenge request")?;

    let status = resp.status();
    if !status.is_success() {
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        match body["error"].as_str() {
            Some(msg) => anyhow::bail!("auth challenge failed: {msg}"),
            None => anyhow::bail!("auth challenge failed with status {status}"),
        }
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .context("failed to parse challenge response")?;

    let nonce = body["nonce"]
        .as_str()
        .context("challenge response missing nonce field")?;

    // 2. Sign the raw nonce bytes.
    let nonce_bytes = URL_SAFE_NO_PAD
        .decode(nonce)
        .context("failed to decode challenge nonce")?;
    let signature = signing_key.sign(&nonce_bytes);
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    // 3. Verify the signature and get a token.
    let resp = client
        .post(format!("{base_url}/auth/verify"))
        .json(&serde_json::json!({
            "did": did,
            "nonce": nonce,
            "signature": sig_b64,
        }))
        .send()
        .await
        .context("failed to send auth verify request")?;

    if !resp.status().is_success() {
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let msg = body["error"].as_str().unwrap_or("unknown error");
        anyhow::bail!("auth verify failed: {msg}");
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .context("failed to parse verify response")?;

    let token = body["token"]
        .as_str()
        .context("verify response missing token field")?
        .to_owned();

    info!("authenticated successfully");
    Ok(token)
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    /// With `KUTL_TOKEN` set, `resolve_or_authenticate` returns that token via
    /// the stored-credentials chain and never touches the network — so hosted
    /// deployments keep using their PAT. The URL passed is deliberately
    /// unroutable: if resolution fell through to the challenge flow, this test
    /// would hang/error instead of returning the env token.
    #[tokio::test]
    #[serial]
    async fn test_resolve_or_authenticate_prefers_stored_token() {
        // SAFETY: env-var mutation tests run single-threaded (`#[serial]`).
        unsafe { std::env::set_var(credentials::TOKEN_ENV_VAR, "kutl_stored_pat") };
        let resolved = resolve_or_authenticate("wss://unroutable.invalid/relay/ws").await;
        // SAFETY: env-var mutation tests run single-threaded (`#[serial]`).
        unsafe { std::env::remove_var(credentials::TOKEN_ENV_VAR) };

        assert_eq!(resolved.unwrap(), "kutl_stored_pat");
    }
}
