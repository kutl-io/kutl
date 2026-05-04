//! DID challenge-response authentication against a kutl relay.
//!
//! Performs the `/auth/challenge` -> sign -> `/auth/verify` flow and returns
//! a bearer token for authenticated WebSocket handshakes.

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use tracing::info;

use crate::url::ws_url_to_http;

/// Authenticate with a relay via DID challenge-response.
///
/// Performs the `/auth/challenge` -> sign -> `/auth/verify` flow.
/// Returns an empty string if the relay does not require auth.
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

    if resp.status() == reqwest::StatusCode::BAD_REQUEST {
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let msg = body["error"].as_str().unwrap_or("");
        if msg.contains("not required") {
            info!("relay does not require authentication");
            return Ok(String::new());
        }
        anyhow::bail!("auth challenge failed: {msg}");
    }

    if !resp.status().is_success() {
        anyhow::bail!("auth challenge failed with status {}", resp.status());
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
