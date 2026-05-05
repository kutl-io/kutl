//! Integration tests for the device authorization flow HTTP endpoints.
//!
//! No Postgres required — device flow state lives in-memory (`AuthStore`).
//! Runs unconditionally in CI.

mod common;

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{decode_envelope, handshake_envelope_with_token};
use kutl_proto::sync::sync_envelope::Payload;
use kutl_relay::config::{DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS, RelayConfig};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Start a relay with `require_auth: true` on a random port.
/// Returns `(addr, _keys_file)` — the keys file must be kept alive.
async fn start_relay_with_auth() -> (String, tempfile::NamedTempFile) {
    use std::io::Write;
    let keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(&keys_file, "# device-flow test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        host: "127.0.0.1".into(),
        port: 0,
        relay_name: "test-relay-device".into(),
        require_auth: true,
        database_url: None,
        outbound_capacity: 64,
        data_dir: None,
        external_url: None,
        ux_url: None,
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
        snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys_file)
}

/// Perform the full DID auth flow (challenge + sign + verify) and return a bearer token.
async fn authenticate(addr: &str, did: &str, signing_key: &SigningKey) -> String {
    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/challenge"))
        .json(&serde_json::json!({"did": did}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let nonce = resp["nonce"].as_str().unwrap();
    let nonce_bytes = URL_SAFE_NO_PAD.decode(nonce).unwrap();
    let signature = signing_key.sign(&nonce_bytes);
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/verify"))
        .json(&serde_json::json!({
            "did": did,
            "nonce": nonce,
            "signature": sig_b64,
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    resp["token"].as_str().unwrap().to_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full device flow lifecycle: initiate → authorize → poll → get token.
#[tokio::test]
async fn test_device_flow_lifecycle() {
    let (addr, _keys) = start_relay_with_auth().await;
    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    // 1. Initiate device flow.
    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/device"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let device_code = resp["device_code"].as_str().unwrap();
    let user_code = resp["user_code"].as_str().unwrap();
    assert!(!device_code.is_empty(), "device_code should not be empty");
    assert_eq!(user_code.len(), 9, "user_code should be XXXX-XXXX format");
    assert_eq!(user_code.as_bytes()[4], b'-');

    // 2. Authenticate a user via DID to get a bearer token for authorization.
    let (did, signing_key) = common::test_keypair();
    let bearer_token = authenticate(&addr, &did, &signing_key).await;

    // 3. Authorize the device request (simulating UX server call).
    let authorize_resp = client
        .post(format!("{base_url}/auth/device/authorize"))
        .header("Authorization", format!("Bearer {bearer_token}"))
        .json(&serde_json::json!({
            "user_code": user_code,
            "token": "kutl_device_test_token",
            "account_id": "test-account-123",
            "display_name": "Test User",
        }))
        .send()
        .await
        .unwrap();
    assert!(
        authorize_resp.status().is_success(),
        "authorize should succeed: {}",
        authorize_resp.status()
    );

    // 4. Poll for the token — should now succeed.
    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/device/token"))
        .json(&serde_json::json!({"device_code": device_code}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["token"].as_str().unwrap(), "kutl_device_test_token");
    assert_eq!(resp["account_id"].as_str().unwrap(), "test-account-123");
    assert_eq!(resp["display_name"].as_str().unwrap(), "Test User");
}

/// Polling immediately after initiation should return 428 (authorization pending).
#[tokio::test]
async fn test_device_flow_pending_returns_precondition_required() {
    let (addr, _keys) = start_relay_with_auth().await;
    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/device"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let device_code = resp["device_code"].as_str().unwrap();

    let poll_resp = client
        .post(format!("{base_url}/auth/device/token"))
        .json(&serde_json::json!({"device_code": device_code}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        poll_resp.status().as_u16(),
        428,
        "pending device should return 428 Precondition Required"
    );
}

/// Polling with a bogus device code should return 404.
#[tokio::test]
async fn test_device_flow_invalid_device_code() {
    let (addr, _keys) = start_relay_with_auth().await;
    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    let resp = client
        .post(format!("{base_url}/auth/device/token"))
        .json(&serde_json::json!({"device_code": "nonexistent-code"}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        404,
        "invalid device code should return 404"
    );
}

/// Authorizing without a bearer token should be rejected.
#[tokio::test]
async fn test_device_flow_authorize_requires_bearer() {
    let (addr, _keys) = start_relay_with_auth().await;
    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    // Initiate to get a valid user_code.
    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/device"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let user_code = resp["user_code"].as_str().unwrap();

    // Try to authorize without any Authorization header.
    let authorize_resp = client
        .post(format!("{base_url}/auth/device/authorize"))
        .json(&serde_json::json!({
            "user_code": user_code,
            "token": "kutl_injected",
            "account_id": "evil",
            "display_name": "Hacker",
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        authorize_resp.status().as_u16(),
        401,
        "authorize without bearer should return 401"
    );
}

/// A token obtained via the device flow should work for WebSocket handshake.
#[tokio::test]
async fn test_device_flow_token_works_for_ws() {
    let (addr, _keys) = start_relay_with_auth().await;
    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    // 1. Initiate device flow.
    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/device"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let device_code = resp["device_code"].as_str().unwrap().to_owned();
    let user_code = resp["user_code"].as_str().unwrap().to_owned();

    // 2. Get a bearer token via DID auth and authorize the device.
    let (did, signing_key) = common::test_keypair();
    let bearer_token = authenticate(&addr, &did, &signing_key).await;

    // The device flow issues its own token — use the bearer to authorize.
    // We'll set the issued token to the bearer token itself for simplicity.
    let authorize_resp = client
        .post(format!("{base_url}/auth/device/authorize"))
        .header("Authorization", format!("Bearer {bearer_token}"))
        .json(&serde_json::json!({
            "user_code": user_code,
            "token": bearer_token,
            "account_id": "ws-test-account",
            "display_name": "WS Test",
        }))
        .send()
        .await
        .unwrap();
    assert!(authorize_resp.status().is_success());

    // 3. Poll for the device token.
    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/device/token"))
        .json(&serde_json::json!({"device_code": device_code}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let device_token = resp["token"].as_str().unwrap();

    // 4. Connect via WebSocket using the device token.
    let ws_url = format!("ws://{addr}/ws");
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();

    let handshake = handshake_envelope_with_token("device-test-client", device_token, "WS Test");
    let bytes = kutl_proto::protocol::encode_envelope(&handshake);
    ws.send(tungstenite::Message::Binary(bytes.into()))
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timed out waiting for HandshakeAck")
        .expect("stream ended")
        .expect("ws error");

    match msg {
        tungstenite::Message::Binary(bytes) => {
            let envelope = decode_envelope(&bytes).unwrap();
            assert!(
                matches!(envelope.payload, Some(Payload::HandshakeAck(_))),
                "expected HandshakeAck, got {envelope:?}"
            );
        }
        other => panic!("expected binary frame, got {other:?}"),
    }
}
