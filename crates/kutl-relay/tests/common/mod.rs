//! Shared test helpers for kutl-relay integration tests.
//!
//! Not all helpers are used in every test file — `dead_code` is expected.

#![allow(dead_code)]

use ed25519_dalek::SigningKey;
use kutl_proto::sync::{SyncEnvelope, sync_envelope::Payload};
use kutl_relay::config::{
    DEFAULT_OUTBOUND_CAPACITY, DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS,
    RelayConfig,
};
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// Channels for a test connection. `data_rx` / `ctrl_rx` must remain
/// owned for the duration of a test so the relay-side senders stay
/// alive — some callers drain them, others only hold them as channel
/// anchors.
pub struct TestConn {
    pub conn_id: u64,
    pub data_rx: mpsc::Receiver<Vec<u8>>,
    pub ctrl_rx: mpsc::Receiver<Vec<u8>>,
}

/// Drain the `SubscribeStatus` envelope the relay sends on ctrl after
/// a subscribe (RFD 0066). Returns the payload for tests that want to
/// inspect it; existing tests that only care about subsequent ctrl
/// messages (broadcasts, etc.) can discard the return value.
///
/// Blocks briefly for the status to arrive. Intended to be called
/// right after a `RelayCommand::Subscribe` is dispatched.
pub async fn drain_subscribe_status(
    ctrl_rx: &mut mpsc::Receiver<Vec<u8>>,
) -> Option<kutl_proto::sync::SubscribeStatus> {
    let bytes = tokio::time::timeout(std::time::Duration::from_millis(200), ctrl_rx.recv())
        .await
        .ok()
        .flatten()?;
    let envelope = SyncEnvelope::decode(bytes.as_slice()).ok()?;
    match envelope.payload {
        Some(Payload::SubscribeStatus(s)) => Some(s),
        // Unexpected — the first ctrl message after subscribe should be
        // SubscribeStatus. If it's something else, surface via None so
        // the caller can decide how to handle it.
        _ => None,
    }
}

/// Ed25519 multicodec prefix (varint-encoded codec 0xed01).
pub const ED25519_MULTICODEC: [u8; 2] = [0xed, 0x01];

/// Generate an Ed25519 keypair and return `(did, signing_key)`.
pub fn test_keypair() -> (String, SigningKey) {
    let secret: [u8; 32] = std::array::from_fn(|_| rand::random::<u8>());
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key();

    let mut multicodec_key = Vec::with_capacity(ED25519_MULTICODEC.len() + 32);
    multicodec_key.extend_from_slice(&ED25519_MULTICODEC);
    multicodec_key.extend_from_slice(public_key.as_bytes());
    let did = format!("did:key:z{}", bs58::encode(&multicodec_key).into_string());

    (did, signing_key)
}

/// Start a relay with `data_dir` set to a temporary directory so that space,
/// invite, and web backends are all initialized and their routes mounted.
///
/// Returns `(base_url, _temp_dir)`. The `TempDir` must be kept alive for the
/// duration of the test so the `SQLite` database is not deleted.
pub async fn start_relay_with_data_dir(external_url: bool) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        host: "127.0.0.1".into(),
        port: 0,
        relay_name: "test-relay".into(),
        require_auth: false,
        database_url: None,
        outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
        data_dir: Some(dir.path().to_path_buf()),
        external_url: if external_url {
            Some(format!("http://{addr}"))
        } else {
            None
        },
        ux_url: None,
        authorized_keys_file: None,
        snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
        snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), dir)
}

/// Register a space via `POST /spaces/register` and return its `space_id`.
pub async fn register_space(client: &reqwest::Client, base_url: &str, name: &str) -> String {
    let resp = client
        .post(format!("{base_url}/spaces/register"))
        .json(&serde_json::json!({"name": name}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "expected 201 from /spaces/register");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["space_id"].as_str().unwrap().to_owned()
}

/// Create an invite via the JSON API and return the invite code.
pub async fn create_invite(client: &reqwest::Client, base_url: &str, space_id: &str) -> String {
    let resp = client
        .post(format!("{base_url}/invites"))
        .json(&serde_json::json!({"space_id": space_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    body["code"].as_str().unwrap().to_owned()
}
