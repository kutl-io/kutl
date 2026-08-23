//! Shared test helpers for kutl-relay integration tests.
//!
//! Not all helpers are used in every test file — `dead_code` is expected.

#![allow(dead_code)]

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer as _, SigningKey};
use kutl_proto::sync::{SyncEnvelope, sync_envelope::Payload};
use kutl_relay::config::RelayConfig;
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// Channels for a test connection. `data_rx` / `ack_rx` / `ctrl_rx` must
/// remain owned for the duration of a test so the relay-side senders stay
/// alive — some callers drain them, others only hold them as channel
/// anchors. `ack_rx` carries this connection's own-acks (handshake /
/// register / rename / unregister acks, errors, query results) on the
/// unbounded own-ack lane; `ctrl_rx` carries broadcasts from other
/// connections (lifecycle, displacement, signals, stale notices).
pub struct TestConn {
    pub conn_id: u64,
    pub data_rx: mpsc::Receiver<Vec<u8>>,
    pub ack_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub ctrl_rx: mpsc::Receiver<Vec<u8>>,
}

/// Drain the `SubscribeStatus` envelope the relay sends after a subscribe.
/// It is a direct response to the connection's own Subscribe, so it rides the
/// own-ack lane (`ack_rx`). Returns the payload for tests that want to inspect
/// it; tests that only care about subsequent messages can discard it.
///
/// Blocks briefly for the status to arrive. Intended to be called
/// right after a `RelayCommand::Subscribe` is dispatched.
pub async fn drain_subscribe_status(
    ack_rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
) -> Option<kutl_proto::sync::SubscribeStatus> {
    let bytes = tokio::time::timeout(std::time::Duration::from_millis(200), ack_rx.recv())
        .await
        .ok()
        .flatten()?;
    let envelope = SyncEnvelope::decode(bytes.as_slice()).ok()?;
    match envelope.payload {
        Some(Payload::SubscribeStatus(s)) => Some(s),
        // Unexpected — the first own-ack frame after subscribe should be
        // SubscribeStatus. If it's something else, surface via None so
        // the caller can decide how to handle it.
        _ => None,
    }
}

/// Generate an Ed25519 keypair and return `(did, signing_key)`.
pub fn test_keypair() -> (String, SigningKey) {
    let secret: [u8; 32] = std::array::from_fn(|_| rand::random::<u8>());
    let signing_key = SigningKey::from_bytes(&secret);
    let did = kutl_signals::did_key_encode(&signing_key.verifying_key());
    (did, signing_key)
}

/// Perform the DID challenge-response auth flow against `base_url` and return
/// the bearer token.
///
/// POSTs `did` to `/auth/challenge`, signs the returned nonce (base64
/// URL-safe, no padding) with `signing_key`, POSTs the signature to
/// `/auth/verify`, and returns the `token` field of the response. `base_url`
/// is the `http://host:port` prefix of the relay.
///
/// This is the HTTP test helper — do not confuse it with
/// `kutl_client::authenticate`, which is a WebSocket helper with a different
/// shape. Raw-flow contract tests that assert on the challenge/verify handshake
/// itself build the requests inline rather than calling this helper.
pub async fn authenticate(base_url: &str, did: &str, signing_key: &SigningKey) -> String {
    let client = reqwest::Client::new();

    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/challenge"))
        .json(&serde_json::json!({ "did": did }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let nonce = resp["nonce"].as_str().unwrap();
    let nonce_bytes = URL_SAFE_NO_PAD.decode(nonce).unwrap();
    let sig_b64 = URL_SAFE_NO_PAD.encode(signing_key.sign(&nonce_bytes).to_bytes());

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

/// File name of the seeded `authorized_keys` file inside a data-dir relay's
/// directory. Callers that need to enroll a DID append a bare line to
/// `dir.path().join(AUTHORIZED_KEYS_FILE)` (live-reloaded per auth check).
pub const AUTHORIZED_KEYS_FILE: &str = "authorized_keys";

/// Append a bare DID line to an `authorized_keys` file, authorizing that DID
/// for all spaces.
///
/// The relay's parser reads the first whitespace token of each non-`#` line, so
/// a lone DID on its own line grants that DID. The file is live-reloaded per
/// auth check, so the DID is usable immediately after this returns. Each
/// `write_all` is a single short, newline-terminated line, so a concurrent
/// reader sees either the whole line or none of it; for tests that append from
/// multiple threads to the *same* file, serialize the calls with an external
/// lock (see [`authorize_did_locked`]).
pub fn authorize_did(keys_path: &std::path::Path, did: &str) {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(keys_path)
        .unwrap();
    f.write_all(format!("{did}\n").as_bytes()).unwrap();
}

/// Like [`authorize_did`] but serialized against `write_lock`, for tests that
/// append to a single shared `authorized_keys` file from multiple threads. The
/// lock guarantees the live-reload read never observes a torn line.
pub fn authorize_did_locked(
    keys_path: &std::path::Path,
    did: &str,
    write_lock: &std::sync::Mutex<()>,
) {
    let _guard = write_lock.lock().unwrap();
    authorize_did(keys_path, did);
}

/// Start a relay with `data_dir` set to a temporary directory so that space,
/// invite, and web backends are all initialized and their routes mounted.
///
/// The relay boots auth-on (auth is unconditional) with a seeded, empty
/// `authorized_keys` file at `dir.path().join(AUTHORIZED_KEYS_FILE)`. The open
/// bootstrap/discovery routes (`/spaces/register`, `/invites`, `/relay-policy`)
/// stay reachable anonymously; auth-gated routes require a bearer whose DID the
/// caller enrolls into that keys file (via [`test_keypair`] + `authenticate`).
///
/// Returns `(base_url, _temp_dir)`. The `TempDir` must be kept alive for the
/// duration of the test so the `SQLite` database (and keys file) is not deleted.
pub async fn start_relay_with_data_dir(external_url: bool) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    // Seed an (empty) authorized-keys file so the relay boots auth-on and
    // callers can enroll a DID by appending to it. `authorized_keys` is
    // live-reloaded per auth check.
    let keys_path = dir.path().join(AUTHORIZED_KEYS_FILE);
    std::fs::write(&keys_path, "# data-dir relay test keys\n").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        data_dir: Some(dir.path().to_path_buf()),
        authorized_keys_file: Some(keys_path),
        external_url: if external_url {
            Some(format!("http://{addr}"))
        } else {
            None
        },
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), dir)
}

/// Start an account-required relay (auth is unconditional) with `external_url`
/// set, for testing the onboarding policy an authenticated relay advertises.
///
/// Returns `(base_url, _temp_dir)`; keep the `TempDir` alive for the test.
pub async fn start_account_required_relay() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    // Unconditional auth needs an authorizer; an OSS relay has no membership backend,
    // so point at an (empty) authorized-keys file inside the data dir, which the
    // TempDir keeps alive for the test.
    let keys_path = dir.path().join(AUTHORIZED_KEYS_FILE);
    std::fs::write(&keys_path, "# test keys\n").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        data_dir: Some(dir.path().to_path_buf()),
        external_url: Some(format!("http://{addr}")),
        authorized_keys_file: Some(keys_path),
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), dir)
}

/// Boot a storeless relay app (no data dir, no space/registry/change backends)
/// from `config` — the kutlhub-shaped storeless path.
///
/// The OSS binary never boots this way: `build_app` requires a data dir. The
/// storeless code paths exist for
/// the kutlhub host relay (`space_backend = None`), so MCP tests that pin the
/// in-memory implicit-create / list-spaces-from-registries behaviour construct
/// that shape explicitly through the host seam. Mirrors the after-merge
/// wiring hosts use (no OSS
/// materializer; with no signal store it would be a no-op anyway).
pub fn build_storeless_app(
    config: RelayConfig,
) -> (
    axum::Router,
    tokio::task::JoinHandle<()>,
    Option<tokio::task::JoinHandle<()>>,
) {
    use std::sync::Arc;
    kutl_relay::build_app_with_backends(
        config,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        // Storeless: no record log, so signal appends are no-ops.
        None,
        Arc::new(kutl_relay::observer::TracingObserver),
        Arc::new(kutl_relay::observer::NoopBeforeMergeObserver),
        // No host feed publisher — the materializer runs regardless.
        None,
        Arc::new(kutl_relay::mcp_tools::NoopToolProvider),
        Arc::new(kutl_relay::mcp_tools::DefaultInstructionsProvider),
    )
    .expect("build storeless relay app")
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
