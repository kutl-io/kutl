//! Shared test helpers for kutl-relay integration tests.
//!
//! Not all helpers are used in every test file — `dead_code` is expected.

#![allow(dead_code)]

pub mod mcp;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer as _, SigningKey};
use futures_util::{SinkExt as _, StreamExt as _};
use kutl_proto::protocol::{
    blob_ops_envelope, decode_envelope, encode_envelope, handshake_envelope_with_token,
    subscribe_envelope, sync_ops_envelope,
};
use kutl_proto::sync::{self, SyncEnvelope, sync_envelope::Payload};
use kutl_relay::config::RelayConfig;
use kutl_relay::relay::{Relay, RelayCommand};
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;

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
    let bytes = tokio::time::timeout(SUBSCRIBE_DRAIN_TIMEOUT, ack_rx.recv())
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

/// Listen to `space` on an in-process actor: `SubscribeSignals` is how a
/// connection becomes present in a space and enrols in its lifecycle
/// broadcasts, as a daemon does at the start of every session. The backlog
/// page it answers with (empty on a relay without records) rides the own-ack
/// lane (`ack_rx`), never `ctrl_rx`; drain it with [`drain_signal_page`].
pub async fn listen(relay: &mut Relay, conn_id: u64, space: &str) {
    relay
        .process_command(RelayCommand::SubscribeSignals {
            conn_id,
            msg: sync::SubscribeSignals {
                space_id: space.into(),
                cursor: None,
            },
        })
        .await;
}

/// Drain the `SignalPage` the relay sends in answer to a `SubscribeSignals`.
/// It is a direct response to the connection's own request, so it rides the
/// own-ack lane (`ack_rx`), ahead of whatever that lane carries next. Returns
/// the page for tests that want to inspect it. Intended to be called right
/// after [`listen`] by a test that then reads the ack lane.
pub async fn drain_signal_page(
    ack_rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
) -> Option<kutl_proto::sync::SignalPage> {
    let bytes = tokio::time::timeout(SUBSCRIBE_DRAIN_TIMEOUT, ack_rx.recv())
        .await
        .ok()
        .flatten()?;
    let envelope = SyncEnvelope::decode(bytes.as_slice()).ok()?;
    match envelope.payload {
        Some(Payload::SignalPage(page)) => Some(page),
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

/// Upper bound on one WebSocket receive in a protocol test. Generous
/// against a loopback relay, where replies land in microseconds; the bound
/// exists so a broken premise FAILS a test in seconds instead of wedging the
/// suite on a reply that never comes.
pub const RECV_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How long a subscribe waits for each of the frames it may be answered with
/// (`SubscribeStatus`, then the catch-up `SyncOps`) before concluding none is
/// coming. Loopback replies land in microseconds; a fresh document sends no
/// catch-up at all, so this bound is what ends the drain.
pub const SUBSCRIBE_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(200);

/// Space id presented in every test handshake.
const HANDSHAKE_SPACE_ID: &str = "d5fe8251-3196-4a97-8d81-66092b9a47dc";

/// A thin WebSocket test client against a running relay.
///
/// Authentication is mandatory, so a client presents a bearer minted through
/// the real challenge-response flow ([`TestClient::handshake_as`]) or one the
/// caller minted itself ([`TestClient::handshake_with_token`]). Which DID a
/// client handshakes as, and how that DID gets authorized, is the calling test
/// file's business.
pub struct TestClient {
    pub ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    /// Relay `host:port`, retained so [`TestClient::handshake_as`] can mint a
    /// bearer via the relay's `/auth/challenge` + `/auth/verify` flow.
    pub addr: String,
}

impl TestClient {
    pub async fn connect(addr: &str) -> Self {
        let url = format!("ws://{addr}/ws");
        let (ws, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("ws connect");
        Self {
            ws,
            addr: addr.to_owned(),
        }
    }

    pub async fn send_envelope(&mut self, envelope: &SyncEnvelope) {
        let bytes = encode_envelope(envelope);
        self.ws
            .send(tungstenite::Message::Binary(bytes.into()))
            .await
            .expect("ws send");
    }

    pub async fn recv_envelope(&mut self) -> SyncEnvelope {
        let msg = tokio::time::timeout(RECV_TIMEOUT, self.ws.next())
            .await
            .expect("recv timed out")
            .expect("stream ended")
            .expect("ws error");

        match msg {
            tungstenite::Message::Binary(bytes) => decode_envelope(&bytes).expect("decode"),
            other => panic!("expected binary frame, got {other:?}"),
        }
    }

    /// Mint a bearer for `did` via the real challenge-response flow and
    /// complete the handshake with it.
    pub async fn handshake_as(&mut self, did: &str, signing_key: &SigningKey) {
        let token = authenticate(&format!("http://{}", self.addr), did, signing_key).await;
        self.handshake_with_token(&token).await;
    }

    /// Complete the handshake with an already-minted bearer token. Polling
    /// probes mint one token and reuse it across many short-lived connections
    /// instead of re-authenticating each iteration.
    pub async fn handshake_with_token(&mut self, token: &str) {
        self.send_envelope(&handshake_envelope_with_token(
            HANDSHAKE_SPACE_ID,
            token,
            "test-client",
        ))
        .await;

        let ack = self.recv_envelope().await;
        assert!(
            matches!(ack.payload, Some(Payload::HandshakeAck(_))),
            "expected HandshakeAck, got {ack:?}"
        );
    }

    /// Subscribe and return the catch-up `SyncOps` if the relay sends one.
    /// Skips over the `SubscribeStatus` envelope that precedes the catch-up.
    pub async fn subscribe(&mut self, space: &str, doc: &str) -> Option<sync::SyncOps> {
        self.send_envelope(&subscribe_envelope(space, doc)).await;

        for _ in 0..3 {
            match tokio::time::timeout(SUBSCRIBE_DRAIN_TIMEOUT, self.ws.next()).await {
                Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => {
                    let env = decode_envelope(&bytes).expect("decode");
                    match env.payload {
                        Some(Payload::SyncOps(ops)) => return Some(ops),
                        Some(Payload::SubscribeStatus(_)) => {}
                        _ => return None,
                    }
                }
                _ => return None,
            }
        }
        None
    }

    pub async fn send_ops(
        &mut self,
        space: &str,
        doc_id: &str,
        ops: Vec<u8>,
        metadata: Vec<sync::ChangeMetadata>,
    ) {
        self.send_envelope(&sync_ops_envelope(
            space,
            doc_id,
            ops,
            metadata,
            std::collections::HashMap::new(),
        ))
        .await;
    }

    pub async fn recv_ops(&mut self) -> sync::SyncOps {
        let envelope = self.recv_envelope().await;
        match envelope.payload {
            Some(Payload::SyncOps(ops)) => ops,
            other => panic!("expected SyncOps, got {other:?}"),
        }
    }

    pub async fn send_blob(
        &mut self,
        space: &str,
        doc_id: &str,
        blob: Vec<u8>,
        hash: Vec<u8>,
        metadata: Option<sync::ChangeMetadata>,
    ) {
        self.send_envelope(&blob_ops_envelope(space, doc_id, blob, hash, metadata))
            .await;
    }

    /// The refusal a rejected `SyncOps` carries.
    pub async fn recv_error(&mut self) -> sync::Error {
        let envelope = self.recv_envelope().await;
        match envelope.payload {
            Some(Payload::SyncOpsRejected(r)) => r.error.expect("a refusal carries its error"),
            other => panic!("expected SyncOpsRejected, got {other:?}"),
        }
    }

    /// The refusal a refused handshake's ack carries.
    pub async fn recv_handshake_refusal(&mut self) -> sync::Error {
        let envelope = self.recv_envelope().await;
        match envelope.payload {
            Some(Payload::HandshakeAck(ack)) => ack.error.expect("a refused ack carries its error"),
            other => panic!("expected a refused HandshakeAck, got {other:?}"),
        }
    }
}

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

/// Boot an in-memory relay app (no data dir, no backends) from `config`.
///
/// No production relay boots this way: the OSS binary requires a data dir
/// (`build_app`) and the hosted relay injects its own backends. What this
/// shape shares with the hosted relay is having no space backend, so MCP
/// tests that pin that behaviour (implicit create on first touch,
/// list-spaces-from-registries) construct it here. Mirrors the after-merge
/// wiring hosts use (no OSS materializer; with no record log it would be a
/// no-op anyway).
pub fn build_in_memory_app(
    config: RelayConfig,
) -> (
    axum::Router,
    tokio::task::JoinHandle<()>,
    Option<tokio::task::JoinHandle<()>>,
) {
    kutl_relay::testing::build_in_memory_app(config, kutl_relay::testing::TestBackends::default())
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
