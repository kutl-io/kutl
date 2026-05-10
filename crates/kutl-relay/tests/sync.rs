mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    PROTOCOL_VERSION_MAJOR, PROTOCOL_VERSION_MINOR, blob_ops_envelope, decode_envelope,
    encode_envelope, handshake_envelope, handshake_envelope_with_token, presence_update_envelope,
    subscribe_envelope, sync_ops_envelope,
};
use kutl_proto::sync::{self, SyncEnvelope, sync_envelope::Payload};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;

use kutl_relay::config::RelayConfig;
use kutl_relay::observer::{AfterMergeObserver, MergedEvent};
use kutl_relay::relay::{Relay, RelayCommand};

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Start a relay on a random port and return the address.
async fn start_relay() -> String {
    start_relay_no_auth().await
}

/// Start a relay on a random port with `require_auth: true`.
/// Returns `(addr, _keys_file)` — the keys file must outlive the test.
async fn start_relay_with_auth() -> (String, tempfile::NamedTempFile) {
    use std::io::Write;
    let mut keys = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys, "# sync test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let mut config = test_relay_config();
    config.require_auth = true;
    config.authorized_keys_file = Some(keys.path().to_path_buf());

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys)
}

async fn start_relay_no_auth() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let mut config = test_relay_config();
    config.require_auth = false;

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

/// Perform the full auth flow (challenge + sign + verify) and return the token.
async fn authenticate(addr: &str, did: &str, signing_key: &SigningKey) -> String {
    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    // 1. Request challenge.
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

    // 2. Sign the nonce bytes.
    let nonce_bytes = URL_SAFE_NO_PAD.decode(nonce).unwrap();
    let signature = signing_key.sign(&nonce_bytes);
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    // 3. Verify.
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

/// A thin WebSocket test client.
struct TestClient {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

impl TestClient {
    async fn connect(addr: &str) -> Self {
        let url = format!("ws://{addr}/ws");
        let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        Self { ws }
    }

    async fn send_envelope(&mut self, envelope: &SyncEnvelope) {
        let bytes = encode_envelope(envelope);
        self.ws
            .send(tungstenite::Message::Binary(bytes.into()))
            .await
            .unwrap();
    }

    async fn recv_envelope(&mut self) -> SyncEnvelope {
        let msg = tokio::time::timeout(Duration::from_secs(5), self.ws.next())
            .await
            .expect("recv timed out")
            .expect("stream ended")
            .expect("ws error");

        match msg {
            tungstenite::Message::Binary(bytes) => decode_envelope(&bytes).unwrap(),
            other => panic!("expected binary frame, got {other:?}"),
        }
    }

    async fn handshake(&mut self) {
        self.send_envelope(&handshake_envelope("test-client")).await;

        let ack = self.recv_envelope().await;
        assert!(
            matches!(ack.payload, Some(Payload::HandshakeAck(_))),
            "expected HandshakeAck, got {ack:?}"
        );
    }

    async fn subscribe(&mut self, space: &str, doc: &str) -> Option<sync::SyncOps> {
        self.send_envelope(&subscribe_envelope(space, doc)).await;

        // Drain envelopes until we see `SyncOps` (catch-up). RFD 0066 added
        // a `SubscribeStatus` envelope that arrives first (ctrl before data).
        for _ in 0..3 {
            match tokio::time::timeout(Duration::from_millis(200), self.ws.next()).await {
                Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => {
                    let env = decode_envelope(&bytes).unwrap();
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

    async fn send_ops(
        &mut self,
        space: &str,
        doc_id: &str,
        ops: Vec<u8>,
        metadata: Vec<sync::ChangeMetadata>,
    ) {
        self.send_envelope(&sync_ops_envelope(space, doc_id, ops, metadata))
            .await;
    }

    async fn recv_ops(&mut self) -> sync::SyncOps {
        let envelope = self.recv_envelope().await;
        match envelope.payload {
            Some(Payload::SyncOps(ops)) => ops,
            other => panic!("expected SyncOps, got {other:?}"),
        }
    }

    async fn send_blob(
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

    async fn recv_error(&mut self) -> sync::Error {
        let envelope = self.recv_envelope().await;
        match envelope.payload {
            Some(Payload::Error(e)) => e,
            other => panic!("expected Error, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Polling helpers
//
// These replace fixed `tokio::time::sleep` calls that were waiting for the
// relay to integrate prior writes before a fresh client could observe them.
// Polling lets the happy path return in single-digit milliseconds while still
// tolerating slow CI machines, and removes a class of timing flakes.
//
// Do NOT use these for tests that legitimately wait on real timing
// (e.g. the background flush task in `storage.rs` / `quota.rs`).
// ---------------------------------------------------------------------------

/// How long to keep polling before declaring a relay state never settled.
const POLL_TIMEOUT: Duration = Duration::from_secs(2);

/// How often to retry inside the polling helpers. Short enough that a
/// fast machine sees no observable wall-clock cost.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Repeatedly subscribe via fresh probe clients until `predicate` returns
/// true on the catch-up `SyncOps`. Returns the matching catch-up.
async fn wait_for_doc_state(
    addr: &str,
    space: &str,
    doc: &str,
    predicate: impl Fn(&sync::SyncOps) -> bool,
) -> sync::SyncOps {
    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let mut probe = TestClient::connect(addr).await;
        probe.handshake().await;
        if let Some(s) = probe.subscribe(space, doc).await
            && predicate(&s)
        {
            return s;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "wait_for_doc_state timed out for {space}/{doc}",
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Convenience: wait until a fresh subscriber sees a non-empty catch-up.
/// Use [`wait_for_doc_state`] when the test needs to wait for specific content.
async fn wait_for_doc_catchup(addr: &str, space: &str, doc: &str) -> sync::SyncOps {
    wait_for_doc_state(addr, space, doc, |s| !s.ops.is_empty()).await
}

/// Repeatedly issue `ListSpaceDocuments` until at least `expected` documents
/// are present. Replaces fixed sleeps that were waiting for register-document
/// envelopes to land before a fresh client lists them.
async fn wait_for_doc_count(addr: &str, space: &str, expected: usize) {
    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let mut probe = TestClient::connect(addr).await;
        probe.handshake().await;
        let req = SyncEnvelope {
            payload: Some(Payload::ListSpaceDocuments(sync::ListSpaceDocuments {
                space_id: space.into(),
            })),
        };
        probe.send_envelope(&req).await;
        let resp = probe.recv_envelope().await;
        if let Some(Payload::ListSpaceDocumentsResult(r)) = resp.payload
            && r.documents.len() >= expected
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "wait_for_doc_count({expected}) timed out for {space}",
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Create a local document with a single edit, returning ops bytes and changes.
fn make_edit(agent_name: &str, text: &str, intent: &str) -> (Vec<u8>, Vec<sync::ChangeMetadata>) {
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent(agent_name).unwrap();
    doc.edit(
        agent,
        agent_name,
        intent,
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, text),
    )
    .unwrap();
    let ops = doc.encode_since(&[]);
    let metadata = doc.changes_since(&[]);
    (ops, metadata)
}

/// Merge received sync ops into a fresh document and return its content.
fn apply_ops(sync_ops: &sync::SyncOps) -> String {
    let mut doc = kutl_core::Document::new();
    doc.merge(&sync_ops.ops, &sync_ops.metadata).unwrap();
    doc.content()
}

// ---------------------------------------------------------------------------
// Standalone relay test helpers
// ---------------------------------------------------------------------------

/// Default space ID for standalone relay tests.
const TEST_SPACE: &str = "space";

/// Default document ID for standalone relay tests.
// Real UUIDs. The relay's `handle_signal` flag-creation path
// validates `document_id` against the RFC 4122 shape (RFD 0042 —
// the registry has been UUID-keyed since the text→uuid
// migration), so synthetic strings like `"doc"` get rejected with
// `InvalidMessage`. Synthetic strings remain fine for paths that
// don't run through the WS signal handler (e.g. RegisterDocument /
// edit_document still accept arbitrary identifiers).
const TEST_DOC: &str = "00000000-0000-4000-8000-0000000000d0";
const TEST_DOC_1: &str = "00000000-0000-4000-8000-0000000000d1";
const TEST_DOC_2: &str = "00000000-0000-4000-8000-0000000000d2";

/// Number of ops to send when flooding a channel — enough to overflow
/// `SLOW_CHANNEL_CAPACITY` (2) several times over.
const FLOOD_OPS: usize = 10;

/// Fast channel capacity — large enough to never trigger eviction.
const FAST_CHANNEL_CAPACITY: usize = 256;

/// Slow channel capacity — small enough to trigger eviction deterministically.
const SLOW_CHANNEL_CAPACITY: usize = 2;

/// Default ctrl channel capacity for tests.
const TEST_CTRL_CAPACITY: usize = 8;

/// Ctrl channel capacity that's just large enough to hold the handshake ack
/// but too small for the ack + a stale notice.
const TINY_CTRL_CAPACITY: usize = 1;

/// Number of slow subscribers in the all-slow test.
const ALL_SLOW_SUBSCRIBER_COUNT: usize = 5;

use common::TestConn;

/// Create a [`RelayConfig`] for standalone relay tests.
fn test_relay_config() -> RelayConfig {
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        outbound_capacity: FAST_CHANNEL_CAPACITY,
        ..Default::default()
    }
}

/// Connect, handshake, and optionally drain the handshake ack.
async fn connect_client_inner(
    relay: &mut Relay,
    conn_id: u64,
    name: &str,
    data_capacity: usize,
    ctrl_capacity: usize,
    drain_ack: bool,
) -> TestConn {
    let (tx, data_rx) = mpsc::channel(data_capacity);
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel(ctrl_capacity);

    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx,
            ctrl_tx,
        })
        .await;
    relay
        .process_command(RelayCommand::Handshake {
            conn_id,
            msg: sync::Handshake {
                client_name: name.into(),
                ..Default::default()
            },
        })
        .await;

    if drain_ack {
        let _ = ctrl_rx.recv().await;
    }

    TestConn {
        conn_id,
        data_rx,
        ctrl_rx,
    }
}

/// Connect and handshake a client. Drains the handshake ack from ctrl.
async fn connect_client(
    relay: &mut Relay,
    conn_id: u64,
    name: &str,
    data_capacity: usize,
    ctrl_capacity: usize,
) -> TestConn {
    connect_client_inner(relay, conn_id, name, data_capacity, ctrl_capacity, true).await
}

/// Connect and handshake a client WITHOUT draining the handshake ack.
/// The ack remains in the ctrl channel, useful for testing ctrl-full scenarios.
async fn connect_client_keep_ack(
    relay: &mut Relay,
    conn_id: u64,
    name: &str,
    data_capacity: usize,
    ctrl_capacity: usize,
) -> TestConn {
    connect_client_inner(relay, conn_id, name, data_capacity, ctrl_capacity, false).await
}

/// Subscribe a connection to the default test document.
async fn subscribe_to_doc(relay: &mut Relay, conn_id: u64) {
    subscribe_to(relay, conn_id, TEST_SPACE, TEST_DOC).await;
}

/// Subscribe a connection to a specific document.
async fn subscribe_to(relay: &mut Relay, conn_id: u64, space: &str, doc: &str) {
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id,
            msg: sync::Subscribe {
                space_id: space.into(),
                document_id: doc.into(),
            },
        })
        .await;
}

/// Send `count` ops from `conn_id` to the default test document.
async fn send_test_ops(relay: &mut Relay, conn_id: u64, count: usize) {
    send_test_ops_to(relay, conn_id, TEST_SPACE, TEST_DOC, count).await;
}

/// Send `count` ops from `conn_id` to a specific document.
async fn send_test_ops_to(relay: &mut Relay, conn_id: u64, space: &str, doc: &str, count: usize) {
    for i in 0..count {
        let (ops, meta) = make_edit(&format!("c{conn_id}-{i}"), &format!("op{i}"), "test");
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(sync::SyncOps {
                    space_id: space.into(),
                    document_id: doc.into(),
                    ops,
                    metadata: meta,
                    ..Default::default()
                }),
            })
            .await;
    }
}

/// Drain all available messages from a data channel.
fn drain_data(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<SyncEnvelope> {
    let mut msgs = Vec::new();
    while let Ok(bytes) = rx.try_recv() {
        msgs.push(decode_envelope(&bytes).expect("relay sent invalid protobuf"));
    }
    msgs
}

/// Drain all available messages from a ctrl channel.
fn drain_ctrl(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<SyncEnvelope> {
    drain_data(rx) // Same implementation, different semantic name.
}

/// Check whether any envelope in the list is a `StaleSubscriber`.
fn has_stale_notice(msgs: &[SyncEnvelope]) -> bool {
    msgs.iter()
        .any(|e| matches!(e.payload, Some(Payload::StaleSubscriber(_))))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_check() {
    let addr = start_relay().await;

    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    tokio::io::AsyncWriteExt::write_all(
        &mut stream,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();

    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut buf)
        .await
        .unwrap();

    let response = String::from_utf8_lossy(&buf);
    assert!(response.contains("200"), "expected 200 in: {response}");
    assert!(response.contains("ok"), "expected 'ok' body in: {response}");
}

#[tokio::test]
async fn test_handshake_roundtrip() {
    let addr = start_relay().await;
    let mut client = TestClient::connect(&addr).await;

    client.send_envelope(&handshake_envelope("test")).await;

    let ack = client.recv_envelope().await;
    match ack.payload {
        Some(Payload::HandshakeAck(ack)) => {
            assert_eq!(ack.relay_name, "test-relay");
            assert_eq!(ack.protocol_version_major, PROTOCOL_VERSION_MAJOR);
            assert_eq!(ack.protocol_version_minor, PROTOCOL_VERSION_MINOR);
        }
        other => panic!("expected HandshakeAck, got {other:?}"),
    }
}

#[tokio::test]
async fn test_subscribe_empty_doc() {
    let addr = start_relay().await;
    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;

    let catch_up = client.subscribe("space1", "doc1").await;
    let catch_up = catch_up.expect("empty doc should still send SyncOps");
    // Empty ops = protobuf default for bytes — signals "no operations applied yet."
    assert!(
        catch_up.ops.is_empty(),
        "empty doc should have zero-length ops"
    );
}

#[tokio::test]
async fn test_two_clients_converge() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a.subscribe("space", "doc").await;

    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    client_b.subscribe("space", "doc").await;

    let (ops, metadata) = make_edit("alice", "hello", "add greeting");
    client_a.send_ops("space", "doc", ops, metadata).await;

    let relayed = client_b.recv_ops().await;
    assert!(!relayed.ops.is_empty());
    assert_eq!(apply_ops(&relayed), "hello");
}

#[tokio::test]
async fn test_concurrent_edits_converge() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a.subscribe("space", "doc").await;

    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    client_b.subscribe("space", "doc").await;

    let (ops_a, meta_a) = make_edit("alice", "AAA", "alice edit");
    let (ops_b, meta_b) = make_edit("bob", "BBB", "bob edit");

    client_a
        .send_ops("space", "doc", ops_a.clone(), meta_a)
        .await;
    client_b
        .send_ops("space", "doc", ops_b.clone(), meta_b)
        .await;

    let relayed_to_b = client_b.recv_ops().await;
    let relayed_to_a = client_a.recv_ops().await;

    // Merge each relay into the corresponding local doc.
    let mut doc_a = kutl_core::Document::new();
    doc_a.merge(&ops_a, &[]).unwrap();
    doc_a
        .merge(&relayed_to_a.ops, &relayed_to_a.metadata)
        .unwrap();

    let mut doc_b = kutl_core::Document::new();
    doc_b.merge(&ops_b, &[]).unwrap();
    doc_b
        .merge(&relayed_to_b.ops, &relayed_to_b.metadata)
        .unwrap();

    assert_eq!(doc_a.content(), doc_b.content());
    let content = doc_a.content();
    assert!(content.contains("AAA"), "missing AAA in: {content}");
    assert!(content.contains("BBB"), "missing BBB in: {content}");
}

// ---------------------------------------------------------------------------
// Blob tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_relay_forwards_blob() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a.subscribe("space", "img.png").await;

    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    client_b.subscribe("space", "img.png").await;

    // Non-UTF-8 content.
    let blob = vec![0xFF, 0xFE, 0x00, 0x01, 0x80, 0x90];
    let hash = vec![0xAA; 32];
    let meta = sync::ChangeMetadata {
        timestamp: 1000,
        ..Default::default()
    };

    client_a
        .send_blob("space", "img.png", blob.clone(), hash.clone(), Some(meta))
        .await;

    let relayed = client_b.recv_ops().await;
    assert_eq!(relayed.ops, blob);
    assert_eq!(relayed.content_hash, hash);
    assert_eq!(relayed.content_mode, i32::from(sync::ContentMode::Blob));
}

#[tokio::test]
async fn test_blob_catch_up() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a.subscribe("space", "doc.pdf").await;

    let blob = vec![0x25, 0x50, 0x44, 0x46]; // %PDF
    let hash = vec![0xBB; 32];
    let meta = sync::ChangeMetadata {
        timestamp: 2000,
        ..Default::default()
    };

    client_a
        .send_blob("space", "doc.pdf", blob.clone(), hash.clone(), Some(meta))
        .await;

    // Late joiner — poll until the relay has integrated the blob write.
    let catch_up = wait_for_doc_catchup(&addr, "space", "doc.pdf").await;
    assert_eq!(catch_up.ops, blob);
    assert_eq!(catch_up.content_hash, hash);
    assert_eq!(catch_up.content_mode, i32::from(sync::ContentMode::Blob));
}

#[tokio::test]
async fn test_blob_lww_newer_wins() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a.subscribe("space", "pic.jpg").await;

    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    client_b.subscribe("space", "pic.jpg").await;

    // Client A sends blob with timestamp 1000.
    let blob_old = vec![0x01; 10];
    let hash_old = vec![0x11; 32];
    client_a
        .send_blob(
            "space",
            "pic.jpg",
            blob_old.clone(),
            hash_old.clone(),
            Some(sync::ChangeMetadata {
                timestamp: 1000,
                ..Default::default()
            }),
        )
        .await;

    // B receives the first blob.
    let first = client_b.recv_ops().await;
    assert_eq!(first.ops, blob_old);

    // Client B sends blob with newer timestamp 2000 — should win.
    let blob_new = vec![0x02; 10];
    let hash_new = vec![0x22; 32];
    client_b
        .send_blob(
            "space",
            "pic.jpg",
            blob_new.clone(),
            hash_new.clone(),
            Some(sync::ChangeMetadata {
                timestamp: 2000,
                ..Default::default()
            }),
        )
        .await;

    let relayed = client_a.recv_ops().await;
    assert_eq!(relayed.ops, blob_new);
    assert_eq!(relayed.content_hash, hash_new);

    // A late joiner should see the newer blob — wait specifically for it
    // so we don't race the LWW resolution.
    let catch_up = wait_for_doc_state(&addr, "space", "pic.jpg", |s| s.ops == blob_new).await;
    assert_eq!(catch_up.ops, blob_new);
}

#[tokio::test]
async fn test_blob_ref_rejected() {
    let addr = start_relay().await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe("space", "big.bin").await;

    // Manually build a BLOB_REF envelope.
    let envelope = sync::SyncEnvelope {
        payload: Some(Payload::SyncOps(sync::SyncOps {
            space_id: "space".into(),
            document_id: "big.bin".into(),
            content_mode: i32::from(sync::ContentMode::BlobRef),
            blob_ref_url: "https://example.com/blob".into(),
            ..Default::default()
        })),
    };

    client.send_envelope(&envelope).await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::InvalidMessage));
    assert!(err.message.contains("BLOB_REF"));
}

#[tokio::test]
async fn test_oversized_blob_rejected() {
    // Regression: a frame slightly larger than ABSOLUTE_BLOB_MAX that slips
    // past the codec layer must be rejected by the application-layer
    // defense-in-depth check in `handle_blob_sync_ops`.
    //
    // RFD 0063 §C4: enforcement is based on `msg.ops.len()` (server-measured)
    // — never on the client-advertised `blob_size` field. We therefore
    // construct a frame with ACTUAL ops bytes just over the cap, not a
    // lied `blob_size`. Because ABSOLUTE_BLOB_MAX is also the WS codec cap,
    // this test exercises the codec-layer rejection in practice (the ws
    // connection closes). The application-layer recheck is covered by the
    // quota integration tests in `tests/quota.rs`.
    let addr = start_relay().await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe("space", "big.bin").await;

    let envelope = sync::SyncEnvelope {
        payload: Some(Payload::SyncOps(sync::SyncOps {
            space_id: "space".into(),
            document_id: "big.bin".into(),
            content_mode: i32::from(sync::ContentMode::Blob),
            ops: vec![0xFF; kutl_proto::protocol::ABSOLUTE_BLOB_MAX + 1],
            content_hash: vec![0; 32],
            metadata: vec![sync::ChangeMetadata {
                timestamp: 1000,
                ..Default::default()
            }],
            ..Default::default()
        })),
    };

    // The codec rejects the frame and closes the connection. A subsequent
    // read should surface the closure rather than a parseable error frame.
    let _ = client
        .ws
        .send(tungstenite::Message::Binary(
            encode_envelope(&envelope).into(),
        ))
        .await;
    let result = client.ws.next().await;
    assert!(
        result.is_none() || result.as_ref().is_some_and(std::result::Result::is_err),
        "expected close or error after oversize frame, got {result:?}"
    );
}

#[tokio::test]
async fn test_ws_rejects_oversize_message() {
    let addr = start_relay().await;
    let url = format!("ws://{addr}/ws");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let oversize = vec![0u8; kutl_proto::protocol::ABSOLUTE_BLOB_MAX + 1];
    // The codec-layer cap should reject this without ever reaching application code.
    let _ = ws.send(tungstenite::Message::Binary(oversize.into())).await;

    // Server should close the connection (or the send itself errored after close).
    let result = ws.next().await;
    assert!(
        matches!(
            result,
            Some(Ok(tungstenite::Message::Close(_)) | Err(_)) | None
        ),
        "expected connection close, got {result:?}"
    );
}

#[tokio::test]
async fn test_late_joiner_catches_up() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a.subscribe("space", "doc").await;

    let (ops, metadata) = make_edit("alice", "hello world", "initial content");
    client_a.send_ops("space", "doc", ops, metadata).await;

    let catch_up = wait_for_doc_catchup(&addr, "space", "doc").await;
    assert_eq!(apply_ops(&catch_up), "hello world");
}

// ---------------------------------------------------------------------------
// Auth tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_auth_challenge_returns_nonce() {
    let (addr, _keys) = start_relay_with_auth().await;
    let (did, _) = common::test_keypair();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/auth/challenge"))
        .json(&serde_json::json!({"did": did}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let nonce = body["nonce"].as_str().expect("nonce field missing");
    let expires_at = body["expires_at"]
        .as_i64()
        .expect("expires_at field missing");

    // Nonce should decode to 32 bytes.
    let decoded = URL_SAFE_NO_PAD.decode(nonce).unwrap();
    assert_eq!(decoded.len(), 32);
    assert!(expires_at > 0);
}

#[tokio::test]
async fn test_auth_full_flow() {
    let (addr, _keys) = start_relay_with_auth().await;
    let (did, signing_key) = common::test_keypair();

    let token = authenticate(&addr, &did, &signing_key).await;
    assert!(token.starts_with("kutl_"), "token should have kutl_ prefix");
    assert!(token.len() > 10, "token should be non-trivial");
}

#[tokio::test]
async fn test_auth_bad_signature_rejected() {
    let (addr, _keys) = start_relay_with_auth().await;
    let (did, _) = common::test_keypair();
    let (_, wrong_key) = common::test_keypair();

    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    // Get a challenge.
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

    // Sign with the wrong key.
    let nonce_bytes = URL_SAFE_NO_PAD.decode(nonce).unwrap();
    let signature = wrong_key.sign(&nonce_bytes);
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    let resp = client
        .post(format!("{base_url}/auth/verify"))
        .json(&serde_json::json!({
            "did": did,
            "nonce": nonce,
            "signature": sig_b64,
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("invalid signature")
    );
}

#[tokio::test]
async fn test_authenticated_handshake_accepted() {
    let (addr, _keys) = start_relay_with_auth().await;
    let (did, signing_key) = common::test_keypair();

    let token = authenticate(&addr, &did, &signing_key).await;

    // Connect WS with the token.
    let url = format!("ws://{addr}/ws");
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let mut client = TestClient { ws };

    client
        .send_envelope(&handshake_envelope_with_token("auth-test", &token, ""))
        .await;

    let ack = client.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "expected HandshakeAck, got {ack:?}"
    );
}

#[tokio::test]
async fn test_unauthenticated_handshake_rejected() {
    let (addr, _keys) = start_relay_with_auth().await;

    let mut client = TestClient::connect(&addr).await;

    // Send handshake without a token.
    client
        .send_envelope(&handshake_envelope("no-auth-client"))
        .await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::AuthFailed));
}

#[tokio::test]
async fn test_invalid_token_handshake_rejected() {
    let (addr, _keys) = start_relay_with_auth().await;

    let mut client = TestClient::connect(&addr).await;

    // Send handshake with a bogus token — should be rejected.
    client
        .send_envelope(&handshake_envelope_with_token(
            "bogus-client",
            "kutl_not-a-real-token",
            "",
        ))
        .await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::AuthFailed));
}

/// Test eviction using standalone relay (no TCP/WebSocket overhead).
///
/// Uses a bounded channel with capacity 2 so we can trigger eviction
/// deterministically without worrying about TCP buffer sizes.
#[tokio::test]
async fn test_stale_subscriber_on_channel_full() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow = connect_client(
        &mut relay,
        2,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Send ops from writer. Don't drain slow — let it fill up.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Data channel: at most SLOW_CHANNEL_CAPACITY SyncOps.
    let data_messages = drain_data(&mut slow.data_rx);
    let sync_ops_count = data_messages
        .iter()
        .filter(|e| matches!(e.payload, Some(Payload::SyncOps(_))))
        .count();

    assert!(
        sync_ops_count <= SLOW_CHANNEL_CAPACITY,
        "expected at most {SLOW_CHANNEL_CAPACITY} SyncOps (channel capacity), got {sync_ops_count}"
    );
    assert_eq!(
        sync_ops_count,
        data_messages.len(),
        "data channel should only contain SyncOps"
    );

    // Control channel: stale notice should be delivered.
    let ctrl_messages = drain_ctrl(&mut slow.ctrl_rx);
    assert!(
        has_stale_notice(&ctrl_messages),
        "expected StaleSubscriber on control channel"
    );

    // Verify eviction: one more op should NOT reach the slow client.
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    assert!(
        slow.data_rx.try_recv().is_err(),
        "slow client received message after eviction — eviction did not work"
    );
}

/// Test that a client can resubscribe after eviction and get full catch-up.
#[tokio::test]
async fn test_resubscribe_after_eviction() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow = connect_client(
        &mut relay,
        2,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Flood to trigger eviction.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Drain slow client's buffered messages.
    drain_data(&mut slow.data_rx);

    // Resubscribe — should get catch-up with all ops.
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    let catch_up_bytes = slow
        .data_rx
        .try_recv()
        .expect("resubscribe should produce catch-up");
    let catch_up_env = decode_envelope(&catch_up_bytes).unwrap();

    let catch_up_ops = match catch_up_env.payload {
        Some(Payload::SyncOps(ops)) => ops,
        other => panic!("expected SyncOps catch-up, got {other:?}"),
    };

    let mut doc = kutl_core::Document::new();
    doc.merge(&catch_up_ops.ops, &catch_up_ops.metadata)
        .unwrap();
    assert!(!doc.content().is_empty(), "catch-up should have content");

    let content = doc.content();
    assert!(content.contains("op0"), "catch-up missing op0");
    assert!(content.contains("op9"), "catch-up missing op9");
}

// ---------------------------------------------------------------------------
// Degradation mode tests — misbehaving / slow / disconnecting clients
// ---------------------------------------------------------------------------

/// Client disconnects mid-stream, reconnects, resubscribes, and gets catch-up.
#[tokio::test]
async fn test_disconnect_mid_stream_recovery() {
    let addr = start_relay().await;

    // Writer establishes content.
    let mut writer = TestClient::connect(&addr).await;
    writer.handshake().await;
    writer.subscribe("space", "doc").await;

    let (ops, metadata) = make_edit("alice", "before-disconnect", "initial");
    writer.send_ops("space", "doc", ops, metadata).await;

    // Reader subscribes, gets catch-up, then disconnects abruptly.
    let mut reader = TestClient::connect(&addr).await;
    reader.handshake().await;
    let catch_up = reader.subscribe("space", "doc").await;
    assert!(catch_up.is_some(), "should get initial catch-up");

    // Drop the reader — simulates abrupt disconnect.
    drop(reader);

    // Writer sends more content while reader is gone.
    let (ops2, metadata2) = make_edit("alice2", "after-disconnect", "more content");
    writer.send_ops("space", "doc", ops2, metadata2).await;

    // Reconnected reader should see both rounds — poll until merged.
    let catch_up2 = wait_for_doc_state(&addr, "space", "doc", |s| {
        let content = apply_ops(s);
        content.contains("before-disconnect") && content.contains("after-disconnect")
    })
    .await;

    let content = apply_ops(&catch_up2);
    assert!(content.contains("before-disconnect"));
    assert!(content.contains("after-disconnect"));
}

/// Write-only client (sends ops, never reads) gets evicted. Uses standalone
/// relay with small channel to avoid timing sensitivity.
#[tokio::test]
async fn test_write_only_client_evicted() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut wo = connect_client(
        &mut relay,
        2,
        "write-only",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, wo.conn_id).await;

    // Write-only client sends ops (acts as writer too).
    send_test_ops(&mut relay, wo.conn_id, FLOOD_OPS).await;

    // Writer floods to fill write-only client's channel.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Check that write-only client received stale notice on ctrl channel.
    let ctrl_messages = drain_ctrl(&mut wo.ctrl_rx);
    assert!(
        has_stale_notice(&ctrl_messages),
        "write-only client should have been evicted with StaleSubscriber"
    );

    // Verify the write-only client's ops were still accepted by the relay.
    drain_data(&mut wo.data_rx);

    // A new subscriber should see all content (from both writer and write-only).
    let mut verifier = connect_client(
        &mut relay,
        3,
        "verifier",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, verifier.conn_id).await;

    let catch_up_bytes = verifier
        .data_rx
        .try_recv()
        .expect("new subscriber should get catch-up");
    let env = decode_envelope(&catch_up_bytes).unwrap();
    let ops = match env.payload {
        Some(Payload::SyncOps(ops)) => ops,
        other => panic!("expected SyncOps, got {other:?}"),
    };
    let content = apply_ops(&ops);
    // Both clients sent FLOOD_OPS ops with text "op0".."op9". CRDT merges all
    // concurrent inserts, so the catch-up must contain text from both.
    assert!(
        content.contains("op0"),
        "relay should have merged ops: {content}"
    );
    // Count how many "op" substrings appear — should be 2 * FLOOD_OPS.
    let op_count = content.matches("op").count();
    assert!(
        op_count >= 2 * FLOOD_OPS,
        "expected ops from both clients ({} total), got {op_count}: {content}",
        2 * FLOOD_OPS,
    );
}

/// Sending ops to a document without subscribing first returns `NotFound`.
#[tokio::test]
async fn test_ops_without_subscribe_returns_not_found() {
    let addr = start_relay().await;
    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;

    // Send ops to a document nobody has subscribed to (no doc slot exists).
    let (ops, metadata) = make_edit("rogue", "should-fail", "no subscribe");
    client.send_ops("space", "doc", ops, metadata).await;

    let err = client.recv_error().await;
    assert_eq!(
        err.code,
        i32::from(sync::ErrorCode::NotFound),
        "expected NotFound error"
    );
}

/// In auth mode, subscribing without handshake (no token) is rejected.
#[tokio::test]
async fn test_subscribe_without_auth_rejected() {
    let (addr, _keys) = start_relay_with_auth().await;
    let mut client = TestClient::connect(&addr).await;

    // Send handshake without a token — should fail.
    client
        .send_envelope(&handshake_envelope("no-token-client"))
        .await;

    let err = client.recv_error().await;
    assert_eq!(
        err.code,
        i32::from(sync::ErrorCode::AuthFailed),
        "expected AuthFailed"
    );
}

/// Sending malformed (non-protobuf) binary data should not crash the relay.
#[tokio::test]
async fn test_malformed_binary_does_not_crash_relay() {
    let addr = start_relay().await;

    // Send garbage bytes.
    let url = format!("ws://{addr}/ws");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws.send(tungstenite::Message::Binary(
        vec![0xFF, 0xFE, 0x00, 0x01].into(),
    ))
    .await
    .unwrap();

    // Relay should still be alive — a different client can connect and work.
    // The connect+handshake+subscribe sequence is itself the liveness probe;
    // if the relay died from the garbage frame, connect would fail.
    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    let catch_up = client.subscribe("space", "doc").await;
    let catch_up = catch_up.expect("empty doc should still send SyncOps");
    assert!(
        catch_up.ops.is_empty(),
        "empty doc should have zero-length ops"
    );
}

/// When a slow client is evicted, healthy clients on the same document
/// continue receiving relays without interruption.
#[tokio::test]
async fn test_healthy_client_unaffected_by_stale_neighbor() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut healthy = connect_client(
        &mut relay,
        2,
        "healthy",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow = connect_client(
        &mut relay,
        3,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, healthy.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Drain the empty-doc catch-up SyncOps that subscribe sends.
    let _ = healthy.data_rx.try_recv();
    let _ = slow.data_rx.try_recv();

    // Writer sends FLOOD_OPS. Slow client's channel fills → eviction.
    // Healthy client should receive all ops.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Healthy client should have received all relays.
    let healthy_msgs = drain_data(&mut healthy.data_rx);
    let healthy_ops_count = healthy_msgs
        .iter()
        .filter(|e| matches!(e.payload, Some(Payload::SyncOps(_))))
        .count();
    assert_eq!(
        healthy_ops_count, FLOOD_OPS,
        "healthy client should receive all {FLOOD_OPS} relays, got {healthy_ops_count}"
    );

    // Slow client should have been evicted.
    let ctrl_messages = drain_ctrl(&mut slow.ctrl_rx);
    assert!(
        has_stale_notice(&ctrl_messages),
        "slow client should have been evicted"
    );

    // Slow client got at most SLOW_CHANNEL_CAPACITY data messages.
    let slow_data = drain_data(&mut slow.data_rx);
    assert!(
        slow_data.len() <= SLOW_CHANNEL_CAPACITY,
        "slow client should have at most {SLOW_CHANNEL_CAPACITY} messages"
    );

    // Healthy client should still receive relays AFTER slow client was evicted.
    send_test_ops(&mut relay, writer.conn_id, 1).await;

    let post_bytes = healthy
        .data_rx
        .try_recv()
        .expect("healthy client should still receive relays after neighbor's eviction");
    let post_env = decode_envelope(&post_bytes).unwrap();
    assert!(
        matches!(post_env.payload, Some(Payload::SyncOps(_))),
        "expected SyncOps"
    );
}

/// Eviction from one document does not affect subscriptions to other documents
/// on the same connection.
#[tokio::test]
async fn test_eviction_scoped_to_document() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut multi = connect_client(
        &mut relay,
        2,
        "multi",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    // Both subscribe to doc-a AND doc-b.
    for doc in ["doc-a", "doc-b"] {
        subscribe_to(&mut relay, writer.conn_id, TEST_SPACE, doc).await;
        subscribe_to(&mut relay, multi.conn_id, TEST_SPACE, doc).await;
    }

    // Flood doc-a to fill multi's channel → eviction from doc-a.
    send_test_ops_to(&mut relay, writer.conn_id, TEST_SPACE, "doc-a", FLOOD_OPS).await;

    // Multi client was evicted from doc-a. Drain its channels.
    drain_data(&mut multi.data_rx);

    let ctrl_messages = drain_ctrl(&mut multi.ctrl_rx);
    let evicted_from_a = ctrl_messages.iter().any(|e| {
        matches!(&e.payload, Some(Payload::StaleSubscriber(stale)) if stale.document_id == "doc-a")
    });
    assert!(evicted_from_a, "multi client should be evicted from doc-a");

    // Write to doc-b — multi client should still receive it.
    send_test_ops_to(&mut relay, writer.conn_id, TEST_SPACE, "doc-b", 1).await;

    let doc_b_bytes = multi
        .data_rx
        .try_recv()
        .expect("multi client should still receive doc-b relays after doc-a eviction");
    let env = decode_envelope(&doc_b_bytes).unwrap();
    match env.payload {
        Some(Payload::SyncOps(ref ops)) => {
            assert_eq!(ops.document_id, "doc-b", "relay should be for doc-b");
        }
        other => panic!("expected SyncOps for doc-b, got {other:?}"),
    }
}

/// Ops sent by a client that is being evicted are still merged into the relay's
/// authoritative document — eviction only stops outbound relays.
#[tokio::test]
async fn test_ops_from_evicted_client_still_merged() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut stale = connect_client(
        &mut relay,
        1,
        "stale",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let filler = connect_client(
        &mut relay,
        2,
        "filler",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, stale.conn_id).await;
    subscribe_to_doc(&mut relay, filler.conn_id).await;

    // Stale client sends ops BEFORE being evicted.
    let (ops, meta) = make_edit("stale-pre", "PRE-EVICTION", "before eviction");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: stale.conn_id,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata: meta,
                ..Default::default()
            }),
        })
        .await;

    // Filler floods to trigger stale client's eviction.
    send_test_ops(&mut relay, filler.conn_id, FLOOD_OPS).await;

    // Stale client sends ops AFTER being evicted — relay should still merge them.
    let (ops, meta) = make_edit("stale-post", "POST-EVICTION", "after eviction");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: stale.conn_id,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata: meta,
                ..Default::default()
            }),
        })
        .await;

    // Drain stale channels.
    drain_data(&mut stale.data_rx);
    drain_ctrl(&mut stale.ctrl_rx);

    // Verifier subscribes — should see ALL content including both stale client ops.
    let mut verifier = connect_client(
        &mut relay,
        3,
        "verifier",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, verifier.conn_id).await;

    let catch_up_bytes = verifier
        .data_rx
        .try_recv()
        .expect("verifier should get catch-up");
    let env = decode_envelope(&catch_up_bytes).unwrap();
    let catch_up = match env.payload {
        Some(Payload::SyncOps(ops)) => ops,
        other => panic!("expected SyncOps catch-up, got {other:?}"),
    };

    let content = apply_ops(&catch_up);
    assert!(
        content.contains("PRE-EVICTION"),
        "relay should have stale client's pre-eviction ops: {content}"
    );
    assert!(
        content.contains("POST-EVICTION"),
        "relay should have stale client's post-eviction ops: {content}"
    );
    assert!(
        content.contains("op0"),
        "relay should have filler ops: {content}"
    );
}

/// All subscribers are slow → all get evicted → relay still serves a new client.
///
/// This is the "100% bad clients" scenario at the channel level. Every
/// subscriber has `SLOW_CHANNEL_CAPACITY`, all get evicted, then a brand-new
/// client connects and gets full service.
#[tokio::test]
async fn test_all_subscribers_slow_relay_serves_new_client() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow_clients = Vec::new();
    for i in 0..ALL_SLOW_SUBSCRIBER_COUNT {
        let conn_id = (i + 2) as u64;
        slow_clients.push(
            connect_client(
                &mut relay,
                conn_id,
                &format!("slow-{i}"),
                SLOW_CHANNEL_CAPACITY,
                TEST_CTRL_CAPACITY,
            )
            .await,
        );
    }

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    for c in &slow_clients {
        subscribe_to_doc(&mut relay, c.conn_id).await;
    }

    // Flood — don't drain any slow client.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Each slow client should have a StaleSubscriber notice (all evicted).
    for c in &mut slow_clients {
        let ctrl = drain_ctrl(&mut c.ctrl_rx);
        assert!(
            has_stale_notice(&ctrl),
            "slow client {} should have been evicted",
            c.conn_id,
        );
    }

    // One more op — should reach nobody (subscriber list cleared).
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    for c in &mut slow_clients {
        drain_data(&mut c.data_rx);
        assert!(
            c.data_rx.try_recv().is_err(),
            "slow client {} should not receive post-eviction op",
            c.conn_id,
        );
    }

    // New fast client connects, subscribes, gets catch-up.
    let new_conn_id = (ALL_SLOW_SUBSCRIBER_COUNT + 2) as u64;
    let mut newcomer = connect_client(
        &mut relay,
        new_conn_id,
        "newcomer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, newcomer.conn_id).await;

    let catch_up = drain_data(&mut newcomer.data_rx);
    assert!(
        !catch_up.is_empty(),
        "newcomer should receive catch-up SyncOps"
    );
    assert!(
        catch_up
            .iter()
            .any(|e| matches!(e.payload, Some(Payload::SyncOps(_)))),
        "catch-up should contain SyncOps"
    );

    // Writer sends another op — newcomer receives it.
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    let relay = newcomer
        .data_rx
        .try_recv()
        .expect("newcomer should receive relayed op");
    let env = decode_envelope(&relay).unwrap();
    assert!(
        matches!(env.payload, Some(Payload::SyncOps(_))),
        "expected SyncOps relay"
    );
}

/// Disconnect after eviction cleans up all internal state — no dangling
/// entries in connections, authenticated, `subscribe_history`, or subscriber maps.
///
/// Proven by: reusing the same `conn_id` after disconnect succeeds.
#[tokio::test]
async fn test_disconnect_after_eviction_cleans_up() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow = connect_client(
        &mut relay,
        2,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Flood → eviction.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    let ctrl = drain_ctrl(&mut slow.ctrl_rx);
    assert!(has_stale_notice(&ctrl), "slow client should be evicted");

    // Disconnect the evicted slow client.
    relay
        .process_command(RelayCommand::Disconnect {
            conn_id: slow.conn_id,
        })
        .await;

    // Reuse the same conn_id with fresh channels.
    let mut reused = connect_client(
        &mut relay,
        2,
        "reused",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, reused.conn_id).await;

    // Should get catch-up.
    let catch_up = drain_data(&mut reused.data_rx);
    assert!(
        catch_up
            .iter()
            .any(|e| matches!(e.payload, Some(Payload::SyncOps(_)))),
        "reused conn should receive catch-up"
    );

    // Writer sends another op — reused conn receives relay.
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    let relay_bytes = reused
        .data_rx
        .try_recv()
        .expect("reused conn should receive relayed op");
    let env = decode_envelope(&relay_bytes).unwrap();
    assert!(
        matches!(env.payload, Some(Payload::SyncOps(_))),
        "expected SyncOps relay"
    );

    // Second round: disconnect and reuse again to prove cleanup is thorough.
    relay
        .process_command(RelayCommand::Disconnect {
            conn_id: reused.conn_id,
        })
        .await;

    let mut reused2 = connect_client(
        &mut relay,
        2,
        "reused2",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, reused2.conn_id).await;

    let catch_up2 = drain_data(&mut reused2.data_rx);
    assert!(
        catch_up2
            .iter()
            .any(|e| matches!(e.payload, Some(Payload::SyncOps(_)))),
        "second reuse should also receive catch-up"
    );
}

/// Ctrl channel full — relay continues without crashing.
///
/// When the ctrl channel is full, the relay silently drops the stale notice
/// (via `let _ = try_send(...)`) but still evicts the subscriber. Proves
/// ctrl backpressure doesn't crash the relay or prevent eviction.
#[tokio::test]
async fn test_ctrl_channel_full_relay_continues() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    // Slow client with TINY ctrl — handshake ack fills the 1 slot.
    let mut slow = connect_client_keep_ack(
        &mut relay,
        2,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TINY_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Flood → data channel fills → eviction → ctrl try_send fails silently.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Ctrl channel should contain exactly 1 message (the handshake ack, NOT stale notice).
    let ctrl = drain_ctrl(&mut slow.ctrl_rx);
    assert_eq!(
        ctrl.len(),
        1,
        "ctrl should have exactly 1 message (handshake ack)"
    );
    assert!(
        matches!(ctrl[0].payload, Some(Payload::HandshakeAck(_))),
        "the only ctrl message should be the handshake ack"
    );

    // Eviction still happened: no more data after draining.
    drain_data(&mut slow.data_rx);
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    assert!(
        slow.data_rx.try_recv().is_err(),
        "slow client should not receive post-eviction op (eviction happened despite ctrl being full)"
    );

    // Relay is still functional: new subscriber gets full service.
    let mut newcomer = connect_client(
        &mut relay,
        3,
        "newcomer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, newcomer.conn_id).await;

    let catch_up = drain_data(&mut newcomer.data_rx);
    assert!(
        catch_up
            .iter()
            .any(|e| matches!(e.payload, Some(Payload::SyncOps(_)))),
        "newcomer should receive catch-up"
    );

    send_test_ops(&mut relay, writer.conn_id, 1).await;
    let relay_bytes = newcomer
        .data_rx
        .try_recv()
        .expect("newcomer should receive relayed op");
    let env = decode_envelope(&relay_bytes).unwrap();
    assert!(
        matches!(env.payload, Some(Payload::SyncOps(_))),
        "expected SyncOps relay"
    );
}

// ---------------------------------------------------------------------------
// Signal tests (relay-only, standalone relay)
// ---------------------------------------------------------------------------

/// Build a `Signal` message with a flag payload for use in relay commands.
fn make_flag_signal(
    space_id: &str,
    document_id: &str,
    author_did: &str,
    kind: i32,
    audience: i32,
    message: &str,
    target_did: &str,
) -> sync::Signal {
    sync::Signal {
        id: String::new(),
        space_id: space_id.to_owned(),
        document_id: Some(document_id.to_owned()),
        author_did: author_did.to_owned(),
        timestamp: 1_700_000_000_000,
        payload: Some(sync::signal::Payload::Flag(sync::FlagPayload {
            kind,
            audience_type: audience,
            target_did: if target_did.is_empty() {
                None
            } else {
                Some(target_did.to_owned())
            },
            message: message.to_owned(),
        })),
    }
}

/// Drain a `Signal` from a ctrl channel, skipping any `SubscribeStatus`
/// envelopes that may precede it (RFD 0066 adds these on every subscribe).
async fn recv_signal(ctrl_rx: &mut mpsc::Receiver<Vec<u8>>) -> sync::Signal {
    for _ in 0..4 {
        let bytes = tokio::time::timeout(Duration::from_secs(1), ctrl_rx.recv())
            .await
            .expect("timeout waiting for signal")
            .expect("channel closed");
        let envelope = decode_envelope(&bytes).unwrap();
        match envelope.payload {
            Some(Payload::Signal(s)) => return s,
            Some(Payload::SubscribeStatus(_)) => {}
            other => panic!("expected Signal, got {other:?}"),
        }
    }
    panic!("too many SubscribeStatus envelopes before Signal");
}

#[tokio::test]
async fn test_signal_flag_delivered() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut b = connect_client(
        &mut relay,
        2,
        "b",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;
    common::drain_subscribe_status(&mut a.ctrl_rx).await;
    common::drain_subscribe_status(&mut b.ctrl_rx).await;

    // A sends flag signal with SPACE audience.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::ReviewRequested),
        i32::from(sync::AudienceType::Space),
        "please review",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // B should receive it via ctrl channel.
    let signal = recv_signal(&mut b.ctrl_rx).await;
    assert_eq!(signal.space_id, TEST_SPACE);
    assert_eq!(signal.document_id.as_deref(), Some(TEST_DOC));
    assert_eq!(signal.author_did, "did:key:zAlice");
    match &signal.payload {
        Some(sync::signal::Payload::Flag(f)) => {
            assert_eq!(f.kind, i32::from(sync::FlagKind::ReviewRequested));
            assert_eq!(f.message, "please review");
        }
        other => panic!("expected Flag payload, got {other:?}"),
    }

    // A should NOT receive it (sender excluded).
    assert!(a.ctrl_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_signal_flag_sender_excluded() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;
    common::drain_subscribe_status(&mut a.ctrl_rx).await;

    // A sends flag signal — no other subscribers.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::Completed),
        i32::from(sync::AudienceType::Space),
        "done",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // A should not receive their own signal.
    assert!(a.ctrl_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_signal_unspecified_audience_rejected() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;
    common::drain_subscribe_status(&mut a.ctrl_rx).await;

    // Send with UNSPECIFIED audience.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::Blocked),
        i32::from(sync::AudienceType::Unspecified),
        "help",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // Should get an error back.
    let bytes = a.ctrl_rx.try_recv().expect("should receive error");
    let envelope = decode_envelope(&bytes).unwrap();
    match envelope.payload {
        Some(Payload::Error(e)) => {
            assert_eq!(e.code, i32::from(sync::ErrorCode::InvalidMessage));
            assert!(e.message.contains("audience"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_signal_participant_needs_target() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;
    common::drain_subscribe_status(&mut a.ctrl_rx).await;

    // Send PARTICIPANT without target_did.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::Question),
        i32::from(sync::AudienceType::Participant),
        "need input",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // Should get an error back.
    let bytes = a.ctrl_rx.try_recv().expect("should receive error");
    let envelope = decode_envelope(&bytes).unwrap();
    match envelope.payload {
        Some(Payload::Error(e)) => {
            assert_eq!(e.code, i32::from(sync::ErrorCode::InvalidMessage));
            assert!(e.message.contains("target_did"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_signal_flag_cross_document() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut b = connect_client(
        &mut relay,
        2,
        "b",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    // A subscribes to doc1, B subscribes to doc2 — same space.
    subscribe_to(&mut relay, 1, TEST_SPACE, TEST_DOC_1).await;
    subscribe_to(&mut relay, 2, TEST_SPACE, TEST_DOC_2).await;
    common::drain_subscribe_status(&mut a.ctrl_rx).await;
    common::drain_subscribe_status(&mut b.ctrl_rx).await;

    // A sends flag signal scoped to the space.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC_1,
        "did:key:zAlice",
        i32::from(sync::FlagKind::ReviewRequested),
        i32::from(sync::AudienceType::Space),
        "review doc1",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // B receives it even though subscribed to a different doc.
    let signal = recv_signal(&mut b.ctrl_rx).await;
    assert_eq!(signal.document_id.as_deref(), Some(TEST_DOC_1));
    match &signal.payload {
        Some(sync::signal::Payload::Flag(f)) => {
            assert_eq!(f.message, "review doc1");
        }
        other => panic!("expected Flag payload, got {other:?}"),
    }

    // A should not receive it.
    assert!(a.ctrl_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_signal_flag_space_sentinel() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut b = connect_client(
        &mut relay,
        2,
        "b",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;
    common::drain_subscribe_status(&mut a.ctrl_rx).await;
    common::drain_subscribe_status(&mut b.ctrl_rx).await;

    // Space-audience flag attached to a real doc UUID. The legacy
    // `"_space"` sentinel was a transitional placeholder when
    // document_id was a free-form string column; with the column
    // typed as uuid in production (RFD 0042), space-wide signals
    // either ride a real doc or use a `None` document_id at the
    // proto level. The relay's broadcast behavior — fan to every
    // subscriber in the space regardless of which doc they're
    // subscribed to — is what this test pins, not the sentinel
    // string itself.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::Info),
        i32::from(sync::AudienceType::Space),
        "space-level notice",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // B receives it because audience is SPACE — fan-out to every
    // subscriber in the space, independent of doc subscription.
    let signal = recv_signal(&mut b.ctrl_rx).await;
    assert_eq!(signal.document_id.as_deref(), Some(TEST_DOC));
    match &signal.payload {
        Some(sync::signal::Payload::Flag(f)) => {
            assert_eq!(f.kind, i32::from(sync::FlagKind::Info));
            assert_eq!(f.message, "space-level notice");
        }
        other => panic!("expected Flag payload, got {other:?}"),
    }

    // A should not receive it.
    assert!(a.ctrl_rx.try_recv().is_err());
}

/// A relay config copy with `require_auth` enabled so
/// `authoritative_author_did` activates.
/// Authorized-keys file shared by all standalone relay auth tests.
/// Populated per-test via `connect_preauth` path (`authorized_keys.rs`
/// re-reads on every check).
static AUTH_KEYS: std::sync::LazyLock<tempfile::NamedTempFile> = std::sync::LazyLock::new(|| {
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    writeln!(f, "did:key:zAlice").unwrap();
    writeln!(f, "did:key:zBob").unwrap();
    writeln!(f, "did:key:zCharlie").unwrap();
    f
});

fn test_relay_config_with_auth() -> RelayConfig {
    let mut config = test_relay_config();
    config.require_auth = true;
    config.authorized_keys_file = Some(AUTH_KEYS.path().to_path_buf());
    config
}

/// Connect a client without running the handshake, then inject an
/// authenticated identity. Needed for `require_auth=true` actor-level
/// tests — the real handshake path requires a live HTTP auth flow.
async fn connect_preauth(
    relay: &mut Relay,
    conn_id: u64,
    identity: &str,
    data_capacity: usize,
    ctrl_capacity: usize,
) -> TestConn {
    let (tx, data_rx) = mpsc::channel(data_capacity);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(ctrl_capacity);
    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx,
            ctrl_tx,
        })
        .await;
    relay.test_set_authenticated(conn_id, identity);
    TestConn {
        conn_id,
        data_rx,
        ctrl_rx,
    }
}

/// Regression for the WS signal spoofing vector. An authenticated peer must
/// not be able to broadcast a flag with someone else's `author_did` — the
/// relay overwrites the client-supplied field with the connection's real
/// authenticated identity.
#[tokio::test]
async fn test_signal_flag_author_did_pinned_to_auth_identity() {
    let mut relay = Relay::new_standalone(test_relay_config_with_auth());

    let mut alice = connect_preauth(
        &mut relay,
        1,
        "did:key:zAlice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut bob = connect_preauth(
        &mut relay,
        2,
        "did:key:zBob",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;
    common::drain_subscribe_status(&mut alice.ctrl_rx).await;
    common::drain_subscribe_status(&mut bob.ctrl_rx).await;

    // Alice crafts a signal claiming to be Charlie.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zCharlie",
        i32::from(sync::FlagKind::Info),
        i32::from(sync::AudienceType::Space),
        "forged signal",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // Bob receives the signal with Alice's real DID, not Charlie's.
    let signal = recv_signal(&mut bob.ctrl_rx).await;
    assert_eq!(
        signal.author_did, "did:key:zAlice",
        "relay must rewrite `author_did` to the sender's authenticated identity"
    );

    // Alice (sender) is still excluded from the broadcast.
    assert!(alice.ctrl_rx.try_recv().is_err());
}

/// Even when the signal is addressed to a specific participant
/// (`AudienceType::Participant`), the author must be the real sender — a
/// spoofed DM would otherwise let a peer impersonate arbitrary senders to
/// a target.
#[tokio::test]
async fn test_signal_flag_participant_dm_author_did_pinned() {
    let mut relay = Relay::new_standalone(test_relay_config_with_auth());

    let mut alice = connect_preauth(
        &mut relay,
        1,
        "did:key:zAlice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut bob = connect_preauth(
        &mut relay,
        2,
        "did:key:zBob",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;
    common::drain_subscribe_status(&mut alice.ctrl_rx).await;
    common::drain_subscribe_status(&mut bob.ctrl_rx).await;

    // Alice tries to DM Bob while impersonating Charlie.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zCharlie",
        i32::from(sync::FlagKind::Info),
        i32::from(sync::AudienceType::Participant),
        "pretend I'm charlie",
        "did:key:zBob",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    let signal = recv_signal(&mut bob.ctrl_rx).await;
    assert_eq!(
        signal.author_did, "did:key:zAlice",
        "DM author must match authenticated sender"
    );

    // Alice does receive a copy in participant mode (she is an endpoint of
    // the DM), but the `author_did` there also matches her real identity.
    let echo = recv_signal(&mut alice.ctrl_rx).await;
    assert_eq!(echo.author_did, "did:key:zAlice");
}

/// An authenticated peer must not be able to broadcast a cursor position
/// labelled as a different user via `PresenceUpdate.participant_did`.
#[tokio::test]
async fn test_presence_update_participant_did_pinned() {
    let mut relay = Relay::new_standalone(test_relay_config_with_auth());

    let _alice = connect_preauth(
        &mut relay,
        1,
        "did:key:zAlice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut bob = connect_preauth(
        &mut relay,
        2,
        "did:key:zBob",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;

    // Drain the subscribe catch-up SyncOps envelopes so the next recv
    // delivers the presence update we're testing for.
    while bob.data_rx.try_recv().is_ok() {}

    // Alice sends presence forging Charlie's DID.
    let msg = sync::PresenceUpdate {
        space_id: TEST_SPACE.into(),
        document_id: TEST_DOC.into(),
        participant_did: "did:key:zCharlie".into(),
        cursor_pos: 7,
        custom_data: Vec::new(),
    };
    relay
        .process_command(RelayCommand::PresenceUpdate { conn_id: 1, msg })
        .await;

    // Bob receives a presence envelope on the data channel.
    let bytes = tokio::time::timeout(Duration::from_secs(1), bob.data_rx.recv())
        .await
        .expect("presence timed out")
        .expect("channel closed");
    let env = decode_envelope(&bytes).unwrap();
    match env.payload {
        Some(Payload::PresenceUpdate(p)) => {
            assert_eq!(
                p.participant_did, "did:key:zAlice",
                "presence participant_did must be rewritten to sender's real DID"
            );
            assert_eq!(p.cursor_pos, 7, "cursor position should pass through");
        }
        other => panic!("expected PresenceUpdate, got {other:?}"),
    }
}

#[tokio::test]
async fn test_list_space_documents_returns_active_entries() {
    let config = test_relay_config();
    let mut relay = Relay::new_standalone(config);

    let mut conn = connect_client(
        &mut relay,
        1,
        "client-1",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    let space = "space-1";

    // Register three documents via relay commands.
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: space.into(),
                document_id: "doc-a".into(),
                path: "a.md".into(),
                metadata: None,
            },
        })
        .await;
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: space.into(),
                document_id: "doc-b".into(),
                path: "b.md".into(),
                metadata: None,
            },
        })
        .await;
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: space.into(),
                document_id: "doc-c".into(),
                path: "c.md".into(),
                metadata: None,
            },
        })
        .await;

    // Soft-delete doc-b.
    relay
        .process_command(RelayCommand::UnregisterDocument {
            conn_id: 1,
            msg: sync::UnregisterDocument {
                space_id: space.into(),
                document_id: "doc-b".into(),
                metadata: None,
            },
        })
        .await;

    // Drain any ctrl messages from registration/unregistration.
    while conn.ctrl_rx.try_recv().is_ok() {}

    // Send ListSpaceDocuments.
    relay
        .process_command(RelayCommand::ListSpaceDocuments {
            conn_id: 1,
            msg: sync::ListSpaceDocuments {
                space_id: space.into(),
            },
        })
        .await;

    // Read the response from ctrl channel.
    let bytes = conn.ctrl_rx.recv().await.expect("expected response");
    let envelope = decode_envelope(&bytes).unwrap();
    match envelope.payload {
        Some(Payload::ListSpaceDocumentsResult(result)) => {
            assert_eq!(result.space_id, space);
            assert_eq!(result.documents.len(), 2, "should have 2 active docs");
            let ids: Vec<&str> = result
                .documents
                .iter()
                .map(|d| d.document_id.as_str())
                .collect();
            assert!(ids.contains(&"doc-a"), "should contain doc-a");
            assert!(ids.contains(&"doc-c"), "should contain doc-c");
            assert!(!ids.contains(&"doc-b"), "should not contain deleted doc-b");
        }
        other => panic!("expected ListSpaceDocumentsResult, got {other:?}"),
    }
}

#[tokio::test]
async fn test_list_space_documents_empty_registry() {
    let config = test_relay_config();
    let mut relay = Relay::new_standalone(config);

    let mut conn = connect_client(
        &mut relay,
        1,
        "client-1",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    // List docs on a space with no registry entries.
    relay
        .process_command(RelayCommand::ListSpaceDocuments {
            conn_id: 1,
            msg: sync::ListSpaceDocuments {
                space_id: "empty-space".into(),
            },
        })
        .await;

    let bytes = conn.ctrl_rx.recv().await.expect("expected response");
    let envelope = decode_envelope(&bytes).unwrap();
    match envelope.payload {
        Some(Payload::ListSpaceDocumentsResult(result)) => {
            assert_eq!(result.space_id, "empty-space");
            assert!(result.documents.is_empty(), "should be empty");
        }
        other => panic!("expected ListSpaceDocumentsResult, got {other:?}"),
    }
}

#[tokio::test]
async fn test_fresh_client_discovers_documents_via_list() {
    let addr = start_relay().await;

    // Client 1 connects and registers two documents.
    let mut c1 = TestClient::connect(&addr).await;
    c1.handshake().await;

    let space = "test-space";

    // Register doc-a.
    let reg_a = sync::SyncEnvelope {
        payload: Some(Payload::RegisterDocument(sync::RegisterDocument {
            space_id: space.into(),
            document_id: "doc-a".into(),
            path: "a.md".into(),
            metadata: None,
        })),
    };
    c1.send_envelope(&reg_a).await;

    // Subscribe + push ops for doc-a so it has content.
    c1.subscribe(space, "doc-a").await;
    let (ops, metadata) = make_edit("c1", "hello", "seed doc-a");
    c1.send_ops(space, "doc-a", ops, metadata).await;

    // Register doc-b.
    let reg_b = sync::SyncEnvelope {
        payload: Some(Payload::RegisterDocument(sync::RegisterDocument {
            space_id: space.into(),
            document_id: "doc-b".into(),
            path: "b.md".into(),
            metadata: None,
        })),
    };
    c1.send_envelope(&reg_b).await;

    // Wait for both registrations to land before listing.
    wait_for_doc_count(&addr, space, 2).await;

    // Client 2 connects fresh (simulates post-clone daemon).
    let mut c2 = TestClient::connect(&addr).await;
    c2.handshake().await;

    // Client 2 sends ListSpaceDocuments.
    let list_req = sync::SyncEnvelope {
        payload: Some(Payload::ListSpaceDocuments(sync::ListSpaceDocuments {
            space_id: space.into(),
        })),
    };
    c2.send_envelope(&list_req).await;

    // Client 2 receives the result.
    let result = c2.recv_envelope().await;
    match result.payload {
        Some(Payload::ListSpaceDocumentsResult(r)) => {
            assert_eq!(r.space_id, space);
            assert_eq!(r.documents.len(), 2);
            let ids: Vec<&str> = r.documents.iter().map(|d| d.document_id.as_str()).collect();
            assert!(ids.contains(&"doc-a"));
            assert!(ids.contains(&"doc-b"));
        }
        other => panic!("expected ListSpaceDocumentsResult, got {other:?}"),
    }

    // Client 2 subscribes to doc-a and gets catch-up ops.
    let catchup = c2.subscribe(space, "doc-a").await;
    assert!(catchup.is_some(), "should receive catch-up ops for doc-a");
}

#[tokio::test]
async fn test_live_peer_document_registration_broadcast() {
    let addr = start_relay().await;
    let space = "union-space";

    // Client 1 connects, registers doc-a, subscribes.
    let mut c1 = TestClient::connect(&addr).await;
    c1.handshake().await;

    let reg_a = sync::SyncEnvelope {
        payload: Some(Payload::RegisterDocument(sync::RegisterDocument {
            space_id: space.into(),
            document_id: "doc-a".into(),
            path: "a.md".into(),
            metadata: None,
        })),
    };
    c1.send_envelope(&reg_a).await;
    c1.subscribe(space, "doc-a").await;

    // Client 2 connects, registers doc-b.
    let mut c2 = TestClient::connect(&addr).await;
    c2.handshake().await;

    // Client 2 first discovers doc-a via ListSpaceDocuments.
    let list_req = sync::SyncEnvelope {
        payload: Some(Payload::ListSpaceDocuments(sync::ListSpaceDocuments {
            space_id: space.into(),
        })),
    };
    c2.send_envelope(&list_req).await;
    let result = c2.recv_envelope().await;
    match result.payload {
        Some(Payload::ListSpaceDocumentsResult(r)) => {
            assert_eq!(r.documents.len(), 1);
            assert_eq!(r.documents[0].document_id, "doc-a");
        }
        other => panic!("expected ListSpaceDocumentsResult, got {other:?}"),
    }

    // Client 2 subscribes to doc-a (discovered) and doc-b (own).
    c2.subscribe(space, "doc-a").await;
    c2.subscribe(space, "doc-b").await;

    // Client 2 registers doc-b.
    let reg_b = sync::SyncEnvelope {
        payload: Some(Payload::RegisterDocument(sync::RegisterDocument {
            space_id: space.into(),
            document_id: "doc-b".into(),
            path: "b.md".into(),
            metadata: None,
        })),
    };
    c2.send_envelope(&reg_b).await;

    // Client 1 should receive the RegisterDocument broadcast for doc-b.
    let broadcast = c1.recv_envelope().await;
    match broadcast.payload {
        Some(Payload::RegisterDocument(msg)) => {
            assert_eq!(msg.document_id, "doc-b");
            assert_eq!(msg.path, "b.md");
        }
        other => panic!("expected RegisterDocument broadcast, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Observer / snippet tests
// ---------------------------------------------------------------------------

/// Test after-merge observer that records all emitted text merge events.
struct CapturingAfterMerge {
    events: Mutex<Vec<MergedEvent>>,
}

impl CapturingAfterMerge {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    fn events(&self) -> Vec<MergedEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl AfterMergeObserver for CapturingAfterMerge {
    fn after_text_merge(&self, event: MergedEvent, _doc: &kutl_core::Document) {
        self.events.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn test_client_provided_snippet_triggers_immediate_after_merge() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    let _conn = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;

    // Send ops with a client-provided snippet in ChangeMetadata.
    let (ops, mut metadata) = make_edit("alice", "hello world", "add greeting");
    metadata[0].change_snippet = "Added a greeting to the document".into();

    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata,
                ..Default::default()
            }),
        })
        .await;

    // The after-merge observer should have received exactly one event
    // (client-provided snippet bypasses the debounce window).
    let events = after.events();
    assert_eq!(events.len(), 1, "expected exactly one after-merge event");
    assert_eq!(events[0].author_did, "alice");
    assert_eq!(events[0].intent, "add greeting");
}

#[tokio::test]
async fn test_edit_without_snippet_deferred_during_debounce() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    let _conn = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;

    // Send ops WITHOUT a client-provided snippet.
    let (ops, metadata) = make_edit("alice", "hello world", "add greeting");

    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata,
                ..Default::default()
            }),
        })
        .await;

    // The edit event is held in pending_edit during the debounce window —
    // no event is emitted immediately. The timer would fire later and
    // emit the event, but standalone mode has no self_tx so the timer
    // is never started.
    let events = after.events();
    assert!(
        events.is_empty(),
        "snippet-eligible edit should be deferred, not emitted immediately"
    );
}

#[tokio::test]
async fn test_deferred_edit_flushed_on_timer() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    let _conn = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;

    // Send ops WITHOUT a client-provided snippet.
    let (ops, metadata) = make_edit("alice", "hello world", "add greeting");

    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata,
                ..Default::default()
            }),
        })
        .await;

    // No event emitted yet — edit is held pending debounce.
    assert!(
        after.events().is_empty(),
        "snippet-eligible edit should be deferred, not emitted immediately"
    );

    // Manually flush the pending edit (simulates timer firing).
    relay
        .process_command(RelayCommand::FlushPendingEdit {
            space_id: TEST_SPACE.into(),
            document_id: TEST_DOC.into(),
        })
        .await;

    let events = after.events();
    assert_eq!(events.len(), 1, "expected one event after flush");
    assert_eq!(events[0].author_did, "alice");
    assert_eq!(events[0].intent, "add greeting");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_different_author_flushes_pending_edit() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    let _c1 = connect_client(
        &mut relay,
        1,
        "alice-client",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let _c2 = connect_client(
        &mut relay,
        2,
        "bob-client",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;

    // Alice edits — should be held (no event yet).
    let (ops_a, meta_a) = make_edit("alice", "hello from alice", "alice writes");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: ops_a,
                metadata: meta_a,
                ..Default::default()
            }),
        })
        .await;

    assert!(after.events().is_empty(), "alice's edit should be deferred");

    // Bob edits same document — should flush alice's pending edit.
    let (ops_b, meta_b) = make_edit("bob", "hello from bob", "bob writes");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 2,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: ops_b,
                metadata: meta_b,
                ..Default::default()
            }),
        })
        .await;

    // Alice's event should have been flushed by the after-merge observer.
    let events = after.events();
    let alice_events: Vec<_> = events
        .iter()
        .filter(|ev| ev.author_did == "alice")
        .collect();

    assert_eq!(alice_events.len(), 1, "alice's edit should be flushed");

    // Bob's edit is now pending (not yet emitted).
    let bob_events: Vec<_> = events.iter().filter(|ev| ev.author_did == "bob").collect();
    assert!(bob_events.is_empty(), "bob's edit should be deferred");

    // Flush bob's pending edit.
    relay
        .process_command(RelayCommand::FlushPendingEdit {
            space_id: TEST_SPACE.into(),
            document_id: TEST_DOC.into(),
        })
        .await;

    let events = after.events();
    let bob_events: Vec<_> = events.iter().filter(|ev| ev.author_did == "bob").collect();
    assert_eq!(bob_events.len(), 1, "bob's edit should be flushed");
}

#[tokio::test]
async fn test_same_author_accumulates_pending_edit() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    let _conn = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;

    // First edit by alice.
    let (ops1, meta1) = make_edit("alice", "first edit here!", "first");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: ops1,
                metadata: meta1,
                ..Default::default()
            }),
        })
        .await;

    // Second edit by alice (same author — should accumulate).
    let (ops2, meta2) = make_edit("alice", "second edit here!", "second");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: ops2,
                metadata: meta2,
                ..Default::default()
            }),
        })
        .await;

    // No events yet — both are accumulated.
    assert!(after.events().is_empty(), "edits should be deferred");

    // Flush.
    relay
        .process_command(RelayCommand::FlushPendingEdit {
            space_id: TEST_SPACE.into(),
            document_id: TEST_DOC.into(),
        })
        .await;

    let events = after.events();
    assert_eq!(events.len(), 1, "should emit one accumulated event");
    assert_eq!(events[0].op_count, 2, "op_count should be accumulated");
    assert_eq!(events[0].intent, "second", "intent should be latest");
}

/// An authenticated client whose DID is not in the `authorized_keys` file
/// should be rejected when sending `SyncOps` to a loaded document.
#[tokio::test]
async fn test_sync_ops_without_authorization_rejected() {
    // Generate two distinct keypairs.
    let (did_a, key_a) = common::test_keypair();
    let (did_b, key_b) = common::test_keypair();

    // Write an authorized_keys file containing only Client A's DID.
    let keys_dir = tempfile::tempdir().unwrap();
    let keys_path = keys_dir.path().join("authorized_keys");
    std::fs::write(&keys_path, &did_a).unwrap();

    // Start relay with require_auth and authorized_keys.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut config = test_relay_config();
    config.require_auth = true;
    config.authorized_keys_file = Some(keys_path);

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Both clients authenticate (obtain tokens).
    let token_a = authenticate(&addr, &did_a, &key_a).await;
    let token_b = authenticate(&addr, &did_b, &key_b).await;

    // Client A: handshake + subscribe to "allowed/doc" — should succeed.
    let mut client_a = TestClient::connect(&addr).await;
    client_a
        .send_envelope(&handshake_envelope_with_token("client-a", &token_a, ""))
        .await;
    let ack = client_a.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "client A handshake should succeed"
    );
    let _catch_up = client_a.subscribe("allowed", "doc").await;

    // Client B: handshake succeeds (token is valid), but DID is not authorized.
    let mut client_b = TestClient::connect(&addr).await;
    client_b
        .send_envelope(&handshake_envelope_with_token("client-b", &token_b, ""))
        .await;
    let ack = client_b.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "client B handshake should succeed (token is valid)"
    );

    // Client B sends SyncOps to "allowed/doc" — should be rejected (DID not authorized).
    let (ops, metadata) = make_edit("intruder", "malicious edit", "attack");
    client_b.send_ops("allowed", "doc", ops, metadata).await;

    let err = client_b.recv_error().await;
    assert_eq!(
        err.code,
        i32::from(sync::ErrorCode::AuthFailed),
        "unauthorized SyncOps should be rejected with AuthFailed"
    );
}

/// Verify that an unauthorized client's `PresenceUpdate` is silently dropped
/// and does NOT reach subscribers.
#[tokio::test]
async fn test_presence_update_without_authorization_dropped() {
    // Generate two distinct keypairs.
    let (did_a, key_a) = common::test_keypair();
    let (did_b, key_b) = common::test_keypair();

    // Authorized_keys contains only Client A's DID.
    let keys_dir = tempfile::tempdir().unwrap();
    let keys_path = keys_dir.path().join("authorized_keys");
    std::fs::write(&keys_path, &did_a).unwrap();

    // Start relay with require_auth and authorized_keys.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut config = test_relay_config();
    config.require_auth = true;
    config.authorized_keys_file = Some(keys_path);

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Both clients authenticate (obtain tokens).
    let token_a = authenticate(&addr, &did_a, &key_a).await;
    let token_b = authenticate(&addr, &did_b, &key_b).await;

    // Client A: handshake + subscribe to "allowed/doc".
    let mut client_a = TestClient::connect(&addr).await;
    client_a
        .send_envelope(&handshake_envelope_with_token("client-a", &token_a, ""))
        .await;
    let ack = client_a.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "client A handshake should succeed"
    );
    let _catch_up = client_a.subscribe("allowed", "doc").await;

    // Client B: handshake succeeds (token is valid), but DID is not authorized.
    let mut client_b = TestClient::connect(&addr).await;
    client_b
        .send_envelope(&handshake_envelope_with_token("client-b", &token_b, ""))
        .await;
    let ack = client_b.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "client B handshake should succeed (token is valid)"
    );

    // Client B sends a PresenceUpdate for "allowed/doc" — should be silently dropped.
    client_b
        .send_envelope(&presence_update_envelope(
            "allowed",
            "doc",
            &did_b,
            42,
            vec![],
        ))
        .await;

    // Give the relay time to process the presence update (if it were forwarded).
    // Then verify Client A received nothing — the unauthorized presence was dropped.
    match tokio::time::timeout(Duration::from_millis(500), client_a.ws.next()).await {
        Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => {
            let envelope = decode_envelope(&bytes).unwrap();
            panic!(
                "client A should NOT receive any message from unauthorized client; got {envelope:?}"
            );
        }
        Err(_) => {} // timeout — expected, presence was dropped
        other => panic!("unexpected WebSocket result: {other:?}"),
    }
}
