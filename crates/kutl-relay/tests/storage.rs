//! Integration tests for relay storage: persistence via flush, reload on subscribe,
//! and the eviction + reload cycle.
//!
//! These tests exercise the full round-trip through a real axum relay (with
//! WebSocket connections and the background flush task) using in-memory backends.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    decode_envelope, encode_envelope, handshake_envelope, subscribe_envelope, sync_ops_envelope,
};
use kutl_proto::sync::{self, sync_envelope::Payload};
use reqwest::header;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite;

use kutl_relay::blob_backend::{BlobBackend, BlobRecord};
use kutl_relay::config::RelayConfig;
use kutl_relay::content_backend::ContentBackend;

// ---------------------------------------------------------------------------
// In-memory test backends
// ---------------------------------------------------------------------------

/// In-memory content backend for integration tests.
///
/// Uses `std::sync::Mutex<HashMap>` for simplicity. The `Arc` wrapper is
/// applied externally so the test can inspect stored content.
struct InMemoryContentBackend {
    store: Mutex<HashMap<(String, String), Vec<u8>>>,
}

impl InMemoryContentBackend {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    /// Return the number of stored documents.
    fn len(&self) -> usize {
        self.store.lock().unwrap().len()
    }

    /// Retrieve stored content for a (space, doc) pair.
    fn get(&self, space_id: &str, doc_id: &str) -> Option<Vec<u8>> {
        self.store
            .lock()
            .unwrap()
            .get(&(space_id.to_owned(), doc_id.to_owned()))
            .cloned()
    }
}

#[async_trait::async_trait]
impl ContentBackend for InMemoryContentBackend {
    async fn load(&self, space_id: &str, doc_id: &str) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self
            .store
            .lock()
            .unwrap()
            .get(&(space_id.to_owned(), doc_id.to_owned()))
            .cloned())
    }

    async fn save(&self, space_id: &str, doc_id: &str, data: &[u8]) -> anyhow::Result<()> {
        self.store
            .lock()
            .unwrap()
            .insert((space_id.to_owned(), doc_id.to_owned()), data.to_vec());
        Ok(())
    }

    async fn delete(&self, space_id: &str, doc_id: &str) -> anyhow::Result<()> {
        self.store
            .lock()
            .unwrap()
            .remove(&(space_id.to_owned(), doc_id.to_owned()));
        Ok(())
    }
}

/// In-memory blob backend for integration tests.
struct InMemoryBlobBackend {
    store: Mutex<HashMap<(String, String), BlobRecord>>,
}

impl InMemoryBlobBackend {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl BlobBackend for InMemoryBlobBackend {
    async fn load(&self, space_id: &str, doc_id: &str) -> anyhow::Result<Option<BlobRecord>> {
        Ok(self
            .store
            .lock()
            .unwrap()
            .get(&(space_id.to_owned(), doc_id.to_owned()))
            .cloned())
    }

    async fn save(&self, space_id: &str, doc_id: &str, blob: &BlobRecord) -> anyhow::Result<()> {
        self.store
            .lock()
            .unwrap()
            .insert((space_id.to_owned(), doc_id.to_owned()), blob.clone());
        Ok(())
    }

    async fn delete(&self, space_id: &str, doc_id: &str) -> anyhow::Result<()> {
        self.store
            .lock()
            .unwrap()
            .remove(&(space_id.to_owned(), doc_id.to_owned()));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Default test relay config.
fn test_relay_config() -> RelayConfig {
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        outbound_capacity: 64,
        ..Default::default()
    }
}

/// Start a relay with the given content backend on a random port.
///
/// Returns the listen address and the `Arc<InMemoryContentBackend>` so the
/// test can inspect persisted content.
async fn start_relay_with_content_backend(backend: Arc<InMemoryContentBackend>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = test_relay_config();
    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app_with_backends(
        config,
        None, // no session backend
        None, // no PAT backend
        None, // no membership backend
        None, // no registry backend
        None, // no space backend
        Some(backend as Arc<dyn ContentBackend>),
        None, // no blob backend
        None, // no invite backend
        None, // no change backend
        None, // no quota backend
        Arc::new(kutl_relay::observer::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        Arc::new(kutl_relay::NoopAfterMergeObserver),
        Arc::new(kutl_relay::mcp_tools::NoopToolProvider),
        Arc::new(kutl_relay::mcp_tools::DefaultInstructionsProvider),
    )
    .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

/// Start a relay with both content and blob backends.
async fn start_relay_with_backends(
    content: Arc<InMemoryContentBackend>,
    blob: Arc<InMemoryBlobBackend>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = test_relay_config();
    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app_with_backends(
        config,
        None, // no session backend
        None, // no PAT backend
        None, // no membership backend
        None, // no registry backend
        None, // no space backend
        Some(content as Arc<dyn ContentBackend>),
        Some(blob as Arc<dyn BlobBackend>),
        None, // no invite backend
        None, // no change backend
        None, // no quota backend
        Arc::new(kutl_relay::observer::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        Arc::new(kutl_relay::NoopAfterMergeObserver),
        Arc::new(kutl_relay::mcp_tools::NoopToolProvider),
        Arc::new(kutl_relay::mcp_tools::DefaultInstructionsProvider),
    )
    .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

/// A thin WebSocket test client (mirrors the pattern from sync.rs).
struct TestClient {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

/// Timeout for receiving WebSocket messages in tests.
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for checking that no message is pending.
const NO_MSG_TIMEOUT: Duration = Duration::from_millis(200);

impl TestClient {
    async fn connect(addr: &str) -> Self {
        let url = format!("ws://{addr}/ws");
        let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        Self { ws }
    }

    async fn send_envelope(&mut self, envelope: &sync::SyncEnvelope) {
        let bytes = encode_envelope(envelope);
        self.ws
            .send(tungstenite::Message::Binary(bytes.into()))
            .await
            .unwrap();
    }

    async fn recv_envelope(&mut self) -> sync::SyncEnvelope {
        let msg = tokio::time::timeout(RECV_TIMEOUT, self.ws.next())
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

    /// Subscribe and return the catch-up `SyncOps` if the relay sends one.
    /// Skips over the `SubscribeStatus` envelope that precedes the catch-up
    /// since RFD 0066 (it arrives first on ctrl, multiplexed biased onto ws).
    async fn subscribe(&mut self, space: &str, doc: &str) -> Option<sync::SyncOps> {
        self.send_envelope(&subscribe_envelope(space, doc)).await;

        for _ in 0..3 {
            match tokio::time::timeout(NO_MSG_TIMEOUT, self.ws.next()).await {
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
}

/// Create a local document with a single edit, returning ops bytes and change metadata.
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

/// Merge received sync ops into a fresh document and return its text content.
fn apply_ops(sync_ops: &sync::SyncOps) -> String {
    let mut doc = kutl_core::Document::new();
    doc.merge(&sync_ops.ops, &sync_ops.metadata).unwrap();
    doc.content()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Duration to wait for the flush task to fire.
///
/// The default flush interval is 1.5s. We wait a bit longer to account for
/// scheduling jitter in CI.
const FLUSH_WAIT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn test_ops_persisted_via_flush() {
    let backend = Arc::new(InMemoryContentBackend::new());
    let addr = start_relay_with_content_backend(Arc::clone(&backend)).await;

    // Connect client, subscribe, send ops.
    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe("space", "doc").await;

    let (ops, metadata) = make_edit("alice", "hello world", "initial");
    client.send_ops("space", "doc", ops, metadata).await;

    // Wait for the background flush task to persist.
    tokio::time::sleep(FLUSH_WAIT).await;

    // Verify the backend has the content.
    assert_eq!(backend.len(), 1, "backend should have exactly one document");
    let stored = backend
        .get("space", "doc")
        .expect("document should be in backend after flush");
    assert!(!stored.is_empty(), "stored bytes should not be empty");

    // Verify stored content is a valid CRDT document.
    let mut doc = kutl_core::Document::new();
    doc.merge(&stored, &[]).unwrap();
    assert_eq!(doc.content(), "hello world");
}

#[tokio::test]
async fn test_reload_after_restart() {
    // Pre-populate a content backend with a document.
    let backend = Arc::new(InMemoryContentBackend::new());
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent("seeder").unwrap();
    doc.edit(
        agent,
        "seeder",
        "seed",
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, "persisted content"),
    )
    .unwrap();
    let encoded = doc.encode_full();
    backend.save("space", "doc", &encoded).await.unwrap();

    // Start relay with this pre-populated backend.
    let addr = start_relay_with_content_backend(Arc::clone(&backend)).await;

    // Connect and subscribe — should receive catch-up from the backend.
    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    let catch_up = client
        .subscribe("space", "doc")
        .await
        .expect("should receive catch-up from backend");

    assert!(!catch_up.ops.is_empty(), "catch-up ops should not be empty");
    assert_eq!(apply_ops(&catch_up), "persisted content");
}

#[tokio::test]
async fn test_flush_then_new_subscriber_sees_content() {
    // This tests the full cycle: write ops -> flush -> new subscriber gets content.
    let backend = Arc::new(InMemoryContentBackend::new());
    let addr = start_relay_with_content_backend(Arc::clone(&backend)).await;

    // Client A connects and writes content.
    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a.subscribe("space", "doc").await;

    let (ops, metadata) = make_edit("alice", "flushed data", "write");
    client_a.send_ops("space", "doc", ops, metadata).await;

    // Give relay time to process.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client B joins and should see the content (from the in-memory document,
    // since it's still loaded).
    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    let catch_up = client_b
        .subscribe("space", "doc")
        .await
        .expect("late joiner should get catch-up");

    assert_eq!(apply_ops(&catch_up), "flushed data");

    // Wait for flush and verify the backend also has it.
    tokio::time::sleep(FLUSH_WAIT).await;
    let stored = backend
        .get("space", "doc")
        .expect("document should be in backend after flush");
    let mut doc = kutl_core::Document::new();
    doc.merge(&stored, &[]).unwrap();
    assert_eq!(doc.content(), "flushed data");
}

#[tokio::test]
async fn test_ephemeral_relay_no_persistence() {
    // Start a relay with NO backends (the default open-source ephemeral mode).
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = test_relay_config();
    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Connect, subscribe, send ops — should work without crash.
    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe("space", "doc").await;

    let (ops, metadata) = make_edit("alice", "ephemeral text", "write");
    client.send_ops("space", "doc", ops, metadata).await;

    // Wait longer than the flush interval — no crash expected.
    tokio::time::sleep(FLUSH_WAIT).await;

    // A second subscriber can still join and see content (from in-memory doc).
    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    let catch_up = client_b
        .subscribe("space", "doc")
        .await
        .expect("should get catch-up even without persistence");
    assert_eq!(apply_ops(&catch_up), "ephemeral text");
}

#[tokio::test]
async fn test_multiple_documents_persisted() {
    let backend = Arc::new(InMemoryContentBackend::new());
    let addr = start_relay_with_content_backend(Arc::clone(&backend)).await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe("space", "doc-a").await;
    client.subscribe("space", "doc-b").await;

    let (ops_a, meta_a) = make_edit("alice", "alpha", "write-a");
    client.send_ops("space", "doc-a", ops_a, meta_a).await;

    let (ops_b, meta_b) = make_edit("bob", "beta", "write-b");
    client.send_ops("space", "doc-b", ops_b, meta_b).await;

    // Wait for flush.
    tokio::time::sleep(FLUSH_WAIT).await;

    assert_eq!(backend.len(), 2, "both documents should be persisted");

    // Verify each document's content.
    let stored_a = backend.get("space", "doc-a").unwrap();
    let mut doc_a = kutl_core::Document::new();
    doc_a.merge(&stored_a, &[]).unwrap();
    assert_eq!(doc_a.content(), "alpha");

    let stored_b = backend.get("space", "doc-b").unwrap();
    let mut doc_b = kutl_core::Document::new();
    doc_b.merge(&stored_b, &[]).unwrap();
    assert_eq!(doc_b.content(), "beta");
}

#[tokio::test]
async fn test_blob_persisted_via_flush() {
    let content_backend = Arc::new(InMemoryContentBackend::new());
    let blob_backend = Arc::new(InMemoryBlobBackend::new());
    let addr =
        start_relay_with_backends(Arc::clone(&content_backend), Arc::clone(&blob_backend)).await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe("space", "image.png").await;

    // Send a blob.
    let blob_data = vec![0x89, 0x50, 0x4E, 0x47]; // PNG magic bytes
    let hash = vec![0xAA; 32];
    let meta = sync::ChangeMetadata {
        timestamp: 1000,
        ..Default::default()
    };
    let envelope = sync::SyncEnvelope {
        payload: Some(Payload::SyncOps(sync::SyncOps {
            space_id: "space".into(),
            document_id: "image.png".into(),
            content_mode: i32::from(sync::ContentMode::Blob),
            ops: blob_data.clone(),
            content_hash: hash.clone(),
            metadata: vec![meta],
            ..Default::default()
        })),
    };
    client.send_envelope(&envelope).await;

    // Wait for flush.
    tokio::time::sleep(FLUSH_WAIT).await;

    // Verify the blob backend has the content.
    let stored = blob_backend
        .store
        .lock()
        .unwrap()
        .get(&("space".to_owned(), "image.png".to_owned()))
        .cloned()
        .expect("blob should be in backend after flush");
    assert_eq!(stored.data, blob_data);
    assert_eq!(stored.hash, hash);
}

#[tokio::test]
async fn test_content_survives_flush_cycle() {
    // Verify that content remains accessible after multiple flush cycles
    // (i.e., the dirty flag is properly managed).
    let backend = Arc::new(InMemoryContentBackend::new());
    let addr = start_relay_with_content_backend(Arc::clone(&backend)).await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe("space", "doc").await;

    // First edit.
    let (ops1, meta1) = make_edit("alice", "first", "edit-1");
    client.send_ops("space", "doc", ops1, meta1).await;

    // Wait for first flush.
    tokio::time::sleep(FLUSH_WAIT).await;

    let stored1 = backend
        .get("space", "doc")
        .expect("should be persisted after first flush");
    let mut doc1 = kutl_core::Document::new();
    doc1.merge(&stored1, &[]).unwrap();
    assert!(
        doc1.content().contains("first"),
        "first edit should be in stored content: {}",
        doc1.content()
    );

    // Second flush should not re-persist (not dirty).
    // We can't directly assert "no save happened" easily, but the content
    // should remain valid.
    tokio::time::sleep(FLUSH_WAIT).await;

    let stored2 = backend
        .get("space", "doc")
        .expect("content should still be present after second flush cycle");
    let mut doc2 = kutl_core::Document::new();
    doc2.merge(&stored2, &[]).unwrap();
    assert!(
        doc2.content().contains("first"),
        "content should be unchanged after clean flush cycle: {}",
        doc2.content()
    );
}

// ---------------------------------------------------------------------------
// MCP + storage integration tests
// ---------------------------------------------------------------------------

/// Authenticate against the relay and return a bearer token.
async fn authenticate(addr: &str, did: &str, signing_key: &SigningKey) -> String {
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let resp: serde_json::Value = client
        .post(format!("{base}/auth/challenge"))
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
        .post(format!("{base}/auth/verify"))
        .json(&serde_json::json!({"did": did, "nonce": nonce, "signature": sig_b64}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    resp["token"].as_str().unwrap().to_owned()
}

/// Send a JSON-RPC request to the MCP endpoint.
async fn mcp_request(
    addr: &str,
    token: &str,
    session_id: Option<&str>,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let client = reqwest::Client::new();
    let mut req = client
        .post(format!("http://{addr}/mcp"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }));

    if let Some(sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }

    let resp = req.send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    body
}

/// Initialize an MCP session and return `(session_id, token)`.
async fn mcp_session(addr: &str, keys_file: &tempfile::NamedTempFile) -> (String, String) {
    use std::io::Write;
    let (did, signing_key) = common::test_keypair();
    writeln!(&*keys_file, "{did}").unwrap();
    let token = authenticate(addr, &did, &signing_key).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/mcp"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "clientInfo": {"name": "test-agent", "version": "0.1"}
            }
        }))
        .send()
        .await
        .unwrap();

    let session_id = resp
        .headers()
        .get("mcp-session-id")
        .expect("initialize should return mcp-session-id")
        .to_str()
        .unwrap()
        .to_owned();

    (session_id, token)
}

/// Start an auth-enabled relay with a content backend.
async fn start_auth_relay_with_content_backend(
    backend: Arc<InMemoryContentBackend>,
) -> (String, tempfile::NamedTempFile) {
    use std::io::Write;
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# storage test keys").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-mcp-storage".into(),
        require_auth: true,
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app_with_backends(
        config,
        None, // no session backend
        None, // no PAT backend
        None, // no membership backend
        None, // no registry backend
        None, // no space backend
        Some(backend as Arc<dyn ContentBackend>),
        None, // no blob backend
        None, // no invite backend
        None, // no change backend
        None, // no quota backend
        Arc::new(kutl_relay::observer::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        Arc::new(kutl_relay::NoopAfterMergeObserver),
        Arc::new(kutl_relay::mcp_tools::NoopToolProvider),
        Arc::new(kutl_relay::mcp_tools::DefaultInstructionsProvider),
    )
    .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys_file)
}

/// MCP edits must set the dirty flag so the flush task persists them.
///
/// This test would have caught the bug where `handle_mcp_edit_document`
/// mutated the document but never set `slot.dirty = true`.
#[tokio::test]
async fn test_mcp_edit_persisted_via_flush() {
    let backend = Arc::new(InMemoryContentBackend::new());
    let (addr, keys) = start_auth_relay_with_content_backend(Arc::clone(&backend)).await;

    let (session_id, token) = mcp_session(&addr, &keys).await;

    // Edit a document via MCP.
    let resp = mcp_request(
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "edit_document",
            "arguments": {
                "space_id": "space",
                "document_id": "mcp-doc",
                "content": "written by mcp",
                "intent": "test-edit"
            }
        }),
    )
    .await;

    // Verify the MCP edit succeeded.
    assert!(
        resp.get("error").is_none(),
        "MCP edit should succeed: {resp}"
    );
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("unexpected MCP response shape: {resp}"));
    assert!(
        text.contains("ops_applied"),
        "response should confirm ops applied: {text}"
    );

    // Extract the auto-generated UUID from the edit response.
    let edit_result: serde_json::Value = serde_json::from_str(text).unwrap();
    let actual_doc_id = edit_result["document_id"].as_str().unwrap();

    // Wait for the flush task.
    tokio::time::sleep(FLUSH_WAIT).await;

    // Verify the backend has the MCP-edited content (keyed by UUID).
    let stored = backend
        .get("space", actual_doc_id)
        .expect("MCP edit should be persisted after flush");
    assert!(!stored.is_empty(), "stored bytes should not be empty");

    let mut doc = kutl_core::Document::new();
    doc.merge(&stored, &[]).unwrap();
    assert_eq!(doc.content(), "written by mcp");
}
