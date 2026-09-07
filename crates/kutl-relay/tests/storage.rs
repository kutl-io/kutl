//! Integration tests for relay storage: persistence via flush, reload on subscribe,
//! and the eviction + reload cycle.
//!
//! These tests exercise the full round-trip through a real axum relay (with
//! WebSocket connections and the background flush task) using in-memory backends.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use kutl_proto::sync::{self, sync_envelope::Payload};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;

use kutl_relay::blob_backend::{BlobBackend, BlobRecord};
use kutl_relay::config::RelayConfig;
use kutl_relay::content_backend::ContentBackend;

use common::TestClient;

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
}

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Default test relay config.
/// Deterministic test signing identity. Authentication is mandatory;
/// [`test_relay_config`] authorizes this DID and [`handshake`] authenticates
/// as it via the real challenge-response flow.
fn test_identity() -> (String, SigningKey) {
    let signing_key = SigningKey::from_bytes(&[9u8; 32]);
    let did = kutl_signals::did_key_encode(&signing_key.verifying_key());
    (did, signing_key)
}

fn test_relay_config() -> RelayConfig {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    // Authentication is mandatory; authorize the fixed test DID for
    // every space via a (uniquely-named, leaked) temp `authorized_keys` file.
    let (did, _) = test_identity();
    let keys_path = std::env::temp_dir().join(format!(
        "kutl-relay-storage-keys-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&keys_path, format!("{did}\n")).expect("write authorized_keys file");
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        outbound_capacity: 64,
        authorized_keys_file: Some(keys_path),
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
    let (app, _relay_handle, _flush_handle) = kutl_relay::testing::build_in_memory_app(
        config,
        kutl_relay::testing::TestBackends {
            content: Some(backend as Arc<dyn ContentBackend>),
            ..Default::default()
        },
    );

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
    let (app, _relay_handle, _flush_handle) = kutl_relay::testing::build_in_memory_app(
        config,
        kutl_relay::testing::TestBackends {
            content: Some(content as Arc<dyn ContentBackend>),
            blob: Some(blob as Arc<dyn BlobBackend>),
            ..Default::default()
        },
    );

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

/// Complete the handshake as [`test_identity`], the DID every relay here
/// authorizes at boot.
async fn handshake(client: &mut TestClient) {
    let (did, signing_key) = test_identity();
    client.handshake_as(&did, &signing_key).await;
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

/// Reconstruct a document from a persisted backend blob, decoding the content
/// envelope (oplog + change-metadata) the same way the relay's cold-load does.
/// Legacy bare oplogs decode to `{ oplog, metadata: None }` and merge cleanly.
fn load_stored(stored: &[u8]) -> kutl_core::Document {
    let decoded = kutl_core::decode_content_envelope(stored);
    let changes = decoded
        .metadata
        .as_ref()
        .map_or(&[][..], |m| m.changes.as_slice());
    let mut doc = kutl_core::Document::new();
    doc.merge(&decoded.oplog, changes).unwrap();
    if let Some(meta) = decoded.metadata {
        doc.merge_author_map(meta.author_by_agent);
    }
    doc
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
    handshake(&mut client).await;
    client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    let (ops, metadata) = make_edit("alice", "hello world", "initial");
    client
        .send_ops("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", ops, metadata)
        .await;

    // Wait for the background flush task to persist.
    tokio::time::sleep(FLUSH_WAIT).await;

    // Verify the backend has the content.
    assert_eq!(backend.len(), 1, "backend should have exactly one document");
    let stored = backend
        .get("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .expect("document should be in backend after flush");
    assert!(!stored.is_empty(), "stored bytes should not be empty");

    // Verify stored content is a valid CRDT document.
    let doc = load_stored(&stored);
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
    backend
        .save("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", &encoded)
        .await
        .unwrap();

    // Start relay with this pre-populated backend.
    let addr = start_relay_with_content_backend(Arc::clone(&backend)).await;

    // Connect and subscribe — should receive catch-up from the backend.
    let mut client = TestClient::connect(&addr).await;
    handshake(&mut client).await;
    let catch_up = client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
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
    handshake(&mut client_a).await;
    client_a
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    let (ops, metadata) = make_edit("alice", "flushed data", "write");
    client_a
        .send_ops("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", ops, metadata)
        .await;

    // Give relay time to process.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client B joins and should see the content (from the in-memory document,
    // since it's still loaded).
    let mut client_b = TestClient::connect(&addr).await;
    handshake(&mut client_b).await;
    let catch_up = client_b
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await
        .expect("late joiner should get catch-up");

    assert_eq!(apply_ops(&catch_up), "flushed data");

    // Wait for flush and verify the backend also has it.
    tokio::time::sleep(FLUSH_WAIT).await;
    let stored = backend
        .get("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .expect("document should be in backend after flush");
    let doc = load_stored(&stored);
    assert_eq!(doc.content(), "flushed data");
}

#[tokio::test]
async fn test_in_memory_relay_no_persistence() {
    // Start a relay with NO backends (the no-backend
    // shape). The OSS binary does not boot this way — `build_app` requires
    // a data dir — so construct the no-backend shape explicitly via the test seam.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = test_relay_config();
    let (app, _relay_handle, _flush_handle) = common::build_in_memory_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Connect, subscribe, send ops — should work without crash.
    let mut client = TestClient::connect(&addr).await;
    handshake(&mut client).await;
    client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    let (ops, metadata) = make_edit("alice", "ephemeral text", "write");
    client
        .send_ops("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", ops, metadata)
        .await;

    // Wait longer than the flush interval — no crash expected.
    tokio::time::sleep(FLUSH_WAIT).await;

    // A second subscriber can still join and see content (from in-memory doc).
    let mut client_b = TestClient::connect(&addr).await;
    handshake(&mut client_b).await;
    let catch_up = client_b
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await
        .expect("should get catch-up even without persistence");
    assert_eq!(apply_ops(&catch_up), "ephemeral text");
}

#[tokio::test]
async fn test_multiple_documents_persisted() {
    let backend = Arc::new(InMemoryContentBackend::new());
    let addr = start_relay_with_content_backend(Arc::clone(&backend)).await;

    let mut client = TestClient::connect(&addr).await;
    handshake(&mut client).await;
    client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc-a")
        .await;
    client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc-b")
        .await;

    let (ops_a, meta_a) = make_edit("alice", "alpha", "write-a");
    client
        .send_ops(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8",
            "doc-a",
            ops_a,
            meta_a,
        )
        .await;

    let (ops_b, meta_b) = make_edit("bob", "beta", "write-b");
    client
        .send_ops(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8",
            "doc-b",
            ops_b,
            meta_b,
        )
        .await;

    // Wait for flush.
    tokio::time::sleep(FLUSH_WAIT).await;

    assert_eq!(backend.len(), 2, "both documents should be persisted");

    // Verify each document's content.
    let stored_a = backend
        .get("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc-a")
        .unwrap();
    assert_eq!(load_stored(&stored_a).content(), "alpha");

    let stored_b = backend
        .get("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc-b")
        .unwrap();
    assert_eq!(load_stored(&stored_b).content(), "beta");
}

#[tokio::test]
async fn test_blob_persisted_via_flush() {
    let content_backend = Arc::new(InMemoryContentBackend::new());
    let blob_backend = Arc::new(InMemoryBlobBackend::new());
    let addr =
        start_relay_with_backends(Arc::clone(&content_backend), Arc::clone(&blob_backend)).await;

    let mut client = TestClient::connect(&addr).await;
    handshake(&mut client).await;
    client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "image.png")
        .await;

    // Send a blob with a forged content_hash. The relay must ignore the
    // client-supplied hash and persist the SHA-256 it computes from the ops
    // bytes, so a malicious or buggy client cannot poison the stored hash that
    // peers use for their last-writer-wins tiebreak.
    let blob_data = vec![0x89, 0x50, 0x4E, 0x47]; // PNG magic bytes
    let forged_hash = vec![0xAA; 32];
    let meta = sync::ChangeMetadata {
        timestamp: 1000,
        ..Default::default()
    };
    let envelope = sync::SyncEnvelope {
        payload: Some(Payload::SyncOps(sync::SyncOps {
            space_id: "3f49dbbf-e051-4b20-8c03-8923424fedf8".into(),
            document_id: "image.png".into(),
            content_mode: i32::from(sync::ContentMode::Blob),
            ops: blob_data.clone(),
            content_hash: forged_hash.clone(),
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
        .get(&(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8".to_owned(),
            "image.png".to_owned(),
        ))
        .cloned()
        .expect("blob should be in backend after flush");
    assert_eq!(stored.data, blob_data);

    // The persisted hash is the server-computed SHA-256 of the ops, not the
    // forged client value.
    let expected_hash = Sha256::digest(&blob_data).to_vec();
    assert_eq!(stored.hash, expected_hash);
    assert_ne!(
        stored.hash, forged_hash,
        "relay must not persist the client-supplied content_hash"
    );
}

#[tokio::test]
async fn test_content_survives_flush_cycle() {
    // Verify that content remains accessible after multiple flush cycles
    // (i.e., the dirty flag is properly managed).
    let backend = Arc::new(InMemoryContentBackend::new());
    let addr = start_relay_with_content_backend(Arc::clone(&backend)).await;

    let mut client = TestClient::connect(&addr).await;
    handshake(&mut client).await;
    client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    // First edit.
    let (ops1, meta1) = make_edit("alice", "first", "edit-1");
    client
        .send_ops("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", ops1, meta1)
        .await;

    // Wait for first flush.
    tokio::time::sleep(FLUSH_WAIT).await;

    let stored1 = backend
        .get("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .expect("should be persisted after first flush");
    let doc1 = load_stored(&stored1);
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
        .get("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .expect("content should still be present after second flush cycle");
    let doc2 = load_stored(&stored2);
    assert!(
        doc2.content().contains("first"),
        "content should be unchanged after clean flush cycle: {}",
        doc2.content()
    );
}

// ---------------------------------------------------------------------------
// MCP + storage integration tests
// ---------------------------------------------------------------------------

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
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::testing::build_in_memory_app(
        config,
        kutl_relay::testing::TestBackends {
            content: Some(backend as Arc<dyn ContentBackend>),
            ..Default::default()
        },
    );

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys_file)
}

/// MCP edits must set the dirty flag so the flush task persists them.
///
/// Guards against an edit path that mutates the document but never sets
/// `slot.dirty = true` — such an edit would silently never persist.
#[tokio::test]
async fn test_mcp_edit_persisted_via_flush() {
    let backend = Arc::new(InMemoryContentBackend::new());
    let (addr, keys) = start_auth_relay_with_content_backend(Arc::clone(&backend)).await;

    let s = common::mcp::McpSession::open(&addr, &keys).await;

    // Create the document via MCP — `edit_document` does not auto-create.
    let create_result = s
        .call_ok(
            "create_document",
            serde_json::json!({
                "space_id": "3f49dbbf-e051-4b20-8c03-8923424fedf8",
                "path": "mcp-doc",
                "content": "written by mcp"
            }),
        )
        .await;
    let actual_doc_id = create_result["document_id"].as_str().unwrap();

    // Wait for the flush task.
    tokio::time::sleep(FLUSH_WAIT).await;

    // Verify the backend has the MCP-edited content (keyed by UUID).
    let stored = backend
        .get("3f49dbbf-e051-4b20-8c03-8923424fedf8", actual_doc_id)
        .expect("MCP edit should be persisted after flush");
    assert!(!stored.is_empty(), "stored bytes should not be empty");

    let doc = load_stored(&stored);
    assert_eq!(doc.content(), "written by mcp");
}
