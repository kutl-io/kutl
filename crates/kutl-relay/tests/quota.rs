//! Integration tests for inbound-path storage quota enforcement (RFD 0063, Tasks 7 and 8).
//!
//! Covers the seven trust-boundary scenarios the two rounds of security review
//! produced, plus the flush-path delta added in Task 8:
//!
//! 1. Blob put at cap rejected.
//! 2. Blob put uses server-measured bytes, not advertised `blob_size` (C4).
//! 3. Delta applies inline, not spawned (C2 — bounds parallel overshoot).
//! 4. Text merge at cap rejected (C3).
//! 5. OSS safety net — no backend means no enforcement.
//! 6. Per-document op-count cap surfaces as `QuotaExceeded` to the client.
//! 7. Flush path applies storage delta after successful text-doc save.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    blob_ops_envelope, decode_envelope, encode_envelope, handshake_envelope, subscribe_envelope,
    sync_ops_envelope,
};
use kutl_proto::sync::{self, SyncEnvelope, sync_envelope::Payload};
use kutl_relay::config::{DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS, RelayConfig};
use kutl_relay::content_backend::ContentBackend;
use kutl_relay::quota_backend::{QuotaBackend, SpaceQuota, StorageDeltaResult};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite;

// ---------------------------------------------------------------------------
// Test space / document identifiers.
// ---------------------------------------------------------------------------

const TEST_SPACE: &str = "space-1";
const TEST_DOC: &str = "doc-1";

// ---------------------------------------------------------------------------
// Mock quota backend.
// ---------------------------------------------------------------------------

/// In-memory `QuotaBackend` used for test scenarios.
///
/// Exposes the recorded deltas via `deltas` for C2 assertions (inline apply).
struct MockQuotaBackend {
    owner_account_id: String,
    used: AtomicI64,
    total: i64,
    per_blob_size: i64,
    deltas: Mutex<Vec<(String, String, i64)>>,
}

impl MockQuotaBackend {
    fn new(used: i64, total: i64, per_blob_size: i64) -> Self {
        Self {
            owner_account_id: "acc-1".into(),
            used: AtomicI64::new(used),
            total,
            per_blob_size,
            deltas: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl QuotaBackend for MockQuotaBackend {
    async fn load_space_quota(&self, _space_id: &str) -> anyhow::Result<Option<SpaceQuota>> {
        Ok(Some(SpaceQuota {
            owner_account_id: self.owner_account_id.clone(),
            storage_bytes_used: self.used.load(Ordering::SeqCst),
            storage_bytes_total: self.total,
            per_blob_size: self.per_blob_size,
        }))
    }

    async fn apply_storage_delta(
        &self,
        space_id: &str,
        doc_id: &str,
        new_size: i64,
    ) -> anyhow::Result<StorageDeltaResult> {
        self.deltas
            .lock()
            .expect("mutex never poisoned in single-thread test")
            .push((space_id.into(), doc_id.into(), new_size));
        // Mock simplification: treat the apply as if the prior size was zero
        // so `delta == new_size`. Tests that need accurate tracking can
        // inspect `deltas` directly.
        let delta = new_size;
        let new_used = self.used.fetch_add(delta, Ordering::SeqCst) + delta;
        Ok(StorageDeltaResult { new_used, delta })
    }
}

// ---------------------------------------------------------------------------
// Test harness — boots a full relay on a random port.
// ---------------------------------------------------------------------------

fn test_relay_config() -> RelayConfig {
    RelayConfig {
        host: "127.0.0.1".into(),
        port: 0,
        relay_name: "quota-test-relay".into(),
        require_auth: false,
        database_url: None,
        outbound_capacity: 256,
        data_dir: None,
        external_url: None,
        ux_url: None,
        authorized_keys_file: None,
        snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
        snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
    }
}

/// Start a relay with the given (optional) quota backend. Returns the
/// bound `ws://` URL prefix.
async fn start_relay_with_quota_backend(quota_backend: Option<Arc<dyn QuotaBackend>>) -> String {
    start_relay_with_backends(None, quota_backend).await
}

/// Start a relay with both a content backend (to exercise the flush path)
/// and an optional quota backend.
async fn start_relay_with_backends(
    content_backend: Option<Arc<dyn ContentBackend>>,
    quota_backend: Option<Arc<dyn QuotaBackend>>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr").to_string();

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app_with_backends(
        test_relay_config(),
        None,
        None,
        None,
        None,
        None,
        content_backend,
        None,
        None,
        None,
        quota_backend,
        Arc::new(kutl_relay::observer::TracingObserver),
        Arc::new(kutl_relay::observer::NoopBeforeMergeObserver),
        Arc::new(kutl_relay::observer::TracingObserver),
        Arc::new(kutl_relay::mcp_tools::NoopToolProvider),
    )
    .expect("build_app_with_backends");

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    addr
}

/// In-memory `ContentBackend` sufficient for flush-path tests.
struct InMemoryContentBackend {
    store: Mutex<HashMap<(String, String), Vec<u8>>>,
}

impl InMemoryContentBackend {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    /// Retrieve stored content for a (space, doc) pair.
    fn get(&self, space_id: &str, doc_id: &str) -> Option<Vec<u8>> {
        self.store
            .lock()
            .expect("mutex never poisoned in single-thread test")
            .get(&(space_id.to_owned(), doc_id.to_owned()))
            .cloned()
    }
}

#[async_trait]
impl ContentBackend for InMemoryContentBackend {
    async fn load(&self, space_id: &str, doc_id: &str) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self
            .store
            .lock()
            .expect("mutex never poisoned in single-thread test")
            .get(&(space_id.to_owned(), doc_id.to_owned()))
            .cloned())
    }

    async fn save(&self, space_id: &str, doc_id: &str, data: &[u8]) -> anyhow::Result<()> {
        self.store
            .lock()
            .expect("mutex never poisoned in single-thread test")
            .insert((space_id.to_owned(), doc_id.to_owned()), data.to_vec());
        Ok(())
    }

    async fn delete(&self, space_id: &str, doc_id: &str) -> anyhow::Result<()> {
        self.store
            .lock()
            .expect("mutex never poisoned in single-thread test")
            .remove(&(space_id.to_owned(), doc_id.to_owned()));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WebSocket test client.
// ---------------------------------------------------------------------------

struct TestClient {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

impl TestClient {
    async fn connect(addr: &str) -> Self {
        let url = format!("ws://{addr}/ws");
        let (ws, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("ws connect");
        Self { ws }
    }

    async fn send_envelope(&mut self, envelope: &SyncEnvelope) {
        let bytes = encode_envelope(envelope);
        self.ws
            .send(tungstenite::Message::Binary(bytes.into()))
            .await
            .expect("ws send");
    }

    async fn recv_envelope(&mut self) -> SyncEnvelope {
        let msg = tokio::time::timeout(Duration::from_secs(5), self.ws.next())
            .await
            .expect("recv timeout")
            .expect("stream ended")
            .expect("ws error");
        match msg {
            tungstenite::Message::Binary(bytes) => decode_envelope(&bytes).expect("decode"),
            other => panic!("expected binary, got {other:?}"),
        }
    }

    async fn handshake(&mut self) {
        self.send_envelope(&handshake_envelope("test-client")).await;
        let ack = self.recv_envelope().await;
        assert!(matches!(ack.payload, Some(Payload::HandshakeAck(_))));
    }

    /// Subscribe to a doc and drain both the `SubscribeStatus` and the
    /// catch-up `SyncOps` reply (if any). RFD 0066 adds the status envelope
    /// that precedes the catch-up on the WS.
    async fn subscribe(&mut self, space: &str, doc: &str) {
        self.send_envelope(&subscribe_envelope(space, doc)).await;
        for _ in 0..2 {
            if tokio::time::timeout(Duration::from_millis(200), self.ws.next())
                .await
                .is_err()
            {
                break;
            }
        }
    }

    async fn recv_error(&mut self) -> sync::Error {
        let envelope = self.recv_envelope().await;
        match envelope.payload {
            Some(Payload::Error(e)) => e,
            other => panic!("expected Error, got {other:?}"),
        }
    }

    async fn recv_sync_ops(&mut self) -> sync::SyncOps {
        let envelope = self.recv_envelope().await;
        match envelope.payload {
            Some(Payload::SyncOps(ops)) => ops,
            other => panic!("expected SyncOps, got {other:?}"),
        }
    }

    async fn send_blob(&mut self, space: &str, doc: &str, bytes: Vec<u8>) {
        let meta = sync::ChangeMetadata {
            timestamp: 1,
            ..Default::default()
        };
        self.send_envelope(&blob_ops_envelope(
            space,
            doc,
            bytes,
            vec![0u8; 32],
            Some(meta),
        ))
        .await;
    }

    /// Send a blob with a lied `blob_size` field — the actual payload size
    /// differs from the advertised value. Exercises the C4 regression path.
    async fn send_blob_with_lied_size(
        &mut self,
        space: &str,
        doc: &str,
        actual_bytes: Vec<u8>,
        advertised_size: i64,
    ) {
        let envelope = SyncEnvelope {
            payload: Some(Payload::SyncOps(sync::SyncOps {
                space_id: space.into(),
                document_id: doc.into(),
                content_mode: i32::from(sync::ContentMode::Blob),
                blob_size: advertised_size,
                ops: actual_bytes,
                content_hash: vec![0u8; 32],
                metadata: vec![sync::ChangeMetadata {
                    timestamp: 1,
                    ..Default::default()
                }],
                ..Default::default()
            })),
        };
        self.send_envelope(&envelope).await;
    }

    async fn send_text_ops(
        &mut self,
        space: &str,
        doc: &str,
        ops: Vec<u8>,
        metadata: Vec<sync::ChangeMetadata>,
    ) {
        self.send_envelope(&sync_ops_envelope(space, doc, ops, metadata))
            .await;
    }
}

/// Construct a valid text CRDT patch inserting `text` at position 0.
fn build_text_patch(agent_name: &str, text: &str) -> (Vec<u8>, Vec<sync::ChangeMetadata>) {
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent(agent_name).expect("register agent");
    doc.edit(
        agent,
        agent_name,
        "test",
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, text),
    )
    .expect("edit");
    let ops = doc.encode_since(&[]);
    let metadata = doc.changes_since(&[]);
    (ops, metadata)
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_blob_put_rejected_when_over_storage_cap() {
    // At-cap: used == total. Any further byte pushes us over.
    let mock = Arc::new(MockQuotaBackend::new(1_000_000, 1_000_000, 5_000_000));
    let addr = start_relay_with_quota_backend(Some(mock.clone())).await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe(TEST_SPACE, TEST_DOC).await;
    client.send_blob(TEST_SPACE, TEST_DOC, vec![0u8; 100]).await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::QuotaExceeded));
    assert!(
        err.details.contains("storage_total"),
        "expected storage_total in details, got {:?}",
        err.details
    );
    assert!(
        mock.deltas.lock().expect("mutex").is_empty(),
        "no delta should be applied on reject"
    );
}

#[tokio::test]
async fn test_blob_put_ignores_advertised_size_uses_actual_bytes() {
    // C4 regression: client advertises blob_size = 0 but ships a large
    // payload. Per-blob cap is 1MB; actual payload is 2MB. Must reject.
    //
    // We intentionally stay under ABSOLUTE_BLOB_MAX (25MB) so the codec
    // cap doesn't fire — the quota-backend check is the one under test.
    let mock = Arc::new(MockQuotaBackend::new(0, 1_000_000_000, 1_000_000));
    let addr = start_relay_with_quota_backend(Some(mock.clone())).await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe(TEST_SPACE, TEST_DOC).await;

    // 2 MB of actual bytes, advertised as 0.
    client
        .send_blob_with_lied_size(TEST_SPACE, TEST_DOC, vec![0u8; 2 * 1024 * 1024], 0)
        .await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::QuotaExceeded));
    assert!(
        err.details.contains("blob_size"),
        "expected blob_size cap in details, got {:?}",
        err.details
    );
    assert!(
        mock.deltas.lock().expect("mutex").is_empty(),
        "no delta should be applied on reject"
    );
}

/// Payload size used for the inline-delta test — small enough to avoid
/// tripping any cap, large enough that mistaking zero bytes for success
/// would be obvious in the delta assertion.
const INLINE_DELTA_BLOB_LEN: usize = 1234;

#[tokio::test]
async fn test_blob_put_applies_delta_inline_on_success() {
    // C2 regression: at the moment a subscriber observes the fan-out,
    // the delta must already be recorded. This proves the delta was
    // awaited inline rather than spawned.
    let mock = Arc::new(MockQuotaBackend::new(0, 10_000_000, 5_000_000));
    let addr = start_relay_with_quota_backend(Some(mock.clone())).await;

    // Sender.
    let mut sender = TestClient::connect(&addr).await;
    sender.handshake().await;
    sender.subscribe(TEST_SPACE, TEST_DOC).await;

    // Observer — a second subscriber that will receive the fan-out.
    let mut observer = TestClient::connect(&addr).await;
    observer.handshake().await;
    observer.subscribe(TEST_SPACE, TEST_DOC).await;

    sender
        .send_blob(TEST_SPACE, TEST_DOC, vec![0u8; INLINE_DELTA_BLOB_LEN])
        .await;

    // Observer receives the blob — this happens after the actor has
    // completed the inbound handler (including the inline delta apply).
    let relayed = observer.recv_sync_ops().await;
    assert_eq!(relayed.ops.len(), INLINE_DELTA_BLOB_LEN);

    let deltas = mock.deltas.lock().expect("mutex");
    assert_eq!(deltas.len(), 1, "exactly one delta recorded");
    #[allow(clippy::cast_possible_wrap)]
    let expected = INLINE_DELTA_BLOB_LEN as i64;
    assert_eq!(deltas[0].2, expected, "delta recorded with actual size");
    #[allow(clippy::cast_possible_wrap)]
    let used = mock.used.load(Ordering::SeqCst);
    assert_eq!(used, expected, "used counter advanced");
}

#[tokio::test]
async fn test_text_merge_rejected_when_over_storage_cap() {
    // C3 regression: text path also gates on storage cap. Without this
    // a user at cap can still accumulate CRDT bytes via text edits.
    let mock = Arc::new(MockQuotaBackend::new(500_000, 500_000, 5_000_000));
    let addr = start_relay_with_quota_backend(Some(mock.clone())).await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe(TEST_SPACE, TEST_DOC).await;

    let (ops, meta) = build_text_patch("alice", "hello");
    client.send_text_ops(TEST_SPACE, TEST_DOC, ops, meta).await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::QuotaExceeded));
    assert!(
        err.details.contains("storage_total"),
        "expected storage_total in details, got {:?}",
        err.details
    );
}

/// Payload size used for the OSS safety-net test — well above any tier
/// `per_blob_size` we expect to ship, and still under `ABSOLUTE_BLOB_MAX`
/// (25 MB, the WS codec cap).
const OSS_SAFETY_NET_BLOB_LEN: usize = 10 * 1024 * 1024;

#[tokio::test]
async fn test_oss_relay_no_quota_backend_accepts_large_blob() {
    // OSS safety net: with quota_backend = None, only ABSOLUTE_BLOB_MAX
    // (25 MB at the WS codec) applies. A 10 MB blob (well over any tier
    // cap) must succeed.
    let addr = start_relay_with_quota_backend(None).await;

    let mut sender = TestClient::connect(&addr).await;
    sender.handshake().await;
    sender.subscribe(TEST_SPACE, TEST_DOC).await;

    let mut observer = TestClient::connect(&addr).await;
    observer.handshake().await;
    observer.subscribe(TEST_SPACE, TEST_DOC).await;

    sender
        .send_blob(TEST_SPACE, TEST_DOC, vec![0u8; OSS_SAFETY_NET_BLOB_LEN])
        .await;

    let relayed = observer.recv_sync_ops().await;
    assert_eq!(relayed.ops.len(), OSS_SAFETY_NET_BLOB_LEN);
}

#[tokio::test]
async fn test_patch_too_large_surfaces_as_quota_exceeded_to_client() {
    // Regression for the op-count / patch-size cap added in Task 3.
    // `PatchTooLarge` (oversized merge patch) surfaces as QUOTA_EXCEEDED
    // with a `blob_size` cap_kind — the client-facing concept is the same
    // ("too big"); a dedicated CapKind::PatchSize would be overkill.
    //
    // Test under the cap for storage so the quota gate passes and we
    // reach the `Document::merge` call.
    let mock = Arc::new(MockQuotaBackend::new(0, 1_000_000_000, 5_000_000));
    let addr = start_relay_with_quota_backend(Some(mock.clone())).await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe(TEST_SPACE, TEST_DOC).await;

    // Oversized ops buffer — exceeds MAX_PATCH_BYTES (1 MB).
    let oversized_patch = vec![0u8; kutl_core::MAX_PATCH_BYTES + 1];
    client
        .send_text_ops(TEST_SPACE, TEST_DOC, oversized_patch, Vec::new())
        .await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::QuotaExceeded));
    assert!(
        err.details.contains("blob_size"),
        "expected blob_size cap_kind for oversized patch, got {:?}",
        err.details
    );
}

#[tokio::test]
async fn test_op_count_cap_surfaces_as_quota_exceeded_to_client() {
    // End-to-end: fill a doc to MAX_OPS_PER_DOC, then try another merge.
    // The actor must translate DocumentError::OpCountExceeded into a
    // QUOTA_EXCEEDED envelope with CapKind::OpCount.
    //
    // We fill via 5 000-byte string inserts to keep the test fast
    // (20 merges rather than 100 000).
    const FILL_BLOCK: usize = 5_000;

    let mock = Arc::new(MockQuotaBackend::new(0, 1_000_000_000, 5_000_000));
    let addr = start_relay_with_quota_backend(Some(mock.clone())).await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe(TEST_SPACE, TEST_DOC).await;

    // Fill the server-side doc to MAX_OPS_PER_DOC via a sequence of merges.
    // Each `edit()` inserts FILL_BLOCK characters which is FILL_BLOCK ops.
    // Encode against the prior version to send only the new delta each
    // round — otherwise the cumulative patch would exceed MAX_PATCH_BYTES.
    let mut local = kutl_core::Document::new();
    let agent = local.register_agent("filler").expect("register");
    let full_blocks = kutl_core::MAX_OPS_PER_DOC / FILL_BLOCK;
    let remainder = kutl_core::MAX_OPS_PER_DOC % FILL_BLOCK;
    let fill_text: String = "a".repeat(FILL_BLOCK);
    let remainder_text: String = "a".repeat(remainder);

    let mut since: Vec<usize> = Vec::new();
    for _ in 0..full_blocks {
        let text = fill_text.clone();
        local
            .edit(agent, "filler", "fill", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(0, &text)
            })
            .expect("edit");
        let ops = local.encode_since(&since);
        let metadata = local.changes_since(&since);
        since = local.local_version();
        client
            .send_text_ops(TEST_SPACE, TEST_DOC, ops, metadata)
            .await;
    }
    if remainder > 0 {
        let text = remainder_text.clone();
        local
            .edit(agent, "filler", "fill", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(0, &text)
            })
            .expect("edit");
        let ops = local.encode_since(&since);
        let metadata = local.changes_since(&since);
        since = local.local_version();
        client
            .send_text_ops(TEST_SPACE, TEST_DOC, ops, metadata)
            .await;
    }
    assert_eq!(local.op_count(), kutl_core::MAX_OPS_PER_DOC);

    // One more edit → should be rejected with OpCountExceeded → translated.
    local
        .edit(
            agent,
            "filler",
            "overflow",
            kutl_core::Boundary::Auto,
            |ctx| ctx.insert(0, "x"),
        )
        .expect("edit");
    let ops = local.encode_since(&since);
    let metadata = local.changes_since(&since);
    client
        .send_text_ops(TEST_SPACE, TEST_DOC, ops, metadata)
        .await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::QuotaExceeded));
    assert!(
        err.details.contains("op_count"),
        "expected op_count cap_kind, got {:?}",
        err.details
    );
}

/// Upper bound on how long to wait for a flush cycle. The default interval
/// is 1500ms; give it some slack so a slow CI host still sees the tick.
const FLUSH_WAIT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn test_text_flush_applies_storage_delta() {
    // RFD 0063 Task 8: after a successful text flush the relay must record
    // the durable size via `apply_storage_delta`. Only the blob inbound path
    // applies a delta at merge time; text merges rely on the flush-path
    // delta to update `storage_bytes_used`.
    let mock = Arc::new(MockQuotaBackend::new(0, 1_000_000_000, 5_000_000));
    let content = Arc::new(InMemoryContentBackend::new());
    let addr = start_relay_with_backends(
        Some(content.clone() as Arc<dyn ContentBackend>),
        Some(mock.clone()),
    )
    .await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client.subscribe(TEST_SPACE, TEST_DOC).await;

    let (ops, meta) = build_text_patch("alice", "hello world");
    client.send_text_ops(TEST_SPACE, TEST_DOC, ops, meta).await;

    // Wait past one flush interval so the background task persists the doc
    // and then calls `apply_storage_delta`.
    tokio::time::sleep(FLUSH_WAIT).await;

    let deltas = mock.deltas.lock().expect("mutex never poisoned");
    assert_eq!(
        deltas.len(),
        1,
        "expected exactly one flush delta for the text doc, got {}",
        deltas.len()
    );
    let flush_delta = &deltas[0];
    assert_eq!(flush_delta.0, TEST_SPACE);
    assert_eq!(flush_delta.1, TEST_DOC);
    assert!(
        flush_delta.2 > 0,
        "flush size_bytes must be > 0, got {}",
        flush_delta.2
    );
    // Cross-check: size_bytes must equal the exact bytes written to storage
    // (the RFD 0063 invariant). A positive-but-wrong value would satisfy the
    // assertion above while leaving the quota counter stale.
    let stored = content
        .get(TEST_SPACE, TEST_DOC)
        .expect("flush must have persisted the document");
    // cast_possible_wrap: stored.len() is bounded by ABSOLUTE_BLOB_MAX (25 MB), well below i64::MAX.
    #[allow(clippy::cast_possible_wrap)]
    let stored_len = stored.len() as i64;
    assert_eq!(
        flush_delta.2, stored_len,
        "flush size_bytes must equal the bytes written to storage"
    );
}
