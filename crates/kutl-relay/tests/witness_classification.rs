//! RFD 0066 Phase 3 integration tests: the subscribe cold-load path
//! produces a richer `SubscribeStatus` reply driven by a cross-check
//! between `ContentBackend::load` and the durability witness.
//!
//! These tests drive a standalone `Relay` with stub backends so we can
//! deterministically inject `Err` returns, `None` returns with a
//! non-zero witness, and "genuinely empty" combinations — verifying
//! each case produces the right `LoadStatus` on the subscribe reply.

mod common;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use kutl_proto::sync::{self, SyncEnvelope, sync_envelope::Payload};
use kutl_relay::blob_backend::{BlobBackend, BlobRecord};
use kutl_relay::config::{
    DEFAULT_OUTBOUND_CAPACITY, DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS,
    RelayConfig,
};
use kutl_relay::content_backend::ContentBackend;
use kutl_relay::registry::RegistryEntry;
use kutl_relay::registry_store::{RegistryBackend, RegistryStoreError, Witness};
use kutl_relay::relay::{Relay, RelayCommand};
use prost::Message;
use tokio::sync::mpsc;

const TEST_SPACE: &str = "test-space";
const TEST_DOC: &str = "test-doc";

// ----------------------------------------------------------------------
// Stub backends with configurable fault injection
// ----------------------------------------------------------------------

struct StubContent {
    /// When `true`, `load()` returns `Err`. Simulates S3 creds broken,
    /// network failure, etc.
    fail_load: AtomicBool,
    /// Bytes to return on load when `fail_load` is false.
    stored: std::sync::Mutex<Option<Vec<u8>>>,
}

impl StubContent {
    fn new_empty() -> Arc<Self> {
        Arc::new(Self {
            fail_load: AtomicBool::new(false),
            stored: std::sync::Mutex::new(None),
        })
    }

    fn new_with_bytes(bytes: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            fail_load: AtomicBool::new(false),
            stored: std::sync::Mutex::new(Some(bytes)),
        })
    }

    fn inject_load_failure(&self) {
        self.fail_load.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ContentBackend for StubContent {
    async fn load(&self, _: &str, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        if self.fail_load.load(Ordering::SeqCst) {
            anyhow::bail!("stub content load failure")
        }
        Ok(self.stored.lock().unwrap().clone())
    }
    async fn save(&self, _: &str, _: &str, _: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Minimal stub blob backend that always reports absent. Errors don't
/// come from here in these tests — blob-path faults are a separate
/// concern exercised by the pure classifier unit tests.
struct AbsentBlob;

#[async_trait]
impl BlobBackend for AbsentBlob {
    async fn load(&self, _: &str, _: &str) -> anyhow::Result<Option<BlobRecord>> {
        Ok(None)
    }
    async fn save(&self, _: &str, _: &str, _: &BlobRecord) -> anyhow::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Registry backend that returns a caller-provided witness on load.
/// All other methods are no-ops — we only care about witness reads
/// for these tests.
struct StubRegistry {
    witness: Witness,
}

impl StubRegistry {
    fn new(witness: Witness) -> Arc<Self> {
        Arc::new(Self { witness })
    }
}

impl RegistryBackend for StubRegistry {
    fn load_all(&self, _: &str) -> Result<Vec<RegistryEntry>, RegistryStoreError> {
        Ok(Vec::new())
    }
    fn load_spaces(&self) -> Result<Vec<String>, RegistryStoreError> {
        Ok(Vec::new())
    }
    fn save_entry(&self, _: &str, _: &RegistryEntry) -> Result<(), RegistryStoreError> {
        Ok(())
    }
    fn delete_entry(&self, _: &str, _: &str, _: i64) -> Result<(), RegistryStoreError> {
        Ok(())
    }
    fn update_edited_at(&self, _: &str, _: &str, _: i64) -> Result<(), RegistryStoreError> {
        Ok(())
    }
    fn load_witness(&self, _: &str, _: &str) -> Result<Witness, RegistryStoreError> {
        Ok(self.witness)
    }
    fn update_witness(&self, _: &str, _: &str, _: &Witness) -> Result<(), RegistryStoreError> {
        Ok(())
    }
}

// ----------------------------------------------------------------------
// Test harness
// ----------------------------------------------------------------------

fn test_config() -> RelayConfig {
    RelayConfig {
        host: "127.0.0.1".into(),
        port: 0,
        relay_name: "test".into(),
        require_auth: false,
        database_url: None,
        outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
        data_dir: None,
        external_url: None,
        ux_url: None,
        authorized_keys_file: None,
        snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
        snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
    }
}

async fn connect_client(
    relay: &mut Relay,
    conn_id: u64,
) -> (mpsc::Receiver<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
    let (data_tx, data_rx) = mpsc::channel(64);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx: data_tx,
            ctrl_tx,
        })
        .await;
    (data_rx, ctrl_rx)
}

async fn subscribe(relay: &mut Relay, conn_id: u64) {
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
            },
        })
        .await;
}

/// Pull the next `SubscribeStatus` off the ctrl channel. Panics if
/// something else arrives first or if the channel stays empty.
async fn recv_status(ctrl_rx: &mut mpsc::Receiver<Vec<u8>>) -> sync::SubscribeStatus {
    let bytes = tokio::time::timeout(std::time::Duration::from_millis(200), ctrl_rx.recv())
        .await
        .expect("timeout waiting for SubscribeStatus")
        .expect("ctrl channel closed");
    let env = SyncEnvelope::decode(bytes.as_slice()).expect("decode envelope");
    match env.payload {
        Some(Payload::SubscribeStatus(s)) => s,
        other => panic!("expected SubscribeStatus, got {other:?}"),
    }
}

// ----------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------

/// RFD 0066 Test 10 (baseline): genuinely first-time doc — backends
/// return None, witness is 0 — classifies as Empty. Editor may open
/// writable.
#[tokio::test]
async fn test_genuinely_empty_classifies_as_empty() {
    let content = StubContent::new_empty();
    let registry = StubRegistry::new(Witness::default()); // persisted_version == 0

    let content_dyn: Arc<dyn ContentBackend> = content.clone();
    let registry_dyn: Arc<dyn RegistryBackend> = registry;
    let mut relay = Relay::new_standalone_with_backend(
        test_config(),
        Some(registry_dyn),
        Some(content_dyn),
        Some(Arc::new(AbsentBlob)),
        None,
    );

    let (_data_rx, mut ctrl_rx) = connect_client(&mut relay, 1).await;
    subscribe(&mut relay, 1).await;

    let status = recv_status(&mut ctrl_rx).await;
    assert_eq!(status.load_status, i32::from(sync::LoadStatus::Empty));
    assert_eq!(status.expected_version, 0);
    assert!(status.reason.is_empty());
}

/// RFD 0066 Test 9: content was previously persisted (witness claims
/// `persisted_version > 0`) but the backend now returns `None` — the
/// zombie-relay / missing-IRSA / S3-object-lost scenario. Must
/// classify as Inconsistent with `expected_version` propagated, and
/// the `SubscribeStatus` reason must be empty (the `expected_version` is
/// the signal, not a string).
#[tokio::test]
async fn test_persisted_then_lost_classifies_as_inconsistent() {
    let content = StubContent::new_empty(); // backend has no bytes
    let registry = StubRegistry::new(Witness {
        persisted_version: 42,
        persisted_at: 1_700_000_000_000,
    });

    let content_dyn: Arc<dyn ContentBackend> = content.clone();
    let registry_dyn: Arc<dyn RegistryBackend> = registry;
    let mut relay = Relay::new_standalone_with_backend(
        test_config(),
        Some(registry_dyn),
        Some(content_dyn),
        Some(Arc::new(AbsentBlob)),
        None,
    );

    let (_data_rx, mut ctrl_rx) = connect_client(&mut relay, 1).await;
    subscribe(&mut relay, 1).await;

    let status = recv_status(&mut ctrl_rx).await;
    assert_eq!(
        status.load_status,
        i32::from(sync::LoadStatus::Inconsistent),
        "witness > 0 + backend None must classify Inconsistent, not Empty"
    );
    assert_eq!(
        status.expected_version, 42,
        "expected_version must echo the witness's persisted_version"
    );
}

/// RFD 0066 Test 8: `ContentBackend::load` returns `Err` — the prod
/// failure mode where IRSA is broken, S3 times out, or network is
/// flaky. This MUST classify as Unavailable regardless of the witness
/// value. The critical invariant: errors are never silently mapped
/// to Empty (which would let the user edit an apparent-empty doc
/// over the real content).
#[tokio::test]
async fn test_backend_error_classifies_as_unavailable_not_empty() {
    let content = StubContent::new_empty();
    content.inject_load_failure();
    // Witness == 0 (fresh doc). Even without prior flush, an Err
    // must still surface as Unavailable — not silently collapsed
    // into Empty. This is the precise invariant the prod incident
    // violated.
    let registry = StubRegistry::new(Witness::default());

    let content_dyn: Arc<dyn ContentBackend> = content.clone();
    let registry_dyn: Arc<dyn RegistryBackend> = registry;
    let mut relay = Relay::new_standalone_with_backend(
        test_config(),
        Some(registry_dyn),
        Some(content_dyn),
        Some(Arc::new(AbsentBlob)),
        None,
    );

    let (_data_rx, mut ctrl_rx) = connect_client(&mut relay, 1).await;
    subscribe(&mut relay, 1).await;

    let status = recv_status(&mut ctrl_rx).await;
    assert_eq!(
        status.load_status,
        i32::from(sync::LoadStatus::Unavailable),
        "backend Err must classify Unavailable, NEVER Empty"
    );
    assert!(
        !status.reason.is_empty(),
        "Unavailable must carry a human-readable reason"
    );
    assert!(
        status.reason.contains("stub content load failure"),
        "reason should propagate the underlying error: got {:?}",
        status.reason
    );
}

/// Complementary: backend error with `witness > 0` also classifies
/// Unavailable (not Inconsistent). The invariant is "any error →
/// Unavailable regardless of witness" — a load failure by itself
/// cannot prove the backend is inconsistent; we just can't determine
/// anything.
#[tokio::test]
async fn test_backend_error_classifies_as_unavailable_even_with_witness() {
    let content = StubContent::new_empty();
    content.inject_load_failure();
    let registry = StubRegistry::new(Witness {
        persisted_version: 99,
        persisted_at: 1_700_000_000_000,
    });

    let content_dyn: Arc<dyn ContentBackend> = content.clone();
    let registry_dyn: Arc<dyn RegistryBackend> = registry;
    let mut relay = Relay::new_standalone_with_backend(
        test_config(),
        Some(registry_dyn),
        Some(content_dyn),
        Some(Arc::new(AbsentBlob)),
        None,
    );

    let (_data_rx, mut ctrl_rx) = connect_client(&mut relay, 1).await;
    subscribe(&mut relay, 1).await;

    let status = recv_status(&mut ctrl_rx).await;
    assert_eq!(status.load_status, i32::from(sync::LoadStatus::Unavailable));
}

/// Found case: backend returns bytes — classifies as Found regardless
/// of witness, and the classic catch-up path runs as before. (Baseline
/// for ensuring the new classification doesn't break normal flow.)
#[tokio::test]
async fn test_backend_with_bytes_classifies_as_found() {
    // Pre-compute valid CRDT-encoded bytes so decode doesn't fail.
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent("author").unwrap();
    doc.edit(
        agent,
        "author",
        "seed",
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, "hello"),
    )
    .unwrap();
    let bytes = doc.encode_full();

    let content = StubContent::new_with_bytes(bytes);
    let registry = StubRegistry::new(Witness {
        persisted_version: 1,
        persisted_at: 1_700_000_000_000,
    });

    let content_dyn: Arc<dyn ContentBackend> = content.clone();
    let registry_dyn: Arc<dyn RegistryBackend> = registry;
    let mut relay = Relay::new_standalone_with_backend(
        test_config(),
        Some(registry_dyn),
        Some(content_dyn),
        Some(Arc::new(AbsentBlob)),
        None,
    );

    let (_data_rx, mut ctrl_rx) = connect_client(&mut relay, 1).await;
    subscribe(&mut relay, 1).await;

    let status = recv_status(&mut ctrl_rx).await;
    assert_eq!(status.load_status, i32::from(sync::LoadStatus::Found));
}
