//! RFD 0066 Phase 2 integration tests: the flush task writes the
//! durability witness after successful backend save, and does NOT write
//! it when the save fails. Also verifies that `FlushCompleted` is
//! suppressed on witness-update failure so the slot stays dirty and
//! the next cycle retries.
//!
//! These tests drive `run_flush_task` directly with stub backends and a
//! stub "actor" (an `mpsc::channel` we control). No axum, no real
//! Postgres — the invariant under test is narrow and the stubs give
//! deterministic control over the exact failure the test is proving.

use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use kutl_relay::blob_backend::{BlobBackend, BlobRecord};
use kutl_relay::content_backend::ContentBackend;
use kutl_relay::flush::run_flush_task;
use kutl_relay::registry::RegistryEntry;
use kutl_relay::registry_store::{RegistryBackend, RegistryStoreError, Witness};
use kutl_relay::relay::{FlushContent, FlushEntry, RelayCommand};

/// `ContentBackend` that records successful saves and can be configured
/// to fail every save attempt.
///
/// `successful_save_count` returns the number of times `save()` actually
/// persisted bytes — i.e., ran the success path past the `fail_save`
/// check. Phase 2 tests assert this stays at 0 when `fail_save` is true,
/// which requires the naming to distinguish "save was attempted" from
/// "save succeeded" (the latter is what the test cares about).
struct StubContentBackend {
    saved: Mutex<Vec<(String, String, Vec<u8>)>>,
    fail_save: bool,
}

impl StubContentBackend {
    fn new(fail_save: bool) -> Self {
        Self {
            saved: Mutex::new(Vec::new()),
            fail_save,
        }
    }

    fn successful_save_count(&self) -> usize {
        self.saved.lock().unwrap().len()
    }
}

#[async_trait]
impl ContentBackend for StubContentBackend {
    async fn load(&self, _: &str, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn save(&self, space_id: &str, doc_id: &str, data: &[u8]) -> anyhow::Result<()> {
        if self.fail_save {
            anyhow::bail!("stub: save failure injected");
        }
        self.saved
            .lock()
            .unwrap()
            .push((space_id.to_owned(), doc_id.to_owned(), data.to_vec()));
        Ok(())
    }

    async fn delete(&self, _: &str, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

/// `RegistryBackend` that records witness writes and can be configured to fail
/// `update_witness` while keeping everything else functional.
struct StubRegistryBackend {
    witnesses: Mutex<HashMap<(String, String), Witness>>,
    update_calls: AtomicUsize,
    fail_update_witness: bool,
}

impl StubRegistryBackend {
    fn new(fail_update_witness: bool) -> Self {
        Self {
            witnesses: Mutex::new(HashMap::new()),
            update_calls: AtomicUsize::new(0),
            fail_update_witness,
        }
    }

    fn update_call_count(&self) -> usize {
        self.update_calls.load(Ordering::SeqCst)
    }

    fn get_witness(&self, space_id: &str, doc_id: &str) -> Option<Witness> {
        self.witnesses
            .lock()
            .unwrap()
            .get(&(space_id.to_owned(), doc_id.to_owned()))
            .copied()
    }
}

impl RegistryBackend for StubRegistryBackend {
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

    fn load_witness(
        &self,
        space_id: &str,
        document_id: &str,
    ) -> Result<Witness, RegistryStoreError> {
        Ok(self
            .witnesses
            .lock()
            .unwrap()
            .get(&(space_id.to_owned(), document_id.to_owned()))
            .copied()
            .unwrap_or_default())
    }

    fn update_witness(
        &self,
        space_id: &str,
        document_id: &str,
        witness: &Witness,
    ) -> Result<(), RegistryStoreError> {
        self.update_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_update_witness {
            // Emit via sqlx::Error so it flows through the
            // RegistryStoreError::Database variant.
            return Err(RegistryStoreError::Database(sqlx::Error::Io(
                std::io::Error::other("stub: update_witness failure injected"),
            )));
        }
        self.witnesses
            .lock()
            .unwrap()
            .insert((space_id.to_owned(), document_id.to_owned()), *witness);
        Ok(())
    }
}

struct NoopBlobBackend;

#[async_trait]
impl BlobBackend for NoopBlobBackend {
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

/// Drive the flush task through exactly one cycle, feeding it one entry,
/// and return the `RelayCommand` it sent back to the actor (if any).
///
/// The flush task's loop pulls a tick, asks us for dirty docs via a
/// `FlushDirty { reply }` command, and waits on the reply channel. We
/// send one `FlushEntry` and close the channel, then collect whatever
/// the flush task sends back.
async fn run_one_flush_cycle(
    content: Arc<dyn ContentBackend>,
    registry: Option<Arc<dyn RegistryBackend>>,
    entry: FlushEntry,
) -> Vec<RelayCommand> {
    let (relay_tx, mut relay_rx) = mpsc::channel::<RelayCommand>(16);
    let blob: Arc<dyn BlobBackend> = Arc::new(NoopBlobBackend);

    // Use a very short interval so the tick fires immediately.
    let handle = tokio::spawn(run_flush_task(
        relay_tx,
        content,
        blob,
        None,
        registry,
        Duration::from_millis(10),
    ));

    // The flush task's first iteration sends FlushDirty to us.
    let cmd = relay_rx
        .recv()
        .await
        .expect("flush task should send FlushDirty");
    let reply_tx = match cmd {
        RelayCommand::FlushDirty { reply } => reply,
        other => panic!(
            "expected FlushDirty as first command, got: {}",
            std::any::type_name_of_val(&other)
        ),
    };

    // Send our entry, then close the reply channel so the flush task
    // finishes this iteration.
    reply_tx.send(entry).await.expect("reply channel open");
    drop(reply_tx);

    // Collect any further commands the flush task sends (FlushCompleted,
    // or nothing if the witness write suppressed it). Timeout because
    // suppression produces no message.
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    while let Ok(Some(cmd)) = tokio::time::timeout_at(deadline, relay_rx.recv()).await {
        // Filter out the next cycle's FlushDirty — we only care
        // about what the flush task produced for our entry.
        if matches!(cmd, RelayCommand::FlushDirty { .. }) {
            break;
        }
        out.push(cmd);
    }

    handle.abort();
    out
}

/// Build a text `FlushEntry` with the given space/doc/version for tests.
fn text_entry(space_id: &str, doc_id: &str, flushed_up_to: u64) -> FlushEntry {
    FlushEntry {
        space_id: space_id.to_owned(),
        doc_id: doc_id.to_owned(),
        content: FlushContent::Text(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        size_bytes: 4,
        flushed_up_to,
    }
}

// ---------------------------------------------------------------------------

/// Happy path: save succeeds, witness advances, `FlushCompleted` is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_witness_advances_on_successful_flush() {
    let content = Arc::new(StubContentBackend::new(false));
    let registry = Arc::new(StubRegistryBackend::new(false));

    let content_dyn: Arc<dyn ContentBackend> = content.clone();
    let registry_dyn: Arc<dyn RegistryBackend> = registry.clone();
    let cmds = run_one_flush_cycle(content_dyn, Some(registry_dyn), text_entry("s", "d", 7)).await;

    assert_eq!(
        content.successful_save_count(),
        1,
        "save should have succeeded once"
    );
    assert_eq!(
        registry.update_call_count(),
        1,
        "witness update should have been called once"
    );
    let w = registry
        .get_witness("s", "d")
        .expect("witness row should exist");
    assert_eq!(w.persisted_version, 7);
    assert!(
        w.persisted_at > 0,
        "persisted_at should be set from now_ms()"
    );

    // FlushCompleted should have been sent.
    assert_eq!(cmds.len(), 1, "expected exactly one FlushCompleted");
    match &cmds[0] {
        RelayCommand::FlushCompleted {
            space_id,
            doc_id,
            flushed_up_to,
        } => {
            assert_eq!(space_id, "s");
            assert_eq!(doc_id, "d");
            assert_eq!(*flushed_up_to, 7);
        }
        _ => panic!("expected FlushCompleted, got something else"),
    }
}

/// Test 12 (RFD 0066): `ContentBackend::save` fails → witness NOT advanced.
/// `update_witness` must never be called when the save fails, and
/// `FlushCompleted` must not be sent (slot stays dirty for retry).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_save_failure_does_not_advance_witness() {
    let content = Arc::new(StubContentBackend::new(true)); // fail save
    let registry = Arc::new(StubRegistryBackend::new(false));

    let content_dyn: Arc<dyn ContentBackend> = content.clone();
    let registry_dyn: Arc<dyn RegistryBackend> = registry.clone();
    let cmds = run_one_flush_cycle(content_dyn, Some(registry_dyn), text_entry("s", "d", 7)).await;

    assert_eq!(
        content.successful_save_count(),
        0,
        "save was attempted but the injected failure bailed before storing bytes"
    );
    assert_eq!(
        registry.update_call_count(),
        0,
        "update_witness must NOT be called when save fails"
    );
    assert!(
        registry.get_witness("s", "d").is_none(),
        "witness row must not exist"
    );

    // No FlushCompleted — slot stays dirty for next cycle's retry.
    assert!(
        cmds.is_empty(),
        "expected no FlushCompleted on save failure (got {} commands)",
        cmds.len()
    );
}

/// Test 13 (RFD 0066): save succeeds but `update_witness` fails. The
/// witness NEVER advances to a value the backend can't back up, and
/// `FlushCompleted` is suppressed — slot stays dirty so the next cycle
/// retries both the (idempotent) save and the witness write. This is
/// what prevents `persisted_version` from ever claiming more than the
/// backend actually has, which would produce false `Inconsistent` on
/// a later cold-load that genuinely has bytes. (Phase 3 asserts the
/// classification consequence; this test asserts the mechanism.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_witness_update_failure_suppresses_flush_completed() {
    let content = Arc::new(StubContentBackend::new(false));
    let registry = Arc::new(StubRegistryBackend::new(true)); // fail update_witness

    let content_dyn: Arc<dyn ContentBackend> = content.clone();
    let registry_dyn: Arc<dyn RegistryBackend> = registry.clone();
    let cmds = run_one_flush_cycle(content_dyn, Some(registry_dyn), text_entry("s", "d", 7)).await;

    assert_eq!(
        content.successful_save_count(),
        1,
        "save should have succeeded"
    );
    assert_eq!(
        registry.update_call_count(),
        1,
        "update_witness should have been attempted"
    );
    assert!(
        registry.get_witness("s", "d").is_none(),
        "witness must not be recorded when update_witness fails"
    );

    // No FlushCompleted — this is the critical invariant. Without this,
    // flushed_counter would advance past persisted_version, violating
    // the lattice guarantee that flushed <= persisted on disk.
    assert!(
        cmds.is_empty(),
        "expected no FlushCompleted when witness update fails (got {} commands)",
        cmds.len()
    );
}

/// OSS-mode compatibility: `registry_backend == None` skips witness entirely
/// and `FlushCompleted` always fires on successful save. No regression for
/// self-hosted ephemeral relays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_no_registry_backend_preserves_existing_behavior() {
    let content = Arc::new(StubContentBackend::new(false));
    let content_dyn: Arc<dyn ContentBackend> = content.clone();
    let cmds = run_one_flush_cycle(content_dyn, None, text_entry("s", "d", 7)).await;

    assert_eq!(
        content.successful_save_count(),
        1,
        "save should have succeeded once"
    );
    assert_eq!(
        cmds.len(),
        1,
        "expected FlushCompleted even without registry backend"
    );
}
