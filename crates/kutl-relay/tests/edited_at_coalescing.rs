//! Integration test: rapid WS merges coalesce `update_edited_at` writes.
//!
//! Verifies that sending N ops in quick succession (at keystroke rate) results
//! in at most 1 `update_edited_at` call per snippet debounce window rather
//! than N calls (one per merge).

use std::sync::{Arc, Mutex};

use kutl_proto::sync;
use kutl_relay::config::RelayConfig;
use kutl_relay::registry::RegistryEntry;
use kutl_relay::registry_store::{RegistryBackend, RegistryStoreError};
use kutl_relay::relay::{Relay, RelayCommand};
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// CountingBackend
// ---------------------------------------------------------------------------

/// Shared call recorder — the test retains an `Arc` to observe the
/// `update_edited_at` calls (one entry per call, holding the author it was
/// stamped with) while the relay owns the `Box<dyn RegistryBackend>` wrapper.
struct EditedAtCounter {
    call_authors: Mutex<Vec<Option<String>>>,
}

impl EditedAtCounter {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            call_authors: Mutex::new(Vec::new()),
        })
    }

    fn count(&self) -> usize {
        self.call_authors.lock().unwrap().len()
    }

    fn last_author(&self) -> Option<String> {
        self.call_authors
            .lock()
            .unwrap()
            .last()
            .and_then(Clone::clone)
    }
}

/// Newtype wrapping `Arc<EditedAtCounter>` so we can implement the foreign
/// `RegistryBackend` trait (orphan rules require the type to be local).
struct CountingBackend(Arc<EditedAtCounter>);

impl CountingBackend {
    fn new(counter: Arc<EditedAtCounter>) -> Self {
        Self(counter)
    }
}

impl RegistryBackend for CountingBackend {
    fn load_all_including_deleted(
        &self,
        _space_id: &str,
    ) -> Result<Vec<RegistryEntry>, RegistryStoreError> {
        Ok(vec![])
    }

    fn load_spaces_including_deleted(&self) -> Result<Vec<String>, RegistryStoreError> {
        Ok(vec![])
    }

    fn save_entry(
        &self,
        _space_id: &str,
        _entry: &RegistryEntry,
        _mirror: &kutl_relay::registry_store::MirrorMetadata<'_>,
    ) -> Result<(), RegistryStoreError> {
        Ok(())
    }

    fn delete_entry(
        &self,
        _space_id: &str,
        _document_id: &str,
        _deleted_at: i64,
    ) -> Result<(), RegistryStoreError> {
        Ok(())
    }

    fn update_edited_at(
        &self,
        _space_id: &str,
        _document_id: &str,
        _ts: i64,
        author_did: Option<&str>,
    ) -> Result<(), RegistryStoreError> {
        self.0
            .call_authors
            .lock()
            .unwrap()
            .push(author_did.map(str::to_owned));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Number of rapid ops to send (simulates keystroke-rate edits).
const RAPID_OP_COUNT: usize = 10;

/// Space ids are relay-minted UUIDs, and the relay enforces that at
/// the authorization boundary — a non-UUID names no space that exists.
const TEST_SPACE: &str = "5171e0a1-0000-4000-8000-00000000000a";
const TEST_DOC: &str = "doc-coalesce";
const ALICE_DID: &str = "did:key:alice";

/// Build a relay config for these tests.
///
/// Authentication is mandatory, so seed an `authorized_keys` file
/// granting [`ALICE_DID`] every space (bare-DID line). The file is written to a
/// unique temp path and intentionally leaked (test-only, tiny).
fn test_config() -> RelayConfig {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let keys_path = std::env::temp_dir().join(format!(
        "kutl-relay-edited-at-keys-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&keys_path, format!("{ALICE_DID}\n")).expect("write authorized_keys file");
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        snippet_debounce_ms: 100,
        authorized_keys_file: Some(keys_path),
        ..Default::default()
    }
}

/// Create a single-op edit authored by `author_did`, using a fresh local
/// document so each edit is independent.
///
/// The `author_did` is injected into the metadata so the relay treats all
/// ops as coming from the same author (preventing premature
/// `flush_pending_edit_for` calls that would fire whenever the author changes
/// and defeat the coalescing test).
fn make_edit(author_did: &str, text: &str) -> (Vec<u8>, Vec<sync::ChangeMetadata>) {
    let mut doc = kutl_core::Document::new();
    let ag = doc.register_agent("agent").unwrap();
    doc.edit(ag, "agent", "test", kutl_core::Boundary::Explicit, |ctx| {
        ctx.insert(0, text)
    })
    .unwrap();
    let ops = doc.encode_since(&[]);
    let metadata = doc
        .changes_since(&[])
        .into_iter()
        .map(|mut m| {
            m.author_did = author_did.to_string();
            m
        })
        .collect();
    (ops, metadata)
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Rapid WS merges from a single author coalesce into exactly 1
/// `update_edited_at` call per `FlushPendingEdit`, not 1 per merge.
///
/// Without coalescing, every merge would fire `update_edited_at` directly —
/// N ops → N calls. With it, `edited_at_pending` is set on every merge and
/// cleared by `flush_pending_edit_for` → 1 call total.
///
/// The standalone relay has no `self_tx`, so the snippet debounce timer never
/// fires automatically. We simulate it by sending `RelayCommand::FlushPendingEdit`
/// directly, which is exactly what the timer does in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rapid_ws_merges_coalesce_edited_at_writes() {
    let counter = EditedAtCounter::new();
    let relay_backend: Arc<dyn RegistryBackend> =
        Arc::new(CountingBackend::new(Arc::clone(&counter)));

    let mut relay =
        Relay::new_standalone_with_backend(test_config(), Some(relay_backend), None, None, None);

    // Connect a client.
    let conn_id: u64 = 1;
    let (data_tx, mut _data_rx) = mpsc::channel(64);
    let (ctrl_tx, _ctrl_rx) = mpsc::channel(64);
    let (ack_tx, _ack_rx) = mpsc::unbounded_channel();

    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx: data_tx,
            ctrl_tx,
            ack_tx,
        })
        .await;

    // Authentication is mandatory; attach the identity directly (the
    // in-process actor test cannot run the real HTTP challenge-response flow).
    // The conn authenticates AS the edit author so `authoritative_author_did`
    // leaves the injected author_did intact.
    relay.test_set_authenticated(conn_id, ALICE_DID);

    // Subscribe.
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
            },
        })
        .await;

    // Send RAPID_OP_COUNT ops back-to-back, all from the same author DID to
    // simulate a single user typing.
    for i in 0..RAPID_OP_COUNT {
        let (ops, meta) = make_edit(ALICE_DID, &format!("k{i}"));
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(sync::SyncOps {
                    space_id: TEST_SPACE.into(),
                    document_id: TEST_DOC.into(),
                    ops,
                    metadata: meta,
                    ..Default::default()
                }),
            })
            .await;
    }

    // Before flushing: edited_at_pending is set but no write has fired yet.
    let count_before = counter.count();
    assert_eq!(
        count_before, 0,
        "expected 0 update_edited_at calls before flush, got {count_before}"
    );

    // Simulate the debounce timer firing — this is exactly what the snippet
    // timer does in production after the debounce window expires.
    relay
        .process_command(RelayCommand::FlushPendingEdit {
            space_id: TEST_SPACE.to_string(),
            document_id: TEST_DOC.to_string(),
        })
        .await;

    let count_after = counter.count();

    // Key assertion: N ops → 1 write (not N writes).
    assert_eq!(
        count_after, 1,
        "expected exactly 1 update_edited_at call after flush, got {count_after}"
    );
}

/// A blob upload whose metadata claims a different author still stamps
/// `edited_at` with the CONNECTION's authenticated DID. Blob flushes fire
/// inline (no debounce), so the forged metadata would reach the registry
/// immediately if the blob path trusted it — this pins the same
/// authenticated-identity rule the text and MCP write paths enforce.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blob_edited_at_pins_authenticated_author_over_forged_metadata() {
    let counter = EditedAtCounter::new();
    let relay_backend: Arc<dyn RegistryBackend> =
        Arc::new(CountingBackend::new(Arc::clone(&counter)));

    let mut relay =
        Relay::new_standalone_with_backend(test_config(), Some(relay_backend), None, None, None);

    let conn_id: u64 = 1;
    let (data_tx, mut _data_rx) = mpsc::channel(64);
    let (ctrl_tx, _ctrl_rx) = mpsc::channel(64);
    let (ack_tx, _ack_rx) = mpsc::unbounded_channel();
    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx: data_tx,
            ctrl_tx,
            ack_tx,
        })
        .await;
    relay.test_set_authenticated(conn_id, ALICE_DID);
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
            },
        })
        .await;

    let blob = b"\x89PNG-not-really".to_vec();
    #[allow(clippy::cast_possible_wrap)]
    let blob_size = blob.len() as i64;
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: blob,
                metadata: vec![sync::ChangeMetadata {
                    author_did: "did:key:mallory".into(),
                    ..Default::default()
                }],
                content_mode: i32::from(sync::ContentMode::Blob),
                blob_size,
                ..Default::default()
            }),
        })
        .await;

    assert_eq!(counter.count(), 1, "blob uploads flush edited_at inline");
    assert_eq!(
        counter.last_author().as_deref(),
        Some(ALICE_DID),
        "edited_at author must be the authenticated DID, not the client-supplied metadata"
    );
}
