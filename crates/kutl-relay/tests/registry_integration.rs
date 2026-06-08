//! Integration tests for the relay document registry lifecycle messages.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use common::TestConn;
use kutl_proto::sync::{self, sync_envelope::Payload};
use kutl_relay::config::RelayConfig;
use kutl_relay::registry_store::SqliteBackend;
use kutl_relay::relay::{Relay, RelayCommand};
use kutl_relay::space_backend::{RegisteredSpace, SpaceBackend, SpaceBackendError};
use prost::Message;
use tokio::sync::mpsc;

const TEST_SPACE: &str = "space-1";

fn test_config(data_dir: Option<std::path::PathBuf>) -> RelayConfig {
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        data_dir,
        ..Default::default()
    }
}

async fn connect_client(relay: &mut Relay, conn_id: u64) -> TestConn {
    let (tx, data_rx) = mpsc::channel(64);
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel(16);

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
                client_name: format!("client-{conn_id}"),
                ..Default::default()
            },
        })
        .await;

    // Drain handshake ack.
    let _ = ctrl_rx.recv().await;

    TestConn {
        conn_id,
        data_rx,
        ctrl_rx,
    }
}

/// Subscribe a client to a document.
///
/// The relay sends a `SubscribeStatus` envelope on ctrl
/// right after the Subscribe command. Callers with direct access to
/// the client's `ctrl_rx` should invoke
/// [`common::drain_subscribe_status`] themselves before asserting on
/// further ctrl messages. This helper only dispatches the command.
async fn subscribe_to_doc(relay: &mut Relay, conn_id: u64, doc: &str) {
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: doc.into(),
            },
        })
        .await;
}

fn meta(did: &str) -> sync::ChangeMetadata {
    sync::ChangeMetadata {
        author_did: did.into(),
        timestamp: kutl_core::now_ms(),
        ..Default::default()
    }
}

fn decode_payload(bytes: &[u8]) -> Option<Payload> {
    sync::SyncEnvelope::decode(bytes).ok()?.payload
}

#[tokio::test]
async fn test_register_broadcasts_to_subscribers() {
    let mut relay = Relay::new_standalone(test_config(None));
    let mut client_a = connect_client(&mut relay, 1).await;
    let mut client_b = connect_client(&mut relay, 2).await;

    // Both subscribe to a doc in the same space.
    subscribe_to_doc(&mut relay, 1, "readme.md").await;
    subscribe_to_doc(&mut relay, 2, "readme.md").await;
    common::drain_subscribe_status(&mut client_a.ctrl_rx).await;
    common::drain_subscribe_status(&mut client_b.ctrl_rx).await;

    // Client A registers a new document.
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                originally_created_at_ms: None,
                source_kind: None,
                source_id: None,
                source_url: None,
                ingestion_job_id: None,
                source_author_display: None,
                title: None,
                content_type: None,
                converted_from_id: None,
                converted_from_filename: None,
                size_bytes: None,
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-000000000001".into(),
                path: "notes/ideas.md".into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;

    // Client B should receive the broadcast via ctrl channel.
    let bytes = client_b.ctrl_rx.recv().await.expect("broadcast expected");
    match decode_payload(&bytes) {
        Some(Payload::RegisterDocument(msg)) => {
            assert_eq!(msg.document_id, "00000000-0000-0000-0000-000000000001");
            assert_eq!(msg.path, "notes/ideas.md");
        }
        other => panic!("expected RegisterDocument, got {other:?}"),
    }

    // Client A should NOT receive their own broadcast, but per RFD 0042
    // amendment 2026-05-24 they DO receive a `RegisterDocumentAck` on
    // their own ctrl channel. Drain that ack and confirm it's success +
    // matches the registered doc.
    let ack_bytes = client_a
        .ctrl_rx
        .recv()
        .await
        .expect("register ack expected on sender's ctrl");
    match decode_payload(&ack_bytes) {
        Some(Payload::RegisterDocumentAck(ack)) => {
            assert!(ack.success);
            assert_eq!(ack.document_id, "00000000-0000-0000-0000-000000000001");
            assert_eq!(ack.entry.expect("entry on success").path, "notes/ideas.md");
        }
        other => panic!("expected RegisterDocumentAck on sender ctrl, got {other:?}"),
    }
    assert!(
        client_a.ctrl_rx.is_empty(),
        "sender should not receive its own broadcast (only the ack)"
    );

    // Registry should have the entry.
    let reg = relay.registry(TEST_SPACE).expect("registry should exist");
    let entry = reg
        .get("00000000-0000-0000-0000-000000000001")
        .expect("entry should exist");
    assert_eq!(entry.path, "notes/ideas.md");
}

#[tokio::test]
async fn test_rename_broadcasts_and_updates_registry() {
    let mut relay = Relay::new_standalone(test_config(None));
    let mut client_a = connect_client(&mut relay, 1).await;
    let mut client_b = connect_client(&mut relay, 2).await;

    subscribe_to_doc(&mut relay, 1, "readme.md").await;
    subscribe_to_doc(&mut relay, 2, "readme.md").await;
    common::drain_subscribe_status(&mut client_a.ctrl_rx).await;
    common::drain_subscribe_status(&mut client_b.ctrl_rx).await;
    let _client_a = client_a;

    // Register first.
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                originally_created_at_ms: None,
                source_kind: None,
                source_id: None,
                source_url: None,
                ingestion_job_id: None,
                source_author_display: None,
                title: None,
                content_type: None,
                converted_from_id: None,
                converted_from_filename: None,
                size_bytes: None,
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-000000000001".into(),
                path: "draft.md".into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;
    // Drain the register broadcast.
    let _ = client_b.ctrl_rx.recv().await;

    // Rename.
    relay
        .process_command(RelayCommand::RenameDocument {
            conn_id: 1,
            msg: sync::RenameDocument {
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-000000000001".into(),
                old_path: "draft.md".into(),
                new_path: "archive/draft.md".into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;

    let bytes = client_b.ctrl_rx.recv().await.expect("rename broadcast");
    match decode_payload(&bytes) {
        Some(Payload::RenameDocument(msg)) => {
            assert_eq!(msg.old_path, "draft.md");
            assert_eq!(msg.new_path, "archive/draft.md");
        }
        other => panic!("expected RenameDocument, got {other:?}"),
    }

    let reg = relay.registry(TEST_SPACE).unwrap();
    assert_eq!(
        reg.get("00000000-0000-0000-0000-000000000001")
            .unwrap()
            .path,
        "archive/draft.md"
    );
}

#[tokio::test]
async fn test_unregister_broadcasts_and_removes() {
    let mut relay = Relay::new_standalone(test_config(None));
    let mut client_a = connect_client(&mut relay, 1).await;
    let mut client_b = connect_client(&mut relay, 2).await;

    subscribe_to_doc(&mut relay, 1, "readme.md").await;
    subscribe_to_doc(&mut relay, 2, "readme.md").await;
    common::drain_subscribe_status(&mut client_a.ctrl_rx).await;
    common::drain_subscribe_status(&mut client_b.ctrl_rx).await;
    let _client_a = client_a;

    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                originally_created_at_ms: None,
                source_kind: None,
                source_id: None,
                source_url: None,
                ingestion_job_id: None,
                source_author_display: None,
                title: None,
                content_type: None,
                converted_from_id: None,
                converted_from_filename: None,
                size_bytes: None,
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-000000000001".into(),
                path: "to-delete.md".into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;
    let _ = client_b.ctrl_rx.recv().await;

    relay
        .process_command(RelayCommand::UnregisterDocument {
            conn_id: 1,
            msg: sync::UnregisterDocument {
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-000000000001".into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;

    let bytes = client_b.ctrl_rx.recv().await.expect("unregister broadcast");
    match decode_payload(&bytes) {
        Some(Payload::UnregisterDocument(msg)) => {
            assert_eq!(msg.document_id, "00000000-0000-0000-0000-000000000001");
        }
        other => panic!("expected UnregisterDocument, got {other:?}"),
    }

    let reg = relay.registry(TEST_SPACE).unwrap();
    assert!(
        reg.get("00000000-0000-0000-0000-000000000001").is_none(),
        "should be soft-deleted"
    );
}

#[tokio::test]
async fn test_duplicate_path_conflict_copies() {
    let mut relay = Relay::new_standalone(test_config(None));
    let mut client_a = connect_client(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 1, "readme.md").await;
    common::drain_subscribe_status(&mut client_a.ctrl_rx).await;

    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                originally_created_at_ms: None,
                source_kind: None,
                source_id: None,
                source_url: None,
                ingestion_job_id: None,
                source_author_display: None,
                title: None,
                content_type: None,
                converted_from_id: None,
                converted_from_filename: None,
                size_bytes: None,
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-000000000001".into(),
                path: "a.md".into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;

    // Drain the success ack from the first register (RFD 0042
    // amendment 2026-05-24 — sender always receives an ack).
    let _ = client_a.ctrl_rx.recv().await;

    // Register a DIFFERENT document at the same path. With the lattice this is
    // not rejected — it conflict-copies: both documents survive, one keeps the
    // path and the other is displaced to its conflict path.
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                originally_created_at_ms: None,
                source_kind: None,
                source_id: None,
                source_url: None,
                ingestion_job_id: None,
                source_author_display: None,
                title: None,
                content_type: None,
                converted_from_id: None,
                converted_from_filename: None,
                size_bytes: None,
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-000000000002".into(),
                path: "a.md".into(),
                metadata: Some(meta("did:bob")),
            },
        })
        .await;

    // The register succeeds via conflict-copy. The sender (conn 1) also receives
    // the displacement broadcast for the prior holder (sent to all, including the
    // operand), which precedes the operand's own ack — so drain frames for the
    // RegisterDocumentAck rather than assuming it arrives first.
    let mut ack = None;
    while let Ok(bytes) = client_a.ctrl_rx.try_recv() {
        if let Some(Payload::RegisterDocumentAck(a)) = decode_payload(&bytes) {
            ack = Some(a);
            break;
        }
    }
    let ack = ack.expect("a RegisterDocumentAck for doc 2 is among the control frames");
    assert!(
        ack.success,
        "duplicate-path register succeeds (conflict-copy)"
    );
    assert_eq!(ack.document_id, "00000000-0000-0000-0000-000000000002");

    // Both documents are alive; one holds "a.md", the other a conflict path.
    let reg = relay.registry(TEST_SPACE).unwrap();
    let p1 = reg
        .get("00000000-0000-0000-0000-000000000001")
        .unwrap()
        .path
        .clone();
    let p2 = reg
        .get("00000000-0000-0000-0000-000000000002")
        .unwrap()
        .path
        .clone();
    assert!(
        (p1 == "a.md") ^ (p2 == "a.md"),
        "exactly one holds the path: {p1} / {p2}"
    );
    assert!(
        p1.contains(".kutl-conflict-") || p2.contains(".kutl-conflict-"),
        "the other is conflict-copied: {p1} / {p2}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_registry_persists_to_disk() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: register documents with SQLite backend.
    {
        let backend = SqliteBackend::open(dir.path()).await.unwrap();
        let mut relay = Relay::new_standalone_with_backend(
            test_config(Some(dir.path().to_path_buf())),
            Some(Arc::new(backend)),
            None,
            None,
            None,
        );
        let _client = connect_client(&mut relay, 1).await;
        subscribe_to_doc(&mut relay, 1, "readme.md").await;

        relay
            .process_command(RelayCommand::RegisterDocument {
                conn_id: 1,
                msg: sync::RegisterDocument {
                    originally_created_at_ms: None,
                    source_kind: None,
                    source_id: None,
                    source_url: None,
                    ingestion_job_id: None,
                    source_author_display: None,
                    title: None,
                    content_type: None,
                    converted_from_id: None,
                    converted_from_filename: None,
                    size_bytes: None,
                    space_id: TEST_SPACE.into(),
                    document_id: "00000000-0000-0000-0000-000000000001".into(),
                    path: "hello.md".into(),
                    metadata: Some(meta("did:alice")),
                },
            })
            .await;

        relay
            .process_command(RelayCommand::RegisterDocument {
                conn_id: 1,
                msg: sync::RegisterDocument {
                    originally_created_at_ms: None,
                    source_kind: None,
                    source_id: None,
                    source_url: None,
                    ingestion_job_id: None,
                    source_author_display: None,
                    title: None,
                    content_type: None,
                    converted_from_id: None,
                    converted_from_filename: None,
                    size_bytes: None,
                    space_id: TEST_SPACE.into(),
                    document_id: "00000000-0000-0000-0000-000000000002".into(),
                    path: "world.md".into(),
                    metadata: Some(meta("did:bob")),
                },
            })
            .await;
    }

    // Phase 2: new relay loads persisted registry from same SQLite database.
    {
        let backend = SqliteBackend::open(dir.path()).await.unwrap();
        let relay = Relay::new_standalone_with_backend(
            test_config(Some(dir.path().to_path_buf())),
            Some(Arc::new(backend)),
            None,
            None,
            None,
        );
        let reg = relay.registry(TEST_SPACE).expect("registry should load");
        assert_eq!(reg.len(), 2);
        assert_eq!(
            reg.get("00000000-0000-0000-0000-000000000001")
                .unwrap()
                .path,
            "hello.md"
        );
        assert_eq!(
            reg.get("00000000-0000-0000-0000-000000000002")
                .unwrap()
                .path,
            "world.md"
        );
    }
}

/// A `SpaceBackend` test double that records `ensure`/`register` calls in a
/// shared map (id -> name) the test can inspect after handing the backend to
/// the actor. `ensure` mirrors the real semantics: create-if-absent, never
/// overwrite. `fail_ensure` makes `ensure` return an error to exercise the
/// abort path.
#[derive(Clone, Default)]
struct RecordingSpaceBackend {
    spaces: Arc<Mutex<HashMap<String, String>>>,
    fail_ensure: bool,
}

#[async_trait::async_trait]
impl SpaceBackend for RecordingSpaceBackend {
    async fn register(&self, name: &str) -> Result<RegisteredSpace, SpaceBackendError> {
        let space_id = format!("id-{name}");
        self.spaces
            .lock()
            .unwrap()
            .insert(space_id.clone(), name.to_owned());
        Ok(RegisteredSpace {
            space_id,
            name: name.to_owned(),
        })
    }

    async fn resolve_by_name(
        &self,
        name: &str,
    ) -> Result<Option<RegisteredSpace>, SpaceBackendError> {
        let map = self.spaces.lock().unwrap();
        Ok(map
            .iter()
            .find(|(_, n)| n.as_str() == name)
            .map(|(id, n)| RegisteredSpace {
                space_id: id.clone(),
                name: n.clone(),
            }))
    }

    async fn resolve_by_id(&self, id: &str) -> Result<Option<RegisteredSpace>, SpaceBackendError> {
        let map = self.spaces.lock().unwrap();
        Ok(map.get(id).map(|n| RegisteredSpace {
            space_id: id.to_owned(),
            name: n.clone(),
        }))
    }

    async fn list_spaces(&self) -> Result<Vec<RegisteredSpace>, SpaceBackendError> {
        let map = self.spaces.lock().unwrap();
        Ok(map
            .iter()
            .map(|(id, n)| RegisteredSpace {
                space_id: id.clone(),
                name: n.clone(),
            })
            .collect())
    }

    async fn ensure(&self, space_id: &str, name: &str) -> Result<(), SpaceBackendError> {
        if self.fail_ensure {
            return Err(SpaceBackendError::Storage("induced ensure failure".into()));
        }
        self.spaces
            .lock()
            .unwrap()
            .entry(space_id.to_owned())
            .or_insert_with(|| name.to_owned());
        Ok(())
    }
}

const UNKNOWN_SPACE: &str = "22222222-2222-2222-2222-222222222222";

fn register_doc_cmd(conn_id: u64, space_id: &str) -> RelayCommand {
    RelayCommand::RegisterDocument {
        conn_id,
        msg: sync::RegisterDocument {
            space_id: space_id.into(),
            document_id: "00000000-0000-0000-0000-0000000000aa".into(),
            path: "notes/x.md".into(),
            metadata: Some(meta("did:alice")),
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn test_register_unknown_space_auto_registers_with_uuid_name() {
    let backend = RecordingSpaceBackend::default();
    let spaces = backend.spaces.clone();
    let mut relay = Relay::new_standalone_with_backend(
        test_config(None),
        None,
        None,
        None,
        Some(Box::new(backend)),
    );
    let _client = connect_client(&mut relay, 1).await;

    relay
        .process_command(register_doc_cmd(1, UNKNOWN_SPACE))
        .await;

    let map = spaces.lock().unwrap();
    assert_eq!(
        map.get(UNKNOWN_SPACE),
        Some(&UNKNOWN_SPACE.to_owned()),
        "an implicitly-synced space must be auto-registered with name == space_id"
    );
}

#[tokio::test]
async fn test_register_aborts_when_ensure_fails() {
    let backend = RecordingSpaceBackend {
        fail_ensure: true,
        ..Default::default()
    };
    let spaces = backend.spaces.clone();
    let mut relay = Relay::new_standalone_with_backend(
        test_config(None),
        None,
        None,
        None,
        Some(Box::new(backend)),
    );
    let mut client = connect_client(&mut relay, 1).await;

    relay
        .process_command(register_doc_cmd(1, UNKNOWN_SPACE))
        .await;

    // Nothing was persisted (ensure failed before any create).
    assert!(
        spaces.lock().unwrap().is_empty(),
        "no space row should exist when ensure fails"
    );

    // The client receives a failure RegisterDocumentAck (success == false).
    let mut saw_failure_ack = false;
    while let Ok(bytes) = client.ctrl_rx.try_recv() {
        if let Some(Payload::RegisterDocumentAck(ack)) = decode_payload(&bytes) {
            assert!(!ack.success, "ack should report failure");
            saw_failure_ack = true;
        }
    }
    assert!(
        saw_failure_ack,
        "a failure RegisterDocumentAck should be sent"
    );
}
