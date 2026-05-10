//! Integration tests for the relay document registry lifecycle messages.

mod common;

use std::sync::Arc;

use common::TestConn;
use kutl_proto::sync::{self, sync_envelope::Payload};
use kutl_relay::config::RelayConfig;
use kutl_relay::registry_store::SqliteBackend;
use kutl_relay::relay::{Relay, RelayCommand};
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
/// RFD 0066: the relay now sends a `SubscribeStatus` envelope on ctrl
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
                space_id: TEST_SPACE.into(),
                document_id: "doc-1".into(),
                path: "notes/ideas.md".into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;

    // Client B should receive the broadcast via ctrl channel.
    let bytes = client_b.ctrl_rx.recv().await.expect("broadcast expected");
    match decode_payload(&bytes) {
        Some(Payload::RegisterDocument(msg)) => {
            assert_eq!(msg.document_id, "doc-1");
            assert_eq!(msg.path, "notes/ideas.md");
        }
        other => panic!("expected RegisterDocument, got {other:?}"),
    }

    // Client A should NOT receive their own broadcast.
    assert!(client_a.ctrl_rx.is_empty());

    // Registry should have the entry.
    let reg = relay.registry(TEST_SPACE).expect("registry should exist");
    let entry = reg.get("doc-1").expect("entry should exist");
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
                space_id: TEST_SPACE.into(),
                document_id: "doc-1".into(),
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
                document_id: "doc-1".into(),
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
    assert_eq!(reg.get("doc-1").unwrap().path, "archive/draft.md");
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
                space_id: TEST_SPACE.into(),
                document_id: "doc-1".into(),
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
                document_id: "doc-1".into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;

    let bytes = client_b.ctrl_rx.recv().await.expect("unregister broadcast");
    match decode_payload(&bytes) {
        Some(Payload::UnregisterDocument(msg)) => {
            assert_eq!(msg.document_id, "doc-1");
        }
        other => panic!("expected UnregisterDocument, got {other:?}"),
    }

    let reg = relay.registry(TEST_SPACE).unwrap();
    assert!(reg.get("doc-1").is_none(), "should be soft-deleted");
}

#[tokio::test]
async fn test_duplicate_register_returns_error() {
    let mut relay = Relay::new_standalone(test_config(None));
    let mut client_a = connect_client(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 1, "readme.md").await;
    common::drain_subscribe_status(&mut client_a.ctrl_rx).await;

    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: TEST_SPACE.into(),
                document_id: "doc-1".into(),
                path: "a.md".into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;

    // Try to register same path again.
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: TEST_SPACE.into(),
                document_id: "doc-2".into(),
                path: "a.md".into(),
                metadata: Some(meta("did:bob")),
            },
        })
        .await;

    // Should receive an error on ctrl.
    let bytes = client_a.ctrl_rx.recv().await.expect("error expected");
    match decode_payload(&bytes) {
        Some(Payload::Error(err)) => {
            assert!(
                err.message.contains("register failed"),
                "unexpected error: {}",
                err.message,
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
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
                    space_id: TEST_SPACE.into(),
                    document_id: "doc-1".into(),
                    path: "hello.md".into(),
                    metadata: Some(meta("did:alice")),
                },
            })
            .await;

        relay
            .process_command(RelayCommand::RegisterDocument {
                conn_id: 1,
                msg: sync::RegisterDocument {
                    space_id: TEST_SPACE.into(),
                    document_id: "doc-2".into(),
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
        assert_eq!(reg.get("doc-1").unwrap().path, "hello.md");
        assert_eq!(reg.get("doc-2").unwrap().path, "world.md");
    }
}
