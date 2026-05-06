//! Tests for path-based `DocKey` to UUID migration.

use kutl_proto::sync::{self, sync_envelope::Payload};
use kutl_relay::config::RelayConfig;
use kutl_relay::relay::{Relay, RelayCommand};
use prost::Message;
use tokio::sync::mpsc;

const TEST_SPACE: &str = "space-1";

fn test_config() -> RelayConfig {
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        ..Default::default()
    }
}

async fn connect_client(relay: &mut Relay, conn_id: u64) -> mpsc::Receiver<Vec<u8>> {
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

    data_rx
}

fn meta(did: &str) -> sync::ChangeMetadata {
    sync::ChangeMetadata {
        author_did: did.into(),
        timestamp: kutl_core::now_ms(),
        ..Default::default()
    }
}

/// Create a CRDT edit with known content.
fn make_edit(agent_name: &str, text: &str) -> (Vec<u8>, Vec<sync::ChangeMetadata>) {
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent(agent_name).unwrap();
    doc.edit(
        agent,
        agent_name,
        "test",
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, text),
    )
    .unwrap();
    let ops = doc.encode_since(&[]);
    let metadata = doc.changes_since(&[]);
    (ops, metadata)
}

/// Merge received sync ops and return the document content.
fn apply_ops(sync_ops: &sync::SyncOps) -> String {
    let mut doc = kutl_core::Document::new();
    doc.merge(&sync_ops.ops, &sync_ops.metadata).unwrap();
    doc.content()
}

fn decode_payload(bytes: &[u8]) -> Option<Payload> {
    sync::SyncEnvelope::decode(bytes).ok()?.payload
}

#[tokio::test]
async fn test_migrate_path_based_doc_keys() {
    let mut relay = Relay::new_standalone(test_config());

    // Connect a client and subscribe to a path-based document.
    let mut data_rx = connect_client(&mut relay, 1).await;

    let path_doc_id = "notes/ideas.md";

    relay
        .process_command(RelayCommand::Subscribe {
            conn_id: 1,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: path_doc_id.into(),
            },
        })
        .await;

    // Drain the initial empty catch-up.
    let _ = data_rx.recv().await;

    // Send some text ops under the path-based document ID.
    let (ops, metadata) = make_edit("alice", "hello world");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: path_doc_id.into(),
                ops,
                metadata,
                ..Default::default()
            }),
        })
        .await;

    // Register a UUID for this path in the registry.
    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: TEST_SPACE.into(),
                document_id: uuid.into(),
                path: path_doc_id.into(),
                metadata: Some(meta("did:alice")),
            },
        })
        .await;

    // Run migration.
    relay.migrate_path_based_keys();

    // Subscribe to the UUID-based document — should get the CRDT content.
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id: 1,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: uuid.into(),
            },
        })
        .await;

    let bytes = data_rx.recv().await.expect("should receive catch-up ops");
    match decode_payload(&bytes) {
        Some(Payload::SyncOps(ops)) => {
            assert_eq!(ops.document_id, uuid);
            let content = apply_ops(&ops);
            assert_eq!(content, "hello world");
        }
        other => panic!("expected SyncOps, got {other:?}"),
    }

    // Subscribe to the old path-based doc — should be empty (migrated away).
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id: 1,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: path_doc_id.into(),
            },
        })
        .await;

    let bytes = data_rx.recv().await.expect("should receive empty catch-up");
    match decode_payload(&bytes) {
        Some(Payload::SyncOps(ops)) => {
            assert_eq!(ops.document_id, path_doc_id);
            assert!(ops.ops.is_empty(), "old path-based doc should have no ops");
        }
        other => panic!("expected empty SyncOps, got {other:?}"),
    }
}

#[tokio::test]
async fn test_migrate_skips_uuid_doc_keys() {
    let mut relay = Relay::new_standalone(test_config());
    let mut data_rx = connect_client(&mut relay, 1).await;

    // Subscribe with a UUID-based document ID (no '/' or '.').
    let uuid = "abc123-def456";
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id: 1,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: uuid.into(),
            },
        })
        .await;

    // Drain catch-up.
    let _ = data_rx.recv().await;

    // Send ops.
    let (ops, metadata) = make_edit("bob", "uuid content");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: uuid.into(),
                ops,
                metadata,
                ..Default::default()
            }),
        })
        .await;

    // Run migration — should not touch UUID-based keys.
    relay.migrate_path_based_keys();

    // Subscribe again — content should still be there under the same key.
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id: 1,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: uuid.into(),
            },
        })
        .await;

    let bytes = data_rx.recv().await.expect("should receive catch-up");
    match decode_payload(&bytes) {
        Some(Payload::SyncOps(ops)) => {
            let content = apply_ops(&ops);
            assert_eq!(content, "uuid content");
        }
        other => panic!("expected SyncOps, got {other:?}"),
    }
}

#[tokio::test]
async fn test_migrate_no_registry_entry_skips() {
    let mut relay = Relay::new_standalone(test_config());
    let mut data_rx = connect_client(&mut relay, 1).await;

    // Subscribe with a path-based document ID but don't register it.
    let path_doc_id = "src/main.rs";
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id: 1,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: path_doc_id.into(),
            },
        })
        .await;

    let _ = data_rx.recv().await;

    let (ops, metadata) = make_edit("charlie", "fn main() {}");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: path_doc_id.into(),
                ops,
                metadata,
                ..Default::default()
            }),
        })
        .await;

    // Run migration — no registry entry, so nothing should change.
    relay.migrate_path_based_keys();

    // Content should still be under the path-based key.
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id: 1,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: path_doc_id.into(),
            },
        })
        .await;

    let bytes = data_rx.recv().await.expect("should receive catch-up");
    match decode_payload(&bytes) {
        Some(Payload::SyncOps(ops)) => {
            let content = apply_ops(&ops);
            assert_eq!(content, "fn main() {}");
        }
        other => panic!("expected SyncOps, got {other:?}"),
    }
}
