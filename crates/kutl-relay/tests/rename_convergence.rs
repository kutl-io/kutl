//! Stress tests for multi-daemon rename convergence.
//!
//! Simulates two daemons connected to the same relay, verifying that
//! registry operations (register, rename, unregister) converge correctly
//! when both daemons are active in the same space.

mod common;

use common::TestConn;
use kutl_proto::sync::{self, sync_envelope::Payload};
use kutl_relay::config::RelayConfig;
use kutl_relay::relay::{Relay, RelayCommand};
use prost::Message;
use tokio::sync::mpsc;

/// Space ids are relay-minted UUIDs, enforced at
/// the authorization boundary — a non-UUID names no space that exists.
const TEST_SPACE: &str = "5171e0a1-0000-4000-8000-000000000008";

/// Number of documents for batch operations.
const BATCH_SIZE: usize = 20;

/// Number of documents for rapid churn.
const CHURN_SIZE: usize = 50;

/// Identity every test connection authenticates as. Authentication is
/// mandatory; [`test_config`] authorizes this DID for every space.
const TEST_DID: &str = "did:key:zTestConn";

fn test_config() -> RelayConfig {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    // Authentication is mandatory; authorize TEST_DID for every space
    // via a (uniquely-named, leaked) temp `authorized_keys` file.
    let keys_path = std::env::temp_dir().join(format!(
        "kutl-relay-rename-conv-keys-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&keys_path, format!("{TEST_DID}\n")).expect("write authorized_keys file");
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        authorized_keys_file: Some(keys_path),
        ..Default::default()
    }
}

async fn connect_client(relay: &mut Relay, conn_id: u64) -> TestConn {
    let (tx, data_rx) = mpsc::channel(256);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(256);
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();

    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx,
            ctrl_tx,
            ack_tx,
        })
        .await;
    // Authentication is mandatory; attach the identity directly (the
    // in-process actor tests cannot run the real HTTP challenge-response flow).
    relay.test_set_authenticated(conn_id, TEST_DID);

    TestConn {
        conn_id,
        data_rx,
        ack_rx,
        ctrl_rx,
    }
}

async fn subscribe(relay: &mut Relay, conn_id: u64, doc: &str) {
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

/// Deterministic UUID document id for index `i` (document ids are UUIDs).
fn doc_id(i: usize) -> String {
    format!("00000000-0000-0000-0000-{i:012}")
}

fn meta(did: &str, timestamp: i64) -> sync::ChangeMetadata {
    sync::ChangeMetadata {
        author_did: did.into(),
        timestamp,
        ..Default::default()
    }
}

fn decode_payload(bytes: &[u8]) -> Option<Payload> {
    sync::SyncEnvelope::decode(bytes).ok()?.payload
}

/// Drain all pending messages from a ctrl channel.
///
/// Filters out `SubscribeStatus` envelopes; these
/// tests count registry broadcasts specifically and shouldn't see the
/// per-subscribe classification noise.
fn drain_ctrl(conn: &mut TestConn) -> Vec<Payload> {
    let mut payloads = Vec::new();
    while let Ok(bytes) = conn.ctrl_rx.try_recv() {
        if let Some(payload) = decode_payload(&bytes) {
            if matches!(payload, Payload::SubscribeStatus(_)) {
                continue;
            }
            payloads.push(payload);
        }
    }
    payloads
}

#[tokio::test]
async fn test_create_convergence() {
    let mut relay = Relay::new_standalone(test_config());
    let _daemon_a = connect_client(&mut relay, 1).await;
    let mut daemon_b = connect_client(&mut relay, 2).await;

    // Both daemons subscribe to a shared anchor document.
    subscribe(&mut relay, 1, "anchor").await;
    subscribe(&mut relay, 2, "anchor").await;

    // Daemon A creates BATCH_SIZE files.
    for i in 0..BATCH_SIZE {
        relay
            .process_command(RelayCommand::RegisterDocument {
                conn_id: 1,
                msg: sync::RegisterDocument {
                    space_id: TEST_SPACE.into(),
                    document_id: doc_id(i),
                    path: format!("notes/file-{i}.md"),
                    metadata: Some(meta("did:daemon-a", kutl_core::now_ms())),
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
                },
            })
            .await;
    }

    // Daemon B should receive all BATCH_SIZE broadcasts.
    let payloads = drain_ctrl(&mut daemon_b);
    let registers: Vec<_> = payloads
        .iter()
        .filter_map(|p| match p {
            Payload::RegisterDocument(msg) => Some(msg),
            _ => None,
        })
        .collect();

    assert_eq!(
        registers.len(),
        BATCH_SIZE,
        "daemon B should receive all {BATCH_SIZE} register broadcasts"
    );

    // Registry should have all entries.
    let reg = relay.registry(TEST_SPACE).expect("registry should exist");
    assert_eq!(reg.len(), BATCH_SIZE);

    for i in 0..BATCH_SIZE {
        let entry = reg.get(&doc_id(i)).expect("entry should exist");
        assert_eq!(entry.path, format!("notes/file-{i}.md"));
    }
}

#[tokio::test]
async fn test_rename_convergence() {
    let mut relay = Relay::new_standalone(test_config());
    let _daemon_a = connect_client(&mut relay, 1).await;
    let mut daemon_b = connect_client(&mut relay, 2).await;

    subscribe(&mut relay, 1, "anchor").await;
    subscribe(&mut relay, 2, "anchor").await;

    // Create BATCH_SIZE files.
    for i in 0..BATCH_SIZE {
        relay
            .process_command(RelayCommand::RegisterDocument {
                conn_id: 1,
                msg: sync::RegisterDocument {
                    space_id: TEST_SPACE.into(),
                    document_id: doc_id(i),
                    path: format!("file-{i}.md"),
                    metadata: Some(meta("did:daemon-a", kutl_core::now_ms())),
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
                },
            })
            .await;
    }
    drain_ctrl(&mut daemon_b);

    // Rename the first half to a subdirectory.
    let rename_count = BATCH_SIZE / 2;
    for i in 0..rename_count {
        relay
            .process_command(RelayCommand::RenameDocument {
                conn_id: 1,
                msg: sync::RenameDocument {
                    space_id: TEST_SPACE.into(),
                    document_id: doc_id(i),
                    old_path: format!("file-{i}.md"),
                    new_path: format!("archive/file-{i}.md"),
                    metadata: Some(meta("did:daemon-a", kutl_core::now_ms())),
                    rename_causal_floor: None,
                },
            })
            .await;
    }

    // Daemon B should receive rename broadcasts.
    let payloads = drain_ctrl(&mut daemon_b);
    let renames: Vec<_> = payloads
        .iter()
        .filter_map(|p| match p {
            Payload::RenameDocument(msg) => Some(msg),
            _ => None,
        })
        .collect();

    assert_eq!(
        renames.len(),
        rename_count,
        "daemon B should receive all {rename_count} rename broadcasts"
    );

    // Verify registry state.
    let reg = relay.registry(TEST_SPACE).unwrap();
    for i in 0..rename_count {
        let entry = reg.get(&doc_id(i)).unwrap();
        assert_eq!(entry.path, format!("archive/file-{i}.md"));
    }
    for i in rename_count..BATCH_SIZE {
        let entry = reg.get(&doc_id(i)).unwrap();
        assert_eq!(entry.path, format!("file-{i}.md"));
    }
}

#[tokio::test]
async fn test_delete_convergence() {
    let mut relay = Relay::new_standalone(test_config());
    let _daemon_a = connect_client(&mut relay, 1).await;
    let mut daemon_b = connect_client(&mut relay, 2).await;

    subscribe(&mut relay, 1, "anchor").await;
    subscribe(&mut relay, 2, "anchor").await;

    // Create 10 files.
    let total = 10;
    for i in 0..total {
        relay
            .process_command(RelayCommand::RegisterDocument {
                conn_id: 1,
                msg: sync::RegisterDocument {
                    space_id: TEST_SPACE.into(),
                    document_id: doc_id(i),
                    path: format!("file-{i}.md"),
                    metadata: Some(meta("did:daemon-a", kutl_core::now_ms())),
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
                },
            })
            .await;
    }
    drain_ctrl(&mut daemon_b);

    // Delete the first 5.
    let delete_count = 5;
    for i in 0..delete_count {
        relay
            .process_command(RelayCommand::UnregisterDocument {
                conn_id: 1,
                msg: sync::UnregisterDocument {
                    space_id: TEST_SPACE.into(),
                    document_id: doc_id(i),
                    metadata: Some(meta("did:daemon-a", kutl_core::now_ms())),
                },
            })
            .await;
    }

    let payloads = drain_ctrl(&mut daemon_b);
    let unregisters: Vec<_> = payloads
        .iter()
        .filter_map(|p| match p {
            Payload::UnregisterDocument(msg) => Some(msg),
            _ => None,
        })
        .collect();

    assert_eq!(
        unregisters.len(),
        delete_count,
        "daemon B should receive all {delete_count} unregister broadcasts"
    );

    // Verify registry: deleted docs gone, remaining docs still present.
    let reg = relay.registry(TEST_SPACE).unwrap();
    for i in 0..delete_count {
        assert!(
            reg.get(&doc_id(i)).is_none(),
            "doc-{i} should be soft-deleted"
        );
    }
    for i in delete_count..total {
        assert!(reg.get(&doc_id(i)).is_some(), "doc-{i} should still exist");
    }
}

#[tokio::test]
async fn test_rapid_churn_convergence() {
    let mut relay = Relay::new_standalone(test_config());
    let _daemon_a = connect_client(&mut relay, 1).await;
    let mut daemon_b = connect_client(&mut relay, 2).await;

    subscribe(&mut relay, 1, "anchor").await;
    subscribe(&mut relay, 2, "anchor").await;

    // Create CHURN_SIZE files.
    for i in 0..CHURN_SIZE {
        relay
            .process_command(RelayCommand::RegisterDocument {
                conn_id: 1,
                msg: sync::RegisterDocument {
                    space_id: TEST_SPACE.into(),
                    document_id: doc_id(i),
                    path: format!("churn/file-{i}.md"),
                    metadata: Some(meta("did:daemon-a", kutl_core::now_ms())),
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
                },
            })
            .await;
    }

    // Rename even-numbered files.
    for i in (0..CHURN_SIZE).step_by(2) {
        relay
            .process_command(RelayCommand::RenameDocument {
                conn_id: 1,
                msg: sync::RenameDocument {
                    space_id: TEST_SPACE.into(),
                    document_id: doc_id(i),
                    old_path: format!("churn/file-{i}.md"),
                    new_path: format!("renamed/file-{i}.md"),
                    metadata: Some(meta("did:daemon-a", kutl_core::now_ms())),
                    rename_causal_floor: None,
                },
            })
            .await;
    }

    // Delete files divisible by 5.
    for i in (0..CHURN_SIZE).step_by(5) {
        relay
            .process_command(RelayCommand::UnregisterDocument {
                conn_id: 1,
                msg: sync::UnregisterDocument {
                    space_id: TEST_SPACE.into(),
                    document_id: doc_id(i),
                    metadata: Some(meta("did:daemon-a", kutl_core::now_ms())),
                },
            })
            .await;
    }

    // Count expected broadcasts.
    let expected_broadcasts = CHURN_SIZE + CHURN_SIZE / 2 + CHURN_SIZE / 5;
    let payloads = drain_ctrl(&mut daemon_b);
    assert_eq!(
        payloads.len(),
        expected_broadcasts,
        "daemon B should receive all {expected_broadcasts} broadcasts"
    );

    // Verify final registry state.
    let reg = relay.registry(TEST_SPACE).unwrap();

    for i in 0..CHURN_SIZE {
        let deleted = i % 5 == 0;
        let renamed = i % 2 == 0;

        if deleted {
            assert!(reg.get(&doc_id(i)).is_none(), "churn-{i} should be deleted");
        } else if renamed {
            let entry = reg.get(&doc_id(i)).unwrap();
            assert_eq!(
                entry.path,
                format!("renamed/file-{i}.md"),
                "churn-{i} should be renamed"
            );
        } else {
            let entry = reg.get(&doc_id(i)).unwrap();
            assert_eq!(
                entry.path,
                format!("churn/file-{i}.md"),
                "churn-{i} should keep original path"
            );
        }
    }
}

#[tokio::test]
async fn test_concurrent_rename_both_daemons() {
    let mut relay = Relay::new_standalone(test_config());
    let mut daemon_a = connect_client(&mut relay, 1).await;
    let mut daemon_b = connect_client(&mut relay, 2).await;

    subscribe(&mut relay, 1, "anchor").await;
    subscribe(&mut relay, 2, "anchor").await;

    // Daemon A registers a document.
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-0000000000cc".into(),
                path: "original.md".into(),
                metadata: Some(meta("did:daemon-a", 1000)),
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
            },
        })
        .await;
    drain_ctrl(&mut daemon_a);
    drain_ctrl(&mut daemon_b);

    // Daemon A renames to path-a (earlier timestamp).
    relay
        .process_command(RelayCommand::RenameDocument {
            conn_id: 1,
            msg: sync::RenameDocument {
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-0000000000cc".into(),
                old_path: "original.md".into(),
                new_path: "path-a.md".into(),
                metadata: Some(meta("did:daemon-a", 2000)),
                rename_causal_floor: None,
            },
        })
        .await;

    // Daemon B renames to path-b (later timestamp — should win).
    relay
        .process_command(RelayCommand::RenameDocument {
            conn_id: 2,
            msg: sync::RenameDocument {
                space_id: TEST_SPACE.into(),
                document_id: "00000000-0000-0000-0000-0000000000cc".into(),
                old_path: "path-a.md".into(),
                new_path: "path-b.md".into(),
                metadata: Some(meta("did:daemon-b", 3000)),
                rename_causal_floor: None,
            },
        })
        .await;

    // The registry should reflect the last rename applied.
    let reg = relay.registry(TEST_SPACE).unwrap();
    let entry = reg.get("00000000-0000-0000-0000-0000000000cc").unwrap();
    assert_eq!(
        entry.path, "path-b.md",
        "later rename should take effect (last-writer-wins)"
    );

    // Both daemons should have received the other's rename broadcast.
    let a_payloads = drain_ctrl(&mut daemon_a);
    let b_payloads = drain_ctrl(&mut daemon_b);

    // Daemon A receives B's rename broadcast.
    assert!(
        a_payloads
            .iter()
            .any(|p| matches!(p, Payload::RenameDocument(msg) if msg.new_path == "path-b.md")),
        "daemon A should receive daemon B's rename"
    );

    // Daemon B receives A's rename broadcast.
    assert!(
        b_payloads
            .iter()
            .any(|p| matches!(p, Payload::RenameDocument(msg) if msg.new_path == "path-a.md")),
        "daemon B should receive daemon A's rename"
    );
}
