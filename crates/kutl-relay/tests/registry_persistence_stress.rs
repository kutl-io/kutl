//! Stress tests for registry persistence across relay restarts and crashes.
//!
//! Verifies that the relay's document registry survives restart cycles
//! and simulated crash scenarios (drop without graceful shutdown).

use std::sync::Arc;

use kutl_proto::sync;
use kutl_relay::config::{
    DEFAULT_OUTBOUND_CAPACITY, DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS,
    RelayConfig,
};
use kutl_relay::registry_store::SqliteBackend;
use kutl_relay::relay::{Relay, RelayCommand};
use std::path::PathBuf;
use tokio::sync::mpsc;

const TEST_SPACE: &str = "persist-space";

/// Number of documents to register.
const DOC_COUNT: usize = 100;

/// Number of documents to rename.
const RENAME_COUNT: usize = 20;

/// Number of documents to delete.
const DELETE_COUNT: usize = 10;

fn test_config(data_dir: PathBuf) -> RelayConfig {
    RelayConfig {
        host: "127.0.0.1".into(),
        port: 0,
        relay_name: "test-relay".into(),
        require_auth: false,
        database_url: None,
        outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
        data_dir: Some(data_dir),
        external_url: None,
        ux_url: None,
        authorized_keys_file: None,
        snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
        snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
    }
}

async fn connect_and_subscribe(relay: &mut Relay, conn_id: u64) {
    let (tx, _data_rx) = mpsc::channel(256);
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel(256);

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

    // Subscribe to an anchor document in the test space.
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id,
            msg: sync::Subscribe {
                space_id: TEST_SPACE.into(),
                document_id: "anchor".into(),
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

/// Create a relay with `SQLite` persistence from the given data directory.
async fn relay_with_sqlite(data_dir: &std::path::Path) -> Relay {
    let backend = SqliteBackend::open(data_dir).await.unwrap();
    Relay::new_standalone_with_backend(
        test_config(data_dir.to_path_buf()),
        Some(Arc::new(backend)),
        None,
        None,
        None,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_relay_restart_preserves_registry() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: register, rename, and delete documents.
    {
        let mut relay = relay_with_sqlite(dir.path()).await;
        connect_and_subscribe(&mut relay, 1).await;

        // Register DOC_COUNT documents.
        for i in 0..DOC_COUNT {
            relay
                .process_command(RelayCommand::RegisterDocument {
                    conn_id: 1,
                    msg: sync::RegisterDocument {
                        space_id: TEST_SPACE.into(),
                        document_id: format!("doc-{i}"),
                        path: format!("files/doc-{i}.md"),
                        metadata: Some(meta("did:alice")),
                    },
                })
                .await;
        }

        // Rename RENAME_COUNT documents.
        for i in 0..RENAME_COUNT {
            relay
                .process_command(RelayCommand::RenameDocument {
                    conn_id: 1,
                    msg: sync::RenameDocument {
                        space_id: TEST_SPACE.into(),
                        document_id: format!("doc-{i}"),
                        old_path: format!("files/doc-{i}.md"),
                        new_path: format!("renamed/doc-{i}.md"),
                        metadata: Some(meta("did:alice")),
                    },
                })
                .await;
        }

        // Delete DELETE_COUNT documents (starting from the end).
        for i in (DOC_COUNT - DELETE_COUNT)..DOC_COUNT {
            relay
                .process_command(RelayCommand::UnregisterDocument {
                    conn_id: 1,
                    msg: sync::UnregisterDocument {
                        space_id: TEST_SPACE.into(),
                        document_id: format!("doc-{i}"),
                        metadata: Some(meta("did:alice")),
                    },
                })
                .await;
        }

        // Relay drops here — persisted state should be written.
    }

    // Phase 2: restart relay from same data dir.
    {
        let relay = relay_with_sqlite(dir.path()).await;
        let reg = relay.registry(TEST_SPACE).expect("registry should load");

        let active_count = DOC_COUNT - DELETE_COUNT;
        assert_eq!(
            reg.len(),
            active_count,
            "should have {active_count} active documents after restart"
        );

        // Verify renamed documents have correct paths.
        for i in 0..RENAME_COUNT {
            let entry = reg
                .get(&format!("doc-{i}"))
                .unwrap_or_else(|| panic!("doc-{i} should exist"));
            assert_eq!(
                entry.path,
                format!("renamed/doc-{i}.md"),
                "doc-{i} should have renamed path"
            );
        }

        // Verify non-renamed, non-deleted documents.
        for i in RENAME_COUNT..(DOC_COUNT - DELETE_COUNT) {
            let entry = reg
                .get(&format!("doc-{i}"))
                .unwrap_or_else(|| panic!("doc-{i} should exist"));
            assert_eq!(
                entry.path,
                format!("files/doc-{i}.md"),
                "doc-{i} should have original path"
            );
        }

        // Verify deleted documents are gone.
        for i in (DOC_COUNT - DELETE_COUNT)..DOC_COUNT {
            assert!(
                reg.get(&format!("doc-{i}")).is_none(),
                "doc-{i} should be deleted"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_relay_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: register documents, then drop abruptly (simulating crash).
    {
        let mut relay = relay_with_sqlite(dir.path()).await;
        connect_and_subscribe(&mut relay, 1).await;

        for i in 0..DOC_COUNT {
            relay
                .process_command(RelayCommand::RegisterDocument {
                    conn_id: 1,
                    msg: sync::RegisterDocument {
                        space_id: TEST_SPACE.into(),
                        document_id: format!("crash-doc-{i}"),
                        path: format!("crash/file-{i}.md"),
                        metadata: Some(meta("did:alice")),
                    },
                })
                .await;
        }

        // Abrupt drop — no graceful shutdown.
        // The registry uses atomic writes (write-to-temp + rename),
        // so partial writes should not corrupt the file.
        drop(relay);
    }

    // Phase 2: restart — registry should be intact.
    {
        let relay = relay_with_sqlite(dir.path()).await;
        let reg = relay
            .registry(TEST_SPACE)
            .expect("registry should survive crash");

        assert_eq!(
            reg.len(),
            DOC_COUNT,
            "all {DOC_COUNT} documents should survive crash"
        );

        for i in 0..DOC_COUNT {
            let entry = reg
                .get(&format!("crash-doc-{i}"))
                .unwrap_or_else(|| panic!("crash-doc-{i} should exist"));
            assert_eq!(entry.path, format!("crash/file-{i}.md"));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multiple_restart_cycles() {
    /// Documents added per restart cycle.
    const DOCS_PER_CYCLE: usize = 10;
    /// Number of restart cycles.
    const CYCLES: usize = 5;

    let dir = tempfile::tempdir().unwrap();

    for cycle in 0..CYCLES {
        let mut relay = relay_with_sqlite(dir.path()).await;
        connect_and_subscribe(&mut relay, 1).await;

        let base = cycle * DOCS_PER_CYCLE;
        for i in 0..DOCS_PER_CYCLE {
            let doc_idx = base + i;
            relay
                .process_command(RelayCommand::RegisterDocument {
                    conn_id: 1,
                    msg: sync::RegisterDocument {
                        space_id: TEST_SPACE.into(),
                        document_id: format!("cycle-doc-{doc_idx}"),
                        path: format!("cycle-{cycle}/file-{i}.md"),
                        metadata: Some(meta("did:alice")),
                    },
                })
                .await;
        }
    }

    // Final verification.
    let relay = relay_with_sqlite(dir.path()).await;
    let reg = relay.registry(TEST_SPACE).expect("registry should load");
    let total = CYCLES * DOCS_PER_CYCLE;

    assert_eq!(
        reg.len(),
        total,
        "should have {total} documents after {CYCLES} restart cycles"
    );

    for cycle in 0..CYCLES {
        for i in 0..DOCS_PER_CYCLE {
            let doc_idx = cycle * DOCS_PER_CYCLE + i;
            let entry = reg.get(&format!("cycle-doc-{doc_idx}")).unwrap();
            assert_eq!(entry.path, format!("cycle-{cycle}/file-{i}.md"));
        }
    }
}
