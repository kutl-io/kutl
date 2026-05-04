use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use kutl_core::{Boundary, Document};
use kutl_daemon::DocumentManager;
use kutl_daemon::bridge;
use kutl_daemon::client::{SyncCommand, SyncEvent};
use kutl_daemon::daemon::{SpaceWorker, SpaceWorkerConfig};

use kutl_relay::config::{DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS, RelayConfig};
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc};

/// Channel capacity used throughout tests.
const TEST_CHANNEL_CAPACITY: usize = 16;

/// Poll interval for loops that check file content or metadata changes.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Timeout for waiting on a single async event (channel recv, WS connect).
const EVENT_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for waiting on multi-step async operations (file sync pipelines).
const PIPELINE_TIMEOUT: Duration = Duration::from_secs(30);

/// Delay for OS-level file watcher registration before writing test files.
/// Required because `notify::RecommendedWatcher` initialization is async at
/// the OS level (e.g. kqueue/FSEvents registration) and the watcher debounces
/// raw events on a 200ms cycle, so retry-style "write until observed" costs
/// more than it saves.
const WATCHER_INIT_DELAY: Duration = Duration::from_millis(200);

/// Create a tempdir with a canonical path (resolves symlinks like /var → /private/var on macOS).
/// This is required because `notify` reports events using real paths, so `strip_prefix` in the
/// file watcher only works when the watched root is also canonical.
fn canonical_tempdir() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let canonical = dir.path().canonicalize().unwrap();
    (dir, canonical)
}

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Build a `SpaceWorkerConfig` with test defaults.
///
/// Only the fields that vary between tests are parameters. Everything else
/// uses sensible defaults (`one_shot: false`, no signing key, etc.).
fn test_daemon_config(
    space_root: PathBuf,
    author_did: &str,
    relay_url: &str,
    space_id: &str,
) -> SpaceWorkerConfig {
    SpaceWorkerConfig {
        space_root,
        author_did: author_did.into(),
        relay_url: relay_url.into(),
        space_id: space_id.into(),
        signing_key: None,
        one_shot: false,
        display_name: String::new(),
        ready: None,
        cancel: tokio_util::sync::CancellationToken::new(),
    }
}

/// Spawn a space worker and wait until it's fully ready (connected, subscribed,
/// initial scan done, watcher running). Returns the task handle.
async fn spawn_ready_daemon(config: SpaceWorkerConfig) -> tokio::task::JoinHandle<()> {
    let ready = Arc::new(Notify::new());
    let daemon = SpaceWorker::new(SpaceWorkerConfig {
        ready: Some(Arc::clone(&ready)),
        ..config
    })
    .unwrap();
    let handle = tokio::spawn(async move {
        if let Err(e) = daemon.run().await {
            eprintln!("daemon exited with error: {e}");
        }
    });
    // Race: either the daemon becomes ready, or the task exits early
    // (e.g. auth rejection). If the task exits first, `ready` will never
    // fire, so we detect that and panic with a clear message.
    tokio::select! {
        () = ready.notified() => {}
        () = async {
            // Poll until the handle is finished. We can't consume it
            // (we need to return it), so we poll periodically.
            loop {
                if handle.is_finished() { return; }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        } => {
            panic!("daemon exited before becoming ready (auth rejection?)");
        }
    }
    handle
}

/// Start a relay on a random port and return the ws:// URL.
async fn start_relay() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        host: "127.0.0.1".into(),
        port: 0,
        relay_name: "test-relay".into(),
        require_auth: false,
        database_url: None,
        outbound_capacity: kutl_relay::config::DEFAULT_OUTBOUND_CAPACITY,
        data_dir: None,
        external_url: None,
        ux_url: None,
        authorized_keys_file: None,
        snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
        snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    format!("ws://{addr}/ws")
}

/// Copy a tracked document (file + .dt sidecar + state.json UUID) from one
/// space to another.
///
/// This ensures both spaces share the same CRDT history and UUID mappings,
/// avoiding merge conflicts and document ID mismatches.
fn copy_tracked_doc(src_root: &Path, dst_root: &Path, rel_path: &Path) {
    // Copy the file itself.
    let dst_file = dst_root.join(rel_path);
    if let Some(parent) = dst_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::copy(src_root.join(rel_path), &dst_file).unwrap();

    // Copy the .dt sidecar.
    let dt_name = format!("{}.dt", rel_path.display());
    let src_dt = src_root.join(".kutl/docs").join(&dt_name);
    let dst_dt = dst_root.join(".kutl/docs").join(&dt_name);
    if let Some(parent) = dst_dt.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::copy(src_dt, dst_dt).unwrap();

    // Copy the changes sidecar if it exists.
    let changes_name = format!("{dt_name}.changes");
    let src_changes = src_root.join(".kutl/docs").join(&changes_name);
    let dst_changes = dst_root.join(".kutl/docs").join(&changes_name);
    if src_changes.exists() {
        std::fs::copy(src_changes, dst_changes).unwrap();
    }

    // Copy state.json (UUID mappings) so both daemons use the same document IDs.
    let src_state = src_root.join(".kutl/state.json");
    let dst_state = dst_root.join(".kutl/state.json");
    if src_state.exists() {
        std::fs::copy(src_state, dst_state).unwrap();
    }
}

/// Initialize a tracked document with given content.
fn init_tracked_doc(dir: &Path, rel_path: &Path, author: &str, content: &str) {
    std::fs::write(dir.join(rel_path), content).unwrap();
    let mut mgr = DocumentManager::new(dir.to_owned()).unwrap();
    let doc = mgr.load_or_create(rel_path).unwrap();
    // Use a short agent name for the CRDT; author is only for metadata.
    let agent = doc.register_agent("test-init").unwrap();
    doc.edit(agent, author, "init", Boundary::Auto, |ctx| {
        ctx.insert(0, content)
    })
    .unwrap();
    mgr.save(rel_path).unwrap();
}

/// Pre-seed a document UUID in `.kutl/state.json` so the daemon uses a
/// known, deterministic document ID instead of generating a random UUID.
fn seed_document_uuid(dir: &Path, rel_path: &str, uuid: &str) {
    let state_path = dir.join(".kutl/state.json");
    let mut state: serde_json::Value = if state_path.exists() {
        serde_json::from_str(&std::fs::read_to_string(&state_path).unwrap()).unwrap()
    } else {
        serde_json::json!({ "documents": {} })
    };
    state["documents"][rel_path] = serde_json::Value::String(uuid.to_owned());
    std::fs::write(&state_path, serde_json::to_string_pretty(&state).unwrap()).unwrap();
}

/// Connect a WS client, wait for Connected, and subscribe to a document.
async fn connect_and_subscribe(
    relay_url: &str,
    space_id: &str,
    client_name: &str,
    document_id: &str,
) -> (mpsc::Sender<SyncCommand>, mpsc::Receiver<SyncEvent>) {
    let (cmd_tx, cmd_rx) = mpsc::channel::<SyncCommand>(TEST_CHANNEL_CAPACITY);
    let (evt_tx, mut evt_rx) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    let url = relay_url.to_owned();
    let sid = space_id.to_owned();
    let name = client_name.to_owned();
    tokio::spawn(async move {
        kutl_daemon::client::run_client(&url, &sid, &name, "", "", cmd_rx, evt_tx)
            .await
            .ok();
    });

    let event = tokio::time::timeout(EVENT_TIMEOUT, evt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, SyncEvent::Connected));

    cmd_tx
        .send(SyncCommand::Subscribe {
            document_id: document_id.to_owned(),
        })
        .await
        .unwrap();

    (cmd_tx, evt_rx)
}

/// Set up a temp space directory with `.kutl/space.json`.
fn setup_space(dir: &Path, space_id: &str, relay_url: &str) {
    let kutl_dir = dir.join(".kutl");
    std::fs::create_dir_all(kutl_dir.join("docs")).unwrap();
    let config = serde_json::json!({
        "space_id": space_id,
        "relay_url": relay_url,
    });
    std::fs::write(
        kutl_dir.join("space.json"),
        serde_json::to_string_pretty(&config).unwrap(),
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Bridge tests
// ---------------------------------------------------------------------------

#[test]
fn test_bridge_diff_insert() {
    let mut doc = Document::new();
    let agent = doc.register_agent("test").unwrap();

    bridge::apply_file_change(&mut doc, agent, "did:test", "hello world").unwrap();
    assert_eq!(doc.content(), "hello world");
    assert_eq!(doc.changes().len(), 1);
}

#[test]
fn test_bridge_diff_edit() {
    let mut doc = Document::new();
    let agent = doc.register_agent("test").unwrap();

    bridge::apply_file_change(&mut doc, agent, "did:test", "hello world").unwrap();
    bridge::apply_file_change(&mut doc, agent, "did:test", "hello rust").unwrap();

    assert_eq!(doc.content(), "hello rust");
    assert_eq!(doc.changes().len(), 2);
}

#[test]
fn test_bridge_roundtrip() {
    let mut doc = Document::new();
    let agent = doc.register_agent("test").unwrap();

    let content = "fn main() {\n    println!(\"hello\");\n}\n";
    bridge::apply_file_change(&mut doc, agent, "did:test", content).unwrap();
    assert_eq!(doc.content(), content);
}

// ---------------------------------------------------------------------------
// Sync client integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_client_connect_and_subscribe() {
    let relay_url = start_relay().await;

    let (cmd_tx, cmd_rx) = mpsc::channel::<SyncCommand>(TEST_CHANNEL_CAPACITY);
    let (event_tx, mut event_rx) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    let url = relay_url.clone();
    let client_handle = tokio::spawn(async move {
        kutl_daemon::client::run_client(&url, "test-space", "did:test", "", "", cmd_rx, event_tx)
            .await
            .ok();
    });

    // Wait for Connected event.
    let event = tokio::time::timeout(EVENT_TIMEOUT, event_rx.recv())
        .await
        .expect("timeout waiting for Connected")
        .expect("channel closed");
    assert!(matches!(event, SyncEvent::Connected));

    // Subscribe to a document.
    cmd_tx
        .send(SyncCommand::Subscribe {
            document_id: "test.txt".into(),
        })
        .await
        .unwrap();

    // Drop command sender to trigger disconnect.
    drop(cmd_tx);

    let event = tokio::time::timeout(EVENT_TIMEOUT, event_rx.recv())
        .await
        .expect("timeout waiting for Disconnected")
        .expect("channel closed");
    assert!(matches!(event, SyncEvent::Disconnected));

    client_handle.abort();
}

#[tokio::test]
async fn test_client_send_and_receive_ops() {
    let relay_url = start_relay().await;

    // Client Alice
    let (alice_cmd, alice_cmd_rx) = mpsc::channel::<SyncCommand>(TEST_CHANNEL_CAPACITY);
    let (alice_evt_tx, mut alice_events) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    let url_alice = relay_url.clone();
    let _handle_alice = tokio::spawn(async move {
        kutl_daemon::client::run_client(
            &url_alice,
            "proj",
            "did:alice",
            "",
            "",
            alice_cmd_rx,
            alice_evt_tx,
        )
        .await
        .ok();
    });

    // Wait for Alice connected
    let event = tokio::time::timeout(EVENT_TIMEOUT, alice_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, SyncEvent::Connected));

    alice_cmd
        .send(SyncCommand::Subscribe {
            document_id: "doc".into(),
        })
        .await
        .unwrap();

    // Drain Alice's catch-up (empty ops for new doc).
    let event = tokio::time::timeout(EVENT_TIMEOUT, alice_events.recv())
        .await
        .expect("alice catch-up timeout")
        .expect("alice channel closed");
    assert!(
        matches!(&event, SyncEvent::RemoteOps { ops, .. } if ops.is_empty()),
        "expected empty catch-up, got {event:?}"
    );

    // Client Bob
    let (bob_cmd, bob_cmd_rx) = mpsc::channel::<SyncCommand>(TEST_CHANNEL_CAPACITY);
    let (bob_evt_tx, mut bob_events) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    let url_bob = relay_url.clone();
    let _handle_bob = tokio::spawn(async move {
        kutl_daemon::client::run_client(
            &url_bob, "proj", "did:bob", "", "", bob_cmd_rx, bob_evt_tx,
        )
        .await
        .ok();
    });

    let event = tokio::time::timeout(EVENT_TIMEOUT, bob_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, SyncEvent::Connected));

    bob_cmd
        .send(SyncCommand::Subscribe {
            document_id: "doc".into(),
        })
        .await
        .unwrap();

    // Drain Bob's catch-up (empty ops for new doc).
    let event = tokio::time::timeout(EVENT_TIMEOUT, bob_events.recv())
        .await
        .expect("bob catch-up timeout")
        .expect("bob channel closed");
    assert!(
        matches!(&event, SyncEvent::RemoteOps { ops, .. } if ops.is_empty()),
        "expected empty catch-up, got {event:?}"
    );

    // A sends ops.
    let mut doc = Document::new();
    let agent = doc.register_agent("alice").unwrap();
    doc.edit(agent, "alice", "add greeting", Boundary::Explicit, |ctx| {
        ctx.insert(0, "hello from A")
    })
    .unwrap();

    let ops = doc.encode_since(&[]);
    let metadata = doc.changes_since(&[]);

    alice_cmd
        .send(SyncCommand::SendOps {
            document_id: "doc".into(),
            ops,
            metadata,
            content_mode: 0,
            content_hash: Vec::new(),
        })
        .await
        .unwrap();

    // Bob should receive the ops.
    let event = tokio::time::timeout(EVENT_TIMEOUT, bob_events.recv())
        .await
        .expect("timeout")
        .expect("closed");

    match event {
        SyncEvent::RemoteOps {
            document_id, ops, ..
        } => {
            assert_eq!(document_id, "doc");
            let mut recv_doc = Document::new();
            recv_doc.merge(&ops, &[]).unwrap();
            assert_eq!(recv_doc.content(), "hello from A");
        }
        other => panic!("expected RemoteOps, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Watcher smoke test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_watcher_detects_file_change() {
    use kutl_daemon::{FileEvent, FileWatcher};

    let (_dir, dir_path) = canonical_tempdir();
    let (tx, mut rx) = mpsc::channel::<FileEvent>(TEST_CHANNEL_CAPACITY);
    let (_suppress_tx, suppress_rx) = mpsc::channel::<PathBuf>(TEST_CHANNEL_CAPACITY);

    let mut watcher = FileWatcher::new(&dir_path, tx, suppress_rx).unwrap();
    let watcher_handle = tokio::spawn(async move { watcher.run().await });

    // Give watcher time to register with the OS.
    tokio::time::sleep(WATCHER_INIT_DELAY).await;

    // Create a new file.
    std::fs::write(dir_path.join("test.txt"), "hello").unwrap();

    let result = tokio::time::timeout(EVENT_TIMEOUT, rx.recv()).await;
    assert!(result.is_ok(), "watcher should detect file creation");
    let event = result.unwrap().unwrap();
    match event {
        FileEvent::Modified { rel_path } | FileEvent::Removed { rel_path } => {
            assert_eq!(rel_path.to_string_lossy(), "test.txt");
        }
        FileEvent::Renamed { old_path, new_path } => {
            // Creation may surface as a rename on some platforms.
            assert!(
                old_path.to_string_lossy().contains("test.txt")
                    || new_path.to_string_lossy().contains("test.txt"),
                "rename should involve test.txt"
            );
        }
    }

    watcher_handle.abort();
}

// ---------------------------------------------------------------------------
// Daemon integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_daemon_local_to_relay() {
    let relay_url = start_relay().await;
    let (_dir, dir_path) = canonical_tempdir();
    let space_id = "test-local-to-relay";
    let doc_uuid = "test-uuid-local-to-relay";

    setup_space(&dir_path, space_id, &relay_url);

    // Create initial tracked file and its .dt counterpart.
    let rel_path = Path::new("test.txt");
    let abs_path = dir_path.join(rel_path);
    init_tracked_doc(&dir_path, rel_path, "did:test", "initial content");

    // Pre-seed UUID so the daemon uses a known document ID.
    seed_document_uuid(&dir_path, "test.txt", doc_uuid);

    // Set up a separate WS client to observe what the relay receives.
    let (_observer_cmd, mut observer_event_rx) =
        connect_and_subscribe(&relay_url, space_id, "did:observer", doc_uuid).await;

    // Start daemon and wait for it to be ready.
    let daemon_handle = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:test",
        &relay_url,
        space_id,
    ))
    .await;

    // Modify the file.
    std::fs::write(&abs_path, "modified content").unwrap();

    // Wait for the file event to propagate through daemon → relay → observer.
    // Accumulate ops into a single document (daemon may send multiple messages).
    let result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        let mut observer_doc = Document::new();
        loop {
            if let Some(SyncEvent::RemoteOps { ops, .. }) = observer_event_rx.recv().await {
                observer_doc.merge(&ops, &[]).unwrap();
                let content = observer_doc.content();
                if content.contains("modified") {
                    return content;
                }
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for modified content on observer"
    );

    daemon_handle.abort();
}

#[tokio::test]
async fn test_daemon_relay_to_local() {
    let relay_url = start_relay().await;
    let (_dir, dir_path) = canonical_tempdir();
    let space_id = "test-relay-to-local";
    let doc_uuid = "test-uuid-relay-to-local";

    setup_space(&dir_path, space_id, &relay_url);

    // Create a tracked document with initial content.
    let rel_path = Path::new("test.txt");
    let abs_path = dir_path.join(rel_path);
    init_tracked_doc(&dir_path, rel_path, "did:daemon", "old content");

    // Pre-seed UUID so the daemon uses a known document ID.
    seed_document_uuid(&dir_path, "test.txt", doc_uuid);

    // Start daemon and wait for it to be ready.
    let daemon_handle = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:daemon",
        &relay_url,
        space_id,
    ))
    .await;

    // External client sends ops to the same document via relay.
    let (ext_cmd_tx, mut ext_event_rx) =
        connect_and_subscribe(&relay_url, space_id, "did:external", doc_uuid).await;

    // Wait for the external client to receive catch-up from the relay. This
    // confirms the relay has processed the daemon's subscription and initial
    // state push — without this barrier, the external client's ops could
    // arrive at the relay before the daemon's state, causing a merge failure.
    let catch_up = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            match ext_event_rx.recv().await {
                Some(SyncEvent::RemoteOps { ops, .. }) if !ops.is_empty() => return,
                None => panic!("external client disconnected before receiving catch-up"),
                _ => {}
            }
        }
    })
    .await;
    assert!(catch_up.is_ok(), "timed out waiting for relay catch-up");

    // Create ops that extend the document.
    let mut ext_doc = Document::new();
    // First load the existing state.
    let mgr2 = DocumentManager::new(dir_path.clone()).unwrap();
    let dt_path = mgr2.dt_path(rel_path);
    if dt_path.exists() {
        ext_doc = Document::load(&dt_path).unwrap();
    }
    let ext_agent = ext_doc.register_agent("did:external").unwrap();
    let version_before = ext_doc.local_version();
    ext_doc
        .edit(
            ext_agent,
            "did:external",
            "remote edit",
            Boundary::Explicit,
            |ctx| {
                ctx.insert(0, "REMOTE: ")?;
                Ok(())
            },
        )
        .unwrap();

    let ops = ext_doc.encode_since(&version_before);
    let metadata = ext_doc.changes_since(&version_before);

    ext_cmd_tx
        .send(SyncCommand::SendOps {
            document_id: doc_uuid.into(),
            ops,
            metadata,
            content_mode: 0,
            content_hash: Vec::new(),
        })
        .await
        .unwrap();

    // Wait for the daemon to receive the ops and write the file.
    let result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if let Ok(content) = std::fs::read_to_string(&abs_path)
                && content.contains("REMOTE: ")
            {
                return content;
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for remote content in file"
    );
    let content = result.unwrap();
    assert!(content.contains("REMOTE: "), "got: {content}");

    daemon_handle.abort();
}

#[tokio::test]
async fn test_two_daemons_converge() {
    let relay_url = start_relay().await;
    let space_id = "test-converge";
    let doc_uuid = "test-uuid-converge";

    let (_dir_a, path_a) = canonical_tempdir();
    let (_dir_b, path_b) = canonical_tempdir();

    setup_space(&path_a, space_id, &relay_url);
    setup_space(&path_b, space_id, &relay_url);

    let rel_path = Path::new("shared.txt");

    // Create tracked doc in A, then copy to B (same CRDT history).
    init_tracked_doc(&path_a, rel_path, "did:a", "line1\nline2\nline3\n");

    // Pre-seed UUID in A, then copy (state.json is copied by copy_tracked_doc).
    seed_document_uuid(&path_a, "shared.txt", doc_uuid);
    copy_tracked_doc(&path_a, &path_b, rel_path);

    // Start daemon A and write modified content BEFORE starting B.
    // This avoids a race: if both daemons start simultaneously, the sync
    // exchange (each pushing identical ops) causes handle_remote_text to
    // overwrite test file changes via its suppress mechanism.
    let handle_a = spawn_ready_daemon(test_daemon_config(
        path_a.clone(),
        "did:daemon-a",
        &relay_url,
        space_id,
    ))
    .await;

    // Modify A's file while only A is running (no sync exchange race).
    std::fs::write(path_a.join(rel_path), "line1\nMODIFIED\nline3\n").unwrap();

    // Give the watcher time to detect and process the change.
    // Use an observer to verify the modification reached the relay.
    let (_obs_cmd, mut obs_events) =
        connect_and_subscribe(&relay_url, space_id, "did:observer", doc_uuid).await;

    let relay_has_modified = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        let mut doc = Document::new();
        loop {
            if let Some(SyncEvent::RemoteOps { ops, .. }) = obs_events.recv().await {
                doc.merge(&ops, &[]).unwrap();
                if doc.content().contains("MODIFIED") {
                    return;
                }
            }
        }
    })
    .await;
    assert!(
        relay_has_modified.is_ok(),
        "modification never reached relay"
    );

    // Now start daemon B. It will receive the modified content via catch-up.
    let handle_b = spawn_ready_daemon(test_daemon_config(
        path_b.clone(),
        "did:daemon-b",
        &relay_url,
        space_id,
    ))
    .await;

    // Poll B's file until content matches.
    let b_path = path_b.join(rel_path);
    let result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if let Ok(content) = std::fs::read_to_string(&b_path)
                && content.contains("MODIFIED")
            {
                return content;
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for B's file to contain MODIFIED"
    );

    handle_a.abort();
    handle_b.abort();
}

// ---------------------------------------------------------------------------
// Blob integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_binary_file_syncs_to_relay() {
    let relay_url = start_relay().await;
    let space_id = "test-blob-sync";
    let doc_uuid = "test-uuid-blob-sync";

    let (_dir, dir_path) = canonical_tempdir();
    setup_space(&dir_path, space_id, &relay_url);

    // Pre-seed UUID for the binary file the daemon will discover.
    seed_document_uuid(&dir_path, "image.bin", doc_uuid);

    // Set up an observer client to verify the blob reaches the relay.
    let (_obs_cmd, mut obs_events) =
        connect_and_subscribe(&relay_url, space_id, "did:observer", doc_uuid).await;

    // Start daemon.
    let daemon_handle = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:daemon",
        &relay_url,
        space_id,
    ))
    .await;

    // Write a binary file (non-UTF-8 content).
    let binary_content: Vec<u8> = (0u8..=255).collect();
    std::fs::write(dir_path.join("image.bin"), &binary_content).unwrap();

    // Observer should receive the blob.
    let result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if let Some(SyncEvent::RemoteOps {
                ops, content_mode, ..
            }) = obs_events.recv().await
                && content_mode == i32::from(kutl_proto::sync::ContentMode::Blob)
            {
                return ops;
            }
        }
    })
    .await;

    assert!(result.is_ok(), "timed out waiting for blob at observer");
    assert_eq!(result.unwrap(), binary_content);

    daemon_handle.abort();
}

#[tokio::test]
async fn test_binary_and_text_coexist() {
    let relay_url = start_relay().await;
    let space_id = "test-coexist";
    let text_uuid = "test-uuid-coexist-text";
    let bin_uuid = "test-uuid-coexist-bin";

    let (_dir, dir_path) = canonical_tempdir();
    setup_space(&dir_path, space_id, &relay_url);

    // Create an initial text file (tracked).
    let text_rel = Path::new("readme.txt");
    init_tracked_doc(&dir_path, text_rel, "did:daemon", "hello text");

    // Pre-seed UUIDs for both documents.
    seed_document_uuid(&dir_path, "readme.txt", text_uuid);
    seed_document_uuid(&dir_path, "data.bin", bin_uuid);

    // Set up observers for both documents.
    let (_obs_text_cmd, mut obs_text_events) =
        connect_and_subscribe(&relay_url, space_id, "did:obs-text", text_uuid).await;
    let (_obs_bin_cmd, mut obs_bin_events) =
        connect_and_subscribe(&relay_url, space_id, "did:obs-bin", bin_uuid).await;

    // Start daemon.
    let daemon_handle = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:daemon",
        &relay_url,
        space_id,
    ))
    .await;

    // Write a binary file.
    let binary_content = vec![0xFF, 0xFE, 0x00, 0x01, 0x80];
    std::fs::write(dir_path.join("data.bin"), &binary_content).unwrap();

    // Modify the text file.
    std::fs::write(dir_path.join("readme.txt"), "updated text").unwrap();

    // Wait for the binary blob at the bin observer.
    let bin_result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if let Some(SyncEvent::RemoteOps {
                ops, content_mode, ..
            }) = obs_bin_events.recv().await
                && content_mode == i32::from(kutl_proto::sync::ContentMode::Blob)
            {
                return ops;
            }
        }
    })
    .await;

    assert!(bin_result.is_ok(), "timed out waiting for blob");
    assert_eq!(bin_result.unwrap(), binary_content);

    // Wait for the text update at the text observer.
    // Accumulate all ops (including initial catch-up) into a single document.
    let text_result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        let mut doc = Document::new();
        loop {
            if let Some(SyncEvent::RemoteOps {
                ops, content_mode, ..
            }) = obs_text_events.recv().await
            {
                // Skip blob ops on this channel (shouldn't happen but be safe).
                if content_mode == i32::from(kutl_proto::sync::ContentMode::Blob) {
                    continue;
                }
                doc.merge(&ops, &[]).unwrap();
                if doc.content().contains("updated") {
                    return doc.content();
                }
            }
        }
    })
    .await;

    assert!(text_result.is_ok(), "timed out waiting for text update");

    daemon_handle.abort();
}

#[tokio::test]
async fn test_blob_lww_newer_wins_daemon() {
    let relay_url = start_relay().await;
    let space_id = "test-blob-lww";
    let doc_uuid = "test-uuid-blob-lww";

    let (_dir, dir_path) = canonical_tempdir();
    setup_space(&dir_path, space_id, &relay_url);

    // Pre-seed UUID for the binary file the daemon will discover.
    seed_document_uuid(&dir_path, "data.bin", doc_uuid);

    // Subscribe the external client BEFORE the daemon starts. This ensures it's
    // already subscribed when the daemon sends the initial blob, so it arrives
    // as a relay (matching the proven pattern from test_binary_file_syncs_to_relay).
    let (ext_cmd_tx, mut ext_event_rx) =
        connect_and_subscribe(&relay_url, space_id, "did:external", doc_uuid).await;

    // Start daemon so the watcher is running.
    let daemon_handle = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:local",
        &relay_url,
        space_id,
    ))
    .await;

    // Write initial binary file AFTER watcher is running so it gets detected.
    // Content must contain invalid UTF-8 so the daemon treats it as binary.
    let old_content: Vec<u8> = (0xF0..=0xFF).collect();
    std::fs::write(dir_path.join("data.bin"), &old_content).unwrap();

    // Wait for the daemon's initial blob to reach the external client (via relay).
    let initial_result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if let Some(SyncEvent::RemoteOps { content_mode, .. }) = ext_event_rx.recv().await
                && content_mode == i32::from(kutl_proto::sync::ContentMode::Blob)
            {
                return;
            }
        }
    })
    .await;
    assert!(
        initial_result.is_ok(),
        "timed out waiting for daemon's initial blob"
    );

    let new_content = vec![0x02; 20];
    let hash = kutl_daemon::sha256_bytes(&new_content);

    // Use a far-future timestamp to guarantee it wins LWW.
    let meta = kutl_proto::sync::ChangeMetadata {
        timestamp: 9_999_999_999_999,
        author_did: "did:external".into(),
        intent: "remote update".into(),
        ..Default::default()
    };

    ext_cmd_tx
        .send(SyncCommand::SendOps {
            document_id: doc_uuid.into(),
            ops: new_content.clone(),
            metadata: vec![meta],
            content_mode: i32::from(kutl_proto::sync::ContentMode::Blob),
            content_hash: hash,
        })
        .await
        .unwrap();

    // Wait for daemon to receive and write the newer blob.
    let file_path = dir_path.join("data.bin");
    let result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if let Ok(content) = std::fs::read(&file_path)
                && content == new_content
            {
                return content;
            }
        }
    })
    .await;

    assert!(result.is_ok(), "timed out waiting for newer blob to win");
    assert_eq!(result.unwrap(), new_content);

    daemon_handle.abort();
}

#[tokio::test]
async fn test_daemon_handles_binary_file_without_crashing() {
    let relay_url = start_relay().await;
    let space_id = "test-blob-resilience";
    let doc_uuid = "test-uuid-blob-resilience";

    let (_dir, dir_path) = canonical_tempdir();
    setup_space(&dir_path, space_id, &relay_url);

    // Pre-seed UUID for the binary file the daemon will discover.
    seed_document_uuid(&dir_path, "small.bin", doc_uuid);

    // Start daemon.
    let daemon_handle = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:local",
        &relay_url,
        space_id,
    ))
    .await;

    // Set up an observer for the binary file's document.
    let (_obs_cmd, mut obs_events) =
        connect_and_subscribe(&relay_url, space_id, "did:observer", doc_uuid).await;

    // Write a binary file and verify it reaches the observer.
    let binary_content = vec![0xAB; 16];
    std::fs::write(dir_path.join("small.bin"), &binary_content).unwrap();

    // Wait for the blob to arrive at the observer.
    let result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if let Some(SyncEvent::RemoteOps {
                ops, content_mode, ..
            }) = obs_events.recv().await
                && content_mode == i32::from(kutl_proto::sync::ContentMode::Blob)
            {
                return ops;
            }
        }
    })
    .await;

    assert!(result.is_ok(), "timed out waiting for binary blob");
    assert_eq!(result.unwrap(), binary_content);

    // Daemon should still be running.
    assert!(
        !daemon_handle.is_finished(),
        "daemon should still be running"
    );

    daemon_handle.abort();
}

// ---------------------------------------------------------------------------
// Auth integration tests
// ---------------------------------------------------------------------------

/// Ed25519 multicodec prefix.
const ED25519_MULTICODEC: [u8; 2] = [0xed, 0x01];

/// Start a relay with `require_auth: true` and return the ws:// URL.
///
/// Returns `(url, _keys_file)` — the keys file must outlive the test because
/// the relay's startup check refuses `require_auth: true` without either a
/// membership backend or an `authorized_keys` file.
async fn start_relay_with_auth() -> (String, tempfile::NamedTempFile) {
    use std::io::Write;
    let mut keys = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys, "# daemon auth test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        host: "127.0.0.1".into(),
        port: 0,
        relay_name: "test-relay-auth".into(),
        require_auth: true,
        database_url: None,
        outbound_capacity: kutl_relay::config::DEFAULT_OUTBOUND_CAPACITY,
        data_dir: None,
        external_url: None,
        ux_url: None,
        authorized_keys_file: Some(keys.path().to_path_buf()),
        snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
        snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("ws://{addr}/ws"), keys)
}

/// Generate an ephemeral Ed25519 identity for testing.
fn test_identity() -> (String, SigningKey) {
    let secret: [u8; 32] = std::array::from_fn(|_| rand::random::<u8>());
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key();

    let mut multicodec_key = Vec::with_capacity(ED25519_MULTICODEC.len() + 32);
    multicodec_key.extend_from_slice(&ED25519_MULTICODEC);
    multicodec_key.extend_from_slice(public_key.as_bytes());
    let did = format!("did:key:z{}", bs58::encode(&multicodec_key).into_string());

    (did, signing_key)
}

#[tokio::test]
async fn test_daemon_authenticates_and_syncs() {
    use std::io::Write;

    let (relay_url, mut keys) = start_relay_with_auth().await;
    let space_id = "test-auth-sync";
    let doc_uuid = "test-uuid-auth-sync";

    let (_dir_a, path_a) = canonical_tempdir();
    let (_dir_b, path_b) = canonical_tempdir();

    setup_space(&path_a, space_id, &relay_url);
    setup_space(&path_b, space_id, &relay_url);

    // Generate identities for each daemon.
    let (did_a, key_a) = test_identity();
    let (did_b, key_b) = test_identity();

    // Authorize both DIDs on the relay (authorized_keys is re-read on every check).
    writeln!(keys, "{did_a}").unwrap();
    writeln!(keys, "{did_b}").unwrap();
    keys.flush().unwrap();

    // Create tracked doc in A, then copy to B (same CRDT history).
    let rel_path = Path::new("auth-test.txt");
    init_tracked_doc(&path_a, rel_path, &did_a, "hello auth");

    // Pre-seed UUID in A, then copy (state.json is copied by copy_tracked_doc).
    seed_document_uuid(&path_a, "auth-test.txt", doc_uuid);
    copy_tracked_doc(&path_a, &path_b, rel_path);

    // Start daemon A with auth, modify file, verify it reaches the relay.
    let handle_a = spawn_ready_daemon(SpaceWorkerConfig {
        signing_key: Some(key_a),
        ..test_daemon_config(path_a.clone(), &did_a, &relay_url, space_id)
    })
    .await;

    std::fs::write(path_a.join(rel_path), "authenticated edit").unwrap();

    // Wait for A's daemon to detect and process the file change by watching
    // the .dt sidecar's modification time. This avoids starting B while A's
    // sync exchange is still in flight (which races with our file write).
    let dt_path = path_a.join(".kutl/docs/auth-test.txt.dt");
    let dt_before = std::fs::metadata(&dt_path).unwrap().modified().unwrap();
    tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if let Ok(meta) = std::fs::metadata(&dt_path)
                && let Ok(modified) = meta.modified()
                && modified > dt_before
            {
                return;
            }
        }
    })
    .await
    .expect("daemon A should process the file change");

    // Now start daemon B. It receives the modified content via catch-up.
    let handle_b = spawn_ready_daemon(SpaceWorkerConfig {
        signing_key: Some(key_b),
        ..test_daemon_config(path_b.clone(), &did_b, &relay_url, space_id)
    })
    .await;

    // Poll daemon B's file until content matches.
    let b_path = path_b.join(rel_path);
    let result = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if let Ok(content) = std::fs::read_to_string(&b_path)
                && content.contains("authenticated")
            {
                return content;
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for authenticated sync from A to B"
    );

    handle_a.abort();
    handle_b.abort();
}

/// Returns `true` if `dir` lives on a case-sensitive filesystem.
///
/// Probes by writing a sentinel file and checking whether its uppercase
/// variant is a distinct path. Used to skip collision tests on macOS
/// case-insensitive APFS/HFS+.
fn is_case_sensitive_fs(dir: &Path) -> bool {
    let probe = dir.join("__kutl_cs_probe__.md");
    let probe_upper = dir.join("__KUTL_CS_PROBE__.md");
    std::fs::write(&probe, "probe").unwrap();
    let sensitive = !probe_upper.exists();
    std::fs::remove_file(&probe).unwrap();
    sensitive
}

/// When the space root contains a case-variant pair, `SpaceWorker::run`
/// must fail startup loudly rather than retry silently.
#[tokio::test]
async fn case_collision_aborts_initial_scan() {
    use kutl_daemon::case_collision::CaseCollisionError;

    let (_keep_dir, space_root) = canonical_tempdir();

    if !is_case_sensitive_fs(&space_root) {
        eprintln!("skipping case_collision_aborts_initial_scan: filesystem is case-insensitive");
        return;
    }

    std::fs::write(space_root.join("foo.md"), "a").unwrap();
    std::fs::write(space_root.join("Foo.md"), "b").unwrap();

    // Relay URL is irrelevant — startup must fail before any network call.
    let cfg = test_daemon_config(
        space_root,
        "did:test:bob",
        "ws://127.0.0.1:1", // unreachable on purpose
        "space-bogus",
    );

    let err = SpaceWorker::new(cfg)
        .expect("construct worker")
        .run()
        .await
        .expect_err("expected case-collision error");

    // The error chain must surface a `CaseCollisionError`.
    let has_collision = err
        .chain()
        .any(|e| e.downcast_ref::<CaseCollisionError>().is_some());
    assert!(
        has_collision,
        "expected CaseCollisionError in chain, got: {err:#}"
    );
}

#[tokio::test]
async fn test_daemon_without_key_rejected_by_auth_relay() {
    let (relay_url, _keys) = start_relay_with_auth().await;

    let (cmd_tx, cmd_rx) = mpsc::channel::<SyncCommand>(TEST_CHANNEL_CAPACITY);
    let (event_tx, mut event_rx) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    let url = relay_url.clone();
    let _handle = tokio::spawn(async move {
        kutl_daemon::client::run_client(&url, "proj", "did:no-auth", "", "", cmd_rx, event_tx)
            .await
            .ok();
    });

    // Without a valid token, the relay should reject the handshake.
    // The client will either get an error event or disconnect.
    let result = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            match event_rx.recv().await {
                Some(
                    SyncEvent::AuthRejected(_) | SyncEvent::Error(_) | SyncEvent::Disconnected,
                )
                | None => return true,
                Some(SyncEvent::Connected) => return false,
                _ => {}
            }
        }
    })
    .await;

    assert!(result.is_ok(), "timed out waiting for rejection");
    assert!(result.unwrap(), "should not have connected without auth");

    drop(cmd_tx);
}

/// When a rename event targets a case-variant of an already-tracked document,
/// the daemon must reject the rename, leave the original mappings untouched,
/// and not propagate anything to the relay.
#[tokio::test]
async fn case_collision_rejects_rename_to_variant() {
    let (_dir, dir_path) = canonical_tempdir();

    if !is_case_sensitive_fs(&dir_path) {
        eprintln!(
            "skipping case_collision_rejects_rename_to_variant: filesystem is case-insensitive"
        );
        return;
    }

    let relay_url = start_relay().await;
    let space_id = "test-collision-rename";
    setup_space(&dir_path, space_id, &relay_url);

    // Seed two tracked docs with distinct UUIDs.
    let a_rel = Path::new("a.md");
    let b_rel = Path::new("b.md");
    init_tracked_doc(&dir_path, a_rel, "did:test", "content a");
    init_tracked_doc(&dir_path, b_rel, "did:test", "content b");
    seed_document_uuid(&dir_path, "a.md", "uuid-a");
    seed_document_uuid(&dir_path, "b.md", "uuid-b");

    // Start daemon (picks up existing docs via initial scan).
    let daemon_handle = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:test",
        &relay_url,
        space_id,
    ))
    .await;

    // Rename b.md -> A.md on disk. This would collide with a.md.
    std::fs::rename(dir_path.join("b.md"), dir_path.join("A.md")).unwrap();

    // Write a sentinel file AFTER the collision-triggering event. The daemon
    // processes file events serially, so once the sentinel's sidecar appears
    // we know the rename event was already observed (and rejected).
    std::fs::write(dir_path.join("_sentinel.md"), "fence").unwrap();
    let sentinel_dt = dir_path.join(".kutl").join("docs").join("_sentinel.md.dt");
    tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if sentinel_dt.exists() {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("daemon should process the sentinel file");

    // Assertions: the rename was rejected, meaning:
    //  - No .kutl/docs/A.md.dt sidecar exists (rename did NOT propagate).
    //  - a.md.dt sidecar still exists (original untouched).
    //  - b.md.dt sidecar still exists (rename rejected — the watcher does not
    //    remove the stale mapping, and DocumentManager's in-memory state
    //    still has b.md registered. The filesystem move happened but the
    //    daemon refused to act on it.)
    let docs_dir = dir_path.join(".kutl").join("docs");
    assert!(
        !docs_dir.join("A.md.dt").exists(),
        "daemon should not have propagated rename; A.md.dt must not exist at {}",
        docs_dir.join("A.md.dt").display()
    );
    assert!(
        docs_dir.join("a.md.dt").exists(),
        "original a.md.dt sidecar should still exist"
    );
    assert!(
        docs_dir.join("b.md.dt").exists(),
        "b.md.dt sidecar should still exist (rename rejected leaves old mapping)"
    );

    daemon_handle.abort();
}

/// When a new file arrives whose path is a case-variant of an already-tracked
/// document, the daemon must reject the event and leave the existing doc in place.
#[tokio::test]
async fn case_collision_rejects_new_variant_file() {
    let (_dir, dir_path) = canonical_tempdir();

    if !is_case_sensitive_fs(&dir_path) {
        eprintln!(
            "skipping case_collision_rejects_new_variant_file: filesystem is case-insensitive"
        );
        return;
    }

    let relay_url = start_relay().await;
    let space_id = "test-collision-new-file";

    setup_space(&dir_path, space_id, &relay_url);

    // Seed tracked "foo.md" with initial content and a known UUID.
    let rel_path = Path::new("foo.md");
    init_tracked_doc(&dir_path, rel_path, "did:test", "original");
    seed_document_uuid(&dir_path, "foo.md", "uuid-foo");

    // Start daemon.
    let daemon_handle = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:test",
        &relay_url,
        space_id,
    ))
    .await;

    // Write a case-variant sibling on disk. The daemon must NOT register it.
    std::fs::write(dir_path.join("Foo.md"), "variant content").unwrap();

    // Write a sentinel file after the collision trigger. The daemon processes
    // events serially, so once the sentinel's sidecar appears we know Foo.md's
    // event was already observed (and rejected).
    std::fs::write(dir_path.join("_sentinel.md"), "fence").unwrap();
    let sentinel_dt = dir_path.join(".kutl").join("docs").join("_sentinel.md.dt");
    tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if sentinel_dt.exists() {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("daemon should process the sentinel file");

    // Primary assertion: the daemon did NOT register Foo.md. register_and_subscribe
    // is what creates the .dt sidecar, so its absence proves rejection.
    let dt_path = dir_path.join(".kutl").join("docs").join("Foo.md.dt");
    assert!(
        !dt_path.exists(),
        "daemon should not have created a .dt sidecar for case-variant Foo.md, but {} exists",
        dt_path.display()
    );

    // And foo.md's sidecar should still be in place.
    let original_dt = dir_path.join(".kutl").join("docs").join("foo.md.dt");
    assert!(
        original_dt.exists(),
        "original foo.md.dt sidecar should still exist at {}",
        original_dt.display()
    );

    daemon_handle.abort();
}
