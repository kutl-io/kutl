use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use kutl_core::{Boundary, Document};
use kutl_daemon::bridge;
use kutl_daemon::client::{SyncCommand, SyncEvent};
use kutl_daemon::daemon::{SpaceWorker, SpaceWorkerConfig};

use kutl_relay::config::RelayConfig;
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc};

/// Install a tracing subscriber for the whole test binary, once, IFF the
/// operator asked for logs.
///
/// This suite runs every daemon and relay IN PROCESS; without a subscriber,
/// `KUTL_LOG=debug` produces
/// nothing and every in-suite failure reads as a bare "timed out", with no way
/// to tell a watcher that never fired from a broadcast that never landed.
/// Gated on `KUTL_LOG`/`RUST_LOG` being set so a
/// normal run stays byte-identical to an unlogged one.
///
/// Writes to STDERR, deliberately: libtest's output capture is per-test-thread,
/// and most of what matters here is logged from spawned tokio tasks (daemon
/// workers, relay actors) that capture would misattribute or drop. Run with
/// `--nocapture 2>file` and every line lands in one stream, interleaved but
/// disambiguated by the `space_id`/port fields the daemon and relay already
/// put on their events.
fn init_test_tracing() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = std::env::var("KUTL_LOG").or_else(|_| std::env::var("RUST_LOG"));
        let Ok(filter) = filter else { return };
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .with_thread_ids(true)
            .try_init();
    });
}

/// Boot a storeless relay app (no data dir, no space/registry/change backends)
/// via the host seam — the kutlhub-shaped storeless path. The OSS
/// binary's `build_app` requires a data dir; a non-durable daemon test that
/// wants in-memory registries constructs the storeless shape explicitly here.
/// (Durable tests keep their `data_dir` + `build_app`.)
fn build_storeless_relay_app(
    config: RelayConfig,
) -> (
    axum::Router,
    tokio::task::JoinHandle<()>,
    Option<tokio::task::JoinHandle<()>>,
) {
    kutl_relay::testing::build_storeless_app(config, kutl_relay::testing::TestBackends::default())
}

/// Channel capacity used throughout tests.
const TEST_CHANNEL_CAPACITY: usize = 16;

/// A fresh, empty blob-upload backlog counter for `run_client` calls that don't
/// exercise the `blob_upload_backlog` metric.
fn zero_backlog() -> Arc<AtomicI64> {
    Arc::new(AtomicI64::new(0))
}

/// The relay authenticates + authorizes unconditionally, so
/// every test connection must present a bearer for an authorized DID. Rather
/// than mint a unique identity per test, the shared-relay helpers authorize ONE
/// fixed identity and every default client/daemon uses it.
///
/// A fixed Ed25519 seed makes the DID deterministic so the process-wide
/// `authorized_keys` file (see [`shared_authorized_keys_path`]) can list it once.
fn shared_test_signing_key() -> SigningKey {
    /// Fixed, non-secret seed for the shared test identity — deterministic so
    /// the `authorized_keys` entry is stable across the whole test binary.
    const SHARED_TEST_SEED: [u8; 32] = [42u8; 32];
    SigningKey::from_bytes(&SHARED_TEST_SEED)
}

/// `did:key` of the shared test identity ([`shared_test_signing_key`]).
fn shared_test_did() -> String {
    // Canonical did:key encoding — same encoder the relay's parser round-trips,
    // so the enrolled DID matches the authenticated one.
    kutl_signals::did_key_encode(&shared_test_signing_key().verifying_key())
}

/// Path to a process-wide `authorized_keys` file that lists the shared test
/// DID. Created once and kept alive for the whole test binary (a leaked
/// `NamedTempFile`), so `start_relay*` can keep returning a bare URL `String`
/// while still satisfying the relay's mandatory-auth startup check.
fn shared_authorized_keys_path() -> &'static Path {
    use std::io::Write;
    use std::sync::OnceLock;
    static KEYS: OnceLock<tempfile::NamedTempFile> = OnceLock::new();
    KEYS.get_or_init(|| {
        let mut keys = tempfile::NamedTempFile::new().expect("create shared authorized_keys");
        writeln!(keys, "# shared test identity (mandatory auth)")
            .and_then(|()| writeln!(keys, "{}", shared_test_did()))
            .expect("write shared authorized_keys");
        keys.flush().expect("flush shared authorized_keys");
        keys
    })
    .path()
}

/// Mint a bearer token for the shared test identity against `relay_url` via the
/// relay's did:key challenge flow.
async fn shared_test_token(relay_url: &str) -> String {
    kutl_client::authenticate(relay_url, &shared_test_did(), &shared_test_signing_key())
        .await
        .expect("mint shared test bearer token")
}

/// Spawn a WS sync client task with an explicit bearer token. The `did` label
/// is the wire `client_name`; `token` is presented in the handshake. Use `""`
/// to model an unauthenticated client (expected to be rejected).
fn spawn_client_with_token(
    relay_url: &str,
    space_id: &str,
    did: &str,
    token: &str,
    cmd_rx: mpsc::UnboundedReceiver<SyncCommand>,
    evt_tx: mpsc::Sender<SyncEvent>,
) -> tokio::task::JoinHandle<()> {
    let url = relay_url.to_owned();
    let space_id = space_id.to_owned();
    let did = did.to_owned();
    let token = token.to_owned();
    tokio::spawn(async move {
        kutl_daemon::client::run_client(
            &url,
            &space_id,
            &did,
            &token,
            "",
            cmd_rx,
            evt_tx,
            zero_backlog(),
        )
        .await
        .ok();
    })
}

/// Spawn a WS sync client authenticated as the shared test identity.
/// Centralizes the connect-client boilerplate the tests repeat: mints a bearer
/// for the shared authorized identity so the relay admits the connection.
async fn spawn_client(
    relay_url: &str,
    space_id: &str,
    did: &str,
    cmd_rx: mpsc::UnboundedReceiver<SyncCommand>,
    evt_tx: mpsc::Sender<SyncEvent>,
) -> tokio::task::JoinHandle<()> {
    let token = shared_test_token(relay_url).await;
    spawn_client_with_token(relay_url, space_id, did, &token, cmd_rx, evt_tx)
}

/// Poll interval for loops that check file content or metadata changes.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Timeout for waiting on a single async event (channel recv, WS connect).
const EVENT_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for waiting on multi-step async operations (file sync pipelines).
const PIPELINE_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for a daemon to reach READY (connect, authenticate, subscribe,
/// initial scan, watcher start).
///
/// Deliberately far more generous than [`PIPELINE_TIMEOUT`]: readiness is a
/// setup precondition, not behavior under test, and it competes for CPU with
/// every other test binary when the suite runs under `cargo test --workspace`
/// (which parallelizes across crates). Bounding setup as tightly as the
/// assertion it precedes turns machine load into a red suite. The assertions
/// that follow readiness keep their own, strict timeouts.
const DAEMON_READY_TIMEOUT: Duration = Duration::from_mins(2);

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
    init_test_tracing();
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
/// uses sensible defaults (`one_shot: false`, etc.).
///
/// Auth defaults to the shared test identity ([`shared_test_did`] /
/// [`shared_test_signing_key`]), which the shared-relay helpers
/// authorize. The positional `author_did` is IGNORED for the wire
/// identity — it is retained only so existing call sites read the same; a test
/// that needs a distinct on-wire identity overrides BOTH `author_did` and
/// `signing_key` via struct-update (see `test_daemon_authenticates_and_syncs`).
fn test_daemon_config(
    space_root: PathBuf,
    _author_did: &str,
    relay_url: &str,
    space_id: &str,
) -> SpaceWorkerConfig {
    let space_root_for_pin = space_root.clone();
    SpaceWorkerConfig {
        // Poll backend: this suite's burst load starves FSEvents stream
        // REGISTRATION system-wide (a first .watch() measured at 98-174s
        // against a ~1ms baseline), which surfaces as "timed out waiting for
        // the watcher" in-suite failures. The poll backend never talks to
        // fseventsd, so the suite cannot starve itself.
        poll_watcher: true,
        space_root,
        author_did: shared_test_did(),
        relay_url: relay_url.into(),
        space_id: space_id.into(),
        signing_key: Some(shared_test_signing_key()),
        one_shot: false,
        display_name: String::new(),
        ready: None,
        cancel: tokio_util::sync::CancellationToken::new(),
        // Inside the space's own `.kutl/`, not the developer's real `~/.kutl`.
        // Two reasons, and both bite: a test run would
        // otherwise append one entry per ephemeral `127.0.0.1:<random port>`
        // relay to a live file, and every worker in this binary would take the
        // same exclusive lock — turning a per-install record into a
        // process-wide bottleneck that exists only under test.
        known_relays_path: Some(space_root_known_relays(&space_root_for_pin)),
    }
}

/// Per-space path for the relay-identity record used by tests.
fn space_root_known_relays(space_root: &Path) -> PathBuf {
    space_root.join(".kutl").join("known_relays.json")
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
    start_relay_with_capacity(RelayConfig::default().outbound_capacity).await
}

/// Like [`start_relay`], but with an explicit per-connection outbound `data`
/// channel capacity. A small capacity lets a test deterministically overflow a
/// slow subscriber's outbound lane and trigger the relay's stale-subscriber
/// eviction (backpressure recovery).
async fn start_relay_with_capacity(outbound_capacity: usize) -> String {
    init_test_tracing();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        outbound_capacity,
        // Mandatory auth: authorize the shared test identity so the
        // default clients/daemons this relay serves can connect.
        authorized_keys_file: Some(shared_authorized_keys_path().to_path_buf()),
        ..Default::default()
    };

    // Storeless boot: in-memory registries; build_app requires a
    // data dir, so construct the storeless shape via the host seam.
    let (app, _relay_handle, _flush_handle) = build_storeless_relay_app(config);
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
fn copy_tracked_doc(src_root: &Path, dst_root: &Path, rel_path: &Path, document_id: &str) {
    // Copy the file itself.
    let dst_file = dst_root.join(rel_path);
    if let Some(parent) = dst_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::copy(src_root.join(rel_path), &dst_file).unwrap();

    // Copy the .dt sidecar (keyed by document id).
    let dt_name = format!("{document_id}.dt");
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

/// The `.dt` sidecar path for a document id under a space root:
/// `<dir>/.kutl/docs/<document-id>.dt` (the convention `SpaceState::dt_path`
/// encodes; tests write/read sidecars directly).
fn dt_sidecar_path(dir: &Path, document_id: &str) -> PathBuf {
    dir.join(".kutl")
        .join("docs")
        .join(format!("{document_id}.dt"))
}

/// Initialize a tracked document with given content. The CRDT sidecar is keyed
/// by `document_id` (matching what `seed_document_uuid` records in state.json),
/// so the daemon loads it on startup.
fn init_tracked_doc(dir: &Path, rel_path: &Path, document_id: &str, author: &str, content: &str) {
    std::fs::write(dir.join(rel_path), content).unwrap();
    let dt_path = dt_sidecar_path(dir, document_id);
    std::fs::create_dir_all(dt_path.parent().unwrap()).unwrap();
    let mut doc = Document::new();
    // Use a short agent name for the CRDT; author is only for metadata.
    let agent = doc.register_agent("test-init").unwrap();
    doc.edit(agent, author, "init", Boundary::Auto, |ctx| {
        ctx.insert(0, content)
    })
    .unwrap();
    doc.save(&dt_path).unwrap();
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
) -> (
    mpsc::UnboundedSender<SyncCommand>,
    mpsc::Receiver<SyncEvent>,
) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (evt_tx, mut evt_rx) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    spawn_client(relay_url, space_id, client_name, cmd_rx, evt_tx).await;

    let event = tokio::time::timeout(EVENT_TIMEOUT, evt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, SyncEvent::Connected { .. }));

    cmd_tx
        .send(SyncCommand::Subscribe {
            document_id: document_id.to_owned(),
        })
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

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (event_tx, mut event_rx) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    let client_handle = spawn_client(&relay_url, "test-space", "did:test", cmd_rx, event_tx).await;

    // Wait for Connected event.
    let event = tokio::time::timeout(EVENT_TIMEOUT, event_rx.recv())
        .await
        .expect("timeout waiting for Connected")
        .expect("channel closed");
    assert!(matches!(event, SyncEvent::Connected { .. }));

    // Subscribe to a document.
    cmd_tx
        .send(SyncCommand::Subscribe {
            document_id: "test.txt".into(),
        })
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
    let (alice_cmd, alice_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (alice_evt_tx, mut alice_events) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    let _handle_alice = spawn_client(
        &relay_url,
        "e73c023a-2e8e-4034-8bb4-d853730e1bfc",
        "did:alice",
        alice_cmd_rx,
        alice_evt_tx,
    )
    .await;

    // Wait for Alice connected
    let event = tokio::time::timeout(EVENT_TIMEOUT, alice_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, SyncEvent::Connected { .. }));

    alice_cmd
        .send(SyncCommand::Subscribe {
            document_id: "doc".into(),
        })
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
    let (bob_cmd, bob_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (bob_evt_tx, mut bob_events) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    let _handle_bob = spawn_client(
        &relay_url,
        "e73c023a-2e8e-4034-8bb4-d853730e1bfc",
        "did:bob",
        bob_cmd_rx,
        bob_evt_tx,
    )
    .await;

    let event = tokio::time::timeout(EVENT_TIMEOUT, bob_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, SyncEvent::Connected { .. }));

    bob_cmd
        .send(SyncCommand::Subscribe {
            document_id: "doc".into(),
        })
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
// Outbound flood must not starve inbound ack reads
// ---------------------------------------------------------------------------

/// Regression guard for a single-`select!` client-loop deadlock: a client whose
/// outbound write half is saturated by a flood of `Subscribe` commands must
/// still receive an inbound `RemoteOps` from a peer within `EVENT_TIMEOUT`.
/// A loop that services reads and writes from one `select!` starves its read
/// arm whenever a socket write is in flight; the client runs send and recv on
/// independent tasks.
///
/// NOTE: with an in-process relay whose inbound channel capacity is 256 the
/// flood may drain faster than it saturates the socket, so this test cannot
/// reliably reproduce the starvation on its own; the client's dedicated unit
/// test wedges the sink deterministically for that.
#[tokio::test]
async fn test_outbound_flood_does_not_starve_inbound_acks() {
    /// Outbound burst multiple of the channel capacity used to saturate the
    /// flooder's write half so a single-`select!` client would starve its reads.
    const FLOOD_FACTOR: usize = 8;

    let relay_url = start_relay().await;

    // Flooder: subscribes, then fires a burst of Subscribe commands far past
    // the channel capacity to keep its write half saturated.
    let (flood_cmd, flood_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (flood_evt_tx, mut flood_events) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);
    let _flooder = spawn_client(
        &relay_url,
        "d8864644-c15d-43be-8c0a-ef2d08c8fc10",
        "did:flooder",
        flood_cmd_rx,
        flood_evt_tx,
    )
    .await;

    let ev = tokio::time::timeout(EVENT_TIMEOUT, flood_events.recv())
        .await
        .expect("flooder connect timeout")
        .expect("flooder channel closed");
    assert!(matches!(ev, SyncEvent::Connected { .. }));

    flood_cmd
        .send(SyncCommand::Subscribe {
            document_id: "shared".into(),
        })
        .unwrap();
    // Drain the flooder's empty catch-up for "shared".
    let ev = tokio::time::timeout(EVENT_TIMEOUT, flood_events.recv())
        .await
        .expect("flooder catch-up timeout")
        .expect("flooder channel closed");
    assert!(matches!(&ev, SyncEvent::RemoteOps { ops, .. } if ops.is_empty()));

    // Saturate the flooder's outbound write half: fire 8x the channel
    // capacity of commands. With a single-select! client these block the
    // read arm whenever a socket write is in flight.
    let flood_task = tokio::spawn(async move {
        for i in 0..(TEST_CHANNEL_CAPACITY * FLOOD_FACTOR) {
            if flood_cmd
                .send(SyncCommand::Subscribe {
                    document_id: format!("noise-{i}"),
                })
                .is_err()
            {
                break;
            }
        }
        flood_cmd // keep the sender alive so the client stays up
    });

    // Writer peer: produce a remote op on "shared" that the flooder must
    // receive even while its own write half is saturated.
    let (writer_cmd, writer_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (writer_evt_tx, mut writer_events) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);
    let _writer = spawn_client(
        &relay_url,
        "d8864644-c15d-43be-8c0a-ef2d08c8fc10",
        "did:writer",
        writer_cmd_rx,
        writer_evt_tx,
    )
    .await;
    let ev = tokio::time::timeout(EVENT_TIMEOUT, writer_events.recv())
        .await
        .expect("writer connect timeout")
        .expect("writer channel closed");
    assert!(matches!(ev, SyncEvent::Connected { .. }));
    writer_cmd
        .send(SyncCommand::Subscribe {
            document_id: "shared".into(),
        })
        .unwrap();
    // Drain writer catch-up.
    let _ = tokio::time::timeout(EVENT_TIMEOUT, writer_events.recv()).await;

    // Build a real op via the Document API so the relay broadcasts RemoteOps.
    // Uses doc.encode_since / doc.changes_since — the same pattern as
    // test_client_send_and_receive_ops (lines 415-416).
    let mut doc = Document::new();
    let agent = doc.register_agent("writer").unwrap();
    doc.edit(agent, "did:writer", "edit", Boundary::Auto, |ctx| {
        ctx.insert(0, "hello from writer")
    })
    .unwrap();
    let ops = doc.encode_since(&[]);
    let metadata = doc.changes_since(&[]);
    writer_cmd
        .send(SyncCommand::SendOps {
            document_id: "shared".into(),
            ops,
            metadata,
            content_mode: 0,
            content_hash: vec![],
        })
        .unwrap();

    // CORE ASSERTION: the flooder, with a saturated write half, still reads
    // the inbound remote op within the timeout. Pre-split this starves.
    let got = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            match flood_events.recv().await {
                Some(SyncEvent::RemoteOps {
                    document_id, ops, ..
                }) if document_id == "shared" && !ops.is_empty() => {
                    return true;
                }
                Some(_) => {}
                None => return false,
            }
        }
    })
    .await
    .expect("flooder did not receive inbound remote op while flooding (read starvation)");
    assert!(
        got,
        "flooder inbound read starved by its own outbound flood"
    );

    flood_task.abort();
}

// ---------------------------------------------------------------------------
// Backpressure eviction must be recoverable
// ---------------------------------------------------------------------------

/// A subscriber whose bounded outbound `data` lane overflows is EVICTED by the
/// relay (`relay_and_evict`), which sends a `StaleSubscriber` notice and removes
/// the subscription. The daemon's WS client must surface that notice as
/// `SyncEvent::StaleSubscriber` so the daemon loop can re-subscribe and recover
/// the broadcasts it missed while behind. A `_ => {}` catch-all in
/// `handle_inbound` would silently drop the notice, stranding the document:
/// the victim never observes a `StaleSubscriber` event and the recv times out.
/// Surfaced, the victim re-subscribes and the relay's catch-up replays the FULL
/// current document state — proving every op missed during the stall is
/// recovered.
///
/// This exercises the real production path end-to-end: the relay's eviction,
/// `client.rs::handle_inbound`'s `StaleSubscriber` arm, and the recovery via
/// a plain `SyncCommand::Subscribe` (the exact command the daemon loop emits in
/// `handle_sync_event`).
#[tokio::test]
async fn test_stale_subscriber_eviction_is_recoverable() {
    /// Number of distinct ops the writer fires at the doc while the victim is
    /// frozen. Each is a separate broadcast; with `outbound_capacity == 1` and a
    /// non-draining victim, the lane overflows well before this many land.
    const FLOOD_OPS: usize = 64;

    // outbound_capacity 1: a single un-acked broadcast fills the victim's lane,
    // so the next broadcast evicts it.
    let relay_url = start_relay_with_capacity(1).await;
    let space_id = "6a06ac70-24ef-4018-814c-0d0a33d2610c";
    let doc_id = "doc";

    // Victim: subscribe, then deliberately stop draining its event channel to
    // model a slow daemon loop. Its small (capacity 1) event channel means the
    // client read task blocks on `event_tx.send().await` once we stop draining,
    // which in turn stops it reading the socket and backs up the relay's
    // outbound `data` lane → overflow → eviction.
    let (victim_cmd, victim_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (victim_evt_tx, mut victim_evt_rx) = mpsc::channel::<SyncEvent>(1);
    let _victim = spawn_client(
        &relay_url,
        space_id,
        "did:victim",
        victim_cmd_rx,
        victim_evt_tx,
    )
    .await;

    let ev = tokio::time::timeout(EVENT_TIMEOUT, victim_evt_rx.recv())
        .await
        .expect("victim connect timeout")
        .expect("victim channel closed");
    assert!(matches!(ev, SyncEvent::Connected { .. }));

    victim_cmd
        .send(SyncCommand::Subscribe {
            document_id: doc_id.into(),
        })
        .unwrap();
    // Drain the victim's empty catch-up so its event channel starts empty.
    let ev = tokio::time::timeout(EVENT_TIMEOUT, victim_evt_rx.recv())
        .await
        .expect("victim catch-up timeout")
        .expect("victim channel closed");
    assert!(matches!(&ev, SyncEvent::RemoteOps { ops, .. } if ops.is_empty()));

    // Writer peer: build the authoritative final document, then replay its ops
    // one change at a time so the relay emits many separate broadcasts at the
    // stalled victim.
    let (writer_cmd, mut writer_evt_rx) =
        connect_and_subscribe(&relay_url, space_id, "did:writer", doc_id).await;
    // Drain the writer's own empty catch-up.
    let _ = tokio::time::timeout(EVENT_TIMEOUT, writer_evt_rx.recv()).await;

    let mut doc = Document::new();
    let agent = doc.register_agent("writer").unwrap();
    for i in 0..FLOOD_OPS {
        let version_before = doc.local_version();
        doc.edit(agent, "did:writer", "edit", Boundary::Explicit, |ctx| {
            ctx.insert(0, &format!("{i},"))
        })
        .unwrap();
        let ops = doc.encode_since(&version_before);
        let metadata = doc.changes_since(&version_before);
        writer_cmd
            .send(SyncCommand::SendOps {
                document_id: doc_id.into(),
                ops,
                metadata,
                content_mode: 0,
                content_hash: vec![],
            })
            .unwrap();
    }
    let final_content = doc.content();

    // The victim resumes draining. It must observe the eviction notice as a
    // `SyncEvent::StaleSubscriber` for this document; a dropped notice makes
    // the recv loop time out.
    let stale = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            match victim_evt_rx.recv().await {
                Some(SyncEvent::StaleSubscriber { document_id }) if document_id == doc_id => {
                    return true;
                }
                // Ignore any catch-up broadcasts that squeezed through before the
                // eviction; we only care that the eviction surfaces.
                Some(_) => {}
                None => return false,
            }
        }
    })
    .await
    .expect("victim never observed a StaleSubscriber eviction (notice dropped)");
    assert!(stale, "victim channel closed before the eviction notice");

    // Recovery: re-subscribe (the exact command the daemon loop emits). The
    // relay's `handle_subscribe` re-adds the victim and replays the full current
    // doc state via catch-up.
    let resubscribe = || {
        victim_cmd
            .send(SyncCommand::Subscribe {
                document_id: doc_id.into(),
            })
            .unwrap();
    };
    resubscribe();

    // Recovery must mirror the daemon loop's `handle_stale_subscriber`: the
    // capacity-1 relay can re-evict the victim WHILE the flood is still draining
    // (the catch-up sits in the bounded `data` lane and the next broadcast
    // overflows it), so a single re-subscribe is not enough — re-subscribe on
    // EVERY eviction until the catch-up lands without being re-evicted. Two
    // distinct failure modes both stem from not doing this:
    //   - a leftover flood broadcast (a delta whose base an empty document lacks)
    //     reaching a fresh per-frame document panicked as `BaseVersionUnknown`;
    //   - a re-eviction dropping the catch-up left the loop waiting → timeout.
    // Accumulate into ONE document and DROP undecodable frames (as production's
    // `merge_remote_ops` does); re-subscribe on each `StaleSubscriber`.
    let mut recv_doc = Document::new();
    let recovered = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            match victim_evt_rx.recv().await {
                Some(SyncEvent::StaleSubscriber { document_id }) if document_id == doc_id => {
                    // Re-evicted mid-recovery — re-subscribe, exactly as
                    // `handle_stale_subscriber` does (a bare re-subscribe is
                    // self-pacing, so no backoff is needed).
                    resubscribe();
                }
                Some(SyncEvent::RemoteOps { ops, .. }) if !ops.is_empty() => {
                    // intentional: a frame whose base version recv_doc doesn't
                    // hold yet is dropped — a later catch-up (a full
                    // `encode_full`) reconstructs the state regardless.
                    let _ = recv_doc.merge(&ops, &[]);
                    if recv_doc.content() == final_content {
                        return true;
                    }
                }
                Some(_) => {}
                None => return false,
            }
        }
    })
    .await
    .expect("victim did not recover the document after re-subscribing");
    assert!(
        recovered,
        "re-subscribe catch-up did not reconstruct the final document state"
    );
}

// ---------------------------------------------------------------------------
// Half-open socket teardown test
// ---------------------------------------------------------------------------

/// Like `start_relay`, but inserts a transparent TCP proxy between the test
/// client and the real relay. Returns `(proxy_ws_url, real_ws_url, proxy_task)`.
/// Aborting the task drops both sides of the proxy TCP stream, which causes the
/// client's `ws_stream.next()` to return an error and drives `read_task` →
/// session cancel → `write_task` exit → `run_client` returns. This is the real
/// half-open-socket scenario: the relay vanishes without sending a close frame.
///
/// The proxy accepts exactly ONE connection (the WS under test), so the bearer
/// must be minted directly against `real_ws_url`, not through the
/// proxy.
async fn start_relay_abortable() -> (String, String, tokio::task::JoinHandle<()>) {
    // Start the real relay.
    let relay_url = start_relay().await;
    // Strip "ws://" to get "host:port/ws", then parse just the host:port.
    let relay_addr = relay_url
        .strip_prefix("ws://")
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_owned();

    // Bind the proxy listener on a random port.
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap().to_string();

    let proxy_task = tokio::spawn(async move {
        // Accept exactly one connection (the test client).
        let (client_stream, _) = proxy_listener.accept().await.unwrap();
        // Open a connection to the real relay.
        let relay_stream = tokio::net::TcpStream::connect(&relay_addr).await.unwrap();
        // Bidirectionally copy until one side closes or the task is aborted.
        // When the task is aborted both streams are dropped, closing the TCP
        // connection on the client side → `ws_stream.next()` returns an error.
        let (mut c_read, mut c_write) = client_stream.into_split();
        let (mut r_read, mut r_write) = relay_stream.into_split();
        tokio::select! {
            _ = tokio::io::copy(&mut c_read, &mut r_write) => {}
            _ = tokio::io::copy(&mut r_read, &mut c_write) => {}
        }
    });

    (format!("ws://{proxy_addr}/ws"), relay_url, proxy_task)
}

/// Proves the no-leak guarantee end-to-end: when the relay vanishes mid-session
/// (socket closes / goes half-open), `run_client` returns and emits
/// `SyncEvent::Disconnected` — neither the read task nor the write task is
/// leaked.
///
/// A regression guard on the split `relay_loop`'s teardown wiring: if the
/// cancellation coupling between
/// the two halves is removed this test will time out on the `client_handle` join.
#[tokio::test]
async fn test_half_open_socket_tears_down_both_halves() {
    let (proxy_url, real_url, proxy_task) = start_relay_abortable().await;

    // Mint the bearer against the REAL relay — the proxy accepts only the single
    // WS connection under test.
    let token = shared_test_token(&real_url).await;

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (event_tx, mut event_rx) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    // Run the real client (through the proxy) and keep its JoinHandle to prove
    // it finishes.
    let client_handle = tokio::spawn(async move {
        kutl_daemon::client::run_client(
            &proxy_url,
            "ho-space",
            "did:ho",
            &token,
            "",
            cmd_rx,
            event_tx,
            zero_backlog(),
        )
        .await
        .ok();
    });

    // Wait for Connected.
    let ev = tokio::time::timeout(EVENT_TIMEOUT, event_rx.recv())
        .await
        .expect("connect timeout")
        .expect("event channel closed");
    assert!(matches!(ev, SyncEvent::Connected { .. }));

    // Kill the relay: abort the TCP proxy, which drops both sides of the socket
    // and causes the client's ws_stream.next() to return an error. The read
    // half observes the close, cancels the session, write_task exits, and
    // run_client returns with Disconnected.
    proxy_task.abort();

    let got_disconnect = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            match event_rx.recv().await {
                // Disconnected event or channel close both confirm teardown.
                Some(SyncEvent::Disconnected) | None => return true,
                Some(_) => {}
            }
        }
    })
    .await
    .expect("client did not tear down after relay died (leaked half?)");
    assert!(got_disconnect);

    // The client task itself must finish (no leaked read/write task keeping
    // run_client alive forever).
    tokio::time::timeout(EVENT_TIMEOUT, client_handle)
        .await
        .expect("run_client did not return after socket death (task leak)")
        .ok();

    // We deliberately hold cmd_tx alive across the assertions so a clean
    // command-channel close can't masquerade as the teardown under test.
    drop(cmd_tx);
}

// ---------------------------------------------------------------------------
// Watcher smoke test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_watcher_detects_file_change() {
    use kutl_daemon::{FileEvent, FileWatcher};

    let (_dir, dir_path) = canonical_tempdir();
    let (tx, mut rx) = mpsc::channel::<FileEvent>(TEST_CHANNEL_CAPACITY);
    let (_suppress_tx, suppress_rx) = mpsc::unbounded_channel::<kutl_daemon::Suppression>();

    let mut watcher = FileWatcher::new(&dir_path, tx, suppress_rx, true).unwrap();
    let watcher_handle = tokio::spawn(async move { watcher.run().await });

    // Give watcher time to register with the OS.
    tokio::time::sleep(WATCHER_INIT_DELAY).await;

    // Create a new file.
    std::fs::write(dir_path.join("test.txt"), "hello").unwrap();

    // FSEvents can deliver a directory-level event for the temp dir (empty
    // rel_path, from its mtime bumping) before — or instead of — the file event
    // when test.txt is created, and the ordering/coalescing is nondeterministic
    // (a smaller binary shifts the race, e.g. line-tables-only vs full debug).
    // Skip such spurious events and keep reading until we observe the test.txt
    // event, bounded by EVENT_TIMEOUT.
    let deadline = std::time::Instant::now() + EVENT_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let result = tokio::time::timeout(remaining, rx.recv()).await;
        assert!(result.is_ok(), "watcher should detect test.txt creation");
        match result.unwrap().unwrap() {
            FileEvent::Modified { rel_path } | FileEvent::Removed { rel_path } => {
                if rel_path.to_string_lossy() == "test.txt" {
                    break;
                }
            }
            FileEvent::Renamed { old_path, new_path } => {
                // Creation may surface as a rename on some platforms.
                if old_path.to_string_lossy().contains("test.txt")
                    || new_path.to_string_lossy().contains("test.txt")
                {
                    break;
                }
            }
        }
        // Any other event (e.g. an empty-rel_path directory event) is spurious;
        // keep waiting for test.txt.
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
    let space_id = "033a037a-4db6-454b-8b7a-38e43defe541";
    let doc_uuid = "test-uuid-local-to-relay";

    setup_space(&dir_path, space_id, &relay_url);

    // Create initial tracked file and its .dt counterpart.
    let rel_path = Path::new("test.txt");
    let abs_path = dir_path.join(rel_path);
    init_tracked_doc(&dir_path, rel_path, doc_uuid, "did:test", "initial content");

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

/// A one-shot `kutl sync` that registers a new
/// file emits a lifecycle stamp, ticking the origin HLC clock to a real wall-clock
/// value. That stamp MUST survive into the persisted `hlc_floor` so the next
/// process (a re-run, or the second `kutl sync` in a create→delete/rename sequence)
/// seeds its clock above it.
///
/// If the floor were only recorded by `save_state`
/// calls that run BEFORE `make_metadata` ticks the clock, with no
/// save-on-session-end, it would persist as `{0,0}` — and every subsequent
/// offline delete/rename would be stamped at zero and lose arbitration at the
/// relay.
#[tokio::test]
async fn test_one_shot_persists_emitted_hlc_floor() {
    let relay_url = start_relay().await;
    let (_dir, dir_path) = canonical_tempdir();
    let space_id = "ee4ce874-4643-4094-8e32-0e7de3ab29aa";
    setup_space(&dir_path, space_id, &relay_url);

    // A brand-new untracked file: the one-shot registers it, ticking the clock to
    // a real wall-clock stamp.
    std::fs::write(dir_path.join("doc.md"), "# hello\n").unwrap();

    // Run one one-shot sync to completion (exits after ONE_SHOT_IDLE_TIMEOUT).
    let config = SpaceWorkerConfig {
        one_shot: true,
        ..test_daemon_config(dir_path.clone(), "did:test", &relay_url, space_id)
    };
    SpaceWorker::new(config)
        .unwrap()
        .run()
        .await
        .expect("one-shot sync");

    let floor = kutl_daemon::state::DaemonState::load(&dir_path.join(".kutl")).hlc_floor;
    assert!(
        floor.is_some_and(|f| f.physical_ms > 0),
        "one-shot must persist its emitted HLC floor, not the pre-tick {{0,0}}; got {floor:?}"
    );
}

/// A one-shot `kutl sync` that registers a
/// new file must mark it confirmed via the typed `RegisterDocumentAck` — the
/// lifecycle broadcast that otherwise drives `confirm_document` excludes the
/// sender, so this ack is the registrant's only confirmation signal.
///
/// Without it the document stays `confirmed = false`, so a later
/// local delete is misclassified by the startup truth table as `SubscribeRemote`
/// (re-download) instead of `SendUnregister` (push the delete) — and deletions
/// never propagate.
#[tokio::test]
async fn test_one_shot_confirms_registered_document() {
    let relay_url = start_relay().await;
    let (_dir, dir_path) = canonical_tempdir();
    let space_id = "8c21dc92-09f9-4c41-8d8d-4655f3776bc6";
    setup_space(&dir_path, space_id, &relay_url);
    std::fs::write(dir_path.join("doc.md"), "# hi\n").unwrap();

    let config = SpaceWorkerConfig {
        one_shot: true,
        ..test_daemon_config(dir_path.clone(), "did:test", &relay_url, space_id)
    };
    SpaceWorker::new(config)
        .unwrap()
        .run()
        .await
        .expect("one-shot sync");

    let state = kutl_daemon::state::DaemonState::load(&dir_path.join(".kutl"));
    assert!(
        state.documents.values().any(|e| e.confirmed),
        "a registered document must be confirmed via its register ack; got {:?}",
        state
            .documents
            .iter()
            .map(|(p, e)| (p.clone(), e.confirmed))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_daemon_relay_to_local() {
    let relay_url = start_relay().await;
    let (_dir, dir_path) = canonical_tempdir();
    let space_id = "6fb9558e-6fb8-4c15-811c-c31a16b7f3df";
    let doc_uuid = "test-uuid-relay-to-local";

    setup_space(&dir_path, space_id, &relay_url);

    // Create a tracked document with initial content.
    let rel_path = Path::new("test.txt");
    let abs_path = dir_path.join(rel_path);
    init_tracked_doc(&dir_path, rel_path, doc_uuid, "did:daemon", "old content");

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
    let dt_path = dt_sidecar_path(&dir_path, doc_uuid);
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
    let space_id = "8082da91-1c60-4472-80c1-ed0bcf55dd2e";
    let doc_uuid = "test-uuid-converge";

    let (_dir_a, path_a) = canonical_tempdir();
    let (_dir_b, path_b) = canonical_tempdir();

    setup_space(&path_a, space_id, &relay_url);
    setup_space(&path_b, space_id, &relay_url);

    let rel_path = Path::new("shared.txt");

    // Create tracked doc in A, then copy to B (same CRDT history).
    init_tracked_doc(
        &path_a,
        rel_path,
        doc_uuid,
        "did:a",
        "line1\nline2\nline3\n",
    );

    // Pre-seed UUID in A, then copy (state.json is copied by copy_tracked_doc).
    seed_document_uuid(&path_a, "shared.txt", doc_uuid);
    copy_tracked_doc(&path_a, &path_b, rel_path, doc_uuid);

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

/// Drive the offline window of the re-mint repro through a raw relay
/// client: unregister `old_id`, register `new_id` at `notes.md`, push its
/// content, and barrier until the relay REGISTRY shows exactly the
/// re-minted world (new doc at the path, old doc gone). Lifecycle frames
/// carry far-future HLC-stamped metadata — the registry is an HLC lattice
/// and unstamped frames cannot win arbitration. Doc ids must be real
/// UUIDs: the registry ignores non-UUID ids.
async fn remint_path_while_offline(relay_url: &str, space_id: &str, old_id: &str, new_id: &str) {
    let (ext_cmd, ext_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (ext_evt_tx, mut ext_events) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);
    spawn_client(relay_url, space_id, "did:external", ext_cmd_rx, ext_evt_tx).await;
    let connected = tokio::time::timeout(EVENT_TIMEOUT, ext_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(connected, SyncEvent::Connected { .. }));

    let lifecycle_meta =
        |intent: &str, physical_ms: u64, ts: i64| kutl_proto::sync::ChangeMetadata {
            timestamp: ts,
            author_did: "did:external".into(),
            intent: intent.into(),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms,
                logical: 0,
                actor: vec![7u8; 16],
            }),
            ..Default::default()
        };
    ext_cmd
        .send(SyncCommand::UnregisterDocument {
            space_id: space_id.into(),
            document_id: old_id.into(),
            metadata: Some(lifecycle_meta(
                "offline-window delete",
                9_999_999_000_000,
                9_999_999_000_000,
            )),
        })
        .unwrap();
    ext_cmd
        .send(SyncCommand::RegisterDocument {
            space_id: space_id.into(),
            document_id: new_id.into(),
            path: "notes.md".into(),
            metadata: Some(lifecycle_meta(
                "offline-window recreate",
                9_999_999_001_000,
                9_999_999_001_000,
            )),
            originally_created_at_ms: None,
        })
        .unwrap();
    ext_cmd
        .send(SyncCommand::Subscribe {
            document_id: new_id.into(),
        })
        .unwrap();

    let mut new_doc = Document::new();
    let agent = new_doc.register_agent("did:external").unwrap();
    let before = new_doc.local_version();
    new_doc
        .edit(
            agent,
            "did:external",
            "recreate",
            Boundary::Explicit,
            |ctx| ctx.insert(0, "recreated content\n"),
        )
        .unwrap();
    ext_cmd
        .send(SyncCommand::SendOps {
            document_id: new_id.into(),
            ops: new_doc.encode_since(&before),
            metadata: new_doc.changes_since(&before),
            content_mode: 0,
            content_hash: Vec::new(),
        })
        .unwrap();

    let registry_settled = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            ext_cmd
                .send(SyncCommand::ListSpaceDocuments {
                    space_id: space_id.into(),
                })
                .unwrap();
            match ext_events.recv().await {
                Some(SyncEvent::SpaceDocuments { documents, .. }) => {
                    let new_at_path = documents
                        .iter()
                        .any(|(id, path)| id.as_str() == new_id && path.as_str() == "notes.md");
                    let old_gone = documents.iter().all(|(id, _)| id.as_str() != old_id);
                    if new_at_path && old_gone {
                        return;
                    }
                }
                Some(_) => {}
                None => panic!("external client disconnected before the registry settled"),
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await;
    assert!(
        registry_settled.is_ok(),
        "relay registry never settled on the re-minted world"
    );
}

/// A daemon offline across a delete-then-recreate of the SAME path must
/// rejoin cleanly. While it was offline, the rest of the cluster
/// unregistered its tracked document and registered a NEW document at the
/// same path. On rejoin, startup reconciliation must settle the old
/// document's local disposition (delete + cleanup) before the new
/// document's subscribe claims the path, and the old document's
/// unregister must not tear down identity rows the new document now owns.
/// Done in the wrong order, the daemon destroys the new document's
/// identity and is left orphaned — an empty documents map and a
/// subscription that drops every catch-up frame.
#[tokio::test]
async fn test_offline_remint_same_path_rejoins_cleanly() {
    init_test_tracing();
    let relay_url = start_relay().await;
    let (_dir, dir_path) = canonical_tempdir();
    let space_id = "3f9f2b7a-52a4-4b06-9c1d-0e8a55aa11ee";
    let old_id = "aaaa1111-2222-4333-8444-555566667777";
    let new_id = "bbbb1111-2222-4333-8444-555566667777";
    let rel_path = Path::new("notes.md");
    let abs_path = dir_path.join(rel_path);

    setup_space(&dir_path, space_id, &relay_url);
    init_tracked_doc(
        &dir_path,
        rel_path,
        old_id,
        "did:daemon",
        "original content\n",
    );
    seed_document_uuid(&dir_path, "notes.md", old_id);

    // Session 1: the daemon registers and pushes the old document, recording
    // path→old_id (confirmed + materialized) in its persisted state. The
    // observer's catch-up is the barrier proving the relay holds it.
    let session1 = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:daemon",
        &relay_url,
        space_id,
    ))
    .await;
    let (_obs_cmd, mut obs_events) =
        connect_and_subscribe(&relay_url, space_id, "did:observer", old_id).await;
    let caught_up = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            match obs_events.recv().await {
                Some(SyncEvent::RemoteOps { ops, .. }) if !ops.is_empty() => return,
                None => panic!("observer disconnected before catch-up"),
                _ => {}
            }
        }
    })
    .await;
    assert!(caught_up.is_ok(), "old doc never reached the relay");

    // The daemon goes offline.
    session1.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // While it is offline, the cluster deletes the document and recreates
    // the same path as a NEW document with different content.
    remint_path_while_offline(&relay_url, space_id, old_id, new_id).await;

    // Session 2: the daemon rejoins. It must converge to the cluster state —
    // the file exists with the NEW document's content and the state map
    // tracks the NEW id at the path.
    let session2 = spawn_ready_daemon(test_daemon_config(
        dir_path.clone(),
        "did:daemon",
        &relay_url,
        space_id,
    ))
    .await;

    let converged = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            let content_ok =
                std::fs::read_to_string(&abs_path).is_ok_and(|c| c.contains("recreated content"));
            if !content_ok {
                continue;
            }
            let state: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(dir_path.join(".kutl/state.json")).unwrap(),
            )
            .unwrap();
            let tracked = state["documents"]["notes.md"]["id"]
                .as_str()
                .or_else(|| state["documents"]["notes.md"].as_str())
                .unwrap_or_default()
                .to_owned();
            if tracked == new_id {
                return;
            }
        }
    })
    .await;
    assert!(
        converged.is_ok(),
        "rejoined daemon never converged: file = {:?}, state = {:?}",
        std::fs::read_to_string(&abs_path).ok(),
        std::fs::read_to_string(dir_path.join(".kutl/state.json")).ok(),
    );

    session2.abort();
}

// ---------------------------------------------------------------------------
// Blob integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_binary_file_syncs_to_relay() {
    let relay_url = start_relay().await;
    let space_id = "a2c8a32b-6a56-4fa4-8e4f-b06c3f6c45ca";
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
    let space_id = "9a95f3d3-c9d3-41b1-8922-4864dd8d217c";
    let text_uuid = "test-uuid-coexist-text";
    let bin_uuid = "test-uuid-coexist-bin";

    let (_dir, dir_path) = canonical_tempdir();
    setup_space(&dir_path, space_id, &relay_url);

    // Create an initial text file (tracked).
    let text_rel = Path::new("readme.txt");
    init_tracked_doc(&dir_path, text_rel, text_uuid, "did:daemon", "hello text");

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
    let space_id = "d1799553-f2c1-481d-89e9-fb5d952854e1";
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
    let space_id = "5ba97049-2944-4d0c-8454-4d66934bb21f";
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

/// Start an auth-on relay (auth is unconditional) and return the ws:// URL.
///
/// Returns `(url, _keys_file)` — the keys file must outlive the test because
/// the relay's startup check refuses to boot without either a membership
/// backend or an `authorized_keys` file.
async fn start_relay_with_auth() -> (String, tempfile::NamedTempFile) {
    use std::io::Write;

    init_test_tracing();
    let mut keys = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys, "# daemon auth test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-relay-auth".into(),
        authorized_keys_file: Some(keys.path().to_path_buf()),
        ..Default::default()
    };

    // Storeless boot: see `build_storeless_relay_app`.
    let (app, _relay_handle, _flush_handle) = build_storeless_relay_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("ws://{addr}/ws"), keys)
}

/// Generate an ephemeral Ed25519 identity for testing.
fn test_identity() -> (String, SigningKey) {
    let secret: [u8; 32] = std::array::from_fn(|_| rand::random::<u8>());
    let signing_key = SigningKey::from_bytes(&secret);
    let did = kutl_signals::did_key_encode(&signing_key.verifying_key());

    (did, signing_key)
}

#[tokio::test]
async fn test_daemon_authenticates_and_syncs() {
    use std::io::Write;

    let (relay_url, mut keys) = start_relay_with_auth().await;
    let space_id = "b8c75c77-4d80-4c27-886b-ba85857682c0";
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
    init_tracked_doc(&path_a, rel_path, doc_uuid, &did_a, "hello auth");

    // Pre-seed UUID in A, then copy (state.json is copied by copy_tracked_doc).
    seed_document_uuid(&path_a, "auth-test.txt", doc_uuid);
    copy_tracked_doc(&path_a, &path_b, rel_path, doc_uuid);

    // Start daemon A with auth, modify file, verify it reaches the relay. This
    // test authorizes did_a/did_b explicitly on its own relay, so it overrides
    // both the wire DID and the signing key (not the shared default identity).
    let handle_a = spawn_ready_daemon(SpaceWorkerConfig {
        author_did: did_a.clone(),
        signing_key: Some(key_a),
        ..test_daemon_config(path_a.clone(), &did_a, &relay_url, space_id)
    })
    .await;

    std::fs::write(path_a.join(rel_path), "authenticated edit").unwrap();

    // Wait for A's daemon to detect and process the file change by watching
    // the .dt sidecar's modification time. This avoids starting B while A's
    // sync exchange is still in flight (which races with our file write).
    let dt_path = path_a.join(".kutl/docs").join(format!("{doc_uuid}.dt"));
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
        author_did: did_b.clone(),
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

// ---------------------------------------------------------------------------
// Signal-record catch-up on connect
// ---------------------------------------------------------------------------

/// Start a DURABLE auth relay (a `data_dir`, so it holds signal segments AND
/// advertises the `signal-records` capability) and return its ws URL plus the
/// keys-file and data-dir handles (kept alive by the caller). Mirrors
/// `start_relay_with_auth` but with the data dir that turns on signal records.
async fn start_durable_auth_relay() -> (String, tempfile::NamedTempFile, tempfile::TempDir) {
    use std::io::Write;

    init_test_tracing();
    let mut keys = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys, "# durable signal-records test keys").unwrap();
    let data_dir = tempfile::TempDir::new().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-durable-signal-relay".into(),
        authorized_keys_file: Some(keys.path().to_path_buf()),
        data_dir: Some(data_dir.path().to_path_buf()),
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("ws://{addr}/ws"), keys, data_dir)
}

/// The per-space signal segment directory under a space root, matching
/// `DaemonSignalStore`'s convention `<root>/.kutl/signals/<space_id>/`.
fn signal_seg_dir(root: &Path, space_id: uuid::Uuid) -> PathBuf {
    root.join(".kutl")
        .join("signals")
        .join(space_id.to_string())
}

/// Build a minimal CREATED signal record with the given HLC physical time.
fn make_record(
    space: uuid::Uuid,
    author_did: &str,
    rec_id: &str,
    ms: u64,
) -> kutl_proto::sync::Signal {
    let mut s = kutl_proto::sync::Signal {
        id: format!("sig-{rec_id}"),
        space_id: space.to_string(),
        record_id: rec_id.to_owned(),
        author_did: author_did.to_owned(),
        actor_did: author_did.to_owned(),
        hlc: Some(kutl_proto::sync::Hlc {
            physical_ms: ms,
            logical: 0,
            actor: vec![0u8; 16],
        }),
        ..Default::default()
    };
    s.set_event(kutl_proto::sync::SignalEventType::Created);
    s
}

/// Pre-seed a space root's signal segments with `records` via a
/// `DaemonSignalStore` (dropped before any daemon opens the same dir, so the
/// daemon's own flock is uncontended).
fn seed_signal_records(root: &Path, space: uuid::Uuid, records: &[kutl_proto::sync::Signal]) {
    let mut store = kutl_daemon::signal_store::DaemonSignalStore::open(root, space).unwrap();
    for r in records {
        store.append(r).unwrap();
    }
}

/// Load a space root's signal segments and return the set of `record_id`s
/// present (the fold-independent ground truth the assertions compare).
fn loaded_record_ids(root: &Path, space: uuid::Uuid) -> std::collections::BTreeSet<String> {
    let seg = signal_seg_dir(root, space);
    kutl_signals::segment::SegmentStore::load(&seg)
        .unwrap()
        .records
        .into_iter()
        .map(|r| r.record_id)
        .filter(|id| !id.is_empty())
        .collect()
}

/// Spawn a full daemon with the given signing key; wait until ready.
async fn spawn_signal_daemon(
    root: &Path,
    did: &str,
    key: SigningKey,
    relay_url: &str,
    space_id: &str,
) -> tokio::task::JoinHandle<()> {
    spawn_ready_daemon(SpaceWorkerConfig {
        author_did: did.to_owned(),
        signing_key: Some(key),
        ..test_daemon_config(root.to_path_buf(), did, relay_url, space_id)
    })
    .await
}

/// Poll until `root`'s segments contain all of `expected` record ids, or panic
/// after `PIPELINE_TIMEOUT`.
async fn await_records(root: &Path, space: uuid::Uuid, expected: &[&str]) {
    let want: std::collections::BTreeSet<String> =
        expected.iter().map(|s| (*s).to_owned()).collect();
    let got = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            let have = loaded_record_ids(root, space);
            if want.is_subset(&have) {
                return have;
            }
        }
    })
    .await;
    assert!(
        got.is_ok(),
        "timed out waiting for records {expected:?} in {}",
        root.display()
    );
}

/// Daemon A holds records the relay lacks; on connect A re-seeds them to
/// the durable relay, then a fresh daemon B connecting to the same relay catches
/// them up into its own local segments. Exercises BOTH catch-up directions
/// (push re-seed + pull) through the real relay HTTP surface.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_signal_catch_up_converges_two_daemons() {
    use std::io::Write;

    let (relay_url, mut keys, _data) = start_durable_auth_relay().await;
    let space = uuid::Uuid::new_v4();
    let space_id = space.to_string();

    let (_dir_a, path_a) = canonical_tempdir();
    let (_dir_b, path_b) = canonical_tempdir();
    setup_space(&path_a, &space_id, &relay_url);
    setup_space(&path_b, &space_id, &relay_url);

    let (did_a, key_a) = test_identity();
    let (did_b, key_b) = test_identity();
    writeln!(keys, "{did_a}").unwrap();
    writeln!(keys, "{did_b}").unwrap();
    keys.flush().unwrap();

    // Pre-seed A's segments with two records the relay does not have.
    let records = [
        make_record(space, &did_a, "rec-a", 10),
        make_record(space, &did_a, "rec-b", 20),
    ];
    seed_signal_records(&path_a, space, &records);

    // A connects → catch-up sees an empty relay high-water → re-seeds both.
    let handle_a = spawn_signal_daemon(&path_a, &did_a, key_a, &relay_url, &space_id).await;

    // B connects fresh → catch-up pulls A's re-seeded records into B's segments.
    let handle_b = spawn_signal_daemon(&path_b, &did_b, key_b, &relay_url, &space_id).await;

    await_records(&path_b, space, &["rec-a", "rec-b"]).await;

    let b_ids = loaded_record_ids(&path_b, space);
    assert!(
        b_ids.contains("rec-a") && b_ids.contains("rec-b"),
        "B's segments must hold A's records after catch-up, got {b_ids:?}"
    );

    handle_a.abort();
    handle_b.abort();
}

/// A ONE-SHOT sync completes a MULTI-chunk re-seed end to end: 120 records =
/// two chunks at the 100-record chunk size, and the pull side (daemon B)
/// pages twice, so both directions cross a chunk/page boundary.
///
/// Honest scope note: at LOCAL round-trip speeds this passes whether or not
/// each `SignalAck` refreshes the idle deadline (both chunks finish inside one
/// idle window), so it pins the multi-chunk walk itself, not the deadline —
/// that failure mode needs network latency exceeding the window and is
/// argued by construction at the arm (each ack grants a fresh window).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_one_shot_reseed_completes_past_one_chunk() {
    use std::io::Write;

    let (relay_url, mut keys, _data) = start_durable_auth_relay().await;
    let space = uuid::Uuid::new_v4();
    let space_id = space.to_string();

    let (_dir_a, path_a) = canonical_tempdir();
    let (_dir_b, path_b) = canonical_tempdir();
    setup_space(&path_a, &space_id, &relay_url);
    setup_space(&path_b, &space_id, &relay_url);

    let (did_a, key_a) = test_identity();
    let (did_b, key_b) = test_identity();
    writeln!(keys, "{did_a}").unwrap();
    writeln!(keys, "{did_b}").unwrap();
    keys.flush().unwrap();

    let records: Vec<_> = (0..120u64)
        .map(|i| make_record(space, &did_a, &format!("rec-{i:03}"), 10 + i))
        .collect();
    seed_signal_records(&path_a, space, &records);

    // ONE-SHOT run to completion — the mode whose idle deadline must not
    // expire between chunks.
    let config = SpaceWorkerConfig {
        one_shot: true,
        author_did: did_a.clone(),
        signing_key: Some(key_a),
        ..test_daemon_config(path_a.clone(), &did_a, &relay_url, &space_id)
    };
    SpaceWorker::new(config)
        .unwrap()
        .run()
        .await
        .expect("one-shot sync");

    // A fresh daemon pulling from the relay proves the SECOND chunk landed:
    // rec-119 exists only if the walk continued past the first ack.
    let handle_b = spawn_signal_daemon(&path_b, &did_b, key_b, &relay_url, &space_id).await;
    await_records(&path_b, space, &["rec-000", "rec-050", "rec-119"]).await;
    handle_b.abort();
}

/// Push a batch of records onto the relay directly over HTTP (as `did`, which
/// must be an authorized member), so the test can arrange relay-side state a
/// daemon then catches up. Pushes over the `SignalReseed` FRAME — the same
/// door every real pusher uses.
async fn relay_reseed(
    relay_url: &str,
    space_id: &str,
    did: &str,
    key: &SigningKey,
    records: Vec<kutl_proto::sync::Signal>,
) {
    let token = kutl_client::authenticate(relay_url, did, key)
        .await
        .expect("authenticate for relay reseed");
    let mut client =
        kutl_client::SyncClient::connect_with_auth(relay_url, "test-reseed", &token, "")
            .await
            .expect("connect for relay reseed");
    client
        .submit_signal(&kutl_proto::protocol::signal_reseed_envelope(
            &uuid::Uuid::new_v4().to_string(),
            space_id,
            records,
        ))
        .await
        .expect("relay reseed ok");
    let _ = client.close().await;
}

/// After a daemon ingests records (its cursor advances), a "restart" (a
/// new worker on the SAME `space_root`) resumes catch-up from the persisted
/// cursor. Proven by placing a NEW record on the relay ABOVE the cursor between
/// runs and asserting the restarted daemon PULLS it (so catch-up genuinely ran
/// on restart) while its earlier records are NOT duplicated. Also asserts a
/// CORRUPT cursor degrades to from-zero without wedging (the daemon still
/// becomes ready and no records are lost or doubled).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_signal_catch_up_resumes_from_persisted_cursor_after_restart() {
    use std::io::Write;

    let (relay_url, mut keys, _data) = start_durable_auth_relay().await;
    let space = uuid::Uuid::new_v4();
    let space_id = space.to_string();

    let (_dir_a, path_a) = canonical_tempdir();
    setup_space(&path_a, &space_id, &relay_url);
    let (did_a, key_a) = test_identity();
    // A second authorized member that publishes records to the relay directly.
    let (did_pub, key_pub) = test_identity();
    writeln!(keys, "{did_a}").unwrap();
    writeln!(keys, "{did_pub}").unwrap();
    keys.flush().unwrap();

    // The publisher puts a,b (physical_ms 10,20) on the relay BEFORE A starts.
    relay_reseed(
        &relay_url,
        &space_id,
        &did_pub,
        &key_pub,
        vec![
            make_record(space, &did_pub, "rec-a", 10),
            make_record(space, &did_pub, "rec-b", 20),
        ],
    )
    .await;

    // Run 1: A (empty local segments) connects and PULLS a,b via catch-up, so
    // its cursor advances to the top of what it ingests (physical_ms 20).
    let handle_a1 =
        spawn_signal_daemon(&path_a, &did_a, key_a.clone(), &relay_url, &space_id).await;
    await_records(&path_a, space, &["rec-a", "rec-b"]).await;
    let seg = signal_seg_dir(&path_a, space);

    handle_a1.abort();
    // Give the aborted worker a moment to drop its flock.
    tokio::time::sleep(POLL_INTERVAL).await;
    // The first run must have written a cursor (the ingest path advances it).
    let cursor_path = seg.join(kutl_signals::catchup::CURSOR_FILE_NAME);
    assert!(
        cursor_path.exists(),
        "the first session must persist a catch-up cursor"
    );

    // Between runs, a DIFFERENT member publishes rec-c (physical_ms 30) — ABOVE
    // A's cursor — to the relay. A holds neither this record nor its bytes.
    relay_reseed(
        &relay_url,
        &space_id,
        &did_pub,
        &key_pub,
        vec![make_record(space, &did_pub, "rec-c", 30)],
    )
    .await;

    // Restart with a VALID persisted cursor: catch-up resumes from it and PULLS
    // rec-c (above the cursor). If the cursor were ignored the pull would still
    // land rec-c, but the resume path is what carries the daemon there.
    let handle_a2 =
        spawn_signal_daemon(&path_a, &did_a, key_a.clone(), &relay_url, &space_id).await;
    await_records(&path_a, space, &["rec-a", "rec-b", "rec-c"]).await;
    let ids = loaded_record_ids(&path_a, space);
    assert_eq!(
        ids,
        ["rec-a", "rec-b", "rec-c"]
            .into_iter()
            .map(str::to_owned)
            .collect::<std::collections::BTreeSet<_>>(),
        "restart resumes catch-up and pulls the new record with no duplicates"
    );
    handle_a2.abort();
    tokio::time::sleep(POLL_INTERVAL).await;

    // Corrupt the cursor: the next restart must degrade to from-zero (idempotent
    // re-overlap), never wedge — the daemon still becomes ready and the three
    // records remain present with no duplicates.
    std::fs::write(&cursor_path, b"{ not valid json").unwrap();
    let handle_a3 = spawn_signal_daemon(&path_a, &did_a, key_a, &relay_url, &space_id).await;
    await_records(&path_a, space, &["rec-a", "rec-b", "rec-c"]).await;
    let ids = loaded_record_ids(&path_a, space);
    assert_eq!(
        ids,
        ["rec-a", "rec-b", "rec-c"]
            .into_iter()
            .map(str::to_owned)
            .collect::<std::collections::BTreeSet<_>>(),
        "a corrupt cursor degrades to from-zero without wedging or duplicating"
    );
    handle_a3.abort();
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

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
    let (event_tx, mut event_rx) = mpsc::channel::<SyncEvent>(TEST_CHANNEL_CAPACITY);

    let _handle = spawn_client_with_token(
        &relay_url,
        "e73c023a-2e8e-4034-8bb4-d853730e1bfc",
        "did:no-auth",
        "",
        cmd_rx,
        event_tx,
    );

    // Without a valid token the relay refuses the handshake — and the refusal
    // must arrive as a rejection carrying the relay's OWN auth verdict, not as
    // a bare disconnect. Surfaces name the credential slot to change off that
    // flag, so losing it turns an actionable refusal back into "token not
    // found". The relay answers with the error frame and closes, so this
    // is the only event the client can produce here.
    let result = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            match event_rx.recv().await {
                Some(SyncEvent::HandshakeRejected { auth_failed, .. }) => {
                    assert!(auth_failed, "a refused bearer must carry the auth verdict");
                    return true;
                }
                Some(SyncEvent::Error { .. } | SyncEvent::Disconnected) | None => {
                    panic!("a refused bearer must surface as HandshakeRejected")
                }
                Some(SyncEvent::Connected { .. }) => return false,
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
    let space_id = "8b7c24d8-5e58-4723-8db6-35c587aa2638";
    setup_space(&dir_path, space_id, &relay_url);

    // Seed two tracked docs with distinct UUIDs.
    let a_rel = Path::new("a.md");
    let b_rel = Path::new("b.md");
    init_tracked_doc(&dir_path, a_rel, "uuid-a", "did:test", "content a");
    init_tracked_doc(&dir_path, b_rel, "uuid-b", "did:test", "content b");
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
    // Sidecars are keyed by document id, so the sentinel's path-named sidecar
    // never appears; instead wait until the daemon has registered _sentinel.md
    // (registration writes state.json). Processing is serial, so by then the
    // earlier rename event was already observed (and rejected).
    let state_path = dir_path.join(".kutl/state.json");
    tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if std::fs::read_to_string(&state_path).is_ok_and(|s| s.contains("_sentinel.md")) {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("daemon should process the sentinel file");
    // daemon did NOT register a document at the colliding path A.md (state.json
    // has no A.md entry), while both original docs' id-keyed sidecars survive.
    let docs_dir = dir_path.join(".kutl").join("docs");
    let state = std::fs::read_to_string(&state_path).unwrap();
    assert!(
        !state.contains("A.md"),
        "daemon should not have propagated rename; no A.md mapping in state: {state}"
    );
    assert!(
        docs_dir.join("uuid-a.dt").exists(),
        "original a.md (uuid-a) sidecar should still exist"
    );
    assert!(
        docs_dir.join("uuid-b.dt").exists(),
        "b.md (uuid-b) sidecar should still exist (rename rejected leaves old mapping)"
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
    let space_id = "0d5c840a-520c-4a37-8f10-9a171863798d";

    setup_space(&dir_path, space_id, &relay_url);

    // Seed tracked "foo.md" with initial content and a known UUID.
    let rel_path = Path::new("foo.md");
    init_tracked_doc(&dir_path, rel_path, "uuid-foo", "did:test", "original");
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
    // Sidecars are id-keyed, so wait on the daemon registering _sentinel.md
    // (which writes state.json) rather than a path-named sidecar. Serial
    // processing means Foo.md's event was already observed (and rejected) by then.
    let state_path = dir_path.join(".kutl/state.json");
    tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if std::fs::read_to_string(&state_path).is_ok_and(|s| s.contains("_sentinel.md")) {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("daemon should process the sentinel file");
    // what records a path → id mapping in state.json, so its absence proves
    // rejection (sidecars are id-keyed, so there is no path-named sidecar to check).
    let state = std::fs::read_to_string(&state_path).unwrap();
    assert!(
        !state.contains("Foo.md"),
        "daemon should not have registered case-variant Foo.md, but state has it: {state}"
    );

    // And foo.md's (uuid-foo) sidecar should still be in place.
    let original_dt = dir_path.join(".kutl").join("docs").join("uuid-foo.dt");
    assert!(
        original_dt.exists(),
        "original foo.md (uuid-foo) sidecar should still exist at {}",
        original_dt.display()
    );

    daemon_handle.abort();
}

// ---------------------------------------------------------------------------
// Bulk-seed deadlock reproduction (the proof the outbound cycles are closed)
// ---------------------------------------------------------------------------

/// Count documents marked `confirmed: true` in a space's `state.json`.
fn count_confirmed(state_path: &Path) -> usize {
    let Ok(raw) = std::fs::read_to_string(state_path) else {
        return 0;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return 0;
    };
    value["documents"].as_object().map_or(0, |docs| {
        docs.values()
            .filter(|d| d["confirmed"].as_bool() == Some(true))
            .count()
    })
}

/// Bulk-seed many fresh documents on disk WHILE the daemon is live and require
/// it to register them ALL — reaching `confirmed: true` in `state.json` — within
/// the timeout.
///
/// Reproduces a `cp -R` of a few hundred files into a daemon-watched space:
/// with the daemon already in its event loop, dropping `N` files makes the
/// watcher fire `N` `file_event`s; for each, the loop registers the document
/// (a `sync_cmd` send) while the relay's acks stream back on `sync_event`. `N` is
/// well past the bounded `CHANNEL_CAPACITY` (64) so the burst overruns the
/// `sync_cmd` channel — the condition under which a BLOCKING bounded outbound
/// send from inside the loop would park
/// the loop while it should be draining inbound, which is why the outbound
/// side is unbounded and gated at intake.
///
/// A regression guard, NOT a deterministic proof of the deadlock: the
/// WS read/write split closes the sender-side cycle here,
/// and the receiving-side `suppress`↔`file_event` cycle is masked in this
/// in-process harness by the relay's slow-subscriber EVICTION (`CTRL_CAPACITY`/
/// data lane = 16, `relay_and_evict`/`send_broadcast`), which sheds a burst
/// before the daemon's own 64-deep channels saturate. This test pins that the
/// bulk path converges and stays non-blocking under the unbounded channels; a
/// timeout here is a stall, not slowness (a healthy daemon does `N` tiny docs in
/// well under a second).
#[tokio::test]
async fn test_bulk_seed_many_documents_does_not_deadlock() {
    /// Number of docs to drop on disk at once; deliberately well past the
    /// bounded `CHANNEL_CAPACITY` of 64 so the registration burst overruns it.
    const N: usize = 200;

    let relay_url = start_relay().await;
    let (_tmp, space_root) = canonical_tempdir();
    let space_id = "2f6b0ea4-88fc-44c0-8e08-4a80b9ffa8fc";
    setup_space(&space_root, space_id, &relay_url);

    // Start the daemon on the (empty) space and wait until it is fully ready
    // (connected, subscribed, initial scan done, watcher running) — so the burst
    // lands on the LIVE event loop, exercising the `file_event` intake
    // concurrently with the `sync_cmd` registration sends and the inbound acks.
    let config = test_daemon_config(space_root.clone(), "did:test-bulk", &relay_url, space_id);
    let handle = tokio::time::timeout(DAEMON_READY_TIMEOUT, spawn_ready_daemon(config))
        .await
        .expect("daemon never became ready within the timeout");

    // Let the OS-level watcher finish registering before the burst, then drop all
    // N files at once (the `cp -R` shape: a flood the watcher debounces into one
    // wave of file events).
    tokio::time::sleep(WATCHER_INIT_DELAY).await;
    for i in 0..N {
        std::fs::write(
            space_root.join(format!("doc_{i:03}.md")),
            format!("# Document {i}\n\nbody for document {i}\n"),
        )
        .unwrap();
    }

    let state_path = space_root.join(".kutl/state.json");
    let synced = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if count_confirmed(&state_path) >= N {
                break;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await;
    assert!(
        synced.is_ok(),
        "only {}/{N} documents confirmed within the timeout (sync loop deadlocked)",
        count_confirmed(&state_path)
    );

    handle.abort();
}

/// A bulk register of well over the bounded
/// channel capacity through a real daemon + relay confirms EVERY doc (the
/// deadlock repro, as an integration check on the gamma placement path).
///
/// Gamma is the placement
/// authority (`classify_*` → `handle` → `reconcile_placement`): each local
/// create emits a `GuardedPlace(Register)` whose driver-applied critical section
/// claims identity + subscribes; the relay's `LifecycleAck` then confirms the
/// doc. A timeout here is a stall in the placement path, not slowness — a
/// healthy daemon does N tiny docs in well under a second.
#[tokio::test]
async fn test_gamma_bulk_register_confirms_all() {
    /// Number of docs to drop at once; well past the bounded
    /// `CHANNEL_CAPACITY` (64) so the registration burst overruns it.
    const N: usize = 100;

    let relay_url = start_relay().await;
    let (_dir, root) = canonical_tempdir();
    let space_id = "2812891c-6f85-40bf-84b4-0fc66949ce77";
    setup_space(&root, space_id, &relay_url);

    let config = test_daemon_config(root.clone(), "did:gamma-bulk", &relay_url, space_id);
    let handle = tokio::time::timeout(DAEMON_READY_TIMEOUT, spawn_ready_daemon(config))
        .await
        .expect("daemon never became ready within the timeout");

    tokio::time::sleep(WATCHER_INIT_DELAY).await;
    for i in 0..N {
        std::fs::write(root.join(format!("doc{i}.md")), format!("content {i}")).unwrap();
    }

    let state_path = root.join(".kutl/state.json");
    let all_confirmed = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if count_confirmed(&state_path) == N {
                break;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await;
    assert!(
        all_confirmed.is_ok(),
        "gamma path: only {}/{N} docs confirmed (no deadlock expected)",
        count_confirmed(&state_path)
    );

    handle.abort();
}

/// Startup re-subscribe of MANY pre-existing relay documents must not deadlock.
///
/// The sibling bulk tests above drive the LOCAL→relay direction (new local files
/// → `RegisterDocument` bursts) against an EMPTY relay, and they start the burst
/// only after the daemon is already at the live `event_loop`. They do not cover
/// the RESTART path: a daemon that already shares N docs with the relay and, on
/// startup, must re-subscribe all of them. There, `startup_reconciliation` sends
/// the subscribe burst and the relay floods its responses back into the bounded
/// inbound `sync_event` channel (`CHANNEL_CAPACITY`) BEFORE `event_loop` — the
/// only drainer — has started. Past the channel depth the WS read loop blocks on
/// `event_tx.send().await` inside `handle_inbound`, so it stops reading frames
/// (and stops answering keepalive pings); the daemon never reaches `ready`.
///
/// A space with a couple hundred docs is enough to hit this in the field: the
/// read loop wedges ~64 acks in and an upstream WS keepalive reaper closes the
/// connection. This pins the daemon-side invariant directly: a restart that
/// re-subscribes N ≫ `CHANNEL_CAPACITY` docs becomes ready and keeps every doc
/// confirmed. A timeout here is the startup deadlock, not slowness.
#[tokio::test]
async fn test_restart_resubscribe_many_documents_does_not_deadlock() {
    /// Well past the bounded inbound `CHANNEL_CAPACITY` (64) so the startup
    /// re-subscribe responses overrun it before the event loop drains.
    const N: usize = 200;

    let relay_url = start_relay().await;
    let (_tmp, root) = canonical_tempdir();
    let space_id = "bc2058c4-c312-4181-85d7-f2ea9ca0eb56";
    setup_space(&root, space_id, &relay_url);
    let state_path = root.join(".kutl/state.json");

    // Phase 1: a first daemon session creates N docs and confirms them all, so
    // the relay (and local state.json) end up holding N shared documents.
    let h1 = tokio::time::timeout(
        PIPELINE_TIMEOUT,
        spawn_ready_daemon(test_daemon_config(
            root.clone(),
            "did:restart",
            &relay_url,
            space_id,
        )),
    )
    .await
    .expect("phase-1 daemon never became ready");
    tokio::time::sleep(WATCHER_INIT_DELAY).await;
    for i in 0..N {
        std::fs::write(
            root.join(format!("doc_{i:03}.md")),
            format!("# Document {i}\n\nbody for document {i}\n"),
        )
        .unwrap();
    }
    tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if count_confirmed(&state_path) >= N {
                break;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "phase-1 seeding stalled: only {}/{N} confirmed",
            count_confirmed(&state_path)
        )
    });
    h1.abort();
    // Let the aborted session drop its WS client + watcher before restarting on
    // the same space dir.
    tokio::time::sleep(WATCHER_INIT_DELAY).await;

    // Phase 2: RESTART. The relay still holds N docs and local state lists them,
    // so startup re-subscribes all N. This is the path that deadlocks.
    let h2 = tokio::time::timeout(
        PIPELINE_TIMEOUT,
        spawn_ready_daemon(test_daemon_config(
            root.clone(),
            "did:restart",
            &relay_url,
            space_id,
        )),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "restarted daemon never became ready (startup re-subscribe of {N} docs deadlocked); \
             {}/{N} still confirmed",
            count_confirmed(&state_path)
        )
    });

    // The restart must keep every doc confirmed (no regression to unconfirmed).
    let still_confirmed = tokio::time::timeout(PIPELINE_TIMEOUT, async {
        loop {
            if count_confirmed(&state_path) >= N {
                break;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await;
    assert!(
        still_confirmed.is_ok(),
        "after restart only {}/{N} docs confirmed",
        count_confirmed(&state_path)
    );

    h2.abort();
}
