mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    PROTOCOL_VERSION_MAJOR, PROTOCOL_VERSION_MINOR, blob_ops_envelope, decode_envelope,
    encode_envelope, handshake_envelope, handshake_envelope_with_token, presence_update_envelope,
    subscribe_envelope, sync_ops_envelope,
};
use kutl_proto::sync::{self, SyncEnvelope, sync_envelope::Payload};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;

use kutl_relay::config::RelayConfig;
use kutl_relay::observer::{AfterMergeObserver, MergedEvent};
use kutl_relay::relay::{Relay, RelayCommand};

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Per-relay `authorized_keys` file registry, keyed by the relay's `host:port`
/// address. Each `start_relay()` relay owns its own live-reloaded keys file, so
/// tests running in parallel never contend on a single shared file. The
/// `Mutex` guarding each file also serializes appends within one relay, so a
/// live-reload read never observes a torn line. The `NamedTempFile` handles are
/// held here for the whole process so the files outlive their tests.
type KeysRegistry =
    std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<AuthKeysFile>>>;

/// A relay's authorized-keys file plus a lock that serializes appends to it.
struct AuthKeysFile {
    file: tempfile::NamedTempFile,
    write_lock: std::sync::Mutex<()>,
}

static AUTH_KEYS_REGISTRY: std::sync::LazyLock<KeysRegistry> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Look up the authorized-keys file for the relay at `addr`.
fn keys_for(addr: &str) -> std::sync::Arc<AuthKeysFile> {
    AUTH_KEYS_REGISTRY
        .lock()
        .unwrap()
        .get(addr)
        .expect("relay address must be registered by start_relay()")
        .clone()
}

/// Append a bare DID line to the `authorized_keys` file for the relay at
/// `addr`, authorizing that DID for all spaces. Live-reloaded per check (and
/// serialized against other appends via the per-relay write-lock), so the DID
/// is usable immediately.
fn authorize_did(addr: &str, did: &str) {
    let keys = keys_for(addr);
    common::authorize_did_locked(keys.file.path(), did, &keys.write_lock);
}

/// Start a relay on a random port and return the address.
///
/// The relay boots auth-on (auth is unconditional) pointed at a fresh per-relay
/// `authorized_keys` file (registered in [`AUTH_KEYS_REGISTRY`] by `addr`).
/// Test clients enroll their own DIDs into that file and authenticate
/// transparently inside [`TestClient::handshake`], so most tests need no
/// per-test auth wiring.
async fn start_relay() -> String {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "# per-relay sync test keys").unwrap();
    let keys_path = file.path().to_path_buf();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    AUTH_KEYS_REGISTRY.lock().unwrap().insert(
        addr.clone(),
        std::sync::Arc::new(AuthKeysFile {
            file,
            write_lock: std::sync::Mutex::new(()),
        }),
    );

    let mut config = test_relay_config();
    config.authorized_keys_file = Some(keys_path);

    // Storeless boot: these sync tests use in-memory registries; the
    // OSS binary's build_app requires a data dir, so construct the storeless
    // shape explicitly via the host seam.
    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

/// Start an auth-on relay on a random port (auth is unconditional) with a
/// fresh, empty temp `authorized_keys` file. Returns `(addr, _keys_file)` — the keys
/// file must outlive the test. Used by auth tests that need an isolated keys
/// file distinct from the shared one.
async fn start_relay_with_auth() -> (String, tempfile::NamedTempFile) {
    use std::io::Write;
    let mut keys = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys, "# sync test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let mut config = test_relay_config();
    config.authorized_keys_file = Some(keys.path().to_path_buf());

    // Storeless boot: see `start_relay`.
    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys)
}

/// Perform the full auth flow (challenge + sign + verify) and return the token.
///
/// Thin `addr`-based wrapper over [`common::authenticate`] — this file's helpers
/// are all keyed by a bare `host:port` address, so wrap it into the `http://`
/// base URL the shared helper expects.
async fn authenticate(addr: &str, did: &str, signing_key: &SigningKey) -> String {
    common::authenticate(&format!("http://{addr}"), did, signing_key).await
}

/// A thin WebSocket test client.
struct TestClient {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    /// Relay address, retained so [`TestClient::handshake`] can mint a bearer
    /// token via the real `/auth/challenge` + `/auth/verify` flow. Empty for
    /// manually-constructed clients that drive their own token handshake.
    addr: String,
}

impl TestClient {
    async fn connect(addr: &str) -> Self {
        let url = format!("ws://{addr}/ws");
        let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        Self {
            ws,
            addr: addr.to_owned(),
        }
    }

    async fn send_envelope(&mut self, envelope: &SyncEnvelope) {
        let bytes = encode_envelope(envelope);
        self.ws
            .send(tungstenite::Message::Binary(bytes.into()))
            .await
            .unwrap();
    }

    async fn recv_envelope(&mut self) -> SyncEnvelope {
        let msg = tokio::time::timeout(Duration::from_secs(5), self.ws.next())
            .await
            .expect("recv timed out")
            .expect("stream ended")
            .expect("ws error");

        match msg {
            tungstenite::Message::Binary(bytes) => decode_envelope(&bytes).unwrap(),
            other => panic!("expected binary frame, got {other:?}"),
        }
    }

    /// Enroll a fresh `test_keypair()` DID into the shared `authorized_keys`
    /// file, mint a real bearer token, and complete the auth-on handshake.
    ///
    /// Centralizes all per-client auth so callers on the default `start_relay()`
    /// relay need no auth wiring of their own.
    async fn handshake(&mut self) {
        let (did, signing_key) = common::test_keypair();
        authorize_did(&self.addr, &did);
        let token = authenticate(&self.addr, &did, &signing_key).await;

        self.handshake_with_token(&token).await;
    }

    /// Complete an auth-on handshake with an already-minted bearer token.
    /// Used by polling probes that mint one token and reuse it across many
    /// short-lived connections instead of re-authenticating each iteration.
    async fn handshake_with_token(&mut self, token: &str) {
        self.send_envelope(&handshake_envelope_with_token(
            "d5fe8251-3196-4a97-8d81-66092b9a47dc",
            token,
            "",
        ))
        .await;

        let ack = self.recv_envelope().await;
        assert!(
            matches!(ack.payload, Some(Payload::HandshakeAck(_))),
            "expected HandshakeAck, got {ack:?}"
        );
    }

    async fn subscribe(&mut self, space: &str, doc: &str) -> Option<sync::SyncOps> {
        self.send_envelope(&subscribe_envelope(space, doc)).await;

        // Drain envelopes until we see `SyncOps` (catch-up). A
        // `SubscribeStatus` envelope arrives first (ctrl before data).
        for _ in 0..3 {
            match tokio::time::timeout(Duration::from_millis(200), self.ws.next()).await {
                Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => {
                    let env = decode_envelope(&bytes).unwrap();
                    match env.payload {
                        Some(Payload::SyncOps(ops)) => return Some(ops),
                        Some(Payload::SubscribeStatus(_)) => {}
                        _ => return None,
                    }
                }
                _ => return None,
            }
        }
        None
    }

    async fn send_ops(
        &mut self,
        space: &str,
        doc_id: &str,
        ops: Vec<u8>,
        metadata: Vec<sync::ChangeMetadata>,
    ) {
        self.send_envelope(&sync_ops_envelope(
            space,
            doc_id,
            ops,
            metadata,
            std::collections::HashMap::new(),
        ))
        .await;
    }

    async fn recv_ops(&mut self) -> sync::SyncOps {
        let envelope = self.recv_envelope().await;
        match envelope.payload {
            Some(Payload::SyncOps(ops)) => ops,
            other => panic!("expected SyncOps, got {other:?}"),
        }
    }

    async fn send_blob(
        &mut self,
        space: &str,
        doc_id: &str,
        blob: Vec<u8>,
        hash: Vec<u8>,
        metadata: Option<sync::ChangeMetadata>,
    ) {
        self.send_envelope(&blob_ops_envelope(space, doc_id, blob, hash, metadata))
            .await;
    }

    async fn recv_error(&mut self) -> sync::Error {
        let envelope = self.recv_envelope().await;
        match envelope.payload {
            Some(Payload::Error(e)) => e,
            other => panic!("expected Error, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Polling helpers
//
// Poll instead of using fixed `tokio::time::sleep` waits for the relay to
// integrate prior writes before a fresh client can observe them.
// Polling lets the happy path return in single-digit milliseconds while still
// tolerating slow CI machines, and removes a class of timing flakes.
//
// Do NOT use these for tests that legitimately wait on real timing
// (e.g. the background flush task in `storage.rs` / `quota.rs`).
// ---------------------------------------------------------------------------

/// How long to keep polling before declaring a relay state never settled.
const POLL_TIMEOUT: Duration = Duration::from_secs(2);

/// How often to retry inside the polling helpers. Short enough that a
/// fast machine sees no observable wall-clock cost.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Delay inserted between two writes that must receive distinct
/// server-authoritative timestamps. The relay stamps each blob with
/// `now_ms()`, so back-to-back writes on localhost can collide in the same
/// millisecond and fall through to the content-hash tiebreak instead of the
/// timestamp ordering under test. A sleep can only run longer than requested,
/// never shorter, so this deterministically advances the wall clock by ≥1ms.
const DISTINCT_TIMESTAMP_DELAY: Duration = Duration::from_millis(10);

/// Repeatedly subscribe via fresh probe clients until `predicate` returns
/// true on the catch-up `SyncOps`. Returns the matching catch-up.
async fn wait_for_doc_state(
    addr: &str,
    space: &str,
    doc: &str,
    predicate: impl Fn(&sync::SyncOps) -> bool,
) -> sync::SyncOps {
    // Enroll + authenticate one probe identity and reuse its token across every
    // poll iteration — re-authenticating per iteration would be needless auth
    // round-trips and keys-file churn.
    let (did, signing_key) = common::test_keypair();
    authorize_did(addr, &did);
    let token = authenticate(addr, &did, &signing_key).await;

    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let mut probe = TestClient::connect(addr).await;
        probe.handshake_with_token(&token).await;
        if let Some(s) = probe.subscribe(space, doc).await
            && predicate(&s)
        {
            return s;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "wait_for_doc_state timed out for {space}/{doc}",
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Convenience: wait until a fresh subscriber sees a non-empty catch-up.
/// Use [`wait_for_doc_state`] when the test needs to wait for specific content.
async fn wait_for_doc_catchup(addr: &str, space: &str, doc: &str) -> sync::SyncOps {
    wait_for_doc_state(addr, space, doc, |s| !s.ops.is_empty()).await
}

/// Repeatedly issue `ListSpaceDocuments` until at least `expected` documents
/// are present. Avoids a fixed sleep waiting for register-document
/// envelopes to land before a fresh client lists them.
async fn wait_for_doc_count(addr: &str, space: &str, expected: usize) {
    // One probe identity + token, reused across poll iterations.
    let (did, signing_key) = common::test_keypair();
    authorize_did(addr, &did);
    let token = authenticate(addr, &did, &signing_key).await;

    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let mut probe = TestClient::connect(addr).await;
        probe.handshake_with_token(&token).await;
        let req = SyncEnvelope {
            payload: Some(Payload::ListSpaceDocuments(sync::ListSpaceDocuments {
                space_id: space.into(),
            })),
        };
        probe.send_envelope(&req).await;
        let resp = probe.recv_envelope().await;
        if let Some(Payload::ListSpaceDocumentsResult(r)) = resp.payload
            && r.documents.len() >= expected
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "wait_for_doc_count({expected}) timed out for {space}",
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Create a local document with a single edit, returning ops bytes and changes.
fn make_edit(agent_name: &str, text: &str, intent: &str) -> (Vec<u8>, Vec<sync::ChangeMetadata>) {
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent(agent_name).unwrap();
    doc.edit(
        agent,
        agent_name,
        intent,
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, text),
    )
    .unwrap();
    let ops = doc.encode_since(&[]);
    let metadata = doc.changes_since(&[]);
    (ops, metadata)
}

/// Merge received sync ops into a fresh document and return its content.
fn apply_ops(sync_ops: &sync::SyncOps) -> String {
    let mut doc = kutl_core::Document::new();
    doc.merge(&sync_ops.ops, &sync_ops.metadata).unwrap();
    doc.content()
}

// ---------------------------------------------------------------------------
// Standalone relay test helpers
// ---------------------------------------------------------------------------

/// Default space ID for standalone relay tests.
/// Space ids are relay-minted UUIDs, and the relay now enforces that at
/// the authorization boundary — a non-UUID names no space that exists.
const TEST_SPACE: &str = "5171e0a1-0000-4000-8000-000000000006";

/// Default document ID for standalone relay tests.
// Real UUIDs. The relay's `handle_signal` flag-creation path
// validates `document_id` against the RFC 4122 shape (the registry is
// UUID-keyed), so synthetic strings like `"doc"` get rejected with
// `InvalidMessage`. Synthetic strings remain fine for paths that
// don't run through the WS signal handler (e.g. RegisterDocument /
// edit_document still accept arbitrary identifiers).
const TEST_DOC: &str = "00000000-0000-4000-8000-0000000000d0";
const TEST_DOC_1: &str = "00000000-0000-4000-8000-0000000000d1";
const TEST_DOC_2: &str = "00000000-0000-4000-8000-0000000000d2";

/// Number of ops to send when flooding a channel — enough to overflow
/// `SLOW_CHANNEL_CAPACITY` (2) several times over.
const FLOOD_OPS: usize = 10;

/// Fast channel capacity — large enough to never trigger eviction.
const FAST_CHANNEL_CAPACITY: usize = 256;

/// Slow channel capacity — small enough to trigger eviction deterministically.
const SLOW_CHANNEL_CAPACITY: usize = 2;

/// Default ctrl channel capacity for tests.
const TEST_CTRL_CAPACITY: usize = 8;

/// Ctrl channel capacity that's just large enough to hold the handshake ack
/// but too small for the ack + a stale notice.
const TINY_CTRL_CAPACITY: usize = 1;

/// Number of slow subscribers in the all-slow test.
const ALL_SLOW_SUBSCRIBER_COUNT: usize = 5;

use common::TestConn;

/// Create a [`RelayConfig`] for standalone relay tests.
fn test_relay_config() -> RelayConfig {
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        outbound_capacity: FAST_CHANNEL_CAPACITY,
        // Authentication is mandatory; point the standalone actor
        // tests at the shared `authorized_keys` file [`connect_client`] enrolls
        // each connection's identity into (live-reloaded per auth check).
        authorized_keys_file: Some(actor_auth_keys_path()),
        ..Default::default()
    }
}

/// Shared `authorized_keys` file for the standalone (in-process) actor tests,
/// paired with a mutex that serializes appends. [`connect_client`] enrolls each
/// connection's identity here before attaching it; `authorized_keys` re-reads
/// the file on every check, so a bare-DID line grants that identity every
/// space. The mutex keeps concurrent tests' appends from interleaving bytes.
static ACTOR_AUTH_KEYS: std::sync::LazyLock<(std::sync::Mutex<()>, tempfile::NamedTempFile)> =
    std::sync::LazyLock::new(|| {
        (
            std::sync::Mutex::new(()),
            tempfile::NamedTempFile::new().unwrap(),
        )
    });

/// Path of the shared actor `authorized_keys` file.
fn actor_auth_keys_path() -> std::path::PathBuf {
    ACTOR_AUTH_KEYS.1.path().to_path_buf()
}

/// Enroll `identity` into the shared actor `authorized_keys` file so a
/// standalone-relay connection authenticated as it passes `authorize_space`.
/// Serialized so a concurrent test's append cannot interleave.
fn enroll_actor_identity(identity: &str) {
    common::authorize_did_locked(&actor_auth_keys_path(), identity, &ACTOR_AUTH_KEYS.0);
}

/// Connect a client and attach an authenticated identity.
///
/// Authentication is mandatory; the in-process actor tests cannot
/// run the real HTTP challenge-response flow, so the connection's identity (the
/// `name`) is enrolled into the shared keys file and attached directly. Tests
/// that assert a specific `author_did` pass that DID as `name`, because
/// `authoritative_author_did` overrides client-supplied metadata with the
/// connection's authenticated identity.
async fn connect_client(
    relay: &mut Relay,
    conn_id: u64,
    name: &str,
    data_capacity: usize,
    ctrl_capacity: usize,
) -> TestConn {
    let (tx, data_rx) = mpsc::channel(data_capacity);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(ctrl_capacity);
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();

    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx,
            ctrl_tx,
            ack_tx,
        })
        .await;
    // Authentication is mandatory; enroll the connection's identity
    // (its `name`) and attach it directly rather than running the real handshake.
    enroll_actor_identity(name);
    relay.test_set_authenticated(conn_id, name);

    TestConn {
        conn_id,
        data_rx,
        ack_rx,
        ctrl_rx,
    }
}

/// Subscribe a connection to the default test document.
async fn subscribe_to_doc(relay: &mut Relay, conn_id: u64) {
    subscribe_to(relay, conn_id, TEST_SPACE, TEST_DOC).await;
}

/// Join a space's SIGNAL stream.
///
/// Distinct from [`subscribe_to_doc`]: a document subscription does not put a
/// connection in the space's signal recipient set. Conflating the two would
/// force a watcher like `kutl watch` to subscribe to a sentinel document it
/// has no interest in.
async fn subscribe_to_signals(relay: &mut Relay, conn_id: u64, space: &str) {
    relay
        .process_command(RelayCommand::SubscribeSignals {
            conn_id,
            msg: sync::SubscribeSignals {
                space_id: space.into(),
                cursor: None,
            },
        })
        .await;
}

/// Subscribe a connection to a specific document.
async fn subscribe_to(relay: &mut Relay, conn_id: u64, space: &str, doc: &str) {
    relay
        .process_command(RelayCommand::Subscribe {
            conn_id,
            msg: sync::Subscribe {
                space_id: space.into(),
                document_id: doc.into(),
            },
        })
        .await;
}

/// Register a document at `path` in the test space (used to emit a lifecycle
/// broadcast to the space's subscribers). The document id is derived from the
/// path so distinct paths yield distinct ids.
async fn register_doc(relay: &mut Relay, conn_id: u64, path: &str) {
    let document_id = format!("00000000-0000-0000-0000-{:012x}", path.len());
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id,
            msg: sync::RegisterDocument {
                space_id: TEST_SPACE.into(),
                document_id,
                path: path.into(),
                metadata: Some(sync::ChangeMetadata {
                    author_did: format!("did:c{conn_id}"),
                    timestamp: kutl_core::now_ms(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        })
        .await;
}

/// Send `count` ops from `conn_id` to the default test document.
async fn send_test_ops(relay: &mut Relay, conn_id: u64, count: usize) {
    send_test_ops_to(relay, conn_id, TEST_SPACE, TEST_DOC, count).await;
}

/// Send `count` ops from `conn_id` to a specific document.
async fn send_test_ops_to(relay: &mut Relay, conn_id: u64, space: &str, doc: &str, count: usize) {
    for i in 0..count {
        let (ops, meta) = make_edit(&format!("c{conn_id}-{i}"), &format!("op{i}"), "test");
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(sync::SyncOps {
                    space_id: space.into(),
                    document_id: doc.into(),
                    ops,
                    metadata: meta,
                    ..Default::default()
                }),
            })
            .await;
    }
}

/// Drain all available messages from a data channel.
fn drain_data(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<SyncEnvelope> {
    let mut msgs = Vec::new();
    while let Ok(bytes) = rx.try_recv() {
        msgs.push(decode_envelope(&bytes).expect("relay sent invalid protobuf"));
    }
    msgs
}

/// Author-inclusion probe: drain a DATA lane and assert a `Signal` frame was
/// among what arrived. Signals ride the data lane, so the probe must watch
/// `data` — a probe on `ctrl` could never see one.
///
/// This is the inverse of what it once asserted. The old probe required that a
/// sender NOT receive their own broadcast, which reads as an obvious economy
/// and is wrong here: this lane populates a client's replica, so withholding a
/// record leaves the author holding less of the space than everyone else, with
/// no later resume able to fill the gap. The economy that IS wanted — not
/// notifying someone about their own writes — is applied by the receiver.
/// Assert no `Signal` frame is pending on a DATA lane.
///
/// The only remaining reason to expect none is that the connection never
/// joined the signal stream. Authorship is NOT such a reason — see
/// [`assert_own_signal_delivered`].
fn assert_no_signal_pending(rx: &mut mpsc::Receiver<Vec<u8>>) {
    use prost::Message as _;
    while let Ok(bytes) = rx.try_recv() {
        let env = SyncEnvelope::decode(bytes.as_slice()).expect("decode envelope");
        assert!(
            !matches!(env.payload, Some(Payload::Signal(_))),
            "a connection that never sent SubscribeSignals must receive no signal frames"
        );
    }
}

fn assert_own_signal_delivered(rx: &mut mpsc::Receiver<Vec<u8>>) {
    use prost::Message as _;
    let mut saw_signal = false;
    while let Ok(bytes) = rx.try_recv() {
        let env = SyncEnvelope::decode(bytes.as_slice()).expect("decode envelope");
        if matches!(env.payload, Some(Payload::Signal(_))) {
            saw_signal = true;
        }
    }
    assert!(
        saw_signal,
        "the author's own connection must receive the record it authored — its \
         replica is otherwise missing a record no later catch-up will return"
    );
}

/// Drain all available messages from a ctrl channel.
fn drain_ctrl(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<SyncEnvelope> {
    drain_data(rx) // Same implementation, different semantic name.
}

/// Drain all available messages from the UNBOUNDED own-ack lane — where
/// eviction `StaleSubscriber` notices ride: the notice is the evicted
/// daemon's only re-subscribe signal, and on a bounded lane the very storm
/// that evicts would also drop it, stranding the daemon permanently.
fn drain_ack(rx: &mut mpsc::UnboundedReceiver<Vec<u8>>) -> Vec<SyncEnvelope> {
    let mut msgs = Vec::new();
    while let Ok(bytes) = rx.try_recv() {
        msgs.push(decode_envelope(&bytes).expect("relay sent invalid protobuf"));
    }
    msgs
}

/// Check whether any envelope in the list is a `StaleSubscriber`.
fn has_stale_notice(msgs: &[SyncEnvelope]) -> bool {
    msgs.iter()
        .any(|e| matches!(e.payload, Some(Payload::StaleSubscriber(_))))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_check() {
    let addr = start_relay().await;

    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    tokio::io::AsyncWriteExt::write_all(
        &mut stream,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();

    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut buf)
        .await
        .unwrap();

    let response = String::from_utf8_lossy(&buf);
    assert!(response.contains("200"), "expected 200 in: {response}");
    assert!(response.contains("ok"), "expected 'ok' body in: {response}");
}

#[tokio::test]
async fn test_handshake_roundtrip() {
    let addr = start_relay().await;
    let mut client = TestClient::connect(&addr).await;

    let (did, signing_key) = common::test_keypair();
    authorize_did(&addr, &did);
    let token = authenticate(&addr, &did, &signing_key).await;
    client
        .send_envelope(&handshake_envelope_with_token(
            "9f86d081-884c-4d65-8a2f-eaa0c55ad015",
            &token,
            "",
        ))
        .await;

    let ack = client.recv_envelope().await;
    match ack.payload {
        Some(Payload::HandshakeAck(ack)) => {
            assert_eq!(ack.relay_name, "test-relay");
            assert_eq!(ack.protocol_version_major, PROTOCOL_VERSION_MAJOR);
            assert_eq!(ack.protocol_version_minor, PROTOCOL_VERSION_MINOR);
        }
        other => panic!("expected HandshakeAck, got {other:?}"),
    }
}

#[tokio::test]
async fn test_subscribe_empty_doc() {
    let addr = start_relay().await;
    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;

    let catch_up = client
        .subscribe("f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "doc1")
        .await;
    let catch_up = catch_up.expect("empty doc should still send SyncOps");
    // Empty ops = protobuf default for bytes — signals "no operations applied yet."
    assert!(
        catch_up.ops.is_empty(),
        "empty doc should have zero-length ops"
    );
}

#[tokio::test]
async fn test_two_clients_converge() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    client_b
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    let (ops, metadata) = make_edit("alice", "hello", "add greeting");
    client_a
        .send_ops("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", ops, metadata)
        .await;

    let relayed = client_b.recv_ops().await;
    assert!(!relayed.ops.is_empty());
    assert_eq!(apply_ops(&relayed), "hello");
}

#[tokio::test]
async fn test_concurrent_edits_converge() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    client_b
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    let (ops_a, meta_a) = make_edit("alice", "AAA", "alice edit");
    let (ops_b, meta_b) = make_edit("bob", "BBB", "bob edit");

    client_a
        .send_ops(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8",
            "doc",
            ops_a.clone(),
            meta_a,
        )
        .await;
    client_b
        .send_ops(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8",
            "doc",
            ops_b.clone(),
            meta_b,
        )
        .await;

    let relayed_to_b = client_b.recv_ops().await;
    let relayed_to_a = client_a.recv_ops().await;

    // Merge each relay into the corresponding local doc.
    let mut doc_a = kutl_core::Document::new();
    doc_a.merge(&ops_a, &[]).unwrap();
    doc_a
        .merge(&relayed_to_a.ops, &relayed_to_a.metadata)
        .unwrap();

    let mut doc_b = kutl_core::Document::new();
    doc_b.merge(&ops_b, &[]).unwrap();
    doc_b
        .merge(&relayed_to_b.ops, &relayed_to_b.metadata)
        .unwrap();

    assert_eq!(doc_a.content(), doc_b.content());
    let content = doc_a.content();
    assert!(content.contains("AAA"), "missing AAA in: {content}");
    assert!(content.contains("BBB"), "missing BBB in: {content}");
}

// ---------------------------------------------------------------------------
// Blob tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_relay_forwards_blob() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "img.png")
        .await;

    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    client_b
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "img.png")
        .await;

    // Non-UTF-8 content. The client sends a forged hash; the relay forwards
    // the server-computed SHA-256 instead.
    let blob = vec![0xFF, 0xFE, 0x00, 0x01, 0x80, 0x90];
    let forged_hash = vec![0xAA; 32];
    let meta = sync::ChangeMetadata {
        timestamp: 1000,
        ..Default::default()
    };

    client_a
        .send_blob(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8",
            "img.png",
            blob.clone(),
            forged_hash,
            Some(meta),
        )
        .await;

    let relayed = client_b.recv_ops().await;
    assert_eq!(relayed.ops, blob);
    assert_eq!(relayed.content_hash, Sha256::digest(&blob).to_vec());
    assert_eq!(relayed.content_mode, i32::from(sync::ContentMode::Blob));
}

#[tokio::test]
async fn test_blob_catch_up() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc.pdf")
        .await;

    let blob = vec![0x25, 0x50, 0x44, 0x46]; // %PDF
    let forged_hash = vec![0xBB; 32];
    let meta = sync::ChangeMetadata {
        timestamp: 2000,
        ..Default::default()
    };

    client_a
        .send_blob(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8",
            "doc.pdf",
            blob.clone(),
            forged_hash,
            Some(meta),
        )
        .await;

    // Late joiner — poll until the relay has integrated the blob write. The
    // catch-up payload carries the server-computed hash, not the client's.
    let catch_up =
        wait_for_doc_catchup(&addr, "3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc.pdf").await;
    assert_eq!(catch_up.ops, blob);
    assert_eq!(catch_up.content_hash, Sha256::digest(&blob).to_vec());
    assert_eq!(catch_up.content_mode, i32::from(sync::ContentMode::Blob));
}

#[tokio::test]
async fn test_blob_lww_newer_wins() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "pic.jpg")
        .await;

    let mut client_b = TestClient::connect(&addr).await;
    client_b.handshake().await;
    client_b
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "pic.jpg")
        .await;

    // Client A sends blob with timestamp 1000. Client-supplied hashes are
    // forged here — the relay computes its own from the ops bytes.
    let blob_old = vec![0x01; 10];
    let forged_hash_old = vec![0x11; 32];
    client_a
        .send_blob(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8",
            "pic.jpg",
            blob_old.clone(),
            forged_hash_old,
            Some(sync::ChangeMetadata {
                timestamp: 1000,
                ..Default::default()
            }),
        )
        .await;

    // B receives the first blob.
    let first = client_b.recv_ops().await;
    assert_eq!(first.ops, blob_old);

    // Ensure B's write lands in a strictly later millisecond so the relay
    // stamps it with a newer server timestamp (the relay ignores the
    // client-supplied timestamp), making this a timestamp-ordering test rather
    // than a same-millisecond hash tiebreak.
    tokio::time::sleep(DISTINCT_TIMESTAMP_DELAY).await;

    // Client B sends the newer blob — strictly later server timestamp wins.
    let blob_new = vec![0x02; 10];
    let forged_hash_new = vec![0x22; 32];
    client_b
        .send_blob(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8",
            "pic.jpg",
            blob_new.clone(),
            forged_hash_new,
            Some(sync::ChangeMetadata {
                timestamp: 2000,
                ..Default::default()
            }),
        )
        .await;

    let relayed = client_a.recv_ops().await;
    assert_eq!(relayed.ops, blob_new);
    assert_eq!(relayed.content_hash, Sha256::digest(&blob_new).to_vec());

    // A late joiner should see the newer blob — wait specifically for it
    // so we don't race the LWW resolution.
    let catch_up = wait_for_doc_state(
        &addr,
        "3f49dbbf-e051-4b20-8c03-8923424fedf8",
        "pic.jpg",
        |s| s.ops == blob_new,
    )
    .await;
    assert_eq!(catch_up.ops, blob_new);
}

#[tokio::test]
async fn test_blob_ref_rejected() {
    let addr = start_relay().await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "big.bin")
        .await;

    // Manually build a BLOB_REF envelope.
    let envelope = sync::SyncEnvelope {
        payload: Some(Payload::SyncOps(sync::SyncOps {
            space_id: "3f49dbbf-e051-4b20-8c03-8923424fedf8".into(),
            document_id: "big.bin".into(),
            content_mode: i32::from(sync::ContentMode::BlobRef),
            blob_ref_url: "https://example.com/blob".into(),
            ..Default::default()
        })),
    };

    client.send_envelope(&envelope).await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::InvalidMessage));
    assert!(err.message.contains("BLOB_REF"));
}

#[tokio::test]
async fn test_oversized_blob_rejected() {
    // Regression: a frame slightly larger than ABSOLUTE_BLOB_MAX that slips
    // past the codec layer must be rejected by the application-layer
    // defense-in-depth check in `handle_blob_sync_ops`.
    //
    // Enforcement is based on `msg.ops.len()` (server-measured)
    // — never on the client-advertised `blob_size` field. We therefore
    // construct a frame with ACTUAL ops bytes just over the cap, not a
    // lied `blob_size`. Because ABSOLUTE_BLOB_MAX is also the WS codec cap,
    // this test exercises the codec-layer rejection in practice (the ws
    // connection closes). The application-layer recheck is covered by the
    // quota integration tests in `tests/quota.rs`.
    let addr = start_relay().await;

    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "big.bin")
        .await;

    let envelope = sync::SyncEnvelope {
        payload: Some(Payload::SyncOps(sync::SyncOps {
            space_id: "3f49dbbf-e051-4b20-8c03-8923424fedf8".into(),
            document_id: "big.bin".into(),
            content_mode: i32::from(sync::ContentMode::Blob),
            ops: vec![0xFF; kutl_proto::protocol::ABSOLUTE_BLOB_MAX + 1],
            content_hash: vec![0; 32],
            metadata: vec![sync::ChangeMetadata {
                timestamp: 1000,
                ..Default::default()
            }],
            ..Default::default()
        })),
    };

    // The codec rejects the frame and closes the connection. A subsequent
    // read should surface the closure rather than a parseable error frame.
    let _ = client
        .ws
        .send(tungstenite::Message::Binary(
            encode_envelope(&envelope).into(),
        ))
        .await;
    let result = client.ws.next().await;
    assert!(
        result.is_none() || result.as_ref().is_some_and(std::result::Result::is_err),
        "expected close or error after oversize frame, got {result:?}"
    );
}

#[tokio::test]
async fn test_ws_rejects_oversize_message() {
    let addr = start_relay().await;
    let url = format!("ws://{addr}/ws");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let oversize = vec![0u8; kutl_proto::protocol::ABSOLUTE_BLOB_MAX + 1];
    // The codec-layer cap should reject this without ever reaching application code.
    let _ = ws.send(tungstenite::Message::Binary(oversize.into())).await;

    // Server should close the connection (or the send itself errored after close).
    let result = ws.next().await;
    assert!(
        matches!(
            result,
            Some(Ok(tungstenite::Message::Close(_)) | Err(_)) | None
        ),
        "expected connection close, got {result:?}"
    );
}

#[tokio::test]
async fn test_late_joiner_catches_up() {
    let addr = start_relay().await;

    let mut client_a = TestClient::connect(&addr).await;
    client_a.handshake().await;
    client_a
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    let (ops, metadata) = make_edit("alice", "hello world", "initial content");
    client_a
        .send_ops("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", ops, metadata)
        .await;

    let catch_up = wait_for_doc_catchup(&addr, "3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc").await;
    assert_eq!(apply_ops(&catch_up), "hello world");
}

// ---------------------------------------------------------------------------
// Auth tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_auth_challenge_returns_nonce() {
    let (addr, _keys) = start_relay_with_auth().await;
    let (did, _) = common::test_keypair();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/auth/challenge"))
        .json(&serde_json::json!({"did": did}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let nonce = body["nonce"].as_str().expect("nonce field missing");
    let expires_at = body["expires_at"]
        .as_i64()
        .expect("expires_at field missing");

    // Nonce should decode to 32 bytes.
    let decoded = URL_SAFE_NO_PAD.decode(nonce).unwrap();
    assert_eq!(decoded.len(), 32);
    assert!(expires_at > 0);
}

#[tokio::test]
async fn test_auth_full_flow() {
    let (addr, _keys) = start_relay_with_auth().await;
    let (did, signing_key) = common::test_keypair();

    let token = authenticate(&addr, &did, &signing_key).await;
    assert!(token.starts_with("kutl_"), "token should have kutl_ prefix");
    assert!(token.len() > 10, "token should be non-trivial");
}

#[tokio::test]
async fn test_auth_bad_signature_rejected() {
    let (addr, _keys) = start_relay_with_auth().await;
    let (did, _) = common::test_keypair();
    let (_, wrong_key) = common::test_keypair();

    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    // Get a challenge.
    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/challenge"))
        .json(&serde_json::json!({"did": did}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let nonce = resp["nonce"].as_str().unwrap();

    // Sign with the wrong key.
    let nonce_bytes = URL_SAFE_NO_PAD.decode(nonce).unwrap();
    let signature = wrong_key.sign(&nonce_bytes);
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    let resp = client
        .post(format!("{base_url}/auth/verify"))
        .json(&serde_json::json!({
            "did": did,
            "nonce": nonce,
            "signature": sig_b64,
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("invalid signature")
    );
}

#[tokio::test]
async fn test_authenticated_handshake_accepted() {
    let (addr, _keys) = start_relay_with_auth().await;
    let (did, signing_key) = common::test_keypair();

    let token = authenticate(&addr, &did, &signing_key).await;

    // Connect WS with the token.
    let url = format!("ws://{addr}/ws");
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let mut client = TestClient {
        ws,
        addr: addr.clone(),
    };

    client
        .send_envelope(&handshake_envelope_with_token(
            "c0b34734-6368-49e6-8e1e-10d598013626",
            &token,
            "",
        ))
        .await;

    let ack = client.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "expected HandshakeAck, got {ack:?}"
    );
}

#[tokio::test]
async fn test_unauthenticated_handshake_rejected() {
    let (addr, _keys) = start_relay_with_auth().await;

    let mut client = TestClient::connect(&addr).await;

    // Send handshake without a token.
    client
        .send_envelope(&handshake_envelope("c85c7bfd-1f8b-4477-8a2f-ceb5b178000e"))
        .await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::AuthFailed));
}

#[tokio::test]
async fn test_invalid_token_handshake_rejected() {
    let (addr, _keys) = start_relay_with_auth().await;

    let mut client = TestClient::connect(&addr).await;

    // Send handshake with a bogus token — should be rejected.
    client
        .send_envelope(&handshake_envelope_with_token(
            "8583fb6e-4d19-4d20-82c6-e8bccccc293a",
            "kutl_not-a-real-token",
            "",
        ))
        .await;

    let err = client.recv_error().await;
    assert_eq!(err.code, i32::from(sync::ErrorCode::AuthFailed));
}

/// Test eviction using standalone relay (no TCP/WebSocket overhead).
///
/// Uses a bounded channel with capacity 2 so we can trigger eviction
/// deterministically without worrying about TCP buffer sizes.
#[tokio::test]
async fn test_stale_subscriber_on_channel_full() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow = connect_client(
        &mut relay,
        2,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Send ops from writer. Don't drain slow — let it fill up.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Data channel: at most SLOW_CHANNEL_CAPACITY SyncOps.
    let data_messages = drain_data(&mut slow.data_rx);
    let sync_ops_count = data_messages
        .iter()
        .filter(|e| matches!(e.payload, Some(Payload::SyncOps(_))))
        .count();

    assert!(
        sync_ops_count <= SLOW_CHANNEL_CAPACITY,
        "expected at most {SLOW_CHANNEL_CAPACITY} SyncOps (channel capacity), got {sync_ops_count}"
    );
    assert_eq!(
        sync_ops_count,
        data_messages.len(),
        "data channel should only contain SyncOps"
    );

    // The stale notice rides the UNBOUNDED own-ack lane (never droppable).
    let ack_messages = drain_ack(&mut slow.ack_rx);
    assert!(
        has_stale_notice(&ack_messages),
        "expected StaleSubscriber on the own-ack lane"
    );

    // Verify eviction: one more op should NOT reach the slow client.
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    assert!(
        slow.data_rx.try_recv().is_err(),
        "slow client received message after eviction — eviction did not work"
    );
}

/// Test that a client can resubscribe after eviction and get full catch-up.
#[tokio::test]
async fn test_resubscribe_after_eviction() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow = connect_client(
        &mut relay,
        2,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Flood to trigger eviction.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Drain slow client's buffered messages.
    drain_data(&mut slow.data_rx);

    // Resubscribe — should get catch-up with all ops.
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    let catch_up_bytes = slow
        .data_rx
        .try_recv()
        .expect("resubscribe should produce catch-up");
    let catch_up_env = decode_envelope(&catch_up_bytes).unwrap();

    let catch_up_ops = match catch_up_env.payload {
        Some(Payload::SyncOps(ops)) => ops,
        other => panic!("expected SyncOps catch-up, got {other:?}"),
    };

    let mut doc = kutl_core::Document::new();
    doc.merge(&catch_up_ops.ops, &catch_up_ops.metadata)
        .unwrap();
    assert!(!doc.content().is_empty(), "catch-up should have content");

    let content = doc.content();
    assert!(content.contains("op0"), "catch-up missing op0");
    assert!(content.contains("op9"), "catch-up missing op9");
}

// ---------------------------------------------------------------------------
// Degradation mode tests — misbehaving / slow / disconnecting clients
// ---------------------------------------------------------------------------

/// Client disconnects mid-stream, reconnects, resubscribes, and gets catch-up.
#[tokio::test]
async fn test_disconnect_mid_stream_recovery() {
    let addr = start_relay().await;

    // Writer establishes content.
    let mut writer = TestClient::connect(&addr).await;
    writer.handshake().await;
    writer
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;

    let (ops, metadata) = make_edit("alice", "before-disconnect", "initial");
    writer
        .send_ops("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", ops, metadata)
        .await;

    // Reader subscribes, gets catch-up, then disconnects abruptly.
    let mut reader = TestClient::connect(&addr).await;
    reader.handshake().await;
    let catch_up = reader
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;
    assert!(catch_up.is_some(), "should get initial catch-up");

    // Drop the reader — simulates abrupt disconnect.
    drop(reader);

    // Writer sends more content while reader is gone.
    let (ops2, metadata2) = make_edit("alice2", "after-disconnect", "more content");
    writer
        .send_ops(
            "3f49dbbf-e051-4b20-8c03-8923424fedf8",
            "doc",
            ops2,
            metadata2,
        )
        .await;

    // Reconnected reader should see both rounds — poll until merged.
    let catch_up2 = wait_for_doc_state(&addr, "3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", |s| {
        let content = apply_ops(s);
        content.contains("before-disconnect") && content.contains("after-disconnect")
    })
    .await;

    let content = apply_ops(&catch_up2);
    assert!(content.contains("before-disconnect"));
    assert!(content.contains("after-disconnect"));
}

/// Write-only client (sends ops, never reads) gets evicted. Uses standalone
/// relay with small channel to avoid timing sensitivity.
#[tokio::test]
async fn test_write_only_client_evicted() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut wo = connect_client(
        &mut relay,
        2,
        "write-only",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, wo.conn_id).await;

    // Write-only client sends ops (acts as writer too).
    send_test_ops(&mut relay, wo.conn_id, FLOOD_OPS).await;

    // Writer floods to fill write-only client's channel.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // The write-only client's stale notice rides the own-ack lane.
    let ack_messages = drain_ack(&mut wo.ack_rx);
    assert!(
        has_stale_notice(&ack_messages),
        "write-only client should have been evicted with StaleSubscriber"
    );

    // Verify the write-only client's ops were still accepted by the relay.
    drain_data(&mut wo.data_rx);

    // A new subscriber should see all content (from both writer and write-only).
    let mut verifier = connect_client(
        &mut relay,
        3,
        "verifier",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, verifier.conn_id).await;

    let catch_up_bytes = verifier
        .data_rx
        .try_recv()
        .expect("new subscriber should get catch-up");
    let env = decode_envelope(&catch_up_bytes).unwrap();
    let ops = match env.payload {
        Some(Payload::SyncOps(ops)) => ops,
        other => panic!("expected SyncOps, got {other:?}"),
    };
    let content = apply_ops(&ops);
    // Both clients sent FLOOD_OPS ops with text "op0".."op9". CRDT merges all
    // concurrent inserts, so the catch-up must contain text from both.
    assert!(
        content.contains("op0"),
        "relay should have merged ops: {content}"
    );
    // Count how many "op" substrings appear — should be 2 * FLOOD_OPS.
    let op_count = content.matches("op").count();
    assert!(
        op_count >= 2 * FLOOD_OPS,
        "expected ops from both clients ({} total), got {op_count}: {content}",
        2 * FLOOD_OPS,
    );
}

/// Sending ops to a document without subscribing first returns `NotFound`.
#[tokio::test]
async fn test_ops_without_subscribe_returns_not_found() {
    let addr = start_relay().await;
    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;

    // Send ops to a document nobody has subscribed to (no doc slot exists).
    let (ops, metadata) = make_edit("rogue", "should-fail", "no subscribe");
    client
        .send_ops("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc", ops, metadata)
        .await;

    let err = client.recv_error().await;
    assert_eq!(
        err.code,
        i32::from(sync::ErrorCode::NotFound),
        "expected NotFound error"
    );
}

/// In auth mode, subscribing without handshake (no token) is rejected.
#[tokio::test]
async fn test_subscribe_without_auth_rejected() {
    let (addr, _keys) = start_relay_with_auth().await;
    let mut client = TestClient::connect(&addr).await;

    // Send handshake without a token — should fail.
    client
        .send_envelope(&handshake_envelope("86d6a019-6c07-471d-850c-0cdfb2979773"))
        .await;

    let err = client.recv_error().await;
    assert_eq!(
        err.code,
        i32::from(sync::ErrorCode::AuthFailed),
        "expected AuthFailed"
    );
}

/// Sending malformed (non-protobuf) binary data should not crash the relay.
#[tokio::test]
async fn test_malformed_binary_does_not_crash_relay() {
    let addr = start_relay().await;

    // Send garbage bytes.
    let url = format!("ws://{addr}/ws");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws.send(tungstenite::Message::Binary(
        vec![0xFF, 0xFE, 0x00, 0x01].into(),
    ))
    .await
    .unwrap();

    // Relay should still be alive — a different client can connect and work.
    // The connect+handshake+subscribe sequence is itself the liveness probe;
    // if the relay died from the garbage frame, connect would fail.
    let mut client = TestClient::connect(&addr).await;
    client.handshake().await;
    let catch_up = client
        .subscribe("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc")
        .await;
    let catch_up = catch_up.expect("empty doc should still send SyncOps");
    assert!(
        catch_up.ops.is_empty(),
        "empty doc should have zero-length ops"
    );
}

/// When a slow client is evicted, healthy clients on the same document
/// continue receiving relays without interruption.
#[tokio::test]
async fn test_healthy_client_unaffected_by_stale_neighbor() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut healthy = connect_client(
        &mut relay,
        2,
        "healthy",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow = connect_client(
        &mut relay,
        3,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, healthy.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Drain the empty-doc catch-up SyncOps that subscribe sends.
    let _ = healthy.data_rx.try_recv();
    let _ = slow.data_rx.try_recv();

    // Writer sends FLOOD_OPS. Slow client's channel fills → eviction.
    // Healthy client should receive all ops.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Healthy client should have received all relays.
    let healthy_msgs = drain_data(&mut healthy.data_rx);
    let healthy_ops_count = healthy_msgs
        .iter()
        .filter(|e| matches!(e.payload, Some(Payload::SyncOps(_))))
        .count();
    assert_eq!(
        healthy_ops_count, FLOOD_OPS,
        "healthy client should receive all {FLOOD_OPS} relays, got {healthy_ops_count}"
    );

    // Slow client should have been evicted (notice on the own-ack lane).
    let ack_messages = drain_ack(&mut slow.ack_rx);
    assert!(
        has_stale_notice(&ack_messages),
        "slow client should have been evicted"
    );

    // Slow client got at most SLOW_CHANNEL_CAPACITY data messages.
    let slow_data = drain_data(&mut slow.data_rx);
    assert!(
        slow_data.len() <= SLOW_CHANNEL_CAPACITY,
        "slow client should have at most {SLOW_CHANNEL_CAPACITY} messages"
    );

    // Healthy client should still receive relays AFTER slow client was evicted.
    send_test_ops(&mut relay, writer.conn_id, 1).await;

    let post_bytes = healthy
        .data_rx
        .try_recv()
        .expect("healthy client should still receive relays after neighbor's eviction");
    let post_env = decode_envelope(&post_bytes).unwrap();
    assert!(
        matches!(post_env.payload, Some(Payload::SyncOps(_))),
        "expected SyncOps"
    );
}

/// Eviction from one document does not affect subscriptions to other documents
/// on the same connection.
#[tokio::test]
async fn test_eviction_scoped_to_document() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut multi = connect_client(
        &mut relay,
        2,
        "multi",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    // Both subscribe to doc-a AND doc-b.
    for doc in [
        "00000000-0000-0000-0000-00000000000a",
        "00000000-0000-0000-0000-00000000000b",
    ] {
        subscribe_to(&mut relay, writer.conn_id, TEST_SPACE, doc).await;
        subscribe_to(&mut relay, multi.conn_id, TEST_SPACE, doc).await;
    }

    // Flood doc-a to fill multi's channel → eviction from doc-a.
    send_test_ops_to(
        &mut relay,
        writer.conn_id,
        TEST_SPACE,
        "00000000-0000-0000-0000-00000000000a",
        FLOOD_OPS,
    )
    .await;

    // Multi client was evicted from doc-a. Drain its channels.
    drain_data(&mut multi.data_rx);

    let ack_messages = drain_ack(&mut multi.ack_rx);
    let evicted_from_a = ack_messages.iter().any(|e| {
        matches!(&e.payload, Some(Payload::StaleSubscriber(stale)) if stale.document_id == "00000000-0000-0000-0000-00000000000a")
    });
    assert!(evicted_from_a, "multi client should be evicted from doc-a");

    // Write to doc-b — multi client should still receive it.
    send_test_ops_to(
        &mut relay,
        writer.conn_id,
        TEST_SPACE,
        "00000000-0000-0000-0000-00000000000b",
        1,
    )
    .await;

    let doc_b_bytes = multi
        .data_rx
        .try_recv()
        .expect("multi client should still receive doc-b relays after doc-a eviction");
    let env = decode_envelope(&doc_b_bytes).unwrap();
    match env.payload {
        Some(Payload::SyncOps(ref ops)) => {
            assert_eq!(
                ops.document_id, "00000000-0000-0000-0000-00000000000b",
                "relay should be for doc-b"
            );
        }
        other => panic!("expected SyncOps for doc-b, got {other:?}"),
    }
}

/// Ops sent by a client that is being evicted are still merged into the relay's
/// authoritative document — eviction only stops outbound relays.
#[tokio::test]
async fn test_ops_from_evicted_client_still_merged() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut stale = connect_client(
        &mut relay,
        1,
        "stale",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let filler = connect_client(
        &mut relay,
        2,
        "filler",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, stale.conn_id).await;
    subscribe_to_doc(&mut relay, filler.conn_id).await;

    // Stale client sends ops BEFORE being evicted.
    let (ops, meta) = make_edit("stale-pre", "PRE-EVICTION", "before eviction");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: stale.conn_id,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata: meta,
                ..Default::default()
            }),
        })
        .await;

    // Filler floods to trigger stale client's eviction.
    send_test_ops(&mut relay, filler.conn_id, FLOOD_OPS).await;

    // Stale client sends ops AFTER being evicted — relay should still merge them.
    let (ops, meta) = make_edit("stale-post", "POST-EVICTION", "after eviction");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: stale.conn_id,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata: meta,
                ..Default::default()
            }),
        })
        .await;

    // Drain stale channels.
    drain_data(&mut stale.data_rx);
    drain_ctrl(&mut stale.ctrl_rx);

    // Verifier subscribes — should see ALL content including both stale client ops.
    let mut verifier = connect_client(
        &mut relay,
        3,
        "verifier",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, verifier.conn_id).await;

    let catch_up_bytes = verifier
        .data_rx
        .try_recv()
        .expect("verifier should get catch-up");
    let env = decode_envelope(&catch_up_bytes).unwrap();
    let catch_up = match env.payload {
        Some(Payload::SyncOps(ops)) => ops,
        other => panic!("expected SyncOps catch-up, got {other:?}"),
    };

    let content = apply_ops(&catch_up);
    assert!(
        content.contains("PRE-EVICTION"),
        "relay should have stale client's pre-eviction ops: {content}"
    );
    assert!(
        content.contains("POST-EVICTION"),
        "relay should have stale client's post-eviction ops: {content}"
    );
    assert!(
        content.contains("op0"),
        "relay should have filler ops: {content}"
    );
}

/// All subscribers are slow → all get evicted → relay still serves a new client.
///
/// This is the "100% bad clients" scenario at the channel level. Every
/// subscriber has `SLOW_CHANNEL_CAPACITY`, all get evicted, then a brand-new
/// client connects and gets full service.
#[tokio::test]
async fn test_all_subscribers_slow_relay_serves_new_client() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow_clients = Vec::new();
    for i in 0..ALL_SLOW_SUBSCRIBER_COUNT {
        let conn_id = (i + 2) as u64;
        slow_clients.push(
            connect_client(
                &mut relay,
                conn_id,
                &format!("slow-{i}"),
                SLOW_CHANNEL_CAPACITY,
                TEST_CTRL_CAPACITY,
            )
            .await,
        );
    }

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    for c in &slow_clients {
        subscribe_to_doc(&mut relay, c.conn_id).await;
    }

    // Flood — don't drain any slow client.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // Each slow client should have a StaleSubscriber notice (all evicted),
    // delivered on the own-ack lane.
    for c in &mut slow_clients {
        let ack = drain_ack(&mut c.ack_rx);
        assert!(
            has_stale_notice(&ack),
            "slow client {} should have been evicted",
            c.conn_id,
        );
    }

    // One more op — should reach nobody (subscriber list cleared).
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    for c in &mut slow_clients {
        drain_data(&mut c.data_rx);
        assert!(
            c.data_rx.try_recv().is_err(),
            "slow client {} should not receive post-eviction op",
            c.conn_id,
        );
    }

    // New fast client connects, subscribes, gets catch-up.
    let new_conn_id = (ALL_SLOW_SUBSCRIBER_COUNT + 2) as u64;
    let mut newcomer = connect_client(
        &mut relay,
        new_conn_id,
        "newcomer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, newcomer.conn_id).await;

    let catch_up = drain_data(&mut newcomer.data_rx);
    assert!(
        !catch_up.is_empty(),
        "newcomer should receive catch-up SyncOps"
    );
    assert!(
        catch_up
            .iter()
            .any(|e| matches!(e.payload, Some(Payload::SyncOps(_)))),
        "catch-up should contain SyncOps"
    );

    // Writer sends another op — newcomer receives it.
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    let relay = newcomer
        .data_rx
        .try_recv()
        .expect("newcomer should receive relayed op");
    let env = decode_envelope(&relay).unwrap();
    assert!(
        matches!(env.payload, Some(Payload::SyncOps(_))),
        "expected SyncOps relay"
    );
}

/// Disconnect after eviction cleans up all internal state — no dangling
/// entries in connections, authenticated, `subscribe_history`, or subscriber maps.
///
/// Proven by: reusing the same `conn_id` after disconnect succeeds.
#[tokio::test]
async fn test_disconnect_after_eviction_cleans_up() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut slow = connect_client(
        &mut relay,
        2,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Flood → eviction.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    let ack = drain_ack(&mut slow.ack_rx);
    assert!(has_stale_notice(&ack), "slow client should be evicted");

    // Disconnect the evicted slow client.
    relay
        .process_command(RelayCommand::Disconnect {
            conn_id: slow.conn_id,
        })
        .await;

    // Reuse the same conn_id with fresh channels.
    let mut reused = connect_client(
        &mut relay,
        2,
        "reused",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, reused.conn_id).await;

    // Should get catch-up.
    let catch_up = drain_data(&mut reused.data_rx);
    assert!(
        catch_up
            .iter()
            .any(|e| matches!(e.payload, Some(Payload::SyncOps(_)))),
        "reused conn should receive catch-up"
    );

    // Writer sends another op — reused conn receives relay.
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    let relay_bytes = reused
        .data_rx
        .try_recv()
        .expect("reused conn should receive relayed op");
    let env = decode_envelope(&relay_bytes).unwrap();
    assert!(
        matches!(env.payload, Some(Payload::SyncOps(_))),
        "expected SyncOps relay"
    );

    // Second round: disconnect and reuse again to prove cleanup is thorough.
    relay
        .process_command(RelayCommand::Disconnect {
            conn_id: reused.conn_id,
        })
        .await;

    let mut reused2 = connect_client(
        &mut relay,
        2,
        "reused2",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, reused2.conn_id).await;

    let catch_up2 = drain_data(&mut reused2.data_rx);
    assert!(
        catch_up2
            .iter()
            .any(|e| matches!(e.payload, Some(Payload::SyncOps(_)))),
        "second reuse should also receive catch-up"
    );
}

/// Ctrl channel full — the eviction notice is DELIVERED anyway.
///
/// The data-lane eviction's stale notice rides the UNBOUNDED own-ack lane, so
/// a completely FULL ctrl lane cannot drop it (on a bounded ctrl lane
/// the very storm that evicts would also drop the only re-subscribe signal —
/// a permanent strand). Proves ctrl backpressure neither crashes the
/// relay, nor prevents eviction, nor loses the notice.
#[tokio::test]
async fn test_ctrl_channel_full_relay_continues() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    // Slow client with a TINY (1-slot) ctrl lane.
    let mut slow = connect_client(
        &mut relay,
        2,
        "slow",
        SLOW_CHANNEL_CAPACITY,
        TINY_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, slow.conn_id).await;

    // Fill the slow client's 1-slot ctrl lane with a lifecycle broadcast (a
    // register in the space reaches every subscriber). Left undrained, the slot
    // is now occupied so the later stale-notice try_send hits a FULL ctrl.
    register_doc(&mut relay, writer.conn_id, "filler.md").await;
    assert_eq!(
        slow.ctrl_rx.len(),
        1,
        "the register broadcast fills the slow client's single ctrl slot"
    );

    // Flood → data channel fills → eviction → ctrl try_send fails silently.
    send_test_ops(&mut relay, writer.conn_id, FLOOD_OPS).await;

    // The ctrl slot still holds exactly the one register broadcast — the
    // stale notice never touches ctrl…
    let ctrl = drain_ctrl(&mut slow.ctrl_rx);
    assert_eq!(
        ctrl.len(),
        1,
        "ctrl holds exactly the register broadcast (the notice does not ride ctrl)"
    );
    assert!(
        matches!(ctrl[0].payload, Some(Payload::RegisterDocument(_))),
        "the only ctrl message should be the register broadcast"
    );
    // …because it was DELIVERED on the unbounded own-ack lane despite the
    // full ctrl (the whole point of the lane move).
    let ack = drain_ack(&mut slow.ack_rx);
    assert!(
        has_stale_notice(&ack),
        "the eviction notice must arrive on the own-ack lane even with ctrl full"
    );

    // Eviction still happened: no more data after draining.
    drain_data(&mut slow.data_rx);
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    assert!(
        slow.data_rx.try_recv().is_err(),
        "slow client should not receive post-eviction op (eviction happened despite ctrl being full)"
    );

    // Relay is still functional: new subscriber gets full service.
    let mut newcomer = connect_client(
        &mut relay,
        3,
        "newcomer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, newcomer.conn_id).await;

    let catch_up = drain_data(&mut newcomer.data_rx);
    assert!(
        catch_up
            .iter()
            .any(|e| matches!(e.payload, Some(Payload::SyncOps(_)))),
        "newcomer should receive catch-up"
    );

    send_test_ops(&mut relay, writer.conn_id, 1).await;
    let relay_bytes = newcomer
        .data_rx
        .try_recv()
        .expect("newcomer should receive relayed op");
    let env = decode_envelope(&relay_bytes).unwrap();
    assert!(
        matches!(env.payload, Some(Payload::SyncOps(_))),
        "expected SyncOps relay"
    );
}

// ---------------------------------------------------------------------------
// Signal tests (relay-only, standalone relay)
// ---------------------------------------------------------------------------

/// Build a `Signal` message with a flag payload for use in relay commands.
fn make_flag_signal(
    space_id: &str,
    document_id: &str,
    author_did: &str,
    kind: i32,
    audience: i32,
    message: &str,
    target_did: &str,
) -> sync::Signal {
    sync::Signal {
        id: String::new(),
        space_id: space_id.to_owned(),
        document_id: Some(document_id.to_owned()),
        author_did: author_did.to_owned(),
        timestamp: 1_700_000_000_000,
        // The deprecated audience pair is the shape under test: these fixtures
        // stand in for records already on disk.
        #[allow(deprecated)]
        payload: Some(sync::signal::Payload::Flag(sync::FlagPayload {
            kind,
            audience_type: audience,
            target_did: if target_did.is_empty() {
                None
            } else {
                Some(target_did.to_owned())
            },
            message: message.to_owned(),
            audience: None,
            anchor_text: None,
        })),
        ..Default::default()
    }
}

/// Drain a `Signal` broadcast from a subscriber's DATA channel. Tolerates a
/// leading `SubscribeStatus` defensively, though that now rides the own-ack
/// lane and so should not appear here.
///
/// The data lane, not `ctrl`: live signals ride the
/// bulk lane alongside document backfill and presence, so that they yield to
/// control traffic instead of preempting it.
async fn recv_signal(data_rx: &mut mpsc::Receiver<Vec<u8>>) -> sync::Signal {
    // SKIPS other envelopes rather than rejecting them. On `ctrl` this helper
    // could reasonably demand the next frame be the Signal; on `data` it shares
    // the lane with document traffic, so a `SyncOps` sitting ahead of the
    // broadcast is ordinary rather than a fault. Bounded so a signal that never
    // arrives still fails as a test rather than hanging.
    const MAX_SKIPPED_FRAMES: usize = 16;
    for _ in 0..MAX_SKIPPED_FRAMES {
        let bytes = tokio::time::timeout(Duration::from_secs(1), data_rx.recv())
            .await
            .expect("timeout waiting for signal")
            .expect("channel closed");
        let envelope = decode_envelope(&bytes).unwrap();
        if let Some(Payload::Signal(s)) = envelope.payload {
            return s;
        }
    }
    panic!("no Signal within {MAX_SKIPPED_FRAMES} frames on the data lane");
}

#[tokio::test]
async fn test_signal_flag_delivered() {
    let mut relay = Relay::new_standalone(test_relay_config());

    // Conn 1 authenticates AS the signal author so `authoritative_author_did`
    // leaves the emitted `author_did` == "did:key:zAlice".
    let mut a = connect_client(
        &mut relay,
        1,
        "did:key:zAlice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut b = connect_client(
        &mut relay,
        2,
        "did:key:zBob",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;
    // Signals are their own stream: a document subscription does not put
    // a connection in the space's signal recipient set.
    subscribe_to_signals(&mut relay, 1, TEST_SPACE).await;
    subscribe_to_signals(&mut relay, 2, TEST_SPACE).await;
    common::drain_subscribe_status(&mut a.ack_rx).await;
    common::drain_subscribe_status(&mut b.ack_rx).await;

    // A sends flag signal with SPACE audience.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::ReviewRequested),
        i32::from(sync::AudienceType::Space),
        "please review",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // B should receive it via ctrl channel.
    let signal = recv_signal(&mut b.data_rx).await;
    assert_eq!(signal.space_id, TEST_SPACE);
    assert_eq!(signal.document_id.as_deref(), Some(TEST_DOC));
    assert_eq!(signal.author_did, "did:key:zAlice");
    match &signal.payload {
        Some(sync::signal::Payload::Flag(f)) => {
            assert_eq!(f.kind, i32::from(sync::FlagKind::ReviewRequested));
            assert_eq!(f.message, "please review");
        }
        other => panic!("expected Flag payload, got {other:?}"),
    }

    // A authored it and must hold it too — probed on the DATA lane, where
    // signals actually ride.
    assert_own_signal_delivered(&mut a.data_rx);
}

/// A connection subscribed to a DOCUMENT but never to SIGNALS receives no
/// signal frames — not even for a record it authored itself.
///
/// `SubscribeSignals` is how a connection joins the signal stream, and space
/// recipients are drawn from that set alone rather than from a union with
/// document subscribers. This test was previously named for sender exclusion
/// and asserted that its author received nothing — which it did, but for this
/// reason rather than that one, so it passed whether or not the suppression it
/// claimed to cover existed at all. Named for what its setup actually builds,
/// it pins something true.
#[tokio::test]
async fn test_document_only_subscriber_receives_no_signal() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    // Deliberately NOT `SubscribeSignals` — a document subscription only.
    subscribe_to_doc(&mut relay, 1).await;
    common::drain_subscribe_status(&mut a.ack_rx).await;

    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::Completed),
        i32::from(sync::AudienceType::Space),
        "done",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    assert_no_signal_pending(&mut a.data_rx);
}

#[tokio::test]
async fn test_signal_unspecified_audience_rejected() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;
    common::drain_subscribe_status(&mut a.ack_rx).await;

    // Send with UNSPECIFIED audience.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::Blocked),
        i32::from(sync::AudienceType::Unspecified),
        "help",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // Should get an error back on the own-ack lane.
    let bytes = a.ack_rx.try_recv().expect("should receive error");
    let envelope = decode_envelope(&bytes).unwrap();
    match envelope.payload {
        Some(Payload::Error(e)) => {
            assert_eq!(e.code, i32::from(sync::ErrorCode::InvalidMessage));
            assert!(e.message.contains("audience"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

/// A merge-reject error frame names the document it rejects, so a client can
/// target recovery (rewind + resubscribe) at the poisoned doc. An unscoped
/// reject is unactionable: the client's scoped-error handling falls through
/// and the rejection degrades into a log line while the client keeps
/// re-sending the same unmergeable history.
#[tokio::test]
async fn test_merge_reject_error_names_the_document() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;
    common::drain_subscribe_status(&mut a.ack_rx).await;

    // A delta whose base the relay never saw: two sequential edits, encoded
    // only from the post-first-edit frontier. The relay's fresh document
    // lacks the first edit, so the merge fails with an unknown base.
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent("a").unwrap();
    doc.edit(agent, "a", "", kutl_core::Boundary::Explicit, |ctx| {
        ctx.insert(0, "first")
    })
    .unwrap();
    let mid = doc.local_version();
    doc.edit(agent, "a", "", kutl_core::Boundary::Explicit, |ctx| {
        ctx.insert(5, "second")
    })
    .unwrap();
    let orphan_ops = doc.encode_since(&mid);
    let orphan_meta = doc.changes_since(&mid);

    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: orphan_ops,
                metadata: orphan_meta,
                ..Default::default()
            }),
        })
        .await;

    let bytes = a.ack_rx.try_recv().expect("should receive error");
    let envelope = decode_envelope(&bytes).unwrap();
    match envelope.payload {
        Some(Payload::Error(e)) => {
            assert_eq!(e.code, i32::from(sync::ErrorCode::InvalidMessage));
            assert_eq!(e.space_id, TEST_SPACE, "reject names the space");
            assert_eq!(e.document_id, TEST_DOC, "reject names the document");
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_signal_participant_needs_target() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;
    common::drain_subscribe_status(&mut a.ack_rx).await;

    // Send PARTICIPANT without target_did.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::Question),
        i32::from(sync::AudienceType::Participant),
        "need input",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // Should get an error back on the own-ack lane.
    let bytes = a.ack_rx.try_recv().expect("should receive error");
    let envelope = decode_envelope(&bytes).unwrap();
    match envelope.payload {
        Some(Payload::Error(e)) => {
            assert_eq!(e.code, i32::from(sync::ErrorCode::InvalidMessage));
            assert!(e.message.contains("target_did"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_signal_flag_cross_document() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut b = connect_client(
        &mut relay,
        2,
        "b",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    // A subscribes to doc1, B subscribes to doc2 — same space. Both also join
    // the space's signal stream, which is what decides signal delivery.
    subscribe_to(&mut relay, 1, TEST_SPACE, TEST_DOC_1).await;
    subscribe_to(&mut relay, 2, TEST_SPACE, TEST_DOC_2).await;
    subscribe_to_signals(&mut relay, 1, TEST_SPACE).await;
    subscribe_to_signals(&mut relay, 2, TEST_SPACE).await;
    common::drain_subscribe_status(&mut a.ack_rx).await;
    common::drain_subscribe_status(&mut b.ack_rx).await;

    // A sends flag signal scoped to the space.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC_1,
        "did:key:zAlice",
        i32::from(sync::FlagKind::ReviewRequested),
        i32::from(sync::AudienceType::Space),
        "review doc1",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // B receives it even though subscribed to a different doc.
    let signal = recv_signal(&mut b.data_rx).await;
    assert_eq!(signal.document_id.as_deref(), Some(TEST_DOC_1));
    match &signal.payload {
        Some(sync::signal::Payload::Flag(f)) => {
            assert_eq!(f.message, "review doc1");
        }
        other => panic!("expected Flag payload, got {other:?}"),
    }

    // A receives the record it authored — probed on the DATA lane.
    assert_own_signal_delivered(&mut a.data_rx);
}

#[tokio::test]
async fn test_space_audience_reaches_every_signal_subscriber() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut a = connect_client(
        &mut relay,
        1,
        "a",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut b = connect_client(
        &mut relay,
        2,
        "b",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;
    // Signals are their own stream: a document subscription does not put
    // a connection in the space's signal recipient set.
    subscribe_to_signals(&mut relay, 1, TEST_SPACE).await;
    subscribe_to_signals(&mut relay, 2, TEST_SPACE).await;
    common::drain_subscribe_status(&mut a.ack_rx).await;
    common::drain_subscribe_status(&mut b.ack_rx).await;

    // A space-audience flag reaches every SIGNAL subscriber in the space,
    // whatever documents they happen to be subscribed to. What is pinned
    // here is that fan-out itself: recipients are decided by signal-stream
    // membership alone, with no sentinel document involved.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zAlice",
        i32::from(sync::FlagKind::Info),
        i32::from(sync::AudienceType::Space),
        "space-level notice",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // B receives it because audience is SPACE — fan-out to every
    // subscriber in the space, independent of doc subscription.
    let signal = recv_signal(&mut b.data_rx).await;
    assert_eq!(signal.document_id.as_deref(), Some(TEST_DOC));
    match &signal.payload {
        Some(sync::signal::Payload::Flag(f)) => {
            assert_eq!(f.kind, i32::from(sync::FlagKind::Info));
            assert_eq!(f.message, "space-level notice");
        }
        other => panic!("expected Flag payload, got {other:?}"),
    }

    // A receives the record it authored — probed on the DATA lane.
    assert_own_signal_delivered(&mut a.data_rx);
}

/// Authorized-keys file shared by all standalone relay auth tests.
/// Populated per-test via `connect_preauth` path (`authorized_keys.rs`
/// re-reads on every check).
static AUTH_KEYS: std::sync::LazyLock<tempfile::NamedTempFile> = std::sync::LazyLock::new(|| {
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    writeln!(f, "did:key:zAlice").unwrap();
    writeln!(f, "did:key:zBob").unwrap();
    writeln!(f, "did:key:zCharlie").unwrap();
    f
});

/// A `test_relay_config` copy pointed at the shared [`AUTH_KEYS`] file.
fn test_relay_config_with_auth() -> RelayConfig {
    let mut config = test_relay_config();
    config.authorized_keys_file = Some(AUTH_KEYS.path().to_path_buf());
    config
}

/// Connect a client without running the handshake, then inject an
/// authenticated identity. Needed for actor-level auth tests — the real
/// handshake path requires a live HTTP auth flow.
async fn connect_preauth(
    relay: &mut Relay,
    conn_id: u64,
    identity: &str,
    data_capacity: usize,
    ctrl_capacity: usize,
) -> TestConn {
    let (tx, data_rx) = mpsc::channel(data_capacity);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(ctrl_capacity);
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();
    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx,
            ctrl_tx,
            ack_tx,
        })
        .await;
    relay.test_set_authenticated(conn_id, identity);
    TestConn {
        conn_id,
        data_rx,
        ack_rx,
        ctrl_rx,
    }
}

/// Regression for the WS signal spoofing vector. An authenticated peer must
/// not be able to broadcast a flag with someone else's `author_did` — the
/// relay overwrites the client-supplied field with the connection's real
/// authenticated identity.
#[tokio::test]
async fn test_signal_flag_author_did_pinned_to_auth_identity() {
    let mut relay = Relay::new_standalone(test_relay_config_with_auth());

    let mut alice = connect_preauth(
        &mut relay,
        1,
        "did:key:zAlice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut bob = connect_preauth(
        &mut relay,
        2,
        "did:key:zBob",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;
    // Signals are their own stream: a document subscription does not put
    // a connection in the space's signal recipient set.
    subscribe_to_signals(&mut relay, 1, TEST_SPACE).await;
    subscribe_to_signals(&mut relay, 2, TEST_SPACE).await;
    common::drain_subscribe_status(&mut alice.ack_rx).await;
    common::drain_subscribe_status(&mut bob.ack_rx).await;

    // Alice crafts a signal claiming to be Charlie.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zCharlie",
        i32::from(sync::FlagKind::Info),
        i32::from(sync::AudienceType::Space),
        "forged signal",
        "",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    // Bob receives the signal with Alice's real DID, not Charlie's.
    let signal = recv_signal(&mut bob.data_rx).await;
    assert_eq!(
        signal.author_did, "did:key:zAlice",
        "relay must rewrite `author_did` to the sender's authenticated identity"
    );

    // Alice authored it and receives it — probed on the DATA lane, where
    // signals ride.
    assert_own_signal_delivered(&mut alice.data_rx);
}

/// Even when the signal is addressed to a specific participant
/// (`AudienceType::Participant`), the author must be the real sender — a
/// spoofed DM would otherwise let a peer impersonate arbitrary senders to
/// a target.
#[tokio::test]
async fn test_signal_flag_participant_dm_author_did_pinned() {
    let mut relay = Relay::new_standalone(test_relay_config_with_auth());

    let mut alice = connect_preauth(
        &mut relay,
        1,
        "did:key:zAlice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut bob = connect_preauth(
        &mut relay,
        2,
        "did:key:zBob",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;
    // Signals are their own stream: a document subscription does not put
    // a connection in the space's signal recipient set.
    subscribe_to_signals(&mut relay, 1, TEST_SPACE).await;
    subscribe_to_signals(&mut relay, 2, TEST_SPACE).await;
    common::drain_subscribe_status(&mut alice.ack_rx).await;
    common::drain_subscribe_status(&mut bob.ack_rx).await;

    // Alice tries to DM Bob while impersonating Charlie.
    let msg = make_flag_signal(
        TEST_SPACE,
        TEST_DOC,
        "did:key:zCharlie",
        i32::from(sync::FlagKind::Info),
        i32::from(sync::AudienceType::Participant),
        "pretend I'm charlie",
        "did:key:zBob",
    );
    relay
        .process_command(RelayCommand::Signal { conn_id: 1, msg })
        .await;

    let signal = recv_signal(&mut bob.data_rx).await;
    assert_eq!(
        signal.author_did, "did:key:zAlice",
        "DM author must match authenticated sender"
    );

    // Alice does receive a copy in participant mode (she is an endpoint of
    // the DM), but the `author_did` there also matches her real identity.
    let echo = recv_signal(&mut alice.data_rx).await;
    assert_eq!(echo.author_did, "did:key:zAlice");
}

/// An authenticated peer must not be able to broadcast a cursor position
/// labelled as a different user via `PresenceUpdate.participant_did`.
#[tokio::test]
async fn test_presence_update_participant_did_pinned() {
    let mut relay = Relay::new_standalone(test_relay_config_with_auth());

    let _alice = connect_preauth(
        &mut relay,
        1,
        "did:key:zAlice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let mut bob = connect_preauth(
        &mut relay,
        2,
        "did:key:zBob",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;

    // Drain the subscribe catch-up SyncOps envelopes so the next recv
    // delivers the presence update we're testing for.
    while bob.data_rx.try_recv().is_ok() {}

    // Alice sends presence forging Charlie's DID.
    let msg = sync::PresenceUpdate {
        space_id: TEST_SPACE.into(),
        document_id: TEST_DOC.into(),
        participant_did: "did:key:zCharlie".into(),
        cursor_pos: 7,
        custom_data: Vec::new(),
    };
    relay
        .process_command(RelayCommand::PresenceUpdate { conn_id: 1, msg })
        .await;

    // Bob receives a presence envelope on the data channel.
    let bytes = tokio::time::timeout(Duration::from_secs(1), bob.data_rx.recv())
        .await
        .expect("presence timed out")
        .expect("channel closed");
    let env = decode_envelope(&bytes).unwrap();
    match env.payload {
        Some(Payload::PresenceUpdate(p)) => {
            assert_eq!(
                p.participant_did, "did:key:zAlice",
                "presence participant_did must be rewritten to sender's real DID"
            );
            assert_eq!(p.cursor_pos, 7, "cursor position should pass through");
        }
        other => panic!("expected PresenceUpdate, got {other:?}"),
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // many sequential RegisterDocument / Unregister envelopes
async fn test_list_space_documents_returns_active_entries() {
    let config = test_relay_config();
    let mut relay = Relay::new_standalone(config);

    let mut conn = connect_client(
        &mut relay,
        1,
        "client-1",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    let space = "475e1f0c-14a9-4b83-8b73-95c80cfd166d";

    // Register three documents via relay commands.
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: space.into(),
                document_id: "00000000-0000-0000-0000-00000000000a".into(),
                path: "a.md".into(),
                metadata: None,
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
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: space.into(),
                document_id: "00000000-0000-0000-0000-00000000000b".into(),
                path: "b.md".into(),
                metadata: None,
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
    relay
        .process_command(RelayCommand::RegisterDocument {
            conn_id: 1,
            msg: sync::RegisterDocument {
                space_id: space.into(),
                document_id: "00000000-0000-0000-0000-00000000000c".into(),
                path: "c.md".into(),
                metadata: None,
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

    // Soft-delete doc-b.
    relay
        .process_command(RelayCommand::UnregisterDocument {
            conn_id: 1,
            msg: sync::UnregisterDocument {
                space_id: space.into(),
                document_id: "00000000-0000-0000-0000-00000000000b".into(),
                metadata: None,
            },
        })
        .await;

    // Drain any own-ack frames from registration/unregistration.
    while conn.ack_rx.try_recv().is_ok() {}

    // Send ListSpaceDocuments.
    relay
        .process_command(RelayCommand::ListSpaceDocuments {
            conn_id: 1,
            msg: sync::ListSpaceDocuments {
                space_id: space.into(),
            },
        })
        .await;

    // Read the response from the own-ack lane (it answers this conn's query).
    let bytes = conn.ack_rx.recv().await.expect("expected response");
    let envelope = decode_envelope(&bytes).unwrap();
    match envelope.payload {
        Some(Payload::ListSpaceDocumentsResult(result)) => {
            assert_eq!(result.space_id, space);
            assert_eq!(result.documents.len(), 2, "should have 2 active docs");
            let ids: Vec<&str> = result
                .documents
                .iter()
                .map(|d| d.document_id.as_str())
                .collect();
            assert!(
                ids.contains(&"00000000-0000-0000-0000-00000000000a"),
                "should contain doc-a"
            );
            assert!(
                ids.contains(&"00000000-0000-0000-0000-00000000000c"),
                "should contain doc-c"
            );
            assert!(
                !ids.contains(&"00000000-0000-0000-0000-00000000000b"),
                "should not contain deleted doc-b"
            );
        }
        other => panic!("expected ListSpaceDocumentsResult, got {other:?}"),
    }
}

#[tokio::test]
async fn test_list_space_documents_empty_registry() {
    let config = test_relay_config();
    let mut relay = Relay::new_standalone(config);

    let mut conn = connect_client(
        &mut relay,
        1,
        "client-1",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    // List docs on a space with no registry entries.
    relay
        .process_command(RelayCommand::ListSpaceDocuments {
            conn_id: 1,
            msg: sync::ListSpaceDocuments {
                space_id: "ba8eaecf-614e-4d4e-8338-06dea0548af7".into(),
            },
        })
        .await;

    let bytes = conn.ack_rx.recv().await.expect("expected response");
    let envelope = decode_envelope(&bytes).unwrap();
    match envelope.payload {
        Some(Payload::ListSpaceDocumentsResult(result)) => {
            assert_eq!(result.space_id, "ba8eaecf-614e-4d4e-8338-06dea0548af7");
            assert!(result.documents.is_empty(), "should be empty");
        }
        other => panic!("expected ListSpaceDocumentsResult, got {other:?}"),
    }
}

#[tokio::test]
async fn test_fresh_client_discovers_documents_via_list() {
    let addr = start_relay().await;

    // Client 1 connects and registers two documents.
    let mut c1 = TestClient::connect(&addr).await;
    c1.handshake().await;

    let space = "3314f713-09a4-40c6-8910-0a2ea70c5c53";

    // Register doc-a.
    let reg_a = sync::SyncEnvelope {
        payload: Some(Payload::RegisterDocument(sync::RegisterDocument {
            space_id: space.into(),
            document_id: "00000000-0000-0000-0000-00000000000a".into(),
            path: "a.md".into(),
            metadata: None,
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
        })),
    };
    c1.send_envelope(&reg_a).await;

    // Subscribe + push ops for doc-a so it has content.
    c1.subscribe(space, "00000000-0000-0000-0000-00000000000a")
        .await;
    let (ops, metadata) = make_edit("c1", "hello", "seed doc-a");
    c1.send_ops(space, "00000000-0000-0000-0000-00000000000a", ops, metadata)
        .await;

    // Register doc-b.
    let reg_b = sync::SyncEnvelope {
        payload: Some(Payload::RegisterDocument(sync::RegisterDocument {
            space_id: space.into(),
            document_id: "00000000-0000-0000-0000-00000000000b".into(),
            path: "b.md".into(),
            metadata: None,
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
        })),
    };
    c1.send_envelope(&reg_b).await;

    // Wait for both registrations to land before listing.
    wait_for_doc_count(&addr, space, 2).await;

    // Client 2 connects fresh (simulates post-clone daemon).
    let mut c2 = TestClient::connect(&addr).await;
    c2.handshake().await;

    // Client 2 sends ListSpaceDocuments.
    let list_req = sync::SyncEnvelope {
        payload: Some(Payload::ListSpaceDocuments(sync::ListSpaceDocuments {
            space_id: space.into(),
        })),
    };
    c2.send_envelope(&list_req).await;

    // Client 2 receives the result.
    let result = c2.recv_envelope().await;
    match result.payload {
        Some(Payload::ListSpaceDocumentsResult(r)) => {
            assert_eq!(r.space_id, space);
            assert_eq!(r.documents.len(), 2);
            let ids: Vec<&str> = r.documents.iter().map(|d| d.document_id.as_str()).collect();
            assert!(ids.contains(&"00000000-0000-0000-0000-00000000000a"));
            assert!(ids.contains(&"00000000-0000-0000-0000-00000000000b"));
        }
        other => panic!("expected ListSpaceDocumentsResult, got {other:?}"),
    }

    // Client 2 subscribes to doc-a and gets catch-up ops.
    let catchup = c2
        .subscribe(space, "00000000-0000-0000-0000-00000000000a")
        .await;
    assert!(catchup.is_some(), "should receive catch-up ops for doc-a");
}

#[tokio::test]
async fn test_live_peer_document_registration_broadcast() {
    let addr = start_relay().await;
    let space = "574677ed-3946-4c23-80ac-58dc8deeb08e";

    // Client 1 connects, registers doc-a, subscribes.
    let mut c1 = TestClient::connect(&addr).await;
    c1.handshake().await;

    let reg_a = sync::SyncEnvelope {
        payload: Some(Payload::RegisterDocument(sync::RegisterDocument {
            space_id: space.into(),
            document_id: "00000000-0000-0000-0000-00000000000a".into(),
            path: "a.md".into(),
            metadata: None,
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
        })),
    };
    c1.send_envelope(&reg_a).await;
    // The sender receives a RegisterDocumentAck on its own
    // connection. Drain it so the subsequent `recv_envelope`
    // calls land on the broadcast for c2's `doc-b` register.
    let _ = c1.recv_envelope().await;
    c1.subscribe(space, "00000000-0000-0000-0000-00000000000a")
        .await;

    // Client 2 connects, registers doc-b.
    let mut c2 = TestClient::connect(&addr).await;
    c2.handshake().await;

    // Client 2 first discovers doc-a via ListSpaceDocuments.
    let list_req = sync::SyncEnvelope {
        payload: Some(Payload::ListSpaceDocuments(sync::ListSpaceDocuments {
            space_id: space.into(),
        })),
    };
    c2.send_envelope(&list_req).await;
    let result = c2.recv_envelope().await;
    match result.payload {
        Some(Payload::ListSpaceDocumentsResult(r)) => {
            assert_eq!(r.documents.len(), 1);
            assert_eq!(
                r.documents[0].document_id,
                "00000000-0000-0000-0000-00000000000a"
            );
        }
        other => panic!("expected ListSpaceDocumentsResult, got {other:?}"),
    }

    // Client 2 subscribes to doc-a (discovered) and doc-b (own).
    c2.subscribe(space, "00000000-0000-0000-0000-00000000000a")
        .await;
    c2.subscribe(space, "00000000-0000-0000-0000-00000000000b")
        .await;

    // Client 2 registers doc-b.
    let reg_b = sync::SyncEnvelope {
        payload: Some(Payload::RegisterDocument(sync::RegisterDocument {
            space_id: space.into(),
            document_id: "00000000-0000-0000-0000-00000000000b".into(),
            path: "b.md".into(),
            metadata: None,
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
        })),
    };
    c2.send_envelope(&reg_b).await;

    // Client 1 should receive the RegisterDocument broadcast for doc-b.
    let broadcast = c1.recv_envelope().await;
    match broadcast.payload {
        Some(Payload::RegisterDocument(msg)) => {
            assert_eq!(msg.document_id, "00000000-0000-0000-0000-00000000000b");
            assert_eq!(msg.path, "b.md");
        }
        other => panic!("expected RegisterDocument broadcast, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Observer / snippet tests
// ---------------------------------------------------------------------------

/// Test after-merge observer that records all emitted text merge events.
struct CapturingAfterMerge {
    events: Mutex<Vec<MergedEvent>>,
}

impl CapturingAfterMerge {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    fn events(&self) -> Vec<MergedEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl AfterMergeObserver for CapturingAfterMerge {
    async fn after_text_merge(&self, event: MergedEvent, _doc: &kutl_core::Document) {
        self.events.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn test_client_provided_snippet_triggers_immediate_after_merge() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    // The connection authenticates AS "alice" so the authoritative author of
    // its edits is "alice" (the relay overrides client metadata).
    let _conn = connect_client(
        &mut relay,
        1,
        "alice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;

    // Send ops with a client-provided snippet in ChangeMetadata.
    let (ops, mut metadata) = make_edit("alice", "hello world", "add greeting");
    metadata[0].change_snippet = "Added a greeting to the document".into();

    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata,
                ..Default::default()
            }),
        })
        .await;

    // The after-merge observer should have received exactly one event
    // (client-provided snippet bypasses the debounce window).
    let events = after.events();
    assert_eq!(events.len(), 1, "expected exactly one after-merge event");
    assert_eq!(events[0].author_did, "alice");
    assert_eq!(events[0].intent, "add greeting");
}

#[tokio::test]
async fn test_edit_without_snippet_deferred_during_debounce() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    let _conn = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;

    // Send ops WITHOUT a client-provided snippet.
    let (ops, metadata) = make_edit("alice", "hello world", "add greeting");

    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata,
                ..Default::default()
            }),
        })
        .await;

    // The edit event is held in pending_edit during the debounce window —
    // no event is emitted immediately. The timer would fire later and
    // emit the event, but standalone mode has no self_tx so the timer
    // is never started.
    let events = after.events();
    assert!(
        events.is_empty(),
        "snippet-eligible edit should be deferred, not emitted immediately"
    );
}

#[tokio::test]
async fn test_deferred_edit_flushed_on_timer() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    let _conn = connect_client(
        &mut relay,
        1,
        "alice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;

    // Send ops WITHOUT a client-provided snippet.
    let (ops, metadata) = make_edit("alice", "hello world", "add greeting");

    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops,
                metadata,
                ..Default::default()
            }),
        })
        .await;

    // No event emitted yet — edit is held pending debounce.
    assert!(
        after.events().is_empty(),
        "snippet-eligible edit should be deferred, not emitted immediately"
    );

    // Manually flush the pending edit (simulates timer firing).
    relay
        .process_command(RelayCommand::FlushPendingEdit {
            space_id: TEST_SPACE.into(),
            document_id: TEST_DOC.into(),
        })
        .await;

    let events = after.events();
    assert_eq!(events.len(), 1, "expected one event after flush");
    assert_eq!(events[0].author_did, "alice");
    assert_eq!(events[0].intent, "add greeting");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_different_author_flushes_pending_edit() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    let _c1 = connect_client(
        &mut relay,
        1,
        "alice",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    let _c2 = connect_client(
        &mut relay,
        2,
        "bob",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;
    subscribe_to_doc(&mut relay, 2).await;

    // Alice edits — should be held (no event yet).
    let (ops_a, meta_a) = make_edit("alice", "hello from alice", "alice writes");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: ops_a,
                metadata: meta_a,
                ..Default::default()
            }),
        })
        .await;

    assert!(after.events().is_empty(), "alice's edit should be deferred");

    // Bob edits same document — should flush alice's pending edit.
    let (ops_b, meta_b) = make_edit("bob", "hello from bob", "bob writes");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 2,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: ops_b,
                metadata: meta_b,
                ..Default::default()
            }),
        })
        .await;

    // Alice's event should have been flushed by the after-merge observer.
    let events = after.events();
    let alice_events: Vec<_> = events
        .iter()
        .filter(|ev| ev.author_did == "alice")
        .collect();

    assert_eq!(alice_events.len(), 1, "alice's edit should be flushed");

    // Bob's edit is now pending (not yet emitted).
    let bob_events: Vec<_> = events.iter().filter(|ev| ev.author_did == "bob").collect();
    assert!(bob_events.is_empty(), "bob's edit should be deferred");

    // Flush bob's pending edit.
    relay
        .process_command(RelayCommand::FlushPendingEdit {
            space_id: TEST_SPACE.into(),
            document_id: TEST_DOC.into(),
        })
        .await;

    let events = after.events();
    let bob_events: Vec<_> = events.iter().filter(|ev| ev.author_did == "bob").collect();
    assert_eq!(bob_events.len(), 1, "bob's edit should be flushed");
}

#[tokio::test]
async fn test_same_author_accumulates_pending_edit() {
    let after = Arc::new(CapturingAfterMerge::new());
    let mut relay = Relay::new_standalone_with_observer(
        test_relay_config(),
        None,
        None,
        None,
        None,
        Arc::new(kutl_relay::NoopObserver),
        Arc::new(kutl_relay::NoopBeforeMergeObserver),
        after.clone(),
    );

    let _conn = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    subscribe_to_doc(&mut relay, 1).await;

    // First edit by alice.
    let (ops1, meta1) = make_edit("alice", "first edit here!", "first");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: ops1,
                metadata: meta1,
                ..Default::default()
            }),
        })
        .await;

    // Second edit by alice (same author — should accumulate).
    let (ops2, meta2) = make_edit("alice", "second edit here!", "second");
    relay
        .process_command(RelayCommand::InboundSyncOps {
            conn_id: 1,
            msg: Box::new(sync::SyncOps {
                space_id: TEST_SPACE.into(),
                document_id: TEST_DOC.into(),
                ops: ops2,
                metadata: meta2,
                ..Default::default()
            }),
        })
        .await;

    // No events yet — both are accumulated.
    assert!(after.events().is_empty(), "edits should be deferred");

    // Flush.
    relay
        .process_command(RelayCommand::FlushPendingEdit {
            space_id: TEST_SPACE.into(),
            document_id: TEST_DOC.into(),
        })
        .await;

    let events = after.events();
    assert_eq!(events.len(), 1, "should emit one accumulated event");
    assert_eq!(events[0].op_count, 2, "op_count should be accumulated");
    assert_eq!(events[0].intent, "second", "intent should be latest");
}

/// An authenticated client whose DID is not in the `authorized_keys` file
/// should be rejected when sending `SyncOps` to a loaded document.
#[tokio::test]
async fn test_sync_ops_without_authorization_rejected() {
    // Generate two distinct keypairs.
    let (did_a, key_a) = common::test_keypair();
    let (did_b, key_b) = common::test_keypair();

    // Write an authorized_keys file containing only Client A's DID.
    let keys_dir = tempfile::tempdir().unwrap();
    let keys_path = keys_dir.path().join("authorized_keys");
    std::fs::write(&keys_path, &did_a).unwrap();

    // Start relay with authorized_keys (auth is unconditional).
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut config = test_relay_config();
    config.authorized_keys_file = Some(keys_path);

    // Storeless boot: in-memory registries; build_app requires a
    // data dir, so construct the storeless shape via the host seam.
    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Both clients authenticate (obtain tokens).
    let token_a = authenticate(&addr, &did_a, &key_a).await;
    let token_b = authenticate(&addr, &did_b, &key_b).await;

    // Client A: handshake + subscribe to "allowed/doc" — should succeed.
    let mut client_a = TestClient::connect(&addr).await;
    client_a
        .send_envelope(&handshake_envelope_with_token(
            "e0b107f9-f96f-49a2-8616-5a2ac7ae5516",
            &token_a,
            "",
        ))
        .await;
    let ack = client_a.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "client A handshake should succeed"
    );
    let _catch_up = client_a.subscribe("allowed", "doc").await;

    // Client B: handshake succeeds (token is valid), but DID is not authorized.
    let mut client_b = TestClient::connect(&addr).await;
    client_b
        .send_envelope(&handshake_envelope_with_token(
            "32e00e98-e076-4aa0-811b-1e93d848b910",
            &token_b,
            "",
        ))
        .await;
    let ack = client_b.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "client B handshake should succeed (token is valid)"
    );

    // Client B sends SyncOps to "allowed/doc" — should be rejected (DID not authorized).
    let (ops, metadata) = make_edit("intruder", "malicious edit", "attack");
    client_b.send_ops("allowed", "doc", ops, metadata).await;

    let err = client_b.recv_error().await;
    assert_eq!(
        err.code,
        i32::from(sync::ErrorCode::AuthFailed),
        "unauthorized SyncOps should be rejected with AuthFailed"
    );
}

/// An authorized-keys entry scoped to one space authorizes that space but
/// rejects any other. Exercises the scope-aware `authorize` path in
/// `authorize_space` Step 3.
#[tokio::test]
async fn test_scoped_authorized_key_confined_to_its_space() {
    let (did_agent, key_agent) = common::test_keypair();

    // Grant the agent DID in ONE space only (a scoped entry, SSH-shaped).
    let keys_dir = tempfile::tempdir().unwrap();
    let keys_path = keys_dir.path().join("authorized_keys");
    std::fs::write(
        &keys_path,
        format!("{did_agent} scope=d9901c19-88b1-4c3a-854b-e26bd354e5bd"),
    )
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut config = test_relay_config();
    config.authorized_keys_file = Some(keys_path);

    // Storeless boot: in-memory registries; build_app requires a
    // data dir, so construct the storeless shape via the host seam.
    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let token_agent = authenticate(&addr, &did_agent, &key_agent).await;

    // In-scope: SyncOps to "space-a/doc" is accepted (fans out to a subscriber).
    let mut peer = TestClient::connect(&addr).await;
    peer.send_envelope(&handshake_envelope_with_token(
        "2ffc1d06-387e-48bb-8a34-312b6c6c3f69",
        &token_agent,
        "",
    ))
    .await;
    let ack = peer.recv_envelope().await;
    assert!(matches!(ack.payload, Some(Payload::HandshakeAck(_))));
    let _catch_up = peer
        .subscribe("d9901c19-88b1-4c3a-854b-e26bd354e5bd", "doc")
        .await;

    let mut agent = TestClient::connect(&addr).await;
    agent
        .send_envelope(&handshake_envelope_with_token(
            "d4f0bc5a-29de-46b5-80f9-aa428f1eedba",
            &token_agent,
            "",
        ))
        .await;
    let ack = agent.recv_envelope().await;
    assert!(matches!(ack.payload, Some(Payload::HandshakeAck(_))));
    let (ops, metadata) = make_edit("agent", "in-scope edit", "edit");
    agent
        .send_ops("d9901c19-88b1-4c3a-854b-e26bd354e5bd", "doc", ops, metadata)
        .await;

    // The peer receives the broadcast — the in-scope write was accepted.
    let forwarded = peer.recv_ops().await;
    assert_eq!(forwarded.space_id, "d9901c19-88b1-4c3a-854b-e26bd354e5bd");
    assert_eq!(forwarded.document_id, "doc");

    // Out of scope: SyncOps to "space-b/doc" is rejected (not in scope).
    let mut agent_b = TestClient::connect(&addr).await;
    agent_b
        .send_envelope(&handshake_envelope_with_token(
            "996a53b5-92e9-4453-8da9-d00b1ccc0428",
            &token_agent,
            "",
        ))
        .await;
    let ack = agent_b.recv_envelope().await;
    assert!(matches!(ack.payload, Some(Payload::HandshakeAck(_))));
    let (ops, metadata) = make_edit("agent", "out-of-scope edit", "edit");
    agent_b
        .send_ops("1e5fda11-1a16-495f-897c-d7e782a85c19", "doc", ops, metadata)
        .await;

    let err = agent_b.recv_error().await;
    assert_eq!(
        err.code,
        i32::from(sync::ErrorCode::AuthFailed),
        "out-of-scope SyncOps should be rejected with AuthFailed"
    );
}

/// Verify that an unauthorized client's `PresenceUpdate` is silently dropped
/// and does NOT reach subscribers.
#[tokio::test]
async fn test_presence_update_without_authorization_dropped() {
    // Generate two distinct keypairs.
    let (did_a, key_a) = common::test_keypair();
    let (did_b, key_b) = common::test_keypair();

    // Authorized_keys contains only Client A's DID.
    let keys_dir = tempfile::tempdir().unwrap();
    let keys_path = keys_dir.path().join("authorized_keys");
    std::fs::write(&keys_path, &did_a).unwrap();

    // Start relay with authorized_keys (auth is unconditional).
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut config = test_relay_config();
    config.authorized_keys_file = Some(keys_path);

    // Storeless boot: in-memory registries; build_app requires a
    // data dir, so construct the storeless shape via the host seam.
    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Both clients authenticate (obtain tokens).
    let token_a = authenticate(&addr, &did_a, &key_a).await;
    let token_b = authenticate(&addr, &did_b, &key_b).await;

    // Client A: handshake + subscribe to "allowed/doc".
    let mut client_a = TestClient::connect(&addr).await;
    client_a
        .send_envelope(&handshake_envelope_with_token(
            "e0b107f9-f96f-49a2-8616-5a2ac7ae5516",
            &token_a,
            "",
        ))
        .await;
    let ack = client_a.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "client A handshake should succeed"
    );
    let _catch_up = client_a.subscribe("allowed", "doc").await;

    // Client B: handshake succeeds (token is valid), but DID is not authorized.
    let mut client_b = TestClient::connect(&addr).await;
    client_b
        .send_envelope(&handshake_envelope_with_token(
            "32e00e98-e076-4aa0-811b-1e93d848b910",
            &token_b,
            "",
        ))
        .await;
    let ack = client_b.recv_envelope().await;
    assert!(
        matches!(ack.payload, Some(Payload::HandshakeAck(_))),
        "client B handshake should succeed (token is valid)"
    );

    // Client B sends a PresenceUpdate for "allowed/doc" — should be silently dropped.
    client_b
        .send_envelope(&presence_update_envelope(
            "allowed",
            "doc",
            &did_b,
            42,
            vec![],
        ))
        .await;

    // Give the relay time to process the presence update (if it were forwarded).
    // Then verify Client A received nothing — the unauthorized presence was dropped.
    match tokio::time::timeout(Duration::from_millis(500), client_a.ws.next()).await {
        Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => {
            let envelope = decode_envelope(&bytes).unwrap();
            panic!(
                "client A should NOT receive any message from unauthorized client; got {envelope:?}"
            );
        }
        Err(_) => {} // timeout — expected, presence was dropped
        other => panic!("unexpected WebSocket result: {other:?}"),
    }
}

/// A signal burst must not evict the
/// connection from a document it is syncing fine.
///
/// Live signals ride the `data` lane, which is per-connection
/// and shared with document traffic. The cost of that is plain: a
/// burst that fills the lane makes the next DOCUMENT op's `try_send` fail, and
/// `relay_and_evict` answers a failed send by removing the connection from
/// `slot.subscribers`. So a space that is merely chatty about signals could
/// knock a peer off a document — nothing about the shared lane alone
/// prevents it.
///
/// The mitigation is that bulk yields: signals stop short of the last
/// slots so document ops always have somewhere to land. This test pins
/// that signal-yield reserve, and it fails without it.
#[tokio::test]
async fn test_a_signal_burst_does_not_evict_an_actively_syncing_document() {
    let mut relay = Relay::new_standalone(test_relay_config());

    let mut writer = connect_client(
        &mut relay,
        1,
        "writer",
        FAST_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;
    // A reader that never drains, so its lane is what fills.
    let mut reader = connect_client(
        &mut relay,
        2,
        "reader",
        SLOW_CHANNEL_CAPACITY,
        TEST_CTRL_CAPACITY,
    )
    .await;

    subscribe_to_doc(&mut relay, writer.conn_id).await;
    subscribe_to_doc(&mut relay, reader.conn_id).await;
    subscribe_to_signals(&mut relay, reader.conn_id, TEST_SPACE).await;
    common::drain_subscribe_status(&mut writer.ack_rx).await;
    common::drain_subscribe_status(&mut reader.ack_rx).await;
    let _ = drain_data(&mut reader.data_rx);

    // A burst of space-audience signals, several times the reader's lane.
    for i in 0..(SLOW_CHANNEL_CAPACITY * 8) {
        let msg = make_flag_signal(
            TEST_SPACE,
            TEST_DOC,
            "did:key:zAlice",
            i32::from(sync::FlagKind::Info),
            i32::from(sync::AudienceType::Space),
            &format!("burst {i}"),
            "",
        );
        relay
            .process_command(RelayCommand::Signal { conn_id: 1, msg })
            .await;
    }

    // A document op arrives while the lane is still under burst pressure. This
    // is the moment the signal-yield reserve is about, and draining first
    // would destroy it — an empty lane accepts anything, so the test would
    // pass by removing the condition it exists to test.
    let _ = drain_ack(&mut reader.ack_rx);
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    let notices = drain_ack(&mut reader.ack_rx);
    assert!(
        !has_stale_notice(&notices),
        "a signal burst evicted the connection from a document it was syncing \
         fine. Signals share the data lane with document traffic and must \
         YIELD to it rather than consume it."
    );

    // And the subscription really is intact, not merely un-notified: once the
    // lane drains, a further op still reaches the reader.
    let _ = drain_data(&mut reader.data_rx);
    send_test_ops(&mut relay, writer.conn_id, 1).await;
    let after = drain_data(&mut reader.data_rx);
    assert!(
        after
            .iter()
            .any(|e| matches!(e.payload, Some(Payload::SyncOps(_)))),
        "the reader stopped receiving document ops after a signal burst — it was \
         dropped from `slot.subscribers`"
    );
}
