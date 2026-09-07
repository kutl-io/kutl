//! Deterministic interleaving of real [`SpaceWorker`] sessions with a real
//! relay actor, in one process, with the test playing the wire.
//!
//! Between a daemon and its relay run three schedulers: the daemon's event
//! loop, the relay actor, and the connection task draining the ack, ctrl and
//! data lanes into the socket. Their interleaving is what the convergence
//! properties are about, and a socket leaves it to the operating system, so
//! a property that holds at every interleaving point can only be sampled
//! over sockets. Here the relay processes a frame when the test says so, a
//! lane delivers a frame when the test says so, and the workers run on the
//! paused clock: a worker runs until it has nothing left to do, and time
//! moves only when the test moves it. A schedule is reproducible, so a
//! property can be checked at every point of it.
//!
//! Real: the worker's whole session (`run`: connect, listen, snapshot,
//! reconcile, scan, event loop, reconnect), the frames it encodes and
//! decodes, the relay actor and every handler, the lanes at their
//! production capacities, the bearer challenge (driven through the actor
//! rather than over HTTP). Played: the socket and the clock. Outside the
//! schedule: the file watcher, which runs on real threads, and anything a
//! socket's partial writes, a kill, or an fsync would do.
//!
//! The clock is tokio's paused clock, which needs a `current_thread`
//! runtime (`#[tokio::test(start_paused = true)]`). A sleep on it completes
//! once nothing is runnable, which is the quiesce primitive; a blocking-pool
//! task (the worker records the relay identity on one) holds the clock
//! until it finishes, so a quiesce waits for it too.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use anyhow::Result;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer as _, SigningKey};
use kutl_proto::protocol::{decode_envelope, handshake_envelope_with_token};
use kutl_proto::sync::{ChangeMetadata, SyncEnvelope, sync_envelope::Payload};
use kutl_relay::config::RelayConfig;
use kutl_relay::record_log::SegmentRecordLog;
use kutl_relay::relay::{ConnId, Relay, RelayCommand};
use kutl_relay::testing::{CTRL_LANE_CAPACITY, client_command};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::session::SessionLink;
use super::{SpaceWorker, SpaceWorkerConfig};
use crate::client::{SyncCommand, SyncEvent, connected_event, handle_inbound, outbound_frame};
use crate::state::DaemonState;

mod scenarios;

/// Identities minted at construction and written to the relay's authorized
/// keys once. The file is re-read on a modification-time change, and two
/// writes inside its resolution could hide the second, so parties draw from
/// this fixed pool rather than being added to the file as they join.
const MAX_PARTIES: usize = 8;

/// Seed byte for party `i`'s Ed25519 key, `PARTY_KEY_SEED_BASE + i`. Fixed,
/// so a party's DID is the same in every run and a failing schedule
/// reproduces from its index alone.
const PARTY_KEY_SEED_BASE: u8 = 0x40;

/// The virtual sleep that lets every other task run: under the paused clock
/// it completes only once nothing is runnable.
const QUIESCE_TICK: Duration = kutl_core::std_duration(kutl_core::SignedDuration::from_millis(1));

/// Virtual time a stopped worker gets to exit. Under the paused clock this
/// fires as soon as the worker parks without exiting, so a worker that
/// ignores cancellation fails the test instead of hanging it.
const STOP_TIMEOUT: Duration = kutl_core::std_duration(kutl_core::SignedDuration::from_secs(60));

/// Virtual time the workers get once the wire is idle, so their deferred
/// work fires: the placement pacing floor (`RECONCILE_PASS_MIN_INTERVAL`,
/// 200 ms), the state-save floor (`MIN_STATE_SAVE_INTERVAL`, 1 s), and the
/// metrics tick (`METRICS_EMIT_INTERVAL`, 10 s), whose ungated placement
/// pass is the idle fixpoint driver. Past all of them, so a wire still idle
/// after a settle means the workers are at their fixpoint.
const SETTLE_WINDOW: Duration = kutl_core::std_duration(kutl_core::SignedDuration::from_secs(15));

/// Steps a schedule may take before the harness calls it a livelock.
const STEP_CEILING: usize = 100_000;

/// The relay URL a worker on the in-process link believes it is connected
/// to: a label for metrics and the known-relays record, never dialed.
const IN_PROCESS_RELAY_URL: &str = "inproc://relay";

/// A session's channel ends, handed to the harness by
/// [`SessionLink::InProcess`] as the worker opens the session.
pub(crate) struct SessionEnds {
    /// The worker's outbound commands, which the harness turns into frames.
    pub(crate) cmd_rx: mpsc::UnboundedReceiver<SyncCommand>,
    /// The worker's inbound events, which the harness feeds from the lanes.
    pub(crate) event_tx: mpsc::Sender<SyncEvent>,
    /// The blob upload backlog the socket writer would decrement.
    pub(crate) blob_backlog: Arc<AtomicI64>,
}

/// One of a connection's outbound lanes, as the relay actor fills them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lane {
    /// Unbounded own-ack lane: replies to the connection's own frames.
    Ack,
    /// Bounded control lane: lifecycle broadcasts and corrections.
    Ctrl,
    /// Bounded data lane: document ops, catch-up, signals.
    Data,
}

impl Lane {
    /// The connection task's drain order: own-acks first, then control,
    /// then data.
    pub(crate) const PRIORITY: [Lane; 3] = [Lane::Ack, Lane::Ctrl, Lane::Data];
}

/// A connection the harness plays.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PartyId(usize);

/// One atomic move of the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// The relay processes the next frame `PartyId` sent.
    Process(PartyId),
    /// `PartyId` receives the next frame waiting on `Lane`.
    Deliver(PartyId, Lane),
    /// The wire was idle: [`SETTLE_WINDOW`] of virtual time passed, in
    /// timer order, for the workers' deferred work.
    Settle,
}

/// The receiving ends of a connection's lanes, which the relay fills.
struct Lanes {
    data: mpsc::Receiver<Vec<u8>>,
    ctrl: mpsc::Receiver<Vec<u8>>,
    ack: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl Lanes {
    fn has(&self, lane: Lane) -> bool {
        match lane {
            Lane::Ack => !self.ack.is_empty(),
            Lane::Ctrl => !self.ctrl.is_empty(),
            Lane::Data => !self.data.is_empty(),
        }
    }

    fn take(&mut self, lane: Lane) -> Option<Vec<u8>> {
        match lane {
            Lane::Ack => self.ack.try_recv().ok(),
            Lane::Ctrl => self.ctrl.try_recv().ok(),
            Lane::Data => self.data.try_recv().ok(),
        }
    }
}

/// What is behind a party's connection.
enum Role {
    /// A real worker. Each session it opens arrives on `sessions`; `live`
    /// is the one the lanes currently feed.
    Daemon {
        sessions: mpsc::UnboundedReceiver<SessionEnds>,
        live: Option<SessionEnds>,
        task: Option<tokio::task::JoinHandle<Result<()>>>,
        cancel: CancellationToken,
        space_root: PathBuf,
    },
    /// Frames the test authors. Everything delivered to it is kept.
    Scripted { received: Vec<Payload> },
}

struct Party {
    conn_id: ConnId,
    did: String,
    key: SigningKey,
    space_id: String,
    /// The party's origin clock, for the lifecycle stamps a scripted party
    /// puts on its own frames.
    clock: kutl_core::HlcClock,
    /// `Some` while connected at the relay.
    lanes: Option<Lanes>,
    /// Frames the party has sent that the relay has not processed.
    to_relay: VecDeque<RelayCommand>,
    /// Every payload the party has sent, in order.
    sent: Vec<Payload>,
    role: Role,
}

/// Queue one frame the party sent, as the connection task would hand it to
/// the actor, keeping its payload for assertions.
fn enqueue(
    party_conn: ConnId,
    to_relay: &mut VecDeque<RelayCommand>,
    sent: &mut Vec<Payload>,
    bytes: &[u8],
) {
    let envelope = decode_envelope(bytes).expect("a worker frame decodes");
    if let Some(payload) = &envelope.payload {
        sent.push(payload.clone());
    }
    to_relay
        .push_back(client_command(party_conn, envelope).expect("a worker frame is a client frame"));
}

/// The harness: one relay actor, the parties connected to it, and the wire
/// between them, moved one step at a time.
pub(crate) struct Interleaver {
    relay: Relay,
    parties: Vec<Party>,
    keys: Vec<SigningKey>,
    outbound_capacity: usize,
    /// Whether the workers have had a settle since the wire last moved; an
    /// idle wire is only idle once they have.
    settled: bool,
    _authorized_keys: tempfile::NamedTempFile,
    roots: Vec<tempfile::TempDir>,
}

/// Install a tracing subscriber for the test binary, once, when the
/// operator asks for logs (`KUTL_LOG`/`RUST_LOG`). Workers and the relay run
/// in process, so without one a failing schedule reads as a bare assertion.
/// Writes to stderr: libtest's capture is per test thread, and the worker
/// logs from its own task.
fn init_test_tracing() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let filter = std::env::var("KUTL_LOG").or_else(|_| std::env::var("RUST_LOG"));
        let Ok(filter) = filter else { return };
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .try_init();
    });
}

/// The test binary's metrics recorder: the workers' counters are the
/// harness's view of what they did (see [`Interleaver::placements`]). Keeps
/// counters cumulatively and ignores gauges and histograms. A draining
/// recorder (one whose snapshot resets what it reads) is wrong here: tests
/// share the process-wide recorder, so one test's read would zero another's
/// count. Counters are read per space, and each schedule uses a fresh space
/// id.
#[derive(Default)]
struct CumulativeCounters {
    counters: std::sync::Mutex<
        std::collections::HashMap<metrics::Key, Arc<std::sync::atomic::AtomicU64>>,
    >,
}

impl metrics::Recorder for CumulativeCounters {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        let cell = Arc::clone(
            self.counters
                .lock()
                .expect("counter map lock")
                .entry(key.clone())
                .or_default(),
        );
        metrics::Counter::from_arc(cell)
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// The process-wide recorder, installed once.
fn counters() -> &'static CumulativeCounters {
    static COUNTERS: std::sync::OnceLock<&'static CumulativeCounters> = std::sync::OnceLock::new();
    COUNTERS.get_or_init(|| {
        let recorder: &'static CumulativeCounters = Box::leak(Box::default());
        metrics::set_global_recorder(recorder)
            .expect("the test binary installs one metrics recorder");
        recorder
    })
}

/// The current value of counter `name` summed over every label set that
/// carries `space` as its `space` label.
fn space_counter(name: &str, space: &str) -> u64 {
    counters()
        .counters
        .lock()
        .expect("counter map lock")
        .iter()
        .filter(|(key, _)| {
            key.name() == name
                && key
                    .labels()
                    .any(|l| l.key() == "space" && l.value() == space)
        })
        .map(|(_, cell)| cell.load(std::sync::atomic::Ordering::Acquire))
        .sum()
}

impl Interleaver {
    /// A relay with a data lane of `outbound_capacity` slots per connection,
    /// [`MAX_PARTIES`] authorized identities, and no backends except, when
    /// `keeps_records`, a segment record log in a temp dir (without one the
    /// relay answers every catch-up with an empty page and cannot re-serve a
    /// record a paused stream missed).
    pub(crate) fn new(outbound_capacity: usize, keeps_records: bool) -> Self {
        use std::io::Write as _;
        init_test_tracing();
        counters();
        let keys: Vec<SigningKey> = (0..MAX_PARTIES)
            .map(|i| {
                let seed = [PARTY_KEY_SEED_BASE + u8::try_from(i).expect("party index fits a byte");
                    ed25519_dalek::SECRET_KEY_LENGTH];
                SigningKey::from_bytes(&seed)
            })
            .collect();
        let mut authorized_keys = tempfile::NamedTempFile::new().expect("authorized keys file");
        for key in &keys {
            writeln!(
                authorized_keys,
                "{}",
                kutl_signals::did_key_encode(&key.verifying_key())
            )
            .expect("write authorized key");
        }
        authorized_keys.flush().expect("flush authorized keys");
        let config = RelayConfig {
            port: 0,
            relay_name: "interleave".into(),
            outbound_capacity,
            authorized_keys_file: Some(authorized_keys.path().to_path_buf()),
            ..Default::default()
        };
        let mut relay = Relay::new_standalone(config);
        let mut roots = Vec::new();
        if keeps_records {
            let dir = tempfile::tempdir().expect("record log dir");
            relay.test_set_record_log(
                Some(Arc::new(SegmentRecordLog::rooted_at(dir.path()))),
                None,
            );
            roots.push(dir);
        }
        Self {
            relay,
            parties: Vec::new(),
            keys,
            outbound_capacity,
            settled: false,
            _authorized_keys: authorized_keys,
            roots,
        }
    }

    /// A fresh, canonical space root a daemon party can be started in, so a
    /// test can lay files down before the worker exists.
    pub(crate) fn space_root(&mut self) -> PathBuf {
        let dir = tempfile::tempdir().expect("space root");
        let path = dir.path().canonicalize().expect("canonical space root");
        self.roots.push(dir);
        path
    }

    fn next_identity(&self) -> (ConnId, String, SigningKey) {
        let index = self.parties.len();
        let key = self
            .keys
            .get(index)
            .unwrap_or_else(|| panic!("at most {MAX_PARTIES} parties"))
            .clone();
        let did = kutl_signals::did_key_encode(&key.verifying_key());
        (ConnId::try_from(index + 1).expect("conn id"), did, key)
    }

    fn push_party(
        &mut self,
        conn_id: ConnId,
        did: String,
        key: SigningKey,
        space_id: &str,
        role: Role,
    ) -> PartyId {
        self.parties.push(Party {
            conn_id,
            did,
            key,
            space_id: space_id.to_owned(),
            clock: kutl_core::HlcClock::new(kutl_core::ActorId(uuid::Uuid::new_v4())),
            lanes: None,
            to_relay: VecDeque::new(),
            sent: Vec::new(),
            role,
        });
        PartyId(self.parties.len() - 1)
    }

    /// Start a real worker for `space_id` in `space_root` on the in-process
    /// link. It runs on the paused clock from here on and opens its first
    /// session as soon as it is polled; [`Self::connect`] answers it.
    pub(crate) fn add_daemon(&mut self, space_root: PathBuf, space_id: &str) -> PartyId {
        let (conn_id, did, key) = self.next_identity();
        let cancel = CancellationToken::new();
        let config = SpaceWorkerConfig {
            space_root: space_root.clone(),
            author_did: did.clone(),
            relay_url: IN_PROCESS_RELAY_URL.into(),
            space_id: space_id.to_owned(),
            signing_key: Some(key.clone()),
            one_shot: false,
            display_name: String::new(),
            ready: None,
            cancel: cancel.clone(),
            // Inside the space's own `.kutl/`, never the developer's record.
            known_relays_path: Some(space_root.join(".kutl").join("known_relays.toml")),
            // The scanning backend: it never talks to the platform watcher,
            // which a test binary's burst load can starve.
            poll_watcher: true,
        };
        let mut worker = SpaceWorker::new(config).expect("space worker");
        let (sessions_tx, sessions) = mpsc::unbounded_channel();
        worker.link = SessionLink::InProcess(sessions_tx);
        let task = tokio::spawn(worker.run());
        self.push_party(
            conn_id,
            did,
            key,
            space_id,
            Role::Daemon {
                sessions,
                live: None,
                task: Some(task),
                cancel,
                space_root,
            },
        )
    }

    /// A party whose frames the test writes.
    pub(crate) fn add_peer(&mut self, space_id: &str) -> PartyId {
        let (conn_id, did, key) = self.next_identity();
        self.push_party(
            conn_id,
            did,
            key,
            space_id,
            Role::Scripted {
                received: Vec::new(),
            },
        )
    }

    pub(crate) fn did(&self, id: PartyId) -> &str {
        &self.parties[id.0].did
    }

    /// Every payload `id` has sent, in order.
    pub(crate) fn sent(&self, id: PartyId) -> &[Payload] {
        &self.parties[id.0].sent
    }

    /// Every payload delivered to scripted party `id`, in order.
    pub(crate) fn received(&self, id: PartyId) -> &[Payload] {
        match &self.parties[id.0].role {
            Role::Scripted { received } => received,
            Role::Daemon { .. } => panic!("a worker's deliveries go to the worker"),
        }
    }

    pub(crate) fn relay(&self) -> &Relay {
        &self.relay
    }

    /// Frames waiting on `id`'s `lane`, undelivered.
    pub(crate) fn pending(&self, id: PartyId, lane: Lane) -> usize {
        let Some(lanes) = &self.parties[id.0].lanes else {
            return 0;
        };
        match lane {
            Lane::Ack => lanes.ack.len(),
            Lane::Ctrl => lanes.ctrl.len(),
            Lane::Data => lanes.data.len(),
        }
    }

    /// The space root daemon party `id` works in.
    pub(crate) fn space_root_of(&self, id: PartyId) -> &Path {
        match &self.parties[id.0].role {
            Role::Daemon { space_root, .. } => space_root,
            Role::Scripted { .. } => panic!("a scripted party has no space root"),
        }
    }

    /// The state daemon party `id` has persisted, read from its `.kutl`.
    pub(crate) fn persisted_state(&self, id: PartyId) -> DaemonState {
        DaemonState::load_readonly(&self.space_root_of(id).join(".kutl"))
    }

    /// Placements the workers of `space` have landed on disk so far: files
    /// written where a registration put them or moved where a rename put
    /// them. A document that settles once costs one; one that moves and
    /// moves back costs three.
    pub(crate) fn placements(space: &str) -> u64 {
        space_counter("kutl_daemon_placements_total", space)
    }

    /// Times the workers of `space` were told their signal stream was paused
    /// and re-subscribed to resume it.
    pub(crate) fn stream_pauses(space: &str) -> u64 {
        space_counter("kutl_daemon_signal_stream_pauses_total", space)
    }

    /// Lifecycle metadata stamped by party `id`'s own clock, for the frames
    /// a scripted party authors.
    pub(crate) fn stamp(&mut self, id: PartyId, intent: &str) -> ChangeMetadata {
        let party = &mut self.parties[id.0];
        let wall = kutl_core::env::now_ms_u64();
        let stamp = party.clock.tick(wall);
        ChangeMetadata {
            timestamp: kutl_core::ms_u64_to_i64_saturating(wall),
            author_did: party.did.clone(),
            intent: intent.into(),
            hlc: Some(stamp.into()),
            ..Default::default()
        }
    }

    /// Mint a bearer for `id` through the actor's own challenge flow: the
    /// same nonce, signature and token validation the HTTP surface runs.
    async fn mint_token(&mut self, id: PartyId) -> String {
        let (did, key) = {
            let party = &self.parties[id.0];
            (party.did.clone(), party.key.clone())
        };
        let (reply, challenge) = oneshot::channel();
        self.relay
            .process_command(RelayCommand::AuthChallenge {
                did: did.clone(),
                reply,
            })
            .await;
        let challenge = challenge
            .await
            .expect("challenge reply")
            .expect("challenge issued");
        let nonce = URL_SAFE_NO_PAD
            .decode(&challenge.nonce)
            .expect("nonce decodes");
        let signature = URL_SAFE_NO_PAD.encode(key.sign(&nonce).to_bytes());
        let (reply, verified) = oneshot::channel();
        self.relay
            .process_command(RelayCommand::AuthVerify {
                did,
                nonce: challenge.nonce,
                signature,
                reply,
            })
            .await;
        verified
            .await
            .expect("verify reply")
            .expect("signature verifies")
            .token
    }

    /// Connect `id` at the relay and queue its handshake as the connection's
    /// first frame. A daemon party's session is accepted first: the worker
    /// opens one as soon as it runs (or as soon as its reconnect backoff
    /// elapses on the paused clock), and its ends replace any earlier
    /// session's.
    pub(crate) async fn connect(&mut self, id: PartyId) {
        if let Role::Daemon { sessions, live, .. } = &mut self.parties[id.0].role {
            *live = Some(sessions.recv().await.expect("the worker opens a session"));
        }
        let (data_tx, data) = mpsc::channel(self.outbound_capacity);
        let (ctrl_tx, ctrl) = mpsc::channel(CTRL_LANE_CAPACITY);
        let (ack_tx, ack) = mpsc::unbounded_channel();
        let conn_id = self.parties[id.0].conn_id;
        self.relay
            .process_command(RelayCommand::Connect {
                conn_id,
                tx: data_tx,
                ctrl_tx,
                ack_tx,
            })
            .await;
        self.parties[id.0].lanes = Some(Lanes { data, ctrl, ack });
        let token = self.mint_token(id).await;
        let party = &mut self.parties[id.0];
        let handshake = handshake_envelope_with_token(&party.did, &token, "");
        self.send(id, &handshake);
    }

    /// Queue `envelope` as a frame `id` sent. The relay processes it on a
    /// later [`Step::Process`].
    pub(crate) fn send(&mut self, id: PartyId, envelope: &SyncEnvelope) {
        let party = &mut self.parties[id.0];
        enqueue(
            party.conn_id,
            &mut party.to_relay,
            &mut party.sent,
            &kutl_proto::protocol::encode_envelope(envelope),
        );
    }

    /// Move every command daemon party `id` has produced onto the wire.
    fn pump(party: &mut Party) {
        let Party {
            conn_id,
            space_id,
            to_relay,
            sent,
            role,
            ..
        } = party;
        let Role::Daemon {
            live: Some(ends), ..
        } = role
        else {
            return;
        };
        while let Ok(cmd) = ends.cmd_rx.try_recv() {
            let bytes = outbound_frame(cmd, space_id, &ends.blob_backlog);
            enqueue(*conn_id, to_relay, sent, &bytes);
        }
    }

    fn pump_all(&mut self) {
        for party in &mut self.parties {
            Self::pump(party);
        }
    }

    /// The relay processes the next frame `id` sent; `false` when none waits.
    pub(crate) async fn process(&mut self, id: PartyId) -> bool {
        Self::pump(&mut self.parties[id.0]);
        let Some(cmd) = self.parties[id.0].to_relay.pop_front() else {
            return false;
        };
        self.relay.process_command(cmd).await;
        self.settled = false;
        true
    }

    /// The relay processes every frame `id` has sent so far.
    pub(crate) async fn process_all(&mut self, id: PartyId) {
        while self.process(id).await {}
    }

    /// `id` receives the next frame on `lane`; `false` when none waits. A
    /// worker decodes it into its events exactly as the socket client would,
    /// a scripted party keeps it.
    pub(crate) async fn deliver(&mut self, id: PartyId, lane: Lane) -> bool {
        let party = &mut self.parties[id.0];
        let Some(bytes) = party.lanes.as_mut().and_then(|lanes| lanes.take(lane)) else {
            return false;
        };
        self.settled = false;
        let party = &mut self.parties[id.0];
        match &mut party.role {
            Role::Scripted { received } => {
                if let Some(payload) = decode_envelope(&bytes)
                    .expect("a relay frame decodes")
                    .payload
                {
                    received.push(payload);
                }
            }
            Role::Daemon { live: None, .. } => {
                // A dead session's socket: the frame goes nowhere.
            }
            Role::Daemon {
                live: Some(ends), ..
            } => {
                let envelope = decode_envelope(&bytes).expect("a relay frame decodes");
                // The handshake ack is the one frame the socket client
                // consumes before its read loop starts.
                if let Some(Payload::HandshakeAck(ack)) = envelope.payload {
                    let _ = ends.event_tx.send(handshake_event(&ack)).await;
                } else {
                    let _ = handle_inbound(&bytes, &ends.event_tx).await;
                }
            }
        }
        true
    }

    /// Drain `id`'s lanes in the connection task's order until all are empty.
    pub(crate) async fn deliver_all(&mut self, id: PartyId) {
        'drain: loop {
            for lane in Lane::PRIORITY {
                if self.deliver(id, lane).await {
                    continue 'drain;
                }
            }
            return;
        }
    }

    /// Let every task run until nothing is runnable.
    pub(crate) async fn quiesce(&mut self) {
        tokio::time::sleep(QUIESCE_TICK).await;
    }

    /// The next move the default schedule makes, after the workers have
    /// reacted to everything so far: the relay processes queued frames
    /// before any lane delivers (a relay ahead of its clients' reads), in
    /// party order, and lanes drain in the connection task's order. An idle
    /// wire gets one settle, so a worker's deferred work can move it again;
    /// `None` once it stays idle through one.
    pub(crate) async fn next_step(&mut self) -> Option<Step> {
        self.quiesce().await;
        self.pump_all();
        for (index, party) in self.parties.iter().enumerate() {
            if !party.to_relay.is_empty() {
                return Some(Step::Process(PartyId(index)));
            }
        }
        for (index, party) in self.parties.iter().enumerate() {
            if let Some(lanes) = &party.lanes {
                for lane in Lane::PRIORITY {
                    if lanes.has(lane) {
                        return Some(Step::Deliver(PartyId(index), lane));
                    }
                }
            }
        }
        (!self.settled).then_some(Step::Settle)
    }

    pub(crate) async fn take(&mut self, step: Step) {
        match step {
            Step::Process(id) => {
                self.process(id).await;
            }
            Step::Deliver(id, lane) => {
                self.deliver(id, lane).await;
            }
            Step::Settle => self.settle().await,
        }
    }

    /// Let [`SETTLE_WINDOW`] of virtual time pass. Under the paused clock the
    /// sleep advances to each timer in deadline order and runs its task
    /// before moving on, so the workers' timers fire one at a time, the same
    /// way every run.
    pub(crate) async fn settle(&mut self) {
        tokio::time::sleep(SETTLE_WINDOW).await;
        self.settled = true;
    }

    /// Run the default schedule until the wire is idle, returning the steps
    /// taken.
    pub(crate) async fn run_to_idle(&mut self) -> usize {
        let mut steps = 0;
        while let Some(step) = self.next_step().await {
            self.take(step).await;
            steps += 1;
            assert!(
                steps < STEP_CEILING,
                "the schedule did not settle within {STEP_CEILING} steps"
            );
        }
        steps
    }

    /// Cut `id`'s connection: the relay sees the disconnect, the lanes are
    /// gone, and a worker learns of it as the socket client would report it.
    /// The worker's reconnect opens a new session, which the next
    /// [`Self::connect`] accepts.
    pub(crate) async fn disconnect(&mut self, id: PartyId) {
        let conn_id = self.parties[id.0].conn_id;
        self.relay
            .process_command(RelayCommand::Disconnect { conn_id })
            .await;
        let party = &mut self.parties[id.0];
        party.lanes = None;
        party.to_relay.clear();
        if let Role::Daemon { live, .. } = &mut party.role
            && let Some(ends) = live.take()
        {
            let _ = ends.event_tx.send(SyncEvent::Disconnected).await;
        }
    }

    /// Stop daemon party `id` and return how its worker exited. The worker
    /// saves its state on the way out, so [`Self::persisted_state`] is
    /// current afterwards.
    pub(crate) async fn stop(&mut self, id: PartyId) -> Result<()> {
        let Role::Daemon { cancel, task, .. } = &mut self.parties[id.0].role else {
            panic!("only a worker can be stopped");
        };
        cancel.cancel();
        let task = task.take().expect("a worker stops once");
        tokio::time::timeout(STOP_TIMEOUT, task)
            .await
            .expect("the worker exits on cancellation")
            .expect("the worker task did not panic")
    }
}

impl Drop for Interleaver {
    /// No worker outlives its harness: one left running would reconnect
    /// into a relay that is gone, forever, and its logs and counters would
    /// land in the runs after it.
    fn drop(&mut self) {
        for party in &mut self.parties {
            if let Role::Daemon { cancel, task, .. } = &mut party.role {
                cancel.cancel();
                if let Some(task) = task.take() {
                    task.abort();
                }
            }
        }
    }
}

/// A short name for a payload, for assertions over a sent sequence.
pub(crate) fn payload_kind(payload: &Payload) -> &'static str {
    match payload {
        Payload::Handshake(_) => "Handshake",
        Payload::SubscribeSignals(_) => "SubscribeSignals",
        Payload::ListSpaceDocuments(_) => "ListSpaceDocuments",
        Payload::Subscribe(_) => "Subscribe",
        Payload::RegisterDocument(_) => "RegisterDocument",
        Payload::RenameDocument(_) => "RenameDocument",
        Payload::UnregisterDocument(_) => "UnregisterDocument",
        Payload::SyncOps(_) => "SyncOps",
        Payload::SignalReseed(_) => "SignalReseed",
        _ => "other",
    }
}

/// The event a handshake ack yields the worker, as the socket client would
/// report it: `Connected`, or the refusal it carries.
fn handshake_event(ack: &kutl_proto::sync::HandshakeAck) -> SyncEvent {
    if let Some(e) = &ack.error {
        let refusal = kutl_proto::protocol::handshake_refusal(e);
        return SyncEvent::HandshakeRejected {
            message: refusal.message,
            auth_failed: refusal.auth_failed,
        };
    }
    if let Err(gap) = kutl_proto::protocol::verify_ack_versions(ack) {
        return SyncEvent::HandshakeRejected {
            message: gap,
            auth_failed: false,
        };
    }
    connected_event(ack)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lone worker starts a session: handshake, then listening before the
    /// snapshot, then the wire goes idle; stopping it persists its state.
    #[tokio::test(start_paused = true)]
    async fn test_lone_worker_session_reaches_idle() {
        let space = uuid::Uuid::new_v4().to_string();
        let mut h = Interleaver::new(RelayConfig::default().outbound_capacity, false);
        let root = h.space_root();
        std::fs::write(root.join("note.md"), "hello\n").unwrap();
        let daemon = h.add_daemon(root.clone(), &space);
        h.connect(daemon).await;

        let steps = h.run_to_idle().await;
        assert!(steps > 0, "the session took no steps");

        let kinds: Vec<&'static str> = h.sent(daemon).iter().map(payload_kind).collect();
        assert_eq!(
            &kinds[..3],
            ["Handshake", "SubscribeSignals", "ListSpaceDocuments"],
            "{kinds:?}"
        );
        assert!(
            kinds.contains(&"RegisterDocument"),
            "the local file was registered: {kinds:?}"
        );
        assert!(
            h.relay()
                .registry(&space)
                .is_some_and(|r| r.get_by_path("note.md").is_some())
        );

        h.stop(daemon).await.unwrap();
        let state = h.persisted_state(daemon);
        assert!(
            state.documents.contains_key("note.md"),
            "{:?}",
            state.documents.keys()
        );
    }
}
