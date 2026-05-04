//! Relay-mediated simulation — tests the full daemon → relay → daemon sync path.
//!
//! Unlike the peer-to-peer [`Simulation`](crate::Simulation), this orchestrator
//! drives a real [`Relay`] actor synchronously via [`Relay::process_command`], with
//! simulated daemons connected through a [`VirtualNetwork`] for configurable
//! latency, loss, and partitions.

use std::sync::Arc;

use kutl_core::{AgentId, Document, Result};
use kutl_proto::protocol::{
    PROTOCOL_VERSION_MAJOR, PROTOCOL_VERSION_MINOR, decode_envelope, encode_envelope,
    sync_ops_envelope,
};
use kutl_proto::sync::sync_envelope::Payload;
use kutl_relay::config::{DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS, RelayConfig};
use kutl_relay::relay::{ConnId, Relay, RelayCommand};
use tokio::sync::mpsc;

use crate::PeerId;
use crate::clock::VirtualClock;
use crate::link::LinkConfig;
use crate::network::{Message, SendResult, VirtualNetwork};
use crate::prng::{net_rng, peer_rng};
use crate::sim_env::SimEnv;

/// [`PeerId`] assigned to the relay in the virtual network.
const RELAY_PEER_ID: PeerId = 0;

/// Capacity of the mpsc channel from relay to each daemon.
const DAEMON_CHANNEL_CAPACITY: usize = 64;

/// Capacity of the control channel from relay to each daemon.
const DAEMON_CTRL_CAPACITY: usize = 8;

/// Re-use the crate-level quiescent tick limit.
use crate::MAX_QUIESCENT_TICKS;

/// Index into the simulation's daemon list.
pub type DaemonId = usize;

/// Relay-mediated simulation orchestrator.
///
/// Drives a real [`Relay`] actor synchronously via [`Relay::process_command`],
/// with simulated daemons sending and receiving through a [`VirtualNetwork`].
pub struct RelaySimulation {
    relay: Relay,
    clock: Arc<VirtualClock>,
    network: VirtualNetwork,
    daemons: Vec<SimDaemon>,
    /// Receivers for relay → daemon outbound data messages.
    daemon_rx: Vec<mpsc::Receiver<Vec<u8>>>,
    /// Receivers for relay → daemon control messages (stale notices).
    daemon_ctrl_rx: Vec<mpsc::Receiver<Vec<u8>>>,
    /// `ConnId` assigned to each daemon in the relay.
    conn_ids: Vec<ConnId>,
    /// Next `ConnId` to assign.
    next_conn_id: ConnId,
    /// Version each daemon last sent to the relay (for delta encoding).
    last_sent: Vec<Vec<usize>>,
    seed: u64,
}

/// A simulated daemon peer.
struct SimDaemon {
    name: String,
    doc: Document,
    agent: AgentId,
    space_id: String,
    doc_id: String,
    connected: bool,
}

impl RelaySimulation {
    /// Create a new relay-mediated simulation with the given PRNG seed.
    pub fn new(seed: u64) -> Self {
        let config = RelayConfig {
            host: "sim".into(),
            port: 0,
            relay_name: "sim-relay".into(),
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
        Self {
            relay: Relay::new_standalone(config),
            clock: VirtualClock::new(0),
            network: VirtualNetwork::new(net_rng(seed)),
            daemons: Vec::new(),
            daemon_rx: Vec::new(),
            daemon_ctrl_rx: Vec::new(),
            conn_ids: Vec::new(),
            next_conn_id: 1,
            last_sent: Vec::new(),
            seed,
        }
    }

    /// Add a daemon peer, auto-handshake, and subscribe to the given document.
    ///
    /// If the relay already has content for this document (from other daemons),
    /// the new daemon receives catch-up ops immediately.
    pub fn add_daemon(&mut self, name: &str, space_id: &str, doc_id: &str) -> Result<DaemonId> {
        let id = self.daemons.len();
        let rng = peer_rng(self.seed, id);
        let env = SimEnv::new(Arc::clone(&self.clock), rng);
        let mut doc = Document::with_env(env);
        let agent = doc.register_agent(name)?;

        self.daemons.push(SimDaemon {
            name: name.to_owned(),
            doc,
            agent,
            space_id: space_id.to_owned(),
            doc_id: doc_id.to_owned(),
            connected: true,
        });

        // Create mpsc channels for relay → daemon (data + control).
        let (tx, rx) = mpsc::channel(DAEMON_CHANNEL_CAPACITY);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(DAEMON_CTRL_CAPACITY);
        self.daemon_rx.push(rx);
        self.daemon_ctrl_rx.push(ctrl_rx);

        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;
        self.conn_ids.push(conn_id);
        self.last_sent.push(Vec::new());

        // Register connection with relay.
        pollster::block_on(self.relay.process_command(RelayCommand::Connect {
            conn_id,
            tx,
            ctrl_tx,
        }));

        // Handshake.
        pollster::block_on(self.relay.process_command(RelayCommand::Handshake {
            conn_id,
            msg: kutl_proto::sync::Handshake {
                protocol_version_major: PROTOCOL_VERSION_MAJOR,
                protocol_version_minor: PROTOCOL_VERSION_MINOR,
                client_name: name.to_owned(),
                features: Vec::new(),
                auth_token: String::new(),
                display_name: String::new(),
            },
        }));

        // Subscribe.
        pollster::block_on(self.relay.process_command(RelayCommand::Subscribe {
            conn_id,
            msg: kutl_proto::sync::Subscribe {
                space_id: space_id.to_owned(),
                document_id: doc_id.to_owned(),
            },
        }));

        // Drain any catch-up messages from relay (HandshakeAck + possible catch-up ops).
        self.drain_relay_to_daemon(id);

        Ok(id)
    }

    /// Insert `text` at `pos` in a daemon's document and send ops to relay.
    ///
    /// # Panics
    ///
    /// Panics if the insert fails (indicates a bug in the simulation setup).
    pub fn insert(&mut self, daemon: DaemonId, pos: usize, text: &str) {
        let d = &mut self.daemons[daemon];
        let agent = d.agent;
        let name = d.name.clone();
        d.doc
            .edit(agent, &name, "insert", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(pos, text)
            })
            .expect("insert failed");

        self.send_daemon_ops(daemon);
    }

    /// Delete `len` characters at `pos` in a daemon's document and send ops.
    ///
    /// # Panics
    ///
    /// Panics if the delete fails (indicates a bug in the simulation setup).
    pub fn delete(&mut self, daemon: DaemonId, pos: usize, len: usize) {
        let d = &mut self.daemons[daemon];
        let agent = d.agent;
        let name = d.name.clone();
        d.doc
            .edit(agent, &name, "delete", kutl_core::Boundary::Auto, |ctx| {
                ctx.delete(pos..pos + len)
            })
            .expect("delete failed");

        self.send_daemon_ops(daemon);
    }

    /// Send any pending ops from a daemon to the relay.
    ///
    /// Useful after healing a partition to flush queued edits.
    pub fn sync_daemon(&mut self, daemon: DaemonId) {
        self.send_daemon_ops(daemon);
    }

    /// Send pending ops from all daemons to the relay.
    pub fn sync_all_daemons(&mut self) {
        for daemon in 0..self.daemons.len() {
            self.send_daemon_ops(daemon);
        }
    }

    /// Advance the clock by one tick and process the delivery cycle.
    pub fn tick(&mut self) {
        self.clock.tick();
        self.deliver();
    }

    /// Tick until the network is quiescent (no pending messages).
    ///
    /// # Panics
    ///
    /// Panics if quiescence is not reached within [`MAX_QUIESCENT_TICKS`] ticks.
    pub fn run_until_quiescent(&mut self) {
        self.deliver();
        let mut guard = 0;
        while !self.network.is_quiescent() {
            self.tick();
            guard += 1;
            assert!(
                guard < MAX_QUIESCENT_TICKS,
                "run_until_quiescent exceeded {MAX_QUIESCENT_TICKS} ticks"
            );
        }
    }

    /// Return a daemon's document content.
    #[must_use]
    pub fn content(&self, daemon: DaemonId) -> String {
        self.daemons[daemon].doc.content()
    }

    /// Return the number of daemons.
    #[must_use]
    pub fn daemon_count(&self) -> usize {
        self.daemons.len()
    }

    /// Assert all daemons have converged to the same content.
    ///
    /// # Panics
    ///
    /// Panics if any two daemons have different content.
    pub fn assert_converged(&self) {
        if self.daemons.len() < 2 {
            return;
        }
        let reference = self.daemons[0].doc.content();
        for d in self.daemons.iter().skip(1) {
            let content = d.doc.content();
            assert_eq!(
                reference, content,
                "daemon '{}' diverged from '{}': {:?} vs {:?}",
                d.name, self.daemons[0].name, content, reference,
            );
        }
    }

    /// Create a network partition between a daemon and the relay.
    pub fn partition(&mut self, daemon: DaemonId) {
        let pid = daemon_peer_id(daemon);
        self.network.partition(pid, RELAY_PEER_ID);
    }

    /// Heal a network partition between a daemon and the relay.
    pub fn heal(&mut self, daemon: DaemonId) {
        let pid = daemon_peer_id(daemon);
        self.network.heal(pid, RELAY_PEER_ID);
    }

    /// Heal all partitions.
    pub fn heal_all(&mut self) {
        self.network.heal_all();
    }

    /// Configure the link from a daemon to the relay.
    pub fn configure_link_to_relay(&mut self, daemon: DaemonId, config: LinkConfig) {
        let pid = daemon_peer_id(daemon);
        self.network.configure_link(pid, RELAY_PEER_ID, config);
    }

    /// Configure the link from the relay to a daemon.
    pub fn configure_link_from_relay(&mut self, daemon: DaemonId, config: LinkConfig) {
        let pid = daemon_peer_id(daemon);
        self.network.configure_link(RELAY_PEER_ID, pid, config);
    }

    /// Disconnect a daemon from the relay.
    ///
    /// The daemon's document is preserved, but the relay drops the connection
    /// and unsubscribes it from all documents. Messages in flight may still
    /// be delivered from the network, but the relay will ignore them.
    ///
    /// # Panics
    ///
    /// Panics if the daemon is already disconnected.
    pub fn disconnect_daemon(&mut self, daemon: DaemonId) {
        assert!(
            self.daemons[daemon].connected,
            "daemon '{}' is already disconnected",
            self.daemons[daemon].name,
        );
        let conn_id = self.conn_ids[daemon];
        pollster::block_on(
            self.relay
                .process_command(RelayCommand::Disconnect { conn_id }),
        );
        self.daemons[daemon].connected = false;
    }

    /// Reconnect a previously disconnected daemon to the relay.
    ///
    /// Re-creates the mpsc channel, assigns a new `ConnId`, performs
    /// handshake + subscribe, and delivers catch-up ops so the daemon
    /// has the latest document state.
    ///
    /// # Panics
    ///
    /// Panics if the daemon is already connected.
    pub fn reconnect_daemon(&mut self, daemon: DaemonId) {
        assert!(
            !self.daemons[daemon].connected,
            "daemon '{}' is already connected",
            self.daemons[daemon].name,
        );

        // New channels for relay → daemon (data + control).
        let (tx, rx) = mpsc::channel(DAEMON_CHANNEL_CAPACITY);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(DAEMON_CTRL_CAPACITY);
        self.daemon_rx[daemon] = rx;
        self.daemon_ctrl_rx[daemon] = ctrl_rx;

        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;
        self.conn_ids[daemon] = conn_id;

        // Register + handshake + subscribe (same as add_daemon).
        pollster::block_on(self.relay.process_command(RelayCommand::Connect {
            conn_id,
            tx,
            ctrl_tx,
        }));

        let d = &self.daemons[daemon];
        pollster::block_on(self.relay.process_command(RelayCommand::Handshake {
            conn_id,
            msg: kutl_proto::sync::Handshake {
                protocol_version_major: PROTOCOL_VERSION_MAJOR,
                protocol_version_minor: PROTOCOL_VERSION_MINOR,
                client_name: d.name.clone(),
                features: Vec::new(),
                auth_token: String::new(),
                display_name: String::new(),
            },
        }));

        pollster::block_on(self.relay.process_command(RelayCommand::Subscribe {
            conn_id,
            msg: kutl_proto::sync::Subscribe {
                space_id: d.space_id.clone(),
                document_id: d.doc_id.clone(),
            },
        }));

        self.daemons[daemon].connected = true;

        // Drain catch-up (HandshakeAck + ops).
        self.drain_relay_to_daemon(daemon);

        // The daemon may have made local edits while disconnected that the
        // relay doesn't have. Send a full sync (encode_full) to the relay so it
        // gets all ops. CRDT merge is idempotent, so sending ops the relay
        // already has is harmless.
        let d = &self.daemons[daemon];
        let ops = d.doc.encode_full();
        if !ops.is_empty() {
            let metadata = d.doc.changes_since(&[]);
            let conn_id = self.conn_ids[daemon];
            pollster::block_on(self.relay.process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(kutl_proto::sync::SyncOps {
                    space_id: d.space_id.clone(),
                    document_id: d.doc_id.clone(),
                    ops,
                    metadata,
                    ..Default::default()
                }),
            }));
            // Drain any relayed ops to other daemons.
            self.drain_relay_outbound();
        }

        self.last_sent[daemon] = self.daemons[daemon].doc.local_version();
    }

    /// Check whether a daemon is currently connected to the relay.
    #[must_use]
    pub fn is_connected(&self, daemon: DaemonId) -> bool {
        self.daemons[daemon].connected
    }

    /// Return the character count of a daemon's document.
    #[must_use]
    pub fn document_size(&self, daemon: DaemonId) -> usize {
        self.daemons[daemon].doc.len()
    }

    /// Return all daemon IDs.
    #[must_use]
    pub fn daemon_ids(&self) -> Vec<DaemonId> {
        (0..self.daemons.len()).collect()
    }

    // ---- internal helpers ----

    /// Encode the daemon's pending ops and inject into the network toward the relay.
    ///
    /// No-op if the daemon is disconnected.
    fn send_daemon_ops(&mut self, daemon: DaemonId) {
        if !self.daemons[daemon].connected {
            return;
        }
        let d = &self.daemons[daemon];
        let since = &self.last_sent[daemon];

        let ops = d.doc.encode_since(since);
        if ops.is_empty() {
            return;
        }

        let metadata = d.doc.changes_since(since);
        let envelope = sync_ops_envelope(&d.space_id, &d.doc_id, ops, metadata);
        let bytes = encode_envelope(&envelope);

        let new_version = d.doc.local_version();
        let tick = self.clock.now();
        let pid = daemon_peer_id(daemon);

        let result = self.network.send(Message {
            from: pid,
            to: RELAY_PEER_ID,
            dt_bytes: bytes,
            changes: Vec::new(),
            delivery_tick: tick,
        });

        // Only advance last_sent if the message was enqueued (not dropped).
        if matches!(result, SendResult::Enqueued) {
            self.last_sent[daemon] = new_version;
        }
    }

    /// Deliver ready network messages and process them.
    fn deliver(&mut self) {
        let now = self.clock.now();
        let ready = self.network.deliver_ready(now);

        // Deliver messages TO the relay, draining outbound after each
        // to prevent channel overflow (mirrors production's concurrent drain).
        if let Some(relay_messages) = ready.get(&RELAY_PEER_ID) {
            for msg in relay_messages {
                self.deliver_to_relay(msg);
                self.drain_relay_outbound();
            }
        }

        // Deliver messages TO daemons.
        for (&peer_id, messages) in &ready {
            if peer_id == RELAY_PEER_ID {
                continue;
            }
            let daemon_id = peer_id_to_daemon(peer_id);
            for msg in messages {
                self.deliver_to_daemon(daemon_id, msg);
            }
        }
    }

    /// Deliver a network message to the relay.
    ///
    /// Only `SyncOps` payloads are dispatched; the simulation never sends
    /// `Handshake` or `Subscribe` via the network (those go directly in
    /// [`add_daemon`](Self::add_daemon)).
    fn deliver_to_relay(&mut self, msg: &Message) {
        let envelope =
            decode_envelope(&msg.dt_bytes).expect("simulation produced invalid envelope bytes");

        match envelope.payload {
            Some(Payload::SyncOps(sync_ops)) => {
                let daemon_id = peer_id_to_daemon(msg.from);
                let conn_id = self.conn_ids[daemon_id];
                pollster::block_on(self.relay.process_command(RelayCommand::InboundSyncOps {
                    conn_id,
                    msg: Box::new(sync_ops),
                }));
            }
            other => panic!("unexpected payload in daemon→relay message: {other:?}"),
        }
    }

    /// Deliver a network message to a daemon (merge ops into its document).
    ///
    /// Only `SyncOps` payloads are merged; `HandshakeAck` is expected during
    /// setup but not during tick delivery. `Error` payloads indicate a relay
    /// bug and cause a panic.
    fn deliver_to_daemon(&mut self, daemon: DaemonId, msg: &Message) {
        let envelope =
            decode_envelope(&msg.dt_bytes).expect("simulation produced invalid envelope bytes");

        match envelope.payload {
            Some(Payload::SyncOps(sync_ops)) => {
                if let Err(e) = self.daemons[daemon]
                    .doc
                    .merge(&sync_ops.ops, &sync_ops.metadata)
                {
                    panic!("daemon '{}' merge failed: {e}", self.daemons[daemon].name);
                }
            }
            Some(Payload::HandshakeAck(_)) => {} // expected, ignore
            other => panic!("unexpected payload in relay→daemon message: {other:?}"),
        }
    }

    /// Drain all relay → daemon mpsc channels and inject messages into the network.
    fn drain_relay_outbound(&mut self) {
        let now = self.clock.now();
        for daemon in 0..self.daemon_rx.len() {
            while let Ok(bytes) = self.daemon_rx[daemon].try_recv() {
                let pid = daemon_peer_id(daemon);
                self.network.send(Message {
                    from: RELAY_PEER_ID,
                    to: pid,
                    dt_bytes: bytes,
                    changes: Vec::new(),
                    delivery_tick: now,
                });
            }
            // Drain control channel (stale notices) — not forwarded via network.
            while self.daemon_ctrl_rx[daemon].try_recv().is_ok() {}
        }
    }

    /// Drain relay outbound directly to a daemon, bypassing the network.
    ///
    /// Used during [`add_daemon`](Self::add_daemon) to deliver catch-up ops
    /// without waiting for network ticks.
    fn drain_relay_to_daemon(&mut self, daemon: DaemonId) {
        while let Ok(bytes) = self.daemon_rx[daemon].try_recv() {
            let envelope = decode_envelope(&bytes).expect("relay produced invalid envelope bytes");
            if let Some(Payload::SyncOps(sync_ops)) = envelope.payload
                && let Err(e) = self.daemons[daemon]
                    .doc
                    .merge(&sync_ops.ops, &sync_ops.metadata)
            {
                panic!(
                    "daemon '{}' catch-up merge failed: {e}",
                    self.daemons[daemon].name
                );
            }
        }
    }
}

/// Convert [`DaemonId`] to [`PeerId`] in the virtual network.
///
/// Relay is [`RELAY_PEER_ID`] (0), daemons start at 1.
fn daemon_peer_id(daemon: DaemonId) -> PeerId {
    daemon + 1
}

/// Convert [`PeerId`] back to [`DaemonId`].
fn peer_id_to_daemon(pid: PeerId) -> DaemonId {
    pid - 1
}
