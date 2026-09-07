//! WebSocket sync client that connects to a kutl relay.
//!
//! The client handles handshake, subscribe, and bidirectional
//! `SyncOps` relay. It communicates with the daemon core via channels.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    RegisterDocumentMetadata, SIGNAL_RESEED_CAPABILITY, blob_ops_envelope, decode_envelope,
    encode_envelope, is_blob_mode, list_space_documents_envelope, register_document_envelope,
    rename_document_envelope, subscribe_envelope, sync_ops_envelope, unregister_document_envelope,
};
use kutl_proto::sync::{ChangeMetadata, StaleReason, StaleStreamReason, sync_envelope::Payload};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Type alias for the WS sink half.
type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;
/// Type alias for the WS stream half.
type WsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Interval between client→relay WebSocket pings. The relay answers pongs at
/// the protocol layer, so any healthy transport produces inbound traffic at
/// least this often even when both sides are idle — which is what arms the
/// liveness window below. Cheap (an empty control frame).
const PING_INTERVAL: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(10));

/// How long the inbound side may stay COMPLETELY silent before the session is
/// declared dead and torn down for a reconnect. Three missed ping/pong rounds:
/// a laptop-sleep wake, a NAT rebind, or a silently dropped peer leaves a
/// half-open socket that produces no read error — without this deadline an
/// IDLE daemon (nothing to write) never notices and stays stranded forever.
const LIVENESS_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(30));

/// Messages from the sync client to the daemon core.
#[derive(Debug)]
pub enum SyncEvent {
    /// Successfully connected and handshake acknowledged. Carries the relay's
    /// advertised signing identity (`HandshakeAck.relay_did`; empty when the
    /// relay declares none), which the worker PINS for the session so
    /// signal-record attestations can be checked against it (advisory), plus
    /// whether the relay accepts client-pushed signal history
    /// (`HandshakeAck.features`, the `signal-reseed` capability).
    Connected {
        /// The relay's `did:key` signing identity from the handshake ack, or
        /// empty when the relay advertises none (graceful degrade).
        relay_did: String,
        /// Whether the ack's `features` include the `signal-reseed` capability
        /// — the worker pushes its surplus records only when this is true.
        accepts_reseed: bool,
    },
    /// Connection lost.
    Disconnected,
    /// Remote ops received for a subscribed document.
    RemoteOps {
        document_id: String,
        ops: Vec<u8>,
        metadata: Vec<ChangeMetadata>,
        /// 0 = text (default), 2 = blob.
        content_mode: i32,
        /// SHA-256 hash for blob content; empty for text.
        content_hash: Vec<u8>,
        /// Uncapped agent→author-DID snapshot the relay attaches on the
        /// catch-up (join) envelope; empty on incremental deltas. Installed
        /// insert-if-absent so blame survives change-record eviction on a
        /// re-seeded joiner.
        author_by_agent_snapshot: std::collections::HashMap<String, String>,
    },
    /// A document was registered in the registry (from another daemon).
    DocumentRegistered {
        document_id: String,
        path: String,
        /// Origin HLC of the registration, for lifecycle ordering.
        hlc: Option<kutl_core::Hlc>,
    },
    /// A document was renamed in the registry (from another daemon).
    DocumentRenamed {
        document_id: String,
        old_path: String,
        new_path: String,
        /// Origin HLC of the rename, for lifecycle ordering.
        hlc: Option<kutl_core::Hlc>,
        /// Causal floor: the `registered_hlc` the renamer observed. Lets this
        /// daemon's placement lattice recognize the rename as causally-after a
        /// clock-skewed registration (else its own placement would undo it).
        rename_causal_floor: Option<kutl_core::Hlc>,
    },
    /// A document was unregistered from the registry (from another daemon).
    DocumentUnregistered {
        document_id: String,
        /// Origin HLC of the unregister, for lifecycle ordering.
        hlc: Option<kutl_core::Hlc>,
    },
    /// The relay acknowledged a register/rename THIS daemon sent, carrying the
    /// persisted document's post-arbitration effective path (the typed-ack
    /// rail). When the effective path differs from where this daemon holds the
    /// document, its own op lost a path collision and was conflict-copied — the
    /// daemon reconciles its local file to `effective_path`. `None` on a failure
    /// ack or an ack without a persisted entry (e.g. unregister).
    LifecycleAck {
        document_id: String,
        effective_path: Option<String>,
        /// Liveness HLC of the persisted document, used to freshness-gate the
        /// ack's self-correction so a late/reordered ack can't move the document
        /// against a causally-newer rename already applied. `None` on a failure
        /// ack or one without a persisted entry.
        hlc: Option<kutl_core::Hlc>,
    },
    /// Relay responded with the list of documents in a space.
    SpaceDocuments {
        space_id: String,
        documents: Vec<(String, String)>, // (document_id, path)
    },
    /// The relay evicted this daemon as a stale subscriber for a document
    /// because its bounded outbound `data` lane overflowed (the daemon fell
    /// behind a flood of broadcasts). The relay has already removed the
    /// subscription; the daemon must re-subscribe to recover, which replays the
    /// full current document state via the relay's catch-up.
    StaleSubscriber {
        /// The document this daemon was evicted from.
        document_id: String,
        /// Why the relay dropped the subscription.
        reason: StaleReason,
    },
    /// The relay could not serve this daemon's signal stream in a space: it
    /// paused the stream (the shared outbound lane was too full to take a
    /// signal frame), refused the subscribe, or could not load the records
    /// for the backlog page. `cause` tells the three apart, and the worker's
    /// remedy differs for each; a session that treats any of them as a
    /// diagnostic goes deaf to signals until it reconnects.
    StaleSignalStream {
        /// The space whose signal stream was paused or refused.
        space_id: String,
        /// Why, as the verdict the worker branches on.
        cause: StaleStreamReason,
        /// The relay's prose, for the log line.
        reason: String,
    },
    /// A broadcast signal record. The relay fans one out per appended record;
    /// the worker ingests it into the space's local segments and advances its
    /// catch-up cursor. A record with an empty `record_id` is a legacy bare
    /// broadcast (not durable) — the ingest path drops it, mirroring the
    /// fold's empty-`record_id` rule.
    ///
    /// Boxed: a `Signal` is far larger than the other variants, so keeping it
    /// inline would bloat every `SyncEvent` on the channel.
    SignalRecord(Box<kutl_proto::sync::Signal>),
    /// One backlog page answering a [`SyncCommand::SubscribeSignals`].
    ///
    /// `more` means the relay has records past `cursor`; the worker re-sends
    /// `SubscribeSignals` carrying that cursor. The cursor is the RELAY's
    /// high-water for the page, never re-derived from the last record — a page
    /// is `max` records plus the whole trailing group sharing its
    /// `physical_ms`, so re-deriving would split a millisecond group and lose
    /// the remainder.
    SignalPage(Box<kutl_proto::sync::SignalPage>),
    /// A `SignalAck` from the relay, correlated by `client_ref`.
    ///
    /// Both outcomes are carried. The chunked re-seed walk releases its next
    /// chunk on a SUCCESSFUL ack for the chunk it has in flight, so success is
    /// load-bearing; a failure is either that walk's abort signal or a submit
    /// refusal the session surfaces as an error. The refusal is rendered to
    /// prose here: the walk aborts on any refusal and a submit refusal is
    /// only logged, so neither branches on the code.
    SignalAck {
        client_ref: String,
        success: bool,
        error: Option<String>,
    },
    /// The relay refused the handshake — do not retry. A rejected token, or a
    /// protocol major this pairing cannot bridge in either direction. Named
    /// for the handshake rather than for auth because a version mismatch
    /// reported as an auth failure sends the reader after credentials that
    /// were never the problem.
    ///
    /// `auth_failed` carries the relay's own verdict — its `ErrorCode` — so
    /// surfaces can tell "the bearer you presented was refused" from every
    /// other refusal, which no credential change fixes. Read the code, never
    /// the prose: `message` is display text and its wording is not a contract.
    HandshakeRejected {
        /// Human-readable refusal from the relay's `Error` payload.
        message: String,
        /// The relay answered `ErrorCode::AuthFailed` — the presented bearer
        /// token was refused.
        auth_failed: bool,
    },
    /// An error message from the relay (non-fatal, session may continue).
    ///
    /// `auth_failed` is set when the relay returned `ErrorCode::AuthFailed` —
    /// i.e. the DID is authenticated but NOT authorized for this space (the
    /// relay's `authorized_keys` allowlist doesn't list it). The
    /// daemon stays connected but can never subscribe/sync, so this must be
    /// surfaced with the operator remedy rather than logged as a generic error.
    Error {
        /// Human-readable message from the relay's `Error` payload.
        message: String,
        /// The relay returned `ErrorCode::AuthFailed` (unauthorized DID).
        auth_failed: bool,
    },
}

/// Messages from the daemon core to the sync client.
#[derive(Debug)]
pub enum SyncCommand {
    /// Subscribe to a document on the relay.
    Subscribe { document_id: String },
    /// Send local ops to the relay.
    SendOps {
        document_id: String,
        ops: Vec<u8>,
        metadata: Vec<ChangeMetadata>,
        /// 0 = text (default), 2 = blob.
        content_mode: i32,
        /// SHA-256 hash for blob content; empty for text.
        content_hash: Vec<u8>,
    },
    /// Register a new document in the relay registry.
    RegisterDocument {
        space_id: String,
        document_id: String,
        path: String,
        metadata: Option<ChangeMetadata>,
        /// Filesystem birthtime in Unix millis. `None`
        /// when the platform/filesystem doesn't expose it or when
        /// the register isn't sourced from a file on disk.
        originally_created_at_ms: Option<i64>,
    },
    /// Rename a document in the relay registry.
    RenameDocument {
        space_id: String,
        document_id: String,
        old_path: String,
        new_path: String,
        metadata: Option<ChangeMetadata>,
        /// Causal floor: the `registered_hlc` the renamer observed for this doc.
        /// Lets the relay's lattice recognize the rename as causally-after a
        /// clock-skewed (future-stamped) registration. `None` if unresolved.
        rename_causal_floor: Option<kutl_core::Hlc>,
    },
    /// Unregister a document from the relay registry.
    UnregisterDocument {
        space_id: String,
        document_id: String,
        metadata: Option<ChangeMetadata>,
    },
    /// Request the list of documents in a space from the relay.
    ListSpaceDocuments { space_id: String },
    /// Listen to the space, and ask for the backlog page after `cursor`.
    ///
    /// Two jobs in one frame, which is why the daemon sends it first thing on
    /// connect and again per page. It puts this connection in the relay's
    /// listener set — the one set behind space-audience signals, lifecycle
    /// broadcasts (register/rename/unregister) and the presence
    /// `list_participants` reports; a connection that never sends it receives
    /// none of those — and it answers with ONE `SignalPage`. The client
    /// re-sends with the returned cursor to walk the rest; a re-send keeps
    /// the same entry, so the subscription is continuous.
    SubscribeSignals {
        space_id: String,
        cursor: Option<kutl_proto::sync::Hlc>,
    },
    /// Push signal history this daemon holds and the relay lacks.
    ///
    /// Fire-and-forget: nothing awaits the outcome (it is only logged).
    /// A REJECTION still arrives, as a `SignalAck` with `success: false`, and is
    /// surfaced as an error — a silently-dropped push would let this daemon
    /// believe history it holds is safely replicated when it is not.
    SignalReseed {
        space_id: String,
        client_ref: String,
        records: Vec<kutl_proto::sync::Signal>,
    },
}

impl SyncCommand {
    /// Short variant name for log lines, mirroring `Effect::name` and for the
    /// same reason: `SendOps` carries a whole file in `ops`, and `SignalReseed`
    /// a batch of records, so rendering the command itself turns one debug line
    /// into megabytes of formatted bytes for every upload. Interesting fields
    /// are logged at the sites that care about them.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Subscribe { .. } => "Subscribe",
            Self::SendOps { .. } => "SendOps",
            Self::RegisterDocument { .. } => "RegisterDocument",
            Self::RenameDocument { .. } => "RenameDocument",
            Self::UnregisterDocument { .. } => "UnregisterDocument",
            Self::ListSpaceDocuments { .. } => "ListSpaceDocuments",
            Self::SubscribeSignals { .. } => "SubscribeSignals",
            Self::SignalReseed { .. } => "SignalReseed",
        }
    }
}

/// Connect to a relay, perform handshake, and run the bidirectional relay.
///
/// Returns when the connection is closed or an unrecoverable error occurs.
/// The caller should handle reconnection with backoff.
#[expect(
    clippy::too_many_arguments,
    reason = "distinct connection params + the shared blob-backlog metric counter; \
              bundling the connection args into a struct would only obscure call sites"
)]
pub async fn run_client(
    relay_url: &str,
    space_id: &str,
    client_name: &str,
    auth_token: &str,
    display_name: &str,
    cmd_rx: mpsc::UnboundedReceiver<SyncCommand>,
    event_tx: mpsc::Sender<SyncEvent>,
    blob_backlog: Arc<AtomicI64>,
) -> Result<()> {
    info!(relay_url, "connecting to relay");

    // The shared connect-and-handshake (`kutl_client::connect_and_handshake`)
    // carries the relay's codec caps: the catch-up can carry blob messages
    // up to the wire cap, and a smaller codec cap rejects them on delivery
    // and wedges the subscription.
    let ws =
        match kutl_client::connect_and_handshake(relay_url, client_name, auth_token, display_name)
            .await
        {
            Ok((ws, ack)) => {
                let _ = event_tx.send(connected_event(&ack)).await;
                ws
            }
            // A verdict from the relay (a refusal or a version gap) is a
            // rejection the session acts on; a transport failure rendered no
            // verdict and is reported like any connect failure.
            Err(
                e @ (kutl_client::HandshakeError::Refused { .. }
                | kutl_client::HandshakeError::VersionGap(_)),
            ) => {
                let auth_failed = matches!(
                    e,
                    kutl_client::HandshakeError::Refused {
                        auth_failed: true,
                        ..
                    }
                );
                let _ = event_tx
                    .send(SyncEvent::HandshakeRejected {
                        message: e.to_string(),
                        auth_failed,
                    })
                    .await;
                return Err(e.into());
            }
            Err(kutl_client::HandshakeError::Transport(e)) => {
                return Err(e.context(format!("failed to connect to {relay_url}")));
            }
        };
    let (ws_sink, ws_stream) = ws.split();

    relay_loop(
        ws_sink,
        ws_stream,
        space_id,
        cmd_rx,
        &event_tx,
        blob_backlog,
    )
    .await;

    let _ = event_tx.send(SyncEvent::Disconnected).await;
    Ok(())
}

/// The event an accepted handshake yields: what the worker pins and gates
/// on. The relay's advertised signing identity (empty when the relay
/// declares none — graceful degrade) and the capability that gates the push
/// half of signal catch-up. The pull half is not gated: the daemon always
/// listens to its space, and a relay without a record log answers with an
/// empty page.
///
/// The one place a `Connected` event is built, so the socket client and an
/// in-process session driven by a test read the same ack the same way.
pub(crate) fn connected_event(ack: &kutl_proto::sync::HandshakeAck) -> SyncEvent {
    let accepts_reseed = ack.features.iter().any(|f| f == SIGNAL_RESEED_CAPABILITY);
    info!(
        relay_name = %ack.relay_name,
        relay_did = %ack.relay_did,
        accepts_reseed,
        "handshake accepted"
    );
    SyncEvent::Connected {
        relay_did: ack.relay_did.clone(),
        accepts_reseed,
    }
}

/// Bidirectional relay split into independent read and write tasks so a blocked
/// socket write can never starve ack reads. Both tasks bind to
/// a child of a per-session [`CancellationToken`]; whichever half ends first
/// cancels the session, which tears down the sibling. A half-open socket
/// therefore cannot leak a task: `relay_loop` awaits both halves before
/// returning, so `ws_sink`/`ws_stream` are dropped before `run_client` emits
/// [`SyncEvent::Disconnected`].
async fn relay_loop(
    ws_sink: WsSink,
    ws_stream: WsStream,
    space_id: &str,
    cmd_rx: mpsc::UnboundedReceiver<SyncCommand>,
    event_tx: &mpsc::Sender<SyncEvent>,
    blob_backlog: Arc<AtomicI64>,
) {
    let session = CancellationToken::new();

    // Write task: owns ws_sink, drains cmd_rx, parks only here on a slow socket.
    let mut write_handle = tokio::spawn(write_task(
        ws_sink,
        cmd_rx,
        space_id.to_owned(),
        blob_backlog,
        session.child_token(),
    ));

    // Read task: owns ws_stream, pumps inbound frames into SyncEvents.
    let mut read_handle = tokio::spawn(read_task(
        ws_stream,
        event_tx.clone(),
        session.child_token(),
    ));

    // Whichever half finishes first cancels the session; cancelling the parent
    // token cancels both child tokens, so the surviving half observes its
    // `cancelled()` arm and returns. Poll the finished handle by `&mut` (so the
    // survivor is NOT dropped — a dropped JoinHandle detaches without awaiting),
    // then await ONLY the survivor. A JoinHandle panics if polled after it
    // returned `Ready`, so the already-finished half must not be re-awaited.
    // Either way both halves are run to completion before return, so no task is
    // leaked and both socket halves are dropped before `Disconnected` is sent.
    let survivor = tokio::select! {
        _ = &mut write_handle => read_handle,
        _ = &mut read_handle => write_handle,
    };
    session.cancel();
    let _ = survivor.await;
}

/// Drain daemon commands to the socket. A full or slow socket parks the daemon
/// here only, never on the read half. The write half ending (channel closed or
/// a write failure) returns, which lets `relay_loop` cancel the session and
/// tear down the read half.
async fn write_task(
    mut ws_sink: WsSink,
    mut cmd_rx: mpsc::UnboundedReceiver<SyncCommand>,
    space_id: String,
    blob_backlog: Arc<AtomicI64>,
    token: CancellationToken,
) {
    // The liveness ping (see PING_INTERVAL): keeps inbound pongs flowing on an
    // idle session so the read half's silence deadline only fires on a dead
    // transport. A failed ping write is itself the disconnect signal.
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = token.cancelled() => break,
            _ = ping.tick() => {
                if let Err(e) = ws_sink
                    .send(tungstenite::Message::Ping(tungstenite::Bytes::default()))
                    .await
                {
                    warn!(error = %e, "ws ping failed, disconnecting");
                    break;
                }
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(cmd) => {
                        let frame = outbound_frame(cmd, &space_id, &blob_backlog);
                        if ws_sink
                            .send(tungstenite::Message::Binary(frame.into()))
                            .await
                            .is_err()
                        {
                            warn!("ws write failed, disconnecting");
                            break;
                        }
                    }
                    None => {
                        // Command channel closed — daemon is shutting down.
                        break;
                    }
                }
            }
        }
    }
}

/// Pump inbound frames into [`SyncEvent`]s. Independent of the write half, so a
/// stalled socket write cannot delay ack reads. The read half ending (close
/// frame, ws error, or stream end) returns, which lets `relay_loop` cancel the
/// session and tear down the write half.
async fn read_task(
    mut ws_stream: WsStream,
    event_tx: mpsc::Sender<SyncEvent>,
    token: CancellationToken,
) {
    // Liveness deadline: ANY inbound frame (data, pong, close) proves the
    // transport alive and re-arms it. The write half pings every
    // PING_INTERVAL and the relay auto-pongs, so a healthy idle session never
    // trips this; a half-open socket (sleep/wake, NAT rebind, silent peer
    // death) produces no frames and no read error — this deadline is the only
    // way an idle daemon ever notices and reconnects.
    let mut deadline = tokio::time::Instant::now() + LIVENESS_TIMEOUT;
    loop {
        tokio::select! {
            () = token.cancelled() => break,
            () = tokio::time::sleep_until(deadline) => {
                warn!(timeout = ?LIVENESS_TIMEOUT, "relay silent past the liveness window, disconnecting");
                break;
            }
            msg = ws_stream.next() => {
                deadline = tokio::time::Instant::now() + LIVENESS_TIMEOUT;
                match msg {
                    Some(Ok(tungstenite::Message::Binary(bytes))) => {
                        tracing::debug!(len = bytes.len(), "received binary frame from relay");
                        if let Err(e) = handle_inbound(&bytes, &event_tx).await {
                            warn!(error = %e, "failed to handle inbound message");
                        }
                    }
                    Some(Ok(tungstenite::Message::Close(_))) => {
                        info!("relay sent close frame");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        warn!(error = %e, "ws read error");
                        break;
                    }
                    None => {
                        info!("ws stream ended");
                        break;
                    }
                }
            }
        }
    }
}

/// Encode one daemon command as the frame the relay receives, clearing a
/// blob send from the upload backlog as it leaves the queue.
///
/// The one place a command becomes bytes, so the socket client and an
/// in-process session driven by a test put the same frame on the wire.
pub(crate) fn outbound_frame(
    cmd: SyncCommand,
    space_id: &str,
    blob_backlog: &AtomicI64,
) -> Vec<u8> {
    tracing::debug!(cmd = cmd.name(), "sending command to relay");
    let blob_mode = i32::from(kutl_proto::sync::ContentMode::Blob);
    if matches!(&cmd, SyncCommand::SendOps { content_mode, .. } if *content_mode == blob_mode) {
        blob_backlog.fetch_sub(1, Ordering::Relaxed);
    }
    encode_envelope(&command_to_envelope(cmd, space_id))
}

/// Convert a `SyncCommand` into a wire envelope.
fn command_to_envelope(cmd: SyncCommand, space_id: &str) -> kutl_proto::sync::SyncEnvelope {
    match cmd {
        SyncCommand::Subscribe { document_id } => subscribe_envelope(space_id, &document_id),
        SyncCommand::SendOps {
            document_id,
            ops,
            metadata,
            content_mode,
            content_hash,
        } => {
            if content_mode == i32::from(kutl_proto::sync::ContentMode::Blob) {
                blob_ops_envelope(
                    space_id,
                    &document_id,
                    ops,
                    content_hash,
                    metadata.into_iter().next(),
                )
            } else {
                // Daemon-outbound edit delta: bindings ride the per-change
                // `metadata`; the uncapped author snapshot is a catch-up-only
                // field the relay populates.
                sync_ops_envelope(
                    space_id,
                    &document_id,
                    ops,
                    metadata,
                    std::collections::HashMap::new(),
                )
            }
        }
        SyncCommand::RegisterDocument {
            space_id: sid,
            document_id,
            path,
            metadata,
            originally_created_at_ms,
        } => register_document_envelope(
            &sid,
            &document_id,
            &path,
            metadata,
            RegisterDocumentMetadata {
                originally_created_at_ms,
                ..Default::default()
            },
        ),
        SyncCommand::RenameDocument {
            space_id: sid,
            document_id,
            old_path,
            new_path,
            metadata,
            rename_causal_floor,
        } => rename_document_envelope(
            &sid,
            &document_id,
            &old_path,
            &new_path,
            metadata,
            rename_causal_floor.map(Into::into),
        ),
        SyncCommand::UnregisterDocument {
            space_id: sid,
            document_id,
            metadata,
        } => unregister_document_envelope(&sid, &document_id, metadata),
        SyncCommand::ListSpaceDocuments { space_id } => list_space_documents_envelope(&space_id),
        SyncCommand::SubscribeSignals { space_id, cursor } => {
            kutl_proto::protocol::subscribe_signals_envelope(&space_id, cursor)
        }
        SyncCommand::SignalReseed {
            space_id,
            client_ref,
            records,
        } => kutl_proto::protocol::signal_reseed_envelope(&client_ref, &space_id, records),
    }
}

/// Extract the origin HLC from a lifecycle message's metadata, if present and
/// well-formed. A `None` actor or malformed bytes degrade to `None` (the daemon
/// then applies the event without HLC ordering — the pre-HLC fallback).
fn hlc_of(metadata: Option<&ChangeMetadata>) -> Option<kutl_core::Hlc> {
    metadata
        .and_then(|m| m.hlc.clone())
        .and_then(|h| kutl_core::Hlc::try_from(h).ok())
}

/// Build and forward a [`SyncEvent::LifecycleAck`] from a typed register/rename
/// ack's components. Both ack message types carry the same `document_id` /
/// `entry` / wire-HLC shape, so this de-duplicates their two arms in
/// [`handle_inbound`] — including the entry→effective-path projection. The wire
/// HLC degrades to `None` if absent or malformed.
async fn send_lifecycle_ack(
    event_tx: &mpsc::Sender<SyncEvent>,
    document_id: String,
    entry: Option<kutl_proto::sync::RegistryEntry>,
    wire_hlc: Option<kutl_proto::sync::Hlc>,
) {
    let hlc = wire_hlc.and_then(|h| kutl_core::Hlc::try_from(h).ok());
    let _ = event_tx
        .send(SyncEvent::LifecycleAck {
            document_id,
            effective_path: entry.map(|e| e.path),
            hlc,
        })
        .await;
}

/// The event a document listing yields: a refused listing is the refusal it
/// carries, never an empty space, so an unauthorized DID surfaces the
/// operator remedy instead of syncing against nothing.
fn space_documents_event(result: kutl_proto::sync::ListSpaceDocumentsResult) -> SyncEvent {
    if let Some(e) = &result.error {
        return error_event(e);
    }
    SyncEvent::SpaceDocuments {
        space_id: result.space_id,
        documents: result
            .documents
            .into_iter()
            .map(|d| (d.document_id, d.path))
            .collect(),
    }
}

/// Map a relay refusal to a [`SyncEvent::Error`], flagging an
/// authorization rejection.
///
/// `AuthFailed` here means the DID authenticated but is not authorized for the
/// space (unlisted in the relay's `authorized_keys`) — flag it so the
/// daemon surfaces the operator remedy instead of logging a generic error and
/// silently never syncing.
fn error_event(e: &kutl_proto::sync::Error) -> SyncEvent {
    // Same classifier as the handshake refusal — the two cannot disagree about
    // what counts as an auth rejection, or about how a payload renders.
    let refusal = kutl_proto::protocol::handshake_refusal(e);
    SyncEvent::Error {
        message: refusal.message,
        auth_failed: refusal.auth_failed,
    }
}

/// Forward the inbound signal frames as worker events, ignoring anything else.
///
/// Grouped because they are one concern arriving four ways: a live broadcast, a
/// backlog page, a refused submit, and the notice that the stream could not be
/// served. Reached as the caller's fallthrough arm, so every frame this daemon
/// does not otherwise handle lands here and is dropped by the final arm.
async fn forward_signal_frame(payload: Payload, event_tx: &mpsc::Sender<SyncEvent>) {
    match payload {
        // A broadcast signal record. An empty `record_id`
        // (a legacy bare broadcast) is forwarded and dropped at ingest.
        Payload::Signal(record) => {
            let _ = event_tx
                .send(SyncEvent::SignalRecord(Box::new(record)))
                .await;
        }
        // The backlog page answering a `SubscribeSignals`.
        Payload::SignalPage(page) => {
            let _ = event_tx.send(SyncEvent::SignalPage(Box::new(page))).await;
        }
        // The relay could not serve the stream — unauthorized, records failed
        // to load, or the data lane overflowed. Surfaced as an error rather
        // than swallowed: dropping it would leave the worker waiting for a page
        // that is never coming, and catch-up would stall silently.
        // BOTH outcomes are forwarded. The chunked re-seed walk awaits a
        // successful ack — it releases the next chunk only when the relay
        // confirms the last. A refusal still carries its reason, and the
        // session decides whether it belongs to a re-seed in progress or is a
        // submit failure to surface.
        Payload::SignalAck(ack) => {
            let _ = event_tx
                .send(SyncEvent::SignalAck {
                    client_ref: ack.client_ref,
                    success: ack.success,
                    error: ack
                        .error
                        .as_ref()
                        .map(|e| kutl_proto::protocol::handshake_refusal(e).message),
                })
                .await;
        }
        Payload::StaleSignalStream(s) => {
            let _ = event_tx
                .send(SyncEvent::StaleSignalStream {
                    cause: s.cause(),
                    space_id: s.space_id,
                    reason: s.reason,
                })
                .await;
        }
        // A compromised read classification is fail-closed everywhere: the
        // relay sent no slot, no subscription, and no catch-up, so silence
        // here would make a wounded document indistinguishable from an
        // empty one. Surface it; Found/Empty statuses need no action (the
        // catch-up, or genuine emptiness, follows on its own).
        Payload::SubscribeStatus(s) => {
            if let Some(e) = &s.error {
                // The relay refused the subscribe itself: no slot, no
                // subscription, no catch-up.
                let refusal = kutl_proto::protocol::handshake_refusal(e);
                let _ = event_tx
                    .send(SyncEvent::Error {
                        message: format!(
                            "subscribe refused for document {}: {}",
                            s.document_id, refusal.message
                        ),
                        auth_failed: refusal.auth_failed,
                    })
                    .await;
            } else if let Some(reason) = kutl_proto::protocol::subscribe_status_failure(&s) {
                let _ = event_tx
                    .send(SyncEvent::Error {
                        message: format!(
                            "subscribe refused for document {}: {reason}; no catch-up will follow",
                            s.document_id
                        ),
                        auth_failed: false,
                    })
                    .await;
            }
        }
        // Every other frame: not this daemon's concern.
        _ => {}
    }
}

/// Process an inbound binary frame from the relay into the worker's events.
///
/// The handshake ack is not handled here: the connect sequence consumes it
/// before the read loop starts (see [`connected_event`]).
pub(crate) async fn handle_inbound(bytes: &[u8], event_tx: &mpsc::Sender<SyncEvent>) -> Result<()> {
    let envelope = decode_envelope(bytes).context("failed to decode SyncEnvelope")?;

    match envelope.payload {
        Some(Payload::SyncOps(ops)) => {
            let blob_mode = is_blob_mode(&ops);
            let event = SyncEvent::RemoteOps {
                document_id: ops.document_id,
                content_mode: ops.content_mode,
                content_hash: if blob_mode {
                    ops.content_hash
                } else {
                    Vec::new()
                },
                ops: ops.ops,
                metadata: ops.metadata,
                author_by_agent_snapshot: ops.author_by_agent_snapshot,
            };
            let _ = event_tx.send(event).await;
        }
        Some(Payload::RegisterDocument(msg)) => {
            let hlc = hlc_of(msg.metadata.as_ref());
            let _ = event_tx
                .send(SyncEvent::DocumentRegistered {
                    document_id: msg.document_id,
                    path: msg.path,
                    hlc,
                })
                .await;
        }
        Some(Payload::RenameDocument(msg)) => {
            let hlc = hlc_of(msg.metadata.as_ref());
            let rename_causal_floor = msg
                .rename_causal_floor
                .and_then(|w| kutl_core::Hlc::try_from(w).ok());
            let _ = event_tx
                .send(SyncEvent::DocumentRenamed {
                    document_id: msg.document_id,
                    old_path: msg.old_path,
                    new_path: msg.new_path,
                    hlc,
                    rename_causal_floor,
                })
                .await;
        }
        Some(Payload::UnregisterDocument(msg)) => {
            let hlc = hlc_of(msg.metadata.as_ref());
            let _ = event_tx
                .send(SyncEvent::DocumentUnregistered {
                    document_id: msg.document_id,
                    hlc,
                })
                .await;
        }
        // Typed acks for THIS daemon's own register/rename. Surface
        // the persisted effective path so the daemon can reconcile its local
        // file when its op lost a path collision (conflict-copy). Both ack types
        // carry the same `document_id`/`entry`/`hlc` shape, so they share one arm.
        Some(Payload::RegisterDocumentAck(ack)) => {
            send_lifecycle_ack(event_tx, ack.document_id, ack.entry, ack.hlc).await;
        }
        Some(Payload::RenameDocumentAck(ack)) => {
            send_lifecycle_ack(event_tx, ack.document_id, ack.entry, ack.hlc).await;
        }
        // An unregister ack carries no path to reconcile (the document is gone),
        // but it IS the typed ack for THIS daemon's own `UnregisterDocument`.
        // Surface it as a `LifecycleAck` with no effective path so the daemon's
        // ack handler clears its outstanding-command backlog for the unregister;
        // `handle_lifecycle_ack` then early-returns (no path → nothing to move).
        Some(Payload::UnregisterDocumentAck(ack)) => {
            let _ = event_tx
                .send(SyncEvent::LifecycleAck {
                    document_id: ack.document_id,
                    effective_path: None,
                    hlc: None,
                })
                .await;
        }
        Some(Payload::ListSpaceDocumentsResult(result)) => {
            let _ = event_tx.send(space_documents_event(result)).await;
        }
        // The relay evicted this daemon from a document because its outbound
        // `data` lane overflowed (proper backpressure). Surface it so
        // the daemon loop re-subscribes and recovers the missed broadcasts via
        // catch-up; dropping it here would strand the document forever.
        Some(Payload::StaleSubscriber(s)) => {
            let _ = event_tx
                .send(SyncEvent::StaleSubscriber {
                    reason: s.reason(),
                    document_id: s.document_id,
                })
                .await;
        }
        // A merge reject names the document it refuses so the log line (and
        // any recovery) targets that document rather than the whole session.
        Some(Payload::SyncOpsRejected(r)) => {
            let refusal = kutl_proto::protocol::handshake_refusal(&r.error.unwrap_or_default());
            let _ = event_tx
                .send(SyncEvent::Error {
                    message: format!(
                        "sync ops rejected for document {} in space {}: {}",
                        r.document_id, r.space_id, refusal.message
                    ),
                    auth_failed: refusal.auth_failed,
                })
                .await;
        }
        Some(payload) => forward_signal_frame(payload, event_tx).await,
        None => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    /// Build an encoded `SyncOpsRejected` envelope with the given code + message.
    fn error_envelope(code: kutl_proto::sync::ErrorCode, message: &str) -> Vec<u8> {
        encode_envelope(&kutl_proto::protocol::sync_ops_rejected_envelope(
            "space",
            "doc",
            kutl_proto::sync::Error {
                code: code.into(),
                message: message.to_owned(),
                ..Default::default()
            },
        ))
    }

    /// A relay `Error` with `AuthFailed` must reach the daemon flagged
    /// `auth_failed`, so a subscribe-authz rejection is surfaced with the
    /// operator remedy rather than as a generic error.
    #[tokio::test]
    async fn test_inbound_auth_failed_error_is_flagged() {
        let (tx, mut rx) = mpsc::channel::<SyncEvent>(1);
        let bytes = error_envelope(
            kutl_proto::sync::ErrorCode::AuthFailed,
            "not authorized for space s1",
        );
        handle_inbound(&bytes, &tx).await.unwrap();

        match rx.recv().await {
            Some(SyncEvent::Error {
                message,
                auth_failed,
            }) => {
                assert!(auth_failed, "AuthFailed code must set the auth_failed flag");
                assert!(message.contains("not authorized"));
            }
            other => panic!("expected SyncEvent::Error, got {other:?}"),
        }
    }

    /// A refused document listing is the refusal it carries, not an empty
    /// space: the daemon must surface the operator remedy rather than sync
    /// against nothing and wait forever.
    #[tokio::test]
    async fn test_inbound_refused_listing_is_the_refusal_not_an_empty_space() {
        let (tx, mut rx) = mpsc::channel(4);
        let bytes = encode_envelope(&kutl_proto::sync::SyncEnvelope {
            payload: Some(Payload::ListSpaceDocumentsResult(
                kutl_proto::sync::ListSpaceDocumentsResult {
                    space_id: "space".into(),
                    documents: Vec::new(),
                    error: Some(kutl_proto::sync::Error {
                        code: kutl_proto::sync::ErrorCode::AuthFailed.into(),
                        message: "did not authorized for space".into(),
                        ..Default::default()
                    }),
                },
            )),
        });
        handle_inbound(&bytes, &tx).await.unwrap();
        drop(tx);
        match rx.recv().await {
            Some(SyncEvent::Error { auth_failed, .. }) => {
                assert!(auth_failed, "the listing's refusal keeps its verdict");
            }
            other => panic!("expected SyncEvent::Error, got {other:?}"),
        }
        assert!(
            rx.recv().await.is_none(),
            "no SpaceDocuments follows a refused listing"
        );
    }

    /// A merge reject surfaces as an error scoped to the document and space
    /// it names, so the log line is actionable rather than a bare message.
    #[tokio::test]
    async fn test_inbound_sync_ops_rejected_names_its_document() {
        let (tx, mut rx) = mpsc::channel::<SyncEvent>(1);
        let bytes = error_envelope(
            kutl_proto::sync::ErrorCode::InvalidMessage,
            "unmergeable history",
        );
        handle_inbound(&bytes, &tx).await.unwrap();
        match rx.recv().await {
            Some(SyncEvent::Error {
                message,
                auth_failed,
            }) => {
                assert!(!auth_failed, "a merge reject is not an auth failure");
                assert!(
                    message.contains("document doc in space space")
                        && message.contains("unmergeable history"),
                    "the message names the document and space: {message}"
                );
            }
            other => panic!("expected SyncEvent::Error, got {other:?}"),
        }
    }

    /// A non-auth relay `Error` (e.g. `QuotaExceeded`) must NOT be flagged
    /// `auth_failed` — only an authorization rejection gets the remedy log.
    #[tokio::test]
    async fn test_inbound_non_auth_error_is_not_flagged() {
        let (tx, mut rx) = mpsc::channel::<SyncEvent>(1);
        let bytes = error_envelope(kutl_proto::sync::ErrorCode::QuotaExceeded, "quota exceeded");
        handle_inbound(&bytes, &tx).await.unwrap();

        match rx.recv().await {
            Some(SyncEvent::Error { auth_failed, .. }) => {
                assert!(
                    !auth_failed,
                    "a non-auth error must not be flagged auth_failed"
                );
            }
            other => panic!("expected SyncEvent::Error, got {other:?}"),
        }
    }

    /// The relay split's teardown contract: both halves bind to a child of a
    /// per-session token, so cancelling the session must tear down BOTH halves.
    /// A half-open socket (one half wedges on a quiet/blocked socket) cannot
    /// leak a task — this locks the per-session child-token semantics that
    /// `relay_loop` relies on to guarantee no leaked half-open task.
    #[tokio::test]
    async fn test_session_cancel_tears_down_both_halves() {
        let session = CancellationToken::new();

        // Stand-in read half: parks until cancelled (models a quiet socket).
        let read = tokio::spawn({
            let token = session.child_token();
            async move {
                token.cancelled().await;
                "read-done"
            }
        });
        // Stand-in write half: parks until cancelled (models a wedged sink).
        let write = tokio::spawn({
            let token = session.child_token();
            async move {
                token.cancelled().await;
                "write-done"
            }
        });

        // Either half ending cancels the session; here we cancel directly.
        session.cancel();

        let (r, w) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(read, write)
        })
        .await
        .expect("both halves must tear down on session cancel");
        assert_eq!(r.expect("read half must not panic"), "read-done");
        assert_eq!(w.expect("write half must not panic"), "write-done");
    }

    /// The relay's `UnregisterDocumentAck` must surface as a `LifecycleAck`
    /// so the driver's `sync_backlog` can decrement. Without this arm the ack
    /// falls through `_ => {}` and the unregister stays counted forever — the
    /// watchdog then error-logs a stall that is really a counter leak, not a
    /// real stall.
    #[tokio::test]
    async fn test_unregister_ack_yields_lifecycle_ack() {
        use kutl_proto::sync::sync_envelope::Payload;
        use kutl_proto::sync::{SyncEnvelope, UnregisterDocumentAck};
        use prost::Message;

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SyncEvent>(4);
        let env = SyncEnvelope {
            payload: Some(Payload::UnregisterDocumentAck(UnregisterDocumentAck {
                space_id: "475e1f0c-14a9-4b83-8b73-95c80cfd166d".to_owned(),
                document_id: "doc-1".to_owned(),
                error: None,
                success: true,
            })),
        };
        let bytes = env.encode_to_vec();
        handle_inbound(&bytes, &tx)
            .await
            .expect("handle_inbound ok");
        match rx.try_recv() {
            Ok(SyncEvent::LifecycleAck {
                document_id,
                effective_path,
                hlc,
            }) => {
                assert_eq!(document_id, "doc-1");
                assert_eq!(
                    effective_path, None,
                    "unregister ack carries no effective path"
                );
                assert_eq!(hlc, None, "unregister ack carries no liveness HLC");
            }
            other => panic!("expected LifecycleAck, got {other:?}"),
        }
    }

    /// A `Payload::Signal` broadcast must surface as a
    /// `SyncEvent::SignalRecord` so the worker can ingest it into local
    /// segments. Without this arm the record falls through `_ => {}` in
    /// `handle_inbound` and is silently eaten — no daemon would ever persist a
    /// broadcast record.
    #[tokio::test]
    async fn test_signal_broadcast_yields_signal_record() {
        use kutl_proto::sync::sync_envelope::Payload;
        use kutl_proto::sync::{Signal, SyncEnvelope};
        use prost::Message;

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SyncEvent>(4);
        let env = SyncEnvelope {
            payload: Some(Payload::Signal(Signal {
                id: "sig-1".to_owned(),
                space_id: "475e1f0c-14a9-4b83-8b73-95c80cfd166d".to_owned(),
                record_id: "rec-1".to_owned(),
                ..Default::default()
            })),
        };
        let bytes = env.encode_to_vec();
        handle_inbound(&bytes, &tx)
            .await
            .expect("handle_inbound ok");
        match rx.try_recv() {
            Ok(SyncEvent::SignalRecord(record)) => {
                assert_eq!(record.record_id, "rec-1");
                assert_eq!(record.id, "sig-1");
            }
            other => panic!("expected SignalRecord, got {other:?}"),
        }
    }
}
