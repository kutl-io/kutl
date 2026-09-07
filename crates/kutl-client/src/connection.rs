//! WebSocket connection and sync protocol management.

use std::collections::{HashSet, VecDeque};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    blob_ops_envelope, decode_envelope, encode_envelope, handshake_envelope_with_token,
    register_document_envelope, subscribe_envelope, subscribe_signals_envelope, sync_ops_envelope,
};
// Re-exported at the crate root as `kutl_client::RegisterDocumentMetadata`
// so external callers can build the bundle without depending on
// kutl-proto directly.
pub use kutl_proto::protocol::RegisterDocumentMetadata;
use kutl_proto::sync::{self, sync_envelope::Payload};
use tokio_tungstenite::tungstenite;

use crate::recovery::RecoveryConfig;

/// The relay socket every Rust client speaks over.
pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Why a relay handshake did not complete, split so a caller acts on the
/// relay's own verdict rather than its prose.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    /// The relay's ack carried a refusal. `auth_failed` is its
    /// `ErrorCode::AuthFailed` verdict, the one refusal that names a
    /// credential to change; `message` is the relay's text.
    #[error("{message}")]
    Refused { message: String, auth_failed: bool },
    /// The ack's protocol version is outside what this client speaks;
    /// retrying cannot heal it.
    #[error("{0}")]
    VersionGap(String),
    /// Transport, decode, or an unexpected frame: no verdict arrived.
    #[error(transparent)]
    Transport(#[from] anyhow::Error),
}

/// Connect to `url` with the relay's codec caps and complete the handshake:
/// the ONE connect-and-handshake sequence for every Rust relay client (the
/// sync client, the daemon, the watch session, the one-shot CLI
/// connections), so a codec or handshake fix lands once. Returns the socket
/// whole and the ack, so each caller reads the features it cares about and
/// renders a refusal in its own voice. No connect timeout is applied here;
/// a caller that needs one wraps the call.
pub async fn connect_and_handshake(
    url: &str,
    client_name: &str,
    auth_token: &str,
    display_name: &str,
) -> std::result::Result<(WsStream, sync::HandshakeAck), HandshakeError> {
    let (mut ws, _) =
        tokio_tungstenite::connect_async_with_config(url, Some(relay_ws_config()), false)
            .await
            .context("failed to connect to relay")?;
    let envelope = handshake_envelope_with_token(client_name, auth_token, display_name);
    ws.send(tungstenite::Message::Binary(
        encode_envelope(&envelope).into(),
    ))
    .await
    .context("failed to send handshake")?;
    let reply = loop {
        let msg = ws
            .next()
            .await
            .context("connection closed before handshake ack")?
            .context("ws error during handshake")?;
        match msg {
            tungstenite::Message::Binary(bytes) => {
                break decode_envelope(&bytes).context("failed to decode handshake response")?;
            }
            tungstenite::Message::Close(_) => {
                return Err(anyhow::anyhow!("connection closed before handshake ack").into());
            }
            _ => {}
        }
    };
    match reply.payload {
        Some(Payload::HandshakeAck(ack)) => {
            if let Some(e) = &ack.error {
                let refusal = kutl_proto::protocol::handshake_refusal(e);
                return Err(HandshakeError::Refused {
                    message: refusal.message,
                    auth_failed: refusal.auth_failed,
                });
            }
            kutl_proto::protocol::verify_ack_versions(&ack).map_err(HandshakeError::VersionGap)?;
            Ok((ws, ack))
        }
        other => Err(anyhow::anyhow!("expected HandshakeAck, got {other:?}").into()),
    }
}

/// Auth header passed into the WebSocket handshake.
struct AuthHeader {
    token: String,
    display_name: String,
}

/// Timeout for waiting on a subscribe catch-up response.
///
/// 500ms is generous for a LAN/localhost round-trip. If the relay has content,
/// the catch-up `SyncOps` arrives in the first frame after subscribe.
/// A timeout just means the document was empty — not an error.
const SUBSCRIBE_CATCHUP_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_millis(500));

/// Maximum number of frames `subscribe` reads while waiting for catch-up.
///
/// The relay multiplexes control and data over one socket, so frames for
/// other channels (another doc's ops, a signal, presence) can interleave
/// ahead of this subscribe's catch-up `SyncOps`. We read a bounded number
/// of frames, buffering non-catch-up frames for `recv` to drain, before
/// giving up and treating the document as empty. The bound prevents an
/// unbounded read loop if the catch-up never arrives.
const SUBSCRIBE_CATCHUP_MAX_FRAMES: usize = 4;

/// The refusal a frame carries, if it is one, rendered as `message (code
/// N)`: every refusal rides the typed reply to the request it refuses. The
/// replies this client can receive one on are a registration ack whose
/// `success` is false (registration is the only lifecycle frame it sends),
/// a subscribe status or handshake ack carrying an `error`, and a rejected
/// ops write.
fn refusal_in(env: &sync::SyncEnvelope) -> Option<String> {
    let reason = |e: &Option<sync::Error>, what: &str| {
        e.as_ref().map_or_else(|| what.to_owned(), render_refusal)
    };
    match &env.payload {
        Some(Payload::RegisterDocumentAck(a)) if !a.success => {
            Some(reason(&a.error, "registration refused"))
        }
        Some(Payload::HandshakeAck(a)) if a.error.is_some() => {
            Some(reason(&a.error, "handshake refused"))
        }
        Some(Payload::SubscribeStatus(s)) if s.error.is_some() => {
            Some(reason(&s.error, "subscribe refused"))
        }
        Some(Payload::SyncOpsRejected(r)) => Some(reason(&r.error, "ops rejected")),
        _ => None,
    }
}

/// A typed refusal as one line of prose: the relay's message with the code a
/// client would branch on, so an error string never loses the verdict.
fn render_refusal(e: &sync::Error) -> String {
    format!("{} (code {})", e.message, e.code)
}

/// The result of routing one received envelope (see [`route_envelope`]).
///
/// Routing is **total and exhaustive**: every payload variant maps to one
/// of these outcomes with no catch-all arm, so a newly added payload kind
/// is a compile error rather than a silently dropped frame.
#[derive(Debug)]
enum RoutedFrame {
    /// A streaming event to surface to the caller of `recv`. Boxed because
    /// `SyncEvent` (dominated by `Signal`'s record envelope) dwarfs
    /// the other variants; the box keeps the routing enum small.
    Event(Box<SyncEvent>),
    /// A read-classification status for a subscribe. Handled specially by
    /// `subscribe`; ignored as a control frame by `recv`.
    Status(sync::SubscribeStatus),
    /// A control / request / lifecycle-ack / result frame that is not a
    /// streaming event. These are consumed via dedicated request-response
    /// methods, not `recv`, so `recv` ignores them rather than surfacing
    /// them. Carries the variant name for debug logging.
    Control(&'static str),
    /// An envelope with no payload set.
    Empty,
}

/// Route an envelope to its [`RoutedFrame`] outcome.
///
/// Pure and exhaustive over every [`Payload`] variant — this is the single
/// demultiplexing chokepoint. Adding a payload kind to the proto forces a
/// new arm here (no `_` fallthrough), so frames can never be silently
/// dropped by omission.
fn route_envelope(envelope: sync::SyncEnvelope) -> RoutedFrame {
    let Some(payload) = envelope.payload else {
        return RoutedFrame::Empty;
    };
    match payload {
        // Streaming events surfaced to `recv` callers.
        Payload::SyncOps(ops) => RoutedFrame::Event(Box::new(SyncEvent::Ops(ops))),
        Payload::StaleSubscriber(s) => RoutedFrame::Event(Box::new(SyncEvent::Stale(s))),
        // The frame whole: the refusal names no space or document of its own,
        // the carrier does, and a caller with several documents in flight
        // needs those fields to know which write was refused.
        Payload::SyncOpsRejected(r) => RoutedFrame::Event(Box::new(SyncEvent::Rejected(r))),
        Payload::PresenceUpdate(p) => RoutedFrame::Event(Box::new(SyncEvent::Presence(p))),
        Payload::Signal(s) => RoutedFrame::Event(Box::new(SyncEvent::Signal(s))),
        // Read classification — special-cased by `subscribe`.
        Payload::SubscribeStatus(s) => RoutedFrame::Status(s),
        // Control / request / result / ack frames: not streaming events.
        Payload::Handshake(_) => RoutedFrame::Control("Handshake"),
        Payload::HandshakeAck(_) => RoutedFrame::Control("HandshakeAck"),
        Payload::Subscribe(_) => RoutedFrame::Control("Subscribe"),
        Payload::Unsubscribe(_) => RoutedFrame::Control("Unsubscribe"),
        Payload::RegisterDocument(_) => RoutedFrame::Control("RegisterDocument"),
        Payload::RenameDocument(_) => RoutedFrame::Control("RenameDocument"),
        Payload::UnregisterDocument(_) => RoutedFrame::Control("UnregisterDocument"),
        Payload::ListSpaceDocuments(_) => RoutedFrame::Control("ListSpaceDocuments"),
        Payload::ListSpaceDocumentsResult(_) => RoutedFrame::Control("ListSpaceDocumentsResult"),
        Payload::JoinSpace(_) => RoutedFrame::Control("JoinSpace"),
        Payload::JoinSpaceResult(_) => RoutedFrame::Control("JoinSpaceResult"),
        Payload::ResolveSpace(_) => RoutedFrame::Control("ResolveSpace"),
        Payload::ResolveSpaceResult(_) => RoutedFrame::Control("ResolveSpaceResult"),
        Payload::RegisterDocumentAck(_) => RoutedFrame::Control("RegisterDocumentAck"),
        Payload::RenameDocumentAck(_) => RoutedFrame::Control("RenameDocumentAck"),
        Payload::UnregisterDocumentAck(_) => RoutedFrame::Control("UnregisterDocumentAck"),
        Payload::UnregisterSpace(_) => RoutedFrame::Control("UnregisterSpace"),
        Payload::UnregisterSpaceAck(_) => RoutedFrame::Control("UnregisterSpaceAck"),
        Payload::TransferSpaceOwnership(_) => RoutedFrame::Control("TransferSpaceOwnership"),
        Payload::TransferSpaceOwnershipAck(_) => RoutedFrame::Control("TransferSpaceOwnershipAck"),
        // Signal submit/stream frames. The relay serves them; this client does
        // not send or consume them via `recv`, so they route as control rather
        // than as events. Listed explicitly because this match is exhaustive on
        // purpose — a frame dropped by omission is the failure mode the
        // exhaustiveness exists to prevent.
        Payload::SubmitFlag(_) => RoutedFrame::Control("SubmitFlag"),
        Payload::SubmitComment(_) => RoutedFrame::Control("SubmitComment"),
        Payload::SubmitReply(_) => RoutedFrame::Control("SubmitReply"),
        Payload::SubmitTransition(_) => RoutedFrame::Control("SubmitTransition"),
        Payload::Barrier(_) => RoutedFrame::Control("Barrier"),
        Payload::BarrierAck(_) => RoutedFrame::Control("BarrierAck"),
        Payload::SubscribeSignals(_) => RoutedFrame::Control("SubscribeSignals"),
        Payload::SignalPage(_) => RoutedFrame::Control("SignalPage"),
        Payload::SignalAck(_) => RoutedFrame::Control("SignalAck"),
        Payload::SignalReseed(_) => RoutedFrame::Control("SignalReseed"),
        Payload::StaleSignalStream(_) => RoutedFrame::Control("StaleSignalStream"),
    }
}

/// How `subscribe` should handle a frame read while awaiting catch-up.
///
/// Computed by [`subscribe_disposition`], a pure function over the envelope
/// and the requested `(space_id, document_id)`, so the addressing invariant
/// can be unit-tested without a live socket.
#[derive(Debug, PartialEq, Eq)]
enum SubscribeDisposition {
    /// A `SyncOps` addressed to the requested doc: this is the catch-up.
    CatchUp,
    /// The relay refused the subscribe: its status frame carries the error.
    Rejected,
    /// An `Inconsistent` read classification — fail closed.
    Inconsistent,
    /// An `Unavailable` read classification — fail closed.
    Unavailable,
    /// Any other frame (another doc's ops, a signal, presence, control):
    /// buffer it for `recv` and keep waiting for catch-up.
    Buffer,
}

/// Decide how `subscribe` should handle `envelope` while awaiting the
/// catch-up for `(target_space, target_doc)`.
///
/// The addressing invariant lives here: a `SyncOps` is accepted as catch-up
/// **only** when its `(space_id, document_id)` matches the requested doc.
/// Ops for any other document are buffered, never returned as this doc's
/// catch-up (which would misattribute them into the wrong CRDT).
fn subscribe_disposition(
    envelope: &sync::SyncEnvelope,
    target_space: &str,
    target_doc: &str,
) -> SubscribeDisposition {
    match &envelope.payload {
        Some(Payload::SyncOps(ops))
            if ops.space_id == target_space && ops.document_id == target_doc =>
        {
            SubscribeDisposition::CatchUp
        }
        Some(Payload::SubscribeStatus(s)) if s.error.is_some() => SubscribeDisposition::Rejected,
        Some(Payload::SubscribeStatus(s)) => match sync::LoadStatus::try_from(s.load_status) {
            Ok(sync::LoadStatus::Inconsistent) => SubscribeDisposition::Inconsistent,
            Ok(sync::LoadStatus::Unavailable) => SubscribeDisposition::Unavailable,
            // Found / Empty / Unspecified: keep waiting for catch-up.
            _ => SubscribeDisposition::Buffer,
        },
        // Non-matching ops, signals, presence, stale, control:
        // buffer for `recv`, never consume-and-drop.
        _ => SubscribeDisposition::Buffer,
    }
}

/// Events received from the relay.
#[derive(Debug)]
pub enum SyncEvent {
    /// Sync operations (text or blob).
    Ops(sync::SyncOps),
    /// Relay evicted this subscription -- client should resubscribe.
    Stale(sync::StaleSubscriber),
    /// The relay refused a `SyncOps` write. The frame names the space and
    /// document it refuses beside the typed error, so the caller reads the
    /// scope from those fields.
    Rejected(sync::SyncOpsRejected),
    /// Presence update.
    Presence(sync::PresenceUpdate),
    /// Signal from another participant (flag, decision, reply).
    Signal(sync::Signal),
}

/// A sync client that manages a WebSocket connection to a kutl relay.
pub struct SyncClient {
    ws: WsStream,
    recovery: RecoveryConfig,
    /// Active subscriptions for auto-resubscribe: `(space_id, document_id)`.
    subscriptions: HashSet<(String, String)>,
    /// Counter for auto-resubscribes triggered by `StaleSubscriber` messages.
    resubscribe_count: u32,
    /// Frames read off the socket by `subscribe` that are not this
    /// subscribe's catch-up (e.g. another doc's ops, a signal, presence).
    /// `recv` drains this before reading the socket so no frame is dropped.
    inbox: VecDeque<sync::SyncEnvelope>,
    /// Correlation counter for [`Self::barrier`]: a barrier's ref only has
    /// to be unique per connection, so a counter is enough and needs no
    /// randomness.
    barrier_seq: u64,
    /// Whether the relay advertised the barrier feature in its handshake
    /// ack; against a relay that did not, [`Self::close`] sends the close
    /// frame directly.
    barrier_supported: bool,
}

/// Upper bound on waiting for a correlated reply (a submit's `SignalAck`, a
/// subscribe's `SignalPage`, a register's `RegisterDocumentAck`, a
/// `BarrierAck`). A reply loop parks every other frame on the inbox while it
/// waits, so an unbounded wait against a relay that died mid-conversation
/// would hang the caller instead of erroring. Generous: covers a slow relay
/// persisting a 100-record chunk or committing a registration under
/// database contention, not an interactive UX budget.
const REPLY_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(30));

/// The WebSocket codec configuration every relay connection uses: message
/// and frame caps at the wire cap (`WS_MESSAGE_MAX`), not the library's
/// 16 MiB defaults. The relay legitimately sends blob messages up to the
/// wire cap (an at-cap blob's message is envelope-larger than the blob
/// policy cap itself); a smaller codec cap rejects them on delivery and
/// the replayed catch-up wedges the subscription silently. ONE definition,
/// so no client can drift from the relay's codec.
#[must_use]
pub(crate) fn relay_ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(kutl_proto::protocol::WS_MESSAGE_MAX))
        .max_frame_size(Some(kutl_proto::protocol::WS_MESSAGE_MAX))
}

impl SyncClient {
    /// Connect to a relay using an already-issued auth token (e.g. a PAT
    /// or a bearer token returned by [`crate::authenticate`]).
    ///
    /// Use this when the caller already has a token in hand and wants to
    /// skip the DID challenge-response flow. The token is forwarded to
    /// the relay in the `Handshake` payload; the relay rejects the
    /// connection if the token is invalid.
    pub async fn connect_with_auth(
        url: &str,
        client_name: &str,
        auth_token: &str,
        display_name: &str,
    ) -> Result<Self> {
        Self::connect_inner(
            url,
            client_name,
            RecoveryConfig::default(),
            AuthHeader {
                token: auth_token.to_owned(),
                display_name: display_name.to_owned(),
            },
        )
        .await
    }

    async fn connect_inner(
        url: &str,
        client_name: &str,
        recovery: RecoveryConfig,
        auth: AuthHeader,
    ) -> Result<Self> {
        // Read the relay's code, never its prose: an auth refusal names the
        // credential slot to change, and every other refusal states its own
        // remedy already.
        let (ws, ack) = connect_and_handshake(url, client_name, &auth.token, &auth.display_name)
            .await
            .map_err(|e| match e {
                HandshakeError::Refused {
                    auth_failed: true, ..
                } => anyhow::anyhow!(crate::credentials::refused_token_remedy(url)),
                HandshakeError::Refused { message, .. } => {
                    anyhow::anyhow!("relay refused the connection: {message}")
                }
                HandshakeError::VersionGap(gap) => anyhow::anyhow!(gap),
                HandshakeError::Transport(e) => e,
            })?;
        let barrier_supported = ack
            .features
            .iter()
            .any(|f| f == kutl_proto::protocol::BARRIER_CAPABILITY);
        Ok(Self {
            ws,
            recovery,
            subscriptions: HashSet::new(),
            resubscribe_count: 0,
            inbox: VecDeque::new(),
            barrier_seq: 0,
            barrier_supported,
        })
    }

    /// Subscribe to a document. Returns catch-up ops if the document has content.
    ///
    /// Returns an error if the relay rejects the subscribe (auth failure,
    /// rate limit, or an `Inconsistent` / `Unavailable` read
    /// classification). A timeout waiting for catch-up is not an error —
    /// it simply means the document had no content to send.
    ///
    /// When the relay classifies the subscribe as `Inconsistent` or
    /// `Unavailable`, it sends a `SubscribeStatus` envelope on ctrl and
    /// then short-circuits — no `SyncOps` follows. Treating that as
    /// "empty doc" is exactly the silent data-loss path the fail-closed
    /// design is meant to prevent on the browser client. The daemon bails
    /// with a structured error for the same reason: CLI callers can
    /// surface it to the user instead of silently syncing an empty doc.
    pub async fn subscribe(
        &mut self,
        space_id: &str,
        document_id: &str,
    ) -> Result<Option<sync::SyncOps>> {
        self.subscriptions
            .insert((space_id.to_owned(), document_id.to_owned()));

        self.send_envelope(&subscribe_envelope(space_id, document_id))
            .await?;

        for _ in 0..SUBSCRIBE_CATCHUP_MAX_FRAMES {
            let env =
                match tokio::time::timeout(SUBSCRIBE_CATCHUP_TIMEOUT, self.recv_envelope()).await {
                    Ok(Ok(env)) => env,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => return Ok(None), // timeout — no catch-up
                };

            match subscribe_disposition(&env, space_id, document_id) {
                SubscribeDisposition::CatchUp => {
                    // Addressing invariant held: this SyncOps is for the
                    // requested doc. Safe to return as catch-up.
                    let Some(Payload::SyncOps(ops)) = env.payload else {
                        unreachable!("CatchUp disposition implies SyncOps payload")
                    };
                    return Ok(Some(ops));
                }
                SubscribeDisposition::Rejected => {
                    self.subscriptions
                        .remove(&(space_id.to_owned(), document_id.to_owned()));
                    let Some(Payload::SubscribeStatus(sync::SubscribeStatus {
                        error: Some(e),
                        ..
                    })) = env.payload
                    else {
                        unreachable!("Rejected disposition implies a refused SubscribeStatus")
                    };
                    bail!("subscribe rejected: {} (code {})", e.message, e.code)
                }
                SubscribeDisposition::Inconsistent | SubscribeDisposition::Unavailable => {
                    self.subscriptions
                        .remove(&(space_id.to_owned(), document_id.to_owned()));
                    // The reason text comes from the shared fail-closed
                    // classifier so every subscribing client describes a
                    // compromised read identically.
                    let reason = match env.payload {
                        Some(Payload::SubscribeStatus(s)) => {
                            kutl_proto::protocol::subscribe_status_failure(&s)
                        }
                        _ => None,
                    }
                    .unwrap_or_else(|| "relay classified the read as compromised".to_owned());
                    bail!("subscribe refused for {space_id}/{document_id}: {reason}");
                }
                SubscribeDisposition::Buffer => {
                    // Not this doc's catch-up (another doc's ops, a signal,
                    // presence, control). Buffer it so `recv` delivers it
                    // instead of dropping it off the multiplexed socket.
                    self.inbox.push_back(env);
                }
            }
        }
        Ok(None)
    }

    /// Send sync operations to the relay.
    pub async fn send_ops(
        &mut self,
        space_id: &str,
        document_id: &str,
        ops: Vec<u8>,
        metadata: Vec<sync::ChangeMetadata>,
    ) -> Result<()> {
        // Client-outbound edit delta: bindings ride the per-change `metadata`;
        // the uncapped author snapshot is a catch-up-only field.
        self.send_envelope(&sync_ops_envelope(
            space_id,
            document_id,
            ops,
            metadata,
            std::collections::HashMap::new(),
        ))
        .await
    }

    /// Send a blob (binary content) for a document.
    ///
    /// `content_hash` is the SHA-256 of `bytes`. `metadata` carries the
    /// authoring `ChangeMetadata` (author DID, intent, timestamp); pass
    /// `None` to omit it.
    pub async fn send_blob(
        &mut self,
        space_id: &str,
        document_id: &str,
        bytes: Vec<u8>,
        content_hash: Vec<u8>,
        metadata: Option<sync::ChangeMetadata>,
    ) -> Result<()> {
        self.send_envelope(&blob_ops_envelope(
            space_id,
            document_id,
            bytes,
            content_hash,
            metadata,
        ))
        .await
    }

    /// Send a registration without awaiting its ack: the body of
    /// [`Self::register_document_acked`], private because a send that is
    /// not awaited can lose its tail on close.
    ///
    /// `provenance` carries source provenance. The daemon path populates
    /// `originally_created_at_ms` only. The ingestion worker populates
    /// all six fields. Other callers pass
    /// [`RegisterDocumentMetadata::default()`].
    async fn register_document(
        &mut self,
        space_id: &str,
        document_id: &str,
        path: &str,
        metadata: Option<sync::ChangeMetadata>,
        provenance: RegisterDocumentMetadata,
    ) -> Result<()> {
        self.send_envelope(&register_document_envelope(
            space_id,
            document_id,
            path,
            metadata,
            provenance,
        ))
        .await
    }

    /// Register a document and await the relay's [`sync::RegisterDocumentAck`]
    /// — the ONE way a client learns that a registration PERSISTED.
    ///
    /// [`Self::register_document`] returns once the envelope is written to
    /// the socket, which says nothing about what the relay did with it. The
    /// relay emits the ack only after its registry insert and the mirror
    /// row have committed, so a `success = true` ack is a persistence
    /// guarantee and a `success = false` ack carries the relay's structured
    /// reason in `error`. A client that sends without reading its acks and
    /// then closes can lose its tail outright: the relay's ack write hits
    /// the closed socket, its write task ends, and its read loop aborts on
    /// that coupling with the client's remaining frames still unread in the
    /// socket buffer — a bulk import silently short by its last pages while
    /// every page reports success. Awaiting each ack makes that race
    /// impossible: the socket cannot close before the relay has committed
    /// every registration.
    ///
    /// Frames for other documents that arrive while waiting are buffered
    /// for [`Self::recv`] by [`Self::await_reply`], never consumed and
    /// dropped off the multiplexed socket. Elapsing [`REPLY_TIMEOUT`] is an
    /// error: the registration is unconfirmed.
    pub async fn register_document_acked(
        &mut self,
        space_id: &str,
        document_id: &str,
        path: &str,
        metadata: Option<sync::ChangeMetadata>,
        provenance: RegisterDocumentMetadata,
    ) -> Result<sync::RegisterDocumentAck> {
        self.register_document(space_id, document_id, path, metadata, provenance)
            .await?;
        let what = format!("RegisterDocumentAck for {space_id}/{document_id}");
        let ack = self
            .await_reply(&what, |env| match &env.payload {
                Some(Payload::RegisterDocumentAck(ack))
                    if ack.space_id == space_id && ack.document_id == document_id =>
                {
                    Some(ack.clone())
                }
                _ => None,
            })
            .await?;
        Ok(ack)
    }

    /// Read frames until `pick` claims one, buffering every other frame for
    /// [`Self::recv`], under ONE deadline of [`REPLY_TIMEOUT`] from now (a
    /// deadline, not a per-frame timeout: a busy socket cannot stretch the
    /// wait). The correlated-reply loop behind every request whose answer is
    /// one matching frame; [`Self::subscribe`] keeps its own loop because a
    /// subscribe's answer is a disposition over several frame kinds.
    async fn await_reply<T>(
        &mut self,
        what: &str,
        mut pick: impl FnMut(&sync::SyncEnvelope) -> Option<T>,
    ) -> Result<T> {
        let deadline = tokio::time::Instant::now() + REPLY_TIMEOUT;
        loop {
            let env = tokio::time::timeout_at(deadline, self.recv_envelope())
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "no {what} within {REPLY_TIMEOUT:?}; the relay may be gone — retry on a \
                         fresh connection"
                    )
                })??;
            if let Some(found) = pick(&env) {
                return Ok(found);
            }
            self.inbox.push_back(env);
        }
    }

    /// Emit a `Signal` envelope (flag / chat / decision / reply).
    ///
    /// Callers construct a [`sync::Signal`] by populating the proto type
    /// directly. The relay always rewrites `author_did` to the
    /// authenticated identity (authentication is mandatory).
    pub async fn send_signal(&mut self, signal: sync::Signal) -> Result<()> {
        self.send_envelope(&sync::SyncEnvelope {
            payload: Some(Payload::Signal(signal)),
        })
        .await
    }

    /// Send an authored submit frame and wait for ITS `SignalAck`.
    ///
    /// The submit frames are request-response over a stream, correlated by
    /// `client_ref`. Matching on the ref rather than taking the
    /// next frame is what makes that safe: a relay may interleave a broadcast,
    /// a `SignalPage`, or another submit's ack in between, and "read the next
    /// envelope" would hand back whichever arrived first.
    ///
    /// Non-matching envelopes are pushed onto the inbox rather than dropped, so
    /// a caller that later calls [`Self::recv`] still sees them in order —
    /// which is why this cannot simply discard what it does not want.
    ///
    /// Errors when the relay refuses the submit, carrying the relay's reason.
    /// A refusal is a normal outcome here, not a transport fault.
    pub async fn submit_signal(
        &mut self,
        envelope: &sync::SyncEnvelope,
    ) -> Result<sync::SignalAck> {
        let client_ref = match envelope.payload {
            Some(Payload::SubmitFlag(ref m)) => m.client_ref.clone(),
            Some(Payload::SubmitComment(ref m)) => m.client_ref.clone(),
            Some(Payload::SubmitReply(ref m)) => m.client_ref.clone(),
            Some(Payload::SubmitTransition(ref m)) => m.client_ref.clone(),
            Some(Payload::SignalReseed(ref m)) => m.client_ref.clone(),
            _ => anyhow::bail!("not an authored submit frame"),
        };
        self.send_envelope(envelope).await?;

        let what = format!("ack for submit {client_ref}");
        let ack = self
            .await_reply(&what, |env| match &env.payload {
                Some(Payload::SignalAck(ack)) if ack.client_ref == client_ref => Some(ack.clone()),
                _ => None,
            })
            .await?;
        if ack.success {
            return Ok(ack);
        }
        anyhow::bail!(
            "{}",
            ack.error
                .as_ref()
                .map_or_else(|| "the relay refused the submit".to_owned(), render_refusal)
        )
    }

    /// Subscribe to a space's signal stream and take the backlog page it
    /// answers with.
    ///
    /// ONE page per subscribe: the relay answers a `SubscribeSignals` with a
    /// single `SignalPage` and expects the caller to re-subscribe with the
    /// returned cursor for more. The cursor is the caller's to keep; the
    /// relay holds none, so a retry re-sends the page that was asked for.
    ///
    /// Correlated by frame type rather than by a ref, because `SignalPage`
    /// carries none — safe for a caller with one subscribe outstanding, which
    /// is what the paging loop above it does.
    ///
    /// A `StaleSignalStream` is an ERROR here, not a page. The relay sends it
    /// instead of a page when the caller is not authorized, the records could
    /// not be loaded, or the data lane overflowed — treating it as "no more
    /// records" would silently truncate the backlog and look like success.
    pub async fn fetch_signal_page(
        &mut self,
        space_id: &str,
        cursor: Option<sync::Hlc>,
    ) -> Result<sync::SignalPage> {
        self.send_envelope(&subscribe_signals_envelope(space_id, cursor))
            .await?;
        let what = format!("signal page for space {space_id}");
        self.await_reply(&what, |env| match &env.payload {
            Some(Payload::SignalPage(page)) => Some(Ok(page.clone())),
            Some(Payload::StaleSignalStream(s)) => Some(Err(anyhow::anyhow!(
                "signal stream unavailable (cause {}): {}",
                s.cause,
                s.reason
            ))),
            _ => None,
        })
        .await?
    }

    /// Receive the next event from the relay. Blocks until an event arrives.
    ///
    /// When auto-recovery is enabled (the default), `StaleSubscriber` messages
    /// trigger an automatic resubscribe. If the resubscribe is rejected (e.g.
    /// rate limited), exponential backoff is applied with progressive attempt
    /// counts before retrying. Both are transparent to the caller unless
    /// recovery is disabled or `max_recovery_attempts` is reached.
    pub async fn recv(&mut self) -> Result<SyncEvent> {
        let mut recovery_attempt: u32 = 0;
        loop {
            // Drain frames buffered by `subscribe` before reading the socket,
            // so no multiplexed frame is dropped or reordered behind live ops.
            let envelope = match self.inbox.pop_front() {
                Some(env) => env,
                None => self.recv_envelope().await?,
            };

            // Auto-recovery on stale eviction is a stateful special case that
            // must run before the pure routing step (it resubscribes and may
            // return the catch-up ops).
            if self.recovery.enabled
                && let Some(Payload::StaleSubscriber(ref s)) = envelope.payload
            {
                self.resubscribe_count += 1;
                tracing::info!(
                    space_id = %s.space_id,
                    document_id = %s.document_id,
                    reason = s.reason,
                    "received stale notice, auto-resubscribing"
                );

                let pid = s.space_id.clone();
                let did = s.document_id.clone();

                // Check if we've exhausted recovery attempts.
                let max = self.recovery.max_recovery_attempts;
                if max > 0 && recovery_attempt >= max {
                    tracing::warn!(
                        attempts = recovery_attempt,
                        "recovery attempts exhausted, returning stale event"
                    );
                    return Ok(SyncEvent::Stale(s.clone()));
                }

                // Apply backoff before retrying (skip on first attempt).
                if recovery_attempt > 0 {
                    let backoff = self.recovery.backoff_for(recovery_attempt);
                    tracing::warn!(
                        attempt = recovery_attempt,
                        backoff_ms = backoff.as_millis(),
                        "resubscribe backing off"
                    );
                    tokio::time::sleep(backoff).await;
                }
                recovery_attempt += 1;

                match self.subscribe(&pid, &did).await {
                    Ok(Some(ops)) => return Ok(SyncEvent::Ops(ops)),
                    Ok(None) => {
                        recovery_attempt = 0;
                        // Subscribe succeeded but no catch-up; continue recv loop.
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            attempt = recovery_attempt,
                            "resubscribe failed, will retry on next stale notice"
                        );
                        // Don't reset recovery_attempt — will backoff more next time.
                    }
                }
                continue;
            }

            // Total, exhaustive routing for every other frame.
            match route_envelope(envelope) {
                RoutedFrame::Event(event) => return Ok(*event),
                RoutedFrame::Status(s) => {
                    // SubscribeStatus belongs to the subscribe round-trip; if
                    // it surfaces here it is informational only.
                    tracing::debug!(
                        load_status = s.load_status,
                        "ignoring subscribe status outside subscribe"
                    );
                }
                RoutedFrame::Control(name) => {
                    tracing::debug!(payload = name, "ignoring control envelope in recv");
                }
                RoutedFrame::Empty => {}
            }
        }
    }

    /// Receive with a timeout. Returns `None` if the timeout expires.
    pub async fn recv_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Option<SyncEvent>> {
        match tokio::time::timeout(timeout, self.recv()).await {
            Ok(Ok(event)) => Ok(Some(event)),
            Ok(Err(e)) => Err(e),
            Err(_) => Ok(None),
        }
    }

    /// Number of auto-resubscribes triggered by stale subscriber evictions.
    pub fn resubscribe_count(&self) -> u32 {
        self.resubscribe_count
    }

    /// Wait until the relay has processed every frame this client sent so
    /// far: send a `Barrier` and read until its ack, buffering every other
    /// frame for [`Self::recv`]. The relay handles a connection's frames in
    /// arrival order and answers the barrier on its own-ack lane, so by the
    /// time the ack arrives every direct reply to an earlier write on that
    /// lane, a refusal included, has been received. Against a relay that
    /// did not advertise the barrier feature this returns at once: nothing
    /// can be learned, and waiting would only time out.
    pub async fn barrier(&mut self) -> Result<()> {
        if !self.barrier_supported {
            tracing::debug!("relay predates the barrier frame; skipping the barrier");
            return Ok(());
        }
        self.barrier_seq += 1;
        let client_ref = format!("barrier-{}", self.barrier_seq);
        self.send_envelope(&sync::SyncEnvelope {
            payload: Some(Payload::Barrier(sync::Barrier {
                client_ref: client_ref.clone(),
            })),
        })
        .await?;
        let what = format!("BarrierAck {client_ref}");
        self.await_reply(&what, |env| match &env.payload {
            Some(Payload::BarrierAck(ack)) if ack.client_ref == client_ref => Some(()),
            _ => None,
        })
        .await
    }

    /// Close the connection gracefully, after every frame this client sent
    /// has been processed by the relay.
    ///
    /// A WebSocket close frame ends the peer's ability to write once it has
    /// READ the frame, so a client that sends its close right behind its
    /// last write races the relay's reply to that write: whichever the
    /// relay's read half reaches first wins, and a reply that loses is
    /// dropped. So the close is preceded by a [`Self::barrier`], whose ack
    /// proves every earlier frame was handled and every reply to them has
    /// arrived; only then is the close frame sent.
    ///
    /// Frames buffered for [`Self::recv`] are discarded here (the connection
    /// is ending), with one exception: a refusal the relay sent in reply to
    /// something this client wrote without awaiting its ack (a lifecycle ack
    /// with `success = false`, a rejected ops write) is the only record of
    /// that write's failure, so refusals are collected and returned as the
    /// error. A relay that does not answer the barrier is reported as well:
    /// nothing can be said about the writes then.
    pub async fn close(mut self) -> Result<()> {
        let barrier = self.barrier().await;
        let _ = self.ws.close(None).await;
        let refusals: Vec<String> = self
            .inbox
            .drain(..)
            .filter_map(|env| refusal_in(&env))
            .collect();
        barrier.context("the relay did not confirm the writes before close")?;
        if refusals.is_empty() {
            return Ok(());
        }
        let reasons: Vec<&str> = refusals
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<&str>>()
            .into_iter()
            .collect();
        bail!(
            "the relay refused {} write(s) before close: {}",
            refusals.len(),
            reasons.join("; ")
        )
    }

    /// Send an envelope over the WebSocket.
    async fn send_envelope(&mut self, envelope: &sync::SyncEnvelope) -> Result<()> {
        let bytes = encode_envelope(envelope);
        self.ws
            .send(tungstenite::Message::Binary(bytes.into()))
            .await
            .context("failed to send message")
    }

    /// Receive and decode the next binary envelope from the WebSocket.
    async fn recv_envelope(&mut self) -> Result<sync::SyncEnvelope> {
        loop {
            let msg = self
                .ws
                .next()
                .await
                .context("connection closed")?
                .context("ws read error")?;

            match msg {
                tungstenite::Message::Binary(bytes) => {
                    return decode_envelope(&bytes).context("failed to decode envelope");
                }
                tungstenite::Message::Close(_) => bail!("connection closed by relay"),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authenticate;
    use std::io::Write;

    /// Boot an auth-on relay (authentication is mandatory) backed by a
    /// fresh `authorized_keys` file.
    ///
    /// Returns the bound address plus the keys-file handle so callers can enroll
    /// per-client DIDs via [`authed_client`]. The relay live-reloads the file on
    /// every auth check, so appending a bare DID line authorizes it immediately.
    /// The handle must be kept alive for the test's duration — its `Drop` deletes
    /// the temp file.
    async fn start_test_relay() -> (String, tempfile::NamedTempFile) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let keys = tempfile::NamedTempFile::new().expect("create authorized_keys file");
        let config = kutl_relay::config::RelayConfig {
            port: 0,
            relay_name: "test-relay".into(),
            authorized_keys_file: Some(keys.path().to_path_buf()),
            ..Default::default()
        };
        // In-memory boot: these connection tests use in-memory registries;
        // the OSS binary's build_app requires a data dir, so construct the
        // no-backend relay via the test seam instead.
        let (app, _handle, _flush_handle) = kutl_relay::testing::build_in_memory_app(
            config,
            kutl_relay::testing::TestBackends::default(),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, keys)
    }

    /// Enroll a fresh did:key identity in the relay's `authorized_keys` file and
    /// mint a bearer for it via the real challenge-response flow.
    ///
    /// A bare DID line grants full access; the relay re-reads the file on every
    /// auth check, so the enrollment takes effect immediately. Returns the minted
    /// bearer token, mirroring how the daemon authenticates in production.
    async fn mint_token(addr: &str, keys: &tempfile::NamedTempFile) -> String {
        enroll_and_mint(addr, keys, None).await
    }

    /// The one enrollment body behind [`mint_token`] and the scoped variant:
    /// generate a did:key identity, append its `authorized_keys` line — bare
    /// (every space) or `scope=<space>` (that space only) — and mint a
    /// bearer for it through the real challenge-response flow.
    async fn enroll_and_mint(
        addr: &str,
        keys: &tempfile::NamedTempFile,
        scope: Option<&str>,
    ) -> String {
        let secret: [u8; 32] = std::array::from_fn(|_| rand::random::<u8>());
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret);
        let did = kutl_signals::did_key_encode(&signing_key.verifying_key());
        let line = match scope {
            Some(space) => format!("{did} scope={space}"),
            None => did.clone(),
        };
        writeln!(keys.as_file(), "{line}").expect("enroll did in authorized_keys");
        keys.as_file().sync_all().expect("flush authorized_keys");
        authenticate(&format!("http://{addr}"), &did, &signing_key)
            .await
            .expect("mint bearer token")
    }

    /// Connect an authenticated [`SyncClient`] to a relay booted by
    /// [`start_test_relay`].
    ///
    /// Enrolls a fresh identity ([`mint_token`]), then completes the
    /// authenticated WebSocket handshake — mirroring how the daemon connects in
    /// production.
    async fn authed_client(addr: &str, keys: &tempfile::NamedTempFile, name: &str) -> SyncClient {
        let token = mint_token(addr, keys).await;
        SyncClient::connect_with_auth(&format!("ws://{addr}/ws"), name, &token, name)
            .await
            .expect("authenticated connect")
    }

    #[tokio::test]
    async fn test_connect_and_handshake() {
        let (addr, keys) = start_test_relay().await;
        let _client = authed_client(&addr, &keys, "test-client").await;
    }

    #[tokio::test]
    async fn test_subscribe_empty_doc() {
        let (addr, keys) = start_test_relay().await;
        let mut client = authed_client(&addr, &keys, "test").await;
        let catch_up = client
            .subscribe("5171e0a1-1111-4000-8000-000000000001", "doc")
            .await
            .unwrap();
        // Relay sends empty ops as a signal that the doc exists but has no content.
        let ops = catch_up.expect("empty doc sends catch-up with empty ops");
        assert!(ops.ops.is_empty(), "ops should be empty for a new doc");
    }

    #[tokio::test]
    async fn test_send_and_recv_ops() {
        let (addr, keys) = start_test_relay().await;

        let mut client_a = authed_client(&addr, &keys, "a").await;
        client_a
            .subscribe("5171e0a1-1111-4000-8000-000000000001", "doc")
            .await
            .unwrap();

        let mut client_b = authed_client(&addr, &keys, "b").await;
        client_b
            .subscribe("5171e0a1-1111-4000-8000-000000000001", "doc")
            .await
            .unwrap();

        // Client A sends an edit.
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("alice").unwrap();
        doc.edit(
            agent,
            "alice",
            "test",
            kutl_core::Boundary::Explicit,
            |ctx| ctx.insert(0, "hello"),
        )
        .unwrap();
        let ops = doc.encode_since(&[]);
        let metadata = doc.changes_since(&[]);
        client_a
            .send_ops("5171e0a1-1111-4000-8000-000000000001", "doc", ops, metadata)
            .await
            .unwrap();

        // Client B receives.
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), client_b.recv())
            .await
            .unwrap()
            .unwrap();

        match event {
            SyncEvent::Ops(sync_ops) => {
                assert!(!sync_ops.ops.is_empty());
                let mut doc_b = kutl_core::Document::new();
                doc_b.merge(&sync_ops.ops, &sync_ops.metadata).unwrap();
                assert_eq!(doc_b.content(), "hello");
            }
            other => panic!("expected Ops, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_late_subscriber_catches_up() {
        let (addr, keys) = start_test_relay().await;

        // Client A writes.
        let mut client_a = authed_client(&addr, &keys, "a").await;
        client_a
            .subscribe("5171e0a1-1111-4000-8000-000000000001", "doc")
            .await
            .unwrap();

        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("alice").unwrap();
        doc.edit(
            agent,
            "alice",
            "test",
            kutl_core::Boundary::Explicit,
            |ctx| ctx.insert(0, "world"),
        )
        .unwrap();
        let ops = doc.encode_since(&[]);
        let metadata = doc.changes_since(&[]);
        client_a
            .send_ops("5171e0a1-1111-4000-8000-000000000001", "doc", ops, metadata)
            .await
            .unwrap();

        // Give relay time to process.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Client B subscribes late -- should get catch-up.
        let mut client_b = authed_client(&addr, &keys, "b").await;
        let catch_up = client_b
            .subscribe("5171e0a1-1111-4000-8000-000000000001", "doc")
            .await
            .unwrap();
        assert!(catch_up.is_some(), "late subscriber should get catch-up");

        let ops = catch_up.unwrap();
        let mut doc_b = kutl_core::Document::new();
        doc_b.merge(&ops.ops, &ops.metadata).unwrap();
        assert_eq!(doc_b.content(), "world");
    }

    #[tokio::test]
    async fn test_recv_timeout_returns_none() {
        let (addr, keys) = start_test_relay().await;
        let mut client = authed_client(&addr, &keys, "test").await;
        client
            .subscribe("5171e0a1-1111-4000-8000-000000000001", "doc")
            .await
            .unwrap();

        let event = client
            .recv_timeout(std::time::Duration::from_millis(50))
            .await
            .unwrap();
        assert!(event.is_none());
    }

    #[tokio::test]
    async fn test_connect_inner_custom_recovery_config() {
        let (addr, keys) = start_test_relay().await;
        let token = mint_token(&addr, &keys).await;
        let config = RecoveryConfig {
            enabled: true,
            ..Default::default()
        };
        // Authenticated connect preserving the custom recovery config.
        let mut client = SyncClient::connect_inner(
            &format!("ws://{addr}/ws"),
            "test",
            config,
            AuthHeader {
                token,
                display_name: "test".to_owned(),
            },
        )
        .await
        .unwrap();
        client
            .subscribe("5171e0a1-1111-4000-8000-000000000001", "doc")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_connect_inner_recovery_disabled() {
        let (addr, keys) = start_test_relay().await;
        let token = mint_token(&addr, &keys).await;
        let config = RecoveryConfig {
            enabled: false,
            ..Default::default()
        };
        // Authenticated connect preserving the disabled recovery config.
        let mut client = SyncClient::connect_inner(
            &format!("ws://{addr}/ws"),
            "test",
            config,
            AuthHeader {
                token,
                display_name: "test".to_owned(),
            },
        )
        .await
        .unwrap();
        // Basic smoke test that disabled recovery does not break connection flow.
        client
            .subscribe("5171e0a1-1111-4000-8000-000000000001", "doc")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_tracks_subscriptions() {
        let (addr, keys) = start_test_relay().await;
        let mut client = authed_client(&addr, &keys, "test").await;

        client
            .subscribe("5171e0a1-2222-4000-8000-00000000000a", "doc-1")
            .await
            .unwrap();
        client
            .subscribe("5171e0a1-2222-4000-8000-00000000000b", "doc-2")
            .await
            .unwrap();
        assert_eq!(client.subscriptions.len(), 2);
        assert!(client.subscriptions.contains(&(
            "5171e0a1-2222-4000-8000-00000000000a".to_owned(),
            "doc-1".to_owned()
        )));
        assert!(client.subscriptions.contains(&(
            "5171e0a1-2222-4000-8000-00000000000b".to_owned(),
            "doc-2".to_owned()
        )));
    }

    // ---- demultiplexer correctness ----

    fn ops_envelope(space: &str, doc: &str) -> sync::SyncEnvelope {
        sync::SyncEnvelope {
            payload: Some(Payload::SyncOps(sync::SyncOps {
                space_id: space.to_owned(),
                document_id: doc.to_owned(),
                ..Default::default()
            })),
        }
    }

    fn signal_envelope(space: &str) -> sync::SyncEnvelope {
        sync::SyncEnvelope {
            payload: Some(Payload::Signal(sync::Signal {
                id: "sig-1".to_owned(),
                space_id: space.to_owned(),
                ..Default::default()
            })),
        }
    }

    #[test]
    fn test_subscribe_disposition_addressing() {
        // SyncOps addressed to the requested doc → catch-up.
        assert_eq!(
            subscribe_disposition(
                &ops_envelope("5171e0a1-1111-4000-8000-000000000001", "doc"),
                "5171e0a1-1111-4000-8000-000000000001",
                "doc"
            ),
            SubscribeDisposition::CatchUp
        );
        // SyncOps for a DIFFERENT doc must NOT be returned as catch-up —
        // it must be buffered (returning it would misattribute the ops
        // into the wrong CRDT).
        assert_eq!(
            subscribe_disposition(
                &ops_envelope("5171e0a1-1111-4000-8000-000000000001", "other-doc"),
                "5171e0a1-1111-4000-8000-000000000001",
                "doc"
            ),
            SubscribeDisposition::Buffer
        );
        // Different space, same doc id → also buffered.
        assert_eq!(
            subscribe_disposition(
                &ops_envelope("other-space", "doc"),
                "5171e0a1-1111-4000-8000-000000000001",
                "doc"
            ),
            SubscribeDisposition::Buffer
        );
    }

    #[test]
    fn test_subscribe_disposition_signal_is_buffered_not_dropped() {
        // A Signal arriving during subscribe must be buffered, not consumed
        // and dropped (a catch-all that returns early here is a silent
        // data-loss path).
        assert_eq!(
            subscribe_disposition(
                &signal_envelope("5171e0a1-1111-4000-8000-000000000001"),
                "5171e0a1-1111-4000-8000-000000000001",
                "doc"
            ),
            SubscribeDisposition::Buffer
        );
    }

    #[test]
    fn test_route_envelope_signal_surfaces_as_event() {
        // The other half of the no-loss guarantee: a buffered Signal routes
        // to a Signal event when `recv` drains it.
        match route_envelope(signal_envelope("5171e0a1-1111-4000-8000-000000000001")) {
            RoutedFrame::Event(event) => match *event {
                SyncEvent::Signal(s) => assert_eq!(s.id, "sig-1"),
                other => panic!("expected Signal event, got {other:?}"),
            },
            other => panic!("expected Signal event, got {other:?}"),
        }
    }

    #[test]
    fn test_route_envelope_ops_surfaces_as_event() {
        match route_envelope(ops_envelope("5171e0a1-1111-4000-8000-000000000001", "doc")) {
            RoutedFrame::Event(event) => match *event {
                SyncEvent::Ops(ops) => {
                    assert_eq!(ops.space_id, "5171e0a1-1111-4000-8000-000000000001");
                    assert_eq!(ops.document_id, "doc");
                }
                other => panic!("expected Ops event, got {other:?}"),
            },
            other => panic!("expected Ops event, got {other:?}"),
        }
    }

    #[test]
    fn test_route_envelope_control_and_empty() {
        // A control/request frame is classified as Control, not surfaced.
        match route_envelope(sync::SyncEnvelope {
            payload: Some(Payload::HandshakeAck(sync::HandshakeAck::default())),
        }) {
            RoutedFrame::Control(name) => assert_eq!(name, "HandshakeAck"),
            other => panic!("expected Control, got {other:?}"),
        }
        // An empty envelope is classified as Empty.
        assert!(matches!(
            route_envelope(sync::SyncEnvelope { payload: None }),
            RoutedFrame::Empty
        ));
    }

    #[tokio::test]
    async fn test_recv_drains_buffered_signal() {
        // End-to-end of the no-loss path: a Signal buffered by subscribe is
        // delivered by recv ahead of the socket.
        let (addr, keys) = start_test_relay().await;
        let mut client = authed_client(&addr, &keys, "test").await;
        client
            .inbox
            .push_back(signal_envelope("5171e0a1-1111-4000-8000-000000000001"));

        let event = client
            .recv_timeout(std::time::Duration::from_millis(200))
            .await
            .unwrap()
            .expect("buffered signal must surface from recv");
        match event {
            SyncEvent::Signal(s) => assert_eq!(s.id, "sig-1"),
            other => panic!("expected Signal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_recv_drains_buffered_ops_for_other_doc() {
        // Ops for another doc that subscribe buffered are delivered by recv
        // (carrying their own address) rather than being lost.
        let (addr, keys) = start_test_relay().await;
        let mut client = authed_client(&addr, &keys, "test").await;
        client.inbox.push_back(ops_envelope(
            "5171e0a1-1111-4000-8000-000000000001",
            "other-doc",
        ));

        let event = client
            .recv_timeout(std::time::Duration::from_millis(200))
            .await
            .unwrap()
            .expect("buffered ops must surface from recv");
        match event {
            SyncEvent::Ops(ops) => assert_eq!(ops.document_id, "other-doc"),
            other => panic!("expected Ops, got {other:?}"),
        }
    }

    /// `register_document_acked` returns the relay's ack — the persistence
    /// guarantee a bare `register_document` cannot give. A registration the
    /// relay commits acks `success = true` carrying the persisted entry.
    #[tokio::test]
    async fn test_register_document_acked_success_carries_entry() {
        let (addr, keys) = start_test_relay().await;
        let mut client = authed_client(&addr, &keys, "acked").await;
        let space = "5171e0a1-1111-4000-8000-00000000aa01";
        let doc = "5171e0a1-2222-4000-8000-00000000aa02";
        let ack = client
            .register_document_acked(
                space,
                doc,
                "notes.md",
                None,
                RegisterDocumentMetadata::default(),
            )
            .await
            .expect("registration acked");
        assert!(
            ack.success,
            "a committed registration acks success: {ack:?}"
        );
        assert_eq!(ack.document_id, doc);
        assert!(
            ack.entry.is_some(),
            "the success ack carries the persisted entry"
        );
    }

    /// A registration the relay refuses acks `success = false` with the
    /// structured reason — the loud signal a send-and-forget register
    /// discards on the floor. A DID enrolled with `scope=<space>` is confined
    /// to that space; registering into any other space is refused at the
    /// relay's per-message authorization, which answers with a refusal ack
    /// rather than a transport failure.
    #[tokio::test]
    async fn test_register_document_acked_refusal_carries_reason() {
        let (addr, keys) = start_test_relay().await;
        let in_scope = "5171e0a1-1111-4000-8000-00000000aa03";
        let out_of_scope = "5171e0a1-1111-4000-8000-00000000aa05";
        let token = enroll_and_mint(&addr, &keys, Some(in_scope)).await;
        let mut client =
            SyncClient::connect_with_auth(&format!("ws://{addr}/ws"), "scoped", &token, "scoped")
                .await
                .expect("a scoped key still authenticates");
        let doc = "5171e0a1-2222-4000-8000-00000000aa04";
        let ack = client
            .register_document_acked(
                out_of_scope,
                doc,
                "notes.md",
                None,
                RegisterDocumentMetadata::default(),
            )
            .await
            .expect("a refusal is still an ack, not a transport error");
        assert!(
            !ack.success,
            "an out-of-scope registration acks failure: {ack:?}"
        );
        assert!(
            ack.error.is_some(),
            "the refusal carries the relay's structured reason"
        );
        assert_eq!(
            ack.document_id, doc,
            "the refusal is correlated to the request"
        );

        // The register door validates the path with the same rule as the
        // MCP door: a traversal path is refused with a structured reason,
        // never registered.
        let traversal = client
            .register_document_acked(
                in_scope,
                "5171e0a1-2222-4000-8000-00000000aa09",
                "../escape.md",
                None,
                RegisterDocumentMetadata::default(),
            )
            .await
            .expect("a refusal is still an ack");
        assert!(
            !traversal.success,
            "a traversal path is refused: {traversal:?}"
        );
        assert!(
            traversal
                .error
                .as_ref()
                .is_some_and(|e| e.message.contains("..")),
            "the refusal names the traversal rule: {traversal:?}"
        );
    }

    /// `close` keeps the socket open until the relay has processed every
    /// frame ahead of the close, and surfaces a refusal the client wrote
    /// without awaiting an ack: a register into a space the key is not
    /// scoped to, sent fire-and-forget, comes back as the close's error
    /// instead of vanishing with the dropped socket.
    #[tokio::test]
    async fn test_close_drains_replies_and_reports_a_refused_write() {
        let (addr, keys) = start_test_relay().await;
        let in_scope = "5171e0a1-1111-4000-8000-00000000aa06";
        let out_of_scope = "5171e0a1-1111-4000-8000-00000000aa07";
        let token = enroll_and_mint(&addr, &keys, Some(in_scope)).await;
        let mut client =
            SyncClient::connect_with_auth(&format!("ws://{addr}/ws"), "closer", &token, "closer")
                .await
                .expect("a scoped key still authenticates");
        client
            .register_document(
                out_of_scope,
                "5171e0a1-2222-4000-8000-00000000aa08",
                "notes.md",
                None,
                RegisterDocumentMetadata::default(),
            )
            .await
            .expect("the envelope leaves the socket");
        let err = client
            .close()
            .await
            .expect_err("a refused write surfaces on close");
        assert!(
            err.to_string().contains("refused 1 write"),
            "close names the refusal: {err}"
        );
    }

    /// The refusal of an ops write rides `SyncOpsRejected`, and `close`
    /// surfaces it the same way: `send_ops` into a space the key is not
    /// scoped to comes back as the close's error.
    #[tokio::test]
    async fn test_close_reports_a_rejected_ops_write() {
        let (addr, keys) = start_test_relay().await;
        let in_scope = "5171e0a1-1111-4000-8000-00000000aa16";
        let out_of_scope = "5171e0a1-1111-4000-8000-00000000aa17";
        let token = enroll_and_mint(&addr, &keys, Some(in_scope)).await;
        let mut client =
            SyncClient::connect_with_auth(&format!("ws://{addr}/ws"), "closer", &token, "closer")
                .await
                .expect("a scoped key still authenticates");
        client
            .send_ops(
                out_of_scope,
                "5171e0a1-2222-4000-8000-00000000aa18",
                vec![1],
                Vec::new(),
            )
            .await
            .expect("the envelope leaves the socket");
        let err = client
            .close()
            .await
            .expect_err("a rejected ops write surfaces on close");
        assert!(
            err.to_string().contains("refused 1 write"),
            "close names the rejection: {err}"
        );
    }
}
