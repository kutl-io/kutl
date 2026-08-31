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

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

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
        Payload::Error(e) => RoutedFrame::Event(Box::new(SyncEvent::Error(e))),
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
    /// The relay rejected the subscribe with an `Error`.
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
        Some(Payload::Error(_)) => SubscribeDisposition::Rejected,
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
    /// Error from relay.
    Error(sync::Error),
    /// Presence update.
    Presence(sync::PresenceUpdate),
    /// Signal from another participant (flag, decision, reply).
    Signal(sync::Signal),
}

/// A sync client that manages a WebSocket connection to a kutl relay.
pub struct SyncClient {
    ws: WsStream,
    client_name: String,
    recovery: RecoveryConfig,
    /// Active subscriptions for auto-resubscribe: `(space_id, document_id)`.
    subscriptions: HashSet<(String, String)>,
    /// Counter for auto-resubscribes triggered by `StaleSubscriber` messages.
    resubscribe_count: u32,
    /// Frames read off the socket by `subscribe` that are not this
    /// subscribe's catch-up (e.g. another doc's ops, a signal, presence).
    /// `recv` drains this before reading the socket so no frame is dropped.
    inbox: VecDeque<sync::SyncEnvelope>,
}

/// Upper bound on waiting for a correlated reply (a submit's `SignalAck`, a
/// subscribe's `SignalPage`). Without it these loops waited forever, parking
/// every other frame on the inbox while they did — a relay that dies
/// mid-conversation hung the caller (the ingestion worker's reply path
/// included) instead of erroring. Generous: covers a slow relay persisting a
/// 100-record chunk, not an interactive UX budget.
const REPLY_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(30));

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
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .context("failed to connect to relay")?;

        let mut client = Self {
            ws,
            client_name: client_name.to_owned(),
            recovery,
            subscriptions: HashSet::new(),
            resubscribe_count: 0,
            inbox: VecDeque::new(),
        };
        client.handshake_with_auth(url, &auth).await?;
        Ok(client)
    }

    /// Perform the handshake exchange carrying an auth token + display name.
    async fn handshake_with_auth(&mut self, url: &str, auth: &AuthHeader) -> Result<()> {
        self.send_envelope(&handshake_envelope_with_token(
            &self.client_name,
            &auth.token,
            &auth.display_name,
        ))
        .await?;

        let envelope = self.recv_envelope().await?;
        match envelope.payload {
            Some(Payload::HandshakeAck(ack)) => {
                match kutl_proto::protocol::verify_ack_versions(&ack) {
                    Ok(()) => Ok(()),
                    Err(e) => bail!(e),
                }
            }
            // Read the relay's code, never its prose: an auth refusal names the
            // credential slot to change, and every other refusal states its own
            // remedy already.
            Some(Payload::Error(e)) => {
                let refusal = kutl_proto::protocol::handshake_refusal(&e);
                if refusal.auth_failed {
                    bail!(crate::credentials::refused_token_remedy(url));
                }
                bail!("relay refused the connection: {}", refusal.message)
            }
            other => bail!("expected HandshakeAck, got {other:?}"),
        }
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
                    let Some(Payload::Error(e)) = env.payload else {
                        unreachable!("Rejected disposition implies Error payload")
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

    /// Register a new document in the relay's space registry.
    ///
    /// `provenance` carries source provenance. The daemon path populates
    /// `originally_created_at_ms` only. The ingestion worker populates
    /// all six fields. Other callers pass
    /// [`RegisterDocumentMetadata::default()`].
    pub async fn register_document(
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

        let deadline = tokio::time::Instant::now() + REPLY_TIMEOUT;
        loop {
            let env = tokio::time::timeout_at(deadline, self.recv_envelope())
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "no ack for submit {client_ref} within {REPLY_TIMEOUT:?};                          the relay may be gone — retry on a fresh connection"
                    )
                })??;
            match env.payload {
                Some(Payload::SignalAck(ack)) if ack.client_ref == client_ref => {
                    if ack.success {
                        return Ok(ack);
                    }
                    anyhow::bail!(
                        "{}",
                        ack.error
                            .unwrap_or_else(|| "the relay refused the submit".to_owned())
                    );
                }
                // Someone else's ack, or an unrelated frame. Keep it for `recv`
                // rather than swallowing it.
                _ => self.inbox.push_back(env),
            }
        }
    }

    /// Subscribe to a space's signal stream and take the backlog page it
    /// answers with.
    ///
    /// ONE page per subscribe: the relay answers a `SubscribeSignals` with a
    /// single `SignalPage` and expects the caller to re-subscribe with the
    /// returned cursor for more. The cursor advances only after
    /// a page is enqueued, so a retry re-sends rather than skips.
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
        let deadline = tokio::time::Instant::now() + REPLY_TIMEOUT;
        loop {
            let env = tokio::time::timeout_at(deadline, self.recv_envelope())
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "no signal page for space {space_id} within {REPLY_TIMEOUT:?};                          the relay may be gone — retry on a fresh connection"
                    )
                })??;
            match env.payload {
                Some(Payload::SignalPage(page)) => return Ok(page),
                Some(Payload::StaleSignalStream(s)) => {
                    anyhow::bail!("signal stream unavailable: {}", s.reason)
                }
                _ => self.inbox.push_back(env),
            }
        }
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

    /// Gracefully close the WebSocket connection.
    pub async fn close(mut self) -> Result<()> {
        let _ = self.ws.close(None).await;
        Ok(())
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
        // Storeless boot: these connection tests use in-memory
        // registries; the OSS binary's build_app requires a data dir, so
        // construct the kutlhub-shaped storeless relay via the host seam instead.
        let (app, _handle, _flush_handle) = kutl_relay::testing::build_storeless_app(
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
        let secret: [u8; 32] = std::array::from_fn(|_| rand::random::<u8>());
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret);
        let did = kutl_signals::did_key_encode(&signing_key.verifying_key());
        writeln!(keys.as_file(), "{did}").expect("enroll did in authorized_keys");
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
}
