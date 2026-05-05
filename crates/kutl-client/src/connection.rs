//! WebSocket connection and sync protocol management.

use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    decode_envelope, encode_envelope, handshake_envelope, subscribe_envelope, sync_ops_envelope,
    unsubscribe_envelope,
};
use kutl_proto::sync::{self, sync_envelope::Payload};
use tokio_tungstenite::tungstenite;

use crate::recovery::RecoveryConfig;

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Timeout for waiting on a subscribe catch-up response.
///
/// 500ms is generous for a LAN/localhost round-trip. If the relay has content,
/// the catch-up `SyncOps` arrives in the first frame after subscribe.
/// A timeout just means the document was empty — not an error.
const SUBSCRIBE_CATCHUP_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_millis(500));

/// Events received from the relay.
#[derive(Debug)]
pub enum SyncEvent {
    /// Sync operations (text or blob).
    Ops(sync::SyncOps),
    /// Relay evicted this subscription -- client should resubscribe.
    Stale(sync::StaleSubscriber),
    /// Error from relay.
    Error(sync::Error),
    /// Change notification.
    ChangeNotify(sync::ChangeNotify),
    /// Presence update.
    Presence(sync::PresenceUpdate),
    /// Signal from another participant (flag, chat, decision, reply).
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
}

impl SyncClient {
    /// Connect to a relay and perform the handshake.
    pub async fn connect(url: &str, client_name: &str) -> Result<Self> {
        Self::connect_with_recovery(url, client_name, RecoveryConfig::default()).await
    }

    /// Connect to a relay with custom recovery configuration.
    pub async fn connect_with_recovery(
        url: &str,
        client_name: &str,
        recovery: RecoveryConfig,
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
        };
        client.handshake().await?;
        Ok(client)
    }

    /// Perform the handshake exchange with the relay.
    async fn handshake(&mut self) -> Result<()> {
        self.send_envelope(&handshake_envelope(&self.client_name))
            .await?;

        let envelope = self.recv_envelope().await?;
        match envelope.payload {
            Some(Payload::HandshakeAck(_)) => Ok(()),
            Some(Payload::Error(e)) => bail!("handshake rejected: {}", e.message),
            other => bail!("expected HandshakeAck, got {other:?}"),
        }
    }

    /// Subscribe to a document. Returns catch-up ops if the document has content.
    ///
    /// Returns an error if the relay rejects the subscribe (auth failure,
    /// rate limit, or — per RFD 0066 — an `Inconsistent` / `Unavailable`
    /// read classification). A timeout waiting for catch-up is not an
    /// error — it simply means the document had no content to send.
    ///
    /// RFD 0066 note: when the relay classifies the subscribe as
    /// `Inconsistent` or `Unavailable`, it sends a `SubscribeStatus`
    /// envelope on ctrl and then short-circuits — no `SyncOps` follows.
    /// Treating that as "empty doc" (which is what pre-RFD behavior did)
    /// is exactly the silent data-loss path the fail-closed design is
    /// meant to prevent on the browser client. The daemon now bails
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

        for _ in 0..4 {
            match tokio::time::timeout(SUBSCRIBE_CATCHUP_TIMEOUT, self.recv_envelope()).await {
                Ok(Ok(env)) => match env.payload {
                    Some(Payload::SyncOps(ops)) => return Ok(Some(ops)),
                    Some(Payload::Error(e)) => {
                        // Remove from tracked subscriptions since it was rejected.
                        self.subscriptions
                            .remove(&(space_id.to_owned(), document_id.to_owned()));
                        bail!("subscribe rejected: {} (code {})", e.message, e.code)
                    }
                    Some(Payload::SubscribeStatus(s)) => {
                        match sync::LoadStatus::try_from(s.load_status) {
                            Ok(sync::LoadStatus::Inconsistent) => {
                                self.subscriptions
                                    .remove(&(space_id.to_owned(), document_id.to_owned()));
                                bail!(
                                    "subscribe refused for {space_id}/{document_id}: \
                                     relay classified as inconsistent \
                                     (witness expected_version {}, backend returned no content)",
                                    s.expected_version
                                );
                            }
                            Ok(sync::LoadStatus::Unavailable) => {
                                self.subscriptions
                                    .remove(&(space_id.to_owned(), document_id.to_owned()));
                                bail!(
                                    "subscribe refused for {space_id}/{document_id}: \
                                     relay classified as unavailable ({})",
                                    s.reason
                                );
                            }
                            // Found / Empty / Unspecified: continue to catch-up.
                            _ => {}
                        }
                    }
                    _ => return Ok(None),
                },
                Ok(Err(e)) => return Err(e),
                Err(_) => return Ok(None), // timeout — no catch-up
            }
        }
        Ok(None)
    }

    /// Unsubscribe from a document.
    pub async fn unsubscribe(&mut self, space_id: &str, document_id: &str) -> Result<()> {
        self.subscriptions
            .remove(&(space_id.to_owned(), document_id.to_owned()));

        self.send_envelope(&unsubscribe_envelope(space_id, document_id))
            .await
    }

    /// Send sync operations to the relay.
    pub async fn send_ops(
        &mut self,
        space_id: &str,
        document_id: &str,
        ops: Vec<u8>,
        metadata: Vec<sync::ChangeMetadata>,
    ) -> Result<()> {
        self.send_envelope(&sync_ops_envelope(space_id, document_id, ops, metadata))
            .await
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
            let envelope = self.recv_envelope().await?;
            match envelope.payload {
                // Auto-recovery: resubscribe on stale eviction.
                Some(Payload::StaleSubscriber(ref s)) if self.recovery.enabled => {
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
                }
                // Pass-through for all event types.
                Some(Payload::SyncOps(ops)) => return Ok(SyncEvent::Ops(ops)),
                Some(Payload::StaleSubscriber(s)) => return Ok(SyncEvent::Stale(s)),
                Some(Payload::Error(e)) => return Ok(SyncEvent::Error(e)),
                Some(Payload::ChangeNotify(n)) => return Ok(SyncEvent::ChangeNotify(n)),
                Some(Payload::PresenceUpdate(p)) => return Ok(SyncEvent::Presence(p)),
                Some(Payload::Signal(s)) => {
                    return Ok(SyncEvent::Signal(s));
                }
                Some(other) => {
                    tracing::debug!(?other, "ignoring unknown envelope payload");
                }
                None => {}
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

    async fn start_test_relay() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let config = kutl_relay::config::RelayConfig {
            port: 0,
            relay_name: "test-relay".into(),
            ..Default::default()
        };
        let (app, _handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn test_connect_and_handshake() {
        let addr = start_test_relay().await;
        let _client = SyncClient::connect(&format!("ws://{addr}/ws"), "test-client")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_empty_doc() {
        let addr = start_test_relay().await;
        let mut client = SyncClient::connect(&format!("ws://{addr}/ws"), "test")
            .await
            .unwrap();
        let catch_up = client.subscribe("space", "doc").await.unwrap();
        // Relay sends empty ops as a signal that the doc exists but has no content.
        let ops = catch_up.expect("empty doc sends catch-up with empty ops");
        assert!(ops.ops.is_empty(), "ops should be empty for a new doc");
    }

    #[tokio::test]
    async fn test_send_and_recv_ops() {
        let addr = start_test_relay().await;

        let mut client_a = SyncClient::connect(&format!("ws://{addr}/ws"), "a")
            .await
            .unwrap();
        client_a.subscribe("space", "doc").await.unwrap();

        let mut client_b = SyncClient::connect(&format!("ws://{addr}/ws"), "b")
            .await
            .unwrap();
        client_b.subscribe("space", "doc").await.unwrap();

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
            .send_ops("space", "doc", ops, metadata)
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
        let addr = start_test_relay().await;

        // Client A writes.
        let mut client_a = SyncClient::connect(&format!("ws://{addr}/ws"), "a")
            .await
            .unwrap();
        client_a.subscribe("space", "doc").await.unwrap();

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
            .send_ops("space", "doc", ops, metadata)
            .await
            .unwrap();

        // Give relay time to process.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Client B subscribes late -- should get catch-up.
        let mut client_b = SyncClient::connect(&format!("ws://{addr}/ws"), "b")
            .await
            .unwrap();
        let catch_up = client_b.subscribe("space", "doc").await.unwrap();
        assert!(catch_up.is_some(), "late subscriber should get catch-up");

        let ops = catch_up.unwrap();
        let mut doc_b = kutl_core::Document::new();
        doc_b.merge(&ops.ops, &ops.metadata).unwrap();
        assert_eq!(doc_b.content(), "world");
    }

    #[tokio::test]
    async fn test_recv_timeout_returns_none() {
        let addr = start_test_relay().await;
        let mut client = SyncClient::connect(&format!("ws://{addr}/ws"), "test")
            .await
            .unwrap();
        client.subscribe("space", "doc").await.unwrap();

        let event = client
            .recv_timeout(std::time::Duration::from_millis(50))
            .await
            .unwrap();
        assert!(event.is_none());
    }

    #[tokio::test]
    async fn test_connect_with_recovery_config() {
        let addr = start_test_relay().await;
        let config = RecoveryConfig {
            enabled: true,
            ..Default::default()
        };
        let mut client =
            SyncClient::connect_with_recovery(&format!("ws://{addr}/ws"), "test", config)
                .await
                .unwrap();
        client.subscribe("space", "doc").await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_with_recovery_disabled() {
        let addr = start_test_relay().await;
        let config = RecoveryConfig {
            enabled: false,
            ..Default::default()
        };
        let mut client =
            SyncClient::connect_with_recovery(&format!("ws://{addr}/ws"), "test", config)
                .await
                .unwrap();
        // Basic smoke test that disabled recovery does not break connection flow.
        client.subscribe("space", "doc").await.unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_tracks_subscriptions() {
        let addr = start_test_relay().await;
        let mut client = SyncClient::connect(&format!("ws://{addr}/ws"), "test")
            .await
            .unwrap();

        client.subscribe("space-a", "doc-1").await.unwrap();
        client.subscribe("space-b", "doc-2").await.unwrap();
        assert_eq!(client.subscriptions.len(), 2);

        client.unsubscribe("space-a", "doc-1").await.unwrap();
        assert_eq!(client.subscriptions.len(), 1);
        assert!(
            client
                .subscriptions
                .contains(&("space-b".to_owned(), "doc-2".to_owned()))
        );
    }
}
