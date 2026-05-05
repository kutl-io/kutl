//! WebSocket sync client that connects to a kutl relay.
//!
//! The client handles handshake, subscribe/unsubscribe, and bidirectional
//! `SyncOps` relay. It communicates with the daemon core via channels.

use anyhow::{Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    blob_ops_envelope, decode_envelope, encode_envelope, handshake_envelope_with_token,
    is_blob_mode, list_space_documents_envelope, register_document_envelope,
    rename_document_envelope, subscribe_envelope, sync_ops_envelope, unregister_document_envelope,
    unsubscribe_envelope,
};
use kutl_proto::sync::{ChangeMetadata, sync_envelope::Payload};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};
use tracing::{info, warn};

/// Type alias for the WS sink half.
type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;
/// Type alias for the WS stream half.
type WsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Messages from the sync client to the daemon core.
#[derive(Debug)]
pub enum SyncEvent {
    /// Successfully connected and handshake acknowledged.
    Connected,
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
    },
    /// A document was registered in the registry (from another daemon).
    DocumentRegistered { document_id: String, path: String },
    /// A document was renamed in the registry (from another daemon).
    DocumentRenamed {
        document_id: String,
        old_path: String,
        new_path: String,
    },
    /// A document was unregistered from the registry (from another daemon).
    DocumentUnregistered { document_id: String },
    /// Relay responded with the list of documents in a space.
    SpaceDocuments {
        space_id: String,
        documents: Vec<(String, String)>, // (document_id, path)
    },
    /// Relay rejected authentication — do not retry with the same credentials.
    AuthRejected(String),
    /// An error message from the relay (non-fatal, session may continue).
    Error(String),
}

/// Messages from the daemon core to the sync client.
#[derive(Debug)]
pub enum SyncCommand {
    /// Subscribe to a document on the relay.
    Subscribe { document_id: String },
    /// Unsubscribe from a document.
    Unsubscribe { document_id: String },
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
    },
    /// Rename a document in the relay registry.
    RenameDocument {
        space_id: String,
        document_id: String,
        old_path: String,
        new_path: String,
        metadata: Option<ChangeMetadata>,
    },
    /// Unregister a document from the relay registry.
    UnregisterDocument {
        space_id: String,
        document_id: String,
        metadata: Option<ChangeMetadata>,
    },
    /// Request the list of documents in a space from the relay.
    ListSpaceDocuments { space_id: String },
}

/// Connect to a relay, perform handshake, and run the bidirectional relay.
///
/// Returns when the connection is closed or an unrecoverable error occurs.
/// The caller should handle reconnection with backoff.
pub async fn run_client(
    relay_url: &str,
    space_id: &str,
    client_name: &str,
    auth_token: &str,
    display_name: &str,
    cmd_rx: mpsc::Receiver<SyncCommand>,
    event_tx: mpsc::Sender<SyncEvent>,
) -> Result<()> {
    info!(relay_url, "connecting to relay");

    let (ws_stream, _response) = tokio_tungstenite::connect_async(relay_url)
        .await
        .with_context(|| format!("failed to connect to {relay_url}"))?;

    let (mut ws_sink, mut ws_stream) = ws_stream.split();

    match perform_handshake(
        &mut ws_sink,
        &mut ws_stream,
        client_name,
        auth_token,
        display_name,
    )
    .await
    {
        Ok(()) => {
            let _ = event_tx.send(SyncEvent::Connected).await;
        }
        Err(e) => {
            let _ = event_tx.send(SyncEvent::AuthRejected(e.to_string())).await;
            return Err(e);
        }
    }

    relay_loop(&mut ws_sink, &mut ws_stream, space_id, cmd_rx, &event_tx).await;

    let _ = event_tx.send(SyncEvent::Disconnected).await;
    Ok(())
}

/// Send handshake and wait for ack.
async fn perform_handshake(
    ws_sink: &mut WsSink,
    ws_stream: &mut WsStream,
    client_name: &str,
    auth_token: &str,
    display_name: &str,
) -> Result<()> {
    let envelope = handshake_envelope_with_token(client_name, auth_token, display_name);
    send_binary(ws_sink, &envelope)
        .await
        .context("failed to send handshake")?;

    let first_msg = ws_stream
        .next()
        .await
        .context("connection closed before handshake ack")?
        .context("ws error during handshake")?;

    match first_msg {
        tungstenite::Message::Binary(bytes) => {
            let envelope =
                decode_envelope(&bytes).context("failed to decode handshake response")?;
            match envelope.payload {
                Some(Payload::HandshakeAck(ack)) => {
                    info!(relay_name = %ack.relay_name, "handshake accepted");
                    Ok(())
                }
                Some(Payload::Error(e)) => {
                    anyhow::bail!("relay rejected handshake: {}", e.message)
                }
                _ => anyhow::bail!("unexpected message during handshake"),
            }
        }
        _ => anyhow::bail!("expected binary handshake ack"),
    }
}

/// Bidirectional relay: daemon commands → WS, WS → daemon events.
async fn relay_loop(
    ws_sink: &mut WsSink,
    ws_stream: &mut WsStream,
    space_id: &str,
    mut cmd_rx: mpsc::Receiver<SyncCommand>,
    event_tx: &mpsc::Sender<SyncEvent>,
) {
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(cmd) => {
                        tracing::debug!(?cmd, "sending command to relay");
                        let envelope = command_to_envelope(cmd, space_id);
                        if send_binary(ws_sink, &envelope).await.is_err() {
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

            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(tungstenite::Message::Binary(bytes))) => {
                        tracing::debug!(len = bytes.len(), "received binary frame from relay");
                        if let Err(e) = handle_inbound(&bytes, event_tx).await {
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

/// Convert a `SyncCommand` into a wire envelope.
fn command_to_envelope(cmd: SyncCommand, space_id: &str) -> kutl_proto::sync::SyncEnvelope {
    match cmd {
        SyncCommand::Subscribe { document_id } => subscribe_envelope(space_id, &document_id),
        SyncCommand::Unsubscribe { document_id } => unsubscribe_envelope(space_id, &document_id),
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
                sync_ops_envelope(space_id, &document_id, ops, metadata)
            }
        }
        SyncCommand::RegisterDocument {
            space_id: sid,
            document_id,
            path,
            metadata,
        } => register_document_envelope(&sid, &document_id, &path, metadata),
        SyncCommand::RenameDocument {
            space_id: sid,
            document_id,
            old_path,
            new_path,
            metadata,
        } => rename_document_envelope(&sid, &document_id, &old_path, &new_path, metadata),
        SyncCommand::UnregisterDocument {
            space_id: sid,
            document_id,
            metadata,
        } => unregister_document_envelope(&sid, &document_id, metadata),
        SyncCommand::ListSpaceDocuments { space_id } => list_space_documents_envelope(&space_id),
    }
}

/// Send a protobuf envelope as a binary WS frame.
async fn send_binary(
    ws_sink: &mut WsSink,
    envelope: &kutl_proto::sync::SyncEnvelope,
) -> Result<(), tungstenite::Error> {
    ws_sink
        .send(tungstenite::Message::Binary(
            encode_envelope(envelope).into(),
        ))
        .await
}

/// Process an inbound binary frame from the relay.
async fn handle_inbound(bytes: &[u8], event_tx: &mpsc::Sender<SyncEvent>) -> Result<()> {
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
            };
            let _ = event_tx.send(event).await;
        }
        Some(Payload::RegisterDocument(msg)) => {
            let _ = event_tx
                .send(SyncEvent::DocumentRegistered {
                    document_id: msg.document_id,
                    path: msg.path,
                })
                .await;
        }
        Some(Payload::RenameDocument(msg)) => {
            let _ = event_tx
                .send(SyncEvent::DocumentRenamed {
                    document_id: msg.document_id,
                    old_path: msg.old_path,
                    new_path: msg.new_path,
                })
                .await;
        }
        Some(Payload::UnregisterDocument(msg)) => {
            let _ = event_tx
                .send(SyncEvent::DocumentUnregistered {
                    document_id: msg.document_id,
                })
                .await;
        }
        Some(Payload::ListSpaceDocumentsResult(result)) => {
            let documents = result
                .documents
                .into_iter()
                .map(|d| (d.document_id, d.path))
                .collect();
            let _ = event_tx
                .send(SyncEvent::SpaceDocuments {
                    space_id: result.space_id,
                    documents,
                })
                .await;
        }
        Some(Payload::Error(e)) => {
            let _ = event_tx.send(SyncEvent::Error(e.message)).await;
        }
        _ => {}
    }

    Ok(())
}
