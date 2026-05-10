//! Per-connection WebSocket task.

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::sync::sync_envelope::Payload;
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::protocol::decode_envelope;
use crate::relay::{ConnId, RelayCommand};

/// Channel capacity for control/admin messages (handshake acks, errors,
/// rate-limit rejections, stale notices) per connection.
const CTRL_CAPACITY: usize = 16;

/// Run a single WebSocket connection, bridging it to the relay actor.
pub async fn run_connection(
    socket: WebSocket,
    relay_tx: mpsc::Sender<RelayCommand>,
    conn_id: ConnId,
    outbound_capacity: usize,
) {
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<u8>>(outbound_capacity);
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<Vec<u8>>(CTRL_CAPACITY);

    if relay_tx
        .send(RelayCommand::Connect {
            conn_id,
            tx: outbound_tx,
            ctrl_tx,
        })
        .await
        .is_err()
    {
        debug!(conn_id, "relay channel closed, dropping connection");
        return;
    }

    info!(conn_id, "websocket connection accepted");

    let (mut ws_sink, mut ws_stream) = socket.split();

    // Write task: forward outbound + control channels → WebSocket.
    // Control messages (stale notices) are prioritized via biased select.
    // The data channel closing terminates the write task. The control channel
    // closing is handled gracefully (it just stops being polled).
    let write_handle = tokio::spawn(async move {
        let mut ctrl_open = true;
        loop {
            tokio::select! {
                biased;
                msg = ctrl_rx.recv(), if ctrl_open => {
                    let Some(bytes) = msg else {
                        ctrl_open = false;
                        continue;
                    };
                    if ws_sink.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
                msg = outbound_rx.recv() => {
                    let Some(bytes) = msg else { break };
                    if ws_sink.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Read loop: WebSocket → decode → relay commands.
    while let Some(msg_result) = ws_stream.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                debug!(conn_id, error = %e, "ws read error");
                break;
            }
        };

        match msg {
            Message::Binary(bytes) => {
                if let Err(e) = dispatch_message(conn_id, &bytes, &relay_tx).await {
                    debug!(conn_id, error = %e, "failed to dispatch message");
                }
            }
            Message::Close(_) => {
                info!(conn_id, "ws close frame received");
                break;
            }
            // Ignore ping/pong/text — axum handles ping/pong automatically.
            _ => {}
        }
    }

    info!(conn_id, "websocket connection closed");
    let _ = relay_tx.send(RelayCommand::Disconnect { conn_id }).await;
    write_handle.abort();
}

/// Decode a binary frame and send the appropriate command to the relay.
async fn dispatch_message(
    conn_id: ConnId,
    bytes: &[u8],
    relay_tx: &mpsc::Sender<RelayCommand>,
) -> anyhow::Result<()> {
    let envelope = decode_envelope(bytes)?;

    let cmd = match envelope.payload {
        Some(Payload::Handshake(msg)) => RelayCommand::Handshake { conn_id, msg },
        Some(Payload::Subscribe(msg)) => RelayCommand::Subscribe { conn_id, msg },
        Some(Payload::Unsubscribe(msg)) => RelayCommand::Unsubscribe { conn_id, msg },
        Some(Payload::SyncOps(msg)) => RelayCommand::InboundSyncOps {
            conn_id,
            msg: Box::new(msg),
        },
        Some(Payload::PresenceUpdate(msg)) => RelayCommand::PresenceUpdate { conn_id, msg },
        Some(Payload::Signal(msg)) => RelayCommand::Signal { conn_id, msg },
        Some(Payload::RegisterDocument(msg)) => RelayCommand::RegisterDocument { conn_id, msg },
        Some(Payload::RenameDocument(msg)) => RelayCommand::RenameDocument { conn_id, msg },
        Some(Payload::UnregisterDocument(msg)) => RelayCommand::UnregisterDocument { conn_id, msg },
        Some(Payload::JoinSpace(msg)) => RelayCommand::JoinSpaceOp { conn_id, msg },
        Some(Payload::ResolveSpace(msg)) => RelayCommand::ResolveSpaceOp { conn_id, msg },
        Some(Payload::ListMySpaces(msg)) => RelayCommand::ListMySpacesOp { conn_id, msg },
        Some(Payload::ListSpaceDocuments(msg)) => RelayCommand::ListSpaceDocuments { conn_id, msg },
        // Server-to-client result messages — reject if received from client.
        Some(
            Payload::JoinSpaceResult(_)
            | Payload::ResolveSpaceResult(_)
            | Payload::ListMySpacesResult(_)
            | Payload::ListSpaceDocumentsResult(_),
        ) => {
            anyhow::bail!("received server-only message from client")
        }
        Some(_) => anyhow::bail!("unsupported message type"),
        None => anyhow::bail!("empty envelope"),
    };

    relay_tx.send(cmd).await?;
    Ok(())
}
