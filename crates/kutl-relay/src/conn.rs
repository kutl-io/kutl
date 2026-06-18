//! Per-connection WebSocket task.

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::sync::sync_envelope::Payload;
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::protocol::decode_envelope;
use crate::relay::{ConnId, RelayCommand};

/// Channel capacity for *broadcast* control messages (lifecycle broadcasts,
/// displacement corrections) per connection. Own-acks (handshake,
/// register/rename/unregister, errors, query results) and stale-subscriber
/// notices ride the unbounded ack lane and never touch this channel — so a
/// full ctrl lane evicts a slow subscriber (recoverable via re-sync) rather
/// than dropping an ack.
///
/// Sized for bulk lifecycle storms: a bulk add/move broadcasts one register/
/// rename frame per doc to every other subscriber, and ctrl overflow evicts
/// the WHOLE connection (~30 s reconnect-resync). Frames are small (~0.1–0.5
/// KiB envelopes), so 256 slots cost ~128 KiB per stalled subscriber worst
/// case — negligible next to the data lane.
const CTRL_CAPACITY: usize = 256;

/// Run a single WebSocket connection, bridging it to the relay actor.
pub async fn run_connection(
    socket: WebSocket,
    relay_tx: mpsc::Sender<RelayCommand>,
    conn_id: ConnId,
    outbound_capacity: usize,
) {
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<u8>>(outbound_capacity);
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<Vec<u8>>(CTRL_CAPACITY);
    // Own-ack lane: unbounded so a direct response to this connection's own
    // command (handshake ack, register/rename/unregister acks, errors, query
    // results) is never dropped. A lost ack strands a doc at `confirmed:false`
    // forever (Mode 2). Volume is bounded by upstream backpressure, so the
    // unbounded lane cannot grow without bound for own-acks.
    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    if relay_tx
        .send(RelayCommand::Connect {
            conn_id,
            tx: outbound_tx,
            ctrl_tx,
            ack_tx,
        })
        .await
        .is_err()
    {
        debug!(conn_id, "relay channel closed, dropping connection");
        return;
    }

    info!(conn_id, "websocket connection accepted");

    let (mut ws_sink, mut ws_stream) = socket.split();

    // Write task: forward ack + control + data channels → WebSocket.
    // Biased priority is ack > control > data: own-acks (never dropped) drain
    // first, then control broadcasts (stale notices, lifecycle broadcasts),
    // then the bulk data stream. The data channel closing terminates the write
    // task. The ack and control channels closing is handled gracefully (they
    // just stop being polled).
    let mut write_handle = tokio::spawn(async move {
        let mut ack_open = true;
        let mut ctrl_open = true;
        loop {
            tokio::select! {
                biased;
                msg = ack_rx.recv(), if ack_open => {
                    let Some(bytes) = msg else {
                        ack_open = false;
                        continue;
                    };
                    if ws_sink.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
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

    // Read loop: WebSocket → decode → relay commands. The loop ALSO ends when
    // the write task ends: the writer exits on a socket send error OR when the
    // relay actor drops this connection's lanes (`handle_disconnect`, e.g. the
    // ctrl-overflow eviction in `send_broadcast`). Without that coupling a
    // relay-side disconnect leaves a ZOMBIE: the relay has purged the
    // connection, the writer is gone, but this read half keeps the socket
    // open and axum keeps auto-answering the client's pings — so the client's
    // inbound-liveness deadline stays fed forever and it waits on a
    // connection that no longer exists (the N=5000 bulk-add flatline).
    // Breaking here drops both socket halves; the client sees the stream end
    // and reconnects into a full resync.
    loop {
        tokio::select! {
            next = ws_stream.next() => {
                let Some(msg_result) = next else { break };
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
            _ = &mut write_handle => {
                info!(conn_id, "write task ended; closing connection");
                break;
            }
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
        Some(Payload::UnregisterSpace(msg)) => RelayCommand::UnregisterSpaceOp { conn_id, msg },
        Some(Payload::TransferSpaceOwnership(msg)) => {
            RelayCommand::TransferSpaceOwnershipOp { conn_id, msg }
        }
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
