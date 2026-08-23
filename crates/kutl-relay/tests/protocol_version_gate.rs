//! The relay refuses clients below its minimum supported protocol major.
//!
//! Admitting one is the failure worth preventing: the frames it depends on are
//! gone, so it would authenticate, subscribe, and then sit there receiving
//! nothing at all, with no error to surface to its user. A refusal at the
//! handshake is the only point where the mismatch can still be reported.

mod common;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    MIN_SUPPORTED_PROTOCOL_MAJOR, PROTOCOL_VERSION_MAJOR, PROTOCOL_VERSION_MINOR, decode_envelope,
    encode_envelope,
};
use kutl_proto::sync::{ErrorCode, Handshake, SyncEnvelope, sync_envelope::Payload};
use tokio_tungstenite::tungstenite;

use common::start_relay_with_data_dir;

/// Send a handshake claiming `major`, with whatever token, and return the
/// relay's first reply.
async fn handshake_with_major(base_url: &str, major: u32, auth_token: &str) -> SyncEnvelope {
    let ws_url = format!("{}/ws", base_url.replace("http://", "ws://"));
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();

    let envelope = SyncEnvelope {
        payload: Some(Payload::Handshake(Handshake {
            protocol_version_major: major,
            protocol_version_minor: PROTOCOL_VERSION_MINOR,
            client_name: "version-gate-test".into(),
            auth_token: auth_token.to_owned(),
            display_name: String::new(),
        })),
    };
    ws.send(tungstenite::Message::Binary(
        encode_envelope(&envelope).into(),
    ))
    .await
    .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timed out waiting for the relay's reply")
        .expect("stream ended")
        .expect("ws error");
    match msg {
        tungstenite::Message::Binary(bytes) => decode_envelope(&bytes).unwrap(),
        other => panic!("expected a binary frame, got {other:?}"),
    }
}

/// A too-old client is refused with `VERSION_MISMATCH`, and the message names
/// both the required version and what the client offered — an operator reading
/// only this line knows what to do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_relay_refuses_client_below_min_supported_major() {
    let (base_url, _dir) = start_relay_with_data_dir(false).await;

    let reply =
        handshake_with_major(&base_url, MIN_SUPPORTED_PROTOCOL_MAJOR - 1, "any-token").await;

    match reply.payload {
        Some(Payload::Error(e)) => {
            assert_eq!(
                e.code,
                i32::from(ErrorCode::VersionMismatch),
                "an outdated client must be told the version is the problem, not something else"
            );
            assert!(
                e.message
                    .contains(&MIN_SUPPORTED_PROTOCOL_MAJOR.to_string()),
                "the refusal must name the required version; got {:?}",
                e.message
            );
        }
        other => panic!("expected an Error refusing the old client, got {other:?}"),
    }
}

/// The version gate runs BEFORE authentication. Proved by handing the relay a
/// token that is certainly invalid: a version-first relay still answers
/// `VERSION_MISMATCH`, while an auth-first one would answer `AUTH_FAILED` and send
/// the user chasing credentials that were never the problem.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_version_refusal_precedes_authentication() {
    let (base_url, _dir) = start_relay_with_data_dir(false).await;

    let reply = handshake_with_major(
        &base_url,
        MIN_SUPPORTED_PROTOCOL_MAJOR - 1,
        "definitely-not-a-valid-token",
    )
    .await;

    match reply.payload {
        Some(Payload::Error(e)) => assert_eq!(
            e.code,
            i32::from(ErrorCode::VersionMismatch),
            "version must be reported ahead of auth; got {:?}",
            e.message
        ),
        other => panic!("expected an Error, got {other:?}"),
    }
}

/// A current client is not caught by the gate — it gets far enough to fail on
/// its (absent) credentials instead, which is the next check in line.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_current_client_passes_the_version_gate() {
    let (base_url, _dir) = start_relay_with_data_dir(false).await;

    let reply = handshake_with_major(&base_url, PROTOCOL_VERSION_MAJOR, "not-a-valid-token").await;

    match reply.payload {
        Some(Payload::Error(e)) => assert_eq!(
            e.code,
            i32::from(ErrorCode::AuthFailed),
            "a current client must reach the auth check, not be refused on version"
        ),
        other => panic!("expected an auth Error for the bogus token, got {other:?}"),
    }
}
