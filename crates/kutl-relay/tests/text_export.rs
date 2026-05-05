//! Integration tests for the text export endpoint.
//!
//! `GET /spaces/:space_id/documents/:document_id/text` returns the current
//! document content as `text/plain`. Requires bearer token authentication.

mod common;

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    decode_envelope, encode_envelope, handshake_envelope_with_token, subscribe_envelope,
    sync_ops_envelope,
};
use kutl_proto::sync::sync_envelope::Payload;
use reqwest::header;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Start a relay with `require_auth: true` and return its address.
async fn start_relay() -> (String, tempfile::NamedTempFile) {
    use std::io::Write;
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# text-export test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-text-export-relay".into(),
        require_auth: true,
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys_file)
}

/// Perform the DID challenge-response auth flow and return the bearer token.
async fn authenticate(addr: &str, did: &str, signing_key: &SigningKey) -> String {
    let client = reqwest::Client::new();
    let base_url = format!("http://{addr}");

    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/challenge"))
        .json(&serde_json::json!({"did": did}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let nonce = resp["nonce"].as_str().unwrap();
    let nonce_bytes = URL_SAFE_NO_PAD.decode(nonce).unwrap();
    let signature = signing_key.sign(&nonce_bytes);
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    let resp: serde_json::Value = client
        .post(format!("{base_url}/auth/verify"))
        .json(&serde_json::json!({
            "did": did,
            "nonce": nonce,
            "signature": sig_b64,
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    resp["token"].as_str().unwrap().to_owned()
}

/// Seed a text document into the relay via WebSocket. Authenticates, subscribes
/// to `space_id/doc_id`, sends the ops, and waits briefly for relay processing.
async fn seed_document(addr: &str, token: &str, space_id: &str, doc_id: &str, text: &str) {
    let url = format!("ws://{addr}/ws");
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let mut ws = ws;

    // Authenticated handshake.
    let envelope = handshake_envelope_with_token("text-export-seeder", token, "");
    let bytes = encode_envelope(&envelope);
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        bytes.into(),
    ))
    .await
    .unwrap();

    // Receive HandshakeAck.
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for HandshakeAck")
        .expect("stream ended")
        .expect("ws error");
    match msg {
        tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
            let env = decode_envelope(&bytes).unwrap();
            assert!(
                matches!(env.payload, Some(Payload::HandshakeAck(_))),
                "expected HandshakeAck"
            );
        }
        other => panic!("expected binary frame, got {other:?}"),
    }

    // Subscribe to the document.
    let envelope = subscribe_envelope(space_id, doc_id);
    let bytes = encode_envelope(&envelope);
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        bytes.into(),
    ))
    .await
    .unwrap();

    // Drain any catch-up response.
    let _ = tokio::time::timeout(Duration::from_millis(200), ws.next()).await;

    // Build and send ops.
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent("seeder").unwrap();
    doc.edit(
        agent,
        "seeder",
        "seed content",
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, text),
    )
    .unwrap();
    let ops = doc.encode_since(&[]);
    let metadata = doc.changes_since(&[]);

    let envelope = sync_ops_envelope(space_id, doc_id, ops, metadata);
    let bytes = encode_envelope(&envelope);
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        bytes.into(),
    ))
    .await
    .unwrap();
    // Callers that need to read the seeded content should poll the relay
    // (e.g. via [`get_text_export_when_ready`]); we don't block here.
}

/// Poll the `/text` export endpoint until it returns 200 with a body, then
/// return that body. Replaces a fixed sleep that was waiting for ops to
/// merge before the GET.
async fn get_text_export_when_ready(addr: &str, token: &str, space: &str, doc: &str) -> String {
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/spaces/{space}/documents/{doc}/text");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = client
            .get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        if status == 200 {
            let ct = resp.headers()["content-type"].to_str().unwrap().to_owned();
            assert!(ct.contains("text/plain"), "expected text/plain, got: {ct}");
            return resp.text().await.unwrap();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "text export never became ready for {space}/{doc} (last status={status})",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_export_text_returns_document_content() {
    use std::io::Write;
    let (addr, keys) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    writeln!(&keys, "{did}").unwrap();
    let token = authenticate(&addr, &did, &signing_key).await;

    seed_document(&addr, &token, "space1", "doc1", "hello world").await;

    let body = get_text_export_when_ready(&addr, &token, "space1", "doc1").await;
    assert_eq!(body, "hello world");
}

#[tokio::test]
async fn test_export_text_returns_404_for_missing_document() {
    use std::io::Write;
    let (addr, keys) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    writeln!(&keys, "{did}").unwrap();
    let token = authenticate(&addr, &did, &signing_key).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://{addr}/spaces/space1/documents/nonexistent/text"
        ))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_export_text_returns_401_for_missing_auth() {
    let (addr, _keys) = start_relay().await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/spaces/space1/documents/doc1/text"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_export_text_returns_404_for_empty_document() {
    use std::io::Write;
    let (addr, keys) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    writeln!(&keys, "{did}").unwrap();
    let token = authenticate(&addr, &did, &signing_key).await;

    // Subscribe to create the document slot but don't send any content.
    // This leaves the document in DocContent::Empty state.
    let url = format!("ws://{addr}/ws");
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let mut ws = ws;

    let envelope = handshake_envelope_with_token("empty-test", &token, "");
    let bytes = encode_envelope(&envelope);
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        bytes.into(),
    ))
    .await
    .unwrap();

    // Receive HandshakeAck.
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("ws error");
    match msg {
        tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
            let env = decode_envelope(&bytes).unwrap();
            assert!(
                matches!(env.payload, Some(Payload::HandshakeAck(_))),
                "expected HandshakeAck"
            );
        }
        other => panic!("expected binary frame, got {other:?}"),
    }

    // Subscribe to create the doc slot (Empty state).
    let envelope = subscribe_envelope("space1", "empty-doc");
    let bytes = encode_envelope(&envelope);
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        bytes.into(),
    ))
    .await
    .unwrap();

    // Drain the SubscribeStatus envelope so we know the relay has processed
    // the subscribe. Both pre- and post-process states return 404 here, but
    // this gives the test a real synchronization point rather than a guess.
    let _ = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;

    // Now request text — should be 404 since document is Empty.
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://{addr}/spaces/space1/documents/empty-doc/text"
        ))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}
