//! Integration tests for the text export endpoint.
//!
//! `GET /spaces/:space_id/documents/:document_id/text` returns the current
//! document content as `text/plain`. Requires bearer token authentication.

mod common;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    RegisterDocumentMetadata, decode_envelope, encode_envelope, handshake_envelope_with_token,
    register_document_envelope, subscribe_envelope, sync_ops_envelope,
};
use kutl_proto::sync::sync_envelope::Payload;
use reqwest::header;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Start an auth-on relay (auth is unconditional) and return its address.
async fn start_relay() -> (String, tempfile::NamedTempFile) {
    use std::io::Write;
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# text-export test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-text-export-relay".into(),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };

    // In-memory boot: docs are seeded over WS-sync into in-memory
    // registries; no persistence needed. The OSS binary's build_app requires
    // a data dir, so construct the no-backend shape via the test seam instead.
    let (app, _relay_handle, _flush_handle) = common::build_in_memory_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys_file)
}

/// Seed a text document into the relay via WebSocket. Authenticates, subscribes
/// to `space_id/doc_id`, sends the ops, and waits briefly for relay processing.
async fn seed_document(addr: &str, token: &str, space_id: &str, doc_id: &str, text: &str) {
    let url = format!("ws://{addr}/ws");
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let mut ws = ws;

    // Authenticated handshake.
    let envelope = handshake_envelope_with_token("4151706b-a1a1-427f-874d-a69bbb745205", token, "");
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

    let envelope = sync_ops_envelope(
        space_id,
        doc_id,
        ops,
        metadata,
        std::collections::HashMap::new(),
    );
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
/// return that body — a real synchronization point on the merge rather than
/// a fixed sleep before the GET.
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
    let (addr, keys) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    common::authorize_did(keys.path(), &did);
    let token = common::authenticate(&format!("http://{addr}"), &did, &signing_key).await;

    seed_document(
        &addr,
        &token,
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "doc1",
        "hello world",
    )
    .await;

    let body = get_text_export_when_ready(
        &addr,
        &token,
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "doc1",
    )
    .await;
    assert_eq!(body, "hello world");
}

#[tokio::test]
async fn test_export_text_returns_404_for_missing_document() {
    let (addr, keys) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    common::authorize_did(keys.path(), &did);
    let token = common::authenticate(&format!("http://{addr}"), &did, &signing_key).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://{addr}/spaces/f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635/documents/nonexistent/text"
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
        .get(format!(
            "http://{addr}/spaces/f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635/documents/doc1/text"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_export_text_returns_404_for_empty_document() {
    let (addr, keys) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    common::authorize_did(keys.path(), &did);
    let token = common::authenticate(&format!("http://{addr}"), &did, &signing_key).await;

    // Subscribe to create the document slot but don't send any content.
    // This leaves the document in DocContent::Empty state.
    let url = format!("ws://{addr}/ws");
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let mut ws = ws;

    let envelope =
        handshake_envelope_with_token("e69b9b04-0da2-4f0d-8550-e5460a77ffda", &token, "");
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
    let envelope = subscribe_envelope("f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "empty-doc");
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
            "http://{addr}/spaces/f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635/documents/empty-doc/text"
        ))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

/// The "register → `send_ops`" pattern (no prior subscribe) must persist
/// content: `register_document_internal` materializes an empty `DocSlot`,
/// so `handle_sync_ops` has a slot to merge the immediately-following ops
/// into. If the in-memory slot were only created by `subscribe`, those ops
/// would be `NotFound`-dropped while the registry entry was still written
/// by `register` — a registered document whose content reads back empty.
#[tokio::test]
async fn test_register_then_sync_ops_without_subscribe_persists_content() {
    let (addr, keys) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    common::authorize_did(keys.path(), &did);
    let token = common::authenticate(&format!("http://{addr}"), &did, &signing_key).await;

    let url = format!("ws://{addr}/ws");
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let mut ws = ws;

    // Handshake.
    let envelope =
        handshake_envelope_with_token("7f13d00d-a378-471e-83b0-04f6d9d7b91f", &token, "");
    let bytes = encode_envelope(&envelope);
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        bytes.into(),
    ))
    .await
    .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for HandshakeAck")
        .expect("stream ended")
        .expect("ws error");
    match msg {
        tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
            let env = decode_envelope(&bytes).unwrap();
            assert!(matches!(env.payload, Some(Payload::HandshakeAck(_))));
        }
        other => panic!("expected binary frame, got {other:?}"),
    }

    // Register (no subscribe — this is the exact worker pattern).
    let envelope = register_document_envelope(
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "doc1",
        "doc1.md",
        None,
        RegisterDocumentMetadata::default(),
    );
    let bytes = encode_envelope(&envelope);
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        bytes.into(),
    ))
    .await
    .unwrap();

    // Send content ops immediately after register — the exact ops that
    // must not be dropped by `handle_sync_ops` with a NotFound envelope.
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent("worker").unwrap();
    doc.edit(
        agent,
        "worker",
        "initial",
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, "ingested content"),
    )
    .unwrap();
    let ops = doc.encode_since(&[]);
    let metadata = doc.changes_since(&[]);
    let envelope = sync_ops_envelope(
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "doc1",
        ops,
        metadata,
        std::collections::HashMap::new(),
    );
    let bytes = encode_envelope(&envelope);
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        bytes.into(),
    ))
    .await
    .unwrap();

    // Read back via the text export endpoint — must reflect the content
    // the worker just pushed. `get_text_export_when_ready` polls until
    // the relay has merged the ops, so we don't race the merge.
    let body = get_text_export_when_ready(
        &addr,
        &token,
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "doc1",
    )
    .await;
    assert_eq!(body, "ingested content");
}
