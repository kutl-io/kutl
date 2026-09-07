mod common;

use std::io::Write;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use kutl_proto::protocol::{
    decode_envelope, encode_envelope, handshake_envelope_with_token, subscribe_envelope,
    sync_ops_envelope,
};
use kutl_proto::sync::{self, sync_envelope::Payload};
use reqwest::header;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite;

use kutl_relay::config::RelayConfig;

use common::mcp::{McpSession, mcp_request};

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Start an auth-on relay (auth is unconditional). Returns `(addr, keys_file)`.
/// Tests must write their DID to `keys_file` before making space-scoped
/// tool calls — `AuthorizedKeys` re-reads the file on every check.
async fn start_mcp_relay() -> (String, tempfile::NamedTempFile) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-mcp-relay".into(),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };

    // In-memory boot: these tests create documents via implicit-create in
    // unregistered spaces — what this shape shares with the hosted relay is
    // having no space backend. The durable harness
    // `start_mcp_relay_persistent` (data_dir set) covers the reject.
    let (app, _relay_handle, _flush_handle) = common::build_in_memory_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys_file)
}

/// Create a WS test client with auth.
struct WsTestClient {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

impl WsTestClient {
    async fn connect_with_auth(addr: &str, token: &str) -> Self {
        let url = format!("ws://{addr}/ws");
        let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let mut client = Self { ws };
        // Send authenticated handshake.
        let envelope =
            handshake_envelope_with_token("c0f26df1-20b4-46a1-8a8f-54bf2334679a", token, "");
        let bytes = encode_envelope(&envelope);
        client
            .ws
            .send(tungstenite::Message::Binary(bytes.into()))
            .await
            .unwrap();
        // Recv HandshakeAck.
        let msg = tokio::time::timeout(Duration::from_secs(5), client.ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("ws error");
        match msg {
            tungstenite::Message::Binary(bytes) => {
                let env = decode_envelope(&bytes).unwrap();
                assert!(
                    matches!(env.payload, Some(Payload::HandshakeAck(_))),
                    "expected HandshakeAck"
                );
            }
            other => panic!("expected binary frame, got {other:?}"),
        }
        client
    }

    async fn subscribe(&mut self, space: &str, doc: &str) -> Option<sync::SyncOps> {
        let envelope = subscribe_envelope(space, doc);
        let bytes = encode_envelope(&envelope);
        self.ws
            .send(tungstenite::Message::Binary(bytes.into()))
            .await
            .unwrap();

        // Drain envelopes until we see `SyncOps` (the catch-up). A
        // `SubscribeStatus` envelope on the ctrl channel arrives before
        // the catch-up due to `biased` in conn's select!. Tests that
        // don't need it can ignore it; others can inspect it via a
        // dedicated helper.
        for _ in 0..3 {
            match tokio::time::timeout(Duration::from_millis(200), self.ws.next()).await {
                Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => {
                    let env = decode_envelope(&bytes).unwrap();
                    match env.payload {
                        Some(Payload::SyncOps(ops)) => return Some(ops),
                        Some(Payload::SubscribeStatus(_)) => {}
                        _ => return None,
                    }
                }
                _ => return None,
            }
        }
        None
    }

    async fn send_ops(
        &mut self,
        space: &str,
        doc_id: &str,
        ops: Vec<u8>,
        metadata: Vec<sync::ChangeMetadata>,
    ) {
        let envelope = sync_ops_envelope(
            space,
            doc_id,
            ops,
            metadata,
            std::collections::HashMap::new(),
        );
        let bytes = encode_envelope(&envelope);
        self.ws
            .send(tungstenite::Message::Binary(bytes.into()))
            .await
            .unwrap();
    }
}

/// Create a document via `create_document` and return the assigned
/// `document_id` UUID. Tests that seed a document via MCP must call this
/// before `edit_document` — `edit_document` does not auto-create.
async fn create_doc_via_mcp(s: &McpSession, space_id: &str, path: &str, content: &str) -> String {
    let result = s
        .call_ok(
            "create_document",
            serde_json::json!({
                "space_id": space_id,
                "path": path,
                "content": content
            }),
        )
        .await;
    result["document_id"]
        .as_str()
        .expect("create_document returns document_id")
        .to_owned()
}

/// Read a document via `read_document` and return the version token it
/// minted. Tests that seed a document must call this before `edit_document`
/// — an edit names the base it was composed against.
async fn read_version_via_mcp(s: &McpSession, space_id: &str, document_id: &str) -> String {
    let result = s
        .call_ok(
            "read_document",
            serde_json::json!({ "space_id": space_id, "document_id": document_id }),
        )
        .await;
    result["version"]
        .as_str()
        .expect("read_document returns version")
        .to_owned()
}

/// Create a local document with a single edit, returning ops bytes and changes.
fn make_edit(agent_name: &str, text: &str, intent: &str) -> (Vec<u8>, Vec<sync::ChangeMetadata>) {
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent(agent_name).unwrap();
    doc.edit(
        agent,
        agent_name,
        intent,
        kutl_core::Boundary::Explicit,
        |ctx| ctx.insert(0, text),
    )
    .unwrap();
    let ops = doc.encode_since(&[]);
    let metadata = doc.changes_since(&[]);
    (ops, metadata)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mcp_initialize() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;
    assert!(
        s.session_id.starts_with("mcp_"),
        "session ID should have mcp_ prefix"
    );
}

/// `InitializeResult.instructions` must contain the default template
/// content (agent instructions).
#[tokio::test]
async fn test_mcp_initialize_includes_instructions() {
    let (addr, keys) = start_mcp_relay().await;
    let (did, signing_key) = common::test_keypair();
    common::authorize_did(keys.path(), &did);
    let token = common::authenticate(&format!("http://{addr}"), &did, &signing_key).await;
    let client = reqwest::Client::new();

    let resp = mcp_request(
        &client,
        &addr,
        &token,
        None,
        "initialize",
        serde_json::json!({
            "protocolVersion": kutl_relay::mcp::MCP_PROTOCOL_VERSION,
            "clientInfo": {"name": "test-agent", "version": "0.1"}
        }),
    )
    .await;

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let instructions = body["result"]["instructions"]
        .as_str()
        .expect("initialize result should include instructions string");
    assert!(
        instructions.contains("Working with kutl through this relay"),
        "instructions should contain OSS template header"
    );
    // The default-visibility passage is the load-bearing claim of the
    // OSS template — it tells agents that signals reach MCP-connected
    // callers only on OSS deployments and that doc-body content is
    // the lowest-common-denominator reach mechanism. If this regresses,
    // the template lies to agents.
    assert!(
        instructions.contains("Signals here reach MCP-connected agents only"),
        "instructions should contain the OSS default-visibility assertion"
    );
    assert!(
        instructions.contains("write it into the doc body"),
        "instructions should recommend doc-body content as the human-visible fallback"
    );
}

#[tokio::test]
async fn test_mcp_no_auth_rejected() {
    let (addr, _keys) = start_mcp_relay().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("http://{addr}/mcp"))
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": kutl_relay::mcp::MCP_PROTOCOL_VERSION,
                "clientInfo": {"name": "test", "version": "0.1"}
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_mcp_tools_list() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    let resp = s.request("tools/list", serde_json::json!({})).await;

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let tools = body["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"read_document"));
    assert!(names.contains(&"edit_document"));
    assert!(names.contains(&"list_documents"));
    assert!(names.contains(&"read_log"));
    assert!(names.contains(&"list_participants"));
    assert!(names.contains(&"status"));
    assert!(names.contains(&"get_changes"));
    assert!(names.contains(&"create_flag"));
    assert!(names.contains(&"create_reply"));
    assert!(names.contains(&"close_flag"));
    assert!(names.contains(&"get_signal_detail"));
}

#[tokio::test]
async fn test_mcp_tools_call_gate_rejects_unadvertised_react() {
    // `react_to_signal` is advertised only by the kutlhub provider; on this OSS
    // (Noop-provider) relay it is neither listed nor executable. The `tools/call`
    // execution gate must reject a direct call naming it — otherwise the
    // advertised and executable surfaces diverge (the leak this guards against).
    // Removing the gate makes this test fail: `react_to_signal` would parse and
    // dispatch, returning a different error, not the unknown-tool result.
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    // Advertised side: react_to_signal is absent from tools/list.
    let list: serde_json::Value = s
        .request("tools/list", serde_json::json!({}))
        .await
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(
        !names.contains(&"react_to_signal"),
        "OSS relay must not advertise react_to_signal"
    );

    // Executable side: a direct tools/call is rejected as an unknown tool.
    let resp = s
        .request(
            "tools/call",
            serde_json::json!({
                "name": "react_to_signal",
                "arguments": {
                    "space_id": "3314f713-09a4-40c6-8910-0a2ea70c5c53",
                    "signal_id": "22222222-2222-4222-8222-222222222222",
                    "emoji": "\u{1f44d}"
                }
            }),
        )
        .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["result"]["isError"],
        serde_json::json!(true),
        "gated tool must return an error result: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        text.contains("unknown tool") && text.contains("react_to_signal"),
        "gate must report react_to_signal as an unknown tool; got: {body}"
    );
}

#[tokio::test]
async fn test_mcp_edit_then_read() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    // Seed via create_document, then edit via edit_document.
    create_doc_via_mcp(
        &s,
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "doc1",
        "initial",
    )
    .await;

    let base_version =
        read_version_via_mcp(&s, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "doc1").await;

    s.call_ok(
        "edit_document",
        serde_json::json!({
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "document_id": "doc1",
            "base_version": base_version,
            "content": "hello world",
            "intent": "initial content"
        }),
    )
    .await;

    // Read back using the original path name (resolve_doc_id maps it).
    let parsed = s
        .call_ok(
            "read_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "doc1"
            }),
        )
        .await;
    assert_eq!(parsed["content"], "hello world");
}

#[tokio::test]
async fn test_read_document_returns_an_opaque_version_token() {
    // The token is the only route a writer has to say which text it based its
    // edit on, so a read that does not mint one leaves every write unsafe.
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    create_doc_via_mcp(
        &s,
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "doc1",
        "# Menu\n",
    )
    .await;

    let parsed = s
        .call_ok(
            "read_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "doc1"
            }),
        )
        .await;
    let version = parsed["version"].as_str().unwrap();
    assert!(
        version.starts_with("kv1."),
        "expected a version token, got {version:?}"
    );
}

#[tokio::test]
async fn test_mcp_read_log_attribution() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    // Seed (no content → no log entry from create_document path).
    create_doc_via_mcp(&s, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "doc1", "").await;

    // Make an edit.
    let base_version =
        read_version_via_mcp(&s, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "doc1").await;
    s.call_ok(
        "edit_document",
        serde_json::json!({
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "document_id": "doc1",
            "base_version": base_version,
            "content": "traced edit",
            "intent": "test attribution"
        }),
    )
    .await;

    // Read the log.
    let entries = s
        .call_ok(
            "read_log",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "doc1"
            }),
        )
        .await;
    let entries = entries.as_array().expect("read_log returns an array");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["author_did"], s.did);
    assert_eq!(entries[0]["intent"], "test attribution");
}

#[tokio::test]
async fn test_mcp_edit_relays_to_ws() {
    let (addr, keys) = start_mcp_relay().await;

    let s = McpSession::open(&addr, &keys).await;

    // Seed via create_document so we get the UUID, then edit.
    let actual_uuid = create_doc_via_mcp(
        &s,
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "doc1",
        "from MCP",
    )
    .await;

    // Authenticate a WS client and subscribe to the auto-generated UUID.
    let (ws_did, ws_key) = common::test_keypair();
    common::authorize_did(keys.path(), &ws_did);
    let ws_token = common::authenticate(&format!("http://{addr}"), &ws_did, &ws_key).await;
    let mut ws_client = WsTestClient::connect_with_auth(&addr, &ws_token).await;
    let catch_up = ws_client
        .subscribe("f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", &actual_uuid)
        .await;

    // Subscribe should return the existing content as catch-up ops.
    let ops = catch_up.expect("should receive catch-up ops on subscribe");
    assert!(!ops.ops.is_empty());

    // Merge and verify content.
    let mut doc = kutl_core::Document::new();
    doc.merge(&ops.ops, &ops.metadata).unwrap();
    assert_eq!(doc.content(), "from MCP");
}

#[tokio::test]
async fn test_ws_edit_readable_via_mcp() {
    let (addr, keys) = start_mcp_relay().await;

    // WS client creates and edits a document.
    let (ws_did, ws_key) = common::test_keypair();
    common::authorize_did(keys.path(), &ws_did);
    let ws_token = common::authenticate(&format!("http://{addr}"), &ws_did, &ws_key).await;
    let mut ws_client = WsTestClient::connect_with_auth(&addr, &ws_token).await;
    ws_client
        .subscribe("f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "doc1")
        .await;

    let (ops, metadata) = make_edit("ws-peer", "WS content", "ws edit");
    ws_client
        .send_ops(
            "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "doc1",
            ops,
            metadata,
        )
        .await;

    // Give relay time to process.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // MCP client reads the document.
    let s = McpSession::open(&addr, &keys).await;

    let parsed = s
        .call_ok(
            "read_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "doc1"
            }),
        )
        .await;
    assert_eq!(parsed["content"], "WS content");
}

#[tokio::test]
async fn test_mcp_list_documents() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    for doc_id in &["doc1", "doc2"] {
        create_doc_via_mcp(
            &s,
            "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            doc_id,
            &format!("content of {doc_id}"),
        )
        .await;
    }

    // List documents.
    let docs = s
        .call_ok(
            "list_documents",
            serde_json::json!({"space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635"}),
        )
        .await;
    let docs = docs.as_array().expect("list_documents returns an array");
    assert_eq!(docs.len(), 2);

    let paths: Vec<&str> = docs.iter().map(|d| d["path"].as_str().unwrap()).collect();
    assert!(paths.contains(&"doc1"));
    assert!(paths.contains(&"doc2"));
}

#[tokio::test]
async fn test_mcp_status() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    create_doc_via_mcp(&s, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "doc1", "test").await;

    // Check status.
    let status = s
        .call_ok(
            "status",
            serde_json::json!({"space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635"}),
        )
        .await;
    assert_eq!(status["document_count"], 1);
    assert!(status["mcp_session_count"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn test_mcp_delete_session() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    // Delete the session.
    let resp = reqwest::Client::new()
        .delete(format!("http://{addr}/mcp"))
        .header(header::AUTHORIZATION, format!("Bearer {}", s.token))
        .header("mcp-session-id", &s.session_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Subsequent requests should fail (session destroyed → 404).
    let resp = s
        .request(
            "tools/call",
            serde_json::json!({
                "name": "read_document",
                "arguments": {"space_id": "f64551fc-d6f0-4823-8b87-971cfb914464", "document_id": "d1"}
            }),
        )
        .await;

    assert_eq!(resp.status(), 404, "expected 404 after session deletion");
}

#[tokio::test]
async fn test_mcp_sse_notifications() {
    let (addr, keys) = start_mcp_relay().await;

    // Session 1: will listen for SSE notifications.
    let s1 = McpSession::open(&addr, &keys).await;

    // Open SSE stream.
    let sse_client = reqwest::Client::new();
    let mut sse_resp = sse_client
        .get(format!("http://{addr}/mcp"))
        .header(header::AUTHORIZATION, format!("Bearer {}", s1.token))
        .header("mcp-session-id", &s1.session_id)
        .send()
        .await
        .unwrap();
    assert_eq!(sse_resp.status(), 200);

    // Give time for SSE to register.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Session 2: makes an edit that should trigger notification to session 1.
    let s2 = McpSession::open(&addr, &keys).await;

    // Seed the doc, then edit (edit_document does not auto-create).
    create_doc_via_mcp(&s2, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "doc1", "").await;

    let base_version =
        read_version_via_mcp(&s2, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "doc1").await;
    s2.call_ok(
        "edit_document",
        serde_json::json!({
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "document_id": "doc1",
            "base_version": base_version,
            "content": "trigger notification",
            "intent": "test SSE"
        }),
    )
    .await;

    // Read SSE events (with timeout).
    let chunk = tokio::time::timeout(Duration::from_secs(5), sse_resp.chunk())
        .await
        .expect("SSE recv timeout")
        .unwrap()
        .expect("SSE stream ended");

    let text = String::from_utf8_lossy(&chunk);
    assert!(
        text.contains("notifications/document/changed"),
        "expected document changed notification, got: {text}"
    );
    assert!(text.contains("test SSE"), "expected intent in notification");
}

/// Start an auth-on relay (auth is unconditional) AND a `data_dir` (for SQLite-backed
/// change/registry backends) and return its `host:port` address.
///
/// Pre-registers `space1` via `POST /spaces/register` because the
/// persistent-mode relay (with `SQLite` `SpaceBackend`) rejects MCP
/// `create_document` / `upload_blob` against unknown spaces.
/// The tests using this fixture
/// assume `space1` is registered — same as kutlhub production where
/// the human owns space creation.
///
/// The caller must keep the returned `TempDir` alive for the test duration so
/// that the underlying `SQLite` file is not deleted.
async fn start_mcp_relay_with_data_dir() -> (String, tempfile::TempDir, tempfile::NamedTempFile) {
    let dir = tempfile::tempdir().unwrap();
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-mcp-relay-data".into(),
        data_dir: Some(dir.path().to_path_buf()),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Pre-register the space the get_changes tests assume exists.
    // Persistent-mode SpaceBackend gates MCP write against unknown
    // spaces; the relay's own /spaces/register endpoint is how the
    // human-equivalent flow registers them. Uses the shared
    // `common::register_space` helper to keep this consistent with
    // the rest of the test surface.
    let client = reqwest::Client::new();
    common::register_space(
        &client,
        &format!("http://{addr}"),
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
    )
    .await;

    (addr, dir, keys_file)
}

// ---------------------------------------------------------------------------
// get_changes integration tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_get_changes_empty() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    let s = McpSession::open(&addr, &keys).await;

    // Call get_changes with no prior events.
    let parsed = s
        .call_ok(
            "get_changes",
            serde_json::json!({ "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635" }),
        )
        .await;
    assert!(
        parsed["signals"].as_array().unwrap().is_empty(),
        "expected no signals"
    );
    assert!(
        parsed["document_changes"].as_array().unwrap().is_empty(),
        "expected no document changes"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_get_changes_after_edit() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    let editor = McpSession::open(&addr, &keys).await;
    let watcher = McpSession::open(&addr, &keys).await;

    // Editor creates a document.
    create_doc_via_mcp(
        &editor,
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "doc1",
        "hello from editor",
    )
    .await;

    // Watcher calls get_changes — should see the document registration.
    let parsed = watcher
        .call_ok(
            "get_changes",
            serde_json::json!({ "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635" }),
        )
        .await;
    let doc_changes = parsed["document_changes"].as_array().unwrap();
    assert!(
        !doc_changes.is_empty(),
        "expected document changes after edit"
    );

    // Second call returns empty (cursor advanced).
    let parsed = watcher
        .call_ok(
            "get_changes",
            serde_json::json!({ "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635" }),
        )
        .await;
    assert!(
        parsed["signals"].as_array().unwrap().is_empty(),
        "expected no signals on second call"
    );
    assert!(
        parsed["document_changes"].as_array().unwrap().is_empty(),
        "expected no document changes on second call (cursor advanced)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_get_changes_signal() {
    // create_flag's parser requires document_id to be a UUID
    // (the relay registry is UUID-keyed). create_document mints the
    // UUID for us; the test threads it through create_flag.
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    let sender = McpSession::open(&addr, &keys).await;
    let watcher = McpSession::open(&addr, &keys).await;

    let test_doc = create_doc_via_mcp(
        &sender,
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "setup-doc.md",
        "content",
    )
    .await;

    // Advance watcher's cursor past the document registration event.
    watcher
        .call_ok(
            "get_changes",
            serde_json::json!({ "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635" }),
        )
        .await;

    // Sender creates a flag signal. Raw request: create_flag answers in
    // plain text, not the JSON payload `call_ok` decodes.
    sender
        .request(
            "tools/call",
            serde_json::json!({
                "name": "create_flag",
                "arguments": {
                    "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                    "document_id": test_doc,
                    "kind": "review_requested",
                    "message": "please review this",
                    "audience": "space"
                }
            }),
        )
        .await;

    // The signal recording is awaited synchronously in the relay, but the MCP
    // response goes through the HTTP layer. Give it a moment to settle.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Watcher should see the signal.
    let parsed = watcher
        .call_ok(
            "get_changes",
            serde_json::json!({ "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635" }),
        )
        .await;
    let signals = parsed["signals"].as_array().unwrap();
    assert!(!signals.is_empty(), "expected signals");
    // The signal is a proto struct serialized via serde; the oneof payload
    // is a tagged enum: { "payload": { "Flag": { ... } } }.
    let flag = &signals[0]["payload"]["Flag"];
    assert_eq!(flag["message"], "please review this");
}

/// Test that MCP sessions expire after the idle TTL using the standalone relay
/// (no HTTP/TCP, so `tokio::time::pause` works cleanly).
#[tokio::test(start_paused = true)]
async fn test_mcp_session_idle_timeout() {
    use kutl_relay::relay::{Relay, RelayCommand};
    use tokio::sync::oneshot;

    let config = kutl_relay::config::RelayConfig {
        port: 0,
        relay_name: "test-idle".into(),
        outbound_capacity: 16,
        ..Default::default()
    };

    let mut relay = Relay::new_standalone(config);
    let did = "did:key:test-idle-agent";

    // Create a session.
    let (reply_tx, reply_rx) = oneshot::channel();
    relay
        .process_command(RelayCommand::McpCreateSession {
            pat: None,
            did: did.into(),
            reply: reply_tx,
        })
        .await;
    let session_id = reply_rx.await.unwrap();
    assert!(session_id.starts_with("mcp_"));

    // Validate it immediately — should succeed.
    let (reply_tx, reply_rx) = oneshot::channel();
    relay
        .process_command(RelayCommand::McpValidateSession {
            session_id: session_id.clone(),
            expected_did: did.into(),
            reply: reply_tx,
        })
        .await;
    assert!(reply_rx.await.unwrap().is_ok(), "session should be valid");

    // Advance time past the 20-minute idle TTL.
    tokio::time::advance(Duration::from_mins(21)).await;

    // Validate again — should fail (expired).
    let (reply_tx, reply_rx) = oneshot::channel();
    relay
        .process_command(RelayCommand::McpValidateSession {
            session_id: session_id.clone(),
            expected_did: did.into(),
            reply: reply_tx,
        })
        .await;
    assert!(
        reply_rx.await.unwrap().is_err(),
        "session should be expired after 21 minutes"
    );
}

/// Test that the periodic reap command removes idle sessions.
#[tokio::test(start_paused = true)]
async fn test_mcp_session_reap() {
    use kutl_relay::relay::{Relay, RelayCommand};
    use tokio::sync::oneshot;

    let config = kutl_relay::config::RelayConfig {
        port: 0,
        relay_name: "test-reap".into(),
        outbound_capacity: 16,
        ..Default::default()
    };

    let mut relay = Relay::new_standalone(config);

    // Create two sessions.
    let (reply_tx, reply_rx) = oneshot::channel();
    relay
        .process_command(RelayCommand::McpCreateSession {
            pat: None,
            did: "did:key:agent-a".into(),
            reply: reply_tx,
        })
        .await;
    let session_a = reply_rx.await.unwrap();

    let (reply_tx, reply_rx) = oneshot::channel();
    relay
        .process_command(RelayCommand::McpCreateSession {
            pat: None,
            did: "did:key:agent-b".into(),
            reply: reply_tx,
        })
        .await;
    let session_b = reply_rx.await.unwrap();

    // Advance 10 minutes, then touch session B (keeping it alive).
    tokio::time::advance(Duration::from_mins(10)).await;
    let (reply_tx, reply_rx) = oneshot::channel();
    relay
        .process_command(RelayCommand::McpValidateSession {
            session_id: session_b.clone(),
            expected_did: "did:key:agent-b".into(),
            reply: reply_tx,
        })
        .await;
    assert!(
        reply_rx.await.unwrap().is_ok(),
        "session B should still be valid"
    );

    // Advance another 11 minutes (total 21 from creation).
    // Session A is now 21 min idle. Session B is 11 min idle (touched at 10 min).
    tokio::time::advance(Duration::from_mins(11)).await;

    // Reap should remove session A but keep session B.
    relay.process_command(RelayCommand::ReapMcpSessions).await;

    // Session A should be gone.
    let (reply_tx, reply_rx) = oneshot::channel();
    relay
        .process_command(RelayCommand::McpValidateSession {
            session_id: session_a.clone(),
            expected_did: "did:key:agent-a".into(),
            reply: reply_tx,
        })
        .await;
    assert!(
        reply_rx.await.unwrap().is_err(),
        "session A should have been reaped"
    );

    // Session B should still be valid.
    let (reply_tx, reply_rx) = oneshot::channel();
    relay
        .process_command(RelayCommand::McpValidateSession {
            session_id: session_b.clone(),
            expected_did: "did:key:agent-b".into(),
            reply: reply_tx,
        })
        .await;
    assert!(
        reply_rx.await.unwrap().is_ok(),
        "session B should survive reap (recently active)"
    );
}

/// After an MCP edit, `get_changes` should surface the document with `edited_at`
/// set. A subsequent edit should also appear via `get_changes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mcp_get_changes_after_edit_shows_edited_at() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    let agent_a = McpSession::open(&addr, &keys).await;
    let agent_b = McpSession::open(&addr, &keys).await;

    // Agent A creates a document with content.
    create_doc_via_mcp(
        &agent_a,
        "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
        "notes.md",
        "# Notes\n\nHello from agent A.",
    )
    .await;

    // Agent B calls get_changes — should see the document with edited_at.
    let parsed = agent_b
        .call_ok(
            "get_changes",
            serde_json::json!({ "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635" }),
        )
        .await;
    let doc_changes = parsed["document_changes"].as_array().unwrap();
    assert!(!doc_changes.is_empty(), "should have document changes");
    // document_id is a UUID; the path is "notes.md".
    assert_eq!(doc_changes[0]["path"], "notes.md");
    assert!(
        doc_changes[0]["edited_at"].is_number(),
        "edited_at should be set"
    );

    // Agent A edits again.
    let base_version =
        read_version_via_mcp(&agent_a, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635", "notes.md").await;
    agent_a
        .call_ok(
            "edit_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "notes.md",
                "base_version": base_version,
                "content": "# Notes\n\nUpdated by agent A.",
                "intent": "update content"
            }),
        )
        .await;

    // Agent B calls get_changes again — should see the edit.
    let parsed = agent_b
        .call_ok(
            "get_changes",
            serde_json::json!({ "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635" }),
        )
        .await;
    let doc_changes = parsed["document_changes"].as_array().unwrap();
    assert_eq!(doc_changes.len(), 1, "should see the edited document");
    assert_eq!(doc_changes[0]["path"], "notes.md");
}

/// Under mandatory auth, the MCP session DID is bound to the *authenticated*
/// caller (from the bearer), not a client-self-asserted `InitializeParams.did`.
/// `list_participants` must therefore report the authenticated DID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mcp_initialize_binds_authenticated_did() {
    let (base_url, dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    // Enroll a real did:key identity and mint a bearer.
    let (did, signing_key) = common::test_keypair();
    let keys_path = dir.path().join(common::AUTHORIZED_KEYS_FILE);
    common::authorize_did(&keys_path, &did);
    let token = common::authenticate(&base_url, &did, &signing_key).await;

    // Boundary normalization: the relay's MCP handlers
    // canonicalize `space_id` against the space backend at entry, so
    // unregistered slugs error with SpaceNotFound. Pre-register here so
    // the list_participants check we actually care about can run.
    let space_id = common::register_space(&client, &base_url, "any-space").await;

    // Initialize with a self-asserted DID that DIFFERS from the bearer's DID.
    // The relay must ignore the self-asserted value and bind the authenticated
    // one, so the spoof never surfaces in list_participants.
    let resp = client
        .post(format!("{base_url}/mcp"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": kutl_relay::mcp::MCP_PROTOCOL_VERSION,
                "clientInfo": { "name": "test", "version": "0.1" },
                "did": "did:demo:agent-alice"
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let session_id = resp
        .headers()
        .get("Mcp-Session-Id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // The session DID should be the authenticated DID, not the self-asserted
    // "did:demo:agent-alice". Verify via list_participants.
    let resp = client
        .post(format!("{base_url}/mcp"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header("Mcp-Session-Id", &session_id)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "list_participants",
                "arguments": { "space_id": space_id }
            }
        }))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains(&did),
        "participant should be the authenticated DID {did}: {text}"
    );
    assert!(
        !text.contains("did:demo:agent-alice"),
        "self-asserted DID must not override the authenticated identity: {text}"
    );
}
