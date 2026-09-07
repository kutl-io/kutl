//! Integration tests for the MCP `create_comment` tool.
//!
//! Covers the agent's two-step comment authoring flow end-to-end:
//! 1. Mint a UUID client-side.
//! 2. Inject `[text]{.cmt #signal-uuid}` into the doc via `edit_document`.
//! 3. Call `create_comment(signal_id=<uuid>, anchor_text=..., message=...)`.
//!
//! Assertions:
//! - The signal lands in relay state with the caller-supplied UUID
//!   (the marker↔signal binding).
//! - `get_signal_detail(<uuid>)` returns the signal with `message`
//!   (comment body) and `anchor_text` (wrapped-span posterity).

mod common;

use std::io::Write;

use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

use common::mcp::McpSession;

/// Start an auth-on relay (auth is unconditional) AND a sqlite-backed
/// `change_backend`. `get_signal_detail` requires the change backend; the
/// `data_dir`'s presence is what wires it up in `build_app`.
async fn start_mcp_relay_with_data_dir() -> (String, tempfile::TempDir, tempfile::NamedTempFile) {
    let dir = tempfile::tempdir().unwrap();
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-mcp-comment-flow".into(),
        data_dir: Some(dir.path().to_path_buf()),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, dir, keys_file)
}

/// Register a space and return the id the RELAY minted for it.
///
/// Persistent-mode relays (with a `SpaceBackend`) gate MCP
/// `create_document` / `upload_blob` against unknown spaces; tests that
/// exercise those tools must pre-register the space the same way a human
/// would.
///
/// The id is not the caller's to choose: `SpaceBackend::register` mints a
/// UUID. Tests use what it returns rather than a literal, which is also what
/// makes them proof that the minted id is a UUID.
async fn register_space_via_http(addr: &str, name: &str) -> String {
    let client = reqwest::Client::new();
    common::register_space(&client, &format!("http://{addr}"), name).await
}

/// Create a document via the `create_document` MCP tool and return its
/// fresh UUID. Used in this file as the prerequisite step before
/// `edit_document` (the marker injection) and `create_comment` (the
/// signal emission).
async fn create_doc(s: &McpSession, space_id: &str, path: &str, content: &str) -> String {
    let result = s
        .call_ok(
            "create_document",
            serde_json::json!({
                "space_id": space_id,
                "path": path,
                "content": content,
            }),
        )
        .await;
    result["document_id"]
        .as_str()
        .expect("create_document returns document_id")
        .to_owned()
}

/// Inject a `[text]{.cmt #signal-uuid}` marker into the document via
/// the `edit_document` MCP tool. Reads first, because an edit names the
/// base it was composed against.
async fn edit_doc(s: &McpSession, space_id: &str, document_id: &str, new_content: &str) {
    let read = s
        .call_ok(
            "read_document",
            serde_json::json!({ "space_id": space_id, "document_id": document_id }),
        )
        .await;
    let base_version = read["version"]
        .as_str()
        .expect("read_document returns version");

    s.call_ok(
        "edit_document",
        serde_json::json!({
            "space_id": space_id,
            "document_id": document_id,
            "base_version": base_version,
            "content": new_content,
            "intent": "inject comment marker",
        }),
    )
    .await;
}

/// Call an authoring tool by name and return the raw JSON-RPC body. The
/// result content's first text item carries `"created flag signal <id>"` on
/// success — plain text, not the JSON payload `call_ok` decodes.
///
/// Takes the tool name because `create_comment` and `create_flag` are
/// separate verbs: the comment flow drives its own, while the
/// stray-parameter test drives `create_flag`.
async fn call_authoring_tool(
    s: &McpSession,
    tool: &str,
    args: serde_json::Value,
) -> serde_json::Value {
    let resp = s
        .request(
            "tools/call",
            serde_json::json!({
                "name": tool,
                "arguments": args,
            }),
        )
        .await;
    assert_eq!(resp.status(), 200);
    resp.json().await.unwrap()
}

/// Full comment authoring flow: marker injection + signal emission, then
/// confirm the signal lands with matching id + `anchor_text`.
#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_comment_flow_end_to_end() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    let space_id = register_space_via_http(&addr, "space-comment").await;
    let s = McpSession::open(&addr, &keys).await;

    // Step 1: create a document with initial content.
    let document_id = create_doc(
        &s,
        "space-comment",
        "notes.md",
        "Hello, the old phrasing world.\n",
    )
    .await;

    // Step 2: agent mints a UUID client-side. This same UUID will be
    // injected into the doc and supplied to create_comment — the
    // marker↔signal binding is load-bearing.
    let signal_uuid = uuid::Uuid::new_v4().to_string();
    let anchor_span = "the old phrasing";

    // Step 3: inject the `[text]{.cmt #uuid}` marker via edit_document.
    let new_content = format!("Hello, [{anchor_span}]{{.cmt #{signal_uuid}}} world.\n");
    edit_doc(&s, "space-comment", &document_id, &new_content).await;

    // Step 4: emit the FLAG_KIND_COMMENT signal with the SAME UUID +
    // wrapped span as posterity snapshot.
    let body = call_authoring_tool(
        &s,
        "create_comment",
        serde_json::json!({
            "space_id": &space_id,
            "document_id": document_id,
            "kind": "comment",
            "message": "rephrase this",
            "audience": "space",
            "signal_id": signal_uuid,
            "anchor_text": anchor_span,
        }),
    )
    .await;
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "create_comment failed: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    // The handler echoes `created flag signal <id>` — extract and confirm
    // it matches the caller-supplied UUID exactly.
    let echoed = text
        .strip_prefix(kutl_relay::mcp_handler::CREATE_FLAG_OK_PREFIX)
        .expect("handler must echo the signal id");
    assert_eq!(
        echoed, signal_uuid,
        "relay must honor the caller-supplied signal UUID for marker↔signal binding"
    );

    // Step 5: fetch the signal detail and assert the persisted shape.
    let detail = s
        .call_ok(
            "get_signal_detail",
            serde_json::json!({
                "space_id": "space-comment",
                "signal_id": signal_uuid,
            }),
        )
        .await;
    assert_eq!(detail["id"], signal_uuid);
    // A comment is a FLAG KIND, not a signal type of its own: the
    // semantics and shape are a flag's, and the kind exists because an import from
    // a source system with no other flag kinds has to land somewhere.
    assert_eq!(detail["signal_type"], "flag");
    // `flag_kind` reads back as the canonical NAME, matching
    // create_comment's input — not the enum ordinal "6".
    assert_eq!(
        detail["flag_kind"], "comment",
        "comment signal must persist as FLAG_KIND_COMMENT"
    );
    assert_eq!(detail["message"], "rephrase this");
    assert_eq!(
        detail["anchor_text"], anchor_span,
        "the wrapped span round-trips via `FlagPayload.anchor_text` on the \
         RECORD, not the projection alone, so it survives a rebuild from \
         segments"
    );
    assert_eq!(detail["document_id"], document_id);
}

/// `create_comment` without `signal_id` must error at the parser layer —
/// the marker↔signal binding precondition is load-bearing, and making it a
/// required schema field is the point of the split.
#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_create_comment_without_signal_id_errors() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    let space_id = register_space_via_http(&addr, "space-c1").await;
    let s = McpSession::open(&addr, &keys).await;

    let document_id = create_doc(&s, &space_id, "x.md", "x").await;
    let text = s
        .call_err(
            "create_comment",
            serde_json::json!({
                "space_id": &space_id,
                "document_id": document_id,
                "message": "nit",
                "anchor_text": "x",
            }),
        )
        .await;
    assert!(
        text.contains("signal_id"),
        "expected signal_id error, got: {text}"
    );
}

/// `create_comment` without `anchor_text` must error — the posterity
/// snapshot is required so the sidebar/UI can render the wrapped span
/// independent of the live document state.
#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_create_comment_without_anchor_text_errors() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    let space_id = register_space_via_http(&addr, "space-c2").await;
    let s = McpSession::open(&addr, &keys).await;

    let document_id = create_doc(&s, &space_id, "x.md", "x").await;
    let signal_uuid = uuid::Uuid::new_v4().to_string();
    let text = s
        .call_err(
            "create_comment",
            serde_json::json!({
                "space_id": &space_id,
                "document_id": document_id,
                "message": "nit",
                "signal_id": signal_uuid,
            }),
        )
        .await;
    assert!(
        text.contains("anchor_text"),
        "expected anchor_text error, got: {text}"
    );
}

/// A non-comment flag tolerates stray `signal_id` and `anchor_text` —
/// the parser drops them silently (lean: forgiving). Confirms the flag
/// lands with the relay-minted id (not the supplied one) and no anchor
/// on `flag_details`.
#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_create_flag_info_with_stray_comment_params_silently_ignored() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    let space_id = register_space_via_http(&addr, "space-info").await;
    let s = McpSession::open(&addr, &keys).await;

    let document_id = create_doc(&s, &space_id, "x.md", "x").await;
    let supplied_uuid = uuid::Uuid::new_v4().to_string();

    let body = call_authoring_tool(
        &s,
        "create_flag",
        serde_json::json!({
            "space_id": &space_id,
            "document_id": document_id,
            "kind": "info",
            "message": "fyi",
            "audience": "space",
            "signal_id": supplied_uuid,
            "anchor_text": "should be dropped",
        }),
    )
    .await;
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "info flag with stray comment params must succeed: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let echoed = text
        .strip_prefix("created flag signal ")
        .expect("handler must echo a signal id");
    assert_ne!(
        echoed, supplied_uuid,
        "non-comment kind must mint a fresh UUID — supplied signal_id is silently dropped"
    );

    let detail = s
        .call_ok(
            "get_signal_detail",
            serde_json::json!({
                "space_id": &space_id,
                "signal_id": echoed,
            }),
        )
        .await;
    // flag_kind reads back as the canonical NAME "info", not "1".
    assert_eq!(detail["flag_kind"], "info");
    assert!(
        detail["anchor_text"].is_null(),
        "non-comment flag must persist anchor_text=NULL even when supplied; got: {}",
        detail["anchor_text"]
    );
}
