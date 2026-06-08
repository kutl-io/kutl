//! Integration tests for the MCP `create_flag` tool's `comment` kind
//! (RFD 0083 §2 — comment-via-MCP folded into `create_flag`).
//!
//! Covers the agent's two-step comment authoring flow end-to-end:
//! 1. Mint a UUID client-side.
//! 2. Inject `[text]{.cmt #signal-uuid}` into the doc via `edit_document`.
//! 3. Call `create_flag(kind=comment, signal_id=<uuid>, anchor_text=...)`.
//!
//! Assertions:
//! - The signal lands in relay state with the caller-supplied UUID
//!   (RFD 0077 marker↔signal binding).
//! - `get_signal_detail(<uuid>)` returns the signal with `message`
//!   (comment body) and `anchor_text` (wrapped-span posterity).

mod common;

use std::io::Write;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use reqwest::header;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

/// Start a relay with `require_auth: true` AND a sqlite-backed
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
        require_auth: true,
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

/// Thin wrapper around the shared `common::register_space` helper —
/// builds the full base URL from the relay's `host:port` address and
/// discards the returned canonical UUID (the MCP tests address the
/// space by slug). Persistent-mode relays (with a `SpaceBackend`)
/// gate MCP `create_document` / `upload_blob` against unknown spaces
/// per RFD 0083 §4 (audit 2026-05-20); tests that exercise those
/// tools must pre-register the space the same way a human would.
async fn register_space_via_http(addr: &str, name: &str) {
    let client = reqwest::Client::new();
    common::register_space(&client, &format!("http://{addr}"), name).await;
}

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

async fn mcp_request(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    session_id: Option<&str>,
    method: &str,
    params: serde_json::Value,
) -> reqwest::Response {
    let mut req = client
        .post(format!("http://{addr}/mcp"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }));

    if let Some(sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }

    req.send().await.unwrap()
}

async fn mcp_session(addr: &str, keys_file: &tempfile::NamedTempFile) -> (String, String, String) {
    let (did, signing_key) = common::test_keypair();
    writeln!(&*keys_file, "{did}").unwrap();
    let token = authenticate(addr, &did, &signing_key).await;
    let client = reqwest::Client::new();

    let resp = mcp_request(
        &client,
        addr,
        &token,
        None,
        "initialize",
        serde_json::json!({
            "protocolVersion": "2025-03-26",
            "clientInfo": {"name": "test-agent", "version": "0.1"}
        }),
    )
    .await;

    let session_id = resp
        .headers()
        .get("mcp-session-id")
        .expect("initialize should return mcp-session-id header")
        .to_str()
        .unwrap()
        .to_owned();

    (session_id, token, did)
}

/// Create a document via the `create_document` MCP tool and return its
/// fresh UUID. Used in this file as the prerequisite step before
/// `edit_document` (the marker injection) and `create_flag` (the
/// signal emission).
async fn create_doc(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    session_id: &str,
    space_id: &str,
    path: &str,
    content: &str,
) -> String {
    let resp = mcp_request(
        client,
        addr,
        token,
        Some(session_id),
        "tools/call",
        serde_json::json!({
            "name": "create_document",
            "arguments": {
                "space_id": space_id,
                "path": path,
                "content": content,
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "create_document failed: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
    parsed["document_id"].as_str().unwrap().to_owned()
}

/// Inject a `[text]{.cmt #signal-uuid}` marker into the document via
/// the `edit_document` MCP tool. Returns the new document content.
async fn edit_doc(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    session_id: &str,
    space_id: &str,
    document_id: &str,
    new_content: &str,
) {
    let resp = mcp_request(
        client,
        addr,
        token,
        Some(session_id),
        "tools/call",
        serde_json::json!({
            "name": "edit_document",
            "arguments": {
                "space_id": space_id,
                "document_id": document_id,
                "content": new_content,
                "intent": "inject comment marker",
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "edit_document failed: {body}"
    );
}

/// Call `create_flag` and return the body. The result content's first
/// text item carries `"created flag signal <id>"` on success.
async fn call_create_flag(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    session_id: &str,
    args: serde_json::Value,
) -> serde_json::Value {
    let resp = mcp_request(
        client,
        addr,
        token,
        Some(session_id),
        "tools/call",
        serde_json::json!({
            "name": "create_flag",
            "arguments": args,
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    resp.json().await.unwrap()
}

/// Read a flag signal's full detail via the `get_signal_detail` MCP
/// tool. Returns the parsed `SignalDetail` JSON.
async fn get_signal_detail(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    session_id: &str,
    space_id: &str,
    signal_id: &str,
) -> serde_json::Value {
    let resp = mcp_request(
        client,
        addr,
        token,
        Some(session_id),
        "tools/call",
        serde_json::json!({
            "name": "get_signal_detail",
            "arguments": {
                "space_id": space_id,
                "signal_id": signal_id,
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "get_signal_detail failed: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

/// Full comment authoring flow: marker injection + signal emission, then
/// confirm the signal lands with matching id + `anchor_text`.
#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_comment_flow_end_to_end() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    register_space_via_http(&addr, "space-comment").await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    // Step 1: create a document with initial content.
    let document_id = create_doc(
        &client,
        &addr,
        &token,
        &session_id,
        "space-comment",
        "notes.md",
        "Hello, the old phrasing world.\n",
    )
    .await;

    // Step 2: agent mints a UUID client-side. This same UUID will be
    // injected into the doc and supplied to create_flag — the binding
    // is load-bearing per RFD 0077.
    let signal_uuid = uuid::Uuid::new_v4().to_string();
    let anchor_span = "the old phrasing";

    // Step 3: inject the `[text]{.cmt #uuid}` marker via edit_document.
    let new_content = format!("Hello, [{anchor_span}]{{.cmt #{signal_uuid}}} world.\n");
    edit_doc(
        &client,
        &addr,
        &token,
        &session_id,
        "space-comment",
        &document_id,
        &new_content,
    )
    .await;

    // Step 4: emit the FLAG_KIND_COMMENT signal with the SAME UUID +
    // wrapped span as posterity snapshot.
    let body = call_create_flag(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space-comment",
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
        "create_flag(comment) failed: {body}"
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
    let detail = get_signal_detail(
        &client,
        &addr,
        &token,
        &session_id,
        "space-comment",
        &signal_uuid,
    )
    .await;
    assert_eq!(detail["id"], signal_uuid);
    assert_eq!(detail["signal_type"], "flag");
    // `flag_kind` is the proto enum value rendered as a string; comment = 6.
    assert_eq!(
        detail["flag_kind"], "6",
        "comment signal must persist as FLAG_KIND_COMMENT"
    );
    assert_eq!(detail["message"], "rephrase this");
    assert_eq!(
        detail["anchor_text"], anchor_span,
        "wrapped span must persist on flag_details.anchor_text per RFD 0077"
    );
    assert_eq!(detail["document_id"], document_id);
}

/// Comment kind without `signal_id` must error at the parser layer —
/// the marker↔signal binding precondition is load-bearing.
#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_create_flag_comment_without_signal_id_errors() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    register_space_via_http(&addr, "space-c1").await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let document_id =
        create_doc(&client, &addr, &token, &session_id, "space-c1", "x.md", "x").await;
    let body = call_create_flag(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space-c1",
            "document_id": document_id,
            "kind": "comment",
            "message": "nit",
            "audience": "space",
            "anchor_text": "x",
        }),
    )
    .await;
    assert!(
        body["result"]["isError"].as_bool().unwrap_or(false),
        "expected isError=true, got: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("signal_id is required"),
        "expected signal_id error, got: {text}"
    );
}

/// Comment kind without `anchor_text` must error — the posterity
/// snapshot is required so the sidebar/UI can render the wrapped span
/// independent of the live document state.
#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_create_flag_comment_without_anchor_text_errors() {
    let (addr, _dir, keys) = start_mcp_relay_with_data_dir().await;
    register_space_via_http(&addr, "space-c2").await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let document_id =
        create_doc(&client, &addr, &token, &session_id, "space-c2", "x.md", "x").await;
    let signal_uuid = uuid::Uuid::new_v4().to_string();
    let body = call_create_flag(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space-c2",
            "document_id": document_id,
            "kind": "comment",
            "message": "nit",
            "audience": "space",
            "signal_id": signal_uuid,
        }),
    )
    .await;
    assert!(
        body["result"]["isError"].as_bool().unwrap_or(false),
        "expected isError=true, got: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("anchor_text is required"),
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
    register_space_via_http(&addr, "space-info").await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let document_id = create_doc(
        &client,
        &addr,
        &token,
        &session_id,
        "space-info",
        "x.md",
        "x",
    )
    .await;
    let supplied_uuid = uuid::Uuid::new_v4().to_string();

    let body = call_create_flag(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space-info",
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

    let detail = get_signal_detail(&client, &addr, &token, &session_id, "space-info", echoed).await;
    // flag_kind = 1 = FLAG_KIND_INFO.
    assert_eq!(detail["flag_kind"], "1");
    assert!(
        detail["anchor_text"].is_null(),
        "non-comment flag must persist anchor_text=NULL even when supplied; got: {}",
        detail["anchor_text"]
    );
}
