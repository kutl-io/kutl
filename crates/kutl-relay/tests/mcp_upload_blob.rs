//! Integration tests for the MCP `upload_blob` tool (RFD 0083 §3).

mod common;

use std::io::Write;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};
use reqwest::header;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

/// Start a relay with the supplied `KUTL_MAX_BLOB_BYTES` cap (passed via
/// `RelayConfig.max_blob_bytes`).
async fn start_relay_with_cap(max_blob_bytes: usize) -> (String, tempfile::NamedTempFile) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-upload-blob".into(),
        require_auth: true,
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        max_blob_bytes,
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys_file)
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
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    (session_id, token, did)
}

async fn call_upload_blob(
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
        serde_json::json!({"name": "upload_blob", "arguments": args}),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "expected success, got: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

async fn call_upload_blob_expect_error(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    session_id: &str,
    args: serde_json::Value,
) -> String {
    let resp = mcp_request(
        client,
        addr,
        token,
        Some(session_id),
        "tools/call",
        serde_json::json!({"name": "upload_blob", "arguments": args}),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["result"]["isError"].as_bool().unwrap_or(false),
        "expected isError, got: {body}"
    );
    body["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// 25 MiB default — generous enough for normal cases.
const TEST_DEFAULT_CAP: usize = 25 * 1024 * 1024;

#[tokio::test]
async fn test_upload_blob_basic() {
    let (addr, keys) = start_relay_with_cap(TEST_DEFAULT_CAP).await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let bytes = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
    let result = call_upload_blob(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": "docimages/diagram.png",
            "content_type": "image/png",
            "bytes": STANDARD.encode(&bytes)
        }),
    )
    .await;
    let doc_id = result["document_id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(doc_id).is_ok());
    assert_eq!(result["content_url"], "docimages/diagram.png");
}

#[tokio::test]
async fn test_upload_blob_replace_same_document_id() {
    let (addr, keys) = start_relay_with_cap(TEST_DEFAULT_CAP).await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let path = "docimages/replace.png";
    let r1 = call_upload_blob(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": path,
            "content_type": "image/png",
            "bytes": STANDARD.encode([1_u8, 2, 3])
        }),
    )
    .await;
    let first_id = r1["document_id"].as_str().unwrap().to_owned();

    let r2 = call_upload_blob(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": path,
            "content_type": "image/png",
            "bytes": STANDARD.encode([9_u8, 9, 9, 9, 9])
        }),
    )
    .await;
    let second_id = r2["document_id"].as_str().unwrap();
    assert_eq!(first_id, second_id, "document_id must be stable on replace");
}

#[tokio::test]
async fn test_upload_blob_replace_preserves_omitted_provenance() {
    let (addr, keys) = start_relay_with_cap(TEST_DEFAULT_CAP).await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let path = "imports/file.docx";

    // First upload with full provenance.
    call_upload_blob(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": path,
            "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "bytes": STANDARD.encode([1_u8, 2, 3]),
            "source_kind": 10,
            "source_id": "drive-abc",
            "source_url": "https://drive.google.com/file/abc",
            "source_author_display": "Original Author",
            "originally_created_at": 1_700_000_000_000_i64
        }),
    )
    .await;

    // Second upload omits everything except path + bytes — provenance
    // must survive (leave-as-is-on-omit per RFD 0083 §3).
    call_upload_blob(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": path,
            "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "bytes": STANDARD.encode([9_u8, 9, 9])
        }),
    )
    .await;

    // Read back via list_documents and assert provenance survived the
    // replace — every field set on the first upload must still be
    // present unchanged after the bytes-only second upload (RFD 0083
    // §3 leave-as-is-on-omit).
    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "list_documents",
            "arguments": {"space_id": "space1"}
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let docs: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
    assert_eq!(docs.len(), 1);
    let doc = &docs[0];
    assert_eq!(doc["path"], path);
    assert_eq!(doc["content_type"], "blob");
    assert_eq!(
        doc["source_kind"], 10,
        "source_kind set on first upload must survive bytes-only replace"
    );
    assert_eq!(doc["source_id"], "drive-abc");
    assert_eq!(doc["source_url"], "https://drive.google.com/file/abc");
    assert_eq!(doc["source_author_display"], "Original Author");
    assert_eq!(doc["originally_created_at"], 1_700_000_000_000_i64);
}

#[tokio::test]
async fn test_upload_blob_path_collision_with_text_doc() {
    let (addr, keys) = start_relay_with_cap(TEST_DEFAULT_CAP).await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    // Create a text document first.
    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "create_document",
            "arguments": {
                "space_id": "space1",
                "path": "shared/page.md",
                "content": "text content here"
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);

    // Attempt to upload a blob at the same path — should error.
    let err = call_upload_blob_expect_error(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": "shared/page.md",
            "content_type": "image/png",
            "bytes": STANDARD.encode([0_u8])
        }),
    )
    .await;
    assert!(
        err.contains("text document") || err.contains("text and blob"),
        "expected text-vs-blob conflict error, got: {err}"
    );
}

#[tokio::test]
async fn test_upload_blob_exceeds_cap() {
    // 1 KiB cap; payload exceeds by one byte.
    const TINY_CAP: usize = 1024;
    let (addr, keys) = start_relay_with_cap(TINY_CAP).await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let oversize = vec![0_u8; TINY_CAP + 1];
    let err = call_upload_blob_expect_error(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": "big.bin",
            "content_type": "application/octet-stream",
            "bytes": STANDARD.encode(&oversize)
        }),
    )
    .await;
    assert!(
        err.contains("blob exceeds configured cap") || err.contains("KUTL_MAX_BLOB_BYTES"),
        "expected blob-too-large error, got: {err}"
    );
}

#[tokio::test]
async fn test_upload_blob_rejects_invalid_base64() {
    // `bytes` must be valid base64 — the parser surfaces the decode
    // error with a hint so the agent learns to encode correctly rather
    // than guessing at the underlying type error.
    let (addr, keys) = start_relay_with_cap(TEST_DEFAULT_CAP).await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let err = call_upload_blob_expect_error(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": "bad.bin",
            "content_type": "application/octet-stream",
            "bytes": "not!base64!"
        }),
    )
    .await;
    assert!(
        err.contains("bytes is not valid base64"),
        "expected base64-decode error message, got: {err}"
    );
}
