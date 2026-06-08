//! Regression test for the tightened `edit_document` semantic (RFD 0083 §1).
//!
//! `edit_document` no longer auto-creates documents. Calling it against
//! an unknown id or path returns a `DocumentNotFound` error whose hint
//! string routes the agent to `create_document` — the message IS the
//! contract.

mod common;

use std::io::Write;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use reqwest::header;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

async fn start_mcp_relay() -> (String, tempfile::NamedTempFile) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = RelayConfig {
        port: 0,
        relay_name: "test-edit-no-create".into(),
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

#[tokio::test]
async fn test_edit_document_unknown_path_returns_hint() {
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "edit_document",
            "arguments": {
                "space_id": "space1",
                "document_id": "nonexistent.md",
                "content": "would have been created in the old surprise",
                "intent": "trying to edit"
            }
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["result"]["isError"].as_bool().unwrap_or(false),
        "edit_document on unknown path must error, got: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("document not found"),
        "expected document-not-found error message, got: {text}"
    );
    // The hint string is part of the contract — agents recover by
    // following it to `create_document`.
    assert!(
        text.contains("create_document"),
        "error must point at create_document for recovery, got: {text}"
    );
}

#[tokio::test]
async fn test_edit_document_unknown_uuid_returns_hint() {
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "edit_document",
            "arguments": {
                "space_id": "space1",
                "document_id": "00000000-0000-4000-8000-000000000000",
                "content": "x",
                "intent": "trying to edit"
            }
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["result"]["isError"].as_bool().unwrap_or(false),
        "edit_document on unknown UUID must error, got: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("document not found") && text.contains("create_document"),
        "error must contain document-not-found + create_document hint, got: {text}"
    );
}

#[tokio::test]
async fn test_edit_document_succeeds_after_create_document() {
    // Sanity: create_document → edit_document on the same path works.
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

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
                "path": "doc.md",
                "content": "v1"
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);

    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "edit_document",
            "arguments": {
                "space_id": "space1",
                "document_id": "doc.md",
                "content": "v2",
                "intent": "follow-up update"
            }
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "edit_document on an existing path must succeed, got: {body}"
    );

    // Round-trip — !isError is necessary but not sufficient. The
    // edit must actually have landed: `read_document` must return
    // `v2`, not `v1`.
    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "read_document",
            "arguments": {
                "space_id": "space1",
                "document_id": "doc.md"
            }
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "read_document after edit_document must succeed, got: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let payload: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(
        payload["content"].as_str(),
        Some("v2"),
        "edit_document must have replaced content with v2, got: {payload}"
    );
}
