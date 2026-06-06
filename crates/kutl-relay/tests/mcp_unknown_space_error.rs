//! Regression tests for the unknown-space behaviour of MCP
//! `create_document` / `upload_blob` (RFD 0083 §4, audit-amended
//! 2026-05-20).
//!
//! Two modes — gated by whether a `space_backend` is configured:
//!
//! - **Persistent mode** (`data_dir` set → `SQLite` `SpaceBackend`):
//!   the space backend is the source of truth. Writing to an
//!   unregistered slug errors with `SpaceNotFound` and routes the
//!   agent at the human creation flow.
//! - **Pure ephemeral mode** (no `data_dir`, no `space_backend`):
//!   the relay has no out-of-band registration path. Falls back to
//!   implicit-create-on-first-touch so test/dev usage keeps working.
//!   This is the only remaining caller of implicit-create.
//!
//! kutlhub-relay always runs in persistent mode (Postgres
//! `SpaceBackend`) so unknown spaces error there too; the OSS
//! ephemeral path is a deliberate test/dev affordance.

mod common;

use std::io::Write;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use reqwest::header;
use tempfile::TempDir;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

async fn start_mcp_relay_ephemeral() -> (String, tempfile::NamedTempFile) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = RelayConfig {
        port: 0,
        relay_name: "test-unknown-space-ephemeral".into(),
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

/// Persistent mode: `data_dir` configured so `build_app` wires up a
/// `SQLite` `SpaceBackend`. Caller keeps the `TempDir` guard alive.
async fn start_mcp_relay_persistent() -> (String, tempfile::NamedTempFile, TempDir) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = RelayConfig {
        port: 0,
        relay_name: "test-unknown-space-persistent".into(),
        require_auth: true,
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        data_dir: Some(data_dir.path().to_path_buf()),
        ..Default::default()
    };
    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, keys_file, data_dir)
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
async fn test_create_document_ephemeral_mode_implicit_create() {
    // Pure ephemeral mode (no `data_dir`, no `space_backend`): no
    // out-of-band registration path exists, so the handler falls back
    // to implicit-create-on-first-touch. This is the only remaining
    // caller of that behaviour and is scoped to test/dev usage.
    let (addr, keys) = start_mcp_relay_ephemeral().await;
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
                "space_id": "never-seen-before",
                "path": "first.md",
                "content": "hello fresh space"
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "ephemeral mode should accept implicit-create silently, got: {body}"
    );

    // The doc should now be enumerable.
    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "list_documents",
            "arguments": {"space_id": "never-seen-before"}
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let docs: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["path"], "first.md");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_document_persistent_mode_unknown_space_errors() {
    // Persistent mode: `space_backend` is the source of truth. Writing
    // to an unregistered slug must error with `SpaceNotFound` and route
    // the agent at the human creation flow. RFD 0083 §4 says space
    // creation is intentionally NOT an MCP capability — `list_spaces`
    // is the discovery primitive; spaces are minted via `kutl init`,
    // desktop, web UI, or `POST /spaces/register`.
    let (addr, keys, _data_dir) = start_mcp_relay_persistent().await;
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
                "space_id": "never-registered",
                "path": "first.md",
                "content": "should not land"
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["result"]["isError"].as_bool().unwrap_or(false),
        "persistent-mode create_document against unknown space must error, got: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("space not found"),
        "expected space-not-found error message, got: {text}"
    );
    // The hint is part of the contract — agents recover by following
    // it to the human creation flow.
    assert!(
        text.contains("list_spaces"),
        "error must point at list_spaces for discovery, got: {text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_blob_persistent_mode_unknown_space_errors() {
    use base64::engine::general_purpose::STANDARD;
    let (addr, keys, _data_dir) = start_mcp_relay_persistent().await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "upload_blob",
            "arguments": {
                "space_id": "never-registered",
                "path": "docimages/x.png",
                "content_type": "image/png",
                "bytes": STANDARD.encode([0u8, 1, 2, 3]),
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["result"]["isError"].as_bool().unwrap_or(false),
        "persistent-mode upload_blob against unknown space must error, got: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("space not found"),
        "expected space-not-found error message, got: {text}"
    );
}
