//! Integration tests for the MCP `list_spaces` tool (RFD 0083 §4).

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
        relay_name: "test-list-spaces".into(),
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

/// Initialize an MCP session for a freshly minted DID. The DID is
/// added to the authorized keys file when `authorize` is true; this
/// lets a single relay host both authorized and unauthorized callers
/// across tests.
async fn mcp_session_with(
    addr: &str,
    keys_file: &tempfile::NamedTempFile,
    authorize: bool,
) -> (String, String, String) {
    let (did, signing_key) = common::test_keypair();
    if authorize {
        writeln!(&*keys_file, "{did}").unwrap();
    }
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

async fn call_list_spaces(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    session_id: &str,
) -> Vec<serde_json::Value> {
    let resp = mcp_request(
        client,
        addr,
        token,
        Some(session_id),
        "tools/call",
        serde_json::json!({"name": "list_spaces", "arguments": {}}),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "list_spaces failed: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

#[tokio::test]
async fn test_list_spaces_authorized_did_sees_spaces() {
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session_with(&addr, &keys, true).await;
    let client = reqwest::Client::new();

    // Create two documents in two different spaces, populating the
    // in-memory registries.
    for (space, path) in &[("space-alpha", "doc.md"), ("space-bravo", "doc.md")] {
        let resp = mcp_request(
            &client,
            &addr,
            &token,
            Some(&session_id),
            "tools/call",
            serde_json::json!({
                "name": "create_document",
                "arguments": {
                    "space_id": space,
                    "path": path,
                    "content": "x"
                }
            }),
        )
        .await;
        assert_eq!(resp.status(), 200);
    }

    let spaces = call_list_spaces(&client, &addr, &token, &session_id).await;
    let slugs: Vec<&str> = spaces.iter().map(|s| s["slug"].as_str().unwrap()).collect();
    assert!(
        slugs.contains(&"space-alpha"),
        "expected space-alpha, got {slugs:?}"
    );
    assert!(
        slugs.contains(&"space-bravo"),
        "expected space-bravo, got {slugs:?}"
    );
}

#[tokio::test]
async fn test_list_spaces_unauthorized_did_sees_empty() {
    let (addr, keys) = start_mcp_relay().await;

    // Session A: authorized — seeds a space.
    let (session_a, token_a, _did_a) = mcp_session_with(&addr, &keys, true).await;
    let client = reqwest::Client::new();
    let resp = mcp_request(
        &client,
        &addr,
        &token_a,
        Some(&session_a),
        "tools/call",
        serde_json::json!({
            "name": "create_document",
            "arguments": {
                "space_id": "seeded-space",
                "path": "doc.md",
                "content": "seed"
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);

    // Session B: NOT in authorized_keys. The relay still issues a
    // session (the keys file gates space-scoped actions), but
    // list_spaces should filter that DID out and return [].
    let (session_b, token_b, _did_b) = mcp_session_with(&addr, &keys, false).await;
    let spaces = call_list_spaces(&client, &addr, &token_b, &session_b).await;
    assert!(
        spaces.is_empty(),
        "unauthorized DID should see no spaces, got {spaces:?}"
    );
}

#[tokio::test]
async fn test_list_spaces_returns_empty_when_no_spaces() {
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session_with(&addr, &keys, true).await;
    let client = reqwest::Client::new();
    let spaces = call_list_spaces(&client, &addr, &token, &session_id).await;
    assert!(spaces.is_empty(), "no spaces yet, got {spaces:?}");
}
