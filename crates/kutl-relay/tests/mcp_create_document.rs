//! Integration tests for the MCP `create_document` tool (RFD 0083 §1).

mod common;

use std::io::Write;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use reqwest::header;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

/// Start a relay with `require_auth: true`. Returns `(addr, keys_file)`.
async fn start_mcp_relay() -> (String, tempfile::NamedTempFile) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-create-document".into(),
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
        .expect("initialize should return mcp-session-id header")
        .to_str()
        .unwrap()
        .to_owned();

    (session_id, token, did)
}

/// Issue a `tools/call` for `create_document` and return the parsed
/// successful payload `{ document_id }`. Asserts no error on the call.
async fn call_create_document(
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
            "name": "create_document",
            "arguments": args
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "expected success, got: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

/// Issue a `list_documents` call and return the parsed array of
/// `McpDocumentSummary` records (with provenance fields surfaced per
/// RFD 0083 § "List enumeration"). Asserts no error.
async fn call_list_documents(
    client: &reqwest::Client,
    addr: &str,
    token: &str,
    session_id: &str,
    space_id: &str,
) -> Vec<serde_json::Value> {
    let resp = mcp_request(
        client,
        addr,
        token,
        Some(session_id),
        "tools/call",
        serde_json::json!({
            "name": "list_documents",
            "arguments": { "space_id": space_id }
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "list_documents failed: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

/// Find a document by path in a `list_documents` response.
fn find_doc<'a>(docs: &'a [serde_json::Value], path: &str) -> &'a serde_json::Value {
    docs.iter()
        .find(|d| d["path"].as_str() == Some(path))
        .unwrap_or_else(|| panic!("expected to find doc at path {path:?} in {docs:?}"))
}

/// Issue a `create_document` call expected to fail; return the error
/// text payload.
async fn call_create_document_expect_error(
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
        serde_json::json!({
            "name": "create_document",
            "arguments": args
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["result"]["isError"].as_bool().unwrap_or(false),
        "expected isError=true, got: {body}"
    );
    body["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn test_create_document_basic() {
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let result = call_create_document(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": "handbook/onboarding.md",
            "content": "# Welcome\n\nGetting started."
        }),
    )
    .await;
    let doc_id = result["document_id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(doc_id).is_ok(), "expected UUID");

    // Read back by path.
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
                "document_id": "handbook/onboarding.md"
            }
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(parsed["content"], "# Welcome\n\nGetting started.");
}

#[tokio::test]
async fn test_create_document_empty_content_allowed() {
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let result = call_create_document(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": "blank.md",
            "content": ""
        }),
    )
    .await;
    assert!(result["document_id"].is_string());

    // Empty doc should appear in list_documents.
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
    assert_eq!(docs[0]["path"], "blank.md");
}

#[tokio::test]
async fn test_create_document_path_collision_errors() {
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    call_create_document(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": "shared/page.md",
            "content": "first"
        }),
    )
    .await;

    let err = call_create_document_expect_error(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": "shared/page.md",
            "content": "second"
        }),
    )
    .await;
    assert!(
        err.contains("path already in use"),
        "expected helpful path collision error, got: {err}"
    );
}

#[tokio::test]
async fn test_create_document_provenance_each_field() {
    // Set each provenance field one at a time and assert via
    // `list_documents` that the persisted record carries exactly that
    // field and no other. Catches dropped/conflated fields the prior
    // version (which only asserted no-error) would have missed.
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let cases: Vec<(&str, &str, serde_json::Value, &str, serde_json::Value)> = vec![
        (
            "p1.md",
            "source_kind",
            serde_json::json!(1),
            "source_kind",
            serde_json::json!(1),
        ),
        (
            "p2.md",
            "source_id",
            serde_json::json!("ext-id"),
            "source_id",
            serde_json::json!("ext-id"),
        ),
        (
            "p3.md",
            "source_url",
            serde_json::json!("https://example.test/p3"),
            "source_url",
            serde_json::json!("https://example.test/p3"),
        ),
        (
            "p4.md",
            "source_author_display",
            serde_json::json!("Jane"),
            "source_author_display",
            serde_json::json!("Jane"),
        ),
        (
            "p5.md",
            "originally_created_at",
            serde_json::json!(1_700_000_000_000_i64),
            "originally_created_at",
            serde_json::json!(1_700_000_000_000_i64),
        ),
        (
            "p6.md",
            "ingestion_job_id",
            serde_json::json!("44444444-4444-4444-8444-444444444444"),
            "ingestion_job_id",
            serde_json::json!("44444444-4444-4444-8444-444444444444"),
        ),
    ];

    let all_provenance_fields = [
        "source_kind",
        "source_id",
        "source_url",
        "source_author_display",
        "originally_created_at",
        "ingestion_job_id",
    ];

    for (path, in_field, in_value, _out_field, _out_value) in &cases {
        let mut args = serde_json::json!({
            "space_id": "space1",
            "path": path,
            "content": "x",
        });
        args[in_field] = in_value.clone();
        call_create_document(&client, &addr, &token, &session_id, args).await;
    }

    let docs = call_list_documents(&client, &addr, &token, &session_id, "space1").await;
    for (path, _in_field, _in_value, out_field, out_value) in &cases {
        let doc = find_doc(&docs, path);
        assert_eq!(
            &doc[out_field], out_value,
            "doc {path}: expected {out_field}={out_value}, got: {doc}",
        );
        // Every OTHER provenance field must be absent (serde
        // `skip_serializing_if = Option::is_none`).
        for other in &all_provenance_fields {
            if *other == *out_field {
                continue;
            }
            assert!(
                doc.get(other).is_none(),
                "doc {path}: provenance field {other} must be absent when only {out_field} was set; got {doc}",
            );
        }
    }
}

#[tokio::test]
async fn test_create_document_provenance_all_together() {
    let (addr, keys) = start_mcp_relay().await;
    let (session_id, token, _did) = mcp_session(&addr, &keys).await;
    let client = reqwest::Client::new();

    let result = call_create_document(
        &client,
        &addr,
        &token,
        &session_id,
        serde_json::json!({
            "space_id": "space1",
            "path": "imports/full.md",
            "content": "imported content",
            "source_kind": 1,
            "source_id": "page-abc",
            "source_url": "https://notion.so/page-abc",
            "source_author_display": "Jane Doe",
            "originally_created_at": 1_700_000_000_000_i64,
            "ingestion_job_id": "55555555-5555-4555-8555-555555555555"
        }),
    )
    .await;
    let doc_id = result["document_id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(doc_id).is_ok());

    // The persisted record carries every provenance field as sent.
    let docs = call_list_documents(&client, &addr, &token, &session_id, "space1").await;
    let doc = find_doc(&docs, "imports/full.md");
    assert_eq!(doc["source_kind"], 1);
    assert_eq!(doc["source_id"], "page-abc");
    assert_eq!(doc["source_url"], "https://notion.so/page-abc");
    assert_eq!(doc["source_author_display"], "Jane Doe");
    assert_eq!(doc["originally_created_at"], 1_700_000_000_000_i64);
    assert_eq!(
        doc["ingestion_job_id"],
        "55555555-5555-4555-8555-555555555555"
    );
}
