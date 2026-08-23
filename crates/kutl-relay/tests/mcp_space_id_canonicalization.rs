//! Integration test for the MCP `space_id` boundary normalization
//! (parking-lot fix). Verifies that an agent which mixes slug-form and
//! UUID-form `space_id` across calls reads/writes against the SAME
//! canonical space rather than creating two disjoint registry entries.
//!
//! Scenario:
//!   1. Register space `agent-mixed-forms` via `POST /spaces/register`,
//!      capturing the canonical UUID.
//!   2. Discover the space via MCP `list_spaces`.
//!   3. Call `create_document` with the SLUG form of `space_id`.
//!   4. Call `list_documents` with the UUID form of `space_id`.
//!   5. Confirm the document appears in the UUID-keyed list (rather
//!      than landing in a ghost slug-keyed parallel registry entry).
//!
//! Pre-fix, step 4 returned an empty list because the registry was
//! keyed off the raw caller-supplied string. Post-fix, both calls land
//! on the same canonical UUID and the doc round-trips.

mod common;

use std::io::Write;

use reqwest::header;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

const SLUG: &str = "agent-mixed-forms";

/// Start an auth-on relay (auth is unconditional) AND a `data_dir` so the
/// `SQLite` `SpaceBackend` is configured — slug↔UUID canonicalization is
/// a persistent-mode behaviour; pure-ephemeral mode has no canonical
/// authority to consult.
async fn start_relay() -> (String, tempfile::TempDir, tempfile::NamedTempFile) {
    let dir = tempfile::tempdir().unwrap();
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-mcp-space-id-canon".into(),
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
            "params": params,
        }));
    if let Some(sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }
    req.send().await.unwrap()
}

async fn mcp_session(addr: &str, keys_file: &tempfile::NamedTempFile) -> (String, String) {
    let (did, signing_key) = common::test_keypair();
    common::authorize_did(keys_file.path(), &did);
    let token = common::authenticate(&format!("http://{addr}"), &did, &signing_key).await;

    let client = reqwest::Client::new();
    let resp = mcp_request(
        &client,
        addr,
        &token,
        None,
        "initialize",
        serde_json::json!({
            "protocolVersion": "2025-03-26",
            "clientInfo": { "name": "test-agent", "version": "0.1" }
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

    (session_id, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mcp_space_id_canonicalization_slug_then_uuid() {
    let (addr, _dir, keys) = start_relay().await;
    let client = reqwest::Client::new();

    // Step 1: register the space via the human-equivalent endpoint and
    // capture its canonical UUID.
    let canonical_uuid = common::register_space(&client, &format!("http://{addr}"), SLUG).await;
    assert!(uuid::Uuid::parse_str(&canonical_uuid).is_ok());
    assert_ne!(canonical_uuid, SLUG);

    let (session_id, token) = mcp_session(&addr, &keys).await;

    // (No `list_spaces` discovery step here: on the OSS in-memory path
    // `list_spaces` enumerates `self.registries`, which is populated
    // lazily on first touch — a freshly-registered space isn't visible
    // until a doc lands in it. The slug-vs-UUID scenario this test
    // covers assumes the agent already knows the space exists; the
    // discovery path is exercised separately in `mcp_list_spaces.rs`
    // under the membership-backed path.)

    // Step 3: write a doc using the SLUG form of `space_id`.
    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "create_document",
            "arguments": {
                "space_id": SLUG,
                "path": "shared/runbook.md",
                "content": "# Runbook\n"
            }
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "create_document via slug failed: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let result: serde_json::Value = serde_json::from_str(text).unwrap();
    let doc_id = result["document_id"].as_str().unwrap().to_owned();
    assert!(uuid::Uuid::parse_str(&doc_id).is_ok());

    // Step 4: read the same space via UUID form. Pre-fix this returned
    // an empty list because the registry was keyed on the raw slug from
    // step 3. Post-fix both calls canonicalize to the same UUID.
    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "list_documents",
            "arguments": { "space_id": canonical_uuid }
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["result"]["isError"].as_bool().unwrap_or(false),
        "list_documents via UUID failed: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let docs: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
    assert_eq!(
        docs.len(),
        1,
        "expected the doc written via slug to appear in the UUID-keyed list"
    );
    assert_eq!(docs[0]["path"].as_str().unwrap(), "shared/runbook.md");
    assert_eq!(docs[0]["document_id"].as_str().unwrap(), doc_id);

    // Reverse direction: read by slug to confirm both directions
    // collapse onto the same registry entry.
    let resp = mcp_request(
        &client,
        &addr,
        &token,
        Some(&session_id),
        "tools/call",
        serde_json::json!({
            "name": "list_documents",
            "arguments": { "space_id": SLUG }
        }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let docs_via_slug: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
    assert_eq!(docs_via_slug, docs, "slug- and UUID-keyed lists must match");
}
