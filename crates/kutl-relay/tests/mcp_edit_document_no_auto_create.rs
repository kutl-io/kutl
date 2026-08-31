//! Pins the `edit_document` semantic:
//! `edit_document` does not auto-create documents. Calling it against
//! an unknown id or path returns a `DocumentNotFound` error whose hint
//! string routes the agent to `create_document` — the message IS the
//! contract.

mod common;

use std::io::Write;

use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

use common::mcp::McpSession;

async fn start_mcp_relay() -> (String, tempfile::NamedTempFile) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = RelayConfig {
        port: 0,
        relay_name: "test-edit-no-create".into(),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };
    // Storeless boot: the space implicit-creates so edit_document
    // against a missing doc returns DocumentNotFound (not shadowed by a
    // SpaceNotFound reject) — the exact contract this file pins. Kept for the
    // kutlhub host relay.
    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, keys_file)
}

#[tokio::test]
async fn test_edit_document_unknown_path_returns_hint() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    // edit_document on an unknown path must error.
    let text = s
        .call_err(
            "edit_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "nonexistent.md",
                // Never examined: an unresolvable document is refused before
                // the base a caller names is ever looked at.
                "base_version": "kv1.not-a-token",
                "content": "would have been created in the old surprise",
                "intent": "trying to edit"
            }),
        )
        .await;
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
    let s = McpSession::open(&addr, &keys).await;

    // edit_document on an unknown UUID must error.
    let text = s
        .call_err(
            "edit_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "00000000-0000-4000-8000-000000000000",
                "base_version": "kv1.not-a-token",
                "content": "x",
                "intent": "trying to edit"
            }),
        )
        .await;
    assert!(
        text.contains("document not found") && text.contains("create_document"),
        "error must contain document-not-found + create_document hint, got: {text}"
    );
}

#[tokio::test]
async fn test_edit_document_succeeds_after_create_document() {
    // Sanity: create_document → edit_document on the same path works.
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    s.call_ok(
        "create_document",
        serde_json::json!({
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "path": "doc.md",
            "content": "v1"
        }),
    )
    .await;

    let read = s
        .call_ok(
            "read_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "doc.md"
            }),
        )
        .await;
    let base_version = read["version"].as_str().unwrap().to_owned();

    // edit_document on an existing path must succeed.
    s.call_ok(
        "edit_document",
        serde_json::json!({
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "document_id": "doc.md",
            "base_version": base_version,
            "content": "v2",
            "intent": "follow-up update"
        }),
    )
    .await;

    // Round-trip — !isError is necessary but not sufficient. The
    // edit must actually have landed: `read_document` must return
    // `v2`, not `v1`.
    let payload = s
        .call_ok(
            "read_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "doc.md"
            }),
        )
        .await;
    assert_eq!(
        payload["content"].as_str(),
        Some("v2"),
        "edit_document must have replaced content with v2, got: {payload}"
    );
}
