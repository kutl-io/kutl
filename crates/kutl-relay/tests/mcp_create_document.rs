//! Integration tests for the MCP `create_document` tool.

mod common;

use std::io::Write;

use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

use common::mcp::McpSession;

/// Start an auth-on relay (auth is unconditional). Returns `(addr, keys_file)`.
async fn start_mcp_relay() -> (String, tempfile::NamedTempFile) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-create-document".into(),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };

    // Storeless boot: these tests pin the in-memory implicit-create
    // behaviour, kept for the kutlhub host relay; the durable-mode reject is
    // covered by mcp_unknown_space_error.rs.
    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys_file)
}

/// Issue a `list_documents` call and return the parsed array of
/// `McpDocumentSummary` records (with provenance fields
/// surfaced). Asserts no error.
async fn call_list_documents(s: &McpSession, space_id: &str) -> Vec<serde_json::Value> {
    let docs = s
        .call_ok(
            "list_documents",
            serde_json::json!({ "space_id": space_id }),
        )
        .await;
    docs.as_array()
        .expect("list_documents returns an array")
        .clone()
}

/// Find a document by path in a `list_documents` response.
fn find_doc<'a>(docs: &'a [serde_json::Value], path: &str) -> &'a serde_json::Value {
    docs.iter()
        .find(|d| d["path"].as_str() == Some(path))
        .unwrap_or_else(|| panic!("expected to find doc at path {path:?} in {docs:?}"))
}

#[tokio::test]
async fn test_create_document_basic() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    let result = s
        .call_ok(
            "create_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "path": "handbook/onboarding.md",
                "content": "# Welcome\n\nGetting started."
            }),
        )
        .await;
    let doc_id = result["document_id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(doc_id).is_ok(), "expected UUID");

    // Read back by path.
    let parsed = s
        .call_ok(
            "read_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "document_id": "handbook/onboarding.md"
            }),
        )
        .await;
    assert_eq!(parsed["content"], "# Welcome\n\nGetting started.");
}

#[tokio::test]
async fn test_create_document_empty_content_allowed() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    let result = s
        .call_ok(
            "create_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "path": "blank.md",
                "content": ""
            }),
        )
        .await;
    assert!(result["document_id"].is_string());

    // Empty doc should appear in list_documents.
    let docs = call_list_documents(&s, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635").await;
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["path"], "blank.md");
}

#[tokio::test]
async fn test_create_document_path_collision_errors() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    s.call_ok(
        "create_document",
        serde_json::json!({
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "path": "shared/page.md",
            "content": "first"
        }),
    )
    .await;

    let err = s
        .call_err(
            "create_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
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
    // field and no other. Catches dropped/conflated fields that a
    // no-error-only assertion would miss.
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

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
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "path": path,
            "content": "x",
        });
        args[in_field] = in_value.clone();
        s.call_ok("create_document", args).await;
    }

    let docs = call_list_documents(&s, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635").await;
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
    let s = McpSession::open(&addr, &keys).await;

    let result = s
        .call_ok(
            "create_document",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
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
    let docs = call_list_documents(&s, "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635").await;
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
