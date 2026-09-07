//! Integration tests for the MCP `upload_blob` tool.

mod common;

use std::io::Write;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

use common::mcp::McpSession;

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
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        max_blob_bytes,
        ..Default::default()
    };

    // In-memory boot: these tests pin the in-memory implicit-create
    // behaviour (kept for the kutlhub host relay); the durable-mode reject is
    // covered by mcp_unknown_space_error.rs.
    let (app, _relay_handle, _flush_handle) = common::build_in_memory_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, keys_file)
}

/// 25 MiB default — generous enough for normal cases.
const TEST_DEFAULT_CAP: usize = 25 * 1024 * 1024;

#[tokio::test]
async fn test_upload_blob_basic() {
    let (addr, keys) = start_relay_with_cap(TEST_DEFAULT_CAP).await;
    let s = McpSession::open(&addr, &keys).await;

    let bytes = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
    let result = s
        .call_ok(
            "upload_blob",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
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
    let s = McpSession::open(&addr, &keys).await;

    let path = "docimages/replace.png";
    let r1 = s
        .call_ok(
            "upload_blob",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
                "path": path,
                "content_type": "image/png",
                "bytes": STANDARD.encode([1_u8, 2, 3])
            }),
        )
        .await;
    let first_id = r1["document_id"].as_str().unwrap().to_owned();

    let r2 = s
        .call_ok(
            "upload_blob",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
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
    let s = McpSession::open(&addr, &keys).await;

    let path = "imports/file.docx";

    // First upload with full provenance.
    s.call_ok(
        "upload_blob",
        serde_json::json!({
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
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
    // must survive (leave-as-is-on-omit).
    s.call_ok(
        "upload_blob",
        serde_json::json!({
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "path": path,
            "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "bytes": STANDARD.encode([9_u8, 9, 9])
        }),
    )
    .await;

    // Read back via list_documents and assert provenance survived the
    // replace — every field set on the first upload must still be
    // present unchanged after the bytes-only second upload
    // (leave-as-is-on-omit).
    let docs = s
        .call_ok(
            "list_documents",
            serde_json::json!({"space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635"}),
        )
        .await;
    let docs = docs.as_array().expect("list_documents returns an array");
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
    let s = McpSession::open(&addr, &keys).await;

    // Create a text document first.
    s.call_ok(
        "create_document",
        serde_json::json!({
            "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
            "path": "shared/page.md",
            "content": "text content here"
        }),
    )
    .await;

    // Attempt to upload a blob at the same path — should error.
    let err = s
        .call_err(
            "upload_blob",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
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
    let s = McpSession::open(&addr, &keys).await;

    let oversize = vec![0_u8; TINY_CAP + 1];
    let err = s
        .call_err(
            "upload_blob",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
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
    let s = McpSession::open(&addr, &keys).await;

    let err = s
        .call_err(
            "upload_blob",
            serde_json::json!({
                "space_id": "f6b7a1c4-9cc1-49a7-8f5f-c1ae8b97e635",
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
