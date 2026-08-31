//! Regression tests for the unknown-space behaviour of MCP
//! `create_document` / `upload_blob`.
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

use tempfile::TempDir;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

use common::mcp::McpSession;

async fn start_mcp_relay_ephemeral() -> (String, tempfile::NamedTempFile) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# MCP test keys").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = RelayConfig {
        port: 0,
        relay_name: "test-unknown-space-storeless".into(),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };
    // Storeless boot: with no space backend, mcp_check_space_registered
    // implicit-creates unknown spaces — the kutlhub-shaped path this arm pins.
    // The OSS binary does not boot this way; the persistent variant below
    // (data_dir set) pins the durable-mode reject.
    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
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

#[tokio::test]
async fn test_create_document_ephemeral_mode_implicit_create() {
    // Pure ephemeral mode (no `data_dir`, no `space_backend`): no
    // out-of-band registration path exists, so the handler falls back
    // to implicit-create-on-first-touch. This is the only remaining
    // caller of that behaviour and is scoped to test/dev usage.
    let (addr, keys) = start_mcp_relay_ephemeral().await;
    let s = McpSession::open(&addr, &keys).await;

    // Ephemeral mode must accept the implicit-create silently.
    s.call_ok(
        "create_document",
        serde_json::json!({
            "space_id": "f87c143e-0e61-468c-803a-06b453fa0588",
            "path": "first.md",
            "content": "hello fresh space"
        }),
    )
    .await;

    // The doc should now be enumerable.
    let docs = s
        .call_ok(
            "list_documents",
            serde_json::json!({"space_id": "f87c143e-0e61-468c-803a-06b453fa0588"}),
        )
        .await;
    let docs = docs.as_array().expect("list_documents returns an array");
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["path"], "first.md");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_document_persistent_mode_unknown_space_errors() {
    // Persistent mode: `space_backend` is the source of truth. Writing
    // to an unregistered slug must error with `SpaceNotFound` and route
    // the agent at the human creation flow. Space creation is
    // intentionally NOT an MCP capability — `list_spaces` is the
    // discovery primitive; spaces are minted via `kutl init`, desktop,
    // web UI, or `POST /spaces/register`.
    let (addr, keys, _data_dir) = start_mcp_relay_persistent().await;
    let s = McpSession::open(&addr, &keys).await;

    // Persistent-mode create_document against an unknown space must error.
    let text = s
        .call_err(
            "create_document",
            serde_json::json!({
                "space_id": "ef765dc4-0dd6-41dc-84f6-567937da5449",
                "path": "first.md",
                "content": "should not land"
            }),
        )
        .await;
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
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    let (addr, keys, _data_dir) = start_mcp_relay_persistent().await;
    let s = McpSession::open(&addr, &keys).await;

    // Persistent-mode upload_blob against an unknown space must error.
    let text = s
        .call_err(
            "upload_blob",
            serde_json::json!({
                "space_id": "ef765dc4-0dd6-41dc-84f6-567937da5449",
                "path": "docimages/x.png",
                "content_type": "image/png",
                "bytes": STANDARD.encode([0u8, 1, 2, 3]),
            }),
        )
        .await;
    assert!(
        text.contains("space not found"),
        "expected space-not-found error message, got: {text}"
    );
}
