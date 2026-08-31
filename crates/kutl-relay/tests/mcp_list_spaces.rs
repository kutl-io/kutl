//! Integration tests for the MCP `list_spaces` tool.

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
        relay_name: "test-list-spaces".into(),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        ..Default::default()
    };

    // Storeless boot: list_spaces enumerates the in-memory
    // registries populated by implicit-create, keyed by literal slug — the
    // kutlhub-shaped storeless path. A durable relay would key by canonical
    // UUID (see mcp_space_id_canonicalization.rs), changing these slug
    // assertions.
    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, keys_file)
}

async fn call_list_spaces(s: &McpSession) -> Vec<serde_json::Value> {
    let spaces = s.call_ok("list_spaces", serde_json::json!({})).await;
    spaces
        .as_array()
        .expect("list_spaces returns an array")
        .clone()
}

#[tokio::test]
async fn test_list_spaces_authorized_did_sees_spaces() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;

    // Create two documents in two different spaces, populating the
    // in-memory registries.
    for (space, path) in &[
        ("e8276918-32d7-49a6-8f8e-e9058ba94e89", "doc.md"),
        ("fba02294-a5ee-4bab-8866-e2aef91e7560", "doc.md"),
    ] {
        s.call_ok(
            "create_document",
            serde_json::json!({
                "space_id": space,
                "path": path,
                "content": "x"
            }),
        )
        .await;
    }

    let spaces = call_list_spaces(&s).await;
    let slugs: Vec<&str> = spaces.iter().map(|s| s["slug"].as_str().unwrap()).collect();
    assert!(
        slugs.contains(&"e8276918-32d7-49a6-8f8e-e9058ba94e89"),
        "expected the alpha space, got {slugs:?}"
    );
    assert!(
        slugs.contains(&"fba02294-a5ee-4bab-8866-e2aef91e7560"),
        "expected the bravo space, got {slugs:?}"
    );
}

#[tokio::test]
async fn test_list_spaces_unauthorized_did_sees_empty() {
    let (addr, keys) = start_mcp_relay().await;

    // Session A: authorized — seeds a space.
    let a = McpSession::open(&addr, &keys).await;
    a.call_ok(
        "create_document",
        serde_json::json!({
            "space_id": "70e5661f-b33e-495f-87d4-1cf7fe18c47d",
            "path": "doc.md",
            "content": "seed"
        }),
    )
    .await;

    // Session B: NOT in authorized_keys. The relay still issues a
    // session (the keys file gates space-scoped actions), but
    // list_spaces should filter that DID out and return [].
    let b = McpSession::open_unauthorized(&addr, &keys).await;
    let spaces = call_list_spaces(&b).await;
    assert!(
        spaces.is_empty(),
        "unauthorized DID should see no spaces, got {spaces:?}"
    );
}

#[tokio::test]
async fn test_list_spaces_returns_empty_when_no_spaces() {
    let (addr, keys) = start_mcp_relay().await;
    let s = McpSession::open(&addr, &keys).await;
    let spaces = call_list_spaces(&s).await;
    assert!(spaces.is_empty(), "no spaces yet, got {spaces:?}");
}
