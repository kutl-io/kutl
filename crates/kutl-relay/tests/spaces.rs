//! Integration tests for OSS space registration and resolution.
//!
//! These tests exercise the `/spaces/register` and `/spaces/resolve` HTTP
//! endpoints through a real axum relay backed by a temporary `SQLite` database.

use tokio::net::TcpListener;

use kutl_relay::config::{
    DEFAULT_OUTBOUND_CAPACITY, DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS,
    RelayConfig,
};

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Start a relay with `data_dir` set to a temporary directory.
///
/// Returns `(base_url, _temp_dir)`. The temp dir must be kept alive for
/// the duration of the test so the `SQLite` database is not deleted.
async fn start_relay_with_spaces() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        host: "127.0.0.1".into(),
        port: 0,
        relay_name: "test-spaces-relay".into(),
        require_auth: false,
        database_url: None,
        outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
        data_dir: Some(dir.path().to_path_buf()),
        external_url: None,
        ux_url: None,
        authorized_keys_file: None,
        snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
        snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), dir)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_register_space() {
    let (base_url, _dir) = start_relay_with_spaces().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/spaces/register"))
        .json(&serde_json::json!({"name": "my-test-space"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "my-test-space");
    assert!(
        body["space_id"].as_str().is_some_and(|s| !s.is_empty()),
        "space_id should be a non-empty string"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_register_duplicate_rejected() {
    let (base_url, _dir) = start_relay_with_spaces().await;
    let client = reqwest::Client::new();

    let first = client
        .post(format!("{base_url}/spaces/register"))
        .json(&serde_json::json!({"name": "duplicate-space"}))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 201);

    let second = client
        .post(format!("{base_url}/spaces/register"))
        .json(&serde_json::json!({"name": "duplicate-space"}))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 409);

    let body: serde_json::Value = second.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|s| s.contains("already taken")),
        "expected conflict error, got: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_resolve_space() {
    let (base_url, _dir) = start_relay_with_spaces().await;
    let client = reqwest::Client::new();

    // Register first.
    let reg_resp = client
        .post(format!("{base_url}/spaces/register"))
        .json(&serde_json::json!({"name": "resolve-me"}))
        .send()
        .await
        .unwrap();
    assert_eq!(reg_resp.status(), 201);
    let reg_body: serde_json::Value = reg_resp.json().await.unwrap();
    let expected_id = reg_body["space_id"].as_str().unwrap();

    // Resolve.
    let resolve_resp = client
        .get(format!("{base_url}/spaces/resolve?name=resolve-me"))
        .send()
        .await
        .unwrap();
    assert_eq!(resolve_resp.status(), 200);

    let resolve_body: serde_json::Value = resolve_resp.json().await.unwrap();
    assert_eq!(resolve_body["space_id"], expected_id);
    assert_eq!(resolve_body["name"], "resolve-me");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_resolve_unknown_returns_404() {
    let (base_url, _dir) = start_relay_with_spaces().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/spaces/resolve?name=no-such-space"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|s| s.contains("not found")),
        "expected not-found error, got: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_register_invalid_name_rejected() {
    let (base_url, _dir) = start_relay_with_spaces().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/spaces/register"))
        .json(&serde_json::json!({"name": "ab"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|s| s.contains("at least 3")),
        "expected validation error, got: {body}"
    );
}
