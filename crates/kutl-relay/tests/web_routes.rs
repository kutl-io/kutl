//! Integration tests for the HTML web admin routes.
//!
//! Tests verify:
//! - `GET /` returns an HTML space list
//! - `GET /spaces/{id}` returns an HTML space detail page
//! - `GET /join/{code}` returns an HTML invite landing page
//! - `POST /invites/form` creates an invite and redirects to the space detail
//! - `POST /invites/revoke` revokes an invite and redirects to the space detail
//! - Web routes are absent when `data_dir` is not set

mod common;

use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_index_returns_html() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    // Register a space so there is something to list.
    common::register_space(&client, &base_url, "index-test-space").await;

    let resp = client.get(format!("{base_url}/")).send().await.unwrap();
    assert_eq!(resp.status(), 200, "GET / should return 200");
    let ct = resp.headers()["content-type"].to_str().unwrap();
    assert!(ct.contains("text/html"), "content-type should be text/html");

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("index-test-space"),
        "space list should contain the registered space name"
    );
    assert!(
        body.contains("Generate invite"),
        "space list should show Generate invite button"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_index_empty_spaces() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{base_url}/")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("No spaces registered yet"),
        "empty space list should show placeholder text"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_space_detail_shows_invites() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let space_id = common::register_space(&client, &base_url, "detail-test-space").await;
    let code = common::create_invite(&client, &base_url, &space_id).await;

    let resp = client
        .get(format!("{base_url}/spaces/{space_id}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "GET /spaces/:id should return 200");
    let ct = resp.headers()["content-type"].to_str().unwrap();
    assert!(ct.contains("text/html"), "content-type should be text/html");

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("detail-test-space"),
        "space detail should include the space name"
    );
    assert!(
        body.contains(&code),
        "space detail should include the invite code"
    );
    assert!(
        body.contains("Revoke"),
        "space detail should show Revoke button"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_space_detail_not_found() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/spaces/nonexistent-space-id"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404, "unknown space_id should return 404");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_join_page_shows_space_info() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let space_id = common::register_space(&client, &base_url, "join-page-space").await;
    let code = common::create_invite(&client, &base_url, &space_id).await;

    let resp = client
        .get(format!("{base_url}/join/{code}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "GET /join/:code should return 200");
    let ct = resp.headers()["content-type"].to_str().unwrap();
    assert!(ct.contains("text/html"), "content-type should be text/html");

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("join-page-space"),
        "join page should show the space name"
    );
    assert!(
        body.contains("kutl://join"),
        "join page should include kutl:// deep-link"
    );
    assert!(
        body.contains("kutl join"),
        "join page should show CLI fallback command"
    );
    assert!(
        body.contains("Open in kutl"),
        "join page should have Open in kutl button"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_join_page_unknown_code() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/join/0000000000000000"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        404,
        "unknown code should return 404 on /join/:code"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("Invite not found"),
        "404 page should say invite not found"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_post_invites_form_creates_invite_and_redirects() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    // Disable redirect following so we can inspect the 303.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let space_id = common::register_space(&client, &base_url, "form-create-space").await;

    let resp = client
        .post(format!("{base_url}/invites/form"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("space_id={space_id}"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        303,
        "POST /invites/form should redirect with 303"
    );
    let location = resp.headers()["location"].to_str().unwrap();
    assert!(
        location.contains(&space_id),
        "redirect should point to /spaces/:space_id, got: {location}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_post_invites_form_unknown_space_returns_404() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/invites/form"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("space_id=nonexistent-space-id")
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        404,
        "unknown space should return 404 from POST /invites/form"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_post_invites_revoke_removes_invite() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let redirect_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let client = reqwest::Client::new();

    let space_id = common::register_space(&client, &base_url, "form-revoke-space").await;
    let code = common::create_invite(&client, &base_url, &space_id).await;

    // Revoke via form POST.
    let resp = redirect_client
        .post(format!("{base_url}/invites/revoke"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("code={code}&space_id={space_id}"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        303,
        "POST /invites/revoke should redirect with 303"
    );

    // The invite should now be gone (validate via JSON API returns 404).
    let check = client
        .get(format!("{base_url}/invites/{code}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        check.status(),
        404,
        "revoked invite should return 404 on GET /invites/:code"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_web_routes_absent_without_data_dir() {
    use kutl_relay::config::{
        DEFAULT_OUTBOUND_CAPACITY, DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS,
        RelayConfig,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        host: "127.0.0.1".into(),
        port: 0,
        relay_name: "test-no-web-relay".into(),
        require_auth: false,
        database_url: None,
        outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
        data_dir: None, // no data_dir -> no backends -> no web routes
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

    let client = reqwest::Client::new();

    // GET / should 404 when no web backend is configured.
    let resp = client.get(format!("http://{addr}/")).send().await.unwrap();
    assert!(
        resp.status() == 404 || resp.status() == 405,
        "GET / should not be mounted without data_dir, got: {}",
        resp.status()
    );
}
