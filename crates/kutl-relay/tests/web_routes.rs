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
async fn test_index_has_create_space_accordion() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{base_url}/")).send().await.unwrap();
    assert_eq!(resp.status(), 200, "GET / should return 200");

    let body = resp.text().await.unwrap();
    assert!(
        body.contains(r#"<details class="create-space">"#),
        "index should render the create-space accordion"
    );
    assert!(
        body.contains("New space"),
        "index should show the New space CTA"
    );
    assert!(
        !body.contains(r#"<details class="create-space" open"#),
        "the accordion should be collapsed by default"
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
    assert!(
        body.contains("Copy invite link"),
        "space detail should show a Copy invite link button"
    );
    assert!(
        body.contains(&format!(r#"data-code="{code}""#)),
        "copy button should carry the invite code in data-code"
    );
    assert!(
        body.contains("location.origin + '/join/'"),
        "copy script should build the absolute /join/ link from the page origin"
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
async fn test_post_spaces_form_creates_space_and_redirects() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    // Disable redirect following so we can inspect the 303.
    let no_redirect = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let name = "form-create-space-ok";
    let resp = no_redirect
        .post(format!("{base_url}/spaces/form"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("name={name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        303,
        "POST /spaces/form should redirect with 303"
    );
    let location = resp.headers()["location"].to_str().unwrap();
    assert!(
        location.starts_with("/spaces/"),
        "redirect should point to /spaces/:space_id, got: {location}"
    );
    assert!(
        location.len() > "/spaces/".len(),
        "redirect should include a space id, got: {location}"
    );

    // Following redirects, the new space should appear in the index listing.
    let client = reqwest::Client::new();
    let index = client.get(format!("{base_url}/")).send().await.unwrap();
    assert_eq!(index.status(), 200);
    let body = index.text().await.unwrap();
    assert!(
        body.contains(name),
        "index should list the newly created space name"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_post_spaces_form_invalid_name_rejected() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    // "Bad Name!" violates the slug rules (uppercase, space, punctuation).
    let resp = client
        .post(format!("{base_url}/spaces/form"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("name=Bad+Name%21")
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "invalid space name should return 400 from POST /spaces/form"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_post_spaces_form_duplicate_name_conflict() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let no_redirect = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let name = "form-dup-space-name";

    // First registration succeeds.
    let first = no_redirect
        .post(format!("{base_url}/spaces/form"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("name={name}"))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 303, "first create should redirect with 303");

    // Second registration with the same name conflicts.
    let second = no_redirect
        .post(format!("{base_url}/spaces/form"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("name={name}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        second.status(),
        409,
        "duplicate space name should return 409 from POST /spaces/form"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_web_routes_absent_without_web_backend() {
    use kutl_relay::config::RelayConfig;

    // Web-admin routes are gated on the web-space backend: a host
    // that manages spaces elsewhere (kutlhub) passes no web backend, and the HTML
    // admin routes are then not mounted. The OSS binary always has one, so
    // construct the storeless (no-backend) shape via the host seam to exercise
    // the absent-route path.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    // Authentication is mandatory; seed an (empty) authorized_keys
    // file so the relay boots. This test is about web-route mounting, not auth.
    let keys_dir = tempfile::tempdir().unwrap();
    let keys_path = keys_dir.path().join("authorized_keys");
    std::fs::write(&keys_path, "# test keys\n").unwrap();
    let config = RelayConfig {
        port: 0,
        relay_name: "test-no-web-relay".into(),
        authorized_keys_file: Some(keys_path),
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = common::build_storeless_app(config);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();

    // GET / should 404 when no web backend is configured.
    let resp = client.get(format!("http://{addr}/")).send().await.unwrap();
    assert!(
        resp.status() == 404 || resp.status() == 405,
        "GET / should not be mounted without a web backend, got: {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_favicon_served() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/favicon.svg"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET /favicon.svg should return 200");
    let ct = resp.headers()["content-type"].to_str().unwrap();
    assert!(
        ct.contains("image/svg+xml"),
        "favicon content-type should be image/svg+xml, got {ct}"
    );
    let cc = resp.headers()["cache-control"].to_str().unwrap();
    assert!(
        cc.contains("immutable"),
        "favicon should be cacheable, got {cc}"
    );
    let body = resp.text().await.unwrap();
    assert!(body.contains("<svg"), "favicon body should be SVG markup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rajdhani_font_served() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/assets/rajdhani.woff2"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "GET /assets/rajdhani.woff2 should return 200"
    );
    let ct = resp.headers()["content-type"].to_str().unwrap();
    assert!(
        ct.contains("font/woff2"),
        "font content-type should be font/woff2, got {ct}"
    );
    let cc = resp.headers()["cache-control"].to_str().unwrap();
    assert!(
        cc.contains("immutable"),
        "font should be cacheable, got {cc}"
    );
    let bytes = resp.bytes().await.unwrap();
    // woff2 files begin with the signature "wOF2".
    assert_eq!(&bytes[0..4], b"wOF2", "served bytes should be a woff2 file");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pages_have_brand_header_and_favicon() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();
    common::register_space(&client, &base_url, "brand-header-space").await;

    let resp = client.get(format!("{base_url}/")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains(r#"class="brand-header""#),
        "page should render the brand header"
    );
    assert!(
        body.contains(r#"rel="icon""#),
        "page head should link the favicon"
    );
    assert!(
        body.contains("<svg"),
        "brand header should inline the logo SVG"
    );
    assert!(
        body.contains("/assets/rajdhani.woff2"),
        "inline stylesheet should reference the embedded font"
    );
}
