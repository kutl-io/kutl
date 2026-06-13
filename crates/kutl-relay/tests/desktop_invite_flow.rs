//! End-to-end tests exercising the desktop app's invite-based onboarding flow
//! against a real OSS relay.
//!
//! These tests simulate the HTTP calls that `kutl-desktop` makes during the
//! setup wizard: validate an invite via `GET /invites/{code}`, verify the join
//! page HTML at `GET /join/{code}`, and confirm space registration via
//! `POST /spaces/register`. The relay runs in-process with `SQLite` persistence.

mod common;

// ---------------------------------------------------------------------------
// Happy path — full invite onboarding flow
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_desktop_invite_flow_happy_path() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    // 1. Space owner registers a space.
    let space_id = common::register_space(&client, &base_url, "team-docs").await;

    // 2. Space owner creates an invite.
    let code = common::create_invite(&client, &base_url, &space_id).await;

    // 3. Desktop app receives a join URL like http://localhost:<port>/join/<code>.
    //    It extracts the code and validates via GET /invites/{code}.
    let validate_resp = client
        .get(format!("{base_url}/invites/{code}"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        validate_resp.status(),
        200,
        "valid invite code should return 200"
    );

    let body: serde_json::Value = validate_resp.json().await.unwrap();
    assert_eq!(
        body["space_id"], space_id,
        "response should contain the correct space_id"
    );
    assert_eq!(
        body["space_name"], "team-docs",
        "response should contain the correct space_name"
    );

    // 4. The web admin join page at GET /join/{code} should render HTML with the
    //    space name and a deep-link for the desktop app.
    let join_page_resp = client
        .get(format!("{base_url}/join/{code}"))
        .send()
        .await
        .unwrap();

    assert_eq!(join_page_resp.status(), 200, "join page should return 200");
    let ct = join_page_resp.headers()["content-type"].to_str().unwrap();
    assert!(ct.contains("text/html"), "join page should be HTML");

    let html = join_page_resp.text().await.unwrap();
    assert!(
        html.contains("team-docs"),
        "join page should display the space name"
    );
}

// ---------------------------------------------------------------------------
// Error handling — expired / non-existent code
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_desktop_invite_expired_code() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    // Register a space so the relay is not completely empty.
    let _space_id = common::register_space(&client, &base_url, "orphan-space").await;

    // Try to validate a code that was never created.
    let resp = client
        .get(format!("{base_url}/invites/0000000000000000"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        404,
        "non-existent invite code should return 404"
    );
}

// ---------------------------------------------------------------------------
// Revocation — invite becomes invalid after DELETE
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_desktop_invite_revoked() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let space_id = common::register_space(&client, &base_url, "revoke-flow-space").await;
    let code = common::create_invite(&client, &base_url, &space_id).await;

    // Validate should succeed initially.
    let valid_resp = client
        .get(format!("{base_url}/invites/{code}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        valid_resp.status(),
        200,
        "invite should be valid before revocation"
    );

    // Space owner revokes the invite.
    let delete_resp = client
        .delete(format!("{base_url}/invites/{code}"))
        .send()
        .await
        .unwrap();
    assert_eq!(delete_resp.status(), 200, "revoke should return 200");

    // Desktop app tries to validate again — should fail.
    let invalid_resp = client
        .get(format!("{base_url}/invites/{code}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        invalid_resp.status(),
        404,
        "revoked invite should return 404"
    );
}

// ---------------------------------------------------------------------------
// Web admin index — lists registered spaces
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_desktop_web_admin_index() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let _space_id = common::register_space(&client, &base_url, "admin-listed-space").await;

    let resp = client.get(format!("{base_url}/")).send().await.unwrap();
    assert_eq!(resp.status(), 200, "GET / should return 200");

    let ct = resp.headers()["content-type"].to_str().unwrap();
    assert!(ct.contains("text/html"), "index should return HTML");

    let html = resp.text().await.unwrap();
    assert!(
        html.contains("admin-listed-space"),
        "index page should list the registered space"
    );
}

// ---------------------------------------------------------------------------
// Space registration — create + duplicate conflict
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_desktop_space_registration() {
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    // First registration should succeed with 201.
    let resp = client
        .post(format!("{base_url}/spaces/register"))
        .json(&serde_json::json!({"name": "my-project"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201, "first registration should return 201");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["space_id"].as_str().is_some_and(|id| !id.is_empty()),
        "response should include a non-empty space_id"
    );
    assert_eq!(
        body["name"], "my-project",
        "response should echo back the space name"
    );

    // Duplicate registration should return 409 Conflict.
    let dup_resp = client
        .post(format!("{base_url}/spaces/register"))
        .json(&serde_json::json!({"name": "my-project"}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        dup_resp.status(),
        409,
        "duplicate space name should return 409 Conflict"
    );
}
