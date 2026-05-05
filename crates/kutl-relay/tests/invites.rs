//! Integration tests for `SqliteInviteBackend` and invite HTTP routes.
//!
//! Both `SqliteSpaceBackend` and `SqliteInviteBackend` share a single pool
//! opened against a temporary `registry.db`, matching the pattern used in
//! production `build_app`.
//!
//! The second section exercises the HTTP endpoints (`POST /invites`,
//! `GET /invites`, `GET /invites/:code`, `DELETE /invites/:code`) through
//! a real axum relay instance.

mod common;

use kutl_relay::invite_backend::{InviteBackend, InviteBackendError};
use kutl_relay::space_backend::{SpaceBackend, SqliteSpaceBackend};
use kutl_relay::sqlite_invite_backend::SqliteInviteBackend;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Open a shared in-memory pool and ensure both backends have their schemas.
async fn open_backends() -> (SqliteSpaceBackend, SqliteInviteBackend) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();

    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(&pool)
        .await
        .unwrap();

    let space_backend = SqliteSpaceBackend::new(pool.clone()).await.unwrap();
    let invite_backend = SqliteInviteBackend::new(pool).await.unwrap();

    (space_backend, invite_backend)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_invite_and_validate() {
    let (spaces, invites) = open_backends().await;

    let space = spaces.register("my-test-space").await.unwrap();

    let record = invites.create_invite(&space.space_id, None).unwrap();
    assert_eq!(record.space_id, space.space_id);
    assert_eq!(record.space_name, "my-test-space");
    assert!(record.expires_at.is_none());
    assert!(!record.code.is_empty());
    // Invite codes are 8 bytes hex-encoded → 16 chars.
    assert_eq!(record.code.len(), 16);

    let info = invites.validate_invite(&record.code).unwrap();
    let info = info.expect("invite should be valid");
    assert_eq!(info.space_id, space.space_id);
    assert_eq!(info.space_name, "my-test-space");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_validate_invite_unknown_code_returns_none() {
    let (_spaces, invites) = open_backends().await;

    let result = invites.validate_invite("0000000000000000").unwrap();
    assert!(result.is_none(), "unknown code should return None");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_validate_invite_expired_returns_err() {
    let (spaces, invites) = open_backends().await;

    let space = spaces.register("expiry-test-space").await.unwrap();

    // expires_at in the past.
    let past_ms: i64 = 1_000;
    let record = invites
        .create_invite(&space.space_id, Some(past_ms))
        .unwrap();

    let result = invites.validate_invite(&record.code).unwrap_err();
    assert!(
        matches!(result, InviteBackendError::Expired(_)),
        "expected Expired error, got: {result}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_validate_invite_future_expiry_is_valid() {
    let (spaces, invites) = open_backends().await;

    let space = spaces.register("future-expiry-space").await.unwrap();

    let record = invites
        .create_invite(&space.space_id, Some(far_future()))
        .unwrap();

    let info = invites.validate_invite(&record.code).unwrap();
    assert!(info.is_some(), "invite with future expiry should be valid");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_revoke_invite() {
    let (spaces, invites) = open_backends().await;

    let space = spaces.register("revoke-test-space").await.unwrap();
    let record = invites.create_invite(&space.space_id, None).unwrap();

    // Revoke returns true when the invite existed.
    let deleted = invites.revoke_invite(&record.code).unwrap();
    assert!(deleted, "revoke should return true when invite existed");

    // Validate after revoke returns None (code gone).
    let result = invites.validate_invite(&record.code).unwrap();
    assert!(result.is_none(), "revoked invite should not validate");

    // Revoking again returns false.
    let deleted_again = invites.revoke_invite(&record.code).unwrap();
    assert!(!deleted_again, "second revoke should return false");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_invites_for_space() {
    let (spaces, invites) = open_backends().await;

    let space_a = spaces.register("list-space-a").await.unwrap();
    let space_b = spaces.register("list-space-b").await.unwrap();

    // Create two invites for space_a, one for space_b.
    let _inv_a1 = invites.create_invite(&space_a.space_id, None).unwrap();
    let _inv_a2 = invites
        .create_invite(&space_a.space_id, Some(far_future()))
        .unwrap();
    let _inv_b = invites.create_invite(&space_b.space_id, None).unwrap();

    let list_a = invites.list_invites(&space_a.space_id).unwrap();
    assert_eq!(list_a.len(), 2, "expected 2 invites for space_a");
    for inv in &list_a {
        assert_eq!(inv.space_id, space_a.space_id);
        assert_eq!(inv.space_name, "list-space-a");
    }

    let list_b = invites.list_invites(&space_b.space_id).unwrap();
    assert_eq!(list_b.len(), 1, "expected 1 invite for space_b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_invites_includes_expired() {
    let (spaces, invites) = open_backends().await;

    let space = spaces.register("list-expired-space").await.unwrap();

    // One expired, one not expired.
    let _expired = invites.create_invite(&space.space_id, Some(1_000)).unwrap();
    let _valid = invites.create_invite(&space.space_id, None).unwrap();

    let list = invites.list_invites(&space.space_id).unwrap();
    // list_invites returns ALL invites including expired ones.
    assert_eq!(list.len(), 2, "list should include expired invites");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_invite_unknown_space_returns_err() {
    let (_spaces, invites) = open_backends().await;

    let result = invites
        .create_invite("nonexistent-space-id", None)
        .unwrap_err();
    assert!(
        matches!(result, InviteBackendError::SpaceNotFound(_)),
        "expected SpaceNotFound, got: {result}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_invite_codes_are_unique() {
    let (spaces, invites) = open_backends().await;

    let space = spaces.register("unique-codes-space").await.unwrap();

    let codes: std::collections::HashSet<String> = (0..10)
        .map(|_| invites.create_invite(&space.space_id, None).unwrap().code)
        .collect();

    assert_eq!(codes.len(), 10, "invite codes should be unique");
}

// ---------------------------------------------------------------------------
// HTTP route integration tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_create_invite() {
    let (base_url, _dir) = common::start_relay_with_data_dir(false).await;
    let client = reqwest::Client::new();

    let space_id = common::register_space(&client, &base_url, "http-invite-space").await;

    let resp = client
        .post(format!("{base_url}/invites"))
        .json(&serde_json::json!({"space_id": space_id}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201, "expected 201 from POST /invites");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["space_id"], space_id);
    assert_eq!(body["space_name"], "http-invite-space");
    assert!(
        body["code"].as_str().is_some_and(|c| c.len() == 16),
        "code should be 16 hex chars, got: {body}"
    );
    assert!(body["expires_at"].is_null(), "no expiry should be null");
    assert!(
        body["created_at"].as_i64().is_some_and(|t| t > 0),
        "created_at should be a positive timestamp"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_create_invite_with_expiry() {
    let (base_url, _dir) = common::start_relay_with_data_dir(false).await;
    let client = reqwest::Client::new();

    let space_id = common::register_space(&client, &base_url, "expiry-invite-space").await;

    let resp = client
        .post(format!("{base_url}/invites"))
        .json(&serde_json::json!({"space_id": space_id, "expires_in_hours": 24}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["expires_at"].as_i64().is_some_and(|t| t > 0),
        "expires_at should be set when expires_in_hours is provided"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_create_invite_unknown_space_returns_404() {
    let (base_url, _dir) = common::start_relay_with_data_dir(false).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/invites"))
        .json(&serde_json::json!({"space_id": "nonexistent-space-id"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404, "unknown space should return 404");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_get_invite_validate() {
    let (base_url, _dir) = common::start_relay_with_data_dir(false).await;
    let client = reqwest::Client::new();

    let space_id = common::register_space(&client, &base_url, "validate-invite-space").await;

    // Create an invite.
    let create_resp = client
        .post(format!("{base_url}/invites"))
        .json(&serde_json::json!({"space_id": space_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), 201);
    let create_body: serde_json::Value = create_resp.json().await.unwrap();
    let code = create_body["code"].as_str().unwrap();

    // Validate the invite.
    let get_resp = client
        .get(format!("{base_url}/invites/{code}"))
        .send()
        .await
        .unwrap();

    assert_eq!(get_resp.status(), 200, "valid code should return 200");
    let get_body: serde_json::Value = get_resp.json().await.unwrap();
    assert_eq!(get_body["space_id"], space_id);
    assert_eq!(get_body["space_name"], "validate-invite-space");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_get_invite_unknown_returns_404() {
    let (base_url, _dir) = common::start_relay_with_data_dir(false).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/invites/0000000000000000"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404, "unknown code should return 404");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_list_invites() {
    let (base_url, _dir) = common::start_relay_with_data_dir(false).await;
    let client = reqwest::Client::new();

    let space_id = common::register_space(&client, &base_url, "list-invites-space").await;

    // Create two invites.
    for _ in 0..2 {
        let resp = client
            .post(format!("{base_url}/invites"))
            .json(&serde_json::json!({"space_id": space_id}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);
    }

    // List invites for the space.
    let list_resp = client
        .get(format!("{base_url}/invites?space_id={space_id}"))
        .send()
        .await
        .unwrap();

    assert_eq!(list_resp.status(), 200);
    let list: serde_json::Value = list_resp.json().await.unwrap();
    let arr = list.as_array().expect("response should be a JSON array");
    assert_eq!(arr.len(), 2, "expected 2 invites in the list");
    for item in arr {
        assert_eq!(item["space_id"], space_id);
        assert_eq!(item["space_name"], "list-invites-space");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_revoke_invite() {
    let (base_url, _dir) = common::start_relay_with_data_dir(false).await;
    let client = reqwest::Client::new();

    let space_id = common::register_space(&client, &base_url, "revoke-invite-space").await;

    // Create an invite.
    let create_resp = client
        .post(format!("{base_url}/invites"))
        .json(&serde_json::json!({"space_id": space_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), 201);
    let code = create_resp.json::<serde_json::Value>().await.unwrap()["code"]
        .as_str()
        .unwrap()
        .to_owned();

    // Revoke the invite.
    let delete_resp = client
        .delete(format!("{base_url}/invites/{code}"))
        .send()
        .await
        .unwrap();
    assert_eq!(delete_resp.status(), 200, "revoke should return 200");

    // Validate after revoke returns 404 (code deleted).
    let get_resp = client
        .get(format!("{base_url}/invites/{code}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_resp.status(),
        404,
        "revoked invite should return 404 on GET"
    );

    // Revoking again returns 404.
    let second_delete = client
        .delete(format!("{base_url}/invites/{code}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        second_delete.status(),
        404,
        "second revoke should return 404"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_invite_routes_absent_without_data_dir() {
    use kutl_relay::config::RelayConfig;

    // Start a relay without data_dir — invite routes should not be mounted.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-no-invites-relay".into(),
        data_dir: None, // no data_dir → no invite backend
        ..Default::default()
    };

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/invites"))
        .json(&serde_json::json!({"space_id": "any"}))
        .send()
        .await
        .unwrap();

    // Without an invite backend the route is not mounted → 404 or 405.
    assert!(
        resp.status() == 404 || resp.status() == 405,
        "invite routes should not be mounted without data_dir, got: {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A timestamp far in the future (year 2099), in milliseconds since epoch.
fn far_future() -> i64 {
    /// Unix timestamp in millis for 2099-01-01 00:00:00 UTC.
    const YEAR_2099_MS: i64 = 4_070_908_800_000;
    YEAR_2099_MS
}
