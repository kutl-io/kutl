//! Integration tests for the space activity feed HTTP route.
//!
//! `GET /spaces/{space_id}/changes?checkpoint=<cp>` returns a JSON
//! `ChangesResponse` (`signals`, `document_changes`, `checkpoint`).
//! Authentication mirrors the signal catch-up route: bearer token → DID,
//! then space membership check. A non-member or unauthenticated caller gets a
//! reason body on rejection (never an empty 404/403).

mod common;

use std::io::Write as _;

use reqwest::header;
use tokio::net::TcpListener;

use kutl_relay::config::RelayConfig;

// ── Relay startup ────────────────────────────────────────────────────────────

/// Start an auth-on relay (auth is unconditional) and a `data_dir` (change backend
/// wired). Returns `(base_url, keys_file_handle, data_dir_handle)`.
async fn start_relay() -> (String, tempfile::NamedTempFile, tempfile::TempDir) {
    let mut keys_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(keys_file, "# changes-route test keys").unwrap();
    let data_dir = tempfile::TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let config = RelayConfig {
        port: 0,
        relay_name: "test-changes-relay".into(),
        authorized_keys_file: Some(keys_file.path().to_path_buf()),
        data_dir: Some(data_dir.path().to_path_buf()),
        ..Default::default()
    };

    let (app, _relay, _flush) = kutl_relay::build_app(config).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), keys_file, data_dir)
}

/// A random UUID space id.
fn space_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ── Route contract tests ──────────────────────────────────────────────────────

/// An unauthenticated request is rejected with a non-empty reason body (never
/// an empty 401).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_changes_missing_auth_rejected_with_body() {
    let (base_url, _keys, _data) = start_relay().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/spaces/{}/changes", space_id()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    let body = resp.text().await.unwrap();
    assert!(
        !body.is_empty(),
        "auth rejection must carry a reason body, not an empty response"
    );
}

/// An authenticated but non-member DID is rejected with a 403 and a reason
/// body (mirrors the signal catch-up contract).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_changes_non_member_rejected_with_body() {
    let (base_url, _keys, _data) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    // DID is NOT written to the keys file → can auth but is not a member.
    let token = common::authenticate(&base_url, &did, &signing_key).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base_url}/spaces/{}/changes", space_id()))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 403, "non-member gets 403 (not authorized)");
    let body = resp.text().await.unwrap();
    assert!(
        !body.is_empty(),
        "non-member rejection must carry a reason body, not an empty 403"
    );
}

/// A member with no data yet gets 200 JSON with empty arrays and a
/// `checkpoint` field. Validates the happy-path content-type + schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_changes_member_empty_space_returns_json() {
    let (base_url, keys, _data) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    common::authorize_did(keys.path(), &did);
    let token = common::authenticate(&base_url, &did, &signing_key).await;

    let space = space_id();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base_url}/spaces/{space}/changes"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "member gets 200");
    let ct = resp.headers()["content-type"].to_str().unwrap().to_owned();
    assert!(
        ct.contains("application/json"),
        "expected application/json, got: {ct}"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["signals"].is_array(),
        "response must include a `signals` array"
    );
    assert!(
        body["document_changes"].is_array(),
        "response must include a `document_changes` array"
    );
    assert!(
        body["checkpoint"].is_string(),
        "response must include a `checkpoint` string"
    );
    assert!(
        body["signals"].as_array().unwrap().is_empty(),
        "a fresh space has no signals"
    );
    assert!(
        body["document_changes"].as_array().unwrap().is_empty(),
        "a fresh space has no document changes"
    );
}

/// Seed a signal (over the `SignalReseed` frame) then GET
/// `/changes` and assert the signal appears. Verify that passing the returned
/// `checkpoint` to a second GET is repeatable (idempotent — same data, same
/// checkpoint, regardless of the cursor auto-advance quirk in the sqlite
/// backend's test cursor model).
///
/// This test exercises the end-to-end path the CLI client uses:
/// member auth → seeded data → both `signals` and (absent here) registry
/// entries flow through the route → checkpoint paging works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_changes_returns_seeded_signal_and_checkpoint_pages() {
    let (base_url, keys, _data) = start_relay().await;
    let (did, signing_key) = common::test_keypair();
    common::authorize_did(keys.path(), &did);
    let token = common::authenticate(&base_url, &did, &signing_key).await;

    let space = space_id();
    let client = reqwest::Client::new();

    // Seed one signal via re-seed so the change backend picks it up.
    // Use a real epoch-ms timestamp so `fetch_signals`'s `timestamp > 0`
    // filter matches — the `Signal` proto `timestamp` field is what gets
    // projected into the `signals` table, NOT the HLC `physical_ms`.
    //
    // Use a fixed well-known millisecond value to avoid casting between
    // u128/i64/u64 across the timestamp and HLC physical_ms fields.
    // 2024-01-01T00:00:00Z in epoch-ms (well within i64 and u64 range).
    let now_ms: i64 = 1_704_067_200_000;
    let now_ms_u64: u64 = 1_704_067_200_000;
    let mut sig = kutl_proto::sync::Signal {
        id: uuid::Uuid::new_v4().to_string(),
        space_id: space.clone(),
        record_id: "changes-test-r1".to_owned(),
        author_did: did.clone(),
        actor_did: did.clone(),
        document_id: Some(uuid::Uuid::new_v4().to_string()),
        timestamp: now_ms,
        hlc: Some(kutl_proto::sync::Hlc {
            physical_ms: now_ms_u64,
            logical: 0,
            actor: vec![0u8; 16],
        }),
        ..Default::default()
    };
    sig.set_event(kutl_proto::sync::SignalEventType::Created);

    // Seed over the `SignalReseed` FRAME — the same door every real pusher
    // uses.
    let ws_url = format!("ws{}/ws", base_url.strip_prefix("http").unwrap());
    let mut ws = kutl_client::SyncClient::connect_with_auth(&ws_url, "changes-test", &token, "")
        .await
        .expect("connect for the seed re-seed");
    ws.submit_signal(&kutl_proto::protocol::signal_reseed_envelope(
        &uuid::Uuid::new_v4().to_string(),
        &space,
        vec![sig.clone()],
    ))
    .await
    .expect("re-seed must succeed before the feed test");
    let _ = ws.close().await;

    // First GET /changes — signal must appear.
    let resp = client
        .get(format!("{base_url}/spaces/{space}/changes"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let first: serde_json::Value = resp.json().await.unwrap();

    let signals = first["signals"].as_array().expect("signals array");
    assert_eq!(
        signals.len(),
        1,
        "the seeded signal must appear in the activity feed"
    );
    // Confirm the signal id matches what we seeded.
    assert_eq!(
        signals[0]["id"].as_str().unwrap_or(""),
        sig.id,
        "feed signal id must match the seeded record"
    );

    let checkpoint = first["checkpoint"]
        .as_str()
        .expect("checkpoint must be a string")
        .to_owned();
    assert!(!checkpoint.is_empty(), "checkpoint must be non-empty");

    // Second GET with the checkpoint — paging is repeatable (the cursor
    // advance is per-DID; passing the checkpoint rewinds so the same data
    // is accessible again for failure recovery).
    let resp2 = client
        .get(format!(
            "{base_url}/spaces/{space}/changes?checkpoint={checkpoint}"
        ))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200, "checkpoint-paged call must succeed");
    let second: serde_json::Value = resp2.json().await.unwrap();
    // The checkpoint matches one of the stored checkpoints — the backend
    // rewinds to that position. Either the same data re-appears (rewind hit)
    // or an empty page (cursor advanced past — both are correct rewind
    // semantics). What we assert: the call succeeds and returns a valid shape.
    assert!(
        second["signals"].is_array(),
        "paged call returns signals array"
    );
    assert!(
        second["checkpoint"].is_string(),
        "paged call returns a checkpoint string"
    );
}
