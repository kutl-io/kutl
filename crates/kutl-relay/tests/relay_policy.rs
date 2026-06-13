//! Integration test for the `GET /relay-policy` discovery endpoint (RFD 0086).
//!
//! The relay advertises its onboarding policy unauthenticated and pre-connect,
//! so a client can learn the auth/invite model before creating identity or
//! opening a WebSocket.

mod common;

use kutl_proto::sync::{AuthModel, InviteModel, NextStep, RelayPolicy};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_relay_policy_endpoint_advertises_oss_anonymous_capability() {
    // OSS relay: SQLite invite backend, no membership backend, no required auth.
    let (base_url, _dir) = common::start_relay_with_data_dir(true).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/relay-policy"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET /relay-policy should return 200");

    let policy: RelayPolicy = resp.json().await.unwrap();
    assert_eq!(
        policy.auth_model(),
        AuthModel::AnonymousDid,
        "OSS relay treats a self-minted DID as the principal"
    );
    assert_eq!(
        policy.invite_model(),
        InviteModel::CapabilityUrl,
        "OSS invites are anonymous capability URLs"
    );
    assert_eq!(
        policy.next(),
        NextStep::Proceed,
        "no auth required -> proceed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_relay_policy_endpoint_advertises_account_required_authenticate() {
    // Account-required relay: exercises handle_relay_policy's own next/auth_url
    // derivation (not just build_relay_policy's pass-through), which the unit
    // tests bypass by passing next/auth_url as literals.
    let (base_url, _dir) = common::start_account_required_relay().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base_url}/relay-policy"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "GET /relay-policy is unauthenticated even on an account-required relay"
    );

    let policy: RelayPolicy = resp.json().await.unwrap();
    assert_eq!(
        policy.auth_model(),
        AuthModel::AccountRequired,
        "an account-required relay requires an account session"
    );
    assert_eq!(
        policy.next(),
        NextStep::Authenticate,
        "account-required -> the client must authenticate"
    );
    assert!(
        !policy.auth_url.is_empty(),
        "an account-required relay must advertise where to authenticate"
    );
}
