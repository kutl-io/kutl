//! `kutl join` journeys against the OSS relay.
//!
//! The OSS relay serves capability-URL invites (`POST /invites` mints a code;
//! `GET /invites/{code}` resolves it anonymously) and bare-name resolution
//! (`GET /spaces/resolve?name=...`). These journeys drive the real CLI
//! invite-redemption flow end to end: policy gate, invite-URL join, bare-name
//! join, and a doc syncing BOTH ways after the join.
//!
//! NOT covered here, deliberately: `owner/slug` join — the OSS relay has no
//! membership backend (its `ResolveSpace` op answers "operation not supported
//! on this relay"), and reaching even that answer needs a valid bearer for the
//! WS handshake, which no CLI verb can mint non-interactively today.
//!
//! The pre-join policy gate applies only to a join whose own calls carry a
//! bearer (`owner/slug`, a MEMBERSHIP_GRANT invite). Capability-URL and
//! bare-name joins resolve anonymously and proceed without a token even on a
//! relay advertising `AuthModel::ACCOUNT_REQUIRED`; the joiner authenticates
//! at sync time as a did:key client.

use kutl_cli_uxr::harness::RelayProcess;
use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::{human_did_of, init_and_authorize, init_space, space_id_of};

/// The `space_id` a joined/inited directory's `.kutl/space.toml` points at.
fn space_config_id(space: &std::path::Path) -> String {
    kutl_client::SpaceConfig::load(space)
        .unwrap_or_else(|e| panic!("read space config under {}: {e}", space.display()))
        .space_id
}

/// The join policy gate, observable from the CLI: a non-interactive
/// `kutl join owner/slug` with NO stored token is refused up front with an
/// actionable error — and fails CLOSED (no identity minted, no space config
/// written). The `owner/slug` form resolves over the authenticated socket,
/// so it is the form the gate guards; the relay advertises
/// ACCOUNT_REQUIRED and the refusal fires before any connection.
#[tokio::test]
async fn join_gate_refuses_unauthenticated_noninteractive() {
    let relay = RelayProcess::spawn().await;

    let home_b = TestHome::new();
    let space_b = home_b.space_dir();
    let join = cli::kutl_in(
        home_b.path(),
        space_b.path(),
        &[
            "join",
            "acme/project",
            "--relay",
            &relay.ws_url(),
            "--dir",
            ".",
        ],
    )
    .await;
    assert!(
        !join.status.success(),
        "gate must refuse an unauthenticated non-interactive join: {}",
        cli::stdout_str(&join)
    );
    let err = cli::stderr_str(&join);
    assert!(
        err.contains(&format!("not authenticated to {}", relay.http_base())),
        "gate error should name the relay it wants auth for: {err}"
    );
    assert!(
        err.contains("kutl auth login"),
        "gate error should name the remedy verb: {err}"
    );

    // Fail-closed: the refusal happens BEFORE identity provisioning or any
    // config write, so nothing half-made is left behind.
    assert!(
        kutl_client::Identity::load_if_present(&home_b.path().join("identity.toml"))
            .unwrap()
            .is_none(),
        "no identity should be minted when the gate refuses"
    );
    assert!(
        !kutl_client::SpaceConfig::is_joined(space_b.path()),
        "no space config should be written when the gate refuses"
    );
}

/// The flagship invite journey: A inits + syncs a doc, mints an invite; B
/// redeems it with `kutl join <invite-url>` (browser-style `/join/{code}`
/// link), lands on the same space, and after authorization a doc syncs BOTH
/// ways. Also pins the error for a dud code.
#[tokio::test]
async fn join_via_invite_url_syncs_both_ways() {
    let relay = RelayProcess::spawn().await;

    // A: init, authorize, seed a doc, push it.
    let home_a = TestHome::new();
    let space_a_dir = home_a.space_dir();
    let space_a = space_a_dir.path();
    let did_a = init_and_authorize(&relay, &home_a, space_a, "acme").await;
    std::fs::write(space_a.join("welcome.md"), "hello from A\n").unwrap();
    let sync_a = cli::kutl_in(home_a.path(), space_a, &["sync"]).await;
    assert!(
        sync_a.status.success(),
        "A's seed sync failed: {}",
        cli::stderr_str(&sync_a)
    );

    let space_id = space_id_of(
        &cli::kutl_in(
            home_a.path(),
            space_a,
            &["space", "list", "--format", "json"],
        )
        .await,
    );
    let code = relay.create_invite(&space_id).await;

    // B: a dud code first — the error must name the code and say why.
    let home_b = TestHome::new();
    let space_b_dir = home_b.space_dir();
    let space_b = space_b_dir.path();
    // Capability-URL redemption is anonymous: no token, and the relay's
    // ACCOUNT_REQUIRED policy does not gate it.
    let dud = cli::kutl_in(
        home_b.path(),
        space_b,
        &[
            "join",
            &format!("{}/join/{}", relay.http_base(), "deadbeef"),
            "--dir",
            ".",
        ],
    )
    .await;
    assert!(!dud.status.success(), "a dud invite code must fail");
    let err = cli::stderr_str(&dud);
    assert!(
        err.contains("invite not found or expired: deadbeef"),
        "dud-code error should name the code and the reason: {err}"
    );

    // B: redeem the real invite via the browser-style landing URL.
    let invite_url = format!("{}/join/{code}", relay.http_base());
    let join = cli::kutl_in(home_b.path(), space_b, &["join", &invite_url, "--dir", "."]).await;
    assert!(
        join.status.success(),
        "an unauthenticated capability-URL join must proceed: {}",
        cli::stderr_str(&join)
    );
    let out = cli::stdout_str(&join);
    assert!(
        out.contains("Joined space acme via invite."),
        "join should confirm the space by name: {out}"
    );
    assert!(
        out.contains(&space_id),
        "join should echo the joined space_id: {out}"
    );
    assert!(
        out.contains(&relay.ws_url()),
        "join should echo the relay it bound the space to: {out}"
    );
    assert!(
        out.contains("Run `kutl daemon start` to begin syncing."),
        "join should tell the user the next step: {out}"
    );
    assert_eq!(
        space_config_id(space_b),
        space_id,
        "B's space config must point at A's space"
    );

    // B's identity was provisioned by the join; authorize it (harness rail —
    // this journey is about `join`, not the authorize verb) and pull.
    let did_b =
        human_did_of(&cli::kutl_in(home_b.path(), space_b, &["status", "--format", "json"]).await);
    assert_ne!(did_b, did_a, "B must have its own identity");

    // HUMAN render, joiner state: identity provisioned, no stored token — the
    // `auth status` line says exactly that and shows the DID.
    let auth = cli::kutl_in(home_b.path(), space_b, &["auth", "status"]).await;
    assert!(auth.status.success());
    let out = cli::stdout_str(&auth);
    assert!(
        out.contains("identity: local (no relay token yet)"),
        "post-join human auth status should show a local identity: {out}"
    );
    assert!(
        out.contains(&did_b),
        "post-join human auth status should show B's DID: {out}"
    );
    relay.authorize_did(&did_b);
    let sync_b = cli::kutl_in(home_b.path(), space_b, &["sync"]).await;
    assert!(
        sync_b.status.success(),
        "B's first sync failed: {}",
        cli::stderr_str(&sync_b)
    );
    assert_eq!(
        std::fs::read_to_string(space_b.join("welcome.md")).expect("welcome.md synced to B"),
        "hello from A\n",
        "A's doc should arrive at B intact"
    );

    // ...and back: B authors, A pulls.
    std::fs::write(space_b.join("reply.md"), "hello from B\n").unwrap();
    let sync_b2 = cli::kutl_in(home_b.path(), space_b, &["sync"]).await;
    assert!(
        sync_b2.status.success(),
        "B's push sync failed: {}",
        cli::stderr_str(&sync_b2)
    );
    let sync_a2 = cli::kutl_in(home_a.path(), space_a, &["sync"]).await;
    assert!(
        sync_a2.status.success(),
        "A's pull sync failed: {}",
        cli::stderr_str(&sync_a2)
    );
    assert_eq!(
        std::fs::read_to_string(space_a.join("reply.md")).expect("reply.md synced to A"),
        "hello from B\n",
        "B's doc should arrive at A intact"
    );
}

/// Bare-name join, the OSS relay's second live form: `kutl join <name>
/// --relay <url>` resolves via `GET /spaces/resolve?name=...` and writes the
/// same space config; an unknown name fails with an error that names it.
#[tokio::test]
async fn join_via_bare_name_resolves_or_names_the_miss() {
    let relay = RelayProcess::spawn().await;
    let home_a = TestHome::new();
    let space_a = home_a.space_dir();
    init_space(&relay, &home_a, space_a.path(), "acme").await;
    let space_id = space_id_of(
        &cli::kutl_in(
            home_a.path(),
            space_a.path(),
            &["space", "list", "--format", "json"],
        )
        .await,
    );

    // Bare-name resolution is anonymous HTTP: no token needed.
    let home_b = TestHome::new();
    let space_b = home_b.space_dir();
    let join = cli::kutl_in(
        home_b.path(),
        space_b.path(),
        &["join", "acme", "--relay", &relay.ws_url(), "--dir", "."],
    )
    .await;
    assert!(
        join.status.success(),
        "bare-name join failed: {}",
        cli::stderr_str(&join)
    );
    let out = cli::stdout_str(&join);
    assert!(
        out.contains("Joined space acme."),
        "bare-name join should confirm by name: {out}"
    );
    assert_eq!(
        space_config_id(space_b.path()),
        space_id,
        "bare-name join must bind to the registered space"
    );

    // Unknown name: the miss is named, not a generic failure.
    let ghost_dir = tempfile::tempdir().unwrap();
    let ghost = cli::kutl_in(
        home_b.path(),
        ghost_dir.path(),
        &["join", "ghost", "--relay", &relay.ws_url(), "--dir", "."],
    )
    .await;
    assert!(!ghost.status.success(), "unknown-name join must fail");
    let err = cli::stderr_str(&ghost);
    assert!(
        err.contains("space not found: ghost"),
        "miss should name the space it looked for: {err}"
    );
}
