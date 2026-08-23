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
//! KNOWN UX WART (asserted as CURRENT behavior, see the gate test): the relay
//! advertises `AuthModel::ACCOUNT_REQUIRED`, so the pre-join policy
//! gate demands a stored bearer token even though capability-URL redemption
//! itself performs zero authenticated calls — ANY non-empty `KUTL_TOKEN`
//! passes the gate and is never validated by the join that follows.

use kutl_cli_uxr::harness::RelayProcess;
use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::{human_did_of, space_id_of};

/// Env var the CLI's token resolution chain reads first. Set to pass the join
/// policy gate; explicitly REMOVED on DID-auth calls so a developer shell
/// can't leak a stale token into them.
const TOKEN_ENV: &str = "KUTL_TOKEN";

/// Init a space named `name` at `space` against `relay`, asserting success.
async fn init_space(relay: &RelayProcess, home: &TestHome, space: &std::path::Path, name: &str) {
    let init = cli::kutl_in(
        home.path(),
        space,
        &[
            "init",
            "--relay",
            &relay.ws_url(),
            "--dir",
            space.to_str().unwrap(),
            "--name",
            name,
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );
}

/// The `space_id` a joined/inited directory's `.kutl/space.json` points at.
fn space_config_id(space: &std::path::Path) -> String {
    let config_path = space.join(".kutl").join("space.json");
    let raw = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", config_path.display()));
    serde_json::from_str::<serde_json::Value>(&raw).expect("space.json is json")["space_id"]
        .as_str()
        .expect("space.json has space_id")
        .to_owned()
}

/// The join policy gate, observable from the CLI: a non-interactive
/// `kutl join <invite-url>` with NO stored token is refused up front with an
/// actionable error — and fails CLOSED (no identity minted, no space config
/// written). Pins the CURRENT gate behavior: the relay advertises
/// ACCOUNT_REQUIRED, so the gate fires even though capability-URL
/// redemption itself would have been anonymous.
#[tokio::test]
async fn join_gate_refuses_unauthenticated_noninteractive() {
    let relay = RelayProcess::spawn().await;
    let home_a = TestHome::new();
    let space_a = home_a.space_dir();
    init_space(&relay, &home_a, space_a.path(), "acme").await;

    // A real, redeemable invite — the joiner is blocked by the gate anyway.
    let space_id = space_id_of(
        &cli::kutl_in(
            home_a.path(),
            space_a.path(),
            &["space", "list", "--format", "json"],
        )
        .await,
    );
    let code = relay.create_invite(&space_id).await;
    let invite_url = format!("{}/join/{code}", relay.http_base());

    let home_b = TestHome::new();
    let space_b = home_b.space_dir();
    let join = cli::kutl_in_env(
        home_b.path(),
        space_b.path(),
        &["join", &invite_url, "--dir", "."],
        &[(TOKEN_ENV, None)],
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
        !home_b.path().join("identity.json").exists(),
        "no identity should be minted when the gate refuses"
    );
    assert!(
        !space_b.path().join(".kutl").join("space.json").exists(),
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
    init_space(&relay, &home_a, space_a, "acme").await;
    let did_a =
        human_did_of(&cli::kutl_in(home_a.path(), space_a, &["status", "--format", "json"]).await);
    relay.authorize_did(&did_a);
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
    // The token only satisfies the policy gate (see module header); the
    // capability-URL redemption that follows is anonymous.
    let gate_token = [(TOKEN_ENV, Some("kutl-any-value-passes-the-gate"))];
    let dud = cli::kutl_in_env(
        home_b.path(),
        space_b,
        &[
            "join",
            &format!("{}/join/{}", relay.http_base(), "deadbeef"),
            "--dir",
            ".",
        ],
        &gate_token,
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
    let join = cli::kutl_in_env(
        home_b.path(),
        space_b,
        &["join", &invite_url, "--dir", "."],
        &gate_token,
    )
    .await;
    assert!(
        join.status.success(),
        "join failed: {}",
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
    // `auth status` line says exactly that and shows the DID (the env token
    // above was per-invocation, so nothing is stored).
    let auth = cli::kutl_in_env(
        home_b.path(),
        space_b,
        &["auth", "status"],
        &[(TOKEN_ENV, None)],
    )
    .await;
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
    let sync_b = cli::kutl_in_env(home_b.path(), space_b, &["sync"], &[(TOKEN_ENV, None)]).await;
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
    let sync_b2 = cli::kutl_in_env(home_b.path(), space_b, &["sync"], &[(TOKEN_ENV, None)]).await;
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

    let gate_token = [(TOKEN_ENV, Some("kutl-any-value-passes-the-gate"))];
    let home_b = TestHome::new();
    let space_b = home_b.space_dir();
    let join = cli::kutl_in_env(
        home_b.path(),
        space_b.path(),
        &["join", "acme", "--relay", &relay.ws_url(), "--dir", "."],
        &gate_token,
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
    let ghost = cli::kutl_in_env(
        home_b.path(),
        ghost_dir.path(),
        &["join", "ghost", "--relay", &relay.ws_url(), "--dir", "."],
        &gate_token,
    )
    .await;
    assert!(!ghost.status.success(), "unknown-name join must fail");
    let err = cli::stderr_str(&ghost);
    assert!(
        err.contains("space not found: ghost"),
        "miss should name the space it looked for: {err}"
    );
}

/// The token side of the join gate, plus the HUMAN `auth status` render:
/// a fresh home is "none provisioned", `auth
/// token` stores, human + json status agree on what is stored (never the full
/// secret), `auth logout` removes and the render flips back.
///
/// (The third render arm — identity provisioned, no token — is asserted in
/// [`join_via_invite_url_syncs_both_ways`], where a joiner is in exactly that
/// state.)
#[tokio::test]
async fn auth_token_lifecycle_and_human_status() {
    // `auth token`/`auth status`/`auth logout` are pure local credential ops —
    // nothing dials this URL, so no relay process is needed.
    let relay_url = "ws://127.0.0.1:39999/ws";
    let home = TestHome::new();
    let cwd = tempfile::tempdir().unwrap();
    let no_env_token = [(TOKEN_ENV, None)];

    // A brand-new home has neither identity nor token, and says so.
    let fresh = cli::kutl_in_env(home.path(), cwd.path(), &["auth", "status"], &no_env_token).await;
    assert!(fresh.status.success());
    let out = cli::stdout_str(&fresh);
    assert!(
        out.contains("identity: none provisioned"),
        "fresh-home human status should say nothing is provisioned: {out}"
    );

    // Store a token for this relay (what a non-interactive joiner does to
    // satisfy the join policy gate).
    let store = cli::kutl_in_env(
        home.path(),
        cwd.path(),
        &[
            "auth",
            "token",
            "kutl_journey_pat_123456",
            "--relay",
            relay_url,
        ],
        &no_env_token,
    )
    .await;
    assert!(
        store.status.success(),
        "auth token failed: {}",
        cli::stderr_str(&store)
    );
    let out = cli::stdout_str(&store);
    assert!(
        out.contains("Token saved."),
        "auth token should confirm the save: {out}"
    );
    assert!(
        out.contains(relay_url),
        "auth token should echo the relay it bound the token to: {out}"
    );

    // Human render: the stored token shows as a truncated prefix @ relay —
    // never the full secret.
    let status =
        cli::kutl_in_env(home.path(), cwd.path(), &["auth", "status"], &no_env_token).await;
    assert!(status.status.success());
    let out = cli::stdout_str(&status);
    assert!(
        out.contains("identity: token"),
        "human status should show token identity: {out}"
    );
    assert!(
        out.contains(relay_url),
        "human status should show the token's relay: {out}"
    );
    assert!(
        !out.contains("kutl_journey_pat_123456"),
        "human status must not print the full token: {out}"
    );

    // JSON agrees, and marks the token as stored (not env-injected).
    let json = cli::json(
        &cli::kutl_in_env(
            home.path(),
            cwd.path(),
            &["auth", "status", "--format", "json"],
            &no_env_token,
        )
        .await,
    );
    let prefix = json["token_prefix"].as_str().expect("token_prefix");
    assert!(
        prefix.starts_with("kutl_") && prefix.len() < "kutl_journey_pat_123456".len(),
        "json token_prefix should be a truncation, got: {prefix}"
    );
    assert_eq!(json["relay_url"], serde_json::json!(relay_url));
    assert_eq!(json["from_env"], serde_json::json!(false));

    // Logout removes the file and says where from; status flips back to the
    // fresh-home line (no identity was ever provisioned here).
    let logout =
        cli::kutl_in_env(home.path(), cwd.path(), &["auth", "logout"], &no_env_token).await;
    assert!(
        logout.status.success(),
        "auth logout failed: {}",
        cli::stderr_str(&logout)
    );
    let out = cli::stdout_str(&logout);
    assert!(
        out.contains("Credentials removed from"),
        "logout should confirm the removal and name the file: {out}"
    );
    let after = cli::kutl_in_env(home.path(), cwd.path(), &["auth", "status"], &no_env_token).await;
    let out = cli::stdout_str(&after);
    assert!(
        out.contains("identity: none provisioned"),
        "post-logout human status should be back to none provisioned: {out}"
    );
}
