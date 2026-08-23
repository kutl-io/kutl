//! `kutl auth` journeys beyond the covered status-after-init case
//! (coverage-lane B12): the token store/logout arc with its status renders
//! (logged-out, file-token, env-token), and the full OAuth device flow driven
//! against the real relay — `kutl auth login` polls while the test plays the
//! approver over the relay's `POST /auth/device/authorize` endpoint, exactly
//! as the UX server does in production.

use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use kutl_cli_uxr::harness::RelayProcess;
use kutl_cli_uxr::harness::binaries::kutl_bin;
use kutl_cli_uxr::harness::cli::{self, TestHome};

/// Env var of the CLI token-resolution chain. Explicitly REMOVED on calls
/// asserting stored-file state so a developer shell can't leak a token in,
/// and explicitly SET for the env-precedence variant.
const TOKEN_ENV: &str = "KUTL_TOKEN";

/// A PAT-shaped token whose first 12 chars ("kutl_journey") are the prefix
/// `auth status` renders.
const STORED_TOKEN: &str = "kutl_journey_tok_123456";

/// An env-provided token whose first 12 chars are "kutl_env_tok".
const ENV_TOKEN: &str = "kutl_env_tok_abcdef";

/// Relay URL stored alongside the PAT (never dialed by these commands).
const STORED_RELAY_URL: &str = "ws://127.0.0.1:19999/ws";

/// Per-line timeout while driving `kutl auth login` (the device flow polls
/// every 5s; 30s absorbs slow CI without hanging the gate on a wedge).
const LOGIN_STEP_TIMEOUT: Duration = Duration::from_secs(30);

/// The token/logout arc and every `auth status` identity render it passes
/// through: logged-out → `auth token` (stored PAT) → file-token render →
/// env-token precedence render → `auth logout` (file really deleted) →
/// logged-out again, plus the idempotent second logout.
#[tokio::test]
async fn auth_token_status_logout_journey() {
    let home = TestHome::new();
    let cwd = tempfile::tempdir().unwrap();
    let no_env_token = [(TOKEN_ENV, None)];

    // Logged out, nothing provisioned: the human render is the exact
    // one-line summary, and the JSON render is null.
    let out = cli::kutl_in_env(home.path(), cwd.path(), &["auth", "status"], &no_env_token).await;
    assert!(out.status.success(), "{}", cli::stderr_str(&out));
    assert_eq!(cli::stdout_str(&out), "identity: none provisioned\n");

    // `auth token` stores the PAT and tells the user what to do next.
    let out = cli::kutl_in_env(
        home.path(),
        cwd.path(),
        &["auth", "token", STORED_TOKEN, "--relay", STORED_RELAY_URL],
        &no_env_token,
    )
    .await;
    assert!(
        out.status.success(),
        "auth token failed: {}",
        cli::stderr_str(&out)
    );
    assert_eq!(
        cli::stdout_str(&out),
        format!(
            "Token saved.\n  relay: {STORED_RELAY_URL}\nUse `kutl join <owner/space>` to connect a space.\n"
        )
    );

    // The credentials landed in $KUTL_HOME/auth.json, owner-only (0600 —
    // the bearer must never be world-readable).
    let creds_path = home.path().join("auth.json");
    let raw = std::fs::read_to_string(&creds_path).expect("auth.json written");
    let creds: serde_json::Value = serde_json::from_str(&raw).expect("auth.json is json");
    assert_eq!(creds["token"], serde_json::json!(STORED_TOKEN));
    assert_eq!(creds["relay_url"], serde_json::json!(STORED_RELAY_URL));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&creds_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "auth.json must be owner-only");
    }

    // JSON render with a file token and no local identity: prefix + relay,
    // did stays null (a token is not a DID identity).
    let out = cli::kutl_in_env(
        home.path(),
        cwd.path(),
        &["auth", "status", "--format", "json"],
        &no_env_token,
    )
    .await;
    assert!(out.status.success(), "{}", cli::stderr_str(&out));
    let v = cli::json(&out);
    assert_eq!(v["token_prefix"], serde_json::json!("kutl_journey"), "{v}");
    assert_eq!(v["relay_url"], serde_json::json!(STORED_RELAY_URL), "{v}");
    assert_eq!(v["from_env"], serde_json::json!(false), "{v}");
    assert!(v["did"].is_null(), "no identity.json → no did: {v}");

    // Human render, file-token variant: the truncated prefix + relay line.
    let out = cli::kutl_in_env(home.path(), cwd.path(), &["auth", "status"], &no_env_token).await;
    assert!(out.status.success(), "{}", cli::stderr_str(&out));
    assert_eq!(
        cli::stdout_str(&out),
        format!("identity: token kutl_journey… @ {STORED_RELAY_URL}\n")
    );

    // $KUTL_TOKEN trumps the (still-present) credentials file — mirroring the
    // resolve_token chain — and the render says where the token came from.
    let with_env_token = [(TOKEN_ENV, Some(ENV_TOKEN))];
    let out = cli::kutl_in_env(
        home.path(),
        cwd.path(),
        &["auth", "status"],
        &with_env_token,
    )
    .await;
    assert!(out.status.success(), "{}", cli::stderr_str(&out));
    assert_eq!(
        cli::stdout_str(&out),
        "identity: token from $KUTL_TOKEN (kutl_env_tok…)\n"
    );
    let out = cli::kutl_in_env(
        home.path(),
        cwd.path(),
        &["auth", "status", "--format", "json"],
        &with_env_token,
    )
    .await;
    let v = cli::json(&out);
    assert_eq!(v["from_env"], serde_json::json!(true), "{v}");
    assert!(v["relay_url"].is_null(), "env token implies no relay: {v}");

    // Logout deletes the credentials file and says which file it removed.
    let out = cli::kutl_in_env(home.path(), cwd.path(), &["auth", "logout"], &no_env_token).await;
    assert!(
        out.status.success(),
        "auth logout failed: {}",
        cli::stderr_str(&out)
    );
    let logout_out = cli::stdout_str(&out);
    assert!(
        logout_out.contains("Credentials removed from")
            && logout_out.contains(creds_path.to_str().unwrap()),
        "logout should name the removed file: {logout_out}"
    );
    assert!(!creds_path.exists(), "auth.json must be gone after logout");

    // The follow-up status shows logged-out again, in both formats.
    let out = cli::kutl_in_env(
        home.path(),
        cwd.path(),
        &["auth", "status", "--format", "json"],
        &no_env_token,
    )
    .await;
    assert!(out.status.success(), "{}", cli::stderr_str(&out));
    assert!(
        cli::json(&out).is_null(),
        "post-logout identity must be null: {}",
        cli::stdout_str(&out)
    );
    let out = cli::kutl_in_env(home.path(), cwd.path(), &["auth", "status"], &no_env_token).await;
    assert_eq!(cli::stdout_str(&out), "identity: none provisioned\n");

    // A second logout is an explicit no-op, not an error.
    let out = cli::kutl_in_env(home.path(), cwd.path(), &["auth", "logout"], &no_env_token).await;
    assert!(
        out.status.success(),
        "repeat logout must not fail: {}",
        cli::stderr_str(&out)
    );
    assert_eq!(cli::stdout_str(&out), "No stored credentials found.\n");
}

/// The OAuth device flow end to end against the real relay: `kutl auth login`
/// requests a device code, displays the verification URL (honoring the
/// relay's configured `KUTL_RELAY_UX_URL`) and user code, and polls; the test
/// approves over `POST /auth/device/authorize` — authenticated with a bearer
/// minted via the did:key challenge flow, exactly the UX server's role — and
/// the CLI lands the credentials and reports who it authenticated as.
#[tokio::test]
async fn auth_login_device_flow_journey() {
    // A scheme no OS handler is registered for: the CLI's browser auto-open
    // (`/usr/bin/open` on macOS — an absolute path, so it cannot be shadowed
    // via PATH) fails fast and falls back to its printed-URL path instead of
    // popping a browser tab on every gate run. This is a REAL relay config
    // knob (the UX server fronts the relay in production), and the journey
    // asserts the configured base is honored in the verification URL.
    let ux_url = "kutl-uxr-noop://device-approvals";
    let relay = RelayProcess::spawn_with_env(&[("KUTL_RELAY_UX_URL", ux_url)]).await;
    let home = TestHome::new();

    let mut child = Command::new(kutl_bin())
        .args(["auth", "login", "--relay", &relay.ws_url()])
        .env("KUTL_HOME", home.path())
        .env("KUTL_LOG", "warn")
        .env_remove(TOKEN_ENV)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn kutl auth login");
    let stdout = child.stdout.take().expect("login stdout");
    let mut lines = BufReader::new(stdout).lines();

    // Read the instructions up to the user code. Rust line-buffers stdout, so
    // the lines arrive as they are printed.
    let mut seen = Vec::new();
    let user_code = loop {
        let line = tokio::time::timeout(LOGIN_STEP_TIMEOUT, lines.next_line())
            .await
            .expect("timed out waiting for the device-flow user code")
            .expect("read login stdout")
            .expect("login exited before printing a user code");
        seen.push(line.clone());
        if let Some(code) = line.strip_prefix("Enter code: ") {
            break code.trim().to_owned();
        }
    };
    assert!(
        seen.iter().any(|l| l == "Open this URL in your browser:"),
        "login must tell the user to open the verification URL: {seen:?}"
    );
    assert!(
        seen.iter().any(|l| l.trim() == format!("{ux_url}/device")),
        "the verification URL must be the relay's configured UX base + /device: {seen:?}"
    );
    assert_eq!(
        user_code.len(),
        9,
        "user code renders as XXXX-XXXX: {user_code:?}"
    );
    assert_eq!(
        user_code.as_bytes()[4],
        b'-',
        "user code renders as XXXX-XXXX: {user_code:?}"
    );

    let http = reqwest::Client::new();
    let authorize_url = format!("{}/auth/device/authorize", relay.http_base());
    let approve_body = serde_json::json!({
        "user_code": user_code,
        "token": "kutl_uxr_device_tok_1",
        "account_id": "acct-uxr-1",
        "display_name": "Journey User",
    });

    // The authorize endpoint is bearer-guarded: an unauthenticated approval
    // attempt is a clean 401.
    let resp = http
        .post(&authorize_url)
        .json(&approve_body)
        .send()
        .await
        .expect("POST authorize (no bearer)");
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = resp.json().await.expect("401 body json");
    assert_eq!(
        body["error"],
        serde_json::json!("missing authorization header"),
        "{body}"
    );

    // Play the approver: mint a bearer via the did:key challenge flow (the
    // approver needs a valid identity but NO allowlist entry — the allowlist
    // gates spaces, not identity).
    let (_approver_did, approver_bearer) = relay.mint_bearer().await;

    // A wrong user code is a clean 404, and burns nothing.
    let resp = http
        .post(&authorize_url)
        .bearer_auth(&approver_bearer)
        .json(&serde_json::json!({
            "user_code": "ZZZZ-ZZZZ",
            "token": "x", "account_id": "x", "display_name": "x",
        }))
        .send()
        .await
        .expect("POST authorize (wrong code)");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await.expect("404 body json");
    assert_eq!(
        body["error"],
        serde_json::json!("invalid user code"),
        "{body}"
    );

    // The real approval.
    let resp = http
        .post(&authorize_url)
        .bearer_auth(&approver_bearer)
        .json(&approve_body)
        .send()
        .await
        .expect("POST authorize");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "authorize should succeed"
    );

    // The polling CLI picks the token up (5s poll interval), saves the
    // credentials, and reports the authenticated identity.
    let mut tail = Vec::new();
    loop {
        let line = tokio::time::timeout(LOGIN_STEP_TIMEOUT, lines.next_line())
            .await
            .expect("timed out waiting for login to complete")
            .expect("read login stdout");
        match line {
            Some(l) => tail.push(l),
            None => break, // EOF — the CLI exited.
        }
    }
    let status = tokio::time::timeout(LOGIN_STEP_TIMEOUT, child.wait())
        .await
        .expect("timed out waiting for login exit")
        .expect("wait for login");
    assert!(status.success(), "login should exit 0: {seen:?} {tail:?}");
    assert!(
        seen.iter()
            .chain(&tail)
            .any(|l| l == "Waiting for authorization..."),
        "login should say it is polling: {seen:?} {tail:?}"
    );
    assert!(
        tail.iter().any(|l| l == "Authenticated as Journey User."),
        "login must report the authenticated display name: {tail:?}"
    );
    assert!(
        tail.iter().any(|l| l.contains("credentials saved to")),
        "login must say where credentials landed: {tail:?}"
    );

    // The stored credentials carry the approved token/account, with the relay
    // URL normalized back to ws-scheme so the daemon can reuse the token.
    let raw = std::fs::read_to_string(home.path().join("auth.json")).expect("auth.json written");
    let creds: serde_json::Value = serde_json::from_str(&raw).expect("auth.json is json");
    assert_eq!(creds["token"], serde_json::json!("kutl_uxr_device_tok_1"));
    assert_eq!(creds["account_id"], serde_json::json!("acct-uxr-1"));
    assert_eq!(creds["display_name"], serde_json::json!("Journey User"));
    assert_eq!(creds["relay_url"], serde_json::json!(relay.ws_url()));

    // The device-flow identity renders with account + relay in BOTH formats.
    let cwd = tempfile::tempdir().unwrap();
    let out = cli::kutl_in_env(
        home.path(),
        cwd.path(),
        &["auth", "status"],
        &[(TOKEN_ENV, None)],
    )
    .await;
    assert!(out.status.success(), "{}", cli::stderr_str(&out));
    assert_eq!(
        cli::stdout_str(&out),
        format!(
            "identity: Journey User (acct-uxr-1) @ {}\n  name:  Journey User\n",
            relay.ws_url()
        )
    );
    let out = cli::kutl_in_env(
        home.path(),
        cwd.path(),
        &["auth", "status", "--format", "json"],
        &[(TOKEN_ENV, None)],
    )
    .await;
    let v = cli::json(&out);
    assert_eq!(v["token_prefix"], serde_json::json!("kutl_uxr_dev"), "{v}");
    assert_eq!(v["account_id"], serde_json::json!("acct-uxr-1"), "{v}");
    assert_eq!(v["display_name"], serde_json::json!("Journey User"), "{v}");
    assert_eq!(v["relay_url"], serde_json::json!(relay.ws_url()), "{v}");
    assert_eq!(v["from_env"], serde_json::json!(false), "{v}");
}
