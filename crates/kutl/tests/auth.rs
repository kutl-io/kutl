//! CLI auth command integration tests.
//!
//! Tests credential storage and CLI output via subprocess invocation.
//! No relay or Postgres required — runs unconditionally in CI.

use std::process::Command;

use serial_test::serial;

/// Build a `Command` for the kutl binary.
fn kutl_cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kutl"))
}

/// Store a PAT via `kutl auth login --token` in a temp HOME directory.
/// Returns the tempdir (must stay alive for the duration of the test).
fn login_with_pat(home: &std::path::Path) -> std::process::Output {
    kutl_cli()
        .args([
            "auth",
            "login",
            "--token",
            "kutl_test_abc123",
            "--relay",
            "ws://bogus:9999/ws",
        ])
        .env("HOME", home)
        .output()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `kutl auth login --token` should save credentials to `~/.kutl/auth.json`.
#[test]
#[serial]
fn test_auth_login_with_token() {
    let dir = tempfile::tempdir().unwrap();
    let output = login_with_pat(dir.path());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "login failed: stdout={stdout}, stderr={stderr}"
    );

    let creds_path = dir.path().join(".kutl/auth.json");
    assert!(creds_path.exists(), "credentials file should be created");

    let contents = std::fs::read_to_string(&creds_path).unwrap();
    assert!(
        contents.contains("kutl_test_abc123"),
        "credentials should contain the token"
    );
    assert!(
        contents.contains("ws://bogus:9999/ws"),
        "credentials should contain the relay URL"
    );
}

/// `kutl auth status` after login should show the relay URL.
#[test]
#[serial]
fn test_auth_status_shows_token() {
    let dir = tempfile::tempdir().unwrap();
    let login_output = login_with_pat(dir.path());
    assert!(login_output.status.success(), "login should succeed first");

    let output = kutl_cli()
        .args(["auth", "status"])
        .env("HOME", dir.path())
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("ws://bogus:9999/ws"),
        "status should show relay URL, got: {stdout}"
    );
    assert!(
        stdout.contains("kutl_test_a"),
        "status should show token prefix, got: {stdout}"
    );
}

/// `kutl auth logout` should remove the credentials file.
#[test]
#[serial]
fn test_auth_logout_removes_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let login_output = login_with_pat(dir.path());
    assert!(login_output.status.success(), "login should succeed first");

    let creds_path = dir.path().join(".kutl/auth.json");
    assert!(
        creds_path.exists(),
        "credentials should exist before logout"
    );

    let output = kutl_cli()
        .args(["auth", "logout"])
        .env("HOME", dir.path())
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "logout failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !creds_path.exists(),
        "credentials file should be deleted after logout"
    );
    assert!(
        stdout.contains("removed") || stdout.contains("Credentials"),
        "logout should confirm removal, got: {stdout}"
    );
}

/// `kutl auth status` with no credentials should report not authenticated.
#[test]
#[serial]
fn test_auth_status_not_authenticated() {
    let dir = tempfile::tempdir().unwrap();

    let output = kutl_cli()
        .args(["auth", "status"])
        .env("HOME", dir.path())
        // Clear KUTL_TOKEN to ensure we don't pick up an env token.
        .env_remove("KUTL_TOKEN")
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stdout_lower = stdout.to_lowercase();
    assert!(
        stdout_lower.contains("not authenticated"),
        "status should indicate not authenticated, got: {stdout}"
    );
}
