use kutl_cli_uxr::harness::cli::{self, TestHome};

/// A port assumed unbound, forcing `kutl init` into local-only mode.
const UNREACHABLE_RELAY_PORT: u16 = 19999;

/// Verify that the three JSON-capable read commands degrade gracefully with no
/// identity provisioned: they must exit 0 and emit valid JSON representing the
/// empty/unauthenticated/no-daemon state.
#[tokio::test]
async fn status_json_degrades_gracefully_without_identity() {
    let home = TestHome::new();
    let cwd = tempfile::tempdir().unwrap();

    for args in [
        ["auth", "status", "--format", "json"].as_slice(),
        ["daemon", "status", "--format", "json"].as_slice(),
        ["status", "--format", "json"].as_slice(),
    ] {
        let out = cli::kutl_in(home.path(), cwd.path(), args).await;
        assert!(
            out.status.success(),
            "{args:?} should exit 0 with no identity: {}",
            cli::stderr_str(&out)
        );
        let v = cli::json(&out);
        // Sanity-check the shape of each sub-struct. An empty/no-identity state
        // is represented as JSON null for `auth status` (just the identity field)
        // and as objects with appropriate defaults for the others.
        match args[0] {
            "auth" => {
                // `auth status --format json` emits `snapshot.identity`, which is
                // `null` when no identity file is present.
                assert!(
                    v.is_null() || v.is_object(),
                    "auth status json should be null or an object: {v}"
                );
            }
            "daemon" => {
                // `daemon status --format json` emits `{ daemon, desktop }`.
                assert!(
                    v.get("daemon").is_some(),
                    "daemon status json must have 'daemon' key: {v}"
                );
            }
            "status" => {
                // Aggregate status must have a `spaces` array (empty) and a
                // `daemon` sub-struct.
                let spaces = v["spaces"]
                    .as_array()
                    .expect("status json has 'spaces' array");
                assert!(
                    spaces.is_empty(),
                    "no spaces registered yet, 'spaces' should be empty: {v}"
                );
                assert!(
                    v.get("daemon").is_some(),
                    "aggregate status json must have 'daemon' key: {v}"
                );
            }
            _ => unreachable!("unexpected command in status-degrade loop: {}", args[0]),
        }
    }
}

/// Config list/set/get round-trip and JSON status after `kutl init` (local-only,
/// no relay). Confirms that:
///   - `config list` reports "no config values set" before any `set`
///   - `config set name <value>` then `config get name` round-trips the value
///   - `auth status --format json` surfaces the identity after init
///   - `status --format json` shows one registered space
#[tokio::test]
async fn config_and_status_journey() {
    let home = TestHome::new();
    let space = home.space_dir();

    // `kutl init` with an unreachable relay falls back to local-only mode
    // (emits a warning on stderr and exits 0). This provisions the DID identity
    // file that `config list/set/get` require.
    let relay_url = format!("ws://127.0.0.1:{UNREACHABLE_RELAY_PORT}");
    let init = cli::kutl_in(
        home.path(),
        space.path(),
        &[
            "init",
            "--relay",
            &relay_url,
            "--dir",
            space.path().to_str().unwrap(),
            "--name",
            "test-config-space",
            "--subfolder",
            "kutl",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init (local-only) failed: {}",
        cli::stderr_str(&init)
    );
    let init_out = cli::stdout_str(&init);
    assert!(
        init_out.contains("Initialized kutl space"),
        "unexpected init output: {init_out}"
    );
    // Local-only init should note it is not registered.
    assert!(
        init_out.contains("registered: no"),
        "expected 'registered: no' in local-only init output: {init_out}"
    );

    // --- config list: expect "no config values set" before any set ---
    let list0 = cli::kutl_in(home.path(), space.path(), &["config", "list"]).await;
    assert!(
        list0.status.success(),
        "config list failed: {}",
        cli::stderr_str(&list0)
    );
    let list0_out = cli::stdout_str(&list0);
    assert!(
        list0_out.contains("no config values set"),
        "expected 'no config values set' before any config set: {list0_out}"
    );

    // --- config set name → config get name round-trip ---
    // `name` and `email` are the two user-settable keys (see `CONFIG_KEYS` in
    // main.rs). We use `name` here as the safe, idempotent choice.
    let set = cli::kutl_in(
        home.path(),
        space.path(),
        &["config", "set", "name", "Alice Test"],
    )
    .await;
    assert!(
        set.status.success(),
        "config set name failed: {}",
        cli::stderr_str(&set)
    );
    let set_out = cli::stdout_str(&set);
    assert!(
        set_out.contains("Set name = Alice Test"),
        "unexpected config set output: {set_out}"
    );

    let get = cli::kutl_in(home.path(), space.path(), &["config", "get", "name"]).await;
    assert!(
        get.status.success(),
        "config get name failed: {}",
        cli::stderr_str(&get)
    );
    let get_out = cli::stdout_str(&get);
    assert!(
        get_out.trim() == "Alice Test",
        "config get name round-trip failed: got {get_out:?}, expected \"Alice Test\""
    );

    // --- config list: now shows the set key ---
    let list1 = cli::kutl_in(home.path(), space.path(), &["config", "list"]).await;
    assert!(
        list1.status.success(),
        "config list (after set) failed: {}",
        cli::stderr_str(&list1)
    );
    let list1_out = cli::stdout_str(&list1);
    assert!(
        list1_out.contains("name = Alice Test"),
        "config list should show the set name: {list1_out}"
    );

    // --- auth status --format json: identity is now provisioned ---
    let auth = cli::kutl_in(
        home.path(),
        space.path(),
        &["auth", "status", "--format", "json"],
    )
    .await;
    assert!(
        auth.status.success(),
        "auth status --format json failed: {}",
        cli::stderr_str(&auth)
    );
    let auth_v = cli::json(&auth);
    // After init, identity should be an object (not null) with a `did` field.
    // Note: `auth status --format json` emits the identity object *directly*, so
    // `v["did"]` is correct here — unlike the aggregate `status --format json`
    // (and `journey::human_did_of`), where the did is nested under
    // `v["identity"]["did"]`. Do not "normalize" one to the other.
    let did = auth_v["did"]
        .as_str()
        .expect("auth status json should have a 'did' field after init");
    assert!(
        did.starts_with("did:key:"),
        "did should start with 'did:key:': {did}"
    );
    // The display name set via `config set name` should surface here.
    let display_name = auth_v["display_name"]
        .as_str()
        .expect("auth status json should have 'display_name' after config set");
    assert_eq!(
        display_name, "Alice Test",
        "auth status display_name should reflect config set"
    );

    // Human render (default format): the load-bearing `kutl auth status` lines —
    // the summary (local identity, no relay token in local-only mode) and the
    // indented DID/name fields.
    let auth_h = cli::kutl_in(home.path(), space.path(), &["auth", "status"]).await;
    assert!(
        auth_h.status.success(),
        "auth status (human) failed: {}",
        cli::stderr_str(&auth_h)
    );
    let auth_out = cli::stdout_str(&auth_h);
    assert!(
        auth_out.contains("identity: local (no relay token yet)"),
        "human auth status should summarise the local-only identity: {auth_out}"
    );
    assert!(
        auth_out.contains("did:   did:key:"),
        "human auth status should show the DID: {auth_out}"
    );
    assert!(
        auth_out.contains("name:  Alice Test"),
        "human auth status should show the configured name: {auth_out}"
    );

    // --- daemon status --format json ---
    let daemon = cli::kutl_in(
        home.path(),
        space.path(),
        &["daemon", "status", "--format", "json"],
    )
    .await;
    assert!(
        daemon.status.success(),
        "daemon status --format json failed: {}",
        cli::stderr_str(&daemon)
    );
    let daemon_v = cli::json(&daemon);
    assert!(
        daemon_v.get("daemon").is_some(),
        "daemon status json must have 'daemon' key: {daemon_v}"
    );
    // No daemon started → running should be false.
    assert_eq!(
        daemon_v["daemon"]["running"].as_bool(),
        Some(false),
        "daemon should not be running: {daemon_v}"
    );

    // --- status --format json: one registered space ---
    let status = cli::kutl_in(home.path(), space.path(), &["status", "--format", "json"]).await;
    assert!(
        status.status.success(),
        "status --format json failed: {}",
        cli::stderr_str(&status)
    );
    let status_v = cli::json(&status);
    let spaces = status_v["spaces"]
        .as_array()
        .expect("status json must have 'spaces' array");
    assert_eq!(
        spaces.len(),
        1,
        "expected exactly one registered space after local-only init: {status_v}"
    );
}
