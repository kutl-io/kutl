use kutl_cli_uxr::harness::RelayProcess;
use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::human_did_of;

/// A new user boots a relay, inits a space against it, and inspects state.
#[tokio::test]
async fn space_bootstrap_journey() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space = home.space_dir();

    // `kutl init` registers the space on the relay (not local-only).
    let init = cli::kutl_in(
        home.path(),
        space.path(),
        &[
            "init",
            "--relay",
            &relay.ws_url(),
            "--dir",
            space.path().to_str().unwrap(),
            "--name",
            "acme",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );
    let out = cli::stdout_str(&init);
    assert!(
        out.contains("Initialized kutl space"),
        "unexpected init output: {out}"
    );
    assert!(
        out.contains("registered: yes"),
        "space should register on a reachable relay: {out}"
    );
    assert!(
        out.contains("Next steps:"),
        "init should print next-step guidance so a new user knows how to sync: {out}"
    );

    // `kutl status --format json` shows exactly one space, pointing at our relay.
    let status = cli::kutl_in(home.path(), space.path(), &["status", "--format", "json"]).await;
    assert!(
        status.status.success(),
        "status failed: {}",
        cli::stderr_str(&status)
    );
    let v = cli::json(&status);
    let spaces = v["spaces"]
        .as_array()
        .expect("status json has a spaces array");
    assert_eq!(
        spaces.len(),
        1,
        "expected exactly one registered space: {v}"
    );

    // `kutl space list --format json` also surfaces it.
    let list = cli::kutl_in(
        home.path(),
        space.path(),
        &["space", "list", "--format", "json"],
    )
    .await;
    assert!(
        list.status.success(),
        "space list failed: {}",
        cli::stderr_str(&list)
    );
    let listed = cli::json(&list);
    assert!(
        !listed.as_array().map(|a| a.is_empty()).unwrap_or(true),
        "space list should be non-empty: {listed}"
    );

    // Human render (default format): the load-bearing `kutl space list` lines —
    // the count header and the per-space line with health mark, name, and relay.
    let list_h = cli::kutl_in(home.path(), space.path(), &["space", "list"]).await;
    assert!(
        list_h.status.success(),
        "space list (human) failed: {}",
        cli::stderr_str(&list_h)
    );
    let list_out = cli::stdout_str(&list_h);
    assert!(
        list_out.contains("spaces (1 registered):"),
        "human space list should carry the count header: {list_out}"
    );
    assert!(
        list_out.contains("✓ acme"),
        "human space list should mark the healthy space by name: {list_out}"
    );
    assert!(
        list_out.contains(&format!("relay={}", relay.ws_url())),
        "human space list should show the relay URL: {list_out}"
    );

    // `kutl space status` (human) adds the relays section with reachability.
    let status_h = cli::kutl_in(home.path(), space.path(), &["space", "status"]).await;
    assert!(
        status_h.status.success(),
        "space status (human) failed: {}",
        cli::stderr_str(&status_h)
    );
    let status_out = cli::stdout_str(&status_h);
    assert!(
        status_out.contains("relays:"),
        "human space status should carry the relays section: {status_out}"
    );
    assert!(
        status_out.contains(&format!("{}   reachable", relay.ws_url())),
        "human space status should show the relay as reachable: {status_out}"
    );
}

/// `kutl space status` is cwd-scoped: it reports the space you are standing
/// in. Outside any space it errors with a pointer at the aggregate `kutl
/// status` instead of dumping the whole registry. Pins the cwd-first contract
/// — a render of every registered space would pass the in-space assertions
/// above without this discriminating case.
#[tokio::test]
async fn space_status_outside_a_space_points_at_kutl_status() {
    let home = TestHome::new();
    let outside = tempfile::tempdir().unwrap();

    let out = cli::kutl_in(home.path(), outside.path(), &["space", "status"]).await;
    assert!(
        !out.status.success(),
        "space status outside a space must fail, got: {}",
        cli::stdout_str(&out)
    );
    let err = cli::stderr_str(&out);
    assert!(
        err.contains("not inside a kutl space"),
        "error should say the cwd is not in a space: {err}"
    );
    assert!(
        err.contains("kutl status"),
        "error should point at the aggregate `kutl status`: {err}"
    );
}

/// A set `$KUTL_HOME` is a hard boundary: a real, registered space OUTSIDE
/// it must not resolve — fail closed. This is the agent-safety guard: a
/// process that declares its workspace via `KUTL_HOME` can never bind to
/// (and then sync or author into) whatever space its cwd happens to sit
/// inside while roaming beyond that boundary.
#[tokio::test]
async fn space_outside_kutl_home_fails_closed() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    // The space deliberately lives OUTSIDE the declared home.
    let space = tempfile::tempdir().unwrap();

    // `init` places explicitly (no resolution walk), so creating the space
    // out there works — the guard is on RESOLUTION, not placement.
    let init = cli::kutl_in(
        home.path(),
        space.path(),
        &[
            "init",
            "--relay",
            &relay.ws_url(),
            "--dir",
            space.path().to_str().unwrap(),
            "--name",
            "beyond",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );

    // Space-scoped verbs run from inside that space must refuse: the
    // declared boundary wins over the cwd.
    let status = cli::kutl_in(home.path(), space.path(), &["space", "status"]).await;
    assert!(
        !status.status.success(),
        "space status outside the KUTL_HOME boundary must fail closed: {}",
        cli::stdout_str(&status)
    );
    assert!(
        cli::stderr_str(&status).contains("not inside a kutl space"),
        "refusal should read as not-inside-a-space: {}",
        cli::stderr_str(&status)
    );

    let sync = cli::kutl_in(home.path(), space.path(), &["sync"]).await;
    assert!(
        !sync.status.success(),
        "sync outside the KUTL_HOME boundary must fail closed: {}",
        cli::stdout_str(&sync)
    );
}

/// Space-scoped verbs work from any SUBDIRECTORY of the space, not just its
/// root — the walk-up mirrors git. `space status` from a nested dir resolves
/// the enclosing space.
#[tokio::test]
async fn space_status_resolves_from_a_subdirectory() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space = home.space_dir();

    let init = cli::kutl_in(
        home.path(),
        space.path(),
        &[
            "init",
            "--relay",
            &relay.ws_url(),
            "--dir",
            space.path().to_str().unwrap(),
            "--name",
            "nested-acme",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );

    let subdir = space.path().join("docs").join("notes");
    std::fs::create_dir_all(&subdir).unwrap();
    let status = cli::kutl_in(home.path(), &subdir, &["space", "status"]).await;
    assert!(
        status.status.success(),
        "space status from a subdir failed: {}",
        cli::stderr_str(&status)
    );
    let out = cli::stdout_str(&status);
    assert!(
        out.contains("nested-acme"),
        "focused status should resolve the enclosing space: {out}"
    );
}

/// The `space leave` arc: leaving forgets the space on this client (registry
/// only) while keeping the working tree and `.kutl/` state; a second leave
/// reports nothing to forget; `kutl join` in the folder re-attaches it.
#[tokio::test]
async fn space_leave_forgets_locally_and_join_reattaches() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space = home.space_dir();

    let init = cli::kutl_in(
        home.path(),
        space.path(),
        &[
            "init",
            "--relay",
            &relay.ws_url(),
            "--dir",
            space.path().to_str().unwrap(),
            "--name",
            "leavable",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );

    // Leave: forgotten on this client, tree kept, membership kept.
    let leave = cli::kutl_in(home.path(), space.path(), &["space", "leave"]).await;
    assert!(
        leave.status.success(),
        "leave failed: {}",
        cli::stderr_str(&leave)
    );
    let out = cli::stdout_str(&leave);
    assert!(
        out.contains("forgotten on this client"),
        "leave should say what happened: {out}"
    );
    assert!(
        out.contains("kept:"),
        "leave should say what was kept: {out}"
    );
    assert!(
        space.path().join(".kutl").join("space.json").exists(),
        "the working tree and .kutl/ state must remain on disk"
    );
    let list = cli::kutl_in(
        home.path(),
        space.path(),
        &["space", "list", "--format", "json"],
    )
    .await;
    let spaces = cli::json(&list);
    assert_eq!(
        spaces.as_array().map(Vec::len),
        Some(0),
        "registry should hold no spaces after leave: {spaces}"
    );

    // A second leave is a clear no-op, not an error.
    let again = cli::kutl_in(home.path(), space.path(), &["space", "leave"]).await;
    assert!(
        again.status.success(),
        "second leave should succeed: {}",
        cli::stderr_str(&again)
    );
    assert!(
        cli::stdout_str(&again).contains("nothing to forget"),
        "second leave should say there is nothing to forget: {}",
        cli::stdout_str(&again)
    );

    // `kutl init` in the folder re-attaches: the on-disk config already
    // names the space, so init's already-initialized branch just
    // re-registers it on this client — no relay round-trip, no auth
    // dependency. (`kutl join` re-attaches too, but its bare-name resolve
    // still demands a stored bearer — the known join-gate wart, tracked
    // separately.)
    let rejoin = cli::kutl_in(
        home.path(),
        space.path(),
        &["init", "--relay", &relay.ws_url()],
    )
    .await;
    assert!(
        rejoin.status.success(),
        "re-attach via init failed: {}",
        cli::stderr_str(&rejoin)
    );
    assert!(
        cli::stdout_str(&rejoin).contains("already initialized"),
        "re-attach should take the already-initialized branch: {}",
        cli::stdout_str(&rejoin)
    );
    let list = cli::kutl_in(
        home.path(),
        space.path(),
        &["space", "list", "--format", "json"],
    )
    .await;
    let spaces = cli::json(&list);
    assert_eq!(
        spaces.as_array().map(Vec::len),
        Some(1),
        "registry should hold the re-joined space: {spaces}"
    );
}

/// `kutl sync` is an interactive one-shot: its stdout carries only the progress
/// lines the user cares about, never the JSON diagnostic stream — those now go
/// to stderr, quiet by default. Guards the log-noise fix so a future refactor
/// can't route library `tracing` back onto the command's stdout.
#[tokio::test]
async fn sync_stdout_is_clean() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

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
            "acme",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );

    // Authorize the human identity so the one-shot sync can push.
    let status = cli::kutl_in(home.path(), space, &["status", "--format", "json"]).await;
    let human_did = human_did_of(&status);
    relay.authorize_did(&human_did);

    // Give sync real work: a document to push.
    std::fs::write(space.join("note.md"), "hello from sync\n").unwrap();

    let sync = cli::kutl_in(home.path(), space, &["sync"]).await;
    assert!(
        sync.status.success(),
        "sync failed: {}",
        cli::stderr_str(&sync)
    );

    let out = cli::stdout_str(&sync);
    assert!(
        out.contains("Sync complete"),
        "sync stdout should confirm completion: {out}"
    );
    // The JSON diagnostic stream must not leak onto stdout.
    for noise in ["tracing initialized", "\"level\":", "\"timestamp\":"] {
        assert!(
            !out.contains(noise),
            "sync stdout must stay clean of JSON logs (found {noise:?}): {out}"
        );
    }
}

/// BUG 6a: `kutl init --relay <garbage>` must be rejected up front — it must
/// exit non-zero, name the bad URL, and leave no `.kutl/space.json` behind.
#[tokio::test]
async fn init_rejects_malformed_relay_url() {
    let home = TestHome::new();
    let space = home.space_dir();

    let init = cli::kutl_in(
        home.path(),
        space.path(),
        &[
            "init",
            "--relay",
            "not-a-url",
            "--dir",
            space.path().to_str().unwrap(),
            "--name",
            "x",
        ],
    )
    .await;

    assert!(
        !init.status.success(),
        "init must fail on a malformed relay URL: {}",
        cli::stdout_str(&init)
    );
    let err = cli::stderr_str(&init);
    assert!(
        err.contains("not-a-url"),
        "error must name the offending relay URL: {err}"
    );
    assert!(
        !space.path().join(".kutl").join("space.json").exists(),
        "no space.json should be written when the relay URL is rejected"
    );
}

/// BUG 6b: `kutl status` must report a space whose relay is unreachable as
/// unhealthy. While the relay is up the space is `healthy: true`; after the
/// relay dies (its port frees), status must flip that space to `healthy:
/// false`.
#[tokio::test]
async fn status_flips_space_unhealthy_when_relay_dies() {
    let relay = RelayProcess::spawn().await;
    let relay_url = relay.ws_url();
    let home = TestHome::new();
    let space = home.space_dir();

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
            "acme",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );

    // Helper: fetch this space's `healthy` flag from `kutl status --format json`.
    let space_healthy = || async {
        let status = cli::kutl_in(home.path(), space.path(), &["status", "--format", "json"]).await;
        assert!(
            status.status.success(),
            "status failed: {}",
            cli::stderr_str(&status)
        );
        let v = cli::json(&status);
        let spaces = v["spaces"].as_array().expect("status json has spaces");
        let s = spaces
            .iter()
            .find(|s| s["relay_url"] == serde_json::json!(relay_url))
            .expect("our space is present in status");
        (
            s["healthy"].as_bool().expect("healthy is a bool"),
            s["unhealthy_reason"].as_str().map(str::to_owned),
        )
    };

    // While the relay is up, the space is healthy.
    let (healthy, _) = space_healthy().await;
    assert!(healthy, "space should be healthy while the relay is up");

    // Kill the relay (kill_on_drop frees the port), then poll until status
    // reflects the unreachable relay. The probe can race the port close, so we
    // retry the assertion on a short bounded loop rather than sleeping blindly.
    drop(relay);

    const MAX_ATTEMPTS: u32 = 25;
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);
    let mut flipped_reason = None;
    for _ in 0..MAX_ATTEMPTS {
        let (healthy, reason) = space_healthy().await;
        if !healthy {
            flipped_reason = Some(reason);
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    let reason =
        flipped_reason.expect("space must flip to unhealthy once its relay is unreachable");
    if let Some(reason) = reason {
        assert!(
            reason.contains("relay"),
            "unhealthy reason should mention the relay: {reason}"
        );
    }
}
