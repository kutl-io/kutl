use kutl_cli_uxr::harness::RelayProcess;
use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::{
    init_and_authorize, init_space, signal_list, space_id_of, status_of,
};
use kutl_cli_uxr::harness::mcp::McpSession;

/// A wall-clock floor well after 1970 (2020-09-13) and before now — pins that
/// timestamps are real, not epoch-zero.
const RECENT_MS_FLOOR: i64 = 1_600_000_000_000;

/// An empty `signal list` (no `--fetch`) reads only the local mirror, so an
/// empty result may just mean the user hasn't pulled — the CLI must point them
/// at `--fetch` rather than leave them at a bare "no signals".
#[tokio::test]
async fn signal_list_empty_hints_fetch() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    init_space(&relay, &home, space, "acme").await;

    // No signals created and no --fetch: the local mirror is empty.
    let list = cli::kutl_in(home.path(), space, &["signal", "list"]).await;
    assert!(
        list.status.success(),
        "signal list failed: {}",
        cli::stderr_str(&list)
    );
    let out = cli::stdout_str(&list);
    assert!(out.contains("no signals"), "expected an empty list: {out}");
    assert!(
        out.contains("--fetch"),
        "an empty list should hint at --fetch: {out}"
    );
}

/// Full lifecycle: an agent raises a flag via MCP, a human triages it via the
/// CLI (close/reopen/resolve), each step verified against authoritative relay
/// state via `--fetch`. Exercises relay-mint authoring end-to-end (the
/// relay authors + attests + binds `author_did` to the authenticated caller) on
/// an auth-required relay, and pins real (non-epoch-zero) timestamps.
#[tokio::test]
async fn signal_lifecycle_journey() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    // Provision + authorize the agent.
    let create = cli::kutl_in(home.path(), space, &["agent", "create", "--name", "bot"]).await;
    assert!(
        create.status.success(),
        "agent create failed: {}",
        cli::stderr_str(&create)
    );
    let agent_did = cli::stdout_str(&create)
        .lines()
        .next()
        .expect("agent create prints the did")
        .trim()
        .to_owned();
    relay.authorize_did(&agent_did);

    // Init the space and authorize the human identity.
    init_and_authorize(&relay, &home, space, "acme").await;
    let space_id = space_id_of(
        &cli::kutl_in(home.path(), space, &["space", "list", "--format", "json"]).await,
    );

    // The agent raises a flag (relay-mint via MCP, challenge-authenticated).
    let mut mcp = McpSession::start(home.path(), space, "bot").await;
    mcp.initialize().await;
    let created = mcp
        .call_tool(
            "create_flag",
            serde_json::json!({
                "space_id": space_id,
                "document_id": uuid::Uuid::new_v4().to_string(),
                "kind": "blocked",
                "message": "the build is red",
                "audience": "space",
            }),
        )
        .await;
    assert!(
        created.get("error").is_none(),
        "create_flag JSON-RPC error: {created}"
    );
    assert!(
        created["result"]["isError"] != serde_json::json!(true),
        "create_flag tool error: {created}"
    );
    drop(mcp); // release the mcp serve connection

    // The human reads authoritative relay state: one open flag, REAL timestamp.
    let list = signal_list(&home, space, false).await;
    assert_eq!(list.len(), 1, "expected exactly one open signal: {list:?}");
    assert_eq!(
        list[0]["status"], "open",
        "new flag should be open: {:?}",
        list[0]
    );
    let created_ms = list[0]["created_ms"]
        .as_i64()
        .expect("created_ms is an integer");
    assert!(
        created_ms >= RECENT_MS_FLOOR,
        "created_ms {created_ms} looks like 1970 (epoch-zero regression)"
    );
    let id = list[0]["id"].as_str().expect("signal id").to_owned();

    // Human closes it (CLI, relay-mint transition, challenge-authenticated) —
    // by an 8-char id PREFIX, git-style: the CLI resolves it against the
    // local fold and the transition must land on the full id.
    let id_prefix = &id[..8];
    let close = cli::kutl_in(home.path(), space, &["signal", "close", id_prefix]).await;
    assert!(
        close.status.success(),
        "close by prefix failed: {}",
        cli::stderr_str(&close)
    );
    assert_eq!(
        status_of(&signal_list(&home, space, true).await, &id),
        "closed"
    );

    // Human reopens it.
    let reopen = cli::kutl_in(home.path(), space, &["signal", "reopen", &id]).await;
    assert!(
        reopen.status.success(),
        "reopen failed: {}",
        cli::stderr_str(&reopen)
    );
    assert_eq!(
        status_of(&signal_list(&home, space, false).await, &id),
        "open"
    );

    // Human resolves it (sugar for close --reason resolved).
    let resolve = cli::kutl_in(home.path(), space, &["signal", "resolve", &id]).await;
    assert!(
        resolve.status.success(),
        "resolve failed: {}",
        cli::stderr_str(&resolve)
    );
    assert_eq!(
        status_of(&signal_list(&home, space, true).await, &id),
        "closed"
    );

    // The transition history reads back the full arc.
    let view = cli::kutl_in(
        home.path(),
        space,
        &["signal", "view", &id, "--fetch", "--format", "json"],
    )
    .await;
    assert!(
        view.status.success(),
        "view failed: {}",
        cli::stderr_str(&view)
    );
    let v = cli::json(&view).to_string();
    for event in ["created", "closed", "reopened"] {
        assert!(
            v.contains(event),
            "view history should include the {event} transition: {v}"
        );
    }

    // Human render (default format): the load-bearing `signal view` lines —
    // the header, the final status, and the history arc with real timestamps.
    let view_h = cli::kutl_in(home.path(), space, &["signal", "view", &id, "--fetch"]).await;
    assert!(
        view_h.status.success(),
        "view (human) failed: {}",
        cli::stderr_str(&view_h)
    );
    let view_out = cli::stdout_str(&view_h);
    assert!(
        view_out.contains(&format!("signal {id}")),
        "human view should open with the signal header: {view_out}"
    );
    assert!(
        view_out.contains("status:   closed"),
        "human view should show the final status: {view_out}"
    );
    assert!(
        view_out.contains("history:"),
        "human view should carry the history section: {view_out}"
    );
    for event in ["created", "closed", "reopened"] {
        assert!(
            view_out.contains(event),
            "human history should include the {event} transition: {view_out}"
        );
    }
    assert!(
        !view_out.contains("1970"),
        "human timestamps must be real, not epoch-zero: {view_out}"
    );
}

/// `kutl signal create` authors a space-level flag (no --doc) via the relay-mint
/// CREATE route (the relay authors + attests a CREATED record and
/// binds `author_did` to the challenge-authenticated caller). Verifies that the
/// created flag appears in `signal list --fetch` as open, carries a real
/// timestamp, and has no document_id (space-level, not doc-scoped).
#[tokio::test]
async fn signal_create_journey() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    // Init the space and authorize the human identity so the relay accepts
    // the signed record.
    init_and_authorize(&relay, &home, space, "test-space").await;

    // Create a space-level flag (no --doc).
    let create = cli::kutl_in(
        home.path(),
        space,
        &[
            "signal",
            "create",
            "--kind",
            "question",
            "--message",
            "can someone review the deploy?",
        ],
    )
    .await;
    assert!(
        create.status.success(),
        "signal create failed: {}",
        cli::stderr_str(&create)
    );

    // The created flag must appear in `signal list` as open with a real timestamp.
    let list = signal_list(&home, space, false).await;
    assert_eq!(list.len(), 1, "created flag should appear: {list:?}");
    assert_eq!(
        list[0]["status"], "open",
        "created flag must be open: {:?}",
        list[0]
    );
    assert!(
        list[0]["document_id"].is_null(),
        "space-level flag has no document: {:?}",
        list[0]
    );
    let created_ms = list[0]["created_ms"]
        .as_i64()
        .expect("created_ms is an integer");
    assert!(
        created_ms >= RECENT_MS_FLOOR,
        "created_ms {created_ms} looks like 1970 (epoch-zero regression)"
    );
    assert_eq!(
        list[0]["flag_kind"], "question",
        "flag-kind renders (not an ordinal): {:?}",
        list[0]
    );
    assert!(
        list[0]["message"]
            .as_str()
            .unwrap()
            .contains("review the deploy"),
        "message renders: {:?}",
        list[0]
    );
}
