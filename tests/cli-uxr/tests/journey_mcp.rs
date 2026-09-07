use kutl_cli_uxr::harness::RelayProcess;
use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::{self, assert_tool_ok};
use kutl_cli_uxr::harness::mcp::McpSession;

/// Bring up a relay + space + agent, then drive `kutl mcp serve` and confirm
/// the tool surface a user/agent would see.
#[tokio::test]
async fn mcp_initialize_and_tool_surface() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space = home.space_dir();

    // Agent + space fixture (agent DID authorized, space inited + registered).
    let (_agent_did, _space_id) =
        journey::provision_agent_space(&relay, &home, space.path(), "bot").await;

    // Drive mcp serve.
    let mut mcp = McpSession::start(home.path(), space.path(), "bot").await;
    mcp.initialize().await;
    let tools = mcp.list_tools().await;

    // The agent-authored signal tools the OSS relay advertises must be present.
    //
    // `reopen_flag` IS advertised on the OSS relay:
    // the OSS relay dispatches it (`parse_reopen_flag` / `handle_mcp_reopen_flag`)
    // AND advertises it in `tools/list` via the base `signal_tools()` set, so
    // the agent tool surface is complete (create/reply/close/reopen/detail). Reopen
    // appends a REOPENED record through the same path as close and works on any
    // deployment. The CLI copies the relay's advertised definitions verbatim
    // (`handle_tools_list` = watch-local + relay_tools). See kutl-relay
    // `signal_tools()`.
    for t in [
        "create_flag",
        "create_reply",
        "close_flag",
        "reopen_flag",
        "get_signal_detail",
    ] {
        assert!(
            tools.contains(&t.to_string()),
            "missing agent tool {t}; got {tools:?}"
        );
    }
    for t in [
        "list_spaces",
        "subscribe_space",
        "unsubscribe_space",
        "wait_for_changes",
    ] {
        assert!(
            tools.contains(&t.to_string()),
            "missing watch-local tool {t}; got {tools:?}"
        );
    }

    // Even though the relay also advertises list_spaces, the CLI
    // must deduplicate — MCP requires unique tool names.
    let list_spaces_count = tools.iter().filter(|t| t.as_str() == "list_spaces").count();
    assert_eq!(
        list_spaces_count, 1,
        "list_spaces must appear exactly once in tools/list; got {list_spaces_count}: {tools:?}"
    );

    // The list_spaces result must NOT leak a host filesystem path.
    // This runs hermetically in CI (unlike the watch.rs unit test, which needs a
    // populated $KUTL_HOME) because `init` above registered a real space.
    let spaces = mcp.call_tool("list_spaces", serde_json::json!({})).await;
    let text = spaces["result"]["content"][0]["text"]
        .as_str()
        .expect("list_spaces returns text content");
    let arr: serde_json::Value =
        serde_json::from_str(text).expect("list_spaces text is a JSON array");
    let elements = arr.as_array().expect("list_spaces is an array");
    for el in elements {
        assert!(
            el.get("path").is_none(),
            "list_spaces must not expose a host path: {el}"
        );
        assert!(
            el.get("space_id").is_some(),
            "list_spaces element should have space_id: {el}"
        );
    }
    assert!(
        !elements.is_empty(),
        "expected at least the inited space in list_spaces"
    );
}

/// A request answered even though stdin closed behind it.
///
/// Requests are handled off the event loop so a blocking call cannot stall it,
/// which means a response can still be in flight when EOF arrives. Anything
/// that pipes one frame and closes — a health check, a setup assertion, a shell
/// one-liner — gets silence if shutdown does not drain first.
#[tokio::test]
async fn mcp_answers_a_request_whose_stdin_closes_immediately() {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space = home.space_dir();
    let (_agent_did, _space_id) =
        journey::provision_agent_space(&relay, &home, space.path(), "bot").await;

    let mut child = tokio::process::Command::new(kutl_cli_uxr::harness::binaries::kutl_bin())
        .args(["mcp", "serve", "--agent", "bot"])
        .env("KUTL_HOME", home.path())
        .env("KUTL_LOG", "warn")
        .current_dir(space.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn kutl mcp serve");

    let mut stdin = child.stdin.take().expect("stdin");
    stdin
        .write_all(
            b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\",\"clientInfo\":{\"name\":\"probe\",\"version\":\"1\"}}}\n",
        )
        .await
        .expect("write initialize");
    stdin.flush().await.expect("flush");
    drop(stdin); // EOF, immediately behind the request

    let stdout = child.stdout.take().expect("stdout");
    let line = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        BufReader::new(stdout).lines().next_line(),
    )
    .await
    .expect("the server must answer before it shuts down")
    .expect("read stdout");

    let line = line.expect("a response, not EOF — the request was dropped on shutdown");
    let resp: serde_json::Value = serde_json::from_str(&line).expect("response is json");
    assert!(
        resp["result"]["protocolVersion"].is_string(),
        "expected an initialize result, got: {resp}"
    );
}

/// The handshake tells the agent which space it is in.
///
/// The server subscribes to the local space at startup and announces it on
/// stderr, which no MCP client shows the model. Every space-scoped tool takes
/// a `space_id`, so an agent that is never told one has no first move: it
/// guesses names until it gives up.
#[tokio::test]
async fn mcp_initialize_names_the_subscribed_space() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space = home.space_dir();

    let (_agent_did, space_id) =
        journey::provision_agent_space(&relay, &home, space.path(), "bot").await;

    let mut mcp = McpSession::start(home.path(), space.path(), "bot").await;
    let instructions = mcp.initialize_instructions().await;

    assert!(
        instructions.contains(&space_id),
        "the handshake must carry the subscribed space id ({space_id}); got:\n{instructions}"
    );
    assert!(
        instructions.contains("wait_for_changes"),
        "the handshake should point at the blocking read; got:\n{instructions}"
    );
}

/// The agent authors the full signal lifecycle via MCP tools (create/reply/
/// close/reopen), and the records carry the AGENT's DID — distinct from the
/// human identity (the relay mints with the session DID, which is the
/// agent's authenticated DID, so agent_did != human_did holds).
#[tokio::test]
async fn mcp_agent_authoring_journey() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    let (agent_did, space_id) = journey::provision_agent_space(&relay, &home, space, "bot").await;
    let human_did = journey::human_did_of(
        &cli::kutl_in(home.path(), space, &["status", "--format", "json"]).await,
    );
    relay.authorize_did(&human_did);

    let mut mcp = McpSession::start(home.path(), space, "bot").await;
    mcp.initialize().await;

    // create_flag
    let flag = mcp
        .call_tool(
            "create_flag",
            serde_json::json!({
                "space_id": space_id, "document_id": uuid::Uuid::new_v4().to_string(),
                "kind": "question", "message": "can someone review the PR?", "audience": "space",
            }),
        )
        .await;
    assert_tool_ok(&flag, "create_flag");

    // authoritative fetch → the flag id
    let list = journey::signal_list(&home, space, false).await;
    assert_eq!(list.len(), 1, "expected one open flag: {list:?}");
    let flag_id = list[0]["id"].as_str().expect("flag id").to_owned();

    // get_signal_detail through the CLI proxy pins two contracts:
    //   (1) the result is NOT double-wrapped — its `content[0].text` is the
    //       SignalDetail JSON directly, not a nested `{"content":...}` envelope
    //       (ToolCallResult.is_error is #[serde(default)], so its absence
    //       never forces the error wrapping);
    //   (2) `flag_kind` reads back as the NAME "question" — matching
    //       create_flag's input — not the enum ordinal "4".
    let detail = mcp
        .call_tool(
            "get_signal_detail",
            serde_json::json!({ "space_id": space_id, "signal_id": flag_id }),
        )
        .await;
    assert_tool_ok(&detail, "get_signal_detail");
    let detail_text = detail["result"]["content"][0]["text"]
        .as_str()
        .expect("get_signal_detail returns text content");
    let parsed: serde_json::Value =
        serde_json::from_str(detail_text).expect("detail text parses as JSON (not double-wrapped)");
    assert!(
        parsed.is_object(),
        "detail must be the SignalDetail object directly: {parsed}"
    );
    assert_eq!(
        parsed["id"].as_str(),
        Some(flag_id.as_str()),
        "detail carries the flag id at top level (not nested under content): {parsed}"
    );
    assert_eq!(
        parsed["space_id"].as_str(),
        Some(space_id.as_str()),
        "detail carries space_id at top level: {parsed}"
    );
    assert_eq!(
        parsed["flag_kind"].as_str(),
        Some("question"),
        "flag_kind must read back as the name 'question', not an ordinal: {parsed}"
    );

    // create_reply (a new CREATED signal referencing the flag)
    let reply = mcp
        .call_tool(
            "create_reply",
            serde_json::json!({
                "space_id": space_id, "parent_signal_id": flag_id, "body": "on it — reviewing now",
            }),
        )
        .await;
    assert_tool_ok(&reply, "create_reply");

    // close_flag then reopen_flag (reopen_flag is the OSS-advertised tool)
    let close = mcp
        .call_tool(
            "close_flag",
            serde_json::json!({ "space_id": space_id, "signal_id": flag_id }),
        )
        .await;
    assert_tool_ok(&close, "close_flag");
    assert_eq!(
        journey::status_of(&journey::signal_list(&home, space, true).await, &flag_id),
        "closed"
    );

    let reopen = mcp
        .call_tool(
            "reopen_flag",
            serde_json::json!({ "space_id": space_id, "signal_id": flag_id }),
        )
        .await;
    assert_tool_ok(&reopen, "reopen_flag");
    assert_eq!(
        journey::status_of(&journey::signal_list(&home, space, false).await, &flag_id),
        "open"
    );
    drop(mcp);

    // Distinct authorship: the flag's records carry the AGENT's DID because the
    // relay mints with the authenticated session DID (the agent, not the human).
    // This holds under relay-mint — no client signing needed.
    let view = cli::kutl_in(
        home.path(),
        space,
        &["signal", "view", &flag_id, "--fetch", "--format", "json"],
    )
    .await;
    assert!(
        view.status.success(),
        "view failed: {}",
        cli::stderr_str(&view)
    );
    let v = cli::json(&view).to_string();
    assert!(
        v.contains(&agent_did),
        "the flag's records should carry the agent DID: {v}"
    );
    assert!(
        !v.contains(&human_did),
        "the human never authored here — no human DID expected: {v}"
    );
}

/// `wait_for_changes` is the portable way for an agent to be woken by activity
/// without polling. Five properties, all against the real binary: a quiet space
/// answers empty at the deadline rather than erroring; a blocked call wakes on
/// a peer's flag; a blocked call wakes on a peer's document edit, which travels
/// a different channel end to end (relay notification frame → SSE → the CLI's
/// parser → the wake broadcast); and a blocked call does not stop the
/// connection serving anything else.
#[tokio::test]
async fn mcp_wait_for_changes_journey() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    let (waiter_did, space_id) = journey::provision_agent_space(&relay, &home, space, "bot").await;
    journey::provision_agent(&relay, &home, space, "peer").await;

    let mut waiter = McpSession::start(home.path(), space, "bot").await;
    waiter.initialize().await;

    // Drain whatever provisioning already put on this agent's cursor, so the
    // quiet-space case below really is quiet.
    let drain = waiter
        .call_tool(
            "wait_for_changes",
            serde_json::json!({ "space_id": space_id, "timeout_seconds": 1 }),
        )
        .await;
    assert_tool_ok(&drain, "wait_for_changes (drain)");

    // A deadline with nothing to report is an ordinary empty answer. An agent
    // looping on this must not have to treat its normal beat as a failure.
    let quiet = waiter
        .call_tool(
            "wait_for_changes",
            serde_json::json!({ "space_id": space_id, "timeout_seconds": 1 }),
        )
        .await;
    let payload = journey::tool_json(&quiet, "wait_for_changes");
    assert_eq!(
        payload["signals"].as_array().map(Vec::len),
        Some(0),
        "a quiet space reports no signals: {payload}"
    );
    assert_eq!(
        payload["document_changes"].as_array().map(Vec::len),
        Some(0),
        "a quiet space reports no document changes: {payload}"
    );

    // Block first, flag second: this is the wake path, not the read-first one.
    let waiting_id = waiter
        .send(
            "tools/call",
            serde_json::json!({
                "name": "wait_for_changes",
                "arguments": { "space_id": space_id, "timeout_seconds": 30 },
            }),
        )
        .await;

    // While that call is outstanding, the connection still answers. This is the
    // regression that would put the request loop back behind its own handlers.
    let list_id = waiter.send("tools/list", serde_json::json!({})).await;
    let listed = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        waiter.read_response(list_id),
    )
    .await
    .expect("a blocked tool call must not stop the connection serving others");
    assert!(
        listed["result"]["tools"].is_array(),
        "tools/list answered while a call was blocked: {listed}"
    );
    // Still outstanding here, before any peer activity exists. That is what
    // makes the wake below attributable to the flag rather than to the read
    // the call performs on its way in.
    assert!(
        !waiter.is_answered(waiting_id),
        "wait_for_changes should still be blocked in a quiet space"
    );

    let mut peer = McpSession::start(home.path(), space, "peer").await;
    peer.initialize().await;
    let flag = peer
        .call_tool(
            "create_flag",
            serde_json::json!({
                "space_id": space_id,
                "document_id": uuid::Uuid::new_v4().to_string(),
                "kind": "question",
                "message": "what should we do about the menu?",
                "audience": "participant",
                "target_did": waiter_did,
            }),
        )
        .await;
    assert_tool_ok(&flag, "create_flag");

    let woken = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        waiter.read_response(waiting_id),
    )
    .await
    .expect("a peer's flag must wake the blocked call before its deadline");
    let payload = journey::tool_json(&woken, "wait_for_changes");
    assert!(
        payload["signals"].as_array().is_some_and(|s| !s.is_empty()),
        "the woken call reports the peer's signal: {payload}"
    );

    // ---- A document edit wakes the same call, over a different channel ----
    //
    // Signals wake through the relay's signal stream. A document change wakes
    // through the notification frame the relay writes onto the SSE stream, which
    // this server parses back into a wake. Nothing above touches that chain, and
    // its two ends agree by field name, so it can stop working end to end while
    // every unit test on either side of it stays green.
    let doc = journey::tool_json(
        &peer
            .call_tool(
                "create_document",
                serde_json::json!({
                    "space_id": space_id, "path": "wake.md",
                    "content": "# Wake\n\nnothing yet.\n",
                    "intent": "seed the document the waiter will be woken by",
                }),
            )
            .await,
        "create_document",
    );
    let doc_id = doc["document_id"].as_str().expect("document id").to_owned();

    // The registration above is itself activity on the waiter's cursor. Consume
    // it, then confirm the space is quiet again — otherwise the block below
    // would return on the create and prove nothing about the edit.
    assert_tool_ok(
        &waiter
            .call_tool(
                "wait_for_changes",
                serde_json::json!({ "space_id": space_id, "timeout_seconds": 5 }),
            )
            .await,
        "wait_for_changes (drain the registration)",
    );
    let quiet_again = journey::tool_json(
        &waiter
            .call_tool(
                "wait_for_changes",
                serde_json::json!({ "space_id": space_id, "timeout_seconds": 2 }),
            )
            .await,
        "wait_for_changes",
    );
    assert_eq!(
        quiet_again["document_changes"].as_array().map(Vec::len),
        Some(0),
        "the registration is drained before the edit case: {quiet_again}"
    );

    let doc_waiting_id = waiter
        .send(
            "tools/call",
            serde_json::json!({
                "name": "wait_for_changes",
                "arguments": { "space_id": space_id, "timeout_seconds": 30 },
            }),
        )
        .await;

    let peer_read = journey::tool_json(
        &peer
            .call_tool(
                "read_document",
                serde_json::json!({ "space_id": space_id, "document_id": doc_id }),
            )
            .await,
        "read_document",
    );
    assert_tool_ok(
        &peer
            .call_tool(
                "edit_document",
                serde_json::json!({
                    "space_id": space_id, "document_id": doc_id,
                    "base_version": peer_read["version"],
                    "content": "# Wake\n\nsomething now.\n",
                    "intent": "give the waiter something to wake on",
                }),
            )
            .await,
        "edit_document",
    );

    let doc_woken = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        waiter.read_response(doc_waiting_id),
    )
    .await
    .expect("a peer's document edit must wake the blocked call before its deadline");
    let payload = journey::tool_json(&doc_woken, "wait_for_changes");
    assert!(
        payload["document_changes"]
            .as_array()
            .is_some_and(|c| !c.is_empty()),
        "the woken call reports the peer's document change: {payload}"
    );
}

/// Two agents, one document, one of them holding a stale read.
///
/// The property the whole write path exists to provide: an agent that never saw
/// a peer's section cannot delete it.
#[tokio::test]
async fn mcp_stale_edit_preserves_a_peer_section() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    let (_did, space_id) = journey::provision_agent_space(&relay, &home, space, "bot").await;
    journey::provision_agent(&relay, &home, space, "peer").await;

    let mut bot = McpSession::start(home.path(), space, "bot").await;
    bot.initialize().await;
    let mut peer = McpSession::start(home.path(), space, "peer").await;
    peer.initialize().await;

    let doc = journey::tool_json(
        &bot.call_tool(
            "create_document",
            serde_json::json!({
                "space_id": space_id, "path": "menu.md",
                "content": "# Menu\n\n## Starter\n\n## Main\n",
                "intent": "seed",
            }),
        )
        .await,
        "create_document",
    );
    let doc_id = doc["document_id"].as_str().expect("document id").to_owned();

    // Bot reads and holds the version while the peer lands a section.
    let stale = journey::tool_json(
        &bot.call_tool(
            "read_document",
            serde_json::json!({ "space_id": space_id, "document_id": doc_id }),
        )
        .await,
        "read_document",
    );
    let stale_version = stale["version"].as_str().expect("version").to_owned();
    let stale_content = stale["content"].as_str().expect("content").to_owned();

    let peer_read = journey::tool_json(
        &peer
            .call_tool(
                "read_document",
                serde_json::json!({ "space_id": space_id, "document_id": doc_id }),
            )
            .await,
        "read_document",
    );
    assert_tool_ok(
        &peer.call_tool("edit_document", serde_json::json!({
            "space_id": space_id, "document_id": doc_id,
            "base_version": peer_read["version"],
            "content": format!("{}\n## Peter\n\nkosher check\n", peer_read["content"].as_str().expect("content")),
            "intent": "add my section",
        })).await,
        "peer edit_document",
    );

    // Bot writes from the stale read.
    assert_tool_ok(
        &bot.call_tool(
            "edit_document",
            serde_json::json!({
                "space_id": space_id, "document_id": doc_id,
                "base_version": stale_version,
                "content": stale_content.replace("## Main\n", "## Main\n\nrisotto\n"),
                "intent": "propose a main",
            }),
        )
        .await,
        "bot edit_document",
    );

    let after = journey::tool_json(
        &bot.call_tool(
            "read_document",
            serde_json::json!({ "space_id": space_id, "document_id": doc_id }),
        )
        .await,
        "read_document",
    );
    let text = after["content"].as_str().expect("content");
    assert!(
        text.contains("risotto"),
        "the stale writer's change landed: {text}"
    );
    assert!(
        text.contains("kosher check"),
        "the peer's section survived: {text}"
    );
}
