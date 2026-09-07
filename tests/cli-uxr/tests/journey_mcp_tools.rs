//! Relay MCP document/introspection tools + session teardown.
//!
//! The agent-facing document surface — create/list/read/edit, the per-document
//! change log, space status and participants — driven through the real
//! `kutl mcp serve` stdio proxy exactly as an agent would, asserting tool
//! RESULTS (content, versions, authorship, timestamps), not just success
//! flags. Session teardown (`DELETE /mcp`) has no stdio surface — the CLI
//! proxy owns its relay session — so the destroy arc drives the relay's
//! Streamable-HTTP MCP endpoint directly, the way any non-CLI MCP client
//! would, on a bearer minted via the did:key challenge flow.

use kutl_cli_uxr::harness::RelayProcess;
use kutl_cli_uxr::harness::cli::TestHome;
use kutl_cli_uxr::harness::journey::{self, assert_tool_ok, tool_json, tool_text};
use kutl_cli_uxr::harness::mcp::McpSession;

/// Initial content `create_document` seeds.
const INITIAL_CONTENT: &str = "# Plan\n\nStep one.\n";
/// Content after the `edit_document` append.
const EDITED_CONTENT: &str = "# Plan\n\nStep one.\nStep two.\n";
/// Timestamp sanity floor (2017-07-14T02:40Z): anything below this is the
/// epoch-0/1970 bug class.
const TIMESTAMP_SANITY_FLOOR_MS: i64 = 1_500_000_000_000;

/// `status` for `space_id`, once the serve session's own relay listener has
/// registered (bounded poll: the subscribe is sent from a background task).
async fn await_listener(mcp: &mut McpSession, space_id: &str) -> serde_json::Value {
    /// Poll cadence and bound: ample for a loopback relay, short enough that
    /// a genuinely missing listener still fails the journey promptly.
    const POLL: std::time::Duration = std::time::Duration::from_millis(100);
    const ATTEMPTS: usize = 50;
    let mut status = serde_json::Value::Null;
    for _ in 0..ATTEMPTS {
        status = tool_json(
            &mcp.call_tool("status", serde_json::json!({ "space_id": space_id }))
                .await,
            "status",
        );
        if status["listener_count"] == serde_json::json!(1) {
            break;
        }
        tokio::time::sleep(POLL).await;
    }
    status
}

/// The full agent document arc over `kutl mcp serve`:
/// create → list → read (UUID and path form) → status → participants →
/// edit → re-read (the edit LANDED) → read_log (+limit) — plus the
/// authenticated HTTP text-export twin of `read_document` and the
/// agent-facing not-found hints for read/edit.
#[tokio::test]
async fn mcp_document_tools_journey() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    let (agent_did, space_id) = journey::provision_agent_space(&relay, &home, space, "bot").await;

    let mut mcp = McpSession::start(home.path(), space, "bot").await;
    mcp.initialize().await;

    // Discoverability: the document/introspection tools must be advertised —
    // an agent can only find them through tools/list.
    let tools = mcp.list_tools().await;
    for t in [
        "create_document",
        "list_documents",
        "read_document",
        "edit_document",
        "read_log",
        "status",
        "list_participants",
    ] {
        assert!(
            tools.contains(&t.to_string()),
            "missing document tool {t}; got {tools:?}"
        );
    }

    // create_document → the minted document UUID comes back in the result.
    let create = mcp
        .call_tool(
            "create_document",
            serde_json::json!({
                "space_id": space_id, "path": "notes/plan.md", "content": INITIAL_CONTENT,
            }),
        )
        .await;
    let created = tool_json(&create, "create_document");
    let doc_id = created["document_id"]
        .as_str()
        .unwrap_or_else(|| panic!("create_document returns document_id: {created}"))
        .to_owned();
    assert!(
        uuid::Uuid::try_parse(&doc_id).is_ok(),
        "document_id should be a UUID: {doc_id}"
    );

    // list_documents → exactly the created doc, typed "text", at its path,
    // with NO provenance keys (natively-authored docs omit them entirely —
    // the tool description promises that).
    let list = tool_json(
        &mcp.call_tool(
            "list_documents",
            serde_json::json!({ "space_id": space_id }),
        )
        .await,
        "list_documents",
    );
    let docs = list.as_array().expect("list_documents returns an array");
    assert_eq!(docs.len(), 1, "expected exactly the created doc: {list}");
    let doc = &docs[0];
    assert_eq!(doc["document_id"], serde_json::json!(doc_id), "{doc}");
    assert_eq!(doc["path"], serde_json::json!("notes/plan.md"), "{doc}");
    assert_eq!(doc["content_type"], serde_json::json!("text"), "{doc}");
    assert!(
        doc["subscriber_count"].is_number(),
        "subscriber_count present: {doc}"
    );
    for provenance_key in ["source_kind", "source_id", "source_url"] {
        assert!(
            doc.get(provenance_key).is_none(),
            "native docs must omit {provenance_key}: {doc}"
        );
    }

    // read_document by UUID → the exact content plus an opaque version.
    let read = tool_json(
        &mcp.call_tool(
            "read_document",
            serde_json::json!({ "space_id": space_id, "document_id": doc_id }),
        )
        .await,
        "read_document",
    );
    assert_eq!(
        read["content"],
        serde_json::json!(INITIAL_CONTENT),
        "{read}"
    );
    let version_before = read["version"]
        .as_str()
        .expect("read_document returns a version string")
        .to_owned();
    assert!(!version_before.is_empty(), "version must be non-empty");

    // read_document by within-space PATH → resolves to the same document
    // (the tool contract: both UUIDs and paths are accepted).
    let by_path = tool_json(
        &mcp.call_tool(
            "read_document",
            serde_json::json!({ "space_id": space_id, "document_id": "notes/plan.md" }),
        )
        .await,
        "read_document",
    );
    assert_eq!(
        by_path["content"],
        serde_json::json!(INITIAL_CONTENT),
        "path-form read must hit the same doc: {by_path}"
    );

    // status → the loaded-slot count, the connections listening to the
    // space, and exactly one MCP session (the proxy holds one relay session
    // for the whole serve lifetime).
    //
    // `document_count` is 1 and agrees with `list_documents`: the serve
    // session holds no doc slot, so it never inflates the count.
    // `listener_count` is 1: the serve session's `SubscribeSignals` is what
    // makes it present in the space. That frame goes out from the serve
    // session's own relay task, so poll until the relay has seen it rather
    // than racing it.
    let status = await_listener(&mut mcp, &space_id).await;
    assert_eq!(status["document_count"], serde_json::json!(1), "{status}");
    assert_eq!(status["listener_count"], serde_json::json!(1), "{status}");
    assert_eq!(
        status["mcp_session_count"],
        serde_json::json!(1),
        "{status}"
    );

    // list_participants → the agent, exactly once, present over websocket:
    // the serve session listens to the space (`SubscribeSignals`), and a
    // listening connection outranks the MCP session the same DID also holds.
    let participants = tool_json(
        &mcp.call_tool(
            "list_participants",
            serde_json::json!({ "space_id": space_id }),
        )
        .await,
        "list_participants",
    );
    let participants = participants
        .as_array()
        .expect("list_participants returns an array");
    assert_eq!(
        participants.len(),
        1,
        "the agent must appear exactly once (DID-deduped): {participants:?}"
    );
    assert_eq!(
        participants[0]["did"],
        serde_json::json!(agent_did),
        "{participants:?}"
    );
    assert_eq!(
        participants[0]["connection_type"],
        serde_json::json!("websocket"),
        "the serve session is present because it listens: {participants:?}"
    );

    // edit_document → the append is merged into the base the read named, so
    // it lands as ops with nothing refused.
    let edit = tool_json(
        &mcp.call_tool(
            "edit_document",
            serde_json::json!({
                "space_id": space_id, "document_id": doc_id,
                "base_version": version_before,
                "content": EDITED_CONTENT, "intent": "add step two",
            }),
        )
        .await,
        "edit_document",
    );
    assert_eq!(edit["document_id"], serde_json::json!(doc_id), "{edit}");
    assert!(
        edit["ops_applied"].as_u64().is_some_and(|n| n >= 1),
        "edit must apply ops: {edit}"
    );
    assert_eq!(
        edit["hunks_refused"],
        serde_json::json!([]),
        "an uncontested append places cleanly: {edit}"
    );
    assert!(
        edit.get("was_bulk").is_none(),
        "a merge never rewrites in bulk, so it reports no bulk flag: {edit}"
    );
    // No version comes back: a merged document holds the caller's content plus
    // whatever a peer contributed, so a token minted here would name text the
    // caller never read. Writing again starts with reading again.
    assert!(
        edit.get("new_version").is_none(),
        "an edit must not hand back a version to write against: {edit}"
    );

    // The edit LANDED: a second read returns the new content, and mints the
    // version any further write must name.
    let reread = tool_json(
        &mcp.call_tool(
            "read_document",
            serde_json::json!({ "space_id": space_id, "document_id": doc_id }),
        )
        .await,
        "read_document",
    );
    assert_eq!(
        reread["content"],
        serde_json::json!(EDITED_CONTENT),
        "the edit must be visible on re-read: {reread}"
    );

    // read_log → both changes, most recent first, attributed to the AGENT,
    // with the supplied intents and sane (non-1970) timestamps.
    let log = tool_json(
        &mcp.call_tool(
            "read_log",
            serde_json::json!({ "space_id": space_id, "document_id": doc_id }),
        )
        .await,
        "read_log",
    );
    let entries = log.as_array().expect("read_log returns an array");
    assert_eq!(
        entries.len(),
        2,
        "one create + one edit = two log entries: {log}"
    );
    assert_eq!(
        entries[0]["intent"],
        serde_json::json!("add step two"),
        "most recent first: {log}"
    );
    assert_eq!(
        entries[1]["intent"],
        serde_json::json!("create_document"),
        "the initial fill carries the create_document intent: {log}"
    );
    for entry in entries {
        assert_eq!(
            entry["author_did"],
            serde_json::json!(agent_did),
            "MCP edits attribute to the session (agent) DID: {entry}"
        );
        assert!(
            entry["timestamp"]
                .as_i64()
                .is_some_and(|t| t > TIMESTAMP_SANITY_FLOOR_MS),
            "timestamp must be real epoch millis, not 0/1970: {entry}"
        );
        assert_eq!(
            entry["boundary"],
            serde_json::json!("explicit"),
            "MCP edits are explicit boundaries: {entry}"
        );
        assert!(
            entry["id"].as_str().is_some_and(|id| !id.is_empty()),
            "log entries carry a change id: {entry}"
        );
    }
    let newest_id = entries[0]["id"].clone();

    // read_log with limit=1 → exactly the newest entry.
    let limited = tool_json(
        &mcp.call_tool(
            "read_log",
            serde_json::json!({ "space_id": space_id, "document_id": doc_id, "limit": 1 }),
        )
        .await,
        "read_log",
    );
    let limited = limited.as_array().expect("read_log returns an array");
    assert_eq!(limited.len(), 1, "limit=1 caps the result: {limited:?}");
    assert_eq!(
        limited[0]["id"], newest_id,
        "limit keeps the MOST RECENT entry: {limited:?}"
    );

    // Agent-facing not-found hints are part of the tool contract: a read miss
    // routes at list_documents, an edit miss routes at create_document.
    let missing = mcp
        .call_tool(
            "read_document",
            serde_json::json!({
                "space_id": space_id,
                "document_id": uuid::Uuid::new_v4().to_string(),
            }),
        )
        .await;
    assert_eq!(
        missing["result"]["isError"],
        serde_json::json!(true),
        "unknown doc read must be a tool error: {missing}"
    );
    assert!(
        tool_text(&missing).contains("use list_documents"),
        "read miss must hint at list_documents: {missing}"
    );

    let edit_missing = mcp
        .call_tool(
            "edit_document",
            serde_json::json!({
                "space_id": space_id, "document_id": "notes/nope.md",
                // Never examined: an unresolvable document is refused before
                // the base a caller names is ever looked at.
                "base_version": "kv1.not-a-token",
                "content": "x", "intent": "test",
            }),
        )
        .await;
    assert_eq!(
        edit_missing["result"]["isError"],
        serde_json::json!(true),
        "unknown doc edit must be a tool error: {edit_missing}"
    );
    assert!(
        tool_text(&edit_missing).contains("use create_document"),
        "edit miss must hint at create_document (no silent auto-create): {edit_missing}"
    );
    drop(mcp);

    // The HTTP text-export twin of read_document
    // (`GET /spaces/{id}/documents/{id}/text`, bearer-authenticated): an
    // AUTHORIZED reader gets the current text verbatim as text/plain.
    let (reader_did, bearer) = relay.mint_bearer().await;
    relay.authorize_did(&reader_did);
    let http = reqwest::Client::new();
    let export_url = |doc: &str| {
        format!(
            "{}/spaces/{}/documents/{}/text",
            relay.http_base(),
            space_id,
            doc
        )
    };
    let resp = http
        .get(export_url(&doc_id))
        .bearer_auth(&bearer)
        .send()
        .await
        .expect("GET text export");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/plain; charset=utf-8"),
        "text export must be text/plain"
    );
    let body = resp.text().await.expect("text export body");
    assert_eq!(
        body, EDITED_CONTENT,
        "export must return the edited content"
    );

    // Unknown document → a clean 404, not a 500.
    let resp = http
        .get(export_url(&uuid::Uuid::new_v4().to_string()))
        .bearer_auth(&bearer)
        .send()
        .await
        .expect("GET text export (missing doc)");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(resp.text().await.unwrap(), "document not found");

    // A valid bearer whose DID is NOT in authorized_keys → 403 (fail-closed
    // on the space, even though the identity itself authenticated fine).
    let (_stranger_did, stranger_bearer) = relay.mint_bearer().await;
    let resp = http
        .get(export_url(&doc_id))
        .bearer_auth(&stranger_bearer)
        .send()
        .await
        .expect("GET text export (unauthorized)");
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(resp.text().await.unwrap(), "not authorized");
}

/// MCP session teardown over the relay's Streamable-HTTP endpoint:
/// initialize mints a session (header-carried id), the session works, then
/// `DELETE /mcp` destroys it — after which BOTH a tool call and a second
/// DELETE fail cleanly with 404 "session not found or expired" (no zombie
/// session, no 500).
#[tokio::test]
async fn mcp_http_session_destroy_journey() {
    let relay = RelayProcess::spawn().await;
    let (_did, bearer) = relay.mint_bearer().await;
    let http = reqwest::Client::new();
    let mcp_url = format!("{}/mcp", relay.http_base());

    // initialize → 200 with the session id in the mcp-session-id header.
    let resp = http
        .post(&mcp_url)
        .bearer_auth(&bearer)
        .json(&serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "clientInfo": { "name": "cli-uxr", "version": "0.1" }
            }
        }))
        .send()
        .await
        .expect("POST /mcp initialize");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let session_id = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .expect("initialize returns an mcp-session-id header")
        .to_owned();
    let body: serde_json::Value = resp.json().await.expect("initialize body json");
    assert_eq!(
        body["result"]["serverInfo"]["name"],
        serde_json::json!("kutl-relay"),
        "{body}"
    );
    assert!(
        body["result"]["instructions"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "initialize must carry agent instructions: {body}"
    );

    // A tools/call without the session header is a clean 400 naming the
    // missing header, not an opaque failure.
    let call_list_spaces = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": { "name": "list_spaces", "arguments": {} }
    });
    let resp = http
        .post(&mcp_url)
        .bearer_auth(&bearer)
        .json(&call_list_spaces)
        .send()
        .await
        .expect("POST /mcp tools/call (no session)");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.expect("error body json");
    assert_eq!(
        body["error"],
        serde_json::json!("missing Mcp-Session-Id header"),
        "{body}"
    );

    // The live session works: list_spaces succeeds (an empty JSON array —
    // nothing registered on this fresh relay).
    let resp = http
        .post(&mcp_url)
        .bearer_auth(&bearer)
        .header("mcp-session-id", &session_id)
        .json(&call_list_spaces)
        .send()
        .await
        .expect("POST /mcp tools/call");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("tools/call body json");
    assert_tool_ok(&body, "list_spaces");
    let spaces: serde_json::Value = serde_json::from_str(&tool_text(&body))
        .unwrap_or_else(|e| panic!("list_spaces text is JSON ({e}): {body}"));
    assert_eq!(spaces, serde_json::json!([]), "fresh relay has no spaces");

    // DELETE /mcp → 204 and the session is GONE.
    let resp = http
        .delete(&mcp_url)
        .bearer_auth(&bearer)
        .header("mcp-session-id", &session_id)
        .send()
        .await
        .expect("DELETE /mcp");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    // A follow-up tool call on the destroyed session fails cleanly.
    let resp = http
        .post(&mcp_url)
        .bearer_auth(&bearer)
        .header("mcp-session-id", &session_id)
        .json(&call_list_spaces)
        .send()
        .await
        .expect("POST /mcp tools/call (destroyed session)");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await.expect("destroyed-session body json");
    assert_eq!(
        body["error"],
        serde_json::json!("session not found or expired"),
        "{body}"
    );

    // Double-destroy is the same clean 404, not a crash.
    let resp = http
        .delete(&mcp_url)
        .bearer_auth(&bearer)
        .header("mcp-session-id", &session_id)
        .send()
        .await
        .expect("DELETE /mcp (again)");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}
