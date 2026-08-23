//! Agent provisioning + authorization journeys.
//!
//! An agent whose DID is NOT in the relay's `authorized_keys` must get a
//! CLEAR, actionable error — one that names `authorized_keys` and echoes the
//! agent DID so the operator can copy-paste the fix — instead of an opaque
//! "not authorized".

use kutl_cli_uxr::harness::RelayProcess;
use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::{self, tool_text};
use kutl_cli_uxr::harness::mcp::McpSession;

/// `kutl agent create` prints a usable DID + an authorized_keys hint that
/// echoes the DID; `kutl agent list` round-trips it.
#[tokio::test]
async fn agent_provisioning_journey() {
    let home = TestHome::new();
    let cwd = tempfile::tempdir().unwrap();

    let create = cli::kutl_in(
        home.path(),
        cwd.path(),
        &["agent", "create", "--name", "bot"],
    )
    .await;
    assert!(
        create.status.success(),
        "agent create failed: {}",
        cli::stderr_str(&create)
    );
    let did = cli::stdout_str(&create)
        .lines()
        .next()
        .expect("agent did")
        .trim()
        .to_owned();
    assert!(
        did.starts_with("did:key:"),
        "stdout should be the DID: {did}"
    );
    let hint = cli::stderr_str(&create);
    assert!(
        hint.contains("authorized_keys"),
        "hint should mention authorized_keys: {hint}"
    );
    assert!(
        hint.contains(&did),
        "hint should echo the agent DID so it's copy-pasteable: {hint}"
    );

    let list = cli::kutl_in(home.path(), cwd.path(), &["agent", "list"]).await;
    assert!(
        list.status.success(),
        "agent list failed: {}",
        cli::stderr_str(&list)
    );
    let listed = cli::stdout_str(&list);
    assert!(
        listed.contains("bot") && listed.contains(&did),
        "agent list should show name + DID: {listed}"
    );
}

/// An unauthorized agent (DID NOT in the relay allowlist) that tries to mutate
/// via MCP gets an ACTIONABLE error on BOTH surfaces, not an opaque "not
/// authorized" / "not a member": the message names `authorized_keys` and echoes
/// the agent DID.
///
/// WHERE IT SURFACES (both tools proxy relay-mint):
/// - `create_flag` (proxied) — the relay-mint authoring call rejects a
///   non-member, which the proxied-tool path enriches via the shared guidance.
/// - `create_document` (proxied) — the relay returns the terse "not a member of
///   space {id}", which the proxied-tool path enriches with the IDENTICAL
///   guidance, so the two surfaces are consistent.
///
/// The agent's did:key handshake succeeds regardless of the allowlist, so `mcp
/// serve` starts fine and the rejection happens at the first tool call — so we
/// do NOT need mcp-serve stderr; the guidance rides the tool result.
#[tokio::test]
async fn unauthorized_agent_gets_actionable_error() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    // Provision the agent — but DO NOT authorize it (allowlist stays empty for
    // this DID). Its did:key handshake will still succeed; authoring won't.
    let create = cli::kutl_in(home.path(), space, &["agent", "create", "--name", "bot"]).await;
    assert!(
        create.status.success(),
        "agent create failed: {}",
        cli::stderr_str(&create)
    );
    let agent_did = cli::stdout_str(&create)
        .lines()
        .next()
        .expect("agent did")
        .trim()
        .to_owned();

    // Open register: init the space against the auth-on relay.
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

    // Resolve the space id. `space list` is a local read (no relay authz), so it
    // works without authorizing anyone.
    let space_id = journey::space_id_of(
        &cli::kutl_in(home.path(), space, &["space", "list", "--format", "json"]).await,
    );

    // Drive `mcp serve` and have the UNAUTHORIZED agent attempt to author a flag.
    let mut mcp = McpSession::start(home.path(), space, "bot").await;
    mcp.initialize().await;
    let flag = mcp
        .call_tool(
            "create_flag",
            serde_json::json!({
                "space_id": space_id,
                "document_id": uuid::Uuid::new_v4().to_string(),
                "kind": "blocked",
                "message": "x",
                "audience": "space",
            }),
        )
        .await;

    // The tool call must FAIL (in-band tool error), and the failure must be the
    // actionable guidance — not an opaque "not authorized".
    assert!(
        flag.get("error").is_none(),
        "expected an in-band tool error, not a JSON-RPC error: {flag}"
    );
    assert_eq!(
        flag["result"]["isError"],
        serde_json::json!(true),
        "unauthorized create_flag should be a tool error: {flag}"
    );
    let text = tool_text(&flag);
    let lower = text.to_lowercase();

    // (1) names the remedy surface, and (2) echoes the agent DID so the operator
    // can copy-paste it. A bare "not authorized" with neither would FAIL here —
    // that's the point.
    assert!(
        lower.contains("authorized_keys"),
        "error must name authorized_keys (actionable), got: {text}"
    );
    assert!(
        text.contains(&agent_did),
        "error must echo the agent DID (copy-pasteable), got: {text}"
    );

    // A PROXIED document mutation (`create_document`) must give the SAME
    // actionable guidance. Like `create_flag` (also proxied relay-mint),
    // the rejection comes back as the terse "not a member of
    // space {id}" — the CLI enriches it with the identical `authorized_keys`
    // guidance so the two surfaces are consistent. Minimal valid args
    // (`space_id`/`path`/`content`) reach the authz gate, which fires BEFORE any
    // path-collision or mutation.
    let doc = mcp
        .call_tool(
            "create_document",
            serde_json::json!({
                "space_id": space_id,
                "path": "handbook/onboarding.md",
                "content": "hello",
            }),
        )
        .await;

    assert!(
        doc.get("error").is_none(),
        "expected an in-band tool error, not a JSON-RPC error: {doc}"
    );
    assert_eq!(
        doc["result"]["isError"],
        serde_json::json!(true),
        "unauthorized create_document should be a tool error: {doc}"
    );
    let doc_text = tool_text(&doc);
    let doc_lower = doc_text.to_lowercase();
    assert!(
        doc_lower.contains("authorized_keys"),
        "proxied create_document error must name authorized_keys (actionable), got: {doc_text}"
    );
    assert!(
        doc_text.contains(&agent_did),
        "proxied create_document error must echo the agent DID (copy-pasteable), got: {doc_text}"
    );
}
