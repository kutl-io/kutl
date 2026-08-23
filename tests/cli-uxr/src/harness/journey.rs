//! Shared journey helpers: authoritative signal reads and identity extraction
//! used across the CLI/MCP signal-lifecycle journeys.

use std::path::Path;

use crate::harness::cli;
use crate::harness::{RelayProcess, TestHome};

/// `signal list --fetch [--all] --format json` → the signal views as an array.
///
/// Pass `include_closed=true` to append `--all` and include closed signals.
pub async fn signal_list(
    home: &TestHome,
    space: &Path,
    include_closed: bool,
) -> Vec<serde_json::Value> {
    let mut args = vec!["signal", "list", "--fetch", "--format", "json"];
    if include_closed {
        args.push("--all");
    }
    let out = cli::kutl_in(home.path(), space, &args).await;
    assert!(
        out.status.success(),
        "signal list failed: {}",
        cli::stderr_str(&out)
    );
    cli::json(&out).as_array().cloned().unwrap_or_default()
}

/// The first space's `space_id` from `space list --format json`.
pub fn space_id_of(out: &std::process::Output) -> String {
    cli::json(out)
        .as_array()
        .and_then(|a| a.first())
        .and_then(|s| s["space_id"].as_str())
        .expect("space_id in space list")
        .to_owned()
}

/// The human identity DID from `status --format json` → `identity.did`.
pub fn human_did_of(out: &std::process::Output) -> String {
    cli::json(out)["identity"]["did"]
        .as_str()
        .expect("identity.did in status --format json")
        .to_owned()
}

/// The status of the signal with `id` in a `signal list` array.
pub fn status_of(list: &[serde_json::Value], id: &str) -> String {
    list.iter()
        .find(|s| s["id"] == id)
        .and_then(|s| s["status"].as_str())
        .unwrap_or_else(|| panic!("signal {id} not found in {list:?}"))
        .to_owned()
}

/// Create an agent key named `agent` and authorize its DID on the relay's live
/// allowlist. Returns the agent DID.
///
/// Authorization happens before the agent connects: under auth-on, `mcp serve`
/// authenticates the agent via the did:key challenge and space calls hit
/// `authorize_space`.
pub async fn provision_agent(
    relay: &RelayProcess,
    home: &TestHome,
    space: &Path,
    agent: &str,
) -> String {
    let create = cli::kutl_in(home.path(), space, &["agent", "create", "--name", agent]).await;
    assert!(
        create.status.success(),
        "agent create failed: {}",
        cli::stderr_str(&create)
    );
    let agent_did = cli::stdout_str(&create)
        .lines()
        .next()
        .expect("agent create prints a line")
        .trim()
        .to_owned();
    assert!(
        agent_did.starts_with("did:key:"),
        "expected a did:key on stdout, got {agent_did:?}"
    );
    relay.authorize_did(&agent_did);
    agent_did
}

/// Provision the standard MCP-journey fixture: create an agent key named
/// `agent` (authorizing its DID on the relay's live allowlist) and init a
/// space at `space` against `relay`. Returns `(agent_did, space_id)`.
pub async fn provision_agent_space(
    relay: &RelayProcess,
    home: &TestHome,
    space: &Path,
    agent: &str,
) -> (String, String) {
    let agent_did = provision_agent(relay, home, space, agent).await;

    // Init a space so the journey has something to operate on. Registration
    // stays open under mandatory auth, so init needs no allowlist
    // entry for the human DID.
    let init = cli::kutl_in(
        home.path(),
        space,
        &[
            "init",
            "--relay",
            &relay.ws_url(),
            "--dir",
            space.to_str().expect("space path is utf-8"),
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

    let space_id = space_id_of(
        &cli::kutl_in(home.path(), space, &["space", "list", "--format", "json"]).await,
    );
    (agent_did, space_id)
}

/// Assert an MCP `tools/call` succeeded: no JSON-RPC error AND no in-band tool
/// error (MCP reports tool failures as a successful response with isError=true).
pub fn assert_tool_ok(resp: &serde_json::Value, tool: &str) {
    assert!(resp.get("error").is_none(), "{tool} JSON-RPC error: {resp}");
    assert!(
        resp["result"]["isError"] != serde_json::json!(true),
        "{tool} tool error: {resp}"
    );
}

/// The concatenated `content[*].text` of an MCP tool result. Tool results
/// carry their payload as text blocks; error guidance may span several.
pub fn tool_text(resp: &serde_json::Value) -> String {
    resp["result"]["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Assert a tool call succeeded and parse its text payload as JSON — the
/// relay's document/introspection tools serialize their results as
/// pretty-printed JSON in `content[0].text`.
pub fn tool_json(resp: &serde_json::Value, tool: &str) -> serde_json::Value {
    assert_tool_ok(resp, tool);
    let text = tool_text(resp);
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("{tool} result text is not JSON ({e}): {text}"))
}

/// The change ids in a `kutl document log --format json` payload, oldest-LAST
/// (the rows are newest-first). Each row's `id` is exactly what
/// `document restore --to` takes — journeys consuming this helper pin that
/// pairing end-to-end. Empty input yields no ids — a failed invocation (e.g.
/// the file not tracked yet mid-poll) prints nothing on stdout, and the
/// caller's timeout panic carries stderr. Non-empty but malformed JSON panics
/// with the raw text so a broken schema is diagnosable rather than an
/// empty-vec mystery.
pub fn document_log_change_ids(json_text: &str) -> Vec<String> {
    if json_text.trim().is_empty() {
        return Vec::new();
    }
    let payload: serde_json::Value = serde_json::from_str(json_text)
        .unwrap_or_else(|e| panic!("document log JSON parse failed ({e}): {json_text}"));
    payload["changes"]
        .as_array()
        .unwrap_or_else(|| panic!("document log JSON has no changes array: {json_text}"))
        .iter()
        .map(|row| {
            row["id"]
                .as_str()
                .unwrap_or_else(|| panic!("change row without id: {row}"))
                .to_owned()
        })
        .collect()
}
