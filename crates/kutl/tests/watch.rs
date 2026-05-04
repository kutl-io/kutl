//! End-to-end integration test for `kutl watch` MCP server.
//!
//! Spawns `kutl watch` as a subprocess, sends JSON-RPC requests via stdin,
//! and verifies responses on stdout. No relay or Postgres required — tests
//! the MCP protocol layer (initialize, tools/list, ping) in isolation.

use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

/// Send a JSON-RPC request line to the process stdin.
async fn send_request(stdin: &mut tokio::process::ChildStdin, req: &serde_json::Value) {
    let line = format!("{}\n", serde_json::to_string(req).unwrap());
    stdin.write_all(line.as_bytes()).await.unwrap();
}

/// Read the next non-empty line from the reader and parse it as JSON.
async fn read_response(
    reader: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
) -> serde_json::Value {
    let line = reader.next_line().await.unwrap().unwrap();
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn test_watch_initialize_and_list_tools() {
    // Spawn kutl watch in an isolated KUTL_HOME (no real spaces.json).
    let home = tempfile::TempDir::new().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_kutl"))
        .args(["watch"])
        .env("KUTL_HOME", home.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start kutl watch");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout).lines();

    // --- initialize ---
    let init_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "clientInfo": { "name": "test", "version": "0.1" }
        }
    });
    send_request(&mut stdin, &init_req).await;

    let resp = read_response(&mut reader).await;
    assert_eq!(resp["id"], 1);
    assert!(
        resp["result"]["capabilities"]["experimental"]["claude/channel"].is_object(),
        "expected claude/channel capability, got: {resp}"
    );
    assert!(
        resp["result"]["capabilities"]["tools"].is_object(),
        "expected tools capability, got: {resp}"
    );
    assert!(
        resp["result"]["instructions"].is_string(),
        "expected instructions, got: {resp}"
    );

    // --- tools/list ---
    let tools_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    });
    send_request(&mut stdin, &tools_req).await;

    let resp = read_response(&mut reader).await;
    assert_eq!(resp["id"], 2);
    let tools = resp["result"]["tools"].as_array().unwrap();
    let tool_names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();

    // Watch-local tools must always be present (no relay needed).
    assert!(
        tool_names.contains(&"list_spaces"),
        "expected list_spaces tool, got: {tool_names:?}"
    );
    assert!(
        tool_names.contains(&"subscribe_space"),
        "expected subscribe_space tool, got: {tool_names:?}"
    );
    assert!(
        tool_names.contains(&"unsubscribe_space"),
        "expected unsubscribe_space tool, got: {tool_names:?}"
    );

    // --- ping ---
    let ping_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "ping",
        "params": {}
    });
    send_request(&mut stdin, &ping_req).await;

    let resp = read_response(&mut reader).await;
    assert_eq!(resp["id"], 3);
    assert!(
        resp["result"].is_object(),
        "expected ping result to be an object, got: {resp}"
    );

    // Clean up — kill the subprocess so it doesn't linger.
    child.kill().await.unwrap();
}
