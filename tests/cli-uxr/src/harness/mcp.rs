//! Drive `kutl mcp serve` over line-delimited JSON-RPC stdio.

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::harness::binaries::kutl_bin;

/// A live `kutl mcp serve` subprocess and its JSON-RPC stdio channels.
pub struct McpSession {
    _child: Child,
    stdin: ChildStdin,
    reader: Lines<BufReader<ChildStdout>>,
    next_id: i64,
    /// Responses read while waiting for a different id. Requests may be
    /// outstanding concurrently and answers arrive in whatever order the
    /// server finishes them, so an answer nobody is waiting for yet is kept
    /// rather than dropped.
    pending: HashMap<i64, serde_json::Value>,
}

impl McpSession {
    /// Spawn `kutl mcp serve --agent <agent>` with `KUTL_HOME=home`, cwd `cwd`
    /// (so it resolves the registered space), and set up stdio.
    pub async fn start(home: &Path, cwd: &Path, agent: &str) -> Self {
        let mut child = Command::new(kutl_bin())
            .args(["mcp", "serve", "--agent", agent])
            .env("KUTL_HOME", home)
            .env("KUTL_LOG", "warn")
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn kutl mcp serve");

        let stdin = child.stdin.take().expect("mcp serve stdin");
        let stdout = child.stdout.take().expect("mcp serve stdout");
        let reader = BufReader::new(stdout).lines();
        Self {
            _child: child,
            stdin,
            reader,
            next_id: 0,
            pending: HashMap::new(),
        }
    }

    fn alloc_id(&mut self) -> i64 {
        self.next_id += 1;
        self.next_id
    }

    /// Write one JSON-RPC request without waiting for its answer, returning the
    /// id to read it back with. Lets a test hold a request open while sending
    /// others down the same connection.
    pub async fn send(&mut self, method: &str, params: serde_json::Value) -> i64 {
        let id = self.alloc_id();
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let line = format!(
            "{}\n",
            serde_json::to_string(&req).expect("serialize mcp request")
        );
        self.stdin
            .write_all(line.as_bytes())
            .await
            .expect("write mcp request");
        self.stdin.flush().await.expect("flush mcp request");
        id
    }

    /// Whether the answer to `id` has already been read off the wire while
    /// waiting for some other request. Lets a test assert that a call was
    /// still outstanding at a particular point.
    pub fn is_answered(&self, id: i64) -> bool {
        self.pending.contains_key(&id)
    }

    /// Read until the response carrying `id` arrives. Notifications are
    /// skipped; answers to other outstanding requests are kept for their own
    /// reader.
    pub async fn read_response(&mut self, id: i64) -> serde_json::Value {
        if let Some(resp) = self.pending.remove(&id) {
            return resp;
        }
        loop {
            let line = self
                .reader
                .next_line()
                .await
                .expect("read mcp line")
                .expect("mcp serve closed stdout unexpectedly");
            let v: serde_json::Value = serde_json::from_str(&line).expect("mcp line is json");
            match v.get("id").and_then(serde_json::Value::as_i64) {
                Some(got) if got == id => return v,
                Some(other) => {
                    self.pending.insert(other, v);
                }
                None => {}
            }
        }
    }

    /// Send one JSON-RPC request and return its response.
    async fn request(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
        let id = self.send(method, params).await;
        self.read_response(id).await
    }

    /// Perform the MCP `initialize` handshake.
    pub async fn initialize(&mut self) {
        self.initialize_instructions().await;
    }

    /// Perform the handshake and return the server's `instructions` — the only
    /// prose a model-side agent sees before it starts, and so the only place
    /// the server can tell it anything it was not asked for.
    pub async fn initialize_instructions(&mut self) -> String {
        let resp = self
            .request(
                "initialize",
                serde_json::json!({
                    "protocolVersion": "2025-03-26",
                    "clientInfo": { "name": "cli-uxr", "version": "0.1" }
                }),
            )
            .await;
        assert!(resp["result"].is_object(), "initialize failed: {resp}");
        resp["result"]["instructions"]
            .as_str()
            .expect("initialize returns instructions")
            .to_owned()
    }

    /// Return the tool names from `tools/list`.
    pub async fn list_tools(&mut self) -> Vec<String> {
        let resp = self.request("tools/list", serde_json::json!({})).await;
        resp["result"]["tools"]
            .as_array()
            .expect("tools/list returns an array")
            .iter()
            .filter_map(|t| t["name"].as_str().map(str::to_owned))
            .collect()
    }

    /// Call a tool and return the raw JSON-RPC response.
    pub async fn call_tool(
        &mut self,
        name: &str,
        arguments: serde_json::Value,
    ) -> serde_json::Value {
        self.request(
            "tools/call",
            serde_json::json!({ "name": name, "arguments": arguments }),
        )
        .await
    }
}
