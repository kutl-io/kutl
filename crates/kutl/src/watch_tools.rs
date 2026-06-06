//! Proxy MCP tool calls to the relay's HTTP endpoint.
//!
//! [`RelayProxy`] manages a single MCP session with a relay, caching
//! the session ID and tool definitions fetched at startup. Tool calls
//! are forwarded as JSON-RPC requests over HTTP and translated back
//! into [`ToolCallResult`] values.

use anyhow::{Context, Result};
use kutl_relay::mcp::{MCP_PROTOCOL_VERSION, ToolCallResult, ToolDefinition};
use serde_json::Value;

/// A document-change notification from the relay SSE stream.
#[derive(Debug)]
pub struct DocChangedEvent {
    /// Space the change belongs to.
    pub space_id: String,
    /// Document that changed.
    pub document_id: String,
    /// DID of the author that produced the change.
    pub author_did: String,
    /// Lifecycle intent string (`edit`, `rename`, `delete`, etc.) from the relay.
    pub intent: String,
}

/// An active relay MCP session for proxying tool calls.
pub struct RelayProxy {
    client: reqwest::Client,
    relay_http_url: String,
    token: String,
    session_id: String,
}

impl RelayProxy {
    /// Initialize a relay MCP session and fetch tool definitions.
    ///
    /// Performs the MCP `initialize` handshake followed by `tools/list`
    /// to discover the relay's available tools. Returns both the proxy
    /// and the fetched tool definitions.
    pub async fn connect(
        relay_url: &str,
        token: &str,
        did: &str,
    ) -> Result<(Self, Vec<ToolDefinition>)> {
        let client = reqwest::Client::new();
        let relay_http_url = kutl_client::relay_url_to_http(relay_url);

        // Initialize MCP session.
        let mut init_params = serde_json::json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "clientInfo": {
                "name": "kutl-watch",
                "version": env!("CARGO_PKG_VERSION")
            }
        });
        if !did.is_empty() {
            init_params["did"] = serde_json::Value::String(did.to_string());
        }

        let init_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": init_params
        });

        let resp = client
            .post(format!("{relay_http_url}/mcp"))
            .header("Authorization", format!("Bearer {token}"))
            .json(&init_body)
            .send()
            .await
            .context("failed to connect to relay MCP endpoint")?;

        let session_id = resp
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|v| {
                v.to_str()
                    .map_err(|e| {
                        tracing::warn!(error = %e, "Mcp-Session-Id header contains non-ASCII bytes");
                    })
                    .ok()
            })
            .map(String::from)
            .context("relay did not return MCP session ID")?;

        // Fetch relay tool definitions.
        let tools_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        });

        let resp = client
            .post(format!("{relay_http_url}/mcp"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Mcp-Session-Id", &session_id)
            .json(&tools_body)
            .send()
            .await
            .context("failed to fetch relay tools")?;

        let json: Value = resp.json().await.context("invalid response from relay")?;
        let tools: Vec<ToolDefinition> =
            serde_json::from_value(json["result"]["tools"].clone()).unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to parse relay tool definitions, using empty list");
                Vec::new()
            });

        let proxy = Self {
            client,
            relay_http_url,
            token: token.to_string(),
            session_id,
        };
        Ok((proxy, tools))
    }

    /// Subscribe to relay SSE notifications for document changes.
    ///
    /// Spawns a background task that connects to `GET /mcp` with the
    /// session ID and forwards `notifications/document/changed` events
    /// through the returned channel.
    pub fn subscribe_notifications(&self) -> tokio::sync::mpsc::Receiver<DocChangedEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let url = format!("{}/mcp", self.relay_http_url);
        let token = self.token.clone();
        let session_id = self.session_id.clone();

        tokio::spawn(async move {
            use futures_util::StreamExt;

            let client = reqwest::Client::new();
            let resp = match client
                .get(&url)
                .header("Authorization", format!("Bearer {token}"))
                .header("Mcp-Session-Id", &session_id)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("kutl watch: SSE notification stream failed: {e}");
                    return;
                }
            };

            let mut stream = resp.bytes_stream();
            let mut buffer = String::new();

            while let Some(chunk) = stream.next().await {
                let Ok(bytes) = chunk else { break };
                buffer.push_str(&String::from_utf8_lossy(&bytes));

                // Parse SSE events from buffer.
                while let Some(pos) = buffer.find("\n\n") {
                    let event_text = buffer[..pos].to_string();
                    buffer = buffer[pos + 2..].to_string();

                    for line in event_text.lines() {
                        if let Some(data) = line.strip_prefix("data:") {
                            let data = data.trim();
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(data)
                                && json.get("method").and_then(|m| m.as_str())
                                    == Some("notifications/document/changed")
                                && let Some(params) = json.get("params")
                            {
                                let event = DocChangedEvent {
                                    space_id: params["space_id"].as_str().unwrap_or("").to_string(),
                                    document_id: params["document_id"]
                                        .as_str()
                                        .unwrap_or("")
                                        .to_string(),
                                    author_did: params["author_did"]
                                        .as_str()
                                        .unwrap_or("")
                                        .to_string(),
                                    intent: params["intent"].as_str().unwrap_or("").to_string(),
                                };
                                if tx.send(event).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        });

        rx
    }

    /// Proxy a tool call to the relay and return the result.
    pub async fn call_tool(&self, tool_name: &str, arguments: &Value) -> ToolCallResult {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments
            }
        });

        let resp = match self
            .client
            .post(format!("{}/mcp", self.relay_http_url))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Mcp-Session-Id", &self.session_id)
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("relay request failed: {e}")),
        };

        let json: Value = match resp.json().await {
            Ok(j) => j,
            Err(e) => return ToolCallResult::error(format!("invalid relay response: {e}")),
        };

        if let Some(result) = json.get("result") {
            serde_json::from_value(result.clone())
                .unwrap_or_else(|_| ToolCallResult::text(result.to_string()))
        } else if let Some(error) = json.get("error") {
            ToolCallResult::error(error.to_string())
        } else {
            ToolCallResult::error("unexpected relay response")
        }
    }
}
