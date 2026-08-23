//! Proxy MCP tool calls to the relay's HTTP endpoint.
//!
//! [`RelayProxy`] manages a single MCP session with a relay, caching
//! the session ID and tool definitions fetched at startup. Tool calls
//! are forwarded as JSON-RPC requests over HTTP and translated back
//! into [`ToolCallResult`] values.
//!
//! Signal tools (`create_flag`/`create_reply`/`close_flag`/`reopen_flag`) flow
//! through this same proxy lane — the relay authors them relay-mint with
//! `author_did` == the authenticated session DID.

use anyhow::{Context, Result};
use kutl_relay::mcp::{MCP_PROTOCOL_VERSION, ToolCallResult, ToolDefinition};
use serde::de::DeserializeOwned;
use serde_json::Value;

/// The document-change frame's method and params, taken from the relay crate
/// that writes them rather than restated here. Sharing the type is what makes a
/// renamed field a build failure on this side instead of a wake that stops
/// arriving with nothing to show for it.
pub use kutl_relay::mcp::{DOC_CHANGED_METHOD, DocumentChangedParams};

/// Extract a document-change event from one SSE event block, or `None` if the
/// block carries something else.
///
/// A block is the text between two blank lines; its `data:` lines carry the
/// JSON-RPC frame. A frame that names [`DOC_CHANGED_METHOD`] but whose params
/// do not parse is reported rather than dropped quietly — that combination
/// means a relay is sending frames this build cannot read, which otherwise
/// presents as document wakes silently ceasing.
fn parse_sse_doc_change(event_text: &str) -> Option<DocumentChangedParams> {
    for line in event_text.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let Ok(json) = serde_json::from_str::<Value>(data.trim()) else {
            continue;
        };
        if json.get("method").and_then(Value::as_str) != Some(DOC_CHANGED_METHOD) {
            continue;
        }
        let params = json.get("params").cloned().unwrap_or(Value::Null);
        match serde_json::from_value::<DocumentChangedParams>(params) {
            Ok(event) => return Some(event),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "relay document-change frame does not match the fields this server reads; \
                     document wakes will not fire"
                );
            }
        }
    }
    None
}

/// A subscription to the relay's document-change notifications.
///
/// The two halves travel together because a closed channel means two different
/// things and only [`Self::established`] tells them apart. A stream that was
/// live and then ended says the session behind it is gone — the relay reaps an
/// idle one, and every later call on that session fails the same way forever,
/// so the session has to be replaced. A stream that never opened says only
/// that this deployment does not carry SSE to this client: a gateway that
/// answers `GET /mcp` with a 404, a proxy that buffers or drops the stream, an
/// auth path that admits tool calls and refuses the stream. The tool lane is
/// working in that case, and replacing the session would abandon a healthy one
/// and mint a fresh one per request.
pub struct NotificationStream {
    /// Document changes as they arrive. Closes when the reading task stops.
    pub events: tokio::sync::mpsc::Receiver<DocumentChangedParams>,
    /// Set once the relay has accepted the stream, and never cleared.
    pub established: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
    pub fn subscribe_notifications(&self) -> NotificationStream {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let established = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let url = format!("{}/mcp", self.relay_http_url);
        let token = self.token.clone();
        let session_id = self.session_id.clone();
        let accepted = std::sync::Arc::clone(&established);

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
                    eprintln!("kutl mcp: SSE notification stream failed: {e}");
                    return;
                }
            };
            // A status this side cannot read a stream out of. reqwest reports
            // it as success and hands back a body that ends at once, so
            // without this check a refusal is indistinguishable from a stream
            // that ran and closed.
            if !resp.status().is_success() {
                eprintln!(
                    "kutl mcp: relay refused the notification stream: HTTP {}",
                    resp.status()
                );
                return;
            }
            // Recorded BEFORE the first read, and never cleared: it answers
            // "was this stream ever live", which is what tells a closed
            // channel apart from one that never opened.
            accepted.store(true, std::sync::atomic::Ordering::Release);

            let mut stream = resp.bytes_stream();
            let mut buffer = String::new();

            while let Some(chunk) = stream.next().await {
                let Ok(bytes) = chunk else { break };
                buffer.push_str(&String::from_utf8_lossy(&bytes));

                // Parse SSE events from buffer.
                while let Some(pos) = buffer.find("\n\n") {
                    let event_text = buffer[..pos].to_string();
                    buffer = buffer[pos + 2..].to_string();

                    if let Some(event) = parse_sse_doc_change(&event_text)
                        && tx.send(event).await.is_err()
                    {
                        return;
                    }
                }
            }
        });

        NotificationStream {
            events: rx,
            established,
        }
    }

    /// A proxy naming a relay nothing answers on.
    ///
    /// For exercising the session state machine, which decides what to do with
    /// a proxy without making a request through it.
    #[cfg(test)]
    pub fn detached() -> Self {
        Self {
            client: reqwest::Client::new(),
            relay_http_url: "http://127.0.0.1:1".to_owned(),
            token: String::new(),
            session_id: String::new(),
        }
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
            serde_json::from_value(result.clone()).unwrap_or_else(|e| {
                // `ToolCallResult::is_error` is `#[serde(default)]`, so this
                // does not fire for valid success payloads. If it ever does, the
                // relay's result shape drifted — surface it rather than silently
                // stringifying (which double-wraps the content).
                tracing::warn!(error = %e, "failed to deserialize relay tool result; using text fallback");
                ToolCallResult::text(result.to_string())
            })
        } else if let Some(error) = json.get("error") {
            ToolCallResult::error(error.to_string())
        } else {
            ToolCallResult::error("unexpected relay response")
        }
    }

    /// Ask the relay a question through a tool call and read its answer as `T`.
    ///
    /// Every question the CLI puts to a relay comes back in the same envelope:
    /// one text block holding JSON, or `is_error` with the relay's reason in
    /// that same block. A refusal is reported as `{refused}: {relay's words}`,
    /// and an answer this build cannot parse carries `unreadable` as its
    /// context, so both failures name the question rather than the tool.
    pub async fn call_tool_json<T: DeserializeOwned>(
        &self,
        tool_name: &str,
        arguments: &Value,
        refused: &str,
        unreadable: &str,
    ) -> Result<T> {
        let result = self.call_tool(tool_name, arguments).await;
        let body = result
            .content
            .first()
            .map(|c| c.text.as_str())
            .unwrap_or_default();
        if result.is_error {
            anyhow::bail!("{refused}: {body}");
        }
        serde_json::from_str(body).with_context(|| unreadable.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The frame as the relay writes it, spelled out rather than round-tripped
    /// through the shared type: the type binds the two ends to each other, and
    /// this binds both to the bytes a deployed relay is already sending.
    const DOC_CHANGED_FRAME: &str = concat!(
        "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/document/changed\",",
        "\"params\":{\"space_id\":\"space-x\",\"document_id\":\"doc-y\",",
        "\"author_did\":\"did:key:zAuthor\",\"intent\":\"edit\"}}"
    );

    #[test]
    fn test_parse_sse_doc_change_reads_every_field() {
        let event = parse_sse_doc_change(DOC_CHANGED_FRAME).expect("a document change");
        assert_eq!(event.space_id, "space-x");
        assert_eq!(event.document_id, "doc-y");
        assert_eq!(event.author_did, "did:key:zAuthor");
        assert_eq!(event.intent, "edit");
    }

    #[test]
    fn test_parse_sse_doc_change_ignores_other_methods() {
        let other = "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/other\",\"params\":{}}";
        assert!(parse_sse_doc_change(other).is_none());
    }

    #[test]
    fn test_parse_sse_doc_change_refuses_a_frame_missing_a_field() {
        // A required field under a name this build does not read. Yielding an
        // event with empty strings in it would wake callers with a space id no
        // read is scoped to, which is indistinguishable from the wait expiring.
        let renamed = "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/document/changed\",\
                       \"params\":{\"space\":\"space-x\",\"document_id\":\"doc-y\",\
                       \"author_did\":\"did:key:zAuthor\",\"intent\":\"edit\"}}";
        assert!(
            parse_sse_doc_change(renamed).is_none(),
            "a frame this server cannot read is not a document change"
        );
    }

    #[test]
    fn test_parse_sse_doc_change_accepts_a_frame_without_an_intent() {
        // `intent` only describes the edit. A frame without one still names a
        // real change to a real document, and refusing it would trade a missing
        // label for a missing wake.
        let no_intent = "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/document/changed\",\
                         \"params\":{\"space_id\":\"space-x\",\"document_id\":\"doc-y\",\
                         \"author_did\":\"did:key:zAuthor\"}}";
        let event = parse_sse_doc_change(no_intent).expect("a document change");
        assert_eq!(event.document_id, "doc-y");
        assert!(event.intent.is_empty());
    }

    /// Serve exactly one `GET /mcp` with `response`, then close, and hand back
    /// a proxy pointed at it. Raw bytes rather than a framework because what is
    /// under test is how this side reads a status line it did not expect.
    async fn proxy_against_one_response(response: &'static str) -> RelayProxy {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            // Read the request head so the client's write completes, then
            // answer and drop the socket.
            let mut head = [0_u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut head).await;
            let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
        });
        RelayProxy {
            client: reqwest::Client::new(),
            relay_http_url: format!("http://{addr}"),
            token: "t".to_owned(),
            session_id: "s".to_owned(),
        }
    }

    /// Wait for the stream's channel to close, which is what the serving loop
    /// selects on. Bounded so a hang fails the test rather than the run.
    async fn drain(stream: &mut NotificationStream) {
        let closed = async { while stream.events.recv().await.is_some() {} };
        tokio::time::timeout(std::time::Duration::from_secs(5), closed)
            .await
            .expect("the notification channel must close");
    }

    /// A relay that answers `GET /mcp` with a status this side cannot read a
    /// stream out of. reqwest calls that a success and hands back a body that
    /// ends at once, so the channel closes exactly as it does for a stream
    /// that ran and stopped — and the session behind it is untouched and
    /// working. Tearing it down here abandons a healthy tool lane and mints a
    /// fresh session per request.
    #[tokio::test]
    async fn test_subscribe_notifications_refused_stream_never_establishes() {
        let proxy = proxy_against_one_response(
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await;
        let mut stream = proxy.subscribe_notifications();
        drain(&mut stream).await;
        assert!(
            !stream
                .established
                .load(std::sync::atomic::Ordering::Acquire),
            "a refused stream was never live"
        );
    }

    /// The stream this side CAN read: accepted, carrying a frame, then ended.
    /// This is the reaped-session shape, and it must stay distinguishable from
    /// the refusal above or the recovery that depends on it stops firing.
    #[tokio::test]
    async fn test_subscribe_notifications_served_stream_establishes_then_ends() {
        let proxy = proxy_against_one_response(concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Type: text/event-stream\r\n",
            "Connection: close\r\n\r\n",
            "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/document/changed\",",
            "\"params\":{\"space_id\":\"space-x\",\"document_id\":\"doc-y\",",
            "\"author_did\":\"did:key:zAuthor\",\"intent\":\"edit\"}}\n\n",
        ))
        .await;
        let mut stream = proxy.subscribe_notifications();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), stream.events.recv())
            .await
            .expect("the frame must arrive")
            .expect("the frame must parse");
        assert_eq!(first.document_id, "doc-y");
        drain(&mut stream).await;
        assert!(
            stream
                .established
                .load(std::sync::atomic::Ordering::Acquire),
            "a stream the relay served was live before it ended"
        );
    }

    #[test]
    fn test_parse_sse_doc_change_tolerates_sse_comment_and_field_lines() {
        // SSE streams carry keep-alive comments and `event:` lines alongside
        // `data:`; only the latter holds the frame.
        let block = format!(": keep-alive\nevent: message\n{DOC_CHANGED_FRAME}");
        assert!(parse_sse_doc_change(&block).is_some());
    }
}
