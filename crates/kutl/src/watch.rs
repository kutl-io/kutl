//! Stdio MCP server for `kutl watch`.
//!
//! Reads JSON-RPC 2.0 requests from stdin, handles them, and writes
//! responses to stdout. Pushes channel notifications from the relay
//! connection as signals (flags) arrive.

use std::collections::HashMap;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use kutl_client::SpaceConfig;
use kutl_proto::protocol::{
    decode_envelope, encode_envelope, handshake_envelope_with_token, subscribe_envelope,
};
use kutl_proto::sync::{self, sync_envelope::Payload};
use kutl_relay::mcp::{
    INVALID_PARAMS, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, MCP_PROTOCOL_VERSION,
    METHOD_NOT_FOUND, PARSE_ERROR, ToolCallParams, ToolCallResult, ToolDefinition,
};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;
use tokio_util::sync::CancellationToken;

use crate::watch_tools::RelayProxy;

/// Capacity of the internal notification channel.
const NOTIFY_CHANNEL_CAPACITY: usize = 64;

/// Document ID used to anchor the watch subscription in the relay.
///
/// The watch command subscribes to this sentinel document so that it appears
/// as a space subscriber and receives space-wide signal broadcasts.
const WATCH_SENTINEL_DOC_ID: &str = "__kutl_watch";

/// Delay between relay reconnection attempts.
const RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// Maximum consecutive reconnection failures before giving up.
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// Initial retry delay for relay proxy reconnection.
const PROXY_INITIAL_RETRY: std::time::Duration = std::time::Duration::from_secs(5);

/// Maximum retry delay for relay proxy (exponential backoff cap).
const PROXY_MAX_RETRY: std::time::Duration = std::time::Duration::from_mins(5);

/// Number of initial connection attempts when resolving the relay proxy at startup.
const INITIAL_PROXY_CONNECT_ATTEMPTS: u32 = 3;

/// Delay between initial relay proxy connection attempts at startup.
const INITIAL_PROXY_CONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Instructions text sent to the agent during MCP initialization.
const SERVER_INSTRUCTIONS: &str = "\
kutl is a collaborative document editor. Participants edit markdown documents \
together in real-time within shared spaces. Changes are synced via CRDT, so \
edits never conflict.\n\n\
## How to discover work\n\n\
There are two ways you receive work:\n\
- **Channel events (push):** When someone flags you, the flag arrives as a \
  channel event. The event content is the flag message; the metadata includes \
  `space_id` (canonical UUID, use this for `get_changes` and other tool \
  calls), `space_name` (human-readable), `kind` (info/question/etc.), \
  `document` (the document id), and `sender` (the author's DID).\n\
- **Polling (pull):** Call `get_changes` with a space_id to check for new \
  signals and document changes since your last check. Both new flags and \
  replies to flags you participated in appear in the results.\n\n\
If you are not yet subscribed to a space, call `subscribe_space` first. If you \
already know your space_id, you can call `get_changes` directly.\n\n\
## Decision syntax\n\n\
Documents use heading-annotated decisions:\n\
- `## ? Open question` — an unresolved question that needs a decision\n\
- `## = Settled answer` — a resolved decision\n\
Sub-decisions use deeper heading levels (e.g. `### ?` under a `## ?` parent).\n\n\
Open questions can be resolved by adding `## =` headings.\n\n\
## Working in documents\n\n\
Use `read_document` and `edit_document` to work in documents. When editing, the \
relay diffs your content against the current state and applies minimal CRDT \
operations — you provide the full desired content, not a patch. Provide a short \
`intent` describing what your edit does.\n\n\
Provide a full desired content, not a patch — the relay computes the minimal diff.";

/// A channel notification to push to the connected agent.
pub struct ChannelEvent {
    /// Human-readable event content.
    pub content: String,
    /// Structured metadata about the event.
    pub meta: serde_json::Map<String, Value>,
}

/// A subscription to a single space on a relay.
///
/// Holds the cancellation token used to shut down the relay listener
/// task when the space is unsubscribed.
struct SpaceSubscription {
    /// Token to cancel the relay listener task.
    cancel: CancellationToken,
}

/// Mutable session state shared between the main loop and tool handlers.
struct WatchState {
    /// Active relay subscriptions, keyed by space name or space ID.
    subscriptions: HashMap<String, SpaceSubscription>,
    /// Agent DID for flag targeting and filtering.
    did: String,
    /// Auth token for relay handshakes.
    auth_token: String,
    /// Sender for pushing channel events to stdout.
    notify_tx: mpsc::Sender<ChannelEvent>,
}

/// Startup state returned by [`setup`].
struct SetupResult {
    state: WatchState,
    local_space: Option<(SpaceConfig, std::path::PathBuf)>,
    relay_proxy: Option<RelayProxy>,
    relay_tools: Vec<ToolDefinition>,
    auth_token: String,
    notify_rx: mpsc::Receiver<ChannelEvent>,
    doc_change_rx: Option<mpsc::Receiver<crate::watch_tools::DocChangedEvent>>,
}

/// Discover space, connect relay, subscribe to events.
async fn setup(poll_only: bool) -> Result<SetupResult> {
    let (did, auth_token) = load_credentials();

    let (notify_tx, notify_rx) = mpsc::channel::<ChannelEvent>(NOTIFY_CHANNEL_CAPACITY);

    let search_root = std::env::var("KUTL_HOME").map_or_else(
        |_| std::env::current_dir().expect("failed to get working directory"),
        std::path::PathBuf::from,
    );
    let local_space = kutl_client::space_config::discover_space(&search_root);

    let mut state = WatchState {
        subscriptions: HashMap::new(),
        did: did.clone(),
        auth_token: auth_token.clone(),
        notify_tx,
    };

    if let Some((ref config, ref space_root)) = local_space {
        let display_name = kutl_client::KutlspaceConfig::display_name(space_root, &config.space_id);
        eprintln!(
            "kutl watch: auto-subscribed to space {} ({})",
            display_name, config.space_id
        );

        let cancel = CancellationToken::new();
        if !poll_only {
            spawn_relay_listener(
                config,
                space_root,
                &did,
                &auth_token,
                state.notify_tx.clone(),
                cancel.clone(),
            );
        }

        state
            .subscriptions
            .insert(config.space_id.clone(), SpaceSubscription { cancel });
    } else {
        eprintln!("kutl watch: no local space found. Use list_spaces and subscribe_space tools.");
    }

    let local_space_ref = local_space.as_ref().map(|(c, _)| c);

    let (relay_proxy, relay_tools) =
        connect_relay_proxy_with_retry(local_space_ref, &auth_token, &did).await;

    if local_space_ref.is_some() && relay_proxy.is_none() {
        anyhow::bail!(
            "failed to connect to relay MCP endpoint after 3 attempts. \
             check that the relay is running and reachable"
        );
    }

    // Subscribe to relay SSE notifications for document changes.
    // Skipped in poll-only mode — agents discover changes via get_changes polling.
    let doc_change_rx = if poll_only {
        None
    } else {
        relay_proxy
            .as_ref()
            .map(RelayProxy::subscribe_notifications)
    };

    Ok(SetupResult {
        state,
        local_space,
        relay_proxy,
        relay_tools,
        auth_token,
        notify_rx,
        doc_change_rx,
    })
}

/// Receive from an optional doc-change channel (for `tokio::select!`).
async fn recv_doc_change(
    rx: &mut Option<mpsc::Receiver<crate::watch_tools::DocChangedEvent>>,
) -> Option<crate::watch_tools::DocChangedEvent> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Serialize `value` to JSON and write it to `stdout` as a newline-delimited
/// frame, then flush.
async fn write_json_frame<T: serde::Serialize>(
    stdout: &mut tokio::io::Stdout,
    value: &T,
    ctx: &'static str,
) -> Result<()> {
    let json = serde_json::to_string(value).context(ctx)?;
    stdout.write_all(json.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}

/// Run the kutl watch MCP server.
pub async fn run(poll_only: bool) -> Result<()> {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    let mut lines = stdin.lines();

    let SetupResult {
        mut state,
        local_space: local_space_with_root,
        mut relay_proxy,
        mut relay_tools,
        auth_token,
        mut notify_rx,
        doc_change_rx,
    } = setup(poll_only).await?;

    // Wrap the optional doc-change receiver so tokio::select! can await it.
    let mut doc_change_rx = doc_change_rx;

    let local_space_ref = local_space_with_root.as_ref().map(|(c, _)| c);

    let mut next_retry_at: Option<tokio::time::Instant> = if relay_proxy.is_none() {
        Some(tokio::time::Instant::now() + PROXY_INITIAL_RETRY)
    } else {
        None
    };
    let mut retry_delay = PROXY_INITIAL_RETRY;

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line.context("stdin read error")? else {
                    break; // EOF — the agent closed the session
                };
                if line.trim().is_empty() {
                    continue;
                }
                maybe_reconnect_proxy(
                    &mut relay_proxy, &mut relay_tools,
                    &mut next_retry_at, &mut retry_delay,
                    local_space_ref, &auth_token, &state.did,
                ).await;
                let response = handle_request(
                    &line,
                    &relay_tools,
                    relay_proxy.as_ref(),
                    Some(&mut state),
                ).await;
                if let Some(resp) = response {
                    write_json_frame(&mut stdout, &resp, "failed to serialize response").await?;
                }
            }
            Some(event) = notify_rx.recv() => {
                let notification = JsonRpcNotification {
                    jsonrpc: "2.0".into(),
                    method: "notifications/claude/channel".into(),
                    params: serde_json::json!({
                        "content": event.content,
                        "meta": event.meta,
                    }),
                };
                write_json_frame(&mut stdout, &notification, "failed to serialize notification").await?;
            }
            Some(doc_event) = recv_doc_change(&mut doc_change_rx) => {
                // Forward document change as a channel event so agents
                // know when others have edited a document.
                let notification = JsonRpcNotification {
                    jsonrpc: "2.0".into(),
                    method: "notifications/claude/channel".into(),
                    params: serde_json::json!({
                        "content": format!(
                            "{} edited {} ({})",
                            doc_event.author_did.rsplit(':').next().unwrap_or("?"),
                            doc_event.document_id,
                            doc_event.intent
                        ),
                        "meta": serde_json::json!({
                            "type": "document_changed",
                            "space_id": doc_event.space_id,
                            "document_id": doc_event.document_id,
                            "author_did": doc_event.author_did,
                            "intent": doc_event.intent,
                        }),
                    }),
                };
                write_json_frame(&mut stdout, &notification, "failed to serialize doc-change notification").await?;
            }
        }
    }

    // Cancel all relay listener tasks on exit.
    for (_, sub) in state.subscriptions {
        sub.cancel.cancel();
    }

    Ok(())
}

/// Load the agent's DID and auth token.
///
/// Returns `(did, auth_token)` where either may be empty if not
/// configured.
fn load_credentials() -> (String, String) {
    let did = if let Some(id) = kutl_client::identity::default_identity_path()
        .ok()
        .and_then(|p| kutl_client::identity::Identity::load(&p).ok())
    {
        eprintln!("kutl watch: identity loaded ({})", id.did);
        id.did
    } else {
        eprintln!("kutl watch: no identity found, flag targeting will be limited");
        String::new()
    };

    let auth_token = kutl_client::credentials::resolve_token(
        kutl_client::credentials::default_credentials_path()
            .map_err(|e| {
                eprintln!("kutl watch: failed to resolve credentials path: {e}");
            })
            .ok()
            .as_deref(),
    )
    .unwrap_or_default();

    (did, auth_token)
}

/// Connect a relay proxy for tool dispatch, if a local space is available.
///
/// Returns `(Some(proxy), tools)` on success, `(None, [])` if no space
/// is configured or the relay is unreachable.
async fn connect_relay_proxy(
    local_space: Option<&SpaceConfig>,
    auth_token: &str,
    did: &str,
) -> (Option<RelayProxy>, Vec<ToolDefinition>) {
    let Some(config) = local_space else {
        return (None, Vec::new());
    };

    match RelayProxy::connect(&config.relay_url, auth_token, did).await {
        Ok((proxy, tools)) => {
            eprintln!(
                "kutl watch: relay proxy connected ({} tools available)",
                tools.len()
            );
            (Some(proxy), tools)
        }
        Err(e) => {
            eprintln!(
                "kutl watch: relay proxy unavailable: {e:#}. \
                 Relay tools (get_changes, read_document, etc.) will not be \
                 available. Will retry on next tool call."
            );
            (None, Vec::new())
        }
    }
}

/// Connect the relay proxy with startup retries.
///
/// The relay may still be initializing when `kutl watch` starts. Retry up
/// to 3 times with a 2-second delay to avoid agents starting with zero tools.
async fn connect_relay_proxy_with_retry(
    local_space: Option<&SpaceConfig>,
    auth_token: &str,
    did: &str,
) -> (Option<RelayProxy>, Vec<ToolDefinition>) {
    let mut result = connect_relay_proxy(local_space, auth_token, did).await;
    for attempt in 1..=INITIAL_PROXY_CONNECT_ATTEMPTS {
        if result.0.is_some() {
            break;
        }
        eprintln!(
            "kutl watch: relay proxy not ready, retry {attempt}/{max} in {delay}s...",
            max = INITIAL_PROXY_CONNECT_ATTEMPTS,
            delay = INITIAL_PROXY_CONNECT_DELAY.as_secs(),
        );
        tokio::time::sleep(INITIAL_PROXY_CONNECT_DELAY).await;
        result = connect_relay_proxy(local_space, auth_token, did).await;
    }
    result
}

/// Attempt relay proxy reconnection if disconnected and backoff has elapsed.
///
/// Uses exponential backoff (5s -> 10s -> 20s -> ... -> 5min cap) to avoid
/// flooding the relay with connection attempts.
async fn maybe_reconnect_proxy(
    relay_proxy: &mut Option<RelayProxy>,
    relay_tools: &mut Vec<ToolDefinition>,
    next_retry_at: &mut Option<tokio::time::Instant>,
    retry_delay: &mut std::time::Duration,
    local_space: Option<&SpaceConfig>,
    auth_token: &str,
    did: &str,
) {
    if relay_proxy.is_some() {
        return;
    }
    let Some(retry_time) = *next_retry_at else {
        return;
    };
    if tokio::time::Instant::now() < retry_time {
        return;
    }

    let (new_proxy, new_tools) = connect_relay_proxy(local_space, auth_token, did).await;
    if new_proxy.is_some() {
        *relay_proxy = new_proxy;
        *relay_tools = new_tools;
        *next_retry_at = None;
        *retry_delay = PROXY_INITIAL_RETRY;
    } else {
        *retry_delay = (*retry_delay * 2).min(PROXY_MAX_RETRY);
        *next_retry_at = Some(tokio::time::Instant::now() + *retry_delay);
    }
}

// ---------------------------------------------------------------------------
// Watch-local tool handlers
// ---------------------------------------------------------------------------

/// Spawn a relay listener task for the given space config.
///
/// Loads `.kutlspace` from `space_root` once at startup to resolve a
/// human-readable display name for MCP notifications. Falls back to
/// `space_id` when `.kutlspace` is absent.
fn spawn_relay_listener(
    config: &SpaceConfig,
    space_root: &std::path::Path,
    did: &str,
    auth_token: &str,
    notify_tx: mpsc::Sender<ChannelEvent>,
    cancel: CancellationToken,
) {
    let display_name = kutl_client::KutlspaceConfig::display_name(space_root, &config.space_id);
    tokio::spawn(relay_listener(
        config.relay_url.clone(),
        config.space_id.clone(),
        auth_token.to_owned(),
        did.to_owned(),
        display_name,
        notify_tx,
        cancel,
    ));
}

/// Search the global space registry for a space matching `space_name_or_id`
/// (matching against either `space_id` from `.kutl/space.json` or `space_name`
/// from `.kutlspace`).
fn resolve_space_config(space_name_or_id: &str) -> Option<(SpaceConfig, std::path::PathBuf)> {
    let path = kutl_client::space_registry::registry_path()
        .map_err(|e| {
            eprintln!("kutl watch: failed to resolve spaces.json path: {e}");
        })
        .ok()?;
    let registry = kutl_client::space_registry::SpaceRegistry::load(&path)
        .map_err(|e| {
            eprintln!("kutl watch: failed to load space registry: {e}");
        })
        .ok()?;
    for space_path in &registry.spaces {
        let root = std::path::Path::new(space_path);
        let Ok(config) = SpaceConfig::load(root) else {
            continue;
        };
        if config.space_id == space_name_or_id {
            return Some((config, root.to_path_buf()));
        }
        if let Ok(Some(ks)) = kutl_client::KutlspaceConfig::load(root)
            && ks.space_name == space_name_or_id
        {
            return Some((config, root.to_path_buf()));
        }
    }
    None
}

/// Dispatch a watch-local tool call, optionally with mutable session state.
///
/// When `state` is `None` (unit tests), subscription-mutating tools return
/// an error indicating state is unavailable.
fn handle_local_tool(name: &str, args: &Value, state: Option<&mut WatchState>) -> ToolCallResult {
    match name {
        "list_spaces" => handle_list_spaces(),
        "subscribe_space" => {
            let space = args
                .get("space")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if space.is_empty() {
                return ToolCallResult::error("missing required argument: space");
            }
            match state {
                Some(s) => handle_subscribe_space(s, space),
                None => ToolCallResult::error("state unavailable in test context"),
            }
        }
        "unsubscribe_space" => {
            let space = args
                .get("space")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if space.is_empty() {
                return ToolCallResult::error("missing required argument: space");
            }
            match state {
                Some(s) => handle_unsubscribe_space(s, space),
                None => ToolCallResult::error("state unavailable in test context"),
            }
        }
        _ => ToolCallResult::error(format!("unknown local tool: {name}")),
    }
}

/// List all spaces registered in `$KUTL_HOME/spaces.json`.
fn handle_list_spaces() -> ToolCallResult {
    let registry_path = match kutl_client::space_registry::registry_path() {
        Ok(p) => p,
        Err(e) => return ToolCallResult::error(format!("failed to find space registry: {e}")),
    };

    let registry = match kutl_client::space_registry::SpaceRegistry::load(&registry_path) {
        Ok(r) => r,
        Err(e) => return ToolCallResult::error(format!("failed to load space registry: {e}")),
    };

    let spaces: Vec<Value> = registry
        .spaces
        .iter()
        .filter_map(|path| {
            let root = std::path::Path::new(path);
            let config = SpaceConfig::load(root).ok()?;
            let space_name = kutl_client::KutlspaceConfig::display_name(root, &config.space_id);
            Some(serde_json::json!({
                "space_name": space_name,
                "space_id": config.space_id,
                "path": path,
            }))
        })
        .collect();

    ToolCallResult::text(
        serde_json::to_string_pretty(&spaces)
            .unwrap_or_else(|e| format!("failed to serialize space list: {e}")),
    )
}

/// Subscribe to a space by name or ID, spawning a relay listener.
fn handle_subscribe_space(state: &mut WatchState, space: &str) -> ToolCallResult {
    if state.subscriptions.contains_key(space) {
        return ToolCallResult::text(format!("already subscribed to {space}"));
    }

    let Some((config, root)) = resolve_space_config(space) else {
        return ToolCallResult::error(format!("space {space} not found in local registry"));
    };

    let display_name = kutl_client::KutlspaceConfig::display_name(&root, &config.space_id);
    eprintln!(
        "kutl watch: subscribed to space {} ({})",
        display_name, config.space_id
    );

    let cancel = CancellationToken::new();
    spawn_relay_listener(
        &config,
        &root,
        &state.did,
        &state.auth_token,
        state.notify_tx.clone(),
        cancel.clone(),
    );

    state
        .subscriptions
        .insert(space.to_string(), SpaceSubscription { cancel });

    ToolCallResult::text(format!("subscribed to {space}"))
}

/// Cancel and remove a space subscription.
fn handle_unsubscribe_space(state: &mut WatchState, space: &str) -> ToolCallResult {
    match state.subscriptions.remove(space) {
        Some(sub) => {
            sub.cancel.cancel();
            ToolCallResult::text(format!("unsubscribed from {space}"))
        }
        None => ToolCallResult::error(format!("not subscribed to {space}")),
    }
}

/// Parse and dispatch a single JSON-RPC request line.
///
/// Returns `None` for notifications (no id), `Some(response)` for
/// requests that require a reply. Tool calls to relay-provided tools
/// are proxied through `relay_proxy` when available. `state` is
/// `Some` when called from the live server loop; `None` in unit tests.
async fn handle_request(
    line: &str,
    relay_tools: &[ToolDefinition],
    relay_proxy: Option<&RelayProxy>,
    state: Option<&mut WatchState>,
) -> Option<JsonRpcResponse> {
    let req: JsonRpcRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            return Some(JsonRpcResponse::error(
                Value::Null,
                PARSE_ERROR,
                format!("invalid JSON: {e}"),
            ));
        }
    };

    let id = req.id.clone().unwrap_or(Value::Null);

    match req.method.as_str() {
        "initialize" => Some(handle_initialize(id)),
        "notifications/initialized" => None,
        "ping" => Some(JsonRpcResponse::success(id, serde_json::json!({}))),
        "tools/list" => Some(handle_tools_list(id, relay_tools)),
        "tools/call" => {
            Some(handle_tools_call(id, &req.params, relay_tools, relay_proxy, state).await)
        }
        _ => Some(JsonRpcResponse::error(
            id,
            METHOD_NOT_FOUND,
            "method not found",
        )),
    }
}

/// Handle the `initialize` handshake.
///
/// Returns server info with `tools` and `claude/channel` capabilities,
/// plus instructions guiding the agent on how to respond to events.
fn handle_initialize(id: Value) -> JsonRpcResponse {
    let result = serde_json::json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": {
            "tools": {},
            "experimental": {
                "claude/channel": {}
            }
        },
        "serverInfo": {
            "name": "kutl-watch",
            "version": env!("CARGO_PKG_VERSION")
        },
        "instructions": SERVER_INSTRUCTIONS
    });
    JsonRpcResponse::success(id, result)
}

/// Handle `tools/list` — return watch-local and relay-proxied tool definitions.
fn handle_tools_list(id: Value, relay_tools: &[ToolDefinition]) -> JsonRpcResponse {
    let mut tools = watch_tool_definitions();
    tools.extend(relay_tools.iter().cloned());
    JsonRpcResponse::success(id, serde_json::json!({ "tools": tools }))
}

/// Watch-local tool names that are handled directly, not proxied.
const WATCH_LOCAL_TOOLS: &[&str] = &["list_spaces", "subscribe_space", "unsubscribe_space"];

/// Handle `tools/call` — dispatch to local handler or relay proxy.
async fn handle_tools_call(
    id: Value,
    params: &Value,
    relay_tools: &[ToolDefinition],
    relay_proxy: Option<&RelayProxy>,
    state: Option<&mut WatchState>,
) -> JsonRpcResponse {
    let call: ToolCallParams = match serde_json::from_value(params.clone()) {
        Ok(c) => c,
        Err(e) => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                format!("invalid tool call params: {e}"),
            );
        }
    };

    let result = if WATCH_LOCAL_TOOLS.contains(&call.name.as_str()) {
        handle_local_tool(&call.name, &call.arguments, state)
    } else if relay_tools.iter().any(|t| t.name == call.name) {
        // Known relay tool — proxy through the relay.
        if let Some(proxy) = relay_proxy {
            proxy.call_tool(&call.name, &call.arguments).await
        } else {
            ToolCallResult::error(
                "not connected to the relay. The relay may be unreachable or \
                 authentication may have failed. Ask the user to verify: \
                 (1) the relay is running, \
                 (2) credentials are valid (run `kutl auth login` to refresh), \
                 (3) the space is configured correctly in .kutl/space.json",
            )
        }
    } else {
        ToolCallResult::error(format!("unknown tool: {}", call.name))
    };

    JsonRpcResponse::success(
        id,
        serde_json::to_value(&result).expect("ToolCallResult is always serializable"),
    )
}

/// Tool definitions for watch-specific tools.
///
/// These are the local tools that `kutl watch` provides directly.
/// Relay-proxied tools are fetched dynamically and merged in
/// [`handle_tools_list`].
fn watch_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "list_spaces".into(),
            description: "List your available kutl spaces.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "subscribe_space".into(),
            description: "Subscribe to a space for the current session. Flags \
                          targeting you will be pushed as channel events."
                .into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "space": {
                        "type": "string",
                        "description": "Space identifier (owner/name or space_id)."
                    }
                },
                "required": ["space"]
            }),
        },
        ToolDefinition {
            name: "unsubscribe_space".into(),
            description: "Stop watching a space.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "space": {
                        "type": "string",
                        "description": "Space identifier to unsubscribe from."
                    }
                },
                "required": ["space"]
            }),
        },
    ]
}

// ---------------------------------------------------------------------------
// Space discovery
// ---------------------------------------------------------------------------

// Space discovery via kutl_client::space_config::discover_space.

// ---------------------------------------------------------------------------
// Relay WebSocket listener
// ---------------------------------------------------------------------------

/// Long-running relay listener task.
///
/// Connects to the relay, subscribes to a sentinel document to receive
/// space-wide signal broadcasts, and forwards matching flag events
/// through `notify_tx`. Reconnects automatically on disconnect.
async fn relay_listener(
    relay_url: String,
    space_id: String,
    auth_token: String,
    agent_did: String,
    display_name: String,
    notify_tx: mpsc::Sender<ChannelEvent>,
    cancel: CancellationToken,
) {
    let mut attempts: u32 = 0;

    loop {
        if cancel.is_cancelled() {
            return;
        }

        match relay_session(
            &relay_url,
            &space_id,
            &auth_token,
            &agent_did,
            &display_name,
            &notify_tx,
            &cancel,
        )
        .await
        {
            Ok(()) => {
                // Clean disconnect (cancel or EOF).
                return;
            }
            Err(e) => {
                attempts += 1;
                eprintln!(
                    "kutl watch: relay connection lost ({e:#}), attempt {attempts}/{MAX_RECONNECT_ATTEMPTS}"
                );
                if attempts >= MAX_RECONNECT_ATTEMPTS {
                    eprintln!(
                        "kutl watch: giving up on relay connection after {attempts} attempts"
                    );
                    return;
                }
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(RECONNECT_DELAY) => {}
                }
            }
        }
    }
}

/// Run a single relay session: connect, handshake, subscribe, read loop.
///
/// Returns `Ok(())` on clean shutdown (cancellation), `Err` on connection
/// failures that should trigger reconnection.
async fn relay_session(
    relay_url: &str,
    space_id: &str,
    auth_token: &str,
    agent_did: &str,
    display_name: &str,
    notify_tx: &mpsc::Sender<ChannelEvent>,
    cancel: &CancellationToken,
) -> Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(relay_url)
        .await
        .context("failed to connect to relay")?;

    let (mut ws_sink, mut ws_stream) = ws.split();

    // Handshake with auth token.
    let hs = handshake_envelope_with_token("kutl-watch", auth_token, "");
    let hs_bytes = encode_envelope(&hs);
    ws_sink
        .send(tungstenite::Message::Binary(hs_bytes.into()))
        .await
        .context("failed to send handshake")?;

    // Wait for HandshakeAck.
    let ack_msg = ws_stream
        .next()
        .await
        .context("connection closed before handshake ack")?
        .context("ws error during handshake")?;

    match ack_msg {
        tungstenite::Message::Binary(bytes) => {
            let envelope = decode_envelope(&bytes).context("failed to decode handshake ack")?;
            match envelope.payload {
                Some(Payload::HandshakeAck(_)) => {}
                Some(Payload::Error(e)) => {
                    anyhow::bail!("handshake rejected: {}", e.message);
                }
                other => {
                    anyhow::bail!("unexpected handshake response: {other:?}");
                }
            }
        }
        other => {
            anyhow::bail!("expected binary handshake ack, got {other:?}");
        }
    }

    eprintln!("kutl watch: connected to relay at {relay_url}");

    // Subscribe to sentinel document to join the space subscriber set.
    let sub = subscribe_envelope(space_id, WATCH_SENTINEL_DOC_ID);
    let sub_bytes = encode_envelope(&sub);
    ws_sink
        .send(tungstenite::Message::Binary(sub_bytes.into()))
        .await
        .context("failed to send subscribe")?;

    // Read loop — process incoming envelopes.
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            msg = ws_stream.next() => {
                let Some(msg) = msg else {
                    anyhow::bail!("relay connection closed");
                };
                let msg = msg.context("ws read error")?;

                let bytes = match msg {
                    tungstenite::Message::Binary(b) => b,
                    tungstenite::Message::Close(_) => {
                        anyhow::bail!("relay sent close frame");
                    }
                    _ => continue,
                };

                let envelope = match decode_envelope(&bytes) {
                    Ok(e) => e,
                    Err(e) => {
                        eprintln!("kutl watch: failed to decode envelope: {e:#}");
                        continue;
                    }
                };

                if let Some(Payload::Signal(ref signal)) = envelope.payload
                    && let Some(sync::signal::Payload::Flag(ref flag)) = signal.payload
                    && should_deliver_flag(signal, flag, agent_did)
                {
                    let event = build_flag_channel_event(signal, flag, space_id, display_name);
                    if let Err(e) = notify_tx.try_send(event) {
                        eprintln!("kutl watch: dropped flag event (channel full): {e}");
                    }
                }
                // All other payloads (SyncOps, StaleSubscriber, etc.) are
                // silently ignored — the watch command only cares about
                // flags.
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Flag filtering
// ---------------------------------------------------------------------------

/// Determine whether an incoming flag should be delivered to this agent.
///
/// Filters out self-authored messages and restricts `Participant` audience
/// to messages targeting this agent's DID.
fn should_deliver_flag(signal: &sync::Signal, flag: &sync::FlagPayload, agent_did: &str) -> bool {
    // Never echo back our own messages.
    if !agent_did.is_empty() && signal.author_did == agent_did {
        return false;
    }

    let audience = sync::AudienceType::try_from(flag.audience_type).unwrap_or_else(|_| {
        eprintln!(
            "kutl watch: unknown audience type {}, treating as unspecified",
            flag.audience_type
        );
        sync::AudienceType::Unspecified
    });

    match audience {
        sync::AudienceType::Participant => {
            // Direct message — only deliver if we are the target.
            let target = flag.target_did.as_deref().unwrap_or("");
            !agent_did.is_empty() && target == agent_did
        }
        sync::AudienceType::Space
        | sync::AudienceType::AgentOwners
        | sync::AudienceType::AgentEditors
        | sync::AudienceType::AgentViewers => true,
        // HumanOwners, HumanEditors, HumanViewers, Unspecified — not for agents.
        _ => false,
    }
}

/// Build a [`ChannelEvent`] from a flag signal.
///
/// The metadata always includes both `space_id` (canonical UUID, stable
/// across renames, suitable for programmatic lookup) and `space_name`
/// (human-readable, may change). Consumers should display `space_name` and
/// reference `space_id` for any programmatic action.
fn build_flag_channel_event(
    signal: &sync::Signal,
    flag: &sync::FlagPayload,
    space_id: &str,
    space_name: &str,
) -> ChannelEvent {
    let mut meta = serde_json::Map::new();
    meta.insert("space_id".into(), Value::String(space_id.to_owned()));
    meta.insert("space_name".into(), Value::String(space_name.to_owned()));
    meta.insert("kind".into(), Value::String(flag_kind_name(flag.kind)));
    meta.insert(
        "document".into(),
        Value::String(signal.document_id.clone().unwrap_or_default()),
    );
    meta.insert("sender".into(), Value::String(signal.author_did.clone()));

    ChannelEvent {
        content: flag.message.clone(),
        meta,
    }
}

/// Convert a `FlagKind` i32 to a human-readable string.
fn flag_kind_name(kind: i32) -> String {
    match sync::FlagKind::try_from(kind) {
        Ok(sync::FlagKind::Info) => "info",
        Ok(sync::FlagKind::Completed) => "completed",
        Ok(sync::FlagKind::ReviewRequested) => "review_requested",
        Ok(sync::FlagKind::Question) => "question",
        Ok(sync::FlagKind::Blocked) => "blocked",
        _ => "unknown",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No relay tools, no proxy, no state — the default test scenario.
    async fn test_handle(line: &str) -> Option<JsonRpcResponse> {
        handle_request(line, &[], None, None).await
    }

    #[tokio::test]
    async fn test_handle_request_initialize() {
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","clientInfo":{"name":"test","version":"1.0"}}}"#;
        let resp = test_handle(req)
            .await
            .expect("initialize should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert_eq!(result["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert!(result["capabilities"]["tools"].is_object());
        assert!(result["capabilities"]["experimental"]["claude/channel"].is_object());
        assert_eq!(result["serverInfo"]["name"], "kutl-watch");
        assert!(result["instructions"].is_string());
    }

    #[tokio::test]
    async fn test_handle_request_notification_returns_none() {
        let req = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        assert!(test_handle(req).await.is_none());
    }

    #[tokio::test]
    async fn test_handle_request_ping() {
        let req = r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#;
        let resp = test_handle(req)
            .await
            .expect("ping should return a response");
        assert!(resp.error.is_none());
        assert_eq!(resp.result, Some(serde_json::json!({})));
    }

    #[tokio::test]
    async fn test_handle_request_tools_list_no_relay() {
        let req = r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/list should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        let tools = result["tools"]
            .as_array()
            .expect("tools should be an array");
        assert_eq!(tools.len(), 3);

        let names: Vec<&str> = tools
            .iter()
            .map(|t| t["name"].as_str().expect("tool should have a name"))
            .collect();
        assert!(names.contains(&"list_spaces"));
        assert!(names.contains(&"subscribe_space"));
        assert!(names.contains(&"unsubscribe_space"));
    }

    #[tokio::test]
    async fn test_handle_request_tools_list_with_relay_tools() {
        let relay_tools = vec![ToolDefinition {
            name: "read_document".into(),
            description: "Read a document.".into(),
            input_schema: serde_json::json!({"type": "object", "properties": {}}),
        }];
        let req = r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#;
        let resp = handle_request(req, &relay_tools, None, None)
            .await
            .expect("tools/list should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        let tools = result["tools"]
            .as_array()
            .expect("tools should be an array");
        // 3 watch-local + 1 relay tool
        assert_eq!(tools.len(), 4);

        let names: Vec<&str> = tools
            .iter()
            .map(|t| t["name"].as_str().expect("tool should have a name"))
            .collect();
        assert!(names.contains(&"read_document"));
    }

    #[tokio::test]
    async fn test_handle_request_list_spaces() {
        // list_spaces does not require state — it reads the global registry.
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_spaces","arguments":{}}}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        // Either succeeds with a JSON array, or fails because $HOME is unavailable
        // in the test environment — both are acceptable non-panic outcomes.
        assert!(resp.result.is_some());
    }

    #[tokio::test]
    async fn test_handle_request_subscribe_space_no_state() {
        // subscribe_space without state returns an informative error.
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"subscribe_space","arguments":{"space":"my-space"}}}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert!(result["isError"].as_bool().expect("should have isError"));
        assert!(
            result["content"][0]["text"]
                .as_str()
                .expect("should have text")
                .contains("state unavailable")
        );
    }

    #[tokio::test]
    async fn test_handle_request_unsubscribe_space_no_state() {
        // unsubscribe_space without state returns an informative error.
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"unsubscribe_space","arguments":{"space":"my-space"}}}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert!(result["isError"].as_bool().expect("should have isError"));
        assert!(
            result["content"][0]["text"]
                .as_str()
                .expect("should have text")
                .contains("state unavailable")
        );
    }

    #[tokio::test]
    async fn test_handle_request_unknown_tool() {
        let req = r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"bogus","arguments":{}}}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert!(result["isError"].as_bool().expect("should have isError"));
        assert!(
            result["content"][0]["text"]
                .as_str()
                .expect("should have text")
                .contains("unknown tool")
        );
    }

    #[tokio::test]
    async fn test_handle_request_relay_tool_without_proxy() {
        let relay_tools = vec![ToolDefinition {
            name: "read_document".into(),
            description: "Read a document.".into(),
            input_schema: serde_json::json!({"type": "object", "properties": {}}),
        }];
        let req = r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"read_document","arguments":{}}}"#;
        let resp = handle_request(req, &relay_tools, None, None)
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert!(result["isError"].as_bool().expect("should have isError"));
        assert!(
            result["content"][0]["text"]
                .as_str()
                .expect("should have text")
                .contains("not connected to the relay")
        );
    }

    #[tokio::test]
    async fn test_handle_request_unknown_method() {
        let req = r#"{"jsonrpc":"2.0","id":6,"method":"bogus/method"}"#;
        let resp = test_handle(req)
            .await
            .expect("unknown method should return error");
        assert!(resp.error.is_some());
        let err = resp.error.expect("should have error");
        assert_eq!(err.code, METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn test_handle_request_invalid_json() {
        let resp = test_handle("not json")
            .await
            .expect("invalid JSON should return error");
        assert!(resp.error.is_some());
        let err = resp.error.expect("should have error");
        assert_eq!(err.code, PARSE_ERROR);
    }

    #[tokio::test]
    async fn test_handle_request_invalid_tool_call_params() {
        let req = r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":"bad"}"#;
        let resp = test_handle(req)
            .await
            .expect("bad params should return error");
        assert!(resp.error.is_some());
        let err = resp.error.expect("should have error");
        assert_eq!(err.code, INVALID_PARAMS);
    }

    // --- Space discovery tests ---

    #[test]
    fn test_discover_space_found() {
        let dir = tempfile::TempDir::new().unwrap();
        let space_root = dir.path();
        let config = SpaceConfig {
            space_id: "test-space".into(),
            relay_url: "ws://localhost:9100/ws".into(),
        };
        config.save(space_root).unwrap();

        let found = kutl_client::space_config::discover_space(space_root);
        assert!(found.is_some());
        let (found_config, found_root) = found.unwrap();
        assert_eq!(found_config.space_id, "test-space");
        assert_eq!(found_root, space_root);
    }

    #[test]
    fn test_discover_space_no_walk_up() {
        // discover_space should NOT walk up to ancestor directories.
        // This prevents finding unrelated spaces when KUTL_HOME is nested
        // inside a workspace tree.
        let dir = tempfile::TempDir::new().unwrap();
        let space_root = dir.path();
        let config = SpaceConfig {
            space_id: "test-space".into(),
            relay_url: "ws://localhost:9100/ws".into(),
        };
        config.save(space_root).unwrap();

        let subdir = space_root.join("src").join("deep");
        std::fs::create_dir_all(&subdir).unwrap();

        // Searching from a subdirectory should NOT find the parent's space.
        unsafe { std::env::set_var("KUTL_HOME", &subdir) };
        let found = kutl_client::space_config::discover_space(&subdir);
        unsafe { std::env::remove_var("KUTL_HOME") };
        assert!(found.is_none(), "should not walk up to parent");
    }

    #[test]
    fn test_discover_space_not_found() {
        let dir = tempfile::TempDir::new().unwrap();
        let found = kutl_client::space_config::discover_space(dir.path());
        assert!(found.is_none());
    }

    // --- Flag filtering tests ---

    fn make_flag_signal(
        author: &str,
        target: &str,
        audience: sync::AudienceType,
        kind: sync::FlagKind,
    ) -> (sync::Signal, sync::FlagPayload) {
        let flag = sync::FlagPayload {
            kind: i32::from(kind),
            audience_type: i32::from(audience),
            target_did: if target.is_empty() {
                None
            } else {
                Some(target.into())
            },
            message: "test message".into(),
        };
        let signal = sync::Signal {
            id: String::new(),
            space_id: "space-1".into(),
            document_id: Some("doc-1".into()),
            author_did: author.into(),
            timestamp: 0,
            payload: Some(sync::signal::Payload::Flag(flag.clone())),
        };
        (signal, flag)
    }

    #[test]
    fn test_filter_rejects_self_authored() {
        let (signal, flag) = make_flag_signal(
            "did:key:me",
            "",
            sync::AudienceType::Space,
            sync::FlagKind::Info,
        );
        assert!(!should_deliver_flag(&signal, &flag, "did:key:me"));
    }

    #[test]
    fn test_filter_accepts_space_audience() {
        let (signal, flag) = make_flag_signal(
            "did:key:other",
            "",
            sync::AudienceType::Space,
            sync::FlagKind::Question,
        );
        assert!(should_deliver_flag(&signal, &flag, "did:key:me"));
    }

    #[test]
    fn test_filter_accepts_agent_audiences() {
        for audience in [
            sync::AudienceType::AgentOwners,
            sync::AudienceType::AgentEditors,
            sync::AudienceType::AgentViewers,
        ] {
            let (signal, flag) = make_flag_signal(
                "did:key:other",
                "",
                audience,
                sync::FlagKind::ReviewRequested,
            );
            assert!(
                should_deliver_flag(&signal, &flag, "did:key:me"),
                "should accept {audience:?}"
            );
        }
    }

    #[test]
    fn test_filter_participant_matching_target() {
        let (signal, flag) = make_flag_signal(
            "did:key:other",
            "did:key:me",
            sync::AudienceType::Participant,
            sync::FlagKind::Blocked,
        );
        assert!(should_deliver_flag(&signal, &flag, "did:key:me"));
    }

    #[test]
    fn test_filter_participant_wrong_target() {
        let (signal, flag) = make_flag_signal(
            "did:key:other",
            "did:key:someone-else",
            sync::AudienceType::Participant,
            sync::FlagKind::Blocked,
        );
        assert!(!should_deliver_flag(&signal, &flag, "did:key:me"));
    }

    #[test]
    fn test_filter_rejects_human_audiences() {
        for audience in [
            sync::AudienceType::HumanOwners,
            sync::AudienceType::HumanEditors,
            sync::AudienceType::HumanViewers,
        ] {
            let (signal, flag) =
                make_flag_signal("did:key:other", "", audience, sync::FlagKind::Info);
            assert!(
                !should_deliver_flag(&signal, &flag, "did:key:me"),
                "should reject {audience:?}"
            );
        }
    }

    #[test]
    fn test_filter_rejects_unspecified_audience() {
        let (signal, flag) = make_flag_signal(
            "did:key:other",
            "",
            sync::AudienceType::Unspecified,
            sync::FlagKind::Info,
        );
        assert!(!should_deliver_flag(&signal, &flag, "did:key:me"));
    }

    #[test]
    fn test_filter_with_empty_did_accepts_space() {
        let (signal, flag) = make_flag_signal(
            "did:key:other",
            "",
            sync::AudienceType::Space,
            sync::FlagKind::Info,
        );
        assert!(should_deliver_flag(&signal, &flag, ""));
    }

    #[test]
    fn test_filter_with_empty_did_rejects_participant() {
        let (signal, flag) = make_flag_signal(
            "did:key:other",
            "",
            sync::AudienceType::Participant,
            sync::FlagKind::Info,
        );
        assert!(!should_deliver_flag(&signal, &flag, ""));
    }

    // --- Flag kind name tests ---

    #[test]
    fn test_flag_kind_names() {
        assert_eq!(flag_kind_name(i32::from(sync::FlagKind::Info)), "info");
        assert_eq!(
            flag_kind_name(i32::from(sync::FlagKind::Completed)),
            "completed"
        );
        assert_eq!(
            flag_kind_name(i32::from(sync::FlagKind::ReviewRequested)),
            "review_requested"
        );
        assert_eq!(
            flag_kind_name(i32::from(sync::FlagKind::Question)),
            "question"
        );
        assert_eq!(
            flag_kind_name(i32::from(sync::FlagKind::Blocked)),
            "blocked"
        );
        assert_eq!(flag_kind_name(99), "unknown");
    }

    // --- Channel event construction tests ---

    #[test]
    fn test_build_flag_channel_event() {
        let (signal, flag) = make_flag_signal(
            "did:key:alice",
            "did:key:bob",
            sync::AudienceType::Participant,
            sync::FlagKind::Question,
        );
        let event = build_flag_channel_event(&signal, &flag, "abc123def456", "team-space");

        assert_eq!(event.content, "test message");
        assert_eq!(event.meta["space_id"], "abc123def456");
        assert_eq!(event.meta["space_name"], "team-space");
        // Old single "space" key must be gone — consumers should use space_id
        // (canonical) and space_name (display).
        assert!(event.meta.get("space").is_none());
        assert_eq!(event.meta["kind"], "question");
        assert_eq!(event.meta["document"], "doc-1");
        assert_eq!(event.meta["sender"], "did:key:alice");
    }
}
