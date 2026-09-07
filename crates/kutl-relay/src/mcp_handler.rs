//! HTTP handlers for the MCP Streamable HTTP transport.
//!
//! Three handlers on `/mcp`:
//! - `POST` — JSON-RPC requests (initialize, tools/list, tools/call, etc.)
//! - `GET`  — SSE notification stream
//! - `DELETE` — session teardown

use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response, Sse, sse};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

use crate::mcp::{
    self, INTERNAL_ERROR, INVALID_PARAMS, JsonRpcRequest, JsonRpcResponse, MCP_PROTOCOL_VERSION,
    METHOD_NOT_FOUND, PARSE_ERROR, ToolCallParams,
};
use crate::mcp_tools;
use crate::relay::{McpSessionId, RelayCommand, RelayDispatchError, dispatch_relay_command};

use super::AppState;

/// MCP notification channel capacity.
const NOTIFY_CHANNEL_CAPACITY: usize = 64;

/// Header name for the MCP session ID.
const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";

// ---------------------------------------------------------------------------
// POST /mcp — JSON-RPC request handler
// ---------------------------------------------------------------------------

/// Handle a JSON-RPC request over MCP Streamable HTTP.
pub(crate) async fn mcp_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    // 1. Extract the (mandatory) bearer token.
    let token = match require_bearer_token(&headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    // 2. Parse JSON-RPC request.
    let req: JsonRpcRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            let resp = JsonRpcResponse::error(
                serde_json::Value::Null,
                PARSE_ERROR,
                format!("invalid JSON: {e}"),
            );
            return json_response(StatusCode::OK, &resp);
        }
    };

    let id = req.id.clone().unwrap_or(serde_json::Value::Null);

    // 3. Route by method.
    match req.method.as_str() {
        "initialize" => handle_initialize(state, &token, id, &req.params).await,
        "notifications/initialized" => {
            // Client acknowledgement — no response needed.
            (StatusCode::ACCEPTED, "").into_response()
        }
        "ping" => {
            let session_id = extract_session_id(&headers);
            if let Some(sid) = &session_id
                && let Err(resp) = validate_session(&state, &token, sid).await
            {
                return resp;
            }
            json_response(
                StatusCode::OK,
                &JsonRpcResponse::success(id, serde_json::json!({})),
            )
        }
        "tools/list" => {
            let session_id = match require_session_id(&headers) {
                Ok(s) => s,
                Err(resp) => return resp,
            };
            if let Err(resp) = validate_session(&state, &token, &session_id).await {
                return resp;
            }
            let tools = mcp_tools::tool_definitions_with_provider(&*state.tool_provider);
            let resp = JsonRpcResponse::success(id, serde_json::json!({"tools": tools}));
            json_response_with_session(StatusCode::OK, &resp, &session_id)
        }
        "tools/call" => {
            let session_id = match require_session_id(&headers) {
                Ok(s) => s,
                Err(resp) => return resp,
            };
            if let Err(resp) = validate_session(&state, &token, &session_id).await {
                return resp;
            }
            handle_tool_call(state, id, &session_id, &req.params).await
        }
        _ => {
            let resp = JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found");
            json_response(StatusCode::OK, &resp)
        }
    }
}

// ---------------------------------------------------------------------------
// GET /mcp — SSE notification stream
// ---------------------------------------------------------------------------

/// Open an SSE stream for MCP notifications.
pub(crate) async fn mcp_get(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let token = match require_bearer_token(&headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let session_id = match require_session_id(&headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    if let Err(resp) = validate_session(&state, &token, &session_id).await {
        return resp;
    }

    let (tx, rx) = mpsc::channel::<String>(NOTIFY_CHANNEL_CAPACITY);

    // Register the notification channel with the relay.
    // intentional: a send failure means the relay is shutting down; the SSE
    // stream then stays empty and the client's keep-alive eventually drops it.
    // Surfacing an error here would not change that outcome.
    let _ = state
        .relay_tx
        .send(RelayCommand::McpRegisterNotifications { session_id, tx })
        .await;

    let stream = ReceiverStream::new(rx);
    let sse_stream = futures_util::StreamExt::map(stream, |json| {
        Ok::<_, std::convert::Infallible>(sse::Event::default().data(json))
    });

    Sse::new(sse_stream)
        .keep_alive(sse::KeepAlive::default())
        .into_response()
}

// ---------------------------------------------------------------------------
// DELETE /mcp — session teardown
// ---------------------------------------------------------------------------

/// Destroy an MCP session.
pub(crate) async fn mcp_delete(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let token = match require_bearer_token(&headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    let session_id = match require_session_id(&headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    if let Err(resp) = validate_session(&state, &token, &session_id).await {
        return resp;
    }

    // intentional: session teardown is best-effort. A send failure means the
    // relay is already shutting down, which tears down every session anyway,
    // so the client's DELETE goal (the session no longer exists) still holds.
    let _ = state
        .relay_tx
        .send(RelayCommand::McpDestroySession { session_id })
        .await;

    StatusCode::NO_CONTENT.into_response()
}

// ---------------------------------------------------------------------------
// Method handlers
// ---------------------------------------------------------------------------

async fn handle_initialize(
    state: AppState,
    token: &str,
    id: serde_json::Value,
    params: &serde_json::Value,
) -> Response {
    // Validate the params shape. Authentication is mandatory, so the
    // token — not any client-self-asserted field — determines the caller's DID.
    if let Err(e) = serde_json::from_value::<mcp::InitializeParams>(params.clone()) {
        let resp = JsonRpcResponse::error(
            id,
            INVALID_PARAMS,
            format!("invalid initialize params: {e}"),
        );
        return json_response(StatusCode::OK, &resp);
    }

    // Resolve caller identity from the validated bearer token.
    let (did, pat) = match validate_token_via_hub(&state, token).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    // Create a session.
    let session_id = match dispatch_relay_command(&state.relay_tx, |reply| {
        RelayCommand::McpCreateSession { did, pat, reply }
    })
    .await
    {
        Ok(session_id) => session_id,
        Err(e) => return relay_dispatch_response(e),
    };

    let result = mcp::InitializeResult {
        protocol_version: MCP_PROTOCOL_VERSION.into(),
        capabilities: mcp::ServerCapabilities {
            tools: serde_json::json!({}),
        },
        server_info: mcp::ServerInfo {
            name: "kutl-relay".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
        instructions: Some(state.instructions_provider.instructions().to_owned()),
    };

    let result_value = match serde_json::to_value(&result) {
        Ok(v) => v,
        Err(e) => {
            let resp = JsonRpcResponse::error(
                id,
                INTERNAL_ERROR,
                format!("failed to serialize initialize result: {e}"),
            );
            return json_response(StatusCode::OK, &resp);
        }
    };
    let resp = JsonRpcResponse::success(id, result_value);
    json_response_with_session(StatusCode::OK, &resp, &session_id)
}

async fn handle_tool_call(
    state: AppState,
    id: serde_json::Value,
    session_id: &str,
    params: &serde_json::Value,
) -> Response {
    // Parse ToolCallParams.
    let call: ToolCallParams = match serde_json::from_value(params.clone()) {
        Ok(c) => c,
        Err(e) => {
            let resp = JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                format!("invalid tool call params: {e}"),
            );
            return json_response(StatusCode::OK, &resp);
        }
    };

    info!(session_id, tool = %call.name, "MCP tool call");

    // Execution gate: a relay runs ONLY the tools its active provider
    // advertises. The allowed set is derived live from
    // `advertised_tool_names(provider)` — the same source as `tools/list` — so
    // the executable surface can never drift from the advertised one, and a
    // provider-gated tool (e.g. `react_to_signal`, added only by the kutlhub
    // provider) cannot run on a relay that does not advertise it even when a
    // client names it directly. An unadvertised name is reported as an unknown
    // tool: on this relay, it is not one.
    if !mcp_tools::advertised_tool_names(&*state.tool_provider).contains(&call.name) {
        return tool_call_response(
            id,
            &mcp::ToolCallResult::error(format!("unknown tool: {}", call.name)),
        );
    }

    // Parse and validate arguments.
    let parsed = match mcp_tools::parse_tool_call(&call.name, &call.arguments) {
        Ok(p) => p,
        Err(err_result) => return tool_call_response(id, &err_result),
    };

    // Dispatch to relay.
    let result = dispatch_tool(&state, session_id, parsed).await;
    tool_call_response(id, &result)
}

/// Serialize a [`mcp::ToolCallResult`] into the MCP `tools/call` response shape:
/// a JSON-RPC success carrying the tool result, with tool-level failures riding
/// as `isError` content rather than JSON-RPC errors. The parse-error path, the
/// execution gate, and the dispatch result all share this. Serialization
/// failure — impossible for a well-formed result — degrades to a JSON-RPC
/// `INTERNAL_ERROR`.
fn tool_call_response(id: serde_json::Value, result: &mcp::ToolCallResult) -> Response {
    match serde_json::to_value(result) {
        Ok(v) => json_response(StatusCode::OK, &JsonRpcResponse::success(id, v)),
        Err(e) => json_response(
            StatusCode::OK,
            &JsonRpcResponse::error(
                id,
                INTERNAL_ERROR,
                format!("failed to serialize tool result: {e}"),
            ),
        ),
    }
}

#[allow(clippy::too_many_lines)]
async fn dispatch_tool(
    state: &AppState,
    session_id: &str,
    parsed: mcp_tools::ParsedToolCall,
) -> mcp::ToolCallResult {
    match parsed {
        mcp_tools::ParsedToolCall::ReadDocument {
            space_id,
            document_id,
        } => {
            dispatch_relay_json(state, |tx| RelayCommand::McpReadDocument {
                session_id: session_id.to_owned(),
                space_id,
                document_id,
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::ListDocuments { space_id } => {
            dispatch_relay_json(state, |tx| RelayCommand::McpListDocuments {
                session_id: session_id.to_owned(),
                space_id,
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::ReadLog {
            space_id,
            document_id,
            limit,
        } => dispatch_read_log(state, session_id, space_id, document_id, limit).await,
        mcp_tools::ParsedToolCall::ListParticipants { space_id } => {
            dispatch_relay_json(state, |tx| RelayCommand::McpListParticipants {
                session_id: session_id.to_owned(),
                space_id,
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::ResolveParticipant { space_id, name } => {
            dispatch_relay_json(state, |tx| RelayCommand::McpResolveParticipant {
                session_id: session_id.to_owned(),
                space_id,
                name,
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::Status { space_id } => {
            dispatch_relay_json(state, |tx| RelayCommand::McpStatus {
                session_id: session_id.to_owned(),
                space_id,
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::EditDocument {
            space_id,
            document_id,
            base_version,
            content,
            intent,
            snippet,
        } => {
            dispatch_edit_document(
                state,
                session_id,
                space_id,
                document_id,
                base_version,
                content,
                intent,
                snippet,
            )
            .await
        }
        mcp_tools::ParsedToolCall::GetChanges {
            space_id,
            checkpoint,
        } => {
            dispatch_relay_json(state, |tx| RelayCommand::McpGetChanges {
                session_id: session_id.to_owned(),
                space_id,
                checkpoint,
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::CreateDocument {
            space_id,
            path,
            content,
            provenance,
        } => {
            dispatch_relay_json(state, |tx| RelayCommand::McpCreateDocument {
                session_id: session_id.to_owned(),
                space_id,
                path,
                content,
                provenance,
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::UploadBlob {
            space_id,
            path,
            content_type,
            bytes,
            provenance,
        } => {
            let max_bytes = state.max_blob_bytes;
            dispatch_relay_json(state, |tx| RelayCommand::McpUploadBlob {
                session_id: session_id.to_owned(),
                space_id,
                path,
                content_type,
                bytes,
                provenance,
                max_bytes,
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::ListSpaces => {
            dispatch_relay_json(state, |tx| RelayCommand::McpListSpaces {
                session_id: session_id.to_owned(),
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::CreateFlag {
            space_id,
            document_id,
            kind,
            message,
            audience,
            target_did,
            signal_id,
            anchor_text,
        } => {
            dispatch_create_flag(
                state,
                session_id,
                space_id,
                document_id,
                kind,
                message,
                audience,
                target_did,
                signal_id,
                anchor_text,
            )
            .await
        }
        mcp_tools::ParsedToolCall::CreateReply {
            space_id,
            parent_signal_id,
            parent_reply_id,
            body,
        } => {
            dispatch_create_reply(
                state,
                session_id,
                space_id,
                parent_signal_id,
                parent_reply_id,
                body,
            )
            .await
        }
        mcp_tools::ParsedToolCall::ReactToSignal {
            space_id,
            signal_id,
            emoji,
            remove,
        } => dispatch_react_to_signal(state, session_id, space_id, signal_id, emoji, remove).await,
        mcp_tools::ParsedToolCall::CloseFlag {
            space_id,
            signal_id,
            reason,
            close_note,
        } => dispatch_close_flag(state, session_id, space_id, signal_id, reason, close_note).await,
        mcp_tools::ParsedToolCall::ReopenFlag {
            space_id,
            signal_id,
        } => dispatch_reopen_flag(state, session_id, space_id, signal_id).await,
        mcp_tools::ParsedToolCall::ListSignals {
            space_id,
            status,
            kind,
            document_id,
            flag_kind,
        } => {
            dispatch_relay_json(state, |tx| RelayCommand::McpListSignals {
                session_id: session_id.to_owned(),
                space_id,
                status,
                kind,
                document_id,
                flag_kind,
                reply: tx,
            })
            .await
        }
        mcp_tools::ParsedToolCall::GetSignalDetail {
            space_id,
            signal_id,
        } => dispatch_get_signal_detail(state, session_id, space_id, signal_id).await,
    }
}

async fn dispatch_read_log(
    state: &AppState,
    session_id: &str,
    space_id: String,
    document_id: String,
    limit: Option<usize>,
) -> mcp::ToolCallResult {
    dispatch_relay_json(state, |tx| RelayCommand::McpReadLog {
        session_id: session_id.to_owned(),
        space_id,
        document_id,
        limit,
        reply: tx,
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_edit_document(
    state: &AppState,
    session_id: &str,
    space_id: String,
    document_id: String,
    base_version: String,
    content: String,
    intent: String,
    snippet: String,
) -> mcp::ToolCallResult {
    dispatch_relay_json(state, |tx| RelayCommand::McpEditDocument {
        session_id: session_id.to_owned(),
        space_id,
        document_id,
        base_version,
        new_content: content,
        intent,
        snippet,
        reply: tx,
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_create_flag(
    state: &AppState,
    session_id: &str,
    space_id: String,
    document_id: String,
    kind: i32,
    message: String,
    audience: i32,
    target_did: String,
    signal_id: Option<String>,
    anchor_text: Option<String>,
) -> mcp::ToolCallResult {
    let result = dispatch_relay_command(&state.relay_tx, |tx| RelayCommand::McpCreateFlag {
        session_id: session_id.to_owned(),
        space_id,
        document_id,
        kind,
        message,
        audience,
        target_did,
        signal_id,
        anchor_text,
        reply: tx,
    })
    .await;
    match result {
        Ok(Ok(id)) => mcp::ToolCallResult::text(format!("{CREATE_FLAG_OK_PREFIX}{id}")),
        Ok(Err(e)) => mcp::ToolCallResult::error(e.to_string()),
        Err(e) => mcp::ToolCallResult::error(e.to_string()),
    }
}

/// Prefix the `create_flag` handler emits on success ahead of the
/// newly-minted signal id. Extracted as `pub const` so integration
/// tests can parse the echoed id without hard-coding the literal.
pub const CREATE_FLAG_OK_PREFIX: &str = "created flag signal ";

async fn dispatch_create_reply(
    state: &AppState,
    session_id: &str,
    space_id: String,
    parent_signal_id: String,
    parent_reply_id: Option<String>,
    body: String,
) -> mcp::ToolCallResult {
    let result = dispatch_relay_command(&state.relay_tx, |tx| RelayCommand::McpCreateReply {
        session_id: session_id.to_owned(),
        space_id,
        parent_signal_id,
        parent_reply_id,
        body,
        reply: tx,
    })
    .await;
    match result {
        Ok(Ok(id)) => mcp::ToolCallResult::text(format!("reply posted ({id})")),
        Ok(Err(e)) => mcp::ToolCallResult::error(e.to_string()),
        Err(e) => mcp::ToolCallResult::error(e.to_string()),
    }
}

async fn dispatch_react_to_signal(
    state: &AppState,
    session_id: &str,
    space_id: String,
    signal_id: String,
    emoji: String,
    remove: bool,
) -> mcp::ToolCallResult {
    let result = dispatch_relay_command(&state.relay_tx, |tx| RelayCommand::McpReactToSignal {
        session_id: session_id.to_owned(),
        space_id,
        signal_id,
        emoji,
        remove,
        reply: tx,
    })
    .await;
    match result {
        Ok(Ok(())) => mcp::ToolCallResult::text("reaction updated"),
        Ok(Err(e)) => mcp::ToolCallResult::error(e.to_string()),
        Err(e) => mcp::ToolCallResult::error(e.to_string()),
    }
}

async fn dispatch_close_flag(
    state: &AppState,
    session_id: &str,
    space_id: String,
    signal_id: String,
    reason: Option<String>,
    close_note: Option<String>,
) -> mcp::ToolCallResult {
    let result = dispatch_relay_command(&state.relay_tx, |tx| RelayCommand::McpCloseFlag {
        session_id: session_id.to_owned(),
        space_id,
        signal_id,
        reason,
        close_note,
        reply: tx,
    })
    .await;
    match result {
        Ok(Ok(())) => mcp::ToolCallResult::text("flag closed"),
        Ok(Err(e)) => mcp::ToolCallResult::error(e.to_string()),
        Err(e) => mcp::ToolCallResult::error(e.to_string()),
    }
}

async fn dispatch_reopen_flag(
    state: &AppState,
    session_id: &str,
    space_id: String,
    signal_id: String,
) -> mcp::ToolCallResult {
    let result = dispatch_relay_command(&state.relay_tx, |tx| RelayCommand::McpReopenFlag {
        session_id: session_id.to_owned(),
        space_id,
        signal_id,
        reply: tx,
    })
    .await;
    match result {
        Ok(Ok(())) => mcp::ToolCallResult::text("flag reopened"),
        Ok(Err(e)) => mcp::ToolCallResult::error(e.to_string()),
        Err(e) => mcp::ToolCallResult::error(e.to_string()),
    }
}

async fn dispatch_get_signal_detail(
    state: &AppState,
    session_id: &str,
    space_id: String,
    signal_id: String,
) -> mcp::ToolCallResult {
    dispatch_relay_json(state, |tx| RelayCommand::McpGetSignalDetail {
        session_id: session_id.to_owned(),
        space_id,
        signal_id,
        reply: tx,
    })
    .await
}

/// A 500 for a relay dispatch failure, in this handler's JSON error shape.
fn relay_dispatch_response(e: RelayDispatchError) -> Response {
    json_error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
}

/// Send a command to the relay, await a `Result<T, McpError>` reply, and
/// serialize the success value as pretty JSON for the MCP tool result.
async fn dispatch_relay_json<T: serde::Serialize>(
    state: &AppState,
    cmd_builder: impl FnOnce(oneshot::Sender<Result<T, crate::relay::McpError>>) -> RelayCommand,
) -> mcp::ToolCallResult {
    match dispatch_relay_command(&state.relay_tx, cmd_builder).await {
        Ok(Ok(value)) => match serde_json::to_string_pretty(&value) {
            Ok(json) => mcp::ToolCallResult::text(json),
            Err(e) => mcp::ToolCallResult::error(format!("failed to serialize result: {e}")),
        },
        Ok(Err(e)) => mcp::ToolCallResult::error(e.to_string()),
        Err(e) => mcp::ToolCallResult::error(e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a bearer token from the `Authorization` header.
fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(String::from)
}

/// Extract the bearer token, requiring it to be present.
///
/// This is the single entry point for token extraction in all HTTP handlers.
/// Authentication is mandatory, so a missing token is always a 401.
#[allow(clippy::result_large_err)]
fn require_bearer_token(headers: &HeaderMap) -> Result<String, Response> {
    match extract_bearer_token(headers) {
        Some(t) => Ok(t),
        None => Err(json_error_response(
            StatusCode::UNAUTHORIZED,
            "missing bearer token",
        )),
    }
}

/// Authenticate an HTTP request and return the caller's DID.
///
/// Validates the bearer token and returns the authenticated DID.
/// Authentication is mandatory.
///
/// This is the single entry point for request authentication outside MCP
/// session flows. MCP session handlers use [`validate_session`] instead
/// (which adds session existence + DID ownership checks on top).
pub(crate) async fn authenticate_request(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<String, Response> {
    let token = require_bearer_token(headers)?;
    let (did, _pat) = validate_token_via_hub(state, &token).await?;
    Ok(did)
}

/// Extract the MCP session ID from headers.
fn extract_session_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get(MCP_SESSION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

/// Require the MCP session ID header, returning an error response if missing.
#[allow(clippy::result_large_err)]
fn require_session_id(headers: &HeaderMap) -> Result<McpSessionId, Response> {
    extract_session_id(headers).ok_or_else(|| {
        json_error_response(StatusCode::BAD_REQUEST, "missing Mcp-Session-Id header")
    })
}

/// Validate a bearer token via the relay and return `(did, pat_context)`.
async fn validate_token_via_hub(
    state: &AppState,
    token: &str,
) -> Result<(String, Option<crate::auth::PatAuthContext>), Response> {
    let result = dispatch_relay_command(&state.relay_tx, |reply| RelayCommand::McpValidateToken {
        token: token.to_owned(),
        reply,
    })
    .await
    .map_err(relay_dispatch_response)?;

    result.map_err(|e| {
        // Transient backend failure — tell the client to retry instead of
        // discarding a possibly-valid token.
        if matches!(e, crate::auth::AuthError::BackendUnavailable) {
            tracing::error!(error = %e, "MCP token validation backend unavailable");
            json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication backend unavailable, retry",
            )
        } else {
            debug!(error = %e, "MCP token validation failed");
            json_error_response(StatusCode::UNAUTHORIZED, "invalid or expired token")
        }
    })
}

/// Validate that a session exists and the token matches its DID.
///
/// The bearer token is validated to obtain a DID, and the session must belong
/// to that DID. Authentication is mandatory.
async fn validate_session(state: &AppState, token: &str, session_id: &str) -> Result<(), Response> {
    let (did, _pat) = validate_token_via_hub(state, token).await?;

    dispatch_relay_command(&state.relay_tx, |reply| RelayCommand::McpValidateSession {
        session_id: session_id.to_owned(),
        expected_did: did,
        reply,
    })
    .await
    .map_err(relay_dispatch_response)?
    .map_err(|_| json_error_response(StatusCode::NOT_FOUND, "session not found or expired"))
}

/// Build a JSON error response (non-JSON-RPC).
fn json_error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::json!({"error": message}).to_string(),
    )
        .into_response()
}

/// Build a JSON-RPC response.
fn json_response(status: StatusCode, resp: &JsonRpcResponse) -> Response {
    let body = serde_json::to_string(resp).unwrap_or_else(|e| {
        // Last-resort fallback: the response itself failed to serialize.
        format!(r#"{{"jsonrpc":"2.0","error":{{"code":{INTERNAL_ERROR},"message":"response serialization failed: {e}"}}}}"#)
    });
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Build a JSON-RPC response with the `Mcp-Session-Id` header.
fn json_response_with_session(
    status: StatusCode,
    resp: &JsonRpcResponse,
    session_id: &str,
) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        HeaderName::from_static(MCP_SESSION_ID_HEADER),
        HeaderValue::from_str(session_id).expect("valid header value"),
    );
    let body = serde_json::to_string(resp).unwrap_or_else(|e| {
        format!(r#"{{"jsonrpc":"2.0","error":{{"code":{INTERNAL_ERROR},"message":"response serialization failed: {e}"}}}}"#)
    });
    (status, headers, body).into_response()
}
