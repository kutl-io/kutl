//! HTTP handler for the space activity feed route.
//!
//! `GET /spaces/{space_id}/changes?checkpoint=<cp>` returns a JSON
//! [`crate::change_backend::ChangesResponse`] — the same wire format the MCP
//! `get_changes` tool uses (`dispatch_relay_json` → `serde_json::to_string`).
//!
//! Authentication mirrors the signal catch-up route
//! ([`crate::http::signals::handle_catch_up`]): the bearer token is resolved to
//! a DID per-request by [`crate::mcp_handler::authenticate_request`], then
//! space membership is authorized inside the relay actor. A non-member or
//! unauthenticated request receives an error status WITH a reason body (never
//! an empty 404/403).
//!
//! When no change backend is configured (a bare host-seam relay, e.g. a
//! standalone test actor) the route responds with an empty
//! `ChangesResponse` (not an error) — the CLI degrades gracefully to a local
//! signals-only view.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::http::header::HeaderMap;
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::mcp_handler::authenticate_request;
use crate::relay::{McpError, RelayCommand, dispatch_relay_command};

use super::super::AppState;

/// Query parameters for the space activity feed.
///
/// `checkpoint` pages deterministically (rewind-to-position); absent, the feed
/// starts from the caller's cursor floor. Mirrors the signal catch-up route's
/// coarse resume.
#[derive(Deserialize)]
pub(crate) struct ChangesQuery {
    /// Opaque checkpoint string returned by a prior call to this route. Absent
    /// for the first page; supply the value from the previous response to page.
    checkpoint: Option<String>,
}

/// `GET /spaces/{space_id}/changes?checkpoint=<cp>` — the space activity feed
/// (edits + signals) as JSON.
///
/// Resolves the bearer token to a DID, then dispatches `GetChanges` to the
/// relay actor, which authorizes space membership and calls
/// [`crate::change_backend::ChangeBackend::get_changes`]. Returns the
/// [`crate::change_backend::ChangesResponse`] serialized as
/// `application/json` — the same wire format the MCP `get_changes` tool uses.
///
/// Degrades to an empty response (not an error) when no change backend is
/// configured.
pub(crate) async fn handle_changes(
    State(state): State<AppState>,
    Path(space_id): Path<String>,
    Query(query): Query<ChangesQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let did = match authenticate_request(&state, &headers).await {
        Ok(did) => did,
        Err(resp) => return resp.into_response(),
    };

    // Both dispatch failure modes collapse to one client-facing message on
    // this route (deliberately coarser than the dispatch error's own text).
    match dispatch_relay_command(&state.relay_tx, |reply| RelayCommand::GetChanges {
        did,
        space_id,
        checkpoint: query.checkpoint,
        reply,
    })
    .await
    {
        Ok(Ok(changes)) => match serde_json::to_vec(&changes) {
            Ok(json) => {
                (StatusCode::OK, [("content-type", "application/json")], json).into_response()
            }
            Err(e) => {
                tracing::warn!(error = %e, "changes route: failed to serialize response");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        },
        Ok(Err(e)) => reject(&e),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "relay unavailable").into_response(),
    }
}

/// Map a relay [`McpError`] to an HTTP response with a non-empty reason body.
fn reject(err: &McpError) -> axum::response::Response {
    let (status, msg) = match err {
        McpError::NotAuthorized { .. } => (StatusCode::FORBIDDEN, "not authorized"),
        other => {
            tracing::warn!(%other, "changes route: unexpected relay error");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    };
    (status, msg).into_response()
}
