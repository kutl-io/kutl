//! HTTP handler for exporting document text content.
//!
//! `GET /spaces/:space_id/documents/:document_id/text` returns the
//! current document content as `text/plain`. Authentication is handled
//! by [`crate::mcp_handler::authenticate_request`].

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use tokio::sync::oneshot;

use crate::mcp_handler::authenticate_request;
use crate::relay::{McpError, RelayCommand};

use super::AppState;

/// Extract text content from a document.
pub(crate) async fn handle_export_text(
    State(state): State<AppState>,
    Path((space_id, document_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let did = match authenticate_request(&state, &headers).await {
        Ok(did) => did,
        Err(resp) => return resp.into_response(),
    };

    // Read document text (authorization checked inside relay actor).
    let (tx, rx) = oneshot::channel();
    if state
        .relay_tx
        .send(RelayCommand::ReadDocumentText {
            did,
            space_id,
            document_id,
            reply: tx,
        })
        .await
        .is_err()
    {
        return (StatusCode::INTERNAL_SERVER_ERROR, "relay unavailable").into_response();
    }

    match rx.await {
        Ok(Ok(text)) => (
            StatusCode::OK,
            [("content-type", "text/plain; charset=utf-8")],
            text,
        )
            .into_response(),
        Ok(Err(e)) => {
            let (status, msg) = match &e {
                McpError::NotAuthorized { .. } => (StatusCode::FORBIDDEN, "not authorized"),
                McpError::DocumentNotFound { .. } => (StatusCode::NOT_FOUND, "document not found"),
                McpError::NotTextDocument => {
                    (StatusCode::BAD_REQUEST, "document is not a text document")
                }
                other => {
                    tracing::warn!(%other, "text_export: unexpected mcp error");
                    (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
                }
            };
            (status, msg).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "relay unavailable").into_response(),
    }
}
