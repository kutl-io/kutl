//! HTTP handlers for invite management (OSS relay only).
//!
//! These routes are mounted when an `InviteBackend` is provided to
//! [`crate::build_app_with_backends`]. kutlhub-relay manages invitations via
//! the UX server and passes `None`, so these routes are not mounted there.
//!
//! The backend is accessed directly from an axum `Extension` — invite
//! operations are pure CRUD and require no relay-actor coordination.

use std::sync::Arc;

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Extension, Json};
use kutl_core::MS_PER_HOUR;
use serde::{Deserialize, Serialize};

use crate::invite_backend::{InviteBackend, InviteBackendError};

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

/// `POST /invites` request body.
#[derive(Deserialize)]
pub(crate) struct CreateInviteRequest {
    /// Relay space UUID to create the invite for.
    pub space_id: String,
    /// Optional expiry expressed as hours from now.
    pub expires_in_hours: Option<u32>,
}

/// Response body for invite creation and listing.
#[derive(Serialize)]
pub(crate) struct InviteResponse {
    /// Opaque invite code.
    pub code: String,
    /// Relay space UUID.
    pub space_id: String,
    /// Human-readable space name.
    pub space_name: String,
    /// Expiry in milliseconds since epoch, or `null` for no expiry.
    pub expires_at: Option<i64>,
    /// Creation timestamp in milliseconds since epoch.
    pub created_at: i64,
}

/// `GET /invites/:code` response body.
#[derive(Serialize)]
pub(crate) struct InviteInfoResponse {
    /// Relay space UUID.
    pub space_id: String,
    /// Human-readable space name.
    pub space_name: String,
}

/// Query parameters for `GET /invites`.
#[derive(Deserialize)]
pub(crate) struct ListInvitesQuery {
    /// Space UUID to list invites for.
    pub space_id: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `POST /invites` — create a new invite for a space.
///
/// Accepts `{ space_id, expires_in_hours? }` and returns 201 with the full
/// invite record. Returns 404 if the space does not exist.
///
/// **Auth:** unauthenticated. The OSS relay has no HTTP auth layer.
/// Deployers should restrict access via private networks or a reverse
/// proxy with SSO (e.g. `OAuth2` Proxy, Authelia).
pub(crate) async fn handle_create_invite(
    Extension(backend): Extension<Arc<dyn InviteBackend>>,
    Json(req): Json<CreateInviteRequest>,
) -> Result<(StatusCode, Json<InviteResponse>), InviteErrorResponse> {
    let expires_at = req.expires_in_hours.map(|h| {
        let now = kutl_core::env::now_ms();
        now + i64::from(h) * MS_PER_HOUR
    });

    let record = backend
        .create_invite(&req.space_id, expires_at)
        .map_err(|e| InviteErrorResponse::from_backend_error(&e))?;

    Ok((
        StatusCode::CREATED,
        Json(InviteResponse {
            code: record.code,
            space_id: record.space_id,
            space_name: record.space_name,
            expires_at: record.expires_at,
            created_at: record.created_at,
        }),
    ))
}

/// `GET /invites?space_id=X` — list all invites for a space.
///
/// Returns all invites including expired ones (callers decide what to show).
///
/// **Auth:** unauthenticated — see [`handle_create_invite`].
pub(crate) async fn handle_list_invites(
    Extension(backend): Extension<Arc<dyn InviteBackend>>,
    axum::extract::Query(query): axum::extract::Query<ListInvitesQuery>,
) -> Result<Json<Vec<InviteResponse>>, InviteErrorResponse> {
    let records = backend
        .list_invites(&query.space_id)
        .map_err(|e| InviteErrorResponse::from_backend_error(&e))?;

    let responses: Vec<InviteResponse> = records
        .into_iter()
        .map(|r| InviteResponse {
            code: r.code,
            space_id: r.space_id,
            space_name: r.space_name,
            expires_at: r.expires_at,
            created_at: r.created_at,
        })
        .collect();

    Ok(Json(responses))
}

/// `GET /invites/:code` — validate an invite code and return space info.
///
/// Returns `{ space_id, space_name }` for valid invites.
/// Returns 404 for unknown codes and 410 Gone for expired codes.
///
/// **Auth:** unauthenticated — see [`handle_create_invite`].
pub(crate) async fn handle_get_invite(
    Extension(backend): Extension<Arc<dyn InviteBackend>>,
    Path(code): Path<String>,
) -> Result<Json<InviteInfoResponse>, InviteErrorResponse> {
    let info = backend
        .validate_invite(&code)
        .map_err(|e| InviteErrorResponse::from_backend_error(&e))?;

    match info {
        Some(info) => Ok(Json(InviteInfoResponse {
            space_id: info.space_id,
            space_name: info.space_name,
        })),
        None => Err(InviteErrorResponse {
            status: StatusCode::NOT_FOUND,
            message: format!("invite not found: {code}"),
        }),
    }
}

/// `DELETE /invites/:code` — revoke an invite.
///
/// Returns 200 if the invite was found and revoked, 404 if it did not exist.
///
/// **Auth:** unauthenticated — see [`handle_create_invite`].
pub(crate) async fn handle_delete_invite(
    Extension(backend): Extension<Arc<dyn InviteBackend>>,
    Path(code): Path<String>,
) -> Result<StatusCode, InviteErrorResponse> {
    let deleted = backend
        .revoke_invite(&code)
        .map_err(|e| InviteErrorResponse::from_backend_error(&e))?;

    if deleted {
        Ok(StatusCode::OK)
    } else {
        Err(InviteErrorResponse {
            status: StatusCode::NOT_FOUND,
            message: format!("invite not found: {code}"),
        })
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Error response for invite endpoints.
pub(crate) struct InviteErrorResponse {
    status: StatusCode,
    message: String,
}

impl InviteErrorResponse {
    fn from_backend_error(e: &InviteBackendError) -> Self {
        match e {
            InviteBackendError::SpaceNotFound(msg) | InviteBackendError::InvalidCode(msg) => Self {
                status: StatusCode::NOT_FOUND,
                message: msg.clone(),
            },
            InviteBackendError::Expired(code) => Self {
                status: StatusCode::GONE,
                message: format!("invite expired: {code}"),
            },
            InviteBackendError::Storage(msg) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: msg.clone(),
            },
        }
    }
}

impl IntoResponse for InviteErrorResponse {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(serde_json::json!({"error": self.message})),
        )
            .into_response()
    }
}
