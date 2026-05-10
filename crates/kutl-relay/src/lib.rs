//! kutl-relay — WebSocket sync relay for real-time collaboration.

pub mod acl;
pub mod auth;
pub mod authorized_keys;
pub mod blob_backend;
pub mod change_backend;
pub mod change_sqlite;
pub mod config;
pub mod content_backend;
pub mod flush;
pub mod invite_backend;
pub mod invite_routes;
mod load_class;
pub mod mcp;
pub mod mcp_handler;
pub mod mcp_tools;
pub mod membership_backend;
pub mod observer;
pub mod pat_backend;
pub mod protocol;
pub mod quota_backend;
pub mod registry;
pub mod registry_store;
pub mod relay;
pub mod session_backend;
pub mod space_backend;
pub mod spaces;
pub mod sqlite_invite_backend;
pub mod telemetry;
pub mod text_export;
pub mod web;

pub use observer::{
    AfterMergeObserver, BeforeMergeObserver, DocumentRegisteredEvent, DocumentRenamedEvent,
    DocumentUnregisteredEvent, EditContentMode, MergedEvent, NoopAfterMergeObserver,
    NoopBeforeMergeObserver, NoopObserver, ReactionEvent, RelayObserver, SignalClosedEvent,
    SignalCreatedEvent, SignalReopenedEvent, TracingObserver,
};

mod conn;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::extract::State;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use config::RelayConfig;
use protocol::ABSOLUTE_BLOB_MAX;
use relay::{Relay, RelayCommand};

/// Capacity of the mpsc channel from connection tasks to the relay actor.
const RELAY_CHANNEL_CAPACITY: usize = 256;

/// Cadence for the open-relay reminder log line. Long enough to keep log
/// volume low, short enough to land in any sensible retention window.
const OPEN_RELAY_REMINDER_INTERVAL: std::time::Duration = std::time::Duration::from_mins(15);

/// Shared application state passed to axum handlers.
#[derive(Clone)]
struct AppState {
    relay_tx: mpsc::Sender<RelayCommand>,
    conn_counter: Arc<AtomicU64>,
    outbound_capacity: usize,
    /// When false, MCP and WebSocket endpoints allow unauthenticated access.
    require_auth: bool,
    /// External URL for constructing verification URLs in device flow.
    external_url: Option<String>,
    /// UX server base URL for user-facing pages (e.g. `/device`).
    ux_url: Option<String>,
    /// Bind host for fallback URL construction.
    host: String,
    /// Bind port for fallback URL construction.
    port: u16,
    /// MCP tool provider for kutlhub-relay extensions (or no-op for OSS).
    tool_provider: Arc<dyn mcp_tools::McpToolProvider>,
    /// MCP instructions provider for cloud-mode scope-1 content. OSS
    /// uses `DefaultInstructionsProvider`; kutlhub-relay overrides
    /// with its own.
    instructions_provider: Arc<dyn mcp_tools::McpInstructionsProvider>,
}

/// Build the axum router and spawn the relay actor (open-source entry point).
///
/// The OSS relay is Postgres-free and ephemeral. It uses an optional `SQLite`
/// registry backend for document registration persistence when `data_dir`
/// is configured.
///
/// When `data_dir` is set, a single shared `SQLite` pool is opened and used
/// by all three backends (registry, spaces, invites) to avoid lock contention.
///
/// Delegates to [`build_app_with_backends`] with `None` for content
/// and blob backends.
///
/// Returns `(Router, relay_handle, flush_handle)`. The flush handle is `None`
/// when no content backend is configured (ephemeral mode).
///
/// # Errors
///
/// Returns an error if the registry backend cannot be opened.
pub async fn build_app(
    config: RelayConfig,
) -> anyhow::Result<(Router, JoinHandle<()>, Option<JoinHandle<()>>)> {
    let (
        registry_backend,
        space_backend_opt,
        web_space_backend_opt,
        invite_backend_opt,
        change_backend_opt,
    ) = if let Some(ref data_dir) = config.data_dir {
        let pool = open_sqlite_pool(data_dir).await?;

        let registry: Arc<dyn registry_store::RegistryBackend> = Arc::new(
            registry_store::SqliteBackend::new(pool.clone())
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        );
        let sb: Box<dyn space_backend::SpaceBackend> = Box::new(
            space_backend::SqliteSpaceBackend::new(pool.clone())
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        );
        // A second backend instance sharing the same pool for the web
        // admin layer. SQLite pool is internally reference-counted, so
        // both instances share a single connection.
        let web_sb: Arc<dyn space_backend::SpaceBackend> = Arc::new(
            space_backend::SqliteSpaceBackend::new(pool.clone())
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        );
        let ib: Box<dyn invite_backend::InviteBackend> = Box::new(
            sqlite_invite_backend::SqliteInviteBackend::new(pool.clone())
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        );
        let cb: Arc<dyn change_backend::ChangeBackend> = Arc::new(
            change_sqlite::SqliteChangeBackend::new(pool)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        );
        (Some(registry), Some(sb), Some(web_sb), Some(ib), Some(cb))
    } else {
        (None, None, None, None, None)
    };

    let instructions_provider: Arc<dyn mcp_tools::McpInstructionsProvider> =
        Arc::new(mcp_tools::DefaultInstructionsProvider);

    Ok(build_app_with_backends_and_web(
        config,
        None,
        None,
        None,
        registry_backend,
        space_backend_opt,
        web_space_backend_opt,
        None,
        None,
        invite_backend_opt,
        change_backend_opt,
        None,
        Arc::new(observer::TracingObserver),
        Arc::new(observer::NoopBeforeMergeObserver),
        Arc::new(observer::TracingObserver),
        Arc::new(mcp_tools::NoopToolProvider),
        instructions_provider,
    ))
}

/// Open a shared `SQLite` pool for the OSS relay's `registry.db`.
///
/// Creates the data directory if it does not exist, enables WAL mode, and
/// limits the pool to a single connection (appropriate for `SQLite`).
pub(crate) async fn open_sqlite_pool(
    data_dir: &std::path::Path,
) -> anyhow::Result<sqlx::sqlite::SqlitePool> {
    std::fs::create_dir_all(data_dir)
        .map_err(|e| anyhow::anyhow!("failed to create data dir: {e}"))?;
    let db_path = data_dir.join("registry.db");
    let url = format!("sqlite:{}?mode=rwc", db_path.display());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .map_err(|e| anyhow::anyhow!("failed to open sqlite pool: {e}"))?;

    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("failed to enable WAL mode: {e}"))?;

    Ok(pool)
}

/// Build the axum router with explicit storage backends.
///
/// This is the entry point for `kutlhub-relay`, which provides its own
/// backends (S3, Postgres). The open-source [`build_app`] delegates here
/// with `None` for content/blob backends.
///
/// When `invite_backend` is `Some`, the invite HTTP routes
/// (`POST /invites`, `GET /invites`, `GET /invites/:code`,
/// `DELETE /invites/:code`) are mounted. kutlhub-relay passes `None` because
/// invitations are managed by the UX server.
///
/// # Errors
///
/// Returns an error if router construction fails.
#[allow(clippy::too_many_arguments)]
pub fn build_app_with_backends(
    config: RelayConfig,
    session_backend: Option<Arc<dyn session_backend::SessionBackend>>,
    pat_backend: Option<Arc<dyn pat_backend::PatBackend>>,
    membership_backend: Option<Arc<dyn membership_backend::MembershipBackend>>,
    registry_backend: Option<Arc<dyn registry_store::RegistryBackend>>,
    space_backend: Option<Box<dyn space_backend::SpaceBackend>>,
    content_backend: Option<Arc<dyn content_backend::ContentBackend>>,
    blob_backend: Option<Arc<dyn blob_backend::BlobBackend>>,
    invite_backend: Option<Box<dyn invite_backend::InviteBackend>>,
    change_backend: Option<Arc<dyn change_backend::ChangeBackend>>,
    quota_backend: Option<Arc<dyn quota_backend::QuotaBackend>>,
    observer: Arc<dyn observer::RelayObserver>,
    before_merge: Arc<dyn observer::BeforeMergeObserver>,
    after_merge: Arc<dyn observer::AfterMergeObserver>,
    tool_provider: Arc<dyn mcp_tools::McpToolProvider>,
    instructions_provider: Arc<dyn mcp_tools::McpInstructionsProvider>,
) -> anyhow::Result<(Router, JoinHandle<()>, Option<JoinHandle<()>>)> {
    Ok(build_app_with_backends_and_web(
        config,
        session_backend,
        pat_backend,
        membership_backend,
        registry_backend,
        space_backend,
        None,
        content_backend,
        blob_backend,
        invite_backend,
        change_backend,
        quota_backend,
        observer,
        before_merge,
        after_merge,
        tool_provider,
        instructions_provider,
    ))
}

/// Build the axum router with explicit storage backends and an optional web
/// space backend for the admin UI.
///
/// The `web_space_backend` parameter, when `Some`, is used exclusively by the
/// HTML admin routes (`GET /`, `GET /spaces/{id}`, `GET /join/{code}`) so
/// that the relay actor can take ownership of the main `space_backend` without
/// sharing it across threads. In the OSS relay both instances share the same
/// underlying `SQLite` pool.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn build_app_with_backends_and_web(
    config: RelayConfig,
    session_backend: Option<Arc<dyn session_backend::SessionBackend>>,
    pat_backend: Option<Arc<dyn pat_backend::PatBackend>>,
    membership_backend: Option<Arc<dyn membership_backend::MembershipBackend>>,
    registry_backend: Option<Arc<dyn registry_store::RegistryBackend>>,
    space_backend: Option<Box<dyn space_backend::SpaceBackend>>,
    web_space_backend: Option<Arc<dyn space_backend::SpaceBackend>>,
    content_backend: Option<Arc<dyn content_backend::ContentBackend>>,
    blob_backend: Option<Arc<dyn blob_backend::BlobBackend>>,
    invite_backend: Option<Box<dyn invite_backend::InviteBackend>>,
    change_backend: Option<Arc<dyn change_backend::ChangeBackend>>,
    quota_backend: Option<Arc<dyn quota_backend::QuotaBackend>>,
    observer: Arc<dyn observer::RelayObserver>,
    before_merge: Arc<dyn observer::BeforeMergeObserver>,
    after_merge: Arc<dyn observer::AfterMergeObserver>,
    tool_provider: Arc<dyn mcp_tools::McpToolProvider>,
    instructions_provider: Arc<dyn mcp_tools::McpInstructionsProvider>,
) -> (Router, JoinHandle<()>, Option<JoinHandle<()>>) {
    // Refuse to start if require_auth is true but there is no way to
    // check space membership. Without a membership backend or an
    // authorized_keys file, authorize_space would allow any
    // authenticated identity into any space — which defeats the
    // purpose of requiring auth in the first place.
    assert!(
        !(config.require_auth
            && membership_backend.is_none()
            && config.authorized_keys_file.is_none()),
        "misconfigured: require_auth is true but neither a membership \
         backend nor an authorized_keys_file is provided. The relay \
         would authenticate connections but authorize everyone into \
         every space. Either provide a membership backend, set \
         KUTL_RELAY_AUTHORIZED_KEYS_FILE, or disable require_auth."
    );

    // Refuse to start with BOTH a membership backend and an
    // authorized_keys file. They are two distinct authz modes: the
    // membership backend checks space_memberships (kutlhub mode),
    // while authorized_keys grants blanket access to listed DIDs
    // (OSS mode). If both are present, the authorized_keys file
    // silently bypasses membership for any DID it contains.
    assert!(
        !(membership_backend.is_some() && config.authorized_keys_file.is_some()),
        "misconfigured: both a membership backend and an \
         authorized_keys_file are configured. These are mutually \
         exclusive authz modes — the keys file would bypass space \
         membership for listed DIDs. Remove one."
    );

    // Refuse to start as an open relay (non-loopback bind, no auth) unless
    // the operator explicitly acknowledges via KUTL_RELAY_ALLOW_OPEN_RELAY.
    // Headless deployments (Docker, systemd) often miss startup warnings,
    // so the only safe default is fail-closed with a directive message.
    let unsafe_open_relay =
        !config::is_loopback_host(&config.host) && !config.require_auth && !config.allow_open_relay;
    assert!(
        !unsafe_open_relay,
        "misconfigured: this configuration would run an unauthenticated relay \
         reachable on the network (KUTL_RELAY_HOST={}, KUTL_RELAY_REQUIRE_AUTH=false). \
         Pick one:\n  \
         Dev:  set KUTL_RELAY_ALLOW_OPEN_RELAY=true (insecure; intended for local dev)\n  \
         Prod: set KUTL_RELAY_AUTHORIZED_KEYS_FILE=<path> and KUTL_RELAY_REQUIRE_AUTH=true",
        config.host
    );

    // Operator opted into running an open relay. Log a startup warning and
    // spawn a recurring reminder so the notice appears in any reasonable
    // log retention window — startup-only logs get rotated out.
    let open_relay =
        !config::is_loopback_host(&config.host) && !config.require_auth && config.allow_open_relay;
    if open_relay {
        tracing::warn!(
            bind = %config.host,
            port = config.port,
            "OPEN RELAY: KUTL_RELAY_ALLOW_OPEN_RELAY=true is set; \
             accepting connections without authentication. \
             For production, set KUTL_RELAY_AUTHORIZED_KEYS_FILE and \
             KUTL_RELAY_REQUIRE_AUTH=true."
        );
        let host = config.host.clone();
        let port = config.port;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(OPEN_RELAY_REMINDER_INTERVAL);
            interval.tick().await; // skip immediate first tick
            loop {
                interval.tick().await;
                tracing::warn!(
                    bind = %host,
                    port = port,
                    "OPEN RELAY still running: no authentication required for \
                     incoming connections"
                );
            }
        });
    }

    let outbound_capacity = config.outbound_capacity;
    let require_auth = config.require_auth;
    let external_url = config.external_url.clone();
    let ux_url = config.ux_url.clone();
    let host = config.host.clone();
    let port = config.port;
    let has_space_backend = space_backend.is_some();
    let invite_backend_arc: Option<Arc<dyn invite_backend::InviteBackend>> =
        invite_backend.map(Arc::from);

    let (relay_tx, relay_rx) = mpsc::channel(RELAY_CHANNEL_CAPACITY);
    let relay = Relay::new(
        config,
        relay_rx,
        session_backend,
        pat_backend,
        membership_backend,
        registry_backend.clone(),
        space_backend,
        content_backend.clone(),
        blob_backend.clone(),
        relay_tx.clone(),
        observer,
        before_merge,
        after_merge,
        change_backend,
        quota_backend.clone(),
    );
    let relay_handle = tokio::spawn(relay.run());

    let flush_handle = flush::spawn_flush_task(
        relay_tx.clone(),
        content_backend,
        blob_backend,
        quota_backend,
        registry_backend,
    );

    let state = AppState {
        relay_tx,
        conn_counter: Arc::new(AtomicU64::new(1)),
        outbound_capacity,
        require_auth,
        external_url: external_url.clone(),
        ux_url,
        host: host.clone(),
        port,
        tool_provider,
        instructions_provider,
    };

    let mut router = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ws", get(ws_upgrade))
        .route("/auth/challenge", post(auth_challenge))
        .route("/auth/verify", post(auth_verify))
        .route("/auth/device", post(auth_device))
        .route("/auth/device/token", post(auth_device_token))
        .route("/auth/device/authorize", post(auth_device_authorize))
        .route(
            "/mcp",
            post(mcp_handler::mcp_post)
                .get(mcp_handler::mcp_get)
                .delete(mcp_handler::mcp_delete),
        )
        .route(
            "/spaces/{space_id}/documents/{document_id}/text",
            get(text_export::handle_export_text),
        );

    // Space registration routes are only added when a space backend is configured
    // (OSS relay with data_dir). kutlhub-relay manages spaces via the UX server.
    if has_space_backend {
        router = router
            .route("/spaces/register", post(handle_register_space))
            .route("/spaces/resolve", get(handle_resolve_space));
    }

    // Invite routes are only added when an invite backend is configured
    // (OSS relay with data_dir). kutlhub-relay manages invitations via the
    // UX server.
    //
    // When a web_space_backend is also provided, mount the HTML admin pages
    // and form handlers alongside the JSON API routes. The `/join/{code}`
    // route serves HTML for browsers; the existing `/invites/{code}` route
    // remains the JSON endpoint for programmatic access.
    if let Some(ib) = invite_backend_arc {
        router = router
            .route(
                "/invites",
                post(invite_routes::handle_create_invite).get(invite_routes::handle_list_invites),
            )
            .route(
                "/invites/{code}",
                get(invite_routes::handle_get_invite).delete(invite_routes::handle_delete_invite),
            );

        if let Some(wsb) = web_space_backend {
            let web_state = web::WebState::new(external_url.as_deref(), &host, port);
            router = router
                .route("/", get(web::handle_index))
                .route("/spaces/{space_id}", get(web::handle_space_detail))
                .route("/join/{code}", get(web::handle_join_page))
                .route("/invites/form", post(web::handle_create_invite_form))
                .route("/invites/revoke", post(web::handle_revoke_invite_form))
                .layer(axum::Extension(web_state))
                .layer(axum::Extension(wsb));
        }

        router = router.layer(axum::Extension(ib));
    }

    let router = router.with_state(state);

    (router, relay_handle, flush_handle)
}

/// WebSocket upgrade handler.
async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> axum::response::Response {
    let conn_id = state.conn_counter.fetch_add(1, Ordering::Relaxed);
    let capacity = state.outbound_capacity;
    // Codec-layer hard cap: reject any frame/message larger than `ABSOLUTE_BLOB_MAX`
    // before it reaches application code. Per-tier quotas enforced separately via
    // `QuotaBackend`.
    ws.max_message_size(ABSOLUTE_BLOB_MAX)
        .max_frame_size(ABSOLUTE_BLOB_MAX)
        .on_upgrade(move |socket| conn::run_connection(socket, state.relay_tx, conn_id, capacity))
}

// ---------------------------------------------------------------------------
// Auth HTTP endpoints
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ChallengeRequest {
    did: String,
}

#[derive(Serialize)]
struct ChallengeResponseBody {
    nonce: String,
    expires_at: i64,
}

#[derive(Deserialize)]
struct VerifyRequest {
    did: String,
    nonce: String,
    signature: String,
}

#[derive(Serialize)]
struct VerifyResponseBody {
    token: String,
    expires_at: i64,
}

/// `POST /auth/challenge` — request a DID challenge nonce.
async fn auth_challenge(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<ChallengeRequest>,
) -> Result<axum::Json<ChallengeResponseBody>, AuthErrorResponse> {
    let (reply_tx, reply_rx) = oneshot::channel();
    state
        .relay_tx
        .send(RelayCommand::AuthChallenge {
            did: req.did,
            reply: reply_tx,
        })
        .await
        .map_err(|_| AuthErrorResponse::internal("relay channel closed"))?;

    let result = reply_rx
        .await
        .map_err(|_| AuthErrorResponse::internal("relay did not respond"))?;

    match result {
        Ok(resp) => Ok(axum::Json(ChallengeResponseBody {
            nonce: resp.nonce,
            expires_at: resp.expires_at,
        })),
        Err(ref e) => Err(AuthErrorResponse::from_auth_error(e)),
    }
}

/// `POST /auth/verify` — submit a signed challenge for token issuance.
async fn auth_verify(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<VerifyRequest>,
) -> Result<axum::Json<VerifyResponseBody>, AuthErrorResponse> {
    let (reply_tx, reply_rx) = oneshot::channel();
    state
        .relay_tx
        .send(RelayCommand::AuthVerify {
            did: req.did,
            nonce: req.nonce,
            signature: req.signature,
            reply: reply_tx,
        })
        .await
        .map_err(|_| AuthErrorResponse::internal("relay channel closed"))?;

    let result = reply_rx
        .await
        .map_err(|_| AuthErrorResponse::internal("relay did not respond"))?;

    match result {
        Ok(resp) => Ok(axum::Json(VerifyResponseBody {
            token: resp.token,
            expires_at: resp.expires_at,
        })),
        Err(ref e) => Err(AuthErrorResponse::from_auth_error(e)),
    }
}

/// Error response for auth endpoints.
struct AuthErrorResponse {
    status: StatusCode,
    message: String,
}

impl AuthErrorResponse {
    fn internal(msg: &str) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.to_owned(),
        }
    }

    fn from_auth_error(e: &auth::AuthError) -> Self {
        use auth::AuthError;
        let status = match e {
            AuthError::AuthNotRequired
            | AuthError::InvalidDid(_)
            | AuthError::MalformedSignature(_)
            | AuthError::MalformedNonce(_) => StatusCode::BAD_REQUEST,
            AuthError::ChallengeNotFound
            | AuthError::ChallengeExpired
            | AuthError::InvalidSignature
            | AuthError::TokenNotFound
            | AuthError::TokenExpired => StatusCode::UNAUTHORIZED,
            AuthError::AuthorizationPending => StatusCode::PRECONDITION_REQUIRED,
            AuthError::DeviceCodeExpired => StatusCode::GONE,
            AuthError::InvalidDeviceCode | AuthError::InvalidUserCode => StatusCode::NOT_FOUND,
        };
        Self {
            status,
            message: e.to_string(),
        }
    }
}

impl IntoResponse for AuthErrorResponse {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            axum::Json(serde_json::json!({"error": self.message})),
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Device auth flow HTTP endpoints
// ---------------------------------------------------------------------------

/// Recommended polling interval for device flow (seconds).
///
/// RFC 8628 Section 3.5 specifies a minimum of 5 seconds.
const DEVICE_POLL_INTERVAL_SECS: u32 = 5;

/// Response body for `POST /auth/device` (device authorization initiation).
#[derive(Serialize)]
struct DeviceRequestBody {
    /// Opaque device code for CLI polling.
    device_code: String,
    /// Human-readable code displayed to the user (e.g. `ABCD-1234`).
    user_code: String,
    /// URL where the user should enter the code.
    verification_url: String,
    /// Recommended polling interval in seconds.
    interval: u32,
}

/// Request body for `POST /auth/device/token` (device token polling).
#[derive(Deserialize)]
struct DeviceTokenRequest {
    /// The device code returned from the initiation request.
    device_code: String,
}

/// Response body for `POST /auth/device/token` (successful authorization).
#[derive(Serialize)]
struct DeviceTokenBody {
    /// Bearer token for relay authentication.
    token: String,
    /// Account identifier of the authorized user.
    account_id: String,
    /// Display name of the authorized user.
    display_name: String,
    /// Relay URL for the CLI to connect to.
    relay_url: String,
}

/// Request body for `POST /auth/device/authorize` (UX server approval).
#[derive(Deserialize)]
struct DeviceAuthorizeRequest {
    /// The user code entered by the user on the `/device` page.
    user_code: String,
    /// Bearer token issued by the relay for the authorized user.
    token: String,
    /// Account identifier of the authorized user.
    account_id: String,
    /// Display name of the authorized user.
    display_name: String,
}

/// `POST /auth/device` — initiate a device authorization flow.
async fn auth_device(
    State(state): State<AppState>,
) -> Result<axum::Json<DeviceRequestBody>, AuthErrorResponse> {
    let (reply_tx, reply_rx) = oneshot::channel();
    state
        .relay_tx
        .send(RelayCommand::CreateDeviceRequest { reply: reply_tx })
        .await
        .map_err(|_| AuthErrorResponse::internal("relay channel closed"))?;

    let result = reply_rx
        .await
        .map_err(|_| AuthErrorResponse::internal("relay did not respond"))?;

    match result {
        Ok(resp) => {
            let verification_base = state
                .ux_url
                .as_deref()
                .or(state.external_url.as_deref())
                .map_or_else(
                    || format!("http://{}:{}", state.host, state.port),
                    ToString::to_string,
                );
            let verification_url = format!("{verification_base}/device");
            Ok(axum::Json(DeviceRequestBody {
                device_code: resp.device_code,
                user_code: resp.user_code,
                verification_url,
                interval: DEVICE_POLL_INTERVAL_SECS,
            }))
        }
        Err(ref e) => Err(AuthErrorResponse::from_auth_error(e)),
    }
}

/// `POST /auth/device/token` — poll for device authorization completion.
///
/// Returns the token on success, 428 if authorization is pending, 410 if expired.
async fn auth_device_token(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<DeviceTokenRequest>,
) -> Result<axum::Json<DeviceTokenBody>, AuthErrorResponse> {
    let (reply_tx, reply_rx) =
        oneshot::channel::<Result<auth::DeviceTokenResponse, auth::AuthError>>();
    state
        .relay_tx
        .send(RelayCommand::PollDevice {
            device_code: req.device_code,
            reply: reply_tx,
        })
        .await
        .map_err(|_| AuthErrorResponse::internal("relay channel closed"))?;

    let result = reply_rx
        .await
        .map_err(|_| AuthErrorResponse::internal("relay did not respond"))?;

    match result {
        Ok(resp) => {
            let relay_url = state.external_url.as_deref().map_or_else(
                || format!("http://{}:{}", state.host, state.port),
                ToString::to_string,
            );
            Ok(axum::Json(DeviceTokenBody {
                token: resp.token,
                account_id: resp.account_id,
                display_name: resp.display_name,
                relay_url,
            }))
        }
        Err(ref e) => Err(AuthErrorResponse::from_auth_error(e)),
    }
}

/// Validate a bearer token from the `Authorization` header.
///
/// Extracts the token, sends it to the relay for validation, and returns
/// an `AuthErrorResponse` on failure. Used by auth endpoints that require
/// an authenticated caller.
/// Validate a bearer token and return the authenticated identity string.
async fn validate_bearer_token(
    relay_tx: &mpsc::Sender<RelayCommand>,
    headers: &axum::http::HeaderMap,
) -> Result<String, AuthErrorResponse> {
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AuthErrorResponse {
            status: StatusCode::UNAUTHORIZED,
            message: "missing authorization header".to_owned(),
        })?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or_else(|| AuthErrorResponse {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid authorization header format".to_owned(),
        })?;

    let (reply_tx, reply_rx) = oneshot::channel();
    relay_tx
        .send(RelayCommand::McpValidateToken {
            token: token.to_owned(),
            reply: reply_tx,
        })
        .await
        .map_err(|_| AuthErrorResponse::internal("relay channel closed"))?;

    let (identity, _pat_hash) = reply_rx
        .await
        .map_err(|_| AuthErrorResponse::internal("relay did not respond"))?
        .map_err(|_| AuthErrorResponse {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid or expired token".to_owned(),
        })?;

    Ok(identity)
}

/// `POST /auth/device/authorize` — authorize a pending device request.
///
/// Called by the UX server after the user approves the login. Requires a
/// valid bearer token in the `Authorization` header to prevent unauthorized
/// injection into the device flow.
async fn auth_device_authorize(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    axum::Json(req): axum::Json<DeviceAuthorizeRequest>,
) -> Result<StatusCode, AuthErrorResponse> {
    // Validate the caller's bearer token. The device flow authorize
    // endpoint is a trusted-caller pattern: the UX server authenticates
    // with its own service token and provides the account_id of the user
    // who approved on the web page. The caller's identity intentionally
    // differs from the account_id being authorized.
    let _caller = validate_bearer_token(&state.relay_tx, &headers).await?;

    let (reply_tx, reply_rx) = oneshot::channel();
    state
        .relay_tx
        .send(RelayCommand::AuthorizeDevice {
            user_code: req.user_code,
            token: req.token,
            account_id: req.account_id,
            display_name: req.display_name,
            reply: reply_tx,
        })
        .await
        .map_err(|_| AuthErrorResponse::internal("relay channel closed"))?;

    let result = reply_rx
        .await
        .map_err(|_| AuthErrorResponse::internal("relay did not respond"))?;

    match result {
        Ok(()) => Ok(StatusCode::OK),
        Err(ref e) => Err(AuthErrorResponse::from_auth_error(e)),
    }
}

// ---------------------------------------------------------------------------
// Space HTTP endpoints (OSS only)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct RegisterSpaceRequest {
    name: String,
}

#[derive(Serialize)]
struct RegisterSpaceResponse {
    space_id: String,
    name: String,
}

/// `POST /spaces/register` — create a new space.
async fn handle_register_space(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<RegisterSpaceRequest>,
) -> Result<
    (StatusCode, axum::Json<RegisterSpaceResponse>),
    (StatusCode, axum::Json<serde_json::Value>),
> {
    let (reply_tx, reply_rx) = oneshot::channel();
    state
        .relay_tx
        .send(RelayCommand::RegisterSpace {
            name: req.name,
            reply: reply_tx,
        })
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({"error": "relay channel closed"})),
            )
        })?;

    let result = reply_rx.await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": "relay did not respond"})),
        )
    })?;

    match result {
        Ok(space) => Ok((
            StatusCode::CREATED,
            axum::Json(RegisterSpaceResponse {
                space_id: space.space_id,
                name: space.name,
            }),
        )),
        Err(space_backend::SpaceBackendError::NameConflict(_)) => Err((
            StatusCode::CONFLICT,
            axum::Json(serde_json::json!({"error": "space name already taken"})),
        )),
        Err(space_backend::SpaceBackendError::InvalidName(ref msg)) => Err((
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({"error": msg})),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": e.to_string()})),
        )),
    }
}

#[derive(Deserialize)]
struct ResolveSpaceQuery {
    name: String,
}

/// `GET /spaces/resolve?name=...` — look up a space by name.
async fn handle_resolve_space(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ResolveSpaceQuery>,
) -> Result<axum::Json<RegisterSpaceResponse>, (StatusCode, axum::Json<serde_json::Value>)> {
    let (reply_tx, reply_rx) = oneshot::channel();
    state
        .relay_tx
        .send(RelayCommand::ResolveSpace {
            name: query.name,
            reply: reply_tx,
        })
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({"error": "relay channel closed"})),
            )
        })?;

    let result = reply_rx.await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": "relay did not respond"})),
        )
    })?;

    match result {
        Ok(Some(space)) => Ok(axum::Json(RegisterSpaceResponse {
            space_id: space.space_id,
            name: space.name,
        })),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error": "space not found"})),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": e.to_string()})),
        )),
    }
}
