//! kutl-relay — WebSocket sync relay for real-time collaboration.

pub mod acl;
pub mod auth;
pub mod authorized_keys;
pub mod blob_backend;
pub mod change_backend;
pub mod change_sqlite;
pub mod config;
pub mod content_backend;
pub mod doc_version;
pub mod flush;
pub mod http;
pub mod identity;
pub(crate) mod identity_error;
pub(crate) mod ids;
pub mod invite_backend;
pub mod invite_routes;
mod load_class;
pub mod markers;
pub mod mcp;
pub mod mcp_handler;
pub mod mcp_tools;
pub mod membership_backend;
pub mod observer;
pub mod pat_backend;
pub mod projection;
pub mod protocol;
pub mod quota_backend;
pub mod record_log;
pub mod registry;
pub mod registry_store;
pub mod relay;
pub mod relay_policy;
pub mod session_backend;
pub(crate) mod signal_record;
pub mod signal_store;
pub mod space_backend;
pub mod spaces;
pub mod sqlite_invite_backend;
pub mod telemetry;
pub mod text_export;
pub mod web;

pub use observer::{
    AfterMergeObserver, BeforeMergeObserver, DocumentRegisteredEvent, DocumentRenamedEvent,
    DocumentUnregisteredEvent, EditContentMode, MergedEvent, NoopAfterMergeObserver,
    NoopBeforeMergeObserver, NoopObserver, ReactionEvent, RelayObserver, SignalRecordEvent,
    TracingObserver,
};

pub use markers::{
    before_merge::BaselineCapture,
    code_fence::skip_fenced_lines,
    comments::{CommentDiff, CommentTracker, parse_comment_markers},
    decisions::{
        DECISION_OPEN_MARKER, DECISION_RESOLVED_MARKER, DecisionDiff, DecisionTracker,
        parse_decisions,
    },
    in_doc_flag::{InDocFlagDiff, InDocFlagTracker, parse_in_doc_flags},
};

mod conn;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::extract::State;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use config::RelayConfig;
use protocol::ABSOLUTE_BLOB_MAX;
use relay::{Relay, RelayCommand, RelayDispatchError, dispatch_relay_command};

/// Bridge a synchronous backend-trait method onto the async runtime.
///
/// The relay actor calls `RegistryBackend` / `InviteBackend` synchronously, so
/// their DB work runs through `block_in_place` + `Handle::block_on`. This
/// requires the multi-thread tokio runtime (`block_in_place` panics on a
/// `current_thread` runtime) and must be called from within a runtime context.
pub fn block_on_sync<T>(fut: impl Future<Output = T>) -> T {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
}

/// Storeless-relay constructors for consumer test suites (behind the
/// off-by-default `testing` feature; see the module header for the dev-only
/// contract).
#[cfg(feature = "testing")]
pub mod testing;

/// Capacity of the mpsc channel from connection tasks to the relay actor.
const RELAY_CHANNEL_CAPACITY: usize = 256;

/// Shared application state passed to axum handlers.
#[derive(Clone)]
struct AppState {
    relay_tx: mpsc::Sender<RelayCommand>,
    conn_counter: Arc<AtomicU64>,
    outbound_capacity: usize,
    /// External URL for constructing verification URLs in device flow.
    external_url: Option<String>,
    /// UX server base URL for user-facing pages (e.g. `/device`).
    ux_url: Option<String>,
    /// Bind host for fallback URL construction.
    host: String,
    /// Bind port for fallback URL construction.
    port: u16,
    /// MCP tool provider for extension hosts (or no-op by default).
    tool_provider: Arc<dyn mcp_tools::McpToolProvider>,
    /// MCP instructions provider for cloud-mode content. The default is
    /// `DefaultInstructionsProvider`; extension hosts may override it.
    instructions_provider: Arc<dyn mcp_tools::McpInstructionsProvider>,
    /// Maximum decoded blob size for `upload_blob`.
    /// Sourced from `RelayConfig.max_blob_bytes` / `KUTL_MAX_BLOB_BYTES`.
    pub(crate) max_blob_bytes: usize,
    /// Whether an HTTP invite backend is wired (for the advertised `RelayPolicy`).
    has_invite_backend: bool,
    /// Whether a membership backend is wired (for the advertised `RelayPolicy`).
    has_membership_backend: bool,
}

/// `GET /relay-policy` — advertise the relay's onboarding policy,
/// unauthenticated and pre-connect, so a client can learn the auth/invite model
/// before creating identity or opening a WebSocket.
async fn handle_relay_policy(
    State(state): State<AppState>,
) -> axum::Json<kutl_proto::sync::RelayPolicy> {
    // Authentication is mandatory: the relay always asks the client
    // to authenticate before proceeding.
    let next = kutl_proto::sync::NextStep::Authenticate;
    // Prefer the configured external URL; otherwise derive one from the bind
    // address via the same wildcard-rejecting helper the device-flow path uses,
    // so we never advertise a non-routable `http://0.0.0.0:port`. If even that
    // is unavailable, leave auth_url empty (the client falls back to the URL it
    // already reached the relay on).
    let auth_url = state
        .external_url
        .clone()
        .or_else(|| fallback_base_url(&state.host, state.port).ok())
        .unwrap_or_default();
    let inputs = relay_policy::RelayPolicyInputs {
        has_invite_backend: state.has_invite_backend,
        has_membership_backend: state.has_membership_backend,
        auth_url,
    };
    axum::Json(relay_policy::build_relay_policy(&inputs, next))
}

/// Build the axum router and spawn the relay actor (open-source entry point).
///
/// The OSS relay always persists its metadata and signal stores:
/// `config.data_dir` is required, and this bails if it is `None`. A single
/// shared `SQLite` pool is opened and used by all backends (registry, spaces,
/// invites, changes) to avoid lock contention, and the relay signing identity
/// is loaded from the same directory.
///
/// Delegates to [`build_app_with_backends`] with `None` for content and blob
/// backends — the OSS relay holds CRDT content and blobs in memory only;
/// clients re-seed.
///
/// Returns `(Router, relay_handle, flush_handle)`. The flush handle is always
/// `None` here because the OSS relay has no content backend.
///
/// # Errors
///
/// Returns an error if `config.data_dir` is unset, or if the registry backend
/// or signing identity cannot be opened.
pub async fn build_app(
    config: RelayConfig,
) -> anyhow::Result<(Router, JoinHandle<()>, Option<JoinHandle<()>>)> {
    let Some(ref data_dir) = config.data_dir else {
        anyhow::bail!("data dir required: set KUTL_RELAY_DATA_DIR");
    };
    let pool = open_sqlite_pool(data_dir).await?;

    // Load the signing identity ONCE per boot. The backfill and
    // the router build both need it; loading here and threading the result
    // means exactly one disk read + one abort decision (a malformed
    // identity aborts startup — see `resolve_relay_identity`).
    //
    // `data_dir` is required on this path, so the resolution can only return
    // `None` if it were dropped — hence the expect rather than an `Option`
    // threaded through a function that has already proven it Some.
    let relay_identity = resolve_relay_identity(&config)?
        .expect("data dir is required on this path, so an identity always resolves");

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
    let change_sqlite = change_sqlite::SqliteChangeBackend::new(pool)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    // Over one reading of each space's fold, clear phantom projection rows
    // (unacked residue of a power cut that kept the row and lost the segment
    // append) and rebuild the projection from the fold. Idempotent, so it is
    // safe at every startup; a per-space failure is skipped-and-warned.
    reconcile_projections_at_startup(&change_sqlite, data_dir).await?;
    let cb: Arc<dyn change_backend::SignalProjection> = Arc::new(change_sqlite);

    let instructions_provider: Arc<dyn mcp_tools::McpInstructionsProvider> =
        Arc::new(mcp_tools::DefaultInstructionsProvider);

    build_app_with_backends_and_web(
        config,
        None,
        None,
        None,
        Some(registry),
        Some(sb),
        Some(web_sb),
        None,
        None,
        Some(ib),
        Some(cb),
        None,
        // No override: this path is the OSS binary, whose log is the segment
        // implementation over `config.data_dir` (constructed below).
        None,
        Arc::new(observer::TracingObserver),
        Arc::new(observer::NoopBeforeMergeObserver),
        // The OSS relay's after-merge observer is the record materializer,
        // constructed inside `build_app_with_backends_and_web` from the relay
        // identity + the dedicated unbounded materialize channel.
        None,
        Arc::new(mcp_tools::NoopToolProvider),
        instructions_provider,
        // Reuse the identity already loaded above for the backfill: one load +
        // one abort decision per boot (see `load_relay_identity`).
        Some(relay_identity),
    )
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

/// Load the relay's signing identity from `data_dir`, aborting on corruption.
///
/// The SINGLE identity-load funnel for one boot: [`build_app`] calls this ONCE
/// early and threads the result into the backfill and the router build, so a
/// boot does exactly one disk read and makes exactly one abort decision.
///
/// - **absent** `identity.json` → generate + persist a fresh identity;
/// - **present + valid** → load it;
/// - **present + malformed** → propagate the error so the caller ABORTS
///   startup. Regenerating would silently flip the relay DID (breaking
///   verification of prior-run signatures); degrading to tier-3 would silently
///   drop attestation. Neither is acceptable, so a corrupt identity is fatal.
///
/// # Errors
///
/// Returns an error only when `identity.json` is present but malformed, or when
/// a freshly-generated identity cannot be persisted. An absent file is not an
/// error.
fn load_relay_identity(data_dir: &std::path::Path) -> anyhow::Result<Arc<identity::RelayIdentity>> {
    let id = identity::RelayIdentity::load_or_generate(data_dir)
        .map_err(|e| anyhow::anyhow!("relay signing identity is corrupt: {e}"))?;
    tracing::info!(relay_did = %id.did(), "loaded relay signing identity");
    Ok(Arc::new(id))
}

/// Resolve which identity — if any — this relay runs with.
///
/// The single place the two config keys are ranked, so no caller has to
/// re-derive the precedence:
///
/// 1. `identity_dir`, when set, **wins** and is loaded strictly: an absent file
///    aborts startup rather than minting one. That is what makes an identity
///    provisionable to a deployment that has no segment store, which is the
///    whole reason the key exists — `data_dir` cannot serve that role, because
///    it also constructs the record log.
/// 2. `data_dir` otherwise, loaded permissively — a self-hoster's first start
///    legitimately has no identity yet, and generating one is correct there.
/// 3. Neither: an identity-less relay (standalone test and sim actors). Every
///    attestation site is `Option`-gated, so this stays a supported mode.
///
/// # Errors
///
/// Returns an error when the resolved identity file is present-but-malformed,
/// when an explicitly configured one is absent, or when a freshly generated one
/// cannot be persisted. Every case is fatal by design.
fn resolve_relay_identity(
    config: &RelayConfig,
) -> anyhow::Result<Option<Arc<identity::RelayIdentity>>> {
    if let Some(dir) = config.identity_dir.as_deref() {
        let id = identity::RelayIdentity::load(dir)
            .map_err(|e| anyhow::anyhow!("relay signing identity could not be loaded: {e}"))?;
        tracing::info!(
            relay_did = %id.did(),
            identity_dir = %dir.display(),
            "loaded relay signing identity from the configured identity dir"
        );
        return Ok(Some(Arc::new(id)));
    }
    config
        .data_dir
        .as_deref()
        .map(load_relay_identity)
        .transpose()
}

/// Clear phantom projection rows and rebuild every space's projection from
/// its segment fold, at relay startup.
///
/// Two jobs over one reading of one fold, per space:
///
/// 1. **Clear.** A projection row with no record behind it is deleted, via
///    [`change_sqlite::SqliteChangeBackend::clear_recordless_rows`]. Such a
///    row can only be residue of a power cut that kept the row's durable
///    commit and lost the un-fsynced segment append — and the ack a caller
///    waits on follows both, so nothing acked can be one. See the clear's
///    own doc for why deletion (not re-minting) is the remedy, and why only
///    startup may do it.
/// 2. **Reconcile.** Rebuild the projection from the fold, healing the
///    divergence the other direction: a CLOSED whose record was lost leaves a
///    row claiming a close the segments no longer support.
///
/// One loop over one fold, so both steps see the same records.
///
/// A per-space failure at either step is **skipped-and-warned**, never
/// propagated: one bad space (a non-UUID `space_id`, a segment IO fault, a
/// torn header) must not brick relay startup. The projection is rebuildable,
/// so good spaces still complete, a skipped space realigns on its next
/// mutation, and the relay always starts.
///
/// Idempotent: a space whose rows all have records clears nothing, and the
/// rebuild is order-insensitive.
///
/// # Errors
///
/// Returns an error only if enumerating spaces fails. Per-space failures are
/// skipped-and-warned, not returned.
async fn reconcile_projections_at_startup(
    change: &change_sqlite::SqliteChangeBackend,
    data_dir: &std::path::Path,
) -> anyhow::Result<()> {
    // Same signals root the relay actor loads, via the one rooting helper so a
    // rename can never desync startup from the actor's store.
    let store = signal_store::SignalStore::new(signal_store::SignalStore::root_for(data_dir));

    // The union of spaces that have segments on disk and spaces that still have
    // projection rows. Segments cover native + healed spaces; the row set
    // additionally catches a space whose every segment was lost — which has
    // both a phantom row to clear and rows to heal.
    let mut spaces: std::collections::BTreeSet<String> = store
        .space_ids_on_disk()
        .map_err(|e| anyhow::anyhow!("failed to list on-disk signal spaces: {e}"))?
        .into_iter()
        .map(|u| u.to_string())
        .collect();
    for space in change
        .spaces_with_signal_rows()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list spaces with signal rows: {e}"))?
    {
        spaces.insert(space);
    }
    if spaces.is_empty() {
        return Ok(());
    }

    let mut cleared_total = 0u64;
    let mut reconciled = 0usize;
    for space in &spaces {
        // A non-UUID space id can only come from a legacy signal row (segments
        // are always UUID-named dirs); the store needs a UUID, so skip it.
        let space_uuid = match uuid::Uuid::parse_str(space) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(
                    space_id = %space,
                    error = %e,
                    "skipping projection reconcile for non-UUID space; relay continues starting"
                );
                continue;
            }
        };
        // ONE load per space, shared by both steps below.
        let records = match store.load_and_warn(space_uuid) {
            Ok(loaded) => loaded.records,
            Err(e) => {
                tracing::warn!(
                    space_id = %space,
                    error = %e,
                    "skipping projection reconcile for space (segment load failed); \
                     relay continues starting"
                );
                continue;
            }
        };

        match change.clear_recordless_rows(space, &records).await {
            Ok(cleared) => {
                if !cleared.is_empty() {
                    cleared_total += cleared.len() as u64;
                    tracing::warn!(
                        space_id = %space,
                        signal_ids = ?cleared,
                        "cleared phantom projection rows with no record behind them \
                         (unacked crash residue; a record any replica holds re-seeds back in)"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    space_id = %space,
                    error = %e,
                    "skipping phantom-row clear for space; relay continues starting"
                );
            }
        }

        match change.reconcile_space_projection(space, &records).await {
            Ok(()) => reconciled += 1,
            Err(e) => {
                tracing::warn!(
                    space_id = %space,
                    error = %e,
                    "skipping projection reconcile for space (rebuild failed); \
                     relay continues starting"
                );
            }
        }
    }
    if cleared_total > 0 {
        tracing::info!(
            rows = cleared_total,
            "cleared phantom projection rows at startup"
        );
    }
    if reconciled > 0 {
        tracing::info!(
            spaces = reconciled,
            "reconciled signal projections from segment fold at startup"
        );
    }
    Ok(())
}

/// Build the axum router with explicit storage backends.
///
/// This is the entry point for hosts that provide their own backends
/// (object store, SQL database). The default [`build_app`] delegates
/// here with `None` for content/blob backends.
///
/// When `invite_backend` is `Some`, the invite HTTP routes
/// (`POST /invites`, `GET /invites`, `GET /invites/:code`,
/// `DELETE /invites/:code`) are mounted. Hosts that manage invitations
/// elsewhere pass `None`.
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
    change_backend: Option<Arc<dyn change_backend::SignalProjection>>,
    quota_backend: Option<Arc<dyn quota_backend::QuotaBackend>>,
    // Where this deployment keeps signal records. `None` falls
    // back to segments under `config.data_dir`, the self-hoster's shape; a host
    // that keeps records elsewhere supplies its own.
    record_log: Option<Arc<dyn record_log::RecordLog>>,
    observer: Arc<dyn observer::RelayObserver>,
    before_merge: Arc<dyn observer::BeforeMergeObserver>,
    after_merge: HostPublisher,
    tool_provider: Arc<dyn mcp_tools::McpToolProvider>,
    instructions_provider: Arc<dyn mcp_tools::McpInstructionsProvider>,
) -> anyhow::Result<(Router, JoinHandle<()>, Option<JoinHandle<()>>)> {
    build_app_with_backends_and_web(
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
        record_log,
        observer,
        before_merge,
        // A host's optional feed publisher. The materializer runs on EVERY
        // relay — this only fans its parse out.
        after_merge,
        tool_provider,
        instructions_provider,
        // Host path: no identity was pre-loaded — load it inside if a data dir
        // is configured.
        None,
    )
}

/// The host's optional after-merge feed publisher.
///
/// **Every relay materializes.** The materializer is the single parse of the
/// document's markers, and a host attaches a [`AfterMergePublisher`] to fan
/// that parse out to its own feed. A host observer that REPLACED the
/// materializer would run its own copy of the marker trackers and mint no
/// records at all — a decision typed in the browser and one typed on disk
/// could not then produce the same record, because one of them would produce
/// no record.
///
/// [`AfterMergePublisher`]: markers::materialize::AfterMergePublisher
type HostPublisher = Option<Arc<dyn markers::materialize::AfterMergePublisher>>;

/// Resolve the after-merge observer and the actor's materialize receiver.
///
/// Always builds the OSS record materializer
/// here — it needs `relay_identity` and its OWN dedicated UNBOUNDED channel,
/// both of which live only inside `build_app`. Creating the channel here
/// resolves the wiring order (the observer takes the sender; the actor takes the
/// receiver) and, unlike the shared bounded command channel, never drops a
/// materialized batch under backpressure — which would silently lose a
/// user-authored signal. Hosts (kutlhub) attach their own feed publisher via
/// the publisher hook and share the one materializer.
///
/// `signal_clock` is the relay's shared HLC clock (built once in `build_app`):
/// the OSS materializer is cloned onto it so its records and the actor's own
/// MCP transitions stamp from one monotonic source. Hosts' provided
/// publishers do not stamp records, so they ignore it.
fn resolve_after_merge(
    publisher: HostPublisher,
    relay_identity: Option<Arc<identity::RelayIdentity>>,
    signal_clock: relay::SharedSignalClock,
    directory: Option<Arc<dyn membership_backend::MembershipBackend>>,
) -> (
    Arc<dyn observer::AfterMergeObserver>,
    Option<markers::materialize::MaterializeReceiver>,
) {
    let (materialize_tx, materialize_rx) = mpsc::unbounded_channel();
    let materializer = Arc::new(markers::materialize::RecordMaterializingObserver::new(
        materialize_tx,
        relay_identity,
        signal_clock,
        directory,
        publisher,
    ));
    (materializer, Some(materialize_rx))
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
    change_backend: Option<Arc<dyn change_backend::SignalProjection>>,
    quota_backend: Option<Arc<dyn quota_backend::QuotaBackend>>,
    // Where this deployment keeps signal records. `None` falls
    // back to segments under `config.data_dir`, the self-hoster's shape; a host
    // that keeps records elsewhere supplies its own.
    record_log: Option<Arc<dyn record_log::RecordLog>>,
    observer: Arc<dyn observer::RelayObserver>,
    before_merge: Arc<dyn observer::BeforeMergeObserver>,
    after_merge: HostPublisher,
    tool_provider: Arc<dyn mcp_tools::McpToolProvider>,
    instructions_provider: Arc<dyn mcp_tools::McpInstructionsProvider>,
    // The relay signing identity already loaded by [`build_app`] for the
    // startup backfill, threaded here so a boot loads + abort-checks it exactly
    // once. `None` on the host entry point ([`build_app_with_backends`]), where
    // it is loaded below instead.
    preloaded_identity: Option<Arc<identity::RelayIdentity>>,
) -> anyhow::Result<(Router, JoinHandle<()>, Option<JoinHandle<()>>)> {
    // Authentication is mandatory. Refuse to start if there is no
    // way to check space membership: without a membership backend or an
    // authorized_keys file, authorize_space would allow any authenticated
    // identity into any space — which defeats the purpose of requiring auth.
    assert!(
        !(membership_backend.is_none() && config.authorized_keys_file.is_none()),
        "misconfigured: neither a membership backend nor an \
         authorized_keys_file is provided. The relay would authenticate \
         connections but authorize everyone into every space. Either \
         provide a membership backend or set KUTL_RELAY_AUTHORIZED_KEYS_FILE."
    );

    // Refuse to start with BOTH a membership backend and an
    // authorized_keys file. They are two distinct authz modes: the
    // membership backend checks space_memberships (DB-backed mode),
    // while authorized_keys grants blanket access to listed DIDs
    // (no-DB mode). If both are present, the authorized_keys file
    // silently bypasses membership for any DID it contains.
    assert!(
        !(membership_backend.is_some() && config.authorized_keys_file.is_some()),
        "misconfigured: both a membership backend and an \
         authorized_keys_file are configured. These are mutually \
         exclusive authz modes — the keys file would bypass space \
         membership for listed DIDs. Remove one."
    );

    // Backend presence for the advertised RelayPolicy, captured
    // before the backends are moved into the relay actor and route layers.
    let has_invite_backend = invite_backend.is_some();
    let has_membership_backend = membership_backend.is_some();

    // The relay signs records it materializes/attests. Which identity
    // (if any) a deployment runs with is ranked in one place —
    // `resolve_relay_identity` — so the host relay can carry one via
    // `identity_dir` WITHOUT the segment store `data_dir` would also build.
    // `None` here therefore means a relay configured with
    // neither key: a standalone test or sim actor.
    //
    // Every failure is fatal rather than degrading, because both degradations
    // are silent: regenerating flips the relay DID (breaking verification of
    // prior-run signatures) and falling back to tier-3 drops attestation.
    //
    // `build_app` already loaded the identity for the startup backfill and
    // threads it in as `preloaded_identity`, so this path re-uses it (one load +
    // one abort per boot). The host entry point passes `None`; there we resolve
    // it here through the same funnel — the abort is identical either way.
    let relay_identity: Option<Arc<identity::RelayIdentity>> = match preloaded_identity {
        Some(id) => Some(id),
        None => resolve_relay_identity(&config)?,
    };

    // This relay's record log. A caller that keeps its records
    // elsewhere (kutlhub's Postgres log) supplies one; otherwise it is the
    // segment implementation over `config.data_dir` — a self-hoster's disk.
    // `None` on both counts means no log at all: standalone test/sim actors
    // carry none and skip appends.
    let record_log: Option<Arc<dyn record_log::RecordLog>> = record_log.or_else(|| {
        config.data_dir.as_deref().map(|dir| {
            Arc::new(record_log::SegmentRecordLog::rooted_at(dir)) as Arc<dyn record_log::RecordLog>
        })
    });

    let outbound_capacity = config.outbound_capacity;
    let external_url = config.external_url.clone();
    let ux_url = config.ux_url.clone();
    let host = config.host.clone();
    let port = config.port;
    let max_blob_bytes = config.max_blob_bytes;
    let flush_interval = std::time::Duration::from_millis(config.flush_interval_ms);
    let has_space_backend = space_backend.is_some();
    let invite_backend_arc: Option<Arc<dyn invite_backend::InviteBackend>> =
        invite_backend.map(Arc::from);

    let (relay_tx, relay_rx) = mpsc::channel(RELAY_CHANNEL_CAPACITY);

    // One shared HLC clock stamps ALL relay-originated signal records: the
    // actor's MCP create/close/reopen transitions and the OSS materializer's
    // marker-derived records. Built here so both the observer (below)
    // and the actor (`Relay::new`) receive the SAME clone; without this the two
    // paths stamped from independent random-actor clocks and could LWW-resolve
    // same-signal transitions in causally-inverted order.
    let signal_clock = relay::new_signal_clock();

    // Resolve the after-merge observer: for the OSS relay
    // this installs the record materializer wired to `relay_identity`, the
    // shared `signal_clock`, and its own dedicated unbounded channel; hosts get
    // their supplied observer and no channel. See [`resolve_after_merge`].
    let (after_merge, materialize_rx) = resolve_after_merge(
        after_merge,
        relay_identity.clone(),
        signal_clock.clone(),
        membership_backend.clone(),
    );

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
        relay_identity,
        record_log,
        materialize_rx,
        signal_clock,
    );
    let relay_handle = tokio::spawn(relay.run());

    let flush_handle = flush::spawn_flush_task(
        relay_tx.clone(),
        content_backend,
        blob_backend,
        quota_backend,
        registry_backend,
        flush_interval,
    );

    let state = AppState {
        relay_tx,
        conn_counter: Arc::new(AtomicU64::new(1)),
        outbound_capacity,
        external_url: external_url.clone(),
        ux_url,
        host: host.clone(),
        port,
        tool_provider,
        instructions_provider,
        max_blob_bytes,
        has_invite_backend,
        has_membership_backend,
    };

    let mut router = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/relay-policy", get(handle_relay_policy))
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
        )
        // No signal routes here, deliberately: a new relay capability is a new
        // FRAME, never a new route — HTTP is for what happens before a session
        // exists. Authoring goes to SubmitFlag/Comment/Reply/Transition,
        // catch-up to SubscribeSignals, replication to SignalReseed.
        .route(
            "/spaces/{space_id}/changes",
            get(http::changes::handle_changes),
        );

    // Space registration routes are always present on the OSS relay (it
    // always has a space backend); absent only when the host manages spaces
    // elsewhere (kutlhub passes no space backend and serves them from the UX
    // server).
    if has_space_backend {
        router = router
            .route("/spaces/register", post(handle_register_space))
            .route("/spaces/resolve", get(handle_resolve_space));
    }

    // Invite routes are always present on the OSS relay (it always has
    // an invite backend); absent only when the host manages invitations
    // elsewhere (kutlhub passes no invite backend and serves them from the UX
    // server).
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
                .route("/spaces/form", post(web::handle_create_space_form))
                .route("/join/{code}", get(web::handle_join_page))
                .route("/invites/form", post(web::handle_create_invite_form))
                .route("/invites/revoke", post(web::handle_revoke_invite_form))
                .route("/favicon.svg", get(web::handle_favicon))
                .route("/assets/rajdhani.woff2", get(web::handle_font_rajdhani))
                .layer(axum::Extension(web_state))
                .layer(axum::Extension(wsb));
        }

        router = router.layer(axum::Extension(ib));
    }

    let router = router.with_state(state);

    Ok((router, relay_handle, flush_handle))
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
    let result = dispatch_relay_command(&state.relay_tx, |reply| RelayCommand::AuthChallenge {
        did: req.did,
        reply,
    })
    .await?;

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
    let result = dispatch_relay_command(&state.relay_tx, |reply| RelayCommand::AuthVerify {
        did: req.did,
        nonce: req.nonce,
        signature: req.signature,
        reply,
    })
    .await?;

    match result {
        Ok(resp) => Ok(axum::Json(VerifyResponseBody {
            token: resp.token,
            expires_at: resp.expires_at,
        })),
        Err(ref e) => Err(AuthErrorResponse::from_auth_error(e)),
    }
}

/// Error response for auth endpoints.
#[derive(Debug)]
struct AuthErrorResponse {
    status: StatusCode,
    message: String,
}

impl From<RelayDispatchError> for AuthErrorResponse {
    fn from(e: RelayDispatchError) -> Self {
        Self::internal(&e.to_string())
    }
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
            AuthError::InvalidDid(_) | AuthError::MalformedSignature(_) => StatusCode::BAD_REQUEST,
            AuthError::ChallengeNotFound
            | AuthError::ChallengeExpired
            | AuthError::InvalidSignature
            | AuthError::TokenNotFound
            | AuthError::TokenExpired => StatusCode::UNAUTHORIZED,
            AuthError::AuthorizationPending => StatusCode::PRECONDITION_REQUIRED,
            AuthError::DeviceCodeExpired => StatusCode::GONE,
            AuthError::InvalidDeviceCode | AuthError::InvalidUserCode => StatusCode::NOT_FOUND,
            // Transient backend failure — signal the client to retry rather
            // than treat the token as invalid.
            AuthError::BackendUnavailable => StatusCode::SERVICE_UNAVAILABLE,
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

/// Build a fallback base URL (`http://host:port`) from the bind address.
///
/// Refuses to construct a URL when `host` is an unspecified/wildcard
/// address (`0.0.0.0` or `::`): such a value names every interface for
/// *binding* but is not a connectable target for a client, so the
/// resulting URL would be broken. This mirrors the crate's fail-closed
/// posture on non-loopback binds (see `build_app_with_backends`): rather
/// than hand a client an unusable `http://0.0.0.0:.../device` URL, direct
/// the operator to set an explicit external URL.
fn fallback_base_url(host: &str, port: u16) -> Result<String, AuthErrorResponse> {
    if let Ok(ip) = host.parse::<std::net::IpAddr>()
        && ip.is_unspecified()
    {
        tracing::error!(
            host,
            "cannot build a device verification URL from a wildcard bind \
             address; set KUTL_RELAY_EXTERNAL_URL (or KUTL_RELAY_UX_URL)"
        );
        return Err(AuthErrorResponse::internal(
            "relay external URL is not configured; set KUTL_RELAY_EXTERNAL_URL",
        ));
    }
    Ok(format!("http://{host}:{port}"))
}

/// `POST /auth/device` — initiate a device authorization flow.
async fn auth_device(
    State(state): State<AppState>,
) -> Result<axum::Json<DeviceRequestBody>, AuthErrorResponse> {
    let result = dispatch_relay_command(&state.relay_tx, |reply| {
        RelayCommand::CreateDeviceRequest { reply }
    })
    .await?;

    match result {
        Ok(resp) => {
            let verification_base = match state.ux_url.as_deref().or(state.external_url.as_deref())
            {
                Some(base) => base.to_owned(),
                None => fallback_base_url(&state.host, state.port)?,
            };
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
    let result = dispatch_relay_command(&state.relay_tx, |reply| RelayCommand::PollDevice {
        device_code: req.device_code,
        reply,
    })
    .await?;

    match result {
        Ok(resp) => {
            let relay_url = match state.external_url.as_deref() {
                Some(base) => base.to_owned(),
                None => fallback_base_url(&state.host, state.port)?,
            };
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

    let (identity, _pat_ctx) =
        dispatch_relay_command(relay_tx, |reply| RelayCommand::McpValidateToken {
            token: token.to_owned(),
            reply,
        })
        .await?
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

    let result = dispatch_relay_command(&state.relay_tx, |reply| RelayCommand::AuthorizeDevice {
        user_code: req.user_code,
        token: req.token,
        account_id: req.account_id,
        display_name: req.display_name,
        reply,
    })
    .await?;

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
///
/// Intentionally unauthenticated: this is a pre-auth discovery /
/// bootstrap endpoint (`kutl init` calls it before any identity exists), it
/// dispatches via `RelayCommand` with no `conn_id` and never calls
/// `authenticate_request`, and the relay has no per-space ownership model yet.
async fn handle_register_space(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<RegisterSpaceRequest>,
) -> Result<
    (StatusCode, axum::Json<RegisterSpaceResponse>),
    (StatusCode, axum::Json<serde_json::Value>),
> {
    let result = dispatch_relay_command(&state.relay_tx, |reply| RelayCommand::RegisterSpace {
        name: req.name,
        reply,
    })
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": e.to_string()})),
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
///
/// Intentionally unauthenticated: pre-auth discovery, dispatched via
/// `RelayCommand` with no `conn_id`, never calls `authenticate_request`, and
/// there is no per-space ownership model to gate a name lookup.
async fn handle_resolve_space(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ResolveSpaceQuery>,
) -> Result<axum::Json<RegisterSpaceResponse>, (StatusCode, axum::Json<serde_json::Value>)> {
    let result = dispatch_relay_command(&state.relay_tx, |reply| RelayCommand::ResolveSpace {
        name: query.name,
        reply,
    })
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": e.to_string()})),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallback_base_url_routable_host() {
        let url = fallback_base_url("relay.example.com", 8080).expect("routable host is allowed");
        assert_eq!(url, "http://relay.example.com:8080");
    }

    #[test]
    fn test_fallback_base_url_loopback_host() {
        let url = fallback_base_url("127.0.0.1", 9000).expect("loopback host is allowed");
        assert_eq!(url, "http://127.0.0.1:9000");
    }

    #[test]
    fn test_fallback_base_url_rejects_wildcard_v4() {
        let err = fallback_base_url("0.0.0.0", 8080).expect_err("wildcard bind is not connectable");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(err.message.contains("KUTL_RELAY_EXTERNAL_URL"));
    }

    #[test]
    fn test_fallback_base_url_rejects_wildcard_v6() {
        let err = fallback_base_url("::", 8080).expect_err("wildcard bind is not connectable");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// `build_app`'s materializer path
    /// installs the record materializer wired to a dedicated unbounded
    /// materialize channel. Construction smoke test — the end-to-end behavior
    /// (edit → record) lives in the e2e suite. Drives the resolved observer
    /// with a decision-heading merge and asserts a batch lands on the receiver
    /// the actor would drain, proving the observer and the actor's channel are
    /// wired together.
    #[tokio::test]
    async fn test_build_app_installs_materializer() {
        use observer::{EditContentMode, MergedEvent};

        let (observer, rx) = resolve_after_merge(None, None, relay::new_signal_clock(), None);
        let mut rx = rx.expect("Materialize wires a materialize receiver");

        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("test").expect("register agent");
        doc.edit(
            agent,
            "test",
            "insert",
            kutl_core::Boundary::Explicit,
            |ctx| ctx.insert(0, "## ? Should we ship?"),
        )
        .expect("edit");
        observer
            .after_text_merge(
                MergedEvent {
                    space_id: "11111111-1111-1111-1111-111111111111".to_owned(),
                    document_id: "doc-1".to_owned(),
                    author_did: "did:key:zHuman".to_owned(),
                    via_pat_id: None,
                    op_count: 1,
                    intent: "edit".to_owned(),
                    content_mode: EditContentMode::Text,
                    timestamp: 1_700_000_000_000,
                },
                &doc,
            )
            .await;

        let batch = rx
            .try_recv()
            .expect("the materializer must send its batch on the unbounded channel");
        assert_eq!(batch.records.len(), 1, "one CREATED decision record");
    }

    /// A PRESENT-BUT-MALFORMED `identity.json` in the data dir
    /// must ABORT `build_app`, NOT silently degrade to a tier-3 relay with an
    /// empty `relay_did`. A torn first-start identity write would otherwise
    /// permanently and silently flip the relay's DID on every subsequent boot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_build_app_aborts_on_malformed_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Simulate a torn identity write: a truncated JSON blob.
        std::fs::write(
            dir.path().join(identity::IDENTITY_FILE_NAME),
            b"{\"did\":\"did:key:zBroken\",\"private_key\":\"AAAA",
        )
        .expect("write malformed identity");

        let config = RelayConfig {
            port: 0,
            relay_name: "test-relay".into(),
            data_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let result = build_app(config).await;
        assert!(
            result.is_err(),
            "a malformed identity.json must abort build_app, not degrade to tier-3"
        );
        // The malformed file must be left untouched — never silently regenerated.
        let after = std::fs::read(dir.path().join(identity::IDENTITY_FILE_NAME))
            .expect("identity file still present");
        assert_eq!(
            after, b"{\"did\":\"did:key:zBroken\",\"private_key\":\"AAAA",
            "the corrupt identity must NOT be overwritten with a fresh one"
        );
    }

    /// A DATA-DIR relay with NO existing `identity.json` (normal first run)
    /// starts cleanly, generating a valid identity — the absent-file path must
    /// not be conflated with the malformed-file abort.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_build_app_absent_identity_generates_and_starts() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(!dir.path().join(identity::IDENTITY_FILE_NAME).exists());
        // Authentication is mandatory; the boot assert needs an
        // authorizer, so seed an (empty) authorized_keys file. This test is
        // about identity generation, not auth.
        let keys_path = dir.path().join("authorized_keys");
        std::fs::write(&keys_path, "# test keys\n").expect("seed keys file");
        let config = RelayConfig {
            port: 0,
            relay_name: "test-relay".into(),
            data_dir: Some(dir.path().to_path_buf()),
            authorized_keys_file: Some(keys_path),
            ..Default::default()
        };
        let _app = build_app(config)
            .await
            .expect("absent identity must generate and start, not abort");
        // A valid identity file now exists and re-loads.
        assert!(dir.path().join(identity::IDENTITY_FILE_NAME).exists());
        identity::RelayIdentity::load_or_generate(dir.path())
            .expect("the generated identity must be valid and re-loadable");
    }

    /// Insert a signal row directly into a bare `signals` table, bypassing
    /// `record_signal` so the row has no record behind it — the phantom shape
    /// the startup clear removes. `space` is bound verbatim so a NON-UUID
    /// `space_id` can be seeded to exercise the skip-and-warn path.
    #[cfg(test)]
    async fn insert_recordless_row(pool: &sqlx::SqlitePool, space: &str, id: &str, ts: i64) {
        // The row carries the feed's page key like any real row: a fixture
        // without one is a shape the projection cannot hold.
        sqlx::query(&format!(
            "INSERT INTO signals
                (seq, id, space_id, document_id, author_did, signal_type,
                 timestamp, flag_kind, audience, message)
             VALUES ({expr}, ?, ?, 'doc-1', 'did:key:zA', 'flag', ?, 1, 0, 'body')",
            expr = change_sqlite::NEXT_SEQ_EXPR,
        ))
        .bind(space)
        .bind(id)
        .bind(space)
        .bind(ts)
        .execute(pool)
        .await
        .expect("insert recordless flag row");
    }

    /// One bad space must NOT brick startup. A row with a NON-UUID `space_id`
    /// fails `Uuid::parse_str` — if that error `?`-propagated out, `build_app`
    /// would refuse to start on EVERY boot. The driver SKIPS-AND-WARNS: it
    /// logs the bad space and continues, so the valid space is still cleared
    /// and the driver returns Ok.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_startup_skips_bad_space_and_clears_good() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory pool");
        let change = change_sqlite::SqliteChangeBackend::new(pool.clone())
            .await
            .expect("backend");

        let good = "12341234-1234-1234-1234-123412341234";
        // A non-UUID space_id exercises the skip-and-warn arm.
        let bad = "my-notes";
        insert_recordless_row(&pool, good, "sig-good", 1_000).await;
        insert_recordless_row(&pool, bad, "sig-bad", 2_000).await;

        let dir = tempfile::tempdir().expect("tempdir");
        // Must NOT propagate the bad space's parse error — relay always starts.
        reconcile_projections_at_startup(&change, dir.path())
            .await
            .expect("a bad-space error must be skipped, not brick startup");

        // The VALID space's phantom was cleared; the bad one was skipped and
        // its row remains untouched.
        let good_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signals WHERE space_id = ?")
            .bind(good)
            .fetch_one(&pool)
            .await
            .expect("count good rows");
        assert_eq!(
            good_rows, 0,
            "the valid space's phantom row must be cleared"
        );
        let bad_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signals WHERE space_id = ?")
            .bind(bad)
            .fetch_one(&pool)
            .await
            .expect("count bad rows");
        assert_eq!(bad_rows, 1, "the skipped space's row is left alone");
    }

    /// The startup pass clears a phantom row while keeping a recorded one,
    /// over one fold: the clear runs on the same loaded records the rebuild
    /// then consumes, and only the row with no record behind it is deleted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_startup_clears_phantoms_and_keeps_recorded_rows() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory pool");
        let change = change_sqlite::SqliteChangeBackend::new(pool.clone())
            .await
            .expect("backend");

        let space = "12341234-1234-1234-1234-123412341234";
        let space_uuid = uuid::Uuid::parse_str(space).expect("space uuid");

        // One signal recorded AND projected — the healthy pair the pass must
        // not touch.
        let kept = kutl_proto::sync::Signal {
            id: "sig-kept".into(),
            space_id: space.into(),
            record_id: "rKept".into(),
            author_did: "did:key:zA".into(),
            actor_did: "did:key:zA".into(),
            timestamp: 1_000,
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: 1_000,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            payload: Some(kutl_proto::sync::signal::Payload::Flag(
                kutl_proto::sync::FlagPayload {
                    kind: 1,
                    message: "kept".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store =
            signal_store::SignalStore::new(signal_store::SignalStore::root_for(dir.path()));
        store.append(space_uuid, &kept).expect("append kept record");
        change_backend::ProjectionWriter::project_record(&change, space, &kept, None)
            .await
            .expect("project kept record");

        // A phantom beside it — a row with no record anywhere.
        insert_recordless_row(&pool, space, "sig-phantom", 2_000).await;
        let rows_before: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM signals WHERE space_id = ?")
                .bind(space)
                .fetch_one(&pool)
                .await
                .expect("count rows");
        assert_eq!(rows_before, 2, "one recorded row and one phantom before");

        reconcile_projections_at_startup(&change, dir.path())
            .await
            .expect("the pass must succeed");

        let remaining: Vec<String> =
            sqlx::query_scalar("SELECT id FROM signals WHERE space_id = ? ORDER BY id")
                .bind(space)
                .fetch_all(&pool)
                .await
                .expect("list remaining rows");
        assert_eq!(
            remaining,
            vec!["sig-kept".to_string()],
            "the phantom is cleared and the recorded row survives the rebuild"
        );
    }

    /// EVERY relay materializes, including one carrying a host publisher.
    /// The publisher rides alongside the materializer, never instead of it: a
    /// host observer that replaced the materializer would parse its own
    /// markers and mint no records at all, and a decision typed in the browser
    /// and one typed on disk could not produce the same record.
    #[test]
    fn test_a_host_publisher_still_gets_a_materializer() {
        struct SilentPublisher;
        #[async_trait::async_trait]
        impl markers::materialize::AfterMergePublisher for SilentPublisher {
            async fn publish_markers(
                &self,
                _event: &observer::MergedEvent,
                _content: &str,
                _outcome: markers::materialize::MarkerOutcome<'_>,
            ) {
            }
        }

        let (_observer, rx) = resolve_after_merge(
            Some(Arc::new(SilentPublisher)),
            None,
            relay::new_signal_clock(),
            None,
        );
        assert!(
            rx.is_some(),
            "a host publisher must not displace the materializer's channel"
        );
    }
}
