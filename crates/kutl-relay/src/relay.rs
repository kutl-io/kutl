//! Relay actor — owns all mutable state and processes commands from connections.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use kutl_core::Boundary;
use kutl_core::lattice::{ContentKind, ContentState, EvictionState, SubscriberMap};
use kutl_proto::sync::{self, ErrorCode};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::acl::{AuthError as AclError, AuthorizedSpace};
use crate::auth::{AuthError, AuthStore, DeviceTokenResponse};
use crate::authorized_keys::AuthorizedKeys;
use crate::blob_backend::BlobBackend;
use crate::config::RelayConfig;
use crate::content_backend::ContentBackend;
use crate::load_class::{LoadClass, LoadOutcome, classify_load};
use crate::observer::{
    AfterMergeObserver, BeforeMergeObserver, DocumentRegisteredEvent, DocumentRenamedEvent,
    DocumentUnregisteredEvent, EditContentMode, MergedEvent, NoopAfterMergeObserver,
    NoopBeforeMergeObserver, ReactionEvent, RelayObserver, SignalClosedEvent, SignalCreatedEvent,
    SignalReopenedEvent,
};
use crate::protocol::{
    ABSOLUTE_BLOB_MAX, CapKind, blob_ops_envelope, encode_envelope, is_blob_mode,
    presence_update_envelope, signal_flag_envelope, stale_subscriber_envelope,
    subscribe_status_envelope, sync_ops_envelope, wrap_error, wrap_handshake_ack,
    wrap_quota_exceeded,
};
use crate::quota_backend::QuotaBackend;
use crate::registry;
use crate::registry_store::RegistryBackend;
use crate::spaces;

/// Sentinel `known_version` indicating the subscriber has the current blob.
const BLOB_KNOWN: &[usize] = &[1];

/// MCP sessions are reaped after 20 minutes of inactivity.
const MCP_SESSION_IDLE_TTL: Duration = Duration::from_mins(20);

/// How often the relay checks for expired MCP sessions.
#[allow(clippy::cast_sign_loss)] // SECONDS_PER_MINUTE is positive
const MCP_SESSION_REAP_INTERVAL: Duration =
    Duration::from_secs(5 * kutl_core::SECONDS_PER_MINUTE as u64);

/// How often the relay prunes stale change data (signals, cursors).
#[allow(clippy::cast_sign_loss)] // SECONDS_PER_HOUR is positive
const CHANGE_REAP_INTERVAL: Duration = Duration::from_secs(kutl_core::SECONDS_PER_HOUR as u64);

/// Maximum `apply_storage_delta` attempts (1 initial + 2 retries).
const QUOTA_DELTA_MAX_ATTEMPTS: usize = 3;

/// Back-off (ms) slept between consecutive failed attempts. Length is
/// `QUOTA_DELTA_MAX_ATTEMPTS - 1`: no sleep after the final attempt.
/// Total sleep budget: 250 ms; typical call finishes in one DB round-trip.
const QUOTA_DELTA_BACKOFF_MS: [u64; QUOTA_DELTA_MAX_ATTEMPTS - 1] = [50, 200];

/// Apply a storage delta with bounded retries on transient failure.
///
/// Success returns `Ok(())` after the first successful apply. Failure on the
/// last attempt returns the underlying error — callers should log and
/// continue, because the blob is already in memory and the monthly
/// reconcile job will correct counter drift.
///
/// `pub(crate)` so the background flush task (`flush.rs`) can reuse the same
/// retry ladder without duplicating logic.
pub(crate) async fn apply_delta_with_retry(
    qb: &dyn QuotaBackend,
    space_id: &str,
    doc_id: &str,
    new_size: i64,
) -> anyhow::Result<()> {
    // Iterate the backoff table: each entry represents a sleep *before* the
    // next retry. If the attempt before the first retry succeeds we return
    // early; otherwise we sleep the prescribed interval and try again.
    for (attempt, &backoff_ms) in QUOTA_DELTA_BACKOFF_MS.iter().enumerate() {
        match qb.apply_storage_delta(space_id, doc_id, new_size).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                warn!(attempt, error = %e, "quota delta failed, retrying");
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }
    // Final attempt after all back-off sleeps are exhausted.
    qb.apply_storage_delta(space_id, doc_id, new_size)
        .await
        .map(|_| ())
}

/// Unique identifier for a WebSocket connection.
pub type ConnId = u64;

/// A single document's encoded content, sent from the relay actor to the flush task.
pub struct FlushEntry {
    /// Space ID.
    pub space_id: String,
    /// Document ID.
    pub doc_id: String,
    /// Content to persist.
    pub content: FlushContent,
    /// Authoritative durable size for this flush (RFD 0063). Measured from
    /// the encoded bytes (text) or the blob payload (blob) at the instant
    /// the entry was constructed, so the post-save `apply_storage_delta`
    /// records exactly what was persisted.
    pub size_bytes: i64,
    /// Snapshot of `ContentState.edit_counter` at the time the entry was
    /// handed to the flush channel. The flush task reports this back on
    /// success so the actor can advance `flushed_counter` to exactly
    /// this value — not beyond, in case new edits arrived since.
    pub flushed_up_to: u64,
}

/// The type of content to flush.
pub enum FlushContent {
    /// CRDT text document — `encode_full()` bytes.
    Text(Vec<u8>),
    /// Binary blob with metadata.
    Blob(crate::blob_backend::BlobRecord),
}

/// Commands sent from connection tasks to the relay actor.
pub enum RelayCommand {
    /// A new connection has been established.
    Connect {
        conn_id: ConnId,
        tx: mpsc::Sender<Vec<u8>>,
        ctrl_tx: mpsc::Sender<Vec<u8>>,
    },
    /// A connection has been closed.
    Disconnect { conn_id: ConnId },
    /// Client sent a Handshake message.
    Handshake {
        conn_id: ConnId,
        msg: sync::Handshake,
    },
    /// Client wants to subscribe to a document.
    Subscribe {
        conn_id: ConnId,
        msg: sync::Subscribe,
    },
    /// Client wants to unsubscribe from a document.
    Unsubscribe {
        conn_id: ConnId,
        msg: sync::Unsubscribe,
    },
    /// Client sent sync operations.
    InboundSyncOps {
        conn_id: ConnId,
        msg: Box<sync::SyncOps>,
    },
    /// Client sent a presence update (cursor/selection).
    PresenceUpdate {
        conn_id: ConnId,
        msg: sync::PresenceUpdate,
    },
    /// Request a DID challenge (from HTTP auth handler).
    AuthChallenge {
        did: String,
        reply: oneshot::Sender<Result<ChallengeResponse, AuthError>>,
    },
    /// Verify a DID challenge-response (from HTTP auth handler).
    AuthVerify {
        did: String,
        nonce: String,
        signature: String,
        reply: oneshot::Sender<Result<VerifyResponse, AuthError>>,
    },
    // ---- MCP commands ----
    /// Validate a bearer token. Returns `(identity, pat_hash)`.
    McpValidateToken {
        token: String,
        reply: oneshot::Sender<Result<(String, Option<String>), AuthError>>,
    },
    /// Validate that a session exists (and optionally belongs to the given DID).
    McpValidateSession {
        session_id: McpSessionId,
        expected_did: Option<String>,
        reply: oneshot::Sender<Result<(), McpError>>,
    },
    /// Create a new MCP session for an authenticated DID.
    McpCreateSession {
        did: String,
        pat_hash: Option<String>,
        reply: oneshot::Sender<McpSessionId>,
    },
    /// Destroy an MCP session.
    McpDestroySession { session_id: McpSessionId },
    /// Read a document's content.
    McpReadDocument {
        session_id: McpSessionId,
        space_id: String,
        document_id: String,
        reply: oneshot::Sender<Result<McpDocumentContent, McpError>>,
    },
    /// Read a document's text content via HTTP. Authenticated by DID
    /// (resolved from bearer token by the HTTP handler).
    ReadDocumentText {
        did: String,
        space_id: String,
        document_id: String,
        reply: oneshot::Sender<Result<String, McpError>>,
    },
    /// List documents in a space.
    McpListDocuments {
        session_id: McpSessionId,
        space_id: String,
        reply: oneshot::Sender<Result<Vec<McpDocumentSummary>, McpError>>,
    },
    /// Read the change log for a document.
    McpReadLog {
        session_id: McpSessionId,
        space_id: String,
        document_id: String,
        limit: Option<usize>,
        reply: oneshot::Sender<Result<Vec<McpLogEntry>, McpError>>,
    },
    /// List participants connected to a space.
    McpListParticipants {
        session_id: McpSessionId,
        space_id: String,
        reply: oneshot::Sender<Result<Vec<McpParticipant>, McpError>>,
    },
    /// Get space status.
    McpStatus {
        session_id: McpSessionId,
        space_id: String,
        reply: oneshot::Sender<Result<McpSpaceStatus, McpError>>,
    },
    /// Edit a document via full-content replacement.
    McpEditDocument {
        session_id: McpSessionId,
        space_id: String,
        document_id: String,
        new_content: String,
        intent: String,
        /// Agent-provided snippet for the activity feed. Empty means
        /// the relay computes one from the diff.
        snippet: String,
        reply: oneshot::Sender<Result<McpEditResult, McpError>>,
    },
    /// Register an SSE notification channel for an MCP session.
    McpRegisterNotifications {
        session_id: McpSessionId,
        tx: mpsc::Sender<String>,
    },
    /// Create a flag signal via MCP.
    McpCreateFlag {
        session_id: McpSessionId,
        space_id: String,
        document_id: String,
        kind: i32,
        message: String,
        audience: i32,
        target_did: String,
        reply: oneshot::Sender<Result<String, McpError>>,
    },
    /// Query changes since the caller's last check.
    McpGetChanges {
        session_id: McpSessionId,
        space_id: String,
        checkpoint: Option<String>,
        reply: oneshot::Sender<Result<crate::change_backend::ChangesResponse, McpError>>,
    },
    /// Create a reply to a signal via MCP.
    McpCreateReply {
        session_id: McpSessionId,
        space_id: String,
        parent_signal_id: String,
        parent_reply_id: Option<String>,
        body: String,
        reply: oneshot::Sender<Result<String, McpError>>,
    },
    /// Add or remove a reaction on a signal via MCP.
    McpReactToSignal {
        session_id: McpSessionId,
        space_id: String,
        signal_id: String,
        emoji: String,
        remove: bool,
        reply: oneshot::Sender<Result<(), McpError>>,
    },
    /// Close a flag signal via MCP.
    McpCloseFlag {
        session_id: McpSessionId,
        space_id: String,
        signal_id: String,
        reason: Option<String>,
        close_note: Option<String>,
        reply: oneshot::Sender<Result<(), McpError>>,
    },
    /// Reopen a previously closed flag signal via MCP.
    McpReopenFlag {
        session_id: McpSessionId,
        space_id: String,
        signal_id: String,
        reply: oneshot::Sender<Result<(), McpError>>,
    },
    /// Fetch full detail for a single signal via MCP.
    McpGetSignalDetail {
        session_id: McpSessionId,
        space_id: String,
        signal_id: String,
        reply: oneshot::Sender<Result<crate::change_backend::SignalDetail, McpError>>,
    },
    /// Create a chat signal via MCP.
    McpCreateChat {
        session_id: McpSessionId,
        space_id: String,
        topic: Option<String>,
        target_did: Option<String>,
        reply: oneshot::Sender<Result<String, McpError>>,
    },
    /// Client sent a signal (flag payload only accepted over WebSocket).
    Signal { conn_id: ConnId, msg: sync::Signal },
    // ---- Document lifecycle commands ----
    /// Client registering a new document in the space registry.
    RegisterDocument {
        conn_id: ConnId,
        msg: sync::RegisterDocument,
    },
    /// Client renaming a document in the space registry.
    RenameDocument {
        conn_id: ConnId,
        msg: sync::RenameDocument,
    },
    /// Client unregistering (soft-deleting) a document from the space registry.
    UnregisterDocument {
        conn_id: ConnId,
        msg: sync::UnregisterDocument,
    },
    // ---- Space ops commands (kutlhub) ----
    /// Accept an invitation code (kutlhub).
    JoinSpaceOp {
        conn_id: ConnId,
        msg: sync::JoinSpace,
    },
    /// Resolve a space by owner/slug (kutlhub).
    ResolveSpaceOp {
        conn_id: ConnId,
        msg: sync::ResolveSpace,
    },
    /// List spaces the caller is a member of (kutlhub).
    ListMySpacesOp {
        conn_id: ConnId,
        msg: sync::ListMySpaces,
    },
    /// List all active documents in a space's registry.
    ListSpaceDocuments {
        conn_id: ConnId,
        msg: sync::ListSpaceDocuments,
    },
    /// Flush task requests encoded bytes for all dirty documents.
    FlushDirty { reply: mpsc::Sender<FlushEntry> },
    /// Flush task confirms a document was persisted successfully.
    /// The actor advances `flushed_counter` to the snapshot value.
    FlushCompleted {
        space_id: String,
        doc_id: String,
        flushed_up_to: u64,
    },
    /// Derive effects for a document (eviction check). Idempotent.
    DeriveEffects {
        space_id: String,
        document_id: String,
    },
    /// Debounce timer fired — flush the pending edit with computed snippet.
    FlushPendingEdit {
        space_id: String,
        document_id: String,
    },
    // ---- Space registration commands (OSS HTTP) ----
    /// Register a new space (from HTTP handler).
    RegisterSpace {
        name: String,
        reply: oneshot::Sender<
            Result<crate::space_backend::RegisteredSpace, crate::space_backend::SpaceBackendError>,
        >,
    },
    /// Resolve a space name to its UUID (from HTTP handler).
    ResolveSpace {
        name: String,
        reply: oneshot::Sender<
            Result<
                Option<crate::space_backend::RegisteredSpace>,
                crate::space_backend::SpaceBackendError,
            >,
        >,
    },
    // ---- Device auth flow commands ----
    /// Create a new device authorization request.
    CreateDeviceRequest {
        reply: oneshot::Sender<Result<DeviceRequestResponse, AuthError>>,
    },
    /// Poll a pending device authorization request.
    PollDevice {
        device_code: String,
        reply: oneshot::Sender<Result<DeviceTokenResponse, AuthError>>,
    },
    /// Authorize a pending device request (called by UX server).
    AuthorizeDevice {
        user_code: String,
        token: String,
        account_id: String,
        display_name: String,
        reply: oneshot::Sender<Result<(), AuthError>>,
    },
    /// Periodically remove idle MCP sessions.
    ReapMcpSessions,
    /// Periodically prune stale change data (signals, cursors).
    ReapChanges,
}

/// Response to an [`RelayCommand::AuthChallenge`].
pub struct ChallengeResponse {
    /// Base64url-encoded nonce that the client must sign.
    pub nonce: String,
    /// Expiry timestamp in Unix milliseconds.
    pub expires_at: i64,
}

/// Response to an [`RelayCommand::AuthVerify`].
pub struct VerifyResponse {
    /// Bearer token to include in the WebSocket handshake.
    pub token: String,
    /// Expiry timestamp in Unix milliseconds.
    pub expires_at: i64,
}

/// Response to an [`RelayCommand::CreateDeviceRequest`].
pub struct DeviceRequestResponse {
    /// Opaque device code for CLI polling.
    pub device_code: String,
    /// Human-readable code displayed to the user (e.g. "ABCD-1234").
    pub user_code: String,
    /// Expiry timestamp in Unix milliseconds.
    pub expires_at: i64,
}

// ---------------------------------------------------------------------------
// MCP types
// ---------------------------------------------------------------------------

/// Unique identifier for an MCP session.
pub type McpSessionId = String;

/// An MCP session tracked by the relay actor.
struct McpSession {
    /// The DID of the authenticated agent.
    did: String,
    /// SHA-256 hash of the PAT that created this session, if PAT-authed.
    /// Used by `authorize_mcp_caller` to enforce per-token space scoping.
    pat_hash: Option<String>,
    /// SSE notification channel (None until GET /mcp registers it).
    notify_tx: Option<mpsc::Sender<String>>,
    /// When the session last handled a request (reset on each validate).
    last_active: tokio::time::Instant,
}

/// Errors specific to MCP tool dispatch.
#[derive(Debug, thiserror::Error)]
pub enum McpError {
    /// The MCP session ID is not recognized.
    #[error("session not found")]
    SessionNotFound,
    /// The requested document does not exist.
    #[error("document not found: {space_id}/{document_id}")]
    DocumentNotFound {
        space_id: String,
        document_id: String,
    },
    /// The document contains binary blob data, not text.
    #[error("document is a binary blob, not text")]
    NotTextDocument,
    /// The caller is not a member of the requested space.
    #[error("not a member of space {space_id}")]
    NotAuthorized { space_id: String },
    /// The signal does not exist or belongs to a different space.
    #[error("signal not found: {signal_id}")]
    SignalNotFound { signal_id: String },
    /// An edit operation failed.
    #[error("edit failed: {0}")]
    EditFailed(String),
    /// An unexpected internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Content and version of a document read via MCP.
#[derive(Debug, Serialize)]
pub struct McpDocumentContent {
    /// The full text content.
    pub content: String,
    /// Current version identifier (stringified for JSON).
    pub version: String,
}

/// Summary of a document for listing.
#[derive(Debug, Serialize)]
pub struct McpDocumentSummary {
    /// Document identifier (UUID).
    pub document_id: String,
    /// Human-readable path (may equal `document_id` if none registered).
    pub path: String,
    /// Content type: "text", "blob", or "empty".
    pub content_type: String,
    /// Number of WS subscribers.
    pub subscriber_count: usize,
}

/// A log entry returned by `read_log`.
#[derive(Debug, Serialize)]
pub struct McpLogEntry {
    /// DID of the change author.
    pub author_did: String,
    /// Human-readable intent.
    pub intent: String,
    /// Timestamp in Unix milliseconds.
    pub timestamp: i64,
    /// Change identifier.
    pub id: String,
    /// Boundary type.
    pub boundary: String,
    /// Whether this change was a full rewrite (bulk path).
    pub full_rewrite: bool,
}

/// A participant connected to a space.
#[derive(Debug, Serialize)]
pub struct McpParticipant {
    /// DID of the participant.
    pub did: String,
    /// Connection type: "websocket" or "mcp".
    pub connection_type: String,
}

/// Space status information.
#[derive(Debug, Serialize)]
pub struct McpSpaceStatus {
    /// Number of documents in the space.
    pub document_count: usize,
    /// Total number of WS subscribers across all docs.
    pub subscriber_count: usize,
    /// Number of active MCP sessions.
    pub mcp_session_count: usize,
}

/// Result of an edit operation via MCP.
#[derive(Debug, Serialize)]
pub struct McpEditResult {
    /// The actual document UUID (may differ from the requested ID if
    /// a non-UUID string was provided and a UUID was auto-generated).
    pub document_id: String,
    /// Version string after the edit.
    pub new_version: String,
    /// Number of CRDT ops applied.
    pub ops_applied: usize,
    /// Whether the bulk (delete-all + insert-all) path was taken.
    pub was_bulk: bool,
}

/// Data and control channels for a single connection.
struct ConnChannels {
    /// Channel for data messages (sync ops relay and catch-up).
    data: mpsc::Sender<Vec<u8>>,
    /// Channel for control/admin messages (handshake acks, errors, rate-limit
    /// rejections, stale notices). Separate from data so admin messages are
    /// delivered even when the data channel is full.
    ctrl: mpsc::Sender<Vec<u8>>,
}

/// The relay actor. Runs in a single task, processes commands sequentially.
///
/// Can be driven in two ways:
/// - **Production:** `Relay::new()` + `relay.run().await` — processes commands from an mpsc channel.
/// - **Simulation:** `Relay::new_standalone()` + `relay.process_command()` — driven synchronously.
pub struct Relay {
    config: RelayConfig,
    connections: HashMap<ConnId, ConnChannels>,
    documents: HashMap<DocKey, DocSlot>,
    rx: Option<mpsc::Receiver<RelayCommand>>,
    /// Auth store for DID challenge-response. Only populated when `require_auth` is true.
    auth: Option<AuthStore>,
    /// Maps `ConnId` to authenticated DID. Only tracked when `require_auth` is true.
    /// Maps connection ID → (identity, `pat_hash`). `pat_hash` is `Some`
    /// when the connection was authenticated via PAT — `authorize_space`
    /// uses it to enforce per-token space scoping.
    authenticated: HashMap<ConnId, (String, Option<String>)>,
    /// Active MCP sessions, keyed by session ID.
    mcp_sessions: HashMap<McpSessionId, McpSession>,
    /// Session persistence backend. `None` for in-memory-only (OSS relay).
    session_backend: Option<Arc<dyn crate::session_backend::SessionBackend>>,
    /// PAT validation backend. `None` when PATs are not supported (OSS relay).
    pat_backend: Option<Arc<dyn crate::pat_backend::PatBackend>>,
    /// Space membership backend. `None` when database-backed ACL is not available (OSS relay).
    membership_backend: Option<Arc<dyn crate::membership_backend::MembershipBackend>>,
    /// Per-space document registries, keyed by `space_id`.
    registries: HashMap<String, registry::DocumentRegistry>,
    /// Persistence backend for registries. `None` disables persistence.
    registry_backend: Option<Arc<dyn RegistryBackend>>,
    /// Space registration backend. `None` disables space registration.
    space_backend: Option<Box<dyn crate::space_backend::SpaceBackend>>,
    /// Persistent content storage. `None` for ephemeral (open-source) relay.
    content_backend: Option<Arc<dyn ContentBackend>>,
    /// Persistent blob storage. `None` for ephemeral (open-source) relay.
    blob_backend: Option<Arc<dyn BlobBackend>>,
    /// Clone of the command channel sender, for self-messaging (eviction timers).
    /// `None` in standalone (simulation/test) mode.
    self_tx: Option<mpsc::Sender<RelayCommand>>,
    /// File-based DID authorization list (OSS relay mode).
    authorized_keys: Option<AuthorizedKeys>,
    /// Connections watching each space for lifecycle events (register/rename/unregister).
    /// Populated when a connection calls `ListSpaceDocuments`.
    space_watchers: HashMap<String, HashSet<ConnId>>,
    /// Observer for relay events (edits, lifecycle, signals).
    observer: Arc<dyn RelayObserver>,
    /// Observer called before each text merge.
    before_merge: Arc<dyn BeforeMergeObserver>,
    /// Observer called after each text merge.
    after_merge: Arc<dyn AfterMergeObserver>,
    /// Change backend for persisting signals and serving agent polls.
    /// `None` for ephemeral (standalone/test) mode.
    change_backend: Option<Arc<dyn crate::change_backend::ChangeBackend>>,
    /// Per-tenant storage quota backend. `None` in OSS relay — kutlhub-relay
    /// injects `PgQuotaBackend` to enforce tier-based limits. Consumed by
    /// `handle_blob_sync_ops` / `handle_text_sync_ops` as the inbound
    /// pre-check, and by the flush task for post-write reconciliation.
    /// See RFD 0063.
    quota_backend: Option<Arc<dyn crate::quota_backend::QuotaBackend>>,
}

/// Key identifying a document within the relay.
#[derive(Clone, PartialEq, Eq, Hash)]
struct DocKey {
    space_id: String,
    document_id: String,
}

/// Accumulated edit event metadata held during the snippet debounce window.
///
/// Created on the first snippet-eligible edit, updated on subsequent edits
/// by the same author, and consumed when the debounce timer fires or a
/// different author edits the same document.
struct PendingEdit {
    /// DID of the author whose edits are being accumulated.
    author_did: String,
    /// Free-form intent string (latest edit wins).
    intent: String,
    /// Total number of ops accumulated in this burst.
    op_count: usize,
    /// Timestamp of the first edit in the burst (millis since epoch).
    timestamp: i64,
}

/// Subscribe-side classification view that's cheap to carry after the
/// `LoadResult`'s `Found` has been consumed (slot inserted into the
/// documents map). Used to drive `SubscribeStatus` envelope construction
/// and the "short-circuit on Inconsistent/Unavailable" control flow.
#[derive(Debug)]
enum SubscribeStatusInfo {
    Found,
    Empty,
    Inconsistent { expected_version: u64 },
    Unavailable { reason: String },
}

/// Result of a subscribe cold-load after RFD 0066 classification.
///
/// `Found` carries the populated `DocSlot`. The other variants carry
/// enough metadata for the subscribe path to report the classification
/// to the client (Phase 3 wire plumbing, Phase 4 editor behavior) and
/// for the relay's structured log line to include the expected version
/// or failure reason for observability.
enum LoadResult {
    /// Backend produced bytes (and they decoded). Caller inserts this
    /// slot into `documents` and proceeds with a normal subscribe.
    Found(DocSlot),
    /// Genuinely first-time document: backends returned no data and the
    /// durability witness says nothing was ever persisted. Safe to open
    /// as a writable empty editor.
    Empty,
    /// The durability witness claims prior content at
    /// `expected_version`, but no backend returned bytes. Content was
    /// lost between a successful flush and the current load — the
    /// "persisted-then-lost" failure mode the hard edit guard must
    /// refuse to paper over (RFD 0066).
    Inconsistent {
        /// The `persisted_version` the witness still holds.
        expected_version: u64,
    },
    /// At least one backend (or the witness read itself) errored.
    /// State is undetermined; refuse to claim either Empty or
    /// Inconsistent until storage health is restored.
    Unavailable {
        /// Human-readable reason, suitable for a client banner.
        reason: String,
    },
}

/// Per-document slot: subscribers + content (text CRDT or blob LWW).
///
/// Dirty tracking uses [`ContentState`] monotonic counters. Use
/// [`DocSlot::mutate_text`] or [`DocSlot::set_blob`] to change content —
/// both automatically record edits, making it impossible to forget.
struct DocSlot {
    subscribers: SubscriberMap,
    /// Document content. Read access is direct (allows split borrows with
    /// `subscribers`). Mutations must go through [`mutate_text`] or
    /// [`set_blob`] which handle dirty tracking.
    content: DocContent,
    /// Monotonic edit/flush counters tracking dirty status.
    /// Private — only mutated via [`mutate_text`], [`set_blob`], and
    /// [`clear_dirty`]. Read via [`is_dirty`].
    content_state: ContentState,
    /// Declarative eviction state (replaces timer-based eviction).
    eviction: EvictionState,
    /// Pending edit event held during snippet debounce. `None` when no
    /// snippet-eligible edit is waiting.
    pending_edit: Option<PendingEdit>,
    /// Debounce timer handle — aborted and replaced on each new edit.
    snippet_timer: Option<tokio::task::JoinHandle<()>>,
    /// Timestamp of most recent merge that hasn't yet been flushed to
    /// `edited_at` in the registry backend. Set on every merge, cleared
    /// by `flush_pending_edit_for`. Separate from `pending_edit` so
    /// snippet-ineligible edits still surface the write.
    edited_at_pending: Option<i64>,
}

impl DocSlot {
    /// Create an empty, clean slot with no subscribers.
    fn empty() -> Self {
        Self {
            subscribers: SubscriberMap::new(),
            content: DocContent::Empty,
            content_state: ContentState::new(),
            eviction: EvictionState::new(),
            pending_edit: None,
            snippet_timer: None,
            edited_at_pending: None,
        }
    }

    /// Create a slot pre-loaded with content (from backend). Not dirty.
    fn with_content(content: DocContent) -> Self {
        Self {
            subscribers: SubscriberMap::new(),
            content,
            content_state: ContentState::new(),
            eviction: EvictionState::new(),
            pending_edit: None,
            snippet_timer: None,
            edited_at_pending: None,
        }
    }

    /// Ensure content is Text, initializing from Empty or replacing Blob.
    /// Does NOT mark dirty — this is initialization, not mutation.
    /// Returns `true` if content was replaced from Blob (caller should
    /// reset subscriber versions).
    #[allow(clippy::cast_sign_loss)]
    fn ensure_text(&mut self) -> bool {
        match &self.content {
            DocContent::Empty => {
                self.content = DocContent::Text(Box::new(kutl_core::Document::new()));
                self.content_state
                    .set_kind(ContentKind::Text, kutl_core::now_ms_u64());
                false
            }
            DocContent::Text(_) => false,
            DocContent::Blob(_) => {
                self.content = DocContent::Text(Box::new(kutl_core::Document::new()));
                self.content_state
                    .set_kind(ContentKind::Text, kutl_core::now_ms_u64());
                true
            }
        }
    }

    /// Apply a fallible mutation to the text document.
    ///
    /// Records an edit only on `Ok`. The closure receives `&mut Document` by
    /// reference — no copies. Panics if content is not Text (caller must
    /// call [`ensure_text`] first).
    fn mutate_text<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut kutl_core::Document) -> Result<T, E>,
    {
        let DocContent::Text(doc) = &mut self.content else {
            panic!("mutate_text called on non-text DocSlot");
        };
        let result = f(doc)?;
        self.content_state.record_edit();
        Ok(result)
    }

    /// Replace content with a blob. Always records an edit.
    #[allow(clippy::cast_sign_loss)]
    fn set_blob(&mut self, blob: BlobData) {
        self.content = DocContent::Blob(blob);
        self.content_state
            .set_kind(ContentKind::Blob, kutl_core::now_ms_u64());
        self.content_state.record_edit();
    }

    /// Whether content has been mutated since last flush.
    fn is_dirty(&self) -> bool {
        self.content_state.is_dirty()
    }
}

/// Content stored for a document — either CRDT text or a binary blob.
enum DocContent {
    /// No content yet — first subscriber hasn't sent ops.
    Empty,
    /// Text document backed by a DT CRDT.
    Text(Box<kutl_core::Document>),
    /// Binary blob (LWW by timestamp).
    Blob(BlobData),
}

/// Last-write-wins binary blob.
struct BlobData {
    content: Vec<u8>,
    hash: Vec<u8>,
    timestamp: i64,
}

impl Relay {
    /// Create a new relay actor with a command channel (production mode).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: RelayConfig,
        rx: mpsc::Receiver<RelayCommand>,
        session_backend: Option<Arc<dyn crate::session_backend::SessionBackend>>,
        pat_backend: Option<Arc<dyn crate::pat_backend::PatBackend>>,
        membership_backend: Option<Arc<dyn crate::membership_backend::MembershipBackend>>,
        registry_backend: Option<Arc<dyn RegistryBackend>>,
        space_backend: Option<Box<dyn crate::space_backend::SpaceBackend>>,
        content_backend: Option<Arc<dyn ContentBackend>>,
        blob_backend: Option<Arc<dyn BlobBackend>>,
        self_tx: mpsc::Sender<RelayCommand>,
        observer: Arc<dyn RelayObserver>,
        before_merge: Arc<dyn BeforeMergeObserver>,
        after_merge: Arc<dyn AfterMergeObserver>,
        change_backend: Option<Arc<dyn crate::change_backend::ChangeBackend>>,
        quota_backend: Option<Arc<dyn crate::quota_backend::QuotaBackend>>,
    ) -> Self {
        let auth = if config.require_auth {
            Some(AuthStore::new())
        } else {
            None
        };
        let authorized_keys = config
            .authorized_keys_file
            .as_ref()
            .map(|path| AuthorizedKeys::new(path.clone()));
        let registries = load_from_backend(registry_backend.as_deref());
        let mut relay = Self {
            config,
            connections: HashMap::new(),
            documents: HashMap::new(),
            rx: Some(rx),
            auth,
            authenticated: HashMap::new(),
            mcp_sessions: HashMap::new(),
            session_backend,
            pat_backend,
            membership_backend,
            registries,
            registry_backend,
            space_backend,
            content_backend,
            blob_backend,
            self_tx: Some(self_tx),
            authorized_keys,
            space_watchers: HashMap::new(),
            observer,
            before_merge,
            after_merge,
            change_backend,
            quota_backend,
        };
        relay.migrate_path_based_keys();
        relay
    }

    /// Create a standalone relay without a command channel (simulation mode).
    ///
    /// Drive it by calling [`Relay::process_command`] directly.
    pub fn new_standalone(config: RelayConfig) -> Self {
        Self::new_standalone_with_backend(config, None, None, None, None)
    }

    /// Create a standalone relay with optional backends and observer.
    pub fn new_standalone_with_backend(
        config: RelayConfig,
        registry_backend: Option<Arc<dyn RegistryBackend>>,
        content_backend: Option<Arc<dyn ContentBackend>>,
        blob_backend: Option<Arc<dyn BlobBackend>>,
        space_backend: Option<Box<dyn crate::space_backend::SpaceBackend>>,
    ) -> Self {
        Self::new_standalone_with_observer(
            config,
            registry_backend,
            content_backend,
            blob_backend,
            space_backend,
            Arc::new(crate::observer::NoopObserver),
            Arc::new(NoopBeforeMergeObserver),
            Arc::new(NoopAfterMergeObserver),
        )
    }

    /// Create a standalone relay with optional backends and a custom observer.
    #[allow(clippy::too_many_arguments)]
    pub fn new_standalone_with_observer(
        config: RelayConfig,
        registry_backend: Option<Arc<dyn RegistryBackend>>,
        content_backend: Option<Arc<dyn ContentBackend>>,
        blob_backend: Option<Arc<dyn BlobBackend>>,
        space_backend: Option<Box<dyn crate::space_backend::SpaceBackend>>,
        observer: Arc<dyn RelayObserver>,
        before_merge: Arc<dyn BeforeMergeObserver>,
        after_merge: Arc<dyn AfterMergeObserver>,
    ) -> Self {
        let auth = if config.require_auth {
            Some(AuthStore::new())
        } else {
            None
        };
        let authorized_keys = config
            .authorized_keys_file
            .as_ref()
            .map(|path| AuthorizedKeys::new(path.clone()));
        let registries = load_from_backend(registry_backend.as_deref());
        let mut relay = Self {
            config,
            connections: HashMap::new(),
            documents: HashMap::new(),
            rx: None,
            auth,
            authenticated: HashMap::new(),
            mcp_sessions: HashMap::new(),
            session_backend: None,
            pat_backend: None,
            membership_backend: None,
            registries,
            registry_backend,
            space_backend,
            content_backend,
            blob_backend,
            self_tx: None,
            authorized_keys,
            space_watchers: HashMap::new(),
            observer,
            before_merge,
            after_merge,
            change_backend: None,
            quota_backend: None,
        };
        relay.migrate_path_based_keys();
        relay
    }

    /// Run the relay command loop. Returns when all senders are dropped.
    ///
    /// # Panics
    ///
    /// Panics if the relay was created with [`Relay::new_standalone`] (no channel).
    pub async fn run(mut self) {
        let mut rx = self
            .rx
            .take()
            .expect("run() requires a channel — use new(), not new_standalone()");

        // Periodic reap of idle MCP sessions.
        if let Some(ref self_tx) = self.self_tx {
            let tx = self_tx.clone();
            tokio::spawn(async move {
                debug!("mcp session reap task started");
                loop {
                    tokio::time::sleep(MCP_SESSION_REAP_INTERVAL).await;
                    if tx.send(RelayCommand::ReapMcpSessions).await.is_err() {
                        break;
                    }
                }
                debug!("mcp session reap task exiting");
            });
        }

        // Periodic pruning of stale change data (signals, cursors).
        if self.change_backend.is_some()
            && let Some(ref self_tx) = self.self_tx
        {
            let tx = self_tx.clone();
            tokio::spawn(async move {
                debug!("change prune task started");
                loop {
                    tokio::time::sleep(CHANGE_REAP_INTERVAL).await;
                    if tx.send(RelayCommand::ReapChanges).await.is_err() {
                        break;
                    }
                }
                debug!("change prune task exiting");
            });
        }

        while let Some(cmd) = rx.recv().await {
            self.process_command(cmd).await;
        }
        info!("relay command channel closed, shutting down");
    }

    /// Process a single relay command.
    ///
    /// Async because some commands (auth verify, token validation) may query
    /// Postgres when a database is configured. When `db` is `None`, the async
    /// calls resolve immediately without yielding.
    #[allow(clippy::too_many_lines)]
    pub async fn process_command(&mut self, cmd: RelayCommand) {
        match cmd {
            RelayCommand::Connect {
                conn_id,
                tx,
                ctrl_tx,
            } => self.handle_connect(conn_id, tx, ctrl_tx),
            RelayCommand::Disconnect { conn_id } => self.handle_disconnect(conn_id),
            RelayCommand::Handshake { conn_id, msg } => self.handle_handshake(conn_id, &msg).await,
            RelayCommand::Subscribe { conn_id, msg } => self.handle_subscribe(conn_id, &msg).await,
            RelayCommand::Unsubscribe { conn_id, msg } => {
                self.handle_unsubscribe(conn_id, &msg);
            }
            RelayCommand::InboundSyncOps { conn_id, msg } => {
                self.handle_sync_ops(conn_id, &msg).await;
            }
            RelayCommand::PresenceUpdate { conn_id, msg } => {
                self.handle_presence_update(conn_id, &msg).await;
            }
            RelayCommand::AuthChallenge { did, reply } => {
                let result = self.handle_auth_challenge(&did);
                // intentional: oneshot recipient may have disconnected; dropping the reply is expected
                let _ = reply.send(result);
            }
            RelayCommand::AuthVerify {
                did,
                nonce,
                signature,
                reply,
            } => {
                let result = self.handle_auth_verify(&did, &nonce, &signature).await;
                let _ = reply.send(result);
            }
            // ---- MCP commands ----
            RelayCommand::McpValidateToken { token, reply } => {
                let result = self.handle_mcp_validate_token(&token).await;
                let _ = reply.send(result);
            }
            RelayCommand::McpValidateSession {
                session_id,
                expected_did,
                reply,
            } => {
                let result =
                    self.handle_mcp_validate_session_did(&session_id, expected_did.as_deref());
                let _ = reply.send(result);
            }
            RelayCommand::McpCreateSession {
                did,
                pat_hash,
                reply,
            } => {
                let session_id = self.handle_mcp_create_session(&did, pat_hash);
                let _ = reply.send(session_id);
            }
            RelayCommand::McpDestroySession { session_id } => {
                self.handle_mcp_destroy_session(&session_id);
            }
            RelayCommand::McpReadDocument {
                session_id,
                space_id,
                document_id,
                reply,
            } => {
                let result = self
                    .handle_mcp_read_document(&session_id, &space_id, &document_id)
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::ReadDocumentText {
                did,
                space_id,
                document_id,
                reply,
            } => {
                let result = self
                    .handle_read_document_text_by_did(&did, &space_id, &document_id)
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpListDocuments {
                session_id,
                space_id,
                reply,
            } => {
                let result = self.handle_mcp_list_documents(&session_id, &space_id).await;
                let _ = reply.send(result);
            }
            RelayCommand::McpReadLog {
                session_id,
                space_id,
                document_id,
                limit,
                reply,
            } => {
                let result = self
                    .handle_mcp_read_log(&session_id, &space_id, &document_id, limit)
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpListParticipants {
                session_id,
                space_id,
                reply,
            } => {
                let result = self
                    .handle_mcp_list_participants(&session_id, &space_id)
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpStatus {
                session_id,
                space_id,
                reply,
            } => {
                let result = self.handle_mcp_status(&session_id, &space_id).await;
                let _ = reply.send(result);
            }
            RelayCommand::McpEditDocument {
                session_id,
                space_id,
                document_id,
                new_content,
                intent,
                snippet,
                reply,
            } => {
                let result = self
                    .handle_mcp_edit_document(
                        &session_id,
                        &space_id,
                        &document_id,
                        &new_content,
                        &intent,
                        &snippet,
                    )
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpRegisterNotifications { session_id, tx } => {
                self.handle_mcp_register_notifications(&session_id, tx);
            }
            RelayCommand::McpCreateFlag {
                session_id,
                space_id,
                document_id,
                kind,
                message,
                audience,
                target_did,
                reply,
            } => {
                let result = self
                    .handle_mcp_create_flag(
                        &session_id,
                        &space_id,
                        &document_id,
                        kind,
                        &message,
                        audience,
                        &target_did,
                    )
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpGetChanges {
                session_id,
                space_id,
                checkpoint,
                reply,
            } => {
                let result = self
                    .handle_mcp_get_changes(&session_id, &space_id, checkpoint.as_deref())
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpCreateReply {
                session_id,
                space_id,
                parent_signal_id,
                parent_reply_id,
                body,
                reply,
            } => {
                let result = self
                    .handle_mcp_create_reply(
                        &session_id,
                        &space_id,
                        &parent_signal_id,
                        parent_reply_id.as_deref(),
                        &body,
                    )
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpReactToSignal {
                session_id,
                space_id,
                signal_id,
                emoji,
                remove,
                reply,
            } => {
                let result = self
                    .handle_mcp_react_to_signal(&session_id, &space_id, &signal_id, &emoji, remove)
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpCloseFlag {
                session_id,
                space_id,
                signal_id,
                reason,
                close_note,
                reply,
            } => {
                let result = self
                    .handle_mcp_close_flag(
                        &session_id,
                        &space_id,
                        &signal_id,
                        reason.as_deref(),
                        close_note.as_deref(),
                    )
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpReopenFlag {
                session_id,
                space_id,
                signal_id,
                reply,
            } => {
                let result = self
                    .handle_mcp_reopen_flag(&session_id, &space_id, &signal_id)
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpGetSignalDetail {
                session_id,
                space_id,
                signal_id,
                reply,
            } => {
                let result = self
                    .handle_mcp_get_signal_detail(&session_id, &space_id, &signal_id)
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpCreateChat {
                session_id,
                space_id,
                topic,
                target_did,
                reply,
            } => {
                let result = self
                    .handle_mcp_create_chat(
                        &session_id,
                        &space_id,
                        topic.as_deref(),
                        target_did.as_deref(),
                    )
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::Signal { conn_id, msg } => {
                self.handle_signal(conn_id, &msg).await;
            }
            // ---- Document lifecycle commands ----
            RelayCommand::RegisterDocument { conn_id, msg } => {
                self.handle_register_document(conn_id, &msg).await;
            }
            RelayCommand::RenameDocument { conn_id, msg } => {
                self.handle_rename_document(conn_id, &msg).await;
            }
            RelayCommand::UnregisterDocument { conn_id, msg } => {
                self.handle_unregister_document(conn_id, &msg).await;
            }
            // ---- Space ops commands (kutlhub) ----
            RelayCommand::JoinSpaceOp { conn_id, msg } => {
                self.handle_join_space_op(conn_id, &msg).await;
            }
            RelayCommand::ResolveSpaceOp { conn_id, msg } => {
                self.handle_resolve_space_op(conn_id, &msg).await;
            }
            RelayCommand::ListMySpacesOp { conn_id, msg } => {
                self.handle_list_my_spaces_op(conn_id, &msg).await;
            }
            RelayCommand::ListSpaceDocuments { conn_id, msg } => {
                info!(conn_id, space_id = %msg.space_id, "listing space documents");
                self.handle_list_space_documents(conn_id, &msg).await;
            }
            // ---- Space registration commands (OSS HTTP) ----
            RelayCommand::RegisterSpace { name, reply } => {
                let result = self.handle_register_space(&name).await;
                let _ = reply.send(result);
            }
            RelayCommand::ResolveSpace { name, reply } => {
                let result = self.handle_resolve_space(&name).await;
                let _ = reply.send(result);
            }
            RelayCommand::FlushDirty { reply } => {
                self.handle_flush_dirty(reply);
            }
            RelayCommand::FlushCompleted {
                space_id,
                doc_id,
                flushed_up_to,
            } => {
                let key = DocKey {
                    space_id,
                    document_id: doc_id,
                };
                if let Some(slot) = self.documents.get_mut(&key) {
                    slot.content_state.mark_flushed_up_to(flushed_up_to);
                }
            }
            RelayCommand::DeriveEffects {
                space_id,
                document_id,
            } => {
                let key = DocKey {
                    space_id,
                    document_id,
                };
                self.derive_effects(&key);
            }
            RelayCommand::FlushPendingEdit {
                space_id,
                document_id,
            } => {
                self.handle_flush_pending_edit(&space_id, &document_id);
            }
            // ---- Device auth flow commands ----
            RelayCommand::CreateDeviceRequest { reply } => {
                let result = self.handle_create_device_request();
                let _ = reply.send(result);
            }
            RelayCommand::PollDevice { device_code, reply } => {
                let result = self.handle_poll_device(&device_code);
                let _ = reply.send(result);
            }
            RelayCommand::AuthorizeDevice {
                user_code,
                token,
                account_id,
                display_name,
                reply,
            } => {
                let result =
                    self.handle_authorize_device(&user_code, token, account_id, display_name);
                let _ = reply.send(result);
            }
            RelayCommand::ReapMcpSessions => {
                self.handle_reap_mcp_sessions();
            }
            RelayCommand::ReapChanges => {
                if let Some(ref backend) = self.change_backend {
                    let backend = Arc::clone(backend);
                    let now = kutl_core::env::now_ms();
                    tokio::spawn(async move {
                        match backend.prune(now).await {
                            Ok(n) if n > 0 => {
                                info!(pruned = n, "pruned stale change data");
                            }
                            Err(e) => {
                                error!(error = %e, "change data pruning failed");
                            }
                            _ => {}
                        }
                    });
                }
            }
        }
    }

    fn handle_connect(
        &mut self,
        conn_id: ConnId,
        tx: mpsc::Sender<Vec<u8>>,
        ctrl_tx: mpsc::Sender<Vec<u8>>,
    ) {
        info!(conn_id, "connection registered");
        self.connections.insert(
            conn_id,
            ConnChannels {
                data: tx,
                ctrl: ctrl_tx,
            },
        );
    }

    fn handle_disconnect(&mut self, conn_id: ConnId) {
        info!(conn_id, "connection removed");
        self.connections.remove(&conn_id);
        self.authenticated.remove(&conn_id);
        for slot in self.documents.values_mut() {
            slot.subscribers.remove(&conn_id);
        }
        for watchers in self.space_watchers.values_mut() {
            watchers.remove(&conn_id);
        }
        let keys_to_check: Vec<DocKey> = self
            .documents
            .iter()
            .filter(|(_, slot)| slot.subscribers.is_empty())
            .map(|(key, _)| key.clone())
            .collect();
        for key in &keys_to_check {
            self.derive_effects(key);
        }
    }

    async fn handle_handshake(&mut self, conn_id: ConnId, msg: &sync::Handshake) {
        info!(conn_id, client_name = %msg.client_name, display_name = %msg.display_name, "handshake received");

        if self.config.require_auth {
            let auth = self
                .auth
                .as_ref()
                .expect("auth store exists when require_auth is true");
            let now = kutl_core::now_ms();
            let result = auth
                .validate_token_with_backend(
                    &msg.auth_token,
                    now,
                    self.session_backend.as_deref(),
                    self.pat_backend.as_deref(),
                )
                .await;
            match result {
                Ok((did, pat_hash)) => {
                    info!(conn_id, %did, "authenticated");
                    self.authenticated.insert(conn_id, (did, pat_hash));
                }
                Err(e) => {
                    debug!(conn_id, error = %e, "authentication failed");
                    let err = wrap_error(ErrorCode::AuthFailed, e.to_string());
                    self.send_ctrl(conn_id, &encode_envelope(&err));
                    self.connections.remove(&conn_id);
                    return;
                }
            }
        }

        let ack = wrap_handshake_ack(&self.config);
        self.send_ctrl(conn_id, &encode_envelope(&ack));
    }

    fn handle_auth_challenge(&mut self, did: &str) -> Result<ChallengeResponse, AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        let (nonce, expires_at) = auth.create_challenge(did, now)?;
        Ok(ChallengeResponse { nonce, expires_at })
    }

    async fn handle_auth_verify(
        &mut self,
        did: &str,
        nonce: &str,
        signature: &str,
    ) -> Result<VerifyResponse, AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        let (token, expires_at) = auth
            .verify_challenge_with_backend(
                did,
                nonce,
                signature,
                now,
                self.session_backend.as_deref(),
            )
            .await?;
        Ok(VerifyResponse { token, expires_at })
    }

    fn handle_create_device_request(&mut self) -> Result<DeviceRequestResponse, AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        let (device_code, user_code, expires_at) = auth.create_device_request(now);
        Ok(DeviceRequestResponse {
            device_code,
            user_code,
            expires_at,
        })
    }

    fn handle_poll_device(&mut self, device_code: &str) -> Result<DeviceTokenResponse, AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        auth.poll_device(device_code, now)
    }

    fn handle_authorize_device(
        &mut self,
        user_code: &str,
        token: String,
        account_id: String,
        display_name: String,
    ) -> Result<(), AuthError> {
        let auth = self.auth.as_mut().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        auth.authorize_device(user_code, token, account_id, display_name, now)
    }

    // -----------------------------------------------------------------------
    // Space ops handlers (kutlhub — requires Postgres)
    // -----------------------------------------------------------------------

    /// Authoritative `author_did` to record for a connection, overriding any
    /// client-supplied value.
    ///
    /// When `require_auth` is enabled, returns the DID the connection
    /// authenticated as (via bearer token or PAT). Any `author_did` or
    /// `participant_did` field arriving over the wire must be replaced with
    /// this value to prevent an authenticated peer from impersonating a
    /// different user in signals, presence, edit metadata, or lifecycle
    /// events.
    ///
    /// When `require_auth` is disabled (dev/simulation mode), the relay has
    /// no authenticated identity to substitute and returns `None` — callers
    /// fall back to the client-supplied field, preserving the existing
    /// unauthenticated behaviour.
    fn authoritative_author_did(&self, conn_id: ConnId) -> Option<&str> {
        if !self.config.require_auth {
            return None;
        }
        self.authenticated.get(&conn_id).map(|(id, _)| id.as_str())
    }

    /// Resolve the caller's account ID from their authenticated identity.
    ///
    /// Handles both `account:<uuid>` (PAT auth) and DID (challenge-response
    /// auth via `custodied_keys`). Returns `None` if the identity cannot be
    /// resolved to an account.
    async fn resolve_account_id(
        &self,
        conn_id: ConnId,
        membership: &dyn crate::membership_backend::MembershipBackend,
    ) -> Option<String> {
        let (identity, _) = self.authenticated.get(&conn_id)?;
        if let Some(account_id) = spaces::extract_account_id(identity) {
            return Some(account_id.to_owned());
        }
        // DID-based identity — resolve via custodied_keys.
        match membership.resolve_did_to_account(identity).await {
            Ok(Some(account_id)) => Some(account_id),
            Ok(None) => {
                debug!(conn_id, did = %identity, "no account found for DID");
                None
            }
            Err(e) => {
                warn!(conn_id, error = %e, "failed to resolve DID to account");
                None
            }
        }
    }

    /// Handle `RegisterSpace` — create a new space via the space backend.
    async fn handle_register_space(
        &self,
        name: &str,
    ) -> Result<crate::space_backend::RegisteredSpace, crate::space_backend::SpaceBackendError>
    {
        if let Err(msg) = spaces::validate_space_name(name) {
            return Err(crate::space_backend::SpaceBackendError::InvalidName(
                msg.to_owned(),
            ));
        }
        let backend = self.space_backend.as_ref().ok_or_else(|| {
            crate::space_backend::SpaceBackendError::Storage("space backend not configured".into())
        })?;
        let result = backend.register(name).await?;
        info!(space_id = %result.space_id, name = %result.name, "space registered");
        Ok(result)
    }

    /// Handle `ResolveSpace` — look up a space by name via the space backend.
    async fn handle_resolve_space(
        &self,
        name: &str,
    ) -> Result<
        Option<crate::space_backend::RegisteredSpace>,
        crate::space_backend::SpaceBackendError,
    > {
        let backend = self.space_backend.as_ref().ok_or_else(|| {
            crate::space_backend::SpaceBackendError::Storage("space backend not configured".into())
        })?;
        let result = backend.resolve_by_name(name).await?;
        if let Some(ref space) = result {
            info!(space_id = %space.space_id, name = %space.name, "space resolved");
        }
        Ok(result)
    }

    /// Handle `JoinSpace` — accept an invitation code and join the space.
    async fn handle_join_space_op(&self, conn_id: ConnId, msg: &sync::JoinSpace) {
        let Some(ref membership) = self.membership_backend else {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "operation not supported on this relay",
            );
            return;
        };

        let Some(account_id) = self.resolve_account_id(conn_id, membership.as_ref()).await else {
            self.send_error(conn_id, ErrorCode::AuthFailed, "not authenticated");
            return;
        };

        match membership.accept_invitation(&msg.code, &account_id).await {
            Ok(result) => {
                self.send_payload(
                    conn_id,
                    sync::sync_envelope::Payload::JoinSpaceResult(sync::JoinSpaceResult {
                        space_id: result.space_id,
                        space_name: result.space_name,
                        space_slug: result.space_slug,
                        owner_account_id: result.owner_account_id,
                    }),
                );
            }
            Err(e) => {
                warn!(conn_id, error = %e, "join space failed");
                self.send_error(conn_id, ErrorCode::InvalidMessage, &e.to_string());
            }
        }
    }

    /// Handle `ResolveSpace` — look up a space by owner slug and space slug.
    async fn handle_resolve_space_op(&self, conn_id: ConnId, msg: &sync::ResolveSpace) {
        let Some(ref membership) = self.membership_backend else {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "operation not supported on this relay",
            );
            return;
        };

        let Some(account_id) = self.resolve_account_id(conn_id, membership.as_ref()).await else {
            self.send_error(conn_id, ErrorCode::AuthFailed, "not authenticated");
            return;
        };

        match membership
            .resolve_space_by_slugs(&msg.owner, &msg.slug)
            .await
        {
            Ok(Some(record)) => {
                // Verify caller is a member.
                match membership
                    .check_membership(&record.space_id, &account_id)
                    .await
                {
                    Ok(Some(_role)) => {
                        let relay_url = self.config.external_url.clone().unwrap_or_default();
                        self.send_payload(
                            conn_id,
                            sync::sync_envelope::Payload::ResolveSpaceResult(
                                sync::ResolveSpaceResult {
                                    space_id: record.space_id,
                                    space_name: record.space_name,
                                    relay_url,
                                },
                            ),
                        );
                    }
                    Ok(None) => {
                        self.send_error(
                            conn_id,
                            ErrorCode::AuthFailed,
                            "not a member of this space",
                        );
                    }
                    Err(e) => {
                        debug!(conn_id, error = %e, "membership check failed");
                        self.send_error(
                            conn_id,
                            ErrorCode::AuthFailed,
                            "internal error checking membership",
                        );
                    }
                }
            }
            Ok(None) => {
                self.send_error(conn_id, ErrorCode::InvalidMessage, "space not found");
            }
            Err(e) => {
                warn!(conn_id, error = %e, "resolve space failed");
                self.send_error(
                    conn_id,
                    ErrorCode::InvalidMessage,
                    "internal error resolving space",
                );
            }
        }
    }

    /// Handle `ListMySpaces` — return all spaces the caller is a member of.
    async fn handle_list_my_spaces_op(&self, conn_id: ConnId, _msg: &sync::ListMySpaces) {
        let Some(ref membership) = self.membership_backend else {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "operation not supported on this relay",
            );
            return;
        };

        let Some(account_id) = self.resolve_account_id(conn_id, membership.as_ref()).await else {
            self.send_error(conn_id, ErrorCode::AuthFailed, "not authenticated");
            return;
        };

        match membership.list_spaces_for_account(&account_id).await {
            Ok(memberships) => {
                let space_infos: Vec<sync::SpaceInfo> = memberships
                    .into_iter()
                    .map(|m| sync::SpaceInfo {
                        space_id: m.space_id,
                        name: m.space_name,
                        slug: m.space_slug,
                        owner_account_id: m.owner_account_id,
                        role: m.role,
                    })
                    .collect();
                self.send_payload(
                    conn_id,
                    sync::sync_envelope::Payload::ListMySpacesResult(sync::ListMySpacesResult {
                        spaces: space_infos,
                    }),
                );
            }
            Err(e) => {
                warn!(conn_id, error = %e, "list spaces failed");
                self.send_error(
                    conn_id,
                    ErrorCode::InvalidMessage,
                    "internal error listing spaces",
                );
            }
        }
    }

    /// Handle `ListSpaceDocuments` — return all active documents in the space registry.
    ///
    /// Also registers the connection as a space watcher so it receives
    /// lifecycle broadcasts (register/rename/unregister) even if it hasn't
    /// subscribed to any specific document yet.
    async fn handle_list_space_documents(
        &mut self,
        conn_id: ConnId,
        msg: &sync::ListSpaceDocuments,
    ) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                return;
            }
        };

        // Register as a space watcher for lifecycle broadcasts.
        self.space_watchers
            .entry(msg.space_id.clone())
            .or_default()
            .insert(conn_id);

        let documents: Vec<sync::DocumentInfo> = self
            .registries
            .get(&msg.space_id)
            .map(|reg| {
                reg.active_entries()
                    .map(|(_, entry)| sync::DocumentInfo {
                        document_id: entry.document_id.clone(),
                        path: entry.path.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let has_conn = self.connections.contains_key(&conn_id);
        info!(
            conn_id,
            space_id = %msg.space_id,
            document_count = documents.len(),
            has_conn,
            "sending ListSpaceDocumentsResult"
        );

        self.send_payload(
            conn_id,
            sync::sync_envelope::Payload::ListSpaceDocumentsResult(
                sync::ListSpaceDocumentsResult {
                    space_id: msg.space_id.clone(),
                    documents,
                },
            ),
        );
    }

    // -----------------------------------------------------------------------
    // Signal handler (flag payload accepted over WebSocket)
    // -----------------------------------------------------------------------

    /// Handle an incoming signal from a WebSocket client.
    ///
    /// Only flag payloads are accepted over WebSocket. Chat/reply/decision
    /// signals are created exclusively through MCP.
    async fn handle_signal(&mut self, conn_id: ConnId, msg: &sync::Signal) {
        // Only flag payloads arrive via WebSocket.
        let Some(sync::signal::Payload::Flag(flag)) = &msg.payload else {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "only flag signals accepted over websocket",
            );
            return;
        };

        let space_id = &msg.space_id;
        let _authorized = match self.authorize_conn(conn_id, space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                return;
            }
        };

        let document_id = match &msg.document_id {
            Some(d) if !d.is_empty() => d.as_str(),
            _ => {
                self.send_error(
                    conn_id,
                    ErrorCode::InvalidMessage,
                    "flag signal requires document_id",
                );
                return;
            }
        };

        // Reject UNSPECIFIED audience.
        if flag.audience_type == i32::from(sync::AudienceType::Unspecified) {
            self.send_error(conn_id, ErrorCode::InvalidMessage, "audience type required");
            return;
        }

        // Reject PARTICIPANT without target_did.
        if flag.audience_type == i32::from(sync::AudienceType::Participant)
            && flag.target_did.as_ref().is_none_or(String::is_empty)
        {
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                "participant audience requires target_did",
            );
            return;
        }

        // Author identity is authoritative when the relay authenticated
        // this connection. Any client-supplied `author_did` would otherwise
        // let an authenticated member impersonate a different DID in signals
        // (including DM targets and persisted `signals.author_did`).
        let author_did = self
            .authoritative_author_did(conn_id)
            .map_or_else(|| msg.author_did.clone(), str::to_owned);

        let _ = self
            .relay_flag_signal(
                space_id,
                document_id,
                &author_did,
                flag.kind,
                flag.audience_type,
                &flag.message,
                flag.target_did.as_deref().unwrap_or(""),
                msg.timestamp,
                Some(&author_did),
                Some(conn_id),
            )
            .await;
    }

    /// Shared core of flag signal fanout.
    ///
    /// Generates a UUID for the signal, builds the relay envelope, broadcasts to
    /// the appropriate subscribers, notifies the observer, and awaits the backend
    /// write synchronously. Returns the new signal id.
    ///
    /// Called from both the WS path (`handle_signal`) and the MCP path
    /// (`handle_mcp_create_flag`).
    ///
    /// `skip_did` — when `Some`, all connections authenticated as that DID are
    /// excluded from the broadcast (used to suppress echo back to the author).
    /// `skip_conn` — when `Some`, this specific connection is also excluded
    /// (fallback for unauthenticated connections where DID-based skip doesn't work).
    /// For `PARTICIPANT` audience the skip is irrelevant because delivery is
    /// restricted to `target_did` and `author_did` regardless.
    #[allow(clippy::too_many_arguments)]
    async fn relay_flag_signal(
        &mut self,
        space_id: &str,
        document_id: &str,
        author_did: &str,
        kind: i32,
        audience_type: i32,
        message: &str,
        target_did: &str,
        timestamp: i64,
        skip_did: Option<&str>,
        skip_conn: Option<ConnId>,
    ) -> String {
        let signal_id = uuid::Uuid::new_v4().to_string();

        let envelope = signal_flag_envelope(
            &signal_id,
            space_id,
            document_id,
            author_did,
            kind,
            audience_type,
            message,
            target_did,
            timestamp,
        );
        let bytes = encode_envelope(&envelope);

        let is_participant = audience_type == i32::from(sync::AudienceType::Participant);

        if is_participant {
            // DM mode: deliver only to target_did and author_did connections.
            for (&cid, channels) in &self.connections {
                if let Some((did, _)) = self.authenticated.get(&cid)
                    && (*did == target_did || *did == author_did)
                {
                    let _ = channels.ctrl.try_send(bytes.clone());
                }
            }
        } else {
            // Broadcast to all unique subscribers in the space, skipping any
            // connections authenticated as skip_did (the author).
            let mut seen = std::collections::HashSet::new();
            for (key, slot) in &self.documents {
                if key.space_id != space_id {
                    continue;
                }
                for (sub_conn_id, _) in slot.subscribers.active_entries() {
                    let is_skipped = skip_conn == Some(sub_conn_id)
                        || skip_did.is_some_and(|skip| {
                            self.authenticated
                                .get(&sub_conn_id)
                                .is_some_and(|(d, _)| d.as_str() == skip)
                        });
                    if !is_skipped
                        && seen.insert(sub_conn_id)
                        && let Some(channels) = self.connections.get(&sub_conn_id)
                    {
                        let _ = channels.ctrl.try_send(bytes.clone());
                    }
                }
            }
        }

        self.observer.on_signal_created(SignalCreatedEvent {
            id: signal_id.clone(),
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            author_did: author_did.to_owned(),
            signal_type: "flag".to_owned(),
            timestamp: kutl_core::env::now_ms(),
            flag_kind: Some(kind.to_string()),
            audience: Some(audience_type.to_string()),
            target_did: Some(target_did.to_owned()),
            message: Some(message.to_owned()),
            topic: None,
            parent_signal_id: None,
            parent_reply_id: None,
            body: None,
        });

        // Persist signal for agent polling via `get_changes`.
        // Awaited synchronously so the relay confirms the write before returning
        // the id to the MCP caller. Failure is non-fatal: the broadcast already
        // happened, so we log a warning and continue.
        if let Some(ref backend) = self.change_backend
            && let Err(e) = backend
                .record_signal(
                    &signal_id,
                    space_id,
                    Some(document_id),
                    author_did,
                    "flag",
                    timestamp,
                    Some(kind),
                    Some(audience_type),
                    Some(message),
                    Some(target_did),
                    None,
                    None,
                    None,
                    None,
                )
                .await
        {
            error!(signal_id = %signal_id, error = %e, "failed to persist flag signal");
        }

        signal_id
    }

    /// Authorize an identity string for access to a space.
    ///
    /// Returns an [`AuthorizedSpace`] on success. The decision tree:
    /// 0. If the connection was PAT-authed, verify the token is scoped for
    ///    this space before any membership check.
    /// 1. If `require_auth` is false, allow.
    /// 2. If no database and no authorized keys file, allow (dev/test mode).
    /// 3. Account-based identity (`account:<uuid>`) — check `space_memberships`.
    /// 4. DID-based identity — resolve via `custodied_keys`, check `space_memberships`.
    /// 5. Fall back to authorized keys file (OSS relay).
    async fn authorize_space(
        &self,
        identity: &str,
        space_id: &str,
        pat_hash: Option<&str>,
    ) -> Result<AuthorizedSpace, AclError> {
        // Step 0: PAT space-scoping — the token may restrict which spaces
        // the holder can access regardless of membership.
        if let Some(hash) = pat_hash
            && let Some(ref backend) = self.pat_backend
        {
            match backend.check_scope(hash, space_id).await {
                Ok(false) => {
                    debug!(%space_id, "rejected: PAT not scoped for this space");
                    return Err(AclError::NotAuthorized {
                        space_id: space_id.to_owned(),
                    });
                }
                Err(e) => {
                    debug!(error = %e, "PAT scope check failed");
                    return Err(AclError::NotAuthorized {
                        space_id: space_id.to_owned(),
                    });
                }
                Ok(true) => {}
            }
        }
        if !self.config.require_auth {
            return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
        }

        // Startup check in build_app_with_backends_and_web guarantees that
        // at least one of membership_backend or authorized_keys is present
        // when require_auth is true. If we somehow reach this point without
        // either, reject rather than silently allowing.

        // Database-backed ACL checks (kutlhub mode).
        if let Some(ref membership) = self.membership_backend {
            // Account-based identity (PAT auth) — check space_memberships table.
            if let Some(account_id) = spaces::extract_account_id(identity) {
                match membership.check_membership(space_id, account_id).await {
                    Ok(Some(_role)) => {
                        return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
                    }
                    Ok(None) => {
                        debug!(%account_id, %space_id, "rejected: not a member of this space");
                        return Err(AclError::NotAuthorized {
                            space_id: space_id.to_owned(),
                        });
                    }
                    Err(e) => {
                        debug!(error = %e, %space_id, "membership check failed");
                        return Err(AclError::NotAuthorized {
                            space_id: space_id.to_owned(),
                        });
                    }
                }
            }

            // DID-based identity (challenge-response auth).
            // Try resolving DID -> account via custodied_keys, then check
            // space_memberships (kutlhub mode).
            if let Ok(Some(account_id)) = membership.resolve_did_to_account(identity).await {
                match membership.check_membership(space_id, &account_id).await {
                    Ok(Some(_role)) => {
                        return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
                    }
                    Ok(None) => {
                        debug!(%account_id, %space_id, "rejected: not a member of this space");
                        return Err(AclError::NotAuthorized {
                            space_id: space_id.to_owned(),
                        });
                    }
                    Err(e) => {
                        debug!(error = %e, %space_id, "membership check failed");
                        return Err(AclError::NotAuthorized {
                            space_id: space_id.to_owned(),
                        });
                    }
                }
            }
        }

        // Fall back to authorized_keys file (OSS relay).
        if let Some(ref authorized_keys) = self.authorized_keys
            && authorized_keys.is_authorized(identity)
        {
            return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
        }

        debug!(did = %identity, %space_id, "rejected: not authorized");
        Err(AclError::NotAuthorized {
            space_id: space_id.to_owned(),
        })
    }

    /// Authorize a WebSocket connection for access to a space.
    ///
    /// Looks up the connection's authenticated identity and delegates to
    /// [`authorize_space`]. When `require_auth` is false, skips identity
    /// lookup (connections may not have authenticated).
    async fn authorize_conn(
        &self,
        conn_id: ConnId,
        space_id: &str,
    ) -> Result<AuthorizedSpace, AclError> {
        if !self.config.require_auth {
            return Ok(AuthorizedSpace::new_unchecked(space_id.to_owned()));
        }
        let (identity, pat_hash) = self
            .authenticated
            .get(&conn_id)
            .ok_or(AclError::NotAuthenticated)?;
        self.authorize_space(identity, space_id, pat_hash.as_deref())
            .await
    }

    /// Try to load a document from the content or blob backend.
    ///
    /// Returns a pre-populated `DocSlot` on success, or `None` if no backend
    /// data exists.
    /// Ensure a document is in the in-memory slot cache. If the slot is
    /// missing, attempts to load from storage backends (content, then blob)
    /// and inserts it into `self.documents` on success. Returns `true` when
    /// the slot exists after the call.
    ///
    /// Both the MCP `read_document` path and the HTTP `text_export` path
    /// use this so the fallback-on-cache-miss behaviour is DRY.
    async fn ensure_doc_loaded(&mut self, key: &DocKey) -> bool {
        if self.documents.contains_key(key) {
            return true;
        }
        // Only `Found` populates the slot here. `Empty` / `Inconsistent`
        // / `Unavailable` callers (currently MCP read_document and HTTP
        // text_export) treat "no slot available" the same way they always
        // have — the subscribe path below is where the new classification
        // actually matters end-to-end.
        if let LoadResult::Found(slot) = self
            .load_from_backends(&key.space_id, &key.document_id)
            .await
        {
            self.documents.insert(key.clone(), slot);
            return true;
        }
        false
    }

    /// Load classification (RFD 0066). Returned by [`load_from_backends`]
    /// so the subscribe path can distinguish genuinely empty from
    /// "we lost something we previously persisted" from "we can't reach
    /// the backend right now." The hard edit guard in Phase 4 gates
    /// editor writability on this classification.
    async fn load_from_backends(&self, space_id: &str, doc_id: &str) -> LoadResult {
        // Witness lookup first. If the relay has no registry backend
        // (OSS ephemeral), the default impl returns the zero witness —
        // which classifies any `Ok(None)` as `Empty`, matching pre-RFD
        // behavior for self-hosted deployments.
        let witness = if let Some(backend) = &self.registry_backend {
            match backend.load_witness(space_id, doc_id) {
                Ok(w) => w,
                Err(e) => {
                    // Witness read failure is itself a storage-side
                    // unavailability — we can't make a safe claim
                    // about Empty vs Inconsistent without the witness.
                    error!(
                        space_id,
                        doc_id,
                        error = %e,
                        "witness load failed — classifying as Unavailable"
                    );
                    return LoadResult::Unavailable {
                        reason: format!("witness load failed: {e}"),
                    };
                }
            }
        } else {
            crate::registry_store::Witness::default()
        };

        // Record backend outcomes for classification, and retain the
        // data for FoundContent/FoundBlob so we can build the slot.
        let mut content_data: Option<Vec<u8>> = None;
        let mut blob_data: Option<crate::blob_backend::BlobRecord> = None;
        let mut unavailable_reason: Option<String> = None;

        let content_outcome = if let Some(backend) = &self.content_backend {
            match backend.load(space_id, doc_id).await {
                Ok(Some(data)) => {
                    content_data = Some(data);
                    LoadOutcome::Present
                }
                Ok(None) => LoadOutcome::Absent,
                Err(e) => {
                    let reason = format!("content backend load error: {e}");
                    error!(space_id, doc_id, error = %e, "content backend load error");
                    unavailable_reason.get_or_insert(reason);
                    LoadOutcome::Errored
                }
            }
        } else {
            LoadOutcome::Absent
        };

        let blob_outcome = if let Some(backend) = &self.blob_backend {
            match backend.load(space_id, doc_id).await {
                Ok(Some(blob)) => {
                    blob_data = Some(blob);
                    LoadOutcome::Present
                }
                Ok(None) => LoadOutcome::Absent,
                Err(e) => {
                    let reason = format!("blob backend load error: {e}");
                    error!(space_id, doc_id, error = %e, "blob backend load error");
                    unavailable_reason.get_or_insert(reason);
                    LoadOutcome::Errored
                }
            }
        } else {
            LoadOutcome::Absent
        };

        let class = classify_load(content_outcome, blob_outcome, witness.persisted_version);
        Self::build_load_result(
            space_id,
            doc_id,
            class,
            content_data,
            blob_data,
            witness.persisted_version,
            unavailable_reason,
        )
    }

    /// Translate a [`LoadClass`] (from the pure classifier) plus the
    /// retained backend data into a [`LoadResult`] the subscribe path
    /// can act on. Factored out of `load_from_backends` so the I/O
    /// path stays readable; this is pure transformation (no async,
    /// no backend calls) and only logs on the three non-`Found` cases.
    ///
    /// The log line for `Inconsistent` and `Unavailable` matches the
    /// `CloudWatch` metric filter patterns in
    /// `commercial/infra/envs/prod/main.tf` — **do NOT change those
    /// message strings without updating the alarm regex.** RFD 0066
    /// Decision 2 + Phase 6.
    fn build_load_result(
        space_id: &str,
        doc_id: &str,
        class: LoadClass,
        content_data: Option<Vec<u8>>,
        blob_data: Option<crate::blob_backend::BlobRecord>,
        witness_version: u64,
        unavailable_reason: Option<String>,
    ) -> LoadResult {
        match class {
            LoadClass::FoundContent => {
                let data = content_data
                    .expect("classify_load says FoundContent but no content_data was captured");
                let mut doc = kutl_core::Document::new();
                if let Err(e) = doc.merge(&data, &[]) {
                    // Decode failure is "bytes exist but we can't use them."
                    // Treat as Unavailable — same class as backend error —
                    // rather than silently falling through to Empty.
                    error!(
                        space_id,
                        doc_id,
                        error = %e,
                        "content decode failed — classifying as Unavailable"
                    );
                    return LoadResult::Unavailable {
                        reason: format!("content decode failed: {e}"),
                    };
                }
                let mut slot = DocSlot::with_content(DocContent::Text(Box::new(doc)));
                // Seed `persisted_version` from the durable witness so
                // the lattice in memory matches the durable projection.
                // `flushed_counter` will catch up on the next successful
                // flush (or stays behind until a dirty edit triggers one,
                // which is fine — the slot isn't dirty yet).
                slot.content_state.mark_persisted(witness_version);
                LoadResult::Found(slot)
            }
            LoadClass::FoundBlob => {
                let blob =
                    blob_data.expect("classify_load says FoundBlob but no blob_data was captured");
                let mut slot = DocSlot::with_content(DocContent::Blob(BlobData {
                    content: blob.data,
                    hash: blob.hash,
                    timestamp: blob.timestamp,
                }));
                slot.content_state.mark_persisted(witness_version);
                LoadResult::Found(slot)
            }
            LoadClass::Empty => {
                debug!(
                    space_id,
                    doc_id, "load classified as Empty (never persisted)"
                );
                LoadResult::Empty
            }
            LoadClass::Inconsistent { expected_version } => {
                error!(
                    space_id,
                    doc_id,
                    expected_version,
                    "Inconsistent classification: witness claims persisted_version \
                     but backend returned no content"
                );
                LoadResult::Inconsistent { expected_version }
            }
            LoadClass::Unavailable => {
                let reason =
                    unavailable_reason.unwrap_or_else(|| "backend unavailable".to_string());
                error!(
                    space_id,
                    doc_id,
                    reason = %reason,
                    "Unavailable classification: backend could not confirm state"
                );
                LoadResult::Unavailable { reason }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_subscribe(&mut self, conn_id: ConnId, msg: &sync::Subscribe) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                return;
            }
        };

        let key = DocKey {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
        };

        // Classify the load if we don't have the doc cached. RFD 0066:
        // the classification drives the SubscribeStatus envelope below
        // and, in Phase 4, gates editor writability on the client.
        // Cache-hit is treated as Found (the doc was loaded successfully
        // at some earlier subscribe, so its state is well-known).
        let load_status = if self.documents.contains_key(&key) {
            SubscribeStatusInfo::Found
        } else {
            match self
                .load_from_backends(&msg.space_id, &msg.document_id)
                .await
            {
                LoadResult::Found(slot) => {
                    self.documents.insert(key.clone(), slot);
                    SubscribeStatusInfo::Found
                }
                LoadResult::Empty => SubscribeStatusInfo::Empty,
                LoadResult::Inconsistent { expected_version } => {
                    SubscribeStatusInfo::Inconsistent { expected_version }
                }
                LoadResult::Unavailable { reason } => SubscribeStatusInfo::Unavailable { reason },
            }
        };

        // Send SubscribeStatus before anything else so the client can
        // render the correct state even on the error paths where no
        // catch-up follows. Old clients will silently ignore the
        // unknown envelope variant and fall back to pre-RFD behavior.
        let status_envelope = match &load_status {
            SubscribeStatusInfo::Found => subscribe_status_envelope(
                &msg.space_id,
                &msg.document_id,
                sync::LoadStatus::Found,
                0,
                String::new(),
            ),
            SubscribeStatusInfo::Empty => subscribe_status_envelope(
                &msg.space_id,
                &msg.document_id,
                sync::LoadStatus::Empty,
                0,
                String::new(),
            ),
            SubscribeStatusInfo::Inconsistent { expected_version } => subscribe_status_envelope(
                &msg.space_id,
                &msg.document_id,
                sync::LoadStatus::Inconsistent,
                *expected_version,
                String::new(),
            ),
            SubscribeStatusInfo::Unavailable { reason } => subscribe_status_envelope(
                &msg.space_id,
                &msg.document_id,
                sync::LoadStatus::Unavailable,
                0,
                reason.clone(),
            ),
        };
        // SubscribeStatus is control-plane metadata, not a document op —
        // sent on `ctrl` so it doesn't interleave with the catch-up ops
        // stream on `data` (matches the `StaleSubscriber` convention).
        if let Some(channels) = self.connections.get(&conn_id) {
            let bytes = encode_envelope(&status_envelope);
            if let Err(e) = channels.ctrl.try_send(bytes) {
                warn!(conn_id, error = %e, "failed to send subscribe_status");
            }
        }

        // Inconsistent and Unavailable short-circuit: no slot is
        // inserted, no subscriber is registered, no catch-up is sent.
        // The hard edit guard (Phase 4) refuses writes on the client
        // side when it sees these statuses. We also don't register a
        // subscriber because there's no canonical state to diff against
        // — a later retry will redo the subscribe from scratch.
        if matches!(
            load_status,
            SubscribeStatusInfo::Inconsistent { .. } | SubscribeStatusInfo::Unavailable { .. }
        ) {
            info!(
                conn_id,
                space_id = %msg.space_id,
                document_id = %msg.document_id,
                status = ?load_status,
                "subscribe refused (classification is not Found/Empty)"
            );
            return;
        }

        let slot = self
            .documents
            .entry(key.clone())
            .or_insert_with(DocSlot::empty);

        // Remove any existing subscription for this conn (re-subscribe).
        slot.subscribers.remove(&conn_id);

        let (catch_up, known_version) = build_catch_up(slot, &msg.space_id, &msg.document_id);

        slot.subscribers
            .subscribe(conn_id, kutl_core::now_ms_u64(), known_version);

        if let Some(envelope) = catch_up {
            let bytes = encode_envelope(&envelope);
            if let Some(channels) = self.connections.get(&conn_id)
                && let Err(e) = channels.data.try_send(bytes)
            {
                warn!(conn_id, error = %e, "failed to send catch-up data");
            }
        }

        info!(
            conn_id,
            space_id = %msg.space_id,
            document_id = %msg.document_id,
            "subscribed"
        );

        // Reset eviction eligibility now that a subscriber is present.
        self.derive_effects(&key);
    }

    fn handle_unsubscribe(&mut self, conn_id: ConnId, msg: &sync::Unsubscribe) {
        let key = DocKey {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
        };
        if let Some(slot) = self.documents.get_mut(&key) {
            slot.subscribers.remove(&conn_id);
        }
        self.derive_effects(&key);
        info!(
            conn_id,
            space_id = %msg.space_id,
            document_id = %msg.document_id,
            "unsubscribed"
        );
    }

    async fn handle_sync_ops(&mut self, conn_id: ConnId, msg: &sync::SyncOps) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                return;
            }
        };

        let key = DocKey {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
        };

        if !self.documents.contains_key(&key) {
            let err = wrap_error(
                ErrorCode::NotFound,
                format!("no document {}/{}", msg.space_id, msg.document_id),
            );
            self.send_ctrl(conn_id, &encode_envelope(&err));
            return;
        }

        if is_blob_mode(msg) {
            self.handle_blob_sync_ops(conn_id, msg, &key).await;
        } else {
            self.handle_text_sync_ops(conn_id, msg, &key).await;
        }
    }

    /// Handle inbound text CRDT sync ops.
    async fn handle_text_sync_ops(&mut self, conn_id: ConnId, msg: &sync::SyncOps, key: &DocKey) {
        // Quota pre-check (RFD 0063 §3, review C3): even without an inline
        // delta, gate text inbound on `storage_bytes_used >= storage_bytes_total`
        // so an account at cap cannot keep accumulating durable CRDT bytes.
        // The flush task (Task 8) updates the counter post-write. `actual_bytes`
        // is unused by the text path (per_blob check is disabled) — pass 0.
        if let Err(err) = self.check_inbound_quota(&msg.space_id, 0, false).await {
            self.send_ctrl(conn_id, &encode_envelope(&err));
            return;
        }

        // Pin observer / event `author_did` to the connection's authenticated
        // identity so a peer cannot attribute its edits to another user in
        // after-merge events, MCP notifications, or the edited_at_pending
        // flush.
        let authoritative = self.authoritative_author_did(conn_id).map(str::to_owned);

        let slot = self.documents.get_mut(key).expect("slot checked by caller");

        // Ensure content is Text (transition from Empty or Blob if needed).
        if slot.ensure_text() {
            // Was Blob — reset subscriber versions since content type changed.
            slot.subscribers.reset_all_versions();
        }

        let mut meta = extract_sync_edit_meta(msg, slot, self.config.snippet_max_doc_chars);
        if let Some(ref did) = authoritative {
            meta.author_did.clone_from(did);
        }

        // If a different author has a pending edit, flush it before merge so
        // the observer sees the pre-merge content. This must happen
        // unconditionally (not gated on snippet_eligible) because the new
        // edit may be non-eligible (client snippet, doc too large) but the
        // *pending* edit still needs to be flushed correctly.
        if let Some(ref pending) = slot.pending_edit
            && pending.author_did != meta.author_did
        {
            self.flush_pending_edit_for(key);
        }

        // Notify before-merge observer (captures baseline for snippet/mention diffing).
        {
            let slot = self.documents.get(key).expect("slot checked by caller");
            if let DocContent::Text(doc) = &slot.content {
                self.before_merge
                    .before_text_merge(&msg.space_id, &msg.document_id, doc);
            }
        }

        // Merge ops. Distinguish the per-document op-count cap and per-patch
        // size cap (RFD 0063 §3) — both surface to the client as
        // `QUOTA_EXCEEDED` with a typed `cap_kind` so the UI can pick the
        // right copy. Other merge errors remain `InvalidMessage`.
        let slot = self.documents.get_mut(key).expect("slot checked by caller");
        let ops = msg.ops.clone();
        let ops_metadata = msg.metadata.clone();
        if let Err(e) = slot.mutate_text(|doc| doc.merge(&ops, &ops_metadata)) {
            warn!(conn_id, error = %e, "failed to merge inbound ops");
            let err = match e {
                kutl_core::Error::OpCountExceeded { cap, .. } => {
                    #[allow(clippy::cast_possible_wrap)]
                    let limit = cap as i64;
                    wrap_quota_exceeded(CapKind::OpCount, limit, None)
                }
                kutl_core::Error::PatchTooLarge { cap, .. } => {
                    #[allow(clippy::cast_possible_wrap)]
                    let limit = cap as i64;
                    wrap_quota_exceeded(CapKind::BlobSize, limit, None)
                }
                _ => wrap_error(ErrorCode::InvalidMessage, e.to_string()),
            };
            self.send_ctrl(conn_id, &encode_envelope(&err));
            return;
        }

        // Mark the doc as having a pending edited_at flush. The write is
        // performed by flush_pending_edit_for, which fires at most once per
        // snippet_debounce_ms per doc (or immediately for snippet-ineligible
        // edits via emit_text_edit_event).
        slot.edited_at_pending = Some(kutl_core::env::now_ms());

        build_and_relay_text_outbound(
            &self.connections,
            slot,
            key,
            Some(conn_id),
            &msg.space_id,
            &msg.document_id,
        );

        // Notify MCP sessions of the inbound edit. Pass the authenticated
        // DID so the notification reports the real editor even when the
        // inbound metadata was forged.
        self.notify_mcp_sessions_from_ws(
            &msg.space_id,
            &msg.document_id,
            &msg.metadata,
            authoritative.as_deref(),
        );

        // Emit the edit event (immediate or deferred depending on snippet eligibility).
        self.emit_text_edit_event(
            key,
            &msg.space_id,
            &msg.document_id,
            meta.author_did,
            meta.intent,
            meta.op_count,
            meta.client_snippet.as_deref(),
            meta.snippet_eligible,
        );
    }

    /// Emit or defer the edit event depending on snippet eligibility.
    ///
    /// - Client snippet present OR not snippet-eligible: flush any pending
    ///   edit, then call `after_text_merge` immediately.
    /// - Snippet-eligible (no client snippet): accumulate into `PendingEdit`
    ///   or create a new one, reset the debounce timer.
    #[allow(clippy::too_many_arguments)]
    fn emit_text_edit_event(
        &mut self,
        key: &DocKey,
        space_id: &str,
        document_id: &str,
        author_did: String,
        intent: String,
        op_count: usize,
        client_snippet: Option<&str>,
        snippet_eligible: bool,
    ) {
        if client_snippet.is_some() || !snippet_eligible {
            self.flush_pending_edit_for(key);
            let slot = self.documents.get(key).expect("slot checked by caller");
            if let DocContent::Text(doc) = &slot.content {
                self.after_merge.after_text_merge(
                    MergedEvent {
                        space_id: space_id.to_owned(),
                        document_id: document_id.to_owned(),
                        author_did,
                        op_count,
                        intent,
                        content_mode: EditContentMode::Text,
                        timestamp: kutl_core::env::now_ms(),
                    },
                    doc,
                );
            }
        } else {
            let slot = self.documents.get_mut(key).expect("slot checked by caller");
            if let Some(ref mut pending) = slot.pending_edit {
                // Same author (different author was already flushed) — accumulate.
                pending.op_count += op_count;
                pending.intent.clone_from(&intent);
            } else {
                slot.pending_edit = Some(PendingEdit {
                    author_did,
                    intent,
                    op_count,
                    timestamp: kutl_core::env::now_ms(),
                });
            }
            self.start_snippet_timer(space_id, document_id);
        }
    }

    /// Load and validate the storage quota for a space on the inbound path.
    ///
    /// Returns:
    /// - `Ok(None)` if no quota backend is configured (OSS relay fast-path)
    ///   or if the per-account caps all pass given `actual_bytes`.
    /// - `Ok(Some(q))` when the caller needs the quota details (the blob
    ///   path does not need them; the text path doesn't call this variant).
    /// - `Err(envelope)` pre-built with the appropriate error if any cap is
    ///   hit, the space is not found, or the load itself fails. Callers
    ///   should forward the envelope to the originating connection and
    ///   return.
    async fn check_inbound_quota(
        &self,
        space_id: &str,
        actual_bytes: i64,
        check_per_blob: bool,
    ) -> Result<(), sync::SyncEnvelope> {
        let Some(ref qb) = self.quota_backend else {
            return Ok(());
        };
        match qb.load_space_quota(space_id).await {
            Ok(Some(q)) => {
                if check_per_blob && actual_bytes > q.per_blob_size {
                    return Err(wrap_quota_exceeded(
                        CapKind::BlobSize,
                        q.per_blob_size,
                        None,
                    ));
                }
                if q.storage_bytes_used >= q.storage_bytes_total {
                    return Err(wrap_quota_exceeded(
                        CapKind::StorageTotal,
                        q.storage_bytes_total,
                        Some(q.storage_bytes_used),
                    ));
                }
                Ok(())
            }
            Ok(None) => Err(wrap_error(ErrorCode::NotFound, "space not found")),
            Err(e) => {
                error!(space_id, error = %e, "quota load failed; denying write");
                Err(wrap_quota_exceeded(CapKind::StorageTotal, 0, None))
            }
        }
    }

    /// Handle inbound binary blob sync ops (LWW, no merging).
    async fn handle_blob_sync_ops(&mut self, conn_id: ConnId, msg: &sync::SyncOps, key: &DocKey) {
        // Flush any pending text edit — blob ops are not snippet-eligible.
        self.flush_pending_edit_for(key);

        // Reject BLOB_REF — not implemented.
        if msg.content_mode == i32::from(sync::ContentMode::BlobRef) {
            let err = wrap_error(ErrorCode::InvalidMessage, "BLOB_REF is not implemented");
            self.send_ctrl(conn_id, &encode_envelope(&err));
            return;
        }

        // RFD 0063 §C4: msg.blob_size is client-advertised and untrusted. All
        // enforcement below uses msg.ops.len() (server-measured). We keep a
        // debug log to spot clients whose advertised size diverges from
        // actual — purely observational.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        if msg.blob_size as usize != msg.ops.len() {
            debug!(
                advertised = msg.blob_size,
                actual = msg.ops.len(),
                "client-advertised blob_size differs from actual payload size"
            );
        }

        // Defense-in-depth: the WS codec already enforces ABSOLUTE_BLOB_MAX
        // on the frame before we parse. Recheck here against actual bytes
        // in case anything slipped through.
        if msg.ops.len() > ABSOLUTE_BLOB_MAX {
            #[allow(clippy::cast_possible_wrap)]
            let limit = ABSOLUTE_BLOB_MAX as i64;
            let err = wrap_quota_exceeded(CapKind::BlobSize, limit, None);
            self.send_ctrl(conn_id, &encode_envelope(&err));
            return;
        }

        // Quota-backend pre-check (skipped on OSS relay when backend is
        // None). Reject based on server-measured `msg.ops.len()` — never
        // on the advertised `msg.blob_size`.
        #[allow(clippy::cast_possible_wrap)]
        let actual_len = msg.ops.len() as i64;
        if let Err(err) = self
            .check_inbound_quota(&msg.space_id, actual_len, true)
            .await
        {
            self.send_ctrl(conn_id, &encode_envelope(&err));
            return;
        }

        let timestamp = msg.metadata.first().map_or(0, |m| m.timestamp);

        let slot = self.documents.get_mut(key).expect("slot checked by caller");

        // LWW: only accept if newer than current blob.
        if let DocContent::Blob(existing) = &slot.content
            && timestamp <= existing.timestamp
        {
            return;
        }

        // Store the blob (transition from any previous content type).
        slot.set_blob(BlobData {
            content: msg.ops.clone(),
            hash: msg.content_hash.clone(),
            timestamp,
        });

        // Advance edited_at so get_changes consumers see the blob edit.
        // Uses the same edited_at_pending mechanism as the text path;
        // flush_pending_edit_for is called at the end of this function
        // (no debounce — blob uploads are atomic, not per-keystroke).
        slot.edited_at_pending = Some(kutl_core::env::now_ms());

        // RFD 0063 §C2: apply the storage delta INLINE (not spawned) before
        // we fan out. Spawning would let back-to-back PUTs each pass the
        // pre-check in parallel and overshoot by N × per_blob_size. Awaiting
        // inline serializes through the actor's single-task loop and bounds
        // overshoot to `in_flight × per_blob_size` per connection.
        //
        // On retry-exhaustion we log + continue — the blob is in memory and
        // will flush; the monthly reconcile corrects any counter drift.
        if let Some(ref qb) = self.quota_backend {
            #[allow(clippy::cast_possible_wrap)]
            let new_size = msg.ops.len() as i64;
            if let Err(e) =
                apply_delta_with_retry(qb.as_ref(), &msg.space_id, &msg.document_id, new_size).await
            {
                error!(
                    space_id = %msg.space_id,
                    doc_id = %msg.document_id,
                    error = %e,
                    "quota delta failed after retries — reconcile will correct",
                );
            }
        }

        // Build the relay envelope (forward as-is to other subscribers).
        let relay_envelope = blob_ops_envelope(
            &msg.space_id,
            &msg.document_id,
            msg.ops.clone(),
            msg.content_hash.clone(),
            msg.metadata.first().cloned(),
        );
        let relay_bytes = encode_envelope(&relay_envelope);

        // Mark sender as having the current blob.
        if let Some(entry) = slot.subscribers.get_mut(&conn_id) {
            entry.known_version = BLOB_KNOWN.to_vec();
        }

        // Build outbound for other subscribers.
        let outbound: Vec<(ConnId, Vec<u8>)> = slot
            .subscribers
            .active_entries()
            .filter(|&(id, _)| id != conn_id)
            .map(|(id, _)| (id, relay_bytes.clone()))
            .collect();

        relay_and_evict(&self.connections, slot, key, outbound, BLOB_KNOWN, 0);

        // Notify observer about the blob edit. Use the authenticated DID
        // when available so a peer cannot forge a different `author_did` in
        // observer fan-out.
        let author_did = self.authoritative_author_did(conn_id).map_or_else(
            || {
                msg.metadata
                    .first()
                    .map_or_else(String::new, |m| m.author_did.clone())
            },
            str::to_owned,
        );
        self.observer.on_blob_edited(MergedEvent {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
            author_did,
            op_count: 1,
            intent: "blob".into(),
            content_mode: EditContentMode::Blob,
            timestamp: kutl_core::env::now_ms(),
        });

        // Flush the edited_at write immediately (blob uploads are
        // complete on arrival — no debounce window like text edits).
        self.flush_pending_edit_for(key);
    }

    // -----------------------------------------------------------------------
    // Presence relay
    // -----------------------------------------------------------------------

    /// Relay a presence update to all other subscribers of the same document.
    ///
    /// The relay performs no presence logic — it is a pure relay. The sender's
    /// presence is forwarded as-is to every other subscriber of the document.
    async fn handle_presence_update(&self, conn_id: ConnId, msg: &sync::PresenceUpdate) {
        if self.authorize_conn(conn_id, &msg.space_id).await.is_err() {
            return;
        }
        let key = DocKey {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
        };

        let Some(slot) = self.documents.get(&key) else {
            return; // not subscribed to this document
        };

        // Pin `participant_did` to the authenticated identity when available
        // so an authenticated peer cannot broadcast cursor positions labelled
        // as another user.
        let participant_did = self
            .authoritative_author_did(conn_id)
            .unwrap_or(&msg.participant_did);

        let envelope = presence_update_envelope(
            &msg.space_id,
            &msg.document_id,
            participant_did,
            msg.cursor_pos,
            msg.custom_data.clone(),
        );
        let bytes = encode_envelope(&envelope);

        for (sub_conn_id, _) in slot.subscribers.active_entries() {
            if sub_conn_id == conn_id {
                continue; // don't echo back to sender
            }
            if let Some(channels) = self.connections.get(&sub_conn_id) {
                let _ = channels.data.try_send(bytes.clone());
            }
        }
    }

    // -----------------------------------------------------------------------
    // MCP handlers
    // -----------------------------------------------------------------------

    async fn handle_mcp_validate_token(
        &self,
        token: &str,
    ) -> Result<(String, Option<String>), AuthError> {
        let auth = self.auth.as_ref().ok_or(AuthError::AuthNotRequired)?;
        let now = kutl_core::now_ms();
        auth.validate_token_with_backend(
            token,
            now,
            self.session_backend.as_deref(),
            self.pat_backend.as_deref(),
        )
        .await
    }

    fn handle_mcp_create_session(&mut self, did: &str, pat_hash: Option<String>) -> McpSessionId {
        let session_id = generate_session_id();
        info!(session_id = %session_id, %did, "MCP session created");
        self.mcp_sessions.insert(
            session_id.clone(),
            McpSession {
                did: did.to_owned(),
                pat_hash,
                notify_tx: None,
                last_active: tokio::time::Instant::now(),
            },
        );
        session_id
    }

    fn handle_mcp_destroy_session(&mut self, session_id: &str) {
        if self.mcp_sessions.remove(session_id).is_some() {
            info!(session_id, "MCP session destroyed");
        }
    }

    /// Validate that a session exists and optionally belongs to the expected DID.
    ///
    /// When `expected_did` is `Some`, both existence and DID ownership are
    /// checked (auth-on path). When `None`, only existence is verified
    /// (auth-off path where the DID was self-asserted at initialize time).
    fn handle_mcp_validate_session_did(
        &mut self,
        session_id: &str,
        expected_did: Option<&str>,
    ) -> Result<(), McpError> {
        let did = self.validate_mcp_session(session_id)?;
        if let Some(expected) = expected_did
            && did != expected
        {
            return Err(McpError::SessionNotFound);
        }
        Ok(())
    }

    /// Validate that an MCP session exists (and is not idle-expired) and return
    /// the DID. Refreshes `last_active` on success.
    fn validate_mcp_session(&mut self, session_id: &str) -> Result<String, McpError> {
        let session = self
            .mcp_sessions
            .get_mut(session_id)
            .ok_or(McpError::SessionNotFound)?;

        if session.last_active.elapsed() > MCP_SESSION_IDLE_TTL {
            info!(session_id, did = %session.did, "MCP session expired (idle)");
            self.mcp_sessions.remove(session_id);
            return Err(McpError::SessionNotFound);
        }

        session.last_active = tokio::time::Instant::now();
        Ok(session.did.clone())
    }

    /// Validate the MCP session and authorize the caller against the space.
    ///
    /// Returns the caller's DID on success. This is the standard 6-line preamble
    /// shared by all MCP handler methods that operate on a specific space.
    async fn authorize_mcp_caller(
        &mut self,
        session_id: &str,
        space_id: &str,
    ) -> Result<String, McpError> {
        let did = self.validate_mcp_session(session_id)?;
        let pat_hash = self
            .mcp_sessions
            .get(session_id)
            .and_then(|s| s.pat_hash.as_deref().map(str::to_owned));
        let _ = self
            .authorize_space(&did, space_id, pat_hash.as_deref())
            .await
            .map_err(|_| McpError::NotAuthorized {
                space_id: space_id.to_owned(),
            })?;
        Ok(did)
    }

    /// Resolve a potentially non-UUID `document_id` to the internal UUID
    /// via the registry's path index. Returns the original value if it's
    /// already a UUID or has no registry mapping.
    fn resolve_doc_id(&self, space_id: &str, document_id: &str) -> String {
        if uuid::Uuid::try_parse(document_id).is_ok() {
            return document_id.to_owned();
        }
        if let Some(reg) = self.registries.get(space_id)
            && let Some(entry) = reg.get_by_path(document_id)
        {
            return entry.document_id.clone();
        }
        document_id.to_owned()
    }

    async fn handle_mcp_read_document(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
    ) -> Result<McpDocumentContent, McpError> {
        self.authorize_mcp_caller(session_id, space_id).await?;

        let resolved_id = self.resolve_doc_id(space_id, document_id);
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: resolved_id,
        };

        if !self.ensure_doc_loaded(&key).await {
            return Err(McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
            });
        }
        let slot = self
            .documents
            .get(&key)
            .expect("ensure_doc_loaded returned true");

        match &slot.content {
            DocContent::Empty => Ok(McpDocumentContent {
                content: String::new(),
                version: "[]".into(),
            }),
            DocContent::Text(doc) => Ok(McpDocumentContent {
                content: doc.content(),
                version: format!("{:?}", doc.local_version()),
            }),
            DocContent::Blob(_) => Err(McpError::NotTextDocument),
        }
    }

    async fn handle_read_document_text_by_did(
        &mut self,
        did: &str,
        space_id: &str,
        document_id: &str,
    ) -> Result<String, McpError> {
        let _authorized = self
            .authorize_space(did, space_id, None)
            .await
            .map_err(|_| McpError::NotAuthorized {
                space_id: space_id.to_owned(),
            })?;

        let resolved_id = self.resolve_doc_id(space_id, document_id);
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: resolved_id,
        };

        if !self.ensure_doc_loaded(&key).await {
            return Err(McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
            });
        }
        let slot = self
            .documents
            .get(&key)
            .expect("ensure_doc_loaded returned true");

        match &slot.content {
            DocContent::Empty => Err(McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
            }),
            DocContent::Text(doc) => Ok(doc.content()),
            DocContent::Blob(_) => Err(McpError::NotTextDocument),
        }
    }

    async fn handle_mcp_list_documents(
        &mut self,
        session_id: &str,
        space_id: &str,
    ) -> Result<Vec<McpDocumentSummary>, McpError> {
        self.authorize_mcp_caller(session_id, space_id).await?;

        // Iterate the registry (source of truth), same as the WS
        // ListSpaceDocuments path. Augment with in-memory content_type
        // and subscriber_count when the doc is loaded.
        let docs: Vec<McpDocumentSummary> = self
            .registries
            .get(space_id)
            .map(|reg| {
                reg.active_entries()
                    .map(|(_, entry)| {
                        let key = DocKey {
                            space_id: space_id.to_owned(),
                            document_id: entry.document_id.clone(),
                        };
                        let (content_type, subscriber_count) = match self.documents.get(&key) {
                            Some(slot) => {
                                let ct = match &slot.content {
                                    DocContent::Empty => "empty",
                                    DocContent::Text(_) => "text",
                                    DocContent::Blob(_) => "blob",
                                };
                                (ct, slot.subscribers.active_count())
                            }
                            None => ("unknown", 0),
                        };
                        McpDocumentSummary {
                            document_id: entry.document_id.clone(),
                            path: entry.path.clone(),
                            content_type: content_type.into(),
                            subscriber_count,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(docs)
    }

    async fn handle_mcp_read_log(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<McpLogEntry>, McpError> {
        self.authorize_mcp_caller(session_id, space_id).await?;

        let resolved_id = self.resolve_doc_id(space_id, document_id);
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: resolved_id,
        };

        let slot = self
            .documents
            .get(&key)
            .ok_or_else(|| McpError::DocumentNotFound {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
            })?;

        let changes = match &slot.content {
            DocContent::Text(doc) => doc.changes(),
            _ => return Ok(Vec::new()),
        };

        let iter = changes.iter().rev();
        let entries: Vec<McpLogEntry> = if let Some(n) = limit {
            iter.take(n).map(change_to_log_entry).collect()
        } else {
            iter.map(change_to_log_entry).collect()
        };

        Ok(entries)
    }

    async fn handle_mcp_list_participants(
        &mut self,
        session_id: &str,
        space_id: &str,
    ) -> Result<Vec<McpParticipant>, McpError> {
        self.authorize_mcp_caller(session_id, space_id).await?;

        let mut participants = Vec::new();

        // Collect WS-authenticated DIDs subscribed to any doc in this space.
        let mut seen_dids = std::collections::HashSet::new();
        for (key, slot) in &self.documents {
            if key.space_id != space_id {
                continue;
            }
            for (sub_conn_id, _) in slot.subscribers.active_entries() {
                if let Some((did, _)) = self.authenticated.get(&sub_conn_id)
                    && seen_dids.insert(did.clone())
                {
                    participants.push(McpParticipant {
                        did: did.clone(),
                        connection_type: "websocket".into(),
                    });
                }
            }
        }

        // Collect MCP sessions (all are space-agnostic for now).
        for session in self.mcp_sessions.values() {
            if seen_dids.insert(session.did.clone()) {
                participants.push(McpParticipant {
                    did: session.did.clone(),
                    connection_type: "mcp".into(),
                });
            }
        }

        Ok(participants)
    }

    async fn handle_mcp_status(
        &mut self,
        session_id: &str,
        space_id: &str,
    ) -> Result<McpSpaceStatus, McpError> {
        self.authorize_mcp_caller(session_id, space_id).await?;

        let mut document_count = 0;
        let mut subscriber_count = 0;

        for (key, slot) in &self.documents {
            if key.space_id == space_id {
                document_count += 1;
                subscriber_count += slot.subscribers.active_count();
            }
        }

        Ok(McpSpaceStatus {
            document_count,
            subscriber_count,
            mcp_session_count: self.mcp_sessions.len(),
        })
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn handle_mcp_edit_document(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
        new_content: &str,
        intent: &str,
        _snippet: &str,
    ) -> Result<McpEditResult, McpError> {
        let did = self.authorize_mcp_caller(session_id, space_id).await?;

        // Resolve a non-UUID document_id to a UUID, registering if new.
        let (actual_doc_id, doc_path) = self.resolve_mcp_edit_doc_id(space_id, document_id);

        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: actual_doc_id.clone(),
        };

        // Auto-register new documents in the relay registry so the UX
        // server mirrors them to Postgres.
        let is_new = !self.documents.contains_key(&key);
        if is_new {
            self.auto_register_new_mcp_doc(space_id, &actual_doc_id, &doc_path, &did)
                .await;
        }

        // Auto-create document slot if it doesn't exist.
        let slot = self
            .documents
            .entry(key.clone())
            .or_insert_with(DocSlot::empty);

        // Ensure content is Text.
        if let DocContent::Blob(_) = &slot.content {
            return Err(McpError::NotTextDocument);
        }
        slot.ensure_text();

        // Notify before-merge observer (captures baseline for snippet/mention diffing).
        if let DocContent::Text(doc) = &slot.content {
            self.before_merge
                .before_text_merge(space_id, &actual_doc_id, doc);
        }

        // Apply the edit. mutate_text sets dirty only if the closure
        // returns Ok, so the zero-ops case (no mutation) stays clean.
        // Use session_id (36 bytes) as CRDT agent name — the DID can
        // exceed diamond-types' 50-byte limit.
        let did_owned = did.clone();
        let session_owned = session_id.to_owned();
        let result = slot.mutate_text(|doc| {
            let agent = doc
                .register_agent(&session_owned)
                .map_err(|e| McpError::EditFailed(e.to_string()))?;
            let outcome = doc
                .replace_content(agent, &did_owned, intent, Boundary::Explicit, new_content)
                .map_err(|e| McpError::EditFailed(e.to_string()))?;
            Ok((outcome, doc.local_version()))
        })?;
        let (outcome, hub_version) = result;

        if outcome.ops_applied == 0 {
            return Ok(McpEditResult {
                document_id: actual_doc_id,
                new_version: format!("{hub_version:?}"),
                ops_applied: 0,
                was_bulk: false,
            });
        }

        // Mark the doc as having a pending edited_at flush. The
        // flush_pending_edit_for call below will write it to the registry.
        let slot = self
            .documents
            .get_mut(&key)
            .expect("doc slot must exist after merge");
        slot.edited_at_pending = Some(kutl_core::env::now_ms());

        let new_version_str = format!("{hub_version:?}");

        // Relay to WS subscribers (per-subscriber delta encoding).
        build_and_relay_text_outbound(
            &self.connections,
            slot,
            &key,
            None, // MCP edits have no WS sender — relay to all subscribers.
            &key.space_id,
            &key.document_id,
        );

        // Notify other MCP sessions.
        self.send_mcp_notification(session_id, &key.space_id, &key.document_id, &did, intent);

        // Flush any pending WS edit (different source), then notify the
        // after-merge observer. MCP edits bypass the debounce window.
        self.flush_pending_edit_for(&key);
        if let DocContent::Text(doc) = &self.documents.get(&key).expect("slot just used").content {
            self.after_merge.after_text_merge(
                MergedEvent {
                    space_id: space_id.to_owned(),
                    document_id: actual_doc_id.clone(),
                    author_did: did,
                    op_count: outcome.ops_applied,
                    intent: intent.to_owned(),
                    content_mode: EditContentMode::Text,
                    timestamp: kutl_core::env::now_ms(),
                },
                doc,
            );
        }

        Ok(McpEditResult {
            document_id: actual_doc_id,
            new_version: new_version_str,
            ops_applied: outcome.ops_applied,
            was_bulk: outcome.was_bulk,
        })
    }

    /// Resolve a `document_id` that may be a path or a UUID.
    ///
    /// If it is a UUID, it is used as-is and both elements of the returned
    /// tuple are the UUID. Otherwise the registry is consulted for an existing
    /// UUID, or a new UUID is generated with the original string as the path.
    fn resolve_mcp_edit_doc_id(&mut self, space_id: &str, document_id: &str) -> (String, String) {
        if uuid::Uuid::try_parse(document_id).is_ok() {
            return (document_id.to_owned(), document_id.to_owned());
        }
        // Check if this path is already registered under a UUID.
        let reg = self.registries.entry(space_id.to_owned()).or_default();
        if let Some(entry) = reg.get_by_path(document_id) {
            (entry.document_id.clone(), document_id.to_owned())
        } else {
            (uuid::Uuid::new_v4().to_string(), document_id.to_owned())
        }
    }

    /// Register a newly created MCP document in the relay registry, broadcast
    /// to WebSocket subscribers, and notify the observer. Resolves the DID to
    /// an account ID if a database is configured.
    async fn auto_register_new_mcp_doc(
        &mut self,
        space_id: &str,
        actual_doc_id: &str,
        doc_path: &str,
        did: &str,
    ) {
        let account_id = if let Some(ref membership) = self.membership_backend {
            membership.resolve_did_to_account(did).await.ok().flatten()
        } else {
            None
        };
        // Ignore registration errors (e.g., path conflict) — the edit can
        // still proceed on the in-memory slot.
        let _ = self.register_document_internal(
            space_id,
            actual_doc_id,
            doc_path,
            did,
            account_id,
            0, // no WebSocket sender to skip
        );
    }

    /// Create a flag signal via MCP.
    #[allow(clippy::too_many_arguments)]
    async fn handle_mcp_create_flag(
        &mut self,
        session_id: &str,
        space_id: &str,
        document_id: &str,
        kind: i32,
        message: &str,
        audience: i32,
        target_did: &str,
    ) -> Result<String, McpError> {
        let did = self.authorize_mcp_caller(session_id, space_id).await?;

        let signal_id = self
            .relay_flag_signal(
                space_id,
                document_id,
                &did,
                kind,
                audience,
                message,
                target_did,
                kutl_core::env::now_ms(),
                Some(&did),
                None,
            )
            .await;

        Ok(signal_id)
    }

    /// Create a chat signal via MCP.
    ///
    /// Chats are not broadcast over WebSocket — they exist only as persisted
    /// signals queryable via `get_changes` and the detail endpoint.
    async fn handle_mcp_create_chat(
        &mut self,
        session_id: &str,
        space_id: &str,
        topic: Option<&str>,
        target_did: Option<&str>,
    ) -> Result<String, McpError> {
        let did = self.authorize_mcp_caller(session_id, space_id).await?;
        let signal_id = uuid::Uuid::new_v4().to_string();
        let now = kutl_core::env::now_ms();

        self.observer.on_signal_created(SignalCreatedEvent {
            id: signal_id.clone(),
            space_id: space_id.to_owned(),
            document_id: String::new(),
            author_did: did.clone(),
            signal_type: "chat".to_owned(),
            timestamp: now,
            flag_kind: None,
            audience: None,
            target_did: target_did.map(str::to_owned),
            message: None,
            topic: topic.map(str::to_owned),
            parent_signal_id: None,
            parent_reply_id: None,
            body: None,
        });

        if let Some(ref backend) = self.change_backend
            && let Err(e) = backend
                .record_signal(
                    &signal_id, space_id, None, &did, "chat", now, None, None, None, target_did,
                    topic, None, None, None,
                )
                .await
        {
            error!(signal_id = %signal_id, error = %e, "failed to persist chat signal");
        }

        Ok(signal_id)
    }

    async fn handle_mcp_get_changes(
        &mut self,
        session_id: &str,
        space_id: &str,
        checkpoint: Option<&str>,
    ) -> Result<crate::change_backend::ChangesResponse, McpError> {
        let did = self.authorize_mcp_caller(session_id, space_id).await?;

        let Some(ref backend) = self.change_backend else {
            return Ok(crate::change_backend::ChangesResponse::default());
        };

        // The cursor must be unique per agent. When auth is enabled, the DID
        // is unique. When auth is disabled, all sessions get DID "anonymous" —
        // using that as the cursor key means agents share one cursor and steal
        // each other's signals. Use session_id as the cursor key instead.
        let cursor_key = if did == "anonymous" { session_id } else { &did };

        backend
            .get_changes(cursor_key, space_id, checkpoint)
            .await
            .map_err(|e| McpError::Internal(e.to_string()))
    }

    /// Create a reply to a signal via MCP.
    ///
    /// Generates a UUID, emits a [`SignalCreatedEvent`] (`signal_type`: "reply"),
    /// and persists via the change backend. No WebSocket broadcast.
    async fn handle_mcp_create_reply(
        &mut self,
        session_id: &str,
        space_id: &str,
        parent_signal_id: &str,
        parent_reply_id: Option<&str>,
        body: &str,
    ) -> Result<String, McpError> {
        let did = self.authorize_mcp_caller(session_id, space_id).await?;
        let signal_id = uuid::Uuid::new_v4().to_string();
        let now = kutl_core::env::now_ms();

        self.observer.on_signal_created(SignalCreatedEvent {
            id: signal_id.clone(),
            space_id: space_id.to_owned(),
            document_id: String::new(),
            author_did: did.clone(),
            signal_type: "reply".to_owned(),
            timestamp: now,
            flag_kind: None,
            audience: None,
            target_did: None,
            message: None,
            topic: None,
            parent_signal_id: Some(parent_signal_id.to_owned()),
            parent_reply_id: parent_reply_id.map(str::to_owned),
            body: Some(body.to_owned()),
        });

        if let Some(ref backend) = self.change_backend
            && let Err(e) = backend
                .record_signal(
                    &signal_id,
                    space_id,
                    None,
                    &did,
                    "reply",
                    now,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(parent_signal_id),
                    parent_reply_id,
                    Some(body),
                )
                .await
        {
            error!(signal_id = %signal_id, error = %e, "failed to persist reply signal");
        }

        Ok(signal_id)
    }

    /// Add or remove a reaction on a signal via MCP.
    ///
    /// Fires the observer event for stream propagation. No direct DB write.
    /// Verify that a signal belongs to the claimed space. Prevents
    /// cross-space mutations where a caller authorized for space-A
    /// supplies a `signal_id` that actually lives in space-B.
    ///
    /// Skipped when no change backend is configured (OSS relay with
    /// ephemeral in-memory signals).
    async fn verify_signal_in_space(
        &self,
        space_id: &str,
        signal_id: &str,
    ) -> Result<(), McpError> {
        let Some(ref backend) = self.change_backend else {
            return Ok(());
        };
        match backend.get_signal_detail(space_id, signal_id).await {
            Ok(_) => Ok(()),
            Err(crate::change_backend::ChangeError::NotFound(_)) => Err(McpError::SignalNotFound {
                signal_id: signal_id.to_owned(),
            }),
            Err(e) => Err(McpError::Internal(e.to_string())),
        }
    }

    async fn handle_mcp_react_to_signal(
        &mut self,
        session_id: &str,
        space_id: &str,
        signal_id: &str,
        emoji: &str,
        remove: bool,
    ) -> Result<(), McpError> {
        let did = self.authorize_mcp_caller(session_id, space_id).await?;
        self.verify_signal_in_space(space_id, signal_id).await?;

        self.observer.on_reaction(ReactionEvent {
            space_id: space_id.to_owned(),
            signal_id: signal_id.to_owned(),
            actor_did: did,
            emoji: emoji.to_owned(),
            remove,
            timestamp: kutl_core::env::now_ms(),
        });

        Ok(())
    }

    /// Close a flag signal via MCP.
    ///
    /// Fires the observer event for stream propagation. When `reason` is
    /// `None`, defaults to `"resolved"`.
    async fn handle_mcp_close_flag(
        &mut self,
        session_id: &str,
        space_id: &str,
        signal_id: &str,
        reason: Option<&str>,
        close_note: Option<&str>,
    ) -> Result<(), McpError> {
        let did = self.authorize_mcp_caller(session_id, space_id).await?;
        self.verify_signal_in_space(space_id, signal_id).await?;

        let resolved_reason = reason.map_or_else(|| "resolved".to_owned(), str::to_owned);

        self.observer.on_signal_closed(SignalClosedEvent {
            space_id: space_id.to_owned(),
            signal_id: signal_id.to_owned(),
            actor_did: did,
            reason: resolved_reason,
            close_note: close_note.map(str::to_owned),
            timestamp: kutl_core::env::now_ms(),
        });

        Ok(())
    }

    /// Reopen a previously closed flag signal via MCP.
    ///
    /// Fires the observer event for stream propagation.
    async fn handle_mcp_reopen_flag(
        &mut self,
        session_id: &str,
        space_id: &str,
        signal_id: &str,
    ) -> Result<(), McpError> {
        let did = self.authorize_mcp_caller(session_id, space_id).await?;
        self.verify_signal_in_space(space_id, signal_id).await?;

        self.observer.on_signal_reopened(SignalReopenedEvent {
            space_id: space_id.to_owned(),
            signal_id: signal_id.to_owned(),
            actor_did: did,
            timestamp: kutl_core::env::now_ms(),
        });

        Ok(())
    }

    /// Fetch full detail for a single signal via MCP.
    ///
    /// Uses the synchronous read path via the change backend. Returns an error
    /// if the backend is not configured or the signal does not exist.
    async fn handle_mcp_get_signal_detail(
        &mut self,
        session_id: &str,
        space_id: &str,
        signal_id: &str,
    ) -> Result<crate::change_backend::SignalDetail, McpError> {
        self.authorize_mcp_caller(session_id, space_id).await?;

        let Some(ref backend) = self.change_backend else {
            return Err(McpError::Internal(
                "signal detail not available on this relay".into(),
            ));
        };

        backend
            .get_signal_detail(space_id, signal_id)
            .await
            .map_err(|e| match e {
                crate::change_backend::ChangeError::NotFound(_) => McpError::SignalNotFound {
                    signal_id: signal_id.to_owned(),
                },
                other => McpError::Internal(other.to_string()),
            })
    }

    fn handle_mcp_register_notifications(&mut self, session_id: &str, tx: mpsc::Sender<String>) {
        if let Some(session) = self.mcp_sessions.get_mut(session_id) {
            session.notify_tx = Some(tx);
            info!(session_id, "MCP SSE notifications registered");
        }
    }

    fn handle_reap_mcp_sessions(&mut self) {
        let before = self.mcp_sessions.len();
        self.mcp_sessions.retain(|id, session| {
            if session.last_active.elapsed() > MCP_SESSION_IDLE_TTL {
                info!(session_id = %id, did = %session.did, "MCP session reaped (idle)");
                false
            } else {
                true
            }
        });
        let reaped = before - self.mcp_sessions.len();
        if reaped > 0 {
            info!(
                reaped,
                remaining = self.mcp_sessions.len(),
                "MCP session reap complete"
            );
        }
    }

    /// Send a JSON-RPC document-changed notification to MCP sessions.
    ///
    /// `exclude_session_id` is skipped (use `""` to notify all sessions).
    /// Dead notification channels are cleaned up automatically.
    fn send_mcp_notification(
        &mut self,
        exclude_session_id: &str,
        space_id: &str,
        document_id: &str,
        author_did: &str,
        intent: &str,
    ) {
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/document/changed",
            "params": {
                "space_id": space_id,
                "document_id": document_id,
                "author_did": author_did,
                "intent": intent
            }
        });
        let json = notification.to_string();

        let mut dead_sessions = Vec::new();

        for (id, session) in &self.mcp_sessions {
            if id == exclude_session_id {
                continue;
            }
            if let Some(tx) = &session.notify_tx
                && tx.try_send(json.clone()).is_err()
            {
                dead_sessions.push(id.clone());
            }
        }

        for id in dead_sessions {
            debug!(session_id = %id, "MCP notification channel closed, removing session");
            self.mcp_sessions.remove(&id);
        }
    }

    /// Notify MCP sessions about a WS-originated edit.
    ///
    /// `authoritative_did`, when `Some`, overrides `change.author_did` for
    /// every notification — this is the relay-authenticated identity of
    /// the sender.
    fn notify_mcp_sessions_from_ws(
        &mut self,
        space_id: &str,
        document_id: &str,
        metadata: &[sync::ChangeMetadata],
        authoritative_did: Option<&str>,
    ) {
        if self.mcp_sessions.is_empty() {
            return;
        }

        for change in metadata {
            let author_did = authoritative_did.unwrap_or(&change.author_did);
            self.send_mcp_notification("", space_id, document_id, author_did, &change.intent);
        }
    }

    // -----------------------------------------------------------------------
    // Document lifecycle handlers
    // -----------------------------------------------------------------------

    /// Register a document in the space registry, persist the entry,
    /// broadcast the registration to all WebSocket subscribers, and
    /// notify the observer. Called by both WebSocket and MCP paths.
    ///
    /// `skip_conn` is the WebSocket connection to exclude from the broadcast
    /// (the sender). Use `0` when there is no WebSocket sender (e.g. MCP).
    fn register_document_internal(
        &mut self,
        space_id: &str,
        document_id: &str,
        path: &str,
        author_did: &str,
        account_id: Option<String>,
        skip_conn: ConnId,
    ) -> Result<(), String> {
        let meta = registry::EntryMetadata {
            author_did: author_did.to_owned(),
            timestamp: kutl_core::env::now_ms(),
            account_id,
        };

        let reg = self.registries.entry(space_id.to_owned()).or_default();
        reg.register(document_id, path, meta)
            .map_err(|e| e.to_string())?;

        persist_entry(
            self.registry_backend.as_deref(),
            &self.registries,
            space_id,
            document_id,
        );

        let proto_msg = sync::RegisterDocument {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            path: path.to_owned(),
            metadata: Some(sync::ChangeMetadata {
                author_did: author_did.to_owned(),
                timestamp: kutl_core::env::now_ms(),
                ..Default::default()
            }),
        };
        self.broadcast_lifecycle_to_space(
            space_id,
            skip_conn,
            sync::sync_envelope::Payload::RegisterDocument(proto_msg),
        );

        self.observer
            .on_document_registered(DocumentRegisteredEvent {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                author_did: author_did.to_owned(),
                path: path.to_owned(),
                timestamp: kutl_core::env::now_ms(),
            });

        Ok(())
    }

    async fn handle_register_document(&mut self, conn_id: ConnId, msg: &sync::RegisterDocument) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                return;
            }
        };

        let mut meta = extract_meta(msg.metadata.as_ref());
        // Overwrite the client-supplied `author_did` with the connection's
        // authenticated identity so `registry.created_by` and the mirrored
        // `documents.created_by` column attribute the registration to the
        // real caller rather than any spoofed DID in the envelope.
        if let Some(did) = self.authoritative_author_did(conn_id) {
            meta.author_did = did.to_owned();
        }

        // Resolve the connection's account_id (kutlhub mode).
        if let Some(ref membership) = self.membership_backend {
            meta.account_id = self.resolve_account_id(conn_id, membership.as_ref()).await;
        }

        if let Err(e) = self.register_document_internal(
            &msg.space_id,
            &msg.document_id,
            &msg.path,
            &meta.author_did,
            meta.account_id,
            conn_id,
        ) {
            warn!(error = %e, doc_id = %msg.document_id, "register document failed");
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                &format!("register failed: {e}"),
            );
        } else {
            info!(conn_id, doc_id = %msg.document_id, "document registered");
        }
    }

    async fn handle_rename_document(&mut self, conn_id: ConnId, msg: &sync::RenameDocument) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                return;
            }
        };

        let authoritative = self.authoritative_author_did(conn_id).map(str::to_owned);
        let mut meta = extract_meta(msg.metadata.as_ref());
        // Pin `renamed_by` to the authenticated identity so the registry
        // entry reflects the real caller.
        if let Some(ref did) = authoritative {
            meta.author_did.clone_from(did);
        }

        let reg = self.registries.entry(msg.space_id.clone()).or_default();

        if let Err(e) = reg.rename(&msg.document_id, &msg.old_path, &msg.new_path, meta) {
            warn!(error = %e, doc_id = %msg.document_id, "rename document failed");
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                &format!("rename failed: {e}"),
            );
            return;
        }

        persist_entry(
            self.registry_backend.as_deref(),
            &self.registries,
            &msg.space_id,
            &msg.document_id,
        );

        // Sanitize the payload forwarded to other subscribers and the
        // observer: overwrite `metadata.author_did` with the authoritative
        // identity so no peer sees the spoofed DID.
        let mut forwarded = msg.clone();
        if let Some(ref did) = authoritative {
            let mut new_meta = forwarded.metadata.unwrap_or_default();
            new_meta.author_did.clone_from(did);
            forwarded.metadata = Some(new_meta);
        }
        let observed_author_did = forwarded
            .metadata
            .as_ref()
            .map_or_else(String::new, |m| m.author_did.clone());

        self.broadcast_lifecycle_to_space(
            &msg.space_id,
            conn_id,
            sync::sync_envelope::Payload::RenameDocument(forwarded),
        );

        self.observer.on_document_renamed(DocumentRenamedEvent {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
            author_did: observed_author_did,
            old_path: msg.old_path.clone(),
            new_path: msg.new_path.clone(),
            timestamp: kutl_core::env::now_ms(),
        });
    }

    async fn handle_unregister_document(
        &mut self,
        conn_id: ConnId,
        msg: &sync::UnregisterDocument,
    ) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.send_error(conn_id, ErrorCode::AuthFailed, &e.to_string());
                return;
            }
        };

        let authoritative = self.authoritative_author_did(conn_id).map(str::to_owned);
        let mut meta = extract_meta(msg.metadata.as_ref());
        if let Some(ref did) = authoritative {
            meta.author_did.clone_from(did);
        }

        let reg = self.registries.entry(msg.space_id.clone()).or_default();

        if let Err(e) = reg.unregister(&msg.document_id, &meta) {
            warn!(error = %e, doc_id = %msg.document_id, "unregister document failed");
            self.send_error(
                conn_id,
                ErrorCode::InvalidMessage,
                &format!("unregister failed: {e}"),
            );
            return;
        }

        // Persist the soft-delete.
        if let Some(ref backend) = self.registry_backend
            && let Some(entry) = self
                .registries
                .get(&msg.space_id)
                .and_then(|r| r.get_any(&msg.document_id))
            && let Some(deleted_at) = entry.deleted_at
            && let Err(e) = backend.delete_entry(&msg.space_id, &msg.document_id, deleted_at)
        {
            error!(error = %e, doc_id = %msg.document_id, "failed to persist unregister");
        }

        // Sanitize the broadcast payload so subscribers never see a spoofed
        // `author_did` on the lifecycle notification.
        let mut forwarded = msg.clone();
        if let Some(ref did) = authoritative {
            let mut new_meta = forwarded.metadata.unwrap_or_default();
            new_meta.author_did.clone_from(did);
            forwarded.metadata = Some(new_meta);
        }
        let observed_author_did = forwarded
            .metadata
            .as_ref()
            .map_or_else(String::new, |m| m.author_did.clone());

        self.broadcast_lifecycle_to_space(
            &msg.space_id,
            conn_id,
            sync::sync_envelope::Payload::UnregisterDocument(forwarded),
        );

        self.observer
            .on_document_unregistered(DocumentUnregisteredEvent {
                space_id: msg.space_id.clone(),
                document_id: msg.document_id.clone(),
                author_did: observed_author_did,
                timestamp: kutl_core::env::now_ms(),
            });
    }

    /// Broadcast a lifecycle message to all subscribers in a space (except sender).
    fn broadcast_lifecycle_to_space(
        &self,
        space_id: &str,
        sender: ConnId,
        payload: sync::sync_envelope::Payload,
    ) {
        let envelope = sync::SyncEnvelope {
            payload: Some(payload),
        };
        let bytes = encode_envelope(&envelope);

        let mut seen = HashSet::new();

        // Include space-level watchers (connections that called ListSpaceDocuments).
        if let Some(watchers) = self.space_watchers.get(space_id) {
            for &watcher_id in watchers {
                if watcher_id != sender && seen.insert(watcher_id) {
                    self.send_ctrl(watcher_id, &bytes);
                }
            }
        }

        // Include per-document subscribers.
        for (key, slot) in &self.documents {
            if key.space_id != space_id {
                continue;
            }
            for (sub_conn_id, _) in slot.subscribers.active_entries() {
                if sub_conn_id != sender && seen.insert(sub_conn_id) {
                    self.send_ctrl(sub_conn_id, &bytes);
                }
            }
        }
    }

    /// Migrate path-based `DocKey`s to UUID-based using the registry.
    ///
    /// Scans all document keys for entries that look like file paths
    /// (containing `/` or `.`), looks up the matching registry entry by path,
    /// and re-keys the document slot from the old path-based key to the new
    /// UUID-based key. Called once at startup and can be triggered after
    /// bulk registration.
    pub fn migrate_path_based_keys(&mut self) {
        let mut total_migrated = 0u32;

        for (space_id, reg) in &self.registries {
            let mut migrations: Vec<(DocKey, String)> = Vec::new();

            for key in self.documents.keys() {
                if key.space_id != *space_id {
                    continue;
                }
                // Heuristic: path-based IDs contain '/' or '.' — UUIDs contain
                // only hex digits and hyphens (no '/' or '.').
                if (key.document_id.contains('/') || key.document_id.contains('.'))
                    && let Some(entry) = reg.get_by_path(&key.document_id)
                {
                    migrations.push((key.clone(), entry.document_id.clone()));
                }
            }

            for (old_key, new_id) in migrations {
                if let Some(slot) = self.documents.remove(&old_key) {
                    let new_key = DocKey {
                        space_id: old_key.space_id,
                        document_id: new_id.clone(),
                    };
                    info!(
                        old_id = %old_key.document_id,
                        new_id = %new_id,
                        "migrated path-based `DocKey` to UUID"
                    );
                    self.documents.insert(new_key, slot);
                    total_migrated += 1;
                }
            }
        }

        if total_migrated > 0 {
            info!(
                count = total_migrated,
                "path-to-UUID DocKey migration complete"
            );
        }
    }

    /// Access the document registry for a space (read-only, for tests/MCP).
    pub fn registry(&self, space_id: &str) -> Option<&registry::DocumentRegistry> {
        self.registries.get(space_id)
    }

    // -----------------------------------------------------------------------
    // Eviction
    // -----------------------------------------------------------------------

    /// Derive side effects from document state. Idempotent — safe to call
    /// after every command or on a timer.
    #[allow(clippy::cast_sign_loss)]
    fn derive_effects(&mut self, key: &DocKey) {
        // Only evict if we have a content backend (data is persisted).
        if self.content_backend.is_none() {
            return;
        }

        let Some(slot) = self.documents.get_mut(key) else {
            return;
        };

        // Reset eviction eligibility if subscribers are present.
        if !slot.subscribers.is_empty() {
            slot.eviction.reset();
            return;
        }

        // Mark eligible if not already.
        let now = kutl_core::now_ms_u64();
        if slot.eviction.eligible_since == 0 {
            slot.eviction.mark_eligible(now);

            // Schedule a check after the grace period.
            if let Some(ref tx) = self.self_tx {
                let tx = tx.clone();
                let space_id = key.space_id.clone();
                let document_id = key.document_id.clone();
                let delay_ms = slot.eviction.grace_period_ms;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    let _ = tx
                        .send(RelayCommand::DeriveEffects {
                            space_id,
                            document_id,
                        })
                        .await;
                });
            }
            return;
        }

        // Pre-check before releasing the mutable borrow on self.documents.
        // The full condition is re-verified below with a shared borrow so
        // flush_pending_edit_for (which takes &mut self) can be called.
        if !slot.eviction.should_evict(now) || slot.is_dirty() {
            return;
        }

        if self.documents.get(key).is_some_and(|s| {
            s.subscribers.is_empty() && s.eviction.should_evict(now) && !s.is_dirty()
        }) {
            self.flush_pending_edit_for(key);
            self.documents.remove(key);
            info!(
                space_id = %key.space_id,
                document_id = %key.document_id,
                "evicted document from memory"
            );
        }
    }

    /// Start or reset the snippet debounce timer for a document.
    ///
    /// Aborts any existing timer and spawns a new one that sends
    /// [`RelayCommand::FlushPendingEdit`] after the configured debounce delay.
    fn start_snippet_timer(&mut self, space_id: &str, document_id: &str) {
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
        };
        let Some(slot) = self.documents.get_mut(&key) else {
            return;
        };

        // Abort any existing timer.
        if let Some(handle) = slot.snippet_timer.take() {
            handle.abort();
        }

        // Spawn a new delayed self-command.
        if let Some(ref tx) = self.self_tx {
            let tx = tx.clone();
            let s_id = space_id.to_owned();
            let d_id = document_id.to_owned();
            let delay = Duration::from_millis(self.config.snippet_debounce_ms);
            let handle = tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = tx
                    .send(RelayCommand::FlushPendingEdit {
                        space_id: s_id,
                        document_id: d_id,
                    })
                    .await;
            });
            slot.snippet_timer = Some(handle);
        }
    }

    /// Flush a pending edit for the given document, notifying the after-merge
    /// observer. No-op if no pending edit exists.
    fn flush_pending_edit_for(&mut self, key: &DocKey) {
        let Some(slot) = self.documents.get_mut(key) else {
            return;
        };

        if let Some(handle) = slot.snippet_timer.take() {
            handle.abort();
        }

        // Flush pending edited_at write (independent of snippet eligibility).
        if let Some(ts) = slot.edited_at_pending.take()
            && let Some(ref backend) = self.registry_backend
            && let Err(e) = backend.update_edited_at(&key.space_id, &key.document_id, ts)
        {
            error!(
                error = %e,
                doc_id = %key.document_id,
                "failed to flush edited_at",
            );
        }

        // Flush pending snippet edit (existing behavior).
        let Some(pending) = slot.pending_edit.take() else {
            return;
        };

        let DocContent::Text(doc) = &slot.content else {
            return;
        };

        self.after_merge.after_text_merge(
            MergedEvent {
                space_id: key.space_id.clone(),
                document_id: key.document_id.clone(),
                author_did: pending.author_did,
                op_count: pending.op_count,
                intent: pending.intent,
                content_mode: EditContentMode::Text,
                timestamp: pending.timestamp,
            },
            doc,
        );
    }

    /// Handle a snippet debounce timer firing — delegates to [`flush_pending_edit_for`].
    fn handle_flush_pending_edit(&mut self, space_id: &str, document_id: &str) {
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
        };
        self.flush_pending_edit_for(&key);
    }

    /// Check whether a document is currently loaded in memory.
    #[cfg(test)]
    pub fn has_document(&self, space_id: &str, doc_id: &str) -> bool {
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: doc_id.to_owned(),
        };
        self.documents.contains_key(&key)
    }

    /// Attach an authenticated identity to a connection.
    ///
    /// Exposed so integration tests can populate the identity map without
    /// running the full HTTP auth flow. Production code should rely on the
    /// `Handshake` path to set this.
    #[doc(hidden)]
    pub fn test_set_authenticated(&mut self, conn_id: ConnId, identity: &str) {
        self.authenticated
            .insert(conn_id, (identity.to_owned(), None));
    }

    fn send_ctrl(&self, conn_id: ConnId, bytes: &[u8]) {
        if let Some(channels) = self.connections.get(&conn_id)
            && let Err(e) = channels.ctrl.try_send(bytes.to_vec())
        {
            warn!(conn_id, error = %e, "failed to send control message");
        }
    }

    /// Send an error frame to a connection via the control channel.
    fn send_error(&self, conn_id: ConnId, code: ErrorCode, message: &str) {
        let frame = encode_envelope(&wrap_error(code, message));
        self.send_ctrl(conn_id, &frame);
    }

    /// Send a `SyncEnvelope` payload to a connection via the control channel.
    fn send_payload(&self, conn_id: ConnId, payload: sync::sync_envelope::Payload) {
        let envelope = sync::SyncEnvelope {
            payload: Some(payload),
        };
        self.send_ctrl(conn_id, &encode_envelope(&envelope));
    }

    /// Encode all dirty documents and send them through the reply channel.
    ///
    /// This is synchronous — `encode_full()` is CPU work and `try_send` is
    /// non-blocking. When the reply sender is dropped at return, the flush
    /// task's receiver sees `None` and stops.
    #[allow(clippy::needless_pass_by_value)] // Sender drop signals end-of-batch
    fn handle_flush_dirty(&mut self, reply: mpsc::Sender<FlushEntry>) {
        for (key, slot) in &mut self.documents {
            if !slot.is_dirty() {
                continue;
            }
            let content = match &slot.content {
                DocContent::Empty => continue,
                DocContent::Text(doc) => {
                    if doc.local_version().is_empty() {
                        continue;
                    }
                    FlushContent::Text(doc.encode_full())
                }
                DocContent::Blob(blob) => FlushContent::Blob(crate::blob_backend::BlobRecord {
                    data: blob.content.clone(),
                    hash: blob.hash.clone(),
                    timestamp: blob.timestamp,
                }),
            };
            // Measure durable size from the encoded bytes we're about to hand
            // off. Casting usize → i64 is safe: payloads are bounded by
            // `ABSOLUTE_BLOB_MAX` (25 MB) well below `i64::MAX`.
            #[allow(clippy::cast_possible_wrap)]
            let size_bytes = match &content {
                FlushContent::Text(data) => data.len() as i64,
                FlushContent::Blob(blob) => blob.data.len() as i64,
            };
            let entry = FlushEntry {
                space_id: key.space_id.clone(),
                doc_id: key.document_id.clone(),
                content,
                size_bytes,
                flushed_up_to: slot.content_state.edit_counter,
            };
            if reply.try_send(entry).is_err() {
                warn!(
                    space_id = %key.space_id,
                    doc_id = %key.document_id,
                    "flush channel full, deferring"
                );
                break;
            }
            // Dirty flag is NOT cleared here. The flush task will send
            // FlushCompleted on successful persist, which advances
            // flushed_counter. If the persist fails, the slot stays
            // dirty and the next flush cycle retries.
        }
    }

    /// Check whether a document has been marked dirty (mutated since last flush).
    #[cfg(test)]
    pub fn is_document_dirty(&self, space_id: &str, doc_id: &str) -> bool {
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: doc_id.to_owned(),
        };
        self.documents.get(&key).is_some_and(DocSlot::is_dirty)
    }
}

/// Relay outbound messages to subscribers, evicting any that can't keep up.
///
/// On successful `try_send`, advances the subscriber's `known_version` to
/// `new_version`. On failure, sends a `StaleSubscriber` notice via the
/// control channel and removes the subscriber.
///
/// Free function to avoid borrow conflicts with `self.documents` and
/// `self.connections`.
/// Build the catch-up envelope and `known_version` for a new subscriber.
///
/// Extracted from `handle_subscribe` to separate the content-type dispatch
/// (pure computation on slot content) from the subscription bookkeeping.
fn build_catch_up(
    slot: &DocSlot,
    space_id: &str,
    document_id: &str,
) -> (Option<kutl_proto::sync::SyncEnvelope>, Vec<usize>) {
    match &slot.content {
        DocContent::Empty => {
            let envelope = sync_ops_envelope(space_id, document_id, Vec::new(), Vec::new());
            (Some(envelope), Vec::new())
        }
        DocContent::Text(doc) => {
            let catch_up = if doc.local_version().is_empty() {
                None
            } else {
                let ops = doc.encode_full();
                let metadata = doc.changes_since(&[]);
                Some(sync_ops_envelope(space_id, document_id, ops, metadata))
            };
            let version = doc.local_version();
            (catch_up, version)
        }
        DocContent::Blob(blob) => {
            let metadata = Some(kutl_proto::sync::ChangeMetadata {
                timestamp: blob.timestamp,
                ..Default::default()
            });
            let envelope = blob_ops_envelope(
                space_id,
                document_id,
                blob.content.clone(),
                blob.hash.clone(),
                metadata,
            );
            (Some(envelope), BLOB_KNOWN.to_vec())
        }
    }
}

fn relay_and_evict(
    connections: &HashMap<ConnId, ConnChannels>,
    slot: &mut DocSlot,
    key: &DocKey,
    outbound: Vec<(ConnId, Vec<u8>)>,
    new_version: &[usize],
    relay_ops_count: u64,
) {
    let mut stale_conns: Vec<ConnId> = Vec::new();

    for (target, bytes) in outbound {
        if let Some(channels) = connections.get(&target) {
            match channels.data.try_send(bytes) {
                Ok(()) => {
                    if let Some(entry) = slot.subscribers.get_mut(&target) {
                        entry.known_version = new_version.to_vec();
                    }
                }
                Err(e) => {
                    let reason = match &e {
                        mpsc::error::TrySendError::Full(_) => sync::StaleReason::ChannelFull,
                        mpsc::error::TrySendError::Closed(_) => sync::StaleReason::ChannelClosed,
                    };
                    debug!(conn_id = target, ?reason, "evicting stale subscriber");

                    let notice = stale_subscriber_envelope(
                        &key.space_id,
                        &key.document_id,
                        reason,
                        1,
                        relay_ops_count,
                        kutl_core::now_ms(),
                    );
                    let _ = channels.ctrl.try_send(encode_envelope(&notice));

                    stale_conns.push(target);
                }
            }
        }
    }

    for conn_id in &stale_conns {
        slot.subscribers.remove(conn_id);
    }
}

/// Fields extracted from inbound `SyncOps` metadata, used for event emission
/// and snippet eligibility decisions.
struct SyncEditMeta {
    author_did: String,
    intent: String,
    /// Client-supplied snippet, if non-empty.
    client_snippet: Option<String>,
    /// Number of metadata entries (one per op group).
    op_count: usize,
    /// Whether this edit qualifies for relay-side snippet computation.
    snippet_eligible: bool,
}

/// Extract edit metadata from a `SyncOps` message and the current slot state.
///
/// `snippet_max_doc_chars` comes from `RelayConfig`. Zero disables relay-side
/// snippet computation.
fn extract_sync_edit_meta(
    msg: &sync::SyncOps,
    slot: &DocSlot,
    snippet_max_doc_chars: usize,
) -> SyncEditMeta {
    let first = msg.metadata.first();
    let author_did = first.map_or_else(String::new, |m| m.author_did.clone());
    let intent = first.map_or_else(String::new, |m| m.intent.clone());
    let client_snippet = first
        .map(|m| &m.change_snippet)
        .filter(|s| !s.is_empty())
        .cloned();
    let op_count = msg.metadata.len();
    let snippet_eligible = snippet_max_doc_chars > 0
        && client_snippet.is_none()
        && matches!(&slot.content, DocContent::Text(doc) if doc.len() <= snippet_max_doc_chars);
    SyncEditMeta {
        author_did,
        intent,
        client_snippet,
        op_count,
        snippet_eligible,
    }
}

/// Build per-subscriber outbound deltas from the current text document and
/// relay them, evicting stale subscribers.
///
/// Also advances the sending connection's `known_version` to the relay's
/// current version (the sender does not receive its own ops back).
///
/// `exclude_conn_id` is the WebSocket sender whose connection should be
/// skipped (it receives an ack via the control channel, not a relay copy).
/// Pass `None` for MCP edits which have no WS sender.
///
/// Free function to avoid borrow conflicts between `self.documents` and
/// `self.connections`.
fn build_and_relay_text_outbound(
    connections: &HashMap<ConnId, ConnChannels>,
    slot: &mut DocSlot,
    key: &DocKey,
    exclude_conn_id: Option<ConnId>,
    space_id: &str,
    document_id: &str,
) {
    let DocContent::Text(doc) = &slot.content else {
        unreachable!();
    };
    let hub_version = doc.local_version();

    // Build per-subscriber outbound deltas, skipping the sender if any.
    let mut outbound: Vec<(ConnId, Vec<u8>)> = Vec::new();
    for (sub_conn_id, entry) in slot.subscribers.active_entries() {
        if Some(sub_conn_id) == exclude_conn_id {
            continue;
        }
        let delta = doc.encode_since(&entry.known_version);
        if delta.is_empty() {
            continue;
        }
        let metadata = doc.changes_since(&entry.known_version);
        let envelope = sync_ops_envelope(space_id, document_id, delta, metadata);
        outbound.push((sub_conn_id, encode_envelope(&envelope)));
    }

    // Advance sender's known_version so it won't receive its own ops back.
    if let Some(sender) = exclude_conn_id
        && let Some(entry) = slot.subscribers.get_mut(&sender)
    {
        entry.known_version.clone_from(&hub_version);
    }

    let relay_ops_count = doc.changes().len() as u64;
    relay_and_evict(
        connections,
        slot,
        key,
        outbound,
        &hub_version,
        relay_ops_count,
    );
}

/// Convert a `ChangeMetadata` proto to an `McpLogEntry`.
fn change_to_log_entry(c: &sync::ChangeMetadata) -> McpLogEntry {
    McpLogEntry {
        author_did: c.author_did.clone(),
        intent: c.intent.clone(),
        timestamp: c.timestamp,
        id: c.id.clone(),
        boundary: match Boundary::try_from(c.boundary) {
            Ok(Boundary::Explicit) => "explicit",
            Ok(Boundary::Auto) => "auto",
            _ => "unspecified",
        }
        .into(),
        full_rewrite: c.full_rewrite,
    }
}

/// Generate a random MCP session ID.
fn generate_session_id() -> String {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    /// Session ID random bytes.
    const SESSION_ID_SIZE: usize = 24;

    let bytes: Vec<u8> = (0..SESSION_ID_SIZE).map(|_| rand::random::<u8>()).collect();
    format!("mcp_{}", URL_SAFE_NO_PAD.encode(&bytes))
}

/// Free function to avoid borrow conflicts between `self.registries` and
/// `self.registry_backend`.
fn persist_entry(
    backend: Option<&dyn RegistryBackend>,
    registries: &HashMap<String, registry::DocumentRegistry>,
    space_id: &str,
    document_id: &str,
) {
    let Some(backend) = backend else { return };
    let Some(reg) = registries.get(space_id) else {
        return;
    };
    let Some(entry) = reg.get(document_id) else {
        return;
    };
    if let Err(e) = backend.save_entry(space_id, entry) {
        error!(error = %e, doc_id = document_id, "failed to persist registry entry");
    }
}

/// Extract registry metadata from a proto `ChangeMetadata`, or default.
fn extract_meta(proto: Option<&sync::ChangeMetadata>) -> registry::EntryMetadata {
    proto.map_or_else(
        || registry::EntryMetadata {
            author_did: String::new(),
            timestamp: 0,
            account_id: None,
        },
        |m| registry::EntryMetadata {
            author_did: m.author_did.clone(),
            timestamp: m.timestamp,
            account_id: None,
        },
    )
}

/// Load all persisted registries from a backend.
fn load_from_backend(
    backend: Option<&dyn RegistryBackend>,
) -> HashMap<String, registry::DocumentRegistry> {
    let Some(backend) = backend else {
        return HashMap::new();
    };
    let spaces = match backend.load_spaces() {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to load space list from registry backend");
            return HashMap::new();
        }
    };
    let mut registries = HashMap::new();
    for space_id in spaces {
        match backend.load_all(&space_id) {
            Ok(entries) => {
                let count = entries.len();
                let reg = registry::DocumentRegistry::from_entries(entries);
                info!(space_id, entries = count, "loaded registry");
                registries.insert(space_id, reg);
            }
            Err(e) => {
                error!(error = %e, space_id, "failed to load registry, skipping space");
            }
        }
    }
    registries
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DEFAULT_SNIPPET_DEBOUNCE_MS, DEFAULT_SNIPPET_MAX_DOC_CHARS, RelayConfig};

    fn test_config() -> RelayConfig {
        RelayConfig {
            host: "127.0.0.1".into(),
            port: 0,
            relay_name: "test-relay".into(),
            require_auth: false,
            database_url: None,
            outbound_capacity: 16,
            data_dir: None,
            external_url: None,
            ux_url: None,
            authorized_keys_file: None,

            snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
            snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
        }
    }

    #[tokio::test]
    async fn test_dirty_flag_set_on_text_mutation() {
        let mut relay = Relay::new_standalone(test_config());

        // Connect + handshake.
        let conn_id = 1;
        let (data_tx, _data_rx) = mpsc::channel(16);
        let (ctrl_tx, mut ctrl_rx) = mpsc::channel(16);
        relay
            .process_command(RelayCommand::Connect {
                conn_id,
                tx: data_tx,
                ctrl_tx,
            })
            .await;
        relay
            .process_command(RelayCommand::Handshake {
                conn_id,
                msg: sync::Handshake {
                    client_name: "test-client".into(),
                    ..Default::default()
                },
            })
            .await;
        // Drain handshake ack.
        let _ = ctrl_rx.recv().await;

        // Subscribe to a document.
        relay
            .process_command(RelayCommand::Subscribe {
                conn_id,
                msg: sync::Subscribe {
                    space_id: "space".into(),
                    document_id: "doc".into(),
                },
            })
            .await;

        // Should not be dirty yet (no mutations).
        assert!(!relay.is_document_dirty("space", "doc"));

        // Create valid CRDT ops using a local document.
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(
            agent,
            "test",
            "insert",
            kutl_core::Boundary::Explicit,
            |ctx| ctx.insert(0, "hello"),
        )
        .unwrap();
        let ops = doc.encode_since(&[]);
        let metadata = doc.changes_since(&[]);

        // Send ops to the relay.
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(sync::SyncOps {
                    space_id: "space".into(),
                    document_id: "doc".into(),
                    ops,
                    metadata,
                    ..Default::default()
                }),
            })
            .await;

        // Document should be dirty after mutation.
        assert!(relay.is_document_dirty("space", "doc"));
    }

    #[tokio::test]
    async fn test_dirty_flag_set_on_blob_mutation() {
        let mut relay = Relay::new_standalone(test_config());

        // Connect + handshake.
        let conn_id = 1;
        let (data_tx, _data_rx) = mpsc::channel(16);
        let (ctrl_tx, mut ctrl_rx) = mpsc::channel(16);
        relay
            .process_command(RelayCommand::Connect {
                conn_id,
                tx: data_tx,
                ctrl_tx,
            })
            .await;
        relay
            .process_command(RelayCommand::Handshake {
                conn_id,
                msg: sync::Handshake {
                    client_name: "test-client".into(),
                    ..Default::default()
                },
            })
            .await;
        let _ = ctrl_rx.recv().await;

        // Subscribe.
        relay
            .process_command(RelayCommand::Subscribe {
                conn_id,
                msg: sync::Subscribe {
                    space_id: "space".into(),
                    document_id: "blob-doc".into(),
                },
            })
            .await;

        assert!(!relay.is_document_dirty("space", "blob-doc"));

        // Send blob ops (content_mode = BLOB_INLINE).
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(sync::SyncOps {
                    space_id: "space".into(),
                    document_id: "blob-doc".into(),
                    ops: b"binary-content".to_vec(),
                    content_hash: b"hash123".to_vec(),
                    content_mode: i32::from(sync::ContentMode::Blob),
                    blob_size: 14,
                    metadata: vec![sync::ChangeMetadata {
                        timestamp: 1000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            })
            .await;

        // Document should be dirty after blob store.
        assert!(relay.is_document_dirty("space", "blob-doc"));
    }

    /// Connect a client, complete handshake, and drain the handshake ack.
    ///
    /// Returns `(data_rx, ctrl_rx)` so the caller can inspect outbound
    /// messages if needed.
    async fn connect_client(
        relay: &mut Relay,
        conn_id: ConnId,
    ) -> (mpsc::Receiver<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
        let (data_tx, data_rx) = mpsc::channel(16);
        let (ctrl_tx, mut ctrl_rx) = mpsc::channel(16);
        relay
            .process_command(RelayCommand::Connect {
                conn_id,
                tx: data_tx,
                ctrl_tx,
            })
            .await;
        relay
            .process_command(RelayCommand::Handshake {
                conn_id,
                msg: sync::Handshake {
                    client_name: "test-client".into(),
                    ..Default::default()
                },
            })
            .await;
        // Drain handshake ack.
        let _ = ctrl_rx.recv().await;
        (data_rx, ctrl_rx)
    }

    /// Subscribe the given connection to a document.
    async fn subscribe_doc(relay: &mut Relay, conn_id: ConnId, space: &str, doc: &str) {
        relay
            .process_command(RelayCommand::Subscribe {
                conn_id,
                msg: sync::Subscribe {
                    space_id: space.into(),
                    document_id: doc.into(),
                },
            })
            .await;
    }

    /// Create valid CRDT ops by inserting text at position 0 and return
    /// `(ops, metadata)` suitable for `InboundSyncOps`.
    fn make_text_ops(text: &str) -> (Vec<u8>, Vec<sync::ChangeMetadata>) {
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(agent, "test", "insert", Boundary::Explicit, |ctx| {
            ctx.insert(0, text)
        })
        .unwrap();
        (doc.encode_since(&[]), doc.changes_since(&[]))
    }

    /// Send text ops for the given space/doc through the relay.
    async fn send_text_ops(relay: &mut Relay, conn_id: ConnId, space: &str, doc: &str, text: &str) {
        let (ops, metadata) = make_text_ops(text);
        relay
            .process_command(RelayCommand::InboundSyncOps {
                conn_id,
                msg: Box::new(sync::SyncOps {
                    space_id: space.into(),
                    document_id: doc.into(),
                    ops,
                    metadata,
                    ..Default::default()
                }),
            })
            .await;
    }

    /// Send `FlushDirty` and collect all entries from the reply channel.
    async fn flush_and_collect(relay: &mut Relay) -> Vec<FlushEntry> {
        let (entry_tx, mut entry_rx) = mpsc::channel::<FlushEntry>(16);
        relay
            .process_command(RelayCommand::FlushDirty { reply: entry_tx })
            .await;
        let mut entries = Vec::new();
        while let Ok(entry) = entry_rx.try_recv() {
            entries.push(entry);
        }
        entries
    }

    /// Simulate a complete flush cycle: collect dirty entries, then
    /// confirm each as persisted (`FlushCompleted`). Returns the entries.
    async fn flush_and_confirm(relay: &mut Relay) -> Vec<FlushEntry> {
        let entries = flush_and_collect(relay).await;
        for entry in &entries {
            relay
                .process_command(RelayCommand::FlushCompleted {
                    space_id: entry.space_id.clone(),
                    doc_id: entry.doc_id.clone(),
                    flushed_up_to: entry.flushed_up_to,
                })
                .await;
        }
        entries
    }

    #[tokio::test]
    async fn test_flush_dirty_produces_entries() {
        let mut relay = Relay::new_standalone(test_config());
        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;
        send_text_ops(&mut relay, conn_id, "space", "doc", "hello").await;

        let entries = flush_and_collect(&mut relay).await;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].space_id, "space");
        assert_eq!(entries[0].doc_id, "doc");
        match &entries[0].content {
            FlushContent::Text(bytes) => assert!(!bytes.is_empty()),
            FlushContent::Blob(_) => panic!("expected text content, got blob"),
        }
    }

    #[tokio::test]
    async fn test_flush_clean_documents_skipped() {
        let mut relay = Relay::new_standalone(test_config());
        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;
        // No ops sent — document is clean.

        let entries = flush_and_collect(&mut relay).await;

        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_flush_clears_dirty_flag() {
        let mut relay = Relay::new_standalone(test_config());
        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;
        send_text_ops(&mut relay, conn_id, "space", "doc", "hello").await;

        assert!(relay.is_document_dirty("space", "doc"));

        // Flush produces one entry but does NOT clear dirty until
        // FlushCompleted confirms persistence.
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 1);
        assert!(
            relay.is_document_dirty("space", "doc"),
            "dirty flag must stay set until FlushCompleted confirms persist"
        );

        // Simulate persistence confirmation.
        for entry in &entries {
            relay
                .process_command(RelayCommand::FlushCompleted {
                    space_id: entry.space_id.clone(),
                    doc_id: entry.doc_id.clone(),
                    flushed_up_to: entry.flushed_up_to,
                })
                .await;
        }
        assert!(
            !relay.is_document_dirty("space", "doc"),
            "dirty flag must clear after FlushCompleted"
        );

        // Second flush should produce zero entries — already clean.
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_flush_without_confirm_stays_dirty_for_retry() {
        let mut relay = Relay::new_standalone(test_config());
        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;
        send_text_ops(&mut relay, conn_id, "space", "doc", "hello").await;

        // First flush: collect entries but do NOT confirm (simulates
        // a backend save failure where the flush task never sends
        // FlushCompleted).
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 1);
        assert!(relay.is_document_dirty("space", "doc"));

        // Second flush: the document should appear again because it's
        // still dirty — the failed first flush is automatically retried.
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 1, "dirty doc must be re-sent on retry");
    }

    #[tokio::test]
    async fn test_flush_multiple_dirty_docs() {
        let mut relay = Relay::new_standalone(test_config());
        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc-a").await;
        subscribe_doc(&mut relay, conn_id, "space", "doc-b").await;
        send_text_ops(&mut relay, conn_id, "space", "doc-a", "alpha").await;
        send_text_ops(&mut relay, conn_id, "space", "doc-b", "beta").await;

        let entries = flush_and_collect(&mut relay).await;

        assert_eq!(entries.len(), 2);
        let doc_ids: HashSet<&str> = entries.iter().map(|e| e.doc_id.as_str()).collect();
        assert!(doc_ids.contains("doc-a"));
        assert!(doc_ids.contains("doc-b"));
    }

    #[tokio::test]
    async fn test_flush_rapid_mutations_single_entry() {
        /// Number of rapid mutations to send before flushing.
        const RAPID_MUTATION_COUNT: usize = 5;

        let mut relay = Relay::new_standalone(test_config());
        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;

        // Send multiple ops to the same document without flushing in between.
        for i in 0..RAPID_MUTATION_COUNT {
            send_text_ops(&mut relay, conn_id, "space", "doc", &format!("edit-{i}")).await;
        }

        // Flush should produce exactly one entry (not 5).
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].doc_id, "doc");
    }

    // ---- InMemoryContentBackend for backend-load tests ----

    use std::sync::Mutex;

    struct InMemoryContentBackend {
        store: Mutex<HashMap<(String, String), Vec<u8>>>,
    }

    impl InMemoryContentBackend {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::content_backend::ContentBackend for InMemoryContentBackend {
        async fn load(&self, space_id: &str, doc_id: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(self
                .store
                .lock()
                .unwrap()
                .get(&(space_id.to_owned(), doc_id.to_owned()))
                .cloned())
        }

        async fn save(&self, space_id: &str, doc_id: &str, data: &[u8]) -> anyhow::Result<()> {
            self.store
                .lock()
                .unwrap()
                .insert((space_id.to_owned(), doc_id.to_owned()), data.to_vec());
            Ok(())
        }

        async fn delete(&self, space_id: &str, doc_id: &str) -> anyhow::Result<()> {
            self.store
                .lock()
                .unwrap()
                .remove(&(space_id.to_owned(), doc_id.to_owned()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_subscribe_loads_from_content_backend() {
        // Pre-populate backend with a document containing "persisted content".
        let backend = InMemoryContentBackend::new();
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(agent, "test", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "persisted content")
        })
        .unwrap();
        let encoded = doc.encode_full();
        backend.save("space", "doc", &encoded).await.unwrap();

        // Create relay with content backend.
        let mut relay = Relay::new_standalone_with_backend(
            test_config(),
            None,
            Some(Arc::new(backend)),
            None,
            None,
        );

        // Connect + subscribe.
        let conn_id = 1;
        let (mut data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;

        // Should receive catch-up data (non-empty ops from the persisted document).
        let bytes = data_rx.try_recv().expect("should receive catch-up");
        assert!(
            !bytes.is_empty(),
            "catch-up should contain persisted content"
        );
    }

    #[tokio::test]
    async fn test_subscribe_without_backend_creates_empty() {
        // No content backend — document should start empty.
        let mut relay = Relay::new_standalone(test_config());

        let conn_id = 1;
        let (mut data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;

        // Should receive an envelope (empty catch-up).
        let bytes = data_rx.try_recv().expect("should receive empty catch-up");
        assert!(
            !bytes.is_empty(),
            "envelope should be present even for empty doc"
        );

        // Decode the envelope to verify the ops are empty.
        let envelope =
            kutl_proto::protocol::decode_envelope(&bytes).expect("should decode as SyncEnvelope");
        match &envelope.payload {
            Some(sync::sync_envelope::Payload::SyncOps(ops)) => {
                assert!(ops.ops.is_empty(), "ops should be empty for new document");
            }
            other => panic!("expected SyncOps payload, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_subscribe_backend_load_not_dirty() {
        // Documents loaded from backend should NOT be marked dirty —
        // they are already persisted.
        let backend = InMemoryContentBackend::new();
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(agent, "test", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();
        backend
            .save("space", "doc", &doc.encode_full())
            .await
            .unwrap();

        let mut relay = Relay::new_standalone_with_backend(
            test_config(),
            None,
            Some(Arc::new(backend)),
            None,
            None,
        );

        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;

        // Loaded from backend — should not be dirty.
        assert!(!relay.is_document_dirty("space", "doc"));
    }

    // ---- Eviction tests ----

    #[tokio::test]
    async fn test_evict_removes_document() {
        let backend = InMemoryContentBackend::new();
        let mut relay = Relay::new_standalone_with_backend(
            test_config(),
            None,
            Some(Arc::new(backend)),
            None,
            None,
        );

        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;
        send_text_ops(&mut relay, conn_id, "space", "doc", "hello").await;

        // Flush + confirm to clear the dirty flag (eviction requires !dirty).
        let _ = flush_and_confirm(&mut relay).await;
        assert!(!relay.is_document_dirty("space", "doc"));

        // Unsubscribe so the document has no subscribers.
        relay
            .process_command(RelayCommand::Unsubscribe {
                conn_id,
                msg: sync::Unsubscribe {
                    space_id: "space".into(),
                    document_id: "doc".into(),
                },
            })
            .await;

        assert!(relay.has_document("space", "doc"));

        // Backdate eligible_since so the grace period has elapsed.
        let key = DocKey {
            space_id: "space".into(),
            document_id: "doc".into(),
        };
        relay
            .documents
            .get_mut(&key)
            .expect("document should exist")
            .eviction
            .eligible_since = 1; // far in the past

        // Simulate DeriveEffects firing after grace period.
        relay
            .process_command(RelayCommand::DeriveEffects {
                space_id: "space".into(),
                document_id: "doc".into(),
            })
            .await;

        // Document should be evicted from memory.
        assert!(!relay.has_document("space", "doc"));
    }

    #[tokio::test]
    async fn test_evict_skipped_when_subscribers_present() {
        let backend = InMemoryContentBackend::new();
        let mut relay = Relay::new_standalone_with_backend(
            test_config(),
            None,
            Some(Arc::new(backend)),
            None,
            None,
        );

        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;
        send_text_ops(&mut relay, conn_id, "space", "doc", "hello").await;

        // Flush + confirm to clear dirty flag.
        let _ = flush_and_confirm(&mut relay).await;

        // Still subscribed — send DeriveEffects anyway.
        relay
            .process_command(RelayCommand::DeriveEffects {
                space_id: "space".into(),
                document_id: "doc".into(),
            })
            .await;

        // Document should still be in memory (subscriber present).
        assert!(relay.has_document("space", "doc"));
    }

    #[tokio::test]
    async fn test_evict_skipped_when_dirty() {
        let backend = InMemoryContentBackend::new();
        let mut relay = Relay::new_standalone_with_backend(
            test_config(),
            None,
            Some(Arc::new(backend)),
            None,
            None,
        );

        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;
        send_text_ops(&mut relay, conn_id, "space", "doc", "hello").await;

        // Do NOT flush — document is dirty.
        assert!(relay.is_document_dirty("space", "doc"));

        // Unsubscribe so no subscribers remain.
        relay
            .process_command(RelayCommand::Unsubscribe {
                conn_id,
                msg: sync::Unsubscribe {
                    space_id: "space".into(),
                    document_id: "doc".into(),
                },
            })
            .await;

        // Backdate eligible_since so the grace period has elapsed.
        let key = DocKey {
            space_id: "space".into(),
            document_id: "doc".into(),
        };
        relay
            .documents
            .get_mut(&key)
            .expect("document should exist")
            .eviction
            .eligible_since = 1; // far in the past

        // Send DeriveEffects while dirty.
        relay
            .process_command(RelayCommand::DeriveEffects {
                space_id: "space".into(),
                document_id: "doc".into(),
            })
            .await;

        // Document should still be in memory (dirty prevents eviction).
        assert!(relay.has_document("space", "doc"));
    }

    #[tokio::test]
    async fn test_no_eviction_without_backend() {
        // No content backend — eviction should never be triggered.
        let mut relay = Relay::new_standalone(test_config());

        let conn_id = 1;
        let (_data_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;
        send_text_ops(&mut relay, conn_id, "space", "doc", "hello").await;

        // Flush + confirm to clear dirty.
        let _ = flush_and_confirm(&mut relay).await;

        // Unsubscribe.
        relay
            .process_command(RelayCommand::Unsubscribe {
                conn_id,
                msg: sync::Unsubscribe {
                    space_id: "space".into(),
                    document_id: "doc".into(),
                },
            })
            .await;

        // Without a content backend, derive_effects should be a no-op.
        // The document should still exist — eviction state should not be
        // marked eligible (content_backend is None, so data would be lost).
        let key = DocKey {
            space_id: "space".into(),
            document_id: "doc".into(),
        };
        let slot = relay.documents.get(&key).expect("document should exist");
        assert_eq!(
            slot.eviction.eligible_since, 0,
            "eviction should not be marked eligible without content backend"
        );
    }
}
