//! Relay actor — owns all mutable state and processes commands from connections.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use kutl_proto::sync::{self, ErrorCode};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::auth::{AuthError, AuthStore, DeviceTokenResponse};
use crate::authorized_keys::AuthorizedKeys;
use crate::blob_backend::BlobBackend;
use crate::config::RelayConfig;
use crate::content_backend::ContentBackend;
use crate::mcp_tools;
use crate::observer::{
    AfterMergeObserver, BeforeMergeObserver, EditContentMode, MergedEvent, NoopAfterMergeObserver,
    NoopBeforeMergeObserver, RelayObserver,
};
use crate::protocol::{encode_envelope, wrap_error};
use crate::registry;
use crate::registry_store::RegistryBackend;

mod auth_handlers;
mod doc_load;
mod lifecycle;
mod mcp;
mod space_ops;
mod sync_ops;

use mcp::{MCP_SESSION_REAP_INTERVAL, McpSession};

// Bound (privately) so relay.rs residue and the sibling child modules keep
// reaching the doc-load family by its pre-split paths (`DocKey`,
// `super::DocSlot`, …) after the move into the `doc_load` child.
use doc_load::{
    BlobData, DocContent, DocKey, DocSlot, EditedAtPending, LoadResult, PendingEdit,
    load_from_backend, relay_and_evict,
};

// Bound (privately) so the sibling mcp child keeps reaching the moved
// registry-lifecycle persist helper by its pre-split `super::persist_entry`
// path after the move into the `lifecycle` child.
use lifecycle::persist_entry;

// Bound (privately) so the sibling child modules keep reaching the moved
// sync-ops items by their pre-split `super::` paths (doc_load.rs uses
// `super::BLOB_KNOWN`; mcp.rs uses `super::build_and_relay_text_outbound`
// and `super::compute_blob_hash`) after the move into the `sync_ops` child.
use sync_ops::{BLOB_KNOWN, build_and_relay_text_outbound, compute_blob_hash};

// Re-exported so the background flush task keeps calling
// `crate::relay::apply_delta_with_retry` by its pre-split qualified path
// (flush.rs) after the move into the `sync_ops` child.
pub(crate) use sync_ops::apply_delta_with_retry;

// Re-exported so the auth HTTP routes' `RelayCommand` reply channels stay
// externally nameable and external `kutl_relay::relay::` paths keep importing
// the auth response payloads from this module after their move into the
// `auth_handlers` child.
pub use auth_handlers::{ChallengeResponse, DeviceRequestResponse, VerifyResponse};

// Re-exported so crate-internal consumers (mcp_handler.rs, text_export.rs) and
// external `kutl_relay::relay::` paths keep importing the MCP family from this
// module after its move into the `mcp` child.
pub use mcp::{
    McpCreateDocumentResult, McpDocumentContent, McpDocumentSummary, McpEditResult, McpError,
    McpLogEntry, McpParticipant, McpSessionId, McpSpaceStatus, McpSpaceSummary,
    McpUploadBlobResult,
};

/// How often the relay prunes stale change data (signals, cursors).
const CHANGE_REAP_INTERVAL: Duration =
    Duration::from_secs(kutl_core::SECONDS_PER_HOUR.unsigned_abs());

/// Unique identifier for a WebSocket connection.
pub type ConnId = u64;

/// Sentinel passed for `skip_conn` when a broadcast originates from
/// a non-WebSocket path (e.g. MCP). The relay's `ConnId` allocator
/// starts at 1, so `0` is guaranteed unreachable for any real
/// subscriber — using it as "no connection to skip" never accidentally
/// drops a real subscriber from the broadcast.
const NO_SKIP_CONN: ConnId = 0;

/// A single document's encoded content, sent from the relay actor to the flush task.
pub struct FlushEntry {
    /// Space ID.
    pub space_id: String,
    /// Document ID.
    pub doc_id: String,
    /// Content to persist.
    pub content: FlushContent,
    /// Authoritative durable size for this flush. Measured from
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
// `large_enum_variant` is size-threshold-dependent (whether it fires depends on
// the exact field sizes of the proto payloads, which shift as fields are added —
// e.g. RenameDocument's causal-floor HLC narrowed the gap): use `allow`, not
// `expect`, so the suppression neither errors when the lint is dormant nor masks
// a genuine new outlier. The decision stands regardless: RegisterDocument carries
// 5 RFD 0081 provenance fields plus 5 RFD 0042 amendment mirror fields; boxing
// every actor command for one outlier costs an allocation on every command — the
// actor channel is tx/rx-bounded, not memory-bounded.
#[allow(clippy::large_enum_variant)]
pub enum RelayCommand {
    /// A new connection has been established.
    Connect {
        conn_id: ConnId,
        tx: mpsc::Sender<Vec<u8>>,
        ctrl_tx: mpsc::Sender<Vec<u8>>,
        /// Unbounded own-ack lane — direct responses to this connection's own
        /// commands (handshake/register/rename/unregister acks, errors, query
        /// results) ride this lane so they are never dropped.
        ack_tx: mpsc::UnboundedSender<Vec<u8>>,
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
    /// Validate a bearer token. Returns `(identity, pat_context)`.
    /// The PAT context (when present) carries both the hash for scope
    /// checks and the `pat_id` UUID for per-PAT signal attribution.
    McpValidateToken {
        token: String,
        reply: oneshot::Sender<Result<(String, Option<crate::auth::PatAuthContext>), AuthError>>,
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
        pat: Option<crate::auth::PatAuthContext>,
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
    /// Create a brand-new text document at a path with optional
    /// provenance metadata. Differs from `McpEditDocument` (which now
    /// errors on unknown ids) — this is the explicit create entry point.
    McpCreateDocument {
        session_id: McpSessionId,
        space_id: String,
        path: String,
        content: String,
        provenance: mcp_tools::ProvenanceArgs,
        reply: oneshot::Sender<Result<McpCreateDocumentResult, McpError>>,
    },
    /// Create or replace a binary blob document at a path with optional
    /// provenance metadata.
    McpUploadBlob {
        session_id: McpSessionId,
        space_id: String,
        path: String,
        content_type: String,
        bytes: Vec<u8>,
        provenance: mcp_tools::ProvenanceArgs,
        /// Cap from `AppState.max_blob_bytes`; the actor only sees the
        /// cap through the command so the configuration lives in one
        /// place.
        max_bytes: usize,
        reply: oneshot::Sender<Result<McpUploadBlobResult, McpError>>,
    },
    /// List spaces the calling DID is authorised for.
    McpListSpaces {
        session_id: McpSessionId,
        reply: oneshot::Sender<Result<Vec<McpSpaceSummary>, McpError>>,
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
        /// Caller-supplied signal UUID for `FLAG_KIND_COMMENT` (RFD 0077
        /// marker↔signal binding). `None` for non-comment kinds; the
        /// relay mints a fresh UUID instead.
        signal_id: Option<String>,
        /// Comment-kind posterity snapshot (RFD 0077). `None` for other kinds.
        anchor_text: Option<String>,
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
    /// Bulk soft-delete every active document in a space (RFD 0042
    /// amendment 2026-05-24 — B-full follow-up). Used by the UX
    /// server's `DeleteSpace` handler instead of N per-doc round-trips.
    UnregisterSpaceOp {
        conn_id: ConnId,
        msg: sync::UnregisterSpace,
    },
    /// Transfer ownership (registry `account_id` + mirror
    /// `created_by`/`updated_by`) of every document in a space to a
    /// new account. Used by the ingestion accept-time re-attribution
    /// path.
    TransferSpaceOwnershipOp {
        conn_id: ConnId,
        msg: sync::TransferSpaceOwnership,
    },
    // ---- Space ops commands ----
    /// Accept an invitation code.
    JoinSpaceOp {
        conn_id: ConnId,
        msg: sync::JoinSpace,
    },
    /// Resolve a space by owner/slug.
    ResolveSpaceOp {
        conn_id: ConnId,
        msg: sync::ResolveSpace,
    },
    /// List spaces the caller is a member of.
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

// ---------------------------------------------------------------------------
// MCP types
// ---------------------------------------------------------------------------

/// Identity of an MCP caller, resolved by [`authorize_mcp_caller`].
///
/// `did` is the underlying author identity — `did:key:...` when the
/// account has a custodied DID, or `account:<account_id>` synthesized
/// fallback for web-only accounts. `via_pat_id` is the `api_tokens.id`
/// of the PAT used to authenticate, when PAT-authenticated; `None` for
/// DID challenge-response or session-token auth. Per RFD 0044 the
/// `via_pat_id` is what lets the activity feed distinguish multiple
/// PATs belonging to the same account ("alice's coding-agent" vs
/// "alice's doc-writer").
#[derive(Debug, Clone)]
pub(crate) struct AuthorIdentity {
    pub(crate) did: String,
    pub(crate) via_pat_id: Option<String>,
}

/// Data and control channels for a single connection.
struct ConnChannels {
    /// Channel for data messages (sync ops relay and catch-up).
    data: mpsc::Sender<Vec<u8>>,
    /// Bounded channel for control broadcasts to *other* subscribers (lifecycle
    /// broadcasts, displacement corrections, signal fan-out, stale notices).
    /// Separate from data so admin messages are delivered even when the data
    /// channel is full. On full, the subscriber is evicted (not dropped in
    /// place) — broadcasts are recoverable via re-sync on reconnect.
    ctrl: mpsc::Sender<Vec<u8>>,
    /// Unbounded own-ack lane for direct responses to *this* connection's own
    /// commands (handshake/register/rename/unregister acks, errors, query
    /// results). Unbounded so an ack is never dropped — a lost lifecycle ack
    /// strands a doc at `confirmed:false` forever (Mode 2). Volume is bounded
    /// by upstream backpressure, so the lane cannot grow without bound.
    ack: mpsc::UnboundedSender<Vec<u8>>,
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
    /// Maps connection ID → (identity, PAT context). The PAT context is
    /// `Some` when the connection was authenticated via PAT — its
    /// `pat_hash` enables `authorize_space` per-token scoping, and its
    /// `pat_id` lets signal-write paths record per-PAT attribution
    /// (RFD 0044).
    authenticated: HashMap<ConnId, (String, Option<crate::auth::PatAuthContext>)>,
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
    /// Per-tenant storage quota backend. `None` without quota enforcement;
    /// DB-backed deployments inject a backend to enforce tier-based limits.
    /// Consumed by `handle_blob_sync_ops` / `handle_text_sync_ops` as the
    /// inbound pre-check, and by the flush task for post-write
    /// reconciliation.
    quota_backend: Option<Arc<dyn crate::quota_backend::QuotaBackend>>,
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
    /// the database when one is configured. When `db` is `None`, the async
    /// calls resolve immediately without yielding.
    #[expect(
        clippy::too_many_lines,
        reason = "flat command-dispatch match; each arm delegates to a handler, so splitting would only hide the dispatch"
    )]
    pub async fn process_command(&mut self, cmd: RelayCommand) {
        match cmd {
            RelayCommand::Connect {
                conn_id,
                tx,
                ctrl_tx,
                ack_tx,
            } => self.handle_connect(conn_id, tx, ctrl_tx, ack_tx),
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
            RelayCommand::McpCreateSession { did, pat, reply } => {
                let session_id = self.handle_mcp_create_session(&did, pat);
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
            RelayCommand::McpCreateDocument {
                session_id,
                space_id,
                path,
                content,
                provenance,
                reply,
            } => {
                let result = self
                    .handle_mcp_create_document(&session_id, &space_id, &path, &content, provenance)
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpUploadBlob {
                session_id,
                space_id,
                path,
                content_type,
                bytes,
                provenance,
                max_bytes,
                reply,
            } => {
                let result = self
                    .handle_mcp_upload_blob(
                        &session_id,
                        &space_id,
                        &path,
                        &content_type,
                        bytes,
                        provenance,
                        max_bytes,
                    )
                    .await;
                let _ = reply.send(result);
            }
            RelayCommand::McpListSpaces { session_id, reply } => {
                let result = self.handle_mcp_list_spaces(&session_id).await;
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
                signal_id,
                anchor_text,
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
                        signal_id.as_deref(),
                        anchor_text.as_deref(),
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
            RelayCommand::UnregisterSpaceOp { conn_id, msg } => {
                self.handle_unregister_space(conn_id, &msg).await;
            }
            RelayCommand::TransferSpaceOwnershipOp { conn_id, msg } => {
                self.handle_transfer_space_ownership(conn_id, &msg).await;
            }
            // ---- Space ops commands ----
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
        ack_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) {
        info!(conn_id, "connection registered");
        self.connections.insert(
            conn_id,
            ConnChannels {
                data: tx,
                ctrl: ctrl_tx,
                ack: ack_tx,
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
        // Drop the conn from every space's watcher set, and remove any space
        // key whose set becomes empty. Without this, `space_watchers` grows
        // unbounded over process lifetime — one residual key per space ever
        // watched, never reclaimed once the last watcher disconnects.
        self.space_watchers.retain(|_, watchers| {
            watchers.remove(&conn_id);
            !watchers.is_empty()
        });
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
        if let Some(pending) = slot.edited_at_pending.take()
            && let Some(ref backend) = self.registry_backend
            && let Err(e) = backend.update_edited_at(
                &key.space_id,
                &key.document_id,
                pending.timestamp,
                Some(&pending.author_did),
            )
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

    /// Test-only inspection: does the in-memory registry contain an
    /// active (non-soft-deleted) entry for the given key?
    ///
    /// Used by the RFD 0042 amendment 2026-05-24 rollback tests to
    /// assert that a `save_entry` failure leaves no in-memory residue
    /// (the in-memory insert must be unwound when persistence fails,
    /// otherwise the registry diverges from the mirror).
    #[doc(hidden)]
    pub fn test_registry_has_active_entry(&self, space_id: &str, document_id: &str) -> bool {
        self.registries
            .get(space_id)
            .is_some_and(|reg| reg.get(document_id).is_some())
    }

    /// Test-only inspection: does the registry have any entry (active
    /// or soft-deleted) for the key? Used by the unregister-rollback
    /// test where the prior-state entry should remain active after a
    /// rolled-back unregister.
    #[doc(hidden)]
    pub fn test_registry_entry_is_soft_deleted(
        &self,
        space_id: &str,
        document_id: &str,
    ) -> Option<bool> {
        self.registries
            .get(space_id)
            .and_then(|reg| reg.get_any(document_id))
            .map(|e| e.deleted_at.is_some())
    }

    /// Test-only inspection: the in-memory entry's current path.
    /// Used by the rename-rollback test to confirm the rename was
    /// unwound after the persistence step failed.
    #[doc(hidden)]
    pub fn test_registry_entry_path(&self, space_id: &str, document_id: &str) -> Option<String> {
        self.registries
            .get(space_id)
            .and_then(|reg| reg.get_any(document_id))
            .map(|e| e.path.clone())
    }

    /// Inject a `MembershipBackend` after the relay is constructed.
    ///
    /// Exposed so integration tests (notably the RFD 0042 amendment
    /// 2026-05-24 ack-envelope tests) can wire up account resolution
    /// onto a standalone relay without exposing every backend slot via
    /// the constructor. Production code uses [`Relay::new`].
    #[doc(hidden)]
    pub fn test_set_membership_backend(
        &mut self,
        backend: Arc<dyn crate::membership_backend::MembershipBackend>,
    ) {
        self.membership_backend = Some(backend);
    }

    /// Send a frame to a connection on its unbounded own-ack lane.
    ///
    /// For frames that are a *direct response to this connection's own command*
    /// (handshake/register/rename/unregister acks, errors, query results). The
    /// lane is unbounded, so this never blocks and never drops on a full
    /// channel — `send` only errors when the receiver is gone (the socket is
    /// dead), in which case the frame is harmlessly discarded. Stays `&self`.
    fn send_ack(&self, conn_id: ConnId, bytes: &[u8]) {
        if let Some(channels) = self.connections.get(&conn_id) {
            let _ = channels.ack.send(bytes.to_vec());
        }
    }

    /// Send a broadcast frame to *another* subscriber on its bounded ctrl lane.
    ///
    /// For lifecycle broadcasts, displacement corrections, and signal fan-out —
    /// frames destined for connections OTHER than the one whose command is being
    /// handled. The ctrl lane is bounded; on a full or closed lane the
    /// subscriber is EVICTED (mirroring the data-lane `relay_and_evict`) rather
    /// than dropping the frame in place. The evicted subscriber re-syncs via
    /// `ListSpaceDocuments` on reconnect, so eviction is recoverable and keeps
    /// the lane provably bounded.
    fn send_broadcast(&mut self, conn_id: ConnId, bytes: &[u8]) {
        let Some(channels) = self.connections.get(&conn_id) else {
            return;
        };
        if let Err(e) = channels.ctrl.try_send(bytes.to_vec()) {
            debug!(conn_id, error = %e, "evicting subscriber: broadcast ctrl lane unavailable");
            self.handle_disconnect(conn_id);
        }
    }

    /// Send an error frame to a connection on its own-ack lane.
    fn send_error(&self, conn_id: ConnId, code: ErrorCode, message: &str) {
        let frame = encode_envelope(&wrap_error(code, message));
        self.send_ack(conn_id, &frame);
    }

    /// Send a `SyncEnvelope` payload to a connection on its own-ack lane.
    fn send_payload(&self, conn_id: ConnId, payload: sync::sync_envelope::Payload) {
        let envelope = sync::SyncEnvelope {
            payload: Some(payload),
        };
        self.send_ack(conn_id, &encode_envelope(&envelope));
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

// ---- Shared test fixtures (used by both `tests` below and `mcp::tests`) ----

#[cfg(test)]
use kutl_core::Boundary;

#[cfg(test)]
fn test_config() -> RelayConfig {
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        outbound_capacity: 16,

        ..Default::default()
    }
}

/// Read the resident blob bytes for a document, or `None` if the slot
/// is absent / not a blob.
#[cfg(test)]
fn resident_blob(relay: &Relay, space: &str, doc: &str) -> Option<Vec<u8>> {
    let key = DocKey {
        space_id: space.into(),
        document_id: doc.into(),
    };
    match &relay.documents.get(&key)?.content {
        DocContent::Blob(b) => Some(b.content.clone()),
        _ => None,
    }
}

/// Connect a client, complete handshake, and drain the handshake ack.
///
/// Returns `(data_rx, ack_rx, ctrl_rx)` so the caller can inspect outbound
/// messages if needed. Own-acks (handshake/register/rename/unregister acks,
/// errors, query results) arrive on `ack_rx`; broadcasts to other
/// subscribers (lifecycle/displacement/signals/stale notices) on `ctrl_rx`.
#[cfg(test)]
async fn connect_client(
    relay: &mut Relay,
    conn_id: ConnId,
) -> (
    mpsc::Receiver<Vec<u8>>,
    mpsc::UnboundedReceiver<Vec<u8>>,
    mpsc::Receiver<Vec<u8>>,
) {
    let (data_tx, data_rx) = mpsc::channel(16);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(16);
    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();
    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx: data_tx,
            ctrl_tx,
            ack_tx,
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
    // Drain handshake ack (own-ack lane).
    let _ = ack_rx.recv().await;
    (data_rx, ack_rx, ctrl_rx)
}

/// Subscribe the given connection to a document.
#[cfg(test)]
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
#[cfg(test)]
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
#[cfg(test)]
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
#[cfg(test)]
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
#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_flush_dirty_produces_entries() {
        let mut relay = Relay::new_standalone(test_config());
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
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
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(&mut relay, conn_id, "space", "doc").await;
        // No ops sent — document is clean.

        let entries = flush_and_collect(&mut relay).await;

        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_flush_clears_dirty_flag() {
        let mut relay = Relay::new_standalone(test_config());
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
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
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
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
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
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
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
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

    // ---- empty space-watcher sets are reclaimed on disconnect ----

    #[tokio::test]
    async fn test_disconnect_drops_empty_space_watcher_set() {
        let mut relay = Relay::new_standalone(test_config());
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;

        // Register as a space watcher via ListSpaceDocuments.
        relay
            .process_command(RelayCommand::ListSpaceDocuments {
                conn_id,
                msg: sync::ListSpaceDocuments {
                    space_id: "watched-space".into(),
                },
            })
            .await;
        assert!(
            relay.space_watchers.contains_key("watched-space"),
            "watcher set should exist after ListSpaceDocuments"
        );

        // Disconnect the only watcher — the now-empty space key must be removed.
        relay
            .process_command(RelayCommand::Disconnect { conn_id })
            .await;
        assert!(
            !relay.space_watchers.contains_key("watched-space"),
            "empty watcher set must be reclaimed on disconnect, not left as a residual key"
        );
    }

    #[tokio::test]
    async fn test_disconnect_keeps_nonempty_space_watcher_set() {
        // With two watchers, disconnecting one must keep the space key (the
        // other watcher still needs lifecycle broadcasts).
        let mut relay = Relay::new_standalone(test_config());
        let (_d1, _a1, _c1) = connect_client(&mut relay, 1).await;
        let (_d2, _a2, _c2) = connect_client(&mut relay, 2).await;

        for conn_id in [1, 2] {
            relay
                .process_command(RelayCommand::ListSpaceDocuments {
                    conn_id,
                    msg: sync::ListSpaceDocuments {
                        space_id: "watched-space".into(),
                    },
                })
                .await;
        }

        relay
            .process_command(RelayCommand::Disconnect { conn_id: 1 })
            .await;
        assert!(
            relay.space_watchers.contains_key("watched-space"),
            "watcher set with a remaining watcher must be kept"
        );
        assert_eq!(
            relay
                .space_watchers
                .get("watched-space")
                .map(std::collections::HashSet::len),
            Some(1)
        );
    }
}
