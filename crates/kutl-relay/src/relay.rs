//! Relay actor — owns all mutable state and processes commands from connections.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use kutl_proto::sync;
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
use crate::protocol::{encode_envelope, sync_ops_rejected_envelope};
use crate::registry;
use crate::registry_store::RegistryBackend;

mod auth_handlers;
/// The projection contract, as an executable test.
///
/// Inside `crate::relay` rather than at the crate root because the admission
/// seams it drives are `pub(in crate::relay)` — a suite that cannot reach them
/// can only test the backend directly, which is the layer where admission
/// defects slip through unseen.
#[cfg(feature = "projection-conformance")]
pub mod conformance;
mod doc_load;
mod lifecycle;
mod mcp;
pub mod signal_log;
mod space_ops;
mod sync_ops;

use mcp::{MCP_SESSION_REAP_INTERVAL, McpSession};

// Bound (privately) so relay.rs residue and the sibling child modules keep
// reaching the doc-load family by its pre-split paths (`DocKey`,
// `super::DocSlot`, …) after the move into the `doc_load` child.
use doc_load::{
    BlobData, DocContent, DocKey, DocSlot, EditedAtPending, PendingEdit, load_from_backend,
    relay_and_evict,
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
    McpLogEntry, McpParticipant, McpResolvedParticipant, McpSessionId, McpSpaceStatus,
    McpSpaceSummary, McpUploadBlobResult,
};

/// Whether a listening connection's signal stream is flowing.
///
/// Signals ride the shared data lane, so a signal frame can find that lane
/// full while the connection is otherwise healthy. The remedy pauses the
/// STREAM: the connection stays a listener (present in the space, receiving
/// lifecycle broadcasts on the ctrl lane) and is told on the ack lane to
/// re-subscribe, which resumes the stream from its own cursor. Pausing
/// rather than revoking membership is what keeps a full data lane from
/// silently costing a party the registrations it would otherwise miss until
/// its next session, and skipping paused streams is what keeps one overflow
/// from becoming a notice per broadcast.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SignalStream {
    /// Signal frames are delivered.
    Flowing,
    /// A signal frame found the data lane full; delivery resumes on the
    /// connection's next `SubscribeSignals`.
    Paused,
}

/// Outcome of [`Relay::try_send_signal_frame`] — the one door for signal
/// traffic onto a connection's shared data lane. Callers map each outcome
/// to the remedy fitting their traffic kind; only `Sent` put the frame on
/// the lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalSend {
    /// Frame enqueued.
    Sent,
    /// Refused without trying: the lane is into the document-headroom
    /// reserve, and signals yield rather than spend it.
    Yielded,
    /// The lane was full outright.
    LaneFull,
    /// The connection is gone; nothing to do.
    Gone,
}

/// Fraction of a connection's data lane reserved for document traffic, as a
/// divisor: one quarter.
///
/// Signals share the `data` lane with document ops and are
/// the lower-priority half of that arrangement — backfill yields to
/// control traffic instead of preempting it, applied within the lane rather
/// than between lanes. A quarter is chosen to absorb a short document burst, not
/// merely one op: reserving a single slot would let two document ops arriving
/// back-to-back evict the connection anyway, which is the same defect at a
/// smaller scale.
const DOCUMENT_RESERVE_DIVISOR: usize = 4;

/// How many slots of a data lane of `max_capacity` are held back from signals.
///
/// At least one wherever a lane HAS a slot to spare, so a small lane — the test
/// suite uses two — still reserves something; a reserve of zero there would
/// silently reinstate the defect.
///
/// **A one-slot lane reserves nothing**, and the special case is load-bearing
/// rather than tidiness. `capacity()` on an empty channel of `max_capacity` 1
/// returns 1, so a reserve of 1 makes `capacity() <= reserve` true even when the
/// lane is completely idle: signals would never be sent on such a connection at
/// all, silently, forever. A lane of one cannot both carry a signal and hold a
/// slot back — so it holds none, and the guarantee that a signal always has
/// somewhere to land simply does not hold at that size. Production is
/// [`DEFAULT_OUTBOUND_CAPACITY`] (512),
/// where the reserve is 128.
fn document_headroom(max_capacity: usize) -> usize {
    if max_capacity < 2 {
        return 0;
    }
    (max_capacity / DOCUMENT_RESERVE_DIVISOR).max(1)
}

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

/// The transition a caller may apply to a single signal: close or reopen,
/// nothing else. Every single-transition door (WS, HTTP, MCP) speaks this
/// enum rather than the full [`kutl_proto::sync::SignalEventType`], and so
/// does the shared emit path they funnel into — which is what makes an
/// announcing TOMBSTONED unrepresentable there instead of merely checked.
/// System-minted transitions (the delete/revive cascades, the orphan
/// tombstone) build their records through a separate path that declares
/// silence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalTransitionEvent {
    /// Close the signal (carries a caller reason, defaulting to `resolved`).
    Closed,
    /// Reopen a previously-closed signal (clears close-state; no reason).
    Reopened,
}

/// Why a dispatched relay command produced no reply.
///
/// `Display` yields the exact user-visible strings the handlers have always
/// sent ("relay channel closed" / "relay did not respond"); routes that
/// collapse both modes into one message ("relay unavailable") do so at the
/// call site. Every HTTP/MCP handler reaches the actor through
/// [`dispatch_relay_command`], so a relay-health probe or a metric on actor
/// death has exactly one place to hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelayDispatchError {
    /// The actor's command channel is closed — the relay task is gone.
    ChannelClosed,
    /// The actor dropped the reply sender without answering.
    NoReply,
}

impl std::fmt::Display for RelayDispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ChannelClosed => "relay channel closed",
            Self::NoReply => "relay did not respond",
        })
    }
}

/// Send a command to the relay actor and await its oneshot reply.
pub(crate) async fn dispatch_relay_command<T>(
    relay_tx: &mpsc::Sender<RelayCommand>,
    cmd_builder: impl FnOnce(oneshot::Sender<T>) -> RelayCommand,
) -> Result<T, RelayDispatchError> {
    let (tx, rx) = oneshot::channel();
    if relay_tx.send(cmd_builder(tx)).await.is_err() {
        return Err(RelayDispatchError::ChannelClosed);
    }
    rx.await.map_err(|_| RelayDispatchError::NoReply)
}

/// Commands sent from connection tasks to the relay actor.
// `large_enum_variant` is size-threshold-dependent (whether it fires depends on
// the exact field sizes of the proto payloads, which shift as fields are added —
// e.g. RenameDocument's causal-floor HLC narrowed the gap): use `allow`, not
// `expect`, so the suppression neither errors when the lint is dormant nor masks
// a genuine new outlier. The decision stands regardless: RegisterDocument carries
// 5 source-provenance fields plus 5 UX-mirror fields; boxing
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
    /// Validate that a session exists and belongs to the given DID.
    McpValidateSession {
        session_id: McpSessionId,
        expected_did: String,
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
    /// Resolve a participant name to the DIDs it names in a space.
    McpResolveParticipant {
        session_id: McpSessionId,
        space_id: String,
        name: String,
        reply: oneshot::Sender<Result<Vec<McpResolvedParticipant>, McpError>>,
    },
    /// Get space status.
    McpStatus {
        session_id: McpSessionId,
        space_id: String,
        reply: oneshot::Sender<Result<McpSpaceStatus, McpError>>,
    },
    /// Edit a document by merging the caller's delta into it.
    McpEditDocument {
        session_id: McpSessionId,
        space_id: String,
        document_id: String,
        /// Version token from the caller's read, naming the text
        /// `new_content` was composed against.
        base_version: String,
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
        /// Caller-supplied signal UUID for `FLAG_KIND_COMMENT` (the
        /// marker↔signal binding). `None` for non-comment kinds; the
        /// relay mints a fresh UUID instead.
        signal_id: Option<String>,
        /// Comment-kind posterity snapshot. `None` for other kinds.
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
    /// Fetch the space activity feed (edits + signals) for the HTTP
    /// `GET /spaces/{space_id}/changes` route. Authenticated by
    /// DID (resolved from the bearer token by the HTTP handler). Like
    /// `McpGetChanges`, this keys the cursor by the caller's DID; the two
    /// variants differ only in how that DID is resolved — `McpGetChanges`
    /// derives it from the authenticated MCP session, this variant from the
    /// HTTP bearer token. Degrades to an empty response when no change backend
    /// is configured.
    GetChanges {
        did: String,
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
    /// List a space's signals, folded from the record log.
    McpListSignals {
        session_id: McpSessionId,
        space_id: String,
        status: String,
        kind: Option<String>,
        document_id: Option<String>,
        flag_kind: Option<String>,
        reply: oneshot::Sender<Result<Vec<kutl_signals::summary::SignalSummary>, McpError>>,
    },
    McpGetSignalDetail {
        session_id: McpSessionId,
        space_id: String,
        signal_id: String,
        reply: oneshot::Sender<Result<crate::change_backend::SignalDetail, McpError>>,
    },
    /// Client sent a signal (flag payload only accepted over WebSocket).
    ///
    /// This door carries a whole record, so it can only ever
    /// serve the one kind it validates. The four `Submit*` commands below
    /// supersede it — arguments rather than a record, one per kind.
    Signal { conn_id: ConnId, msg: sync::Signal },
    /// Ordering barrier: answered on the own-ack lane once every command
    /// this connection sent ahead of it has been handled (the actor handles
    /// commands in arrival order, so reaching this one is the proof).
    Barrier { conn_id: ConnId, msg: sync::Barrier },
    /// Authored door — flag.
    SubmitFlag {
        conn_id: ConnId,
        msg: sync::SubmitFlag,
    },
    /// Authored door — comment.
    SubmitComment {
        conn_id: ConnId,
        msg: sync::SubmitComment,
    },
    /// Authored door — reply. Replies ride their own frame: the
    /// record-carrying `Signal` door above validates only flags and rejects
    /// every other payload.
    SubmitReply {
        conn_id: ConnId,
        msg: sync::SubmitReply,
    },
    /// Join a space's signal stream and receive one backfill page.
    SubscribeSignals {
        conn_id: ConnId,
        msg: sync::SubscribeSignals,
    },
    /// Authored door — close / reopen.
    SubmitTransition {
        conn_id: ConnId,
        msg: sync::SubmitTransition,
    },
    /// A peer pushing signal history it holds and this relay does not
    /// (re-seed). Distinct from the authored frames above because it carries
    /// whole RECORDS: replication legitimately moves records, authoring never
    /// does.
    SignalReseedFrame {
        conn_id: ConnId,
        msg: sync::SignalReseed,
    },
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
    /// Bulk soft-delete every active document in a space. Used by the UX
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
/// `did` is the underlying author identity, the `did:key:...` the PAT's
/// custodied key resolves to (the historical `account:<account_id>` form
/// survives only in backfilled provenance). `via_pat_id` is the `api_tokens.id`
/// of the PAT used to authenticate, when PAT-authenticated; `None` for
/// DID challenge-response or session-token auth. The
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
    /// Bounded channel for control broadcasts to *other* connections (lifecycle
    /// broadcasts to the space's listeners, displacement corrections, stale
    /// notices). Separate from data so admin messages are
    /// delivered even when the data channel is full. On full, the connection
    /// is evicted (not dropped in place) — broadcasts are recoverable via
    /// re-sync on reconnect.
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
///
/// Handlers that await take `&mut self`, never `&self`, even when they only
/// read: the actor runs as one spawned future, which must be `Send`, and an
/// `async fn` on `&self` yields a future holding a shared borrow of the whole
/// actor, documents included. The documents' engine is not `Sync` (its rope
/// buffers local edits behind a `RefCell`), so that borrow would make the
/// actor's future non-`Send`. A mutable borrow only needs `Send`.
///
/// Decisions are the exception. A decision reads what it needs from the
/// actor, consults a backend, and returns a verdict without acting on it:
/// `authorize_space`, `authorize_conn`, `resolve_account_id`,
/// `check_inbound_quota`, `signal_is_in_space`, `mcp_check_space_registered`.
/// Each keeps `&self` and returns an owned future (`impl Future + Send +
/// use<>`) built from cloned backend handles and its inputs, so the block
/// borrows nothing from the actor and the compiler still forbids it from
/// mutating the actor; the handler awaiting it holds only its own mutable
/// borrow. Two consequences. The actor state a decision reads is read when
/// the future is built, so await it before touching that state. And a
/// method that acts on the verdict (sends a rejection, mutates) is a
/// handler and takes `&mut self`, however small. Token validation stays a
/// handler: it consults the actor's own in-memory challenge store, which
/// cannot be cloned into a future.
pub struct Relay {
    config: RelayConfig,
    connections: HashMap<ConnId, ConnChannels>,
    documents: HashMap<DocKey, DocSlot>,
    rx: Option<mpsc::Receiver<RelayCommand>>,
    /// Dedicated UNBOUNDED receiver for marker-materialized record batches
    /// produced by the
    /// [`RecordMaterializingObserver`](crate::markers::materialize::RecordMaterializingObserver).
    /// Separate from `rx` so a materialization enqueue can never
    /// drop under command-channel backpressure and silently lose a
    /// user-authored signal — see the observer's module docs on no-drop.
    /// `None` in standalone/simulation mode (no materializer installed). The
    /// actor is the SOLE segment writer, so draining here (append + project +
    /// broadcast) is non-re-entrant.
    materialize_rx: Option<crate::markers::materialize::MaterializeReceiver>,
    /// Auth store for DID challenge-response. Authentication is mandatory.
    auth: AuthStore,
    /// Maps `ConnId` to authenticated DID.
    /// Maps connection ID → (identity, PAT context). The PAT context is
    /// `Some` when the connection was authenticated via PAT — its
    /// `pat_hash` enables `authorize_space` per-token scoping, and its
    /// `pat_id` lets signal-write paths record per-PAT attribution.
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
    /// Persistence backend for registries. `None` only on standalone
    /// test/sim actors and the in-memory test relay; the OSS binary (sqlite)
    /// and the hosted relay (Postgres) always have one.
    registry_backend: Option<Arc<dyn RegistryBackend>>,
    /// Space registration backend. `None` on the kutlhub host relay (spaces
    /// managed by the UX server) and standalone test/sim actors; the OSS binary
    /// always has one.
    space_backend: Option<Arc<dyn crate::space_backend::SpaceBackend>>,
    /// Persistent content storage. `None` for ephemeral (open-source) relay.
    content_backend: Option<Arc<dyn ContentBackend>>,
    /// Persistent blob storage. `None` for ephemeral (open-source) relay.
    blob_backend: Option<Arc<dyn BlobBackend>>,
    /// Clone of the command channel sender, for self-messaging (eviction timers).
    /// `None` in standalone (simulation/test) mode.
    self_tx: Option<mpsc::Sender<RelayCommand>>,
    /// File-based DID authorization list (OSS relay mode).
    authorized_keys: Option<Arc<AuthorizedKeys>>,
    /// Connections listening to each space: the one set behind everything
    /// space-wide.
    ///
    /// A connection enters by `SubscribeSignals` (a daemon's first request in
    /// every session, the only one `kutl mcp serve` makes) and leaves on
    /// disconnect. It is the recipient set for signal fan-out and for
    /// lifecycle broadcasts (register, rename, unregister, displacement), and
    /// the presence set behind `list_participants` and `status`: to be
    /// present in a space is to be listening to it. Document subscriptions
    /// are deliberately not a membership (a client subscribed to zero
    /// documents must not need a sentinel document to be here, and one
    /// subscribed to documents without listening is not here), and
    /// `ListSpaceDocuments` is a pure query.
    ///
    /// The value is the state of that connection's signal stream; see
    /// [`SignalStream`]. Catch-up cursors are not kept here: a re-subscribe
    /// carries the client's own cursor, and paging is idempotent.
    listeners: HashMap<String, HashMap<ConnId, SignalStream>>,
    /// Observer for relay events (edits, lifecycle, signals).
    observer: Arc<dyn RelayObserver>,
    /// Observer called before each text merge.
    before_merge: Arc<dyn BeforeMergeObserver>,
    /// Observer called after each text merge.
    after_merge: Arc<dyn AfterMergeObserver>,
    /// Per-tenant storage quota backend. `None` without quota enforcement;
    /// DB-backed deployments inject a backend to enforce tier-based limits.
    /// Consumed by `handle_blob_sync_ops` / `handle_text_sync_ops` as the
    /// inbound pre-check, and by the flush task for post-write
    /// reconciliation.
    quota_backend: Option<Arc<dyn crate::quota_backend::QuotaBackend>>,
    /// The relay's own `did:key` signing identity.
    /// `None` on standalone test actors and the in-memory test relay, or when
    /// identity load fails at startup — records degrade to tier-3 asserted in
    /// that case. The OSS binary loads one from its data dir; the hosted
    /// relay from its identity directory.
    pub(crate) signing_identity: Option<Arc<crate::identity::RelayIdentity>>,
    /// Where this relay durably keeps signal records.
    /// Segments on a self-hoster's disk, Postgres on the hosted product —
    /// the deployment decides, which is why this is a trait object rather
    /// than a concrete segment store. `None` on the in-memory test relay and
    /// on standalone test actors, in which case appends are no-ops.
    pub(crate) record_log: signal_log::SignalLogHandle,
    /// Monotonic clock that stamps ALL relay-originated signal
    /// records: the actor's own MCP create/close/reopen transitions AND the
    /// materializer's marker-derived records draw from this SAME clock (it is
    /// cloned into the [`RecordMaterializingObserver`]). Guarantees
    /// strictly-increasing HLCs even within one millisecond, so records emitted
    /// in sequence (e.g. a materialized transition then an MCP transition for
    /// the same signal) order by their HLC — causally, never by a random
    /// `record_id`/actor-uuid tiebreak. Shared behind a `Mutex` because the
    /// observer stamps from `&self`; the observer's `after_text_merge` runs ON
    /// this actor task, so the lock is effectively uncontended and never held
    /// across an `.await`. Seeded with a fresh per-process actor id; not
    /// persisted across restart — wall-clock forward progress covers ordering
    /// after a restart.
    ///
    /// [`RecordMaterializingObserver`]: crate::markers::materialize::RecordMaterializingObserver
    pub(crate) signal_clock: SharedSignalClock,
    /// `(space_id, document_id)` pairs whose after-merge observer trackers have
    /// been seeded from the durable records THIS PROCESS (restart
    /// correctness). The after-merge observer holds its known-sets in process
    /// memory with no persistence, so on every restart the first merge of a
    /// marker-bearing doc would otherwise diff against an EMPTY baseline —
    /// re-emitting unchanged markers as duplicate CREATEDs and, worse, missing
    /// the CLOSED(WITHDRAWN) for a marker removed in that first merge (an
    /// empty-vs-empty diff). Before the first after-merge call for a doc, the
    /// actor loads that doc's durable records and calls
    /// [`AfterMergeObserver::seed_doc_from_records`] so the diff reflects the
    /// true pre-merge state, then records the pair here to seed only once.
    initialized_docs: HashSet<(String, String)>,
}

/// A single HLC clock shared by the relay actor and the OSS record
/// materializer, so every relay-originated signal record draws its stamp from
/// ONE monotonic source (see [`Relay::signal_clock`]). Wrapped in a `std::sync`
/// `Mutex`: the observer stamps from `&self` on the actor task, so the guard is
/// never held across an `.await` and cannot deadlock.
pub(crate) type SharedSignalClock = Arc<std::sync::Mutex<kutl_core::HlcClock>>;

/// Build the monotonic clock that stamps relay-originated signal records.
///
/// Seeded with a fresh per-process actor id so distinct relay processes never
/// share an HLC actor. Not persisted across restart. Returned wrapped
/// so the same clock can be cloned into the materializer.
pub(crate) fn new_signal_clock() -> SharedSignalClock {
    Arc::new(std::sync::Mutex::new(kutl_core::HlcClock::new(
        kutl_core::ActorId(uuid::Uuid::new_v4()),
    )))
}

/// Await the next materialized-record batch from an optional receiver.
///
/// Lets the `run()` `select!` treat "no materializer installed" uniformly: when
/// the receiver is `None` this future is `Pending` forever, disarming the
/// branch; when `Some`, it yields the receiver's next value (`None` on channel
/// close). Extracted so the borrow of the `Option` stays local to the branch.
async fn recv_materialized(
    rx: &mut Option<crate::markers::materialize::MaterializeReceiver>,
) -> Option<crate::markers::materialize::MaterializedBatch> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Everything [`Relay::init`] needs to build the actor: the config plus every
/// optional backend/channel slot. Named-field form (rather than a 19-arg fn)
/// so the production and standalone constructors state their differing slots
/// explicitly and cannot mis-order positional arguments.
struct InitArgs {
    config: RelayConfig,
    rx: Option<mpsc::Receiver<RelayCommand>>,
    session_backend: Option<Arc<dyn crate::session_backend::SessionBackend>>,
    pat_backend: Option<Arc<dyn crate::pat_backend::PatBackend>>,
    membership_backend: Option<Arc<dyn crate::membership_backend::MembershipBackend>>,
    registry_backend: Option<Arc<dyn RegistryBackend>>,
    space_backend: Option<Arc<dyn crate::space_backend::SpaceBackend>>,
    content_backend: Option<Arc<dyn ContentBackend>>,
    blob_backend: Option<Arc<dyn BlobBackend>>,
    self_tx: Option<mpsc::Sender<RelayCommand>>,
    observer: Arc<dyn RelayObserver>,
    before_merge: Arc<dyn BeforeMergeObserver>,
    after_merge: Arc<dyn AfterMergeObserver>,
    change_backend: Option<Arc<dyn crate::change_backend::SignalProjection>>,
    quota_backend: Option<Arc<dyn crate::quota_backend::QuotaBackend>>,
    relay_identity: Option<Arc<crate::identity::RelayIdentity>>,
    record_log: Option<Arc<dyn crate::record_log::RecordLog>>,
    materialize_rx: Option<crate::markers::materialize::MaterializeReceiver>,
    signal_clock: SharedSignalClock,
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
        space_backend: Option<Arc<dyn crate::space_backend::SpaceBackend>>,
        content_backend: Option<Arc<dyn ContentBackend>>,
        blob_backend: Option<Arc<dyn BlobBackend>>,
        self_tx: mpsc::Sender<RelayCommand>,
        observer: Arc<dyn RelayObserver>,
        before_merge: Arc<dyn BeforeMergeObserver>,
        after_merge: Arc<dyn AfterMergeObserver>,
        change_backend: Option<Arc<dyn crate::change_backend::SignalProjection>>,
        quota_backend: Option<Arc<dyn crate::quota_backend::QuotaBackend>>,
        relay_identity: Option<Arc<crate::identity::RelayIdentity>>,
        record_log: Option<Arc<dyn crate::record_log::RecordLog>>,
        materialize_rx: Option<crate::markers::materialize::MaterializeReceiver>,
        signal_clock: SharedSignalClock,
    ) -> Self {
        Self::init(InitArgs {
            config,
            rx: Some(rx),
            session_backend,
            pat_backend,
            membership_backend,
            registry_backend,
            space_backend,
            content_backend,
            blob_backend,
            self_tx: Some(self_tx),
            observer,
            before_merge,
            after_merge,
            change_backend,
            quota_backend,
            relay_identity,
            record_log,
            materialize_rx,
            signal_clock,
        })
    }

    /// Shared field-initialization core of [`Relay::new`] and the standalone
    /// test-support constructors — the ONE place the full actor struct literal
    /// is written, so the two entry modes cannot drift. Derives the auth store,
    /// authorized-keys watcher, and registry cache from the config/backends and
    /// runs the path-key migration.
    fn init(args: InitArgs) -> Self {
        // Authentication is mandatory.
        let auth = AuthStore::new();
        let authorized_keys = args
            .config
            .authorized_keys_file
            .as_ref()
            .map(|path| Arc::new(AuthorizedKeys::new(path.clone())));
        let registries = load_from_backend(args.registry_backend.as_deref());
        let mut relay = Self {
            config: args.config,
            connections: HashMap::new(),
            documents: HashMap::new(),
            rx: args.rx,
            materialize_rx: args.materialize_rx,
            auth,
            authenticated: HashMap::new(),
            mcp_sessions: HashMap::new(),
            session_backend: args.session_backend,
            pat_backend: args.pat_backend,
            membership_backend: args.membership_backend,
            registries,
            registry_backend: args.registry_backend,
            space_backend: args.space_backend,
            content_backend: args.content_backend,
            blob_backend: args.blob_backend,
            self_tx: args.self_tx,
            authorized_keys,
            listeners: HashMap::new(),
            observer: args.observer,
            before_merge: args.before_merge,
            after_merge: args.after_merge,
            quota_backend: args.quota_backend,
            signing_identity: args.relay_identity,
            // Log and projection go in together: appending a record and
            // projecting it are one act, and the handle is what makes that
            // structural rather than remembered.
            record_log: signal_log::SignalLogHandle::new(args.record_log, args.change_backend),
            signal_clock: args.signal_clock,
            initialized_docs: HashSet::new(),
        };
        relay.migrate_path_based_keys();
        relay
    }

    /// Create a standalone relay without a command channel (simulation mode).
    ///
    /// Drive it by calling [`Relay::process_command`] directly.
    ///
    /// Test-support surface: production code constructs via [`Relay::new`]
    /// (`build_app`); the standalone trio exists for kutl-sim and the
    /// out-of-crate integration tests that drive the actor in-process.
    #[doc(hidden)]
    pub fn new_standalone(config: RelayConfig) -> Self {
        Self::new_standalone_with_backend(config, None, None, None, None)
    }

    /// Create a standalone relay with optional backends and observer.
    ///
    /// Test-support surface: see [`Relay::new_standalone`].
    #[doc(hidden)]
    pub fn new_standalone_with_backend(
        config: RelayConfig,
        registry_backend: Option<Arc<dyn RegistryBackend>>,
        content_backend: Option<Arc<dyn ContentBackend>>,
        blob_backend: Option<Arc<dyn BlobBackend>>,
        space_backend: Option<Arc<dyn crate::space_backend::SpaceBackend>>,
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
    ///
    /// Test-support surface: see [`Relay::new_standalone`]. Delegates to the
    /// same [`Relay::init`] core as [`Relay::new`] with no command channel and
    /// no session/pat/membership/change/quota backends.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn new_standalone_with_observer(
        config: RelayConfig,
        registry_backend: Option<Arc<dyn RegistryBackend>>,
        content_backend: Option<Arc<dyn ContentBackend>>,
        blob_backend: Option<Arc<dyn BlobBackend>>,
        space_backend: Option<Arc<dyn crate::space_backend::SpaceBackend>>,
        observer: Arc<dyn RelayObserver>,
        before_merge: Arc<dyn BeforeMergeObserver>,
        after_merge: Arc<dyn AfterMergeObserver>,
    ) -> Self {
        Self::init(InitArgs {
            config,
            rx: None,
            change_backend: None,
            session_backend: None,
            pat_backend: None,
            membership_backend: None,
            registry_backend,
            space_backend,
            content_backend,
            blob_backend,
            self_tx: None,
            observer,
            before_merge,
            after_merge,
            quota_backend: None,
            relay_identity: None,
            record_log: None,
            materialize_rx: None,
            signal_clock: new_signal_clock(),
        })
    }

    /// Return the relay's signing identity for use by signal record tasks.
    /// `None` when no identity is configured.
    pub(crate) fn signing_identity(&self) -> Option<&Arc<crate::identity::RelayIdentity>> {
        self.signing_identity.as_ref()
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

        // The dedicated UNBOUNDED materialize channel, when a
        // materializer is installed. Drained as an additional select branch
        // below so materialized-record batches are never dropped under command-
        // channel backpressure. `None` (no materializer) collapses that branch.
        let mut materialize_rx = self.materialize_rx.take();

        // Log the relay signing identity at startup. Signal record tasks
        // access the identity via `self.signing_identity()`.
        if let Some(id) = self.signing_identity() {
            debug!(relay_did = %id.did(), "relay signing identity active");
        } else {
            debug!("no relay signing identity; signal records will be tier-3 asserted");
        }

        // Log record-log presence at startup.
        if self.record_log.is_configured() {
            debug!("record log active; signal records are durable");
        } else {
            debug!("no record log; signal record appends are no-ops");
        }

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
        if self.record_log.reads().is_some()
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

        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    let Some(cmd) = cmd else {
                        info!("relay command channel closed, shutting down");
                        break;
                    };
                    self.process_command(cmd).await;
                }
                // Drain the materialize channel as a peer branch. `recv` on an
                // absent receiver (`None`) never resolves, so a relay without a
                // materializer just runs the command branch. The drain calls the
                // SAME handler as the old command dispatch — the actor is the
                // sole segment writer, so this is non-re-entrant. A closed
                // materialize channel yields `None`: disarm the branch (drop the
                // receiver) and keep serving commands rather than busy-looping.
                batch = recv_materialized(&mut materialize_rx) => {
                    match batch {
                        Some(batch) => {
                            self.handle_materialized_records(&batch.space_id, batch.records)
                                .await;
                        }
                        None => materialize_rx = None,
                    }
                }
            }
        }

        // Best-effort drain on clean loop exit: the command channel closed, but
        // the unbounded materialize channel may still hold queued batches whose
        // trackers have already advanced past them (dropping would permanently
        // lose those user-authored signals). Drain what is buffered via
        // `try_recv` and append it through the same handler the live branch
        // uses. Residual: a hard task abort (production SIGTERM without graceful
        // shutdown wired) still drops in-flight batches — a separate durability
        // item, out of scope here.
        if let Some(mut rx) = materialize_rx {
            while let Ok(batch) = rx.try_recv() {
                self.handle_materialized_records(&batch.space_id, batch.records)
                    .await;
            }
        }
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
            RelayCommand::Disconnect { conn_id } => self.handle_disconnect(conn_id).await,
            RelayCommand::Handshake { conn_id, msg } => self.handle_handshake(conn_id, &msg).await,
            RelayCommand::Subscribe { conn_id, msg } => self.handle_subscribe(conn_id, &msg).await,
            RelayCommand::Unsubscribe { conn_id, msg } => {
                self.handle_unsubscribe(conn_id, &msg).await;
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
                let result = self.handle_mcp_validate_session_did(&session_id, &expected_did);
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
            RelayCommand::McpResolveParticipant {
                session_id,
                space_id,
                name,
                reply,
            } => {
                let result = self
                    .handle_mcp_resolve_participant(&session_id, &space_id, &name)
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
                base_version,
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
                        &base_version,
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
            RelayCommand::GetChanges {
                did,
                space_id,
                checkpoint,
                reply,
            } => {
                let result = self
                    .handle_get_changes(&did, &space_id, checkpoint.as_deref())
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
            RelayCommand::McpListSignals {
                session_id,
                space_id,
                status,
                kind,
                document_id,
                flag_kind,
                reply,
            } => {
                let result = self
                    .handle_mcp_list_signals(
                        &session_id,
                        &space_id,
                        &status,
                        kind.as_deref(),
                        document_id.as_deref(),
                        flag_kind.as_deref(),
                    )
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
            RelayCommand::Barrier { conn_id, msg } => {
                // Answered only for an authenticated connection: before the
                // handshake there is nothing to order, and an unauthenticated
                // peer must not be able to grow the own-ack lane for free.
                if self.authenticated.contains_key(&conn_id) {
                    self.send_payload(
                        conn_id,
                        sync::sync_envelope::Payload::BarrierAck(sync::BarrierAck {
                            client_ref: msg.client_ref,
                        }),
                    );
                } else {
                    debug!(conn_id, "barrier before handshake ignored");
                }
            }
            RelayCommand::SubmitFlag { conn_id, msg } => {
                self.handle_submit_flag(conn_id, &msg).await;
            }
            RelayCommand::SubmitComment { conn_id, msg } => {
                self.handle_submit_comment(conn_id, &msg).await;
            }
            RelayCommand::SubmitReply { conn_id, msg } => {
                self.handle_submit_reply(conn_id, &msg).await;
            }
            RelayCommand::SubmitTransition { conn_id, msg } => {
                self.handle_submit_transition(conn_id, &msg).await;
            }
            RelayCommand::SignalReseedFrame { conn_id, msg } => {
                self.handle_submit_reseed(conn_id, msg).await;
            }
            RelayCommand::SubscribeSignals { conn_id, msg } => {
                self.handle_subscribe_signals(conn_id, &msg).await;
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
                self.derive_effects(&key).await;
            }
            RelayCommand::FlushPendingEdit {
                space_id,
                document_id,
            } => {
                self.handle_flush_pending_edit(&space_id, &document_id)
                    .await;
            }
            // ---- Device auth flow commands ----
            RelayCommand::CreateDeviceRequest { reply } => {
                let result = self.handle_create_device_request();
                let _ = reply.send(Ok(result));
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
                if let Some(backend) = self.record_log.reads_owned() {
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

    async fn handle_disconnect(&mut self, conn_id: ConnId) {
        info!(conn_id, "connection removed");
        self.connections.remove(&conn_id);
        self.authenticated.remove(&conn_id);
        for slot in self.documents.values_mut() {
            slot.subscribers.remove(&conn_id);
        }
        // Drop the conn from every space it listened to, and remove any space
        // key whose set becomes empty. Without this, `listeners` grows
        // unbounded over process lifetime — one residual key per space ever
        // listened to, never reclaimed once the last listener disconnects.
        self.listeners.retain(|_, conns| {
            conns.remove(&conn_id);
            !conns.is_empty()
        });
        let keys_to_check: Vec<DocKey> = self
            .documents
            .iter()
            .filter(|(_, slot)| slot.subscribers.is_empty())
            .map(|(key, _)| key.clone())
            .collect();
        for key in &keys_to_check {
            self.derive_effects(key).await;
        }
    }

    /// Access the document registry for a space (read-only).
    ///
    /// Test-support surface: product code (including the MCP handlers) reads
    /// `self.registries` directly; this accessor exists for the out-of-crate
    /// registry integration/persistence tests.
    #[doc(hidden)]
    pub fn registry(&self, space_id: &str) -> Option<&registry::DocumentRegistry> {
        self.registries.get(space_id)
    }

    // -----------------------------------------------------------------------
    // Eviction
    // -----------------------------------------------------------------------

    /// Derive side effects from document state. Idempotent — safe to call
    /// after every command or on a timer.
    #[allow(clippy::cast_sign_loss)]
    async fn derive_effects(&mut self, key: &DocKey) {
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
            self.flush_pending_edit_for(key).await;
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

    /// Seed the after-merge observer's per-document baseline from durable
    /// records, ONCE per `(space, document)` per process (restart
    /// correctness). No-op after the first call for a doc, and a no-op on a
    /// relay without a record log — seeding an empty baseline is exactly
    /// what a fresh in-memory known-set already is.
    ///
    /// The observer holds its marker known-sets purely in process memory, so
    /// after a restart the first merge of a marker-bearing doc would otherwise
    /// diff the current content against an EMPTY set — re-emitting unchanged
    /// markers as duplicate CREATEDs and MISSING the CLOSED(WITHDRAWN) for a
    /// marker removed in that same first merge. This loads the space's records,
    /// filters to `document_id`, and hands them to
    /// [`AfterMergeObserver::seed_doc_from_records`] so the next
    /// `after_text_merge` diff reflects the durable pre-merge state. Stateless
    /// observers default that method to a no-op, so this is inert for them.
    ///
    /// A load failure is logged and the pair is left UNSEEDED (no
    /// `initialized_docs` insert) so a later merge retries rather than diffing
    /// against a wrong baseline.
    async fn ensure_doc_seeded(&mut self, space_id: &str, document_id: &str) {
        let pair = (space_id.to_owned(), document_id.to_owned());
        if self.initialized_docs.contains(&pair) {
            return;
        }
        if !self.record_log.is_configured() {
            // Ephemeral relay: the fresh in-memory known-set already IS the
            // empty durable baseline. Mark seeded so we do not re-check.
            self.initialized_docs.insert(pair);
            return;
        }
        let Ok(space_uuid) = uuid::Uuid::parse_str(space_id) else {
            error!(%space_id, "seed skipped: space_id is not a uuid");
            return;
        };
        // One document's history, not the whole space's: the seed runs per
        // document, so a substrate that can index by document reads only what
        // it needs (the trait's default filters a whole-space read).
        let doc_records = match self.record_log.load_document(space_uuid, document_id).await {
            Ok(records) => records,
            Err(e) => {
                error!(
                    error = %e,
                    %space_id,
                    %document_id,
                    "failed to load records to seed after-merge baseline — will retry on next merge"
                );
                return;
            }
        };
        self.after_merge
            .seed_doc_from_records(space_id, document_id, &doc_records);
        self.initialized_docs.insert(pair);
    }

    /// Forget an unregistered document on the after-merge side: the
    /// observer's per-document state and this process's seeded mark go
    /// together, so a document re-registered under the same id is seeded
    /// afresh from its records before its first merge
    /// ([`Self::ensure_doc_seeded`]) instead of diffing against the
    /// markers it carried before it was removed.
    pub(super) fn forget_document_markers(&mut self, space_id: &str, document_id: &str) {
        self.after_merge
            .on_document_unregistered(space_id, document_id);
        self.initialized_docs
            .remove(&(space_id.to_owned(), document_id.to_owned()));
    }

    /// Seed the after-merge baseline (once per doc — see [`Self::ensure_doc_seeded`])
    /// and then invoke the after-merge observer on the document's current
    /// (post-merge) content. The single funnel for firing `after_text_merge`, so
    /// the restart re-derive happens on every merge path (WS edit, snippet
    /// flush, MCP edit) without duplicating the seed logic.
    async fn invoke_after_merge(&mut self, key: &DocKey, event: MergedEvent) {
        // A soft-deleted document fires nothing: the delete cascade
        // tombstoned its markers, so a diff here (an eviction flush, a stale
        // debounce, an MCP merge on the deleted id) would re-materialize
        // them as fresh CREATEDs on a document that no longer exists. An
        // edit that revives the document clears `deleted_at` before it
        // reaches this funnel, so a revived document still fires.
        if self.document_is_soft_deleted(&key.space_id, &key.document_id) {
            return;
        }
        self.ensure_doc_seeded(&key.space_id, &key.document_id)
            .await;
        // Copy the content out before the await below: a `&Document` held
        // across it would make the actor's future non-`Send`, and the engine
        // is not `Sync` (its rope buffers local edits behind a `RefCell`).
        // Every observer this relay installs parses the content, so the copy
        // is not wasted on the text merges this funnel serves.
        let content = match self.documents.get(key).map(|slot| &slot.content) {
            Some(DocContent::Text(doc)) => doc.content(),
            _ => return,
        };
        // The relay's per-merge audit line (level table: a merge is a
        // "significant state transition" = info). Emitted from the single
        // merge funnel so it survives regardless of which after-merge
        // observer is installed — the OSS materializer and kutlhub's
        // stream-emitting enricher each log nothing here.
        info!(
            space_id = %event.space_id,
            document_id = %event.document_id,
            author_did = %event.author_did,
            op_count = event.op_count,
            intent = %event.intent,
            "text edited"
        );
        self.after_merge.after_text_merge(event, &content).await;
    }

    /// Flush a pending edit for the given document, notifying the after-merge
    /// observer. No-op if no pending edit exists.
    async fn flush_pending_edit_for(&mut self, key: &DocKey) {
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

        // Only text docs carry a pending snippet edit; `invoke_after_merge`
        // re-checks the content mode and no-ops for a blob slot. Releasing the
        // `slot` borrow here lets the `&mut self` seed step run before the fire.
        let event = MergedEvent {
            space_id: key.space_id.clone(),
            document_id: key.document_id.clone(),
            author_did: pending.author_did,
            via_pat_id: pending.via_pat_id,
            op_count: pending.op_count,
            intent: pending.intent,
            content_mode: EditContentMode::Text,
            timestamp: pending.timestamp,
        };
        self.invoke_after_merge(key, event).await;
    }

    /// Handle a snippet debounce timer firing — delegates to [`flush_pending_edit_for`].
    async fn handle_flush_pending_edit(&mut self, space_id: &str, document_id: &str) {
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
        };
        self.flush_pending_edit_for(&key).await;
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
    /// Used by the registry rollback tests to
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

    /// Whether the registry holds `document_id` as soft-deleted. `false` for
    /// a document the registry never saw: unknown is not deleted.
    fn document_is_soft_deleted(&self, space_id: &str, document_id: &str) -> bool {
        self.registries
            .get(space_id)
            .and_then(|reg| reg.get_any(document_id))
            .and_then(|entry| entry.deleted_at)
            .is_some()
    }

    /// Give the actor a record log (and optionally the projection that
    /// goes with it) after construction.
    ///
    /// Exposed so a test can build the standalone actor and then decide
    /// whether it keeps signal records; the log and the projection go in
    /// together because appending and projecting are one act.
    #[doc(hidden)]
    pub fn test_set_record_log(
        &mut self,
        log: Option<Arc<dyn crate::record_log::RecordLog>>,
        projection: Option<Arc<dyn crate::change_backend::SignalProjection>>,
    ) {
        self.record_log = signal_log::SignalLogHandle::new(log, projection);
    }

    /// Inject a `MembershipBackend` after the relay is constructed.
    ///
    /// Exposed so integration tests (notably the ack-envelope
    /// tests) can wire up account resolution
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

    /// Send a broadcast frame to *another* connection on its bounded ctrl lane.
    ///
    /// For lifecycle broadcasts to the space's listeners and displacement
    /// corrections — frames destined for connections OTHER than the one whose
    /// command is being handled. The ctrl lane is bounded;
    /// on a full or closed lane the connection is EVICTED (mirroring the
    /// data-lane `relay_and_evict`) rather than dropping the frame in place.
    /// The evicted connection re-subscribes on reconnect, so eviction is
    /// recoverable and keeps the lane provably bounded.
    async fn send_broadcast(&mut self, conn_id: ConnId, bytes: &[u8]) {
        let Some(channels) = self.connections.get(&conn_id) else {
            return;
        };
        if let Err(e) = channels.ctrl.try_send(bytes.to_vec()) {
            debug!(conn_id, error = %e, "evicting connection: broadcast ctrl lane unavailable");
            self.handle_disconnect(conn_id).await;
        }
    }

    /// Broadcast a live signal record on the DATA lane, evicting-and-notifying
    /// rather than dropping the connection.
    ///
    /// Two differences from [`Self::send_broadcast`], and both are the point.
    ///
    /// **The lane.** Signals do NOT ride `ctrl`, which is sized for lifecycle
    /// storms and is the highest-priority lane. They belong on `data` with
    /// document backfill and presence: bulk traffic should yield to control
    /// traffic rather than preempt it. The cost of sharing is real —
    /// a burst of live signal frames that fills the lane makes the next document op's
    /// `try_send` fail — which is why paging discipline is load-bearing
    /// rather than an afterthought.
    ///
    /// **The recovery.** A full lane must not tear down the WHOLE connection:
    /// a listener slow to drain one signal broadcast would lose its document
    /// sync as collateral. Nor must it cost the listener its place in the
    /// space: lifecycle broadcasts ride the ctrl lane, and a party that
    /// stopped receiving registrations because its data lane was briefly
    /// full would miss documents until its next session. So the signal
    /// STREAM is paused and the connection told, which is the recoverable
    /// half of "pause and notify"; see [`SignalStream`].
    ///
    /// The notice rides the **ack** lane, never the lane that just overflowed.
    /// That is not incidental — a storm of notices on a bounded lane can
    /// overflow it and re-strand the very listeners the notices exist to
    /// save. `send_stale_signal_stream` already sends on `ack`, which is why
    /// this can simply call it.
    fn send_signal_broadcast(&mut self, space_id: &str, conn_id: ConnId, bytes: &[u8]) {
        match self.try_send_signal_frame(conn_id, bytes.to_vec()) {
            SignalSend::Sent | SignalSend::Gone => {}
            SignalSend::Yielded => {
                self.pause_signal_stream(
                    space_id,
                    conn_id,
                    "signal stream yielded to document traffic on a full lane; re-subscribe",
                );
            }
            SignalSend::LaneFull => {
                self.pause_signal_stream(
                    space_id,
                    conn_id,
                    "signal broadcast did not fit the data lane; re-subscribe",
                );
            }
        }
    }

    /// Enqueue one frame of SIGNAL traffic onto a connection's shared data
    /// lane, yielding to document traffic.
    ///
    /// This is the ONE door for signal frames onto the lane: every live
    /// broadcast comes through it, so the document-headroom reserve binds
    /// every signal writer. (A catch-up page is a reply to the connection's
    /// own request and rides the own-ack lane instead, so it never
    /// yields.) Signals stop short of the last slots
    /// so a document op always has somewhere to land: sharing `data` means
    /// a signal burst that fills the lane makes the next document op's
    /// `try_send` fail, and `relay_and_evict` answers a failed send by
    /// removing that connection from `slot.subscribers` — a space that is
    /// merely chatty about signals would knock its peers off documents they
    /// were syncing fine.
    ///
    /// Refusing early is the whole mechanism: the reserved slots are not
    /// for signals to use later, they are for document traffic to use at
    /// all. Which is also why this is a capacity check and not a retry —
    /// the point is to LEAVE the room, not to wait for it. Callers choose
    /// the remedy per traffic kind (pause the stream for a broadcast).
    pub(crate) fn try_send_signal_frame(&self, conn_id: ConnId, bytes: Vec<u8>) -> SignalSend {
        let Some(channels) = self.connections.get(&conn_id) else {
            return SignalSend::Gone;
        };
        if channels.data.capacity() <= document_headroom(channels.data.max_capacity()) {
            return SignalSend::Yielded;
        }
        if channels.data.try_send(bytes).is_err() {
            return SignalSend::LaneFull;
        }
        SignalSend::Sent
    }

    /// Pause a listener's signal stream and tell it why.
    ///
    /// The connection stays a listener (present, receiving lifecycle
    /// broadcasts); only signal delivery stops, until its next
    /// `SubscribeSignals` sets the stream flowing again. Skipping paused
    /// streams in the fan-out is what stops a notice storm: one overflow is
    /// one notice, however many broadcasts follow before the re-subscribe.
    /// The stream state is the only per-listener value kept; a re-subscribe
    /// carries the client's own cursor, so nothing is lost by not resuming
    /// relay-side.
    ///
    /// The notice rides the ack lane, never the lane that just overflowed.
    fn pause_signal_stream(&mut self, space_id: &str, conn_id: ConnId, reason: &str) {
        debug!(conn_id, space_id, reason, "pausing signal stream");
        if let Some(stream) = self
            .listeners
            .get_mut(space_id)
            .and_then(|conns| conns.get_mut(&conn_id))
        {
            *stream = SignalStream::Paused;
        }
        self.send_stale_signal_stream(
            conn_id,
            space_id,
            kutl_proto::sync::StaleStreamReason::PausedLaneFull,
            reason,
        );
    }

    /// Refuse one `SyncOps` frame on the sender's own-ack lane, scoped to
    /// its document. Accepted ops are never acked; a refusal always is.
    fn send_sync_ops_rejected(
        &self,
        conn_id: ConnId,
        space_id: &str,
        document_id: &str,
        error: sync::Error,
    ) {
        let frame = encode_envelope(&sync_ops_rejected_envelope(space_id, document_id, error));
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
                    // Persist the oplog together with the durable change-metadata
                    // snapshot as one opaque content envelope,
                    // so blame's author map survives a relay cold-load/restart.
                    let list = kutl_core::ChangeList {
                        changes: doc.changes().to_vec(),
                        author_by_agent: doc.author_map(),
                    };
                    FlushContent::Text(kutl_core::encode_content_envelope(
                        &doc.encode_full(),
                        &list,
                    ))
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
                // The Text payload is the content envelope (oplog + the
                // `ChangeList`, including the uncapped `author_by_agent` map),
                // so `size_bytes` and the `ABSOLUTE_BLOB_MAX` flush-cap
                // include the metadata bytes — the correct durable size. Author
                // map GC is deferred.
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

/// Test DIDs authorized by [`test_config`]'s seeded `authorized_keys` file.
///
/// Authentication is mandatory, so the standalone actor tests need
/// an authorizer: an OSS relay with no membership backend authorizes via an
/// `authorized_keys` file, and a bare-DID line grants every space. This is the
/// allowlist of identities the in-crate tests act AS — [`TEST_CONN_DID`] (the
/// identity [`connect_client`] attaches) plus the per-scenario member/seeder
/// DIDs. `did:key:zStranger` is deliberately ABSENT so the non-member
/// rejection tests still observe `NotAuthorized`.
#[cfg(test)]
const TEST_AUTHORIZED_DIDS: &[&str] = &[
    TEST_CONN_DID,
    "did:test",
    "did:a",
    "did:b",
    "did:agent",
    "did:author",
    "did:closer",
    "did:key:zAlice",
    "did:key:zAMember",
    "did:key:zAuthor",
    "did:key:zBMember",
    "did:key:zBob",
    "did:key:zCarol",
    "did:key:zDeadSse",
    "did:key:zDeleter",
    "did:key:zHuman",
    "did:key:zLiveSse",
    "did:key:zM",
    "did:key:zReader",
    "did:key:zReplier",
    "did:key:zReviver",
    "did:key:zSeeder",
    "did:key:zTarget",
];

/// A `RelayConfig` for the standalone actor unit tests.
///
/// Seeds an `authorized_keys` file with [`TEST_AUTHORIZED_DIDS`] so
/// authenticated connections (and DID-scoped catch-up/re-seed callers) pass
/// `authorize_space` under mandatory auth. Each call writes a
/// UNIQUELY-named file under the OS temp dir — the file is `authorized_keys`
/// which live-reloads per auth check, so a fixed shared path would let one
/// test's truncate+rewrite race another's read. A per-call name isolates
/// concurrent tests. The file is intentionally leaked (test-only, tiny).
#[cfg(test)]
fn test_config() -> RelayConfig {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = format!(
        "kutl-relay-test-authorized-keys-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let keys_path = std::env::temp_dir().join(unique);
    std::fs::write(&keys_path, format!("{}\n", TEST_AUTHORIZED_DIDS.join("\n")))
        .expect("write test authorized_keys file");
    RelayConfig {
        port: 0,
        relay_name: "test-relay".into(),
        outbound_capacity: 16,
        authorized_keys_file: Some(keys_path),
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

/// Fixed authenticated identity attached to connections by [`connect_client`].
///
/// Authentication is mandatory, so every actor-level unit-test
/// connection must carry an authenticated DID before it can subscribe or
/// register. These in-process tests drive [`Relay::process_command`] directly
/// with no HTTP transport, so they cannot run the real challenge-response
/// token flow; [`Relay::test_set_authenticated`] populates the identity map
/// exactly as a successful handshake would.
#[cfg(test)]
pub(crate) const TEST_CONN_DID: &str = "did:test:conn";

/// Connect a client and attach an authenticated identity.
///
/// Returns `(data_rx, ack_rx, ctrl_rx)` so the caller can inspect outbound
/// messages if needed. Own-acks (register/rename/unregister acks, errors,
/// query results) arrive on `ack_rx`; broadcasts to other subscribers
/// (lifecycle/displacement/signals/stale notices) on `ctrl_rx`.
///
/// The real HTTP handshake (challenge-response bearer validation) is not
/// reachable from these in-process actor tests, so the connection is
/// authenticated directly via [`Relay::test_set_authenticated`] with
/// [`TEST_CONN_DID`] — the same end state a successful handshake produces.
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
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();
    relay
        .process_command(RelayCommand::Connect {
            conn_id,
            tx: data_tx,
            ctrl_tx,
            ack_tx,
        })
        .await;
    relay.test_set_authenticated(conn_id, TEST_CONN_DID);
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
    doc.delta_since(&[])
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

/// Attach a command receiver and a materialize receiver to a standalone
/// relay so it can be driven through the real [`Relay::run`] loop.
///
/// `new_standalone` leaves `rx` / `materialize_rx` `None` (it is driven by
/// direct `process_command` calls). This wires both channels for tests that
/// need the run-loop's own materialize drain-on-exit behavior. `self_tx` stays
/// `None`, so `run()` spawns no background reap tasks.
#[cfg(test)]
fn attach_run_channels(
    relay: &mut Relay,
    rx: mpsc::Receiver<RelayCommand>,
    materialize_rx: crate::markers::materialize::MaterializeReceiver,
) {
    relay.rx = Some(rx);
    relay.materialize_rx = Some(materialize_rx);
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
mod headroom_tests {
    use super::document_headroom;
    use crate::config::DEFAULT_OUTBOUND_CAPACITY;

    /// A ONE-SLOT lane reserves nothing, or signals become undeliverable on it.
    ///
    /// `capacity()` on an empty channel of `max_capacity` 1 returns 1, so a
    /// reserve of 1 makes the `capacity() <= reserve` check true even when the
    /// lane is idle — which would stop
    /// signal delivery on such a connection permanently and silently.
    #[test]
    fn test_a_one_slot_lane_reserves_nothing() {
        assert_eq!(
            document_headroom(1),
            0,
            "a lane of one has no slot to spare"
        );
        assert_eq!(document_headroom(0), 0);
    }

    /// Every lane big enough to spare a slot reserves at least one, so the
    /// document-headroom guarantee never silently degrades to nothing.
    #[test]
    fn test_every_usable_lane_reserves_at_least_one_slot() {
        for cap in 2..=16_usize {
            assert!(
                document_headroom(cap) >= 1,
                "capacity {cap} reserved nothing, which reinstates the defect"
            );
            assert!(
                document_headroom(cap) < cap,
                "capacity {cap} reserved the whole lane, which starves signals"
            );
        }
    }

    /// The production lane keeps a quarter back for documents.
    #[test]
    fn test_production_capacity_reserves_a_quarter() {
        assert_eq!(document_headroom(DEFAULT_OUTBOUND_CAPACITY), 128);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_flush_dirty_produces_entries() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;
        send_text_ops(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
            "hello",
        )
        .await;

        let entries = flush_and_collect(&mut relay).await;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].space_id, "5171e0a1-1111-4000-8000-000000000001");
        assert_eq!(entries[0].doc_id, "doc");
        match &entries[0].content {
            FlushContent::Text(bytes) => {
                assert!(!bytes.is_empty());
                // The flushed Text payload is a content
                // envelope, not a bare oplog: it carries the change-metadata so
                // blame survives cold-load. It must begin with the magic prefix.
                assert_eq!(
                    bytes[..kutl_core::ENVELOPE_MAGIC.len()],
                    kutl_core::ENVELOPE_MAGIC,
                    "flush must emit a content envelope, not a bare oplog",
                );
            }
            FlushContent::Blob(_) => panic!("expected text content, got blob"),
        }
    }

    #[tokio::test]
    async fn test_flush_clean_documents_skipped() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;
        // No ops sent — document is clean.

        let entries = flush_and_collect(&mut relay).await;

        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_flush_clears_dirty_flag() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;
        send_text_ops(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
            "hello",
        )
        .await;

        assert!(relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "doc"));

        // Flush produces one entry but does NOT clear dirty until
        // FlushCompleted confirms persistence.
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 1);
        assert!(
            relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "doc"),
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
            !relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "doc"),
            "dirty flag must clear after FlushCompleted"
        );

        // Second flush should produce zero entries — already clean.
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_flush_without_confirm_stays_dirty_for_retry() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;
        send_text_ops(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
            "hello",
        )
        .await;

        // First flush: collect entries but do NOT confirm (simulates
        // a backend save failure where the flush task never sends
        // FlushCompleted).
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 1);
        assert!(relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "doc"));

        // Second flush: the document should appear again because it's
        // still dirty — the failed first flush is automatically retried.
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 1, "dirty doc must be re-sent on retry");
    }

    #[tokio::test]
    async fn test_flush_multiple_dirty_docs() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc-a",
        )
        .await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc-b",
        )
        .await;
        send_text_ops(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc-a",
            "alpha",
        )
        .await;
        send_text_ops(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc-b",
            "beta",
        )
        .await;

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

        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;

        // Send multiple ops to the same document without flushing in between.
        for i in 0..RAPID_MUTATION_COUNT {
            send_text_ops(
                &mut relay,
                conn_id,
                "5171e0a1-1111-4000-8000-000000000001",
                "doc",
                &format!("edit-{i}"),
            )
            .await;
        }

        // Flush should produce exactly one entry (not 5).
        let entries = flush_and_collect(&mut relay).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].doc_id, "doc");
    }

    // ---- empty listener sets are reclaimed on disconnect ----

    /// Listen to a space from `conn_id` (the daemon's first request).
    async fn listen(relay: &mut Relay, conn_id: ConnId, space: &str) {
        relay
            .process_command(RelayCommand::SubscribeSignals {
                conn_id,
                msg: sync::SubscribeSignals {
                    space_id: space.into(),
                    cursor: None,
                },
            })
            .await;
    }

    #[tokio::test]
    async fn test_disconnect_drops_empty_listener_set() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        let space = "5171e0a1-1111-4000-8000-000000000003";

        listen(&mut relay, conn_id, space).await;
        assert!(
            relay.listeners.contains_key(space),
            "listener set should exist after SubscribeSignals"
        );

        // Disconnect the only listener — the now-empty space key must be removed.
        relay
            .process_command(RelayCommand::Disconnect { conn_id })
            .await;
        assert!(
            !relay.listeners.contains_key(space),
            "empty listener set must be reclaimed on disconnect, not left as a residual key"
        );
    }

    /// A refused re-subscribe ends the listening: the notice says the
    /// connection is not a listener, and the set agrees, so a connection
    /// whose authorization lapsed stops receiving the space's broadcasts.
    #[tokio::test]
    async fn test_refused_subscribe_signals_removes_the_listener() {
        let mut relay = Relay::new_standalone(test_config());
        let (_data, _ack, _ctrl) = connect_client(&mut relay, 1).await;
        let space = "5171e0a1-1111-4000-8000-000000000003";
        listen(&mut relay, 1, space).await;
        assert!(
            relay.listeners.contains_key(space),
            "listening after an authorized subscribe"
        );

        // The identity the connection now carries is not authorized here.
        relay.test_set_authenticated(1, "did:key:zStranger");
        listen(&mut relay, 1, space).await;
        assert!(
            !relay.listeners.contains_key(space),
            "a refused subscribe removes the connection from the listener set"
        );
    }

    #[tokio::test]
    async fn test_disconnect_keeps_nonempty_listener_set() {
        // With two listeners, disconnecting one must keep the space key (the
        // other listener still needs lifecycle broadcasts and signals).
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let (_d1, _a1, _c1) = connect_client(&mut relay, 1).await;
        let (_d2, _a2, _c2) = connect_client(&mut relay, 2).await;
        let space = "5171e0a1-1111-4000-8000-000000000003";

        for conn_id in [1, 2] {
            listen(&mut relay, conn_id, space).await;
        }

        relay
            .process_command(RelayCommand::Disconnect { conn_id: 1 })
            .await;
        assert!(
            relay.listeners.contains_key(space),
            "listener set with a remaining listener must be kept"
        );
        assert_eq!(relay.listeners.get(space).map(HashMap::len), Some(1));
    }

    /// Build a durable relay whose after-merge observer is a REAL OSS record
    /// materializer sharing `signal_clock`, backed by a signal store at `dir`.
    /// Returns the relay and the materialize receiver the actor would drain.
    /// Fresh trackers each call, so calling it twice against the same `dir`
    /// stands in for a process restart (durable segments survive; in-memory
    /// tracker known-sets do not).
    #[cfg(test)]
    fn materializing_relay(
        dir: &std::path::Path,
    ) -> (Relay, crate::markers::materialize::MaterializeReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let signal_clock = new_signal_clock();
        let observer = Arc::new(
            crate::markers::materialize::RecordMaterializingObserver::new(
                tx,
                None,
                signal_clock,
                None,
                None,
            ),
        );
        let config = test_config();
        let mut relay = Relay::new_standalone_with_observer(
            config,
            None,
            None,
            None,
            None,
            Arc::new(crate::observer::NoopObserver),
            Arc::new(NoopBeforeMergeObserver),
            observer,
        );
        relay.test_set_record_log(
            Some(Arc::new(crate::record_log::SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(dir.to_path_buf()),
            ))),
            None,
        );
        (relay, rx)
    }

    /// Install a resident text document whose content is EXACTLY `content`
    /// (replacing any prior slot), so `invoke_after_merge` reads a deterministic
    /// post-merge state. Unlike `send_text_ops` (which merges ops into the
    /// resident CRDT, accumulating content), this sets the whole content.
    #[cfg(test)]
    fn set_resident_text(relay: &mut Relay, key: &DocKey, content: &str) {
        let mut slot = DocSlot::empty();
        slot.ensure_text();
        slot.mutate_text(|doc| {
            let agent = doc.register_agent("test")?;
            doc.edit(agent, "test", "insert", Boundary::Explicit, |ctx| {
                ctx.insert(0, content)
            })
        })
        .expect("edit resident text");
        relay.documents.insert(key.clone(), slot);
    }

    /// A text merge event on `space`/`doc` at a fixed timestamp.
    #[cfg(test)]
    fn merge_event(space: &str, doc: &str) -> MergedEvent {
        MergedEvent {
            space_id: space.to_owned(),
            document_id: doc.to_owned(),
            author_did: "did:key:zHuman".to_owned(),
            via_pat_id: None,
            op_count: 1,
            intent: "edit".to_owned(),
            content_mode: EditContentMode::Text,
            timestamp: 1_700_000_000_000,
        }
    }

    /// Drain every buffered materialized batch into the store via the SAME
    /// handler the run-loop uses (append + project + broadcast), mirroring the
    /// actor's materialize branch. Returns the number of records appended.
    #[cfg(test)]
    async fn drain_materialized(
        relay: &mut Relay,
        rx: &mut crate::markers::materialize::MaterializeReceiver,
    ) -> usize {
        let mut count = 0;
        while let Ok(batch) = rx.try_recv() {
            count += batch.records.len();
            relay
                .handle_materialized_records(&batch.space_id, batch.records)
                .await;
        }
        count
    }

    /// An unregistered document is forgotten on the after-merge side and
    /// re-seeded from its durable records if it comes back: its unchanged
    /// marker then materializes nothing. Keeping the seeded mark while
    /// dropping the tracker would diff the returning document against an
    /// empty baseline and re-emit the marker as a duplicate CREATED.
    #[tokio::test]
    async fn test_forgotten_document_is_reseeded_from_records() {
        use kutl_signals::segment::SegmentStore;

        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        let doc = "doc-forgotten";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };
        let marker = "## ? Should we ship?";
        let (mut relay, mut rx) = materializing_relay(dir.path());
        set_resident_text(&mut relay, &key, marker);
        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);
        assert_eq!(
            SegmentStore::load(&dir.path().join(space.to_string()))
                .unwrap()
                .records
                .len(),
            1,
            "the CREATED is durable"
        );

        relay.forget_document_markers(&space_id, doc);
        assert!(
            !relay
                .initialized_docs
                .contains(&(space_id.clone(), doc.to_owned())),
            "the seeded mark goes with the tracker state"
        );

        set_resident_text(&mut relay, &key, marker);
        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(
            drain_materialized(&mut relay, &mut rx).await,
            0,
            "re-seeded from its records, the unchanged marker materializes nothing"
        );
    }

    /// Deleting a document settles its parked edit before the delete cascade
    /// and the tracker forget: the parked edit is dropped and its timer
    /// stopped, so the debounce armed before the delete materializes nothing
    /// afterwards. Left parked, it would fire after the cascade, re-seed from
    /// the tombstoned records (an empty baseline) and re-materialize the
    /// marker as a fresh CREATED on a deleted document.
    #[tokio::test]
    async fn test_handle_unregister_document_settles_the_parked_edit_before_forgetting_markers() {
        use kutl_signals::segment::SegmentStore;

        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        // The registry keys documents by UUID, so the deleted id must be one.
        let doc = "0d0c0d0c-0d0c-4d0c-8d0c-0d0c0d0c0d0c";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };
        let (mut relay, mut rx) = materializing_relay(dir.path());
        let (_data, _ack, _ctrl) = connect_client(&mut relay, 1).await;
        relay
            .registries
            .entry(space_id.clone())
            .or_default()
            .register(
                doc,
                "notes.md",
                registry::EntryMetadata {
                    author_did: TEST_CONN_DID.to_owned(),
                    timestamp: 1_000,
                    ..Default::default()
                },
            );
        set_resident_text(&mut relay, &key, "## ? Should we ship?");
        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);

        // A human's debounced edit is parked on the slot, its timer armed,
        // when the delete lands.
        let slot = relay.documents.get_mut(&key).expect("doc slot exists");
        slot.pending_edit = Some(PendingEdit {
            author_did: "did:key:zTypist".to_owned(),
            via_pat_id: None,
            intent: "edit".to_owned(),
            op_count: 1,
            timestamp: 42,
        });
        slot.snippet_timer = Some(tokio::spawn(std::future::pending()));

        relay
            .handle_unregister_document(
                1,
                &sync::UnregisterDocument {
                    space_id: space_id.clone(),
                    document_id: doc.to_owned(),
                    // Stamped above the registration so the delete wins.
                    metadata: Some(sync::ChangeMetadata {
                        author_did: TEST_CONN_DID.to_owned(),
                        timestamp: 9_000,
                        ..Default::default()
                    }),
                },
            )
            .await;

        assert!(
            relay.document_is_soft_deleted(&space_id, doc),
            "the delete won"
        );
        let slot = relay
            .documents
            .get(&key)
            .expect("the slot stays resident until eviction");
        assert!(
            slot.pending_edit.is_none(),
            "the parked edit is settled by the delete"
        );
        assert!(
            slot.snippet_timer.is_none(),
            "the debounce timer is stopped by the delete"
        );
        assert_eq!(
            drain_materialized(&mut relay, &mut rx).await,
            0,
            "settling the unchanged text materializes nothing"
        );

        // The debounce that was armed before the delete fires anyway.
        relay.handle_flush_pending_edit(&space_id, doc).await;
        assert_eq!(
            drain_materialized(&mut relay, &mut rx).await,
            0,
            "a flush after the delete materializes nothing"
        );

        let records = SegmentStore::load(&dir.path().join(&space_id))
            .unwrap()
            .records;
        let created = records
            .iter()
            .filter(|r| r.event() == sync::SignalEventType::Created)
            .count();
        assert_eq!(
            created, 1,
            "the marker was created once; the delete minted no second CREATED"
        );
        assert_eq!(
            records.len(),
            2,
            "the CREATED, then the cascade's TOMBSTONED"
        );
    }

    /// The after-merge funnel fires nothing for a soft-deleted document even
    /// once its trackers are forgotten: a merge routed there after the delete
    /// (an eviction flush, an MCP edit on the deleted id) would otherwise
    /// re-seed from the tombstoned records and re-materialize the unchanged
    /// marker as a fresh CREATED.
    #[tokio::test]
    async fn test_invoke_after_merge_skips_a_soft_deleted_document() {
        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        let doc = "0d0c0d0c-0d0c-4d0c-8d0c-0d0c0d0c0d0d";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };
        let meta = |ts: i64| registry::EntryMetadata {
            author_did: "did:key:zHuman".to_owned(),
            timestamp: ts,
            ..Default::default()
        };
        let (mut relay, mut rx) = materializing_relay(dir.path());
        relay
            .registries
            .entry(space_id.clone())
            .or_default()
            .register(doc, "notes.md", meta(1_000));
        set_resident_text(&mut relay, &key, "## ? Should we ship?");
        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);

        // The delete, in the handler's order: soft-delete, cascade, forget.
        relay
            .registries
            .get_mut(&space_id)
            .expect("space registry")
            .unregister(doc, &meta(9_000));
        relay
            .cascade_delete_signals_logged(&space_id, doc, "did:key:zHuman")
            .await;
        relay.forget_document_markers(&space_id, doc);

        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(
            drain_materialized(&mut relay, &mut rx).await,
            0,
            "a soft-deleted document fires no after-merge diff"
        );
    }

    /// GUARD (restart re-derive): a relay restart
    /// leaves the after-merge observer's marker known-sets EMPTY (they live only
    /// in process memory). Without seeding the baseline from the durable records,
    /// the first post-restart merge of a marker-bearing doc (a) re-emits an
    /// unchanged marker as a DUPLICATE CREATED and (b) — worse — MISSES the
    /// CLOSED(WITHDRAWN) for a marker removed in that first merge (an
    /// empty-vs-empty diff). `ensure_doc_seeded` re-derives the trackers from the
    /// segments before the first fire, so an unchanged marker yields no record
    /// and a removed one yields its close.
    #[tokio::test]
    async fn test_restart_reseeds_tracker_baseline_from_records() {
        use kutl_signals::fold::SpaceSignalState;
        use kutl_signals::segment::SegmentStore;

        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        let doc = "doc-restart";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };
        let marker = "## ? Should we ship?";

        // --- Pre-restart: author a decision marker → one CREATED persists. ---
        {
            let (mut relay, mut rx) = materializing_relay(dir.path());
            set_resident_text(&mut relay, &key, marker);
            relay
                .invoke_after_merge(&key, merge_event(&space_id, doc))
                .await;
            let appended = drain_materialized(&mut relay, &mut rx).await;
            assert_eq!(appended, 1, "the new decision materializes one CREATED");
        }
        let created_count = SegmentStore::load(&dir.path().join(space.to_string()))
            .unwrap()
            .records
            .len();
        assert_eq!(created_count, 1, "exactly one record on disk pre-restart");

        // --- Restart scenario (i): UNCHANGED marker → NO duplicate CREATED. ---
        // Fresh trackers (empty), same durable store dir. With the baseline
        // re-derived from the segments the diff is empty, so nothing
        // re-materializes. Without the re-derive this re-emits the decision as
        // a second CREATED (unbounded segment growth + repeat notifications).
        {
            let (mut relay, mut rx) = materializing_relay(dir.path());
            set_resident_text(&mut relay, &key, marker);
            relay
                .invoke_after_merge(&key, merge_event(&space_id, doc))
                .await;
            let after_unchanged = drain_materialized(&mut relay, &mut rx).await;
            assert_eq!(
                after_unchanged, 0,
                "an unchanged marker after restart must NOT re-materialize a duplicate CREATED"
            );
        }

        // --- Restart scenario (ii): the FIRST post-restart merge REMOVES the
        // marker → its CLOSED(WITHDRAWN) IS materialized. This is the worst-case
        // bug: with an empty baseline the removal is an empty-vs-empty diff, so
        // the close is NEVER emitted and the signal is stuck Open FOREVER. The
        // re-derived baseline knows the decision, so its removal closes it. ---
        {
            let (mut relay, mut rx) = materializing_relay(dir.path());
            set_resident_text(&mut relay, &key, "no decisions here");
            relay
                .invoke_after_merge(&key, merge_event(&space_id, doc))
                .await;
            let after_removed = drain_materialized(&mut relay, &mut rx).await;
            assert_eq!(
                after_removed, 1,
                "removing a marker in the first post-restart merge must materialize its \
                 CLOSED(WITHDRAWN) — an empty baseline would miss the close and strand it Open"
            );
        }

        // The durable fold now shows exactly ONE signal id, Closed(WITHDRAWN):
        // neither stuck Open nor duplicated across the two restart scenarios.
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let mut fold = SpaceSignalState::default();
        for record in loaded.records {
            fold.apply(record);
        }
        let states: Vec<_> = fold.iter().map(|(_, s)| s.status.clone()).collect();
        assert_eq!(
            states.len(),
            1,
            "one signal id total — no duplicate CREATED"
        );
        assert_eq!(
            states[0],
            kutl_signals::fold::SignalStatus::Closed,
            "the decision folds to Closed after its post-restart removal"
        );
    }

    /// Read the resident text content of `key`'s document slot.
    #[cfg(test)]
    fn resident_text(relay: &Relay, key: &DocKey) -> String {
        match &relay.documents.get(key).expect("doc slot exists").content {
            DocContent::Text(doc) => doc.content(),
            _ => panic!("text document expected"),
        }
    }

    /// GUARD (the decision flip): a decision is marker-born — the document
    /// heading is the source and the record is derived — so a resolve through
    /// the shared transition path must be performed as the `? → =` marker
    /// edit, with the CLOSED(RESOLVED) minted by the materializer from that
    /// merge. A path that writes the record directly leaves the heading
    /// reading `## ?` forever — the resolution never reaches the one surface
    /// that is the decision's source of truth — which the first assertion
    /// pins. The restart half then pins the flip's payoff: document and fold
    /// agree, so a post-restart merge of the document mints nothing and the
    /// resolution holds on every surface.
    #[tokio::test]
    async fn test_decision_resolve_flips_the_marker_and_survives_restart() {
        use kutl_signals::fold::{SignalStatus, SpaceSignalState};
        use kutl_signals::segment::SegmentStore;

        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        let doc = "doc-flip";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };

        // --- Author the decision; its CREATED materializes. ---
        let post_resolve_content;
        {
            let (mut relay, mut rx) = materializing_relay(dir.path());
            set_resident_text(&mut relay, &key, "## ? Should we ship?\n\nbody text\n");
            relay
                .invoke_after_merge(&key, merge_event(&space_id, doc))
                .await;
            assert_eq!(
                drain_materialized(&mut relay, &mut rx).await,
                1,
                "the new decision materializes one CREATED"
            );
            let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
            let signal_id = loaded.records[0].id.clone();

            // --- Resolve through the shared transition path (every door
            // funnels here: WS, HTTP, MCP). ---
            relay
                .emit_transition_record(
                    &signal_id,
                    &space_id,
                    SignalTransitionEvent::Closed,
                    &space_ops::TransitionAuthor {
                        actor_did: "did:key:zCloser",
                        close_reason: Some(sync::CloseReason::Resolved),
                        note: None,
                        via_pat_id: None,
                    },
                )
                .await
                .expect("resolve succeeds");

            // The DOCUMENT carries the resolution.
            let content = resident_text(&relay, &key);
            assert!(
                content.contains("## = Should we ship?"),
                "resolve must flip the heading to `=`: {content:?}"
            );
            assert!(
                !content.contains("## ?"),
                "no open marker remains: {content:?}"
            );
            assert!(
                content.contains("body text"),
                "the flip touches only the marker: {content:?}"
            );

            // The CLOSED(RESOLVED) is minted by the MATERIALIZER from the
            // flip's own merge — not written directly by the transition path.
            assert_eq!(
                drain_materialized(&mut relay, &mut rx).await,
                1,
                "the flip's merge materializes exactly the CLOSE"
            );
            post_resolve_content = resident_text(&relay, &key);
        }

        // Durable fold agrees: one signal, Closed(Resolved).
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let mut fold = SpaceSignalState::default();
        for record in loaded.records {
            fold.apply(record);
        }
        let states: Vec<_> = fold.iter().collect();
        assert_eq!(states.len(), 1, "one signal id total");
        assert_eq!(states[0].1.status, SignalStatus::Closed);
        assert_eq!(
            states[0].1.close_reason(),
            Some(sync::CloseReason::Resolved as i32)
        );

        // --- Restart + a merge of the document AS THE FLIP LEFT IT: the
        // resolution HOLDS. The content is threaded from part 1, not typed
        // here — if the transition path had left the heading `## ?`, this
        // merge is exactly the one that would mint the reverting REOPENED. ---
        {
            let (mut relay, mut rx) = materializing_relay(dir.path());
            set_resident_text(&mut relay, &key, &post_resolve_content);
            relay
                .invoke_after_merge(&key, merge_event(&space_id, doc))
                .await;
            assert_eq!(
                drain_materialized(&mut relay, &mut rx).await,
                0,
                "the post-restart merge must not revert the resolution"
            );
        }
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let mut fold = SpaceSignalState::default();
        for record in loaded.records {
            fold.apply(record);
        }
        assert_eq!(
            fold.iter().next().expect("the decision exists").1.status,
            SignalStatus::Closed,
            "the resolution survives restart + merge"
        );
    }

    /// A decision close under a non-resolved reason is REFUSED: flipping `?`
    /// to `=` would durably record RESOLVED against the caller's stated
    /// intent, and there is no marker spelling for declined/withdrawn — a
    /// withdrawal is the heading's removal, an ordinary document edit.
    #[tokio::test]
    async fn test_decision_close_with_non_resolved_reason_is_refused() {
        use kutl_signals::segment::SegmentStore;

        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        let doc = "doc-declined";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };

        let (mut relay, mut rx) = materializing_relay(dir.path());
        set_resident_text(&mut relay, &key, "## ? Should we ship?\n");
        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let signal_id = loaded.records[0].id.clone();

        let outcome = relay
            .emit_transition_record(
                &signal_id,
                &space_id,
                SignalTransitionEvent::Closed,
                &space_ops::TransitionAuthor {
                    actor_did: "did:key:zCloser",
                    close_reason: Some(sync::CloseReason::Declined),
                    note: None,
                    via_pat_id: None,
                },
            )
            .await;
        assert!(
            matches!(
                outcome,
                Err(crate::change_backend::ChangeError::InvalidArgument { .. })
            ),
            "a declined close on a decision must be refused as a client error: {outcome:?}"
        );
        // Nothing moved: the heading is untouched and no record was minted or
        // materialized.
        assert!(resident_text(&relay, &key).contains("## ? Should we ship?"));
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 0);
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        assert_eq!(loaded.records.len(), 1, "only the CREATED is durable");
    }

    /// GUARD (flip attribution): a pending debounced edit by ANOTHER identity
    /// must not steal the flip's transition. The relay-applied edit path
    /// flushes a different identity's pending edit BEFORE mutating (as the WS
    /// merge path does), so the pending author's event fires against the
    /// content they actually produced and the flip's own event — carrying the
    /// actor — is the one whose diff holds the `? → =` transition. With the
    /// orders reversed, the CLOSED(RESOLVED) is authored by whoever happened
    /// to be typing in the document, with their timestamp.
    #[tokio::test]
    async fn test_decision_flip_is_attributed_to_the_actor_not_a_pending_editor() {
        use kutl_signals::segment::SegmentStore;

        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        let doc = "doc-attrib";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };

        let (mut relay, mut rx) = materializing_relay(dir.path());
        set_resident_text(&mut relay, &key, "## ? Should we ship?\n");
        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let signal_id = loaded.records[0].id.clone();

        // A human is mid-burst in the same document: their debounced edit is
        // parked on the slot when the resolve arrives.
        relay
            .documents
            .get_mut(&key)
            .expect("doc slot exists")
            .pending_edit = Some(PendingEdit {
            author_did: "did:key:zTypist".to_owned(),
            via_pat_id: None,
            intent: "edit".to_owned(),
            op_count: 1,
            timestamp: 42,
        });

        relay
            .emit_transition_record(
                &signal_id,
                &space_id,
                SignalTransitionEvent::Closed,
                &space_ops::TransitionAuthor {
                    actor_did: "did:key:zCloser",
                    close_reason: Some(sync::CloseReason::Resolved),
                    note: None,
                    via_pat_id: None,
                },
            )
            .await
            .expect("resolve succeeds");

        assert_eq!(
            drain_materialized(&mut relay, &mut rx).await,
            1,
            "exactly one CLOSED materializes"
        );
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let closed = loaded
            .records
            .iter()
            .find(|r| r.event() == sync::SignalEventType::Closed)
            .expect("the CLOSED record exists");
        assert_eq!(
            closed.author_did, "did:key:zCloser",
            "the transition names the actor who resolved, not the pending editor"
        );
        assert_ne!(
            closed.timestamp, 42,
            "the transition carries its own time, not the pending edit's"
        );
    }

    /// GUARD (fold heal): when the document already reads `## =` but the fold
    /// still says Open — the CLOSED was lost, e.g. a materialized append that
    /// failed mid-batch — a resolve must not ack success while leaving the
    /// fold un-healed forever. The document is the source and already agrees
    /// with the intent, so the transition record is appended directly: doc
    /// and record agree after the write, which is exactly the state the flip
    /// exists to preserve.
    #[tokio::test]
    async fn test_decision_resolve_heals_a_fold_that_lost_the_close() {
        use kutl_signals::fold::{SignalStatus, SpaceSignalState};
        use kutl_signals::segment::SegmentStore;

        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        let doc = "doc-heal";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };

        let (mut relay, mut rx) = materializing_relay(dir.path());
        set_resident_text(&mut relay, &key, "## ? Should we ship?\n");
        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let signal_id = loaded.records[0].id.clone();

        // The lost-CLOSE state: the document reads resolved, the fold does
        // not (the merge that flipped it materialized a CLOSED whose append
        // failed). Modeled by setting the content without a merge.
        set_resident_text(&mut relay, &key, "## = Should we ship?\n");

        relay
            .emit_transition_record(
                &signal_id,
                &space_id,
                SignalTransitionEvent::Closed,
                &space_ops::TransitionAuthor {
                    actor_did: "did:key:zCloser",
                    close_reason: Some(sync::CloseReason::Resolved),
                    note: None,
                    via_pat_id: None,
                },
            )
            .await
            .expect("resolve succeeds");

        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let mut fold = SpaceSignalState::default();
        for record in loaded.records {
            fold.apply(record);
        }
        assert_eq!(
            fold.iter().next().expect("the decision exists").1.status,
            SignalStatus::Closed,
            "a resolve on an already-flipped heading heals the fold instead of \
             acking success over a permanently-open record"
        );
    }

    /// GUARD (note delivery): a close note on a decision lands as body text
    /// under the heading even when the heading already reads `## =` — the
    /// tool contract promises the note is placed, and dropping it behind a
    /// success ack loses the caller's rationale silently.
    #[tokio::test]
    async fn test_decision_close_note_lands_when_heading_already_resolved() {
        use kutl_signals::segment::SegmentStore;

        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        let doc = "doc-note";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };

        let (mut relay, mut rx) = materializing_relay(dir.path());
        set_resident_text(&mut relay, &key, "## ? Should we ship?\n");
        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let signal_id = loaded.records[0].id.clone();

        let close = |note: Option<&'static str>| space_ops::TransitionAuthor {
            actor_did: "did:key:zCloser",
            close_reason: Some(sync::CloseReason::Resolved),
            note,
            via_pat_id: None,
        };
        relay
            .emit_transition_record(
                &signal_id,
                &space_id,
                SignalTransitionEvent::Closed,
                &close(None),
            )
            .await
            .expect("first resolve succeeds");
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);

        // Second resolve, now with the rationale: the heading already reads
        // `=`, but the note must still land.
        relay
            .emit_transition_record(
                &signal_id,
                &space_id,
                SignalTransitionEvent::Closed,
                &close(Some("chose Postgres; benchmarks in the appendix")),
            )
            .await
            .expect("re-resolve with note succeeds");
        let content = resident_text(&relay, &key);
        assert!(
            content.contains("chose Postgres"),
            "the note lands in the document: {content:?}"
        );
        // The note-only edit changes no marker state, so nothing new
        // materializes.
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 0);
    }

    /// The reopen direction of the decision flip: `= → ?` through the same
    /// path, with the REOPENED minted by the materializer from the merge.
    #[tokio::test]
    async fn test_decision_reopen_flips_the_marker_back() {
        use kutl_signals::fold::{SignalStatus, SpaceSignalState};
        use kutl_signals::segment::SegmentStore;

        let dir = tempfile::TempDir::new().unwrap();
        let space = uuid::Uuid::new_v4();
        let space_id = space.to_string();
        let doc = "doc-reflip";
        let key = DocKey {
            space_id: space_id.clone(),
            document_id: doc.to_owned(),
        };

        let (mut relay, mut rx) = materializing_relay(dir.path());
        set_resident_text(&mut relay, &key, "## ? Should we ship?\n");
        relay
            .invoke_after_merge(&key, merge_event(&space_id, doc))
            .await;
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);
        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let signal_id = loaded.records[0].id.clone();

        relay
            .emit_transition_record(
                &signal_id,
                &space_id,
                SignalTransitionEvent::Closed,
                &space_ops::TransitionAuthor {
                    actor_did: "did:key:zCloser",
                    close_reason: Some(sync::CloseReason::Resolved),
                    note: None,
                    via_pat_id: None,
                },
            )
            .await
            .expect("resolve succeeds");
        assert_eq!(drain_materialized(&mut relay, &mut rx).await, 1);

        relay
            .emit_transition_record(
                &signal_id,
                &space_id,
                SignalTransitionEvent::Reopened,
                &space_ops::TransitionAuthor {
                    actor_did: "did:key:zReopener",
                    close_reason: None,
                    note: None,
                    via_pat_id: None,
                },
            )
            .await
            .expect("reopen succeeds");

        let content = resident_text(&relay, &key);
        assert!(
            content.contains("## ? Should we ship?"),
            "reopen must flip the heading back to `?`: {content:?}"
        );
        assert_eq!(
            drain_materialized(&mut relay, &mut rx).await,
            1,
            "the reopen flip's merge materializes exactly the REOPENED"
        );

        let loaded = SegmentStore::load(&dir.path().join(space.to_string())).unwrap();
        let mut fold = SpaceSignalState::default();
        for record in loaded.records {
            fold.apply(record);
        }
        let states: Vec<_> = fold.iter().collect();
        assert_eq!(states.len(), 1, "one signal id total");
        assert_eq!(
            states[0].1.status,
            SignalStatus::Open,
            "the decision folds back to Open"
        );
    }

    /// GUARD (shared signal clock): the actor's MCP-transition path and the
    /// materializer's marker path stamp from ONE shared HLC clock, so records
    /// issued across the two paths are strictly monotonic and order by causal
    /// sequence — never by a random per-clock actor-uuid tiebreak. With
    /// independent clocks (distinct random actors), same-wall-ms transitions
    /// could LWW-resolve in causally-inverted order.
    #[tokio::test]
    async fn test_shared_clock_orders_cross_path_records_monotonically() {
        use crate::observer::AfterMergeObserver as _;

        // Build one shared clock and hand a clone to a materializer (the actor
        // holds the other clone — modeled here by ticking it directly).
        let clock = new_signal_clock();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let materializer = crate::markers::materialize::RecordMaterializingObserver::new(
            tx,
            None,
            clock.clone(),
            None,
            None,
        );

        let ts = 1_700_000_000_000_i64;
        let ev = MergedEvent {
            space_id: uuid::Uuid::new_v4().to_string(),
            document_id: "doc".to_owned(),
            author_did: "did:key:zHuman".to_owned(),
            via_pat_id: None,
            op_count: 1,
            intent: "edit".to_owned(),
            content_mode: EditContentMode::Text,
            timestamp: ts,
        };

        // Materializer stamps a CREATED (path 1), sent on its channel …
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("test").expect("agent");
        doc.edit(agent, "test", "insert", Boundary::Explicit, |ctx| {
            ctx.insert(0, "## ? Ship it?")
        })
        .expect("edit");
        materializer.after_text_merge(ev, &doc.content()).await;
        let batch = rx.try_recv().expect("materialized batch");
        let mat_hlc = batch.records[0]
            .hlc
            .clone()
            .expect("materialized record has an hlc");

        // … then the actor stamps its own transition from the SAME clock (path
        // 2) at the same wall ms.
        let actor_hlc: sync::Hlc = clock
            .lock()
            .expect("clock lock")
            .tick(ts.max(0).cast_unsigned())
            .into();

        // Strictly increasing across the two paths: same actor bytes, and the
        // second stamp dominates the first. A random-actor tiebreak would give
        // the two records DIFFERENT actor bytes at the same (physical, logical).
        assert_eq!(
            mat_hlc.actor, actor_hlc.actor,
            "both paths stamp with the SAME clock actor (one shared clock)"
        );
        assert!(
            (actor_hlc.physical_ms, actor_hlc.logical) > (mat_hlc.physical_ms, mat_hlc.logical),
            "the later-issued cross-path record has the strictly greater HLC \
             (causal order, not a random tiebreak): mat={mat_hlc:?} actor={actor_hlc:?}"
        );
    }

    /// A batch queued on the unbounded materialize channel when the command
    /// channel closes must NEVER be lost — the observer's trackers already
    /// advanced past it, so a dropped batch permanently loses a user-authored
    /// signal. Enqueue a valid materialized record, close the command channel so
    /// `run()` exits, and assert the record landed in the durable store.
    ///
    /// The batch is handled by EITHER the live `select!` materialize branch (if
    /// it wins the race with the command-close) OR the post-loop drain — both
    /// leave the record on disk, so this stays green. WITHOUT the drain the test
    /// is flaky-RED: whenever the command-close branch wins the `select!` race
    /// the queued batch is lost (verified: ~40% failures with the drain removed).
    #[tokio::test]
    async fn test_run_drains_materialize_channel_on_exit() {
        use crate::markers::materialize::MaterializedBatch;

        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let dir = tempfile::TempDir::new().unwrap();
        relay.test_set_record_log(
            Some(Arc::new(crate::record_log::SegmentRecordLog::new(
                crate::signal_store::SignalStore::new(dir.path().to_path_buf()),
            ))),
            None,
        );

        let (cmd_tx, cmd_rx) = mpsc::channel::<RelayCommand>(4);
        let (mat_tx, mat_rx) = mpsc::unbounded_channel::<MaterializedBatch>();
        attach_run_channels(&mut relay, cmd_rx, mat_rx);

        // A fully-stamped materialized record: valid record_id + 16-byte-actor
        // HLC so it passes the well-formedness gate on the drain path.
        let space = uuid::Uuid::new_v4();
        let mut record = sync::Signal {
            id: "sig-1".into(),
            space_id: space.to_string(),
            document_id: Some("doc-1".into()),
            record_id: uuid::Uuid::new_v4().to_string(),
            hlc: Some(sync::Hlc {
                physical_ms: 1,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        record.set_event(sync::SignalEventType::Created);
        mat_tx
            .send(MaterializedBatch {
                space_id: space.to_string(),
                records: vec![record],
            })
            .expect("enqueue batch");

        // Close the command channel so `run()` breaks and reaches the drain.
        // The materialize sender stays alive (its batch is buffered), so the
        // drain is the ONLY thing that can process it.
        drop(cmd_tx);
        // `run()` consumes the relay; when it returns, its store writer is
        // dropped, releasing the segment LOCK for the fresh reader below.
        relay.run().await;

        // The queued batch was drained and appended after the loop exited: a
        // fresh reader over the same segment dir sees the record on disk.
        let loaded = crate::signal_store::SignalStore::new(dir.path().to_path_buf())
            .load(space)
            .expect("load segments");
        assert_eq!(
            loaded.records.len(),
            1,
            "the queued materialized batch must be drained + appended on clean exit"
        );
        assert_eq!(loaded.records[0].id, "sig-1");
    }
}
