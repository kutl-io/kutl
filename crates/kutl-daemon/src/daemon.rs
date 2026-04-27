//! Per-space sync worker: event loop orchestrating watcher, sync client, and documents.
//!
//! Each [`SpaceWorker`] bridges files on disk with the relay for a single space:
//! - Watching for local file changes → diffing into CRDT ops → sending to relay
//! - Receiving remote ops from relay → merging into CRDT → writing to disk

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use kutl_proto::protocol::{ABSOLUTE_BLOB_MAX, is_blob_mode};
use kutl_proto::sync::ChangeMetadata;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::SafeRelayPath;
use crate::blob_state::{BlobState, BlobStateMap, sha256_bytes, sha256_hex};
use crate::bridge;
use crate::client::{self, SyncCommand, SyncEvent};
use crate::documents::DocumentManager;
use crate::reconcile::{self, ReconcileInputs, StartupAction};
use crate::state::DaemonState;
use crate::watcher::{self, FileEvent, FileWatcher};

/// Outcome of a single session, used to decide whether to reconnect or exit.
enum SessionOutcome {
    /// Relay disconnected or channels closed — reconnect.
    Disconnected,
    /// Cancellation requested — shut down gracefully.
    Shutdown,
    /// Relay explicitly rejected authentication — do not retry.
    AuthRejected(String),
}

/// Maximum reconnect backoff delay.
const MAX_BACKOFF: Duration = kutl_core::std_duration(kutl_core::SignedDuration::from_secs(30));
/// Initial reconnect delay.
const INITIAL_BACKOFF: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_millis(500));
/// Channel capacity for sync events, commands, and file events.
const CHANNEL_CAPACITY: usize = 64;

/// Number of hex characters used for the CRDT agent name (48 bits of entropy).
const AGENT_NAME_HEX_LEN: usize = 12;

/// Idle timeout for one-shot sync mode: exit after this much inactivity.
const ONE_SHOT_IDLE_TIMEOUT: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(2));

/// Configuration for a per-space sync worker.
pub struct SpaceWorkerConfig {
    /// Path to the space root directory (should be canonicalized).
    pub space_root: PathBuf,
    /// DID identity of this peer.
    pub author_did: String,
    /// Relay WebSocket URL.
    pub relay_url: String,
    /// Space ID.
    pub space_id: String,
    /// Ed25519 signing key for relay authentication. `None` skips auth.
    pub signing_key: Option<ed25519_dalek::SigningKey>,
    /// If true, sync once and exit instead of running persistently.
    pub one_shot: bool,
    /// Self-declared display name sent in the handshake.
    pub display_name: String,
    /// Fired once the space worker is fully ready (connected, subscribed,
    /// watcher running). `None` in production; used by tests to avoid
    /// sleep-based waits.
    pub ready: Option<Arc<Notify>>,
    /// Cancellation token. When triggered, the worker shuts down gracefully.
    pub cancel: CancellationToken,
}

/// A per-space sync worker that orchestrates file watching, sync client, and
/// document management for a single space directory.
pub struct SpaceWorker {
    config: SpaceWorkerConfig,
    doc_manager: DocumentManager,
    /// Session-scoped CRDT agent name (short random alphanumeric string).
    ///
    /// Each daemon instance gets a unique agent name so that concurrent
    /// writers (even with the same DID) never collide on CRDT sequence
    /// numbers. The raw DID is preserved in `config.author_did` for
    /// metadata attribution.
    ///
    /// diamond-types limits agent names to 50 UTF-8 bytes.
    agent_name: String,
    /// Last synced version per document, for computing deltas.
    last_synced: HashMap<PathBuf, Vec<usize>>,
    /// Last-synced state for binary files (LWW).
    blob_state: BlobStateMap,
    /// Maps watched file paths to their (`document_uuid`, inode) for rename detection.
    /// When a file disappears and a new file appears with the same inode, the daemon
    /// recognizes it as a rename rather than a delete + create.
    file_identity: HashMap<PathBuf, FileIdentity>,
    /// Reverse mapping from UUID to relative path.
    uuid_to_path: HashMap<String, PathBuf>,
    /// Local daemon state cache persisted to `.kutl/state.json`.
    state: DaemonState,
}

/// Tracks the UUID and inode assigned to a file path in the relay registry.
///
/// The inode enables rename detection on platforms (e.g. macOS `FSEvents`)
/// that don't emit paired rename events. When a "new" file appears whose
/// inode matches an existing identity, the daemon treats it as a rename
/// rather than a delete + create.
#[derive(Debug, Clone)]
struct FileIdentity {
    /// UUID assigned to this document in the relay registry.
    document_uuid: String,
    /// Inode at registration time (None on non-Unix or if stat failed).
    inode: Option<u64>,
}

impl SpaceWorker {
    /// Create a new space worker from configuration.
    pub fn new(config: SpaceWorkerConfig) -> Result<Self> {
        let agent_name = generate_agent_name();
        let mut doc_manager = DocumentManager::new(config.space_root.clone())?;
        doc_manager.scan_existing()?;

        let blob_state = BlobStateMap::load(&config.space_root).unwrap_or_else(|e| {
            error!(error = %e, "failed to load blob state, starting fresh");
            BlobStateMap::default()
        });

        let kutl_dir = config.space_root.join(".kutl");
        let state = DaemonState::load(&kutl_dir);

        // Populate file_identity and uuid_to_path from cached state.
        let mut file_identity = HashMap::new();
        let mut uuid_to_path = HashMap::new();
        for (path_str, uuid) in &state.validated_documents() {
            let rel_path = PathBuf::from(path_str);
            let abs_path = config.space_root.join(&rel_path);
            let inode = crate::inode::get_inode(&abs_path);
            file_identity.insert(
                rel_path.clone(),
                FileIdentity {
                    document_uuid: uuid.clone(),
                    inode,
                },
            );
            uuid_to_path.insert(uuid.clone(), rel_path);
        }

        Ok(Self {
            config,
            doc_manager,
            agent_name,
            last_synced: HashMap::new(),
            blob_state,
            file_identity,
            uuid_to_path,
            state,
        })
    }

    /// Path to the `.kutl` directory for this space.
    fn kutl_dir(&self) -> PathBuf {
        self.config.space_root.join(".kutl")
    }

    /// Get the UUID for a file, or generate and register a new one.
    ///
    /// Records the inode for rename detection and persists the mapping.
    /// Local paths from the file watcher are validated through `SafeRelayPath`
    /// before storage — they should always pass since they're relative paths
    /// within the space root.
    fn get_or_create_uuid(&mut self, rel_path: &Path) -> String {
        if let Some(identity) = self.file_identity.get(rel_path) {
            return identity.document_uuid.clone();
        }

        let uuid = uuid::Uuid::new_v4().to_string();
        let safe_path = SafeRelayPath::new(&rel_path_to_string(rel_path))
            .expect("local file paths within space root must be valid");
        self.register_identity(safe_path, uuid.clone());
        uuid
    }

    /// Look up the relative path for a document UUID.
    fn path_for_uuid(&self, uuid: &str) -> Option<&PathBuf> {
        self.uuid_to_path.get(uuid)
    }

    /// Resolve the relative path for a document UUID, falling back to
    /// treating the document ID as a relay-supplied path (validated via
    /// [`SafeRelayPath`]) for backwards compatibility with pre-UUID documents.
    fn resolve_path(&self, document_id: &str) -> Result<PathBuf> {
        if let Some(path) = self.path_for_uuid(document_id) {
            return Ok(path.clone());
        }
        SafeRelayPath::new(document_id).map(SafeRelayPath::into_path_buf)
    }

    /// Register identity for a document (path ↔ UUID) and persist.
    ///
    /// Accepts a [`SafeRelayPath`] to ensure relay-supplied paths have been
    /// validated before entering the identity map.
    fn register_identity(&mut self, rel_path: SafeRelayPath, document_uuid: String) {
        let rel_path = rel_path.into_path_buf();
        let path_str = rel_path_to_string(&rel_path);
        let abs_path = self.config.space_root.join(&rel_path);
        let inode = crate::inode::get_inode(&abs_path);
        self.file_identity.insert(
            rel_path.clone(),
            FileIdentity {
                document_uuid: document_uuid.clone(),
                inode,
            },
        );
        self.state.documents.insert(path_str, document_uuid.clone());
        self.uuid_to_path.insert(document_uuid, rel_path);
        if let Err(e) = self.state.save(&self.kutl_dir()) {
            error!(error = %e, "failed to persist daemon state");
        }
    }

    /// Unregister identity for a document and persist.
    fn unregister_identity(&mut self, document_uuid: &str) {
        if let Some(rel_path) = self.uuid_to_path.remove(document_uuid) {
            self.file_identity.remove(&rel_path);
            self.state.documents.remove(&rel_path_to_string(&rel_path));
            if let Err(e) = self.state.save(&self.kutl_dir()) {
                error!(error = %e, "failed to persist daemon state");
            }
        }
    }

    /// Move identity from one path to another and persist.
    fn move_identity(&mut self, old_path: &Path, new_path: PathBuf, document_uuid: &str) {
        let new_path_str = rel_path_to_string(&new_path);
        let abs_new = self.config.space_root.join(&new_path);
        let inode = crate::inode::get_inode(&abs_new);
        self.file_identity.remove(old_path);
        self.file_identity.insert(
            new_path.clone(),
            FileIdentity {
                document_uuid: document_uuid.to_string(),
                inode,
            },
        );
        self.state.documents.remove(&rel_path_to_string(old_path));
        self.state
            .documents
            .insert(new_path_str, document_uuid.to_string());
        self.uuid_to_path
            .insert(document_uuid.to_string(), new_path);
        if let Err(e) = self.state.save(&self.kutl_dir()) {
            error!(error = %e, "failed to persist daemon state");
        }
    }

    /// Remove all local state for a document (last-synced, blob state, CRDT sidecar).
    fn cleanup_document_state(&mut self, rel_path: &Path) {
        self.last_synced.remove(rel_path);
        self.blob_state.remove(rel_path);
        self.doc_manager.remove(rel_path);
    }

    /// Write CRDT content to disk if it differs from the file.
    ///
    /// Used after rename operations to flush remote ops that were merged
    /// into the CRDT while the rename was in flight.
    async fn flush_crdt_if_stale(
        &self,
        rel_path: &Path,
        suppress_tx: &mpsc::Sender<PathBuf>,
    ) -> Result<()> {
        let Some(doc) = self.doc_manager.get(rel_path) else {
            return Ok(());
        };
        let crdt_content = doc.content();
        if crdt_content.is_empty() {
            return Ok(());
        }
        let abs_path = self.config.space_root.join(rel_path);
        if let Ok(file_content) = std::fs::read_to_string(&abs_path)
            && file_content != crdt_content
        {
            debug!(
                path = %rel_path.display(),
                "flushing CRDT content after rename"
            );
            let _ = suppress_tx.send(rel_path.to_owned()).await;
            std::fs::write(&abs_path, &crdt_content)?;
        }
        Ok(())
    }

    /// Build a `ChangeMetadata` with the daemon's author DID.
    fn make_metadata(&self, intent: &str) -> ChangeMetadata {
        ChangeMetadata {
            timestamp: kutl_core::now_ms(),
            author_did: self.config.author_did.clone(),
            intent: intent.into(),
            ..Default::default()
        }
    }

    /// Register a document with the relay and subscribe to it.
    async fn register_and_subscribe(
        &self,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        document_id: &str,
        path: &str,
        intent: &str,
    ) -> Result<()> {
        let meta = self.make_metadata(intent);
        sync_cmd_tx
            .send(SyncCommand::RegisterDocument {
                space_id: self.config.space_id.clone(),
                document_id: document_id.to_owned(),
                path: path.to_owned(),
                metadata: Some(meta),
            })
            .await?;
        sync_cmd_tx
            .send(SyncCommand::Subscribe {
                document_id: document_id.to_owned(),
            })
            .await?;
        Ok(())
    }

    /// Run the daemon event loop with automatic reconnection.
    ///
    /// Returns `Ok(())` on graceful shutdown (cancellation).
    pub async fn run(mut self) -> Result<()> {
        // Pre-flight: case-variant duplicates are bad state, not transient.
        // Run once before any session so we never enter the retry loop.
        if let Err(err) = crate::case_collision::detect_case_collisions(&self.config.space_root) {
            error!(
                space_id = %self.config.space_id,
                "{}",
                err.format_user_message()
            );
            return Err(err.into());
        }

        if self.config.one_shot {
            return self.run_once().await;
        }

        let mut backoff = INITIAL_BACKOFF;

        loop {
            match self.run_session(false).await {
                Ok(SessionOutcome::Disconnected) => {
                    info!("session ended cleanly, reconnecting");
                    backoff = INITIAL_BACKOFF;
                    tokio::select! {
                        () = tokio::time::sleep(INITIAL_BACKOFF) => {}
                        () = self.config.cancel.cancelled() => {
                            info!("cancelled during reconnect backoff");
                            return Ok(());
                        }
                    }
                }
                Ok(SessionOutcome::Shutdown) => {
                    info!("received shutdown signal");
                    return Ok(());
                }
                Ok(SessionOutcome::AuthRejected(msg)) => {
                    return Err(anyhow::anyhow!("authentication rejected by relay: {msg}"));
                }
                Err(e) => {
                    warn!(error = %e, ?backoff, "session failed, reconnecting");
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = self.config.cancel.cancelled() => {
                            info!("cancelled during reconnect backoff");
                            return Ok(());
                        }
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    /// Run a single sync pass: connect, push local state, pull remote state, exit.
    async fn run_once(&mut self) -> Result<()> {
        match self.run_session(true).await? {
            SessionOutcome::Shutdown => info!("sync complete"),
            SessionOutcome::Disconnected => warn!("relay disconnected before sync completed"),
            SessionOutcome::AuthRejected(msg) => {
                anyhow::bail!("authentication rejected by relay: {msg}");
            }
        }
        Ok(())
    }

    /// Resolve a bearer token from stored credentials or DID challenge-response.
    ///
    /// Stored file credentials are only used if their `relay_url` matches
    /// the current relay. This prevents stale tokens from a different relay
    /// causing infinite auth-rejection retry loops.
    async fn resolve_auth_token(&self) -> String {
        // Priority 1: KUTL_TOKEN env var (explicit override, always trusted).
        if let Ok(token) = std::env::var(kutl_client::credentials::TOKEN_ENV_VAR)
            && !token.is_empty()
        {
            info!("using token from environment");
            return token;
        }

        // Priority 2: stored credentials, only if relay URL matches.
        let creds_path = kutl_client::credentials::default_credentials_path().ok();
        if let Some(path) = creds_path.as_deref()
            && let Ok(Some(creds)) = kutl_client::credentials::StoredCredentials::load(path)
            && creds.relay_url == self.config.relay_url
        {
            info!("using stored token");
            return creds.token;
        }
        if let Some(ref signing_key) = self.config.signing_key {
            return kutl_client::authenticate(
                &self.config.relay_url,
                &self.config.author_did,
                signing_key,
            )
            .await
            .unwrap_or_else(|e| {
                error!(error = %e, "authentication failed, proceeding without token");
                String::new()
            });
        }
        String::new()
    }

    /// Run a single session: authenticate, connect, relay, return on disconnect.
    async fn run_session(&mut self, one_shot: bool) -> Result<SessionOutcome> {
        let auth_token = self.resolve_auth_token().await;

        let (sync_cmd_tx, sync_cmd_rx) = mpsc::channel::<SyncCommand>(CHANNEL_CAPACITY);
        let (sync_event_tx, mut sync_event_rx) = mpsc::channel::<SyncEvent>(CHANNEL_CAPACITY);
        let (file_event_tx, mut file_event_rx) = mpsc::channel::<FileEvent>(CHANNEL_CAPACITY);
        let (suppress_tx, suppress_rx) = mpsc::channel::<PathBuf>(CHANNEL_CAPACITY);

        // Spawn the WS client.
        let relay_url = self.config.relay_url.clone();
        let space_id = self.config.space_id.clone();
        let client_name = self.config.author_did.clone();
        let display_name = self.config.display_name.clone();
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client::run_client(
                &relay_url,
                &space_id,
                &client_name,
                &auth_token,
                &display_name,
                sync_cmd_rx,
                sync_event_tx,
            )
            .await
            {
                error!(error = %e, "sync client error");
            }
        });

        // Wait for connection before subscribing.
        match sync_event_rx.recv().await {
            Some(SyncEvent::Connected) => {
                info!("connected to relay");
            }
            Some(SyncEvent::AuthRejected(msg)) => {
                return Ok(SessionOutcome::AuthRejected(msg));
            }
            other => {
                anyhow::bail!("expected Connected event, got: {other:?}");
            }
        }

        // Discover remote documents, reconcile with local state, and execute.
        self.startup_reconciliation(&sync_cmd_tx, &suppress_tx, &mut sync_event_rx)
            .await?;

        // Scan all files on disk. For files already tracked by reconciliation,
        // this diffs file content against the CRDT and sends ops for any
        // offline edits. For new files, this registers and subscribes.
        //
        // This must run BEFORE processing remote catch-up so that local file
        // edits are applied to the CRDT against the pre-sync baseline. The
        // subsequent event loop merges remote ops, and diamond-types handles
        // the concurrent position transforms correctly.
        self.initial_file_scan(&sync_cmd_tx, &suppress_tx).await?;

        // Persist the full set of document UUIDs we know about so the next
        // session can detect documents that were unregistered while offline.
        self.state.remote_document_ids = self.uuid_to_path.keys().cloned().collect();
        if let Err(e) = self.state.save(&self.kutl_dir()) {
            error!(error = %e, "failed to persist remote document ids");
        }

        // Start file watcher (space_root is already canonical from caller).
        let mut watcher = FileWatcher::new(&self.config.space_root, file_event_tx, suppress_rx)?;
        let watcher_handle = tokio::spawn(async move {
            watcher.run().await;
        });

        if let Some(ref notify) = self.config.ready {
            notify.notify_one();
        }

        // Main event loop.
        let result = self
            .event_loop(
                &mut file_event_rx,
                &mut sync_event_rx,
                &sync_cmd_tx,
                &suppress_tx,
                one_shot,
            )
            .await;

        watcher_handle.abort();
        client_handle.abort();

        result
    }

    /// Core select loop processing file and sync events.
    ///
    /// In one-shot mode, exits after [`ONE_SHOT_IDLE_TIMEOUT`] of inactivity
    /// (no sync events received), indicating the initial sync exchange is complete.
    async fn event_loop(
        &mut self,
        file_event_rx: &mut mpsc::Receiver<FileEvent>,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<PathBuf>,
        one_shot: bool,
    ) -> Result<SessionOutcome> {
        // The idle timer is only polled when `one_shot` is true (via the select
        // guard). In persistent mode it's never polled, so no system timer is
        // registered and the cost is just the struct on the stack.
        let idle_timeout = tokio::time::sleep(ONE_SHOT_IDLE_TIMEOUT);
        tokio::pin!(idle_timeout);

        loop {
            tokio::select! {
                Some(file_event) = file_event_rx.recv() => {
                    if one_shot {
                        idle_timeout.as_mut().reset(tokio::time::Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
                    }
                    if let Err(e) = self.handle_file_event(file_event, sync_cmd_tx, suppress_tx).await {
                        error!(error = %e, "error handling file event");
                    }
                }
                Some(sync_event) = sync_event_rx.recv() => {
                    // Reset idle timer on each sync event.
                    if one_shot {
                        idle_timeout.as_mut().reset(tokio::time::Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
                    }
                    match sync_event {
                        SyncEvent::Disconnected => {
                            info!("disconnected from relay");
                            return Ok(SessionOutcome::Disconnected);
                        }
                        other => {
                            if let Err(e) = self.handle_sync_event(other, suppress_tx, sync_cmd_tx).await {
                                error!(error = %e, "error handling sync event");
                            }
                        }
                    }
                }
                () = &mut idle_timeout, if one_shot => {
                    info!("sync idle timeout, exiting");
                    return Ok(SessionOutcome::Shutdown);
                }
                () = self.config.cancel.cancelled() => {
                    return Ok(SessionOutcome::Shutdown);
                }
                else => break,
            }
        }
        Ok(SessionOutcome::Disconnected)
    }

    /// Gather inputs, run reconciliation, and execute the resulting actions.
    ///
    /// Replaces the previous scattered approach (inline `ListSpaceDocuments`
    /// handling, `reconcile_remotely_deleted`, local doc registration,
    /// `reconcile_missing_files`) with a single flow:
    ///
    /// 1. Fetch the relay's active document list
    /// 2. Build reconciliation inputs from three sources of truth
    /// 3. Call [`reconcile::reconcile_startup`] to produce actions
    /// 4. Execute each action in order
    async fn startup_reconciliation(
        &mut self,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<PathBuf>,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
    ) -> Result<()> {
        // Step 1: Fetch the relay's active document list.
        debug!(space_id = %self.config.space_id, "sending ListSpaceDocuments request");
        sync_cmd_tx
            .send(SyncCommand::ListSpaceDocuments {
                space_id: self.config.space_id.clone(),
            })
            .await?;

        let remote_active = self.wait_for_document_list(sync_event_rx).await?;

        // Step 2: Build reconciliation inputs.
        let state_entries: HashMap<PathBuf, String> = self
            .state
            .documents
            .iter()
            .map(|(k, v)| (PathBuf::from(k), v.clone()))
            .collect();
        let previously_remote: std::collections::HashSet<String> =
            self.state.remote_document_ids.iter().cloned().collect();

        let inputs = ReconcileInputs {
            state_entries: &state_entries,
            previously_remote: &previously_remote,
            remote_active: &remote_active,
            space_root: &self.config.space_root,
        };

        // Step 3: Produce actions.
        let actions = reconcile::reconcile_startup(&inputs);

        // Step 4: Execute actions.
        self.execute_reconcile_actions(&actions, sync_cmd_tx, suppress_tx)
            .await
    }

    /// Execute a list of reconciliation actions produced by the truth table.
    async fn execute_reconcile_actions(
        &mut self,
        actions: &[StartupAction],
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<PathBuf>,
    ) -> Result<()> {
        // Collect IDs to unregister after the loop (avoids borrow conflicts).
        let mut to_unregister = Vec::new();

        for action in actions {
            match action {
                StartupAction::SubscribeRemote { document_id, path } => {
                    info!(%document_id, path = %path, "subscribing to remote document");
                    if !self.uuid_to_path.contains_key(document_id) {
                        self.register_identity(path.clone(), document_id.clone());
                    }
                    sync_cmd_tx
                        .send(SyncCommand::Subscribe {
                            document_id: document_id.clone(),
                        })
                        .await?;
                }
                StartupAction::SyncLocal { document_id, path } => {
                    self.sync_local_document(document_id, path, sync_cmd_tx)
                        .await?;
                }
                StartupAction::SendUnregister { document_id, path } => {
                    info!(path = %path.display(), %document_id, "file deleted locally, unregistering");
                    self.cleanup_document_state(path);
                    let meta = self.make_metadata("file delete");
                    sync_cmd_tx
                        .send(SyncCommand::UnregisterDocument {
                            space_id: self.config.space_id.clone(),
                            document_id: document_id.clone(),
                            metadata: Some(meta),
                        })
                        .await?;
                    to_unregister.push(document_id.clone());
                }
                StartupAction::RenameLocal {
                    document_id,
                    old_path,
                    new_path,
                } => {
                    let new_path_buf = new_path.as_path().to_path_buf();
                    info!(
                        %document_id,
                        old = %old_path.display(),
                        new = %new_path,
                        "document renamed remotely, renaming locally"
                    );
                    let old_abs = self.config.space_root.join(old_path);
                    let new_abs = new_path.under(&self.config.space_root);
                    if let Some(parent) = new_abs.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let _ = suppress_tx.send(old_path.clone()).await;
                    let _ = suppress_tx.send(new_path_buf.clone()).await;
                    std::fs::rename(&old_abs, &new_abs).with_context(|| {
                        format!(
                            "failed to rename {} to {}",
                            old_abs.display(),
                            new_abs.display()
                        )
                    })?;

                    self.doc_manager.rename(old_path, new_path.as_path())?;
                    self.move_identity(old_path, new_path_buf.clone(), document_id);

                    // Sync at the new path.
                    self.sync_local_document(document_id, &new_path_buf, sync_cmd_tx)
                        .await?;
                }
                StartupAction::DeleteLocal { document_id, path } => {
                    info!(path = %path.display(), %document_id, "document unregistered remotely, deleting locally");
                    let abs_path = self.config.space_root.join(path);
                    if abs_path.exists() {
                        let _ = suppress_tx.send(path.clone()).await;
                        if let Err(e) = std::fs::remove_file(&abs_path) {
                            error!(path = %abs_path.display(), error = %e, "failed to delete file");
                        }
                    }
                    self.cleanup_document_state(path);
                    to_unregister.push(document_id.clone());
                }
                StartupAction::CleanupState { document_id, path } => {
                    info!(path = %path.display(), %document_id, "cleaning up stale state entry");
                    self.cleanup_document_state(path);
                    to_unregister.push(document_id.clone());
                }
            }
        }

        for document_id in &to_unregister {
            self.unregister_identity(document_id);
        }

        Ok(())
    }

    /// Wait for the `SpaceDocuments` response, returning UUID → validated path map.
    ///
    /// Validates each relay-supplied path through [`SafeRelayPath`], skipping
    /// documents with invalid paths (traversal, absolute, `.kutl` prefix).
    async fn wait_for_document_list(
        &self,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
    ) -> Result<HashMap<String, SafeRelayPath>> {
        loop {
            match sync_event_rx.recv().await {
                Some(SyncEvent::SpaceDocuments { documents, .. }) => {
                    let mut map = HashMap::with_capacity(documents.len());
                    for (doc_id, path) in documents {
                        match SafeRelayPath::new(&path) {
                            Ok(safe) => {
                                map.insert(doc_id, safe);
                            }
                            Err(e) => {
                                error!(%doc_id, "skipping document with invalid path: {e}");
                            }
                        }
                    }
                    return Ok(map);
                }
                Some(SyncEvent::Error(msg)) => {
                    anyhow::bail!("relay rejected document discovery: {msg}");
                }
                Some(SyncEvent::Disconnected) | None => {
                    anyhow::bail!("disconnected during document discovery");
                }
                Some(other) => {
                    warn!(?other, "unexpected event during document discovery");
                }
            }
        }
    }

    /// Register a local document with the relay, subscribe, and push CRDT ops.
    ///
    /// Sends all ops since the beginning — the relay deduplicates anything
    /// it already has. This ensures the relay has the full CRDT state for
    /// forwarding to other subscribers.
    async fn sync_local_document(
        &mut self,
        document_id: &str,
        path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        self.register_and_subscribe(
            sync_cmd_tx,
            document_id,
            &rel_path_to_string(path),
            "startup sync",
        )
        .await?;

        if let Some(doc) = self.doc_manager.get(path)
            && !doc.local_version().is_empty()
        {
            let (ops, metadata) = encode_delta(doc, &[]);
            sync_cmd_tx
                .send(SyncCommand::SendOps {
                    document_id: document_id.to_string(),
                    ops,
                    metadata,
                    content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                    content_hash: Vec::new(),
                })
                .await?;
            self.last_synced
                .insert(path.to_owned(), doc.local_version());
        }

        Ok(())
    }

    /// Walk the space directory and process all files. For files already
    /// tracked by reconciliation, diffs content against the CRDT. For new
    /// files, registers and subscribes.
    async fn initial_file_scan(
        &mut self,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<PathBuf>,
    ) -> Result<()> {
        let space_root = self.config.space_root.clone();

        let mut count = 0u32;

        // Collect paths first to avoid borrow conflicts with self.
        let paths: Vec<PathBuf> = walkdir::WalkDir::new(&space_root)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.file_type().is_file())
            .filter_map(|e| {
                let rel = e.path().strip_prefix(&space_root).ok()?.to_path_buf();
                if watcher::should_ignore(&rel) {
                    return None;
                }
                Some(rel)
            })
            .collect();

        for rel_path in paths {
            // Skip files that already have a CRDT document loaded AND whose
            // content matches the file on disk. Process files with offline
            // edits (content differs from CRDT).
            if let Some(doc) = self.doc_manager.get(&rel_path) {
                let abs_path = self.doc_manager.file_path(&rel_path);
                match std::fs::read_to_string(&abs_path) {
                    Ok(file_content) if file_content == doc.content() => continue,
                    _ => {} // File differs or can't be read — process it
                }
            }

            let event = FileEvent::Modified {
                rel_path: rel_path.clone(),
            };
            if let Err(e) = self
                .handle_file_event(event, sync_cmd_tx, suppress_tx)
                .await
            {
                error!(path = %rel_path.display(), error = %e, "initial scan: failed to process file");
            } else {
                count += 1;
            }
        }

        if count > 0 {
            info!(count, "initial scan: processed existing files");
        }

        Ok(())
    }

    /// Handle a local file change: diff → CRDT ops → send to relay.
    async fn handle_file_event(
        &mut self,
        event: FileEvent,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<PathBuf>,
    ) -> Result<()> {
        match event {
            FileEvent::Modified { rel_path } => {
                self.handle_file_modified(&rel_path, sync_cmd_tx, suppress_tx)
                    .await
            }
            FileEvent::Removed { rel_path } => {
                self.handle_file_removed(&rel_path, sync_cmd_tx).await
            }
            FileEvent::Renamed { old_path, new_path } => {
                self.handle_file_renamed(&old_path, &new_path, sync_cmd_tx, suppress_tx)
                    .await
            }
        }
    }

    /// Handle a modified or newly created file.
    async fn handle_file_modified(
        &mut self,
        rel_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<PathBuf>,
    ) -> Result<()> {
        let abs_path = self.doc_manager.file_path(rel_path);
        let content = match std::fs::read_to_string(&abs_path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                return self.handle_blob_change(rel_path, sync_cmd_tx).await;
            }
            Err(e) => {
                return Err(e).with_context(|| format!("failed to read {}", abs_path.display()));
            }
        };

        // Inode-based rename detection: if this path is unknown but its inode
        // matches an existing identity, treat it as a rename. This handles
        // platforms (macOS FSEvents) that don't emit paired rename events.
        if !self.file_identity.contains_key(rel_path)
            && let Some(new_inode) = crate::inode::get_inode(&abs_path)
        {
            let match_found = self
                .file_identity
                .iter()
                .find(|(_, id)| id.inode == Some(new_inode))
                .map(|(old_path, id)| (old_path.clone(), id.document_uuid.clone()));

            if let Some((old_path, document_id)) = match_found {
                info!(?old_path, ?rel_path, "detected rename via inode match");

                self.move_identity(&old_path, rel_path.to_owned(), &document_id);
                if let Some(version) = self.last_synced.remove(&old_path) {
                    self.last_synced.insert(rel_path.to_owned(), version);
                }
                self.doc_manager.rename(&old_path, rel_path)?;

                let meta = self.make_metadata("file rename");
                sync_cmd_tx
                    .send(SyncCommand::RenameDocument {
                        space_id: self.config.space_id.clone(),
                        document_id,
                        old_path: rel_path_to_string(&old_path),
                        new_path: rel_path_to_string(rel_path),
                        metadata: Some(meta),
                    })
                    .await?;

                // Flush CRDT content if remote ops were merged mid-rename.
                self.flush_crdt_if_stale(rel_path, suppress_tx).await?;

                return Ok(());
            }
        }

        let is_new = self.doc_manager.get(rel_path).is_none();
        if is_new {
            let tracked: Vec<PathBuf> = self.doc_manager.known_documents();
            if let Some(existing) = crate::case_collision::find_case_variant(
                rel_path,
                tracked.iter().map(PathBuf::as_path),
            ) {
                error!(
                    new_path = %rel_path.display(),
                    existing_path = %existing.display(),
                    "case_collision_rejected: new file would collide with tracked document, skipping"
                );
                return Ok(());
            }
        }
        let document_id = self.get_or_create_uuid(rel_path);

        // Register and subscribe if this is a new file.
        if is_new {
            self.register_and_subscribe(
                sync_cmd_tx,
                &document_id,
                &rel_path_to_string(rel_path),
                "file change",
            )
            .await?;
        }

        let doc = self.doc_manager.load_or_create(rel_path)?;
        let agent = doc.register_agent(&self.agent_name)?;

        let version_before = doc.local_version();

        let author_did = self.config.author_did.clone();
        bridge::apply_file_change(doc, agent, &author_did, &content)?;

        let version_after = doc.local_version();
        if version_before == version_after {
            return Ok(());
        }

        let since = self
            .last_synced
            .get(rel_path)
            .map_or_else(Vec::new, Clone::clone);
        let (ops, metadata) = encode_delta(doc, &since);

        sync_cmd_tx
            .send(SyncCommand::SendOps {
                document_id,
                ops,
                metadata,
                content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                content_hash: Vec::new(),
            })
            .await?;

        self.last_synced.insert(rel_path.to_owned(), version_after);
        self.doc_manager.save(rel_path)?;

        Ok(())
    }

    /// Handle a file rename detected by the watcher.
    async fn handle_file_renamed(
        &mut self,
        old_path: &Path,
        new_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
        suppress_tx: &mpsc::Sender<PathBuf>,
    ) -> Result<()> {
        info!(?old_path, ?new_path, "file renamed");

        let old_path_str = rel_path_to_string(old_path);
        let new_path_str = rel_path_to_string(new_path);

        // Look up the UUID for the old path; if unknown, treat as a new file.
        let Some(identity) = self.file_identity.get(old_path).cloned() else {
            return self
                .handle_file_modified(new_path, sync_cmd_tx, suppress_tx)
                .await;
        };
        let document_id = identity.document_uuid;

        // Reject rename-to-collide: if new_path is a case-variant of another
        // tracked document (not the source), refuse to propagate. The
        // filesystem holds both files; we will not sync the rename target
        // until the user resolves the collision.
        let tracked: Vec<PathBuf> = self.doc_manager.known_documents();
        let tracked_iter = tracked
            .iter()
            .filter(|p| p.as_path() != old_path)
            .map(PathBuf::as_path);
        if let Some(existing) = crate::case_collision::find_case_variant(new_path, tracked_iter) {
            error!(
                old_path = %old_path.display(),
                new_path = %new_path.display(),
                existing_path = %existing.display(),
                "case_collision_rejected: rename target would collide with tracked document, ignoring event"
            );
            return Ok(());
        }

        self.move_identity(old_path, new_path.to_owned(), &document_id);

        // Move last_synced version from old to new path.
        if let Some(version) = self.last_synced.remove(old_path) {
            self.last_synced.insert(new_path.to_owned(), version);
        }

        // Move the CRDT sidecar to the new path.
        if let Err(e) = self.doc_manager.rename(old_path, new_path) {
            error!(error = %e, "failed to rename document sidecar");
        }

        // Send RenameDocument to relay.
        let meta = self.make_metadata("file rename");
        sync_cmd_tx
            .send(SyncCommand::RenameDocument {
                space_id: self.config.space_id.clone(),
                document_id,
                old_path: old_path_str,
                new_path: new_path_str,
                metadata: Some(meta),
            })
            .await?;

        // Flush CRDT content if remote ops were merged mid-rename.
        self.flush_crdt_if_stale(new_path, suppress_tx).await?;

        Ok(())
    }

    /// Handle a file removal.
    async fn handle_file_removed(
        &mut self,
        rel_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        // If we have an identity, send UnregisterDocument.
        if let Some(identity) = self.file_identity.get(rel_path).cloned() {
            let document_id = identity.document_uuid;
            self.unregister_identity(&document_id);

            let meta = self.make_metadata("file delete");
            sync_cmd_tx
                .send(SyncCommand::UnregisterDocument {
                    space_id: self.config.space_id.clone(),
                    document_id,
                    metadata: Some(meta),
                })
                .await?;
        }

        Ok(())
    }

    /// Handle a sync event from the relay: merge remote ops → write to disk.
    async fn handle_sync_event(
        &mut self,
        event: SyncEvent,
        suppress_tx: &mpsc::Sender<PathBuf>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        match event {
            SyncEvent::RemoteOps {
                document_id,
                ops,
                metadata,
                content_mode,
                content_hash,
            } => {
                // Build a temporary SyncOps to check blob mode.
                let check = kutl_proto::sync::SyncOps {
                    content_mode,
                    ..Default::default()
                };
                if is_blob_mode(&check) {
                    self.handle_remote_blob(&document_id, ops, metadata, content_hash, suppress_tx)
                        .await?;
                } else {
                    self.handle_remote_text(&document_id, ops, metadata, suppress_tx, sync_cmd_tx)
                        .await?;
                }
            }
            SyncEvent::DocumentRegistered { document_id, path } => {
                let safe_path = match SafeRelayPath::new(&path) {
                    Ok(p) => p,
                    Err(e) => {
                        error!(%document_id, "ignoring document registration with invalid path: {e}");
                        return Ok(());
                    }
                };
                info!(%document_id, %path, "remote document registered");
                if !self.uuid_to_path.contains_key(&document_id) {
                    self.register_identity(safe_path, document_id.clone());
                    sync_cmd_tx
                        .send(SyncCommand::Subscribe { document_id })
                        .await
                        .context("failed to subscribe to newly registered document")?;
                }
            }
            SyncEvent::DocumentRenamed {
                document_id,
                old_path,
                new_path,
            } => {
                let safe_old = match SafeRelayPath::new(&old_path) {
                    Ok(p) => p,
                    Err(e) => {
                        error!(%document_id, "ignoring document rename with invalid old path: {e}");
                        return Ok(());
                    }
                };
                let safe_new = match SafeRelayPath::new(&new_path) {
                    Ok(p) => p,
                    Err(e) => {
                        error!(%document_id, "ignoring document rename with invalid new path: {e}");
                        return Ok(());
                    }
                };
                self.handle_remote_rename(
                    &document_id,
                    &safe_old,
                    &safe_new,
                    suppress_tx,
                    sync_cmd_tx,
                )
                .await?;
            }
            SyncEvent::DocumentUnregistered { document_id } => {
                info!(%document_id, "remote document unregistered");

                // Delete the file from disk and clean up CRDT state.
                if let Some(rel_path) = self.uuid_to_path.get(&document_id).cloned() {
                    let abs_path = self.config.space_root.join(&rel_path);
                    if abs_path.exists() {
                        let _ = suppress_tx.send(rel_path.clone()).await;
                        if let Err(e) = std::fs::remove_file(&abs_path) {
                            error!(path = %abs_path.display(), error = %e, "failed to delete unregistered file");
                        }
                    }
                    self.cleanup_document_state(&rel_path);
                }

                self.unregister_identity(&document_id);
            }
            SyncEvent::SpaceDocuments { .. } | SyncEvent::Connected | SyncEvent::Disconnected => {
                // Only expected during session setup; ignore if received later.
            }
            SyncEvent::AuthRejected(msg) | SyncEvent::Error(msg) => {
                error!(msg, "relay error");
            }
        }

        Ok(())
    }

    /// Handle a remote document rename: incorporate pending edits, move
    /// identity + CRDT sidecar, rename file on disk.
    ///
    /// Both paths are pre-validated [`SafeRelayPath`] references, ensuring
    /// the relay cannot trick the daemon into writing outside the space root.
    async fn handle_remote_rename(
        &mut self,
        document_id: &str,
        old_path: &SafeRelayPath,
        new_path: &SafeRelayPath,
        suppress_tx: &mpsc::Sender<PathBuf>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        info!(%document_id, %old_path, %new_path, "remote document renamed");
        let old_rel = old_path.as_path().to_path_buf();
        let new_rel = new_path.as_path().to_path_buf();

        // Incorporate any pending local edits at the old path before
        // renaming — once we rename, the old-path watcher event would
        // find nothing and the edits would be lost.
        let abs_old = old_path.under(&self.config.space_root);
        if let Ok(doc) = self.doc_manager.load_or_create(&old_rel)
            && let Some((ops, meta)) = incorporate_pending_edits(
                &abs_old,
                &old_rel,
                doc,
                &self.agent_name,
                &self.config.author_did,
            )?
        {
            sync_cmd_tx
                .send(SyncCommand::SendOps {
                    document_id: document_id.to_owned(),
                    ops,
                    metadata: meta,
                    content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                    content_hash: Vec::new(),
                })
                .await
                .context("failed to push pending edit ops before rename")?;
            self.last_synced
                .insert(old_rel.clone(), doc.local_version());
            self.doc_manager.save(&old_rel)?;
        }

        self.move_identity(&old_rel, new_rel.clone(), document_id);

        // Move the CRDT sidecar and in-memory doc entry.
        if let Err(e) = self.doc_manager.rename(&old_rel, &new_rel) {
            error!(error = %e, "failed to rename document sidecar");
        }

        // Move last_synced.
        if let Some(v) = self.last_synced.remove(&old_rel) {
            self.last_synced.insert(new_rel.clone(), v);
        }

        // Rename the file on disk if it hasn't been renamed already.
        let abs_new = new_path.under(&self.config.space_root);
        if abs_old.exists() && !abs_new.exists() {
            if let Some(parent) = abs_new.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = suppress_tx.send(old_rel).await;
            let _ = suppress_tx.send(new_rel).await;
            if let Err(e) = std::fs::rename(&abs_old, &abs_new) {
                error!(error = %e, "failed to apply remote rename to disk");
            }
        }

        Ok(())
    }

    /// Handle remote text CRDT ops: merge → write to disk.
    ///
    /// Before merging remote ops, checks whether the file on disk has
    /// pending local edits (written by the user but not yet processed
    /// by the watcher). If so, applies them to the CRDT *first* — this
    /// ensures the local edit positions are computed against the base
    /// state, and the subsequent remote merge handles concurrent position
    /// transforms correctly via the CRDT engine.
    async fn handle_remote_text(
        &mut self,
        document_id: &str,
        ops: Vec<u8>,
        metadata: Vec<ChangeMetadata>,
        suppress_tx: &mpsc::Sender<PathBuf>,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        // Empty ops = document exists but has no content yet. Ensure the
        // file exists on disk (may be a zero-byte file).
        if ops.is_empty() {
            let rel_path = self.resolve_path(document_id)?;
            let abs_path = self.doc_manager.file_path(&rel_path);
            if !abs_path.exists() {
                if let Some(parent) = abs_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let _ = suppress_tx.send(rel_path).await;
                std::fs::write(&abs_path, "")
                    .with_context(|| format!("failed to create empty {}", abs_path.display()))?;
            }
            return Ok(());
        }

        // Look up the UUID→path mapping, falling back to treating the ID as a path
        // for backwards compatibility with pre-UUID documents.
        let rel_path = self.resolve_path(document_id)?;
        let abs_path = self.doc_manager.file_path(&rel_path);

        let doc = self.doc_manager.load_or_create(&rel_path)?;

        // Incorporate pending local edits BEFORE merging remote ops so
        // the CRDT engine handles concurrent position transforms.
        let local_ops_to_push = incorporate_pending_edits(
            &abs_path,
            &rel_path,
            doc,
            &self.agent_name,
            &self.config.author_did,
        )?;

        // Merge remote ops.
        let version_before_merge = doc.local_version();
        doc.merge(&ops, &metadata)?;
        let version_after_merge = doc.local_version();

        let state_changed =
            local_ops_to_push.is_some() || version_after_merge != version_before_merge;

        // Push local edit ops to the relay so other clients see them.
        if let Some((local_ops, local_meta)) = local_ops_to_push {
            sync_cmd_tx
                .send(SyncCommand::SendOps {
                    document_id: document_id.to_owned(),
                    ops: local_ops,
                    metadata: local_meta,
                    content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                    content_hash: Vec::new(),
                })
                .await?;
        }

        // Only write the file if the state actually changed.
        // Redundant merges (e.g. receiving our own ops back via catch-up)
        // must NOT write + suppress, or we risk swallowing a concurrent
        // local file edit's watcher event.
        if state_changed {
            let content = doc.content();

            // If the file is gone from its expected path but we had a known
            // inode, the user probably renamed or deleted it locally. Don't
            // recreate the file — the watcher event will handle the new path.
            // We still merge ops into the CRDT above so the content is current.
            if !abs_path.exists()
                && let Some(identity) = self.file_identity.get(&rel_path)
                && identity.inode.is_some()
            {
                debug!(
                    path = %rel_path.display(),
                    "skipping write: file gone but has known inode (likely renamed)"
                );
                self.last_synced
                    .insert(rel_path.clone(), doc.local_version());
                self.doc_manager.save(&rel_path)?;
                return Ok(());
            }

            if let Some(parent) = abs_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let _ = suppress_tx.send(rel_path.clone()).await;

            std::fs::write(&abs_path, &content)
                .with_context(|| format!("failed to write {}", abs_path.display()))?;
        }

        self.last_synced
            .insert(rel_path.clone(), doc.local_version());
        self.doc_manager.save(&rel_path)?;

        Ok(())
    }

    /// Handle a local binary file change: hash, compare, send to relay.
    async fn handle_blob_change(
        &mut self,
        rel_path: &Path,
        sync_cmd_tx: &mpsc::Sender<SyncCommand>,
    ) -> Result<()> {
        let abs_path = self.doc_manager.file_path(rel_path);
        let bytes = match std::fs::read(&abs_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(e).with_context(|| format!("failed to read {}", abs_path.display()));
            }
        };

        if bytes.len() > ABSOLUTE_BLOB_MAX {
            warn!(
                path = %abs_path.display(),
                size = bytes.len(),
                limit = ABSOLUTE_BLOB_MAX,
                "blob too large, skipping"
            );
            return Ok(());
        }

        let hash_hex = sha256_hex(&bytes);

        // Skip if hash unchanged.
        if let Some(existing) = self.blob_state.get(rel_path)
            && existing.hash == hash_hex
        {
            return Ok(());
        }

        let is_new = self.blob_state.get(rel_path).is_none();
        let document_id = self.get_or_create_uuid(rel_path);

        // Register and subscribe if new document.
        if is_new {
            self.register_and_subscribe(
                sync_cmd_tx,
                &document_id,
                &rel_path_to_string(rel_path),
                "file change",
            )
            .await?;
        }

        let hash_bytes = sha256_bytes(&bytes);
        let metadata = self.make_metadata("file change");
        let timestamp = metadata.timestamp;

        #[allow(clippy::cast_possible_wrap)]
        let size = bytes.len() as i64;

        sync_cmd_tx
            .send(SyncCommand::SendOps {
                document_id,
                ops: bytes,
                metadata: vec![metadata],
                content_mode: i32::from(kutl_proto::sync::ContentMode::Blob),
                content_hash: hash_bytes,
            })
            .await?;

        self.blob_state.insert(
            rel_path.to_owned(),
            BlobState {
                hash: hash_hex,
                timestamp,
                size,
            },
        );
        self.blob_state.save(&self.config.space_root)?;

        Ok(())
    }

    /// Handle a remote binary blob: LWW by timestamp, write to disk.
    async fn handle_remote_blob(
        &mut self,
        document_id: &str,
        ops: Vec<u8>,
        metadata: Vec<ChangeMetadata>,
        _content_hash: Vec<u8>,
        suppress_tx: &mpsc::Sender<PathBuf>,
    ) -> Result<()> {
        // Empty ops are a catch-up signal for documents with no content yet.
        if ops.is_empty() {
            return Ok(());
        }

        let rel_path = self.resolve_path(document_id)?;
        let abs_path = self.doc_manager.file_path(&rel_path);

        let remote_timestamp = metadata.first().map_or(0, |m| m.timestamp);

        // LWW: only accept if remote is newer.
        if let Some(existing) = self.blob_state.get(&rel_path)
            && remote_timestamp <= existing.timestamp
        {
            return Ok(());
        }

        if let Some(parent) = abs_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let _ = suppress_tx.send(rel_path.clone()).await;

        std::fs::write(&abs_path, &ops)
            .with_context(|| format!("failed to write {}", abs_path.display()))?;

        let hash_hex = sha256_hex(&ops);
        #[allow(clippy::cast_possible_wrap)]
        let size = ops.len() as i64;

        self.blob_state.insert(
            rel_path,
            BlobState {
                hash: hash_hex,
                timestamp: remote_timestamp,
                size,
            },
        );
        self.blob_state.save(&self.config.space_root)?;

        Ok(())
    }
}

/// Convert a relative path to a forward-slash string for use in wire messages.
///
/// This is the same logic as the deprecated `path_to_document_id`, but is used
/// internally for path→string in lifecycle message payloads (not as document identity).
fn rel_path_to_string(rel_path: &Path) -> String {
    rel_path
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Encode ops and metadata as a delta since the given version.
fn encode_delta(doc: &kutl_core::Document, since: &[usize]) -> (Vec<u8>, Vec<ChangeMetadata>) {
    let ops = doc.encode_since(since);
    let metadata = doc.changes_since(since);
    (ops, metadata)
}

/// Incorporate pending local file edits into the CRDT.
///
/// Reads the file at `abs_path`, diffs against the CRDT content, and
/// applies any changes. Returns `(ops, metadata)` for the delta, or
/// `None` if the file matches the CRDT or doesn't exist on disk.
fn incorporate_pending_edits(
    abs_path: &Path,
    rel_path: &Path,
    doc: &mut kutl_core::Document,
    agent_name: &str,
    author_did: &str,
) -> Result<Option<(Vec<u8>, Vec<ChangeMetadata>)>> {
    if !abs_path.exists() {
        return Ok(None);
    }
    let Ok(file_content) = std::fs::read_to_string(abs_path) else {
        return Ok(None);
    };
    if file_content == doc.content() {
        return Ok(None);
    }

    debug!(path = %rel_path.display(), "incorporating pending local edit");
    let version_before = doc.local_version();
    let agent = doc.register_agent(agent_name)?;
    bridge::apply_file_change(doc, agent, author_did, &file_content)?;

    let (ops, meta) = encode_delta(doc, &version_before);
    if ops.is_empty() {
        Ok(None)
    } else {
        Ok(Some((ops, meta)))
    }
}

/// Generate a short random agent name for CRDT sessions.
///
/// Uses the first 12 hex characters of a v4 UUID (48 bits of entropy)
/// — more than enough to avoid collisions across concurrent daemon
/// sessions.
fn generate_agent_name() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..AGENT_NAME_HEX_LEN].to_owned()
}
