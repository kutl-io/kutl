//! Doc-load / subscribe / catch-up / eviction family of the relay actor:
//! the per-document slot cache types ([`DocKey`], [`DocSlot`], [`DocContent`]),
//! backend cold-load classification ([`LoadResult`]), subscribe/unsubscribe
//! handling with catch-up construction, and the lane-overflow eviction rail
//! with its own-ack-lane `StaleSubscriber` notice.
//!
//! Child module of the relay actor (`super`) so the `impl Relay` block here
//! reaches the actor's private fields directly. `process_command` in
//! relay.rs routes subscribe/unsubscribe to these handlers, and the
//! flush/snippet/sync-ops paths reach the slot types and
//! [`relay_and_evict`] through `super`.

use std::collections::HashMap;

use kutl_core::lattice::{ContentKind, ContentState, EvictionState, SubscriberMap};
use kutl_proto::sync::{self, ErrorCode};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::load_class::{LoadClass, LoadOutcome, classify_load};
use crate::protocol::{
    blob_ops_envelope, encode_envelope, refusal, stale_subscriber_envelope,
    subscribe_status_envelope, sync_ops_envelope,
};
use crate::registry;
use crate::registry_store::RegistryBackend;

use super::{BLOB_KNOWN, ConnChannels, ConnId, Relay};

/// Key identifying a document within the relay.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct DocKey {
    pub(super) space_id: String,
    pub(super) document_id: String,
}

/// Accumulated edit event metadata held during the snippet debounce window.
///
/// Created on the first snippet-eligible edit, updated on subsequent edits
/// by the same author, and consumed when the debounce timer fires or a
/// different author edits the same document.
pub(super) struct PendingEdit {
    /// DID of the author whose edits are being accumulated.
    pub(super) author_did: String,
    /// Token that author authenticated with, when they used one.
    ///
    /// Part of the identity a burst accumulates under, not a passenger on
    /// it: a person and their agent share a DID, so a burst keyed on the DID
    /// alone can mix edits from both and then stamp one credential on all of
    /// them. Whichever it stamped would be wrong for half.
    pub(super) via_pat_id: Option<String>,
    /// Free-form intent string (latest edit wins).
    pub(super) intent: String,
    /// Total number of ops accumulated in this burst.
    pub(super) op_count: usize,
    /// Timestamp of the first edit in the burst (millis since epoch).
    pub(super) timestamp: i64,
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

/// Result of a subscribe cold-load after classification.
///
/// `Found` carries the populated `DocSlot`. The other variants carry
/// enough metadata for the subscribe path to report the classification
/// to the client and
/// for the relay's structured log line to include the expected version
/// or failure reason for observability.
pub(super) enum LoadResult {
    /// Backend produced bytes (and they decoded). Caller inserts this
    /// slot into `documents` and proceeds with a normal subscribe.
    ///
    /// Boxed so the enum size isn't dominated by the populated-slot
    /// case — a populated `DocSlot` is ~240 bytes.
    Found(Box<DocSlot>),
    /// Genuinely first-time document: backends returned no data and the
    /// durability witness says nothing was ever persisted. Safe to open
    /// as a writable empty editor.
    Empty,
    /// The durability witness claims prior content at
    /// `expected_version`, but no backend returned bytes. Content was
    /// lost between a successful flush and the current load — the
    /// "persisted-then-lost" failure mode the hard edit guard must
    /// refuse to paper over.
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
pub(super) struct DocSlot {
    pub(super) subscribers: SubscriberMap,
    /// Document content. Read access is direct (allows split borrows with
    /// `subscribers`). Mutations must go through [`mutate_text`] or
    /// [`set_blob`] which handle dirty tracking.
    pub(super) content: DocContent,
    /// Monotonic edit/flush counters tracking dirty status.
    /// Private — only mutated via [`mutate_text`], [`set_blob`], and
    /// [`clear_dirty`]. Read via [`is_dirty`].
    pub(super) content_state: ContentState,
    /// Declarative eviction state (replaces timer-based eviction).
    pub(super) eviction: EvictionState,
    /// Pending edit event held during snippet debounce. `None` when no
    /// snippet-eligible edit is waiting.
    pub(super) pending_edit: Option<PendingEdit>,
    /// Debounce timer handle — aborted and replaced on each new edit.
    pub(super) snippet_timer: Option<tokio::task::JoinHandle<()>>,
    /// Timestamp + author DID of most recent merge that hasn't yet
    /// been flushed to `edited_at` in the registry backend. Set on
    /// every merge, cleared by `flush_pending_edit_for`. Separate
    /// from `pending_edit` so snippet-ineligible edits still surface
    /// the write.
    ///
    /// The DID is carried alongside the timestamp so a registry
    /// backend that mirrors documents to a database (kutlhub's hosted
    /// relay does) can patch its last-editor column in the same
    /// flush. Last writer wins within a debounce window.
    pub(super) edited_at_pending: Option<EditedAtPending>,
}

/// Per-slot pending `edited_at` flush state: bundles the timestamp
/// and author DID so a mirroring registry backend can patch its
/// last-editor column alongside the edit timestamp in a single
/// `RegistryBackend::update_edited_at` call.
#[derive(Clone, Debug)]
pub(super) struct EditedAtPending {
    pub(super) timestamp: i64,
    pub(super) author_did: String,
}

impl DocSlot {
    /// Create an empty, clean slot with no subscribers.
    pub(super) fn empty() -> Self {
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
    pub(super) fn ensure_text(&mut self) -> bool {
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
    pub(super) fn mutate_text<F, T, E>(&mut self, f: F) -> Result<T, E>
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
    pub(super) fn set_blob(&mut self, blob: BlobData) {
        self.content = DocContent::Blob(blob);
        self.content_state
            .set_kind(ContentKind::Blob, kutl_core::now_ms_u64());
        self.content_state.record_edit();
    }

    /// Whether content has been mutated since last flush.
    pub(super) fn is_dirty(&self) -> bool {
        self.content_state.is_dirty()
    }
}

/// Content stored for a document — either CRDT text or a binary blob.
pub(super) enum DocContent {
    /// No content yet — first subscriber hasn't sent ops.
    Empty,
    /// Text document backed by a DT CRDT.
    Text(Box<kutl_core::Document>),
    /// Binary blob (LWW by timestamp).
    Blob(BlobData),
}

/// Last-write-wins binary blob.
pub(super) struct BlobData {
    pub(super) content: Vec<u8>,
    pub(super) hash: Vec<u8>,
    pub(super) timestamp: i64,
}

impl Relay {
    /// Ensure a document is in the in-memory slot cache. If the slot is
    /// missing, attempts to load from storage backends (content, then blob)
    /// and inserts it into `self.documents` on success. Returns `true` when
    /// the slot exists after the call.
    ///
    /// Both the MCP `read_document` path and the HTTP `text_export` path
    /// use this so the fallback-on-cache-miss behaviour is DRY.
    pub(super) async fn ensure_doc_loaded(&mut self, key: &DocKey) -> bool {
        if self.documents.contains_key(key) {
            return true;
        }
        // Only `Found` populates the slot here. `Empty` / `Inconsistent`
        // / `Unavailable` callers (MCP `read_document` and HTTP
        // `text_export`) all treat this as "no slot available" — the
        // subscribe path is where the classification is surfaced to the
        // client end-to-end.
        if let LoadResult::Found(slot) = self
            .load_from_backends(&key.space_id, &key.document_id)
            .await
        {
            self.documents.insert(key.clone(), *slot);
            return true;
        }
        false
    }

    /// Hydrate the in-memory slot for a document before a relay-applied text
    /// edit (an MCP edit, or the decision-marker flip). A no-op when the slot
    /// is already resident.
    ///
    /// Loading FIRST is what lets the blob guard fire: without it the edit
    /// path would `or_insert` a fresh empty text slot for an evicted blob and
    /// silently overwrite the binary content with text. The load is
    /// classified rather than discarded: `Found` inserts the slot (a blob is
    /// then refused by the caller's text guard), a genuinely `Empty` doc is
    /// safe to write as text, but `Inconsistent`/`Unavailable` mean an
    /// evicted blob cannot be ruled out — fail closed with the reason.
    pub(super) async fn hydrate_doc_for_edit(
        &mut self,
        space_id: &str,
        doc_id: &str,
    ) -> Result<(), String> {
        let key = DocKey {
            space_id: space_id.to_owned(),
            document_id: doc_id.to_owned(),
        };
        if self.documents.contains_key(&key) {
            return Ok(());
        }
        match self.load_from_backends(space_id, doc_id).await {
            LoadResult::Found(slot) => {
                self.documents.insert(key, *slot);
                Ok(())
            }
            LoadResult::Empty => Ok(()),
            LoadResult::Inconsistent { .. } | LoadResult::Unavailable { .. } => {
                Err("document could not be loaded for editing; storage is \
                 temporarily unavailable or its content is inconsistent — retry"
                    .to_owned())
            }
        }
    }

    /// Load classification. Returned by [`load_from_backends`]
    /// so the subscribe path can distinguish genuinely empty from
    /// "we lost something we previously persisted" from "we can't reach
    /// the backend right now." The hard edit guard gates
    /// editor writability on this classification.
    pub(super) async fn load_from_backends(&mut self, space_id: &str, doc_id: &str) -> LoadResult {
        // Witness lookup first. If the relay has no registry backend (a
        // standalone test actor or the in-memory test relay), the
        // default impl returns the zero witness — which classifies any
        // `Ok(None)` as `Empty`, matching the in-memory registry model.
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
    /// The log line for `Inconsistent` and `Unavailable` is consumed by
    /// downstream monitoring filters — **do NOT change those message
    /// strings without coordinating with the alarm regex maintained
    /// out-of-tree.**
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
                // The persisted blob is a content envelope (oplog + the durable
                // change-metadata snapshot). Legacy bare oplogs decode to
                // `{ oplog: <whole blob>, metadata: None }`, so `changes` is
                // empty and the load is content-only.
                let decoded = kutl_core::decode_content_envelope(&data);
                let changes = decoded
                    .metadata
                    .as_ref()
                    .map_or(&[][..], |m| m.changes.as_slice());
                if let Err(e) = doc.merge(&decoded.oplog, changes) {
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
                // Recover the uncapped author map so blame survives restart.
                if let Some(meta) = decoded.metadata {
                    doc.merge_author_map(meta.author_by_agent);
                }
                let mut slot = DocSlot::with_content(DocContent::Text(Box::new(doc)));
                // Seed `persisted_version` from the durable witness so
                // the lattice in memory matches the durable projection.
                // `flushed_counter` will catch up on the next successful
                // flush (or stays behind until a dirty edit triggers one,
                // which is fine — the slot isn't dirty yet).
                slot.content_state.mark_persisted(witness_version);
                LoadResult::Found(Box::new(slot))
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
                LoadResult::Found(Box::new(slot))
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

    /// The `SubscribeStatus` for a classified load: the relay's read
    /// classification, with the witness version for an inconsistent read and
    /// the reason for an unavailable one.
    fn status_envelope_for(
        msg: &sync::Subscribe,
        load_status: &SubscribeStatusInfo,
    ) -> sync::SyncEnvelope {
        let (status, expected_version, reason) = match load_status {
            SubscribeStatusInfo::Found => (sync::LoadStatus::Found, 0, String::new()),
            SubscribeStatusInfo::Empty => (sync::LoadStatus::Empty, 0, String::new()),
            SubscribeStatusInfo::Inconsistent { expected_version } => (
                sync::LoadStatus::Inconsistent,
                *expected_version,
                String::new(),
            ),
            SubscribeStatusInfo::Unavailable { reason } => {
                (sync::LoadStatus::Unavailable, 0, reason.clone())
            }
        };
        subscribe_status_envelope(
            &msg.space_id,
            &msg.document_id,
            status,
            expected_version,
            reason,
            None,
        )
    }

    /// Refuse a subscribe: the refusal rides the status frame every
    /// subscribe gets, scoped to the document by its fields, and nothing
    /// else follows (no slot, no subscription, no catch-up).
    fn refuse_subscribe(&self, conn_id: ConnId, msg: &sync::Subscribe, error: sync::Error) {
        let status = subscribe_status_envelope(
            &msg.space_id,
            &msg.document_id,
            sync::LoadStatus::Unspecified,
            0,
            String::new(),
            Some(error),
        );
        self.send_ack(conn_id, &encode_envelope(&status));
    }

    pub(super) async fn handle_subscribe(&mut self, conn_id: ConnId, msg: &sync::Subscribe) {
        let _authorized = match self.authorize_conn(conn_id, &msg.space_id).await {
            Ok(a) => a,
            Err(e) => {
                self.refuse_subscribe(conn_id, msg, refusal(ErrorCode::AuthFailed, e.to_string()));
                return;
            }
        };

        let key = DocKey {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
        };

        // Classify the load if we don't have the doc cached. The
        // classification drives the SubscribeStatus envelope below and
        // gates editor writability on the client. Cache-hit is treated
        // as Found (the doc was loaded successfully at some earlier
        // subscribe, so its state is well-known).
        let load_status = if self.documents.contains_key(&key) {
            SubscribeStatusInfo::Found
        } else {
            match self
                .load_from_backends(&msg.space_id, &msg.document_id)
                .await
            {
                LoadResult::Found(slot) => {
                    self.documents.insert(key.clone(), *slot);
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
        // catch-up follows. Clients that predate the envelope variant
        // silently ignore it.
        let status_envelope = Self::status_envelope_for(msg, &load_status);
        // SubscribeStatus is control-plane metadata, not a document op — it is a
        // direct response to this connection's own Subscribe, so it rides the
        // own-ack lane (never dropped) and stays off the `data` channel so it
        // doesn't interleave with the catch-up ops stream.
        self.send_ack(conn_id, &encode_envelope(&status_envelope));

        // Inconsistent and Unavailable short-circuit: no slot is
        // inserted, no subscriber is registered, no catch-up is sent.
        // The hard edit guard refuses writes on the client
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
                evict_for_failed_catch_up(channels, slot, conn_id, msg, &e);
            }
        }

        info!(
            conn_id,
            space_id = %msg.space_id,
            document_id = %msg.document_id,
            "subscribed"
        );

        // Reset eviction eligibility now that a subscriber is present.
        self.derive_effects(&key).await;
    }

    pub(super) async fn handle_unsubscribe(&mut self, conn_id: ConnId, msg: &sync::Unsubscribe) {
        let key = DocKey {
            space_id: msg.space_id.clone(),
            document_id: msg.document_id.clone(),
        };
        if let Some(slot) = self.documents.get_mut(&key) {
            slot.subscribers.remove(&conn_id);
        }
        self.derive_effects(&key).await;
        info!(
            conn_id,
            space_id = %msg.space_id,
            document_id = %msg.document_id,
            "unsubscribed"
        );
    }
}

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
            let envelope = sync_ops_envelope(
                space_id,
                document_id,
                Vec::new(),
                Vec::new(),
                std::collections::HashMap::new(),
            );
            (Some(envelope), Vec::new())
        }
        DocContent::Text(doc) => {
            let catch_up = if doc.local_version().is_empty() {
                None
            } else {
                let ops = doc.encode_full();
                let metadata = doc.changes_since(&[]);
                // Catch-up (join) carries the uncapped agent→author-DID snapshot
                // so a fresh subscriber can install bindings the FIFO-capped
                // `changes_since` no longer holds (blame durability).
                Some(sync_ops_envelope(
                    space_id,
                    document_id,
                    ops,
                    metadata,
                    doc.author_map(),
                ))
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

/// Send an eviction's `StaleSubscriber` notice on the UNBOUNDED own-ack lane.
///
/// The notice is the ONLY re-subscribe signal the evicted daemon gets; a
/// droppable lane here is a permanent silent strand — the notice is lost in
/// exactly the storm that triggers the eviction. ONE owner for that lane
/// choice, shared by the live broadcast
/// eviction (`relay_and_evict`) and the failed-catch-up eviction
/// (`evict_for_failed_catch_up`). `send` errors only when the socket is gone
/// — harmlessly discarded. `relay_ops_count` is informational for the daemon
/// (0 when unknown, the catch-up case).
fn send_eviction_notice(
    channels: &ConnChannels,
    space_id: &str,
    document_id: &str,
    e: &mpsc::error::TrySendError<Vec<u8>>,
    relay_ops_count: u64,
) {
    let reason = match e {
        mpsc::error::TrySendError::Full(_) => sync::StaleReason::ChannelFull,
        mpsc::error::TrySendError::Closed(_) => sync::StaleReason::ChannelClosed,
    };
    send_stale_notice(channels, space_id, document_id, reason, relay_ops_count);
}

/// Send a `StaleSubscriber` notice with `reason` on the unbounded own-ack
/// lane. The lane choice is [`send_eviction_notice`]'s; this is the shared
/// sender for every reason.
fn send_stale_notice(
    channels: &ConnChannels,
    space_id: &str,
    document_id: &str,
    reason: sync::StaleReason,
    relay_ops_count: u64,
) {
    let notice = stale_subscriber_envelope(
        space_id,
        document_id,
        reason,
        1,
        relay_ops_count,
        kutl_core::now_ms(),
    );
    let _ = channels.ack.send(encode_envelope(&notice));
}

impl Relay {
    /// Retire a document whose in-memory engine is no longer trustworthy
    /// and tell every subscriber to re-subscribe.
    ///
    /// Reached when a contained engine panic may have left the engine
    /// half-applied. The slot goes; the next subscribe reloads the document
    /// from the content backend and replays a catch-up. Unflushed edits
    /// since the last flush are lost for this one document, the loss a
    /// crash costs every loaded document. The pending edit is not flushed
    /// first: its snapshot would be read from the engine being retired.
    pub(super) fn discard_poisoned_document(&mut self, key: &DocKey) {
        let Some(slot) = self.documents.remove(key) else {
            return;
        };
        if let Some(timer) = slot.snippet_timer {
            timer.abort();
        }
        let mut notified = 0_usize;
        for (conn_id, _) in slot.subscribers.active_entries() {
            if let Some(channels) = self.connections.get(&conn_id) {
                send_stale_notice(
                    channels,
                    &key.space_id,
                    &key.document_id,
                    sync::StaleReason::DocumentReloaded,
                    0,
                );
                notified += 1;
            }
        }
        error!(
            space_id = %key.space_id,
            document_id = %key.document_id,
            subscribers_notified = notified,
            "retired document after an engine panic; subscribers told to reload"
        );
    }
}

/// Relay outbound messages to subscribers, evicting any that can't keep up.
///
/// On successful `try_send`, advances the subscriber's `known_version` to
/// `new_version`. On failure, sends a `StaleSubscriber` notice on the
/// UNBOUNDED own-ack lane and removes the subscriber.
///
/// The notice must not be droppable: a bulk broadcast storm evicts a slow
/// subscriber from MANY docs at once, and that many notices overflow the
/// bounded cap-16 ctrl lane — each dropped notice leaves its subscriber
/// stranded registered-and-content-less forever, with nothing to retry.
/// Same rationale as [`evict_for_failed_catch_up`]'s
/// notice; the ack lane's volume stays bounded by upstream backpressure.
///
/// Free function to avoid borrow conflicts with `self.documents` and
/// `self.connections`.
pub(super) fn relay_and_evict(
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
                    debug!(conn_id = target, error = %e, "evicting stale subscriber");
                    send_eviction_notice(
                        channels,
                        &key.space_id,
                        &key.document_id,
                        &e,
                        relay_ops_count,
                    );
                    stale_conns.push(target);
                }
            }
        }
    }

    for conn_id in &stale_conns {
        slot.subscribers.remove(conn_id);
    }
}

/// A dropped catch-up must not strand the subscriber: it would sit registered
/// and content-less FOREVER — no eviction, no notice, no retry (the silent
/// sibling of the live-broadcast overflow). Route it through the same recovery
/// rail as `relay_and_evict`: drop the subscription and notify, so the daemon
/// re-subscribes once its lane drains and the retried catch-up replays the
/// full state. A large subscribe burst can overflow the bounded data lane, so
/// this path is reachable in ordinary operation.
///
/// The notice is a DIRECT RESPONSE to this connection's own `Subscribe`, so it
/// rides the unbounded own-ack lane (never drops) — on the bounded ctrl lane a
/// subscribe-burst's notice storm overflows the cap and the lost notices
/// re-strand the subscriber the notice exists to save.
fn evict_for_failed_catch_up(
    channels: &ConnChannels,
    slot: &mut DocSlot,
    conn_id: ConnId,
    msg: &sync::Subscribe,
    e: &mpsc::error::TrySendError<Vec<u8>>,
) {
    warn!(
        conn_id,
        space_id = %msg.space_id,
        document_id = %msg.document_id,
        error = %e,
        "catch-up overflowed the data lane; evicting for re-subscribe"
    );
    send_eviction_notice(channels, &msg.space_id, &msg.document_id, e, 0);
    slot.subscribers.remove(&conn_id);
}

/// Load all persisted registries from a backend.
///
/// Loads tombstoned entries too (`*_including_deleted`), so a deleted
/// document's `deleted_hlc` survives a restart and gates a later lower-HLC
/// re-register from resurrecting it. The rebuilt `DocumentRegistry` keeps
/// tombstones inert — they hold no path and are filtered from active views.
pub(super) fn load_from_backend(
    backend: Option<&dyn RegistryBackend>,
) -> HashMap<String, registry::DocumentRegistry> {
    let Some(backend) = backend else {
        return HashMap::new();
    };
    let spaces = match backend.load_spaces_including_deleted() {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to load space list from registry backend");
            return HashMap::new();
        }
    };
    let mut registries = HashMap::new();
    for space_id in spaces {
        match backend.load_all_including_deleted(&space_id) {
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
    use std::sync::Arc;

    use kutl_core::Boundary;

    use crate::content_backend::ContentBackend;

    use super::super::{
        RelayCommand, TEST_CONN_DID, connect_client, flush_and_confirm, send_text_ops,
        subscribe_doc, test_config,
    };
    use super::*;

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
        backend
            .save("5171e0a1-1111-4000-8000-000000000001", "doc", &encoded)
            .await
            .unwrap();

        // Create relay with content backend.
        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, None, Some(Arc::new(backend)), None, None);

        // Connect + subscribe.
        let conn_id = 1;
        let (mut data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;

        // Should receive catch-up data (non-empty ops from the persisted document).
        let bytes = data_rx.try_recv().expect("should receive catch-up");
        assert!(
            !bytes.is_empty(),
            "catch-up should contain persisted content"
        );
    }

    /// A relay cold-load of a content-envelope blob (oplog + `ChangeList`) must
    /// recover the durable author map so blame does not read `"unknown"` after
    /// a restart. Builds the persisted blob with the kutl-core codec directly so
    /// the cold-load decode is exercised independently of the flush path.
    #[tokio::test]
    async fn test_cold_load_recovers_author_map_from_envelope() {
        // Author a document so `author_map()` has {"a" -> "did:a"}.
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("a").unwrap();
        doc.edit(agent, "did:a", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello envelope")
        })
        .unwrap();
        assert_eq!(doc.author_of("a"), "did:a", "precondition: map populated");
        let expected_content = doc.content();

        // Persist as a content envelope, built with the codec directly.
        let list = kutl_core::ChangeList {
            changes: doc.changes().to_vec(),
            author_by_agent: doc.author_map(),
        };
        let blob = kutl_core::encode_content_envelope(&doc.encode_full(), &list);
        assert_eq!(
            blob[..kutl_core::ENVELOPE_MAGIC.len()],
            kutl_core::ENVELOPE_MAGIC,
            "codec produced a real envelope",
        );

        let backend = InMemoryContentBackend::new();
        backend
            .save("5171e0a1-1111-4000-8000-000000000001", "doc", &blob)
            .await
            .unwrap();

        // Cold-load via subscribe.
        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, None, Some(Arc::new(backend)), None, None);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;

        // Inspect the reconstructed in-memory document.
        let key = DocKey {
            space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
            document_id: "doc".into(),
        };
        let slot = relay
            .documents
            .get(&key)
            .expect("document should be loaded into memory");
        let DocContent::Text(loaded) = &slot.content else {
            panic!("expected a text document after cold-load");
        };
        assert_eq!(
            loaded.author_of("a"),
            "did:a",
            "cold-load must recover the durable author map from the envelope",
        );
        assert_eq!(
            loaded.content(),
            expected_content,
            "cold-load must reconstruct the same content",
        );
    }

    /// Backward-compat: a bare `encode_full()` oplog (no envelope — the legacy
    /// persisted format) still cold-loads. Its content reconstructs, but
    /// there is no persisted map so blame reads `"unknown"` until the next
    /// re-flush self-heals it into an envelope.
    #[tokio::test]
    async fn test_cold_load_of_legacy_bare_oplog_is_lossy_but_ok() {
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("a").unwrap();
        doc.edit(agent, "did:a", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "legacy content")
        })
        .unwrap();
        let expected_content = doc.content();

        // Persist a BARE oplog — no envelope wrapper.
        let bare = doc.encode_full();
        assert_ne!(
            bare[..kutl_core::ENVELOPE_MAGIC.len()],
            kutl_core::ENVELOPE_MAGIC,
            "a bare oplog is not an envelope",
        );

        let backend = InMemoryContentBackend::new();
        backend
            .save("5171e0a1-1111-4000-8000-000000000001", "doc", &bare)
            .await
            .unwrap();

        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, None, Some(Arc::new(backend)), None, None);
        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;

        let key = DocKey {
            space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
            document_id: "doc".into(),
        };
        let slot = relay
            .documents
            .get(&key)
            .expect("legacy blob should still load");
        let DocContent::Text(loaded) = &slot.content else {
            panic!("expected a text document after cold-load");
        };
        assert_eq!(
            loaded.content(),
            expected_content,
            "legacy bare-oplog content must still reconstruct",
        );
        assert_eq!(
            loaded.author_of("a"),
            "unknown",
            "legacy blobs carry no map; blame is unknown until a re-flush self-heals",
        );
    }

    /// Belt-and-suspenders integration proof that the blame pipeline chains: a
    /// relay that cold-loads a document purely from a
    /// persisted content envelope (no in-session authoring) must SERVE the
    /// recovered author map in a fresh subscriber's catch-up. This exercises the
    /// envelope codec, cold-load recovery, and the catch-up snapshot
    /// wire field end to end.
    #[tokio::test]
    async fn test_cold_loaded_author_map_reaches_subscriber_catch_up() {
        // Author a document so `author_map()` == {"a" -> "did:a"}.
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("a").unwrap();
        doc.edit(agent, "did:a", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();
        assert_eq!(doc.author_of("a"), "did:a", "precondition: map populated");

        // Persist as a content envelope (oplog + ChangeList) via the codec.
        let list = kutl_core::ChangeList {
            changes: doc.changes().to_vec(),
            author_by_agent: doc.author_map(),
        };
        let blob = kutl_core::encode_content_envelope(&doc.encode_full(), &list);
        let backend = InMemoryContentBackend::new();
        backend
            .save("5171e0a1-1111-4000-8000-000000000001", "doc", &blob)
            .await
            .unwrap();

        // Boot a relay backed by the persisted envelope and cold-load via
        // subscribe. There is NO in-session authoring; the map can only come
        // from the persisted envelope.
        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, None, Some(Arc::new(backend)), None, None);
        let conn_id = 1;
        let (mut data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;

        // The subscriber's own catch-up must carry the recovered snapshot.
        let bytes = data_rx.try_recv().expect("catch-up");
        let envelope =
            kutl_proto::protocol::decode_envelope(&bytes).expect("should decode as SyncEnvelope");
        match &envelope.payload {
            Some(sync::sync_envelope::Payload::SyncOps(ops)) => {
                assert!(!ops.ops.is_empty(), "catch-up should serve content");
                assert_eq!(
                    ops.author_by_agent_snapshot.get("a").map(String::as_str),
                    Some("did:a"),
                    "cold-loaded author map must reach the subscriber's catch-up snapshot",
                );
            }
            other => panic!("expected SyncOps payload, got {other:?}"),
        }
    }

    /// Backward-compat at the integration level: a bare `encode_full()` oplog
    /// (no envelope — a legacy blob) cold-loads and still serves
    /// content, but the catch-up carries an empty author snapshot — the
    /// documented self-heal-until-re-flush case.
    #[tokio::test]
    async fn test_legacy_bare_oplog_serves_empty_snapshot_in_catch_up() {
        let mut doc = kutl_core::Document::new();
        let agent = doc.register_agent("a").unwrap();
        doc.edit(agent, "did:a", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "legacy content")
        })
        .unwrap();

        // Persist a BARE oplog — no envelope wrapper.
        let bare = doc.encode_full();
        let backend = InMemoryContentBackend::new();
        backend
            .save("5171e0a1-1111-4000-8000-000000000001", "doc", &bare)
            .await
            .unwrap();

        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, None, Some(Arc::new(backend)), None, None);
        let conn_id = 1;
        let (mut data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;

        let bytes = data_rx.try_recv().expect("catch-up");
        let envelope =
            kutl_proto::protocol::decode_envelope(&bytes).expect("should decode as SyncEnvelope");
        match &envelope.payload {
            Some(sync::sync_envelope::Payload::SyncOps(ops)) => {
                assert!(
                    !ops.ops.is_empty(),
                    "legacy blob content must still serve in catch-up",
                );
                assert!(
                    ops.author_by_agent_snapshot.is_empty(),
                    "a legacy blob carries no map until a re-flush self-heals it",
                );
            }
            other => panic!("expected SyncOps payload, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_subscribe_without_backend_creates_empty() {
        // No content backend — document should start empty.
        let config = test_config();
        let mut relay = Relay::new_standalone(config);

        let conn_id = 1;
        let (mut data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;

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
            .save(
                "5171e0a1-1111-4000-8000-000000000001",
                "doc",
                &doc.encode_full(),
            )
            .await
            .unwrap();

        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, None, Some(Arc::new(backend)), None, None);

        let conn_id = 1;
        let (_data_rx, _ack_rx, _ctrl_rx) = connect_client(&mut relay, conn_id).await;
        subscribe_doc(
            &mut relay,
            conn_id,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;

        // Loaded from backend — should not be dirty.
        assert!(!relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "doc"));
    }

    // ---- Eviction tests ----

    #[tokio::test]
    async fn test_evict_removes_document() {
        let backend = InMemoryContentBackend::new();
        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, None, Some(Arc::new(backend)), None, None);

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

        // Flush + confirm to clear the dirty flag (eviction requires !dirty).
        let _ = flush_and_confirm(&mut relay).await;
        assert!(!relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "doc"));

        // Unsubscribe so the document has no subscribers.
        relay
            .process_command(RelayCommand::Unsubscribe {
                conn_id,
                msg: sync::Unsubscribe {
                    space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                    document_id: "doc".into(),
                },
            })
            .await;

        assert!(relay.has_document("5171e0a1-1111-4000-8000-000000000001", "doc"));

        // Backdate eligible_since so the grace period has elapsed.
        let key = DocKey {
            space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
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
                space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                document_id: "doc".into(),
            })
            .await;

        // Document should be evicted from memory.
        assert!(!relay.has_document("5171e0a1-1111-4000-8000-000000000001", "doc"));
    }

    #[tokio::test]
    async fn test_evict_skipped_when_subscribers_present() {
        let backend = InMemoryContentBackend::new();
        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, None, Some(Arc::new(backend)), None, None);

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

        // Flush + confirm to clear dirty flag.
        let _ = flush_and_confirm(&mut relay).await;

        // Still subscribed — send DeriveEffects anyway.
        relay
            .process_command(RelayCommand::DeriveEffects {
                space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                document_id: "doc".into(),
            })
            .await;

        // Document should still be in memory (subscriber present).
        assert!(relay.has_document("5171e0a1-1111-4000-8000-000000000001", "doc"));
    }

    #[tokio::test]
    async fn test_evict_skipped_when_dirty() {
        let backend = InMemoryContentBackend::new();
        let config = test_config();
        let mut relay =
            Relay::new_standalone_with_backend(config, None, Some(Arc::new(backend)), None, None);

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

        // Do NOT flush — document is dirty.
        assert!(relay.is_document_dirty("5171e0a1-1111-4000-8000-000000000001", "doc"));

        // Unsubscribe so no subscribers remain.
        relay
            .process_command(RelayCommand::Unsubscribe {
                conn_id,
                msg: sync::Unsubscribe {
                    space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                    document_id: "doc".into(),
                },
            })
            .await;

        // Backdate eligible_since so the grace period has elapsed.
        let key = DocKey {
            space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
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
                space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                document_id: "doc".into(),
            })
            .await;

        // Document should still be in memory (dirty prevents eviction).
        assert!(relay.has_document("5171e0a1-1111-4000-8000-000000000001", "doc"));
    }

    #[tokio::test]
    async fn test_no_eviction_without_backend() {
        // No content backend — eviction should never be triggered.
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

        // Flush + confirm to clear dirty.
        let _ = flush_and_confirm(&mut relay).await;

        // Unsubscribe.
        relay
            .process_command(RelayCommand::Unsubscribe {
                conn_id,
                msg: sync::Unsubscribe {
                    space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
                    document_id: "doc".into(),
                },
            })
            .await;

        // Without a content backend, derive_effects should be a no-op.
        // The document should still exist — eviction state should not be
        // marked eligible (content_backend is None, so data would be lost).
        let key = DocKey {
            space_id: "5171e0a1-1111-4000-8000-000000000001".into(),
            document_id: "doc".into(),
        };
        let slot = relay.documents.get(&key).expect("document should exist");
        assert_eq!(
            slot.eviction.eligible_since, 0,
            "eviction should not be marked eligible without content backend"
        );
    }

    /// A live-broadcast eviction's `StaleSubscriber` notice must ride the
    /// UNBOUNDED own-ack lane. The eviction fires precisely when the
    /// subscriber's bounded lanes are overflowing, so a notice on the bounded
    /// ctrl lane is dropped in exactly the storm it exists to recover from —
    /// the subscriber then sits registered-and-content-less forever with
    /// nothing to retry.
    #[tokio::test]
    async fn test_broadcast_eviction_notice_rides_unbounded_ack_lane() {
        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let publisher: ConnId = 1;
        let subscriber: ConnId = 2;
        let (_pub_data, _pub_ack, _pub_ctrl) = connect_client(&mut relay, publisher).await;
        let (mut sub_data, mut sub_ack, _sub_ctrl) = connect_client(&mut relay, subscriber).await;

        subscribe_doc(
            &mut relay,
            subscriber,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;
        // Drain the subscribe-time frames so the data lane starts empty.
        while sub_data.try_recv().is_ok() {}
        while sub_ack.try_recv().is_ok() {}

        // Storm: more broadcasts than the cap-16 data lane holds, never
        // drained — the overflowing send must evict the subscriber.
        for i in 0..20 {
            send_text_ops(
                &mut relay,
                publisher,
                "5171e0a1-1111-4000-8000-000000000001",
                "doc",
                &format!("edit {i}\n"),
            )
            .await;
        }

        let mut notice_on_ack = false;
        while let Ok(bytes) = sub_ack.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && matches!(
                    env.payload,
                    Some(sync::sync_envelope::Payload::StaleSubscriber(_))
                )
            {
                notice_on_ack = true;
            }
        }
        assert!(
            notice_on_ack,
            "the eviction notice must arrive on the unbounded ack lane (a \
             bounded-ctrl notice is dropped by the very storm that evicts)"
        );

        // And the eviction itself happened: the subscription is gone, so a
        // further op produces no new data frame.
        while sub_data.try_recv().is_ok() {}
        send_text_ops(
            &mut relay,
            publisher,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
            "after eviction\n",
        )
        .await;
        assert!(
            sub_data.try_recv().is_err(),
            "the evicted subscriber receives no further data frames"
        );
    }

    /// A `Subscribe` whose catch-up frame finds the data lane FULL must route
    /// through the same recovery rail as live-fanout overflow: drop the
    /// subscription and put a `StaleSubscriber` notice on the unbounded
    /// own-ack lane. Direct pin for `evict_for_failed_catch_up`.
    #[tokio::test]
    async fn test_subscribe_catch_up_overflow_evicts_with_ack_lane_notice() {
        /// One-slot data lane so a single pre-filled frame forces the
        /// catch-up `try_send` onto the overflow path.
        const ONE_SLOT_DATA: usize = 1;

        let config = test_config();
        let mut relay = Relay::new_standalone(config);
        let publisher: ConnId = 1;
        let (_pub_data, _pub_ack, _pub_ctrl) = connect_client(&mut relay, publisher).await;
        subscribe_doc(
            &mut relay,
            publisher,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;
        send_text_ops(
            &mut relay,
            publisher,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
            "content\n",
        )
        .await;

        // The victim: a 1-slot data lane we pre-fill ourselves (the retained
        // tx clone models a conn writer that hasn't had a scheduling slot).
        let victim: ConnId = 2;
        let (v_data_tx, mut v_data_rx) = mpsc::channel(ONE_SLOT_DATA);
        let (v_ctrl_tx, _v_ctrl_rx) = mpsc::channel(16);
        let (v_ack_tx, mut v_ack_rx) = mpsc::unbounded_channel();
        let lane_filler = v_data_tx.clone();
        relay
            .process_command(RelayCommand::Connect {
                conn_id: victim,
                tx: v_data_tx,
                ctrl_tx: v_ctrl_tx,
                ack_tx: v_ack_tx,
            })
            .await;
        // Authentication is mandatory; attach the identity directly
        // (the in-process actor tests cannot run the real HTTP handshake).
        relay.test_set_authenticated(victim, TEST_CONN_DID);
        while v_ack_rx.try_recv().is_ok() {}
        lane_filler
            .try_send(Vec::new())
            .expect("pre-fill the single data slot");

        subscribe_doc(
            &mut relay,
            victim,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
        )
        .await;

        let mut notice_on_ack = false;
        while let Ok(bytes) = v_ack_rx.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && let Some(sync::sync_envelope::Payload::StaleSubscriber(s)) = env.payload
            {
                assert_eq!(s.document_id, "doc", "notice names the stranded doc");
                notice_on_ack = true;
            }
        }
        assert!(
            notice_on_ack,
            "the failed catch-up's eviction notice must ride the unbounded ack lane"
        );

        // The subscription was dropped: drain the pre-fill, then a further op
        // must produce no data frame for the victim.
        while v_data_rx.try_recv().is_ok() {}
        send_text_ops(
            &mut relay,
            publisher,
            "5171e0a1-1111-4000-8000-000000000001",
            "doc",
            "after\n",
        )
        .await;
        assert!(
            v_data_rx.try_recv().is_err(),
            "the evicted subscription receives no further data frames"
        );

        // Per-doc recovery, not connection death: the victim stays connected
        // (the daemon's re-subscribe rides the same connection).
        assert!(
            relay.connections.contains_key(&victim),
            "catch-up eviction drops the subscription, not the connection"
        );
    }
}
