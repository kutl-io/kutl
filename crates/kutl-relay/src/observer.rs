//! Relay observer trait and event types.
//!
//! The [`RelayObserver`] trait decouples relay event emission from a specific
//! backend (e.g. Redis Streams). The default is [`NoopObserver`];
//! deployments can plug in a streams-backed implementation.
//!
//! Text merges use a separate two-phase observer pattern:
//! [`BeforeMergeObserver`] captures pre-merge state and
//! [`AfterMergeObserver`] processes post-merge results. This keeps
//! content-level work (snippet extraction, mention parsing) out of the
//! OSS relay.

use kutl_core::Document;

/// Whether an edit targeted text or blob content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditContentMode {
    /// Text CRDT operations (diamond-types).
    Text,
    /// Binary blob replacement (last-writer-wins).
    Blob,
}

/// Common metadata assembled by the relay for each text merge.
#[derive(Debug, Clone)]
pub struct MergedEvent {
    /// Space containing the document.
    pub space_id: String,
    /// Document that was edited.
    pub document_id: String,
    /// DID of the author who submitted the ops.
    pub author_did: String,
    /// Token the author authenticated with, when they used one.
    ///
    /// Carried beside `author_did` rather than folded into it because a
    /// token binds to its owner's key: a person and their agent share a DID,
    /// and this is the only thing that separates them. `None` means the
    /// author authenticated directly.
    pub via_pat_id: Option<String>,
    /// Number of ops in the batch.
    pub op_count: usize,
    /// Free-form intent string from the first metadata entry.
    pub intent: String,
    /// Whether the edit was a text or blob operation.
    pub content_mode: EditContentMode,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
}

/// A signal record was admitted — created, closed, reopened, or any event a
/// future kind brings with it.
///
/// **Carries the record rather than a flattening of it.** A hand-maintained
/// subset of a record's fields needs a struct, an emitter and a parser edited
/// in step, with nothing checking that all three were, and whatever a
/// flattening drops is lost before it reaches any wire — no downstream
/// encoding can recover it. A consumer that wants a field reads it off the
/// record.
#[derive(Debug, Clone)]
pub struct SignalRecordEvent {
    /// The record itself. Its `event` field says what happened to the signal.
    pub record: kutl_proto::sync::Signal,
    /// The PAT the authoring action authenticated with, when it did. Not on
    /// the record: it describes the credential that drove the write, not the
    /// write.
    pub via_pat_id: Option<String>,
}

/// A reaction was added or removed on a signal.
#[derive(Debug, Clone)]
pub struct ReactionEvent {
    /// Space containing the signal.
    pub space_id: String,
    /// ID of the signal being reacted to.
    pub signal_id: String,
    /// DID of the actor who added or removed the reaction.
    pub actor_did: String,
    /// `api_tokens.id` of the PAT the actor used, when PAT-authenticated.
    /// `None` for DID challenge-response auth; surfaces per-PAT attribution
    /// downstream of the observer.
    pub via_pat_id: Option<String>,
    /// Emoji or short value for the reaction.
    pub emoji: String,
    /// If true, the reaction is being removed; if false, it is being added.
    pub remove: bool,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
}

/// A document was registered in the space manifest.
#[derive(Debug, Clone)]
pub struct DocumentRegisteredEvent {
    /// Space the document was registered in.
    pub space_id: String,
    /// Unique document identifier.
    pub document_id: String,
    /// DID of the author who registered the document.
    pub author_did: String,
    /// File path within the space.
    pub path: String,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
}

/// A document was renamed in the space manifest.
#[derive(Debug, Clone)]
pub struct DocumentRenamedEvent {
    /// Space containing the document.
    pub space_id: String,
    /// Document that was renamed.
    pub document_id: String,
    /// DID of the author who renamed the document.
    pub author_did: String,
    /// Previous file path.
    pub old_path: String,
    /// New file path.
    pub new_path: String,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
}

/// A document was unregistered from the space manifest.
#[derive(Debug, Clone)]
pub struct DocumentUnregisteredEvent {
    /// Space the document was removed from.
    pub space_id: String,
    /// Document that was unregistered.
    pub document_id: String,
    /// DID of the author who unregistered the document.
    pub author_did: String,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
}

/// Called before the relay merges ops into a text document.
///
/// Implementations can capture baseline state (e.g., `doc.content()`)
/// for later diffing. The OSS relay uses a no-op implementation.
pub trait BeforeMergeObserver: Send + Sync {
    /// Called with the document in its pre-merge state.
    fn before_text_merge(&self, space_id: &str, document_id: &str, doc: &Document);
}

/// Called after the relay merges ops into a text document.
///
/// Receives the merged document's content and common metadata.
/// Implementations derive content-level artifacts (snippets, mention diffs)
/// and emit events. The OSS relay installs its record materializer; a host
/// may install its own. The content arrives as `&str` rather than
/// `&Document`: the observer's future is held across awaits on the actor
/// task, and a document borrow there would require the engine to be `Sync`,
/// which its edit-buffering rope is not.
#[async_trait::async_trait]
pub trait AfterMergeObserver: Send + Sync {
    /// Called with the document's post-merge content.
    ///
    /// **Async** so the OSS materializer can resolve a mention's account id to
    /// a DID before signing the record; `target_did` is inside the signed
    /// canonical bytes, so it cannot be filled in afterwards. The merge path
    /// already awaits external I/O here — `invoke_after_merge` loads a space's
    /// records via `ensure_doc_seeded` first — so this adds no new class of
    /// blocking.
    ///
    /// **NEVER await anything that calls back into the relay actor.** This runs
    /// INSIDE the actor's handling of a merge, so the actor cannot service a
    /// request made from here and would deadlock — a rule the async signature
    /// cannot enforce. Awaiting an independent backend (the membership
    /// directory, the record log) is fine; awaiting the actor is not. Results
    /// still travel back to the actor over the non-blocking unbounded channel,
    /// never by awaiting a reply.
    async fn after_text_merge(&self, event: MergedEvent, content: &str);

    /// Seed the observer's per-document baseline from its DURABLE records
    /// before the first [`Self::after_text_merge`] for that document in this
    /// process (restart correctness).
    ///
    /// Observers that hold marker known-sets purely in process memory (the OSS
    /// [`RecordMaterializingObserver`]) must rebuild those sets from the folded
    /// records on restart; otherwise the first post-restart merge diffs the
    /// current content against an EMPTY baseline and either re-emits unchanged
    /// markers as duplicate CREATEDs or misses a removed marker's
    /// CLOSED(WITHDRAWN). The relay actor calls this once per `(space,
    /// document)` per process with that document's records.
    ///
    /// Defaults to a no-op so stateless observers (`TracingObserver`,
    /// `NoopAfterMergeObserver`, hosts' provided enrichers) are unaffected.
    ///
    /// [`RecordMaterializingObserver`]: crate::markers::materialize::RecordMaterializingObserver
    fn seed_doc_from_records(
        &self,
        _space_id: &str,
        _document_id: &str,
        _records: &[kutl_proto::sync::Signal],
    ) {
    }

    /// Drop the observer's per-document state once the document is
    /// unregistered. The marker known-sets are keyed by `(space, document)`
    /// and nothing else removes an entry, so without this a relay holds
    /// every document it ever unregistered; and a document re-registered
    /// under the same id must be seeded afresh from its records before its
    /// first merge, like any document this process has not merged yet, not
    /// diffed against the markers it carried before it was removed.
    /// Defaults to a no-op for stateless observers.
    fn on_document_unregistered(&self, _space_id: &str, _document_id: &str) {}
}

/// A no-op before-merge observer.
///
/// Used by the open-source relay which has no content processing.
pub struct NoopBeforeMergeObserver;

impl BeforeMergeObserver for NoopBeforeMergeObserver {
    fn before_text_merge(&self, _space_id: &str, _document_id: &str, _doc: &Document) {}
}

/// A no-op after-merge observer.
///
/// Used by the open-source relay which has no content processing.
pub struct NoopAfterMergeObserver;

#[async_trait::async_trait]
impl AfterMergeObserver for NoopAfterMergeObserver {
    async fn after_text_merge(&self, _event: MergedEvent, _content: &str) {}
}

/// Trait for observing relay events.
///
/// Implementations must be [`Send`] + [`Sync`] so they can be shared across
/// async tasks. Methods take `&self` and must not block — implementations
/// should spawn background work if I/O is needed.
pub trait RelayObserver: Send + Sync {
    /// Called when a blob document is edited (LWW replacement).
    fn on_blob_edited(&self, event: MergedEvent);

    /// Called when a document is registered in the space manifest.
    fn on_document_registered(&self, event: DocumentRegisteredEvent);

    /// Called when a document is renamed in the space manifest.
    fn on_document_renamed(&self, event: DocumentRenamedEvent);

    /// Called when a document is unregistered from the space manifest.
    fn on_document_unregistered(&self, event: DocumentUnregisteredEvent);

    /// Called when a signal record is admitted — created, closed, reopened, or
    /// whatever a future kind brings. One method rather than one per event:
    /// the record says what happened, so a new event type reaches every
    /// observer without a trait change, and an observer that does not
    /// recognise it still receives it rather than having no arm to be called
    /// on.
    fn on_signal_record(&self, event: SignalRecordEvent);

    /// Called when a reaction is added or removed on a signal. Reactions are
    /// not records, which is why this stays separate.
    fn on_reaction(&self, event: ReactionEvent);
}

/// A no-op observer that discards all events.
///
/// Used by the open-source relay which has no external event sink.
pub struct NoopObserver;

impl RelayObserver for NoopObserver {
    fn on_blob_edited(&self, _event: MergedEvent) {}
    fn on_document_registered(&self, _event: DocumentRegisteredEvent) {}
    fn on_document_renamed(&self, _event: DocumentRenamedEvent) {}
    fn on_document_unregistered(&self, _event: DocumentUnregisteredEvent) {}
    fn on_signal_record(&self, _event: SignalRecordEvent) {}
    fn on_reaction(&self, _event: ReactionEvent) {}
}

/// Observer that emits structured tracing events for all relay activity.
///
/// Replaces `NoopObserver` in the OSS relay to provide visibility into
/// edits, signals, and document lifecycle without external dependencies.
pub struct TracingObserver;

impl RelayObserver for TracingObserver {
    fn on_blob_edited(&self, event: MergedEvent) {
        tracing::info!(
            space_id = %event.space_id,
            document_id = %event.document_id,
            author_did = %event.author_did,
            op_count = event.op_count,
            intent = %event.intent,
            "blob edited"
        );
    }

    fn on_document_registered(&self, event: DocumentRegisteredEvent) {
        tracing::info!(
            space_id = %event.space_id,
            document_id = %event.document_id,
            path = %event.path,
            author_did = %event.author_did,
            "document registered"
        );
    }

    fn on_document_renamed(&self, event: DocumentRenamedEvent) {
        tracing::info!(
            space_id = %event.space_id,
            document_id = %event.document_id,
            old_path = %event.old_path,
            new_path = %event.new_path,
            author_did = %event.author_did,
            "document renamed"
        );
    }

    fn on_document_unregistered(&self, event: DocumentUnregisteredEvent) {
        tracing::info!(
            space_id = %event.space_id,
            document_id = %event.document_id,
            "document unregistered"
        );
    }

    fn on_signal_record(&self, event: SignalRecordEvent) {
        let record = &event.record;
        tracing::info!(
            space_id = %record.space_id,
            signal_id = %record.id,
            record_id = %record.record_id,
            signal_event = ?record.event(),
            kind = ?kutl_signals::summary::kind_of(record),
            author_did = %record.author_did,
            actor_did = %record.actor_did,
            "signal record"
        );
    }

    fn on_reaction(&self, event: ReactionEvent) {
        tracing::info!(
            space_id = %event.space_id,
            signal_id = %event.signal_id,
            actor_did = %event.actor_did,
            emoji = %event.emoji,
            "reaction"
        );
    }
}
