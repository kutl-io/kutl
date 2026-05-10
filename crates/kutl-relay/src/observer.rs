//! Relay observer trait and event types.
//!
//! The [`RelayObserver`] trait decouples relay event emission from a specific
//! backend (e.g. Redis Streams). The OSS relay uses [`NoopObserver`]; the
//! commercial `kutlhub-relay` provides a Redis-backed implementation.
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
    /// Number of ops in the batch.
    pub op_count: usize,
    /// Free-form intent string from the first metadata entry.
    pub intent: String,
    /// Whether the edit was a text or blob operation.
    pub content_mode: EditContentMode,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
}

/// A new signal was created (flag or reply).
#[derive(Debug, Clone)]
pub struct SignalCreatedEvent {
    /// Relay-generated UUID for this signal.
    pub id: String,
    /// Space containing the document.
    pub space_id: String,
    /// Document the signal is attached to.
    pub document_id: String,
    /// DID of the author who created the signal.
    pub author_did: String,
    /// Signal type: `"flag"` or `"reply"`.
    pub signal_type: String,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
    // Flag fields (present when signal_type = "flag")
    /// Flag kind (e.g. `review`, `question`). Present for flags.
    pub flag_kind: Option<String>,
    /// Audience scope (e.g. `all`, `owner`). Present for flags.
    pub audience: Option<String>,
    /// DID of the targeted recipient, if any. Present for flags.
    pub target_did: Option<String>,
    /// Human-readable message accompanying the flag.
    pub message: Option<String>,
    // Reply fields (present when signal_type = "reply")
    /// ID of the parent signal being replied to. Present for replies.
    pub parent_signal_id: Option<String>,
    /// ID of the parent reply, for threaded replies.
    pub parent_reply_id: Option<String>,
    /// Body text of the reply.
    pub body: Option<String>,
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
    /// Emoji or short value for the reaction.
    pub emoji: String,
    /// If true, the reaction is being removed; if false, it is being added.
    pub remove: bool,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
}

/// A flag was closed.
#[derive(Debug, Clone)]
pub struct SignalClosedEvent {
    /// Space containing the signal.
    pub space_id: String,
    /// ID of the signal being closed.
    pub signal_id: String,
    /// DID of the actor who closed the signal.
    pub actor_did: String,
    /// Close reason: `"resolved"`, `"declined"`, or `"withdrawn"`.
    pub reason: String,
    /// Optional note explaining the closure.
    pub close_note: Option<String>,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
}

/// A previously closed flag was reopened.
#[derive(Debug, Clone)]
pub struct SignalReopenedEvent {
    /// Space containing the signal.
    pub space_id: String,
    /// ID of the signal being reopened.
    pub signal_id: String,
    /// DID of the actor who reopened the signal.
    pub actor_did: String,
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
/// Receives the merged document and common metadata. Implementations
/// derive content-level artifacts (snippets, mention diffs) and emit
/// events. The OSS relay uses a no-op implementation.
pub trait AfterMergeObserver: Send + Sync {
    /// Called with the document in its post-merge state.
    fn after_text_merge(&self, event: MergedEvent, doc: &Document);
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

impl AfterMergeObserver for NoopAfterMergeObserver {
    fn after_text_merge(&self, _event: MergedEvent, _doc: &Document) {}
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

    /// Called when a signal (flag or reply) is created.
    fn on_signal_created(&self, event: SignalCreatedEvent);

    /// Called when a reaction is added or removed on a signal.
    fn on_reaction(&self, event: ReactionEvent);

    /// Called when a flag is closed.
    fn on_signal_closed(&self, event: SignalClosedEvent);

    /// Called when a previously closed flag is reopened.
    fn on_signal_reopened(&self, event: SignalReopenedEvent);
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
    fn on_signal_created(&self, _event: SignalCreatedEvent) {}
    fn on_reaction(&self, _event: ReactionEvent) {}
    fn on_signal_closed(&self, _event: SignalClosedEvent) {}
    fn on_signal_reopened(&self, _event: SignalReopenedEvent) {}
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

    fn on_signal_created(&self, event: SignalCreatedEvent) {
        tracing::info!(
            space_id = %event.space_id,
            signal_id = %event.id,
            signal_type = %event.signal_type,
            author_did = %event.author_did,
            document_id = %event.document_id,
            message = event.message.as_deref().unwrap_or(
                event.body.as_deref().unwrap_or("-")
            ),
            "signal created"
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

    fn on_signal_closed(&self, event: SignalClosedEvent) {
        tracing::info!(
            space_id = %event.space_id,
            signal_id = %event.signal_id,
            actor_did = %event.actor_did,
            reason = %event.reason,
            "signal closed"
        );
    }

    fn on_signal_reopened(&self, event: SignalReopenedEvent) {
        tracing::info!(
            space_id = %event.space_id,
            signal_id = %event.signal_id,
            actor_did = %event.actor_did,
            "signal reopened"
        );
    }
}

impl AfterMergeObserver for TracingObserver {
    fn after_text_merge(&self, event: MergedEvent, _doc: &Document) {
        tracing::info!(
            space_id = %event.space_id,
            document_id = %event.document_id,
            author_did = %event.author_did,
            op_count = event.op_count,
            intent = %event.intent,
            "text edited"
        );
    }
}
