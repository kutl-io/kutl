//! Backend trait for querying changes in a space.
//!
//! Implementations query existing data rather than maintaining a parallel
//! event log. The default backend uses `SQLite`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::registry::RegistryEntry;

/// Response from [`ChangeBackend::get_changes`].
///
/// Structured as a map of arrays — each section is always present but may
/// be empty depending on what changed and the backend's capabilities.
#[derive(Debug, Default, Serialize)]
pub struct ChangesResponse {
    /// Signals since last check. Uses the proto type directly
    /// (serde derives added to kutl-proto via prost-build config).
    pub signals: Vec<kutl_proto::sync::Signal>,
    /// Documents registered, renamed, or deleted since last check.
    /// Uses the existing [`RegistryEntry`] type from the document registry.
    pub document_changes: Vec<RegistryEntry>,
    /// Opaque checkpoint for failure recovery. The agent can pass this back
    /// on the next call to replay from this point if a previous call failed.
    pub checkpoint: String,
}

/// Whether `signal` reaches `did` at all.
///
/// **The** answer to "does this record reach this participant", shared by every
/// surface that delivers one: the pull path narrows a page with it
/// ([`ChangesResponse::retain_addressed_to`]) and the push path decides whether
/// to wake a waiting agent with it. Two surfaces deciding this separately is
/// how they drift, and a caller would then see a record on one and not the
/// other depending only on which they happened to call.
///
/// Only a flag carries an audience. A reply, chat, or decision addresses no
/// one and reaches everyone, so a thread stays legible to everyone who can see
/// the signal it hangs off.
///
/// **A delivery filter, not an access boundary.** It narrows what arrives
/// unasked-for; a caller naming a signal id still reads it through
/// [`ChangeBackend::get_signal_detail`], and this does not pretend otherwise.
/// It also says nothing about authorship — a surface that must not echo a
/// caller's own writes back applies that on top.
#[must_use]
pub fn signal_reaches(signal: &kutl_proto::sync::Signal, did: &str) -> bool {
    use kutl_proto::sync::signal::Payload;
    use kutl_proto::vocab::AudienceDelivery;

    let Some(Payload::Flag(flag)) = &signal.payload else {
        return true;
    };
    match kutl_proto::vocab::flag_audience_delivery(flag) {
        AudienceDelivery::Space => true,
        AudienceDelivery::Participant(target) => target == did,
        // Malformed addressing reaches nobody. The projection widens this to
        // space-wide so a stored row says something; doing the same here would
        // hand an unaddressed flag to every member.
        AudienceDelivery::Undeliverable => {
            // A record that reaches no one is a message its author believes
            // they sent. Silence here makes that undiagnosable from either
            // end, so it is worth a line even though dropping is correct.
            tracing::warn!(
                signal_id = %signal.id,
                space_id = %signal.space_id,
                author_did = %signal.author_did,
                "dropping flag with malformed audience: it addresses nobody"
            );
            false
        }
    }
}

impl ChangesResponse {
    /// Drop the signals `did` is not addressed by: a flag naming a different
    /// participant, and a flag addressed to nobody.
    ///
    /// A backend answers "what happened in this space", which is a wider
    /// question than "what arrived for me". Applying the audience here rather
    /// than in each substrate's query keeps one rule over every backend.
    /// [`signal_reaches`] is that rule.
    pub fn retain_addressed_to(&mut self, did: &str) {
        self.signals.retain(|signal| signal_reaches(signal, did));
    }
}

/// Errors from change backend operations.
#[derive(Debug, thiserror::Error)]
pub enum ChangeError {
    /// A database-level error occurred.
    #[error("database error: {0}")]
    Db(String),
    /// The requested resource was not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// An unexpected internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
    /// The caller's arguments were invalid — a **client** error, not a fault.
    ///
    /// Distinct from [`Self::Internal`] because moving a validation check inward
    /// changes how it surfaces: the doors map every `ChangeError` they do not
    /// recognize to an internal error, so a user mistake would come back as a
    /// 500. Every door must map this to its own
    /// invalid-argument shape.
    #[error("invalid argument: {reason}")]
    InvalidArgument {
        /// Why the argument was rejected, safe to return to the caller.
        reason: String,
    },
}

impl From<sqlx::Error> for ChangeError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e.to_string())
    }
}

/// A projection row's substrate-independent served shape — the input to the
/// ONE row→proto conversion both change backends share.
///
/// Each backend maps its own columns (integer discriminants on sqlite, enum
/// strings on Postgres) into this shape; [`ServedSignalRow::into_proto`] then
/// owns field placement and the close-state carry, so a served fact is added
/// once, not once per substrate — two independent serve paths is how the
/// substrates historically came to differ about what a row says on the wire.
pub struct ServedSignalRow {
    /// Signal identifier.
    pub id: String,
    /// Space the row belongs to.
    pub space_id: String,
    /// Document anchor, when the signal has one.
    pub document_id: Option<String>,
    /// The signal's author.
    pub author_did: String,
    /// Birth time, Unix milliseconds — the only time-like field a served
    /// feed row carries.
    pub created_at_ms: i64,
    /// The payload the backend assembled from its detail columns.
    pub payload: Option<kutl_proto::sync::signal::Payload>,
    /// Close state; `Some` serves the row stamped `CLOSED` with its reason.
    pub closed: Option<ServedClose>,
}

/// The close state a served row carries.
pub struct ServedClose {
    /// The stored reason discriminant, served verbatim. A raw `i32` rather
    /// than the enum: proto3 enums are open, so a reason minted by a newer
    /// writer must ride through an older server unchanged instead of
    /// collapsing to UNSPECIFIED.
    pub reason: i32,
}

impl ServedSignalRow {
    /// Serve the row as a wire `Signal`.
    ///
    /// Record-envelope fields stay at their defaults — a projection row does
    /// not carry the envelope. A closed row is stamped `CLOSED` with its
    /// reason: the change feed re-serves a signal when its state changes, and
    /// a re-served row that looked exactly like its CREATED would leave the
    /// reader unable to tell WHAT changed. Open rows keep the default
    /// (unspecified) event.
    #[must_use]
    pub fn into_proto(self) -> kutl_proto::sync::Signal {
        let mut proto = kutl_proto::sync::Signal {
            id: self.id,
            space_id: self.space_id,
            document_id: self.document_id,
            author_did: self.author_did,
            timestamp: self.created_at_ms,
            payload: self.payload,
            ..Default::default()
        };
        if let Some(closed) = self.closed {
            proto.set_event(kutl_proto::sync::SignalEventType::Closed);
            proto.close_reason = closed.reason;
        }
        proto
    }
}

/// Full signal detail returned by [`ChangeBackend::get_signal_detail`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalDetail {
    /// Signal identifier.
    pub id: String,
    /// Space the signal belongs to.
    pub space_id: String,
    /// Document the signal is attached to, if any.
    pub document_id: Option<String>,
    /// DID of the signal author.
    pub author_did: String,
    /// Signal type discriminator (e.g. `"flag"` or `"reply"`).
    pub signal_type: String,
    /// Creation time as milliseconds since epoch.
    pub timestamp: i64,
    /// Flag kind string, for flag signals.
    pub flag_kind: Option<String>,
    /// Audience scope, for flag signals.
    pub audience: Option<String>,
    /// Target DID, for directed signals.
    pub target_did: Option<String>,
    /// Human-readable message, for flag signals.
    pub message: Option<String>,
    /// Original wrapped text for comment-kind flags. Display-only
    /// posterity snapshot — the live anchor binding is in document content
    /// via the `[text]{.cmt #signal-uuid}` marker. `None` for non-comment
    /// flags and for document-level comments without an inline marker.
    pub anchor_text: Option<String>,
    /// Close time as milliseconds since epoch, if the signal has been closed.
    pub closed_at: Option<i64>,
    /// Parent signal identifier, for reply signals.
    pub parent_signal_id: Option<String>,
    /// Parent reply identifier, for nested replies.
    pub parent_reply_id: Option<String>,
    /// Body text, for reply signals.
    pub body: Option<String>,
    /// Nested replies for the detail view.
    pub replies: Vec<SignalReplyDetail>,
    /// Nested reactions for the detail view.
    pub reactions: Vec<SignalReactionDetail>,
    /// The signal's lifecycle audit trail, oldest first.
    ///
    /// Not produced by the backend — the projection holds current state, and a
    /// trail of what a signal has BEEN is a question only the record log can
    /// answer. `handle_mcp_get_signal_detail` fills this from the log after the
    /// backend returns, so a relay with no log simply reports an empty trail
    /// rather than a wrong one.
    #[serde(default)]
    pub transitions: Vec<kutl_signals::summary::TransitionEntry>,
}

/// A reply in a signal detail response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalReplyDetail {
    /// Reply identifier.
    pub id: String,
    /// Parent reply identifier, for nested replies.
    pub parent_reply_id: Option<String>,
    /// DID of the reply author.
    pub author_did: String,
    /// Body text of the reply.
    pub body: String,
    /// Creation time as milliseconds since epoch.
    pub created_at: i64,
}

/// A reaction in a signal detail response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalReactionDetail {
    /// DID of the actor who reacted.
    pub actor_did: String,
    /// Emoji string for the reaction.
    pub emoji: String,
    /// Creation time as milliseconds since epoch.
    pub created_at: i64,
}

/// Map a proto `FlagKind` i32 value to its canonical name.
///
/// Delegates to [`kutl_proto::vocab::flag_kind_to_str`] — the single source
/// of truth shared by every [`ChangeBackend`], the CLI, and the relay MCP
/// tools. Unrecognized values default to `"info"`.
#[must_use]
pub fn kind_i32_to_str(kind: i32) -> &'static str {
    kutl_proto::vocab::flag_kind_to_str(kind)
}

/// Map a proto `AudienceType` i32 value to its canonical name.
///
/// Delegates to [`kutl_proto::vocab::audience_type_to_str`] — the single
/// source of truth shared by every [`ChangeBackend`], the CLI, and the relay
/// MCP tools. Unrecognized values default to `"participant"`.
#[must_use]
pub fn audience_i32_to_str(audience: i32) -> &'static str {
    kutl_proto::vocab::audience_type_to_str(audience)
}

/// Map a proto `CloseReason` to its canonical name.
///
/// The third of this trio, delegating to
/// [`kutl_signals::payloads::close_reason_to_wire`] for the same reason the
/// other two delegate: a backend that spelled the labels out again is how a
/// vocabulary drifts. `Unspecified` folds to `"resolved"` so a malformed reason
/// never stores an empty string.
#[must_use]
pub fn close_reason_to_str(reason: kutl_proto::sync::CloseReason) -> &'static str {
    kutl_signals::payloads::close_reason_to_wire(reason)
}

/// Parse a checkpoint string of the form `"{signal_cursor}:{reg_cursor}"`.
///
/// Both halves are WHOLE positions in the feed they page — the signal position
/// and the registry position — so a checkpoint names a place to resume from
/// without anything left to infer. A checkpoint minted under a different
/// meaning for either number still parses; [`resolve_cursors`] is what refuses
/// to act on it, because it will match no slot in the reader's ring.
pub fn parse_checkpoint(cp: &str) -> Option<(i64, i64)> {
    let (a, r) = cp.split_once(':')?;
    Some((a.parse().ok()?, r.parse().ok()?))
}

/// Where a read should start, and whether a checkpoint the caller supplied was
/// honoured.
///
/// The flag exists because the two outcomes are otherwise identical from the
/// caller's side: a replay that worked and a replay that was silently ignored
/// both return a page. A backend reports the second so an unhonoured replay is
/// diagnosable rather than looking like a quiet space.
pub struct ResolvedCursors {
    /// The `(signal, registry)` position to read from.
    pub cursors: (i64, i64),
    /// A checkpoint was supplied and named no position the ring holds, so the
    /// read resumed at the live cursor rather than rewinding.
    pub checkpoint_unrecognized: bool,
}

impl ResolvedCursors {
    /// Read from the live cursor, with no checkpoint involved.
    #[must_use]
    pub fn live(cursors: (i64, i64)) -> Self {
        Self {
            cursors,
            checkpoint_unrecognized: false,
        }
    }
}

/// Resolve cursor positions, handling checkpoint-based rewind.
///
/// Given the current cursor values and the checkpoint ring (prev1, prev2),
/// determines which cursors to use based on the optional checkpoint parameter.
///
/// A checkpoint that matches no slot falls through to the live cursor rather
/// than erroring — a reader whose checkpoint has aged out of the two-slot ring
/// should keep reading forward, not stop — and says so through
/// [`ResolvedCursors::checkpoint_unrecognized`].
#[must_use]
pub fn resolve_cursors(
    checkpoint: Option<&str>,
    current: (i64, i64),
    prev1: (i64, i64),
    prev2: (i64, i64),
    stored_checkpoint: &str,
) -> ResolvedCursors {
    let Some(cp) = checkpoint else {
        return ResolvedCursors::live(current);
    };
    let rewind = parse_checkpoint(cp).filter(|parsed| {
        // The stored checkpoint string, then each ring slot.
        parse_checkpoint(stored_checkpoint).as_ref() == Some(parsed)
            || prev1 == *parsed
            || prev2 == *parsed
    });
    match rewind {
        Some(parsed) => ResolvedCursors::live(parsed),
        None => ResolvedCursors {
            cursors: current,
            checkpoint_unrecognized: true,
        },
    }
}

/// The projection columns a signal record's payload decomposes into, borrowed
/// from the record for the lifetime of one write.
///
/// The payload kind fills exactly the columns it owns and leaves every other
/// one `None`; [`Self::signal_type`] is the discriminator the read paths use to
/// decide which group to reconstruct.
///
/// **This is the one decomposition.** Every backend goes through it rather
/// than deriving its own from the record's fields, because per-backend
/// decomposition is how two substrates' idea of a projected flag drifts: a
/// rebuild that reads the
/// audience off the record through the precedence accessor while a create
/// path passes the caller's raw `(audience_type, target_did)` pair projects a
/// participant flag one way on write and another on rebuild.
///
/// Identity columns (`id`, `space_id`, `document_id`, `author_did`,
/// `timestamp`) are deliberately NOT here. They are one field access each, and
/// the fold-backed rebuild must key its row to the *authoritative* space rather
/// than to the record's own `space_id` — a distinction that would be invisible
/// if this type carried it (see `ProjectedRow::from_folded`).
#[derive(Default)]
pub struct PayloadColumns<'a> {
    /// The `signals.signal_type` discriminator: `"flag"`, `"reply"`, `"chat"`,
    /// `"decision"`, or `"unknown"` for a record with no payload or a
    /// transition payload.
    pub signal_type: &'static str,
    /// `FlagKind` discriminant; flags only.
    pub flag_kind: Option<i32>,
    /// `AudienceType` discriminant, resolved through the typed-audience-first
    /// precedence rule ([`kutl_proto::vocab::flag_audience_untyped`]); flags
    /// only.
    pub audience: Option<i32>,
    /// Flag message, or a chat's topic.
    pub message: Option<&'a str>,
    /// The addressed participant's DID, or a chat's target; `None` for a
    /// space-wide flag.
    pub target_did: Option<&'a str>,
    /// The anchor excerpt a `FLAG_KIND_COMMENT` record carries. Every other
    /// kind leaves it `None`, and the upsert preserves whatever the create
    /// path recorded rather than nulling it.
    pub anchor_text: Option<&'a str>,
    /// The signal this reply hangs off; replies only.
    pub parent_signal_id: Option<&'a str>,
    /// The reply this reply hangs off, when it is not top-level.
    pub parent_reply_id: Option<&'a str>,
    /// Reply body.
    pub body: Option<&'a str>,
    /// Stable hash of the decision heading; decisions only.
    pub decision_title_hash: Option<&'a str>,
    /// Decision heading text.
    pub decision_title: Option<&'a str>,
    /// Decision heading depth.
    pub decision_depth: Option<i32>,
    /// Hash of the enclosing decision heading, when nested.
    pub decision_parent_hash: Option<&'a str>,
}

impl<'a> PayloadColumns<'a> {
    /// Decompose `record`'s payload into the columns it owns.
    ///
    /// Classifying every kind explicitly — rather than falling through to a
    /// reply-shaped catch-all — is what keeps a chat or decision from being
    /// projected as a blank reply.
    #[must_use]
    pub fn from_record(record: &'a kutl_proto::sync::Signal) -> Self {
        use kutl_proto::sync::signal::Payload;

        let mut cols = Self::default();
        let signal_type = match &record.payload {
            Some(Payload::Flag(f)) => {
                cols.flag_kind = Some(f.kind);
                // Through the precedence accessor, not the raw pair: a peer on
                // a newer build replicates flags that set only the typed
                // audience, and reading `audience_type` directly would project
                // those as UNSPECIFIED.
                let (audience, target_did) = kutl_proto::vocab::flag_audience_untyped(f);
                cols.audience = Some(audience);
                cols.target_did = target_did;
                cols.message = Some(f.message.as_str());
                // Set for `FLAG_KIND_COMMENT` only in practice, but read off
                // the payload for every flag rather than branching on kind —
                // the record owns `anchor_text`, so a rebuild carries it
                // instead of depending on whatever the create path left behind.
                cols.anchor_text = f.anchor_text.as_deref();
                "flag"
            }
            Some(Payload::Reply(r)) => {
                cols.parent_signal_id = Some(r.parent_signal_id.as_str());
                cols.parent_reply_id = r.parent_reply_id.as_deref();
                cols.body = Some(r.body.as_str());
                "reply"
            }
            // `Payload::Chat` reuses `message` for the topic and `target_did`;
            // the read paths read them back off the "chat" discriminator.
            Some(Payload::Chat(c)) => {
                cols.message = c.topic.as_deref();
                cols.target_did = c.target_did.as_deref();
                "chat"
            }
            Some(Payload::Decision(d)) => {
                // Decision resolved-ness is NOT stored as a payload state;
                // it is derived from the fold's
                // close-state.
                cols.decision_title_hash = Some(d.title_hash.as_str());
                cols.decision_title = Some(d.title.as_str());
                cols.decision_depth = Some(d.depth);
                cols.decision_parent_hash = d.parent_hash.as_deref();
                "decision"
            }
            // Both project base columns only. A record with no payload is a
            // legacy or future kind that still folds to a CREATED; a CREATED
            // carrying a TRANSITION payload is malformed, since a transition is
            // never a signal's CREATED — the authored seam rejects that, but the
            // replicated path cannot, so it has to land somewhere.
            Some(Payload::Transition(_)) | None => "unknown",
        };

        Self {
            signal_type,
            ..cols
        }
    }
}

/// The content-overlay decomposition, shared with the fold-backed
/// summary — one definition of which fields an edit can and cannot change,
/// re-exported here for the projection substrates (see
/// [`kutl_signals::content`] for the per-field carried-ness rule).
pub use kutl_signals::content::{ContentOverlay, DecisionContent};

/// The projection's write half: turn one admitted record into rows.
///
/// Separate from [`ChangeBackend`], which reads, because the two answer
/// different questions and only one of them can silently do nothing. There is
/// deliberately **no default body** — a projection that cannot project a record
/// is not a projection, and a defaulted no-op would make a backend that writes
/// nothing indistinguishable from one that wrote successfully: every caller
/// reads the inherited `Ok(())` as success while records silently vanish from
/// that substrate's rows.
///
/// **Reachable only from the admission seam.** `Relay` holds this behind
/// [`crate::relay::signal_log::SignalLogHandle`], whose fields are private to
/// its own module, so the projection write happens in the same function as the
/// log append and a new door cannot append without projecting. See that
/// module's docs.
#[async_trait]
pub trait ProjectionWriter: Send + Sync {
    /// Project one admitted record into the `signals` row and its detail tables.
    ///
    /// `space_id` is the space the record was ADMITTED into, passed separately
    /// rather than read off `record.space_id`: on the replicated path the record
    /// is a peer's bytes, and the admitting seam is the only authority on where
    /// it belongs.
    ///
    /// `record` is otherwise the ONLY source of the row's columns:
    /// identity comes off the record's own fields and everything
    /// payload-shaped comes from [`PayloadColumns::from_record`], so a row and
    /// the record behind it cannot disagree. Passing the record rather than
    /// pre-decomposed positional fields is deliberate: a decomposed signature
    /// makes every backend repeat the decomposition, which is how substrates
    /// drift (see [`PayloadColumns`]).
    ///
    /// **Every event, not just creates.** Implementations dispatch on
    /// [`kutl_proto::sync::Signal::event`]:
    ///
    /// | Event | What the projection does |
    /// |---|---|
    /// | `CREATED` (and `UNSPECIFIED`, its legacy spelling) | insert the row and its detail row, INCLUDING any birth close-state |
    /// | `CLOSED` | record the close: closed-at, reason, note |
    /// | `REOPENED` | clear the close state and log a lifecycle event |
    /// | `TOMBSTONED` | hide the signal — deletion-ladder rung 1 |
    /// | `EDITED` | overlay the content columns per [`ContentOverlay`]; lifecycle untouched |
    ///
    /// The CREATED row carries close-state because a record can be born closed:
    /// a `## = …` decision heading materializes as a CREATED whose
    /// `close_reason` is `RESOLVED`. A projection that bound
    /// only the envelope would show it Open forever.
    ///
    /// A transition for an unknown signal is a NO-OP rather than an error: the
    /// fold parks an orphan transition and applies it once its `CREATED`
    /// arrives (the persist-don't-broadcast rule), so a projection that
    /// errored here would fail an emit the record layer accepted.
    ///
    /// `via_pat_id` is the one column with no home on the record, because it
    /// describes the *session* rather than the signal: the `api_tokens.id`
    /// UUID of the PAT that authored it, or `None` for DID-auth (challenge
    /// response or session) and for every non-authored seam. Stored on
    /// `signals.via_pat_id` for per-PAT attribution. The OSS
    /// sqlite backend ignores it — it has no PATs.
    ///
    /// The commercial PG backend resolves the
    /// `author_account_id` relational anchor from `custodied_keys` at
    /// its own write boundary — callers do NOT pass a pre-resolved
    /// value. The OSS sqlite backend has no accounts table and stores
    /// `author_did` only.
    async fn project_record(
        &self,
        space_id: &str,
        record: &kutl_proto::sync::Signal,
        via_pat_id: Option<&str>,
    ) -> Result<(), ChangeError>;
}

/// Re-derive a space's projection rows from its COMPLETE record set.
///
/// A repair, never a write path. Distinct from [`ProjectionWriter`] so the two
/// cannot be confused at a call site: the per-record write happens on every
/// admission, this happens when a whole space's rows must be reconciled against
/// the log — today only re-seed, which ingests records in arbitrary order and
/// therefore genuinely needs the fold rather than an incremental apply.
///
/// Implementations fold the records with `kutl_signals::fold` and rewrite the
/// rows to match — order-insensitive and idempotent by construction, so a global
/// HLC watermark is never the correctness mechanism.
#[async_trait]
pub trait ProjectionRebuild: Send + Sync {
    /// Rebuild every row for `space_id` from `records`, the complete set for
    /// that space. The caller folds nothing itself.
    async fn rebuild_space(
        &self,
        space_id: &str,
        records: &[kutl_proto::sync::Signal],
    ) -> Result<(), ChangeError>;
}

/// Backend for querying changes in a space.
///
/// Implementations query existing data (signals, document registry)
/// rather than maintaining a parallel event log.
///
/// **Reads and retention only.** The write half is [`ProjectionWriter`] and the
/// repair half is [`ProjectionRebuild`]; neither is a method here, so a caller
/// holding a `&dyn ChangeBackend` cannot write through it.
#[async_trait]
pub trait ChangeBackend: Send + Sync {
    /// Get changes since this DID's last check for the given space.
    ///
    /// Auto-advances the cursor for the next call. If `checkpoint` is
    /// provided, rewinds to that position first for failure recovery.
    async fn get_changes(
        &self,
        did: &str,
        space_id: &str,
        checkpoint: Option<&str>,
    ) -> Result<ChangesResponse, ChangeError>;

    /// This backend's whole-space rebuild, when it has one.
    ///
    /// `None` is a STATEMENT, not an absence: "these rows are maintained
    /// incrementally by [`ProjectionWriter::project_record`] and there is no
    /// fold to re-derive them from." Required rather than defaulted, so that
    /// statement is made deliberately at the impl site instead of being
    /// inherited in silence — a defaulted `None` would hide a backend that
    /// projects nothing.
    fn rebuild(&self) -> Option<&dyn ProjectionRebuild>;

    /// Does `space_id` already have a signal with this id?
    ///
    /// A cheap primary-key probe, deliberately not [`Self::get_signal_detail`]:
    /// that joins replies and reactions to answer a question this one settles
    /// from the key alone, and the authored seam asks it on every create.
    ///
    /// It exists for the supplied-id collision check. A caller may
    /// choose a signal's id — the comment flow must, since the inline
    /// `[text]{.cmt #uuid}` marker has to name the same UUID as the signal — and
    /// a second create naming an id already in use is a caller mistake rather
    /// than an edit.
    ///
    /// Answered from the PROJECTION, which is a derived index rather than the
    /// source of truth, so this is a caller-error guard and not a uniqueness
    /// guarantee: on a durable relay a signal can exist in segments while its
    /// row is missing (a projection write that failed, healed at the next
    /// boot), and that collision goes unseen. The fold is what actually keeps a
    /// duplicate id safe — duplicate `CREATED`s collapse to the min-order one —
    /// so a miss here degrades to the pre-check behaviour rather than to
    /// corruption.
    async fn signal_exists(&self, space_id: &str, signal_id: &str) -> Result<bool, ChangeError>;

    /// Fetch full signal detail with replies and reactions.
    async fn get_signal_detail(
        &self,
        space_id: &str,
        signal_id: &str,
    ) -> Result<SignalDetail, ChangeError>;

    /// Prune data older than the retention window.
    ///
    /// Called periodically by the relay's reaper task. Implementations define
    /// their own retention policy. Returns the number of rows pruned.
    async fn prune(&self, now_ms: i64) -> Result<u64, ChangeError>;
}

/// What a host supplies as its signal projection: reads plus the write half.
///
/// Blanket-implemented, so a backend earns it by implementing both halves and
/// no impl block is written by hand. The relay stores one
/// `Arc<dyn SignalProjection>` rather than a reads-handle and a writes-handle,
/// because two fields would let a deployment wire one and not the other — the
/// same "configured but silently not writing" state this split exists to make
/// unconstructible.
pub trait SignalProjection: ChangeBackend + ProjectionWriter {}

impl<T: ChangeBackend + ProjectionWriter> SignalProjection for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_proto::sync::{
        Audience, AudienceType, ChatPayload, FlagKind, FlagPayload, ReplyPayload, Signal, audience,
        signal,
    };

    fn served_row(closed: Option<ServedClose>) -> ServedSignalRow {
        ServedSignalRow {
            id: "sig-1".to_owned(),
            space_id: "space-1".to_owned(),
            document_id: Some("doc-1".to_owned()),
            author_did: "did:key:author".to_owned(),
            created_at_ms: 7_000,
            payload: None,
            closed,
        }
    }

    #[test]
    fn test_served_closed_row_is_stamped_closed_with_its_reason() {
        let proto = served_row(Some(ServedClose {
            reason: i32::from(kutl_proto::sync::CloseReason::Declined),
        }))
        .into_proto();
        assert_eq!(proto.event(), kutl_proto::sync::SignalEventType::Closed);
        assert_eq!(
            proto.close_reason(),
            kutl_proto::sync::CloseReason::Declined
        );
        assert_eq!(proto.timestamp, 7_000, "birth time, not close time");
    }

    #[test]
    fn test_served_open_row_keeps_the_default_event() {
        let proto = served_row(None).into_proto();
        assert_eq!(
            proto.event(),
            kutl_proto::sync::SignalEventType::Unspecified
        );
        assert_eq!(proto.close_reason, 0);
    }

    #[test]
    fn test_served_unknown_reason_rides_through_verbatim() {
        // proto3 enums are open: a reason minted by a newer writer must not
        // collapse to UNSPECIFIED on an older server.
        let proto = served_row(Some(ServedClose { reason: 99 })).into_proto();
        assert_eq!(proto.close_reason, 99);
    }

    /// Who wrote every fixture below.
    const AUTHOR: &str = "did:key:zAuthor";
    /// Who a directed fixture addresses.
    const TARGET: &str = "did:key:zTarget";
    /// A space member the fixtures never name.
    const BYSTANDER: &str = "did:key:zBystander";

    /// A record authored by [`AUTHOR`] carrying `payload`.
    fn authored(payload: Option<signal::Payload>) -> Signal {
        Signal {
            id: "11111111-1111-4111-8111-111111111111".to_owned(),
            space_id: "22222222-2222-4222-8222-222222222222".to_owned(),
            author_did: AUTHOR.to_owned(),
            record_id: "33333333-3333-4333-8333-333333333333".to_owned(),
            payload,
            ..Default::default()
        }
    }

    /// A flag carrying the typed audience the authored seam mints.
    fn flag_to(to: audience::To) -> Signal {
        authored(Some(signal::Payload::Flag(FlagPayload {
            kind: i32::from(FlagKind::Question),
            message: "who is bringing dessert?".to_owned(),
            audience: Some(Audience { to: Some(to) }),
            ..Default::default()
        })))
    }

    /// A flag addressing one participant by DID.
    fn directed_flag(target: &str) -> Signal {
        flag_to(audience::To::Participant(audience::Participant {
            did: target.to_owned(),
        }))
    }

    /// A flag addressing the whole space.
    fn broadcast_flag() -> Signal {
        flag_to(audience::To::Space(audience::Space {}))
    }

    /// A flag shaped the way rows written before the typed audience are: the
    /// deprecated pair, no typed field.
    #[allow(deprecated)]
    fn legacy_flag(audience_type: AudienceType, target_did: Option<&str>) -> Signal {
        authored(Some(signal::Payload::Flag(FlagPayload {
            kind: i32::from(FlagKind::Info),
            message: "m".to_owned(),
            audience_type: i32::from(audience_type),
            target_did: target_did.map(str::to_owned),
            audience: None,
            anchor_text: None,
        })))
    }

    #[test]
    fn test_signal_reaches_directed_flag_reaches_its_target() {
        assert!(signal_reaches(&directed_flag(TARGET), TARGET));
    }

    /// The author of a directed flag is not in its audience, so this rule
    /// withholds their own flag from them. That is the audience answering
    /// honestly — "who is this addressed to" has one answer, and a surface that
    /// wants to echo an author's own writes back reads the record by id or
    /// keeps its own sent-list rather than widening this.
    #[test]
    fn test_signal_reaches_directed_flag_withheld_from_its_own_author() {
        assert!(!signal_reaches(&directed_flag(TARGET), AUTHOR));
    }

    #[test]
    fn test_signal_reaches_directed_flag_withheld_from_a_third_party() {
        assert!(!signal_reaches(&directed_flag(TARGET), BYSTANDER));
    }

    #[test]
    fn test_signal_reaches_broadcast_flag_reaches_everyone() {
        let flag = broadcast_flag();
        for did in [AUTHOR, TARGET, BYSTANDER] {
            assert!(
                signal_reaches(&flag, did),
                "space-wide flag must reach {did}"
            );
        }
    }

    /// A flag's audience is the only thing this rule reads, so every other
    /// payload reaches everyone — which is what keeps a thread legible to
    /// everybody who can see the flag it hangs off.
    ///
    /// The chat fixture names a `target_did` and still reaches a bystander:
    /// narrowing is a flag's typed audience, not any field that happens to hold
    /// a DID. Chat is not yet authored anywhere, so nothing depends on that
    /// today; a chat-authoring path arriving must decide whether chat narrows
    /// and change this rule if so, rather than assume the field already works.
    #[test]
    fn test_signal_reaches_non_flag_payloads_reach_everyone() {
        let reply = authored(Some(signal::Payload::Reply(ReplyPayload {
            parent_signal_id: "44444444-4444-4444-8444-444444444444".to_owned(),
            parent_reply_id: None,
            body: "i can".to_owned(),
        })));
        let chat = authored(Some(signal::Payload::Chat(ChatPayload {
            topic: Some("dessert".to_owned()),
            target_did: Some(BYSTANDER.to_owned()),
        })));
        for did in [AUTHOR, TARGET, BYSTANDER] {
            assert!(signal_reaches(&reply, did), "reply must reach {did}");
            assert!(signal_reaches(&chat, did), "chat must reach {did}");
        }
    }

    /// A transition record carries no payload — it is not a flag, so it is not
    /// narrowed. Withholding one would hide a close from the space that watched
    /// the flag open.
    #[test]
    fn test_signal_reaches_payloadless_record_reaches_everyone() {
        assert!(signal_reaches(&authored(None), BYSTANDER));
    }

    /// A participant audience with an empty DID addresses nobody. It reaches no
    /// one at all, its own author included: the projection widens this shape to
    /// space-wide so a stored row says something, and doing the same here would
    /// hand an unaddressed flag to every member.
    #[test]
    fn test_signal_reaches_flag_addressed_to_nobody_reaches_no_one() {
        let malformed = directed_flag("");
        for did in [AUTHOR, TARGET, BYSTANDER] {
            assert!(
                !signal_reaches(&malformed, did),
                "an unaddressed flag must not reach {did}"
            );
        }
    }

    /// A row written before the typed audience existed still narrows. Reading
    /// only the typed field would broadcast every legacy directed flag.
    #[test]
    fn test_signal_reaches_reads_the_legacy_audience_pair() {
        let legacy = legacy_flag(AudienceType::Participant, Some(TARGET));
        assert!(signal_reaches(&legacy, TARGET));
        assert!(!signal_reaches(&legacy, BYSTANDER));
    }

    /// The retired role audiences are space-wide, not undeliverable — a flag
    /// replicated from an older peer stays visible rather than vanishing.
    #[test]
    fn test_signal_reaches_retired_role_audience_is_space_wide() {
        let role = legacy_flag(AudienceType::AgentOwners, None);
        assert!(signal_reaches(&role, BYSTANDER));
    }

    /// The page filter is the same rule applied across a batch — a caller sees
    /// exactly what the wake path would have woken them for.
    #[test]
    fn test_retain_addressed_to_keeps_only_what_reaches() {
        let mut page = ChangesResponse {
            signals: vec![
                directed_flag(TARGET),
                directed_flag(BYSTANDER),
                broadcast_flag(),
            ],
            ..Default::default()
        };
        page.retain_addressed_to(TARGET);
        assert_eq!(
            page.signals.len(),
            2,
            "the flag for someone else is dropped"
        );
        assert!(
            page.signals.iter().all(|s| signal_reaches(s, TARGET)),
            "every retained signal reaches the caller"
        );
    }
}
