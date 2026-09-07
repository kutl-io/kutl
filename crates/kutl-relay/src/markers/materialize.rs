//! The OSS record materializer: turns document-marker diffs into signal
//! records on merge. Composes the shared `markers::*` trackers; hands the built
//! records back to the relay actor for the append + project + broadcast so
//! the actor stays the SOLE segment writer. No Redis (that stays commercial).
//!
//! **Re-entrancy + no-drop.** `after_text_merge` runs SYNCHRONOUSLY inside the
//! relay actor's own handling of a content merge. Calling back into the busy
//! actor for a reply would deadlock (the actor cannot process the reply until
//! `after_text_merge` returns). The observer therefore holds a [`MaterializeSender`]
//! — a clone of a DEDICATED UNBOUNDED channel to the actor — and does a
//! NON-BLOCKING `.send(..)` of a [`MaterializedBatch`]. Unbounded matters: the
//! trackers advance their known-sets INSIDE `build_records` (to "this decision
//! now exists") BEFORE the send, so a dropped enqueue would silently and
//! PERMANENTLY lose a user-authored signal — the next merge would not re-emit
//! it and no recovery path exists. Materialized records are the relay's OWN
//! bounded production (a handful per debounced merge), so an unbounded channel
//! is safe (memory bounded by merge rate) and, unlike a bounded `try_send`,
//! never drops under backpressure. This matches the "the fold never discards"
//! philosophy. The send is still non-blocking and awaits no reply, so
//! deadlock-freedom holds; the actor drains the batch on a later loop
//! iteration. The unbounded `.send` fails ONLY when the actor's receiver is
//! dropped (shutdown), where losing the batch is fine.
//!
//! **Signing.** A materialized record is signed by the relay AS materializer:
//! the relay signature goes in the record's `signature` FIELD (via
//! [`kutl_signals::sign_record`] with [`SignatureRole::Materializer`]), NOT in
//! the attestation. For [`kutl_signals::verify_record`] to pass, `actor_did`
//! is set to the relay's DID (the materializer performed this event).
//! `author_did` carries the introducing change's author (the human who wrote
//! the marker) as informational attribution. Absent a relay identity the
//! record degrades to tier-3 asserted: empty signature, `sig_role`
//! `UNSPECIFIED`, no attestation, `actor_did` empty.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kutl_proto::sync::{self, CloseReason, Signal, SignalEventType, SignatureRole, signal};
use tracing::warn;
use uuid::Uuid;

use crate::identity::RelayIdentity;
use crate::markers::comments::{CommentDiff, CommentMarker, CommentTracker};
use crate::markers::decisions::{DecisionDiff, DecisionEvent, DecisionTracker};
use crate::markers::in_doc_flag::{InDocFlag, InDocFlagDiff, InDocFlagTracker};
use crate::markers::stable_id::mention_signal_id;
use crate::observer::{AfterMergeObserver, EditContentMode, MergedEvent};
use crate::relay::SharedSignalClock;

/// What one merge's marker parse produced: the tracker diffs, and the records
/// built from them.
///
/// Handed to an [`AfterMergePublisher`] so a host can fan the same parse out to
/// its own feed without re-parsing the document.
pub struct MarkerOutcome<'a> {
    /// Mentions added/removed this merge.
    pub mentions: &'a InDocFlagDiff,
    /// Decision headings opened/resolved/reopened/removed this merge.
    pub decisions: &'a DecisionDiff,
    /// Comment markers removed or emptied this merge.
    pub comments: &'a CommentDiff,
    /// The records built from those diffs.
    ///
    /// A diff entry says WHICH signal changed; the matching record additionally
    /// carries THIS event's own `record_id`, which the projection uses as the
    /// primary key of the row it writes (a reopen's `signal_lifecycle_events`
    /// id, for one). A publisher that needs to address that row looks the record
    /// up with [`MarkerOutcome::record_for`] — never by zipping against build
    /// order, which silently mis-pairs the moment a bucket changes.
    pub records: &'a [Signal],
}

impl MarkerOutcome<'_> {
    /// The record this merge produced for `signal_id`'s `event` transition.
    ///
    /// Exact rather than positional: one merge produces at most one record per
    /// `(signal id, event type)` — a decision cannot both resolve and reopen in
    /// a single diff, and a mention that is added and removed in one merge
    /// cancels in the tracker's set difference before any record is built.
    #[must_use]
    pub fn record_for(&self, signal_id: &str, event: SignalEventType) -> Option<&Signal> {
        self.records
            .iter()
            .find(|r| r.id == signal_id && r.event() == event)
    }
}

/// A host hook called after the materializer has parsed a merge and built its
/// records.
///
/// There is **one parse** per merge: the materializer runs the trackers, and a
/// host publishes from that parse — it never runs its own copy of the trackers
/// or re-parses the content, which would fork identity and drift.
///
/// **Why the hook receives the diffs as well as the records.** A feed event
/// carries presentation detail a protocol record deliberately does not: a
/// mention's `display_name`, a decision's `depth` and `offset`. Those describe
/// how to render a thing, not what happened, and putting them in an immutable
/// signed record would be the wrong home. The diffs also carry IDENTITY — each
/// entry names the signal id the materializer minted the record under — so a
/// publisher can address exactly the signal the relay wrote instead of inventing
/// its own (the relay is the sole projection writer).
///
/// Runs on the merge path inside the relay actor, so the same rule as
/// [`crate::observer::AfterMergeObserver::after_text_merge`] applies: never
/// await anything that calls back into the actor.
#[async_trait::async_trait]
pub trait AfterMergePublisher: Send + Sync {
    /// Publish one merge's markers. `content` is the post-merge document text
    /// the diffs were parsed from — a publisher that renders a snippet or diff
    /// needs it, and re-reading the document here would race the next merge.
    ///
    /// Errors are the implementation's to log: a feed that fails must not fail
    /// the merge that produced it.
    async fn publish_markers(&self, event: &MergedEvent, content: &str, outcome: MarkerOutcome<'_>);
}

/// A batch of marker-materialized records for one space, sent from the
/// observer to the relay actor over the dedicated UNBOUNDED materialize
/// channel. Carried on its own channel (not the shared bounded command
/// channel) so an enqueue can never drop under backpressure and silently lose
/// a user-authored signal — see the module docs on no-drop.
pub struct MaterializedBatch {
    /// The space the merge that produced these records belongs to.
    pub space_id: String,
    /// The already-built, HLC-stamped, (with identity) MATERIALIZER-signed
    /// records; the actor validates, appends, projects, and broadcasts each.
    pub records: Vec<Signal>,
}

/// Non-blocking, never-full sender the observer uses to hand materialized
/// batches back to the relay actor. See [`MaterializedBatch`].
pub type MaterializeSender = tokio::sync::mpsc::UnboundedSender<MaterializedBatch>;

/// Actor-side receiver drained by the relay's command loop as an additional
/// select branch. See [`MaterializeSender`].
pub type MaterializeReceiver = tokio::sync::mpsc::UnboundedReceiver<MaterializedBatch>;

/// Observes merges and materializes marker-derived signal records.
///
/// Holds the shared trackers, a monotonic clock for HLC stamping, an optional
/// relay identity for MATERIALIZER signing, and a non-blocking, never-full
/// handle back to the relay actor's command loop.
pub struct RecordMaterializingObserver {
    /// In-doc flag (`@[Name](kind:id)`) tracker — mention add/remove.
    mentions: InDocFlagTracker,
    /// Decision heading tracker — open/resolve/reopen/remove.
    decisions: DecisionTracker,
    /// Comment marker tracker — lifecycle-only removal/emptying.
    comments: CommentTracker,
    /// The relay signing identity, when adopted. Absent → tier-3.
    relay_identity: Option<Arc<RelayIdentity>>,
    /// Monotonic clock stamping every materialized record's HLC. This is the
    /// SAME clock the relay actor stamps MCP create/close/reopen records from
    /// (cloned in via [`RecordMaterializingObserver::new`]), so a materialized
    /// transition and an MCP transition for one signal within a single
    /// wall-millisecond order by their HLC — causally, not by a random
    /// actor-uuid tiebreak. The `Mutex` guard is never held across an `.await`
    /// and, because `after_text_merge` runs on the actor task, is uncontended.
    clock: SharedSignalClock,
    /// The host's feed publisher, when one is installed.
    ///
    /// `None` on a self-hosted relay, which has no second consumer of the
    /// parse. kutlhub supplies one to emit its Redis feed events from this
    /// single parse.
    publisher: Option<Arc<dyn AfterMergePublisher>>,
    /// The account directory, when this deployment has one.
    ///
    /// Resolves the *account id* an in-doc mention marker carries into the
    /// `target_did` the record field wants. `None` on a relay with no
    /// membership backend — an OSS relay has no accounts to look up, so every
    /// mention it sees degrades, which is an ordinary configuration state.
    directory: Option<Arc<dyn crate::membership_backend::MembershipBackend>>,
    /// Memoized `account_id → did`, so a mention costs at most one lookup per
    /// account per process.
    ///
    /// Caching is CORRECT here, not merely fast: a custodied key is minted once
    /// per account and the directory's oldest-first pick cannot change, so an
    /// entry only ever goes absent → present. A hit can never be stale in a
    /// harmful direction. Misses are deliberately NOT cached — that is the case
    /// that might resolve later, once the account is provisioned.
    resolved_dids: Mutex<HashMap<String, String>>,
    /// Non-blocking, UNBOUNDED handle to the actor's materialize channel.
    /// Materialized records ride a [`MaterializedBatch`] via `.send`, which
    /// never drops under backpressure — see the module docs on no-drop.
    materialize_tx: MaterializeSender,
}

impl RecordMaterializingObserver {
    /// Build a materializer.
    ///
    /// `materialize_tx` is a clone of the actor's dedicated UNBOUNDED
    /// materialize sender (it MUST already exist when the observer is
    /// constructed — see the module docs on re-entrancy). `relay_identity` is
    /// the loaded relay identity when the operator adopted one; `None` degrades
    /// every record to tier-3 asserted. `signal_clock` is a CLONE of the actor's
    /// shared HLC clock, so materialized records and the actor's own MCP
    /// transitions stamp from one monotonic source.
    pub fn new(
        materialize_tx: MaterializeSender,
        relay_identity: Option<Arc<RelayIdentity>>,
        signal_clock: SharedSignalClock,
        directory: Option<Arc<dyn crate::membership_backend::MembershipBackend>>,
        publisher: Option<Arc<dyn AfterMergePublisher>>,
    ) -> Self {
        Self {
            publisher,
            mentions: InDocFlagTracker::new(),
            decisions: DecisionTracker::new(),
            comments: CommentTracker::new(),
            relay_identity,
            clock: signal_clock,
            directory,
            resolved_dids: Mutex::new(HashMap::new()),
            materialize_tx,
        }
    }

    /// Resolve a mention marker's account id to a DID, memoized.
    ///
    /// `None` means "this relay cannot address that person" — no directory
    /// configured, or an account with no custodied key. The caller degrades;
    /// it never mints.
    async fn resolve_target(&self, account_id: &str) -> Option<String> {
        if let Some(hit) = self
            .resolved_dids
            .lock()
            .expect("resolved-did cache poisoned")
            .get(account_id)
        {
            return Some(hit.clone());
        }
        let directory = self.directory.as_ref()?;
        match directory.resolve_account_to_did(account_id).await {
            Ok(Some(did)) => {
                self.resolved_dids
                    .lock()
                    .expect("resolved-did cache poisoned")
                    .insert(account_id.to_owned(), did.clone());
                Some(did)
            }
            Ok(None) => None,
            Err(e) => {
                warn!(
                    error = %e,
                    account_id,
                    "mention target lookup failed; materializing unaddressed"
                );
                None
            }
        }
    }

    /// Build the full set of records for a merge without sending them.
    ///
    /// Split out so the record-building logic is unit-testable without a live
    /// actor channel. Returns records in a stable order: mentions, then
    /// decisions (opened/resolved/reopened/removed), then comments.
    /// The records for one merge, discarding the diffs. Retained because most
    /// tests care only about what was materialized.
    #[cfg(test)]
    async fn build_records(&self, event: &MergedEvent, content: &str) -> Vec<Signal> {
        self.parse_merge(event, content).await.3
    }

    /// Parse one merge: advance the trackers, build the records, and return
    /// BOTH — the diffs carry presentation detail the records deliberately do
    /// not, and a host publisher needs each half (see [`AfterMergePublisher`]).
    async fn parse_merge(
        &self,
        event: &MergedEvent,
        content: &str,
    ) -> (InDocFlagDiff, DecisionDiff, CommentDiff, Vec<Signal>) {
        let mut records = Vec::new();

        // --- Mentions (in-doc flags): added → CREATED, edited → EDITED,
        // removed → CLOSED. ---
        let flag_diff = self
            .mentions
            .update(&event.space_id, &event.document_id, content);
        for flag in &flag_diff.added {
            records.push(
                self.mention_record(event, flag, SignalEventType::Created)
                    .await,
            );
        }
        for flag in &flag_diff.edited {
            records.push(
                self.mention_record(event, flag, SignalEventType::Edited)
                    .await,
            );
        }
        for flag in &flag_diff.removed {
            records.push(
                self.mention_record(event, flag, SignalEventType::Closed)
                    .await,
            );
        }

        // --- Decisions: keyed by title hash. ---
        let decision_diff = self
            .decisions
            .update(&event.space_id, &event.document_id, content);
        for d in &decision_diff.opened {
            records.push(self.decision_record(event, d, SignalEventType::Created, None));
        }
        for d in &decision_diff.born_resolved {
            // Born resolved (a first-seen `## = …`): a CREATED that carries
            // close_reason=RESOLVED, meaning "born already Closed(Resolved)".
            // The fold seeds status Closed(Resolved) from the create's reason,
            // so it becomes a VISIBLE signal — an orphan CLOSE with no CREATED
            // would park in the fold forever.
            records.push(self.decision_record(
                event,
                d,
                SignalEventType::Created,
                Some(CloseReason::Resolved),
            ));
        }
        for d in &decision_diff.renamed {
            // A rename is content, not lifecycle: an EDITED carrying the
            // payload as it reads now. Emitted BEFORE any
            // transition from the same merge, so a resolve-with-rename reads
            // "edited, then closed" in the trail — either order folds
            // identically, since the two tracks are independent.
            records.push(self.decision_record(event, d, SignalEventType::Edited, None));
        }
        for d in &decision_diff.resolved {
            // Resolved is a CLOSED whose reason is RESOLVED (the answer landed) —
            // the `? → =` edit transition on an already-created signal.
            records.push(self.decision_record(
                event,
                d,
                SignalEventType::Closed,
                Some(CloseReason::Resolved),
            ));
        }
        for d in &decision_diff.reopened {
            records.push(self.decision_record(event, d, SignalEventType::Reopened, None));
        }
        for d in &decision_diff.removed {
            // A removed decision is a WITHDRAWN close (the marker vanished).
            records.push(self.decision_record(
                event,
                d,
                SignalEventType::Closed,
                Some(CloseReason::Withdrawn),
            ));
        }

        // --- Comments: removal/emptying → CLOSED(WITHDRAWN), anchor change
        // → EDITED. ---
        let comment_diff = self
            .comments
            .update(&event.space_id, &event.document_id, content);
        for marker in &comment_diff.edited {
            records.push(self.comment_edit_record(event, marker));
        }
        for marker in comment_diff.removed.iter().chain(&comment_diff.emptied) {
            records.push(self.comment_close_record(event, &marker.signal_id));
        }

        (flag_diff, decision_diff, comment_diff, records)
    }

    /// Build one mention record. `added` → CREATED flag with the mention's
    /// kind + target; `edited` → EDITED carrying the content as it reads now;
    /// `removed` → CLOSED(WITHDRAWN). Keyed on
    /// `(document_id, kind, target_id)` so the EDITED and the CLOSE target
    /// the CREATED signal.
    async fn mention_record(
        &self,
        event: &MergedEvent,
        flag: &InDocFlag,
        kind_event: SignalEventType,
    ) -> Signal {
        let signal_id = mention_signal_id(&event.document_id, &flag.kind, &flag.target_id);
        // An EDITED carries the same full payload a CREATED does — the
        // content NOW — resolved through the same directory path, since the
        // audience is part of the payload it replaces nothing without.
        let payload = if matches!(
            kind_event,
            SignalEventType::Created | SignalEventType::Edited
        ) {
            // A mention IS a targeted flag, so its audience is `Participant`
            // addressed to the mentioned person.
            // The marker carries an ACCOUNT ID (the editor's picker knows
            // accounts); the record field wants a DID, so the directory maps it
            // here — before signing, because `target_did` is inside the signed
            // canonical bytes and cannot be filled in afterwards.
            //
            // Unresolvable (no directory, or an account with no custodied key)
            // degrades to a space-audience flag addressed to nobody. Leaving it
            // unmaterialized would lose the author's gesture with no trace,
            // and audience is a delivery filter rather than an access
            // boundary — so degrading costs surfacing, not access. It warns,
            // because an unresolvable account means something is wrong
            // upstream rather than that a user did something unusual.
            let audience = if let Some(did) = self.resolve_target(&flag.target_id).await {
                sync::Audience {
                    to: Some(sync::audience::To::Participant(
                        sync::audience::Participant { did },
                    )),
                }
            } else {
                warn!(
                    account_id = %flag.target_id,
                    document_id = %event.document_id,
                    kind = %flag.kind,
                    "mention target could not be resolved to a DID; \
                     materializing space-audience and unaddressed"
                );
                kutl_proto::vocab::space_audience()
            };
            #[allow(deprecated)]
            Some(signal::Payload::Flag(sync::FlagPayload {
                kind: flag_kind_to_i32(&flag.kind),
                message: flag.message_or_mention(),
                audience: Some(audience),
                anchor_text: None,
                // The mention SUBJECT — the marker's account id — rides the
                // deprecated pair field, because the typed audience cannot
                // carry it: a degraded mention has no participant at all, and
                // a resolved one carries the DID, not the account id the
                // tracker's identity and re-derivation need. This is the
                // space-audience-with-a-target shape the materialized seam
                // exists for; without it every mention would be DROPPED from
                // the rebuilt known-set on restart and re-emitted on the next
                // merge.
                target_did: Some(flag.target_id.clone()),
                ..Default::default()
            }))
        } else {
            // Transitions carry no payload; the fold keys off the signal id.
            None
        };
        self.finish_record(
            event,
            &signal_id,
            kind_event,
            payload,
            matches!(kind_event, SignalEventType::Closed).then_some(CloseReason::Withdrawn),
        )
    }

    /// Build one decision record under the id the tracker has carried for this
    /// decision since it was first seen ([`DecisionEvent::signal_id`]).
    ///
    /// Deriving the id here from the CURRENT `title_hash` instead would be
    /// wrong: a resolution that also rewrites the heading changes the hash, so
    /// the CLOSE would name a signal with no CREATED and park in the fold
    /// forever.
    ///
    /// `close_reason` is `Some` on the CLOSED branches (RESOLVED for a `? → =`
    /// resolution, WITHDRAWN for a removal) AND on a born-resolved CREATED
    /// (RESOLVED — a `## = …` heading born already closed);
    /// the caller chooses which. On a CREATED it is threaded onto the record
    /// verbatim, so the fold seeds the birth close-state from it.
    fn decision_record(
        &self,
        event: &MergedEvent,
        d: &DecisionEvent,
        kind_event: SignalEventType,
        close_reason: Option<CloseReason>,
    ) -> Signal {
        // An EDITED carries the same complete payload a CREATED does — the
        // heading as it reads NOW; a decision edit is a full
        // content replacement, never a partial overlay.
        let payload = if matches!(
            kind_event,
            SignalEventType::Created | SignalEventType::Edited
        ) {
            // Born-resolved (`## = …`) is encoded by the CREATED's close_reason
            // (threaded in via the caller), not by a payload state field —
            // the payload deliberately carries no state.
            Some(signal::Payload::Decision(sync::DecisionPayload {
                title_hash: d.title_hash.to_string(),
                title: d.title.clone(),
                depth: i32::from(d.depth),
                parent_hash: d.parent_hash.map(|h| h.to_string()),
            }))
        } else {
            None
        };
        self.finish_record(event, &d.signal_id, kind_event, payload, close_reason)
    }

    /// Build one comment-marker CLOSE(WITHDRAWN) record. The marker carries a
    /// real signal uuid, so the signal id is that uuid verbatim.
    fn comment_close_record(&self, event: &MergedEvent, signal_id: &str) -> Signal {
        self.finish_record(
            event,
            signal_id,
            SignalEventType::Closed,
            None,
            Some(CloseReason::Withdrawn),
        )
    }

    /// Build one comment-anchor EDITED record.
    ///
    /// The payload carries ONLY what the marker knows: the comment's kind and
    /// the anchor as it reads now. `message` stays empty deliberately — the
    /// comment's body is not in the document, and an empty message is the
    /// overlay rule's "not carried", so the projection preserves the authored
    /// body (see `ContentOverlay`). Space audience, matching where a comment
    /// is visible.
    fn comment_edit_record(&self, event: &MergedEvent, marker: &CommentMarker) -> Signal {
        let payload = Some(signal::Payload::Flag(sync::FlagPayload {
            kind: i32::from(sync::FlagKind::Comment),
            audience: Some(kutl_proto::vocab::space_audience()),
            anchor_text: Some(marker.anchor_text.clone()),
            ..Default::default()
        }));
        self.finish_record(
            event,
            &marker.signal_id,
            SignalEventType::Edited,
            payload,
            None,
        )
    }

    /// Populate the record envelope, stamp the HLC, and sign as MATERIALIZER
    /// (or leave asserted, tier-3, without an identity).
    ///
    /// - `record_id` — fresh v4 UUID for THIS event.
    /// - `hlc` — the relay's shared monotonic clock, ticked once per record.
    /// - `author_did` — the introducing change's author (informational).
    /// - WITH identity: `actor_did` = relay DID, then `sign_record(…,
    ///   Materializer)` fills the `signature` field and `sig_role`.
    /// - WITHOUT identity: `actor_did` empty, `sig_role` UNSPECIFIED, empty
    ///   `signature`, no attestation.
    fn finish_record(
        &self,
        event: &MergedEvent,
        signal_id: &str,
        kind_event: SignalEventType,
        payload: Option<signal::Payload>,
        close_reason: Option<CloseReason>,
    ) -> Signal {
        let hlc: sync::Hlc = self
            .clock
            .lock()
            .expect("signal clock lock poisoned")
            .tick(event.timestamp.max(0).cast_unsigned())
            .into();

        let mut record = Signal {
            id: signal_id.to_owned(),
            space_id: event.space_id.clone(),
            document_id: Some(event.document_id.clone()),
            author_did: event.author_did.clone(),
            timestamp: event.timestamp,
            record_id: Uuid::new_v4().to_string(),
            hlc: Some(hlc),
            payload,
            ..Default::default()
        };
        record.set_event(kind_event);
        if let Some(reason) = close_reason {
            record.set_close_reason(reason);
        }

        if let Some(identity) = self.relay_identity.as_deref() {
            // The relay performed THIS materialization event: actor_did = the
            // relay DID so verify_record checks the signature against it.
            identity.did().clone_into(&mut record.actor_did);
            kutl_signals::sign_record(
                &mut record,
                identity.signing_key(),
                SignatureRole::Materializer,
            );
        }
        // else: tier-3 asserted — actor_did stays empty, no signature, no
        // attestation, sig_role UNSPECIFIED. author_did still carries the human.

        record
    }
}

#[async_trait::async_trait]
impl AfterMergeObserver for RecordMaterializingObserver {
    fn on_document_unregistered(&self, space_id: &str, document_id: &str) {
        self.mentions.remove(space_id, document_id);
        self.decisions.remove(space_id, document_id);
        self.comments.remove(space_id, document_id);
    }

    /// Rebuild every tracker's known-set for one document from its durable
    /// records, so a restart does not reset the baseline. The relay actor calls this once
    /// per `(space, document)` per process, BEFORE the first `after_text_merge`
    /// for that doc, so the subsequent diff reflects the true durable pre-merge
    /// state instead of an empty in-memory baseline. Each tracker folds only the
    /// records for `document_id` (see the trackers' `rederive_from` docs); a
    /// marker whose CREATED never durably landed is absent and gets re-emitted
    /// on the next merge — the intended recovery path.
    fn seed_doc_from_records(&self, space_id: &str, document_id: &str, records: &[Signal]) {
        self.mentions.rederive_from(space_id, document_id, records);
        self.decisions.rederive_from(space_id, document_id, records);
        self.comments.rederive_from(space_id, document_id, records);
    }

    async fn after_text_merge(&self, event: MergedEvent, content: &str) {
        // Only text merges carry markers; blob edits have no parseable content.
        if event.content_mode != EditContentMode::Text {
            return;
        }
        let (mentions, decisions, comments, records) = self.parse_merge(&event, content).await;

        // Fan the SAME parse out to the host's feed before the records go to
        // the actor. Runs even when no record was built: a
        // merge that changed nothing still produces empty diffs, and a
        // publisher that wants to emit an edit event regardless is entitled to
        // see that. Awaited, so a publisher can do I/O — but never one that
        // calls back into the actor (see `AfterMergePublisher`).
        if let Some(ref publisher) = self.publisher {
            publisher
                .publish_markers(
                    &event,
                    content,
                    MarkerOutcome {
                        mentions: &mentions,
                        decisions: &decisions,
                        comments: &comments,
                        records: &records,
                    },
                )
                .await;
        }

        if records.is_empty() {
            return;
        }
        // NON-BLOCKING, NEVER-FULL send on the dedicated UNBOUNDED materialize
        // channel: the actor is busy inside the merge that called us, so
        // blocking or awaiting a reply here would deadlock. `.send` returns
        // immediately and never reports Full, so a record is never dropped
        // under backpressure (the trackers have already advanced past it —
        // dropping would silently and permanently lose a user signal). It
        // fails only when the actor's receiver is gone (shutdown), where
        // dropping the batch is fine.
        let space_id = event.space_id.clone();
        let count = records.len();
        if self
            .materialize_tx
            .send(MaterializedBatch {
                space_id: event.space_id,
                records,
            })
            .is_err()
        {
            warn!(
                space_id = %space_id,
                dropped = count,
                "materialized records dropped: relay actor shutting down (materialize channel closed)"
            );
        }
    }
}

/// Map a mention marker's flag-kind string to the proto [`sync::FlagKind`]
/// enum value. Mirrors the client + `mcp_tools::parse_flag_kind` mapping;
/// unknown kinds (already filtered by the parser's `VALID_KINDS`) fall back to
/// `INFO` rather than failing the whole merge.
fn flag_kind_to_i32(kind: &str) -> i32 {
    let variant = match kind {
        "completed" => sync::FlagKind::Completed,
        "review_requested" => sync::FlagKind::ReviewRequested,
        "question" => sync::FlagKind::Question,
        "blocked" => sync::FlagKind::Blocked,
        // "info" and any residual unknown kind.
        _ => sync::FlagKind::Info,
    };
    i32::from(variant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_core::Document;
    use kutl_signals::{RecordVerification, verify_record};
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    const SPACE: &str = "11111111-1111-1111-1111-111111111111";
    const DOC: &str = "doc-1";
    const HUMAN_DID: &str = "did:key:zHumanAuthor";

    /// Build a document whose content is `content` (via a single edit).
    fn text_doc(content: &str) -> Document {
        let mut doc = Document::new();
        let agent = doc.register_agent("test").expect("register agent");
        doc.edit(
            agent,
            "test",
            "insert",
            kutl_core::Boundary::Explicit,
            |ctx| ctx.insert(0, content),
        )
        .expect("edit");
        doc
    }

    /// A text merge event with a fixed timestamp.
    fn text_event() -> MergedEvent {
        MergedEvent {
            space_id: SPACE.to_owned(),
            document_id: DOC.to_owned(),
            author_did: HUMAN_DID.to_owned(),
            via_pat_id: None,
            op_count: 1,
            intent: "edit".to_owned(),
            content_mode: EditContentMode::Text,
            timestamp: 1_700_000_000_000,
        }
    }

    /// A fresh shared HLC clock for a test observer (stands in for the actor's).
    fn test_clock() -> crate::relay::SharedSignalClock {
        crate::relay::new_signal_clock()
    }

    /// Build an observer with a throwaway unbounded materialize channel and no
    /// identity.
    fn observer_without_identity() -> RecordMaterializingObserver {
        let (tx, _rx) = mpsc::unbounded_channel();
        RecordMaterializingObserver::new(tx, None, test_clock(), None, None)
    }

    /// Build an observer with a real relay identity loaded under `dir`.
    fn observer_with_identity(dir: &TempDir) -> (RecordMaterializingObserver, String) {
        let id = RelayIdentity::load_or_generate(dir.path()).expect("identity");
        let did = id.did().to_owned();
        let (tx, _rx) = mpsc::unbounded_channel();
        (
            RecordMaterializingObserver::new(tx, Some(Arc::new(id)), test_clock(), None, None),
            did,
        )
    }

    /// Adding a `## ? X` decision heading materializes exactly one CREATED
    /// decision record with a title-hash-derived id; removing it materializes
    /// a CLOSED(WITHDRAWN) record for the SAME signal id.
    #[tokio::test]
    async fn test_decision_added_materializes_created_record() {
        let obs = observer_without_identity();
        let ev = text_event();

        // First observation establishes the "before" baseline (empty), so the
        // added heading surfaces as CREATED.
        let created = obs.build_records(&ev, "## ? Should we ship?").await;
        assert_eq!(created.len(), 1, "one CREATED record: {created:?}");
        let rec = &created[0];
        assert_eq!(rec.event(), SignalEventType::Created);
        assert!(matches!(rec.payload, Some(signal::Payload::Decision(_))));
        let created_id = rec.id.clone();
        assert!(!created_id.is_empty());
        assert!(Uuid::parse_str(&created_id).is_ok(), "id is a uuid");

        // Removing the heading materializes a CLOSED(WITHDRAWN) for the same id.
        let closed = obs.build_records(&ev, "no decisions here").await;
        assert_eq!(closed.len(), 1, "one CLOSED record: {closed:?}");
        let close = &closed[0];
        assert_eq!(close.event(), SignalEventType::Closed);
        assert_eq!(close.close_reason(), CloseReason::Withdrawn);
        assert_eq!(
            close.id, created_id,
            "the withdrawal must reference the created signal id"
        );
    }

    /// Unregistering a document drops the observer's state for it: the same
    /// heading merged again afterwards is a fresh CREATED, as for a document
    /// this observer never saw, rather than an empty diff against the removed
    /// document's markers.
    #[tokio::test]
    async fn test_unregistered_document_state_is_dropped() {
        let obs = observer_without_identity();
        let ev = text_event();
        let created = obs.build_records(&ev, "## ? Should we ship?").await;
        assert_eq!(created.len(), 1, "one CREATED record: {created:?}");
        assert!(
            obs.build_records(&ev, "## ? Should we ship?")
                .await
                .is_empty(),
            "unchanged content diffs to nothing while the document is known"
        );
        obs.on_document_unregistered(SPACE, DOC);
        let again = obs.build_records(&ev, "## ? Should we ship?").await;
        assert_eq!(again.len(), 1, "the heading is new again: {again:?}");
        assert_eq!(again[0].event(), SignalEventType::Created);
    }

    /// Removing a mention marker materializes a CLOSED(WITHDRAWN) record whose
    /// id matches the CREATED record minted when the mention appeared.
    #[tokio::test]
    async fn test_mention_removed_materializes_closed_withdrawn() {
        let obs = observer_without_identity();
        let ev = text_event();

        let created = obs
            .build_records(&ev, "hi @[Alice](review_requested:acc-1) please look")
            .await;
        assert_eq!(created.len(), 1, "one CREATED record: {created:?}");
        assert_eq!(created[0].event(), SignalEventType::Created);
        let created_id = created[0].id.clone();

        let closed = obs.build_records(&ev, "hi, no mention now").await;
        assert_eq!(closed.len(), 1, "one CLOSED record: {closed:?}");
        assert_eq!(closed[0].event(), SignalEventType::Closed);
        assert_eq!(closed[0].close_reason(), CloseReason::Withdrawn);
        assert_eq!(closed[0].id, created_id);
        // Transition carries no payload — the fold keys off the signal id.
        assert!(closed[0].payload.is_none());
    }

    /// With a relay identity, a materialized record is signed as MATERIALIZER:
    /// the relay signature lives in the `signature` FIELD (not the
    /// attestation), `actor_did` is the relay DID, and `verify_record` passes.
    #[tokio::test]
    async fn test_materialized_record_signed_as_materializer_with_identity() {
        let dir = TempDir::new().unwrap();
        let (obs, relay_did) = observer_with_identity(&dir);
        let ev = text_event();

        let records = obs.build_records(&ev, "## ? Ship it?").await;
        assert_eq!(records.len(), 1);
        let rec = &records[0];

        assert_eq!(rec.sig_role(), SignatureRole::Materializer);
        assert!(!rec.signature.is_empty(), "signature field must be set");
        assert_eq!(rec.actor_did, relay_did, "actor_did = relay DID");
        // The MATERIALIZER signature IS the relay's cryptographic contribution;
        // there is no separate attestation.
        assert!(
            rec.attestation.is_none(),
            "no attestation on materialize path"
        );
        // author_did still carries the human who wrote the marker.
        assert_eq!(rec.author_did, HUMAN_DID);

        assert!(
            matches!(
                verify_record(
                    rec,
                    kutl_signals::RelayTrust::pinned(std::slice::from_ref(&relay_did))
                ),
                RecordVerification::SignedValid
            ),
            "verify_record must accept the MATERIALIZER signature of a PINNED relay"
        );
        // The same record, byte-identical, read by a verifier that has NOT
        // pinned this relay. This is the pair that shows the binding is real:
        // if both answered `SignedValid`, `RelayTrust` would be decoration.
        assert!(
            matches!(
                verify_record(rec, kutl_signals::RelayTrust::none()),
                RecordVerification::UnboundMaterializer { .. }
            ),
            "an unpinned verifier must not grant tier-1 to a relay-signed record"
        );
    }

    /// A stub directory that maps exactly one account.
    struct OneAccountDirectory {
        account_id: String,
        did: String,
    }

    #[async_trait::async_trait]
    impl crate::membership_backend::MembershipBackend for OneAccountDirectory {
        async fn resolve_account_to_did(&self, account_id: &str) -> anyhow::Result<Option<String>> {
            Ok((account_id == self.account_id).then(|| self.did.clone()))
        }
        async fn check_membership(&self, _: &str, _: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
        async fn resolve_did_to_account(&self, _: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
        async fn resolve_space_by_slugs(
            &self,
            _: &str,
            _: &str,
        ) -> anyhow::Result<Option<crate::membership_backend::SpaceRecord>> {
            Ok(None)
        }
        async fn list_spaces_for_account(
            &self,
            _: &str,
        ) -> anyhow::Result<Vec<crate::membership_backend::SpaceMembershipInfo>> {
            Ok(Vec::new())
        }
        async fn accept_invitation(
            &self,
            _: &str,
            _: &str,
        ) -> anyhow::Result<crate::membership_backend::AcceptInvitationResult> {
            anyhow::bail!("not used")
        }
        async fn list_space_participants(
            &self,
            _: &str,
        ) -> anyhow::Result<Vec<crate::membership_backend::SpaceParticipant>> {
            Ok(Vec::new())
        }
    }

    /// A mention whose account resolves carries `Audience::Participant`
    /// addressed to that DID. The marker holds an ACCOUNT
    /// id; the record must hold a DID, and it has to be resolved BEFORE signing
    /// because `target_did` is inside the signed canonical bytes.
    #[tokio::test]
    async fn test_mention_resolves_its_target_to_a_participant_audience() {
        const ACCOUNT: &str = "11111111-2222-3333-4444-555555555555";
        const DID: &str = "did:key:zMentionedPerson";

        let (tx, _rx) = mpsc::unbounded_channel();
        let obs = RecordMaterializingObserver::new(
            tx,
            None,
            test_clock(),
            Some(Arc::new(OneAccountDirectory {
                account_id: ACCOUNT.to_owned(),
                did: DID.to_owned(),
            })),
            None,
        );

        let records = obs
            .build_records(
                &text_event(),
                &format!("@[Ada](review_requested:{ACCOUNT})"),
            )
            .await;
        assert_eq!(records.len(), 1);
        let Some(sync::signal::Payload::Flag(flag)) = &records[0].payload else {
            panic!("expected a flag payload");
        };
        let Some(sync::audience::To::Participant(p)) =
            flag.audience.as_ref().and_then(|a| a.to.as_ref())
        else {
            panic!("expected a participant audience, got {:?}", flag.audience);
        };
        assert_eq!(
            p.did, DID,
            "the ACCOUNT id must not survive into the record"
        );
    }

    /// A mention's persisted message is the picker-typed body when the author
    /// typed one, and `@DisplayName` when they did not.
    ///
    /// The materializer is the writer, so the fallback must live here: an
    /// empty message would persist as a blank flag card while the live feed
    /// event still read `@Alice`.
    #[tokio::test]
    async fn test_mention_message_falls_back_to_the_display_name() {
        let obs = observer_without_identity();
        let ev = text_event();

        let typed = obs
            .build_records(&ev, "@[Ada](review_requested:acc-1|look at section 2)")
            .await;
        let Some(signal::Payload::Flag(flag)) = &typed[0].payload else {
            panic!("expected a flag payload");
        };
        assert_eq!(flag.message, "look at section 2");

        // No `|` at all, and an empty `|` — a real distinction at the marker
        // boundary, but neither gives a renderer anything to show.
        for marker in ["@[Ada](info:acc-2)", "@[Ada](info:acc-3|)"] {
            let records = obs.build_records(&ev, marker).await;
            let added = records
                .iter()
                .find(|r| r.event() == SignalEventType::Created)
                .expect("one CREATED for the new mention");
            let Some(signal::Payload::Flag(flag)) = &added.payload else {
                panic!("expected a flag payload");
            };
            assert_eq!(flag.message, "@Ada", "marker {marker} lost its fallback");
        }
    }

    /// A mention the directory cannot resolve degrades to space-audience and
    /// unaddressed rather than vanishing. Leaving it unmaterialized would lose
    /// the author's gesture with no trace; audience is a delivery
    /// filter, not an access boundary, so degrading costs surfacing only.
    #[tokio::test]
    async fn test_unresolvable_mention_degrades_to_space_audience() {
        let (tx, _rx) = mpsc::unbounded_channel();
        // No directory at all — an OSS relay, which has no accounts to look up.
        let obs = RecordMaterializingObserver::new(tx, None, test_clock(), None, None);

        let records = obs
            .build_records(
                &text_event(),
                "@[Ada](review_requested:11111111-2222-3333-4444-555555555555)",
            )
            .await;
        assert_eq!(records.len(), 1, "the flag is still materialized");
        let Some(sync::signal::Payload::Flag(flag)) = &records[0].payload else {
            panic!("expected a flag payload");
        };
        assert!(
            matches!(
                flag.audience.as_ref().and_then(|a| a.to.as_ref()),
                Some(sync::audience::To::Space(_))
            ),
            "expected a space audience, got {:?}",
            flag.audience
        );
    }

    /// A relay RESTART preserves its DID, so a record it materialized before
    /// the restart still binds to the pin a client recorded.
    ///
    /// This is the property the whole `known_relays` file rests on: a client
    /// pins once and keeps verifying across restarts. If a restart minted a
    /// fresh identity, every client would see a rotation warning on every relay
    /// bounce and the signal would be worthless. Asserted by loading the
    /// identity twice from one directory — which is exactly what a restart does
    /// — and re-verifying the record signed by the first load against the DID
    /// reported by the second.
    #[tokio::test]
    async fn test_relay_restart_preserves_the_pinned_identity() {
        let dir = TempDir::new().unwrap();
        let (obs, did_before) = observer_with_identity(&dir);
        let records = obs.build_records(&text_event(), "## ? Ship it?").await;
        let rec = &records[0];

        // Restart: a second load from the same directory, as a fresh process
        // would perform.
        drop(obs);
        let after = RelayIdentity::load_or_generate(dir.path()).expect("identity after restart");
        assert_eq!(
            after.did(),
            did_before,
            "a restart must not mint a new relay identity"
        );

        // The client's side: it pinned `did_before` and has not reconnected.
        let pinned = [did_before];
        assert!(
            matches!(
                verify_record(rec, kutl_signals::RelayTrust::pinned(&pinned)),
                RecordVerification::SignedValid
            ),
            "a pre-restart record must still verify against the recorded pin"
        );
    }

    /// Without a relay identity, a materialized record is tier-3 asserted:
    /// empty signature, `sig_role` UNSPECIFIED, no attestation, empty
    /// `actor_did` — but `author_did` still names the human.
    #[tokio::test]
    async fn test_materialized_record_asserted_without_identity() {
        let obs = observer_without_identity();
        let ev = text_event();

        let records = obs.build_records(&ev, "## ? Ship it?").await;
        assert_eq!(records.len(), 1);
        let rec = &records[0];

        assert!(rec.signature.is_empty(), "asserted → no signature");
        assert_eq!(rec.sig_role(), SignatureRole::Unspecified);
        assert!(rec.attestation.is_none());
        assert!(rec.actor_did.is_empty(), "no relay DID without identity");
        assert_eq!(rec.author_did, HUMAN_DID, "human author preserved");
        assert!(matches!(
            verify_record(rec, kutl_signals::RelayTrust::none()),
            RecordVerification::Unsigned
        ));
    }

    /// Resolving a decision materializes a CLOSED(RESOLVED) — distinct from the
    /// WITHDRAWN close a removal produces.
    #[tokio::test]
    async fn test_decision_resolved_materializes_closed_resolved() {
        let obs = observer_without_identity();
        let ev = text_event();
        obs.build_records(&ev, "## ? Which database?").await;
        // Same TITLE, new state: a pure `? → =` resolve with no rename, so
        // exactly one record. (The rewrite-the-heading-too path — the
        // recommended resolve flow — additionally emits an EDITED; that
        // composite is `test_resolve_with_rename_...`'s subject.)
        let resolved = obs.build_records(&ev, "## = Which database?").await;
        assert_eq!(resolved.len(), 1, "one record: {resolved:?}");
        assert_eq!(resolved[0].event(), SignalEventType::Closed);
        assert_eq!(resolved[0].close_reason(), CloseReason::Resolved);
    }

    /// A heading authored DIRECTLY as `## = Settled` is a FIRST-seen resolved
    /// decision — born already resolved. It must materialize a single CREATED
    /// carrying `close_reason=RESOLVED` (NOT an orphan CLOSE-with-no-CREATED,
    /// which would park in the fold forever). Folding the
    /// record yields a VISIBLE signal with status Closed(Resolved).
    #[tokio::test]
    async fn test_born_resolved_decision_materializes_created_with_resolved_reason() {
        use kutl_signals::fold::{SignalStatus, SpaceSignalState};

        let obs = observer_without_identity();
        let ev = text_event();

        // The heading's FIRST-seen state is `=` (resolved) — born resolved.
        let records = obs.build_records(&ev, "## = Settled").await;
        assert_eq!(records.len(), 1, "one born-resolved record: {records:?}");
        let rec = &records[0];
        assert_eq!(
            rec.event(),
            SignalEventType::Created,
            "a born-resolved decision is a CREATED, not an orphan CLOSE"
        );
        assert_eq!(
            rec.close_reason(),
            CloseReason::Resolved,
            "the CREATED carries close_reason=RESOLVED (born already Closed(Resolved))"
        );
        assert!(
            matches!(rec.payload, Some(signal::Payload::Decision(_))),
            "the born-resolved CREATED carries the decision payload"
        );

        // Fold the one record: the born-resolved signal is VISIBLE and
        // Closed(Resolved). An orphan CLOSE would park and yield no signal.
        let mut fold = SpaceSignalState::default();
        fold.apply(rec.clone());
        let state = fold
            .get(&rec.id)
            .expect("the born-resolved signal is visible in the fold");
        assert_eq!(state.status, SignalStatus::Closed);
        assert_eq!(state.close_reason(), Some(CloseReason::Resolved as i32));
    }

    /// A merge with no marker changes yields no records (the common case).
    #[tokio::test]
    async fn test_no_marker_change_yields_no_records() {
        let obs = observer_without_identity();
        let ev = text_event();
        obs.build_records(&ev, "plain text, no markers").await;
        let again = obs
            .build_records(&ev, "plain text, no markers, edited")
            .await;
        assert!(again.is_empty(), "no markers changed: {again:?}");
    }

    /// A removed comment marker materializes a CLOSED(WITHDRAWN) keyed on the
    /// marker's own signal uuid.
    #[tokio::test]
    async fn test_comment_marker_removed_materializes_closed_withdrawn() {
        let obs = observer_without_identity();
        let ev = text_event();
        let uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        obs.build_records(&ev, &format!("[note]{{.cmt #{uuid}}}"))
            .await;
        let closed = obs.build_records(&ev, "note removed").await;
        assert_eq!(closed.len(), 1, "one record: {closed:?}");
        assert_eq!(closed[0].event(), SignalEventType::Closed);
        assert_eq!(closed[0].close_reason(), CloseReason::Withdrawn);
        assert_eq!(closed[0].id, uuid, "id is the marker's own signal uuid");
    }

    /// Blob edits carry no parseable content — `after_text_merge` short-circuits
    /// on the blob-mode guard, so `build_records` is never reached and NOTHING is
    /// sent on the materialize channel. The doc's TEXT form DOES hold a real
    /// marker (a decision heading), so the emptiness is the blob guard's doing,
    /// not an inert fixture. The Text-mode CONTROL below runs the SAME fixture
    /// and asserts it DOES emit — so this test cannot pass by the fixture being
    /// inert.
    #[tokio::test]
    async fn test_blob_edit_emits_nothing() {
        // Hold the receiver so we can observe what (if anything) is sent.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let obs = RecordMaterializingObserver::new(tx, None, test_clock(), None, None);
        // A doc whose text form contains a real marker — a decision heading that
        // WOULD materialize one CREATED record on a Text merge.
        let doc = text_doc("## ? Should we ship?");

        let mut blob_ev = text_event();
        blob_ev.content_mode = EditContentMode::Blob;
        obs.after_text_merge(blob_ev, &doc.content()).await;
        // Blob merges materialize nothing: the receiver is empty.
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "a blob merge must send no materialized batch"
        );

        // CONTROL: the SAME fixture on a Text merge DOES emit — proving the
        // marker is real and the empty blob result is the guard, not inertness.
        let text_ev = text_event();
        obs.after_text_merge(text_ev, &doc.content()).await;
        let batch = rx
            .try_recv()
            .expect("a text merge of the same marker-bearing doc must emit a batch");
        assert_eq!(
            batch.records.len(),
            1,
            "one CREATED decision, got {} records",
            batch.records.len()
        );
        assert_eq!(batch.records[0].event(), SignalEventType::Created);
    }

    /// Materializations ride a DEDICATED
    /// UNBOUNDED channel, so many rapid merges never drop a batch under
    /// backpressure — even far past any plausible bounded-channel cap.
    /// The trackers advance their known-sets inside `build_records` before the
    /// send, so a dropped enqueue would permanently lose a signal; assert every
    /// merge's records reach the receiver.
    #[tokio::test]
    async fn test_rapid_merges_never_drop_under_saturation() {
        // Drive well past a typical bounded-channel capacity (256).
        const MERGES: usize = 1_000;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let obs = RecordMaterializingObserver::new(tx, None, test_clock(), None, None);
        // Each merge presents a doc whose content holds exactly one distinct
        // mention, so the tracker always diffs a change (the previous mention
        // closes, the new one opens) — every merge materializes a non-empty
        // batch. The receiver is NEVER drained mid-loop, standing in for an
        // actor too busy to keep up: a bounded try_send would drop every
        // batch past its cap; the unbounded channel drops none.
        for i in 0..MERGES {
            let ev = text_event();
            let doc = text_doc(&format!("hi @[Alice](review_requested:acc-{i}) look"));
            // Route through after_text_merge (the real send path) so the
            // unbounded `.send` — not build_records — is exercised.
            obs.after_text_merge(ev, &doc.content()).await;
        }
        // The unbounded receiver must hold one batch per merge — nothing was
        // dropped despite draining nothing until now.
        let mut received = 0;
        while rx.try_recv().is_ok() {
            received += 1;
        }
        assert_eq!(
            received, MERGES,
            "every merge's batch must survive; unbounded channel never drops"
        );
    }

    /// A resolution that ALSO rewrites the heading must close the signal that
    /// was created, not a fresh one derived from the new title.
    ///
    /// The differ absorbs a same-offset rewrite by proximity, so `title_hash`
    /// on the CLOSED event is the NEW title's — while the CREATED record was
    /// minted under the OLD one. Deriving the id at record-build time would
    /// therefore produce a CLOSE for a signal that never existed, which the
    /// fold parks forever: the decision would stay Open on every reader and
    /// the resolution lost. Identity is carried by the tracker instead, and
    /// this asserts it.
    #[tokio::test]
    async fn test_resolve_with_rename_closes_the_created_signal() {
        let obs = observer_without_identity();
        let ev = text_event();

        let created = obs.build_records(&ev, "## ? Which database?").await;
        assert_eq!(created.len(), 1, "one CREATED: {created:?}");
        let created_id = created[0].id.clone();

        // Same heading line, resolved AND renamed in one edit — the
        // recommended happy path ("rewrite the question as a statement when
        // resolving"). Two records on two tracks: the EDITED
        // carries the new title — the only durable record of it, since the
        // CLOSE's payload slot belongs to the transition note — then the
        // CLOSED(RESOLVED) moves the lifecycle.
        let resolved = obs.build_records(&ev, "## = We chose Postgres").await;
        assert_eq!(resolved.len(), 2, "EDITED + CLOSED: {resolved:?}");
        assert_eq!(resolved[0].event(), SignalEventType::Edited);
        let Some(signal::Payload::Decision(edited_payload)) = &resolved[0].payload else {
            panic!("the EDITED must carry the decision payload: {resolved:?}");
        };
        assert_eq!(edited_payload.title, "We chose Postgres");
        assert_eq!(resolved[1].event(), SignalEventType::Closed);
        for record in &resolved {
            assert_eq!(
                record.id, created_id,
                "both records must target the signal that was created, not a \
                 title-derived id that has no CREATED"
            );
        }

        // And it really is an id the new title does NOT derive to — otherwise
        // the assertion above would hold even with the broken derivation.
        assert_ne!(
            created_id,
            crate::markers::stable_id::decision_signal_id(
                DOC,
                crate::markers::decisions::hash_title("We chose Postgres")
            ),
            "fixture must actually exercise the rename"
        );
    }

    /// A decision renamed while OPEN and then removed must withdraw the
    /// signal it was created under. A rename is not silent — it materializes
    /// an EDITED on the SAME carried id — and the later withdrawal still
    /// names that id.
    #[tokio::test]
    async fn test_rename_then_remove_withdraws_the_created_signal() {
        let obs = observer_without_identity();
        let ev = text_event();

        let created = obs.build_records(&ev, "## ? Alpha").await;
        let created_id = created[0].id.clone();

        // Same-state rename: an EDITED record carrying the birth identity.
        let renamed = obs.build_records(&ev, "## ? Alpha, restated").await;
        assert_eq!(renamed.len(), 1, "one EDITED: {renamed:?}");
        assert_eq!(renamed[0].event(), SignalEventType::Edited);
        assert_eq!(renamed[0].id, created_id);

        let removed = obs.build_records(&ev, "gone").await;
        assert_eq!(removed.len(), 1, "one CLOSED: {removed:?}");
        assert_eq!(removed[0].close_reason(), CloseReason::Withdrawn);
        assert_eq!(
            removed[0].id, created_id,
            "the withdrawal must name the id the decision was created under"
        );
    }

    /// A mention record round-trips through re-derivation: the tracker
    /// rebuilt from the records the materializer ACTUALLY emitted must
    /// reproduce the known-set, so unchanged content diffs empty after a
    /// restart. This drives the REAL record — the `in_doc_flag` rederive tests
    /// use hand-built fixtures with the deprecated audience pair, which would
    /// let a record that dropped the subject field slip through:
    /// the tracker's rederive requires `target_did` (the mention SUBJECT,
    /// which the typed audience cannot carry — a degraded mention has no
    /// participant, and a resolved one carries the DID, not the account id).
    #[tokio::test]
    async fn test_materialized_mention_survives_rederive() {
        let obs = observer_without_identity();
        let ev = text_event();
        let content = "@[Alice](question:acc-1|please review)";
        let records = obs.build_records(&ev, content).await;
        assert_eq!(records.len(), 1);

        // Restart: rebuild the tracker from the emitted records.
        obs.mentions.remove(&ev.space_id, &ev.document_id);
        obs.mentions
            .rederive_from(&ev.space_id, &ev.document_id, &records);

        // Unchanged content must diff EMPTY — a dropped subject field makes
        // the rederived known-set lose the mention, and the next merge then
        // re-emits its CREATED (plus a spurious feed event and bell fanout)
        // on every restart-touched document.
        let diff = obs.mentions.update(&ev.space_id, &ev.document_id, content);
        assert!(diff.added.is_empty(), "spurious re-add: {:?}", diff.added);
        assert!(diff.edited.is_empty(), "phantom edit: {:?}", diff.edited);
    }

    /// A mention message edit materializes an EDITED flag record carrying the
    /// content NOW, under the mention's derived (unchanged) identity.
    #[tokio::test]
    async fn test_mention_message_edit_materializes_edited_record() {
        let obs = observer_without_identity();
        let ev = text_event();

        let created = obs
            .build_records(&ev, "@[Alice](question:acc-1|first draft)")
            .await;
        assert_eq!(created.len(), 1);
        let created_id = created[0].id.clone();

        let edited = obs
            .build_records(&ev, "@[Alice](question:acc-1|second draft)")
            .await;
        assert_eq!(edited.len(), 1, "one EDITED: {edited:?}");
        assert_eq!(edited[0].event(), SignalEventType::Edited);
        assert_eq!(
            edited[0].id, created_id,
            "identity persists across the edit"
        );
        let Some(signal::Payload::Flag(payload)) = &edited[0].payload else {
            panic!("the EDITED must carry the flag payload: {edited:?}");
        };
        assert_eq!(payload.message, "second draft");
    }

    /// A comment-anchor edit materializes an EDITED under the marker's uuid,
    /// carrying the new anchor and an EMPTY message — "not carried" under the
    /// overlay rule, so the projection preserves the authored body.
    #[tokio::test]
    async fn test_comment_anchor_edit_materializes_edited_record() {
        let obs = observer_without_identity();
        let ev = text_event();
        let uuid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

        obs.build_records(&ev, &format!("[before]{{.cmt #{uuid}}}"))
            .await;
        let edited = obs
            .build_records(&ev, &format!("after, moved: [after]{{.cmt #{uuid}}}"))
            .await;
        assert_eq!(edited.len(), 1, "one EDITED: {edited:?}");
        assert_eq!(edited[0].event(), SignalEventType::Edited);
        assert_eq!(edited[0].id, uuid);
        let Some(signal::Payload::Flag(payload)) = &edited[0].payload else {
            panic!("the EDITED must carry the flag payload: {edited:?}");
        };
        assert_eq!(payload.anchor_text.as_deref(), Some("after"));
        assert!(
            payload.message.is_empty(),
            "a comment-anchor edit cannot know the body; empty = not carried"
        );
    }
}
