//! The relay's one door to its record log.
//!
//! **All signal record writes go through here. If you are adding a second, stop.**
//!
//! The [`RecordLog`](crate::record_log::RecordLog) handle is private to this
//! module, and the only things that can reach it are the three admission seams
//! below. A new authored door therefore cannot append a record by finding the
//! handle — the field is not visible to a sibling module — it has to edit this
//! file, which surfaces in review as a diff to a file whose first line says not
//! to. That is what makes "every record was admitted by something" checkable
//! rather than reviewable.
//!
//! The three seams exist because their records need different checks and one
//! function cannot carry all three:
//!
//! - **authored** — this relay minted it for a caller who asked. Tightened
//!   rules apply: they land on callers who can be told to fix their request.
//! - **replicated** — a peer authored it and re-seed pushed it here. Only
//!   well-formedness and space binding, because tightening this path would
//!   retroactively reject records a peer already accepted and lose history.
//! - **materialized** — the marker materializer derived it from a document's
//!   own content. Separate because it *could not pass* the authored rules: it
//!   splits author from actor, and a mention pairs a SPACE audience with a
//!   non-empty `target_did`, the one shape the typed `Audience` has no arm
//!   for.
//!
//! Reads are deliberately NOT restricted. Catch-up, the delete cascades, the
//! projection and the startup heal all need a space's records, and none of them
//! can write. The invariant this module protects is about the append.

use std::sync::Arc;

use kutl_proto::sync::{self as sync, Signal, SignalEventType};
use tracing::error;
use uuid::Uuid;

use crate::change_backend::{ChangeBackend, ChangeError, ProjectionRebuild, SignalProjection};
use crate::observer::SignalRecordEvent;
use crate::record_log::{RecordLog, RecordLogError};
use crate::signal_record::validate_record;

use super::{ConnId, Relay};

/// The relay's record log, with its append reachable only from this module.
///
/// A newtype rather than a bare `Option<Arc<dyn RecordLog>>` on the actor for
/// one reason: Rust field privacy is per-module, and `Relay` is declared in the
/// parent, so a field there is visible to every sibling module — `mcp.rs`,
/// `space_ops.rs`, `lifecycle.rs` and the rest. Moving the handle behind this
/// type moves the privacy boundary to this file.
///
/// # Enforcement
///
/// The log itself is unreachable through this handle — there is no accessor and
/// the field is private, so the only code that can append is the seams below:
///
/// ```compile_fail,E0616
/// use kutl_relay::relay::signal_log::SignalLogHandle;
/// use kutl_relay::record_log::RecordLog;
/// use std::sync::Arc;
///
/// // A door that wants to append reaches for the log. It cannot have it.
/// fn steal(handle: &SignalLogHandle) -> &Option<Arc<dyn RecordLog>> {
///     &handle.log
/// }
/// ```
///
/// That doctest covers what an out-of-crate caller can attempt. The stronger
/// half is not a test at all: a sibling module inside this crate that reached
/// for `self.record_log.log` would fail to compile, so a violating edit never
/// builds rather than failing a test someone could delete.
pub struct SignalLogHandle {
    /// Private to this module. This is the whole enforcement mechanism.
    log: Option<Arc<dyn RecordLog>>,
    /// The projection, private for exactly the reason `log` is.
    ///
    /// Held HERE rather than as a field on `Relay` so that appending a record
    /// and projecting it are the same act, in the same function, with no way
    /// for a caller to do one and forget the other. The forgetting is silent
    /// when it happens: a backend whose write half is an inherited no-op gives
    /// the omitted call nowhere to fail, so the missing rows simply never
    /// surface.
    ///
    /// Note the type: `SignalProjection`, which is reads AND writes together.
    /// Two fields would let a deployment wire one and not the other, which is
    /// the same silent-skip state expressed a different way.
    projection: Option<Arc<dyn SignalProjection>>,
}

impl SignalLogHandle {
    /// Wrap the configured log and projection. Either may be `None`: a relay
    /// that keeps no records (a standalone test or sim actor), or one with no
    /// projection to maintain.
    ///
    /// They are independent — a relay may project without keeping records, and
    /// the projection write below is deliberately NOT gated on the log being
    /// present. Gating it would reintroduce the silent skip through the back
    /// door.
    #[must_use]
    pub fn new(
        log: Option<Arc<dyn RecordLog>>,
        projection: Option<Arc<dyn SignalProjection>>,
    ) -> Self {
        Self { log, projection }
    }

    /// The projection's READ half, for the query paths.
    ///
    /// Deliberately narrowed to `&dyn ChangeBackend`: a sibling module holding
    /// this cannot reach `ProjectionWriter`, so reads stay open (as the module
    /// doc says) while writes stay behind the admission seam. There is no
    /// corresponding write accessor, and that absence is the enforcement.
    #[must_use]
    pub fn reads(&self) -> Option<&dyn ChangeBackend> {
        self.projection.as_deref().map(|p| p as &dyn ChangeBackend)
    }

    /// An OWNED read handle, for work that outlives the borrow — the periodic
    /// prune reaper spawns a task and cannot hold a reference into the actor.
    ///
    /// Upcast to `ChangeBackend` for the same reason [`Self::reads`] is: what
    /// escapes this module must not be able to write.
    #[must_use]
    pub fn reads_owned(&self) -> Option<Arc<dyn ChangeBackend>> {
        self.projection.clone().map(|p| p as Arc<dyn ChangeBackend>)
    }

    /// This projection's whole-space rebuild, when it has one.
    ///
    /// The repair path — see [`crate::change_backend::ProjectionRebuild`]. Only
    /// re-seed uses it, because only re-seed ingests records in an order the
    /// incremental write cannot reason about.
    #[must_use]
    pub fn rebuild(&self) -> Option<&dyn ProjectionRebuild> {
        self.projection.as_deref().and_then(ChangeBackend::rebuild)
    }

    /// Does this relay keep records at all?
    ///
    /// What `supports_signal_records` is advertised from: a relay with no log
    /// cannot serve catch-up, whatever substrate the others use.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.log.is_some()
    }

    /// Every record this relay holds for `space`, or an empty vec when it keeps
    /// none. Reads are open — see the module doc.
    ///
    /// For serving a page, use [`Self::list`]: this reads the whole space.
    pub async fn load_space(&self, space: Uuid) -> Result<Vec<Signal>, RecordLogError> {
        match self.log {
            Some(ref log) => log.load_space(space).await,
            None => Ok(Vec::new()),
        }
    }

    /// Every record this relay holds for one SIGNAL in `space`, or an empty
    /// vec when it keeps none. Reads are open — see the module doc.
    ///
    /// Prefer this over filtering [`Self::load_space`]: a substrate that can
    /// index by signal reads only what it needs — see
    /// [`RecordLog::load_signal`](crate::record_log::RecordLog::load_signal).
    pub async fn load_signal(
        &self,
        space: Uuid,
        signal_id: &str,
    ) -> Result<Vec<Signal>, RecordLogError> {
        match self.log {
            Some(ref log) => log.load_signal(space, signal_id).await,
            None => Ok(Vec::new()),
        }
    }

    /// Every record this relay holds for one DOCUMENT in `space`, or an empty
    /// vec when it keeps none. Reads are open — see the module doc.
    ///
    /// Prefer this over filtering [`Self::load_space`]: a substrate that can
    /// index by document reads only what it needs — see
    /// [`RecordLog::load_document`](crate::record_log::RecordLog::load_document).
    pub async fn load_document(
        &self,
        space: Uuid,
        document_id: &str,
    ) -> Result<Vec<Signal>, RecordLogError> {
        match self.log {
            Some(ref log) => log.load_document(space, document_id).await,
            None => Ok(Vec::new()),
        }
    }

    /// A page's worth of records after `since` — see
    /// [`RecordLog::list`](crate::record_log::RecordLog::list) for the contract,
    /// which is not simply `LIMIT`.
    ///
    /// A relay with no log returns empty rather than erroring, matching
    /// [`Self::load_space`]: "this deployment keeps no records" is an answer,
    /// not a failure.
    pub async fn list(
        &self,
        space: Uuid,
        since: Option<&kutl_proto::sync::Hlc>,
        limit: usize,
    ) -> Result<Vec<Signal>, RecordLogError> {
        match self.log {
            Some(ref log) => log.list(space, since, limit).await,
            None => Ok(Vec::new()),
        }
    }

    /// Release any per-space resources the log holds.
    pub async fn evict(&self, space: Uuid) {
        if let Some(ref log) = self.log {
            log.evict(space).await;
        }
    }

    /// Whether the log holds an open writer for `space` — the observable behind
    /// [`Self::evict`].
    #[must_use]
    pub fn has_open_writer(&self, space: Uuid) -> bool {
        self.log
            .as_ref()
            .is_some_and(|log| log.has_open_writer(space))
    }
}

/// Whether admitting a record fires
/// [`RelayObserver::on_signal_record`](crate::observer::RelayObserver::on_signal_record)
/// — the live feed and bell fan-out on deployments that consume it, the
/// tracing log otherwise.
///
/// Required wherever it appears, never defaulted, and the requirement is the
/// point: a cascaded CLOSE and a single CLOSE are the same record, so the
/// seam cannot tell "one person closed this" from "one of two hundred swept
/// by a document delete" — only the caller knows. A default in either
/// direction re-creates a bug this module exists to prevent. Defaulting to
/// announce turns every cascade into a per-signal storm; defaulting to
/// silent makes a forgotten announcement indistinguishable from a declared
/// one — an omission that presents as success.
///
/// Replication carries no such parameter, deliberately. Every admitted
/// record replicates to every subscriber, so a parameter there could only
/// create a way to be wrong. Announcement is the half of delivery that
/// genuinely depends on caller context the record does not carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Announce {
    /// One caller did one thing; downstream consumers say so.
    Feed,
    /// Declared silence. Legal only when the covering action announces (a
    /// document delete), another publisher announces with context this seam
    /// lacks (the marker materializer's merge-path publisher), or the record
    /// is not news (replicated history; a signal born hidden).
    Silent,
}

impl Relay {
    /// Admit an **authored** record — one this relay minted for a caller who
    /// asked for it — and append it to the segment store.
    ///
    /// The tail of every per-kind builder, and the authored admission seam. The
    /// split matters because authored and replicated records need different
    /// checks and one function cannot carry both: a rule tightened here applies
    /// to callers who can be told to fix their request, whereas tightening
    /// [`Self::append_replicated_record`] would retroactively reject a peer's
    /// already-accepted records.
    ///
    /// What it checks beyond well-formedness:
    ///
    /// - **The audience names someone**, for a payload kind that carries one. The
    ///   builders enforce this too and are the only way to obtain an intent, so
    ///   this is defence in depth at the boundary rather than the primary gate —
    ///   deliberately, because the seam is what a future door reaches even if it
    ///   finds some other way to build a record.
    ///
    /// Caller mistakes surface as [`ChangeError::InvalidArgument`], never
    /// `Internal`: moving a check inward must not turn a user error into a 500.
    /// Genuine faults — a malformed space id, a failed append — stay
    /// `Internal`.
    ///
    /// Validation runs BEFORE the storeless early-return, so it gates a storeless
    /// relay (kutlhub) exactly as it gates a durable one.
    ///
    /// - **A CREATE does not name a signal that already exists.** A caller may
    ///   choose the id — the comment flow must, since the inline
    ///   `[text]{.cmt #uuid}` marker has to name the same UUID as the signal —
    ///   and a second create under an id already in use is a caller mistake, not
    ///   an edit. Only creates are checked: a TRANSITION's `signal_id` is a
    ///   *reference* to an existing signal, so the same condition is required
    ///   there rather than forbidden.
    ///
    ///   Asked of the projection, which is a derived index rather than the
    ///   source of truth, so this is a caller-error guard and not a uniqueness
    ///   guarantee — see [`ChangeBackend::signal_exists`](crate::change_backend::ChangeBackend::signal_exists)
    ///   for the case it cannot see. What keeps a duplicate id *safe* is still
    ///   the fold: duplicate `CREATED`s collapse to the min-order one, so a miss
    ///   here degrades to the previous behaviour rather than to corruption.
    ///
    ///   Note this makes a retried create under the same supplied id an error
    ///   rather than a silent merge. No path retries one today; on the submit
    ///   frames, `client_ref` + `SignalAck` are what make a retry idempotent,
    ///   and this check is what makes the un-idempotent case loud in the
    ///   meantime.
    ///
    /// **Id well-formedness is on the CREATE branch only**, and the distinction
    /// is the whole reason it can be here at all. The shape rule applies to a
    /// *caller-supplied* `signal_id` — the id a CREATE may supply so an inline
    /// `[text]{.cmt #uuid}` marker names the signal it anchors. On a create the
    /// caller is *minting* an identity, and a malformed one is durable garbage.
    /// On a transition the same field is a *reference* to an id that already
    /// exists, and the HTTP transition route accepts non-UUID ids; a blanket
    /// rule would silently make legacy signals untransitionable. Referenced ids
    /// are checked by [`signal_is_in_space`](Relay::signal_is_in_space) instead,
    /// which is the stronger question anyway — "does it exist here", not "is it
    /// shaped right".
    pub(super) async fn append_authored_record(
        &mut self,
        record: &Signal,
        signal_id: &str,
        space_id: &str,
        // The only column with no home on the record: it describes the SESSION
        // that authored this, not the signal. Authored records are
        // the only ones that can have one — a replicated record came from a
        // peer and a materialized one from the relay itself.
        via_pat_id: Option<&str>,
        // Authored records are also the only ones whose announcement the
        // caller must decide — see [`Announce`]. The other two seams own
        // their answer, because it is derivable from what they are.
        announce: Announce,
    ) -> Result<(), crate::change_backend::ChangeError> {
        if let Some(sync::signal::Payload::Flag(flag)) = record.payload.as_ref() {
            match flag.audience.as_ref().and_then(|a| a.to.as_ref()) {
                None => {
                    return Err(crate::change_backend::ChangeError::InvalidArgument {
                        reason: "audience is required".to_owned(),
                    });
                }
                Some(sync::audience::To::Participant(p)) if p.did.is_empty() => {
                    return Err(crate::change_backend::ChangeError::InvalidArgument {
                        reason: "participant audience requires a non-empty did".to_owned(),
                    });
                }
                // Exhaustive on purpose: no wildcard. This seam is the
                // defence-in-depth copy of `authoring.rs::check_audience`, and a
                // `Some(_)` arm would let a NEW `Audience::To` variant through
                // the one layer whose job is to stop exactly that. Listing the
                // variants makes a new arm a compile error here, as it already
                // is there.
                Some(sync::audience::To::Space(_) | sync::audience::To::Participant(_)) => {}
            }
        }
        // UNSPECIFIED is CREATED's legacy spelling (see the proto). EDITED is
        // deliberately NON-create: it targets an existing signal,
        // so the duplicate-id refusal below is required-not-forbidden for it,
        // exactly as for a transition. (No authored door mints one anyway —
        // EDITED reaches the log through the materialized seam.)
        let is_create = matches!(
            record.event(),
            SignalEventType::Created | SignalEventType::Unspecified
        );
        if is_create {
            let invalid =
                |reason: String| crate::change_backend::ChangeError::InvalidArgument { reason };
            crate::ids::check_uuid("signal_id", signal_id).map_err(invalid)?;
            // Absent is legal — a space-level flag and every reply carry no
            // document. Empty-string is the
            // proto's way of spelling absent on the doors whose field is not
            // `optional`, so it reduces to the same thing rather than to a
            // malformed id.
            if let Some(document_id) = record.document_id.as_deref().filter(|d| !d.is_empty()) {
                crate::ids::check_uuid("document_id", document_id).map_err(invalid)?;
            }
            if let Some(backend) = self.record_log.reads()
                && backend.signal_exists(space_id, signal_id).await?
            {
                return Err(invalid(format!(
                    "signal id {signal_id} already exists in this space"
                )));
            }
        }
        self.append_admitted_record(record, signal_id, space_id, via_pat_id, announce)
            .await
    }

    /// Admit a **replicated** record — one authored elsewhere and pushed here by
    /// re-seed — and append it to the segment store.
    ///
    /// The replicated admission seam. Well-formedness and space binding ONLY:
    /// the authored seam's tightened checks must not run here, because these
    /// records were legal when their author wrote them and rejecting them now
    /// would lose history a peer already accepted. The single exception is the
    /// transition-note bound, which lives in `validate_record` and so applies to
    /// both — see [`RecordAdmission::NoteTooLong`](crate::signal_record::RecordAdmission).
    ///
    /// The seam OWNS its one distinguishing rule: cross-tenant space binding
    /// runs HERE (`check_replicated`), not only in the re-seed loop's richer
    /// `admit_replicated_record` gate — so a future second caller cannot
    /// reach the log through this seam with a record bound to another space.
    /// The re-seed path still pre-admits for per-record reject accounting and
    /// advisory provenance logging; the re-run here is cheap and idempotent.
    pub(super) async fn append_replicated_record(
        &mut self,
        record: &Signal,
        signal_id: &str,
        space_id: &str,
    ) -> Result<(), crate::change_backend::ChangeError> {
        crate::signal_record::check_replicated(record, space_id).map_err(|e| {
            crate::change_backend::ChangeError::InvalidArgument {
                reason: e.to_string(),
            }
        })?;
        // Silent, and the seam owns that answer rather than asking its caller:
        // a replicated record is a peer's history, not news, and replaying it
        // into the live feed would re-notify a space about things that
        // happened arbitrarily long ago.
        self.append_admitted_record(record, signal_id, space_id, None, Announce::Silent)
            .await
    }

    /// Admit a **materialized** record — one the marker materializer derived from
    /// a document's own content — and append it.
    ///
    /// The third seam. It is separate from the authored one because a
    /// materialized record *could not pass* the authored rules. It splits author
    /// from actor — the human wrote the marker, the relay performed the append —
    /// and it pairs a SPACE audience with a non-empty `target_did` for a
    /// mention, the one shape the typed `Audience` has no arm for. So this is
    /// not a second copy of a rule; it is a path whose records the authored rule
    /// would refuse.
    ///
    /// Well-formedness only; it reaches the log through the same tail as
    /// everything else rather than holding the handle itself.
    pub(super) async fn append_materialized_record(
        &mut self,
        record: &Signal,
        signal_id: &str,
        space_id: &str,
    ) -> Result<(), crate::change_backend::ChangeError> {
        // Silent, and the seam owns that answer: marker-derived records are
        // announced from the merge path — a deployment's `AfterMergeObserver`
        // publishes them with merge context (marker geometry, snippet) this
        // seam does not hold — so an announcement here would be that event's
        // duplicate wherever such an observer is installed, and on a relay
        // with none there is no feed to tell.
        self.append_admitted_record(record, signal_id, space_id, None, Announce::Silent)
            .await
    }

    /// The shared tail all three seams end in: well-formedness, then the append.
    ///
    /// Private so neither seam can be bypassed by calling "the append" directly —
    /// a door has to pick which admission rules apply to it, which is the whole
    /// point of there being two.
    ///
    /// **The projection write lives here, not at the call sites.** Appending a
    /// record and projecting it are one act: a record that landed but produced
    /// no row is a signal nobody can read, which is indistinguishable from
    /// having dropped it. Fusing them means a new door cannot append without
    /// projecting — there is no second step to remember, where hand-written
    /// append + project pairs at every call site have to agree and silently do
    /// not when one path forgets a half.
    ///
    /// Order is load-bearing: append first, project second. The record log is
    /// the source of truth on both deployments, so a projection
    /// row must never exist for a record that failed to land. The reverse — a
    /// record with no row — is recoverable, by rebuild on a substrate that has
    /// one or by the startup healer.
    ///
    /// Returns `Ok(())` when the record was admitted, appended and projected.
    /// A `None` log or a `None` projection each skip their own half silently
    /// and INDEPENDENTLY: a storeless relay keeps no records, a projectionless
    /// one maintains no rows, and neither implies the other. On any failure the
    /// caller halts the emit (no broadcast) so no peer sees a record this relay
    /// did not durably accept.
    ///
    /// **The announcement lives here too**, behind the [`Announce`] the caller
    /// was required to supply, and last: every stage this relay is configured
    /// for must succeed before the announcement fires (a storeless or
    /// projectionless deployment skips its absent stages, so the guarantee is
    /// "nothing configured failed", not "a segment exists"). The full tail is
    /// validate, append, project, replicate, announce — in that order.
    async fn append_admitted_record(
        &mut self,
        record: &Signal,
        signal_id: &str,
        space_id: &str,
        via_pat_id: Option<&str>,
        announce: Announce,
    ) -> Result<(), crate::change_backend::ChangeError> {
        if let Err(e) = validate_record(record) {
            error!(
                signal_id = %signal_id,
                error = %e,
                "signal record failed well-formedness gate — halting emit"
            );
            return Err(crate::change_backend::ChangeError::Internal(e.to_string()));
        }
        if let Some(ref log) = self.record_log.log {
            // Fail loud on a non-UUID space_id: skipping the append while the
            // caller still broadcasts would silently diverge the store from
            // peers. Space ids are UUIDs (pre-validated upstream), so this is
            // dead in practice — but the halt-the-emit semantics are correct.
            let space_uuid = uuid::Uuid::parse_str(space_id).map_err(|_| {
                error!(
                    signal_id = %signal_id,
                    space_id = %space_id,
                    "space_id is not a UUID; halting emit"
                );
                crate::change_backend::ChangeError::Internal(format!(
                    "space_id must be a UUID; got {space_id:?}"
                ))
            })?;
            let log = Arc::clone(log);
            log.append(space_uuid, record)
                .await
                .inspect_err(|e| {
                    error!(
                        signal_id = %signal_id,
                        error = %e,
                        "failed to append signal record — halting emit"
                    );
                })
                .map_err(|e| crate::change_backend::ChangeError::Internal(e.to_string()))?;
        }
        if let Some(ref projection) = self.record_log.projection {
            let projection = Arc::clone(projection);
            projection
                .project_record(space_id, record, via_pat_id)
                .await
                .inspect_err(|e| {
                    error!(
                        signal_id = %signal_id,
                        error = %e,
                        "failed to project signal record — halting emit"
                    );
                })?;
        }
        self.replicate_record(space_id, record);
        if announce == Announce::Feed {
            self.observer.on_signal_record(SignalRecordEvent {
                record: record.clone(),
                via_pat_id: via_pat_id.map(str::to_owned),
            });
        }
        Ok(())
    }

    /// Push `record` to every connection subscribed to this space's signals.
    ///
    /// **Part of admission, not a step after it.** Appending a record and
    /// putting it on the wire are one act: a record that landed and reached
    /// nobody is a record only the relay has, and no client can ask for it
    /// later — a resume requests records after its cursor, and one it never
    /// received is behind it. Leaving this to the caller made silence the
    /// default a new door inherits, and every door that forgot produced a
    /// record its author was told had been accepted and no peer could read.
    ///
    /// **No filters, and none may be added.** Every subscriber gets every
    /// record. Audience decides who is TOLD and how a record renders, never
    /// what a replica contains — the pull doors and the wake path apply it, and
    /// catch-up has never applied it at all, so narrowing here would only
    /// decide which clients are temporarily behind. Whoever authored it gets it
    /// too: the ack carries an id, not the record, so a submitting client that
    /// was skipped here would be the one client missing what it just wrote.
    ///
    /// Infallible by design. A subscriber whose lane is full is evicted and
    /// told to re-subscribe, which is recovery rather than a failed emit; the
    /// record is already durable by the time this runs.
    fn replicate_record(&mut self, space_id: &str, record: &Signal) {
        let Some(subs) = self.signal_subscribers.get(space_id) else {
            return;
        };
        let recipients: Vec<ConnId> = subs
            .keys()
            .copied()
            .filter(|cid| self.connections.contains_key(cid))
            .collect();
        if recipients.is_empty() {
            return;
        }
        let bytes = crate::protocol::encode_envelope(&sync::SyncEnvelope {
            payload: Some(sync::sync_envelope::Payload::Signal(record.clone())),
        });
        for cid in recipients {
            self.send_signal_broadcast(space_id, cid, &bytes);
        }
    }
}

impl Relay {
    /// Create a reply signal — the shared reply door.
    ///
    /// Both the MCP `create_reply` tool and the `SubmitReply` frame end here,
    /// so a reply authored over the wire and one authored through a tool
    /// produce the same record.
    ///
    /// The caller owns authorization and parent-in-space verification; this is
    /// the build + stamp + append + project + notify tail. A builder rejection
    /// surfaces as `InvalidArgument`, never `Internal`: moving a check inward
    /// must not turn a user mistake into a 500.
    pub(super) async fn relay_reply_signal(
        &mut self,
        space_id: &str,
        parent_signal_id: &str,
        parent_reply_id: Option<&str>,
        body: &str,
        author_did: &str,
        via_pat_id: Option<&str>,
    ) -> Result<String, crate::change_backend::ChangeError> {
        let signal_id = uuid::Uuid::new_v4().to_string();
        let now = kutl_core::env::now_ms();

        // --- build + stamp + validate + segment append ---
        // Order: append to segments FIRST, then record_signal projection, then
        // observer notification. A failure at any stage halts the whole emit.
        // The reply builder carries the parent linkage and the body bound; a
        // reply's audience is inherited from its parent transitively, so it
        // takes none. Carrying the fields into the segment record is
        // what lets a rebuild reconstruct linkage + body from segments alone.
        let intent = kutl_signals::authoring::SignalIntent::reply(
            kutl_signals::authoring::RecordEnvelope {
                space_id: space_id.to_owned(),
                document_id: None,
                signal_id: signal_id.clone(),
                timestamp: now,
            },
            parent_signal_id.to_owned(),
            parent_reply_id.map(str::to_owned),
            body.to_owned(),
        )
        // A builder rejection is a CALLER mistake, so it maps to the client-error
        // variant (HTTP 400), never to Internal — moving a check inward must not
        // turn a user mistake into a 500.
        .map_err(|e| crate::change_backend::ChangeError::InvalidArgument {
            reason: e.to_string(),
        })?;
        self.stamp_and_append_created(
            &intent,
            &signal_id,
            space_id,
            author_did,
            via_pat_id,
            Announce::Feed,
        )
        .await?;
        // Nothing follows. The admission seam appended, projected, replicated
        // and announced in one act; a persistence failure halted this function
        // above, so reaching this line means the reply is durable and readable
        // — no phantom success, and no second step for a future edit to
        // forget.

        Ok(signal_id)
    }
}

/// The four authored doors as they arrive over the wire.
///
/// They live beside the seams on purpose: a submit frame's whole job is to
/// reach an admission seam, and keeping the two in one file is what the
/// module's opening line is about.
impl Relay {
    /// Emit a [`SignalAck`](sync::SignalAck) on the ACK lane.
    ///
    /// The ack lane, not ctrl or data: a rejection must reach its submitter
    /// even when the lane that would carry a broadcast is congested, and the
    /// ack lane is the one that cannot drop. On success this is sent AFTER the
    /// projection commits, so ack implies readable.
    fn send_signal_ack(
        &self,
        conn_id: super::ConnId,
        client_ref: &str,
        signal_id: &str,
        record_id: &str,
        outcome: Result<(), String>,
    ) {
        let ack = sync::SignalAck {
            client_ref: client_ref.to_owned(),
            signal_id: signal_id.to_owned(),
            record_id: record_id.to_owned(),
            success: outcome.is_ok(),
            error: outcome.err(),
        };
        let envelope = sync::SyncEnvelope {
            payload: Some(sync::sync_envelope::Payload::SignalAck(ack)),
        };
        self.send_ack(conn_id, &crate::protocol::encode_envelope(&envelope));
    }

    /// Authored door — flag.
    pub(super) async fn handle_submit_flag(
        &mut self,
        conn_id: super::ConnId,
        msg: &sync::SubmitFlag,
    ) {
        if !self
            .authorize_submit(conn_id, &msg.client_ref, &msg.space_id)
            .await
        {
            return;
        }
        // Relay-asserted, never client-supplied: an authenticated member could
        // otherwise author under someone else's DID — and the PAT half rides
        // along so an agent's flag is attributed to the agent, not its person.
        let author = self.authoritative_identity(conn_id);
        // The frame already carries the typed audience — hand it straight over.
        // Flattening it to the deprecated pair and rebuilding inside the door
        // would round-trip through the one converter that WIDENS an
        // unrecognized audience to space-wide.
        let outcome = self
            .relay_flag_signal(
                msg.signal_id.as_deref(),
                &msg.space_id,
                msg.document_id.as_deref(),
                &author.did,
                msg.kind,
                msg.audience.clone().unwrap_or_default(),
                &msg.message,
                None,
                kutl_core::env::now_ms(),
                author.via_pat_id.as_deref(),
            )
            .await;
        self.ack_submit(conn_id, &msg.client_ref, outcome);
    }

    /// Authored door — comment. Same tail as a flag, with the kind and the
    /// anchor the comment builder needs.
    pub(super) async fn handle_submit_comment(
        &mut self,
        conn_id: super::ConnId,
        msg: &sync::SubmitComment,
    ) {
        if !self
            .authorize_submit(conn_id, &msg.client_ref, &msg.space_id)
            .await
        {
            return;
        }
        let author = self.authoritative_identity(conn_id);
        let outcome = self
            .relay_flag_signal(
                Some(&msg.signal_id),
                &msg.space_id,
                Some(&msg.document_id),
                &author.did,
                i32::from(sync::FlagKind::Comment),
                msg.audience.clone().unwrap_or_default(),
                &msg.message,
                msg.anchor_text.as_deref(),
                kutl_core::env::now_ms(),
                author.via_pat_id.as_deref(),
            )
            .await;
        self.ack_submit(conn_id, &msg.client_ref, outcome);
    }

    /// Authored door — reply.
    ///
    /// The ack matters as much as the append: a submitter that hears nothing
    /// cannot tell a delivered reply from a dropped one.
    pub(super) async fn handle_submit_reply(
        &mut self,
        conn_id: super::ConnId,
        msg: &sync::SubmitReply,
    ) {
        if !self
            .authorize_submit(conn_id, &msg.client_ref, &msg.space_id)
            .await
        {
            return;
        }
        if !self
            .submit_targets_signal_in_space(
                conn_id,
                &msg.client_ref,
                &msg.space_id,
                &msg.parent_signal_id,
            )
            .await
        {
            return;
        }
        let author = self.authoritative_identity(conn_id);
        let outcome = self
            .relay_reply_signal(
                &msg.space_id,
                &msg.parent_signal_id,
                msg.parent_reply_id.as_deref(),
                &msg.body,
                &author.did,
                author.via_pat_id.as_deref(),
            )
            .await;
        self.ack_submit(conn_id, &msg.client_ref, outcome);
    }

    /// Authored door — close / reopen.
    pub(super) async fn handle_submit_transition(
        &mut self,
        conn_id: super::ConnId,
        msg: &sync::SubmitTransition,
    ) {
        // Order matters, and it is not arbitrary:
        //
        // 1. EVENT LEGALITY first — structural, no I/O, and it leaks nothing.
        //    A tombstone is unauthorable whether or not the signal exists, so
        //    saying so does not require looking it up.
        // 2. AUTHORIZATION next, before anything that reveals space contents.
        // 3. SIGNAL-IN-SPACE last, because its answer ("that signal is not
        //    here") is itself information about the space, and only a member
        //    may have it.
        let actor = self.authoritative_identity(conn_id);
        // TOMBSTONED has no wire representation on the authored path: it stays
        // system-minted by the delete cascades. The frame's enum can spell it,
        // so this is the one event check the frame set cannot make structural —
        // downstream it is: the emit path takes the two-variant transition
        // enum, so this door has to produce one or refuse.
        let event = match msg.event() {
            SignalEventType::Closed => super::SignalTransitionEvent::Closed,
            SignalEventType::Reopened => super::SignalTransitionEvent::Reopened,
            _ => {
                self.send_signal_ack(
                    conn_id,
                    &msg.client_ref,
                    &msg.signal_id,
                    "",
                    Err(
                        "only close and reopen are authorable; a tombstone is system-minted"
                            .to_owned(),
                    ),
                );
                return;
            }
        };
        if !self
            .authorize_submit(conn_id, &msg.client_ref, &msg.space_id)
            .await
        {
            return;
        }
        if !self
            .submit_targets_signal_in_space(conn_id, &msg.client_ref, &msg.space_id, &msg.signal_id)
            .await
        {
            return;
        }
        // An unrecognized discriminant is an ERROR, never coerced to a default
        // — the mirror of the `FlagKind` treatment at `space_ops.rs:1598`.
        // `and_then(Result::ok)` would silently rewrite a bad value to `None`,
        // which reads downstream as "unspecified" and renders as `resolved`: a
        // client built against a newer proto would see its close acked
        // `success: true` under the wrong reason.
        let mut close_reason = None;
        if let Some(d) = msg.close_reason {
            let Ok(reason) = sync::CloseReason::try_from(d) else {
                self.send_signal_ack(
                    conn_id,
                    &msg.client_ref,
                    "",
                    "",
                    Err(format!("unknown close_reason discriminant {d}")),
                );
                return;
            };
            close_reason = Some(reason);
        }
        let author = super::space_ops::TransitionAuthor {
            actor_did: &actor.did,
            close_reason,
            note: msg.note.as_deref(),
            via_pat_id: actor.via_pat_id.as_deref(),
        };
        let outcome = self
            .emit_transition_record(&msg.signal_id, &msg.space_id, event, &author)
            .await;
        match outcome {
            Ok(outcome) => {
                // A decision's transition is performed as the marker flip, so
                // there is no synchronous record id: the CLOSED/REOPENED
                // follows via the materializer, and the ack acknowledges the
                // edit. An empty record_id on a successful ack IS that
                // contract.
                let record_id = match outcome {
                    super::space_ops::TransitionOutcome::Recorded(record_id) => record_id,
                    super::space_ops::TransitionOutcome::MarkerFlipped => String::new(),
                };
                self.send_signal_ack(conn_id, &msg.client_ref, &msg.signal_id, &record_id, Ok(()));
            }
            Err(e) => {
                self.send_signal_ack(
                    conn_id,
                    &msg.client_ref,
                    &msg.signal_id,
                    "",
                    Err(e.to_string()),
                );
            }
        }
    }

    /// Ingest a peer's pushed signal history (re-seed) over the WS frame.
    ///
    /// Replication, not authoring, and the difference is why it is a separate
    /// door rather than a fifth submit: this carries whole RECORDS, already
    /// stamped and signed by whichever relay minted them, and they are appended
    /// as-is. The authored frames carry ARGUMENTS precisely so a client cannot
    /// assert an identity field; here the records legitimately came from
    /// somewhere else, and re-stamping them would destroy the history being
    /// replicated. `append_replicated_record` applies the looser rules that
    /// distinction requires.
    ///
    /// A relay may refuse the whole path — `accepts_reseed`, checked inside
    /// [`Relay::handle_signal_reseed`] — because a deployment holding an
    /// attractive signing key should not accept client-pushed history at all.
    ///
    /// **Rejections make the ack a failure.** A re-seed that dropped records
    /// while answering `success: true` would be indistinguishable from one that
    /// took them all, and the pusher would advance its cursor over history the
    /// relay does not have. Duplicates do NOT fail it: re-seed is idempotent by
    /// design, and a duplicate means the relay already holds the record.
    pub(super) async fn handle_submit_reseed(
        &mut self,
        conn_id: super::ConnId,
        msg: sync::SignalReseed,
    ) {
        if !self
            .authorize_submit(conn_id, &msg.client_ref, &msg.space_id)
            .await
        {
            return;
        }
        let did = self.authoritative_author_did(conn_id).to_owned();
        // Taken by value and MOVED: the batch is up to `MAX_RESEED_BATCH`
        // records, and cloning it to read a length copied all of them.
        let record_count = msg.records.len();
        let client_ref = msg.client_ref;
        let outcome = self
            .handle_signal_reseed(&did, &msg.space_id, msg.records)
            .await;

        let result = match outcome {
            Err(e) => Err(e.to_string()),
            Ok(o) if o.rejected > 0 => Err(format!(
                "re-seed rejected {} of {record_count} records ({} appended, {} duplicate)",
                o.rejected, o.appended, o.duplicate
            )),
            Ok(_) => Ok(()),
        };
        self.send_signal_ack(conn_id, &client_ref, "", "", result);
    }

    /// Confirm a signal named by a caller actually lives in the space they were
    /// authorized for.
    ///
    /// Membership alone is not enough for any frame that names a PRE-EXISTING
    /// signal: the caller supplies both ids, so a member of space A could name a
    /// signal from space B and reply to it or transition it.
    ///
    /// Shared by every surface that takes a `(space_id, signal_id)` pair from a
    /// caller — the WS submit frames below and MCP's
    /// [`verify_signal_in_space`](Relay::verify_signal_in_space), which maps the
    /// error into its own shape. One probe, because two would drift: a check
    /// that lives on one surface alone is exactly how another surface ships
    /// without it.
    ///
    /// Reads through `get_signal_detail`, which on both substrates selects from
    /// the active view — so a soft-deleted signal is `NotFound` here, the same
    /// refusal every other surface gives.
    ///
    /// Skipped when no projection is configured: a storeless relay cannot
    /// answer, and refusing every such frame on one would be worse than the risk.
    pub(super) async fn signal_is_in_space(
        &self,
        space_id: &str,
        signal_id: &str,
    ) -> Result<(), ChangeError> {
        let Some(backend) = self.record_log.reads() else {
            return Ok(());
        };
        backend
            .get_signal_detail(space_id, signal_id)
            .await
            .map(drop)
    }

    /// [`Self::signal_is_in_space`] for a submit frame, acking the rejection.
    ///
    /// A projection outage is reported as one — not as "not in this space",
    /// which would tell the caller their perfectly good signal id is wrong.
    async fn submit_targets_signal_in_space(
        &self,
        conn_id: super::ConnId,
        client_ref: &str,
        space_id: &str,
        signal_id: &str,
    ) -> bool {
        let reason = match self.signal_is_in_space(space_id, signal_id).await {
            Ok(()) => return true,
            Err(ChangeError::NotFound(_)) => {
                format!("signal {signal_id} is not in this space")
            }
            Err(e) => format!("cannot verify signal {signal_id}: {e}"),
        };
        self.send_signal_ack(conn_id, client_ref, "", "", Err(reason));
        false
    }

    /// Authorize this connection for the space a submit frame names, acking the
    /// rejection rather than dropping it.
    ///
    /// **Every authored submit frame must call this.** A frame handler that
    /// read `msg.space_id` straight off the wire and went to the door would let
    /// any authenticated connection write a signal into ANY space by naming it,
    /// member or not.
    ///
    /// Every sibling door does this: twelve WS handlers across `space_ops`,
    /// `lifecycle`, `sync_ops` and `doc_load` open by calling
    /// [`authorize_conn`](Relay::authorize_conn), and the HTTP routes go through
    /// `authorize_space`. The submit frames must not be the exception.
    ///
    /// Rejection is ACKED, not silently dropped, because the frame protocol's
    /// whole point is that a submit is answered — a caller that gets silence
    /// cannot distinguish "refused" from "lost".
    async fn authorize_submit(
        &self,
        conn_id: super::ConnId,
        client_ref: &str,
        space_id: &str,
    ) -> bool {
        match self.authorize_conn(conn_id, space_id).await {
            Ok(_authorized) => true,
            Err(e) => {
                self.send_signal_ack(conn_id, client_ref, "", "", Err(e.to_string()));
                false
            }
        }
    }

    /// Ack a create-shaped submit: the signal id on success, the reason on
    /// failure.
    fn ack_submit(
        &self,
        conn_id: super::ConnId,
        client_ref: &str,
        outcome: Result<String, crate::change_backend::ChangeError>,
    ) {
        match outcome {
            Ok(signal_id) => self.send_signal_ack(conn_id, client_ref, &signal_id, "", Ok(())),
            Err(e) => self.send_signal_ack(conn_id, client_ref, "", "", Err(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use kutl_proto::sync::{self, SignalEventType};
    use uuid::Uuid;

    use super::super::connect_client;
    use super::super::space_ops::catch_up_tests::{
        RecordingObserver, relay_with_projection, relay_with_projection_and_observer,
    };

    /// Author one space-level flag and prove the observer is live — the
    /// liveness leg every silence pin needs before its silence claim means
    /// anything. A copy of this that drifts turns a silence pin into a
    /// vacuous assertion, which is why there is one of it.
    async fn author_one_flag_and_prove_the_observer_is_live(
        relay: &mut crate::relay::Relay,
        ack: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        observer: &RecordingObserver,
        space_id: &str,
    ) {
        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-live".into(),
                    space_id: space_id.to_owned(),
                    document_id: None,
                    signal_id: None,
                    kind: i32::from(sync::FlagKind::Info),
                    message: "authored live".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;
        assert!(take_ack(ack).success, "setup: the flag must land");
        assert_eq!(
            observer.announcement_count(),
            1,
            "setup: the observer is live"
        );
    }

    /// Replay `record` into a fresh peer space as pushed history over the
    /// re-seed frame, asserting the frame is accepted and the record landed.
    /// Returns the peer space id and the records it holds afterwards.
    async fn replay_one_record_into_a_peer_space(
        relay: &mut crate::relay::Relay,
        ack: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        record: &sync::Signal,
    ) -> (String, Vec<sync::Signal>) {
        let peer_space = Uuid::new_v4().to_string();
        let mut replayed = record.clone();
        replayed.space_id.clone_from(&peer_space);
        relay
            .handle_submit_reseed(
                1,
                sync::SignalReseed {
                    records: vec![replayed],
                    space_id: peer_space.clone(),
                    client_ref: "ref-reseed".into(),
                },
            )
            .await;
        assert!(take_ack(ack).success, "the re-seed frame must be accepted");
        let stored = relay
            .record_log
            .load_space(Uuid::parse_str(&peer_space).unwrap())
            .await
            .expect("load");
        assert_eq!(stored.len(), 1, "the pushed record must be appended");
        (peer_space, stored)
    }

    /// Decode the ack frame a submit produced, draining the connection's ack
    /// lane until one appears.
    fn take_ack(ack: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) -> sync::SignalAck {
        while let Ok(bytes) = ack.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && let Some(sync::sync_envelope::Payload::SignalAck(a)) = env.payload
            {
                return a;
            }
        }
        panic!("no SignalAck on the ack lane");
    }

    /// A NON-MEMBER's submit is rejected, and the rejection is ACKED.
    ///
    /// A submit frame handler that read `msg.space_id` off the wire and went
    /// straight to the door would let any authenticated connection write a
    /// signal into ANY space by naming it — and the frames are the browser's
    /// primary write path.
    ///
    /// `did:key:zStranger` is deliberately absent from `TEST_AUTHORIZED_DIDS`.
    ///
    /// The ack matters as much as the refusal: a dropped frame leaves the caller
    /// unable to tell "refused" from "lost", which is the silence the ack lane
    /// exists to close.
    #[tokio::test]
    async fn test_submit_flag_from_a_non_member_is_rejected_and_acked() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        // Re-identify as a DID the authorized_keys file does NOT list.
        relay.test_set_authenticated(1, "did:key:zStranger");
        while ack.try_recv().is_ok() {}

        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-stranger".into(),
                    space_id: space_id.clone(),
                    document_id: None,
                    signal_id: None,
                    kind: i32::from(sync::FlagKind::Info),
                    message: "should never land".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;

        let got = take_ack(&mut ack);
        assert!(
            !got.success,
            "a non-member's submit must be refused, got success"
        );

        // And nothing was written. A refusal that still persists the record
        // would be the same hole with a worse error message.
        let records = relay
            .record_log
            .load_space(Uuid::parse_str(&space_id).unwrap())
            .await
            .expect("load");
        assert!(
            records.is_empty(),
            "a refused submit must append nothing, found {} records",
            records.len()
        );
    }

    /// A create whose caller-supplied ids are not UUIDs is refused, and told
    /// which field and what it received.
    ///
    /// The submit doors reach `relay_flag_signal` directly. Unchecked, a
    /// path-shaped `document_id` rides all the way to the projection's
    /// `::uuid` cast and comes back as an opaque driver error — for a mistake
    /// the caller could have been told about in one sentence.
    #[tokio::test]
    async fn test_submit_create_with_non_uuid_ids_is_rejected_and_names_the_field() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        // A path where a document id belongs — the exact shape the projection's
        // UUID-typed column exists to make impossible.
        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-path-doc".into(),
                    space_id: space_id.clone(),
                    document_id: Some("notes/daily-digest.md".into()),
                    signal_id: None,
                    kind: i32::from(sync::FlagKind::Info),
                    message: "should never land".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;
        let got = take_ack(&mut ack);
        assert!(!got.success, "a path is not a document id");
        let err = got.error.unwrap_or_default();
        assert!(
            err.contains("document_id") && err.contains("daily-digest.md"),
            "the refusal must name the field AND echo what was passed, got: {err}"
        );

        // The same rule on the id the caller mints for marker binding.
        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-bad-signal-id".into(),
                    space_id: space_id.clone(),
                    document_id: None,
                    signal_id: Some("flag-7".into()),
                    kind: i32::from(sync::FlagKind::Info),
                    message: "should never land".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;
        let got = take_ack(&mut ack);
        assert!(!got.success, "a caller-minted signal id must be a UUID");
        let err = got.error.unwrap_or_default();
        assert!(
            err.contains("signal_id") && err.contains("flag-7"),
            "the refusal must name the field AND echo what was passed, got: {err}"
        );

        // Neither refusal wrote anything: a rejection that still appends is the
        // same hole with a better error message.
        let records = relay
            .record_log
            .load_space(Uuid::parse_str(&space_id).unwrap())
            .await
            .expect("load");
        assert!(
            records.is_empty(),
            "refused creates must append nothing, found {} records",
            records.len()
        );
    }

    /// A submit frame that omits `audience` is refused, not broadcast.
    ///
    /// Flattening the typed field with `audience_to_untyped` maps `None`
    /// straight to `(Space, None)` — an absent audience becomes a manufactured
    /// space-wide one before the record is ever built, and the seam's "audience
    /// is required" check cannot fire because by then the audience is present
    /// and says `Space`. The door must hand the typed field over untouched.
    ///
    /// The failure mode is the quiet kind: the caller gets `success: true`, the
    /// record is durable and well-formed, and the only thing wrong is that
    /// everyone in the space was notified about something addressed to no one.
    #[tokio::test]
    async fn test_submit_flag_without_an_audience_is_refused_not_broadcast() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-no-audience".into(),
                    space_id: space_id.clone(),
                    document_id: None,
                    signal_id: None,
                    kind: i32::from(sync::FlagKind::Info),
                    message: "addressed to nobody".into(),
                    audience: None,
                },
            )
            .await;

        let got = take_ack(&mut ack);
        assert!(
            !got.success,
            "a frame with no audience must be refused, not silently made space-wide"
        );
        let err = got.error.unwrap_or_default();
        assert!(
            err.contains("audience is required"),
            "and the caller must be told why, got: {err}"
        );

        let records = relay
            .record_log
            .load_space(Uuid::parse_str(&space_id).unwrap())
            .await
            .expect("load");
        assert!(
            records.is_empty(),
            "nothing may be appended for a refused frame, found {}",
            records.len()
        );
    }

    /// The replicated seam itself refuses a record bound to another space —
    /// the cross-tenant guard cannot be skipped by calling the seam without
    /// the re-seed loop's richer admission gate in front of it.
    #[tokio::test]
    async fn test_replicated_seam_rejects_a_foreign_space_binding() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let authorized = Uuid::new_v4().to_string();
        let foreign = Uuid::new_v4();

        let record = super::super::space_ops::catch_up_tests::record_with_id(
            foreign, "sig-x", 1_000, 0, "rForeign",
        );
        let err = relay
            .append_replicated_record(&record, "sig-x", &authorized)
            .await
            .expect_err("a record bound to another space must not reach the log");
        assert!(
            err.to_string().contains("space"),
            "the refusal names the binding: {err}"
        );

        let records = relay
            .record_log
            .load_space(Uuid::parse_str(&authorized).unwrap())
            .await
            .expect("load");
        assert!(records.is_empty(), "nothing may be appended");
    }

    /// A signal whose id is not a UUID can still be closed.
    ///
    /// This is the half of the rule that is easy to lose. The shape check is on
    /// the CREATE branch because a create MINTS an identity, and a malformed one
    /// is durable garbage; a transition REFERENCES an id that already exists,
    /// and the HTTP transition route accepts non-UUID ids today. Records with
    /// such ids are also exactly what replication carries in from an older peer,
    /// and `append_replicated_record` deliberately does not re-judge them.
    ///
    /// So: seed one through the replicated seam, then close it through the
    /// authored transition door. If someone ever "tidies up" by hoisting the
    /// check out of the `is_create` branch, this goes red — and without it, the
    /// symptom in the field would be that a legacy signal becomes impossible to
    /// resolve, with an error blaming its id.
    #[tokio::test]
    async fn test_a_legacy_non_uuid_signal_id_can_still_be_transitioned() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let legacy_id = "legacy-signal-7";

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        let seeded = super::super::space_ops::catch_up_tests::record_with_id(
            space, legacy_id, 1_000, 0, "rLegacy",
        );
        relay
            .append_replicated_record(&seeded, legacy_id, &space_id)
            .await
            .expect("replication accepts a record an older peer already accepted");

        relay
            .handle_submit_transition(
                1,
                &sync::SubmitTransition {
                    client_ref: "ref-close-legacy".into(),
                    space_id: space_id.clone(),
                    signal_id: legacy_id.into(),
                    event: i32::from(SignalEventType::Closed),
                    close_reason: Some(i32::from(sync::CloseReason::Resolved)),
                    note: None,
                },
            )
            .await;

        let got = take_ack(&mut ack);
        assert!(
            got.success,
            "closing a legacy signal must work; got: {:?}",
            got.error
        );
    }

    /// A member of one space cannot transition a signal that lives in another.
    ///
    /// Space membership is not sufficient for a frame that names a
    /// PRE-EXISTING signal: the caller supplies both the space id and the
    /// signal id, so a member of space A could name a signal from space B and
    /// close it. The same hole `mcp.rs::verify_signal_in_space` closes on the
    /// MCP surface, quoted there as "a member of space-A could reply to a
    /// parent signal in space-B".
    #[tokio::test]
    async fn test_submit_transition_cannot_reach_a_signal_in_another_space() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_a = Uuid::new_v4().to_string();
        let space_b = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        // A flag that lives in space B.
        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-b".into(),
                    space_id: space_b.clone(),
                    document_id: None,
                    signal_id: None,
                    kind: i32::from(sync::FlagKind::Info),
                    message: "lives in B".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;
        let created = take_ack(&mut ack);
        assert!(created.success, "setup: the flag must land in space B");
        let signal_id = created.signal_id;

        // Now close it while claiming space A. The caller is authorized for A
        // (the test DID is allowlisted for every space), so ONLY the
        // signal-in-space check can stop this.
        let mut msg = sync::SubmitTransition {
            client_ref: "ref-cross".into(),
            space_id: space_a,
            signal_id: signal_id.clone(),
            event: 0,
            close_reason: Some(i32::from(sync::CloseReason::Resolved)),
            note: None,
        };
        msg.set_event(SignalEventType::Closed);
        relay.handle_submit_transition(1, &msg).await;

        let got = take_ack(&mut ack);
        assert!(
            !got.success,
            "closing a signal from another space must be refused, got success"
        );
        assert!(
            got.error
                .as_deref()
                .is_some_and(|e| e.contains("not in this space")),
            "the rejection should name the reason, got {:?}",
            got.error
        );
    }

    /// A `SubmitReply` is accepted and acked — a reply authored over the wire
    /// is durable, and its submitter hears so on the ack lane.
    #[tokio::test]
    async fn test_submit_reply_is_accepted_and_acked() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        // A parent flag to reply to.
        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-flag".into(),
                    space_id: space_id.clone(),
                    document_id: Some(doc.clone()),
                    kind: i32::from(sync::FlagKind::Question),
                    message: "shall we?".into(),
                    audience: Some(kutl_proto::vocab::audience_from_untyped(
                        i32::from(sync::AudienceType::Space),
                        None,
                    )),
                    signal_id: None,
                },
            )
            .await;
        let flag_ack = take_ack(&mut ack);
        assert!(flag_ack.success, "flag submit failed: {:?}", flag_ack.error);
        assert_eq!(
            flag_ack.client_ref, "ref-flag",
            "the ack correlates by client_ref"
        );
        let parent_id = flag_ack.signal_id;

        relay
            .handle_submit_reply(
                1,
                &sync::SubmitReply {
                    client_ref: "ref-reply".into(),
                    space_id: space_id.clone(),
                    parent_signal_id: parent_id.clone(),
                    parent_reply_id: None,
                    body: "yes".into(),
                },
            )
            .await;
        let reply_ack = take_ack(&mut ack);
        assert!(
            reply_ack.success,
            "a reply over the wire must be accepted: {:?}",
            reply_ack.error
        );
        assert_eq!(reply_ack.client_ref, "ref-reply");
        assert_ne!(
            reply_ack.signal_id, parent_id,
            "the reply is its own signal"
        );
    }

    /// `SubscribeSignals` returns the space's backlog as a `SignalPage` on the
    /// data lane — catch-up over the socket, with no HTTP route involved.
    #[tokio::test]
    async fn test_subscribe_signals_pages_the_backlog() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        let (mut data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}
        while data.try_recv().is_ok() {}

        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-1".into(),
                    space_id: space_id.clone(),
                    document_id: Some(doc),
                    kind: i32::from(sync::FlagKind::Info),
                    message: "in the backlog".into(),
                    audience: Some(kutl_proto::vocab::audience_from_untyped(
                        i32::from(sync::AudienceType::Space),
                        None,
                    )),
                    signal_id: None,
                },
            )
            .await;
        assert!(take_ack(&mut ack).success);
        while data.try_recv().is_ok() {}

        relay
            .handle_subscribe_signals(
                1,
                &sync::SubscribeSignals {
                    space_id: space_id.clone(),
                    cursor: None,
                },
            )
            .await;

        let mut page = None;
        while let Ok(bytes) = data.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && let Some(sync::sync_envelope::Payload::SignalPage(p)) = env.payload
            {
                page = Some(p);
            }
        }
        let page = page.expect("a SignalPage must arrive on the data lane");
        assert_eq!(
            page.records.len(),
            1,
            "the backlog holds the created record"
        );
        assert_eq!(page.records[0].event(), SignalEventType::Created);
        assert!(!page.more, "one record fits one page");
        assert!(page.cursor.is_some(), "the page carries a resume cursor");
    }

    /// A client subscribed to ZERO documents still receives a space-wide
    /// signal.
    ///
    /// The recipient set must not be derived from the union of the space's
    /// document subscribers: that would force a connection that wants signals
    /// to subscribe to a document it does not care about, via a sentinel
    /// document id. `SubscribeSignals` says what it means, and no sentinel
    /// exists.
    #[tokio::test]
    async fn test_signal_subscriber_with_no_documents_receives_a_broadcast() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = Uuid::new_v4().to_string();

        let (_author_data, mut author_ack, _author_ctrl) = connect_client(&mut relay, 1).await;
        // A second connection that subscribes to the SIGNAL stream and to no
        // document whatsoever.
        let (mut watcher_data, _ack, mut watcher_ctrl) = connect_client(&mut relay, 2).await;
        // A distinct identity: the author is skipped from its own broadcast, and
        // `connect_client` gives every test connection the same DID.
        relay.test_set_authenticated(2, "did:key:zReader");
        relay
            .handle_subscribe_signals(
                2,
                &sync::SubscribeSignals {
                    space_id: space_id.clone(),
                    cursor: None,
                },
            )
            .await;
        while watcher_ctrl.try_recv().is_ok() {}
        while author_ack.try_recv().is_ok() {}

        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-1".into(),
                    space_id: space_id.clone(),
                    document_id: None,
                    kind: i32::from(sync::FlagKind::Info),
                    message: "everyone should see this".into(),
                    audience: Some(kutl_proto::vocab::audience_from_untyped(
                        i32::from(sync::AudienceType::Space),
                        None,
                    )),
                    signal_id: None,
                },
            )
            .await;
        assert!(take_ack(&mut author_ack).success);

        let mut saw_signal = false;
        while let Ok(bytes) = watcher_data.try_recv() {
            if let Ok(env) = kutl_proto::protocol::decode_envelope(&bytes)
                && matches!(env.payload, Some(sync::sync_envelope::Payload::Signal(_)))
            {
                saw_signal = true;
            }
        }
        assert!(
            saw_signal,
            "a signal subscriber with no document subscriptions must still receive \
             the space broadcast"
        );
    }

    /// A tombstone is not authorable. The frame's enum can spell it — event
    /// legality is a property of the frame SET, not of one field — so this is
    /// the single event check the wire shape cannot make structural, and it
    /// comes back as a rejection ack rather than an Error frame.
    #[tokio::test]
    async fn test_submit_transition_refuses_a_tombstone() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = Uuid::new_v4().to_string();
        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        let mut msg = sync::SubmitTransition {
            client_ref: "ref-tomb".into(),
            space_id,
            signal_id: Uuid::new_v4().to_string(),
            event: 0,
            close_reason: None,
            note: None,
        };
        msg.set_event(SignalEventType::Tombstoned);
        relay.handle_submit_transition(1, &msg).await;

        let got = take_ack(&mut ack);
        assert!(!got.success, "a tombstone must not be authorable");
        assert!(
            got.error
                .as_deref()
                .is_some_and(|e| e.contains("system-minted")),
            "the rejection should say why, got {:?}",
            got.error
        );
    }

    /// Closing and reopening over the WS door FIRES the observer.
    ///
    /// The emit lives INSIDE the admission seam, not at any door's call
    /// site. An emit left at one call site and forgotten at another produces a
    /// correct, durable, acked transition that generates neither a live feed
    /// event nor a bell entry — nothing fails; the notification simply never
    /// happens. That is why this test drives the WS door rather than the
    /// observer directly: the claim is "a door cannot forget", not "the emit
    /// function works".
    #[tokio::test]
    async fn test_submit_transition_fires_the_observer_for_close_and_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        let observer = std::sync::Arc::new(RecordingObserver::default());
        let (mut relay, _pool) = relay_with_projection_and_observer(&dir, observer.clone()).await;
        let space_id = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-create".into(),
                    space_id: space_id.clone(),
                    document_id: None,
                    signal_id: None,
                    kind: i32::from(sync::FlagKind::Info),
                    message: "needs a decision".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;
        let created = take_ack(&mut ack);
        assert!(created.success, "setup: the flag must land");
        let signal_id = created.signal_id;

        // Close it, naming a non-default reason so the event's payload is
        // distinguishable from the default rather than accidentally right.
        let mut close = sync::SubmitTransition {
            client_ref: "ref-close".into(),
            space_id: space_id.clone(),
            signal_id: signal_id.clone(),
            event: 0,
            close_reason: Some(i32::from(sync::CloseReason::Declined)),
            note: Some("not this quarter".into()),
        };
        close.set_event(SignalEventType::Closed);
        relay.handle_submit_transition(1, &close).await;
        assert!(take_ack(&mut ack).success, "the close must be accepted");

        {
            let closed = observer.with_event(SignalEventType::Closed);
            assert_eq!(
                closed.len(),
                1,
                "a WS close must announce exactly one CLOSED record, got {}",
                closed.len()
            );
            let record = &closed[0];
            assert_eq!(record.id, signal_id);
            assert_eq!(record.space_id, space_id);
            assert_eq!(
                kutl_signals::payloads::close_reason_to_wire(record.close_reason()),
                "declined",
                "the caller's reason must reach the consumer, not the default"
            );
            let note = match &record.payload {
                Some(sync::signal::Payload::Transition(t)) => Some(t.note.as_str()),
                _ => None,
            };
            assert_eq!(note, Some("not this quarter"));
        }

        let mut reopen = sync::SubmitTransition {
            client_ref: "ref-reopen".into(),
            space_id: space_id.clone(),
            signal_id: signal_id.clone(),
            event: 0,
            close_reason: None,
            note: None,
        };
        reopen.set_event(SignalEventType::Reopened);
        relay.handle_submit_transition(1, &reopen).await;
        let reopen_ack = take_ack(&mut ack);
        assert!(reopen_ack.success, "the reopen must be accepted");

        let reopened = observer.with_event(SignalEventType::Reopened);
        assert_eq!(
            reopened.len(),
            1,
            "a WS reopen must announce exactly one REOPENED record, got {}",
            reopened.len()
        );
        assert_eq!(reopened[0].id, signal_id);
        // The record the relay wrote is the one announced, so a consumer
        // addressing that lifecycle row does not have to mint an id.
        assert_eq!(
            reopened[0].record_id, reopen_ack.record_id,
            "the announced record_id must be the one the ack reported"
        );
    }

    /// Both create doors FIRE the observer, and what they announce is the
    /// admitted record itself.
    ///
    /// The create half of the announcement contract, beside the transition
    /// half above. Pinned through the doors rather than the emit function for
    /// the same reason: the claim is that a door cannot return success while
    /// telling nobody.
    #[tokio::test]
    async fn test_create_doors_fire_the_observer_with_the_created_record() {
        let dir = tempfile::TempDir::new().unwrap();
        let observer = std::sync::Arc::new(RecordingObserver::default());
        let (mut relay, _pool) = relay_with_projection_and_observer(&dir, observer.clone()).await;
        let space_id = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-flag".into(),
                    space_id: space_id.clone(),
                    document_id: None,
                    signal_id: None,
                    kind: i32::from(sync::FlagKind::Question),
                    message: "which database?".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;
        let flag_ack = take_ack(&mut ack);
        assert!(flag_ack.success, "setup: the flag must land");

        relay
            .handle_submit_reply(
                1,
                &sync::SubmitReply {
                    client_ref: "ref-reply".into(),
                    space_id: space_id.clone(),
                    parent_signal_id: flag_ack.signal_id.clone(),
                    parent_reply_id: None,
                    body: "postgres".into(),
                },
            )
            .await;
        let reply_ack = take_ack(&mut ack);
        assert!(reply_ack.success, "setup: the reply must land");

        let created = observer.with_event(SignalEventType::Created);
        assert_eq!(
            created.len(),
            2,
            "each create door announces exactly its one record, got {}",
            created.len()
        );
        assert_eq!(
            observer.announcement_count(),
            2,
            "and nothing but the two CREATEDs — a leaked announcement of any \
             other event would hide behind a per-event count"
        );
        assert_eq!(created[0].id, flag_ack.signal_id);
        assert_eq!(created[1].id, reply_ack.signal_id);
        // The RECORD is what travels: a consumer reads the parent linkage off
        // it rather than off a hand-flattened field set.
        match &created[1].payload {
            Some(sync::signal::Payload::Reply(r)) => {
                assert_eq!(
                    r.parent_signal_id, flag_ack.signal_id,
                    "the announced reply must carry its parent"
                );
            }
            other => panic!("the reply's announcement must carry a reply payload, got {other:?}"),
        }
    }

    /// The document cascades announce NOTHING, in either direction: neither
    /// the delete's tombstones nor the revive's reopens.
    ///
    /// Their consumers watch the document event; a per-signal announcement
    /// storm is the reason batch silence exists. The revive direction is the
    /// higher-risk half of the pin: REOPENED is exactly the event the
    /// single-transition path DOES announce, so a slip there turns one
    /// document revive into N feed rows. Each silence claim is paired with
    /// proof the cascade did its work, so silence cannot mean "did nothing".
    #[tokio::test]
    async fn test_document_cascades_announce_nothing_in_either_direction() {
        let dir = tempfile::TempDir::new().unwrap();
        let observer = std::sync::Arc::new(RecordingObserver::default());
        let (mut relay, _pool) = relay_with_projection_and_observer(&dir, observer.clone()).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();
        let doc = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        let mut expected_ids = Vec::new();
        for client_ref in ["ref-a", "ref-b"] {
            relay
                .handle_submit_flag(
                    1,
                    &sync::SubmitFlag {
                        client_ref: client_ref.into(),
                        space_id: space_id.clone(),
                        document_id: Some(doc.clone()),
                        signal_id: None,
                        kind: i32::from(sync::FlagKind::Info),
                        message: "attached to the doomed doc".into(),
                        audience: Some(kutl_proto::vocab::space_audience()),
                    },
                )
                .await;
            let got = take_ack(&mut ack);
            assert!(got.success, "setup: the flag must land");
            expected_ids.push(got.signal_id);
        }
        expected_ids.sort();
        assert_eq!(
            observer.announcement_count(),
            2,
            "setup: both creates announced, so the observer is live"
        );

        relay
            .cascade_document_delete_signals(&space_id, &doc, "did:key:zDeleter")
            .await
            .expect("the delete cascade must succeed");

        // The cascade tombstoned EXACTLY the two signals — bound by id, so a
        // cascade that double-tombstoned one and missed the other cannot pass.
        let records = relay.record_log.load_space(space).await.expect("load");
        let mut tombstoned_ids: Vec<String> = records
            .iter()
            .filter(|r| r.event() == SignalEventType::Tombstoned)
            .map(|r| r.id.clone())
            .collect();
        tombstoned_ids.sort();
        assert_eq!(tombstoned_ids, expected_ids, "both signals tombstoned");
        assert_eq!(
            observer.announcement_count(),
            2,
            "the delete cascade must announce nothing beyond the two creates"
        );

        relay
            .cascade_document_revive_signals(&space_id, &doc, "did:key:zReviver")
            .await
            .expect("the revive cascade must succeed");

        let records = relay.record_log.load_space(space).await.expect("load");
        let mut reopened_ids: Vec<String> = records
            .iter()
            .filter(|r| r.event() == SignalEventType::Reopened)
            .map(|r| r.id.clone())
            .collect();
        reopened_ids.sort();
        assert_eq!(reopened_ids, expected_ids, "both signals reopened");
        assert_eq!(
            observer.announcement_count(),
            2,
            "the revive cascade must announce nothing — REOPENED announces on \
             the single-transition path, and this is the pin that keeps the \
             cascade from inheriting that"
        );
    }

    /// The space-delete cascade announces NONE of its tombstones either — the
    /// space event covers the delete.
    #[tokio::test]
    async fn test_space_delete_cascade_announces_nothing() {
        let dir = tempfile::TempDir::new().unwrap();
        let observer = std::sync::Arc::new(RecordingObserver::default());
        let (mut relay, _pool) = relay_with_projection_and_observer(&dir, observer.clone()).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        author_one_flag_and_prove_the_observer_is_live(&mut relay, &mut ack, &observer, &space_id)
            .await;

        relay
            .cascade_space_delete_signals_logged(&space_id, "did:key:zDeleter")
            .await;

        // The cascade did its work — the log-and-continue wrapper swallows
        // errors, so the record assertion is what keeps a failed cascade from
        // reading as a silent success here.
        let records = relay.record_log.load_space(space).await.expect("load");
        let tombstoned = records
            .iter()
            .filter(|r| r.event() == SignalEventType::Tombstoned)
            .count();
        assert_eq!(tombstoned, 1, "the flag must have been tombstoned");
        assert_eq!(
            observer.announcement_count(),
            1,
            "the space-delete cascade must announce nothing beyond the create"
        );
    }

    /// A re-seed announces NOTHING: replicated records are a peer's history,
    /// not news, and replaying them into the live feed would re-notify a
    /// space about things that happened arbitrarily long ago.
    #[tokio::test]
    async fn test_reseed_announces_nothing() {
        let dir = tempfile::TempDir::new().unwrap();
        let observer = std::sync::Arc::new(RecordingObserver::default());
        let (mut relay, _pool) = relay_with_projection_and_observer(&dir, observer.clone()).await;
        let space_id = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        author_one_flag_and_prove_the_observer_is_live(&mut relay, &mut ack, &observer, &space_id)
            .await;

        let minted = relay
            .record_log
            .load_space(Uuid::parse_str(&space_id).unwrap())
            .await
            .expect("load");
        replay_one_record_into_a_peer_space(&mut relay, &mut ack, &minted[0]).await;

        assert_eq!(
            observer.announcement_count(),
            1,
            "an appended history record must not announce"
        );
    }

    /// Materialized ingest announces NOTHING through this observer:
    /// marker-derived records are announced from the merge path by a
    /// deployment's `AfterMergeObserver`, which holds merge context this seam
    /// lacks, so an announcement here would be that event's duplicate.
    #[tokio::test]
    async fn test_materialized_ingest_announces_nothing() {
        let dir = tempfile::TempDir::new().unwrap();
        let observer = std::sync::Arc::new(RecordingObserver::default());
        let (mut relay, _pool) = relay_with_projection_and_observer(&dir, observer.clone()).await;
        let space = Uuid::new_v4();
        let space_id = space.to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        author_one_flag_and_prove_the_observer_is_live(&mut relay, &mut ack, &observer, &space_id)
            .await;

        // A well-formed record wearing a fresh identity, arriving the way the
        // marker materializer's batches do.
        let minted = relay.record_log.load_space(space).await.expect("load");
        let mut materialized = minted[0].clone();
        materialized.id = Uuid::new_v4().to_string();
        materialized.record_id = "rMaterialized".into();

        relay
            .handle_materialized_records(&space_id, vec![materialized.clone()])
            .await;

        let stored = relay.record_log.load_space(space).await.expect("load");
        assert!(
            stored.iter().any(|r| r.id == materialized.id),
            "the materialized record must be appended"
        );
        assert_eq!(
            observer.announcement_count(),
            1,
            "a materialized record must not announce through this observer"
        );
    }

    /// GUARD (agent attribution): every WS authoring door records the PAT its
    /// connection authenticated with. The relay stores `(did, pat)` per
    /// connection, and a person and their agent share a DID — so a door that
    /// reads only the DID half credits the agent's work to the person, on
    /// every surface downstream of the announcement (the event's
    /// `via_pat_id` is what a feed byline names the agent by).
    #[tokio::test]
    async fn test_every_ws_door_threads_the_connection_pat() {
        let dir = tempfile::TempDir::new().unwrap();
        let observer = std::sync::Arc::new(RecordingObserver::default());
        let (mut relay, _pool) = relay_with_projection_and_observer(&dir, observer.clone()).await;
        let space_id = Uuid::new_v4().to_string();
        let document_id = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}
        // The same DID, now PAT-authenticated: the agent acting for its
        // person.
        relay.authenticated.insert(
            1,
            (
                super::super::TEST_CONN_DID.to_owned(),
                Some(crate::auth::PatAuthContext {
                    pat_hash: "hash".to_owned(),
                    pat_id: "pat-123".to_owned(),
                }),
            ),
        );

        // Door 1 — flag.
        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-flag".into(),
                    space_id: space_id.clone(),
                    document_id: Some(document_id.clone()),
                    kind: i32::from(sync::FlagKind::Question),
                    message: "agent asks".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                    signal_id: None,
                },
            )
            .await;
        let flag_ack = take_ack(&mut ack);
        assert!(flag_ack.success, "flag door accepts: {:?}", flag_ack.error);
        let flag_id = flag_ack.signal_id;

        // Door 2 — comment.
        relay
            .handle_submit_comment(
                1,
                &sync::SubmitComment {
                    client_ref: "ref-comment".into(),
                    space_id: space_id.clone(),
                    document_id: document_id.clone(),
                    signal_id: Uuid::new_v4().to_string(),
                    message: "agent comments".into(),
                    anchor_text: Some("anchored text".into()),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;
        assert!(take_ack(&mut ack).success, "comment door accepts");

        // Door 3 — reply to the flag.
        relay
            .handle_submit_reply(
                1,
                &sync::SubmitReply {
                    client_ref: "ref-reply".into(),
                    space_id: space_id.clone(),
                    parent_signal_id: flag_id.clone(),
                    parent_reply_id: None,
                    body: "agent answers".into(),
                },
            )
            .await;
        assert!(take_ack(&mut ack).success, "reply door accepts");

        // Door 4 — transition (close the flag).
        let mut close = sync::SubmitTransition {
            client_ref: "ref-close".into(),
            space_id: space_id.clone(),
            signal_id: flag_id,
            close_reason: None,
            note: None,
            event: 0,
        };
        close.set_event(sync::SignalEventType::Closed);
        relay.handle_submit_transition(1, &close).await;
        assert!(take_ack(&mut ack).success, "transition door accepts");

        // Door 5 — the legacy `Signal` frame (flag payloads only).
        let legacy = sync::Signal {
            space_id: space_id.clone(),
            document_id: Some(document_id),
            timestamp: 1_700_000_000_000,
            payload: Some(sync::signal::Payload::Flag(sync::FlagPayload {
                kind: i32::from(sync::FlagKind::Info),
                message: "agent flags, legacy frame".into(),
                audience: Some(kutl_proto::vocab::space_audience()),
                ..Default::default()
            })),
            ..Default::default()
        };
        relay.handle_signal(1, &legacy).await;

        let via_pats = observer.announced_via_pats();
        assert_eq!(via_pats.len(), 5, "five doors, five announcements");
        for (i, via_pat) in via_pats.iter().enumerate() {
            assert_eq!(
                via_pat.as_deref(),
                Some("pat-123"),
                "announcement {i} must carry the connection's PAT — a None \
                 here renders the agent's work as its person's"
            );
        }
    }

    /// One input table: a single logical flag, authored through every
    /// relay-side door.
    const PARITY_KIND: sync::FlagKind = sync::FlagKind::Question;
    const PARITY_MESSAGE: &str = "which database?";

    /// Drive [`PARITY_KIND`]/[`PARITY_MESSAGE`] through WS and MCP.
    ///
    /// Each door is given the SAME logical input in whatever shape it accepts —
    /// a typed `Audience` on the frame, an untyped `AudienceType` int on the
    /// other — which is the point: the doors take different arguments and
    /// must still produce one record shape.
    async fn drive_every_door(relay: &mut super::super::Relay, space_id: &str, document_id: &str) {
        let space_audience = i32::from(sync::AudienceType::Space);

        let (_data, mut ack, _ctrl) = connect_client(relay, 1).await;
        while ack.try_recv().is_ok() {}

        // Door 1 — WS submit frame. Also the browser, which authors through
        // these frames.
        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-ws".into(),
                    space_id: space_id.to_owned(),
                    document_id: Some(document_id.to_owned()),
                    signal_id: None,
                    kind: i32::from(PARITY_KIND),
                    message: PARITY_MESSAGE.into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;
        assert!(take_ack(&mut ack).success, "the WS door must accept");

        // Door 2 — MCP tool call.
        super::super::mcp::tests::register_mcp_session(
            relay,
            "session-5c",
            super::super::TEST_CONN_DID,
        );
        relay
            .handle_mcp_create_flag(
                "session-5c",
                space_id,
                document_id,
                i32::from(PARITY_KIND),
                PARITY_MESSAGE,
                space_audience,
                "",
                None,
                None,
            )
            .await
            .expect("the MCP door must accept");
    }

    /// Every authoring door produces the same record.
    ///
    /// The "one write path" claim: a flag authored over WS or through MCP is
    /// byte-identical except in the fields that are non-deterministic by
    /// construction. If the doors drifted, "one write path" would be a
    /// statement about plumbing rather than about data, and a consumer
    /// reading the log would have to know which door wrote each row.
    ///
    /// **Two doors, not three.** The browser is not a third code path — it
    /// authors through the WS submit frames, so driving `handle_submit_flag`
    /// here IS driving the browser.
    ///
    /// **Exempt, each for a stated reason:**
    /// - `record_id`, `hlc` — minted per record; equality would mean the doors
    ///   were producing the same record twice, not the same shape.
    /// - `Signal.id` — minted per door when the caller supplies none. Asserted
    ///   as *occupancy* plus distinctness instead.
    /// - `timestamp` — `now_ms()` on MCP, client-supplied on WS. Occupancy
    ///   only.
    /// - `attestation`, `signature` — a wall clock rides the attestation, so
    ///   these are asserted PRESENT rather than byte-equal.
    ///
    /// Everything else is compared by blanking the exempt fields and testing
    /// the records for equality, so a field added to `Signal` later is covered
    /// automatically — a per-field assertion list would silently stop covering
    /// the new field, which is the failure mode this shape avoids.
    #[tokio::test]
    async fn test_all_authoring_doors_produce_identical_records() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        // A standalone test relay has no signing identity, and `attest_on_ingest`
        // stamps only when one is present — so without this the attestation
        // assertion below would be testing the fixture, not the doors.
        relay.signing_identity = Some(std::sync::Arc::new(
            crate::identity::RelayIdentity::load_or_generate(dir.path())
                .expect("generate a relay identity"),
        ));
        let space_id = Uuid::new_v4().to_string();
        let document_id = Uuid::new_v4().to_string();

        drive_every_door(&mut relay, &space_id, &document_id).await;

        let records = relay
            .record_log
            .load_space(Uuid::parse_str(&space_id).unwrap())
            .await
            .expect("load");
        assert_eq!(
            records.len(),
            2,
            "each door must append exactly one record, got {}",
            records.len()
        );

        // The exempt fields, asserted as occupancy before they are blanked —
        // otherwise blanking would hide a door that emitted nothing at all.
        for (i, record) in records.iter().enumerate() {
            assert!(!record.id.is_empty(), "door {i}: Signal.id must be minted");
            assert!(!record.record_id.is_empty(), "door {i}: record_id");
            assert!(record.hlc.is_some(), "door {i}: hlc must be stamped");
            assert!(record.timestamp > 0, "door {i}: timestamp");
            assert!(
                record.attestation.is_some(),
                "door {i}: the relay must attest its own record"
            );
        }

        // Minted per door, so distinct. Equality here would mean two doors
        // addressed the same signal.
        let ids: std::collections::HashSet<&str> = records.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids.len(), 2, "each door mints its own signal id");

        let blanked: Vec<kutl_proto::sync::Signal> = records
            .iter()
            .map(|record| {
                let mut copy = record.clone();
                copy.id = String::new();
                copy.record_id = String::new();
                copy.hlc = None;
                copy.timestamp = 0;
                copy.attestation = None;
                copy.signature = Vec::new();
                copy
            })
            .collect();

        for (i, other) in blanked.iter().enumerate().skip(1) {
            assert_eq!(
                &blanked[0], other,
                "door {i} produced a different record shape than the WS door \
                 — the doors have drifted"
            );
        }
    }

    /// The re-seed FRAME works, and a refusing relay says so on the ack lane.
    ///
    /// The frame is the daemon's push direction — with no dispatch arm for it,
    /// an inbound `SignalReseed` would fall through to `unsupported message
    /// type` and pushed history would have no landing spot.
    ///
    /// The refusal half is the load-bearing one. `accepts_reseed = false` is a
    /// deployment saying "do not accept client-pushed history here", and a
    /// pusher that read `success: true` would advance its cursor past records
    /// this relay never stored.
    #[tokio::test]
    async fn test_reseed_frame_appends_and_a_refusing_relay_acks_the_refusal() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mut relay, _pool) = relay_with_projection(&dir).await;
        let space_id = Uuid::new_v4().to_string();

        let (_data, mut ack, _ctrl) = connect_client(&mut relay, 1).await;
        while ack.try_recv().is_ok() {}

        // Mint a real record by authoring one, then replay it as a peer's
        // history into a DIFFERENT space — a hand-built record would not carry
        // the envelope fields `validate_record` requires.
        relay
            .handle_submit_flag(
                1,
                &sync::SubmitFlag {
                    client_ref: "ref-seed".into(),
                    space_id: space_id.clone(),
                    document_id: None,
                    signal_id: None,
                    kind: i32::from(sync::FlagKind::Info),
                    message: "history".into(),
                    audience: Some(kutl_proto::vocab::space_audience()),
                },
            )
            .await;
        assert!(take_ack(&mut ack).success, "setup: the flag must land");
        let minted = relay
            .record_log
            .load_space(Uuid::parse_str(&space_id).unwrap())
            .await
            .expect("load");
        assert_eq!(minted.len(), 1);

        replay_one_record_into_a_peer_space(&mut relay, &mut ack, &minted[0]).await;

        // A relay that refuses re-seed must REFUSE, not silently drop.
        relay.config.accepts_reseed = false;
        let refused_space = Uuid::new_v4().to_string();
        let mut for_refusal = minted[0].clone();
        for_refusal.space_id.clone_from(&refused_space);
        relay
            .handle_submit_reseed(
                1,
                sync::SignalReseed {
                    records: vec![for_refusal],
                    space_id: refused_space.clone(),
                    client_ref: "ref-refused".into(),
                },
            )
            .await;
        let refusal = take_ack(&mut ack);
        assert!(
            !refusal.success,
            "a relay with accepts_reseed=false must refuse on the ack lane"
        );
        let none = relay
            .record_log
            .load_space(Uuid::parse_str(&refused_space).unwrap())
            .await
            .expect("load");
        assert!(
            none.is_empty(),
            "a refused re-seed must append nothing, found {}",
            none.len()
        );
    }
}
