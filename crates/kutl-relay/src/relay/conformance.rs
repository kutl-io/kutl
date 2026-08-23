//! The contract every [`SignalProjection`] must satisfy, as an executable test.
//!
//! **Why this exists, and why it drives the RELAY rather than the backend.**
//! Three defects reached the commercial substrate through the gap between "the
//! backend does what you ask" and "the relay asks correctly" — marker-derived
//! signals that never reached Postgres at all, born-resolved decisions that
//! projected Open, and reviving a document that never restored its signals. The
//! commercial backend already had ~20 passing tests, and none of them could have
//! caught any of it: they construct `PgChangeBackend` directly and call methods
//! on it. That proves the backend obeys. It cannot prove the relay asks, and
//! every one of those bugs lived in the asking.
//!
//! So this drives records through [`Relay::append_authored_record`] — the real
//! admission seam, the same one every door funnels into — and then reads them
//! back through the projection's own query path. It lives inside `crate::relay`
//! because those seams are `pub(in crate::relay)`; a suite outside this crate
//! physically cannot reach them, which is precisely how the existing commercial
//! tests ended up one layer too low.
//!
//! **Why it is generic.** The two substrates diverged silently for months: the
//! OSS sqlite projection has a fold-backed rebuild that repaired each defect
//! milliseconds later, and Postgres has none, so it simply carried the damage.
//! Every Rust suite in the repo runs sqlite. A conformance suite that only ran
//! there would re-create the blind spot in a new location. Instantiate it once
//! per substrate — sqlite in OSS, a Postgres testcontainer in commercial — and
//! the two are provably answering the same questions.
//!
//! Feature-gated (`projection-conformance`) so the scaffolding is not compiled
//! into a production relay. It is deliberately part of the PUBLIC surface: a
//! self-hoster who writes their own [`SignalProjection`] gets a suite to
//! validate it against, which is the same audience [`ProjectionWriter`] having
//! no default body is protecting.

use std::sync::Arc;

use kutl_proto::sync::{self, CloseReason, SignalEventType};
use uuid::Uuid;

use crate::change_backend::SignalProjection;
use crate::record_log::SegmentRecordLog;
use crate::relay::Relay;
// Silent on every injection below: these records are contract fixtures, not
// news, and the suite's subject is the projection rather than the feed.
use crate::relay::signal_log::Announce;
use crate::signal_store::SignalStore;

/// A fixed wall-clock base, so a failure reports a stable timestamp rather than
/// whatever the machine's clock said.
const T0: i64 = 1_700_000_000_000;

/// Assert every invariant a [`SignalProjection`] must uphold, driving records
/// through the relay's admission seam and reading them back through the
/// projection's own query path.
///
/// `records_dir` is a scratch directory for the segment log — the caller owns it
/// so the temp-dir crate stays out of this crate's production dependencies.
///
/// `space_id` and `document_id` are supplied rather than generated because the
/// two substrates disagree about what a valid id is: sqlite accepts any string,
/// while Postgres has real foreign keys and needs rows the caller has already
/// seeded. Taking them as parameters is what lets ONE set of assertions run
/// against both.
///
/// # Panics
///
/// On any contract violation, with a message naming which invariant failed.
pub async fn assert_projection_contract(
    projection: Arc<dyn SignalProjection>,
    records_dir: &std::path::Path,
    space_id: &str,
    document_id: &str,
) {
    let mut relay = Relay::new_standalone(crate::relay::RelayConfig::default());
    relay.record_log = crate::relay::signal_log::SignalLogHandle::new(
        Some(Arc::new(SegmentRecordLog::new(SignalStore::new(
            records_dir.to_path_buf(),
        )))),
        Some(Arc::clone(&projection)),
    );

    let (space, doc) = (space_id, document_id);

    assert_created_is_readable(&mut relay, &projection, space, doc).await;
    assert_born_closed_projects_closed(&mut relay, &projection, space, doc).await;
    assert_close_then_reopen_round_trips(&mut relay, &projection, space, doc).await;
    assert_tombstone_hides_but_keeps_identity(&mut relay, &projection, space, doc).await;
    assert_close_state_is_kind_independent(&mut relay, &projection, space, doc).await;
    assert_edit_overlays_content_not_lifecycle(&mut relay, &projection, space, doc).await;
    assert_close_resurfaces_in_the_change_feed(&mut relay, &projection, space, doc).await;
    assert_transition_trail_carries_the_close(&mut relay, space, doc).await;
    assert_rebuild_reconverges_the_projection(&mut relay, &projection, space, doc).await;
}

/// Every substrate exposes a rebuild, and rebuilding from the record log
/// reconverges on the same answer the incremental writes produced.
///
/// A projection is a derivative of the records; a substrate without a
/// rebuild turns any divergence — a crash between the durable record append
/// and the projection write, an orphan skip resolved later, a projection bug
/// fixed after the fact — into permanent damage repairable only by hand.
/// This case is also the rebuild's one permanent caller on deployments whose
/// product stance never invokes it in production: exercised on every run,
/// pinned against the other substrate, never dead code.
async fn assert_rebuild_reconverges_the_projection(
    relay: &mut Relay,
    projection: &Arc<dyn SignalProjection>,
    space: &str,
    doc: &str,
) {
    let id = Uuid::new_v4().to_string();
    relay
        .append_authored_record(
            &flag_created(&id, space, doc, "conformance: rebuild reconverges"),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("create");
    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Closed, T0 + 1_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("close");

    let before = projection
        .get_signal_detail(space, &id)
        .await
        .expect("detail before rebuild");

    let space_uuid = Uuid::parse_str(space).expect("conformance spaces are uuids");
    let records = relay
        .record_log
        .load_space(space_uuid)
        .await
        .expect("load the record log");
    let rebuild = projection
        .rebuild()
        .expect("every substrate exposes a rebuild — a projection is a derivative of the records");
    rebuild
        .rebuild_space(space, &records)
        .await
        .expect("rebuild");
    // Twice: a rebuild over an already-correct projection must be a no-op,
    // not an amplifier.
    rebuild
        .rebuild_space(space, &records)
        .await
        .expect("rebuild is idempotent");

    let after = projection
        .get_signal_detail(space, &id)
        .await
        .expect("detail after rebuild");
    assert_eq!(
        before.closed_at, after.closed_at,
        "rebuild must reconverge on the incremental answer"
    );
    assert_eq!(before.message, after.message);
    assert!(after.closed_at.is_some(), "and the close survives");
}

/// The transition trail is derived from the RECORD LOG through one shared
/// fold, identically on both substrates — a close's reason and note reach
/// every deployment's detail read, not just the one whose backend happens to
/// store them. This also exercises the per-signal log read each substrate
/// answers its own way: a filtered scan of segment files, or an indexed
/// fetch on a database.
async fn assert_transition_trail_carries_the_close(relay: &mut Relay, space: &str, doc: &str) {
    let id = Uuid::new_v4().to_string();
    relay
        .append_authored_record(
            &flag_created(&id, space, doc, "conformance: trail carries the close"),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("create");

    let mut close = transition(&id, space, SignalEventType::Closed, T0 + 1_000);
    close.payload = Some(sync::signal::Payload::Transition(sync::TransitionPayload {
        note: "conformance: not this quarter".to_owned(),
    }));
    relay
        .append_authored_record(&close, &id, space, None, Announce::Silent)
        .await
        .expect("close");

    let trail = relay.signal_transition_history(space, &id).await;
    let closed = trail
        .iter()
        .find(|t| t.event == "closed")
        .expect("the trail must carry the close");
    assert_eq!(
        closed.close_reason.as_deref(),
        Some("resolved"),
        "and its reason"
    );
    assert_eq!(
        closed.note.as_deref(),
        Some("conformance: not this quarter"),
        "and its note"
    );
}

/// A lifecycle change RE-SURFACES its signal in the change feed.
///
/// The feed pages in last-activity order: a reader whose cursor has passed a
/// signal's birth must receive the row again when it closes, and the
/// re-served row must say so — `event = CLOSED` plus the reason — or the
/// resurfacing is indistinguishable from a birth. The counter-half is
/// equally binding: a CLOSED replay carrying identical values changes
/// nothing and must re-deliver nothing, or every redelivered transition
/// would re-serve every closed signal to every reader.
async fn assert_close_resurfaces_in_the_change_feed(
    relay: &mut Relay,
    projection: &Arc<dyn SignalProjection>,
    space: &str,
    doc: &str,
) {
    let id = Uuid::new_v4().to_string();
    relay
        .append_authored_record(
            &flag_created(&id, space, doc, "conformance: resurface on close"),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("create");

    let reader = "did:key:zConformanceFeedReader";
    let drained = projection
        .get_changes(reader, space, None)
        .await
        .expect("drain the birth");
    assert!(
        drained.signals.iter().any(|s| s.id == id),
        "the birth must reach the feed before a close can re-surface it"
    );

    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Closed, T0 + 1_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("close");
    let resurfaced = projection
        .get_changes(reader, space, None)
        .await
        .expect("read after close");
    let row = resurfaced
        .signals
        .iter()
        .find(|s| s.id == id)
        .expect("a close must re-surface the signal in the change feed");
    assert_eq!(
        row.event(),
        SignalEventType::Closed,
        "the re-served row must carry its lifecycle state"
    );
    assert_eq!(row.close_reason(), CloseReason::Resolved, "and the reason");

    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Closed, T0 + 1_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("replayed close");
    let after_replay = projection
        .get_changes(reader, space, None)
        .await
        .expect("read after replay");
    assert!(
        !after_replay.signals.iter().any(|s| s.id == id),
        "an identical-value CLOSED replay must not re-surface the row"
    );
}

/// **Ack implies readable.** A record the seam
/// accepted must be returned by the read path with no retry.
///
/// This is the invariant defect 1 violated: `handle_materialized_records`
/// projected through a method the commercial backend had never implemented, so
/// the append succeeded, the broadcast fired, and no row was ever written.
async fn assert_created_is_readable(
    relay: &mut Relay,
    projection: &Arc<dyn SignalProjection>,
    space: &str,
    doc: &str,
) {
    let id = relay
        .relay_flag_signal(
            None,
            space,
            Some(doc),
            "did:key:zConformanceAuthor",
            i32::from(sync::FlagKind::ReviewRequested),
            kutl_proto::vocab::space_audience(),
            "conformance: created is readable",
            None,
            T0,
            None,
        )
        .await
        .expect("the seam must accept a well-formed flag");

    let detail = projection
        .get_signal_detail(space, &id)
        .await
        .expect("a signal the seam accepted must be readable — ack implies readable");
    assert_eq!(detail.id, id, "the read path returned a different signal");
    assert!(
        detail.closed_at.is_none(),
        "an ordinary CREATED must project OPEN, got closed_at = {:?}",
        detail.closed_at
    );
}

/// A record can be BORN CLOSED, and the projection must say so.
///
/// A `## = …` decision heading materializes as a CREATED carrying
/// `close_reason = RESOLVED`. Defect 2: the incremental
/// insert bound no close-state, so it projected Open — on OSS the fold-backed
/// rebuild corrected it moments later, and on Postgres nothing ever did.
async fn assert_born_closed_projects_closed(
    relay: &mut Relay,
    projection: &Arc<dyn SignalProjection>,
    space: &str,
    doc: &str,
) {
    let id = Uuid::new_v4().to_string();
    let mut record = flag_created(&id, space, doc, "conformance: born closed");
    record.set_close_reason(CloseReason::Resolved);
    relay
        .append_authored_record(&record, &id, space, None, Announce::Silent)
        .await
        .expect("the seam must accept a born-closed CREATED");

    let detail = projection
        .get_signal_detail(space, &id)
        .await
        .expect("a born-closed signal is still readable");
    assert!(
        detail.closed_at.is_some(),
        "a CREATED carrying close_reason=RESOLVED must project CLOSED, not Open — \
         this is the invariant a projection that binds only the envelope breaks"
    );
}

/// CLOSED sets the close-state; REOPENED clears it. The ordinary lifecycle.
async fn assert_close_then_reopen_round_trips(
    relay: &mut Relay,
    projection: &Arc<dyn SignalProjection>,
    space: &str,
    doc: &str,
) {
    let id = Uuid::new_v4().to_string();
    let created = flag_created(&id, space, doc, "conformance: close then reopen");
    relay
        .append_authored_record(&created, &id, space, None, Announce::Silent)
        .await
        .expect("create");

    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Closed, T0 + 1_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("close");
    let closed = projection
        .get_signal_detail(space, &id)
        .await
        .expect("read");
    assert!(
        closed.closed_at.is_some(),
        "a CLOSED transition must set the projection's close-state"
    );

    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Reopened, T0 + 2_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("reopen");
    let reopened = projection
        .get_signal_detail(space, &id)
        .await
        .expect("read");
    assert!(
        reopened.closed_at.is_none(),
        "a REOPENED transition must clear the close-state, got {:?}",
        reopened.closed_at
    );
}

/// A TOMBSTONE hides the signal but must NOT release its id, and a later
/// REOPENED must bring it back.
///
/// Two defects met here. Postgres never cleared `deleted_at`, so reviving a
/// deleted document reopened its signals and left every one invisible (defect
/// 3). And a projection that DELETES rather than hides releases the id — after
/// which the duplicate-create guard, which reads through `signal_exists`, hands
/// it back out and a second CREATED can be authored under it.
async fn assert_tombstone_hides_but_keeps_identity(
    relay: &mut Relay,
    projection: &Arc<dyn SignalProjection>,
    space: &str,
    doc: &str,
) {
    let id = Uuid::new_v4().to_string();
    let created = flag_created(&id, space, doc, "conformance: tombstone keeps identity");
    relay
        .append_authored_record(&created, &id, space, None, Announce::Silent)
        .await
        .expect("create");

    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Tombstoned, T0 + 1_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("tombstone");

    assert!(
        projection.get_signal_detail(space, &id).await.is_err(),
        "a tombstoned signal must not be readable"
    );
    assert!(
        projection
            .signal_exists(space, &id)
            .await
            .expect("existence probe"),
        "a tombstoned signal must KEEP its id — releasing it lets a second \
         CREATED be authored under the same id, which the fold then collapses"
    );

    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Reopened, T0 + 2_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("revive");
    let revived = projection
        .get_signal_detail(space, &id)
        .await
        .expect("a reopened signal must be readable again — this is what a document revive does");
    assert!(
        revived.closed_at.is_none(),
        "a revived signal must be Open, got closed_at = {:?}",
        revived.closed_at
    );
}

/// A minimal well-formed CREATED flag record.
/// A DECISION reads back closed, exactly as a flag does.
///
/// Every other case in this suite builds a FLAG, and that blind spot hid a real
/// divergence: Postgres derived the detail's `closed_at` from the
/// `flag_details` LEFT JOIN, and only a flag has such a row — so a closed
/// decision or reply read back OPEN while the same response's transition trail
/// showed the close. sqlite read the envelope column and was right for every
/// kind. A contract suite that only ever exercises one kind cannot see a
/// per-kind divergence, which is the point of this case: the assertion is not
/// "decisions work", it is "close-state does not depend on the kind".
async fn assert_close_state_is_kind_independent(
    relay: &mut Relay,
    projection: &Arc<dyn SignalProjection>,
    space: &str,
    doc: &str,
) {
    let id = Uuid::new_v4().to_string();
    relay
        .append_authored_record(
            &decision_created(&id, space, doc),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("create decision");

    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Closed, T0 + 1_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("close decision");

    let closed = projection
        .get_signal_detail(space, &id)
        .await
        .expect("read");
    assert!(
        closed.closed_at.is_some(),
        "a closed DECISION must read back closed — close-state is carried by \
         the signal, not by a per-kind detail row"
    );

    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Reopened, T0 + 2_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("reopen decision");
    let reopened = projection
        .get_signal_detail(space, &id)
        .await
        .expect("read");
    assert!(
        reopened.closed_at.is_none(),
        "reopening a DECISION must clear its close-state too"
    );
}

/// An EDIT overlays content and touches nothing else.
///
/// Three invariants in one walk. The carried field replaces (`message`
/// reads back as the edit's). The uncarried field persists (`anchor_text`
/// survives — a comment-anchor edit cannot know the comment's message, so
/// the overlay rule is what stops an edit from nulling columns it did not
/// carry). And the lifecycle is untouched: the signal was CLOSED before the
/// edit and must still read CLOSED after — an edit never reopens.
///
/// The edit arrives through the MATERIALIZED seam, because that is the only
/// path that mints one: EDITED is not authorable, and the
/// authored transition door refuses it.
async fn assert_edit_overlays_content_not_lifecycle(
    relay: &mut Relay,
    projection: &Arc<dyn SignalProjection>,
    space: &str,
    doc: &str,
) {
    let id = Uuid::new_v4().to_string();
    let mut created = flag_created(&id, space, doc, "conformance: before the edit");
    if let Some(sync::signal::Payload::Flag(f)) = &mut created.payload {
        f.anchor_text = Some("conformance: the anchored excerpt".to_owned());
    }
    relay
        .append_authored_record(&created, &id, space, None, Announce::Silent)
        .await
        .expect("create");
    relay
        .append_authored_record(
            &transition(&id, space, SignalEventType::Closed, T0 + 1_000),
            &id,
            space,
            None,
            Announce::Silent,
        )
        .await
        .expect("close");

    let mut edit = flag_created(&id, space, doc, "conformance: after the edit");
    edit.set_event(SignalEventType::Edited);
    edit.timestamp = T0 + 2_000;
    if let Some(hlc) = &mut edit.hlc {
        hlc.physical_ms = (T0 + 2_000).cast_unsigned();
    }
    relay
        .append_materialized_record(&edit, &id, space)
        .await
        .expect("the materialized seam must accept an EDITED record");

    let detail = projection
        .get_signal_detail(space, &id)
        .await
        .expect("an edited signal is still readable");
    assert_eq!(
        detail.message.as_deref(),
        Some("conformance: after the edit"),
        "an EDITED record's carried message must replace the projected one"
    );
    assert_eq!(
        detail.anchor_text.as_deref(),
        Some("conformance: the anchored excerpt"),
        "a field the edit did not carry must PERSIST — an edit that nulls \
         uncarried columns breaks the overlay rule"
    );
    assert!(
        detail.closed_at.is_some(),
        "an edit must not reopen — content and lifecycle are separate tracks"
    );
}

/// A CREATED decision record, for the kind-independence case above.
fn decision_created(id: &str, space: &str, doc: &str) -> sync::Signal {
    let mut record = flag_created(id, space, doc, "conformance: decision");
    record.payload = Some(sync::signal::Payload::Decision(sync::DecisionPayload {
        title: "conformance: does close-state depend on kind?".to_owned(),
        ..Default::default()
    }));
    record
}

fn flag_created(id: &str, space: &str, doc: &str, message: &str) -> sync::Signal {
    let mut record = sync::Signal {
        id: id.to_owned(),
        space_id: space.to_owned(),
        document_id: Some(doc.to_owned()),
        author_did: "did:key:zConformanceAuthor".to_owned(),
        actor_did: "did:key:zConformanceAuthor".to_owned(),
        timestamp: T0,
        record_id: Uuid::new_v4().to_string(),
        hlc: Some(sync::Hlc {
            physical_ms: T0.cast_unsigned(),
            logical: 0,
            actor: vec![0u8; 16],
        }),
        payload: Some(sync::signal::Payload::Flag(sync::FlagPayload {
            kind: i32::from(sync::FlagKind::Info),
            message: message.to_owned(),
            audience: Some(kutl_proto::vocab::space_audience()),
            anchor_text: None,
            ..Default::default()
        })),
        ..Default::default()
    };
    record.set_event(SignalEventType::Created);
    record
}

/// A minimal well-formed transition record for `id`.
fn transition(id: &str, space: &str, event: SignalEventType, ms: i64) -> sync::Signal {
    let mut record = sync::Signal {
        id: id.to_owned(),
        space_id: space.to_owned(),
        author_did: "did:key:zConformanceAuthor".to_owned(),
        actor_did: "did:key:zConformanceAuthor".to_owned(),
        timestamp: ms,
        record_id: Uuid::new_v4().to_string(),
        hlc: Some(sync::Hlc {
            physical_ms: ms.cast_unsigned(),
            logical: 0,
            actor: vec![0u8; 16],
        }),
        ..Default::default()
    };
    record.set_event(event);
    if event == SignalEventType::Closed {
        record.set_close_reason(CloseReason::Resolved);
    }
    record
}
