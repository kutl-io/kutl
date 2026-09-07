//! The deterministic fold from event records to signal state.
//!
//! Merge across replicas is set union (records are immutable; dedup by
//! `record_id`); state is order-insensitive: transitions resolve by
//! last-writer-wins on the total order `(hlc, actor_did, record_id)`.
//! Content resolves the same way on its own track: the max-order
//! EDITED record's payload is the content now, independent of lifecycle, so
//! an edit never reopens and a transition never reverts content.
//! The fold is total — orphan transitions (tombstones included) park until
//! their CREATED arrives, and unknown event kinds are counted but skipped
//! (the record bytes persist in segments and still replicate).
//!
//! The fold never discards records; boundedness is the ingest layer's job
//! (callers cap what they admit, loudly).
//!
//! Precondition: `record_id` ↔ record-bytes is a bijection. A hostile peer
//! signing two byte-distinct records with one `record_id` breaks set-union
//! convergence; ingest layers must verify duplicate `record_id`s against
//! stored segment bytes before folding.

use std::collections::{BTreeMap, HashMap, HashSet};

use kutl_core::hlc::{ActorId, Hlc};
use kutl_proto::sync::{CloseReason, Signal, SignalEventType};
use uuid::Uuid;

/// Current lifecycle status of a signal, derived by the fold.
///
/// The derived `Ord` is arbitrary declaration order — it exists so test
/// snapshots sort deterministically and carries no lifecycle semantics.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SignalStatus {
    /// No transition, or the latest transition is REOPENED.
    Open,
    /// The latest transition is CLOSED.
    Closed,
    /// The latest transition is TOMBSTONED (soft delete; projections hide).
    Tombstoned,
}

/// The total order key for transition LWW: HLC extended by the defensive
/// final keys (`actor_did`, then `record_id`) so the order is total even
/// under HLC ties. Derived from a record by [`order_key`].
pub type OrderKey = (Hlc, String, String);

/// Folded state of one signal.
#[derive(Clone, Debug)]
pub struct SignalState {
    /// The CREATED record carrying the payload (min-order CREATED wins;
    /// later duplicates are tolerated and ignored).
    pub created: Signal,
    /// Derived status per the max-order transition.
    pub status: SignalStatus,
    /// Order key of the transition that produced `status` (None = never
    /// transitioned).
    latest_transition: Option<OrderKey>,
    /// The winning transition's `(timestamp_ms, close_reason)` — the same
    /// LWW machinery that derives `status` also derives close-state, so
    /// projections never re-implement the total order in SQL. `None` until
    /// the first transition applies; carries the values of whichever
    /// transition currently owns `latest_transition`. Bounded to one small
    /// tuple per signal (unlike parked transitions, which are never
    /// record-retained — see [`SpaceSignalState::pending`]).
    latest_transition_close_state: Option<CloseState>,
    /// The content track: the max-order EDITED record, whose
    /// payload is the content now. Independent of the lifecycle track —
    /// rewording a resolved decision leaves it resolved. Unlike transitions,
    /// the winning RECORD is retained (its payload IS the state), bounded to
    /// one per signal exactly like `created`. `None` = never edited; content
    /// is then the CREATED's payload.
    latest_edit: Option<(OrderKey, Signal)>,
}

/// The close-state a transition contributes: the transition's wall-clock
/// millisecond and its `CloseReason` proto value. A projection reads this
/// off the folded state when `status == Closed`, and its timestamp when
/// `status == Tombstoned`; it is stale-but-harmless for `Open` (callers gate on
/// status).
#[derive(Clone, Copy, Debug)]
struct CloseState {
    /// The transition record's `timestamp` (Unix millis) — the wall time
    /// the closer/reopener performed the event.
    timestamp_ms: i64,
    /// The transition's `close_reason` proto enum value (meaningful on
    /// CLOSED; `CLOSE_REASON_UNSPECIFIED` = 0 otherwise).
    close_reason: i32,
}

impl SignalState {
    /// Close time in Unix millis when the signal is currently `Closed`
    /// (`None` for `Open`/`Tombstoned` or a never-transitioned signal).
    ///
    /// Derived from the winning transition under the same total order that
    /// produced [`Self::status`], so it is order-insensitive by
    /// construction.
    pub fn closed_at_ms(&self) -> Option<i64> {
        match self.status {
            SignalStatus::Closed => self.latest_transition_close_state.map(|c| c.timestamp_ms),
            SignalStatus::Open | SignalStatus::Tombstoned => None,
        }
    }

    /// Tombstone time in Unix millis when the signal is currently
    /// `Tombstoned` (`None` otherwise).
    ///
    /// The mirror of [`Self::closed_at_ms`], derived from the same winning
    /// transition under the same total order. A projection that HIDES a
    /// tombstoned row rather than deleting it needs the timestamp to write, and
    /// taking it from the fold keeps the incremental write and the fold-backed
    /// rebuild agreeing about the same record.
    #[must_use]
    pub fn tombstoned_at_ms(&self) -> Option<i64> {
        match self.status {
            SignalStatus::Tombstoned => self.latest_transition_close_state.map(|c| c.timestamp_ms),
            SignalStatus::Open | SignalStatus::Closed => None,
        }
    }

    /// The winning CLOSED transition's `CloseReason` proto value when the
    /// signal is currently `Closed` (`None` otherwise).
    pub fn close_reason(&self) -> Option<i32> {
        match self.status {
            SignalStatus::Closed => self.latest_transition_close_state.map(|c| c.close_reason),
            SignalStatus::Open | SignalStatus::Tombstoned => None,
        }
    }

    /// The record whose payload is the signal's content NOW: the
    /// max-order EDITED record once any edit has applied, else the CREATED.
    ///
    /// Like the lifecycle track, the birth is the content *floor* — it never
    /// occupies an `OrderKey`, so any edit wins over it regardless of how the
    /// two records' keys compare. Content-bearing consumers read payload
    /// fields from here; birth attributes (author, timestamp, signal
    /// identity) stay on [`Self::created`] — an edit changes what a signal
    /// says, never who made it or when it was born.
    #[must_use]
    pub fn content(&self) -> &Signal {
        self.edited().unwrap_or(&self.created)
    }

    /// The winning EDITED record, when any edit has applied.
    ///
    /// For consumers that need to distinguish "content is the birth payload"
    /// from "content was replaced" — a projection rebuild overlays the edit
    /// onto birth columns rather than reading one record wholesale.
    #[must_use]
    pub fn edited(&self) -> Option<&Signal> {
        self.latest_edit.as_ref().map(|(_, r)| r)
    }
}

/// Folded state of a whole space: apply records in any order, read
/// signal states out. Projections and the CLI both consume this.
#[derive(Default)]
pub struct SpaceSignalState {
    /// Folded signals keyed by signal id. `BTreeMap` so iteration is
    /// deterministic (ascending id) — this crate's headline is determinism,
    /// and golden-output consumers read `iter()`.
    signals: BTreeMap<String, SignalState>,
    /// Orphan transitions keyed by signal id, parked until their CREATED
    /// arrives. Only the order key, derived status, and the small fixed-size
    /// close-state are parked — never the record — which bounds
    /// attacker-controlled amplification (a parked transition cannot pin an
    /// arbitrarily large payload in memory); record bytes live in segments.
    pending: HashMap<String, Vec<(OrderKey, SignalStatus, CloseState)>>,
    /// Orphan EDITED records, parked until their CREATED arrives. Unlike
    /// `pending`, the record must be retained (its payload is the content) —
    /// but only the max-order candidate per signal id, since LWW means no
    /// other can ever win. That keeps the amplification bound: at most one
    /// record per distinct orphan id, the same shape `created` itself costs.
    pending_edits: HashMap<String, (OrderKey, Signal)>,
    /// Dedup set (`record_id`) — set-union semantics.
    seen: HashSet<String>,
}

/// Convert a proto HLC (by reference) into the core `Hlc` for ordering.
/// A missing OR malformed HLC (actor not 16 bytes) maps uniformly to the
/// zero key — deterministic, and ordered before any real stamp.
///
/// The valid case delegates to kutl-core's canonical `TryFrom` — the single
/// definition of the proto→core mapping that determines LWW order, so it
/// cannot drift from the rest of the system. The fold stays TOTAL (never
/// errors): it is fed from untrusted segments and network catch-up, so a
/// single malformed record must degrade to a deterministic key rather than
/// break the fold. A well-formed record always carries a valid HLC; ingest
/// rejects malformed ones at the boundary, so this degenerate
/// mapping is a backstop, not a live path.
///
/// Shared by the fold (ordering records) and the catch-up filter (comparing
/// against a cursor). Both must agree: a missing HLC is always "before" any
/// real stamp, so a missing-HLC record only ever appears in from-zero
/// catch-ups.
pub(crate) fn proto_hlc_to_core(hlc: Option<&kutl_proto::sync::Hlc>) -> Hlc {
    hlc.and_then(|h| Hlc::try_from(h.clone()).ok())
        .unwrap_or(Hlc {
            physical_ms: 0,
            logical: 0,
            actor: ActorId(Uuid::nil()),
        })
}

/// Convert a record's proto HLC into the core `Hlc` for ordering.
fn order_hlc(record: &Signal) -> Hlc {
    proto_hlc_to_core(record.hlc.as_ref())
}

/// The fold's total order for a record: `(hlc, actor_did, record_id)`.
///
/// Public so a caller that needs records IN FOLD ORDER — a projection
/// rebuild replaying history, a trail sorting transitions — sorts by this
/// function instead of re-spelling the tuple. A second spelling of the
/// order is a divergence waiting for the day one copy changes.
#[must_use]
pub fn order_key(record: &Signal) -> OrderKey {
    (
        order_hlc(record),
        record.actor_did.clone(),
        record.record_id.clone(),
    )
}

/// Extract the close-state a transition record contributes to the fold.
fn close_state_of(record: &Signal) -> CloseState {
    CloseState {
        timestamp_ms: record.timestamp,
        close_reason: record.close_reason,
    }
}

/// The birth `(status, close_state)` a CREATED record seeds. A
/// CREATED may carry a `close_reason` meaning "born already `Closed(reason)`";
/// absent (`CLOSE_REASON_UNSPECIFIED` == 0) it is born `Open`. The birth is
/// the lattice *floor*, not a transition — it never occupies an `OrderKey`, so
/// a later real transition always LWWs on top of it against a `None` baseline.
fn born_state(created: &Signal) -> (SignalStatus, Option<CloseState>) {
    if created.close_reason == CloseReason::Unspecified as i32 {
        (SignalStatus::Open, None)
    } else {
        (SignalStatus::Closed, Some(close_state_of(created)))
    }
}

/// Re-derive the birth floor after the min-order CREATED changed (a lower-order
/// duplicate arrived). A real transition, if present, owns `status`/close-state
/// by order and sits above the floor — leave it untouched; only when no
/// transition has applied does birth alone determine the observable state.
/// This keeps born status independent of CREATED arrival order.
fn reseed_born_state(state: &mut SignalState) {
    if state.latest_transition.is_none() {
        let (status, close_state) = born_state(&state.created);
        state.status = status;
        state.latest_transition_close_state = close_state;
    }
}

/// Apply one EDITED record to `state` iff its key is the new maximum on the
/// content track — last-writer-wins, same total order as transitions but
/// tracked independently (an edit never reopens).
fn lww_edit(state: &mut SignalState, key: OrderKey, record: Signal) {
    if state
        .latest_edit
        .as_ref()
        .is_some_and(|(prev, _)| *prev >= key)
    {
        return; // an already-applied edit is later in the total order
    }
    state.latest_edit = Some((key, record));
}

/// Apply one transition to `state` iff its key is the new maximum in the
/// total order — last-writer-wins. Close-state travels with the winning
/// transition so projections read it off the folded state directly.
fn lww_transition(
    state: &mut SignalState,
    key: OrderKey,
    status: SignalStatus,
    close_state: CloseState,
) {
    if state
        .latest_transition
        .as_ref()
        .is_some_and(|prev| *prev >= key)
    {
        return; // an already-applied transition is later in the total order
    }
    state.status = status;
    state.latest_transition = Some(key);
    state.latest_transition_close_state = Some(close_state);
}

impl SpaceSignalState {
    /// Apply one record. Idempotent (duplicates no-op); order-insensitive.
    pub fn apply(&mut self, record: Signal) {
        // An empty record_id is a true legacy broadcast (pre-records senders
        // never set the field) — not a record; drop it before dedup so it
        // cannot occupy the "" key. Non-empty repeats are set-union no-ops.
        if record.record_id.is_empty() || !self.seen.insert(record.record_id.clone()) {
            return;
        }
        let Ok(event) = SignalEventType::try_from(record.event) else {
            // Future kind: skip (still counted in `record_count` via `seen`).
            return;
        };
        match event {
            // UNSPECIFIED here is a real record (record_id present) minted
            // by an upgraded node that left `event` unset — fold it as its
            // default meaning, CREATED. (True legacy broadcasts carry no
            // record_id and were dropped above.)
            SignalEventType::Unspecified | SignalEventType::Created => {
                self.apply_created(record);
            }
            SignalEventType::Closed => self.apply_transition(&record, SignalStatus::Closed),
            SignalEventType::Reopened => self.apply_transition(&record, SignalStatus::Open),
            SignalEventType::Tombstoned => {
                self.apply_transition(&record, SignalStatus::Tombstoned);
            }
            SignalEventType::Edited => self.apply_edited(record),
        }
    }

    /// LWW-apply an EDITED on the content track if the signal exists; park
    /// the max-order orphan candidate otherwise (see `pending_edits` for the
    /// retention bound).
    ///
    /// Records that carry no content (payload-less, or a transition payload)
    /// are dropped from the track entirely: they decompose to an empty
    /// overlay, so they can never contribute anything — but if one became
    /// the WINNING record, fold readers handed it wholesale would regress
    /// below birth content while the projections correctly ignore it.
    fn apply_edited(&mut self, record: Signal) {
        if !crate::content::carries_content(&record) {
            return;
        }
        let key = order_key(&record);
        if let Some(state) = self.signals.get_mut(&record.id) {
            lww_edit(state, key, record);
        } else {
            match self.pending_edits.entry(record.id.clone()) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if e.get().0 < key {
                        e.insert((key, record));
                    }
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert((key, record));
                }
            }
        }
    }

    fn apply_created(&mut self, record: Signal) {
        let id = record.id.clone();
        // Duplicate CREATED for one signal id: min-order birth wins.
        // The born status derives from the min-order CREATED's
        // `close_reason`, so replacing `created` with a lower-order duplicate
        // must re-seed the birth floor; any real transition still wins by
        // order (see `reseed_born_state`). Transitions key on signal id, not
        // on which CREATED won, so `latest_transition` itself is untouched.
        if let Some(state) = self.signals.get_mut(&id) {
            if order_key(&record) < order_key(&state.created) {
                state.created = record;
                reseed_born_state(state);
            }
        } else {
            let (status, close_state) = born_state(&record);
            let mut state = SignalState {
                created: record,
                status,
                latest_transition: None,
                latest_transition_close_state: close_state,
                latest_edit: None,
            };
            // Drain transitions that arrived before this CREATED, in park
            // order — LWW makes the outcome order-insensitive regardless.
            // They apply on top of the birth floor (a REOPENED after a
            // born-Closed → Open), the baseline being `None` as they expect.
            if let Some(parked) = self.pending.remove(&id) {
                for (key, status, close_state) in parked {
                    lww_transition(&mut state, key, status, close_state);
                }
            }
            // Same for the parked content candidate — already max-order.
            if let Some((key, edit)) = self.pending_edits.remove(&id) {
                lww_edit(&mut state, key, edit);
            }
            self.signals.insert(id, state);
        }
    }

    /// LWW-apply a transition if the signal exists; park
    /// `(key, status, close_state)` otherwise (see the `pending` field docs
    /// for why the full record is never parked).
    fn apply_transition(&mut self, record: &Signal, status: SignalStatus) {
        let key = order_key(record);
        let close_state = close_state_of(record);
        if let Some(state) = self.signals.get_mut(&record.id) {
            lww_transition(state, key, status, close_state);
        } else {
            self.pending
                .entry(record.id.clone())
                .or_default()
                .push((key, status, close_state));
        }
    }

    /// Folded state for one signal id (None until its CREATED arrives).
    pub fn get(&self, signal_id: &str) -> Option<&SignalState> {
        self.signals.get(signal_id)
    }

    /// Number of distinct records applied (incl. parked + future-kind).
    pub fn record_count(&self) -> usize {
        self.seen.len()
    }

    /// All signals in ascending signal-id order (deterministic — for
    /// projections, the CLI, and golden-output tests).
    pub fn iter(&self) -> impl Iterator<Item = (&String, &SignalState)> {
        self.signals.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_proto::sync::{CloseReason, Signal, SignalEventType};

    /// Build a record with just enough fields for the fold.
    fn rec(signal_id: &str, record_id: &str, event: SignalEventType, ms: u64) -> Signal {
        let mut s = Signal {
            id: signal_id.into(),
            space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
            record_id: record_id.into(),
            actor_did: format!("did:key:zActor{ms}"),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: ms,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        s.set_event(event);
        s
    }

    #[test]
    fn test_created_then_closed_then_reopened_is_open() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        st.apply(rec("a", "r2", SignalEventType::Closed, 2));
        st.apply(rec("a", "r3", SignalEventType::Reopened, 3));
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Open);
    }

    #[test]
    fn test_transition_before_created_parks_then_applies() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r2", SignalEventType::Closed, 2));
        assert!(
            st.get("a").is_none(),
            "orphan transition must not create state"
        );
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Closed);
    }

    #[test]
    fn test_duplicate_record_id_is_idempotent() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        assert_eq!(st.record_count(), 1);
    }

    #[test]
    fn test_duplicate_created_min_order_wins_both_arrival_orders() {
        // High-order CREATED first, min-order second:
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r5", SignalEventType::Created, 5));
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        assert_eq!(st.get("a").unwrap().created.record_id, "r1");

        // Reverse arrival order must agree (payload identity is
        // order-insensitive, not just status):
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        st.apply(rec("a", "r5", SignalEventType::Created, 5));
        assert_eq!(st.get("a").unwrap().created.record_id, "r1");
    }

    #[test]
    fn test_born_closed_resolved_reports_closed_reason_and_time() {
        // A born-Closed(Resolved) CREATED with no transition must report its
        // birth close-state — the case that regresses to "gone" if
        // `latest_transition_close_state` is not seeded on birth.
        let mut st = SpaceSignalState::default();
        let mut born = rec("a", "r1", SignalEventType::Created, 1);
        born.timestamp = 1_111;
        born.close_reason = CloseReason::Resolved as i32;
        st.apply(born);
        let s = st.get("a").unwrap();
        assert_eq!(s.status, SignalStatus::Closed);
        assert_eq!(s.close_reason(), Some(CloseReason::Resolved as i32));
        assert_eq!(s.closed_at_ms(), Some(1_111));
    }

    #[test]
    fn test_born_open_created_has_no_close_state() {
        // A CREATED with no close_reason is born Open — exactly as today.
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        let s = st.get("a").unwrap();
        assert_eq!(s.status, SignalStatus::Open);
        assert_eq!(s.close_reason(), None);
        assert_eq!(s.closed_at_ms(), None);
    }

    #[test]
    fn test_born_closed_then_reopened_is_open() {
        // A transition LWWs on top of the birth floor: REOPENED after a
        // born-Closed → Open, with close-state cleared.
        let mut st = SpaceSignalState::default();
        let mut born = rec("a", "r1", SignalEventType::Created, 1);
        born.close_reason = CloseReason::Resolved as i32;
        st.apply(born);
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Closed);
        st.apply(rec("a", "r2", SignalEventType::Reopened, 2));
        let s = st.get("a").unwrap();
        assert_eq!(s.status, SignalStatus::Open);
        assert_eq!(s.close_reason(), None);
        assert_eq!(s.closed_at_ms(), None);
    }

    #[test]
    fn test_born_status_independent_of_duplicate_created_arrival_order() {
        // A high-order born-Open duplicate must not mask the min-order
        // born-Closed CREATED, in either arrival order — born status derives
        // from the min-order create, not from arrival.
        let born_closed = || {
            let mut r = rec("a", "r1", SignalEventType::Created, 1);
            r.timestamp = 1_000;
            r.close_reason = CloseReason::Declined as i32;
            r
        };
        let born_open = || rec("a", "r5", SignalEventType::Created, 5);

        for (first, second) in [(born_open(), born_closed()), (born_closed(), born_open())] {
            let mut st = SpaceSignalState::default();
            st.apply(first);
            st.apply(second);
            let s = st.get("a").unwrap();
            assert_eq!(s.created.record_id, "r1", "min-order create wins identity");
            assert_eq!(s.status, SignalStatus::Closed);
            assert_eq!(s.close_reason(), Some(CloseReason::Declined as i32));
            assert_eq!(s.closed_at_ms(), Some(1_000));
        }
    }

    #[test]
    fn test_close_state_derived_and_cleared_by_reopen() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        // Open signals expose no close-state.
        assert_eq!(st.get("a").unwrap().closed_at_ms(), None);
        assert_eq!(st.get("a").unwrap().close_reason(), None);

        // Close with a wall-clock timestamp + a close_reason value.
        let mut closed = rec("a", "r2", SignalEventType::Closed, 2);
        closed.timestamp = 2_222;
        closed.close_reason = 2; // CLOSE_REASON_DECLINED
        st.apply(closed);
        assert_eq!(st.get("a").unwrap().closed_at_ms(), Some(2_222));
        assert_eq!(st.get("a").unwrap().close_reason(), Some(2));

        // A later reopen clears close-state.
        st.apply(rec("a", "r3", SignalEventType::Reopened, 3));
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Open);
        assert_eq!(st.get("a").unwrap().closed_at_ms(), None);
        assert_eq!(st.get("a").unwrap().close_reason(), None);
    }

    #[test]
    fn test_close_state_from_max_order_transition_ignores_arrival_order() {
        // CLOSED is the max-order transition; its close-state must win even
        // when a lower-order REOPENED arrives afterward.
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        st.apply(rec("a", "r2", SignalEventType::Reopened, 3));
        let mut closed = rec("a", "r3", SignalEventType::Closed, 5);
        closed.timestamp = 5_555;
        closed.close_reason = 1; // CLOSE_REASON_RESOLVED
        st.apply(closed);
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Closed);
        assert_eq!(st.get("a").unwrap().closed_at_ms(), Some(5_555));
        assert_eq!(st.get("a").unwrap().close_reason(), Some(1));
    }

    #[test]
    fn test_tombstone_wins_as_latest_transition_and_is_reversible() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        st.apply(rec("a", "r2", SignalEventType::Tombstoned, 5));
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Tombstoned);
        st.apply(rec("a", "r3", SignalEventType::Reopened, 6));
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Open);
    }

    #[test]
    fn test_concurrent_transitions_resolve_by_hlc_lww() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        // Arrival order reversed from HLC order:
        st.apply(rec("a", "r3", SignalEventType::Reopened, 9));
        st.apply(rec("a", "r2", SignalEventType::Closed, 5));
        assert_eq!(
            st.get("a").unwrap().status,
            SignalStatus::Open,
            "max-HLC transition wins"
        );
    }

    #[test]
    fn test_unknown_event_value_retained_but_skipped() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        let mut future = rec("a", "r9", SignalEventType::Created, 9);
        future.event = 42; // an event kind from the future
        st.apply(future);
        assert_eq!(
            st.record_count(),
            2,
            "future record is counted (it still replicates)"
        );
        assert_eq!(
            st.get("a").unwrap().status,
            SignalStatus::Open,
            "but does not perturb state"
        );
    }

    #[test]
    fn test_legacy_unspecified_event_folds_as_created() {
        let mut st = SpaceSignalState::default();
        let legacy = rec("a", "r1", SignalEventType::Unspecified, 1);
        st.apply(legacy);
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Open);
    }

    #[test]
    fn test_empty_record_id_is_fully_ignored() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "", SignalEventType::Created, 1));
        assert_eq!(st.record_count(), 0, "legacy broadcast is not a record");
        assert!(st.get("a").is_none());
    }

    #[test]
    fn test_missing_hlc_orders_before_any_real_stamp() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r1", SignalEventType::Created, 5));
        let mut no_hlc = rec("a", "r2", SignalEventType::Closed, 0);
        no_hlc.hlc = None;
        st.apply(no_hlc);
        // The zero-clock key still applies as the only transition:
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Closed);
        // ... but any real stamp beats it in LWW, however early:
        st.apply(rec("a", "r3", SignalEventType::Reopened, 1));
        assert_eq!(st.get("a").unwrap().status, SignalStatus::Open);
    }

    /// Build a record carrying a flag payload whose `message` is the
    /// observable content.
    fn flag_rec(
        signal_id: &str,
        record_id: &str,
        event: SignalEventType,
        ms: u64,
        message: &str,
    ) -> Signal {
        let mut r = rec(signal_id, record_id, event, ms);
        r.payload = Some(kutl_proto::sync::signal::Payload::Flag(
            kutl_proto::sync::FlagPayload {
                message: message.into(),
                ..Default::default()
            },
        ));
        r
    }

    fn content_message(st: &SpaceSignalState, id: &str) -> String {
        match &st.get(id).unwrap().content().payload {
            Some(kutl_proto::sync::signal::Payload::Flag(f)) => f.message.clone(),
            other => panic!("expected flag payload, got {other:?}"),
        }
    }

    #[test]
    fn test_edited_updates_content_not_identity() {
        let mut st = SpaceSignalState::default();
        st.apply(flag_rec("a", "r1", SignalEventType::Created, 1, "birth"));
        st.apply(flag_rec("a", "r2", SignalEventType::Edited, 2, "now"));
        let s = st.get("a").unwrap();
        assert_eq!(content_message(&st, "a"), "now");
        assert_eq!(s.created.record_id, "r1", "birth record untouched");
        assert_eq!(
            s.status,
            SignalStatus::Open,
            "an edit is not a lifecycle event"
        );
    }

    #[test]
    fn test_edit_does_not_reopen() {
        // The two-track orthogonality pin: rewording a resolved decision
        // leaves it resolved, even when the edit is the latest event.
        let mut st = SpaceSignalState::default();
        st.apply(flag_rec("a", "r1", SignalEventType::Created, 1, "birth"));
        let mut closed = rec("a", "r2", SignalEventType::Closed, 2);
        closed.close_reason = CloseReason::Resolved as i32;
        st.apply(closed);
        st.apply(flag_rec("a", "r3", SignalEventType::Edited, 3, "now"));
        let s = st.get("a").unwrap();
        assert_eq!(s.status, SignalStatus::Closed);
        assert_eq!(s.close_reason(), Some(CloseReason::Resolved as i32));
        assert_eq!(content_message(&st, "a"), "now");
    }

    #[test]
    fn test_transition_does_not_revert_content() {
        let mut st = SpaceSignalState::default();
        st.apply(flag_rec("a", "r1", SignalEventType::Created, 1, "birth"));
        st.apply(flag_rec("a", "r2", SignalEventType::Edited, 2, "now"));
        st.apply(rec("a", "r3", SignalEventType::Closed, 3));
        assert_eq!(content_message(&st, "a"), "now");
    }

    #[test]
    fn test_concurrent_edits_resolve_by_hlc_lww() {
        let mut st = SpaceSignalState::default();
        st.apply(flag_rec("a", "r1", SignalEventType::Created, 1, "birth"));
        // Arrival order reversed from HLC order:
        st.apply(flag_rec("a", "r3", SignalEventType::Edited, 9, "late"));
        st.apply(flag_rec("a", "r2", SignalEventType::Edited, 5, "early"));
        assert_eq!(content_message(&st, "a"), "late", "max-HLC edit wins");
    }

    #[test]
    fn test_orphan_edit_parks_then_applies() {
        let mut st = SpaceSignalState::default();
        st.apply(flag_rec("a", "r2", SignalEventType::Edited, 2, "now"));
        assert!(st.get("a").is_none(), "orphan edit must not create state");
        st.apply(flag_rec("a", "r1", SignalEventType::Created, 1, "birth"));
        assert_eq!(content_message(&st, "a"), "now");
    }

    #[test]
    fn test_orphan_edits_keep_only_max_order() {
        // Both orphan arrival orders must converge on the max-order edit —
        // the parked slot retains one candidate, and LWW says which.
        for (first, second) in [("early", "late"), ("late", "early")] {
            let mut st = SpaceSignalState::default();
            let key_of = |m: &str| if m == "early" { (5, "r2") } else { (9, "r3") };
            let (ms, rid) = key_of(first);
            st.apply(flag_rec("a", rid, SignalEventType::Edited, ms, first));
            let (ms, rid) = key_of(second);
            st.apply(flag_rec("a", rid, SignalEventType::Edited, ms, second));
            st.apply(flag_rec("a", "r1", SignalEventType::Created, 1, "birth"));
            assert_eq!(content_message(&st, "a"), "late");
        }
    }

    #[test]
    fn test_payloadless_edited_never_occupies_the_content_track() {
        // A payload-less (or transition-payload) EDITED can never contribute
        // content, so it must not become the winning content record — the
        // projections' overlay decomposition ignores it, and a fold reader
        // handed it wholesale would regress below birth content.
        let mut st = SpaceSignalState::default();
        st.apply(flag_rec("a", "r1", SignalEventType::Created, 1, "birth"));
        st.apply(rec("a", "r2", SignalEventType::Edited, 2)); // no payload
        assert_eq!(
            content_message(&st, "a"),
            "birth",
            "a content-free EDITED must leave content() on the CREATED"
        );
        // A later REAL edit still wins.
        st.apply(flag_rec("a", "r3", SignalEventType::Edited, 3, "now"));
        assert_eq!(content_message(&st, "a"), "now");
    }

    #[test]
    fn test_edit_below_birth_key_still_wins_content() {
        // The birth is the content FLOOR, not an OrderKey occupant: an edit
        // whose key orders below the CREATED's (zero-HLC backstop) still
        // replaces content — mirroring how a zero-key transition applies.
        let mut st = SpaceSignalState::default();
        st.apply(flag_rec("a", "r1", SignalEventType::Created, 5, "birth"));
        let mut low = flag_rec("a", "r2", SignalEventType::Edited, 0, "now");
        low.hlc = None;
        st.apply(low);
        assert_eq!(content_message(&st, "a"), "now");
    }

    #[test]
    fn test_multiple_parked_transitions_drain_to_max_order() {
        let mut st = SpaceSignalState::default();
        st.apply(rec("a", "r5", SignalEventType::Closed, 5));
        st.apply(rec("a", "r3", SignalEventType::Reopened, 3));
        assert!(
            st.get("a").is_none(),
            "transitions park until their CREATED arrives"
        );
        st.apply(rec("a", "r1", SignalEventType::Created, 1));
        assert_eq!(
            st.get("a").unwrap().status,
            SignalStatus::Closed,
            "max-order parked transition wins"
        );
    }

    use proptest::prelude::*;

    /// Small-pool record generator: few signal ids and coarse clocks so
    /// permutations collide interestingly.
    ///
    /// `close_reason` varies over `0..5` (0 = UNSPECIFIED, so a CREATED is
    /// sometimes born-Open and sometimes born-`Closed(Resolved/Declined/
    /// Withdrawn/Superseded)`; a CLOSED carries a reason), and `timestamp`
    /// tracks the clock so the born/close-state wall time is observable — the
    /// confluence net thereby covers close-state, not just status.
    /// The mixed event pool naturally yields REOPENED/CLOSED after a
    /// born-Closed, the transitions that must LWW over the birth floor.
    fn arb_record(i: usize) -> impl Strategy<Value = Signal> {
        let events = prop_oneof![
            Just(SignalEventType::Created),
            Just(SignalEventType::Closed),
            Just(SignalEventType::Reopened),
            Just(SignalEventType::Tombstoned),
            Just(SignalEventType::Edited),
        ];
        ("[ab]", events, 0u64..5, 0i32..5).prop_map(move |(sid, ev, ms, reason)| {
            let mut r = rec(&sid, &format!("r{i}"), ev, ms);
            r.timestamp = ms.cast_signed(); // ms ∈ 0..5, never wraps
            r.close_reason = reason;
            r
        })
    }

    fn arb_records() -> impl Strategy<Value = Vec<Signal>> {
        (1usize..14).prop_flat_map(|n| (0..n).map(arb_record).collect::<Vec<_>>())
    }

    /// Full observable state, not just status: payload identity (which
    /// CREATED won), the winning transition key, the projected close-state
    /// (`close_reason()`/`closed_at_ms()`), and the winning CONTENT identity
    /// (which record `content()` resolves to — the second track) must
    /// all be order-insensitive — a status-only snapshot lets a
    /// delete-the-min-order-rule mutant pass, and omitting close-state would
    /// miss a born-Closed whose seed is arrival-order-dependent.
    type Snapshot = Vec<(
        String,
        SignalStatus,
        String,
        Option<OrderKey>,
        Option<i32>,
        Option<i64>,
        String,
    )>;

    fn snapshot(st: &SpaceSignalState) -> Snapshot {
        let mut v: Vec<_> = st
            .iter()
            .map(|(id, s)| {
                (
                    id.clone(),
                    s.status.clone(),
                    s.created.record_id.clone(),
                    s.latest_transition.clone(),
                    s.close_reason(),
                    s.closed_at_ms(),
                    s.content().record_id.clone(),
                )
            })
            .collect();
        v.sort();
        v
    }

    proptest! {
        /// Permutation invariance: any record order yields identical state.
        #[test]
        fn fold_is_order_independent(
            records in arb_records(),
            perm in prop::collection::vec(any::<prop::sample::Index>(), 0..24),
        ) {
            let mut a = SpaceSignalState::default();
            for r in &records { a.apply(r.clone()); }

            let mut shuffled = records.clone();
            let n = shuffled.len();
            for (k, idx) in perm.iter().enumerate() {
                let i = k % n;
                let j = idx.index(n);
                shuffled.swap(i, j);
            }
            let mut b = SpaceSignalState::default();
            for r in &shuffled { b.apply(r.clone()); }

            prop_assert_eq!(snapshot(&a), snapshot(&b));
        }

        /// Partition-merge confluence: fold(A ∪ B) == fold(A) then B.
        #[test]
        fn fold_is_confluent_across_partitions(
            records in arb_records(),
            split in any::<prop::sample::Index>(),
        ) {
            let cut = split.index(records.len() + 1);
            let mut merged = SpaceSignalState::default();
            for r in &records { merged.apply(r.clone()); }

            let mut staged = SpaceSignalState::default();
            for r in &records[..cut] { staged.apply(r.clone()); }
            for r in &records[cut..] { staged.apply(r.clone()); }
            // Duplicate delivery of the first partition (idempotence):
            for r in &records[..cut] { staged.apply(r.clone()); }

            prop_assert_eq!(snapshot(&merged), snapshot(&staged));
        }
    }
}
