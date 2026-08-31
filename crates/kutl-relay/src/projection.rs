//! What a signal record does to a projection — derived once, executed per
//! substrate.
//!
//! Both substrates store the same records behind the [`crate::record_log`]
//! trait; their projections are denormalizations derived from those records.
//! This module owns the derivation: which event mutates what, the values a
//! mutation stamps, and the scoping rule every write obeys. A backend
//! translates these facts into its own SQL and columns; it never re-derives
//! them — two independent derivations is how substrates come to disagree
//! about what a record says. The payload half of the same rule is
//! [`crate::change_backend::PayloadColumns`]; this is the lifecycle half.

use kutl_proto::sync::{CloseReason, Signal, SignalEventType};

/// The admitting seam's verdict on which space a record belongs to.
///
/// A newtype rather than a bare `&str` so a projection write cannot silently
/// be scoped by `record.space_id` — on the replicated path that field is a
/// peer's claim, and a write keyed by the claim can reach (or veto) a row in
/// a space the record was never admitted to. Constructed once, where the
/// seam's parameter enters the backend; every write helper takes this type.
#[derive(Clone, Copy)]
pub struct AdmittedSpace<'a>(&'a str);

impl<'a> AdmittedSpace<'a> {
    /// Wrap the space id the ADMITTING SEAM supplied — never
    /// `record.space_id`.
    #[must_use]
    pub fn new(space_id: &'a str) -> Self {
        Self(space_id)
    }

    /// The verdict space, for binding into a query.
    #[must_use]
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

/// The values a CLOSED record stamps onto its row.
///
/// One derivation for both substrates: the stamp is the RECORD's own time
/// (never the projection clock), the reason is the record's discriminant,
/// and the note rides the transition payload. A backend that stores fewer
/// of these (narrower denormalization) still derives the ones it stores
/// from here.
pub struct CloseFacts<'a> {
    /// The record's own time, Unix milliseconds.
    pub closed_at_ms: i64,
    /// Why it closed.
    pub reason: CloseReason,
    /// The closer's free-text note, `None` when the payload carries none.
    pub note: Option<&'a str>,
}

/// A CREATED row's birth close-state, when the record was born closed.
///
/// A CREATED carrying a `close_reason` was born closed — a `## = …` heading
/// materializes as a CREATED with reason RESOLVED. `UNSPECIFIED` is proto3's
/// default and means "no reason", which on a CREATED means born open. The
/// stamp is the CREATED's own time, so birth and close coincide by
/// construction; there is no note, because a CREATED has no transition
/// payload to carry one.
pub struct BornClose {
    /// The CREATED record's own time, Unix milliseconds.
    pub closed_at_ms: i64,
    /// The reason the record was born closed with.
    pub reason: CloseReason,
}

/// How a record mutates the projection.
pub enum ProjectionMutation<'a> {
    /// CREATED, or its legacy UNSPECIFIED spelling: insert the row (and its
    /// detail), carrying [`BornClose`] when the record was born closed.
    Insert(Option<BornClose>),
    /// CLOSED: stamp the close state.
    Close(CloseFacts<'a>),
    /// REOPENED: clear ALL close state and un-hide the row — a reopen is how
    /// the document-revive cascade brings a tombstoned signal back.
    Reopen,
    /// EDITED: overlay content per the record's
    /// [`crate::change_backend::ContentOverlay`]; any editor stamp lands only
    /// when a stored value actually changed. Replay quietness is by
    /// VALUE-DIFF, not rows-matched — a database counts matched rows even
    /// when the new values equal the old, so a rows-matched gate would
    /// re-stamp (and re-surface) on every redelivery.
    Edit,
    /// TOMBSTONED: hide the row at the record's own time. First tombstone
    /// wins — a redelivered tombstone must not move the stamp.
    Hide {
        /// The tombstone record's own time, Unix milliseconds.
        at_ms: i64,
    },
}

/// Classify how `record` mutates the projection — the single dispatch both
/// substrates share.
#[must_use]
pub fn classify(record: &Signal) -> ProjectionMutation<'_> {
    match record.event() {
        SignalEventType::Closed => ProjectionMutation::Close(close_facts(record)),
        SignalEventType::Reopened => ProjectionMutation::Reopen,
        SignalEventType::Edited => ProjectionMutation::Edit,
        SignalEventType::Tombstoned => ProjectionMutation::Hide {
            at_ms: record.timestamp,
        },
        SignalEventType::Created | SignalEventType::Unspecified => {
            ProjectionMutation::Insert(born_close(record))
        }
    }
}

/// The close facts a CLOSED record carries. See [`CloseFacts`].
#[must_use]
pub fn close_facts(record: &Signal) -> CloseFacts<'_> {
    let note = match record.payload.as_ref() {
        Some(kutl_proto::sync::signal::Payload::Transition(t)) if !t.note.is_empty() => {
            Some(t.note.as_str())
        }
        _ => None,
    };
    CloseFacts {
        closed_at_ms: record.timestamp,
        reason: record.close_reason(),
        note,
    }
}

/// The birth close-state of a CREATED record, `None` when born open. See
/// [`BornClose`].
#[must_use]
pub fn born_close(record: &Signal) -> Option<BornClose> {
    match record.close_reason() {
        CloseReason::Unspecified => None,
        reason => Some(BornClose {
            closed_at_ms: record.timestamp,
            reason,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(event: SignalEventType, reason: CloseReason, note: &str) -> Signal {
        let mut r = Signal {
            id: "sig-1".to_owned(),
            space_id: "space-1".to_owned(),
            timestamp: 5_000,
            payload: (!note.is_empty()).then(|| {
                kutl_proto::sync::signal::Payload::Transition(kutl_proto::sync::TransitionPayload {
                    note: note.to_owned(),
                })
            }),
            ..Default::default()
        };
        r.set_event(event);
        r.set_close_reason(reason);
        r
    }

    #[test]
    fn test_classify_close_carries_the_records_facts() {
        let r = record(SignalEventType::Closed, CloseReason::Declined, "not now");
        let ProjectionMutation::Close(facts) = classify(&r) else {
            panic!("a CLOSED record classifies as Close");
        };
        assert_eq!(facts.closed_at_ms, 5_000);
        assert_eq!(facts.reason, CloseReason::Declined);
        assert_eq!(facts.note, Some("not now"));
    }

    #[test]
    fn test_classify_close_without_note() {
        let r = record(SignalEventType::Closed, CloseReason::Resolved, "");
        let ProjectionMutation::Close(facts) = classify(&r) else {
            panic!("Close expected");
        };
        assert_eq!(facts.note, None);
    }

    #[test]
    fn test_born_closed_created_vs_born_open() {
        let born = record(SignalEventType::Created, CloseReason::Resolved, "");
        let ProjectionMutation::Insert(Some(b)) = classify(&born) else {
            panic!("a CREATED with a reason is born closed");
        };
        assert_eq!(b.closed_at_ms, 5_000);
        assert_eq!(b.reason, CloseReason::Resolved);

        let open = record(SignalEventType::Created, CloseReason::Unspecified, "");
        assert!(matches!(classify(&open), ProjectionMutation::Insert(None)));
    }

    #[test]
    fn test_legacy_unspecified_event_is_a_created() {
        let r = record(SignalEventType::Unspecified, CloseReason::Unspecified, "");
        assert!(matches!(classify(&r), ProjectionMutation::Insert(None)));
    }

    #[test]
    fn test_hide_carries_the_records_time() {
        let r = record(SignalEventType::Tombstoned, CloseReason::Unspecified, "");
        let ProjectionMutation::Hide { at_ms } = classify(&r) else {
            panic!("Hide expected");
        };
        assert_eq!(at_ms, 5_000);
    }
}
