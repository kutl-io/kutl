//! Summarizing and filtering a folded space — the shared shape behind
//! `kutl signal list` and MCP `list_signals`.
//!
//! Both surfaces answer the same question ("what signals are in this space,
//! narrowed how?") and must give the same answer. Two doors each carrying
//! their own copy of the rule is how they drift, so the rule lives here,
//! beside the fold it reads, and each surface only renders.
//!
//! The fold is the source, not the projection. That keeps the answer identical
//! on a self-hosted relay and on kutlhub, and identical again on a client
//! reading its own segments — the projection is derived, and deriving a list
//! from it would be a second path to a fact the fold already owns.

use kutl_proto::sync::{Signal, signal};

use crate::fold::{SignalState, SignalStatus, SpaceSignalState};

/// The record kind a signal's CREATED payload carries.
///
/// A *record* kind (flag / chat / decision / reply), which is a different axis
/// from a flag's *intent* kind (`info` / `question` / …) — the two are
/// orthogonal and both are filterable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalKind {
    /// A flag payload (`FlagPayload`). Comments are a flag KIND, so they
    /// summarize as `flag`.
    Flag,
    /// A chat payload (`ChatPayload`).
    Chat,
    /// A decision payload (`DecisionPayload`).
    Decision,
    /// A reply payload (`ReplyPayload`).
    Reply,
}

impl SignalKind {
    /// The stable lowercase label used in human and JSON output.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            SignalKind::Flag => "flag",
            SignalKind::Chat => "chat",
            SignalKind::Decision => "decision",
            SignalKind::Reply => "reply",
        }
    }

    /// Parse a lowercase label back into a kind. `None` for anything else.
    #[must_use]
    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "flag" => Some(SignalKind::Flag),
            "chat" => Some(SignalKind::Chat),
            "decision" => Some(SignalKind::Decision),
            "reply" => Some(SignalKind::Reply),
            _ => None,
        }
    }
}

/// The record kind of a folded signal's CREATED record.
///
/// `None` for a record carrying no payload or a transition payload — a
/// transition is an event about a signal, never a signal itself.
#[must_use]
pub fn kind_of(created: &Signal) -> Option<SignalKind> {
    match created.payload {
        Some(signal::Payload::Flag(_)) => Some(SignalKind::Flag),
        Some(signal::Payload::Chat(_)) => Some(SignalKind::Chat),
        Some(signal::Payload::Decision(_)) => Some(SignalKind::Decision),
        Some(signal::Payload::Reply(_)) => Some(SignalKind::Reply),
        Some(signal::Payload::Transition(_)) | None => None,
    }
}

/// Which lifecycle states a listing includes.
///
/// Tombstoned is never included by any variant. A tombstone is a soft delete —
/// it is hidden from the projection too — so "show me everything" means
/// everything a reader may still see, not everything the log holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatusFilter {
    /// Open signals only.
    #[default]
    Open,
    /// Closed signals only.
    Closed,
    /// Open and closed.
    All,
}

impl StatusFilter {
    /// Whether a folded status passes. Tombstoned is always hidden.
    #[must_use]
    pub fn admits(self, status: &SignalStatus) -> bool {
        match status {
            SignalStatus::Tombstoned => false,
            SignalStatus::Open => matches!(self, StatusFilter::Open | StatusFilter::All),
            SignalStatus::Closed => matches!(self, StatusFilter::Closed | StatusFilter::All),
        }
    }
}

/// How a listing narrows a folded space. Every field unset lists every open
/// signal, which is the useful default for both surfaces.
#[derive(Clone, Copy, Debug, Default)]
pub struct SignalFilters<'a> {
    /// Which lifecycle states to include.
    pub status: StatusFilter,
    /// Restrict to one record kind.
    pub kind: Option<SignalKind>,
    /// Restrict to signals attached to this document id.
    pub document_id: Option<&'a str>,
    /// Restrict to flags of this intent kind (a `FlagKind` proto
    /// discriminant). Only signals whose CREATED payload is a `Flag` with a
    /// matching `kind` pass — so it implies the `flag` record kind.
    pub flag_kind: Option<i32>,
}

impl SignalFilters<'_> {
    /// Whether one folded signal passes every active filter.
    #[must_use]
    pub fn admits(&self, state: &SignalState) -> bool {
        if !self.status.admits(&state.status) {
            return false;
        }
        if let Some(want) = self.kind
            && kind_of(&state.created) != Some(want)
        {
            return false;
        }
        if let Some(want) = self.document_id
            && state.created.document_id.as_deref() != Some(want)
        {
            return false;
        }
        if let Some(want) = self.flag_kind {
            let matches_flag = matches!(
                &state.created.payload,
                Some(signal::Payload::Flag(f)) if f.kind == want
            );
            if !matches_flag {
                return false;
            }
        }
        true
    }
}

/// One folded signal, flattened for listing.
///
/// Deliberately not `SignalDetail` (`kutl-relay`'s per-signal read): a listing
/// answers "what is here", a detail answers "everything about this one". Nested
/// replies and reactions belong to the second and would make a list of a busy
/// space unbounded.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SignalSummary {
    /// Signal id.
    pub id: String,
    /// The document the signal is attached to, if any.
    pub document_id: Option<String>,
    /// Record kind label (`flag`/`chat`/`decision`/`reply`), absent when the
    /// CREATED record carries no payload.
    pub kind: Option<&'static str>,
    /// The flag's message or the reply's body, when the record carries one.
    pub message: Option<String>,
    /// The flag intent-kind label, flags only.
    pub flag_kind: Option<&'static str>,
    /// Who the signal is for (`participant`/`space`), flags only — a reply,
    /// chat, or decision addresses no one.
    ///
    /// Named as the per-signal detail read names it, because it is the same
    /// fact: a listing that spelled audience differently would make a caller
    /// learn the concept twice.
    pub audience: Option<&'static str>,
    /// The addressed participant's DID, when the audience names one. `None`
    /// for a space-wide flag, which is what distinguishes "someone should look
    /// at this" from "you should".
    pub target_did: Option<String>,
    /// Current status (`open`/`closed`). Tombstoned never appears — the filter
    /// excludes it.
    pub status: &'static str,
    /// The CREATED record's wall-clock timestamp (Unix millis).
    pub created_ms: i64,
    /// The close time (Unix millis) when currently closed.
    pub closed_ms: Option<i64>,
    /// DID of the signal's author.
    pub author_did: String,
}

/// Summarize one folded signal.
///
/// Content reads apply the [`crate::content::ContentOverlay`] rule — carried
/// fields from the winning EDITED replace, uncarried fields keep the birth
/// value. Reading the winning record wholesale instead would
/// blank a comment's message on an anchor-only edit, since that edit
/// deliberately does not carry the body. Identity and birth attributes stay
/// on the CREATED record.
#[must_use]
pub fn summarize(id: &str, state: &SignalState) -> SignalSummary {
    let (mut message, flag_kind) = match &state.created.payload {
        Some(signal::Payload::Flag(f)) => (
            Some(f.message.clone()),
            Some(kutl_proto::vocab::flag_kind_to_str(f.kind)),
        ),
        Some(signal::Payload::Reply(r)) => (Some(r.body.clone()), None),
        _ => (None, None),
    };
    if let Some(edit) = state.edited() {
        let overlay = crate::content::ContentOverlay::from_record(edit);
        if let Some(m) = overlay.message.or(overlay.body) {
            message = Some(m.to_owned());
        }
    }
    // Audience is a birth attribute, so it is read off CREATED with no overlay:
    // an edit revises what a flag says, never who it was addressed to.
    // Resolved through the precedence accessor rather than the stored pair, so
    // a flag replicated from a peer that sets only the typed audience does not
    // summarize as unaddressed.
    let (audience, target_did) = match &state.created.payload {
        Some(signal::Payload::Flag(f)) => {
            let (audience, target_did) = kutl_proto::vocab::flag_audience_untyped(f);
            (
                Some(kutl_proto::vocab::audience_type_to_str(audience)),
                target_did.map(str::to_owned),
            )
        }
        _ => (None, None),
    };
    SignalSummary {
        id: id.to_owned(),
        document_id: state.created.document_id.clone(),
        kind: kind_of(&state.created).map(SignalKind::label),
        message,
        flag_kind,
        audience,
        target_did,
        status: status_label(&state.status),
        created_ms: state.created.timestamp,
        closed_ms: state.closed_at_ms(),
        author_did: state.created.author_did.clone(),
    }
}

/// Every signal in `fold` passing `filters`, in the fold's deterministic
/// ascending-id order.
#[must_use]
pub fn list(fold: &SpaceSignalState, filters: &SignalFilters<'_>) -> Vec<SignalSummary> {
    fold.iter()
        .filter(|(_, state)| filters.admits(state))
        .map(|(id, state)| summarize(id, state))
        .collect()
}

/// One event in a signal's lifecycle audit trail.
///
/// Every record carrying the signal's id is one of these — the CREATED that
/// opened it, each transition after, and any EDITED content record.
/// Unlike [`SignalSummary`], which is the signal's current state,
/// this is how it got there.
/// Owned string fields rather than `&'static str` labels: this rides inside
/// `SignalDetail`, which round-trips through JSON, so it must deserialize too.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TransitionEntry {
    /// This event's record id.
    pub record_id: String,
    /// The event kind (`created`/`closed`/`reopened`/`tombstoned`/`edited`).
    pub event: String,
    /// The event's wall-clock timestamp (Unix millis).
    pub timestamp_ms: i64,
    /// The DID that performed this event, when the record carries one.
    pub actor_did: Option<String>,
    /// The close reason label carried by a CLOSED transition, if any.
    pub close_reason: Option<String>,
    /// The caller-supplied note carried by a transition.
    pub note: Option<String>,
}

/// The stable lowercase label for a transition event kind.
///
/// `UNSPECIFIED` reads as `created`: a record predating the event field is a
/// CREATED by construction, and the fold treats it the same way.
#[must_use]
pub fn event_label(event: kutl_proto::sync::SignalEventType) -> &'static str {
    use kutl_proto::sync::SignalEventType;
    match event {
        SignalEventType::Unspecified | SignalEventType::Created => "created",
        SignalEventType::Closed => "closed",
        SignalEventType::Reopened => "reopened",
        SignalEventType::Tombstoned => "tombstoned",
        SignalEventType::Edited => "edited",
    }
}

/// The transition audit trail for `signal_id`, oldest first.
///
/// Reads RAW records rather than the fold, because the fold deliberately keeps
/// only the winning transition — that is what makes it a state machine. The
/// trail is the opposite question ("what happened, in order"), so it needs
/// every record, including ones the LWW discarded.
///
/// **Ordered by HLC**, using the fold's own order key — not by wall-clock
/// timestamp. Two transitions on one signal routinely land in the SAME
/// millisecond (a close immediately followed by a reopen, a cascade), and a
/// wall-clock sort then falls back to a tiebreak that has nothing to do with
/// causality: it would show a reopen before the close it undid. The HLC's
/// logical counter is what distinguishes them, and it is the same order the
/// fold uses to decide the winning transition — so the trail and the state
/// agree by construction.
///
/// Records with an empty `record_id` are legacy bare broadcasts, not durable
/// events, and are skipped — the same rule the fold applies.
#[must_use]
pub fn transition_history<'a>(
    signal_id: &str,
    records: impl IntoIterator<Item = &'a Signal>,
) -> Vec<TransitionEntry> {
    use kutl_proto::sync::SignalEventType;

    let mut events: Vec<(crate::fold::OrderKey, TransitionEntry)> = records
        .into_iter()
        .filter(|r| r.id == signal_id && !r.record_id.is_empty())
        .map(|r| {
            // `close_reason` rides the record itself, not the transition
            // payload — it is meaningful on a CREATED too (a born-resolved
            // decision). `UNSPECIFIED` means "not a close", so it
            // reports as absent rather than as a reason named "unspecified".
            let close_reason = match r.close_reason() {
                kutl_proto::sync::CloseReason::Unspecified => None,
                reason => Some(crate::payloads::close_reason_to_wire(reason).to_owned()),
            };
            let note = match &r.payload {
                Some(signal::Payload::Transition(t)) if !t.note.is_empty() => Some(t.note.clone()),
                _ => None,
            };
            let entry = TransitionEntry {
                record_id: r.record_id.clone(),
                event: event_label(
                    SignalEventType::try_from(r.event).unwrap_or(SignalEventType::Created),
                )
                .to_owned(),
                timestamp_ms: r.timestamp,
                actor_did: (!r.actor_did.is_empty()).then(|| r.actor_did.clone()),
                close_reason,
                note,
            };
            (crate::fold::order_key(r), entry)
        })
        .collect();
    events.sort_by(|a, b| a.0.cmp(&b.0));
    events.into_iter().map(|(_, entry)| entry).collect()
}

/// The stable lowercase label for a folded status.
#[must_use]
pub fn status_label(status: &SignalStatus) -> &'static str {
    match status {
        SignalStatus::Open => "open",
        SignalStatus::Closed => "closed",
        SignalStatus::Tombstoned => "tombstoned",
    }
}

#[cfg(test)]
mod tests {
    use kutl_proto::sync::{FlagKind, FlagPayload, Hlc, ReplyPayload, Signal, SignalEventType};

    use super::*;

    /// A CREATED record with a flag payload of the given intent kind.
    fn flag(signal_id: &str, kind: FlagKind, document_id: Option<&str>, ms: u64) -> Signal {
        let mut s = Signal {
            id: signal_id.into(),
            space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
            record_id: format!("rec-{signal_id}"),
            author_did: "did:key:zAuthor".into(),
            document_id: document_id.map(str::to_owned),
            timestamp: ms.cast_signed(),
            hlc: Some(Hlc {
                physical_ms: ms,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            payload: Some(signal::Payload::Flag(FlagPayload {
                kind: i32::from(kind),
                message: format!("body of {signal_id}"),
                ..Default::default()
            })),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// A CREATED record with a reply payload.
    fn reply(signal_id: &str, ms: u64) -> Signal {
        let mut s = flag(signal_id, FlagKind::Info, None, ms);
        s.payload = Some(signal::Payload::Reply(ReplyPayload {
            parent_signal_id: "parent".into(),
            body: "a reply".into(),
            ..Default::default()
        }));
        s
    }

    /// A CLOSED transition for `signal_id`.
    fn closed(signal_id: &str, ms: u64) -> Signal {
        let mut s = flag(signal_id, FlagKind::Info, None, ms);
        s.record_id = format!("rec-{signal_id}-closed");
        s.payload = None;
        s.set_event(SignalEventType::Closed);
        s
    }

    fn fold_of(records: Vec<Signal>) -> SpaceSignalState {
        let mut fold = SpaceSignalState::default();
        for r in records {
            fold.apply(r);
        }
        fold
    }

    /// The default listing is open-only, and a closed signal drops out of it
    /// while still being reachable via `Closed`/`All`.
    #[test]
    fn test_status_filter_partitions_open_and_closed() {
        let fold = fold_of(vec![
            flag("open-one", FlagKind::Info, None, 1),
            flag("shut", FlagKind::Info, None, 2),
            closed("shut", 3),
        ]);

        let ids = |status| -> Vec<String> {
            list(
                &fold,
                &SignalFilters {
                    status,
                    ..Default::default()
                },
            )
            .into_iter()
            .map(|s| s.id)
            .collect()
        };

        assert_eq!(ids(StatusFilter::Open), vec!["open-one"]);
        assert_eq!(ids(StatusFilter::Closed), vec!["shut"]);
        assert_eq!(ids(StatusFilter::All), vec!["open-one", "shut"]);
    }

    /// The two kind axes are independent: `kind` selects the record type,
    /// `flag_kind` the flag's intent. Setting `flag_kind` implies a flag, so a
    /// reply cannot satisfy it.
    #[test]
    fn test_kind_and_flag_kind_are_orthogonal() {
        let fold = fold_of(vec![
            flag("q", FlagKind::Question, None, 1),
            flag("i", FlagKind::Info, None, 2),
            reply("r", 3),
        ]);

        let by_kind = list(
            &fold,
            &SignalFilters {
                kind: Some(SignalKind::Reply),
                ..Default::default()
            },
        );
        assert_eq!(by_kind.len(), 1);
        assert_eq!(by_kind[0].kind, Some("reply"));

        let by_flag_kind = list(
            &fold,
            &SignalFilters {
                flag_kind: Some(i32::from(FlagKind::Question)),
                ..Default::default()
            },
        );
        assert_eq!(by_flag_kind.len(), 1);
        assert_eq!(by_flag_kind[0].id, "q");
    }

    /// A document filter narrows to signals attached to that document, and
    /// space-level signals (no document) are excluded rather than treated as
    /// matching everything.
    #[test]
    fn test_document_filter_excludes_space_level_signals() {
        let fold = fold_of(vec![
            flag("on-doc", FlagKind::Info, Some("doc-1"), 1),
            flag("space-level", FlagKind::Info, None, 2),
            flag("other-doc", FlagKind::Info, Some("doc-2"), 3),
        ]);
        let got = list(
            &fold,
            &SignalFilters {
                document_id: Some("doc-1"),
                ..Default::default()
            },
        );
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, "on-doc");
    }

    /// A tombstoned signal is invisible to every status filter, including
    /// `All`. It is a soft delete, so "everything" means everything a reader
    /// may still see.
    #[test]
    fn test_tombstoned_is_hidden_from_every_status() {
        let mut gone = flag("gone", FlagKind::Info, None, 2);
        gone.record_id = "rec-gone-tomb".into();
        gone.payload = None;
        gone.set_event(SignalEventType::Tombstoned);
        let fold = fold_of(vec![flag("gone", FlagKind::Info, None, 1), gone]);

        for status in [StatusFilter::Open, StatusFilter::Closed, StatusFilter::All] {
            assert!(
                list(
                    &fold,
                    &SignalFilters {
                        status,
                        ..Default::default()
                    }
                )
                .is_empty(),
                "{status:?} must not surface a tombstoned signal"
            );
        }
    }

    /// The summary carries the fields both surfaces render, including the
    /// close time, which comes off the fold's LWW close-state rather than
    /// being re-derived from the transition records.
    #[test]
    fn test_comment_anchor_edit_does_not_blank_the_summarized_message() {
        // A comment-anchor EDITED carries an EMPTY message — "not carried"
        // under the overlay rule — and the projections preserve the authored
        // body. The fold-backed summary must apply the SAME overlay: reading
        // the winning EDITED wholesale would blank the comment's body on the
        // CLI list and MCP list_signals the moment anyone edits the anchored
        // span.
        let mut fold = SpaceSignalState::default();
        let mut created = flag("c1", FlagKind::Comment, Some("doc-1"), 1);
        if let Some(signal::Payload::Flag(f)) = &mut created.payload {
            f.message = "the authored comment body".into();
            f.anchor_text = Some("the original span".into());
        }
        fold.apply(created);

        let mut edited = flag("c1", FlagKind::Comment, Some("doc-1"), 2);
        edited.record_id = "rec-edit".into();
        edited.set_event(SignalEventType::Edited);
        if let Some(signal::Payload::Flag(f)) = &mut edited.payload {
            f.message = String::new(); // not carried
            f.anchor_text = Some("the edited span".into());
        }
        fold.apply(edited);

        let state = fold.get("c1").expect("folded");
        let summary = summarize("c1", state);
        assert_eq!(
            summary.message.as_deref(),
            Some("the authored comment body"),
            "an anchor-only edit must not blank the summarized message"
        );
    }

    #[test]
    fn test_mention_message_edit_updates_the_summarized_message() {
        // The carried half of the overlay: a mention EDITED's non-empty
        // message replaces the summary's.
        let mut fold = SpaceSignalState::default();
        fold.apply(flag("m1", FlagKind::Question, Some("doc-1"), 1));
        let mut edited = flag("m1", FlagKind::Question, Some("doc-1"), 2);
        edited.record_id = "rec-edit".into();
        edited.set_event(SignalEventType::Edited);
        if let Some(signal::Payload::Flag(f)) = &mut edited.payload {
            f.message = "the reworded ask".into();
        }
        fold.apply(edited);

        let summary = summarize("m1", fold.get("m1").expect("folded"));
        assert_eq!(summary.message.as_deref(), Some("the reworded ask"));
    }

    #[test]
    fn test_summary_carries_close_time_from_the_fold() {
        let fold = fold_of(vec![
            flag("shut", FlagKind::Blocked, Some("doc-9"), 1),
            closed("shut", 42),
        ]);
        let got = list(
            &fold,
            &SignalFilters {
                status: StatusFilter::Closed,
                ..Default::default()
            },
        );
        assert_eq!(got.len(), 1);
        let s = &got[0];
        assert_eq!(s.status, "closed");
        assert_eq!(s.flag_kind, Some("blocked"));
        assert_eq!(s.document_id.as_deref(), Some("doc-9"));
        assert_eq!(s.closed_ms, Some(42));
        assert_eq!(s.author_did, "did:key:zAuthor");
    }

    /// A listing says who each signal is for. Without it a caller cannot tell
    /// a space-wide flag from one naming them personally, which is the
    /// difference between "someone should look at this" and "you should".
    /// Spelled the way the per-signal detail read spells it — an `audience`
    /// name plus a `target_did` — so one concept has one shape across the two
    /// reads.
    #[test]
    fn test_summary_carries_the_audience_and_target() {
        let addressed = {
            let mut f = flag("addressed", FlagKind::Question, None, 1);
            if let Some(signal::Payload::Flag(p)) = &mut f.payload {
                p.audience = Some(kutl_proto::vocab::participant_audience("did:key:zBob"));
            }
            f
        };
        let broadcast = {
            let mut f = flag("broadcast", FlagKind::Info, None, 2);
            if let Some(signal::Payload::Flag(p)) = &mut f.payload {
                p.audience = Some(kutl_proto::vocab::space_audience());
            }
            f
        };
        let fold = fold_of(vec![addressed, broadcast, reply("answered", 3)]);

        let summary_of = |id: &str| summarize(id, fold.get(id).expect("folded"));

        let addressed = summary_of("addressed");
        assert_eq!(addressed.audience, Some("participant"));
        assert_eq!(addressed.target_did.as_deref(), Some("did:key:zBob"));

        let broadcast = summary_of("broadcast");
        assert_eq!(broadcast.audience, Some("space"));
        assert_eq!(
            broadcast.target_did, None,
            "a broadcast names no one, so the target stays absent"
        );

        let answered = summary_of("answered");
        assert_eq!(
            (answered.audience, answered.target_did),
            (None, None),
            "only a flag carries an audience"
        );
    }

    /// Every kind label round-trips through `from_label`, so the MCP schema
    /// enum and the CLI's `--kind` values cannot drift apart from what the
    /// filter actually accepts.
    #[test]
    fn test_kind_labels_round_trip() {
        for kind in [
            SignalKind::Flag,
            SignalKind::Chat,
            SignalKind::Decision,
            SignalKind::Reply,
        ] {
            assert_eq!(SignalKind::from_label(kind.label()), Some(kind));
        }
        assert_eq!(SignalKind::from_label("transition"), None);
    }
}

#[cfg(test)]
mod transition_tests {
    use kutl_proto::sync::{
        CloseReason, FlagKind, FlagPayload, Hlc, Signal, SignalEventType, TransitionPayload,
    };

    use super::*;

    /// A record for `signal_id` at `ms`, of the given event.
    fn rec(signal_id: &str, record_id: &str, event: SignalEventType, ms: i64) -> Signal {
        let mut s = Signal {
            id: signal_id.into(),
            record_id: record_id.into(),
            actor_did: "did:key:zActor".into(),
            timestamp: ms,
            hlc: Some(Hlc {
                physical_ms: ms.cast_unsigned(),
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        s.set_event(event);
        s
    }

    /// The trail is every record for the signal, oldest first — including
    /// transitions the fold's LWW discarded, which is the whole difference
    /// between "what happened" and "what is true now".
    #[test]
    fn test_history_keeps_every_record_in_order() {
        let records = vec![
            rec("a", "r3", SignalEventType::Reopened, 30),
            rec("a", "r1", SignalEventType::Created, 10),
            rec("a", "r2", SignalEventType::Closed, 20),
            rec("other", "r9", SignalEventType::Created, 15),
        ];
        let trail = transition_history("a", &records);
        assert_eq!(
            trail.iter().map(|t| t.event.as_str()).collect::<Vec<_>>(),
            vec!["created", "closed", "reopened"],
            "oldest first, and another signal's records are excluded"
        );
    }

    /// Two transitions inside ONE wall-clock millisecond order by the HLC's
    /// logical counter, not by wall time or record id. This is the case a
    /// timestamp sort gets backwards — it would show the reopen before the
    /// close it undid, because the tiebreak (a random record id) has nothing to
    /// do with causality.
    #[test]
    fn test_history_orders_same_millisecond_transitions_causally() {
        let mut closed = rec("a", "rZ", SignalEventType::Closed, 10);
        let mut reopened = rec("a", "rA", SignalEventType::Reopened, 10);
        // Same physical ms; the logical counter is what separates them. Record
        // ids are chosen so a lexicographic tiebreak would invert the pair.
        closed.hlc.as_mut().unwrap().logical = 1;
        reopened.hlc.as_mut().unwrap().logical = 2;

        let trail = transition_history("a", &[reopened, closed]);
        assert_eq!(
            trail.iter().map(|t| t.event.as_str()).collect::<Vec<_>>(),
            vec!["closed", "reopened"],
            "the HLC's logical counter must decide, not the record id"
        );
    }

    /// A close carries its reason and note; a create carries neither, and an
    /// `UNSPECIFIED` reason reports as absent rather than as a reason named
    /// "unspecified".
    #[test]
    fn test_history_surfaces_close_reason_and_note() {
        let mut created = rec("a", "r1", SignalEventType::Created, 10);
        created.payload = Some(signal::Payload::Flag(FlagPayload {
            kind: i32::from(FlagKind::Question),
            ..Default::default()
        }));
        let mut closed = rec("a", "r2", SignalEventType::Closed, 20);
        closed.set_close_reason(CloseReason::Declined);
        closed.payload = Some(signal::Payload::Transition(TransitionPayload {
            note: "not doing this".into(),
        }));

        let trail = transition_history("a", &[created, closed]);
        assert_eq!(trail[0].close_reason, None, "a create carries no reason");
        assert_eq!(trail[0].note, None);
        assert_eq!(trail[1].close_reason.as_deref(), Some("declined"));
        assert_eq!(trail[1].note.as_deref(), Some("not doing this"));
    }

    /// A legacy bare broadcast (empty `record_id`) is not a durable event and
    /// is skipped — the same rule the fold applies.
    #[test]
    fn test_history_skips_recordless_broadcasts() {
        let records = vec![
            rec("a", "", SignalEventType::Created, 5),
            rec("a", "r1", SignalEventType::Created, 10),
        ];
        assert_eq!(transition_history("a", &records).len(), 1);
    }
}
