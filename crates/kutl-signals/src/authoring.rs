//! Shared, pure signal-record assembly: intent -> unsigned Signal.
//!
//! A pure seam that turns an authoring [`SignalIntent`] into an UNSIGNED,
//! un-attested [`Signal`] record. It performs NO signing and NO IO — the relay
//! attests (`observed_at`) after assembly. Every relay-mint path
//! (create/transition/backfill) assembles through this one seam so the paths
//! cannot drift on the record shape. Relay-mint is the only authoring path —
//! clients never sign records.

use kutl_proto::sync::{
    Audience, CloseReason, DecisionPayload, FlagKind, FlagPayload, Hlc, ReplyPayload, Signal,
    SignalEventType, TransitionPayload, audience, signal,
};

/// Maximum length, in characters, of user-authored body text on a record — a
/// flag or comment `message`, a reply `body`, a transition `note`.
///
/// The hosted browser surface enforces the same bound before submitting, so
/// the two must agree — raising this bound means raising every enforcement
/// point at once. Counted in `char`s rather than bytes: JavaScript's `.length`
/// counts UTF-16 code units, so a char count is never *stricter* than a check
/// running in front of it, and text the browser accepted is never rejected
/// here.
pub const MAX_BODY_CHARS: usize = 4_000;

/// Why a per-kind builder refused to produce an intent.
///
/// Every variant is a caller mistake, never an internal fault — a door maps
/// these to a client-facing argument error, never to an internal one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IntentError {
    /// No audience at all: `Audience.to` unset.
    #[error("audience is required")]
    AudienceMissing,
    /// A participant audience naming nobody — the dead end the typed audience
    /// exists to remove, and still representable because `to` *is* set.
    #[error("participant audience requires a non-empty did")]
    ParticipantDidEmpty,
    /// A comment was authored through the flag builder, which has no
    /// `anchor_text` parameter to give it.
    #[error("a comment is authored through the comment builder, which takes its anchor text")]
    CommentAsFlagKind,
    /// A required text field was empty.
    #[error("{field} is required")]
    Required {
        /// The field name, as the caller's surface spells it.
        field: &'static str,
    },
    /// A text field exceeded [`MAX_BODY_CHARS`].
    #[error("{field} must be at most {max} characters, got {len}")]
    TooLong {
        /// The field name, as the caller's surface spells it.
        field: &'static str,
        /// The bound that was exceeded.
        max: usize,
        /// The length supplied.
        len: usize,
    },
    /// A transition builder was handed `CREATED` or `UNSPECIFIED`.
    #[error("event {event} is not a transition; created records use a per-kind builder")]
    NotATransition {
        /// The offending `SignalEventType` discriminant.
        event: i32,
    },
}

/// The plumbing every authored record carries, whatever its kind.
///
/// Its fields are public, unlike [`SignalIntent`]'s, and the asymmetry is the
/// point: this struct carries no shape rules to enforce — it is addressing and a
/// clock reading. All the legality lives in the per-kind builders, so there is
/// nothing here for a caller to get wrong by constructing it directly.
pub struct RecordEnvelope {
    /// The space the record belongs to.
    pub space_id: String,
    /// The document the signal is attached to, if any.
    pub document_id: Option<String>,
    /// The signal id this record belongs to. For a CREATED record the caller
    /// mints a fresh signal id; for a transition it is the target signal.
    pub signal_id: String,
    /// The wall-clock event time (Unix millis) the record carries. The relay
    /// fold reads it as the signal's creation time (CREATED) or its close time
    /// (CLOSED transition), so it rides in the signed canonical bytes. The
    /// relay-mint paths stamp the observed wall-ms here; legacy client-signed
    /// records carry `0` and derive their creation
    /// time from the HLC instead.
    pub timestamp: i64,
}

/// The caller's authoring intent, independent of who signs or how it ships.
///
/// # There is no way to build one but a per-kind builder
///
/// Every field is private, so a new authoring door cannot hand-roll an intent —
/// it must go through [`SignalIntent::flag`], [`SignalIntent::comment`],
/// [`SignalIntent::decision`], [`SignalIntent::reply`] or
/// [`SignalIntent::transition`], each of which enforces its kind's shape
/// rules. Adding a fifth authored kind means
/// editing this file, which surfaces in review as a diff to the one place that
/// says so.
///
/// Struct-literal construction does not compile:
///
/// ```compile_fail,E0451
/// use kutl_proto::sync::SignalEventType;
/// use kutl_signals::authoring::SignalIntent;
///
/// let _ = SignalIntent {
///     space_id: "94b97b29-5a98-4a13-81e0-f766d259cd39".to_owned(),
///     document_id: None,
///     event: SignalEventType::Created,
///     close_reason: None,
///     signal_id: "sig1".to_owned(),
///     timestamp: 0,
///     payload: None,
/// };
/// ```
///
/// The same intent through its builder does — the positive control, so the
/// `compile_fail` case above cannot pass merely because the type path is wrong:
///
/// ```
/// use kutl_proto::sync::{AudienceType, FlagKind};
/// use kutl_signals::authoring::{RecordEnvelope, SignalIntent};
///
/// let intent = SignalIntent::flag(
///     RecordEnvelope {
///         space_id: "94b97b29-5a98-4a13-81e0-f766d259cd39".to_owned(),
///         document_id: None,
///         signal_id: "sig1".to_owned(),
///         timestamp: 0,
///     },
///     FlagKind::Info,
///     "needs review".to_owned(),
///     kutl_proto::vocab::audience_from_untyped(i32::from(AudienceType::Space), None),
/// )
/// .expect("a well-formed flag intent");
/// # let _ = intent;
/// ```
#[derive(Debug)]
pub struct SignalIntent {
    /// The space the record belongs to.
    space_id: String,
    /// The document the signal is attached to, if any.
    document_id: Option<String>,
    /// Which lifecycle event this record carries.
    event: SignalEventType,
    /// The close reason — meaningful on `CLOSED` only.
    close_reason: Option<CloseReason>,
    /// The signal id this record belongs to.
    signal_id: String,
    /// The wall-clock event time (Unix millis) — see [`RecordEnvelope::timestamp`].
    timestamp: i64,
    /// The payload oneof. `None` for a transition carrying no note.
    payload: Option<signal::Payload>,
}

impl SignalIntent {
    /// A flag: an urgency or action signal addressed to a space or a participant.
    ///
    /// Rejects `FLAG_KIND_COMMENT`, which has no `anchor_text` parameter here —
    /// use [`SignalIntent::comment`]. Same wire shape, deliberate entrance.
    pub fn flag(
        env: RecordEnvelope,
        kind: FlagKind,
        message: String,
        audience: Audience,
    ) -> Result<Self, IntentError> {
        if kind == FlagKind::Comment {
            return Err(IntentError::CommentAsFlagKind);
        }
        check_audience(&audience)?;
        check_body("message", &message)?;
        Ok(Self::created(
            env,
            signal::Payload::Flag(flag_payload(kind, message, audience)),
        ))
    }

    /// A comment: an observation anchored to a document range or to the whole
    /// document. `anchor_text` is the wrapped passage, absent for a
    /// document-level comment and for a source system that supplied none.
    ///
    /// Produces a `FLAG_KIND_COMMENT` **flag** — a comment is a flag kind, not a
    /// signal type of its own. It keeps a builder of its own
    /// anyway, for two reasons: it is the only kind that carries `anchor_text`, so
    /// [`SignalIntent::flag`] has nowhere to put one; and authoring a comment
    /// should be a deliberate call rather than something a caller falls into by
    /// passing `kind: comment`.
    pub fn comment(
        env: RecordEnvelope,
        message: String,
        audience: Audience,
        anchor_text: Option<String>,
    ) -> Result<Self, IntentError> {
        check_audience(&audience)?;
        check_body("message", &message)?;
        let mut payload = flag_payload(FlagKind::Comment, message, audience);
        payload.anchor_text = anchor_text;
        Ok(Self::created(env, signal::Payload::Flag(payload)))
    }

    /// A decision, materialized from a document heading or callout. Its audience
    /// is space-wide by definition — the document's ACL is the
    /// boundary — so it takes none.
    pub fn decision(
        env: RecordEnvelope,
        title_hash: String,
        title: String,
        depth: i32,
        parent_hash: Option<String>,
    ) -> Result<Self, IntentError> {
        if title_hash.is_empty() {
            return Err(IntentError::Required {
                field: "title_hash",
            });
        }
        check_body("title", &title)?;
        Ok(Self::created(
            env,
            signal::Payload::Decision(DecisionPayload {
                title_hash,
                title,
                depth,
                parent_hash,
            }),
        ))
    }

    /// A reply to a signal, or to another reply. Its audience is inherited from
    /// the parent transitively, so it takes none.
    pub fn reply(
        env: RecordEnvelope,
        parent_signal_id: String,
        parent_reply_id: Option<String>,
        body: String,
    ) -> Result<Self, IntentError> {
        if parent_signal_id.is_empty() {
            return Err(IntentError::Required {
                field: "parent_signal_id",
            });
        }
        check_body("body", &body)?;
        Ok(Self::created(
            env,
            signal::Payload::Reply(ReplyPayload {
                parent_signal_id,
                parent_reply_id,
                body,
            }),
        ))
    }

    /// A lifecycle transition: close, reopen or tombstone an existing signal.
    ///
    /// The `note` rides in the record as a [`TransitionPayload`]
    /// rather than an observer side-channel, so it survives a rebuild.
    /// **A transition with no note carries no payload at all** — not an empty
    /// one — so its canonical bytes are byte-identical to legacy note-less
    /// transition records already on disk.
    pub fn transition(
        env: RecordEnvelope,
        event: SignalEventType,
        close_reason: Option<CloseReason>,
        note: Option<String>,
    ) -> Result<Self, IntentError> {
        match event {
            SignalEventType::Closed | SignalEventType::Reopened | SignalEventType::Tombstoned => {}
            // EDITED is content, not lifecycle, and is materializer-minted
            // — no authoring path builds one here.
            SignalEventType::Created | SignalEventType::Unspecified | SignalEventType::Edited => {
                return Err(IntentError::NotATransition {
                    event: i32::from(event),
                });
            }
        }
        let payload = match note {
            Some(note) if !note.is_empty() => {
                check_body("note", &note)?;
                Some(signal::Payload::Transition(TransitionPayload { note }))
            }
            _ => None,
        };
        Ok(Self {
            space_id: env.space_id,
            document_id: env.document_id,
            event,
            close_reason,
            signal_id: env.signal_id,
            timestamp: env.timestamp,
            payload,
        })
    }

    /// Re-materialize a record for an **existing** signal, verbatim. Recovery,
    /// **not authoring** — do not reach for this from a door.
    ///
    /// The crash-tail backfill mints records for projection rows that were acked
    /// to a user but whose record never landed. Those rows predate
    /// every rule the per-kind builders enforce, so putting them through a builder
    /// would *reject* acked user data — a legacy comment carries
    /// `FLAG_KIND_COMMENT`, an old row may carry an empty message or an audience
    /// that no builder would accept today. So this validates nothing and takes the
    /// payload as-is.
    ///
    /// It is deliberately not a fifth authoring door: it names an existing signal
    /// rather than creating one, and it lives here — beside the builders, inside
    /// the façade — so adding a *real* door still means editing this file and
    /// showing up in review. If you are adding an authoring path and this looks
    /// convenient, it is the wrong function.
    #[must_use]
    pub fn rematerialize(env: RecordEnvelope, payload: Option<signal::Payload>) -> Self {
        Self {
            space_id: env.space_id,
            document_id: env.document_id,
            event: SignalEventType::Created,
            close_reason: None,
            signal_id: env.signal_id,
            timestamp: env.timestamp,
            payload,
        }
    }

    /// Shared tail for the four CREATED kinds.
    fn created(env: RecordEnvelope, payload: signal::Payload) -> Self {
        Self {
            space_id: env.space_id,
            document_id: env.document_id,
            event: SignalEventType::Created,
            close_reason: None,
            signal_id: env.signal_id,
            timestamp: env.timestamp,
            payload: Some(payload),
        }
    }
}

/// Build a `FlagPayload` carrying only the typed audience.
///
/// The deprecated `audience_type` / `target_did` pair is left unset, which is
/// what makes "a writer MUST NOT set both" true by construction
/// here rather than by a check — no authored flag can reach the both-set state
/// the seam rejects.
fn flag_payload(kind: FlagKind, message: String, audience: Audience) -> FlagPayload {
    let mut payload = FlagPayload {
        message,
        audience: Some(audience),
        anchor_text: None,
        ..Default::default()
    };
    payload.set_kind(kind);
    payload
}

/// An audience must name someone: `to` set, and a participant's DID non-empty.
///
/// Reads the proto shape directly rather than reducing to the untyped pair — the
/// two malformed cases are distinguishable here and collapse there.
fn check_audience(audience: &Audience) -> Result<(), IntentError> {
    match &audience.to {
        None => Err(IntentError::AudienceMissing),
        Some(audience::To::Participant(p)) if p.did.is_empty() => {
            Err(IntentError::ParticipantDidEmpty)
        }
        Some(audience::To::Participant(_) | audience::To::Space(_)) => Ok(()),
    }
}

/// A body field must be non-empty and within [`MAX_BODY_CHARS`].
fn check_body(field: &'static str, text: &str) -> Result<(), IntentError> {
    if text.is_empty() {
        return Err(IntentError::Required { field });
    }
    let len = text.chars().count();
    if len > MAX_BODY_CHARS {
        return Err(IntentError::TooLong {
            field,
            max: MAX_BODY_CHARS,
            len,
        });
    }
    Ok(())
}

/// Assemble an UNSIGNED, un-attested [`Signal`] record from an intent. Mints a
/// fresh `record_id` (v4 UUID); the relay attests after assembly.
///
/// Sets `id`/`space_id`/`document_id` from the intent, `actor_did` +
/// `author_did` to `actor_did`, `event`/`hlc`/`timestamp`, `close_reason` only
/// when the event is `CLOSED`, and the payload oneof. Leaves `signature` empty
/// (relay-minted records are relay-vouched, not member-signed) and
/// `attestation` `None` (the relay fills it in at ingest).
#[must_use]
pub fn assemble_record(intent: &SignalIntent, actor_did: &str, hlc: Hlc) -> Signal {
    let mut record = Signal {
        id: intent.signal_id.clone(),
        space_id: intent.space_id.clone(),
        document_id: intent.document_id.clone(),
        author_did: actor_did.to_owned(),
        actor_did: actor_did.to_owned(),
        record_id: uuid::Uuid::new_v4().to_string(),
        timestamp: intent.timestamp,
        hlc: Some(hlc),
        payload: intent.payload.clone(),
        ..Default::default()
    };
    record.set_event(intent.event);
    // The close reason is meaningful on CLOSED only (matches the fold + the
    // relay-mint transition path).
    if intent.event == SignalEventType::Closed
        && let Some(reason) = intent.close_reason
    {
        record.set_close_reason(reason);
    }
    record
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_proto::sync::{AudienceType, Hlc};

    /// A canonical 16-byte actor for an intent HLC.
    const ACTOR_16: [u8; 16] = [0xcd; 16];

    fn env() -> RecordEnvelope {
        RecordEnvelope {
            space_id: "065ae81c-c74c-429c-af66-163df73991b4".into(),
            document_id: Some("7c1d2e3f-4444-4a3e-9c60-444444444444".into()),
            signal_id: "5e0f7bb2-6db5-4a3e-9c60-111111111111".into(),
            timestamp: 1_752_400_000_000,
        }
    }

    fn space_audience() -> Audience {
        kutl_proto::vocab::audience_from_untyped(i32::from(AudienceType::Space), None)
    }

    /// The flag builder emits ONLY the typed audience, never the deprecated pair.
    /// "A writer must not set both" is therefore true by construction here rather
    /// than by a check the seam has to make.
    #[test]
    fn test_flag_builder_sets_typed_audience_and_not_the_pair() {
        let intent = SignalIntent::flag(
            env(),
            FlagKind::Question,
            "why".into(),
            kutl_proto::vocab::audience_from_untyped(
                i32::from(AudienceType::Participant),
                Some("did:key:zBob"),
            ),
        )
        .expect("well-formed");
        let Some(signal::Payload::Flag(flag)) = &intent.payload else {
            panic!("expected a flag payload");
        };
        assert!(flag.audience.is_some(), "the typed audience is set");
        #[allow(deprecated)]
        {
            assert_eq!(
                flag.audience_type,
                i32::from(AudienceType::Unspecified),
                "the deprecated audience_type must stay unset"
            );
            assert!(
                flag.target_did.is_none(),
                "the deprecated target_did must stay unset"
            );
        }
        assert_eq!(flag.kind(), FlagKind::Question);
    }

    /// A comment cannot be smuggled through the flag builder as the retired
    /// `FLAG_KIND_COMMENT` — that is what makes the comment builder the only way
    /// to author one.
    #[test]
    fn test_flag_builder_rejects_the_retired_comment_kind() {
        let err = SignalIntent::flag(env(), FlagKind::Comment, "body".into(), space_audience())
            .expect_err("comment-as-flag-kind must be refused");
        assert_eq!(err, IntentError::CommentAsFlagKind);
    }

    /// Both malformed audiences are refused, and they are refused distinguishably
    /// — `to` unset is a different caller mistake from a participant naming nobody.
    #[test]
    fn test_builders_reject_malformed_audiences() {
        let err = SignalIntent::flag(env(), FlagKind::Info, "m".into(), Audience { to: None })
            .expect_err("an audience with `to` unset must be refused");
        assert_eq!(err, IntentError::AudienceMissing);

        let empty_did = kutl_proto::vocab::participant_audience(String::new());
        let err = SignalIntent::comment(env(), "m".into(), empty_did, None)
            .expect_err("a participant naming nobody must be refused");
        assert_eq!(err, IntentError::ParticipantDidEmpty);
    }

    /// The body bound is enforced in chars, and an empty body is a distinct error
    /// from an over-long one. Named per field so the caller's message points at the
    /// argument they actually passed.
    #[test]
    fn test_body_bound_and_emptiness() {
        let err = SignalIntent::flag(env(), FlagKind::Info, String::new(), space_audience())
            .expect_err("empty message must be refused");
        assert_eq!(err, IntentError::Required { field: "message" });

        // Multi-byte chars: the bound counts characters, so this is exactly at the
        // limit even though it is three times as many bytes.
        let at_limit = "é".repeat(MAX_BODY_CHARS);
        SignalIntent::flag(env(), FlagKind::Info, at_limit, space_audience())
            .expect("exactly at the limit is allowed");

        let over = "é".repeat(MAX_BODY_CHARS + 1);
        let err = SignalIntent::flag(env(), FlagKind::Info, over, space_audience())
            .expect_err("over the limit must be refused");
        assert_eq!(
            err,
            IntentError::TooLong {
                field: "message",
                max: MAX_BODY_CHARS,
                len: MAX_BODY_CHARS + 1,
            }
        );

        let err = SignalIntent::reply(env(), "parent".into(), None, String::new())
            .expect_err("empty reply body must be refused");
        assert_eq!(err, IntentError::Required { field: "body" });
    }

    /// A transition builder refuses CREATED, UNSPECIFIED and EDITED, so a
    /// transition path can mint neither a create nor a content edit (EDITED
    /// is materializer-minted).
    #[test]
    fn test_transition_builder_rejects_non_transition_events() {
        for event in [
            SignalEventType::Created,
            SignalEventType::Unspecified,
            SignalEventType::Edited,
        ] {
            let err = SignalIntent::transition(env(), event, None, None)
                .expect_err("a non-transition event must be refused");
            assert_eq!(
                err,
                IntentError::NotATransition {
                    event: i32::from(event)
                }
            );
        }
        for event in [
            SignalEventType::Closed,
            SignalEventType::Reopened,
            SignalEventType::Tombstoned,
        ] {
            SignalIntent::transition(env(), event, None, None).expect("a transition is allowed");
        }
    }

    /// A note-less transition carries NO payload — not an empty one — so its
    /// canonical bytes match a legacy note-less transition record. A supplied
    /// note rides in a `TransitionPayload`.
    #[test]
    fn test_transition_note_payload_presence() {
        let bare = SignalIntent::transition(env(), SignalEventType::Closed, None, None)
            .expect("well-formed");
        assert!(
            bare.payload.is_none(),
            "a note-less transition must carry no payload at all"
        );

        // An empty note is the same as no note, not an empty payload.
        let empty =
            SignalIntent::transition(env(), SignalEventType::Closed, None, Some(String::new()))
                .expect("well-formed");
        assert!(empty.payload.is_none(), "an empty note carries no payload");

        let noted = SignalIntent::transition(
            env(),
            SignalEventType::Closed,
            None,
            Some("superseded by the new plan".into()),
        )
        .expect("well-formed");
        let Some(signal::Payload::Transition(t)) = &noted.payload else {
            panic!("expected a transition payload");
        };
        assert_eq!(t.note, "superseded by the new plan");
    }

    /// `rematerialize` validates nothing, by design: the crash-tail backfill mints
    /// records for rows that predate every builder rule, and rejecting them would
    /// drop acked user data.
    #[test]
    fn test_rematerialize_accepts_shapes_the_builders_refuse() {
        #[allow(deprecated)]
        let legacy_comment = signal::Payload::Flag(FlagPayload {
            kind: i32::from(FlagKind::Comment),
            audience_type: i32::from(AudienceType::Participant),
            target_did: Some(String::new()),
            message: String::new(),
            audience: None,
            anchor_text: None,
        });
        let intent = SignalIntent::rematerialize(env(), Some(legacy_comment));
        let Some(signal::Payload::Flag(flag)) = &intent.payload else {
            panic!("expected the payload through verbatim");
        };
        #[allow(deprecated)]
        {
            assert_eq!(flag.kind, i32::from(FlagKind::Comment));
            assert!(flag.message.is_empty());
        }
    }

    fn test_hlc() -> Hlc {
        Hlc {
            physical_ms: 1_752_400_000_123,
            logical: 3,
            actor: ACTOR_16.to_vec(),
        }
    }

    /// A CREATED intent yields a well-formed unsigned record: a fresh v4
    /// `record_id`, the given `actor_did` (also mirrored to `author_did`), the
    /// intent's `event`/`hlc`/`space_id`/`document_id`/payload, an empty
    /// `signature`, and no `attestation`.
    #[test]
    fn test_assemble_record_created_is_well_formed_and_unsigned() {
        let intent = SignalIntent {
            space_id: "065ae81c-c74c-429c-af66-163df73991b4".into(),
            document_id: Some("7c1d2e3f-4444-4a3e-9c60-444444444444".into()),
            event: SignalEventType::Created,
            close_reason: None,
            signal_id: "5e0f7bb2-6db5-4a3e-9c60-111111111111".into(),
            timestamp: 1_752_400_000_000,
            payload: Some(signal::Payload::Flag(FlagPayload {
                message: "needs review".into(),
                ..Default::default()
            })),
        };
        let record = assemble_record(&intent, "did:key:zActor", test_hlc());

        assert_eq!(record.id, intent.signal_id);
        assert_eq!(record.space_id, intent.space_id);
        assert_eq!(record.document_id, intent.document_id);
        assert_eq!(record.actor_did, "did:key:zActor");
        assert_eq!(
            record.author_did, "did:key:zActor",
            "author_did mirrors actor_did on assembly"
        );
        assert_eq!(record.event(), SignalEventType::Created);
        assert_eq!(
            record.timestamp, intent.timestamp,
            "the intent timestamp rides into the record (signed canonical bytes)"
        );
        assert_eq!(record.hlc, Some(test_hlc()));
        assert!(
            matches!(record.payload, Some(signal::Payload::Flag(_))),
            "the CREATED payload rides through"
        );

        // Unsigned + un-attested: the caller and relay fill these later.
        assert!(record.signature.is_empty(), "signature must be empty");
        assert!(record.attestation.is_none(), "attestation must be None");

        // A fresh, non-empty v4 record_id.
        assert!(!record.record_id.is_empty(), "record_id must be minted");
        let parsed = uuid::Uuid::parse_str(&record.record_id).expect("record_id is a valid UUID");
        assert_eq!(parsed.get_version_num(), 4, "record_id must be a v4 UUID");
    }

    /// A CLOSED transition intent carries the close reason and no payload.
    #[test]
    fn test_assemble_record_closed_sets_reason_no_payload() {
        let intent = SignalIntent {
            space_id: "94b97b29-5a98-4a13-81e0-f766d259cd39".into(),
            document_id: None,
            event: SignalEventType::Closed,
            close_reason: Some(CloseReason::Resolved),
            signal_id: "sig-1".into(),
            timestamp: 0,
            payload: None,
        };
        let record = assemble_record(&intent, "did:key:zActor", test_hlc());

        assert_eq!(record.event(), SignalEventType::Closed);
        assert_eq!(record.close_reason(), CloseReason::Resolved);
        assert!(record.payload.is_none(), "a transition carries no payload");
        assert!(record.document_id.is_none());
    }

    /// A REOPENED transition carries no close reason even if the intent
    /// mistakenly supplies one — the reason is meaningful on CLOSED only.
    #[test]
    fn test_assemble_record_reopened_has_no_close_reason() {
        let intent = SignalIntent {
            space_id: "94b97b29-5a98-4a13-81e0-f766d259cd39".into(),
            document_id: None,
            event: SignalEventType::Reopened,
            close_reason: Some(CloseReason::Resolved),
            signal_id: "sig-1".into(),
            timestamp: 0,
            payload: None,
        };
        let record = assemble_record(&intent, "did:key:zActor", test_hlc());

        assert_eq!(record.event(), SignalEventType::Reopened);
        assert_eq!(
            record.close_reason(),
            CloseReason::Unspecified,
            "a REOPENED record must not carry a close reason"
        );
    }

    /// The assembled record passes the relay-side well-formedness shape it will
    /// be admitted against: a non-empty `record_id` and a 16-byte-actor HLC.
    /// (Mirrors `kutl_relay::signal_record::validate_record`'s two structural
    /// checks so an assembled record is admissible without a re-stamp.)
    #[test]
    fn test_assemble_record_passes_wellformedness_shape() {
        let intent = SignalIntent {
            space_id: "94b97b29-5a98-4a13-81e0-f766d259cd39".into(),
            document_id: None,
            event: SignalEventType::Closed,
            close_reason: Some(CloseReason::Withdrawn),
            signal_id: "sig-1".into(),
            timestamp: 0,
            payload: None,
        };
        let record = assemble_record(&intent, "did:key:zActor", test_hlc());

        assert!(!record.record_id.is_empty());
        let hlc = record.hlc.as_ref().expect("hlc set");
        assert_eq!(hlc.actor.len(), 16, "hlc actor must be 16 bytes");
    }

    /// Two assemblies mint different `record_id`s (UUID v4, non-deterministic).
    #[test]
    fn test_assemble_record_mints_unique_ids() {
        let intent = SignalIntent {
            space_id: "94b97b29-5a98-4a13-81e0-f766d259cd39".into(),
            document_id: None,
            event: SignalEventType::Created,
            close_reason: None,
            signal_id: "sig-1".into(),
            timestamp: 0,
            payload: None,
        };
        let a = assemble_record(&intent, "did:key:zActor", test_hlc());
        let b = assemble_record(&intent, "did:key:zActor", test_hlc());
        assert_ne!(a.record_id, b.record_id);
    }
}
