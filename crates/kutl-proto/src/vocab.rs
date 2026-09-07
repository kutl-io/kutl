//! Canonical string labels for the `FlagKind` and `AudienceType` proto enums,
//! and the resolvers for the typed `Audience` that replaces the latter.
//!
//! The single source of truth shared by the CLI (`signal create`/`list`), the
//! relay MCP tools, and every `ChangeBackend`, so the flag/audience vocabulary
//! never drifts between surfaces. Discriminants are referenced via
//! the generated proto enums, never bare literals, so a `.proto` renumber can
//! never silently desync these labels.
//!
//! `audience_type_*` speak the deprecated wire pair and stay for as long as
//! legacy records and the Connect surface do. The string mappers cannot express
//! the typed shape, because `participant` carries a DID.
//!
//! Every audience read applies one precedence rule — typed field when
//! set, deprecated pair otherwise — and that rule lives in
//! [`flag_audience_untyped`] alone. Which accessor to use depends on the question,
//! and the split is deliberate:
//!
//! | Question | Accessor |
//! |---|---|
//! | Who should this reach? | [`flag_audience_delivery`] |
//! | What goes in the projection's `(audience, target_did)` columns? | [`flag_audience_untyped`] |
//! | Convert a stored pair to/from the typed [`Audience`] | [`audience_from_untyped`] / [`audience_to_untyped`] |
//!
//! Storage widens a malformed or retired audience to space-wide so a row always
//! says something; delivery must not, because widening there notifies everybody.
//! Using the storage converter for a delivery decision is the specific bug this
//! layout exists to prevent.

use crate::sync::{Audience, AudienceType, FlagKind, FlagPayload, audience};

/// Map a `FlagKind` i32 discriminant to its canonical lowercase name.
/// `0` (unspecified) and `1` both map to `"info"`.
#[must_use]
pub fn flag_kind_to_str(kind: i32) -> &'static str {
    match FlagKind::try_from(kind) {
        Ok(FlagKind::Completed) => "completed",
        Ok(FlagKind::ReviewRequested) => "review_requested",
        Ok(FlagKind::Question) => "question",
        Ok(FlagKind::Blocked) => "blocked",
        Ok(FlagKind::Comment) => "comment",
        // Info, Unspecified, or an unknown discriminant → "info".
        _ => "info",
    }
}

/// Parse a canonical flag-kind name to its `FlagKind` i32 discriminant.
#[must_use]
pub fn flag_kind_from_str(s: &str) -> Option<i32> {
    let kind = match s {
        "info" => FlagKind::Info,
        "completed" => FlagKind::Completed,
        "review_requested" => FlagKind::ReviewRequested,
        "question" => FlagKind::Question,
        "blocked" => FlagKind::Blocked,
        "comment" => FlagKind::Comment,
        _ => return None,
    };
    Some(i32::from(kind))
}

/// Every flag kind, in the order surfaces should present them: the kinds that
/// ask something of a reader come before the ones that only report, so an agent
/// scanning the list meets `review_requested`/`question`/`blocked` first.
pub const FLAG_KINDS: &[FlagKind] = &[
    FlagKind::Info,
    FlagKind::ReviewRequested,
    FlagKind::Question,
    FlagKind::Blocked,
    FlagKind::Completed,
    FlagKind::Comment,
];

/// The kinds a surface may offer for AUTHORING — every kind but `comment`,
/// which is meaningless without an inline marker bound to a span of document
/// text and so gets its own verb everywhere (the editor's comment affordance,
/// MCP's `create_comment`). Filtering for existing comments is unrestricted;
/// only minting one through a general flag door is not.
pub const AUTHORABLE_FLAG_KINDS: &[FlagKind] = &[
    FlagKind::Info,
    FlagKind::ReviewRequested,
    FlagKind::Question,
    FlagKind::Blocked,
    FlagKind::Completed,
];

/// What kind of attention each flag kind asks for, in one lowercase clause.
///
/// The single source every surface renders — the CLI's `--kind`/`--flag-kind`
/// help and the MCP `create_flag` tool description — so an agent meets the same
/// sentence wherever it looks, and picking a kind is a decision about intent
/// rather than a guess at a bare name.
///
/// Exhaustive on purpose, with no catch-all: a kind added to the protocol fails
/// to compile here until someone says what it means. That failure is how the
/// surfaces stay complete; nothing checks their text.
#[must_use]
pub const fn flag_kind_guidance(kind: FlagKind) -> &'static str {
    match kind {
        // Unspecified reads back as `info` everywhere, so it means the same.
        FlagKind::Unspecified | FlagKind::Info => "informational; no action expected",
        FlagKind::Completed => "marks work finished",
        FlagKind::ReviewRequested => "please look at this and respond",
        FlagKind::Question => "an open question that needs an answer",
        FlagKind::Blocked => "something cannot proceed until this is resolved",
        FlagKind::Comment => "a remark anchored to a span of document text",
    }
}

/// The canonical names of `kinds` — for an error message that lists what was
/// accepted, or a JSON-schema `enum`.
#[must_use]
pub fn flag_kind_names(kinds: &[FlagKind]) -> Vec<&'static str> {
    kinds
        .iter()
        .map(|k| flag_kind_to_str(i32::from(*k)))
        .collect()
}

/// Map an `AudienceType` i32 discriminant to its canonical name.
#[must_use]
pub fn audience_type_to_str(audience: i32) -> &'static str {
    match AudienceType::try_from(audience) {
        Ok(AudienceType::Space) => "space",
        Ok(AudienceType::HumanOwners) => "human_owners",
        Ok(AudienceType::HumanEditors) => "human_editors",
        Ok(AudienceType::HumanViewers) => "human_viewers",
        Ok(AudienceType::AgentOwners) => "agent_owners",
        Ok(AudienceType::AgentEditors) => "agent_editors",
        Ok(AudienceType::AgentViewers) => "agent_viewers",
        // Participant, Unspecified, or an unknown discriminant → "participant".
        _ => "participant",
    }
}

/// Parse an audience name to its `AudienceType` i32 discriminant.
///
/// **For reading stored values, not for authoring** — see
/// [`authorable_audience_from_str`]. It still accepts all eight names because a
/// projection column or a legacy record may carry any of them, and a reader that
/// refused six of them would silently rewrite history.
#[must_use]
pub fn audience_type_from_str(s: &str) -> Option<i32> {
    let audience = match s {
        "participant" => AudienceType::Participant,
        "space" => AudienceType::Space,
        "human_owners" => AudienceType::HumanOwners,
        "human_editors" => AudienceType::HumanEditors,
        "human_viewers" => AudienceType::HumanViewers,
        "agent_owners" => AudienceType::AgentOwners,
        "agent_editors" => AudienceType::AgentEditors,
        "agent_viewers" => AudienceType::AgentViewers,
        _ => return None,
    };
    Some(i32::from(audience))
}

/// The `FlagKind` discriminant meaning "unspecified" — proto3's default, and the
/// value a reader falls back to when a stored kind is absent or unparseable.
/// Named so a bare `0` never stands in for an enum discriminant.
pub const FLAG_KIND_UNSPECIFIED: i32 = FlagKind::Unspecified as i32;

/// The `AudienceType` discriminant meaning "unspecified". See
/// [`FLAG_KIND_UNSPECIFIED`]; every resolver here treats it as space-wide,
/// because the alternative reading (participant with no target) reaches nobody.
pub const AUDIENCE_TYPE_UNSPECIFIED: i32 = AudienceType::Unspecified as i32;

/// Every audience name that can appear in STORED data, newest-shape first.
///
/// Not the set a caller may author — see [`AUTHORABLE_AUDIENCES`]. The six role
/// names remain here because rows and records already carry them.
pub const AUDIENCES: &[&str] = &[
    "participant",
    "space",
    "human_owners",
    "human_editors",
    "human_viewers",
    "agent_owners",
    "agent_editors",
    "agent_viewers",
];

/// The audience names a caller may AUTHOR, for `--help` and error messages.
///
/// Two, not eight. The six role audiences are retired: the typed
/// `Audience` has no arm for them, and shipping an arm while deferring the
/// filtering would let records be accepted meaning "space-wide" and silently
/// change meaning once filtering lands. The cutover is deliberately hard, with
/// no deprecation window — `--audience human_owners` fails rather
/// than warning.
pub const AUTHORABLE_AUDIENCES: &[&str] = &["participant", "space"];

/// Parse an audience name a caller may AUTHOR, rejecting the retired six.
///
/// The authoring counterpart to [`audience_type_from_str`], which still reads all
/// eight. Same storage-vs-authoring split as the audience accessors above: what a
/// reader must tolerate and what a writer may produce are different sets, and
/// conflating them is how a retired value creeps back onto the wire.
#[must_use]
pub fn authorable_audience_from_str(s: &str) -> Option<i32> {
    match s {
        "participant" => Some(i32::from(AudienceType::Participant)),
        "space" => Some(i32::from(AudienceType::Space)),
        _ => None,
    }
}

/// Resolve the legacy `(audience_type, target_did)` pair to a typed
/// [`Audience`].
///
/// **For storage, not delivery** — see [`flag_audience_delivery`] for that.
///
/// Only a `PARTICIPANT` carrying a non-empty DID becomes `Participant`; every
/// other input widens to `Space`, which is a repair rather than a reading. A
/// projection column has to say *something*, and `Space` is the choice that
/// over-notifies rather than losing a row's audience entirely. Four inputs widen:
///
/// - `UNSPECIFIED`, which [`audience_type_to_str`] renders as `"participant"`
///   with no target — an audience that delivers to nobody.
/// - `PARTICIPANT` with an empty or absent target, the same dead end reached from
///   the other direction.
/// - The six retired role audiences, which have no typed arm.
/// - An unknown discriminant, which arrives on the replication path.
///
/// # Why this must not decide delivery
///
/// The widening destroys the distinction a delivery surface needs: the two dead
/// ends address nobody, and folding them into `Space` turns an unaddressed flag
/// into one everybody is notified about. [`flag_audience_delivery`] returns them
/// as `Undeliverable`, which is why a delivery caller uses it and never this.
/// (The retired role audiences widen the same way in both — they cut over to
/// space-wide outright.)
#[must_use]
pub fn audience_from_untyped(audience_type: i32, target_did: Option<&str>) -> Audience {
    // Storage and delivery share one classification and differ only in what they
    // do with `Undeliverable` — here it widens to `Space`, there it is its own
    // answer. Expressing the divergence as this two-arm match keeps it at one
    // visible site instead of two enum walks that have to be kept in agreement.
    let to = match classify_audience(audience_type, target_did) {
        AudienceDelivery::Participant(did) => audience::To::Participant(audience::Participant {
            did: did.to_owned(),
        }),
        AudienceDelivery::Space | AudienceDelivery::Undeliverable => {
            audience::To::Space(audience::Space {})
        }
    };
    Audience { to: Some(to) }
}

/// The space-wide audience: this signal is a broadcast to every member.
///
/// A constructor rather than a literal because `Audience { to: Some(To::Space(
/// Space {})) }` is three levels of wrapping around no data, written out at two
/// dozen sites. The nesting is prost's, not ours, and spelling it by hand at
/// each site is how one of them ends up building `Audience { to: None }` —
/// which is not "space-wide", it is "no audience at all".
#[must_use]
pub fn space_audience() -> Audience {
    Audience {
        to: Some(audience::To::Space(audience::Space {})),
    }
}

/// An audience addressed to exactly one participant.
///
/// The DID is not checked here — [`audience_from_untyped_checked`] is where an
/// empty one is refused, because that is the boundary a caller crosses. This is
/// a constructor, not a gate.
#[must_use]
pub fn participant_audience(did: impl Into<String>) -> Audience {
    Audience {
        to: Some(audience::To::Participant(audience::Participant {
            did: did.into(),
        })),
    }
}

/// Resolve an `(audience_type, target_did)` pair a CALLER supplied to a typed
/// [`Audience`], refusing every shape an author must not be able to express.
///
/// The authoring counterpart to [`audience_from_untyped`], and the two must not
/// be swapped. That one WIDENS — an unspecified, unknown, or targetless
/// audience becomes `Space` — which is right for storage, where a row has to say
/// something, and catastrophic on the authoring path, where `Space` means
/// *notify the entire space*. A caller who sent a typo, or a newer client
/// sending a discriminant this build has never heard of, would have it silently
/// upgraded into a broadcast. So: authoring refuses, storage repairs.
///
/// Returns the rejection message rather than a typed error because the three
/// doors deliver it differently — a WS `ErrorCode::InvalidMessage` frame, an
/// MCP `RecordRejected`, a `ChangeError::InvalidArgument` on the record seam.
/// The wording is what must not diverge; the envelope is each door's business.
/// (Same split as `kutl-relay`'s `ids::check_uuid`, and the reason this crate
/// stays on `prost` + `serde` with no error-type dependency.)
///
/// # What it refuses
///
/// - `UNSPECIFIED` and unknown discriminants.
/// - `PARTICIPANT` with an empty or absent DID — addresses nobody.
/// - `SPACE` carrying a target DID — the typed `Audience` has no arm for
///   "space-wide but addressed to someone", so the conversion
///   would drop the target silently.
///
/// One shared converter, on purpose: each door (WS, MCP, the record seam)
/// spelling its own copy of these checks is how they diverge — e.g. one door
/// rejecting a targeted `SPACE` while another accepts it.
///
/// The six retired role audiences are NOT refused here: they cut over to
/// space-wide outright, so they resolve to `Space` on both paths.
///
/// # Errors
///
/// Returns the caller-facing rejection message for any of the shapes above.
pub fn audience_from_untyped_checked(
    audience_type: i32,
    target_did: Option<&str>,
) -> Result<Audience, String> {
    let target = target_did.unwrap_or("");
    // One arm per variant, each answering for its own target rule. A shared
    // `_ if !target.is_empty()` guard ahead of the variants would be shorter and
    // wrong: it would catch UNSPECIFIED first and tell the caller their
    // `target_did` is the problem when the missing audience is.
    match AudienceType::try_from(audience_type) {
        Err(_) => Err(format!("unknown audience discriminant {audience_type}")),
        Ok(AudienceType::Unspecified) => Err("audience type required".to_owned()),
        Ok(AudienceType::Participant) => {
            if target.is_empty() {
                return Err("participant audience requires target_did".to_owned());
            }
            Ok(participant_audience(target))
        }
        // The retired six join `space` — see `AUTHORABLE_AUDIENCES`.
        Ok(
            AudienceType::Space
            | AudienceType::AgentOwners
            | AudienceType::AgentEditors
            | AudienceType::AgentViewers
            | AudienceType::HumanOwners
            | AudienceType::HumanEditors
            | AudienceType::HumanViewers,
        ) => {
            if !target.is_empty() {
                return Err(
                    "target_did must be empty when audience is 'space' (space audience \
                            is a broadcast); use audience 'participant' to target a specific user"
                        .to_owned(),
                );
            }
            Ok(space_audience())
        }
    }
}

/// Classify a `(audience_type, target_did)` pair. The single walk over
/// [`AudienceType`], shared by the storage and delivery accessors so the rules
/// they agree on — a participant needs a non-empty DID, the retired role
/// audiences are space-wide — exist once.
fn classify_audience(audience_type: i32, target_did: Option<&str>) -> AudienceDelivery<'_> {
    match AudienceType::try_from(audience_type) {
        Ok(AudienceType::Participant) => match target_did {
            Some(did) if !did.is_empty() => AudienceDelivery::Participant(did),
            // Participant with no target addresses nobody — the dead end that
            // motivated the typed shape in the first place.
            _ => AudienceDelivery::Undeliverable,
        },
        // The six retired role audiences join `space` — see `AudienceDelivery`.
        Ok(
            AudienceType::Space
            | AudienceType::AgentOwners
            | AudienceType::AgentEditors
            | AudienceType::AgentViewers
            | AudienceType::HumanOwners
            | AudienceType::HumanEditors
            | AudienceType::HumanViewers,
        ) => AudienceDelivery::Space,
        Ok(AudienceType::Unspecified) | Err(_) => AudienceDelivery::Undeliverable,
    }
}

/// How a flag's audience resolves for a **delivery** decision.
///
/// Three outcomes, and the third is why this exists rather than an [`Audience`]:
/// it keeps [`audience_from_untyped`]'s widening — right for a projection column,
/// wrong for delivery — out of the decision entirely, and makes the call site an
/// exhaustive match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudienceDelivery<'a> {
    /// Everyone in the space.
    ///
    /// Also where the six retired role audiences land, cut over to space-wide
    /// in one step. Widening them is
    /// a **surfacing** change and not a disclosure one, because audience is a
    /// delivery filter rather than an access boundary — every synced client
    /// already holds the record, so the flag becomes more visible, never newly
    /// readable.
    Space,
    /// One participant, by DID.
    Participant(&'a str),
    /// Addressed to nobody determinable: `UNSPECIFIED`, a participant audience
    /// with no target, or an unrecognized discriminant.
    ///
    /// Distinct from [`Self::Space`] on purpose, and **not** transitional — a
    /// malformed audience can arrive by replication at any time.
    /// [`audience_from_untyped`] folds these into space-wide, which is the right
    /// repair for a projection column (a stored row must say *something*) and the
    /// wrong one here, where it turns "malformed addressing" into "notify
    /// everybody." A delivery surface should drop.
    Undeliverable,
}

/// Resolve a flag's audience for a delivery decision, applying the precedence
/// rule. **The** accessor for "who should this reach."
#[must_use]
pub fn flag_audience_delivery(flag: &FlagPayload) -> AudienceDelivery<'_> {
    let (audience_type, target_did) = flag_audience_untyped(flag);
    classify_audience(audience_type, target_did)
}

/// Reduce a typed [`Audience`] back to the legacy `(audience_type, target_did)`
/// pair the projections still store, borrowing the DID rather than cloning it.
///
/// The inverse of [`audience_from_untyped`], and lossless in the only direction
/// that matters, because the typed shape has strictly fewer states: the six role
/// audiences have no typed arm to come back from.
///
/// **Faithful, not normalizing** — and the distinction matters for one input.
/// `None` and an `Audience` with `to` unset carry no addressee at all, so they
/// reduce to space-wide. But a `Participant` with an **empty DID** reduces to
/// `(PARTICIPANT, Some(""))`, unchanged: that is a malformed audience, and
/// reporting it verbatim is what lets [`flag_audience_delivery`] classify it
/// `Undeliverable`. Normalizing it to space-wide here would make a malformed
/// record notify everybody — the same trap [`audience_from_untyped`] documents
/// from the other direction.
///
/// The authored seam rejects an empty participant DID, so a well-formed record
/// never reaches this case.
#[must_use]
pub fn audience_to_untyped(audience: Option<&Audience>) -> (i32, Option<&str>) {
    match audience.and_then(|a| a.to.as_ref()) {
        Some(audience::To::Participant(p)) => {
            (i32::from(AudienceType::Participant), Some(p.did.as_str()))
        }
        Some(audience::To::Space(_)) | None => (i32::from(AudienceType::Space), None),
    }
}

/// A flag's audience, applying the precedence rule: the typed field
/// when set, the deprecated pair otherwise. **The** flag-audience accessor.
///
/// Reading `audience_type` directly is a bug the moment a peer on a newer build
/// replicates a flag that sets only the typed field — the untyped read sees
/// `UNSPECIFIED`, which projects a participant-scoped signal as space-wide and
/// makes the CLI agent watch drop the flag entirely.
///
/// Returns the pair rather than an [`Audience`] for two reasons: it borrows, so a
/// projection can bind it per row without allocating; and it is LOSSLESS, so a
/// legacy row's stored discriminant is never rewritten by a rebuild. Precedence
/// itself lives here and nowhere else — a caller re-deriving it is how the halves
/// drift. For a delivery decision use [`flag_audience_delivery`], which layers
/// classification on top of this.
#[must_use]
pub fn flag_audience_untyped(flag: &FlagPayload) -> (i32, Option<&str>) {
    if flag.audience.is_some() {
        return audience_to_untyped(flag.audience.as_ref());
    }
    // The deprecated-pair half of the precedence rule; reading it is the intent.
    #[allow(deprecated)]
    (flag.audience_type, flag.target_did.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flag_kind_round_trips() {
        for name in flag_kind_names(FLAG_KINDS) {
            let i = flag_kind_from_str(name).expect("known name parses");
            assert_eq!(flag_kind_to_str(i), name, "round-trip for {name}");
        }
        assert_eq!(flag_kind_from_str("nope"), None);
        assert_eq!(flag_kind_to_str(0), "info");
    }

    #[test]
    fn test_every_flag_kind_carries_guidance() {
        for kind in FLAG_KINDS {
            assert!(
                !flag_kind_guidance(*kind).is_empty(),
                "{} needs a one-line meaning",
                flag_kind_to_str(i32::from(*kind))
            );
        }
    }

    #[test]
    fn test_authorable_kinds_are_every_kind_but_comment() {
        let expected: Vec<FlagKind> = FLAG_KINDS
            .iter()
            .copied()
            .filter(|k| *k != FlagKind::Comment)
            .collect();
        assert_eq!(
            AUTHORABLE_FLAG_KINDS, expected,
            "comment is the only kind a surface may filter for but not author"
        );
    }

    #[test]
    fn test_audience_round_trips() {
        for name in AUDIENCES {
            let i = audience_type_from_str(name).expect("known name parses");
            assert_eq!(audience_type_to_str(i), *name, "round-trip for {name}");
        }
        assert_eq!(audience_type_from_str("nope"), None);
    }

    /// Round-tripping through the typed shape, asserted as the pair so the two
    /// mappers are checked against each other rather than each in isolation.
    const SPACE_PAIR: (i32, Option<&str>) = (AudienceType::Space as i32, None);

    /// A participant audience survives the legacy mapping only when it actually
    /// names someone.
    #[test]
    fn test_audience_from_legacy_participant_with_did() {
        let a = audience_from_untyped(i32::from(AudienceType::Participant), Some("did:key:zBob"));
        assert_eq!(
            audience_to_untyped(Some(&a)),
            (i32::from(AudienceType::Participant), Some("did:key:zBob"))
        );
    }

    /// The two dead ends that today resolve to "participant with no target",
    /// i.e. delivery to nobody, must both become space-wide.
    #[test]
    fn test_audience_from_legacy_targetless_dead_ends_become_space() {
        for (audience_type, target) in [
            (i32::from(AudienceType::Participant), None),
            (i32::from(AudienceType::Participant), Some("")),
            (i32::from(AudienceType::Unspecified), None),
            // UNSPECIFIED renders as "participant" via audience_type_to_str, so
            // a stray target must not resurrect participant delivery.
            (i32::from(AudienceType::Unspecified), Some("did:key:zBob")),
        ] {
            let a = audience_from_untyped(audience_type, target);
            assert_eq!(
                audience_to_untyped(Some(&a)),
                SPACE_PAIR,
                "audience_type {audience_type} target {target:?} should be space-wide"
            );
        }
    }

    /// Role audiences have no typed arm, so converting one INTO the typed shape
    /// collapses it to space-wide — which is why `audience_from_untyped` must never
    /// be used on a delivery decision (see `flag_audience_untyped`, which
    /// preserves them). An unknown discriminant collapses the same way, so an
    /// unreadable audience over-notifies rather than dropping delivery.
    #[test]
    fn test_audience_from_legacy_roles_and_unknown_collapse_to_space() {
        let roles = [
            AudienceType::HumanOwners,
            AudienceType::HumanEditors,
            AudienceType::HumanViewers,
            AudienceType::AgentOwners,
            AudienceType::AgentEditors,
            AudienceType::AgentViewers,
        ];
        for role in roles {
            let a = audience_from_untyped(i32::from(role), Some("did:key:zBob"));
            assert_eq!(audience_to_untyped(Some(&a)), SPACE_PAIR, "role {role:?}");
        }
        let unknown = audience_from_untyped(9_999, Some("did:key:zBob"));
        assert_eq!(audience_to_untyped(Some(&unknown)), SPACE_PAIR);
    }

    /// The authoring converter REFUSES every shape the storage one repairs.
    ///
    /// This pair of tests is the whole point of there being two functions. The
    /// one above pins that unknown and retired audiences collapse to space-wide;
    /// this one pins that the same inputs are rejected when a CALLER supplies
    /// them. Collapse the two and an unrecognized audience — a typo, or a newer
    /// client's discriminant — is silently upgraded into a broadcast to
    /// everybody.
    #[test]
    fn test_authoring_converter_refuses_what_storage_widens() {
        // Unspecified: the pair's dead end. Storage widens it; authoring must not.
        let err = audience_from_untyped_checked(i32::from(AudienceType::Unspecified), None)
            .expect_err("an unspecified audience must not become a broadcast");
        assert!(err.contains("audience type required"), "got: {err}");

        // An unknown discriminant — what a newer client sends this build.
        let err = audience_from_untyped_checked(9_999, None)
            .expect_err("an unreadable audience must not become a broadcast");
        assert!(
            err.contains("9999"),
            "the refusal must name the value: {err}"
        );

        // Participant addressing nobody.
        let err = audience_from_untyped_checked(i32::from(AudienceType::Participant), Some(""))
            .expect_err("a participant audience with no target addresses nobody");
        assert!(err.contains("requires target_did"), "got: {err}");

        // Space carrying a target — a broadcast cannot also be addressed to
        // one person; the shared converter owns this rule for every door.
        let err =
            audience_from_untyped_checked(i32::from(AudienceType::Space), Some("did:key:zBob"))
                .expect_err("a broadcast cannot also be addressed to one person");
        assert!(err.contains("must be empty"), "got: {err}");
    }

    /// The two shapes a caller MAY author still convert, and the retired six
    /// still resolve rather than being refused — they cut over to
    /// space-wide outright, so rejecting them would break a legal caller.
    #[test]
    fn test_authoring_converter_accepts_the_authorable_shapes() {
        let space = audience_from_untyped_checked(i32::from(AudienceType::Space), None)
            .expect("space with no target is the ordinary broadcast");
        assert_eq!(audience_to_untyped(Some(&space)), SPACE_PAIR);

        let dm = audience_from_untyped_checked(
            i32::from(AudienceType::Participant),
            Some("did:key:zBob"),
        )
        .expect("participant with a target is a DM");
        assert_eq!(
            audience_to_untyped(Some(&dm)),
            (i32::from(AudienceType::Participant), Some("did:key:zBob"))
        );

        for role in [
            AudienceType::HumanOwners,
            AudienceType::HumanEditors,
            AudienceType::HumanViewers,
            AudienceType::AgentOwners,
            AudienceType::AgentEditors,
            AudienceType::AgentViewers,
        ] {
            let a = audience_from_untyped_checked(i32::from(role), None)
                .unwrap_or_else(|e| panic!("retired role {role:?} must resolve, not refuse: {e}"));
            assert_eq!(audience_to_untyped(Some(&a)), SPACE_PAIR, "role {role:?}");
        }
    }

    /// A legacy-shaped flag: the deprecated pair is a fixture of what is already
    /// on disk.
    #[allow(deprecated)]
    fn legacy_flag(audience_type: AudienceType, target_did: Option<&str>) -> FlagPayload {
        FlagPayload {
            kind: i32::from(FlagKind::Info),
            audience_type: i32::from(audience_type),
            target_did: target_did.map(str::to_owned),
            message: "m".into(),
            audience: None,
            anchor_text: None,
        }
    }

    /// The precedence rule: typed field wins, legacy pair is the fallback.
    #[test]
    fn test_flag_audience_legacy_precedence() {
        let legacy_only = legacy_flag(AudienceType::Participant, Some("did:key:zLegacy"));
        assert_eq!(
            flag_audience_untyped(&legacy_only),
            (
                i32::from(AudienceType::Participant),
                Some("did:key:zLegacy")
            ),
            "with no typed audience the deprecated pair is read"
        );

        // Both set is rejected at the authored seam, but a reader still has to be
        // deterministic about it: the typed field wins.
        let both = FlagPayload {
            audience: Some(audience_from_untyped(
                i32::from(AudienceType::Participant),
                Some("did:key:zTyped"),
            )),
            ..legacy_only
        };
        assert_eq!(
            flag_audience_untyped(&both),
            (i32::from(AudienceType::Participant), Some("did:key:zTyped")),
            "the typed audience takes precedence over the deprecated pair"
        );
    }

    /// A flag from a newer peer that sets ONLY the typed field must not read as
    /// space-wide. Without precedence the untyped read sees UNSPECIFIED, which
    /// projects a participant-scoped signal space-wide and makes the CLI agent
    /// watch drop it — the pair of bugs this accessor exists to prevent.
    #[test]
    fn test_flag_audience_legacy_typed_only_is_not_space() {
        let typed_only = FlagPayload {
            audience: Some(audience_from_untyped(
                i32::from(AudienceType::Participant),
                Some("did:key:zBob"),
            )),
            ..legacy_flag(AudienceType::Unspecified, None)
        };
        assert_eq!(
            flag_audience_untyped(&typed_only),
            (i32::from(AudienceType::Participant), Some("did:key:zBob"))
        );
    }

    /// The precedence read passes a legacy discriminant through UNCHANGED, retired
    /// role audiences included.
    ///
    /// This is a **storage** invariant, not a delivery one — delivery folds those
    /// six into space-wide (the retirement cutover). It matters because this accessor feeds
    /// the projection: if it normalized on the way through, rebuilding a space from
    /// its records would silently rewrite the stored audience of every row written
    /// before the cutover, mutating data on a read path.
    #[test]
    fn test_flag_audience_legacy_passes_discriminants_through_verbatim() {
        for role in [
            AudienceType::HumanOwners,
            AudienceType::HumanEditors,
            AudienceType::HumanViewers,
            AudienceType::AgentOwners,
            AudienceType::AgentEditors,
            AudienceType::AgentViewers,
        ] {
            let flag = legacy_flag(role, None);
            assert_eq!(
                flag_audience_untyped(&flag),
                (i32::from(role), None),
                "role {role:?} must pass through the precedence read untouched"
            );
        }
    }

    /// Every legacy audience value maps to exactly one delivery outcome, and the
    /// three groups that behave differently stay distinguishable. Exhaustive over
    /// the enum plus an unknown discriminant, because a value silently landing in
    /// the wrong group is a delivery bug with no other symptom.
    #[test]
    fn test_flag_audience_delivery_covers_every_legacy_value() {
        let cases = [
            (AudienceType::Space, None, AudienceDelivery::Space),
            (
                AudienceType::Participant,
                Some("did:key:zBob"),
                AudienceDelivery::Participant("did:key:zBob"),
            ),
            // All six retired role audiences cut over to space-wide, the
            // human_* three included.
            (AudienceType::AgentOwners, None, AudienceDelivery::Space),
            (AudienceType::AgentEditors, None, AudienceDelivery::Space),
            (AudienceType::AgentViewers, None, AudienceDelivery::Space),
            (AudienceType::HumanOwners, None, AudienceDelivery::Space),
            (AudienceType::HumanEditors, None, AudienceDelivery::Space),
            (AudienceType::HumanViewers, None, AudienceDelivery::Space),
            (
                AudienceType::Unspecified,
                None,
                AudienceDelivery::Undeliverable,
            ),
        ];
        for (audience_type, target, expected) in cases {
            let flag = legacy_flag(audience_type, target);
            assert_eq!(
                flag_audience_delivery(&flag),
                expected,
                "{audience_type:?} target {target:?}"
            );
        }

        // A stray target must not turn a non-participant audience into a DM, and
        // an unreadable discriminant must not become space-wide.
        assert_eq!(
            flag_audience_delivery(&legacy_flag(AudienceType::Space, Some("did:key:zBob"))),
            AudienceDelivery::Space
        );
        #[allow(deprecated)]
        let unknown = FlagPayload {
            audience_type: 9_999,
            ..legacy_flag(AudienceType::Space, None)
        };
        assert_eq!(
            flag_audience_delivery(&unknown),
            AudienceDelivery::Undeliverable
        );
    }

    /// The two dead ends must be `Undeliverable`, NOT `Space`. Folding them into
    /// space-wide is right for a projection column and wrong here: it converts an
    /// unaddressed flag into one every participant is notified about.
    #[test]
    fn test_flag_audience_delivery_dead_ends_are_undeliverable_not_space() {
        for target in [None, Some("")] {
            assert_eq!(
                flag_audience_delivery(&legacy_flag(AudienceType::Participant, target)),
                AudienceDelivery::Undeliverable,
                "participant with target {target:?}"
            );
        }
        // The same inputs through the projection-facing converter DO widen to
        // space-wide — the divergence is the point, so assert it rather than
        // leaving the two rules looking accidentally inconsistent.
        assert_eq!(
            audience_to_untyped(Some(&audience_from_untyped(
                i32::from(AudienceType::Participant),
                None
            ))),
            SPACE_PAIR
        );
    }

    /// A typed `Participant` with an EMPTY DID is malformed, and the two
    /// directions must disagree about it on purpose.
    ///
    /// `audience_to_untyped` reports it verbatim so delivery can see it is
    /// unaddressed; `flag_audience_delivery` then calls it `Undeliverable`. If the
    /// reduction normalized it to space-wide instead, a malformed record would
    /// notify every participant. The authored seam rejects the
    /// shape, so this pins the behaviour of a record that should
    /// never exist but is representable — and arrives by replication if a peer
    /// ever writes one.
    #[test]
    fn test_empty_participant_did_is_undeliverable_not_space() {
        let malformed = Audience {
            to: Some(audience::To::Participant(audience::Participant {
                did: String::new(),
            })),
        };
        assert_eq!(
            audience_to_untyped(Some(&malformed)),
            (i32::from(AudienceType::Participant), Some("")),
            "the reduction must report the empty DID verbatim, not normalize it"
        );

        let flag = FlagPayload {
            audience: Some(malformed),
            anchor_text: None,
            ..legacy_flag(AudienceType::Unspecified, None)
        };
        assert_eq!(
            flag_audience_delivery(&flag),
            AudienceDelivery::Undeliverable,
            "an empty participant DID addresses nobody and must not deliver space-wide"
        );
    }

    /// A typed-only flag from a newer peer resolves for delivery too — the
    /// precedence rule reaches this accessor, not just the column one.
    #[test]
    fn test_flag_audience_delivery_honours_typed_field() {
        let typed_only = FlagPayload {
            audience: Some(audience_from_untyped(
                i32::from(AudienceType::Participant),
                Some("did:key:zBob"),
            )),
            ..legacy_flag(AudienceType::Unspecified, None)
        };
        assert_eq!(
            flag_audience_delivery(&typed_only),
            AudienceDelivery::Participant("did:key:zBob")
        );
    }

    /// A comment is a `FLAG_KIND_COMMENT` flag, so it resolves
    /// through the same accessor as every other flag — the point of keeping it a
    /// kind. `anchor_text` is the only field that distinguishes it, and it plays no
    /// part in audience resolution.
    #[test]
    fn test_comment_is_a_flag_for_audience_purposes() {
        let mut comment = legacy_flag(AudienceType::Unspecified, None);
        comment.set_kind(FlagKind::Comment);
        comment.anchor_text = Some("the wrapped passage".into());
        comment.audience = Some(audience_from_untyped(
            i32::from(AudienceType::Participant),
            Some("did:key:zBob"),
        ));
        assert_eq!(
            flag_audience_delivery(&comment),
            AudienceDelivery::Participant("did:key:zBob"),
            "a comment resolves through the flag accessor like any other kind"
        );
        assert_eq!(
            flag_audience_untyped(&comment),
            (i32::from(AudienceType::Participant), Some("did:key:zBob"))
        );
    }

    /// An `Audience` with `to` unset is malformed, and so is an absent one; both
    /// must read as space-wide rather than one panicking and another dropping
    /// delivery.
    #[test]
    fn test_audience_unset_to_is_space() {
        assert_eq!(
            audience_to_untyped(Some(&Audience { to: None })),
            SPACE_PAIR
        );
        assert_eq!(audience_to_untyped(None), SPACE_PAIR);
    }
}
