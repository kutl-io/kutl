//! The content overlay an `EDITED` record contributes.
//!
//! One decomposition for every reader of the content track — the fold-backed
//! summary and both projection substrates — so they cannot disagree about
//! which fields an edit can and cannot change.

use kutl_proto::sync::Signal;

/// The complete decision-content replacement an EDITED decision carries.
///
/// A `DecisionPayload` is always whole — the tracker that mints it re-derives
/// every field from the current heading — so a decision edit REPLACES all
/// four columns outright, `parent_hash = None` included.
pub struct DecisionContent<'a> {
    /// Stable hash of the heading as it reads now.
    pub title_hash: &'a str,
    /// The heading text as it reads now.
    pub title: &'a str,
    /// Heading depth.
    pub depth: i32,
    /// Hash of the enclosing decision heading, when nested.
    pub parent_hash: Option<&'a str>,
}

/// The content fields one EDITED record carries.
///
/// **Overlay semantics**: a field the payload carries replaces the stored
/// value; a field it does not carry leaves it alone. "Carried" is judged
/// per field, by the strongest presence signal its proto shape offers:
///
/// - `message` / `body` — carried iff NON-EMPTY. Proto3 plain strings have
///   no presence bit, so emptiness is the only signal; it is unambiguous
///   here because no edit path produces an empty one (a mention composite
///   falls back to `@DisplayName`), while a comment-anchor edit — which
///   cannot know the comment's body — deliberately leaves `message` empty.
/// - `anchor_text` — carried iff `Some`. A proto3 `optional`, so true
///   presence: `Some("")` is a carried empty anchor.
/// - `decision` — the whole payload, always (see [`DecisionContent`]).
#[derive(Default)]
pub struct ContentOverlay<'a> {
    /// Replacement flag message (or chat topic); `None` = not carried.
    pub message: Option<&'a str>,
    /// Replacement comment anchor excerpt; `None` = not carried.
    pub anchor_text: Option<&'a str>,
    /// Replacement reply body; `None` = not carried.
    pub body: Option<&'a str>,
    /// Complete decision-content replacement; `None` for non-decisions.
    pub decision: Option<DecisionContent<'a>>,
}

impl<'a> ContentOverlay<'a> {
    /// Decompose an EDITED record's payload into its content overlay.
    ///
    /// Total: a transition-payload or payload-less EDITED (malformed, but the
    /// replicated path cannot refuse it) decomposes to an empty overlay that
    /// changes nothing. The fold refuses to let such a record occupy the
    /// content track at all (see [`carries_content`]).
    #[must_use]
    pub fn from_record(record: &'a Signal) -> Self {
        use kutl_proto::sync::signal::Payload;

        match &record.payload {
            Some(Payload::Flag(f)) => Self {
                message: (!f.message.is_empty()).then_some(f.message.as_str()),
                anchor_text: f.anchor_text.as_deref(),
                ..Self::default()
            },
            Some(Payload::Chat(c)) => Self {
                message: c.topic.as_deref().filter(|t| !t.is_empty()),
                ..Self::default()
            },
            Some(Payload::Reply(r)) => Self {
                body: (!r.body.is_empty()).then_some(r.body.as_str()),
                ..Self::default()
            },
            Some(Payload::Decision(d)) => Self {
                decision: Some(DecisionContent {
                    title_hash: d.title_hash.as_str(),
                    title: d.title.as_str(),
                    depth: d.depth,
                    parent_hash: d.parent_hash.as_deref(),
                }),
                ..Self::default()
            },
            Some(Payload::Transition(_)) | None => Self::default(),
        }
    }
}

/// Whether an EDITED record can contribute content at all.
///
/// The fold's content track admits only records that pass this — a
/// payload-less or transition-payload EDITED decomposes to an empty overlay,
/// and letting it become the WINNING content record would hand fold readers
/// a record with nothing in it while the projections correctly ignore it.
#[must_use]
pub fn carries_content(record: &Signal) -> bool {
    use kutl_proto::sync::signal::Payload;
    !matches!(&record.payload, Some(Payload::Transition(_)) | None)
}
