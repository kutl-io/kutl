//! In-doc flag marker parser and differ for `@[Name](kind:id)` syntax.
//!
//! The marker is the editor-UX serialization of a `create_flag(kind,
//! target_did)` directive — typing `@` in the editor produces the
//! `@[Name](kind:id)` form, and this observer materializes it into a
//! real flag signal. A "mention" is not a distinct entity; it's the
//! user-facing word for the targeting carried by a flag, here
//! serialized inline.
//!
//! Parses inline markers from document text using the same regex as
//! the browser client, and diffs against a known set to produce
//! add/remove events. Downstream consumers (UX server stream-consumer)
//! turn those events into actual flag-create / flag-close ops, the
//! same shape an explicit MCP `create_flag` RPC would produce.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex};

use kutl_proto::sync::{FlagKind, Signal, signal};
use kutl_signals::fold::{SignalStatus, SpaceSignalState};

/// A parsed in-doc flag marker from document text.
///
/// Each marker is a fully-formed flag-create directive: it carries the
/// flag kind and the target id. Equivalent to the args of an MCP
/// `create_flag(kind, target_did)` call.
#[derive(Debug, Clone)]
pub struct InDocFlag {
    /// Flag kind (e.g., `info`, `review_requested`).
    pub kind: String,
    /// Target account ID.
    pub target_id: String,
    /// Display name from the marker syntax.
    pub display_name: String,
    /// Picker-typed body — the optional `|<message>` portion of the
    /// marker. `None` when the picker was confirmed without typing a
    /// body (legitimate case). An empty `|` (e.g., `(question:abc|)`) is
    /// normalized to `Some("")` rather than `None` — empty-after-bar is
    /// a distinct user gesture from no-bar-at-all, even if downstream
    /// renderers happen to treat both the same way today.
    pub message: Option<String>,
}

impl InDocFlag {
    /// The message this mention carries: the picker-typed body when the author
    /// typed one, otherwise `@DisplayName`.
    ///
    /// A mention with no body still needs to READ as something — a flag whose
    /// message is the empty string renders as a blank card. The materializer
    /// writes the record, so the rule has to be here: applied only in a
    /// downstream renderer, the persisted message would be empty while the
    /// live feed event said `@Alice`.
    ///
    /// "No body" collapses `None` (no `|` at all) and `Some("")` (an empty
    /// `|`): the two are a real distinction at the marker boundary, but neither
    /// gives a renderer anything to show.
    #[must_use]
    pub fn message_or_mention(&self) -> String {
        self.message
            .as_deref()
            .filter(|m| !m.is_empty())
            .map_or_else(|| format!("@{}", self.display_name), str::to_owned)
    }

    /// Whether this entry's CONTENT is known, i.e. safe to diff against.
    ///
    /// A parsed marker always is (the regex requires a non-empty display
    /// name). The one unknown case is a re-derived entry whose record carried
    /// no message to restore — comparing against it would emit a phantom
    /// EDITED for the mention on the first merge after every restart, so the
    /// differ absorbs those silently instead.
    fn content_known(&self) -> bool {
        !self.display_name.is_empty() || self.message.is_some()
    }
}

impl PartialEq for InDocFlag {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.target_id == other.target_id
    }
}

impl Eq for InDocFlag {}

impl std::hash::Hash for InDocFlag {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.target_id.hash(state);
    }
}

/// Valid flag kinds carried inline (must match client and proto enum).
///
/// `comment` is excluded — comments use the `[text]{.cmt #uuid}` marker
/// (see `comments.rs`), not the `@[…]` form. `decision` is excluded —
/// decisions use heading-form / callout-form (see `decisions.rs`).
pub(crate) const VALID_KINDS: &[&str] = &[
    "info",
    "completed",
    "review_requested",
    "question",
    "blocked",
];

/// Compiled regex for `@[Name](kind:id)` and `@[Name](kind:id|note)` syntax.
static IN_DOC_FLAG_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"@\[([^\]]+)\]\((\w+):([^)|]+)(?:\|([^)]*))?\)").expect("valid regex")
});

/// Parse all `@[Name](kind:id)` markers from document text.
pub fn parse_in_doc_flags(content: &str) -> HashSet<InDocFlag> {
    IN_DOC_FLAG_RE
        .captures_iter(content)
        .filter_map(|cap| {
            let kind = cap[2].to_owned();
            if !VALID_KINDS.contains(&kind.as_str()) {
                return None;
            }
            Some(InDocFlag {
                display_name: cap[1].to_owned(),
                kind,
                target_id: cap[3].to_owned(),
                // Group 4 is the `|<message>` portion. The regex's
                // outer `(?:…)?` makes the whole group optional, so
                // `cap.get(4)` distinguishes "no bar" (None) from
                // "bar present, body empty" (Some("")).
                message: cap.get(4).map(|m| m.as_str().to_owned()),
            })
        })
        .collect()
}

/// Diff result between two in-doc-flag sets.
///
/// Contains the markers added and removed relative to the previously
/// known set for a given document.
pub struct InDocFlagDiff {
    /// Markers present in `current` but not in `previous`.
    pub added: Vec<InDocFlag>,
    /// Markers present in `previous` but not in `current`.
    pub removed: Vec<InDocFlag>,
    /// Markers present in BOTH whose rendered content — the
    /// [`InDocFlag::message_or_mention`] composite, exactly what the record
    /// persists — changed. Content, not identity: each becomes an EDITED
    /// record. The `(kind, target_id)` equality key alone cannot see such a
    /// change, and untracked it would leave the projected message stale
    /// forever.
    pub edited: Vec<InDocFlag>,
}

/// Tracks known in-doc flag markers per document and computes diffs.
pub struct InDocFlagTracker {
    /// Known markers keyed by `(space_id, document_id)`.
    known: Mutex<HashMap<(String, String), HashSet<InDocFlag>>>,
}

impl Default for InDocFlagTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl InDocFlagTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            known: Mutex::new(HashMap::new()),
        }
    }

    /// Update the known set for a document and return the diff.
    ///
    /// On first call for a document, all parsed markers are "added"
    /// (the previous set is empty).
    pub fn update(&self, space_id: &str, document_id: &str, content: &str) -> InDocFlagDiff {
        let stripped = crate::markers::code_fence::skip_fenced_lines(content);
        let current = parse_in_doc_flags(&stripped);
        let mut known = self
            .known
            .lock()
            .expect("in-doc flag tracker lock poisoned");
        let key = (space_id.to_owned(), document_id.to_owned());
        let previous = known.get(&key).cloned().unwrap_or_default();

        let added: Vec<InDocFlag> = current.difference(&previous).cloned().collect();
        let removed: Vec<InDocFlag> = previous.difference(&current).cloned().collect();
        // The set intersection is where content edits hide: `Hash`/`Eq` cover
        // only `(kind, target_id)`, so `HashSet::get` retrieves the PREVIOUS
        // struct for an identity present in both, and the composites can be
        // compared even though the set difference cannot see them.
        let edited: Vec<InDocFlag> = current
            .iter()
            .filter(|flag| {
                previous.get(flag).is_some_and(|prev| {
                    prev.content_known() && prev.message_or_mention() != flag.message_or_mention()
                })
            })
            .cloned()
            .collect();

        known.insert(key, current);

        InDocFlagDiff {
            added,
            removed,
            edited,
        }
    }

    /// Remove tracking state for a document (e.g., on eviction).
    pub fn remove(&self, space_id: &str, document_id: &str) {
        self.known
            .lock()
            .expect("in-doc flag tracker lock poisoned")
            .remove(&(space_id.to_owned(), document_id.to_owned()));
    }

    /// Rebuild the known set for a document from the folded signal records.
    ///
    /// Called on doc re-observe after an eviction (see [`Self::remove`]): the
    /// known set is reconstructed from the records that ACTUALLY materialized,
    /// so a subsequent [`Self::update`] on unchanged content yields an empty
    /// diff instead of spuriously re-adding every mention. Because it folds only
    /// appended records, a mention whose CREATED never landed is absent here and
    /// gets re-emitted on the next merge — the intended recovery path.
    ///
    /// A folded mention (flag) signal maps to a known entry keyed by
    /// `(kind, target_id)` when its status is `Open`; a `Closed`/`Tombstoned`
    /// signal (the mention was removed) leaves no entry. Comment-kind flags are
    /// skipped — they are the [`crate::markers::comments::CommentTracker`]'s.
    /// `records` may hold signals for other documents / kinds; only mention
    /// signals belonging to `document_id` are folded in.
    pub fn rederive_from(&self, space_id: &str, document_id: &str, records: &[Signal]) {
        // Fold every record for this document — transitions (CLOSED) carry NO
        // payload, so they cannot be pre-filtered by kind; the fold parks orphan
        // transitions until their CREATED lands. Kind classification (mention vs.
        // comment) happens below, off the folded CREATED payload.
        let mut fold = SpaceSignalState::default();
        for record in records {
            if record.document_id.as_deref() == Some(document_id) {
                fold.apply(record.clone());
            }
        }

        let mut rebuilt = HashSet::new();
        for (_, state) in fold.iter() {
            if state.status != SignalStatus::Open {
                continue; // removed or tombstoned mention holds no known entry
            }
            let Some(signal::Payload::Flag(payload)) = &state.created.payload else {
                continue;
            };
            let Some(kind) = flag_kind_from_i32(payload.kind) else {
                continue; // comment-kind or unknown kind is not an in-doc flag
            };
            // Read RAW, deliberately NOT through the audience accessor. On the
            // materializer path `target_did` does not carry an audience target:
            // the record pairs it with `audience_type = SPACE`,
            // so it is the mention SUBJECT, and resolving it as an audience
            // would yield space-wide-with-no-target and silently drop every
            // mention from the rebuilt tracker.
            #[allow(deprecated)]
            let Some(target_id) = payload.target_did.clone() else {
                continue; // an in-doc flag always targets an account
            };
            // The rendered composite is restored from the fold's CONTENT
            // track (the record's `message` IS `message_or_mention()` at
            // materialization), so the next update can tell a real edit from
            // unchanged content. Stored in `message` with an empty
            // `display_name`: `message_or_mention()` then reproduces the
            // record's composite exactly. A record with no message leaves the
            // entry content-unknown (see [`InDocFlag::content_known`]) and
            // the first diff absorbs rather than emits.
            let content_message = match &state.content().payload {
                Some(signal::Payload::Flag(f)) if !f.message.is_empty() => Some(f.message.clone()),
                _ => None,
            };
            rebuilt.insert(InDocFlag {
                kind,
                target_id,
                display_name: String::new(),
                message: content_message,
            });
        }

        let key = (space_id.to_owned(), document_id.to_owned());
        self.known
            .lock()
            .expect("in-doc flag tracker lock poisoned")
            .insert(key, rebuilt);
    }
}

/// Map a proto [`FlagKind`] value back to the inline-marker kind string, or
/// `None` when the kind is not a valid in-doc flag (comment or unknown).
///
/// Inverse of the materializer's `flag_kind_to_i32`; the returned strings are
/// exactly the [`VALID_KINDS`] the parser accepts, so a re-derived entry keys
/// identically to one produced by [`parse_in_doc_flags`].
fn flag_kind_from_i32(kind: i32) -> Option<String> {
    let variant = FlagKind::try_from(kind).ok()?;
    let name = match variant {
        FlagKind::Info => "info",
        FlagKind::Completed => "completed",
        FlagKind::ReviewRequested => "review_requested",
        FlagKind::Question => "question",
        FlagKind::Blocked => "blocked",
        // Comment markers are the CommentTracker's; unspecified is not a flag.
        FlagKind::Comment | FlagKind::Unspecified => return None,
    };
    Some(name.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_marker() {
        let content = "Hello @[Alice](info:acc-123) please review";
        let flags = parse_in_doc_flags(content);
        assert_eq!(flags.len(), 1);
        let f = flags.iter().next().unwrap();
        assert_eq!(f.display_name, "Alice");
        assert_eq!(f.kind, "info");
        assert_eq!(f.target_id, "acc-123");
    }

    #[test]
    fn test_parse_marker_with_note() {
        let content = "Hello @[Alice](info:acc-123|please review the intro)";
        let flags = parse_in_doc_flags(content);
        assert_eq!(flags.len(), 1);
        let f = flags.iter().next().unwrap();
        assert_eq!(f.display_name, "Alice");
        assert_eq!(f.kind, "info");
        assert_eq!(f.target_id, "acc-123");
        assert_eq!(f.message.as_deref(), Some("please review the intro"));
    }

    #[test]
    fn test_parse_marker_without_message_is_none() {
        // No `|` at all — picker confirmed without typing a body.
        let flags = parse_in_doc_flags("@[Alice](info:acc-123)");
        let f = flags.iter().next().unwrap();
        assert_eq!(f.message, None);
    }

    #[test]
    fn test_parse_marker_empty_bar_is_some_empty() {
        // `|` present but body empty — distinct user gesture from
        // "no bar at all". We preserve the distinction.
        let flags = parse_in_doc_flags("@[Carol](question:abc|)");
        let f = flags.iter().next().unwrap();
        assert_eq!(f.message.as_deref(), Some(""));
    }

    #[test]
    fn test_tracker_added_carries_message() {
        // Round-trip through InDocFlagDiff::added to confirm the
        // picker-typed body survives the diff path that drives
        // `in_doc_flag_added` stream emission.
        let tracker = InDocFlagTracker::new();
        let diff = tracker.update(
            "s1",
            "d1",
            "@[Alice](review_requested:a1|please look at section 2)",
        );
        assert_eq!(diff.added.len(), 1);
        assert_eq!(
            diff.added[0].message.as_deref(),
            Some("please look at section 2")
        );
    }

    #[test]
    fn test_tracker_message_change_emits_edited() {
        // A message-only change is an EDITED. The `(kind, target_id)` equality
        // key cannot see it, so without the content comparison the projected
        // message would go stale forever.
        let tracker = InDocFlagTracker::new();
        tracker.update("s1", "d1", "@[Alice](question:a1|first draft)");
        let diff = tracker.update("s1", "d1", "@[Alice](question:a1|second draft)");
        assert!(diff.added.is_empty(), "same identity: {:?}", diff.added);
        assert!(diff.removed.is_empty(), "same identity: {:?}", diff.removed);
        assert_eq!(diff.edited.len(), 1, "diff.edited: {:?}", diff.edited);
        assert_eq!(diff.edited[0].message.as_deref(), Some("second draft"));
    }

    #[test]
    fn test_tracker_display_name_change_emits_edited() {
        // With no picker body the rendered composite is `@DisplayName`, so a
        // display-name change IS a content change.
        let tracker = InDocFlagTracker::new();
        tracker.update("s1", "d1", "@[Alice](question:a1)");
        let diff = tracker.update("s1", "d1", "@[Alice Smith](question:a1)");
        assert_eq!(diff.edited.len(), 1, "diff.edited: {:?}", diff.edited);
        assert_eq!(diff.edited[0].message_or_mention(), "@Alice Smith");
    }

    #[test]
    fn test_tracker_unchanged_content_emits_no_edited() {
        let tracker = InDocFlagTracker::new();
        let content = "@[Alice](question:a1|please review)";
        tracker.update("s1", "d1", content);
        let diff = tracker.update("s1", "d1", content);
        assert!(diff.edited.is_empty(), "diff.edited: {:?}", diff.edited);
    }

    /// Re-derivation restores the record's message so the composite is
    /// comparable across a restart: unchanged content emits nothing, a real
    /// edit still emits.
    #[test]
    fn test_rederive_restores_message_for_edit_detection() {
        let tracker = InDocFlagTracker::new();
        let mut record = created_mention_record("d1", FlagKind::Question, "a1");
        if let Some(signal::Payload::Flag(f)) = &mut record.payload {
            f.message = "please review".into();
        }
        tracker.rederive_from("s1", "d1", std::slice::from_ref(&record));

        // Unchanged content: the parsed composite equals the record's.
        let diff = tracker.update("s1", "d1", "@[Alice](question:a1|please review)");
        assert!(
            diff.edited.is_empty(),
            "phantom edit after restart: {:?}",
            diff.edited
        );

        // A real edit across the restart boundary must still be seen.
        tracker.remove("s1", "d1");
        tracker.rederive_from("s1", "d1", &[record]);
        let diff = tracker.update("s1", "d1", "@[Alice](question:a1|changed my mind)");
        assert_eq!(diff.edited.len(), 1, "diff.edited: {:?}", diff.edited);
        assert_eq!(diff.edited[0].message.as_deref(), Some("changed my mind"));
    }

    /// A re-derived entry whose record carried NO message is content-unknown:
    /// the first post-restart diff absorbs its text silently (no phantom
    /// EDITED), and only a LATER change emits.
    #[test]
    fn test_rederive_without_message_absorbs_then_tracks() {
        let tracker = InDocFlagTracker::new();
        // Legacy record shape: empty message.
        let record = created_mention_record("d1", FlagKind::Question, "a1");
        tracker.rederive_from("s1", "d1", std::slice::from_ref(&record));

        let diff = tracker.update("s1", "d1", "@[Alice](question:a1|first observed)");
        assert!(
            diff.edited.is_empty(),
            "content-unknown must absorb, not emit: {:?}",
            diff.edited
        );

        // The absorb updated the known content, so a real change now emits.
        let diff = tracker.update("s1", "d1", "@[Alice](question:a1|now changed)");
        assert_eq!(diff.edited.len(), 1, "diff.edited: {:?}", diff.edited);
    }

    #[test]
    fn test_parse_multiple_markers() {
        let content = "@[Alice](review_requested:a1) and @[Bob](blocked:b2)";
        let flags = parse_in_doc_flags(content);
        assert_eq!(flags.len(), 2);
    }

    #[test]
    fn test_parse_invalid_kind_filtered() {
        let content = "@[Alice](unknown_kind:acc-123) stays";
        let flags = parse_in_doc_flags(content);
        assert!(flags.is_empty());
    }

    #[test]
    fn test_parse_no_markers() {
        let flags = parse_in_doc_flags("just plain text");
        assert!(flags.is_empty());
    }

    #[test]
    fn test_parse_all_valid_kinds() {
        let content = "\
            @[A](info:1) \
            @[B](completed:2) \
            @[C](review_requested:3) \
            @[D](question:4) \
            @[E](blocked:5)";
        let flags = parse_in_doc_flags(content);
        assert_eq!(flags.len(), 5);
    }

    #[test]
    fn test_dedup_identical_markers() {
        let content = "@[Alice](info:a1) and again @[Alice](info:a1)";
        let flags = parse_in_doc_flags(content);
        assert_eq!(flags.len(), 1);
    }

    #[test]
    fn test_tracker_first_update_all_added() {
        let tracker = InDocFlagTracker::new();
        let diff = tracker.update("s1", "d1", "Hello @[Alice](info:a1)");
        assert_eq!(diff.added.len(), 1);
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_tracker_no_change_empty_diff() {
        let tracker = InDocFlagTracker::new();
        tracker.update("s1", "d1", "Hello @[Alice](info:a1)");
        let diff = tracker.update("s1", "d1", "Hello @[Alice](info:a1)");
        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_tracker_added_and_removed() {
        let tracker = InDocFlagTracker::new();
        tracker.update("s1", "d1", "@[Alice](info:a1) @[Bob](blocked:b1)");
        let diff = tracker.update(
            "s1",
            "d1",
            "@[Alice](info:a1) @[Carol](review_requested:c1)",
        );
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].display_name, "Carol");
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].display_name, "Bob");
    }

    #[test]
    fn test_tracker_remove_clears_state() {
        let tracker = InDocFlagTracker::new();
        tracker.update("s1", "d1", "@[Alice](info:a1)");
        tracker.remove("s1", "d1");
        let diff = tracker.update("s1", "d1", "@[Alice](info:a1)");
        assert_eq!(diff.added.len(), 1);
    }

    /// Build a CREATED mention (flag) signal mirroring the materializer's
    /// output for an added `@[Name](kind:id)` marker.
    fn created_mention_record(document_id: &str, kind: FlagKind, target_id: &str) -> Signal {
        let mut s = Signal {
            id: format!("mention-{}-{target_id}", i32::from(kind)),
            space_id: "5171e0a1-1111-4000-8000-000000000002".into(),
            document_id: Some(document_id.to_owned()),
            record_id: format!("r-{target_id}"),
            // The deprecated audience pair is the shape under test: these fixtures
            // stand in for records already on disk.
            #[allow(deprecated)]
            payload: Some(signal::Payload::Flag(kutl_proto::sync::FlagPayload {
                kind: i32::from(kind),
                audience_type: 0,
                target_did: Some(target_id.to_owned()),
                message: String::new(),
                audience: None,
                anchor_text: None,
            })),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: 1,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        s.set_event(kutl_proto::sync::SignalEventType::Created);
        s
    }

    /// Re-deriving the known set from the CREATED mention records must reproduce
    /// what a fresh `update()` holds, so a subsequent `update()` on unchanged
    /// content diffs empty — an empty known set would spuriously re-add.
    #[test]
    fn test_rederive_then_update_same_content_empty_diff() {
        let content = "hi @[Alice](review_requested:acc-1) and @[Bob](blocked:acc-2)";
        let tracker = InDocFlagTracker::new();
        let first = tracker.update("s1", "d1", content);
        assert_eq!(first.added.len(), 2, "baseline added: {:?}", first.added);

        tracker.remove("s1", "d1");
        let records = vec![
            created_mention_record("d1", FlagKind::ReviewRequested, "acc-1"),
            created_mention_record("d1", FlagKind::Blocked, "acc-2"),
        ];
        tracker.rederive_from("s1", "d1", &records);

        let diff = tracker.update("s1", "d1", content);
        assert!(diff.added.is_empty(), "spurious added: {:?}", diff.added);
        assert!(
            diff.removed.is_empty(),
            "spurious removed: {:?}",
            diff.removed
        );
    }

    /// A closed (removed) mention is NOT re-derived as known: re-deriving from a
    /// CREATED+CLOSED pair then updating to the mention-free content produces no
    /// spurious removed.
    #[test]
    fn test_rederive_closed_mention_absent() {
        let tracker = InDocFlagTracker::new();
        let created = created_mention_record("d1", FlagKind::Info, "acc-9");
        let mut closed = Signal {
            id: created.id.clone(),
            space_id: "5171e0a1-1111-4000-8000-000000000002".into(),
            document_id: Some("d1".into()),
            record_id: "r-close".into(),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: 2,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        closed.set_event(kutl_proto::sync::SignalEventType::Closed);
        closed.set_close_reason(kutl_proto::sync::CloseReason::Withdrawn);
        tracker.rederive_from("s1", "d1", &[created, closed]);

        let diff = tracker.update("s1", "d1", "no mentions here");
        assert!(
            diff.removed.is_empty(),
            "closed mention re-derived: {:?}",
            diff.removed
        );
        assert!(diff.added.is_empty());
    }

    /// Comment-kind flag records are the `CommentTracker`'s; the in-doc flag
    /// tracker must ignore them when re-deriving.
    #[test]
    fn test_rederive_ignores_comment_kind_flags() {
        let tracker = InDocFlagTracker::new();
        let comment = created_mention_record("d1", FlagKind::Comment, "acc-c");
        tracker.rederive_from("s1", "d1", &[comment]);
        // Adding a real mention still surfaces as added — the comment record
        // did not pollute the known set.
        let diff = tracker.update("s1", "d1", "@[Al](info:acc-1)");
        assert_eq!(
            diff.added.len(),
            1,
            "comment flag leaked into known set: {:?}",
            diff.added
        );
    }

    #[test]
    fn test_tracker_skips_markers_inside_fence() {
        let tracker = InDocFlagTracker::new();
        let content = "@[Alice](info:a1) outside\n```\n@[Bob](blocked:b1) inside fence\n```\n@[Carol](review_requested:c1) also outside";
        let diff = tracker.update("s1", "d1", content);
        // Only Alice and Carol should be detected — Bob is inside the fence.
        assert_eq!(diff.added.len(), 2);
        let names: Vec<&str> = diff.added.iter().map(|m| m.display_name.as_str()).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Carol"));
        assert!(!names.contains(&"Bob"));
    }
}
