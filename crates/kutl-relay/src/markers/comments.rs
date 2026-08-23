//! Comment marker parser and lifecycle tracker.
//!
//! Parses inline comment markers `[anchored text]{.cmt #signal-uuid}`
//! from document text and diffs against a known set to produce:
//! - `removed` — markers that disappeared between merges.
//! - `emptied` — markers whose wrapped text became empty
//!   (`[]{.cmt #signal-uuid}`), a withdrawal cue.
//! - `edited` — markers whose wrapped text changed to a different
//!   non-empty value; an EDITED record carrying the anchor as it reads
//!   now.
//!
//! Signal creation is owned by the editor's explicit `CreateFlag` RPC
//! — this tracker never emits a `comment_added` event (parallels the
//! in-doc-flag-marker pattern but with the marker carrying no
//! synthesizable body).

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex};

use kutl_proto::sync::{FlagKind, Signal, signal};
use kutl_signals::fold::{SignalStatus, SpaceSignalState};

/// A parsed comment marker from document text.
#[derive(Debug, Clone)]
pub struct CommentMarker {
    /// Signal UUID (lowercase). Hash + equality key.
    pub signal_id: String,
    /// Wrapped text at parse time. Feeds the empty-span (withdrawal) and
    /// changed-span (EDITED) detections; the canonical anchor is
    /// the record log's content track — the latest EDITED's `anchor_text`,
    /// else the CREATED's.
    pub anchor_text: String,
}

impl PartialEq for CommentMarker {
    fn eq(&self, other: &Self) -> bool {
        self.signal_id == other.signal_id
    }
}

impl Eq for CommentMarker {}

impl std::hash::Hash for CommentMarker {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signal_id.hash(state);
    }
}

/// Compiled regex for `[text]{.cmt #signal-uuid}`.
///
/// UUID-strict (8-4-4-4-12 hex) and case-insensitive — server-side
/// parsing must match the client's `comment-syntax.ts` recognizer.
static COMMENT_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r"(?i)\[([^\]]*)\]\{\.cmt #([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\}",
    )
    .expect("valid regex")
});

/// Parse all `[text]{.cmt #uuid}` markers from document text.
///
/// Skips markers inside fenced code blocks (consistent with
/// `InDocFlagTracker`'s `code_fence::skip_fenced_lines`).
pub fn parse_comment_markers(content: &str) -> HashSet<CommentMarker> {
    COMMENT_RE
        .captures_iter(content)
        .map(|cap| CommentMarker {
            anchor_text: cap[1].to_owned(),
            signal_id: cap[2].to_lowercase(),
        })
        .collect()
}

/// Diff between two marker sets for a document.
#[derive(Debug)]
pub struct CommentDiff {
    /// Markers present in `previous` but not in `current` (deleted).
    pub removed: Vec<CommentMarker>,
    /// Markers present in both, where the wrapped text was non-empty
    /// before and is empty now. Treated as a withdrawal cue.
    pub emptied: Vec<CommentMarker>,
    /// Markers present in both whose wrapped text changed to a different
    /// NON-empty value (the empty outcome is [`Self::emptied`]'s). Each
    /// becomes an EDITED record carrying the anchor as it reads now —
    /// without that record the projected anchor text would go stale
    /// forever.
    pub edited: Vec<CommentMarker>,
}

/// Placeholder wrapped text for a re-derived comment marker whose records
/// carry NO anchor to restore (payloads predating the
/// record-owned `anchor_text` field). A NON-empty placeholder is used deliberately:
/// the emptied-transition ([`CommentDiff::emptied`]) fires only on a
/// non-empty→empty edit, so seeding "was non-empty" preserves that contract —
/// a genuine later emptying still fires — while never spuriously emitting one
/// (the diff needs the CURRENT text to be empty, which unchanged content is
/// not). The edited-detection treats it as content-unknown and absorbs
/// rather than emits ([`CommentDiff::edited`]).
const REDERIVED_ANCHOR_PLACEHOLDER: &str = "\u{0}rederived";

/// Tracks known comment markers per document and computes lifecycle diffs.
///
/// **Does not emit `added` events.** Signal creation is owned by the
/// editor's explicit `CreateFlag` RPC; the body lives in the signal
/// only and cannot be synthesized from the marker.
///
/// **Eviction / rejoin.** [`Self::remove`] drops a document's state on
/// eviction; [`Self::rederive_from`] rebuilds it from the folded records on
/// re-observe, so a marker removed during the eviction window is not silently
/// missed (the re-derived known-set reflects the ACTUALLY-appended records, and
/// the next merge diffs against it). `InDocFlagTracker` and `DecisionTracker`
/// share the same shape and the same pair of hooks.
///
/// **Growth bound.** Absent an eviction call, state accumulates per
/// `(space_id, document_id)` for the relay's lifetime — memory grows linearly
/// with observed-doc count, acceptable for a relay restarted on every deploy.
pub struct CommentTracker {
    /// Per-document known markers keyed by `(space_id, document_id)`.
    /// The full struct is stored (not just the signal id) so the
    /// emptied-transition can compare prior vs. current wrapped text.
    known: Mutex<HashMap<(String, String), HashMap<String, CommentMarker>>>,
}

impl Default for CommentTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CommentTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            known: Mutex::new(HashMap::new()),
        }
    }

    /// Update the known set for a document and return the diff.
    ///
    /// On first call for a document, markers in `content` are recorded
    /// as known but produce no diff entries (no prior state to compare
    /// against).
    pub fn update(&self, space_id: &str, document_id: &str, content: &str) -> CommentDiff {
        let stripped = crate::markers::code_fence::skip_fenced_lines(content);
        let current_set = parse_comment_markers(&stripped);
        let mut current_by_id: HashMap<String, CommentMarker> = HashMap::new();
        for m in current_set {
            current_by_id.insert(m.signal_id.clone(), m);
        }

        let mut known = self.known.lock().expect("comment tracker lock poisoned");
        let key = (space_id.to_owned(), document_id.to_owned());
        let previous = known.get(&key).cloned().unwrap_or_default();

        let mut removed = Vec::new();
        let mut emptied = Vec::new();
        let mut edited = Vec::new();

        for (id, prev) in &previous {
            match current_by_id.get(id) {
                None => removed.push(prev.clone()),
                Some(cur) => {
                    if !prev.anchor_text.is_empty() && cur.anchor_text.is_empty() {
                        emptied.push(cur.clone());
                    } else if !prev.anchor_text.is_empty()
                        && cur.anchor_text != prev.anchor_text
                        && prev.anchor_text != REDERIVED_ANCHOR_PLACEHOLDER
                    {
                        // `!prev.is_empty()`: an empty previous span means the
                        // marker was EMPTIED — the signal is withdrawn, and a
                        // refill (e.g. an undo) must not announce an edit on a
                        // closed comment. Absorbed silently, like the
                        // content-unknown placeholder below.
                        // A changed non-empty span is a content edit.
                        // The placeholder guard: a re-derived entry
                        // whose record carried no anchor is content-unknown,
                        // and comparing against it would emit a phantom
                        // EDITED on the first merge after every restart.
                        edited.push(cur.clone());
                    }
                }
            }
        }

        known.insert(key, current_by_id);
        CommentDiff {
            removed,
            emptied,
            edited,
        }
    }

    /// Remove tracking state for a document (e.g. on eviction).
    pub fn remove(&self, space_id: &str, document_id: &str) {
        self.known
            .lock()
            .expect("comment tracker lock poisoned")
            .remove(&(space_id.to_owned(), document_id.to_owned()));
    }

    /// Rebuild the known set for a document from the folded signal records.
    ///
    /// Called on doc re-observe after an eviction (see [`Self::remove`]): the
    /// known set is reconstructed from the records that ACTUALLY materialized so
    /// a comment removed while the doc was evicted is not silently missed, and a
    /// subsequent [`Self::update`] on unchanged content yields an empty diff.
    /// Because it folds only appended records, a comment whose CREATED never
    /// landed is absent here.
    ///
    /// Comment signals are `Comment`-kind flag records whose id IS the marker
    /// uuid. A folded comment that is `Open` becomes a known marker;
    /// a `Closed`(withdrawn) or `Tombstoned` one leaves no entry. The wrapped
    /// text is unrecoverable from the record (see
    /// [`REDERIVED_ANCHOR_PLACEHOLDER`]), so a non-empty placeholder is seeded.
    /// `records` may hold signals for other documents / kinds; only comment
    /// signals belonging to `document_id` are folded in.
    pub fn rederive_from(&self, space_id: &str, document_id: &str, records: &[Signal]) {
        // Fold every record for this document — transitions (CLOSED) carry NO
        // payload, so they cannot be pre-filtered by kind; the fold parks orphan
        // transitions until their CREATED lands. Comment classification happens
        // below, off the folded CREATED payload.
        let mut fold = SpaceSignalState::default();
        for record in records {
            if record.document_id.as_deref() == Some(document_id) {
                fold.apply(record.clone());
            }
        }

        let mut rebuilt: HashMap<String, CommentMarker> = HashMap::new();
        for (id, state) in fold.iter() {
            if state.status != SignalStatus::Open || !is_comment_signal(&state.created) {
                continue; // non-comment, withdrawn, or tombstoned → no known entry
            }
            // The anchor is restored from the fold's CONTENT track when the
            // record carries one (an anchor-bearing CREATED, and any EDITED
            // by construction), so the next update can tell a
            // real anchor edit from unchanged content. The placeholder is the
            // fallback for records that predate anchor-bearing payloads —
            // those entries stay content-unknown and the differ absorbs their
            // first observed text silently.
            let anchor = match &state.content().payload {
                Some(signal::Payload::Flag(f)) => f.anchor_text.clone(),
                _ => None,
            };
            rebuilt.insert(
                id.clone(),
                CommentMarker {
                    signal_id: id.clone(),
                    anchor_text: anchor.unwrap_or_else(|| REDERIVED_ANCHOR_PLACEHOLDER.to_owned()),
                },
            );
        }

        let key = (space_id.to_owned(), document_id.to_owned());
        self.known
            .lock()
            .expect("comment tracker lock poisoned")
            .insert(key, rebuilt);
    }
}

/// Whether a record is a comment signal: a `Comment`-kind flag payload.
///
/// Comment markers are created as `create_flag(kind=comment, …)` and a
/// comment stays a FLAG KIND rather than becoming its own signal type
/// — the reason it exists at all is as the single construct an import gets
/// from a source system that has no other flag kinds. So this stays a `kind`
/// check, and it is what distinguishes a comment from an in-doc mention flag.
fn is_comment_signal(record: &Signal) -> bool {
    matches!(
        &record.payload,
        Some(signal::Payload::Flag(flag)) if flag.kind == i32::from(FlagKind::Comment)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const U1: &str = "11111111-1111-1111-1111-111111111111";
    const U2: &str = "22222222-2222-2222-2222-222222222222";

    #[test]
    fn test_parse_single_marker() {
        let content = format!("before [wrapped]{{.cmt #{U1}}} after");
        let markers = parse_comment_markers(&content);
        assert_eq!(markers.len(), 1);
        let m = markers.iter().next().unwrap();
        assert_eq!(m.signal_id, U1);
        assert_eq!(m.anchor_text, "wrapped");
    }

    #[test]
    fn test_parse_lowercases_uuid() {
        let content = "[x]{.cmt #AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE}";
        let markers = parse_comment_markers(content);
        let m = markers.iter().next().unwrap();
        assert_eq!(m.signal_id, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    }

    #[test]
    fn test_parse_rejects_malformed_uuid() {
        let content = "[x]{.cmt #not-a-uuid}";
        assert!(parse_comment_markers(content).is_empty());
    }

    #[test]
    fn test_parse_rejects_wrong_class() {
        let content = format!("[x]{{.note #{U1}}}");
        assert!(parse_comment_markers(&content).is_empty());
    }

    #[test]
    fn test_parse_multiple() {
        let content = format!("[a]{{.cmt #{U1}}} [b]{{.cmt #{U2}}}");
        assert_eq!(parse_comment_markers(&content).len(), 2);
    }

    #[test]
    fn test_parse_empty_wrapped_text() {
        let content = format!("[]{{.cmt #{U1}}}");
        let markers = parse_comment_markers(&content);
        assert_eq!(markers.len(), 1);
        assert_eq!(markers.iter().next().unwrap().anchor_text, "");
    }

    #[test]
    fn test_tracker_first_update_no_diff() {
        let tracker = CommentTracker::new();
        let diff = tracker.update("s", "d", &format!("[x]{{.cmt #{U1}}}"));
        assert!(diff.removed.is_empty());
        assert!(diff.emptied.is_empty());
    }

    #[test]
    fn test_tracker_emits_removed() {
        let tracker = CommentTracker::new();
        tracker.update("s", "d", &format!("[x]{{.cmt #{U1}}}"));
        let diff = tracker.update("s", "d", "");
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].signal_id, U1);
        assert!(diff.emptied.is_empty());
    }

    #[test]
    fn test_tracker_emits_emptied_on_inner_text_empty() {
        let tracker = CommentTracker::new();
        tracker.update("s", "d", &format!("[wrapped]{{.cmt #{U1}}}"));
        let diff = tracker.update("s", "d", &format!("[]{{.cmt #{U1}}}"));
        assert!(diff.removed.is_empty());
        assert_eq!(diff.emptied.len(), 1);
        assert_eq!(diff.emptied[0].signal_id, U1);
    }

    #[test]
    fn test_tracker_no_event_when_marker_starts_empty() {
        // A marker that was empty from the first observation never
        // transitions to "emptied". The editor wouldn't create that
        // shape — but be defensive.
        let tracker = CommentTracker::new();
        tracker.update("s", "d", &format!("[]{{.cmt #{U1}}}"));
        let diff = tracker.update("s", "d", &format!("[]{{.cmt #{U1}}}"));
        assert!(diff.removed.is_empty());
        assert!(diff.emptied.is_empty());
    }

    #[test]
    fn test_tracker_removed_and_emptied_coexist() {
        let tracker = CommentTracker::new();
        tracker.update("s", "d", &format!("[a]{{.cmt #{U1}}} [b]{{.cmt #{U2}}}"));
        // U1 disappears; U2 keeps its marker but is emptied.
        let diff = tracker.update("s", "d", &format!("[]{{.cmt #{U2}}}"));
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].signal_id, U1);
        assert_eq!(diff.emptied.len(), 1);
        assert_eq!(diff.emptied[0].signal_id, U2);
    }

    #[test]
    fn test_tracker_anchor_text_edit_with_nonempty_result_emits_edited() {
        // A non-empty → non-empty anchor change is content and must be
        // recorded (`edited` → an EDITED record); the lifecycle
        // buckets stay silent.
        let tracker = CommentTracker::new();
        tracker.update("s", "d", &format!("[before]{{.cmt #{U1}}}"));
        let diff = tracker.update("s", "d", &format!("[after]{{.cmt #{U1}}}"));
        assert!(diff.removed.is_empty());
        assert!(diff.emptied.is_empty());
        assert_eq!(diff.edited.len(), 1, "diff.edited must carry the change");
        assert_eq!(diff.edited[0].anchor_text, "after");
        assert_eq!(diff.edited[0].signal_id, U1);
    }

    #[test]
    fn test_tracker_unchanged_anchor_emits_no_edited() {
        let tracker = CommentTracker::new();
        let content = format!("[same]{{.cmt #{U1}}}");
        tracker.update("s", "d", &content);
        let diff = tracker.update("s", "d", &content);
        assert!(diff.edited.is_empty(), "diff.edited: {:?}", diff.edited);
    }

    #[test]
    fn test_tracker_refill_after_emptied_emits_nothing() {
        // Emptying withdrew the signal; refilling the span (e.g. an undo)
        // must not announce an edit on a CLOSED comment — there is no reopen
        // path for comments, so the edit would be a phantom on a withdrawn
        // signal. The known-set still absorbs the new text.
        let tracker = CommentTracker::new();
        tracker.update("s", "d", &format!("[text]{{.cmt #{U1}}}"));
        let emptied = tracker.update("s", "d", &format!("[]{{.cmt #{U1}}}"));
        assert_eq!(emptied.emptied.len(), 1);
        let refilled = tracker.update("s", "d", &format!("[revived]{{.cmt #{U1}}}"));
        assert!(
            refilled.edited.is_empty(),
            "a refill on a withdrawn signal must not emit: {refilled:?}"
        );
        assert!(refilled.emptied.is_empty());
    }

    /// Re-derivation restores the anchor from the record's content track, so
    /// a restart does not blind the edited-detection.
    #[test]
    fn test_rederive_restores_anchor_for_edit_detection() {
        let tracker = CommentTracker::new();
        let mut record = created_comment_record("d", U1);
        if let Some(signal::Payload::Flag(f)) = &mut record.payload {
            f.anchor_text = Some("the original span".into());
        }
        tracker.rederive_from("s", "d", std::slice::from_ref(&record));

        // Unchanged content: no phantom edit after the restart.
        let diff = tracker.update("s", "d", &format!("[the original span]{{.cmt #{U1}}}"));
        assert!(
            diff.edited.is_empty(),
            "phantom edit after restart: {:?}",
            diff.edited
        );

        // A real edit across the restart boundary is still seen.
        tracker.remove("s", "d");
        tracker.rederive_from("s", "d", &[record]);
        let diff = tracker.update("s", "d", &format!("[a changed span]{{.cmt #{U1}}}"));
        assert_eq!(diff.edited.len(), 1, "diff.edited: {:?}", diff.edited);
        assert_eq!(diff.edited[0].anchor_text, "a changed span");
    }

    /// A re-derived entry whose records carry NO anchor (the placeholder) is
    /// content-unknown: the first observed text is absorbed silently, and
    /// only a later change emits.
    #[test]
    fn test_rederive_without_anchor_absorbs_then_tracks() {
        let tracker = CommentTracker::new();
        let record = created_comment_record("d", U1); // anchor_text: None
        tracker.rederive_from("s", "d", std::slice::from_ref(&record));

        let diff = tracker.update("s", "d", &format!("[first observed]{{.cmt #{U1}}}"));
        assert!(
            diff.edited.is_empty(),
            "content-unknown must absorb, not emit: {:?}",
            diff.edited
        );

        let diff = tracker.update("s", "d", &format!("[now changed]{{.cmt #{U1}}}"));
        assert_eq!(diff.edited.len(), 1, "diff.edited: {:?}", diff.edited);
    }

    #[test]
    fn test_tracker_remove_clears_state() {
        let tracker = CommentTracker::new();
        tracker.update("s", "d", &format!("[x]{{.cmt #{U1}}}"));
        tracker.remove("s", "d");
        let diff = tracker.update("s", "d", &format!("[x]{{.cmt #{U1}}}"));
        // After remove, the doc is "new" — no diff.
        assert!(diff.removed.is_empty());
        assert!(diff.emptied.is_empty());
    }

    /// Build a CREATED comment signal (a `Comment`-kind flag whose id is the
    /// marker uuid).
    fn created_comment_record(document_id: &str, signal_id: &str) -> Signal {
        let mut s = Signal {
            id: signal_id.to_owned(),
            space_id: "5171e0a1-1111-4000-8000-000000000005".into(),
            document_id: Some(document_id.to_owned()),
            record_id: format!("r-{signal_id}"),
            // The deprecated audience pair is the shape under test: these fixtures
            // stand in for records already on disk.
            #[allow(deprecated)]
            payload: Some(signal::Payload::Flag(kutl_proto::sync::FlagPayload {
                kind: i32::from(FlagKind::Comment),
                audience_type: 0,
                target_did: None,
                message: "the comment body".into(),
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

    /// Re-deriving the known set from an Open comment record keeps the marker
    /// known, so a subsequent `update()` on unchanged content emits no removed
    /// (an empty known set would have missed the marker entirely, and a later
    /// removal would then go unreported).
    #[test]
    fn test_rederive_then_update_same_content_no_spurious_event() {
        let content = format!("here [wrapped span]{{.cmt #{U1}}} note");
        let tracker = CommentTracker::new();
        // Baseline: first update establishes the known marker (no diff yet).
        tracker.update("s", "d", &content);

        tracker.remove("s", "d");
        tracker.rederive_from("s", "d", &[created_comment_record("d", U1)]);

        // Unchanged content: the re-derived known marker matches → no diff.
        let diff = tracker.update("s", "d", &content);
        assert!(
            diff.removed.is_empty(),
            "spurious removed: {:?}",
            diff.removed
        );
        assert!(
            diff.emptied.is_empty(),
            "spurious emptied: {:?}",
            diff.emptied
        );

        // And a genuine removal after re-derivation IS reported — proving the
        // marker was actually re-derived as known (not left absent).
        let removed = tracker.update("s", "d", "note, comment gone");
        assert_eq!(
            removed.removed.len(),
            1,
            "removal must fire: {:?}",
            removed.removed
        );
        assert_eq!(removed.removed[0].signal_id, U1);
    }

    /// A re-derived known comment whose text is later emptied still fires the
    /// emptied transition — the non-empty placeholder preserves the contract.
    #[test]
    fn test_rederive_then_empty_fires_emptied() {
        let tracker = CommentTracker::new();
        tracker.rederive_from("s", "d", &[created_comment_record("d", U1)]);
        let diff = tracker.update("s", "d", &format!("[]{{.cmt #{U1}}}"));
        assert_eq!(
            diff.emptied.len(),
            1,
            "emptied must fire: {:?}",
            diff.emptied
        );
        assert_eq!(diff.emptied[0].signal_id, U1);
    }

    /// A withdrawn (closed) comment must NOT be re-derived as known.
    #[test]
    fn test_rederive_withdrawn_comment_absent() {
        let tracker = CommentTracker::new();
        let created = created_comment_record("d", U1);
        let mut closed = Signal {
            id: U1.to_owned(),
            space_id: "5171e0a1-1111-4000-8000-000000000005".into(),
            document_id: Some("d".into()),
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
        tracker.rederive_from("s", "d", &[created, closed]);

        // The marker is absent from the known set: removing the (already gone)
        // marker from empty content produces no spurious removed.
        let diff = tracker.update("s", "d", "");
        assert!(
            diff.removed.is_empty(),
            "withdrawn comment re-derived: {:?}",
            diff.removed
        );
    }

    #[test]
    fn test_tracker_skips_markers_inside_fence() {
        let tracker = CommentTracker::new();
        let content = format!("[outside]{{.cmt #{U1}}}\n```\n[inside]{{.cmt #{U2}}}\n```\n");
        // First-update establishes the baseline; doesn't emit diff entries.
        tracker.update("s", "d", &content);
        // Now remove all content: U1 should be in removed, U2 was never tracked.
        let diff = tracker.update("s", "d", "");
        let removed_ids: Vec<&str> = diff.removed.iter().map(|m| m.signal_id.as_str()).collect();
        assert!(removed_ids.contains(&U1));
        assert!(!removed_ids.contains(&U2));
    }
}
