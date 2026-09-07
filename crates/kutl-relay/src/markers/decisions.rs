//! Decision parser, differ, and tracker for heading-annotated decisions.
//!
//! Parses `## ? Open question` and `## = Resolved answer` headings from
//! document text, tracks known decisions per document, and computes diffs
//! (opened, resolved, reopened, removed) on each update.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use kutl_proto::sync::{Signal, signal};
use kutl_signals::fold::{SignalStatus, SpaceSignalState};

use crate::markers::code_fence::skip_fenced_lines;
use crate::markers::stable_id::{TITLE_HASH_TAG, decision_signal_id, stable_v5};

/// Marker prefix for open (unresolved) decisions in ATX headings.
pub const DECISION_OPEN_MARKER: &str = "? ";

/// Marker prefix for resolved decisions in ATX headings.
pub const DECISION_RESOLVED_MARKER: &str = "= ";

/// Maximum byte distance for proximity-based matching of renamed decisions.
///
/// The window trades recall against false absorption — an unrelated decision
/// created near where another was deleted in the same edit inherits the dead
/// one's identity, silently — so it cannot be unbounded; memory plays no part
/// in the choice. Paragraph scale: with the title-prefix pass taking the
/// common rename shape, proximity serves whole-title rewrites, which
/// typically arrive with the answer written around the heading — a couple of
/// paragraphs of local drift must not sever identity, while a heading that
/// moved further than this is safer treated as a new decision than absorbed.
const DECISION_PROXIMITY_CHARS: usize = 2048;

/// Regex matching decision headings with `?` or `=` markers.
///
/// Captures: (1) heading hashes, (2) marker char, (3) title text.
/// Built from marker constants. Uses multiline mode so `^` matches
/// start of each line. `\s*` before title allows zero-space after marker.
static DECISION_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    let open = regex::escape(DECISION_OPEN_MARKER.trim_end());
    let resolved = regex::escape(DECISION_RESOLVED_MARKER.trim_end());
    regex::Regex::new(&format!(r"(?m)^(#{{1,6}})\s+({open}|{resolved})\s*(.+)$"))
        .expect("valid decision regex")
});

/// State of a decision heading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionState {
    /// Decision is open / unresolved (`?` marker).
    Open,
    /// Decision is resolved (`=` marker).
    Resolved,
}

/// A parsed decision from document text.
#[derive(Debug, Clone)]
pub struct Decision {
    /// Heading depth (1–6, number of `#` characters).
    pub depth: u8,
    /// Current state of the decision.
    pub state: DecisionState,
    /// Normalized title text (after the marker).
    pub title: String,
    /// Hash of the normalized title (marker-independent).
    pub title_hash: u64,
    /// Hash of the parent decision in the heading hierarchy, if any.
    pub parent_hash: Option<u64>,
    /// Character offset of the heading line in the (fence-stripped) content.
    pub offset: usize,
}

/// Normalize a decision title by trimming and collapsing internal whitespace.
pub fn normalize_title(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Compute a stable hash for a normalized decision title.
///
/// The value is DURABLE: it is persisted in a decision record's
/// `DecisionPayload.title_hash` and re-parsed on tracker re-derivation, so it
/// MUST NOT change across toolchains. It is derived from a SPEC-PINNED
/// RFC-4122 v5 (SHA-1) UUID over the normalized title (see
/// [`crate::markers::stable_id`]) — NOT `DefaultHasher`, whose algorithm std
/// does not guarantee across Rust releases. The `u64` is the v5 UUID's first
/// eight bytes, little-endian; the return type stays `u64` so the existing
/// `.to_string()` / `.parse::<u64>()` round-trip and hierarchy comparisons are
/// unchanged.
pub fn hash_title(title: &str) -> u64 {
    let normalized = normalize_title(title);
    let id = stable_v5(TITLE_HASH_TAG, &[&normalized]);
    let mut first_eight = [0u8; 8];
    first_eight.copy_from_slice(&id.as_bytes()[..8]);
    u64::from_le_bytes(first_eight)
}

/// Parse all decision headings from document content.
///
/// Content should be pre-processed with [`skip_fenced_lines`] to avoid
/// matching headings inside code blocks. Parent tracking uses a depth
/// stack: each decision's parent is the nearest ancestor at a shallower
/// heading level.
pub fn parse_decisions(content: &str) -> Vec<Decision> {
    let mut decisions = Vec::new();
    // Stack of (depth, title_hash) for parent tracking.
    let mut parent_stack: Vec<(u8, u64)> = Vec::new();

    for cap in DECISION_RE.captures_iter(content) {
        let hashes = &cap[1];
        // Regex ensures 1–6 hashes, so truncation is impossible.
        #[allow(clippy::cast_possible_truncation)]
        let depth = hashes.len() as u8;
        let marker = &cap[2];
        let raw_title = &cap[3];

        let state = if marker == "?" {
            DecisionState::Open
        } else {
            DecisionState::Resolved
        };

        let title = raw_title.trim().to_owned();
        let title_hash = hash_title(&title);

        // Pop entries at depth >= current to find the parent.
        while parent_stack.last().is_some_and(|(d, _)| *d >= depth) {
            parent_stack.pop();
        }
        let parent_hash = parent_stack.last().map(|(_, h)| *h);
        parent_stack.push((depth, title_hash));

        // Compute character offset of the match start.
        let offset = cap.get(0).expect("full match always exists").start();

        decisions.push(Decision {
            depth,
            state,
            title,
            title_hash,
            parent_hash,
            offset,
        });
    }

    decisions
}

/// Outcome of [`splice_decision_marker`].
#[derive(Debug)]
pub enum MarkerSplice {
    /// The document content with the required change applied: the heading's
    /// marker flipped, the note inserted under it, or both.
    Updated(String),
    /// The heading already reads the target marker and no note was given —
    /// nothing to change.
    AlreadyInTargetState,
    /// No decision heading with this title hash exists in the document.
    NotFound,
}

/// Flip one decision heading's marker (`?` ↔ `=`) in raw document content.
///
/// `title_hash` names the heading by its CURRENT title (the fold's content
/// track after any rename), and `target` is the state the heading should read
/// after the splice. `note`, when given, always lands: it is inserted as a
/// BLOCKQUOTED paragraph directly under the heading — even when the heading
/// already reads the target marker, because a caller's rationale must not be
/// dropped behind a success. Never into the title, so the heading's identity
/// is untouched by construction.
///
/// The blockquote is not presentation: this content is re-parsed by every
/// marker tracker on the same merge, and a `> ` prefix is what keeps
/// line-anchored grammar in the note inert — a stray triple-backtick line
/// cannot open a fence that blanks (and thereby withdraws) every marker
/// below it, and a `## ?` line cannot mint a decision. The note text itself
/// is preserved verbatim inside the quote.
///
/// The parse runs on fence-stripped content (so a look-alike heading inside a
/// code block cannot be the target), but the returned content is the RAW input
/// with only the splice applied. [`skip_fenced_lines`] preserves line count
/// and passes non-fenced lines through verbatim, so the heading's line index
/// in the stripped text addresses the identical line in the raw text.
#[must_use]
pub fn splice_decision_marker(
    content: &str,
    title_hash: u64,
    target: DecisionState,
    note: Option<&str>,
) -> MarkerSplice {
    let stripped = skip_fenced_lines(content);
    let Some(decision) = parse_decisions(&stripped)
        .into_iter()
        .find(|d| d.title_hash == title_hash)
    else {
        return MarkerSplice::NotFound;
    };
    let flip_needed = decision.state != target;
    if !flip_needed && note.is_none() {
        return MarkerSplice::AlreadyInTargetState;
    }

    // The heading's line index in the stripped text addresses the same line in
    // the raw text (line count preserved, non-fenced lines verbatim).
    let line_index = stripped[..decision.offset].matches('\n').count();
    let mut line_start = 0;
    let mut heading_line = "";
    for (i, line) in content.split_inclusive('\n').enumerate() {
        if i == line_index {
            heading_line = line;
            break;
        }
        line_start += line.len();
    }

    // Within the heading line, the marker is the single ASCII byte after the
    // hashes and their whitespace — the same structure the parse regex just
    // matched, so a miss here means the line mapping broke, not the document.
    let after_hashes = heading_line.trim_start_matches('#');
    let after_ws = after_hashes.trim_start();
    if !after_ws.starts_with(['?', '=']) {
        return MarkerSplice::NotFound;
    }

    let mut new_content = content.to_owned();
    if flip_needed {
        let marker_pos = line_start + (heading_line.len() - after_ws.len());
        let flipped_marker = match target {
            DecisionState::Open => "?",
            DecisionState::Resolved => "=",
        };
        new_content.replace_range(marker_pos..=marker_pos, flipped_marker);
    }

    if let Some(note) = note {
        let quoted = blockquote(note);
        let line_end = line_start + heading_line.len();
        if heading_line.ends_with('\n') {
            new_content.insert_str(line_end, &format!("\n{quoted}\n"));
        } else {
            // Heading is the last line with no terminator: give it one, then
            // the note as its own paragraph.
            new_content.push_str("\n\n");
            new_content.push_str(&quoted);
        }
    }

    MarkerSplice::Updated(new_content)
}

/// Prefix every line of `text` with a markdown blockquote marker (`> `, bare
/// `>` on empty lines). See [`splice_decision_marker`] for why quoting is
/// load-bearing rather than cosmetic.
fn blockquote(text: &str) -> String {
    text.lines()
        .map(|line| {
            if line.is_empty() {
                ">".to_owned()
            } else {
                format!("> {line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The [`DecisionState`] a folded decision signal implies for its heading, or
/// `None` when the heading no longer exists (withdrawn or tombstoned).
///
/// The single reading of fold status shared by tracker re-derivation and the
/// transition path's flip, so the two cannot disagree on what a fold status
/// means: `Open` is unconditionally an open heading (a born-`=` decision
/// folds to `Closed(Resolved)`, never `Open`); `Closed` with reason RESOLVED
/// is a resolved heading; any other close, or a tombstone, means the heading
/// is gone.
#[must_use]
pub fn decision_state_from_fold(
    status: &SignalStatus,
    close_reason: Option<i32>,
) -> Option<DecisionState> {
    let resolved = matches!(
        close_reason.and_then(|r| kutl_proto::sync::CloseReason::try_from(r).ok()),
        Some(kutl_proto::sync::CloseReason::Resolved)
    );
    match status {
        SignalStatus::Open => Some(DecisionState::Open),
        SignalStatus::Closed if resolved => Some(DecisionState::Resolved),
        SignalStatus::Closed | SignalStatus::Tombstoned => None,
    }
}

/// A decision event emitted as part of a diff.
#[derive(Debug, Clone)]
pub struct DecisionEvent {
    /// The signal id this decision has been known by since it was first seen.
    ///
    /// Minted from `(document_id, title_hash)` at birth and then carried across
    /// every later diff — NOT re-derived from the current title. A resolution
    /// that also rewrites the heading changes `title_hash`, so re-deriving would
    /// address a signal that was never created and the CLOSE would orphan.
    /// Every consumer (the materializer's record, a host's feed event) names the
    /// decision by this id.
    pub signal_id: String,
    /// Current title text.
    pub title: String,
    /// Previous title if the decision was renamed.
    pub old_title: Option<String>,
    /// Current decision state.
    pub state: DecisionState,
    /// Hash of the current title.
    pub title_hash: u64,
    /// Hash of the previous title if renamed.
    pub old_title_hash: Option<u64>,
    /// Parent decision hash, if any.
    pub parent_hash: Option<u64>,
    /// Heading depth (1–6).
    pub depth: u8,
    /// Character offset in the document.
    pub offset: usize,
}

/// Diff between two snapshots of decisions in a document.
#[derive(Debug, Default)]
pub struct DecisionDiff {
    /// Newly appeared open decisions (a first-seen `## ? …` heading).
    pub opened: Vec<DecisionEvent>,
    /// First-seen decisions authored DIRECTLY as resolved (a `## = …` heading
    /// with no prior open form). These are *born* resolved: the materializer
    /// emits each as a `CREATED` carrying `close_reason=RESOLVED`,
    /// NOT a bare CLOSE — a CLOSE with no CREATED orphan-parks in the
    /// fold forever. Distinct from [`Self::resolved`], which is the `? → =`
    /// edit transition on an already-created signal.
    pub born_resolved: Vec<DecisionEvent>,
    /// Decisions that transitioned from open to resolved (the `? → =` edit) —
    /// a CLOSE(RESOLVED) on the existing signal.
    pub resolved: Vec<DecisionEvent>,
    /// Decisions that transitioned from resolved back to open.
    pub reopened: Vec<DecisionEvent>,
    /// Decisions that were present before but are now gone.
    pub removed: Vec<DecisionEvent>,
    /// Decisions whose TITLE changed while identity held — a proximity match,
    /// which by construction failed the title-hash pass. Content, not
    /// lifecycle: each becomes an EDITED record carrying the payload as it
    /// reads now, independent of any state transition the same
    /// merge also produced. Dropping a rename instead would leave the
    /// projected title stale forever.
    pub renamed: Vec<DecisionEvent>,
}

/// Internal snapshot of a known decision for diffing.
#[derive(Debug, Clone)]
struct KnownDecision {
    /// The decision's durable signal id — see [`DecisionEvent::signal_id`].
    /// Carried forward on every match so a rename never re-keys it.
    signal_id: String,
    title: String,
    title_hash: u64,
    state: DecisionState,
    depth: u8,
    offset: usize,
    parent_hash: Option<u64>,
}

/// Tracks known decisions per document and computes diffs on update.
pub struct DecisionTracker {
    /// Known decisions keyed by `(space_id, document_id)`.
    known: Mutex<HashMap<(String, String), Vec<KnownDecision>>>,
}

impl Default for DecisionTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl DecisionTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            known: Mutex::new(HashMap::new()),
        }
    }

    /// Update the known decisions for a document and return the diff.
    ///
    /// Internally applies code-fence stripping before parsing. On first
    /// call for a document, all parsed decisions appear as opened/resolved.
    pub fn update(&self, space_id: &str, document_id: &str, content: &str) -> DecisionDiff {
        let stripped = skip_fenced_lines(content);
        let current = parse_decisions(&stripped);

        let mut known_guard = self.known.lock().expect("decision tracker lock poisoned");
        let key = (space_id.to_owned(), document_id.to_owned());
        let previous = known_guard.get(&key).cloned().unwrap_or_default();

        // The new known-set comes OUT of the diff rather than straight off the
        // parse: matching is what says which parsed heading is which known
        // decision, and therefore which signal id each one keeps.
        let (diff, new_known) = compute_diff(document_id, &previous, &current);
        known_guard.insert(key, new_known);

        diff
    }

    /// Remove tracking state for a document (e.g., on eviction).
    pub fn remove(&self, space_id: &str, document_id: &str) {
        self.known
            .lock()
            .expect("decision tracker lock poisoned")
            .remove(&(space_id.to_owned(), document_id.to_owned()));
    }

    /// Rebuild the known-set for a document from the folded signal records.
    ///
    /// Called on doc re-observe after an eviction (see [`Self::remove`]): the
    /// known-set is reconstructed from the records that ACTUALLY materialized,
    /// so a subsequent [`Self::update`] on unchanged content yields an empty
    /// diff instead of spuriously re-opening every decision. Because it folds
    /// only appended records, a decision whose CREATED never landed (e.g. the
    /// append hard-failed after the tracker advanced) is simply absent here and
    /// gets re-emitted on the next merge — the intended recovery path.
    ///
    /// Reconstruction of each known state from a folded decision signal:
    /// - status `Open` → an open decision. Resolved-ness lives entirely in the
    ///   lifecycle status (the payload carries no `state` field): a
    ///   born-`=` decision folds to `Closed(Resolved)`, never to `Open`, so an
    ///   `Open` status is unconditionally an open heading;
    /// - status `Closed` with reason `RESOLVED` → resolved — covers BOTH the
    ///   `? → =` edit (a CLOSE transition) AND a born-`=` decision (a CREATED
    ///   carrying `close_reason=RESOLVED`, whose fold seeds status Closed);
    /// - status `Closed` with any other reason (a WITHDRAWN removal) or status
    ///   `Tombstoned` → the heading is gone, so the entry is absent.
    ///
    /// `records` may hold signals for other documents / kinds; only decision
    /// signals belonging to `document_id` are folded in.
    pub fn rederive_from(&self, space_id: &str, document_id: &str, records: &[Signal]) {
        // Fold every record for this document — transitions (CLOSED/REOPENED)
        // carry NO payload, so they cannot be pre-filtered by kind; the fold
        // parks orphan transitions until their CREATED lands. Classification by
        // kind happens below, off the folded CREATED payload.
        let mut fold = SpaceSignalState::default();
        for record in records {
            if record.document_id.as_deref() == Some(document_id) {
                fold.apply(record.clone());
            }
        }

        let mut rebuilt = Vec::new();
        for (signal_id, state) in fold.iter() {
            // Classified by the BIRTH payload (kind is identity), but the
            // title/hash/depth are read from the fold's CONTENT track: after a
            // rename the known-set must carry the title as it reads NOW, or
            // the first post-restart parse mismatches the hash and emits a
            // phantom rename-EDITED for every renamed decision.
            let Some(signal::Payload::Decision(_)) = &state.created.payload else {
                continue;
            };
            let Some(signal::Payload::Decision(payload)) = &state.content().payload else {
                continue;
            };
            // The fold key IS the durable signal id, so re-derivation restores
            // the identity the decision was actually created under — even when
            // the heading has since been renamed and no longer derives to it.
            let Some(known) =
                known_decision_from_fold(signal_id, payload, &state.status, state.close_reason())
            else {
                continue;
            };
            rebuilt.push(known);
        }

        let key = (space_id.to_owned(), document_id.to_owned());
        self.known
            .lock()
            .expect("decision tracker lock poisoned")
            .insert(key, rebuilt);
    }
}

/// Reconstruct one known decision from its folded signal, or `None` when the
/// decision no longer exists in the document (withdrawn or tombstoned).
///
/// `offset` is reset to `0`: the fold does not retain the heading's character
/// offset, and offset only participates in the differ's proximity pass — which
/// runs on entries that FAILED the title-hash pass. A re-derived entry always
/// carries the exact `title_hash`, so [`compute_diff`] matches it in pass 1 and
/// never consults offset.
fn known_decision_from_fold(
    signal_id: &str,
    payload: &kutl_proto::sync::DecisionPayload,
    status: &SignalStatus,
    close_reason: Option<i32>,
) -> Option<KnownDecision> {
    // Status is the sole authority on state — the payload carries no `state`
    // field. The mapping lives in `decision_state_from_fold`, shared with the
    // transition path's flip.
    let state = decision_state_from_fold(status, close_reason)?;

    let title_hash = payload.title_hash.parse::<u64>().ok()?;
    let parent_hash = payload
        .parent_hash
        .as_ref()
        .and_then(|h| h.parse::<u64>().ok());
    // depth is a heading level (1–6); a negative or oversized value cannot come
    // from the parser, so clamp defensively rather than panicking.
    let depth = u8::try_from(payload.depth).unwrap_or(1);

    Some(KnownDecision {
        signal_id: signal_id.to_owned(),
        title: payload.title.clone(),
        title_hash,
        state,
        depth,
        offset: 0,
        parent_hash,
    })
}

/// Compute the diff between previous known decisions and current parsed
/// decisions, and the new known-set to store.
///
/// Three-pass matching, cheapest and most certain first:
/// 1. Match by title hash (handles moves and simple state transitions).
/// 2. Match remaining entries whose title extends or trims a previous one at
///    a word boundary — identity through the resolving edit itself.
/// 3. Match what is still unmatched by offset proximity (title rewrites).
///
/// Returns the new known-set alongside the diff because matching is also what
/// decides IDENTITY: a matched heading keeps the signal id its predecessor
/// carried, and only a genuinely new heading mints one from `document_id` plus
/// its title hash. Rebuilding the known-set straight off the parse instead
/// would silently re-key every renamed decision.
fn compute_diff(
    document_id: &str,
    previous: &[KnownDecision],
    current: &[Decision],
) -> (DecisionDiff, Vec<KnownDecision>) {
    let mut diff = DecisionDiff::default();

    // Track which entries have been matched.
    let mut prev_matched = vec![false; previous.len()];
    // The signal id each current heading inherits, `None` until it matches a
    // known decision (and forever if it is new).
    let mut curr_ids: Vec<Option<String>> = vec![None; current.len()];

    // Pass 1: match by title_hash.
    for (ci, curr) in current.iter().enumerate() {
        for (pi, prev) in previous.iter().enumerate() {
            if !prev_matched[pi] && prev.title_hash == curr.title_hash {
                prev_matched[pi] = true;
                curr_ids[ci] = Some(prev.signal_id.clone());
                // Check for state transition.
                emit_state_transition(&mut diff, prev, curr, None, None);
                // A structure change under the same title is CONTENT too:
                // depth and parent_hash are two of the four columns a
                // decision EDITED replaces, so absorbing them silently would
                // leave the projected decision stale. `old_title`
                // stays `None` — the title did not change.
                if prev.depth != curr.depth || prev.parent_hash != curr.parent_hash {
                    diff.renamed.push(DecisionEvent {
                        signal_id: prev.signal_id.clone(),
                        title: curr.title.clone(),
                        old_title: None,
                        state: curr.state,
                        title_hash: curr.title_hash,
                        old_title_hash: None,
                        parent_hash: curr.parent_hash,
                        depth: curr.depth,
                        offset: curr.offset,
                    });
                }
                break;
            }
        }
    }

    // Pass 2: match unmatched by title prefix at a word boundary — the
    // resolving edit's natural shape (`? Appetizer` → `= Appetizer — mezze
    // spread`), which changes the hash by construction and outruns the
    // proximity window whenever the document grew above the heading.
    match_by_title_prefix(
        &mut diff,
        previous,
        current,
        &mut prev_matched,
        &mut curr_ids,
    );

    // Pass 3: match unmatched by offset proximity.
    match_by_proximity(
        &mut diff,
        previous,
        current,
        &mut prev_matched,
        &mut curr_ids,
    );

    // Every current heading now has an id: inherited if it matched, freshly
    // minted if it is new. Build the known-set to store in the same walk that
    // emits the birth events, so the two can never disagree about identity.
    let mut new_known = Vec::with_capacity(current.len());
    for (ci, curr) in current.iter().enumerate() {
        let matched = curr_ids[ci].is_some();
        let signal_id = curr_ids[ci]
            .take()
            .unwrap_or_else(|| decision_signal_id(document_id, curr.title_hash));
        new_known.push(KnownDecision {
            signal_id: signal_id.clone(),
            title: curr.title.clone(),
            title_hash: curr.title_hash,
            state: curr.state,
            depth: curr.depth,
            offset: curr.offset,
            parent_hash: curr.parent_hash,
        });
        if matched {
            continue;
        }
        let event = DecisionEvent {
            signal_id,
            title: curr.title.clone(),
            old_title: None,
            state: curr.state,
            title_hash: curr.title_hash,
            old_title_hash: None,
            parent_hash: curr.parent_hash,
            depth: curr.depth,
            offset: curr.offset,
        };
        match curr.state {
            DecisionState::Open => diff.opened.push(event),
            // A first-seen `= …` heading is BORN resolved — it routes to the
            // born_resolved bucket (a CREATED with close_reason=RESOLVED), not
            // to `resolved` (the `? → =` edit's CLOSE). Routing it to `resolved`
            // would emit a CLOSE with no CREATED, which orphan-parks forever.
            DecisionState::Resolved => diff.born_resolved.push(event),
        }
    }

    // Unmatched previous → removed decisions. The removal names the id the
    // decision has carried since birth, which is what makes a
    // rename-then-delete close the signal that actually exists.
    for (pi, prev) in previous.iter().enumerate() {
        if prev_matched[pi] {
            continue;
        }
        diff.removed.push(DecisionEvent {
            signal_id: prev.signal_id.clone(),
            title: prev.title.clone(),
            old_title: None,
            state: prev.state,
            title_hash: prev.title_hash,
            old_title_hash: None,
            parent_hash: prev.parent_hash,
            depth: prev.depth,
            offset: prev.offset,
        });
    }

    (diff, new_known)
}

/// Pass 3 of [`compute_diff`]: match still-unmatched headings to the nearest
/// unmatched previous decision within [`DECISION_PROXIMITY_CHARS`].
///
/// A proximity match IS a rename — the heading failed the title-hash pass, so
/// its content changed while its identity held. `renamed` is therefore
/// emitted unconditionally: a same-state rename is an edit, and a
/// resolve-with-rename is an edit AND a close, as two records on their
/// separate tracks.
fn match_by_proximity(
    diff: &mut DecisionDiff,
    previous: &[KnownDecision],
    current: &[Decision],
    prev_matched: &mut [bool],
    curr_ids: &mut [Option<String>],
) {
    for (ci, curr) in current.iter().enumerate() {
        if curr_ids[ci].is_some() {
            continue;
        }
        // Find the nearest unmatched previous decision within proximity.
        let mut best: Option<(usize, usize)> = None;
        for (pi, prev) in previous.iter().enumerate() {
            if prev_matched[pi] {
                continue;
            }
            let dist = curr.offset.abs_diff(prev.offset);
            if dist <= DECISION_PROXIMITY_CHARS && best.is_none_or(|(_, d)| dist < d) {
                best = Some((pi, dist));
            }
        }
        if let Some((pi, _)) = best {
            absorb_rename_match(diff, previous, pi, curr, ci, prev_matched, curr_ids);
        }
    }
}

/// Pass 2 of [`compute_diff`]: match still-unmatched headings whose
/// normalized title extends (or trims) an unmatched previous title at a
/// non-alphanumeric boundary.
///
/// This is what keeps identity through the resolving edit: `? Appetizer`
/// rewritten to `= Appetizer — mezze spread` fails the title-hash pass by
/// construction, and the proximity pass fails with it whenever the document
/// grew above the heading. The boundary requirement is what keeps `Design`
/// from matching `Designated owner`. Longest shared prefix wins; ties keep
/// the first candidate in previous order, matching the proximity pass's
/// determinism. Like a proximity match, a prefix match IS a rename.
fn match_by_title_prefix(
    diff: &mut DecisionDiff,
    previous: &[KnownDecision],
    current: &[Decision],
    prev_matched: &mut [bool],
    curr_ids: &mut [Option<String>],
) {
    for (ci, curr) in current.iter().enumerate() {
        if curr_ids[ci].is_some() {
            continue;
        }
        let curr_norm = normalize_title(&curr.title);
        let mut best: Option<(usize, usize)> = None;
        for (pi, prev) in previous.iter().enumerate() {
            if prev_matched[pi] {
                continue;
            }
            let prev_norm = normalize_title(&prev.title);
            let Some(len) = boundary_prefix_len(&curr_norm, &prev_norm) else {
                continue;
            };
            if best.is_none_or(|(_, l)| len > l) {
                best = Some((pi, len));
            }
        }
        if let Some((pi, _)) = best {
            absorb_rename_match(diff, previous, pi, curr, ci, prev_matched, curr_ids);
        }
    }
}

/// The shared prefix length when one normalized title is a PROPER prefix of
/// the other and the remainder begins at a non-alphanumeric character —
/// `None` otherwise. Both directions are accepted: answers get appended to
/// titles, and titles also get trimmed.
fn boundary_prefix_len(a: &str, b: &str) -> Option<usize> {
    let (short, long) = if a.len() < b.len() { (a, b) } else { (b, a) };
    if short.is_empty() || short.len() == long.len() || !long.starts_with(short) {
        return None;
    }
    let next = long[short.len()..].chars().next()?;
    (!next.is_alphanumeric()).then_some(short.len())
}

/// Absorb a rename-shaped match: the current heading at `ci` inherits the
/// previous decision at `pi`'s identity, the rename rides the content track,
/// and any state change rides the lifecycle track. Shared by the prefix and
/// proximity passes so the two cannot disagree about what a match emits.
fn absorb_rename_match(
    diff: &mut DecisionDiff,
    previous: &[KnownDecision],
    pi: usize,
    curr: &Decision,
    ci: usize,
    prev_matched: &mut [bool],
    curr_ids: &mut [Option<String>],
) {
    prev_matched[pi] = true;
    let prev = &previous[pi];
    curr_ids[ci] = Some(prev.signal_id.clone());
    // A rename INTO another known decision's exact title would mint
    // an EDITED whose title_hash collides with that decision's — the
    // record log would admit it durably while a projection that
    // enforces per-document title-hash uniqueness (and has no rebuild
    // to heal itself) refuses the update forever. The differ can see
    // a LIVE collision, so it suppresses the edit and absorbs the
    // known-set update instead — identical titles in one document
    // already collapse to one signal at birth, so the corner is
    // degenerate. (A collision with a WITHDRAWN decision's historical
    // title is invisible here; such a projection degrades that case
    // with a warn.)
    let collides = previous
        .iter()
        .any(|p| p.signal_id != prev.signal_id && p.title_hash == curr.title_hash);
    if collides {
        tracing::warn!(
            signal_id = %prev.signal_id,
            title = %curr.title,
            "decision renamed into another decision's title; edit suppressed"
        );
    } else {
        diff.renamed.push(DecisionEvent {
            signal_id: prev.signal_id.clone(),
            title: curr.title.clone(),
            old_title: Some(prev.title.clone()),
            state: curr.state,
            title_hash: curr.title_hash,
            old_title_hash: Some(prev.title_hash),
            parent_hash: curr.parent_hash,
            depth: curr.depth,
            offset: curr.offset,
        });
    }
    emit_state_transition(
        diff,
        prev,
        curr,
        Some(prev.title.clone()),
        Some(prev.title_hash),
    );
}

/// Emit a state-transition event if the state changed between matched entries.
///
/// The event carries `prev.signal_id`: a resolution that also rewrote the
/// heading changes `title_hash`, so deriving the id from the CURRENT title here
/// would address a signal that was never created.
fn emit_state_transition(
    diff: &mut DecisionDiff,
    prev: &KnownDecision,
    curr: &Decision,
    old_title: Option<String>,
    old_title_hash: Option<u64>,
) {
    if prev.state == curr.state {
        return;
    }
    let event = DecisionEvent {
        signal_id: prev.signal_id.clone(),
        title: curr.title.clone(),
        old_title,
        state: curr.state,
        title_hash: curr.title_hash,
        old_title_hash,
        parent_hash: curr.parent_hash,
        depth: curr.depth,
        offset: curr.offset,
    };
    match (prev.state, curr.state) {
        (DecisionState::Open, DecisionState::Resolved) => diff.resolved.push(event),
        (DecisionState::Resolved, DecisionState::Open) => diff.reopened.push(event),
        _ => {} // same state already handled above
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_open_decision() {
        let content = "## ? Should we use JWT?";
        let decisions = parse_decisions(content);
        assert_eq!(decisions.len(), 1);
        let d = &decisions[0];
        assert_eq!(d.depth, 2);
        assert_eq!(d.state, DecisionState::Open);
        assert_eq!(d.title, "Should we use JWT?");
        assert!(d.parent_hash.is_none());
    }

    #[test]
    fn test_parse_resolved_decision() {
        let content = "## = We chose JWT";
        let decisions = parse_decisions(content);
        assert_eq!(decisions.len(), 1);
        let d = &decisions[0];
        assert_eq!(d.depth, 2);
        assert_eq!(d.state, DecisionState::Resolved);
        assert_eq!(d.title, "We chose JWT");
    }

    #[test]
    fn test_parse_nested_decisions() {
        let content = "## ? Auth approach\n### ? Token format\n### ? Token lifetime";
        let decisions = parse_decisions(content);
        assert_eq!(decisions.len(), 3);

        // First is the parent — no parent_hash.
        assert!(decisions[0].parent_hash.is_none());

        // Second and third are children of the first.
        assert_eq!(decisions[1].parent_hash, Some(decisions[0].title_hash));
        assert_eq!(decisions[2].parent_hash, Some(decisions[0].title_hash));
    }

    #[test]
    fn test_no_decisions() {
        let content = "## Regular heading\nSome text\n### Another heading";
        let decisions = parse_decisions(content);
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_decision_inside_code_fence_ignored() {
        let content = "## ? Real decision\n```\n## ? Fake decision\n```\n## = Another real one";
        let stripped = skip_fenced_lines(content);
        let decisions = parse_decisions(&stripped);
        assert_eq!(decisions.len(), 2);
        assert_eq!(decisions[0].title, "Real decision");
        assert_eq!(decisions[1].title, "Another real one");
    }

    #[test]
    fn test_hash_excludes_marker() {
        // Parsing `## ? Title` and `## = Title` should produce the same
        // title_hash because the marker is stripped before hashing.
        let open_decisions = parse_decisions("## ? Should we use JWT?");
        let resolved_decisions = parse_decisions("## = Should we use JWT?");
        assert_eq!(
            open_decisions[0].title_hash,
            resolved_decisions[0].title_hash
        );
    }

    #[test]
    fn test_hash_normalizes_whitespace() {
        let h1 = hash_title("Should  we   use JWT?");
        let h2 = hash_title("Should we use JWT?");
        assert_eq!(h1, h2);
    }

    /// GOLDEN: `title_hash` is a DURABLE value — persisted in a
    /// decision record's `DecisionPayload.title_hash` and re-parsed on tracker
    /// re-derivation. It is pinned here to HARDCODED `u64`s for fixed titles.
    /// A silent change (e.g. a toolchain-varying hasher like `DefaultHasher`)
    /// would re-key every decision and orphan open ones; any change here must
    /// be a conscious, versioned format decision.
    #[test]
    fn test_hash_title_golden() {
        assert_eq!(hash_title("Should we ship?"), 4_346_530_350_143_911_828);
        assert_eq!(hash_title("We chose Postgres"), 3_482_287_969_140_073_502);
    }

    // --- splice_decision_marker tests ---

    #[test]
    fn test_splice_resolves_an_open_heading() {
        let content = "# Doc\n\n## ? Should we ship?\n\nbody stays\n";
        let outcome = splice_decision_marker(
            content,
            hash_title("Should we ship?"),
            DecisionState::Resolved,
            None,
        );
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a flip, got {outcome:?}");
        };
        assert_eq!(new_content, "# Doc\n\n## = Should we ship?\n\nbody stays\n");
    }

    #[test]
    fn test_splice_reopens_a_resolved_heading() {
        let content = "## = Should we ship?\ndetails\n";
        let outcome = splice_decision_marker(
            content,
            hash_title("Should we ship?"),
            DecisionState::Open,
            None,
        );
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a flip, got {outcome:?}");
        };
        assert_eq!(new_content, "## ? Should we ship?\ndetails\n");
    }

    #[test]
    fn test_splice_already_in_target_state() {
        let outcome = splice_decision_marker(
            "## = Should we ship?\n",
            hash_title("Should we ship?"),
            DecisionState::Resolved,
            None,
        );
        assert!(
            matches!(outcome, MarkerSplice::AlreadyInTargetState),
            "got {outcome:?}"
        );
    }

    #[test]
    fn test_splice_unknown_hash_not_found() {
        let outcome = splice_decision_marker(
            "## ? Should we ship?\n",
            hash_title("A different decision"),
            DecisionState::Resolved,
            None,
        );
        assert!(matches!(outcome, MarkerSplice::NotFound), "got {outcome:?}");
    }

    #[test]
    fn test_splice_targets_the_real_heading_past_a_fenced_lookalike() {
        // The fenced copy sits ABOVE the real heading, so a raw-offset (or
        // fence-blind) implementation would splice inside the code block.
        let content = "```\n## ? Should we ship?\n```\n\n## ? Should we ship?\n";
        let outcome = splice_decision_marker(
            content,
            hash_title("Should we ship?"),
            DecisionState::Resolved,
            None,
        );
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a flip, got {outcome:?}");
        };
        assert_eq!(
            new_content,
            "```\n## ? Should we ship?\n```\n\n## = Should we ship?\n"
        );
    }

    #[test]
    fn test_splice_targets_the_matching_title_among_several() {
        let content = "## ? First\n## ? Second\n## ? Third\n";
        let outcome =
            splice_decision_marker(content, hash_title("Second"), DecisionState::Resolved, None);
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a flip, got {outcome:?}");
        };
        assert_eq!(new_content, "## ? First\n## = Second\n## ? Third\n");
    }

    #[test]
    fn test_splice_note_lands_under_the_heading_never_in_the_title() {
        let content = "## ? Should we ship?\n\nexisting body\n";
        let outcome = splice_decision_marker(
            content,
            hash_title("Should we ship?"),
            DecisionState::Resolved,
            Some("Yes — the gates are green."),
        );
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a flip, got {outcome:?}");
        };
        assert_eq!(
            new_content,
            "## = Should we ship?\n\n> Yes — the gates are green.\n\nexisting body\n"
        );
        // The heading's identity is untouched: same title, same hash.
        let decisions = parse_decisions(&skip_fenced_lines(&new_content));
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].title_hash, hash_title("Should we ship?"));
    }

    #[test]
    fn test_splice_note_lands_even_when_already_in_target_state() {
        // Re-resolving an already-`=` heading with a rationale must place the
        // rationale rather than dropping it behind a success.
        let content = "## = Should we ship?\n\nexisting body\n";
        let outcome = splice_decision_marker(
            content,
            hash_title("Should we ship?"),
            DecisionState::Resolved,
            Some("chose Postgres"),
        );
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a note-only update, got {outcome:?}");
        };
        assert_eq!(
            new_content,
            "## = Should we ship?\n\n> chose Postgres\n\nexisting body\n"
        );
    }

    #[test]
    fn test_splice_note_fence_lines_cannot_swallow_sibling_markers() {
        // A note carrying an unbalanced code fence would, unquoted, blank
        // every line below it in the fence-stripped parse — reading as the
        // REMOVAL of every later marker and withdrawing signals the caller
        // never named. Blockquoting keeps the fence inert: both headings must
        // still parse afterwards, and no new decision may appear.
        let content = "## ? First\n\n## ? Second\n";
        let outcome = splice_decision_marker(
            content,
            hash_title("First"),
            DecisionState::Resolved,
            Some("fixed by:\n```rust\nfn f() {}\n## ? Sneaky heading"),
        );
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a flip, got {outcome:?}");
        };
        let decisions = parse_decisions(&skip_fenced_lines(&new_content));
        let titles: Vec<&str> = decisions.iter().map(|d| d.title.as_str()).collect();
        assert_eq!(
            titles,
            vec!["First", "Second"],
            "both real headings survive and nothing new is minted: {new_content:?}"
        );
        assert_eq!(decisions[0].state, DecisionState::Resolved);
        assert_eq!(decisions[1].state, DecisionState::Open);
    }

    #[test]
    fn test_splice_multiline_note_blockquotes_every_line() {
        let content = "## ? Should we ship?\n";
        let outcome = splice_decision_marker(
            content,
            hash_title("Should we ship?"),
            DecisionState::Resolved,
            Some("first\n\nsecond"),
        );
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a flip, got {outcome:?}");
        };
        assert_eq!(
            new_content,
            "## = Should we ship?\n\n> first\n>\n> second\n"
        );
    }

    #[test]
    fn test_splice_heading_at_eof_without_trailing_newline() {
        let content = "intro\n\n## ? Should we ship?";
        let outcome = splice_decision_marker(
            content,
            hash_title("Should we ship?"),
            DecisionState::Resolved,
            Some("Shipped."),
        );
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a flip, got {outcome:?}");
        };
        assert_eq!(new_content, "intro\n\n## = Should we ship?\n\n> Shipped.");
    }

    #[test]
    fn test_splice_deep_heading_keeps_depth() {
        let content = "## ? Parent\n### ? Child\n";
        let outcome =
            splice_decision_marker(content, hash_title("Child"), DecisionState::Resolved, None);
        let MarkerSplice::Updated(new_content) = outcome else {
            panic!("expected a flip, got {outcome:?}");
        };
        assert_eq!(new_content, "## ? Parent\n### = Child\n");
    }

    // --- DecisionTracker tests ---

    #[test]
    fn test_tracker_first_update_open_and_born_resolved() {
        // On first observation a `## ? …` heading is OPENED and a `## = …`
        // heading is BORN resolved — the latter routes to `born_resolved`, NOT
        // `resolved` (which is reserved for the `? → =` edit transition).
        let tracker = DecisionTracker::new();
        let diff = tracker.update("s1", "d1", "## ? First\n## = Second");
        assert_eq!(diff.opened.len(), 1);
        assert_eq!(diff.opened[0].title, "First");
        assert_eq!(diff.born_resolved.len(), 1);
        assert_eq!(diff.born_resolved[0].title, "Second");
        assert!(
            diff.resolved.is_empty(),
            "a first-seen `=` is born_resolved, not a resolve transition: {diff:?}"
        );
        assert!(diff.reopened.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_tracker_no_change_empty_diff() {
        let tracker = DecisionTracker::new();
        tracker.update("s1", "d1", "## ? First");
        let diff = tracker.update("s1", "d1", "## ? First");
        assert!(diff.opened.is_empty());
        assert!(diff.resolved.is_empty());
        assert!(diff.reopened.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_tracker_state_transition_same_title() {
        let tracker = DecisionTracker::new();
        tracker.update("s1", "d1", "## ? Should we use JWT?");
        let diff = tracker.update("s1", "d1", "## = Should we use JWT?");
        assert_eq!(diff.resolved.len(), 1);
        assert_eq!(diff.resolved[0].title, "Should we use JWT?");
        assert!(diff.resolved[0].old_title.is_none());
        assert!(diff.opened.is_empty());
        assert!(diff.reopened.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_tracker_state_transition_rewritten_title() {
        let tracker = DecisionTracker::new();
        tracker.update("s1", "d1", "## ? Should we use JWT?");
        // Same offset (line 1), different title and state.
        let diff = tracker.update("s1", "d1", "## = We chose JWT for auth");
        assert_eq!(diff.resolved.len(), 1);
        let event = &diff.resolved[0];
        assert_eq!(event.title, "We chose JWT for auth");
        assert_eq!(event.old_title.as_deref(), Some("Should we use JWT?"));
        assert!(event.old_title_hash.is_some());
        // A resolve-with-rename is ALSO a content change: the rename rides
        // its own track as `renamed` (an EDITED record), which is
        // the only place the new title is durably recorded — the CLOSE's
        // payload slot belongs to the transition note.
        assert_eq!(diff.renamed.len(), 1, "diff.renamed: {:?}", diff.renamed);
        assert_eq!(diff.renamed[0].signal_id, event.signal_id);
    }

    /// The resolving edit's natural shape at real document scale: the answer
    /// is appended to the title AND the document has grown far above the
    /// heading, so the title-hash pass misses (title changed) and the
    /// proximity pass misses (offset drifted past its window). The prefix
    /// pass is what keeps identity: same signal id, a resolve + rename, not
    /// a withdrawn-plus-born-resolved pair.
    #[test]
    fn test_tracker_prefix_match_survives_title_growth_and_offset_drift() {
        let tracker = DecisionTracker::new();
        let before = format!("{}\n## ? Appetizer", "intro\n".repeat(4));
        let first = tracker.update("s1", "d1", &before);
        assert_eq!(first.opened.len(), 1);
        let born_id = first.opened[0].signal_id.clone();

        // ~8KB of prose lands above the heading in the same edit that
        // resolves it with the answer folded into the title.
        let after = format!(
            "{}\n## = Appetizer — mezze spread, nut-free by construction",
            "filler prose line\n".repeat(500),
        );
        let diff = tracker.update("s1", "d1", &after);
        assert_eq!(
            diff.resolved.len(),
            1,
            "resolved: {:?} born_resolved: {:?} removed: {:?}",
            diff.resolved,
            diff.born_resolved,
            diff.removed
        );
        assert_eq!(diff.resolved[0].signal_id, born_id);
        assert_eq!(diff.resolved[0].old_title.as_deref(), Some("Appetizer"));
        assert_eq!(diff.renamed.len(), 1, "the rename rides its own track");
        assert_eq!(diff.renamed[0].signal_id, born_id);
        assert!(diff.born_resolved.is_empty());
        assert!(diff.removed.is_empty());
    }

    /// The extension must begin at a non-alphanumeric boundary: `Design`
    /// does not match `Designated owner`. Out of proximity range too, so
    /// nothing matches — the old decision withdraws and the new one is born.
    #[test]
    fn test_tracker_prefix_match_requires_a_word_boundary() {
        let tracker = DecisionTracker::new();
        let first = tracker.update("s1", "d1", "## ? Design");
        let born_id = first.opened[0].signal_id.clone();

        let after = format!(
            "{}\n## = Designated owner",
            "filler prose line\n".repeat(500),
        );
        let diff = tracker.update("s1", "d1", &after);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].signal_id, born_id);
        assert_eq!(diff.born_resolved.len(), 1);
        assert_ne!(diff.born_resolved[0].signal_id, born_id);
        assert!(diff.resolved.is_empty());
        assert!(diff.renamed.is_empty());
    }

    /// Among several boundary-prefix candidates the LONGEST shared prefix
    /// wins: `Choose DB — postgres` belongs to `Choose DB`, not `Choose`.
    #[test]
    fn test_tracker_prefix_match_longest_prefix_wins() {
        let tracker = DecisionTracker::new();
        let first = tracker.update("s1", "d1", "## ? Choose\n## ? Choose DB");
        assert_eq!(first.opened.len(), 2);
        let choose_db_id = first.opened[1].signal_id.clone();

        let after = format!(
            "{}\n## = Choose DB — postgres",
            "filler prose line\n".repeat(500),
        );
        let diff = tracker.update("s1", "d1", &after);
        assert_eq!(diff.resolved.len(), 1);
        assert_eq!(
            diff.resolved[0].signal_id, choose_db_id,
            "the longer prefix owns the resolution"
        );
        assert_eq!(diff.removed.len(), 1, "the shorter title withdraws");
    }

    #[test]
    fn test_tracker_decision_removed() {
        let tracker = DecisionTracker::new();
        tracker.update("s1", "d1", "## ? First\n## ? Second");
        let diff = tracker.update("s1", "d1", "## ? First");
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].title, "Second");
    }

    #[test]
    fn test_tracker_move_no_event() {
        let tracker = DecisionTracker::new();
        tracker.update("s1", "d1", "## ? First\n## ? Second");
        // Swap order — title hashes match, state unchanged → no events.
        let diff = tracker.update("s1", "d1", "## ? Second\n## ? First");
        assert!(diff.opened.is_empty());
        assert!(diff.resolved.is_empty());
        assert!(diff.reopened.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_tracker_remove_clears_state() {
        let tracker = DecisionTracker::new();
        tracker.update("s1", "d1", "## ? First");
        tracker.remove("s1", "d1");
        // After remove, next update treats everything as new.
        let diff = tracker.update("s1", "d1", "## ? First");
        assert_eq!(diff.opened.len(), 1);
    }

    #[test]
    fn test_tracker_same_state_rename_emits_renamed() {
        // A same-state rename is a content change and must be RECORDED as
        // `renamed` (an EDITED record); what a feed surfaces is a separate
        // per-surface decision. Lifecycle buckets stay silent.
        let tracker = DecisionTracker::new();
        tracker.update("s1", "d1", "## ? Alpha\n## ? Beta");
        let diff = tracker.update("s1", "d1", "## ? Alpha renamed\n## ? Beta renamed");
        assert!(diff.opened.is_empty());
        assert!(diff.resolved.is_empty());
        assert!(diff.reopened.is_empty());
        assert!(diff.removed.is_empty());
        assert_eq!(diff.renamed.len(), 2, "both renames must be recorded");
        let mut pairs: Vec<(&str, &str)> = diff
            .renamed
            .iter()
            .map(|e| {
                (
                    e.old_title
                        .as_deref()
                        .expect("a rename carries what it replaced"),
                    e.title.as_str(),
                )
            })
            .collect();
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            vec![("Alpha", "Alpha renamed"), ("Beta", "Beta renamed")],
            "each rename must pair its OWN old title with its new one"
        );
    }

    #[test]
    fn test_tracker_depth_change_emits_edited() {
        // Re-nesting is a content change: depth and parent_hash are two of
        // the four columns a decision EDITED replaces, and absorbing them
        // silently would leave the projected decision stale.
        let tracker = DecisionTracker::new();
        tracker.update("s1", "d1", "## ? Alpha");
        let diff = tracker.update("s1", "d1", "### ? Alpha");
        assert_eq!(diff.renamed.len(), 1, "diff: {diff:?}");
        assert_eq!(diff.renamed[0].depth, 3);
        assert!(
            diff.renamed[0].old_title.is_none(),
            "same title — a structure change is not a rename of the text"
        );
        // Unchanged content afterwards stays quiet.
        let diff = tracker.update("s1", "d1", "### ? Alpha");
        assert!(diff.renamed.is_empty(), "diff: {diff:?}");
    }

    #[test]
    fn test_tracker_rename_into_existing_title_is_absorbed_not_emitted() {
        // Renaming Beta to Alpha's exact title would mint an EDITED whose
        // title_hash collides with Alpha's — durably admitted to the log,
        // then refused forever by a projection that enforces per-document
        // title-hash uniqueness and has no rebuild to heal itself. The
        // differ can SEE a live collision, so it suppresses the edit and
        // absorbs the known-set update for this degenerate corner (identical
        // titles in one document already collapse to one signal at birth).
        let tracker = DecisionTracker::new();
        let filler = "lorem ipsum ".repeat(30);
        tracker.update("s1", "d1", &format!("## ? Alpha\n\n{filler}\n\n## ? Beta"));
        let diff = tracker.update("s1", "d1", &format!("## ? Alpha\n\n{filler}\n\n## ? Alpha"));
        assert!(
            diff.renamed.is_empty(),
            "a colliding rename must not mint an EDITED: {diff:?}"
        );
        assert!(
            diff.removed.is_empty(),
            "Beta still proximity-matched: {diff:?}"
        );
        assert!(diff.opened.is_empty(), "diff: {diff:?}");
    }

    #[test]
    fn test_tracker_rename_carries_the_birth_signal_id() {
        // The EDITED record must target the signal that exists — the id
        // minted at birth — not one re-derived from the new title.
        let tracker = DecisionTracker::new();
        let first = tracker.update("s1", "d1", "## ? Which database?");
        let born_id = first.opened[0].signal_id.clone();
        let diff = tracker.update("s1", "d1", "## ? Which database engine?");
        assert_eq!(diff.renamed.len(), 1);
        assert_eq!(
            diff.renamed[0].signal_id, born_id,
            "a rename keeps the identity it was born with"
        );
        assert_eq!(diff.renamed[0].title, "Which database engine?");
    }

    /// Build a CREATED decision signal mirroring what the materializer emits
    /// for an added heading, keyed on `(document_id, title_hash)`.
    ///
    /// Born-resolved is expressed by the caller setting `close_reason=RESOLVED`
    /// on the returned record (as the materializer does) — the payload
    /// carries no `state` field.
    fn created_decision_record(document_id: &str, title: &str) -> Signal {
        let title_hash = hash_title(title);
        let mut s = Signal {
            id: format!("dec-{title_hash}"),
            space_id: "5171e0a1-1111-4000-8000-000000000002".into(),
            document_id: Some(document_id.to_owned()),
            record_id: format!("r-{title_hash}"),
            payload: Some(signal::Payload::Decision(
                kutl_proto::sync::DecisionPayload {
                    title_hash: title_hash.to_string(),
                    title: normalize_title(title),
                    depth: 2,
                    parent_hash: None,
                },
            )),
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

    /// Re-deriving a doc's known-set from its CREATED records must reproduce
    /// exactly the state a fresh `update()` over the same content holds — so a
    /// subsequent `update()` on unchanged content yields an EMPTY diff. An empty
    /// known-set would spuriously re-open the decision.
    #[test]
    fn test_rederive_then_update_same_content_empty_diff() {
        let content = "## ? Should we ship?\n## = We chose Postgres";
        let tracker = DecisionTracker::new();
        // Seed the known-set the way a live merge would.
        let first = tracker.update("s1", "d1", content);
        assert_eq!(first.opened.len(), 1, "baseline opened: {first:?}");
        assert_eq!(
            first.born_resolved.len(),
            1,
            "baseline born_resolved: {first:?}"
        );

        // Evict, then re-derive from the records update() would have produced:
        // an OPEN heading → CREATED(Open); a born-`=` heading → a CREATED
        // carrying close_reason=RESOLVED, which the fold
        // seeds to Closed(Resolved).
        tracker.remove("s1", "d1");
        let mut born_resolved = created_decision_record("d1", "We chose Postgres");
        born_resolved.set_close_reason(kutl_proto::sync::CloseReason::Resolved);
        let records = vec![
            created_decision_record("d1", "Should we ship?"),
            born_resolved,
        ];
        tracker.rederive_from("s1", "d1", &records);

        let diff = tracker.update("s1", "d1", content);
        assert!(diff.opened.is_empty(), "spurious opened: {diff:?}");
        assert!(
            diff.born_resolved.is_empty(),
            "spurious born_resolved: {diff:?}"
        );
        assert!(diff.resolved.is_empty(), "spurious resolved: {diff:?}");
        assert!(diff.reopened.is_empty(), "spurious reopened: {diff:?}");
        assert!(diff.removed.is_empty(), "spurious removed: {diff:?}");
    }

    /// A born-resolved decision (a heading first authored as `## = …`)
    /// materializes as a CREATED carrying `close_reason=RESOLVED`. Re-deriving
    /// the known-set from that record must read it as PRESENT (status Closed,
    /// reason Resolved) — NOT as gone. Proof: after re-derivation, removing the
    /// heading yields a `removed` event; had the reader treated
    /// born-`Closed(Resolved)` as absent (the withdrawn/tombstoned arm), the
    /// known-set would be empty and removing the heading would produce NOTHING.
    #[test]
    fn test_rederive_born_resolved_reads_present_not_gone() {
        let tracker = DecisionTracker::new();
        let mut born = created_decision_record("d1", "We chose Postgres");
        born.set_close_reason(kutl_proto::sync::CloseReason::Resolved);
        tracker.rederive_from("s1", "d1", &[born]);

        // The heading is gone from the content → a PRESENT known-entry surfaces
        // as removed. An absent (mis-read-as-gone) entry would produce nothing.
        let diff = tracker.update("s1", "d1", "no decisions here");
        assert_eq!(
            diff.removed.len(),
            1,
            "born-resolved must re-derive as PRESENT, not gone: {diff:?}"
        );
        assert_eq!(diff.removed[0].title, "We chose Postgres");
        assert!(diff.opened.is_empty());
        assert!(diff.born_resolved.is_empty());
        assert!(diff.resolved.is_empty());
    }

    /// A decision whose CREATED then CLOSED(RESOLVED): the fold status is Closed,
    /// but the heading still exists (now `## = …`), so re-derivation keeps it as
    /// Resolved and a subsequent update on the resolved content diffs empty.
    #[test]
    fn test_rederive_resolved_via_close_keeps_heading() {
        let resolved_content = "## = We chose Postgres";
        let tracker = DecisionTracker::new();
        tracker.update("s1", "d1", "## ? Which database?");
        tracker.update("s1", "d1", resolved_content);
        tracker.remove("s1", "d1");

        // Records: CREATED(Open) then CLOSED(Resolved) for the SAME signal id.
        // The resolved heading text changed; the CREATED payload title mirrors
        // the current `=` heading (proximity-matched at materialize time), so
        // the re-derived title is the resolved one.
        let created = created_decision_record("d1", "We chose Postgres");
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
        closed.set_close_reason(kutl_proto::sync::CloseReason::Resolved);
        tracker.rederive_from("s1", "d1", &[created, closed]);

        let diff = tracker.update("s1", "d1", resolved_content);
        assert!(diff.opened.is_empty(), "spurious opened: {diff:?}");
        assert!(diff.resolved.is_empty(), "spurious resolved: {diff:?}");
        assert!(diff.reopened.is_empty(), "spurious reopened: {diff:?}");
        assert!(diff.removed.is_empty(), "spurious removed: {diff:?}");
    }

    /// A withdrawn (removed) decision must NOT be re-derived as known — the
    /// heading is gone, so re-deriving then updating to the WITHOUT-heading
    /// content produces no event (and a fresh add of the same heading opens).
    #[test]
    fn test_rederive_withdrawn_decision_absent() {
        let tracker = DecisionTracker::new();
        let mut created = created_decision_record("d1", "Doomed decision");
        created.record_id = "r-create".into();
        let mut withdrawn = Signal {
            id: created.id.clone(),
            space_id: "5171e0a1-1111-4000-8000-000000000002".into(),
            document_id: Some("d1".into()),
            record_id: "r-withdraw".into(),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: 2,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        withdrawn.set_event(kutl_proto::sync::SignalEventType::Closed);
        withdrawn.set_close_reason(kutl_proto::sync::CloseReason::Withdrawn);
        tracker.rederive_from("s1", "d1", &[created, withdrawn]);

        // Content without the heading: no spurious removed (it was absent).
        let diff = tracker.update("s1", "d1", "no decisions here");
        assert!(
            diff.removed.is_empty(),
            "withdrawn re-derived as known: {diff:?}"
        );
        assert!(diff.opened.is_empty());
        assert!(diff.resolved.is_empty());
    }

    /// A first-seen heading is born with the `(document_id, title_hash)`-derived
    /// id, and a later transition on it reports that SAME id even though the
    /// title — and therefore the derivation — has changed.
    #[test]
    fn test_signal_id_is_minted_at_birth_then_carried_through_a_rename() {
        let tracker = DecisionTracker::new();
        let opened = tracker.update("s1", "d1", "## ? Which database?");
        let born_id = opened.opened[0].signal_id.clone();
        assert_eq!(
            born_id,
            decision_signal_id("d1", hash_title("Which database?")),
            "a new decision mints from its first title"
        );

        // Resolve and rename in one edit — proximity-matched, so the event
        // carries the NEW title_hash but must keep the BORN id.
        let resolved = tracker.update("s1", "d1", "## = We chose Postgres");
        assert_eq!(resolved.resolved.len(), 1, "one resolve: {resolved:?}");
        assert_eq!(resolved.resolved[0].signal_id, born_id);
        assert_ne!(
            born_id,
            decision_signal_id("d1", resolved.resolved[0].title_hash),
            "the fixture must actually change the derivation"
        );
    }

    /// Re-derivation restores the id the decision was ACTUALLY created under —
    /// read off the fold key, not recomputed from the payload title. The fixture
    /// uses an id that the payload's own title does not derive to (as happens
    /// after any rename), so a recomputing implementation would produce a
    /// different id here and the subsequent removal would close a signal that
    /// does not exist.
    #[test]
    fn test_rederive_restores_the_created_signal_id() {
        let tracker = DecisionTracker::new();
        let created = created_decision_record("d1", "We chose Postgres");
        let record_id = created.id.clone();
        assert_ne!(
            record_id,
            decision_signal_id("d1", hash_title("We chose Postgres")),
            "fixture id must differ from the title derivation"
        );
        tracker.rederive_from("s1", "d1", &[created]);

        let diff = tracker.update("s1", "d1", "no decisions here");
        assert_eq!(diff.removed.len(), 1, "one removal: {diff:?}");
        assert_eq!(
            diff.removed[0].signal_id, record_id,
            "the removal must name the id on the record, not a re-derived one"
        );
    }

    /// A whole-title rewrite — no shared prefix for the prefix pass — with
    /// paragraph-scale local drift: the answer written around the heading
    /// shifts its offset by a few hundred bytes. Proximity's window must
    /// cover that, or the one rename shape left to it severs identity for
    /// exactly the edits people make.
    #[test]
    fn test_tracker_proximity_covers_paragraph_scale_drift() {
        let tracker = DecisionTracker::new();
        let first = tracker.update("s1", "d1", "## ? Should we use Postgres or MySQL");
        let born_id = first.opened[0].signal_id.clone();

        // ~800 bytes of rationale land above the heading in the edit that
        // rewrites the whole title into the answer.
        let after = format!("{}\n## = Postgres", "rationale prose line\n".repeat(40));
        let diff = tracker.update("s1", "d1", &after);
        assert_eq!(
            diff.resolved.len(),
            1,
            "resolved: {:?} born_resolved: {:?} removed: {:?}",
            diff.resolved,
            diff.born_resolved,
            diff.removed
        );
        assert_eq!(diff.resolved[0].signal_id, born_id);
        assert!(diff.born_resolved.is_empty());
        assert!(diff.removed.is_empty());
    }

    /// The window's outer edge still exists: a heading that moved further
    /// than [`DECISION_PROXIMITY_CHARS`] with a whole-title rewrite is a
    /// removal plus a birth, not a silent absorption.
    #[test]
    fn test_tracker_proximity_window_still_bounds_the_match() {
        let tracker = DecisionTracker::new();
        let first = tracker.update("s1", "d1", "## ? Should we use Postgres or MySQL");
        let born_id = first.opened[0].signal_id.clone();

        let after = format!("{}\n## = Postgres", "filler prose line\n".repeat(500));
        let diff = tracker.update("s1", "d1", &after);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.removed[0].signal_id, born_id);
        assert_eq!(diff.born_resolved.len(), 1);
        assert!(diff.resolved.is_empty());
    }

    #[test]
    fn test_tracker_proximity_nearest_wins() {
        // Two previous decisions at different offsets, BOTH within the
        // proximity window of the single renamed decision near the first
        // one's position. Proximity must match the current to the NEAREST
        // previous (Alpha, offset 0) — not Beta — so Beta surfaces as
        // `removed`. If the algorithm picked the wrong candidate (Beta),
        // Alpha would be `removed` instead and this assertion would fail.
        let tracker = DecisionTracker::new();
        let filler = "lorem ipsum ".repeat(30);
        let initial = format!("## ? Alpha\n\n{filler}\n\n## ? Beta");
        tracker.update("s1", "d1", &initial);
        // New document has only a renamed Alpha near offset 0.
        let diff = tracker.update("s1", "d1", "## ? Gamma");
        // Beta was far from the only current decision → removed.
        assert_eq!(diff.removed.len(), 1, "diff.removed: {:?}", diff.removed);
        assert_eq!(diff.removed[0].title, "Beta");
        // Gamma matched Alpha by proximity — not a birth but a RENAME:
        // recorded as `renamed`, with no opened event.
        assert!(diff.opened.is_empty(), "diff.opened: {:?}", diff.opened);
        assert_eq!(diff.renamed.len(), 1, "diff.renamed: {:?}", diff.renamed);
        assert_eq!(diff.renamed[0].old_title.as_deref(), Some("Alpha"));
        assert!(diff.resolved.is_empty());
        assert!(diff.reopened.is_empty());
    }

    /// Re-derivation must restore the title as it reads NOW — the content
    /// track's EDITED, not the CREATED's birth title. With birth-only
    /// restoration the first post-restart parse would fail the title-hash
    /// pass, proximity-match, and emit a phantom rename-EDITED for every
    /// previously renamed decision — a storm on every relay restart, once
    /// per renamed decision per document.
    #[test]
    fn test_rederive_after_rename_yields_empty_diff() {
        let tracker = DecisionTracker::new();
        let created = created_decision_record("d1", "Which database?");
        let mut edited = created_decision_record("d1", "Which database engine?");
        // The EDITED targets the signal born under the ORIGINAL title's id,
        // with a later HLC so the content track resolves to it.
        edited.id = created.id.clone();
        edited.record_id = "r-edit".into();
        edited.set_event(kutl_proto::sync::SignalEventType::Edited);
        if let Some(hlc) = &mut edited.hlc {
            hlc.physical_ms = 2;
        }
        tracker.rederive_from("s1", "d1", &[created, edited]);

        let diff = tracker.update("s1", "d1", "## ? Which database engine?");
        assert!(diff.opened.is_empty(), "spurious opened: {diff:?}");
        assert!(
            diff.renamed.is_empty(),
            "phantom rename after restart: {diff:?}"
        );
        assert!(diff.removed.is_empty(), "spurious removed: {diff:?}");
    }
}
