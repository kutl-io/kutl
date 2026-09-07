//! `kutl signal list` / `kutl signal view` — the read window onto a space's
//! local signal segments.
//!
//! Reads segments directly from `<space_root>/.kutl/signals/<space_id>/` via
//! [`SegmentStore::load`] and folds them into a [`SpaceSignalState`]. No daemon
//! needs to be running: the append-only segment format serves lock-free reads.
//! `--fetch` first pulls the latest records from the relay (which DOES take the
//! store's single-writer lock — when the running daemon holds it, the fetch is
//! skipped with a stderr note and the local fold is served: the daemon is
//! already live-syncing those records).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use kutl_proto::sync::{FlagKind as ProtoKind, Signal, signal};
use kutl_proto::vocab::flag_kind_guidance;
use kutl_signals::fold::{SignalState, SignalStatus, SpaceSignalState};
use kutl_signals::segment::SegmentStore;
use kutl_signals::summary::{StatusFilter, TransitionEntry};
use serde::Serialize;

use crate::{OutputFormat, SignalCreateArgs, SignalListArgs};

/// The signal kind selector for `--kind`, matched against the CREATED record's
/// payload oneof. A [`clap::ValueEnum`] so the flag validates at parse time.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalKind {
    /// A flag payload (`FlagPayload`).
    Flag,
    /// A chat payload (`ChatPayload`).
    Chat,
    /// A decision payload (`DecisionPayload`).
    Decision,
    /// A reply payload (`ReplyPayload`).
    Reply,
}

impl SignalKind {
    /// The shared kind this clap value denotes.
    ///
    /// The CLI keeps its own enum only because it is a `clap::ValueEnum`;
    /// putting clap in `kutl-signals` to share one type would be the wrong
    /// trade. Every rule ABOUT a kind — its label, and what payload it matches
    /// — lives once, in `kutl_signals::summary`.
    fn to_shared(self) -> kutl_signals::summary::SignalKind {
        use kutl_signals::summary::SignalKind as Shared;
        match self {
            SignalKind::Flag => Shared::Flag,
            SignalKind::Chat => Shared::Chat,
            SignalKind::Decision => Shared::Decision,
            SignalKind::Reply => Shared::Reply,
        }
    }
}

/// The flag intent-kind selector, shared by `signal create --kind` and
/// `signal list --flag-kind`. A [`clap::ValueEnum`] so the flag validates at
/// parse time and clap renders the accepted values into `--help`.
///
/// Orthogonal to [`SignalKind`], which selects the record type: this says what
/// *kind of attention* a flag warrants, that says whether a record is a flag at
/// all.
///
/// Each variant's `--help` line is [`kutl_proto::vocab::flag_kind_guidance`],
/// the same sentence the MCP `create_flag` tool description renders, rather
/// than a restatement of it: an agent that reads either surface is told the
/// same thing about what a kind is for.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
#[value(rename_all = "snake_case")]
pub enum FlagKindArg {
    /// `FlagKind::Info`.
    #[value(help = flag_kind_guidance(ProtoKind::Info))]
    Info,
    /// `FlagKind::ReviewRequested`.
    #[value(help = flag_kind_guidance(ProtoKind::ReviewRequested))]
    ReviewRequested,
    /// `FlagKind::Question`.
    #[value(help = flag_kind_guidance(ProtoKind::Question))]
    Question,
    /// `FlagKind::Blocked`.
    #[value(help = flag_kind_guidance(ProtoKind::Blocked))]
    Blocked,
    /// `FlagKind::Completed`.
    #[value(help = flag_kind_guidance(ProtoKind::Completed))]
    Completed,
    /// `FlagKind::Comment`. Filterable, but not creatable from the CLI — it
    /// needs the inline marker the editor and MCP `create_comment` bind for it.
    #[value(help = flag_kind_guidance(ProtoKind::Comment))]
    Comment,
}

impl FlagKindArg {
    /// The proto kind this clap value denotes.
    ///
    /// The CLI keeps its own enum only because it is a `clap::ValueEnum`;
    /// putting clap in `kutl-proto` to share one type would be the wrong trade.
    /// Every rule ABOUT a kind — its wire label, its meaning — lives once, in
    /// `kutl_proto::vocab`.
    fn to_proto(self) -> ProtoKind {
        match self {
            FlagKindArg::Info => ProtoKind::Info,
            FlagKindArg::Completed => ProtoKind::Completed,
            FlagKindArg::ReviewRequested => ProtoKind::ReviewRequested,
            FlagKindArg::Question => ProtoKind::Question,
            FlagKindArg::Blocked => ProtoKind::Blocked,
            FlagKindArg::Comment => ProtoKind::Comment,
        }
    }

    /// The canonical wire label, resolved through the shared vocabulary rather
    /// than restated here.
    fn label(self) -> &'static str {
        kutl_proto::vocab::flag_kind_to_str(i32::from(self.to_proto()))
    }
}

/// The close-reason selector for `kutl signal close --reason`. A
/// [`clap::ValueEnum`] so the flag validates at parse time; maps to the relay's
/// lowercase wire reason strings (`resolved` | `declined` | `withdrawn`).
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseReasonArg {
    /// The flag was addressed (default).
    Resolved,
    /// The flag was rejected / won't-fix.
    Declined,
    /// The flag was retracted by its author.
    Withdrawn,
}

impl CloseReasonArg {
    /// The lowercase wire string the relay's transition surfaces expect — the
    /// inverse of the shared [`kutl_signals::payloads::close_reason_from_wire`].
    fn wire_label(self) -> &'static str {
        match self {
            CloseReasonArg::Resolved => "resolved",
            CloseReasonArg::Declined => "declined",
            CloseReasonArg::Withdrawn => "withdrawn",
        }
    }
}

/// Which transition a `kutl signal` verb applies. `Resolve` is sugar for
/// `Close` with the reason forced to `resolved` (it shares the close code path).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransitionVerb {
    /// `kutl signal close` — CLOSE with a caller reason (defaults to resolved).
    Close,
    /// `kutl signal reopen` — REOPEN (clears close-state; no reason).
    Reopen,
    /// `kutl signal resolve` — CLOSE with the reason forced to resolved.
    Resolve,
}

/// Build the [`kutl_client::signal_catchup::TransitionRequest`] a verb sends
/// to the relay (as a `SubmitTransition` WS frame). Single source of truth for
/// the close/reopen/resolve intent so the three CLI verbs cannot drift: `close` carries the caller reason
/// (defaulting to `resolved`); `resolve` forces the reason to `resolved`;
/// `reopen` carries none.
fn build_transition_request(
    verb: TransitionVerb,
    reason: Option<CloseReasonArg>,
) -> kutl_client::signal_catchup::TransitionRequest {
    use kutl_client::signal_catchup::{TransitionEvent, TransitionRequest};
    match verb {
        TransitionVerb::Close => TransitionRequest {
            event: TransitionEvent::Closed,
            reason: Some(
                reason
                    .unwrap_or(CloseReasonArg::Resolved)
                    .wire_label()
                    .to_owned(),
            ),
        },
        TransitionVerb::Resolve => TransitionRequest {
            event: TransitionEvent::Closed,
            reason: Some(CloseReasonArg::Resolved.wire_label().to_owned()),
        },
        TransitionVerb::Reopen => TransitionRequest {
            event: TransitionEvent::Reopened,
            reason: None,
        },
    }
}

/// The kind of a signal derived from its CREATED record's payload oneof.
/// `None` for a record with no payload set (a legacy or malformed CREATED), and
/// for a CREATED carrying a transition payload, which is malformed the same way —
/// a transition is never a signal's CREATED.
fn kind_label(created: &Signal) -> Option<&'static str> {
    kutl_signals::summary::kind_of(created).map(kutl_signals::summary::SignalKind::label)
}

/// Resolve the shared status filter from the CLI's mutually-informative flags.
/// `--all` wins over the pair; `--open`/`--closed` combine; nothing selected
/// defaults to open only. What each variant ADMITS is defined once, in
/// `kutl_signals::summary::StatusFilter`.
fn status_filter_from_flags(open: bool, closed: bool, all: bool) -> StatusFilter {
    if all || (open && closed) {
        StatusFilter::All
    } else if closed {
        StatusFilter::Closed
    } else {
        StatusFilter::Open
    }
}

/// A serializable projection of one folded signal for `--format json` and the
/// human render. Built off the fold's [`SignalState`] — never a `Debug`-print
/// of the proto.
#[derive(Debug, Serialize)]
pub struct SignalView {
    /// Signal id.
    pub id: String,
    /// The space id the signal belongs to.
    pub space_id: String,
    /// The space's human name (from `.kutlspace`, falling back to the id).
    pub space_name: String,
    /// The document the signal is attached to, if any.
    pub document_id: Option<String>,
    /// The signal's kind (`flag`/`chat`/`decision`/`reply`), if a payload is set.
    pub kind: Option<&'static str>,
    /// The flag's message / reply's body, when the record carries one.
    pub message: Option<String>,
    /// The flag intent-kind name (`info`/`question`/`blocked`/…), flags only.
    pub flag_kind: Option<&'static str>,
    /// Who the signal is for (`participant`/`space`), flags only.
    pub audience: Option<&'static str>,
    /// The addressed participant's DID, when the audience names one.
    pub target_did: Option<String>,
    /// The DID of whoever raised the signal.
    ///
    /// Distinct from [`target_did`](Self::target_did) and rendered apart from
    /// it: one says who is asking, the other who was asked. A reader deciding
    /// whether something is theirs to answer needs both, and a surface that
    /// carried only the target would leave every space-wide flag anonymous.
    pub author_did: String,
    /// The signal's current status (`open`/`closed`).
    pub status: &'static str,
    /// Why the signal is closed (`resolved`/`declined`/`withdrawn`/
    /// `superseded`), `None` while it is open.
    ///
    /// `status` alone collapses two different endings: a decision the space
    /// settled and one whose heading was deleted out from under it both read
    /// `closed`. A caller counting "nothing is open" as agreement scores the
    /// second as the first.
    pub close_reason: Option<&'static str>,
    /// The CREATED record's wall-clock timestamp (Unix millis).
    pub created_ms: i64,
    /// The close time (Unix millis) when the signal is currently closed.
    pub closed_ms: Option<i64>,
}

/// The stable lowercase label for a status. Tombstoned is included for the
/// transition audit trail only; projections never surface it.
fn status_label(status: &SignalStatus) -> &'static str {
    kutl_signals::summary::status_label(status)
}

/// The close reason the fold's winning CLOSED transition carries, as the same
/// wire label the transition trail and the relay's detail read use. `None`
/// unless the signal is currently closed.
///
/// The fold hands back the RAW discriminant off the winning transition, so a
/// value this build has no name for reaches here — a variant a newer relay
/// added, or one a newer peer authored whose records are in the local
/// segments. It is reported as NO reason rather than coerced.
///
/// Coercing would defeat the field. It exists because `closed` collapses two
/// different endings, and `resolved` is the label on the agreement side: a
/// caller counting closed-and-resolved as settled would score an ending it
/// cannot read as agreement. `None` says only what is true — this build cannot
/// name why — and both consumers already handle it, since an open signal
/// reports the same.
fn close_reason_label(state: &SignalState) -> Option<&'static str> {
    state
        .close_reason()
        .and_then(|reason| kutl_proto::sync::CloseReason::try_from(reason).ok())
        .map(kutl_signals::payloads::close_reason_to_wire)
}

/// Build a [`SignalView`] from one folded signal.
fn view_of(id: &str, state: &SignalState, space_id: &str, space_name: &str) -> SignalView {
    // The signal's own fields come from the shared summary — the same values
    // MCP `list_signals` returns. The CLI adds only what is CLI-shaped: which
    // registered space this came out of, which a relay-side caller already
    // knows because they asked about one space.
    let s = kutl_signals::summary::summarize(id, state);
    SignalView {
        id: s.id,
        space_id: space_id.to_owned(),
        space_name: space_name.to_owned(),
        document_id: s.document_id,
        kind: s.kind,
        message: s.message,
        flag_kind: s.flag_kind,
        audience: s.audience,
        target_did: s.target_did,
        author_did: s.author_did,
        status: s.status,
        close_reason: close_reason_label(state),
        created_ms: s.created_ms,
        closed_ms: s.closed_ms,
    }
}

/// The filters a `list` applies to a folded space, independent of where the
/// space came from — the seam that keeps the render logic testable.
///
/// Two orthogonal kind axes are available:
/// - `kind`: record type — `flag`, `chat`, `decision`, `reply`.
/// - `flag_kind`: the flag's intent-kind discriminant — `info`, `question`,
///   `blocked`, etc. Only flags carry a flag-kind; other record types are
///   rejected when this filter is set.
struct ListFilters<'a> {
    status: StatusFilter,
    kind: Option<SignalKind>,
    doc: Option<&'a str>,
    /// Filter by flag intent-kind (`FlagKind` i32 discriminant). Only signals
    /// whose CREATED payload is a `Flag` with a matching `kind` field pass.
    flag_kind: Option<i32>,
}

impl ListFilters<'_> {
    /// Whether one folded signal passes every active filter. Tombstoned is
    /// rejected by the status filter, so it is never admitted here.
    fn admits(&self, state: &SignalState) -> bool {
        if !self.status.admits(&state.status) {
            return false;
        }
        if let Some(want) = self.kind
            && kutl_signals::summary::kind_of(&state.created) != Some(want.to_shared())
        {
            return false;
        }
        if let Some(want) = self.doc
            && state.created.document_id.as_deref() != Some(want)
        {
            return false;
        }
        if let Some(want) = self.flag_kind {
            let is_match = matches!(
                &state.created.payload,
                Some(signal::Payload::Flag(f)) if f.kind == want
            );
            if !is_match {
                return false;
            }
        }
        true
    }
}

/// Collect the [`SignalView`]s from one folded space that pass `filters`,
/// ascending by signal id (the fold's deterministic order).
fn views_for_space(
    fold: &SpaceSignalState,
    space_id: &str,
    space_name: &str,
    filters: &ListFilters<'_>,
) -> Vec<SignalView> {
    fold.iter()
        .filter(|(_, state)| filters.admits(state))
        .map(|(id, state)| view_of(id, state, space_id, space_name))
        .collect()
}

/// One signal's log entry for `kutl document log`: a timestamp key and the
/// pre-rendered block, so the caller can merge it into the change timeline
/// without re-deriving any signal detail.
pub struct SignalLogEntry {
    /// The CREATED record's wall-clock timestamp (Unix millis) — the sort key
    /// that interleaves this entry into the change timeline.
    pub timestamp_ms: i64,
    /// The rendered log block for this signal (no trailing newline).
    pub block: String,
}

/// Load the signals attached to `document_id` in the given space and render
/// each as a [`SignalLogEntry`] for interleaving into `kutl document log`.
///
/// Reads the space's local segments directly (no daemon, no lock). Tombstoned
/// signals are hidden — the projection never surfaces a soft-deleted signal.
///
/// # Errors
///
/// Returns an error if the space's segments cannot be read.
pub fn document_log_signals(
    space_root: &Path,
    space_id: &str,
    document_id: &str,
) -> Result<Vec<SignalLogEntry>> {
    let fold = fold_space_segments(space_root, space_id)?;
    let entries = fold
        .iter()
        .filter(|(_, state)| !matches!(state.status, SignalStatus::Tombstoned))
        .filter(|(_, state)| state.created.document_id.as_deref() == Some(document_id))
        .map(|(id, state)| SignalLogEntry {
            timestamp_ms: state.created.timestamp,
            block: render_signal_log_block(id, state),
        })
        .collect();
    Ok(entries)
}

/// The signals attached to `document_id`, as the same [`SignalView`]
/// projection `signal list --format json` emits — the structured counterpart
/// of [`document_log_signals`] for `document log --format json`.
///
/// # Errors
///
/// Returns an error if the space's segments cannot be read.
pub fn document_signal_views(
    space_root: &Path,
    space_id: &str,
    document_id: &str,
) -> Result<Vec<SignalView>> {
    let fold = fold_space_segments(space_root, space_id)?;
    let space_name = kutl_client::KutlspaceConfig::display_name(space_root, space_id);
    Ok(fold
        .iter()
        .filter(|(_, state)| !matches!(state.status, SignalStatus::Tombstoned))
        .filter(|(_, state)| state.created.document_id.as_deref() == Some(document_id))
        .map(|(id, state)| view_of(id, state, space_id, &space_name))
        .collect())
}

/// Render one signal as a `kutl document log` block, distinct from a change
/// entry (a `signal <id>` header rather than `change <id>`).
pub(crate) fn render_signal_log_block(id: &str, state: &SignalState) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "signal {id}");
    let _ = writeln!(
        out,
        "Kind:   {kind}",
        kind = kind_label(&state.created).unwrap_or("-")
    );
    // Addressed flags name their recipient here too — the timeline is a
    // surface a reader scans for signals that concern them, same as the list.
    let s = kutl_signals::summary::summarize(id, state);
    if let Some(who) = addressed_to(s.target_did.as_deref(), s.audience) {
        let _ = writeln!(out, "For:    {who}");
    }
    let _ = writeln!(
        out,
        "Status: {status}",
        status = status_label(&state.status)
    );
    let _ = write!(
        out,
        "Date:   {}",
        crate::format_timestamp(state.created.timestamp)
    );
    out
}

/// Render a flat list of signal views as a terse, aligned human table grouped
/// by space then document. Empty input renders a short notice.
fn render_list_human(views: &[SignalView]) -> String {
    let mut out = String::new();
    if views.is_empty() {
        let _ = writeln!(out, "no signals");
        return out;
    }
    let mut last_space: Option<&str> = None;
    let mut last_doc: Option<&Option<String>> = None;
    for v in views {
        if last_space != Some(v.space_id.as_str()) {
            let _ = writeln!(
                out,
                "space {name} ({id})",
                name = v.space_name,
                id = v.space_id
            );
            last_space = Some(v.space_id.as_str());
            last_doc = None;
        }
        if last_doc != Some(&v.document_id) {
            let doc = v.document_id.as_deref().unwrap_or("(space-level)");
            let _ = writeln!(out, "  {doc}");
            last_doc = Some(&v.document_id);
        }
        let flag_kind_col = v.flag_kind.unwrap_or("-");
        let preview = v
            .message
            .as_deref()
            .map(message_preview)
            .unwrap_or_default();
        let _ = write!(
            out,
            "    {status:<6} {kind:<8} {flag_kind:<18} {id}  {preview}",
            status = v.status,
            kind = v.kind.unwrap_or("-"),
            flag_kind = flag_kind_col,
            id = v.id,
        );
        // Who raised it, in the same `by <did>` grammar the per-signal
        // transition trail uses — one spelling of attribution across both
        // reads. Before the `→` marker so the row says who is asking before
        // who was asked.
        let _ = write!(out, "  by {author}", author = v.author_did);
        // A space broadcast stays unmarked; anything narrower names its
        // recipient, so the marker's presence alone distinguishes "for the
        // space" from "for someone" without opening the signal.
        if let Some(who) = addressed_to(v.target_did.as_deref(), v.audience) {
            let _ = write!(out, "  → {who}");
        }
        let _ = writeln!(out);
    }
    out
}

/// Who a signal addresses when that is narrower than the whole space: the
/// recipient DID when one is named, the audience label otherwise (a legacy
/// group audience reads back even though it can no longer be authored).
/// `None` for space broadcasts and for kinds that carry no audience.
fn addressed_to<'a>(target_did: Option<&'a str>, audience: Option<&'a str>) -> Option<&'a str> {
    match (target_did, audience) {
        (Some(did), _) => Some(did),
        (None, Some(aud)) if aud != space_audience_label() => Some(aud),
        _ => None,
    }
}

/// The label the shared summary gives the space broadcast — derived from the
/// typed audience so this render and the summary cannot spell it apart.
fn space_audience_label() -> &'static str {
    kutl_proto::vocab::audience_type_to_str(i32::from(kutl_proto::sync::AudienceType::Space))
}

/// Render one signal's detail plus its transition history as human text.
fn render_view_human(view: &SignalView, transitions: &[TransitionEntry]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "signal {id}", id = view.id);
    let _ = writeln!(
        out,
        "  space:    {name} ({id})",
        name = view.space_name,
        id = view.space_id
    );
    if let Some(doc) = view.document_id.as_deref() {
        let _ = writeln!(out, "  document: {doc}");
    }
    let _ = writeln!(out, "  kind:     {kind}", kind = view.kind.unwrap_or("-"));
    if let Some(fk) = view.flag_kind {
        let _ = writeln!(out, "  flag-kind: {fk}");
    }
    // Who the signal is for: the recipient DID when one is named, the
    // audience label otherwise (`space` is the broadcast).
    if let Some(aud) = view.audience {
        let who = view.target_did.as_deref().unwrap_or(aud);
        let _ = writeln!(out, "  for:      {who}");
    }
    if let Some(ref msg) = view.message {
        let _ = writeln!(out, "  message:  {msg}");
    }
    let _ = writeln!(out, "  status:   {status}", status = view.status);
    // The trail below reports the reason per transition; a reader asking what
    // this signal IS should not have to reconstruct it from the trail.
    if let Some(reason) = view.close_reason {
        let _ = writeln!(out, "  reason:   {reason}");
    }
    let _ = writeln!(
        out,
        "  created:  {}",
        crate::format_timestamp(view.created_ms)
    );
    if let Some(closed) = view.closed_ms {
        let _ = writeln!(out, "  closed:   {}", crate::format_timestamp(closed));
    }
    let _ = writeln!(out, "history:");
    for t in transitions {
        let actor = t.actor_did.as_deref().unwrap_or("-");
        let _ = writeln!(
            out,
            "  {when}  {event:<10} by {actor}",
            when = crate::format_timestamp(t.timestamp_ms),
            event = t.event,
        );
        // The close reason and note ride the record itself. Indented under
        // their event so the trail still reads as one line per transition at
        // a glance.
        if let Some(ref reason) = t.close_reason {
            let _ = writeln!(out, "              reason: {reason}");
        }
        if let Some(ref note) = t.note {
            let _ = writeln!(out, "              note:   {note}");
        }
    }
    out
}

/// A registered space and the fold of its local segments.
struct FoldedSpace {
    space_id: String,
    space_name: String,
    space_root: PathBuf,
    fold: SpaceSignalState,
}

/// The per-space segment directory — the shared layout definition.
fn segments_dir(space_root: &Path, space_id: &str) -> PathBuf {
    kutl_signals::segment::signals_dir(space_root, space_id)
}

/// Load one space's local segments and fold them into the per-signal LWW
/// state. A space with no segment directory folds to an empty state (not an
/// error). The one segment-read shared by every local signal projection.
fn fold_space_segments(space_root: &Path, space_id: &str) -> Result<SpaceSignalState> {
    let dir = segments_dir(space_root, space_id);
    let store = SegmentStore::load(&dir)
        .with_context(|| format!("loading signal segments from {}", dir.display()))?;
    let mut fold = SpaceSignalState::default();
    for record in store.records {
        fold.apply(record);
    }
    Ok(fold)
}

/// Load and fold one space's local segments by its root directory.
fn folded_space_at(space_root: &Path) -> Result<FoldedSpace> {
    folded_space_from(space_root, kutl_client::SpaceConfig::load(space_root)?)
}

/// Fold one space's local segments given its already-loaded config.
fn folded_space_from(space_root: &Path, config: kutl_client::SpaceConfig) -> Result<FoldedSpace> {
    let space_name = kutl_client::KutlspaceConfig::display_name(space_root, &config.space_id);
    let fold = fold_space_segments(space_root, &config.space_id)?;
    Ok(FoldedSpace {
        space_id: config.space_id,
        space_name,
        space_root: space_root.to_path_buf(),
        fold,
    })
}

/// Enumerate registered spaces, loading and folding each one's local segments.
fn folded_spaces() -> Result<Vec<FoldedSpace>> {
    let registry_path = crate::space::registry_path()?;
    let registry = kutl_client::SpaceRegistry::load(&registry_path)
        .with_context(|| format!("loading the space registry at {}", registry_path.display()))?;

    let mut out = Vec::new();
    for path_str in &registry.spaces {
        let space_root = PathBuf::from(path_str);
        if !kutl_client::SpaceConfig::is_joined(&space_root) {
            continue; // A stale registry entry is skipped, not fatal.
        }
        // One unreadable config must not take the cross-space view down
        // with it: warn and move on to the spaces that can be read.
        let config = match kutl_client::SpaceConfig::load(&space_root) {
            Ok(config) => config,
            Err(e) => {
                eprintln!(
                    "warning: skipping {}: its space config cannot be read: {e:#}",
                    space_root.display()
                );
                continue;
            }
        };
        out.push(folded_space_from(&space_root, config)?);
    }
    Ok(out)
}

/// Handle `kutl signal list`.
///
/// Cwd-first: inside a space, the list (and any `--fetch`) scopes to that
/// space; outside one, it is the all-registered-spaces overview.
///
/// # Errors
///
/// Returns an error if the registry or a space's segments cannot be read, or
/// if a `--fetch` pull fails (network, auth, or a daemon-held segment lock).
pub async fn cmd_signal_list(args: SignalListArgs) -> Result<()> {
    let scope_root = crate::cwd_enclosing_space()?;
    if args.fetch {
        fetch_spaces(scope_root.as_deref()).await?;
    }

    let spaces = match &scope_root {
        Some(root) => vec![folded_space_at(root)?],
        None => folded_spaces()?,
    };
    let status = status_filter_from_flags(args.status.open, args.status.closed, args.status.all);
    let filters = ListFilters {
        status,
        kind: args.kind,
        doc: args.doc.as_deref(),
        flag_kind: args.flag_kind.map(|k| i32::from(k.to_proto())),
    };

    let mut views = Vec::new();
    for space in &spaces {
        views.extend(views_for_space(
            &space.fold,
            &space.space_id,
            &space.space_name,
            &filters,
        ));
    }

    match args.format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&views)?);
        }
        OutputFormat::Human => {
            print!("{}", render_list_human(&views));
            // A `--fetch`-less list reads only the local mirror; if it came back
            // empty, the user may just be missing a pull rather than have no
            // signals — point them at the flag rather than leave them guessing.
            if views.is_empty() && !args.fetch {
                println!("run `kutl signal list --fetch` to pull the latest from the relay");
            }
        }
    }
    Ok(())
}

/// Handle `kutl signal view <id>`.
///
/// Cwd-first, like `list`: inside a space, the id is looked up in that
/// space's fold; outside one, across all registered spaces.
///
/// # Errors
///
/// Returns an error if the registry or segments cannot be read, the signal id
/// is not found in scope, or a `--fetch` pull fails.
pub async fn cmd_signal_view(id: &str, format: OutputFormat, fetch: bool) -> Result<()> {
    let scope_root = crate::cwd_enclosing_space()?;
    if fetch {
        fetch_spaces(scope_root.as_deref()).await?;
    }

    let spaces = match &scope_root {
        Some(root) => vec![folded_space_at(root)?],
        None => folded_spaces()?,
    };
    // Resolve the id (or unique prefix, git-style) within scope; outside a
    // space the prefix must be unique across every registered space.
    let mut candidates: Vec<(&FoldedSpace, String)> = Vec::new();
    for s in &spaces {
        if let Some(full) = find_signal_by_prefix(&s.fold, id)? {
            candidates.push((s, full));
        }
    }
    let (space, full_id) = match candidates.len() {
        1 => candidates.remove(0),
        0 => match &scope_root {
            // Only suggest --fetch when this read didn't already pull.
            Some(_) if !fetch => anyhow::bail!(
                "signal {id} not found in space '{name}' — run `kutl signal view {id} --fetch` to pull the latest from the relay",
                name = spaces[0].space_name
            ),
            Some(_) => anyhow::bail!(
                "signal {id} not found in space '{name}'",
                name = spaces[0].space_name
            ),
            None => anyhow::bail!("signal {id} not found in any registered space"),
        },
        _ => {
            let listed = candidates
                .iter()
                .map(|(s, full)| format!("{full} (space '{}')", s.space_name))
                .collect::<Vec<_>>()
                .join("\n  ");
            anyhow::bail!(
                "signal id prefix '{id}' is ambiguous across registered spaces — candidates:\n  {listed}"
            )
        }
    };
    let id = full_id.as_str();
    let state = space
        .fold
        .get(id)
        .expect("signal id present — resolved by prefix match above");
    let view = view_of(id, state, &space.space_id, &space.space_name);

    // Transition history comes from the RAW segment records for this id (the
    // fold collapses history under LWW), sorted by wall-clock then record id.
    let dir = segments_dir(&space.space_root, &space.space_id);
    let store = SegmentStore::load(&dir)
        .with_context(|| format!("loading signal segments from {}", dir.display()))?;
    let transitions = transition_history(id, &store);

    match format {
        OutputFormat::Json => {
            let payload = serde_json::json!({
                "signal": view,
                "history": transitions,
            });
            println!("{}", serde_json::to_string_pretty(&payload)?);
        }
        OutputFormat::Human => {
            print!("{}", render_view_human(&view, &transitions));
        }
    }
    Ok(())
}

/// Handle `kutl signal close <id>` — append a CLOSED transition record via the
/// relay's transition endpoint.
///
/// # Errors
///
/// Returns an error if the signal cannot be resolved to exactly one registered
/// space, no relay token is available, or the relay rejects the transition.
pub async fn cmd_signal_close(id: &str, reason: Option<CloseReasonArg>) -> Result<()> {
    apply_transition(id, build_transition_request(TransitionVerb::Close, reason)).await
}

/// Handle `kutl signal reopen <id>` — append a REOPENED transition record.
///
/// # Errors
///
/// See [`cmd_signal_close`].
pub async fn cmd_signal_reopen(id: &str) -> Result<()> {
    apply_transition(id, build_transition_request(TransitionVerb::Reopen, None)).await
}

/// Handle `kutl signal resolve <id>` — sugar for `close --reason resolved`.
///
/// # Errors
///
/// See [`cmd_signal_close`].
pub async fn cmd_signal_resolve(id: &str) -> Result<()> {
    apply_transition(id, build_transition_request(TransitionVerb::Resolve, None)).await
}

/// Handle `kutl signal reply <parent_signal_id> --message <body>` — append a
/// reply record via the relay.
///
/// Gives the CLI parity with MCP replies, so the person who raised a signal
/// can answer one the same way an agent can. The space is the one enclosing
/// the current directory, the same way the transition verbs resolve theirs;
/// the PARENT's id must be present in its local fold.
///
/// `parent_reply` nests this reply inside an existing thread; omitted, it
/// answers the signal directly.
///
/// Read-your-writes is intentionally dropped, as for the transition verbs: the
/// reply reaches the local mirror via daemon ingest or a later
/// `signal list --fetch`.
///
/// # Errors
///
/// Returns an error if the registry/segments cannot be read, the parent signal
/// is not found (or is ambiguous) across registered spaces, the relay token is
/// unavailable, or the relay rejects the reply.
pub async fn cmd_signal_reply(
    parent_signal_id: &str,
    message: &str,
    parent_reply: Option<&str>,
) -> Result<()> {
    let (space, parent_signal_id) = resolve_signal_space(parent_signal_id)?;
    let parent_signal_id = parent_signal_id.as_str();
    let config = kutl_client::SpaceConfig::load(&space.space_root)
        .with_context(|| format!("loading the space config at {}", space.space_root.display()))?;
    let token = kutl_client::resolve_or_authenticate(&config.relay_url)
        .await
        .context("authenticating to the relay for the signal reply")?;

    let ack = submit_signal_frame(
        &config.relay_url,
        &token,
        kutl_proto::protocol::submit_reply_envelope(
            &new_client_ref(),
            &space.space_id,
            parent_signal_id,
            parent_reply,
            message,
        ),
    )
    .await
    .with_context(|| format!("transmitting the reply to signal {parent_signal_id}"))?;

    // The ack carries the minted id directly.
    println!("replied to {parent_signal_id} as {}", ack.signal_id);
    Ok(())
}

/// The client name this CLI presents at the relay handshake.
const CLI_CLIENT_NAME: &str = "kutl-cli";

/// Open a connection, send ONE authored submit frame, wait for its ack, close.
///
/// Every signal-writing verb needs the identical shape — connect, submit,
/// await the ack keyed by `client_ref`, hang up — so it lives here once
/// rather than three times.
///
/// A connection per invocation, deliberately: a CLI verb is a one-shot, and
/// the alternative is a daemon-shaped session for a command that exits
/// immediately. The handshake cost is one round trip.
async fn submit_signal_frame(
    relay_url: &str,
    token: &str,
    envelope: kutl_proto::sync::SyncEnvelope,
) -> Result<kutl_proto::sync::SignalAck> {
    let mut client =
        kutl_client::SyncClient::connect_with_auth(relay_url, CLI_CLIENT_NAME, token, "")
            .await
            .context("connecting to the relay")?;
    let ack = client.submit_signal(&envelope).await;
    // Close on the failure path too: a refused submit is a normal outcome, and
    // leaking the socket would leave the relay holding a connection for a
    // process that is about to exit.
    let _ = client.close().await;
    ack
}

/// A fresh correlation id for one submit. The relay echoes it on the ack, which
/// is how the reply is matched to the request rather than to whatever frame
/// happened to arrive next.
fn new_client_ref() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Resolve the signal's space, load the relay token, submit the transition
/// intent to the relay (the relay authors + attests a relay-vouched
/// record with `actor_did` == the authenticated caller), and print a
/// one-line confirmation. Shared by all three verbs.
///
/// The space is the one enclosing the current directory (cwd-first, like
/// every authoring verb); the signal `id` must be present in its local fold.
///
/// Read-your-writes is intentionally dropped: the transition lands in
/// the local mirror only via daemon ingest or a subsequent `signal list --fetch`
/// — the CLI never signs or appends records locally.
///
/// # Errors
///
/// Returns an error if the current directory is not inside a space, the
/// segments cannot be read, the signal is not in the space's fold, the relay
/// token is unavailable, or the relay rejects the transition.
async fn apply_transition(
    id: &str,
    request: kutl_client::signal_catchup::TransitionRequest,
) -> Result<()> {
    let (space, id) = resolve_signal_space(id)?;
    let id = id.as_str();

    // Per-space relay URL comes from the space's own config (mirrors
    // `fetch_one_space`). The bearer token comes from the stored credentials
    // chain when present (hosted kutlhub PAT), else a did:key challenge against
    // this space's relay (self-hosted OSS relay).
    let config = kutl_client::SpaceConfig::load(&space.space_root)
        .with_context(|| format!("loading the space config at {}", space.space_root.display()))?;
    let token = kutl_client::resolve_or_authenticate(&config.relay_url)
        .await
        .context("authenticating to the relay for the signal transition")?;

    // Submit the keyless intent: the relay mints + attests the transition
    // record with `actor_did` == the authenticated caller and broadcasts it.
    let event = match request.event {
        kutl_client::signal_catchup::TransitionEvent::Closed => {
            kutl_proto::sync::SignalEventType::Closed
        }
        kutl_client::signal_catchup::TransitionEvent::Reopened => {
            kutl_proto::sync::SignalEventType::Reopened
        }
    };
    let close_reason = request
        .reason
        .as_deref()
        .map(kutl_signals::payloads::close_reason_from_wire);
    submit_signal_frame(
        &config.relay_url,
        &token,
        kutl_proto::protocol::submit_transition_envelope(
            &new_client_ref(),
            &space.space_id,
            id,
            event,
            close_reason,
            None,
        ),
    )
    .await
    .with_context(|| format!("applying the transition to signal {id}"))?;

    print_transition_confirmation(id, &request);
    Ok(())
}

/// The space a transition or reply targets: the one enclosing the current
/// directory. The signal `id` must be present in that space's local fold —
/// authoring verbs are cwd-scoped, so a signal that lives in a different
/// space is reported as absent rather than resolved behind the user's back.
fn resolve_signal_space(id: &str) -> Result<(FoldedSpace, String)> {
    let root = crate::require_cwd_space()?;
    let space = folded_space_at(&root)?;
    let Some(full_id) = find_signal_by_prefix(&space.fold, id)? else {
        anyhow::bail!(
            "signal {id} not found in space '{name}' — run `kutl signal list --fetch` to pull the latest, or `cd` into the space that owns it",
            name = space.space_name
        );
    };
    Ok((space, full_id))
}

/// Minimum length of a signal id prefix. Signal ids are uuids nobody should
/// have to type in full; four characters keeps prefixes convenient while an
/// accidental match against a space's handful of signals stays unlikely —
/// and an actual collision is refused loudly, never guessed through.
const MIN_SIGNAL_ID_PREFIX: usize = 4;

/// Resolve a user-supplied signal id — full or a unique prefix, git-style —
/// against one fold. `Ok(Some(full_id))` on an exact or unique-prefix match,
/// `Ok(None)` when nothing matches (the caller owns the not-found wording),
/// `Err` on a too-short prefix or an ambiguous one (listing the candidates).
fn find_signal_by_prefix(fold: &SpaceSignalState, input: &str) -> Result<Option<String>> {
    // An exact id always wins, whatever its length.
    if fold.get(input).is_some() {
        return Ok(Some(input.to_owned()));
    }
    if input.len() < MIN_SIGNAL_ID_PREFIX {
        anyhow::bail!(
            "'{input}' is too short — pass a signal id or a prefix of at least {MIN_SIGNAL_ID_PREFIX} characters"
        );
    }
    let matches: Vec<&str> = fold
        .iter()
        .map(|(id, _)| id.as_str())
        .filter(|id| id.starts_with(input))
        .collect();
    match matches.len() {
        0 => Ok(None),
        1 => Ok(Some(matches[0].to_owned())),
        _ => {
            let listed = matches.join("\n  ");
            anyhow::bail!("signal id prefix '{input}' is ambiguous — candidates:\n  {listed}")
        }
    }
}

/// Print a one-line human confirmation of an applied transition.
fn print_transition_confirmation(
    id: &str,
    request: &kutl_client::signal_catchup::TransitionRequest,
) {
    use kutl_client::signal_catchup::TransitionEvent;
    match request.event {
        TransitionEvent::Closed => {
            let reason = request.reason.as_deref().unwrap_or("resolved");
            println!("closed {id} ({reason})");
        }
        TransitionEvent::Reopened => println!("reopened {id}"),
    }
}

/// The transition audit trail for one signal id, from raw segment records.
///
/// Delegates to `kutl_signals::summary` — the same derivation MCP
/// `get_signal_detail` returns, so the CLI's `signal view` and an agent's
/// detail read show the same history in the same order.
fn transition_history(id: &str, store: &SegmentStore) -> Vec<TransitionEntry> {
    kutl_signals::summary::transition_history(id, &store.records)
}

/// Truncate a message to at most this many characters for the list preview.
const MESSAGE_PREVIEW_LEN: usize = 60;

/// Return a short, single-line preview of `msg` for the human list column.
/// Collapses any whitespace run (incl. newlines) to a single space, then clips
/// at [`MESSAGE_PREVIEW_LEN`] characters, appending `…` when clipped. Clips by
/// CHARACTERS, not bytes, so arbitrary user text (emoji, accents, CJK) never
/// panics on a mid-codepoint boundary.
fn message_preview(msg: &str) -> String {
    let flat = msg.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut clipped: String = flat.chars().take(MESSAGE_PREVIEW_LEN).collect();
    if flat.chars().count() > MESSAGE_PREVIEW_LEN {
        clipped.push('…');
    }
    clipped
}

/// Run a one-shot `--fetch` — against the one space at `only_root` when the
/// caller is standing inside a space, else against every registered space —
/// pulling records the relay holds into local segments under the store's
/// single-writer lock.
///
/// # Errors
///
/// Returns an error if the registry cannot be read, the segment lock is held
/// by the running daemon, or a relay request fails.
async fn fetch_spaces(only_root: Option<&Path>) -> Result<()> {
    let roots: Vec<PathBuf> = if let Some(root) = only_root {
        vec![root.to_path_buf()]
    } else {
        let registry_path = crate::space::registry_path()?;
        let registry = kutl_client::SpaceRegistry::load(&registry_path).with_context(|| {
            format!("loading the space registry at {}", registry_path.display())
        })?;
        registry.spaces.iter().map(PathBuf::from).collect()
    };

    // A challenge-minted token is relay-specific, so resolve a token PER RELAY
    // inside the loop (keyed on each space's `relay_url`), not once up front.
    // Cache by relay_url so repeated relays don't re-run the challenge flow;
    // the stored-credentials chain (hosted PAT) short-circuits before any
    // network call regardless.
    let mut tokens: HashMap<String, String> = HashMap::new();

    for space_root in &roots {
        let Ok(config) = kutl_client::SpaceConfig::load(space_root) else {
            continue;
        };
        if !tokens.contains_key(&config.relay_url) {
            let token = kutl_client::resolve_or_authenticate(&config.relay_url)
                .await
                .with_context(|| format!("authenticating to the relay at {}", config.relay_url))?;
            tokens.insert(config.relay_url.clone(), token);
        }
        let token = &tokens[&config.relay_url];
        fetch_one_space(space_root, &config.space_id, &config.relay_url, token).await?;
    }
    Ok(())
}

/// Pull records for one space into its local segments. Takes the daemon signal
/// store's single-writer lock. A running daemon holding that lock is not an
/// error: the daemon is already live-syncing the very records a fetch would
/// pull, so the fetch is skipped with a one-line stderr note and the caller
/// serves the local fold.
///
/// # Errors
///
/// Returns an error if the space id is not a UUID, the lock cannot be taken
/// for a reason OTHER than a live daemon, a relay page fails, or an
/// append/cursor write fails.
async fn fetch_one_space(
    space_root: &Path,
    space_id: &str,
    relay_url: &str,
    token: &str,
) -> Result<()> {
    let uuid = uuid::Uuid::parse_str(space_id)
        .with_context(|| format!("space id {space_id} is not a uuid — cannot key its segments"))?;
    // Opening the store takes the single-writer flock.
    let mut store = match kutl_daemon::signal_store::DaemonSignalStore::open(space_root, uuid) {
        Ok(store) => store,
        Err(e)
            if e.downcast_ref::<kutl_signals::Error>()
                .is_some_and(|se| matches!(se, kutl_signals::Error::Locked { .. })) =>
        {
            eprintln!(
                "note: the daemon is live-syncing {space_id} — skipped the fetch, serving the local fold"
            );
            return Ok(());
        }
        Err(e) => {
            return Err(e.context(format!("cannot open the signal store for {space_id}")));
        }
    };

    let dir = store.dir().to_path_buf();
    // The persisted cursor can sit mid-`physical_ms`-group (ingest advances
    // it per record), and the relay's serve filter is coarse — resume one
    // millisecond early so the cursor's own group is re-served rather than
    // half-skipped. Overlap is idempotent; see
    // `kutl_signals::catchup::resume_floor`.
    let mut since = kutl_signals::catchup::load_cursor(&dir)
        .context("loading the signal catch-up cursor")?
        .and_then(|c| kutl_signals::catchup::resume_floor(&c));

    // One connection for the whole walk. The relay answers each
    // `SubscribeSignals` with ONE page and expects a re-subscribe carrying the
    // returned cursor; the cursor is ours to carry, the relay keeps none.
    let mut client =
        kutl_client::SyncClient::connect_with_auth(relay_url, CLI_CLIENT_NAME, token, "")
            .await
            .context("connecting to the relay to fetch signals")?;

    loop {
        let page = client
            .fetch_signal_page(space_id, since.clone())
            .await
            .context("fetching a signal catch-up page")?;
        for record in &page.records {
            append_fetched(&mut store, record, &dir)?;
        }
        if !page.more {
            break;
        }
        // The relay's own high-water for the page it just sent. Re-deriving it
        // from the last record would split a millisecond group and lose the
        // remainder, which is the boundary rule `catchup::page` exists to hold.
        since.clone_from(&page.cursor);
    }
    let _ = client.close().await;
    Ok(())
}

/// Append one fetched record and advance the cursor, dropping legacy bare
/// broadcasts (empty `record_id`). Mirrors the daemon ingest's append+cursor
/// discipline; the segment store dedups a re-appended `record_id` internally.
fn append_fetched(
    store: &mut kutl_daemon::signal_store::DaemonSignalStore,
    record: &Signal,
    dir: &Path,
) -> Result<()> {
    if record.record_id.is_empty() {
        return Ok(()); // legacy bare broadcast — not a durable record
    }
    store
        .append(record)
        .context("appending a fetched signal record to local segments")?;
    if let Some(ref hlc) = record.hlc {
        kutl_signals::catchup::save_cursor(dir, hlc)
            .with_context(|| format!("saving the signal cursor at {}", dir.display()))?;
    }
    Ok(())
}

/// `kutl signal create` — create a flag via the relay-mint CREATE path,
/// exactly like MCP `create_flag`: the relay authors + attests a relay-vouched
/// record with `author_did` == the authenticated caller and mints the signal id.
/// A space-level flag (no `--doc`) carries `document_id: None`; `--doc` binds it
/// to a tracked document.
///
/// Read-your-writes is intentionally dropped: the created flag lands
/// in the local mirror only via daemon ingest or a subsequent `signal list
/// --fetch` — the CLI never signs or appends records locally.
///
/// # Errors
///
/// Returns an error if the kind/audience is invalid, the space cannot be
/// resolved, the relay token is unavailable, or the relay rejects the record.
pub async fn cmd_signal_create(args: SignalCreateArgs) -> Result<()> {
    use kutl_proto::sync::AudienceType;

    // `comment` needs the inline-marker signal_id + anchor_text (editor/MCP).
    // It stays in the kind vocabulary because `signal list --flag-kind comment`
    // is a legitimate read; only authoring it here is refused.
    if args.kind == FlagKindArg::Comment {
        anyhow::bail!(
            "comment flags are created via the editor/MCP inline-marker flow, not the CLI"
        );
    }

    // Validate the audience up front for a clear CLI error (the relay re-parses
    // it at its boundary; validating here keeps the message local and lists the
    // accepted values). `--kind` needs no such check: clap rejects an unknown
    // one before this runs.
    let audience_i =
        kutl_proto::vocab::authorable_audience_from_str(&args.audience).with_context(|| {
            format!(
                "invalid --audience '{}' (one of {})",
                args.audience,
                kutl_proto::vocab::AUTHORABLE_AUDIENCES.join(", ")
            )
        })?;
    // What the caller typed. Whether it names a DID or a person is settled
    // after authentication, since resolving a name takes a relay round-trip;
    // the pair rules below only care whether a recipient was named at all.
    let recipient = args.to.clone().unwrap_or_default();

    // Mirror MCP create_flag validation exactly.
    if audience_i == i32::from(AudienceType::Participant) && recipient.is_empty() {
        anyhow::bail!("--to is required when --audience is 'participant'");
    }
    if audience_i == i32::from(AudienceType::Space) && !recipient.is_empty() {
        anyhow::bail!(
            "--to must be empty when --audience is 'space' (a broadcast); \
             use --audience participant to target a user"
        );
    }

    let space_root = crate::require_cwd_space()?;
    let config = kutl_client::SpaceConfig::load(&space_root)
        .with_context(|| format!("loading the space config at {}", space_root.display()))?;

    // `--doc` binds to a tracked document id; omitted => a space-level flag.
    let document_id = match args.doc.as_deref() {
        Some(path) => Some(resolve_document_id(&space_root, path)?),
        None => None,
    };

    let token = kutl_client::resolve_or_authenticate(&config.relay_url)
        .await
        .context("authenticating to the relay for the signal create")?;

    let target_did = resolve_recipient(&config, &token, &recipient).await?;

    // Submit the keyless intent: the relay mints + attests the CREATED flag with
    // `author_did` == the authenticated caller and broadcasts it.
    let ack = submit_signal_frame(
        &config.relay_url,
        &token,
        kutl_proto::protocol::submit_flag_envelope(
            &new_client_ref(),
            &config.space_id,
            document_id.as_deref(),
            args.kind.to_proto(),
            &args.message,
            // The typed `Audience` is derived from whether a target was named,
            // so the `--audience`/`--to` pair is validated above and
            // then collapses to this one question.
            (!target_did.is_empty()).then_some(target_did.as_str()),
        ),
    )
    .await
    .context("transmitting the signal create to the relay")?;

    println!("created {} flag {}", args.kind.label(), ack.signal_id);
    Ok(())
}

/// Resolve what `--to` named to the DID the record carries.
///
/// A DID is used verbatim and never looked up: it is unambiguous, and a lookup
/// could only turn a working identifier into a failure. Anything else is a
/// person's name, which only the relay can answer for — names live in its
/// authorization list, not on this machine.
///
/// Ambiguity is refused rather than resolved. A name matching two participants
/// is the caller's to settle, because picking one silently sends someone else's
/// mail; the error names the candidates so the caller can pass a DID instead.
///
/// An empty recipient stays empty — the caller is broadcasting.
async fn resolve_recipient(
    config: &kutl_client::SpaceConfig,
    token: &str,
    recipient: &str,
) -> Result<String> {
    if recipient.is_empty() || recipient.starts_with("did:") {
        return Ok(recipient.to_owned());
    }

    // The DID is optional in the handshake; the bearer token carries identity.
    let (proxy, _tools) = crate::watch_tools::RelayProxy::connect(&config.relay_url, token, "")
        .await
        .with_context(|| format!("asking the relay who '{recipient}' is"))?;
    let found: Vec<ResolvedParticipant> = proxy
        .call_tool_json(
            "resolve_participant",
            &serde_json::json!({ "space_id": config.space_id, "name": recipient }),
            &format!("the relay could not resolve '{recipient}'"),
            &format!("reading the relay's answer for '{recipient}'"),
        )
        .await?;

    match found.as_slice() {
        [one] => Ok(one.did.clone()),
        [] => anyhow::bail!(
            "no participant in this space is named '{recipient}' — a participant with no \
             name configured is still reachable by passing their DID to --to"
        ),
        many => {
            let candidates: Vec<String> = many
                .iter()
                .map(|p| format!("{} ({})", p.name, p.did))
                .collect();
            anyhow::bail!(
                "'{recipient}' names {} participants here ({}) — pass one of those DIDs to \
                 --to instead",
                many.len(),
                candidates.join(", ")
            )
        }
    }
}

/// One entry of the relay's answer to `resolve_participant`.
#[derive(serde::Deserialize)]
struct ResolvedParticipant {
    did: String,
    /// Canonical name the relay resolved this DID to — the same name typed
    /// on the `--to` flag when there's exactly one match, but distinct per
    /// candidate when there are several. Ambiguity errors list this instead
    /// of a bare DID so the caller can tell candidates apart.
    name: String,
}

/// Resolve a within-space document path to its tracked document id, read from
/// the daemon's local state map (`<space_root>/.kutl/state.ksnap`). The map is
/// keyed by the path relative to the space root; normalize the user input to
/// that form before looking up. Errors if the path is not tracked yet.
fn resolve_document_id(space_root: &Path, path: &str) -> Result<String> {
    let state = kutl_daemon::state::DaemonState::load_readonly(&space_root.join(".kutl"));
    let rel = normalize_space_rel_path(space_root, path)?;
    state
        .documents
        .get(&rel)
        .map(|entry| entry.id.clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "document '{path}' is not tracked in this space yet \
                 — sync it first, or omit --doc for a space-level flag"
            )
        })
}

/// Normalize a user-supplied document path to the space-relative,
/// forward-slash key form the daemon uses in `state.documents`. The one
/// path→key normalizer, shared by `--doc` resolution here and the
/// `document log`/`blame`/`restore` resolver. Accepts a path relative to
/// the space root or an absolute path inside it; rejects a path outside.
/// Strips leading `CurDir` (`./`) components so `./notes/x.md` and
/// `notes/x.md` both resolve to the same key. Mirrors the daemon's
/// `rel_path_to_string` logic (which operates on already-stripped paths) —
/// replicated here because that helper is `pub(crate)` in `kutl-daemon`.
pub(crate) fn normalize_space_rel_path(space_root: &Path, path: &str) -> Result<String> {
    use std::path::Component;

    let candidate = Path::new(path);
    let rel = if candidate.is_absolute() {
        candidate.strip_prefix(space_root).map_err(|_| {
            anyhow::anyhow!(
                "path {path} is outside the space at {}",
                space_root.display()
            )
        })?
    } else {
        candidate
    };
    // Filter out CurDir (`.`) components so `./notes/x.md` → `notes/x.md`.
    let parts: Vec<_> = rel
        .components()
        .filter(|c| !matches!(c, Component::CurDir))
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    Ok(parts.join("/"))
}

/// How often the `--follow` poll loop checks for new activity (2 s).
///
/// A short interval keeps the live feed snappy without hammering the relay.
/// Named so it can be adjusted at a single call site with no magic literals.
const FEED_FOLLOW_POLL: std::time::Duration = std::time::Duration::from_secs(2);

/// Arguments for `kutl space feed`.
#[derive(clap::Args, Clone)]
pub struct FeedArgs {
    /// Live-tail: keep polling for new activity until interrupted. With
    /// `--format json` the output becomes NDJSON — one record per line —
    /// so it can be appended to a file and parsed as it arrives.
    #[arg(long)]
    pub follow: bool,
    /// Output format.
    #[arg(long, value_enum, default_value_t)]
    pub format: OutputFormat,
}

/// Build a feed `LogBlock` for one registry-entry edit. Uses the effective
/// timestamp (`max(created_at, renamed_at, deleted_at, edited_at)` over whichever
/// are set) as the sort key, and the corresponding actor DID as the author column.
///
/// `seq` is an arbitrary monotonic tiebreaker supplied by the caller (e.g. the
/// entry's position in the returned slice); it mirrors the `cmd_log` convention.
pub(crate) fn render_change_entry(
    seq: usize,
    entry: &kutl_proto::sync::RegistryEntry,
) -> crate::LogBlock {
    // Effective timestamp: the most recent of the timestamps that are set.
    let effective_ms = entry
        .deleted_at
        .into_iter()
        .chain(entry.edited_at)
        .chain(entry.renamed_at)
        .chain(std::iter::once(entry.created_at))
        .max()
        .unwrap_or(entry.created_at);

    // Last actor: the renamer if the rename is the latest event, else the creator.
    let last_actor = if entry.renamed_at.is_some_and(|ra| {
        ra >= entry.deleted_at.unwrap_or(i64::MIN)
            && ra >= entry.edited_at.unwrap_or(i64::MIN)
            && ra >= entry.created_at
    }) {
        entry.renamed_by.as_deref().unwrap_or(&entry.created_by)
    } else {
        &entry.created_by
    };

    let mut text = String::new();
    let _ = writeln!(text, "change {path}", path = entry.path);
    let _ = writeln!(text, "Author: {last_actor}");
    let _ = write!(text, "Date:   {}", crate::format_timestamp(effective_ms));

    crate::LogBlock {
        timestamp_ms: effective_ms,
        seq,
        text,
    }
}

/// Merge `document_changes` and `signals` into time-sorted [`LogBlock`]s
/// (newest first). Each signal is folded on the fly to read its state;
/// each edit is rendered via [`render_change_entry`].
fn merge_feed_blocks(
    signals: &[kutl_proto::sync::Signal],
    document_changes: &[kutl_proto::sync::RegistryEntry],
) -> Vec<crate::LogBlock> {
    let mut blocks = Vec::with_capacity(signals.len() + document_changes.len());

    // Edit blocks: seq = position in the slice.
    for (seq, entry) in document_changes.iter().enumerate() {
        blocks.push(render_change_entry(seq, entry));
    }

    // Signal blocks: fold each raw Signal to read its state, then render.
    // `seq` is set past every edit so a same-ms edit sorts above its signal.
    let edit_count = document_changes.len();
    for (i, raw) in signals.iter().enumerate() {
        let mut fold = kutl_signals::fold::SpaceSignalState::default();
        fold.apply(raw.clone());
        if let Some(state) = fold.get(&raw.id) {
            let text = render_signal_log_block(&raw.id, state);
            blocks.push(crate::LogBlock {
                timestamp_ms: raw.timestamp,
                seq: edit_count + i,
                text,
            });
        }
    }

    // Most-recent first (mirrors `cmd_log`'s sort).
    blocks.sort_by(|a, b| {
        b.timestamp_ms
            .cmp(&a.timestamp_ms)
            .then_with(|| b.seq.cmp(&a.seq))
    });
    blocks
}

/// Print the merged blocks to stdout (human format).
fn print_feed_blocks(blocks: &[crate::LogBlock]) {
    for (i, block) in blocks.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("{}", block.text);
    }
}

/// Whether an error is a genuine "relay unreachable" (connect refused / DNS /
/// timeout) rather than an auth rejection, a 5xx, a malformed URL, or a decode
/// failure. Walks the anyhow source chain for a [`reqwest::Error`] and uses its
/// typed predicates — never message-substring sniffing (that is version-,
/// locale-, and platform-dependent, and would silently hide real failures).
fn is_relay_unreachable(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|re| re.is_connect() || re.is_timeout())
    })
}

/// Print the offline note to stderr, then render the local signals-only feed.
/// The single degradation path shared by the token-resolution and get-changes
/// unreachable cases.
///
/// `--follow` does not degrade. A follow is a STREAM: the human form promises
/// to keep printing until interrupted, and the JSON form promises NDJSON — one
/// record per line, which is what lets a consumer parse what has landed so far.
/// The local render is neither. It prints one snapshot in a different shape
/// (`signal list`'s array of views, not the feed's tagged records) and returns,
/// so a consumer tailing the stream meets a parse error where it expected a
/// gap, and a human watching gets a silent exit that reads as "nothing more
/// happened". A follow that cannot follow says so.
///
/// # Errors
///
/// Returns an error when the caller asked to follow, or if the local segments
/// cannot be read or serialized.
fn feed_offline_fallback(space: &FoldedSpace, args: &FeedArgs) -> Result<()> {
    if args.follow {
        anyhow::bail!(
            "relay unreachable, so there is nothing to follow — drop --follow to see this \
             space's local signals"
        );
    }
    eprintln!(
        "note: relay unreachable — showing local signals only (edits need a connected relay)"
    );
    render_local_signals_only(space, args.format)
}

/// Handle `kutl space feed`.
///
/// Shows the activity feed of the space enclosing the current directory,
/// fetched from the relay (edits + signals interleaved newest first). A
/// one-shot run falls back to local signals-only when the relay is not
/// reachable. `--format json` prints the raw feed page; `--follow` polls for
/// new activity until interrupted, and with `--format json` streams NDJSON.
///
/// # Errors
///
/// Returns an error if the current directory is not inside a space, the
/// segments cannot be read, or the relay rejects the request (auth failure,
/// 5xx, malformed URL, decode error). A genuinely unreachable relay (connect
/// refused / DNS / timeout) degrades to the local signals-only fallback for a
/// one-shot run, and errors under `--follow` — a stream that cannot stream is
/// not a shorter stream (see [`feed_offline_fallback`]).
pub async fn cmd_space_feed(args: FeedArgs) -> Result<()> {
    let space = folded_space_at(&crate::require_cwd_space()?)?;
    let config = kutl_client::SpaceConfig::load(&space.space_root)
        .with_context(|| format!("loading the space config at {}", space.space_root.display()))?;

    // Resolve the relay token. An unreachable relay degrades to local
    // signals-only; any other failure (auth rejection, 5xx, bad URL) surfaces.
    let token = match kutl_client::resolve_or_authenticate(&config.relay_url).await {
        Ok(t) => t,
        Err(e) if is_relay_unreachable(&e) => {
            return feed_offline_fallback(&space, &args);
        }
        Err(e) => return Err(e.context("resolving the relay token for the feed")),
    };

    let client = kutl_client::SignalCatchUpClient::from_ws_url(&config.relay_url);

    // First page. Same rule: only a truly unreachable relay falls back.
    let page = match client.get_changes(&config.space_id, &token, None).await {
        Ok(p) => p,
        Err(e) if is_relay_unreachable(&e) => {
            return feed_offline_fallback(&space, &args);
        }
        Err(e) => return Err(e.context("fetching the space feed")),
    };

    match args.format {
        OutputFormat::Json if args.follow => print!("{}", feed_ndjson_page(&page)?),
        OutputFormat::Json => {
            let payload = serde_json::json!({
                "signals": page.signals,
                "document_changes": page.document_changes,
                "checkpoint": page.checkpoint,
            });
            println!("{}", serde_json::to_string_pretty(&payload)?);
            return Ok(());
        }
        OutputFormat::Human => {
            let blocks = merge_feed_blocks(&page.signals, &page.document_changes);
            if blocks.is_empty() && !args.follow {
                println!("no activity");
                return Ok(());
            }
            print_feed_blocks(&blocks);
        }
    }

    if !args.follow {
        return Ok(());
    }

    // `--follow`: poll for new pages until interrupted (Ctrl-C → process exits).
    let mut checkpoint = page.checkpoint;
    loop {
        std::thread::sleep(FEED_FOLLOW_POLL);
        let next = match client
            .get_changes(&config.space_id, &token, Some(&checkpoint))
            .await
        {
            Ok(p) => p,
            Err(e) => {
                // A transient error during follow doesn't kill the loop — warn
                // and retry next interval.
                eprintln!("warning: feed poll failed: {e}");
                continue;
            }
        };
        match args.format {
            OutputFormat::Json => print!("{}", feed_ndjson_page(&next)?),
            OutputFormat::Human => {
                let new_blocks = merge_feed_blocks(&next.signals, &next.document_changes);
                if !new_blocks.is_empty() {
                    println!();
                    print_feed_blocks(&new_blocks);
                }
            }
        }
        if !next.checkpoint.is_empty() {
            checkpoint = next.checkpoint;
        }
    }
}

/// One line of the `--follow --format json` stream: a feed record, tagged with
/// which half of the feed it came from.
///
/// The nesting under `record` is what keeps the tag from colliding with a field
/// of the record itself, and the records are byte-identical to the ones the
/// one-shot form nests under `signals` and `document_changes` — one vocabulary,
/// two framings.
#[derive(Serialize)]
struct FeedLine<'a, T: Serialize> {
    /// `signal` or `document_change`.
    r#type: &'static str,
    /// The record, in the same shape the one-shot form emits.
    record: &'a T,
}

/// Render one feed page as NDJSON — one record per line, each line a complete
/// JSON object, ending in a newline when non-empty.
///
/// `--follow` is a stream: a reader appending it to a file has to parse what
/// has landed so far, which the one-shot form's single enclosing object makes
/// impossible. A page with nothing in it renders nothing, so a consumer never
/// filters empty envelopes out to find activity.
///
/// A page is a batch and not a total order: signals come before the same page's
/// document changes, and the relay's order within each holds.
fn feed_ndjson_page(page: &kutl_client::FeedPage) -> Result<String> {
    let mut out = String::new();
    for signal in &page.signals {
        let line = serde_json::to_string(&FeedLine {
            r#type: "signal",
            record: signal,
        })
        .context("rendering a feed signal as NDJSON")?;
        let _ = writeln!(out, "{line}");
    }
    for change in &page.document_changes {
        let line = serde_json::to_string(&FeedLine {
            r#type: "document_change",
            record: change,
        })
        .context("rendering a feed document change as NDJSON")?;
        let _ = writeln!(out, "{line}");
    }
    Ok(out)
}

/// Render signals-only from the local fold when the relay is not reachable.
/// Prints a flat list (all statuses) as human text or JSON. This is the offline
/// degradation path — callers have already printed the one-line stderr note.
fn render_local_signals_only(space: &FoldedSpace, format: OutputFormat) -> Result<()> {
    let filters = ListFilters {
        status: StatusFilter::All,
        kind: None,
        doc: None,
        flag_kind: None,
    };
    let views = views_for_space(&space.fold, &space.space_id, &space.space_name, &filters);
    match format {
        OutputFormat::Json => {
            // Match `signal list --format json` shape.
            println!("{}", serde_json::to_string_pretty(&views)?);
        }
        OutputFormat::Human => {
            if views.is_empty() {
                println!("no signals");
            } else {
                print!("{}", render_list_human(&views));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use kutl_proto::sync::SignalEventType;

    use super::*;
    use kutl_proto::sync::{ChatPayload, FlagPayload, Hlc};

    /// Every kind's `--help` line is the shared vocabulary's sentence, not a
    /// restatement of it. Fails the moment a variant stops sharing.
    #[test]
    fn test_kind_help_is_the_shared_guidance() {
        use clap::ValueEnum as _;

        for arg in FlagKindArg::value_variants() {
            let help = arg
                .to_possible_value()
                .expect("no kind is skipped")
                .get_help()
                .map(ToString::to_string)
                .expect("every kind renders help");
            assert_eq!(
                help,
                kutl_proto::vocab::flag_kind_guidance(arg.to_proto()),
                "{arg:?} must render the shared guidance"
            );
        }
    }

    /// The CLI offers exactly the shared vocabulary, in its order — the kinds
    /// that ask something of a reader first. clap presents variants in
    /// declaration order, so nothing but this keeps the two lists in step.
    #[test]
    fn test_kind_order_follows_the_shared_vocabulary() {
        use clap::ValueEnum as _;

        let offered: Vec<ProtoKind> = FlagKindArg::value_variants()
            .iter()
            .map(|arg| arg.to_proto())
            .collect();
        assert_eq!(
            offered,
            kutl_proto::vocab::FLAG_KINDS,
            "--kind must offer the shared vocabulary in its order"
        );
    }

    /// Build a CREATED record with a flag payload attached to `doc`.
    fn created_flag(id: &str, rec: &str, doc: Option<&str>, ms: i64) -> Signal {
        let mut s = Signal {
            id: id.into(),
            space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
            document_id: doc.map(str::to_owned),
            timestamp: ms,
            record_id: rec.into(),
            payload: Some(signal::Payload::Flag(FlagPayload::default())),
            hlc: Some(Hlc {
                physical_ms: u64::try_from(ms).unwrap_or(0),
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// Build a CREATED record with a chat payload.
    fn created_chat(id: &str, rec: &str, doc: Option<&str>, ms: i64) -> Signal {
        let mut s = created_flag(id, rec, doc, ms);
        s.payload = Some(signal::Payload::Chat(ChatPayload::default()));
        s
    }

    /// Build a transition record (CLOSED/REOPENED/TOMBSTONED) for `id`.
    fn transition(id: &str, rec: &str, event: SignalEventType, ms: i64) -> Signal {
        let mut s = Signal {
            id: id.into(),
            space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
            timestamp: ms,
            record_id: rec.into(),
            hlc: Some(Hlc {
                physical_ms: u64::try_from(ms).unwrap_or(0),
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        s.set_event(event);
        s
    }

    /// A fold with three signals: `open-flag` (open, doc=a.md, flag),
    /// `closed-chat` (closed, doc=b.md, chat), `dead` (tombstoned, doc=a.md).
    fn sample_fold() -> SpaceSignalState {
        let mut fold = SpaceSignalState::default();
        fold.apply(created_flag("open-flag", "r1", Some("a.md"), 10));
        fold.apply(created_chat("closed-chat", "r2", Some("b.md"), 20));
        fold.apply(transition("closed-chat", "r3", SignalEventType::Closed, 25));
        fold.apply(created_flag("dead", "r4", Some("a.md"), 30));
        fold.apply(transition("dead", "r5", SignalEventType::Tombstoned, 35));
        fold
    }

    #[test]
    fn test_find_signal_by_prefix_exact_unique_missing_short() {
        let fold = sample_fold();
        // Exact id resolves as-is.
        assert_eq!(
            find_signal_by_prefix(&fold, "open-flag").unwrap(),
            Some("open-flag".to_owned())
        );
        // A unique >=4-char prefix resolves to the full id.
        assert_eq!(
            find_signal_by_prefix(&fold, "open").unwrap(),
            Some("open-flag".to_owned())
        );
        // No match is Ok(None) — the caller owns the not-found wording.
        assert_eq!(find_signal_by_prefix(&fold, "zzzz").unwrap(), None);
        // A too-short non-exact input errors with the minimum length.
        let err = find_signal_by_prefix(&fold, "op").unwrap_err();
        assert!(
            err.to_string().contains("at least 4"),
            "should name the minimum: {err}"
        );
    }

    #[test]
    fn test_find_signal_by_prefix_ambiguous_lists_candidates() {
        let mut fold = SpaceSignalState::default();
        fold.apply(created_flag("abcd-one", "r1", None, 10));
        fold.apply(created_flag("abcd-two", "r2", None, 20));
        let err = find_signal_by_prefix(&fold, "abcd").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("abcd-one") && msg.contains("abcd-two"),
            "ambiguity lists every candidate: {msg}"
        );
    }

    /// A daemon-held segment flock degrades `--fetch` to a graceful skip:
    /// `fetch_one_space` returns Ok WITHOUT touching the relay. The bogus
    /// relay URL is the discriminator — reaching for it would fail loudly,
    /// so Ok proves the skip happened before any network.
    #[tokio::test]
    async fn test_fetch_one_space_skips_gracefully_when_daemon_holds_lock() {
        let space_dir = tempfile::TempDir::new().unwrap();
        let space_id = "be18b85f-77fc-424d-8379-acf19e8a1ce6";
        let uuid = uuid::Uuid::parse_str(space_id).unwrap();
        // Hold the single-writer flock like a running daemon would.
        let _daemon_store =
            kutl_daemon::signal_store::DaemonSignalStore::open(space_dir.path(), uuid)
                .expect("take the writer lock");

        fetch_one_space(space_dir.path(), space_id, "ws://127.0.0.1:1/ws", "tok")
            .await
            .expect("held lock must skip the fetch, not error");
    }

    /// A lock failure that is NOT contention (here: the signals dir path is
    /// occupied by a FILE, so the store cannot create it) still errors —
    /// the graceful skip is reserved for a live daemon.
    #[tokio::test]
    async fn test_fetch_one_space_still_errors_on_non_contention_failures() {
        let space_dir = tempfile::TempDir::new().unwrap();
        let kutl_dir = space_dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        std::fs::write(kutl_dir.join("signals"), "not a directory").unwrap();

        let err = fetch_one_space(
            space_dir.path(),
            "be18b85f-77fc-424d-8379-acf19e8a1ce6",
            "ws://127.0.0.1:1/ws",
            "tok",
        )
        .await
        .expect_err("a non-contention store failure must surface");
        assert!(
            format!("{err:#}").contains("cannot open the signal store"),
            "got: {err:#}"
        );
    }

    #[test]
    fn test_find_signal_by_prefix_exact_wins_over_ambiguity() {
        // Git-style: an input that IS a full id resolves to it even when it
        // also prefixes other ids.
        let mut fold = SpaceSignalState::default();
        fold.apply(created_flag("abcd", "r1", None, 10));
        fold.apply(created_flag("abcd-two", "r2", None, 20));
        assert_eq!(
            find_signal_by_prefix(&fold, "abcd").unwrap(),
            Some("abcd".to_owned())
        );
    }

    fn collect(fold: &SpaceSignalState, filters: &ListFilters<'_>) -> Vec<SignalView> {
        views_for_space(fold, "sp", "my-space", filters)
    }

    /// A CLOSED transition for `id` carrying `reason`.
    fn closed_because(
        id: &str,
        rec: &str,
        reason: kutl_proto::sync::CloseReason,
        ms: i64,
    ) -> Signal {
        let mut s = transition(id, rec, SignalEventType::Closed, ms);
        s.set_close_reason(reason);
        s
    }

    /// A withdrawn signal and a resolved one are both `closed`, so status alone
    /// cannot tell a decision the space settled from one whose heading was
    /// deleted out from under it. A counter that treats "no signal is open" as
    /// agreement scores the second as the first.
    #[test]
    fn test_view_of_separates_a_withdrawn_close_from_a_resolved_one() {
        use kutl_proto::sync::CloseReason;

        let mut fold = SpaceSignalState::default();
        fold.apply(created_flag("settled", "r1", None, 10));
        fold.apply(closed_because("settled", "r2", CloseReason::Resolved, 15));
        fold.apply(created_flag("pulled", "r3", None, 20));
        fold.apply(closed_because("pulled", "r4", CloseReason::Withdrawn, 25));
        fold.apply(created_flag("live", "r5", None, 30));

        let views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let of = |id: &str| {
            views
                .iter()
                .find(|v| v.id == id)
                .unwrap_or_else(|| panic!("{id} is in the fold"))
        };

        assert_eq!(of("settled").status, "closed");
        assert_eq!(of("pulled").status, "closed");
        assert_eq!(of("settled").close_reason, Some("resolved"));
        assert_eq!(of("pulled").close_reason, Some("withdrawn"));
        assert_eq!(
            of("live").close_reason,
            None,
            "an open signal has no close reason to report"
        );
    }

    /// An ending this build has no name for is reported as no ending, never as
    /// `resolved`.
    ///
    /// The reason exists to keep a settled decision apart from one withdrawn
    /// out from under the space, and `resolved` is the label on the agreement
    /// side — so guessing it for a discriminant a newer peer authored puts a
    /// close this build cannot read on exactly the side the field was added to
    /// protect. `None` is what the human renderer already omits and what a
    /// JSON consumer already handles, because an open signal reports the same.
    #[test]
    fn test_view_of_does_not_name_an_ending_it_cannot_read() {
        let mut fold = SpaceSignalState::default();
        fold.apply(created_flag("future", "r1", None, 10));
        let mut close = transition("future", "r2", SignalEventType::Closed, 15);
        // Set past the enum: what a peer on a newer build writes, replicated
        // into this one's segments.
        close.close_reason = 99;
        fold.apply(close);

        let views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let view = views
            .iter()
            .find(|v| v.id == "future")
            .expect("in the fold");
        assert_eq!(view.status, "closed", "the signal is closed either way");
        assert_eq!(
            view.close_reason, None,
            "an unrecognized ending must not be reported as agreement"
        );
    }

    /// A space with nothing in it, for the paths that decide before reading.
    fn empty_space() -> FoldedSpace {
        FoldedSpace {
            space_id: "sp".to_owned(),
            space_name: "my-space".to_owned(),
            space_root: PathBuf::from("/nonexistent"),
            fold: SpaceSignalState::default(),
        }
    }

    /// A one-shot feed degrades to the local signals when the relay is not
    /// reachable, and says so on stderr.
    #[test]
    fn test_feed_offline_fallback_renders_locally_for_a_one_shot() {
        let args = FeedArgs {
            follow: false,
            format: OutputFormat::Json,
        };
        assert!(feed_offline_fallback(&empty_space(), &args).is_ok());
    }

    /// `--follow` promises a stream. The local render is a single snapshot in
    /// another shape — `signal list`'s array of views rather than the feed's
    /// one-record-per-line — so degrading to it hands a JSON consumer a parse
    /// error where it expected a gap, and hands a human a silent exit that
    /// reads as the feed having gone quiet. Both formats refuse instead.
    #[test]
    fn test_feed_offline_fallback_refuses_to_follow_an_unreachable_relay() {
        for format in [OutputFormat::Json, OutputFormat::Human] {
            let args = FeedArgs {
                follow: true,
                format,
            };
            let err = feed_offline_fallback(&empty_space(), &args)
                .expect_err("a follow with no relay must fail loudly");
            assert!(
                format!("{err:#}").contains("--follow"),
                "the error must name the flag it is refusing, got: {err:#}"
            );
        }
    }

    /// The JSON a programmatic caller reads carries the reason, not just the
    /// status — otherwise the distinction exists only inside the process.
    #[test]
    fn test_view_of_close_reason_is_in_the_json() {
        use kutl_proto::sync::CloseReason;

        let mut fold = SpaceSignalState::default();
        fold.apply(created_flag("pulled", "r1", None, 10));
        fold.apply(closed_because("pulled", "r2", CloseReason::Withdrawn, 15));

        let views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let json = serde_json::to_value(&views).expect("views serialize");
        assert_eq!(json[0]["close_reason"], "withdrawn");
    }

    #[test]
    fn test_default_shows_open_only_never_tombstoned() {
        let fold = sample_fold();
        let views = collect(
            &fold,
            &ListFilters {
                status: status_filter_from_flags(false, false, false),
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let ids: Vec<&str> = views.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, vec!["open-flag"], "default lists open signals only");
        assert!(
            !ids.contains(&"dead"),
            "tombstoned signal must never appear"
        );
    }

    #[test]
    fn test_closed_filter_shows_closed_not_tombstoned() {
        let fold = sample_fold();
        let views = collect(
            &fold,
            &ListFilters {
                status: status_filter_from_flags(false, true, false),
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let ids: Vec<&str> = views.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, vec!["closed-chat"]);
        assert!(!ids.contains(&"dead"), "tombstoned must never appear");
    }

    #[test]
    fn test_all_filter_shows_open_and_closed_never_tombstoned() {
        let fold = sample_fold();
        let views = collect(
            &fold,
            &ListFilters {
                status: status_filter_from_flags(false, false, true),
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let mut ids: Vec<&str> = views.iter().map(|v| v.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["closed-chat", "open-flag"]);
        assert!(
            !ids.contains(&"dead"),
            "tombstoned must never appear even under --all"
        );
    }

    #[test]
    fn test_kind_filter_matches_payload() {
        let fold = sample_fold();
        // --all + --kind chat → only the chat signal.
        let views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: Some(SignalKind::Chat),
                doc: None,
                flag_kind: None,
            },
        );
        let ids: Vec<&str> = views.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, vec!["closed-chat"]);
    }

    #[test]
    fn test_doc_filter_matches_created_document_id() {
        let fold = sample_fold();
        // --all + --doc b.md → only the b.md signal (a.md's dead one is hidden).
        let views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: Some("b.md"),
                flag_kind: None,
            },
        );
        let ids: Vec<&str> = views.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, vec!["closed-chat"]);
    }

    #[test]
    fn test_flag_kind_filter_admits_matching_intent_only() {
        use kutl_proto::sync::FlagPayload;

        // Build a fold with a `question` flag and a `blocked` flag (both open).
        let question_kind = kutl_proto::vocab::flag_kind_from_str("question")
            .expect("question is a known flag-kind");
        let blocked_kind =
            kutl_proto::vocab::flag_kind_from_str("blocked").expect("blocked is a known flag-kind");

        let mut question_signal = created_flag("sig-question", "r10", None, 100);
        question_signal.payload = Some(signal::Payload::Flag(FlagPayload {
            kind: question_kind,
            ..Default::default()
        }));

        let mut blocked_signal = created_flag("sig-blocked", "r11", None, 110);
        blocked_signal.payload = Some(signal::Payload::Flag(FlagPayload {
            kind: blocked_kind,
            ..Default::default()
        }));

        let mut fold = SpaceSignalState::default();
        fold.apply(question_signal);
        fold.apply(blocked_signal);

        // --flag-kind question admits only the question flag.
        let question_views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: None,
                flag_kind: Some(question_kind),
            },
        );
        let ids: Vec<&str> = question_views.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["sig-question"],
            "--flag-kind question must admit only the question flag"
        );

        // --flag-kind blocked admits only the blocked flag.
        let blocked_views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: None,
                flag_kind: Some(blocked_kind),
            },
        );
        let ids: Vec<&str> = blocked_views.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["sig-blocked"],
            "--flag-kind blocked must admit only the blocked flag"
        );
    }

    #[test]
    fn test_json_output_is_structured_not_debug() {
        let fold = sample_fold();
        let views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let json = serde_json::to_string(&views).expect("views serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        let arr = parsed.as_array().expect("json array");
        assert_eq!(arr.len(), 2);
        // Every entry carries the structured fields (not a proto Debug blob).
        for entry in arr {
            assert!(entry.get("id").is_some(), "id field: {entry}");
            assert!(entry.get("status").is_some(), "status field: {entry}");
            assert!(entry.get("kind").is_some(), "kind field: {entry}");
        }
        // Spot-check one signal's kind + status round-trip through JSON.
        let closed = arr
            .iter()
            .find(|e| e["id"] == "closed-chat")
            .expect("closed-chat present");
        assert_eq!(closed["status"], "closed");
        assert_eq!(closed["kind"], "chat");
    }

    #[test]
    fn test_human_render_groups_and_hides_tombstoned() {
        let fold = sample_fold();
        let views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let out = render_list_human(&views);
        assert!(out.contains("space my-space (sp)"), "space header:\n{out}");
        assert!(out.contains("a.md"), "doc group:\n{out}");
        assert!(out.contains("open-flag"), "open signal listed:\n{out}");
        assert!(out.contains("closed-chat"), "closed signal listed:\n{out}");
        assert!(!out.contains("dead"), "tombstoned hidden:\n{out}");
    }

    #[test]
    fn test_transition_history_orders_oldest_first_and_shows_tombstone_event() {
        // The AUDIT trail (view) may show a tombstoned EVENT even though the
        // projection hides the tombstoned signal.
        let store = SegmentStore {
            records: vec![
                transition("dead", "r5", SignalEventType::Tombstoned, 35),
                created_flag("dead", "r4", Some("a.md"), 30),
            ],
            quarantined: vec![],
        };
        let history = transition_history("dead", &store);
        let events: Vec<&str> = history.iter().map(|t| t.event.as_str()).collect();
        assert_eq!(
            events,
            vec!["created", "tombstoned"],
            "oldest-first audit trail includes the tombstone event"
        );
    }

    #[test]
    fn test_status_filter_from_flags_all_wins() {
        assert_eq!(
            status_filter_from_flags(true, false, false),
            StatusFilter::Open
        );
        assert_eq!(
            status_filter_from_flags(false, true, false),
            StatusFilter::Closed
        );
        assert_eq!(
            status_filter_from_flags(false, false, true),
            StatusFilter::All
        );
        assert_eq!(
            status_filter_from_flags(true, true, false),
            StatusFilter::All,
            "open+closed together == all"
        );
    }

    /// A `close` builds a CLOSED transition with the mapped reason.
    #[test]
    fn test_build_transition_request_close_carries_reason() {
        let req = build_transition_request(TransitionVerb::Close, Some(CloseReasonArg::Declined));
        assert_eq!(
            req.event,
            kutl_client::signal_catchup::TransitionEvent::Closed
        );
        assert_eq!(req.reason.as_deref(), Some("declined"));
    }

    /// A `close` with no `--reason` defaults to `resolved`.
    #[test]
    fn test_build_transition_request_close_defaults_to_resolved() {
        let req = build_transition_request(TransitionVerb::Close, None);
        assert_eq!(
            req.event,
            kutl_client::signal_catchup::TransitionEvent::Closed
        );
        assert_eq!(req.reason.as_deref(), Some("resolved"));
    }

    /// `resolve` is sugar for `close --reason resolved` — it forces the reason
    /// to resolved regardless of any caller value.
    #[test]
    fn test_build_transition_request_resolve_forces_resolved() {
        let req = build_transition_request(TransitionVerb::Resolve, None);
        assert_eq!(
            req.event,
            kutl_client::signal_catchup::TransitionEvent::Closed
        );
        assert_eq!(req.reason.as_deref(), Some("resolved"));
    }

    /// A `reopen` builds a REOPENED transition with no reason.
    #[test]
    fn test_build_transition_request_reopen_has_no_reason() {
        let req = build_transition_request(TransitionVerb::Reopen, None);
        assert_eq!(
            req.event,
            kutl_client::signal_catchup::TransitionEvent::Reopened
        );
        assert_eq!(req.reason, None);
    }

    /// `--format json` says who each signal is for. The values come off the
    /// shared summary, so the CLI and MCP `list_signals` cannot disagree about
    /// a signal's audience.
    #[test]
    fn test_view_carries_the_audience_and_target() {
        let mut addressed = created_flag("sig-addressed", "rec-a", None, 1);
        if let Some(signal::Payload::Flag(f)) = &mut addressed.payload {
            f.audience = Some(kutl_proto::vocab::participant_audience("did:key:zBob"));
        }
        let mut broadcast = created_flag("sig-broadcast", "rec-b", None, 2);
        if let Some(signal::Payload::Flag(f)) = &mut broadcast.payload {
            f.audience = Some(kutl_proto::vocab::space_audience());
        }

        let mut fold = SpaceSignalState::default();
        fold.apply(addressed);
        fold.apply(broadcast);

        let view = |id: &str| view_of(id, fold.get(id).expect("the signal folds"), "sp", "space");

        let addressed = view("sig-addressed");
        assert_eq!(addressed.audience, Some("participant"));
        assert_eq!(addressed.target_did.as_deref(), Some("did:key:zBob"));

        let broadcast = view("sig-broadcast");
        assert_eq!(broadcast.audience, Some("space"));
        assert_eq!(broadcast.target_did, None);
    }

    /// Every row says who raised it, including a space broadcast — which
    /// names nobody as a recipient and would otherwise be anonymous.
    ///
    /// Author and recipient must stay distinguishable in the text. A reader
    /// needs to tell "asking me" from "asked by me", and the demo rig reads
    /// receipt out of the `by <did>` attribution while deliberately refusing
    /// to count the `→` target — collapsing the two spellings would score an
    /// agent that only ever talked AT its peers as having listened.
    #[test]
    fn test_list_human_says_who_raised_each_signal() {
        let mut addressed = created_flag("sig-addressed", "rec-a", None, 1);
        addressed.author_did = "did:key:zAlice".into();
        if let Some(signal::Payload::Flag(f)) = &mut addressed.payload {
            f.audience = Some(kutl_proto::vocab::participant_audience("did:key:zBob"));
        }
        let mut broadcast = created_flag("sig-broadcast", "rec-b", None, 2);
        broadcast.author_did = "did:key:zAlice".into();
        if let Some(signal::Payload::Flag(f)) = &mut broadcast.payload {
            f.audience = Some(kutl_proto::vocab::space_audience());
        }
        let mut fold = SpaceSignalState::default();
        fold.apply(addressed);
        fold.apply(broadcast);
        let views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let out = render_list_human(&views);

        for id in ["sig-addressed", "sig-broadcast"] {
            let row = out.lines().find(|l| l.contains(id)).expect("row renders");
            assert!(
                row.contains("by did:key:zAlice"),
                "{id} must name its author: {row}"
            );
        }

        // The addressed row carries both, and they must not read as one fact.
        let addressed_row = out
            .lines()
            .find(|l| l.contains("sig-addressed"))
            .expect("addressed row renders");
        assert!(
            addressed_row.contains("by did:key:zAlice") && addressed_row.contains("→ did:key:zBob"),
            "author and recipient must both appear, distinctly: {addressed_row}"
        );
        // The broadcast names an author but no recipient.
        let broadcast_row = out
            .lines()
            .find(|l| l.contains("sig-broadcast"))
            .expect("broadcast row renders");
        assert!(
            !broadcast_row.contains('→'),
            "a broadcast names no recipient: {broadcast_row}"
        );
    }

    /// A list row addressed to a participant names its recipient; a space
    /// broadcast carries no marker. The two must be distinguishable at a
    /// glance — a reader scanning for signals naming them should not have to
    /// open each one.
    #[test]
    fn test_list_human_shows_who_an_addressed_flag_is_for() {
        let mut addressed = created_flag("sig-addressed", "rec-a", None, 1);
        if let Some(signal::Payload::Flag(f)) = &mut addressed.payload {
            f.audience = Some(kutl_proto::vocab::participant_audience("did:key:zBob"));
        }
        let mut broadcast = created_flag("sig-broadcast", "rec-b", None, 2);
        if let Some(signal::Payload::Flag(f)) = &mut broadcast.payload {
            f.audience = Some(kutl_proto::vocab::space_audience());
        }
        let mut fold = SpaceSignalState::default();
        fold.apply(addressed);
        fold.apply(broadcast);
        let views = collect(
            &fold,
            &ListFilters {
                status: StatusFilter::All,
                kind: None,
                doc: None,
                flag_kind: None,
            },
        );
        let out = render_list_human(&views);
        let addressed_row = out
            .lines()
            .find(|l| l.contains("sig-addressed"))
            .expect("addressed row renders");
        let broadcast_row = out
            .lines()
            .find(|l| l.contains("sig-broadcast"))
            .expect("broadcast row renders");
        assert!(
            addressed_row.contains("→ did:key:zBob"),
            "recipient visible on the row: {addressed_row}"
        );
        assert!(
            !broadcast_row.contains('→'),
            "a broadcast row carries no addressing marker: {broadcast_row}"
        );
    }

    /// The view detail answers "who is this for" directly: the recipient DID
    /// when one is named, the audience label otherwise.
    #[test]
    fn test_view_human_shows_who_the_signal_is_for() {
        let mut addressed = created_flag("sig-addressed", "rec-a", None, 1);
        if let Some(signal::Payload::Flag(f)) = &mut addressed.payload {
            f.audience = Some(kutl_proto::vocab::participant_audience("did:key:zBob"));
        }
        let mut broadcast = created_flag("sig-broadcast", "rec-b", None, 2);
        if let Some(signal::Payload::Flag(f)) = &mut broadcast.payload {
            f.audience = Some(kutl_proto::vocab::space_audience());
        }
        let mut fold = SpaceSignalState::default();
        fold.apply(addressed);
        fold.apply(broadcast);
        let view = |id: &str| view_of(id, fold.get(id).expect("the signal folds"), "sp", "space");

        let out = render_view_human(&view("sig-addressed"), &[]);
        assert!(
            out.contains("for:      did:key:zBob"),
            "addressed detail names the recipient:\n{out}"
        );
        let out = render_view_human(&view("sig-broadcast"), &[]);
        assert!(
            out.contains("for:      space"),
            "broadcast detail says the whole space:\n{out}"
        );
    }

    /// The `CloseReasonArg` maps to the client's lowercase reason strings.
    #[test]
    fn test_close_reason_arg_wire_labels() {
        assert_eq!(CloseReasonArg::Resolved.wire_label(), "resolved");
        assert_eq!(CloseReasonArg::Declined.wire_label(), "declined");
        assert_eq!(CloseReasonArg::Withdrawn.wire_label(), "withdrawn");
    }

    /// Regression (the 1970 created-at bug): a CLOSED transition
    /// carrying a real wall-clock `timestamp` (== its HLC `physical_ms`), folded
    /// with a CREATED record, reads back that real time as the CLI view's
    /// `closed_ms`. The relay-mint transition path stamps the record's
    /// timestamp from the relay wall clock via the SAME `assemble_record` this
    /// exercises, so a 0 here is the 1970 display bug the readers render.
    #[test]
    fn test_transition_timestamp_visible_in_view() {
        let now_ms: u64 = 1_752_400_000_000;
        // The real transition builder — a CLOSED transition with no note, which is
        // exactly what the relay mints today.
        let intent = kutl_signals::authoring::SignalIntent::transition(
            kutl_signals::authoring::RecordEnvelope {
                space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
                document_id: None,
                signal_id: "sig-1".into(),
                timestamp: kutl_core::ms_u64_to_i64_saturating(now_ms),
            },
            SignalEventType::Closed,
            Some(kutl_proto::sync::CloseReason::Resolved),
            None,
        )
        .expect("a well-formed transition intent");
        let hlc = Hlc {
            physical_ms: now_ms,
            logical: 0,
            actor: vec![0u8; 16],
        };
        let closed = kutl_signals::authoring::assemble_record(&intent, "did:key:zActor", hlc);
        assert_eq!(
            closed.timestamp,
            kutl_core::ms_u64_to_i64_saturating(closed.hlc.as_ref().unwrap().physical_ms),
            "record.timestamp must equal hlc.physical_ms (one clock read)"
        );
        assert_ne!(
            closed.timestamp, 0,
            "a real transition timestamp is non-zero"
        );

        // Fold a CREATED (at a different, earlier time) + the CLOSED transition,
        // then read the CLI view: closed_ms is the transition's real wall time.
        let created_ms: i64 = 1_600_000_000_000;
        let mut fold = SpaceSignalState::default();
        fold.apply(created_flag(
            "sig-1",
            "rec-created",
            Some("doc-1"),
            created_ms,
        ));
        fold.apply(closed);

        let state = fold.get("sig-1").expect("the signal folds");
        let view = view_of("sig-1", state, "sp", "my-space");
        assert_eq!(view.status, "closed");
        assert_eq!(
            view.closed_ms,
            Some(kutl_core::ms_u64_to_i64_saturating(now_ms)),
            "the view's closed_ms is the transition's real wall time"
        );
        assert_eq!(
            view.created_ms, created_ms,
            "the view's created_ms is the CREATED record's wall time"
        );
    }

    /// `normalize_space_rel_path` strips leading `./` and joins components with
    /// `/` so `./notes/plan.md` and `notes/plan.md` both yield `"notes/plan.md"`,
    /// and an absolute path inside the space is also accepted.
    #[test]
    fn test_normalize_space_rel_path_strips_dot_slash_and_joins() {
        let root = PathBuf::from("/space/root");

        let plain = normalize_space_rel_path(&root, "notes/plan.md").unwrap();
        assert_eq!(plain, "notes/plan.md");

        let dotted = normalize_space_rel_path(&root, "./notes/plan.md").unwrap();
        assert_eq!(dotted, "notes/plan.md", "./notes form must normalize");

        let absolute = normalize_space_rel_path(&root, "/space/root/notes/plan.md").unwrap();
        assert_eq!(
            absolute, "notes/plan.md",
            "absolute path inside space works"
        );
    }

    /// `view_of` populates `flag_kind` (the intent kind) and `message` from the
    /// CREATED record's payload. A flag exposes both; a reply exposes `message`
    /// only (its body); chat/decision/None expose neither.
    #[test]
    fn test_view_of_populates_flag_kind_and_message() {
        use kutl_proto::sync::{FlagPayload, ReplyPayload};

        // A question flag with a message.
        let question_kind_i32 = kutl_proto::vocab::flag_kind_from_str("question").unwrap();
        let mut flag_signal = Signal {
            id: "sig-flag".into(),
            space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
            timestamp: 1_700_000_000_000,
            record_id: "r1".into(),
            payload: Some(signal::Payload::Flag(FlagPayload {
                kind: question_kind_i32,
                message: "can someone review the deploy?".into(),
                ..Default::default()
            })),
            hlc: Some(Hlc {
                physical_ms: 1_700_000_000_000,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        flag_signal.set_event(SignalEventType::Created);

        let mut fold = SpaceSignalState::default();
        fold.apply(flag_signal);
        let state = fold.get("sig-flag").unwrap();
        let view = view_of("sig-flag", state, "sp", "my-space");

        assert_eq!(
            view.flag_kind,
            Some("question"),
            "flag_kind from FlagPayload"
        );
        assert_eq!(
            view.message.as_deref(),
            Some("can someone review the deploy?"),
            "message from FlagPayload"
        );

        // A reply exposes message (body) but no flag_kind.
        let mut reply_signal = Signal {
            id: "sig-reply".into(),
            space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
            timestamp: 1_700_000_000_001,
            record_id: "r2".into(),
            payload: Some(signal::Payload::Reply(ReplyPayload {
                parent_signal_id: "sig-flag".into(),
                body: "looks good to me".into(),
                ..Default::default()
            })),
            hlc: Some(Hlc {
                physical_ms: 1_700_000_000_001,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        reply_signal.set_event(SignalEventType::Created);

        let mut fold2 = SpaceSignalState::default();
        fold2.apply(reply_signal);
        let state2 = fold2.get("sig-reply").unwrap();
        let view2 = view_of("sig-reply", state2, "sp", "my-space");

        assert_eq!(view2.flag_kind, None, "reply has no flag_kind");
        assert_eq!(
            view2.message.as_deref(),
            Some("looks good to me"),
            "reply body surfaces as message"
        );
    }

    /// `message_preview` clips by CHARACTERS, not bytes: a message whose
    /// byte-60 boundary lands mid-multibyte-char must not panic. `"é"` is two
    /// bytes, so 40 of them is 80 bytes / 40 chars — byte-slicing at 60 would
    /// split a codepoint. The result is char-truncated (≤ len + the `…` mark).
    #[test]
    fn test_message_preview_clips_by_char_not_byte() {
        let msg = "é".repeat(40); // 80 bytes, 40 chars — under the char cap.
        let preview = message_preview(&msg);
        // No panic reaching here is the core assertion; 40 chars < cap → no `…`.
        assert_eq!(
            preview.chars().count(),
            40,
            "a 40-char message under the cap is not clipped"
        );

        // A long non-ASCII message (over the cap) clips to cap chars + `…`.
        let long = "é".repeat(100); // 200 bytes, 100 chars.
        let preview = message_preview(&long);
        assert_eq!(
            preview.chars().count(),
            MESSAGE_PREVIEW_LEN + 1,
            "over-cap clips to the cap plus the ellipsis"
        );
        assert!(
            preview.ends_with('…'),
            "clipped preview ends with an ellipsis"
        );
    }

    /// `message_preview` collapses embedded newlines/whitespace to single
    /// spaces so a multi-line message can't break the one-row-per-signal table.
    #[test]
    fn test_message_preview_flattens_newlines() {
        let preview = message_preview("line one\nline two\t  line three");
        assert!(
            !preview.contains('\n'),
            "preview has no newline: {preview:?}"
        );
        assert!(!preview.contains('\t'), "preview has no tab: {preview:?}");
        assert_eq!(preview, "line one line two line three");
    }

    /// An absolute path outside the space root returns an error.
    #[test]
    fn test_normalize_space_rel_path_rejects_outside() {
        let root = PathBuf::from("/space/root");
        let err = normalize_space_rel_path(&root, "/other/dir/file.md").unwrap_err();
        assert!(
            err.to_string().contains("outside the space"),
            "error mentions outside: {err}"
        );
    }

    /// `resolve_document_id` looks up the normalized path in the daemon's
    /// the state snapshot and returns the document uuid. An untracked path errors.
    #[test]
    fn test_resolve_document_id_finds_tracked_and_rejects_untracked() {
        use kutl_daemon::state::{DaemonState, DocEntry};

        let space_root = tempfile::TempDir::new().unwrap();
        let kutl_dir = space_root.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();

        // Write a state snapshot with one tracked document.
        let doc_uuid = uuid::Uuid::from_u128(0xdead_beef).to_string();
        let mut state = DaemonState::default();
        state.documents.insert(
            "notes/plan.md".to_owned(),
            DocEntry {
                id: doc_uuid.clone(),
                confirmed: true,
                inode: None,
                last_written_hash: None,
            },
        );
        state.save(&kutl_dir).unwrap();

        // Tracked path resolves.
        let id = resolve_document_id(space_root.path(), "notes/plan.md").unwrap();
        assert_eq!(id, doc_uuid);

        // `./notes/plan.md` normalizes to the same key.
        let id2 = resolve_document_id(space_root.path(), "./notes/plan.md").unwrap();
        assert_eq!(id2, doc_uuid, "dot-prefixed path resolves to the same id");

        // Untracked path errors.
        let err = resolve_document_id(space_root.path(), "notes/missing.md").unwrap_err();
        assert!(
            err.to_string().contains("not tracked"),
            "error mentions not tracked: {err}"
        );
    }

    // --- space feed (merge/interleave) tests --------------------------------

    /// Build a minimal `RegistryEntry` for feed merge tests.
    fn registry_entry(
        path: &str,
        created_at: i64,
        edited_at: Option<i64>,
    ) -> kutl_proto::sync::RegistryEntry {
        kutl_proto::sync::RegistryEntry {
            document_id: uuid::Uuid::new_v4().to_string(),
            path: path.to_owned(),
            created_by: "did:key:creator".to_owned(),
            created_at,
            renamed_by: None,
            renamed_at: None,
            deleted_at: None,
            edited_at,
            account_id: None,
            originally_created_at: None,
            source_kind: None,
            source_id: None,
            source_url: None,
            ingestion_job_id: None,
            source_author_display: None,
        }
    }

    /// `merge_feed_blocks` interleaves edits and signals newest-first, and both
    /// kinds appear in the rendered output.
    #[test]
    fn test_merge_feed_blocks_interleaves_both_kinds_newest_first() {
        // An edit at t=200, a signal at t=100.
        let entry = registry_entry("notes/plan.md", 200, None);
        let signal = created_flag("sig-a", "r1", None, 100);

        let blocks = merge_feed_blocks(&[signal], &[entry]);
        assert_eq!(blocks.len(), 2, "one edit + one signal = two blocks");

        // First block is newest: the edit at t=200.
        assert_eq!(blocks[0].timestamp_ms, 200, "edit (t=200) is first");
        assert!(
            blocks[0].text.contains("notes/plan.md"),
            "edit block shows the path: {}",
            blocks[0].text
        );

        // Second block: the signal at t=100.
        assert_eq!(blocks[1].timestamp_ms, 100, "signal (t=100) is second");
        assert!(
            blocks[1].text.contains("sig-a"),
            "signal block shows the id: {}",
            blocks[1].text
        );
    }

    /// `render_change_entry` uses `created_at` as the effective timestamp when
    /// no rename / edit / delete is present, and the `created_by` DID as author.
    #[test]
    fn test_render_change_entry_created_only() {
        let entry = registry_entry("docs/intro.md", 1_000, None);
        let block = render_change_entry(0, &entry);

        assert_eq!(block.timestamp_ms, 1_000, "effective ts = created_at");
        assert!(
            block.text.contains("docs/intro.md"),
            "path in block: {}",
            block.text
        );
        assert!(
            block.text.contains("did:key:creator"),
            "author did in block: {}",
            block.text
        );
        assert!(
            block.text.contains("Date:"),
            "Date line present: {}",
            block.text
        );
    }

    /// `render_change_entry` picks `edited_at` over `created_at` as the
    /// effective timestamp when it is later.
    #[test]
    fn test_render_change_entry_edited_at_wins_over_created_at() {
        let entry = registry_entry("docs/intro.md", 1_000, Some(5_000));
        let block = render_change_entry(0, &entry);
        assert_eq!(
            block.timestamp_ms, 5_000,
            "effective ts = edited_at (later)"
        );
    }

    /// `render_change_entry` picks `renamed_at` over `created_at` when it is
    /// later, and uses `renamed_by` as the author when it is the most-recent event.
    #[test]
    fn test_render_change_entry_renamed_at_wins_and_shows_renamed_by() {
        let mut entry = registry_entry("docs/intro.md", 1_000, None);
        entry.renamed_at = Some(3_000);
        entry.renamed_by = Some("did:key:renamer".to_owned());

        let block = render_change_entry(0, &entry);
        assert_eq!(block.timestamp_ms, 3_000, "effective ts = renamed_at");
        assert!(
            block.text.contains("did:key:renamer"),
            "renamed_by DID in block: {}",
            block.text
        );
    }

    /// A purely-local signals feed (empty edit list) still produces blocks.
    #[test]
    fn test_merge_feed_blocks_signals_only() {
        let signal = created_flag("sig-local", "r1", None, 500);
        let blocks = merge_feed_blocks(&[signal], &[]);
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].text.contains("sig-local"), "{}", blocks[0].text);
    }

    /// A feed/log block for an addressed flag names its recipient — the third
    /// human surface after list and view; none of them may hide who a flag
    /// is for.
    #[test]
    fn test_signal_log_block_names_the_recipient() {
        let mut addressed = created_flag("sig-addressed", "r1", None, 500);
        if let Some(signal::Payload::Flag(f)) = &mut addressed.payload {
            f.audience = Some(kutl_proto::vocab::participant_audience("did:key:zBob"));
        }
        let blocks = merge_feed_blocks(&[addressed], &[]);
        assert_eq!(blocks.len(), 1);
        assert!(
            blocks[0].text.contains("did:key:zBob"),
            "the block names the recipient: {}",
            blocks[0].text
        );
    }

    /// An empty feed produces no blocks.
    #[test]
    fn test_merge_feed_blocks_empty() {
        let blocks = merge_feed_blocks(&[], &[]);
        assert!(blocks.is_empty(), "no input → no blocks");
    }

    /// `--follow --format json` is a stream, so each record has to stand alone
    /// on its own line: a reader appending the output to a file parses what has
    /// landed without waiting for a close brace that never comes.
    #[test]
    fn test_feed_ndjson_page_emits_one_tagged_record_per_line() {
        let page = kutl_client::FeedPage {
            signals: vec![created_flag("sig-1", "r1", Some("a.md"), 500)],
            document_changes: vec![registry_entry("docs/intro.md", 1_000, None)],
            checkpoint: "7:9".to_owned(),
        };

        let out = feed_ndjson_page(&page).expect("a page renders");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2, "one line per record: {out}");

        let first: serde_json::Value =
            serde_json::from_str(lines[0]).expect("line 1 is one object");
        assert_eq!(first["type"], "signal");
        assert_eq!(first["record"]["id"], "sig-1");

        let second: serde_json::Value =
            serde_json::from_str(lines[1]).expect("line 2 is one object");
        assert_eq!(second["type"], "document_change");
        assert_eq!(second["record"]["path"], "docs/intro.md");
    }

    /// A poll that found nothing writes nothing. A stream padded with empty
    /// objects makes a consumer filter noise to find activity.
    #[test]
    fn test_feed_ndjson_page_empty_page_writes_nothing() {
        let page = kutl_client::FeedPage {
            signals: Vec::new(),
            document_changes: Vec::new(),
            checkpoint: "0:0".to_owned(),
        };
        assert_eq!(feed_ndjson_page(&page).expect("an empty page renders"), "");
    }
}
