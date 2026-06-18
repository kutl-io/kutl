//! File watcher with debouncing and echo suppression.
//!
//! Uses `notify` to watch a space directory for file changes. Includes
//! manual debouncing and suppression of events caused by the daemon's own writes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use notify::event::{ModifyKind, RenameMode};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{debug, warn};

mod rename_tracker;
mod suppression;

pub use suppression::Suppression;

use rename_tracker::{RenameResolution, RenameTracker};
use suppression::SuppressionSet;

/// Debounce delay for file system events.
const DEBOUNCE_DELAY: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_millis(200));

// Files and directories beginning with `.` are unconditionally ignored. This
// covers `.kutl/` (daemon state), `.git/` (git plumbing), `.gitignore` and
// `.kutlspace` (system files), `.DS_Store`, and any future hidden files.
// kutl spaces are for documents, and the subfolder convention makes the
// visible-name denylist vestigial.

/// Debounce priority for events that don't map to a file action (e.g. `Access`).
const PRIORITY_OTHER: u8 = 1;
/// Debounce priority for content-changing events (`Create`, `Modify`).
const PRIORITY_CONTENT: u8 = 2;
/// Debounce priority for deletion events (`Remove`).
const PRIORITY_REMOVE: u8 = 3;

/// Events emitted by the file watcher.
#[derive(Debug, Clone)]
pub enum FileEvent {
    /// A file was modified or created.
    Modified { rel_path: PathBuf },
    /// A file was removed.
    Removed { rel_path: PathBuf },
    /// A file was renamed (detected from OS rename events).
    /// The daemon correlates by inode when explicit rename events aren't available.
    Renamed {
        /// Old relative path.
        old_path: PathBuf,
        /// New relative path.
        new_path: PathBuf,
    },
}

/// Watches a space directory for file changes.
pub struct FileWatcher {
    space_root: PathBuf,
    /// Paths currently suppressed (daemon's own writes), with per-entry TTL.
    suppressed: SuppressionSet,
    /// Raw notify watcher handle (must stay alive).
    _watcher: RecommendedWatcher,
    /// Buffer for raw events from notify callback (shared with callback thread).
    raw_buffer: Arc<Mutex<Vec<notify::Event>>>,
    /// Sends debounced events to the daemon.
    event_tx: mpsc::Sender<FileEvent>,
    /// Receives suppression commands from the daemon. Unbounded so the daemon's
    /// loop never blocks producing a suppression (the deadlock-cycle fix); the
    /// watcher drains it with `try_recv` per poll tick.
    suppress_rx: mpsc::UnboundedReceiver<Suppression>,
}

impl FileWatcher {
    /// Create a new file watcher for the given space root.
    ///
    /// Expects `space_root` to be a canonical (resolved) path so that
    /// `strip_prefix` works correctly with OS-level file event paths.
    ///
    /// Returns the watcher and a sender for echo suppression. Send a
    /// relative path before writing to suppress the resulting watcher event.
    pub fn new(
        space_root: &Path,
        event_tx: mpsc::Sender<FileEvent>,
        suppress_rx: mpsc::UnboundedReceiver<Suppression>,
    ) -> Result<Self> {
        let raw_buffer: Arc<Mutex<Vec<notify::Event>>> = Arc::new(Mutex::new(Vec::new()));
        let buffer_clone = Arc::clone(&raw_buffer);

        let mut watcher =
            notify::recommended_watcher(move |res: std::result::Result<notify::Event, _>| {
                if let Ok(event) = res
                    && let Ok(mut buf) = buffer_clone.lock()
                {
                    buf.push(event);
                }
            })
            .context("failed to create file watcher")?;

        watcher
            .watch(space_root, RecursiveMode::Recursive)
            .with_context(|| format!("failed to watch directory {}", space_root.display()))?;

        Ok(Self {
            space_root: space_root.to_owned(),
            suppressed: SuppressionSet::default(),
            _watcher: watcher,
            raw_buffer,
            event_tx,
            suppress_rx,
        })
    }

    /// Run the watcher event loop with debouncing.
    ///
    /// Polls for raw events on a timer, debounces them, filters ignored
    /// paths and suppressed writes, then sends `FileEvent`s to the daemon.
    pub async fn run(&mut self) {
        let mut pending: HashMap<PathBuf, EventKind> = HashMap::new();
        // Pending rename-from halves awaiting their matching rename-to, paired
        // by platform cookie and carried across poll cycles.
        let mut rename_tracker = RenameTracker::default();
        // Completed rename pairs detected during this poll interval.
        let mut rename_pairs: Vec<(PathBuf, PathBuf)> = Vec::new();

        let mut poll_interval = tokio::time::interval(DEBOUNCE_DELAY);
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            poll_interval.tick().await;

            // Drain suppression commands.
            while let Ok(suppression) = self.suppress_rx.try_recv() {
                match suppression {
                    Suppression::Single(path, expected) => {
                        self.suppressed.insert(path, expected);
                    }
                    Suppression::RenamePair { old, new, hash } => {
                        self.suppressed.insert_rename_pair(old, new, hash);
                    }
                }
            }

            // Drain raw events from the shared buffer.
            let raw_events: Vec<notify::Event> = {
                let Ok(mut buf) = self.raw_buffer.lock() else {
                    warn!("raw_buffer mutex poisoned, skipping poll");
                    continue;
                };
                buf.drain(..).collect()
            };

            // Single-path `Modify(Name(Any))` events seen this poll. macOS
            // FSEvents reports a rename it cannot correlate as TWO of these
            // (old name, new name); for a CASE-ONLY rename they are the only
            // signal — see the pairing pass after this loop.
            let mut name_any_singles: Vec<PathBuf> = Vec::new();

            for event in &raw_events {
                self.ingest_raw_event(
                    event,
                    &mut pending,
                    &mut rename_tracker,
                    &mut rename_pairs,
                    &mut name_any_singles,
                );
            }

            // Pair CASE-ONLY renames out of the uncorrelated Name(Any) singles.
            // On a case-insensitive filesystem this is the only viable signal:
            // routed as plain Modifieds (the old behavior), the old name
            // classifies as a no-op edit (the case-insensitive read resolves to
            // the renamed file) and the new name is eaten by the case-collision
            // guard — the rename is silently invisible cluster-wide. ONLY
            // casefold-equal distinct names pair (direction = whichever exact
            // byte name the parent listing shows); every other Name(Any) single
            // keeps the pending-Modified path — atomic editor saves (tmp →
            // target renames) rely on it.
            let (case_pairs, leftovers) = pair_case_only_renames(name_any_singles, |rel| {
                exact_name_on_disk(&self.space_root, rel)
            });
            rename_pairs.extend(case_pairs);
            for rel in leftovers {
                // Leftovers stay on the content route UNCONDITIONALLY. Routing
                // a gone-path leftover as a Remove here was a CATASTROPHIC
                // regression (caught by the matched A/B, 3/12 vs 12/12): the
                // old half of an ordinary uncorrelated local rename is exactly
                // such a single, and a Remove on it UNREGISTERS the tracked doc
                // cluster-wide before the new half's inode-rename detection
                // runs. The daemon — which knows tracked state — resolves a
                // missing-path Modified instead: a rename old-half is a no-op
                // there, while a vanished DIRECTORY (tracked children, all
                // gone) expands to per-child removals (`expand_removed_dir`).
                Self::upsert_pending(&mut pending, rel, EventKind::Modify(ModifyKind::Any));
            }

            // Rename-from halves whose match never arrived (after the carry
            // window) are demoted to deletions.
            for old in rename_tracker.age_and_take_expired() {
                pending.insert(old, EventKind::Remove(notify::event::RemoveKind::Any));
            }

            // Flush pending events.
            self.flush_pending(&mut pending, &mut rename_pairs).await;

            // Expire stale echo suppressions whose event never arrived.
            self.suppressed.tick();
        }
    }

    /// Sort one raw `notify` event into the poll's debounce structures: rename
    /// pairs, the cookie-correlated rename tracker, the uncorrelated
    /// `Name(Any)` singles (the case-only-rename pairing input), or the plain
    /// pending map.
    fn ingest_raw_event(
        &self,
        event: &notify::Event,
        pending: &mut HashMap<PathBuf, EventKind>,
        rename_tracker: &mut RenameTracker,
        rename_pairs: &mut Vec<(PathBuf, PathBuf)>,
        name_any_singles: &mut Vec<PathBuf>,
    ) {
        match event.kind {
            // A single-path rename notice the platform could not pair.
            // Collected for the casefold pairing pass; everything unpaired
            // falls through to the pending map exactly as the `_` arm would
            // have routed it.
            EventKind::Modify(ModifyKind::Name(RenameMode::Any)) if event.paths.len() == 1 => {
                if let Some(rel) = self.to_relative(&event.paths[0])
                    && !Self::should_ignore(&rel)
                {
                    name_any_singles.push(rel);
                }
            }
            // Both paths in one event (e.g. macOS kqueue).
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
                if event.paths.len() >= 2 {
                    match (
                        self.to_relative(&event.paths[0]),
                        self.to_relative(&event.paths[1]),
                    ) {
                        (Some(old), Some(new))
                            if !Self::should_ignore(&old) && !Self::should_ignore(&new) =>
                        {
                            rename_pairs.push((old, new));
                        }
                        // Renamed OUT of the watched space (Finder "Move to
                        // Trash" is exactly this, often at the directory
                        // level): the path left the space, so it is a removal
                        // — dropping the half-outside pair entirely made a
                        // trashed tree silently invisible cluster-wide.
                        (Some(old), None) if !Self::should_ignore(&old) => {
                            Self::upsert_pending(
                                pending,
                                old,
                                EventKind::Remove(notify::event::RemoveKind::Any),
                            );
                        }
                        _ => {}
                    }
                }
            }
            // First half of a rename pair.
            EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                let tracker = event.attrs.tracker();
                for path in &event.paths {
                    if let Some(rel) = self.to_relative(path)
                        && !Self::should_ignore(&rel)
                    {
                        rename_tracker.on_from(tracker, rel);
                    }
                }
            }
            // Second half of a rename pair.
            EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                let tracker = event.attrs.tracker();
                for path in &event.paths {
                    if let Some(rel) = self.to_relative(path) {
                        if Self::should_ignore(&rel) {
                            continue;
                        }
                        match rename_tracker.on_to(tracker, rel) {
                            RenameResolution::Paired { old_path, new_path } => {
                                rename_pairs.push((old_path, new_path));
                            }
                            RenameResolution::NewFile { new_path } => {
                                pending.insert(new_path, EventKind::Modify(ModifyKind::Any));
                            }
                        }
                    }
                }
            }
            // Platform couldn't distinguish rename type, or non-rename events:
            // standard debounce logic.
            _ => {
                for path in &event.paths {
                    if let Some(rel) = self.to_relative(path)
                        && !Self::should_ignore(&rel)
                    {
                        Self::upsert_pending(pending, rel, event.kind);
                    }
                }
            }
        }
    }

    /// Insert or upgrade a pending event (higher priority wins).
    fn upsert_pending(pending: &mut HashMap<PathBuf, EventKind>, rel: PathBuf, kind: EventKind) {
        pending
            .entry(rel)
            .and_modify(|existing| {
                if Self::event_priority(kind) > Self::event_priority(*existing) {
                    *existing = kind;
                }
            })
            .or_insert(kind);
    }

    /// Flush pending events, applying suppression.
    ///
    /// Only removes a suppression when it actually suppresses an event,
    /// so suppressions persist until consumed rather than being cleared
    /// each flush cycle.
    async fn flush_pending(
        &mut self,
        pending: &mut HashMap<PathBuf, EventKind>,
        rename_pairs: &mut Vec<(PathBuf, PathBuf)>,
    ) {
        if pending.is_empty() && rename_pairs.is_empty() {
            return;
        }

        // Emit rename pairs first.
        for (old_path, new_path) in rename_pairs.drain(..) {
            // PAIR-MATCHED: fires only for the daemon's OWN registered rename
            // pair (either linked half suffices; both are cleared). An
            // unrelated single write/removal entry sharing one of these paths
            // is left alone and can no longer swallow a USER rename.
            if self
                .suppressed
                .consume_rename_pair_echo(&old_path, &new_path)
            {
                debug!(?old_path, ?new_path, "suppressed echo rename event");
                continue;
            }
            // A directory move (`mv notes/ archive/`) arrives as ONE dir-level
            // pair — no per-child events fire. Expand it into per-child renames
            // (the engine tracks documents, not directories), each re-running
            // the pair suppression so a daemon-originated file rename into the
            // tree is still suppressed individually.
            if self.space_root.join(&new_path).is_dir() {
                if !self.send_dir_rename_children(&old_path, &new_path).await {
                    return;
                }
                continue;
            }
            let event = FileEvent::Renamed { old_path, new_path };
            if self.event_tx.send(event).await.is_err() {
                warn!("file event channel closed");
                return;
            }
        }

        let events: Vec<(PathBuf, EventKind)> = pending.drain().collect();

        for (rel_path, kind) in events {
            // An unpaired dir-shaped Create/Modified (platforms that don't pair
            // directory renames deliver the move as Create/Modified on the NEW
            // directory plus a Remove on the old) expands into per-child events:
            // each child is a fresh path carrying a tracked file's inode, so the
            // existing per-file inode-rename detection re-keys its identity (a
            // rename, not a delete + fresh mint). Without the expansion the
            // directory path itself was classified — its content read fails
            // (`EISDIR`) into a garbage Modified the daemon then errors on.
            if matches!(kind, EventKind::Create(_) | EventKind::Modify(_))
                && self.space_root.join(&rel_path).is_dir()
            {
                for child in files_under(&self.space_root, &rel_path) {
                    if let Some(file_event) =
                        classify_flush_event(&self.space_root, &mut self.suppressed, &child, kind)
                        && self.event_tx.send(file_event).await.is_err()
                    {
                        warn!("file event channel closed");
                        return;
                    }
                }
                continue;
            }
            if let Some(file_event) =
                classify_flush_event(&self.space_root, &mut self.suppressed, &rel_path, kind)
            {
                if self.event_tx.send(file_event).await.is_err() {
                    warn!("file event channel closed");
                    return;
                }
            } else {
                debug!(?rel_path, "suppressed echo or non-actionable event");
            }
        }
    }

    /// Emit per-child `Renamed` events for a directory-level rename pair
    /// (`old_dir` → `new_dir`): every file now under `new_dir` is mapped back to
    /// its pre-move path under `old_dir` by suffix. Returns `false` when the
    /// event channel closed (the caller's stop signal).
    async fn send_dir_rename_children(&mut self, old_dir: &Path, new_dir: &Path) -> bool {
        for new_child in files_under(&self.space_root, new_dir) {
            let Ok(suffix) = new_child.strip_prefix(new_dir) else {
                // Unreachable in practice: `files_under(new_dir)` yields only
                // paths under `new_dir`. Skip rather than emit a wrong pair.
                continue;
            };
            let old_child = old_dir.join(suffix);
            // PAIR-MATCHED, as in the top-level pair route above.
            if self
                .suppressed
                .consume_rename_pair_echo(&old_child, &new_child)
            {
                debug!(
                    ?old_child,
                    ?new_child,
                    "suppressed echo rename event (dir expansion)"
                );
                continue;
            }
            let event = FileEvent::Renamed {
                old_path: old_child,
                new_path: new_child,
            };
            if self.event_tx.send(event).await.is_err() {
                warn!("file event channel closed");
                return false;
            }
        }
        true
    }

    /// Convert an absolute path to a space-relative path.
    fn to_relative(&self, abs_path: &Path) -> Option<PathBuf> {
        abs_path
            .strip_prefix(&self.space_root)
            .ok()
            .map(PathBuf::from)
    }

    /// Priority ordering for debouncing: higher values win when multiple
    /// events arrive for the same path in the same poll interval.
    ///
    /// Remove > Create/Modify > Access/Other. This prevents a trailing
    /// `Access(Close(Write))` from shadowing a `Modify` event on Linux.
    fn event_priority(kind: EventKind) -> u8 {
        match kind {
            EventKind::Remove(_) => PRIORITY_REMOVE,
            EventKind::Create(_) | EventKind::Modify(_) => PRIORITY_CONTENT,
            _ => PRIORITY_OTHER,
        }
    }

    /// Check if a relative path should be ignored.
    ///
    /// Any path component beginning with `.` is ignored. See the doc comment
    /// at the top of this module for rationale.
    ///
    /// Also available as the free function [`should_ignore`] for use outside
    /// the watcher (e.g. initial file scan on startup).
    fn should_ignore(rel_path: &Path) -> bool {
        for component in rel_path.components() {
            if let std::path::Component::Normal(name) = component {
                let name_str = name.to_string_lossy();
                if name_str.starts_with('.') {
                    return true;
                }
            }
        }
        false
    }
}

/// Check if a relative path should be ignored (same rules as the file watcher).
///
/// Used by the daemon's initial file scan to apply consistent filtering.
pub fn should_ignore(rel_path: &Path) -> bool {
    FileWatcher::should_ignore(rel_path)
}

/// Pair case-only rename halves out of uncorrelated single-path `Name(Any)`
/// notices: two DISTINCT names that casefold-equal are one rename, ordered by
/// `exact_on_disk` (the half whose exact byte name the parent listing shows is
/// the NEW name). Everything else — including pairs where the direction is
/// ambiguous (both or neither present) — is returned as leftovers for the
/// ordinary pending-Modified route. Pure (presence injected) for unit testing.
fn pair_case_only_renames(
    singles: Vec<PathBuf>,
    exact_on_disk: impl Fn(&Path) -> bool,
) -> (Vec<(PathBuf, PathBuf)>, Vec<PathBuf>) {
    let mut pairs = Vec::new();
    let mut leftovers: Vec<PathBuf> = Vec::new();
    let mut remaining = singles;
    while let Some(candidate) = remaining.pop() {
        let folded = candidate.to_string_lossy().to_lowercase();
        let partner = remaining.iter().position(|other| {
            other.as_path() != candidate.as_path()
                && other.to_string_lossy().to_lowercase() == folded
        });
        let Some(idx) = partner else {
            leftovers.push(candidate);
            continue;
        };
        let other = remaining.remove(idx);
        match (exact_on_disk(&candidate), exact_on_disk(&other)) {
            // Exactly one name is on disk byte-exact: it is the rename target.
            (true, false) => pairs.push((other, candidate)),
            (false, true) => pairs.push((candidate, other)),
            // Ambiguous (transient disk state): fall back to plain Modifieds.
            _ => {
                leftovers.push(candidate);
                leftovers.push(other);
            }
        }
    }
    (pairs, leftovers)
}

/// Whether the EXACT byte name of space-relative `rel` appears in its parent
/// directory listing. On a case-insensitive filesystem `exists()` resolves any
/// casing, so a case-only rename's OLD name still "exists" — only the literal
/// directory entry distinguishes the two halves.
fn exact_name_on_disk(space_root: &Path, rel: &Path) -> bool {
    let Some(name) = rel.file_name() else {
        return false;
    };
    let parent_abs = match rel.parent() {
        Some(parent) => space_root.join(parent),
        None => space_root.to_path_buf(),
    };
    std::fs::read_dir(parent_abs).is_ok_and(|entries| {
        entries
            .filter_map(std::result::Result::ok)
            .any(|entry| entry.file_name() == name)
    })
}

/// Recursively list the FILES under the directory at space-relative `rel_dir`,
/// as space-relative paths with ignored paths filtered, sorted for
/// deterministic emission order. The expansion source for directory-level
/// watcher events: a `mv notes/ archive/` surfaces as a single dir-shaped
/// event (a rename pair or an unpaired Create/Modified, by platform) and must
/// be fanned out into per-child file events — the sync engine tracks
/// documents, not directories.
fn files_under(space_root: &Path, rel_dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = walkdir::WalkDir::new(space_root.join(rel_dir))
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter_map(|entry| {
            entry
                .path()
                .strip_prefix(space_root)
                .ok()
                .map(Path::to_path_buf)
        })
        .filter(|rel| !FileWatcher::should_ignore(rel))
        .collect();
    files.sort();
    files
}

/// Read the current on-disk bytes for echo classification, logging — not
/// swallowing — a transient read error (permissions, the file vanishing between
/// `exists()` and `read`). On failure returns empty bytes, which is the SAFE
/// direction: a daemon-written echo's stored hash will not match `sha256([])`,
/// so the event surfaces as a genuine change rather than being mis-suppressed.
fn read_current_for_echo(abs: &Path, rel_path: &Path) -> Vec<u8> {
    match std::fs::read(abs) {
        Ok(bytes) => bytes,
        Err(err) => {
            debug!(
                path = %rel_path.display(),
                %err,
                "content read failed during echo classification; treating as non-echo"
            );
            Vec::new()
        }
    }
}

/// Classify one debounced filesystem event into the [`FileEvent`] to emit, or
/// `None` when it is suppressed (a daemon-own echo) or carries no document
/// change. Pure of the `notify` watcher and the async channel, so the §4.2
/// recreate-over-delete logic is unit-testable without spinning a real watcher.
///
/// The load-bearing case is the first `Remove` arm: a delete and a recreate of
/// the same path that coalesced to a single `Remove` within one debounce window
/// (the removal wins the priority race) but whose file is **back on disk** at
/// flush. It must surface as the recreate (a content change), not be swallowed
/// as the daemon's own removal echo — otherwise two peers recreating a
/// tombstoned path concurrently each lose the other's content and diverge.
///
/// # Contract — DO NOT collapse the per-kind suppression dispatch
///
/// Each arm MUST consume via the suppression type that matches the event kind:
/// a removal echo (a `None`-payload suppression, registered by `remove_doc` /
/// the old half of a `RenamePair`) is cleared ONLY via `consume_removal_echo`;
/// a write echo (a `Some(hash)` suppression, registered by `write_doc` / the
/// new half of a `RenamePair`) is cleared ONLY via `consume_write_echo`; an OS
/// rename PAIR is cleared ONLY via `consume_rename_pair_echo`, which is now
/// TYPE-enforced to fire solely for the daemon's own registered pair (an
/// unrelated single entry sharing a path cannot swallow a user rename). The
/// per-kind arms below remain — the typed entries enforce the pairing, the
/// arms still pick the matching consumer. The arms look like duplicated
/// boilerplate, but the divergence is the whole point: routing a removal/rename
/// event through `consume_write_echo` MISSES the daemon's own removal/rename
/// suppressions and **resurrects files mid-rename** — the catastrophic §1.3/§4.2
/// regression (commit `5dd87257`; a single green pass did NOT catch it — only a
/// back-to-back A/B against clean HEAD did). The ONLY arm that may legitimately
/// consult both is the Remove-while-file-exists sub-case below (the recreate it
/// coalesced over). Change nothing else here without that A/B + the expensive lane.
fn classify_flush_event(
    space_root: &Path,
    suppressed: &mut SuppressionSet,
    rel_path: &Path,
    kind: EventKind,
) -> Option<FileEvent> {
    let abs = space_root.join(rel_path);
    match kind {
        EventKind::Remove(_) if abs.exists() => {
            // The recreate is already on disk. Clear the stale removal echo (the
            // daemon's own delete); the write-echo guard still suppresses a file
            // the daemon itself wrote back (e.g. a remote recreate it applied).
            let _ = suppressed.consume_removal_echo(rel_path);
            let current = read_current_for_echo(&abs, rel_path);
            (!suppressed.consume_write_echo(rel_path, &current)).then(|| FileEvent::Modified {
                rel_path: rel_path.to_path_buf(),
            })
        }
        EventKind::Remove(_) => {
            (!suppressed.consume_removal_echo(rel_path)).then(|| FileEvent::Removed {
                rel_path: rel_path.to_path_buf(),
            })
        }
        EventKind::Create(_) | EventKind::Modify(_) => {
            // Read the file's current content to tell an echo (matches the bytes
            // the daemon wrote) from a genuine concurrent edit.
            let current = read_current_for_echo(&abs, rel_path);
            (!suppressed.consume_write_echo(rel_path, &current)).then(|| FileEvent::Modified {
                rel_path: rel_path.to_path_buf(),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Casefold pairing of uncorrelated `Name(Any)` singles: a case-only
    /// rename pairs with the on-disk exact name as the target; non-case
    /// singles (the atomic-save tmp + target shape) and direction-ambiguous
    /// pairs stay leftovers for the ordinary pending-Modified route.
    #[test]
    fn test_pair_case_only_renames() {
        let on_disk = |present: &'static [&'static str]| {
            move |rel: &Path| present.iter().any(|p| Path::new(p) == rel)
        };

        // The case-only rename: old gone byte-exact, new present.
        let (pairs, leftovers) = pair_case_only_renames(
            vec![pb("readme.md"), pb("README.md")],
            on_disk(&["README.md"]),
        );
        assert_eq!(pairs, vec![(pb("readme.md"), pb("README.md"))]);
        assert!(leftovers.is_empty());

        // Atomic-save shape (tmp + target, NOT casefold-equal): never paired.
        let (pairs, mut leftovers) =
            pair_case_only_renames(vec![pb("doc.md.tmp1"), pb("doc.md")], on_disk(&["doc.md"]));
        assert!(pairs.is_empty());
        leftovers.sort();
        assert_eq!(leftovers, vec![pb("doc.md"), pb("doc.md.tmp1")]);

        // Direction-ambiguous (both names listed — transient): not paired.
        let (pairs, leftovers) =
            pair_case_only_renames(vec![pb("a.md"), pb("A.md")], on_disk(&["a.md", "A.md"]));
        assert!(pairs.is_empty());
        assert_eq!(leftovers.len(), 2);

        // Nested paths pair too, and an unrelated single passes through.
        let (pairs, leftovers) = pair_case_only_renames(
            vec![pb("docs/note.md"), pb("other.md"), pb("docs/Note.md")],
            on_disk(&["docs/Note.md", "other.md"]),
        );
        assert_eq!(pairs, vec![(pb("docs/note.md"), pb("docs/Note.md"))]);
        assert_eq!(leftovers, vec![pb("other.md")]);
    }

    /// The dir-event expansion source: nested files come back space-relative
    /// and sorted, ignored (dot-prefixed) paths are filtered, directories
    /// themselves are not listed, and a non-directory input yields nothing.
    #[test]
    fn test_files_under_lists_nested_files_filtered_and_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("archive/sub")).unwrap();
        std::fs::write(root.join("archive/b.md"), "b").unwrap();
        std::fs::write(root.join("archive/a.md"), "a").unwrap();
        std::fs::write(root.join("archive/sub/c.md"), "c").unwrap();
        std::fs::write(root.join("archive/.hidden.md"), "x").unwrap();
        std::fs::write(root.join("outside.md"), "o").unwrap();

        assert_eq!(
            files_under(root, Path::new("archive")),
            vec![
                PathBuf::from("archive/a.md"),
                PathBuf::from("archive/b.md"),
                PathBuf::from("archive/sub/c.md"),
            ]
        );
    }

    #[test]
    fn test_should_ignore_hidden_paths() {
        // Hidden files and directories at any depth are ignored.
        assert!(FileWatcher::should_ignore(Path::new(".git/config")));
        assert!(FileWatcher::should_ignore(Path::new(".kutl/docs/foo.dt")));
        assert!(FileWatcher::should_ignore(Path::new(".DS_Store")));
        assert!(FileWatcher::should_ignore(Path::new(".gitignore")));
        assert!(FileWatcher::should_ignore(Path::new(".kutlspace")));
        assert!(FileWatcher::should_ignore(Path::new("docs/.hidden/foo.md")));
        assert!(FileWatcher::should_ignore(Path::new("a/b/.config.toml")));

        // Visible files and directories are NOT ignored, even with names that
        // were historically hardcoded (kutl spaces should not contain these).
        assert!(!FileWatcher::should_ignore(Path::new("src/main.rs")));
        assert!(!FileWatcher::should_ignore(Path::new("docs/intro.md")));
        assert!(!FileWatcher::should_ignore(Path::new(
            "node_modules/foo/bar.js"
        )));
        assert!(!FileWatcher::should_ignore(Path::new("target/debug/x")));
    }

    /// Shared across the watcher-family test mods (`rename_tracker`,
    /// `suppression` import it as `crate::watcher::tests::pb`).
    pub(super) fn pb(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    /// §4.2: a delete+recreate that coalesces to a single `Remove` within one
    /// debounce window must surface as the recreate (a content change) when the
    /// file is back on disk at flush — NOT swallowed as the daemon's removal
    /// echo (which loses a concurrent recreate-at-tombstone and diverges peers).
    /// The complements: a `Remove` whose file is gone still emits `Removed`, and
    /// the daemon's own delete echo (file gone) is suppressed. Deterministic
    /// guard behind the watcher fix that was previously only exercised by the
    /// wall-clock e2e — and pure of `notify`, so it runs in milliseconds.
    #[test]
    fn test_classify_remove_with_file_present_is_a_recreate() {
        let dir = tempfile::tempdir().unwrap();
        let mut sup = SuppressionSet::default();
        let remove = || EventKind::Remove(notify::event::RemoveKind::Any);

        // The recreate already landed on disk: the path EXISTS at flush.
        let back = pb("doc.md");
        std::fs::write(dir.path().join(&back), b"base\nREVIVE\n").unwrap();
        assert!(
            matches!(
                classify_flush_event(dir.path(), &mut sup, &back, remove()),
                Some(FileEvent::Modified { .. })
            ),
            "a Remove whose file is back must classify as the recreate (Modified)"
        );

        // A Remove whose file is genuinely gone stays a Removed.
        let gone = pb("gone.md");
        assert!(
            matches!(
                classify_flush_event(dir.path(), &mut sup, &gone, remove()),
                Some(FileEvent::Removed { .. })
            ),
            "a Remove whose file is absent must classify as Removed"
        );

        // The daemon's own delete echo (file gone + removal suppression) is dropped.
        let echo = pb("echo.md");
        sup.insert(echo.clone(), None);
        assert!(
            classify_flush_event(dir.path(), &mut sup, &echo, remove()).is_none(),
            "the daemon's own removal echo is suppressed"
        );
    }
}
