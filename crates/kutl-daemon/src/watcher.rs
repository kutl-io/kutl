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

/// Extra poll cycles an unmatched rename-from half waits for its matching
/// rename-to before being demoted to a deletion. On Linux the two halves of a
/// rename can land in adjacent poll intervals, so dropping an unmatched `From`
/// at the end of its first poll would degrade a real rename into delete+create
/// (losing document identity).
const RENAME_PAIR_CARRY_POLLS: u8 = 1;

/// Poll cycles an echo-suppression entry lives before expiring. Bounds the
/// window in which a stranded suppression (whose echo event never arrived)
/// could swallow a later genuine edit, and prevents unbounded growth.
const SUPPRESS_TTL_POLLS: u8 = 5;

/// Resolution of a rename-to event half.
#[derive(Debug, PartialEq, Eq)]
enum RenameResolution {
    /// Paired with a previously-seen rename-from half.
    Paired {
        old_path: PathBuf,
        new_path: PathBuf,
    },
    /// No matching from-half: treat the path as a newly created file.
    NewFile { new_path: PathBuf },
}

/// Correlates rename-from / rename-to event halves across poll cycles.
///
/// Linux inotify reports a rename as separate `From` and `To` events that share
/// a "tracker" cookie; macOS kqueue reports both paths in one event. When
/// several renames batch into one poll, pairing by arrival order mis-pairs them
/// (moving the wrong document to the wrong path), so we pair by cookie and only
/// fall back to FIFO arrival order for platforms that supply no cookie.
///
/// Unmatched `From` halves are carried for [`RENAME_PAIR_CARRY_POLLS`] extra
/// polls — the matching `To` may arrive in the next interval — before being
/// demoted to deletions.
#[derive(Default)]
struct RenameTracker {
    /// Cookie-keyed pending from-halves: `tracker -> (old_path, polls_waited)`.
    tracked: HashMap<usize, (PathBuf, u8)>,
    /// Fallback for platforms that supply no cookie, in arrival order.
    untracked: Vec<(PathBuf, u8)>,
}

impl RenameTracker {
    /// Record a rename-from half. `tracker` is the platform rename cookie, if any.
    fn on_from(&mut self, tracker: Option<usize>, old_path: PathBuf) {
        match tracker {
            Some(t) => {
                self.tracked.insert(t, (old_path, 0));
            }
            None => self.untracked.push((old_path, 0)),
        }
    }

    /// Resolve a rename-to half against the pending from-halves.
    fn on_to(&mut self, tracker: Option<usize>, new_path: PathBuf) -> RenameResolution {
        if let Some(t) = tracker
            && let Some((old_path, _)) = self.tracked.remove(&t)
        {
            return RenameResolution::Paired { old_path, new_path };
        }
        // No cookie, or no matching cookie: fall back to FIFO arrival order.
        if self.untracked.is_empty() {
            RenameResolution::NewFile { new_path }
        } else {
            let (old_path, _) = self.untracked.remove(0);
            RenameResolution::Paired { old_path, new_path }
        }
    }

    /// Age pending from-halves; return old paths whose match never arrived
    /// (now demoted to deletions). Call once per poll, after processing events.
    fn age_and_take_expired(&mut self) -> Vec<PathBuf> {
        let mut expired = Vec::new();
        let mut age = |path: &PathBuf, waited: &mut u8| -> bool {
            if *waited >= RENAME_PAIR_CARRY_POLLS {
                expired.push(path.clone());
                false
            } else {
                *waited += 1;
                true
            }
        };
        self.tracked.retain(|_, (path, waited)| age(path, waited));
        self.untracked
            .retain_mut(|(path, waited)| age(path, waited));
        expired
    }
}

/// An echo-suppression command from the daemon: the path it wrote, plus the
/// content hash it wrote — `None` for a removal (the echo is a `Removed` event
/// with no content). A write echo is dropped only when the file's current
/// content hashes to this value, so a genuine concurrent edit (different
/// content) is never swallowed.
pub type Suppression = (PathBuf, Option<Vec<u8>>);

/// Echo-suppression set with a per-entry TTL.
///
/// The daemon registers a path here just before writing it, so the resulting
/// watcher event is recognised as its own echo and dropped. Writes carry the
/// content hash they produced: a write echo is then distinguished from a genuine
/// concurrent edit *by content*, so the suppression cannot silently swallow a
/// real edit even when the two coalesce into one poll-cycle event. A TTL bounds
/// the set's growth if an echo never arrives.
#[derive(Default)]
struct SuppressionSet {
    /// `path -> (expected content hash, or None for a removal; poll cycles left)`.
    entries: HashMap<PathBuf, (Option<Vec<u8>>, u8)>,
}

impl SuppressionSet {
    /// Register `path` for echo suppression. `expected` is the content hash the
    /// daemon wrote, or `None` for a removal.
    fn insert(&mut self, path: PathBuf, expected: Option<Vec<u8>>) {
        self.entries.insert(path, (expected, SUPPRESS_TTL_POLLS));
    }

    /// Consume a rename echo by path, payload-agnostic ON PURPOSE: a rename
    /// registers a `None` (removal) half and a `Some(hash)` (write) half, and the
    /// flush must clear both by path. This is the ONLY consumer that ignores the
    /// payload — do not reuse it for non-rename events (a removal/write echo must
    /// go through `consume_removal_echo`/`consume_write_echo`; see the contract on
    /// `classify_flush_event`). Returns whether a suppression existed.
    fn consume_rename_echo(&mut self, path: &Path) -> bool {
        self.entries.remove(path).is_some()
    }

    /// Consume a write echo: a Create/Modify event whose current content matches
    /// what the daemon wrote. A genuine edit (different content) is NOT consumed,
    /// so it is never swallowed. Returns whether the event was an echo.
    ///
    /// `current` is the raw on-disk bytes; the suppression stores the SHA-256
    /// hash the daemon wrote (see [`write_doc`]), so we hash `current` here and
    /// compare hash-to-hash. (Comparing the stored hash against raw bytes would
    /// never match — the echo would slip through to the edit handler.)
    fn consume_write_echo(&mut self, path: &Path, current: &[u8]) -> bool {
        let current_hash = crate::blob_state::sha256_bytes(current);
        match self.entries.get(path) {
            Some((Some(expected), _)) if expected.as_slice() == current_hash.as_slice() => {
                self.entries.remove(path);
                true
            }
            _ => false,
        }
    }

    /// Consume a removal echo: a `Removed` event for a path the daemon deleted
    /// (registered with no content). Returns whether the event was an echo.
    fn consume_removal_echo(&mut self, path: &Path) -> bool {
        match self.entries.get(path) {
            Some((None, _)) => {
                self.entries.remove(path);
                true
            }
            _ => false,
        }
    }

    /// Expire entries whose TTL has elapsed. Call once per poll.
    fn tick(&mut self) {
        self.entries.retain(|_, (_, ttl)| {
            *ttl = ttl.saturating_sub(1);
            *ttl > 0
        });
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

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
    /// Receives suppression commands from the daemon.
    suppress_rx: mpsc::Receiver<Suppression>,
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
        suppress_rx: mpsc::Receiver<Suppression>,
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
            while let Ok((path, expected)) = self.suppress_rx.try_recv() {
                self.suppressed.insert(path, expected);
            }

            // Drain raw events from the shared buffer.
            let raw_events: Vec<notify::Event> = {
                let Ok(mut buf) = self.raw_buffer.lock() else {
                    warn!("raw_buffer mutex poisoned, skipping poll");
                    continue;
                };
                buf.drain(..).collect()
            };

            for event in raw_events {
                match event.kind {
                    // Both paths in one event (e.g. macOS kqueue).
                    EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
                        if event.paths.len() >= 2
                            && let (Some(old), Some(new)) = (
                                self.to_relative(&event.paths[0]),
                                self.to_relative(&event.paths[1]),
                            )
                            && !Self::should_ignore(&old)
                            && !Self::should_ignore(&new)
                        {
                            rename_pairs.push((old, new));
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
                                        pending
                                            .insert(new_path, EventKind::Modify(ModifyKind::Any));
                                    }
                                }
                            }
                        }
                    }
                    // Platform couldn't distinguish rename type, or non-rename
                    // events: standard debounce logic.
                    _ => {
                        for path in &event.paths {
                            if let Some(rel) = self.to_relative(path)
                                && !Self::should_ignore(&rel)
                            {
                                Self::upsert_pending(&mut pending, rel, event.kind);
                            }
                        }
                    }
                }
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
            // Consume BOTH suppressions without short-circuiting: a
            // daemon-originated rename suppresses both halves, and a `||` would
            // leave the second entry behind to swallow the next genuine edit.
            let suppressed_old = self.suppressed.consume_rename_echo(&old_path);
            let suppressed_new = self.suppressed.consume_rename_echo(&new_path);
            if suppressed_old || suppressed_new {
                debug!(?old_path, ?new_path, "suppressed echo rename event");
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
/// the old half of `rename_doc`) is cleared ONLY via `consume_removal_echo`
/// (or `consume_rename_echo` for the rename pair); a write echo (a `Some(hash)`
/// suppression, registered by `write_doc` / the new half of `rename_doc`) is
/// cleared ONLY via `consume_write_echo`. The arms look like duplicated
/// boilerplate, but the divergence is the whole point: routing a removal/rename
/// event through `consume_write_echo` MISSES the daemon's own removal/rename
/// suppressions and **resurrects files mid-rename** — the catastrophic §1.3/§4.2
/// regression (commit `5dd87257`; a single green pass did NOT catch it — only a
/// back-to-back A/B against clean HEAD did). The ONLY arm that may legitimately
/// consult both is the Remove-while-file-exists sub-case below (the recreate it
/// coalesced over). Change nothing else here without that A/B + the expensive lane.
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

    fn pb(s: &str) -> PathBuf {
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

    /// Concurrent renames must pair by cookie, not arrival order. With two
    /// renames in flight, a `To` pairs with the same-cookie `From` even when
    /// that is not the most-recent `From` (arrival-order pairing would mis-pair
    /// and move the wrong document).
    #[test]
    fn test_rename_tracker_pairs_by_cookie() {
        let mut t = RenameTracker::default();
        t.on_from(Some(1), pb("a.md"));
        t.on_from(Some(2), pb("b.md"));

        assert_eq!(
            t.on_to(Some(1), pb("A.md")),
            RenameResolution::Paired {
                old_path: pb("a.md"),
                new_path: pb("A.md")
            }
        );
        assert_eq!(
            t.on_to(Some(2), pb("B.md")),
            RenameResolution::Paired {
                old_path: pb("b.md"),
                new_path: pb("B.md")
            }
        );
    }

    /// Platforms with no cookie fall back to FIFO arrival order.
    #[test]
    fn test_rename_tracker_untracked_fifo_fallback() {
        let mut t = RenameTracker::default();
        t.on_from(None, pb("a.md"));
        t.on_from(None, pb("b.md"));

        assert_eq!(
            t.on_to(None, pb("A.md")),
            RenameResolution::Paired {
                old_path: pb("a.md"),
                new_path: pb("A.md")
            }
        );
    }

    /// A rename-to with no matching from-half is a newly created file.
    #[test]
    fn test_rename_tracker_unmatched_to_is_new_file() {
        let mut t = RenameTracker::default();
        assert_eq!(
            t.on_to(Some(9), pb("new.md")),
            RenameResolution::NewFile {
                new_path: pb("new.md")
            }
        );
    }

    /// An unmatched from-half is carried one extra poll (its `To` may land
    /// in the next interval) before being demoted to a deletion.
    #[test]
    fn test_rename_tracker_carries_unmatched_from_one_poll() {
        let mut t = RenameTracker::default();
        t.on_from(Some(1), pb("a.md"));

        // End of the poll it arrived in: carried, not demoted.
        assert!(t.age_and_take_expired().is_empty());

        // Its matching To arrives in the next poll and still pairs.
        assert_eq!(
            t.on_to(Some(1), pb("A.md")),
            RenameResolution::Paired {
                old_path: pb("a.md"),
                new_path: pb("A.md")
            }
        );
    }

    /// After the carry window, an unmatched from-half is demoted to a deletion.
    #[test]
    fn test_rename_tracker_demotes_after_carry_window() {
        let mut t = RenameTracker::default();
        t.on_from(Some(1), pb("a.md"));

        assert!(t.age_and_take_expired().is_empty()); // carried one poll
        assert_eq!(t.age_and_take_expired(), vec![pb("a.md")]); // then demoted
        assert!(t.age_and_take_expired().is_empty()); // and gone
    }

    /// Rename echoes are consumed by path (both halves) regardless of content.
    #[test]
    fn test_suppression_set_consumes_each_entry_independently() {
        let mut s = SuppressionSet::default();
        s.insert(pb("old.md"), None);
        s.insert(pb("new.md"), Some(b"hi".to_vec()));

        assert!(s.consume_rename_echo(Path::new("old.md")));
        assert!(s.consume_rename_echo(Path::new("new.md")));
        assert!(!s.consume_rename_echo(Path::new("old.md"))); // already consumed
        assert_eq!(s.len(), 0);
    }

    /// A suppression whose echo never arrives expires after its TTL rather
    /// than lingering to swallow a later genuine edit.
    #[test]
    fn test_suppression_set_expires_after_ttl() {
        let mut s = SuppressionSet::default();
        s.insert(pb("x.md"), None);

        // Survives within the TTL window.
        s.tick();
        assert!(s.consume_removal_echo(Path::new("x.md")));

        // A fresh entry expires once TTL polls elapse without being consumed.
        s.insert(pb("y.md"), None);
        for _ in 0..SUPPRESS_TTL_POLLS {
            s.tick();
        }
        assert!(!s.consume_removal_echo(Path::new("y.md")));
        assert_eq!(s.len(), 0);
    }

    /// The core guarantee: a write echo is consumed only when the current
    /// content matches what the daemon wrote. A genuine concurrent edit
    /// (different content) is NOT consumed, so it is never swallowed.
    #[test]
    fn test_write_echo_distinguishes_genuine_edit_by_content() {
        let mut s = SuppressionSet::default();
        // The suppression stores the HASH the daemon wrote, exactly as
        // `write_doc` does in production (raw content would never match).
        s.insert(
            pb("doc.md"),
            Some(crate::blob_state::sha256_bytes(b"daemon-wrote\n")),
        );

        // A genuine concurrent edit (different bytes) is not an echo — emitted.
        assert!(!s.consume_write_echo(Path::new("doc.md"), b"user-edited\n"));
        // The entry survives so the real echo can still be matched later.
        assert_eq!(s.len(), 1);
        // The actual echo (matching raw bytes, hashed internally) is consumed.
        assert!(s.consume_write_echo(Path::new("doc.md"), b"daemon-wrote\n"));
        assert_eq!(s.len(), 0);

        // A removal suppression is not consumed by a write echo (wrong kind).
        s.insert(pb("gone.md"), None);
        assert!(!s.consume_write_echo(Path::new("gone.md"), b"anything"));
    }
}
