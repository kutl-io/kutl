//! File watcher with debouncing and echo suppression.
//!
//! Uses `notify` to watch a space directory for file changes. Includes
//! manual debouncing and suppression of events caused by the daemon's own writes.

use std::collections::{HashMap, HashSet};
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
// `.kutlspace` (RFD 0060 system files), `.DS_Store`, and any future hidden
// files. See RFD 0060 for the rationale: kutl spaces are for documents, and
// the subfolder convention makes the visible-name denylist vestigial.

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
    /// Paths currently suppressed (daemon's own writes).
    suppressed: HashSet<PathBuf>,
    /// Raw notify watcher handle (must stay alive).
    _watcher: RecommendedWatcher,
    /// Buffer for raw events from notify callback (shared with callback thread).
    raw_buffer: Arc<Mutex<Vec<notify::Event>>>,
    /// Sends debounced events to the daemon.
    event_tx: mpsc::Sender<FileEvent>,
    /// Receives suppression commands from the daemon.
    suppress_rx: mpsc::Receiver<PathBuf>,
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
        suppress_rx: mpsc::Receiver<PathBuf>,
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
            suppressed: HashSet::new(),
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
        // Buffered rename-from paths, waiting for a matching rename-to.
        let mut pending_rename_from: Vec<PathBuf> = Vec::new();
        // Completed rename pairs detected during this poll interval.
        let mut rename_pairs: Vec<(PathBuf, PathBuf)> = Vec::new();

        let mut poll_interval = tokio::time::interval(DEBOUNCE_DELAY);
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            poll_interval.tick().await;

            // Drain suppression commands.
            while let Ok(path) = self.suppress_rx.try_recv() {
                self.suppressed.insert(path);
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
                        for path in &event.paths {
                            if let Some(rel) = self.to_relative(path)
                                && !Self::should_ignore(&rel)
                            {
                                pending_rename_from.push(rel);
                            }
                        }
                    }
                    // Second half of a rename pair.
                    EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                        for path in &event.paths {
                            if let Some(rel) = self.to_relative(path) {
                                if Self::should_ignore(&rel) {
                                    continue;
                                }
                                if let Some(old) = pending_rename_from.pop() {
                                    rename_pairs.push((old, rel));
                                } else {
                                    // Unmatched rename-to: treat as new file.
                                    pending.insert(rel, EventKind::Modify(ModifyKind::Any));
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

            // Unmatched rename-from paths: treat as deletions.
            for old in pending_rename_from.drain(..) {
                pending.insert(old, EventKind::Remove(notify::event::RemoveKind::Any));
            }

            // Flush pending events.
            self.flush_pending(&mut pending, &mut rename_pairs).await;
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
            if self.suppressed.remove(&old_path) || self.suppressed.remove(&new_path) {
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
            if self.suppressed.remove(&rel_path) {
                debug!(?rel_path, "suppressed echo event");
                continue;
            }

            let file_event = match kind {
                EventKind::Remove(_) => FileEvent::Removed {
                    rel_path: rel_path.clone(),
                },
                EventKind::Create(_) | EventKind::Modify(_) => FileEvent::Modified {
                    rel_path: rel_path.clone(),
                },
                _ => continue,
            };

            if self.event_tx.send(file_event).await.is_err() {
                warn!("file event channel closed");
                return;
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
}
