//! Echo suppression for the file watcher: the daemon registers its own
//! writes, removals, and rename pairs here so the resulting watcher events
//! are recognised as echoes and dropped, never re-synced as user edits.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Poll cycles an echo-suppression entry lives before expiring. Bounds the
/// window in which a stranded suppression (whose echo event never arrived)
/// could swallow a later genuine edit, and prevents unbounded growth.
const SUPPRESS_TTL_POLLS: u8 = 5;

/// An echo-suppression command from the daemon.
///
/// `Single` is a write (`Some(hash)`) or removal (`None`) the daemon
/// performed at one path. `RenamePair` is a daemon rename registered as ONE
/// pair: the old half doubles as a removal echo and the new half as a write
/// echo when the OS delivers the move as uncorrelated singles, but the OS
/// rename-PAIR consumer matches only this exact pair — an unrelated single
/// entry that happens to share a path with a USER rename can no longer
/// swallow it (pair-matched suppression).
#[derive(Debug)]
pub enum Suppression {
    /// One write (`Some(content hash)`) or removal (`None`) at `path`.
    Single(PathBuf, Option<Vec<u8>>),
    /// A daemon rename `old`→`new`; `hash` is the moved content's hash
    /// (`None` when unreadable at rename time).
    RenamePair {
        old: PathBuf,
        new: PathBuf,
        hash: Option<Vec<u8>>,
    },
}

/// One registered echo-suppression entry.
struct SuppressionEntry {
    /// The content hash the daemon wrote (`None` for a removal — and for a
    /// rename's OLD half, whose uncorrelated-single echo is a `Removed`).
    payload: Option<Vec<u8>>,
    /// `Some(other)` marks a rename HALF and names its partner path. Only the
    /// OS rename-PAIR consumer reads this: it consumes iff the event's two
    /// paths are exactly a registered pair. The payload-matching consumers
    /// (`consume_write_echo`/`consume_removal_echo`) ignore it, so a daemon
    /// rename delivered as uncorrelated singles still suppresses by kind.
    pair: Option<PathBuf>,
    /// Poll cycles left before expiry.
    ttl: u8,
}

/// Echo-suppression set with a per-entry TTL.
///
/// The daemon registers a path here just before writing it, so the resulting
/// watcher event is recognised as its own echo and dropped. Writes carry the
/// content hash they produced: a write echo is then distinguished from a genuine
/// concurrent edit *by content*, so the suppression cannot silently swallow a
/// real edit even when the two coalesce into one poll-cycle event. A TTL bounds
/// the set's growth if an echo never arrives. Rename suppressions are
/// PAIR-MATCHED: the daemon registers both halves as one linked pair, and the
/// OS rename-pair consumer fires only for that exact pair — never for an
/// unrelated single entry sharing a path with a user rename.
#[derive(Default)]
pub struct SuppressionSet {
    entries: HashMap<PathBuf, SuppressionEntry>,
}

impl SuppressionSet {
    /// Register `path` for single-event echo suppression. `expected` is the
    /// content hash the daemon wrote, or `None` for a removal.
    pub fn insert(&mut self, path: PathBuf, expected: Option<Vec<u8>>) {
        self.entries.insert(
            path,
            SuppressionEntry {
                payload: expected,
                pair: None,
                ttl: SUPPRESS_TTL_POLLS,
            },
        );
    }

    /// Register a daemon rename `old`→`new` as ONE linked pair: the old half
    /// carries the removal payload (`None`), the new half the write payload —
    /// so an uncorrelated-singles delivery still suppresses by kind — and each
    /// names the other so the pair consumer matches exactly this rename.
    pub fn insert_rename_pair(&mut self, old: PathBuf, new: PathBuf, hash: Option<Vec<u8>>) {
        self.entries.insert(
            old.clone(),
            SuppressionEntry {
                payload: None,
                pair: Some(new.clone()),
                ttl: SUPPRESS_TTL_POLLS,
            },
        );
        self.entries.insert(
            new,
            SuppressionEntry {
                payload: hash,
                pair: Some(old),
                ttl: SUPPRESS_TTL_POLLS,
            },
        );
    }

    /// Consume a rename-PAIR echo: fires iff a registered rename pair links
    /// exactly `old`↔`new` (either half suffices — the other may already have
    /// been consumed by an uncorrelated single delivery), removing ONLY the
    /// halves that belong to this pair. An unrelated `Single` entry sharing
    /// `old` or `new` is left alone and cannot swallow a USER rename — the
    /// pair-matched closure of the suppression-collision class. Returns
    /// whether the event was the daemon's own rename echo.
    pub fn consume_rename_pair_echo(&mut self, old: &Path, new: &Path) -> bool {
        let old_is_half = self
            .entries
            .get(old)
            .is_some_and(|e| e.pair.as_deref() == Some(new));
        let new_is_half = self
            .entries
            .get(new)
            .is_some_and(|e| e.pair.as_deref() == Some(old));
        if old_is_half {
            self.entries.remove(old);
        }
        if new_is_half {
            self.entries.remove(new);
        }
        old_is_half || new_is_half
    }

    /// Consume a write echo: a Create/Modify event whose current content matches
    /// what the daemon wrote. A genuine edit (different content) is NOT consumed,
    /// so it is never swallowed. Returns whether the event was an echo.
    ///
    /// `current` is the raw on-disk bytes; the suppression stores the SHA-256
    /// hash the daemon wrote (see [`write_doc`]), so we hash `current` here and
    /// compare hash-to-hash. (Comparing the stored hash against raw bytes would
    /// never match — the echo would slip through to the edit handler.)
    /// Pair-agnostic by design: a rename's NEW half delivered as an
    /// uncorrelated Create/Modify single is still its write echo.
    pub fn consume_write_echo(&mut self, path: &Path, current: &[u8]) -> bool {
        let current_hash = crate::blob_state::sha256_bytes(current);
        match self.entries.get(path) {
            Some(SuppressionEntry {
                payload: Some(expected),
                ..
            }) if expected.as_slice() == current_hash.as_slice() => {
                self.entries.remove(path);
                true
            }
            _ => false,
        }
    }

    /// Whether a write is registered at `path`, so an event there could be
    /// its echo. The caller reads and hashes the file only when this holds:
    /// a user's own write has no entry, and reading a large file back to
    /// learn that is the cost this check avoids.
    pub fn expects_write(&self, path: &Path) -> bool {
        matches!(
            self.entries.get(path),
            Some(SuppressionEntry {
                payload: Some(_),
                ..
            })
        )
    }

    /// Consume a removal echo: a `Removed` event for a path the daemon deleted
    /// (registered with no content). Pair-agnostic by design: a rename's OLD
    /// half delivered as an uncorrelated `Removed` single is still its removal
    /// echo. Returns whether the event was an echo.
    pub fn consume_removal_echo(&mut self, path: &Path) -> bool {
        match self.entries.get(path) {
            Some(SuppressionEntry { payload: None, .. }) => {
                self.entries.remove(path);
                true
            }
            _ => false,
        }
    }

    /// Expire entries whose TTL has elapsed. Call once per poll.
    pub fn tick(&mut self) {
        self.entries.retain(|_, e| {
            e.ttl = e.ttl.saturating_sub(1);
            e.ttl > 0
        });
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watcher::tests::pb;

    /// A registered rename PAIR is consumed by the pair consumer (both halves
    /// leave together), and a second consume is a no-op.
    #[test]
    fn test_rename_pair_suppressed_and_cleared() {
        let mut s = SuppressionSet::default();
        s.insert_rename_pair(pb("old.md"), pb("new.md"), Some(b"hi".to_vec()));

        assert!(s.consume_rename_pair_echo(Path::new("old.md"), Path::new("new.md")));
        assert_eq!(s.len(), 0);
        assert!(!s.consume_rename_pair_echo(Path::new("old.md"), Path::new("new.md")));
    }

    /// THE pair-matched closure (the §1.3 suppression-collision class): a USER
    /// rename pair sharing a path with an UNRELATED daemon write/removal entry
    /// is NOT swallowed, and the unrelated entry survives for its own echo.
    #[test]
    fn test_user_rename_not_swallowed_by_unrelated_single_entry() {
        let mut s = SuppressionSet::default();
        // The daemon wrote new.md (a remote edit landing) — a Single entry.
        s.insert(
            pb("new.md"),
            Some(crate::blob_state::sha256_bytes(b"daemon-wrote\n")),
        );
        // The user renames old.md -> new.md concurrently: NOT our pair.
        assert!(
            !s.consume_rename_pair_echo(Path::new("old.md"), Path::new("new.md")),
            "an unrelated write entry must not suppress a user rename"
        );
        // The write entry survives for the daemon's own echo.
        assert!(s.consume_write_echo(Path::new("new.md"), b"daemon-wrote\n"));
        // Same for a removal entry at the OLD path.
        s.insert(pb("gone.md"), None);
        assert!(!s.consume_rename_pair_echo(Path::new("gone.md"), Path::new("dst.md")));
        assert!(s.consume_removal_echo(Path::new("gone.md")));
    }

    /// A daemon rename delivered as UNCORRELATED singles still suppresses by
    /// kind: the old half as a removal echo, the new half as a write echo —
    /// and a half already consumed that way doesn't stop the pair consumer
    /// from matching on the surviving half.
    #[test]
    fn test_rename_halves_consumable_as_singles() {
        let mut s = SuppressionSet::default();
        s.insert_rename_pair(
            pb("old.md"),
            pb("new.md"),
            Some(crate::blob_state::sha256_bytes(b"body")),
        );
        assert!(s.consume_removal_echo(Path::new("old.md")));
        assert!(s.consume_write_echo(Path::new("new.md"), b"body"));
        assert_eq!(s.len(), 0);

        // Partial: the Removed single consumed the old half first; the OS pair
        // still matches via the surviving new half.
        s.insert_rename_pair(pb("a.md"), pb("b.md"), None);
        assert!(s.consume_removal_echo(Path::new("a.md")));
        assert!(s.consume_rename_pair_echo(Path::new("a.md"), Path::new("b.md")));
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

    /// `expects_write` answers from the entry alone: a registered write says
    /// yes, a removal entry and an unregistered path say no.
    #[test]
    fn test_expects_write_reads_the_entry_kind() {
        let mut s = SuppressionSet::default();
        assert!(!s.expects_write(Path::new("doc.md")));
        s.insert(pb("doc.md"), Some(crate::blob_state::sha256_bytes(b"x")));
        s.insert(pb("gone.md"), None);
        assert!(s.expects_write(Path::new("doc.md")));
        assert!(!s.expects_write(Path::new("gone.md")));
    }
}
