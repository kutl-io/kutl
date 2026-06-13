//! Rename-half correlation for the file watcher: pairs rename-from /
//! rename-to event halves by platform cookie across poll cycles.

use std::collections::HashMap;
use std::path::PathBuf;

/// Extra poll cycles an unmatched rename-from half waits for its matching
/// rename-to before being demoted to a deletion. On Linux the two halves of a
/// rename can land in adjacent poll intervals, so dropping an unmatched `From`
/// at the end of its first poll would degrade a real rename into delete+create
/// (losing document identity).
const RENAME_PAIR_CARRY_POLLS: u8 = 1;

/// Resolution of a rename-to event half.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RenameResolution {
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
pub(super) struct RenameTracker {
    /// Cookie-keyed pending from-halves: `tracker -> (old_path, polls_waited)`.
    tracked: HashMap<usize, (PathBuf, u8)>,
    /// Fallback for platforms that supply no cookie, in arrival order.
    untracked: Vec<(PathBuf, u8)>,
}

impl RenameTracker {
    /// Record a rename-from half. `tracker` is the platform rename cookie, if any.
    pub(super) fn on_from(&mut self, tracker: Option<usize>, old_path: PathBuf) {
        match tracker {
            Some(t) => {
                self.tracked.insert(t, (old_path, 0));
            }
            None => self.untracked.push((old_path, 0)),
        }
    }

    /// Resolve a rename-to half against the pending from-halves.
    pub(super) fn on_to(&mut self, tracker: Option<usize>, new_path: PathBuf) -> RenameResolution {
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
    pub(super) fn age_and_take_expired(&mut self) -> Vec<PathBuf> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watcher::tests::pb;

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
}
