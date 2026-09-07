//! `Occupant` + `DiskShadow`: the in-memory disk model for the sans-IO core.
//!
//! `DiskShadow` is the in-memory twin of the space directory that the
//! placement cascade reads instead of live disk. It is
//! `SpaceState::file_identity` re-keyed into the two lookup directions the
//! cascade needs, plus `Occupant::Untracked`, which models a file on disk that
//! no document has been arbitrated onto yet.
//!
//! Updated only on shell-ACK of a successful disk effect; never on emit. No
//! `std::fs`, no `.await`, no clock reads.

use super::casefold;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use uuid::Uuid;

/// Who holds a case-folded path in the daemon's in-memory disk model.
///
/// Keyed by `casefold(rel)` in `DiskShadow::shadow_occupant`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Occupant {
    /// A document this daemon tracks holds the path.
    ///
    /// The `Uuid` is the document identity that `shadow_path` is keyed on. A
    /// desired placement onto a `Tracked` path is safe — the cascade can move
    /// the incumbent if needed.
    Tracked(Uuid),
    /// A file on disk that no document has been arbitrated onto yet — the FS-1/2
    /// race. A desired placement onto an `Untracked` path defers (unless the
    /// document is a revival).
    Untracked,
}

/// In-memory mirror of on-disk identity, in the directions the cascade reads.
///
/// Updated only on shell-ACK of success; never on emit. Answers the cascade's
/// "who holds this path" question in O(1) instead of O(files) disk I/O.
#[derive(Debug, Default)]
pub struct DiskShadow {
    /// id → current on-disk relative path (re-key of `SpaceState::file_identity`
    /// values). The cascade reads this to find where a doc lives now.
    pub shadow_path: HashMap<Uuid, PathBuf>,
    /// case-folded path → occupant. Keyed by `casefold(rel)`, which folds
    /// through `kutl_core::lattice::fold_path`, the same rule the relay's
    /// arbitration groups paths by, so the two agree about which paths collide.
    pub shadow_occupant: HashMap<String, Occupant>,
}

impl DiskShadow {
    /// Whether any occupant (tracked or untracked) holds `rel`: the in-core
    /// stand-in for a live `exists()` probe. Pure: no IO.
    #[must_use]
    pub fn occupied(&self, rel: &Path) -> bool {
        self.shadow_occupant.contains_key(&casefold(rel))
    }

    /// Record that document `id` is now on disk at `rel`.
    ///
    /// Inserts:
    /// - `shadow_path[id] = rel`
    /// - `shadow_occupant[casefold(rel)] = Tracked(id)`
    ///
    /// The recorded inode lives in `FileIdentity::inode`, not here.
    ///
    /// Pure: no IO.
    pub fn set_tracked(&mut self, rel: &Path, id: Uuid) {
        self.shadow_path.insert(id, rel.to_path_buf());
        self.shadow_occupant
            .insert(casefold(rel), Occupant::Tracked(id));
    }

    /// Vacate the shadow for a COMPLETED removal of `rel`: drop the occupant
    /// (tracked or untracked) and — when a `Tracked`
    /// occupant still pointed here — its `shadow_path` placement. A doc whose
    /// `shadow_path` already moved elsewhere keeps its current placement (the
    /// removal of a stale old path must not unplace it).
    ///
    /// The removal twin of [`Self::rename_fold`]; call only AFTER the disk
    /// removal succeeds or the file is confirmed absent (fold on shell-ACK,
    /// never on emit). Pure: no IO.
    pub fn remove_fold(&mut self, rel: &Path) {
        if let Some(Occupant::Tracked(uid)) = self.shadow_occupant.remove(&casefold(rel))
            && self.shadow_path.get(&uid).is_some_and(|p| p == rel)
        {
            self.shadow_path.remove(&uid);
        }
    }

    /// Re-key the shadow for a COMPLETED rename `old`→`new`: vacate `old` and
    /// occupy `new` (with `id` when it resolves to a real UUID).
    ///
    /// The single shadow transition for a successful rename, shared by the pure
    /// core's [`crate::core::DaemonCore::apply_effect_result`] (`RenameApplied`)
    /// AND the imperative cascade's disk-move reconcilers. The imperative path
    /// moves the file with the free `rename_doc` helper, which only touches
    /// `file_identity` — without this fold the shadow keeps recording the OLD
    /// path, so a later remote edit's `should_skip_remote_write` sees the file
    /// ABSENT at `new` (shadow stale) plus a known inode and silently SKIPS the
    /// disk write, stranding the observer at pre-edit content (the moved-then-
    /// edited divergence). Call only AFTER the disk move succeeds (fold on
    /// shell-ACK of success, never on emit).
    ///
    /// Pure: no IO.
    pub fn rename_fold(&mut self, old: &Path, new: &Path, id: Option<Uuid>) {
        self.shadow_occupant.remove(&casefold(old));
        if let Some(id) = id {
            self.set_tracked(new, id);
        }
    }
}

/// The TWIN deferral predicate, shared across the IO boundary: is a desired
/// target held by a FOREIGN untracked occupant? The precedence lives here ONCE — an identity claim at the target
/// beats any untracked evidence, and a `Tracked` occupant never reads as
/// foreign (the cascade may move a tracked incumbent; it must never clobber an
/// untracked one).
///
/// Each caller supplies only its own DISK EVIDENCE for `file_present_evidence`:
/// the core's `reconcile_placement` passes its shadow belief (an
/// [`Occupant::Untracked`] marker is the only disk the pure core can see); the
/// shell's `stat_untracked` passes its live atomic stat. That difference IS the
/// deliberate TOCTOU asymmetry — the shell sees occupants the shadow has not
/// folded — and is pinned by `test_twin_deferral_predicate_equivalence`.
#[must_use]
pub fn held_by_foreign_untracked(
    occupant: Option<&Occupant>,
    file_present_evidence: bool,
    identity_claimed: bool,
) -> bool {
    file_present_evidence && !identity_claimed && !matches!(occupant, Some(Occupant::Tracked(_)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn uid(n: u8) -> Uuid {
        let mut b = [0u8; 16];
        b[15] = n;
        Uuid::from_bytes(b)
    }

    #[test]
    fn test_untracked_path_is_occupied_by_other() {
        // An untracked file on disk is "occupied".
        let mut s = DiskShadow::default();
        s.shadow_occupant.insert("a.md".into(), Occupant::Untracked);
        assert_eq!(s.shadow_occupant.get("a.md"), Some(&Occupant::Untracked));
        assert!(s.occupied(&PathBuf::from("a.md")));
    }

    #[test]
    fn test_tracked_path_occupant_is_keyed_case_folded() {
        // shadow_occupant is keyed by case-folded path (matches arbitrate, registry.rs:92).
        let mut s = DiskShadow::default();
        s.set_tracked(&PathBuf::from("Notes/A.md"), uid(1));
        assert_eq!(
            s.shadow_occupant.get("notes/a.md"),
            Some(&Occupant::Tracked(uid(1)))
        );
        assert_eq!(
            s.shadow_path.get(&uid(1)),
            Some(&PathBuf::from("Notes/A.md"))
        );
    }

    #[test]
    fn test_casefold_normalises_separators_and_lowercases() {
        // casefold must agree with the arbitration's `fold_path` rule and
        // normalise path separators cross-platform.
        let p = PathBuf::from("Notes/Sub/A.md");
        assert_eq!(casefold(&p), "notes/sub/a.md");
    }

    #[test]
    fn test_rename_fold_vacates_old_and_occupies_new() {
        // The regression guard for the moved-then-edited divergence: after a
        // rename fold, the shadow must report the file PRESENT at the new path
        // (so `should_skip_remote_write`'s `file_on_disk` is true) and ABSENT at
        // the old path.
        let mut s = DiskShadow::default();
        s.set_tracked(&PathBuf::from("a.md"), uid(1));

        s.rename_fold(
            &PathBuf::from("a.md"),
            &PathBuf::from("archive/a.md"),
            Some(uid(1)),
        );

        // Old path vacated.
        assert_eq!(s.shadow_occupant.get("a.md"), None);
        // New path occupied by the same doc, present on disk for the write check.
        assert_eq!(
            s.shadow_occupant.get("archive/a.md"),
            Some(&Occupant::Tracked(uid(1)))
        );
        assert_eq!(
            s.shadow_path.get(&uid(1)),
            Some(&PathBuf::from("archive/a.md"))
        );
    }

    /// The removal fold vacates both maps — occupant and the tracked doc's
    /// placement — so a later register at the path sees it
    /// genuinely free (or genuinely held by a NEW untracked file) instead of
    /// the deleted incumbent: a stale `Tracked` occupant after a delete makes
    /// `stat_untracked` mistake a foreign recreate for the incumbent and
    /// place-adopt it instead of deferring.
    #[test]
    fn test_remove_fold_vacates_occupant_and_placement() {
        let mut s = DiskShadow::default();
        s.set_tracked(&PathBuf::from("doc.md"), uid(1));

        s.remove_fold(&PathBuf::from("doc.md"));

        assert_eq!(s.shadow_occupant.get("doc.md"), None);
        assert_eq!(s.shadow_path.get(&uid(1)), None);
    }

    /// Removing a doc's STALE old path must not unplace a doc that already
    /// moved elsewhere: only the occupant whose `shadow_path` still points at
    /// the removed path loses its placement.
    #[test]
    fn test_remove_fold_keeps_moved_doc_placement() {
        let mut s = DiskShadow::default();
        s.set_tracked(&PathBuf::from("a.md"), uid(1));
        // The doc moved on (shadow_path now b.md) but a stale occupant entry
        // for a.md lingers (a degraded fold sequence).
        s.shadow_path.insert(uid(1), PathBuf::from("b.md"));

        s.remove_fold(&PathBuf::from("a.md"));

        assert_eq!(s.shadow_occupant.get("a.md"), None);
        assert_eq!(
            s.shadow_path.get(&uid(1)),
            Some(&PathBuf::from("b.md")),
            "a doc placed elsewhere keeps its placement"
        );
    }
}
