//! `Occupant` + `DiskShadow`: the in-memory disk model for the sans-IO core.
//!
//! `DiskShadow` is the in-memory twin of the space directory that the gamma
//! cascade (Phase 4) reads instead of live disk. It is `SpaceState::file_identity`
//! re-keyed into the two lookup directions the cascade needs,
//! plus the genuinely new `Occupant::Untracked` that models the FS-1/2 race.
//!
//! Updated only on shell-ACK of a successful disk effect (graft 3); never on
//! emit. No `std::fs`, no `.await`, no clock reads.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::core::rel_path_to_string;

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
    /// race. A desired placement onto an `Untracked` path defers (unless revival,
    /// graft 2). Replaces the bare `.exists()` probe of the former
    /// `path_occupied_by_other`.
    Untracked,
}

/// In-memory mirror of on-disk identity, in the directions the cascade reads.
///
/// Updated only on shell-ACK of success (graft 3); never on emit. Provides O(1)
/// answers to the cascade question that would otherwise need O(files) disk I/O:
/// - the former `path_occupied_by_other` → `shadow_occupant.get(casefold(rel))`
#[derive(Debug, Default)]
pub struct DiskShadow {
    /// id → current on-disk relative path (re-key of `SpaceState::file_identity`
    /// values). The cascade reads this to find where a doc lives now.
    pub shadow_path: HashMap<Uuid, PathBuf>,
    /// case-folded path → occupant. The O(1) replacement for the former
    /// `path_occupied_by_other`. Keyed by `casefold(rel)` so
    /// lookups agree with `RegistryLattice::arbitrate`'s `.to_lowercase()`
    /// (`registry.rs:92`).
    pub shadow_occupant: HashMap<String, Occupant>,
    /// path → recorded inode (the `FileIdentity::inode`, re-keyed for conform).
    /// A path with no captured inode (e.g. the platform doesn't expose one)
    /// simply has no entry — no writer ever records an absent inode. The sim's
    /// missing-source conform (`DaemonSim::apply_guarded_place`) reads this to
    /// recover a doc's recorded inode; the real driver uses `file_identity`
    /// for the same purpose.
    pub shadow_path_inode: HashMap<PathBuf, u64>,
}

impl DiskShadow {
    /// Record that document `id` is now on disk at `rel`.
    ///
    /// Inserts:
    /// - `shadow_path[id] = rel`
    /// - `shadow_occupant[casefold(rel)] = Tracked(id)`
    ///
    /// Does NOT update `shadow_path_inode` — call [`set_inode`] separately once the
    /// post-op inode is known (graft 3).
    ///
    /// Pure: no IO.
    pub fn set_tracked(&mut self, rel: &Path, id: Uuid) {
        self.shadow_path.insert(id, rel.to_path_buf());
        self.shadow_occupant
            .insert(casefold(rel), Occupant::Tracked(id));
    }

    /// Record the inode for the file at `rel`.
    ///
    /// Inserts `shadow_path_inode[rel] = Some(inode)` — the path→inode direction
    /// the missing-source conform reads to recover a doc's recorded inode.
    ///
    /// Pure: no IO.
    pub fn set_inode(&mut self, inode: u64, rel: &Path) {
        self.shadow_path_inode.insert(rel.to_path_buf(), inode);
    }

    /// Vacate the shadow for a COMPLETED removal of `rel`: drop the occupant
    /// (tracked or untracked), the path→inode entry, and — when a `Tracked`
    /// occupant still pointed here — its `shadow_path` placement. A doc whose
    /// `shadow_path` already moved elsewhere keeps its current placement (the
    /// removal of a stale old path must not unplace it).
    ///
    /// The removal twin of [`Self::rename_fold`]; call only AFTER the disk
    /// removal succeeds or the file is confirmed absent (graft 3: fold on
    /// shell-ACK, never on emit). Pure: no IO.
    pub fn remove_fold(&mut self, rel: &Path) {
        if let Some(Occupant::Tracked(uid)) = self.shadow_occupant.remove(&casefold(rel))
            && self.shadow_path.get(&uid).is_some_and(|p| p == rel)
        {
            self.shadow_path.remove(&uid);
        }
        self.shadow_path_inode.remove(rel);
    }

    /// Re-key the shadow for a COMPLETED rename `old`→`new`: vacate `old` and
    /// occupy `new` (with `id` when it resolves to a real UUID), moving the inode
    /// mapping over.
    ///
    /// The single shadow transition for a successful rename, shared by the pure
    /// core's [`crate::core::DaemonCore::apply_effect_result`] (`RenameApplied`)
    /// AND the imperative cascade's disk-move reconcilers. The imperative path
    /// moves the file with the free `rename_doc` helper, which only touches
    /// `file_identity` — without this fold the shadow keeps recording the OLD
    /// path, so a later remote edit's `should_skip_remote_write` sees the file
    /// ABSENT at `new` (shadow stale) plus a known inode and silently SKIPS the
    /// disk write, stranding the observer at pre-edit content (the moved-then-
    /// edited divergence). Call only AFTER the disk move succeeds (graft 3:
    /// fold on shell-ACK of success, never on emit).
    ///
    /// Pure: no IO.
    pub fn rename_fold(&mut self, old: &Path, new: &Path, id: Option<Uuid>, inode: Option<u64>) {
        self.shadow_occupant.remove(&casefold(old));
        self.shadow_path_inode.remove(old);
        if let Some(id) = id {
            self.set_tracked(new, id);
        }
        if let Some(inode) = inode {
            self.set_inode(inode, new);
        }
    }
}

/// The TWIN deferral predicate, shared across the IO boundary (stage 2 of the
/// graft-1 agreement): is a desired target held by a FOREIGN untracked
/// occupant? The precedence lives here ONCE — an identity claim at the target
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

/// Case-fold a relative path to a string key, matching `RegistryLattice::arbitrate`'s
/// `.to_lowercase()` (`registry.rs:92`), so path grouping in the cascade agrees
/// with the relay by construction.
///
/// Uses `rel_path_to_string` for cross-platform separator normalisation, then
/// lowercases to match the relay's case-fold.
pub(crate) fn casefold(p: &Path) -> String {
    rel_path_to_string(p).to_lowercase()
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
        // Replaces the former path_occupied_by_other's `.exists()` branch:
        // an untracked file on disk is "occupied".
        let mut s = DiskShadow::default();
        s.shadow_occupant.insert("a.md".into(), Occupant::Untracked);
        assert_eq!(s.shadow_occupant.get("a.md"), Some(&Occupant::Untracked));
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
        // casefold must agree with RegistryLattice::arbitrate's .to_lowercase()
        // (registry.rs:92) and normalise path separators cross-platform.
        let p = PathBuf::from("Notes/Sub/A.md");
        assert_eq!(casefold(&p), "notes/sub/a.md");
    }

    #[test]
    fn test_rename_fold_vacates_old_and_occupies_new() {
        // The regression guard for the moved-then-edited divergence: after a
        // rename fold, the shadow must report the file PRESENT at the new path
        // (so `should_skip_remote_write`'s `file_on_disk` is true) and ABSENT at
        // the old path, with the recorded inode re-keyed onto the new path.
        let mut s = DiskShadow::default();
        s.set_tracked(&PathBuf::from("a.md"), uid(1));
        s.set_inode(7, &PathBuf::from("a.md"));

        s.rename_fold(
            &PathBuf::from("a.md"),
            &PathBuf::from("archive/a.md"),
            Some(uid(1)),
            Some(7),
        );

        // Old path vacated in BOTH occupant and path-inode maps.
        assert_eq!(s.shadow_occupant.get("a.md"), None);
        assert_eq!(s.shadow_path_inode.get(&PathBuf::from("a.md")), None);
        // New path occupied by the same doc, present on disk for the write check,
        // with its recorded inode re-keyed onto the new path.
        assert_eq!(
            s.shadow_occupant.get("archive/a.md"),
            Some(&Occupant::Tracked(uid(1)))
        );
        assert_eq!(
            s.shadow_path.get(&uid(1)),
            Some(&PathBuf::from("archive/a.md"))
        );
        assert_eq!(
            s.shadow_path_inode.get(&PathBuf::from("archive/a.md")),
            Some(&7)
        );
    }

    /// The removal fold vacates all three maps — occupant, path→inode, and the
    /// tracked doc's placement — so a later register at the path sees it
    /// genuinely free (or genuinely held by a NEW untracked file) instead of
    /// the deleted incumbent. The §4.2 guard: a stale `Tracked` occupant after
    /// a delete makes `stat_untracked` mistake a foreign recreate for the
    /// incumbent and place-adopt it instead of deferring.
    #[test]
    fn test_remove_fold_vacates_occupant_inode_and_placement() {
        let mut s = DiskShadow::default();
        s.set_tracked(&PathBuf::from("doc.md"), uid(1));
        s.set_inode(7, &PathBuf::from("doc.md"));

        s.remove_fold(&PathBuf::from("doc.md"));

        assert_eq!(s.shadow_occupant.get("doc.md"), None);
        assert_eq!(s.shadow_path_inode.get(&PathBuf::from("doc.md")), None);
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

    #[test]
    fn test_set_inode_records_path_inode_mapping() {
        // shadow_path_inode maps the path back to its inode for conform decisions.
        let mut s = DiskShadow::default();
        s.set_inode(42, &PathBuf::from("doc.md"));
        assert_eq!(s.shadow_path_inode.get(&PathBuf::from("doc.md")), Some(&42));
    }
}
