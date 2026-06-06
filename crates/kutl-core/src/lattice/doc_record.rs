use uuid::Uuid;

use super::Lattice;
use crate::hlc::Hlc;

/// Tracks a document's existence and path as a per-document join-semilattice.
///
/// Lifecycle events carry hybrid logical clocks ([`Hlc`]) assigned at their
/// origin, not wall-clock timestamps — so merge is causal and reproducible
/// (see the lattice-lifecycle design note). Each field is an `Option<Hlc>`;
/// `None` is the bottom element ("never"), and `None < Some(_)` gives the
/// sentinel semantics directly.
///
/// Merge semantics: field-by-field max over the HLCs. Path comes from whichever
/// of `registered_hlc`/`renamed_hlc` is latest (an edit touch does NOT move the
/// path); on an HLC tie the lexicographically larger path wins (the final,
/// deterministic tiebreak).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocRecord {
    /// Document UUID.
    pub document_id: Uuid,
    /// Current document path. Determined by the latest of registered/renamed.
    pub path: String,
    /// When the document was registered. `None` if unknown.
    pub registered_hlc: Option<Hlc>,
    /// When the document was last renamed. `None` if never renamed.
    pub renamed_hlc: Option<Hlc>,
    /// When the document's content was last edited (a lifecycle "touch").
    /// `None` if never touched. Used for the edit-revives rule: an edit whose
    /// HLC exceeds `deleted_hlc` makes the document alive again. Does not affect
    /// path selection — an edit is not a rename.
    pub touched_hlc: Option<Hlc>,
    /// When the document was deleted. `None` if never deleted.
    pub deleted_hlc: Option<Hlc>,
    /// Conflict-copy marker: set when this document lost a path-arbitration to a
    /// higher-priority claimant of its intended path. This is **not** lattice
    /// merge state — [`Lattice::merge`] ignores it. It is a pure projection of
    /// the record set, recomputed from scratch by
    /// [`RegistryLattice::arbitrate`](super::RegistryLattice) after every merge:
    /// among all records sharing an intended path, the max `(path_hlc, id)` owns
    /// it and the rest are displaced. A *deleted* winner keeps its claim (it is
    /// still the max at that path), so an already-displaced loser does not
    /// reclaim a path freed only by the winner's deletion (no auto-reclaim, v1);
    /// a winner that *renames away* leaves the contended path, so the loser
    /// reclaims it (this is what makes arbitration confluent — order-independent).
    /// The `path` field keeps the *intended* path; [`Self::effective_path`]
    /// resolves to the conflict path while displaced.
    pub displaced: bool,
}

/// The reserved infix that marks a generated conflict path. User-chosen paths
/// must be rejected at the naming boundary if they contain it (so a user file
/// can't shadow a generated conflict path); this module asserts the invariant
/// locally via [`conflict_path`].
pub const CONFLICT_INFIX: &str = ".kutl-conflict-";

/// Derive the deterministic conflict path for a displaced document: the full id
/// in a reserved namespace, preserving directory and extension.
/// `dir/foo.md` → `dir/foo.kutl-conflict-<id>.md`; `foo` (no ext) →
/// `foo.kutl-conflict-<id>`. A pure function of `(intended, id)`, so every node
/// computes the same path (confluent).
///
/// Collision-free **across distinct document ids** (the embedded full id is the
/// document's own UUID). This assumes `intended` does not itself embed
/// [`CONFLICT_INFIX`] — enforced at the naming boundary (path validation) and
/// asserted here in debug. `intended` is also assumed to be a validated relative
/// path (no empty / `.` / `..` segments); those are rejected upstream by
/// `SafeRelayPath`.
#[must_use]
pub fn conflict_path(intended: &str, id: &Uuid) -> String {
    debug_assert!(
        !intended.contains(CONFLICT_INFIX),
        "conflict_path called on an already-conflicted path (would double-apply): {intended}"
    );
    let (dir, name) = match intended.rfind('/') {
        Some(i) => (&intended[..=i], &intended[i + 1..]),
        None => ("", intended),
    };
    // Split on the last dot, but treat a leading dot (dotfile) as part of the
    // stem, not an extension.
    let (stem, ext) = match name.rfind('.') {
        Some(i) if i > 0 => (&name[..i], &name[i..]),
        _ => (name, ""),
    };
    format!("{dir}{stem}.kutl-conflict-{id}{ext}")
}

impl DocRecord {
    /// The latest alive-asserting event (registration, rename, or edit touch).
    ///
    /// `None` only for a degenerate record with no asserting event at all.
    #[must_use]
    pub fn liveness_hlc(&self) -> Option<Hlc> {
        self.registered_hlc
            .max(self.renamed_hlc)
            .max(self.touched_hlc)
    }

    /// The HLC governing the current path (latest of registration/rename). The
    /// election key (with `document_id`) for path arbitration across documents.
    #[must_use]
    pub fn path_hlc(&self) -> Option<Hlc> {
        self.registered_hlc.max(self.renamed_hlc)
    }

    /// Path-selection priority within a single document's merge: the governing
    /// HLC plus a rename-beats-register tiebreaker. A rename is causally after
    /// the registration of the same document, so at an *equal* HLC the rename's
    /// path wins. This only bites the no-origin-HLC fallback, where sequential
    /// same-millisecond ops would otherwise tie; real origin HLCs never tie.
    fn path_priority(&self) -> (Option<Hlc>, bool) {
        if self.renamed_hlc.is_some() && self.renamed_hlc >= self.registered_hlc {
            (self.renamed_hlc, true)
        } else {
            (self.registered_hlc, false)
        }
    }

    /// Whether the document is alive: never deleted, or revived by a
    /// registration / rename / edit-touch after the deletion.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.deleted_hlc.is_none() || self.liveness_hlc() > self.deleted_hlc
    }

    /// The current intended path (what the user named it), regardless of
    /// displacement. See [`Self::effective_path`] for where the file actually
    /// resolves.
    #[must_use]
    pub fn current_path(&self) -> &str {
        &self.path
    }

    /// The path the file actually resolves to: the intended path, or — while
    /// displaced by a path-arbitration loss — the derived conflict path. This is
    /// what `path_index`, the broadcast, and the daemon reconcile to.
    #[must_use]
    pub fn effective_path(&self) -> String {
        if self.displaced {
            conflict_path(&self.path, &self.document_id)
        } else {
            self.path.clone()
        }
    }

    /// The externally-observable lifecycle projection — where the document
    /// resolves and its lifecycle stamps, with displacement and revival already
    /// applied. The single source of the projection rules so callers don't
    /// re-implement them field by field.
    #[must_use]
    pub fn project(&self) -> LifecycleProjection {
        LifecycleProjection {
            effective_path: self.effective_path(),
            registered_hlc: self.registered_hlc,
            renamed_hlc: self.renamed_hlc,
            touched_hlc: self.touched_hlc,
            // The deletion stamp is observable only while the document is
            // *actually* dead; a revived document (rename/edit after the delete)
            // projects `None` — it is alive again.
            deleted_hlc: if self.is_alive() {
                None
            } else {
                self.deleted_hlc
            },
        }
    }
}

/// The observable lifecycle of a [`DocRecord`], with displacement and revival
/// resolved. Returned by [`DocRecord::project`]; consumers (the relay registry's
/// `RegistryEntry` projection, the daemon reconciler) read this rather than the
/// raw fields so the rules live in one place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleProjection {
    /// The path the document resolves to (its conflict path while displaced).
    pub effective_path: String,
    /// Registration stamp.
    pub registered_hlc: Option<Hlc>,
    /// Last-rename stamp.
    pub renamed_hlc: Option<Hlc>,
    /// Last edit-touch stamp.
    pub touched_hlc: Option<Hlc>,
    /// Deletion stamp, present only while the document is dead (revival → `None`).
    pub deleted_hlc: Option<Hlc>,
}

impl Lattice for DocRecord {
    fn merge(&mut self, other: &Self) {
        debug_assert_eq!(
            self.document_id, other.document_id,
            "cannot merge DocRecords with different document_ids"
        );

        // Path governed by the latest registration/rename on each side, with a
        // rename-beats-register tiebreaker at equal HLC.
        let self_pp = self.path_priority();
        let other_pp = other.path_priority();

        // Fields: join (max) per HLC field.
        self.registered_hlc = self.registered_hlc.max(other.registered_hlc);
        self.renamed_hlc = self.renamed_hlc.max(other.renamed_hlc);
        self.touched_hlc = self.touched_hlc.max(other.touched_hlc);
        self.deleted_hlc = self.deleted_hlc.max(other.deleted_hlc);

        // `displaced` is intentionally NOT merged: it is a pure projection of the
        // whole record set, recomputed by `RegistryLattice::arbitrate` after the
        // merge. Folding it in here (as a monotonic OR) made arbitration
        // non-confluent — a loser stayed displaced under one merge order but not
        // another once the winner renamed away. See the `displaced` field doc.

        // Path: from the side with the later path HLC. On an exact HLC tie the
        // lexicographically larger path wins — the final deterministic tiebreak
        // (HLC stamps are unique per op, so ties arise only between equal
        // merged maxima).
        if other_pp > self_pp || (other_pp == self_pp && other.path > self.path) {
            self.path.clone_from(&other.path);
        }
    }

    fn lattice_le(&self, other: &Self) -> bool {
        // `displaced` is excluded: it is not merge state (a derived projection
        // computed by `RegistryLattice::arbitrate`), so it has no place in the
        // merge lattice's partial order.
        self.registered_hlc <= other.registered_hlc
            && self.renamed_hlc <= other.renamed_hlc
            && self.touched_hlc <= other.touched_hlc
            && self.deleted_hlc <= other.deleted_hlc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::ActorId;
    use proptest::prelude::*;

    /// Fixed UUID for property tests (identity doesn't affect merge).
    const TEST_UUID: Uuid = Uuid::from_bytes([
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ]);

    fn actor(n: u8) -> ActorId {
        let mut b = [0u8; 16];
        b[15] = n;
        ActorId(Uuid::from_bytes(b))
    }

    /// Concise HLC builder for tests.
    fn h(physical_ms: u64, logical: u32, a: u8) -> Hlc {
        Hlc {
            physical_ms,
            logical,
            actor: actor(a),
        }
    }

    fn rec(
        path: &str,
        registered: Option<Hlc>,
        renamed: Option<Hlc>,
        touched: Option<Hlc>,
        deleted: Option<Hlc>,
    ) -> DocRecord {
        DocRecord {
            document_id: TEST_UUID,
            path: path.into(),
            registered_hlc: registered,
            renamed_hlc: renamed,
            touched_hlc: touched,
            deleted_hlc: deleted,
            displaced: false,
        }
    }

    fn arb_opt_hlc() -> impl Strategy<Value = Option<Hlc>> {
        prop_oneof![
            Just(None),
            (0u64..6, 0u32..3, 0u8..3).prop_map(|(p, l, a)| Some(h(p, l, a))),
        ]
    }

    fn arb_doc_record() -> impl Strategy<Value = DocRecord> {
        // `displaced` is deliberately not generated: it is not merge state (a
        // derived projection of `RegistryLattice::arbitrate`), so the merge-law
        // proptests below must not vary it.
        (
            "[a-z]{1,4}",
            arb_opt_hlc(),
            arb_opt_hlc(),
            arb_opt_hlc(),
            arb_opt_hlc(),
        )
            .prop_map(|(path, r, rn, t, d)| rec(&path, r, rn, t, d))
    }

    proptest! {
        #[test]
        fn commutativity(a in arb_doc_record(), b in arb_doc_record()) {
            let mut ab = a.clone(); ab.merge(&b);
            let mut ba = b.clone(); ba.merge(&a);
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn associativity(a in arb_doc_record(), b in arb_doc_record(), c in arb_doc_record()) {
            let mut ab_c = a.clone(); ab_c.merge(&b); ab_c.merge(&c);
            let mut a_bc = a.clone();
            let mut bc = b.clone(); bc.merge(&c);
            a_bc.merge(&bc);
            prop_assert_eq!(ab_c, a_bc);
        }

        #[test]
        fn idempotency(a in arb_doc_record()) {
            let mut aa = a.clone(); aa.merge(&a);
            prop_assert_eq!(a, aa);
        }

        #[test]
        fn monotonic_growth(a in arb_doc_record(), b in arb_doc_record()) {
            let mut merged = a.clone(); merged.merge(&b);
            prop_assert!(a.lattice_le(&merged));
            prop_assert!(b.lattice_le(&merged));
        }

        /// The observable `(path, alive)` is a deterministic function of the
        /// merged HLC fields — merge order cannot change it (the proof
        /// obligation that `lattice_le` excluding `path` left open).
        #[test]
        fn observable_is_merge_order_independent(
            a in arb_doc_record(), b in arb_doc_record()
        ) {
            let mut ab = a.clone(); ab.merge(&b);
            let mut ba = b.clone(); ba.merge(&a);
            // The *effective* path (incl. displacement) and aliveness are
            // deterministic functions of the merged state, order-independent.
            prop_assert_eq!(ab.effective_path(), ba.effective_path());
            prop_assert_eq!(ab.is_alive(), ba.is_alive());
            prop_assert_eq!(ab.displaced, ba.displaced);
        }
    }

    #[test]
    fn test_merge_does_not_touch_displaced() {
        // `displaced` is a derived projection (RegistryLattice::arbitrate), not
        // merge state: merge must leave it exactly as it found it on `self`,
        // regardless of the other side. (Folding it in as a monotonic OR was the
        // §7.3 confluence bug.)
        let mut a = rec("foo", Some(h(1, 0, 1)), None, None, None);
        let mut b = a.clone();
        b.displaced = true;
        a.merge(&b);
        assert!(
            !a.displaced,
            "merge does not import the other side's displaced"
        );

        let mut c = rec("foo", Some(h(1, 0, 1)), None, None, None);
        c.displaced = true;
        let clean = rec("foo", Some(h(1, 0, 1)), None, None, None);
        c.merge(&clean);
        assert!(c.displaced, "merge does not clear self's displaced either");
    }

    #[test]
    fn test_effective_path_resolves_to_conflict_when_displaced() {
        let mut r = rec("notes/foo.md", Some(h(1, 0, 1)), None, None, None);
        assert_eq!(r.effective_path(), "notes/foo.md");
        r.displaced = true;
        assert_eq!(
            r.effective_path(),
            format!("notes/foo.kutl-conflict-{TEST_UUID}.md"),
            "displaced → conflict path: dir + stem preserved, full id, ext kept"
        );
    }

    #[test]
    fn test_conflict_path_variants() {
        let id = TEST_UUID;
        assert_eq!(
            conflict_path("foo.md", &id),
            format!("foo.kutl-conflict-{id}.md")
        );
        assert_eq!(
            conflict_path("a/b/foo.md", &id),
            format!("a/b/foo.kutl-conflict-{id}.md")
        );
        assert_eq!(conflict_path("foo", &id), format!("foo.kutl-conflict-{id}"));
        assert_eq!(
            conflict_path(".hidden", &id),
            format!(".hidden.kutl-conflict-{id}")
        );
        // Multi-dot: split on the LAST dot (the real extension), so the compound
        // extension's earlier dots stay in the stem.
        assert_eq!(
            conflict_path("a/archive.tar.gz", &id),
            format!("a/archive.tar.kutl-conflict-{id}.gz")
        );
    }

    #[test]
    #[should_panic(expected = "would double-apply")]
    fn test_conflict_path_rejects_already_conflicted_in_debug() {
        // The naming boundary must keep CONFLICT_INFIX out of intended paths;
        // this asserts the invariant locally so a double-apply can't pass silently.
        let _ = conflict_path(&conflict_path("foo.md", &TEST_UUID), &TEST_UUID);
    }

    #[test]
    fn test_alive_when_never_deleted() {
        assert!(rec("foo", Some(h(10, 0, 1)), None, None, None).is_alive());
    }

    #[test]
    fn test_dead_when_deleted_after_register() {
        assert!(!rec("foo", Some(h(5, 0, 1)), None, None, Some(h(10, 0, 1))).is_alive());
    }

    #[test]
    fn test_revived_by_rename_after_delete() {
        assert!(
            rec(
                "bar",
                Some(h(5, 0, 1)),
                Some(h(15, 0, 1)),
                None,
                Some(h(10, 0, 1))
            )
            .is_alive()
        );
    }

    #[test]
    fn test_revived_by_reregister_after_delete() {
        assert!(rec("foo", Some(h(15, 0, 1)), None, None, Some(h(10, 0, 1))).is_alive());
    }

    #[test]
    fn test_project_applies_revival_displacement_and_tombstone() {
        // Alive: tombstone projects None, effective path = intended.
        let alive = rec("foo.md", Some(h(1, 0, 1)), None, None, None);
        let p = alive.project();
        assert_eq!(p.effective_path, "foo.md");
        assert_eq!(p.deleted_hlc, None);

        // Dead: tombstone projects the deletion stamp.
        let dead = rec("foo.md", Some(h(1, 0, 1)), None, None, Some(h(5, 0, 1)));
        assert_eq!(dead.project().deleted_hlc, Some(h(5, 0, 1)));

        // Revived (rename after delete): alive again, tombstone projects None.
        let revived = rec(
            "bar.md",
            Some(h(1, 0, 1)),
            Some(h(9, 0, 1)),
            None,
            Some(h(5, 0, 1)),
        );
        assert_eq!(revived.project().deleted_hlc, None);

        // Displaced: effective path is the conflict path.
        let mut displaced = rec("foo.md", Some(h(1, 0, 1)), None, None, None);
        displaced.displaced = true;
        assert_eq!(
            displaced.project().effective_path,
            conflict_path("foo.md", &TEST_UUID)
        );
    }

    #[test]
    fn test_revived_by_edit_touch_after_delete() {
        // The edit-revives decision: a content touch whose HLC exceeds the
        // deletion makes the document alive again, even with no rename.
        assert!(
            rec(
                "foo",
                Some(h(5, 0, 1)),
                None,
                Some(h(20, 0, 1)),
                Some(h(10, 0, 1))
            )
            .is_alive()
        );
    }

    #[test]
    fn test_edit_touch_before_delete_does_not_revive() {
        assert!(
            !rec(
                "foo",
                Some(h(5, 0, 1)),
                None,
                Some(h(8, 0, 1)),
                Some(h(10, 0, 1))
            )
            .is_alive()
        );
    }

    #[test]
    fn test_touch_does_not_move_path() {
        // An edit touch with the highest HLC must not change the path: only
        // registration/rename select the path.
        let mut a = rec("original", Some(h(5, 0, 1)), None, None, None);
        let b = rec("ignored", None, None, Some(h(99, 0, 1)), None);
        a.merge(&b);
        assert_eq!(a.path, "original");
    }

    #[test]
    fn test_merge_path_from_later_rename() {
        let mut a = rec("original", Some(h(5, 0, 1)), None, None, None);
        let b = rec("renamed", Some(h(3, 0, 1)), Some(h(10, 0, 1)), None, None);
        a.merge(&b);
        assert_eq!(a.path, "renamed");
    }

    #[test]
    fn test_merge_path_from_later_registration() {
        let mut a = rec("old", Some(h(3, 0, 1)), None, None, None);
        let b = rec("new", Some(h(10, 0, 1)), None, None, None);
        a.merge(&b);
        assert_eq!(a.path, "new");
    }

    #[test]
    fn test_rename_beats_register_at_equal_hlc() {
        // Register and rename of the same doc with the SAME hlc (the no-origin-
        // HLC same-millisecond case): the rename wins (causally after), not the
        // lexicographic path tiebreak.
        let mut registered = rec("draft.md", Some(h(5, 0, 1)), None, None, None);
        let renamed = rec("archive/draft.md", None, Some(h(5, 0, 1)), None, None);
        registered.merge(&renamed);
        assert_eq!(
            registered.path, "archive/draft.md",
            "rename beats register at equal hlc"
        );
        // Order-independent: merge the other way.
        let registered_b = rec("draft.md", Some(h(5, 0, 1)), None, None, None);
        let mut renamed_b = rec("archive/draft.md", None, Some(h(5, 0, 1)), None, None);
        renamed_b.merge(&registered_b);
        assert_eq!(renamed_b.path, "archive/draft.md");
    }

    #[test]
    fn test_concurrent_rename_lww_by_hlc() {
        // Two renames of the same doc to different names; higher HLC wins,
        // deterministically, regardless of merge order.
        let mut a = rec("foo", Some(h(1, 0, 1)), Some(h(5, 0, 1)), None, None);
        let b = rec("foo", Some(h(1, 0, 1)), Some(h(7, 0, 2)), None, None);
        a.path = "bar_a".into();
        let mut b2 = b.clone();
        b2.path = "bar_b".into();
        let mut ab = a.clone();
        ab.path = "bar_a".into();
        ab.merge(&b2);
        assert_eq!(ab.path, "bar_b", "higher renamed_hlc (7>5) wins the path");
    }
}
