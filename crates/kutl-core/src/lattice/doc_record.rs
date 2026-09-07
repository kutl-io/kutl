use uuid::Uuid;

use super::Lattice;
use crate::hlc::Hlc;

/// Tracks a document's existence and path as a per-document join-semilattice.
///
/// Lifecycle events carry hybrid logical clocks ([`Hlc`]) assigned at their
/// origin, not wall-clock timestamps — so merge is causal and reproducible.
/// Each field is an `Option<Hlc>`; `None` is the bottom element ("never"),
/// and `None < Some(_)` gives the sentinel semantics directly.
///
/// Merge semantics: field-by-field max over the HLCs. Path comes from whichever
/// of `registered_hlc`/`renamed_hlc` is latest (an edit touch does NOT move the
/// path); on an HLC tie the lexicographically larger path wins (the final,
/// deterministic tiebreak).
///
/// A rename also carries a `rename_causal_floor` — the `registered_hlc` the
/// renamer had observed for the doc when it renamed. It lets a rename supersede
/// the register even when the renamer's own (un-skewed) `renamed_hlc` fell below
/// a *future* `registered_hlc` written by a clock-skewed registrant: the rename
/// is causally after the register it observed, and the floor records that. See
/// [`Self::path_priority`].
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
    /// Causal floor of the last rename: the `registered_hlc` the renamer had
    /// recorded for this doc when it issued the rename. `None` if the record has
    /// no rename, or the rename came from a pre-floor client. Used by
    /// [`Self::path_priority`] so a rename can supersede a *future*-stamped
    /// registration (a clock-skewed registrant's REGISTER) when the rename is
    /// causally after the register it observed but its own HLC fell below it.
    pub rename_causal_floor: Option<Hlc>,
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

/// The case-fold every path-collision surface shares: full-string
/// [`str::to_lowercase`].
///
/// Arbitration groups intended paths by this fold; the relay's registry
/// projection, occupant capture, and divergence detector fold effective paths
/// with it; the daemon's shadow, identity index, and case-collision probes
/// fold with it; and the storage mirror's `UNIQUE(lower(path))` constraint
/// matches it for the ASCII-dominant paths kutl handles (`PostgreSQL`'s
/// `lower()` and Rust `to_lowercase` agree there). ONE implementation, so no
/// two surfaces can ever disagree about which paths collide.
#[must_use]
pub fn fold_path(path: &str) -> String {
    path.to_lowercase()
}

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

/// Invert [`conflict_path`]: recover `(intended, id)` from a generated
/// conflict path. `dir/foo.kutl-conflict-<id>.md` → (`dir/foo.md`, id);
/// `foo.kutl-conflict-<id>` → (`foo`, id). `None` when the path carries no
/// [`CONFLICT_INFIX`] or the segment after it is not a full UUID — a
/// user-manufactured lookalike, which callers must treat as unsyncable (the
/// namespace is reserved; a register for it is refused).
///
/// Recovery matters for a displaced-before-confirmed local document: its
/// state maps only the conflict DISK path, but registers must carry the
/// INTENDED path (records hold intended paths; conflict paths are per-node
/// derivations) — this inverse is how a restart recovers it.
#[must_use]
pub fn intended_from_conflict_path(path: &str) -> Option<(String, Uuid)> {
    let infix_at = path.rfind(CONFLICT_INFIX)?;
    let prefix = &path[..infix_at];
    let rest = &path[infix_at + CONFLICT_INFIX.len()..];
    let (id_str, ext) = match rest.rfind('.') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let id = Uuid::parse_str(id_str).ok()?;
    Some((format!("{prefix}{ext}"), id))
}

impl DocRecord {
    /// A registration fragment: only `registered_hlc` is set. Merge (`observe`)
    /// takes a per-field max, so the other lifecycle stamps carry forward
    /// unchanged on an existing record. Which `Option` slot a stamp lands in
    /// decides path arbitration — construct fragments through these named
    /// constructors rather than struct literals so the intent is the thing
    /// written.
    #[must_use]
    pub fn register(
        document_id: Uuid,
        path: impl Into<String>,
        registered_hlc: Option<Hlc>,
    ) -> Self {
        Self {
            document_id,
            path: path.into(),
            registered_hlc,
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: None,
            displaced: false,
        }
    }

    /// A rename fragment: `renamed_hlc` plus the causal floor (the
    /// `registered_hlc` the renamer had observed), which lets the rename
    /// supersede a clock-skewed future-stamped register. Argument order is
    /// stamp-then-floor; see [`Self::path_priority`] for why the floor exists.
    #[must_use]
    pub fn rename(
        document_id: Uuid,
        path: impl Into<String>,
        renamed_hlc: Option<Hlc>,
        rename_causal_floor: Option<Hlc>,
    ) -> Self {
        Self {
            document_id,
            path: path.into(),
            registered_hlc: None,
            renamed_hlc,
            rename_causal_floor,
            touched_hlc: None,
            deleted_hlc: None,
            displaced: false,
        }
    }

    /// A tombstone fragment: only `deleted_hlc` is set. `path` is the doc's
    /// current path, carried forward so the tombstone keeps its path-group
    /// membership.
    #[must_use]
    pub fn delete(document_id: Uuid, path: impl Into<String>, deleted_hlc: Option<Hlc>) -> Self {
        Self {
            document_id,
            path: path.into(),
            registered_hlc: None,
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc,
            displaced: false,
        }
    }

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
    ///
    /// A rename supersedes the registration when EITHER its own `renamed_hlc`
    /// dominates the `registered_hlc`, OR its `rename_causal_floor` dominates it.
    /// The floor handles the clock-skew case: a remote REGISTER is folded
    /// watermark-only (its skew never `recv`'d), so a cross-worker rename of a
    /// skewed registrant's doc carries a `renamed_hlc` below the future
    /// `registered_hlc` — but the renamer observed that register before renaming,
    /// so the floor (== the observed register HLC) records the causal order. When
    /// the rename supersedes, the ordering HLC is `max(renamed_hlc, floor)`, so a
    /// floor-superseding rename sorts by the register stamp it observed — high
    /// enough to beat the bare register (rename tiebreaker) but NOT a concurrent
    /// rename by the registrant itself (whose own clock advanced past its
    /// register). The priority is a pure function of the merged record, so every
    /// replica elects the same winner regardless of arrival order — the §7.1
    /// clock-skew determinism guarantee.
    fn path_priority(&self) -> (Option<Hlc>, bool) {
        let rename_supersedes = self.renamed_hlc.is_some()
            && (self.renamed_hlc >= self.registered_hlc
                || self.rename_causal_floor >= self.registered_hlc);
        if rename_supersedes {
            (self.renamed_hlc.max(self.rename_causal_floor), true)
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
    /// what the relay's path lookup, the broadcast, and the daemon reconcile to.
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
            rename_causal_floor: self.rename_causal_floor,
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
    /// Causal floor of the last rename (the observed `registered_hlc`); persisted
    /// so the rename's skew-superseding priority survives a relay restart.
    pub rename_causal_floor: Option<Hlc>,
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
        self.rename_causal_floor = self.rename_causal_floor.max(other.rename_causal_floor);
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
            rename_causal_floor: None,
            touched_hlc: touched,
            deleted_hlc: deleted,
            displaced: false,
        }
    }

    /// Like [`rec`] but with an explicit rename causal floor — for the
    /// clock-skew supersession tests.
    fn rec_floor(
        path: &str,
        registered: Option<Hlc>,
        renamed: Option<Hlc>,
        floor: Option<Hlc>,
    ) -> DocRecord {
        DocRecord {
            document_id: TEST_UUID,
            path: path.into(),
            registered_hlc: registered,
            renamed_hlc: renamed,
            rename_causal_floor: floor,
            touched_hlc: None,
            deleted_hlc: None,
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
        // proptests below must not vary it. `rename_causal_floor` IS varied: it
        // is merge state and participates in path arbitration, so the merge-law
        // and order-independence proptests must exercise it.
        //
        // REALIZABILITY INVARIANT: a floor is set ONLY by a rename op, which also
        // sets `renamed_hlc`; no op ever produces a floor without a rename. The
        // lattice only ever merges such fragments, so a record with
        // `rename_causal_floor.is_some()` always has `renamed_hlc.is_some()` (the
        // merge maxes both, so the merged renamed is >= every fragment's). We
        // generate only realizable records — a detached floor (floor set, rename
        // absent) cannot occur and is excluded so the confluence proptests test
        // the lattice the system actually produces. (A free-floating floor field
        // is NOT confluent, and need not be: it is unreachable.)
        (
            "[a-z]{1,4}",
            arb_opt_hlc(),
            arb_opt_hlc(),
            arb_opt_hlc(),
            arb_opt_hlc(),
            arb_opt_hlc(),
        )
            .prop_map(|(path, r, rn, floor, t, d)| {
                let mut record = rec(&path, r, rn, t, d);
                // Floor only when a rename is present (realizability).
                record.rename_causal_floor = if record.renamed_hlc.is_some() {
                    floor
                } else {
                    None
                };
                record
            })
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

        /// The observable `(path, alive)` is a deterministic function of the
        /// merged HLC fields — merge order cannot change it (`path` is not
        /// itself max-merged, so order independence needs its own proof).
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
    fn test_intended_from_conflict_path_roundtrips() {
        // The inverse must recover exactly what conflict_path consumed, for
        // every shape the derivation produces — this is what lets a restart
        // register a displaced-before-confirmed doc under its INTENDED path.
        let id = TEST_UUID;
        for intended in ["foo.md", "dir/foo.md", "foo", "a/archive.tar.gz", ".env"] {
            let derived = conflict_path(intended, &id);
            assert_eq!(
                intended_from_conflict_path(&derived),
                Some((intended.to_owned(), id)),
                "roundtrip failed for {intended}"
            );
        }
    }

    #[test]
    fn test_intended_from_conflict_path_rejects_lookalikes() {
        // No infix at all, and an infix followed by a non-UUID (a
        // user-manufactured name) must both fail — callers treat those as
        // unsyncable rather than deriving a bogus intended path.
        assert_eq!(intended_from_conflict_path("plain.md"), None);
        assert_eq!(
            intended_from_conflict_path("foo.kutl-conflict-not-a-uuid.md"),
            None
        );
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
    fn test_causal_floor_lost_rename_supersedes_skewed_register() {
        // The lost-rename bug: worker A (skew +5s) registers `draft.md` at a
        // FUTURE hlc (5000ms). Un-skewed worker B renames it to `archive.md`;
        // B's own renamed_hlc (10ms, un-skewed) falls BELOW the register, but B
        // observed the register before renaming so it carries the register hlc as
        // its causal floor. The rename must supersede the register's path.
        let register = h(5000, 0, 1); // A, skewed
        let rename = h(10, 0, 2); // B, un-skewed, below the register
        let mut r = rec_floor("draft.md", Some(register), Some(rename), Some(register));
        r.path = "archive.md".into();
        // Path arbitration within the single doc's merge: the rename supersedes.
        let (key, is_rename) = r.path_priority();
        assert!(
            is_rename,
            "floor lets the rename supersede the skewed register"
        );
        assert_eq!(
            key,
            Some(register),
            "ordering key is max(renamed_hlc, floor) = the register stamp"
        );

        // And it survives a merge with the bare register fragment (no floor),
        // in BOTH orders — the rename path wins.
        let reg_fragment = rec("draft.md", Some(register), None, None, None);
        let mut a = r.clone();
        a.merge(&reg_fragment);
        assert_eq!(
            a.path, "archive.md",
            "rename path wins after merge (order A)"
        );
        let mut b = reg_fragment.clone();
        b.merge(&r);
        assert_eq!(
            b.path, "archive.md",
            "rename path wins after merge (order B)"
        );
    }

    #[test]
    fn test_causal_floor_seven_one_mirror_skewed_registrant_wins() {
        // §7.1 mirror: A (+5s) and B (0) both rename the SAME doc that A
        // registered (future hlc). A's rename uses A's own advanced clock; B's
        // rename is un-skewed but carries A's register hlc as its floor. A's
        // concurrent rename must still win, reproducibly — the floor lets B beat
        // the *register*, not A's rename. (This is exactly what the reverted
        // daemon-side Hlc::successor lift broke.)
        let register = h(5000, 0, 1); // A's skewed register
        // A's own rename: its clock advanced since its own register (same ms,
        // logical+1). Supersedes via the normal branch; ordering key = renamed.
        let a_rename = rec_floor(
            "from_a.md",
            Some(register),
            Some(h(5000, 1, 1)),
            Some(register),
        );
        let (a_key, a_is_rename) = a_rename.path_priority();
        assert!(a_is_rename);
        assert_eq!(a_key, Some(h(5000, 1, 1)), "A sorts by its own rename hlc");

        // B's rename: un-skewed (below register), floor == register. Supersedes
        // via the floor; ordering key = max(B, register) = the register stamp.
        let b_rename = rec_floor(
            "from_b.md",
            Some(register),
            Some(h(20, 0, 2)),
            Some(register),
        );
        let (b_key, b_is_rename) = b_rename.path_priority();
        assert!(b_is_rename, "B supersedes the register via the floor");
        assert_eq!(
            b_key,
            Some(register),
            "B sorts by the register stamp it observed, NOT a lifted B-clock"
        );

        // A's ordering key STRICTLY dominates B's: (5000,1,A) > (5000,0,A).
        assert!(a_key > b_key, "A's concurrent rename dominates B's");

        // The full lattice agrees, order-independently: A's path wins both ways.
        let mut ab = a_rename.clone();
        ab.merge(&b_rename);
        let mut ba = b_rename.clone();
        ba.merge(&a_rename);
        assert_eq!(ab.path, "from_a.md", "A wins (merge order A⊕B)");
        assert_eq!(ba.path, "from_a.md", "A wins (merge order B⊕A)");
        assert_eq!(ab.path, ba.path, "confluent");
    }

    #[test]
    fn test_causal_floor_stale_rename_does_not_beat_revival_register() {
        // Stale-rename-vs-revival: an old rename observed an OLDER register
        // (floor = 5). The doc is later re-registered (revival) at a HIGHER hlc
        // (20). The stale rename's floor (5) does NOT dominate the new register
        // (20), and its own renamed_hlc (10) also does not — so the revival's
        // register path must win.
        let stale_rename = rec_floor(
            "old_name.md",
            Some(h(5, 0, 1)),  // the register the rename observed
            Some(h(10, 0, 1)), // the rename itself
            Some(h(5, 0, 1)),  // floor = observed register
        );
        let revival = rec("revived_name.md", Some(h(20, 0, 2)), None, None, None);

        // Merge: the higher register hlc (20) dominates the stale rename (10) and
        // its floor (5), so the rename does NOT supersede → revival path wins.
        let mut a = stale_rename.clone();
        a.merge(&revival);
        assert_eq!(a.path, "revived_name.md", "revival register wins (order A)");
        let mut b = revival.clone();
        b.merge(&stale_rename);
        assert_eq!(b.path, "revived_name.md", "revival register wins (order B)");
    }

    #[test]
    fn test_causal_floor_three_op_confluence_register_two_renames() {
        // Confluence over all orderings of {register, A-rename, B-rename} (the
        // §7.1 cluster). Every permutation must converge to A's path.
        let register_frag = rec("draft.md", Some(h(5000, 0, 1)), None, None, None);
        let a_rename = {
            let mut r = rec_floor(
                "draft.md",
                Some(h(5000, 0, 1)),
                Some(h(5000, 1, 1)),
                Some(h(5000, 0, 1)),
            );
            r.path = "from_a.md".into();
            r
        };
        let b_rename = {
            let mut r = rec_floor(
                "draft.md",
                Some(h(5000, 0, 1)),
                Some(h(20, 0, 2)),
                Some(h(5000, 0, 1)),
            );
            r.path = "from_b.md".into();
            r
        };

        let frags = [register_frag, a_rename, b_rename];
        let perms: [[usize; 3]; 6] = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for perm in perms {
            let mut acc = frags[perm[0]].clone();
            acc.merge(&frags[perm[1]]);
            acc.merge(&frags[perm[2]]);
            assert_eq!(
                acc.path, "from_a.md",
                "permutation {perm:?} must converge to A's path"
            );
        }
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
