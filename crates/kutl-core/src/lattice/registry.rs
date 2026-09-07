//! The registry as a whole is a join-semilattice — not just each [`DocRecord`].
//!
//! A map `document_id → DocRecord` is a pointwise lattice, but the *path
//! projection* (`path → id`) is single-valued only with cross-id coordination.
//! [`RegistryLattice`] provides it: [`observe`](RegistryLattice::observe) merges
//! one record by id, then runs **path arbitration** — for each set of records
//! sharing a (case-folded) intended path, the highest-priority one
//! `(path_hlc, document_id)` keeps the path and the rest are marked
//! [`DocRecord::displaced`] so they resolve to their own loser-only conflict
//! paths. Displacement is a pure function of the record set, **not** a
//! monotonic flag: a merge re-elects only the (at most two) path groups it
//! touched, and the result equals a from-scratch recompute, so it is
//! deterministic and confluent: a record set yields the same canonical
//! holders and the same displacements on every node, in any merge order.

use std::collections::{BTreeSet, HashMap};

use uuid::Uuid;

use super::{DocRecord, Lattice, fold_path};
use crate::Hlc;

/// The lifecycle lattice for one space: a map of per-document [`DocRecord`]s with
/// cross-document path arbitration applied after every merge.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RegistryLattice {
    records: HashMap<Uuid, DocRecord>,
    /// Case-folded intended path → every record (alive AND tombstoned) whose
    /// [`DocRecord::current_path`] folds to it — arbitration's grouping, held
    /// as an index so [`Self::observe`] re-elects only the (at most two)
    /// groups the merge touched instead of recomputing every group. A pure
    /// function of `records` (`BTreeSet` keeps it order-independent, so the
    /// derived `PartialEq`/`Clone` stay sound). Every mutation that can
    /// change a record's path goes through [`Self::observe`] /
    /// [`Self::observe_all`] / [`Self::restore_records`], which keep it in
    /// step; [`Self::touch_in_place`] changes no path and leaves it alone.
    by_path: HashMap<String, BTreeSet<Uuid>>,
}

impl RegistryLattice {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge `incoming` into the record for its id (insert if new), then
    /// re-run path arbitration. Total — never rejects; idempotent and
    /// commutative over the record set. Returns the merged record, so a caller
    /// can read what won (e.g. whether its op set the winning path) without a
    /// re-lookup.
    ///
    /// Arbitration here is INCREMENTAL: a merge mutates exactly one record, so
    /// only the path groups whose membership or member stamps it touched — the
    /// record's group before the merge and its group after — can elect
    /// differently; every other group's inputs are byte-identical to the last
    /// pass. Re-electing just those two groups is therefore equal to the
    /// from-scratch recompute (same [`Self::elect`] kernel), without the
    /// O(all-records) walk per observe that made a bulk burst quadratic.
    pub fn observe(&mut self, incoming: DocRecord) -> &DocRecord {
        let id = incoming.document_id;
        self.merge_and_elect(incoming, None);
        self.records.get(&id).expect("just inserted or merged")
    }

    /// As [`Self::observe`], additionally returning every id whose election
    /// this merge could have touched: the members of the record's path group
    /// before the merge and after (the merged id included). A small SUPERSET
    /// of the ids whose observable projection actually changed — callers diff
    /// against their own previous state — so a projection layer can update
    /// per-affected-id instead of walking every record per op.
    pub fn observe_affected(&mut self, incoming: DocRecord) -> (Vec<Uuid>, &DocRecord) {
        let id = incoming.document_id;
        let mut affected = Vec::new();
        self.merge_and_elect(incoming, Some(&mut affected));
        (
            affected,
            self.records.get(&id).expect("just inserted or merged"),
        )
    }

    /// Merge one record and re-elect the path groups it touched. `affected`
    /// collects the members of those groups when a caller wants them
    /// ([`Self::observe_affected`]); the plain [`Self::observe`] passes
    /// `None` and allocates nothing.
    fn merge_and_elect(&mut self, incoming: DocRecord, mut affected: Option<&mut Vec<Uuid>>) {
        let id = incoming.document_id;
        let old_group = self.records.get(&id).map(|r| fold_path(r.current_path()));
        self.records
            .entry(id)
            .and_modify(|r| r.merge(&incoming))
            .or_insert(incoming);
        let new_group = fold_path(
            self.records
                .get(&id)
                .expect("just inserted or merged")
                .current_path(),
        );
        if old_group.as_deref() != Some(new_group.as_str()) {
            if let Some(old) = old_group {
                self.drop_from_group(&old, id);
                // Losing a member can hand the old path to a new winner.
                self.elect(&old);
                if let (Some(out), Some(members)) =
                    (affected.as_deref_mut(), self.by_path.get(&old))
                {
                    out.extend(members.iter().copied());
                }
            }
            self.by_path
                .entry(new_group.clone())
                .or_default()
                .insert(id);
        }
        // Always re-elect the record's (possibly unchanged) group: even a
        // same-path merge can raise its `path_hlc` and flip the election.
        self.elect(&new_group);
        if let (Some(out), Some(members)) = (affected, self.by_path.get(&new_group)) {
            out.extend(members.iter().copied());
        }
    }

    /// Merge many records, then arbitrate **once** at the end — O(n) instead of
    /// the O(n²) of calling [`observe`](Self::observe) per record (each of which
    /// re-arbitrates the whole growing set). For bulk loads (e.g. rehydrating a
    /// space's persisted registry) where only the final projection matters.
    pub fn observe_all(&mut self, incoming: impl IntoIterator<Item = DocRecord>) {
        for record in incoming {
            self.records
                .entry(record.document_id)
                .and_modify(|r| r.merge(&record))
                .or_insert(record);
        }
        self.arbitrate();
    }

    /// Path arbitration, recomputed from scratch as a pure function of the record
    /// set (so it is order-independent — confluent).
    ///
    /// Group **every** record (alive and tombstoned) by its current *intended*
    /// (case-folded) path; in each group the max `(path_hlc, document_id)` owns
    /// the path (`displaced = false`) and the rest are displaced. Recomputing —
    /// rather than only ever setting `displaced` — is what makes it confluent: a
    /// loser's displacement is a function of who else currently claims its path,
    /// not of the order events arrived.
    ///
    /// Two decided behaviors fall out of *which* records are grouped and *by what
    /// path*:
    /// - **No auto-reclaim after a delete (v1).** A tombstoned winner is still
    ///   grouped (it keeps its intended path), so it remains the max and an
    ///   already-displaced loser stays displaced — the canonical path is not
    ///   reclaimed just because the winner was deleted.
    /// - **Reclaim after a rename-away.** A winner that renamed away now has a
    ///   *different* intended path, so it leaves the contended group; the loser
    ///   becomes the sole (or new max) claimant and reclaims the path. This is
    ///   precisely the case monotonic displacement got wrong (non-confluent).
    ///
    /// One pass suffices: a displaced loser resolves to its conflict path (which
    /// embeds its full id and cannot collide), creating no new contention.
    ///
    /// This is the from-scratch form — rebuild the path-group index, then run
    /// the shared [`Self::elect`] kernel over every group. The bulk paths
    /// ([`Self::observe_all`], [`Self::restore_records`]) use it;
    /// [`Self::observe`] re-elects incrementally with the SAME kernel, so the
    /// two forms cannot disagree on any group they both visit.
    fn arbitrate(&mut self) {
        self.rebuild_path_index();
        let groups: Vec<String> = self.by_path.keys().cloned().collect();
        for group in &groups {
            self.elect(group);
        }
    }

    /// Rebuild [`Self::by_path`] from scratch — the bulk-path sibling of the
    /// per-observe maintenance.
    fn rebuild_path_index(&mut self) {
        self.by_path.clear();
        for (id, r) in &self.records {
            self.by_path
                .entry(fold_path(r.current_path()))
                .or_default()
                .insert(*id);
        }
    }

    /// Remove `id` from a path group, dropping the group when it empties (an
    /// empty group left behind would elect nobody but also never be cleaned).
    fn drop_from_group(&mut self, group: &str, id: Uuid) {
        if let Some(members) = self.by_path.get_mut(group) {
            members.remove(&id);
            if members.is_empty() {
                self.by_path.remove(group);
            }
        }
    }

    /// Re-run the election within ONE casefolded-path group: the max
    /// `(path_hlc, document_id)` member owns the path (`displaced = false`)
    /// and the rest are displaced. The single election kernel — full and
    /// incremental arbitration both call it. A group absent from the index is
    /// a no-op.
    fn elect(&mut self, group: &str) {
        let Some(members) = self.by_path.get(group) else {
            return;
        };
        // A group exists only while it has a member (`drop_from_group`
        // removes an emptied one), so the election always has a winner.
        let winner = members
            .iter()
            .copied()
            .max_by(|a, b| {
                let (ra, rb) = (&self.records[a], &self.records[b]);
                (ra.path_hlc(), *a).cmp(&(rb.path_hlc(), *b))
            })
            .expect("group is non-empty");
        for id in members {
            self.records
                .get_mut(id)
                .expect("id came from the record set")
                .displaced = *id != winner;
        }
    }

    /// The record for `id`, if present (alive or tombstoned).
    #[must_use]
    pub fn get(&self, id: &Uuid) -> Option<&DocRecord> {
        self.records.get(id)
    }

    /// Raise an existing record's `touched_hlc` in place (max-merge the stamp)
    /// WITHOUT re-running arbitration. Returns whether the record existed.
    ///
    /// Safe to skip arbitration because a touch raises only `touched_hlc`, which
    /// feeds `is_alive`/`liveness_hlc` but NOT `path_hlc` (= max(registered,
    /// renamed)) — the election, every record's `displaced` flag, and every
    /// effective path are unchanged. The caller MUST guard that the record is
    /// alive and undisplaced: a reviving (dead→alive) touch changes
    /// `effective_path_index` membership and must go through [`observe`](Self::observe).
    pub fn touch_in_place(&mut self, id: &Uuid, hlc: Hlc) -> bool {
        match self.records.get_mut(id) {
            Some(r) => {
                // The caller's precondition, checked in debug/test builds (the
                // guard itself lives a crate away, in `DocumentRegistry::touch`):
                // an in-place touch is only sound on an alive, undisplaced record.
                // A reviving (dead→alive) or displaced touch changes
                // effective-path-index membership and MUST go through `observe`.
                debug_assert!(
                    r.is_alive() && !r.displaced,
                    "touch_in_place requires an alive, undisplaced record; a reviving \
                     or displaced touch must go through observe (arbitration)"
                );
                r.touched_hlc = r.touched_hlc.max(Some(hlc));
                true
            }
            None => false,
        }
    }

    /// Directly set (or, with `None`, remove) the records for the given ids,
    /// bypassing merge. **Only for capture-restore rollback** — restoring the
    /// snapshot taken before an `observe` whose persistence then failed. Not
    /// a lattice operation; never use it to apply a change.
    ///
    /// Runs ONE full re-arbitration afterwards, for the whole set: a restore
    /// rewrites records' flags outside the election kernel, and with
    /// incremental observes there is no from-scratch pass on the next merge
    /// to absorb the discrepancy, so the rollback path pays the O(records)
    /// recompute once so the hot path never has to.
    pub fn restore_records(&mut self, prior: impl IntoIterator<Item = (Uuid, Option<DocRecord>)>) {
        for (id, record) in prior {
            match record {
                Some(r) => {
                    self.records.insert(id, r);
                }
                None => {
                    self.records.remove(&id);
                }
            }
        }
        self.arbitrate();
    }

    /// The path projection: each alive document's [`DocRecord::effective_path`]
    /// (its conflict path while displaced, else its intended path) → its id. At
    /// most one id per path, by construction.
    ///
    /// The `collect` into a `HashMap` silently keeps the last value on a
    /// duplicate key, so a (hypothetical, arbitration-bug) double-book collapses
    /// here rather than erroring. The relay registry's projection, which
    /// claims each alive record's path one at a time and counts a claim onto
    /// a held path, is the authoritative fail-loud detector for that
    /// invariant. `arbitrate` keeps it quiet (proptest
    /// `at_most_one_canonical_holder_per_path`).
    #[must_use]
    pub fn effective_path_index(&self) -> HashMap<String, Uuid> {
        self.records
            .iter()
            .filter(|(_, r)| r.is_alive())
            .map(|(id, r)| (r.effective_path(), *id))
            .collect()
    }

    /// Iterate over all records (alive and tombstoned), e.g. for persistence.
    pub fn records(&self) -> impl Iterator<Item = (&Uuid, &DocRecord)> {
        self.records.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{ActorId, Hlc};

    fn actor(n: u8) -> ActorId {
        let mut b = [0u8; 16];
        b[15] = n;
        ActorId(Uuid::from_bytes(b))
    }

    fn h(physical_ms: u64, a: u8) -> Hlc {
        Hlc {
            physical_ms,
            logical: 0,
            actor: actor(a),
        }
    }

    fn id(n: u8) -> Uuid {
        let mut b = [0u8; 16];
        b[0] = n;
        Uuid::from_bytes(b)
    }

    fn registered(document_id: Uuid, path: &str, at: Hlc) -> DocRecord {
        DocRecord::register(document_id, path, Some(at))
    }

    /// The path projection has exactly one id per effective path, always.
    fn assert_paths(reg: &RegistryLattice, expected: &[(&str, Uuid)]) {
        let idx = reg.effective_path_index();
        let mut got: Vec<(String, Uuid)> = idx.into_iter().collect();
        got.sort();
        let mut want: Vec<(String, Uuid)> = expected
            .iter()
            .map(|(p, i)| ((*p).to_owned(), *i))
            .collect();
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn test_single_register_holds_its_path() {
        let mut reg = RegistryLattice::new();
        reg.observe(registered(id(1), "foo.md", h(1, 1)));
        assert_paths(&reg, &[("foo.md", id(1))]);
    }

    #[test]
    fn test_touch_in_place_preserves_arbitration() {
        // A touch on an alive, undisplaced record (the relay's reproject-skipping
        // fast path) must change ONLY that record's touched_hlc — never any
        // record's displaced flag or the effective-path index.
        let mut reg = RegistryLattice::new();
        // Two docs collide on "a.md": id(2) (higher HLC) wins, id(1) is displaced.
        reg.observe(registered(id(1), "a.md", h(1000, 1)));
        reg.observe(registered(id(2), "a.md", h(2000, 2)));
        let index_before = reg.effective_path_index();
        let win_displaced = reg.get(&id(2)).unwrap().displaced;
        let lose_displaced = reg.get(&id(1)).unwrap().displaced;

        // Touch the alive, undisplaced winner in place.
        assert!(reg.touch_in_place(&id(2), h(3000, 2)));

        assert_eq!(
            reg.effective_path_index(),
            index_before,
            "a touch leaves the effective-path index unchanged"
        );
        assert_eq!(reg.get(&id(2)).unwrap().displaced, win_displaced);
        assert_eq!(reg.get(&id(1)).unwrap().displaced, lose_displaced);
        assert_eq!(
            reg.get(&id(2)).unwrap().touched_hlc,
            Some(h(3000, 2)),
            "the touch raised the stamp"
        );
    }

    #[test]
    fn test_rename_rename_same_doc_one_name() {
        // One document D renamed to two names concurrently; higher HLC wins, one
        // name everywhere, no duplicate. (Journey F rename/rename.)
        let mut a = registered(id(1), "foo.md", h(1, 1));
        a.path = "bar_a.md".into();
        a.renamed_hlc = Some(h(5, 1));
        let mut b = registered(id(1), "foo.md", h(1, 1));
        b.path = "bar_b.md".into();
        b.renamed_hlc = Some(h(7, 2));

        let mut reg = RegistryLattice::new();
        reg.observe(a);
        reg.observe(b);
        assert_paths(&reg, &[("bar_b.md", id(1))]); // higher renamed_hlc wins
        assert!(
            !reg.get(&id(1)).unwrap().displaced,
            "same doc — not a collision"
        );
    }

    #[test]
    fn test_create_create_same_path_conflict_copy() {
        // Two DIFFERENT docs created at the same path → both alive, one canonical,
        // the other displaced to its conflict path. (Journey F create/create.)
        let mut reg = RegistryLattice::new();
        reg.observe(registered(id(1), "notes.md", h(5, 1)));
        reg.observe(registered(id(2), "notes.md", h(3, 2))); // lower path_hlc → loser

        let winner = id(1);
        let loser = id(2);
        assert!(!reg.get(&winner).unwrap().displaced);
        assert!(
            reg.get(&loser).unwrap().displaced,
            "lower priority is displaced"
        );
        assert_paths(
            &reg,
            &[
                ("notes.md", winner),
                (&crate::lattice::conflict_path("notes.md", &loser), loser),
            ],
        );
    }

    #[test]
    fn test_conflict_copy_is_order_independent() {
        // Same two docs, observed in the opposite order → identical outcome.
        let mut r1 = RegistryLattice::new();
        r1.observe(registered(id(1), "notes.md", h(5, 1)));
        r1.observe(registered(id(2), "notes.md", h(3, 2)));

        let mut r2 = RegistryLattice::new();
        r2.observe(registered(id(2), "notes.md", h(3, 2)));
        r2.observe(registered(id(1), "notes.md", h(5, 1)));

        assert_eq!(r1.effective_path_index(), r2.effective_path_index());
        assert_eq!(
            r1.get(&id(1)).unwrap().displaced,
            r2.get(&id(1)).unwrap().displaced
        );
        assert_eq!(
            r1.get(&id(2)).unwrap().displaced,
            r2.get(&id(2)).unwrap().displaced
        );
    }

    #[test]
    fn test_displaced_loser_does_not_reclaim_after_winner_deleted() {
        // No auto-reclaim: a tombstoned winner keeps its intended path, so it
        // stays the group max and the loser stays displaced.
        let mut reg = RegistryLattice::new();
        reg.observe(registered(id(1), "notes.md", h(5, 1)));
        reg.observe(registered(id(2), "notes.md", h(3, 2))); // id(2) displaced
        assert!(reg.get(&id(2)).unwrap().displaced);

        // Delete the winner id(1).
        let mut del = registered(id(1), "notes.md", h(5, 1));
        del.deleted_hlc = Some(h(9, 1));
        reg.observe(del);

        assert!(
            reg.get(&id(2)).unwrap().displaced,
            "loser stays displaced (no reclaim)"
        );
        assert_paths(
            &reg,
            &[(&crate::lattice::conflict_path("notes.md", &id(2)), id(2))],
        );
    }

    #[test]
    fn test_create_at_path_freed_by_delete_is_not_displaced() {
        // A new doc created at a path whose prior holder is TOMBSTONED cleanly
        // owns the path — a dead record never contends (alive-only arbitration).
        let mut reg = RegistryLattice::new();
        let mut dead = registered(id(1), "notes.md", h(1, 1));
        dead.deleted_hlc = Some(h(2, 1));
        reg.observe(dead);
        reg.observe(registered(id(2), "notes.md", h(5, 2)));

        assert!(
            !reg.get(&id(2)).unwrap().displaced,
            "sole alive claimant owns the path"
        );
        assert_paths(&reg, &[("notes.md", id(2))]);
    }

    #[test]
    fn test_case_variant_paths_collide() {
        // Foo.md vs foo.md case-fold to the same path → arbitrated, matching the
        // DB's UNIQUE(lower(path)).
        let mut reg = RegistryLattice::new();
        reg.observe(registered(id(1), "Foo.md", h(5, 1)));
        reg.observe(registered(id(2), "foo.md", h(3, 2)));
        assert!(
            reg.get(&id(2)).unwrap().displaced,
            "case-variant is a collision"
        );
    }

    /// `restore_records` re-arbitrates: a restored snapshot carrying flags no
    /// election produced (here a loser hand-flipped to undisplaced) is
    /// recomputed on the spot. With incremental per-observe elections there is
    /// no later from-scratch pass to absorb a stale flag, so the rollback seam
    /// itself must leave the flags a pure function of the record set.
    #[test]
    fn test_restore_records_rearbitrates() {
        let mut reg = RegistryLattice::new();
        reg.observe(registered(id(1), "notes.md", h(5, 1)));
        reg.observe(registered(id(2), "notes.md", h(3, 2)));
        let mut corrupted = reg.get(&id(2)).unwrap().clone();
        assert!(corrupted.displaced, "loser starts displaced");
        corrupted.displaced = false;

        reg.restore_records([(id(2), Some(corrupted))]);

        assert!(
            reg.get(&id(2)).unwrap().displaced,
            "restore re-elects the group: the loser cannot stay undisplaced"
        );
    }

    // --- §7.3: path-arbitration confluence over shuffled / partial merges ---
    //
    // The catalog §7.3 contract: applying the same set of lifecycle events in ANY
    // order — including cross-id path collisions and partial (split) merges — must
    // yield the IDENTICAL observable registry: same per-id projection
    // `(effective_path, is_alive, displaced)` and same `effective_path_index`.
    // These proptests are the randomized form the example tests above seed.

    mod confluence {
        use super::*;
        use proptest::prelude::*;

        /// A single lifecycle event for one of a few documents at one of a few
        /// paths. Kept to a SMALL domain (3 ids × 3 paths × bounded HLCs) so
        /// collisions and revivals are hit densely.
        #[derive(Clone, Debug)]
        enum Event {
            Register { id: u8, path: u8, at: Hlc },
            Rename { id: u8, path: u8, at: Hlc },
            Delete { id: u8, at: Hlc },
        }

        const PATHS: [&str; 3] = ["a.md", "b.md", "c.md"];

        fn apply(reg: &mut RegistryLattice, ev: &Event) {
            match ev {
                Event::Register { id: i, path, at } => {
                    reg.observe(registered(id(*i), PATHS[*path as usize], *at));
                }
                Event::Rename { id: i, path, at } => {
                    // A rename fragment: carries only renamed_hlc + the new path.
                    // (Partial DocRecord — merge field-maxes it into any existing.)
                    let mut r = registered(id(*i), PATHS[*path as usize], *at);
                    r.registered_hlc = None;
                    r.renamed_hlc = Some(*at);
                    reg.observe(r);
                }
                Event::Delete { id: i, at } => {
                    let mut r = registered(id(*i), "", *at);
                    r.registered_hlc = None;
                    r.deleted_hlc = Some(*at);
                    reg.observe(r);
                }
            }
        }

        /// One id's observable projection row:
        /// `(document_id, effective_path, is_alive, displaced)`.
        type IdProjection = (Uuid, String, bool, bool);
        /// One `(effective_path, document_id)` entry of the effective-path index.
        type PathIndexEntry = (String, Uuid);

        /// The full observable projection of a lattice: every id's
        /// `(effective_path, is_alive, displaced)` plus the path index. Two
        /// lattices that built the same event set in different orders must match
        /// on this exactly.
        fn projection(reg: &RegistryLattice) -> (Vec<IdProjection>, Vec<PathIndexEntry>) {
            let mut per_id: Vec<IdProjection> = reg
                .records()
                .map(|(id, r)| (*id, r.effective_path(), r.is_alive(), r.displaced))
                .collect();
            per_id.sort();
            let mut idx: Vec<PathIndexEntry> = reg.effective_path_index().into_iter().collect();
            idx.sort();
            (per_id, idx)
        }

        fn arb_event() -> impl Strategy<Value = Event> {
            let id_s = 1u8..4; // ids 1,2,3
            let path_s = 0u8..3; // a/b/c
            let hlc_s = (1u64..6, 0u8..3).prop_map(|(p, a)| h(p, a));
            prop_oneof![
                (id_s.clone(), path_s.clone(), hlc_s.clone())
                    .prop_map(|(id, path, at)| Event::Register { id, path, at }),
                (id_s.clone(), path_s, hlc_s.clone()).prop_map(|(id, path, at)| Event::Rename {
                    id,
                    path,
                    at
                }),
                (id_s, hlc_s).prop_map(|(id, at)| Event::Delete { id, at }),
            ]
        }

        proptest! {
            /// Confluence: the same event multiset applied in two independent
            /// orders yields the identical observable registry. This is the core
            /// §7.3 property — arbitration (incl. cross-id collisions, revival,
            /// displacement) is a deterministic function of the event SET, not the
            /// order. `perm` is an independent shuffle of the same events.
            ///
            /// This proptest originally CAUGHT A REAL CONFLUENCE BUG (now fixed):
            /// `displaced` was a MONOTONIC flag — `arbitrate` only ever SET it,
            /// never recomputed — so a loser that lost a *transient* collision
            /// (winner later renamed away from the contended path) stayed displaced
            /// under one event order but not another. Fixed by making `displaced` a
            /// pure projection: `arbitrate` recomputes it from scratch every merge
            /// over ALL records grouped by intended path (registry.rs). A winner
            /// that renames away leaves the group → loser reclaims (confluent); a
            /// *deleted* winner keeps its intended path → stays the max → loser
            /// stays displaced (the decided no-auto-reclaim,
            /// `test_displaced_loser_does_not_reclaim_after_winner_deleted`).
            #[test]
            fn arbitration_is_order_independent(
                events in prop::collection::vec(arb_event(), 1..12),
                perm in prop::collection::vec(any::<prop::sample::Index>(), 0..24),
            ) {
                let mut r1 = RegistryLattice::new();
                for ev in &events {
                    apply(&mut r1, ev);
                }

                // Build a permutation of the SAME events via the random indices
                // (Fisher-Yates-ish using sampled indices), then apply to r2.
                let mut shuffled = events.clone();
                let n = shuffled.len();
                for (k, idx) in perm.iter().enumerate() {
                    let i = k % n;
                    let j = idx.index(n);
                    shuffled.swap(i, j);
                }
                let mut r2 = RegistryLattice::new();
                for ev in &shuffled {
                    apply(&mut r2, ev);
                }

                prop_assert_eq!(projection(&r1), projection(&r2));
            }

            /// Idempotency at the registry level: re-applying every event a second
            /// time changes nothing observable (merge is a join).
            #[test]
            fn re_applying_events_is_idempotent(
                events in prop::collection::vec(arb_event(), 1..12),
            ) {
                let mut reg = RegistryLattice::new();
                for ev in &events {
                    apply(&mut reg, ev);
                }
                let once = projection(&reg);
                for ev in &events {
                    apply(&mut reg, ev);
                }
                prop_assert_eq!(once, projection(&reg));
            }

            /// The incremental per-observe election equals the from-scratch
            /// recompute: bulk-rebuilding a lattice (`observe_all` — clean
            /// index, every group re-elected) from the incrementally built
            /// lattice's own records yields the identical structure — records,
            /// displaced flags, and path-group index alike. Guards the index
            /// maintenance in `observe` (stale group membership would show up
            /// here as a flag or index mismatch).
            #[test]
            fn incremental_arbitration_matches_from_scratch(
                events in prop::collection::vec(arb_event(), 1..12),
            ) {
                let mut inc = RegistryLattice::new();
                for ev in &events {
                    apply(&mut inc, ev);
                }
                let mut scratch = RegistryLattice::new();
                scratch.observe_all(inc.records().map(|(_, r)| r.clone()));
                prop_assert_eq!(&inc, &scratch);
            }

            /// Invariant: at most one alive, non-displaced document per effective
            /// path — the path index never double-books a path. Holds for ANY
            /// event set (the safety property arbitration guarantees).
            #[test]
            fn at_most_one_canonical_holder_per_path(
                events in prop::collection::vec(arb_event(), 1..12),
            ) {
                let mut reg = RegistryLattice::new();
                for ev in &events {
                    apply(&mut reg, ev);
                }
                // effective_path_index maps each effective path → exactly one id;
                // its very construction would panic/overwrite on a double-book, so
                // assert every alive record's effective path is unique.
                let mut seen = std::collections::HashSet::new();
                for (_id, r) in reg.records() {
                    if r.is_alive() {
                        let p = r.effective_path();
                        prop_assert!(seen.insert(p.clone()), "two alive docs at one effective path: {}", p);
                    }
                }
            }
        }
    }
}
