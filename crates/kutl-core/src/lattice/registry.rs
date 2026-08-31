//! The registry as a whole is a join-semilattice — not just each [`DocRecord`].
//!
//! A map `document_id → DocRecord` is a pointwise lattice, but the *path
//! projection* (`path → id`) is single-valued only with cross-id coordination.
//! [`RegistryLattice`] provides it: [`observe`](RegistryLattice::observe) merges
//! one record by id, then runs **path arbitration** — for each set of records
//! sharing a (case-folded) intended path, the highest-priority one
//! `(path_hlc, document_id)` keeps the path and the rest are marked
//! [`DocRecord::displaced`] so they resolve to their own loser-only conflict
//! paths. Displacement is recomputed from scratch every merge (a pure function
//! of the record set), **not** a monotonic flag — so it is deterministic and
//! confluent: a record set yields the same canonical holders and the same
//! displacements on every node, in any merge order.

use std::collections::HashMap;

use uuid::Uuid;

use super::{DocRecord, Lattice};
use crate::Hlc;

/// The lifecycle lattice for one space: a map of per-document [`DocRecord`]s with
/// cross-document path arbitration applied after every merge.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RegistryLattice {
    records: HashMap<Uuid, DocRecord>,
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
    pub fn observe(&mut self, incoming: DocRecord) -> &DocRecord {
        let id = incoming.document_id;
        self.records
            .entry(id)
            .and_modify(|r| r.merge(&incoming))
            .or_insert(incoming);
        self.arbitrate();
        self.records.get(&id).expect("just inserted or merged")
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
    fn arbitrate(&mut self) {
        let mut groups: HashMap<String, Vec<Uuid>> = HashMap::new();
        for (id, r) in &self.records {
            groups
                .entry(r.current_path().to_lowercase())
                .or_default()
                .push(*id);
        }
        for ids in groups.into_values() {
            let winner = ids
                .iter()
                .copied()
                .max_by(|a, b| {
                    let (ra, rb) = (&self.records[a], &self.records[b]);
                    (ra.path_hlc(), *a).cmp(&(rb.path_hlc(), *b))
                })
                .expect("group is non-empty");
            for id in ids {
                self.records
                    .get_mut(&id)
                    .expect("id came from the record set")
                    .displaced = id != winner;
            }
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

    /// Directly set (or, with `None`, remove) the record for `id`, bypassing
    /// merge. **Only for capture-restore rollback** — restoring a snapshot taken
    /// before an `observe` whose persistence then failed. Not a lattice
    /// operation; never use it to apply a change.
    pub fn restore_record(&mut self, id: Uuid, prior: Option<DocRecord>) {
        match prior {
            Some(r) => {
                self.records.insert(id, r);
            }
            None => {
                self.records.remove(&id);
            }
        }
    }

    /// The path projection: each alive document's [`DocRecord::effective_path`]
    /// (its conflict path while displaced, else its intended path) → its id. At
    /// most one id per path, by construction.
    ///
    /// The `collect` into a `HashMap` silently keeps the last value on a
    /// duplicate key, so a (hypothetical, arbitration-bug) double-book collapses
    /// here rather than erroring. The relay's `detect_divergence` — which reads
    /// the full record/entry set, not this collapsed map — is the authoritative
    /// fail-loud detector for that invariant. `arbitrate` keeps it quiet
    /// (proptest `at_most_one_canonical_holder_per_path`).
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
        // No auto-reclaim: after the winner is deleted the loser stays at its
        // conflict path (displaced is monotonic).
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
