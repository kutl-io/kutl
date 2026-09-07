//! `reconcile_placement`: placement as a confluent lattice projection (gamma).
//!
//! Each pass diffs the desired `id → effective_path` assignment (the pure
//! [`desired_assignment`] mirror of `RegistryLattice::arbitrate`)
//! against the in-memory [`DiskShadow`](crate::core::DiskShadow), and emits a
//! [`Effect::GuardedPlace`] for every **mover** (a doc whose desired path differs
//! from where it currently sits on disk). Movers are ordered `(path_hlc, id)` so a
//! displaced loser (which resolves to a collision-free conflict path) is vacated
//! BEFORE the winner places onto the freed path — the same order the relay
//! arbitrates, so daemon and relay agree by construction and reach the same
//! disk state under injected clock skew, whichever of them observed events first.
//!
//! This is NOT a procedural worklist: each pass is a pure recompute from
//! `known_records` + the shadow, so a displaced loser's correction is a
//! function of who else currently claims its path, never of arrival order.
//!
//! `deferred` is a DERIVED set: each pass clears and rebuilds `state.deferred`,
//! so a doc that left `known_records` (a tombstone) is no longer a mover and so
//! leaves no stale entry to resurrect — ghost resurrection is structurally
//! impossible. A mover whose desired path is held by an [`Occupant::Untracked`]
//! DEFERS (records in `deferred`, emits no `GuardedPlace`) UNLESS it is
//! revival-exempt (`state.exempt_revival`, seeded from `was_seen_before` before the
//! HLC fold), in which case it places onto its own orphan. The `expected_free` flag
//! carries the core's belief so the shell can assert, inside its atomic
//! stat-and-place critical section, that the TOCTOU window stayed shut.
//!
//! No `std::fs`, no `.await`, no `mpsc`, no clock read.

use std::collections::{HashMap, HashSet};

use kutl_core::lattice::RegistryLattice;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::SafeRelayPath;
use crate::core::{Effect, Occupant, PlaceKind, SpaceState, rel_path_to_string};

/// The desired `id → effective_path` map for all alive documents. Pure function
/// of the record set, so daemon and relay compute the same result for the same
/// inputs — the agreement that makes convergence reproducible under clock
/// skew. Built by inverting
/// `RegistryLattice::effective_path_index`, which already
/// applies `arbitrate`'s winner election and the loser's conflict-path
/// resolution — this carries no separate copy of the arbitration rule.
///
/// Exposed as `pub` so `kutl-sim` can derive placement decisions without an
/// extra crate boundary workaround.
#[must_use]
pub fn desired_assignment(reg: &RegistryLattice) -> HashMap<Uuid, String> {
    reg.effective_path_index()
        .into_iter()
        .map(|(path, id)| (id, path))
        .collect()
}

/// Maximum [`Effect::GuardedPlace`] emissions per reconcile pass.
///
/// Pacing for bulk bursts: an uncapped pass over a large freshly-registered
/// space emits thousands of places at once, and each Register-kind place
/// rides a `Subscribe` to the relay, a storm that overflows the relay's
/// bounded per-connection lanes and converts the win into eviction-resync
/// churn. The value is half the relay's per-connection data lane
/// (`DEFAULT_OUTBOUND_CAPACITY`, 512), leaving headroom for the ack/notify
/// traffic sharing it; the coupling is by hand, so a smaller relay lane
/// needs a smaller cap here.
///
/// Correctness under the cap comes from the lattice: the cascade is
/// confluent and idempotent at the fixpoint, and movers are emitted in the
/// arbitrate election order (losers vacate before their winners), so a
/// capped PREFIX of that order preserves the dependency direction.
/// Liveness comes from the pass itself: a truncated pass re-arms
/// `placement_dirty` and flags `placement_truncated`, so the driver's
/// drain-edge probe runs the next chunk as soon as the intake is drained,
/// bypassing the trickle floor. A Register-kind place answers with content
/// ops, not a lifecycle ack, so nothing else would re-drive the remainder
/// before the metrics-tick backstop.
const RECONCILE_PLACEMENTS_PER_PASS: usize = 256;

/// [`reconcile_placement`] gated on the driver's intake signal: while the
/// event loop still holds undispatched events (`state.intake_backlogged`),
/// return no effects — the recompute runs once when the burst drains (plus
/// every `MetricsTick`), not once per event. Removes the O(N) recompute per
/// event that made bulk add/move O(N²) in total.
pub(crate) fn reconcile_unless_backlogged(state: &mut SpaceState) -> Vec<Effect> {
    if state.intake_backlogged {
        debug!("reconcile skipped: intake backlogged");
        // Arm the driver's drained-intake probe: a skipped pass is owed, and
        // the burst's trailing inputs may carry no reconcile tail to pay it.
        state.placement_dirty = true;
        return Vec::new();
    }
    debug!("reconcile running: intake drained");
    reconcile_placement(state)
}

/// The placement cascade as a confluent lattice projection, run synchronously
/// after a `handle` mutation.
///
/// gamma: compute the desired `id → effective_path` from `known_records` (the
/// pure mirror of the relay's `arbitrate`), diff it against the on-disk shadow,
/// and emit a [`Effect::GuardedPlace`] for every mover. A **mover** is a doc whose
/// desired path differs from its current `shadow.shadow_path` — a doc already
/// sitting at its desired path emits nothing.
///
/// Movers are placed in `(path_hlc, id)` order so a displaced loser (whose desired
/// path is its collision-free conflict path) vacates its old path BEFORE the
/// winner places onto the freed path — the SAME order the relay's `arbitrate`
/// elects, so daemon and relay reconcile to the same disk state regardless of
/// who observed events first or whose clock is ahead.
///
/// The fork: a mover this daemon already tracks on disk (`shadow_path` has it)
/// emits [`PlaceKind::Rename`] (vacate the old path, move our held file); an
/// untracked mover emits [`PlaceKind::Register`] (the content streams in via
/// the shell-emitted `Subscribe` on the place's ACK — `place_now`, daemon.rs).
///
/// A mover whose desired path is held by an [`Occupant::Untracked`]
/// DEFERS (recorded in `state.deferred`, no `GuardedPlace` emitted) unless it is
/// revival-exempt (`state.exempt_revival`). `state.deferred` is CLEARED and rebuilt
/// every pass, so it is a pure function of `known_records` + the shadow + the
/// exemption set — never a procedural worklist. A doc that left `known_records` (a
/// tombstone) is not in `desired`, so it is not a mover and leaves no stale
/// deferral: the resurrection bug is structural.
///
/// `expected_free` is the core's belief, read from the shadow, that the target
/// holds no untracked occupant — the TOCTOU assertion the shell checks inside
/// its atomic stat-and-place critical section. A revival placing onto its
/// OWN untracked orphan emits `expected_free = false` (it knows the occupant is
/// there and adopts it); the shell sees the agreement and places. A normal mover
/// onto a free path emits `expected_free = true`; if the shell's atomic stat then
/// finds an occupant, that disagreement is the TOCTOU race the guard catches.
///
/// The shadow is updated only on shell-ACK of a landed disk effect, so a
/// re-run after the place lands sees `shadow_path == desired` for that doc and
/// emits nothing — the recompute is idempotent at the fixpoint.
///
/// A revival exemption is spent by the first pass that could act on it: it
/// survives a gated pass (which never enters here) and a pass that left its
/// doc unemitted at the emission cap, and nothing else — whether the doc
/// placed, already sat at its path, or resolved elsewhere, the pass has seen
/// it and a later mover onto the same path faces the occupant guard again.
pub fn reconcile_placement(state: &mut SpaceState) -> Vec<Effect> {
    assert_identity_indexes_consistent(state);
    // This pass pays any skipped-gate debt (see `placement_dirty`).
    state.placement_dirty = false;
    let desired = desired_assignment(&state.known_records);

    // `deferred` is DERIVED, not a worklist: clear it and rebuild from this pass's
    // movers, so a doc that left `known_records` (a tombstone) cannot leave a stale
    // entry to resurrect.
    state.deferred.clear();

    // Movers: the desired path differs from where the doc currently sits on disk.
    let mut movers: Vec<(Uuid, String)> = desired
        .iter()
        .filter(|(id, want)| {
            state
                .shadow
                .shadow_path
                .get(id)
                .map(|p| rel_path_to_string(p))
                .as_deref()
                != Some(want.as_str())
        })
        .map(|(id, want)| (*id, want.clone()))
        .collect();

    // Order by (path_hlc, id), the relay's `arbitrate` election key, so a
    // loser-to-conflict move is emitted before the winner that takes its
    // freed path. Reusing the lattice's own key is why the projection
    // reproduces the relay's outcome under clock skew for free: there is no
    // second copy of the order to drift. The key is computed once per mover,
    // not once per comparison.
    movers.sort_by_cached_key(|(id, _)| {
        (
            state
                .known_records
                .get(id)
                .and_then(kutl_core::lattice::DocRecord::path_hlc),
            *id,
        )
    });

    let mut effects = Vec::new();
    let mut truncated = false;
    // Movers the emission cap left unemitted: the only docs whose revival
    // exemption this pass must carry to the next.
    let mut carried: HashSet<Uuid> = HashSet::new();
    for (id, want) in movers {
        let Some(target) = safe_target(&want) else {
            // An unparseable desired path can't be placed; the boundary handlers
            // already reject invalid relay paths, so this is unreachable for a
            // real record set. Skip rather than panic.
            warn!(%id, %want, "skipping mover with unparseable desired path");
            continue;
        };

        // Defer iff the desired path is held by an Untracked occupant AND
        // this doc is NOT revival-exempt. The shadow occupant map is keyed by
        // the shared `fold_path` case rule (`want` is already in wire form,
        // so the fold applies directly).
        //
        // STALENESS: an `Untracked` marker is overridden when `file_identity`
        // claims the target — the pure twin of the shell's `stat_untracked`
        // precedence: an identity-claimed path is not a foreign untracked
        // occupant. The marker goes stale when the shell's stat-and-place
        // disagreement races the local create's mint; the mint now also folds `Tracked` straight
        // over the marker (shadow-at-mint) and subscribes explicitly, so this
        // override is defense in depth for any identity-claimed-but-unfolded
        // window rather than the sole guard. Historically its absence
        // stranded the minted doc deferred on its OWN file forever — never
        // placed, and never subscribed back when the `Subscribe` rode the
        // place ACK — so the winner-doc's post-conflict edits silently never
        // arrived (the f_conflicts create/create divergence).
        let occupant = state
            .shadow
            .shadow_occupant
            .get(&kutl_core::lattice::fold_path(&want));
        let held_by_untracked = crate::core::shadow::held_by_foreign_untracked(
            occupant,
            // The pure core's only disk evidence is its shadow belief: an
            // Untracked marker says a foreign file is present.
            matches!(occupant, Some(Occupant::Untracked)),
            state.file_identity.contains_key(&target),
        );
        // Exempt only for the path the revival was seeded for: a doc whose
        // desired path moved on since its register is an ordinary mover.
        let exempt = state
            .exempt_revival
            .get(&id)
            .is_some_and(|seeded| seeded == &want);
        if held_by_untracked && !exempt {
            // Defer: record the path we wait on and emit NO place — the occupant
            // must vacate first (the relay issues its move, then a later reconcile
            // re-derives this doc as a now-placeable mover).
            debug!(%id, target = %want, "placement deferred: target held by untracked occupant");
            if let Ok(safe) = SafeRelayPath::new(&want) {
                state.deferred.insert(id.to_string(), safe);
            }
            // An exemption is spent only by a pass that could act on the
            // seeded path; a deferred mover keeps it for the pass that can.
            carried.insert(id);
            continue;
        }

        // Pacing cap: emission stops for this pass, but the loop keeps
        // running so `deferred` stays a complete pure function of the full
        // state (a capped pass must not under-report deferrals). The
        // unemitted movers are still movers next pass; the truncation is
        // recorded below so that pass is owed (see
        // [`RECONCILE_PLACEMENTS_PER_PASS`]).
        if effects.len() >= RECONCILE_PLACEMENTS_PER_PASS {
            truncated = true;
            carried.insert(id);
            continue;
        }

        // The fork: tracked here → Rename (vacate the old path); else → Register.
        let place_kind = match state.shadow.shadow_path.get(&id).cloned() {
            Some(old_rel) => PlaceKind::Rename { old_rel },
            None => PlaceKind::Register,
        };
        // expected_free: the shadow shows no untracked occupant at the target. A
        // revival adopting its own orphan reaches here with `held_by_untracked` true
        // → `expected_free = false`, signaling the shell to place over the occupant
        // it knows is there. A normal free place is `true`.
        let expected_free = !held_by_untracked;
        effects.push(Effect::GuardedPlace {
            id: id.to_string(),
            target,
            expected_free,
            place_kind,
        });
    }
    // A capped pass owes the remainder: re-arm the drain-edge probe and mark
    // the debt burst-class so the trickle floor does not pace bulk.
    state.placement_truncated = truncated;
    if truncated {
        state.placement_dirty = true;
    }
    // An exemption is spent by the first pass that could act on it. A doc
    // that placed, one already at its path, and one whose desired path moved
    // on have all been seen by an ungated pass; an exemption kept past that
    // would let a later mover onto the seeded path claim a foreign untracked
    // file there instead of deferring. Only a mover the cap left unemitted
    // keeps it, for the pass that will emit it.
    state.exempt_revival.retain(|id, _| carried.contains(id));
    // Pass summary, only when the pass decided something — the fixpoint (no
    // movers, nothing deferred) stays silent so a periodic MetricsTick reconcile
    // does not flood the log.
    if !effects.is_empty() || !state.deferred.is_empty() {
        debug!(
            places = effects.len(),
            deferred = state.deferred.len(),
            truncated,
            "reconcile pass"
        );
    }
    effects
}

/// Drift tripwire for the identity indexes (debug builds only): a mutation
/// of `file_identity` that bypassed the `identity_*` choke points fails here
/// loudly instead of silently mis-answering the indexed probes.
///
/// The length check is O(1) and runs in every debug build. The full walks
/// (every index row points at an identity that agrees with it, and every
/// identity is indexed) are O(tracked) per pass and run in unit-test builds
/// only, so a debug-profile perf run does not pay the O(N)-per-pass term
/// the indexes exist to remove.
fn assert_identity_indexes_consistent(state: &SpaceState) {
    debug_assert_eq!(
        state.identity_idx.tracked.len(),
        state.file_identity.len(),
        "identity indexes drifted from file_identity — mutate via identity_insert/remove/set_inode \
         or call rebuild_identity_indexes after a bulk build"
    );
    #[cfg(test)]
    {
        let idx = &state.identity_idx;
        let inode_rows_agree = idx.by_inode.iter().all(|(ino, rels)| {
            rels.iter().all(|rel| {
                state
                    .file_identity
                    .get(rel)
                    .is_some_and(|id| id.inode == Some(*ino))
            })
        });
        let inodes_indexed = state.file_identity.iter().all(|(rel, id)| {
            id.inode.is_none_or(|ino| {
                idx.by_inode
                    .get(&ino)
                    .is_some_and(|rels| rels.contains(rel))
            })
        });
        let fold_rows_agree = idx
            .by_casefold
            .iter()
            .all(|(key, rels)| rels.iter().all(|rel| &crate::core::casefold(rel) == key));
        let folds_indexed = state.file_identity.keys().all(|rel| {
            idx.by_casefold
                .get(&crate::core::casefold(rel))
                .is_some_and(|rels| rels.contains(rel))
        });
        debug_assert!(
            inode_rows_agree && inodes_indexed && fold_rows_agree && folds_indexed,
            "identity indexes drifted from file_identity — mutate via identity_insert/remove/set_inode \
             or call rebuild_identity_indexes after a bulk build"
        );
    }
}

/// Parse a desired effective path into a placement target, returning `None` for an
/// unparseable/unsafe path. Wraps [`SafeRelayPath::new`] so the same validation the
/// relay-supplied paths pass guards the cascade's targets too.
fn safe_target(want: &str) -> Option<std::path::PathBuf> {
    SafeRelayPath::new(want)
        .ok()
        .map(SafeRelayPath::into_path_buf)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use kutl_core::hlc::{ActorId, Hlc};
    use kutl_core::lattice::{DocRecord, RegistryLattice};
    use uuid::Uuid;

    use super::*;

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

    fn rec(id: Uuid, path: &str, registered: Hlc) -> DocRecord {
        DocRecord::register(id, path, Some(registered))
    }

    /// `desired_assignment` must produce exactly the inverse of
    /// `effective_path_index` — same winner at a contended path, same
    /// conflict-path resolution for the loser — for ANY record set. This is
    /// the agreement with the relay's `arbitrate` that reproducibility under
    /// clock skew depends on.
    #[test]
    fn test_desired_assignment_matches_effective_path_index() {
        let win = Uuid::from_u128(2); // higher id wins the (path_hlc, id) tie
        let lose = Uuid::from_u128(1);
        let mut reg = RegistryLattice::new();
        // Same path, same path_hlc → `max(path_hlc, id)` picks `win`.
        reg.observe(rec(lose, "notes/a.md", h(5, 1)));
        reg.observe(rec(win, "notes/a.md", h(5, 1)));

        let desired = desired_assignment(&reg);

        // The winner resolves to the clean path; the loser to its conflict path.
        assert_eq!(desired.get(&win).map(String::as_str), Some("notes/a.md"));
        assert_eq!(
            desired.get(&lose).cloned(),
            Some(kutl_core::lattice::conflict_path("notes/a.md", &lose))
        );

        // And it is the exact inverse of the lattice's own projection.
        let index = reg.effective_path_index();
        for (id, path) in &desired {
            assert_eq!(
                index.get(path),
                Some(id),
                "id {id} not the index holder of {path}"
            );
        }
        assert_eq!(
            desired.len(),
            index.len(),
            "desired and index disagree on cardinality"
        );
    }

    /// A tombstoned record is NOT in the desired assignment (only alive docs
    /// resolve to a path), mirroring `effective_path_index`'s `is_alive` filter.
    #[test]
    fn test_desired_assignment_excludes_tombstoned() {
        let id = Uuid::from_u128(7);
        let mut reg = RegistryLattice::new();
        let mut r = rec(id, "gone.md", h(1, 1));
        r.deleted_hlc = Some(h(2, 1));
        reg.observe(r);
        assert!(!desired_assignment(&reg).contains_key(&id));
    }

    fn st() -> SpaceState {
        SpaceState::new_for_test("space-1".into(), PathBuf::from("/tmp/x"), "did:a".into())
    }

    fn uid(n: u8) -> Uuid {
        let mut b = [0u8; 16];
        b[15] = n;
        Uuid::from_bytes(b)
    }

    fn alive_record(id: Uuid, path: &str) -> DocRecord {
        DocRecord::register(
            id,
            path,
            Some(Hlc {
                physical_ms: 1,
                logical: 0,
                actor: ActorId(uid(99)),
            }),
        )
    }

    #[test]
    fn test_alive_unmaterialized_record_emits_guarded_register() {
        // gamma: an alive, not-yet-on-disk doc is a mover — emit a
        // GuardedPlace(Register) onto its desired path (the untracked fork of the
        // former `materialize_at`). The shell's place-ACK subscribes it.
        let mut s = st();
        s.known_records.observe(alive_record(uid(1), "a.md"));
        let effects = reconcile_placement(&mut s);
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { id, target, expected_free, place_kind }
                    if *id == uid(1).to_string()
                        && target.as_path() == std::path::Path::new("a.md")
                        && *expected_free
                        && *place_kind == PlaceKind::Register
            )),
            "expected a GuardedPlace(Register) for the unmaterialized doc, got {effects:?}"
        );
    }

    /// A stale `Occupant::Untracked` marker must NOT defer the doc whose own
    /// `file_identity` claims the target — the pure twin of the shell's
    /// `stat_untracked` precedence (daemon.rs): an identity-claimed path is
    /// not an untracked occupant. The marker goes stale when a shell
    /// disagreement races the local create's mint; deferring the minted doc on
    /// its own file strands it unplaced and unsubscribed forever (the
    /// `f_conflicts` create/create divergence).
    #[test]
    fn test_identity_claimed_target_overrides_stale_untracked_marker() {
        let mut s = st();
        s.known_records.observe(alive_record(uid(1), "a.md"));
        s.shadow
            .shadow_occupant
            .insert("a.md".into(), Occupant::Untracked);
        // The doc's own mint claimed the path (get_or_create_uuid).
        s.file_identity.insert(
            PathBuf::from("a.md"),
            crate::core::FileIdentity {
                document_uuid: uid(1).to_string(),
                inode: None,
                last_written_hash: None,
            },
        );
        s.rebuild_identity_indexes();
        let effects = reconcile_placement(&mut s);
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { id, target, place_kind, .. }
                    if *id == uid(1).to_string()
                        && target.as_path() == Path::new("a.md")
                        && *place_kind == PlaceKind::Register
            )),
            "an identity-claimed target must place (stale marker overridden), got {effects:?}"
        );
        assert!(
            !s.deferred.contains_key(&uid(1).to_string()),
            "the claiming doc must not be deferred on its own file"
        );
    }

    /// The deferral is unchanged when NO identity claims the marked target: a
    /// genuinely untracked occupant (a foreign create, not yet minted) still
    /// defers a non-exempt mover — the no-clobber contract.
    #[test]
    fn test_unclaimed_untracked_marker_still_defers() {
        let mut s = st();
        s.known_records.observe(alive_record(uid(1), "a.md"));
        s.shadow
            .shadow_occupant
            .insert("a.md".into(), Occupant::Untracked);
        let effects = reconcile_placement(&mut s);
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::GuardedPlace { .. })),
            "a genuinely untracked occupant must still defer, got {effects:?}"
        );
        assert!(
            s.deferred.contains_key(&uid(1).to_string()),
            "the blocked mover is recorded as deferred"
        );
    }

    #[test]
    fn test_materialized_record_emits_nothing() {
        // A doc already sitting at its desired path is not a mover — no effect.
        let mut s = st();
        s.known_records.observe(alive_record(uid(1), "a.md"));
        s.shadow.set_tracked(&PathBuf::from("a.md"), uid(1));
        let effects = reconcile_placement(&mut s);
        assert!(
            effects.is_empty(),
            "a doc already at its desired path should not re-place, got {effects:?}"
        );
    }

    #[test]
    fn test_record_at_wrong_path_emits_guarded_rename() {
        // A tracked doc whose desired path differs from where it sits on disk is a
        // mover with PlaceKind::Rename (vacate the old path) — the tracked fork of
        // the former `materialize_at`.
        let mut s = st();
        // The doc is registered at "b.md" but the shadow still holds it at "a.md".
        s.known_records.observe(alive_record(uid(1), "b.md"));
        s.shadow.set_tracked(&PathBuf::from("a.md"), uid(1));
        let effects = reconcile_placement(&mut s);
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { id, target, place_kind, .. }
                    if *id == uid(1).to_string()
                        && target.as_path() == std::path::Path::new("b.md")
                        && *place_kind == PlaceKind::Rename { old_rel: PathBuf::from("a.md") }
            )),
            "expected a GuardedPlace(Rename a.md→b.md), got {effects:?}"
        );
    }

    #[test]
    fn test_tombstoned_record_emits_nothing() {
        // A dead (unregistered) doc is not in `desired_assignment`, so it is never
        // a mover — the resurrection bug is structural: a non-alive
        // record simply leaves the desired set.
        let mut s = st();
        let mut rec = alive_record(uid(1), "a.md");
        rec.deleted_hlc = Some(Hlc {
            physical_ms: 2,
            logical: 0,
            actor: ActorId(uid(99)),
        });
        s.known_records.observe(rec);
        let effects = reconcile_placement(&mut s);
        assert!(
            effects.is_empty(),
            "a tombstoned doc must not be placed, got {effects:?}"
        );
    }

    #[test]
    fn test_loser_to_conflict_emitted_before_winner() {
        // Two docs collide on "a.md"; the loser (lower path_hlc) is displaced
        // to its conflict path and MUST be emitted BEFORE the winner that takes the
        // freed path — the (path_hlc, id) order reused from arbitrate.
        let mut s = st();
        let lose = uid(1);
        let win = uid(2);
        // Same path; win has the higher path_hlc so it owns "a.md".
        s.known_records.observe(DocRecord::register(
            lose,
            "a.md",
            Some(Hlc {
                physical_ms: 5,
                logical: 0,
                actor: ActorId(uid(7)),
            }),
        ));
        s.known_records.observe(DocRecord::register(
            win,
            "a.md",
            Some(Hlc {
                physical_ms: 9,
                logical: 0,
                actor: ActorId(uid(7)),
            }),
        ));
        let effects = reconcile_placement(&mut s);
        let order: Vec<&str> = effects
            .iter()
            .filter_map(|e| match e {
                Effect::GuardedPlace { id, .. } => Some(id.as_str()),
                _ => None,
            })
            .collect();
        let lose_pos = order.iter().position(|id| *id == lose.to_string());
        let win_pos = order.iter().position(|id| *id == win.to_string());
        assert!(
            lose_pos.is_some() && win_pos.is_some() && lose_pos < win_pos,
            "loser-to-conflict must be emitted before the winner, got {order:?}"
        );
    }
    /// A pass truncated by [`RECONCILE_PLACEMENTS_PER_PASS`] emits exactly the
    /// cap, re-arms `placement_dirty`, and flags `placement_truncated`; once
    /// those places land, the next pass emits the remainder and clears both.
    /// Without the re-arm the remainder waits on the metrics tick.
    #[test]
    fn test_capped_pass_rearms_and_next_pass_emits_the_rest() {
        /// Movers beyond the cap, so a second pass has something left to emit.
        const OVERFLOW: usize = 40;
        let mut s = st();
        let total = RECONCILE_PLACEMENTS_PER_PASS + OVERFLOW;
        for i in 0..total {
            let id = Uuid::from_u128(u128::try_from(i).expect("small index") + 1);
            s.known_records
                .observe(alive_record(id, &format!("docs/d{i:04}.md")));
        }

        let first = reconcile_placement(&mut s);
        assert_eq!(
            first.len(),
            RECONCILE_PLACEMENTS_PER_PASS,
            "a pass emits at most the cap"
        );
        assert!(s.placement_dirty, "a truncated pass owes the remainder");
        assert!(s.placement_truncated, "the debt is marked burst-class");

        // The emitted places land: the shadow now holds each at its path.
        for eff in &first {
            if let Effect::GuardedPlace { id, target, .. } = eff {
                s.shadow
                    .set_tracked(target, Uuid::parse_str(id).expect("uuid id"));
            }
        }
        let second = reconcile_placement(&mut s);
        assert_eq!(
            second.len(),
            OVERFLOW,
            "the next pass emits exactly the remainder"
        );
        assert!(!s.placement_dirty, "an uncapped pass leaves no debt");
        assert!(!s.placement_truncated);
    }

    /// The revival exemption seeded by a register is spent by the pass that
    /// places, even when that pass is not the register's own: with the
    /// intake backlogged the register's tail is gated, and the owed pass must
    /// still see the exemption and place the revival onto its own orphan
    /// instead of deferring it forever.
    #[test]
    fn test_revival_exemption_survives_a_gated_pass() {
        use crate::core::{DaemonCore, Event, EventStamp, Occupant};
        let mut s = st();
        let id = uid(7);
        let doc = id.to_string();
        let stamp_hlc = h(10, 1);
        // Seen before (a prior lifecycle op applied) makes the register a
        // revival; its own orphan sits untracked at the path.
        s.record_lifecycle_hlc(&doc, h(1, 1));
        s.shadow.shadow_occupant.insert(
            crate::core::casefold(Path::new("orphan.md")),
            Occupant::Untracked,
        );

        s.intake_backlogged = true;
        let gated = DaemonCore::handle(
            &mut s,
            Event::RemoteRegister {
                document_id: doc.clone(),
                path: "orphan.md".into(),
                stamp: EventStamp {
                    wall_ms: 10,
                    origin_hlc: Some(stamp_hlc),
                },
            },
        );
        assert!(
            !gated
                .iter()
                .any(|e| matches!(e, Effect::GuardedPlace { .. })),
            "the register's own pass is gated"
        );
        assert!(s.placement_dirty, "the gated pass is owed");

        // The owed pass runs on a later event (here the metrics tick).
        s.intake_backlogged = false;
        let paid = DaemonCore::handle(
            &mut s,
            Event::MetricsTick {
                stamp: EventStamp {
                    wall_ms: 11,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            paid.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { id: placed_id, expected_free: false, .. } if *placed_id == doc
            )),
            "the revival places onto its own orphan in the owed pass: {paid:?}"
        );
        assert!(
            !s.deferred.contains_key(&doc),
            "a revival is never deferred"
        );
        assert!(s.exempt_revival.is_empty(), "the pass spent the exemption");
    }

    /// A revival that sorts past the emission cap keeps its exemption: the
    /// pass that could not emit it must not spend it, or the next pass
    /// defers the doc on its own orphan forever.
    #[test]
    fn test_capped_pass_keeps_the_exemption_of_an_unemitted_revival() {
        use crate::core::Occupant;
        /// Movers ahead of the revival in election order, one past the cap.
        const AHEAD: usize = RECONCILE_PLACEMENTS_PER_PASS + 1;
        let mut s = st();
        // Older registrations sort first; the revival's stamp is newest.
        for i in 0..AHEAD {
            let id = Uuid::from_u128(u128::try_from(i).expect("small index") + 1);
            s.known_records.observe(DocRecord::register(
                id,
                format!("docs/d{i:04}.md"),
                Some(h(1, 1)),
            ));
        }
        let revival = uid(200);
        s.known_records
            .observe(DocRecord::register(revival, "orphan.md", Some(h(9, 1))));
        s.shadow.shadow_occupant.insert(
            crate::core::casefold(Path::new("orphan.md")),
            Occupant::Untracked,
        );
        s.exempt_revival.insert(revival, "orphan.md".to_owned());
        let places_revival = |effects: &[Effect]| {
            effects.iter().any(|e| {
                matches!(e, Effect::GuardedPlace { id, expected_free: false, .. } if *id == revival.to_string())
            })
        };

        let first = reconcile_placement(&mut s);
        assert!(
            !places_revival(&first),
            "the revival sorts past the cap and is not emitted this pass"
        );
        assert!(
            s.exempt_revival.contains_key(&revival),
            "the unemitted revival keeps its exemption"
        );
        assert!(!s.deferred.contains_key(&revival.to_string()));

        for eff in &first {
            if let Effect::GuardedPlace { id, target, .. } = eff {
                s.shadow
                    .set_tracked(target, Uuid::parse_str(id).expect("uuid id"));
            }
        }
        let second = reconcile_placement(&mut s);
        assert!(
            places_revival(&second),
            "the next pass places the revival onto its own orphan: {second:?}"
        );
        assert!(s.exempt_revival.is_empty(), "placing spent the exemption");
    }

    /// An exemption seeded for a doc already sitting at its path (a relay
    /// re-assert of a doc this daemon holds) is spent by the first ungated
    /// pass, not held for the session: once the file leaves locally and a
    /// foreign untracked file appears at the same path, the doc is a mover
    /// onto that path again and must DEFER behind the occupant. A stale
    /// exemption would let it place over the foreign file and adopt it.
    #[test]
    fn test_exemption_of_a_doc_at_its_path_is_spent_by_the_next_pass() {
        use crate::core::Occupant;
        let mut s = st();
        let id = uid(3);
        let path = PathBuf::from("p.md");
        s.known_records.observe(alive_record(id, "p.md"));
        s.shadow.set_tracked(&path, id);
        s.exempt_revival.insert(id, "p.md".to_owned());

        let effects = reconcile_placement(&mut s);
        assert!(
            effects.is_empty(),
            "a doc at its path is not a mover, got {effects:?}"
        );
        assert!(
            s.exempt_revival.is_empty(),
            "the pass spent the exemption it had no place to use"
        );

        // The file leaves locally (shadow vacated) and a foreign untracked
        // file appears at the same path; the doc is a mover onto it again.
        s.shadow.remove_fold(&path);
        s.shadow
            .shadow_occupant
            .insert(crate::core::casefold(&path), Occupant::Untracked);
        let effects = reconcile_placement(&mut s);
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::GuardedPlace { .. })),
            "the mover defers behind the foreign file, got {effects:?}"
        );
        assert!(
            s.deferred.contains_key(&id.to_string()),
            "the blocked mover is recorded as deferred"
        );
    }
}
