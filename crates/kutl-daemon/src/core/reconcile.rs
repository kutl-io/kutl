//! `reconcile_placement`: placement as a confluent lattice projection (gamma).
//!
//! Phase 4 replaces the Phase-3 `Subscribe`-emitting stub with the gamma
//! recompute: diff the desired `id → effective_path` assignment (the pure
//! [`desired_assignment`] mirror of `RegistryLattice::arbitrate`, `registry.rs:88`)
//! against the in-memory [`DiskShadow`](crate::core::DiskShadow), and emit a
//! [`Effect::GuardedPlace`] for every **mover** (a doc whose desired path differs
//! from where it currently sits on disk). Movers are ordered `(path_hlc, id)` so a
//! displaced loser (which resolves to a collision-free conflict path) is vacated
//! BEFORE the winner places onto the freed path — the same order the relay
//! arbitrates, so daemon and relay agree by construction (the §7.1 key).
//!
//! This is NOT a procedural worklist (it replaces the former imperative chain
//! `drain_pending → materialize_at → place_rename → reconcile_disk_for_rename →
//! conform_or_materialize_at`): each pass is a pure
//! recompute from `known_records` + the shadow, so a displaced loser's correction
//! is a function of who else currently claims its path, never of arrival order.
//!
//! Graft 2 (Task 3) makes `deferred` a DERIVED set: each pass clears and rebuilds
//! `state.deferred`, so a doc that left `known_records` (a tombstone) is no longer
//! a mover and so leaves no stale entry to resurrect — the ghost-resurrection
//! class is structural. A mover whose desired path is held by an [`Occupant::Untracked`]
//! DEFERS (records in `deferred`, emits no `GuardedPlace`) UNLESS it is
//! revival-exempt (`state.exempt_revival`, seeded from `was_seen_before` before the
//! HLC fold), in which case it places onto its own orphan. The `expected_free` flag
//! carries the core's belief so the shell can assert the TOCTOU window stayed shut
//! (graft 1, Task 4).
//!
//! No `std::fs`, no `.await`, no `mpsc`, no clock read.

use std::collections::HashMap;

use kutl_core::lattice::RegistryLattice;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::SafeRelayPath;
use crate::core::{Effect, Occupant, PlaceKind, SpaceState, rel_path_to_string};

/// The desired `id → effective_path` map for all alive documents. Pure function
/// of the record set, so daemon and relay compute the same result for the same
/// inputs (the §7.1-critical agreement). Built by inverting
/// `RegistryLattice::effective_path_index` (`registry.rs:175`), which already
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
/// elects (`registry.rs:102`), so daemon and relay reconcile to the same disk
/// state regardless of who observed events first (the §7.1 reproducibility key).
///
/// The fork mirrors the former imperative `materialize_at`: a mover this daemon
/// already tracks on disk (`shadow_path` has it) emits
/// [`PlaceKind::Rename`] (vacate the old path, move our held file); an untracked
/// mover emits [`PlaceKind::Register`] (the content streams in via the
/// shell-emitted `Subscribe` on the place's ACK — `place_now`, daemon.rs).
///
/// graft 2: a mover whose desired path is held by an [`Occupant::Untracked`]
/// DEFERS (recorded in `state.deferred`, no `GuardedPlace` emitted) unless it is
/// revival-exempt (`state.exempt_revival`). `state.deferred` is CLEARED and rebuilt
/// every pass, so it is a pure function of `known_records` + the shadow + the
/// exemption set — never a procedural worklist. A doc that left `known_records` (a
/// tombstone) is not in `desired`, so it is not a mover and leaves no stale
/// deferral: the resurrection bug is structural.
///
/// `expected_free` is the core's belief, read from the shadow, that the target
/// holds no untracked occupant — graft 1's TOCTOU assertion the shell checks
/// inside its atomic stat-and-place critical section. A revival placing onto its
/// OWN untracked orphan emits `expected_free = false` (it knows the occupant is
/// there and adopts it); the shell sees the agreement and places. A normal mover
/// onto a free path emits `expected_free = true`; if the shell's atomic stat then
/// finds an occupant, that disagreement is the TOCTOU race graft 1 catches.
///
/// The shadow is updated only on shell-ACK of a landed disk effect (graft 3), so a
/// re-run after the place lands sees `shadow_path == desired` for that doc and
/// emits nothing — the recompute is idempotent at the fixpoint.
pub(crate) fn reconcile_placement(state: &mut SpaceState) -> Vec<Effect> {
    let desired = desired_assignment(&state.known_records);

    // `deferred` is DERIVED, not a worklist: clear it and rebuild from this pass's
    // movers, so a doc that left `known_records` (a tombstone) cannot leave a stale
    // entry to resurrect (graft 2 / the ghost-resurrection class).
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

    // Order by (path_hlc, id) — the relay's `arbitrate` election key
    // (registry.rs:102) — so a loser-to-conflict move is emitted before the winner
    // that takes its freed path. Reusing the lattice's own key is why gamma gives
    // §7.1 reproducibility for free: there is no second copy of the order to drift.
    movers.sort_by(|(ida, _), (idb, _)| {
        let key = |id: &Uuid| {
            (
                state
                    .known_records
                    .get(id)
                    .and_then(kutl_core::lattice::DocRecord::path_hlc),
                *id,
            )
        };
        key(ida).cmp(&key(idb))
    });

    let mut effects = Vec::new();
    for (id, want) in movers {
        let Some(target) = safe_target(&want) else {
            // An unparseable desired path can't be placed; the boundary handlers
            // already reject invalid relay paths, so this is unreachable for a
            // real record set. Skip rather than panic.
            warn!(%id, %want, "skipping mover with unparseable desired path");
            continue;
        };

        // graft 2: defer iff the desired path is held by an Untracked occupant AND
        // this doc is NOT revival-exempt. The shadow occupant is keyed case-folded
        // (matching arbitrate's `.to_lowercase()`, registry.rs:92).
        //
        // STALENESS: an `Untracked` marker is overridden when `file_identity`
        // claims the target — the pure twin of the shell's `stat_untracked`
        // precedence: an identity-claimed path is not a foreign untracked
        // occupant. The marker goes stale when a graft-1 disagreement races
        // the local create's mint; the mint now also folds `Tracked` straight
        // over the marker (shadow-at-mint) and subscribes explicitly, so this
        // override is defense in depth for any identity-claimed-but-unfolded
        // window rather than the sole guard. Historically its absence
        // stranded the minted doc deferred on its OWN file forever — never
        // placed, and never subscribed back when the `Subscribe` rode the
        // place ACK — so the winner-doc's post-conflict edits silently never
        // arrived (the f_conflicts create/create divergence).
        let occupant = state.shadow.shadow_occupant.get(&want.to_lowercase());
        let held_by_untracked = crate::core::shadow::held_by_foreign_untracked(
            occupant,
            // The pure core's only disk evidence is its shadow belief: an
            // Untracked marker says a foreign file is present.
            matches!(occupant, Some(Occupant::Untracked)),
            state.file_identity.contains_key(&target),
        );
        let exempt = state.exempt_revival.contains(&id);
        // A revival exemption can only apply to a doc the cascade is actually
        // placing — i.e. one that is in `known_records` (it is a mover, so it must
        // be). This guards against an exemption set for an id with no record (a
        // drift that would silently clobber an untracked occupant).
        debug_assert!(
            !exempt || state.known_records.get(&id).is_some(),
            "a revival exemption must correspond to a known record"
        );
        if held_by_untracked && !exempt {
            // Defer: record the path we wait on and emit NO place — the occupant
            // must vacate first (the relay issues its move, then a later reconcile
            // re-derives this doc as a now-placeable mover).
            debug!(%id, target = %want, "placement deferred: target held by untracked occupant");
            if let Ok(safe) = SafeRelayPath::new(&want) {
                state.deferred.insert(id.to_string(), safe);
            }
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
    // Pass summary, only when the pass decided something — the fixpoint (no
    // movers, nothing deferred) stays silent so a periodic MetricsTick reconcile
    // does not flood the log.
    if !effects.is_empty() || !state.deferred.is_empty() {
        debug!(
            places = effects.len(),
            deferred = state.deferred.len(),
            "reconcile pass"
        );
    }
    effects
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
        DocRecord {
            document_id: id,
            path: path.into(),
            registered_hlc: Some(registered),
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: None,
            displaced: false,
        }
    }

    /// `desired_assignment` must produce exactly the inverse of
    /// `effective_path_index` — same winner at a contended path, same
    /// conflict-path resolution for the loser — for ANY record set. This is
    /// the agreement with the relay's `arbitrate` (registry.rs:88) that §7.1
    /// reproducibility depends on.
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
    /// resolve to a path), mirroring `effective_path_index`'s `is_alive` filter
    /// (registry.rs:178).
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
        DocRecord {
            document_id: id,
            path: path.to_owned(),
            registered_hlc: Some(Hlc {
                physical_ms: 1,
                logical: 0,
                actor: ActorId(uid(99)),
            }),
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: None,
            displaced: false,
        }
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
    /// not an untracked occupant. The marker goes stale when a graft-1
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
            },
        );
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

    /// The FS-1 defer is unchanged when NO identity claims the marked target: a
    /// genuinely untracked occupant (a foreign create, not yet minted) still
    /// defers a non-exempt mover — the graft-2 no-clobber contract.
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
        // §7.1: two docs collide on "a.md"; the loser (lower path_hlc) is displaced
        // to its conflict path and MUST be emitted BEFORE the winner that takes the
        // freed path — the (path_hlc, id) order reused from arbitrate (registry.rs:102).
        let mut s = st();
        let lose = uid(1);
        let win = uid(2);
        // Same path; win has the higher path_hlc so it owns "a.md".
        s.known_records.observe(DocRecord {
            document_id: lose,
            path: "a.md".to_owned(),
            registered_hlc: Some(Hlc {
                physical_ms: 5,
                logical: 0,
                actor: ActorId(uid(7)),
            }),
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: None,
            displaced: false,
        });
        s.known_records.observe(DocRecord {
            document_id: win,
            path: "a.md".to_owned(),
            registered_hlc: Some(Hlc {
                physical_ms: 9,
                logical: 0,
                actor: ActorId(uid(7)),
            }),
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: None,
            displaced: false,
        });
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
}
