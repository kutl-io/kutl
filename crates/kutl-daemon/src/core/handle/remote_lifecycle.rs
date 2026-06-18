//! Remote lifecycle handlers: register/rename/unregister and the typed
//! lifecycle ack (RFD 0042).
//!
//! The four remote lifecycle arms (register/rename/unregister/ack) preserve the
//! load-bearing HLC gating asymmetry: register is fold-only (NOT gated — a
//! revival must always apply), while rename/unregister/ack are freshness-gated
//! via [`SpaceState::lifecycle_event_is_fresh`].

use kutl_core::Hlc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::helpers::{
    causal_floor_for, cleanup_document_state, confirm_document, move_identity, parsed_id,
    register_identity_remote, seed_record, seed_remote_record, unregister_identity,
};
use crate::SafeRelayPath;
use crate::core::{Effect, EventStamp, SpaceState, reconcile_placement, rel_path_to_string};

/// Port of the former imperative `handle_remote_register`: a document another daemon
/// registered. **Fold-only, NOT freshness-gated** — see the asymmetry note on
/// [`SpaceState::lifecycle_event_is_fresh`].
///
/// A register is a creation/revival, never a competitor to drop: it either mints
/// a brand-new id (no prior watermark) or re-asserts a known one (a REVIVAL,
/// which MUST apply). So unlike rename/unregister it is NOT gated on freshness —
/// gating risks dropping a revival whose origin stamp is older than the loser's
/// recorded delete watermark. `is_revival` is captured BEFORE the HLC fold below,
/// which would otherwise make every register look already-known.
/// The fold advances the per-doc watermark (monotonic max) so
/// a later genuinely-stale rename/delete is dropped, but we do NOT `recv` it into
/// the clock (a register stamp can carry a peer's wall-clock skew).
pub(super) fn handle_remote_register(
    state: &mut SpaceState,
    document_id: &str,
    path: &str,
    stamp: EventStamp,
) -> Vec<Effect> {
    let Ok(safe_path) = SafeRelayPath::new(path) else {
        // Ignore a registration with an invalid path.
        warn!(%document_id, %path, "ignoring remote registration with invalid path");
        return Vec::new();
    };

    // REVIVAL detection MUST be read before the fold: folding
    // first would record this register's stamp and make every register look
    // already-known. A register for a doc we have already applied a lifecycle op
    // for is a REVIVAL (its delete lost to a concurrent edit; the relay re-asserts
    // it at its OWN path), which is exempt from path-collision deferral.
    let was_seen_before = state.has_applied_lifecycle(document_id);
    if was_seen_before && let Ok(id) = Uuid::parse_str(document_id) {
        // graft 2: thread the exemption into the cascade's deferral predicate
        // (the port of the former `defer_if_occupied(exempt_revival)`). The set
        // is per-pass — cleared at the top of `handle` — so this only exempts the
        // reconcile THIS register triggers below.
        state.exempt_revival.insert(id);
    }

    // Fold-only (not gated, not recv'd into the clock).
    if let Some(hlc) = stamp.origin_hlc {
        state.record_lifecycle_hlc(document_id, hlc);
        // Record the register stamp specifically — the persisted causal-floor
        // source for a later local rename of this doc (including an OFFLINE
        // rename re-emitted after a restart, which carries no other causal
        // proof over this registration). Mirrors the procedural
        // `handle_remote_register` (daemon.rs).
        state.record_register_hlc(document_id, hlc);
    }

    if state.uuid_to_path.contains_key(document_id) {
        // Already tracked (e.g. our own create echoed back) — record that the
        // relay has now acknowledged it. The cascade re-derives
        // placement from the unchanged record set. Persist the confirm flip so it
        // survives a restart (the imperative `confirm_document` save_states here too).
        let mut effects = Vec::new();
        if confirm_document(state, document_id) {
            effects.push(Effect::SaveState);
        }
        effects.extend(reconcile_placement(state));
        return effects;
    }

    // Fold the remote registration into `known_records` so the cascade places it.
    // The path-arbitration conflict-copy deferral the procedural
    // `defer_if_occupied`/`place_register` did is the cascade's
    // job: `reconcile_placement` derives defer-vs-place from `known_records`, the
    // shadow, and `exempt_revival` (seeded above from `was_seen_before`).
    //
    // IDENTITY-FOLLOWS-PLACEMENT (carried concern 1, FS-1/FS-2): we do NOT claim
    // identity (`file_identity`/`uuid_to_path`) here — the eventual placement does
    // (the shell's `GuardedPlace(Register)`, which the real daemon claims in
    // `place_now` — formerly `place_register`'s NOT-deferred branch). Claiming
    // eagerly re-introduces TWO conflations: a DEFERRED register leaves a stale
    // path-claim the startup scan resolves back onto (FS-2 — two authors merged into
    // one), and a PLACING register masks a foreign file racing onto the same path
    // (FS-1 — the atomic stat can no longer tell our own placement's file from a
    // concurrent local create, so the local bytes get clobbered). Seeding ONLY the
    // record keeps the lattice driving the cascade while identity rides the place.
    let rel = safe_path.into_path_buf();
    let registered_hlc = stamp.origin_hlc;
    seed_remote_record(state, document_id, &rel, registered_hlc, None, None, None);
    reconcile_placement(state)
}

/// Port of the former imperative `handle_remote_rename`: a document another daemon
/// renamed. **Freshness-gated** (drop via
/// [`SpaceState::lifecycle_event_is_fresh`]): a causally-older rename is a stale
/// echo of a superseded op and is dropped.
///
/// On accept: move the identity to the authoritative path (or register it if we
/// don't track it yet) and fold the rename into `known_records`. The disk move is
/// gamma's: `reconcile_placement` derives a `GuardedPlace(Rename)` for the doc (and
/// re-arbitrates any collision the new path created) in `(path_hlc, id)` order — we
/// do NOT emit a collision-blind `Effect::RenameFile` that would race it.
pub(super) fn handle_remote_rename(
    state: &mut SpaceState,
    document_id: &str,
    // The sender's advisory old name. The move resolves the real
    // local source by id from `uuid_to_path`, so the advisory name is unused here
    // — a concurrent rename/rename means our local path has already diverged from
    // it (the §1.3 rename/rename split).
    _old_path: &str,
    new_path: &str,
    // The renamer's causal floor (the register HLC it observed); folded so this
    // daemon's placement lattice treats the rename as causally-after a skewed
    // registration. `None` from a pre-floor peer.
    rename_causal_floor: Option<Hlc>,
    stamp: EventStamp,
) -> Vec<Effect> {
    let Ok(new_safe) = SafeRelayPath::new(new_path) else {
        warn!(%document_id, %new_path, "ignoring remote rename with invalid path");
        return Vec::new();
    };

    // Freshness gate: drop a causally-older rename (and advance the clock past
    // it) — floor-aware: a rename stamped below this doc's watermark but
    // floored at-or-above it observed the very registration the watermark
    // records, so it is causally after it (see `rename_event_is_fresh`; the
    // combined-stressors third-observer lost rename).
    if !state.rename_event_is_fresh(
        document_id,
        stamp.origin_hlc,
        rename_causal_floor,
        stamp.wall_ms,
    ) {
        // The watermark is unchanged by a drop, so this reads the very stamp the
        // gate compared against.
        debug!(
            %document_id,
            incoming = ?stamp.origin_hlc,
            floor = ?rename_causal_floor,
            watermark = ?state.lifecycle_hlc.get(document_id),
            "gate drop: stale remote rename"
        );
        return Vec::new();
    }

    let new_rel = new_safe.into_path_buf();
    let renamed_hlc = stamp.origin_hlc;

    // `old_path` is the sender's advisory old name; the move resolves the real
    // local source by id, so we read the path WE hold for the id.
    let local_old = state.uuid_to_path.get(document_id).cloned();
    if local_old.is_some() {
        // Tracked: fold ONLY the lattice record. IDENTITY-FOLLOWS-PLACEMENT: we do
        // NOT eagerly `move_identity` here — the cascade's `GuardedPlace(Rename)`
        // claims identity at the authoritative path when (and only when) it actually
        // moves the file (`place_now`, daemon.rs).
        //
        // Eagerly moving identity is unsound when the cascade's occupied-target guard
        // then SKIPS the disk move (a concurrent rename/create already holds `new`):
        // the identity would point at `new` while our file is still at the old path.
        // A subsequent LOCAL create at `new` is then mis-seen as an EDIT of THIS
        // document (its path is tracked) instead of a new document, and the colliding
        // create is silently lost (the §2.3 rename-onto-concurrent-create flake).
        // Leaving identity at the old path until placement keeps `file_identity`
        // consistent with the disk, mirroring `handle_remote_register` (carried
        // concern 1). gamma OWNS the disk move — no `Effect::RenameFile` here.
        seed_remote_record(
            state,
            document_id,
            &new_rel,
            None,
            renamed_hlc,
            rename_causal_floor,
            None,
        );
    } else {
        // Untracked here: we hold no file or CRDT to move, so register the
        // identity and let the content stream in to land at the path
        // (the former `materialize_at`'s untracked branch).
        //
        // DELIBERATE EXCEPTION to identity-follows-placement (carried
        // concern 1, stated at `handle_remote_register`): this claim happens
        // BEFORE any placement. It is safe precisely because the doc is
        // untracked here — there is no local file whose adoption could merge
        // two authors (FS-2) and no placement whose atomic stat the claim
        // could mask (FS-1); the claim is what lets the streaming content
        // resolve a path at all. Every other identity claim rides the
        // placement ACK. Do not generalize from this site.
        //
        // Fold the rename AS a rename (with its floor), mirroring the relay's
        // own fold (`handle_rename_document` → `registry.rename`) — NOT as a
        // register at the rename's stamp. "Untracked" keys on identity, which
        // follows PLACEMENT; `known_records` frequently already holds the doc's
        // (possibly clock-skewed) register from the fold-only broadcast — e.g.
        // a doc deferred behind a collision. Folding the rename as a low-stamp
        // register fragment would lose the path arbitration to that skewed
        // register on max-merge, silently keeping the old path (the same
        // third-observer lost rename the gate's floor arm closes, one step
        // left). A doc with NO prior record still resolves: a rename-only
        // record wins its path via `path_priority`'s rename arm.
        register_identity_remote(state, &new_rel, document_id);
        seed_remote_record(
            state,
            document_id,
            &new_rel,
            None,
            renamed_hlc,
            rename_causal_floor,
            None,
        );
    }
    reconcile_placement(state)
}

/// Port of the former imperative `handle_remote_unregister`: a document another
/// daemon deleted. **Freshness-gated** — a delete superseded by a newer lifecycle
/// op (a concurrent rename that won, keeping the document alive) is dropped.
///
/// On accept: emit `Effect::RemoveFile` to delete our held file (`remove_doc`,
/// daemon.rs), clear local state, fold the delete into `known_records` as a
/// tombstone, and drop the identity. The cascade re-derives placement.
pub(super) fn handle_remote_unregister(
    state: &mut SpaceState,
    document_id: &str,
    stamp: EventStamp,
) -> Vec<Effect> {
    // Freshness gate: drop a stale (superseded) delete (and advance the clock).
    if !state.lifecycle_event_is_fresh(document_id, stamp.origin_hlc, stamp.wall_ms) {
        debug!(
            %document_id,
            incoming = ?stamp.origin_hlc,
            watermark = ?state.lifecycle_hlc.get(document_id),
            "gate drop: stale remote unregister"
        );
        return Vec::new();
    }

    let deleted_hlc = stamp.origin_hlc;
    // CARRIED CONCERN 2 (displaced-unregister-orphan): the file to remove is at the
    // doc's CURRENT on-disk location, which for a DISPLACED doc is its conflict path
    // (the cascade moved it there via `GuardedPlace(Rename)`; `shadow_path` tracks
    // that), NOT the `uuid_to_path` entry, which the pure cascade never rewrites on a
    // displacement (it is a projection, not a `move_identity`). Removing
    // `uuid_to_path[id]` would delete the WINNER now sitting at the original path and
    // orphan the loser's conflict file. Prefer the shadow's live path; fall back to
    // `uuid_to_path` for a doc this daemon never placed (no shadow entry).
    let freed = state.doc_disk_path(document_id);
    let mut effects = Vec::new();
    if let Some(rel) = &freed {
        // Delete the file from disk (remove_doc → RemoveFile, which
        // suppresses the removal echo with no hash). The shadow is NOT vacated
        // here: like every other disk effect, the vacate folds on shell-ACK
        // (`apply_remove` reports `FileRemoved` even when the file was already
        // gone — the goal state is met either way — and `remove_fold` clears
        // occupant + shadow_path + inode). Folding on EMIT here was the one
        // exception to the "shadow updates only on shell-ACK" invariant, and
        // it is not load-bearing: the tombstoned doc is not a mover, a winner
        // taking the freed path sees a `Tracked` occupant (never a defer), and
        // the driver applies this `RemoveFile` before any same-pass place, so
        // the disk is vacated by the time a place executes.
        effects.push(Effect::RemoveFile { rel: rel.clone() });
        cleanup_document_state(state, rel);
    }
    // ALWAYS tombstone the doc in `known_records`, regardless of whether a file was
    // on disk to remove. A DEFERRED register (carried concern 1) never claimed a
    // path, so `freed` is None — but the lattice record IS alive (the cascade was
    // deferring it), so the delete MUST fold `deleted_hlc` to make it a tombstone;
    // otherwise the unregistered-but-never-placed doc stays an alive mover and the
    // resurrection guard (`37093896`) leaks. The intended path for the tombstone is
    // the freed path if known, else the doc's current intended path from the record
    // (a delete doesn't change `current_path`; the lattice carries it forward).
    let tombstone_path = freed.map(|rel| rel_path_to_string(&rel)).or_else(|| {
        state
            .known_records
            .get(&parsed_id(document_id))
            .map(|r| r.current_path().to_owned())
    });
    if let Some(path) = tombstone_path {
        seed_record(state, document_id, &path, None, None, None, deleted_hlc);
    }
    // The identity drop rewrites the persisted document map in memory; emit the
    // coalesced persist so state.json is not stale across a crash (the
    // imperative twin saves inline — the `4bfaa9c9` dropped-effect class;
    // surfaced by the tree-delete e2e reading a stale on-disk map on the
    // observer). Gated on the doc actually having been tracked so a stale
    // duplicate echo does not churn disk writes.
    if state.uuid_to_path.contains_key(document_id) {
        effects.push(Effect::SaveState);
    }
    unregister_identity(state, document_id);
    effects.extend(reconcile_placement(state));
    effects
}

/// Port of the former imperative `handle_lifecycle_ack`: the relay acknowledged THIS
/// daemon's own register/rename (RFD 0042 typed-ack rail). Confirms the document,
/// then — only when the ack signals we LOST a path collision (the effective path
/// is a conflict path) — self-corrects by reapplying the rename to that conflict
/// path. **Freshness-gated** (after classification, before the mutating
/// apply): a late/reordered ack must not move the document against a
/// causally-newer rename already applied.
pub(super) fn handle_lifecycle_ack(
    state: &mut SpaceState,
    document_id: &str,
    effective_path: Option<&str>,
    stamp: EventStamp,
) -> Vec<Effect> {
    let Some(effective) = effective_path else {
        // `effective_path` is `None` on a failed ack — nothing to confirm or move.
        debug!(%document_id, "lifecycle ack with no effective path (failed op); ignoring");
        return Vec::new();
    };

    // A successful ack confirms our own create — the ONLY signal a registrant gets
    // (the lifecycle broadcast excludes the sender). Confirm is monotone, so a
    // rename ack re-confirming is a no-op. Persist the flip so the
    // confirm survives a restart, on EVERY return path below (the imperative
    // `confirm_document` save_state'd on the same flip).
    let mut effects = Vec::new();
    if confirm_document(state, document_id) {
        effects.push(Effect::SaveState);
    }

    // Classify: the (old, new) move only when we LOST a collision (effective is a
    // conflict path) AND we still track the doc at a different path. `None` for
    // every no-op (won op, untracked, already reconciled, unparseable).
    let Some(new_rel) = ack_self_correction_target(state, document_id, effective) else {
        return effects;
    };

    // Freshness gate AFTER classification, BEFORE the mutating apply:
    // drop a stale/duplicate ack, advancing the clock past it.
    if !state.lifecycle_event_is_fresh(document_id, stamp.origin_hlc, stamp.wall_ms) {
        debug!(
            %document_id,
            incoming = ?stamp.origin_hlc,
            watermark = ?state.lifecycle_hlc.get(document_id),
            "gate drop: stale lifecycle ack"
        );
        return effects;
    }

    // Self-correct: move our document to the arbitrated conflict path. This is the
    // same fold as a remote rename (the imperative ack path reused the former
    // `handle_remote_rename`); the freshness gate already advanced the watermark, so seed
    // the record with the ack's stamp as the rename hlc. gamma OWNS the disk move:
    // the cascade derives the `GuardedPlace(Rename)` onto the conflict path below
    // (no separate `Effect::RenameFile`, which would race the arbitrated order).
    let renamed_hlc = stamp.origin_hlc;
    // The self-correction is a conflict-copy rename of our OWN doc; carry our
    // recorded registration as the floor so it supersedes the register branch
    // consistently (mirroring a local rename's floor).
    let rename_causal_floor = causal_floor_for(state, document_id);
    if let Some(old_rel) = state.uuid_to_path.get(document_id).cloned() {
        info!(
            %document_id,
            old = %old_rel.display(),
            new = %new_rel.display(),
            "ack self-correction: lost path collision, relocating to conflict path"
        );
        move_identity(state, &old_rel, new_rel.clone(), document_id);
        seed_remote_record(
            state,
            document_id,
            &new_rel,
            None,
            renamed_hlc,
            rename_causal_floor,
            None,
        );
    }
    effects.extend(reconcile_placement(state));
    effects
}

/// Classify a register/rename ack: the safe path to move our OWN document to, but
/// ONLY when the ack signals we LOST a path collision (the effective path is a
/// conflict path) AND the document is tracked here AND not already at that path.
/// `None` for every no-op case (won op, untracked, already reconciled,
/// unparseable). Pure (reads + a constructor); the caller applies the gate + move.
///
/// Port of the former imperative `ack_requires_self_correction`; returns only the
/// `new` target — the `old` source is resolved from `uuid_to_path` by the caller
/// (the local move is reconciled by id, not the advisory old path).
fn ack_self_correction_target(
    state: &SpaceState,
    document_id: &str,
    effective: &str,
) -> Option<std::path::PathBuf> {
    if !effective.contains(kutl_core::lattice::CONFLICT_INFIX) {
        return None;
    }
    let current = state.uuid_to_path.get(document_id)?; // not tracked → nothing of ours
    if rel_path_to_string(current) == effective {
        return None; // already reconciled to the conflict path
    }
    SafeRelayPath::new(effective)
        .ok()
        .map(SafeRelayPath::into_path_buf)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::core::handle::register_identity;
    use crate::core::handle::test_support::{doc_id, hlc, st, track_remote};
    use crate::core::{DaemonCore, Event};

    #[test]
    fn test_remote_rename_floored_past_register_watermark_applies() {
        // The combined-stressors lost-rename (third-observer drop): a remote
        // REGISTER folds its (clock-skewed, future) HLC into this observer's
        // lifecycle watermark; a peer then renames that doc with a stamp BELOW
        // the watermark but carrying `rename_causal_floor` == the register
        // stamp it observed. The freshness gate must apply the supersession
        // rule of `DocRecord::path_priority` (`renamed >= applied OR floor >=
        // applied`) — without the floor arm the observer silently drops the
        // very rename the relay's lattice accepts: registrant (which records
        // no watermark at mint) and renamer converge to the new path while
        // every third worker keeps the old one.
        let mut s = st();
        let id = doc_id();
        let registrant = kutl_core::hlc::ActorId(Uuid::from_u128(0xA));
        let renamer = kutl_core::hlc::ActorId(Uuid::from_u128(0xB));
        let register = hlc(registrant, 5000); // skewed: far above the renamer's clock
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteRegister {
                document_id: id.clone(),
                path: "skew.md".into(),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: Some(register),
                },
            },
        );
        // The cross-worker rename: stamped below the watermark, floored at it.
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteRename {
                document_id: id.clone(),
                old_path: "skew.md".into(),
                new_path: "moved.md".into(),
                rename_causal_floor: Some(register),
                stamp: EventStamp {
                    wall_ms: 200,
                    origin_hlc: Some(hlc(renamer, 200)),
                },
            },
        );
        let uid = Uuid::parse_str(&id).expect("doc id is a uuid");
        assert_eq!(
            crate::core::desired_assignment(&s.known_records)
                .get(&uid)
                .map(String::as_str),
            Some("moved.md"),
            "a floored rename past a skewed register watermark must apply"
        );
    }

    #[test]
    fn test_remote_rename_floored_applies_on_tracked_observer() {
        // The TRACKED-arm twin of the floored-rename case — the exact shape of
        // the combined-stressors third observer: the doc was PLACED here
        // (identity claimed), the skewed register watermark is folded, and the
        // peer's floored rename must re-derive the placement to the new path.
        let mut s = st();
        let id = doc_id();
        let registrant = kutl_core::hlc::ActorId(Uuid::from_u128(0xA));
        let renamer = kutl_core::hlc::ActorId(Uuid::from_u128(0xB));
        let register = hlc(registrant, 5000);
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteRegister {
                document_id: id.clone(),
                path: "skew.md".into(),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: Some(register),
                },
            },
        );
        // The placement landed: identity claimed + shadow tracked (place_now).
        register_identity(&mut s, Path::new("skew.md"), id.clone(), true);
        let uid = Uuid::parse_str(&id).expect("doc id is a uuid");
        s.shadow.set_tracked(Path::new("skew.md"), uid);

        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteRename {
                document_id: id.clone(),
                old_path: "skew.md".into(),
                new_path: "moved.md".into(),
                rename_causal_floor: Some(register),
                stamp: EventStamp {
                    wall_ms: 200,
                    origin_hlc: Some(hlc(renamer, 200)),
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { id: gid, target, place_kind, .. }
                    if *gid == id
                        && target.as_path() == Path::new("moved.md")
                        && matches!(place_kind, crate::core::PlaceKind::Rename { old_rel } if old_rel.as_path() == Path::new("skew.md"))
            )),
            "the tracked observer must move its held file to the renamed path, got {effects:?}"
        );
    }

    #[test]
    fn test_remote_rename_floor_below_watermark_still_drops() {
        // The stale-rename-vs-revival rule survives the floor arm: a rename
        // whose floor is the OLD register loses to a NEWER lifecycle watermark
        // (a revival's register), exactly as `path_priority` orders it.
        let mut s = st();
        let id = doc_id();
        let registrant = kutl_core::hlc::ActorId(Uuid::from_u128(0xA));
        let renamer = kutl_core::hlc::ActorId(Uuid::from_u128(0xB));
        // The REVIVAL register: watermark at 6000.
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteRegister {
                document_id: id.clone(),
                path: "alive.md".into(),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: Some(hlc(registrant, 6000)),
                },
            },
        );
        // A stale rename from before the revival: floor = the OLD register (5000).
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteRename {
                document_id: id.clone(),
                old_path: "alive.md".into(),
                new_path: "stale.md".into(),
                rename_causal_floor: Some(hlc(registrant, 5000)),
                stamp: EventStamp {
                    wall_ms: 200,
                    origin_hlc: Some(hlc(renamer, 200)),
                },
            },
        );
        let uid = Uuid::parse_str(&id).expect("doc id is a uuid");
        assert_eq!(
            crate::core::desired_assignment(&s.known_records)
                .get(&uid)
                .map(String::as_str),
            Some("alive.md"),
            "a rename floored below the watermark is stale and must drop"
        );
    }

    #[test]
    fn test_stale_remote_rename_is_dropped_by_lifecycle_gate() {
        // `SpaceState::lifecycle_event_is_fresh` drops a causally-older rename.
        let mut s = st();
        let actor = s.hlc.last().actor;
        let id = doc_id();
        let newer = hlc(actor, 200);
        let older = hlc(actor, 100);
        // Apply the newer rename first so the watermark is high.
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteRename {
                document_id: id.clone(),
                old_path: "a.md".into(),
                new_path: "b.md".into(),
                rename_causal_floor: None,
                stamp: EventStamp {
                    wall_ms: 200,
                    origin_hlc: Some(newer),
                },
            },
        );
        // The older rename is stale: NO further rename/register effect.
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteRename {
                document_id: id,
                old_path: "b.md".into(),
                new_path: "c.md".into(),
                rename_causal_floor: None,
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: Some(older),
                },
            },
        );
        assert!(
            !effects.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { .. } | Effect::RegisterDocument { .. }
            )),
            "stale remote rename must be dropped (no place, no register), got {effects:?}"
        );
    }

    #[test]
    fn test_revival_register_is_exempt_from_freshness_gate() {
        // is_revival captured BEFORE the HLC fold: a register for
        // a doc with an existing watermark still applies (revival never dropped).
        let mut s = st();
        let actor = s.hlc.last().actor;
        let id = doc_id();
        // Seed a prior lifecycle watermark for the doc at a HIGH hlc.
        s.record_lifecycle_hlc(&id, hlc(actor, 500));
        // A register at a LOWER hlc must still place (revival), not be gated.
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteRegister {
                document_id: id,
                path: "a.md".into(),
                stamp: EventStamp {
                    wall_ms: 10,
                    origin_hlc: Some(hlc(actor, 100)),
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { place_kind, .. }
                    if *place_kind == crate::core::PlaceKind::Register
            )),
            "revival register must place via GuardedPlace(Register), got {effects:?}"
        );
    }

    #[test]
    fn test_fresh_remote_rename_of_tracked_doc_emits_guarded_rename() {
        // A fresh remote rename of a doc this daemon tracks moves it on disk via the
        // gamma cascade: the cascade derives a GuardedPlace(Rename a.md→b.md) (the
        // tracked fork of the former `materialize_at`) — NOT a collision-blind
        // imperative Effect::RenameFile.
        let mut s = st();
        let actor = s.hlc.last().actor;
        let id = doc_id();
        // Track the doc at "a.md" first (a remote register places it).
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteRegister {
                document_id: id.clone(),
                path: "a.md".into(),
                stamp: EventStamp {
                    wall_ms: 10,
                    origin_hlc: Some(hlc(actor, 100)),
                },
            },
        );
        // Reflect the placement in the shadow (graft-3 ACK) so the doc sits at "a.md".
        s.shadow
            .set_tracked(&PathBuf::from("a.md"), Uuid::parse_str(&id).unwrap());
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteRename {
                document_id: id,
                old_path: "a.md".into(),
                new_path: "b.md".into(),
                rename_causal_floor: None,
                stamp: EventStamp {
                    wall_ms: 300,
                    origin_hlc: Some(hlc(actor, 300)),
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { target, place_kind, .. }
                    if target.as_path() == Path::new("b.md")
                        && *place_kind == crate::core::PlaceKind::Rename {
                            old_rel: PathBuf::from("a.md"),
                        }
            )),
            "fresh tracked remote rename must derive GuardedPlace(Rename a.md→b.md), got {effects:?}"
        );
        assert_eq!(s.uuid_to_path.get(&doc_id()), Some(&PathBuf::from("b.md")));
    }

    #[test]
    fn test_fresh_remote_unregister_of_tracked_doc_emits_remove_file() {
        // A fresh remote unregister of a tracked doc deletes it on disk:
        // Effect::RemoveFile (remove_doc, daemon.rs) + identity dropped.
        let mut s = st();
        let actor = s.hlc.last().actor;
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteUnregister {
                document_id: id.clone(),
                stamp: EventStamp {
                    wall_ms: 200,
                    origin_hlc: Some(hlc(actor, 200)),
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::RemoveFile { rel } if rel.as_path() == Path::new("a.md")
            )),
            "fresh remote unregister must emit RemoveFile, got {effects:?}"
        );
        assert!(
            !s.uuid_to_path.contains_key(&id),
            "the unregistered doc's identity must be dropped"
        );
    }

    #[test]
    fn test_stale_remote_unregister_is_dropped() {
        // A delete superseded by a newer lifecycle op is dropped (the
        // rename-wins-over-delete convergence).
        let mut s = st();
        let actor = s.hlc.last().actor;
        let id = doc_id();
        s.record_lifecycle_hlc(&id, hlc(actor, 500));
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteUnregister {
                document_id: id,
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: Some(hlc(actor, 100)),
                },
            },
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::RemoveFile { .. })),
            "a stale remote unregister must not delete, got {effects:?}"
        );
    }
}
