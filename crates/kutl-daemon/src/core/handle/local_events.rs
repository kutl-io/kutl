//! Local file-event handlers: the watcher's modify/remove/rename arms and the
//! shared register-and-subscribe mint.

use kutl_core::Hlc;
use tracing::{debug, error};
use uuid::Uuid;

use super::blob::blob_change;
use super::helpers::{
    causal_floor_for, cleanup_document_state, diff_into_crdt, get_or_create_uuid,
    make_lifecycle_metadata, move_identity, observe_unregister_remote, register_document_effect,
    seed_remote_record, unregister_identity,
};
use crate::core::{Effect, EventStamp, SpaceState, reconcile_placement, rel_path_to_string};

/// Port of `handle_file_modified` (daemon.rs). The watcher already read
/// the bytes at the edge, so `content` carries them: `Some(text)` is the text
/// path, `None` is the binary/blob path (`handle_file_modified`'s `InvalidData`
/// branch).
///
/// CRDT NOTE: the in-memory CRDT store now lives in `SpaceState::documents`
/// (Task 5b). An edit of an already-tracked text doc diffs the new bytes into the
/// CRDT and ships the delta purely (`diff_into_crdt`, ported in Task 5c); the
/// new-doc registration is the piece Task 5 ships. The blob hash/compare
/// (`handle_blob_change`, daemon.rs) stays on the imperative driver until the blob handler ports
/// (the driver cuts over in Task 6b).
pub(super) fn handle_file_modified(
    state: &mut SpaceState,
    rel: &std::path::Path,
    content: Option<&[u8]>,
    stamp: EventStamp,
) -> Vec<Effect> {
    // Defensive: the driver always carries the bytes it read at the edge.
    let Some(content) = content else {
        return Vec::new();
    };

    // A path the daemon has never tracked is a new document. (Inode-rename and
    // overwrite-rename detection — `try_local_inode_rename`/`try_overwrite_rename`,
    // daemon.rs — are driver concerns: they read live inodes.)
    let was_tracked = state.file_identity.contains_key(rel);
    if was_tracked {
        // An edit of an already-tracked doc. CONTENT-MODE DETECTION (the
        // allowed blob residue): valid UTF-8 diffs into the CRDT and ships the
        // delta; anything else is the blob LWW path — same identity, same
        // funnel, different content rule.
        let Some(document_id) = state
            .file_identity
            .get(rel)
            .map(|id| id.document_uuid.clone())
        else {
            return Vec::new();
        };
        if let Ok(text) = std::str::from_utf8(content) {
            let mut effects = Vec::new();
            if state.get_doc(&document_id).is_none() {
                // First text content for a tracked-but-never-synced identity
                // (a pre-seeded state.json entry whose file appears after
                // startup — the truth table's leave-alone row, the TEXT twin
                // of the blob pre-seed case): no CRDT has ever been loaded or
                // created for the doc, so nothing has registered or
                // subscribed it. The former imperative handler keyed
                // register+subscribe on exactly this get_doc miss; without it
                // the ops ship to a relay that never registered the doc and
                // the daemon never hears peers' edits.
                effects.extend(mint_register_subscribe(state, &document_id, rel, stamp));
            }
            effects.extend(diff_into_crdt(state, &document_id, rel, text));
            return effects;
        }
        let mut effects = Vec::new();
        if state.blob_state.get(rel).is_none() {
            // First blob content for a tracked-but-never-synced identity: a
            // pre-seeded state.json entry whose file appears after startup
            // takes the truth table's (no-relay, no-disk, never-remote)
            // leave-alone row, so nothing has registered or subscribed it
            // yet. The former handle_blob_change keyed register+subscribe on
            // this exact blob_state miss; without it the relay never delivers
            // peers' blobs for the doc.
            effects.extend(mint_register_subscribe(state, &document_id, rel, stamp));
        }
        effects.extend(blob_change(state, &document_id, rel, content, stamp));
        return effects;
    }

    // Case-collision guard (mirrors `handle_file_modified`, daemon.rs): a new file
    // that is a case-variant of
    // a tracked document is rejected — the filesystem holds both, we do not sync.
    // Borrow the tracked paths directly — cloning the whole index per
    // untracked-file event was pure allocation (perf §3.1).
    if let Some(existing) = crate::case_collision::find_case_variant(
        rel,
        state.file_identity.keys().map(std::path::PathBuf::as_path),
    ) {
        error!(
            new_path = %rel.display(),
            existing_path = %existing.display(),
            "case_collision_rejected: new file would collide with tracked document, skipping"
        );
        return Vec::new();
    }

    // Conflict-copy namespace guard: a `<name>.kutl-conflict-<id>` path is a
    // LOCAL materialization artifact of a displaced document, never an
    // authored file — the relay refuses to register the namespace outright,
    // so minting a fresh document here would wedge every later sync on a
    // rejection the relay is guaranteed to send. An untracked file in the
    // namespace (a leftover artifact, or a user-created name collision)
    // stays local until the user renames it out of the namespace.
    if rel_path_to_string(rel).contains(kutl_core::lattice::CONFLICT_INFIX) {
        debug!(
            rel = %rel.display(),
            "skipping mint for an untracked conflict-copy-namespace path"
        );
        return Vec::new();
    }

    // Mint the document and record its identity + lattice record, then place it.
    let document_id = get_or_create_uuid(state, rel);
    debug!(rel = %rel.display(), %document_id, "minted document for new local file");
    let mut effects = mint_register_subscribe(state, &document_id, rel, stamp);

    // Send the new document's initial content too: the new-doc path registers
    // AND ships the first content. Text diffs into the CRDT (an empty file
    // diffs to no ops — a no-op for a brand-new empty file); binary ships the
    // full bytes through the blob LWW path. The mint above (identity, shadow
    // fold, register, subscribe, persist) is SHARED — content mode only picks
    // the content rule.
    match std::str::from_utf8(content) {
        Ok(text) => effects.extend(diff_into_crdt(state, &document_id, rel, text)),
        Err(_) => effects.extend(blob_change(state, &document_id, rel, content, stamp)),
    }
    effects
}

/// The shared REGISTER-AND-SUBSCRIBE mint unit, in its load-bearing order:
/// fold the shadow at `rel` (this event IS the watcher seeing the file on
/// disk — the rm/mv/create local-fold trilogy; the fold makes the mint a
/// reconcile fixpoint, no self-`GuardedPlace`), persist the identity
/// immediately (a crash must not lose the path↔UUID mapping and re-mint a
/// duplicate), register, subscribe EXPLICITLY (before the shadow-at-mint fold
/// the Subscribe rode the self-place's Register arm as the only site — the
/// implicit coupling behind the missing-subscribe class), and re-derive
/// placement. The inode follows at the driver's `RegisterDocument` edge
/// (`record_local_register_inode`).
///
/// ONE owner for the five-step sequence — partial application of it was a
/// repeated bug class (dropped-persistence, missing-subscribe). The three
/// callers differ only in their guard: the new-doc mint (untracked path), the
/// text pre-seed (`get_doc` miss), and the blob pre-seed (`blob_state` miss).
fn mint_register_subscribe(
    state: &mut SpaceState,
    document_id: &str,
    rel: &std::path::Path,
    stamp: EventStamp,
) -> Vec<Effect> {
    if let Ok(uid) = Uuid::parse_str(document_id) {
        state.shadow.set_tracked(rel, uid);
    }
    let mut effects = vec![
        Effect::SaveState,
        register_document_effect(state, document_id, rel, stamp),
        Effect::Subscribe {
            document_id: document_id.to_owned(),
        },
    ];
    effects.extend(reconcile_placement(state));
    effects
}

/// Port of `handle_file_removed` (daemon.rs). A tracked path's removal is
/// an unregister: clear local state, drop the identity, and emit
/// `UnregisterDocument` carrying the delete's lifecycle HLC.
pub(super) fn handle_file_removed(
    state: &mut SpaceState,
    rel: &std::path::Path,
    stamp: EventStamp,
) -> Vec<Effect> {
    let Some(identity) = state.file_identity.get(rel).cloned() else {
        debug!(rel = %rel.display(), "removal of untracked path ignored");
        return Vec::new();
    };
    let document_id = identity.document_uuid;

    cleanup_document_state(state, rel);
    let meta = make_lifecycle_metadata(state, &document_id, "file delete", stamp);
    let deleted_hlc = meta.hlc.clone().and_then(|w| Hlc::try_from(w).ok());
    // current_path is unchanged by a delete; carry the known path through.
    observe_unregister_remote(state, &document_id, rel, deleted_hlc);
    unregister_identity(state, &document_id);
    // Vacate the shadow: the file IS gone from disk — this event is the watcher
    // observing the user's own `rm`, so there is no RemoveFile/ACK round-trip
    // to fold on (unlike the remote-unregister twin). Without this the occupant
    // stays `Tracked` forever and the next register's `stat_untracked` mistakes
    // a foreign recreate at this path for the tracked incumbent — placing (and
    // adopting the recreate's bytes as a "pending edit") instead of deferring:
    // the §4.2 concurrent-recreate merge corruption, deterministic on the
    // deleter's side.
    state.shadow.remove_fold(rel);

    let mut effects = vec![Effect::UnregisterDocument {
        space_id: state.space_id.clone(),
        document_id,
        metadata: Some(meta),
    }];
    // The identity drop above rewrote the persisted document map in memory;
    // emit the coalesced persist so state.json is not stale across a crash
    // (the imperative twin `unregister_identity` saves inline — omitting the
    // effect here is the dropped-persistence class).
    effects.push(Effect::SaveState);
    effects.extend(reconcile_placement(state));
    effects
}

/// Port of `handle_file_renamed` (daemon.rs). A rename of a tracked path
/// emits `RenameDocument`; a rename whose source is unknown falls through to the
/// new-doc modify path on the destination.
pub(super) fn handle_file_renamed(
    state: &mut SpaceState,
    old: &std::path::Path,
    new: &std::path::Path,
    stamp: EventStamp,
) -> Vec<Effect> {
    let Some(identity) = state.file_identity.get(old).cloned() else {
        // Source unknown: treat as a new file at the destination (the
        // `handle_file_renamed` untracked-source fall-through).
        debug!(
            old = %old.display(),
            new = %new.display(),
            "rename source untracked; treating destination as new file"
        );
        return handle_file_modified(state, new, Some(b""), stamp);
    };
    let document_id = identity.document_uuid;

    // Reject rename-to-collide with another tracked document (mirrors
    // `handle_file_renamed`, daemon.rs).
    // Borrow the tracked paths directly — cloning the whole index per rename
    // event was pure allocation (perf §3.1).
    if let Some(existing) = crate::case_collision::find_case_variant(
        new,
        state
            .file_identity
            .keys()
            .filter(|p| p.as_path() != old)
            .map(std::path::PathBuf::as_path),
    ) {
        error!(
            old_path = %old.display(),
            new_path = %new.display(),
            existing_path = %existing.display(),
            "case_collision_rejected: rename target would collide with tracked document, ignoring event"
        );
        return Vec::new();
    }

    move_identity(state, old, new.to_path_buf(), &document_id);
    // Fold the DiskShadow alongside identity: the user's rename already moved
    // the file on disk, and its recorded inode follows it (POSIX rename). The
    // imperative twin (`try_local_inode_rename`) folds via `fold_shadow_rename`;
    // without this fold the shadow keeps claiming `old`, every subsequent
    // `reconcile_placement` derives a phantom `GuardedPlace(Rename old→…)` from
    // a path the file no longer occupies, and a later remote rename's conform
    // finds `recorded_inode(old) = None` (identity re-keyed) → MATERIALIZE —
    // duplicating the doc on disk next to the user's renamed file.
    let inode = state.shadow.shadow_path_inode.get(old).copied();
    if let Ok(uid) = Uuid::parse_str(&document_id) {
        state.shadow.rename_fold(old, new, Some(uid), inode);
    }
    // Resolve the causal floor (the register HLC we recorded for this doc) BEFORE
    // folding the rename, so it is the prior registration — not max'd with this
    // rename's own stamp. Lets the relay treat our rename as causally-after a
    // clock-skewed registration (see `DocRecord::path_priority`).
    let rename_causal_floor = causal_floor_for(state, &document_id);
    let meta = make_lifecycle_metadata(state, &document_id, "file rename", stamp);
    let renamed_hlc = meta.hlc.clone().and_then(|w| Hlc::try_from(w).ok());
    seed_remote_record(
        state,
        &document_id,
        new,
        None,
        renamed_hlc,
        rename_causal_floor,
        None,
    );

    // A LOCAL rename's file already moved on disk (the user/editor did it — this
    // event IS the watcher seeing it), so there is no disk move to emit: we only
    // tell the relay (`RenameDocument`) and let the cascade reconcile placement —
    // a no-op BY CONSTRUCTION now that the shadow fold above re-keyed the doc to
    // `new` (desired == shadow_path, so it is not a mover).
    let mut effects = vec![Effect::RenameDocument {
        space_id: state.space_id.clone(),
        document_id,
        old_path: rel_path_to_string(old),
        new_path: rel_path_to_string(new),
        metadata: Some(meta),
        rename_causal_floor,
    }];
    // `move_identity` above re-keyed the persisted document map in memory;
    // emit the coalesced persist so state.json is not stale across a crash
    // (the imperative twin saves inline — omitting the effect here is the
    // dropped-persistence class; surfaced by the case-only-rename
    // e2e reading a stale on-disk map).
    effects.push(Effect::SaveState);
    effects.extend(reconcile_placement(state));
    effects
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::core::handle::test_support::{
        doc_id, hlc, register_identity_only, st, track_remote,
    };
    use crate::core::shadow::casefold;
    use crate::core::{DaemonCore, Event};

    #[test]
    fn test_untracked_conflict_namespace_path_is_never_minted() {
        // An untracked file whose name carries the conflict-copy infix is a
        // LOCAL materialization artifact of a displaced document, never an
        // authored file. The relay refuses to register the namespace
        // outright, so minting a fresh document for it guarantees a rejected
        // register on every later sync — the wedge a joiner hits when the
        // AGENTS.md they wrote collides with the space's copy. The path must
        // be left alone: no mint, no register, no identity.
        let mut s = st();
        let rel = PathBuf::from("AGENTS.kutl-conflict-2a1e3540-ea14-4093-846a-d89e27928383.md");
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: rel.clone(),
                content: Some(b"stray artifact".to_vec()),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::RegisterDocument { .. })),
            "a conflict-namespace path must not register, got {effects:?}"
        );
        assert!(
            !s.file_identity.contains_key(&rel),
            "no identity may be minted for a conflict-namespace path"
        );
    }

    #[test]
    fn test_local_modify_of_untracked_text_registers_subscribes_and_folds_shadow() {
        // A modify on a never-tracked path is a new doc: register AND ship the
        // initial content (handle_file_modified new-doc branch falls through to
        // apply_file_change + SendOps). The mint folds the shadow ITSELF (the
        // rm/mv/create local-fold trilogy — the file is already on disk) and
        // emits an EXPLICIT Subscribe, so it is a reconcile fixpoint: no
        // self-GuardedPlace, no dependence on a place ACK for the subscription
        // (the implicit coupling behind the missing-subscribe class).
        let mut s = st();
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("a.md"),
                content: Some(b"hello".to_vec()),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::RegisterDocument { .. })),
            "new untracked text file must register, got {effects:?}"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Subscribe { .. })),
            "the mint must subscribe explicitly, got {effects:?}"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::GuardedPlace { .. })),
            "the shadow-at-mint fold makes the mint a fixpoint — no self-place, got {effects:?}"
        );
        assert!(
            s.shadow
                .shadow_occupant
                .contains_key(&casefold(Path::new("a.md"))),
            "the mint folds the shadow at its own path"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::SendOps { ops, .. } if !ops.is_empty())),
            "the new doc's initial content must be shipped as ops, got {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(e, Effect::SaveDoc { .. })),
            "the new doc's CRDT must be persisted, got {effects:?}"
        );
    }

    #[test]
    fn test_local_mint_overrides_stale_untracked_marker_and_folds_tracked() {
        // The f_conflicts create/create strand (the gamma default-flip blocker):
        // a remote register's GuardedPlace races a not-yet-flushed local create —
        // the shell's atomic stat reports the occupant (UntrackedFileObserved)
        // and the remote doc defers (graft 1+2, correct so far). But when the
        // local create THEN mints its own doc at the path, the Untracked marker
        // is STALE: the mint now folds `Tracked` straight over it (and
        // `file_identity` claims the path, which the deferral predicate must
        // honor exactly as the shell's `stat_untracked` does). Historically
        // deferring here stranded the minted doc unplaced AND unsubscribed
        // (its Subscribe rode the place ACK before the shadow-at-mint fold),
        // so the winner-doc's post-conflict edit never reached this daemon
        // (the captured e2e divergence).
        let mut s = st();
        let peer = doc_id();
        let remote_actor = kutl_core::hlc::ActorId(Uuid::from_u128(0xBEEF));
        // 1. The peer's register arrives first (older stamp): a mover onto dup.md.
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteRegister {
                document_id: peer.clone(),
                path: "dup.md".into(),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: Some(hlc(remote_actor, 100)),
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { id, target, .. }
                    if *id == peer && target.as_path() == Path::new("dup.md")
            )),
            "precondition: the lone remote register is a mover onto dup.md, got {effects:?}"
        );
        // 2. The shell's atomic stat found the unflushed local create: the
        //    graft-1 disagreement marks the path and the peer defers.
        let _ = DaemonCore::handle(
            &mut s,
            Event::UntrackedFileObserved {
                rel: PathBuf::from("dup.md"),
                stamp: EventStamp {
                    wall_ms: 150,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            s.deferred.contains_key(&peer),
            "precondition: the remote doc defers behind the untracked occupant"
        );
        // 3. The local create's watcher event flushes: the mint claims the path
        //    with a LATER stamp, so the minted doc wins dup.md.
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("dup.md"),
                content: Some(b"from-local".to_vec()),
                stamp: EventStamp {
                    wall_ms: 200,
                    origin_hlc: None,
                },
            },
        );
        let minted = s
            .file_identity
            .get(Path::new("dup.md"))
            .map(|fi| fi.document_uuid.clone())
            .expect("the mint claims identity at dup.md");
        assert!(
            !s.deferred.contains_key(&minted),
            "the minted doc must not defer on its own file (stale Untracked marker)"
        );
        // The mint FOLDS Tracked over the stale Untracked marker and subscribes
        // explicitly — it is a reconcile fixpoint, so no self-place is emitted
        // and nothing depends on a place ACK for the subscription.
        assert!(
            matches!(
                s.shadow.shadow_occupant.get(&casefold(Path::new("dup.md"))),
                Some(crate::core::Occupant::Tracked(_))
            ),
            "the mint folds Tracked over the stale Untracked marker"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Subscribe { document_id } if *document_id == minted)),
            "the minted doc subscribes explicitly at mint, got {effects:?}"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::GuardedPlace { id, .. } if *id == minted)),
            "the minted doc is a fixpoint — no self-place, got {effects:?}"
        );
        // The losing peer re-derives to its conflict sibling in the same pass —
        // the displacement is not blocked by the minted doc's claim.
        let peer_uid = Uuid::parse_str(&peer).expect("peer id is a uuid");
        let conflict = kutl_core::lattice::conflict_path("dup.md", &peer_uid);
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::GuardedPlace { id, target, .. }
                    if *id == peer && target.as_path() == Path::new(&conflict)
            )),
            "the losing peer register must re-derive to its conflict path, got {effects:?}"
        );
    }

    /// A core-path local rename must fold the [`DiskShadow`] alongside identity —
    /// the parity guard against the imperative twin (`try_local_inode_rename`'s
    /// `fold_shadow_rename`). Without the fold the shadow keeps claiming the old
    /// path: every reconcile derives a phantom `GuardedPlace(Rename old→…)`, and
    /// a later remote rename's conform finds `recorded_inode(old) = None` and
    /// MATERIALIZES a duplicate next to the user's renamed file.
    #[test]
    fn test_local_rename_folds_shadow_and_is_not_a_phantom_mover() {
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        s.shadow.set_inode(7, Path::new("a.md"));

        let effects = DaemonCore::handle(
            &mut s,
            Event::FileRenamed {
                old: PathBuf::from("a.md"),
                new: PathBuf::from("b.md"),
                stamp: EventStamp {
                    wall_ms: 20,
                    origin_hlc: None,
                },
            },
        );

        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::RenameDocument { .. })),
            "a tracked local rename must emit RenameDocument, got {effects:?}"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::GuardedPlace { .. })),
            "a folded shadow must not leave the doc a phantom mover, got {effects:?}"
        );

        let uid = Uuid::parse_str(&id).unwrap();
        assert_eq!(s.shadow.shadow_path.get(&uid), Some(&PathBuf::from("b.md")));
        assert_eq!(
            s.shadow.shadow_occupant.get("a.md"),
            None,
            "old path must be vacated in the shadow"
        );
        assert_eq!(
            s.shadow.shadow_path_inode.get(Path::new("b.md")),
            Some(&7),
            "the recorded inode follows the file to the new path"
        );
    }

    /// The TEXT twin of the blob pre-seed hole (wave-2 follow-up): a
    /// tracked-but-never-synced identity (a pre-seeded state.json entry whose
    /// file appears after startup — the truth table's leave-alone row) must
    /// register + subscribe on its first text content, exactly as the former
    /// imperative handler's get_doc-is_new rule did. Without it the doc's ops
    /// ship to a relay that never registered it and the daemon never
    /// subscribes — invisible to peers, deaf to their edits.
    #[test]
    fn test_preseeded_text_identity_registers_and_subscribes_on_first_content() {
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "seeded.md");
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("seeded.md"),
                content: Some(b"first body".to_vec()),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::RegisterDocument { path, .. } if path == "seeded.md")),
            "first content of a never-synced identity must register, got {effects:?}"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Subscribe { .. })),
            "…and subscribe, got {effects:?}"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::SendOps { ops, .. } if !ops.is_empty())),
            "…and ship the content, got {effects:?}"
        );

        // A SECOND edit of the now-synced doc must NOT re-register.
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("seeded.md"),
                content: Some(b"first body, edited".to_vec()),
                stamp: EventStamp {
                    wall_ms: 200,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::RegisterDocument { .. })),
            "a synced doc's edit must not re-register, got {effects:?}"
        );
    }

    #[test]
    fn test_local_modify_of_tracked_doc_emits_sendops_delta_and_savedoc() {
        // A local modify of an already-tracked doc diffs the new bytes into the
        // CRDT and ships the delta (Effect::SendOps) + persists (Effect::SaveDoc)
        // — the ported handle_file_modified non-empty path (daemon.rs).
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        // First modify seeds the content (delta from empty).
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("a.md"),
                content: Some(b"local edit".to_vec()),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: None,
                },
            },
        );
        let text_mode = i32::from(kutl_proto::sync::ContentMode::Text);
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::SendOps { document_id, ops, content_mode, .. }
                    if document_id == &id && !ops.is_empty() && *content_mode == text_mode
            )),
            "a local modify producing ops must SendOps the delta, got {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e, Effect::SaveDoc { document_id } if document_id == &id
            )),
            "a local modify must persist the CRDT via SaveDoc, got {effects:?}"
        );
        assert_eq!(
            s.get_doc(&id).map(kutl_core::Document::content).as_deref(),
            Some("local edit")
        );
        // A second modify with identical content is a CRDT no-op: no SendOps.
        let effects2 = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("a.md"),
                content: Some(b"local edit".to_vec()),
                stamp: EventStamp {
                    wall_ms: 110,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !effects2.iter().any(|e| matches!(e, Effect::SendOps { .. })),
            "an identical-content modify must not re-send, got {effects2:?}"
        );
    }
}
