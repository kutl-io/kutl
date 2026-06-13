//! Remote content path: the `RemoteOps` arm and the pure CRDT merge of a
//! non-empty remote-ops payload.

use kutl_proto::protocol::is_blob_mode;
use kutl_proto::sync::ChangeMetadata;
use tracing::{debug, error, warn};

use super::blob::merge_remote_blob;
use super::helpers::{
    incorporate_pending_edits, is_untracked_uuid, note_op_cap_status, observe_remote_hlc,
    should_skip_remote_write,
};
use crate::blob_state::sha256_bytes;
use crate::core::shadow::casefold;
use crate::core::{Effect, EventStamp, SpaceState};

/// Port of the `RemoteOps` arm of `handle_sync_event` (daemon.rs) + the former
/// imperative `handle_remote_text`, through the disk write.
///
/// First folds every observed remote stamp into the clock via
/// `stamp.wall_ms`-injected `recv` (porting `observe_remote_hlc`,
/// reading the injected wall rather than `skewed_now_ms_u64`).
///
/// CRDT MERGE (Task 5c): the merge is now PURE — `state.documents` holds the
/// in-memory CRDT, so `doc.merge` / `incorporate_pending_edits` / `doc.content()`
/// (the former `handle_remote_text` merge body) run in-core. The merged content is written via
/// `Effect::WriteFile` (gated by `should_skip_remote_write` read against the
/// shadow + recorded inode, NOT live disk) and persisted via `Effect::SaveDoc`;
/// any local divergence the merge surfaced is shipped back as `Effect::SendOps`.
/// The zero-content "ensure the file exists" path needs no
/// merge and is handled above.
pub(super) fn handle_remote_ops(
    state: &mut SpaceState,
    document_id: &str,
    ops: &[u8],
    metadata: &[ChangeMetadata],
    content_mode: i32,
    local_content: Option<&str>,
    stamp: EventStamp,
) -> Vec<Effect> {
    // Advance the origin clock past every observed remote stamp (mirrors
    // `handle_sync_event`'s RemoteOps arm, daemon.rs)
    // so a lifecycle/edit op this daemon emits afterward is causally after them.
    for m in metadata {
        observe_remote_hlc(state, Some(m), stamp);
    }

    // CONTENT-MODE fork (the allowed blob residue): blob ops take the pure
    // LWW merge; text falls through to the CRDT merge. Both observed their
    // remote stamps above, matching the former imperative arm's order.
    let check = kutl_proto::sync::SyncOps {
        content_mode,
        ..Default::default()
    };
    if is_blob_mode(&check) {
        return merge_remote_blob(state, document_id, ops, metadata);
    }

    // Skip a content op for an untracked UUID (locally deleted or not yet
    // registered): materializing it would diverge (the §1.5 edit-vs-delete race).
    if is_untracked_uuid(state, document_id) {
        debug!(%document_id, "skipping remote content op for untracked document");
        return Vec::new();
    }

    // Empty ops = the document exists but has no content yet. Ensure the file
    // exists on disk — write only when it is ABSENT (a possibly zero-byte file),
    // the one remote-text path that needs no CRDT merge (the former
    // `handle_remote_text` empty-ops branch).
    // The suppress hash for empty content is computed in-core and carried inline
    // (Cycle B closed by construction).
    if ops.is_empty() {
        // Prefer-shadow: a displaced doc's file lives at its conflict path,
        // not the `uuid_to_path` original (`SpaceState::doc_disk_path`).
        let Some(rel) = state.doc_disk_path(document_id) else {
            debug!(%document_id, "no local path for empty-content op; ignoring");
            return Vec::new();
        };
        // The shadow's occupant map is the in-core stand-in for the old
        // `abs_path.exists()` probe. Already present → nothing to
        // do (the real code's `if !abs_path.exists()` guard).
        let file_on_disk = state.shadow.shadow_occupant.contains_key(&casefold(&rel));
        if file_on_disk {
            return Vec::new();
        }
        let content = Vec::new();
        return vec![Effect::WriteFile {
            suppress_hash: Some(sha256_bytes(&content)),
            rel,
            content,
        }];
    }

    // Non-empty ops: merge purely against the in-memory CRDT (the former
    // `handle_remote_text` merge body). Prefer-shadow: for a DISPLACED doc the
    // merged content must land at its conflict path — writing at the
    // `uuid_to_path` original would clobber the WINNER's file there
    // (`SpaceState::doc_disk_path`, carried concern 2).
    let Some(rel) = state.doc_disk_path(document_id) else {
        debug!(%document_id, "no local path for remote ops; ignoring");
        return Vec::new();
    };
    merge_remote_ops(state, document_id, &rel, ops, metadata, local_content)
}

/// Merge a non-empty remote-ops payload into the in-memory CRDT and emit the
/// resulting disk + persistence + delta effects. Port of the former imperative
/// `handle_remote_text`'s merge body, made pure: the CRDT lives in
/// `state.documents`, so `incorporate_pending_edits` / `doc.merge` / `doc.content`
/// run in-core with no IO.
///
/// LOCAL-BEFORE-REMOTE (FS-5): pending local edits are incorporated BEFORE the
/// remote merge so the CRDT transforms concurrent positions correctly. The
/// imperative path read the *live file* to find that divergence;
/// the pure core takes that divergence as `local_content` —
/// the driver reads the on-disk bytes at the edge (`next_event`) and carries them
/// on the event. When `local_content` is `Some` and differs from the in-memory
/// `doc.content()`, those bytes ARE the pending local edit and are diffed into the
/// CRDT first; `None` falls back to `doc.content()` (a structural no-op), which is
/// the sim path and any caller that cannot read live disk. Either way the ordering
/// is preserved — the local edit is never silently dropped.
fn merge_remote_ops(
    state: &mut SpaceState,
    document_id: &str,
    rel: &std::path::Path,
    ops: &[u8],
    metadata: &[ChangeMetadata],
    local_content: Option<&str>,
) -> Vec<Effect> {
    let agent_name = state.agent_name.clone();
    let author_did = state.author_did.clone();
    let doc = state.load_or_create_doc(document_id);

    // Incorporate pending local edits BEFORE merging remote ops (FS-5). The
    // divergence source is the driver's live-disk read (`local_content`); absent
    // it, fall back to the in-memory doc content (a no-op) so the ordering is
    // preserved for callers that cannot read disk (the sim).
    let current_content = local_content.map_or_else(|| doc.content(), str::to_owned);
    let local_delta = incorporate_pending_edits(doc, &agent_name, &author_did, &current_content);

    // Merge the remote ops, then decide whether anything actually changed: a
    // redundant merge (our own ops echoed back via catch-up) must NOT write +
    // suppress, or it risks swallowing a concurrent local edit's watcher event.
    let version_before = doc.local_version();
    if let Err(e) = doc.merge(ops, metadata) {
        if matches!(e, kutl_core::Error::OpCountExceeded { .. }) {
            // ERROR, not warn: this replica can no longer absorb peers' edits
            // on this document — the cluster is permanently diverging. Fires
            // once per rejected payload (the op-cap twin of the blob-size
            // error in `classify_blob_bytes`).
            error!(
                %document_id,
                rel = %rel.display(),
                error = %e,
                "dropping remote ops: document at the op cap; replicas are diverging"
            );
            crate::metrics_calls::record_error(crate::metrics_calls::error_category::DOC_AT_OP_CAP);
            let mut effects = Vec::new();
            note_op_cap_status(state, document_id, true, &mut effects);
            return effects;
        }
        // A malformed/oversized remote patch is dropped (the engine rejects it);
        // there is nothing valid to materialize.
        warn!(
            %document_id,
            ops_len = ops.len(),
            "dropping remote ops the engine rejected (malformed or oversized)"
        );
        return Vec::new();
    }
    // A successful merge can still OVERSHOOT the cap (one big create lands the
    // doc over it in a single bounded merge — the core warns); record/clear
    // the status flag from the post-merge count so every replica that absorbs
    // an over-cap create flags it immediately, not at its first rejected edit.
    let at_cap_after_merge = doc.op_count() >= kutl_core::MAX_OPS_PER_DOC;
    let version_after = doc.local_version();
    let state_changed = local_delta.is_some() || version_after != version_before;

    // Advance last_synced to the post-merge version so a later local edit's delta
    // is computed from here, not re-encoding the just-merged remote ops
    // (the former `handle_remote_text` set last_synced to version_after_merge in
    // BOTH the skip-write and normal branches).
    if state_changed {
        state.last_synced.insert(rel.to_path_buf(), version_after);
    }

    let mut effects = Vec::new();

    if at_cap_after_merge {
        // The id-carrying twin of the core's anonymous post-merge overshoot
        // warn: this merge landed the document at/over the op cap, so every
        // future edit to it (local or remote) will be rejected.
        error!(
            %document_id,
            rel = %rel.display(),
            cap = kutl_core::MAX_OPS_PER_DOC,
            "document reached the op cap after merge; further edits will not sync"
        );
        crate::metrics_calls::record_error(crate::metrics_calls::error_category::DOC_AT_OP_CAP);
    }
    note_op_cap_status(state, document_id, at_cap_after_merge, &mut effects);

    // Ship any local divergence the incorporate surfaced so peers see it.
    if let Some((local_ops, local_meta)) = local_delta {
        effects.push(Effect::SendOps {
            document_id: document_id.to_owned(),
            ops: local_ops,
            metadata: local_meta,
            content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
            content_hash: Vec::new(),
        });
    }

    if !state_changed {
        // A redundant merge (e.g. our own ops echoed back via catch-up): do NOT
        // write/suppress — that risks swallowing a concurrent local edit's watcher
        // event. The former imperative tail re-`save_doc`d + reset
        // last_synced here, but with `version_after ==
        // version_before` that re-persists an unchanged CRDT to the same version —
        // inert IO. The pure core elides it (no `SaveDoc`, no last_synced churn):
        // behavior-equivalent and a deliberate step off the §1.1 per-edit-disk path.
        return effects;
    }

    // Compute the merged content lazily — only now that we know a write may be
    // needed (the eager `doc.content()` of Task 5b is gone in the pure core).
    let content = state
        .get_doc(document_id)
        .map_or_else(String::new, kutl_core::Document::content);
    let content_bytes = content.into_bytes();

    // should_skip_remote_write against the shadow + recorded
    // inode, NOT live disk: a file gone from disk but with a known inode is a
    // local rename/delete — merge the CRDT and persist, but do NOT recreate it.
    let file_on_disk = state.shadow.shadow_occupant.contains_key(&casefold(rel));
    let identity_inode = state.file_identity.get(rel).and_then(|id| id.inode);
    if should_skip_remote_write(file_on_disk, identity_inode) {
        debug!(
            %document_id,
            rel = %rel.display(),
            "skipping remote write: tracked file gone from disk (local rename/delete in flight)"
        );
    } else {
        // First materialization of a remote document persists its inode. The doc was
        // registered with a null inode (`register_identity`: the file did not exist
        // at SubscribeRemote time), and the landing `WriteFile`'s
        // `apply_write`/`apply_effect_result` folds the real post-op inode into
        // `file_identity` + the shadow. When the recorded inode is still `None` —
        // i.e. this is the first write that will record a real one — emit
        // `Effect::SaveState` AFTER the write so it snapshots the just-recorded
        // inode. Without this the inode lives only in memory and is lost on restart,
        // so `detect_offline_renames` reads `inode: null` on rejoin and never matches
        // a file renamed while offline: the moved file becomes a phantom new doc
        // (lifecycle §3.1) or survives a cluster delete (§3.6).
        //
        // The imperative `handle_remote_text` persisted on `!abs_path.exists()`;
        // that signal does not survive the port because the
        // placement cascade marks the path occupied in the shadow BEFORE the content
        // op lands (so `file_on_disk` is already true here). Gating on the recorded
        // inode being absent is the faithful, placement-order-independent equivalent:
        // it fires exactly once, on the first write that records the inode, and is
        // not hot (the inode is stable across later edits, which keep it `Some`).
        let inode_not_yet_persisted = identity_inode.is_none();
        effects.push(Effect::WriteFile {
            suppress_hash: Some(sha256_bytes(&content_bytes)),
            rel: rel.to_path_buf(),
            content: content_bytes,
        });
        if inode_not_yet_persisted {
            effects.push(Effect::SaveState);
        }
    }

    // Persist the merged CRDT either way (the write may be skipped, but the merge
    // still advanced the document — the former `handle_remote_text` saved the
    // doc in both branches).
    effects.push(Effect::SaveDoc {
        document_id: document_id.to_owned(),
    });
    effects
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use uuid::Uuid;

    use super::*;
    use crate::core::handle::test_support::{
        doc_id, fill_doc_to_op_cap, register_identity_only, remote_ops_for, st, track_remote,
    };
    use crate::core::{DaemonCore, Event};

    #[test]
    fn test_remote_ops_advances_clock_then_no_clock_read_in_core() {
        // RemoteOps folds the observed HLC into the clock via stamp (mirroring
        // `handle_sync_event`),
        // purely — the core never calls Hlc::now().
        let mut s = st();
        let before = s.hlc.last();
        let hlc = kutl_core::Hlc {
            physical_ms: 9_999,
            logical: 0,
            actor: before.actor,
        };
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: "doc-1".into(),
                ops: vec![],
                metadata: vec![],
                content_mode: 0,
                local_content: None,
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: Some(hlc),
                },
            },
        );
        // Empty ops/metadata => no merge effect, but the call is pure & total.
        assert!(s.hlc.last() >= before);
    }

    // ── Task 5c: the ported content-merge arms ──────────────────────────────

    /// Carried concern 2 (the prefer-shadow accessor): a DISPLACED doc's
    /// identity map still records the original path (the pure cascade is a
    /// projection — it never `move_identity`s on displacement), but its bytes
    /// live at the conflict path the cascade moved them to. A remote merge's
    /// `WriteFile` must follow the SHADOW path — writing at the identity path
    /// would clobber the WINNER's file sitting there.
    #[test]
    fn test_remote_ops_write_lands_at_displaced_shadow_path() {
        let mut s = st();
        let id = doc_id();
        let uid = Uuid::parse_str(&id).unwrap();
        // Identity at the original path; the shadow says the cascade displaced
        // the file to its conflict sibling.
        register_identity_only(&mut s, &id, "a.md");
        let conflict = kutl_core::lattice::conflict_path("a.md", &uid);
        s.shadow.set_tracked(Path::new(&conflict), uid);
        let (ops, metadata) = remote_ops_for("displaced body");
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: None,
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        let write_rel = effects.iter().find_map(|e| match e {
            Effect::WriteFile { rel, .. } => Some(rel.clone()),
            _ => None,
        });
        assert_eq!(
            write_rel.as_deref(),
            Some(Path::new(&conflict)),
            "the merge write must land at the displaced (shadow) path, not the identity path"
        );
    }

    #[test]
    fn test_remote_ops_merge_on_tracked_doc_emits_write_and_savedoc() {
        // A non-empty remote-ops merge on a tracked doc materializes the merged
        // content (Effect::WriteFile) and persists the CRDT (Effect::SaveDoc) —
        // the ported former `handle_remote_text` merge.
        let mut s = st();
        let id = doc_id();
        // Track at "a.md" but with NO file on disk yet (shadow occupant unset),
        // so the merged remote content must be written for first materialization.
        register_identity_only(&mut s, &id, "a.md");
        let (ops, metadata) = remote_ops_for("hello world");
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: None,
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::WriteFile { rel, content, suppress_hash }
                    if rel.as_path() == Path::new("a.md")
                        && content == b"hello world"
                        && suppress_hash.as_deref() == Some(sha256_bytes(b"hello world").as_slice())
            )),
            "remote merge must write the merged content with a suppress hash, got {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e, Effect::SaveDoc { document_id } if document_id == &id
            )),
            "remote merge must persist the CRDT via SaveDoc, got {effects:?}"
        );
        // First materialization: the doc was registered with a null inode (file
        // absent at SubscribeRemote time), so the landing WriteFile records the real
        // inode and the merge must emit SaveState to PERSIST it — otherwise the
        // inode lives only in memory and `detect_offline_renames` reads `null` on
        // rejoin, turning an offline rename into a phantom new doc (lifecycle §3.1)
        // or surviving a cluster delete (§3.6). SaveState must come AFTER WriteFile
        // so it snapshots the just-recorded inode.
        let write_pos = effects
            .iter()
            .position(|e| matches!(e, Effect::WriteFile { .. }));
        let save_state_pos = effects.iter().position(|e| matches!(e, Effect::SaveState));
        assert!(
            save_state_pos.is_some(),
            "first materialization must persist the recorded inode via SaveState, got {effects:?}"
        );
        assert!(
            write_pos < save_state_pos,
            "SaveState must follow WriteFile so it snapshots the recorded inode, got {effects:?}"
        );
        // The merged content is now the in-memory CRDT content.
        assert_eq!(
            s.get_doc(&id).map(kutl_core::Document::content).as_deref(),
            Some("hello world")
        );
    }

    #[test]
    fn test_remote_ops_at_cap_rejection_flags_doc_and_emits_savestate() {
        // An at-cap doc rejects remote merges (OpCountExceeded). The rejection
        // must be LOUD and durable: the rel path lands in the persisted
        // `at_op_cap` set (what `kutl status` surfaces) and a SaveState ships
        // it to disk. Nothing merged — no WriteFile.
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        fill_doc_to_op_cap(&mut s, &id);
        let (ops, metadata) = remote_ops_for("peer edit");
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: None,
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            s.state.at_op_cap.contains(&id),
            "a rejected at-cap merge must record the doc in at_op_cap"
        );
        assert!(
            effects.iter().any(|e| matches!(e, Effect::SaveState)),
            "the at-cap flag must persist via SaveState, got {effects:?}"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::WriteFile { .. })),
            "nothing merged — no write may land, got {effects:?}"
        );
    }

    #[test]
    fn test_remote_overshoot_create_flags_at_cap_on_merge_success() {
        // One big remote create can land a doc over the cap in a SINGLE
        // bounded merge (the A4a >cap-text shape): the merge SUCCEEDS, but
        // every future edit will be rejected. The post-merge count must flag
        // the doc immediately — not at its first rejected edit.
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "big.md");
        let big = "a".repeat(kutl_core::MAX_OPS_PER_DOC + 1);
        let (ops, metadata) = remote_ops_for(&big);
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: None,
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::WriteFile { .. })),
            "the overshooting create itself still materializes, got {effects:?}"
        );
        assert!(
            s.state.at_op_cap.contains(&id),
            "an over-cap doc must be flagged at merge time, not first rejection"
        );
        assert!(
            effects.iter().any(|e| matches!(e, Effect::SaveState)),
            "the at-cap flag must persist via SaveState, got {effects:?}"
        );
    }

    #[test]
    fn test_remote_merge_skips_write_when_file_gone_but_inode_known() {
        // should_skip_remote_write: a file gone from disk but
        // with a known inode is a local rename/delete — merge the CRDT, persist,
        // but do NOT recreate the file. The merge still emits SaveDoc, never
        // WriteFile.
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        // Model "gone but inode known": drop the shadow occupant (file absent) and
        // record an inode on the identity (it was materialized before).
        s.shadow
            .shadow_occupant
            .remove(&casefold(&PathBuf::from("a.md")));
        if let Some(fi) = s.file_identity.get_mut(&PathBuf::from("a.md")) {
            fi.inode = Some(99);
        }
        let (ops, metadata) = remote_ops_for("hello world");
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: None,
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::WriteFile { .. })),
            "gone-but-inode-known must skip the write (likely renamed), got {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e, Effect::SaveDoc { document_id } if document_id == &id
            )),
            "the merge still persists the CRDT even when the write is skipped, got {effects:?}"
        );
    }

    #[test]
    fn test_remote_merge_does_not_resave_state_when_inode_already_known() {
        // The first-materialization SaveState fires exactly once: a later remote
        // merge on a doc whose inode is ALREADY recorded must NOT re-persist state
        // (the inode is stable across edits, so this is not a hot per-edit cost).
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        // Model "already materialized": the file is on disk (shadow occupant kept by
        // track_remote) AND its inode is recorded — the state after the first write.
        if let Some(fi) = s.file_identity.get_mut(&PathBuf::from("a.md")) {
            fi.inode = Some(42);
        }
        let (ops, metadata) = remote_ops_for("hello world");
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: None,
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::WriteFile { .. })),
            "a state-changing merge still writes the merged content, got {effects:?}"
        );
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::SaveState)),
            "no SaveState when the inode is already recorded (not a first materialization), got {effects:?}"
        );
    }

    #[test]
    fn test_remote_ops_incorporates_local_content_before_merge() {
        // FS-5: when the event carries live-disk bytes that differ from the CRDT,
        // those bytes are diffed in BEFORE the remote merge and shipped back as a
        // SendOps delta — the local edit is not silently dropped (FS-5).
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        // Seed the CRDT with a known base so a divergent local_content produces a
        // real delta.
        let _ = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("a.md"),
                content: Some(b"base".to_vec()),
                stamp: EventStamp {
                    wall_ms: 20,
                    origin_hlc: None,
                },
            },
        );
        let (ops, metadata) = remote_ops_for("remote text");
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                // The live file diverged from the CRDT base — a pending local edit.
                local_content: Some("base plus local".to_owned()),
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(
                e, Effect::SendOps { document_id, ops, .. } if document_id == &id && !ops.is_empty()
            )),
            "the diverged local content must ship as a SendOps delta (FS-5), got {effects:?}"
        );
    }
}
