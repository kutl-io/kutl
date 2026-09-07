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
use crate::blob_state::HashedContent;
use crate::core::{Effect, EventStamp, SpaceState};

/// The effects of a remote-ops payload the engine refused.
///
/// An op-cap rejection records the cap status; an engine panic retires the
/// in-memory engine (reload from the sidecar, then re-subscribe so the
/// relay's catch-up restores what the sidecar lacks); anything else is a
/// malformed or oversized patch with nothing valid to materialize.
fn reject_remote_ops(
    state: &mut SpaceState,
    document_id: &str,
    rel: &std::path::Path,
    ops_len: usize,
    e: &kutl_core::Error,
) -> Vec<Effect> {
    match e {
        kutl_core::Error::OpCountExceeded { .. } => {
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
            let op_count = state
                .get_doc(document_id)
                .map_or(0, kutl_core::Document::op_count);
            let mut effects = Vec::new();
            note_op_cap_status(state, document_id, op_count, &mut effects);
            effects
        }
        kutl_core::Error::EnginePanicked { .. } => {
            // ERROR: the engine may be half-applied, and the reload drops
            // every op not yet in the sidecar.
            error!(
                %document_id,
                rel = %rel.display(),
                error = %e,
                "remote ops crashed the engine; reloading the document from its sidecar and re-subscribing"
            );
            crate::metrics_calls::record_error(crate::metrics_calls::error_category::ENGINE_PANIC);
            vec![
                Effect::ReloadDoc {
                    document_id: document_id.to_owned(),
                },
                Effect::Subscribe {
                    document_id: document_id.to_owned(),
                },
            ]
        }
        _ => {
            // A malformed/oversized remote patch is dropped (the engine rejects it);
            // there is nothing valid to materialize.
            warn!(
                %document_id,
                ops_len,
                "dropping remote ops the engine rejected (malformed or oversized)"
            );
            Vec::new()
        }
    }
}

/// The pure-core `RemoteOps` arm, through the disk write.
///
/// First folds every observed remote stamp into the clock via
/// `stamp.wall_ms`-injected `recv` (`observe_remote_hlc` reads the injected
/// wall, never the live clock).
///
/// CRDT MERGE: the merge is PURE — `state.documents` holds the
/// in-memory CRDT, so `doc.merge` / `incorporate_pending_edits` / `doc.content()`
/// run in-core. The merged content is written via
/// `Effect::WriteFile` (gated by `should_skip_remote_write` read against the
/// shadow + recorded inode, NOT live disk) and persisted via `Effect::SaveDoc`;
/// any local divergence the merge surfaced is shipped back as `Effect::SendOps`.
/// The zero-content "ensure the file exists" path needs no
/// merge and is handled separately below.
// The args are the independent fields of the `RemoteOps` event this dispatch
// fn unpacks (payload + content-mode + local divergence + catch-up snapshot +
// stamp); bundling them into a struct would just re-spread them at every arm.
#[allow(clippy::too_many_arguments)]
pub(super) fn handle_remote_ops(
    state: &mut SpaceState,
    document_id: &str,
    ops: &[u8],
    metadata: &[ChangeMetadata],
    content_mode: i32,
    local_content: Option<&str>,
    author_by_agent_snapshot: &std::collections::HashMap<String, String>,
    stamp: EventStamp,
) -> Vec<Effect> {
    // Advance the origin clock past every observed remote stamp so a
    // lifecycle/edit op this daemon emits afterward is causally after them.
    for m in metadata {
        observe_remote_hlc(state, Some(m), stamp);
    }

    // CONTENT-MODE fork: blob ops take the pure LWW merge; text falls
    // through to the CRDT merge. Both observe their remote stamps above,
    // before either merge runs.
    let check = kutl_proto::sync::SyncOps {
        content_mode,
        ..Default::default()
    };
    if is_blob_mode(&check) {
        return merge_remote_blob(state, document_id, ops, metadata);
    }

    // Skip a content op for an untracked UUID (locally deleted or not yet
    // registered): materializing it would diverge (the edit-vs-delete race).
    if is_untracked_uuid(state, document_id) {
        debug!(%document_id, "skipping remote content op for untracked document");
        return Vec::new();
    }

    // Empty ops = the document exists but has no content yet. Ensure the file
    // exists on disk — write only when it is ABSENT (a possibly zero-byte file),
    // the one remote-text path that needs no CRDT merge. The driver's write
    // funnel hashes the (empty) bytes and registers the echo suppression.
    // Prefer-shadow: a displaced doc's file lives at its conflict path, not
    // the `uuid_to_path` original (`SpaceState::doc_disk_path`); a merge
    // written at the original would clobber the WINNER's file there.
    let Some(rel) = state.doc_disk_path(document_id) else {
        debug!(%document_id, empty = ops.is_empty(), "no local path for remote op; ignoring");
        return Vec::new();
    };
    if ops.is_empty() {
        // Already present → nothing to do.
        if state.shadow.occupied(&rel) {
            return Vec::new();
        }
        let content = Vec::new();
        return vec![Effect::WriteFile {
            rel,
            content: HashedContent::new(content),
        }];
    }

    // Non-empty ops: merge purely against the in-memory CRDT.
    merge_remote_ops(
        state,
        document_id,
        &rel,
        ops,
        metadata,
        local_content,
        author_by_agent_snapshot,
    )
}

/// Merge a non-empty remote-ops payload into the in-memory CRDT and emit the
/// resulting disk + persistence + delta effects. Pure: the CRDT lives in
/// `state.documents`, so `incorporate_pending_edits` / `doc.merge` / `doc.content`
/// run in-core with no IO.
///
/// LOCAL-BEFORE-REMOTE: pending local edits are incorporated BEFORE the
/// remote merge so the CRDT transforms concurrent positions correctly.
/// The divergence source is `local_content` —
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
    author_by_agent_snapshot: &std::collections::HashMap<String, String>,
) -> Vec<Effect> {
    let agent_name = state.agent_name.clone();
    let author_did = state.author_did.clone();
    // Read before `doc` borrows the state: known bytes with the CRDT behind
    // them (see `known_bytes_ahead_of_crdt`) are not pending local edits.
    let disk_is_known = local_content.is_some_and(|c| state.known_bytes_ahead_of_crdt(rel, c));
    // A retired engine (unreadable sidecar) is being refilled by this merge:
    // the file can only be diffed against real content AFTER it lands. The
    // mark is read here and spent only once the merge succeeds; a rejected
    // payload leaves it for the next delivery.
    let refill = state.awaiting_content.contains(document_id);
    let doc = state.load_or_create_doc(document_id);

    // Incorporate pending local edits BEFORE merging remote ops. The
    // divergence source is the driver's live-disk read (`local_content`); absent
    // it, fall back to the in-memory doc content (a no-op) so the ordering is
    // preserved for callers that cannot read disk (the sim).
    let current_content = local_content.map_or_else(|| doc.content(), str::to_owned);
    let mut local_delta = if refill {
        None
    } else if disk_is_known {
        debug!(
            %document_id,
            rel = %rel.display(),
            "disk matches this daemon's last write; CRDT is behind it, nothing local to incorporate"
        );
        None
    } else {
        incorporate_pending_edits(doc, &agent_name, &author_did, &current_content)
    };

    // Merge the remote ops, then decide whether anything actually changed: a
    // redundant merge (our own ops echoed back via catch-up) must NOT write +
    // suppress, or it risks swallowing a concurrent local edit's watcher event.
    let version_before = doc.local_version();
    if let Err(e) = doc.merge(ops, metadata) {
        return reject_remote_ops(state, document_id, rel, ops.len(), &e);
    }
    if refill {
        // Diffed against the refilled content, an edit made while the sidecar
        // was unreadable is exactly the edit it is; diffed against the empty
        // engine before the merge it would have re-inserted the whole file.
        // Only the driver's disk read is a baseline here: the pre-merge
        // fallback was the empty engine's content, and diffing the refilled
        // engine against that would erase the document and ship the erasure.
        local_delta = local_content
            .and_then(|disk| incorporate_pending_edits(doc, &agent_name, &author_did, disk));
        debug!(
            %document_id,
            rel = %rel.display(),
            local_edit = local_delta.is_some(),
            "refilled a retired engine from the relay; the file was diffed against it"
        );
    }
    // Install the catch-up author snapshot. On a join (or a
    // cold-loaded relay re-seed), the relay attaches its uncapped agent→author
    // map here; the FIFO-capped per-change `metadata` folded by `doc.merge`
    // above no longer carries an evicted survivor's binding, so blame would show
    // "unknown" without this union. Insert-if-absent (single-minter invariant),
    // additive, and off the hot path: incremental deltas carry an empty map, so
    // this is a no-op on the steady-state broadcast path.
    if !author_by_agent_snapshot.is_empty() {
        doc.merge_author_map(author_by_agent_snapshot.clone());
    }
    // A successful merge can still OVERSHOOT the cap (one big create lands the
    // doc over it in a single bounded merge — the core warns); record/clear
    // the status flag from the post-merge count so every replica that absorbs
    // an over-cap create flags it immediately, not at its first rejected edit.
    let ops_after_merge = doc.op_count();
    let version_after = doc.local_version();
    if refill {
        state.awaiting_content.remove(document_id);
    }
    let state_changed = local_delta.is_some() || version_after != version_before;

    // Advance last_synced to the post-merge version so a later local edit's delta
    // is computed from here, not re-encoding the just-merged remote ops.
    // This holds in BOTH the skip-write and normal branches.
    if state_changed {
        state.last_synced.insert(rel.to_path_buf(), version_after);
    }

    let mut effects = Vec::new();

    if note_op_cap_status(state, document_id, ops_after_merge, &mut effects) {
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
        // event. With `version_after == version_before`, a re-persist here would
        // write an unchanged CRDT to the same version — inert IO — so the core
        // also elides it (no `SaveDoc`, no last_synced churn).
        return effects;
    }

    // Compute the merged content lazily — only now that we know a write may be
    // needed.
    let content = state
        .get_doc(document_id)
        .map_or_else(String::new, kutl_core::Document::content);
    let content_bytes = content.into_bytes();

    // should_skip_remote_write against the shadow + recorded
    // inode, NOT live disk: a file gone from disk but with a known inode is a
    // local rename/delete — merge the CRDT and persist, but do NOT recreate it.
    let file_on_disk = state.shadow.occupied(rel);
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
        // or survives a cluster delete.
        //
        // A file-absent probe cannot serve as the trigger here: the
        // placement cascade marks the path occupied in the shadow BEFORE the content
        // op lands (so `file_on_disk` is already true here). Gating on the recorded
        // inode being absent is the placement-order-independent signal:
        // it fires exactly once, on the first write that records the inode, and is
        // not hot (the inode is stable across later edits, which keep it `Some`).
        let inode_not_yet_persisted = identity_inode.is_none();
        effects.push(Effect::WriteFile {
            rel: rel.to_path_buf(),
            content: HashedContent::new(content_bytes),
        });
        if inode_not_yet_persisted {
            effects.push(Effect::SaveState);
        }
    }

    // Persist the merged CRDT AFTER the write, deliberately: the driver
    // applies effects in order, and a kill between the two picks which torn
    // state a restart sees. File-ahead-of-sidecar makes the startup scan
    // re-incorporate on-disk text it cannot find in the CRDT — content
    // DUPLICATED, nothing lost. Sidecar-ahead-of-file leaves the placed
    // (still empty) file behind a full CRDT, and the scan reads the empty as
    // a deliberate truncation — content ERASED cluster-wide. Duplication is
    // recoverable; deletion is not. Two journaled hashes narrow the
    // duplication window: the last-written hash (bytes this daemon last
    // wrote or observed) and the write intent the funnel journals before
    // the rename, so a landed file whose sidecar never followed is
    // recognised as this daemon's own materialization and fetched back from
    // the relay rather than re-incorporated. What remains is a kill between
    // the intent line and the rename (inert: the bytes never matched) and a
    // user edit landing in the instant between the rename and the kill,
    // which incorporates as the edit it is.
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
    use crate::core::casefold;
    use crate::core::handle::test_support::{
        doc_id, fill_doc_to_op_cap, register_identity_only, remote_ops_for, st, track_remote,
    };
    use crate::core::{DaemonCore, Event};

    /// An engine panic on remote ops retires the in-memory engine: reload
    /// from the sidecar, then re-subscribe so the catch-up restores what the
    /// sidecar lacks. An ordinary decode error (the control) drops the
    /// payload and emits nothing.
    #[test]
    fn test_engine_panic_on_remote_ops_reloads_and_resubscribes() {
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "doc.md", 1);
        let rel = Path::new("doc.md");

        let decode = kutl_core::Error::Decode("bad magic".into());
        assert!(reject_remote_ops(&mut s, &id, rel, 3, &decode).is_empty());

        let panicked = kutl_core::Error::EnginePanicked {
            message: "index out of bounds".into(),
        };
        let effects = reject_remote_ops(&mut s, &id, rel, 3, &panicked);
        assert!(
            matches!(
                &effects[..],
                [Effect::ReloadDoc { document_id: reload }, Effect::Subscribe { document_id: sub }]
                    if *reload == id && *sub == id
            ),
            "reload then re-subscribe, got {effects:?}"
        );
    }

    #[test]
    fn test_remote_ops_advances_clock_then_no_clock_read_in_core() {
        // RemoteOps folds the observed HLC into the clock via stamp,
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
                author_by_agent_snapshot: std::collections::HashMap::new(),
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: Some(hlc),
                },
            },
        );
        // Empty ops/metadata => no merge effect, but the call is pure & total.
        assert!(s.hlc.last() >= before);
    }

    // ── The content-merge arms ──────────────────────────────────────────────

    /// The prefer-shadow accessor: a DISPLACED doc's
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
                author_by_agent_snapshot: std::collections::HashMap::new(),
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

    /// The crash-window door on the MERGE path: after a restart adopted a file
    /// whose sidecar never landed, the CRDT is loaded but empty and the file
    /// holds the remote content. When catch-up delivers the ops, the pre-merge
    /// incorporation must NOT diff the file in as a local edit (the ops carry
    /// that content). The last-written hash is the evidence: disk bytes that
    /// match it are known, not pending. Without the hash the doubling
    /// reproduces, which is what makes this test's first half load-bearing.
    #[test]
    fn test_remote_ops_merge_skips_incorporating_disk_that_matches_last_write() {
        const CONTENT: &str = "burst file 83\n";
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "a.md");
        s.load_or_create_doc(&id);
        s.identity_set_written_hash(
            Path::new("a.md"),
            crate::blob_state::sha256_hex(CONTENT.as_bytes()),
        );
        let (ops, metadata) = remote_ops_for(CONTENT);
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: Some(CONTENT.to_owned()),
                author_by_agent_snapshot: std::collections::HashMap::new(),
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::SendOps { .. })),
            "known disk bytes are not a local edit: no ops go back to the relay, got {effects:?}"
        );
        assert_eq!(
            s.get_doc(&id).map(kutl_core::Document::content).as_deref(),
            Some(CONTENT),
            "the content lands exactly once"
        );

        // Control: the same merge with no recorded hash takes the old door and
        // doubles the content — the guard above is what prevents it.
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "a.md");
        let (ops, metadata) = remote_ops_for(CONTENT);
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: Some(CONTENT.to_owned()),
                author_by_agent_snapshot: std::collections::HashMap::new(),
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(e, Effect::SendOps { .. })),
            "control: without the hash the disk bytes incorporate as a local edit"
        );
        assert_eq!(
            s.get_doc(&id).map(kutl_core::Document::content).as_deref(),
            Some("burst file 83\nburst file 83\n"),
            "control: the old door doubles the content"
        );
    }

    #[test]
    fn test_remote_ops_merge_on_tracked_doc_emits_write_and_savedoc() {
        // A non-empty remote-ops merge on a tracked doc materializes the merged
        // content (Effect::WriteFile) and persists the CRDT (Effect::SaveDoc).
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
                author_by_agent_snapshot: std::collections::HashMap::new(),
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::WriteFile { rel, content }
                    if rel.as_path() == Path::new("a.md") && content.bytes() == b"hello world".as_slice()
            )),
            "remote merge must write the merged content, got {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e, Effect::SaveDoc { document_id } if document_id == &id
            )),
            "remote merge must persist the CRDT via SaveDoc, got {effects:?}"
        );
        // Crash-ordering pin: the file lands BEFORE the sidecar persists,
        // DELIBERATELY. A kill between the two decides which torn state a
        // restart sees: file-ahead-of-sidecar re-incorporates and DUPLICATES
        // content (recoverable); sidecar-ahead-of-file leaves the placed
        // empty file reading as a truncation and ERASES the doc cluster-wide.
        // Flipping this order trades duplication for deletion — do not.
        let save_idx = effects
            .iter()
            .position(|e| matches!(e, Effect::SaveDoc { .. }))
            .expect("SaveDoc present");
        let write_idx = effects
            .iter()
            .position(|e| matches!(e, Effect::WriteFile { .. }))
            .expect("WriteFile present");
        assert!(
            write_idx < save_idx,
            "WriteFile must precede SaveDoc (duplication beats deletion on a crash), got {effects:?}"
        );
        // First materialization: the doc was registered with a null inode (file
        // absent at SubscribeRemote time), so the landing WriteFile records the real
        // inode and the merge must emit SaveState to PERSIST it — otherwise the
        // inode lives only in memory and `detect_offline_renames` reads `null` on
        // rejoin, turning an offline rename into a phantom new doc
        // or surviving a cluster delete. SaveState must come AFTER WriteFile
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
                author_by_agent_snapshot: std::collections::HashMap::new(),
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
        // bounded merge: the merge SUCCEEDS, but
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
                author_by_agent_snapshot: std::collections::HashMap::new(),
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
                author_by_agent_snapshot: std::collections::HashMap::new(),
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
                author_by_agent_snapshot: std::collections::HashMap::new(),
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
        // When the event carries live-disk bytes that differ from the CRDT,
        // those bytes are diffed in BEFORE the remote merge and shipped back as a
        // SendOps delta — the local edit is not silently dropped.
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
                author_by_agent_snapshot: std::collections::HashMap::new(),
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
            "the diverged local content must ship as a SendOps delta, got {effects:?}"
        );
    }

    #[test]
    fn test_remote_ops_installs_catch_up_author_snapshot() {
        // A catch-up envelope carries the relay's uncapped agent→author-DID
        // snapshot for agents the FIFO-capped per-change `metadata` no longer
        // holds (an evicted survivor / cold-loaded-relay re-seed). The daemon
        // must UNION that snapshot into the doc's author map so blame resolves
        // the evicted agent instead of "unknown". The snapshot binds
        // "evicted-agent", which the merged ops (agent "peer") never carry.
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "a.md");
        let (ops, metadata) = remote_ops_for("hello world");
        let mut snapshot = std::collections::HashMap::new();
        snapshot.insert("evicted-agent".to_owned(), "did:evicted".to_owned());
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: None,
                author_by_agent_snapshot: snapshot,
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        let doc = s.get_doc(&id).expect("doc materialized by the merge");
        assert_eq!(
            doc.author_of("evicted-agent"),
            "did:evicted",
            "the catch-up snapshot must install the evicted survivor's binding, \
             else blame shows \"unknown\""
        );
    }

    /// A refill with no disk read must not diff the refilled engine against
    /// the pre-merge fallback (the empty engine's content): that would erase
    /// the document and ship the erasure. The merge writes the refilled
    /// content, ships nothing local, and spends the mark.
    #[test]
    fn test_refill_merge_without_a_disk_read_ships_no_erasure() {
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "a.md");
        s.awaiting_content.insert(id.clone());
        let (ops, metadata) = remote_ops_for("refilled text");
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: None,
                author_by_agent_snapshot: std::collections::HashMap::new(),
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::SendOps { .. })),
            "nothing local to ship on a refill without a disk read, got {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::WriteFile { rel, content }
                    if rel.as_path() == Path::new("a.md") && content.bytes() == b"refilled text".as_slice()
            )),
            "the refilled content lands on disk, got {effects:?}"
        );
        assert_eq!(
            s.get_doc(&id).map(kutl_core::Document::content).as_deref(),
            Some("refilled text"),
            "the engine holds the relay's content, not an erasure"
        );
        assert!(
            !s.awaiting_content.contains(&id),
            "a successful refill spends the mark"
        );
    }

    /// The refill's re-diff runs against the driver's disk read: an edit made
    /// while the sidecar was unreadable is exactly the edit it is.
    #[test]
    fn test_refill_merge_diffs_the_disk_read_against_the_refilled_engine() {
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "a.md");
        s.awaiting_content.insert(id.clone());
        let (ops, metadata) = remote_ops_for("refilled text");
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: Some("refilled text plus local".to_owned()),
                author_by_agent_snapshot: std::collections::HashMap::new(),
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
            "the local edit is diffed against the refill and shipped, got {effects:?}"
        );
        assert_eq!(
            s.get_doc(&id).map(kutl_core::Document::content).as_deref(),
            Some("refilled text plus local"),
            "the engine holds the refill plus the local edit, once"
        );
    }

    /// A payload the engine rejects refills nothing, so the mark stays for
    /// the next delivery; spending it here would let that delivery diff the
    /// whole file into the empty engine.
    #[test]
    fn test_rejected_payload_keeps_the_refill_mark() {
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "a.md");
        s.awaiting_content.insert(id.clone());
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops: vec![0xff; 16],
                metadata: Vec::new(),
                content_mode: 0,
                local_content: None,
                author_by_agent_snapshot: std::collections::HashMap::new(),
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            s.awaiting_content.contains(&id),
            "a rejected payload leaves the document awaiting its refill"
        );
    }
}
