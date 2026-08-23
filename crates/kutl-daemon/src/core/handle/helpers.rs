//! Shared helpers ported from `SpaceWorker` — identity/lifecycle bookkeeping
//! and the content-merge helpers — all pure over [`SpaceState`].

use kutl_core::lattice::DocRecord;
use kutl_core::{Hlc, ms_u64_to_i64_saturating};
use kutl_proto::sync::ChangeMetadata;
use tracing::{error, warn};
use uuid::Uuid;

use crate::core::{Effect, EventStamp, FileIdentity, SpaceState, rel_path_to_string};

// ── helpers ported from SpaceWorker (now &mut SpaceState, no IO) ─────────────

/// Port of `get_or_create_uuid` (daemon/identity.rs), minus the live inode read
/// (the inode lands in the shadow on shell-ACK of the placement, graft 3).
pub(super) fn get_or_create_uuid(state: &mut SpaceState, rel: &std::path::Path) -> String {
    if let Some(identity) = state.file_identity.get(rel) {
        return identity.document_uuid.clone();
    }
    let uuid = Uuid::new_v4().to_string();
    // A LOCAL create is unconfirmed until the relay acks it (the `confirmed`
    // flag `register_identity` records).
    register_identity(state, rel, uuid.clone(), /* confirmed */ false);
    uuid
}

/// Track a REMOTE document's identity at `rel` (the relay already holds it, so it
/// is `confirmed`). Mirrors `register_identity(.., /* confirmed */ true)`
/// (daemon/identity.rs).
pub(super) fn register_identity_remote(
    state: &mut SpaceState,
    rel: &std::path::Path,
    document_uuid: &str,
) {
    register_identity(
        state,
        rel,
        document_uuid.to_owned(),
        /* confirmed */ true,
    );
}

/// Mark a tracked document as confirmed by the relay (monotone). Port of the
/// former imperative `confirm_document`. Returns `true` iff this call actually
/// FLIPPED the doc from unconfirmed to confirmed, so the caller can emit
/// `Effect::SaveState` to persist the flip (the imperative `confirm_document`
/// `save_state`d on the same condition). A no-op re-confirm
/// returns `false` so we don't churn `state.json`.
pub(super) fn confirm_document(state: &mut SpaceState, document_uuid: &str) -> bool {
    let Some(rel) = state.uuid_to_path.get(document_uuid) else {
        return false;
    };
    let path_str = rel_path_to_string(rel);
    state.state.confirm(&path_str)
}

/// Port of `register_identity` (daemon/identity.rs), minus the inode read +
/// persistence (the driver persists on the resulting `Effect::SaveState`; the
/// inode lands in the shadow on shell-ACK). Records the path↔id mapping and the
/// `DaemonState` document entry. `confirmed` is `false` for a local create
/// (awaiting the relay ack) and `true` for a remote document the relay already
/// holds.
///
/// `pub`: the sim's shell (`DaemonSim::apply_guarded_place`) claims identity at
/// place time with this exact transition, mirroring the real driver's claim in
/// `place_now` — identity-reading driver glue (the §1.3 remote-rename drain,
/// the inode-rename detector) consults these maps.
pub fn register_identity(
    state: &mut SpaceState,
    rel: &std::path::Path,
    document_uuid: String,
    confirmed: bool,
) {
    let path_str = rel_path_to_string(rel);
    // A `last_synced` frontier is only meaningful against the document it was
    // recorded for. Binding a DIFFERENT document at this path (or claiming a
    // path with an orphaned entry) invalidates it — inheriting it would feed
    // the newcomer's delta encode times its oplog does not contain. Same-doc
    // re-registration keeps the frontier: no forced full re-encode.
    let rebinding = state
        .file_identity
        .get(rel)
        .is_none_or(|id| id.document_uuid != document_uuid);
    if rebinding {
        state.last_synced.remove(rel);
    }
    state.file_identity.insert(
        rel.to_path_buf(),
        FileIdentity {
            document_uuid: document_uuid.clone(),
            inode: None,
        },
    );
    state.state.set(path_str, document_uuid.clone(), confirmed);
    state.uuid_to_path.insert(document_uuid, rel.to_path_buf());
}

/// Drop every trace of a document from the identity maps.
///
/// The single implementation: the imperative shell calls this and then saves,
/// rather than keeping its own copy. Nothing in here touches the filesystem, so
/// there was never a second body to justify — and while there were two, they
/// drifted three separate ways (a missing successor guard, an unpruned register
/// stamp, a stale op-cap flag), each one silent.
///
/// Path-keyed rows come out only while they still belong to `document_uuid`. A
/// successor document may have re-claimed the path between this document's
/// teardown and this call: startup executes the old document's delete, a later
/// subscribe registers the new document at the same path, and the deferred
/// unregister of the old id runs after both. Removing by path unconditionally
/// would destroy the successor's identity.
pub(crate) fn unregister_identity(state: &mut SpaceState, document_uuid: &str) {
    let Some(rel) = state.uuid_to_path.remove(document_uuid) else {
        return;
    };
    let path_still_ours = state
        .file_identity
        .get(&rel)
        .is_some_and(|id| id.document_uuid == document_uuid);
    if path_still_ours {
        state.file_identity.remove(&rel);
        state.state.documents.remove(&rel_path_to_string(&rel));
    }
    // Prune the register stamp with the doc; a dead doc's floor has no further
    // use, and leaving it grows state.json without bound.
    state.register_hlc.remove(document_uuid);
    // An unregistered doc can't be at (or approaching) the op cap anymore.
    // Keyed by id, so no path-domain concerns.
    state.state.at_op_cap.remove(document_uuid);
    state.state.approaching_op_cap.remove(document_uuid);
}

/// Port of `move_identity` (daemon/identity.rs), minus the inode read + persistence.
///
/// `pub` for the same reason as [`register_identity`]: the sim's shell re-keys
/// identity on a placed rename exactly as the real driver does.
pub fn move_identity(
    state: &mut SpaceState,
    old: &std::path::Path,
    new: std::path::PathBuf,
    document_uuid: &str,
) {
    let new_path_str = rel_path_to_string(&new);
    let old_path_str = rel_path_to_string(old);
    let old_inode = state.file_identity.get(old).and_then(|id| id.inode);
    state.file_identity.remove(old);
    state.file_identity.insert(
        new.clone(),
        FileIdentity {
            document_uuid: document_uuid.to_owned(),
            // Carry the recorded inode forward; the shell refreshes it on ACK.
            inode: old_inode,
        },
    );
    let confirmed = state
        .state
        .documents
        .get(&old_path_str)
        .is_some_and(|e| e.confirmed);
    state.state.documents.remove(&old_path_str);
    state
        .state
        .set(new_path_str, document_uuid.to_owned(), confirmed);
    // A renamed BLOB keeps its LWW state under the doc's current path (the
    // newer-wins guard resolves by path). Pure in-memory re-key; the next blob
    // op's save persists it (the imperative twin persists inline).
    state.blob_state.rename(old, &new);
    // `last_synced` is path-keyed but its frontier is local times of the
    // document bound at the path — it must follow the document. A frontier
    // left behind at `old` poisons whatever doc later binds there, and a
    // frontier STRANDED at `new` by a prior occupant must not be inherited
    // by the mover: either way the next edit's delta encode would walk
    // history with times the bound oplog does not contain.
    if let Some(v) = state.last_synced.remove(old) {
        state.last_synced.insert(new.clone(), v);
    } else {
        state.last_synced.remove(&new);
    }
    state.uuid_to_path.insert(document_uuid.to_owned(), new);
}

/// Tear down a document's per-path state: last-synced frontier, blob state,
/// and the in-memory CRDT document itself. Dropping the document matters —
/// a cleaned-up doc left resident is resurrection bait, revived by the next
/// `load_or_create_doc` hit instead of minting fresh. The one thing that
/// stays with the shell is the on-disk `.dt` sidecar removal (IO).
///
/// Must run while `file_identity[rel]` still holds the doc's binding — it
/// resolves the document id through it.
pub(crate) fn cleanup_document_state(state: &mut SpaceState, rel: &std::path::Path) {
    state.last_synced.remove(rel);
    state.blob_state.remove(rel);
    if let Some(id) = state
        .file_identity
        .get(rel)
        .map(|i| i.document_uuid.clone())
    {
        state.remove_doc_in_memory(&id);
    }
}

/// Parse a document id to a [`Uuid`], falling back to the nil UUID for a
/// non-parseable (legacy) id. Used only to probe membership in the
/// `Uuid`-keyed `exempt_revival` set — a nil id is never a revival, so the
/// fallback simply reads as "not exempt".
pub(super) fn parsed_id(document_id: &str) -> Uuid {
    Uuid::parse_str(document_id).unwrap_or(Uuid::nil())
}

/// Port of `is_untracked_uuid` (daemon.rs): a content op for a document we
/// hold no path for whose id is a real UUID — locally deleted or not yet
/// registered. Skipped, never coerced to a garbage `<uuid>` file.
pub(super) fn is_untracked_uuid(state: &SpaceState, document_id: &str) -> bool {
    !state.uuid_to_path.contains_key(document_id) && Uuid::parse_str(document_id).is_ok()
}

/// Port of `observe_remote_hlc` (daemon.rs) reading the injected
/// `stamp.wall_ms` rather than `skewed_now_ms_u64`: advance the origin clock past
/// an observed remote stamp so any op this daemon emits afterward is causally
/// ordered after it.
pub(super) fn observe_remote_hlc(
    state: &mut SpaceState,
    meta: Option<&ChangeMetadata>,
    stamp: EventStamp,
) {
    // A malformed remote hlc is dropped (`observe_remote_hlc` in daemon.rs logs +
    // ignores it).
    if let Some(wire) = meta.and_then(|m| m.hlc.clone())
        && let Ok(remote) = Hlc::try_from(wire)
    {
        state.hlc.recv(remote, stamp.wall_ms);
    }
}

/// Port of `make_lifecycle_metadata` (daemon.rs): build metadata for a
/// LOCAL lifecycle op and fold its HLC into this document's watermark. Stamps
/// against the injected `stamp.wall_ms` via `state.hlc.tick` (porting
/// `make_metadata`, daemon.rs) rather than reading a clock.
/// Tick the origin clock against the injected wall and build the wire
/// `ChangeMetadata` for a LOCAL op — the shared stamp-and-build half of every
/// local emitter (lifecycle metadata, the register effect, the blob send).
/// Returns the typed [`Hlc`] alongside, because what each caller RECORDS with
/// it differs deliberately (watermark vs register floor vs nothing — see the
/// two-mechanism note on [`SpaceState::lifecycle_event_is_fresh`]).
pub(super) fn stamp_metadata(
    state: &mut SpaceState,
    intent: &str,
    stamp: EventStamp,
) -> (ChangeMetadata, Hlc) {
    let wall = stamp.wall_ms;
    let hlc = state.hlc.tick(wall);
    let meta = ChangeMetadata {
        timestamp: ms_u64_to_i64_saturating(wall),
        author_did: state.author_did.clone(),
        intent: intent.into(),
        hlc: Some(hlc.into()),
        ..Default::default()
    };
    (meta, hlc)
}

pub(super) fn make_lifecycle_metadata(
    state: &mut SpaceState,
    document_id: &str,
    intent: &str,
    stamp: EventStamp,
) -> ChangeMetadata {
    let (meta, hlc) = stamp_metadata(state, intent, stamp);
    // note_local_lifecycle_hlc (daemon.rs): record the watermark so a stale
    // remote echo of this now-superseded op is later dropped.
    state.record_lifecycle_hlc(document_id, hlc);
    meta
}

/// Build the `RegisterDocument` effect for a new local document and seed its
/// lattice record so `reconcile_placement` can place it.
///
/// Ports `register_and_subscribe`'s register half (daemon.rs). The
/// `Subscribe` half is emitted by `reconcile_placement` (the cascade sub-call).
///
/// BIRTHTIME: `register_and_subscribe` stats the filesystem birthtime;
/// that is IO, so the pure core emits `None` here and the
/// driver attaches the real birthtime when it applies the effect (Task 6).
pub(super) fn register_document_effect(
    state: &mut SpaceState,
    document_id: &str,
    rel: &std::path::Path,
    stamp: EventStamp,
) -> Effect {
    let (meta, hlc) = stamp_metadata(state, "file change", stamp);
    // Record the mint's register stamp as the causal-floor source for a later
    // local rename (persisted across restarts — the offline-rename re-emit
    // depends on it). Mirrors the imperative `register_and_subscribe`.
    state.record_register_hlc(document_id, hlc);
    // SYMMETRY: the registrant seeds its own lifecycle watermark with the mint
    // stamp, exactly what a remote REGISTER folds into every observer — so the
    // freshness gate's inputs are uniform across registrant and observers (the
    // asymmetry hid the third-observer floored-rename gate hole). SEED ONLY:
    // this is the fold-only `record_lifecycle_hlc` (monotonic max) — the mint
    // must never GATE on the watermark (that would drop revivals) and never
    // `recv` a peer stamp into the clock here (that would leak peer skew,
    // breaking §7.1 reproducibility).
    state.record_lifecycle_hlc(document_id, hlc);
    seed_record(
        state,
        document_id,
        &rel_path_to_string(rel),
        Some(hlc),
        None,
        None,
        None,
    );
    Effect::RegisterDocument {
        space_id: state.space_id.clone(),
        document_id: document_id.to_owned(),
        path: rel_path_to_string(rel),
        metadata: Some(meta),
    }
}

/// Fold one lifecycle observation into `known_records` so the placement cascade
/// (`reconcile_placement`) re-derives this document's placement. The single
/// `DocRecord`-construction funnel for every local and remote register/rename/
/// unregister (a non-UUID `document_id` is a no-op — `known_records` keys by
/// [`Uuid`]). The lattice's `observe` is a max-merge, so passing only the field
/// this op sets (registered/renamed/deleted) and `None` for the rest carries the
/// other watermarks forward unchanged.
pub(super) fn seed_record(
    state: &mut SpaceState,
    document_id: &str,
    path: &str,
    registered_hlc: Option<Hlc>,
    renamed_hlc: Option<Hlc>,
    rename_causal_floor: Option<Hlc>,
    deleted_hlc: Option<Hlc>,
) {
    let Ok(id) = Uuid::parse_str(document_id) else {
        return;
    };
    state.known_records.observe(DocRecord {
        document_id: id,
        path: path.to_owned(),
        registered_hlc,
        renamed_hlc,
        rename_causal_floor,
        touched_hlc: None,
        deleted_hlc,
        displaced: false,
    });
}

/// The causal floor for a LOCAL rename of `document_id`: the `registered_hlc`
/// this daemon recorded for the doc in `known_records`. Read BEFORE folding the
/// rename so it is the prior registration, not `max(register, this rename)`. Lets
/// the relay treat our rename as causally-after a clock-skewed registration (see
/// [`DocRecord::path_priority`]). `None` if the doc is untracked or unparsable.
pub(super) fn causal_floor_for(state: &SpaceState, document_id: &str) -> Option<Hlc> {
    let id = Uuid::parse_str(document_id).ok()?;
    state.known_records.get(&id).and_then(|r| r.registered_hlc)
}

/// Fold a register/rename observation for a remote document into `known_records`.
/// Thin path-typed wrapper over [`seed_record`] used by the lifecycle arms.
///
/// `rename_causal_floor` is the register HLC a RENAME observed (`None` for a
/// register/delete fold). It lets the placement lattice treat the rename as
/// causally-after a clock-skewed registration (see `DocRecord::path_priority`).
pub(super) fn seed_remote_record(
    state: &mut SpaceState,
    document_id: &str,
    rel: &std::path::Path,
    registered_hlc: Option<Hlc>,
    renamed_hlc: Option<Hlc>,
    rename_causal_floor: Option<Hlc>,
    deleted_hlc: Option<Hlc>,
) {
    seed_record(
        state,
        document_id,
        &rel_path_to_string(rel),
        registered_hlc,
        renamed_hlc,
        rename_causal_floor,
        deleted_hlc,
    );
}

/// Fold a remote unregister (delete) into `known_records` as a tombstone so the
/// cascade no longer projects a placement for it. `current_path` is unchanged by
/// a delete, so the freed path is carried through.
pub(super) fn observe_unregister_remote(
    state: &mut SpaceState,
    document_id: &str,
    rel: &std::path::Path,
    deleted_hlc: Option<Hlc>,
) {
    seed_remote_record(state, document_id, rel, None, None, None, deleted_hlc);
}

// ── content-merge helpers (ported from daemon.rs, made pure) ─────────────────

/// Diff new file `content` into a tracked document's CRDT and emit the delta +
/// persistence effects. Port of `handle_file_modified`'s non-empty `SendOps` body
/// (daemon.rs), made pure over `state.documents`.
///
/// Computes the delta against `last_synced[rel]` (the version last shipped to the
/// relay), updates `last_synced`, and — only when the edit produced ops — emits
/// `Effect::SendOps` (the delta) + `Effect::SaveDoc`. An identical-content edit is
/// a CRDT no-op and emits nothing.
pub(super) fn diff_into_crdt(
    state: &mut SpaceState,
    document_id: &str,
    rel: &std::path::Path,
    text: &str,
) -> Vec<Effect> {
    let agent_name = state.agent_name.clone();
    let author_did = state.author_did.clone();
    // Snapshot last_synced before the `doc` borrow (which borrows `state`).
    let since = state.last_synced.get(rel).cloned().unwrap_or_default();
    let doc = state.load_or_create_doc(document_id);
    let Ok(agent) = doc.register_agent(&agent_name) else {
        warn!(%document_id, "failed to register CRDT agent; local edit not shipped");
        return Vec::new();
    };
    let version_before = doc.local_version();
    if crate::bridge::apply_file_change(doc, agent, &author_did, text).is_err() {
        warn!(
            %document_id,
            rel = %rel.display(),
            "failed to apply local edit to CRDT; edit not shipped"
        );
        return Vec::new();
    }
    let version_after = doc.local_version();
    if version_before == version_after {
        // No CRDT change (identical content) — nothing to send or persist.
        return Vec::new();
    }
    let (ops, metadata) = encode_delta(doc, &since);
    let op_count = doc.op_count();
    let at_cap = op_count >= kutl_core::MAX_OPS_PER_DOC;
    if at_cap {
        // ERROR, not warn: the relay rejects every delta of an at-cap
        // document, so the user keeps editing a file that no longer syncs
        // anywhere. Fires once per content change of the affected file (the
        // op-cap twin of `classify_blob_bytes`'s blob-size error).
        error!(
            %document_id,
            rel = %rel.display(),
            cap = kutl_core::MAX_OPS_PER_DOC,
            "text document at the op cap; local edits no longer sync"
        );
        crate::metrics_calls::record_error(crate::metrics_calls::error_category::DOC_AT_OP_CAP);
    }
    state.last_synced.insert(rel.to_path_buf(), version_after);
    let mut effects = vec![
        Effect::SendOps {
            document_id: document_id.to_owned(),
            ops,
            metadata,
            content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
            content_hash: Vec::new(),
        },
        Effect::SaveDoc {
            document_id: document_id.to_owned(),
        },
    ];
    note_op_cap_status(state, document_id, op_count, &mut effects);
    effects
}

/// One owner for the op-cap status flags (`DaemonState::at_op_cap` and
/// `DaemonState::approaching_op_cap`), the persisted records `kutl status`
/// reads to surface frozen and nearly-frozen documents.
///
/// Derives both statuses from the document's current `op_count`: at
/// [`kutl_core::MAX_OPS_PER_DOC`] the document is frozen; at
/// [`kutl_core::OP_CAP_WARN_THRESHOLD`] (but under the cap) it is
/// approaching — the actionable window in which edits still sync. The sets
/// are kept disjoint. Records/clears the DOCUMENT ID — not a path: the flag
/// marks the document's history, and the detection sites see different path
/// domains (the watcher's disk path vs `doc_disk_path`'s conflict path for a
/// displaced doc), so a path key could record under one name and prune under
/// another. Ids survive renames for free; `kutl status` resolves id → path
/// at read time. Emits [`Effect::SaveState`] only on a transition so
/// steady-state edits don't churn the persisted state. Detection sites own
/// their at-cap `error!` lines (the local-edit and remote-merge directions
/// read differently); the approach crossing is warned here, once, because
/// every detection site should surface it identically.
pub(super) fn note_op_cap_status(
    state: &mut SpaceState,
    document_id: &str,
    op_count: usize,
    effects: &mut Vec<Effect>,
) {
    let at_cap = op_count >= kutl_core::MAX_OPS_PER_DOC;
    let approaching = !at_cap && op_count >= kutl_core::OP_CAP_WARN_THRESHOLD;
    let cap_changed = if at_cap {
        state.state.at_op_cap.insert(document_id.to_owned())
    } else {
        state.state.at_op_cap.remove(document_id)
    };
    let approach_changed = if approaching {
        state
            .state
            .approaching_op_cap
            .insert(document_id.to_owned())
    } else {
        state.state.approaching_op_cap.remove(document_id)
    };
    if approach_changed && approaching {
        // WARN, not error: edits still sync — this is the window in which
        // splitting or compacting the document avoids the freeze. The
        // persisted set makes the crossing fire once, not per edit.
        warn!(
            %document_id,
            op_count,
            cap = kutl_core::MAX_OPS_PER_DOC,
            "document is approaching the edit-history cap; at the cap its edits stop syncing"
        );
    }
    if cap_changed || approach_changed {
        effects.push(Effect::SaveState);
    }
}

/// Incorporate a pending local edit into the CRDT BEFORE a remote merge (FS-5).
/// Port of the former imperative `incorporate_pending_edits`, made pure: the
/// divergence source is the passed `current_content` (the in-memory `doc.content()`
/// for a tracked doc) rather than a live-disk read.
///
/// Diffs `current_content` against the CRDT's content and, if they differ,
/// applies the change and returns the encoded `(ops, metadata)` delta to ship.
/// Returns `None` when nothing diverges (the in-core case, since `current_content`
/// IS the doc's content) or the diff produced no ops. In the pure core this is a
/// structural no-op; the ordering is preserved so a driver that later carries the
/// real live-disk divergence on the event slots straight in (NOT dropped — FS-5 is
/// load-bearing for §1.3 convergence).
pub(super) fn incorporate_pending_edits(
    doc: &mut kutl_core::Document,
    agent_name: &str,
    author_did: &str,
    current_content: &str,
) -> Option<(Vec<u8>, Vec<ChangeMetadata>)> {
    if doc.content_eq(current_content) {
        return None;
    }
    let version_before = doc.local_version();
    let agent = doc.register_agent(agent_name).ok()?;
    crate::bridge::apply_file_change(doc, agent, author_did, current_content).ok()?;
    let (ops, meta) = encode_delta(doc, &version_before);
    if ops.is_empty() {
        None
    } else {
        Some((ops, meta))
    }
}

/// Whether a merged remote write should be SKIPPED. Port of the former
/// imperative `should_skip_remote_write`: a file gone from its expected
/// path but with a known inode is a local rename/delete — merge the CRDT, but do
/// NOT recreate the file (the watcher event for the new path handles it).
///
/// PURITY: `file_on_disk` is read from the in-memory shadow (occupant map), NOT a
/// live `exists()` probe; `inode` is the recorded `FileIdentity::inode`. The
/// `inode.is_some()` check is load-bearing — it distinguishes a file that existed
/// locally and is now gone (has a recorded inode) from a brand-new remote document
/// never written here (no inode → MUST be written, or initial sync breaks).
pub(super) fn should_skip_remote_write(file_on_disk: bool, inode: Option<u64>) -> bool {
    !file_on_disk && inode.is_some()
}

/// Encode `(ops, metadata)` as the CRDT delta since `since`. Port of
/// `encode_delta` (daemon.rs).
fn encode_delta(doc: &kutl_core::Document, since: &[usize]) -> (Vec<u8>, Vec<ChangeMetadata>) {
    (doc.encode_since(since), doc.changes_since(since))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::core::handle::test_support::{
        doc_id, fill_doc_to_op_cap, remote_ops_for, st, track_remote,
    };
    use crate::core::{DaemonCore, Event};

    /// Migrated from the imperative twin's suite at the procedural sunset:
    /// the `should_skip_remote_write` truth table. A new remote document
    /// (tracked, no recorded inode) MUST be written — gating on tracked-path
    /// presence alone would break initial peer sync; a locally-removed file
    /// (inode recorded, gone from disk) is NOT recreated; present files and
    /// untracked absent paths are always written.
    #[test]
    fn test_should_skip_remote_write_truth_table() {
        // New remote doc: absent + no inode => write.
        assert!(!should_skip_remote_write(false, None));
        // Locally removed (inode recorded, file gone) => skip.
        assert!(should_skip_remote_write(false, Some(42)));
        // Present file => always write.
        assert!(!should_skip_remote_write(true, Some(7)));
        assert!(!should_skip_remote_write(true, None));
    }

    /// A successor document that re-claimed the path keeps its identity when
    /// the predecessor's deferred unregister finally runs. Resolving by path
    /// alone would tear down the successor — the corruption this guard exists
    /// to prevent, and which the shell-side copy of this function guarded
    /// while this one did not.
    #[test]
    fn test_unregister_leaves_a_successors_claim_on_the_path_intact() {
        let mut s = st();
        let old = doc_id();
        let new = "22222222-2222-4222-8222-222222222222".to_owned();
        let rel = PathBuf::from("a.md");

        track_remote(&mut s, &old, "a.md", 10);
        // The successor registers at the same path before the old id's
        // unregister lands.
        register_identity(&mut s, &rel, new.clone(), true);

        unregister_identity(&mut s, &old);

        assert_eq!(
            s.file_identity.get(&rel).map(|i| i.document_uuid.as_str()),
            Some(new.as_str()),
            "the successor's identity must survive the predecessor's unregister"
        );
        assert!(
            s.state.documents.contains_key("a.md"),
            "the path must still be tracked for the successor"
        );
        assert_eq!(
            s.uuid_to_path.get(&new),
            Some(&rel),
            "the successor's id must still resolve to the path"
        );
        assert!(
            !s.uuid_to_path.contains_key(&old),
            "the predecessor's own id row is always dropped"
        );
    }

    /// Id-keyed rows are pruned with the document. Left behind, the register
    /// stamp grows state.json without bound and the op-cap flags keep
    /// reporting a document that no longer exists.
    #[test]
    fn test_unregister_prunes_the_id_keyed_rows() {
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        s.record_register_hlc(&id, s.hlc.last());
        s.state.at_op_cap.insert(id.clone());
        s.state.approaching_op_cap.insert(id.clone());

        unregister_identity(&mut s, &id);

        assert!(!s.register_hlc.contains_key(&id), "register stamp pruned");
        assert!(!s.state.at_op_cap.contains(&id), "at-cap flag pruned");
        assert!(
            !s.state.approaching_op_cap.contains(&id),
            "approaching-cap flag pruned"
        );
    }

    #[test]
    fn test_local_edit_at_cap_flags_doc_and_still_ships_delta() {
        // diff_into_crdt's at-cap detection: the local edit still applies and
        // ships (behavior unchanged — the relay rejects it there and acks
        // QUOTA_EXCEEDED), but the freeze is recorded loudly and durably.
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        fill_doc_to_op_cap(&mut s, &id);
        // APPEND to the existing content (edit distance 1): a replacement
        // here would Myers-diff a 1M-char doc against unrelated text, which
        // is O(N·D) ≈ N² — minutes of test wall-clock for no extra coverage.
        let mut typed = s
            .get_doc(&id)
            .map(kutl_core::Document::content)
            .unwrap_or_default()
            .into_bytes();
        typed.push(b'z');
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("a.md"),
                content: Some(typed),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            s.state.at_op_cap.contains(&id),
            "an at-cap local edit must record the doc in at_op_cap"
        );
        assert!(
            effects.iter().any(|e| matches!(e, Effect::SaveState)),
            "the at-cap flag must persist via SaveState, got {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(e, Effect::SendOps { .. })),
            "behavior preserved: the delta still ships (relay rejects it), got {effects:?}"
        );
    }

    #[test]
    fn test_local_edit_below_cap_clears_stale_at_op_cap_flag() {
        // The heal direction: a doc recorded at-cap under an OLDER (lower) cap
        // value syncs again after a cap raise. The first successful edit must
        // clear the persisted flag (and SaveState the removal) so `kutl
        // status` stops reporting a document that is no longer frozen.
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        s.state.at_op_cap.insert(id.clone());
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("a.md"),
                content: Some(b"small edit".to_vec()),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !s.state.at_op_cap.contains(&id),
            "a successful below-cap edit must clear the stale flag"
        );
        assert!(
            effects.iter().any(|e| matches!(e, Effect::SaveState)),
            "the flag removal must persist via SaveState, got {effects:?}"
        );
    }

    #[test]
    fn test_at_op_cap_savestate_fires_only_on_transitions() {
        // The helper's no-churn claim, negative arms: SaveState rides at-cap
        // TRANSITIONS only. A second rejected merge on an already-flagged
        // doc, and an ordinary below-cap edit on an unflagged doc, must both
        // emit NO SaveState.
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        fill_doc_to_op_cap(&mut s, &id);
        let (ops, metadata) = remote_ops_for("peer edit");
        let first = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops: ops.clone(),
                metadata: metadata.clone(),
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
            first.iter().any(|e| matches!(e, Effect::SaveState)),
            "the first rejection is the transition, got {first:?}"
        );
        let second = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops,
                metadata,
                content_mode: 0,
                local_content: None,
                author_by_agent_snapshot: std::collections::HashMap::new(),
                stamp: EventStamp {
                    wall_ms: 60,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !second.iter().any(|e| matches!(e, Effect::SaveState)),
            "a steady-state rejection must not churn SaveState, got {second:?}"
        );

        // Unflagged doc, steady-state below-cap edit: no SaveState. The
        // FIRST content event legitimately emits one (the pre-seed register
        // persists register_hlc), so seed with an initial edit and assert on
        // the second.
        let mut s2 = st();
        let id2 = doc_id();
        track_remote(&mut s2, &id2, "b.md", 10);
        let _seed = DaemonCore::handle(
            &mut s2,
            Event::FileModified {
                rel: PathBuf::from("b.md"),
                content: Some(b"first edit".to_vec()),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: None,
                },
            },
        );
        let effects = DaemonCore::handle(
            &mut s2,
            Event::FileModified {
                rel: PathBuf::from("b.md"),
                content: Some(b"second edit".to_vec()),
                stamp: EventStamp {
                    wall_ms: 110,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::SaveState)),
            "a steady-state below-cap edit must not emit SaveState, got {effects:?}"
        );
    }

    #[test]
    fn test_note_op_cap_status_three_state_transitions() {
        // The single owner's full ladder: below-warn → approaching → at-cap →
        // cleared. The two persisted sets stay DISJOINT at every rung (a doc
        // is approaching XOR frozen, never both) and SaveState rides only the
        // transitions.
        let save_states = |effects: &[Effect]| {
            effects
                .iter()
                .filter(|e| matches!(e, Effect::SaveState))
                .count()
        };
        let mut s = st();
        let id = doc_id();
        let mut effects = Vec::new();

        // Below the warn threshold: nothing recorded, no SaveState.
        note_op_cap_status(
            &mut s,
            &id,
            kutl_core::OP_CAP_WARN_THRESHOLD - 1,
            &mut effects,
        );
        assert!(!s.state.approaching_op_cap.contains(&id));
        assert!(!s.state.at_op_cap.contains(&id));
        assert_eq!(save_states(&effects), 0, "no transition, no SaveState");

        // Crossing the warn threshold records APPROACHING, not at-cap.
        note_op_cap_status(&mut s, &id, kutl_core::OP_CAP_WARN_THRESHOLD, &mut effects);
        assert!(s.state.approaching_op_cap.contains(&id));
        assert!(!s.state.at_op_cap.contains(&id));
        assert_eq!(save_states(&effects), 1, "the crossing persists once");

        // Steady state inside the warning band: no churn.
        note_op_cap_status(
            &mut s,
            &id,
            kutl_core::OP_CAP_WARN_THRESHOLD + 10,
            &mut effects,
        );
        assert_eq!(
            save_states(&effects),
            1,
            "no transition, no extra SaveState"
        );

        // Reaching the cap MOVES the doc from approaching to at-cap.
        note_op_cap_status(&mut s, &id, kutl_core::MAX_OPS_PER_DOC, &mut effects);
        assert!(s.state.at_op_cap.contains(&id));
        assert!(
            !s.state.approaching_op_cap.contains(&id),
            "the sets must stay disjoint at the cap"
        );
        assert_eq!(save_states(&effects), 2, "the freeze persists once");

        // Falling below both thresholds (a cap raise or future compaction)
        // clears everything.
        note_op_cap_status(&mut s, &id, 0, &mut effects);
        assert!(!s.state.at_op_cap.contains(&id));
        assert!(!s.state.approaching_op_cap.contains(&id));
        assert_eq!(save_states(&effects), 3, "the clear persists once");
    }

    // ── last_synced follows document identity ───────────────────────────────
    //
    // `last_synced` maps a PATH to a frontier of local CRDT times, and those
    // times are meaningful only against the oplog of the document bound at
    // that path when they were recorded. A frontier that outlives its
    // path↔document binding is poison: the next local edit at the path feeds
    // it to `encode_since`, whose history walk unwraps a failed lookup on
    // times the new document's oplog does not contain.

    #[test]
    fn test_move_identity_rekeys_last_synced() {
        use std::path::Path;

        let mut s = st();
        let x = doc_id();
        track_remote(&mut s, &x, "a.md", 10);
        s.last_synced.insert(PathBuf::from("a.md"), vec![7]);

        move_identity(&mut s, Path::new("a.md"), PathBuf::from("b.md"), &x);

        assert!(
            !s.last_synced.contains_key(Path::new("a.md")),
            "the old path must not keep a frontier a different doc could inherit"
        );
        assert_eq!(
            s.last_synced.get(Path::new("b.md")),
            Some(&vec![7]),
            "the frontier follows the document to its new path"
        );
    }

    #[test]
    fn test_move_identity_drops_stale_frontier_at_the_destination() {
        use std::path::Path;

        let mut s = st();
        let x = doc_id();
        track_remote(&mut s, &x, "a.md", 10);
        // A prior occupant stranded a frontier at the destination; the mover
        // has none of its own. Inheriting the stranded entry would bind a
        // FOREIGN frontier to X's oplog — the same poison the rebind guard
        // blocks on registration.
        s.last_synced.insert(PathBuf::from("b.md"), vec![599]);

        move_identity(&mut s, Path::new("a.md"), PathBuf::from("b.md"), &x);

        assert!(
            !s.last_synced.contains_key(Path::new("b.md")),
            "a stranded destination frontier must not be inherited by the mover"
        );
    }

    #[test]
    fn test_register_identity_of_a_different_doc_drops_stale_last_synced() {
        use std::path::Path;

        let mut s = st();
        let x = doc_id();
        track_remote(&mut s, &x, "a.md", 10);
        s.last_synced.insert(PathBuf::from("a.md"), vec![7]);

        // Re-registering the SAME doc keeps the frontier — a re-confirmation
        // must not force a full re-encode on the next edit.
        register_identity(&mut s, Path::new("a.md"), x.clone(), true);
        assert_eq!(
            s.last_synced.get(Path::new("a.md")),
            Some(&vec![7]),
            "same-doc re-registration keeps the frontier"
        );

        // A DIFFERENT doc claiming the path invalidates it: a frontier is
        // only meaningful against the oplog that minted it.
        register_identity(
            &mut s,
            Path::new("a.md"),
            "22222222-2222-4222-8222-222222222222".to_owned(),
            true,
        );
        assert!(
            !s.last_synced.contains_key(Path::new("a.md")),
            "rebinding the path to a different doc drops the stale frontier"
        );
    }

    #[test]
    fn test_cleanup_document_state_drops_the_in_memory_document() {
        use std::path::Path;

        let mut s = st();
        let x = doc_id();
        track_remote(&mut s, &x, "a.md", 10);
        let _ = s.load_or_create_doc(&x);
        assert!(s.get_doc(&x).is_some(), "doc resident before cleanup");

        cleanup_document_state(&mut s, Path::new("a.md"));

        assert!(
            s.get_doc(&x).is_none(),
            "a cleaned-up document must not stay resident (resurrection bait)"
        );
    }

    #[test]
    fn test_diff_into_crdt_survives_a_poisoned_last_synced() {
        // Even a frontier that escaped every re-key (or a corrupted map) must
        // not panic the edit: the core degrades the delta to the full stream
        // and the edit still ships.
        let mut s = st();
        let x = doc_id();
        track_remote(&mut s, &x, "a.md", 10);
        s.last_synced.insert(PathBuf::from("a.md"), vec![3, 599]);

        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("a.md"),
                content: Some(b"hello".to_vec()),
                stamp: EventStamp {
                    wall_ms: 20,
                    origin_hlc: None,
                },
            },
        );

        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::SendOps { ops, .. } if !ops.is_empty())),
            "the edit must still ship ops, got {effects:?}"
        );
    }
}
