//! Identity/inode/sidecar bookkeeping for [`SpaceWorker`]: the path ↔ UUID
//! identity map, the `.dt` sidecar IO edge (save/scan/remove), the `moved_inode`
//! carry-forward rule, and the offline-rename batch detector. (The inode →
//! rename-source probe itself lives on `SpaceState::rename_source`, backed by
//! the identity indexes.)

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::SafeRelayPath;
use crate::blob_state::HashedContent;
use crate::watcher::{self, Suppression};

use super::SpaceWorker;

impl SpaceWorker {
    /// Persist the in-memory CRDT document for `document_id` to its `.dt`
    /// sidecar. The `.dt` IO edge for the pure `state.documents` store (port of
    /// the former `DocumentManager::save`); a no-op for an unknown id.
    pub(super) fn save_doc(&self, document_id: &str) -> Result<()> {
        if let Some(doc) = self.state.get_doc(document_id) {
            let dt_path = self.state.dt_path(document_id);
            doc.save(&dt_path)
                .with_context(|| format!("failed to save {}", dt_path.display()))?;
        }
        Ok(())
    }

    /// Replace the in-memory CRDT for `document_id` with its `.dt` sidecar
    /// copy, or drop it when no sidecar exists (the document was never
    /// saved). The `.dt`-read edge for an engine that can no longer be
    /// trusted; the core pairs it with a re-subscribe so the relay's
    /// catch-up restores what the sidecar lacks. The untrusted engine is
    /// gone in every case: a sidecar that cannot be read is retired (moved
    /// aside as `.corrupt`), exactly as the startup scan retires one, and a
    /// document with no sidecar at all is dropped. Both leave the document
    /// marked as awaiting the relay's refill, so its file is diffed against
    /// real content and never read as a new document.
    pub(super) fn reload_doc(&mut self, document_id: &str) {
        self.state.remove_doc_in_memory(document_id);
        let dt_path = self.state.dt_path(document_id);
        if !dt_path.exists() {
            // No sidecar to reload from: the engine is gone until the relay's
            // catch-up refills it, and the file on disk is diffed against that
            // refill, never against an empty engine.
            self.state.awaiting_content.insert(document_id.to_owned());
            warn!(%document_id, "dropped an untrusted document that was never saved; it refills from the relay");
            return;
        }
        match Self::load_owned_doc(&dt_path) {
            Ok(doc) => {
                info!(%document_id, "reloaded document from its sidecar");
                self.state.documents.insert(document_id.to_owned(), doc);
            }
            Err(e) => self.retire_unreadable_sidecar(document_id, &dt_path, &e),
        }
    }

    /// The one action for a `.dt` sidecar that cannot be read, at the startup
    /// scan and the runtime reload alike: move it and its change sidecar
    /// aside as `.corrupt` so the bytes survive for a post-mortem, say so at
    /// error level, drop the engine, and mark the document as awaiting
    /// content. The document stays tracked under its id: the relay's
    /// catch-up refills a fresh engine, and the file on disk is diffed
    /// against the refilled content (`awaiting_content`), so an edit made
    /// while the sidecar was unreadable folds in once and the file is never
    /// read as a brand-new document.
    fn retire_unreadable_sidecar(
        &mut self,
        document_id: &str,
        dt_path: &Path,
        error: &kutl_core::Error,
    ) {
        self.state.remove_doc_in_memory(document_id);
        kutl_core::envelope::quarantine_document(
            dt_path,
            &format!("document {document_id} sidecar cannot be read: {error}"),
        );
        let changes = kutl_core::change_sidecar_path(dt_path);
        if changes.exists() {
            kutl_core::envelope::quarantine(
                kutl_core::envelope::Kind::Changes,
                &changes,
                "retired with its unreadable document sidecar",
            );
        }
        self.state.awaiting_content.insert(document_id.to_owned());
    }

    /// Load a document from its `.dt` sidecar as the file's owner: a change
    /// sidecar still in its pre-envelope shape is rewritten here, at the one
    /// door every document load takes, so a space is migrated at its first
    /// start and a reader that does not own the store never writes into it.
    fn load_owned_doc(dt_path: &Path) -> kutl_core::Result<kutl_core::Document> {
        let mut doc = kutl_core::Document::load(dt_path)?;
        if doc.needs_sidecar_rewrite()
            && let Err(e) = doc.rewrite_sidecar(dt_path)
        {
            warn!(path = %dt_path.display(), error = %e, "could not rewrite the change sidecar as an envelope; it stays in use");
        }
        Ok(doc)
    }

    /// Delete the `.dt` sidecar for `document_id` (the disk half of a document
    /// removal; the in-memory drop is `SpaceState::remove_doc_in_memory`). Port
    /// of `DocumentManager::remove`'s sidecar delete.
    pub(super) fn remove_doc_sidecar(&self, document_id: &str) {
        let dt_path = self.state.dt_path(document_id);
        if dt_path.exists()
            && let Err(e) = std::fs::remove_file(&dt_path)
        {
            warn!(path = %dt_path.display(), error = %e, "failed to remove dt sidecar");
        }
    }

    /// Scan `.kutl/docs/` on startup and load existing `.dt` sidecars into the
    /// in-memory CRDT store, keyed by id. The `.dt`-read edge that populates
    /// `state.documents` (port of `DocumentManager::scan_existing`).
    pub(super) fn scan_docs(&mut self) {
        let docs_dir = self.config.space_root.join(".kutl").join("docs");
        let dt_files = walk_dt_files(&docs_dir).unwrap_or_default();
        for dt_path in dt_files {
            let Some(id) = dt_to_id(&dt_path) else {
                continue;
            };
            if self.state.documents.contains_key(&id) {
                continue;
            }
            match Self::load_owned_doc(&dt_path) {
                Ok(doc) => {
                    info!(document_id = %id, "loaded existing document");
                    self.state.documents.insert(id, doc);
                }
                Err(e) => self.retire_unreadable_sidecar(&id, &dt_path, &e),
            }
        }
    }

    /// The inode currently recorded for a tracked path, if any.
    pub(super) fn recorded_inode(&self, rel_path: &Path) -> Option<u64> {
        self.state
            .file_identity
            .get(rel_path)
            .and_then(|id| id.inode)
    }

    /// Every non-ignored file under the space root with its inode, in one
    /// walk order. The per-lookup [`Self::space_file_with_inode`] and the
    /// bulk [`Self::space_inode_index`] both read this, so a hardlinked or
    /// duplicate inode resolves to the same path whichever asks. Lazy: a
    /// caller that stops at the first match walks no further.
    fn space_files(&self) -> impl Iterator<Item = (PathBuf, u64)> + '_ {
        let root = &self.config.space_root;
        walkdir::WalkDir::new(root)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.file_type().is_file())
            .filter_map(move |e| {
                let rel = e.path().strip_prefix(root).ok()?.to_path_buf();
                if watcher::should_ignore(&rel) {
                    return None;
                }
                let inode = crate::inode::get_inode(e.path())?;
                Some((rel, inode))
            })
    }

    /// Find a non-hidden file under the space root carrying `inode`, if any.
    ///
    /// Used to tell a genuinely-deleted document from one that was locally
    /// *relocated* (a concurrent rename), where the file still exists at a new
    /// path carrying the same inode. `None` inode never matches, and never
    /// walks: an absent inode is common (a doc registered while its file is
    /// absent), so the miss must stay O(1).
    pub(super) fn space_file_with_inode(&self, inode: Option<u64>) -> Option<PathBuf> {
        let target = inode?;
        self.space_files()
            .find(|(_, inode)| *inode == target)
            .map(|(rel, _)| rel)
    }

    /// Register identity for a document (path ↔ UUID) and persist.
    ///
    /// Accepts a [`SafeRelayPath`] to ensure relay-supplied paths have been
    /// validated before entering the identity map.
    ///
    /// SHADOW: when the file is already on disk (a LOCAL create/rename — the user
    /// wrote it, so no daemon `WriteFile` effect will ever land to grow the shadow),
    /// record it occupied here. The pure core answers the empty-ops/skip-write
    /// "is this file on disk?" question from `shadow.shadow_occupant`, NOT a live
    /// `exists()`; without this a freshly-registered local file is seen as ABSENT
    /// and the empty-ops "ensure the file exists" echo clobbers it with empty
    /// content. This is the runtime analog of [`Self::seed_shadow`]'s startup pass.
    /// A remote materialization registers BEFORE its content is written, so the
    /// file is genuinely absent and the shadow is correctly left untouched (the
    /// landing `WriteFile` grows it via `apply_effect_result`).
    pub(super) fn register_identity(
        &mut self,
        rel_path: SafeRelayPath,
        document_uuid: String,
        confirmed: bool,
    ) {
        let rel_path = rel_path.into_path_buf();
        // The single implementation of the map transitions (incl. the
        // stale-frontier invalidation on rebind) is the pure
        // [`crate::core::handle::register_identity`]; this shell adds the
        // live inode, the disk-backed shadow claim, and the coalesced
        // persist mark.
        let shadow_id = uuid::Uuid::parse_str(&document_uuid).unwrap_or_else(|_| uuid::Uuid::nil());
        crate::core::handle::register_identity(
            &mut self.state,
            &rel_path,
            document_uuid,
            confirmed,
        );
        let abs_path = self.config.space_root.join(&rel_path);
        let inode = crate::inode::get_inode(&abs_path);
        self.state.identity_set_inode(&rel_path, inode);
        if abs_path.exists() {
            self.state.shadow.set_tracked(&rel_path, shadow_id);
        }
        self.mark_identity_dirty();
    }

    /// Record that an identity mutation needs persisting: pend the coalesced
    /// snapshot save and drain the O(1) identity journal now. Coalesced, not
    /// inline: `save_state` serializes the WHOLE doc table, so an inline save
    /// per registered or moved file is O(docs) each, the measured O(N²) term
    /// in bulk adds and moves, paid on BOTH sides of a move (the detector
    /// and the receiver via `place_now`). The loop's `flush_state_if_caught_up`
    /// persists once when the intake drains (shutdown, disconnect and one-shot
    /// end flush unconditionally); crash durability rides the journal appended
    /// here.
    fn mark_identity_dirty(&mut self) {
        self.state_dirty = true;
        self.drain_identity_journal();
    }

    /// Unregister identity for a document and queue the coalesced persist.
    ///
    /// The bookkeeping itself lives in
    /// [`crate::core::handle::helpers::unregister_identity`] — the shell adds
    /// only the dirty mark, the same coalesced-save lane the pure core reaches
    /// via `Effect::SaveState`.
    pub(super) fn unregister_identity(&mut self, document_uuid: &str) {
        crate::core::handle::unregister_identity(&mut self.state, document_uuid);
        self.mark_identity_dirty();
    }

    /// Move identity from one path to another and queue the coalesced persist.
    ///
    /// The single implementation of the map transitions (identity, documents,
    /// blob state, `last_synced`) is the pure
    /// [`crate::core::handle::move_identity`]; this shell adds only what the
    /// pure core cannot touch — the live inode read and the persist mark.
    pub(super) fn move_identity(
        &mut self,
        old_path: &Path,
        new_path: PathBuf,
        document_uuid: &str,
    ) {
        let abs_new = self.config.space_root.join(&new_path);
        let dest = new_path.clone();
        // Persist blob state below only when the move actually carries a blob
        // entry: for a TEXT doc the fold's `blob_state.rename` is a no-op,
        // and an unconditional save is an O(blob-map) disk write per moved
        // file — a bulk move of a text corpus pays it N times for nothing.
        let moves_blob = self.state.blob_state.get(old_path).is_some();
        crate::core::handle::move_identity(&mut self.state, old_path, new_path, document_uuid);
        // The pure fold carried the OLD inode forward; refresh from the live
        // file at the destination, keeping the carried one as the fallback.
        let carried = self.state.file_identity.get(&dest).and_then(|id| id.inode);
        self.state.identity_set_inode(
            &dest,
            moved_inode(crate::inode::get_inode(&abs_new), carried),
        );
        if moves_blob && let Err(e) = self.state.blob_state.save(&self.config.space_root) {
            error!(error = %e, "failed to persist blob state after rename");
        }
        self.mark_identity_dirty();
    }

    /// Remove all local state for a document (last-synced, blob state, CRDT
    /// sidecar). The sidecar is keyed by document id, so resolve it from
    /// `file_identity` here — before any caller drops the identity.
    pub(super) fn cleanup_document_state(&mut self, rel_path: &Path) {
        // The single implementation of the in-memory teardown is the pure
        // [`crate::core::handle::cleanup_document_state`]; this shell adds
        // only the on-disk `.dt` sidecar removal. Resolve the id BEFORE the
        // pure fold — it reads `file_identity[rel]` too, and both need the
        // binding intact.
        let id = self.uuid_at(rel_path);
        crate::core::handle::cleanup_document_state(&mut self.state, rel_path);
        if let Some(id) = id {
            self.remove_doc_sidecar(&id);
        }
    }

    /// The document id tracked at `rel_path`, if any (path → id resolver).
    pub(super) fn uuid_at(&self, rel_path: &Path) -> Option<String> {
        self.state
            .file_identity
            .get(rel_path)
            .map(|id| id.document_uuid.clone())
    }

    /// The tracked document a LIVE local rename left behind: the recorded
    /// holder of `inode` whose own file is gone from disk (see
    /// [`SpaceState::rename_source`] for the inode-reuse guard). The one
    /// definition of "gone" for plain rename detection — the classify
    /// candidacy probe and the imperative detector share it, so the two can
    /// never disagree about what counts as a vacated source.
    ///
    /// [`SpaceState::rename_source`]: crate::core::SpaceState::rename_source
    pub(super) fn live_rename_source(&self, inode: u64) -> Option<(PathBuf, String)> {
        self.rename_source_excluding(inode, None)
    }

    /// As [`Self::live_rename_source`], for the OVERWRITE-rename shape (a
    /// tracked doc `mv`'d onto the already-tracked `target`): the mover must
    /// additionally be a path other than `target` itself — the target is
    /// occupied by definition, so it can never be its own mover's vacated
    /// source.
    pub(super) fn overwrite_rename_source(
        &self,
        inode: u64,
        target: &Path,
    ) -> Option<(PathBuf, String)> {
        self.rename_source_excluding(inode, Some(target))
    }

    /// The one live-disk probe behind both rename-source shapes: a candidate
    /// counts as vacated when its old path is gone from disk, and never when
    /// it is the `occupied` path itself.
    fn rename_source_excluding(
        &self,
        inode: u64,
        occupied: Option<&Path>,
    ) -> Option<(PathBuf, String)> {
        let space_root = &self.config.space_root;
        self.state.rename_source(inode, |old| {
            occupied.is_some_and(|t| old == t) || space_root.join(old).exists()
        })
    }

    /// Documents renamed on disk while this daemon was offline: a tracked
    /// document whose recorded path is now absent, but whose recorded inode
    /// resolves to a file at a *different* path under the space root. Returns
    /// `(document_id, recorded_old_path, current_local_path)`.
    ///
    /// Pure detection (stat + inode walk, no mutation). The caller resolves each
    /// against the relay's authoritative state in [`Self::startup_reconciliation`]
    /// *before* the reconcile truth table runs, so a recorded-path-gone document
    /// is not misread as a local delete that destroys the identity before the
    /// new-file scan could re-bind the moved file (the split that mints a
    /// spurious second UUID for one document).
    pub(super) fn detect_offline_renames(&self) -> Vec<(String, PathBuf, PathBuf)> {
        let mut out = Vec::new();
        // One inode → path index for the whole batch, built lazily on the
        // FIRST gone doc: the common zero-gone-docs startup walks nothing, and
        // a bulk offline move costs one tree walk instead of one per gone doc
        // (gone-docs × files-in-space stats dominated startup on a large
        // space).
        let mut index: Option<HashMap<u64, PathBuf>> = None;
        for (old_path, identity) in &self.state.file_identity {
            if self.config.space_root.join(old_path).exists() {
                continue; // recorded path still present — not renamed away
            }
            let idx = index.get_or_insert_with(|| self.space_inode_index());
            if let Some(new_local) = identity.inode.and_then(|ino| idx.get(&ino).cloned())
                && new_local != *old_path
            {
                out.push((identity.document_uuid.clone(), old_path.clone(), new_local));
            }
        }
        out
    }

    /// One-pass inode → relative-path index of every non-ignored file in the
    /// space, FIRST match kept per inode, over the same walk
    /// [`Self::space_file_with_inode`] reads. Built once per batch of
    /// gone-path questions (`detect_offline_renames`, a directory removal's
    /// expansion), where a walk per question would be quadratic.
    pub(super) fn space_inode_index(&self) -> HashMap<u64, PathBuf> {
        let mut index = HashMap::new();
        for (rel, inode) in self.space_files() {
            index.entry(inode).or_insert(rel);
        }
        index
    }

    /// Restore a tracked file from its CRDT: the startup guard's answer to an
    /// interrupted materialization (the disk bytes are this daemon's own
    /// last write, so the difference is content the CRDT holds and the file
    /// never finished receiving). Returns whether a restore was written.
    /// `Ok(false)` means the CRDT is empty: nothing to restore FROM, and
    /// writing an empty file over the user's bytes would be the erasure the
    /// guard exists to prevent, so the caller incorporates the file instead.
    pub(super) fn restore_from_crdt(
        &mut self,
        rel_path: &Path,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<bool> {
        let Some(crdt_content) = self.crdt_content_at(rel_path) else {
            return Ok(false);
        };
        let abs_path = self.config.space_root.join(rel_path);
        self.write_doc(
            rel_path,
            &abs_path,
            &HashedContent::new(crdt_content.into_bytes()),
            suppress_tx,
        )?;
        Ok(true)
    }

    /// The non-empty CRDT content of the document tracked at `rel_path`, or
    /// `None` when the path is untracked, the doc is not loaded, or its
    /// content is empty (nothing a flush could write). The one preamble
    /// behind every CRDT-to-disk flush.
    fn crdt_content_at(&self, rel_path: &Path) -> Option<String> {
        let id = self.uuid_at(rel_path)?;
        let content = self.state.get_doc(&id)?.content();
        (!content.is_empty()).then_some(content)
    }

    /// Write CRDT content to disk if it differs from the file.
    ///
    /// Used after rename operations to flush remote ops that were merged
    /// into the CRDT while the rename was in flight.
    pub(super) fn flush_crdt_if_stale(
        &mut self,
        rel_path: &Path,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> Result<()> {
        let Some(crdt_content) = self.crdt_content_at(rel_path) else {
            return Ok(());
        };
        let abs_path = self.config.space_root.join(rel_path);
        if let Ok(file_content) = std::fs::read_to_string(&abs_path)
            && file_content != crdt_content
        {
            debug!(
                path = %rel_path.display(),
                "flushing CRDT content after rename"
            );
            self.write_doc(
                rel_path,
                &abs_path,
                &HashedContent::new(crdt_content.into_bytes()),
                suppress_tx,
            )?;
        }
        Ok(())
    }
}

/// Recursively find all `.dt` sidecar files under a directory (the startup scan
/// edge for the in-memory CRDT store).
fn walk_dt_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    walk_dt_recursive(dir, &mut result)?;
    Ok(result)
}

fn walk_dt_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_dt_recursive(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "dt") {
            out.push(path);
        }
    }
    Ok(())
}

/// Extract the document id from a sidecar path: `.../<document-id>.dt` →
/// `<document-id>`.
fn dt_to_id(dt_path: &Path) -> Option<String> {
    dt_path
        .file_stem()
        .and_then(|s| s.to_str())
        .map(str::to_owned)
}

/// The inode to record for a document after moving its identity to a new path.
///
/// Prefer the file actually on disk at the new path. When the new path has no
/// file yet, fall back to the document's previously-recorded inode rather than
/// recording `None`.
///
/// The fallback is load-bearing for concurrent-rename convergence. When a remote
/// rename for document `D` arrives before the local file has reached the
/// authoritative path — e.g. the user concurrently renamed `D` to a *different*
/// local name, so the file currently sits there carrying `D`'s inode while the
/// authoritative path is not yet on disk — recording `None` would discard the
/// inode that links `D` to its on-disk file. The relocated file would then look
/// untracked to `SpaceState::rename_source` and be minted as a spurious new
/// document, splitting `D` in two. Preserving the inode keeps the relocated file
/// matchable as `D`, so the local rename is re-attributed to `D` and the relay's
/// lattice arbitrates a single winning path. (A genuinely deleted document is
/// removed from `file_identity` entirely, so it can never be matched here.)
fn moved_inode(new_path_on_disk: Option<u64>, previously_recorded: Option<u64>) -> Option<u64> {
    new_path_on_disk.or(previously_recorded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{SyncCommand, SyncEvent};
    use crate::core::{DaemonCore, Event, EventStamp, FileIdentity};
    use crate::daemon::session::CHANNEL_CAPACITY;
    use crate::daemon::tests::test_worker;
    use crate::watcher::FileEvent;

    fn identity(uuid: &str, inode: u64) -> FileIdentity {
        FileIdentity {
            document_uuid: uuid.to_owned(),
            inode: Some(inode),
            last_written_hash: None,
        }
    }

    /// An IO-free [`SpaceState`] tracking the given `(path, uuid, inode)`
    /// entries through the identity choke points.
    fn state_with(entries: &[(&str, &str, u64)]) -> crate::core::SpaceState {
        let mut s = crate::core::SpaceState::new_for_test(
            "identity-test".into(),
            PathBuf::from("/nonexistent/identity-test"),
            "did:test".into(),
        );
        for (rel, uuid, ino) in entries {
            s.identity_insert(PathBuf::from(rel), identity(uuid, *ino));
        }
        s
    }

    /// A new file whose inode matches a tracked document whose old path is gone
    /// is a genuine rename — the source path and UUID are reported.
    #[test]
    fn test_rename_source_matches_when_old_path_gone() {
        let s = state_with(&[("notes.md", "uuid-notes", 42)]);

        assert_eq!(
            s.rename_source(42, |_| false),
            Some((PathBuf::from("notes.md"), "uuid-notes".to_owned()))
        );
    }

    /// Inode-reuse guard: when the matched document's old path still exists on
    /// disk, the inode match is a coincidence — the kernel reused a freed inode
    /// for an unrelated new file — so it must NOT be reported as a rename.
    ///
    /// Reproduces the false-rename bug: `resumable.md` is still tracked and
    /// still on disk, but its recorded inode (freed by an earlier atomic
    /// rewrite) was reused by a newly created file at a different path.
    #[test]
    fn test_rename_source_rejects_when_old_path_still_exists() {
        let s = state_with(&[("resumable.md", "uuid-resumable", 99)]);

        assert_eq!(s.rename_source(99, |_| true), None);
    }

    /// No tracked document has the observed inode — nothing is reported.
    #[test]
    fn test_rename_source_no_inode_match() {
        let s = state_with(&[("a.md", "uuid-a", 1)]);

        assert_eq!(s.rename_source(2, |_| false), None);
    }

    /// A recorded inode goes stale when an editor rewrites the file (atomic
    /// tmp-rename changes the inode). A stale inode masks a genuine later
    /// rename — `rename_source` can't match the current inode — until the
    /// recorded value is refreshed. Reproduces the missed-rename bug.
    #[test]
    fn test_stale_inode_masks_rename_until_refreshed() {
        // foo.md was registered at inode 7; an atomic rewrite later moved its
        // content to inode 8, but the recorded value is still the freed 7.
        let mut s = state_with(&[("foo.md", "uuid-foo", 7)]);

        // foo.md is renamed to bar.md; rename preserves the current inode (8),
        // and foo.md no longer exists. The stale recorded inode (7) masks it:
        assert_eq!(s.rename_source(8, |_| false), None);

        // Refreshing the recorded inode to the file's current value restores
        // detection of the rename — and retires the freed inode as bait.
        s.identity_set_inode(Path::new("foo.md"), Some(8));
        assert_eq!(
            s.rename_source(8, |_| false),
            Some((PathBuf::from("foo.md"), "uuid-foo".to_owned()))
        );
        assert_eq!(s.rename_source(7, |_| false), None);
    }

    /// Concurrent-rename convergence: a remote rename of `D` (`foo`→`bar_a`)
    /// arrives on a worker that has *locally* renamed the same file to a
    /// different name (`foo`→`bar_b`). The local file sits at `bar_b` carrying
    /// `D`'s inode; `bar_a` is not yet on disk. Moving `D`'s identity to `bar_a`
    /// must preserve its inode so the watcher re-attributes `bar_b` to `D`
    /// instead of minting a new document. Recording `bar_a`'s (absent) inode as
    /// `None` is the bug that splits `D`.
    #[test]
    fn test_moved_inode_preserves_identity_when_new_path_absent() {
        const D_INODE: u64 = 7;
        let mut s = state_with(&[("foo.md", "D", D_INODE)]);

        // move_identity(foo→bar_a) with bar_a absent on disk (its inode is None).
        let old_inode = s
            .file_identity
            .get(Path::new("foo.md"))
            .and_then(|id| id.inode);
        s.identity_remove(Path::new("foo.md"));
        s.identity_insert(
            PathBuf::from("bar_a.md"),
            FileIdentity {
                document_uuid: "D".to_owned(),
                inode: moved_inode(None, old_inode),
                last_written_hash: None,
            },
        );

        // The local relocation (bar_b carries D's inode; bar_a is not on disk) is
        // recognized as a rename of D — not an untracked file to mint anew.
        assert_eq!(
            s.rename_source(D_INODE, |_| false),
            Some((PathBuf::from("bar_a.md"), "D".to_owned())),
            "relocated file must be matched as D, not minted as a new document"
        );
    }

    /// When the new path *does* have a file on disk (the ordinary local-rename
    /// case), its on-disk inode is recorded — the fallback never masks reality.
    #[test]
    fn test_moved_inode_prefers_on_disk_inode() {
        assert_eq!(moved_inode(Some(9), Some(7)), Some(9));
        assert_eq!(moved_inode(None, Some(7)), Some(7));
        assert_eq!(moved_inode(None, None), None);
    }

    /// SIGKILL simulation: identities registered and moved through the shell
    /// funnels reach disk via the O(1) identity journal WITHOUT any
    /// `save_state` — a fresh `DaemonState::load` (what a restart does) must
    /// see them. This is the crash window the coalesced snapshot save
    /// opened (restart re-registered its own files into conflict copies).
    #[test]
    fn test_identity_journal_survives_kill_without_save() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        std::fs::write(dir.path().join("a.md"), "alpha\n").unwrap();
        worker.register_identity(
            SafeRelayPath::new("a.md").unwrap(),
            "11111111-1111-4111-8111-111111111111".to_owned(),
            /* confirmed */ false,
        );
        worker.move_identity(
            Path::new("a.md"),
            PathBuf::from("b.md"),
            "11111111-1111-4111-8111-111111111111",
        );

        // NO save_state: what a SIGKILL right here would leave on disk.
        let reloaded = crate::state::DaemonState::load(&worker.kutl_dir());
        assert!(
            !reloaded.documents.contains_key("a.md"),
            "the journaled move vacated the old path"
        );
        assert_eq!(
            reloaded.documents.get("b.md").map(|e| e.id.as_str()),
            Some("11111111-1111-4111-8111-111111111111"),
            "the journaled registration + move survive a kill without any save"
        );
    }

    /// A case-variant of a DIFFERENT tracked path is a collision hit; an
    /// untracked casefold is a miss; the candidate's own path is never a
    /// collision (an ordinary re-tracking); and a rename's own source is
    /// excludable. The index-backed probe carries the same semantics the old
    /// linear `find_case_variant` scan pinned.
    #[test]
    fn test_tracked_case_variant_semantics() {
        let s = state_with(&[("foo.md", "uuid-foo", 1), ("bar.md", "uuid-bar", 2)]);
        assert_eq!(
            s.tracked_case_variant(Path::new("Foo.md"), None),
            Some(&PathBuf::from("foo.md")),
            "a case-variant of another tracked path is a hit"
        );
        assert_eq!(
            s.tracked_case_variant(Path::new("baz.md"), None),
            None,
            "an untracked casefold is a miss"
        );
        assert_eq!(
            s.tracked_case_variant(Path::new("foo.md"), None),
            None,
            "the candidate's own path is not a collision"
        );
        assert_eq!(
            s.tracked_case_variant(Path::new("Foo.md"), Some(Path::new("foo.md"))),
            None,
            "a rename's own source is excludable"
        );
    }

    /// Refreshing an untracked path is a no-op (no spurious entry created,
    /// no spurious inode-index entry to false-match later).
    #[test]
    fn test_set_inode_untracked_path_is_noop() {
        let mut s = state_with(&[]);
        s.identity_set_inode(Path::new("ghost.md"), Some(5));
        assert!(s.file_identity.is_empty());
        assert_eq!(s.rename_source(5, |_| false), None);
    }

    const TEST_DOC_ID: &str = "11111111-1111-4111-8111-111111111111";

    /// Migrated from `documents.rs::test_dt_to_id`: extract the id from a sidecar
    /// path stem (pure).
    #[test]
    fn test_dt_to_id() {
        assert_eq!(
            dt_to_id(Path::new(&format!(
                "/tmp/space/.kutl/docs/{TEST_DOC_ID}.dt"
            ))),
            Some(TEST_DOC_ID.to_owned())
        );
    }

    /// Migrated from `documents.rs::test_save_and_reload`: `save_doc` writes the
    /// `.dt` sidecar, and a fresh worker's startup `scan_docs` reloads it.
    #[test]
    fn test_save_doc_and_reload_via_scan() {
        let dir = tempfile::tempdir().unwrap();

        let mut worker = test_worker(dir.path().to_path_buf());
        {
            let doc = worker.state.load_or_create_doc(TEST_DOC_ID);
            let agent = doc.register_agent("test").unwrap();
            doc.edit(agent, "test", "init", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(0, "hello")
            })
            .unwrap();
        }
        worker.save_doc(TEST_DOC_ID).unwrap();
        // The incumbent must exit before a successor takes the space: the
        // producer flock admits one worker per root.
        drop(worker);

        // A fresh worker over the same root scans the sidecar on startup.
        let worker2 = test_worker(dir.path().to_path_buf());
        assert_eq!(
            worker2.state.get_doc(TEST_DOC_ID).unwrap().content(),
            "hello"
        );
    }

    /// Migrated from `documents.rs::test_scan_existing`: a sidecar written by one
    /// worker is loaded into `state.documents` by another's `scan_docs`.
    #[test]
    fn test_scan_docs_loads_existing_sidecar() {
        let dir = tempfile::tempdir().unwrap();

        let mut worker = test_worker(dir.path().to_path_buf());
        {
            let doc = worker.state.load_or_create_doc(TEST_DOC_ID);
            let agent = doc.register_agent("test").unwrap();
            doc.edit(agent, "test", "init", kutl_core::Boundary::Auto, |ctx| {
                ctx.insert(0, "aaa")
            })
            .unwrap();
        }
        worker.save_doc(TEST_DOC_ID).unwrap();
        // The incumbent must exit before a successor takes the space: the
        // producer flock admits one worker per root.
        drop(worker);

        let mut worker2 = test_worker(dir.path().to_path_buf());
        // Clear and re-scan to prove scan_docs (not just `new`) does the load.
        worker2.state.documents.clear();
        worker2.scan_docs();
        assert!(worker2.state.get_doc(TEST_DOC_ID).is_some());
        assert_eq!(worker2.state.get_doc(TEST_DOC_ID).unwrap().content(), "aaa");
    }

    /// A BATCH of offline renames resolves through the one-pass inode
    /// index (built lazily on the first gone doc) — every moved doc is found,
    /// the unmoved doc is not, with tuples identical to the former
    /// per-gone-doc full-tree walk (first match per inode, same scan order).
    #[test]
    fn test_detect_offline_renames_batch_via_inode_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        for (i, name) in ["a.md", "b.md", "c.md"].iter().enumerate() {
            let abs = dir.path().join(name);
            std::fs::write(&abs, format!("body {i}")).unwrap();
            worker.register_identity(
                SafeRelayPath::new(name).unwrap(),
                format!("00000000-0000-0000-0000-00000000000{i}"),
                /* confirmed */ true,
            );
            worker
                .state
                .identity_set_inode(Path::new(name), crate::inode::get_inode(&abs));
        }
        std::fs::rename(dir.path().join("a.md"), dir.path().join("a-moved.md")).unwrap();
        std::fs::rename(dir.path().join("c.md"), dir.path().join("c-moved.md")).unwrap();

        let mut detected = worker.detect_offline_renames();
        detected.sort();
        assert_eq!(
            detected,
            vec![
                (
                    "00000000-0000-0000-0000-000000000000".to_owned(),
                    PathBuf::from("a.md"),
                    PathBuf::from("a-moved.md"),
                ),
                (
                    "00000000-0000-0000-0000-000000000002".to_owned(),
                    PathBuf::from("c.md"),
                    PathBuf::from("c-moved.md"),
                ),
            ],
            "both offline moves detected in one index pass; the unmoved doc is not"
        );
    }

    /// Offline-rename inode persistence across restart (the dropped-persistence
    /// and save-coalescing classes). A remote document registered while its file is
    /// ABSENT records `inode: None`; its first content materialization
    /// (`merge_remote_ops`) writes the file, reads the real OS inode into
    /// `file_identity`, and emits a coalesced `Effect::SaveState` so the inode is
    /// PERSISTED into the state snapshot. After a daemon RESTART (`SpaceWorker::new`
    /// reloading the persisted state) and an offline `mv` of the file, the persisted
    /// inode is the ONLY way `detect_offline_renames` can match the moved file —
    /// its recorded path is gone, so the inode can no longer be read from disk
    /// there. Drop the persist and the restart reads `inode: null`,
    /// `space_file_with_inode(None)` never matches, and the moved file
    /// becomes a phantom.
    ///
    /// Drives the REAL glue chain end to end at the unit layer: `register_identity`
    /// (the `SubscribeRemote` registration that records `inode: None` for an absent
    /// file) → `DaemonCore::handle(Event::RemoteOps)` + `apply_effect` (the gamma
    /// `merge_remote_ops` materialization that writes the file, folds the real inode,
    /// and emits `Effect::SaveState`) → `flush_state_if_caught_up` (the
    /// coalesced persist) → a fresh `SpaceWorker` over the same `.kutl` dir (the real
    /// restart) → an out-of-band `mv` on the temp disk → `detect_offline_renames`.
    /// `merge_remote_ops` is always-compiled core, so this exercises the
    /// inode-persist fix on the core path.
    #[test]
    fn test_offline_rename_inode_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let doc = "11111111-1111-1111-1111-111111111111";
        let old_rel = PathBuf::from("notes.md");
        let moved_rel = PathBuf::from("moved.md");

        // ── session 1: register an absent remote doc, then materialize it ──
        {
            let mut worker = test_worker(dir.path().to_path_buf());

            // SubscribeRemote registration: the file does not exist yet, so the
            // recorded inode is None (the bug-1 precondition).
            worker.register_identity(
                SafeRelayPath::new("notes.md").unwrap(),
                doc.to_owned(),
                /* confirmed */ true,
            );
            assert_eq!(
                worker.recorded_inode(&old_rel),
                None,
                "an absent remote doc registers with a null inode (the bug-1 precondition)"
            );

            // Build real CRDT ops for the doc's first content, as a peer would
            // broadcast them (the `encode_since`/`changes_since` pattern the
            // integration relay tests use).
            let (ops, metadata) = {
                let mut peer = kutl_core::Document::new();
                let agent = peer.register_agent("peer").unwrap();
                peer.edit(
                    agent,
                    "did:peer",
                    "edit",
                    kutl_core::Boundary::Auto,
                    |ctx| ctx.insert(0, "hello from a peer"),
                )
                .unwrap();
                (peer.encode_since(&[]), peer.changes_since(&[]))
            };

            let (sync_cmd_tx, _sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
            let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();
            // Empty intake channels: the loop is "caught up", so the coalesced
            // SaveState flushes in the same iteration that recorded the inode.
            let (_fe_tx, fe_rx) = mpsc::channel::<FileEvent>(CHANNEL_CAPACITY);
            let (_se_tx, se_rx) = mpsc::channel::<SyncEvent>(CHANNEL_CAPACITY);

            // The materialization: run the pure core and route its effects through
            // the real driver (writes the file, reads + folds the live inode, marks
            // the coalesced persist dirty).
            let effects = DaemonCore::handle(
                &mut worker.state,
                Event::RemoteOps {
                    document_id: doc.to_owned(),
                    ops,
                    metadata,
                    content_mode: i32::from(kutl_proto::sync::ContentMode::Text),
                    local_content: None,
                    author_by_agent_snapshot: std::collections::HashMap::new(),
                    stamp: EventStamp {
                        wall_ms: 1,
                        origin_hlc: None,
                    },
                },
            );
            for eff in effects {
                worker
                    .apply_effect(eff, &sync_cmd_tx, &suppress_tx)
                    .expect("apply materialization effect");
            }

            // The write folded the real OS inode into the in-memory identity.
            let live_inode = worker.recorded_inode(&old_rel);
            assert!(
                live_inode.is_some(),
                "the first materialization must fold a real inode into file_identity"
            );

            // Flush the coalesced persist (the channels are empty → caught up). This
            // is the save-coalescing step that writes the just-recorded inode to the snapshot.
            worker.flush_state_if_caught_up(&fe_rx, &se_rx);
        }

        // ── the offline rename: mv the file on disk while "offline" ──
        std::fs::rename(dir.path().join(&old_rel), dir.path().join(&moved_rel))
            .expect("move the materialized file out of band");

        // ── session 2: the restart reads the snapshot, detection runs ──
        let worker2 = test_worker(dir.path().to_path_buf());
        assert!(
            worker2.recorded_inode(&old_rel).is_some(),
            "the restart must reload the persisted inode for the recorded (now-gone) path"
        );
        let detected = worker2.detect_offline_renames();
        assert_eq!(
            detected,
            vec![(doc.to_owned(), old_rel.clone(), moved_rel.clone())],
            "detect_offline_renames must match the moved file by its persisted inode"
        );
    }

    /// The imperative twin of the pure `move_identity` carries the same
    /// invariant: `last_synced` frontiers are local times valid only against
    /// the document bound at the path, so the frontier must follow the
    /// document on every identity move — a left-behind entry poisons the next
    /// edit of whatever doc later binds there.
    #[test]
    fn test_move_identity_rekeys_last_synced() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        worker
            .state
            .last_synced
            .insert(PathBuf::from("a.md"), vec![7]);

        worker.move_identity(Path::new("a.md"), PathBuf::from("b.md"), "uuid-x");

        assert!(
            !worker.state.last_synced.contains_key(Path::new("a.md")),
            "the old path must not keep a frontier a different doc could inherit"
        );
        assert_eq!(
            worker.state.last_synced.get(Path::new("b.md")),
            Some(&vec![7]),
            "the frontier follows the document to its new path"
        );
    }
}
