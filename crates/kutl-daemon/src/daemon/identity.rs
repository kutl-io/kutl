//! Identity/inode/sidecar bookkeeping for [`SpaceWorker`]: the path ↔ UUID
//! identity map, the `.dt` sidecar IO edge (save/scan/remove), and the
//! inode-based rename detection helpers (`find_rename_source` / `moved_inode` /
//! `refresh_inode`) plus the offline-rename batch detector.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::SafeRelayPath;
use crate::core::{FileIdentity, rel_path_to_string};
use crate::watcher::{self, Suppression};

use super::SpaceWorker;
use super::effects::write_doc;

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
            if let Some(id) = dt_to_id(&dt_path)
                && let std::collections::hash_map::Entry::Vacant(entry) =
                    self.state.documents.entry(id.clone())
            {
                match kutl_core::Document::load(&dt_path) {
                    Ok(doc) => {
                        info!(document_id = %id, "loaded existing document");
                        entry.insert(doc);
                    }
                    Err(e) => {
                        warn!(document_id = %id, error = %e, "failed to load document, skipping");
                    }
                }
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

    /// Find a non-hidden file under the space root carrying `inode`, if any.
    ///
    /// Used to tell a genuinely-deleted document from one that was locally
    /// *relocated* (a concurrent rename), where the file still exists at a new
    /// path carrying the same inode. `None` inode never matches.
    pub(super) fn space_file_with_inode(&self, inode: Option<u64>) -> Option<PathBuf> {
        let target = inode?;
        let root = &self.config.space_root;
        walkdir::WalkDir::new(root)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.file_type().is_file())
            .find_map(|e| {
                let rel = e.path().strip_prefix(root).ok()?.to_path_buf();
                if watcher::should_ignore(&rel) {
                    return None;
                }
                (crate::inode::get_inode(e.path()) == Some(target)).then_some(rel)
            })
    }

    /// Get the UUID for a file, or generate and register a new one.
    ///
    /// Records the inode for rename detection and persists the mapping.
    /// Local paths from the file watcher are validated through `SafeRelayPath`
    /// before storage — they should always pass since they're relative paths
    /// within the space root.
    pub(super) fn get_or_create_uuid(&mut self, rel_path: &Path) -> String {
        if let Some(identity) = self.state.file_identity.get(rel_path) {
            return identity.document_uuid.clone();
        }

        let uuid = uuid::Uuid::new_v4().to_string();
        let safe_path = SafeRelayPath::new(&rel_path_to_string(rel_path))
            .expect("local file paths within space root must be valid");
        // A locally-created file is not yet relay-confirmed; it becomes
        // confirmed when the relay acknowledges its registration.
        self.register_identity(safe_path, uuid.clone(), false);
        uuid
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
        let path_str = rel_path_to_string(&rel_path);
        let abs_path = self.config.space_root.join(&rel_path);
        let inode = crate::inode::get_inode(&abs_path);
        self.state.file_identity.insert(
            rel_path.clone(),
            FileIdentity {
                document_uuid: document_uuid.clone(),
                inode,
            },
        );
        if abs_path.exists() {
            let id = uuid::Uuid::parse_str(&document_uuid).unwrap_or_else(|_| uuid::Uuid::nil());
            self.state.shadow.set_tracked(&rel_path, id);
            if let Some(inode) = inode {
                self.state.shadow.set_inode(inode, &rel_path);
            }
        }
        self.state
            .state
            .set(path_str, document_uuid.clone(), confirmed);
        self.state.uuid_to_path.insert(document_uuid, rel_path);
        self.save_state();
    }

    /// Unregister identity for a document and persist.
    pub(super) fn unregister_identity(&mut self, document_uuid: &str) {
        if let Some(rel_path) = self.state.uuid_to_path.remove(document_uuid) {
            self.state.file_identity.remove(&rel_path);
            self.state
                .state
                .documents
                .remove(&rel_path_to_string(&rel_path));
            // Prune the persisted register stamp with the doc (save_state below
            // syncs the removal); a dead doc's floor has no further use.
            self.state.register_hlc.remove(document_uuid);
            // An unregistered doc can't be "frozen at the op cap" anymore
            // (keyed by id, so no path-domain concerns).
            self.state.state.at_op_cap.remove(document_uuid);
            self.save_state();
        }
    }

    /// Move identity from one path to another and persist.
    pub(super) fn move_identity(
        &mut self,
        old_path: &Path,
        new_path: PathBuf,
        document_uuid: &str,
    ) {
        let new_path_str = rel_path_to_string(&new_path);
        let old_path_str = rel_path_to_string(old_path);
        let abs_new = self.config.space_root.join(&new_path);
        let old_inode = self
            .state
            .file_identity
            .get(old_path)
            .and_then(|id| id.inode);
        let inode = moved_inode(crate::inode::get_inode(&abs_new), old_inode);
        self.state.file_identity.remove(old_path);
        self.state.file_identity.insert(
            new_path.clone(),
            FileIdentity {
                document_uuid: document_uuid.to_string(),
                inode,
            },
        );
        // A rename preserves the document's confirmed status — it is the same
        // relay document at a new path.
        let confirmed = self
            .state
            .state
            .documents
            .get(&old_path_str)
            .is_some_and(|e| e.confirmed);
        self.state.state.documents.remove(&old_path_str);
        self.state
            .state
            .set(new_path_str, document_uuid.to_string(), confirmed);
        // A renamed BLOB keeps its LWW state under the doc's current path (the
        // newer-wins guard resolves by path); persist alongside the identity.
        self.state.blob_state.rename(old_path, &new_path);
        if let Err(e) = self.state.blob_state.save(&self.config.space_root) {
            error!(error = %e, "failed to persist blob state after rename");
        }
        self.state
            .uuid_to_path
            .insert(document_uuid.to_string(), new_path);
        self.save_state();
    }

    /// Remove all local state for a document (last-synced, blob state, CRDT
    /// sidecar). The sidecar is keyed by document id, so resolve it from
    /// `file_identity` here — before any caller drops the identity.
    pub(super) fn cleanup_document_state(&mut self, rel_path: &Path) {
        self.state.last_synced.remove(rel_path);
        self.state.blob_state.remove(rel_path);
        if let Some(id) = self.uuid_at(rel_path) {
            self.state.remove_doc_in_memory(&id);
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
        // (perf §3.4 — gone-docs × files-in-space stats dominated startup on a
        // large space).
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
    /// space, FIRST match kept per inode — the same `WalkDir` scan order
    /// [`Self::space_file_with_inode`] uses, so hardlinked/duplicate inodes
    /// resolve to the identical path the per-doc walk found. Built once per
    /// `detect_offline_renames` batch.
    fn space_inode_index(&self) -> HashMap<u64, PathBuf> {
        let root = &self.config.space_root;
        let mut index = HashMap::new();
        for e in walkdir::WalkDir::new(root)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.file_type().is_file())
        {
            let Ok(rel) = e.path().strip_prefix(root) else {
                continue;
            };
            if watcher::should_ignore(rel) {
                continue;
            }
            if let Some(ino) = crate::inode::get_inode(e.path()) {
                index.entry(ino).or_insert_with(|| rel.to_path_buf());
            }
        }
        index
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
        let Some(id) = self.uuid_at(rel_path) else {
            return Ok(());
        };
        let Some(doc) = self.state.get_doc(&id) else {
            return Ok(());
        };
        let crdt_content = doc.content();
        if crdt_content.is_empty() {
            return Ok(());
        }
        let abs_path = self.config.space_root.join(rel_path);
        if let Ok(file_content) = std::fs::read_to_string(&abs_path)
            && file_content != crdt_content
        {
            debug!(
                path = %rel_path.display(),
                "flushing CRDT content after rename"
            );
            write_doc(
                &mut self.state.file_identity,
                rel_path,
                &abs_path,
                crdt_content.as_bytes(),
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

/// Find the previously-tracked document that a newly-observed file was renamed
/// from, based on a matching inode.
///
/// A matching inode alone is **not** sufficient evidence of a rename: the
/// kernel reuses inode numbers after a file is unlinked, so a freshly created
/// file can be handed a freed inode that a still-tracked document recorded
/// earlier (the daemon does not refresh a document's stored inode on every
/// atomic content rewrite, so the recorded value is frequently a now-freed
/// inode). We therefore only treat the match as a rename when the candidate
/// old path no longer exists on disk: a genuine rename moves the file away from
/// its old path, whereas inode reuse leaves the old file untouched in place.
///
/// `old_path_exists` reports whether a candidate old path is still present on
/// disk; it is injected so the decision can be unit-tested without provoking a
/// real kernel inode reuse.
pub(super) fn find_rename_source(
    file_identity: &HashMap<PathBuf, FileIdentity>,
    new_inode: u64,
    old_path_exists: impl Fn(&Path) -> bool,
) -> Option<(PathBuf, String)> {
    file_identity
        .iter()
        .find(|(old_path, id)| id.inode == Some(new_inode) && !old_path_exists(old_path))
        .map(|(old_path, id)| (old_path.clone(), id.document_uuid.clone()))
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
/// untracked to [`find_rename_source`] and be minted as a spurious new document,
/// splitting `D` in two. Preserving the inode keeps the relocated file matchable
/// as `D`, so the local rename is re-attributed to `D` and the relay's lattice
/// arbitrates a single winning path. (A genuinely deleted document is removed
/// from `file_identity` entirely, so it can never be matched here.)
fn moved_inode(new_path_on_disk: Option<u64>, previously_recorded: Option<u64>) -> Option<u64> {
    new_path_on_disk.or(previously_recorded)
}

/// Update the recorded inode for an already-tracked path to its current
/// on-disk value.
///
/// Editors rewrite files via the atomic tmp-rename dance, which gives the file
/// a fresh inode on every save. If the recorded inode is never refreshed it
/// goes stale, which causes two distinct failures: a genuine later rename of
/// the file is *missed* (its current inode no longer matches the recorded one,
/// see [`find_rename_source`]), and the stale value is a now-freed inode that a
/// newly created file may be assigned, inviting a false rename match. Keeping
/// the recorded inode current on every observed change avoids both.
///
/// No-op if the path is not tracked or the inode could not be read.
pub(super) fn refresh_inode(
    file_identity: &mut HashMap<PathBuf, FileIdentity>,
    rel_path: &Path,
    current_inode: Option<u64>,
) {
    if let Some(identity) = file_identity.get_mut(rel_path) {
        identity.inode = current_inode;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{SyncCommand, SyncEvent};
    use crate::core::{DaemonCore, Event, EventStamp};
    use crate::daemon::session::CHANNEL_CAPACITY;
    use crate::daemon::tests::test_worker;
    use crate::watcher::FileEvent;

    fn identity(uuid: &str, inode: u64) -> FileIdentity {
        FileIdentity {
            document_uuid: uuid.to_owned(),
            inode: Some(inode),
        }
    }

    /// A new file whose inode matches a tracked document whose old path is gone
    /// is a genuine rename — the source path and UUID are reported.
    #[test]
    fn test_find_rename_source_matches_when_old_path_gone() {
        let mut map = HashMap::new();
        map.insert(PathBuf::from("notes.md"), identity("uuid-notes", 42));

        let found = find_rename_source(&map, 42, |_| false);

        assert_eq!(
            found,
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
    fn test_find_rename_source_rejects_when_old_path_still_exists() {
        let mut map = HashMap::new();
        map.insert(
            PathBuf::from("resumable.md"),
            identity("uuid-resumable", 99),
        );

        let found = find_rename_source(&map, 99, |_| true);

        assert_eq!(found, None);
    }

    /// No tracked document has the observed inode — nothing is reported.
    #[test]
    fn test_find_rename_source_no_inode_match() {
        let mut map = HashMap::new();
        map.insert(PathBuf::from("a.md"), identity("uuid-a", 1));

        assert_eq!(find_rename_source(&map, 2, |_| false), None);
    }

    /// A recorded inode goes stale when an editor rewrites the file (atomic
    /// tmp-rename changes the inode). A stale inode masks a genuine later
    /// rename — `find_rename_source` can't match the current inode — until the
    /// recorded value is refreshed. Reproduces the missed-rename bug.
    #[test]
    fn test_stale_inode_masks_rename_until_refreshed() {
        let mut map = HashMap::new();
        // foo.md was registered at inode 7; an atomic rewrite later moved its
        // content to inode 8, but the recorded value is still the freed 7.
        map.insert(PathBuf::from("foo.md"), identity("uuid-foo", 7));

        // foo.md is renamed to bar.md; rename preserves the current inode (8),
        // and foo.md no longer exists. The stale recorded inode (7) masks it:
        assert_eq!(find_rename_source(&map, 8, |_| false), None);

        // Refreshing the recorded inode to the file's current value restores
        // detection of the rename.
        refresh_inode(&mut map, Path::new("foo.md"), Some(8));
        assert_eq!(
            find_rename_source(&map, 8, |_| false),
            Some((PathBuf::from("foo.md"), "uuid-foo".to_owned()))
        );
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
        let mut map = HashMap::new();
        map.insert(PathBuf::from("foo.md"), identity("D", D_INODE));

        // move_identity(foo→bar_a) with bar_a absent on disk (its inode is None).
        let old_inode = map.get(Path::new("foo.md")).and_then(|id| id.inode);
        map.remove(Path::new("foo.md"));
        map.insert(
            PathBuf::from("bar_a.md"),
            FileIdentity {
                document_uuid: "D".to_owned(),
                inode: moved_inode(None, old_inode),
            },
        );

        // The local relocation (bar_b carries D's inode; bar_a is not on disk) is
        // recognized as a rename of D — not an untracked file to mint anew.
        assert_eq!(
            find_rename_source(&map, D_INODE, |_| false),
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

    /// Refreshing an untracked path is a no-op (no spurious entry created).
    #[test]
    fn test_refresh_inode_untracked_path_is_noop() {
        let mut map = HashMap::new();
        refresh_inode(&mut map, Path::new("ghost.md"), Some(5));
        assert!(map.is_empty());
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

        let mut worker2 = test_worker(dir.path().to_path_buf());
        // Clear and re-scan to prove scan_docs (not just `new`) does the load.
        worker2.state.documents.clear();
        worker2.scan_docs();
        assert!(worker2.state.get_doc(TEST_DOC_ID).is_some());
        assert_eq!(worker2.state.get_doc(TEST_DOC_ID).unwrap().content(), "aaa");
    }

    /// §3.4: a BATCH of offline renames resolves through the one-pass inode
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
            refresh_inode(
                &mut worker.state.file_identity,
                Path::new(name),
                crate::inode::get_inode(&abs),
            );
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

    /// Bug 1 / offline-rename inode persistence across restart (commits
    /// `4bfaa9c9` + `4d67eb7b`). A remote document registered while its file is
    /// ABSENT records `inode: None`; its first content materialization
    /// (`merge_remote_ops`) writes the file, reads the real OS inode into
    /// `file_identity`, and emits a coalesced `Effect::SaveState` so the inode is
    /// PERSISTED into `state.json`. After a daemon RESTART (`SpaceWorker::new`
    /// reloading the persisted state) and an offline `mv` of the file, the persisted
    /// inode is the ONLY way `detect_offline_renames` can match the moved file —
    /// its recorded path is gone, so the inode can no longer be read from disk
    /// there. Drop the persist and the restart reads `inode: null`,
    /// `space_file_with_inode(None)` never matches, and the moved file becomes a
    /// phantom (the `4bfaa9c9` bug, previously caught only by the
    /// `fs_converge_offline_rejoin` e2e at ~65s+).
    ///
    /// Drives the REAL glue chain end to end at the unit layer: `register_identity`
    /// (the `SubscribeRemote` registration that records `inode: None` for an absent
    /// file) → `DaemonCore::handle(Event::RemoteOps)` + `apply_effect` (the gamma
    /// `merge_remote_ops` materialization that writes the file, folds the real inode,
    /// and emits `Effect::SaveState`) → `flush_state_if_caught_up` (the `4d67eb7b`
    /// coalesced persist) → a fresh `SpaceWorker` over the same `.kutl` dir (the real
    /// restart) → an out-of-band `mv` on the temp disk → `detect_offline_renames`.
    /// Config-agnostic: `merge_remote_ops` is always-compiled core, so this exercises
    /// the inode-persist fix on the always-compiled core path.
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
            // is the `4d67eb7b` step that writes the just-recorded inode to state.json.
            worker.flush_state_if_caught_up(&fe_rx, &se_rx);
        }

        // ── the offline rename: mv the file on disk while "offline" ──
        std::fs::rename(dir.path().join(&old_rel), dir.path().join(&moved_rel))
            .expect("move the materialized file out of band");

        // ── session 2: the restart reads state.json, detection runs ──
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
}
