//! Document manager for per-file CRDT documents stored in `.kutl/docs/`.
//!
//! Sidecars are keyed by **document id** (a UUID), not by path:
//! `.kutl/docs/<document-id>.dt`. This decouples a document's CRDT state from
//! wherever it currently lives on disk, so a rename never moves the sidecar
//! (the id is stable) and — crucially — two distinct documents that transiently
//! contend for one path never share one sidecar (the precondition for
//! conflict-copy; see `2026-06-01-daemon-uuid-rekey-plan.md`). The on-disk path
//! ↔ id mapping lives in the daemon's `file_identity` / `uuid_to_path`, not here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use kutl_core::Document;
use tracing::info;

/// Manages per-document `Document` instances, sidecar-stored by id in
/// `.kutl/docs/<document-id>.dt`.
pub struct DocumentManager {
    space_root: PathBuf,
    docs_dir: PathBuf,
    documents: HashMap<String, Document>,
}

impl DocumentManager {
    /// Create a new document manager for the given space.
    pub fn new(space_root: PathBuf) -> Result<Self> {
        let docs_dir = space_root.join(".kutl").join("docs");
        std::fs::create_dir_all(&docs_dir)
            .with_context(|| format!("failed to create {}", docs_dir.display()))?;

        Ok(Self {
            space_root,
            docs_dir,
            documents: HashMap::new(),
        })
    }

    /// Get or create the document for `document_id`.
    ///
    /// If the sidecar exists on disk (in `.kutl/docs/`), loads it. Otherwise
    /// creates a new empty document.
    pub fn load_or_create(&mut self, document_id: &str) -> Result<&mut Document> {
        if !self.documents.contains_key(document_id) {
            let dt_path = self.dt_path(document_id);
            let doc = if dt_path.exists() {
                Document::load(&dt_path)
                    .with_context(|| format!("failed to load {}", dt_path.display()))?
            } else {
                Document::new()
            };
            self.documents.insert(document_id.to_owned(), doc);
        }

        Ok(self.documents.get_mut(document_id).expect("just inserted"))
    }

    /// Save the document for `document_id` to `.kutl/docs/`.
    pub fn save(&self, document_id: &str) -> Result<()> {
        let dt_path = self.dt_path(document_id);
        if let Some(doc) = self.documents.get(document_id) {
            doc.save(&dt_path)
                .with_context(|| format!("failed to save {}", dt_path.display()))?;
        }
        Ok(())
    }

    /// The `.dt` sidecar path for a document id: `.kutl/docs/<document-id>.dt`.
    /// Flat (a document id is a UUID — no path separators), so no nesting.
    pub fn dt_path(&self, document_id: &str) -> PathBuf {
        self.docs_dir.join(format!("{document_id}.dt"))
    }

    /// The absolute on-disk path for a relative space path (the user's file).
    /// Unrelated to sidecar storage — purely `space_root` + `rel_path`.
    pub fn file_path(&self, rel_path: &Path) -> PathBuf {
        self.space_root.join(rel_path)
    }

    /// Scan `.kutl/docs/` on startup and load existing sidecars, keyed by id.
    pub fn scan_existing(&mut self) -> Result<()> {
        let dt_files = walk_dt_files(&self.docs_dir).unwrap_or_default();
        for dt_path in dt_files {
            if let Some(id) = dt_to_id(&dt_path)
                && let std::collections::hash_map::Entry::Vacant(entry) =
                    self.documents.entry(id.clone())
            {
                match Document::load(&dt_path) {
                    Ok(doc) => {
                        info!(document_id = %id, "loaded existing document");
                        entry.insert(doc);
                    }
                    Err(e) => {
                        tracing::warn!(document_id = %id, error = %e, "failed to load document, skipping");
                    }
                }
            }
        }
        Ok(())
    }

    /// Remove a document from memory and delete its `.dt` sidecar from disk.
    pub fn remove(&mut self, document_id: &str) {
        self.documents.remove(document_id);
        let dt_path = self.dt_path(document_id);
        if dt_path.exists()
            && let Err(e) = std::fs::remove_file(&dt_path)
        {
            tracing::warn!(path = %dt_path.display(), error = %e, "failed to remove dt sidecar");
        }
    }

    /// Get a reference to a loaded document by id.
    pub fn get(&self, document_id: &str) -> Option<&Document> {
        self.documents.get(document_id)
    }
}

/// Recursively find all `.dt` files under a directory.
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

/// Extract the document id from a sidecar path: `.../<document-id>.dt` → `<document-id>`.
fn dt_to_id(dt_path: &Path) -> Option<String> {
    dt_path
        .file_stem()
        .and_then(|s| s.to_str())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_core::Boundary;
    use tempfile::TempDir;

    const DOC_A: &str = "11111111-1111-4111-8111-111111111111";

    #[test]
    fn test_dt_path_mapping() {
        let mgr = DocumentManager {
            space_root: PathBuf::from("/tmp/space"),
            docs_dir: PathBuf::from("/tmp/space/.kutl/docs"),
            documents: HashMap::new(),
        };
        assert_eq!(
            mgr.dt_path(DOC_A),
            PathBuf::from(format!("/tmp/space/.kutl/docs/{DOC_A}.dt"))
        );
    }

    #[test]
    fn test_dt_to_id() {
        assert_eq!(
            dt_to_id(Path::new(&format!("/tmp/space/.kutl/docs/{DOC_A}.dt"))),
            Some(DOC_A.to_owned())
        );
    }

    #[test]
    fn test_load_or_create_new() {
        let dir = TempDir::new().unwrap();
        let mut mgr = DocumentManager::new(dir.path().to_owned()).unwrap();
        let doc = mgr.load_or_create(DOC_A).unwrap();
        assert!(doc.is_empty());
    }

    #[test]
    fn test_save_and_reload() {
        let dir = TempDir::new().unwrap();
        let mut mgr = DocumentManager::new(dir.path().to_owned()).unwrap();

        let doc = mgr.load_or_create(DOC_A).unwrap();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(agent, "test", "init", Boundary::Auto, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();

        mgr.save(DOC_A).unwrap();

        // Reload from scratch.
        let mut mgr2 = DocumentManager::new(dir.path().to_owned()).unwrap();
        let doc2 = mgr2.load_or_create(DOC_A).unwrap();
        assert_eq!(doc2.content(), "hello");
    }

    #[test]
    fn test_scan_existing() {
        let dir = TempDir::new().unwrap();
        let mut mgr = DocumentManager::new(dir.path().to_owned()).unwrap();

        let doc = mgr.load_or_create(DOC_A).unwrap();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(agent, "test", "init", Boundary::Auto, |ctx| {
            ctx.insert(0, "aaa")
        })
        .unwrap();
        mgr.save(DOC_A).unwrap();

        let mut mgr2 = DocumentManager::new(dir.path().to_owned()).unwrap();
        mgr2.scan_existing().unwrap();
        assert!(mgr2.get(DOC_A).is_some());
        assert_eq!(mgr2.get(DOC_A).unwrap().content(), "aaa");
    }
}
