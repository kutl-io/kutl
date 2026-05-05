//! Document manager for per-file CRDT documents stored in `.kutl/docs/`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use kutl_core::Document;
use tracing::info;

/// Manages per-file `Document` instances stored in `.kutl/docs/`.
pub struct DocumentManager {
    space_root: PathBuf,
    docs_dir: PathBuf,
    documents: HashMap<PathBuf, Document>,
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

    /// Get or create a document for the given relative path.
    ///
    /// If the document exists on disk (in `.kutl/docs/`), loads it.
    /// Otherwise creates a new empty document.
    pub fn load_or_create(&mut self, rel_path: &Path) -> Result<&mut Document> {
        if !self.documents.contains_key(rel_path) {
            let dt_path = self.dt_path(rel_path);
            let doc = if dt_path.exists() {
                Document::load(&dt_path)
                    .with_context(|| format!("failed to load {}", dt_path.display()))?
            } else {
                Document::new()
            };
            self.documents.insert(rel_path.to_owned(), doc);
        }

        Ok(self.documents.get_mut(rel_path).expect("just inserted"))
    }

    /// Save a document to `.kutl/docs/`.
    pub fn save(&self, rel_path: &Path) -> Result<()> {
        let dt_path = self.dt_path(rel_path);
        if let Some(parent) = dt_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if let Some(doc) = self.documents.get(rel_path) {
            doc.save(&dt_path)
                .with_context(|| format!("failed to save {}", dt_path.display()))?;
        }
        Ok(())
    }

    /// Return the `.dt` file path for a given relative path.
    pub fn dt_path(&self, rel_path: &Path) -> PathBuf {
        // e.g. src/main.rs → .kutl/docs/src/main.rs.dt
        self.docs_dir.join(format!("{}.dt", rel_path.display()))
    }

    /// Return the absolute file path for a relative path in the space.
    pub fn file_path(&self, rel_path: &Path) -> PathBuf {
        self.space_root.join(rel_path)
    }

    /// List all known document relative paths (from loaded docs + on-disk .dt files).
    pub fn known_documents(&self) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = self.documents.keys().cloned().collect();

        // Also scan .kutl/docs/ for .dt files not yet loaded.
        if let Ok(entries) = walk_dt_files(&self.docs_dir) {
            for dt_path in entries {
                if let Some(rel) = dt_to_rel_path(&self.docs_dir, &dt_path)
                    && !paths.contains(&rel)
                {
                    paths.push(rel);
                }
            }
        }

        paths
    }

    /// Scan `.kutl/docs/` on startup and load existing documents.
    pub fn scan_existing(&mut self) -> Result<()> {
        let dt_files = walk_dt_files(&self.docs_dir).unwrap_or_default();
        for dt_path in dt_files {
            if let Some(rel) = dt_to_rel_path(&self.docs_dir, &dt_path)
                && let std::collections::hash_map::Entry::Vacant(entry) =
                    self.documents.entry(rel.clone())
            {
                match Document::load(&dt_path) {
                    Ok(doc) => {
                        info!(?rel, "loaded existing document");
                        entry.insert(doc);
                    }
                    Err(e) => {
                        tracing::warn!(?rel, error = %e, "failed to load document, skipping");
                    }
                }
            }
        }
        Ok(())
    }

    /// Remove a document from memory and delete its `.dt` sidecar from disk.
    pub fn remove(&mut self, rel_path: &Path) {
        self.documents.remove(rel_path);
        let dt_path = self.dt_path(rel_path);
        if dt_path.exists()
            && let Err(e) = std::fs::remove_file(&dt_path)
        {
            tracing::warn!(path = %dt_path.display(), error = %e, "failed to remove dt sidecar");
        }
    }

    /// Rename a document: move the `.dt` sidecar and update the in-memory map.
    pub fn rename(&mut self, old_path: &Path, new_path: &Path) -> Result<()> {
        let old_dt = self.dt_path(old_path);
        let new_dt = self.dt_path(new_path);
        if old_dt.exists() {
            if let Some(parent) = new_dt.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::rename(&old_dt, &new_dt).with_context(|| {
                format!(
                    "failed to rename sidecar {} to {}",
                    old_dt.display(),
                    new_dt.display()
                )
            })?;
        }
        if let Some(doc) = self.documents.remove(old_path) {
            self.documents.insert(new_path.to_path_buf(), doc);
        }
        Ok(())
    }

    /// Get a reference to a loaded document.
    pub fn get(&self, rel_path: &Path) -> Option<&Document> {
        self.documents.get(rel_path)
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

/// Convert a `.dt` path back to the relative space path.
///
/// E.g. `.kutl/docs/src/main.rs.dt` → `src/main.rs`
fn dt_to_rel_path(docs_dir: &Path, dt_path: &Path) -> Option<PathBuf> {
    let relative_to_docs = dt_path.strip_prefix(docs_dir).ok()?;
    let s = relative_to_docs.to_string_lossy();
    let stripped = s.strip_suffix(".dt")?;
    Some(PathBuf::from(stripped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_core::Boundary;
    use tempfile::TempDir;

    #[test]
    fn test_dt_path_mapping() {
        let mgr = DocumentManager {
            space_root: PathBuf::from("/tmp/space"),
            docs_dir: PathBuf::from("/tmp/space/.kutl/docs"),
            documents: HashMap::new(),
        };
        assert_eq!(
            mgr.dt_path(Path::new("src/main.rs")),
            PathBuf::from("/tmp/space/.kutl/docs/src/main.rs.dt")
        );
    }

    #[test]
    fn test_dt_to_rel_path() {
        let docs = Path::new("/tmp/space/.kutl/docs");
        assert_eq!(
            dt_to_rel_path(docs, Path::new("/tmp/space/.kutl/docs/src/main.rs.dt")),
            Some(PathBuf::from("src/main.rs"))
        );
    }

    #[test]
    fn test_load_or_create_new() {
        let dir = TempDir::new().unwrap();
        let mut mgr = DocumentManager::new(dir.path().to_owned()).unwrap();
        let doc = mgr.load_or_create(Path::new("test.txt")).unwrap();
        assert!(doc.is_empty());
    }

    #[test]
    fn test_save_and_reload() {
        let dir = TempDir::new().unwrap();
        let mut mgr = DocumentManager::new(dir.path().to_owned()).unwrap();

        let doc = mgr.load_or_create(Path::new("test.txt")).unwrap();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(agent, "test", "init", Boundary::Auto, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();

        mgr.save(Path::new("test.txt")).unwrap();

        // Reload from scratch.
        let mut mgr2 = DocumentManager::new(dir.path().to_owned()).unwrap();
        let doc2 = mgr2.load_or_create(Path::new("test.txt")).unwrap();
        assert_eq!(doc2.content(), "hello");
    }

    #[test]
    fn test_scan_existing() {
        let dir = TempDir::new().unwrap();
        let mut mgr = DocumentManager::new(dir.path().to_owned()).unwrap();

        let doc = mgr.load_or_create(Path::new("a.txt")).unwrap();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(agent, "test", "init", Boundary::Auto, |ctx| {
            ctx.insert(0, "aaa")
        })
        .unwrap();
        mgr.save(Path::new("a.txt")).unwrap();

        let mut mgr2 = DocumentManager::new(dir.path().to_owned()).unwrap();
        mgr2.scan_existing().unwrap();
        assert!(mgr2.get(Path::new("a.txt")).is_some());
        assert_eq!(mgr2.get(Path::new("a.txt")).unwrap().content(), "aaa");
    }
}
