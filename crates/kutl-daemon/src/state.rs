//! Local daemon state persistence (`.kutl/state.json`).
//!
//! Caches path→UUID mappings so the daemon can restart quickly without
//! re-querying the relay registry for every known file.

use std::collections::HashMap;
use std::path::Path;

use tracing::warn;

use crate::safe_path::SafeRelayPath;

/// Local cache of path → UUID mappings for fast daemon restarts.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DaemonState {
    /// Maps relative file paths to document UUIDs.
    pub documents: HashMap<String, String>,
    /// Document UUIDs confirmed as active by the relay in the last session.
    /// Used on the next startup to detect documents that were unregistered
    /// by another client while we were offline.
    #[serde(default)]
    pub remote_document_ids: Vec<String>,
}

/// Name of the state file within the `.kutl` directory.
const STATE_FILE: &str = "state.json";

impl DaemonState {
    /// Load state from the `.kutl` directory. Returns an empty state if the
    /// file is missing or corrupted.
    pub fn load(kutl_dir: &Path) -> Self {
        let path = kutl_dir.join(STATE_FILE);
        match std::fs::read_to_string(&path) {
            Ok(data) => serde_json::from_str(&data).unwrap_or_else(|e| {
                warn!(error = %e, "corrupt state file, starting fresh");
                Self::empty()
            }),
            Err(_) => Self::empty(),
        }
    }

    /// Persist state to the `.kutl` directory.
    ///
    /// Uses atomic write (temp file + rename) to prevent corruption on crash.
    /// Creates the directory if it doesn't exist.
    pub fn save(&self, kutl_dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(kutl_dir)?;
        let path = kutl_dir.join(STATE_FILE);
        let data = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        let tmp = tempfile::NamedTempFile::new_in(kutl_dir)?;
        std::fs::write(tmp.path(), data)?;
        tmp.persist(path).map_err(|e| e.error)?;
        Ok(())
    }

    /// Return only the documents whose paths pass `SafeRelayPath` validation.
    ///
    /// Malicious or corrupt paths (traversal, absolute, .kutl-prefixed) are
    /// logged and skipped.
    pub fn validated_documents(&self) -> HashMap<String, String> {
        self.documents
            .iter()
            .filter(|(path, _)| match SafeRelayPath::new(path) {
                Ok(_) => true,
                Err(e) => {
                    warn!(path, "skipping invalid path in state.json: {e}");
                    false
                }
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Create an empty state.
    pub fn empty() -> Self {
        Self {
            documents: HashMap::new(),
            remote_document_ids: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_validated_documents_filters_unsafe_paths() {
        let state = DaemonState {
            documents: HashMap::from([
                ("notes/readme.md".to_owned(), "uuid-1".to_owned()),
                ("../../../etc/passwd".to_owned(), "uuid-2".to_owned()),
                ("/absolute/path.md".to_owned(), "uuid-3".to_owned()),
                (".kutl/state.json".to_owned(), "uuid-4".to_owned()),
            ]),
            remote_document_ids: vec![],
        };

        let safe = state.validated_documents();
        assert_eq!(safe.len(), 1);
        assert!(safe.contains_key("notes/readme.md"));
    }

    #[test]
    fn test_save_and_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");

        let mut state = DaemonState::empty();
        state
            .documents
            .insert("notes/ideas.md".into(), "uuid-1".into());
        state.documents.insert("readme.md".into(), "uuid-2".into());
        state.remote_document_ids = vec!["uuid-1".into(), "uuid-2".into()];
        state.save(&kutl_dir).unwrap();

        let loaded = DaemonState::load(&kutl_dir);
        assert_eq!(loaded.documents.len(), 2);
        assert_eq!(loaded.documents.get("notes/ideas.md").unwrap(), "uuid-1");
        assert_eq!(loaded.documents.get("readme.md").unwrap(), "uuid-2");
        assert_eq!(loaded.remote_document_ids.len(), 2);
        assert!(loaded.remote_document_ids.contains(&"uuid-1".to_string()));
        assert!(loaded.remote_document_ids.contains(&"uuid-2".to_string()));
    }

    #[test]
    fn test_load_missing_returns_empty() {
        let state = DaemonState::load(Path::new("/nonexistent/.kutl"));
        assert!(state.documents.is_empty());
    }

    #[test]
    fn test_load_legacy_state_without_remote_ids() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        // Old format without remote_document_ids field.
        std::fs::write(
            kutl_dir.join(STATE_FILE),
            r#"{"documents":{"doc.md":"uuid-1"}}"#,
        )
        .unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert_eq!(state.documents.len(), 1);
        assert!(
            state.remote_document_ids.is_empty(),
            "legacy state should default to empty remote_document_ids"
        );
    }

    #[test]
    fn test_load_corrupted_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        std::fs::write(kutl_dir.join(STATE_FILE), "not valid json").unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert!(state.documents.is_empty());
    }
}
