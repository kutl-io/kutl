//! Relay-maintained document registry for UUID-based document identity.
//!
//! Each space has its own `DocumentRegistry` that maps document UUIDs to paths.
//! Operations are structured (register, rename, unregister) — not free-form edits.

use std::collections::HashMap;

/// Source provenance metadata, optional on every register.
///
/// The daemon path populates `originally_created_at_ms` only (filesystem
/// birthtime). The ingestion worker populates all six fields. Internal
/// callers (MCP, presence) pass [`Default::default`].
///
/// Bundled into one struct rather than positional arguments so the
/// `register_document_internal` signature doesn't grow past clippy's
/// `too_many_arguments` ceiling — and so adding a seventh provenance
/// field later doesn't touch every caller.
#[derive(Debug, Default, Clone)]
pub struct RegisterRfd0081 {
    /// Filesystem birthtime in Unix millis (daemon path).
    pub originally_created_at_ms: Option<i64>,
    /// `SourceKind` enum value (proto-encoded as `u32`).
    pub source_kind: Option<u32>,
    /// Source-specific identifier (e.g. Notion page id).
    pub source_id: Option<String>,
    /// Canonical URL at the source.
    pub source_url: Option<String>,
    /// Ingestion job that produced this document (UUID string).
    pub ingestion_job_id: Option<String>,
    /// Author label as reported by the source (free-form display).
    pub source_author_display: Option<String>,
}

/// Metadata for a registry operation (who did it, when).
#[derive(Debug, Clone, Default)]
pub struct EntryMetadata {
    /// DID of the actor who performed the operation.
    pub author_did: String,
    /// Unix millis timestamp.
    pub timestamp: i64,
    /// Resolved account UUID from the authenticated connection.
    pub account_id: Option<String>,
    /// Filesystem birthtime in Unix millis. Set on register operations
    /// when the daemon's host filesystem exposes a creation time;
    /// `None` otherwise. Ignored on rename/unregister (the registry
    /// stores it once at register time).
    pub originally_created_at: Option<i64>,
    /// `SourceKind` enum value (the proto enum encoded as `u32`).
    /// `None` on the daemon path; the persistence layer substitutes
    /// the DB default of 0 (native).
    pub source_kind: Option<u32>,
    /// Source-specific identifier (e.g. Notion page id). `None` on
    /// the daemon path.
    pub source_id: Option<String>,
    /// Canonical URL at the source (e.g. notion.so/page). `None` on
    /// the daemon path.
    pub source_url: Option<String>,
    /// Ingestion job that produced this document. `None` on the
    /// daemon path.
    pub ingestion_job_id: Option<String>,
    /// Author label as reported by the source (free-form display
    /// string, not a kutl DID). `None` on the daemon path.
    pub source_author_display: Option<String>,
}

/// A single entry in the document registry.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RegistryEntry {
    /// Unique document identifier (UUID).
    pub document_id: String,
    /// Current path within the space.
    pub path: String,
    /// DID of the creator.
    pub created_by: String,
    /// Unix millis when the document was registered.
    pub created_at: i64,
    /// DID of the last renamer, if ever renamed.
    pub renamed_by: Option<String>,
    /// Unix millis of the last rename, if ever renamed.
    pub renamed_at: Option<i64>,
    /// Unix millis of soft-deletion, if deleted.
    pub deleted_at: Option<i64>,
    /// Resolved account UUID (DB-backed mode). `None` without a DB.
    pub account_id: Option<String>,
    /// Unix millis of the last content edit, if any.
    pub edited_at: Option<i64>,
    /// Filesystem birthtime in Unix millis. Set once at register time
    /// from the daemon's `stat`; never updated.
    pub originally_created_at: Option<i64>,
    /// `SourceKind` enum value. `None` for native registers (daemon
    /// path); the persistence layer substitutes 0.
    pub source_kind: Option<u32>,
    /// Source identifier. `None` for native registers.
    pub source_id: Option<String>,
    /// Source URL. `None` for native registers.
    pub source_url: Option<String>,
    /// Ingestion job id. `None` for native registers.
    pub ingestion_job_id: Option<String>,
    /// Source author display name. `None` for native registers.
    pub source_author_display: Option<String>,
}

/// Relay-maintained document registry for a single space.
///
/// Maps document UUIDs to paths. Only accepts structured operations
/// (register, rename, unregister) — not free-form edits.
pub struct DocumentRegistry {
    /// Keyed by `document_id`.
    entries: HashMap<String, RegistryEntry>,
    /// Reverse index: path → `document_id` (active entries only).
    path_index: HashMap<String, String>,
}

impl DocumentRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            path_index: HashMap::new(),
        }
    }

    /// Build a registry from a list of entries (e.g. loaded from a database).
    ///
    /// Builds both the `entries` map and `path_index` from the flat list.
    /// Only non-deleted entries are indexed by path.
    pub fn from_entries(entries: Vec<RegistryEntry>) -> Self {
        let mut reg = Self::new();
        for entry in entries {
            if entry.deleted_at.is_none() {
                reg.path_index
                    .insert(entry.path.clone(), entry.document_id.clone());
            }
            reg.entries.insert(entry.document_id.clone(), entry);
        }
        reg
    }

    /// Register a new document. Fails if the path or `document_id` is already taken.
    pub fn register(
        &mut self,
        document_id: &str,
        path: &str,
        meta: EntryMetadata,
    ) -> Result<&RegistryEntry, RegistryError> {
        if self.entries.contains_key(document_id) {
            return Err(RegistryError::DuplicateId(document_id.to_string()));
        }
        if self.path_index.contains_key(path) {
            return Err(RegistryError::PathOccupied(path.to_string()));
        }
        let entry = RegistryEntry {
            document_id: document_id.to_string(),
            path: path.to_string(),
            created_by: meta.author_did,
            created_at: meta.timestamp,
            renamed_by: None,
            renamed_at: None,
            deleted_at: None,
            account_id: meta.account_id,
            edited_at: None,
            originally_created_at: meta.originally_created_at,
            source_kind: meta.source_kind,
            source_id: meta.source_id,
            source_url: meta.source_url,
            ingestion_job_id: meta.ingestion_job_id,
            source_author_display: meta.source_author_display,
        };
        self.path_index
            .insert(path.to_string(), document_id.to_string());
        self.entries.insert(document_id.to_string(), entry);
        Ok(self.entries.get(document_id).expect("just inserted"))
    }

    /// Rename a document. Fails if the document doesn't exist or new path is taken.
    pub fn rename(
        &mut self,
        document_id: &str,
        old_path: &str,
        new_path: &str,
        meta: EntryMetadata,
    ) -> Result<&RegistryEntry, RegistryError> {
        let entry = self
            .entries
            .get(document_id)
            .ok_or_else(|| RegistryError::NotFound(document_id.to_string()))?;
        if entry.deleted_at.is_some() {
            return Err(RegistryError::NotFound(document_id.to_string()));
        }
        if entry.path != old_path {
            return Err(RegistryError::PathMismatch {
                expected: entry.path.clone(),
                got: old_path.to_string(),
            });
        }
        // Check new path isn't taken by a different document.
        if let Some(existing_id) = self.path_index.get(new_path)
            && existing_id != document_id
        {
            return Err(RegistryError::PathOccupied(new_path.to_string()));
        }
        self.path_index.remove(old_path);
        self.path_index
            .insert(new_path.to_string(), document_id.to_string());
        let entry = self.entries.get_mut(document_id).expect("checked above");
        entry.path = new_path.to_string();
        entry.renamed_by = Some(meta.author_did);
        entry.renamed_at = Some(meta.timestamp);
        Ok(entry)
    }

    /// Remove a document entry entirely from the registry, without
    /// soft-delete bookkeeping.
    ///
    /// RFD 0042 amendment 2026-05-24: this is the rollback primitive
    /// used by the relay's lifecycle handlers when the backend
    /// `save_entry` fails after a successful in-memory `register`. The
    /// in-memory mutation must be undone so the registry never
    /// diverges from the persisted mirror. Not for general use — soft
    /// delete via [`Self::unregister`] is the user-facing operation.
    pub fn remove_for_rollback(&mut self, document_id: &str) {
        if let Some(entry) = self.entries.remove(document_id) {
            // Path index is only populated for active rows. Defensive:
            // only drop the path mapping if it still points at us.
            if let Some(existing) = self.path_index.get(&entry.path)
                && existing == document_id
            {
                self.path_index.remove(&entry.path);
            }
        }
    }

    /// Restore a prior rename in place — used by the relay's
    /// rename-handler rollback path (RFD 0042 amendment 2026-05-24)
    /// when the backend `save_entry` fails after an in-memory rename
    /// has committed. `prior_path`, `prior_renamed_by`, and
    /// `prior_renamed_at` are captured before the rename; this method
    /// re-establishes them.
    pub fn restore_after_rename_rollback(
        &mut self,
        document_id: &str,
        current_path: &str,
        prior_path: &str,
        prior_renamed_by: Option<String>,
        prior_renamed_at: Option<i64>,
    ) {
        // Drop the new path → doc binding only if it still points at us
        // (no concurrent reuse).
        if let Some(existing) = self.path_index.get(current_path)
            && existing == document_id
        {
            self.path_index.remove(current_path);
        }
        // Restore the old path → doc binding unless something else has
        // already claimed it.
        if !self.path_index.contains_key(prior_path) {
            self.path_index
                .insert(prior_path.to_string(), document_id.to_string());
        }
        if let Some(entry) = self.entries.get_mut(document_id) {
            entry.path = prior_path.to_string();
            entry.renamed_by = prior_renamed_by;
            entry.renamed_at = prior_renamed_at;
        }
    }

    /// Transfer `account_id` for every entry (active + soft-deleted)
    /// in the registry to a new account.
    ///
    /// RFD 0042 amendment 2026-05-24 (B-full follow-up): used by the
    /// relay's `TransferSpaceOwnership` envelope handler at
    /// ingestion accept time. Both active and soft-deleted entries
    /// flip so the mirror's `created_by`/`updated_by` columns stay
    /// consistent with the registry across the full row set.
    pub fn transfer_ownership(&mut self, new_account_id: &str) {
        for entry in self.entries.values_mut() {
            entry.account_id = Some(new_account_id.to_owned());
        }
    }

    /// Undo a soft-delete in place — used by the relay's
    /// unregister-handler rollback path (RFD 0042 amendment 2026-05-24)
    /// when the backend `delete_entry` fails after the in-memory
    /// `unregister` has committed.
    pub fn restore_after_unregister_rollback(&mut self, document_id: &str) {
        if let Some(entry) = self.entries.get_mut(document_id) {
            entry.deleted_at = None;
            // Re-insert path → doc binding only if nothing has reclaimed
            // the path while we held the soft-delete.
            if !self.path_index.contains_key(&entry.path) {
                self.path_index
                    .insert(entry.path.clone(), document_id.to_string());
            }
        }
    }

    /// Soft-delete a document. Releases its path for reuse.
    pub fn unregister(
        &mut self,
        document_id: &str,
        meta: &EntryMetadata,
    ) -> Result<(), RegistryError> {
        let entry = self
            .entries
            .get_mut(document_id)
            .ok_or_else(|| RegistryError::NotFound(document_id.to_string()))?;
        if entry.deleted_at.is_some() {
            return Err(RegistryError::NotFound(document_id.to_string()));
        }
        self.path_index.remove(&entry.path);
        entry.deleted_at = Some(meta.timestamp);
        Ok(())
    }

    /// Look up an active (non-deleted) document by ID.
    pub fn get(&self, document_id: &str) -> Option<&RegistryEntry> {
        self.entries
            .get(document_id)
            .filter(|e| e.deleted_at.is_none())
    }

    /// Look up any document by ID, including soft-deleted entries.
    pub fn get_any(&self, document_id: &str) -> Option<&RegistryEntry> {
        self.entries.get(document_id)
    }

    /// Mutable lookup of any entry by ID, including soft-deleted ones.
    /// Used by callers that need to merge in late-arriving metadata
    /// (e.g. provenance leave-as-is-on-omit semantics from RFD 0083).
    pub fn get_mut_any(&mut self, document_id: &str) -> Option<&mut RegistryEntry> {
        self.entries.get_mut(document_id)
    }

    /// Look up an active document by path.
    pub fn get_by_path(&self, path: &str) -> Option<&RegistryEntry> {
        let id = self.path_index.get(path)?;
        self.get(id)
    }

    /// Iterate over all active (non-deleted) entries.
    pub fn active_entries(&self) -> impl Iterator<Item = (&str, &RegistryEntry)> {
        self.entries
            .iter()
            .filter(|(_, e)| e.deleted_at.is_none())
            .map(|(id, e)| (id.as_str(), e))
    }

    /// All entries including soft-deleted, for persistence.
    pub fn all_entries(&self) -> impl Iterator<Item = &RegistryEntry> {
        self.entries.values()
    }

    /// Number of active entries.
    pub fn len(&self) -> usize {
        self.path_index.len()
    }

    /// Whether the registry has no active entries.
    pub fn is_empty(&self) -> bool {
        self.path_index.is_empty()
    }
}

impl Default for DocumentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors from registry operations.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// A document with this ID is already registered.
    #[error("document already registered: {0}")]
    DuplicateId(String),
    /// The target path is already occupied by another document.
    #[error("path already occupied: {0}")]
    PathOccupied(String),
    /// No active document found with this ID.
    #[error("document not found: {0}")]
    NotFound(String),
    /// The current path doesn't match what was expected.
    #[error("path mismatch: expected {expected}, got {got}")]
    PathMismatch {
        /// The actual current path.
        expected: String,
        /// The path that was provided.
        got: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(did: &str, ts: i64) -> EntryMetadata {
        EntryMetadata {
            author_did: did.to_string(),
            timestamp: ts,
            ..Default::default()
        }
    }

    #[test]
    fn test_register_and_lookup() {
        let mut reg = DocumentRegistry::new();
        let result = reg.register("doc-1", "notes/ideas.md", meta("did:alice", 1000));
        assert!(result.is_ok());
        let entry = reg.get("doc-1").unwrap();
        assert_eq!(entry.path, "notes/ideas.md");
        assert_eq!(entry.created_by, "did:alice");
    }

    #[test]
    fn test_register_duplicate_path_rejected() {
        let mut reg = DocumentRegistry::new();
        reg.register("doc-1", "notes/ideas.md", meta("did:alice", 1000))
            .unwrap();
        let result = reg.register("doc-2", "notes/ideas.md", meta("did:bob", 2000));
        assert!(result.is_err());
    }

    #[test]
    fn test_register_duplicate_id_rejected() {
        let mut reg = DocumentRegistry::new();
        reg.register("doc-1", "notes/ideas.md", meta("did:alice", 1000))
            .unwrap();
        let result = reg.register("doc-1", "other.md", meta("did:bob", 2000));
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_happy_path() {
        let mut reg = DocumentRegistry::new();
        reg.register("doc-1", "draft.md", meta("did:alice", 1000))
            .unwrap();
        let result = reg.rename(
            "doc-1",
            "draft.md",
            "archive/draft.md",
            meta("did:bob", 2000),
        );
        assert!(result.is_ok());
        let entry = reg.get("doc-1").unwrap();
        assert_eq!(entry.path, "archive/draft.md");
        assert_eq!(entry.renamed_by.as_deref(), Some("did:bob"));
        assert_eq!(entry.renamed_at, Some(2000));
    }

    #[test]
    fn test_rename_to_occupied_path_rejected() {
        let mut reg = DocumentRegistry::new();
        reg.register("doc-1", "a.md", meta("did:alice", 1000))
            .unwrap();
        reg.register("doc-2", "b.md", meta("did:alice", 1000))
            .unwrap();
        let result = reg.rename("doc-1", "a.md", "b.md", meta("did:alice", 2000));
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_nonexistent_rejected() {
        let mut reg = DocumentRegistry::new();
        let result = reg.rename("doc-1", "a.md", "b.md", meta("did:alice", 1000));
        assert!(result.is_err());
    }

    #[test]
    fn test_unregister_soft_delete() {
        let mut reg = DocumentRegistry::new();
        reg.register("doc-1", "notes/ideas.md", meta("did:alice", 1000))
            .unwrap();
        reg.unregister("doc-1", &meta("did:bob", 2000)).unwrap();
        // Soft-deleted: get returns None for active lookups
        assert!(reg.get("doc-1").is_none());
        // Path is released: can register a new doc at the same path
        let result = reg.register("doc-2", "notes/ideas.md", meta("did:carol", 3000));
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_active_entries() {
        let mut reg = DocumentRegistry::new();
        reg.register("doc-1", "a.md", meta("did:alice", 1000))
            .unwrap();
        reg.register("doc-2", "b.md", meta("did:alice", 1000))
            .unwrap();
        reg.unregister("doc-1", &meta("did:alice", 2000)).unwrap();
        let active: Vec<_> = reg.active_entries().collect();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].0, "doc-2");
    }

    #[test]
    fn test_from_entries() {
        let entries = vec![
            RegistryEntry {
                document_id: "doc-1".to_string(),
                path: "a.md".to_string(),
                created_by: "did:alice".to_string(),
                created_at: 1000,
                ..Default::default()
            },
            RegistryEntry {
                document_id: "doc-2".to_string(),
                path: "b.md".to_string(),
                created_by: "did:bob".to_string(),
                created_at: 2000,
                deleted_at: Some(3000),
                ..Default::default()
            },
        ];
        let reg = DocumentRegistry::from_entries(entries);
        assert_eq!(reg.len(), 1);
        assert!(reg.get("doc-1").is_some());
        assert!(reg.get("doc-2").is_none());
        assert!(reg.get_by_path("a.md").is_some());
        assert!(reg.get_by_path("b.md").is_none());
    }

    #[test]
    fn test_rename_conflict_lww() {
        let mut reg = DocumentRegistry::new();
        reg.register("doc-1", "a.md", meta("did:alice", 1000))
            .unwrap();
        // First rename
        reg.rename("doc-1", "a.md", "b.md", meta("did:bob", 2000))
            .unwrap();
        // Second rename
        reg.rename("doc-1", "b.md", "c.md", meta("did:carol", 3000))
            .unwrap();
        let entry = reg.get("doc-1").unwrap();
        assert_eq!(entry.path, "c.md");
    }
}
