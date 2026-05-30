//! Persistence backends for the document registry.
//!
//! The `RegistryBackend` trait abstracts storage so the relay can use `SQLite`
//! for self-hosted deployments and other SQL backends in DB-backed deployments.

use std::path::Path;

use crate::registry::RegistryEntry;

/// Errors from registry storage operations.
#[derive(Debug, thiserror::Error)]
pub enum RegistryStoreError {
    /// Database error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Migration error.
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// A mirror-enabled backend was handed a [`RegistryEntry`] with
    /// `account_id = None`.
    ///
    /// Kutlhub deployments resolve `did → account_id` during the
    /// WebSocket handshake via `custodied_keys`, so by the time
    /// [`RegistryBackend::save_entry`] is called the `account_id` must
    /// be `Some`. A `None` reaching the mirror INSERT means either the
    /// auth path was bypassed or the membership backend dropped the
    /// resolution — either way, silently skipping the mirror INSERT
    /// would produce the exact `relay_registry` ↔ `documents_table`
    /// divergence the RFD 0042 amendment was designed to prevent.
    /// Returning this error fails the lifecycle ack and rolls back the
    /// in-memory registry mutation (see `persist_entry` in relay.rs).
    #[error(
        "save_entry called with account_id = none on a mirror-enabled backend: \
         kutlhub registrations must have a resolved account_id (space {space_id}, \
         document {document_id})"
    )]
    MissingAccountIdForMirror {
        /// Space the offending register targeted.
        space_id: String,
        /// Document the offending register targeted.
        document_id: String,
    },
}

/// UX-only metadata threaded alongside [`RegistryBackend::save_entry`].
///
/// These fields are **mirror-only** — they are not part of [`RegistryEntry`]
/// and are not persisted in the registry itself. They ride along to the
/// backend so the kutlhub-relay's PG mirror can populate the corresponding
/// `documents.*` columns. Backends without a mirror (the OSS `SQLite`
/// path) ignore everything in here.
///
/// All five fields are `Option<&str>` / `Option<i64>` so callers can pass
/// `Default::default()` on a re-register that doesn't carry new metadata;
/// the backend's `ON CONFLICT DO UPDATE SET col = COALESCE($new, col)`
/// pattern protects an earlier non-null write from being clobbered.
///
/// RFD 0042 amendment 2026-05-24 introduced `title` + `content_type` here;
/// the 2026-05-24 follow-up added the convert-path trio
/// (`converted_from_id`, `converted_from_filename`, `size_bytes`) to
/// close the `documentsTable` escape hatch on the UX-server side.
#[derive(Clone, Copy, Debug, Default)]
pub struct MirrorMetadata<'a> {
    /// `documents.title` — UX-supplied display title.
    pub title: Option<&'a str>,
    /// `documents.content_type` — MIME type (sniffed or supplied).
    pub content_type: Option<&'a str>,
    /// `documents.converted_from_id` — pointer back to the source doc
    /// when this row is the markdown output of a convert operation.
    pub converted_from_id: Option<&'a str>,
    /// `documents.converted_from_filename` — original filename for the
    /// convert source (e.g. "Meeting Notes.docx").
    pub converted_from_filename: Option<&'a str>,
    /// `documents.size_bytes` — at-register byte count of the original
    /// upload, used by tier-quota accounting (RFD 0063).
    pub size_bytes: Option<i64>,
}

/// Durable projection of a document's persisted state.
///
/// `persisted_version` mirrors `ContentState.persisted_version` and records
/// the max `edit_counter` value the storage backend has confirmed durable.
/// `persisted_at` is the wall-clock time of that confirmation, kept for
/// observability only; it is not part of the lattice algebra.
///
/// A document that has never been successfully flushed returns a witness
/// with `persisted_version == 0`. Read classification treats that case
/// combined with a `None` load result as legitimately empty; any other
/// combination signals `Inconsistent`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Witness {
    /// Max `edit_counter` value confirmed durable.
    pub persisted_version: u64,
    /// Wall-clock of the confirmation (millis since epoch).
    pub persisted_at: i64,
}

/// Abstraction over registry persistence.
///
/// The in-memory `DocumentRegistry` remains the source of truth during
/// operation; the backend is used for startup loading and write-behind
/// persistence.
pub trait RegistryBackend: Send + Sync {
    /// Load all active (non-deleted) entries for a space.
    fn load_all(&self, space_id: &str) -> Result<Vec<RegistryEntry>, RegistryStoreError>;

    /// List all space IDs that have at least one active entry.
    fn load_spaces(&self) -> Result<Vec<String>, RegistryStoreError>;

    /// Upsert a registry entry (insert or update).
    ///
    /// `mirror` carries UX-only metadata (`title`, `content_type`,
    /// convert provenance, `size_bytes`) threaded through to backends
    /// that maintain a UX-facing `documents` mirror alongside the
    /// registry (RFD 0042 amendment 2026-05-24 + follow-up). These
    /// fields are NOT stored on [`RegistryEntry`]; they ride along to
    /// the backend so the mirror INSERT can populate the corresponding
    /// `documents.*` columns. Backends without a mirror (the OSS
    /// `SQLite` path) ignore them.
    ///
    /// On a re-register (e.g., a rename's persist), callers pass an
    /// empty [`MirrorMetadata`] — backends that mirror must use
    /// `COALESCE`-style update semantics so a later all-`None` write
    /// does not clobber earlier non-null values.
    fn save_entry(
        &self,
        space_id: &str,
        entry: &RegistryEntry,
        mirror: &MirrorMetadata<'_>,
    ) -> Result<(), RegistryStoreError>;

    /// Soft-delete an entry by setting its `deleted_at` timestamp.
    fn delete_entry(
        &self,
        space_id: &str,
        document_id: &str,
        deleted_at: i64,
    ) -> Result<(), RegistryStoreError>;

    /// Update the `edited_at` timestamp for a document, plus the
    /// optional `author_did` of the editor.
    ///
    /// RFD 0042 amendment 2026-05-24 (B-full follow-up): backends
    /// that maintain a `documents` mirror (`kutlhub-relay`'s PG path)
    /// also patch `documents_table.updated_at` + `updated_by` here,
    /// resolving `author_did` → `account_id` via the
    /// `account:<uuid>` short-form or the `custodied_keys` lookup
    /// for `did:key:…` identities. Backends without a mirror ignore
    /// `author_did`. Passing `None` for `author_did` updates only
    /// the registry's `edited_at` column and leaves the mirror's
    /// `updated_by` untouched (used by tests + flush paths that
    /// don't track per-edit attribution).
    fn update_edited_at(
        &self,
        space_id: &str,
        document_id: &str,
        edited_at: i64,
        author_did: Option<&str>,
    ) -> Result<(), RegistryStoreError>;

    /// Transfer ownership of every document in a space to a new
    /// account (RFD 0042 amendment 2026-05-24 — B-full follow-up).
    ///
    /// Used by the relay's `TransferSpaceOwnership` envelope handler.
    /// Updates `relay_registry.account_id` for every active document
    /// in the space AND mirrors the change to `documents_table.
    /// created_by` / `updated_by` so the UX-facing columns match.
    ///
    /// Returns the count of registry rows actually touched. The
    /// default implementation is a no-op (returns 0) so OSS backends
    /// without a mirror (`SQLite`) don't need to implement this.
    fn transfer_space_ownership(
        &self,
        _space_id: &str,
        _new_account_id: &str,
    ) -> Result<u32, RegistryStoreError> {
        Ok(0)
    }

    /// Load the durability witness for a document. Backends
    /// without witness support return the default witness
    /// (`persisted_version = 0`, `persisted_at = 0`). Read classification
    /// treats `persisted_version == 0` as "never successfully flushed."
    ///
    /// Default impl returns the zero witness — preserves backward
    /// compatibility for backends that haven't opted in. Implementations
    /// backed by durable SQL storage override this.
    fn load_witness(
        &self,
        _space_id: &str,
        _document_id: &str,
    ) -> Result<Witness, RegistryStoreError> {
        Ok(Witness::default())
    }

    /// Write a durability witness. Called by the flush task
    /// after `ContentBackend::save` returns `Ok`, before sending
    /// `FlushCompleted` to the relay actor.
    ///
    /// Upsert with `max` semantics on `persisted_version`: a replayed
    /// or out-of-order write cannot regress the field. Durably-projected
    /// SQL backends must preserve this invariant at the
    /// SQL layer (`MAX(excluded, existing)` / `GREATEST`).
    ///
    /// Default impl is a no-op — preserves backward compatibility for
    /// backends without witness support. Callers must tolerate this:
    /// if the backend doesn't persist the witness, `persisted_version`
    /// on reload stays at 0 and subsequent `Ok(None)` loads classify
    /// as `Empty`, matching the ephemeral-relay model.
    fn update_witness(
        &self,
        _space_id: &str,
        _document_id: &str,
        _witness: &Witness,
    ) -> Result<(), RegistryStoreError> {
        Ok(())
    }
}

/// SQLite-backed registry storage for self-hosted relays.
///
/// Stores all spaces in a single `registry.db` file using WAL mode
/// for concurrent read safety.
pub struct SqliteBackend {
    pool: sqlx::sqlite::SqlitePool,
}

impl SqliteBackend {
    /// Create a backend from an already-open pool.
    ///
    /// Ensures the `registry` table exists. Use this when sharing a pool with
    /// other backends (e.g. [`crate::space_backend::SqliteSpaceBackend`] and
    /// [`crate::sqlite_invite_backend::SqliteInviteBackend`]) to avoid lock
    /// contention from multiple pools on the same `SQLite` file.
    pub async fn new(pool: sqlx::sqlite::SqlitePool) -> Result<Self, RegistryStoreError> {
        // Create schema.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS registry (
                space_id              TEXT    NOT NULL,
                document_id           TEXT    NOT NULL,
                path                  TEXT    NOT NULL,
                created_by            TEXT    NOT NULL,
                created_at            INTEGER NOT NULL,
                renamed_by            TEXT,
                renamed_at            INTEGER,
                deleted_at            INTEGER,
                edited_at             INTEGER,
                originally_created_at INTEGER,
                PRIMARY KEY (space_id, document_id)
            )",
        )
        .execute(&pool)
        .await?;

        // Migration: add edited_at to existing databases.
        sqlx::query("ALTER TABLE registry ADD COLUMN edited_at INTEGER")
            .execute(&pool)
            .await
            .ok(); // Ignore "duplicate column" error on re-run.

        // Migration: add originally_created_at to existing databases.
        sqlx::query("ALTER TABLE registry ADD COLUMN originally_created_at INTEGER")
            .execute(&pool)
            .await
            .ok(); // Ignore "duplicate column" error on re-run.

        // document_witnesses stores the durable projection of
        // `ContentState.persisted_version` per document. Sibling table
        // (rather than columns on `registry`) because the witness is
        // relay-internal durability metadata with `MAX`-upsert semantics
        // distinct from registry's LWW-by-timestamp model.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS document_witnesses (
                space_id          TEXT    NOT NULL,
                document_id       TEXT    NOT NULL,
                persisted_version INTEGER NOT NULL,
                persisted_at      INTEGER NOT NULL,
                PRIMARY KEY (space_id, document_id)
            )",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    /// Open (or create) a `SQLite` registry database at `data_dir/registry.db`.
    ///
    /// Creates the schema if it doesn't exist and enables WAL mode.
    pub async fn open(data_dir: &Path) -> Result<Self, RegistryStoreError> {
        let pool = crate::open_sqlite_pool(data_dir)
            .await
            .map_err(|e| sqlx::Error::Io(std::io::Error::other(e.to_string())))?;
        Self::new(pool).await
    }

    /// Close the pool, flushing pending writes.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

impl RegistryBackend for SqliteBackend {
    fn load_all(&self, space_id: &str) -> Result<Vec<RegistryEntry>, RegistryStoreError> {
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        // Use block_in_place since the relay actor calls this synchronously.
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let rows = sqlx::query_as::<_, RegistryRow>(
                    "SELECT document_id, path, created_by, created_at,
                            renamed_by, renamed_at, deleted_at, edited_at,
                            originally_created_at
                     FROM registry
                     WHERE space_id = ? AND deleted_at IS NULL",
                )
                .bind(&space_id)
                .fetch_all(&pool)
                .await?;

                Ok(rows.into_iter().map(RegistryRow::into_entry).collect())
            })
        })
    }

    fn load_spaces(&self) -> Result<Vec<String>, RegistryStoreError> {
        let pool = self.pool.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let rows: Vec<(String,)> = sqlx::query_as(
                    "SELECT DISTINCT space_id FROM registry WHERE deleted_at IS NULL",
                )
                .fetch_all(&pool)
                .await?;

                Ok(rows.into_iter().map(|(s,)| s).collect())
            })
        })
    }

    fn save_entry(
        &self,
        space_id: &str,
        entry: &RegistryEntry,
        // RFD 0042 amendment 2026-05-24: `MirrorMetadata` is mirror-only
        // — the OSS SQLite path has no `documents` table to mirror to,
        // so the bundle is silently dropped here.
        _mirror: &MirrorMetadata<'_>,
    ) -> Result<(), RegistryStoreError> {
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        let entry = entry.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(
                    "INSERT INTO registry
                        (space_id, document_id, path, created_by, created_at,
                         renamed_by, renamed_at, deleted_at, originally_created_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (space_id, document_id) DO UPDATE SET
                        path = excluded.path,
                        renamed_by = excluded.renamed_by,
                        renamed_at = excluded.renamed_at,
                        deleted_at = excluded.deleted_at",
                )
                .bind(&space_id)
                .bind(&entry.document_id)
                .bind(&entry.path)
                .bind(&entry.created_by)
                .bind(entry.created_at)
                .bind(&entry.renamed_by)
                .bind(entry.renamed_at)
                .bind(entry.deleted_at)
                .bind(entry.originally_created_at)
                .execute(&pool)
                .await?;

                Ok(())
            })
        })
    }

    fn delete_entry(
        &self,
        space_id: &str,
        document_id: &str,
        deleted_at: i64,
    ) -> Result<(), RegistryStoreError> {
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        let document_id = document_id.to_string();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(
                    "UPDATE registry SET deleted_at = ?
                     WHERE space_id = ? AND document_id = ?",
                )
                .bind(deleted_at)
                .bind(&space_id)
                .bind(&document_id)
                .execute(&pool)
                .await?;

                Ok(())
            })
        })
    }

    fn update_edited_at(
        &self,
        space_id: &str,
        document_id: &str,
        edited_at: i64,
        _author_did: Option<&str>,
    ) -> Result<(), RegistryStoreError> {
        // SQLite backend has no `documents` mirror — author_did is
        // ignored. Only `registry.edited_at` is patched.
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        let document_id = document_id.to_string();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(
                    "UPDATE registry SET edited_at = ? WHERE space_id = ? AND document_id = ?",
                )
                .bind(edited_at)
                .bind(&space_id)
                .bind(&document_id)
                .execute(&pool)
                .await?;
                Ok(())
            })
        })
    }

    fn load_witness(
        &self,
        space_id: &str,
        document_id: &str,
    ) -> Result<Witness, RegistryStoreError> {
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        let document_id = document_id.to_string();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let row: Option<(i64, i64)> = sqlx::query_as(
                    "SELECT persisted_version, persisted_at
                     FROM document_witnesses
                     WHERE space_id = ? AND document_id = ?",
                )
                .bind(&space_id)
                .bind(&document_id)
                .fetch_optional(&pool)
                .await?;

                Ok(row.map_or_else(Witness::default, |(v, t)| Witness {
                    #[expect(
                        clippy::cast_sign_loss,
                        reason = "sqlite has no unsigned type; persisted_version is always >= 0"
                    )]
                    persisted_version: v as u64,
                    persisted_at: t,
                }))
            })
        })
    }

    fn update_witness(
        &self,
        space_id: &str,
        document_id: &str,
        witness: &Witness,
    ) -> Result<(), RegistryStoreError> {
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        let document_id = document_id.to_string();
        let witness = *witness;
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // MAX-upsert: a replayed or out-of-order write cannot
                // regress `persisted_version`. `persisted_at` follows
                // the winning `persisted_version` (observability hint,
                // not algebraic).
                sqlx::query(
                    "INSERT INTO document_witnesses
                        (space_id, document_id, persisted_version, persisted_at)
                     VALUES (?, ?, ?, ?)
                     ON CONFLICT (space_id, document_id) DO UPDATE SET
                        persisted_at = CASE
                            WHEN excluded.persisted_version > document_witnesses.persisted_version
                            THEN excluded.persisted_at
                            ELSE document_witnesses.persisted_at
                        END,
                        persisted_version = MAX(
                            excluded.persisted_version,
                            document_witnesses.persisted_version
                        )",
                )
                .bind(&space_id)
                .bind(&document_id)
                .bind(i64::try_from(witness.persisted_version).unwrap_or(i64::MAX))
                .bind(witness.persisted_at)
                .execute(&pool)
                .await?;
                Ok(())
            })
        })
    }
}

/// Row type for `SQLite` registry queries.
///
/// Shared by [`SqliteBackend`] and
/// [`crate::change_sqlite::SqliteChangeBackend`] to avoid duplicating the
/// `FromRow` derive and conversion logic.
#[derive(sqlx::FromRow)]
pub(crate) struct RegistryRow {
    pub(crate) document_id: String,
    pub(crate) path: String,
    pub(crate) created_by: String,
    pub(crate) created_at: i64,
    pub(crate) renamed_by: Option<String>,
    pub(crate) renamed_at: Option<i64>,
    pub(crate) deleted_at: Option<i64>,
    pub(crate) edited_at: Option<i64>,
    pub(crate) originally_created_at: Option<i64>,
}

impl RegistryRow {
    pub(crate) fn into_entry(self) -> RegistryEntry {
        // The SQLite-backed registry doesn't persist the 5 source-
        // provenance fields — there is no in-relay ingestion pipeline,
        // so source_kind defaults to native (0) and the other four
        // are NULL on reload.
        RegistryEntry {
            document_id: self.document_id,
            path: self.path,
            created_by: self.created_by,
            created_at: self.created_at,
            renamed_by: self.renamed_by,
            renamed_at: self.renamed_at,
            deleted_at: self.deleted_at,
            account_id: None,
            edited_at: self.edited_at,
            originally_created_at: self.originally_created_at,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sqlite_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();

        // Initially empty.
        let spaces = backend.load_spaces().unwrap();
        assert!(spaces.is_empty());

        // Save entries for two spaces.
        let entry1 = RegistryEntry {
            document_id: "doc-1".to_string(),
            path: "notes/a.md".to_string(),
            created_by: "did:alice".to_string(),
            created_at: 1000,
            ..Default::default()
        };
        let entry2 = RegistryEntry {
            document_id: "doc-2".to_string(),
            path: "notes/b.md".to_string(),
            created_by: "did:bob".to_string(),
            created_at: 2000,
            ..Default::default()
        };
        backend
            .save_entry("space-a", &entry1, &MirrorMetadata::default())
            .unwrap();
        backend
            .save_entry("space-b", &entry2, &MirrorMetadata::default())
            .unwrap();

        let spaces = backend.load_spaces().unwrap();
        assert_eq!(spaces.len(), 2);

        let entries = backend.load_all("space-a").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].document_id, "doc-1");
        assert_eq!(entries[0].path, "notes/a.md");

        // Update (rename).
        let renamed = RegistryEntry {
            path: "archive/a.md".to_string(),
            renamed_by: Some("did:carol".to_string()),
            renamed_at: Some(3000),
            ..entry1.clone()
        };
        backend
            .save_entry("space-a", &renamed, &MirrorMetadata::default())
            .unwrap();
        let entries = backend.load_all("space-a").unwrap();
        assert_eq!(entries[0].path, "archive/a.md");
        assert_eq!(entries[0].renamed_by.as_deref(), Some("did:carol"));

        // Soft-delete.
        backend.delete_entry("space-a", "doc-1", 4000).unwrap();
        let entries = backend.load_all("space-a").unwrap();
        assert!(entries.is_empty());

        // Space no longer appears.
        let spaces = backend.load_spaces().unwrap();
        assert_eq!(spaces.len(), 1);
        assert_eq!(spaces[0], "space-b");

        backend.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sqlite_upsert_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();

        let entry = RegistryEntry {
            document_id: "doc-1".to_string(),
            path: "a.md".to_string(),
            created_by: "did:alice".to_string(),
            created_at: 1000,
            ..Default::default()
        };

        // Insert twice — should not error.
        backend
            .save_entry("space-a", &entry, &MirrorMetadata::default())
            .unwrap();
        backend
            .save_entry("space-a", &entry, &MirrorMetadata::default())
            .unwrap();

        let entries = backend.load_all("space-a").unwrap();
        assert_eq!(entries.len(), 1);

        backend.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_witness_default_when_unset() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();
        let w = backend.load_witness("space-a", "doc-x").unwrap();
        assert_eq!(w, Witness::default());
        assert_eq!(w.persisted_version, 0);
        backend.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_witness_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();
        let w = Witness {
            persisted_version: 42,
            persisted_at: 1_700_000_000_000,
        };
        backend.update_witness("space-a", "doc-1", &w).unwrap();
        let loaded = backend.load_witness("space-a", "doc-1").unwrap();
        assert_eq!(loaded, w);
        backend.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_witness_upsert_max_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();

        // Write v=10, then v=5 (out-of-order older) — v=10 wins.
        backend
            .update_witness(
                "s",
                "d",
                &Witness {
                    persisted_version: 10,
                    persisted_at: 1000,
                },
            )
            .unwrap();
        backend
            .update_witness(
                "s",
                "d",
                &Witness {
                    persisted_version: 5,
                    persisted_at: 2000,
                },
            )
            .unwrap();
        let w = backend.load_witness("s", "d").unwrap();
        assert_eq!(w.persisted_version, 10);
        // persisted_at follows the winning version, not the later write.
        assert_eq!(w.persisted_at, 1000);

        // Idempotent replay — writing the same witness is a no-op.
        backend
            .update_witness(
                "s",
                "d",
                &Witness {
                    persisted_version: 10,
                    persisted_at: 1000,
                },
            )
            .unwrap();
        let w = backend.load_witness("s", "d").unwrap();
        assert_eq!(w.persisted_version, 10);

        // Forward progress — v=15 wins, persisted_at moves.
        backend
            .update_witness(
                "s",
                "d",
                &Witness {
                    persisted_version: 15,
                    persisted_at: 3000,
                },
            )
            .unwrap();
        let w = backend.load_witness("s", "d").unwrap();
        assert_eq!(w.persisted_version, 15);
        assert_eq!(w.persisted_at, 3000);

        backend.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sqlite_update_edited_at() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();

        let entry = RegistryEntry {
            document_id: "doc-1".into(),
            path: "workspace.md".into(),
            created_by: "did:demo:alice".into(),
            created_at: 1000,
            ..Default::default()
        };
        backend
            .save_entry("space-1", &entry, &MirrorMetadata::default())
            .unwrap();

        backend
            .update_edited_at("space-1", "doc-1", 2000, None)
            .unwrap();

        let entries = backend.load_all("space-1").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].edited_at, Some(2000));

        // Second update overwrites.
        backend
            .update_edited_at("space-1", "doc-1", 3000, None)
            .unwrap();
        let entries = backend.load_all("space-1").unwrap();
        assert_eq!(entries[0].edited_at, Some(3000));

        backend.close().await;
    }
}
