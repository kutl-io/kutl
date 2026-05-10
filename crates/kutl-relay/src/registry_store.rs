//! Persistence backends for the document registry.
//!
//! The `RegistryBackend` trait abstracts storage so the relay can use `SQLite`
//! for self-hosted deployments and Postgres when running behind kutlhub.com.

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
/// combination signals `Inconsistent` (RFD 0066).
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
    fn save_entry(&self, space_id: &str, entry: &RegistryEntry) -> Result<(), RegistryStoreError>;

    /// Soft-delete an entry by setting its `deleted_at` timestamp.
    fn delete_entry(
        &self,
        space_id: &str,
        document_id: &str,
        deleted_at: i64,
    ) -> Result<(), RegistryStoreError>;

    /// Update the `edited_at` timestamp for a document.
    fn update_edited_at(
        &self,
        space_id: &str,
        document_id: &str,
        edited_at: i64,
    ) -> Result<(), RegistryStoreError>;

    /// Load the durability witness for a document (RFD 0066). Backends
    /// without witness support return the default witness
    /// (`persisted_version = 0`, `persisted_at = 0`). Read classification
    /// treats `persisted_version == 0` as "never successfully flushed."
    ///
    /// Default impl returns the zero witness — preserves backward
    /// compatibility for backends that haven't opted in. Implementations
    /// backed by durable storage (`SQLite`, Postgres) override this.
    fn load_witness(
        &self,
        _space_id: &str,
        _document_id: &str,
    ) -> Result<Witness, RegistryStoreError> {
        Ok(Witness::default())
    }

    /// Write a durability witness (RFD 0066). Called by the flush task
    /// after `ContentBackend::save` returns `Ok`, before sending
    /// `FlushCompleted` to the relay actor.
    ///
    /// Upsert with `max` semantics on `persisted_version`: a replayed
    /// or out-of-order write cannot regress the field. Durably-projected
    /// backends (`SQLite`, Postgres) must preserve this invariant at the
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
                space_id    TEXT    NOT NULL,
                document_id TEXT    NOT NULL,
                path        TEXT    NOT NULL,
                created_by  TEXT    NOT NULL,
                created_at  INTEGER NOT NULL,
                renamed_by  TEXT,
                renamed_at  INTEGER,
                deleted_at  INTEGER,
                edited_at   INTEGER,
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

        // RFD 0066: document_witnesses stores the durable projection of
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
                            renamed_by, renamed_at, deleted_at, edited_at
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

    fn save_entry(&self, space_id: &str, entry: &RegistryEntry) -> Result<(), RegistryStoreError> {
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        let entry = entry.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(
                    "INSERT INTO registry
                        (space_id, document_id, path, created_by, created_at,
                         renamed_by, renamed_at, deleted_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
    ) -> Result<(), RegistryStoreError> {
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
}

impl RegistryRow {
    pub(crate) fn into_entry(self) -> RegistryEntry {
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
            renamed_by: None,
            renamed_at: None,
            deleted_at: None,
            account_id: None,
            edited_at: None,
        };
        let entry2 = RegistryEntry {
            document_id: "doc-2".to_string(),
            path: "notes/b.md".to_string(),
            created_by: "did:bob".to_string(),
            created_at: 2000,
            renamed_by: None,
            renamed_at: None,
            deleted_at: None,
            account_id: None,
            edited_at: None,
        };
        backend.save_entry("space-a", &entry1).unwrap();
        backend.save_entry("space-b", &entry2).unwrap();

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
        backend.save_entry("space-a", &renamed).unwrap();
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
            renamed_by: None,
            renamed_at: None,
            deleted_at: None,
            account_id: None,
            edited_at: None,
        };

        // Insert twice — should not error.
        backend.save_entry("space-a", &entry).unwrap();
        backend.save_entry("space-a", &entry).unwrap();

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
            renamed_by: None,
            renamed_at: None,
            deleted_at: None,
            account_id: None,
            edited_at: None,
        };
        backend.save_entry("space-1", &entry).unwrap();

        backend.update_edited_at("space-1", "doc-1", 2000).unwrap();

        let entries = backend.load_all("space-1").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].edited_at, Some(2000));

        // Second update overwrites.
        backend.update_edited_at("space-1", "doc-1", 3000).unwrap();
        let entries = backend.load_all("space-1").unwrap();
        assert_eq!(entries[0].edited_at, Some(3000));

        backend.close().await;
    }
}
