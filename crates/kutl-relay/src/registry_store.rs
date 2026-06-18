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
    /// in-memory registry mutation (see `persist_entry` in relay/lifecycle.rs).
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

    /// Load all entries for a space INCLUDING soft-deleted (tombstoned) ones.
    ///
    /// Used only by registry rebuild on relay startup, so a deleted document's
    /// tombstone (`deleted_hlc`) survives a restart and a later lower-HLC
    /// re-register cannot resurrect it. Active-set callers use [`load_all`](Self::load_all).
    fn load_all_including_deleted(
        &self,
        space_id: &str,
    ) -> Result<Vec<RegistryEntry>, RegistryStoreError>;

    /// List all space IDs that have at least one active entry.
    fn load_spaces(&self) -> Result<Vec<String>, RegistryStoreError>;

    /// List all space IDs that have at least one entry, including spaces whose
    /// only remaining entries are tombstones — so such a space still rehydrates
    /// its tombstones on restart. Rebuild-only companion to [`load_spaces`](Self::load_spaces).
    fn load_spaces_including_deleted(&self) -> Result<Vec<String>, RegistryStoreError>;

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

    /// Persist several entries as ONE atomic unit (all commit or none).
    ///
    /// Used by path-collision conflict-copy, which moves the displaced loser(s)
    /// off the contested path and the operated document onto it. Items are
    /// applied in order, so the caller passes losers first and the operated
    /// document last (the same ordering [`save_entry`] callers relied on, so the
    /// mirror's `UNIQUE(lower(path))` is never momentarily violated). On any
    /// error the whole batch rolls back, so a mid-batch failure can never leave
    /// a committed prefix that diverges from the rolled-back in-memory registry.
    ///
    /// The default implementation loops [`save_entry`](Self::save_entry) and is
    /// therefore NOT atomic — real backends override it with a single
    /// transaction; the default keeps test doubles compiling.
    fn save_batch(
        &self,
        space_id: &str,
        items: &[(&RegistryEntry, &MirrorMetadata<'_>)],
    ) -> Result<(), RegistryStoreError> {
        for (entry, mirror) in items {
            self.save_entry(space_id, entry, mirror)?;
        }
        Ok(())
    }

    /// Soft-delete an entry by setting its `deleted_at` timestamp.
    fn delete_entry(
        &self,
        space_id: &str,
        document_id: &str,
        deleted_at: i64,
    ) -> Result<(), RegistryStoreError>;

    /// Soft-delete several entries as ONE atomic unit (all commit or none).
    ///
    /// Used by bulk space deletion ([`DocumentRegistry::unregister_many`]'s
    /// persist), which applies the whole batch in memory before persisting; the
    /// single transaction means a mid-batch failure can never leave a committed
    /// prefix that diverges from the rolled-back in-memory registry.
    ///
    /// `deletes` carries `(document_id, deleted_at)` pairs. The default loops
    /// [`delete_entry`](Self::delete_entry) and is therefore NOT atomic — real
    /// backends override it with a single transaction; the default keeps test
    /// doubles compiling.
    fn delete_batch(
        &self,
        space_id: &str,
        deletes: &[(&str, i64)],
    ) -> Result<(), RegistryStoreError> {
        for (document_id, deleted_at) in deletes {
            self.delete_entry(space_id, document_id, *deleted_at)?;
        }
        Ok(())
    }

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
                rename_causal_floor_at INTEGER,
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

        // Migration: add rename_causal_floor_at — the register HLC a rename
        // observed (millis), so a rename that beat a clock-skewed registration
        // via its causal floor still supersedes after a relay restart.
        sqlx::query("ALTER TABLE registry ADD COLUMN rename_causal_floor_at INTEGER")
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

/// SELECT active (non-deleted) entries for a space.
const SQLITE_LOAD_ALL: &str = "SELECT document_id, path, created_by, created_at,
            renamed_by, renamed_at, deleted_at, edited_at, originally_created_at,
            rename_causal_floor_at
     FROM registry
     WHERE space_id = ? AND deleted_at IS NULL";

/// As [`SQLITE_LOAD_ALL`] but INCLUDING tombstones (registry rebuild only).
const SQLITE_LOAD_ALL_INCLUDING_DELETED: &str = "SELECT document_id, path, created_by, created_at,
            renamed_by, renamed_at, deleted_at, edited_at, originally_created_at,
            rename_causal_floor_at
     FROM registry
     WHERE space_id = ?";

/// SELECT space IDs with at least one active entry.
const SQLITE_LOAD_SPACES: &str = "SELECT DISTINCT space_id FROM registry WHERE deleted_at IS NULL";

/// As [`SQLITE_LOAD_SPACES`] but counting tombstone-only spaces (rebuild only).
const SQLITE_LOAD_SPACES_INCLUDING_DELETED: &str = "SELECT DISTINCT space_id FROM registry";

/// Soft-delete one registry row. Shared by `delete_entry` (single autocommit)
/// and `delete_batch` (inside one transaction).
const SQLITE_SOFT_DELETE: &str =
    "UPDATE registry SET deleted_at = ? WHERE space_id = ? AND document_id = ?";

/// Upsert one registry row. Shared by `save_entry` (single autocommit) and
/// `save_batch` (inside one transaction).
const SQLITE_SAVE_ENTRY: &str = "INSERT INTO registry
        (space_id, document_id, path, created_by, created_at,
         renamed_by, renamed_at, deleted_at, originally_created_at,
         rename_causal_floor_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT (space_id, document_id) DO UPDATE SET
        path = excluded.path,
        renamed_by = excluded.renamed_by,
        renamed_at = excluded.renamed_at,
        deleted_at = excluded.deleted_at,
        rename_causal_floor_at = excluded.rename_causal_floor_at";

/// Run the registry upsert against any executor (a pool for a single write, or a
/// `&mut` transaction for a batched write). Keeps `save_entry` and `save_batch`
/// on one bind sequence.
async fn sqlite_upsert_registry_row<'e, E>(
    executor: E,
    space_id: &str,
    entry: &RegistryEntry,
) -> Result<(), RegistryStoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query(SQLITE_SAVE_ENTRY)
        .bind(space_id)
        .bind(&entry.document_id)
        .bind(&entry.path)
        .bind(&entry.created_by)
        .bind(entry.created_at)
        .bind(&entry.renamed_by)
        .bind(entry.renamed_at)
        .bind(entry.deleted_at)
        .bind(entry.originally_created_at)
        .bind(entry.rename_causal_floor_at)
        .execute(executor)
        .await?;
    Ok(())
}

impl SqliteBackend {
    /// Shared entry reader; `sql` selects whether tombstones are included.
    fn fetch_entries(
        &self,
        space_id: &str,
        sql: &'static str,
    ) -> Result<Vec<RegistryEntry>, RegistryStoreError> {
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        // Use block_in_place since the relay actor calls this synchronously.
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let rows = sqlx::query_as::<_, RegistryRow>(sql)
                    .bind(&space_id)
                    .fetch_all(&pool)
                    .await?;
                Ok(rows.into_iter().map(RegistryRow::into_entry).collect())
            })
        })
    }

    /// Shared space-list reader; `sql` selects whether tombstone-only spaces count.
    fn fetch_spaces(&self, sql: &'static str) -> Result<Vec<String>, RegistryStoreError> {
        let pool = self.pool.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let rows: Vec<(String,)> = sqlx::query_as(sql).fetch_all(&pool).await?;
                Ok(rows.into_iter().map(|(s,)| s).collect())
            })
        })
    }
}

impl RegistryBackend for SqliteBackend {
    fn load_all(&self, space_id: &str) -> Result<Vec<RegistryEntry>, RegistryStoreError> {
        self.fetch_entries(space_id, SQLITE_LOAD_ALL)
    }

    fn load_all_including_deleted(
        &self,
        space_id: &str,
    ) -> Result<Vec<RegistryEntry>, RegistryStoreError> {
        self.fetch_entries(space_id, SQLITE_LOAD_ALL_INCLUDING_DELETED)
    }

    fn load_spaces(&self) -> Result<Vec<String>, RegistryStoreError> {
        self.fetch_spaces(SQLITE_LOAD_SPACES)
    }

    fn load_spaces_including_deleted(&self) -> Result<Vec<String>, RegistryStoreError> {
        self.fetch_spaces(SQLITE_LOAD_SPACES_INCLUDING_DELETED)
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
            tokio::runtime::Handle::current()
                .block_on(async { sqlite_upsert_registry_row(&pool, &space_id, &entry).await })
        })
    }

    fn save_batch(
        &self,
        space_id: &str,
        items: &[(&RegistryEntry, &MirrorMetadata<'_>)],
    ) -> Result<(), RegistryStoreError> {
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        // Clone entries so the owned set can move into the async block (mirror is
        // dropped — the OSS path has no `documents` table).
        let entries: Vec<RegistryEntry> = items.iter().map(|(e, _)| (*e).clone()).collect();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut tx = pool.begin().await?;
                for entry in &entries {
                    sqlite_upsert_registry_row(&mut *tx, &space_id, entry).await?;
                }
                tx.commit().await?;
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
                sqlx::query(SQLITE_SOFT_DELETE)
                    .bind(deleted_at)
                    .bind(&space_id)
                    .bind(&document_id)
                    .execute(&pool)
                    .await?;

                Ok(())
            })
        })
    }

    fn delete_batch(
        &self,
        space_id: &str,
        deletes: &[(&str, i64)],
    ) -> Result<(), RegistryStoreError> {
        let pool = self.pool.clone();
        let space_id = space_id.to_string();
        // Own the pairs so they can move into the async block.
        let deletes: Vec<(String, i64)> = deletes
            .iter()
            .map(|(d, t)| ((*d).to_string(), *t))
            .collect();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut tx = pool.begin().await?;
                for (document_id, deleted_at) in &deletes {
                    sqlx::query(SQLITE_SOFT_DELETE)
                        .bind(deleted_at)
                        .bind(&space_id)
                        .bind(document_id)
                        .execute(&mut *tx)
                        .await?;
                }
                tx.commit().await?;
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
    pub(crate) rename_causal_floor_at: Option<i64>,
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
            rename_causal_floor_at: self.rename_causal_floor_at,
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

    /// The `rename_causal_floor_at` column round-trips: a rename that beat a
    /// clock-skewed register via its causal floor persists the floor (millis), so
    /// after a relay restart the reload reconstructs it and the rename still
    /// supersedes the future-stamped register. Without persistence, a new op after
    /// restart could re-arbitrate the path back to the register.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sqlite_rename_causal_floor_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();
        // Skewed register at 5000ms; an un-skewed rename at 10ms that observed it
        // (floor = 5000). The renamer's own stamp is below the register.
        let entry = RegistryEntry {
            document_id: "doc-1".to_string(),
            path: "archive/a.md".to_string(),
            created_by: "did:skewed".to_string(),
            created_at: 5000,
            renamed_by: Some("did:unskewed".to_string()),
            renamed_at: Some(10),
            rename_causal_floor_at: Some(5000),
            ..Default::default()
        };
        backend
            .save_entry("space-a", &entry, &MirrorMetadata::default())
            .unwrap();
        backend.close().await;

        // Reopen (simulating a relay restart) and confirm the floor survived.
        let reopened = SqliteBackend::open(dir.path()).await.unwrap();
        let entries = reopened.load_all("space-a").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].rename_causal_floor_at,
            Some(5000),
            "causal floor must survive the restart so the rename keeps superseding"
        );
        reopened.close().await;
    }

    /// A soft-deleted document stays absent from the active set after the store
    /// is reopened against the same `data_dir` — i.e. a relay restart does NOT
    /// resurrect a deleted doc from the persisted registry. This is the
    /// store-level half of catalog §5.1 (tombstones survive a restart): fast,
    /// container-free, and exercising the real durable reload path the ephemeral
    /// OSS fixture is exempt from.
    ///
    /// The tombstone FACT survives rebuild too: `load_from_backend` reads via
    /// `load_all_including_deleted` / `load_spaces_including_deleted`, so a
    /// deleted document's `deleted_hlc` is reconstructed across a restart and a
    /// later lower-HLC re-register cannot resurrect it (the asserted reopen
    /// below, plus `test_reloaded_tombstone_blocks_stale_reregister`). Earlier
    /// this was a parking-lot gap because rebuild read `load_all`, which filters
    /// tombstones; the rebuild-only inclusive reader closed it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sqlite_tombstone_not_resurrected_across_reopen() {
        let dir = tempfile::tempdir().unwrap();

        // Register a doc, then soft-delete it, then drop the pool (simulating the
        // relay process going away).
        {
            let backend = SqliteBackend::open(dir.path()).await.unwrap();
            let entry = RegistryEntry {
                document_id: "doc-1".to_string(),
                path: "notes/a.md".to_string(),
                created_by: "did:alice".to_string(),
                created_at: 1000,
                ..Default::default()
            };
            backend
                .save_entry("space-a", &entry, &MirrorMetadata::default())
                .unwrap();
            backend.delete_entry("space-a", "doc-1", 4000).unwrap();
            assert!(
                backend.load_all("space-a").unwrap().is_empty(),
                "deleted doc is absent from the active set before reopen"
            );
            backend.close().await;
        }

        // Reopen against the same data_dir — the "restart". The deleted doc must
        // stay gone; it must not reappear in the active set.
        let reopened = SqliteBackend::open(dir.path()).await.unwrap();
        assert!(
            reopened.load_all("space-a").unwrap().is_empty(),
            "a soft-deleted doc must NOT resurrect in the active set after a restart"
        );
        assert!(
            reopened.load_spaces().unwrap().is_empty(),
            "a space whose only doc was deleted exposes no active docs after restart"
        );

        // ...but the rebuild reader recovers the tombstone FACT across the
        // restart, so `deleted_hlc` is not lost (the #4 fix).
        let reloaded = reopened.load_all_including_deleted("space-a").unwrap();
        assert_eq!(
            reloaded.len(),
            1,
            "rebuild reader returns the tombstone after reopen"
        );
        assert_eq!(
            reloaded[0].deleted_at,
            Some(4000),
            "deleted_at survives the restart"
        );
        assert_eq!(
            reopened.load_spaces_including_deleted().unwrap(),
            vec!["space-a".to_string()],
            "the tombstone-only space still rehydrates after restart"
        );

        // The tombstone ROW is durable (the data survived) — proven by re-saving
        // the same id: the ON CONFLICT upsert finds the existing row, and the
        // doc remains deleted because the row's deleted_at is intact. (If the
        // row had been physically dropped, this would create a fresh ALIVE row.)
        let revived_attempt = RegistryEntry {
            document_id: "doc-1".to_string(),
            path: "notes/a.md".to_string(),
            created_by: "did:alice".to_string(),
            created_at: 1000,
            deleted_at: Some(4000),
            ..Default::default()
        };
        reopened
            .save_entry("space-a", &revived_attempt, &MirrorMetadata::default())
            .unwrap();
        assert!(
            reopened.load_all("space-a").unwrap().is_empty(),
            "the persisted tombstone row keeps the doc deleted across reopen"
        );

        reopened.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sqlite_load_all_including_deleted_returns_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();
        let entry = RegistryEntry {
            document_id: "doc-1".to_string(),
            path: "notes/a.md".to_string(),
            created_by: "did:alice".to_string(),
            created_at: 1000,
            ..Default::default()
        };
        backend
            .save_entry("space-a", &entry, &MirrorMetadata::default())
            .unwrap();
        backend.delete_entry("space-a", "doc-1", 4000).unwrap();

        // Active reader still hides it (contract unchanged).
        assert!(backend.load_all("space-a").unwrap().is_empty());
        assert!(backend.load_spaces().unwrap().is_empty());

        // Rebuild reader returns the tombstone WITH its deleted_at preserved.
        let all = backend.load_all_including_deleted("space-a").unwrap();
        assert_eq!(all.len(), 1, "rebuild reader returns the tombstoned row");
        assert_eq!(
            all[0].deleted_at,
            Some(4000),
            "deleted_at survives the reload"
        );

        // A space whose only doc is deleted still rehydrates.
        assert_eq!(
            backend.load_spaces_including_deleted().unwrap(),
            vec!["space-a".to_string()]
        );
        backend.close().await;
    }

    /// The #4 regression: a tombstone reloaded across a restart (via the
    /// tombstone-inclusive reader → `from_entries`) must block a stale, lower-HLC
    /// re-register from resurrecting the document — while a genuinely newer op
    /// still revives it. Composes the exact rebuild path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_reloaded_tombstone_blocks_stale_reregister() {
        use crate::registry::{DocumentRegistry, EntryMetadata};

        let id = "00000000-0000-0000-0000-000000000001";
        let dir = tempfile::tempdir().unwrap();
        {
            let backend = SqliteBackend::open(dir.path()).await.unwrap();
            let entry = RegistryEntry {
                document_id: id.to_string(),
                path: "notes/a.md".to_string(),
                created_by: "did:alice".to_string(),
                created_at: 1000,
                ..Default::default()
            };
            backend
                .save_entry("space-a", &entry, &MirrorMetadata::default())
                .unwrap();
            backend.delete_entry("space-a", id, 4000).unwrap();
            backend.close().await;
        }

        // Restart rebuild: tombstone-inclusive read → from_entries.
        let reopened = SqliteBackend::open(dir.path()).await.unwrap();
        let entries = reopened.load_all_including_deleted("space-a").unwrap();
        let mut reg = DocumentRegistry::from_entries(entries);

        assert!(reg.get(id).is_none(), "reloaded doc is dead");
        assert!(
            reg.get_any(id).is_some_and(|e| e.deleted_at == Some(4000)),
            "the tombstone fact survived the reload"
        );

        // Stale re-register (ts 2000 < delete ts 4000) must NOT resurrect.
        let stale = EntryMetadata {
            author_did: "did:alice".to_string(),
            timestamp: 2000,
            ..Default::default()
        };
        let _ = reg.register(id, "notes/a.md", stale);
        assert!(
            reg.get(id).is_none(),
            "a re-register older than the tombstone must not resurrect the doc"
        );

        // A genuinely newer op (ts 5000 > 4000) DOES revive — correct both ways.
        let fresh = EntryMetadata {
            author_did: "did:alice".to_string(),
            timestamp: 5000,
            ..Default::default()
        };
        let _ = reg.register(id, "notes/a.md", fresh);
        assert!(
            reg.get(id).is_some(),
            "a re-register newer than the tombstone revives the doc"
        );
        reopened.close().await;
    }

    fn entry_at(doc_id: &str, path: &str) -> RegistryEntry {
        RegistryEntry {
            document_id: doc_id.to_string(),
            path: path.to_string(),
            created_by: "did:alice".to_string(),
            created_at: 1000,
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sqlite_save_batch_commits_all() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();
        let m = MirrorMetadata::default();
        let a = entry_at("doc-a", "a.md");
        let b = entry_at("doc-b", "b.md");
        backend
            .save_batch("space-a", &[(&a, &m), (&b, &m)])
            .unwrap();
        let mut paths: Vec<String> = backend
            .load_all("space-a")
            .unwrap()
            .into_iter()
            .map(|e| e.path)
            .collect();
        paths.sort();
        assert_eq!(paths, vec!["a.md".to_string(), "b.md".to_string()]);
        backend.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sqlite_save_batch_rolls_back_on_midbatch_failure() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();
        let m = MirrorMetadata::default();

        // Test-only unique path index to mimic the PG mirror's
        // `uq_documents_space_path_active` so a mid-batch winner write can be
        // *rejected* (SQLite's production `registry` table has only a PK).
        sqlx::query("CREATE UNIQUE INDEX test_uq_path ON registry(space_id, path)")
            .execute(&backend.pool)
            .await
            .unwrap();

        // An out-of-band live doc already holds the winner's path P.
        let squatter = entry_at("doc-squat", "P.md");
        backend.save_entry("space-a", &squatter, &m).unwrap();

        // Batch: a displaced loser at its conflict path, then the winner at P —
        // whose insert collides with the squatter on the unique path index.
        let loser = entry_at("doc-loser", "x.kutl-conflict-doc-loser.md");
        let winner = entry_at("doc-winner", "P.md");
        let err = backend.save_batch("space-a", &[(&loser, &m), (&winner, &m)]);
        assert!(err.is_err(), "winner's path collision aborts the batch");

        // The loser write rolled back with the winner — it must NOT be on disk.
        let ids: Vec<String> = backend
            .load_all_including_deleted("space-a")
            .unwrap()
            .into_iter()
            .map(|e| e.document_id)
            .collect();
        assert!(
            !ids.contains(&"doc-loser".to_string()),
            "the committed-prefix loser rolled back with the failed winner: {ids:?}"
        );
        assert_eq!(
            ids,
            vec!["doc-squat".to_string()],
            "only the pre-existing row survives"
        );
        backend.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sqlite_delete_batch_soft_deletes_all() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path()).await.unwrap();
        let m = MirrorMetadata::default();
        let a = entry_at("doc-a", "a.md");
        let b = entry_at("doc-b", "b.md");
        backend
            .save_batch("space-a", &[(&a, &m), (&b, &m)])
            .unwrap();

        backend
            .delete_batch("space-a", &[("doc-a", 7000), ("doc-b", 8000)])
            .unwrap();

        assert!(
            backend.load_all("space-a").unwrap().is_empty(),
            "both soft-deleted — no active rows remain"
        );
        let mut all: Vec<(String, Option<i64>)> = backend
            .load_all_including_deleted("space-a")
            .unwrap()
            .into_iter()
            .map(|e| (e.document_id, e.deleted_at))
            .collect();
        all.sort();
        assert_eq!(
            all,
            vec![
                ("doc-a".to_string(), Some(7000)),
                ("doc-b".to_string(), Some(8000))
            ],
            "tombstones carry their per-doc deleted_at"
        );
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
