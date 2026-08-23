//! `SQLite`-backed implementation of [`InviteBackend`].
//!
//! Shares a `SqlitePool` with [`crate::space_backend::SqliteSpaceBackend`] so
//! that both backends hit the same `registry.db` file through a single
//! connection pool, avoiding `SQLite` locking contention.

use crate::invite_backend::{InviteBackend, InviteBackendError, InviteInfo, InviteRecord};

/// Number of random bytes used to generate invite codes.
///
/// 8 bytes → 16 hex characters, which gives 2^64 ≈ 1.8 × 10^19 possible codes.
const INVITE_CODE_BYTES: usize = 8;

/// `SQLite`-backed invite backend for the OSS relay.
///
/// Stores invite codes in an `invites` table that JOINs the `spaces` table
/// for human-readable space names. The table is created automatically when
/// `new` is called.
pub struct SqliteInviteBackend {
    pool: sqlx::sqlite::SqlitePool,
}

impl SqliteInviteBackend {
    /// Create a backend from an already-open pool, creating the `invites`
    /// table if it does not exist.
    ///
    /// The caller is responsible for WAL mode and pool lifecycle. In
    /// production this pool is shared with
    /// [`crate::space_backend::SqliteSpaceBackend`].
    ///
    /// # Errors
    ///
    /// Returns `Storage` if the schema migration fails.
    pub async fn new(pool: sqlx::sqlite::SqlitePool) -> Result<Self, InviteBackendError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS invites (
                code       TEXT    PRIMARY KEY,
                space_id   TEXT    NOT NULL,
                expires_at INTEGER,
                created_at INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .map_err(|e| InviteBackendError::Storage(e.to_string()))?;

        Ok(Self { pool })
    }
}

impl InviteBackend for SqliteInviteBackend {
    fn create_invite(
        &self,
        space_id: &str,
        expires_at: Option<i64>,
    ) -> Result<InviteRecord, InviteBackendError> {
        let pool = self.pool.clone();
        let space_id_owned = space_id.to_owned();
        let code = generate_invite_code();
        let now = kutl_core::env::now_ms();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // Verify space exists and retrieve its name in one query.
                let row: Option<(String,)> =
                    sqlx::query_as("SELECT name FROM spaces WHERE space_id = ?")
                        .bind(&space_id_owned)
                        .fetch_optional(&pool)
                        .await
                        .map_err(|e| InviteBackendError::Storage(e.to_string()))?;

                let (space_name,) =
                    row.ok_or_else(|| InviteBackendError::SpaceNotFound(space_id_owned.clone()))?;

                sqlx::query(
                    "INSERT INTO invites (code, space_id, expires_at, created_at)
                     VALUES (?, ?, ?, ?)",
                )
                .bind(&code)
                .bind(&space_id_owned)
                .bind(expires_at)
                .bind(now)
                .execute(&pool)
                .await
                .map_err(|e| InviteBackendError::Storage(e.to_string()))?;

                Ok(InviteRecord {
                    code,
                    space_id: space_id_owned,
                    space_name,
                    expires_at,
                    created_at: now,
                })
            })
        })
    }

    fn validate_invite(&self, code: &str) -> Result<Option<InviteInfo>, InviteBackendError> {
        let pool = self.pool.clone();
        let code_owned = code.to_owned();
        let now = kutl_core::env::now_ms();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // JOIN spaces to get space_name in a single round-trip.
                let row: Option<(String, String, Option<i64>)> = sqlx::query_as(
                    "SELECT i.space_id, s.name, i.expires_at
                     FROM invites i
                     JOIN spaces s ON s.space_id = i.space_id
                     WHERE i.code = ?",
                )
                .bind(&code_owned)
                .fetch_optional(&pool)
                .await
                .map_err(|e| InviteBackendError::Storage(e.to_string()))?;

                match row {
                    None => Ok(None),
                    Some((space_id, space_name, expires_at)) => {
                        if expires_at.is_some_and(|exp| exp <= now) {
                            return Err(InviteBackendError::Expired(code_owned));
                        }
                        Ok(Some(InviteInfo {
                            space_id,
                            space_name,
                        }))
                    }
                }
            })
        })
    }

    fn list_invites(&self, space_id: &str) -> Result<Vec<InviteRecord>, InviteBackendError> {
        let pool = self.pool.clone();
        let space_id_owned = space_id.to_owned();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let rows: Vec<(String, String, Option<i64>, i64)> = sqlx::query_as(
                    "SELECT i.code, s.name, i.expires_at, i.created_at
                     FROM invites i
                     JOIN spaces s ON s.space_id = i.space_id
                     WHERE i.space_id = ?
                     ORDER BY i.created_at ASC",
                )
                .bind(&space_id_owned)
                .fetch_all(&pool)
                .await
                .map_err(|e| InviteBackendError::Storage(e.to_string()))?;

                let records = rows
                    .into_iter()
                    .map(|(code, space_name, expires_at, created_at)| InviteRecord {
                        code,
                        space_id: space_id_owned.clone(),
                        space_name,
                        expires_at,
                        created_at,
                    })
                    .collect();

                Ok(records)
            })
        })
    }

    fn revoke_invite(&self, code: &str) -> Result<bool, InviteBackendError> {
        let pool = self.pool.clone();
        let code_owned = code.to_owned();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let result = sqlx::query("DELETE FROM invites WHERE code = ?")
                    .bind(&code_owned)
                    .execute(&pool)
                    .await
                    .map_err(|e| InviteBackendError::Storage(e.to_string()))?;

                Ok(result.rows_affected() > 0)
            })
        })
    }
}

/// Generate a random invite code: 8 random bytes hex-encoded (16 chars).
fn generate_invite_code() -> String {
    let bytes: [u8; INVITE_CODE_BYTES] = std::array::from_fn(|_| rand::random::<u8>());
    bytes.iter().fold(String::with_capacity(16), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}
