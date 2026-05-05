//! Space management backend trait.
//!
//! The OSS relay uses [`SqliteSpaceBackend`] for name→UUID mapping.
//! kutlhub-relay provides its own implementation backed by the UX
//! server's Postgres tables.

use async_trait::async_trait;
use thiserror::Error;

/// Errors from space backend operations.
#[derive(Debug, Error)]
pub enum SpaceBackendError {
    /// Space name already taken.
    #[error("space name already taken: {0}")]
    NameConflict(String),
    /// Invalid space name.
    #[error("invalid space name: {0}")]
    InvalidName(String),
    /// Storage error.
    #[error("storage error: {0}")]
    Storage(String),
}

/// Result of registering a new space.
#[derive(Debug, Clone)]
pub struct RegisteredSpace {
    /// Relay-assigned UUID for the space.
    pub space_id: String,
    /// Human-readable name.
    pub name: String,
}

/// Backend for space registration and resolution.
///
/// The OSS relay needs only `register`, `resolve_by_name`, `resolve_by_id`,
/// and `list_spaces`. kutlhub-relay extends this with membership and
/// invitation support via its own implementation.
#[async_trait]
pub trait SpaceBackend: Send + Sync {
    /// Register a new space with the given name.
    ///
    /// Generates a UUID, stores the name→UUID mapping, and returns
    /// the new space record. Returns `NameConflict` if the name is
    /// already taken.
    async fn register(&self, name: &str) -> Result<RegisteredSpace, SpaceBackendError>;

    /// Resolve a space name to its UUID.
    async fn resolve_by_name(
        &self,
        name: &str,
    ) -> Result<Option<RegisteredSpace>, SpaceBackendError>;

    /// Resolve a space UUID to its record. Point lookup by primary key.
    async fn resolve_by_id(&self, id: &str) -> Result<Option<RegisteredSpace>, SpaceBackendError>;

    /// List all registered spaces.
    ///
    /// Returns all spaces ordered by name.
    async fn list_spaces(&self) -> Result<Vec<RegisteredSpace>, SpaceBackendError>;
}

/// `SQLite`-backed space backend for the OSS relay.
///
/// Stores space name→UUID mappings in a `spaces` table within the
/// same `registry.db` used by the document registry.
pub struct SqliteSpaceBackend {
    pool: sqlx::sqlite::SqlitePool,
}

impl SqliteSpaceBackend {
    /// Create a backend from an already-open pool.
    ///
    /// Ensures the `spaces` table exists. Use this when sharing a pool with
    /// other backends (e.g. [`crate::sqlite_invite_backend::SqliteInviteBackend`])
    /// to avoid lock contention from multiple pools on the same `SQLite` file.
    pub async fn new(pool: sqlx::sqlite::SqlitePool) -> Result<Self, SpaceBackendError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS spaces (
                space_id   TEXT    PRIMARY KEY,
                name       TEXT    NOT NULL UNIQUE,
                created_at INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .map_err(|e| SpaceBackendError::Storage(e.to_string()))?;

        Ok(Self { pool })
    }

    /// Open (or create) the `SQLite` database and ensure the `spaces` table exists.
    #[cfg(test)]
    pub async fn open(data_dir: &std::path::Path) -> Result<Self, SpaceBackendError> {
        let pool = crate::open_sqlite_pool(data_dir)
            .await
            .map_err(|e| SpaceBackendError::Storage(e.to_string()))?;
        Self::new(pool).await
    }
}

#[async_trait]
impl SpaceBackend for SqliteSpaceBackend {
    async fn register(&self, name: &str) -> Result<RegisteredSpace, SpaceBackendError> {
        let space_id = uuid::Uuid::new_v4().to_string();
        let now = kutl_core::env::now_ms();

        sqlx::query("INSERT INTO spaces (space_id, name, created_at) VALUES (?, ?, ?)")
            .bind(&space_id)
            .bind(name)
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                if let sqlx::Error::Database(ref dbe) = e
                    && dbe.code().as_deref() == Some("2067")
                {
                    return SpaceBackendError::NameConflict(name.to_owned());
                }
                SpaceBackendError::Storage(e.to_string())
            })?;

        Ok(RegisteredSpace {
            space_id,
            name: name.to_owned(),
        })
    }

    async fn resolve_by_name(
        &self,
        name: &str,
    ) -> Result<Option<RegisteredSpace>, SpaceBackendError> {
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT space_id, name FROM spaces WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| SpaceBackendError::Storage(e.to_string()))?;

        Ok(row.map(|(space_id, name)| RegisteredSpace { space_id, name }))
    }

    async fn resolve_by_id(&self, id: &str) -> Result<Option<RegisteredSpace>, SpaceBackendError> {
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT space_id, name FROM spaces WHERE space_id = ?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| SpaceBackendError::Storage(e.to_string()))?;

        Ok(row.map(|(space_id, name)| RegisteredSpace { space_id, name }))
    }

    async fn list_spaces(&self) -> Result<Vec<RegisteredSpace>, SpaceBackendError> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT space_id, name FROM spaces ORDER BY name ASC")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| SpaceBackendError::Storage(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|(space_id, name)| RegisteredSpace { space_id, name })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_register_and_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteSpaceBackend::open(dir.path()).await.unwrap();

        let result = backend.register("bright-falcon-a3f2").await.unwrap();
        assert_eq!(result.name, "bright-falcon-a3f2");
        assert!(!result.space_id.is_empty());

        let resolved = backend
            .resolve_by_name("bright-falcon-a3f2")
            .await
            .unwrap()
            .expect("should find space");
        assert_eq!(resolved.space_id, result.space_id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_register_duplicate_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteSpaceBackend::open(dir.path()).await.unwrap();

        backend.register("my-space-1234").await.unwrap();
        let err = backend.register("my-space-1234").await.unwrap_err();
        assert!(
            matches!(err, SpaceBackendError::NameConflict(_)),
            "expected NameConflict, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resolve_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteSpaceBackend::open(dir.path()).await.unwrap();

        let result = backend.resolve_by_name("no-such-space").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_resolve_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteSpaceBackend::open(dir.path()).await.unwrap();

        let registered = backend.register("id-test-space").await.unwrap();
        let found = backend.resolve_by_id(&registered.space_id).await.unwrap();
        assert_eq!(
            found.as_ref().map(|s| &s.name),
            Some(&"id-test-space".to_owned())
        );

        let missing = backend.resolve_by_id("not-a-real-uuid").await.unwrap();
        assert!(missing.is_none());
    }
}
