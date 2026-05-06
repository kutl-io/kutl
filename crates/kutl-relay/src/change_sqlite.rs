//! `SQLite` implementation of [`ChangeBackend`].
//!
//! Persists signals (flags, chats, replies) and per-DID cursors. Document
//! changes are queried from the existing `registry` table maintained by
//! [`crate::registry_store`].

use async_trait::async_trait;
use sqlx::sqlite::SqlitePool;

use crate::change_backend::{
    self, ChangeBackend, ChangeError, ChangesResponse, SignalDetail, SignalReplyDetail,
};
use crate::registry::RegistryEntry;
use crate::registry_store::RegistryRow;

/// Maximum events returned per query.
const MAX_EVENTS_PER_QUERY: i64 = 100;

/// Retention period: 30 days in milliseconds.
const RETENTION_MS: i64 = 30 * kutl_core::MS_PER_DAY;

/// SQL to create the flat signals table.
///
/// Intentionally does NOT carry the `closed_at` column from the kutlhub
/// Postgres schema (RFD 0071 Phase 1). State transitions (close, reopen)
/// have no recording path on the OSS relay — the only consumers of close
/// state today are kutlhub's TS UX server (Catch-Up, board, signal-detail)
/// and the kutlhub Redis stream observer. There is no contemplated
/// migration path from an OSS-only deployment to kutlhub (the free → paid
/// path stays inside kutlhub the whole way; the OSS → kutlhub story is
/// "join a kutlhub space and let the daemon push your local docs", not a
/// database transplant). Carrying schema bloat here would suggest support
/// that doesn't exist.
const CREATE_SIGNALS_TABLE: &str = "\
    CREATE TABLE IF NOT EXISTS signals (\
        id               TEXT    NOT NULL PRIMARY KEY,\
        space_id         TEXT    NOT NULL,\
        document_id      TEXT,\
        author_did       TEXT    NOT NULL,\
        signal_type      TEXT    NOT NULL,\
        timestamp        INTEGER NOT NULL,\
        flag_kind        INTEGER,\
        audience         INTEGER,\
        message          TEXT,\
        target_did       TEXT,\
        topic            TEXT,\
        parent_signal_id TEXT,\
        parent_reply_id  TEXT,\
        body             TEXT\
    )";

/// SQL to create the space+timestamp index on signals.
const CREATE_SIGNALS_INDEX: &str = "\
    CREATE INDEX IF NOT EXISTS idx_signals_space_ts \
    ON signals (space_id, timestamp)";

/// `SQLite`-backed change backend for self-hosted relays.
///
/// Shares the same pool as the registry backend to avoid lock contention.
pub struct SqliteChangeBackend {
    pool: SqlitePool,
}

impl SqliteChangeBackend {
    /// Create a backend from an already-open pool.
    ///
    /// Creates the `signals` and `change_cursors` tables if they do not exist.
    /// The pool should be the same one used by
    /// [`crate::registry_store::SqliteBackend`].
    ///
    /// # Errors
    ///
    /// Returns [`ChangeError::Db`] if table creation fails.
    pub async fn new(pool: SqlitePool) -> Result<Self, ChangeError> {
        sqlx::query(CREATE_SIGNALS_TABLE).execute(&pool).await?;

        sqlx::query(CREATE_SIGNALS_INDEX).execute(&pool).await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS change_cursors (
                did              TEXT    NOT NULL,
                space_id         TEXT    NOT NULL,
                signal_cursor    INTEGER NOT NULL DEFAULT 0,
                reg_cursor       INTEGER NOT NULL DEFAULT 0,
                prev1_signal     INTEGER NOT NULL DEFAULT 0,
                prev1_reg        INTEGER NOT NULL DEFAULT 0,
                prev2_signal     INTEGER NOT NULL DEFAULT 0,
                prev2_reg        INTEGER NOT NULL DEFAULT 0,
                checkpoint       TEXT    NOT NULL DEFAULT '',
                updated_at       INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (did, space_id)
            )",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }
}

/// Row type for reading signals from `SQLite`.
#[derive(sqlx::FromRow)]
struct SignalRow {
    id: String,
    space_id: String,
    document_id: Option<String>,
    author_did: String,
    signal_type: String,
    timestamp: i64,
    flag_kind: Option<i32>,
    audience: Option<i32>,
    message: Option<String>,
    target_did: Option<String>,
    topic: Option<String>,
    parent_signal_id: Option<String>,
    parent_reply_id: Option<String>,
    body: Option<String>,
}

impl SignalRow {
    /// Convert a flat row into a proto `Signal` with the correct payload variant.
    fn into_proto(self) -> kutl_proto::sync::Signal {
        use kutl_proto::sync::signal::Payload;

        let payload = match self.signal_type.as_str() {
            "flag" => Some(Payload::Flag(kutl_proto::sync::FlagPayload {
                kind: self.flag_kind.unwrap_or(0),
                audience_type: self.audience.unwrap_or(0),
                target_did: self.target_did,
                message: self.message.unwrap_or_default(),
            })),
            "chat" => Some(Payload::Chat(kutl_proto::sync::ChatPayload {
                topic: self.topic,
                target_did: self.target_did,
            })),
            "reply" => Some(Payload::Reply(kutl_proto::sync::ReplyPayload {
                parent_signal_id: self.parent_signal_id.unwrap_or_default(),
                parent_reply_id: self.parent_reply_id,
                body: self.body.unwrap_or_default(),
            })),
            _ => None,
        };

        kutl_proto::sync::Signal {
            id: self.id,
            space_id: self.space_id,
            document_id: self.document_id,
            author_did: self.author_did,
            timestamp: self.timestamp,
            payload,
        }
    }
}

/// Row type for reading the cursor ring from `SQLite`.
#[derive(sqlx::FromRow)]
struct CursorRow {
    signal_cursor: i64,
    reg_cursor: i64,
    prev1_signal: i64,
    prev1_reg: i64,
    prev2_signal: i64,
    prev2_reg: i64,
    checkpoint: String,
}

impl SqliteChangeBackend {
    /// Fetch signals with `timestamp > signal_cursor` for the given space.
    async fn fetch_signals(
        &self,
        space_id: &str,
        signal_cursor: i64,
    ) -> Result<(Vec<kutl_proto::sync::Signal>, i64), ChangeError> {
        let rows = sqlx::query_as::<_, SignalRow>(
            "SELECT id, space_id, document_id, author_did, signal_type,
                    timestamp, flag_kind, audience, message, target_did,
                    topic, parent_signal_id, parent_reply_id, body
             FROM signals
             WHERE space_id = ? AND timestamp > ?
             ORDER BY timestamp ASC
             LIMIT ?",
        )
        .bind(space_id)
        .bind(signal_cursor)
        .bind(MAX_EVENTS_PER_QUERY)
        .fetch_all(&self.pool)
        .await?;

        let new_cursor = rows.last().map_or(signal_cursor, |r| r.timestamp);
        let protos = rows.into_iter().map(SignalRow::into_proto).collect();
        Ok((protos, new_cursor))
    }

    /// Fetch registry entries changed since `reg_cursor` for the given space.
    async fn fetch_registry_changes(
        &self,
        space_id: &str,
        reg_cursor: i64,
    ) -> Result<(Vec<RegistryEntry>, i64), ChangeError> {
        let rows = sqlx::query_as::<_, RegistryRow>(
            "SELECT document_id, path, created_by, created_at,
                    renamed_by, renamed_at, deleted_at, edited_at
             FROM registry
             WHERE space_id = ?
               AND (created_at > ? OR renamed_at > ? OR deleted_at > ? OR edited_at > ?)
             ORDER BY COALESCE(edited_at, deleted_at, renamed_at, created_at) ASC
             LIMIT ?",
        )
        .bind(space_id)
        .bind(reg_cursor)
        .bind(reg_cursor)
        .bind(reg_cursor)
        .bind(reg_cursor)
        .bind(MAX_EVENTS_PER_QUERY)
        .fetch_all(&self.pool)
        .await?;

        let new_cursor = rows.iter().fold(reg_cursor, |acc, r| {
            acc.max(r.created_at)
                .max(r.renamed_at.unwrap_or(0))
                .max(r.deleted_at.unwrap_or(0))
                .max(r.edited_at.unwrap_or(0))
        });
        let entries = rows.into_iter().map(RegistryRow::into_entry).collect();
        Ok((entries, new_cursor))
    }

    /// Upsert the cursor row, shifting the checkpoint ring.
    async fn save_cursors(
        &self,
        did: &str,
        space_id: &str,
        signal_cursor: i64,
        reg_cursor: i64,
        checkpoint: &str,
    ) -> Result<(), ChangeError> {
        sqlx::query(
            "INSERT INTO change_cursors
                (did, space_id, signal_cursor, reg_cursor,
                 prev1_signal, prev1_reg, prev2_signal, prev2_reg,
                 checkpoint, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (did, space_id) DO UPDATE SET
                prev2_signal  = change_cursors.prev1_signal,
                prev2_reg     = change_cursors.prev1_reg,
                prev1_signal  = change_cursors.signal_cursor,
                prev1_reg     = change_cursors.reg_cursor,
                signal_cursor = excluded.signal_cursor,
                reg_cursor    = excluded.reg_cursor,
                checkpoint    = excluded.checkpoint,
                updated_at    = excluded.updated_at",
        )
        .bind(did)
        .bind(space_id)
        .bind(signal_cursor)
        .bind(reg_cursor)
        .bind(0i64) // prev1 values for fresh insert
        .bind(0i64)
        .bind(0i64) // prev2 values for fresh insert
        .bind(0i64)
        .bind(checkpoint)
        .bind(kutl_core::env::now_ms())
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl ChangeBackend for SqliteChangeBackend {
    async fn get_changes(
        &self,
        did: &str,
        space_id: &str,
        checkpoint: Option<&str>,
    ) -> Result<ChangesResponse, ChangeError> {
        let cursor = sqlx::query_as::<_, CursorRow>(
            "SELECT signal_cursor, reg_cursor, prev1_signal, prev1_reg,
                    prev2_signal, prev2_reg, checkpoint
             FROM change_cursors
             WHERE did = ? AND space_id = ?",
        )
        .bind(did)
        .bind(space_id)
        .fetch_optional(&self.pool)
        .await?;

        let (signal_cursor, reg_cursor) = match cursor.as_ref() {
            Some(c) => change_backend::resolve_cursors(
                checkpoint,
                (c.signal_cursor, c.reg_cursor),
                (c.prev1_signal, c.prev1_reg),
                (c.prev2_signal, c.prev2_reg),
                &c.checkpoint,
            ),
            None => (0, 0),
        };

        // Record the starting position as the checkpoint so clients can rewind
        // to re-fetch this response if needed.
        let new_checkpoint = format!("{signal_cursor}:{reg_cursor}");

        let (signals, new_signal) = self.fetch_signals(space_id, signal_cursor).await?;
        let (document_changes, new_reg) = self.fetch_registry_changes(space_id, reg_cursor).await?;

        self.save_cursors(did, space_id, new_signal, new_reg, &new_checkpoint)
            .await?;

        Ok(ChangesResponse {
            signals,
            document_changes,
            checkpoint: new_checkpoint,
        })
    }

    async fn record_signal(
        &self,
        id: &str,
        space_id: &str,
        document_id: Option<&str>,
        author_did: &str,
        signal_type: &str,
        timestamp: i64,
        flag_kind: Option<i32>,
        audience: Option<i32>,
        message: Option<&str>,
        target_did: Option<&str>,
        topic: Option<&str>,
        parent_signal_id: Option<&str>,
        parent_reply_id: Option<&str>,
        body: Option<&str>,
    ) -> Result<(), ChangeError> {
        sqlx::query(
            "INSERT INTO signals
                (id, space_id, document_id, author_did, signal_type,
                 timestamp, flag_kind, audience, message, target_did,
                 topic, parent_signal_id, parent_reply_id, body)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(space_id)
        .bind(document_id)
        .bind(author_did)
        .bind(signal_type)
        .bind(timestamp)
        .bind(flag_kind)
        .bind(audience)
        .bind(message)
        .bind(target_did)
        .bind(topic)
        .bind(parent_signal_id)
        .bind(parent_reply_id)
        .bind(body)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_signal_detail(
        &self,
        space_id: &str,
        signal_id: &str,
    ) -> Result<SignalDetail, ChangeError> {
        // Filter by both id and space_id: the caller is authorized against a
        // specific space, and we must not return a signal that lives in a
        // different space even if its id matches. The Postgres variant
        // (`pg_change.rs::fetch_signal_header`) already does this.
        let row = sqlx::query_as::<_, SignalRow>(
            "SELECT id, space_id, document_id, author_did, signal_type,
                    timestamp, flag_kind, audience, message, target_did,
                    topic, parent_signal_id, parent_reply_id, body
             FROM signals
             WHERE id = ? AND space_id = ?",
        )
        .bind(signal_id)
        .bind(space_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ChangeError::NotFound(format!("signal {signal_id} not found")))?;

        // Fetch replies (signals of type "reply" referencing this signal).
        let reply_rows = sqlx::query_as::<_, SignalRow>(
            "SELECT id, space_id, document_id, author_did, signal_type,
                    timestamp, flag_kind, audience, message, target_did,
                    topic, parent_signal_id, parent_reply_id, body
             FROM signals
             WHERE parent_signal_id = ? AND signal_type = 'reply'
             ORDER BY timestamp ASC",
        )
        .bind(signal_id)
        .fetch_all(&self.pool)
        .await?;

        let replies = reply_rows
            .into_iter()
            .map(|r| SignalReplyDetail {
                id: r.id,
                parent_reply_id: r.parent_reply_id,
                author_did: r.author_did,
                body: r.body.unwrap_or_default(),
                created_at: r.timestamp,
            })
            .collect();

        Ok(SignalDetail {
            id: row.id,
            space_id: row.space_id,
            document_id: row.document_id,
            author_did: row.author_did,
            signal_type: row.signal_type,
            timestamp: row.timestamp,
            flag_kind: row.flag_kind.map(|k| k.to_string()),
            audience: row.audience.map(|a| a.to_string()),
            target_did: row.target_did,
            message: row.message,
            closed_at: None,
            topic: row.topic,
            parent_signal_id: row.parent_signal_id,
            parent_reply_id: row.parent_reply_id,
            body: row.body,
            replies,
            reactions: Vec::new(), // Reactions only exist in kutlhub's Postgres
        })
    }

    async fn prune(&self, now_ms: i64) -> Result<u64, ChangeError> {
        let cutoff = now_ms - RETENTION_MS;

        let signal_result = sqlx::query("DELETE FROM signals WHERE timestamp < ?")
            .bind(cutoff)
            .execute(&self.pool)
            .await?;

        let cursor_result = sqlx::query("DELETE FROM change_cursors WHERE updated_at < ?")
            .bind(cutoff)
            .execute(&self.pool)
            .await?;

        let reg_result =
            sqlx::query("DELETE FROM registry WHERE deleted_at IS NOT NULL AND deleted_at < ?")
                .bind(cutoff)
                .execute(&self.pool)
                .await?;

        Ok(signal_result.rows_affected()
            + cursor_result.rows_affected()
            + reg_result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create an in-memory `SQLite` pool with the `registry` table
    /// (normally created by `SqliteBackend`).
    async fn test_pool() -> SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        // Create the registry table that normally exists from registry_store.
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
        .await
        .unwrap();

        pool
    }

    #[tokio::test]
    async fn test_get_changes_empty() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();

        assert!(resp.signals.is_empty());
        assert!(resp.document_changes.is_empty());
        assert_eq!(resp.checkpoint, "0:0");
    }

    #[tokio::test]
    async fn test_get_changes_flag_signal_round_trip() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        // Record a flag signal.
        backend
            .record_signal(
                "sig-test-1",
                "space-1",
                Some("doc-1"),
                "did:bob",
                "flag",
                1000,
                Some(1), // flag_kind
                Some(0), // audience
                Some("please review"),
                None, // target_did
                None, // topic
                None, // parent_signal_id
                None, // parent_reply_id
                None, // body
            )
            .await
            .unwrap();

        // First call should return the signal.
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 1);
        assert_eq!(resp.signals[0].id, "sig-test-1");
        assert_eq!(resp.signals[0].document_id.as_deref(), Some("doc-1"));
        assert_eq!(resp.signals[0].author_did, "did:bob");

        // Verify the payload is a Flag.
        let payload = resp.signals[0].payload.as_ref().unwrap();
        match payload {
            kutl_proto::sync::signal::Payload::Flag(f) => {
                assert_eq!(f.message, "please review");
                assert_eq!(f.kind, 1);
            }
            _ => panic!("expected Flag payload"),
        }

        // Second call should be empty (cursor advanced).
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert!(resp.signals.is_empty());
    }

    #[tokio::test]
    async fn test_get_changes_separate_cursors_per_did() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        backend
            .record_signal(
                "sig-test-2",
                "space-1",
                Some("doc-1"),
                "did:carol",
                "flag",
                1000,
                Some(1),
                Some(0),
                Some("hello"),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        // Alice sees the event.
        let resp_alice = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp_alice.signals.len(), 1);

        // Bob also sees the same event (independent cursor).
        let resp_bob = backend
            .get_changes("did:bob", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp_bob.signals.len(), 1);

        // Alice's second call is empty.
        let resp_alice2 = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert!(resp_alice2.signals.is_empty());
    }

    #[tokio::test]
    async fn test_get_changes_registry_changes_included() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();

        // Insert a registry entry directly (simulating registry_store).
        sqlx::query(
            "INSERT INTO registry (space_id, document_id, path, created_by, created_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind("space-1")
        .bind("doc-1")
        .bind("notes/a.md")
        .bind("did:alice")
        .bind(5000i64)
        .execute(&pool)
        .await
        .unwrap();

        let resp = backend
            .get_changes("did:bob", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.document_changes.len(), 1);
        assert_eq!(resp.document_changes[0].document_id, "doc-1");
        assert_eq!(resp.document_changes[0].path, "notes/a.md");

        // Second call should be empty (cursor advanced past created_at=5000).
        let resp = backend
            .get_changes("did:bob", "space-1", None)
            .await
            .unwrap();
        assert!(resp.document_changes.is_empty());
    }

    #[tokio::test]
    async fn test_prune_retention() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        // Old signal (timestamp 1000).
        backend
            .record_signal(
                "sig-old",
                "space-1",
                Some("doc-1"),
                "did:alice",
                "flag",
                1000,
                Some(1),
                Some(0),
                Some("old"),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        // New signal (timestamp far in the future).
        let recent_ts = 100_000_000_000i64;
        backend
            .record_signal(
                "sig-new",
                "space-1",
                Some("doc-2"),
                "did:alice",
                "flag",
                recent_ts,
                Some(1),
                Some(0),
                Some("new"),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        // Prune with now = recent_ts + 1 (so cutoff = recent_ts + 1 - 30d).
        // The old signal at 1000 is well before the cutoff; the new one is not.
        let pruned = backend.prune(recent_ts + 1).await.unwrap();
        assert_eq!(pruned, 1, "should prune exactly the old signal");

        // Verify: only the new signal remains.
        let resp = backend
            .get_changes("did:bob", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 1);
        match resp.signals[0].payload.as_ref().unwrap() {
            kutl_proto::sync::signal::Payload::Flag(f) => {
                assert_eq!(f.message, "new");
            }
            _ => panic!("expected Flag payload"),
        }
    }

    #[tokio::test]
    async fn test_get_changes_invalid_checkpoint_falls_through() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();
        let did = "did:key:agent";
        let space = "s1";

        // Record a flag signal.
        backend
            .record_signal(
                "sig-cp-1",
                space,
                Some("doc1"),
                "did:key:alice",
                "flag",
                1000,
                Some(1),
                Some(2),
                Some("hello"),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        // First call -- gets the signal and a checkpoint.
        let resp = backend.get_changes(did, space, None).await.unwrap();
        assert_eq!(resp.signals.len(), 1);
        let valid_checkpoint = resp.checkpoint.clone();

        // Immediately replay with the valid checkpoint -- still in the ring
        // (stored_checkpoint slot) -- should return the same signal again.
        let resp = backend
            .get_changes(did, space, Some(&valid_checkpoint))
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 1);
        match resp.signals[0].payload.as_ref().unwrap() {
            kutl_proto::sync::signal::Payload::Flag(f) => {
                assert_eq!(f.message, "hello");
            }
            _ => panic!("expected Flag payload"),
        }

        // Call with a parseable but unrecognised checkpoint (values that don't
        // match any ring slot) -- should fall through to the current cursor
        // position, returning empty and not an error.
        let resp = backend
            .get_changes(did, space, Some("bogus:invalid"))
            .await
            .unwrap();
        assert!(resp.signals.is_empty());

        // Call with a completely unparseable checkpoint string -- also falls
        // through to the current cursor position, returning empty and not an error.
        let resp = backend
            .get_changes(did, space, Some("not-a-checkpoint"))
            .await
            .unwrap();
        assert!(resp.signals.is_empty());
    }

    #[tokio::test]
    async fn test_get_changes_checkpoint_rewind() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        // Record the first signal.
        backend
            .record_signal(
                "sig-rewind-1",
                "space-1",
                Some("doc-1"),
                "did:bob",
                "flag",
                1000,
                Some(1),
                Some(0),
                Some("first"),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        // First call: should return the first signal and a checkpoint.
        let resp1 = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp1.signals.len(), 1);
        let checkpoint = resp1.checkpoint.clone();

        // Record a second signal.
        backend
            .record_signal(
                "sig-rewind-2",
                "space-1",
                Some("doc-2"),
                "did:bob",
                "flag",
                2000,
                Some(1),
                Some(0),
                Some("second"),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        // Second call using the checkpoint from step 1: cursor rewinds to before
        // the first call, so both signals should be returned.
        let resp2 = backend
            .get_changes("did:alice", "space-1", Some(&checkpoint))
            .await
            .unwrap();
        assert_eq!(
            resp2.signals.len(),
            2,
            "checkpoint rewind should return both signals"
        );
    }

    #[tokio::test]
    async fn test_get_signal_detail_with_replies() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        // Record a chat signal.
        backend
            .record_signal(
                "chat-1",
                "space-1",
                None,
                "did:alice",
                "chat",
                1000,
                None,
                None,
                None,
                None,
                Some("design review"),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        // Record a reply to the chat.
        backend
            .record_signal(
                "reply-1",
                "space-1",
                None,
                "did:bob",
                "reply",
                2000,
                None,
                None,
                None,
                None,
                None,
                Some("chat-1"),
                None,
                Some("looks good"),
            )
            .await
            .unwrap();

        let detail = backend
            .get_signal_detail("space-1", "chat-1")
            .await
            .unwrap();

        assert_eq!(detail.id, "chat-1");
        assert_eq!(detail.signal_type, "chat");
        assert_eq!(detail.topic.as_deref(), Some("design review"));
        assert_eq!(detail.replies.len(), 1);
        assert_eq!(detail.replies[0].id, "reply-1");
        assert_eq!(detail.replies[0].body, "looks good");
        assert!(detail.reactions.is_empty());
    }

    #[tokio::test]
    async fn test_get_signal_detail_not_found() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        let result = backend.get_signal_detail("space-1", "nonexistent").await;

        assert!(matches!(result, Err(ChangeError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_get_changes_includes_edited_documents() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();

        // Register a document.
        sqlx::query(
            "INSERT INTO registry (space_id, document_id, path, created_by, created_at)
             VALUES ('space-1', 'doc-1', 'workspace.md', 'did:demo:alice', 1000)",
        )
        .execute(&pool)
        .await
        .unwrap();

        // First call: sees the new document.
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.document_changes.len(), 1);
        assert_eq!(resp.document_changes[0].document_id, "doc-1");
        assert!(resp.document_changes[0].edited_at.is_none());

        // Second call: no changes.
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert!(resp.document_changes.is_empty());

        // Simulate an edit by updating edited_at.
        sqlx::query("UPDATE registry SET edited_at = 2000 WHERE document_id = 'doc-1'")
            .execute(&pool)
            .await
            .unwrap();

        // Third call: sees the edit.
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.document_changes.len(), 1);
        assert_eq!(resp.document_changes[0].edited_at, Some(2000));
    }

    #[tokio::test]
    async fn test_chat_and_reply_payloads() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        // Record a chat signal.
        backend
            .record_signal(
                "chat-2",
                "space-1",
                None,
                "did:alice",
                "chat",
                1000,
                None,
                None,
                None,
                Some("did:bob"),
                Some("topic-1"),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        // Record a reply signal.
        backend
            .record_signal(
                "reply-2",
                "space-1",
                None,
                "did:bob",
                "reply",
                2000,
                None,
                None,
                None,
                None,
                None,
                Some("chat-2"),
                None,
                Some("my reply body"),
            )
            .await
            .unwrap();

        let resp = backend
            .get_changes("did:eve", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 2);

        // Verify chat payload.
        match resp.signals[0].payload.as_ref().unwrap() {
            kutl_proto::sync::signal::Payload::Chat(c) => {
                assert_eq!(c.topic.as_deref(), Some("topic-1"));
                assert_eq!(c.target_did.as_deref(), Some("did:bob"));
            }
            _ => panic!("expected Chat payload"),
        }

        // Verify reply payload.
        match resp.signals[1].payload.as_ref().unwrap() {
            kutl_proto::sync::signal::Payload::Reply(r) => {
                assert_eq!(r.parent_signal_id, "chat-2");
                assert_eq!(r.body, "my reply body");
            }
            _ => panic!("expected Reply payload"),
        }
    }
}
