//! `SQLite` implementation of [`ChangeBackend`].
//!
//! Persists signals (flags, replies) and per-DID cursors. Document
//! changes are queried from the existing `registry` table maintained by
//! [`crate::registry_store`].

use async_trait::async_trait;
use sqlx::sqlite::SqlitePool;

use crate::change_backend::{
    self, ChangeBackend, ChangeError, ChangesResponse, ContentOverlay, PayloadColumns,
    ProjectionRebuild, ProjectionWriter, SignalDetail, SignalReplyDetail,
};
use crate::registry::RegistryEntry;
use crate::registry_store::RegistryRow;

/// Maximum events returned per query (page size). `usize` for in-Rust page
/// comparisons; converted to `i64` only at the SQL `LIMIT` bind sites. Also
/// the default page size for the HTTP signal catch-up route
/// ([`crate::http::signals`]).
pub(crate) const MAX_EVENTS_PER_QUERY: usize = 100;

/// Retention period for per-DID change cursors: 30 days in milliseconds.
///
/// This bounds ONLY the `change_cursors` reaper. Signal
/// rows are a rebuildable projection of the segment records (the source of
/// truth), so prune never deletes them by age: a windowed trim cannot hold —
/// the next `project_records` refolds the COMPLETE segment set and re-upserts
/// every signal, resurrecting exactly what a window trimmed.
const RETENTION_MS: i64 = 30 * kutl_core::MS_PER_DAY;

/// SQL to create the flat signals table.
///
/// This table is a REBUILDABLE PROJECTION derived from the append-only
/// signal records in segments — it is not the source
/// of truth. [`SqliteChangeBackend::project_records`] folds a space's
/// records with `kutl_signals::fold` and rewrites these rows to match, so
/// every column is derived and the whole table can be dropped and rebuilt.
///
/// `seq` is the projection's own append counter and the change feed's page
/// key: one greater than the largest `seq` in the table at the moment the row
/// is inserted, and never touched again by an update. It is what makes the feed
/// deliverable exactly once. A `(timestamp, id)` key cannot: `timestamp` is a
/// millisecond, so two records routinely share one, and `id` is a random UUID,
/// so a row committed after a read but inside the cursor's millisecond sorts
/// below the cursor about half the time and is then unreachable forever. `seq`
/// is assigned at insert, so a row that lands later is above every cursor
/// already handed out — with no dependence on the clock and none on the shape
/// of an id.
///
/// `NOT NULL` because a row without a key is invisible to the feed and to
/// nothing else: `seq > cursor` is false for a NULL, so the signal is stored,
/// readable by id, and never delivered to anyone. An insert path that forgets
/// the key therefore fails here instead of producing that. A table migrated by
/// [`ADD_SEQ_COLUMN`] carries the weaker nullable form — `ALTER` cannot add a
/// `NOT NULL` column without a constant default and no constant is right —
/// which is why the constraint is worth having on every table created from
/// here, including every test's.
///
/// A real column rather than the table's implicit `rowid`, which carries the
/// same insertion order and would need no storage: `VACUUM` renumbers rowids
/// for a table whose primary key is not an `INTEGER PRIMARY KEY`, and this
/// one's is composite and textual. A routine maintenance command would
/// therefore renumber the feed's page key underneath every stored cursor, and
/// the compaction only ever lowers it — so every cursor would sit above rows it
/// had never delivered, which is the failure the key exists to prevent. `seq`
/// survives `VACUUM`, survives the composite-key rebuild below (which copies
/// it), and is carried forward by the upsert.
///
/// `closed_at` / `close_reason` are the derived close-state: the fold
/// resolves each signal's lifecycle (CREATED → CLOSED → REOPENED …) by
/// last-writer-wins over the HLC total order, and the winning CLOSED
/// transition populates these columns (both `NULL` when the signal folds
/// to `Open`, including after a REOPENED).
/// The PRIMARY KEY is COMPOSITE `(space_id, id)`, not `id` alone. A signal id
/// is only unique WITHIN a space on the projection: the re-seed path lets a
/// peer supply a chosen signal `id`, so an id-alone PK would let an attacker's
/// record (ingested into their own space) conflict-collide with another space's
/// row. Scoping the key to `(space_id, id)` makes a same-id row in another
/// space a distinct row that can never be reached across the space boundary.
///
/// Shared by the `CREATE TABLE IF NOT EXISTS` at open and by the composite-PK
/// rebuild's scratch table, so the two cannot disagree about what a projection
/// row is. With two literal column lists they drift: a column added by an
/// idempotent ALTER is absent from the scratch table the rebuild renames over
/// it, silently dropping whichever column was added last.
const SIGNALS_TABLE_BODY: &str = "(\
        seq              INTEGER NOT NULL,\
        id               TEXT    NOT NULL,\
        space_id         TEXT    NOT NULL,\
        document_id      TEXT,\
        author_did       TEXT    NOT NULL,\
        signal_type      TEXT    NOT NULL,\
        timestamp        INTEGER NOT NULL,\
        flag_kind        INTEGER,\
        audience         INTEGER,\
        message          TEXT,\
        target_did       TEXT,\
        anchor_text      TEXT,\
        parent_signal_id TEXT,\
        parent_reply_id  TEXT,\
        body             TEXT,\
        closed_at        INTEGER,\
        close_reason     INTEGER,\
        decision_title_hash  TEXT,\
        decision_title       TEXT,\
        decision_depth       INTEGER,\
        decision_parent_hash TEXT,\
        record_id            TEXT,\
        deleted_at       INTEGER,\
        PRIMARY KEY (space_id, id)\
    )";

/// The active-signal view: every row that has not been tombstoned.
///
/// The pattern this codebase already uses for soft-deleted entities —
/// `documents_active`, `spaces_active` on the Postgres side. It exists so a
/// reader cannot FORGET the filter: reads select from here, writes target the
/// base table. Thirty-four hand-written `deleted_at IS NULL` predicates across
/// the stack are what the alternative looks like, and every one of them is a
/// rule someone has to remember.
/// Built with [`SIGNAL_COLUMNS`] rather than `SELECT *`, and `deleted_at` is
/// DELIBERATELY OMITTED — every row here is non-deleted by construction, so the
/// column carries no information, and leaving it out means a reader who tries
/// to filter on it gets an error instead of writing a redundant predicate that
/// looks like diligence. Mirrors `documents_active` on the Postgres side, whose
/// schema comment puts it as "excludes the deletedAt column so referencing it
/// is a compile error".
fn signals_active_view_sql() -> String {
    format!(
        "CREATE VIEW signals_active AS \
         SELECT {SIGNAL_COLUMNS}, record_id \
         FROM signals WHERE deleted_at IS NULL"
    )
}

/// Dropped and recreated on every startup, right after the column ALTERs, so
/// the view's `*` always resolves over the CURRENT column set. A
/// `CREATE VIEW IF NOT EXISTS` would freeze whatever shape existed when it was
/// first made and silently omit later columns.
const DROP_SIGNALS_ACTIVE_VIEW: &str = "DROP VIEW IF EXISTS signals_active";

/// Idempotent ALTERs adding the derived close-state columns to a
/// pre-existing `signals` table. New databases already carry them via
/// [`SIGNALS_TABLE_BODY`]; on an older self-hosted relay DB these run once
/// and then no-op (a rebuild would also suffice, since the table is a
/// projection — the guarded ALTER just avoids a destructive rebuild).
const ADD_CLOSED_AT_COLUMN: &str = "ALTER TABLE signals ADD COLUMN closed_at INTEGER";
/// Companion to [`ADD_CLOSED_AT_COLUMN`] for the close-reason column.
const ADD_CLOSE_REASON_COLUMN: &str = "ALTER TABLE signals ADD COLUMN close_reason INTEGER";
/// Soft-delete marker for a TOMBSTONED record: hide, never delete.
///
/// A row-delete is not survivable: a `REOPENED` record carries no payload, so
/// nothing incremental could resurrect a row that was gone — only a
/// whole-space refold could, and the Postgres substrate has no refold. Both
/// substrates hide rather than delete, so a reopen is an ordinary update on
/// both.
const ADD_DELETED_AT_COLUMN: &str = "ALTER TABLE signals ADD COLUMN deleted_at INTEGER";

/// Idempotent ALTER for the `anchor_text` column. Stand-alone
/// migration runs on every startup so older self-hosted relay
/// databases gain the column without a destructive
/// rebuild. New databases pick it up via [`SIGNALS_TABLE_BODY`] above
/// and the ALTER is a no-op.
const ADD_ANCHOR_TEXT_COLUMN: &str = "ALTER TABLE signals ADD COLUMN anchor_text TEXT";

/// Idempotent ALTERs adding the `Payload::Decision` columns to a pre-existing
/// `signals` table. A `DecisionPayload` carries fields (`title_hash`, `title`,
/// `depth`, `parent_hash`) with no equivalent in the flag/reply
/// columns; without dedicated storage a projected decision would be
/// mis-materialized. New databases already carry these via
/// [`SIGNALS_TABLE_BODY`]; on older self-hosted DBs they run once then no-op.
/// (`Payload::Chat` needs no new columns — it reuses `message` for the topic
/// and `target_did`.) There is no `decision_state` column in the current
/// shape — resolved-ness lives in `close_reason`; an older DB may keep an
/// orphaned `decision_state` column, which is never read.
const ADD_DECISION_TITLE_HASH_COLUMN: &str =
    "ALTER TABLE signals ADD COLUMN decision_title_hash TEXT";
/// Companion to [`ADD_DECISION_TITLE_HASH_COLUMN`] for the decision title.
const ADD_DECISION_TITLE_COLUMN: &str = "ALTER TABLE signals ADD COLUMN decision_title TEXT";
/// Companion to [`ADD_DECISION_TITLE_HASH_COLUMN`] for the heading depth.
const ADD_DECISION_DEPTH_COLUMN: &str = "ALTER TABLE signals ADD COLUMN decision_depth INTEGER";
/// Companion to [`ADD_DECISION_TITLE_HASH_COLUMN`] for the optional parent hash.
const ADD_DECISION_PARENT_HASH_COLUMN: &str =
    "ALTER TABLE signals ADD COLUMN decision_parent_hash TEXT";

/// Idempotent ALTER for the `record_id` column: the id of the
/// CREATED record this row was projected from.
///
/// Nullable, and the null is meaningful — it says "this row has no record
/// behind it", which is precisely the condition the startup healer looks for.
/// A legacy row written before records existed carries `NULL` until the healer
/// either finds its record in the segments or mints one.
const ADD_RECORD_ID_COLUMN: &str = "ALTER TABLE signals ADD COLUMN record_id TEXT";

/// The change feed's page key, for a database whose `signals` table predates
/// it. Nullable because an `ALTER` cannot add a `NOT NULL` column without a
/// constant default, and no constant is correct here — every row needs its own
/// value. [`BACKFILL_SEQ_COLUMN`] supplies them immediately after.
const ADD_SEQ_COLUMN: &str = "ALTER TABLE signals ADD COLUMN seq INTEGER";

/// Give every pre-existing row a page key, in the order the table already
/// holds them.
///
/// `rowid` is the insertion order this projection wrote them in, which is the
/// order they should be delivered in, so seeding from it is exact rather than
/// approximate. Runs at open, before anything reads the feed, and matches
/// nothing once every row has a key.
const BACKFILL_SEQ_COLUMN: &str = "UPDATE signals SET seq = rowid WHERE seq IS NULL";

/// The page key a newly inserted row takes: one past the largest key its space
/// already holds. Binds one parameter, the space id.
///
/// Shared by every path that inserts a signal row, so the two cannot mint keys
/// by different rules — which would be invisible until a reader silently
/// stopped receiving one path's writes.
pub(crate) const NEXT_SEQ_EXPR: &str =
    "(SELECT IFNULL(MAX(seq), 0) + 1 FROM signals WHERE space_id = ?)";

/// Retire the space+timestamp index. It served an ordered scan the startup
/// healer made over a space's rows; the healer is gone (startup now clears
/// recordless rows instead of re-minting records for them), no remaining
/// query orders by `(space_id, timestamp)`, and an unused index is a write
/// per insert for no read. Dropped rather than merely no-longer-created so
/// existing databases shed it too.
const DROP_SIGNALS_TS_INDEX: &str = "DROP INDEX IF EXISTS idx_signals_space_ts";

/// The change feed's index: one space's rows in page-key order, which is the
/// only ordering [`SqliteChangeBackend::fetch_signals`] asks for. It also
/// answers the `MAX(seq)` probe each insert makes to mint the next key, in one
/// index seek rather than a scan — which is why the key is minted per space
/// rather than table-wide. Nothing compares two spaces' keys: a cursor belongs
/// to one `(did, space_id)` and every read is space-scoped.
const CREATE_SIGNALS_SEQ_INDEX: &str = "\
    CREATE INDEX IF NOT EXISTS idx_signals_space_seq \
    ON signals (space_id, seq)";

/// PARTIAL index over exactly the healer's question: which rows have no record
/// behind them?
///
/// Partial rather than a plain index on `record_id`, because the query tests
/// **absence**. On a healthy relay this index is empty, so the startup scan
/// reads nothing — which is the whole point. The alternative is folding every
/// segment record in every space, building a `HashSet` of every signal id, and
/// diffing it against every projection row, at every boot, before the actor
/// loop starts, only to discover that nothing is missing.
const CREATE_SIGNALS_RECORDLESS_INDEX: &str = "\
    CREATE INDEX IF NOT EXISTS idx_signals_recordless \
    ON signals (space_id) WHERE record_id IS NULL";

/// The signal columns selected by every `signals` query, in `SignalRow` order.
/// Centralized so the change-feed and detail queries cannot drift.
const SIGNAL_COLUMNS: &str = "seq, id, space_id, document_id, author_did, signal_type, \
     timestamp, flag_kind, audience, message, target_did, \
     anchor_text, parent_signal_id, parent_reply_id, body, closed_at, close_reason, \
     decision_title_hash, decision_title, decision_depth, \
     decision_parent_hash";

/// The registry columns selected by every `registry` query, in `RegistryRow` order.
const REGISTRY_COLUMNS: &str = "document_id, path, created_by, created_at, \
     renamed_by, renamed_at, deleted_at, edited_at, originally_created_at, \
     rename_causal_floor_at";

/// SQL expression for a registry row's effective change time: the most recent
/// of its lifecycle timestamps. Used as the single keyset ordering key so the
/// WHERE filter, ORDER BY, and cursor advancement all agree on one key —
/// computing them from separate expressions (a 4-way OR filter, a `COALESCE`
/// ordering, a `max(...)` advance) yields three keys that can disagree.
const REGISTRY_EFFECTIVE_TS: &str =
    "MAX(created_at, COALESCE(renamed_at, 0), COALESCE(deleted_at, 0), COALESCE(edited_at, 0))";

/// Outcome of trimming a fetched page to complete ordering-key groups.
///
/// Public so the proprietary `kutlhub-relay` Postgres change backend can reuse
/// the same gap-free keyset advance as the OSS sqlite backend.
pub enum PageTrim<T> {
    /// The page is gap-free as returned; advance the cursor to `new_cursor`.
    Complete { rows: Vec<T>, new_cursor: i64 },
    /// The entire full page shares one ordering key, so more rows may share it
    /// beyond the page limit. The caller must re-fetch the whole `boundary`
    /// group before advancing the cursor to it.
    Overflow { boundary: i64 },
}

/// Decide how to advance a keyset cursor over a page ordered ascending by an
/// integer key, without ever dropping rows that share the boundary key.
///
/// A naive cursor advances to the last row's key and re-queries with strict
/// `>`. If a group sharing the boundary key straddles the page limit, the rows
/// past the cut are skipped forever (silent, unrecoverable loss). This trims
/// the trailing (possibly incomplete) key group: every row with a smaller key
/// is fully present, because the page is contiguous and ordered, so it is safe
/// to deliver and to advance the cursor to. The trailing group is re-fetched on
/// the next call. When the whole full page is a single key group, the caller
/// must drain that group explicitly (`Overflow`) — trimming would make no
/// progress.
///
/// Public so the proprietary `kutlhub-relay` Postgres change backend can reuse
/// it (see `PageTrim`).
pub fn trim_to_complete_groups<T>(
    rows: Vec<T>,
    limit: usize,
    cursor: i64,
    key: impl Fn(&T) -> i64,
) -> PageTrim<T> {
    let Some(last) = rows.last() else {
        return PageTrim::Complete {
            rows,
            new_cursor: cursor,
        };
    };
    let last_key = key(last);

    // Short page: the query exhausted everything past the cursor, so the whole
    // range is present and nothing can straddle a (non-existent) next page.
    if rows.len() < limit {
        return PageTrim::Complete {
            rows,
            new_cursor: last_key,
        };
    }

    // Full page entirely within one key group: more rows may share it past the
    // cut, so the caller must drain the group.
    if key(&rows[0]) == last_key {
        return PageTrim::Overflow { boundary: last_key };
    }

    // Drop the trailing (possibly incomplete) group; smaller keys are complete.
    let kept: Vec<T> = rows.into_iter().filter(|r| key(r) < last_key).collect();
    let new_cursor = kept.iter().map(&key).max().unwrap_or(cursor);
    PageTrim::Complete {
        rows: kept,
        new_cursor,
    }
}

/// Run an idempotent `ALTER TABLE … ADD COLUMN`, swallowing only `SQLite`'s
/// duplicate-column error (the column already exists on a fresh DB) and
/// propagating everything else (DB locked, disk full, permission denied) —
/// swallowing the whole error class would mask real persistence problems.
///
/// We deliberately don't introspect `PRAGMA table_info` first: the
/// catch-and-ignore is shorter, and a missing table is already handled by
/// the preceding `CREATE TABLE IF NOT EXISTS`.
async fn add_column_if_absent(pool: &SqlitePool, alter_sql: &str) -> Result<(), ChangeError> {
    if let Err(e) = sqlx::query(alter_sql).execute(pool).await
        && !e.to_string().contains("duplicate column name")
    {
        return Err(e.into());
    }
    Ok(())
}

/// Rebuild a legacy `signals` table whose PRIMARY KEY is `id` alone into the
/// composite `(space_id, id)` shape ([[`SIGNALS_TABLE_BODY`]]), preserving every
/// existing row.
///
/// `SQLite` cannot ALTER a PRIMARY KEY in place, so this does the standard
/// table-rebuild dance inside one transaction: create the new-schema table,
/// copy every column across, drop the old table, rename the new one into place,
/// then re-create the index. It is **rebuild-COPY** (not rebuild-empty): the
/// existing rows are carried over verbatim, keyed under their stored `space_id`,
/// so no row is ever left unqueryable and correctness does not depend on a later
/// re-projection pass running.
///
/// Idempotent + guarded: it inspects `PRAGMA table_info(signals)` and runs the
/// rebuild ONLY when the `id` column is the sole PK member. On a fresh DB (or a
/// DB already migrated) the composite PK is present, the guard trips, and the
/// function no-ops. Safe to call on every startup.
/// Discard a `change_cursors` table whose signal cursor is a millisecond.
///
/// A cursor row says where a reader got to, in the units the feed pages on.
/// The feed pages on `signals.seq`, and a millisecond value is astronomically
/// larger than any of them: kept, such a row would sit above every signal the
/// space will ever hold and that reader would be handed nothing, forever,
/// without an error. Dropping the table resets every reader to the start of
/// its space, which re-delivers records some of them have already seen — the
/// recoverable direction, and the only one available, since a millisecond
/// cannot be translated into a page key.
///
/// A cursor is a resumption hint, not a record: the reaper already discards
/// them by age, so losing them costs a re-read and nothing else.
///
/// The legacy column IS the marker. The recreated table does not carry it, so
/// this matches once and never again.
async fn drop_cursors_keyed_on_a_millisecond(pool: &SqlitePool) -> Result<(), ChangeError> {
    let columns: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM pragma_table_info('change_cursors')")
            .fetch_all(pool)
            .await?;
    if columns.iter().any(|(name,)| name == "signal_cursor") {
        tracing::info!(
            "change feed cursors are keyed on a millisecond; discarding them so readers \
             resume on the projection's page key"
        );
        sqlx::query("DROP TABLE change_cursors")
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn migrate_signals_composite_pk(pool: &SqlitePool) -> Result<(), ChangeError> {
    // PRAGMA table_info returns one row per column; the `pk` field is the
    // 1-based position of the column within the PRIMARY KEY (0 = not part of
    // the PK). The legacy shape has exactly `id` with pk=1 and `space_id` with
    // pk=0; the new shape has both `space_id` and `id` with pk>0.
    let columns: Vec<(String, i64)> =
        sqlx::query_as("SELECT name, pk FROM pragma_table_info('signals')")
            .fetch_all(pool)
            .await?;

    // No table yet (shouldn't happen — CREATE runs first) → nothing to migrate.
    if columns.is_empty() {
        return Ok(());
    }
    let space_in_pk = columns
        .iter()
        .any(|(name, pk)| name == "space_id" && *pk > 0);
    if space_in_pk {
        // Already composite-keyed — nothing to do.
        return Ok(());
    }

    // Legacy id-only PK: rebuild-copy into the composite-PK shape. One
    // transaction so a crash mid-rebuild cannot leave a half-migrated table.
    let mut tx = pool.begin().await?;
    sqlx::query(&format!("CREATE TABLE signals_new {SIGNALS_TABLE_BODY}"))
        .execute(&mut *tx)
        .await?;
    // `deleted_at` is in this list for a reason worth stating: it is the
    // soft-delete marker, so omitting it would carry every row across as
    // VISIBLE and silently un-delete every tombstoned signal. A rebuild-copy
    // that drops a column is indistinguishable from one that never had it.
    sqlx::query(&format!(
        "INSERT INTO signals_new ({SIGNAL_COLUMNS}, record_id, deleted_at) \
         SELECT {SIGNAL_COLUMNS}, record_id, deleted_at FROM signals"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query("DROP TABLE signals").execute(&mut *tx).await?;
    sqlx::query("ALTER TABLE signals_new RENAME TO signals")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// A registry row's effective change time (matches [`REGISTRY_EFFECTIVE_TS`]).
fn registry_effective_ts(r: &RegistryRow) -> i64 {
    r.created_at
        .max(r.renamed_at.unwrap_or(0))
        .max(r.deleted_at.unwrap_or(0))
        .max(r.edited_at.unwrap_or(0))
}

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
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS signals {SIGNALS_TABLE_BODY}"
        ))
        .execute(&pool)
        .await?;

        // Idempotent column ALTERs for pre-existing self-hosted relay DBs.
        // On a fresh DB the columns already exist (via CREATE above), so the
        // duplicate-column error is expected and swallowed; any other error
        // (DB locked, disk full, permission denied) propagates.
        for alter in [
            ADD_ANCHOR_TEXT_COLUMN,
            ADD_CLOSED_AT_COLUMN,
            ADD_CLOSE_REASON_COLUMN,
            ADD_DECISION_TITLE_HASH_COLUMN,
            ADD_DECISION_TITLE_COLUMN,
            ADD_DECISION_DEPTH_COLUMN,
            ADD_DECISION_PARENT_HASH_COLUMN,
            ADD_RECORD_ID_COLUMN,
            ADD_DELETED_AT_COLUMN,
            ADD_SEQ_COLUMN,
        ] {
            add_column_if_absent(&pool, alter).await?;
        }

        // Before anything reads the feed and before the rebuild below copies
        // the column, so every row carries a page key from here on.
        sqlx::query(BACKFILL_SEQ_COLUMN).execute(&pool).await?;

        // Rebuild a legacy id-only-PK `signals` table into the composite
        // `(space_id, id)` PK shape (runs after the ALTERs so every column
        // exists to copy; guarded + idempotent — no-ops on a fresh/migrated DB).
        migrate_signals_composite_pk(&pool).await?;

        // The active-signal view, created LAST: the composite-PK rebuild above
        // does `DROP TABLE signals`, and a view built before that would be left
        // dangling — SQLite reports "no such table: main.signals" the next time
        // anything selects from it. Dropped and recreated on every startup so
        // its column list always matches the table it was just built over.
        sqlx::query(DROP_SIGNALS_ACTIVE_VIEW).execute(&pool).await?;
        sqlx::query(&signals_active_view_sql())
            .execute(&pool)
            .await?;

        sqlx::query(DROP_SIGNALS_TS_INDEX).execute(&pool).await?;
        sqlx::query(CREATE_SIGNALS_SEQ_INDEX).execute(&pool).await?;
        sqlx::query(CREATE_SIGNALS_RECORDLESS_INDEX)
            .execute(&pool)
            .await?;

        drop_cursors_keyed_on_a_millisecond(&pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS change_cursors (
                did              TEXT    NOT NULL,
                space_id         TEXT    NOT NULL,
                signal_seq       INTEGER NOT NULL DEFAULT 0,
                reg_cursor       INTEGER NOT NULL DEFAULT 0,
                prev1_signal_seq INTEGER NOT NULL DEFAULT 0,
                prev1_reg        INTEGER NOT NULL DEFAULT 0,
                prev2_signal_seq INTEGER NOT NULL DEFAULT 0,
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
    /// The projection's append counter for this row: the change feed's page
    /// key, read back so a page can advance the cursor to its last row.
    seq: i64,
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
    /// Comment-kind posterity snapshot. `None` for non-comment flags.
    anchor_text: Option<String>,
    parent_signal_id: Option<String>,
    parent_reply_id: Option<String>,
    body: Option<String>,
    /// Derived close-state: close time in Unix millis when the
    /// signal folds to `Closed`, `None` when it folds to `Open`.
    closed_at: Option<i64>,
    /// The winning close's reason discriminant, alongside `closed_at`. Served
    /// on the change feed so a re-surfaced closed row can say WHY it closed;
    /// `SignalDetail` derives its label elsewhere.
    close_reason: Option<i64>,
    /// `Payload::Decision` fields — populated only for decision rows
    /// (`signal_type = "decision"`), `None` for every other kind. Reconstructed
    /// into a `DecisionPayload` by [`SignalRow::into_proto`].
    decision_title_hash: Option<String>,
    decision_title: Option<String>,
    decision_depth: Option<i32>,
    decision_parent_hash: Option<String>,
}

impl SignalRow {
    /// Convert a flat row into a proto `Signal` with the correct payload variant.
    fn into_proto(self) -> kutl_proto::sync::Signal {
        use kutl_proto::sync::signal::Payload;

        // Classify every payload kind explicitly. A row whose `signal_type`
        // matches no known kind materializes with no payload rather than
        // silently masquerading as another kind (a catch-all arm would
        // project chats and decisions as blank replies).
        let payload = match self.signal_type.as_str() {
            // A read adapter, not a record writer: this `Signal` is served to
            // the read path and never appended to a log, so it keeps emitting
            // the deprecated audience pair that the Connect surface still
            // reads.
            #[allow(deprecated)]
            "flag" => Some(Payload::Flag(kutl_proto::sync::FlagPayload {
                kind: self
                    .flag_kind
                    .unwrap_or(kutl_proto::vocab::FLAG_KIND_UNSPECIFIED),
                audience_type: self
                    .audience
                    .unwrap_or(kutl_proto::vocab::AUDIENCE_TYPE_UNSPECIFIED),
                target_did: self.target_did,
                message: self.message.unwrap_or_default(),
                audience: None,
                // Meaningful for `FLAG_KIND_COMMENT` only, and NULL for every
                // other kind — the same shape the column has. A comment is a flag
                // kind, so it needs no arm of its own here.
                anchor_text: self.anchor_text,
            })),
            "reply" => Some(Payload::Reply(kutl_proto::sync::ReplyPayload {
                parent_signal_id: self.parent_signal_id.unwrap_or_default(),
                parent_reply_id: self.parent_reply_id,
                body: self.body.unwrap_or_default(),
            })),
            // `Payload::Chat` reuses `message` for the topic and `target_did`.
            "chat" => Some(Payload::Chat(kutl_proto::sync::ChatPayload {
                topic: self.message,
                target_did: self.target_did,
            })),
            "decision" => Some(Payload::Decision(kutl_proto::sync::DecisionPayload {
                title_hash: self.decision_title_hash.unwrap_or_default(),
                title: self.decision_title.unwrap_or_default(),
                depth: self.decision_depth.unwrap_or(0),
                parent_hash: self.decision_parent_hash,
            })),
            unknown => {
                // Schema drift: a kind the projection can store but this build
                // cannot name. Loud, because the symptom downstream is a
                // payload-less signal rather than an error.
                tracing::warn!(
                    signal_id = %self.id,
                    signal_type = unknown,
                    "signal row has an unrecognized signal_type; projecting with no payload"
                );
                None
            }
        };

        // The shared serve core owns field placement and the close-state
        // carry; this backend only assembles the payload from its columns.
        crate::change_backend::ServedSignalRow {
            id: self.id,
            space_id: self.space_id,
            document_id: self.document_id,
            author_did: self.author_did,
            created_at_ms: self.timestamp,
            payload,
            closed: self
                .closed_at
                .is_some()
                .then(|| crate::change_backend::ServedClose {
                    reason: self
                        .close_reason
                        .and_then(|r| i32::try_from(r).ok())
                        .unwrap_or_default(),
                }),
        }
        .into_proto()
    }
}

/// Row type for reading the cursor ring from `SQLite`.
#[derive(sqlx::FromRow)]
struct CursorRow {
    /// The `signals.seq` of the last row delivered to this reader.
    signal_seq: i64,
    reg_cursor: i64,
    prev1_signal_seq: i64,
    prev1_reg: i64,
    prev2_signal_seq: i64,
    prev2_reg: i64,
    checkpoint: String,
}

impl SqliteChangeBackend {
    /// Fetch signals past `cursor` for the given space, and return the cursor
    /// to resume from.
    ///
    /// The cursor is a `signals.seq` — the projection's append counter for this
    /// space, assigned when a row is inserted. It is a TOTAL order over the
    /// space's rows and it agrees with the order the projection learned them
    /// in, which is what makes the feed deliverable exactly once: whatever is
    /// committed after a read carries a key above the cursor that read handed
    /// out, so it is reachable on the next call regardless of what its
    /// timestamp says or how its id sorts.
    ///
    /// Paging on `(timestamp, id)` instead cannot hold. Two records inside one
    /// millisecond are ordinary — the relay ticks its clock within a
    /// millisecond so records emit in sequence order — and ids are random
    /// UUIDs, so a row committed after a read but inside the cursor's
    /// millisecond sorts below the cursor about half the time. It is then below
    /// the keyset forever: the write still wakes the caller, and the read that
    /// follows returns nothing.
    ///
    /// `seq` is unique within a space, so a page advances to its last row
    /// unconditionally. Nothing can straddle the page limit and nothing needs
    /// draining.
    ///
    /// The delivery order is therefore the order this projection accepted
    /// records, not the order their authors timestamped them. For a feed that
    /// answers "what is new to me" those are the same thing for locally
    /// authored records, and for a record replicated from a peer the arrival
    /// order is the honest one.
    async fn fetch_signals(
        &self,
        space_id: &str,
        cursor: i64,
    ) -> Result<(Vec<kutl_proto::sync::Signal>, i64), ChangeError> {
        let rows = sqlx::query_as::<_, SignalRow>(&format!(
            "SELECT {SIGNAL_COLUMNS} FROM signals_active \
             WHERE space_id = ? AND seq > ? \
             ORDER BY seq ASC LIMIT ?"
        ))
        .bind(space_id)
        .bind(cursor)
        .bind(i64::try_from(MAX_EVENTS_PER_QUERY).expect("page size fits i64"))
        .fetch_all(&self.pool)
        .await?;

        let new_cursor = rows.last().map_or(cursor, |last| last.seq);

        let protos = rows.into_iter().map(SignalRow::into_proto).collect();
        Ok((protos, new_cursor))
    }

    /// Fetch registry entries changed since `reg_cursor` for the given space.
    ///
    /// Orders, filters, and advances the cursor on a single effective key
    /// ([`REGISTRY_EFFECTIVE_TS`]). That key is not unique, so unlike the
    /// signal feed this one cannot simply resume at its last row: rows sharing
    /// the boundary key are kept whole across the page limit by
    /// [`trim_to_complete_groups`].
    async fn fetch_registry_changes(
        &self,
        space_id: &str,
        reg_cursor: i64,
    ) -> Result<(Vec<RegistryEntry>, i64), ChangeError> {
        let page = sqlx::query_as::<_, RegistryRow>(&format!(
            "SELECT {REGISTRY_COLUMNS} FROM registry \
             WHERE space_id = ? AND {REGISTRY_EFFECTIVE_TS} > ? \
             ORDER BY {REGISTRY_EFFECTIVE_TS} ASC LIMIT ?"
        ))
        .bind(space_id)
        .bind(reg_cursor)
        .bind(i64::try_from(MAX_EVENTS_PER_QUERY).expect("page size fits i64"))
        .fetch_all(&self.pool)
        .await?;

        let (rows, new_cursor) = match trim_to_complete_groups(
            page,
            MAX_EVENTS_PER_QUERY,
            reg_cursor,
            registry_effective_ts,
        ) {
            PageTrim::Complete { rows, new_cursor } => (rows, new_cursor),
            PageTrim::Overflow { boundary } => {
                let rows = sqlx::query_as::<_, RegistryRow>(&format!(
                    "SELECT {REGISTRY_COLUMNS} FROM registry \
                     WHERE space_id = ? AND {REGISTRY_EFFECTIVE_TS} = ? \
                     ORDER BY document_id ASC"
                ))
                .bind(space_id)
                .bind(boundary)
                .fetch_all(&self.pool)
                .await?;
                (rows, boundary)
            }
        };

        let entries = rows.into_iter().map(RegistryRow::into_entry).collect();
        Ok((entries, new_cursor))
    }

    /// Upsert the cursor row, shifting the checkpoint ring.
    async fn save_cursors(
        &self,
        did: &str,
        space_id: &str,
        signal_seq: i64,
        reg_cursor: i64,
        checkpoint: &str,
    ) -> Result<(), ChangeError> {
        // The ring slots hold whole positions, the same two numbers the
        // checkpoint string is made of. A slot that stored part of a position
        // would leave a rewind to infer the rest, and the only thing it could
        // infer it from is where the reader has since got to.
        sqlx::query(
            "INSERT INTO change_cursors
                (did, space_id, signal_seq, reg_cursor,
                 prev1_signal_seq, prev1_reg, prev2_signal_seq, prev2_reg,
                 checkpoint, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (did, space_id) DO UPDATE SET
                prev2_signal_seq = change_cursors.prev1_signal_seq,
                prev2_reg        = change_cursors.prev1_reg,
                prev1_signal_seq = change_cursors.signal_seq,
                prev1_reg        = change_cursors.reg_cursor,
                signal_seq       = excluded.signal_seq,
                reg_cursor       = excluded.reg_cursor,
                checkpoint       = excluded.checkpoint,
                updated_at       = excluded.updated_at",
        )
        .bind(did)
        .bind(space_id)
        .bind(signal_seq)
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

    /// List the distinct `space_id`s that have at least one `signals` row.
    /// Feeds the startup pass's space set: a space whose every segment was
    /// lost still has rows to reconcile against, and would be invisible to a
    /// segments-only enumeration.
    ///
    /// # Errors
    ///
    /// Returns [`ChangeError::Db`] if the query fails.
    pub async fn spaces_with_signal_rows(&self) -> Result<Vec<String>, ChangeError> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT DISTINCT space_id FROM signals")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|(s,)| s).collect())
    }

    /// Delete every projection row that has no record behind it — the
    /// startup phantom-clear.
    ///
    /// A `signals` row can outlive its record in exactly one way: the
    /// admission seam appends to segments FIRST and projects SECOND, but
    /// per-record segment appends are not fsynced while the row's commit is,
    /// so a power cut inside the writeback window can keep the row and lose
    /// the append. The ack a caller waits on is sent only after replication,
    /// which follows projection — so a row with no record behind it is, by
    /// construction, residue of a create nobody heard succeed. The remedy is
    /// deletion, not re-minting: if the lost record reached any replica
    /// before the cut, re-seed restores it (with its ORIGINAL `record_id`)
    /// and the projection row is rebuilt from it; if it reached no one, the
    /// author was never acked and retries. A re-minted stand-in would be
    /// wrong in both cases — a duplicate identity in the first, a
    /// resurrected failure in the second.
    ///
    /// Left in place, such a row is a ghost: visible to every projection
    /// read, and blocking a same-id retry at the authored seam's
    /// duplicate-id check. The startup rebuild cannot remove it —
    /// [`Self::reconcile_space_projection`] deliberately never deletes a row
    /// merely absent from the fold, because re-seed shares that code and a
    /// re-seed batch is not a complete fold. This pass runs only at startup,
    /// where `present` IS the complete local fold, which is what makes the
    /// deletion safe here and nowhere else.
    ///
    /// # Why the presence test is a fold and not the indexed `record_id` column
    ///
    /// `signals.record_id` records which record a row was projected from, and
    /// `WHERE record_id IS NULL` looks like it should replace this scan. It
    /// cannot: `record_signal` writes `record_id` at create time, in the same
    /// commit as the row, so a row whose record was then lost still carries
    /// one. The column answers "was a record id ever recorded for this row",
    /// which is not the same question as "does that record still exist".
    ///
    /// Idempotent: a space whose rows all have records deletes nothing.
    /// Returns the cleared ids so the caller can log them.
    ///
    /// # Errors
    ///
    /// Returns [`ChangeError::Db`] if the row read or a delete fails.
    pub async fn clear_recordless_rows(
        &self,
        space_id: &str,
        present: &[kutl_proto::sync::Signal],
    ) -> Result<Vec<String>, ChangeError> {
        let present_ids: std::collections::HashSet<&str> =
            present.iter().map(|r| r.id.as_str()).collect();
        let rows: Vec<(String,)> = sqlx::query_as("SELECT id FROM signals WHERE space_id = ?")
            .bind(space_id)
            .fetch_all(&self.pool)
            .await?;
        let mut cleared = Vec::new();
        for (id,) in rows {
            if present_ids.contains(id.as_str()) {
                continue;
            }
            sqlx::query("DELETE FROM signals WHERE space_id = ? AND id = ?")
                .bind(space_id)
                .bind(&id)
                .execute(&self.pool)
                .await?;
            cleared.push(id);
        }
        Ok(cleared)
    }

    /// Upsert one signal row from its folded state.
    ///
    /// The base columns come from the folded CREATED record (payload identity
    /// is min-order-wins, order-insensitive); `closed_at` / `close_reason` come
    /// from the fold's winning transition. On conflict, base + close columns are
    /// refreshed but `anchor_text` is PRESERVED: the posterity snapshot
    /// is display-only metadata that never rides in the segment record, so a
    /// segment-derived rebuild has no value to write and must not clobber the
    /// one the create path recorded (`INSERT`s a segment-only signal with a NULL
    /// anchor, which is correct — catch-up signals never had one).
    async fn upsert_projected_signal(&self, row: &ProjectedRow<'_>) -> Result<(), ChangeError> {
        bind_projected_insert(row).execute(&self.pool).await?;
        Ok(())
    }

    /// Hide the projection row for a tombstoned signal,
    /// scoped to `(space_id, id)` — the projection PRIMARY KEY.
    ///
    /// A signal id is unique only WITHIN a space, so the delete MUST be
    /// space-scoped: an id-alone delete would let a tombstone folded in one
    /// space remove a same-id row belonging to another. A no-op when no row
    /// exists (a signal tombstoned before it was ever projected).
    async fn hide_projected_signal(
        &self,
        space: crate::projection::AdmittedSpace<'_>,
        id: &str,
        at_ms: i64,
    ) -> Result<(), ChangeError> {
        // Only the FIRST tombstone wins, so a redelivered record does not move
        // the timestamp — matching the Postgres arm's `AND deleted_at IS NULL`.
        sqlx::query(
            "UPDATE signals SET deleted_at = ? \
             WHERE space_id = ? AND id = ? AND deleted_at IS NULL",
        )
        .bind(at_ms)
        .bind(space.as_str())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record a CLOSED transition on the projection row.
    ///
    /// Scoped to `(space_id, id)`, matching the composite primary key, and the
    /// space is the ADMITTING seam's verdict — never `record.space_id`, a
    /// peer's claim on the replicated path — so a transition admitted into one
    /// space can never close a same-id signal in another. A row that is not
    /// there yet is an orphan transition, which the fold parks until its
    /// `CREATED` arrives — so zero rows updated is the expected outcome, not a
    /// failure.
    async fn close_projected_signal(
        &self,
        space: crate::projection::AdmittedSpace<'_>,
        id: &str,
        facts: &crate::projection::CloseFacts<'_>,
    ) -> Result<(), ChangeError> {
        // `seq` moves to the head of the feed WHEN THE STATE CHANGES (the
        // null-safe `IS NOT` guard): the change feed pages on `seq`, so a
        // close that kept its birth key would never surface to a reader whose
        // cursor already passed it. The guard is what keeps an idempotent
        // replay — a rebuild re-projecting history — from re-delivering rows
        // that did not change.
        sqlx::query(&format!(
            "UPDATE signals SET closed_at = ?, close_reason = ?, seq = {NEXT_SEQ_EXPR}
             WHERE space_id = ? AND id = ?
               AND (closed_at IS NOT ? OR close_reason IS NOT ?)"
        ))
        .bind(facts.closed_at_ms)
        // The discriminant verbatim, which is what the fold stores for the
        // winning transition — so a rebuild does not disagree with this write.
        .bind(i32::from(facts.reason))
        .bind(space.as_str())
        .bind(space.as_str())
        .bind(id)
        .bind(facts.closed_at_ms)
        .bind(i32::from(facts.reason))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Clear the close state a CLOSED transition left. Companion to
    /// [`Self::close_projected_signal`], with the same key scoping and the same
    /// tolerance for an orphan.
    async fn reopen_projected_signal(
        &self,
        space: crate::projection::AdmittedSpace<'_>,
        id: &str,
    ) -> Result<(), ChangeError> {
        // Clears `deleted_at` as well as the close state. A reopen is how the
        // document-revive cascade brings a tombstoned signal back, so a reopen
        // that left the row hidden would revive the document and silently leave
        // its signals invisible — which is exactly what Postgres did until this
        // change, because nothing there ever cleared `deleted_at`.
        // Re-sequenced like a close, and guarded the same way: only a reopen
        // that actually clears something moves the row in the feed.
        sqlx::query(&format!(
            "UPDATE signals SET closed_at = NULL, close_reason = NULL, deleted_at = NULL,
                    seq = {NEXT_SEQ_EXPR}
             WHERE space_id = ? AND id = ?
               AND (closed_at IS NOT NULL OR close_reason IS NOT NULL
                    OR deleted_at IS NOT NULL)"
        ))
        .bind(space.as_str())
        .bind(space.as_str())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Overlay an EDITED record's content onto the projection row.
    ///
    /// Same key scoping and orphan tolerance as the transition helpers: zero
    /// rows updated means the CREATED has not projected yet, and the
    /// fold-backed rebuild reconciles content from the record log. Lifecycle
    /// columns are untouched — an edit never reopens.
    ///
    /// Overlay semantics come from [`ContentOverlay`]: carried fields
    /// replace, absent fields persist, a decision payload replaces all four
    /// decision columns outright.
    async fn edit_projected_signal(
        &self,
        space: crate::projection::AdmittedSpace<'_>,
        record: &kutl_proto::sync::Signal,
    ) -> Result<(), ChangeError> {
        let overlay = ContentOverlay::from_record(record);
        if let Some(d) = &overlay.decision {
            sqlx::query(&format!(
                "UPDATE signals SET decision_title_hash = ?, decision_title = ?,
                     decision_depth = ?, decision_parent_hash = ?, seq = {NEXT_SEQ_EXPR}
                 WHERE space_id = ? AND id = ?
                   AND (decision_title_hash IS NOT ? OR decision_title IS NOT ?
                        OR decision_depth IS NOT ? OR decision_parent_hash IS NOT ?)"
            ))
            .bind(d.title_hash)
            .bind(d.title)
            .bind(d.depth)
            .bind(d.parent_hash)
            .bind(space.as_str())
            .bind(space.as_str())
            .bind(record.id.as_str())
            .bind(d.title_hash)
            .bind(d.title)
            .bind(d.depth)
            .bind(d.parent_hash)
            .execute(&self.pool)
            .await?;
        } else {
            // The guard compares the EFFECTIVE post-COALESCE values, so an
            // overlay carrying only unchanged (or absent) fields is a no-op
            // that leaves the row where the feed already served it.
            sqlx::query(&format!(
                "UPDATE signals SET message = COALESCE(?, message),
                     anchor_text = COALESCE(?, anchor_text),
                     body = COALESCE(?, body), seq = {NEXT_SEQ_EXPR}
                 WHERE space_id = ? AND id = ?
                   AND (COALESCE(?, message) IS NOT message
                        OR COALESCE(?, anchor_text) IS NOT anchor_text
                        OR COALESCE(?, body) IS NOT body)"
            ))
            .bind(overlay.message)
            .bind(overlay.anchor_text)
            .bind(overlay.body)
            .bind(space.as_str())
            .bind(space.as_str())
            .bind(record.id.as_str())
            .bind(overlay.message)
            .bind(overlay.anchor_text)
            .bind(overlay.body)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// Re-align a space's projection rows to the fold of its segment `records`
    /// at startup, NON-destructively (the crash-tail heal).
    ///
    /// For every folded signal this UPSERTS its row (refreshing close-state to
    /// match the fold and PRESERVING `anchor_text`) and DELETES rows whose fold
    /// status is Tombstoned — the SAME upsert + targeted tombstone-delete
    /// [`Self::project_records`] uses. A row merely ABSENT from `records` is
    /// PRESERVED, never deleted: it is either an un-backfilled legacy row (the
    /// startup backfill is skip-and-warned per space, so a transient IO fault
    /// leaves rows un-minted) or a crash-tail-lost CREATE — in both cases the
    /// projection row is the SOLE surviving copy of that acked user data, and
    /// backfill reads its source from this same `signals` table, so deleting it
    /// would be unrecoverable data loss. A crash-tail-lost CREATE thus leaves a
    /// row the segments lack (catch-up peers won't receive it — the inherent
    /// no-fsync reality), which is strictly better than losing the user's data.
    ///
    /// It still HEALS the close-state divergence it was built for: segment
    /// appends are not fsynced per record, so a power loss can drop the tail of
    /// the active segment AFTER the projection row was committed. The classic
    /// case is a CREATED-then-CLOSED signal whose CLOSED record is lost: the
    /// fold now says Open, and the upsert refreshes the stale row's close-state
    /// (`closed_at` cleared) to match — the per-mutation `project_records` upsert
    /// does the same, but this is the one-shot startup pass that re-derives every
    /// signal's close-state from the (post-crash) segment truth.
    ///
    /// Idempotent, order-insensitive, and byte-identical to
    /// [`Self::project_records`] over the loaded segments (it delegates to it),
    /// so it does not fight the per-mutation projection. Rows are keyed to the
    /// AUTHORITATIVE `space_id`, never the record's own `space_id` field.
    ///
    /// # Errors
    ///
    /// Returns [`ChangeError::Db`] if an upsert or a tombstone-delete fails.
    pub async fn reconcile_space_projection(
        &self,
        space_id: &str,
        records: &[kutl_proto::sync::Signal],
    ) -> Result<(), ChangeError> {
        // Startup reconcile and re-seed want identical semantics: upsert every
        // folded signal (preserving anchor_text via ON CONFLICT) and delete
        // only tombstoned ids, NEVER a row merely absent from the fold.
        // Delegate so the two paths cannot drift apart.
        ProjectionRebuild::rebuild_space(self, space_id, records).await
    }
}

/// Build the bound INSERT-or-upsert for one projection row, ready to execute
/// against any executor (the shared pool for [`SqliteChangeBackend::upsert_projected_signal`],
/// or a transaction for [`SqliteChangeBackend::reconcile_space_projection`]).
///
/// The `ON CONFLICT (space_id, id) DO UPDATE` refreshes every derived column.
/// `anchor_text` refreshes CONDITIONALLY, via `COALESCE(excluded, existing)`,
/// and the asymmetry is deliberate:
///
/// - A comment record carries its own anchor excerpt,
///   so a rebuild has a value to write and an edited anchor must be able to
///   refresh. An unconditional preserve would pin the first value forever.
/// - A legacy comment — `FlagKind::COMMENT`, anchor written only to the
///   projection by the create path — carries nothing, so an unconditional
///   refresh would null out every existing anchor on the first rebuild — real
///   data loss for records already on disk.
///
/// `COALESCE` satisfies both: a record that supplies a value wins, a record that
/// supplies none leaves the stored value alone. The one case it cannot express
/// is deliberately clearing an anchor, which no path asks for — a document-level
/// comment never had one to clear.
///
/// Centralizing the column list + bind order here keeps the two call sites from
/// drifting apart.
fn bind_projected_insert<'q>(
    row: &'q ProjectedRow<'q>,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    sqlx::query(&PROJECTED_INSERT_SQL)
        .bind(row.space_id)
        .bind(row.id)
        .bind(row.space_id)
        .bind(row.document_id)
        .bind(row.author_did)
        .bind(row.signal_type)
        .bind(row.timestamp)
        .bind(row.flag_kind)
        .bind(row.audience)
        .bind(row.message)
        .bind(row.target_did)
        .bind(row.anchor_text)
        .bind(row.parent_signal_id)
        .bind(row.parent_reply_id)
        .bind(row.body)
        .bind(row.closed_at)
        .bind(row.close_reason)
        .bind(row.decision_title_hash)
        .bind(row.decision_title)
        .bind(row.decision_depth)
        .bind(row.decision_parent_hash)
        .bind(row.record_id)
        .bind(row.deleted_at)
}

/// The projection upsert, composed once so [`NEXT_SEQ_EXPR`] can be shared with
/// the other insert path. Held in a static because the query borrows its SQL
/// for as long as the caller holds the query.
static PROJECTED_INSERT_SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        // `seq` is absent from the DO UPDATE list: a re-projected CREATED (a
        // rebuild, a replicated duplicate) must not move the row in the feed.
        // State CHANGES are what re-deliver — the close/reopen/edit writes
        // re-sequence the row themselves, each guarded so only an actual
        // change moves it. `seq` is therefore a last-ACTIVITY key: a reader
        // pages past a row once per state it has seen, not once ever.
        "INSERT INTO signals
            (seq, id, space_id, document_id, author_did, signal_type,
             timestamp, flag_kind, audience, message, target_did,
             anchor_text, parent_signal_id, parent_reply_id, body,
             closed_at, close_reason,
             decision_title_hash, decision_title,
             decision_depth, decision_parent_hash, record_id, deleted_at)
         VALUES ({NEXT_SEQ_EXPR},
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (space_id, id) DO UPDATE SET
            document_id          = excluded.document_id,
            author_did           = excluded.author_did,
            signal_type          = excluded.signal_type,
            timestamp            = excluded.timestamp,
            flag_kind            = excluded.flag_kind,
            audience             = excluded.audience,
            message              = excluded.message,
            target_did           = excluded.target_did,
            anchor_text          = COALESCE(excluded.anchor_text, signals.anchor_text),
            parent_signal_id     = excluded.parent_signal_id,
            parent_reply_id      = excluded.parent_reply_id,
            body                 = excluded.body,
            closed_at            = excluded.closed_at,
            close_reason         = excluded.close_reason,
            decision_title_hash  = excluded.decision_title_hash,
            decision_title       = excluded.decision_title,
            decision_depth       = excluded.decision_depth,
            decision_parent_hash = excluded.decision_parent_hash,
            record_id            = excluded.record_id,
            -- Hidden-ness comes FROM THE FOLD, in both directions: a tombstone
            -- sets it, a reopen clears it, and the rebuild reaches the same
            -- answer as the incremental path for the same records. Binding it
            -- rather than hardcoding NULL is what lets the rebuild un-hide;
            -- binding it rather than branching is what guarantees a tombstoned
            -- signal still HAS a row. Both matter: `signal_exists` reads the
            -- base table precisely so that hiding a signal does not release its
            -- id, and a rebuild that left no row would hand that id back.
            deleted_at           = excluded.deleted_at"
    )
});

/// The projection columns derived from a folded [`SignalState`], borrowed
/// from the folded record for the lifetime of one upsert. Separates the
/// proto→column extraction from the SQL bind so neither can silently drift.
struct ProjectedRow<'a> {
    id: &'a str,
    space_id: &'a str,
    document_id: Option<&'a str>,
    author_did: &'a str,
    signal_type: &'static str,
    timestamp: i64,
    flag_kind: Option<i32>,
    audience: Option<i32>,
    message: Option<&'a str>,
    target_did: Option<&'a str>,
    /// `None` for every record that carries no anchor — including a legacy
    /// `FLAG_KIND_COMMENT` one, written before the record owned the field — so
    /// the upsert preserves the stored value rather than nulling it (see
    /// [`SqliteChangeBackend::upsert_projected_signal`]).
    anchor_text: Option<&'a str>,
    parent_signal_id: Option<&'a str>,
    parent_reply_id: Option<&'a str>,
    body: Option<&'a str>,
    closed_at: Option<i64>,
    /// When this signal is TOMBSTONED, the tombstone's wall-clock millisecond;
    /// `None` when it is visible. Derived from the fold like every other column
    /// here, so the rebuild expresses hidden-ness the same way it expresses
    /// close-state instead of as a separate control-flow branch.
    deleted_at: Option<i64>,
    close_reason: Option<i32>,
    /// `Payload::Decision` fields, all `None` for non-decision signals.
    decision_title_hash: Option<&'a str>,
    decision_title: Option<&'a str>,
    decision_depth: Option<i32>,
    decision_parent_hash: Option<&'a str>,
    /// The CREATED record this row was folded from. A row rebuilt from the fold
    /// demonstrably HAS a record behind it, so the rebuild must write this — a
    /// rebuild that left it null would tell the startup healer that every row it
    /// just re-derived is recordless.
    record_id: &'a str,
}

impl<'a> ProjectedRow<'a> {
    /// Build the projection row for signal `id` from its folded state, keyed to
    /// the AUTHORITATIVE `space_id` the projection was invoked for.
    ///
    /// The projection row's `space_id` comes from `space_id` (the space
    /// `project_records` was called with — the authorized path space), NEVER
    /// from `state.created.space_id`. A record's own `space_id` field is
    /// attacker-controllable on the re-seed path; binding the projection to the
    /// authoritative space is what prevents a record ingested into space A from
    /// materializing a row under space B (cross-tenant injection).
    ///
    /// Payload columns come from the folded CREATED record through
    /// [`PayloadColumns::from_record`] — the same decomposition
    /// [`ChangeBackend::record_signal`] projects a freshly-authored record with,
    /// so a rebuilt row and a written one agree by construction. Close-state
    /// comes from the fold's winning transition, and content from the fold's
    /// winning EDITED via the same [`ContentOverlay`] rule as the incremental
    /// write — without which every rebuild would revert an edited
    /// row to its birth content.
    fn from_folded(
        space_id: &'a str,
        id: &'a str,
        state: &'a kutl_signals::fold::SignalState,
    ) -> Self {
        let created = &state.created;
        let mut cols = PayloadColumns::from_record(created);
        if let Some(edit) = state.edited() {
            let overlay = ContentOverlay::from_record(edit);
            if let Some(d) = overlay.decision {
                cols.decision_title_hash = Some(d.title_hash);
                cols.decision_title = Some(d.title);
                cols.decision_depth = Some(d.depth);
                cols.decision_parent_hash = d.parent_hash;
            }
            if let Some(message) = overlay.message {
                cols.message = Some(message);
            }
            if let Some(anchor) = overlay.anchor_text {
                cols.anchor_text = Some(anchor);
            }
            if let Some(body) = overlay.body {
                cols.body = Some(body);
            }
        }

        Self {
            id,
            space_id,
            document_id: created.document_id.as_deref(),
            author_did: &created.author_did,
            signal_type: cols.signal_type,
            timestamp: created.timestamp,
            flag_kind: cols.flag_kind,
            audience: cols.audience,
            message: cols.message,
            target_did: cols.target_did,
            anchor_text: cols.anchor_text,
            parent_signal_id: cols.parent_signal_id,
            parent_reply_id: cols.parent_reply_id,
            body: cols.body,
            closed_at: state.closed_at_ms(),
            deleted_at: state.tombstoned_at_ms(),
            close_reason: state.close_reason(),
            decision_title_hash: cols.decision_title_hash,
            decision_title: cols.decision_title,
            decision_depth: cols.decision_depth,
            decision_parent_hash: cols.decision_parent_hash,
            record_id: &created.record_id,
        }
    }
}

#[async_trait]
impl ChangeBackend for SqliteChangeBackend {
    /// This substrate rebuilds. Its rows are a pure function of the record set,
    /// which is what lets re-seed ingest records in arbitrary order and still
    /// land on the right answer.
    fn rebuild(&self) -> Option<&dyn ProjectionRebuild> {
        Some(self)
    }

    async fn get_changes(
        &self,
        did: &str,
        space_id: &str,
        checkpoint: Option<&str>,
    ) -> Result<ChangesResponse, ChangeError> {
        let cursor = sqlx::query_as::<_, CursorRow>(
            "SELECT signal_seq, reg_cursor,
                    prev1_signal_seq, prev1_reg,
                    prev2_signal_seq, prev2_reg, checkpoint
             FROM change_cursors
             WHERE did = ? AND space_id = ?",
        )
        .bind(did)
        .bind(space_id)
        .fetch_optional(&self.pool)
        .await?;

        let resolved = match cursor.as_ref() {
            Some(c) => change_backend::resolve_cursors(
                checkpoint,
                (c.signal_seq, c.reg_cursor),
                (c.prev1_signal_seq, c.prev1_reg),
                (c.prev2_signal_seq, c.prev2_reg),
                &c.checkpoint,
            ),
            None => change_backend::ResolvedCursors::live((0, 0)),
        };
        if resolved.checkpoint_unrecognized {
            // The caller asked to replay a response and did not get one. The
            // answer that follows is an ordinary read from wherever they had
            // reached, which is indistinguishable from a successful replay of a
            // response that happened to be empty.
            tracing::warn!(
                did = %did,
                space_id = %space_id,
                checkpoint = ?checkpoint,
                "checkpoint names no position this reader's ring holds; \
                 reading from the live cursor instead of replaying"
            );
        }
        let (signal_seq, reg_cursor) = resolved.cursors;

        // The position this response starts from, in full: passing it back
        // replays from this position. `signal_seq` is a whole position on its
        // own — the projection's per-space activity counter — so a rewind has
        // nothing left to infer. Replay is at-least-once, not byte-exact: a
        // row whose state changed after this response was cut has been
        // re-sequenced PAST the replay window and arrives on a later page
        // instead (newer state, never a lost row).
        let new_checkpoint = format!("{signal_seq}:{reg_cursor}");

        let (signals, new_signal_seq) = self.fetch_signals(space_id, signal_seq).await?;
        let (document_changes, new_reg) = self.fetch_registry_changes(space_id, reg_cursor).await?;

        self.save_cursors(did, space_id, new_signal_seq, new_reg, &new_checkpoint)
            .await?;

        Ok(ChangesResponse {
            signals,
            document_changes,
            checkpoint: new_checkpoint,
        })
    }

    async fn signal_exists(&self, space_id: &str, signal_id: &str) -> Result<bool, ChangeError> {
        // Straight down the composite primary key, space-scoped like every
        // other lookup here: a signal id is unique only WITHIN a space.
        //
        // THE ONE READ THAT DELIBERATELY USES THE BASE TABLE, not
        // `signals_active`. This answers "is this id taken?", and a tombstoned
        // signal's id IS still taken — the fold keeps its CREATED forever, so a
        // caller allowed to reuse the id would mint a second CREATED that the
        // fold then collapses against the first. Hiding a row must not release
        // its identity.
        let hit: Option<(i64,)> =
            sqlx::query_as("SELECT 1 FROM signals WHERE space_id = ? AND id = ?")
                .bind(space_id)
                .bind(signal_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(hit.is_some())
    }

    async fn get_signal_detail(
        &self,
        space_id: &str,
        signal_id: &str,
    ) -> Result<SignalDetail, ChangeError> {
        // Filter by both id and space_id: the caller is authorized against a
        // specific space, and we must not return a signal that lives in a
        // different space even if its id matches.
        let row = sqlx::query_as::<_, SignalRow>(&format!(
            "SELECT {SIGNAL_COLUMNS} FROM signals_active WHERE id = ? AND space_id = ?"
        ))
        .bind(signal_id)
        .bind(space_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ChangeError::NotFound(format!("signal {signal_id} not found")))?;

        // Fetch replies (signals of type "reply" referencing this signal).
        // Scope by space_id too: a reply may reference a parent in another
        // space (the create-reply path does not re-verify the parent's space),
        // and we must not surface a cross-space reply's author or body here.
        let reply_rows = sqlx::query_as::<_, SignalRow>(&format!(
            "SELECT {SIGNAL_COLUMNS} FROM signals_active \
             WHERE parent_signal_id = ? AND space_id = ? AND signal_type = 'reply' \
             ORDER BY timestamp ASC"
        ))
        .bind(signal_id)
        .bind(space_id)
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
            // Surface canonical NAMES (not enum ordinals) so the detail matches
            // create_flag's input and the Postgres backend.
            flag_kind: row
                .flag_kind
                .map(|k| change_backend::kind_i32_to_str(k).to_string()),
            audience: row
                .audience
                .map(|a| change_backend::audience_i32_to_str(a).to_string()),
            target_did: row.target_did,
            message: row.message,
            anchor_text: row.anchor_text,
            // Derived close-state from the fold-backed projection:
            // set when the signal folds to Closed, NULL after a later REOPENED.
            closed_at: row.closed_at,
            parent_signal_id: row.parent_signal_id,
            parent_reply_id: row.parent_reply_id,
            body: row.body,
            replies,
            reactions: Vec::new(), // Reactions live in DB-backed deployments only
            // Filled by the detail handler from the record log, not the
            // projection — see `signal_transition_history`.
            transitions: Vec::new(),
        })
    }

    /// Prune stale change data.
    ///
    /// Signal rows are a rebuildable projection of the
    /// append-only segment records (the source of truth), so prune NEVER
    /// deletes them by age. An age-windowed trim
    /// cannot hold: the next `project_records` refolds the complete
    /// segment set and re-upserts every signal, resurrecting exactly what the
    /// window trimmed (and with a NULL `anchor_text`). Only the `change_cursors`
    /// reaper (bounded by [`RETENTION_MS`]) and the deleted-registry sweep run
    /// here.
    async fn prune(&self, now_ms: i64) -> Result<u64, ChangeError> {
        let cursor_cutoff = now_ms - RETENTION_MS;

        let cursor_result = sqlx::query("DELETE FROM change_cursors WHERE updated_at < ?")
            .bind(cursor_cutoff)
            .execute(&self.pool)
            .await?;

        let reg_result =
            sqlx::query("DELETE FROM registry WHERE deleted_at IS NOT NULL AND deleted_at < ?")
                .bind(cursor_cutoff)
                .execute(&self.pool)
                .await?;

        Ok(cursor_result.rows_affected() + reg_result.rows_affected())
    }
}

#[async_trait]
impl ProjectionWriter for SqliteChangeBackend {
    async fn project_record(
        &self,
        // The AUTHORITATIVE space, from the admitting seam — never
        // `record.space_id`, which on the replicated path is a peer's
        // assertion rather than this relay's. Every write below keys its row
        // by this value, so a record trusted about its own space cannot reach
        // (or create) a row in a space it was never admitted to.
        space_id: &str,
        record: &kutl_proto::sync::Signal,
        // OSS sqlite has no PATs (personal access tokens are kutlhub-only).
        // Argument accepted for trait conformance; intentionally ignored.
        _via_pat_id: Option<&str>,
    ) -> Result<(), ChangeError> {
        use crate::projection::{AdmittedSpace, ProjectionMutation};

        let space = AdmittedSpace::new(space_id);

        // A transition rewrites an existing row rather than inserting one.
        // This is the ONLY per-record writer on both substrates now: the
        // fold-backed rebuild is a repair path (`ProjectionRebuild`), not
        // something that runs after every admission. WHAT each record does is
        // derived by the shared classification; this backend only executes.
        let born = match crate::projection::classify(record) {
            ProjectionMutation::Close(facts) => {
                return self.close_projected_signal(space, &record.id, &facts).await;
            }
            ProjectionMutation::Reopen => {
                return self.reopen_projected_signal(space, &record.id).await;
            }
            ProjectionMutation::Hide { at_ms } => {
                // Deletion-ladder rung 1: the row is HIDDEN, not removed.
                //
                // A row delete would be unsurvivable: a
                // REOPENED record carries no payload, so nothing incremental
                // could bring the row back — only a whole-space refold could.
                // Both substrates hide, so a reopen is an ordinary update on
                // both, and reads exclude the row by selecting from
                // `signals_active`.
                return self.hide_projected_signal(space, &record.id, at_ms).await;
            }
            // Content overlay, lifecycle untouched.
            ProjectionMutation::Edit => {
                return self.edit_projected_signal(space, record).await;
            }
            ProjectionMutation::Insert(born) => born,
        };

        let cols = PayloadColumns::from_record(record);
        sqlx::query(
            // ON CONFLICT DO NOTHING makes a replayed CREATED a no-op rather
            // than a primary-key error. The same record projected twice must
            // leave one row, not fail an emit whose record already landed.
            // `closed_at` / `close_reason` are bound HERE, on the CREATED, not
            // only on the transition arms: a record can be born closed. A
            // `## = …` decision heading materializes as a CREATED whose
            // `close_reason` is RESOLVED, and a projection
            // that bound only the envelope would show it Open forever. The
            // fold-backed rebuild gets this right on its own (`ProjectedRow::
            // from_folded` reads `state.closed_at_ms()`), which would mask the
            // omission on this substrate — the refold corrects it milliseconds
            // later — but Postgres has no refold.
            // The page key comes off the same expression the upsert uses; a
            // row inserted without one would be stored and never delivered.
            &format!(
                "INSERT INTO signals
                (seq, id, space_id, document_id, author_did, signal_type,
                 timestamp, flag_kind, audience, message, target_did,
                 anchor_text, parent_signal_id, parent_reply_id, body,
                 decision_title_hash, decision_title, decision_depth,
                 decision_parent_hash, record_id, closed_at, close_reason)
             VALUES ({NEXT_SEQ_EXPR},
                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT DO NOTHING"
            ),
        )
        .bind(space.as_str())
        .bind(record.id.as_str())
        .bind(space.as_str())
        .bind(record.document_id.as_deref())
        .bind(record.author_did.as_str())
        .bind(cols.signal_type)
        .bind(record.timestamp)
        .bind(cols.flag_kind)
        .bind(cols.audience)
        .bind(cols.message)
        .bind(cols.target_did)
        .bind(cols.anchor_text)
        .bind(cols.parent_signal_id)
        .bind(cols.parent_reply_id)
        .bind(cols.body)
        // The decision columns must be bound here, matching the upsert:
        // without them a decision projected through this INSERT drops its
        // title and depth until a rebuild happens to run — and rebuilds are a
        // repair lane, not a scheduled pass, on every substrate.
        .bind(cols.decision_title_hash)
        .bind(cols.decision_title)
        .bind(cols.decision_depth)
        .bind(cols.decision_parent_hash)
        // The record this row was projected from. Its absence is what marks a
        // row the startup healer must mint a record for.
        .bind(record.record_id.as_str())
        // Birth close-state, from the shared derivation. `None` for an
        // ordinary CREATED; `Some` only when the record carries a close
        // reason, i.e. it was born closed. Mirrors what the fold computes, so
        // the incremental write and the fold-backed rebuild agree about a
        // born-closed record instead of only the latter being right.
        .bind(born.as_ref().map(|b| b.closed_at_ms))
        .bind(born.as_ref().map(|b| i32::from(b.reason)))
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl ProjectionRebuild for SqliteChangeBackend {
    /// Rewrite this space's projection rows to equal the fold of `records`.
    ///
    /// `records` is the space's COMPLETE record set (the segments).
    /// This is the fold-backed, order-insensitive projection: it
    /// folds every record with [`SpaceSignalState`] and upserts one row per
    /// folded signal, so the result is identical for ANY arrival order (a
    /// global HLC watermark would be wrong here — a low-HLC record arriving
    /// after a high-HLC one, or a stale CLOSED after a newer REOPENED, would
    /// be mis-handled; the fold's per-signal LWW is the correctness
    /// mechanism instead).
    ///
    /// Idempotent: projecting the same records twice leaves the row count and
    /// contents unchanged. Tombstoned signals are DELETED from the projection
    /// (soft delete — deletion-ladder rung 1: projections hide the
    /// signal), so a prior Open/Closed row is cleared rather than left stale.
    /// `anchor_text` is preserved across upserts (see
    /// [`Self::upsert_projected_signal`]).
    ///
    /// The delete is TARGETED to the ids the fold reports Tombstoned; it does
    /// not reconcile-delete rows merely absent from `records` (that would drop
    /// legacy pre-backfill rows whose segments have not been materialized).
    async fn rebuild_space(
        &self,
        space_id: &str,
        records: &[kutl_proto::sync::Signal],
    ) -> Result<(), ChangeError> {
        use kutl_signals::fold::SpaceSignalState;

        let mut state = SpaceSignalState::default();
        for record in records {
            state.apply(record.clone());
        }

        for (id, signal_state) in state.iter() {
            // NO tombstone branch. A tombstoned signal gets a row like any
            // other, carrying `deleted_at` from the fold — hidden is a VALUE
            // here, not a control-flow path.
            //
            // A branch would be wrong: the hide is an
            // UPDATE, so on a projection that has no row yet (a fresh rebuild,
            // or the startup reconcile) it matches nothing, and skipping the
            // insert leaves the signal
            // in the log with NO row at all — and `signal_exists`
            // deliberately reads the base table so that hiding a signal does
            // not release its id, so the duplicate-create guard would hand
            // that id straight back out.
            // Key the row to the AUTHORITATIVE `space_id` (the space this
            // projection was invoked for), never the record's own `space_id`
            // field — see `ProjectedRow::from_folded`.
            let row = ProjectedRow::from_folded(space_id, id, signal_state);
            self.upsert_projected_signal(&row).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
impl SqliteChangeBackend {
    /// Project a record under its OWN `space_id`.
    ///
    /// Test-only. [`ProjectionWriter::project_record`] takes the admitting
    /// space separately, because on the replicated path a record is a peer's
    /// bytes and cannot be trusted to name where it belongs. Every test below
    /// builds its own record, so the two are the same value by construction and
    /// threading it twice at seventeen call sites would obscure rather than
    /// clarify. Production code must keep passing the admitting space.
    async fn project_record_for_test(
        &self,
        record: &kutl_proto::sync::Signal,
        via_pat_id: Option<&str>,
    ) -> Result<(), ChangeError> {
        let space_id = record.space_id.clone();
        ProjectionWriter::project_record(self, &space_id, record, via_pat_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_proto::sync::{
        Audience, DecisionPayload, FlagKind, FlagPayload, ReplyPayload, Signal, audience, signal,
    };

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
        .await
        .unwrap();

        pool
    }

    /// A CREATED record for the projection tests: identity here, payload from
    /// [`flag_payload`] / [`reply_payload`]. Splitting the two is what keeps a
    /// fixture that covers every kind down to one identity signature.
    fn test_row(
        id: &str,
        space_id: &str,
        document_id: Option<&str>,
        author_did: &str,
        timestamp: i64,
        payload: signal::Payload,
    ) -> Signal {
        Signal {
            id: id.to_owned(),
            space_id: space_id.to_owned(),
            document_id: document_id.map(str::to_owned),
            author_did: author_did.to_owned(),
            timestamp,
            payload: Some(payload),
            // Minted, like `assemble_record` mints it. A record with no
            // record_id never reaches the projection — the admission seam
            // rejects one — so a fixture without it would test a shape
            // production cannot produce.
            record_id: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        }
    }

    /// A flag payload. `target_did` picks the audience: `None` is space-wide,
    /// `Some(did)` addresses that participant — the only two arms the typed
    /// `Audience` has, so a fixture cannot express a shape the
    /// authored seam would refuse.
    fn flag_payload(kind: FlagKind, target_did: Option<&str>, message: &str) -> signal::Payload {
        let to = match target_did {
            Some(did) => audience::To::Participant(audience::Participant {
                did: did.to_owned(),
            }),
            None => audience::To::Space(audience::Space {}),
        };
        signal::Payload::Flag(FlagPayload {
            kind: i32::from(kind),
            message: message.to_owned(),
            audience: Some(Audience { to: Some(to) }),
            ..Default::default()
        })
    }

    /// A reply payload, carrying the parent linkage a rebuild reconstructs from.
    fn reply_payload(
        parent_signal_id: &str,
        parent_reply_id: Option<&str>,
        body: &str,
    ) -> signal::Payload {
        signal::Payload::Reply(ReplyPayload {
            parent_signal_id: parent_signal_id.to_owned(),
            parent_reply_id: parent_reply_id.map(str::to_owned),
            body: body.to_owned(),
        })
    }

    /// A decision payload for a top-level (`depth = 1`, no parent) heading.
    fn decision_payload(title_hash: &str, title: &str) -> signal::Payload {
        /// Depth of a top-level `##` decision heading.
        const TOP_LEVEL_DEPTH: i32 = 1;

        signal::Payload::Decision(DecisionPayload {
            title_hash: title_hash.to_owned(),
            title: title.to_owned(),
            depth: TOP_LEVEL_DEPTH,
            parent_hash: None,
        })
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
            .project_record_for_test(
                &test_row(
                    "sig-test-1",
                    "space-1",
                    Some("doc-1"),
                    "did:bob",
                    1000,
                    flag_payload(FlagKind::Info, None, "please review"),
                ),
                None,
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

    /// A payload-less lifecycle transition record for `sig`, the shape the
    /// admission seam mints for a close or reopen.
    fn transition_record(
        sig: &str,
        space: &str,
        event: kutl_proto::sync::SignalEventType,
        close_reason: Option<i32>,
        timestamp: i64,
    ) -> Signal {
        let mut s = Signal {
            id: sig.to_owned(),
            space_id: space.to_owned(),
            author_did: "did:key:zCloser".to_owned(),
            actor_did: "did:key:zCloser".to_owned(),
            timestamp,
            close_reason: close_reason.unwrap_or_default(),
            record_id: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        };
        s.set_event(event);
        s
    }

    /// The admitting seam's space verdict — not the record's own claim —
    /// decides which row a projection write may touch. Signal ids are
    /// globally unique, so an id-plus-claim WHERE would let a record admitted
    /// under one space reach a row in another.
    /// `project_record_for_test` collapses claim and verdict by construction,
    /// so these three call the trait directly with the two split.
    #[tokio::test]
    async fn test_transition_admitted_for_another_space_cannot_reach_this_signal() {
        use kutl_proto::sync::SignalEventType;

        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        backend
            .project_record_for_test(
                &test_row(
                    "sig-1",
                    "space-1",
                    Some("doc-1"),
                    "did:bob",
                    1000,
                    flag_payload(FlagKind::Info, None, "please review"),
                ),
                None,
            )
            .await
            .unwrap();

        // A close whose claim names the signal's true home, admitted under a
        // DIFFERENT space: it must not reach the row.
        let close = transition_record(
            "sig-1",
            "space-1",
            SignalEventType::Closed,
            Some(i32::from(kutl_proto::sync::CloseReason::Resolved)),
            2000,
        );
        ProjectionWriter::project_record(&backend, "space-other", &close, None)
            .await
            .unwrap();

        let detail = backend.get_signal_detail("space-1", "sig-1").await.unwrap();
        assert_eq!(
            detail.closed_at, None,
            "a transition admitted for another space must not close this signal"
        );
    }

    #[tokio::test]
    async fn test_transition_applies_under_the_admitting_space_despite_a_forged_claim() {
        use kutl_proto::sync::SignalEventType;

        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        backend
            .project_record_for_test(
                &test_row(
                    "sig-1",
                    "space-1",
                    Some("doc-1"),
                    "did:bob",
                    1000,
                    flag_payload(FlagKind::Info, None, "please review"),
                ),
                None,
            )
            .await
            .unwrap();

        let mut close = transition_record(
            "sig-1",
            "space-1",
            SignalEventType::Closed,
            Some(i32::from(kutl_proto::sync::CloseReason::Resolved)),
            2000,
        );
        close.space_id = "space-forged".to_owned();
        ProjectionWriter::project_record(&backend, "space-1", &close, None)
            .await
            .unwrap();

        let detail = backend.get_signal_detail("space-1", "sig-1").await.unwrap();
        assert!(
            detail.closed_at.is_some(),
            "the verified space admitted the close; the record's claim must not veto it"
        );
    }

    #[tokio::test]
    async fn test_created_lands_in_the_admitting_space_not_the_claimed_one() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        let created = test_row(
            "sig-1",
            "space-forged",
            Some("doc-1"),
            "did:bob",
            1000,
            flag_payload(FlagKind::Info, None, "please review"),
        );
        ProjectionWriter::project_record(&backend, "space-1", &created, None)
            .await
            .unwrap();

        assert!(
            backend.get_signal_detail("space-1", "sig-1").await.is_ok(),
            "the row must live in the admitting space"
        );
        assert!(
            backend
                .get_signal_detail("space-forged", "sig-1")
                .await
                .is_err(),
            "and not in the claimed one"
        );
    }

    /// GUARD (activity feed): a lifecycle change RE-SURFACES its signal.
    /// `seq` is a last-ACTIVITY key, not a birth key — a reader whose cursor
    /// has passed a signal must receive the row again when it closes or
    /// reopens, carrying the new state; a feed keyed on birth order never
    /// shows a change at all. The counter-half matters equally: an
    /// IDEMPOTENT replay of the same transition (a rebuild re-projecting
    /// history) must NOT re-deliver — only actual state change moves a row.
    #[tokio::test]
    async fn test_lifecycle_changes_resurface_the_signal_in_the_change_feed() {
        use kutl_proto::sync::SignalEventType;

        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        backend
            .project_record_for_test(
                &test_row(
                    "sig-1",
                    "space-1",
                    Some("doc-1"),
                    "did:bob",
                    1000,
                    flag_payload(FlagKind::Info, None, "please review"),
                ),
                None,
            )
            .await
            .unwrap();

        // The reader passes the birth.
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 1);

        // Close → the signal comes around again, and the served row SAYS so.
        let close = transition_record(
            "sig-1",
            "space-1",
            SignalEventType::Closed,
            Some(i32::from(kutl_proto::sync::CloseReason::Declined)),
            2000,
        );
        backend.project_record_for_test(&close, None).await.unwrap();
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 1, "a close must re-surface the signal");
        assert_eq!(resp.signals[0].id, "sig-1");
        assert_eq!(
            resp.signals[0].event(),
            SignalEventType::Closed,
            "the re-served row must carry its lifecycle state"
        );
        assert_eq!(
            resp.signals[0].close_reason(),
            kutl_proto::sync::CloseReason::Declined,
            "and the reason"
        );

        // The SAME close replayed (a rebuild re-projecting history): quiet.
        backend.project_record_for_test(&close, None).await.unwrap();
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert!(
            resp.signals.is_empty(),
            "an idempotent replay must not re-deliver: {:?}",
            resp.signals
        );

        // Reopen → around again, open.
        let reopen = transition_record("sig-1", "space-1", SignalEventType::Reopened, None, 3000);
        backend
            .project_record_for_test(&reopen, None)
            .await
            .unwrap();
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 1, "a reopen must re-surface the signal");
        assert_ne!(
            resp.signals[0].event(),
            SignalEventType::Closed,
            "the re-served row is open again"
        );

        // The same reopen replayed: quiet.
        backend
            .project_record_for_test(&reopen, None)
            .await
            .unwrap();
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert!(
            resp.signals.is_empty(),
            "an idempotent reopen replay must not re-deliver"
        );
    }

    /// The EDITED overlay is a change like any other: a content edit
    /// re-surfaces the signal, and replaying the same edit does not.
    #[tokio::test]
    async fn test_content_edits_resurface_the_signal_in_the_change_feed() {
        use kutl_proto::sync::SignalEventType;

        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        backend
            .project_record_for_test(
                &test_row(
                    "sig-e",
                    "space-1",
                    Some("doc-1"),
                    "did:bob",
                    1000,
                    flag_payload(FlagKind::Info, None, "first wording"),
                ),
                None,
            )
            .await
            .unwrap();
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 1);

        // An EDITED record rewording the flag.
        let mut edit = test_row(
            "sig-e",
            "space-1",
            Some("doc-1"),
            "did:bob",
            2000,
            flag_payload(FlagKind::Info, None, "second wording"),
        );
        edit.set_event(SignalEventType::Edited);
        backend.project_record_for_test(&edit, None).await.unwrap();
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 1, "an edit must re-surface the signal");
        match resp.signals[0].payload.as_ref().unwrap() {
            kutl_proto::sync::signal::Payload::Flag(f) => {
                assert_eq!(f.message, "second wording");
            }
            other => panic!("expected flag payload, got {other:?}"),
        }

        // The same edit replayed: quiet.
        backend.project_record_for_test(&edit, None).await.unwrap();
        let resp = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert!(
            resp.signals.is_empty(),
            "an idempotent edit replay must not re-deliver"
        );
    }

    /// Record `count` flag signals in `space`, all at `timestamp`, with ids
    /// `{prefix}-{i}`.
    async fn record_signals_at(
        backend: &SqliteChangeBackend,
        space: &str,
        prefix: &str,
        count: usize,
        timestamp: i64,
    ) {
        for i in 0..count {
            backend
                .project_record(
                    space,
                    &test_row(
                        &format!("{prefix}-{i}"),
                        space,
                        Some("doc-1"),
                        "did:bob",
                        timestamp,
                        flag_payload(FlagKind::Info, None, "m"),
                    ),
                    None,
                )
                .await
                .unwrap();
        }
    }

    /// Drain `get_changes` until no signals are returned (capped to avoid an
    /// infinite loop on a stuck cursor) and return all signal ids seen.
    async fn drain_signal_ids(
        backend: &SqliteChangeBackend,
        did: &str,
        space: &str,
    ) -> Vec<String> {
        let mut ids = Vec::new();
        for _ in 0..100 {
            let resp = backend.get_changes(did, space, None).await.unwrap();
            if resp.signals.is_empty() {
                break;
            }
            ids.extend(resp.signals.into_iter().map(|s| s.id));
        }
        ids
    }

    /// More signals share a single millisecond than fit in one page. The
    /// cursor must drain the whole boundary group rather than advancing past
    /// it and dropping the overflow forever.
    #[tokio::test]
    async fn test_get_changes_same_millisecond_overflow_not_lost() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        let count = MAX_EVENTS_PER_QUERY + 50;
        record_signals_at(&backend, "space-1", "sig", count, 1000).await;

        let ids = drain_signal_ids(&backend, "did:alice", "space-1").await;
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), count, "all same-ms signals must be delivered");
    }

    /// A second signal minted in the same millisecond as the last one a caller
    /// read must still be delivered.
    ///
    /// Two records in one millisecond are ordinary — the relay ticks its clock
    /// within a millisecond precisely so records emit in sequence order — and a
    /// `get_changes` landing between the two is ordinary too. A cursor that
    /// remembers only the millisecond puts the second record permanently below
    /// it: the write wakes the caller over the socket, and the read that
    /// follows returns nothing.
    #[tokio::test]
    async fn test_get_changes_second_signal_in_the_read_millisecond_arrives() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        record_signals_at(&backend, "space-1", "first", 1, 1000).await;
        let first = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(first.signals.len(), 1, "the first signal arrives");

        // A peer's signal commits in the same millisecond, after that read.
        record_signals_at(&backend, "space-1", "second", 1, 1000).await;

        let second = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(
            second
                .signals
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["second-0"],
            "a row committed later in the cursor's millisecond must not fall below it"
        );
    }

    /// The same case as above with the ids the other way round.
    ///
    /// Ids are minted `Uuid::new_v4()`, so they carry no order at all: whatever
    /// an id-based tiebreaker does for an ascending pair it does the opposite
    /// of for a descending one, and half of production's inputs are descending.
    /// A page key that orders with the log is indifferent to both.
    #[tokio::test]
    async fn test_get_changes_later_row_sorting_below_the_cursor_id_arrives() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        record_signals_at(&backend, "space-1", "zzz", 1, 1000).await;
        let first = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(first.signals.len(), 1, "the first signal arrives");

        // Same millisecond, committed after the read, id sorting BELOW it.
        record_signals_at(&backend, "space-1", "aaa", 1, 1000).await;

        let second = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(
            second
                .signals
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["aaa-0"],
            "a row committed later in the cursor's millisecond must not fall below it"
        );
    }

    /// A checkpoint names the position a response started from, so passing it
    /// back re-delivers that response. The page whose rows all share the
    /// cursor's millisecond is the one a client is most likely to lose and
    /// least able to re-derive, so it is the one pinned here.
    #[tokio::test]
    async fn test_get_changes_checkpoint_replays_a_page_inside_one_millisecond() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        record_signals_at(&backend, "space-1", "first", 1, 1000).await;
        backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();

        record_signals_at(&backend, "space-1", "second", 1, 1000).await;
        let page = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(
            page.signals
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["second-0"],
        );

        let replay = backend
            .get_changes("did:alice", "space-1", Some(&page.checkpoint))
            .await
            .unwrap();
        assert_eq!(
            replay
                .signals
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["second-0"],
            "a checkpoint must re-deliver the response it names"
        );
    }

    /// A group sharing the boundary millisecond straddles the page limit. The
    /// rows beyond the cut must not be skipped when the cursor advances.
    #[tokio::test]
    async fn test_get_changes_boundary_millisecond_straddle_not_lost() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        // Fill most of a page at ts=1000, then a group at ts=2000 that the
        // first page's limit cuts through.
        let at_1000 = MAX_EVENTS_PER_QUERY - 1;
        record_signals_at(&backend, "space-1", "early", at_1000, 1000).await;
        record_signals_at(&backend, "space-1", "late", 5, 2000).await;

        let ids = drain_signal_ids(&backend, "did:alice", "space-1").await;
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(
            unique.len(),
            at_1000 + 5,
            "signals beyond the page cut must not be dropped"
        );
    }

    /// Registry changes sharing one effective timestamp must also drain fully
    /// rather than being cut at the page limit.
    #[tokio::test]
    async fn test_get_changes_registry_same_millisecond_overflow_not_lost() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();

        let count = MAX_EVENTS_PER_QUERY + 50;
        for i in 0..count {
            sqlx::query(
                "INSERT INTO registry (space_id, document_id, path, created_by, created_at)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind("space-1")
            .bind(format!("doc-{i}"))
            .bind(format!("notes/{i}.md"))
            .bind("did:alice")
            .bind(5000i64)
            .execute(&pool)
            .await
            .unwrap();
        }

        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let resp = backend
                .get_changes("did:bob", "space-1", None)
                .await
                .unwrap();
            if resp.document_changes.is_empty() {
                break;
            }
            for c in resp.document_changes {
                seen.insert(c.document_id);
            }
        }
        assert_eq!(
            seen.len(),
            count,
            "all same-ms registry changes must be delivered"
        );
    }

    /// A reply whose parent lives in a different space must not be surfaced in
    /// the parent's space detail view (cross-space isolation).
    #[tokio::test]
    async fn test_get_signal_detail_replies_scoped_to_space() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        // Parent flag in space-1.
        backend
            .project_record_for_test(
                &test_row(
                    "parent-1",
                    "space-1",
                    Some("doc-1"),
                    "did:alice",
                    1000,
                    flag_payload(FlagKind::Info, None, "m"),
                ),
                None,
            )
            .await
            .unwrap();
        // Reply in space-2 that references the space-1 parent.
        backend
            .project_record_for_test(
                &test_row(
                    "reply-x",
                    "space-2",
                    Some("doc-1"),
                    "did:mallory",
                    1001,
                    reply_payload("parent-1", None, "leaked"),
                ),
                None,
            )
            .await
            .unwrap();

        let detail = backend
            .get_signal_detail("space-1", "parent-1")
            .await
            .unwrap();
        assert!(
            detail.replies.is_empty(),
            "cross-space reply must not appear in the parent's space detail"
        );
    }

    #[tokio::test]
    async fn test_get_changes_separate_cursors_per_did() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        backend
            .project_record_for_test(
                &test_row(
                    "sig-test-2",
                    "space-1",
                    Some("doc-1"),
                    "did:carol",
                    1000,
                    flag_payload(FlagKind::Info, None, "hello"),
                ),
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
    async fn test_get_changes_invalid_checkpoint_falls_through() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();
        let did = "did:key:agent";
        let space = "5171e0a1-1111-4000-8000-000000000002";

        // Record a flag signal.
        backend
            .project_record_for_test(
                &test_row(
                    "sig-cp-1",
                    space,
                    Some("doc1"),
                    "did:key:alice",
                    1000,
                    flag_payload(FlagKind::Info, None, "hello"),
                ),
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
            .project_record_for_test(
                &test_row(
                    "sig-rewind-1",
                    "space-1",
                    Some("doc-1"),
                    "did:bob",
                    1000,
                    flag_payload(FlagKind::Info, None, "first"),
                ),
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
            .project_record_for_test(
                &test_row(
                    "sig-rewind-2",
                    "space-1",
                    Some("doc-2"),
                    "did:bob",
                    2000,
                    flag_payload(FlagKind::Info, None, "second"),
                ),
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
    async fn test_get_signal_detail_flag_with_replies() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        // Record a flag signal.
        backend
            .project_record_for_test(
                &test_row(
                    "flag-1",
                    "space-1",
                    Some("doc-1"),
                    "did:alice",
                    1000,
                    flag_payload(FlagKind::Info, None, "please review"),
                ),
                None,
            )
            .await
            .unwrap();

        // Record a reply to the flag.
        backend
            .project_record_for_test(
                &test_row(
                    "reply-1",
                    "space-1",
                    None,
                    "did:bob",
                    2000,
                    reply_payload("flag-1", None, "looks good"),
                ),
                None,
            )
            .await
            .unwrap();

        let detail = backend
            .get_signal_detail("space-1", "flag-1")
            .await
            .unwrap();

        assert_eq!(detail.id, "flag-1");
        assert_eq!(detail.signal_type, "flag");
        assert_eq!(detail.replies.len(), 1);
        assert_eq!(detail.replies[0].id, "reply-1");
        assert_eq!(detail.replies[0].body, "looks good");
        assert!(detail.reactions.is_empty());
    }

    /// `flag_kind`/`audience` are surfaced as canonical NAMES (matching
    /// `create_flag`'s input and the Postgres backend), never enum ordinals.
    #[tokio::test]
    async fn test_get_signal_detail_flag_kind_audience_names() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        // FlagKind::Question → "question", a space audience → "space".
        backend
            .project_record_for_test(
                &test_row(
                    "flag-q",
                    "space-1",
                    Some("doc-1"),
                    "did:alice",
                    1000,
                    flag_payload(FlagKind::Question, None, "can someone review?"),
                ),
                None,
            )
            .await
            .unwrap();

        let detail = backend
            .get_signal_detail("space-1", "flag-q")
            .await
            .unwrap();
        assert_eq!(detail.flag_kind.as_deref(), Some("question"));
        assert_eq!(detail.audience.as_deref(), Some("space"));
    }

    /// A decision projected through the create path keeps its title and depth.
    ///
    /// The create-path INSERT must bind the decision columns itself: without
    /// them the title would survive only where a rebuild happens to overwrite
    /// the row afterwards, and rebuilds are a repair lane, not a scheduled
    /// pass. `get_changes` reconstructing the `DecisionPayload` is what
    /// proves the columns landed.
    #[tokio::test]
    async fn test_record_signal_decision_projects_title_and_depth() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        backend
            .project_record_for_test(
                &test_row(
                    "dec-1",
                    "space-1",
                    Some("doc-1"),
                    "did:alice",
                    1000,
                    decision_payload("h4sh", "Ship on Friday"),
                ),
                None,
            )
            .await
            .unwrap();

        let resp = backend
            .get_changes("did:bob", "space-1", None)
            .await
            .unwrap();
        assert_eq!(resp.signals.len(), 1);
        match resp.signals[0].payload.as_ref().unwrap() {
            signal::Payload::Decision(d) => {
                assert_eq!(d.title, "Ship on Friday");
                assert_eq!(d.title_hash, "h4sh");
                assert_eq!(d.depth, 1);
                assert_eq!(d.parent_hash, None);
            }
            other => panic!("expected a Decision payload, got {other:?}"),
        }
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

    // -----------------------------------------------------------------------
    // Fold-backed projection
    // -----------------------------------------------------------------------

    use kutl_proto::sync::SignalEventType;
    use kutl_signals::fold::{SignalStatus, SpaceSignalState};

    /// A well-formed CREATED flag record for signal `sig`.
    fn created_flag(space: &str, sig: &str, record_id: &str, ms: u64) -> Signal {
        let mut s = Signal {
            id: sig.into(),
            space_id: space.into(),
            document_id: Some("doc-1".into()),
            author_did: "did:key:zAuthor".into(),
            timestamp: ms.cast_signed(),
            record_id: record_id.into(),
            actor_did: "did:key:zAuthor".into(),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: ms,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            payload: Some(kutl_proto::sync::signal::Payload::Flag(
                // The deprecated audience pair is the shape under test: these fixtures
                // stand in for records already on disk.
                #[allow(deprecated)]
                kutl_proto::sync::FlagPayload {
                    kind: 1,
                    audience_type: 0,
                    target_did: None,
                    message: "please review".into(),
                    audience: None,
                    anchor_text: None,
                },
            )),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// A transition record (CLOSED/REOPENED) for signal `sig`.
    fn transition(
        space: &str,
        sig: &str,
        record_id: &str,
        event: SignalEventType,
        ms: u64,
        close_reason: i32,
    ) -> Signal {
        let mut s = Signal {
            id: sig.into(),
            space_id: space.into(),
            author_did: "did:key:zCloser".into(),
            timestamp: ms.cast_signed(),
            record_id: record_id.into(),
            actor_did: "did:key:zCloser".into(),
            close_reason,
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: ms,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        s.set_event(event);
        s
    }

    /// Read `(closed_at, close_reason)` directly off a projected row.
    async fn projected_status(
        pool: &SqlitePool,
        space: &str,
        sig: &str,
    ) -> (Option<i64>, Option<i32>) {
        let row: (Option<i64>, Option<i32>) = sqlx::query_as(
            "SELECT closed_at, close_reason FROM signals WHERE id = ? AND space_id = ?",
        )
        .bind(sig)
        .bind(space)
        .fetch_one(pool)
        .await
        .unwrap();
        row
    }

    /// Count rows in the signals table for a space.
    /// VISIBLE rows for a space — the view, not the table.
    ///
    /// A tombstone hides rather than deletes, so "how many rows exist" and "how
    /// many are visible" are different questions now. Every assertion here means
    /// the second; [`stored_count`] answers the first.
    async fn row_count(pool: &SqlitePool, space: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM signals_active WHERE space_id = ?")
            .bind(space)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Rows STORED for a space, hidden ones included — the base table.
    ///
    /// Paired with [`row_count`] to prove a tombstone HID the row rather than
    /// removing it: the delete was unsurvivable, because a REOPENED record
    /// carries no payload and nothing incremental could bring a deleted row
    /// back.
    async fn stored_count(pool: &SqlitePool, space: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM signals WHERE space_id = ?")
            .bind(space)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Projecting a record set twice is idempotent, and the projected rows
    /// equal folding the same records with `SpaceSignalState`.
    #[tokio::test]
    async fn test_projection_idempotent_and_equivalent_to_fold() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "11111111-1111-1111-1111-111111111111";

        let records = vec![
            created_flag(space, "sig-a", "r1", 1_000),
            transition(space, "sig-a", "r2", SignalEventType::Closed, 2_000, 1),
        ];

        backend.rebuild_space(space, &records).await.unwrap();
        let after_first = row_count(&pool, space).await;
        assert_eq!(after_first, 1, "one signal projected to one row");

        // Project the SAME records again — idempotent: no new rows.
        backend.rebuild_space(space, &records).await.unwrap();
        assert_eq!(
            row_count(&pool, space).await,
            after_first,
            "re-projecting the same records must not add rows"
        );

        // The projected row must equal the fold: closed (closed_at set,
        // reason = 1).
        let mut fold = SpaceSignalState::default();
        for r in &records {
            fold.apply(r.clone());
        }
        let folded = fold.get("sig-a").unwrap();
        assert_eq!(folded.status, SignalStatus::Closed, "fold says Closed");
        let (closed_at, reason) = projected_status(&pool, space, "sig-a").await;
        assert_eq!(closed_at, folded.closed_at_ms());
        assert_eq!(closed_at, Some(2_000), "closed_at = CLOSED transition ms");
        assert_eq!(reason, folded.close_reason());
        assert_eq!(reason, Some(1), "close_reason = CLOSE_REASON_RESOLVED");
    }

    /// `get_signal_detail` reports `closed_at` set after a CLOSED
    /// projection and unset after a subsequent REOPENED projection.
    #[tokio::test]
    async fn test_get_signal_detail_closed_at_set_then_cleared_by_reopen() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "22222222-2222-2222-2222-222222222222";

        // Create + close.
        backend
            .rebuild_space(
                space,
                &[
                    created_flag(space, "sig-c", "r1", 1_000),
                    transition(space, "sig-c", "r2", SignalEventType::Closed, 2_000, 1),
                ],
            )
            .await
            .unwrap();
        let detail = backend.get_signal_detail(space, "sig-c").await.unwrap();
        assert_eq!(
            detail.closed_at,
            Some(2_000),
            "closed_at must be set after CLOSED"
        );

        // Reopen (higher HLC) — closed_at must clear.
        backend
            .rebuild_space(
                space,
                &[
                    created_flag(space, "sig-c", "r1", 1_000),
                    transition(space, "sig-c", "r2", SignalEventType::Closed, 2_000, 1),
                    transition(space, "sig-c", "r3", SignalEventType::Reopened, 3_000, 0),
                ],
            )
            .await
            .unwrap();
        let detail = backend.get_signal_detail(space, "sig-c").await.unwrap();
        assert_eq!(
            detail.closed_at, None,
            "closed_at must be cleared after a later REOPENED"
        );
    }

    /// `record_signal` projects a transition, not only a create.
    ///
    /// A rebuild would reach the same answer, but rebuilds are a repair
    /// lane — the incremental arm is the only writer in steady state on
    /// every substrate, and the point of the shared seam is that both
    /// substrates project the same record the same way. Driving
    /// `record_signal` directly, with no `project_records` behind it, is what
    /// proves the arm rather than the fold.
    #[tokio::test]
    async fn test_record_signal_projects_close_then_reopen() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "44444444-4444-4444-4444-444444444444";

        backend
            .project_record_for_test(
                &test_row(
                    "sig-t",
                    space,
                    Some("doc-1"),
                    "did:alice",
                    1_000,
                    flag_payload(FlagKind::Question, None, "shall we?"),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(projected_status(&pool, space, "sig-t").await, (None, None));

        backend
            .project_record_for_test(
                &transition(space, "sig-t", "r2", SignalEventType::Closed, 2_000, 1),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            projected_status(&pool, space, "sig-t").await,
            (Some(2_000), Some(1)),
            "CLOSED must set closed_at and the reason discriminant"
        );

        backend
            .project_record_for_test(
                &transition(space, "sig-t", "r3", SignalEventType::Reopened, 3_000, 0),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            projected_status(&pool, space, "sig-t").await,
            (None, None),
            "REOPENED must clear both close columns"
        );
    }

    /// Deletion-ladder rung 1: a TOMBSTONED transition HIDES the row — it is
    /// invisible through `signals_active` but still stored.
    ///
    /// An outright delete would be unsurvivable: a REOPENED record
    /// carries no payload, so nothing incremental could resurrect the row and
    /// only a whole-space refold could — and the Postgres substrate has no
    /// refold. Asserting BOTH halves is the point: invisible proves the
    /// hide took effect, still-stored proves it is recoverable.
    #[tokio::test]
    async fn test_record_signal_tombstone_hides_the_row() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "55555555-5555-5555-5555-555555555555";

        backend
            .project_record_for_test(
                &test_row(
                    "sig-x",
                    space,
                    None,
                    "did:alice",
                    1_000,
                    flag_payload(FlagKind::Info, None, "bye"),
                ),
                None,
            )
            .await
            .unwrap();
        backend
            .project_record_for_test(
                &transition(space, "sig-x", "r2", SignalEventType::Tombstoned, 2_000, 0),
                None,
            )
            .await
            .unwrap();

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM signals_active WHERE id = ? AND space_id = ?")
                .bind("sig-x")
                .bind(space)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(remaining, 0, "a tombstoned signal must be invisible");
    }

    /// A transition whose CREATED has not arrived updates nothing and does not
    /// error. The fold parks an orphan transition and applies it later,
    /// so failing here would break an emit the record layer
    /// accepted — and on the replicated path orphans are routine.
    #[tokio::test]
    async fn test_record_signal_orphan_transition_is_a_no_op() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "66666666-6666-6666-6666-666666666666";

        backend
            .project_record_for_test(
                &transition(space, "ghost", "r1", SignalEventType::Closed, 2_000, 1),
                None,
            )
            .await
            .expect("an orphan transition must not error");

        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signals WHERE space_id = ?")
            .bind(space)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 0, "an orphan transition must not conjure a row");
    }

    /// The out-of-order test a global watermark would fail: applying
    /// CREATED(hlc1), REOPENED(hlc2), CLOSED(hlc3) in SHUFFLED order must
    /// still project `Closed` (the max-HLC transition wins) in BOTH orders.
    /// This proves per-signal LWW, not arrival-order dependence.
    #[tokio::test]
    async fn test_projection_out_of_order_records_closed_wins_both_orders() {
        let space = "33333333-3333-3333-3333-333333333333";
        let created = created_flag(space, "sig-o", "r1", 1_000);
        let reopened = transition(space, "sig-o", "r2", SignalEventType::Reopened, 2_000, 0);
        // CLOSED has the max HLC (3_000) so it must be the winning transition.
        let closed = transition(space, "sig-o", "r3", SignalEventType::Closed, 3_000, 2);

        // Order 1: [CLOSED(3), CREATED(1), REOPENED(2)]
        let pool1 = test_pool().await;
        let backend1 = SqliteChangeBackend::new(pool1.clone()).await.unwrap();
        backend1
            .rebuild_space(space, &[closed.clone(), created.clone(), reopened.clone()])
            .await
            .unwrap();
        let (closed_at_1, reason_1) = projected_status(&pool1, space, "sig-o").await;
        assert!(
            closed_at_1.is_some(),
            "order [3,1,2]: max-HLC CLOSED must win → closed_at set"
        );
        assert_eq!(closed_at_1, Some(3_000));
        assert_eq!(reason_1, Some(2), "close_reason from the winning CLOSED");

        // Order 2: [REOPENED(2), CLOSED(3), CREATED(1)]
        let pool2 = test_pool().await;
        let backend2 = SqliteChangeBackend::new(pool2.clone()).await.unwrap();
        backend2
            .rebuild_space(space, &[reopened.clone(), closed.clone(), created.clone()])
            .await
            .unwrap();
        let (closed_at_2, reason_2) = projected_status(&pool2, space, "sig-o").await;
        assert!(
            closed_at_2.is_some(),
            "order [2,3,1]: max-HLC CLOSED must win → closed_at set"
        );
        assert_eq!(closed_at_2, Some(3_000));
        assert_eq!(reason_2, Some(2));

        // Both orders must project identically (per-signal LWW, not
        // arrival-order dependence).
        assert_eq!(
            (closed_at_1, reason_1),
            (closed_at_2, reason_2),
            "shuffled arrival orders must project the same close-state"
        );
    }

    // -----------------------------------------------------------------------
    // Projection == fold(segments)
    // -----------------------------------------------------------------------

    /// Build a CREATED reply record for signal `sig` that references `parent`.
    fn created_reply(space: &str, sig: &str, record_id: &str, parent: &str, ms: u64) -> Signal {
        let mut s = Signal {
            id: sig.into(),
            space_id: space.into(),
            document_id: Some("doc-r".into()),
            author_did: "did:key:zReplier".into(),
            timestamp: ms.cast_signed(),
            record_id: record_id.into(),
            actor_did: "did:key:zReplier".into(),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: ms,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            payload: Some(kutl_proto::sync::signal::Payload::Reply(
                kutl_proto::sync::ReplyPayload {
                    parent_signal_id: parent.into(),
                    parent_reply_id: None,
                    body: "looks good to me".into(),
                },
            )),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// Read `parent_signal_id` for a signal row directly from the pool.
    async fn projected_parent(pool: &SqlitePool, space: &str, sig: &str) -> Option<String> {
        sqlx::query_scalar("SELECT parent_signal_id FROM signals WHERE id = ? AND space_id = ?")
            .bind(sig)
            .bind(space)
            .fetch_optional(pool)
            .await
            .unwrap()
            .flatten()
    }

    /// Read the `signal_type` column for a projected row.
    async fn projected_type(pool: &SqlitePool, space: &str, sig: &str) -> String {
        sqlx::query_scalar("SELECT signal_type FROM signals WHERE id = ? AND space_id = ?")
            .bind(sig)
            .bind(space)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// The headline correctness property: for any mixed record set,
    /// `SqliteChangeBackend`'s projection equals `fold(segments)` —
    /// same status, `closed_at`, `close_reason`, and reply parent linkage.
    ///
    /// Record set:
    /// - sig-a  CREATED (flag, r1, 1 000ms)  — open signal
    /// - sig-b  CREATED (flag, r2, 2 000ms)  — will be closed then reopened
    /// - sig-b  CLOSED  (r3, 3 000ms)
    /// - sig-b  REOPENED (r4, 4 000ms)        — final status: Open
    /// - sig-c  CREATED (flag, r5, 5 000ms)  — will stay closed
    /// - sig-c  CLOSED  (r6, 6 000ms, reason=2)
    /// - sig-d  CREATED (reply, r7, 7 000ms) → parent sig-a
    /// - sig-c  CREATED dup (`record_id` r5)   — must be a dedup no-op
    ///
    /// Applied in a genuinely shuffled (non-sorted) order, THROUGH the real
    /// segment codec (`SignalStore::append` → `SegmentStore::load`), so both
    /// `project_records` and the reference fold read the codec's output rather
    /// than an in-memory Vec. Assertions compare non-trivial discriminating
    /// fields for every signal.
    #[allow(clippy::too_many_lines, clippy::similar_names)]
    #[tokio::test]
    async fn test_projection_equals_segment_fold() {
        use kutl_proto::sync::signal::Payload;

        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "77777777-7777-7777-7777-777777777777";
        let space_uuid = uuid::Uuid::parse_str(space).unwrap();

        let rec_a_create = created_flag(space, "sig-a", "r1", 1_000);
        let rec_b_create = created_flag(space, "sig-b", "r2", 2_000);
        let rec_b_close = transition(space, "sig-b", "r3", SignalEventType::Closed, 3_000, 1);
        let rec_b_reopen = transition(space, "sig-b", "r4", SignalEventType::Reopened, 4_000, 0);
        let rec_c_create = created_flag(space, "sig-c", "r5", 5_000);
        let rec_c_close = transition(space, "sig-c", "r6", SignalEventType::Closed, 6_000, 2);
        let rec_d_create = created_reply(space, "sig-d", "r7", "sig-a", 7_000);
        // Duplicate: same `record_id` as rec_c_create — fold and projection
        // must both treat it as a set-union no-op.
        let rec_c_dup = created_flag(space, "sig-c", "r5", 5_500);

        // Genuinely shuffled: close before create for sig-b/sig-c, reply first,
        // duplicate sandwiched in the middle — NOT sorted by HLC or timestamp.
        let shuffled = vec![
            rec_c_close.clone(),  // sig-c CLOSED — arrives before sig-c's CREATED
            rec_d_create.clone(), // sig-d CREATED (reply)
            rec_b_close.clone(),  // sig-b CLOSED
            rec_c_dup.clone(),    // sig-c CREATED dup (record_id r5, must no-op)
            rec_a_create.clone(), // sig-a CREATED
            rec_b_reopen.clone(), // sig-b REOPENED
            rec_b_create.clone(), // sig-b CREATED
            rec_c_create.clone(), // sig-c CREATED (canonical r5)
        ];

        // Round-trip the whole set through the segment codec before projecting.
        let dir = TempDir::new().unwrap();
        let mut store = SignalStore::new(dir.path().join("signals"));
        for record in &shuffled {
            store.append(space_uuid, record).unwrap();
        }
        let loaded = store.load(space_uuid).unwrap();
        assert_eq!(
            loaded.records.len(),
            shuffled.len(),
            "every appended record must reload from segments"
        );

        backend.rebuild_space(space, &loaded.records).await.unwrap();

        // Compute the reference fold over the SAME reloaded record set.
        let mut fold = SpaceSignalState::default();
        for r in &loaded.records {
            fold.apply(r.clone());
        }

        // ── sig-a: CREATED flag, never transitioned → Open ──────────────────
        let sig_a = fold.get("sig-a").expect("fold must know sig-a");
        assert_eq!(sig_a.status, SignalStatus::Open, "sig-a: fold says Open");
        let (a_closed_at, a_reason) = projected_status(&pool, space, "sig-a").await;
        assert_eq!(
            a_closed_at,
            sig_a.closed_at_ms(),
            "sig-a: projection closed_at == fold"
        );
        assert_eq!(a_closed_at, None, "sig-a: open — closed_at must be None");
        assert_eq!(
            a_reason,
            sig_a.close_reason(),
            "sig-a: projection close_reason == fold"
        );

        // ── sig-b: CREATED + CLOSED(3k) + REOPENED(4k) → Open ───────────────
        let sig_b = fold.get("sig-b").expect("fold must know sig-b");
        assert_eq!(
            sig_b.status,
            SignalStatus::Open,
            "sig-b: REOPENED(4k) wins → Open"
        );
        let (b_closed_at, b_reason) = projected_status(&pool, space, "sig-b").await;
        assert_eq!(
            b_closed_at,
            sig_b.closed_at_ms(),
            "sig-b: projection closed_at == fold"
        );
        assert_eq!(
            b_closed_at, None,
            "sig-b: reopened — closed_at must be None"
        );
        assert_eq!(
            b_reason,
            sig_b.close_reason(),
            "sig-b: projection close_reason == fold"
        );

        // ── sig-c: CREATED + CLOSED(6k, reason=2), dup deduped → Closed ─────
        let sig_c = fold.get("sig-c").expect("fold must know sig-c");
        assert_eq!(
            sig_c.status,
            SignalStatus::Closed,
            "sig-c: CLOSED(6k) wins → Closed"
        );
        let (c_closed_at, c_reason) = projected_status(&pool, space, "sig-c").await;
        assert_eq!(
            c_closed_at,
            sig_c.closed_at_ms(),
            "sig-c: projection closed_at == fold"
        );
        assert_eq!(
            c_closed_at,
            Some(6_000),
            "sig-c: closed_at is the CLOSED transition's ms"
        );
        assert_eq!(
            c_reason,
            sig_c.close_reason(),
            "sig-c: projection close_reason == fold"
        );
        assert_eq!(
            c_reason,
            Some(2),
            "sig-c: close_reason = CLOSE_REASON_DECLINED"
        );

        // ── sig-d: CREATED reply → parent_signal_id == sig-a ────────────────
        let sig_d = fold.get("sig-d").expect("fold must know sig-d");
        assert_eq!(
            sig_d.status,
            SignalStatus::Open,
            "sig-d: reply never transitioned → Open"
        );
        // Verify the parent linkage survives the projection round-trip.
        let projected_parent_id = projected_parent(&pool, space, "sig-d").await;
        assert_eq!(
            projected_parent_id.as_deref(),
            Some("sig-a"),
            "sig-d: parent_signal_id must be sig-a in the projection"
        );
        // The reply payload in the fold's created record must also agree.
        match &sig_d.created.payload {
            Some(Payload::Reply(rp)) => {
                assert_eq!(
                    rp.parent_signal_id, "sig-a",
                    "sig-d fold: parent_signal_id must be sig-a"
                );
            }
            other => panic!("sig-d fold: expected Reply payload, got {other:?}"),
        }
        assert_eq!(
            projected_type(&pool, space, "sig-d").await.as_str(),
            "reply",
            "sig-d: signal_type must be 'reply' in the projection"
        );

        // ── dedup: record_id r5 appears twice; only 4 distinct signals ───────
        assert_eq!(
            fold.record_count(),
            7, // r1 r2 r3 r4 r5 r6 r7 — rec_c_dup shares r5's record_id
            "fold must count 7 distinct records (rec_c_dup is a dedup no-op)"
        );
        // Projection must yield exactly 4 rows (sig-a, sig-b, sig-c, sig-d).
        assert_eq!(
            row_count(&pool, space).await,
            4,
            "exactly 4 distinct signals must be projected"
        );
    }

    // -----------------------------------------------------------------------
    // Startup phantom-clear + prune never deletes truth
    // -----------------------------------------------------------------------

    use crate::signal_store::SignalStore;
    use tempfile::TempDir;

    /// Insert a legacy flag row directly (the pre-records shape the backfill
    /// reads from). Bypasses `record_signal` so the row carries no record
    /// envelope, exactly like a pre-records relay database.
    async fn insert_legacy_flag(
        pool: &SqlitePool,
        space: &str,
        id: &str,
        author: &str,
        ts: i64,
        message: &str,
    ) {
        // Carries a page key like any other row: the open-time backfill gives
        // one to every row that predates the column, so a fixture standing in
        // for a legacy row without one would be a shape the relay cannot hold.
        sqlx::query(&format!(
            "INSERT INTO signals
                (seq, id, space_id, document_id, author_did, signal_type,
                 timestamp, flag_kind, audience, message)
             VALUES ({NEXT_SEQ_EXPR}, ?, ?, ?, ?, 'flag', ?, 1, 0, ?)"
        ))
        .bind(space)
        .bind(id)
        .bind(space)
        .bind("doc-1")
        .bind(author)
        .bind(ts)
        .bind(message)
        .execute(pool)
        .await
        .unwrap();
    }

    /// The trap that keeps the phantom-clear's presence test a fold rather
    /// than a `WHERE record_id IS NULL` lookup.
    ///
    /// `record_signal` stamps `record_id` in the same commit as the row, and
    /// the segment append that precedes it is not fsynced. So a power cut can
    /// leave a row that carries a `record_id` naming a record the segments no
    /// longer hold — recordless in fact, but not by that column. Narrowing
    /// the trigger to the column would strand exactly this row as a permanent
    /// ghost: visible to every read, blocking a same-id retry, and cleared by
    /// nothing.
    #[tokio::test]
    async fn test_clear_removes_a_row_whose_record_id_outlived_its_record() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "77777777-7777-7777-7777-777777777777";

        // A create, projected the way production projects it — row and
        // record_id committed together.
        let created = test_row(
            "sig-crash",
            space,
            None,
            "did:alice",
            1_000,
            flag_payload(FlagKind::Info, None, "never acked"),
        );
        backend
            .project_record_for_test(&created, None)
            .await
            .unwrap();
        let stamped: Option<String> =
            sqlx::query_scalar("SELECT record_id FROM signals WHERE id = ? AND space_id = ?")
                .bind("sig-crash")
                .bind(space)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            stamped.as_deref(),
            Some(created.record_id.as_str()),
            "the row carries the record id even though the append was never fsynced"
        );

        // The power cut: the segment tail is gone, so the space folds empty
        // while the row survives with its record_id intact.
        let dir = TempDir::new().unwrap();
        let mut store = SignalStore::new(dir.path().join("signals"));
        let cleared = clear(&backend, space, &mut store).await;

        assert_eq!(
            cleared,
            vec!["sig-crash".to_string()],
            "a row whose record is gone must be cleared, however its record_id reads"
        );
        assert_eq!(row_count(&pool, space).await, 0, "the ghost row is gone");
    }

    /// Load the space's records and clear, the way startup does — one fold
    /// load, then the phantom-clear over it. Returns the cleared ids.
    async fn clear(
        backend: &SqliteChangeBackend,
        space: &str,
        store: &mut SignalStore,
    ) -> Vec<String> {
        let space_uuid = uuid::Uuid::parse_str(space).unwrap();
        let present = store.load_and_warn(space_uuid).unwrap().records;
        backend
            .clear_recordless_rows(space, &present)
            .await
            .unwrap()
    }

    /// The clear touches ONLY phantom rows: a row whose record is present in
    /// the fold survives, and a second pass over the healed state is a no-op.
    #[tokio::test]
    async fn test_clear_keeps_recorded_rows_and_is_idempotent() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "44444444-4444-4444-4444-444444444444";
        let space_uuid = uuid::Uuid::parse_str(space).unwrap();

        // One signal projected AND recorded — the healthy shape.
        let recorded = test_row(
            "sig-recorded",
            space,
            None,
            "did:alice",
            1_000,
            flag_payload(FlagKind::Info, None, "durable"),
        );
        backend
            .project_record_for_test(&recorded, None)
            .await
            .unwrap();
        let dir = TempDir::new().unwrap();
        let mut store = SignalStore::new(dir.path().join("signals"));
        store.append(space_uuid, &recorded).unwrap();

        // One phantom beside it — a row with no record anywhere.
        insert_legacy_flag(&pool, space, "sig-phantom", "did:key:zB", 2_000, "ghost").await;
        assert_eq!(row_count(&pool, space).await, 2);

        let cleared = clear(&backend, space, &mut store).await;
        assert_eq!(
            cleared,
            vec!["sig-phantom".to_string()],
            "only the phantom is cleared"
        );
        assert_eq!(
            row_count(&pool, space).await,
            1,
            "the recorded row survives the clear"
        );

        // Idempotent: nothing left to clear.
        let cleared_again = clear(&backend, space, &mut store).await;
        assert!(cleared_again.is_empty(), "second pass clears nothing");
        assert_eq!(row_count(&pool, space).await, 1);
    }

    /// prune never deletes signal rows by age (a window cannot hold: the next
    /// `project_records` refolds the complete segment set and resurrects any
    /// trimmed row). The segment records (the truth) are also untouched.
    #[tokio::test]
    async fn test_prune_does_not_delete_records() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "66666666-6666-6666-6666-666666666666";
        let space_uuid = uuid::Uuid::parse_str(space).unwrap();

        // An ancient signal, projected AND recorded — the row the OLD
        // age-prune would have deleted, with its segment record (the truth)
        // beside it.
        let old = test_row(
            "sig-old",
            space,
            None,
            "did:key:zA",
            1_000,
            flag_payload(FlagKind::Info, None, "old"),
        );
        backend.project_record_for_test(&old, None).await.unwrap();
        let dir = TempDir::new().unwrap();
        let mut store = SignalStore::new(dir.path().join("signals"));
        store.append(space_uuid, &old).unwrap();
        assert_eq!(
            store.load(space_uuid).unwrap().records.len(),
            1,
            "one segment record is the source of truth"
        );

        // Even a far-future `now` must NOT delete the ancient signal row.
        let far_future = 100_000_000_000i64;
        let pruned = backend.prune(far_future).await.unwrap();
        assert_eq!(pruned, 0, "prune trims no signal rows");
        assert_eq!(
            row_count(&pool, space).await,
            1,
            "prune must not delete signal rows (segments are truth)"
        );

        // The SEGMENTS/records are untouched — the truth survives and the
        // projection is fully re-derivable from it.
        assert_eq!(
            store.load(space_uuid).unwrap().records.len(),
            1,
            "prune never touches the segment records (the truth)"
        );
    }

    // -----------------------------------------------------------------------
    // Round-trip property: project(load(dir)) == fold(load(dir)), THROUGH the
    // real segment codec, over all event types (incl. TOMBSTONED) and all
    // payload kinds (Flag, Chat, Decision, Reply).
    // -----------------------------------------------------------------------

    /// Deterministic 64-bit PRNG (`SplitMix64`). Seeded from a fixed constant so
    /// the generated record mix is reproducible run-to-run — no thread-rng, no
    /// system clock (this repo bans nondeterministic sources in gate tests).
    struct SplitMix64(u64);

    impl SplitMix64 {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        /// A value in `0..bound` (bound must be non-zero).
        fn below(&mut self, bound: u64) -> u64 {
            self.next_u64() % bound
        }
    }

    /// The four `signal::Payload` kinds the fold can carry. Named so the
    /// generator can pick one per CREATED record and the assertion can
    /// reconstruct the expected proto payload.
    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    enum PayloadKind {
        Flag,
        Chat,
        Decision,
        Reply,
        /// A comment with a range anchor.
        Comment,
        /// A document-level comment: no anchor, which is a legitimate shape
        /// rather than a missing value.
        CommentNoAnchor,
    }

    /// Build a CREATED record for signal `sig` carrying `kind`'s payload.
    fn created_with_payload(
        space: &str,
        sig: &str,
        record_id: &str,
        kind: PayloadKind,
        ms: u64,
    ) -> Signal {
        use kutl_proto::sync::signal::Payload;
        let payload = match kind {
            // The deprecated audience pair is the shape under test: these fixtures
            // stand in for records already on disk.
            #[allow(deprecated)]
            PayloadKind::Flag => Payload::Flag(kutl_proto::sync::FlagPayload {
                kind: 1,
                audience_type: 2,
                target_did: Some(format!("did:key:zTarget-{sig}")),
                message: format!("flag body for {sig}"),
                audience: None,
                anchor_text: None,
            }),
            PayloadKind::Chat => Payload::Chat(kutl_proto::sync::ChatPayload {
                topic: Some(format!("topic for {sig}")),
                target_did: Some(format!("did:key:zChatTarget-{sig}")),
            }),
            PayloadKind::Decision => Payload::Decision(kutl_proto::sync::DecisionPayload {
                title_hash: format!("hash-{sig}"),
                title: format!("Decision {sig}"),
                depth: 3,
                parent_hash: Some(format!("parent-hash-{sig}")),
            }),
            PayloadKind::Reply => Payload::Reply(kutl_proto::sync::ReplyPayload {
                parent_signal_id: format!("parent-of-{sig}"),
                parent_reply_id: Some(format!("prev-reply-{sig}")),
                body: format!("reply body for {sig}"),
            }),
            // A comment is a FLAG carrying `FLAG_KIND_COMMENT` plus an anchor —
            // these two variants exist to keep `anchor_text`
            // round-tripping under test, including the document-level comment
            // whose anchor is legitimately absent.
            PayloadKind::Comment => {
                #[allow(deprecated)]
                let mut p = kutl_proto::sync::FlagPayload {
                    kind: i32::from(kutl_proto::sync::FlagKind::Comment),
                    audience_type: 0,
                    target_did: None,
                    message: format!("comment body for {sig}"),
                    audience: Some(kutl_proto::vocab::audience_from_untyped(
                        i32::from(kutl_proto::sync::AudienceType::Participant),
                        Some(&format!("did:key:zCommentTarget-{sig}")),
                    )),
                    anchor_text: Some(format!("the anchored text for {sig}")),
                };
                p.set_kind(kutl_proto::sync::FlagKind::Comment);
                Payload::Flag(p)
            }
            PayloadKind::CommentNoAnchor => {
                #[allow(deprecated)]
                let mut p = kutl_proto::sync::FlagPayload {
                    kind: i32::from(kutl_proto::sync::FlagKind::Comment),
                    audience_type: 0,
                    target_did: None,
                    message: format!("document-level comment for {sig}"),
                    audience: Some(kutl_proto::vocab::audience_from_untyped(
                        i32::from(kutl_proto::sync::AudienceType::Space),
                        None,
                    )),
                    anchor_text: None,
                };
                p.set_kind(kutl_proto::sync::FlagKind::Comment);
                Payload::Flag(p)
            }
        };
        let mut s = Signal {
            id: sig.into(),
            space_id: space.into(),
            document_id: Some(format!("doc-{sig}")),
            author_did: format!("did:key:zAuthor-{sig}"),
            timestamp: ms.cast_signed(),
            record_id: record_id.into(),
            actor_did: format!("did:key:zAuthor-{sig}"),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: ms,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            payload: Some(payload),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// Compare a projected row (as re-materialized by `into_proto`) against the
    /// fold's created payload — full payload-variant + field fidelity.
    fn assert_payload_round_trips(projected: &Signal, folded_created: &Signal) {
        use kutl_proto::sync::signal::Payload;
        match (projected.payload.as_ref(), folded_created.payload.as_ref()) {
            // The deprecated audience pair is the shape under test: these fixtures
            // stand in for records already on disk.
            #[allow(deprecated)]
            (Some(Payload::Flag(p)), Some(Payload::Flag(f))) => {
                assert_eq!(p.kind, f.kind, "flag kind");
                // Through the accessor, not the raw pair: the record carries the
                // TYPED audience while `into_proto` emits the untyped pair from the
                // projection columns, so the two agree on the audience without
                // agreeing field-for-field.
                assert_eq!(
                    kutl_proto::vocab::flag_audience_untyped(p),
                    kutl_proto::vocab::flag_audience_untyped(f),
                    "flag audience"
                );
                assert_eq!(p.message, f.message, "flag message");
                // `anchor_text` round-trips through the projection because the
                // RECORD carries it — the invariant the upsert's
                // COALESCE exists to preserve. Absent for every kind but comment,
                // and legitimately absent for a document-level comment.
                assert_eq!(p.anchor_text, f.anchor_text, "flag anchor_text");
            }
            (Some(Payload::Chat(p)), Some(Payload::Chat(f))) => {
                assert_eq!(p.topic, f.topic, "chat topic");
                assert_eq!(p.target_did, f.target_did, "chat target_did");
            }
            (Some(Payload::Decision(p)), Some(Payload::Decision(f))) => {
                assert_eq!(p.title_hash, f.title_hash, "decision title_hash");
                assert_eq!(p.title, f.title, "decision title");
                assert_eq!(p.depth, f.depth, "decision depth");
                assert_eq!(p.parent_hash, f.parent_hash, "decision parent_hash");
            }
            (Some(Payload::Reply(p)), Some(Payload::Reply(f))) => {
                assert_eq!(p.parent_signal_id, f.parent_signal_id, "reply parent");
                assert_eq!(p.parent_reply_id, f.parent_reply_id, "reply parent_reply");
                assert_eq!(p.body, f.body, "reply body");
            }
            (proj, fold) => panic!("payload variant mismatch: projected {proj:?} vs fold {fold:?}"),
        }
    }

    /// Read one projected row back into a proto `Signal` via the real query
    /// path, or `None` when the row is absent (tombstoned / never projected).
    async fn projected_signal(pool: &SqlitePool, space: &str, sig: &str) -> Option<Signal> {
        let row = sqlx::query_as::<_, SignalRow>(&format!(
            "SELECT {SIGNAL_COLUMNS} FROM signals_active WHERE id = ? AND space_id = ?"
        ))
        .bind(sig)
        .bind(space)
        .fetch_optional(pool)
        .await
        .unwrap();
        row.map(SignalRow::into_proto)
    }

    /// The headline correctness property, exercised THROUGH the segment codec:
    /// write a randomized (but deterministic) mix of records — all four event
    /// types incl. TOMBSTONED, all four payload kinds, multiple records per
    /// signal, in shuffled arrival order — via `SignalStore::append`, reload
    /// the space with `SegmentStore::load`, then assert the SQL projection of
    /// those loaded records equals folding the SAME loaded records:
    ///
    /// * every non-tombstoned signal's projected TYPE, close-state, and
    ///   payload-bearing fields match the fold (payload round-trips exactly);
    /// * every tombstoned signal is ABSENT from the projection.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_projection_equals_fold_through_segments() {
        // Fixed seed → reproducible record mix. Chosen arbitrarily; any
        // constant works because the assertion holds for every generated set.
        const SEED: u64 = 0x5EED_00AB_C0DE_F00D;
        /// How many distinct signals to exercise. Enough to cover every
        /// (payload kind × final status) combination with room to spare.
        const SIGNAL_COUNT: usize = 24;
        /// Max extra transition records appended after each signal's CREATED.
        const MAX_EXTRA_TRANSITIONS: u64 = 4;

        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "88888888-8888-8888-8888-888888888888";
        let space_uuid = uuid::Uuid::parse_str(space).unwrap();

        let dir = TempDir::new().unwrap();
        let mut store = SignalStore::new(dir.path().join("signals"));

        let mut prng = SplitMix64(SEED);
        let payload_kinds = [
            PayloadKind::Flag,
            PayloadKind::Chat,
            PayloadKind::Decision,
            PayloadKind::Reply,
            PayloadKind::Comment,
            PayloadKind::CommentNoAnchor,
        ];
        let transition_events = [
            SignalEventType::Closed,
            SignalEventType::Reopened,
            SignalEventType::Tombstoned,
        ];

        // Build every signal's records (CREATED + a random transition chain),
        // stamping HLCs from a single ever-advancing clock so LWW is
        // well-defined regardless of the later shuffle.
        let mut records: Vec<Signal> = Vec::new();
        let mut ms: u64 = 1_000;
        // Which payload kinds the seeded PRNG actually drew. Asserted below: with
        // a fixed seed, "every kind is covered" is a property of this test, not a
        // probabilistic hope — a new kind that the PRNG never happens to pick
        // would otherwise look verified while never being exercised.
        let mut kinds_drawn = std::collections::HashSet::new();
        for s in 0..SIGNAL_COUNT {
            let sig = format!("sig-{s:02}");
            let kind_idx = prng.below(u64::try_from(payload_kinds.len()).unwrap());
            let kind = payload_kinds[usize::try_from(kind_idx).unwrap()];
            kinds_drawn.insert(kind);
            ms += 1;
            records.push(created_with_payload(
                space,
                &sig,
                &format!("r-{s:02}-create"),
                kind,
                ms,
            ));
            let extra = prng.below(MAX_EXTRA_TRANSITIONS + 1);
            for t in 0..extra {
                let ev_idx = prng.below(u64::try_from(transition_events.len()).unwrap());
                let event = transition_events[usize::try_from(ev_idx).unwrap()];
                ms += 1;
                let close_reason = i32::try_from(prng.below(3)).unwrap();
                records.push(transition(
                    space,
                    &sig,
                    &format!("r-{s:02}-t{t}"),
                    event,
                    ms,
                    close_reason,
                ));
            }
        }
        assert_eq!(
            kinds_drawn.len(),
            payload_kinds.len(),
            "every payload kind must actually be exercised; drawn: {kinds_drawn:?}"
        );

        // Shuffle arrival order deterministically (Fisher–Yates with the PRNG),
        // then append every record through the real segment codec.
        for i in (1..records.len()).rev() {
            let j = usize::try_from(prng.below(u64::try_from(i + 1).unwrap())).unwrap();
            records.swap(i, j);
        }
        for record in &records {
            store.append(space_uuid, record).unwrap();
        }

        // Reload from segments — the projection and the reference fold BOTH read
        // the codec's output, not the in-memory Vec.
        let loaded = store.load(space_uuid).unwrap();
        assert_eq!(
            loaded.records.len(),
            records.len(),
            "every appended record must reload from segments"
        );

        backend.rebuild_space(space, &loaded.records).await.unwrap();

        let mut fold = SpaceSignalState::default();
        for r in &loaded.records {
            fold.apply(r.clone());
        }

        // Every folded signal: tombstoned ⇒ absent; otherwise the projected row
        // round-trips type, close-state, and payload fields.
        let mut visible = 0i64;
        for (id, folded) in fold.iter() {
            let projected = projected_signal(&pool, space, id).await;
            if folded.status == SignalStatus::Tombstoned {
                assert!(
                    projected.is_none(),
                    "tombstoned {id} must be absent from the projection"
                );
                continue;
            }
            visible += 1;
            let projected = projected
                .unwrap_or_else(|| panic!("non-tombstoned {id} must have a projected row"));

            // Close-state equivalence.
            let (closed_at, reason) = projected_status(&pool, space, id).await;
            assert_eq!(
                closed_at,
                folded.closed_at_ms(),
                "{id}: projected closed_at must equal the fold"
            );
            assert_eq!(
                reason,
                folded.close_reason(),
                "{id}: projected close_reason must equal the fold"
            );

            // Payload round-trip: variant + fields reconstructed by into_proto.
            assert_payload_round_trips(&projected, &folded.created);
        }

        // The projection holds EXACTLY the visible (non-tombstoned) signals —
        // no stale rows for tombstoned ones, no phantom rows.
        assert_eq!(
            row_count(&pool, space).await,
            visible,
            "projection row count must equal the visible signal count"
        );
    }

    /// Two DIFFERENT spaces can hold a signal row with the SAME id: the
    /// composite `(space_id, id)` PRIMARY KEY makes them distinct rows, and
    /// projecting one space must never overwrite or delete the other's row.
    /// This is the projection-layer half of the cross-tenant injection defense.
    #[tokio::test]
    async fn test_same_signal_id_isolated_across_spaces() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        let space_b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        let shared_id = "shared-signal-id";

        // Same signal id in each space, distinct messages.
        let mut a_rec = created_flag(space_a, shared_id, "ra", 1_000);
        if let Some(kutl_proto::sync::signal::Payload::Flag(f)) = a_rec.payload.as_mut() {
            f.message = "space A message".into();
        }
        let mut b_rec = created_flag(space_b, shared_id, "rb", 2_000);
        if let Some(kutl_proto::sync::signal::Payload::Flag(f)) = b_rec.payload.as_mut() {
            f.message = "space B message".into();
        }

        backend
            .rebuild_space(space_a, std::slice::from_ref(&a_rec))
            .await
            .unwrap();
        backend
            .rebuild_space(space_b, std::slice::from_ref(&b_rec))
            .await
            .unwrap();

        // Both rows coexist, each under its own space with its own message.
        assert_eq!(row_count(&pool, space_a).await, 1);
        assert_eq!(row_count(&pool, space_b).await, 1);
        let a_detail = backend.get_signal_detail(space_a, shared_id).await.unwrap();
        let b_detail = backend.get_signal_detail(space_b, shared_id).await.unwrap();
        assert_eq!(a_detail.message.as_deref(), Some("space A message"));
        assert_eq!(b_detail.message.as_deref(), Some("space B message"));

        // Tombstoning the signal in space A must NOT delete space B's row.
        let tomb = transition(
            space_a,
            shared_id,
            "ra2",
            SignalEventType::Tombstoned,
            3_000,
            0,
        );
        backend
            .rebuild_space(space_a, &[a_rec, tomb])
            .await
            .unwrap();
        assert_eq!(row_count(&pool, space_a).await, 0, "space A row tombstoned");
        assert_eq!(
            row_count(&pool, space_b).await,
            1,
            "space B's same-id row must survive a space-A tombstone"
        );
    }

    /// The FULL startup migration path upgrades a genuinely OLD pre-records
    /// legacy DB — an `id`-only-PK `signals` table that predates the
    /// `anchor_text`, close-state, and decision columns — to the current
    /// shape without losing a row or a column value.
    ///
    /// This seeds ONLY the columns a pre-records relay actually had (no
    /// `anchor_text` / `closed_at` / `close_reason` / `decision_*`), then runs
    /// `new()` so the production order fires: `CREATE TABLE IF NOT EXISTS`
    /// (no-op on the existing table) → the `ADD COLUMN` ALTERs (which add the
    /// missing derived columns to the OLD table) → the composite-PK rebuild
    /// (which now finds every `SIGNAL_COLUMNS` column present to copy). A test
    /// that pre-seeds the later columns would skip that ALTER-then-rebuild
    /// interaction and never prove a real upgrade.
    /// Seed a genuinely OLD pre-records `signals` table into `pool`: id-only PK,
    /// ONLY the columns that existed before the anchor /
    /// close-state / decision ALTERs, plus one flag row and one reply row. The
    /// later columns are DELIBERATELY absent so the full `new()` path must
    /// ADD them via ALTER before the composite-PK rebuild copies.
    async fn seed_pre_phase2_legacy_signals(pool: &SqlitePool) {
        sqlx::query(
            "CREATE TABLE signals (\
                id TEXT NOT NULL PRIMARY KEY, space_id TEXT NOT NULL, document_id TEXT,\
                author_did TEXT NOT NULL, signal_type TEXT NOT NULL, timestamp INTEGER NOT NULL,\
                flag_kind INTEGER, audience INTEGER, message TEXT, target_did TEXT,\
                parent_signal_id TEXT, parent_reply_id TEXT, body TEXT)",
        )
        .execute(pool)
        .await
        .unwrap();
        // Sanity: the seeded table really lacks the later columns, so the
        // test exercises the ALTER-then-rebuild path (not a pre-migrated shape).
        let legacy_cols: Vec<String> =
            sqlx::query_scalar("SELECT name FROM pragma_table_info('signals')")
                .fetch_all(pool)
                .await
                .unwrap();
        for absent in [
            "anchor_text",
            "closed_at",
            "close_reason",
            "decision_title_hash",
        ] {
            assert!(
                !legacy_cols.iter().any(|c| c == absent),
                "seed must be a genuine pre-records table (no {absent} column)"
            );
        }
        // A flag and a reply row so both the base and threaded columns are
        // exercised across the copy.
        sqlx::query(
            "INSERT INTO signals \
                 (id, space_id, document_id, author_did, signal_type, timestamp, flag_kind, \
                  audience, message, target_did)\
             VALUES ('sig-legacy-1', 'space-x', 'doc-x', 'did:key:zA', 'flag', 1000, 3, 0, \
                     'one', 'did:key:zTarget')",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO signals \
                 (id, space_id, author_did, signal_type, timestamp, parent_signal_id, body)\
             VALUES ('sig-legacy-2', 'space-x', 'did:key:zB', 'reply', 2000, \
                     'sig-legacy-1', 'a reply body')",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_migrate_composite_pk_preserves_rows() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        seed_pre_phase2_legacy_signals(&pool).await;

        // `new()` runs the FULL migration: CREATE-if-not-exists → ALTERs →
        // composite-PK rebuild, in production order.
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();

        // Every legacy row survived the rebuild-copy.
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signals")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 2, "rebuild-copy must preserve every existing row");

        // The PK is now composite: space_id is a PK member.
        let space_pk: i64 = sqlx::query_scalar(
            "SELECT pk FROM pragma_table_info('signals') WHERE name = 'space_id'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(space_pk > 0, "space_id must be part of the PRIMARY KEY");

        // The ALTER added every later column and the rebuild kept them.
        let migrated_cols: Vec<String> =
            sqlx::query_scalar("SELECT name FROM pragma_table_info('signals')")
                .fetch_all(&pool)
                .await
                .unwrap();
        for added in [
            "anchor_text",
            "closed_at",
            "close_reason",
            "decision_title_hash",
            "decision_title",
            "decision_depth",
            "decision_parent_hash",
            // The rebuild builds its scratch table from the same shared
            // definition the open-time CREATE uses, so a newly-added column
            // cannot be silently dropped by the rename that follows.
            "record_id",
        ] {
            assert!(
                migrated_cols.iter().any(|c| c == added),
                "migrated table must carry the {added} column"
            );
        }

        // The flag row's every seeded column value survived the copy.
        let flag = backend
            .get_signal_detail("space-x", "sig-legacy-1")
            .await
            .unwrap();
        assert_eq!(flag.message.as_deref(), Some("one"));
        // flag_kind/audience are exposed as canonical NAMES, not enum
        // ordinals. Seed values: flag_kind=3 (review_requested), audience=0
        // (unspecified → participant).
        assert_eq!(flag.flag_kind.as_deref(), Some("review_requested"));
        assert_eq!(flag.audience.as_deref(), Some("participant"));
        assert_eq!(flag.target_did.as_deref(), Some("did:key:zTarget"));
        // The pre-existing (null) derived columns copied across as NULL, not a
        // spurious value.
        assert_eq!(flag.closed_at, None, "closed_at must remain NULL post-copy");
        // The reply row's threaded columns survived too.
        assert_eq!(
            flag.replies.len(),
            1,
            "the reply row must survive and thread to its parent"
        );
        assert_eq!(flag.replies[0].body, "a reply body");

        // Idempotent: a second construction on the already-migrated DB no-ops.
        let _again = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let count_again: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signals")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count_again, 2, "second migration must be a no-op");
    }

    /// Rows that predate the change feed's page key still reach a reader.
    ///
    /// The backfill seeds each one from the order the table already holds
    /// them, so a database upgraded in place serves its whole history rather
    /// than starting from whatever lands next.
    #[tokio::test]
    async fn test_migrated_legacy_rows_are_delivered_by_the_feed() {
        let pool = test_pool().await;
        seed_pre_phase2_legacy_signals(&pool).await;

        let backend = SqliteChangeBackend::new(pool).await.unwrap();
        let page = backend
            .get_changes("did:alice", "space-x", None)
            .await
            .unwrap();
        assert_eq!(
            page.signals
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["sig-legacy-1", "sig-legacy-2"],
            "every pre-existing row must be deliverable, in the order it was stored"
        );
    }

    /// A cursor row left by a build that paged on a millisecond is discarded.
    ///
    /// Kept, it would sit above every page key its space will ever hold and
    /// that reader would be handed nothing for the rest of the deployment's
    /// life — with no error anywhere to say so. Resuming from the start of the
    /// space instead re-delivers, which a reader can absorb.
    #[tokio::test]
    async fn test_cursors_keyed_on_a_millisecond_are_discarded() {
        let pool = test_pool().await;
        sqlx::query(
            "CREATE TABLE change_cursors (
                did              TEXT    NOT NULL,
                space_id         TEXT    NOT NULL,
                signal_cursor    INTEGER NOT NULL DEFAULT 0,
                reg_cursor       INTEGER NOT NULL DEFAULT 0,
                prev1_signal     INTEGER NOT NULL DEFAULT 0,
                prev1_reg        INTEGER NOT NULL DEFAULT 0,
                prev2_signal     INTEGER NOT NULL DEFAULT 0,
                prev2_reg        INTEGER NOT NULL DEFAULT 0,
                signal_cursor_id TEXT    NOT NULL DEFAULT '',
                checkpoint       TEXT    NOT NULL DEFAULT '',
                updated_at       INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (did, space_id)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        // A wall-clock millisecond: astronomically above any page key.
        sqlx::query("INSERT INTO change_cursors (did, space_id, signal_cursor) VALUES (?, ?, ?)")
            .bind("did:alice")
            .bind("space-1")
            .bind(1_754_500_000_000_i64)
            .execute(&pool)
            .await
            .unwrap();

        let backend = SqliteChangeBackend::new(pool).await.unwrap();
        record_signals_at(&backend, "space-1", "sig", 1, 1000).await;

        let page = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(
            page.signals
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["sig-0"],
            "a reader carrying a millisecond cursor must not be starved"
        );
    }

    /// A checkpoint whose numbers mean something other than a position in this
    /// feed — a wall-clock millisecond, say — parses, so shape alone cannot
    /// refuse it. What refuses it is that it names no position this reader's
    /// ring holds: the read continues from where the reader actually is rather
    /// than seeking to a place the number does not describe.
    #[tokio::test]
    async fn test_get_changes_checkpoint_from_a_millisecond_cursor_does_not_rewind() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool).await.unwrap();

        record_signals_at(&backend, "space-1", "sig", 1, 1000).await;
        let first = backend
            .get_changes("did:alice", "space-1", None)
            .await
            .unwrap();
        assert_eq!(first.signals.len(), 1);

        let stale = backend
            .get_changes("did:alice", "space-1", Some("1754500000000:0"))
            .await
            .unwrap();
        assert!(
            stale.signals.is_empty(),
            "a checkpoint from another cursor meaning must not move this reader"
        );
    }

    /// A tombstone applied AFTER a signal was already projected (Open/Closed)
    /// must HIDE its projection row on the next rebuild, not leave a stale
    /// visible row (deletion-ladder rung 1).
    #[tokio::test]
    async fn test_projection_hides_tombstoned_row_after_prior_project() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "99999999-9999-9999-9999-999999999999";

        let create = created_flag(space, "sig-t", "r1", 1_000);
        // First projection: the signal is Open and visible.
        backend
            .rebuild_space(space, std::slice::from_ref(&create))
            .await
            .unwrap();
        assert_eq!(row_count(&pool, space).await, 1, "signal starts visible");

        // A later tombstone arrives; re-project the full set.
        let tomb = transition(space, "sig-t", "r2", SignalEventType::Tombstoned, 2_000, 0);
        backend.rebuild_space(space, &[create, tomb]).await.unwrap();
        assert_eq!(
            row_count(&pool, space).await,
            0,
            "tombstone must delete the previously-projected row"
        );
    }

    // -----------------------------------------------------------------------
    // Startup reconcile heals crash-tail projection divergence
    // (a lost segment tail that the committed projection contradicts).
    // -----------------------------------------------------------------------

    /// Crash-tail close-state divergence: a signal is CREATED then CLOSED, so
    /// the projection row records `closed_at`. A power loss then drops the
    /// CLOSED record from the active segment tail (`truncate_active_segment_tail`),
    /// so the segment fold now says the signal is OPEN — but the stale projection
    /// still reports CLOSED forever, because nothing refolds the segments at
    /// startup.
    ///
    /// End-to-end heal check: `reconcile_space_projection` over the truncated
    /// segments rebuilds the projection from the fold and the row's `closed_at`
    /// clears (Open). This exercises the crash-tail fault path through the real
    /// segment codec; the phantom-row sibling test below is the discriminator the
    /// per-mutation `project_records` upsert cannot satisfy (it never deletes a
    /// row merely absent from the fold), while this close-state case verifies the
    /// startup refold produces the correct OPEN result.
    #[tokio::test]
    async fn test_reconcile_heals_crash_tail_lost_close_record() {
        // Chop the active-segment tail: model a power loss after the CLOSED
        // record's frame was partially written. 16 bytes shears the final
        // record's frame (see the fault-harness self-tests).
        const CHOP: u64 = 16;

        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "abababab-abab-abab-abab-abababababab";
        let space_uuid = uuid::Uuid::parse_str(space).unwrap();

        let create = created_flag(space, "sig-x", "r1", 1_000);
        let close = transition(space, "sig-x", "r2", SignalEventType::Closed, 2_000, 1);

        // Write CREATED + CLOSED through the real segment codec, then project
        // the loaded records so the committed projection row shows CLOSED.
        let dir = TempDir::new().unwrap();
        let signals_root = dir.path().join("signals");
        let mut store = SignalStore::new(signals_root.clone());
        store.append(space_uuid, &create).unwrap();
        store.append(space_uuid, &close).unwrap();
        drop(store); // release the per-space segment LOCK before re-reading.

        let store = SignalStore::new(signals_root.clone());
        let before = store.load(space_uuid).unwrap();
        assert_eq!(before.records.len(), 2, "CREATED + CLOSED on disk");
        backend.rebuild_space(space, &before.records).await.unwrap();
        let (closed_at, reason) = projected_status(&pool, space, "sig-x").await;
        assert_eq!(closed_at, Some(2_000), "projection row shows CLOSED");
        assert_eq!(reason, Some(1));

        // Power loss: the CLOSED record's frame is torn off the active segment.
        let space_dir = signals_root.join(space_uuid.to_string());
        drop(store);
        kutl_signals::testing::truncate_active_segment_tail(&space_dir, CHOP).unwrap();

        // The fold of the surviving segments now says OPEN (CLOSED is gone).
        let store = SignalStore::new(signals_root.clone());
        let after_crash = store.load(space_uuid).unwrap();
        assert_eq!(
            after_crash.records.len(),
            1,
            "the CLOSED tail record must be gone after truncation"
        );
        let mut fold = SpaceSignalState::default();
        for r in &after_crash.records {
            fold.apply(r.clone());
        }
        assert_eq!(
            fold.get("sig-x").unwrap().status,
            SignalStatus::Open,
            "segment fold now says the signal is OPEN"
        );

        // The stale projection STILL says CLOSED (the divergence the fix heals).
        let (stale_closed_at, _) = projected_status(&pool, space, "sig-x").await;
        assert_eq!(
            stale_closed_at,
            Some(2_000),
            "before reconcile, the projection still contradicts the fold (CLOSED)"
        );

        // Reconcile from the truncated segment fold — the row must become OPEN.
        backend
            .reconcile_space_projection(space, &after_crash.records)
            .await
            .unwrap();
        let (healed_closed_at, healed_reason) = projected_status(&pool, space, "sig-x").await;
        assert_eq!(
            healed_closed_at, None,
            "reconcile heals the row to OPEN (closed_at cleared)"
        );
        assert_eq!(healed_reason, None, "close_reason cleared with the heal");
        assert_eq!(
            row_count(&pool, space).await,
            1,
            "the (now-open) signal is still present, just no longer closed"
        );
    }

    /// A projection row whose signal id is ABSENT from the segment fold (its
    /// only CREATED record was lost on a crash tail, or it is an un-backfilled
    /// legacy row) is the SOLE surviving copy of that user data — reconcile must
    /// PRESERVE it, never delete it.
    ///
    /// A delete-all-then-reinsert reconcile would wipe every
    /// row absent from the fold; because backfill reads its source from this
    /// same `signals` table, that data would then be unrecoverable. Reconcile
    /// upserts the fold's signals and deletes only tombstoned ids, so an absent
    /// row survives (identical semantics to `project_records`).
    #[tokio::test]
    async fn test_reconcile_preserves_row_absent_from_fold() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd";

        // Two signals projected; only one survives in the fold.
        let keep = created_flag(space, "sig-keep", "r1", 1_000);
        let absent = created_flag(space, "sig-absent", "r2", 2_000);
        backend
            .rebuild_space(space, &[keep.clone(), absent])
            .await
            .unwrap();
        assert_eq!(row_count(&pool, space).await, 2, "both rows projected");

        // The fold's truth contains only `sig-keep` (`sig-absent`'s record was
        // lost on crash / never backfilled). Its projection row is the last copy.
        backend
            .reconcile_space_projection(space, std::slice::from_ref(&keep))
            .await
            .unwrap();
        assert_eq!(
            row_count(&pool, space).await,
            2,
            "the row absent from the fold must be PRESERVED (its only surviving copy)"
        );
        let keep_detail = backend.get_signal_detail(space, "sig-keep").await.unwrap();
        assert_eq!(keep_detail.id, "sig-keep", "the folded signal remains");
        let absent_detail = backend
            .get_signal_detail(space, "sig-absent")
            .await
            .unwrap();
        assert_eq!(
            absent_detail.id, "sig-absent",
            "the absent-from-fold signal's row must survive the reconcile"
        );
    }

    /// Reconcile must not null out `anchor_text` (the posterity
    /// snapshot, display-only, never in a segment record) on every restart. A
    /// delete-all-then-reinsert would drop the row and re-`INSERT` a
    /// segment-derived row whose `anchor_text` is always NULL; the upsert path
    /// preserves it via `ON CONFLICT ... DO UPDATE` (which omits the column).
    #[tokio::test]
    async fn test_reconcile_preserves_anchor_text() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "efefefef-efef-efef-efef-efefefefefef";

        // Project a signal whose CREATED record is in the segment fold, then
        // stamp a non-null anchor_text onto its row (as the create path does).
        let create = created_flag(space, "sig-anchor", "r1", 1_000);
        backend
            .rebuild_space(space, std::slice::from_ref(&create))
            .await
            .unwrap();
        sqlx::query("UPDATE signals SET anchor_text = ? WHERE space_id = ? AND id = ?")
            .bind("the quoted paragraph")
            .bind(space)
            .bind("sig-anchor")
            .execute(&pool)
            .await
            .unwrap();

        // Reconcile from the fold (which carries no anchor_text) must keep it.
        backend
            .reconcile_space_projection(space, std::slice::from_ref(&create))
            .await
            .unwrap();

        let anchor: Option<String> =
            sqlx::query_scalar("SELECT anchor_text FROM signals WHERE space_id = ? AND id = ?")
                .bind(space)
                .bind("sig-anchor")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            anchor.as_deref(),
            Some("the quoted paragraph"),
            "reconcile must PRESERVE anchor_text, not null it on restart"
        );
    }

    /// A rebuild from the fold must carry an EDITED record's content, not
    /// revert the row to birth content.
    ///
    /// The incremental EDITED arm and the fold-backed rebuild are two writers
    /// of the same row: without the content track in
    /// `ProjectedRow::from_folded`, every startup reconcile would silently
    /// undo every edit — a defect that presents as success, since the rebuild
    /// completes and the row is well-formed, just stale.
    #[tokio::test]
    async fn test_rebuild_carries_edited_content_not_birth() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "edededed-eded-eded-eded-edededededed";

        let mut create = created_flag(space, "sig-edited", "r1", 1_000);
        if let Some(kutl_proto::sync::signal::Payload::Flag(f)) = &mut create.payload {
            f.anchor_text = Some("the birth anchor".into());
        }
        let mut edit = created_flag(space, "sig-edited", "r2", 2_000);
        edit.set_event(SignalEventType::Edited);
        if let Some(kutl_proto::sync::signal::Payload::Flag(f)) = &mut edit.payload {
            f.message = "please review the NEW title".into();
            f.anchor_text = None; // NOT carried — must persist from birth
        }

        backend.rebuild_space(space, &[create, edit]).await.unwrap();

        let (message, anchor): (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT message, anchor_text FROM signals WHERE space_id = ? AND id = ?",
        )
        .bind(space)
        .bind("sig-edited")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            message.as_deref(),
            Some("please review the NEW title"),
            "a fold-backed rebuild must project the content track, not birth content"
        );
        assert_eq!(
            anchor.as_deref(),
            Some("the birth anchor"),
            "a field the edit did not carry must PERSIST through a rebuild — \
             the uncarried half of the overlay rule"
        );
    }

    /// The decision half of the incremental EDITED arm: a full-payload
    /// replacement of all four content columns on this substrate too (the
    /// conformance edit case is flag-shaped).
    #[tokio::test]
    async fn test_edited_decision_replaces_decision_columns() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "edededed-eded-eded-eded-ededededed01";

        let decision = |record_id: &str, title: &str, hash: &str, depth: i32, ms: u64| {
            let mut s = created_flag(space, "sig-dec", record_id, ms);
            s.payload = Some(kutl_proto::sync::signal::Payload::Decision(
                kutl_proto::sync::DecisionPayload {
                    title_hash: hash.into(),
                    title: title.into(),
                    depth,
                    parent_hash: None,
                },
            ));
            s
        };
        let create = decision("r1", "Which database?", "hash-v1", 2, 1_000);
        let mut edit = decision("r2", "Which database engine?", "hash-v2", 3, 2_000);
        edit.set_event(SignalEventType::Edited);

        ProjectionWriter::project_record(&backend, space, &create, None)
            .await
            .unwrap();
        ProjectionWriter::project_record(&backend, space, &edit, None)
            .await
            .unwrap();

        let (title, hash, depth): (Option<String>, Option<String>, Option<i32>) = sqlx::query_as(
            "SELECT decision_title, decision_title_hash, decision_depth \
               FROM signals WHERE space_id = ? AND id = ?",
        )
        .bind(space)
        .bind("sig-dec")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(title.as_deref(), Some("Which database engine?"));
        assert_eq!(hash.as_deref(), Some("hash-v2"));
        assert_eq!(depth, Some(3));
    }

    /// A tombstone rebuilt onto a projection that has NO row yet must still
    /// leave one — hidden.
    ///
    /// The gap under test: a rebuild that BRANCHES on Tombstoned and issues
    /// an `UPDATE` matches nothing when no row exists, then skips
    /// the insert. The signal ends up in the record log with no row at
    /// all. That is not merely a missing row: `signal_exists` deliberately
    /// reads the BASE table so that hiding a signal does not release its id,
    /// so with no row the duplicate-create guard hands the id straight back
    /// out and a second CREATED can be authored under it.
    ///
    /// Reachable from the startup reconcile, which folds a space's records
    /// against a projection that may legitimately be empty. The other
    /// tombstone tests seed the row with a prior CREATED-only rebuild first;
    /// this one starts from an empty projection.
    #[tokio::test]
    async fn test_rebuild_tombstone_onto_empty_projection_leaves_a_hidden_row() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "7c7c7c7c-7c7c-7c7c-7c7c-7c7c7c7c7c7c";

        // Straight to [CREATED, TOMBSTONED] with nothing projected beforehand.
        backend
            .rebuild_space(
                space,
                &[
                    created_flag(space, "sig-t", "r1", 1_000),
                    transition(space, "sig-t", "r2", SignalEventType::Tombstoned, 2_000, 0),
                ],
            )
            .await
            .unwrap();

        assert_eq!(row_count(&pool, space).await, 0, "tombstoned: not visible");
        assert_eq!(
            stored_count(&pool, space).await,
            1,
            "tombstoned: the row must still EXIST, hidden — its id stays reserved"
        );
        assert!(
            backend.signal_exists(space, "sig-t").await.unwrap(),
            "a hidden signal still owns its id; releasing it would admit a \
             second CREATED under the same id"
        );

        // And the rebuild can bring it back, because hidden-ness is a folded
        // column rather than a branch.
        backend
            .rebuild_space(
                space,
                &[
                    created_flag(space, "sig-t", "r1", 1_000),
                    transition(space, "sig-t", "r2", SignalEventType::Tombstoned, 2_000, 0),
                    transition(space, "sig-t", "r3", SignalEventType::Reopened, 3_000, 0),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            row_count(&pool, space).await,
            1,
            "reopen un-hides on rebuild"
        );
    }

    /// A signal whose fold status is Tombstoned must have its projection row
    /// HIDDEN by reconcile (deletion-ladder rung 1), even though
    /// reconcile still never clears rows merely absent from the fold.
    #[tokio::test]
    async fn test_reconcile_hides_tombstoned_row() {
        let pool = test_pool().await;
        let backend = SqliteChangeBackend::new(pool.clone()).await.unwrap();
        let space = "45454545-4545-4545-4545-454545454545";

        let create = created_flag(space, "sig-tomb", "r1", 1_000);
        backend
            .rebuild_space(space, std::slice::from_ref(&create))
            .await
            .unwrap();
        assert_eq!(row_count(&pool, space).await, 1, "signal starts visible");

        // The fold now folds to Tombstoned; reconcile must delete the row.
        let tomb = transition(
            space,
            "sig-tomb",
            "r2",
            SignalEventType::Tombstoned,
            2_000,
            0,
        );
        backend
            .reconcile_space_projection(space, &[create, tomb])
            .await
            .unwrap();
        assert_eq!(
            row_count(&pool, space).await,
            0,
            "a tombstoned signal's projection row must be deleted by reconcile"
        );
    }
}
