//! The relay's signal record log.
//!
//! Where a relay durably keeps signal records, behind a trait so a deployment
//! can use what it actually has: append-only segment files for a self-hoster
//! with a disk, managed Postgres for the hosted product.
//!
//! **The split is client-vs-server, not OSS-vs-commercial.** Clients keep
//! inspectable files, because that is the product — the daemon writes
//! `<space_root>/.kutl/signals/` and always will. Servers use whatever their
//! deployment durably provides. Two implementations existing for *different
//! reasons* — user-facing inspectability and operational durability — is what a
//! trait is for.
//!
//! Deliberately **not** on `ChangeBackend`: that is the projection layer, and
//! the projection is a rebuildable derivative of this log rather than a peer of
//! it. Deliberately **unsealed and implementable out-of-crate**, exactly like
//! `ChangeBackend`: the Postgres implementation is proprietary and lives
//! outside this crate.
//!
//! It is relay-side only. The daemon's store
//! (`kutl-daemon/src/signal_store.rs`) is single-space and flocked to one
//! working tree; this one is a multi-space registry. Different cardinality,
//! different concurrency; the genuinely shared part already lives in
//! `kutl-signals`.
//!
//! # Why the log is load-bearing
//!
//! Catch-up is the only way a client obtains historical records — live
//! broadcast covers only the window it was connected for. A relay without a log
//! leaves a newly-joined device, or one offline during activity, with no path
//! to a space's signal history at all.

use std::sync::Mutex;

use async_trait::async_trait;
use kutl_proto::sync::{Hlc, Signal};
use uuid::Uuid;

use crate::signal_store::{SignalStore, SignalStoreError};

/// Errors from record log operations.
#[derive(Debug, thiserror::Error)]
pub enum RecordLogError {
    /// A segment read or write failed.
    #[error("{0}")]
    Segment(#[from] SignalStoreError),
    /// A backing store outside this crate failed — the Postgres log, say.
    /// Carries the implementation's own message.
    #[error("record log backend error: {0}")]
    Backend(String),
}

/// A relay's durable home for signal records.
///
/// One read primitive and one write primitive, plus the two operations a
/// per-space file handle needs and a Postgres table does not. Paging is
/// deliberately absent: it lives in `kutl_signals::catchup`, over the records
/// this returns, so every substrate pages identically and the boundary rule
/// (complete `physical_ms` groups, strict resume) exists once.
#[async_trait]
pub trait RecordLog: Send + Sync {
    /// Append one record to `space`'s log.
    ///
    /// Callers reach this through the relay's admission seams, never directly —
    /// that is what makes "every record was admitted by something" checkable
    /// rather than reviewable.
    async fn append(&self, space: Uuid, record: &Signal) -> Result<(), RecordLogError>;

    /// Every record this log holds for `space`, in storage order.
    ///
    /// The fold is order-insensitive, so "storage order" is a convenience for
    /// the pager rather than a correctness requirement.
    ///
    /// **This is the whole-space read.** Rebuilds and folds need it; the pager
    /// does not — see [`Self::list`].
    async fn load_space(&self, space: Uuid) -> Result<Vec<Signal>, RecordLogError>;

    /// Every record this log holds for one DOCUMENT in `space`.
    ///
    /// The marker-baseline seed needs exactly one document's history, and it
    /// runs once per document rather than once per space. The default reads
    /// the whole space and filters in memory — correct everywhere, and no
    /// worse than what a sequential file read would do anyway. A
    /// database-backed log should override it with an indexed read, for the
    /// same reason [`Self::list`] exists rather than paging off
    /// [`Self::load_space`]: filtering a whole space per document turns each
    /// seed into a full-history scan, so the cost of touching one document
    /// grows with the space's entire history.
    async fn load_document(
        &self,
        space: Uuid,
        document_id: &str,
    ) -> Result<Vec<Signal>, RecordLogError> {
        Ok(self
            .load_space(space)
            .await?
            .into_iter()
            .filter(|r| r.document_id.as_deref() == Some(document_id))
            .collect())
    }

    /// Every record this log holds for one SIGNAL in `space` — its CREATED
    /// and every transition and edit after it.
    ///
    /// The transition-trail read needs exactly one signal's history, once per
    /// detail request. The default reads the whole space and filters in
    /// memory — correct everywhere and no worse than a sequential file read —
    /// but a database-backed log should override it with an indexed read, for
    /// the reason [`Self::load_document`] gives: filtering a whole space per
    /// signal turns every detail request into a full-history scan and decode,
    /// so the cost of looking at one signal grows with the space's entire
    /// history.
    async fn load_signal(
        &self,
        space: Uuid,
        signal_id: &str,
    ) -> Result<Vec<Signal>, RecordLogError> {
        Ok(self
            .load_space(space)
            .await?
            .into_iter()
            .filter(|r| r.id == signal_id)
            .collect())
    }

    /// The next page's worth of records after `since`, in fold order — the
    /// primitive `SignalPage` is served from.
    ///
    /// Separate from [`Self::load_space`] because the two substrates have
    /// genuinely different costs, which is why the trait is paged rather than
    /// one loader. Serving a 200-record page by fetching every record in the
    /// space and discarding the rest is free on a sequential file read and
    /// quadratic on a database: every subscribe would fetch and protobuf-decode
    /// the entire space to send its first page, so the cost of joining a space
    /// would grow with that space's whole history.
    ///
    /// # The contract, which is not simply `LIMIT`
    ///
    /// Return every record strictly after `since` in fold order
    /// `(hlc, actor_did, record_id)`, truncated to `limit` records **plus the
    /// remainder of the `physical_ms` group the `limit`-th record belongs to**.
    ///
    /// The group completion is load-bearing, not a nicety. `catchup::page`
    /// emits only COMPLETE `physical_ms` groups and judges completeness from the
    /// slice handed to it; a slice cut mid-group reads as a finished group, so
    /// the pager emits it and advances the cursor past records it never sent.
    /// That is a hole in a client's history, and a silent one — every frame
    /// involved is well-formed. [`kutl_signals::catchup::take_with_complete_boundary`]
    /// is the rule in executable form, and the segment implementation *is* that
    /// call; a SQL implementation must reproduce it.
    ///
    /// Callers pass one more than the page size they mean to serve, because
    /// `page` reports `more` as "the slice held records past the cut" and cannot
    /// otherwise tell a full page from the last one.
    ///
    /// `since = None` is a from-zero page and includes missing-HLC records,
    /// which fold to `physical_ms = 0`; `Some(floor)` is strictly after
    /// `floor.physical_ms` and therefore excludes them.
    async fn list(
        &self,
        space: Uuid,
        since: Option<&Hlc>,
        limit: usize,
    ) -> Result<Vec<Signal>, RecordLogError>;

    /// Release any per-space resources.
    ///
    /// A segment log holds an open writer per space and must close it when the
    /// space goes away; a Postgres log holds nothing and no-ops.
    async fn evict(&self, space: Uuid);

    /// Does this log currently hold an open writer for `space`?
    ///
    /// The observable behind [`Self::evict`], which otherwise has no visible
    /// effect to assert. Backends with no per-space handle always report
    /// `false`, which is the truth for them rather than a stub.
    fn has_open_writer(&self, space: Uuid) -> bool;
}

/// The append-only segment implementation — a self-hoster's disk.
///
/// Wraps [`SignalStore`] in a mutex because the trait hands out `&self` while
/// the store's writer cache needs `&mut`. The lock is never held across an
/// await: every segment operation is synchronous file IO, so the guard is taken
/// and dropped inside one statement.
pub struct SegmentRecordLog {
    store: Mutex<SignalStore>,
}

impl SegmentRecordLog {
    /// Build a segment log over an existing store.
    #[must_use]
    pub fn new(store: SignalStore) -> Self {
        Self {
            store: Mutex::new(store),
        }
    }

    /// Build a segment log rooted at `<data_dir>/signals`.
    #[must_use]
    pub fn rooted_at(data_dir: &std::path::Path) -> Self {
        Self::new(SignalStore::new(SignalStore::root_for(data_dir)))
    }

    /// The store, or a panic if a previous holder panicked mid-append.
    ///
    /// A poisoned lock here means a segment write panicked, which is not a
    /// state to limp on: the relay's durable log would be of unknown
    /// consistency and every subsequent append would be guesswork.
    fn locked(&self) -> std::sync::MutexGuard<'_, SignalStore> {
        self.store.lock().expect("signal store lock poisoned")
    }
}

#[async_trait]
impl RecordLog for SegmentRecordLog {
    async fn append(&self, space: Uuid, record: &Signal) -> Result<(), RecordLogError> {
        self.locked().append(space, record)?;
        Ok(())
    }

    async fn load_space(&self, space: Uuid) -> Result<Vec<Signal>, RecordLogError> {
        // `load_and_warn` rather than `load`: a quarantined segment is a
        // diagnostic this substrate has and Postgres does not, so it is warned
        // about here rather than widened into the trait.
        Ok(self.locked().load_and_warn(space)?.records)
    }

    async fn list(
        &self,
        space: Uuid,
        since: Option<&Hlc>,
        limit: usize,
    ) -> Result<Vec<Signal>, RecordLogError> {
        // Reads the whole space and slices, which is not a shortcut — segments
        // are append-only files read sequentially, so there is no partial read
        // to be had and a "paged" file read would be the same I/O plus seeking.
        // The win this method exists for is the database's; here it buys a
        // uniform call site and, more usefully, makes the two substrates
        // answerable to one differential test.
        let all = self.load_space(space).await?;
        Ok(
            kutl_signals::catchup::take_with_complete_boundary(&all, since, limit)
                .into_iter()
                .cloned()
                .collect(),
        )
    }

    async fn evict(&self, space: Uuid) {
        self.locked().evict(space);
    }

    fn has_open_writer(&self, space: Uuid) -> bool {
        self.locked().has_writer(space)
    }
}
