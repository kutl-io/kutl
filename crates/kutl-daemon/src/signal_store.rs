//! Per-space append-only signal segment store for the daemon.
//! Segments live under the space's working tree at
//! `<space_root>/.kutl/signals/<space_id>/`, colocated with the daemon's
//! other per-space state so a single directory captures everything the
//! daemon persists for a space.
//!
//! A [`DaemonSignalStore`] is a single-writer: on `open` it takes an
//! exclusive `flock` on `<space_root>/.kutl/signals/LOCK`, held for the
//! store's lifetime, so two daemons cannot race appends into the same
//! working tree. READERS (the `kutl` CLI reading segments for `signal
//! list`/`document log`) never lock — the append-only segment format is
//! safe to read up to the last complete record concurrently with a writer.

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use kutl_proto::sync::Signal;
use kutl_signals::segment::{SegmentStore, SegmentWriter};
use uuid::Uuid;

/// Name of the store-lifetime single-writer lock file, taken directly under
/// the signals root (`<space_root>/.kutl/signals/LOCK`, see `kutl_signals::segment::signals_root`). Distinct from
/// the per-space segment `LOCK` that `SegmentWriter::open` takes one level
/// deeper — this one guards the whole working tree against a second daemon.
const LOCK_FILE_NAME: &str = "LOCK";

/// `created_at_ms` header value for freshly-created segments. The daemon does
/// not backdate segment headers; ingested records carry their own HLC. Mirrors
/// the relay's `SignalStore` posture.
const SEGMENT_CREATED_AT_MS: i64 = 0;

/// Per-space append-only signal segment store held by a daemon space worker.
///
/// Owns the single-writer `flock` on `<space_root>/.kutl/signals/LOCK` for its
/// whole lifetime (the `_lock` field), lazily opens the space's
/// [`SegmentWriter`] on first [`append`](Self::append), and serves reads via
/// [`load`](Self::load). One store per `(space_root, space_id)`.
pub struct DaemonSignalStore {
    /// Per-space segment directory: `<space_root>/.kutl/signals/<space_id>/`.
    space_dir: PathBuf,
    /// Space this store writes; stamped into segment headers.
    space_id: Uuid,
    /// Lazily opened on first append; evicted (set to `None`) on append error
    /// so the next append re-opens a fresh, self-healed writer.
    writer: Option<SegmentWriter>,
    /// The store-lifetime single-writer lock. Held open (and `flock`ed) for as
    /// long as the store lives; dropping the store releases it. Never read.
    _lock: File,
}

impl DaemonSignalStore {
    /// Open the signal store for `space_id` under `space_root`, taking the
    /// single-writer `flock` on `<space_root>/.kutl/signals/LOCK`.
    ///
    /// The lock is held for the store's lifetime. A second `open` on the same
    /// `space_root` (a second daemon over the same working tree) fails while
    /// the first store lives, guaranteeing a single writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the signals directory cannot be created, the lock
    /// file cannot be opened, or the lock is already held by another writer.
    pub fn open(space_root: &Path, space_id: Uuid) -> Result<Self> {
        let signals_dir = kutl_signals::segment::signals_root(space_root);
        std::fs::create_dir_all(&signals_dir).with_context(|| {
            format!(
                "creating signal segments directory {}",
                signals_dir.display()
            )
        })?;

        let lock_path = signals_dir.join(LOCK_FILE_NAME);
        let lock = File::options()
            .create(true)
            .truncate(false) // sentinel file; never overwritten
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("opening signal writer lock {}", lock_path.display()))?;
        // Rust 1.89+ stabilised `File::try_lock`; `WouldBlock` means another
        // daemon holds the single-writer lock for this working tree. Same
        // mechanism kutl-signals uses for its per-space segment lock — and the
        // same TYPED error, so callers can downcast to distinguish "another
        // writer is live" (degradable: serve local reads) from a real failure.
        lock.try_lock().map_err(|e| match e {
            std::fs::TryLockError::WouldBlock => {
                anyhow::Error::new(kutl_signals::error::Error::Locked {
                    path: signals_dir.clone(),
                })
            }
            std::fs::TryLockError::Error(io) => anyhow::Error::new(io).context(format!(
                "acquiring signal writer lock {}",
                lock_path.display()
            )),
        })?;

        Ok(Self {
            space_dir: signals_dir.join(space_id.to_string()),
            space_id,
            writer: None,
            _lock: lock,
        })
    }

    /// Append one record to the space's active segment, lazily opening the
    /// writer on first use.
    ///
    /// On an append error the cached writer is EVICTED (dropped) before the
    /// error returns. A transient failure (ENOSPC, a create/rotation fault)
    /// POISONS a `SegmentWriter`, after which it rejects every later append —
    /// evicting it lets the next append re-open a fresh writer, which
    /// `scan_valid_prefix`-recovers the segment. Without the eviction one
    /// transient error would permanently wedge all signal ingest for the
    /// space until the daemon restarts. The failed append still returns `Err`;
    /// the eviction just keeps the space from staying wedged. This mirrors
    /// the relay's `SignalStore::append` eviction-on-error posture.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer cannot be opened or the append fails
    /// (after evicting the writer so a retry self-heals).
    pub fn append(&mut self, record: &Signal) -> Result<()> {
        if self.writer.is_none() {
            let w = SegmentWriter::open(&self.space_dir, self.space_id, SEGMENT_CREATED_AT_MS)
                .with_context(|| {
                    format!(
                        "opening signal segment writer at {}",
                        self.space_dir.display()
                    )
                })?;
            self.writer = Some(w);
        }
        let writer = self
            .writer
            .as_mut()
            .expect("writer opened just above this call");
        if let Err(e) = writer.append(record) {
            // Evict the poisoned/failed writer so the NEXT append re-opens a
            // fresh one (self-heal). Dropping it releases its fds + the
            // per-space segment flock.
            self.writer = None;
            return Err(anyhow::Error::new(e).context(format!(
                "appending signal record to {}",
                self.space_dir.display()
            )));
        }
        Ok(())
    }

    /// The per-space segment directory
    /// (`<space_root>/.kutl/signals/<space_id>/`). The catch-up cursor
    /// (`cursor.json`) lives directly here alongside the segments, so the
    /// ingest path advances it with
    /// [`kutl_signals::catchup::save_cursor`](kutl_signals::catchup::save_cursor).
    pub fn dir(&self) -> &Path {
        &self.space_dir
    }

    /// Load all of the space's records (sealed + active). Reader path — no
    /// writer needed; drives the cursor high-water and re-seed computation.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment directory cannot be read.
    pub fn load(&self) -> Result<SegmentStore> {
        SegmentStore::load(&self.space_dir)
            .with_context(|| format!("loading signal segments from {}", self.space_dir.display()))
    }
}

#[cfg(test)]
mod tests {
    use kutl_proto::sync::{Hlc, Signal, SignalEventType};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::DaemonSignalStore;

    /// Build a minimal CREATED signal record for round-trip tests.
    fn created(space: Uuid, sig: &str, rec: &str) -> Signal {
        let mut s = Signal {
            id: sig.into(),
            space_id: space.to_string(),
            record_id: rec.into(),
            hlc: Some(Hlc {
                physical_ms: 1,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// Round-trip: append a record, reload the segments, see the record.
    #[test]
    fn test_append_then_load_round_trips() {
        let root = TempDir::new().unwrap();
        let space = Uuid::from_u128(1);
        let mut store = DaemonSignalStore::open(root.path(), space).unwrap();
        store.append(&created(space, "sig1", "rec1")).unwrap();

        let loaded = store.load().unwrap();
        assert_eq!(loaded.records.len(), 1);
        assert_eq!(loaded.records[0].record_id, "rec1");
    }

    /// A second store on the same `space_root` cannot acquire the flock.
    #[test]
    fn test_second_open_on_same_root_is_locked() {
        let root = TempDir::new().unwrap();
        let space = Uuid::from_u128(1);
        let _first = DaemonSignalStore::open(root.path(), space).unwrap();

        let second = DaemonSignalStore::open(root.path(), space);
        assert!(
            second.is_err(),
            "a second writer on the same space_root must fail to acquire the flock"
        );
    }
}
