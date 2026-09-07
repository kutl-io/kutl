//! Per-space append-only signal segment writers, cached on the relay actor.
//! Segments live at `<data_dir>/signals/<space_id>/`.
//! Single-threaded by the actor loop; no locking beyond the process-wide
//! segment `LOCK` that `SegmentWriter::open` takes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use kutl_proto::sync::Signal;
use kutl_signals::segment::{SegmentStore, SegmentWriter};
use uuid::Uuid;

/// `created_at_ms` header value for freshly-created segments. The relay
/// does not backdate segment headers; records carry their own HLC.
const SEGMENT_CREATED_AT_MS: i64 = 0;

/// Error type for signal store operations.
#[derive(Debug, thiserror::Error)]
pub enum SignalStoreError {
    /// A segment operation failed.
    #[error("{0}")]
    Segment(#[from] kutl_signals::Error),
}

/// Convenience alias for signal store results.
pub type Result<T> = std::result::Result<T, SignalStoreError>;

/// Cache of per-space segment writers.
///
/// Held on the `Relay` actor; single-threaded by the `RelayCommand` loop.
pub struct SignalStore {
    root: PathBuf,
    writers: HashMap<Uuid, SegmentWriter>,
}

/// Subdirectory of the relay's data dir that holds per-space signal segments.
/// The single source of truth for segment rooting: the
/// actor's store, the startup backfill, and the projection reconcile MUST all
/// root here or they desync (the reconcile's correctness depends on reading the
/// same bytes the actor wrote).
const SIGNALS_SUBDIR: &str = "signals";

impl SignalStore {
    /// The signals segment root under `data_dir` (`<data_dir>/signals`).
    ///
    /// The ONE place `<data_dir>/signals` is spelled, so a future rename cannot
    /// desync the actor's store from the startup backfill/reconcile, whose
    /// correctness depends on byte-identical rooting.
    pub fn root_for(data_dir: &Path) -> PathBuf {
        data_dir.join(SIGNALS_SUBDIR)
    }

    /// Build a store rooted at `<data_dir>/signals`.
    pub fn new(signals_root: PathBuf) -> Self {
        Self {
            root: signals_root,
            writers: HashMap::new(),
        }
    }

    fn space_dir(&self, space: Uuid) -> PathBuf {
        self.root.join(space.to_string())
    }

    /// Append one record to a space's active segment, lazily opening the
    /// writer on first use. The caller is responsible for well-formedness
    /// validation before calling this.
    ///
    /// On an append error the cached writer is EVICTED (dropped) before the
    /// error is returned. A transient failure (ENOSPC, a create/rotation
    /// fault) POISONS a `SegmentWriter`, after which it rejects every later
    /// append — evicting it lets the next append re-open a fresh writer,
    /// which `scan_valid_prefix`-recovers the segment. Without the eviction one
    /// transient error would permanently wedge all signal mutations for the
    /// space until the relay restarts. The failed append still returns `Err`
    /// (the caller counts it rejected); the eviction only keeps the space
    /// from wedging permanently.
    pub fn append(&mut self, space: Uuid, record: &Signal) -> Result<()> {
        let dir = self.space_dir(space);
        let writer = match self.writers.entry(space) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(v) => {
                let w = SegmentWriter::open(&dir, space, SEGMENT_CREATED_AT_MS)?;
                v.insert(w)
            }
        };
        if let Err(e) = writer.append(record) {
            // Evict the poisoned/failed writer so the NEXT append re-opens a
            // fresh one (self-heal). Dropping it releases its fds + the flock.
            self.writers.remove(&space);
            return Err(e.into());
        }
        Ok(())
    }

    /// Evict (drop) a space's cached writer, releasing its two open fds (the
    /// active segment + the flock'd `LOCK`) and the flock. A no-op when no
    /// writer is cached for the space.
    ///
    /// Called on space unregister so a long-lived relay does not hold a
    /// writer FOREVER for every space it ever appended to (an fd leak, and a
    /// latent stale-fd hazard if the space's data dir is later removed). The
    /// segments persist on disk; the next append to the space re-creates the
    /// writer and reads back all prior records. Dropping the `SegmentWriter`
    /// flushes/closes cleanly (its `Drop` handles that).
    pub fn evict(&mut self, space: Uuid) {
        self.writers.remove(&space);
    }

    /// Whether a writer is currently cached for `space`. Test/introspection
    /// helper — the eviction paths assert on this.
    pub fn has_writer(&self, space: Uuid) -> bool {
        self.writers.contains_key(&space)
    }

    /// Load all of a space's records (sealed + active). Reader path —
    /// no writer needed; used by projection rebuild and catch-up. A
    /// quarantined segment is logged at error level and counted by the
    /// loader itself, and the returned store lists it, so the healthy
    /// remainder is served without the loss going unreported.
    pub fn load(&self, space: Uuid) -> Result<SegmentStore> {
        Ok(SegmentStore::load(&self.space_dir(space))?)
    }

    /// List the space ids that have an on-disk segment directory.
    ///
    /// Reads the immediate children of the signals root and returns every
    /// subdirectory whose name parses as a UUID (the per-space segment dirs
    /// [`Self::space_dir`] creates). Drives the startup projection reconcile,
    /// which must cover spaces that have segments regardless of whether their
    /// legacy `signals` rows still exist. A missing root (no space ever
    /// appended) yields an empty list, not an error.
    ///
    /// # Errors
    ///
    /// Returns an IO error if the root exists but cannot be read.
    pub fn space_ids_on_disk(&self) -> std::io::Result<Vec<Uuid>> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            // No signals root yet (nothing ever appended) → no spaces.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut spaces = Vec::new();
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
                && let Ok(space) = Uuid::parse_str(name)
            {
                spaces.push(space);
            }
        }
        Ok(spaces)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_proto::sync::{Signal, SignalEventType};
    use tempfile::TempDir;
    use uuid::Uuid;

    fn created(space: Uuid, sig: &str, rec: &str) -> Signal {
        let mut s = Signal {
            id: sig.into(),
            space_id: space.to_string(),
            record_id: rec.into(),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: 1,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// Append routes to the right per-space dir and reload sees the record.
    #[test]
    fn test_append_creates_space_dir_and_persists() {
        let dir = TempDir::new().unwrap();
        let space = Uuid::from_u128(1);
        let mut store = SignalStore::new(dir.path().to_path_buf());
        store
            .append(space, &created(space, "sig1", "rec1"))
            .unwrap();
        let loaded = store.load(space).unwrap();
        assert_eq!(loaded.records.len(), 1);
        assert_eq!(loaded.records[0].record_id, "rec1");
        // Second space is isolated.
        let other = Uuid::from_u128(2);
        assert_eq!(store.load(other).unwrap().records.len(), 0);
    }

    /// The writer is cached: two appends to one space reuse one writer.
    #[test]
    fn test_append_reuses_writer() {
        let dir = TempDir::new().unwrap();
        let space = Uuid::from_u128(1);
        let mut store = SignalStore::new(dir.path().to_path_buf());
        store.append(space, &created(space, "s", "r1")).unwrap();
        store.append(space, &created(space, "s", "r2")).unwrap();
        assert_eq!(store.load(space).unwrap().records.len(), 2);
    }

    /// Poison-wedge self-heal: a transient append failure that
    /// poisons the cached writer must not permanently wedge the space. The
    /// fault harness makes the space dir read-only so an append that must
    /// rotate faults; the store evicts the poisoned writer, and once the dir
    /// is writable again the NEXT append succeeds (proving a fresh writer was
    /// re-opened) with both surviving records readable.
    ///
    /// Without the eviction the second append would still fail (wedged)
    /// because the poisoned writer stays cached.
    #[cfg(unix)]
    #[test]
    fn test_append_evicts_poisoned_writer_and_self_heals() {
        use kutl_signals::segment::SegmentWriter;
        use kutl_signals::testing::set_space_writable;

        let dir = TempDir::new().unwrap();
        let space = Uuid::from_u128(1);
        let space_dir = dir.path().join(space.to_string());
        let mut store = SignalStore::new(dir.path().to_path_buf());

        // First append: writer created + cached, one record on disk.
        store.append(space, &created(space, "s", "r1")).unwrap();
        assert!(store.has_writer(space), "writer cached after first append");

        // Shrink the cached writer's rotation threshold so the NEXT append
        // must rotate — creating new directory entries (seal temp + fresh
        // active), which a read-only dir forbids.
        store
            .writers
            .get_mut(&space)
            .expect("writer cached")
            .rotate_threshold_for_test(1);

        set_space_writable(&space_dir, false).unwrap();
        let faulted = store.append(space, &created(space, "s", "r2"));
        assert!(
            faulted.is_err(),
            "an append that must rotate under a read-only dir must fault"
        );
        // The poisoned writer was evicted. Without eviction it would
        // remain cached and every later append would stay wedged.
        assert!(
            !store.has_writer(space),
            "poisoned writer must be evicted on append error"
        );

        // Restore writability and retry: the next append re-opens a fresh
        // writer (scan_valid_prefix recovers the segment) and succeeds.
        set_space_writable(&space_dir, true).unwrap();
        store
            .append(space, &created(space, "s", "r2b"))
            .expect("after eviction + restored writability the append self-heals");

        // Both surviving records are readable (r1 from before the fault, r2b
        // from the healed writer). Ensure no live writer holds the lock during
        // the read by evicting it first.
        store.evict(space);
        let loaded = store.load(space).unwrap();
        assert!(loaded.quarantined.is_empty());
        let mut ids: Vec<_> = loaded.records.iter().map(|r| r.record_id.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["r1", "r2b"]);

        // Cleanup: SegmentWriter::open would otherwise leave the dir writable
        // for the TempDir drop; make sure the temp dir can be removed.
        let _ = SegmentWriter::open(&space_dir, space, 0);
    }

    /// Evict on unregister: after `evict`, the writer entry is gone,
    /// and a subsequent append re-creates it and still reads back all prior
    /// records (segments persist; only the in-memory writer was dropped).
    #[test]
    fn test_evict_drops_writer_no_data_loss() {
        let dir = TempDir::new().unwrap();
        let space = Uuid::from_u128(7);
        let mut store = SignalStore::new(dir.path().to_path_buf());
        store.append(space, &created(space, "s", "r1")).unwrap();
        assert!(store.has_writer(space), "writer cached after append");

        store.evict(space);
        assert!(!store.has_writer(space), "evict drops the cached writer");
        // Evicting an absent writer is a no-op.
        store.evict(space);

        // Re-append: the writer is re-created and prior records survive.
        store.append(space, &created(space, "s", "r2")).unwrap();
        assert!(store.has_writer(space), "append re-creates the writer");
        store.evict(space);
        let loaded = store.load(space).unwrap();
        let mut ids: Vec<_> = loaded.records.iter().map(|r| r.record_id.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["r1", "r2"], "no data loss across evict");
    }

    /// Surface quarantined segments: a corrupted sealed segment is
    /// quarantined by the loader, which serves the healthy remainder and
    /// lists the quarantine on the returned store (the observable surface;
    /// the loader's own error log is not captured here).
    #[test]
    fn test_load_serves_remainder_and_surfaces_quarantine() {
        use kutl_signals::segment::SegmentWriter;
        use kutl_signals::testing::corrupt_sealed_segment;

        let dir = TempDir::new().unwrap();
        let space = Uuid::from_u128(9);
        let space_dir = dir.path().join(space.to_string());

        // Write enough records with a tiny rotation threshold to seal at least
        // one segment, then drop the writer so no live lock blocks quarantine.
        {
            let mut w = SegmentWriter::open(&space_dir, space, SEGMENT_CREATED_AT_MS).unwrap();
            w.rotate_threshold_for_test(256);
            for n in 0..40 {
                w.append(&created(space, "s", &format!("rec-{n}"))).unwrap();
            }
        }

        let store = SignalStore::new(dir.path().to_path_buf());
        let before = store.load(space).unwrap();
        assert_eq!(before.records.len(), 40);
        assert!(before.quarantined.is_empty());

        corrupt_sealed_segment(&space_dir).unwrap();

        // (a) load succeeds and serves the healthy (non-corrupt) records.
        // (b) the quarantined list is non-empty (a .corrupt file appears).
        let loaded = store.load(space).unwrap();
        assert!(
            !loaded.records.is_empty() && loaded.records.len() < 40,
            "healthy remainder served, corrupt segment's records absent: {}",
            loaded.records.len()
        );
        assert_eq!(
            loaded.quarantined.len(),
            1,
            "the corrupt sealed segment must be quarantined + surfaced"
        );
        assert!(
            loaded.quarantined[0]
                .path
                .to_string_lossy()
                .ends_with(".corrupt"),
            "quarantine path names the .corrupt file"
        );
    }
}
