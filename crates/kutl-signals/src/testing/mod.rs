//! Faithful, filesystem-level storage-fault injectors for tests
//! (behind the `testing` feature).
//!
//! This is the storage twin of the e2e network-fault injector `FlapProxy`:
//! a FAITHFUL EXTERNAL injector that manipulates real on-disk segment state,
//! NOT an in-process code seam bolted into production `SegmentWriter` /
//! `SegmentStore`. Production `kutl-signals` code carries no test-only hook,
//! env flag, or fault field — every fault here is produced by editing the
//! segment FILES and their directory on disk, exactly as a real crash,
//! bit-rot event, or permission change would leave them.
//!
//! These primitives exist so the relay's recovery paths can be proven under
//! real storage faults: crash-tail divergence (torn active-segment tail),
//! backfill partial-truncation, a poisoned-writer wedge, and ignored
//! quarantine of a corrupt sealed segment.
//!
//! On-disk layout these injectors target (see [`crate::segment`]):
//! - `current.kseg` — the ACTIVE segment: a 30-byte header followed by
//!   length-prefixed records, each framed `u32 LE len | u32 LE crc32(body) |
//!   body`.
//! - `<hlcmin>-<hlcmax>-<count>-<crc>.kseg.zst` — SEALED segments: a
//!   whole-file zstd blob (header + records + a 24-byte footer whose trailing
//!   u32 is a whole-file CRC).
//! - `<name>.corrupt` — quarantined segments a `SegmentStore::load` set aside.
//!
//! Modeling note — process-kill crashes: a "kill between doc-persist and
//! segment-append" crash is modeled by [`truncate_active_segment_tail`] (the
//! segment-side result of that crash — an un-fsynced tail record that never
//! reached durable storage). This module deliberately builds NO process-kill
//! machinery; the on-disk end state is what recovery must handle, and that is
//! what these injectors produce.
//!
//! Scope: these are direct on-disk manipulators. There is intentionally no
//! mock `SignalStore` and no trait abstraction here.

use std::io;
use std::path::{Path, PathBuf};

/// Active-segment filename within a space's signals directory. Mirrors
/// [`crate::segment::ACTIVE_SEGMENT_NAME`]; re-stated here so the injectors
/// depend only on the stable on-disk convention, never on internal items.
const ACTIVE_SEGMENT_NAME: &str = "current.kseg";
/// Filename suffix of a sealed compressed segment. Stable on-disk convention.
const SEALED_SEGMENT_EXT: &str = ".kseg.zst";
/// Directory mode granting the owner read + execute but NOT write — an
/// append to a segment that must be CREATED or ROTATED (a new directory
/// entry) then fails, while an already-open fd keeps writing.
#[cfg(unix)]
const MODE_READ_ONLY: u32 = 0o500;
/// Directory mode granting the owner read + write + execute — the normal
/// writable state a space's signals directory runs in.
#[cfg(unix)]
const MODE_WRITABLE: u32 = 0o700;

/// The active and sealed segment paths found in a space's signals directory.
///
/// The active path is `None` when no `current.kseg` exists (normal right
/// after a rotation, or before the first writer open). Sealed paths are
/// returned sorted lexicographically, which — thanks to the zero-padded
/// integer naming — is also chronological order.
#[derive(Debug, Clone)]
pub struct SegmentPaths {
    /// Path to the active segment (`current.kseg`) if it is present.
    pub active: Option<PathBuf>,
    /// Sealed segment paths (`*.kseg.zst`), sorted lexicographically. Excludes
    /// quarantined (`.corrupt`) and in-flight seal temp (`.tmp`) files.
    pub sealed: Vec<PathBuf>,
}

/// Locate a space's active and sealed segment paths under `space_dir`.
///
/// `space_dir` is the per-space signals directory
/// (`<data_dir>/signals/<space_id>/`). A missing directory yields empty
/// results rather than an error, matching how `SegmentStore::load` treats a
/// not-yet-created space.
pub fn segment_paths(space_dir: &Path) -> io::Result<SegmentPaths> {
    let active_candidate = space_dir.join(ACTIVE_SEGMENT_NAME);
    let active = active_candidate.exists().then_some(active_candidate);

    let mut sealed = Vec::new();
    match std::fs::read_dir(space_dir) {
        Ok(rd) => {
            for entry in rd {
                let path = entry?.path();
                let is_sealed = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(SEALED_SEGMENT_EXT));
                if is_sealed {
                    sealed.push(path);
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    sealed.sort();
    Ok(SegmentPaths { active, sealed })
}

/// Shrink the ACTIVE segment file by `bytes` from the end, simulating a crash
/// that lost the un-fsynced tail (a torn/partial final frame).
///
/// This is the on-disk end state of a "kill between doc-persist and
/// segment-append" crash: the last record's bytes never reached durable
/// storage, so recovery must truncate the torn tail and re-pull the record
/// from the replication set. On the next [`crate::segment::SegmentStore::load`]
/// or `SegmentWriter::open`, `scan_valid_prefix` truncates at the last complete,
/// CRC-valid record, so the truncated record is gone and load returns fewer
/// records.
///
/// Errors if there is no active segment, or if `bytes` would shrink the file
/// below or to zero length (a zero-length active file is a distinct
/// transient state, not a torn tail — refusing keeps the fault faithful).
pub fn truncate_active_segment_tail(space_dir: &Path, bytes: u64) -> io::Result<()> {
    let paths = segment_paths(space_dir)?;
    let active = paths.active.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("no active segment to truncate in {}", space_dir.display()),
        )
    })?;
    let file = std::fs::OpenOptions::new().write(true).open(&active)?;
    let current_len = file.metadata()?.len();
    if bytes >= current_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "truncating {bytes} bytes from an active segment of {current_len} bytes would empty it: {}",
                active.display()
            ),
        ));
    }
    file.set_len(current_len - bytes)?;
    Ok(())
}

/// Flip a byte in the payload region of a SEALED segment so its whole-file
/// CRC check fails on the next [`crate::segment::SegmentStore::load`], which
/// then QUARANTINES it (renames to `<name>.corrupt`).
///
/// When several sealed segments exist, the FIRST in lexicographic (=
/// chronological) order is corrupted. The byte flipped sits at the midpoint
/// of the compressed file — inside the zstd payload, never the trailing CRC
/// field — so the corruption changes decoded content and is caught by the
/// whole-file CRC.
///
/// Errors if there is no sealed segment to corrupt.
pub fn corrupt_sealed_segment(space_dir: &Path) -> io::Result<()> {
    let paths = segment_paths(space_dir)?;
    let target = paths.sealed.into_iter().next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("no sealed segment to corrupt in {}", space_dir.display()),
        )
    })?;
    let mut data = std::fs::read(&target)?;
    if data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("sealed segment is empty: {}", target.display()),
        ));
    }
    // Flip a mid-file byte: inside the zstd payload, well clear of both ends.
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&target, &data)?;
    Ok(())
}

/// Toggle the space's signals directory read-only / writable.
///
/// Read-only (`0o500`) makes any append that must CREATE or ROTATE a segment
/// file — i.e. add a new directory entry — fail with a real [`io::Error`],
/// while an already-open fd keeps writing into the existing active segment.
/// This faults the open/rotation path specifically, which is exactly what a
/// writer-eviction recovery must survive. Restoring writability (`0o700`)
/// makes a fresh `SegmentWriter::open` succeed again, proving the fault is
/// real and reversible.
///
/// Platform note: Unix only. On non-Unix targets this is a no-op returning
/// `Ok(())`, because directory write permission is not modelled the same way;
/// gate storage-fault tests that need this primitive with `#[cfg(unix)]`.
#[cfg(unix)]
pub fn set_space_writable(space_dir: &Path, writable: bool) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = if writable {
        MODE_WRITABLE
    } else {
        MODE_READ_ONLY
    };
    std::fs::set_permissions(space_dir, std::fs::Permissions::from_mode(mode))
}

/// Non-Unix stub for [`set_space_writable`] — a no-op. See the Unix
/// definition's platform note.
#[cfg(not(unix))]
pub fn set_space_writable(_space_dir: &Path, _writable: bool) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{SegmentStore, SegmentWriter};
    use kutl_proto::sync::{Signal, SignalEventType};
    use tempfile::TempDir;
    use uuid::Uuid;

    const SPACE: Uuid = Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788);
    /// Small rotation threshold used to force sealing in self-tests.
    const SMALL_ROTATE_THRESHOLD: u64 = 256;

    /// A padded record so a handful of appends cross a small rotation
    /// threshold and produce a sealed segment.
    fn rec(n: u32) -> Signal {
        let mut s = Signal {
            id: format!("sig-{n}"),
            space_id: SPACE.to_string(),
            record_id: format!("rec-{n}"),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: u64::from(n),
                logical: 0,
                actor: vec![0u8; 16],
            }),
            payload: Some(kutl_proto::sync::signal::Payload::Flag(
                kutl_proto::sync::FlagPayload {
                    message: format!("record {n} {}", "x".repeat(64)),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// `segment_paths` finds the active segment and every sealed segment,
    /// and returns empty results for a directory that does not exist.
    #[test]
    fn test_segment_paths_finds_active_and_sealed() {
        let dir = TempDir::new().unwrap();
        // Missing directory: empty, not an error.
        let missing = dir.path().join("nope");
        let empty = segment_paths(&missing).unwrap();
        assert!(empty.active.is_none());
        assert!(empty.sealed.is_empty());

        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        w.rotate_threshold_for_test(SMALL_ROTATE_THRESHOLD);
        for n in 0..40 {
            w.append(&rec(n)).unwrap();
        }
        drop(w);

        let paths = segment_paths(dir.path()).unwrap();
        assert!(paths.active.is_some(), "active segment must be found");
        assert!(
            !paths.sealed.is_empty(),
            "rotation must have sealed at least one segment"
        );
        // Sealed paths are sorted.
        let mut sorted = paths.sealed.clone();
        sorted.sort();
        assert_eq!(paths.sealed, sorted, "sealed paths must be returned sorted");
    }

    /// PROOF for `truncate_active_segment_tail`: the active file shrinks by
    /// exactly `bytes`, a subsequent `SegmentStore::load` still succeeds
    /// (torn-tail recovery) but returns FEWER records — the truncated tail
    /// record is gone.
    #[test]
    fn test_truncate_active_tail_drops_last_record() {
        // Chop enough bytes to shear the final record's frame. The last
        // record's frame is (8 overhead + body); lopping 16 bytes off the
        // end guarantees the tail frame is incomplete.
        const CHOP: u64 = 16;
        let dir = TempDir::new().unwrap();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            for n in 0..5 {
                w.append(&rec(n)).unwrap();
            }
        }
        let before = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(before.records.len(), 5);
        assert!(before.quarantined.is_empty());

        let active = segment_paths(dir.path()).unwrap().active.unwrap();
        let len_before = std::fs::metadata(&active).unwrap().len();

        truncate_active_segment_tail(dir.path(), CHOP).unwrap();

        let len_after = std::fs::metadata(&active).unwrap().len();
        assert_eq!(
            len_after,
            len_before - CHOP,
            "active file must shrink by exactly the requested byte count"
        );

        // Torn-tail recovery: load still succeeds, but with fewer records.
        let after = SegmentStore::load(dir.path()).unwrap();
        assert!(
            after.quarantined.is_empty(),
            "torn tail is recovered, not quarantined"
        );
        assert!(
            after.records.len() < before.records.len(),
            "truncated tail record must be gone: {} !< {}",
            after.records.len(),
            before.records.len()
        );
        // The surviving records are a prefix — the truncated one is the last.
        assert!(
            !after.records.iter().any(|r| r.record_id == "rec-4"),
            "the last (truncated) record must be absent"
        );
    }

    /// `truncate_active_segment_tail` refuses to empty the file or run with
    /// no active segment, keeping the fault faithful.
    #[test]
    fn test_truncate_active_tail_guards() {
        let dir = TempDir::new().unwrap();
        // No active segment yet.
        match truncate_active_segment_tail(dir.path(), 1) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            other => panic!("expected NotFound with no active segment, got {other:?}"),
        }

        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        w.append(&rec(0)).unwrap();
        drop(w);
        let active = segment_paths(dir.path()).unwrap().active.unwrap();
        let len = std::fs::metadata(&active).unwrap().len();
        // Truncating the whole file (or more) is refused.
        match truncate_active_segment_tail(dir.path(), len) {
            Err(e) if e.kind() == io::ErrorKind::InvalidInput => {}
            other => panic!("expected InvalidInput when emptying the file, got {other:?}"),
        }
    }

    /// PROOF for `corrupt_sealed_segment`: after corruption, `load`
    /// QUARANTINES the sealed segment (a `.corrupt` file appears and the
    /// corrupt segment's records are absent from `loaded.records`).
    #[test]
    fn test_corrupt_sealed_segment_is_detected() {
        let dir = TempDir::new().unwrap();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.rotate_threshold_for_test(SMALL_ROTATE_THRESHOLD);
            for n in 0..40 {
                w.append(&rec(n)).unwrap();
            }
        }
        let before = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(before.records.len(), 40);
        assert!(before.quarantined.is_empty());

        let first_sealed = segment_paths(dir.path())
            .unwrap()
            .sealed
            .into_iter()
            .next()
            .expect("a sealed segment must exist");

        corrupt_sealed_segment(dir.path()).unwrap();

        let after = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(
            after.quarantined.len(),
            1,
            "the corrupt sealed segment must be quarantined"
        );
        assert!(
            after.quarantined[0]
                .path
                .to_string_lossy()
                .ends_with(kutl_core::envelope::CORRUPT_EXT),
            "quarantine path must end with .corrupt"
        );
        assert!(
            after.quarantined[0].path.exists(),
            "the .corrupt file must exist on disk"
        );
        assert!(
            !first_sealed.exists(),
            "the original sealed name must be gone (renamed to .corrupt)"
        );
        assert!(
            after.records.len() < before.records.len(),
            "the corrupt segment's records must be absent: {} !< {}",
            after.records.len(),
            before.records.len()
        );
    }

    /// `corrupt_sealed_segment` errors when there is nothing to corrupt.
    #[test]
    fn test_corrupt_sealed_segment_requires_a_sealed_segment() {
        let dir = TempDir::new().unwrap();
        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        w.append(&rec(0)).unwrap();
        drop(w);
        // Only an active segment exists — no sealed segment to corrupt.
        match corrupt_sealed_segment(dir.path()) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            other => panic!("expected NotFound with no sealed segment, got {other:?}"),
        }
    }

    /// PROOF for `set_space_writable`: with the dir read-only, an append that
    /// must create/rotate a segment file returns `Err` (a real io error);
    /// after restoring writability a fresh `SegmentWriter::open` + append
    /// succeeds — the fault is real and reversible.
    #[cfg(unix)]
    #[test]
    fn test_set_space_writable_faults_segment_creation() {
        let dir = TempDir::new().unwrap();
        let space_dir = dir.path().join("space");
        // Establish the space dir (writer creates it) then drop the writer so
        // no open fd masks the permission fault on the create/rotate path.
        {
            let mut w = SegmentWriter::open(&space_dir, SPACE, 0).unwrap();
            w.append(&rec(0)).unwrap();
        }

        set_space_writable(&space_dir, false).unwrap();

        // A rotation must create a NEW sealed file + a fresh active file —
        // both are new directory entries, which a read-only dir forbids.
        // Force it: reopen (allowed to open the existing LOCK? no — LOCK is a
        // new entry only if absent; it exists, so open reads it) then append
        // past a tiny threshold to trigger seal_and_rotate's tmp-file create.
        let open_result = SegmentWriter::open(&space_dir, SPACE, 0);
        let faulted = match open_result {
            // Some platforms fault at open (LOCK re-create / sweep write);
            // others open fine and fault when rotation creates the seal temp.
            Err(_) => true,
            Ok(mut w) => {
                w.rotate_threshold_for_test(SMALL_ROTATE_THRESHOLD);
                // Append until a rotation is forced or a write faults.
                let mut hit_err = false;
                for n in 1..200 {
                    if w.append(&rec(n)).is_err() {
                        hit_err = true;
                        break;
                    }
                }
                hit_err
            }
        };
        assert!(
            faulted,
            "a read-only space dir must fault the segment create/rotation path"
        );

        // Reversible: restore writability, a fresh open + append succeeds.
        set_space_writable(&space_dir, true).unwrap();
        let mut w = SegmentWriter::open(&space_dir, SPACE, 0).unwrap();
        w.append(&rec(100)).unwrap();
        drop(w);
        let loaded = SegmentStore::load(&space_dir).unwrap();
        assert!(
            loaded.records.iter().any(|r| r.record_id == "rec-100"),
            "after restoring writability the append must persist"
        );
    }
}
