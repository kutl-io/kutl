//! Append-only per-space segment storage.
//!
//! Layout inside a space's signals dir:
//! - `current.kseg` — active segment: header, then length-prefixed records.
//! - `<hlcmin>-<hlcmax>-<count>-<crc>.kseg.zst` — sealed segments
//!   (whole-file zstd; header + records + footer with CRC).
//! - `LOCK` — flock'd single-writer guard. Readers never lock (append-only
//!   files are safe to read to the last complete record).
//! - `<name>.corrupt` — quarantined segments that failed integrity checks.
//!   Sealed quarantine names are content-derived (collision-proof); a
//!   re-corrupted ACTIVE segment overwrites its previous `.corrupt` —
//!   accepted best-effort (the records are re-fetchable via catch-up).
//!   This crate calls no logger itself; every quarantine goes through
//!   `kutl_core::envelope::quarantine`, which logs at error level with the
//!   kind, the reason and the recovery action, and counts the file. A
//!   corrupt active segment a live writer holds is left in place and
//!   reported the same way. A `.corrupt` file still in the directory is
//!   reported as quarantined by every later load too, so the fold stays
//!   known-incomplete until the file is moved out.
//!
//! This is a log segment, not an `SSTable`: no sort order, no compaction —
//! segments never serve online queries (projections do).

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use kutl_core::envelope::{self, FRAME_OVERHEAD, HEADER_LEN, Kind};
use kutl_proto::sync::Signal;
use prost::Message;
use uuid::Uuid;

use crate::error::{Error, Result};

/// Active-segment filename within a space's signals directory.
pub(crate) const ACTIVE_SEGMENT_NAME: &str = "current.kseg";
/// Lockfile guarding the single-writer rule.
pub(crate) const LOCK_FILE_NAME: &str = "LOCK";
/// Extension for sealed compressed segments.
const SEALED_SEGMENT_EXT: &str = ".kseg.zst";
/// Extension for in-flight seal temp files (swept on writer open).
const TMP_EXT: &str = ".tmp";
/// Reason reported for a `.corrupt` file an earlier load set aside that
/// still sits in the directory.
const QUARANTINE_RESIDUE_REASON: &str = "quarantined on an earlier load; still on disk";
/// Seal-and-rotate threshold for the active segment. 4 MiB keeps sealed
/// files cheap to ship whole during catch-up while bounding rewrite cost.
const SEGMENT_ROTATE_BYTES: u64 = 4 * 1024 * 1024;
/// Sealed-segment footer size in bytes:
/// `record_count` (4) + `hlc_min` (8) + `hlc_max` (8) + whole-file crc32 (4).
const FOOTER_LEN: usize = 24;
/// Maximum record payload in bytes (16 MiB). Enforced before encoding so we
/// never allocate a large buffer for a record that will be rejected; the cap
/// is generous for any realistic signal. Note the cap is 4x the rotation
/// threshold: rotation is checked before each append, so one max-size record
/// can overshoot the threshold and a sealed segment can reach ~20 MiB
/// uncompressed — `MAX_SEALED_UNCOMPRESSED_BYTES` is derived from exactly
/// that bound.
pub const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
/// Upper bound on a sealed segment's uncompressed size, enforced while
/// decoding — never trust the zstd frame's declared content size (a corrupt
/// or hostile header could otherwise balloon memory). Derivation: rotation
/// triggers once the active segment reaches `SEGMENT_ROTATE_BYTES` (4 MiB)
/// and runs before the append, so a sealed payload can overshoot by at most
/// one max-size record: 4 MiB + `MAX_RECORD_BYTES` (16 MiB) +
/// `FRAME_OVERHEAD` + `FOOTER_LEN` ≈ 20 MiB; 32 MiB adds slack.
const MAX_SEALED_UNCOMPRESSED_BYTES: u64 = 32 * 1024 * 1024;
/// zstd compression level 0 = the zstd crate's default (currently level 3).
/// The sentinel value 0 delegates the choice to the library.
const ZSTD_DEFAULT_LEVEL: i32 = 0;

/// State directory a space keeps under its root.
const KUTL_STATE_DIR: &str = ".kutl";
/// Subdirectory of the state dir holding per-space signal segments.
const SIGNALS_SUBDIR: &str = "signals";

/// The signals root under a space root: `<space_root>/.kutl/signals`. Holds
/// the per-space segment directories and the daemon's single-writer LOCK.
#[must_use]
pub fn signals_root(space_root: &std::path::Path) -> std::path::PathBuf {
    space_root.join(KUTL_STATE_DIR).join(SIGNALS_SUBDIR)
}

/// The per-space signal-segments directory: `<space_root>/.kutl/signals/<space_id>`.
///
/// The ONE definition of this layout. The daemon's store, the CLI's readers,
/// and status collection all resolve segments through it — load-bearing
/// because drift here is SILENT, not loud: [`SegmentStore::load`] on a
/// missing directory returns an empty store, so a consumer spelling the path
/// itself would report "no signals" forever if the layout ever moved.
#[must_use]
pub fn signals_dir(space_root: &std::path::Path, space_id: &str) -> std::path::PathBuf {
    signals_root(space_root).join(space_id)
}

/// Single-writer appender for a space's active segment.
pub struct SegmentWriter {
    dir: PathBuf,
    /// Space UUID written into the segment header; consumed by
    /// `seal_and_rotate` to name the sealed-segment file.
    space_id: Uuid,
    file: File,
    len: u64,
    /// Rotation threshold; `SEGMENT_ROTATE_BYTES` outside tests.
    rotate_threshold: u64,
    /// Millisecond timestamp supplied at open time; stamped into newly
    /// created headers and reused when `seal_and_rotate` opens the next
    /// active segment.
    created_at_ms: i64,
    /// Set to `true` on the first failed `write_all` inside `append`.
    /// A partial write advances the file offset past a torn frame while
    /// leaving `self.len` stale; a subsequent append on the same writer
    /// would write after the torn bytes (a reopen self-heals via
    /// `scan_valid_prefix`, but same-writer reuse strands records). Poisoning
    /// fails all further appends fast without silently corrupting the file.
    poisoned: bool,
    _lock: File, // held for the writer's lifetime
}

impl SegmentWriter {
    /// Open (or create) the active segment, taking the writer lock and
    /// truncating any torn tail from a crashed writer.
    ///
    /// `created_at_ms` is stamped into the header of a newly created
    /// segment. It is informational only — this crate takes no ambient
    /// clock by design; callers supply their own (tests pass 0).
    pub fn open(dir: &Path, space_id: Uuid, created_at_ms: i64) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::SegmentWrite {
            path: dir.into(),
            source: e,
        })?;
        let lock_path = dir.join(LOCK_FILE_NAME);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false) // sentinel file; never overwritten
            .write(true)
            .open(&lock_path)
            .map_err(|e| Error::SegmentWrite {
                path: lock_path.clone(),
                source: e,
            })?;
        // Rust 1.89+ stabilised `File::try_lock`; the `WouldBlock` variant
        // signals another writer holds the lock.
        if let Err(e) = lock.try_lock() {
            return match e {
                std::fs::TryLockError::WouldBlock => Err(Error::Locked { path: dir.into() }),
                std::fs::TryLockError::Error(io) => Err(Error::SegmentWrite {
                    path: lock_path.clone(),
                    source: io,
                }),
            };
        }

        // Sweep crash-orphaned seal temp files. Safe under the freshly
        // acquired writer lock: only a writer creates them, and a leftover
        // one is a seal that never renamed into place (its records are still
        // in the active segment). Only files this writer created
        // (`<sealed>.kseg.zst.tmp`) are swept. Best-effort: a sweep failure
        // only means the orphan lingers until the next open.
        if let Ok(rd) = std::fs::read_dir(dir) {
            for path in rd.flatten().map(|e| e.path()) {
                let is_seal_tmp = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(TMP_EXT) && n.contains(SEALED_SEGMENT_EXT));
                if is_seal_tmp {
                    // intentional: best-effort cleanup, see sweep comment
                    let _ = std::fs::remove_file(&path);
                }
            }
        }

        let (file, len) = Self::open_active_only(dir, space_id, created_at_ms)?;
        Ok(Self {
            dir: dir.into(),
            space_id,
            file,
            len,
            rotate_threshold: SEGMENT_ROTATE_BYTES,
            created_at_ms,
            poisoned: false,
            _lock: lock,
        })
    }

    /// Open/create + recover the active segment file. Shared by `open`
    /// and the post-rotation fresh start.
    ///
    /// An empty file gets its header from the shared record-log open. An
    /// existing file is scanned and its torn tail truncated; one whose
    /// header is corrupt is quarantined and a fresh segment is initialised
    /// in its place, so callers never wedge on bad header bytes.
    fn open_active_only(dir: &Path, space_id: Uuid, created_at_ms: i64) -> Result<(File, u64)> {
        let path = dir.join(ACTIVE_SEGMENT_NAME);
        let (mut file, mut len) = envelope::open_log(Kind::Segment, &path, space_id, created_at_ms)
            .map_err(|e| Error::SegmentWrite {
                path: path.clone(),
                source: e,
            })?;
        // Crash recovery: walk records; truncate at the first torn one.
        match scan_valid_prefix(&mut file, &path) {
            Err(Error::SegmentCorrupt { reason, .. }) => {
                drop(file);
                envelope::quarantine(Kind::Segment, &path, &reason);
                return init_fresh_active(&path, space_id, created_at_ms);
            }
            Err(e) => return Err(e),
            Ok((header_space, valid_bytes)) => {
                if header_space != space_id {
                    return Err(Error::SegmentCorrupt {
                        path,
                        reason: "segment belongs to a different space".into(),
                    });
                }
                let valid = u64::try_from(valid_bytes.len()).expect("file position fits in u64");
                if valid < len {
                    file.set_len(valid).map_err(|e| Error::SegmentWrite {
                        path: path.clone(),
                        source: e,
                    })?;
                    len = valid;
                }
                file.seek(SeekFrom::End(0))
                    .map_err(|e| Error::SegmentRead {
                        path: path.clone(),
                        source: e,
                    })?;
            }
        }
        Ok((file, len))
    }

    /// Append one record; rotates (seal + fresh active segment) past the
    /// size threshold. Rotation happens BEFORE the append so a sealed
    /// segment never exceeds the threshold by more than one record.
    ///
    /// Durability contract: on return the record is durable against
    /// process crash (the bytes sit in the OS page cache) but NOT against
    /// power loss or kernel panic until OS writeback completes. That is
    /// acceptable here because a record's authoritative home is the
    /// replication set (relay ⇄ clients): a tail truncated by crash
    /// recovery is re-pulled via catch-up. The residual accepted loss is
    /// a locally-originated record created within the writeback window
    /// and never transmitted. Explicit sync points land at seal/rotation.
    pub fn append(&mut self, record: &Signal) -> Result<()> {
        // A prior failed write_all left the file offset past a torn frame
        // while `self.len` is stale. Fail fast rather than silently writing
        // after torn bytes — a reopen self-heals via scan_valid_prefix.
        if self.poisoned {
            return Err(Error::SegmentWrite {
                path: self.dir.join(ACTIVE_SEGMENT_NAME),
                source: std::io::Error::other(
                    "writer poisoned by an earlier failed append; reopen the segment",
                ),
            });
        }

        // Reject oversized records before encoding cost is sunk.
        let encoded_len = record.encoded_len();
        if encoded_len > MAX_RECORD_BYTES {
            return Err(Error::RecordTooLarge {
                bytes: encoded_len,
                max: MAX_RECORD_BYTES,
            });
        }

        if self.len >= self.rotate_threshold {
            self.seal_and_rotate()?;
        }
        // Encoding completes before the first `write_all` — recovery's
        // tear heuristic in `scan_valid_prefix` relies on this invariant.
        let bytes = record.encode_to_vec();
        // Recovery treats a zero length prefix as end-of-valid-prefix
        // (see `scan_valid_prefix`); the writer must never emit one.
        debug_assert!(
            !bytes.is_empty(),
            "a signal record never encodes to zero bytes"
        );
        // encoded_len ≤ MAX_RECORD_BYTES ≪ u32::MAX, so the frame's length
        // prefix cannot overflow.
        self.file
            .write_all(&envelope::encode_frame(&bytes))
            .map_err(|e| {
                self.poisoned = true;
                Error::SegmentWrite {
                    path: self.dir.join(ACTIVE_SEGMENT_NAME),
                    source: e,
                }
            })?;
        self.len += u64::try_from(FRAME_OVERHEAD + bytes.len()).expect("frame length fits in u64");
        Ok(())
    }

    /// Seal the active segment and start a fresh one.
    ///
    /// The sealed file format is zstd-compressed:
    /// - segment header (30 bytes)
    /// - CRC-framed records
    /// - footer: `record_count` (u32 LE) | `hlc_min` (u64 LE) | `hlc_max` (u64 LE) |
    ///   whole-file crc32 (u32 LE, covers header+records+footer-minus-crc).
    ///
    /// The sealed filename is
    /// `{hlc_min:020}-{hlc_max:020}-{count:010}-{crc32:08x}.kseg.zst` with
    /// zero-padded integers so lexicographic order equals chronological order.
    ///
    /// Crash windows and the recovery invariant:
    /// 1. A crash between the sealed rename and the active-file removal
    ///    leaves BOTH files on disk, so `load` returns those records twice.
    ///    Benign: the fold dedups by `record_id`.
    /// 2. On writer reopen the stale past-threshold active is re-sealed by
    ///    the next append. Because the sealed name is derived from content
    ///    (hlc range, count, whole-file CRC) the re-seal produces the
    ///    identical filename and idempotently overwrites the first copy —
    ///    the duplicate window self-heals. The CRC name component preserves
    ///    this by construction: same content ⇒ same CRC ⇒ same name.
    fn seal_and_rotate(&mut self) -> Result<()> {
        let active_path = self.dir.join(ACTIVE_SEGMENT_NAME);

        // --- Read the full active segment ---
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| Error::SegmentRead {
                path: active_path.clone(),
                source: e,
            })?;
        let mut raw = Vec::new();
        self.file
            .read_to_end(&mut raw)
            .map_err(|e| Error::SegmentRead {
                path: active_path.clone(),
                source: e,
            })?;

        // --- Decode records to compute footer metadata ---
        // If decode fails here the seal aborts and the error propagates out
        // of `append`; every subsequent append keeps failing loudly until a
        // reopen truncates the active segment at the bad frame. Intended
        // loud degradation: the truncated tail is re-fetchable from the
        // replication set (see `append`'s durability contract).
        let mut records = Vec::new();
        decode_records(&raw[HEADER_LEN..], &active_path, &mut records)?;
        let count = u32::try_from(records.len()).expect("record count fits in u32");
        let (hlc_min, hlc_max) = hlc_range(&records);

        // --- Build footer (minus CRC) ---
        // FOOTER_LEN = 24: count(4) + hlc_min(8) + hlc_max(8) + crc(4)
        let mut footer_body = Vec::with_capacity(FOOTER_LEN - size_of::<u32>());
        footer_body.extend_from_slice(&count.to_le_bytes());
        footer_body.extend_from_slice(&hlc_min.to_le_bytes());
        footer_body.extend_from_slice(&hlc_max.to_le_bytes());

        // CRC covers header + records + footer body (everything except the trailing crc field).
        let mut crc_hasher = crc32fast::Hasher::new();
        crc_hasher.update(&raw);
        crc_hasher.update(&footer_body);
        let whole_crc = crc_hasher.finalize();

        let mut footer = footer_body;
        footer.extend_from_slice(&whole_crc.to_le_bytes());
        debug_assert_eq!(footer.len(), FOOTER_LEN);

        // --- Compress header + records + footer ---
        // `raw` is already owned; extend it in place with the footer instead
        // of allocating a second ~4 MiB buffer. The zstd input is the
        // concatenation of header + records + footer.
        raw.extend_from_slice(&footer);
        let compressed = zstd::encode_all(raw.as_slice(), ZSTD_DEFAULT_LEVEL).map_err(|e| {
            Error::SegmentWrite {
                path: active_path.clone(),
                source: e,
            }
        })?;

        // --- Write to .tmp, sync, rename, dir-fsync ---
        // Content-derived name. Widths: 20 = digit count of u64::MAX and
        // 10 = digit count of u32::MAX, so zero-padding makes lexicographic
        // order equal numeric order. The whole-file CRC component
        // disambiguates segments with an identical hlc range and count
        // (e.g. HLC-less batches all named `(0, 0, n)`) — without it this
        // rename would silently overwrite an earlier sealed file. Deriving
        // the name from content rather than a sequence number preserves the
        // crash-replay self-heal described in the method doc: re-sealing
        // the same bytes produces the same name, so the overwrite is
        // idempotent.
        let sealed_name =
            format!("{hlc_min:020}-{hlc_max:020}-{count:010}-{whole_crc:08x}{SEALED_SEGMENT_EXT}");
        let tmp_path = self.dir.join(format!("{sealed_name}{TMP_EXT}"));
        let sealed_path = self.dir.join(&sealed_name);
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)
                .map_err(|e| Error::SegmentWrite {
                    path: tmp_path.clone(),
                    source: e,
                })?;
            tmp.write_all(&compressed)
                .map_err(|e| Error::SegmentWrite {
                    path: tmp_path.clone(),
                    source: e,
                })?;
            tmp.sync_data().map_err(|e| Error::SegmentWrite {
                path: tmp_path.clone(),
                source: e,
            })?;
        }
        std::fs::rename(&tmp_path, &sealed_path).map_err(|e| Error::SegmentWrite {
            path: sealed_path.clone(),
            source: e,
        })?;
        // Fsync the directory so the rename-into-place is durable. Without
        // a dir fsync a power loss between the rename and the next journal
        // flush can leave the directory entry missing (classic zero-length-
        // file-after-power-loss bug).
        {
            let dir_file = File::open(&self.dir).map_err(|e| Error::SegmentWrite {
                path: self.dir.clone(),
                source: e,
            })?;
            dir_file.sync_data().map_err(|e| Error::SegmentWrite {
                path: self.dir.clone(),
                source: e,
            })?;
        }

        // --- Remove old active file, start fresh ---
        std::fs::remove_file(&active_path).map_err(|e| Error::SegmentWrite {
            path: active_path.clone(),
            source: e,
        })?;

        let (file, len) = Self::open_active_only(&self.dir, self.space_id, self.created_at_ms)?;
        self.file = file;
        self.len = len;
        Ok(())
    }

    /// Override the rotation threshold. Test seam — compiled only for this
    /// crate's own tests and the `testing` feature (which kutl-relay enables
    /// as a dev-dependency to force rotation from its tests); never present
    /// in production builds.
    #[cfg(any(test, feature = "testing"))]
    pub fn rotate_threshold_for_test(&mut self, bytes: u64) {
        self.rotate_threshold = bytes;
    }

    /// Set the poisoned flag. Test seam only — simulates a failed `write_all`
    /// without requiring a real I/O error.
    #[cfg(test)]
    fn poison_for_test(&mut self) {
        self.poisoned = true;
    }
}

/// Create a fresh, fully-initialised active segment at `path`: empty the
/// file, then let the shared record-log open stamp and sync the header, so
/// the segment is valid if the process crashes before any record is
/// appended. Its one caller is the quarantine path, which has already moved
/// the corrupt bytes aside (best effort: when that rename fails the bytes
/// are lost, because wedging on the bad file would be worse).
fn init_fresh_active(path: &Path, space_id: Uuid, created_at_ms: i64) -> Result<(File, u64)> {
    let write_err = |e| Error::SegmentWrite {
        path: path.into(),
        source: e,
    };
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(write_err)?;
    envelope::open_log(Kind::Segment, path, space_id, created_at_ms).map_err(write_err)
}

/// Quarantine `path` through the shared envelope helper (rename to
/// `<path>.corrupt`, error log, count) and return a [`QuarantinedSegment`]
/// pointing where the file now sits: the quarantine path, or the original
/// when the rename failed (the file is held by a live writer), so the file
/// is noted as quarantined even when it could not be moved.
fn quarantine_to_corrupt(path: &Path, reason: impl Into<String>) -> QuarantinedSegment {
    let reason = reason.into();
    let at = envelope::quarantine(Kind::Segment, path, &reason);
    QuarantinedSegment { path: at, reason }
}

/// Note a corrupt active segment that stays where it is because a live
/// writer holds it: logged and counted through the shared envelope helper,
/// reported at its own path.
fn quarantine_in_place(path: &Path, reason: impl Into<String>) -> QuarantinedSegment {
    let reason = reason.into();
    envelope::note_corrupt_in_place(Kind::Segment, path, &reason);
    QuarantinedSegment {
        path: path.into(),
        reason,
    }
}

/// Read an active segment whole and return the space UUID from its header
/// and the bytes of its valid prefix (header + complete records), the run
/// truncated to what `kutl_core::envelope::read_log` accepted. The tear
/// rules (a frame that does not fit, a zero length prefix, a CRC mismatch)
/// live on the envelope's scan.
fn scan_valid_prefix(file: &mut File, path: &Path) -> Result<(Uuid, Vec<u8>)> {
    let mut buf = Vec::new();
    file.seek(SeekFrom::Start(0))
        .map_err(|e| Error::SegmentRead {
            path: path.into(),
            source: e,
        })?;
    file.read_to_end(&mut buf).map_err(|e| Error::SegmentRead {
        path: path.into(),
        source: e,
    })?;
    let (header, _, run_len, _) =
        envelope::read_log(Kind::Segment, &buf).map_err(|e| Error::SegmentCorrupt {
            path: path.into(),
            reason: e.to_string(),
        })?;
    buf.truncate(HEADER_LEN + run_len);
    Ok((header.owner, buf))
}

/// Load one sealed (`.kseg.zst`) segment.
///
/// On any integrity failure the file is renamed to `<name>.corrupt` (best-
/// effort) and the failure is returned as an `Err(QuarantinedSegment)`.
/// Callers continue loading the remaining segments.
fn load_sealed(path: &Path) -> std::result::Result<Vec<Signal>, QuarantinedSegment> {
    let quarantine = |reason: &str| -> QuarantinedSegment { quarantine_to_corrupt(path, reason) };

    // Cap the compressed size before reading: zstd never expands its input
    // by more than a sliver, so a compressed file larger than the
    // uncompressed cap is corrupt by definition — and rejecting it here
    // avoids pulling a huge file into memory at all.
    let compressed_len = std::fs::metadata(path)
        .map_err(|_| quarantine("failed to stat sealed segment"))?
        .len();
    if compressed_len > MAX_SEALED_UNCOMPRESSED_BYTES {
        return Err(quarantine("compressed size exceeds the uncompressed cap"));
    }
    let compressed =
        std::fs::read(path).map_err(|_| quarantine("failed to read sealed segment"))?;

    // Bounded decode: never trust the zstd frame's declared content size.
    // `take(cap + 1)` lets us detect (rather than silently truncate) a
    // stream that decompresses past the cap.
    let decoder = zstd::Decoder::new(compressed.as_slice())
        .map_err(|_| quarantine("zstd decoder init failed"))?;
    let mut raw = Vec::new();
    decoder
        .take(MAX_SEALED_UNCOMPRESSED_BYTES + 1)
        .read_to_end(&mut raw)
        .map_err(|_| quarantine("zstd decode failed"))?;
    if u64::try_from(raw.len()).expect("buffer length fits in u64") > MAX_SEALED_UNCOMPRESSED_BYTES
    {
        return Err(quarantine("decompressed size exceeds the cap"));
    }

    if raw.len() < HEADER_LEN + FOOTER_LEN {
        return Err(quarantine("shorter than header plus footer"));
    }

    // Validate whole-file CRC (covers everything except the trailing crc32).
    let crc_split = raw.len() - size_of::<u32>();
    let crc_payload = &raw[..crc_split];
    let stored_crc = u32::from_le_bytes(raw[crc_split..].try_into().expect("4-byte slice"));
    if crc32fast::hash(crc_payload) != stored_crc {
        return Err(quarantine("whole-file crc mismatch"));
    }

    // CRC passed but header invalid cannot happen for our own sealed
    // output; it is still an integrity failure, quarantined like the rest.
    if envelope::decode_header(Kind::Segment, &raw).is_err() {
        return Err(quarantine("header invalid despite matching whole-file crc"));
    }

    // Decode records from the body between header and footer.
    let record_run = &raw[HEADER_LEN..raw.len() - FOOTER_LEN];
    let mut records = Vec::new();
    decode_records(record_run, path, &mut records)
        .map_err(|_| quarantine("record decode failed despite matching whole-file crc"))?;
    Ok(records)
}

/// Compute the `[hlc_min, hlc_max]` range over the `physical_ms` fields of
/// each record's HLC. Returns `(0, 0)` for an empty slice or one with no
/// HLC fields.
fn hlc_range(records: &[Signal]) -> (u64, u64) {
    let mut min = u64::MAX;
    let mut max = 0u64;
    for r in records {
        if let Some(hlc) = &r.hlc {
            min = min.min(hlc.physical_ms);
            max = max.max(hlc.physical_ms);
        }
    }
    if min == u64::MAX { (0, 0) } else { (min, max) }
}

/// A segment file that failed an integrity check and was set aside.
#[derive(Debug)]
pub struct QuarantinedSegment {
    /// Path to the quarantined file: `<name>.corrupt` after a successful
    /// rename, or the original path when the rename failed or was skipped
    /// (e.g. a live writer holds the lock).
    pub path: PathBuf,
    /// Human-readable failure description for ingest-layer logging.
    pub reason: String,
}

/// Everything read back from a space's segment directory.
pub struct SegmentStore {
    /// All records from every segment: sealed segments first (lexicographic
    /// filename order), then the active segment. Treat this as a multiset —
    /// the cross-segment ordering is deterministic but NOT semantic; the
    /// fold's HLC total order is the correctness contract.
    pub records: Vec<Signal>,
    /// Segments missing from `records`: the ones this load set aside, and
    /// the `.corrupt` files an earlier load set aside that still sit in the
    /// directory. Non-empty means the fold is incomplete, on every load
    /// until the files are moved out.
    pub quarantined: Vec<QuarantinedSegment>,
}

impl SegmentStore {
    /// Load every record in the directory.
    ///
    /// Sealed segments are read first in lexicographic filename order (the
    /// zero-padded integer names sort correctly), then the active segment is
    /// appended. Corrupt sealed files are quarantined (renamed to
    /// `<name>.corrupt`) and loading continues. A `.corrupt` file an earlier
    /// load left in the directory is reported in `quarantined` as well, so
    /// the fold reads as incomplete on every load until the file is moved
    /// out.
    pub fn load(dir: &Path) -> Result<Self> {
        let mut records = Vec::new();
        let mut quarantined = Vec::new();

        // One directory pass: sealed segments, plus the `.corrupt` files
        // earlier loads set aside (quarantine residue).
        let mut sealed_paths: Vec<PathBuf> = Vec::new();
        let mut residue: Vec<PathBuf> = Vec::new();
        match std::fs::read_dir(dir) {
            Ok(rd) => {
                for path in rd.flatten().map(|e| e.path()) {
                    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                        continue;
                    };
                    if name.ends_with(SEALED_SEGMENT_EXT) {
                        sealed_paths.push(path);
                    } else if name.ends_with(envelope::CORRUPT_EXT) {
                        residue.push(path);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(Error::SegmentRead {
                    path: dir.into(),
                    source: e,
                });
            }
        }

        // --- Sealed segments in lexicographic (= chronological) order ---
        sealed_paths.sort();
        for path in &sealed_paths {
            match load_sealed(path) {
                Ok(seg_records) => records.extend(seg_records),
                Err(q) => quarantined.push(q),
            }
        }

        load_active(dir, &mut records, &mut quarantined)?;

        // A `.corrupt` file is a hole in the fold for as long as it sits in
        // the directory, not only on the load that renamed it: a reader
        // deciding what the fold lacks must see it on every load. A
        // re-corrupted active segment overwrites its previous `.corrupt`,
        // so a path this load already reported is not reported twice.
        residue.sort();
        residue.retain(|path| quarantined.iter().all(|q| q.path != *path));
        quarantined.extend(residue.into_iter().map(|path| QuarantinedSegment {
            path,
            reason: QUARANTINE_RESIDUE_REASON.to_owned(),
        }));

        Ok(Self {
            records,
            quarantined,
        })
    }
}

/// Append the active segment's valid prefix to `records`. A corrupt header
/// or record run is quarantined into `quarantined` (renamed to `.corrupt`
/// when no live writer holds the lock, noted in place otherwise). A missing
/// or zero-byte active segment is a normal transient and adds nothing.
fn load_active(
    dir: &Path,
    records: &mut Vec<Signal>,
    quarantined: &mut Vec<QuarantinedSegment>,
) -> Result<()> {
    // TOCTOU-safe: open and check NotFound.
    let active = dir.join(ACTIVE_SEGMENT_NAME);
    let mut file = match File::open(&active) {
        // No active segment — normal after a rotation or first open.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(Error::SegmentRead {
                path: active.clone(),
                source: e,
            });
        }
        Ok(file) => file,
    };
    let active_len = file
        .metadata()
        .map_err(|e| Error::SegmentRead {
            path: active.clone(),
            source: e,
        })?
        .len();
    if active_len == 0 {
        // A zero-byte active file is a normal transient: both `open` and
        // rotation create the file before writing its header. Skip it — no
        // records, no quarantine.
        return Ok(());
    }
    match scan_valid_prefix(&mut file, &active) {
        Err(Error::SegmentCorrupt { reason, .. }) => {
            // Corrupt active header. Rename only when no live writer holds
            // the lock — pulling the file out from under a live writer would
            // destroy its active segment. The probe lock is held across the
            // rename to close the check-then-rename race.
            match try_probe_writer_lock(dir) {
                Some(_probe_lock) => {
                    quarantined.push(quarantine_to_corrupt(&active, reason));
                }
                None => quarantined.push(quarantine_in_place(
                    &active,
                    format!("{reason}; live writer holds the lock, file left in place"),
                )),
            }
        }
        Err(e) => return Err(e),
        Ok((_header_space, valid_bytes)) => {
            // `valid_bytes` is the already-validated prefix from
            // `scan_valid_prefix`'s `read_to_end` — no second read needed.
            // A decode failure on the active segment is quarantined (like
            // sealed segments) rather than propagated, so sealed records
            // already loaded are not discarded. The active segment's records
            // are re-fetchable via catch-up.
            if decode_records(&valid_bytes[HEADER_LEN..], &active, records).is_err() {
                // Same guard as the corrupt-header arm above: renaming the
                // active segment is destructive, so only do it when no live
                // writer holds the lock (probe held across the rename). Any
                // records decoded before the failure are already in
                // `records` — they are CRC- and decode-valid, so keeping
                // them loses nothing.
                match try_probe_writer_lock(dir) {
                    Some(_probe_lock) => {
                        quarantined.push(quarantine_to_corrupt(
                            &active,
                            "active segment record decode failed",
                        ));
                    }
                    None => quarantined.push(quarantine_in_place(
                        &active,
                        "active segment record decode failed; \
                         live writer holds the lock, file left in place",
                    )),
                }
            }
        }
    }
    Ok(())
}

/// Try to take the writer lock without blocking. Returns the locked file
/// (held until dropped) when no writer is live, `None` when a writer holds
/// the lock or the probe itself fails — treated as held, because the only
/// callers use a free lock as licence for destructive moves and must stay
/// conservative.
fn try_probe_writer_lock(dir: &Path) -> Option<File> {
    let lock_path = dir.join(LOCK_FILE_NAME);
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false) // sentinel file; never overwritten
        .write(true)
        .open(&lock_path)
        .ok()?;
    lock.try_lock().ok().map(|()| lock)
}

/// Decode a frame run into signals. The envelope's strict decode scans the
/// run, so a torn or misframed run is `Error::SegmentCorrupt`, never a
/// panic, whether the bytes came from a scanned active segment or a sealed
/// file.
fn decode_records(buf: &[u8], path: &Path, out: &mut Vec<Signal>) -> Result<()> {
    let payloads = envelope::decode_frames(buf).map_err(|e| Error::SegmentCorrupt {
        path: path.into(),
        reason: e.to_string(),
    })?;
    for payload in payloads {
        out.push(Signal::decode(payload)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{corrupt_sealed_segment, segment_paths};
    use kutl_proto::sync::{Signal, SignalEventType};
    use tempfile::TempDir;
    use uuid::Uuid;

    const SPACE: Uuid = Uuid::from_u128(0x065a_e81c_c74c_429c_af66_163d_f739_91b4);

    /// Paths of sealed (`.kseg.zst`) segments in `dir`, in name order.
    fn sealed_paths(dir: &Path) -> Vec<PathBuf> {
        segment_paths(dir).unwrap().sealed
    }

    fn message_padding(n: u32) -> kutl_proto::sync::signal::Payload {
        kutl_proto::sync::signal::Payload::Flag(kutl_proto::sync::FlagPayload {
            message: format!("record {n} {}", "x".repeat(64)),
            ..Default::default()
        })
    }

    /// A record with NO hlc — its sealed segment names as `(0, 0, count)`.
    fn rec_no_hlc(record_id: &str) -> Signal {
        let mut s = Signal {
            id: "sig-0".into(),
            space_id: SPACE.to_string(),
            record_id: record_id.into(),
            hlc: None,
            payload: Some(message_padding(0)),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    fn rec(n: u32) -> Signal {
        let mut s = Signal {
            id: format!("sig-{}", n % 3),
            space_id: SPACE.to_string(),
            record_id: format!("rec-{n}"),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: u64::from(n),
                logical: 0,
                actor: vec![0u8; 16],
            }),
            payload: Some(message_padding(n)),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    #[test]
    fn test_append_and_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        for n in 0..10 {
            w.append(&rec(n)).unwrap();
        }
        drop(w);
        let loaded = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(loaded.records.len(), 10);
        assert_eq!(loaded.records[3].record_id, "rec-3");
        assert!(loaded.quarantined.is_empty());
    }

    #[test]
    fn test_reopen_appends_after_existing_records() {
        let dir = TempDir::new().unwrap();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.append(&rec(0)).unwrap();
        }
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.append(&rec(1)).unwrap();
        }
        let loaded = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(loaded.records.len(), 2);
    }

    /// WAL discipline: a torn tail write is truncated away on reopen and
    /// the writer continues from the last complete record.
    ///
    /// The torn frame fixture includes both the length prefix (4 bytes)
    /// and a CRC field (4 bytes) — 8 bytes of `FRAME_OVERHEAD` —
    /// followed by fewer body bytes than the length prefix promises.
    #[test]
    fn test_crash_truncated_tail_recovers() {
        let dir = TempDir::new().unwrap();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.append(&rec(0)).unwrap();
            w.append(&rec(1)).unwrap();
        }
        // Simulate a torn write: length prefix (4) + crc placeholder (4)
        // promising 999 body bytes, but only 3 present.
        let current = dir.path().join(ACTIVE_SEGMENT_NAME);
        let mut bytes = std::fs::read(&current).unwrap();
        bytes.extend_from_slice(&999u32.to_le_bytes()); // length prefix
        bytes.extend_from_slice(&0u32.to_le_bytes()); // crc (any value — torn)
        bytes.extend_from_slice(&[1, 2, 3]); // only 3 of 999 body bytes
        std::fs::write(&current, bytes).unwrap();

        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        w.append(&rec(2)).unwrap();
        drop(w);
        let loaded = SegmentStore::load(dir.path()).unwrap();
        let ids: Vec<_> = loaded.records.iter().map(|r| r.record_id.clone()).collect();
        assert_eq!(ids, vec!["rec-0", "rec-1", "rec-2"]);
    }

    /// A complete-length frame with a bad CRC is truncated on reopen.
    /// (The CRC frame catches corruption before any decode is attempted.)
    #[test]
    fn test_crash_garbage_frame_recovers() {
        let dir = TempDir::new().unwrap();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.append(&rec(0)).unwrap();
        }
        // A full frame: length = 3, bad CRC, 3 garbage body bytes.
        let current = dir.path().join(ACTIVE_SEGMENT_NAME);
        let mut bytes = std::fs::read(&current).unwrap();
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&0xDEAD_BEEFu32.to_le_bytes()); // bad crc
        bytes.extend_from_slice(&[0xFF; 3]);
        std::fs::write(&current, bytes).unwrap();

        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        w.append(&rec(1)).unwrap();
        drop(w);
        let loaded = SegmentStore::load(dir.path()).unwrap();
        let ids: Vec<_> = loaded.records.iter().map(|r| r.record_id.clone()).collect();
        assert_eq!(ids, vec!["rec-0", "rec-1"]);
    }

    /// A zero-run tail (a zeroed page from power-loss writeback) is
    /// truncated away with NO phantom records.
    #[test]
    fn test_crash_zero_run_tail_no_phantom_records() {
        let dir = TempDir::new().unwrap();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.append(&rec(0)).unwrap();
        }
        let current = dir.path().join(ACTIVE_SEGMENT_NAME);
        let mut bytes = std::fs::read(&current).unwrap();
        bytes.extend_from_slice(&[0u8; 8]);
        std::fs::write(&current, bytes).unwrap();

        // Reader alone must not see phantoms.
        let loaded = SegmentStore::load(dir.path()).unwrap();
        let ids: Vec<_> = loaded.records.iter().map(|r| r.record_id.clone()).collect();
        assert_eq!(ids, vec!["rec-0"]);

        // Reopen truncates the zero run and the writer resumes cleanly.
        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        w.append(&rec(1)).unwrap();
        drop(w);
        let loaded = SegmentStore::load(dir.path()).unwrap();
        let ids: Vec<_> = loaded.records.iter().map(|r| r.record_id.clone()).collect();
        assert_eq!(ids, vec!["rec-0", "rec-1"]);
    }

    /// Rotation seals the active segment and `SegmentStore::load` returns
    /// all records across sealed + active segments.
    #[test]
    fn test_rotation_seals_and_reader_sees_all_records() {
        let dir = TempDir::new().unwrap();
        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        w.rotate_threshold_for_test(256);
        for n in 0..40 {
            w.append(&rec(n)).unwrap();
        }
        drop(w);
        assert!(
            !sealed_paths(dir.path()).is_empty(),
            "rotation must have sealed at least one segment"
        );
        let loaded = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(loaded.records.len(), 40, "sealed + active records all load");
        assert!(loaded.quarantined.is_empty());
    }

    /// A corrupt sealed segment is quarantined (renamed to `.corrupt`) and
    /// loading continues with the remaining segments.
    #[test]
    fn test_corrupt_sealed_segment_is_quarantined_not_fatal() {
        let dir = TempDir::new().unwrap();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.rotate_threshold_for_test(256);
            for n in 0..40 {
                w.append(&rec(n)).unwrap();
            }
        }

        // The injector corrupts the first sealed segment in name order.
        let sealed_files = sealed_paths(dir.path());
        assert!(!sealed_files.is_empty());
        let corrupt_target = &sealed_files[0];
        corrupt_sealed_segment(dir.path()).unwrap();

        let loaded = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(loaded.quarantined.len(), 1);
        assert!(
            loaded.quarantined[0]
                .path
                .to_string_lossy()
                .ends_with(envelope::CORRUPT_EXT),
            "quarantine path must end with .corrupt"
        );
        assert!(
            loaded.quarantined[0].path.exists(),
            "quarantined file must exist on disk"
        );
        assert!(
            !corrupt_target.exists(),
            "original sealed name must be gone"
        );
        assert!(
            !loaded.records.is_empty() && loaded.records.len() < 40,
            "some records must load from non-corrupt segments"
        );
    }

    /// A second concurrent writer is rejected with `Error::Locked`.
    #[test]
    fn test_second_writer_is_locked_out() {
        let dir = TempDir::new().unwrap();
        let _w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        match SegmentWriter::open(dir.path(), SPACE, 0) {
            Err(crate::Error::Locked { .. }) => {}
            Ok(_) => panic!("expected Locked but got Ok"),
            Err(e) => panic!("expected Locked, got {e:?}"),
        }
    }

    /// The open-time `.tmp` sweep claims only the seal temps this writer
    /// creates: another writer's atomic-write scratch file and an unrelated
    /// `.tmp` in the directory are not its to clean.
    #[test]
    fn test_tmp_sweep_only_claims_seal_temps() {
        let dir = TempDir::new().unwrap();
        let scratch = dir.path().join(".kutl-tmp-deadbeef-1-0");
        let unrelated = dir.path().join("unrelated.tmp");
        let seal_tmp = dir.path().join("foo.kseg.zst.tmp");
        std::fs::write(&scratch, b"{").unwrap();
        std::fs::write(&unrelated, b"{").unwrap();
        std::fs::write(&seal_tmp, b"garbage").unwrap();

        let _w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();

        assert!(
            scratch.exists(),
            "another writer's scratch file is not the sweep's"
        );
        assert!(unrelated.exists(), "an unrelated temp is not the sweep's");
        assert!(!seal_tmp.exists(), "orphaned seal temp must be swept");
    }

    /// A quarantined sealed segment stays reported on every later load, not
    /// only on the one that renamed it: the `.corrupt` file is a hole in the
    /// fold for as long as it sits in the directory, and moving it out
    /// clears the report.
    #[test]
    fn test_load_reports_quarantine_residue_on_every_later_load() {
        let dir = TempDir::new().unwrap();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.rotate_threshold_for_test(256);
            for n in 0..40 {
                w.append(&rec(n)).unwrap();
            }
        }
        corrupt_sealed_segment(dir.path()).unwrap();

        let first = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(first.quarantined.len(), 1, "the first load quarantines");
        let corrupt_path = first.quarantined[0].path.clone();
        let healthy = first.records.len();
        assert!(healthy < 40, "the quarantined segment's records are absent");

        let second = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(
            second.records.len(),
            healthy,
            "the quarantined segment's records stay absent"
        );
        assert_eq!(
            second.quarantined.len(),
            1,
            "the residue is reported as quarantined on the next load"
        );
        assert_eq!(second.quarantined[0].path, corrupt_path);
        assert_eq!(second.quarantined[0].reason, QUARANTINE_RESIDUE_REASON);

        std::fs::remove_file(&corrupt_path).unwrap();
        let third = SegmentStore::load(dir.path()).unwrap();
        assert!(
            third.quarantined.is_empty(),
            "moving the file out clears the report"
        );
        assert_eq!(third.records.len(), healthy);
    }

    /// A corrupt active-segment header is quarantined by `SegmentWriter::open`
    /// (renamed to `.corrupt`) and a fresh segment is started. `SegmentStore::load`
    /// on a dir containing only a corrupt-header active file returns 0 records
    /// and 1 quarantined path.
    #[test]
    fn test_corrupt_active_header_quarantines_and_restarts() {
        let dir = TempDir::new().unwrap();
        // Write a valid segment, then corrupt its magic bytes.
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.append(&rec(0)).unwrap();
        }
        let current = dir.path().join(ACTIVE_SEGMENT_NAME);
        let mut bytes = std::fs::read(&current).unwrap();
        bytes[0] = 0x00;
        bytes[1] = 0x00;
        bytes[2] = 0x00;
        bytes[3] = 0x00;
        std::fs::write(&current, &bytes).unwrap();

        // Writer must succeed and start fresh (not panic or error).
        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        w.append(&rec(1)).unwrap();
        drop(w);

        // The .corrupt file must exist.
        let corrupt = current.with_extension("kseg.corrupt");
        assert!(
            corrupt.exists(),
            "corrupt active header must be quarantined"
        );

        // For a dir with ONLY a corrupt-header active file:
        // load returns empty records and 1 quarantined path.
        let dir2 = TempDir::new().unwrap();
        let current2 = dir2.path().join(ACTIVE_SEGMENT_NAME);
        {
            let mut w2 = SegmentWriter::open(dir2.path(), SPACE, 0).unwrap();
            w2.append(&rec(0)).unwrap();
        }
        let mut bytes2 = std::fs::read(&current2).unwrap();
        bytes2[0] = 0x00;
        std::fs::write(&current2, &bytes2).unwrap();

        let loaded = SegmentStore::load(dir2.path()).unwrap();
        assert_eq!(loaded.records.len(), 0);
        assert_eq!(loaded.quarantined.len(), 1);
    }

    /// A record larger than `MAX_RECORD_BYTES` is rejected before encoding.
    /// The writer's length is unchanged and a subsequent small append succeeds.
    #[test]
    fn test_record_too_large_rejected() {
        let dir = TempDir::new().unwrap();
        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        let len_before = w.len;

        // Build an oversized record by stuffing a payload larger than MAX_RECORD_BYTES.
        let huge_payload = "x".repeat(MAX_RECORD_BYTES + 1);
        let mut big = Signal {
            id: "sig-big".into(),
            space_id: SPACE.to_string(),
            record_id: "rec-big".into(),
            payload: Some(kutl_proto::sync::signal::Payload::Flag(
                kutl_proto::sync::FlagPayload {
                    message: huge_payload,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        big.set_event(SignalEventType::Created);

        match w.append(&big) {
            Err(crate::Error::RecordTooLarge { .. }) => {}
            other => panic!("expected RecordTooLarge, got {other:?}"),
        }
        // Writer length must not have changed.
        assert_eq!(
            w.len, len_before,
            "length must not change after rejected record"
        );
        // Subsequent small append must succeed.
        w.append(&rec(0)).unwrap();
    }

    /// With a monotonic-HLC fixture (`physical_ms == n`) sealed filenames
    /// sort chronologically, so `load` happens to return records in append
    /// order across segments. This is a property of the fixture, NOT a
    /// general guarantee — cross-segment ordering is a deterministic
    /// multiset order; the fold's HLC total order is the correctness
    /// contract.
    #[test]
    fn test_sealed_filename_orders_lexicographically() {
        let dir = TempDir::new().unwrap();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.rotate_threshold_for_test(256);
            for n in 0..40 {
                w.append(&rec(n)).unwrap();
            }
        }

        let loaded = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(loaded.records.len(), 40);
        let record_ids: Vec<_> = loaded.records.iter().map(|r| r.record_id.clone()).collect();
        let expected: Vec<_> = (0..40u32).map(|n| format!("rec-{n}")).collect();
        assert_eq!(
            record_ids, expected,
            "with monotonic HLCs, lexicographic file order preserves append order"
        );
    }

    /// Two sealed segments with an identical `(hlc_min,
    /// hlc_max, count)` — e.g. HLC-less batches, which all name `(0, 0, n)`
    /// — must NOT collide. The whole-file CRC component in the sealed
    /// filename disambiguates them; without it the
    /// second rename would silently overwrite the first sealed file, destroying
    /// its records.
    #[test]
    fn test_hlc_less_rotations_do_not_collide() {
        let dir = TempDir::new().unwrap();
        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        // Rotate on every append past the bare header so each sealed
        // segment holds exactly one record: identical `(0, 0, 1)` prefixes.
        w.rotate_threshold_for_test(u64::try_from(HEADER_LEN).unwrap() + 1);
        w.append(&rec_no_hlc("batch-a")).unwrap();
        w.append(&rec_no_hlc("batch-b")).unwrap(); // seals [batch-a]
        w.append(&rec_no_hlc("batch-c")).unwrap(); // seals [batch-b]
        drop(w);

        assert_eq!(
            sealed_paths(dir.path()).len(),
            2,
            "two same-range sealed segments must coexist"
        );
        let loaded = SegmentStore::load(dir.path()).unwrap();
        assert!(loaded.quarantined.is_empty());
        let mut ids: Vec<_> = loaded.records.iter().map(|r| r.record_id.clone()).collect();
        ids.sort();
        assert_eq!(
            ids,
            vec!["batch-a", "batch-b", "batch-c"],
            "all records survive same-range rotations"
        );
    }

    /// A zero-byte active file is a normal transient (create-then-write
    /// gaps at open and rotation): `load` must skip it entirely — no
    /// records, no quarantine, file left untouched. A reader quarantining
    /// it would destroy a live writer's active segment.
    #[test]
    fn test_zero_byte_active_is_skipped_not_quarantined() {
        let dir = TempDir::new().unwrap();
        let current = dir.path().join(ACTIVE_SEGMENT_NAME);
        std::fs::write(&current, b"").unwrap();

        let loaded = SegmentStore::load(dir.path()).unwrap();
        assert!(loaded.records.is_empty());
        assert!(loaded.quarantined.is_empty());
        assert!(current.exists(), "zero-byte active must be left in place");
        assert_eq!(std::fs::metadata(&current).unwrap().len(), 0);
    }

    /// Crash window: loss between the sealed rename and the active-file
    /// removal leaves BOTH files on disk. `load` then sees the sealed batch
    /// twice (benign: the fold dedups by `record_id`), and a writer reopen
    /// self-heals — re-sealing identical content produces the identical
    /// content-derived filename, so the overwrite is idempotent and the
    /// duplicates disappear.
    #[test]
    fn test_crash_between_rename_and_remove_duplicates_then_self_heals() {
        let dir = TempDir::new().unwrap();
        let current = dir.path().join(ACTIVE_SEGMENT_NAME);

        // Append until the first rotation, snapshotting the active bytes
        // before each append: the last snapshot is the exact content the
        // rotation sealed.
        let mut pre_rotation = Vec::new();
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.rotate_threshold_for_test(256);
            let mut n = 0u32;
            while sealed_paths(dir.path()).is_empty() {
                pre_rotation = std::fs::read(&current).unwrap();
                w.append(&rec(n)).unwrap();
                n += 1;
                assert!(n < 100, "rotation must happen within 100 appends");
            }
        }
        assert_eq!(sealed_paths(dir.path()).len(), 1);

        // Manufacture the crash state: sealed file present AND the
        // pre-rotation active restored (the removal never happened).
        std::fs::write(&current, &pre_rotation).unwrap();

        let loaded = SegmentStore::load(dir.path()).unwrap();
        let dup = loaded
            .records
            .iter()
            .filter(|r| r.record_id == "rec-0")
            .count();
        assert_eq!(dup, 2, "crash window duplicates the sealed batch");

        // Reopen: the restored active is past-threshold, so the next append
        // re-seals identical content -> identical name -> idempotent
        // overwrite of the existing sealed file.
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.rotate_threshold_for_test(256);
            w.append(&rec(90)).unwrap();
        }
        assert_eq!(
            sealed_paths(dir.path()).len(),
            1,
            "re-seal must reuse the same filename"
        );
        let healed = SegmentStore::load(dir.path()).unwrap();
        assert!(healed.quarantined.is_empty());
        let dup = healed
            .records
            .iter()
            .filter(|r| r.record_id == "rec-0")
            .count();
        assert_eq!(dup, 1, "self-heal removes the duplicates");
        assert!(
            healed.records.iter().any(|r| r.record_id == "rec-90"),
            "the append that triggered the re-seal must survive"
        );
    }

    /// A poisoned writer rejects all subsequent appends with `SegmentWrite`,
    /// preventing writes after a torn frame. A fresh open on the same dir
    /// self-heals via `scan_valid_prefix` and succeeds normally.
    #[test]
    fn test_poisoned_writer_rejects_append() {
        let dir = TempDir::new().unwrap();
        let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
        w.append(&rec(0)).unwrap();
        w.poison_for_test();
        // All subsequent appends on the poisoned writer must fail.
        match w.append(&rec(1)) {
            Err(crate::Error::SegmentWrite { .. }) => {}
            other => panic!("expected SegmentWrite from poisoned writer, got {other:?}"),
        }
        // A second attempt also fails — the poison is sticky.
        match w.append(&rec(2)) {
            Err(crate::Error::SegmentWrite { .. }) => {}
            other => panic!("expected SegmentWrite from poisoned writer (2nd), got {other:?}"),
        }
        drop(w);
        // A fresh open self-heals and can read the record that did land.
        let loaded = SegmentStore::load(dir.path()).unwrap();
        assert!(loaded.quarantined.is_empty());
        let ids: Vec<_> = loaded.records.iter().map(|r| r.record_id.clone()).collect();
        assert_eq!(ids, vec!["rec-0"]);
    }

    /// A corrupt active segment (valid header but record decode failure)
    /// is quarantined by `SegmentStore::load`, and sealed records that
    /// loaded before it are NOT discarded — load returns `Ok`.
    #[test]
    fn test_corrupt_active_record_run_quarantined_sealed_records_survive() {
        let dir = TempDir::new().unwrap();
        // Write enough records to force a rotation (sealed segments exist)
        // then add a record to the fresh active.
        {
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.rotate_threshold_for_test(256);
            for n in 0..10 {
                w.append(&rec(n)).unwrap();
            }
        }
        assert!(
            !sealed_paths(dir.path()).is_empty(),
            "rotation must have occurred"
        );

        // Corrupt the active segment's record run (past a valid header) by
        // overwriting everything after the header with garbage that passes
        // scan_valid_prefix's CRC check but fails prost decode. Craft a frame
        // with a length that fits, a matching CRC, but garbage prost body.
        let active = dir.path().join(ACTIVE_SEGMENT_NAME);
        {
            // Build a fresh one-record active with a valid header…
            let mut w = SegmentWriter::open(dir.path(), SPACE, 0).unwrap();
            w.append(&rec(99)).unwrap();
            drop(w);
        }
        // …then replace the record run with one frame the codec framed
        // itself: a valid CRC around bytes that cannot decode as Signal, so
        // scan_valid_prefix accepts the frame and the decode fails.
        let mut raw = std::fs::read(&active).unwrap();
        raw.truncate(HEADER_LEN);
        raw.extend_from_slice(&envelope::encode_frame(&[0xFF; 16]));
        std::fs::write(&active, &raw).unwrap();

        // load must return Ok (not Err) even though the active is corrupt,
        // and the sealed records must survive.
        let loaded = SegmentStore::load(dir.path()).unwrap();
        assert_eq!(
            loaded.quarantined.len(),
            1,
            "corrupt active must be quarantined"
        );
        assert!(
            !loaded.records.is_empty(),
            "sealed records must survive a corrupt active segment"
        );
    }
}
