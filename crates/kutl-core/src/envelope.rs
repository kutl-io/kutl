//! The one on-disk framing for every file only the daemon reads: a 30-byte
//! header naming the file's kind and layout version, then CRC-framed
//! protobuf records.
//!
//! ```text
//! header (30 bytes): magic (4) | version u16 LE | owner id (16) | created_at_ms i64 LE
//! frame  (8 + n)   : payload length u32 LE | crc32 of payload u32 LE | payload
//! ```
//!
//! Two uses, one codec. A **snapshot** is a header followed by exactly one
//! frame, replaced whole through [`crate::fs::write_atomic`]
//! ([`write_snapshot`], [`load_or_recover`]). A **record
//! log** is a header followed by appended frames ([`open_log`] stamps the
//! header on an empty file, [`read_log`] validates and scans, and
//! [`truncate_torn`] cuts a torn tail); a reader takes the frames up to the
//! last valid one and ignores what follows, so a crash mid-append loses
//! only the torn frame. Readers detect a file by its magic and version,
//! never by its name: a file with the wrong magic, a version this reader
//! does not decode, or a snapshot whose CRC fails is corrupt, and the caller
//! takes the kind's recovery action. Recovery is never silent: the caller
//! quarantines the file with [`quarantine`], which renames it beside its
//! replacement, logs at error level with the kind's action, and counts it
//! for the daemon's metrics.
//!
//! Compatibility runs on two rules. Payloads are protobuf messages, so
//! fields are additive and never renumbered. A reader decodes the current
//! layout version and the one before it; anything older takes the recovery
//! action. Files written before the envelope existed (JSON, or a bare
//! protobuf message) are read through a caller-supplied legacy decoder that
//! carries the version at which it must be deleted, and a test in the
//! caller's crate names each of its decoders and fails when that version
//! has shipped.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use prost::Message;
use uuid::Uuid;

/// Byte width of the magic that opens every envelope file.
const MAGIC_LEN: usize = 4;
/// Byte width of the layout version that follows the magic.
const VERSION_BYTES: usize = 2;
/// Byte width of the owner id.
const OWNER_BYTES: usize = 16;
/// Byte width of the creation stamp.
const CREATED_AT_BYTES: usize = 8;
/// Header offsets, in layout order.
const VERSION_OFFSET: usize = MAGIC_LEN;
const OWNER_OFFSET: usize = VERSION_OFFSET + VERSION_BYTES;
const CREATED_AT_OFFSET: usize = OWNER_OFFSET + OWNER_BYTES;
/// Header length: magic, version, owner id, creation stamp.
pub const HEADER_LEN: usize = CREATED_AT_OFFSET + CREATED_AT_BYTES;
/// Byte width of the little-endian u32 length prefix framing each record.
const FRAME_LEN_BYTES: usize = 4;
/// Byte width of the CRC32 field that follows the length prefix.
const FRAME_CRC_BYTES: usize = 4;
/// Per-record frame overhead: length prefix (4) + crc32 (4).
pub const FRAME_OVERHEAD: usize = FRAME_LEN_BYTES + FRAME_CRC_BYTES;
/// Extension appended to a quarantined file.
pub const CORRUPT_EXT: &str = ".corrupt";

/// Files this process has quarantined, for a gauge the daemon's metrics
/// tick reads: a nonzero value is a file that was rebuilt rather than read.
static QUARANTINED: AtomicU64 = AtomicU64::new(0);
/// Files this process has rewritten from their pre-envelope shape, for the
/// same gauge family: a migration is expected once per file, and a count
/// that keeps climbing means a rewrite is not landing.
static MIGRATED: AtomicU64 = AtomicU64::new(0);

/// The number of files [`quarantine`] has moved aside in this process.
#[must_use]
pub fn quarantine_count() -> u64 {
    QUARANTINED.load(Ordering::Relaxed)
}

/// The number of files [`note_migration`] has recorded in this process.
#[must_use]
pub fn migration_count() -> u64 {
    MIGRATED.load(Ordering::Relaxed)
}

/// The kinds of envelope file. The table is the complete list of places a
/// format decision can live: each kind names its magic, its current layout
/// version, the file name its pre-envelope shape lived under when the move
/// renamed it, and the recovery action a corrupt file of that kind takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    /// A signal segment (`.kseg`): a record log of `Signal` messages.
    Segment,
    /// The daemon's identity state (`state.ksnap`): a snapshot.
    State,
    /// The daemon's binary-file state (`blob-state.ksnap`): a snapshot.
    BlobState,
    /// The identity journal (`identity.klog`): a record log.
    IdentityLog,
    /// A document's change-metadata sidecar (`<doc>.changes`): a snapshot.
    Changes,
}

impl Kind {
    /// Every kind, for the tests that walk the table.
    pub const ALL: [Kind; 5] = [
        Kind::Segment,
        Kind::State,
        Kind::BlobState,
        Kind::IdentityLog,
        Kind::Changes,
    ];

    /// The four bytes that open a file of this kind.
    const fn magic(self) -> &'static [u8; MAGIC_LEN] {
        match self {
            Kind::Segment => b"KSEG",
            Kind::State => b"KDST",
            Kind::BlobState => b"KBST",
            Kind::IdentityLog => b"KIDL",
            Kind::Changes => b"KCHG",
        }
    }

    /// The layout version this build writes. Bumped only for a change to
    /// the header or frame layout of this kind, never for a message change.
    #[must_use]
    pub const fn current_version(self) -> u16 {
        match self {
            Kind::Segment | Kind::State | Kind::BlobState | Kind::IdentityLog | Kind::Changes => 1,
        }
    }

    /// The oldest layout version this build still reads: the version before
    /// the current one, floored at 1. Version 0 is the pre-envelope shape,
    /// which has no header and is read through a [`Legacy`] decoder instead.
    #[must_use]
    pub const fn oldest_supported_version(self) -> u16 {
        oldest_supported(self.current_version())
    }

    /// Whether this reader decodes `version` of the layout.
    #[must_use]
    pub const fn supports(self, version: u16) -> bool {
        version >= self.oldest_supported_version() && version <= self.current_version()
    }

    /// The file name the pre-envelope shape lived under when the move
    /// renamed the file, beside the envelope's path. `None` when the name
    /// did not change and the old bytes sit at the same path.
    #[must_use]
    pub const fn old_name(self) -> Option<&'static str> {
        match self {
            Kind::State => Some("state.json"),
            Kind::BlobState => Some("blob-state.json"),
            Kind::IdentityLog => Some("identity.log"),
            Kind::Segment | Kind::Changes => None,
        }
    }

    /// What a reader does after quarantining a corrupt file of this kind:
    /// the one place the recovery action is spelled, so the error log and
    /// the code that takes it cannot disagree.
    #[must_use]
    pub const fn recovery_action(self) -> &'static str {
        match self {
            Kind::Segment => "reinitialise the segment; its records are refetched by catch-up",
            Kind::State => {
                "start from an empty state and rebuild from the relay's document list and the files on disk"
            }
            Kind::BlobState => {
                "start with an empty blob state; every blob is re-hashed and re-sent once"
            }
            Kind::IdentityLog => {
                "skip journal replay; the snapshot stands alone until the next save"
            }
            Kind::Changes => {
                "load the document without change metadata; the next save writes a fresh sidecar"
            }
        }
    }
}

/// The oldest layout version a reader at `current` still decodes: the one
/// before it, floored at 1.
const fn oldest_supported(current: u16) -> u16 {
    if current > 1 { current - 1 } else { 1 }
}

/// A decoder for the shape a file had before it became an envelope. It is
/// owned by the crate that owns the message type, lives in that crate's one
/// legacy module, and carries the layout version at which it must be gone:
/// [`Legacy::assert_not_expired`] fails the crate's tests once
/// [`Kind::current_version`] reaches it.
pub struct Legacy<T> {
    /// The kind whose pre-envelope shape this decodes.
    pub kind: Kind,
    /// The layout version at which this decoder is deleted.
    pub expires_at_version: u16,
    /// Decode the pre-envelope bytes, or `None` when they are not that shape.
    pub decode: fn(&[u8]) -> Option<T>,
}

impl<T> Legacy<T> {
    /// Fail when the version that ends this decoder's window has shipped.
    /// Called from the owning crate's expiry test, once per registered
    /// decoder.
    ///
    /// # Panics
    ///
    /// When the kind's current version is at or past `expires_at_version`.
    pub fn assert_not_expired(&self) {
        assert!(
            self.kind.current_version() < self.expires_at_version,
            "the legacy decoder for {:?} expired at version {}: delete it, its types, \
             and its fixtures",
            self.kind,
            self.expires_at_version
        );
    }
}

/// What went wrong with an envelope file. The reason strings are for the
/// error log line the caller writes before taking the recovery action.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The file could not be read or written.
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// The header is missing, carries another kind's magic, or names a
    /// version this reader does not decode.
    #[error("bad header: {0}")]
    Header(String),
    /// A frame is torn, its CRC does not match, or the run is misframed.
    #[error("bad frame: {0}")]
    Frame(String),
    /// The payload does not decode as the expected message.
    #[error("bad payload: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Result alias for envelope operations.
pub type Result<T> = std::result::Result<T, Error>;

/// The parsed header of an envelope file: what the reader did not already
/// know when it asked (the kind is the caller's argument, checked against
/// the magic).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    /// The layout version the writer used.
    pub version: u16,
    /// The owner id the writer stamped (a space id, or nil).
    pub owner: Uuid,
    /// The creation stamp the writer supplied, in Unix milliseconds.
    pub created_at_ms: i64,
}

/// Encode a header for `kind` at its current version.
#[must_use]
pub fn encode_header(kind: Kind, owner: Uuid, created_at_ms: i64) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..MAGIC_LEN].copy_from_slice(kind.magic());
    h[VERSION_OFFSET..OWNER_OFFSET].copy_from_slice(&kind.current_version().to_le_bytes());
    h[OWNER_OFFSET..CREATED_AT_OFFSET].copy_from_slice(owner.as_bytes());
    h[CREATED_AT_OFFSET..].copy_from_slice(&created_at_ms.to_le_bytes());
    h
}

/// Parse and validate the header at the front of `buf` for `kind`: the
/// magic must be `kind`'s and the version one this reader decodes.
pub fn decode_header(kind: Kind, buf: &[u8]) -> Result<Header> {
    if buf.len() < HEADER_LEN {
        return Err(Error::Header(format!(
            "{} bytes, shorter than the {HEADER_LEN}-byte header",
            buf.len()
        )));
    }
    if &buf[..MAGIC_LEN] != kind.magic() {
        return Err(Error::Header(format!(
            "magic {:?} is not {:?}",
            String::from_utf8_lossy(&buf[..MAGIC_LEN]),
            String::from_utf8_lossy(kind.magic())
        )));
    }
    let version = u16::from_le_bytes(
        buf[VERSION_OFFSET..OWNER_OFFSET]
            .try_into()
            .expect("2-byte slice"),
    );
    if !kind.supports(version) {
        return Err(Error::Header(format!(
            "layout version {version} is outside the versions this build reads ({}..={})",
            kind.oldest_supported_version(),
            kind.current_version()
        )));
    }
    let owner = Uuid::from_slice(&buf[OWNER_OFFSET..CREATED_AT_OFFSET])
        .expect("header owner slice is exactly 16 bytes");
    let created_at_ms = i64::from_le_bytes(
        buf[CREATED_AT_OFFSET..HEADER_LEN]
            .try_into()
            .expect("8-byte slice"),
    );
    Ok(Header {
        version,
        owner,
        created_at_ms,
    })
}

/// Whether `buf` opens with `kind`'s magic: the cheap test a caller uses to
/// tell an envelope file from its pre-envelope shape before decoding either.
#[must_use]
pub fn has_magic(kind: Kind, buf: &[u8]) -> bool {
    buf.len() >= MAGIC_LEN && &buf[..MAGIC_LEN] == kind.magic()
}

/// Frame `payload` for appending: length prefix, CRC32, bytes.
#[must_use]
pub fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let len = u32::try_from(payload.len()).expect("a frame payload is bounded by the caller");
    let mut out = Vec::with_capacity(FRAME_OVERHEAD + payload.len());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// How a scan of a frame run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanEnd {
    /// Every byte was a complete, CRC-valid frame.
    Clean,
    /// The run ended in a frame that does not fit, a zero length prefix
    /// where the scan refuses them (see [`scan_frames`]), or a CRC
    /// mismatch; the valid prefix stops before it.
    Torn,
}

/// Walk the frames in `run` (the bytes after a header) and return the
/// payload slices of the valid prefix and the byte length of that prefix.
/// Used for record logs, where a torn tail is expected after a crash and
/// the file is truncated to the header plus what this returns (see
/// [`truncate_torn`]). A zero length prefix ends the prefix: zero-filled
/// pages after power loss would otherwise decode as an empty message and
/// pass, so a record log never admits an empty frame.
#[must_use]
pub fn scan_frames(run: &[u8]) -> (Vec<&[u8]>, usize, ScanEnd) {
    scan_frames_with(run, false)
}

/// [`scan_frames`] with the zero-length rule as a parameter: a snapshot is
/// replaced whole and never has zero-filled pages, and may hold a message
/// that encodes to no bytes, so its one frame may be empty.
fn scan_frames_with(run: &[u8], allow_empty: bool) -> (Vec<&[u8]>, usize, ScanEnd) {
    let mut payloads = Vec::new();
    let mut pos = 0usize;
    loop {
        let remaining = run.len() - pos;
        if remaining < FRAME_OVERHEAD {
            let end = if remaining == 0 {
                ScanEnd::Clean
            } else {
                ScanEnd::Torn
            };
            return (payloads, pos, end);
        }
        let len = u32::from_le_bytes(
            run[pos..pos + FRAME_LEN_BYTES]
                .try_into()
                .expect("4-byte slice"),
        );
        if len == 0 && !allow_empty {
            return (payloads, pos, ScanEnd::Torn);
        }
        let len = usize::try_from(len).expect("u32 fits in usize on any supported platform");
        if len > remaining - FRAME_OVERHEAD {
            return (payloads, pos, ScanEnd::Torn);
        }
        let crc_start = pos + FRAME_LEN_BYTES;
        let stored_crc = u32::from_le_bytes(
            run[crc_start..crc_start + FRAME_CRC_BYTES]
                .try_into()
                .expect("4-byte slice"),
        );
        let body_start = crc_start + FRAME_CRC_BYTES;
        let body = &run[body_start..body_start + len];
        if crc32fast::hash(body) != stored_crc {
            return (payloads, pos, ScanEnd::Torn);
        }
        payloads.push(body);
        pos = body_start + len;
    }
}

/// Decode every frame in `run` strictly: a torn or misframed run is an
/// error. Used for snapshots and sealed runs, where a tear is corruption
/// rather than an expected crash artifact.
pub fn decode_frames(run: &[u8]) -> Result<Vec<&[u8]>> {
    decode_frames_with(run, false)
}

fn decode_frames_with(run: &[u8], allow_empty: bool) -> Result<Vec<&[u8]>> {
    let (payloads, valid_len, end) = scan_frames_with(run, allow_empty);
    if end == ScanEnd::Torn {
        return Err(Error::Frame(format!(
            "run of {} bytes is not whole frames: valid prefix ends at {valid_len}",
            run.len()
        )));
    }
    Ok(payloads)
}

/// Encode a snapshot: header plus exactly one frame around `message`. A
/// snapshot's owner id is nil: every snapshot kind is a per-space
/// bookkeeping file whose directory already names the space.
fn encode_snapshot<M: Message>(kind: Kind, created_at_ms: i64, message: &M) -> Vec<u8> {
    let payload = message.encode_to_vec();
    let mut out = Vec::with_capacity(HEADER_LEN + FRAME_OVERHEAD + payload.len());
    out.extend_from_slice(&encode_header(kind, Uuid::nil(), created_at_ms));
    out.extend_from_slice(&encode_frame(&payload));
    out
}

/// Replace `path` with a snapshot of `message`, atomically.
pub fn write_snapshot<M: Message>(
    kind: Kind,
    path: &Path,
    created_at_ms: i64,
    message: &M,
) -> Result<()> {
    crate::fs::write_atomic(path, &encode_snapshot(kind, created_at_ms, message))?;
    Ok(())
}

/// [`write_snapshot`], then read the file back and decode it: the write a
/// caller performs before it removes a pre-envelope file, so the old shape
/// is never dropped on the strength of an unproven replacement.
pub fn write_snapshot_verified<M: Message + Default>(
    kind: Kind,
    path: &Path,
    created_at_ms: i64,
    message: &M,
) -> Result<()> {
    write_snapshot(kind, path, created_at_ms, message)?;
    decode_snapshot::<M>(kind, &std::fs::read(path)?)?;
    Ok(())
}

/// Decode a snapshot from `bytes`: a validated header and exactly one
/// CRC-valid frame holding the message.
pub fn decode_snapshot<M: Message + Default>(kind: Kind, bytes: &[u8]) -> Result<(Header, M)> {
    let header = decode_header(kind, bytes)?;
    // A message that encodes to no bytes (an empty state) is one empty
    // frame, which a snapshot admits (see `scan_frames_with`).
    let payloads = decode_frames_with(&bytes[HEADER_LEN..], true)?;
    let [payload] = payloads.as_slice() else {
        return Err(Error::Frame(format!(
            "a snapshot holds one frame, found {}",
            payloads.len()
        )));
    };
    Ok((header, M::decode(*payload)?))
}

/// What reading a snapshot found.
#[derive(Debug)]
pub enum Loaded<M> {
    /// The current envelope shape.
    Envelope(M),
    /// The pre-envelope shape, decoded by the caller's legacy decoder. The
    /// caller writes the current shape on its next save and, when the
    /// legacy shape lived under another name, removes that file after the
    /// new one is verified in place.
    Legacy(M),
}

/// Read a snapshot of `kind` at `path`, or its pre-envelope shape when a
/// [`Legacy`] decoder is supplied: the envelope at `path` first; then the
/// legacy file, at `path` when the kind's name did not change, else under
/// [`Kind::old_name`] beside it. `Ok(None)` when neither exists. A present
/// envelope that does not decode is an error, and the caller's recovery
/// action, unless a decodable legacy file still sits beside it: that file
/// is the last good copy, so it is served (with the envelope's fault logged
/// at error level) and the caller's next save replaces the damaged
/// envelope. A present file that is neither shape is an error.
fn read_snapshot<M: Message + Default>(
    kind: Kind,
    path: &Path,
    legacy: Option<&Legacy<M>>,
) -> Result<Option<Loaded<M>>> {
    debug_assert!(
        legacy.is_none_or(|l| l.kind == kind),
        "legacy decoder paired with its kind"
    );
    let envelope_error = match std::fs::read(path) {
        Ok(bytes) if has_magic(kind, &bytes) => match decode_snapshot(kind, &bytes) {
            Ok((_, message)) => return Ok(Some(Loaded::Envelope(message))),
            Err(e) => Some(e),
        },
        Ok(bytes) if kind.old_name().is_none() => {
            // Present, but not an envelope, at a name the pre-envelope shape
            // also used: that shape, or corruption. Too short to carry a
            // magic is corruption, never a legacy shape.
            return match legacy.filter(|_| bytes.len() >= MAGIC_LEN) {
                Some(l) => (l.decode)(&bytes)
                    .map(|m| Some(Loaded::Legacy(m)))
                    .ok_or_else(|| {
                        Error::Header("neither an envelope nor the legacy shape".into())
                    }),
                None => Err(Error::Header("no envelope magic".into())),
            };
        }
        Ok(_) => Some(Error::Header("no envelope magic".into())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    let (Some(l), Some(old_name)) = (legacy, kind.old_name()) else {
        return envelope_error.map_or(Ok(None), Err);
    };
    let old_path = path.with_file_name(old_name);
    match std::fs::read(&old_path) {
        Ok(bytes) => match (l.decode)(&bytes) {
            Some(m) => {
                if let Some(e) = &envelope_error {
                    tracing::error!(
                        kind = ?kind, path = %path.display(), legacy = %old_path.display(),
                        reason = %e,
                        "corrupt envelope; serving the pre-envelope file beside it until the next save replaces it"
                    );
                }
                Ok(Some(Loaded::Legacy(m)))
            }
            None => Err(envelope_error.unwrap_or_else(|| {
                Error::Header(format!("{} is not the legacy shape", old_path.display()))
            })),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => envelope_error.map_or(Ok(None), Err),
        Err(e) => Err(e.into()),
    }
}

/// What a reader does with a file it cannot read: the file's owner takes
/// the recovery action (quarantine a corrupt snapshot, cut a torn log); a
/// reader beside a running owner leaves the file alone for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recovery {
    /// Quarantine a corrupt snapshot; truncate a torn log.
    Act,
    /// Log and degrade; touch nothing.
    Skip,
}

/// `read_snapshot` with the recovery decision folded in: a snapshot that
/// does not decode is quarantined (or, for a reader that does not own the
/// file, logged and left) and reads as absent, so the caller rebuilds per
/// the kind's action. Only an IO failure is an error.
pub fn load_or_recover<M: Message + Default>(
    kind: Kind,
    path: &Path,
    legacy: Option<&Legacy<M>>,
    recovery: Recovery,
) -> std::io::Result<Option<Loaded<M>>> {
    match read_snapshot(kind, path, legacy) {
        Ok(loaded) => Ok(loaded),
        Err(Error::Io(e)) => Err(e),
        Err(e) => {
            match recovery {
                Recovery::Act => {
                    quarantine_snapshot(kind, path, &e.to_string());
                }
                Recovery::Skip => note_corrupt_in_place(kind, path, &e.to_string()),
            }
            Ok(None)
        }
    }
}

/// Retire the pre-envelope file of `kind` in `dir` now that its envelope at
/// `replacement` is in place and read back, so the file never has two
/// sources, and record the migration. A kind whose legacy bytes sat at the
/// same path has nothing to retire. Best effort: an absent file is the
/// steady state, and a failed removal is logged, never fatal, because the
/// envelope is already the source every reader prefers.
pub fn retire_legacy(kind: Kind, dir: &Path, replacement: &Path) {
    let Some(old) = kind.old_name() else {
        return;
    };
    let old_path = dir.join(old);
    match std::fs::remove_file(&old_path) {
        Ok(()) => note_migration(kind, &old_path, replacement),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!(
            kind = ?kind, file = %old_path.display(), error = %e,
            "could not remove the pre-envelope file"
        ),
    }
}

/// Open the record log at `path` for `kind`, creating it with a header
/// stamped `owner` and `created_at_ms` when it is empty (the header is
/// synced, so a crash before the first record still leaves a valid file).
/// Returns the file, positioned at its end, and its length. An existing
/// file's contents are not validated here: a writer that reopens after a
/// crash reads it with [`read_log`] and cuts a torn tail first.
pub fn open_log(
    kind: Kind,
    path: &Path,
    owner: Uuid,
    created_at_ms: i64,
) -> std::io::Result<(std::fs::File, u64)> {
    use std::io::{Seek, Write};
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let mut len = file.metadata()?.len();
    if len == 0 {
        file.write_all(&encode_header(kind, owner, created_at_ms))?;
        file.sync_data()?;
        len = u64::try_from(HEADER_LEN).expect("the header length fits in u64");
    }
    file.seek(std::io::SeekFrom::End(0))?;
    Ok((file, len))
}

/// Read a record log's bytes: validate the header for `kind`, then scan
/// the frame run. Returns the header, the payloads of the valid prefix,
/// the length of that prefix in bytes after the header (the argument
/// [`truncate_torn`] takes), and how the scan ended.
pub fn read_log(kind: Kind, bytes: &[u8]) -> Result<(Header, Vec<&[u8]>, usize, ScanEnd)> {
    let header = decode_header(kind, bytes)?;
    let (payloads, valid_run_len, end) = scan_frames(&bytes[HEADER_LEN..]);
    Ok((header, payloads, valid_run_len, end))
}

/// Cut the record log at `path` back to the prefix [`read_log`] accepted:
/// its header plus `valid_run_len` bytes of whole frames, so the next append
/// lands where replay reads rather than behind a tear.
pub fn truncate_torn(path: &Path, valid_run_len: usize) -> std::io::Result<()> {
    let len = u64::try_from(HEADER_LEN + valid_run_len).expect("a file length fits in u64");
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)?
        .set_len(len)
}

/// Record that a file of `kind` was rewritten from its pre-envelope shape:
/// an info line naming both files and the versions, and one count for the
/// daemon's metrics.
pub fn note_migration(kind: Kind, from: &Path, to: &Path) {
    MIGRATED.fetch_add(1, Ordering::Relaxed);
    tracing::info!(
        kind = ?kind,
        from = %from.display(),
        to = %to.display(),
        from_version = 0u16,
        to_version = kind.current_version(),
        "rewrote a pre-envelope file as an envelope"
    );
}

/// The path a corrupt file is moved to: `<path>.corrupt`.
#[must_use]
pub fn corrupt_path_for(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(CORRUPT_EXT);
    PathBuf::from(s)
}

/// Move a corrupt file aside as `<path>.corrupt` so its bytes survive for a
/// post-mortem, and say so at error level with the kind, the reason, and
/// the kind's recovery action, which the caller takes next. Best effort: a
/// failed rename is logged and the caller proceeds with its recovery
/// regardless, because wedging on the bad file would be worse than losing
/// the forensic copy. Returns the path the file now sits at.
pub fn quarantine(kind: Kind, path: &Path, reason: &str) -> PathBuf {
    move_aside(&format!("{kind:?}"), kind.recovery_action(), path, reason)
}

/// What the daemon does after quarantining a document's CRDT sidecar: the
/// one place that action is spelled, as [`Kind::recovery_action`] is for
/// envelope kinds.
const DOCUMENT_RECOVERY_ACTION: &str =
    "drop the engine and await the relay's catch-up, which refills it";

/// [`quarantine`] for a document's CRDT sidecar (`<doc>.dt`), which is not
/// an envelope file and so has no [`Kind`]: the same move-aside, log line,
/// and counter, with the daemon's recovery action.
pub fn quarantine_document(path: &Path, reason: &str) -> PathBuf {
    move_aside("Document", DOCUMENT_RECOVERY_ACTION, path, reason)
}

/// The shared body of [`quarantine`] and [`quarantine_document`]: count,
/// rename to `<path>.corrupt`, and log the outcome with `kind` and `action`.
fn move_aside(kind: &str, action: &str, path: &Path, reason: &str) -> PathBuf {
    QUARANTINED.fetch_add(1, Ordering::Relaxed);
    let dest = corrupt_path_for(path);
    match std::fs::rename(path, &dest) {
        Ok(()) => {
            tracing::error!(
                kind, path = %path.display(), quarantined = %dest.display(),
                reason, action, "corrupt file quarantined"
            );
            dest
        }
        Err(e) => {
            tracing::error!(
                kind, path = %path.display(), error = %e,
                reason, action, "corrupt file could not be quarantined"
            );
            path.to_owned()
        }
    }
}

/// Say so, and count, for a corrupt file that cannot be moved aside: a
/// live writer holds it, so the reader leaves it in place and serves what
/// it could read. Same log line as [`quarantine`], same counter.
pub fn note_corrupt_in_place(kind: Kind, path: &Path, reason: &str) {
    QUARANTINED.fetch_add(1, Ordering::Relaxed);
    tracing::error!(
        kind = ?kind, path = %path.display(), reason,
        action = kind.recovery_action(), "corrupt file left in place"
    );
}

/// [`quarantine`] for a snapshot read through [`read_snapshot`]: the file
/// that failed is the envelope at `path` when it exists, else the
/// pre-envelope file under the kind's old name beside it.
fn quarantine_snapshot(kind: Kind, path: &Path, reason: &str) -> PathBuf {
    let offending = match kind.old_name() {
        Some(old) if !path.exists() => path.with_file_name(old),
        _ => path.to_owned(),
    };
    quarantine(kind, &offending, reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(ms: u64) -> kutl_proto::sync::Hlc {
        kutl_proto::sync::Hlc {
            physical_ms: ms,
            logical: 0,
            actor: Vec::new(),
        }
    }

    /// A legacy decoder for the tests: the bytes are the decimal millis.
    fn millis_legacy(kind: Kind) -> Legacy<kutl_proto::sync::Hlc> {
        Legacy {
            kind,
            expires_at_version: 2,
            decode: |b| {
                std::str::from_utf8(b)
                    .ok()?
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .map(hlc)
            },
        }
    }

    #[test]
    fn test_header_round_trips_and_is_thirty_bytes() {
        let owner = Uuid::from_u128(7);
        let h = encode_header(Kind::State, owner, 42);
        assert_eq!(h.len(), 30);
        assert_eq!(HEADER_LEN, 30);
        let parsed = decode_header(Kind::State, &h).unwrap();
        assert_eq!(
            parsed,
            Header {
                version: 1,
                owner,
                created_at_ms: 42
            }
        );
    }

    #[test]
    fn test_segment_header_bytes_match_the_pre_codec_layout() {
        // The signal segment's header predates this module; its bytes must
        // not move, or every sealed segment on disk becomes unreadable.
        let owner = Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
        let h = encode_header(Kind::Segment, owner, -1);
        let mut expected = Vec::new();
        expected.extend_from_slice(b"KSEG");
        expected.extend_from_slice(&1u16.to_le_bytes());
        expected.extend_from_slice(owner.as_bytes());
        expected.extend_from_slice(&(-1i64).to_le_bytes());
        assert_eq!(h.as_slice(), expected.as_slice());
    }

    #[test]
    fn test_wrong_magic_and_unsupported_version_are_header_errors() {
        let h = encode_header(Kind::State, Uuid::nil(), 0);
        assert!(matches!(
            decode_header(Kind::Changes, &h),
            Err(Error::Header(_))
        ));
        let mut future = h;
        future[VERSION_OFFSET..OWNER_OFFSET].copy_from_slice(&9u16.to_le_bytes());
        assert!(matches!(
            decode_header(Kind::State, &future),
            Err(Error::Header(_))
        ));
        assert!(matches!(
            decode_header(Kind::State, &h[..10]),
            Err(Error::Header(_))
        ));
    }

    #[test]
    fn test_version_window_is_current_and_the_one_before() {
        assert_eq!(oldest_supported(1), 1);
        assert_eq!(oldest_supported(2), 1);
        assert_eq!(oldest_supported(3), 2);
        for kind in Kind::ALL {
            assert!(kind.supports(kind.current_version()));
            assert!(kind.supports(kind.oldest_supported_version()));
            assert!(!kind.supports(kind.current_version() + 1));
        }
        let magics: std::collections::HashSet<_> = Kind::ALL.iter().map(|k| k.magic()).collect();
        assert_eq!(magics.len(), Kind::ALL.len(), "magics are distinct");
    }

    #[test]
    fn test_scan_frames_stops_at_tear_zero_and_bad_crc() {
        let mut run = encode_frame(b"one");
        run.extend_from_slice(&encode_frame(b"two"));
        let (p, len, end) = scan_frames(&run);
        assert_eq!(p, vec![b"one".as_slice(), b"two".as_slice()]);
        assert_eq!(len, run.len());
        assert_eq!(end, ScanEnd::Clean);

        let mut torn = run.clone();
        torn.extend_from_slice(&encode_frame(b"three")[..5]);
        let (p, len, end) = scan_frames(&torn);
        assert_eq!(p.len(), 2);
        assert_eq!(len, run.len());
        assert_eq!(end, ScanEnd::Torn);

        let mut zeros = run.clone();
        zeros.extend_from_slice(&[0u8; 16]);
        let (p, _, end) = scan_frames(&zeros);
        assert_eq!((p.len(), end), (2, ScanEnd::Torn));

        let mut bad = run.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        let (p, len, end) = scan_frames(&bad);
        assert_eq!((p.len(), end), (1, ScanEnd::Torn));
        assert_eq!(len, encode_frame(b"one").len());
        assert!(decode_frames(&bad).is_err());
    }

    #[test]
    fn test_snapshot_round_trip_and_crc_corruption() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("x.ksnap");
        let msg = hlc(5);
        write_snapshot_verified(Kind::State, &path, 1, &msg).unwrap();
        let Loaded::Envelope(back) =
            read_snapshot::<kutl_proto::sync::Hlc>(Kind::State, &path, None)
                .unwrap()
                .unwrap()
        else {
            panic!("expected an envelope")
        };
        assert_eq!(back, msg);

        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            read_snapshot::<kutl_proto::sync::Hlc>(Kind::State, &path, None),
            Err(Error::Frame(_))
        ));

        // A message that encodes to nothing round-trips as one empty frame.
        let empty = kutl_proto::sync::Hlc::default();
        assert!(
            empty.encode_to_vec().is_empty(),
            "premise: an all-default message is zero bytes"
        );
        write_snapshot_verified(Kind::State, &path, 1, &empty).unwrap();
        let Loaded::Envelope(back) =
            read_snapshot::<kutl_proto::sync::Hlc>(Kind::State, &path, None)
                .unwrap()
                .unwrap()
        else {
            panic!("expected an envelope")
        };
        assert_eq!(back, empty);
        assert!(
            read_snapshot::<kutl_proto::sync::Hlc>(Kind::State, &dir.path().join("none"), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_snapshot_reads_legacy_shape_at_old_name_and_same_name() {
        let dir = tempfile::TempDir::new().unwrap();
        // A kind whose move renamed the file: the legacy shape sits beside.
        let legacy = millis_legacy(Kind::State);
        legacy.assert_not_expired();
        std::fs::write(dir.path().join("state.json"), "77").unwrap();
        let path = dir.path().join("state.ksnap");
        let Loaded::Legacy(m) = read_snapshot(Kind::State, &path, Some(&legacy))
            .unwrap()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(m.physical_ms, 77);

        // A kind whose name did not change: the legacy shape at the path.
        let same_name = millis_legacy(Kind::Changes);
        let path = dir.path().join("doc.changes");
        std::fs::write(&path, "0088").unwrap();
        let Loaded::Legacy(m) = read_snapshot(Kind::Changes, &path, Some(&same_name))
            .unwrap()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(m.physical_ms, 88);
        std::fs::write(&path, "not a number").unwrap();
        assert!(read_snapshot(Kind::Changes, &path, Some(&same_name)).is_err());
        // Too short to carry a magic is corruption, never handed to the decoder.
        std::fs::write(&path, "7").unwrap();
        assert!(matches!(
            read_snapshot(Kind::Changes, &path, Some(&same_name)),
            Err(Error::Header(_))
        ));
    }

    /// A corrupt envelope with a decodable legacy file still beside it
    /// serves the legacy copy: the last good state is not thrown away
    /// because its replacement was damaged before the old file was removed.
    #[test]
    fn test_corrupt_envelope_falls_back_to_a_surviving_legacy_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let legacy = millis_legacy(Kind::State);
        let path = dir.path().join("state.ksnap");
        write_snapshot(Kind::State, &path, 0, &hlc(5)).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert!(
            read_snapshot(Kind::State, &path, Some(&legacy)).is_err(),
            "no legacy file: corrupt"
        );
        std::fs::write(dir.path().join("state.json"), "77").unwrap();
        // A wrong magic is the same fault as a bad CRC: the legacy copy serves.
        std::fs::write(dir.path().join("magicless.ksnap"), "junk").unwrap();
        assert!(matches!(
            read_snapshot(
                Kind::State,
                &dir.path().join("magicless.ksnap"),
                Some(&legacy)
            ),
            Ok(Some(Loaded::Legacy(_)))
        ));
        let Loaded::Legacy(m) = read_snapshot(Kind::State, &path, Some(&legacy))
            .unwrap()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(m.physical_ms, 77);
        assert_eq!(
            quarantine_snapshot(Kind::State, &path, "crc"),
            corrupt_path_for(&path)
        );
        assert!(
            !path.exists(),
            "the corrupt envelope, not the legacy file, was moved aside"
        );
    }

    #[test]
    #[should_panic(expected = "expired at version 1")]
    fn test_expired_legacy_decoder_fails_the_tripwire() {
        let expired: Legacy<kutl_proto::sync::Hlc> = Legacy {
            kind: Kind::Changes,
            expires_at_version: 1,
            decode: |_| None,
        };
        expired.assert_not_expired();
    }

    #[test]
    fn test_open_log_stamps_a_header_once_and_read_log_scans_it() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("x.klog");
        let owner = Uuid::from_u128(9);
        let (mut file, len) = open_log(Kind::IdentityLog, &path, owner, 7).unwrap();
        assert_eq!(len, HEADER_LEN as u64, "an empty file gets the header");
        std::io::Write::write_all(&mut file, &encode_frame(b"one")).unwrap();
        drop(file);
        let (_, len) = open_log(Kind::IdentityLog, &path, Uuid::nil(), 0).unwrap();
        assert_eq!(
            len,
            (HEADER_LEN + encode_frame(b"one").len()) as u64,
            "a reopen keeps the file"
        );

        let bytes = std::fs::read(&path).unwrap();
        let (header, payloads, run_len, end) = read_log(Kind::IdentityLog, &bytes).unwrap();
        assert_eq!(header.owner, owner, "the first writer's header stands");
        assert_eq!(payloads, vec![b"one".as_slice()]);
        assert_eq!((run_len, end), (encode_frame(b"one").len(), ScanEnd::Clean));
        assert!(matches!(
            read_log(Kind::State, &bytes),
            Err(Error::Header(_))
        ));

        // A torn tail: read_log reports the valid prefix, truncate_torn cuts it.
        let mut torn = bytes.clone();
        torn.extend_from_slice(&encode_frame(b"two")[..5]);
        std::fs::write(&path, &torn).unwrap();
        let (_, _, run_len, end) = read_log(Kind::IdentityLog, &torn).unwrap();
        assert_eq!(end, ScanEnd::Torn);
        truncate_torn(&path, run_len).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn test_load_or_recover_quarantines_only_when_acting() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("x.ksnap");
        std::fs::write(&path, b"junk").unwrap();
        let before = quarantine_count();
        let loaded =
            load_or_recover::<kutl_proto::sync::Hlc>(Kind::State, &path, None, Recovery::Skip)
                .unwrap();
        assert!(
            loaded.is_none() && path.exists(),
            "skip: absent to the reader, file untouched"
        );
        let loaded =
            load_or_recover::<kutl_proto::sync::Hlc>(Kind::State, &path, None, Recovery::Act)
                .unwrap();
        assert!(loaded.is_none() && !path.exists() && corrupt_path_for(&path).exists());
        // The counters are process-global and other tests in this binary
        // quarantine and migrate concurrently, so only the direction is
        // asserted, never an exact delta.
        assert!(quarantine_count() > before);
        // Retiring a legacy file is a no-op for a kind that never renamed, and
        // removes the old-name file for one that did.
        retire_legacy(Kind::Changes, dir.path(), &path);
        std::fs::write(dir.path().join("state.json"), b"{}").unwrap();
        let migrated = migration_count();
        retire_legacy(Kind::State, dir.path(), &path);
        assert!(!dir.path().join("state.json").exists());
        assert!(migration_count() > migrated);
    }

    #[test]
    fn test_quarantine_moves_the_file_aside_and_counts() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("x.ksnap");
        std::fs::write(&path, b"junk").unwrap();
        let before = quarantine_count();
        let dest = quarantine(Kind::State, &path, "no envelope magic");
        assert!(!path.exists());
        assert_eq!(std::fs::read(dest).unwrap(), b"junk");
        // Direction only: the counter is process-global (see the sibling test).
        assert!(quarantine_count() > before);
        // The legacy old-name file is the offender when no envelope exists.
        std::fs::write(dir.path().join("state.json"), b"junk").unwrap();
        let dest = quarantine_snapshot(Kind::State, &path, "not the legacy shape");
        assert_eq!(dest, corrupt_path_for(&dir.path().join("state.json")));
        let before = migration_count();
        note_migration(Kind::State, &dir.path().join("state.json"), &path);
        assert!(migration_count() > before);
    }
}
