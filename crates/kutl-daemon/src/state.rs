//! Local daemon state persistence: the snapshot `.kutl/state.ksnap` and the
//! identity journal `.kutl/identity.klog` replayed over it.
//!
//! Caches path→document mappings so the daemon can restart quickly without
//! re-querying the relay registry for every known file.
//!
//! Each entry carries a monotone `confirmed` flag — "has the relay acknowledged
//! this document" — which lives *with* the document rather than in a parallel
//! collection. That is the input the startup reconciler's `was_remote` axis
//! reads (see [`crate::reconcile`]): a doc gone from the relay but still on disk
//! is a remote *deletion* to propagate (`confirmed`) versus a never-synced local
//! file to push up (not `confirmed`). Keeping the flag on the record makes it
//! impossible for the two to drift — the bug a separate `remote_document_ids`
//! snapshot caused, where documents learned mid-session were never recorded as
//! confirmed and so were wrongly re-registered on the next start.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use kutl_core::envelope::{self, Kind, Loaded, Recovery};
use kutl_core::{ActorId, Hlc, HlcClock, Uuid};
use kutl_proto::daemon::{
    DaemonState as DaemonStateProto, DocEntry as DocEntryProto, HlcFloor as HlcFloorProto,
    IdentityRecord, IdentityRemoval, IdentitySnapshot, RegisterHlc as RegisterHlcProto,
    WriteIntent, identity_record,
};
use prost::Message;
use tracing::warn;

use crate::safe_path::SafeRelayPath;

/// A tracked document: its UUID plus whether the relay has acknowledged it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocEntry {
    /// Document UUID in the relay registry.
    pub id: String,
    /// Whether the relay has confirmed this document exists. Monotone: once the
    /// relay has acknowledged a document (we received its registration or it
    /// appeared in the space's document list) it stays confirmed for the
    /// document's lifetime; only unregistration removes the entry entirely.
    pub confirmed: bool,
    /// Last-known inode of the file at this path. Persisted so a file renamed
    /// while the daemon was *offline* can still be located by inode on restart:
    /// the recorded path is gone, so its inode can no longer be read from disk,
    /// yet the moved file carries the same inode (a rename preserves it). `None`
    /// on platforms without inodes.
    pub inode: Option<u64>,
    /// Hex SHA-256 of the last bytes this daemon's write funnel put in the
    /// file (see `FileIdentity::last_written_hash` for the recovery rule it
    /// feeds). `None` before the funnel's first write.
    pub last_written_hash: Option<String>,
}

/// Persisted hybrid-logical-clock floor: this device's last-emitted stamp's
/// `(physical_ms, logical)`. The `actor` is the device id (stored separately,
/// not duplicated here). Seeds the clock on restart so it never emits a stamp at
/// or below one already emitted before the restart (monotonic restart).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct HlcFloor {
    /// Physical component of the last-emitted stamp. Declaration order matters:
    /// `derive(Ord)` compares `physical_ms` then `logical`.
    pub physical_ms: u64,
    /// Logical component of the last-emitted stamp.
    pub logical: u32,
}

/// Persisted REGISTER stamp for one document: the registration HLC this daemon
/// observed (its own mint, or the first remote register it saw). Unlike
/// [`HlcFloor`] it carries the full stamp including the REGISTRANT's actor —
/// not necessarily this device — because it round-trips into lattice
/// comparisons ([`crate::core::SpaceState::register_hlc`]) where the actor is
/// the deterministic tiebreaker. The actor is stored as a UUID string (the
/// `device_id` precedent); a corrupt entry is skipped on load, degrading to
/// floor-absent — never to a wrong floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterHlc {
    /// Physical component of the registration stamp.
    pub physical_ms: u64,
    /// Logical component of the registration stamp.
    pub logical: u32,
    /// The registrant's device actor (UUID string).
    pub actor: String,
}

impl From<Hlc> for HlcFloor {
    fn from(hlc: Hlc) -> Self {
        Self {
            physical_ms: hlc.physical_ms,
            logical: hlc.logical,
        }
    }
}

impl From<Hlc> for RegisterHlc {
    fn from(hlc: Hlc) -> Self {
        Self {
            physical_ms: hlc.physical_ms,
            logical: hlc.logical,
            actor: hlc.actor.0.to_string(),
        }
    }
}

impl RegisterHlc {
    /// Convert back to a lattice [`Hlc`]; `None` when the stored actor string is
    /// not a UUID (a corrupt entry — skipped, see the type docs).
    pub fn to_hlc(&self) -> Option<Hlc> {
        let actor = Uuid::parse_str(&self.actor).ok()?;
        Some(Hlc {
            physical_ms: self.physical_ms,
            logical: self.logical,
            actor: ActorId(actor),
        })
    }
}

/// Local cache of path → document mappings for fast daemon restarts.
/// `Clone` is the sim's persisted-snapshot mirror (`DaemonSim::restart`).
#[derive(Debug, Default, Clone)]
pub struct DaemonState {
    /// Maps relative file paths to their document entry. A snapshot carrying
    /// only `device_id` (the e2e harness's device pin, written before first
    /// start) is a valid snapshot with no documents.
    pub documents: HashMap<String, DocEntry>,
    /// Per-install device id (a UUID string), the HLC actor for stamps this
    /// daemon originates. Generated once on first use; distinct per
    /// installation, NOT the account DID — so two devices of one user cannot
    /// collide on identical HLC stamps. `None` until generated.
    pub device_id: Option<String>,
    /// The HLC floor — this device's last-emitted stamp. `None` until the first
    /// stamp.
    pub hlc_floor: Option<HlcFloor>,
    /// Per-document REGISTER stamp (document id → the registration HLC this
    /// daemon observed) — the causal-floor source for a LATER rename of the doc.
    /// Persisted because an OFFLINE rename is re-emitted at the NEXT startup
    /// (`reconcile_offline_renames`) stamped at the pre-offline floor, so the
    /// recorded register is its only causal proof over the registration; without
    /// it the relay's `path_priority` drops the rename as a stale
    /// pre-registration echo (the offline-ingest rejoin bug). Synced from the
    /// live map on every persist; pruned on unregister.
    pub register_hlc: HashMap<String, RegisterHlc>,
    /// Write intents the funnel journaled before renaming a materialization
    /// into place, keyed by path: the hex SHA-256 of the bytes it was about
    /// to place. Journal replay carries them here; a later snapshot or
    /// removal line for the path retires each one. The startup scan consumes
    /// the map (a file whose bytes match its intent is this daemon's own
    /// interrupted write, never a user edit) and clears it when it is done; a
    /// copy that reached the snapshot before that survives until the next
    /// save and is re-checked, inertly, on the next start.
    pub pending_writes: HashMap<String, String>,
    /// Document IDS of text documents detected at
    /// [`kutl_core::MAX_OPS_PER_DOC`]: their edits no longer merge anywhere,
    /// so replicas of these documents are diverging. Surfaced by `kutl
    /// status` (which resolves id → path at read time via `documents`).
    /// Keyed by id, not path: the flag marks the document's history, ids
    /// survive renames for free, and the detection sites see different path
    /// domains for displaced docs. Recorded/cleared by the core's
    /// `note_op_cap_status` (cleared when a later edit or merge succeeds
    /// below the cap, e.g. after a cap raise); pruned on unregister.
    /// `BTreeSet` for a stable persisted order.
    pub at_op_cap: std::collections::BTreeSet<String>,
    /// Document IDS of text documents at or above
    /// [`kutl_core::OP_CAP_WARN_THRESHOLD`] but still under the cap — the
    /// early warning `kutl status` surfaces while edits still sync, so the
    /// owner can split or compact the document before it freezes. Same
    /// conventions as [`Self::at_op_cap`] (id-keyed, single owner
    /// `note_op_cap_status`, pruned on unregister); the two sets are
    /// disjoint — reaching the cap moves a document from this set to that
    /// one.
    pub approaching_op_cap: std::collections::BTreeSet<String>,
}

/// Name of the state snapshot within the `.kutl` directory: an envelope of
/// kind [`Kind::State`] holding one `kutl.daemon.v1.DaemonState` record.
const STATE_FILE: &str = "state.ksnap";

/// The identity journal beside it — an envelope record log of kind
/// [`Kind::IdentityLog`], appended per identity mutation, replayed on load,
/// removed by every full save (see [`IdentityJournalLine`]).
const IDENTITY_JOURNAL_FILE: &str = "identity.klog";

/// One identity-journal record for a single path, mirroring the on-disk
/// `IdentityRecord` kind for kind (see [`JournalLineKind`]).
///
/// The snapshot is written COALESCED (once per drained burst, rate-limited),
/// so a crash between saves would forget identities the daemon already acted
/// on — a restart then re-registers its own files and arbitration mints
/// conflict copies. The journal closes that window at O(1) per event: the
/// shell appends one line per identity mutation as it happens, and
/// [`DaemonState::load`] replays the journal over the loaded state (last line
/// per path wins; the floor max-merges). Every successful full save truncates
/// it, so the journal only ever holds the tail since the last save.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityJournalLine {
    /// The path this line is about (canonical forward-slash form).
    pub path: String,
    /// What happened at `path`.
    pub kind: JournalLineKind,
    /// The clock floor at append time; max-merged at replay so a restart
    /// never re-emits a stamp at or below one already journaled. `None` on a
    /// write intent, which is not a clock event.
    pub hlc_floor: Option<HlcFloor>,
}

/// The record kinds the journal holds, one variant per on-disk `oneof` arm,
/// so a line's meaning is its variant and never inferred from which field
/// happens to be set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalLineKind {
    /// The entry now at the path, with the registration stamp for its
    /// document at append time (the causal floor an offline rename must
    /// carry).
    Snapshot {
        entry: DocEntry,
        register_hlc: Option<RegisterHlc>,
    },
    /// The path's entry is gone.
    Removal,
    /// The funnel is about to place bytes with this hex SHA-256 at the path.
    /// Written on its own line ahead of the rename, so a restart can tell
    /// those bytes from a user's edit even when the sidecar never followed.
    WriteIntent { hash: String },
}

impl IdentityJournalLine {
    /// The entry a snapshot line carries; `None` for a removal or an intent.
    #[must_use]
    pub fn entry(&self) -> Option<&DocEntry> {
        match &self.kind {
            JournalLineKind::Snapshot { entry, .. } => Some(entry),
            JournalLineKind::Removal | JournalLineKind::WriteIntent { .. } => None,
        }
    }

    /// Whether this line removes the path's entry.
    #[must_use]
    pub fn is_removal(&self) -> bool {
        matches!(self.kind, JournalLineKind::Removal)
    }

    /// The hash a write-intent line announces; `None` for the other kinds.
    #[must_use]
    pub fn pending_write(&self) -> Option<&str> {
        match &self.kind {
            JournalLineKind::WriteIntent { hash } => Some(hash),
            JournalLineKind::Snapshot { .. } | JournalLineKind::Removal => None,
        }
    }

    /// The on-disk record for this line.
    fn to_record(&self) -> IdentityRecord {
        let kind = match &self.kind {
            JournalLineKind::WriteIntent { hash } => {
                identity_record::Kind::WriteIntent(WriteIntent { hash: hash.clone() })
            }
            JournalLineKind::Snapshot {
                entry,
                register_hlc,
            } => identity_record::Kind::Snapshot(IdentitySnapshot {
                entry: Some(entry.to_proto()),
                register_hlc: register_hlc.as_ref().map(RegisterHlc::to_proto),
            }),
            JournalLineKind::Removal => identity_record::Kind::Removal(IdentityRemoval {}),
        };
        IdentityRecord {
            path: self.path.clone(),
            kind: Some(kind),
            hlc_floor: self.hlc_floor.map(HlcFloor::to_proto),
        }
    }

    /// The line for an on-disk record, or `None` for a record kind this
    /// build does not know: replay stops there rather than guessing.
    fn from_record(record: IdentityRecord) -> Option<Self> {
        let path = record.path;
        let hlc_floor = record.hlc_floor.map(HlcFloor::from_proto);
        let kind = match record.kind? {
            identity_record::Kind::WriteIntent(w) => JournalLineKind::WriteIntent { hash: w.hash },
            identity_record::Kind::Snapshot(snap) => JournalLineKind::Snapshot {
                entry: DocEntry::from_proto(snap.entry?),
                register_hlc: snap.register_hlc.map(RegisterHlc::from_proto),
            },
            identity_record::Kind::Removal(_) => JournalLineKind::Removal,
        };
        Some(Self {
            path,
            kind,
            hlc_floor,
        })
    }
}

/// Frame `lines` as the bytes an append adds to the identity journal: one
/// CRC-framed `IdentityRecord` per line, in order.
pub(crate) fn encode_journal_records(lines: &[IdentityJournalLine]) -> Vec<u8> {
    let mut out = Vec::new();
    for line in lines {
        out.extend_from_slice(&envelope::encode_frame(&line.to_record().encode_to_vec()));
    }
    out
}

/// Decode a journal file back into its lines, for tests that inspect what an
/// append put on disk.
#[cfg(test)]
pub(crate) fn read_journal_records(kutl_dir: &Path) -> Vec<IdentityJournalLine> {
    let bytes = std::fs::read(identity_journal_path(kutl_dir)).expect("journal exists");
    envelope::decode_header(Kind::IdentityLog, &bytes).expect("journal header");
    let (payloads, _, _) = envelope::scan_frames(&bytes[envelope::HEADER_LEN..]);
    payloads
        .into_iter()
        .map(|p| {
            IdentityJournalLine::from_record(IdentityRecord::decode(p).expect("record decodes"))
                .expect("known record kind")
        })
        .collect()
}

/// The header a fresh identity journal opens with, for tests that build a
/// journal by hand; the appender stamps it through `envelope::open_log`.
#[cfg(test)]
pub(crate) fn journal_header(owner: Uuid, created_at_ms: i64) -> [u8; envelope::HEADER_LEN] {
    envelope::encode_header(Kind::IdentityLog, owner, created_at_ms)
}

impl DocEntry {
    fn to_proto(&self) -> DocEntryProto {
        DocEntryProto {
            id: self.id.clone(),
            confirmed: self.confirmed,
            inode: self.inode,
            last_written_hash: self.last_written_hash.clone(),
        }
    }

    fn from_proto(p: DocEntryProto) -> Self {
        Self {
            id: p.id,
            confirmed: p.confirmed,
            inode: p.inode,
            last_written_hash: p.last_written_hash,
        }
    }
}

impl HlcFloor {
    fn to_proto(self) -> HlcFloorProto {
        HlcFloorProto {
            physical_ms: self.physical_ms,
            logical: self.logical,
        }
    }

    fn from_proto(p: HlcFloorProto) -> Self {
        Self {
            physical_ms: p.physical_ms,
            logical: p.logical,
        }
    }
}

impl RegisterHlc {
    fn to_proto(&self) -> RegisterHlcProto {
        RegisterHlcProto {
            physical_ms: self.physical_ms,
            logical: self.logical,
            actor: self.actor.clone(),
        }
    }

    fn from_proto(p: RegisterHlcProto) -> Self {
        Self {
            physical_ms: p.physical_ms,
            logical: p.logical,
            actor: p.actor,
        }
    }
}

/// The identity journal's location inside a `.kutl` directory.
pub fn identity_journal_path(kutl_dir: &Path) -> PathBuf {
    kutl_dir.join(IDENTITY_JOURNAL_FILE)
}

impl DaemonState {
    /// Load state from the `.kutl` directory, then replay the identity
    /// journal over it (see [`IdentityJournalLine`]). Returns an empty state
    /// if the snapshot is missing; a snapshot that is neither the envelope
    /// nor a pre-envelope shape is quarantined and the state starts empty,
    /// to be rebuilt from the relay's document list and the files on disk. A
    /// journal with a torn tail is truncated to its valid prefix, so later
    /// appends land where replay reads. A pre-envelope `state.json` loads
    /// through its legacy decoder and is replaced by the next save.
    ///
    /// For the daemon, which owns the files. A reader beside a running
    /// daemon uses [`Self::load_readonly`].
    pub fn load(kutl_dir: &Path) -> Self {
        Self::load_with(kutl_dir, Recovery::Act)
    }

    /// [`Self::load`] without the recovery actions: a corrupt snapshot reads
    /// as empty and a torn or corrupt journal stops replay, with every file
    /// left in place for the daemon to recover. For the CLI, which reads a
    /// space's state beside the daemon that owns it.
    pub fn load_readonly(kutl_dir: &Path) -> Self {
        Self::load_with(kutl_dir, Recovery::Skip)
    }

    fn load_with(kutl_dir: &Path, recovery: Recovery) -> Self {
        let mut state = Self::load_state_file(kutl_dir, recovery);
        state.replay_identity_journal(kutl_dir, recovery);
        state
    }

    fn load_state_file(kutl_dir: &Path, recovery: Recovery) -> Self {
        let path = kutl_dir.join(STATE_FILE);
        match envelope::load_or_recover::<DaemonStateProto>(
            Kind::State,
            &path,
            Some(&crate::legacy::STATE_V0),
            recovery,
        ) {
            Ok(None) => Self::empty(),
            Ok(Some(Loaded::Envelope(proto) | Loaded::Legacy(proto))) => Self::from_proto(proto),
            Err(e) => {
                warn!(error = %e, path = %path.display(), "could not read the state snapshot; starting fresh");
                Self::empty()
            }
        }
    }

    /// Fold the identity journal's records into this state, in file order:
    /// first a pre-envelope `identity.log` left by the previous build, then
    /// the envelope log. Replay stops at a torn frame (a crash mid-append
    /// loses only the record being written), and at a record whose kind
    /// this build does not know, rather than reading it as something else.
    fn replay_identity_journal(&mut self, kutl_dir: &Path, recovery: Recovery) {
        let mut replayed = 0usize;
        if let Some(old_name) = Kind::IdentityLog.old_name()
            && let Ok(bytes) = std::fs::read(kutl_dir.join(old_name))
        {
            for record in (crate::legacy::IDENTITY_LOG_V0.decode)(&bytes).unwrap_or_default() {
                match IdentityJournalLine::from_record(record) {
                    Some(line) => {
                        self.apply_journal_line(line);
                        replayed += 1;
                    }
                    None => break,
                }
            }
        }
        let path = identity_journal_path(kutl_dir);
        let bytes = std::fs::read(&path).unwrap_or_default();
        if !bytes.is_empty() {
            match envelope::read_log(Kind::IdentityLog, &bytes) {
                Err(e) => match recovery {
                    Recovery::Act => {
                        envelope::quarantine(Kind::IdentityLog, &path, &e.to_string());
                    }
                    Recovery::Skip => warn!(
                        error = %e,
                        path = %path.display(),
                        "corrupt identity journal; this read-only load skips replay and leaves recovery to the daemon"
                    ),
                },
                Ok((_, payloads, valid_run_len, end)) => {
                    for payload in payloads {
                        let Ok(record) = IdentityRecord::decode(payload) else {
                            tracing::error!(path = %path.display(), "identity journal record does not decode; replay stops here");
                            break;
                        };
                        let Some(line) = IdentityJournalLine::from_record(record) else {
                            tracing::error!(path = %path.display(), "identity journal record of an unknown kind; replay stops here");
                            break;
                        };
                        self.apply_journal_line(line);
                        replayed += 1;
                    }
                    if end == envelope::ScanEnd::Torn {
                        Self::truncate_torn_journal(&path, valid_run_len, recovery);
                    }
                }
            }
        }
        if replayed > 0 {
            warn!(
                replayed,
                "replayed identity journal over persisted state (unclean shutdown)"
            );
        }
    }

    /// Cut a torn tail off the journal so the next append lands where
    /// replay reads: a record appended behind a tear would never replay.
    fn truncate_torn_journal(path: &Path, valid_run_len: usize, recovery: Recovery) {
        match recovery {
            Recovery::Act => match envelope::truncate_torn(path, valid_run_len) {
                Ok(()) => warn!(
                    path = %path.display(),
                    valid_len = envelope::HEADER_LEN + valid_run_len,
                    "identity journal had a torn tail; truncated to its valid prefix"
                ),
                Err(e) => tracing::error!(
                    path = %path.display(),
                    error = %e,
                    "identity journal has a torn tail that could not be truncated; records appended behind it will not replay"
                ),
            },
            Recovery::Skip => warn!(
                path = %path.display(),
                "identity journal has a torn tail; replay stops here"
            ),
        }
    }

    fn to_proto(&self) -> DaemonStateProto {
        DaemonStateProto {
            documents: self
                .documents
                .iter()
                .map(|(p, e)| (p.clone(), e.to_proto()))
                .collect(),
            device_id: self.device_id.clone(),
            hlc_floor: self.hlc_floor.map(HlcFloor::to_proto),
            register_hlc: self
                .register_hlc
                .iter()
                .map(|(id, r)| (id.clone(), r.to_proto()))
                .collect(),
            pending_writes: self.pending_writes.clone(),
            at_op_cap: self.at_op_cap.iter().cloned().collect(),
            approaching_op_cap: self.approaching_op_cap.iter().cloned().collect(),
        }
    }

    fn from_proto(p: DaemonStateProto) -> Self {
        Self {
            documents: p
                .documents
                .into_iter()
                .map(|(path, e)| (path, DocEntry::from_proto(e)))
                .collect(),
            device_id: p.device_id,
            hlc_floor: p.hlc_floor.map(HlcFloor::from_proto),
            register_hlc: p
                .register_hlc
                .into_iter()
                .map(|(id, r)| (id, RegisterHlc::from_proto(r)))
                .collect(),
            pending_writes: p.pending_writes,
            at_op_cap: p.at_op_cap.into_iter().collect(),
            approaching_op_cap: p.approaching_op_cap.into_iter().collect(),
        }
    }

    /// Apply one journal line: set or remove the path's entry, record the
    /// document's register stamp, and max-merge the clock floor. Public
    /// because the sim's modeled restart replays its modeled journal through
    /// THIS fold — the same single-implementation rule as `sync_persisted`.
    pub fn apply_journal_line(&mut self, line: IdentityJournalLine) {
        let IdentityJournalLine {
            path,
            kind,
            hlc_floor,
        } = line;
        match kind {
            // An intent is not a clock event: it returns before the floor
            // merge, so a floor set on one is never applied.
            JournalLineKind::WriteIntent { hash } => {
                self.pending_writes.insert(path, hash);
                return;
            }
            // A snapshot or removal for the path supersedes any intent for
            // it: the write it announced either completed (the snapshot
            // carries the landed hash) or the document is gone.
            JournalLineKind::Snapshot {
                entry,
                register_hlc,
            } => {
                self.pending_writes.remove(&path);
                if let Some(reg) = register_hlc {
                    self.register_hlc.insert(entry.id.clone(), reg);
                }
                self.documents.insert(path, entry);
            }
            JournalLineKind::Removal => {
                self.pending_writes.remove(&path);
                // A removal retires the register stamp with the entry, as
                // the live `unregister_identity` does; a stamp that outlived
                // its document would seed a causal floor for an id that no
                // longer exists here.
                // ... but only once no path carries the id: a rename
                // journals a removal for the old path and an insert for the
                // new one under the SAME id, and the renamed document's
                // causal floor must survive.
                if let Some(gone) = self.documents.remove(&path)
                    && !self.documents.values().any(|e| e.id == gone.id)
                {
                    self.register_hlc.remove(&gone.id);
                }
            }
        }
        if let Some(floor) = hlc_floor
            && self.hlc_floor.is_none_or(|cur| floor > cur)
        {
            self.hlc_floor = Some(floor);
        }
    }

    /// Persist state to the `.kutl` directory.
    ///
    /// Replaces the file through [`kutl_core::fs::write_atomic`], the one
    /// atomic-replace rule, so a crash mid-save leaves the previous complete
    /// snapshot. Creates the directory if it doesn't exist. A successful save truncates
    /// the identity journal: the full snapshot subsumes every journaled line,
    /// and the WRITE-then-truncate order means a crash between the two merely
    /// replays already-current lines (idempotent — last line per path wins).
    pub fn save(&self, kutl_dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(kutl_dir)?;
        let path = kutl_dir.join(STATE_FILE);
        envelope::write_snapshot_verified(
            Kind::State,
            &path,
            kutl_core::env::now_ms(),
            &self.to_proto(),
        )
        .map_err(|e| match e {
            envelope::Error::Io(source) => source,
            other => std::io::Error::other(other),
        })?;
        // The envelope is in place and read back: a pre-envelope snapshot or
        // journal left by the previous build has been read once (on load)
        // and is now superseded, so it goes, and the file never has two
        // sources.
        envelope::retire_legacy(Kind::State, kutl_dir, &path);
        envelope::retire_legacy(
            Kind::IdentityLog,
            kutl_dir,
            &identity_journal_path(kutl_dir),
        );
        let journal = identity_journal_path(kutl_dir);
        if journal.exists()
            && let Err(e) = std::fs::remove_file(&journal)
        {
            // A journal that outlives the save is not merely stale bytes: its
            // lines replay OVER this snapshot on the next load (last line per
            // path wins), so a pre-save removal line would delete an entry the
            // snapshot legitimately holds. Empty the file in place before
            // accepting that risk; only if both fail do we surface the error.
            if let Err(e2) = std::fs::write(&journal, "") {
                warn!(
                    remove_error = %e, write_error = %e2,
                    "failed to truncate the identity journal after a full save; stale lines would replay"
                );
                // The snapshot landed but the journal it subsumes did not
                // retire: report the save as failed so the caller keeps its
                // pending deltas and retries the whole save, truncation
                // included, rather than trusting a snapshot stale lines will
                // overwrite on the next load.
                return Err(e2);
            }
            warn!(error = %e, "identity journal removal failed; emptied in place instead");
        }
        Ok(())
    }

    /// Project the persisted document map into the live identity maps a
    /// (re)start seeds — `file_identity` (keyed by validated path) and the
    /// uuid → path identity map. The ONE implementation behind the daemon's
    /// startup load and the sim's modeled restart.
    ///
    /// `fallback_inode` supplies a live-disk inode probe (given the ABSOLUTE
    /// path) for entries persisted without one — legacy state files; the sim
    /// passes a no-op, its model has no real inodes. The persisted inode is
    /// preferred: a file renamed while the daemon was offline left its
    /// recorded path empty, so the inode can no longer be read from disk
    /// there, yet the persisted value still identifies the moved file.
    #[must_use]
    pub fn identity_maps(
        &self,
        space_root: &Path,
        fallback_inode: impl Fn(&Path) -> Option<u64>,
    ) -> (
        HashMap<PathBuf, crate::core::FileIdentity>,
        HashMap<String, PathBuf>,
    ) {
        let mut file_identity = HashMap::new();
        let mut uuid_to_path = HashMap::new();
        for (path_str, uuid) in &self.validated_documents() {
            let rel_path = PathBuf::from(path_str);
            let inode = self
                .documents
                .get(path_str)
                .and_then(|e| e.inode)
                .or_else(|| fallback_inode(&space_root.join(&rel_path)));
            file_identity.insert(
                rel_path.clone(),
                crate::core::FileIdentity {
                    document_uuid: uuid.clone(),
                    inode,
                    last_written_hash: self
                        .documents
                        .get(path_str)
                        .and_then(|e| e.last_written_hash.clone()),
                },
            );
            uuid_to_path.insert(uuid.clone(), rel_path);
        }
        (file_identity, uuid_to_path)
    }

    /// Return `path → UUID` for documents whose paths pass `SafeRelayPath`
    /// validation. Malicious or corrupt paths (traversal, absolute,
    /// `.kutl`-prefixed) are logged and skipped.
    pub fn validated_documents(&self) -> HashMap<String, String> {
        self.documents
            .iter()
            .filter(|(path, _)| match SafeRelayPath::new(path) {
                Ok(_) => true,
                Err(e) => {
                    warn!(path, "skipping invalid path in the state snapshot: {e}");
                    false
                }
            })
            .map(|(k, v)| (k.clone(), v.id.clone()))
            .collect()
    }

    /// UUIDs the relay has acknowledged — the reconciler's `was_remote` set.
    pub fn confirmed_ids(&self) -> HashSet<String> {
        self.documents
            .values()
            .filter(|e| e.confirmed)
            .map(|e| e.id.clone())
            .collect()
    }

    /// Insert or update the entry for `path`. `confirmed` joins monotonically
    /// with any existing value — a confirmed document never reverts.
    pub fn set(&mut self, path: String, id: String, confirmed: bool) {
        let existing = self.documents.get(&path);
        let confirmed = confirmed || existing.is_some_and(|e| e.confirmed);
        // Carry any known inode and written hash forward; the live values are
        // re-synced from `file_identity` on every persist (see daemon
        // `save_state`) and on every journal drain.
        let inode = existing.and_then(|e| e.inode);
        let last_written_hash = existing.and_then(|e| e.last_written_hash.clone());
        self.documents.insert(
            path,
            DocEntry {
                id,
                confirmed,
                inode,
                last_written_hash,
            },
        );
    }

    /// Record the inode for the document at `path`, if an entry exists. The
    /// daemon syncs live inodes here before each persist so a file renamed while
    /// the daemon is later offline remains locatable by inode across the restart.
    pub fn set_inode(&mut self, path: &str, inode: Option<u64>) {
        if let Some(entry) = self.documents.get_mut(path) {
            entry.inode = inode;
        }
    }

    /// Record the funnel's last-written content hash for the document at
    /// `path`, if an entry exists — synced from `file_identity` on every
    /// persist, like the inode.
    pub fn set_written_hash(&mut self, path: &str, hash: Option<String>) {
        if let Some(entry) = self.documents.get_mut(path) {
            entry.last_written_hash = hash;
        }
    }

    /// Mark the document at `path` as confirmed by the relay (monotone join).
    /// Returns true only on an actual `false → true` transition, so callers can
    /// skip a redundant persist.
    pub fn confirm(&mut self, path: &str) -> bool {
        match self.documents.get_mut(path) {
            Some(entry) if !entry.confirmed => {
                entry.confirmed = true;
                true
            }
            _ => false,
        }
    }

    /// The per-install device actor for HLC stamping, generated and stored on
    /// first use (and on a corrupt/unparseable stored id). The caller persists
    /// the state after first use so the id is stable across restarts.
    pub fn ensure_device_actor(&mut self) -> ActorId {
        let id = self
            .device_id
            .as_deref()
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or_else(|| {
                let fresh = Uuid::new_v4();
                self.device_id = Some(fresh.to_string());
                fresh
            });
        ActorId(id)
    }

    /// The origin HLC clock a (re)start seeds: the device actor (generated on
    /// first use, see [`Self::ensure_device_actor`]) restored over the
    /// persisted floor, so the clock never emits a stamp at or below one an
    /// earlier process already emitted. The ONE implementation behind the
    /// daemon's constructor and the sim's modeled restart.
    pub fn restore_clock(&mut self) -> HlcClock {
        let actor = self.ensure_device_actor();
        match self.hlc_floor {
            Some(floor) => HlcClock::restore(
                actor,
                Hlc {
                    physical_ms: floor.physical_ms,
                    logical: floor.logical,
                    actor,
                },
            ),
            None => HlcClock::new(actor),
        }
    }

    /// The persisted per-document REGISTER stamps as live [`Hlc`]s — the
    /// causal floor a lifecycle op re-emitted after a restart must carry to
    /// supersede the registration. A corrupt entry is skipped: floor-absent
    /// degrades to a lost rename, never to a wrong floor. The ONE projection
    /// behind the daemon's constructor and the sim's modeled restart.
    #[must_use]
    pub fn register_hlc_map(&self) -> HashMap<String, Hlc> {
        self.register_hlc
            .iter()
            .filter_map(|(id, reg)| reg.to_hlc().map(|hlc| (id.clone(), hlc)))
            .collect()
    }

    /// Record an emitted HLC as the new floor. Monotone — only ever advances,
    /// so a redundant or out-of-order record cannot regress the floor.
    pub fn record_emitted_hlc(&mut self, hlc: Hlc) {
        let next = HlcFloor::from(hlc);
        if self.hlc_floor.is_none_or(|cur| next > cur) {
            self.hlc_floor = Some(next);
        }
    }

    /// Create an empty state.
    pub fn empty() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, confirmed: bool) -> DocEntry {
        DocEntry {
            id: id.to_owned(),
            confirmed,
            inode: None,
            last_written_hash: None,
        }
    }

    /// A write intent replays into `pending_writes`, is neither an entry nor
    /// a removal, and is retired by the snapshot or removal that follows the
    /// write it announced.
    #[test]
    fn test_identity_journal_write_intent_replays_and_retires() {
        let dir = tempfile::tempdir().unwrap();
        DaemonState::empty().save(dir.path()).unwrap();
        let intent = IdentityJournalLine {
            path: "doc.md".into(),
            kind: JournalLineKind::WriteIntent { hash: "abc".into() },
            hlc_floor: None,
        };
        assert!(
            matches!(
                intent.to_record().kind,
                Some(identity_record::Kind::WriteIntent(_))
            ),
            "an intent is its own record kind, never a removal"
        );
        write_journal(dir.path(), std::slice::from_ref(&intent));
        let loaded = DaemonState::load(dir.path());
        assert_eq!(
            loaded.pending_writes.get("doc.md").map(String::as_str),
            Some("abc"),
            "an intent record lands in pending_writes"
        );
        assert!(
            !loaded.documents.contains_key("doc.md"),
            "an intent is not an entry and not a removal"
        );

        // The snapshot after a completed write retires the intent.
        write_journal(
            dir.path(),
            &[
                intent.clone(),
                IdentityJournalLine {
                    path: "doc.md".into(),
                    kind: JournalLineKind::Snapshot {
                        entry: entry("uuid-doc", true),
                        register_hlc: None,
                    },
                    hlc_floor: None,
                },
            ],
        );
        let loaded = DaemonState::load(dir.path());
        assert!(
            loaded.pending_writes.is_empty(),
            "a snapshot for the path retires its intent"
        );
        assert!(loaded.documents.contains_key("doc.md"));

        // So does a removal.
        write_journal(
            dir.path(),
            &[
                intent,
                IdentityJournalLine {
                    path: "doc.md".into(),
                    kind: JournalLineKind::Removal,
                    hlc_floor: None,
                },
            ],
        );
        let loaded = DaemonState::load(dir.path());
        assert!(
            loaded.pending_writes.is_empty(),
            "a removal for the path retires its intent"
        );
    }

    /// A record whose kind this build does not know stops replay there,
    /// keeping the records before it: it is never read as something else.
    #[test]
    fn test_identity_journal_unknown_record_kind_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        DaemonState::empty().save(dir.path()).unwrap();
        let good = IdentityJournalLine {
            path: "good.md".into(),
            kind: JournalLineKind::Snapshot {
                entry: entry("uuid-good", true),
                register_hlc: None,
            },
            hlc_floor: None,
        };
        let later = IdentityJournalLine {
            path: "later.md".into(),
            kind: JournalLineKind::Snapshot {
                entry: entry("uuid-later", true),
                register_hlc: None,
            },
            hlc_floor: None,
        };
        let unknown = IdentityRecord {
            path: "mystery.md".into(),
            kind: None,
            hlc_floor: None,
        };
        let mut bytes = journal_header(Uuid::nil(), 0).to_vec();
        bytes.extend_from_slice(&encode_journal_records(std::slice::from_ref(&good)));
        bytes.extend_from_slice(&envelope::encode_frame(&unknown.encode_to_vec()));
        bytes.extend_from_slice(&encode_journal_records(std::slice::from_ref(&later)));
        std::fs::write(identity_journal_path(dir.path()), bytes).unwrap();

        let loaded = DaemonState::load(dir.path());
        assert!(
            loaded.documents.contains_key("good.md"),
            "records before it replay"
        );
        assert!(!loaded.documents.contains_key("mystery.md"));
        assert!(
            !loaded.documents.contains_key("later.md"),
            "replay stops at the unknown kind"
        );
    }

    /// Write journal `lines` into `kutl_dir`'s identity journal as the
    /// driver's appender would: a header, then one frame per line.
    fn write_journal(kutl_dir: &Path, lines: &[IdentityJournalLine]) {
        let mut bytes = journal_header(Uuid::nil(), 0).to_vec();
        bytes.extend_from_slice(&encode_journal_records(lines));
        std::fs::write(identity_journal_path(kutl_dir), bytes).unwrap();
    }

    /// A crash between coalesced saves leaves the journal holding the tail:
    /// load must fold it over the persisted snapshot — inserts, removals,
    /// register stamps, and the max-merged clock floor alike.
    #[test]
    fn test_identity_journal_replays_over_persisted_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut base = DaemonState::empty();
        base.set("kept.md".into(), "uuid-kept".into(), true);
        base.set("moved.md".into(), "uuid-moved".into(), true);
        base.hlc_floor = Some(HlcFloor {
            physical_ms: 5,
            logical: 0,
        });
        base.save(dir.path()).unwrap();

        write_journal(
            dir.path(),
            &[
                // moved.md → renamed.md (removal + insert), one fresh register.
                IdentityJournalLine {
                    path: "moved.md".into(),
                    kind: JournalLineKind::Removal,
                    hlc_floor: Some(HlcFloor {
                        physical_ms: 9,
                        logical: 1,
                    }),
                },
                IdentityJournalLine {
                    path: "renamed.md".into(),
                    kind: JournalLineKind::Snapshot {
                        entry: DocEntry {
                            id: "uuid-moved".into(),
                            confirmed: true,
                            inode: Some(42),
                            last_written_hash: None,
                        },
                        register_hlc: Some(RegisterHlc {
                            physical_ms: 3,
                            logical: 0,
                            actor: Uuid::nil().to_string(),
                        }),
                    },
                    hlc_floor: Some(HlcFloor {
                        physical_ms: 9,
                        logical: 1,
                    }),
                },
                IdentityJournalLine {
                    path: "fresh.md".into(),
                    kind: JournalLineKind::Snapshot {
                        entry: entry("uuid-fresh", false),
                        register_hlc: None,
                    },
                    hlc_floor: None,
                },
            ],
        );

        let loaded = DaemonState::load(dir.path());
        assert!(loaded.documents.contains_key("kept.md"), "base entry kept");
        assert!(
            !loaded.documents.contains_key("moved.md"),
            "journaled removal applied"
        );
        let renamed = loaded
            .documents
            .get("renamed.md")
            .expect("journaled insert");
        assert_eq!(renamed.id, "uuid-moved");
        assert_eq!(
            renamed.inode,
            Some(42),
            "journaled inode survives the crash"
        );
        assert!(
            loaded.documents.contains_key("fresh.md"),
            "unsaved registration survives the crash"
        );
        assert_eq!(
            loaded.register_hlc.get("uuid-moved").map(|r| r.physical_ms),
            Some(3),
            "journaled register stamp restored"
        );
        assert_eq!(
            loaded.hlc_floor,
            Some(HlcFloor {
                physical_ms: 9,
                logical: 1
            }),
            "clock floor max-merged from the journal"
        );
    }

    /// A full save subsumes the journal and truncates it: a line the journal
    /// held for a path the snapshot does NOT carry must not replay after the
    /// save (the snapshot is the whole truth once it lands).
    #[test]
    fn test_identity_journal_truncated_by_save() {
        let dir = tempfile::tempdir().unwrap();
        write_journal(
            dir.path(),
            &[IdentityJournalLine {
                path: "stale.md".into(),
                kind: JournalLineKind::Snapshot {
                    entry: entry("uuid-stale", false),
                    register_hlc: None,
                },
                hlc_floor: None,
            }],
        );
        let mut state = DaemonState::empty();
        state.set("a.md".into(), "uuid-a".into(), false);
        state.save(dir.path()).unwrap();
        assert!(
            !identity_journal_path(dir.path()).exists(),
            "save truncates the journal"
        );
        let loaded = DaemonState::load(dir.path());
        assert!(
            loaded.documents.contains_key("a.md"),
            "the snapshot is loaded"
        );
        assert!(
            !loaded.documents.contains_key("stale.md"),
            "a journal line the snapshot subsumed does not replay after the save"
        );
    }

    /// A crash mid-append tears the journal's last record; replay keeps
    /// every record before the tear and loses only the torn one.
    #[test]
    fn test_identity_journal_torn_tail_keeps_prior_lines() {
        let dir = tempfile::tempdir().unwrap();
        DaemonState::empty().save(dir.path()).unwrap();
        let good = IdentityJournalLine {
            path: "good.md".into(),
            kind: JournalLineKind::Snapshot {
                entry: entry("uuid-good", true),
                register_hlc: None,
            },
            hlc_floor: None,
        };
        let torn = IdentityJournalLine {
            path: "torn.md".into(),
            kind: JournalLineKind::Snapshot {
                entry: entry("uuid-torn", true),
                register_hlc: None,
            },
            hlc_floor: None,
        };
        let mut bytes = journal_header(Uuid::nil(), 0).to_vec();
        bytes.extend_from_slice(&encode_journal_records(std::slice::from_ref(&good)));
        let torn_frame = encode_journal_records(std::slice::from_ref(&torn));
        bytes.extend_from_slice(&torn_frame[..torn_frame.len() / 2]);
        std::fs::write(identity_journal_path(dir.path()), bytes).unwrap();

        let loaded = DaemonState::load(dir.path());
        assert!(
            loaded.documents.contains_key("good.md"),
            "lines before the tear replay"
        );
        assert_eq!(loaded.documents.len(), 1, "the torn line is dropped");
    }

    #[test]
    fn test_validated_documents_filters_unsafe_paths() {
        let state = DaemonState {
            documents: HashMap::from([
                ("notes/readme.md".to_owned(), entry("uuid-1", true)),
                ("../../../etc/passwd".to_owned(), entry("uuid-2", true)),
                ("/absolute/path.md".to_owned(), entry("uuid-3", false)),
                (".kutl/state.json".to_owned(), entry("uuid-4", false)),
            ]),
            ..Default::default()
        };

        let safe = state.validated_documents();
        assert_eq!(safe.len(), 1);
        assert_eq!(safe.get("notes/readme.md").unwrap(), "uuid-1");
    }

    #[test]
    fn test_save_and_load_round_trip_preserves_confirmed() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");

        let mut state = DaemonState::empty();
        state.set("notes/ideas.md".into(), "uuid-1".into(), true);
        state.set("draft.md".into(), "uuid-2".into(), false);
        state.save(&kutl_dir).unwrap();

        let loaded = DaemonState::load(&kutl_dir);
        assert_eq!(loaded.documents.len(), 2);
        assert!(loaded.documents.get("notes/ideas.md").unwrap().confirmed);
        assert!(!loaded.documents.get("draft.md").unwrap().confirmed);
        assert_eq!(
            loaded.confirmed_ids(),
            HashSet::from(["uuid-1".to_owned()]),
            "only the relay-confirmed doc is in the was_remote set"
        );
    }

    #[test]
    fn test_inode_persists_across_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");

        let mut state = DaemonState::empty();
        state.set("doc.md".into(), "uuid-1".into(), true);
        // The daemon syncs live inodes via set_inode before each persist.
        state.set_inode("doc.md", Some(4242));
        state.save(&kutl_dir).unwrap();

        // A renamed-away file's inode must survive the restart — it is the only
        // way to locate the moved file once its recorded path is gone.
        let loaded = DaemonState::load(&kutl_dir);
        assert_eq!(loaded.documents.get("doc.md").unwrap().inode, Some(4242));

        // set() preserves a known inode rather than clobbering it to None.
        let mut reloaded = loaded;
        reloaded.set("doc.md".into(), "uuid-1".into(), true);
        assert_eq!(
            reloaded.documents.get("doc.md").unwrap().inode,
            Some(4242),
            "set must carry the existing inode forward"
        );
    }

    #[test]
    fn test_confirm_is_monotone() {
        let mut state = DaemonState::empty();
        state.set("a.md".into(), "uuid-1".into(), true);
        // A later set with confirmed=false must NOT revert the confirmed flag.
        state.set("a.md".into(), "uuid-1".into(), false);
        assert!(
            state.documents.get("a.md").unwrap().confirmed,
            "confirmed must not revert to false"
        );
        // confirm() reports only an actual transition: already-confirmed and
        // missing both return false.
        assert!(!state.confirm("a.md"));
        assert!(!state.confirm("missing.md"));
        state.set("b.md".into(), "uuid-2".into(), false);
        assert!(state.confirm("b.md"), "first confirmation is a transition");
        assert!(!state.confirm("b.md"), "second confirmation is a no-op");
    }

    #[test]
    fn test_load_missing_returns_empty() {
        let state = DaemonState::load(Path::new("/nonexistent/.kutl"));
        assert!(state.documents.is_empty());
    }

    #[test]
    fn test_load_legacy_without_remote_ids_field() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        // Oldest format: documents only, no remote_document_ids.
        std::fs::write(
            kutl_dir.join("state.json"),
            r#"{"documents":{"doc.md":"uuid-1"}}"#,
        )
        .unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert_eq!(state.documents.len(), 1);
        assert!(
            !state.documents.get("doc.md").unwrap().confirmed,
            "no remote_document_ids → unconfirmed (will re-confirm on next sync)"
        );
    }

    /// A pre-seeded state file carrying ONLY `device_id` (the e2e harness's
    /// device pin, written before the daemon's first start) must parse as the
    /// current format and keep the pinned id — not fall through to the corrupt
    /// branch and silently re-randomize the HLC tiebreaker.
    #[test]
    fn test_load_device_pin_only_state_keeps_device_id() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        std::fs::write(
            kutl_dir.join("state.json"),
            r#"{ "device_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" }"#,
        )
        .unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert_eq!(
            state.device_id.as_deref(),
            Some("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            "the pinned device id must survive the load"
        );
        assert!(state.documents.is_empty());
    }

    #[test]
    fn test_load_corrupted_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        std::fs::write(kutl_dir.join(STATE_FILE), "not an envelope").unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert!(state.documents.is_empty());
        assert!(
            !kutl_dir.join(STATE_FILE).exists(),
            "the corrupt snapshot was moved aside"
        );
        assert!(envelope::corrupt_path_for(&kutl_dir.join(STATE_FILE)).exists());
    }

    /// A snapshot whose header is intact but whose payload is damaged fails
    /// its CRC: that is corruption to quarantine, never a decode.
    #[test]
    fn test_load_crc_corruption_is_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        let mut state = DaemonState::empty();
        state.set("doc.md".into(), "uuid-1".into(), true);
        state.save(&kutl_dir).unwrap();
        let path = kutl_dir.join(STATE_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();
        assert!(
            envelope::has_magic(Kind::State, &bytes),
            "premise: the header is intact, only the payload is damaged"
        );

        let loaded = DaemonState::load(&kutl_dir);
        assert!(loaded.documents.is_empty(), "a CRC mismatch loads nothing");
        assert!(!path.exists(), "the corrupt snapshot was moved aside");
        assert!(envelope::corrupt_path_for(&path).exists());
    }

    /// A read-only load beside a running daemon never takes a recovery
    /// action: the corrupt file stays for the owner to quarantine.
    #[test]
    fn test_load_readonly_leaves_a_corrupt_snapshot_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        let path = kutl_dir.join(STATE_FILE);
        std::fs::write(&path, "not an envelope").unwrap();

        let state = DaemonState::load_readonly(&kutl_dir);
        assert!(state.documents.is_empty());
        assert!(path.exists(), "nothing was moved");
        assert!(!envelope::corrupt_path_for(&path).exists());
    }

    /// The pre-envelope files the last release left — `state.json` beside
    /// `identity.log` — load through the public loader with the journal
    /// replayed over the snapshot, and the first save replaces both with the
    /// envelope and removes them.
    #[test]
    fn test_legacy_fixtures_load_replay_and_migrate_through_the_public_loader() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        std::fs::write(
            kutl_dir.join("state.json"),
            include_bytes!("../legacy-fixtures/v0/state.json"),
        )
        .unwrap();
        std::fs::write(
            kutl_dir.join("identity.log"),
            include_bytes!("../legacy-fixtures/v0/identity.log"),
        )
        .unwrap();
        let doc_id = "0f0f0f0f-0000-4000-8000-000000000001";
        let device = "0f0f0f0f-0000-4000-8000-0000000000dd";

        let loaded = DaemonState::load(&kutl_dir);
        let entry = loaded
            .documents
            .get("doc.md")
            .expect("the snapshot's document");
        assert_eq!(entry.id, doc_id);
        assert!(entry.confirmed);
        assert_eq!(entry.inode, Some(4242));
        assert_eq!(loaded.device_id.as_deref(), Some(device));
        assert_eq!(
            loaded.at_op_cap.iter().collect::<Vec<_>>(),
            vec![&doc_id.to_owned()]
        );
        // The journal's write intent for doc.md was retired by the snapshot
        // line that followed it; the snapshot's own pending write stands.
        assert_eq!(
            loaded.pending_writes.keys().collect::<Vec<_>>(),
            vec!["pending.md"]
        );
        // The journal's register stamp replaces the snapshot's.
        let reg = loaded.register_hlc.get(doc_id).expect("register stamp");
        assert_eq!(
            (reg.physical_ms, reg.logical, reg.actor.as_str()),
            (1_700_000_000_000, 1, device)
        );
        // The clock floor max-merged through both journal lines.
        assert_eq!(
            loaded.hlc_floor,
            Some(HlcFloor {
                physical_ms: 1_700_000_000_002,
                logical: 0
            })
        );
        assert!(
            !loaded.documents.contains_key("gone.md"),
            "the removal line never resurrects a path"
        );

        loaded.save(&kutl_dir).unwrap();
        assert!(!kutl_dir.join("state.json").exists(), "state.json retired");
        assert!(
            !kutl_dir.join("identity.log").exists(),
            "identity.log retired"
        );
        let bytes = std::fs::read(kutl_dir.join(STATE_FILE)).unwrap();
        assert!(
            envelope::has_magic(Kind::State, &bytes),
            "the envelope stands"
        );
        let again = DaemonState::load(&kutl_dir);
        assert_eq!(again.documents, loaded.documents);
        assert_eq!(again.hlc_floor, loaded.hlc_floor);
        assert_eq!(again.register_hlc, loaded.register_hlc);
        assert_eq!(again.pending_writes, loaded.pending_writes);
    }

    /// A torn tail is cut off at load, so a record appended afterwards sits
    /// where replay reads rather than behind the tear.
    #[test]
    fn test_identity_journal_torn_tail_is_truncated_so_later_appends_replay() {
        let dir = tempfile::tempdir().unwrap();
        DaemonState::empty().save(dir.path()).unwrap();
        let good = IdentityJournalLine {
            path: "good.md".into(),
            kind: JournalLineKind::Snapshot {
                entry: entry("uuid-good", true),
                register_hlc: None,
            },
            hlc_floor: None,
        };
        let torn = IdentityJournalLine {
            path: "torn.md".into(),
            kind: JournalLineKind::Snapshot {
                entry: entry("uuid-torn", true),
                register_hlc: None,
            },
            hlc_floor: None,
        };
        let good_frame = encode_journal_records(std::slice::from_ref(&good));
        let mut bytes = journal_header(Uuid::nil(), 0).to_vec();
        bytes.extend_from_slice(&good_frame);
        let torn_frame = encode_journal_records(std::slice::from_ref(&torn));
        bytes.extend_from_slice(&torn_frame[..torn_frame.len() / 2]);
        let path = identity_journal_path(dir.path());
        std::fs::write(&path, bytes).unwrap();

        let _ = DaemonState::load(dir.path());
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            (envelope::HEADER_LEN + good_frame.len()) as u64,
            "truncated to the valid prefix"
        );
        let later = IdentityJournalLine {
            path: "later.md".into(),
            kind: JournalLineKind::Snapshot {
                entry: entry("uuid-later", true),
                register_hlc: None,
            },
            hlc_floor: None,
        };
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(
            &mut f,
            &encode_journal_records(std::slice::from_ref(&later)),
        )
        .unwrap();
        let loaded = DaemonState::load(dir.path());
        assert!(loaded.documents.contains_key("good.md"));
        assert!(
            loaded.documents.contains_key("later.md"),
            "the append after the cut replays"
        );
        assert_eq!(loaded.documents.len(), 2);

        // A read-only load leaves the tear alone.
        std::fs::write(&path, {
            let mut b = journal_header(Uuid::nil(), 0).to_vec();
            b.extend_from_slice(&good_frame);
            b.extend_from_slice(&torn_frame[..torn_frame.len() / 2]);
            b
        })
        .unwrap();
        let before = std::fs::metadata(&path).unwrap().len();
        let ro = DaemonState::load_readonly(dir.path());
        assert!(ro.documents.contains_key("good.md"));
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            before,
            "nothing truncated"
        );
    }

    fn hlc(physical_ms: u64, logical: u32) -> Hlc {
        Hlc {
            physical_ms,
            logical,
            actor: ActorId(Uuid::nil()),
        }
    }

    #[test]
    fn test_device_actor_generated_and_stable_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");

        let mut state = DaemonState::empty();
        assert!(state.device_id.is_none());
        let actor1 = state.ensure_device_actor();
        assert!(
            state.device_id.is_some(),
            "first use generates and stores it"
        );
        // Idempotent within a session.
        assert_eq!(actor1, state.ensure_device_actor());
        state.save(&kutl_dir).unwrap();

        // Survives a restart unchanged.
        let mut reloaded = DaemonState::load(&kutl_dir);
        assert_eq!(
            reloaded.ensure_device_actor(),
            actor1,
            "device actor is stable across restarts"
        );
    }

    #[test]
    fn test_device_actor_regenerated_when_corrupt() {
        let mut state = DaemonState {
            device_id: Some("not-a-uuid".to_owned()),
            ..Default::default()
        };
        let actor = state.ensure_device_actor();
        assert_ne!(actor.0, Uuid::nil());
        assert_eq!(
            state.device_id.as_deref(),
            Some(actor.0.to_string().as_str()),
            "a corrupt stored id is replaced with a fresh valid one"
        );
    }

    #[test]
    fn test_hlc_floor_is_monotone() {
        let mut state = DaemonState::empty();
        assert!(state.hlc_floor.is_none());
        state.record_emitted_hlc(hlc(100, 0));
        assert_eq!(
            state.hlc_floor,
            Some(HlcFloor {
                physical_ms: 100,
                logical: 0
            })
        );
        // Advances on a later stamp.
        state.record_emitted_hlc(hlc(100, 5));
        assert_eq!(
            state.hlc_floor,
            Some(HlcFloor {
                physical_ms: 100,
                logical: 5
            })
        );
        // Never regresses on an out-of-order/older stamp.
        state.record_emitted_hlc(hlc(50, 9));
        assert_eq!(
            state.hlc_floor,
            Some(HlcFloor {
                physical_ms: 100,
                logical: 5
            }),
            "the floor only advances"
        );
        state.record_emitted_hlc(hlc(101, 0));
        assert_eq!(
            state.hlc_floor,
            Some(HlcFloor {
                physical_ms: 101,
                logical: 0
            })
        );
    }

    /// The per-doc register stamp must round-trip save/load with its FOREIGN
    /// actor intact (the registrant's device, not ours) — it is an offline
    /// rename's only causal proof over the registration after a restart.
    #[test]
    fn test_register_hlc_round_trips_with_foreign_actor() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");

        let foreign = Hlc {
            physical_ms: 1_234,
            logical: 7,
            actor: ActorId(Uuid::from_u128(42)),
        };
        let mut state = DaemonState::empty();
        state
            .register_hlc
            .insert("uuid-1".to_owned(), RegisterHlc::from(foreign));
        state.save(&kutl_dir).unwrap();

        let loaded = DaemonState::load(&kutl_dir);
        assert_eq!(
            loaded
                .register_hlc
                .get("uuid-1")
                .and_then(RegisterHlc::to_hlc),
            Some(foreign),
            "the register stamp round-trips with the registrant's actor"
        );

        // A corrupt actor string degrades to floor-absent, never a wrong floor.
        let corrupt = RegisterHlc {
            physical_ms: 1,
            logical: 0,
            actor: "not-a-uuid".to_owned(),
        };
        assert_eq!(corrupt.to_hlc(), None);
    }

    #[test]
    fn test_legacy_state_loads_with_no_device_id_or_floor() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        std::fs::write(
            kutl_dir.join("state.json"),
            r#"{"documents":{"doc.md":{"id":"uuid-1","confirmed":true}}}"#,
        )
        .unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert_eq!(state.documents.len(), 1);
        assert!(state.device_id.is_none(), "legacy file → no device id yet");
        assert!(state.hlc_floor.is_none(), "legacy file → no floor yet");
    }
}
