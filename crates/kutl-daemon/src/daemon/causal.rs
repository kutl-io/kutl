//! HLC stamping, causal floors, and persisted clock state for [`SpaceWorker`]:
//! metadata minting (fresh and explicit-stamp),
//! the pre-offline floor and the offline-delete stamp, the per-doc
//! register/lifecycle HLC watermarks, the local-rename causal floor,
//! `fold_local_rename_record`, which mirrors a locally-emitted rename into the
//! placement lattice, and the state-snapshot persistence funnel (`save_state` /
//! the coalesced caught-up flush) plus the `KUTL_CLOCK_SKEW_MS` test seam.

use std::path::Path;
use std::time::Duration;

use tokio::time::Instant;

use kutl_core::Hlc;
use kutl_proto::sync::ChangeMetadata;
use tokio::sync::mpsc;
use tracing::error;

use crate::client::SyncEvent;
use crate::core::rel_path_to_string;
use crate::watcher::FileEvent;

use super::SpaceWorker;

/// Minimum spacing between coalesced state-snapshot writes
/// ([`SpaceWorker::flush_state_if_caught_up`]).
///
/// `save_state` rewrites the whole file — O(docs) — so its cost must be
/// bounded per unit time, not per event. One second keeps a large space's
/// rewrite to ~1% duty cycle under a sustained flood while a lone change
/// (last save long past) still persists in the iteration that made it.
pub(super) const MIN_STATE_SAVE_INTERVAL: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(1));

/// Journal lines appended since the last successful snapshot before the drain
/// forces a compacting save (by marking state dirty for the flush lane).
///
/// Bounds `.kutl/identity.klog` growth in edit-only sessions, where nothing
/// else ever dirties state: the budget was sized for JSON lines at ~400
/// bytes each (~1.6 MB), and an envelope record is smaller, so it bounds
/// the journal and the crash-replay work well under that. Low enough to
/// bound waste, high enough that it rarely fires first during a bulk burst,
/// which dirties state itself. A materialization costs two records (the
/// funnel's write intent, then the post-write snapshot), and the forced save
/// past the budget is the same coalesced flush the burst is already waiting
/// on.
///
/// Public so the simulation's modeled journal drain applies the same budget
/// rather than re-pinning the number.
pub const JOURNAL_LINES_FORCE_SAVE: u32 = 4096;

impl SpaceWorker {
    /// Current wall-clock millis with this daemon's test skew applied. The single
    /// physical-time source for HLC stamping, so an injected skew shifts every
    /// stamp consistently. Saturates rather than wrapping at the u64 bounds.
    pub(super) fn skewed_now_ms_u64(&self) -> u64 {
        kutl_core::now_ms_u64().saturating_add_signed(self.state.clock_skew_ms)
    }

    /// Build a `ChangeMetadata` with the daemon's author DID and a fresh origin
    /// HLC stamp. The stamp orders this op causally in the lifecycle lattice;
    /// `timestamp` (millis) is kept for display/mirror and equals `physical_ms`.
    pub(super) fn make_metadata(&mut self, intent: &str) -> ChangeMetadata {
        let wall = self.skewed_now_ms_u64();
        let stamp = self.state.hlc.tick(wall);
        ChangeMetadata {
            // Mirror the skewed physical time the stamp was taken at, so the
            // display/mirror `timestamp` stays equal to the HLC `physical_ms`.
            timestamp: kutl_core::ms_u64_to_i64_saturating(wall),
            author_did: self.config.author_did.clone(),
            intent: intent.into(),
            hlc: Some(stamp.into()),
            ..Default::default()
        }
    }

    /// Metadata stamped with an explicit (non-fresh) HLC. Used for a lifecycle
    /// op whose true origin time precedes "now" — notably an offline rename
    /// re-emitted on rejoin: stamping it with the daemon's pre-offline clock
    /// floor (rather than a fresh `now`) makes it lose to any cluster op that
    /// happened after the daemon went offline (no last-to-rejoin clobber) — a
    /// stale offline op carries its old time, so it loses to recent cluster
    /// ops.
    fn make_metadata_with_hlc(&self, intent: &str, stamp: Hlc) -> ChangeMetadata {
        ChangeMetadata {
            timestamp: kutl_core::ms_u64_to_i64_saturating(stamp.physical_ms),
            author_did: self.config.author_did.clone(),
            intent: intent.into(),
            hlc: Some(stamp.into()),
            ..Default::default()
        }
    }

    /// This daemon's pre-offline clock floor: the high-water mark of stamps it
    /// emitted before the current session. Meaningful only during startup
    /// reconciliation — before the event loop processes any remote op (which
    /// would tick the clock past it). A lifecycle op re-emitted on rejoin (an
    /// offline rename or an offline delete) is stamped with this floor so it
    /// loses to any cluster op that happened after the daemon went offline (no
    /// last-to-rejoin clobber): the stale offline op carries its
    /// old time and cannot beat a causally-later concurrent edit/rename.
    pub(super) fn offline_floor(&self) -> Hlc {
        self.state.hlc.last()
    }

    /// HLC for an offline (startup-detected) delete of `document_id`.
    ///
    /// A delete only supersedes a document when its HLC dominates the document's
    /// liveness in the relay lattice (`DocRecord::is_alive`). The binding term is
    /// the content touch: the relay raises `touched_hlc` to
    /// `Hlc::physical_touch(content_ms)` = `{content_ms, logical: u32::MAX}`, where
    /// `content_ms` is the latest content-edit timestamp — and nothing at the same
    /// `physical_ms` outranks `u32::MAX`. So stamp the delete one millisecond above
    /// the latest content timestamp *in our own CRDT*: it dominates every edit we
    /// have OBSERVED (a genuine self-delete wins), yet still loses to a peer edit
    /// we have NOT observed — whose timestamp is newer and is absent from our CRDT
    /// — so that concurrent edit revives the document. Reading our own CRDT
    /// (not the relay's current state) is what excludes the unobserved peer edit.
    ///
    /// A document with no content edit has no content touch; fall back to the
    /// pre-offline floor, which covers its registration stamp.
    pub(super) fn offline_delete_stamp(&self, document_id: &str) -> Hlc {
        /// One millisecond: the smallest step that lifts the delete above a content
        /// touch at the same physical millisecond (whose `logical` is `u32::MAX`
        /// and so cannot be beaten at an equal `physical_ms`).
        const OVER_CONTENT_TOUCH_MS: u64 = 1;

        let floor = self.offline_floor();
        match self.state.content_touch(document_id) {
            Some(touch) => Hlc {
                physical_ms: touch.physical_ms.saturating_add(OVER_CONTENT_TOUCH_MS),
                logical: 0,
                actor: floor.actor,
            },
            None => floor,
        }
    }

    /// Record the HLC of a lifecycle op this daemon just produced locally, so a
    /// stale remote echo of a now-superseded op for the same document is later
    /// dropped by [`SpaceState::lifecycle_event_is_fresh`].
    fn note_local_lifecycle_hlc(&mut self, document_id: &str, meta: &ChangeMetadata) {
        if let Some(hlc) = meta.hlc.clone().and_then(|w| Hlc::try_from(w).ok()) {
            self.state.record_lifecycle_hlc(document_id, hlc);
        }
    }

    /// The causal floor to attach to a LOCAL rename of `document_id`: the
    /// `registered_hlc` this daemon had observed for the doc. The relay's lattice
    /// uses it so a rename of a clock-skewed registrant's doc supersedes the
    /// future-stamped registration even when this (un-skewed) daemon's own rename
    /// HLC falls below it — the renamer demonstrably observed the register before
    /// renaming, which the floor records (see `DocRecord::path_priority`).
    ///
    /// The source is the per-doc REGISTER HLC (`register_hlc`), populated for
    /// every doc this daemon created or saw registered, and PERSISTED across
    /// restarts (the state snapshot): an OFFLINE rename is re-emitted at the next
    /// startup stamped at the pre-offline floor — `{0,0}` for a pure observer
    /// that never emitted a stamp — so the recorded register is its only
    /// causal proof over the registration (a `None` floor there loses to the
    /// registration at the relay and the rename is dropped cluster-wide). It
    /// MUST be the register specifically — NOT the lifecycle watermark, which
    /// also absorbs renames: when two daemons A and B both rename one doc
    /// under clock skew, if B's watermark had folded A's concurrently-observed
    /// rename, B's floor would reach A's rename HLC and TIE it, and the
    /// lexicographic-path tiebreak would flip the skew winner run-to-run
    /// (non-deterministic). Under gamma the placement lattice's
    /// `registered_hlc` is an equivalent in-session source (kept as a
    /// fallback). `None` only if the register was never recorded, which
    /// degrades to the original lost-rename behavior — never to a wrong-high
    /// floor.
    pub(super) fn rename_causal_floor(&self, document_id: &str) -> Option<Hlc> {
        if let Some(reg) = self.state.register_hlc.get(document_id).copied() {
            return Some(reg);
        }
        if let Ok(id) = uuid::Uuid::parse_str(document_id)
            && let Some(reg) = self
                .state
                .known_records
                .get(&id)
                .and_then(|r| r.registered_hlc)
        {
            return Some(reg);
        }
        None
    }

    /// Fold a LOCAL rename this daemon detected via live inodes into the
    /// placement lattice (`known_records`), mirroring what the pure core's
    /// `handle_file_renamed` (`core/handle/local_events.rs`) does.
    ///
    /// The imperative local-rename detectors (`try_local_inode_rename`,
    /// `handle_overwrite_rename`) run at the driver edge because they read live
    /// inodes the pure core cannot. They update `file_identity` and the shadow and
    /// emit the `RenameDocument` to the relay — but they DO NOT touch `known_records`.
    /// Under gamma `known_records` is the placement authority: a stale record (still
    /// at the pre-rename path) makes `reconcile_placement` derive a `GuardedPlace`
    /// that moves the file BACK to its old path (undoing the user's rename) and
    /// thrashes against the relay's eventual broadcast. Folding the rename here keeps
    /// the lattice consistent with the disk the detector just reconciled, so the next
    /// `reconcile_placement` sees the doc already placed and emits nothing.
    pub(super) fn fold_local_rename_record(
        &mut self,
        document_id: &str,
        new_rel: &Path,
        meta: &ChangeMetadata,
        rename_causal_floor: Option<Hlc>,
    ) {
        let Ok(id) = uuid::Uuid::parse_str(document_id) else {
            return;
        };
        let renamed_hlc = meta.hlc.clone().and_then(|w| Hlc::try_from(w).ok());
        // A rename fragment: merge takes the max per HLC field (preserving the
        // existing registration) and the rename's path wins (`path_priority`,
        // rename-beats-register). The causal floor lets the rename supersede
        // even a future-stamped (skewed) register.
        self.state
            .known_records
            .observe(kutl_core::lattice::DocRecord::rename(
                id,
                rel_path_to_string(new_rel),
                renamed_hlc,
                rename_causal_floor,
            ));
    }

    /// Build metadata for a LOCAL lifecycle op (rename/delete/displace) and record
    /// its HLC as this document's watermark. Stamping a lifecycle op and recording
    /// its watermark always go together — the watermark is what drops the stale
    /// echo the op would otherwise re-apply — so they live in one call that can't
    /// be half-used.
    pub(super) fn make_lifecycle_metadata(
        &mut self,
        document_id: &str,
        intent: &str,
    ) -> ChangeMetadata {
        let meta = self.make_metadata(intent);
        self.note_local_lifecycle_hlc(document_id, &meta);
        meta
    }

    /// As [`Self::make_lifecycle_metadata`] but with an explicit `stamp` — the
    /// offline-floor cases (a rename/delete that happened while offline must carry
    /// a pre-offline floor so it loses to a concurrent online op).
    pub(super) fn make_lifecycle_metadata_with_hlc(
        &mut self,
        document_id: &str,
        intent: &str,
        stamp: Hlc,
    ) -> ChangeMetadata {
        let meta = self.make_metadata_with_hlc(intent, stamp);
        self.note_local_lifecycle_hlc(document_id, &meta);
        meta
    }

    /// Append every pending identity change to `.kutl/identity.klog` — the
    /// O(1)-per-event durability sibling of the coalesced state snapshot save.
    ///
    /// The coalesced save left a crash window: a SIGKILL between saves forgot
    /// registrations and moves the daemon had already acted on, and the
    /// restart re-registered its own files into conflict copies. Each pending
    /// path is snapshotted from CURRENT in-memory state (identity from
    /// `file_identity`, the monotone confirmed flag from the persisted entry,
    /// the document's register stamp, the live clock floor) so replay is a
    /// last-line-wins fold, idempotent under re-append. Append failure keeps
    /// the paths pending — the next drain or full save retries — and is
    /// logged loudly: an unwritable journal means crash recovery is degraded
    /// to the coalesced save's window.
    pub(super) fn drain_identity_journal(&mut self) {
        if self.state.journal_pending.is_empty() {
            return;
        }
        // The batch, its records and their order, comes from the shared
        // builder (`SpaceState::journal_batch`); the sim's modeled drain
        // appends the SAME batch, so the two persist contracts cannot drift.
        // This side owns only the framing and the append.
        let batch = self.state.journal_batch();
        let count = u32::try_from(batch.len()).unwrap_or(u32::MAX);
        let frames = crate::state::encode_journal_records(&batch);
        if self.append_identity_journal(&frames, count) {
            self.state.journal_pending.clear();
        }
    }

    /// Append `frames` (whole envelope frames) to the identity journal and
    /// charge them to the force-save budget; returns whether they landed. A
    /// journal that does not exist yet, or is empty, opens with its header
    /// first. Refuses while the tail is torn: after a failed append the tail
    /// may hold a torn frame, and good frames behind it are unreachable on
    /// replay. A full save (which removes the journal) is the only safe way
    /// forward, so a failure forces one. Callers keep their own pending
    /// evidence in memory until it lands.
    fn append_identity_journal(&mut self, frames: &[u8], count: u32) -> bool {
        if self.journal_tail == super::JournalTail::Torn {
            return false;
        }
        let journal = crate::state::identity_journal_path(&self.kutl_dir());
        let owner = uuid::Uuid::parse_str(&self.config.space_id).unwrap_or_default();
        let created_at_ms = kutl_core::ms_u64_to_i64_saturating(self.skewed_now_ms_u64());
        let appended = kutl_core::envelope::open_log(
            kutl_core::envelope::Kind::IdentityLog,
            &journal,
            owner,
            created_at_ms,
        )
        .and_then(|(mut f, _)| std::io::Write::write_all(&mut f, frames));
        match appended {
            Ok(()) => {
                self.journal_lines_since_save = self.journal_lines_since_save.saturating_add(count);
                // Growth bound: pure content edits refresh inodes without ever
                // dirtying state, so an edit-only session would append forever
                // (truncation lives in `save_state`). Past the threshold, mark
                // state dirty so the flush lane compacts the journal into one
                // snapshot at its usual drained-intake + interval cadence.
                if self.journal_lines_since_save > JOURNAL_LINES_FORCE_SAVE {
                    self.state_dirty = true;
                }
                true
            }
            Err(e) => {
                error!(
                    error = %e,
                    "failed to append the identity journal; forcing a full save"
                );
                self.journal_tail = super::JournalTail::Torn;
                self.state_dirty = true;
                false
            }
        }
    }

    /// Journal a write intent for `rel`: the funnel is about to rename bytes
    /// with this hex SHA-256 into place. Written immediately, as its own
    /// record, so a kill between the rename and the sidecar save still leaves
    /// a restart the evidence to tell its own materialization from a user
    /// edit. Fails open: without the record a restart falls back to the
    /// sidecar and the last-written hash.
    pub(super) fn journal_pending_write(&mut self, rel: &std::path::Path, hash: &str) {
        let line = crate::state::IdentityJournalLine {
            path: crate::core::rel_path_to_string(rel),
            kind: crate::state::JournalLineKind::WriteIntent {
                hash: hash.to_owned(),
            },
            hlc_floor: None,
        };
        let frames = crate::state::encode_journal_records(std::slice::from_ref(&line));
        self.append_identity_journal(&frames, 1);
    }

    /// Publish `rel`'s identity snapshot to the journal now rather than at the
    /// loop tail: after the write funnel places bytes, so a kill before the
    /// tail cannot leave a materialized file whose persisted entry has no
    /// inode (the startup truth table would read its later absence as never
    /// materialized), and where a write intent already on disk must be
    /// retired before a kill could pair it with the file. Idempotent with the
    /// drain (last record per path wins). A path with no identity has nothing
    /// to publish: a removal is only ever journaled by the drain, for a path
    /// the core marked as removed.
    pub(super) fn journal_identity_now(&mut self, rel: &std::path::Path) {
        if !self.state.file_identity.contains_key(rel) {
            return;
        }
        let line = self.state.journal_line_for(rel);
        let frames = crate::state::encode_journal_records(std::slice::from_ref(&line));
        if self.append_identity_journal(&frames, 1) {
            self.state.journal_pending.remove(rel);
        }
    }

    /// Sync the HLC floor from the live clock, then persist daemon state. The
    /// single funnel for state persistence so the floor (the monotonic-restart
    /// seed) is never left behind a stamp the clock has already emitted.
    ///
    /// The dirty flags clear ONLY on a successful save: the snapshot then
    /// subsumes every pending identity-journal line (the save truncates the
    /// journal) and any pending `Effect::SaveState`. A FAILED save keeps both
    /// `journal_pending` and `state_dirty` set, so the deltas stay eligible
    /// for the next journal drain and the next flush retry, the same
    /// keep-on-failure contract the journal drain itself holds; clearing
    /// them before the save would drop the deltas from both durability
    /// channels with no retry.
    ///
    /// `last_state_save` advances even on failure so a persistently failing
    /// disk retries at the flush floor's cadence instead of every event.
    pub(super) fn save_state(&mut self) {
        self.last_state_save = Some(Instant::now());
        // The pre-serialize sync (inodes, HLC floor, register stamps) is the
        // shared `SpaceState::sync_persisted` — the sim's modeled flush calls
        // the SAME function, so the two persist contracts cannot drift.
        self.state.sync_persisted();
        match self.state.state.save(&self.kutl_dir()) {
            Ok(()) => {
                self.state.journal_pending.clear();
                self.journal_lines_since_save = 0;
                self.state_dirty = false;
                // The save truncated the journal: a torn tail is gone.
                self.journal_tail = super::JournalTail::Intact;
            }
            Err(e) => {
                error!(error = %e, "failed to persist daemon state; deltas stay pending");
            }
        }
    }

    /// Flush a coalesced [`Effect::SaveState`] iff one is pending AND the loop is
    /// caught up (both intake channels drained) AND the last save is at least
    /// [`MIN_STATE_SAVE_INTERVAL`] old. Called from the sync loop after
    /// each event: during a bulk burst the intake stays non-empty (the observer
    /// runs behind a flood), so this is a no-op — no inline O(docs) rewrite — and
    /// the single save happens the instant the burst drains. The drained-channel
    /// gate also keeps `save_state`'s blocking disk write off the hot path while a
    /// backlog is pending (a save mid-flood would itself risk a relay eviction).
    ///
    /// The interval floor covers the flood the drained test cannot see: events
    /// TRICKLING in over the network (a peer's bulk move arriving op by op)
    /// leave the intake momentarily empty after every event, which flushed the
    /// O(docs) rewrite per event — the dominant per-event cost measured on the
    /// receiving side of a bulk move. With the floor, a flood costs at most one
    /// save per interval, and trailing dirty state persists on the next flush
    /// probe (at latest the 10s metrics tick, which rides the same loop).
    ///
    /// For a lone materialization (the offline-rename case) the channel is
    /// already empty AND the last save is old, so the inode is persisted in the
    /// same iteration that recorded it — before any abrupt stop.
    pub(super) fn flush_state_if_caught_up(
        &mut self,
        file_event_rx: &mpsc::Receiver<FileEvent>,
        sync_event_rx: &mpsc::Receiver<SyncEvent>,
    ) {
        if self.state_dirty
            && self.intake_drained(file_event_rx, sync_event_rx)
            && self
                .last_state_save
                .is_none_or(|last| last.elapsed() >= MIN_STATE_SAVE_INTERVAL)
        {
            self.save_state();
        }
    }
}

/// Read the `KUTL_CLOCK_SKEW_MS` test seam: a signed millisecond offset added to
/// every HLC physical-time reading this daemon takes. Unset or unparseable → 0
/// (no skew). Used only by the reproducibility-under-skew acceptance test;
/// production never sets it.
pub(super) fn read_clock_skew_ms() -> i64 {
    std::env::var("KUTL_CLOCK_SKEW_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use crate::SafeRelayPath;
    use crate::bridge;
    use crate::client::SyncCommand;
    use crate::core::{DaemonCore, Event, EventStamp};
    use crate::daemon::tests::test_worker;
    use crate::watcher::Suppression;

    /// `offline_delete_stamp` must lift an offline delete one ms above the latest
    /// content edit it has OBSERVED, so the delete dominates the relay's content
    /// touch (`physical_touch(content_ms)`, logical = `u32::MAX`) and a genuine
    /// self-delete wins. A peer edit it has NOT observed is absent from the CRDT,
    /// so it stays above the delete's basis and revives the document.
    #[test]
    fn test_offline_delete_stamp_is_one_ms_above_observed_content() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let doc_id = "11111111-1111-1111-1111-111111111111";

        let content_ms = {
            let doc = worker.state.load_or_create_doc(doc_id);
            let agent = doc.register_agent("did:test").unwrap();
            bridge::apply_file_change(doc, agent, "did:test", "hello").unwrap();
            doc.changes()
                .iter()
                .map(|c| c.timestamp)
                .max()
                .expect("a content edit recorded a change")
        };

        let stamp = worker.offline_delete_stamp(doc_id);
        assert_eq!(
            stamp.physical_ms,
            u64::try_from(content_ms).unwrap() + 1,
            "offline delete must be stamped one ms above the latest observed content edit"
        );
        assert_eq!(stamp.logical, 0);
    }

    /// A document with no observed content (no CRDT changes) has no content touch
    /// to beat, so the stamp falls back to the pre-offline floor (which covers the
    /// document's registration stamp).
    #[test]
    fn test_offline_delete_stamp_falls_back_to_floor_without_content() {
        let dir = tempfile::tempdir().unwrap();
        let worker = test_worker(dir.path().to_path_buf());
        let stamp = worker.offline_delete_stamp("22222222-2222-2222-2222-222222222222");
        assert_eq!(stamp, worker.offline_floor());
    }

    /// An OFFLINE
    /// rename's causal floor is the register HLC this daemon observed — and it
    /// must SURVIVE A RESTART. The offline rename is re-emitted at the NEXT
    /// startup (`reconcile_offline_renames`), stamped at the pre-offline floor
    /// (`{0,0}` for a pure observer that never emitted a stamp), so the floor is
    /// its ONLY causal proof over the registration: without it the relay's
    /// `path_priority` reads the rename as a stale pre-registration echo and
    /// drops it cluster-wide (the rejoiner keeps its renamed file, every peer
    /// keeps the old path, permanent divergence).
    ///
    /// Drives the real remote-register entry (`Event::RemoteRegister`), then
    /// persists and restarts a fresh `SpaceWorker` over the same `.kutl` dir.
    #[test]
    fn test_rename_causal_floor_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let doc = "22222222-2222-4222-8222-222222222222";
        let reg_stamp = Hlc {
            physical_ms: 7_000,
            logical: 3,
            actor: kutl_core::ActorId(uuid::Uuid::from_u128(7)),
        };

        // ── session 1: observe a peer's register through the real path ──
        {
            let mut worker = test_worker(dir.path().to_path_buf());
            let (sync_cmd_tx, _sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();

            {
                let (suppress_tx, _suppress_rx) = mpsc::unbounded_channel::<Suppression>();
                let effects = DaemonCore::handle(
                    &mut worker.state,
                    Event::RemoteRegister {
                        document_id: doc.to_owned(),
                        path: "observed.md".to_owned(),
                        stamp: EventStamp {
                            wall_ms: 7_000,
                            origin_hlc: Some(reg_stamp),
                        },
                    },
                );
                for eff in effects {
                    worker
                        .apply_effect(eff, &sync_cmd_tx, &suppress_tx)
                        .expect("apply remote-register effect");
                }
            }

            assert_eq!(
                worker.rename_causal_floor(doc),
                Some(reg_stamp),
                "the observed register stamp is the floor within the session"
            );
            worker.save_state();
        }

        // ── session 2: the restart must still know the register it observed ──
        let worker2 = test_worker(dir.path().to_path_buf());
        assert_eq!(
            worker2.rename_causal_floor(doc),
            Some(reg_stamp),
            "the register HLC must survive a restart — an offline rename re-emitted \
             at the next startup carries no other causal proof over the registration"
        );
    }

    /// A FAILED state-snapshot save must keep the deltas in BOTH durability
    /// channels — `journal_pending` (the next drain retries the journal) and
    /// `state_dirty` (the next flush retries the save) — and the retry after
    /// the disk recovers persists them and clears both. Clearing before the
    /// save verdict dropped a failed save's deltas with no retry path.
    #[test]
    fn test_save_state_failure_keeps_deltas_pending() {
        const DOC: &str = "11111111-1111-4111-8111-111111111111";
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());

        // A FILE at the `.kutl` path makes every persist refuse
        // (`create_dir_all` cannot make a directory of it) — the journal
        // append inside `register_identity` included, so the delta is pending
        // in both channels when the save attempt runs.
        let kutl_dir = worker.kutl_dir();
        let _ = std::fs::remove_dir_all(&kutl_dir);
        std::fs::write(&kutl_dir, "not a directory").unwrap();

        worker.register_identity(
            SafeRelayPath::new("a.md").unwrap(),
            DOC.to_owned(),
            /* confirmed */ false,
        );
        worker.save_state();
        assert!(
            worker.state_dirty,
            "a failed save must keep state_dirty for the flush retry"
        );
        assert!(
            worker.state.journal_pending.contains(Path::new("a.md")),
            "a failed save must keep the delta journal-pending"
        );

        // Disk recovers: the retry persists the delta and clears both channels.
        std::fs::remove_file(&kutl_dir).unwrap();
        worker.save_state();
        assert!(
            !worker.state_dirty,
            "a successful save clears the dirty flag"
        );
        assert!(
            worker.state.journal_pending.is_empty(),
            "a successful save subsumes every pending journal line"
        );
        let reloaded = crate::state::DaemonState::load(&worker.kutl_dir());
        assert_eq!(
            reloaded.documents.get("a.md").map(|e| e.id.as_str()),
            Some(DOC),
            "the retried save persisted the delta the failed save kept pending"
        );
    }

    /// The drained journal batch must put present-entry lines before removal
    /// lines. Replay stops at the first torn line, so every prefix must leave
    /// state no worse than its base: a torn tail that kept an insert but lost
    /// a removal leaves a stale extra entry (healed by the offline-rename
    /// pre-pass), while the reverse order could replay a move as
    /// remove-without-insert — the document forgotten, re-registered into a
    /// conflict copy on restart.
    #[test]
    fn test_journal_drain_orders_inserts_before_removals() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        // Tracked paths drain as insert lines; pended-but-untracked paths (a
        // remove leaves exactly this) drain as removal lines.
        let tracked = ["a.md", "b.md", "c.md", "d.md"];
        let removed = ["w.md", "x.md", "y.md", "z.md"];
        for name in tracked {
            worker.state.identity_insert(
                PathBuf::from(name),
                crate::core::FileIdentity {
                    document_uuid: format!("uuid-{name}"),
                    inode: None,
                    last_written_hash: None,
                },
            );
        }
        let journal = crate::state::identity_journal_path(&worker.kutl_dir());
        // `journal_pending` is a HashSet, so a lucky iteration order could
        // pass by accident; a fresh set (fresh hash seed) per round makes the
        // orders independent and an accidental all-rounds pass negligible.
        for round in 0..5 {
            worker.state.journal_pending = std::collections::HashSet::new();
            for name in tracked.iter().chain(&removed) {
                worker.state.journal_pending.insert(PathBuf::from(name));
            }
            let _ = std::fs::remove_file(&journal);
            worker.drain_identity_journal();
            let is_removal: Vec<bool> = crate::state::read_journal_records(&worker.kutl_dir())
                .iter()
                .map(crate::state::IdentityJournalLine::is_removal)
                .collect();
            assert_eq!(is_removal.len(), tracked.len() + removed.len());
            assert!(
                is_removal.is_sorted(),
                "round {round}: a removal line preceded an insert line: {is_removal:?}"
            );
        }
    }
}
