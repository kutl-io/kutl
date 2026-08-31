//! HLC stamping, causal floors, and persisted clock state for [`SpaceWorker`]:
//! metadata minting (fresh and explicit-stamp),
//! the pre-offline floor and the offline-delete stamp, the per-doc
//! register/lifecycle HLC watermarks, the local-rename causal floor, the
//! `fold_local_*` pair that mirrors locally-emitted lifecycle ops into the
//! placement lattice, and the `state.json` persistence funnel (`save_state` /
//! the coalesced caught-up flush) plus the `KUTL_CLOCK_SKEW_MS` test seam.

use std::path::Path;

use kutl_core::Hlc;
use kutl_proto::sync::ChangeMetadata;
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::client::SyncEvent;
use crate::core::rel_path_to_string;
use crate::watcher::FileEvent;

use super::SpaceWorker;

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
        let observed_content_ms = self
            .state
            .get_doc(document_id)
            .and_then(|doc| doc.changes().iter().map(|c| c.timestamp).max())
            .and_then(|ts| u64::try_from(ts).ok());
        match observed_content_ms {
            Some(ms) => Hlc {
                physical_ms: ms.saturating_add(OVER_CONTENT_TOUCH_MS),
                logical: 0,
                actor: floor.actor,
            },
            None => floor,
        }
    }

    /// Advance the origin clock past an observed remote stamp, so any op this
    /// daemon emits afterward is causally ordered after it.
    pub(super) fn observe_remote_hlc(&mut self, meta: Option<&ChangeMetadata>) {
        if let Some(wire) = meta.and_then(|m| m.hlc.clone()) {
            match Hlc::try_from(wire) {
                Ok(remote) => {
                    let wall = self.skewed_now_ms_u64();
                    self.state.hlc.recv(remote, wall);
                }
                Err(e) => warn!(error = %e, "ignoring malformed remote hlc"),
            }
        }
    }

    /// Advance the per-document lifecycle watermark to `hlc` (monotonic max).
    pub(super) fn record_lifecycle_hlc(&mut self, document_id: &str, hlc: Hlc) {
        let slot = self
            .state
            .lifecycle_hlc
            .entry(document_id.to_owned())
            .or_insert(hlc);
        if hlc > *slot {
            *slot = hlc;
        }
    }

    /// Record the REGISTER HLC for a document (monotonic max) — the precise
    /// source for a later rename's causal floor. Called for a local register and
    /// the first remote register observed. Kept separate from the lifecycle
    /// watermark, which absorbs renames (see [`Self::rename_causal_floor`]).
    pub(super) fn record_register_hlc(&mut self, document_id: &str, hlc: Hlc) {
        self.state.record_register_hlc(document_id, hlc);
    }

    /// Record the HLC of a lifecycle op this daemon just produced locally, so a
    /// stale remote echo of a now-superseded op for the same document is later
    /// dropped by [`SpaceState::lifecycle_event_is_fresh`].
    fn note_local_lifecycle_hlc(&mut self, document_id: &str, meta: &ChangeMetadata) {
        if let Some(hlc) = meta.hlc.clone().and_then(|w| Hlc::try_from(w).ok()) {
            self.record_lifecycle_hlc(document_id, hlc);
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
    /// restarts (`state.json`): an OFFLINE rename is re-emitted at the next
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

    /// Fold a LOCAL rename this daemon just detected via live inodes into the gamma
    /// placement lattice (`known_records`), mirroring what the pure core's
    /// `handle_file_renamed` (`core/handle/local_events.rs`) does
    /// (`seed_remote_record(... renamed_hlc)`, `core/handle/helpers.rs`).
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
    ///
    /// (That doc block describes [`Self::fold_local_rename_record`], defined
    /// below — the rename sibling of this fn; both fold a locally-emitted
    /// lifecycle op into the lattice the placement projection reads.)
    ///
    /// Fold a LOCAL register this daemon just emitted imperatively (the startup
    /// scan, `register_local_document`, and the blob path all register through
    /// `register_and_subscribe`, bypassing the core's mint fold) into the gamma
    /// placement lattice. Without it the lattice cannot arbitrate this doc's
    /// path against concurrently-broadcast peers — a rejoiner's own offline
    /// creation is invisible to its own `reconcile_placement`, so a same-path
    /// collision resolves against half the record set (the double-rejoin
    /// missing-canonical). The register sibling of
    /// [`Self::fold_local_rename_record`].
    ///
    /// Every caller registers a document the relay does not yet list. That is
    /// the invariant that keeps this fold safe: registering (and folding) a
    /// relay-KNOWN doc re-runs its settled path election with a fresh stamp
    /// on this node alone, permanently diverging the cluster. Startup
    /// announces known docs via `subscribe_and_push`, which never reaches
    /// this fold.
    pub(super) fn fold_local_register_record(
        &mut self,
        document_id: &str,
        path: &str,
        meta: &ChangeMetadata,
    ) {
        let Ok(id) = uuid::Uuid::parse_str(document_id) else {
            return;
        };
        let registered_hlc = meta.hlc.clone().and_then(|w| Hlc::try_from(w).ok());
        self.state
            .known_records
            .observe(kutl_core::lattice::DocRecord::register(
                id,
                path,
                registered_hlc,
            ));
    }

    /// Fold a LOCAL rename this daemon detected via live inodes into the
    /// placement lattice — the rename sibling of
    /// [`Self::fold_local_register_record`]; the full rationale (a stale
    /// record makes `reconcile_placement` move the file BACK, undoing the
    /// user's rename) is the doc block above that sibling.
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
    /// be half-used. (The local-register emit is intentionally NOT a lifecycle
    /// metadata: it is `&self` and the first op for a document, with no prior
    /// watermark to advance.)
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

    /// Sync the HLC floor from the live clock, then persist daemon state. The
    /// single funnel for state persistence so the floor (the monotonic-restart
    /// seed) is never left behind a stamp the clock has already emitted.
    pub(super) fn save_state(&mut self) {
        // The pre-serialize sync (inodes, HLC floor, register stamps) is the
        // shared `SpaceState::sync_persisted` — the sim's modeled flush calls
        // the SAME function, so the two persist contracts cannot drift.
        self.state.sync_persisted();
        if let Err(e) = self.state.state.save(&self.kutl_dir()) {
            error!(error = %e, "failed to persist daemon state");
        }
        // Any persist clears the coalesced-save flag: this write captured every
        // pending change (it serializes the WHOLE state), so a direct caller
        // subsumes a pending `Effect::SaveState` too.
        self.state_dirty = false;
    }

    /// Flush a coalesced [`Effect::SaveState`] iff one is pending AND the loop is
    /// caught up (both intake channels drained). Called from the sync loop after
    /// each event: during a bulk burst the intake stays non-empty (the observer
    /// runs behind a flood), so this is a no-op — no inline O(docs) rewrite — and
    /// the single save happens the instant the burst drains. The drained-channel
    /// gate also keeps `save_state`'s blocking disk write off the hot path while a
    /// backlog is pending (a save mid-flood would itself risk a relay eviction).
    /// For a lone materialization (the offline-rename case) the channel is already
    /// empty, so the inode is persisted in the same iteration that recorded it —
    /// before any abrupt stop.
    pub(super) fn flush_state_if_caught_up(
        &mut self,
        file_event_rx: &mpsc::Receiver<FileEvent>,
        sync_event_rx: &mpsc::Receiver<SyncEvent>,
    ) {
        if self.state_dirty && file_event_rx.is_empty() && sync_event_rx.is_empty() {
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
}
