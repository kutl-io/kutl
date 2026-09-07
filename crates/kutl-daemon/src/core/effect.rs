//! `PlaceKind` + `Effect` enum + `EffectResult` result envelopes.
//!
//! Everything the pure core wants done, returned as data. Disk mutations are
//! executed only by the shell's suppression-bundling funnels, so the
//! suppress-send can never be forgotten: the `suppress_tx` sends live inside
//! the `write_doc`/`rename_doc`/`remove_doc` funnels (daemon/effects.rs), never
//! beside a caller's mutation. The shell reports each disk effect's outcome
//! back as an `EffectResult` so the shadow updates on success only.
//!
//! No tokio, no `std::fs`, no `mpsc`, no clock reads.

use std::path::PathBuf;

use kutl_proto::sync::ChangeMetadata;

use crate::blob_state::HashedContent;

/// Why a doc is being placed onto a free target — selects the shell's funnel
/// and the post-op shadow update. Mirrors the former `materialize_at` fork.
///
/// `Copy` is intentionally omitted: `Rename { old_rel: PathBuf }` holds a
/// `PathBuf` which is not `Copy`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaceKind {
    /// Untracked here: register + subscribe; the content streams in (the
    /// former `place_register` path). Vacates nothing.
    Register,
    /// Tracked here: move our held file onto the target (the former
    /// `place_rename` path). Vacates `old_rel`.
    Rename { old_rel: PathBuf },
}

/// Everything the core wants done, returned as data. The outbound relay
/// variants mirror `SyncCommand` (client.rs) one for one. Disk mutations
/// carry their suppression inline so the suppress-echo can't be forgotten:
/// there is no separate `suppress_tx.send().await` — the sends live inside
/// the `write_doc`/`rename_doc`/`remove_doc` funnels, daemon/effects.rs.
#[derive(Debug)]
pub enum Effect {
    // ── outbound to the relay (mirrors `SyncCommand`, client.rs) ──
    /// Subscribe to a document on the relay (`SyncCommand::Subscribe`).
    Subscribe { document_id: String },
    /// Send local CRDT ops to the relay (`SyncCommand::SendOps`).
    SendOps {
        document_id: String,
        ops: Vec<u8>,
        metadata: Vec<ChangeMetadata>,
        /// 0 = text (default), 2 = blob.
        content_mode: i32,
        /// SHA-256 hash for blob content; empty for text.
        content_hash: Vec<u8>,
    },
    /// Register a new document in the relay registry
    /// (`SyncCommand::RegisterDocument`).
    RegisterDocument {
        space_id: String,
        document_id: String,
        path: String,
        metadata: Option<ChangeMetadata>,
    },
    /// Rename a document in the relay registry (`SyncCommand::RenameDocument`).
    RenameDocument {
        space_id: String,
        document_id: String,
        old_path: String,
        new_path: String,
        metadata: Option<ChangeMetadata>,
        /// Causal floor: the `registered_hlc` this daemon observed for the doc.
        /// Carried to the relay so the rename supersedes a clock-skewed (future-
        /// stamped) registration (see `DocRecord::path_priority`). `None` if
        /// unresolved.
        rename_causal_floor: Option<kutl_core::Hlc>,
    },
    /// Unregister a document from the relay registry
    /// (`SyncCommand::UnregisterDocument`).
    UnregisterDocument {
        space_id: String,
        document_id: String,
        metadata: Option<ChangeMetadata>,
    },
    // ── disk mutations (funnel: `write_doc`/`rename_doc`/`remove_doc` in
    //    daemon/effects.rs; suppression carried inline so it cannot be
    //    forgotten) ──
    /// Port of `write_doc` (daemon/effects.rs): write `content` to `rel`.
    /// The echo suppression is keyed by the hash of `content`, which travels
    /// with the bytes inside [`HashedContent`]: its only constructor hashes,
    /// so no producer can mis-key it, and a producer that hashed for its own
    /// decision (the blob merge's LWW compare) is not hashed again in the
    /// funnel. Shell ACKs `FileWritten`.
    WriteFile {
        rel: PathBuf,
        content: HashedContent,
    },
    /// Port of `remove_doc` (daemon/effects.rs): delete `rel`, suppress
    /// `(rel, None)` (the send in `remove_doc`).
    RemoveFile { rel: PathBuf },

    // ── atomic shell-guarded place ──
    /// Place `id` onto `target` ONLY if `target` is still free of an untracked
    /// occupant — the untracked `stat` and the place are one non-suspending
    /// critical section in the shell (as the former `reconcile_disk_for_rename`
    /// did: `exists()` then rename, adjacent). On an
    /// unexpected untracked file the shell does NOT place; it injects
    /// `UntrackedFileObserved` and re-runs `handle()`. `expected_free` is the
    /// core's belief from the shadow, passed so the shell can assert the TOCTOU
    /// window stayed closed.
    GuardedPlace {
        id: String,
        target: PathBuf,
        /// The core's belief: `true` iff the shadow showed the target path
        /// free of any `Occupant::Untracked` at effect-emit time.
        expected_free: bool,
        place_kind: PlaceKind,
    },

    // ── CRDT sidecar persistence (the `.dt` IO at the edge) ──
    /// Persist the in-memory CRDT `Document` for `document_id` to its `.dt`
    /// sidecar (`SpaceState::dt_path`); applied by `SpaceWorker::save_doc`.
    SaveDoc { document_id: String },
    /// Replace the in-memory CRDT `Document` for `document_id` with the copy
    /// in its `.dt` sidecar, or drop it when it was never saved. Emitted
    /// when a contained engine panic leaves the in-memory engine untrusted;
    /// the core pairs it with a `Subscribe` so the relay's catch-up restores
    /// whatever the sidecar lacks. Applied by `SpaceWorker::reload_doc`.
    ReloadDoc { document_id: String },

    // ── housekeeping ──
    /// Persist `.kutl/state.ksnap` + the HLC floor
    /// (`SpaceWorker::save_state`).
    SaveState,
    /// Persist the blob LWW map (`blob-state.ksnap`). The blob path's twin of
    /// `SaveState`: emitted by the core's blob handlers after an LWW-state
    /// mutation; the real `BlobStateMap::save` is an inline write (not
    /// coalesced), matching the former imperative `handle_blob_change`/
    /// `handle_remote_blob` saves.
    SaveBlobState,
    /// Refresh per-space `/metrics` gauges (`SpaceWorker::emit_periodic_metrics`):
    /// queue depth, blob backlog, progress staleness.
    EmitMetrics,
}

impl Effect {
    /// Short variant name for log lines. `WriteFile`/`SendOps` carry full file
    /// content, so the driver's per-effect log identifies the effect by name
    /// only — failure paths log the interesting fields at their own sites.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Subscribe { .. } => "Subscribe",
            Self::SendOps { .. } => "SendOps",
            Self::RegisterDocument { .. } => "RegisterDocument",
            Self::RenameDocument { .. } => "RenameDocument",
            Self::UnregisterDocument { .. } => "UnregisterDocument",
            Self::WriteFile { .. } => "WriteFile",
            Self::RemoveFile { .. } => "RemoveFile",
            Self::GuardedPlace { .. } => "GuardedPlace",
            Self::SaveDoc { .. } => "SaveDoc",
            Self::ReloadDoc { .. } => "ReloadDoc",
            Self::SaveState => "SaveState",
            Self::SaveBlobState => "SaveBlobState",
            Self::EmitMetrics => "EmitMetrics",
        }
    }
}

/// Post-op feedback the shell returns for shadow truth. On failure the shell
/// returns nothing → the shadow is left unchanged → the next reconcile
/// re-derives and retries (the stateless re-stat self-heal behind the
/// log-and-swallow in `apply_write`/`apply_rename`/`apply_remove`).
#[derive(Debug)]
pub enum EffectResult {
    /// A `WriteFile` / `GuardedPlace(Register)` landed at `rel`. The post-op
    /// inode is recorded by the funnel in `FileIdentity::inode`, not carried.
    FileWritten { rel: PathBuf },
    /// A `GuardedPlace(Rename)` landed at `new`.
    RenameApplied { old: PathBuf, new: PathBuf },
    /// A `RemoveFile` landed (or the file was already gone — the goal state is
    /// met either way): `rel` is vacated on disk. Without this fold a delete
    /// leaves a stale `Tracked` occupant in the shadow forever, and the next
    /// register's `stat_untracked` mistakes a foreign recreate at the path for
    /// the tracked incumbent — placing (and adopting the recreate's bytes)
    /// instead of deferring (the concurrent-recreate merge corruption).
    FileRemoved { rel: PathBuf },
}
