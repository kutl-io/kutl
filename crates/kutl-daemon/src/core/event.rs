//! `EventStamp` + the `Event` inbound union.
//!
//! No tokio, no `std::fs`, no `mpsc`, no clock reads — those live only in the
//! driver (`daemon.rs`) and at the sim edge.

use std::path::PathBuf;

use kutl_proto::sync::ChangeMetadata;

/// Injected time for one event: the wall reading the driver took (skew already
/// applied, `SpaceWorker::skewed_now_ms_u64`) and, for a remote lifecycle/edit
/// event, its origin HLC (`client.rs:368` `hlc_of`). The core ticks/recvs
/// against these instead of reading a clock, so a sim can replay any
/// interleaving deterministically.
#[derive(Debug, Clone, Copy)]
pub struct EventStamp {
    /// Skewed wall-clock millis at event entry (`SpaceWorker::skewed_now_ms_u64`).
    pub wall_ms: u64,
    /// Origin HLC of a remote lifecycle/edit op, if it carried one. `None` for a
    /// local file event or the pre-HLC fallback (`client.rs:371`).
    pub origin_hlc: Option<kutl_core::Hlc>,
}

/// Every input the pure core reacts to. Local file events, relay sync events,
/// timers, and the two cascade-probe feedbacks — one ordered stream.
#[derive(Debug, Clone)]
pub enum Event {
    // ── from the watcher (`watcher.rs` `FileEvent`) ──
    /// A file was created or modified. The watcher already read the bytes at the
    /// edge and discarded them (`handle_file_modified`, daemon.rs); carry them so
    /// the content read stays out of the core. `None` ⇒ the file is binary/blob
    /// (the `InvalidData` branch of `handle_file_modified`); the driver attaches
    /// the blob hash.
    FileModified {
        rel: PathBuf,
        content: Option<Vec<u8>>,
        stamp: EventStamp,
    },
    /// A file was removed (`FileEvent::Removed`).
    FileRemoved { rel: PathBuf, stamp: EventStamp },
    /// The watcher paired a rename (`FileEvent::Renamed`).
    FileRenamed {
        old: PathBuf,
        new: PathBuf,
        stamp: EventStamp,
    },

    // ── from the relay (`client.rs:31` `SyncEvent`) ──
    /// Remote CRDT ops for a subscribed doc (`client.rs:36`).
    ///
    /// `local_content` carries the CURRENT on-disk file bytes (text only) for a
    /// tracked doc, read by the driver at the edge (`next_event`), so the core can
    /// incorporate a pending local edit BEFORE the remote merge (local-before-remote
    /// ordering). `None` when the
    /// driver could not read text there (the file is absent, binary, or the doc
    /// is untracked) or for non-driver callers (the sim passes `None`).
    RemoteOps {
        document_id: String,
        ops: Vec<u8>,
        metadata: Vec<ChangeMetadata>,
        content_mode: i32,
        local_content: Option<String>,
        /// Uncapped agent→author-DID snapshot carried on the catch-up (join)
        /// envelope; empty on incremental deltas. Installed insert-if-absent
        /// after the merge so blame survives change-record eviction on a
        /// re-seeded joiner.
        author_by_agent_snapshot: std::collections::HashMap<String, String>,
        stamp: EventStamp,
    },
    /// A doc was registered by another daemon (`client.rs:46`). `stamp.origin_hlc`
    /// is the register HLC (`client.rs:394`).
    RemoteRegister {
        document_id: String,
        path: String,
        stamp: EventStamp,
    },
    /// A doc was renamed by another daemon (`client.rs:53`).
    RemoteRename {
        document_id: String,
        old_path: String,
        new_path: String,
        /// Causal floor the renamer attached: the `registered_hlc` it observed.
        /// Folded into `known_records` so this daemon's placement lattice treats
        /// the rename as causally-after a clock-skewed registration (else its own
        /// `desired_assignment` would keep the file at the pre-rename path).
        rename_causal_floor: Option<kutl_core::Hlc>,
        stamp: EventStamp,
    },
    /// A doc was unregistered by another daemon (`client.rs:61`).
    RemoteUnregister {
        document_id: String,
        stamp: EventStamp,
    },
    /// The relay acked THIS daemon's own register/rename (`client.rs:73`).
    LifecycleAck {
        document_id: String,
        effective_path: Option<String>,
        stamp: EventStamp,
    },
    // ── timers (driver-injected; replace `tokio::time` in the loop) ──
    /// The periodic `/metrics` refresh tick (the `metrics_tick` arm of `SpaceWorker::next_event`).
    MetricsTick { stamp: EventStamp },

    // ── the `GuardedPlace` shell feedback ──
    /// The shell's atomic stat found `rel` occupied by an untracked file when it
    /// went to place a doc there — re-run reconcile so the doc defers instead of
    /// clobbering.
    UntrackedFileObserved { rel: PathBuf, stamp: EventStamp },
    /// The shell's probe found `rel` is now free (e.g. a local `rm` of an
    /// untracked occupant) — re-trigger the deferred reconcile.
    UntrackedFileRemoved { rel: PathBuf, stamp: EventStamp },
}

impl Event {
    /// Short variant name for log lines. `FileModified`/`RemoteOps` carry full
    /// content bytes, so intake logs identify the event by name only — the
    /// per-decision logs in `handle` carry the interesting fields.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::FileModified { .. } => "FileModified",
            Self::FileRemoved { .. } => "FileRemoved",
            Self::FileRenamed { .. } => "FileRenamed",
            Self::RemoteOps { .. } => "RemoteOps",
            Self::RemoteRegister { .. } => "RemoteRegister",
            Self::RemoteRename { .. } => "RemoteRename",
            Self::RemoteUnregister { .. } => "RemoteUnregister",
            Self::LifecycleAck { .. } => "LifecycleAck",
            Self::MetricsTick { .. } => "MetricsTick",
            Self::UntrackedFileObserved { .. } => "UntrackedFileObserved",
            Self::UntrackedFileRemoved { .. } => "UntrackedFileRemoved",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_event_stamp_carries_wall_and_optional_hlc() {
        let stamp = EventStamp {
            wall_ms: 1_700_000_000_123,
            origin_hlc: None,
        };
        assert_eq!(stamp.wall_ms, 1_700_000_000_123);
        assert!(stamp.origin_hlc.is_none());
    }

    #[test]
    fn test_event_file_modified_carries_bytes_at_edge() {
        // The watcher reads+discards the bytes (`handle_file_modified`); the Event carries
        // them so the content read stays out of the core.
        let ev = Event::FileModified {
            rel: PathBuf::from("notes/a.md"),
            content: Some(b"hello".to_vec()),
            stamp: EventStamp {
                wall_ms: 1,
                origin_hlc: None,
            },
        };
        match ev {
            Event::FileModified { rel, content, .. } => {
                assert_eq!(rel, PathBuf::from("notes/a.md"));
                assert_eq!(content.as_deref(), Some(b"hello".as_ref()));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_event_remote_register_carries_origin_hlc_in_stamp() {
        // RemoteRegister's origin HLC rides in the stamp (client.rs:394 / hlc_of).
        let hlc = kutl_core::Hlc {
            physical_ms: 5,
            logical: 0,
            actor: kutl_core::hlc::ActorId(uuid::Uuid::nil()),
        };
        let ev = Event::RemoteRegister {
            document_id: "doc-1".into(),
            path: "a.md".into(),
            stamp: EventStamp {
                wall_ms: 9,
                origin_hlc: Some(hlc),
            },
        };
        match ev {
            Event::RemoteRegister { stamp, .. } => assert_eq!(stamp.origin_hlc, Some(hlc)),
            _ => panic!("wrong variant"),
        }
    }
}
