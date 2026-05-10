//! In-memory document snapshot store for crash/recovery simulation.

use std::collections::HashMap;

use kutl_core::Change;

use crate::PeerId;

/// Snapshot of a peer's document state.
#[derive(Debug, Clone)]
pub struct DocSnapshot {
    /// `Document::encode_full()` output — the diamond-types oplog blob.
    pub dt_bytes: Vec<u8>,
    /// Change metadata at the time of the snapshot.
    pub changes: Vec<Change>,
}

/// In-memory store mapping peer id to last persisted snapshot.
#[derive(Debug, Default)]
pub struct MemoryDocStore {
    snapshots: HashMap<PeerId, DocSnapshot>,
}

impl MemoryDocStore {
    /// Save a snapshot for the given peer.
    pub fn save(&mut self, peer: PeerId, snapshot: DocSnapshot) {
        self.snapshots.insert(peer, snapshot);
    }

    /// Retrieve the snapshot for a peer, if one exists.
    pub fn get(&self, peer: PeerId) -> Option<&DocSnapshot> {
        self.snapshots.get(&peer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_and_get() {
        let mut store = MemoryDocStore::default();
        store.save(
            0,
            DocSnapshot {
                dt_bytes: vec![1, 2, 3],
                changes: vec![],
            },
        );
        let snap = store.get(0).unwrap();
        assert_eq!(snap.dt_bytes, vec![1, 2, 3]);
    }

    #[test]
    fn test_get_missing_returns_none() {
        let store = MemoryDocStore::default();
        assert!(store.get(0).is_none());
    }
}
