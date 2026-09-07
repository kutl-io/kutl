use std::collections::BTreeMap;

use super::ConnId;

/// A subscriber's state for a single document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriberEntry {
    /// When this connection subscribed (millis since epoch).
    pub subscribed_at: u64,
    /// When this connection unsubscribed. 0 if currently active.
    pub unsubscribed_at: u64,
    /// For text docs: diamond-types local version for delta encoding.
    /// For blobs: sentinel `[1]` means "has current blob."
    pub known_version: Vec<usize>,
}

impl SubscriberEntry {
    /// Whether this subscription is currently active.
    pub fn is_active(&self) -> bool {
        self.subscribed_at > self.unsubscribed_at
    }
}

/// Tracks active subscriptions per connection for a document.
///
/// `is_empty()` returns true when no entry is active (all unsubscribed
/// or no entries at all).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriberMap {
    entries: BTreeMap<ConnId, SubscriberEntry>,
}

impl SubscriberMap {
    /// Create an empty subscriber map.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Whether there are any active subscribers.
    pub fn is_empty(&self) -> bool {
        !self.entries.values().any(SubscriberEntry::is_active)
    }

    /// Count of active subscribers.
    pub fn active_count(&self) -> usize {
        self.entries.values().filter(|e| e.is_active()).count()
    }

    /// Record a subscription (local relay operation).
    pub fn subscribe(&mut self, conn_id: ConnId, now: u64, known_version: Vec<usize>) {
        self.entries.insert(
            conn_id,
            SubscriberEntry {
                subscribed_at: now,
                unsubscribed_at: 0,
                known_version,
            },
        );
    }

    /// Remove an entry entirely (garbage collection on disconnect).
    pub fn remove(&mut self, conn_id: &ConnId) {
        self.entries.remove(conn_id);
    }

    /// Get a mutable subscriber entry by connection ID (active only).
    pub fn get_mut(&mut self, conn_id: &ConnId) -> Option<&mut SubscriberEntry> {
        self.entries.get_mut(conn_id).filter(|e| e.is_active())
    }

    /// Iterate over active subscriber entries.
    pub fn active_entries(&self) -> impl Iterator<Item = (ConnId, &SubscriberEntry)> {
        self.entries
            .iter()
            .filter(|(_, e)| e.is_active())
            .map(|(&id, e)| (id, e))
    }

    /// Reset `known_version` for all active subscribers (used on content mode change).
    pub fn reset_all_versions(&mut self) {
        for entry in self.entries.values_mut() {
            if entry.is_active() {
                entry.known_version = Vec::new();
            }
        }
    }
}

impl Default for SubscriberMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_active_after_subscribe() {
        let e = SubscriberEntry {
            subscribed_at: 10,
            unsubscribed_at: 0,
            known_version: vec![],
        };
        assert!(e.is_active());
    }

    #[test]
    fn test_inactive_after_unsubscribe() {
        let e = SubscriberEntry {
            subscribed_at: 5,
            unsubscribed_at: 10,
            known_version: vec![],
        };
        assert!(!e.is_active());
    }

    #[test]
    fn test_resubscribe_is_active() {
        let e = SubscriberEntry {
            subscribed_at: 15,
            unsubscribed_at: 10,
            known_version: vec![],
        };
        assert!(e.is_active());
    }

    #[test]
    fn test_map_empty_when_no_entries() {
        let m = SubscriberMap::new();
        assert!(m.is_empty());
    }

    #[test]
    fn test_map_empty_when_all_inactive() {
        let mut m = SubscriberMap::new();
        m.entries.insert(
            1,
            SubscriberEntry {
                subscribed_at: 5,
                unsubscribed_at: 10,
                known_version: vec![],
            },
        );
        assert!(m.is_empty());
    }

    #[test]
    fn test_map_not_empty_when_active() {
        let mut m = SubscriberMap::new();
        m.subscribe(1, 5, vec![]);
        assert!(!m.is_empty());
        assert_eq!(m.active_count(), 1);
    }

    #[test]
    fn test_map_remove_on_disconnect() {
        let mut m = SubscriberMap::new();
        m.subscribe(1, 5, vec![]);
        m.subscribe(2, 6, vec![]);
        m.remove(&1);
        assert_eq!(m.active_count(), 1);
        assert!(m.get_mut(&1).is_none());
    }

    #[test]
    fn test_reset_all_versions() {
        let mut m = SubscriberMap::new();
        m.subscribe(1, 5, vec![1, 2, 3]);
        m.subscribe(2, 6, vec![4, 5]);
        m.reset_all_versions();
        assert!(m.get_mut(&1).unwrap().known_version.is_empty());
        assert!(m.get_mut(&2).unwrap().known_version.is_empty());
    }
}
