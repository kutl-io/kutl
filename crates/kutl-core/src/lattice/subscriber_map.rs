use std::collections::BTreeMap;

use super::{ConnId, Lattice};

/// A subscriber's state for a single document.
///
/// Merge: field-by-field max for timestamps. `known_version` comes from
/// whichever entry has the later `subscribed_at`. On tie, lexicographically
/// larger version wins (deterministic tiebreak).
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

impl Lattice for SubscriberEntry {
    fn merge(&mut self, other: &Self) {
        let take_other_version = other.subscribed_at > self.subscribed_at
            || (other.subscribed_at == self.subscribed_at
                && other.known_version > self.known_version);

        self.subscribed_at = self.subscribed_at.max(other.subscribed_at);
        self.unsubscribed_at = self.unsubscribed_at.max(other.unsubscribed_at);

        if take_other_version {
            self.known_version.clone_from(&other.known_version);
        }
    }

    fn lattice_le(&self, other: &Self) -> bool {
        self.subscribed_at <= other.subscribed_at && self.unsubscribed_at <= other.unsubscribed_at
    }
}

/// Tracks active subscriptions per connection for a document.
///
/// Merge: union of `ConnId`s, with per-entry lattice merge on collision.
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

    /// Record a subscription (local relay operation, not a lattice merge).
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

    /// Record an unsubscription (local relay operation).
    pub fn unsubscribe(&mut self, conn_id: ConnId, now: u64) {
        if let Some(entry) = self.entries.get_mut(&conn_id) {
            entry.unsubscribed_at = now;
        }
    }

    /// Remove an entry entirely (garbage collection on disconnect).
    pub fn remove(&mut self, conn_id: &ConnId) {
        self.entries.remove(conn_id);
    }

    /// Get a subscriber entry by connection ID (active only).
    pub fn get(&self, conn_id: &ConnId) -> Option<&SubscriberEntry> {
        self.entries.get(conn_id).filter(|e| e.is_active())
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

    /// Mutable iterate over active subscriber entries.
    pub fn active_entries_mut(&mut self) -> impl Iterator<Item = (ConnId, &mut SubscriberEntry)> {
        self.entries
            .iter_mut()
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

impl Lattice for SubscriberMap {
    fn merge(&mut self, other: &Self) {
        for (&conn_id, other_entry) in &other.entries {
            match self.entries.get_mut(&conn_id) {
                Some(self_entry) => self_entry.merge(other_entry),
                None => {
                    self.entries.insert(conn_id, other_entry.clone());
                }
            }
        }
    }

    fn lattice_le(&self, other: &Self) -> bool {
        self.entries.iter().all(|(k, v)| {
            other
                .entries
                .get(k)
                .is_some_and(|other_v| v.lattice_le(other_v))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_subscriber_entry() -> impl Strategy<Value = SubscriberEntry> {
        (
            0u64..100,
            0u64..100,
            proptest::collection::vec(0usize..50, 0..4),
        )
            .prop_map(
                |(subscribed_at, unsubscribed_at, known_version)| SubscriberEntry {
                    subscribed_at,
                    unsubscribed_at,
                    known_version,
                },
            )
    }

    fn arb_subscriber_map() -> impl Strategy<Value = SubscriberMap> {
        proptest::collection::btree_map(0u64..5, arb_subscriber_entry(), 0..4)
            .prop_map(|entries| SubscriberMap { entries })
    }

    // ---- SubscriberEntry property tests ----

    proptest! {
        #[test]
        fn entry_commutativity(a in arb_subscriber_entry(), b in arb_subscriber_entry()) {
            let mut ab = a.clone(); ab.merge(&b);
            let mut ba = b.clone(); ba.merge(&a);
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn entry_associativity(
            a in arb_subscriber_entry(),
            b in arb_subscriber_entry(),
            c in arb_subscriber_entry()
        ) {
            let mut ab_c = a.clone(); ab_c.merge(&b); ab_c.merge(&c);
            let mut a_bc = a.clone();
            let mut bc = b.clone(); bc.merge(&c);
            a_bc.merge(&bc);
            prop_assert_eq!(ab_c, a_bc);
        }

        #[test]
        fn entry_idempotency(a in arb_subscriber_entry()) {
            let mut aa = a.clone(); aa.merge(&a);
            prop_assert_eq!(a, aa);
        }

        #[test]
        fn entry_monotonic_growth(a in arb_subscriber_entry(), b in arb_subscriber_entry()) {
            let mut merged = a.clone(); merged.merge(&b);
            prop_assert!(a.lattice_le(&merged));
            prop_assert!(b.lattice_le(&merged));
        }
    }

    // ---- SubscriberMap property tests ----

    proptest! {
        #[test]
        fn map_commutativity(a in arb_subscriber_map(), b in arb_subscriber_map()) {
            let mut ab = a.clone(); ab.merge(&b);
            let mut ba = b.clone(); ba.merge(&a);
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn map_associativity(
            a in arb_subscriber_map(),
            b in arb_subscriber_map(),
            c in arb_subscriber_map()
        ) {
            let mut ab_c = a.clone(); ab_c.merge(&b); ab_c.merge(&c);
            let mut a_bc = a.clone();
            let mut bc = b.clone(); bc.merge(&c);
            a_bc.merge(&bc);
            prop_assert_eq!(ab_c, a_bc);
        }

        #[test]
        fn map_idempotency(a in arb_subscriber_map()) {
            let mut aa = a.clone(); aa.merge(&a);
            prop_assert_eq!(a, aa);
        }

        #[test]
        fn map_monotonic_growth(a in arb_subscriber_map(), b in arb_subscriber_map()) {
            let mut merged = a.clone(); merged.merge(&b);
            prop_assert!(a.lattice_le(&merged));
            prop_assert!(b.lattice_le(&merged));
        }
    }

    // ---- Derived behavior tests ----

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
        m.subscribe(1, 5, vec![]);
        m.unsubscribe(1, 10);
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
        assert!(m.get(&1).is_none());
    }

    #[test]
    fn test_reset_all_versions() {
        let mut m = SubscriberMap::new();
        m.subscribe(1, 5, vec![1, 2, 3]);
        m.subscribe(2, 6, vec![4, 5]);
        m.reset_all_versions();
        assert!(m.get(&1).unwrap().known_version.is_empty());
        assert!(m.get(&2).unwrap().known_version.is_empty());
    }
}
