use super::Lattice;

/// The kind of content stored in a document.
///
/// Ordering: `Empty < Blob < Text`. Used as a tiebreaker when two entries
/// have the same `kind_changed_at` timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ContentKind {
    /// No content yet.
    Empty = 0,
    /// Binary blob (LWW by timestamp).
    Blob = 1,
    /// Text document backed by a CRDT.
    Text = 2,
}

/// Tracks content kind and dirty/clean status via monotonic counters.
///
/// `is_dirty()` = `edit_counter > flushed_counter`. This replaces a mutable
/// boolean flag, which is not a valid semilattice (`merge(true, false)` has
/// no principled answer). Monotonic counters give the same information with
/// a valid merge (max).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentState {
    /// Current content kind.
    pub kind: ContentKind,
    /// When the kind was last changed (millis since epoch).
    pub kind_changed_at: u64,
    /// Monotonically increasing edit counter. Incremented on each mutation.
    pub edit_counter: u64,
    /// Monotonically increasing flush counter. Set to `edit_counter` on flush.
    pub flushed_counter: u64,
    /// Max `edit_counter` value confirmed durable by the storage backend.
    /// Unlike `flushed_counter` (in-memory only), this is projected to
    /// durable storage via [`RegistryBackend::update_witness`] on every
    /// successful backend flush and loaded back on relay restart. The read
    /// classification path uses it to distinguish "genuinely empty"
    /// (`persisted_version == 0`) from "inconsistent" (backend returned
    /// None but `persisted_version > 0`). RFD 0066.
    pub persisted_version: u64,
}

impl ContentState {
    /// Create a new empty content state.
    ///
    /// `persisted_version` defaults to 0, which the RFD 0066 classifier
    /// interprets as "never successfully persisted" — the sentinel that
    /// distinguishes a genuine first-time doc from "we lost content we
    /// previously persisted." See the `classify_load` docs in
    /// `kutl-sync-protocol`.
    pub fn new() -> Self {
        Self {
            kind: ContentKind::Empty,
            kind_changed_at: 0,
            edit_counter: 0,
            flushed_counter: 0,
            persisted_version: 0,
        }
    }

    /// Whether content has been mutated since last flush.
    pub fn is_dirty(&self) -> bool {
        self.edit_counter > self.flushed_counter
    }

    /// Record an edit (increment the edit counter).
    pub fn record_edit(&mut self) {
        self.edit_counter += 1;
    }

    /// Mark as flushed (set `flushed_counter` to current `edit_counter`).
    pub fn mark_flushed(&mut self) {
        self.flushed_counter = self.edit_counter;
    }

    /// Advance `flushed_counter` to at least `counter`. Used by the flush
    /// feedback path: the actor snapshots `edit_counter` at send time,
    /// and the flush task reports back the snapshot on successful persist.
    /// `max` makes this idempotent and order-independent — duplicate or
    /// out-of-order confirmations are harmless.
    pub fn mark_flushed_up_to(&mut self, counter: u64) {
        self.flushed_counter = self.flushed_counter.max(counter);
    }

    /// Advance `persisted_version` to at least `version`. Called after a
    /// successful backend flush *and* a successful durable witness write.
    /// `max` semantics make this idempotent and monotone — replayed
    /// confirmations or out-of-order witness loads cannot regress the field.
    pub fn mark_persisted(&mut self, version: u64) {
        self.persisted_version = self.persisted_version.max(version);
    }

    /// Transition to a new content kind. Returns `true` if the kind changed.
    pub fn set_kind(&mut self, kind: ContentKind, now: u64) -> bool {
        if self.kind == kind {
            return false;
        }
        self.kind = kind;
        self.kind_changed_at = now;
        true
    }
}

impl Default for ContentState {
    fn default() -> Self {
        Self::new()
    }
}

impl Lattice for ContentState {
    fn merge(&mut self, other: &Self) {
        // Kind: from whichever has the later kind_changed_at.
        // On tie, higher ContentKind discriminant wins (Text > Blob > Empty).
        if other.kind_changed_at > self.kind_changed_at
            || (other.kind_changed_at == self.kind_changed_at && other.kind > self.kind)
        {
            self.kind = other.kind;
        }
        self.kind_changed_at = self.kind_changed_at.max(other.kind_changed_at);
        self.edit_counter = self.edit_counter.max(other.edit_counter);
        self.flushed_counter = self.flushed_counter.max(other.flushed_counter);
        self.persisted_version = self.persisted_version.max(other.persisted_version);
    }

    fn lattice_le(&self, other: &Self) -> bool {
        self.kind_changed_at <= other.kind_changed_at
            && self.edit_counter <= other.edit_counter
            && self.flushed_counter <= other.flushed_counter
            && self.persisted_version <= other.persisted_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_content_kind() -> impl Strategy<Value = ContentKind> {
        (0u8..3).prop_map(|v| match v {
            0 => ContentKind::Empty,
            1 => ContentKind::Blob,
            _ => ContentKind::Text,
        })
    }

    fn arb_content_state() -> impl Strategy<Value = ContentState> {
        (
            arb_content_kind(),
            0u64..100,
            0u64..100,
            0u64..100,
            0u64..100,
        )
            .prop_map(
                |(kind, kind_changed_at, edit_counter, flushed_counter, persisted_version)| {
                    ContentState {
                        kind,
                        kind_changed_at,
                        edit_counter,
                        flushed_counter,
                        persisted_version,
                    }
                },
            )
    }

    proptest! {
        #[test]
        fn commutativity(a in arb_content_state(), b in arb_content_state()) {
            let mut ab = a.clone(); ab.merge(&b);
            let mut ba = b.clone(); ba.merge(&a);
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn associativity(
            a in arb_content_state(),
            b in arb_content_state(),
            c in arb_content_state()
        ) {
            let mut ab_c = a.clone(); ab_c.merge(&b); ab_c.merge(&c);
            let mut a_bc = a.clone();
            let mut bc = b.clone(); bc.merge(&c);
            a_bc.merge(&bc);
            prop_assert_eq!(ab_c, a_bc);
        }

        #[test]
        fn idempotency(a in arb_content_state()) {
            let mut aa = a.clone(); aa.merge(&a);
            prop_assert_eq!(a, aa);
        }

        #[test]
        fn monotonic_growth(a in arb_content_state(), b in arb_content_state()) {
            let mut merged = a.clone(); merged.merge(&b);
            prop_assert!(a.lattice_le(&merged));
            prop_assert!(b.lattice_le(&merged));
        }
    }

    #[test]
    fn test_dirty_when_edited() {
        let mut s = ContentState::new();
        assert!(!s.is_dirty());
        s.record_edit();
        assert!(s.is_dirty());
    }

    #[test]
    fn test_clean_after_flush() {
        let mut s = ContentState::new();
        s.record_edit();
        s.record_edit();
        s.mark_flushed();
        assert!(!s.is_dirty());
    }

    #[test]
    fn test_dirty_again_after_flush_and_edit() {
        let mut s = ContentState::new();
        s.record_edit();
        s.mark_flushed();
        s.record_edit();
        assert!(s.is_dirty());
    }

    #[test]
    fn test_mark_flushed_up_to_advances() {
        let mut s = ContentState::new();
        s.record_edit(); // edit_counter=1
        s.record_edit(); // edit_counter=2
        s.mark_flushed_up_to(1);
        assert!(s.is_dirty()); // edit_counter=2 > flushed_counter=1
        s.mark_flushed_up_to(2);
        assert!(!s.is_dirty()); // caught up
    }

    #[test]
    fn test_mark_flushed_up_to_idempotent() {
        let mut s = ContentState::new();
        s.record_edit();
        s.mark_flushed_up_to(1);
        s.mark_flushed_up_to(1); // duplicate — no-op
        assert!(!s.is_dirty());
    }

    #[test]
    fn test_mark_flushed_up_to_does_not_regress() {
        let mut s = ContentState::new();
        s.record_edit(); // 1
        s.record_edit(); // 2
        s.mark_flushed_up_to(2);
        s.mark_flushed_up_to(1); // out-of-order older snapshot — harmless
        assert!(!s.is_dirty()); // flushed_counter stayed at 2
    }

    #[test]
    fn test_edit_between_snapshot_and_confirm_stays_dirty() {
        let mut s = ContentState::new();
        s.record_edit(); // 1
        let snapshot = s.edit_counter; // snapshot=1
        s.record_edit(); // 2 — edit arrives after snapshot
        s.mark_flushed_up_to(snapshot); // confirms up to 1
        assert!(s.is_dirty()); // edit_counter=2 > flushed_counter=1
    }

    #[test]
    fn test_set_kind_returns_true_on_change() {
        let mut s = ContentState::new();
        assert!(s.set_kind(ContentKind::Text, 10));
        assert_eq!(s.kind, ContentKind::Text);
        assert_eq!(s.kind_changed_at, 10);
    }

    #[test]
    fn test_set_kind_returns_false_on_same() {
        let mut s = ContentState::new();
        assert!(!s.set_kind(ContentKind::Empty, 10));
        assert_eq!(s.kind_changed_at, 0); // unchanged
    }

    #[test]
    fn test_merge_kind_from_later_timestamp() {
        let mut a = ContentState {
            kind: ContentKind::Text,
            kind_changed_at: 5,
            edit_counter: 0,
            flushed_counter: 0,
            persisted_version: 0,
        };
        let b = ContentState {
            kind: ContentKind::Blob,
            kind_changed_at: 10,
            edit_counter: 0,
            flushed_counter: 0,
            persisted_version: 0,
        };
        a.merge(&b);
        assert_eq!(a.kind, ContentKind::Blob);
    }

    #[test]
    fn test_merge_kind_tiebreak_higher_wins() {
        let mut a = ContentState {
            kind: ContentKind::Blob,
            kind_changed_at: 5,
            edit_counter: 0,
            flushed_counter: 0,
            persisted_version: 0,
        };
        let b = ContentState {
            kind: ContentKind::Text,
            kind_changed_at: 5,
            edit_counter: 0,
            flushed_counter: 0,
            persisted_version: 0,
        };
        a.merge(&b);
        assert_eq!(a.kind, ContentKind::Text); // Text > Blob
    }

    #[test]
    fn test_mark_persisted_advances() {
        let mut s = ContentState::new();
        assert_eq!(s.persisted_version, 0);
        s.mark_persisted(5);
        assert_eq!(s.persisted_version, 5);
        s.mark_persisted(10);
        assert_eq!(s.persisted_version, 10);
    }

    #[test]
    fn test_mark_persisted_does_not_regress() {
        let mut s = ContentState::new();
        s.mark_persisted(10);
        s.mark_persisted(5); // out-of-order older witness — harmless
        assert_eq!(s.persisted_version, 10);
    }

    #[test]
    fn test_mark_persisted_idempotent() {
        let mut s = ContentState::new();
        s.mark_persisted(7);
        s.mark_persisted(7); // duplicate — no-op
        assert_eq!(s.persisted_version, 7);
    }

    #[test]
    fn test_merge_takes_max_persisted_version() {
        let mut a = ContentState {
            kind: ContentKind::Text,
            kind_changed_at: 0,
            edit_counter: 0,
            flushed_counter: 0,
            persisted_version: 5,
        };
        let b = ContentState {
            kind: ContentKind::Text,
            kind_changed_at: 0,
            edit_counter: 0,
            flushed_counter: 0,
            persisted_version: 12,
        };
        a.merge(&b);
        assert_eq!(a.persisted_version, 12);
    }
}
