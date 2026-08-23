/// Grace period before evicting an unsubscribed document (milliseconds).
pub const DEFAULT_EVICTION_GRACE_MS: u64 = 30_000;

/// Declarative eviction state — replaces timer-based eviction.
///
/// `should_evict(now)` replaces the eviction timer. Set `eligible_since`
/// when the last subscriber leaves; reset to 0 when a subscriber arrives.
/// No races because there's no timer to cancel — the relay checks current
/// state and acts accordingly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvictionState {
    /// When the document became eligible for eviction (millis since epoch).
    /// 0 means not eligible (subscribers present or never set).
    pub eligible_since: u64,
    /// Grace period before eviction (milliseconds).
    pub grace_period_ms: u64,
}

impl EvictionState {
    /// Create a new eviction state with the default grace period.
    pub fn new() -> Self {
        Self {
            eligible_since: 0,
            grace_period_ms: DEFAULT_EVICTION_GRACE_MS,
        }
    }

    /// Whether the document should be evicted now.
    pub fn should_evict(&self, now: u64) -> bool {
        self.eligible_since > 0 && now.saturating_sub(self.eligible_since) >= self.grace_period_ms
    }

    /// Mark the document as eligible for eviction.
    pub fn mark_eligible(&mut self, now: u64) {
        if self.eligible_since == 0 {
            self.eligible_since = now;
        }
    }

    /// Reset eligibility (subscriber arrived).
    pub fn reset(&mut self) {
        self.eligible_since = 0;
    }
}

impl Default for EvictionState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_evictable_when_not_eligible() {
        let s = EvictionState {
            eligible_since: 0,
            grace_period_ms: 30_000,
        };
        assert!(!s.should_evict(100_000));
    }

    #[test]
    fn test_not_evictable_before_grace_period() {
        let s = EvictionState {
            eligible_since: 1000,
            grace_period_ms: 30_000,
        };
        assert!(!s.should_evict(20_000)); // only 19s elapsed
    }

    #[test]
    fn test_evictable_after_grace_period() {
        let s = EvictionState {
            eligible_since: 1000,
            grace_period_ms: 30_000,
        };
        assert!(s.should_evict(31_000)); // 30s elapsed
    }

    #[test]
    fn test_evictable_exactly_at_grace_period() {
        let s = EvictionState {
            eligible_since: 1000,
            grace_period_ms: 30_000,
        };
        assert!(s.should_evict(31_000)); // exactly 30s
    }

    #[test]
    fn test_mark_eligible_only_sets_once() {
        let mut s = EvictionState::new();
        s.mark_eligible(100);
        assert_eq!(s.eligible_since, 100);
        s.mark_eligible(200); // no-op — already eligible
        assert_eq!(s.eligible_since, 100);
    }

    #[test]
    fn test_reset_clears_eligibility() {
        let mut s = EvictionState::new();
        s.mark_eligible(100);
        s.reset();
        assert_eq!(s.eligible_since, 0);
        assert!(!s.should_evict(200_000));
    }
}
