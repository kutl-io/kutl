//! Virtual clock for deterministic simulation time.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// A virtual clock backed by an atomic counter.
///
/// All peers in a simulation share a single `VirtualClock` via `Arc`.
/// Time only advances when explicitly told to — there is no wall-clock drift.
#[derive(Debug)]
pub struct VirtualClock {
    millis: AtomicI64,
}

impl VirtualClock {
    /// Create a new clock starting at the given millisecond value.
    pub fn new(start: i64) -> Arc<Self> {
        Arc::new(Self {
            millis: AtomicI64::new(start),
        })
    }

    /// Return the current virtual time in milliseconds.
    pub fn now(&self) -> i64 {
        self.millis.load(Ordering::Relaxed)
    }

    /// Advance the clock by one millisecond.
    pub fn tick(&self) -> i64 {
        self.advance(1)
    }

    /// Advance the clock by `ms` milliseconds, returning the new time.
    pub fn advance(&self, ms: i64) -> i64 {
        self.millis.fetch_add(ms, Ordering::Relaxed) + ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_starts_at_given_time() {
        let clock = VirtualClock::new(1000);
        assert_eq!(clock.now(), 1000);
    }

    #[test]
    fn test_tick_advances_by_one() {
        let clock = VirtualClock::new(0);
        assert_eq!(clock.tick(), 1);
        assert_eq!(clock.now(), 1);
    }

    #[test]
    fn test_advance_by_amount() {
        let clock = VirtualClock::new(100);
        assert_eq!(clock.advance(50), 150);
        assert_eq!(clock.now(), 150);
    }
}
