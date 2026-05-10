//! Per-link network properties for simulating adversarial conditions.

use std::ops::Range;

/// Per-link network properties.
///
/// Controls latency, packet loss, and message reordering for a directed
/// link between two peers.
#[derive(Debug, Clone)]
pub struct LinkConfig {
    /// Random latency in ticks added to the delivery tick.
    pub latency: Range<i64>,
    /// Probability `[0.0, 1.0)` of dropping a message.
    pub loss: f64,
    /// Probability `[0.0, 1.0)` of adding extra random delay.
    pub reorder: f64,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            latency: 0..1,
            loss: 0.0,
            reorder: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_noop() {
        let config = LinkConfig::default();
        assert_eq!(config.latency, 0..1);
        assert!(config.loss.abs() < f64::EPSILON);
        assert!(config.reorder.abs() < f64::EPSILON);
    }
}
