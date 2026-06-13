//! Auto-recovery from stale subscriber evictions.

use std::time::Duration;

use kutl_core::{SignedDuration, std_duration};

/// Maximum exponent to prevent overflow in backoff calculation.
const MAX_BACKOFF_EXPONENT: u32 = 30;

/// Default maximum recovery attempts before giving up.
const DEFAULT_MAX_RECOVERY_ATTEMPTS: u32 = 10;

/// Default initial backoff delay.
const DEFAULT_INITIAL_BACKOFF: Duration = std_duration(SignedDuration::from_millis(100));

/// Default maximum backoff delay.
const DEFAULT_MAX_BACKOFF: Duration = std_duration(SignedDuration::from_secs(30));

/// Configuration for recovery behavior.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Initial backoff delay after `RATE_LIMITED`.
    pub initial_backoff: Duration,
    /// Maximum backoff delay.
    pub max_backoff: Duration,
    /// Backoff multiplier (typically 2.0 for exponential).
    pub backoff_multiplier: f64,
    /// Whether auto-recovery is enabled.
    pub enabled: bool,
    /// Maximum consecutive recovery attempts before giving up and returning
    /// the `Stale` event to the caller. `0` means unlimited.
    pub max_recovery_attempts: u32,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            backoff_multiplier: 2.0,
            enabled: true,
            max_recovery_attempts: DEFAULT_MAX_RECOVERY_ATTEMPTS,
        }
    }
}

impl RecoveryConfig {
    /// Compute the backoff duration for the nth retry attempt.
    ///
    /// Uses exponential backoff: `initial_backoff * multiplier^attempt`, capped
    /// at `max_backoff`.
    #[allow(clippy::cast_possible_wrap)]
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        let clamped = attempt.min(MAX_BACKOFF_EXPONENT) as i32;
        let delay = self
            .initial_backoff
            .mul_f64(self.backoff_multiplier.powi(clamped));
        delay.min(self.max_backoff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_for_exponential() {
        let config = RecoveryConfig::default();
        assert_eq!(config.backoff_for(0), Duration::from_millis(100));
        assert_eq!(config.backoff_for(1), Duration::from_millis(200));
        assert_eq!(config.backoff_for(2), Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let config = RecoveryConfig {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            ..Default::default()
        };
        // 1, 2, 4, 8 -> capped at 5
        assert_eq!(config.backoff_for(3), Duration::from_secs(5));
    }

    #[test]
    fn test_backoff_large_attempt_does_not_overflow() {
        let config = RecoveryConfig::default();
        // Should not panic and should be capped at max_backoff.
        let result = config.backoff_for(u32::MAX);
        assert_eq!(result, config.max_backoff);
    }

    #[test]
    fn test_backoff_custom_multiplier() {
        let config = RecoveryConfig {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_mins(1),
            backoff_multiplier: 3.0,
            ..Default::default()
        };
        // 50, 150, 450, 1350
        assert_eq!(config.backoff_for(0), Duration::from_millis(50));
        assert_eq!(config.backoff_for(1), Duration::from_millis(150));
        assert_eq!(config.backoff_for(2), Duration::from_millis(450));
        assert_eq!(config.backoff_for(3), Duration::from_millis(1350));
    }
}
