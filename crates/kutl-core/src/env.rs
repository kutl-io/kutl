//! Environment abstraction for non-deterministic dependencies.
//!
//! Production code uses [`SystemEnv`] (wall-clock time + OS randomness).
//! Simulation tests inject a virtual implementation for deterministic replay.

use std::fmt::Debug;
use std::sync::Arc;

use uuid::Uuid;

/// Milliseconds per second.
pub const MS_PER_SECOND: i64 = 1_000;
/// Milliseconds per minute.
pub const MS_PER_MINUTE: i64 = 60_000;
/// Milliseconds per hour.
pub const MS_PER_HOUR: i64 = 3_600_000;
/// Milliseconds per day.
pub const MS_PER_DAY: i64 = 86_400_000;
/// Milliseconds per week.
pub const MS_PER_WEEK: i64 = 604_800_000;

/// Seconds per minute.
pub const SECONDS_PER_MINUTE: i64 = 60;
/// Seconds per hour.
pub const SECONDS_PER_HOUR: i64 = 3_600;
/// Seconds per day.
pub const SECONDS_PER_DAY: i64 = 86_400;
/// Seconds per week.
pub const SECONDS_PER_WEEK: i64 = 604_800;

/// Provides time and identity generation to the sync engine.
///
/// Abstracting these behind a trait lets simulation tests replace
/// wall-clock time and OS randomness with deterministic alternatives.
pub trait Env: Send + Sync + Debug {
    /// Current time as milliseconds since the Unix epoch.
    fn now_millis(&self) -> i64;

    /// Generate a unique identifier (UUID v4).
    fn gen_id(&self) -> Uuid;
}

/// Shared, cheaply-cloneable handle to an [`Env`] implementation.
pub type SharedEnv = Arc<dyn Env>;

/// Current time in Unix milliseconds.
///
/// Convenience function for production code that doesn't need the [`Env`]
/// testing seam. Backed by [`jiff::Timestamp`].
pub fn now_ms() -> i64 {
    jiff::Timestamp::now().as_millisecond()
}

/// Current time in Unix milliseconds as `u64`.
///
/// Convenience wrapper for lattice timestamps, which use unsigned millis.
#[allow(clippy::cast_sign_loss)]
pub fn now_ms_u64() -> u64 {
    now_ms() as u64
}

/// Convert a [`jiff::SignedDuration`] to milliseconds.
///
/// Const-evaluable. Silently truncates if the duration overflows `i64`,
/// which is impossible for any duration under ~292 million years.
#[allow(clippy::cast_possible_truncation)]
pub const fn duration_ms(d: jiff::SignedDuration) -> i64 {
    d.as_millis() as i64
}

/// Convert a [`jiff::SignedDuration`] to [`std::time::Duration`].
///
/// Const-evaluable. Use this to define timeout/interval constants that
/// need a `std::time::Duration` for tokio or std APIs.
///
/// # Panics
///
/// Panics (at compile time if used in a const context) if the duration is
/// negative.
#[allow(clippy::cast_sign_loss)]
pub const fn std_duration(d: jiff::SignedDuration) -> std::time::Duration {
    let secs = d.as_secs();
    let nanos = d.subsec_nanos();
    assert!(secs >= 0 && nanos >= 0, "negative duration");
    std::time::Duration::new(secs as u64, nanos as u32)
}

/// Convert a [`std::time::Duration`] (e.g. from [`std::time::Instant::elapsed`])
/// to fractional milliseconds.
pub fn elapsed_ms(d: std::time::Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Production [`Env`] backed by the system clock and OS randomness.
#[derive(Debug)]
pub struct SystemEnv;

impl Env for SystemEnv {
    fn now_millis(&self) -> i64 {
        now_ms()
    }

    fn gen_id(&self) -> Uuid {
        Uuid::new_v4()
    }
}

/// Create a shared [`SystemEnv`] instance.
pub fn system_env() -> SharedEnv {
    Arc::new(SystemEnv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_env_generates_unique_ids() {
        let env = system_env();
        let a = env.gen_id();
        let b = env.gen_id();
        assert_ne!(a, b);
        assert_eq!(a.get_version(), Some(uuid::Version::Random));
    }

    #[test]
    fn test_system_env_time_positive() {
        let env = system_env();
        assert!(env.now_millis() > 0);
    }

    #[test]
    fn test_std_duration_from_millis() {
        const D: std::time::Duration = std_duration(jiff::SignedDuration::from_millis(500));
        assert_eq!(D, std::time::Duration::from_millis(500));
    }

    #[test]
    fn test_std_duration_from_secs() {
        const D: std::time::Duration = std_duration(jiff::SignedDuration::from_secs(30));
        assert_eq!(D, std::time::Duration::from_secs(30));
    }

    #[test]
    fn test_elapsed_ms_conversion() {
        let d = std::time::Duration::from_millis(1500);
        let ms = elapsed_ms(d);
        assert!((ms - 1500.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_elapsed_ms_subsecond() {
        let d = std::time::Duration::from_micros(500);
        let ms = elapsed_ms(d);
        assert!((ms - 0.5).abs() < f64::EPSILON);
    }
}
