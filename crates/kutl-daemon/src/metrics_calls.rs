//! Metric emission points exposed to the daemon supervisor.
//!
//! All metric *facade* calls live here so the names are colocated and
//! easy to grep. The recorder is installed by the `kutl` binary crate
//! (the daemon process); this crate only emits.

/// Gauge for the number of registered spaces currently being supervised.
const SPACES_REGISTERED: &str = "kutl_daemon_spaces_registered";

/// Gauge for relay connection state per relay (0 or 1).
const RELAY_CONNECTED: &str = "kutl_daemon_relay_connected";

/// Counter for relay reconnection attempts.
const RELAY_RECONNECTS: &str = "kutl_daemon_relay_reconnects_total";

/// Record the current count of registered spaces.
#[allow(clippy::cast_precision_loss)]
// space counts are bounded to a few hundred at most; f64 round-trips them losslessly.
pub fn record_active_spaces(count: u64) {
    metrics::gauge!(SPACES_REGISTERED).set(count as f64);
}

/// Record a relay's connection state (0 = disconnected, 1 = connected).
pub fn record_relay_connected(relay_url: &str, connected: bool) {
    metrics::gauge!(RELAY_CONNECTED, "relay" => relay_url.to_owned())
        .set(f64::from(u8::from(connected)));
}

/// Increment the relay reconnect counter.
pub fn record_relay_reconnect(relay_url: &str) {
    metrics::counter!(RELAY_RECONNECTS, "relay" => relay_url.to_owned()).increment(1);
}
