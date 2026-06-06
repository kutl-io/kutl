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

/// Gauge for the depth of a space's outbound sync-command queue.
const SYNC_QUEUE_DEPTH: &str = "kutl_daemon_sync_queue_depth";

/// Gauge for a space's blob uploads queued but not yet sent to the relay.
const BLOB_UPLOAD_BACKLOG: &str = "kutl_daemon_blob_upload_backlog";

/// Counter for daemon errors, partitioned by [`error_category`].
const ERRORS_TOTAL: &str = "kutl_daemon_errors_total";

/// Gauge for seconds since a space last made sync progress.
const SECONDS_SINCE_LAST_PROGRESS: &str = "kutl_daemon_seconds_since_last_progress";

/// Category labels for [`record_error`]. A small closed set keyed to the
/// daemon's funnel points (the event loop + the relay client task), so the
/// `category` label stays low-cardinality and greppable.
pub mod error_category {
    /// The relay client task failed (connect/handshake/transport).
    pub const RELAY: &str = "relay";
    /// A local filesystem event could not be processed.
    pub const FILE_EVENT: &str = "file_event";
    /// A remote sync event could not be applied.
    pub const SYNC_EVENT: &str = "sync_event";
}

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

/// Record a space's outbound sync-command queue depth (buffered commands
/// awaiting transmission to the relay). Nonzero under relay backpressure.
#[allow(clippy::cast_precision_loss)]
// queue depth is bounded by the channel capacity (tens); f64 is exact here.
pub fn record_sync_queue_depth(space_id: &str, depth: u64) {
    metrics::gauge!(SYNC_QUEUE_DEPTH, "space" => space_id.to_owned()).set(depth as f64);
}

/// Record a space's blob-upload backlog (blob sends queued but not yet drained
/// to the relay). Nonzero when binary uploads back up behind a slow relay.
#[allow(clippy::cast_precision_loss)]
// backlog is bounded by the channel capacity (tens); f64 is exact here.
pub fn record_blob_upload_backlog(space_id: &str, backlog: u64) {
    metrics::gauge!(BLOB_UPLOAD_BACKLOG, "space" => space_id.to_owned()).set(backlog as f64);
}

/// Increment the error counter for `category` (use an [`error_category`] const).
pub fn record_error(category: &'static str) {
    metrics::counter!(ERRORS_TOTAL, "category" => category).increment(1);
}

/// Record how many seconds have elapsed since a space last made sync progress
/// (a file or sync event successfully processed). Refreshed on a timer so the
/// gauge keeps climbing while the daemon is idle or wedged.
#[allow(clippy::cast_precision_loss)]
// elapsed seconds within a session fit f64 exactly for any realistic uptime.
pub fn record_seconds_since_last_progress(space_id: &str, seconds: u64) {
    metrics::gauge!(SECONDS_SINCE_LAST_PROGRESS, "space" => space_id.to_owned())
        .set(seconds as f64);
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    /// Every facade call must emit a metric of the documented name, labels, and
    /// value into the Prometheus exposition output. Uses a scoped local recorder
    /// so the assertion is hermetic (no global recorder install).
    #[test]
    fn test_facade_emits_named_metrics_with_values() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            record_active_spaces(2);
            record_sync_queue_depth("space-a", 3);
            record_blob_upload_backlog("space-a", 1);
            record_error(error_category::FILE_EVENT);
            record_seconds_since_last_progress("space-a", 42);
        });

        let rendered = handle.render();
        for expected in [
            "kutl_daemon_spaces_registered 2",
            "kutl_daemon_sync_queue_depth{space=\"space-a\"} 3",
            "kutl_daemon_blob_upload_backlog{space=\"space-a\"} 1",
            "kutl_daemon_errors_total{category=\"file_event\"} 1",
            "kutl_daemon_seconds_since_last_progress{space=\"space-a\"} 42",
        ] {
            assert!(
                rendered.contains(expected),
                "metrics output missing {expected:?}; got:\n{rendered}"
            );
        }
    }
}
