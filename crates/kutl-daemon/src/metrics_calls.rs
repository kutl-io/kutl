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

/// Gauge for the count of in-flight ack-bearing lifecycle commands not yet acked
/// by the relay (`RegisterDocument`/`RenameDocument`/`UnregisterDocument`/
/// `ListSpaceDocuments`). Drives the non-silent watchdog in the metrics tick.
const SYNC_BACKLOG: &str = "kutl_daemon_sync_backlog";

/// Gauge for a space's blob uploads queued but not yet sent to the relay.
const BLOB_UPLOAD_BACKLOG: &str = "kutl_daemon_blob_upload_backlog";

/// Counter for daemon errors, partitioned by [`error_category`].
const ERRORS_TOTAL: &str = "kutl_daemon_errors_total";

/// Counter for signal-stream pauses per space — times the relay paused this
/// daemon's signal stream under outbound backpressure and it had to
/// re-subscribe to resume.
///
/// Separate from [`ERRORS_TOTAL`] because a pause is recovered rather than
/// failed, and folding it into the error count would either overstate errors or,
/// left out entirely, hide a space that is chronically behind.
const SIGNAL_STREAM_PAUSES: &str = "kutl_daemon_signal_stream_pauses_total";
/// Counter: placements this daemon made on disk, per space — a file written
/// where a registration put it, or moved where a rename put it. Placement
/// churn: a document that settles in one place costs one; a document that
/// moves and moves back costs three.
const PLACEMENTS: &str = "kutl_daemon_placements_total";

/// Gauge for seconds since a space last made sync progress.
const SECONDS_SINCE_LAST_PROGRESS: &str = "kutl_daemon_seconds_since_last_progress";

/// Gauge for the files this process has quarantined as corrupt (moved aside
/// as `.corrupt` and replaced by a recovery action). Any nonzero value is a
/// file that was rebuilt rather than read, and wants a look at the error log.
const FILES_QUARANTINED: &str = "kutl_daemon_files_quarantined";
/// Files this process rewrote from their pre-envelope shape. Expected once
/// per file after an upgrade; a count that keeps climbing means a rewrite
/// is not landing.
const FILES_MIGRATED: &str = "kutl_daemon_files_migrated";

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
    /// A binary file exceeds `ABSOLUTE_BLOB_MAX` and is NOT being synced — the
    /// user's file is silently unprotected until they shrink or remove it.
    pub const BLOB_TOO_LARGE: &str = "blob_too_large";
    /// A text document is at `MAX_OPS_PER_DOC` — edits to it no longer merge
    /// anywhere, so replicas of this document are permanently diverging.
    pub const DOC_AT_OP_CAP: &str = "doc_at_op_cap";
    /// Remote ops crashed the CRDT engine; the document was reloaded from
    /// its sidecar and re-subscribed. No well-formed relay sends such bytes.
    pub const ENGINE_PANIC: &str = "engine_panic";
}

/// Set an unlabelled gauge from a count. Every count this daemon reports is
/// small (files, spaces: at most thousands), far inside the 2^53 integers
/// f64 carries exactly, so the cast is lossless.
#[allow(clippy::cast_precision_loss)]
fn set_gauge(name: &'static str, count: u64) {
    metrics::gauge!(name).set(count as f64);
}

/// Set a per-space gauge from a count; the same exactness argument as
/// [`set_gauge`] (backlogs are bounded by channel capacities, seconds by a
/// session's uptime).
#[allow(clippy::cast_precision_loss)]
fn set_space_gauge(name: &'static str, space_id: &str, value: u64) {
    metrics::gauge!(name, "space" => space_id.to_owned()).set(value as f64);
}

/// Record the running count of files quarantined as corrupt by this process.
pub fn record_files_quarantined(count: u64) {
    set_gauge(FILES_QUARANTINED, count);
}

/// Record the running count of files this process rewrote from their
/// pre-envelope shape.
pub fn record_files_migrated(count: u64) {
    set_gauge(FILES_MIGRATED, count);
}

/// Record the current count of registered spaces.
pub fn record_active_spaces(count: u64) {
    set_gauge(SPACES_REGISTERED, count);
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

/// Record a space's in-flight ack-bearing lifecycle command backlog. Nonzero
/// while the relay has not yet acked a `RegisterDocument`, `RenameDocument`,
/// `UnregisterDocument`, or `ListSpaceDocuments` command. The watchdog in
/// [`SpaceWorker::emit_periodic_metrics`] logs at ERROR when this stays
/// positive past [`STALE_PROGRESS_THRESHOLD`].
pub fn record_sync_backlog(space_id: &str, backlog: u64) {
    set_space_gauge(SYNC_BACKLOG, space_id, backlog);
}

/// Record a space's blob-upload backlog (blob sends queued but not yet drained
/// to the relay). Nonzero when binary uploads back up behind a slow relay.
pub fn record_blob_upload_backlog(space_id: &str, backlog: u64) {
    set_space_gauge(BLOB_UPLOAD_BACKLOG, space_id, backlog);
}

/// Increment the error counter for `category` (use an [`error_category`] const).
pub fn record_error(category: &'static str) {
    metrics::counter!(ERRORS_TOTAL, "category" => category).increment(1);
}

/// Count one placement landed on disk for `space_id`.
pub fn record_placement(space_id: &str) {
    metrics::counter!(PLACEMENTS, "space" => space_id.to_owned()).increment(1);
}

/// Increment the signal-stream pause counter for `space_id`.
pub fn record_signal_stream_pause(space_id: &str) {
    metrics::counter!(SIGNAL_STREAM_PAUSES, "space" => space_id.to_owned()).increment(1);
}

/// Record how many seconds have elapsed since a space last made sync progress
/// (a file or sync event successfully processed). Refreshed on a timer so the
/// gauge keeps climbing while the daemon is idle or wedged.
pub fn record_seconds_since_last_progress(space_id: &str, seconds: u64) {
    set_space_gauge(SECONDS_SINCE_LAST_PROGRESS, space_id, seconds);
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
            record_files_quarantined(5);
            record_files_migrated(7);
            record_relay_connected("relay-a", true);
            record_relay_reconnect("relay-a");
            record_sync_backlog("space-a", 3);
            record_blob_upload_backlog("space-a", 1);
            record_error(error_category::FILE_EVENT);
            record_placement("space-a");
            record_signal_stream_pause("space-a");
            record_seconds_since_last_progress("space-a", 42);
        });

        let rendered = handle.render();
        for expected in [
            "kutl_daemon_spaces_registered 2",
            "kutl_daemon_files_quarantined 5",
            "kutl_daemon_files_migrated 7",
            "kutl_daemon_relay_connected{relay=\"relay-a\"} 1",
            "kutl_daemon_relay_reconnects_total{relay=\"relay-a\"} 1",
            "kutl_daemon_sync_backlog{space=\"space-a\"} 3",
            "kutl_daemon_blob_upload_backlog{space=\"space-a\"} 1",
            "kutl_daemon_errors_total{category=\"file_event\"} 1",
            "kutl_daemon_placements_total{space=\"space-a\"} 1",
            "kutl_daemon_signal_stream_pauses_total{space=\"space-a\"} 1",
            "kutl_daemon_seconds_since_last_progress{space=\"space-a\"} 42",
        ] {
            assert!(
                rendered.contains(expected),
                "metrics output missing {expected:?}; got:\n{rendered}"
            );
        }
    }
}
