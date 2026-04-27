//! JSON report output.

use serde::Serialize;

/// Final report output as JSON.
#[derive(Debug, Serialize)]
pub struct Report {
    pub scenario: String,
    pub client_count: usize,
    pub total_ops: usize,
    pub duration_ms: u64,
    pub ops_per_sec: f64,
    pub convergence_time_ms: u64,
    pub p50_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub final_doc_size: usize,
    pub converged: bool,
    /// Total auto-resubscribes across all clients (eviction recovery).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_resubscribes: Option<u32>,
    /// Resubscribes from intentionally degraded clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded_resubscribes: Option<u32>,
    /// Resubscribes from healthy (non-degraded) clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub healthy_resubscribes: Option<u32>,
    /// Total WebSocket reconnections (churner scenario).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_reconnects: Option<u32>,
    /// Number of relay pod kills (chaos scenario).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relay_restarts: Option<u32>,
}

impl Report {
    /// Build a report with the common fields every scenario provides.
    ///
    /// Optional fields (`total_resubscribes`, `degraded_resubscribes`,
    /// `healthy_resubscribes`, `total_reconnects`, `relay_restarts`) default
    /// to `None`. Use the builder methods to set them.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scenario: &str,
        client_count: usize,
        total_ops: usize,
        duration_ms: u64,
        convergence_time_ms: u64,
        latencies: &[f64],
        final_doc_size: usize,
        converged: bool,
    ) -> Self {
        Self {
            scenario: scenario.into(),
            client_count,
            total_ops,
            duration_ms,
            ops_per_sec: compute_ops_per_sec(total_ops, duration_ms),
            convergence_time_ms,
            p50_latency_ms: percentile(latencies, 0.5),
            p99_latency_ms: percentile(latencies, 0.99),
            final_doc_size,
            converged,
            total_resubscribes: None,
            degraded_resubscribes: None,
            healthy_resubscribes: None,
            total_reconnects: None,
            relay_restarts: None,
        }
    }

    /// Set eviction stats from a split of degraded/healthy clients.
    pub fn with_eviction_stats(mut self, total: u32, degraded: u32, healthy: u32) -> Self {
        self.total_resubscribes = Some(total);
        self.degraded_resubscribes = Some(degraded);
        self.healthy_resubscribes = Some(healthy);
        self
    }

    /// Set total resubscribes only (no degraded/healthy split).
    pub fn with_resubscribes(mut self, total: u32) -> Self {
        self.total_resubscribes = Some(total);
        self
    }

    /// Set total reconnects (churner/chaos scenarios).
    pub fn with_reconnects(mut self, total: u32) -> Self {
        self.total_reconnects = Some(total);
        self
    }

    /// Set relay restart count (chaos scenario).
    pub fn with_relay_restarts(mut self, count: u32) -> Self {
        self.relay_restarts = Some(count);
        self
    }

    /// Print the report as JSON to stdout.
    pub fn print(&self) {
        println!(
            "{}",
            serde_json::to_string_pretty(self).expect("report serialization should not fail")
        );
    }
}

/// Milliseconds per second.
const MS_PER_SEC: f64 = 1000.0;

/// Compute ops/sec from total ops and elapsed duration.
#[allow(clippy::cast_precision_loss)]
pub fn compute_ops_per_sec(total_ops: usize, duration_ms: u64) -> f64 {
    if duration_ms == 0 {
        return 0.0;
    }
    total_ops as f64 / (duration_ms as f64 / MS_PER_SEC)
}

/// Compute the percentile value from a sorted slice of latencies.
///
/// `pct` is 0.0–1.0 (e.g. 0.5 for p50, 0.99 for p99).
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
pub fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64) * pct).ceil() as usize;
    let idx = idx.min(sorted.len()).saturating_sub(1);
    sorted[idx]
}
