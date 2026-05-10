//! Configuration loaded from environment variables.

/// Stress test configuration.
#[derive(Debug, Clone)]
pub struct StressConfig {
    /// Relay WebSocket URL.
    pub relay_url: String,
    /// Number of concurrent WS clients.
    pub client_count: usize,
    /// Total ops each client sends.
    pub ops_per_client: usize,
    /// Target ops per second per client (0 = unlimited).
    pub ops_per_sec: usize,
    /// Scenario to run.
    pub scenario: String,
    /// Space ID for subscriptions.
    pub space_id: String,
    /// Document ID for subscriptions.
    pub doc_id: String,
    /// Number of clients to degrade in `slow_client` scenario.
    pub degraded_count: usize,
    /// How long degraded clients pause reading (seconds) in `pause_read` scenario.
    pub pause_duration_secs: u64,
    /// Number of connect/disconnect cycles per churner client.
    pub churn_cycles: usize,
    /// Delay between churn reconnect cycles (ms).
    pub churn_delay_ms: u64,
    /// Kubernetes label selector for relay pods (chaos scenario).
    pub relay_pod_label: String,
    /// Number of relay pod kills in chaos scenario.
    pub chaos_kills: usize,
    /// Interval between relay pod kills (seconds).
    pub chaos_interval_secs: u64,
    /// Wall-clock timeout for the entire scenario (seconds). 0 = no timeout.
    pub timeout_secs: u64,
}

impl StressConfig {
    /// Load config from environment variables with defaults.
    pub fn from_env() -> Self {
        Self {
            relay_url: env_or("KUTL_STRESS_RELAY_URL", "ws://relay:9100/ws"),
            client_count: env_parse("KUTL_STRESS_CLIENT_COUNT", 10),
            ops_per_client: env_parse("KUTL_STRESS_OPS_PER_CLIENT", 1000),
            ops_per_sec: env_parse("KUTL_STRESS_OPS_PER_SEC", 100),
            scenario: env_or("KUTL_STRESS_SCENARIO", "sustained"),
            space_id: env_or("KUTL_STRESS_SPACE_ID", "stress-space"),
            doc_id: env_or("KUTL_STRESS_DOC_ID", "stress-doc"),
            degraded_count: env_parse("KUTL_STRESS_DEGRADED_COUNT", 2),
            pause_duration_secs: env_parse("KUTL_STRESS_PAUSE_DURATION_SECS", 5),
            churn_cycles: env_parse("KUTL_STRESS_CHURN_CYCLES", 50),
            churn_delay_ms: env_parse("KUTL_STRESS_CHURN_DELAY_MS", 100),
            relay_pod_label: env_or("KUTL_STRESS_RELAY_POD_LABEL", "app=relay"),
            chaos_kills: env_parse("KUTL_STRESS_CHAOS_KILLS", 3),
            chaos_interval_secs: env_parse("KUTL_STRESS_CHAOS_INTERVAL_SECS", 10),
            timeout_secs: env_parse("KUTL_STRESS_TIMEOUT_SECS", 300),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
