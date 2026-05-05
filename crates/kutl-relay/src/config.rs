/// Default per-connection outbound data channel capacity.
pub const DEFAULT_OUTBOUND_CAPACITY: usize = 64;

/// Default maximum document size (in characters) for snippet extraction.
pub const DEFAULT_SNIPPET_MAX_DOC_CHARS: usize = 10_000;

/// Default debounce delay for snippet computation in milliseconds.
pub const DEFAULT_SNIPPET_DEBOUNCE_MS: u64 = 2_000;

/// Relay server configuration, populated from environment variables.
pub struct RelayConfig {
    /// Bind address (default `127.0.0.1` — loopback only).
    ///
    /// Production deployments must explicitly set `KUTL_RELAY_HOST=0.0.0.0`
    /// (Dockerfiles + k8s manifests already do). Defaulting to loopback keeps
    /// `kutl-relay` started without env vars from accidentally exposing an
    /// auth-disabled relay to the network.
    pub host: String,
    /// Bind port (default `9100`).
    pub port: u16,
    /// Human-readable relay name sent in `HandshakeAck`.
    pub relay_name: String,
    /// When true, connections must present a valid auth token in the
    /// Handshake. When false, any connection is accepted (simulation/dev mode).
    ///
    /// Default depends on the bind address: `false` when binding to a loopback
    /// address (`127.0.0.1`, `::1`, `localhost`), `true` otherwise. This
    /// makes `kutl-relay` started locally with no env vars work for trial
    /// without bypassing auth, while keeping any non-loopback bind safe by
    /// default.
    pub require_auth: bool,
    /// Postgres connection string. When `None`, the relay operates in no-database
    /// mode (in-memory auth, no ACL enforcement) for backward compatibility.
    pub database_url: Option<String>,
    /// Per-connection outbound data channel capacity. When the channel fills,
    /// the relay evicts the slow subscriber. Default: 64.
    pub outbound_capacity: usize,
    /// Directory for persistent relay data (document registries, etc.).
    /// When `None`, registries live only in memory.
    pub data_dir: Option<std::path::PathBuf>,
    /// External URL for the relay, used to construct verification URLs.
    /// Falls back to `http://{host}:{port}` when not set.
    pub external_url: Option<String>,
    /// Base URL of the UX server for user-facing pages.
    ///
    /// Used to construct the device flow verification URL (the `/device` page
    /// lives on the UX server, not the relay). Falls back to `external_url`,
    /// then to `http://{host}:{port}` when not set.
    pub ux_url: Option<String>,
    /// Path to a plain-text file listing authorized DIDs (one per line).
    ///
    /// Used by the OSS relay for file-based access control. When set, DIDs
    /// in this file are allowed to subscribe to any space. When `None`, the
    /// authorized keys check is skipped (kutlhub mode uses Postgres ACL).
    pub authorized_keys_file: Option<std::path::PathBuf>,
    /// Maximum document size (in characters) for snippet extraction.
    /// Documents larger than this skip snippet computation entirely.
    /// Set to 0 to disable snippet extraction.
    pub snippet_max_doc_chars: usize,
    /// Debounce delay for snippet computation in milliseconds.
    /// After this quiet period, the relay computes and emits the change snippet.
    pub snippet_debounce_ms: u64,
}

impl RelayConfig {
    /// Read configuration from environment variables with sensible defaults.
    ///
    /// | Variable | Default |
    /// |---|---|
    /// | `KUTL_RELAY_HOST` | `127.0.0.1` (loopback only) |
    /// | `KUTL_RELAY_PORT` | `9100` |
    /// | `KUTL_RELAY_NAME` | `kutl-relay-dev` |
    /// | `KUTL_RELAY_REQUIRE_AUTH` | `false` on loopback, `true` otherwise |
    /// | `KUTL_DATABASE_URL` | *(none)* — shared with UX server (RFD 0034) |
    /// | `KUTL_RELAY_OUTBOUND_CAPACITY` | `64` |
    /// | `KUTL_RELAY_DATA_DIR` | *(none)* — enables registry persistence |
    /// | `KUTL_RELAY_EXTERNAL_URL` | *(none)* — used for device flow verification URL |
    /// | `KUTL_RELAY_UX_URL` | *(none)* — UX server base URL for user-facing pages |
    /// | `KUTL_RELAY_AUTHORIZED_KEYS_FILE` | *(none)* — file-based DID authorization |
    /// | `KUTL_RELAY_SNIPPET_MAX_DOC_CHARS` | `10000` |
    /// | `KUTL_RELAY_SNIPPET_DEBOUNCE_MS` | `2000` |
    pub fn from_env() -> Self {
        let host = std::env::var("KUTL_RELAY_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let require_auth = std::env::var("KUTL_RELAY_REQUIRE_AUTH")
            .map_or_else(|_| !is_loopback_host(&host), |v| v == "true" || v == "1");
        Self {
            host,
            port: std::env::var("KUTL_RELAY_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(9100),
            relay_name: std::env::var("KUTL_RELAY_NAME")
                .unwrap_or_else(|_| "kutl-relay-dev".into()),
            require_auth,
            database_url: std::env::var("KUTL_DATABASE_URL").ok(),
            outbound_capacity: std::env::var("KUTL_RELAY_OUTBOUND_CAPACITY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_OUTBOUND_CAPACITY),
            data_dir: std::env::var("KUTL_RELAY_DATA_DIR").ok().map(Into::into),
            external_url: std::env::var("KUTL_RELAY_EXTERNAL_URL").ok(),
            ux_url: std::env::var("KUTL_RELAY_UX_URL").ok(),
            authorized_keys_file: std::env::var("KUTL_RELAY_AUTHORIZED_KEYS_FILE")
                .ok()
                .map(Into::into),
            snippet_max_doc_chars: std::env::var("KUTL_RELAY_SNIPPET_MAX_DOC_CHARS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_SNIPPET_MAX_DOC_CHARS),
            snippet_debounce_ms: std::env::var("KUTL_RELAY_SNIPPET_DEBOUNCE_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_SNIPPET_DEBOUNCE_MS),
        }
    }

    /// Return the socket address string for binding.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Return true if `host` resolves to a loopback-only bind.
///
/// Recognises `127.0.0.0/8`, `::1`, and the literal `localhost`. Anything
/// else (including `0.0.0.0` and any external interface IP) is treated as
/// non-loopback. Used to decide the `require_auth` default.
fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return ip.is_loopback();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_loopback_host_recognises_v4_v6_and_name() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("127.0.0.42"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("LOCALHOST"));
    }

    #[test]
    fn test_is_loopback_host_rejects_external() {
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("10.0.0.1"));
        assert!(!is_loopback_host("relay.example.com"));
        assert!(!is_loopback_host("::"));
    }
}
