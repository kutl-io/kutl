/// Default bind port (loopback `127.0.0.1:9100`).
pub const DEFAULT_RELAY_PORT: u16 = 9100;

/// Default per-connection outbound data channel capacity (frames).
///
/// Sized to absorb a several-hundred-doc catch-up burst without evicting the
/// subscriber: a space rejoin replays one full-state frame per subscribed
/// doc, and the writer may not get a scheduling slot until the burst is
/// queued (the measured 300-doc burst evicted at 64; 512 absorbs it with
/// ~1.7× headroom). Overflow is non-silent — the subscriber is evicted with
/// a `StaleSubscriber` notice on the unbounded ack lane and recovers via
/// re-subscribe — so the cap trades per-stalled-subscriber memory (~4 MiB at
/// typical 8 KiB frames; the per-frame ceiling is `MAX_PATCH_BYTES` for text
/// and the blob cap for blobs, so a byte-budgeted lane — not a frame count —
/// is the right guard if payload-heavy spaces appear) against eviction-resync
/// churn (~30 s per cycle). Env-tunable via `KUTL_RELAY_OUTBOUND_CAPACITY`
/// for A/B measurement.
pub const DEFAULT_OUTBOUND_CAPACITY: usize = 512;

/// Default maximum document size (in characters) for snippet extraction.
pub const DEFAULT_SNIPPET_MAX_DOC_CHARS: usize = 10_000;

/// Default debounce delay for snippet computation in milliseconds.
pub const DEFAULT_SNIPPET_DEBOUNCE_MS: u64 = 2_000;

/// Default interval between write-behind flushes to storage backends, in milliseconds.
///
/// Each tick the flush task asks the relay for dirty documents and persists
/// them to the content/blob backends. Lower values reduce time-to-durability
/// at the cost of more backend writes (and more witness updates); higher
/// values batch better but lengthen the "settle" window tests must outlast.
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1_500;

/// Default maximum blob upload size, in bytes (25 MiB).
///
/// Shared cap between the MCP `upload_blob` tool (this crate) and the UX
/// server's `/api/blob` endpoint — picking
/// different limits per surface would create arbitrary asymmetries between
/// agent and human uploads. 25 MiB covers the docx/pdf agent-replace case
/// while staying within JSON-RPC/HTTP comfort zones. Larger uploads are a
/// deferred follow-up (two-phase upload via pre-signed URL or chunked
/// streaming).
///
/// **Cross-language alignment:** the UX server duplicates this literal for
/// its `/api/blob` endpoint. The
/// env var `KUTL_MAX_BLOB_BYTES` is the canonical override surface for
/// both. A unit test pins the numeric value (`26_214_400`) here so any
/// change is loud; the TS side has a matching expectation.
pub const DEFAULT_MAX_BLOB_BYTES: usize = 25 * 1024 * 1024;

/// Relay server configuration, populated from environment variables.
pub struct RelayConfig {
    /// Bind address (default `127.0.0.1` — loopback only).
    ///
    /// Production deployments must explicitly set `KUTL_RELAY_HOST=0.0.0.0`
    /// (Dockerfiles + k8s manifests already do). Defaulting to loopback keeps
    /// `kutl-relay` started without env vars from accidentally exposing the
    /// relay to the network.
    pub host: String,
    /// Bind port (default `9100`).
    pub port: u16,
    /// Human-readable relay name sent in `HandshakeAck`.
    pub relay_name: String,
    /// Per-connection outbound data channel capacity. When the channel fills,
    /// the relay evicts the slow subscriber. Default:
    /// [`DEFAULT_OUTBOUND_CAPACITY`].
    pub outbound_capacity: usize,
    /// Directory for persistent relay data (document registries, signal store,
    /// signing identity). The OSS binary resolves this to a platform default
    /// when unset and `build_app` requires it; a host that wires its
    /// own backends (kutlhub) leaves it `None`.
    pub data_dir: Option<std::path::PathBuf>,
    /// Directory holding **only** the relay's signing identity, for a
    /// deployment that wants an identity without a segment store.
    ///
    /// Exists because `data_dir` conflates two unrelated capabilities: it is
    /// where segments live, so pointing it at a directory to give a host relay
    /// an identity would also construct a `SegmentRecordLog` and un-gate
    /// re-seed — turning on client-supplied history as a side effect of
    /// provisioning a key.
    ///
    /// When set it **wins** over `data_dir` for the identity, and it is loaded
    /// with [`RelayIdentity::load`], not `load_or_generate`: an operator who
    /// names an identity directory is asking for a specific key, so an absent
    /// file is a failed secret mount and aborts startup rather than minting a
    /// per-replica DID.
    ///
    /// [`RelayIdentity::load`]: crate::identity::RelayIdentity::load
    pub identity_dir: Option<std::path::PathBuf>,
    /// Whether this relay accepts **client-pushed** signal history (the
    /// `SignalReseed` WS frame). Default `true`.
    ///
    /// `true` is right for a self-hosted relay, where re-seed is how a client
    /// restores history the relay lost (clients are the source of
    /// truth). kutlhub sets it `false`: its signing key is the most attractive
    /// in the system, and a leaked relay key mints records every peer treats as
    /// tier-1, so the deployment holding that key does not also accept history
    /// from callers.
    ///
    /// Refusing does not affect **reads** — catch-up still serves.
    pub accepts_reseed: bool,
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
    /// Used by the relay for file-based access control. When set, DIDs in
    /// this file are allowed to subscribe to any space. When `None`, the
    /// authorized keys check is skipped (DB-backed mode enforces ACLs via
    /// the database).
    pub authorized_keys_file: Option<std::path::PathBuf>,
    /// Maximum document size (in characters) for snippet extraction.
    /// Documents larger than this skip snippet computation entirely.
    /// Set to 0 to disable snippet extraction.
    pub snippet_max_doc_chars: usize,
    /// Debounce delay for snippet computation in milliseconds.
    /// After this quiet period, the relay computes and emits the change snippet.
    pub snippet_debounce_ms: u64,
    /// Interval between write-behind flushes to storage backends, in milliseconds.
    /// Each tick the flush task drains dirty documents and persists them.
    pub flush_interval_ms: u64,
    /// Maximum blob upload size, in bytes.
    ///
    /// Enforced by the MCP `upload_blob` tool and matched by the UX server's
    /// `/api/blob` endpoint via the same `KUTL_MAX_BLOB_BYTES` env var.
    pub max_blob_bytes: usize,
}

impl RelayConfig {
    /// Read configuration from environment variables with sensible defaults.
    ///
    /// | Variable | Default |
    /// |---|---|
    /// | `KUTL_RELAY_HOST` | `127.0.0.1` (loopback only) |
    /// | `KUTL_RELAY_PORT` | `9100` |
    /// | `KUTL_RELAY_NAME` | `kutl-relay-dev` |
    /// | `KUTL_RELAY_OUTBOUND_CAPACITY` | `512` (`DEFAULT_OUTBOUND_CAPACITY`) |
    /// | `KUTL_RELAY_DATA_DIR` | platform default (Docker image sets `/var/lib/kutl`); RAM-only relays point this at a tmpfs |
    /// | `KUTL_RELAY_IDENTITY_DIR` | *(none)* — identity without a segment store; absent file aborts startup |
    /// | `KUTL_RELAY_ACCEPTS_RESEED` | `true` — set `0`/`false` to refuse client-pushed history |
    /// | `KUTL_RELAY_EXTERNAL_URL` | *(none)* — used for device flow verification URL |
    /// | `KUTL_RELAY_UX_URL` | *(none)* — UX server base URL for user-facing pages |
    /// | `KUTL_RELAY_AUTHORIZED_KEYS_FILE` | *(none)* — file-based DID authorization |
    /// | `KUTL_RELAY_SNIPPET_MAX_DOC_CHARS` | `10000` |
    /// | `KUTL_RELAY_SNIPPET_DEBOUNCE_MS` | `2000` |
    /// | `KUTL_RELAY_FLUSH_INTERVAL_MS` | `1500` |
    /// | `KUTL_MAX_BLOB_BYTES` | `26214400` (25 MiB) — shared with UX server `/api/blob` |
    pub fn from_env() -> Self {
        let host = std::env::var("KUTL_RELAY_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        Self {
            host,
            port: std::env::var("KUTL_RELAY_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_RELAY_PORT),
            relay_name: std::env::var("KUTL_RELAY_NAME")
                .unwrap_or_else(|_| "kutl-relay-dev".into()),
            outbound_capacity: std::env::var("KUTL_RELAY_OUTBOUND_CAPACITY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_OUTBOUND_CAPACITY),
            data_dir: std::env::var("KUTL_RELAY_DATA_DIR").ok().map(Into::into),
            identity_dir: std::env::var("KUTL_RELAY_IDENTITY_DIR")
                .ok()
                .map(Into::into),
            // Opt-OUT, not opt-in: re-seed is a self-hoster's recovery path and
            // silently losing it on a config typo would be worse than the
            // narrow exposure it carries. The one deployment that must refuse
            // sets it explicitly in code, not via the environment.
            accepts_reseed: !matches!(
                std::env::var("KUTL_RELAY_ACCEPTS_RESEED")
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .as_str(),
                "0" | "false"
            ),
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
            flush_interval_ms: std::env::var("KUTL_RELAY_FLUSH_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_FLUSH_INTERVAL_MS),
            max_blob_bytes: std::env::var("KUTL_MAX_BLOB_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_MAX_BLOB_BYTES),
        }
    }

    /// Return the socket address string for binding.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Default for RelayConfig {
    /// Loopback-bound, in-memory config — matches what
    /// [`RelayConfig::from_env`] produces when no env vars are set.
    ///
    /// Test sites use `RelayConfig { port: 0, ..Default::default() }` so
    /// adding new fields here doesn't require touching every call site.
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: DEFAULT_RELAY_PORT,
            relay_name: "kutl-relay-dev".into(),
            outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
            data_dir: None,
            identity_dir: None,
            accepts_reseed: true,
            external_url: None,
            ux_url: None,
            authorized_keys_file: None,
            snippet_max_doc_chars: DEFAULT_SNIPPET_MAX_DOC_CHARS,
            snippet_debounce_ms: DEFAULT_SNIPPET_DEBOUNCE_MS,
            flush_interval_ms: DEFAULT_FLUSH_INTERVAL_MS,
            max_blob_bytes: DEFAULT_MAX_BLOB_BYTES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_matches_loopback_safe_config() {
        // Validates every field — drift between this test and `Default`
        // is the failure mode the test is meant to catch, since 26 test
        // sites now lean on `..Default::default()` for unspecified fields.
        let c = RelayConfig::default();
        assert_eq!(c.host, "127.0.0.1");
        assert_eq!(c.port, 9100);
        assert_eq!(c.relay_name, "kutl-relay-dev");
        assert_eq!(c.outbound_capacity, DEFAULT_OUTBOUND_CAPACITY);
        assert_eq!(c.data_dir, None);
        assert_eq!(c.identity_dir, None);
        assert!(c.accepts_reseed);
        assert_eq!(c.external_url, None);
        assert_eq!(c.ux_url, None);
        assert_eq!(c.authorized_keys_file, None);
        assert_eq!(c.snippet_max_doc_chars, DEFAULT_SNIPPET_MAX_DOC_CHARS);
        assert_eq!(c.snippet_debounce_ms, DEFAULT_SNIPPET_DEBOUNCE_MS);
        assert_eq!(c.flush_interval_ms, DEFAULT_FLUSH_INTERVAL_MS);
        assert_eq!(c.max_blob_bytes, DEFAULT_MAX_BLOB_BYTES);
    }

    /// Pin the literal numeric value of [`DEFAULT_MAX_BLOB_BYTES`]. The
    /// same number is duplicated in the UX server's blob endpoint; this test
    /// makes any unilateral change loud on this side. A matching TS
    /// test pins the other side.
    #[test]
    fn test_default_max_blob_bytes_pinned() {
        const EXPECTED_BYTES_25_MIB: usize = 26_214_400;
        assert_eq!(
            DEFAULT_MAX_BLOB_BYTES, EXPECTED_BYTES_25_MIB,
            "DEFAULT_MAX_BLOB_BYTES must stay in sync with the UX server's blob cap; update both or neither"
        );
    }
}
