//! Telemetry initialization shared across all kutl Rust binaries.
//!
//! Provides a single [`init_tracing`] entry point that configures structured
//! JSON logging by default, with a human-readable compact mode for local
//! development.

use tracing_subscriber::EnvFilter;

/// Environment variable to enable human-readable compact log output.
const KUTL_LOG_PRETTY_ENV: &str = "KUTL_LOG_PRETTY";

/// Initialize structured logging for a kutl service.
///
/// # Output format
///
/// Produces JSON-formatted log lines by default. Set `KUTL_LOG_PRETTY=1` to
/// switch to a compact, human-readable format for local development.
///
/// # Env var precedence (filter)
///
/// 1. `KUTL_LOG` — preferred, follows the `KUTL_*` naming convention.
/// 2. `RUST_LOG` — fallback for compatibility with standard Rust tooling.
/// 3. Default: `"info"`.
///
/// # Structured fields
///
/// Emits an initial log event with `service` and `version` fields so that
/// log aggregators can correlate all subsequent output from this process.
pub fn init_tracing(service_name: &str) {
    let filter = build_env_filter();

    let pretty = std::env::var(KUTL_LOG_PRETTY_ENV)
        .ok()
        .is_some_and(|v| v == "1");

    if pretty {
        tracing_subscriber::fmt()
            .compact()
            .with_env_filter(filter)
            .with_target(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_target(true)
            .init();
    }

    tracing::info!(
        service = service_name,
        version = env!("CARGO_PKG_VERSION"),
        "tracing initialized"
    );
}

/// Build an [`EnvFilter`] using the standard kutl env-var precedence:
/// `KUTL_LOG` → `RUST_LOG` → `"info"`.
pub fn build_env_filter() -> EnvFilter {
    std::env::var("KUTL_LOG")
        .or_else(|_| std::env::var("RUST_LOG"))
        .map_or_else(|_| EnvFilter::new("info"), EnvFilter::new)
}
