//! Telemetry initialization shared across all kutl Rust binaries.
//!
//! Two entry points:
//! - [`init_tracing`] for long-running services — structured JSON on **stdout**
//!   (the stream log aggregators consume), `info` by default.
//! - [`init_cli_tracing`] for interactive CLI commands — diagnostics on
//!   **stderr** (so the command's own stdout output stays clean and pipeable),
//!   quiet (`warn`) by default.

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

/// Environment variable to enable human-readable compact log output.
const KUTL_LOG_PRETTY_ENV: &str = "KUTL_LOG_PRETTY";

/// Default log filter for long-running services.
const SERVICE_DEFAULT_FILTER: &str = "info";

/// Default log filter for interactive CLI commands. Quiet by default so a
/// normal `kutl <cmd>` isn't buried under info-level diagnostics; opt back in
/// with `KUTL_LOG=info`.
const CLI_DEFAULT_FILTER: &str = "warn";

/// Initialize structured logging for a long-running kutl service.
///
/// # Output format
///
/// Produces JSON-formatted log lines on **stdout** by default. Set
/// `KUTL_LOG_PRETTY=1` to switch to a compact, human-readable format for local
/// development.
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
    install(build_env_filter(), is_pretty(), std::io::stdout);

    tracing::info!(
        service = service_name,
        version = env!("CARGO_PKG_VERSION"),
        "tracing initialized"
    );
}

/// Initialize logging for an interactive CLI command.
///
/// Routes diagnostics to **stderr** so the command's own stdout output stays
/// clean and pipeable, and defaults the filter to `"warn"` so a successful run
/// is quiet. `KUTL_LOG`/`RUST_LOG` still override the level and
/// `KUTL_LOG_PRETTY=1` still selects the compact format.
pub fn init_cli_tracing() {
    install(
        build_env_filter_with_default(CLI_DEFAULT_FILTER),
        is_pretty(),
        std::io::stderr,
    );
}

/// Install the global subscriber with the given filter, format, and writer.
fn install<W>(filter: EnvFilter, pretty: bool, writer: W)
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    if pretty {
        tracing_subscriber::fmt()
            .compact()
            .with_env_filter(filter)
            .with_target(true)
            .with_writer(writer)
            .init();
    } else {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_target(true)
            .with_writer(writer)
            .init();
    }
}

/// Whether compact human-readable output was requested via `KUTL_LOG_PRETTY=1`.
fn is_pretty() -> bool {
    std::env::var(KUTL_LOG_PRETTY_ENV)
        .ok()
        .is_some_and(|v| v == "1")
}

/// Build an [`EnvFilter`] using the standard kutl env-var precedence:
/// `KUTL_LOG` → `RUST_LOG` → `"info"`.
pub fn build_env_filter() -> EnvFilter {
    build_env_filter_with_default(SERVICE_DEFAULT_FILTER)
}

/// Build an [`EnvFilter`] with the standard `KUTL_LOG` → `RUST_LOG` precedence,
/// falling back to `default` when neither is set.
fn build_env_filter_with_default(default: &str) -> EnvFilter {
    std::env::var("KUTL_LOG")
        .or_else(|_| std::env::var("RUST_LOG"))
        .map_or_else(|_| EnvFilter::new(default), EnvFilter::new)
}
