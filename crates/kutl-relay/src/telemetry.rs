//! Telemetry initialization shared across all kutl Rust binaries.
//!
//! Two entry points:
//! - [`init_tracing`] for long-running services — structured JSON on **stdout**
//!   (the stream log aggregators consume), `info` by default.
//! - [`init_cli_tracing`] for interactive CLI commands — diagnostics on
//!   **stderr** (so the command's own stdout output stays clean and pipeable),
//!   quiet (`warn`) by default.

use std::backtrace::{Backtrace, BacktraceStatus};
use std::panic::PanicHookInfo;

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
    install_panic_hook();

    tracing::info!(
        service = service_name,
        version = env!("CARGO_PKG_VERSION"),
        "tracing initialized"
    );
}

/// Route every panic through tracing so it lands in the structured log
/// stream as an error event; a backtrace rides along when `RUST_BACKTRACE`
/// asks for one. The hook installed before this one (the default stderr
/// writer) still runs afterwards: the subscriber's filter drops the event
/// when `KUTL_LOG` names only other targets, and a panic must never go
/// unreported. Contained panics (a task boundary, `catch_unwind`) report
/// here too: the hook runs before unwinding starts.
fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let report = panic_report(info);
        let backtrace = Backtrace::capture();
        if backtrace.status() == BacktraceStatus::Captured {
            tracing::error!(
                message = %report.message,
                location = %report.location,
                backtrace = %backtrace,
                "panic"
            );
        } else {
            tracing::error!(message = %report.message, location = %report.location, "panic");
        }
        previous(info);
    }));
}

/// Location text when the panic carries none.
const UNKNOWN_PANIC_LOCATION: &str = "unknown";

/// What the panic hook logs: the payload's message and `file:line:column`.
struct PanicReport {
    message: String,
    location: String,
}

/// Read the loggable parts of a panic.
fn panic_report(info: &PanicHookInfo<'_>) -> PanicReport {
    PanicReport {
        message: kutl_core::panic_payload_message(info.payload()),
        location: info.location().map_or_else(
            || UNKNOWN_PANIC_LOCATION.to_owned(),
            |l| format!("{}:{}:{}", l.file(), l.line(), l.column()),
        ),
    }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// The panic hook is process-global, so the tests that swap it run one
    /// at a time: a hook installed by one test while another's probe panic
    /// is in flight would record that panic in the wrong place.
    static HOOK_LOCK: Mutex<()> = Mutex::new(());

    /// The installed hook chains the one it replaced, so a panic still
    /// reaches the previous writer (the default hook's stderr line in a
    /// service) when the subscriber's filter drops the tracing event. Same
    /// process-global discipline as the probe below: a recording hook stands
    /// in for the default one for the duration of one caught panic.
    #[test]
    fn test_install_panic_hook_chains_the_previous_hook() {
        let _serial = HOOK_LOCK.lock().unwrap();
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            recorder.lock().unwrap().push(panic_report(info).message);
        }));
        install_panic_hook();
        let _ = std::panic::catch_unwind(|| panic!("chained probe {}", 11));
        std::panic::set_hook(previous);

        assert!(
            seen.lock().unwrap().iter().any(|m| m == "chained probe 11"),
            "the previous hook still saw the panic"
        );
    }

    /// The hook is process-global, so the probe installs a recording hook
    /// only for the duration of one caught panic and then restores the
    /// previous one; any other panic that lands in the window is recorded
    /// too, which is why the assertion searches rather than indexes.
    #[test]
    fn test_panic_report_carries_message_and_location() {
        let _serial = HOOK_LOCK.lock().unwrap();
        let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let report = panic_report(info);
            recorder
                .lock()
                .unwrap()
                .push((report.message, report.location));
        }));
        let _ = std::panic::catch_unwind(|| panic!("telemetry probe {}", 7));
        std::panic::set_hook(previous);

        let seen = seen.lock().unwrap();
        let (_, location) = seen
            .iter()
            .find(|(message, _)| message == "telemetry probe 7")
            .expect("the probe panic reached the hook");
        assert!(
            location.contains("telemetry.rs:"),
            "location should name this file: {location}"
        );
    }
}
