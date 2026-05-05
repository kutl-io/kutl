//! Prometheus exporter installation and HTTP server for `/metrics`.
//!
//! Single public entry point — [`install_metrics_and_serve`] — used by
//! both the CLI daemon (`kutl daemon run`) and the desktop tray app
//! (`kutl-desktop`). The two binaries are mutually exclusive at runtime
//! (paired PID files), so they never compete for the same port.
//!
//! Bound to `127.0.0.1` by default. Address override via the
//! `KUTL_DAEMON_METRICS_ADDR` env var. Bind failure is logged and
//! ignored — `/metrics` is diagnostic, not load-bearing.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::Router;
use axum::routing::get;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing::{info, warn};

/// Default address for the metrics endpoint.
const DEFAULT_METRICS_ADDR: &str = "127.0.0.1:9091";

/// Env var that overrides the metrics bind address.
const METRICS_ADDR_ENV: &str = "KUTL_DAEMON_METRICS_ADDR";

/// Install the Prometheus recorder and spawn the HTTP `/metrics` server.
///
/// Resolves the bind address from [`METRICS_ADDR_ENV`] or the default,
/// installs the global Prometheus recorder, and spawns an axum task
/// serving `/metrics`. The spawned task runs for the daemon's lifetime.
///
/// Calling twice in one process returns an error from the underlying
/// recorder install (recorder install is global state).
///
/// Bind failure logs a warning and returns `Ok(())` — `/metrics` is
/// diagnostic, not load-bearing; the daemon continues without it.
pub async fn install_metrics_and_serve() -> Result<()> {
    let addr = resolve_addr()?;
    let handle = install_recorder()?;
    serve(handle, addr).await;
    Ok(())
}

fn install_recorder() -> Result<PrometheusHandle> {
    PrometheusBuilder::new()
        .install_recorder()
        .context("failed to install prometheus recorder")
}

fn resolve_addr() -> Result<SocketAddr> {
    let raw = std::env::var(METRICS_ADDR_ENV).unwrap_or_else(|_| DEFAULT_METRICS_ADDR.to_owned());
    raw.parse()
        .with_context(|| format!("invalid {METRICS_ADDR_ENV}={raw}"))
}

async fn serve(handle: PrometheusHandle, addr: SocketAddr) {
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move { handle.render() }
        }),
    );

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!(addr = %addr, error = %e, "failed to bind metrics port; metrics endpoint unavailable");
            return;
        }
    };

    info!(addr = %addr, "metrics endpoint listening");

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            warn!(error = %e, "metrics server exited");
        }
    });
}
