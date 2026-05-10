//! kutl-stress — load and stress testing tool for the kutl relay.

use anyhow::Result;
use kutl_core::{SignedDuration, std_duration};
use tracing::info;

mod client;
mod config;
mod report;
mod scenario;

#[tokio::main]
async fn main() -> Result<()> {
    kutl_relay::telemetry::init_tracing("stress");

    let config = config::StressConfig::from_env();

    info!(
        scenario = %config.scenario,
        clients = config.client_count,
        ops = config.ops_per_client,
        relay = %config.relay_url,
        timeout_secs = config.timeout_secs,
        "kutl-stress starting"
    );

    let report = if config.timeout_secs > 0 {
        let timeout = std_duration(SignedDuration::from_secs(
            i64::try_from(config.timeout_secs).unwrap_or(i64::MAX),
        ));
        if let Ok(result) = tokio::time::timeout(timeout, scenario::run(&config)).await {
            result?
        } else {
            tracing::error!(timeout_secs = config.timeout_secs, "scenario timed out");
            std::process::exit(2);
        }
    } else {
        scenario::run(&config).await?
    };

    report.print();

    // Convergence is not expected when:
    // - chaos scenario: relay state is lost on restart
    // - all clients are degraded: nobody drains, so nobody converges
    let convergence_expected =
        config.scenario != "chaos" && config.degraded_count < config.client_count;

    if !report.converged && convergence_expected {
        std::process::exit(1);
    }

    Ok(())
}
