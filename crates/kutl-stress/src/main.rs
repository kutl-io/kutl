//! kutl-stress — load and stress testing tool for the kutl relay.
//!
//! Configuration is env-driven (`KUTL_STRESS_*`, see `config.rs`). Relay
//! authentication is mandatory: every connect performs the did:key
//! challenge-response using the identity at `$KUTL_HOME/identity.json`
//! (generated on first run), so that DID must be enrolled in the relay's
//! `authorized_keys` allowlist — locally by appending the DID as a line to
//! that file (the relay live-reloads it); a k8s deployment seeds the
//! allowlist `ConfigMap` at provision time.
//!
//! `kutl-stress mint-identity` prints the identity JSON to stdout (creating
//! it first if missing) for provisioning flows that need the identity file
//! without running a scenario.

use anyhow::{Result, bail};
use kutl_core::{SignedDuration, std_duration};
use tracing::info;

mod client;
mod config;
mod report;
mod scenario;

/// Argv-selected run mode (the scenario itself is env-selected).
#[derive(Debug, PartialEq, Eq)]
enum Mode {
    /// Run the env-configured stress scenario (no arguments).
    Run,
    /// Print the local identity JSON to stdout, minting it if missing.
    MintIdentity,
}

/// Parse argv (excluding argv[0]) into a [`Mode`].
fn parse_mode(args: &[String]) -> Result<Mode> {
    match args {
        [] => Ok(Mode::Run),
        [arg] if arg == "mint-identity" => Ok(Mode::MintIdentity),
        other => bail!(
            "unknown arguments {other:?}\n\
             usage: kutl-stress [mint-identity]\n\
             \n\
             (no args)      run the KUTL_STRESS_* env-configured scenario;\n\
             \x20              connects authenticated as $KUTL_HOME/identity.json\n\
             \x20              (enroll that DID as a line in the relay's\n\
             \x20              authorized_keys file — it live-reloads)\n\
             mint-identity  print the identity JSON to stdout, minting it first\n\
             \x20              if missing (for provisioning; stdout stays pure)"
        ),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // mint-identity is handled BEFORE init_tracing: tracing JSON goes to
    // stdout, and provisioning scripts pipe this command's stdout straight
    // into an identity file.
    if parse_mode(&args)? == Mode::MintIdentity {
        let identity = kutl_client::load_or_generate()?;
        println!("{}", serde_json::to_string_pretty(&identity)?);
        return Ok(());
    }

    kutl_relay::telemetry::init_tracing("stress");

    let config = config::StressConfig::from_env();

    // One shared identity for all stress clients (client_name distinguishes
    // them); every connect re-runs the did:key challenge-response so
    // reconnects survive a relay restart wiping its in-memory token store.
    let identity = kutl_client::load_or_generate()?;

    info!(
        scenario = %config.scenario,
        clients = config.client_count,
        ops = config.ops_per_client,
        relay = %config.relay_url,
        timeout_secs = config.timeout_secs,
        did = %identity.did,
        "kutl-stress starting"
    );

    let report = if config.timeout_secs > 0 {
        let timeout = std_duration(SignedDuration::from_secs(
            i64::try_from(config.timeout_secs).unwrap_or(i64::MAX),
        ));
        if let Ok(result) = tokio::time::timeout(timeout, scenario::run(&config, &identity)).await {
            result?
        } else {
            tracing::error!(timeout_secs = config.timeout_secs, "scenario timed out");
            std::process::exit(2);
        }
    } else {
        scenario::run(&config, &identity).await?
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn test_parse_mode_no_args_runs_scenario() {
        assert_eq!(parse_mode(&args(&[])).unwrap(), Mode::Run);
    }

    #[test]
    fn test_parse_mode_mint_identity() {
        assert_eq!(
            parse_mode(&args(&["mint-identity"])).unwrap(),
            Mode::MintIdentity
        );
    }

    #[test]
    fn test_parse_mode_unknown_arg_is_error() {
        let err = parse_mode(&args(&["bogus"])).unwrap_err();
        assert!(err.to_string().contains("usage"), "got: {err}");
        // Extra args after mint-identity are also rejected.
        assert!(parse_mode(&args(&["mint-identity", "x"])).is_err());
    }
}
