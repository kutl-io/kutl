use kutl_relay::config::RelayConfig;

/// Platform data directory for a bare-binary relay run with no
/// `KUTL_RELAY_DATA_DIR` set: the OS data directory (e.g.
/// `~/.local/share` on Linux, `~/Library/Application Support` on macOS) under
/// `kutl-relay`, falling back to `./kutl-relay-data` when the OS data directory
/// cannot be resolved.
fn default_relay_data_dir() -> std::path::PathBuf {
    dirs::data_dir().map_or_else(
        || std::path::PathBuf::from("kutl-relay-data"),
        |d| d.join("kutl-relay"),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kutl_relay::telemetry::init_tracing("relay");

    let mut config = RelayConfig::from_env();
    // The data directory is defaulted, not optional. A bare binary
    // with no KUTL_RELAY_DATA_DIR persists to a platform data dir (the Docker
    // image sets the env var to /var/lib/kutl). Logged loudly so operators see
    // where state lives; a RAM-only relay sets KUTL_RELAY_DATA_DIR to a tmpfs.
    let data_dir = config
        .data_dir
        .get_or_insert_with(default_relay_data_dir)
        .clone();
    tracing::info!(data_dir = %data_dir.display(), "relay data directory");
    let addr = config.addr();

    let (app, relay_handle, _flush_handle) = kutl_relay::build_app(config).await?;

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "relay listening");
    kutl_relay::serve(listener, app, relay_handle).await
}
