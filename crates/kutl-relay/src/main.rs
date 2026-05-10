use axum::serve::ListenerExt;
use kutl_relay::config::RelayConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kutl_relay::telemetry::init_tracing("relay");

    let config = RelayConfig::from_env();
    let addr = config.addr();

    let (app, _relay_handle, _flush_handle) = kutl_relay::build_app(config).await?;

    let listener = tokio::net::TcpListener::bind(&addr).await?.tap_io(|tcp| {
        let _ = tcp.set_nodelay(true);
    });
    tracing::info!(%addr, "relay listening");
    axum::serve(listener, app).await?;

    Ok(())
}
