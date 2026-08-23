use kutl_cli_uxr::harness::RelayProcess;

#[tokio::test]
async fn relay_boots_and_serves() {
    let relay = RelayProcess::spawn().await;
    // A bound ws url means the readiness probe succeeded.
    assert!(relay.ws_url().starts_with("ws://127.0.0.1:"));
    assert!(relay.ws_url().ends_with("/ws"));
    // Dropping `relay` here kills the child (kill_on_drop).
}
