//! End-to-end test for the daemon /metrics endpoint.
//!
//! Spawns `kutl daemon run` in a temp HOME with a custom metrics
//! address, polls /metrics until it responds, and asserts the
//! known gauge name appears in the output.

use std::process::Stdio;
use std::time::Duration;

use serial_test::serial;

/// Number of times to poll the `/metrics` endpoint before giving up.
/// Combined with [`METRICS_POLL_INTERVAL`] this sets the total budget.
/// Generous to absorb CI cold-start latency.
const METRICS_POLL_ITERATIONS: u32 = 60;
/// Delay between `/metrics` polls.
const METRICS_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Per-request timeout for `/metrics` polls.
const METRICS_POLL_REQUEST_TIMEOUT: Duration = Duration::from_millis(200);

#[tokio::test]
#[serial]
async fn test_daemon_serves_metrics_on_localhost() {
    let home = tempfile::tempdir().unwrap();
    let space = tempfile::tempdir().unwrap();

    // Seed an identity by running `kutl init` once. Without an identity
    // file, `kutl daemon run` exits before installing the metrics
    // recorder.
    let init = std::process::Command::new(env!("CARGO_BIN_EXE_kutl"))
        .args(["init", "--relay", "ws://127.0.0.1:1/bogus"])
        .env("HOME", home.path())
        .env_remove("KUTL_HOME")
        .current_dir(space.path())
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "kutl init failed: {}",
        String::from_utf8_lossy(&init.stderr)
    );

    // Pick an unused port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let addr = format!("127.0.0.1:{port}");

    // Spawn the daemon in the foreground.
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_kutl"))
        .args(["daemon", "run"])
        .env("HOME", home.path())
        .env_remove("KUTL_HOME")
        .env("KUTL_DAEMON_METRICS_ADDR", &addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // Wait for the metrics endpoint to come up.
    let url = format!("http://{addr}/metrics");
    let client = reqwest::Client::new();
    let mut last_err = None;
    let mut body = None;
    for _ in 0..METRICS_POLL_ITERATIONS {
        match client
            .get(&url)
            .timeout(METRICS_POLL_REQUEST_TIMEOUT)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                body = Some(resp.text().await.unwrap());
                break;
            }
            Ok(resp) => {
                last_err = Some(format!("status {}", resp.status()));
            }
            Err(e) => {
                last_err = Some(e.to_string());
            }
        }
        tokio::time::sleep(METRICS_POLL_INTERVAL).await;
    }

    let _ = child.kill();
    let _ = child.wait();

    let body = body.unwrap_or_else(|| {
        panic!("metrics endpoint at {url} never responded; last err: {last_err:?}")
    });
    assert!(
        body.contains("kutl_daemon_spaces_registered"),
        "metrics output should include spaces gauge, got:\n{body}"
    );
}
