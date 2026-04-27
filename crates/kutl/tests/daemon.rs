//! Integration tests for global daemon start/stop/status lifecycle.
//!
//! These tests spawn the real `kutl` binary with `HOME` set to a temp
//! directory, so the PID file (`~/.kutl/daemon.pid`) and space registry
//! are isolated from the real user environment.
//!
//! The daemon will fail to connect to a relay (bogus URL) but still runs
//! and responds to signals, which is enough to test lifecycle management.

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use serial_test::serial;

fn kutl_cli(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kutl"));
    cmd.env("HOME", home);
    cmd
}

/// Create a minimal initialized space in a temp directory.
///
/// Sets `HOME` so the space registry goes to the fake home.
fn init_space(root: &Path, home: &Path) {
    let output = kutl_cli(home)
        .args(["init", "--relay", "ws://127.0.0.1:1/bogus"])
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Read the PID from `~/.kutl/daemon.pid`.
fn read_pid(home: &Path) -> Option<u32> {
    let path = home.join(".kutl").join("daemon.pid");
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Check if a process is alive.
fn is_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Wait briefly for a condition, polling every 100ms up to the given duration.
fn wait_for(timeout: Duration, f: impl Fn() -> bool) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Create isolated home and space directories.
fn setup() -> (tempfile::TempDir, tempfile::TempDir) {
    let home_dir = tempfile::tempdir().unwrap();
    let space_dir = tempfile::tempdir().unwrap();
    // Ensure ~/.kutl exists.
    std::fs::create_dir_all(home_dir.path().join(".kutl")).unwrap();
    (home_dir, space_dir)
}

/// Stop the daemon (best-effort cleanup).
fn cleanup(home: &Path) {
    let _ = kutl_cli(home).args(["daemon", "stop"]).output();
}

#[test]
#[serial]
fn test_daemon_start_stop_lifecycle() {
    let (home_dir, space_dir) = setup();
    let home = home_dir.path();
    let root = space_dir.path();
    init_space(root, home);

    // Start the daemon.
    let output = kutl_cli(home).args(["daemon", "start"]).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "start failed: {}{}",
        stdout,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("Daemon started"), "stdout: {stdout}");

    // Verify PID file exists and process is alive.
    let pid = read_pid(home).expect("PID file should exist after start");
    assert!(
        wait_for(Duration::from_secs(2), || is_alive(pid)),
        "daemon process should be alive"
    );

    // Stop the daemon.
    let output = kutl_cli(home).args(["daemon", "stop"]).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("stopped") || stdout.contains("killed"),
        "stdout: {stdout}"
    );

    // Verify PID file is removed and process is dead.
    assert!(
        wait_for(Duration::from_secs(2), || !is_alive(pid)),
        "daemon should be dead after stop"
    );
    assert!(read_pid(home).is_none(), "PID file should be removed");
}

#[test]
#[serial]
fn test_daemon_status_running() {
    let (home_dir, space_dir) = setup();
    let home = home_dir.path();
    let root = space_dir.path();
    init_space(root, home);

    // Start daemon first.
    let output = kutl_cli(home).args(["daemon", "start"]).output().unwrap();
    assert!(output.status.success());

    // Check status.
    let output = kutl_cli(home).args(["daemon", "status"]).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("running"), "stdout: {stdout}");
    assert!(stdout.contains("PID"), "stdout: {stdout}");

    // Cleanup.
    cleanup(home);
}

#[test]
#[serial]
fn test_daemon_status_not_running() {
    let (home_dir, _space_dir) = setup();
    let home = home_dir.path();

    let output = kutl_cli(home).args(["daemon", "status"]).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("not running"), "stdout: {stdout}");
}

#[test]
#[serial]
fn test_daemon_start_already_running() {
    let (home_dir, space_dir) = setup();
    let home = home_dir.path();
    let root = space_dir.path();
    init_space(root, home);

    // Start first.
    let output = kutl_cli(home).args(["daemon", "start"]).output().unwrap();
    assert!(output.status.success());

    // Start again — should report already running.
    let output = kutl_cli(home).args(["daemon", "start"]).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("already running"), "stdout: {stdout}");

    // Cleanup.
    cleanup(home);
}

#[test]
#[serial]
fn test_daemon_stop_not_running() {
    let (home_dir, _space_dir) = setup();
    let home = home_dir.path();

    let output = kutl_cli(home).args(["daemon", "stop"]).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("not running"), "stdout: {stdout}");
}
