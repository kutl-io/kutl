//! `kutl daemon start/status/stop` lifecycle journeys (coverage-lane B8) —
//! background daemon management and its pidfile hygiene: status when stopped,
//! double start, stale-pidfile clearing, and the PID-reuse identity guard
//! (`$KUTL_HOME/daemon.pid` + the `daemon.pid.id` start-time sidecar).
//!
//! No relay is needed: the supervisor stays alive with an unreachable relay
//! (workers retry/respawn; the supervisor never exits on worker failure), and
//! the pidfile machinery under test is purely local.

use std::path::Path;

use kutl_cli_uxr::harness::cli::{self, TestHome};

/// A port assumed unbound, forcing `kutl init` into local-only mode.
const UNREACHABLE_RELAY_PORT: u16 = 19997;

/// Env var the daemon reads for its metrics bind address. Passed per-test with
/// an ephemeral port so parallel journeys never contend for the default.
const METRICS_ADDR_ENV: &str = "KUTL_DAEMON_METRICS_ADDR";

/// Best-effort SIGKILL backstop for the background daemon.
///
/// `kutl daemon start` spawns a DETACHED process (the CLI's child, not the
/// harness's), so tokio's `kill_on_drop` cannot cover it. The journey's real
/// teardown is `kutl daemon stop`; this guard only prevents a leaked daemon if
/// an assertion panics mid-test. Killing an already-stopped PID fails silently.
struct DaemonPidGuard(u32);

impl Drop for DaemonPidGuard {
    fn drop(&mut self) {
        // Quiet on the happy path: the daemon is normally already stopped, so
        // `kill` would print "No such process" to the test's stderr.
        let _ = std::process::Command::new("kill")
            .args(["-9", &self.0.to_string()])
            .stderr(std::process::Stdio::null())
            .status();
    }
}

/// An ephemeral loopback metrics address (bind :0, read the assigned port).
fn free_metrics_addr() -> String {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read local addr")
        .port();
    format!("127.0.0.1:{port}")
}

/// A PID guaranteed dead: spawn a trivial child and reap it (the just-freed
/// PID is not instantly recycled by the OS).
fn dead_pid() -> u32 {
    let mut child = std::process::Command::new("true")
        .spawn()
        .expect("spawn a trivial child");
    let pid = child.id();
    child.wait().expect("reap the child");
    pid
}

/// The PID from a `Daemon started (PID <n>)` line.
fn parse_started_pid(out: &str) -> u32 {
    out.split("(PID ")
        .nth(1)
        .and_then(|rest| rest.split(')').next())
        .and_then(|pid| pid.trim().parse().ok())
        .unwrap_or_else(|| panic!("no parsable PID in daemon start output: {out}"))
}

/// `kutl init` a local-only space (provisions the signing identity `daemon
/// run` requires and registers one space for the supervisor to watch).
async fn init_local_space(home: &TestHome, space: &Path) {
    let relay_url = format!("ws://127.0.0.1:{UNREACHABLE_RELAY_PORT}");
    let init = cli::kutl_in(
        home.path(),
        space,
        &[
            "init",
            "--relay",
            &relay_url,
            "--dir",
            space.to_str().unwrap(),
            "--name",
            "acme",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init (local-only) failed: {}",
        cli::stderr_str(&init)
    );
}

/// `daemon status --format json` → the `daemon` sub-struct's `(running, pid)`.
async fn daemon_status(home: &TestHome, cwd: &Path) -> (bool, Option<u64>) {
    let out = cli::kutl_in(home.path(), cwd, &["daemon", "status", "--format", "json"]).await;
    assert!(
        out.status.success(),
        "daemon status failed: {}",
        cli::stderr_str(&out)
    );
    let v = cli::json(&out);
    (
        v["daemon"]["running"].as_bool().expect("running is a bool"),
        v["daemon"]["pid"].as_u64(),
    )
}

/// With no daemon ever started: status must say "not running" (human render
/// pins the load-bearing line — coverage-lane B10 for `render_daemon_status`),
/// and `stop` must be a friendly no-op, not an error.
#[tokio::test]
async fn status_and_stop_with_no_daemon() {
    let home = TestHome::new();
    let cwd = tempfile::tempdir().unwrap();

    // Human render (default format): the daemon liveness line names the home
    // it inspected so the user can tell WHICH daemon is not running.
    let status = cli::kutl_in(home.path(), cwd.path(), &["daemon", "status"]).await;
    assert!(
        status.status.success(),
        "daemon status failed: {}",
        cli::stderr_str(&status)
    );
    let out = cli::stdout_str(&status);
    assert!(
        out.contains("daemon: not running"),
        "human status should say not running: {out}"
    );
    assert!(
        out.contains(home.path().to_str().unwrap()),
        "human status should name $KUTL_HOME: {out}"
    );

    let (running, pid) = daemon_status(&home, cwd.path()).await;
    assert!(!running, "no daemon was started");
    assert!(pid.is_none(), "no PID when not running");

    let stop = cli::kutl_in(home.path(), cwd.path(), &["daemon", "stop"]).await;
    assert!(
        stop.status.success(),
        "stop with no daemon must be a friendly no-op: {}",
        cli::stderr_str(&stop)
    );
    assert!(
        cli::stdout_str(&stop).contains("daemon is not running"),
        "stop should say the daemon is not running: {}",
        cli::stdout_str(&stop)
    );
}

/// The full background lifecycle: start (pidfile + identity sidecar written,
/// status flips to running in both formats), start again (already-running
/// notice, same PID), stop (graceful, pidfile + sidecar removed, status flips
/// back), stop again (no-op).
#[tokio::test]
async fn start_status_stop_lifecycle() {
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();
    init_local_space(&home, space).await;

    let metrics_addr = free_metrics_addr();
    let metrics_env = [(METRICS_ADDR_ENV, Some(metrics_addr.as_str()))];

    // Start: the spawned `daemon run` inherits the CLI's env, so the ephemeral
    // metrics address rides along.
    let start = cli::kutl_in_env(home.path(), space, &["daemon", "start"], &metrics_env).await;
    assert!(
        start.status.success(),
        "daemon start failed: {}",
        cli::stderr_str(&start)
    );
    let start_out = cli::stdout_str(&start);
    assert!(
        start_out.contains("Daemon started (PID "),
        "start should announce the PID: {start_out}"
    );
    assert!(
        start_out.contains("log: "),
        "start should name the log file: {start_out}"
    );
    let pid = parse_started_pid(&start_out);
    let _guard = DaemonPidGuard(pid);

    // Pidfile hygiene: the PID file holds the announced PID and the identity
    // sidecar (process start time) exists so later signals can detect reuse.
    let pid_file = home.path().join("daemon.pid");
    let id_file = home.path().join("daemon.pid.id");
    let recorded: u32 = std::fs::read_to_string(&pid_file)
        .expect("daemon.pid written")
        .trim()
        .parse()
        .expect("daemon.pid holds a PID");
    assert_eq!(recorded, pid, "pidfile must record the announced PID");
    let identity = std::fs::read_to_string(&id_file).expect("daemon.pid.id written");
    assert!(
        identity.starts_with("starttime="),
        "identity sidecar carries the start time: {identity}"
    );
    assert!(
        home.path().join("logs").join("daemon.log").exists(),
        "the announced log file exists"
    );

    // Status flips to running — JSON and the human line (B10).
    let (running, status_pid) = daemon_status(&home, space).await;
    assert!(running, "daemon should be running after start");
    assert_eq!(status_pid, Some(u64::from(pid)), "status reports the PID");
    let human = cli::kutl_in(home.path(), space, &["daemon", "status"]).await;
    let human_out = cli::stdout_str(&human);
    assert!(
        human_out.contains(&format!("daemon: running (PID {pid}")),
        "human status should show the running daemon: {human_out}"
    );

    // Start again: a clear already-running notice, same PID, exit 0.
    let again = cli::kutl_in_env(home.path(), space, &["daemon", "start"], &metrics_env).await;
    assert!(
        again.status.success(),
        "second start must not fail: {}",
        cli::stderr_str(&again)
    );
    assert!(
        cli::stdout_str(&again).contains(&format!("Daemon is already running (PID {pid})")),
        "second start should say already running: {}",
        cli::stdout_str(&again)
    );
    let recorded_again: u32 = std::fs::read_to_string(&pid_file)
        .expect("daemon.pid still present")
        .trim()
        .parse()
        .expect("daemon.pid holds a PID");
    assert_eq!(
        recorded_again, pid,
        "double start must not rewrite the pidfile"
    );

    // Stop: graceful SIGTERM (stop_daemon blocks until the process exits), and
    // both the pidfile and the identity sidecar are removed.
    let stop = cli::kutl_in(home.path(), space, &["daemon", "stop"]).await;
    assert!(
        stop.status.success(),
        "daemon stop failed: {}",
        cli::stderr_str(&stop)
    );
    assert!(
        cli::stdout_str(&stop).contains(&format!("daemon stopped (PID {pid})")),
        "stop should confirm the graceful shutdown: {}",
        cli::stdout_str(&stop)
    );
    assert!(!pid_file.exists(), "stop must remove the pidfile");
    assert!(!id_file.exists(), "stop must remove the identity sidecar");
    let (running, _) = daemon_status(&home, space).await;
    assert!(!running, "status must flip back after stop");

    // Stop again: friendly no-op.
    let stop_again = cli::kutl_in(home.path(), space, &["daemon", "stop"]).await;
    assert!(
        stop_again.status.success(),
        "stop after stop must be a no-op: {}",
        cli::stderr_str(&stop_again)
    );
    assert!(
        cli::stdout_str(&stop_again).contains("daemon is not running"),
        "second stop should say not running: {}",
        cli::stdout_str(&stop_again)
    );
}

/// A pidfile pointing at a dead PID is stale state, not a running daemon:
/// status reports not-running, stop clears the file (saying so), and start
/// clears it silently and boots a fresh daemon.
#[tokio::test]
async fn stale_pidfile_is_cleared() {
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();
    init_local_space(&home, space).await;

    let pid_file = home.path().join("daemon.pid");
    let stale = dead_pid();
    std::fs::write(&pid_file, stale.to_string()).unwrap();

    // Status: a dead PID must not be reported as a running daemon.
    let status = cli::kutl_in(home.path(), space, &["daemon", "status"]).await;
    assert!(
        cli::stdout_str(&status).contains("daemon: not running"),
        "a stale pidfile must not read as running: {}",
        cli::stdout_str(&status)
    );

    // Stop: names the stale state it cleared.
    let stop = cli::kutl_in(home.path(), space, &["daemon", "stop"]).await;
    assert!(
        stop.status.success(),
        "stop on a stale pidfile failed: {}",
        cli::stderr_str(&stop)
    );
    assert!(
        cli::stdout_str(&stop).contains("daemon is not running (stale PID file removed)"),
        "stop should say it removed the stale pidfile: {}",
        cli::stdout_str(&stop)
    );
    assert!(!pid_file.exists(), "stop must remove the stale pidfile");

    // Start: the stale-pid guard clears the leftover and boots fresh.
    std::fs::write(&pid_file, dead_pid().to_string()).unwrap();
    let metrics_addr = free_metrics_addr();
    let start = cli::kutl_in_env(
        home.path(),
        space,
        &["daemon", "start"],
        &[(METRICS_ADDR_ENV, Some(metrics_addr.as_str()))],
    )
    .await;
    assert!(
        start.status.success(),
        "start over a stale pidfile failed: {}",
        cli::stderr_str(&start)
    );
    let start_out = cli::stdout_str(&start);
    assert!(
        start_out.contains("Daemon started (PID "),
        "start should boot fresh over a stale pidfile: {start_out}"
    );
    let pid = parse_started_pid(&start_out);
    let _guard = DaemonPidGuard(pid);

    let stop = cli::kutl_in(home.path(), space, &["daemon", "stop"]).await;
    assert!(
        cli::stdout_str(&stop).contains(&format!("daemon stopped (PID {pid})")),
        "cleanup stop should confirm shutdown: {}",
        cli::stdout_str(&stop)
    );
}

/// The PID-reuse identity guard, driven through the CLI: a pidfile pointing at
/// a LIVE process whose recorded identity sidecar does not match must never be
/// signalled — `stop` reports the recycled PID, clears the stale state, and
/// leaves the unrelated process untouched (verify_daemon_identity /
/// identity_permits_signal).
#[tokio::test]
async fn recycled_pid_is_never_signalled() {
    let home = TestHome::new();
    let cwd = tempfile::tempdir().unwrap();

    // An unrelated live process standing in for a recycled PID.
    let mut bystander = tokio::process::Command::new("sleep")
        .arg("300")
        .kill_on_drop(true)
        .spawn()
        .expect("spawn bystander sleep");
    let pid = bystander.id().expect("bystander pid");

    let pid_file = home.path().join("daemon.pid");
    let id_file = home.path().join("daemon.pid.id");
    std::fs::write(&pid_file, pid.to_string()).unwrap();
    // A start time that cannot match the just-spawned bystander.
    std::fs::write(&id_file, "starttime=1").unwrap();

    // KNOWN GAP (captured in the coverage-lane notes, asserted as CURRENT
    // behavior): `daemon status` checks liveness only — no identity verify —
    // so a recycled PID reads as a running daemon here even though `stop`
    // (correctly) refuses to signal it below.
    let (running, status_pid) = daemon_status(&home, cwd.path()).await;
    assert!(
        running,
        "current behavior: status trusts liveness alone for a recycled PID"
    );
    assert_eq!(status_pid, Some(u64::from(pid)));

    // Stop: the identity guard refuses to signal, names the recycled PID, and
    // clears the stale pidfile + sidecar.
    let stop = cli::kutl_in(home.path(), cwd.path(), &["daemon", "stop"]).await;
    assert!(
        stop.status.success(),
        "stop on a recycled PID failed: {}",
        cli::stderr_str(&stop)
    );
    assert!(
        cli::stdout_str(&stop).contains(&format!("daemon is not running (PID {pid} was recycled")),
        "stop should report the recycled PID: {}",
        cli::stdout_str(&stop)
    );
    assert!(!pid_file.exists(), "stop must clear the recycled pidfile");
    assert!(!id_file.exists(), "stop must clear the identity sidecar");

    // The load-bearing assertion: the unrelated process was NOT signalled.
    assert!(
        bystander.try_wait().expect("try_wait bystander").is_none(),
        "the identity guard must never signal an unrelated live process"
    );

    // With the stale state cleared, status flips to not running.
    let (running, _) = daemon_status(&home, cwd.path()).await;
    assert!(!running, "status is clean once the stale state is cleared");
}
