//! A real `kutl daemon run` subprocess with an ephemeral metrics port and
//! kill-on-drop.
//!
//! The daemon is the global supervisor (`kutl daemon run`): it reads
//! `$KUTL_HOME/spaces.json`, spawns a watcher/sync worker per registered space,
//! and serves a Prometheus `/metrics` endpoint. Readiness is signalled by that
//! metrics endpoint accepting a TCP connection — the same "the listener is up"
//! probe [`RelayProcess`](crate::harness::RelayProcess) uses for the relay.
//!
//! Because the supervisor only spawns workers for spaces already present in
//! `spaces.json` at boot (new ones arrive via SIGHUP), the caller must
//! `kutl init` the space BEFORE spawning the daemon so its worker starts
//! immediately. Each daemon is given its own ephemeral metrics port (via
//! `KUTL_DAEMON_METRICS_ADDR`) so parallel tests never collide on the default.

use std::net::TcpListener;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::harness::binaries::kutl_bin;

/// How long to wait for the daemon's metrics endpoint to accept connections
/// before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(20);
/// Delay between readiness probes.
const READY_POLL: Duration = Duration::from_millis(100);

/// A running `kutl daemon run` process (the global space supervisor). Killed
/// when dropped.
pub struct DaemonProcess {
    _child: Child,
    /// The ephemeral metrics port this daemon binds; readiness polls it.
    metrics_port: u16,
}

/// Grab an ephemeral free port by binding :0 and reading the assigned port.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read local addr")
        .port()
}

impl DaemonProcess {
    /// Spawn `kutl daemon run` against `home` (`$KUTL_HOME`, which supplies the
    /// signing identity and the `spaces.json` registry). The space must already
    /// be `kutl init`-ed under this home so the supervisor spawns its worker at
    /// boot. Waits until the metrics endpoint is accepting connections.
    ///
    /// `cwd` is the space directory — harmless for the global supervisor, but
    /// kept consistent with the CLI runner so the process's view of the tree
    /// matches the test's.
    pub async fn spawn(home: &Path, cwd: &Path) -> Self {
        let metrics_port = free_port();

        let mut cmd = Command::new(kutl_bin());
        cmd.args(["daemon", "run"])
            .env("KUTL_HOME", home)
            .env(
                "KUTL_DAEMON_METRICS_ADDR",
                format!("127.0.0.1:{metrics_port}"),
            )
            .env("KUTL_LOG", "warn")
            .current_dir(cwd)
            .kill_on_drop(true)
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let child = cmd.spawn().expect("spawn kutl daemon run");
        let daemon = Self {
            _child: child,
            metrics_port,
        };
        daemon.await_ready().await;
        daemon
    }

    /// Poll a TCP connect to the metrics port until the daemon is serving. A
    /// successful connect means the axum `/metrics` listener is bound, which
    /// the daemon does at startup before running the supervisor — so the
    /// process is alive and past init. The functional gate (the space worker
    /// having ingested a file) is asserted separately by the test polling
    /// `kutl document log`.
    async fn await_ready(&self) {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            if tokio::net::TcpStream::connect(("127.0.0.1", self.metrics_port))
                .await
                .is_ok()
            {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "kutl daemon did not become ready on metrics port {}",
                    self.metrics_port
                );
            }
            tokio::time::sleep(READY_POLL).await;
        }
    }
}
