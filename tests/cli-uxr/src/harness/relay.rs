//! A real `kutl-relay` subprocess with an ephemeral port and kill-on-drop.
//!
//! Relay auth is mandatory. The relay is pointed at a
//! file-based DID allowlist (`KUTL_RELAY_AUTHORIZED_KEYS_FILE`) that the
//! harness creates fresh and initially EMPTY. An empty allowlist satisfies the
//! relay's boot assert (a keys file is a valid no-DB authz
//! mode) — it just means no DID is authorized yet. The file is live-reloaded on
//! every auth check, so tests append DIDs at runtime via
//! [`RelayProcess::authorize_did`] and they take effect immediately.

use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::harness::binaries::relay_bin;

/// How long to wait for the relay to accept TCP connections before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(20);
/// Delay between readiness probes.
const READY_POLL: Duration = Duration::from_millis(100);

/// A running `kutl-relay` process with auth required. Killed when dropped.
pub struct RelayProcess {
    _child: Child,
    port: u16,
    _data_dir: tempfile::TempDir,
    /// Path to the live-reloaded DID allowlist. Owned by the data-dir tempdir,
    /// so it lives as long as the process. DIDs are appended at runtime.
    keys_path: PathBuf,
    /// The relay's captured stdout+stderr (structured JSON logs).
    log_path: PathBuf,
}

/// Grab an ephemeral free port by binding :0 and reading the assigned port.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read local addr")
        .port()
}

impl RelayProcess {
    /// Spawn a relay with a fresh, initially-empty
    /// file-based DID allowlist. Callers authorize DIDs at runtime with
    /// [`RelayProcess::authorize_did`] (the file is live-reloaded per auth
    /// check, so appends take effect immediately).
    pub async fn spawn() -> Self {
        Self::spawn_with_env(&[]).await
    }

    /// [`RelayProcess::spawn`] with extra relay environment variables layered
    /// on top of the harness defaults (e.g. `KUTL_RELAY_UX_URL`, which the
    /// device-flow journey sets to control the verification URL the relay
    /// hands to `kutl auth login`).
    pub async fn spawn_with_env(envs: &[(&str, &str)]) -> Self {
        let port = free_port();
        let data_dir = tempfile::tempdir().expect("relay data dir");

        // Create the empty allowlist inside the data-dir tempdir so it lives as
        // long as the process. An empty file satisfies the boot assert.
        let keys_path = data_dir.path().join("authorized_keys");
        std::fs::File::create(&keys_path).expect("create authorized_keys file");

        // The relay's structured logs (stdout+stderr) go to a file so
        // journeys can assert on operator-facing warnings (e.g. a malformed
        // authorized_keys line being dropped loudly).
        let log_path = data_dir.path().join("relay.log");
        let log_file = std::fs::File::create(&log_path).expect("create relay log file");
        let log_file_err = log_file.try_clone().expect("clone relay log handle");

        let mut cmd = Command::new(relay_bin());
        cmd.env("KUTL_RELAY_HOST", "127.0.0.1")
            .env("KUTL_RELAY_PORT", port.to_string())
            .env("KUTL_RELAY_NAME", "cli-uxr-relay")
            .env("KUTL_RELAY_DATA_DIR", data_dir.path())
            .env("KUTL_RELAY_AUTHORIZED_KEYS_FILE", &keys_path)
            .env("KUTL_LOG", "warn")
            .kill_on_drop(true)
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file_err));
        for (name, value) in envs {
            cmd.env(name, value);
        }

        let child = cmd.spawn().expect("spawn kutl-relay");
        let relay = Self {
            _child: child,
            port,
            _data_dir: data_dir,
            keys_path,
            log_path,
        };
        relay.await_ready().await;
        relay
    }

    /// Authorize `did` for all spaces (a bare authorized_keys line). The relay
    /// re-reads the file per auth check, so this takes effect immediately.
    pub fn authorize_did(&self, did: &str) {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&self.keys_path)
            .expect("open authorized_keys for append");
        writeln!(f, "{}", did.trim()).expect("append authorized did");
    }

    /// Path to the relay's live-reloaded `authorized_keys` file, for journeys
    /// that exercise the operator file-edit contract (append a DID line;
    /// the relay picks it up on the next auth check) beyond the
    /// [`RelayProcess::authorize_did`] convenience rail.
    pub fn keys_path(&self) -> &Path {
        &self.keys_path
    }

    /// The relay's captured log output so far (structured JSON lines).
    /// Journeys assert operator-facing warnings here — e.g. that a malformed
    /// `authorized_keys` line is dropped LOUDLY, never silently.
    pub fn read_log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// The relay's HTTP base URL (`http://127.0.0.1:<port>`), for the
    /// relay-served HTTP surfaces (invites, relay-policy, web pages).
    pub fn http_base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Mint a relay bearer token for a fresh, throwaway did:key identity via
    /// the product challenge flow (`POST /auth/challenge` → sign →
    /// `POST /auth/verify` — the same `kutl_client::authenticate` the CLI
    /// uses). Returns `(did, token)`.
    ///
    /// Identity auth is allowlist-free: ANY valid did:key can mint a bearer
    /// (the allowlist gates *space* operations, not identity). Call
    /// [`RelayProcess::authorize_did`] with the returned DID when the token
    /// will touch a space-scoped surface.
    pub async fn mint_bearer(&self) -> (String, String) {
        let identity = kutl_client::Identity::generate();
        let signing_key = identity
            .decode_signing_key()
            .expect("decode generated signing key");
        let token = kutl_client::authenticate(&self.ws_url(), &identity.did, &signing_key)
            .await
            .expect("did:key challenge flow against the relay");
        (identity.did, token)
    }

    /// Mint an invite code for `space_id` via the relay's operator rail
    /// (`POST /invites` — the JSON endpoint behind the web UI's "Generate
    /// invite" button; there is no CLI verb for minting). Returns the code.
    pub async fn create_invite(&self, space_id: &str) -> String {
        let resp = reqwest::Client::new()
            .post(format!("{}/invites", self.http_base()))
            .json(&serde_json::json!({ "space_id": space_id }))
            .send()
            .await
            .expect("POST /invites");
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::CREATED,
            "invite creation should return 201"
        );
        let body: serde_json::Value = resp.json().await.expect("invite response json");
        body["code"]
            .as_str()
            .unwrap_or_else(|| panic!("invite response has a code: {body}"))
            .to_owned()
    }

    /// Poll a TCP connect until the relay is accepting (it binds, then serves —
    /// a successful connect means the listener is up and routes are live).
    async fn await_ready(&self) {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        loop {
            if tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                .await
                .is_ok()
            {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("kutl-relay did not become ready on port {}", self.port);
            }
            tokio::time::sleep(READY_POLL).await;
        }
    }

    /// The WebSocket URL a client uses (`ws://127.0.0.1:<port>/ws`).
    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}/ws", self.port)
    }
}
