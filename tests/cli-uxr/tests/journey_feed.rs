//! Faithful real-binary e2e for `kutl space feed`.
//!
//! Boots a real relay + a real `kutl daemon run` supervisor, makes ≥1 file
//! edit (daemon records it → a registry `document_changes` entry on the relay)
//! and raises ≥1 signal (`kutl signal create`), then asserts that
//! `kutl space feed --format json` returns BOTH kinds of activity.
//!
//! Test 2 exercises the local signals-only offline fallback: after a signal
//! exists locally the relay is made unreachable (killed) and the feed degrades
//! gracefully (exit 0, local signal present, stderr note, no hard error).
//!
//! Anti-flake: nothing sleeps-and-hopes for relay propagation. The edit wait
//! polls `document log` on a bounded loop; `signal create` is synchronous (the
//! relay writes to its `signals` table before returning 200). Both pieces are
//! therefore guaranteed at the relay BEFORE `space feed` is called — which is
//! called ONCE, avoiding cursor-advance races.

use std::path::Path;
use std::time::{Duration, Instant};

use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::{document_log_change_ids, human_did_of};
use kutl_cli_uxr::harness::{DaemonProcess, RelayProcess};

// ── poll constants ────────────────────────────────────────────────────────────

/// How long to wait for the daemon to record a file edit in `document log`.
const CHANGE_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay between `document log` polls.
const CHANGE_POLL: Duration = Duration::from_millis(200);

// ── unique content so assertions can't pass by coincidence ───────────────────

/// File content written in the edit step. Deliberately distinctive so the edit
/// path assertion cannot match an accidental stale entry.
const EDIT_CONTENT: &str = "feed-journey — unique edit content for relay assertion";
/// Signal message — contains a phrase that the assertion can match precisely.
const SIGNAL_MESSAGE: &str = "review-the-feed-deploy-unique-signal-feed-journey";
/// Filename written during the edit step.
const EDIT_FILE: &str = "feed-notes.md";
/// Space name for the online journey.
const ONLINE_SPACE_NAME: &str = "feed-online-space";
/// Space name for the offline journey.
const OFFLINE_SPACE_NAME: &str = "feed-offline-space";
/// Signal message for the offline fallback journey.
const OFFLINE_SIGNAL_MSG: &str = "offline-signal-unique-for-fallback-test-feed-journey";

// ── helpers ───────────────────────────────────────────────────────────────────

/// Poll `kutl document log <file>` until ≥`want` changes appear, returning the
/// change ids (newest-first). Panics with the last log output on timeout.
///
/// Once this returns, the daemon has recorded the edit LOCALLY and, because the
/// relay's `RegisterDocument` ACK is received before the first delta is stored
/// locally, the relay's registry already holds the document entry.
async fn await_change_count(
    home: &TestHome,
    space: &Path,
    file: &Path,
    want: usize,
) -> Vec<String> {
    let deadline = Instant::now() + CHANGE_TIMEOUT;
    let file_arg = file.to_str().expect("utf-8 path");
    loop {
        let out = cli::kutl_in(
            home.path(),
            space,
            &["document", "log", file_arg, "--format", "json"],
        )
        .await;
        let text = cli::stdout_str(&out);
        let ids = document_log_change_ids(&text);
        if ids.len() >= want {
            return ids;
        }
        if Instant::now() >= deadline {
            panic!(
                "daemon did not record {want} change(s) for {} within {CHANGE_TIMEOUT:?};\
                 last log:\n{text}\nstderr:\n{}",
                file.display(),
                cli::stderr_str(&out),
            );
        }
        tokio::time::sleep(CHANGE_POLL).await;
    }
}

// ── Test 1: online feed contains both edits and signals ───────────────────────

/// Online feed shows BOTH a document edit and a signal interleaved.
///
/// Boot relay + daemon, write a file (daemon records it → registry entry on
/// the relay), create a signal (`kutl signal create` — synchronous, relay
/// stores it before returning 200), then call `space feed --format json` ONCE.
/// No cursor-advance race: the feed is called only when both items are already
/// at the relay.
#[tokio::test]
async fn space_feed_online_shows_edits_and_signals() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    // Init the space pointing at the real relay.
    let init = cli::kutl_in(
        home.path(),
        space,
        &[
            "init",
            "--relay",
            &relay.ws_url(),
            "--dir",
            space.to_str().unwrap(),
            "--name",
            ONLINE_SPACE_NAME,
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );

    // Authorize the human identity so the daemon can sync to the relay and so
    // signal create is accepted by the relay's auth gate.
    let human_did =
        human_did_of(&cli::kutl_in(home.path(), space, &["status", "--format", "json"]).await);
    relay.authorize_did(&human_did);

    // Boot the daemon AFTER init (supervisor reads spaces.json at start, so the
    // space worker starts immediately).
    let _daemon = DaemonProcess::spawn(home.path(), space).await;

    // Write the file and poll `document log` until the daemon records ≥1 change.
    // By the time this returns, the relay's registry already holds the
    // document entry (RegisterDocument ACK precedes the local change record).
    let file = space.join(EDIT_FILE);
    std::fs::write(&file, EDIT_CONTENT).expect("write edit file");
    await_change_count(&home, space, &file, 1).await;

    // Create a space-level signal. `signal create` is synchronous end-to-end:
    // the relay calls `project_space_signals` before returning 200, so the
    // signal is in the relay's `signals` table when this call returns.
    let create = cli::kutl_in(
        home.path(),
        space,
        &[
            "signal",
            "create",
            "--kind",
            "question",
            "--message",
            SIGNAL_MESSAGE,
        ],
    )
    .await;
    assert!(
        create.status.success(),
        "signal create failed: {}",
        cli::stderr_str(&create)
    );

    // Both items are now at the relay. Call `space feed --format json` ONCE
    // (cursor starts at 0 → fetches all data). Do NOT call it in a loop: every
    // call advances the per-DID cursor, so a second call would miss already-
    // returned items (no --checkpoint CLI arg to rewind).
    let feed_out = cli::kutl_in(home.path(), space, &["space", "feed", "--format", "json"]).await;
    assert!(
        feed_out.status.success(),
        "space feed failed: {}",
        cli::stderr_str(&feed_out)
    );
    let feed_stdout = cli::stdout_str(&feed_out);
    let feed: serde_json::Value = serde_json::from_str(&feed_stdout).unwrap_or_else(|e| {
        panic!(
            "space feed stdout was not valid JSON ({e}):\n{feed_stdout}\nstderr:\n{}",
            cli::stderr_str(&feed_out)
        )
    });

    // Assert the edit entry: `document_changes` must contain an entry whose
    // `path` matches the file we wrote.
    let changes = feed["document_changes"]
        .as_array()
        .expect("feed json has a 'document_changes' array");
    let edit_entry = changes
        .iter()
        .find(|e| e["path"].as_str().unwrap_or("") == EDIT_FILE)
        .unwrap_or_else(|| {
            panic!(
                "feed 'document_changes' must contain an entry for {EDIT_FILE};\n\
                 document_changes: {changes:?}\nfull feed:\n{feed}"
            )
        });
    assert!(
        !edit_entry["path"].as_str().unwrap_or("").is_empty(),
        "edit entry has a non-empty path: {edit_entry}"
    );

    // Assert the signal entry: `signals` must contain an entry whose payload
    // carries the distinctive signal message. The `--format json` shape for
    // signals is the raw `Signal` proto, so the message lives at
    // `payload.Flag.message` (not at the top-level `message` key).
    let signals = feed["signals"]
        .as_array()
        .expect("feed json has a 'signals' array");
    let signal_entry = signals
        .iter()
        .find(|s| {
            // Check payload.Flag.message first (proto shape from `space feed`).
            let flag_msg = s["payload"]["Flag"]["message"].as_str().unwrap_or("");
            // Also accept a top-level `message` key (signal-list view shape,
            // if the serialization ever changes).
            let top_msg = s["message"].as_str().unwrap_or("");
            flag_msg.contains(SIGNAL_MESSAGE) || top_msg.contains(SIGNAL_MESSAGE)
        })
        .unwrap_or_else(|| {
            panic!(
                "feed 'signals' must contain an entry with message containing {SIGNAL_MESSAGE:?};\n\
                 signals: {signals:?}\nfull feed:\n{feed}"
            )
        });
    assert!(
        !signal_entry["id"].as_str().unwrap_or("").is_empty(),
        "signal entry has a non-empty id: {signal_entry}"
    );
}

// ── Test 2: offline fallback degrades to local signals-only ──────────────────

/// Offline fallback: after the relay is killed, `space feed` exits 0, emits the
/// "relay unreachable" note on stderr, and the local signal appears in the
/// output (human or JSON format depending on the fallback path).
///
/// Setup: init a space with a real relay (so the space config has a valid relay
/// URL), create a signal via the relay-mint route, then pull it into local
/// segments with `signal list --fetch` while the relay is up (`signal create`
/// does not write locally, so the offline fallback needs the record
/// mirrored locally first), then kill the relay and call `space feed`. No daemon
/// is needed — the offline fallback reads local segments only.
#[tokio::test]
async fn space_feed_offline_fallback_shows_local_signals() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    // Init the space with a real relay so the space config records its URL.
    let init = cli::kutl_in(
        home.path(),
        space,
        &[
            "init",
            "--relay",
            &relay.ws_url(),
            "--dir",
            space.to_str().unwrap(),
            "--name",
            OFFLINE_SPACE_NAME,
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );

    // Authorize so `signal create` can transmit to the relay.
    let human_did =
        human_did_of(&cli::kutl_in(home.path(), space, &["status", "--format", "json"]).await);
    relay.authorize_did(&human_did);

    // Create a signal while the relay is up (relay-mint). The record
    // is NOT written locally by create; a follow-up `signal list --fetch` pulls
    // it into local segments so the offline fallback can read it after the relay
    // dies.
    let create = cli::kutl_in(
        home.path(),
        space,
        &[
            "signal",
            "create",
            "--kind",
            "question",
            "--message",
            OFFLINE_SIGNAL_MSG,
        ],
    )
    .await;
    assert!(
        create.status.success(),
        "signal create failed: {}",
        cli::stderr_str(&create)
    );

    // Pull the relay-minted record into local segments while the relay is still
    // reachable — create does not write locally, so without this
    // the offline fallback would find nothing.
    let fetch = cli::kutl_in(
        home.path(),
        space,
        &["signal", "list", "--fetch", "--format", "json"],
    )
    .await;
    assert!(
        fetch.status.success(),
        "signal list --fetch failed: {}",
        cli::stderr_str(&fetch)
    );

    // Kill the relay — dropping its handle closes the process and frees the
    // port, making the relay URL unreachable for subsequent calls.
    drop(relay);

    // Give the OS a moment to reclaim the port so the connect is refused
    // quickly rather than timing out. A single short sleep is acceptable
    // here: we are waiting for a *process* to die (observable, not a
    // distributed state change), and the assertion below is on the
    // graceful-degradation path, not on relay propagation.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // `space feed` with an unreachable relay must exit 0 (graceful degradation).
    let out = cli::kutl_in(home.path(), space, &["space", "feed", "--format", "json"]).await;
    let stdout = cli::stdout_str(&out);
    let stderr = cli::stderr_str(&out);

    assert!(
        out.status.success(),
        "space feed should exit 0 in offline fallback, got non-zero;\
         \nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("relay unreachable"),
        "stderr should contain the offline degradation note; got: {stderr}"
    );

    // The offline fallback renders local signals via `render_local_signals_only`
    // which in `--format json` mode outputs the `signal list` array shape
    // (a top-level JSON array, not the {signals, document_changes} envelope).
    // Assert the signal message appears in stdout regardless of exact shape.
    assert!(
        stdout.contains(OFFLINE_SIGNAL_MSG),
        "offline fallback stdout must contain the local signal message;\n\
         stdout: {stdout}"
    );

    // Stronger: if stdout is a JSON array, assert the entry is present.
    if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&stdout) {
        assert!(
            arr.iter().any(|s| s["message"]
                .as_str()
                .unwrap_or("")
                .contains(OFFLINE_SIGNAL_MSG)),
            "offline fallback JSON array must contain the local signal;\narr: {arr:?}"
        );
    }
}
