//! Faithful real-binary e2e for `kutl document blame`.
//!
//! Boots a real relay + a real `kutl daemon run` supervisor, lets the daemon
//! author a multi-line working file, then runs the real `kutl document blame`
//! binary and asserts correct per-line authorship end to end.
//!
//! In this single-daemon harness EVERY edit flows through the one daemon
//! session-agent bound to `config.author_did`, so every line is attributed to
//! the daemon owner's DID. This is the documented "single-owner /
//! out-of-protocol" attribution boundary, and it makes the assertion decisive
//! and non-flaky. Multi-author per-line distinction and the restore-remote path
//! are pinned by deterministic in-process unit tests; this journey proves the
//! end-to-end wiring, not the per-author distinction.
//!
//! Anti-flake: nothing sleeps-and-hopes. The wait polls the real condition (the
//! `document log` change COUNT) on a bounded retry loop and panics clearly on
//! timeout, so a flake is diagnosable rather than mysterious.

use std::path::Path;
use std::time::{Duration, Instant};

use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::{document_log_change_ids, human_did_of};
use kutl_cli_uxr::harness::{DaemonProcess, RelayProcess};

/// The multi-line working-file content. Three lines plus a trailing newline —
/// the trailing newline yields NO phantom 4th line (matching
/// `segment_blame_lines`).
const NOTES: &str = "line one\nline two\nline three\n";
/// The three lines of [`NOTES`], in order — the expected `text` of each row.
const LINES: [&str; 3] = ["line one", "line two", "line three"];

/// How long to wait for the daemon to record an expected number of changes.
const CHANGE_TIMEOUT: Duration = Duration::from_secs(20);
/// Delay between `document log` polls while waiting for changes to land.
const CHANGE_POLL: Duration = Duration::from_millis(100);

/// Poll `kutl document log <file>` until it reports at least `want` changes,
/// returning the change ids (oldest-last). Panics on timeout with the last
/// observed log so a flake is diagnosable rather than mysterious.
async fn await_change_count(
    home: &TestHome,
    space: &Path,
    file: &Path,
    want: usize,
) -> Vec<String> {
    let deadline = Instant::now() + CHANGE_TIMEOUT;
    let file_arg = file.to_str().expect("utf-8 file path");
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
                "daemon did not record {want} change(s) for {} within {CHANGE_TIMEOUT:?}; last log:\n{text}\nstderr:\n{}",
                file.display(),
                cli::stderr_str(&out),
            );
        }
        tokio::time::sleep(CHANGE_POLL).await;
    }
}

/// The daemon-backed round-trip: the daemon authors a multi-line file → run
/// `kutl document blame --format json` → every line is attributed to the daemon
/// owner's DID, with the expected line numbers and text. Exercises the real
/// `kutl` CLI, `kutl daemon run`, and the relay end to end.
#[tokio::test]
async fn document_blame_journey() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    // Init the space (registers it in $KUTL_HOME/spaces.json so the daemon
    // supervisor spawns its worker at boot).
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
            "blame-space",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );

    // Authorize the human identity so the daemon's sync to the relay is
    // accepted (and so its authored ops carry that durable DID).
    let human_did =
        human_did_of(&cli::kutl_in(home.path(), space, &["status", "--format", "json"]).await);
    relay.authorize_did(&human_did);

    // Start the daemon and wait until it is ready (metrics endpoint up).
    let _daemon = DaemonProcess::spawn(home.path(), space).await;

    // The daemon authors a multi-line file, then we wait until it has recorded
    // + saved the edit (so the `.dt` sidecar exists with the author map + tips).
    let file = space.join("notes.md");
    std::fs::write(&file, NOTES).expect("write notes");
    await_change_count(&home, space, &file, 1).await;

    // Blame as JSON.
    let out = cli::kutl_in(
        home.path(),
        space,
        &[
            "document",
            "blame",
            file.to_str().unwrap(),
            "--format",
            "json",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "blame --format json failed: {}",
        cli::stderr_str(&out)
    );

    // An array of `{line, author, text}`, one row per line.
    let json = cli::json(&out);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("blame json should be an array, got {json}"));
    assert_eq!(
        rows.len(),
        LINES.len(),
        "blame should have one row per line (no phantom trailing line), got {json}"
    );

    // Every row: expected line number + text, and attributed to the daemon
    // owner's DID — the decisive e2e attribution assertion.
    for (idx, (row, want_text)) in rows.iter().zip(LINES).enumerate() {
        let want_line = (idx + 1) as u64;
        assert_eq!(
            row["line"].as_u64(),
            Some(want_line),
            "row {idx} line number should be {want_line}, got {json}"
        );
        assert_eq!(
            row["text"].as_str(),
            Some(want_text),
            "row {idx} text should be {want_text:?}, got {json}"
        );
        assert_eq!(
            row["author"].as_str(),
            Some(human_did.as_str()),
            "row {idx} should be attributed to the daemon owner {human_did}, got {json}"
        );
    }

    // Human format prints `{author}\t{line}\t{text}` per line; assert it carries
    // the owner DID and each line's text.
    let human = cli::kutl_in(
        home.path(),
        space,
        &[
            "document",
            "blame",
            file.to_str().unwrap(),
            "--format",
            "human",
        ],
    )
    .await;
    assert!(
        human.status.success(),
        "blame --format human failed: {}",
        cli::stderr_str(&human)
    );
    let human_out = cli::stdout_str(&human);
    assert!(
        human_out.contains(&human_did),
        "human blame should carry the owner DID {human_did}, got:\n{human_out}"
    );
    for want_text in LINES {
        assert!(
            human_out.contains(want_text),
            "human blame should carry line {want_text:?}, got:\n{human_out}"
        );
    }
}
