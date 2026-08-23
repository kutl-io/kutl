//! Faithful real-binary e2e for `kutl document restore`.
//!
//! Boots a real relay + a real `kutl daemon run` supervisor, lets the daemon
//! record two distinct edits of a working file as separate `document log`
//! changes, then restores the file to the EARLIER change and asserts the
//! working file on disk holds the historical content again — and that the
//! restore syncs forward as a new edit (never a history rewrite).
//!
//! Anti-flake: nothing sleeps-and-hopes. Every wait polls the real condition
//! (daemon metrics up, the expected `document log` change COUNT) on a bounded
//! retry loop and panics clearly on timeout. The two file states are made
//! unambiguously different so the historical-content assertion is decisive, and
//! the second edit is written only AFTER change #1 has landed — so the daemon's
//! 200ms debounce records them as two changes rather than coalescing them.

use std::path::Path;
use std::time::{Duration, Instant};

use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::{document_log_change_ids, human_did_of};
use kutl_cli_uxr::harness::{DaemonProcess, RelayProcess};

/// The two working-file states. Deliberately very different so "the file is the
/// historical content again" cannot pass by coincidence.
const V1: &str = "v1";
const V2: &str = "v1 plus a great deal more content, added in the second edit";

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

/// The daemon-backed round-trip: two edits → restore to the first → the working
/// file holds the historical content again → the restore syncs forward as a new
/// change. Exercises the real `kutl` CLI, `kutl daemon run`, and the relay end
/// to end.
#[tokio::test]
async fn document_restore_journey() {
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
            "restore-space",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init failed: {}",
        cli::stderr_str(&init)
    );

    // Authorize the human identity so the daemon's sync to the relay is
    // accepted (the round-trip forward-edit assertion depends on it).
    let human_did =
        human_did_of(&cli::kutl_in(home.path(), space, &["status", "--format", "json"]).await);
    relay.authorize_did(&human_did);

    // Start the daemon and wait until it is ready (metrics endpoint up).
    let _daemon = DaemonProcess::spawn(home.path(), space).await;

    // First edit: write "v1", then wait until the daemon has recorded it as a
    // single change BEFORE the second write — so the debounce records two
    // distinct changes instead of coalescing them.
    let file = space.join("notes.md");
    std::fs::write(&file, V1).expect("write v1");
    await_change_count(&home, space, &file, 1).await;

    // Second edit: overwrite with a clearly different state, then wait for the
    // second change to land.
    std::fs::write(&file, V2).expect("write v2");
    let ids = await_change_count(&home, space, &file, 2).await;

    // `document log` prints newest-first, so the EARLIEST change (the "v1"
    // state) is last in the id list.
    let first_id = ids.last().expect("at least two changes").clone();

    // Restore the working file to the first change.
    let restore = cli::kutl_in(
        home.path(),
        space,
        &[
            "document",
            "restore",
            file.to_str().unwrap(),
            "--to",
            &first_id,
        ],
    )
    .await;
    assert!(
        restore.status.success(),
        "restore --to {first_id} failed: {}",
        cli::stderr_str(&restore)
    );

    // The working file on disk is the historical "v1" content again.
    let on_disk = std::fs::read_to_string(&file).expect("read restored file");
    assert_eq!(
        on_disk, V1,
        "restore --to <first-change> should rewrite the working file to its v1 content, got {on_disk:?}"
    );

    // Stronger: the restore is a FORWARD edit, not a history rewrite — with the
    // daemon still running, `document log` grows a third change (the rewritten
    // file diffed forward and synced end to end).
    let after = await_change_count(&home, space, &file, 3).await;
    assert!(
        after.len() >= 3,
        "restore should sync forward as a new change (>=3 total), saw {}",
        after.len()
    );
}
