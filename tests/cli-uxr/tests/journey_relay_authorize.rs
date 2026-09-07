//! Operator authorization journeys (mandatory auth, file contract).
//!
//! Auth is unconditional: a relay boots against a DID allowlist
//! (`authorized_keys`) and rejects every space operation until the caller's
//! DID is in it. The operator interface is the FILE, not a CLI verb: append a
//! DID line (git-ops friendly — config under version control, no
//! ssh-and-type), and the relay live-reloads it on the next auth check.
//! These journeys prove that contract end to end: the fail-closed rejection a
//! user actually sees (naming the file remedy and echoing the DID), the
//! file-edit unblocking the SAME sync with no relay restart, and a malformed
//! line failing CLOSED with a loud relay warning — never silently widening or
//! wedging access.

use kutl_cli_uxr::harness::RelayProcess;
use kutl_cli_uxr::harness::cli::{self, TestHome};
use kutl_cli_uxr::harness::journey::{human_did_of, init_space};

/// Append one raw line to the relay's live `authorized_keys` file — the real
/// operator motion this journey exists to exercise.
fn append_keys_line(relay: &RelayProcess, line: &str) {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(relay.keys_path())
        .expect("open authorized_keys for append");
    writeln!(f, "{line}").expect("append authorized_keys line");
}

/// The full operator arc: sync is REJECTED while the allowlist is empty
/// (fail-closed, with an actionable error naming the file contract and the
/// DID to add), then appending the DID as a line to `authorized_keys` lets
/// the SAME sync succeed immediately — live-reload, no relay restart.
#[tokio::test]
async fn authorized_keys_file_edit_unblocks_sync_via_live_reload() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    // Registration stays open under mandatory auth, so init works
    // before any DID is authorized.
    init_space(&relay, &home, space, "acme").await;

    let status = cli::kutl_in(home.path(), space, &["status", "--format", "json"]).await;
    let did = human_did_of(&status);

    // Give sync real work so the authorization gate is actually exercised.
    std::fs::write(space.join("note.md"), "hello\n").unwrap();

    // FAIL-CLOSED: with an empty allowlist the sync must be rejected, and the
    // rejection must be actionable — the terse relay error names the space,
    // and the diagnostic names the file contract and echoes the DID so the
    // operator can copy-paste the exact line to add.
    let denied = cli::kutl_in(home.path(), space, &["sync"]).await;
    assert!(
        !denied.status.success(),
        "sync must fail while the DID is unauthorized: {}",
        cli::stdout_str(&denied)
    );
    let err = cli::stderr_str(&denied);
    assert!(
        err.contains("not authorized for space"),
        "rejection should carry the relay's denial: {err}"
    );
    assert!(
        err.contains("authorized_keys"),
        "rejection should name the file contract: {err}"
    );
    assert!(
        err.contains(&did),
        "rejection should echo the DID to authorize: {err}"
    );

    // The OPERATOR MOTION: append the DID as a bare line. No CLI verb, no
    // relay restart.
    append_keys_line(&relay, &did);

    // LIVE-RELOAD: the same sync now succeeds.
    let sync = cli::kutl_in(home.path(), space, &["sync"]).await;
    assert!(
        sync.status.success(),
        "sync should succeed after the file edit (live-reload): {}",
        cli::stderr_str(&sync)
    );
    let out = cli::stdout_str(&sync);
    assert!(
        out.contains("Sync complete"),
        "sync should confirm completion: {out}"
    );
}

/// A malformed `authorized_keys` line fails CLOSED — the DID it names stays
/// unauthorized — and the relay logs the drop LOUDLY, so a hand-edit typo is
/// diagnosable instead of silently denying access. A later valid line for the
/// same DID then authorizes it (the malformed line never wedges the file).
#[tokio::test]
async fn malformed_authorized_keys_line_fails_closed_and_logs_loudly() {
    let relay = RelayProcess::spawn().await;
    let home = TestHome::new();
    let space_dir = home.space_dir();
    let space = space_dir.path();

    init_space(&relay, &home, space, "typo-acme").await;
    let status = cli::kutl_in(home.path(), space, &["status", "--format", "json"]).await;
    let did = human_did_of(&status);
    std::fs::write(space.join("note.md"), "hello\n").unwrap();

    // The classic hand-edit typo: an empty scope value. The parser must drop
    // the WHOLE line (fail closed) rather than widen or guess.
    append_keys_line(&relay, &format!("{did} scope="));

    let denied = cli::kutl_in(home.path(), space, &["sync"]).await;
    assert!(
        !denied.status.success(),
        "a malformed line must not authorize its DID: {}",
        cli::stdout_str(&denied)
    );

    // LOUD: the relay's log names the drop and the reason. This is the
    // operator's only signal that the edit didn't take — it must exist.
    let log = relay.read_log();
    assert!(
        log.contains("dropping authorized_keys entry"),
        "relay must log the dropped malformed line: {log}"
    );
    assert!(
        log.contains("malformed scope"),
        "relay log should name the reason: {log}"
    );

    // A valid bare line for the same DID still works afterwards — the
    // malformed line is skipped, not fatal to the file.
    append_keys_line(&relay, &did);
    let sync = cli::kutl_in(home.path(), space, &["sync"]).await;
    assert!(
        sync.status.success(),
        "a valid line after the malformed one should authorize: {}",
        cli::stderr_str(&sync)
    );
}
