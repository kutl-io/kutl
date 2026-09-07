use std::path::PathBuf;
use std::process::Command;

use kutl_core::{Boundary, Document};

fn kutl_cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kutl"))
}

/// The tracked working-tree path every fixture uses.
const REL_PATH: &str = "notes/foo.md";

/// A registered space under an isolated `$KUTL_HOME` with one tracked
/// document at [`REL_PATH`], its history saved from `doc` — the scaffolding
/// `kutl document log` resolves through (cwd-first walk + state.json).
struct SpaceFixture {
    _home: tempfile::TempDir,
    kutl_home: PathBuf,
    space_root: PathBuf,
    space_id: String,
    document_id: String,
}

fn space_with_tracked_doc(doc: &Document) -> SpaceFixture {
    use kutl_daemon::state::{DaemonState, DocEntry};

    let home = tempfile::tempdir().unwrap();
    let kutl_home = home.path().join(".kutl-home");
    std::fs::create_dir_all(&kutl_home).unwrap();

    // A space keyed by a real UUID (segment dirs are keyed by space id),
    // INSIDE the declared KUTL_HOME — a set KUTL_HOME is a hard resolution
    // boundary and the cwd-first walk refuses spaces outside it.
    let space_id = uuid::Uuid::from_u128(0x1234).to_string();
    let space_root = kutl_home.join("space");
    std::fs::create_dir_all(&space_root).unwrap();
    kutl_client::SpaceConfig {
        space_id: space_id.clone(),
        relay_url: "ws://127.0.0.1:9100/ws".into(),
    }
    .save(&space_root)
    .unwrap();

    // Register the space under the isolated $KUTL_HOME.
    let mut reg = kutl_client::SpaceRegistry::default();
    reg.add(space_root.to_str().unwrap());
    reg.save(&kutl_home.join("spaces.toml")).unwrap();

    // The tracked document: internal history store under its id, the
    // working-tree file at the tracked rel path, and the state.json mapping.
    let document_id = uuid::Uuid::from_u128(0xABCD).to_string();
    let dt_path = space_root
        .join(".kutl")
        .join("docs")
        .join(format!("{document_id}.dt"));
    doc.save(&dt_path).unwrap();
    std::fs::create_dir_all(space_root.join("notes")).unwrap();
    std::fs::write(space_root.join(REL_PATH), doc.content()).unwrap();

    let mut state = DaemonState::default();
    state.documents.insert(
        REL_PATH.to_owned(),
        DocEntry {
            id: document_id.clone(),
            confirmed: true,
            inode: None,
            last_written_hash: None,
        },
    );
    state.save(&space_root.join(".kutl")).unwrap();

    SpaceFixture {
        _home: home,
        kutl_home,
        space_root,
        space_id,
        document_id,
    }
}

/// Run `kutl document log <REL_PATH>` from the fixture space root.
fn run_log(fixture: &SpaceFixture) -> std::process::Output {
    kutl_cli()
        .env("KUTL_HOME", &fixture.kutl_home)
        .current_dir(&fixture.space_root)
        .args(["document", "log", REL_PATH])
        .output()
        .unwrap()
}

/// Build a CREATED signal record attached to `document_id` at `timestamp_ms`.
fn signal_record(
    space_id: &str,
    document_id: &str,
    signal_id: &str,
    record_id: &str,
    timestamp_ms: i64,
) -> kutl_proto::sync::Signal {
    use kutl_proto::sync::{FlagPayload, Hlc, Signal, SignalEventType, signal};
    let mut s = Signal {
        id: signal_id.into(),
        space_id: space_id.into(),
        document_id: Some(document_id.into()),
        timestamp: timestamp_ms,
        record_id: record_id.into(),
        payload: Some(signal::Payload::Flag(FlagPayload::default())),
        hlc: Some(Hlc {
            physical_ms: u64::try_from(timestamp_ms).unwrap(),
            logical: 0,
            actor: vec![0u8; 16],
        }),
        ..Default::default()
    };
    s.set_event(SignalEventType::Created);
    s
}

#[test]
fn test_log_shows_changes() {
    let mut doc = Document::new();
    let agent = doc.register_agent("alice").unwrap();
    doc.edit(agent, "alice", "add greeting", Boundary::Explicit, |ctx| {
        ctx.insert(0, "hello world")
    })
    .unwrap();
    doc.edit(agent, "alice", "fix typo", Boundary::Auto, |ctx| {
        ctx.delete(5..11)?;
        ctx.insert(5, " there")
    })
    .unwrap();
    let fixture = space_with_tracked_doc(&doc);

    let output = run_log(&fixture);
    assert!(
        output.status.success(),
        "kutl log failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();

    // Most recent first.
    let fix_pos = stdout.find("fix typo").expect("missing 'fix typo'");
    let greet_pos = stdout.find("add greeting").expect("missing 'add greeting'");
    assert!(
        fix_pos < greet_pos,
        "most recent change should appear first"
    );

    assert!(stdout.contains("Author: alice"));
    assert!(stdout.contains("explicit"));
    assert!(stdout.contains("auto"));
}

#[test]
fn test_log_no_changes_recorded() {
    // A tracked document with an empty history renders the notice, not an
    // error — the file is real, it just has nothing recorded yet.
    let fixture = space_with_tracked_doc(&Document::new());

    let output = run_log(&fixture);
    assert!(
        output.status.success(),
        "kutl log failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("no changes recorded"), "got: {stdout}");
}

#[test]
fn test_log_accepts_working_tree_path_and_interleaves_signals() {
    use kutl_daemon::signal_store::DaemonSignalStore;

    let mut doc = Document::new();
    let agent = doc.register_agent("alice").unwrap();
    doc.edit(agent, "alice", "seed the note", Boundary::Explicit, |ctx| {
        ctx.insert(0, "hello")
    })
    .unwrap();
    let change_ms = doc.changes()[0].timestamp;
    let fixture = space_with_tracked_doc(&doc);

    // A signal record attached to this document, one second AFTER the change,
    // so the interleaved timeline must list it before the change.
    let signal_ms = change_ms + 1000;
    {
        let mut store =
            DaemonSignalStore::open(&fixture.space_root, uuid::Uuid::from_u128(0x1234)).unwrap();
        store
            .append(&signal_record(
                &fixture.space_id,
                &fixture.document_id,
                "sig-1",
                "rec-1",
                signal_ms,
            ))
            .unwrap();
    } // drop releases the store lock before the CLI reads

    let output = run_log(&fixture);
    assert!(
        output.status.success(),
        "kutl document log failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();

    // The change entry is present.
    assert!(
        stdout.contains("seed the note"),
        "log must show the change entry:\n{stdout}"
    );
    // The signal record is interleaved into the log.
    assert!(
        stdout.contains("sig-1"),
        "log must show the interleaved signal record:\n{stdout}"
    );
    // Ordering: the newer signal appears BEFORE the older change (most-recent
    // first, matching the git-log-style order).
    let sig_pos = stdout.find("sig-1").expect("signal present");
    let change_pos = stdout.find("seed the note").expect("change present");
    assert!(
        sig_pos < change_pos,
        "newer signal should precede older change:\n{stdout}"
    );
}

#[test]
fn test_log_rejects_dt_paths_and_untracked_files() {
    // Internal .dt paths left the command contract: rejected with a pointer
    // at the working-tree path, whether or not the file exists.
    let output = kutl_cli()
        .args(["document", "log", "/tmp/does_not_exist.dt"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("internal storage"),
        "should reject .dt paths: {stderr}"
    );

    // An untracked working-tree file inside a real space errors with the
    // tracking remedy.
    let fixture = space_with_tracked_doc(&Document::new());
    std::fs::write(fixture.space_root.join("notes/other.md"), "x").unwrap();
    let output = kutl_cli()
        .env("KUTL_HOME", &fixture.kutl_home)
        .current_dir(&fixture.space_root)
        .args(["document", "log", "notes/other.md"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("not tracked"),
        "should name the tracking remedy: {stderr}"
    );
}
