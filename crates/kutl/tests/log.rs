use std::process::Command;

use kutl_core::{Boundary, Document};

fn kutl_cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kutl"))
}

#[test]
fn test_log_shows_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.dt");

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
    doc.save(&path).unwrap();

    let output = kutl_cli()
        .args(["log", path.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(output.status.success(), "kutl log failed");
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
fn test_log_empty_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.dt");

    let doc = Document::new();
    doc.save(&path).unwrap();

    // Remove the sidecar so there are no changes.
    let sidecar = path.with_extension("dt.changes");
    let _ = std::fs::remove_file(&sidecar);

    let output = kutl_cli()
        .args(["log", path.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("no changes recorded"));
}

#[test]
fn test_log_missing_file() {
    let output = kutl_cli()
        .args(["log", "/tmp/does_not_exist.dt"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("failed to load"));
}
