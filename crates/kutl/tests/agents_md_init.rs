//! Integration tests for the AGENTS.md managed-section behavior of
//! `kutl init` and `kutl init --update`.

use std::path::Path;
use std::process::Command;
use tempfile::tempdir;

fn kutl_bin() -> &'static str {
    env!("CARGO_BIN_EXE_kutl")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).expect("read")
}

fn run_init(workdir: &Path, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(kutl_bin());
    cmd.current_dir(workdir)
        .env("KUTL_HOME", workdir)
        .arg("init")
        .args(args);
    cmd.output().expect("spawn kutl")
}

fn make_git_repo(root: &Path) {
    let out = Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(root)
        .output()
        .expect("git init");
    assert!(out.status.success(), "git init failed: {out:?}");
}

#[test]
fn test_init_writes_agents_md_managed_block_at_repo_root() {
    let dir = tempdir().unwrap();
    make_git_repo(dir.path());

    // Use a fake relay URL — `kutl init` writes config files without
    // contacting the relay (Unreachable → local-only space).
    let out = run_init(
        dir.path(),
        &["--relay", "http://localhost:0", "--name", "demo"],
    );
    assert!(
        out.status.success(),
        "init failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let agents = dir.path().join("AGENTS.md");
    assert!(agents.exists(), "AGENTS.md was not created");

    let body = read(&agents);
    assert!(
        body.contains("<!-- kutl:start v="),
        "managed-block start marker missing: {body}"
    );
    assert!(
        body.contains("<!-- kutl:end -->"),
        "managed-block end marker missing"
    );
    assert!(body.contains("# Working in a kutl-aware repo"));
}

#[test]
fn test_init_appends_when_agents_md_exists_without_markers() {
    let dir = tempdir().unwrap();
    make_git_repo(dir.path());
    let agents = dir.path().join("AGENTS.md");
    std::fs::write(&agents, "# Repo guide\n\nExisting content.\n").unwrap();

    let out = run_init(
        dir.path(),
        &["--relay", "http://localhost:0", "--name", "demo"],
    );
    assert!(out.status.success());

    let body = read(&agents);
    assert!(body.starts_with("# Repo guide\n\nExisting content.\n"));
    assert!(body.contains("<!-- kutl:start v="));
}

#[test]
fn test_init_update_replaces_stale_block_in_place() {
    let dir = tempdir().unwrap();
    make_git_repo(dir.path());
    let agents = dir.path().join("AGENTS.md");
    // Hand-craft a stale block (use a version older than the current
    // crate version — 0.0.1 is guaranteed older than any released build).
    let stale = "# Repo guide\n\n\
        <!-- kutl:start v=0.0.1 - managed by `kutl init`, edits inside this block may be overwritten -->\n\
        old body\n\
        <!-- kutl:end -->\n\
        \nFooter\n";
    std::fs::write(&agents, stale).unwrap();

    // First, init without --update. The space does not yet exist, so init
    // will create it. The relay will be unreachable and we get a local-only
    // space. AGENTS.md already has a block so apply_block returns Stale
    // (Default mode) and must NOT modify the file.
    let out = run_init(
        dir.path(),
        &["--relay", "http://localhost:0", "--name", "demo"],
    );
    assert!(out.status.success());
    assert_eq!(
        read(&agents),
        stale,
        "default mode must not modify the file"
    );

    // Now run with --update against the already-initialized tree. The
    // short-circuit detects .kutlspace at the repo root and refreshes
    // AGENTS.md only, then exits.
    let out = run_init(
        dir.path(),
        &[
            "--relay",
            "http://localhost:0",
            "--name",
            "demo",
            "--update",
        ],
    );
    assert!(
        out.status.success(),
        "init --update failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let body = read(&agents);
    assert!(
        body.contains("# Repo guide\n"),
        "prefix content lost: {body}"
    );
    assert!(body.contains("Footer"), "suffix content lost: {body}");
    assert!(
        !body.contains("v=0.0.1"),
        "stale sentinel not replaced: {body}"
    );
    assert!(
        !body.contains("old body"),
        "stale body not replaced: {body}"
    );
}

#[test]
fn test_join_writes_agents_md_managed_block() {
    // `kutl join` resolves the git repo root and writes AGENTS.md before
    // making any network call. We use a bare-name join against an
    // unreachable relay. The join itself will fail (no relay at
    // localhost:0), but AGENTS.md should still be written because the
    // write happens before the network round-trip.
    let dir = tempdir().unwrap();
    make_git_repo(dir.path());

    let mut cmd = Command::new(kutl_bin());
    cmd.current_dir(dir.path())
        .env("KUTL_HOME", dir.path())
        .args(["join", "demo", "--relay", "http://localhost:0"]);
    let out = cmd.output().expect("spawn kutl");

    let agents = dir.path().join("AGENTS.md");
    assert!(
        agents.exists(),
        "AGENTS.md not written by `kutl join`; stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let body = read(&agents);
    assert!(body.contains("<!-- kutl:start v="));
    assert!(body.contains("# Working in a kutl-aware repo"));
}

#[test]
fn test_init_writes_agents_md_outside_git_repo() {
    // Non-git directory. AGENTS.md should land at the dir's root.
    let dir = tempdir().unwrap();
    // Deliberately do NOT call make_git_repo here.

    let out = run_init(
        dir.path(),
        &["--relay", "http://localhost:0", "--name", "demo"],
    );
    assert!(
        out.status.success(),
        "init failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let agents = dir.path().join("AGENTS.md");
    assert!(agents.exists(), "AGENTS.md was not created in non-git dir");

    let body = read(&agents);
    assert!(body.contains("<!-- kutl:start v="));
    assert!(body.contains("# Working in a kutl-aware repo"));
}

#[test]
fn test_watch_refuses_on_stale_incompatible_when_non_tty() {
    let dir = tempdir().unwrap();
    make_git_repo(dir.path());

    let out = run_init(
        dir.path(),
        &["--relay", "http://localhost:0", "--name", "demo"],
    );
    assert!(out.status.success());

    // Find AGENTS.md (it lives at the repo root, NOT inside the kutl subfolder).
    let agents = dir.path().join("AGENTS.md");
    let body = read(&agents);
    let current = env!("CARGO_PKG_VERSION");
    let rewritten = body.replace(&format!("v={current}"), "v=0.0.1");
    std::fs::write(&agents, rewritten).unwrap();

    let out = Command::new(kutl_bin())
        .current_dir(dir.path())
        .env("KUTL_HOME", dir.path())
        .arg("watch")
        .output()
        .expect("spawn watch");

    assert!(
        !out.status.success(),
        "watch should refuse on stale-incompatible AGENTS.md in non-TTY"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("incompatible") || stderr.contains("kutl init --update"),
        "expected refusal message; got: {stderr}"
    );
}

#[test]
fn test_watch_refuses_on_absent_agents_md_when_kutlspace_present_non_tty() {
    // Initialize, then DELETE AGENTS.md. kutl watch should refuse
    // because the local space is present but the contract is missing.
    let dir = tempdir().unwrap();
    make_git_repo(dir.path());

    let out = run_init(
        dir.path(),
        &["--relay", "http://localhost:0", "--name", "demo"],
    );
    assert!(out.status.success());

    let agents = dir.path().join("AGENTS.md");
    std::fs::remove_file(&agents).expect("remove AGENTS.md");

    let out = Command::new(kutl_bin())
        .current_dir(dir.path())
        .env("KUTL_HOME", dir.path())
        .arg("watch")
        .output()
        .expect("spawn watch");

    assert!(
        !out.status.success(),
        "watch should refuse on absent AGENTS.md when local space is present"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("no agent instructions") || stderr.contains("kutl init --update"),
        "expected absent-state refusal message; got: {stderr}"
    );
}

#[test]
fn test_watch_silent_when_no_local_space() {
    // Plain non-git directory with no .kutlspace. kutl watch should
    // not fail the AGENTS.md check (there's no kutl context to enforce).
    // The watch command itself may still fail for other reasons (no
    // space to subscribe to), but it shouldn't fail the AGENTS.md
    // check specifically.
    let dir = tempdir().unwrap();
    // Deliberately do NOT init or create .kutlspace.

    let mut child = Command::new(kutl_bin())
        .current_dir(dir.path())
        .env("KUTL_HOME", dir.path())
        .arg("watch")
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn watch");
    std::thread::sleep(std::time::Duration::from_millis(300));
    let _ = child.kill();
    let out = child.wait_with_output().expect("wait");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("no agent instructions"),
        "watch should not enforce AGENTS.md absent the local space; got: {stderr}"
    );
}
