//! Integration test: `kutl join` provisions a DID identity at
//! `$KUTL_HOME/identity.json` even when the relay round-trip fails.
//!
//! Regression for the "joiner gets told to run `kutl init`" bug:
//! before this fix, `cmd_join` wrote `.kutlspace`, `.kutl/space.json`,
//! and AGENTS.md but never called `identity::load_or_generate`. A user
//! who joined an existing space then hit "no identity found — run
//! `kutl init` first" on their first `kutl sync` / `kutl daemon` /
//! DID-auth attempt — pointing them at the wrong command. The fix
//! makes `cmd_join` symmetric with `cmd_init`.

use std::process::Command;
use tempfile::tempdir;

#[test]
fn kutl_join_provisions_identity_even_when_relay_unreachable() {
    let workdir = tempdir().expect("tempdir for workspace");
    let kutl_home = tempdir().expect("tempdir for KUTL_HOME");

    let bin = env!("CARGO_BIN_EXE_kutl");
    // Bare-name join against an unreachable relay. The network call
    // will fail, but identity provisioning happens before the
    // round-trip, so the file should exist regardless.
    let output = Command::new(bin)
        .args([
            "join",
            "no-such-space",
            "--dir",
            workdir.path().to_str().unwrap(),
            "--relay",
            "ws://127.0.0.1:1",
        ])
        .env("KUTL_HOME", kutl_home.path())
        .output()
        .expect("spawn kutl");

    // We expect the join to fail (unreachable relay).
    assert!(
        !output.status.success(),
        "expected nonzero exit (relay unreachable); stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Identity must have been written before the network attempt.
    let id_path = kutl_home.path().join("identity.json");
    assert!(
        id_path.exists(),
        "expected identity at {} after `kutl join` even on relay failure",
        id_path.display(),
    );

    // Parse and verify shape.
    let id_text = std::fs::read_to_string(&id_path).expect("read identity.json");
    assert!(
        id_text.contains("did:key:z"),
        "identity.json should contain a did:key entry; got: {id_text}",
    );
    assert!(
        id_text.contains("private_key"),
        "identity.json should contain a private_key field; got: {id_text}",
    );
}
