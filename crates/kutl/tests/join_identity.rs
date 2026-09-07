//! Integration test: `kutl join` provisions a DID identity at
//! `$KUTL_HOME/identity.toml` before the relay round-trip, so a joiner is
//! never told to run `kutl init` on their first sync, daemon, or DID-auth
//! call, even when the join itself fails.

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
    let id_path = kutl_home.path().join("identity.toml");
    assert!(
        id_path.exists(),
        "expected identity at {} after `kutl join` even on relay failure",
        id_path.display(),
    );

    // Parse and verify shape.
    let id_text = std::fs::read_to_string(&id_path).expect("read identity.toml");
    assert!(
        id_text.contains("did:key:z"),
        "identity.toml should contain a did:key entry; got: {id_text}",
    );
    assert!(
        id_text.contains("private_key"),
        "identity.toml should contain a private_key field; got: {id_text}",
    );
}
