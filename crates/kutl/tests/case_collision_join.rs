//! Integration test: `kutl join` refuses to join into a directory that
//! already contains case-variant duplicates.

use std::path::Path;
use std::process::Command;
use tempfile::tempdir;

fn is_case_sensitive_fs(dir: &Path) -> bool {
    let lower = dir.join("__kutl_cs_probe__.md");
    let upper = dir.join("__KUTL_CS_PROBE__.md");
    std::fs::write(&lower, "probe").unwrap();
    let sensitive = !upper.exists();
    std::fs::remove_file(&lower).ok();
    sensitive
}

#[test]
fn kutl_join_rejects_case_collision_in_target_dir() {
    let dir = tempdir().unwrap();

    if !is_case_sensitive_fs(dir.path()) {
        eprintln!(
            "skipping kutl_join_rejects_case_collision_in_target_dir: filesystem is case-insensitive"
        );
        return;
    }

    std::fs::write(dir.path().join("foo.md"), "a").unwrap();
    std::fs::write(dir.path().join("Foo.md"), "b").unwrap();

    let bin = env!("CARGO_BIN_EXE_kutl");
    let output = Command::new(bin)
        .args([
            "join",
            "some-space",
            "--dir",
            dir.path().to_str().unwrap(),
            "--relay",
            "ws://127.0.0.1:1",
        ])
        .output()
        .expect("spawn kutl");

    assert!(
        !output.status.success(),
        "expected nonzero exit; stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("case-variant paths cannot coexist"),
        "expected collision message on stderr, got: {stderr}"
    );
    assert!(
        !dir.path().join(".kutlspace").exists(),
        ".kutlspace must not exist after a rejected join"
    );
}
