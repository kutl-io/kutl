//! Integration test: `kutl init` refuses to create a space in a directory
//! that already contains case-variant duplicates.

use std::path::Path;
use std::process::Command;
use tempfile::tempdir;

/// Probe whether the filesystem hosting `dir` is case-sensitive by writing
/// a sentinel file with one casing and checking whether the opposite-case
/// lookup finds the same file. Returns true on case-sensitive (Linux
/// ext4/btrfs, macOS case-sensitive APFS), false on case-insensitive
/// (macOS default APFS, most Windows NTFS volumes).
fn is_case_sensitive_fs(dir: &Path) -> bool {
    let lower = dir.join("__kutl_cs_probe__.md");
    let upper = dir.join("__KUTL_CS_PROBE__.md");
    std::fs::write(&lower, "probe").unwrap();
    let sensitive = !upper.exists();
    std::fs::remove_file(&lower).ok();
    sensitive
}

#[test]
fn kutl_init_rejects_case_collision_in_space_root() {
    let dir = tempdir().unwrap();

    if !is_case_sensitive_fs(dir.path()) {
        eprintln!(
            "skipping kutl_init_rejects_case_collision_in_space_root: filesystem is case-insensitive"
        );
        return;
    }

    std::fs::write(dir.path().join("foo.md"), "a").unwrap();
    std::fs::write(dir.path().join("Foo.md"), "b").unwrap();

    let bin = env!("CARGO_BIN_EXE_kutl");
    let output = Command::new(bin)
        .args([
            "init",
            "--dir",
            dir.path().to_str().unwrap(),
            "--relay",
            "ws://127.0.0.1:1",
            "--name",
            "test-space",
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
        !dir.path().join(".kutl").exists(),
        ".kutl/ must not exist after a rejected init"
    );
}
