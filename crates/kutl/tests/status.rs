//! CLI integration tests for `kutl status`.
//!
//! Each test sets `HOME` to a temp directory and invokes the binary,
//! parses the resulting JSON, and asserts on shape.

use std::process::Command;

use serial_test::serial;

fn kutl_cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kutl"))
}

#[test]
#[serial]
fn test_status_json_schema_minimal() {
    let dir = tempfile::tempdir().unwrap();

    let output = kutl_cli()
        .args(["status", "--format", "json"])
        .env("HOME", dir.path())
        .env_remove("KUTL_HOME")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "status --format json exited nonzero: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("output should be valid JSON");

    // Schema: top-level keys must be present.
    for key in [
        "kutl_home",
        "daemon",
        "desktop",
        "identity",
        "spaces",
        "relays",
    ] {
        assert!(v.get(key).is_some(), "missing top-level key: {key}");
    }
    assert!(v["spaces"].is_array(), "spaces must be array");
    assert!(v["relays"].is_array(), "relays must be array");
    assert_eq!(v["daemon"]["running"], false);
}
