use kutl_cli_uxr::harness::cli::{self, TestHome};

/// Reserved-but-unbuilt commands must fail with a CLEAR "not yet built"
/// message — not an opaque clap "unrecognized subcommand".
#[tokio::test]
async fn reserved_commands_report_not_yet_built() {
    let home = TestHome::new();
    let cwd = tempfile::tempdir().unwrap();

    for args in [
        ["space", "delete"].as_slice(),
        ["space", "config"].as_slice(),
    ] {
        let out = cli::kutl_in(home.path(), cwd.path(), args).await;
        assert!(
            !out.status.success(),
            "reserved `{args:?}` should exit non-zero"
        );
        let msg = format!("{}{}", cli::stdout_str(&out), cli::stderr_str(&out)).to_lowercase();
        assert!(
            msg.contains("not yet") || msg.contains("not built") || msg.contains("reserved"),
            "reserved `{args:?}` should say it is not yet built, got: {msg}"
        );
    }
}
