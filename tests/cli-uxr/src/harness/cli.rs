//! Isolated `KUTL_HOME` and a runner for the real `kutl` CLI binary.

use std::path::Path;
use std::process::Output;

use tokio::process::Command;

use crate::harness::binaries::kutl_bin;

/// A throwaway `KUTL_HOME` directory (deleted on drop).
pub struct TestHome {
    dir: tempfile::TempDir,
}

impl TestHome {
    /// Create a fresh, empty home directory.
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("create KUTL_HOME"),
        }
    }

    /// Path to pass as `KUTL_HOME`.
    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    /// A directory for a SPACE, created INSIDE this home. A set
    /// `$KUTL_HOME` is a hard boundary: space-scoped verbs resolve only
    /// spaces within it (fail closed outside), so journeys place their
    /// spaces under the home they run against — mirroring the
    /// agent-workspace layout the boundary exists to protect.
    pub fn space_dir(&self) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("space-")
            .tempdir_in(self.dir.path())
            .expect("create space dir under KUTL_HOME")
    }
}

impl Default for TestHome {
    fn default() -> Self {
        Self::new()
    }
}

/// Run `kutl <args>` with `KUTL_HOME=home`, working directory `cwd`, capturing
/// stdout/stderr. `cwd` matters for space-scoped commands (they resolve the
/// space from the current directory).
pub async fn kutl_in(home: &Path, cwd: &Path, args: &[&str]) -> Output {
    kutl_in_env(home, cwd, args, &[]).await
}

/// [`kutl_in`] with per-invocation environment overrides. Each `(name, value)`
/// pair is set for `Some(value)` and explicitly REMOVED for `None` — removal
/// matters for journeys asserting the no-env error path of a verb whose
/// default comes from the environment, which must not inherit a value from
/// the developer's shell.
pub async fn kutl_in_env(
    home: &Path,
    cwd: &Path,
    args: &[&str],
    envs: &[(&str, Option<&str>)],
) -> Output {
    let mut cmd = Command::new(kutl_bin());
    cmd.args(args)
        .env("KUTL_HOME", home)
        // Keep init non-interactive: no git repo under a tempdir, so the
        // subfolder prompt never triggers; but pin a value defensively.
        .env("KUTL_LOG", "warn")
        .current_dir(cwd);
    for (name, value) in envs {
        match value {
            Some(v) => cmd.env(name, v),
            None => cmd.env_remove(name),
        };
    }
    cmd.output().await.expect("run kutl")
}

/// Decoded stdout.
pub fn stdout_str(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Decoded stderr.
pub fn stderr_str(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Parse stdout as JSON (panics with the raw output on failure).
pub fn json(out: &Output) -> serde_json::Value {
    let s = stdout_str(out);
    serde_json::from_str(&s).unwrap_or_else(|e| panic!("stdout was not valid json ({e}): {s}"))
}
