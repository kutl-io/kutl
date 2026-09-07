//! The `$KUTL_HOME` walk boundary, shared by every bounded ancestor walk.
//!
//! A set `KUTL_HOME` declares the process's workspace: the space walk-up
//! and the git-repo detection both refuse to cross it. The two rules every
//! such walk must agree on live here, once — how the ceiling is resolved
//! from the environment, and how containment is compared. The comparison
//! canonicalizes the ceiling so symlinks on either side cannot defeat it
//! (macOS `/tmp` and `/var` are symlinks into `/private`, so an env-var
//! ceiling and a resolved cwd can name the same directory through
//! different paths); a walk that hand-rolled a raw `starts_with` here
//! would give a different verdict than its sibling in the same process.

use std::path::{Path, PathBuf};

/// A canonicalized walk boundary. `None` inside means unbounded (no
/// `KUTL_HOME` declared — a human's shell, where walks behave like git's).
pub(crate) struct Ceiling(Option<PathBuf>);

impl Ceiling {
    /// The process-wide boundary: `$KUTL_HOME`, canonicalized.
    pub(crate) fn from_env() -> Self {
        let raw = std::env::var_os("KUTL_HOME").map(PathBuf::from);
        Self::explicit(raw.as_deref())
    }

    /// An explicit boundary (tests, and callers that carry their own).
    /// A ceiling that cannot be canonicalized (not on disk yet) is used
    /// as given — refusing it outright would turn a missing directory
    /// into an unbounded walk, the fail-open direction.
    pub(crate) fn explicit(ceiling: Option<&Path>) -> Self {
        Self(ceiling.map(|c| c.canonicalize().unwrap_or_else(|_| c.to_path_buf())))
    }

    /// Whether `p` sits within the boundary. `p` must already be
    /// canonical — both walks canonicalize their start before walking, so
    /// every candidate they test is.
    pub(crate) fn contains(&self, p: &Path) -> bool {
        match &self.0 {
            Some(c) => p.starts_with(c),
            None => true,
        }
    }
}
