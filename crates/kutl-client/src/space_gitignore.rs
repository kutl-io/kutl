//! Helper for writing the `.gitignore` file inside a kutl space.
//!
//! Per RFD 0060, every kutl space has a `.gitignore` at its root that hides
//! everything inside the space from git EXCEPT the `.gitignore` and
//! `.kutlspace` system files. This protects the space from `git checkout`,
//! `git pull`, and other tree-rewriting git operations.

use std::path::Path;

use anyhow::{Context, Result};

/// File name for the kutl-managed gitignore.
pub const GITIGNORE_FILENAME: &str = ".gitignore";

/// The contents written to `.gitignore` in every kutl space.
///
/// Excludes everything except itself and `.kutlspace` so that git can track
/// the system files (which propagate via git, not via the relay) without
/// touching document content.
pub const GITIGNORE_CONTENTS: &str = "\
# This directory is managed by kutl (real-time document collaboration).
# Document content is not tracked by git — it lives in the kutl relay.
# To participate, install kutl (https://kutl.io) and run `kutl join`.
*
!.gitignore
!.kutlspace
";

/// Write the kutl-managed `.gitignore` to `<space_root>/.gitignore`.
///
/// Overwrites any existing file. The destination directory must already exist.
pub fn write_space_gitignore(space_root: &Path) -> Result<()> {
    let path = space_root.join(GITIGNORE_FILENAME);
    std::fs::write(&path, GITIGNORE_CONTENTS)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_write_space_gitignore_writes_expected_contents() {
        let dir = TempDir::new().unwrap();
        write_space_gitignore(dir.path()).unwrap();

        let written = std::fs::read_to_string(dir.path().join(".gitignore")).unwrap();
        assert_eq!(written, GITIGNORE_CONTENTS);
    }
}
