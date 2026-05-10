//! Logic for `kutl surface` (RFD 0060).
//!
//! Surface copies a space's documents into the parent (or configured target)
//! directory, "lifting" them out of the kutl folder into the git working
//! tree. This module owns the pure logic — file enumeration, target
//! validation, and the copy operation. The CLI command in the `kutl` crate
//! wires it to user input.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use walkdir::WalkDir;

/// Sentinel header that `kutl surface` prepends to every surfaced file.
/// RFD 0075 §"Surface mechanics relevant to the contract": the agent
/// reads this line and recognizes that the file is a mirror, not a
/// source of truth.
pub const SURFACE_SENTINEL_HEADER: &str =
    "<!-- surfaced from kutl space — edit in kutl, not here -->";

/// Prepend `SURFACE_SENTINEL_HEADER` to `content` if the first line is
/// not already the sentinel. Idempotent.
#[must_use]
pub fn prepend_sentinel_if_missing(content: &str) -> String {
    let first_line_end = content.find('\n').unwrap_or(content.len());
    let first_line = content[..first_line_end].trim_end_matches('\r');
    if first_line == SURFACE_SENTINEL_HEADER {
        return content.to_owned();
    }
    let mut out = String::with_capacity(content.len() + SURFACE_SENTINEL_HEADER.len() + 1);
    out.push_str(SURFACE_SENTINEL_HEADER);
    out.push('\n');
    out.push_str(content);
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// Result of enumerating documents under a space root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceFile {
    /// Path relative to the space root.
    pub rel_path: PathBuf,
}

/// Enumerate the documents that `kutl surface` would copy from `space_root`.
///
/// Skips any path component starting with `.` (covers `.kutl/`, `.git/`,
/// `.gitignore`, `.kutlspace`, and any other hidden files). Only files (not
/// directories) are returned.
///
/// Hidden directories are pruned via `walkdir::filter_entry` so a populated
/// `.kutl/docs/` (full of CRDT sidecars) is not walked at all.
pub fn enumerate_surface_files(space_root: &Path) -> Result<Vec<SurfaceFile>> {
    let mut out = Vec::new();
    let walker = WalkDir::new(space_root)
        .into_iter()
        .filter_entry(|e| !is_hidden_entry_below_root(e, space_root));
    for entry in walker {
        let entry = entry.with_context(|| format!("walking {}", space_root.display()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(space_root)
            .context("walkdir produced a path outside the root")?
            .to_path_buf();
        out.push(SurfaceFile { rel_path: rel });
    }
    Ok(out)
}

/// True if the walkdir entry is a hidden file or directory (name starts with
/// `.`) AND is not the walk root itself. The root may itself live under a
/// hidden directory (e.g. a kutl space at `~/.config/kutl-test/`); we never
/// want to refuse to descend into the root.
fn is_hidden_entry_below_root(entry: &walkdir::DirEntry, root: &Path) -> bool {
    if entry.path() == root {
        return false;
    }
    entry
        .file_name()
        .to_str()
        .is_some_and(|name| name.starts_with('.'))
}

/// Resolve and prepare the surface target path relative to the space root.
///
/// `target` is the value from `[surface] target` in `.kutlspace`. The function:
/// 1. Joins `space_root` with `target`.
/// 2. **Creates the directory if it doesn't exist** (side effect — this is
///    not a pure query).
/// 3. Canonicalizes and returns the absolute path.
///
/// Errors if the resolved path is inside the space itself (would cause a
/// sync loop) or is itself a kutl space (would mix two teams' content).
pub fn resolve_surface_target(space_root: &Path, target: &str) -> Result<PathBuf> {
    let raw = space_root.join(target);
    std::fs::create_dir_all(&raw)
        .with_context(|| format!("failed to create surface target {}", raw.display()))?;
    let canonical = raw
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", raw.display()))?;

    // Reject if the target is inside the space itself.
    let canonical_root = space_root
        .canonicalize()
        .with_context(|| format!("failed to canonicalize space root {}", space_root.display()))?;
    if canonical.starts_with(&canonical_root) {
        anyhow::bail!(
            "surface target {} is inside the kutl space {}",
            canonical.display(),
            canonical_root.display()
        );
    }

    // Reject if the target is itself a kutl space.
    if canonical.join(".kutlspace").exists() {
        anyhow::bail!(
            "surface target {} is a kutl space (contains .kutlspace)",
            canonical.display()
        );
    }

    Ok(canonical)
}

/// Copy surfaced files from `space_root` into `target`.
///
/// Creates parent directories as needed. Overwrites existing files. Returns
/// the number of files copied. Skips no files itself — the caller is
/// responsible for filtering via [`enumerate_surface_files`].
pub fn copy_surface_files(
    space_root: &Path,
    target: &Path,
    files: &[SurfaceFile],
) -> Result<usize> {
    let mut count = 0;
    for file in files {
        let src = space_root.join(&file.rel_path);
        let dst = target.join(&file.rel_path);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let content = std::fs::read_to_string(&src)
            .with_context(|| format!("read surfaced source {}", src.display()))?;
        let stamped = prepend_sentinel_if_missing(&content);
        std::fs::write(&dst, stamped)
            .with_context(|| format!("write surfaced target {}", dst.display()))?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, "").unwrap();
    }

    #[test]
    fn test_enumerate_skips_hidden() {
        let dir = TempDir::new().unwrap();
        touch(&dir.path().join("docs/intro.md"));
        touch(&dir.path().join("specs/foo.md"));
        touch(&dir.path().join(".kutl/state.json"));
        touch(&dir.path().join(".gitignore"));
        touch(&dir.path().join(".kutlspace"));
        touch(&dir.path().join("docs/.hidden.md"));

        let files = enumerate_surface_files(dir.path()).unwrap();
        let mut rel: Vec<_> = files.into_iter().map(|f| f.rel_path).collect();
        rel.sort();
        assert_eq!(
            rel,
            vec![
                PathBuf::from("docs/intro.md"),
                PathBuf::from("specs/foo.md"),
            ]
        );
    }

    #[test]
    fn test_resolve_surface_target_parent_directory() {
        let parent = TempDir::new().unwrap();
        let space = parent.path().join("kutl");
        std::fs::create_dir(&space).unwrap();

        let target = resolve_surface_target(&space, "../").unwrap();
        assert_eq!(target, parent.path().canonicalize().unwrap());
    }

    #[test]
    fn test_resolve_surface_target_inside_space_rejected() {
        let space = TempDir::new().unwrap();
        let result = resolve_surface_target(space.path(), "./subdir");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("inside the kutl space")
        );
    }

    #[test]
    fn test_resolve_surface_target_other_kutl_space_rejected() {
        let parent = TempDir::new().unwrap();
        let space = parent.path().join("kutl");
        std::fs::create_dir(&space).unwrap();
        let other_space = parent.path().join("other");
        std::fs::create_dir(&other_space).unwrap();
        std::fs::write(other_space.join(".kutlspace"), "space_name = \"x\"\n").unwrap();

        let result = resolve_surface_target(&space, "../other");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(".kutlspace"));
    }

    #[test]
    fn test_copy_surface_files() {
        let parent = TempDir::new().unwrap();
        let space = parent.path().join("kutl");
        std::fs::create_dir(&space).unwrap();

        std::fs::create_dir_all(space.join("specs")).unwrap();
        std::fs::write(space.join("specs/foo.md"), "spec content").unwrap();
        std::fs::write(space.join("README.md"), "readme content").unwrap();
        // System file that should NOT be copied (also wouldn't be enumerated).
        std::fs::write(space.join(".kutlspace"), "space_name = \"x\"\n").unwrap();

        let files = enumerate_surface_files(&space).unwrap();
        let target = resolve_surface_target(&space, "../").unwrap();
        let copied = copy_surface_files(&space, &target, &files).unwrap();

        assert_eq!(copied, 2);

        let foo_out = std::fs::read_to_string(target.join("specs/foo.md")).unwrap();
        assert!(
            foo_out.starts_with(SURFACE_SENTINEL_HEADER),
            "sentinel missing from specs/foo.md"
        );
        assert!(foo_out.contains("spec content"));

        let readme_out = std::fs::read_to_string(target.join("README.md")).unwrap();
        assert!(
            readme_out.starts_with(SURFACE_SENTINEL_HEADER),
            "sentinel missing from README.md"
        );
        assert!(readme_out.contains("readme content"));

        assert!(!target.join(".kutlspace").exists());
    }

    #[test]
    fn test_prepend_sentinel_adds_header_to_plain_markdown() {
        let input = "# My document\n\nBody.\n";
        let out = prepend_sentinel_if_missing(input);
        assert!(out.starts_with(SURFACE_SENTINEL_HEADER));
        assert!(out.contains("# My document"));
    }

    #[test]
    fn test_prepend_sentinel_is_idempotent() {
        let input = "# My document\n\nBody.\n";
        let once = prepend_sentinel_if_missing(input);
        let twice = prepend_sentinel_if_missing(&once);
        assert_eq!(once, twice, "sentinel must not be applied twice");
    }

    #[test]
    fn test_prepend_sentinel_recognizes_existing_header_with_crlf() {
        // A file written on Windows might use \r\n line endings.
        let already = format!("{SURFACE_SENTINEL_HEADER}\r\n# Doc\r\n");
        let out = prepend_sentinel_if_missing(&already);
        assert_eq!(out, already, "must not re-prepend on CRLF input");
    }

    #[test]
    fn test_prepend_sentinel_handles_empty_file() {
        let out = prepend_sentinel_if_missing("");
        assert!(out.starts_with(SURFACE_SENTINEL_HEADER));
        assert!(out.ends_with('\n'));
    }

    #[test]
    fn test_copy_surface_files_applies_sentinel_to_each_file() {
        use std::path::PathBuf;

        let parent = TempDir::new().unwrap();
        let space = parent.path().join("kutl");
        std::fs::create_dir(&space).unwrap();
        let target_dir = parent.path().join("target");
        std::fs::create_dir(&target_dir).unwrap();

        // Two source files: one plain, one already carrying the sentinel.
        let plain_rel = PathBuf::from("notes.md");
        let already_rel = PathBuf::from("preface.md");
        std::fs::write(space.join(&plain_rel), "# Plain\n\nBody.\n").unwrap();
        std::fs::write(
            space.join(&already_rel),
            format!("{SURFACE_SENTINEL_HEADER}\n# Preface\n"),
        )
        .unwrap();

        let files = vec![
            SurfaceFile {
                rel_path: plain_rel.clone(),
            },
            SurfaceFile {
                rel_path: already_rel.clone(),
            },
        ];

        let n = copy_surface_files(&space, &target_dir, &files).unwrap();
        assert_eq!(n, 2);

        let plain_out = std::fs::read_to_string(target_dir.join(&plain_rel)).unwrap();
        assert!(
            plain_out.starts_with(SURFACE_SENTINEL_HEADER),
            "plain file did not receive sentinel: {plain_out}"
        );
        assert!(plain_out.contains("# Plain"));

        let already_out = std::fs::read_to_string(target_dir.join(&already_rel)).unwrap();
        // Must not be double-prepended: exactly one occurrence of the sentinel.
        assert_eq!(
            already_out.matches(SURFACE_SENTINEL_HEADER).count(),
            1,
            "sentinel was double-applied: {already_out}"
        );
    }
}
