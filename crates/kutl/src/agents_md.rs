//! AGENTS.md managed-section format, sentinel parsing, and staleness check.
//!
//! The kutl binary writes a managed section into the repo-root AGENTS.md,
//! delimited by `kutl:start` / `kutl:end` HTML comments. The opening
//! marker carries a version sentinel — the kutl version that rendered the
//! block — which agent-facing commands compare against the running binary
//! to decide whether the section is current, stale-but-compatible, or
//! incompatible.

use std::io;
use std::path::{Path, PathBuf};

/// Canonical AGENTS.md content embedded in the binary. Iteration cadence
/// equals release cadence — this is the source of truth for the
/// kutl-as-tool scope of agent instructions.
pub const TEMPLATE: &str = include_str!("agents_md_template.md");

/// Earliest kutl version whose rendered AGENTS.md block is still
/// considered compatible with this binary. Initially set to the kutl
/// version that introduces the sentinel (the same version the running
/// binary stamps into freshly written blocks); bumped only when an
/// AGENTS.md content change is contract-breaking — i.e., a previously
/// documented path or workflow now actively misleads agents rather
/// than just being out of date. Cosmetic and additive changes do not
/// bump this.
pub const MIN_COMPATIBLE_KUTL_VERSION: &str = "0.1.6";

const START_MARKER_PREFIX: &str = "<!-- kutl:start v=";
const START_MARKER_SUFFIX: &str =
    " - managed by `kutl init`, edits inside this block may be overwritten -->";
const END_MARKER: &str = "<!-- kutl:end -->";

/// A parsed managed block extracted from an AGENTS.md file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerBlock {
    /// Sentinel version recorded in the start marker (e.g., "0.1.6").
    pub version: String,
    /// Body content between the markers, excluding the markers themselves
    /// and their trailing newlines.
    pub body: String,
    /// Byte offset of the first character of the start marker.
    pub start_byte: usize,
    /// Byte offset one past the last character of the end marker (or the
    /// trailing newline immediately after it, if present).
    pub end_byte: usize,
}

/// Render a fresh managed block stamped with the given kutl version.
///
/// Output is `<start marker>\n<TEMPLATE (trailing newlines stripped)>\n<end marker>\n`.
/// The trailing newline ensures clean concatenation when appending to
/// an existing file.
#[must_use]
pub fn render(version: &str) -> String {
    let template = TEMPLATE.trim_end_matches('\n');
    format!("{START_MARKER_PREFIX}{version}{START_MARKER_SUFFIX}\n{template}\n{END_MARKER}\n")
}

/// Parse the first kutl-managed block in `content`. Anchors to the
/// first occurrence of the start-marker prefix and parses from there;
/// returns `None` if that occurrence is malformed (missing suffix,
/// malformed version, missing end marker), without scanning for a
/// later well-formed block. This is intentional — kutl writes at most
/// one managed block, and a second one is corrupt state.
#[must_use]
pub fn parse_first_block(content: &str) -> Option<MarkerBlock> {
    let start_idx = content.find(START_MARKER_PREFIX)?;
    let after_prefix = start_idx + START_MARKER_PREFIX.len();

    let suffix_idx = content[after_prefix..].find(START_MARKER_SUFFIX)?;
    let version_end = after_prefix + suffix_idx;
    let version = content[after_prefix..version_end].trim();

    // Reject malformed versions early; parse_first_block returning None
    // means "no usable managed block here", which the caller treats as
    // append-or-leave-alone depending on context.
    if semver::Version::parse(version).is_err() {
        return None;
    }

    let body_start = version_end + START_MARKER_SUFFIX.len();
    let body_start = skip_one_newline(content, body_start);

    let end_offset = content[body_start..].find(END_MARKER)?;
    let body_end = body_start + end_offset;
    let end_marker_end = body_end + END_MARKER.len();
    let end_byte = skip_one_newline(content, end_marker_end);

    let body = content[body_start..body_end]
        .trim_end_matches('\n')
        .to_owned();

    Some(MarkerBlock {
        version: version.to_owned(),
        body,
        start_byte: start_idx,
        end_byte,
    })
}

fn skip_one_newline(s: &str, idx: usize) -> usize {
    let bytes = s.as_bytes();
    if idx < bytes.len() && bytes[idx] == b'\r' && idx + 1 < bytes.len() && bytes[idx + 1] == b'\n'
    {
        idx + 2
    } else if idx < bytes.len() && bytes[idx] == b'\n' {
        idx + 1
    } else {
        idx
    }
}

/// Result of comparing a managed-block sentinel version against the
/// running binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Staleness {
    /// Sentinel matches the running binary, or is newer (which is the
    /// operator's problem to resolve, not the binary's to warn about).
    Current,
    /// Sentinel is older than the running binary but at or above the
    /// minimum compatible version. Action: warn, suggest `kutl init
    /// --update`. Command continues.
    StaleCompatible,
    /// Sentinel is older than the minimum compatible version. The
    /// agent-facing surface should refuse-or-prompt before continuing.
    StaleIncompatible,
}

/// Compare a sentinel version against the running binary's version and
/// the minimum-compatible threshold. All three inputs must parse as
/// semver.
///
/// # Errors
///
/// Returns an error if any of the version strings fail to parse.
pub fn staleness_for(
    sentinel: &str,
    current: &str,
    min_compatible: &str,
) -> Result<Staleness, semver::Error> {
    let sentinel_v = semver::Version::parse(sentinel)?;
    let current_v = semver::Version::parse(current)?;
    let min_v = semver::Version::parse(min_compatible)?;

    if sentinel_v >= current_v {
        return Ok(Staleness::Current);
    }
    if sentinel_v >= min_v {
        Ok(Staleness::StaleCompatible)
    } else {
        Ok(Staleness::StaleIncompatible)
    }
}

/// Convenience: compare against the running binary's `CARGO_PKG_VERSION`
/// and the binary's `MIN_COMPATIBLE_KUTL_VERSION` constant.
///
/// # Errors
///
/// Returns an error if `sentinel` does not parse as semver. The other
/// two inputs are compile-time constants and known to parse.
pub fn current_binary_staleness(sentinel: &str) -> Result<Staleness, semver::Error> {
    staleness_for(
        sentinel,
        env!("CARGO_PKG_VERSION"),
        MIN_COMPATIBLE_KUTL_VERSION,
    )
}

/// Resolve the AGENTS.md anchor for a given starting directory.
///
/// Priority:
/// 1. The git repository root, if `start_dir` is inside a git working tree.
/// 2. Otherwise, `start_dir` itself — the directory the user chose as the
///    project root when initializing without git. AGENTS.md is written
///    next to the `.kutlspace` file rather than at a git anchor.
///
/// AGENTS.md content covers more than git-coexistence (the "what kutl is"
/// / cross-scope pointers / staleness sections are useful regardless of
/// git), so non-git kutl installs still get the contract written.
#[must_use]
pub fn anchor_for(start_dir: &Path) -> PathBuf {
    kutl_client::find_git_repo_root(start_dir).unwrap_or_else(|| start_dir.to_path_buf())
}

/// Behavior selector for `apply_block`. `Default` is what `kutl init`
/// invokes; `ForceUpdate` is what `kutl init --update` invokes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyMode {
    /// Create when missing, append when no markers, leave alone when
    /// markers exist (returning `Stale` info if applicable so the caller
    /// can warn).
    Default,
    /// Create when missing, append when no markers, replace block when
    /// markers exist. Short-circuits to `AlreadyCurrent` if the
    /// would-be-rendered block is byte-identical to what is already on
    /// disk.
    ForceUpdate,
}

/// Outcome of an `apply_block` call. Callers translate to log lines or
/// user-visible warnings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// File did not exist; created with managed block as the entire
    /// content.
    Created,
    /// File existed without markers; managed block appended.
    Appended,
    /// Existing managed block replaced in place (only possible with
    /// `ApplyMode::ForceUpdate`).
    Replaced,
    /// Existing managed block already matched the rendered output;
    /// nothing written.
    AlreadyCurrent,
    /// Existing managed block is older than the rendered version, but
    /// `ApplyMode::Default` was used so the file was left alone. The
    /// caller is expected to warn with the included staleness info.
    Stale {
        /// The version string from the existing block's sentinel.
        sentinel: String,
        /// How stale the existing block is relative to the running binary.
        staleness: Staleness,
    },
}

/// Apply a managed AGENTS.md block at `path`. See [`ApplyMode`] and
/// [`ApplyOutcome`] for the behavior and result variants.
///
/// When `parse_first_block` returns `None` — whether because the file
/// has no kutl markers at all, or because an existing block is
/// malformed (e.g., garbled version string) — both modes append a fresh
/// managed section rather than attempting to repair. Any corrupt
/// content is left in place; recovery from a malformed block is a
/// human-supervised edit.
///
/// # Errors
///
/// Returns I/O errors from reading or writing the file.
pub fn apply_block(path: &Path, version: &str, mode: ApplyMode) -> io::Result<ApplyOutcome> {
    let rendered = render(version);

    let existing = match std::fs::read_to_string(path) {
        Ok(s) => Some(s),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };

    let Some(existing) = existing else {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, &rendered)?;
        return Ok(ApplyOutcome::Created);
    };

    let Some(block) = parse_first_block(&existing) else {
        // No usable markers — append the rendered block, ensuring a blank
        // line between existing content and the new section.
        let mut out = existing.clone();
        if !out.ends_with('\n') {
            out.push('\n');
        }
        if !out.ends_with("\n\n") {
            out.push('\n');
        }
        out.push_str(&rendered);
        std::fs::write(path, out)?;
        return Ok(ApplyOutcome::Appended);
    };

    let mut replacement = String::with_capacity(existing.len());
    replacement.push_str(&existing[..block.start_byte]);
    replacement.push_str(&rendered);
    replacement.push_str(&existing[block.end_byte..]);

    if replacement == existing {
        return Ok(ApplyOutcome::AlreadyCurrent);
    }

    match mode {
        ApplyMode::ForceUpdate => {
            std::fs::write(path, replacement)?;
            Ok(ApplyOutcome::Replaced)
        }
        ApplyMode::Default => {
            // Don't modify the file; report what we would have done.
            // current_binary_staleness compares the existing sentinel against
            // the running binary's CARGO_PKG_VERSION and MIN_COMPATIBLE_KUTL_VERSION.
            // If the sentinel doesn't parse as semver (shouldn't happen since
            // parse_first_block validated it), fall back to StaleIncompatible —
            // conservative rather than panicking.
            let staleness =
                current_binary_staleness(&block.version).unwrap_or(Staleness::StaleIncompatible);
            Ok(ApplyOutcome::Stale {
                sentinel: block.version,
                staleness,
            })
        }
    }
}

/// Result of a startup-time staleness check.
#[derive(Debug)]
pub enum CheckOutcome {
    /// No managed block was found (file missing or markers absent).
    Absent,
    /// Block is current relative to the running binary.
    Current,
    /// Block is stale but compatible. Caller should warn and continue.
    StaleCompatible { sentinel: String },
    /// Block is stale and incompatible. Caller decides whether to
    /// prompt or refuse based on TTY context.
    StaleIncompatible { sentinel: String },
}

/// Look up the AGENTS.md at `repo_root/AGENTS.md` and report its
/// staleness state without modifying the file.
///
/// # Errors
///
/// Returns I/O errors when the file exists but cannot be read.
pub fn check_at_repo_root(repo_root: &Path) -> io::Result<CheckOutcome> {
    let path = repo_root.join("AGENTS.md");
    let contents = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(CheckOutcome::Absent),
        Err(e) => return Err(e),
    };

    let Some(block) = parse_first_block(&contents) else {
        return Ok(CheckOutcome::Absent);
    };

    let staleness =
        current_binary_staleness(&block.version).unwrap_or(Staleness::StaleIncompatible);

    Ok(match staleness {
        Staleness::Current => CheckOutcome::Current,
        Staleness::StaleCompatible => CheckOutcome::StaleCompatible {
            sentinel: block.version,
        },
        Staleness::StaleIncompatible => CheckOutcome::StaleIncompatible {
            sentinel: block.version,
        },
    })
}

/// Handle a stale-incompatible AGENTS.md by either prompting (TTY) or
/// refusing (non-TTY). Returns `Ok(())` only when the situation is
/// resolved — either by an in-place refresh or by the caller already
/// having dealt with it.
///
/// # Errors
///
/// Returns an error when the user declines or when the environment is
/// non-interactive, with a message instructing how to fix it.
pub fn handle_incompatible(repo_root: &Path, sentinel: &str) -> anyhow::Result<()> {
    use std::io::{IsTerminal, Write};

    let path = repo_root.join("AGENTS.md");
    let current = env!("CARGO_PKG_VERSION");

    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        anyhow::bail!(
            "{} kutl block was generated by v{sentinel} which is incompatible with the running v{current}; run `kutl init --update` and re-run this command",
            path.display()
        );
    }

    eprint!(
        "AGENTS.md kutl block at {} was generated by v{sentinel} (incompatible with v{current}). Refresh now? [Y/n] ",
        path.display(),
    );
    let _ = std::io::stderr().flush();

    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    let answer = answer.trim().to_ascii_lowercase();
    if !(answer.is_empty() || answer == "y" || answer == "yes") {
        anyhow::bail!("declined; run `kutl init --update` when ready and re-run this command");
    }

    let outcome = apply_block(&path, current, ApplyMode::ForceUpdate)?;
    match outcome {
        ApplyOutcome::Replaced | ApplyOutcome::Created | ApplyOutcome::Appended => {
            eprintln!("refreshed kutl-managed section in {}", path.display());
            Ok(())
        }
        ApplyOutcome::AlreadyCurrent => Ok(()),
        ApplyOutcome::Stale { .. } => {
            // ForceUpdate must not return Stale; treat as bug.
            anyhow::bail!("internal error: ForceUpdate returned Stale outcome")
        }
    }
}

/// Handle an absent AGENTS.md (file missing OR existing but with no
/// kutl markers) when the caller has determined a kutl space is
/// present at this anchor. Same severity gradient as
/// [`handle_incompatible`]: refuse on non-TTY, prompt on TTY, write the
/// managed block on accept.
///
/// # Errors
///
/// Returns an error when the user declines or when the environment is
/// non-interactive, with a message instructing how to fix it.
pub fn handle_absent(repo_root: &Path) -> anyhow::Result<()> {
    use std::io::{IsTerminal, Write};

    let path = repo_root.join("AGENTS.md");
    let current = env!("CARGO_PKG_VERSION");

    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        anyhow::bail!(
            "no agent instructions found at {}; run `kutl init --update` and re-run this command",
            path.display()
        );
    }

    eprint!(
        "no agent instructions at {}. Write them now? [Y/n] ",
        path.display(),
    );
    let _ = std::io::stderr().flush();

    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    let answer = answer.trim().to_ascii_lowercase();
    if !(answer.is_empty() || answer == "y" || answer == "yes") {
        anyhow::bail!("declined; run `kutl init --update` when ready and re-run this command");
    }

    let outcome = apply_block(&path, current, ApplyMode::ForceUpdate)?;
    match outcome {
        ApplyOutcome::Created | ApplyOutcome::Appended | ApplyOutcome::Replaced => {
            eprintln!("wrote agent instructions to {}", path.display());
            Ok(())
        }
        ApplyOutcome::AlreadyCurrent => Ok(()),
        ApplyOutcome::Stale { .. } => {
            anyhow::bail!("internal error: ForceUpdate returned Stale outcome")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_produces_expected_block_shape() {
        let out = render("0.1.6");
        assert!(
            out.starts_with("<!-- kutl:start v=0.1.6 - managed by `kutl init`, edits inside this block may be overwritten -->\n"),
            "unexpected start, got:\n{out}"
        );
        assert!(
            out.trim_end().ends_with("<!-- kutl:end -->"),
            "unexpected end, got:\n{out}"
        );
        // Body is the embedded template, verbatim.
        assert!(out.contains("# Working in a kutl-aware repo"));
        assert!(out.contains("`kutl init --update`"));
    }

    #[test]
    fn test_parse_first_block_extracts_version_and_body() {
        let s = render("0.1.6");
        let block = parse_first_block(&s).expect("should parse");
        assert_eq!(block.version, "0.1.6");
        assert!(block.body.contains("# Working in a kutl-aware repo"));
        assert_eq!(block.start_byte, 0);
        assert_eq!(block.end_byte, s.len());
    }

    #[test]
    fn test_parse_first_block_finds_block_in_surrounding_content() {
        let prefix = "# Custom repo guidance\n\nSome existing notes.\n\n";
        let suffix = "\n\nMore repo-owned content below.\n";
        let block = render("0.1.5");
        let full = format!("{prefix}{block}{suffix}");

        let parsed = parse_first_block(&full).expect("should parse");
        assert_eq!(parsed.version, "0.1.5");
        assert_eq!(parsed.start_byte, prefix.len());
        assert_eq!(parsed.end_byte, prefix.len() + block.len());
    }

    #[test]
    fn test_parse_first_block_returns_none_when_no_markers() {
        assert!(parse_first_block("# Just a normal AGENTS.md\n\nNo kutl section.").is_none());
    }

    #[test]
    fn test_parse_first_block_returns_none_when_only_one_marker() {
        // Real start marker (correct prefix, valid version, correct
        // suffix) but no `kutl:end`. Exercises the end-marker-missing
        // path of parse_first_block, not the start-suffix-missing path.
        let half = "<!-- kutl:start v=0.1.6 - managed by `kutl init`, edits inside this block may be overwritten -->\nbody but no end\n";
        assert!(parse_first_block(half).is_none());
    }

    #[test]
    fn test_parse_first_block_rejects_malformed_version() {
        let bad = "<!-- kutl:start v=not-a-version - x -->\nbody\n<!-- kutl:end -->";
        assert!(parse_first_block(bad).is_none());
    }

    #[test]
    fn test_staleness_current_when_versions_match() {
        let s = staleness_for("0.1.6", "0.1.6", "0.1.5").unwrap();
        assert_eq!(s, Staleness::Current);
    }

    #[test]
    fn test_staleness_compatible_when_sentinel_older_but_at_or_above_min() {
        let s = staleness_for("0.1.6", "0.1.7", "0.1.5").unwrap();
        assert_eq!(s, Staleness::StaleCompatible);
        let s = staleness_for("0.1.5", "0.1.7", "0.1.5").unwrap();
        assert_eq!(s, Staleness::StaleCompatible);
    }

    #[test]
    fn test_staleness_incompatible_when_sentinel_below_min() {
        let s = staleness_for("0.1.4", "0.1.7", "0.1.5").unwrap();
        assert_eq!(s, Staleness::StaleIncompatible);
    }

    #[test]
    fn test_staleness_treats_newer_sentinel_than_binary_as_current() {
        // A binary running against an AGENTS.md written by a newer kutl
        // is the operator's problem, not ours — don't warn loudly. Treat
        // as Current.
        let s = staleness_for("0.2.0", "0.1.7", "0.1.5").unwrap();
        assert_eq!(s, Staleness::Current);
    }

    #[test]
    fn test_staleness_returns_err_on_unparseable_input() {
        assert!(staleness_for("not-semver", "0.1.7", "0.1.5").is_err());
        assert!(staleness_for("0.1.6", "bad", "0.1.5").is_err());
        assert!(staleness_for("0.1.6", "0.1.7", "junk").is_err());
    }

    #[test]
    fn test_current_binary_staleness_with_current_version() {
        // The running binary's own version is always Current relative to itself.
        let current = env!("CARGO_PKG_VERSION");
        let s = current_binary_staleness(current).unwrap();
        assert_eq!(s, Staleness::Current);
    }

    #[test]
    fn test_current_binary_staleness_err_on_bad_sentinel() {
        assert!(current_binary_staleness("not-a-semver").is_err());
    }

    use tempfile::tempdir;

    fn read(path: &std::path::Path) -> String {
        std::fs::read_to_string(path).expect("read")
    }

    #[test]
    fn test_anchor_for_returns_git_root_inside_git_repo() {
        let dir = tempdir().unwrap();
        // Create a git repo via shell git init.
        let out = std::process::Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(dir.path())
            .output()
            .expect("git init");
        assert!(out.status.success());
        // Create a nested subdirectory and check that anchor_for resolves
        // to the git root, not the nested dir.
        let nested = dir.path().join("a/b/c");
        std::fs::create_dir_all(&nested).unwrap();
        // Resolve symlinks because tempdir on macOS may include /private prefix.
        let expected = std::fs::canonicalize(dir.path()).unwrap();
        let actual = std::fs::canonicalize(anchor_for(&nested)).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_anchor_for_returns_start_dir_outside_git_repo() {
        let dir = tempdir().unwrap();
        // Plain directory — no git init. anchor_for returns the dir itself.
        let actual = std::fs::canonicalize(anchor_for(dir.path())).unwrap();
        let expected = std::fs::canonicalize(dir.path()).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_apply_block_creates_file_when_missing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");

        let outcome = apply_block(&path, "0.1.6", ApplyMode::Default).unwrap();

        assert_eq!(outcome, ApplyOutcome::Created);
        let contents = read(&path);
        assert!(contents.starts_with("<!-- kutl:start v=0.1.6 "));
        assert!(contents.ends_with("<!-- kutl:end -->\n"));
    }

    #[test]
    fn test_apply_block_appends_when_file_has_no_markers() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");
        std::fs::write(&path, "# My repo\n\nExisting content.\n").unwrap();

        let outcome = apply_block(&path, "0.1.6", ApplyMode::Default).unwrap();

        assert_eq!(outcome, ApplyOutcome::Appended);
        let contents = read(&path);
        assert!(contents.starts_with("# My repo\n\nExisting content.\n"));
        assert!(contents.contains("<!-- kutl:start v=0.1.6 "));
    }

    #[test]
    fn test_apply_block_default_leaves_existing_block_alone_when_current() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");
        let existing = format!(
            "# repo\n\n{}\n\nfooter\n",
            render(env!("CARGO_PKG_VERSION")).trim_end()
        );
        std::fs::write(&path, &existing).unwrap();

        let outcome = apply_block(&path, env!("CARGO_PKG_VERSION"), ApplyMode::Default).unwrap();

        assert!(matches!(outcome, ApplyOutcome::AlreadyCurrent));
        assert_eq!(read(&path), existing);
    }

    #[test]
    fn test_apply_block_force_update_replaces_block_in_place() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");
        let stale_block = render("0.1.5");
        let prefix = "# repo\n\nNotes.\n\n";
        let suffix = "\n\nFooter content.\n";
        std::fs::write(&path, format!("{prefix}{stale_block}{suffix}")).unwrap();

        let outcome = apply_block(&path, "0.1.6", ApplyMode::ForceUpdate).unwrap();

        assert_eq!(outcome, ApplyOutcome::Replaced);
        let contents = read(&path);
        assert!(contents.starts_with(prefix));
        assert!(contents.contains("<!-- kutl:start v=0.1.6 "));
        assert!(contents.ends_with(suffix));
    }

    #[test]
    fn test_apply_block_force_update_short_circuits_when_byte_identical() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");
        let current = env!("CARGO_PKG_VERSION");
        let existing = format!("# repo\n\n{}\n\nfooter\n", render(current).trim_end());
        std::fs::write(&path, &existing).unwrap();

        let outcome = apply_block(&path, current, ApplyMode::ForceUpdate).unwrap();

        assert_eq!(outcome, ApplyOutcome::AlreadyCurrent);
        assert_eq!(read(&path), existing);
    }

    #[test]
    fn test_check_at_repo_root_absent_when_file_missing() {
        let dir = tempdir().unwrap();
        let outcome = check_at_repo_root(dir.path()).unwrap();
        assert!(matches!(outcome, CheckOutcome::Absent));
    }

    #[test]
    fn test_check_at_repo_root_absent_when_file_has_no_markers() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "no markers here\n").unwrap();
        let outcome = check_at_repo_root(dir.path()).unwrap();
        assert!(matches!(outcome, CheckOutcome::Absent));
    }

    #[test]
    fn test_check_at_repo_root_current_when_sentinel_matches() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("AGENTS.md"),
            render(env!("CARGO_PKG_VERSION")),
        )
        .unwrap();
        let outcome = check_at_repo_root(dir.path()).unwrap();
        assert!(matches!(outcome, CheckOutcome::Current));
    }

    #[test]
    fn test_check_at_repo_root_stale_incompatible_when_below_min() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), render("0.0.1")).unwrap();
        let outcome = check_at_repo_root(dir.path()).unwrap();
        assert!(matches!(outcome, CheckOutcome::StaleIncompatible { .. }));
    }

    #[test]
    fn test_apply_block_default_reports_stale_state_without_modifying() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");
        // Use 0.0.1 — guaranteed older than CARGO_PKG_VERSION and below
        // MIN_COMPATIBLE_KUTL_VERSION, so staleness is StaleIncompatible.
        let stale_block = render("0.0.1");
        std::fs::write(&path, &stale_block).unwrap();

        let outcome = apply_block(&path, env!("CARGO_PKG_VERSION"), ApplyMode::Default).unwrap();

        match outcome {
            ApplyOutcome::Stale {
                sentinel,
                staleness,
            } => {
                assert_eq!(sentinel, "0.0.1");
                assert_eq!(staleness, Staleness::StaleIncompatible);
            }
            other => panic!("expected Stale, got {other:?}"),
        }
        // File untouched.
        assert_eq!(read(&path), stale_block);
    }
}
