//! `kutl surface` journeys (coverage-lane B7) — the space→mirror dogfooding
//! flow: copy a space's documents into a configured target directory, stamping
//! each mirrored file with the sentinel header so agents know it is a mirror,
//! not a source of truth.
//!
//! Surface is a pure filesystem verb, so these journeys run against a
//! local-only space (init with an unreachable relay) — no relay boot needed.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use kutl_cli_uxr::harness::cli::{self, TestHome};

/// A port assumed unbound, forcing `kutl init` into local-only mode.
const UNREACHABLE_RELAY_PORT: u16 = 19998;

/// Initialize a local-only space at `<parent>/space` and return its root.
///
/// Deletes the kutl-managed `AGENTS.md` that init writes at the space root so
/// the surfaced file set is exactly the fixtures each journey creates.
async fn init_space(home: &TestHome, parent: &Path) -> PathBuf {
    let space = parent.join("space");
    let relay_url = format!("ws://127.0.0.1:{UNREACHABLE_RELAY_PORT}");
    let init = cli::kutl_in(
        home.path(),
        parent,
        &[
            "init",
            "--relay",
            &relay_url,
            "--dir",
            space.to_str().unwrap(),
            "--name",
            "acme",
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "init (local-only) failed: {}",
        cli::stderr_str(&init)
    );
    std::fs::remove_file(space.join("AGENTS.md"))
        .expect("init writes a kutl-managed AGENTS.md at the space root");
    space
}

/// Overwrite `.kutlspace` with the given `[surface] target`, preserving the
/// space name the journeys init with.
fn set_surface_target(space: &Path, target: &str) {
    std::fs::write(
        space.join(".kutlspace"),
        format!("space_name = \"acme\"\n\n[surface]\ntarget = \"{target}\"\n"),
    )
    .unwrap();
}

/// Write `content` to `path`, creating parent directories as needed.
fn write_doc(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

/// All regular files under `root`, as root-relative paths.
fn collect_files(root: &Path) -> BTreeSet<PathBuf> {
    fn walk(dir: &Path, root: &Path, out: &mut BTreeSet<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, root, out);
            } else {
                out.insert(path.strip_prefix(root).unwrap().to_path_buf());
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(root, root, &mut out);
    out
}

/// The full surface arc against one mirror target:
/// - empty space → "No documents to surface." (target still created on demand);
/// - the exact mirrored file set: root doc + subdir doc + pre-sentineled doc,
///   with hidden files at two levels skipped;
/// - sentinel prepended as the first line, exactly once, source untouched;
/// - idempotent re-surface (byte-identical mirror, no double sentinel);
/// - the mirror is a mirror: local scribbles are overwritten and source edits
///   flow through on the next surface.
#[tokio::test]
async fn surface_journey() {
    let home = TestHome::new();
    let parent_dir = home.space_dir();
    let parent = parent_dir.path();
    let space = init_space(&home, parent).await;
    set_surface_target(&space, "../mirror");

    let mirror = parent.join("mirror");
    assert!(!mirror.exists(), "no mirror before the first surface");

    // Empty space: nothing to copy, but resolve_surface_target still creates
    // the target directory (it runs before enumeration).
    let empty = cli::kutl_in(home.path(), &space, &["surface"]).await;
    assert!(
        empty.status.success(),
        "surface (empty space) failed: {}",
        cli::stderr_str(&empty)
    );
    assert!(
        cli::stdout_str(&empty).contains("No documents to surface."),
        "empty space should say so: {}",
        cli::stdout_str(&empty)
    );
    assert!(
        mirror.exists(),
        "surface creates the target directory on demand"
    );
    assert!(
        collect_files(&mirror).is_empty(),
        "nothing may be copied from an empty space"
    );

    // Fixtures: a plain root doc, a subdir doc, a doc already carrying the
    // sentinel, and hidden files at two levels that must be skipped (the
    // populated .kutl/ state dir from init is pruned the same way).
    let notes_src = "# Notes\n\nplain body\n";
    write_doc(&space.join("notes.md"), notes_src);
    write_doc(&space.join("docs/intro.md"), "# Intro\n");
    write_doc(
        &space.join("preface.md"),
        &format!(
            "{}\n# Preface\n",
            kutl_client::surface::SURFACE_SENTINEL_HEADER
        ),
    );
    write_doc(&space.join(".secret.md"), "hidden root file\n");
    write_doc(&space.join("docs/.draft.md"), "hidden nested file\n");

    let first = cli::kutl_in(home.path(), &space, &["surface"]).await;
    assert!(
        first.status.success(),
        "surface failed: {}",
        cli::stderr_str(&first)
    );
    let out = cli::stdout_str(&first);
    assert!(
        out.contains("Surfacing 3 files from"),
        "surface should announce the count and paths: {out}"
    );
    for listed in ["  notes.md", "  docs/intro.md", "  preface.md"] {
        assert!(
            out.contains(listed),
            "file list should show {listed:?}: {out}"
        );
    }
    assert!(
        out.contains("Surfaced 3 files."),
        "surface should confirm the copy count: {out}"
    );
    for hidden in [".secret.md", ".draft.md"] {
        assert!(
            !out.contains(hidden),
            "hidden file {hidden} must not be surfaced: {out}"
        );
    }

    // The exact mirrored file set — hidden files skipped, subdir preserved.
    let expected: BTreeSet<PathBuf> = [
        PathBuf::from("notes.md"),
        PathBuf::from("docs/intro.md"),
        PathBuf::from("preface.md"),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        collect_files(&mirror),
        expected,
        "mirror must contain exactly the non-hidden documents"
    );

    // Sentinel is the exact first line of every mirrored file; the body is
    // preserved; a source that already carries it is not double-stamped.
    let notes = std::fs::read_to_string(mirror.join("notes.md")).unwrap();
    assert_eq!(
        notes.lines().next(),
        Some(kutl_client::surface::SURFACE_SENTINEL_HEADER),
        "sentinel must be the first line: {notes}"
    );
    assert!(notes.contains("# Notes"), "body preserved: {notes}");
    let intro = std::fs::read_to_string(mirror.join("docs/intro.md")).unwrap();
    assert_eq!(
        intro.lines().next(),
        Some(kutl_client::surface::SURFACE_SENTINEL_HEADER),
        "subdir doc gets the sentinel too: {intro}"
    );
    let preface = std::fs::read_to_string(mirror.join("preface.md")).unwrap();
    assert_eq!(
        preface
            .matches(kutl_client::surface::SURFACE_SENTINEL_HEADER)
            .count(),
        1,
        "pre-sentineled source must not be double-stamped: {preface}"
    );

    // The SOURCE is never stamped — surface reads the space, writes the mirror.
    assert_eq!(
        std::fs::read_to_string(space.join("notes.md")).unwrap(),
        notes_src,
        "surface must not modify the source document"
    );

    // Idempotent re-surface: byte-identical mirror, still exactly one sentinel.
    let second = cli::kutl_in(home.path(), &space, &["surface"]).await;
    assert!(
        second.status.success(),
        "re-surface failed: {}",
        cli::stderr_str(&second)
    );
    assert!(
        cli::stdout_str(&second).contains("Surfaced 3 files."),
        "re-surface reports the same copy count: {}",
        cli::stdout_str(&second)
    );
    let notes_again = std::fs::read_to_string(mirror.join("notes.md")).unwrap();
    assert_eq!(notes_again, notes, "re-surface must be byte-identical");
    assert_eq!(
        notes_again
            .matches(kutl_client::surface::SURFACE_SENTINEL_HEADER)
            .count(),
        1,
        "re-surface must not stack sentinels: {notes_again}"
    );

    // The mirror is a mirror: a local scribble is overwritten on the next
    // surface, and a source edit flows through.
    write_doc(&space.join("notes.md"), "# Notes\n\nplain body\n\nmore\n");
    write_doc(&mirror.join("docs/intro.md"), "local edit — will be lost\n");
    let third = cli::kutl_in(home.path(), &space, &["surface"]).await;
    assert!(
        third.status.success(),
        "third surface failed: {}",
        cli::stderr_str(&third)
    );
    let notes_refreshed = std::fs::read_to_string(mirror.join("notes.md")).unwrap();
    assert!(
        notes_refreshed.contains("more"),
        "source edit must flow to the mirror: {notes_refreshed}"
    );
    assert_eq!(
        notes_refreshed
            .matches(kutl_client::surface::SURFACE_SENTINEL_HEADER)
            .count(),
        1,
        "refresh must not stack sentinels: {notes_refreshed}"
    );
    assert_eq!(
        std::fs::read_to_string(mirror.join("docs/intro.md")).unwrap(),
        format!(
            "{}\n# Intro\n",
            kutl_client::surface::SURFACE_SENTINEL_HEADER
        ),
        "a mirror-side scribble must be overwritten by re-surface"
    );
}

/// `kutl surface` without a `[surface]` section must fail and name the exact
/// fix (the `.kutlspace` section to add).
#[tokio::test]
async fn surface_without_config_names_the_fix() {
    let home = TestHome::new();
    let parent_dir = home.space_dir();
    let space = init_space(&home, parent_dir.path()).await;

    let out = cli::kutl_in(home.path(), &space, &["surface"]).await;
    assert!(
        !out.status.success(),
        "surface must fail without a [surface] target: {}",
        cli::stdout_str(&out)
    );
    let err = cli::stderr_str(&out);
    assert!(
        err.contains("no [surface] target configured"),
        "error should name the missing config: {err}"
    );
    assert!(
        err.contains("add a `[surface]` section"),
        "error should name the fix: {err}"
    );
}

/// A target inside the space itself is rejected (it would cause a sync loop).
#[tokio::test]
async fn surface_rejects_target_inside_space() {
    let home = TestHome::new();
    let parent_dir = home.space_dir();
    let space = init_space(&home, parent_dir.path()).await;
    set_surface_target(&space, "mirror-inside");

    let out = cli::kutl_in(home.path(), &space, &["surface"]).await;
    assert!(
        !out.status.success(),
        "surface must reject a target inside the space: {}",
        cli::stdout_str(&out)
    );
    let err = cli::stderr_str(&out);
    assert!(
        err.contains("inside the kutl space"),
        "error should say the target is inside the space: {err}"
    );
}

/// A target that is itself a kutl space is rejected (surfacing into another
/// team's space would mix two spaces' content).
#[tokio::test]
async fn surface_rejects_target_that_is_a_space() {
    let home = TestHome::new();
    let parent_dir = home.space_dir();
    let parent = parent_dir.path();
    let space = init_space(&home, parent).await;

    let other = parent.join("other");
    std::fs::create_dir(&other).unwrap();
    std::fs::write(other.join(".kutlspace"), "space_name = \"other\"\n").unwrap();
    set_surface_target(&space, "../other");

    let out = cli::kutl_in(home.path(), &space, &["surface"]).await;
    assert!(
        !out.status.success(),
        "surface must reject a target that is a kutl space: {}",
        cli::stdout_str(&out)
    );
    let err = cli::stderr_str(&out);
    assert!(
        err.contains("is a kutl space"),
        "error should say the target is a kutl space: {err}"
    );
}
