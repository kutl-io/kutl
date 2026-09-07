//! Build and locate the `kutl` and `kutl-relay` binaries.
//!
//! This crate is a standalone package, so `CARGO_BIN_EXE_*` (which is set only
//! for a package's own binaries) is unavailable. `escargot` builds the two
//! sibling crates by manifest path and returns the compiled executable path.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

fn manifest_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn build(bin: &str, crate_dir: &str) -> PathBuf {
    // manifest_dir is oss/tests/cli-uxr; the OSS crates live at oss/crates/*.
    let manifest = manifest_dir()
        .join("../../crates")
        .join(crate_dir)
        .join("Cargo.toml");
    escargot::CargoBuild::new()
        .bin(bin)
        .manifest_path(&manifest)
        .run()
        .unwrap_or_else(|e| panic!("failed to build {bin}: {e}"))
        .path()
        .to_path_buf()
}

/// Absolute path to the freshly-built `kutl` CLI binary.
pub fn kutl_bin() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| build("kutl", "kutl"))
}

/// Absolute path to the freshly-built `kutl-relay` binary.
pub fn relay_bin() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| build("kutl-relay", "kutl-relay"))
}
