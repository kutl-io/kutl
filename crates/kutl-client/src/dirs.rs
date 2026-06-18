//! Kutl configuration directory resolution.
//!
//! Checks `KUTL_HOME` first, then falls back to `~/.kutl`. This allows
//! running multiple daemon instances with isolated config directories
//! (e.g., for the demo simulation suite) without overriding `HOME`.

use std::path::PathBuf;

use anyhow::{Context, Result};

/// Environment variable to override the kutl config directory.
const KUTL_HOME_ENV: &str = "KUTL_HOME";

/// Return the kutl config directory.
///
/// Precedence: `$KUTL_HOME` > `~/.kutl`.
pub fn kutl_home() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var(KUTL_HOME_ENV) {
        return Ok(PathBuf::from(dir));
    }
    let home = dirs::home_dir().context("could not determine home directory")?;
    Ok(home.join(".kutl"))
}
