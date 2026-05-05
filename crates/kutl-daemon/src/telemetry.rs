//! Per-space log file utilities for kutl-daemon.
//!
//! Global tracing initialization is provided by
//! [`kutl_relay::telemetry::init_tracing`]. This module handles the per-space
//! log file paths and openers used by the CLI supervisor to give each space
//! worker its own log file.

use std::path::Path;

/// Initialize per-space log file writing.
///
/// Creates `$KUTL_HOME/logs/` if needed and returns the path for the
/// space's log file at `$KUTL_HOME/logs/<space_id>.log`. The supervisor
/// itself logs to `$KUTL_HOME/logs/daemon.log` via stdout redirection
/// from `daemon_mgmt`.
pub fn space_log_path(space_id: &str) -> std::io::Result<std::path::PathBuf> {
    let kutl_home = kutl_client::kutl_home().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("could not determine kutl home: {e}"),
        )
    })?;
    let logs_dir = kutl_home.join("logs");
    std::fs::create_dir_all(&logs_dir)?;
    Ok(logs_dir.join(format!("{space_id}.log")))
}

/// Open a log file for appending, creating parent directories as needed.
pub fn open_log_file(path: &Path) -> std::io::Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}
