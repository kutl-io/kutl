//! Tauri IPC commands for the setup wizard and space management.
//!
//! These commands are invoked from the webview via `window.__TAURI__.core.invoke()`
//! and handle invite validation, space joining/creation, folder picking, and
//! space lifecycle management.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tauri::{AppHandle, State};
use tauri_plugin_autostart::ManagerExt;
use tokio::sync::Mutex;
use tracing::info;

use crate::daemon::{self, DaemonSupervisor};

/// Timeout for all outbound relay HTTP requests.
const RELAY_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

// ── Response types ──────────────────────────────────────────────────

/// Information about a validated invite.
#[derive(Debug, Serialize)]
pub struct InviteInfoResponse {
    /// Relay-assigned space UUID.
    pub space_id: String,
    /// Human-readable space name.
    pub space_name: String,
    /// HTTP base URL of the relay that owns this invite.
    pub relay_url: String,
}

/// Result of successfully joining or creating a space.
#[derive(Debug, Serialize)]
pub struct JoinResult {
    /// Relay-assigned space UUID.
    pub space_id: String,
    /// Human-readable space name.
    pub space_name: String,
    /// Absolute path to the local folder for this space.
    pub folder: String,
}

/// Summary info for a single managed space.
#[derive(Debug, Serialize)]
pub struct SpaceInfoResponse {
    /// Relay-assigned space UUID.
    pub space_id: String,
    /// Human-readable space name.
    pub space_name: String,
    /// Absolute path to the local folder.
    pub folder: String,
    /// Relay WebSocket URL.
    pub relay_url: String,
    /// Current sync state: "connecting", "synced", or "error".
    pub state: String,
    /// Error message, if state is "error".
    pub error: Option<String>,
}

// ── Internal deserialization types ──────────────────────────────────

/// Relay response for `GET /invites/{code}`.
#[derive(serde::Deserialize)]
struct RelayInviteInfo {
    space_id: String,
    space_name: String,
}

/// Relay response for `POST /spaces/register`.
#[derive(serde::Deserialize)]
struct RegisterSpaceResponse {
    space_id: String,
    name: String,
}

/// Minimal space config for pruning the registry by space ID.
#[derive(serde::Deserialize)]
struct MinSpaceConfig {
    space_id: String,
}

// ── Commands ────────────────────────────────────────────────────────

/// Parse an invite URL, extract the relay host and code, and validate
/// the invite against the relay's `GET /invites/{code}` endpoint.
///
/// Returns the space ID, space name, and relay base URL.
#[tauri::command]
pub async fn validate_invite(url: String) -> Result<InviteInfoResponse, String> {
    let (relay_base, code) = parse_invite_url(&url)?;

    let endpoint = format!("{relay_base}/invites/{code}");
    let resp = reqwest::Client::builder()
        .timeout(RELAY_REQUEST_TIMEOUT)
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))?
        .get(&endpoint)
        .send()
        .await
        .map_err(|e| format!("failed to reach relay: {e}"))?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err("invite not found or expired".into());
    }
    if resp.status() == reqwest::StatusCode::GONE {
        return Err("invite has expired".into());
    }
    if !resp.status().is_success() {
        return Err(format!("relay returned status {}", resp.status()));
    }

    let info: RelayInviteInfo = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse invite response: {e}"))?;

    Ok(InviteInfoResponse {
        space_id: info.space_id,
        space_name: info.space_name,
        relay_url: relay_base,
    })
}

/// Open the invite URL in the user's default browser.
///
/// Uses `tauri-plugin-shell` to launch the system browser.
#[tauri::command]
#[allow(deprecated)]
pub async fn open_invite_in_browser(url: String, app: AppHandle) -> Result<(), String> {
    use tauri_plugin_shell::ShellExt;

    app.shell()
        .open(&url, None)
        .map_err(|e| format!("failed to open browser: {e}"))
}

/// Complete the join flow: write space config, update the global registry,
/// and start the daemon worker for the new space.
#[tauri::command]
pub async fn complete_join(
    space_id: String,
    space_name: String,
    relay_url: String,
    token: Option<String>,
    folder: String,
    state: State<'_, Arc<Mutex<DaemonSupervisor>>>,
    app: AppHandle,
) -> Result<JoinResult, String> {
    // 1. Load or generate identity.
    let identity = daemon::load_identity().map_err(|e| format!("failed to load identity: {e}"))?;

    // 2. If token provided, store to ~/.kutl/auth.json.
    if let Some(ref tok) = token {
        let creds_path = kutl_client::credentials::default_credentials_path()
            .map_err(|e| format!("failed to determine credentials path: {e}"))?;
        let creds = kutl_client::StoredCredentials {
            token: tok.clone(),
            relay_url: relay_url.clone(),
            account_id: String::new(),
            display_name: identity.display_name.clone(),
        };
        creds
            .save(&creds_path)
            .map_err(|e| format!("failed to save credentials: {e}"))?;
    }

    // 3. Create folder and write .kutl/space.json.
    let folder_path = PathBuf::from(&folder);
    std::fs::create_dir_all(&folder_path)
        .map_err(|e| format!("failed to create folder {folder}: {e}"))?;

    let ws_url = kutl_client::normalize_relay_url(&relay_url);
    let config = kutl_client::SpaceConfig {
        space_id: space_id.clone(),
        relay_url: ws_url,
    };
    config
        .save(&folder_path)
        .map_err(|e| format!("failed to write space config: {e}"))?;
    kutl_client::KutlspaceConfig {
        space_name: space_name.clone(),
        surface: None,
    }
    .save(&folder_path)
    .map_err(|e| format!("failed to write .kutlspace: {e}"))?;

    // 4. Update ~/.kutl/spaces.json registry (flock-protected).
    let canonical = folder_path
        .canonicalize()
        .map_err(|e| format!("failed to canonicalize folder path: {e}"))?;
    let canonical_str = canonical
        .to_str()
        .ok_or_else(|| "folder path contains invalid UTF-8".to_owned())?
        .to_owned();

    kutl_client::SpaceRegistry::update(|registry| {
        registry.add(&canonical_str);
    })
    .map_err(|e| format!("failed to update space registry: {e}"))?;

    // 5. Start the supervisor worker for the new space.
    start_space_worker(&state, &identity, &canonical_str).await?;

    // 6. Update tray menu.
    crate::update_tray_from_supervisor(&app, &state).await;

    info!(
        space_id = %space_id,
        space_name = %space_name,
        folder = %folder,
        "space joined successfully"
    );

    Ok(JoinResult {
        space_id,
        space_name,
        folder,
    })
}

/// Register a new space on the relay, then complete the join flow.
///
/// Posts `{ name }` to the relay's `POST /spaces/register` endpoint,
/// then delegates to the same logic as [`complete_join`].
#[tauri::command]
pub async fn create_space(
    name: String,
    relay_url: String,
    folder: String,
    state: State<'_, Arc<Mutex<DaemonSupervisor>>>,
    app: AppHandle,
) -> Result<JoinResult, String> {
    let http_base = kutl_client::relay_url_to_http(&relay_url);

    let endpoint = format!("{http_base}/spaces/register");
    let client = reqwest::Client::builder()
        .timeout(RELAY_REQUEST_TIMEOUT)
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))?;
    let resp = client
        .post(&endpoint)
        .json(&serde_json::json!({ "name": name }))
        .send()
        .await
        .map_err(|e| format!("failed to reach relay: {e}"))?;

    if resp.status() == reqwest::StatusCode::CONFLICT {
        return Err("space name already taken".into());
    }
    if resp.status() == reqwest::StatusCode::BAD_REQUEST {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("invalid space name: {body}"));
    }
    if !resp.status().is_success() {
        return Err(format!("relay returned status {}", resp.status()));
    }

    let reg: RegisterSpaceResponse = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse registration response: {e}"))?;

    // Delegate to complete_join with the registered space info.
    complete_join(reg.space_id, reg.name, relay_url, None, folder, state, app).await
}

/// Return the current list of managed spaces with their sync statuses.
#[tauri::command]
pub async fn list_spaces(
    state: State<'_, Arc<Mutex<DaemonSupervisor>>>,
) -> Result<Vec<SpaceInfoResponse>, String> {
    let sup = state.lock().await;
    let statuses = sup.space_statuses();

    Ok(statuses
        .into_iter()
        .map(|s| {
            let (state_str, error) = match &s.state {
                daemon::SyncState::Connecting => ("connecting".to_owned(), None),
                daemon::SyncState::Synced => ("synced".to_owned(), None),
                daemon::SyncState::Error(msg) => ("error".to_owned(), Some(msg.clone())),
            };
            SpaceInfoResponse {
                space_id: s.space_id,
                space_name: s.space_name,
                folder: s.space_root.display().to_string(),
                relay_url: s.relay_url,
                state: state_str,
                error,
            }
        })
        .collect())
}

/// Stop a space worker and remove it from the global registry.
#[tauri::command]
pub async fn stop_space(
    space_id: String,
    state: State<'_, Arc<Mutex<DaemonSupervisor>>>,
    app: AppHandle,
) -> Result<(), String> {
    {
        let mut sup = state.lock().await;
        sup.stop_space(&space_id)
            .await
            .map_err(|e| format!("failed to stop space: {e}"))?;
    }

    // Remove from registry (flock-protected).
    let target_space_id = space_id.clone();
    kutl_client::SpaceRegistry::update(|registry| {
        registry.spaces.retain(|path| {
            let config_path = PathBuf::from(path).join(".kutl").join("space.json");
            match std::fs::read_to_string(&config_path) {
                Ok(data) => {
                    // Parse and check if this is the space we're removing.
                    match serde_json::from_str::<MinSpaceConfig>(&data) {
                        Ok(config) => config.space_id != target_space_id,
                        Err(_) => true, // keep entries we can't parse
                    }
                }
                Err(_) => false, // prune entries whose config is missing
            }
        });
    })
    .map_err(|e| format!("failed to update space registry: {e}"))?;

    // Update tray menu.
    crate::update_tray_from_supervisor(&app, &state).await;

    info!(space_id = %space_id, "space stopped and removed");

    Ok(())
}

/// Show a native folder picker dialog and return the selected path.
///
/// Returns `None` if the user cancelled the dialog.
#[tauri::command]
pub async fn pick_folder(app: AppHandle) -> Result<Option<String>, String> {
    use tauri_plugin_dialog::DialogExt;

    let path = app.dialog().file().blocking_pick_folder();

    match path {
        Some(p) => {
            let path_buf = p
                .into_path()
                .map_err(|e| format!("failed to convert file path: {e}"))?;
            Ok(path_buf.to_str().map(str::to_owned))
        }
        None => Ok(None),
    }
}

/// Return the user's home directory path.
///
/// Used by the wizard JS to construct default folder paths.
#[tauri::command]
pub async fn get_home_dir() -> Result<String, String> {
    dirs::home_dir()
        .and_then(|p| p.to_str().map(str::to_owned))
        .ok_or_else(|| "could not determine home directory".to_owned())
}

/// Open a folder in the system file manager.
///
/// Accepts either an absolute filesystem path (`/home/user/docs`) or a
/// `file://` URL. Any other URL scheme (e.g. `http://`, `javascript:`) is
/// rejected to prevent the webview from opening arbitrary URLs via this
/// command.
#[tauri::command]
#[allow(deprecated)]
pub async fn open_folder(path: String, app: AppHandle) -> Result<(), String> {
    use tauri_plugin_shell::ShellExt;

    // Reject non-file URL schemes to prevent arbitrary URL opening.
    if let Some(scheme_end) = path.find("://") {
        let scheme = &path[..scheme_end];
        if scheme != "file" {
            return Err(format!("unsupported URL scheme: {scheme}"));
        }
    } else if !std::path::Path::new(&path).is_absolute() {
        // Not a URL and not an absolute path — reject.
        return Err("path must be an absolute filesystem path or a file:// URL".to_owned());
    }

    // On macOS, `open` on a directory opens Finder.
    // ShellExt::open with a file:// URL works cross-platform.
    let url = if path.starts_with("file://") {
        path
    } else {
        format!("file://{path}")
    };

    app.shell()
        .open(&url, None)
        .map_err(|e| format!("failed to open folder: {e}"))
}

/// Return whether the app is configured to launch at login.
#[tauri::command]
pub async fn get_launch_at_login(app: AppHandle) -> Result<bool, String> {
    app.autolaunch()
        .is_enabled()
        .map_err(|e| format!("failed to query autostart state: {e}"))
}

/// Enable or disable launching the app at login.
#[tauri::command]
pub async fn set_launch_at_login(enabled: bool, app: AppHandle) -> Result<(), String> {
    let manager = app.autolaunch();
    if enabled {
        manager
            .enable()
            .map_err(|e| format!("failed to enable autostart: {e}"))
    } else {
        manager
            .disable()
            .map_err(|e| format!("failed to disable autostart: {e}"))
    }
}

/// Return the about URL for the current deployment.
///
/// If any synced space uses a kutlhub.com relay, returns `https://kutlhub.com`.
/// Otherwise returns `https://kutl.io`.
#[tauri::command]
pub async fn about_url(state: State<'_, Arc<Mutex<DaemonSupervisor>>>) -> Result<String, String> {
    let sup = state.lock().await;
    let statuses = sup.space_statuses();

    let uses_kutlhub = statuses.iter().any(|s| s.relay_url.contains("kutlhub.com"));

    if uses_kutlhub {
        Ok("https://kutlhub.com".to_owned())
    } else {
        Ok("https://kutl.io".to_owned())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Parse an invite URL into (`relay_http_base`, `code`).
///
/// Supports formats:
/// - `http(s)://host/join/CODE`
/// - `http(s)://host/invites/CODE`
fn parse_invite_url(url: &str) -> Result<(String, String), String> {
    let trimmed = url.trim();

    // Try to parse as a full URL.
    let parsed = reqwest::Url::parse(trimmed).map_err(|_| {
        "invalid invite URL: expected a full URL like https://relay.example.com/join/CODE"
            .to_owned()
    })?;

    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(format!("unsupported URL scheme: {scheme}"));
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| "invite URL has no host".to_owned())?;

    let port_suffix = parsed.port().map_or(String::new(), |p| format!(":{p}"));
    let base = format!("{scheme}://{host}{port_suffix}");

    // Extract the code from the path: last segment after /join/ or /invites/.
    let path = parsed.path();
    let code = extract_code_from_path(path)?;

    Ok((base, code))
}

/// Extract the invite code from a URL path.
///
/// Looks for `/join/{code}` or `/invites/{code}` patterns.
fn extract_code_from_path(path: &str) -> Result<String, String> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // Look for "join" or "invites" followed by a code segment.
    for window in segments.windows(2) {
        if (window[0] == "join" || window[0] == "invites") && !window[1].is_empty() {
            return Ok(window[1].to_owned());
        }
    }

    Err("could not extract invite code from URL path".into())
}

/// Build a worker config and start it in the supervisor.
async fn start_space_worker(
    state: &State<'_, Arc<Mutex<DaemonSupervisor>>>,
    identity: &daemon::LoadedIdentity,
    canonical_path: &str,
) -> Result<(), String> {
    let (config, space_name) = daemon::build_worker_config(canonical_path, identity)
        .ok_or_else(|| format!("failed to build worker config for {canonical_path}"))?;

    let mut sup = state.lock().await;
    sup.start_space(config, space_name)
        .await
        .map_err(|e| format!("failed to start space worker: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_invite_url_join_path() {
        let (base, code) = parse_invite_url("https://relay.example.com/join/abc123").unwrap();
        assert_eq!(base, "https://relay.example.com");
        assert_eq!(code, "abc123");
    }

    #[test]
    fn test_parse_invite_url_invites_path() {
        let (base, code) = parse_invite_url("http://localhost:9100/invites/xyz789").unwrap();
        assert_eq!(base, "http://localhost:9100");
        assert_eq!(code, "xyz789");
    }

    #[test]
    fn test_parse_invite_url_with_port() {
        let (base, code) = parse_invite_url("https://relay.example.com:8443/join/code1").unwrap();
        assert_eq!(base, "https://relay.example.com:8443");
        assert_eq!(code, "code1");
    }

    #[test]
    fn test_parse_invite_url_invalid() {
        assert!(parse_invite_url("not-a-url").is_err());
    }

    #[test]
    fn test_parse_invite_url_no_code() {
        assert!(parse_invite_url("https://relay.example.com/join/").is_err());
    }

    #[test]
    fn test_extract_code_from_path_join() {
        assert_eq!(extract_code_from_path("/join/abc").unwrap(), "abc");
    }

    #[test]
    fn test_extract_code_from_path_invites() {
        assert_eq!(extract_code_from_path("/invites/xyz").unwrap(), "xyz");
    }

    #[test]
    fn test_extract_code_from_path_no_match() {
        assert!(extract_code_from_path("/other/stuff").is_err());
    }
}
