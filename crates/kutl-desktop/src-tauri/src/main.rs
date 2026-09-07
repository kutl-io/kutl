//! kutl desktop application — Tauri v2 shell.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;
mod daemon;
mod notifications;
mod tray;

use std::sync::Arc;

use tauri::{Emitter, Manager};
use tauri_plugin_deep_link::DeepLinkExt;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Maximum allowed length for a space name received via deep link.
const MAX_SPACE_NAME_LEN: usize = 128;

/// Maximum allowed length for a space ID received via deep link.
const MAX_SPACE_ID_LEN: usize = 128;

/// Interval between supervisor maintenance passes (harvest worker exits,
/// retry failed workers, promote connecting ones, refresh the tray, notify).
const MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Marker file written to `~/.kutl/` on first launch to record that autostart
/// has been enabled by default. Its presence prevents re-enabling autostart on
/// subsequent launches.
const AUTOSTART_INIT_MARKER: &str = "desktop-autostart-initialized";

fn main() {
    // Initialize tracing before anything else.
    kutl_relay::telemetry::init_tracing("desktop");

    let app = tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_single_instance::init(|app, args, _cwd| {
            // On macOS, when a second instance is launched via a deep link
            // (e.g. clicking a kutl:// URL while the app is already running),
            // the OS passes the URL as a command-line argument to the new
            // instance.  The single-instance plugin forwards those args here
            // to the already-running instance so we can process the deep link.
            for arg in &args {
                if let Ok(url) = url::Url::parse(arg)
                    && url.scheme() == "kutl"
                {
                    handle_deep_link(app, &url);
                }
            }
        }))
        .plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            None,
        ))
        .invoke_handler(tauri::generate_handler![
            commands::validate_invite,
            commands::complete_join,
            commands::create_space,
            commands::list_spaces,
            commands::stop_space,
            commands::pick_folder,
            commands::get_home_dir,
            commands::open_folder,
            commands::get_launch_at_login,
            commands::set_launch_at_login,
            commands::about_url,
        ])
        .setup(|app| {
            // Hide the dock icon on macOS — kutl lives in the system tray.
            #[cfg(target_os = "macos")]
            app.handle()
                .set_activation_policy(tauri::ActivationPolicy::Accessory)?;

            tray::setup_tray(app.handle())?;

            // Enable autostart on first launch (write marker to prevent repeats).
            enable_autostart_once(app.handle());

            // Write desktop PID file for mutual exclusion with CLI daemon.
            if let Err(e) = daemon::write_desktop_pid() {
                warn!(error = %e, "failed to write desktop PID file");
            }

            // Create the supervisor and store it in Tauri state.
            let supervisor = Arc::new(Mutex::new(daemon::DaemonSupervisor::new()));
            app.manage(supervisor.clone());

            // Spawn the async supervisor initialization on Tauri's async runtime.
            let app_handle = app.handle().clone();
            let supervisor_for_init = supervisor.clone();
            tauri::async_runtime::spawn(async move {
                initialize_supervisor(app_handle, supervisor_for_init).await;
            });

            // Spawn the periodic supervisor maintenance task.
            let app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                maintenance_loop(app_handle).await;
            });

            // Route OS termination signals into the same graceful exit path
            // as the tray's Quit item, so logout / shutdown / kill also let
            // in-flight writes finish and remove the PID file.
            #[cfg(unix)]
            install_signal_handlers(app.handle().clone());

            // Register the deep-link handler.  On macOS this only fires for
            // bundled .app launches — `cargo tauri dev` will not trigger it.
            let dl_handle = app.handle().clone();
            app.deep_link().on_open_url(move |event| {
                for url in event.urls() {
                    if url.scheme() == "kutl" {
                        handle_deep_link(&dl_handle, &url);
                    }
                }
            });

            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("failed to build kutl desktop");

    app.run(|app, event| {
        match event {
            tauri::RunEvent::Exit => {
                // Gracefully shut down all daemon workers, awaiting completion
                // so in-flight writes finish before the process exits. This
                // arm runs on the platform's main-loop thread, outside the
                // tokio runtime, so blocking on the runtime is safe here.
                if let Some(supervisor) = app.try_state::<Arc<Mutex<daemon::DaemonSupervisor>>>() {
                    tauri::async_runtime::block_on(async {
                        supervisor.lock().await.shutdown().await;
                    });
                }
                daemon::remove_desktop_pid();
            }
            tauri::RunEvent::WindowEvent {
                event: tauri::WindowEvent::Destroyed,
                ..
            } => {
                // When the last window closes, switch back to Accessory policy
                // so the dock icon is hidden again.
                #[cfg(target_os = "macos")]
                {
                    let open_windows = app.webview_windows();
                    if open_windows.is_empty() {
                        let _ = app.set_activation_policy(tauri::ActivationPolicy::Accessory);
                    }
                }
            }
            _ => {}
        }
    });
}

/// Initialize the daemon supervisor: load identity, check for CLI daemon,
/// load the space registry, start workers, and update the tray menu.
async fn initialize_supervisor(
    app: tauri::AppHandle,
    supervisor: Arc<Mutex<daemon::DaemonSupervisor>>,
) {
    // Check if the CLI daemon is already running.
    match daemon::cli_daemon_running() {
        Ok(Some(pid)) => {
            warn!(
                pid,
                "CLI daemon is running, desktop supervisor will not start workers"
            );
            tray::update_tray_menu(&app, &[], &daemon::OverallStatus::CliDaemonRunning);
            return;
        }
        Ok(None) => {}
        Err(e) => {
            warn!(error = %e, "failed to check CLI daemon status, proceeding anyway");
        }
    }

    // Install Prometheus recorder + start /metrics server before any
    // worker spawns. Bind failure or duplicate-install (e.g. a stray CLI
    // daemon process) is non-fatal — `/metrics` is diagnostic.
    if let Err(e) = kutl_daemon::install_metrics_and_serve().await {
        warn!(error = %e, "metrics endpoint disabled");
    }

    // Load identity.
    let identity = match daemon::load_identity() {
        Ok(id) => {
            info!(did = %id.did, "loaded identity");
            id
        }
        Err(e) => {
            error!(error = %e, "failed to load identity — run `kutl init` first");
            tray::update_tray_menu(&app, &[], &daemon::OverallStatus::NoSpaces);
            return;
        }
    };

    // Load registry and start workers.
    {
        let mut sup = supervisor.lock().await;
        if let Err(e) = sup.reload_from_registry(&identity).await {
            error!(error = %e, "failed to load space registry");
        }
    }

    // Update tray with initial status.
    update_tray_from_supervisor(&app, &supervisor).await;

    // After the heuristic delay, promote connecting workers to synced.
    let delay = daemon::sync_heuristic_delay();
    tokio::time::sleep(delay).await;

    {
        let mut sup = supervisor.lock().await;
        // Reap first so a worker that already failed reports its own
        // message instead of a placeholder.
        sup.reap_finished_workers().await;
        sup.promote_connecting_workers();
    }

    // Update tray with promoted statuses.
    update_tray_from_supervisor(&app, &supervisor).await;

    // Notify for any spaces that failed to connect during startup.
    let mut throttle = notifications::NotificationThrottle::new();
    let statuses = {
        let sup = supervisor.lock().await;
        sup.space_statuses()
    };
    for status in &statuses {
        if let daemon::SyncState::Error(ref msg) = status.state {
            throttle.notify_if_allowed(&app, &status.space_id, &status.space_name, msg);
        }
    }
}

/// Listen for SIGTERM/SIGINT and route them into the app's exit path, which
/// runs the graceful worker shutdown in the `RunEvent::Exit` handler.
#[cfg(unix)]
fn install_signal_handlers(app_handle: tauri::AppHandle) {
    tauri::async_runtime::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let (Ok(mut term), Ok(mut int)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
        ) else {
            warn!("failed to install termination signal handlers");
            return;
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
        info!("termination signal received, shutting down");
        app_handle.exit(0);
    });
}

/// Read the supervisor's current state and update the tray menu.
pub(crate) async fn update_tray_from_supervisor(
    app: &tauri::AppHandle,
    supervisor: &Arc<Mutex<daemon::DaemonSupervisor>>,
) {
    let sup = supervisor.lock().await;
    let statuses = sup.space_statuses();
    let overall = daemon::overall_status(&statuses);
    tray::update_tray_menu(app, &statuses, &overall);
}

/// Enable autostart on the first ever launch, then write a marker file so
/// subsequent launches do not toggle it again.
///
/// Silently ignores all errors — autostart is a convenience feature and must
/// never block startup.
fn enable_autostart_once(app: &tauri::AppHandle) {
    use tauri_plugin_autostart::ManagerExt;

    let marker = match daemon::kutl_home() {
        Ok(home) => home.join(AUTOSTART_INIT_MARKER),
        Err(e) => {
            warn!(error = %e, "could not determine kutl home for autostart marker");
            return;
        }
    };

    if marker.exists() {
        return;
    }

    // Enroll only from an installed bundle. No marker is written on skip, so
    // the first launch of a properly installed app still enrolls.
    #[cfg(target_os = "macos")]
    if !std::env::current_exe().is_ok_and(|exe| exe_in_app_bundle(&exe)) {
        tracing::debug!("not running from an app bundle, skipping autostart enrollment");
        return;
    }

    // First launch: enable autostart.
    let manager = app.autolaunch();
    if let Err(e) = manager.enable() {
        warn!(error = %e, "failed to enable autostart on first launch");
    } else {
        info!("autostart enabled on first launch");
    }

    // Write the marker regardless of whether enable() succeeded, so we don't
    // keep retrying on every subsequent launch.
    if let Some(parent) = marker.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::write(&marker, "") {
        warn!(error = %e, path = %marker.display(), "failed to write autostart marker");
    }
}

/// True when `exe` sits inside a macOS `.app` bundle.
///
/// The login item records this executable's absolute path, so enrolling a
/// bare binary (a build-tree or unpacked-download launch) would pin login to
/// a path that may not survive. Only an installed bundle is a stable enough
/// target to enroll by default.
#[cfg(target_os = "macos")]
fn exe_in_app_bundle(exe: &std::path::Path) -> bool {
    exe.components().any(|c| {
        std::path::Path::new(c.as_os_str())
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("app"))
    })
}

/// Periodic supervisor maintenance: harvest worker exits, restart failed
/// workers, promote connecting ones, refresh the tray when anything changed,
/// and send OS notifications for spaces in the `Error` state.
///
/// Runs forever in a background tokio task. Uses the per-space throttle in
/// [`notifications::NotificationThrottle`] to suppress duplicate alerts.
async fn maintenance_loop(app: tauri::AppHandle) {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let mut throttle = notifications::NotificationThrottle::new();
    let mut last_render: Option<Vec<String>> = None;

    loop {
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;

        let Some(supervisor) = app.try_state::<Arc<Mutex<daemon::DaemonSupervisor>>>() else {
            // Supervisor not yet registered — skip this cycle.
            continue;
        };

        let statuses = {
            let mut sup = supervisor.lock().await;
            sup.maintain().await;
            sup.space_statuses()
        };

        // Refresh the tray only when the rendered menu would change, so an
        // open menu isn't rebuilt out from under the user every cycle.
        let render: Vec<String> = statuses
            .iter()
            .map(|s| daemon::space_menu_label(&s.space_name, &s.state))
            .chain(std::iter::once(
                daemon::status_line(&daemon::overall_status(&statuses)).to_owned(),
            ))
            .collect();
        if last_render.as_ref() != Some(&render) {
            update_tray_from_supervisor(&app, &supervisor).await;
            last_render = Some(render);
        }

        for status in &statuses {
            if let daemon::SyncState::Error(ref msg) = status.state {
                throttle.notify_if_allowed(&app, &status.space_id, &status.space_name, msg);
            }
        }
    }
}

/// Parse and validate a `kutl://join` deep-link URL, then emit a Tauri event
/// to the webview with the validated join parameters.
///
/// Accepted format:
/// ```text
/// kutl://join?relay=wss://relay.example.com/ws&space_id=abc&space_name=team-docs&token=kutl_xyz
/// ```
///
/// Validation rules:
/// - `relay` must start with `ws://` or `wss://`
/// - `space_name` must not contain `..`, `/`, or `\` (path traversal)
/// - `space_id` must be hex or UUID characters only
/// - `token` is optional; when present it is passed through as-is
///
/// Any validation failure is logged at WARN level and the deep link is silently
/// dropped — we never surface raw URLs to the webview without validation.
fn handle_deep_link(app: &tauri::AppHandle, url: &url::Url) {
    // Only handle the "join" path.
    if url.host_str() != Some("join") {
        warn!(url = %url, "ignoring unrecognised kutl:// deep link");
        return;
    }

    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();

    // --- relay validation ---
    let relay = if let Some(r) = params.get("relay") {
        r.as_ref().to_owned()
    } else {
        warn!("deep link missing required 'relay' parameter");
        return;
    };
    if !relay.starts_with("ws://") && !relay.starts_with("wss://") {
        warn!(relay = %relay, "deep link relay URL has disallowed scheme (must be ws:// or wss://)");
        return;
    }

    // --- space_id validation ---
    let space_id = if let Some(id) = params.get("space_id") {
        id.as_ref().to_owned()
    } else {
        warn!("deep link missing required 'space_id' parameter");
        return;
    };
    if space_id.len() > MAX_SPACE_ID_LEN
        || !space_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
    {
        warn!(space_id = %space_id, "deep link space_id failed validation");
        return;
    }

    // --- space_name validation ---
    let space_name = if let Some(n) = params.get("space_name") {
        n.as_ref().to_owned()
    } else {
        warn!("deep link missing required 'space_name' parameter");
        return;
    };
    if space_name.len() > MAX_SPACE_NAME_LEN
        || space_name.contains("..")
        || space_name.contains('/')
        || space_name.contains('\\')
    {
        warn!(space_name = %space_name, "deep link space_name failed validation (path traversal)");
        return;
    }

    // --- token (optional) ---
    let token: Option<String> = params.get("token").map(|t| t.as_ref().to_owned());

    info!(
        relay = %relay,
        space_id = %space_id,
        space_name = %space_name,
        has_token = token.is_some(),
        "handling kutl://join deep link"
    );

    let payload = serde_json::json!({
        "relay": relay,
        "spaceId": space_id,
        "spaceName": space_name,
        "token": token,
    });

    if let Err(e) = app.emit("deep-link-join", payload) {
        warn!(error = %e, "failed to emit deep-link-join event to webview");
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn test_exe_in_app_bundle_detects_bundle() {
        assert!(exe_in_app_bundle(std::path::Path::new(
            "/Applications/kutl.app/Contents/MacOS/kutl-desktop"
        )));
    }

    #[test]
    fn test_exe_in_app_bundle_rejects_bare_binary() {
        assert!(!exe_in_app_bundle(std::path::Path::new(
            "/Users/b/code/kutl/oss/crates/kutl-desktop/src-tauri/target/debug/kutl-desktop"
        )));
    }
}
