//! System tray icon and menu for the kutl desktop application.
//!
//! Sets up a tray icon with a static menu on launch, then rebuilds it
//! dynamically as the daemon supervisor reports space status changes.
//!
//! Each space entry expands into two clickable items:
//! - `space-open-{id}` — opens the local folder in Finder / Explorer
//! - `space-browser-{id}` — opens the relay's web URL for the space

use tauri::AppHandle;
use tauri::Manager;
use tauri::menu::{Menu, MenuItem, PredefinedMenuItem};
use tauri::tray::TrayIconBuilder;

use crate::daemon::{self, OverallStatus, SpaceStatus};

/// Default width of the main wizard window in logical pixels.
const MAIN_WINDOW_WIDTH: f64 = 600.0;

/// Default height of the main wizard window in logical pixels.
const MAIN_WINDOW_HEIGHT: f64 = 500.0;

/// Width of the preferences window in logical pixels.
const PREFS_WINDOW_WIDTH: f64 = 500.0;

/// Height of the preferences window in logical pixels.
const PREFS_WINDOW_HEIGHT: f64 = 450.0;

/// Tray icon embedded at compile time from the simplified black-on-transparent logo.
/// Used as a macOS template image so the OS handles light/dark menu bar rendering.
const TRAY_ICON: tauri::image::Image<'_> = tauri::include_image!("./icons/tray-icon.png");

/// Stable tray icon identifier for lookups via `AppHandle::tray_by_id`.
const TRAY_ID: &str = "kutl-tray";

/// Build and register the system tray icon with an initial "Starting..." menu.
///
/// Menu layout:
/// ```text
/// kutl — Starting...
/// ─────────────────
/// Join a space...
/// Preferences...
/// ─────────────────
/// Quit kutl
/// ```
pub fn setup_tray(app: &AppHandle) -> tauri::Result<()> {
    let menu = build_static_menu(app, "kutl \u{2014} Starting...")?;

    TrayIconBuilder::with_id(TRAY_ID)
        .icon(TRAY_ICON)
        .icon_as_template(true)
        .menu(&menu)
        .show_menu_on_left_click(true)
        .tooltip("kutl")
        .on_menu_event(handle_menu_event)
        .build(app)?;

    Ok(())
}

/// Rebuild the tray menu to reflect current space statuses.
///
/// Replaces the entire menu. Call this whenever the supervisor's state
/// changes (after initial load, after the sync heuristic timer fires,
/// or when a worker exits).
pub fn update_tray_menu(app: &AppHandle, statuses: &[SpaceStatus], overall: &OverallStatus) {
    if let Err(e) = try_update_tray_menu(app, statuses, overall) {
        tracing::error!(error = %e, "failed to update tray menu");
    }
}

/// Inner implementation that propagates errors.
fn try_update_tray_menu(
    app: &AppHandle,
    statuses: &[SpaceStatus],
    overall: &OverallStatus,
) -> tauri::Result<()> {
    let tray = app
        .tray_by_id(TRAY_ID)
        .ok_or_else(|| tauri::Error::AssetNotFound("tray icon not found".into()))?;

    let status_text = daemon::status_line(overall);

    // Build menu items.
    let status_item = MenuItem::with_id(app, "status", status_text, false, None::<&str>)?;
    let sep1 = PredefinedMenuItem::separator(app)?;

    // Space list items (disabled — informational only for v1).
    let mut items: Vec<Box<dyn tauri::menu::IsMenuItem<tauri::Wry>>> = Vec::new();
    items.push(Box::new(status_item));
    items.push(Box::new(sep1));

    for space in statuses {
        tracing::debug!(
            space_id = %space.space_id,
            space_name = %space.space_name,
            space_root = %space.space_root.display(),
            relay_url = %space.relay_url,
            state = ?space.state,
            "updating tray menu for space"
        );
        let label = daemon::space_menu_label(&space.space_name, &space.state);
        // Space status line (disabled — informational).
        let status_item = MenuItem::with_id(
            app,
            format!("space-{}", space.space_id),
            &label,
            false,
            None::<&str>,
        )?;
        items.push(Box::new(status_item));

        // "Open folder" action.
        let open_folder_item = MenuItem::with_id(
            app,
            format!("space-open-{}", space.space_id),
            "  Open folder",
            true,
            None::<&str>,
        )?;
        items.push(Box::new(open_folder_item));

        // "Open in browser" action.
        let open_browser_item = MenuItem::with_id(
            app,
            format!("space-browser-{}", space.space_id),
            "  Open in browser",
            true,
            None::<&str>,
        )?;
        items.push(Box::new(open_browser_item));
    }

    if !statuses.is_empty() {
        items.push(Box::new(PredefinedMenuItem::separator(app)?));
    }

    let join = MenuItem::with_id(app, "join", "Join a space...", true, None::<&str>)?;
    let prefs = MenuItem::with_id(app, "preferences", "Preferences...", true, None::<&str>)?;
    let sep_bottom = PredefinedMenuItem::separator(app)?;
    let quit = MenuItem::with_id(app, "quit", "Quit kutl", true, None::<&str>)?;

    items.push(Box::new(join));
    items.push(Box::new(prefs));
    items.push(Box::new(sep_bottom));
    items.push(Box::new(quit));

    let refs: Vec<&dyn tauri::menu::IsMenuItem<tauri::Wry>> =
        items.iter().map(std::convert::AsRef::as_ref).collect();
    let menu = Menu::with_items(app, &refs)?;

    tray.set_menu(Some(menu))?;
    Ok(())
}

/// Build a static menu with a given status line (used for initial and
/// CLI-daemon-running states).
fn build_static_menu(app: &AppHandle, status_text: &str) -> tauri::Result<Menu<tauri::Wry>> {
    let status = MenuItem::with_id(app, "status", status_text, false, None::<&str>)?;
    let sep1 = PredefinedMenuItem::separator(app)?;
    let join = MenuItem::with_id(app, "join", "Join a space...", true, None::<&str>)?;
    let prefs = MenuItem::with_id(app, "preferences", "Preferences...", true, None::<&str>)?;
    let sep2 = PredefinedMenuItem::separator(app)?;
    let quit = MenuItem::with_id(app, "quit", "Quit kutl", true, None::<&str>)?;

    Menu::with_items(app, &[&status, &sep1, &join, &prefs, &sep2, &quit])
}

/// Handle menu item clicks.
///
/// Signature matches `TrayIconBuilder::on_menu_event` callback type.
#[allow(clippy::needless_pass_by_value)]
fn handle_menu_event(app: &AppHandle, event: tauri::menu::MenuEvent) {
    let id = event.id.as_ref();
    match id {
        "quit" => app.exit(0),
        "join" => open_main_window(app),
        "preferences" => open_preferences_window(app),
        _ if id.starts_with("space-open-") => {
            let space_id = &id["space-open-".len()..];
            open_space_folder(app, space_id);
        }
        _ if id.starts_with("space-browser-") => {
            let space_id = &id["space-browser-".len()..];
            open_space_in_browser(app, space_id);
        }
        _ => {}
    }
}

/// Open the main wizard window, or focus it if already open.
fn open_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.set_focus();
        return;
    }

    #[cfg(target_os = "macos")]
    {
        use tauri::ActivationPolicy;
        let _ = app.set_activation_policy(ActivationPolicy::Regular);
    }

    match tauri::WebviewWindowBuilder::new(app, "main", tauri::WebviewUrl::App("index.html".into()))
        .title("kutl")
        .inner_size(MAIN_WINDOW_WIDTH, MAIN_WINDOW_HEIGHT)
        .resizable(true)
        .build()
    {
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "failed to open main window"),
    }
}

/// Look up a space by ID from the supervisor and open a URL derived from it.
///
/// Handles the supervisor lookup, async spawn, lock, and status lookup.
/// The `build_url` closure receives the [`SpaceStatus`] and returns the
/// URL to open. Errors are logged but silently swallowed — a missing tray
/// action does not warrant surfacing an alert.
fn open_space_url(
    app: &AppHandle,
    space_id: &str,
    action: &str,
    build_url: impl FnOnce(&daemon::SpaceStatus) -> String + Send + 'static,
) {
    use std::sync::Arc;
    use tauri_plugin_shell::ShellExt;
    use tokio::sync::Mutex;

    let Some(supervisor) = app.try_state::<Arc<Mutex<crate::daemon::DaemonSupervisor>>>() else {
        tracing::warn!(space_id = %space_id, action = %action, "supervisor not available");
        return;
    };
    let supervisor = Arc::clone(&supervisor);

    let app = app.clone();
    let space_id = space_id.to_owned();
    let action = action.to_owned();

    tauri::async_runtime::spawn(async move {
        let sup = supervisor.lock().await;
        let statuses = sup.space_statuses();

        let Some(status) = statuses.iter().find(|s| s.space_id == space_id) else {
            tracing::warn!(space_id = %space_id, action = %action, "space not found");
            return;
        };

        let url = build_url(status);

        #[allow(deprecated)]
        if let Err(e) = app.shell().open(&url, None) {
            tracing::error!(error = %e, url = %url, action = %action, "failed to open URL");
        }
    });
}

/// Open the local folder for a space in Finder / Explorer.
fn open_space_folder(app: &AppHandle, space_id: &str) {
    open_space_url(app, space_id, "open-folder", |status| {
        let path = status.space_root.display().to_string();
        format!("file://{path}")
    });
}

/// Open the relay's web URL for a space in the default browser.
///
/// Derives the HTTP URL from the relay WebSocket URL:
/// - `ws://host:port/ws`  -> `http://host:port/spaces/{space_id}`
/// - `wss://host/ws`      -> `https://host/spaces/{space_id}`
fn open_space_in_browser(app: &AppHandle, space_id: &str) {
    let sid = space_id.to_owned();
    open_space_url(app, space_id, "open-browser", move |status| {
        let http_base = kutl_client::ws_url_to_http(&status.relay_url);
        format!("{http_base}/spaces/{sid}")
    });
}

/// Open the preferences window, or focus it if already open.
fn open_preferences_window(app: &AppHandle) {
    // Guard against duplicate windows — focus the existing one.
    if let Some(window) = app.get_webview_window("preferences") {
        let _ = window.set_focus();
        return;
    }

    // Switch to Regular activation policy so the window appears in Cmd-Tab.
    #[cfg(target_os = "macos")]
    {
        use tauri::ActivationPolicy;
        let _ = app.set_activation_policy(ActivationPolicy::Regular);
    }

    match tauri::WebviewWindowBuilder::new(
        app,
        "preferences",
        tauri::WebviewUrl::App("preferences.html".into()),
    )
    .title("kutl Preferences")
    .inner_size(PREFS_WINDOW_WIDTH, PREFS_WINDOW_HEIGHT)
    .resizable(false)
    .build()
    {
        Ok(_window) => {}
        Err(e) => tracing::error!(error = %e, "failed to open preferences window"),
    }
}
