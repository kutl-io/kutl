//! Native OS notification dispatch for the kutl desktop application.
//!
//! Provides throttled sync-error notifications so the user is alerted when a
//! space worker enters the `Error` state, without spamming repeated alerts.

use std::collections::HashMap;
use std::time::Instant;

use tauri::AppHandle;
use tauri_plugin_notification::NotificationExt;

/// Minimum interval between error notifications for the same space.
///
/// If a space remains in an error state, at most one notification is sent
/// per this duration.
const NOTIFICATION_THROTTLE: std::time::Duration = std::time::Duration::from_mins(5);

/// Per-space notification throttle state.
///
/// Tracks when the last notification was sent for each space (keyed by
/// space ID) so we never fire more than one notification per
/// [`NOTIFICATION_THROTTLE`] interval per space.
pub struct NotificationThrottle {
    last_notified: HashMap<String, Instant>,
}

impl NotificationThrottle {
    /// Create a new, empty throttle state.
    pub fn new() -> Self {
        Self {
            last_notified: HashMap::new(),
        }
    }

    /// Return `true` if enough time has passed since the last notification
    /// for `space_id` to send a new one.
    fn should_notify(&self, space_id: &str) -> bool {
        match self.last_notified.get(space_id) {
            None => true,
            Some(last) => last.elapsed() >= NOTIFICATION_THROTTLE,
        }
    }

    /// Record that a notification was just sent for `space_id`.
    fn record(&mut self, space_id: &str) {
        self.last_notified
            .insert(space_id.to_owned(), Instant::now());
    }

    /// Send a sync-error notification for `space_id` if the throttle allows it.
    ///
    /// Does nothing (silently) if the notification would be within the throttle
    /// window, or if the OS notification API returns an error.
    pub fn notify_if_allowed(
        &mut self,
        app: &AppHandle,
        space_id: &str,
        space_name: &str,
        message: &str,
    ) {
        if !self.should_notify(space_id) {
            return;
        }
        notify_sync_error(app, space_name, message);
        self.record(space_id);
    }
}

/// Send a native OS notification for a sync error.
///
/// The notification title is `"kutl — {space_name}"` and the body is `message`.
/// Errors from the notification API are silently ignored — a missing notification
/// is not worth crashing or warning the user about.
pub fn notify_sync_error(app: &AppHandle, space_name: &str, message: &str) {
    let _ = app
        .notification()
        .builder()
        .title(format!("kutl \u{2014} {space_name}"))
        .body(message)
        .show();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_throttle_allows_first_notification() {
        let throttle = NotificationThrottle::new();
        assert!(throttle.should_notify("space-abc"));
    }

    #[test]
    fn test_throttle_blocks_second_notification() {
        let mut throttle = NotificationThrottle::new();
        throttle.record("space-abc");
        assert!(!throttle.should_notify("space-abc"));
    }

    #[test]
    fn test_throttle_independent_per_space() {
        let mut throttle = NotificationThrottle::new();
        throttle.record("space-abc");
        // A different space is not throttled.
        assert!(throttle.should_notify("space-xyz"));
    }
}
