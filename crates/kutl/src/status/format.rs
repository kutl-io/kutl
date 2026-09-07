//! Human-readable renderer for [`super::schema::ClientStatus`].

use std::fmt::Write;

use super::schema::ClientStatus;
use super::{space_activity, write_daemon, write_frozen_docs, write_identity, write_relays};

/// Seconds in one minute (`u64` mirror of the canonical `kutl_core` constant).
const SECONDS_PER_MINUTE: u64 = kutl_core::SECONDS_PER_MINUTE.unsigned_abs();
/// Seconds in one hour (`u64` mirror of the canonical `kutl_core` constant).
const SECONDS_PER_HOUR: u64 = kutl_core::SECONDS_PER_HOUR.unsigned_abs();
/// Seconds in one day (`u64` mirror of the canonical `kutl_core` constant).
const SECONDS_PER_DAY: u64 = kutl_core::SECONDS_PER_DAY.unsigned_abs();

/// Display column width for space names in `kutl status` output.
const NAME_COLUMN_WIDTH: usize = 16;

/// Display column width for space paths in `kutl status` output.
const PATH_COLUMN_WIDTH: usize = 40;

/// Render a human-readable status summary.
pub fn render_human(s: &ClientStatus) -> String {
    let mut out = String::new();

    // Daemon + desktop lines (shared with `kutl daemon status`).
    write_daemon(&mut out, s);

    write_identity(&mut out, s.identity.as_ref());

    let _ = writeln!(out);

    // Spaces section.
    if s.spaces.is_empty() {
        let _ = writeln!(out, "no spaces registered");
    } else {
        let _ = writeln!(out, "spaces ({count} registered):", count = s.spaces.len());
        for space in &s.spaces {
            let mark = if space.healthy { "✓" } else { "✗" };
            let detail = if space.healthy {
                format!(
                    "relay={url} last-activity={activity} signals={signals}",
                    url = space.relay_url,
                    activity = space_activity(space),
                    signals = space.open_signals,
                )
            } else {
                format!(
                    "<{reason}>",
                    reason = space.unhealthy_reason.as_deref().unwrap_or("unhealthy")
                )
            };
            let _ = writeln!(
                out,
                "  {mark} {name:<name_width$} {path:<path_width$} {detail}",
                name = space.name,
                path = space.path,
                name_width = NAME_COLUMN_WIDTH,
                path_width = PATH_COLUMN_WIDTH,
            );
            write_frozen_docs(&mut out, space);
        }
    }

    let _ = writeln!(out);

    // Relays section (shared with `kutl space status`).
    write_relays(&mut out, &s.relays);

    out
}

/// Format an elapsed-seconds duration as a short human-readable "Ns ago" string.
///
/// Shared by the aggregate `kutl status` renderer and the focused space render
/// (via [`super::space_activity`]) so the two never drift.
pub(super) fn format_age(seconds: u64) -> String {
    if seconds < SECONDS_PER_MINUTE {
        format!("{seconds}s ago")
    } else if seconds < SECONDS_PER_HOUR {
        format!("{minutes}m ago", minutes = seconds / SECONDS_PER_MINUTE)
    } else if seconds < SECONDS_PER_DAY {
        format!("{hours}h ago", hours = seconds / SECONDS_PER_HOUR)
    } else {
        format!("{days}d ago", days = seconds / SECONDS_PER_DAY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::schema::{DaemonInfo, RelayInfo, SpaceInfo};

    #[test]
    fn test_render_human_daemon_down_no_spaces() {
        let status = ClientStatus {
            kutl_home: "/tmp/.kutl".into(),
            daemon: DaemonInfo {
                running: false,
                pid: None,
            },
            desktop: DaemonInfo {
                running: false,
                pid: None,
            },
            identity: None,
            spaces: vec![],
            relays: vec![],
        };

        let rendered = render_human(&status);
        assert!(
            rendered.contains("daemon: not running"),
            "rendered:\n{rendered}"
        );
        assert!(
            rendered.contains("identity: none provisioned"),
            "rendered:\n{rendered}"
        );
        assert!(
            rendered.contains("no spaces registered"),
            "rendered:\n{rendered}"
        );
    }

    #[test]
    fn test_render_human_with_running_daemon_and_space() {
        let status = ClientStatus {
            kutl_home: "/tmp/.kutl".into(),
            daemon: DaemonInfo {
                running: true,
                pid: Some(12345),
            },
            desktop: DaemonInfo {
                running: false,
                pid: None,
            },
            identity: None,
            spaces: vec![SpaceInfo {
                path: "/x/proj".into(),
                name: "proj".into(),
                space_id: "a5614527-0ce6-43be-8d1d-d012b7394867".into(),
                relay_url: "ws://r/ws".into(),
                healthy: true,
                unhealthy_reason: None,
                last_activity_seconds: Some(12),
                docs_at_op_cap: vec!["notes/big.md".into()],
                docs_approaching_op_cap: vec!["notes/warm.md".into()],
                open_signals: 3,
                surface_target: None,
            }],
            relays: vec![RelayInfo {
                url: "ws://r/ws".into(),
                reachable: true,
                error: None,
            }],
        };

        let rendered = render_human(&status);
        assert!(
            rendered.contains("daemon: running (PID 12345"),
            "rendered:\n{rendered}"
        );
        assert!(rendered.contains("/x/proj"), "rendered:\n{rendered}");
        assert!(rendered.contains("12s ago"), "rendered:\n{rendered}");
        assert!(rendered.contains("reachable"), "rendered:\n{rendered}");
        assert!(
            rendered.contains("! notes/big.md is at the edit-history cap"),
            "rendered:\n{rendered}"
        );
        assert!(
            rendered.contains("! notes/warm.md is approaching the edit-history cap"),
            "rendered:\n{rendered}"
        );
        // The per-space open-signal count is surfaced (fixture has 3).
        assert!(
            rendered.contains("signals=3"),
            "aggregate render must show the open-signal count:\n{rendered}"
        );
    }
}
