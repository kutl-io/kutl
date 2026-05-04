//! Human-readable renderer for [`super::schema::ClientStatus`].

use std::fmt::Write;

use super::schema::ClientStatus;

/// Seconds in one minute.
const SECONDS_PER_MINUTE: u64 = 60;
/// Seconds in one hour.
const SECONDS_PER_HOUR: u64 = 60 * 60;
/// Seconds in one day.
const SECONDS_PER_DAY: u64 = 60 * 60 * 24;

/// Display column width for space names in `kutl status` output.
const NAME_COLUMN_WIDTH: usize = 16;

/// Display column width for space paths in `kutl status` output.
const PATH_COLUMN_WIDTH: usize = 40;

/// Render a human-readable status summary.
pub fn render_human(s: &ClientStatus) -> String {
    let mut out = String::new();

    // Daemon line.
    match (&s.daemon.running, s.daemon.pid) {
        (true, Some(pid)) => {
            let _ = writeln!(
                out,
                "daemon: running (PID {pid}, $KUTL_HOME={home})",
                home = s.kutl_home
            );
        }
        _ => {
            let _ = writeln!(
                out,
                "daemon: not running ($KUTL_HOME={home})",
                home = s.kutl_home
            );
        }
    }

    // Desktop line, only if running.
    if let (true, Some(pid)) = (s.desktop.running, s.desktop.pid) {
        let _ = writeln!(out, "desktop: running (PID {pid})");
    }

    // Identity line.
    match &s.identity {
        Some(id) if id.from_env => {
            let _ = writeln!(
                out,
                "identity: token from $KUTL_TOKEN ({prefix}…)",
                prefix = id.token_prefix
            );
        }
        Some(id) => {
            let label = match (id.display_name.as_deref(), id.account_id.as_deref()) {
                (Some(name), Some(acct)) => format!("{name} ({acct})"),
                (Some(s), None) | (None, Some(s)) => s.to_owned(),
                (None, None) => format!("token {prefix}…", prefix = id.token_prefix),
            };
            match id.relay_url.as_deref() {
                Some(url) => {
                    let _ = writeln!(out, "identity: {label} @ {url}");
                }
                None => {
                    let _ = writeln!(out, "identity: {label}");
                }
            }
        }
        None => {
            let _ = writeln!(out, "identity: not authenticated");
        }
    }

    let _ = writeln!(out);

    // Spaces section.
    if s.spaces.is_empty() {
        let _ = writeln!(out, "no spaces registered");
    } else {
        let _ = writeln!(out, "spaces ({count} registered):", count = s.spaces.len());
        for space in &s.spaces {
            let mark = if space.healthy { "✓" } else { "✗" };
            let activity = match space.last_activity_seconds {
                Some(secs) => format_age(secs),
                None => "—".into(),
            };
            let detail = if space.healthy {
                format!(
                    "relay={url} last-activity={activity}",
                    url = space.relay_url
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
        }
    }

    let _ = writeln!(out);

    // Relays section.
    if !s.relays.is_empty() {
        let _ = writeln!(out, "relays:");
        for relay in &s.relays {
            if relay.reachable {
                let _ = writeln!(out, "  {url}   reachable", url = relay.url);
            } else {
                let err = relay.error.as_deref().unwrap_or("not probed");
                let _ = writeln!(out, "  {url}   unreachable ({err})", url = relay.url);
            }
        }
    }

    out
}

/// Format an elapsed-seconds duration as a short human-readable "Ns ago" string.
fn format_age(seconds: u64) -> String {
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
            rendered.contains("identity: not authenticated"),
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
                space_id: "id".into(),
                relay_url: "ws://r/ws".into(),
                healthy: true,
                unhealthy_reason: None,
                last_activity_seconds: Some(12),
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
    }
}
