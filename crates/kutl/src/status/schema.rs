//! Stable JSON-serializable types for `kutl status`.
//!
//! External readers (support tooling, scrapers, agents) depend on the
//! field names and shapes — treat changes as breaking.

use serde::Serialize;

/// Top-level client status snapshot.
///
/// Stable JSON schema — external readers (support tooling, scrapers, agents)
/// depend on field names and shapes. Treat changes as breaking.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ClientStatus {
    /// `$KUTL_HOME` resolved at invocation.
    pub kutl_home: String,
    /// Daemon liveness and PID, if running.
    pub daemon: DaemonInfo,
    /// Desktop tray app liveness and PID, if running.
    pub desktop: DaemonInfo,
    /// Stored credentials / DID identity.
    pub identity: Option<IdentityInfo>,
    /// Registered spaces (from `$KUTL_HOME/spaces.json`), with per-space state.
    pub spaces: Vec<SpaceInfo>,
    /// Unique relays referenced across registered spaces, with reachability.
    pub relays: Vec<RelayInfo>,
}

/// Daemon process state (also reused for the desktop tray app).
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DaemonInfo {
    /// `true` if the PID file exists and the process is alive.
    pub running: bool,
    /// Process ID if running, else `None`.
    pub pid: Option<u32>,
}

/// Identity / authentication state.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct IdentityInfo {
    /// Account ID on the relay. `None` until the relay validates the token.
    pub account_id: Option<String>,
    /// Human-readable display name. `None` if not set.
    pub display_name: Option<String>,
    /// Token prefix for at-a-glance disambiguation. Never the full token.
    pub token_prefix: String,
    /// Relay URL the credentials apply to. `None` when the token came from
    /// `$KUTL_TOKEN` (no implied relay).
    pub relay_url: Option<String>,
    /// `true` if the token came from `$KUTL_TOKEN`, not the credentials file.
    pub from_env: bool,
}

/// Per-space registered state.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SpaceInfo {
    /// Absolute path to the space root.
    pub path: String,
    /// Space name from `.kutlspace`, falling back to `space_id`.
    pub name: String,
    /// Space UUID.
    pub space_id: String,
    /// Relay WebSocket URL for this space.
    pub relay_url: String,
    /// `true` if the registered path still exists and `.kutl/space.json` is readable.
    pub healthy: bool,
    /// Free-form human-readable reason the space is unhealthy. Conventionally
    /// `"path missing"` or `"config unreadable"`, but treat as opaque from
    /// reader code. `None` when `healthy` is `true`.
    pub unhealthy_reason: Option<String>,
    /// Seconds since the last modification of `.kutl/space.json`. Best-effort
    /// proxy for "last activity"; not authoritative caught-up-ness.
    pub last_activity_seconds: Option<u64>,
}

impl SpaceInfo {
    /// Construct a `SpaceInfo` for a registered path that cannot be loaded
    /// (path missing, config unreadable, etc.).
    pub(super) fn unhealthy(path_str: &str, reason: &'static str) -> Self {
        Self {
            path: path_str.to_owned(),
            name: path_str.to_owned(),
            space_id: String::new(),
            relay_url: String::new(),
            healthy: false,
            unhealthy_reason: Some(reason.to_owned()),
            last_activity_seconds: None,
        }
    }
}

/// Relay reachability probe result.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct RelayInfo {
    /// Relay WebSocket URL.
    pub url: String,
    /// `true` if the HTTP equivalent of the relay URL responded within timeout.
    pub reachable: bool,
    /// Error string if `reachable` is `false`. `None` on success.
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_status_serializes_to_expected_shape() {
        let s = ClientStatus {
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
        let v = serde_json::to_value(&s).unwrap();
        for key in [
            "kutl_home",
            "daemon",
            "desktop",
            "identity",
            "spaces",
            "relays",
        ] {
            assert!(v.get(key).is_some(), "missing key: {key}");
        }
        assert_eq!(v["daemon"]["running"], false);
        assert_eq!(v["daemon"]["pid"], serde_json::Value::Null);
        assert!(v["spaces"].is_array());
        assert!(v["relays"].is_array());
    }
}
