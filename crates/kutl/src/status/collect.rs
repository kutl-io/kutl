//! Static collection of client status from the local file system.
//!
//! No network calls — pure reads from `$KUTL_HOME` and registered space
//! roots. Network reachability lives in [`super::probe`].

use std::path::Path;

use anyhow::{Context, Result};

use super::schema::{ClientStatus, DaemonInfo, IdentityInfo, RelayInfo, SpaceInfo};

/// Maximum token characters surfaced in status output.
const TOKEN_PREFIX_LEN: usize = 12;

/// Collect the static portion of client status: registered spaces, identity,
/// daemon liveness via PID files, relay enumeration. No network calls.
///
/// `kutl_home_dir` is the resolved `$KUTL_HOME` (e.g., `~/.kutl`).
pub fn collect_static(kutl_home_dir: &Path) -> Result<ClientStatus> {
    let registry = load_registry(kutl_home_dir)?;
    let spaces = enumerate_spaces(&registry);
    let relays = enumerate_relays(&spaces);
    let identity = load_identity(kutl_home_dir)?;
    let daemon = check_pid_file(&kutl_home_dir.join("daemon.pid"));
    let desktop = check_pid_file(&kutl_home_dir.join("desktop.pid"));

    Ok(ClientStatus {
        kutl_home: kutl_home_dir.display().to_string(),
        daemon,
        desktop,
        identity,
        spaces,
        relays,
    })
}

fn load_registry(kutl_home_dir: &Path) -> Result<kutl_client::SpaceRegistry> {
    let path = kutl_home_dir.join("spaces.json");
    kutl_client::SpaceRegistry::load(&path)
        .with_context(|| format!("failed to load registry at {}", path.display()))
}

fn enumerate_spaces(registry: &kutl_client::SpaceRegistry) -> Vec<SpaceInfo> {
    registry
        .spaces
        .iter()
        .map(|p| build_space_info(p))
        .collect()
}

fn build_space_info(path_str: &str) -> SpaceInfo {
    let path = Path::new(path_str);

    if !path.exists() {
        return SpaceInfo::unhealthy(path_str, "path missing");
    }

    let Ok(config) = kutl_client::SpaceConfig::load(path) else {
        return SpaceInfo::unhealthy(path_str, "config unreadable");
    };

    let name = kutl_client::KutlspaceConfig::display_name(path, &config.space_id);
    let last_activity_seconds = filesystem_age_seconds(&path.join(".kutl").join("space.json"));

    SpaceInfo {
        path: path_str.to_owned(),
        name,
        space_id: config.space_id,
        relay_url: config.relay_url,
        healthy: true,
        unhealthy_reason: None,
        last_activity_seconds,
    }
}

fn enumerate_relays(spaces: &[SpaceInfo]) -> Vec<RelayInfo> {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for s in spaces {
        if s.relay_url.is_empty() {
            continue;
        }
        if seen.insert(&s.relay_url) {
            out.push(RelayInfo {
                url: s.relay_url.clone(),
                reachable: false,
                error: None,
            });
        }
    }
    out
}

/// Convert an empty `String` to `None`. Used to map credentials-file fields
/// ("" means "not yet populated") into the schema's optional shape.
fn nonempty(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

fn load_identity(kutl_home_dir: &Path) -> Result<Option<IdentityInfo>> {
    // Priority 1: env var.
    if let Ok(token) = std::env::var(kutl_client::credentials::TOKEN_ENV_VAR)
        && !token.is_empty()
    {
        let prefix: String = token.chars().take(TOKEN_PREFIX_LEN).collect();
        return Ok(Some(IdentityInfo {
            account_id: None,
            display_name: None,
            token_prefix: prefix,
            relay_url: None,
            from_env: true,
        }));
    }

    // Priority 2: credentials file at $KUTL_HOME/auth.json.
    let path = kutl_home_dir.join("auth.json");
    let creds = kutl_client::StoredCredentials::load(&path)?;
    Ok(creds.map(|c| {
        let prefix: String = c.token.chars().take(TOKEN_PREFIX_LEN).collect();
        IdentityInfo {
            account_id: nonempty(c.account_id),
            display_name: nonempty(c.display_name),
            token_prefix: prefix,
            relay_url: Some(c.relay_url),
            from_env: false,
        }
    }))
}

fn check_pid_file(path: &Path) -> DaemonInfo {
    match kutl_client::read_pid_file(path) {
        Ok(Some(pid)) if kutl_client::is_process_alive(pid) => DaemonInfo {
            running: true,
            pid: Some(pid),
        },
        _ => DaemonInfo {
            running: false,
            pid: None,
        },
    }
}

fn filesystem_age_seconds(path: &Path) -> Option<u64> {
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    let elapsed = std::time::SystemTime::now().duration_since(modified).ok()?;
    Some(elapsed.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_client::{SpaceConfig, SpaceRegistry};
    use tempfile::TempDir;

    /// Build a fake `$KUTL_HOME` with a registered space and return the tempdir
    /// and the home path.
    fn fake_home_with_space() -> (TempDir, std::path::PathBuf) {
        let home = TempDir::new().unwrap();
        let kutl_home = home.path().join(".kutl");
        std::fs::create_dir_all(&kutl_home).unwrap();

        // Create a space root with a valid space.json.
        let space_root = home.path().join("project");
        std::fs::create_dir_all(&space_root).unwrap();
        SpaceConfig {
            space_id: "abc-123".into(),
            relay_url: "ws://127.0.0.1:9100/ws".into(),
        }
        .save(&space_root)
        .unwrap();

        // Register that path in spaces.json.
        let registry_path = kutl_home.join("spaces.json");
        let mut reg = SpaceRegistry::default();
        reg.add(space_root.to_str().unwrap());
        let json = serde_json::to_string_pretty(&reg).unwrap();
        std::fs::write(&registry_path, json).unwrap();

        (home, kutl_home)
    }

    #[test]
    fn test_collect_static_returns_registered_space() {
        let (home, kutl_home) = fake_home_with_space();

        let status = collect_static(&kutl_home).expect("collect_static should succeed");

        assert_eq!(status.kutl_home, kutl_home.display().to_string());
        assert_eq!(status.spaces.len(), 1);
        let space = &status.spaces[0];
        assert!(space.healthy, "space should be healthy");
        assert_eq!(space.space_id, "abc-123");
        assert_eq!(space.relay_url, "ws://127.0.0.1:9100/ws");
        assert!(
            space.last_activity_seconds.is_some(),
            "freshly written space.json should produce an activity stamp"
        );
        let age = space.last_activity_seconds.unwrap();
        assert!(age < 60, "stamp should be recent (got {age}s)");

        // Identity is None when no auth.json exists.
        assert!(status.identity.is_none());

        // Daemon and desktop both not running.
        assert!(!status.daemon.running);
        assert!(!status.desktop.running);

        // One relay enumerated, reachability not yet probed (default false).
        assert_eq!(status.relays.len(), 1);
        assert_eq!(status.relays[0].url, "ws://127.0.0.1:9100/ws");

        drop(home); // keep tempdir alive until here
    }

    #[test]
    fn test_collect_static_flags_stale_space_path() {
        let home = TempDir::new().unwrap();
        let kutl_home = home.path().join(".kutl");
        std::fs::create_dir_all(&kutl_home).unwrap();

        // Register a path that does not exist.
        let mut reg = SpaceRegistry::default();
        reg.add("/nonexistent/path/that/never/was");
        let json = serde_json::to_string_pretty(&reg).unwrap();
        std::fs::write(kutl_home.join("spaces.json"), json).unwrap();

        let status = collect_static(&kutl_home).expect("collect_static should succeed");
        assert_eq!(status.spaces.len(), 1);
        let space = &status.spaces[0];
        assert!(!space.healthy);
        assert_eq!(space.unhealthy_reason.as_deref(), Some("path missing"));
    }
}
