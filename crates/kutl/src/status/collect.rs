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
    // Op-cap surfacing: the daemon records doc IDS at (and approaching) the
    // CRDT op cap in its persisted state (`note_op_cap_status`); `load`
    // returns an empty state when the file is missing OR corrupt (a corrupt
    // state.json therefore reads as "no frozen docs"; the daemon rebuilds
    // it), so this stays a pure-FS read.
    let daemon_state = kutl_daemon::state::DaemonState::load(&path.join(".kutl"));
    let docs_at_op_cap = resolve_doc_ids(&daemon_state, &daemon_state.at_op_cap);
    let docs_approaching_op_cap = resolve_doc_ids(&daemon_state, &daemon_state.approaching_op_cap);

    let open_signals = count_open_signals(path, &config.space_id);
    let surface_target = read_surface_target(path);

    SpaceInfo {
        path: path_str.to_owned(),
        name,
        space_id: config.space_id,
        relay_url: config.relay_url,
        healthy: true,
        unhealthy_reason: None,
        last_activity_seconds,
        docs_at_op_cap,
        docs_approaching_op_cap,
        open_signals,
        surface_target,
    }
}

/// Read the raw surface target from a space's `.kutlspace`, if configured.
/// Raw — not resolved (resolution would `create_dir_all`).
fn read_surface_target(space_root: &Path) -> Option<String> {
    kutl_client::KutlspaceConfig::load(space_root)
        .ok()
        .flatten()
        .and_then(|k| k.surface.map(|s| s.target))
}

/// Count a space's currently-open signals by folding its local segments at
/// `<space_root>/.kutl/signals/<space_id>/`. A space with no segment directory
/// folds to zero (`SegmentStore::load` returns empty on a missing dir), and any
/// read error also degrades to zero — status must never fail on signal state.
fn count_open_signals(space_root: &Path, space_id: &str) -> usize {
    let dir = kutl_signals::segment::signals_dir(space_root, space_id);
    let Ok(store) = kutl_signals::segment::SegmentStore::load(&dir) else {
        return 0;
    };
    let mut fold = kutl_signals::fold::SpaceSignalState::default();
    for record in store.records {
        fold.apply(record);
    }
    fold.iter()
        .filter(|(_, state)| matches!(state.status, kutl_signals::fold::SignalStatus::Open))
        .count()
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
    // Local DID side (identity.json). Present after `kutl init` /
    // `kutl join` even before any relay round-trip.
    let id_path = kutl_home_dir.join("identity.json");
    let local = if id_path.exists() {
        Some(kutl_client::identity::Identity::load(&id_path)?)
    } else {
        None
    };

    // Credential side — env var trumps the file, mirroring the
    // resolution chain in `kutl_client::credentials::resolve_token`.
    let (token_prefix, relay_url, account_id, creds_display_name, from_env) = if let Ok(token) =
        std::env::var(kutl_client::credentials::TOKEN_ENV_VAR)
        && !token.is_empty()
    {
        let prefix: String = token.chars().take(TOKEN_PREFIX_LEN).collect();
        (Some(prefix), None, None, None, true)
    } else {
        let path = kutl_home_dir.join("auth.json");
        let creds = kutl_client::StoredCredentials::load(&path)?;
        match creds {
            Some(c) => {
                let prefix: String = c.token.chars().take(TOKEN_PREFIX_LEN).collect();
                (
                    Some(prefix),
                    Some(c.relay_url),
                    nonempty(c.account_id),
                    nonempty(c.display_name),
                    false,
                )
            }
            None => (None, None, None, None, false),
        }
    };

    // Suppress the section entirely when neither side has anything.
    if local.is_none() && token_prefix.is_none() {
        return Ok(None);
    }

    // Display name preference: identity.json wins (it's what the user
    // explicitly set via `kutl config set name ...`); fall back to the
    // relay-side display name only when local is absent.
    let display_name = local
        .as_ref()
        .and_then(|id| id.display_name.clone())
        .or(creds_display_name);

    Ok(Some(IdentityInfo {
        did: local.as_ref().map(|id| id.did.clone()),
        display_name,
        email: local.as_ref().and_then(|id| id.email.clone()),
        account_id,
        token_prefix,
        relay_url,
        from_env,
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

/// Resolve a persisted set of document ids to display paths via the state's
/// path → entry map (falling back to the raw id for a document the map no
/// longer names), sorted for stable output. Shared by the at-cap and
/// approaching-cap lists.
fn resolve_doc_ids(
    state: &kutl_daemon::state::DaemonState,
    ids: &std::collections::BTreeSet<String>,
) -> Vec<String> {
    let mut out: Vec<String> = ids
        .iter()
        .map(|id| {
            state
                .documents
                .iter()
                .find(|(_, entry)| &entry.id == id)
                .map_or_else(|| id.clone(), |(rel, _)| rel.clone())
        })
        .collect();
    out.sort();
    out
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
            space_id: "5942d94f-5248-42e0-829b-f0a1e5a6dcc9".into(),
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

    /// Write signal segments into the fixture space's `.kutl/signals/<id>/`
    /// dir: two open signals and one closed one (the closed must NOT count).
    fn seed_signals(space_root: &Path, space_id: &str) {
        use kutl_proto::sync::{FlagPayload, Hlc, Signal, SignalEventType, signal};
        use kutl_signals::segment::SegmentWriter;

        fn created(id: &str, rec: &str, ms: u64) -> Signal {
            let mut s = Signal {
                id: id.into(),
                space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
                timestamp: i64::try_from(ms).unwrap(),
                record_id: rec.into(),
                payload: Some(signal::Payload::Flag(FlagPayload::default())),
                hlc: Some(Hlc {
                    physical_ms: ms,
                    logical: 0,
                    actor: vec![0u8; 16],
                }),
                ..Default::default()
            };
            s.set_event(SignalEventType::Created);
            s
        }
        fn closed(id: &str, rec: &str, ms: u64) -> Signal {
            let mut s = created(id, rec, ms);
            s.record_id = rec.into();
            s.payload = None;
            s.set_event(SignalEventType::Closed);
            s
        }

        let dir = kutl_signals::segment::signals_dir(space_root, space_id);
        let mut writer = SegmentWriter::open(&dir, uuid::Uuid::nil(), 0).unwrap();
        writer.append(&created("open-1", "r1", 10)).unwrap();
        writer.append(&created("open-2", "r2", 20)).unwrap();
        writer.append(&created("gone", "r3", 30)).unwrap();
        writer.append(&closed("gone", "r4", 40)).unwrap();
    }

    #[test]
    fn test_collect_static_counts_open_signals() {
        let (home, kutl_home) = fake_home_with_space();
        // The fixture space root is `<home>/project`, space_id `abc-123`.
        let space_root = home.path().join("project");
        seed_signals(&space_root, "5942d94f-5248-42e0-829b-f0a1e5a6dcc9");

        let status = collect_static(&kutl_home).expect("collect_static should succeed");
        let space = &status.spaces[0];
        assert_eq!(
            space.open_signals, 2,
            "two open signals folded from segments; the closed one is excluded"
        );
        drop(home);
    }

    #[test]
    fn test_collect_static_open_signals_zero_without_segments() {
        // A space with no signals directory must report zero, not error.
        let (home, kutl_home) = fake_home_with_space();
        let status = collect_static(&kutl_home).expect("collect_static should succeed");
        assert_eq!(status.spaces[0].open_signals, 0);
        drop(home);
    }

    #[test]
    fn test_collect_static_returns_registered_space() {
        let (home, kutl_home) = fake_home_with_space();

        let status = collect_static(&kutl_home).expect("collect_static should succeed");

        assert_eq!(status.kutl_home, kutl_home.display().to_string());
        assert_eq!(status.spaces.len(), 1);
        let space = &status.spaces[0];
        assert!(space.healthy, "space should be healthy");
        assert_eq!(space.space_id, "5942d94f-5248-42e0-829b-f0a1e5a6dcc9");
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

    #[test]
    fn test_read_surface_target_present() {
        let dir = TempDir::new().unwrap();
        kutl_client::KutlspaceConfig {
            space_name: "my-space".into(),
            surface: Some(kutl_client::SurfaceConfig {
                target: "../docs/".into(),
            }),
        }
        .save(dir.path())
        .unwrap();

        assert_eq!(
            read_surface_target(dir.path()),
            Some("../docs/".to_owned()),
            "should return the configured target"
        );
    }

    #[test]
    fn test_read_surface_target_absent() {
        let dir = TempDir::new().unwrap();
        // No .kutlspace written — must return None, not error.
        assert_eq!(
            read_surface_target(dir.path()),
            None,
            "should return None when .kutlspace is missing"
        );
    }

    #[test]
    fn test_read_surface_target_no_surface_section() {
        let dir = TempDir::new().unwrap();
        kutl_client::KutlspaceConfig {
            space_name: "plain-space".into(),
            surface: None,
        }
        .save(dir.path())
        .unwrap();

        assert_eq!(
            read_surface_target(dir.path()),
            None,
            "should return None when .kutlspace has no [surface] section"
        );
    }

    #[test]
    fn test_collect_static_surfaces_target_when_configured() {
        let (home, kutl_home) = fake_home_with_space();
        let space_root = home.path().join("project");

        // Write a .kutlspace with a surface target into the fixture space.
        kutl_client::KutlspaceConfig {
            space_name: "project-space".into(),
            surface: Some(kutl_client::SurfaceConfig {
                target: "../surfaced/".into(),
            }),
        }
        .save(&space_root)
        .unwrap();

        let status = collect_static(&kutl_home).expect("collect_static should succeed");
        assert_eq!(
            status.spaces[0].surface_target.as_deref(),
            Some("../surfaced/"),
            "status should surface the configured surface target"
        );
        drop(home);
    }

    #[test]
    fn test_collect_static_surface_target_none_without_config() {
        let (home, kutl_home) = fake_home_with_space();

        // No .kutlspace in the fixture space — surface_target must be None.
        let status = collect_static(&kutl_home).expect("collect_static should succeed");
        assert!(
            status.spaces[0].surface_target.is_none(),
            "status surface_target should be None when .kutlspace is absent"
        );
        drop(home);
    }

    #[test]
    fn test_resolve_doc_ids_maps_ids_to_paths_with_id_fallback() {
        // The persisted sets hold DOC IDS; status shows paths. A flagged id
        // present in the documents map resolves to its rel path; an id the
        // map no longer names (e.g. raced with an unregister) falls back to
        // the raw id rather than disappearing silently. The resolver is
        // shared by the at-cap and approaching-cap lists.
        let mut state = kutl_daemon::state::DaemonState::default();
        state.documents.insert(
            "notes/big.md".to_owned(),
            kutl_daemon::state::DocEntry {
                id: "uuid-1".to_owned(),
                confirmed: true,
                inode: None,
            },
        );
        state.at_op_cap.insert("uuid-1".to_owned());
        state.at_op_cap.insert("uuid-unmapped".to_owned());
        assert_eq!(
            resolve_doc_ids(&state, &state.at_op_cap),
            vec!["notes/big.md".to_owned(), "uuid-unmapped".to_owned()]
        );
        state.approaching_op_cap.insert("uuid-1".to_owned());
        assert_eq!(
            resolve_doc_ids(&state, &state.approaching_op_cap),
            vec!["notes/big.md".to_owned()]
        );
    }
}
