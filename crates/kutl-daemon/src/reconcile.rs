//! Startup reconciliation: classifies documents by comparing three sources
//! of truth (state.json, relay registry, filesystem) and produces a list of
//! actions. Pure logic — no I/O, no side effects, fully testable.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::SafeRelayPath;

/// An action to take during startup based on the reconciliation truth table.
///
/// ```text
/// on_relay  on_disk  was_remote  Action
/// ────────  ───────  ──────────  ──────────────────
/// true      true     -           SyncLocal (or RenameLocal if path differs)
/// true      false    true        SendUnregister
/// true      false    false       SubscribeRemote
/// false     true     true        DeleteLocal
/// false     true     false       SyncLocal
/// false     false    true        CleanupState
/// false     false    false       (no action)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupAction {
    /// Document exists on relay but we don't know about it. Register the
    /// identity locally and subscribe to receive content.
    SubscribeRemote {
        document_id: String,
        path: SafeRelayPath,
    },
    /// We have a locally-known document. Register with the relay, subscribe,
    /// and push our CRDT ops.
    SyncLocal { document_id: String, path: PathBuf },
    /// File was deleted locally while we were offline. Tell the relay to
    /// unregister the document so the deletion propagates to other clients.
    SendUnregister { document_id: String, path: PathBuf },
    /// Document was unregistered on the relay by another client. Delete the
    /// file from disk and clean up all local state.
    DeleteLocal { document_id: String, path: PathBuf },
    /// Document was renamed on the relay while we were offline. Rename
    /// the local file and update state.json to match.
    RenameLocal {
        document_id: String,
        old_path: PathBuf,
        new_path: SafeRelayPath,
    },
    /// Stale entry: gone from both disk and relay. Clean up state.json.
    CleanupState { document_id: String, path: PathBuf },
}

/// Inputs gathered from the three sources of truth before reconciliation.
pub struct ReconcileInputs<'a> {
    /// Path → UUID from state.json (what we knew last session).
    pub state_entries: &'a HashMap<PathBuf, String>,
    /// UUIDs confirmed by the relay in the previous session.
    pub previously_remote: &'a HashSet<String>,
    /// UUID → path from the current `ListSpaceDocuments` response.
    pub remote_active: &'a HashMap<String, SafeRelayPath>,
    /// Root directory for checking file existence on disk.
    pub space_root: &'a Path,
}

/// Reconcile three sources of truth into a list of startup actions.
///
/// Examines every document known from any source and classifies it according
/// to the truth table documented on [`StartupAction`]. New files on disk with
/// no state entry are NOT handled here — they're discovered by the file scan
/// that runs after reconciliation.
pub fn reconcile_startup(inputs: &ReconcileInputs<'_>) -> Vec<StartupAction> {
    let mut actions = Vec::new();

    // Pre-build a set of UUIDs known in local state for O(1) lookups.
    let state_uuids: HashSet<&str> = inputs.state_entries.values().map(String::as_str).collect();

    // 1. Remote documents not in local state → subscribe.
    for (uuid, path) in inputs.remote_active {
        if !state_uuids.contains(uuid.as_str()) {
            actions.push(StartupAction::SubscribeRemote {
                document_id: uuid.clone(),
                path: path.clone(),
            });
        }
    }

    // 2. Locally-known documents (from state.json) → classify by truth table.
    for (path, uuid) in inputs.state_entries {
        let on_disk = inputs.space_root.join(path).exists();
        let on_relay = inputs.remote_active.contains_key(uuid);
        let was_remote = inputs.previously_remote.contains(uuid);

        let action = match (on_relay, on_disk, was_remote) {
            (true, true, _) => {
                // on_relay is true, so remote_active must have this UUID.
                let relay_path = &inputs.remote_active[uuid];
                if relay_path.as_path() == path {
                    Some(StartupAction::SyncLocal {
                        document_id: uuid.clone(),
                        path: path.clone(),
                    })
                } else {
                    Some(StartupAction::RenameLocal {
                        document_id: uuid.clone(),
                        old_path: path.clone(),
                        new_path: relay_path.clone(),
                    })
                }
            }
            (false, true, false) => Some(StartupAction::SyncLocal {
                document_id: uuid.clone(),
                path: path.clone(),
            }),
            (true, false, true) => Some(StartupAction::SendUnregister {
                document_id: uuid.clone(),
                path: path.clone(),
            }),
            (true, false, false) => Some(StartupAction::SubscribeRemote {
                document_id: uuid.clone(),
                path: inputs.remote_active[uuid].clone(),
            }),
            (false, true, true) => Some(StartupAction::DeleteLocal {
                document_id: uuid.clone(),
                path: path.clone(),
            }),
            (false, false, true) => Some(StartupAction::CleanupState {
                document_id: uuid.clone(),
                path: path.clone(),
            }),
            // Not on relay, not on disk, never confirmed by relay.
            // Likely pre-seeded UUID for a file that hasn't been created
            // yet (e.g. binary files written after daemon start). Leave alone.
            (false, false, false) => None,
        };

        if let Some(a) = action {
            actions.push(a);
        }
    }

    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a `SafeRelayPath` in tests (panics on invalid input).
    fn safe(s: &str) -> SafeRelayPath {
        SafeRelayPath::new(s).unwrap()
    }

    fn make_inputs<'a>(
        state: &'a HashMap<PathBuf, String>,
        previously_remote: &'a HashSet<String>,
        remote: &'a HashMap<String, SafeRelayPath>,
        space_root: &'a Path,
    ) -> ReconcileInputs<'a> {
        ReconcileInputs {
            state_entries: state,
            previously_remote,
            remote_active: remote,
            space_root,
        }
    }

    #[test]
    fn test_new_remote_document_subscribes() {
        let state = HashMap::new();
        let prev = HashSet::new();
        let mut remote = HashMap::new();
        remote.insert("uuid-1".into(), safe("doc.md"));

        let dir = tempfile::tempdir().unwrap();
        let inputs = make_inputs(&state, &prev, &remote, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            StartupAction::SubscribeRemote {
                document_id: "uuid-1".into(),
                path: safe("doc.md"),
            }
        );
    }

    #[test]
    fn test_known_remote_document_already_in_state_not_subscribed_again() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("doc.md"), "uuid-1".into());
        let prev = HashSet::from(["uuid-1".into()]);
        let mut remote = HashMap::new();
        remote.insert("uuid-1".into(), safe("doc.md"));

        let dir = tempfile::tempdir().unwrap();
        // Create the file so it's "on disk".
        std::fs::write(dir.path().join("doc.md"), "content").unwrap();

        let inputs = make_inputs(&state, &prev, &remote, dir.path());
        let actions = reconcile_startup(&inputs);

        // Should produce SyncLocal, not a duplicate SubscribeRemote.
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], StartupAction::SyncLocal { .. }));
    }

    #[test]
    fn test_locally_deleted_file_unregisters() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("deleted.md"), "uuid-1".into());
        let prev = HashSet::from(["uuid-1".into()]);
        let mut remote = HashMap::new();
        remote.insert("uuid-1".into(), safe("deleted.md"));

        let dir = tempfile::tempdir().unwrap();
        // File does NOT exist on disk.

        let inputs = make_inputs(&state, &prev, &remote, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            StartupAction::SendUnregister {
                document_id: "uuid-1".into(),
                path: PathBuf::from("deleted.md"),
            }
        );
    }

    #[test]
    fn test_remotely_deleted_file_deleted_locally() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("gone.md"), "uuid-1".into());
        let prev = HashSet::from(["uuid-1".into()]);
        let remote = HashMap::new(); // Not on relay.

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("gone.md"), "content").unwrap();

        let inputs = make_inputs(&state, &prev, &remote, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            StartupAction::DeleteLocal {
                document_id: "uuid-1".into(),
                path: PathBuf::from("gone.md"),
            }
        );
    }

    #[test]
    fn test_locally_created_never_synced_registers() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("new.md"), "uuid-1".into());
        let prev = HashSet::new(); // Never confirmed by relay.
        let remote = HashMap::new(); // Not on relay.

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("new.md"), "content").unwrap();

        let inputs = make_inputs(&state, &prev, &remote, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], StartupAction::SyncLocal { .. }));
    }

    #[test]
    fn test_stale_entry_cleaned_up() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("stale.md"), "uuid-1".into());
        let prev = HashSet::from(["uuid-1".into()]);
        let remote = HashMap::new();

        let dir = tempfile::tempdir().unwrap();
        // Not on disk, not on relay.

        let inputs = make_inputs(&state, &prev, &remote, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            StartupAction::CleanupState {
                document_id: "uuid-1".into(),
                path: PathBuf::from("stale.md"),
            }
        );
    }

    #[test]
    fn test_preseeded_uuid_no_file_left_alone() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("future.bin"), "uuid-1".into());
        let prev = HashSet::new(); // Never confirmed by relay.
        let remote = HashMap::new(); // Not on relay.

        let dir = tempfile::tempdir().unwrap();
        // File does NOT exist yet (will be written after daemon starts).

        let inputs = make_inputs(&state, &prev, &remote, dir.path());
        let actions = reconcile_startup(&inputs);

        assert!(
            actions.is_empty(),
            "pre-seeded UUID with no file should produce no action"
        );
    }

    #[test]
    fn test_remote_rename_produces_rename_local() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("old-name.md"), "uuid-1".into());
        let prev = HashSet::from(["uuid-1".into()]);
        let mut remote = HashMap::new();
        // Relay says uuid-1 is now at "new-name.md" (renamed).
        remote.insert("uuid-1".into(), safe("new-name.md"));

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("old-name.md"), "content").unwrap();

        let inputs = make_inputs(&state, &prev, &remote, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            StartupAction::RenameLocal {
                document_id: "uuid-1".into(),
                old_path: PathBuf::from("old-name.md"),
                new_path: safe("new-name.md"),
            }
        );
    }

    #[test]
    fn test_crash_recovery_subscribes_instead_of_unregistering() {
        // Scenario: daemon crashed after registering identity but before
        // writing file to disk. State.json has the entry, relay has the
        // doc, but file is NOT on disk. was_remote is false (session
        // didn't complete to save remote_document_ids).
        let mut state = HashMap::new();
        state.insert(PathBuf::from("partial.md"), "uuid-1".into());
        let prev = HashSet::new(); // Session didn't complete.
        let mut remote = HashMap::new();
        remote.insert("uuid-1".into(), safe("partial.md"));

        let dir = tempfile::tempdir().unwrap();
        // File NOT on disk.

        let inputs = make_inputs(&state, &prev, &remote, dir.path());
        let actions = reconcile_startup(&inputs);

        // Should subscribe (re-download), NOT unregister.
        assert_eq!(actions.len(), 1);
        assert!(
            matches!(actions[0], StartupAction::SubscribeRemote { .. }),
            "crash recovery should subscribe, not unregister: {actions:?}"
        );
    }
}
