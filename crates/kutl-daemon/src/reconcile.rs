//! Startup reconciliation: classifies documents by comparing three sources
//! of truth (the state snapshot, relay registry, filesystem) and produces a list of
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
/// true      false    true        SendUnregister if materialized, else SubscribeRemote
/// true      false    false       SubscribeRemote
/// false     true     true        DeleteLocal
/// false     true     false       RegisterLocal
/// false     false    true        CleanupState
/// false     false    false       (no action)
/// ```
///
/// The `SyncLocal` / `RegisterLocal` split is load-bearing: only a document
/// the relay does NOT already list may be registered. A register mints a
/// fresh origin stamp and folds it into the placement lattice, re-running
/// path arbitration — for an already-settled document that re-election is
/// poison. On a restart after a path collision, both contenders re-register
/// with fresh stamps and whichever lands second steals the contested path on
/// this node alone (the relay treats re-registers as idempotent and peers
/// fold remote registers watermark-only, so the flip never propagates):
/// permanent cluster divergence. A relay-listed document is therefore only
/// re-subscribed and its pending ops pushed — never re-registered.
///
/// The `materialized` axis on the `SendUnregister` row separates "the user
/// deleted the file while we were offline" from "we never held the file's
/// bytes in the first place". A remote doc's identity is claimed (and
/// `confirmed`) at place time and persisted by the coalesced save, but its
/// CONTENT arrives later — a crash inside that gap restarts with a state
/// entry, `on_relay`, `was_remote`, and NO file on disk. Reading that as a
/// local delete unregisters the doc on the relay and propagates a
/// cluster-wide DELETION of a document this daemon never even held (the
/// crash-mid-bulk observer arm: 8 of 150 docs destroyed everywhere). The
/// persisted inode is the discriminator: it is recorded exactly when the
/// file's bytes first land on disk here, so `inode: None` ⇒ never
/// materialized ⇒ the absence is the crash window, not a delete ⇒
/// re-subscribe and let the content stream in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupAction {
    /// Document exists on relay but we don't know about it. Register the
    /// identity locally and subscribe to receive content.
    SubscribeRemote {
        document_id: String,
        path: SafeRelayPath,
    },
    /// A locally-known document the relay also lists. Subscribe and push our
    /// CRDT ops — do NOT re-register (see the truth-table note: a re-register's
    /// fresh stamp re-runs settled path arbitration on this node only).
    SyncLocal { document_id: String, path: PathBuf },
    /// A locally-known document the relay does not have. Register it (a fresh
    /// origin stamp is correct — the document is genuinely new to the
    /// cluster), subscribe, and push our CRDT ops.
    RegisterLocal { document_id: String, path: PathBuf },
    /// File was deleted locally while we were offline. Tell the relay to
    /// unregister the document so the deletion propagates to other clients.
    SendUnregister { document_id: String, path: PathBuf },
    /// Document was unregistered on the relay by another client. Delete the
    /// file from disk and clean up all local state.
    DeleteLocal { document_id: String, path: PathBuf },
    /// Document was renamed on the relay while we were offline. Rename
    /// the local file and update the state snapshot to match.
    RenameLocal {
        document_id: String,
        old_path: PathBuf,
        new_path: SafeRelayPath,
    },
    /// Stale entry: gone from both disk and relay. Clean up the state snapshot.
    CleanupState { document_id: String, path: PathBuf },
}

/// Inputs gathered from the three sources of truth before reconciliation.
pub struct ReconcileInputs<'a> {
    /// Path → UUID from the state snapshot (what we knew last session).
    pub state_entries: &'a HashMap<PathBuf, String>,
    /// UUIDs confirmed by the relay in the previous session.
    pub previously_remote: &'a HashSet<String>,
    /// UUID → path from the current `ListSpaceDocuments` response.
    pub remote_active: &'a HashMap<String, SafeRelayPath>,
    /// UUIDs whose persisted entry records an inode — documents whose file
    /// bytes have actually landed on this disk at least once. The inode is
    /// recorded when the first write lands and persisted with the entry, so
    /// its absence marks an identity claimed but never materialized (the
    /// crash window) — see the truth-table note on the `SendUnregister` row.
    pub materialized: &'a HashSet<String>,
    /// Root directory for checking file existence on disk.
    pub space_root: &'a Path,
}

/// Reconcile three sources of truth into a list of startup actions.
///
/// Examines every document known from any source and classifies it according
/// to the truth table documented on [`StartupAction`]. New files on disk with
/// no state entry are NOT handled here — they're discovered by the file scan
/// that runs after reconciliation.
///
/// ORDERING INVARIANT: every action that settles a path's LOCAL disposition
/// (delete, rename-away, unregister, stale-entry cleanup) is emitted before
/// any `SubscribeRemote`, and the executor runs actions in emission order.
/// A subscribe claims its path's identity; if a local action for the same
/// path ran after it (the re-mint shape: the old document deleted and a NEW
/// one registered at the same path while this daemon was offline), the
/// path-keyed teardown would destroy the NEW document's file and identity,
/// leaving an orphaned subscription that drops every catch-up frame.
pub fn reconcile_startup(inputs: &ReconcileInputs<'_>) -> Vec<StartupAction> {
    let mut actions = Vec::new();
    // Subscribes are collected separately and appended LAST — see the
    // ordering invariant above. This covers both subscribe sources: unknown
    // remote documents and the crash-recovery arm of the truth table.
    let mut subscribes = Vec::new();

    // Pre-build a set of UUIDs known in local state for O(1) lookups.
    let state_uuids: HashSet<&str> = inputs.state_entries.values().map(String::as_str).collect();

    // 1. Remote documents not in local state → subscribe.
    for (uuid, path) in inputs.remote_active {
        if !state_uuids.contains(uuid.as_str()) {
            subscribes.push(StartupAction::SubscribeRemote {
                document_id: uuid.clone(),
                path: path.clone(),
            });
        }
    }

    // 2. Locally-known documents (from the state snapshot) → classify by truth table.
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
            (false, true, false) => Some(StartupAction::RegisterLocal {
                document_id: uuid.clone(),
                path: path.clone(),
            }),
            (true, false, true) => {
                if inputs.materialized.contains(uuid.as_str()) {
                    // The file's bytes were on this disk and now are not: a
                    // genuine delete while offline — propagate it.
                    Some(StartupAction::SendUnregister {
                        document_id: uuid.clone(),
                        path: path.clone(),
                    })
                } else {
                    // Identity claimed + confirmed but the content never
                    // landed here (crash inside the place→materialize gap):
                    // the absence is NOT a delete. Re-subscribe and let the
                    // content stream in — unregistering would destroy the
                    // document cluster-wide.
                    Some(StartupAction::SubscribeRemote {
                        document_id: uuid.clone(),
                        path: inputs.remote_active[uuid].clone(),
                    })
                }
            }
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
            // The crash-recovery arm re-subscribes to a doc this daemon
            // already tracks; it claims a path like any other subscribe, so
            // it obeys the same emit-last rule.
            if matches!(a, StartupAction::SubscribeRemote { .. }) {
                subscribes.push(a);
            } else {
                actions.push(a);
            }
        }
    }

    actions.extend(subscribes);
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
        materialized: &'a HashSet<String>,
        space_root: &'a Path,
    ) -> ReconcileInputs<'a> {
        ReconcileInputs {
            state_entries: state,
            previously_remote,
            remote_active: remote,
            materialized,
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
        let materialized = HashSet::new();
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
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

        let materialized = HashSet::new();
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
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
        // File does NOT exist on disk, but it WAS materialized here (the
        // persisted entry records an inode) — so this is a genuine offline
        // delete, not the crash window.
        let materialized = HashSet::from(["uuid-1".to_owned()]);

        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
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

    /// The crash-mid-bulk observer window: identity claimed + `confirmed`
    /// persisted by the coalesced save, but the doc's content never landed on
    /// disk (no recorded inode) before the crash. Reading the absence as a
    /// local delete unregisters the doc on the relay and DESTROYS it
    /// cluster-wide (8/150 docs lost in the e2e red run). It must re-subscribe
    /// instead.
    #[test]
    fn test_never_materialized_doc_resubscribes_instead_of_unregistering() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("partial.md"), "uuid-1".into());
        // Confirmed (the relay register echo / startup confirm landed and the
        // coalesced save persisted it) — the old was_remote=false crash guard
        // does NOT cover this.
        let prev = HashSet::from(["uuid-1".into()]);
        let mut remote = HashMap::new();
        remote.insert("uuid-1".into(), safe("partial.md"));

        let dir = tempfile::tempdir().unwrap();
        // File NOT on disk, and never materialized (no recorded inode).
        let materialized = HashSet::new();

        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            StartupAction::SubscribeRemote {
                document_id: "uuid-1".into(),
                path: safe("partial.md"),
            },
            "a confirmed-but-never-materialized doc must re-subscribe, not unregister"
        );
    }

    /// A displaced path-arbitration loser across a restart. The relay lists
    /// EFFECTIVE (post-arbitration) paths, so a settled loser is listed at
    /// the same conflict path its state entry keys — the classification is
    /// `SyncLocal` (subscribe + push), NOT `RegisterLocal`. Re-registering it
    /// would mint a fresh stamp at its intended path and re-run the election
    /// this node already lost, flipping the winner on this node alone.
    #[test]
    fn test_displaced_doc_on_relay_syncs_without_reregister() {
        let id = "02ac22fe-77f3-4625-ba73-daf47b71c295";
        let conflict = format!("target.kutl-conflict-{id}.md");
        let mut state = HashMap::new();
        state.insert(PathBuf::from(&conflict), id.into());
        let prev = HashSet::from([id.into()]);
        let mut remote = HashMap::new();
        remote.insert(id.into(), safe(&conflict));

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(&conflict), "loser body\n").unwrap();

        let materialized = HashSet::from([id.to_owned()]);
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            StartupAction::SyncLocal {
                document_id: id.into(),
                path: PathBuf::from(&conflict),
            },
            "a settled loser re-subscribes at its conflict path; it must never \
             re-enter the election it lost"
        );
    }

    /// A displaced doc whose winner renamed AWAY (or which the cluster renamed)
    /// while this daemon was offline: the relay's effective path for it is no
    /// longer this node's recorded conflict path. Conforming to the relay path
    /// (un-displacing) is correct.
    #[test]
    fn test_displaced_doc_renamed_remotely_conforms_to_relay_path() {
        let id = "02ac22fe-77f3-4625-ba73-daf47b71c295";
        let conflict = format!("target.kutl-conflict-{id}.md");
        let mut state = HashMap::new();
        state.insert(PathBuf::from(&conflict), id.into());
        let prev = HashSet::from([id.into()]);
        let mut remote = HashMap::new();
        remote.insert(id.into(), safe("elsewhere.md"));

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(&conflict), "loser body\n").unwrap();

        let materialized = HashSet::from([id.to_owned()]);
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            StartupAction::RenameLocal {
                document_id: id.into(),
                old_path: PathBuf::from(&conflict),
                new_path: safe("elsewhere.md"),
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

        let materialized = HashSet::new();
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
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

        let materialized = HashSet::new();
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
        let actions = reconcile_startup(&inputs);

        assert_eq!(actions.len(), 1);
        assert!(
            matches!(actions[0], StartupAction::RegisterLocal { .. }),
            "a doc the relay has never seen is the one case that registers"
        );
    }

    #[test]
    fn test_stale_entry_cleaned_up() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("stale.md"), "uuid-1".into());
        let prev = HashSet::from(["uuid-1".into()]);
        let remote = HashMap::new();

        let dir = tempfile::tempdir().unwrap();
        // Not on disk, not on relay.

        let materialized = HashSet::new();
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
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

        let materialized = HashSet::new();
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
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

        let materialized = HashSet::new();
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
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

        let materialized = HashSet::new();
        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
        let actions = reconcile_startup(&inputs);

        // Should subscribe (re-download), NOT unregister.
        assert_eq!(actions.len(), 1);
        assert!(
            matches!(actions[0], StartupAction::SubscribeRemote { .. }),
            "crash recovery should subscribe, not unregister: {actions:?}"
        );
    }

    /// Index of the first action matching `pred`, panicking with the full
    /// action list when absent — the ordering tests below all need it.
    fn index_of(
        actions: &[StartupAction],
        what: &str,
        pred: impl Fn(&StartupAction) -> bool,
    ) -> usize {
        actions
            .iter()
            .position(pred)
            .unwrap_or_else(|| panic!("expected {what} in {actions:?}"))
    }

    /// The re-mint shape: while this daemon was offline, the document at
    /// `doc.md` was deleted and a NEW document was created at the same path.
    /// Local state still maps the path to the OLD id; the relay knows only
    /// the NEW id there.
    ///
    /// Every action settling a path's LOCAL disposition must be emitted
    /// before a remote subscribe may claim that path. Executed the other way
    /// round, the subscribe re-points the path's identity at the new id and
    /// the following `DeleteLocal` — resolving by path — destroys the NEW
    /// document's file, state, and identity, leaving an orphaned
    /// subscription that drops every catch-up frame.
    #[test]
    fn test_remint_same_path_emits_local_delete_before_subscribe() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("doc.md"), "old-id".into());
        let prev = HashSet::from(["old-id".into()]);
        let mut remote = HashMap::new();
        remote.insert("new-id".into(), safe("doc.md"));

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("doc.md"), "old content").unwrap();
        let materialized = HashSet::from(["old-id".into()]);

        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
        let actions = reconcile_startup(&inputs);

        let delete = index_of(
            &actions,
            "DeleteLocal(old-id)",
            |a| matches!(a, StartupAction::DeleteLocal { document_id, .. } if document_id == "old-id"),
        );
        let subscribe = index_of(
            &actions,
            "SubscribeRemote(new-id)",
            |a| matches!(a, StartupAction::SubscribeRemote { document_id, .. } if document_id == "new-id"),
        );
        assert!(
            delete < subscribe,
            "local delete must free the path before the remote subscribe claims it: {actions:?}"
        );
    }

    /// Sibling of the re-mint shape: the file was ALSO deleted locally while
    /// offline, so the stale entry resolves to `CleanupState` (the relay no
    /// longer knows the old id either). Same invariant: the local
    /// disposition — which still tears down path-keyed state — is emitted
    /// before the subscribe that claims the path.
    #[test]
    fn test_remint_after_local_delete_emits_cleanup_before_subscribe() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("doc.md"), "old-id".into());
        let prev = HashSet::from(["old-id".into()]);
        let mut remote = HashMap::new();
        remote.insert("new-id".into(), safe("doc.md"));

        let dir = tempfile::tempdir().unwrap();
        // File NOT on disk (deleted locally while offline too).
        let materialized = HashSet::from(["old-id".into()]);

        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
        let actions = reconcile_startup(&inputs);

        let cleanup = index_of(
            &actions,
            "CleanupState(old-id)",
            |a| matches!(a, StartupAction::CleanupState { document_id, .. } if document_id == "old-id"),
        );
        let subscribe = index_of(
            &actions,
            "SubscribeRemote(new-id)",
            |a| matches!(a, StartupAction::SubscribeRemote { document_id, .. } if document_id == "new-id"),
        );
        assert!(
            cleanup < subscribe,
            "the stale-entry cleanup must settle before the remote subscribe claims the path: {actions:?}"
        );
    }

    /// Sibling: the old document was renamed AWAY remotely and a new
    /// document claimed its former path. The rename (which frees the path)
    /// must be emitted before the subscribe that claims it.
    #[test]
    fn test_rename_away_emits_before_subscribe_claims_old_path() {
        let mut state = HashMap::new();
        state.insert(PathBuf::from("doc.md"), "old-id".into());
        let prev = HashSet::from(["old-id".into()]);
        let mut remote = HashMap::new();
        remote.insert("old-id".into(), safe("moved.md"));
        remote.insert("new-id".into(), safe("doc.md"));

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("doc.md"), "content").unwrap();
        let materialized = HashSet::from(["old-id".into()]);

        let inputs = make_inputs(&state, &prev, &remote, &materialized, dir.path());
        let actions = reconcile_startup(&inputs);

        let rename = index_of(
            &actions,
            "RenameLocal(old-id)",
            |a| matches!(a, StartupAction::RenameLocal { document_id, .. } if document_id == "old-id"),
        );
        let subscribe = index_of(
            &actions,
            "SubscribeRemote(new-id)",
            |a| matches!(a, StartupAction::SubscribeRemote { document_id, .. } if document_id == "new-id"),
        );
        assert!(
            rename < subscribe,
            "the rename must free the path before the remote subscribe claims it: {actions:?}"
        );
    }
}
