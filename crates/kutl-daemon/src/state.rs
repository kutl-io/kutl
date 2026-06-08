//! Local daemon state persistence (`.kutl/state.json`).
//!
//! Caches path→document mappings so the daemon can restart quickly without
//! re-querying the relay registry for every known file.
//!
//! Each entry carries a monotone `confirmed` flag — "has the relay acknowledged
//! this document" — which lives *with* the document rather than in a parallel
//! collection. That is the input the startup reconciler's `was_remote` axis
//! reads (see [`crate::reconcile`]): a doc gone from the relay but still on disk
//! is a remote *deletion* to propagate (`confirmed`) versus a never-synced local
//! file to push up (not `confirmed`). Keeping the flag on the record makes it
//! impossible for the two to drift — the bug a separate `remote_document_ids`
//! snapshot caused, where documents learned mid-session were never recorded as
//! confirmed and so were wrongly re-registered on the next start.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use kutl_core::{ActorId, Hlc, Uuid};
use tracing::warn;

use crate::safe_path::SafeRelayPath;

/// A tracked document: its UUID plus whether the relay has acknowledged it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DocEntry {
    /// Document UUID in the relay registry.
    pub id: String,
    /// Whether the relay has confirmed this document exists. Monotone: once the
    /// relay has acknowledged a document (we received its registration or it
    /// appeared in the space's document list) it stays confirmed for the
    /// document's lifetime; only unregistration removes the entry entirely.
    pub confirmed: bool,
    /// Last-known inode of the file at this path. Persisted so a file renamed
    /// while the daemon was *offline* can still be located by inode on restart:
    /// the recorded path is gone, so its inode can no longer be read from disk,
    /// yet the moved file carries the same inode (a rename preserves it). `None`
    /// on legacy state files (defaulted) and on platforms without inodes.
    #[serde(default)]
    pub inode: Option<u64>,
}

/// Persisted hybrid-logical-clock floor: this device's last-emitted stamp's
/// `(physical_ms, logical)`. The `actor` is the device id (stored separately,
/// not duplicated here). Seeds the clock on restart so it never emits a stamp at
/// or below one already emitted before the restart (monotonic restart).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct HlcFloor {
    /// Physical component of the last-emitted stamp. Declaration order matters:
    /// `derive(Ord)` compares `physical_ms` then `logical`.
    pub physical_ms: u64,
    /// Logical component of the last-emitted stamp.
    pub logical: u32,
}

/// Local cache of path → document mappings for fast daemon restarts.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct DaemonState {
    /// Maps relative file paths to their document entry.
    pub documents: HashMap<String, DocEntry>,
    /// Per-install device id (a UUID string), the HLC actor for stamps this
    /// daemon originates. Generated once on first use; distinct per
    /// installation, NOT the account DID — so two devices of one user cannot
    /// collide on identical HLC stamps. `None` on legacy state files (defaulted).
    #[serde(default)]
    pub device_id: Option<String>,
    /// The HLC floor — this device's last-emitted stamp. `None` until the first
    /// stamp. Defaulted on legacy state files.
    #[serde(default)]
    pub hlc_floor: Option<HlcFloor>,
}

/// Legacy on-disk shape (path → UUID string, plus a parallel confirmed-UUID
/// list). Read once for migration to [`DaemonState`].
#[derive(serde::Deserialize)]
struct LegacyState {
    documents: HashMap<String, String>,
    #[serde(default)]
    remote_document_ids: Vec<String>,
}

/// Name of the state file within the `.kutl` directory.
const STATE_FILE: &str = "state.json";

impl DaemonState {
    /// Load state from the `.kutl` directory. Returns an empty state if the
    /// file is missing or corrupted. Transparently migrates the legacy format
    /// (separate `documents` map + `remote_document_ids` list).
    pub fn load(kutl_dir: &Path) -> Self {
        let path = kutl_dir.join(STATE_FILE);
        let Ok(data) = std::fs::read_to_string(&path) else {
            return Self::empty();
        };
        // Current format.
        if let Ok(state) = serde_json::from_str::<Self>(&data) {
            return state;
        }
        // Legacy format → migrate (confirmed = was in remote_document_ids).
        if let Ok(legacy) = serde_json::from_str::<LegacyState>(&data) {
            return Self::from_legacy(legacy);
        }
        warn!("corrupt state file, starting fresh");
        Self::empty()
    }

    fn from_legacy(legacy: LegacyState) -> Self {
        let remote: HashSet<&str> = legacy
            .remote_document_ids
            .iter()
            .map(String::as_str)
            .collect();
        let documents = legacy
            .documents
            .into_iter()
            .map(|(path, id)| {
                let confirmed = remote.contains(id.as_str());
                (
                    path,
                    DocEntry {
                        id,
                        confirmed,
                        inode: None,
                    },
                )
            })
            .collect();
        Self {
            documents,
            ..Default::default()
        }
    }

    /// Persist state to the `.kutl` directory.
    ///
    /// Uses atomic write (temp file + rename) to prevent corruption on crash.
    /// Creates the directory if it doesn't exist.
    pub fn save(&self, kutl_dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(kutl_dir)?;
        let path = kutl_dir.join(STATE_FILE);
        let data = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        let tmp = tempfile::NamedTempFile::new_in(kutl_dir)?;
        std::fs::write(tmp.path(), data)?;
        tmp.persist(path).map_err(|e| e.error)?;
        Ok(())
    }

    /// Return `path → UUID` for documents whose paths pass `SafeRelayPath`
    /// validation. Malicious or corrupt paths (traversal, absolute,
    /// `.kutl`-prefixed) are logged and skipped.
    pub fn validated_documents(&self) -> HashMap<String, String> {
        self.documents
            .iter()
            .filter(|(path, _)| match SafeRelayPath::new(path) {
                Ok(_) => true,
                Err(e) => {
                    warn!(path, "skipping invalid path in state.json: {e}");
                    false
                }
            })
            .map(|(k, v)| (k.clone(), v.id.clone()))
            .collect()
    }

    /// UUIDs the relay has acknowledged — the reconciler's `was_remote` set.
    pub fn confirmed_ids(&self) -> HashSet<String> {
        self.documents
            .values()
            .filter(|e| e.confirmed)
            .map(|e| e.id.clone())
            .collect()
    }

    /// Insert or update the entry for `path`. `confirmed` joins monotonically
    /// with any existing value — a confirmed document never reverts.
    pub fn set(&mut self, path: String, id: String, confirmed: bool) {
        let existing = self.documents.get(&path);
        let confirmed = confirmed || existing.is_some_and(|e| e.confirmed);
        // Carry any known inode forward; the live value is re-synced from
        // `file_identity` on every persist (see daemon `save_state`).
        let inode = existing.and_then(|e| e.inode);
        self.documents.insert(
            path,
            DocEntry {
                id,
                confirmed,
                inode,
            },
        );
    }

    /// Record the inode for the document at `path`, if an entry exists. The
    /// daemon syncs live inodes here before each persist so a file renamed while
    /// the daemon is later offline remains locatable by inode across the restart.
    pub fn set_inode(&mut self, path: &str, inode: Option<u64>) {
        if let Some(entry) = self.documents.get_mut(path) {
            entry.inode = inode;
        }
    }

    /// Mark the document at `path` as confirmed by the relay (monotone join).
    /// Returns true only on an actual `false → true` transition, so callers can
    /// skip a redundant persist.
    pub fn confirm(&mut self, path: &str) -> bool {
        match self.documents.get_mut(path) {
            Some(entry) if !entry.confirmed => {
                entry.confirmed = true;
                true
            }
            _ => false,
        }
    }

    /// The per-install device actor for HLC stamping, generated and stored on
    /// first use (and on a corrupt/unparseable stored id). The caller persists
    /// the state after first use so the id is stable across restarts.
    pub fn ensure_device_actor(&mut self) -> ActorId {
        let id = self
            .device_id
            .as_deref()
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or_else(|| {
                let fresh = Uuid::new_v4();
                self.device_id = Some(fresh.to_string());
                fresh
            });
        ActorId(id)
    }

    /// Record an emitted HLC as the new floor. Monotone — only ever advances,
    /// so a redundant or out-of-order record cannot regress the floor.
    pub fn record_emitted_hlc(&mut self, hlc: Hlc) {
        let next = HlcFloor {
            physical_ms: hlc.physical_ms,
            logical: hlc.logical,
        };
        if self.hlc_floor.is_none_or(|cur| next > cur) {
            self.hlc_floor = Some(next);
        }
    }

    /// Create an empty state.
    pub fn empty() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, confirmed: bool) -> DocEntry {
        DocEntry {
            id: id.to_owned(),
            confirmed,
            inode: None,
        }
    }

    #[test]
    fn test_validated_documents_filters_unsafe_paths() {
        let state = DaemonState {
            documents: HashMap::from([
                ("notes/readme.md".to_owned(), entry("uuid-1", true)),
                ("../../../etc/passwd".to_owned(), entry("uuid-2", true)),
                ("/absolute/path.md".to_owned(), entry("uuid-3", false)),
                (".kutl/state.json".to_owned(), entry("uuid-4", false)),
            ]),
            ..Default::default()
        };

        let safe = state.validated_documents();
        assert_eq!(safe.len(), 1);
        assert_eq!(safe.get("notes/readme.md").unwrap(), "uuid-1");
    }

    #[test]
    fn test_save_and_load_round_trip_preserves_confirmed() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");

        let mut state = DaemonState::empty();
        state.set("notes/ideas.md".into(), "uuid-1".into(), true);
        state.set("draft.md".into(), "uuid-2".into(), false);
        state.save(&kutl_dir).unwrap();

        let loaded = DaemonState::load(&kutl_dir);
        assert_eq!(loaded.documents.len(), 2);
        assert!(loaded.documents.get("notes/ideas.md").unwrap().confirmed);
        assert!(!loaded.documents.get("draft.md").unwrap().confirmed);
        assert_eq!(
            loaded.confirmed_ids(),
            HashSet::from(["uuid-1".to_owned()]),
            "only the relay-confirmed doc is in the was_remote set"
        );
    }

    #[test]
    fn test_inode_persists_across_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");

        let mut state = DaemonState::empty();
        state.set("doc.md".into(), "uuid-1".into(), true);
        // The daemon syncs live inodes via set_inode before each persist.
        state.set_inode("doc.md", Some(4242));
        state.save(&kutl_dir).unwrap();

        // A renamed-away file's inode must survive the restart — it is the only
        // way to locate the moved file once its recorded path is gone.
        let loaded = DaemonState::load(&kutl_dir);
        assert_eq!(loaded.documents.get("doc.md").unwrap().inode, Some(4242));

        // set() preserves a known inode rather than clobbering it to None.
        let mut reloaded = loaded;
        reloaded.set("doc.md".into(), "uuid-1".into(), true);
        assert_eq!(
            reloaded.documents.get("doc.md").unwrap().inode,
            Some(4242),
            "set must carry the existing inode forward"
        );
    }

    #[test]
    fn test_confirm_is_monotone() {
        let mut state = DaemonState::empty();
        state.set("a.md".into(), "uuid-1".into(), true);
        // A later set with confirmed=false must NOT revert the confirmed flag.
        state.set("a.md".into(), "uuid-1".into(), false);
        assert!(
            state.documents.get("a.md").unwrap().confirmed,
            "confirmed must not revert to false"
        );
        // confirm() reports only an actual transition: already-confirmed and
        // missing both return false.
        assert!(!state.confirm("a.md"));
        assert!(!state.confirm("missing.md"));
        state.set("b.md".into(), "uuid-2".into(), false);
        assert!(state.confirm("b.md"), "first confirmation is a transition");
        assert!(!state.confirm("b.md"), "second confirmation is a no-op");
    }

    #[test]
    fn test_load_missing_returns_empty() {
        let state = DaemonState::load(Path::new("/nonexistent/.kutl"));
        assert!(state.documents.is_empty());
    }

    #[test]
    fn test_load_legacy_migrates_confirmed_from_remote_ids() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        // Legacy format: documents map + parallel remote_document_ids list.
        std::fs::write(
            kutl_dir.join(STATE_FILE),
            r#"{"documents":{"synced.md":"uuid-1","local.md":"uuid-2"},"remote_document_ids":["uuid-1"]}"#,
        )
        .unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert_eq!(state.documents.len(), 2);
        assert!(
            state.documents.get("synced.md").unwrap().confirmed,
            "doc in remote_document_ids migrates to confirmed"
        );
        assert!(
            !state.documents.get("local.md").unwrap().confirmed,
            "doc absent from remote_document_ids migrates to unconfirmed"
        );
    }

    #[test]
    fn test_load_legacy_without_remote_ids_field() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        // Oldest format: documents only, no remote_document_ids.
        std::fs::write(
            kutl_dir.join(STATE_FILE),
            r#"{"documents":{"doc.md":"uuid-1"}}"#,
        )
        .unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert_eq!(state.documents.len(), 1);
        assert!(
            !state.documents.get("doc.md").unwrap().confirmed,
            "no remote_document_ids → unconfirmed (will re-confirm on next sync)"
        );
    }

    #[test]
    fn test_load_corrupted_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        std::fs::write(kutl_dir.join(STATE_FILE), "not valid json").unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert!(state.documents.is_empty());
    }

    fn hlc(physical_ms: u64, logical: u32) -> Hlc {
        Hlc {
            physical_ms,
            logical,
            actor: ActorId(Uuid::nil()),
        }
    }

    #[test]
    fn test_device_actor_generated_and_stable_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");

        let mut state = DaemonState::empty();
        assert!(state.device_id.is_none());
        let actor1 = state.ensure_device_actor();
        assert!(
            state.device_id.is_some(),
            "first use generates and stores it"
        );
        // Idempotent within a session.
        assert_eq!(actor1, state.ensure_device_actor());
        state.save(&kutl_dir).unwrap();

        // Survives a restart unchanged.
        let mut reloaded = DaemonState::load(&kutl_dir);
        assert_eq!(
            reloaded.ensure_device_actor(),
            actor1,
            "device actor is stable across restarts"
        );
    }

    #[test]
    fn test_device_actor_regenerated_when_corrupt() {
        let mut state = DaemonState {
            device_id: Some("not-a-uuid".to_owned()),
            ..Default::default()
        };
        let actor = state.ensure_device_actor();
        assert_ne!(actor.0, Uuid::nil());
        assert_eq!(
            state.device_id.as_deref(),
            Some(actor.0.to_string().as_str()),
            "a corrupt stored id is replaced with a fresh valid one"
        );
    }

    #[test]
    fn test_hlc_floor_is_monotone() {
        let mut state = DaemonState::empty();
        assert!(state.hlc_floor.is_none());
        state.record_emitted_hlc(hlc(100, 0));
        assert_eq!(
            state.hlc_floor,
            Some(HlcFloor {
                physical_ms: 100,
                logical: 0
            })
        );
        // Advances on a later stamp.
        state.record_emitted_hlc(hlc(100, 5));
        assert_eq!(
            state.hlc_floor,
            Some(HlcFloor {
                physical_ms: 100,
                logical: 5
            })
        );
        // Never regresses on an out-of-order/older stamp.
        state.record_emitted_hlc(hlc(50, 9));
        assert_eq!(
            state.hlc_floor,
            Some(HlcFloor {
                physical_ms: 100,
                logical: 5
            }),
            "the floor only advances"
        );
        state.record_emitted_hlc(hlc(101, 0));
        assert_eq!(
            state.hlc_floor,
            Some(HlcFloor {
                physical_ms: 101,
                logical: 0
            })
        );
    }

    #[test]
    fn test_legacy_state_loads_with_no_device_id_or_floor() {
        let dir = tempfile::tempdir().unwrap();
        let kutl_dir = dir.path().join(".kutl");
        std::fs::create_dir_all(&kutl_dir).unwrap();
        std::fs::write(
            kutl_dir.join(STATE_FILE),
            r#"{"documents":{"doc.md":{"id":"uuid-1","confirmed":true}}}"#,
        )
        .unwrap();

        let state = DaemonState::load(&kutl_dir);
        assert_eq!(state.documents.len(), 1);
        assert!(state.device_id.is_none(), "legacy file → no device id yet");
        assert!(state.hlc_floor.is_none(), "legacy file → no floor yet");
    }
}
