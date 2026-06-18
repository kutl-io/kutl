//! Relay-maintained document registry for UUID-based document identity.
//!
//! Each space has its own [`DocumentRegistry`]. Lifecycle conflict resolution is
//! a join-semilattice: register/rename/unregister build [`DocRecord`]s and feed
//! them to a [`RegistryLattice`], which HLC-merges per document and arbitrates
//! path collisions (conflict-copy). The registry is **total** — it never rejects
//! on conflict; concurrent operations converge by the lattice. The [`entries`]
//! map carries provenance + a projection of each record's path/lifecycle for the
//! persistence mirror and the read API.
//!
//! [`entries`]: DocumentRegistry::entries

use std::collections::HashMap;

use kutl_core::lattice::{DocRecord, LifecycleProjection, RegistryLattice};
use kutl_core::{ActorId, Hlc, Uuid};
use tracing::{error, warn};

/// HLC actor for ops that carry no origin HLC (pre-HLC clients, internal
/// callers): an all-zeros UUID, which sorts below any real device actor — so a
/// genuine HLC-stamped op wins a `(physical_ms, logical)` tie against it. Such
/// ops order purely by `physical_ms` (wall-clock), the strictly-less-wrong
/// fallback.
const LEGACY_ACTOR: ActorId = ActorId(Uuid::nil());

/// The origin HLC for a metadata, falling back to a `(timestamp, 0,
/// LEGACY_ACTOR)` stamp when none was supplied on the wire.
fn hlc_of(meta: &EntryMetadata) -> Hlc {
    meta.hlc.unwrap_or(Hlc {
        physical_ms: u64::try_from(meta.timestamp).unwrap_or(0),
        logical: 0,
        actor: LEGACY_ACTOR,
    })
}

fn millis(h: Option<Hlc>) -> Option<i64> {
    h.map(|h| i64::try_from(h.physical_ms).unwrap_or(i64::MAX))
}

/// Source provenance metadata, optional on every register.
///
/// The daemon path populates `originally_created_at_ms` only (filesystem
/// birthtime). The ingestion worker populates all six fields. Internal
/// callers (MCP, presence) pass [`Default::default`].
#[derive(Debug, Default, Clone)]
pub struct RegisterRfd0081 {
    /// Filesystem birthtime in Unix millis (daemon path).
    pub originally_created_at_ms: Option<i64>,
    /// `SourceKind` enum value (proto-encoded as `u32`).
    pub source_kind: Option<u32>,
    /// Source-specific identifier (e.g. Notion page id).
    pub source_id: Option<String>,
    /// Canonical URL at the source.
    pub source_url: Option<String>,
    /// Ingestion job that produced this document (UUID string).
    pub ingestion_job_id: Option<String>,
    /// Author label as reported by the source (free-form display).
    pub source_author_display: Option<String>,
}

/// Metadata for a registry operation (who did it, when, and the origin HLC).
#[derive(Debug, Clone, Default)]
pub struct EntryMetadata {
    /// DID of the actor who performed the operation.
    pub author_did: String,
    /// Unix millis timestamp (display / mirror; equals the HLC `physical_ms`).
    pub timestamp: i64,
    /// Origin HLC — the authoritative lifecycle order. `None` from pre-HLC
    /// clients or internal callers; [`hlc_of`] then synthesizes one from
    /// `timestamp`.
    pub hlc: Option<Hlc>,
    /// Resolved account UUID from the authenticated connection.
    pub account_id: Option<String>,
    /// Filesystem birthtime in Unix millis. Set on register operations
    /// when the daemon's host filesystem exposes a creation time;
    /// `None` otherwise. Ignored on rename/unregister (the registry
    /// stores it once at register time).
    pub originally_created_at: Option<i64>,
    /// `SourceKind` enum value (the proto enum encoded as `u32`).
    pub source_kind: Option<u32>,
    /// Source-specific identifier (e.g. Notion page id). `None` on
    /// the daemon path.
    pub source_id: Option<String>,
    /// Canonical URL at the source (e.g. notion.so/page). `None` on
    /// the daemon path.
    pub source_url: Option<String>,
    /// Ingestion job that produced this document. `None` on the
    /// daemon path.
    pub ingestion_job_id: Option<String>,
    /// Author label as reported by the source (free-form display
    /// string, not a kutl DID). `None` on the daemon path.
    pub source_author_display: Option<String>,
}

/// A single entry in the document registry.
///
/// Path and the lifecycle millis (`created_at`/`renamed_at`/`deleted_at`/
/// `edited_at`) are a **projection** of the document's merged [`DocRecord`] —
/// synced after every mutation, not mutated independently. Provenance fields
/// (`created_by`, `account_id`, `source_*`, `renamed_by`) are set by the
/// operation that wins and preserved across merges.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RegistryEntry {
    /// Unique document identifier (UUID).
    pub document_id: String,
    /// Current effective path within the space (conflict path while displaced).
    pub path: String,
    /// DID of the creator.
    pub created_by: String,
    /// Unix millis when the document was registered.
    pub created_at: i64,
    /// DID of the last renamer, if ever renamed.
    pub renamed_by: Option<String>,
    /// Unix millis of the last rename, if ever renamed.
    pub renamed_at: Option<i64>,
    /// Unix millis of the last rename's causal floor — the `registered_hlc` the
    /// renamer observed when it renamed (see [`DocRecord::rename_causal_floor`]).
    /// Persisted so a rename that beat a clock-skewed registration via the floor
    /// still supersedes after a relay restart (without it, a post-restart re-
    /// arbitration could flip the path back to the future-stamped register).
    /// `None` if never renamed, or the rename predates the floor.
    pub rename_causal_floor_at: Option<i64>,
    /// Unix millis of soft-deletion, if deleted.
    pub deleted_at: Option<i64>,
    /// Resolved account UUID (DB-backed mode). `None` without a DB.
    pub account_id: Option<String>,
    /// Unix millis of the last content edit, if any.
    pub edited_at: Option<i64>,
    /// Filesystem birthtime in Unix millis. Set once at register time
    /// from the daemon's `stat`; never updated.
    pub originally_created_at: Option<i64>,
    /// `SourceKind` enum value. `None` for native registers (daemon
    /// path); the persistence layer substitutes 0.
    pub source_kind: Option<u32>,
    /// Source identifier. `None` for native registers.
    pub source_id: Option<String>,
    /// Source URL. `None` for native registers.
    pub source_url: Option<String>,
    /// Ingestion job id. `None` for native registers.
    pub ingestion_job_id: Option<String>,
    /// Source author display name. `None` for native registers.
    pub source_author_display: Option<String>,
}

/// Captured prior state for capture-restore rollback. The lattice merge is a
/// max and is **not invertible**, so a persist failure after an in-memory
/// `observe` is undone by restoring the snapshot of every record/entry the
/// operation could have touched (the document plus any path-collision peers).
#[derive(Debug, Default)]
pub struct RollbackToken {
    prior: Vec<(Uuid, Option<DocRecord>, Option<RegistryEntry>)>,
}

/// Relay-maintained document registry for a single space.
pub struct DocumentRegistry {
    /// Lifecycle conflict-resolution core: per-document `DocRecord`s with
    /// cross-document path arbitration.
    lattice: RegistryLattice,
    /// Provenance + projected lifecycle, keyed by document UUID.
    entries: HashMap<Uuid, RegistryEntry>,
    /// Cached effective-path → id projection, rebuilt after every mutation.
    path_index: HashMap<String, Uuid>,
    /// Lifecycle-divergence detector: the number of times a
    /// projection has been observed to violate the "at most one alive document
    /// per effective path" invariant. Confluent arbitration should keep this at
    /// zero forever; a nonzero value means a confluence premise was violated and
    /// the registry would otherwise serve two canonical holders for one path —
    /// it must not be silent (RFD 0066 precedent for content). A monotonic
    /// backstop counter, surfaced via [`Self::divergence_count`].
    divergence_count: u64,
}

impl DocumentRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            lattice: RegistryLattice::new(),
            entries: HashMap::new(),
            path_index: HashMap::new(),
            divergence_count: 0,
        }
    }

    /// Build a registry from persisted entries (millis-based). Each entry's
    /// lifecycle millis are reconstructed into an HLC-valued `DocRecord` (with
    /// `LEGACY_ACTOR`), so a reloaded registry merges consistently with new
    /// HLC-stamped ops. Persisted rows are mutually non-colliding (the DB's path
    /// uniqueness), so arbitration is a no-op on load.
    #[must_use]
    pub fn from_entries(entries: Vec<RegistryEntry>) -> Self {
        let mut reg = Self::new();
        // Insert every record then arbitrate ONCE (observe_all), instead of
        // observe-per-row which re-arbitrates the whole growing set — O(n) load
        // rather than O(n²). The trailing reproject() builds the entry projection
        // + path index from the single arbitration pass.
        let mut records = Vec::with_capacity(entries.len());
        for entry in entries {
            let Some(did) = parse_id(&entry.document_id) else {
                warn!(id = %entry.document_id, "skipping persisted entry with non-UUID id");
                continue;
            };
            records.push(record_from_entry(did, &entry));
            reg.entries.insert(did, entry);
        }
        reg.lattice.observe_all(records);
        reg.reproject();
        reg
    }

    /// Capture the prior state of the documents an operation on `id` targeting
    /// `path` could touch: `id` itself plus any alive record sharing the
    /// (case-folded) target path.
    fn capture(&self, id: Uuid, path: Option<&str>) -> RollbackToken {
        let mut ids = vec![id];
        if let Some(p) = path {
            let folded = p.to_lowercase();
            for (other, entry) in &self.entries {
                if *other != id && entry.deleted_at.is_none() && entry.path.to_lowercase() == folded
                {
                    ids.push(*other);
                }
            }
        }
        RollbackToken {
            prior: ids
                .into_iter()
                .map(|i| {
                    (
                        i,
                        self.lattice.get(&i).cloned(),
                        self.entries.get(&i).cloned(),
                    )
                })
                .collect(),
        }
    }

    /// Undo an `observe` whose persistence failed, restoring the snapshot.
    pub fn restore(&mut self, token: RollbackToken) {
        for (id, record, entry) in token.prior {
            self.lattice.restore_record(id, record);
            match entry {
                Some(e) => {
                    self.entries.insert(id, e);
                }
                None => {
                    self.entries.remove(&id);
                }
            }
        }
        self.reproject();
    }

    /// Register a document. Total — a path already held by another document is
    /// not rejected; the lower-priority document is conflict-copied. Returns the
    /// rollback token for the caller's persist path, plus the **other** documents
    /// this register displaced to their conflict paths (path arbitration). The
    /// caller must persist those displaced losers — off the contested path —
    /// before persisting this document, so the winner's claim does not collide
    /// with a loser's stale row under `UNIQUE(lower(path))`. Usually empty.
    pub fn register(
        &mut self,
        document_id: &str,
        path: &str,
        meta: EntryMetadata,
    ) -> (RollbackToken, Vec<String>) {
        let Some(did) = parse_id(document_id) else {
            warn!(id = %document_id, "register: non-UUID document id, ignoring");
            return (RollbackToken::default(), Vec::new());
        };
        let token = self.capture(did, Some(path));
        let hlc = hlc_of(&meta);
        self.lattice.observe(DocRecord {
            document_id: did,
            path: path.to_owned(),
            registered_hlc: Some(hlc),
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: None,
            displaced: false,
        });
        // Provenance: create on first register; preserve on re-register/revival.
        // `move` consumes `meta` into the new entry (closure runs only when
        // vacant; on re-register the existing provenance is untouched).
        self.entries
            .entry(did)
            .or_insert_with(move || RegistryEntry {
                document_id: document_id.to_owned(),
                created_by: meta.author_did,
                created_at: meta.timestamp,
                account_id: meta.account_id,
                originally_created_at: meta.originally_created_at,
                source_kind: meta.source_kind,
                source_id: meta.source_id,
                source_url: meta.source_url,
                ingestion_job_id: meta.ingestion_job_id,
                source_author_display: meta.source_author_display,
                ..Default::default()
            });
        let changed = self.reproject();
        (token, Self::displaced_others(did, changed))
    }

    /// Rename a document. Total — concurrent renames of the same document
    /// resolve by HLC (last writer wins); a rename onto a path held by a
    /// *different* document conflict-copies the loser. `old_path` is advisory and
    /// ignored (identity is the document id).
    pub fn rename(
        &mut self,
        document_id: &str,
        _old_path: &str,
        new_path: &str,
        meta: EntryMetadata,
        rename_causal_floor: Option<Hlc>,
    ) -> (RollbackToken, Vec<String>) {
        let Some(did) = parse_id(document_id) else {
            warn!(id = %document_id, "rename: non-UUID document id, ignoring");
            return (RollbackToken::default(), Vec::new());
        };
        let token = self.capture(did, Some(new_path));
        let hlc = hlc_of(&meta);
        // This rename won iff the merged record's `renamed_hlc` is the stamp we
        // supplied; `observe` returns the merged record so there's no re-lookup.
        let won = self
            .lattice
            .observe(DocRecord {
                document_id: did,
                path: new_path.to_owned(),
                registered_hlc: None,
                renamed_hlc: Some(hlc),
                // Causal floor: the register HLC the renamer observed, so a
                // rename of a clock-skewed registrant's doc supersedes the
                // future-stamped register even with a lower `renamed_hlc`.
                rename_causal_floor,
                touched_hlc: None,
                deleted_hlc: None,
                displaced: false,
            })
            .renamed_hlc
            == Some(hlc);
        // Track the winning renamer's author (lives on the entry, not the lattice).
        if won && let Some(entry) = self.entries.get_mut(&did) {
            entry.renamed_by = Some(meta.author_did);
        }
        let changed = self.reproject();
        (token, Self::displaced_others(did, changed))
    }

    /// From a `reproject` change set, the ids OTHER than the operated document —
    /// i.e. documents this operation displaced to a conflict path. These must be
    /// persisted (off their old paths) before the operated document.
    fn displaced_others(operated: Uuid, changed: Vec<Uuid>) -> Vec<String> {
        changed
            .into_iter()
            .filter(|id| *id != operated)
            .map(|id| id.to_string())
            .collect()
    }

    /// Soft-delete a document. Total — re-deleting is a no-op merge.
    pub fn unregister(&mut self, document_id: &str, meta: &EntryMetadata) -> RollbackToken {
        let Some(did) = parse_id(document_id) else {
            warn!(id = %document_id, "unregister: non-UUID document id, ignoring");
            return RollbackToken::default();
        };
        let token = self.capture(did, None);
        let hlc = hlc_of(meta);
        // Merge a deletion onto the existing record (keep its path).
        let path = self
            .lattice
            .get(&did)
            .map_or_else(String::new, |r| r.current_path().to_owned());
        self.lattice.observe(DocRecord {
            document_id: did,
            path,
            registered_hlc: None,
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: None,
            deleted_hlc: Some(hlc),
            displaced: false,
        });
        self.reproject();
        token
    }

    /// Unregister many documents in one pass — O(documents) total instead of the
    /// O(documents²) of calling [`unregister`](Self::unregister) once per id
    /// (each of which re-arbitrates and re-projects the whole space). Used by
    /// bulk space deletion.
    ///
    /// The result is byte-identical to N sequential `unregister` calls: a
    /// deletion only sets `deleted_hlc` and keeps the document's path, so it
    /// changes neither `current_path` nor `path_hlc` — the inputs to path
    /// arbitration. Arbitrating once over all N deletions therefore elects the
    /// same winners (and the same `displaced` flags) as arbitrating after each
    /// one. The single combined [`RollbackToken`] restores every targeted
    /// record/entry if the caller's persist fails, matching the all-or-nothing
    /// in-memory mutation.
    pub fn unregister_many(
        &mut self,
        document_ids: &[String],
        meta: &EntryMetadata,
    ) -> RollbackToken {
        let hlc = hlc_of(meta);
        let dids: Vec<Uuid> = document_ids
            .iter()
            .filter_map(|id| {
                parse_id(id).or_else(|| {
                    warn!(id = %id, "unregister_many: non-UUID document id, ignoring");
                    None
                })
            })
            .collect();
        // One combined rollback snapshot of every targeted record + entry. A
        // deletion can't alter another record's arbitration (it leaves
        // `current_path`/`path_hlc` untouched), so capturing just the targets is
        // sufficient — the same reasoning behind `unregister` capturing with
        // `path = None`.
        let token = RollbackToken {
            prior: dids
                .iter()
                .map(|i| {
                    (
                        *i,
                        self.lattice.get(i).cloned(),
                        self.entries.get(i).cloned(),
                    )
                })
                .collect(),
        };
        // Build one deletion record per target (each keeps its current path),
        // merge them all, then arbitrate + reproject exactly ONCE.
        let deletions: Vec<DocRecord> = dids
            .iter()
            .map(|did| {
                let path = self
                    .lattice
                    .get(did)
                    .map_or_else(String::new, |r| r.current_path().to_owned());
                DocRecord {
                    document_id: *did,
                    path,
                    registered_hlc: None,
                    renamed_hlc: None,
                    rename_causal_floor: None,
                    touched_hlc: None,
                    deleted_hlc: Some(hlc),
                    displaced: false,
                }
            })
            .collect();
        self.lattice.observe_all(deletions);
        self.reproject();
        token
    }

    /// Record a content edit as a lifecycle TOUCH at the edit's origin `hlc`.
    ///
    /// This is the edit-revives mechanism: a content edit
    /// raises the document's `touched_hlc`, so an edit whose HLC exceeds a
    /// concurrent delete's `deleted_hlc` keeps the document alive
    /// ([`DocRecord::is_alive`]). The touch does NOT move the path (an edit is not
    /// a rename — `path_priority` ignores `touched_hlc`), so a no-collision edit is
    /// observationally inert except for liveness. Pure lattice merge: idempotent,
    /// commutative, and stamped with the EDIT'S ORIGIN hlc (not a fresh one), so an
    /// offline edit correctly carries its old time and cannot revive past a
    /// causally-later delete.
    pub fn touch(&mut self, document_id: &str, hlc: Hlc) {
        let Some(did) = parse_id(document_id) else {
            warn!(id = %document_id, "touch: non-UUID document id, ignoring");
            return;
        };
        // Fast path (the steady-state common case): an edit-touch on an alive,
        // undisplaced document raises only its `touched_hlc`, which feeds
        // liveness but not `path_hlc` — so it cannot change arbitration (any
        // record's displaced flag or effective path) or the path index. Update
        // the single record + its entry's `edited_at` in place and skip the
        // O(docs) observe/reproject/detect_divergence. The full merge is reserved
        // for the cases a touch can actually change the projection: a deleted doc
        // (a touch may revive it → effective-path-index membership changes) or a
        // displaced one. (Verified equivalent to the slow path by
        // `test_touch_fast_path_matches_slow_path` + the core lattice test.)
        if self
            .lattice
            .get(&did)
            .is_some_and(|r| r.is_alive() && !r.displaced)
        {
            self.lattice.touch_in_place(&did, hlc);
            if let Some(entry) = self.entries.get_mut(&did) {
                entry.edited_at = millis(Some(hlc));
            }
            return;
        }

        // Slow path: revival / displaced / unknown — keep the current path and
        // re-arbitrate.
        let path = self
            .lattice
            .get(&did)
            .map_or_else(String::new, |r| r.current_path().to_owned());
        self.lattice.observe(DocRecord {
            document_id: did,
            path,
            registered_hlc: None,
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: Some(hlc),
            deleted_hlc: None,
            displaced: false,
        });
        self.reproject();
    }

    /// Sync the projection: every entry's path/lifecycle millis from its merged
    /// record, and the path index. Creates a minimal entry for any record
    /// lacking one (e.g. a rename of a never-registered id).
    ///
    /// Returns the ids whose **effective path changed** in this pass. The caller
    /// uses this to persist every moved entry, not just the operated document:
    /// when an operation displaces a *different* document to its conflict path
    /// (path arbitration), that displaced loser's row must be written off the
    /// contested path before the winner can claim it — otherwise the DB's
    /// `UNIQUE(lower(path))` rejects the winner. See `DocumentRegistry::register`.
    ///
    /// O(documents); a per-touched-document incremental projection is the next
    /// optimization (the flagged bulk-burst liveness concern), not required for
    /// correctness.
    fn reproject(&mut self) -> Vec<Uuid> {
        // Collect each record's PROJECTION (not a full DocRecord clone) to release
        // the `self.lattice` borrow before mutating `self.entries`. project()
        // already allocates the effective-path String we move into the entry, so
        // this is one allocation per record instead of cloning the whole record
        // (its path String + four HLCs) and then projecting.
        let projections: Vec<(Uuid, LifecycleProjection)> = self
            .lattice
            .records()
            .map(|(id, r)| (*id, r.project()))
            .collect();
        let mut path_changed = Vec::new();
        for (id, p) in projections {
            let entry = self.entries.entry(id).or_insert_with(|| RegistryEntry {
                document_id: id.to_string(),
                ..Default::default()
            });
            // `p` (effective path, revival-aware tombstone) comes from
            // `DocRecord::project`; here we only map its stamps to millis.
            if entry.path != p.effective_path {
                path_changed.push(id);
            }
            entry.path = p.effective_path;
            entry.created_at = millis(p.registered_hlc).unwrap_or(0);
            entry.renamed_at = millis(p.renamed_hlc);
            entry.rename_causal_floor_at = millis(p.rename_causal_floor);
            entry.deleted_at = millis(p.deleted_hlc);
            entry.edited_at = millis(p.touched_hlc);
        }
        // `effective_path_index` collects into a `HashMap`, which silently keeps
        // the LAST value on a duplicate key — so if arbitration ever (via a
        // future bug) left two alive records at one effective path, this index
        // would serve one and drop the other with no signal. That silent
        // collapse is intentional here; `detect_divergence` below (which iterates
        // `self.entries`, NOT this collapsed index) is the authoritative,
        // fail-loud divergence detector. Do not rely on `path_index.len()` to
        // catch a double-book.
        self.path_index = self.lattice.effective_path_index().into_iter().collect();
        self.detect_divergence();
        path_changed
    }

    /// Lifecycle-divergence detector: assert the projection's
    /// invariant — at most one ALIVE document resolves to any one effective path
    /// — and count + log every violation rather than silently serving an
    /// inconsistent registry. Confluent arbitration ([`RegistryLattice`]) keeps
    /// this quiet; a fire means a confluence premise was violated (a future
    /// arbitration bug, or a corrupt persisted reload), which RFD 0066's
    /// fail-loud precedent says must be observable, not silent. O(documents),
    /// in step with `reproject`.
    fn detect_divergence(&mut self) {
        // Fold case to match `arbitrate`'s case-INSENSITIVE grouping (and the
        // DB's `UNIQUE(lower(path))`): the safety net's notion of "same path"
        // must match the arbitration that guarantees the invariant, or two alive
        // records differing only in case would slip past the detector while the
        // mirror's lower(path) uniqueness rejects one of them.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut violations = 0u64;
        for entry in self.entries.values() {
            if entry.deleted_at.is_none() && !seen.insert(entry.path.to_lowercase()) {
                violations += 1;
                error!(
                    path = %entry.path,
                    document_id = %entry.document_id,
                    "lifecycle divergence: duplicate alive document at an effective path already held"
                );
            }
        }
        self.divergence_count += violations;
    }

    /// The lifecycle-divergence counter: how many times the
    /// projection invariant (one alive document per effective path) has been
    /// observed violated. Zero in all correct operation; a nonzero value is the
    /// queryable, non-silent signal that a confluence premise broke.
    #[must_use]
    pub fn divergence_count(&self) -> u64 {
        self.divergence_count
    }

    /// Transfer `account_id` for every entry (active + soft-deleted) to a new
    /// account. Provenance-only; does not touch the lattice.
    pub fn transfer_ownership(&mut self, new_account_id: &str) {
        for entry in self.entries.values_mut() {
            entry.account_id = Some(new_account_id.to_owned());
        }
    }

    /// Look up an active (non-deleted) document by ID.
    #[must_use]
    pub fn get(&self, document_id: &str) -> Option<&RegistryEntry> {
        parse_id(document_id)
            .and_then(|id| self.entries.get(&id))
            .filter(|e| e.deleted_at.is_none())
    }

    /// Look up any document by ID, including soft-deleted entries.
    #[must_use]
    pub fn get_any(&self, document_id: &str) -> Option<&RegistryEntry> {
        parse_id(document_id).and_then(|id| self.entries.get(&id))
    }

    /// Mutable lookup of any entry by ID, including soft-deleted ones. Used by
    /// callers that merge in late-arriving metadata (provenance leave-as-is-on-
    /// omit semantics, RFD 0083). Provenance only — path/lifecycle are projected.
    #[must_use]
    pub fn get_mut_any(&mut self, document_id: &str) -> Option<&mut RegistryEntry> {
        parse_id(document_id).and_then(move |id| self.entries.get_mut(&id))
    }

    /// Look up an active document by its effective path.
    #[must_use]
    pub fn get_by_path(&self, path: &str) -> Option<&RegistryEntry> {
        let id = self.path_index.get(path)?;
        self.entries.get(id).filter(|e| e.deleted_at.is_none())
    }

    /// The document's current liveness HLC: the latest of its registration,
    /// rename, or edit-touch stamps (see [`DocRecord::liveness_hlc`]). Used to
    /// stamp a corrective re-register when a delete lost to a concurrent edit,
    /// so the loser's freshness gate accepts the revival.
    #[must_use]
    pub fn liveness_hlc(&self, document_id: &str) -> Option<Hlc> {
        parse_id(document_id).and_then(|id| self.lattice.get(&id).and_then(DocRecord::liveness_hlc))
    }

    /// The document's INTENDED (un-displaced) path — the lattice record's path.
    /// This differs from [`get`](Self::get)'s effective path while the document
    /// is conflict-copied (displaced): the effective path embeds the conflict
    /// infix, the intended path is the user's chosen name. Used to decide
    /// idempotent-re-register equality (a re-register names the intended path).
    #[must_use]
    pub fn intended_path(&self, document_id: &str) -> Option<String> {
        parse_id(document_id)
            .and_then(|id| self.lattice.get(&id))
            .map(|r| r.current_path().to_owned())
    }

    /// Iterate over all active (non-deleted) entries.
    pub fn active_entries(&self) -> impl Iterator<Item = (&str, &RegistryEntry)> {
        self.entries
            .values()
            .filter(|e| e.deleted_at.is_none())
            .map(|e| (e.document_id.as_str(), e))
    }

    /// All entries including soft-deleted, for persistence.
    pub fn all_entries(&self) -> impl Iterator<Item = &RegistryEntry> {
        self.entries.values()
    }

    /// Number of active entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.path_index.len()
    }

    /// Whether the registry has no active entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.path_index.is_empty()
    }
}

impl Default for DocumentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a document id string into the UUID the lattice keys on. Returns `None`
/// for a non-UUID id (which should not occur — the system is UUID-identity); the
/// caller logs and skips rather than panicking.
fn parse_id(document_id: &str) -> Option<Uuid> {
    Uuid::parse_str(document_id).ok()
}

/// Reconstruct an HLC-valued `DocRecord` from a persisted (millis) entry, using
/// `LEGACY_ACTOR` for the reconstructed stamps.
///
/// All reconstructed stamps carry `logical = 0` and the nil `LEGACY_ACTOR`, so a
/// genuine HLC op (non-nil actor) always wins the `(physical_ms, logical, actor)`
/// tie against a reloaded one — the intended strictly-less-wrong fallback. Two
/// reloaded stamps of one doc at the same `physical_ms` (e.g. a register + rename
/// reloaded together) tie exactly on the HLC and resolve downstream by
/// `path_priority` (rename beats register) then the lexicographic path tiebreak —
/// i.e. reload preserves the live system's deterministic tiebreak, by design.
/// Full-HLC persistence (carrying real logical/actor across reload) is the
/// separate Phase-3 work.
fn record_from_entry(id: Uuid, entry: &RegistryEntry) -> DocRecord {
    let stamp = |ms: i64| Hlc {
        physical_ms: u64::try_from(ms).unwrap_or(0),
        logical: 0,
        actor: LEGACY_ACTOR,
    };
    DocRecord {
        document_id: id,
        path: entry.path.clone(),
        registered_hlc: Some(stamp(entry.created_at)),
        renamed_hlc: entry.renamed_at.map(stamp),
        // Reconstruct the rename causal floor (millis, LEGACY_ACTOR) so a rename
        // that beat a clock-skewed register via the floor still supersedes after
        // a relay restart. `None` for pre-floor persisted rows; such a row falls
        // back to the reloaded `path` (already the arbitration winner at persist
        // time), so reload itself is correct — only a *new* post-restart op on a
        // pre-floor row could re-arbitrate without it (documented residual).
        rename_causal_floor: entry.rename_causal_floor_at.map(stamp),
        touched_hlc: entry.edited_at.map(stamp),
        deleted_hlc: entry.deleted_at.map(stamp),
        displaced: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(did: &str, ts: i64) -> EntryMetadata {
        EntryMetadata {
            author_did: did.to_string(),
            timestamp: ts,
            ..Default::default()
        }
    }

    fn uuid(n: u8) -> String {
        let mut b = [0u8; 16];
        b[0] = n;
        Uuid::from_bytes(b).to_string()
    }

    #[test]
    fn test_register_and_lookup() {
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "notes/ideas.md", meta("did:alice", 1000));
        let entry = reg.get(&uuid(1)).unwrap();
        assert_eq!(entry.path, "notes/ideas.md");
        assert_eq!(entry.created_by, "did:alice");
        assert!(reg.get_by_path("notes/ideas.md").is_some());
    }

    #[test]
    fn test_register_same_path_conflict_copies_not_rejected() {
        // The behavior change from the lattice: a second doc at an occupied path
        // is NOT rejected — it's conflict-copied. Both survive.
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "notes.md", meta("did:alice", 5000)); // higher ts → winner
        reg.register(&uuid(2), "notes.md", meta("did:bob", 3000));
        assert_eq!(reg.get(&uuid(1)).unwrap().path, "notes.md");
        let loser = reg.get(&uuid(2)).unwrap();
        assert!(
            loser.path.contains(".kutl-conflict-"),
            "loser conflict-copied: {}",
            loser.path
        );
        assert!(reg.get_by_path("notes.md").is_some());
    }

    #[test]
    fn test_register_winning_reports_displaced_prior_holder() {
        // A register that WINS a contested path displaces the prior holder to its
        // conflict path AND reports it. The relay handler persists that loser off
        // the contested path before the winner's claim, so the mirror's
        // `UNIQUE(lower(path))` never rejects the winner (OSS/kutlhub parity).
        let mut reg = DocumentRegistry::new();
        let (_t1, d1) = reg.register(&uuid(1), "notes.md", meta("did:a", 1000));
        assert!(d1.is_empty(), "the first register displaces nothing");

        // Higher ts → higher path_hlc → uuid(2) wins the path, uuid(1) displaced.
        let (_t2, d2) = reg.register(&uuid(2), "notes.md", meta("did:b", 5000));
        assert_eq!(
            reg.get(&uuid(2)).unwrap().path,
            "notes.md",
            "winner holds the path"
        );
        assert!(
            reg.get(&uuid(1)).unwrap().path.contains(".kutl-conflict-"),
            "prior holder displaced to its conflict path"
        );
        assert_eq!(
            d2,
            vec![uuid(1)],
            "the displaced prior holder is reported so the caller persists it first"
        );
    }

    #[test]
    fn test_register_losing_reports_no_other_displaced() {
        // When the operated register LOSES (lower priority), only it moves to a
        // conflict path; no OTHER document is displaced, so nothing extra to
        // persist — `persist_entry` already writes the operated loser at its
        // effective (conflict) path.
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "notes.md", meta("did:a", 5000)); // winner
        let (_t2, d2) = reg.register(&uuid(2), "notes.md", meta("did:b", 3000)); // loser
        assert!(
            reg.get(&uuid(2)).unwrap().path.contains(".kutl-conflict-"),
            "operated loser is itself displaced"
        );
        assert!(d2.is_empty(), "no OTHER document displaced");
    }

    #[test]
    fn test_rename_happy_path() {
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "draft.md", meta("did:alice", 1000));
        reg.rename(
            &uuid(1),
            "draft.md",
            "archive/draft.md",
            meta("did:bob", 2000),
            None,
        );
        let entry = reg.get(&uuid(1)).unwrap();
        assert_eq!(entry.path, "archive/draft.md");
        assert_eq!(entry.renamed_by.as_deref(), Some("did:bob"));
        assert_eq!(entry.renamed_at, Some(2000));
    }

    #[test]
    fn test_concurrent_rename_lww_is_order_independent() {
        // Same doc, two renames; the higher-HLC (here higher ts) name wins
        // regardless of arrival order, and the loser's path is not indexed.
        for (first, first_ts, second, second_ts) in [
            ("bar_b.md", 3000, "bar_a.md", 2000),
            ("bar_a.md", 2000, "bar_b.md", 3000),
        ] {
            let mut reg = DocumentRegistry::new();
            reg.register(&uuid(1), "foo.md", meta("did:a", 1000));
            reg.rename(&uuid(1), "foo.md", first, meta("did:x", first_ts), None);
            reg.rename(&uuid(1), "foo.md", second, meta("did:y", second_ts), None);
            assert_eq!(reg.get(&uuid(1)).unwrap().path, "bar_b.md");
            assert!(reg.get_by_path("bar_b.md").is_some());
            assert!(reg.get_by_path("bar_a.md").is_none());
            assert!(reg.get_by_path("foo.md").is_none());
        }
    }

    #[test]
    fn test_rename_delete_same_doc_is_order_independent() {
        // §1.4 rename/delete same doc: HLC decides the terminal state and it is
        // independent of arrival order (the race fs_converge exercises only under
        // load). Delete later (ts 3000 > rename 2000) → gone, both orders.
        for delete_first in [false, true] {
            let mut reg = DocumentRegistry::new();
            reg.register(&uuid(1), "foo.md", meta("did:a", 1000));
            if delete_first {
                reg.unregister(&uuid(1), &meta("did:y", 3000));
                reg.rename(&uuid(1), "foo.md", "bar.md", meta("did:x", 2000), None);
            } else {
                reg.rename(&uuid(1), "foo.md", "bar.md", meta("did:x", 2000), None);
                reg.unregister(&uuid(1), &meta("did:y", 3000));
            }
            assert!(
                reg.get(&uuid(1)).is_none(),
                "delete (3000) beats rename (2000): gone (delete_first={delete_first})"
            );
            assert!(
                reg.get_by_path("bar.md").is_none(),
                "no live path (delete_first={delete_first})"
            );
        }
        // Rename later (ts 3000 > delete 2000) → rename revives at the new name.
        for delete_first in [false, true] {
            let mut reg = DocumentRegistry::new();
            reg.register(&uuid(1), "foo.md", meta("did:a", 1000));
            if delete_first {
                reg.unregister(&uuid(1), &meta("did:y", 2000));
                reg.rename(&uuid(1), "foo.md", "bar.md", meta("did:x", 3000), None);
            } else {
                reg.rename(&uuid(1), "foo.md", "bar.md", meta("did:x", 3000), None);
                reg.unregister(&uuid(1), &meta("did:y", 2000));
            }
            assert_eq!(
                reg.get(&uuid(1))
                    .expect("rename (3000) revives over delete (2000)")
                    .path,
                "bar.md",
                "revived at the new name (delete_first={delete_first})"
            );
        }
    }

    #[test]
    fn test_rename_rename_onto_one_path_conflict_copies() {
        // §2.2 two DISTINCT docs renamed onto the same path collide → conflict-copy
        // (one canonical, one displaced), order-independent. Exercises the rename
        // op feeding cross-document path arbitration (not just create).
        for swap in [false, true] {
            let mut reg = DocumentRegistry::new();
            reg.register(&uuid(1), "a.md", meta("did:a", 1000));
            reg.register(&uuid(2), "b.md", meta("did:b", 1000));
            if swap {
                reg.rename(&uuid(2), "b.md", "shared.md", meta("did:b", 3000), None);
                reg.rename(&uuid(1), "a.md", "shared.md", meta("did:a", 2000), None);
            } else {
                reg.rename(&uuid(1), "a.md", "shared.md", meta("did:a", 2000), None);
                reg.rename(&uuid(2), "b.md", "shared.md", meta("did:b", 3000), None);
            }
            assert_eq!(
                reg.get(&uuid(2)).unwrap().path,
                "shared.md",
                "higher-HLC rename (3000) wins the path (swap={swap})"
            );
            assert!(
                reg.get(&uuid(1)).unwrap().path.contains(".kutl-conflict-"),
                "the lower-HLC renamer is conflict-copied (swap={swap})"
            );
            assert!(
                reg.get_by_path("shared.md").is_some(),
                "path held by one doc (swap={swap})"
            );
        }
    }

    #[test]
    fn test_rename_onto_concurrent_create_conflict_copies() {
        // §2.3 a rename and a create land on the same path → conflict-copy. The
        // higher path-HLC wins regardless of op kind (create vs rename) or order.
        for swap in [false, true] {
            let mut reg = DocumentRegistry::new();
            reg.register(&uuid(2), "b.md", meta("did:b", 1000));
            if swap {
                reg.rename(&uuid(2), "b.md", "shared.md", meta("did:b", 2000), None);
                reg.register(&uuid(1), "shared.md", meta("did:a", 3000));
            } else {
                reg.register(&uuid(1), "shared.md", meta("did:a", 3000));
                reg.rename(&uuid(2), "b.md", "shared.md", meta("did:b", 2000), None);
            }
            assert_eq!(
                reg.get(&uuid(1)).unwrap().path,
                "shared.md",
                "higher-HLC create (3000) wins over the rename (2000) (swap={swap})"
            );
            assert!(
                reg.get(&uuid(2)).unwrap().path.contains(".kutl-conflict-"),
                "the renamed loser is conflict-copied (swap={swap})"
            );
        }
    }

    #[test]
    fn test_unregister_soft_delete_releases_path() {
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "notes/ideas.md", meta("did:alice", 1000));
        reg.unregister(&uuid(1), &meta("did:bob", 2000));
        assert!(reg.get(&uuid(1)).is_none(), "soft-deleted: not active");
        // The path is free — a new doc cleanly owns it (alive-only arbitration).
        reg.register(&uuid(2), "notes/ideas.md", meta("did:carol", 3000));
        assert!(!reg.get(&uuid(2)).unwrap().path.contains(".kutl-conflict-"));
    }

    /// A comparable snapshot of the whole projection: every entry's
    /// path/lifecycle millis (sorted), the effective-path index (sorted), and
    /// the divergence counter. Used to prove two registries are byte-identical.
    #[expect(clippy::type_complexity, reason = "test-local snapshot tuple")]
    fn projection_snapshot(
        reg: &DocumentRegistry,
    ) -> (
        Vec<(Uuid, String, i64, Option<i64>, Option<i64>, Option<i64>)>,
        Vec<(String, Uuid)>,
        u64,
    ) {
        let mut entries: Vec<_> = reg
            .entries
            .iter()
            .map(|(id, e)| {
                (
                    *id,
                    e.path.clone(),
                    e.created_at,
                    e.renamed_at,
                    e.deleted_at,
                    e.edited_at,
                )
            })
            .collect();
        entries.sort();
        let mut index: Vec<_> = reg
            .path_index
            .iter()
            .map(|(p, id)| (p.clone(), *id))
            .collect();
        index.sort();
        (entries, index, reg.divergence_count())
    }

    #[test]
    fn test_unregister_many_matches_per_doc_loop() {
        // Build the SAME state two ways — a displaced conflict-copy pair plus a
        // plain doc — then delete all three: once via the per-doc `unregister`
        // loop, once via `unregister_many`. The final projection must be
        // byte-identical, proving the batch path is a pure optimization.
        let build = || {
            let mut reg = DocumentRegistry::new();
            reg.register(&uuid(1), "notes.md", meta("did:a", 5000)); // winner
            reg.register(&uuid(2), "notes.md", meta("did:b", 3000)); // displaced loser
            reg.register(&uuid(3), "other.md", meta("did:c", 4000));
            reg
        };
        let ids = vec![uuid(1), uuid(2), uuid(3)];

        let mut per_doc = build();
        for id in &ids {
            per_doc.unregister(id, &meta("did:del", 9000));
        }

        let mut batched = build();
        batched.unregister_many(&ids, &meta("did:del", 9000));

        assert_eq!(
            projection_snapshot(&per_doc),
            projection_snapshot(&batched),
            "batch unregister must yield the same projection as the per-doc loop"
        );
        // And the bulk effect itself: everything gone, index empty, no divergence.
        assert!(
            batched.active_entries().next().is_none(),
            "all docs soft-deleted"
        );
        assert!(batched.get_by_path("notes.md").is_none());
        assert!(batched.get_by_path("other.md").is_none());
        assert_eq!(batched.divergence_count(), 0);
    }

    #[test]
    fn test_unregister_many_restore_round_trips() {
        // The combined rollback token restores the WHOLE batch (every targeted
        // record + entry), matching the all-or-nothing in-memory mutation.
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "notes.md", meta("did:a", 5000));
        reg.register(&uuid(2), "notes.md", meta("did:b", 3000)); // displaced loser
        let before = projection_snapshot(&reg);

        let ids = vec![uuid(1), uuid(2)];
        let token = reg.unregister_many(&ids, &meta("did:del", 9000));
        assert!(reg.active_entries().next().is_none(), "deleted in memory");

        reg.restore(token);
        assert_eq!(
            projection_snapshot(&reg),
            before,
            "restore undoes the whole batch"
        );
        assert_eq!(reg.get(&uuid(1)).unwrap().path, "notes.md");
        assert!(reg.get(&uuid(2)).unwrap().path.contains(".kutl-conflict-"));
    }

    #[test]
    fn test_rename_after_delete_revives() {
        // Edit/rename whose HLC exceeds the deletion revives the document.
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "doc.md", meta("did:a", 1000));
        reg.unregister(&uuid(1), &meta("did:a", 2000));
        assert!(reg.get(&uuid(1)).is_none());
        reg.rename(&uuid(1), "doc.md", "doc-back.md", meta("did:a", 3000), None);
        let entry = reg.get(&uuid(1)).expect("revived");
        assert_eq!(entry.path, "doc-back.md");
    }

    #[test]
    fn test_touch_after_delete_revives_keeps_path() {
        // An edit-touch later than the deletion revives the doc at its SAME path
        // (a touch is not a rename — catalog §1.5/§3.4 edit-revives).
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "doc.md", meta("did:a", 1000));
        reg.unregister(&uuid(1), &meta("did:a", 2000));
        assert!(reg.get(&uuid(1)).is_none(), "deleted before the touch");
        reg.touch(&uuid(1), Hlc::physical_touch(3000));
        let entry = reg.get(&uuid(1)).expect("revived by the later touch");
        assert_eq!(
            entry.path, "doc.md",
            "revival keeps the path; a touch is not a rename"
        );
        assert_eq!(
            reg.liveness_hlc(&uuid(1)),
            Some(Hlc::physical_touch(3000)),
            "liveness reflects the reviving touch"
        );
    }

    #[test]
    fn test_touch_before_delete_does_not_revive() {
        // A touch OLDER than the deletion (e.g. a stale offline edit) cannot
        // revive past a causally-later delete.
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "doc.md", meta("did:a", 1000));
        reg.touch(&uuid(1), Hlc::physical_touch(2000));
        reg.unregister(&uuid(1), &meta("did:a", 5000));
        assert!(
            reg.get(&uuid(1)).is_none(),
            "a delete strictly later than the edit wins"
        );
    }

    #[test]
    fn test_touch_at_same_ms_as_delete_revives() {
        // The edit-vs-delete TIEBREAK (catalog §1.5): an edit at the delete's
        // EXACT physical-ms revives. The synthetic edit-touch is
        // `physical_touch` (logical = u32::MAX), built to win the same-millisecond
        // tie against a delete (whose `hlc_of` logical is 0) — see the
        // `physical_touch` doc + `doc_record`'s "real origin HLCs never tie" note.
        //
        // This is the case the §1.5 e2e test (`test_conflict_edit_delete`) cannot
        // pin: two genuinely-concurrent online ops are stamped by two different
        // clocks (the edit by kutl-core's `Document` env, the delete by the daemon
        // HLC), so their relative physical-ms is a real race. Here the HLCs are
        // assigned, not minted, so the tiebreak is deterministic. Contrast
        // `test_touch_before_delete_does_not_revive`: a strictly-later delete wins.
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "doc.md", meta("did:a", 1000));
        reg.touch(&uuid(1), Hlc::physical_touch(2000));
        reg.unregister(&uuid(1), &meta("did:a", 2000));
        let entry = reg
            .get(&uuid(1))
            .expect("a same-ms edit-touch beats the delete (tie favors the edit)");
        assert_eq!(
            entry.path, "doc.md",
            "revival keeps the path; a touch is not a rename"
        );
    }

    #[test]
    fn test_touch_fast_path_matches_slow_path() {
        // A touch on an alive, undisplaced document takes the reproject-skipping
        // fast path: it must set edited_at and leave a displaced sibling and the
        // divergence detector exactly as a full reproject would — i.e. unchanged.
        let mut reg = DocumentRegistry::new();
        // doc1 (higher ts) wins "a.md"; doc2 (lower) is displaced to its conflict path.
        reg.register(&uuid(1), "a.md", meta("did:a", 2000));
        reg.register(&uuid(2), "a.md", meta("did:b", 1000));
        let sibling_path = reg.get_any(&uuid(2)).unwrap().path.clone();
        assert!(
            sibling_path.contains(".kutl-conflict-"),
            "doc2 is displaced to its conflict path: {sibling_path}"
        );

        // Touch the alive, undisplaced winner — exercises the fast path.
        reg.touch(&uuid(1), Hlc::physical_touch(3000));

        assert_eq!(
            reg.get(&uuid(1)).unwrap().edited_at,
            Some(3000),
            "the fast path sets edited_at"
        );
        assert_eq!(
            reg.get_any(&uuid(2)).unwrap().path,
            sibling_path,
            "the displaced sibling is untouched by a fast-path touch"
        );
        assert_eq!(
            reg.divergence_count(),
            0,
            "a fast-path touch trips no divergence"
        );
    }

    #[test]
    fn test_divergence_detector_quiet_under_normal_arbitration() {
        // Catalog §9.1: confluent arbitration must never trip the detector — two
        // docs at one path resolve to canonical + conflict-copy, NOT two holders.
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "notes.md", meta("did:a", 5000));
        reg.register(&uuid(2), "notes.md", meta("did:b", 3000)); // displaced
        assert_eq!(
            reg.divergence_count(),
            0,
            "a normal path collision is resolved, not a divergence"
        );
    }

    #[test]
    fn test_divergence_detector_fires_on_injected_violation() {
        // Catalog §9.1: if a confluence premise is ever violated — two ALIVE docs
        // at one effective path — the detector must fire (not be silent). Inject
        // the violation by forcing the displaced loser back to undisplaced via
        // the lattice's rollback seam (which bypasses arbitration), simulating a
        // hypothetical future arbitration bug, then reproject.
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "notes.md", meta("did:a", 5000));
        reg.register(&uuid(2), "notes.md", meta("did:b", 3000));
        assert_eq!(reg.divergence_count(), 0, "quiet before injection");

        let loser = Uuid::parse_str(&uuid(2)).unwrap();
        let mut rec = reg.lattice.get(&loser).expect("loser record").clone();
        assert!(rec.displaced, "loser is displaced under normal arbitration");
        rec.displaced = false; // corrupt: both now claim notes.md
        reg.lattice.restore_record(loser, Some(rec));
        reg.reproject();

        assert!(
            reg.divergence_count() >= 1,
            "detector fires on two alive docs at one effective path"
        );
    }

    #[test]
    fn test_divergence_detector_is_case_folded() {
        // Arbitration folds case (matching the DB's `UNIQUE(lower(path))`), so two
        // docs at case-variant paths collide and one is displaced. Inject the
        // violation — both alive at `Foo.md` / `foo.md` — via the rollback seam. A
        // raw-case detector would see two DIFFERENT paths and stay silent; the
        // folded detector must fire, matching the case-insensitivity that
        // guarantees the one-alive-per-path invariant.
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "Foo.md", meta("did:a", 5000)); // winner (higher ts)
        reg.register(&uuid(2), "foo.md", meta("did:b", 3000)); // displaced (case collision)
        assert_eq!(reg.divergence_count(), 0, "quiet before injection");

        let loser = Uuid::parse_str(&uuid(2)).unwrap();
        let mut rec = reg.lattice.get(&loser).expect("loser record").clone();
        assert!(
            rec.displaced,
            "case-variant loser is displaced under normal arbitration"
        );
        rec.displaced = false; // corrupt: both now alive at Foo.md / foo.md
        reg.lattice.restore_record(loser, Some(rec));
        reg.reproject();

        assert!(
            reg.divergence_count() >= 1,
            "case-folded detector fires on two alive docs at case-variant effective paths"
        );
    }

    #[test]
    fn test_rollback_restores_prior_state() {
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "a.md", meta("did:a", 1000));
        // A rename, captured, then rolled back → prior path/state restored.
        let (token, _displaced) = reg.rename(&uuid(1), "a.md", "b.md", meta("did:a", 2000), None);
        assert_eq!(reg.get(&uuid(1)).unwrap().path, "b.md");
        reg.restore(token);
        assert_eq!(
            reg.get(&uuid(1)).unwrap().path,
            "a.md",
            "rollback restored prior path"
        );
        assert!(reg.get_by_path("a.md").is_some());
        assert!(reg.get_by_path("b.md").is_none());
    }

    #[test]
    fn test_register_rollback_removes_new_doc() {
        let mut reg = DocumentRegistry::new();
        let (token, _displaced) = reg.register(&uuid(1), "a.md", meta("did:a", 1000));
        assert!(reg.get(&uuid(1)).is_some());
        reg.restore(token);
        assert!(
            reg.get(&uuid(1)).is_none(),
            "rolled-back register removes the doc"
        );
        assert!(reg.get_by_path("a.md").is_none());
    }

    #[test]
    fn test_from_entries_reconstructs_and_indexes() {
        let entries = vec![
            RegistryEntry {
                document_id: uuid(1),
                path: "a.md".into(),
                created_by: "did:alice".into(),
                created_at: 1000,
                ..Default::default()
            },
            RegistryEntry {
                document_id: uuid(2),
                path: "b.md".into(),
                created_by: "did:bob".into(),
                created_at: 2000,
                deleted_at: Some(3000),
                ..Default::default()
            },
        ];
        let reg = DocumentRegistry::from_entries(entries);
        assert_eq!(reg.len(), 1);
        assert!(reg.get(&uuid(1)).is_some());
        assert!(
            reg.get(&uuid(2)).is_none(),
            "tombstone stays dead on reload"
        );
        assert!(reg.get_by_path("a.md").is_some());
        assert!(reg.get_by_path("b.md").is_none());
    }
}
