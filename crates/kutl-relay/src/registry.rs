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

use kutl_core::lattice::{DocRecord, LifecycleProjection, RegistryLattice, fold_path};
use kutl_core::{ActorId, Hlc, Uuid};
use tracing::{error, warn};

/// HLC actor for ops that carry no origin HLC (pre-HLC clients, internal
/// callers): an all-zeros UUID, which sorts below any real device actor — so a
/// genuine HLC-stamped op wins a `(physical_ms, logical)` tie against it. Such
/// ops order purely by `physical_ms` (wall-clock), the strictly-less-wrong
/// fallback.
const LEGACY_ACTOR: ActorId = ActorId(Uuid::nil());

/// The divergence detector's log lines. Both start with the same prefix so
/// the claim-time check and the load-time check grep as one signal.
const DIVERGENCE_MSG: &str =
    "lifecycle divergence: duplicate alive document at an effective path already held";
/// Load-time form: a persisted alive row that arbitration had to move.
const LOAD_DIVERGENCE_MSG: &str =
    "lifecycle divergence: persisted alive document moved by arbitration on load";

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
pub struct SourceProvenance {
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
    /// Case-folded effective path → the ALIVE entries projecting to it, each
    /// with its exact-case effective path. One structure serves three
    /// readers: the exact-path lookup ([`Self::get_by_path`] matches the
    /// exact path inside the folded slot), the O(1) occupant lookup for
    /// [`Self::capture`], and the divergence detector (a slot holding more
    /// than one id IS the violation). Written only through
    /// [`Self::claim_alive`] and [`Self::vacate_alive`]; whoever replaces an
    /// entry outside [`Self::reproject_ids`] vacates its row first.
    alive_by_folded: HashMap<String, Vec<(String, Uuid)>>,
    /// Lifecycle-divergence detector: the number of times the projection has
    /// been observed to violate the "at most one alive document per effective
    /// path" invariant — a claim landing on a held folded slot, or a persisted
    /// alive row that arbitration had to move on load. Confluent arbitration
    /// keeps this at zero; a nonzero value means a confluence premise was
    /// violated and the registry would otherwise serve two canonical holders
    /// for one path — it must not be silent. A monotonic backstop counter,
    /// surfaced via [`Self::divergence_count`].
    divergence_count: u64,
}

impl DocumentRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            lattice: RegistryLattice::new(),
            entries: HashMap::new(),
            alive_by_folded: HashMap::new(),
            divergence_count: 0,
        }
    }

    /// Build a registry from persisted entries (millis-based). Each entry's
    /// lifecycle millis are reconstructed into an HLC-valued `DocRecord` (with
    /// `LEGACY_ACTOR`), so a reloaded registry merges consistently with new
    /// HLC-stamped ops. Persisted rows are mutually non-colliding (the DB's
    /// alive-path uniqueness), so arbitration is a no-op on load for every
    /// alive row: one it DOES move was persisted in violation of the
    /// one-alive-per-path invariant, and is counted and logged as a
    /// divergence rather than silently healed. A tombstone regrouped under a
    /// later claimant is ordinary no-reclaim bookkeeping, not a violation.
    #[must_use]
    pub fn from_entries(entries: Vec<RegistryEntry>) -> Self {
        let mut reg = Self::new();
        // Insert every record then arbitrate ONCE (observe_all), instead of
        // observe-per-row which re-arbitrates the whole growing set — O(n) load
        // rather than O(n²). The trailing reproject() builds the entry projection
        // + alive-path map from the single arbitration pass.
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
        for id in reg.reproject() {
            let Some(entry) = reg.entries.get(&id).filter(|e| e.deleted_at.is_none()) else {
                continue;
            };
            let persisted = reg.lattice.get(&id).map_or("", DocRecord::current_path);
            error!(
                persisted_path = %persisted,
                path = %entry.path,
                document_id = %id,
                "{}",
                LOAD_DIVERGENCE_MSG
            );
            reg.divergence_count += 1;
        }
        reg
    }

    /// Capture the prior state of the documents an operation on `id` targeting
    /// `path` could touch: `id` itself plus any alive record sharing the
    /// (case-folded) target path.
    fn capture(&self, id: Uuid, path: Option<&str>) -> RollbackToken {
        let mut ids = vec![id];
        if let Some(p) = path {
            // The alive-per-folded-path map answers this in O(1).
            for (_, other) in self
                .alive_by_folded
                .get(&fold_path(p))
                .into_iter()
                .flatten()
            {
                if *other != id {
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
        // One arbitration for the whole captured set, not one per record:
        // a rollback touches the operation's document and every alive
        // record sharing its target path.
        let mut records = Vec::with_capacity(token.prior.len());
        for (id, record, entry) in token.prior {
            records.push((id, record));
            // The failed op's projection claimed rows for this id; drop them
            // with the entry it projected, since the reproject below reads
            // the prior row from the entry it finds.
            if let Some(current) = self.entries.remove(&id)
                && current.deleted_at.is_none()
            {
                self.vacate_alive(id, &current.path);
            }
            if let Some(e) = entry {
                self.entries.insert(id, e);
            }
        }
        self.lattice.restore_records(records);
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
        let (affected, _) =
            self.lattice
                .observe_affected(DocRecord::register(did, path, Some(hlc)));
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
        let changed = self.reproject_ids(&affected);
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
        // Causal floor: the register HLC the renamer observed, so a rename of
        // a clock-skewed registrant's doc supersedes the future-stamped
        // register even with a lower `renamed_hlc`.
        let (affected, won) = {
            let (affected, merged) = self.lattice.observe_affected(DocRecord::rename(
                did,
                new_path,
                Some(hlc),
                rename_causal_floor,
            ));
            (affected, merged.renamed_hlc == Some(hlc))
        };
        // Track the winning renamer's author (lives on the entry, not the lattice).
        if won && let Some(entry) = self.entries.get_mut(&did) {
            entry.renamed_by = Some(meta.author_did);
        }
        let changed = self.reproject_ids(&affected);
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
        let (affected, _) =
            self.lattice
                .observe_affected(DocRecord::delete(did, self.keep_path(&did), Some(hlc)));
        self.reproject_ids(&affected);
        token
    }

    /// The path this document currently holds, or empty if unknown — what a
    /// path-preserving op (a delete, or a revival/displaced touch) carries
    /// forward onto its fragment.
    fn keep_path(&self, did: &Uuid) -> String {
        self.lattice
            .get(did)
            .map_or_else(String::new, |r| r.current_path().to_owned())
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
            .map(|did| DocRecord::delete(*did, self.keep_path(did), Some(hlc)))
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
        // record's displaced flag or effective path) or the alive-path map.
        // Update the single record + its entry's `edited_at` in place and skip
        // the O(docs) observe/reproject. The full merge is reserved
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
        let path = self.keep_path(&did);
        let (affected, _) = self.lattice.observe_affected(DocRecord {
            document_id: did,
            path,
            registered_hlc: None,
            renamed_hlc: None,
            rename_causal_floor: None,
            touched_hlc: Some(hlc),
            deleted_hlc: None,
            displaced: false,
        });
        self.reproject_ids(&affected);
    }

    /// Sync every entry's projection: the O(documents) form of
    /// [`Self::reproject_ids`] for loads, rollback restores, and batch
    /// unregisters, whose from-scratch arbitration can flip any record. Same
    /// return contract.
    fn reproject(&mut self) -> Vec<Uuid> {
        let all: Vec<Uuid> = self.lattice.records().map(|(id, _)| *id).collect();
        self.reproject_ids(&all)
    }

    /// Sync the projection for `affected`: fold each id's path/lifecycle
    /// millis from its merged record into its entry (creating a minimal entry
    /// for a record lacking one, e.g. a rename of a never-registered id), and
    /// patch the alive-per-path map for exactly the rows those ids held or
    /// now claim.
    ///
    /// `affected` comes from [`RegistryLattice::observe_affected`] — a superset
    /// of every id whose projection could differ from the last pass — so
    /// entries outside it are untouched by construction (their records and
    /// election inputs did not change). All vacates apply before any claim so
    /// a winner/loser swapping one path inside a single op cannot transit
    /// through a false two-holders state; a claim that still lands on an
    /// occupied folded path is a genuine divergence and is counted + logged.
    ///
    /// Returns the ids whose **effective path changed** in this pass. The caller
    /// uses this to persist every moved entry, not just the operated document:
    /// when an operation displaces a *different* document to its conflict path
    /// (path arbitration), that displaced loser's row must be written off the
    /// contested path before the winner can claim it — otherwise the DB's
    /// `UNIQUE(lower(path))` rejects the winner. See `DocumentRegistry::register`.
    fn reproject_ids(&mut self, affected: &[Uuid]) -> Vec<Uuid> {
        let mut ids: Vec<Uuid> = affected.to_vec();
        ids.sort_unstable();
        ids.dedup();
        let mut path_changed = Vec::new();
        // Per id: the prior projection (path, alive) the index rows were
        // built from. The new projection is read back from the entry.
        let mut prior: Vec<(Uuid, String, bool)> = Vec::new();
        for id in ids {
            let Some(p) = self.lattice.get(&id).map(DocRecord::project) else {
                continue;
            };
            let entry = self.entries.entry(id).or_insert_with(|| RegistryEntry {
                document_id: id.to_string(),
                ..Default::default()
            });
            let (old_path, old_alive) = apply_projection(entry, p);
            if old_path != entry.path {
                path_changed.push(id);
            }
            prior.push((id, old_path, old_alive));
        }
        // Vacate first, then claim, so an id moving between two contended
        // paths never briefly double-books either.
        for (id, old_path, old_alive) in &prior {
            if *old_alive {
                self.vacate_alive(*id, old_path);
            }
        }
        for (id, _, _) in &prior {
            let Some(entry) = self.entries.get(id) else {
                continue;
            };
            if entry.deleted_at.is_none() {
                let path = entry.path.clone();
                self.claim_alive(*id, &path);
            }
        }
        #[cfg(test)]
        self.assert_alive_index_matches_entries();
        path_changed
    }

    /// Drop `id`'s row for `path` from the alive-per-path map. An empty path
    /// has no row ([`Self::claim_alive`] never writes one), so there is
    /// nothing to vacate.
    fn vacate_alive(&mut self, id: Uuid, path: &str) {
        if path.is_empty() {
            return;
        }
        let folded = fold_path(path);
        if let Some(slot) = self.alive_by_folded.get_mut(&folded) {
            slot.retain(|(_, x)| *x != id);
            if slot.is_empty() {
                self.alive_by_folded.remove(&folded);
            }
        }
    }

    /// Record `id` as alive at `path`, counting and logging a violation of
    /// the one-alive-per-folded-path invariant. The ONE implementation of
    /// that rule: every projection, per-op or bulk, goes through it. Folds
    /// case to match arbitration's case-insensitive grouping (and the DB's
    /// `UNIQUE(lower(path))`), so two alive records differing only in case
    /// cannot slip past while the mirror's uniqueness rejects one. An empty
    /// path is not a location: it gets no row, so a record that projects to
    /// one cannot accumulate rows under `""`.
    fn claim_alive(&mut self, id: Uuid, path: &str) {
        if path.is_empty() {
            return;
        }
        let slot = self.alive_by_folded.entry(fold_path(path)).or_default();
        slot.retain(|(_, x)| *x != id);
        slot.push((path.to_owned(), id));
        if slot.len() > 1 {
            self.divergence_count += 1;
            error!(path = %path, document_id = %id, "{}", DIVERGENCE_MSG);
        }
    }

    /// Unit-test-build tripwire (the same pattern as the daemon's deep
    /// identity-index check): the incremental `alive_by_folded` bookkeeping
    /// must agree with a from-scratch rebuild of the entries it indexes.
    /// O(docs) per op, so test builds only; always-on it would put a full
    /// sweep back into every per-op path this projection exists to avoid.
    /// Slot ORDER is not part of the contract; compare as sets.
    #[cfg(test)]
    fn assert_alive_index_matches_entries(&self) {
        type Slots = HashMap<String, std::collections::BTreeSet<(String, Uuid)>>;
        let mut want: Slots = HashMap::new();
        for (id, entry) in &self.entries {
            if entry.deleted_at.is_none() && !entry.path.is_empty() {
                want.entry(fold_path(&entry.path))
                    .or_default()
                    .insert((entry.path.clone(), *id));
            }
        }
        let got: Slots = self
            .alive_by_folded
            .iter()
            .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
            .collect();
        assert_eq!(
            want, got,
            "alive_by_folded drifted from the entries it indexes — update it \
             only through claim_alive/vacate_alive"
        );
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
    /// omit semantics). Provenance only — path/lifecycle are projected.
    #[must_use]
    pub fn get_mut_any(&mut self, document_id: &str) -> Option<&mut RegistryEntry> {
        parse_id(document_id).and_then(move |id| self.entries.get_mut(&id))
    }

    /// Look up an active document by its exact-case effective path.
    #[must_use]
    pub fn get_by_path(&self, path: &str) -> Option<&RegistryEntry> {
        // Latest claim first, so a divergent slot serves the row claimed last.
        let (_, id) = self
            .alive_by_folded
            .get(&fold_path(path))?
            .iter()
            .rev()
            .find(|(p, _)| p == path)?;
        self.entries.get(id)
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

    /// Number of active (non-deleted) entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.active_entries().count()
    }

    /// Whether the registry has no active entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.active_entries().next().is_none()
    }
}

impl Default for DocumentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Fold one record's lifecycle projection into its registry entry, returning
/// the entry's PRIOR `(path, alive)` for the caller's index bookkeeping.
/// `p` (effective path, revival-aware tombstone) comes from
/// `DocRecord::project`; here we only map its stamps to millis.
fn apply_projection(entry: &mut RegistryEntry, p: LifecycleProjection) -> (String, bool) {
    let old_alive = entry.deleted_at.is_none();
    let old_path = std::mem::replace(&mut entry.path, p.effective_path);
    entry.created_at = millis(p.registered_hlc).unwrap_or(0);
    entry.renamed_at = millis(p.renamed_hlc);
    entry.rename_causal_floor_at = millis(p.rename_causal_floor);
    entry.deleted_at = millis(p.deleted_hlc);
    entry.edited_at = millis(p.touched_hlc);
    (old_path, old_alive)
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

    /// A persisted row as the backend hands it back: alive at `path` since
    /// `created_at`.
    fn persisted(n: u8, path: &str, created_at: i64) -> RegistryEntry {
        RegistryEntry {
            document_id: uuid(n),
            path: path.into(),
            created_by: format!("did:{n}"),
            created_at,
            ..Default::default()
        }
    }

    /// The alive-per-folded-path map as a comparable value: slots sorted,
    /// rows within a slot sorted (order is not part of the contract).
    fn alive_slots(reg: &DocumentRegistry) -> Vec<(String, Vec<(String, Uuid)>)> {
        let mut slots: Vec<(String, Vec<(String, Uuid)>)> = reg
            .alive_by_folded
            .iter()
            .map(|(k, v)| {
                let mut rows = v.clone();
                rows.sort_unstable();
                (k.clone(), rows)
            })
            .collect();
        slots.sort();
        slots
    }

    /// Incremental reproject equals the full pass: after a dense op mix (a
    /// collision, an escape-rename, a displacing rename, a delete, a reviving
    /// touch, a case collision), a full `reproject()` over the same lattice
    /// changes nothing the per-op bookkeeping already holds — entry paths,
    /// the alive-per-folded-path map, and the divergence counter alike.
    #[test]
    fn test_incremental_reproject_matches_full() {
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "a.md", meta("did:a", 5000));
        reg.register(&uuid(2), "a.md", meta("did:b", 3000)); // collision loser
        reg.rename(&uuid(2), "a.md", "b.md", meta("did:b", 6000), None); // escapes
        reg.rename(&uuid(1), "a.md", "b.md", meta("did:a", 7000), None); // displaces 2
        reg.unregister(&uuid(2), &meta("did:b", 8000));
        reg.touch(&uuid(2), hlc_of(&meta("did:b", 9000))); // revival (slow path)
        reg.register(&uuid(3), "B.md", meta("did:c", 9500)); // case collision

        let snap_entries: std::collections::BTreeMap<String, (String, Option<i64>)> = reg
            .entries
            .values()
            .map(|e| (e.document_id.clone(), (e.path.clone(), e.deleted_at)))
            .collect();
        let snap_alive = alive_slots(&reg);
        let snap_div = reg.divergence_count();

        assert!(
            reg.reproject().is_empty(),
            "a full pass over a maintained projection moves nothing"
        );

        let full_entries: std::collections::BTreeMap<String, (String, Option<i64>)> = reg
            .entries
            .values()
            .map(|e| (e.document_id.clone(), (e.path.clone(), e.deleted_at)))
            .collect();

        assert_eq!(snap_entries, full_entries, "entry projections match");
        assert_eq!(
            snap_alive,
            alive_slots(&reg),
            "alive-per-folded-path map matches"
        );
        assert_eq!(
            snap_div, 0,
            "a legal op mix fires no divergence incrementally"
        );
        assert_eq!(
            reg.divergence_count(),
            0,
            "a legal op mix fires no divergence in the full pass either"
        );
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
        // Rename/delete of the same doc: HLC decides the terminal state and it
        // is independent of arrival order (a race that otherwise only shows up
        // under load). Delete later (ts 3000 > rename 2000) → gone, both orders.
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
        // Two DISTINCT docs renamed onto the same path collide → conflict-copy
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
        // A rename and a create land on the same path → conflict-copy. The
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
    /// path/lifecycle millis (sorted), the alive-per-folded-path map (sorted),
    /// and the divergence counter. Used to prove two registries are
    /// byte-identical.
    #[expect(clippy::type_complexity, reason = "test-local snapshot tuple")]
    fn projection_snapshot(
        reg: &DocumentRegistry,
    ) -> (
        Vec<(Uuid, String, i64, Option<i64>, Option<i64>, Option<i64>)>,
        Vec<(String, Vec<(String, Uuid)>)>,
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
        (entries, alive_slots(reg), reg.divergence_count())
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
        // (a touch is not a rename, so revival keeps the path).
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
        // The edit-vs-delete TIEBREAK: an edit at the delete's
        // EXACT physical-ms revives. The synthetic edit-touch is
        // `physical_touch` (logical = u32::MAX), built to win the same-millisecond
        // tie against a delete (whose `hlc_of` logical is 0) — see the
        // `physical_touch` doc + `doc_record`'s "real origin HLCs never tie" note.
        //
        // This is the case an end-to-end test cannot
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
        // Confluent arbitration must never trip the detector — two
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

    /// Two ALIVE persisted rows at one path can only be a corrupt reload
    /// (the DB's alive-path uniqueness forbids it). Arbitration displaces
    /// one on load; that move must be counted and logged, not silently
    /// healed.
    #[test]
    fn test_divergence_fires_on_colliding_persisted_rows() {
        let reg = DocumentRegistry::from_entries(vec![
            persisted(1, "notes.md", 5000),
            persisted(2, "notes.md", 3000),
        ]);

        assert_eq!(
            reg.divergence_count(),
            1,
            "the moved alive row is counted once"
        );
        assert_eq!(
            reg.get_by_path("notes.md").expect("winner").document_id,
            uuid(1),
            "the later claim keeps the path"
        );
        assert_ne!(
            reg.get(&uuid(2)).expect("loser").path,
            "notes.md",
            "the loser projects to its conflict path"
        );
    }

    /// Arbitration folds case (matching the DB's `UNIQUE(lower(path))`), so
    /// persisted rows at `Foo.md` / `foo.md` collide on load and the loser's
    /// move is counted — a raw-case check would see two different paths and
    /// stay silent. The exact-case lookup still resolves only the path a row
    /// actually holds.
    #[test]
    fn test_divergence_on_load_is_case_folded() {
        let reg = DocumentRegistry::from_entries(vec![
            persisted(1, "Foo.md", 5000),
            persisted(2, "foo.md", 3000),
        ]);

        assert_eq!(reg.divergence_count(), 1);
        assert_eq!(
            reg.get_by_path("Foo.md").expect("winner").document_id,
            uuid(1)
        );
        assert!(
            reg.get_by_path("foo.md").is_none(),
            "the lookup is exact-case: nothing alive holds `foo.md`"
        );
    }

    /// Only an ALIVE row moving on load is a violation. A tombstone regrouped
    /// under a later alive claimant is no-reclaim bookkeeping (the DB's
    /// uniqueness covers alive rows only, so the pair is a legal persisted
    /// state); an alive row displaced by a later-stamped tombstone at its
    /// path, though, silently changes a live document's location on restart
    /// and is counted.
    #[test]
    fn test_load_counts_only_moved_alive_rows() {
        let dead = |n, path, created_at, deleted_at| RegistryEntry {
            deleted_at: Some(deleted_at),
            ..persisted(n, path, created_at)
        };

        let tombstone_loses = DocumentRegistry::from_entries(vec![
            persisted(1, "a.md", 5000),
            dead(2, "a.md", 3000, 4000),
        ]);
        assert_eq!(
            tombstone_loses.divergence_count(),
            0,
            "a regrouped tombstone is not counted"
        );
        assert_eq!(
            tombstone_loses
                .get_by_path("a.md")
                .expect("holder")
                .document_id,
            uuid(1)
        );

        let tombstone_wins = DocumentRegistry::from_entries(vec![
            persisted(1, "a.md", 3000),
            dead(2, "a.md", 5000, 6000),
        ]);
        assert_eq!(
            tombstone_wins.divergence_count(),
            1,
            "a moved alive row is counted"
        );
        assert!(
            tombstone_wins.get_by_path("a.md").is_none(),
            "the alive row was displaced to its conflict path"
        );
    }

    /// The inline detector: a per-op claim that lands on an occupied folded
    /// slot must fire the divergence counter. Arbitration never produces that
    /// state through the public API, so the occupant is injected directly —
    /// the projected entry and its row, as a corrupt in-memory projection
    /// would hold them.
    #[test]
    fn test_incremental_projection_inline_detector_fires() {
        let mut reg = DocumentRegistry::new();
        reg.register(&uuid(1), "a.md", meta("did:a", 5000));
        reg.register(&uuid(2), "b.md", meta("did:b", 5000));

        let intruder = Uuid::parse_str(&uuid(1)).unwrap();
        reg.vacate_alive(intruder, "a.md");
        reg.entries.get_mut(&intruder).expect("entry").path = "b.md".to_owned();
        reg.claim_alive(intruder, "b.md");
        let baseline = reg.divergence_count();
        assert_eq!(baseline, 1, "the injected double-claim is counted");

        // A THIRD doc registers at the corrupt path: arbitration (which sees
        // only honest lattice records) resolves the doc-2 collision, and the
        // winner's claim lands on a folded slot the intruder still occupies —
        // the inline detector must count it.
        reg.register(&uuid(3), "b.md", meta("did:c", 7000));
        assert!(
            reg.divergence_count() > baseline,
            "a claim onto an occupied folded path fires (got {} vs baseline {baseline})",
            reg.divergence_count()
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
            persisted(1, "a.md", 1000),
            RegistryEntry {
                deleted_at: Some(3000),
                ..persisted(2, "b.md", 2000)
            },
        ];
        let reg = DocumentRegistry::from_entries(entries);
        assert_eq!(reg.len(), 1);
        assert_eq!(
            reg.divergence_count(),
            0,
            "a conforming reload moves nothing"
        );
        assert!(reg.get(&uuid(1)).is_some());
        assert!(
            reg.get(&uuid(2)).is_none(),
            "tombstone stays dead on reload"
        );
        assert!(reg.get_by_path("a.md").is_some());
        assert!(reg.get_by_path("b.md").is_none());
    }
}
