//! CRDT document — the primary public type for synchronized text.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::Duration;

use diamond_types::AgentId;
use similar::{ChangeTag, TextDiffConfig};

use crate::change::{Boundary, Change, ChangeList, RemoteTip, new_change, span_end, span_start};
use crate::engine::Engine;
use crate::env::{SharedEnv, system_env};
use crate::envelope::{self, Kind, Loaded};
use crate::error::{Error, Result};
use crate::merge::{Hunk, HunkRefusal, hunks, locate};

/// Maximum agent name length in UTF-8 bytes (diamond-types hard limit).
pub const MAX_AGENT_NAME_BYTES: usize = 50;

/// Maximum number of [`Change`] metadata records retained per document.
///
/// The CRDT op log is bounded by [`crate::MAX_OPS_PER_DOC`], but the intent
/// log (`changes`) is independent: it grows by one entry per edit and per
/// distinct merged change, and is fully (de)serialized on every save/load.
/// Without a cap it grows forever in long-lived mesh sessions.
///
/// Changes are append-only *metadata* (author/intent annotations) and are
/// not load-bearing for CRDT convergence — the op log carries the
/// authoritative state. When the count exceeds this cap, the oldest entries
/// are dropped (FIFO), retaining the most recent and most relevant intents.
///
/// Deliberately NOT coupled to [`crate::MAX_OPS_PER_DOC`]: changes are
/// fully (de)serialized on every save (~69 bytes each), so this cap is what
/// bounds the per-save sidecar cost (~7 MB worst case). Since a change spans
/// one or more ops, this is a generous bound that steady-state usage never
/// reaches; FIFO eviction makes a tighter-than-ops cap safe.
pub const MAX_CHANGES_PER_DOC: usize = 100_000;

/// The primary public type for kutl-core.
///
/// A `Document` owns an [`Engine`] (the diamond-types CRDT state) and a
/// `Vec<Change>` (the intent-annotated change history).
#[derive(Debug, Clone)]
pub struct Document {
    pub(crate) engine: Engine,
    changes: Vec<Change>,
    /// O(1) dedup index of the ids currently present in `changes`.
    /// Kept in sync with `changes` by [`Document::record_change`].
    seen_change_ids: HashSet<String>,
    /// Uncapped `agent → author_did` snapshot, independent of the FIFO-capped
    /// `changes` log. Records the durable author DID for each diamond-types
    /// per-session agent so blame can resolve a surviving char's agent even
    /// after its change record is evicted past [`MAX_CHANGES_PER_DOC`].
    author_by_agent: HashMap<String, String>,
    /// The change sidecar on disk is the pre-envelope shape: the owner of
    /// the file rewrites it (see [`Document::rewrite_sidecar`]) so the
    /// space is migrated at its first start rather than at each document's
    /// next save. A reader that does not own the file leaves it alone.
    sidecar_is_legacy: bool,
    env: SharedEnv,
}

/// Outcome of a [`Document::replace_content`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplaceOutcome {
    /// Number of coalesced insert/delete actions applied (one per contiguous
    /// changed run; at most 2 on the bulk path). NOT the engine's sequence-
    /// number growth, which is per-character — read `op_count()` deltas for
    /// that.
    pub ops_applied: usize,
    /// Whether the bulk (delete-all + insert-all) path was taken.
    pub was_bulk: bool,
}

/// Outcome of a [`Document::merge_from_base`] call.
#[derive(Debug)]
pub struct MergeOutcome {
    /// Number of coalesced insert/delete actions issued across the placed
    /// hunks (one per contiguous changed run within a hunk, so a hunk whose
    /// context and unchanged interior are untouched contributes only for what
    /// actually changed). NOT the engine's sequence-number growth, which is
    /// per-character — read `op_count()` deltas for that.
    pub ops_applied: usize,
    /// Hunks that found their place in current content.
    pub hunks_applied: usize,
    /// Hunks that did not, each paired with the first line of the region it
    /// wanted that carries text, so a caller can tell which part of an edit
    /// did not land. Empty only when every line of the region is blank.
    pub refused: Vec<(HunkRefusal, String)>,
}

/// Minimum character count (in both old and new) before considering the bulk
/// path. Below this the diff is fast enough that the overhead isn't worth it.
const BULK_MIN_LEN: usize = 500;

/// Minimum absolute size disparity (bytes) before the majority-ratio check
/// may route a save to the bulk path. Below this, the char diff is cheap
/// regardless of ratio, and bulk's costs (double ops, blame reassigned to
/// the saver) buy nothing. Sized at the scale where a majority-disparity
/// diff genuinely approaches its O(N·D) worst case.
const BULK_DISPARITY_FLOOR: usize = 2048;

/// `SimHash` Hamming-distance threshold (out of 64 bits). When the distance
/// exceeds this value the documents are considered structurally different
/// enough to skip the character-level diff entirely.
///
/// Calibrated so that: small edits produce distance ~0-3, rename-style
/// refactorings produce distance ~10-11, and full replacements (truly
/// different content) produce distance ~30.  Threshold of 20 avoids false
/// positives on moderate refactorings while still catching genuine rewrites.
const BULK_HAMMING_THRESHOLD: u32 = 20;

/// Divisor of the larger text's length for the size-disparity bulk trigger: a
/// disparity greater than `max(old_len, new_len) / BULK_DISPARITY_DIVISOR`
/// (more than half the larger text appearing or disappearing) forces the bulk
/// path, independent of `SimHash`.
///
/// The disparity is a lower bound on the edit distance D, and `SimHash`
/// cannot see it: deleting most of a document barely changes its trigram
/// distribution (a 147KB doc shrunk to a 2KB index measured a distance of 9,
/// far under [`BULK_HAMMING_THRESHOLD`]). At majority disparity the char diff
/// is near its O(N·D) worst case AND its minimal ops would approach bulk's
/// `old+new` anyway, so bulk costs little extra.
///
/// A RATIO, not an absolute count: an absolute threshold priced every
/// grow-by-a-few-KB save of a large document — the routine agent whole-file-
/// rewrite pattern — at bulk's `old+new` sequence numbers (~2× the file per
/// save, the op-cap wedge mechanism) while re-attributing unchanged content
/// to the rewriter (destroying blame). Pathological cases the ratio cannot
/// see are bounded by [`DIFF_TIMEOUT`] instead. A side effect of the ratio:
/// filling an empty or near-empty document always counts as bulk (the fill
/// IS most of the larger text), which is the honest `full_rewrite` label for
/// a from-scratch fill and costs the same ops as the diff path would.
const BULK_DISPARITY_DIVISOR: usize = 2;

/// Ceiling on the char-level diff's search time. The guards ahead of it (the
/// size-disparity ratio, `SimHash`) route clearly-different texts to the bulk
/// path, but a text can fool both — e.g. appending a near-copy of the whole
/// document keeps the disparity at exactly half and the trigram distribution
/// unchanged — and the unbounded O(N·D) search then blocks the daemon's event
/// loop for minutes. On timeout `similar` finishes with a coarser but still
/// correct diff, so the cost of firing is extra ops, never a wrong document.
/// Generous enough that ordinary saves never hit it (their diffs finish in
/// milliseconds).
pub(crate) const DIFF_TIMEOUT: Duration = Duration::from_millis(500);

/// Character n-gram size for `SimHash` computation.
const SIMHASH_NGRAM: usize = 3;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;

/// A borrow-scoped helper passed into [`Document::edit`] closures.
///
/// `EditContext` exposes only `insert` and `delete` — preventing the
/// closure from doing anything other than editing.
pub struct EditContext<'a> {
    engine: &'a mut Engine,
    agent: AgentId,
    ops_made: usize,
}

impl EditContext<'_> {
    /// Insert `text` at the given character position.
    pub fn insert(&mut self, pos: usize, text: &str) -> Result<()> {
        self.engine.insert(self.agent, pos, text)?;
        self.ops_made += 1;
        Ok(())
    }

    /// Delete characters in the given range.
    pub fn delete(&mut self, range: Range<usize>) -> Result<()> {
        self.engine.delete(self.agent, range)?;
        self.ops_made += 1;
        Ok(())
    }
}

impl Document {
    /// Create a new, empty document using the system environment.
    #[must_use]
    pub fn new() -> Self {
        Self::with_env(system_env())
    }

    /// Create a new, empty document with a custom environment.
    #[must_use]
    pub fn with_env(env: SharedEnv) -> Self {
        Self {
            engine: Engine::new(),
            changes: Vec::new(),
            seen_change_ids: HashSet::new(),
            author_by_agent: HashMap::new(),
            sidecar_is_legacy: false,
            env,
        }
    }

    /// Register an agent name and return its numeric id.
    ///
    /// Returns [`Error::AgentNameTooLong`] if `name` exceeds
    /// [`MAX_AGENT_NAME_BYTES`].
    pub fn register_agent(&mut self, name: &str) -> Result<AgentId> {
        if name.len() > MAX_AGENT_NAME_BYTES {
            return Err(Error::AgentNameTooLong {
                len: name.len(),
                max: MAX_AGENT_NAME_BYTES,
            });
        }
        Ok(self.engine.get_or_create_agent(name))
    }

    /// Return the current document content.
    #[must_use]
    pub fn content(&self) -> String {
        self.engine.content()
    }

    /// Reconstruct the document content as of a version frontier — the
    /// point-in-time snapshot `document restore` re-asserts as a forward edit.
    /// See [`Engine::checkout`](crate::engine::Engine::checkout).
    #[must_use]
    pub fn content_at(&self, version: &[usize]) -> String {
        self.engine.checkout(version)
    }

    /// Reconstruct the content as of a specific [`Change`]'s frontier, resolving
    /// the cut through the change's *portable* author tip when available.
    ///
    /// A change's `version_span.end` is a frontier of **local** times — valid
    /// only on the replica that authored it. Once the change is merged into a
    /// peer, those local indices name different ops (or none), so checking out
    /// at the raw span end reconstructs the wrong content. The portable
    /// `author_tip` (`agent`, `seq`) survives the hop: we translate it back to
    /// *this* replica's local time via
    /// [`Engine::local_frontier_for`](crate::engine::Engine::local_frontier_for)
    /// and check out there. When the tip is absent (older changes) or not yet
    /// present locally, we fall back to the local span end — correct for a
    /// locally-authored change, the pre-tip status quo otherwise.
    #[must_use]
    pub fn content_at_change(&self, change: &Change) -> String {
        if let Some(tip) = &change.author_tip
            && let Some(t) = self.engine.local_frontier_for(&tip.agent, tip.seq)
        {
            return self.content_at(&[t]);
        }
        self.content_at(&span_end(change))
    }

    /// Whether the current content equals `other`, compared chunk-wise against
    /// the rope — no O(doc-size) `String` materialization (see
    /// [`Engine::content_eq`]).
    #[must_use]
    pub fn content_eq(&self, other: &str) -> bool {
        self.engine.content_eq(other)
    }

    /// Resolve a diamond-types per-session agent to its durable author DID.
    ///
    /// Returns `"unknown"` for an agent with no recorded binding (including the
    /// empty agent name of an empty line). Total — never panics. Backs
    /// [`Document::blame_with_text`] so blame survives change-record eviction.
    #[must_use]
    pub fn author_of(&self, agent: &str) -> String {
        self.author_by_agent
            .get(agent)
            .cloned()
            .unwrap_or_else(|| "unknown".to_owned())
    }

    /// A clone of the durable agent→author-DID map backing
    /// [`Document::blame_with_text`].
    ///
    /// The relay snapshots this into the persisted content envelope so the
    /// uncapped map survives restart.
    #[must_use]
    pub fn author_map(&self) -> std::collections::HashMap<String, String> {
        self.author_by_agent.clone()
    }

    /// Union `other` into the durable author map, insert-if-absent (first
    /// binding wins).
    ///
    /// Matches the binding rule in [`Document::record_change`]
    /// (`.entry(..).or_insert(..)`): the first author to claim an agent wins.
    ///
    /// **Single-minter invariant.** The relay mints every author binding, so
    /// there is exactly one `author_did` per per-session diamond-types agent,
    /// and this union is convergent regardless of arrival order — every source
    /// agrees on each agent's binding, and insert-if-absent can only ever
    /// re-affirm it.
    /// A persisted snapshot is a strict superset of the bindings its replayed
    /// changes carry, so merging the snapshot after replaying the changes is
    /// order-safe: it fills gaps and never overwrites an already-correct entry.
    pub fn merge_author_map(&mut self, other: std::collections::HashMap<String, String>) {
        for (k, v) in other {
            self.author_by_agent.entry(k).or_insert(v);
        }
    }

    /// Re-bind historical agents from the change log: every change carries
    /// its author DID and the local-time span its ops occupy on this replica,
    /// so an agent missing from the author map (recorded before the durable
    /// map existed, when bindings were never persisted) can be recovered from
    /// the span it owns. Insert-if-absent, oldest change first — a live
    /// binding always wins, per the single-minter rule
    /// ([`Document::merge_author_map`]). Multi-element span frontiers are
    /// skipped (a per-edit change advances a linear frontier; anything else
    /// is not attributable to one author). Returns the number of bindings
    /// added; in-memory only — the next save persists them.
    pub fn backfill_author_bindings(&mut self) -> usize {
        let mut added = 0;
        for change in &self.changes {
            if change.author_did.is_empty() {
                continue;
            }
            let (start, end) = (span_start(change), span_end(change));
            if start.len() > 1 || end.len() != 1 {
                continue;
            }
            let first_time = start.first().map_or(0, |&t| t + 1);
            let end_time = end[0] + 1;
            for agent in self.engine.agents_in_time_range(first_time, end_time) {
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.author_by_agent.entry(agent)
                {
                    e.insert(change.author_did.clone());
                    added += 1;
                }
            }
        }
        added
    }

    /// Attribute each line of the current content to its durable author DID,
    /// carrying the line's text alongside the author.
    ///
    /// Returns `(1-based line number, author DID, line text)` triples.
    /// Follows the git-blame convention: a line's author is its first
    /// character's, and an empty line's is its terminating newline's. The
    /// newline convention lives in exactly one place
    /// ([`segment_blame_lines`]). Lines with no known author resolve to
    /// `"unknown"`.
    #[must_use]
    pub fn blame_with_text(&self) -> Vec<(usize, String, String)> {
        let blame = self.engine.blame_agents();
        segment_blame_lines(&self.content())
            .into_iter()
            .map(|(n, first, text)| {
                let agent = first
                    .and_then(|i| blame.get(i))
                    .map_or("", |(a, _)| a.as_str());
                (n, self.author_of(agent), text)
            })
            .collect()
    }

    /// Return the document length in characters.
    #[must_use]
    pub fn len(&self) -> usize {
        self.engine.len()
    }

    /// Return `true` if the document is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.engine.is_empty()
    }

    /// Return a reference to all recorded changes.
    #[must_use]
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }

    /// The primary editing API.
    ///
    /// The closure receives an [`EditContext`] scoped to the given agent.
    /// A version span is captured before and after the closure runs; if
    /// any operations were made, a [`Change`] is recorded.
    pub fn edit(
        &mut self,
        agent: AgentId,
        author: &str,
        intent: &str,
        boundary: Boundary,
        f: impl FnOnce(&mut EditContext<'_>) -> Result<()>,
    ) -> Result<()> {
        let version_before = self.engine.local_version();

        let mut ctx = EditContext {
            engine: &mut self.engine,
            agent,
            ops_made: 0,
        };

        f(&mut ctx)?;

        let ops_made = ctx.ops_made;
        if ops_made == 0 {
            return Ok(());
        }

        let version_after = self.engine.local_version();

        let mut change = new_change(
            author,
            intent,
            boundary,
            &version_before,
            &version_after,
            self.env.now_millis(),
            self.env.gen_id().to_string(),
        );
        change.author_tip = self
            .engine
            .remote_tip(&version_after)
            .map(|(agent, seq)| RemoteTip { agent, seq });
        self.record_change(change);

        Ok(())
    }

    /// Record a change, deduplicating by id and enforcing the count cap.
    ///
    /// A change whose id is already present is ignored (O(1) via
    /// `seen_change_ids`). When appending would exceed
    /// [`MAX_CHANGES_PER_DOC`], the oldest changes are dropped (FIFO) and
    /// their ids removed from the dedup index, keeping the two structures
    /// consistent.
    fn record_change(&mut self, change: Change) {
        // Learn the durable agent→author binding first — independent of dedup
        // and FIFO eviction, so it survives even when the change record itself
        // is later dropped past the cap. Insert-if-absent: the first author to
        // claim an agent wins (agents are per-session UUIDs, so collisions do
        // not occur in practice; this is defensive determinism).
        if let Some(tip) = &change.author_tip
            && !change.author_did.is_empty()
        {
            self.author_by_agent
                .entry(tip.agent.clone())
                .or_insert_with(|| change.author_did.clone());
        }

        if !self.seen_change_ids.insert(change.id.clone()) {
            return; // already recorded
        }
        self.changes.push(change);

        // Enforce the cap by evicting the oldest entries.
        if self.changes.len() > MAX_CHANGES_PER_DOC {
            let excess = self.changes.len() - MAX_CHANGES_PER_DOC;
            for dropped in self.changes.drain(0..excess) {
                self.seen_change_ids.remove(&dropped.id);
            }
        }
    }

    /// Merge remote operations and their associated changes.
    ///
    /// Deduplicates changes by `id` (O(1) via the seen-ids index) to prevent
    /// metadata bloat in mesh topologies where the same change may arrive from
    /// multiple peers, and bounds the intent log at [`MAX_CHANGES_PER_DOC`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::PatchTooLarge`] if `op_bytes` exceeds
    /// [`crate::MAX_PATCH_BYTES`]. Returns [`Error::OpCountExceeded`] if
    /// this document is already at or above [`crate::MAX_OPS_PER_DOC`].
    pub fn merge(&mut self, op_bytes: &[u8], remote_changes: &[Change]) -> Result<()> {
        // Three-check op-count enforcement.
        // 1. Patch byte cap — bounds per-merge damage regardless of current state.
        if op_bytes.len() > crate::MAX_PATCH_BYTES {
            return Err(Error::PatchTooLarge {
                size: op_bytes.len(),
                cap: crate::MAX_PATCH_BYTES,
            });
        }
        // 2. Pre-merge cap check — reject if already at/over cap.
        let current_ops = self.engine.op_count();
        if current_ops >= crate::MAX_OPS_PER_DOC {
            return Err(Error::OpCountExceeded {
                current: current_ops,
                cap: crate::MAX_OPS_PER_DOC,
            });
        }

        self.engine.merge_bytes(op_bytes)?;

        // 3. Post-merge assertion — log overshoot; next merge rejected by check 2.
        let after_ops = self.engine.op_count();
        if after_ops > crate::MAX_OPS_PER_DOC {
            tracing::warn!(
                current = after_ops,
                cap = crate::MAX_OPS_PER_DOC,
                overshoot = after_ops - crate::MAX_OPS_PER_DOC,
                "op-count overshot after merge; next merge will be rejected"
            );
        }

        for rc in remote_changes {
            // O(1) dedup via the seen-ids index, plus FIFO cap enforcement.
            self.record_change(rc.clone());
        }
        Ok(())
    }

    /// Persist the document to disk.
    ///
    /// Writes `<path>` as the `.dt` binary blob and `<path>.changes` as an
    /// envelope snapshot of the change-metadata `ChangeList`.
    ///
    /// Each file is written atomically ([`crate::fs::write_atomic`]): a crash
    /// mid-save must leave the previous complete version, never a truncated
    /// oplog — a truncated `.dt` loads as an empty or partial document, and
    /// the next startup scan then re-incorporates the on-disk text as fresh
    /// ops, duplicating the content cluster-wide. The pair is still two
    /// files: a crash between them leaves a newer oplog with older intent
    /// metadata, which only affects attribution display, never merge state.
    pub fn save(&self, path: &Path) -> Result<()> {
        let dt_bytes = self.engine.encode_full();
        write_atomic(path, &dt_bytes)?;
        self.write_change_sidecar(path)?;
        Ok(())
    }

    /// Write the change-metadata sidecar beside the `.dt` at `path` as an
    /// envelope snapshot, returning the sidecar's path.
    fn write_change_sidecar(&self, path: &Path) -> Result<PathBuf> {
        let sidecar = change_sidecar_path(path);
        let list = ChangeList {
            changes: self.changes.clone(),
            author_by_agent: self.author_by_agent.clone(),
        };
        write_sidecar(&sidecar, self.env.now_millis(), &list)?;
        Ok(sidecar)
    }

    /// Whether the change sidecar read at load was the pre-envelope shape,
    /// so the file's owner should [`Document::rewrite_sidecar`] it.
    #[must_use]
    pub fn needs_sidecar_rewrite(&self) -> bool {
        self.sidecar_is_legacy
    }

    /// Rewrite the pre-envelope change sidecar beside `path` (the `.dt`
    /// path) as an envelope. Only the file's owner calls this: a read-only
    /// reader never writes into another process's document store.
    pub fn rewrite_sidecar(&mut self, path: &Path) -> Result<()> {
        let sidecar = self.write_change_sidecar(path)?;
        // The legacy shape shared the sidecar's path, so the rewrite is in
        // place and there is no old file to retire.
        envelope::note_migration(Kind::Changes, &sidecar, &sidecar);
        self.sidecar_is_legacy = false;
        Ok(())
    }

    /// Load a document from disk using the system environment.
    ///
    /// Reads the `.dt` binary blob from `path` and the optional
    /// `.changes` protobuf sidecar. Tolerates a missing sidecar. For the
    /// file's owner: a sidecar that does not decode is quarantined. A
    /// reader beside a running owner uses [`Self::load_readonly`].
    pub fn load(path: &Path) -> Result<Self> {
        Self::load_with_env(path, system_env())
    }

    /// [`Self::load`] without the recovery action: a sidecar that does not
    /// decode is left in place for the owner to recover and the document
    /// loads without change metadata. For a reader (the CLI) beside the
    /// daemon that owns the document store.
    pub fn load_readonly(path: &Path) -> Result<Self> {
        Self::load_with(path, system_env(), envelope::Recovery::Skip)
    }

    /// [`Self::load`] with a custom environment.
    pub fn load_with_env(path: &Path, env: SharedEnv) -> Result<Self> {
        Self::load_with(path, env, envelope::Recovery::Act)
    }

    fn load_with(path: &Path, env: SharedEnv, recovery: envelope::Recovery) -> Result<Self> {
        let dt_bytes = std::fs::read(path).map_err(|source| Error::FileRead {
            path: path.to_owned(),
            source,
        })?;
        let engine = Engine::load_from(&dt_bytes)?;

        let sidecar = change_sidecar_path(path);
        let mut sidecar_is_legacy = false;
        // A sidecar that does not decode is quarantined (or, for a reader
        // that does not own it, left in place) and the document loads
        // without change metadata: the sidecar carries attribution, never
        // merge state, so blame for the edits it recorded is lost on this
        // replica while the content is untouched; the owner's next save
        // writes a fresh one.
        let (changes, author_by_agent) = match envelope::load_or_recover::<ChangeList>(
            Kind::Changes,
            &sidecar,
            Some(&crate::legacy::CHANGES_V0),
            recovery,
        ) {
            Ok(None) => (Vec::new(), HashMap::new()),
            Ok(Some(Loaded::Envelope(list))) => (list.changes, list.author_by_agent),
            Ok(Some(Loaded::Legacy(list))) => {
                sidecar_is_legacy = true;
                (list.changes, list.author_by_agent)
            }
            Err(source) => {
                return Err(Error::FileRead {
                    path: sidecar,
                    source,
                });
            }
        };

        let mut doc = Self {
            engine,
            changes: Vec::new(),
            seen_change_ids: HashSet::new(),
            author_by_agent: HashMap::new(),
            sidecar_is_legacy,
            env,
        };
        // Recover the uncapped agent→author map BEFORE the replay: it holds
        // bindings whose change records were evicted past the cap (so are not in
        // `changes`). The replay's insert-if-absent then re-affirms surviving
        // bindings without overwriting these map-recovered ones.
        doc.author_by_agent = author_by_agent;
        // Re-record through the dedup/cap path so a sidecar that exceeds the
        // cap (e.g. written by an older build) is bounded on load and the
        // dedup index is populated.
        for change in changes {
            doc.record_change(change);
        }
        Ok(doc)
    }

    /// Encode operations added since `version` as a binary delta.
    ///
    /// An out-of-range frontier (bookkeeping recorded against a different
    /// document) degrades to the full stream — see
    /// [`Engine::encode_since`](crate::engine::Engine::encode_since), which
    /// owns the guard. [`Self::changes_since`] applies the same test so the
    /// shipped metadata stays aligned with the ops.
    #[must_use]
    pub fn encode_since(&self, version: &[usize]) -> Vec<u8> {
        self.engine.encode_since(version)
    }

    /// Return changes not yet seen by `version`.
    ///
    /// A change is "unseen" when `version` does not strictly dominate its
    /// span start — i.e. it is concurrent, equal, or newer. Uses proper
    /// vector-clock dominance rather than lexicographic comparison.
    ///
    /// A frontier outside this document's oplog is treated as empty — every
    /// change is returned — mirroring [`Self::encode_since`]'s degrade so ops
    /// and metadata describe the same cut.
    #[must_use]
    pub fn changes_since(&self, version: &[usize]) -> Vec<Change> {
        let version = if self.engine.frontier_within(version) {
            version
        } else {
            &[]
        };
        self.changes
            .iter()
            .filter(|c| !version_dominates(version, &span_start(c)))
            .cloned()
            .collect()
    }

    /// The `(ops, metadata)` delta since `version`: [`Self::encode_since`] and
    /// [`Self::changes_since`] read at one cut, so the shipped metadata always
    /// describes exactly the ops beside it. A caller that must skip the
    /// metadata for an empty delta (a broadcast to an idle subscriber) reads
    /// the two halves separately.
    #[must_use]
    pub fn delta_since(&self, version: &[usize]) -> (Vec<u8>, Vec<Change>) {
        (self.encode_since(version), self.changes_since(version))
    }

    /// Encode the full document state as a binary blob.
    #[must_use]
    pub fn encode_full(&self) -> Vec<u8> {
        self.engine.encode_full()
    }

    /// Replace the entire document content with `new_content`.
    ///
    /// Computes a character-level diff between the current content and
    /// `new_content` (search bounded by [`DIFF_TIMEOUT`]), then applies
    /// minimal insert/delete operations via the CRDT engine — preserving the
    /// sequence-number budget and per-character attribution of everything
    /// that didn't change. Texts that are genuinely mostly-different (a
    /// majority size disparity, or a `SimHash`-distant same-size rewrite)
    /// collapse to a bulk delete-all + insert-all instead: there the minimal
    /// diff would cost nearly as many ops anyway and the O(N·D) search is at
    /// its worst. Returns a [`ReplaceOutcome`] describing what happened
    /// (0 ops if content is unchanged).
    pub fn replace_content(
        &mut self,
        agent: AgentId,
        author: &str,
        intent: &str,
        boundary: Boundary,
        new_content: &str,
    ) -> Result<ReplaceOutcome> {
        let old = self.content();

        if old == new_content {
            return Ok(ReplaceOutcome {
                ops_applied: 0,
                was_bulk: false,
            });
        }

        let old_len = old.chars().count();
        let new_len = new_content.chars().count();

        // Fast path: use SimHash to detect structurally different content
        // *before* running the expensive character-level diff.
        if texts_are_very_different(&old, new_content, old_len, new_len) {
            self.edit(agent, author, intent, boundary, |ctx| {
                if old_len > 0 {
                    ctx.delete(0..old_len)?;
                }
                if !new_content.is_empty() {
                    ctx.insert(0, new_content)?;
                }
                Ok(())
            })?;

            // Invariant: `edit()` appends exactly one Change when ops > 0.
            // Retroactively mark it as a full rewrite.
            if let Some(last) = self.changes.last_mut() {
                last.full_rewrite = true;
            }

            let bulk_ops = usize::from(old_len > 0) + usize::from(!new_content.is_empty());
            return Ok(ReplaceOutcome {
                ops_applied: bulk_ops,
                was_bulk: true,
            });
        }

        // Normal path: character-level diff with minimal CRDT operations.
        let ops = diff_actions(old.as_str(), new_content, 0);

        let op_count = ops.len();
        self.edit(agent, author, intent, boundary, |ctx| {
            apply_diff_actions(ctx, &ops)
        })?;

        Ok(ReplaceOutcome {
            ops_applied: op_count,
            was_bulk: false,
        })
    }

    /// Apply the difference between `base` and `updated` to current content.
    ///
    /// `base` is the text the writer last read. Diffing against it captures
    /// what the writer meant to change; diffing against current content would
    /// capture that plus every concurrent edit the writer never saw, and apply
    /// the latter as deletions.
    ///
    /// Each hunk is placed independently against current content: a hunk
    /// whose region survives unchanged since `base` lands; a hunk whose
    /// region a peer altered or removed is refused rather than guessed at.
    /// Placed hunks are applied latest-region-first so that applying one
    /// cannot invalidate another's not-yet-applied offsets.
    ///
    /// Within a placed hunk only the minimal character-level delta is issued,
    /// not a rewrite of the whole region: a hunk's context lines and the
    /// unchanged lines between two of its changes are not the writer's work
    /// and keep their author, their sequence numbers, and their identity to a
    /// concurrently-editing replica.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying CRDT operations fail (see
    /// [`Document::edit`]).
    pub fn merge_from_base(
        &mut self,
        agent: AgentId,
        author: &str,
        intent: &str,
        boundary: Boundary,
        base: &str,
        updated: &str,
    ) -> Result<MergeOutcome> {
        let mut refused = Vec::new();
        let mut placed: Vec<(Range<usize>, Hunk)> = Vec::new();

        for hunk in hunks(base, updated) {
            match locate(&self.content(), &hunk.before) {
                Ok(range) => placed.push((range, hunk)),
                Err(reason) => {
                    // The first line that carries text, not simply the first:
                    // a hunk opens on leading context, which in markdown is
                    // routinely the blank line above a heading. This one
                    // sentence is all a writer gets about which part of its
                    // edit to reapply, and a blank excerpt says nothing.
                    let excerpt = hunk
                        .before
                        .lines()
                        .map(str::trim)
                        .find(|line| !line.is_empty())
                        .unwrap_or_default()
                        .to_owned();
                    refused.push((reason, excerpt));
                }
            }
        }

        if placed.is_empty() {
            return Ok(MergeOutcome {
                ops_applied: 0,
                hunks_applied: 0,
                refused,
            });
        }

        // Latest region first: applying a hunk shifts every offset after it,
        // and going backwards leaves the untouched ones valid.
        placed.sort_by_key(|(range, _)| std::cmp::Reverse(range.start));

        let hunks_applied = placed.len();
        // A hunk's `before` and `after` share every line of context and every
        // unchanged line between two changes that fell into the same hunk.
        // Deleting the region and re-inserting `after` would rewrite all of
        // that: it reassigns blame for text nobody edited, costs sequence
        // numbers proportional to the hunk rather than the edit, and turns a
        // peer's concurrent edit inside the context into a delete-vs-edit
        // race that relocates the peer's characters. Diffing within the hunk
        // touches only what actually changed.
        let ops: Vec<DiffAction> = placed
            .iter()
            .flat_map(|(range, hunk)| diff_actions(&hunk.before, &hunk.after, range.start))
            .collect();
        let ops_applied = ops.len();
        self.edit(agent, author, intent, boundary, |ctx| {
            apply_diff_actions(ctx, &ops)
        })?;

        Ok(MergeOutcome {
            ops_applied,
            hunks_applied,
            refused,
        })
    }

    /// Return the total number of CRDT operations stored in this document.
    ///
    /// Used by quota enforcement to check whether the document
    /// is at or above the per-document cap before accepting new remote ops.
    #[must_use]
    pub fn op_count(&self) -> usize {
        self.engine.op_count()
    }

    /// Return the current local version of the underlying engine.
    #[must_use]
    pub fn local_version(&self) -> Vec<usize> {
        self.engine.local_version()
    }
}

/// Internal diff action — one coalesced run of consecutive same-tag character
/// changes.
enum DiffAction {
    Insert {
        pos: usize,
        text: String,
        /// Position one past the run's last inserted char — the coalescing
        /// probe for whether the next insert continues this run.
        end: usize,
    },
    Delete(Range<usize>),
}

/// Emit the minimal character-level delta from `old` to `new` as engine ops,
/// with every position shifted by `base_offset` — the character position of
/// `old` within the document being edited.
///
/// The diff's search is bounded by [`DIFF_TIMEOUT`]; on timeout `similar`
/// finishes with a coarser but still correct diff, so the cost of firing is
/// extra ops, never a wrong document. Consecutive same-tag changes are
/// coalesced into one action per run: the diff yields one change per
/// CHARACTER, and issuing a thousand one-char engine calls for one contiguous
/// paste is pure overhead (the engine's sequence-number accounting is
/// per-character either way).
///
/// The returned actions must be applied in order: positions track a cursor
/// through the text as it is progressively mutated, so applying one action
/// leaves the next one's positions correct.
fn diff_actions(old: &str, new: &str, base_offset: usize) -> Vec<DiffAction> {
    let diff = TextDiffConfig::default()
        .timeout(DIFF_TIMEOUT)
        .diff_chars(old, new);
    let mut ops: Vec<DiffAction> = Vec::new();
    let mut cursor = base_offset;

    for change in diff.iter_all_changes() {
        let char_len = change.value().chars().count();
        match change.tag() {
            ChangeTag::Equal => {
                cursor += char_len;
            }
            ChangeTag::Delete => match ops.last_mut() {
                // Consecutive deletes all target `cursor` (each removal
                // shifts the next char into place), so a run extends one
                // range. An intervening Equal advanced `cursor`, so a stale
                // earlier delete can never match it.
                Some(DiffAction::Delete(range)) if range.start == cursor => {
                    range.end += char_len;
                }
                _ => ops.push(DiffAction::Delete(cursor..cursor + char_len)),
            },
            ChangeTag::Insert => {
                match ops.last_mut() {
                    Some(DiffAction::Insert { end, text, .. }) if *end == cursor => {
                        text.push_str(change.value());
                        *end += char_len;
                    }
                    _ => ops.push(DiffAction::Insert {
                        pos: cursor,
                        text: change.value().to_owned(),
                        end: cursor + char_len,
                    }),
                }
                cursor += char_len;
            }
        }
    }

    ops
}

/// Issue `ops` against the engine, in the order [`diff_actions`] produced them.
fn apply_diff_actions(ctx: &mut EditContext<'_>, ops: &[DiffAction]) -> Result<()> {
    for op in ops {
        match op {
            DiffAction::Insert { pos, text, .. } => ctx.insert(*pos, text)?,
            DiffAction::Delete(range) => ctx.delete(range.clone())?,
        }
    }
    Ok(())
}

impl Default for Document {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute a 64-bit `SimHash` over character trigrams of `text`.
///
/// `SimHash` produces a locality-sensitive fingerprint: similar texts produce
/// hashes with small Hamming distance, dissimilar texts produce distant hashes.
fn simhash(text: &str) -> u64 {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() < SIMHASH_NGRAM {
        let mut h = FNV_OFFSET;
        for &c in &chars {
            h ^= c as u64;
            h = h.wrapping_mul(FNV_PRIME);
        }
        return h;
    }

    let mut v = [0i32; 64];
    for window in chars.windows(SIMHASH_NGRAM) {
        let mut h = FNV_OFFSET;
        for &c in window {
            h ^= c as u64;
            h = h.wrapping_mul(FNV_PRIME);
        }
        for (i, slot) in v.iter_mut().enumerate() {
            if (h >> i) & 1 == 1 {
                *slot += 1;
            } else {
                *slot -= 1;
            }
        }
    }

    let mut hash: u64 = 0;
    for (i, &slot) in v.iter().enumerate() {
        if slot > 0 {
            hash |= 1 << i;
        }
    }
    hash
}

/// Return `true` if two texts are structurally different enough to warrant
/// skipping the character-level diff in favour of a bulk delete + insert.
fn texts_are_very_different(old: &str, new: &str, old_len: usize, new_len: usize) -> bool {
    // A majority size disparity is a majority edit distance → the char diff is
    // near its O(N·D) worst case and its minimal ops approach bulk's anyway.
    // Check this FIRST, before the short-doc fast-path: SimHash misses huge
    // add/deletes (deleting most of a doc barely shifts its trigram
    // distribution — a 147KB→2KB shrink measured distance 9), and the early
    // return below would otherwise route a big→tiny shrink to the slow diff.
    //
    // The absolute floor keeps the ratio from firing on small documents (a
    // 200→600-char edit is a majority disparity but a trivial diff): bulk
    // pays delete-all + insert-all under the saver's agent, which both
    // doubles the op count AND reassigns every surviving character's blame
    // to the saver — acceptable only where the diff is genuinely near its
    // worst case, which a small-disparity edit never is. Ratio and floor
    // are BOTH required: the ratio supplies proportionality (a 2KB change
    // to a 100KB doc is not a rewrite), the floor supplies scale.
    if old_len.abs_diff(new_len) > old_len.max(new_len) / BULK_DISPARITY_DIVISOR
        && old_len.abs_diff(new_len) > BULK_DISPARITY_FLOOR
    {
        return true;
    }

    // Short documents: diff is cheap, and SimHash is unreliable on short text.
    if old_len < BULK_MIN_LEN || new_len < BULK_MIN_LEN {
        return false;
    }

    // Similar sizes: SimHash catches a same-size rewrite (very different content).
    let h_old = simhash(old);
    let h_new = simhash(new);
    let distance = (h_old ^ h_new).count_ones();
    distance > BULK_HAMMING_THRESHOLD
}

/// Segment `content` into its lines for blame, carrying each line's text and
/// the blame-vector index of its first character.
///
/// Returns `(1-based line number, first-char index, line text)` triples. The
/// first-char index is `Some(i)` — the character position of the line's first
/// char in the per-character blame vector from
/// [`Engine::blame_agents`](crate::engine::Engine::blame_agents) — for every
/// line of a non-empty document (an empty line owns its terminating
/// newline), or `None` for the single line of an empty document (which has
/// no character to attribute). The text excludes the terminating `'\n'`.
///
/// This is the single place the newline convention lives:
/// [`Document::blame_with_text`] routes through it so line splitting can
/// never drift.
fn segment_blame_lines(content: &str) -> Vec<(usize, Option<usize>, String)> {
    let mut out: Vec<(usize, Option<usize>, String)> = Vec::new();
    let mut line_number: usize = 1;
    let mut char_index: usize = 0;
    let mut line_start: usize = char_index;
    let mut line_text = String::new();

    for ch in content.chars() {
        if ch == '\n' {
            // An empty line still owns one character — its terminating
            // newline (at `line_start == char_index` right here) — so it
            // attributes to whoever inserted that newline rather than to
            // nobody.
            let first = Some(line_start);
            out.push((line_number, first, std::mem::take(&mut line_text)));
            line_number += 1;
            char_index += 1;
            line_start = char_index;
        } else {
            line_text.push(ch);
            char_index += 1;
        }
    }

    // Trailing line: content that did not end in a newline, or (when the
    // content is empty) the single empty line of an empty document.
    if line_start < char_index || out.is_empty() {
        let first = (line_start < char_index).then_some(line_start);
        out.push((line_number, first, line_text));
    }

    out
}

/// [`crate::fs::write_atomic`] with the error mapped onto the document's
/// write error, naming the destination.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    crate::fs::write_atomic(path, bytes).map_err(|source| Error::FileWrite {
        path: path.to_owned(),
        source,
    })
}

/// Write `list` as the envelope snapshot at `sidecar`, atomically.
fn write_sidecar(sidecar: &Path, now_ms: i64, list: &ChangeList) -> Result<()> {
    envelope::write_snapshot(Kind::Changes, sidecar, now_ms, list).map_err(|e| match e {
        envelope::Error::Io(source) => Error::FileWrite {
            path: sidecar.to_owned(),
            source,
        },
        other => Error::Decode(Box::new(other)),
    })
}

/// The change-metadata sidecar beside the `.dt` at `dt_path`: the same
/// stem with `.changes` appended to the extension (`foo.dt` →
/// `foo.dt.changes`). The one derivation, shared with the daemon, which
/// moves the pair aside together when the `.dt` cannot be read.
#[must_use]
pub fn change_sidecar_path(dt_path: &Path) -> PathBuf {
    dt_path.with_extension(sidecar_extension(dt_path))
}

/// Build the sidecar extension by appending `.changes` to any
/// existing extension. For example `foo.dt` → `dt.changes`.
fn sidecar_extension(path: &Path) -> String {
    match path.extension().and_then(|e| e.to_str()) {
        Some(ext) => format!("{ext}.changes"),
        None => "changes".to_owned(),
    }
}

/// Returns `true` if `version` strictly dominates `span` — meaning
/// `version` has seen every operation that precedes `span`, and at
/// least one agent has progressed further. When this returns `true`,
/// the change at `span` is already known to the holder of `version`.
fn version_dominates(version: &[usize], span: &[usize]) -> bool {
    version.len() >= span.len()
        && span.iter().zip(version).all(|(s, v)| v >= s)
        && (span.iter().zip(version).any(|(s, v)| v > s) || version.len() > span.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A majority ratio on a SMALL disparity must not bulk: a 200→600-char
    /// edit is a trivial diff, and bulk would double the ops and reassign
    /// the surviving 200 chars' blame to the saver. The floor keeps the
    /// ratio check to the scale where the diff genuinely degrades.
    #[test]
    fn test_texts_not_very_different_below_disparity_floor() {
        let old = "x".repeat(200);
        let new = "x".repeat(600);
        assert!(
            !texts_are_very_different(&old, &new, 200, 600),
            "sub-floor majority disparity takes the diff path"
        );
        // Scale both sides up past the floor with the same 3x ratio: bulk.
        let old = "y".repeat(2000);
        let new = "y".repeat(6000);
        assert!(
            texts_are_very_different(&old, &new, 2000, 6000),
            "the same ratio past the floor takes the bulk path"
        );
    }

    #[test]
    fn test_texts_are_very_different_flags_large_size_disparity() {
        // A large doc shrunk to a small one is a huge edit-distance change that
        // `TextDiff::from_chars` handles in O(N·D) (effectively quadratic). SimHash
        // measures only trigram-distribution similarity, so two markdown texts of
        // wildly different SIZES look "similar" (low Hamming distance) and slip
        // through to the slow path. Regression guard: a 147KB→1.9KB shrink
        // (real SimHash distance 9) froze the scan for 221s. The guard
        // must flag a large size disparity regardless of SimHash.
        let para = "- backlog item about `relay` sync and `daemon` startup, plus prose.\n";
        let old = para.repeat(2000); // ~136 KB
        let new = para.repeat(8); // ~540 bytes — still ≥ BULK_MIN_LEN
        let old_len = old.chars().count();
        let new_len = new.chars().count();

        // Precondition: identical vocabulary → SimHash deems them similar. This is
        // exactly the blind spot; without it the test wouldn't exercise the bug.
        let simhash_distance = (simhash(&old) ^ simhash(&new)).count_ones();
        assert!(
            simhash_distance <= BULK_HAMMING_THRESHOLD,
            "precondition: SimHash sees these as similar (distance {simhash_distance})"
        );

        assert!(
            texts_are_very_different(&old, &new, old_len, new_len),
            "a large→small shrink must take the bulk path despite low SimHash distance"
        );
    }

    #[test]
    fn test_replace_content_large_append_takes_diff_path() {
        // A save that GROWS a document by a few KB (the agent whole-file-rewrite
        // pattern: same content plus an appended section) must take the char
        // diff, not the bulk path. Bulk here costs old+new sequence numbers
        // (~2× the file) per save — the op-cap wedge mechanism — when the true
        // edit distance is just the appended text. The disparity guard is a
        // RATIO of the larger length, so "grew by 3KB" is only bulk-worthy on a
        // document small enough that 3KB IS most of it.
        let para = "- backlog item about `relay` sync and `daemon` startup, plus prose.\n";
        let old = para.repeat(90); // ~6 KB
        let appended = para.repeat(45); // ~3 KB — large absolutely, small relative to the doc
        let new = format!("{old}{appended}");

        let mut doc = Document::new();
        let agent = doc.register_agent("t").unwrap();
        doc.replace_content(agent, "a", "seed", Boundary::Explicit, &old)
            .unwrap();
        let ops_before = doc.op_count();

        let outcome = doc
            .replace_content(agent, "a", "append", Boundary::Explicit, &new)
            .unwrap();

        assert!(
            !outcome.was_bulk,
            "a grow-by-3KB save on a 6KB doc must take the diff path"
        );
        assert_eq!(doc.content(), new, "content must equal the new text");
        let appended_len = appended.chars().count();
        let grew_by = doc.op_count() - ops_before;
        assert!(
            grew_by <= appended_len + appended_len / 10,
            "op count must grow by ~the appended text ({appended_len} chars), \
             not old+new (bulk); grew by {grew_by}"
        );
    }

    /// A sidecar written before the envelope (bare prost) still loads, and
    /// the load reports it and the owner's rewrite replaces it in place, so
    /// the space is migrated at its first start rather than at the
    /// document's next save.
    #[test]
    fn test_legacy_sidecar_loads_and_is_rewritten_as_an_envelope() {
        let dir = tmp();
        let dt = dt_path(dir.path());
        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.replace_content(alice, "did:alice", "seed", Boundary::Explicit, "one\n")
            .unwrap();
        doc.save(&dt).unwrap();
        let changes_path = dt.with_extension("dt.changes");
        let bytes = std::fs::read(&changes_path).unwrap();
        let (_, list) =
            envelope::decode_snapshot::<crate::ChangeList>(Kind::Changes, &bytes).unwrap();
        let mut bare = Vec::new();
        prost::Message::encode(&list, &mut bare).unwrap();
        std::fs::write(&changes_path, &bare).unwrap();
        assert!(
            !envelope::has_magic(Kind::Changes, &bare),
            "premise: bare prost"
        );

        let mut loaded = Document::load(&dt).unwrap();
        assert_eq!(
            loaded.changes().len(),
            1,
            "the legacy sidecar's change loaded"
        );
        assert!(loaded.needs_sidecar_rewrite(), "the load reports the shape");
        assert!(
            !envelope::has_magic(Kind::Changes, &std::fs::read(&changes_path).unwrap()),
            "a load never writes into the document store"
        );
        loaded.rewrite_sidecar(&dt).unwrap();
        assert!(!loaded.needs_sidecar_rewrite());
        let rewritten = std::fs::read(&changes_path).unwrap();
        assert!(
            envelope::has_magic(Kind::Changes, &rewritten),
            "rewritten in place"
        );
        assert_eq!(
            envelope::decode_snapshot::<crate::ChangeList>(Kind::Changes, &rewritten)
                .unwrap()
                .1,
            list
        );
    }

    /// A corrupt sidecar is quarantined beside the document, which loads
    /// without change metadata; the next save writes a fresh sidecar.
    #[test]
    fn test_corrupt_sidecar_is_quarantined_and_the_document_still_loads() {
        let dir = tmp();
        let dt = dt_path(dir.path());
        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.replace_content(alice, "did:alice", "seed", Boundary::Explicit, "one\n")
            .unwrap();
        doc.save(&dt).unwrap();
        let changes_path = dt.with_extension("dt.changes");
        let mut bytes = std::fs::read(&changes_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff; // the frame's CRC no longer matches
        std::fs::write(&changes_path, &bytes).unwrap();

        let loaded = Document::load(&dt).unwrap();
        assert_eq!(loaded.content(), "one\n", "content is untouched");
        assert!(loaded.changes().is_empty(), "no change metadata");
        assert!(
            !changes_path.exists(),
            "the corrupt sidecar was moved aside"
        );
        assert!(envelope::corrupt_path_for(&changes_path).exists());
        loaded.save(&dt).unwrap();
        assert!(
            changes_path.exists(),
            "the next save writes a fresh sidecar"
        );
    }

    /// A read-only load beside the owner leaves a corrupt sidecar where it
    /// is and still serves the content.
    #[test]
    fn test_load_readonly_leaves_a_corrupt_sidecar_in_place() {
        let dir = tmp();
        let dt = dt_path(dir.path());
        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.replace_content(alice, "did:alice", "seed", Boundary::Explicit, "one\n")
            .unwrap();
        doc.save(&dt).unwrap();
        let changes_path = dt.with_extension("dt.changes");
        let mut bytes = std::fs::read(&changes_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff; // the frame's CRC no longer matches
        std::fs::write(&changes_path, &bytes).unwrap();

        let loaded = Document::load_readonly(&dt).unwrap();
        assert_eq!(loaded.content(), "one\n", "content is untouched");
        assert!(loaded.changes().is_empty(), "no change metadata");
        assert_eq!(
            std::fs::read(&changes_path).unwrap(),
            bytes,
            "the corrupt sidecar is left in place for its owner"
        );
        assert!(!envelope::corrupt_path_for(&changes_path).exists());
    }

    #[test]
    fn test_blame_backfills_historical_bindings_from_change_log() {
        // A sidecar written before the durable author map existed loads with
        // an EMPTY map, so every line blames to "unknown" even though the
        // change log carries each edit's author and local-time span. The
        // backfill re-binds those agents from the spans — recovering
        // attribution for history recorded before the map was introduced.
        let dir = tmp();
        let dt = dt_path(dir.path());
        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.replace_content(
            alice,
            "did:alice",
            "seed",
            Boundary::Explicit,
            "alpha line\n",
        )
        .unwrap();
        let bob = doc.register_agent("bob").unwrap();
        doc.replace_content(
            bob,
            "did:bob",
            "append",
            Boundary::Explicit,
            "alpha line\nbravo line\n",
        )
        .unwrap();
        doc.save(&dt).unwrap();

        // Reproduce the legacy sidecar: no author map AND no per-change
        // author tips (both arrived with the durable-attribution work), just
        // authors + spans — exactly what a pre-attribution sidecar
        // deserializes to. Loading replays changes through `record_change`,
        // which can re-mint bindings from tips, so tips must go too or the
        // load self-heals and the backfill has nothing to prove.
        let changes_path = dt.with_extension("dt.changes");
        let bytes = std::fs::read(&changes_path).unwrap();
        let (_, mut list) =
            envelope::decode_snapshot::<crate::ChangeList>(Kind::Changes, &bytes).unwrap();
        assert!(!list.author_by_agent.is_empty(), "premise: map was written");
        list.author_by_agent.clear();
        for change in &mut list.changes {
            change.author_tip = None;
        }
        // A pre-attribution sidecar is also pre-envelope: bare prost bytes.
        let mut stripped = Vec::new();
        prost::Message::encode(&list, &mut stripped).unwrap();
        std::fs::write(&changes_path, stripped).unwrap();

        let mut legacy = Document::load(&dt).unwrap();
        assert!(
            legacy
                .blame_with_text()
                .iter()
                .all(|(_, author, _)| author == "unknown"),
            "premise: a stripped map blames everything to unknown"
        );

        let added = legacy.backfill_author_bindings();
        assert!(added >= 2, "both historical agents re-bind, got {added}");
        let rows = legacy.blame_with_text();
        assert_eq!(
            rows.first().map(|(_, a, _)| a.as_str()),
            Some("did:alice"),
            "alice's line recovers her authorship"
        );
        assert_eq!(
            rows.get(1).map(|(_, a, _)| a.as_str()),
            Some("did:bob"),
            "bob's appended line recovers his authorship"
        );
    }

    #[test]
    fn test_blame_attributes_empty_lines_to_their_newline() {
        // An empty line still has a character — its terminating newline — and
        // that newline has an author. Blaming it "unknown" on a healthy doc
        // reads as missing attribution where none is missing.
        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.replace_content(
            alice,
            "did:alice",
            "seed",
            Boundary::Explicit,
            "top\n\nbottom\n",
        )
        .unwrap();
        let rows = doc.blame_with_text();
        assert_eq!(
            rows.get(1).map(|(_, a, _)| a.as_str()),
            Some("did:alice"),
            "the empty middle line attributes via its newline"
        );
    }

    #[test]
    fn test_replace_content_diff_path_preserves_blame() {
        // The bulk path re-attributes every surviving character to the
        // rewriter; the diff path must not. After a second author appends a
        // section, the first author still owns the opening line.
        let para = "- backlog item about `relay` sync and `daemon` startup, plus prose.\n";
        let old = para.repeat(90); // ~6 KB
        let new = format!("{old}{}", para.repeat(45)); // +3 KB

        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.replace_content(alice, "did:alice", "seed", Boundary::Explicit, &old)
            .unwrap();
        let bob = doc.register_agent("bob").unwrap();
        doc.replace_content(bob, "did:bob", "append", Boundary::Explicit, &new)
            .unwrap();

        let rows = doc.blame_with_text();
        assert_eq!(
            rows.first().map(|(_, author, _)| author.as_str()),
            Some("did:alice"),
            "the opening line must still blame to its original author after \
             another author's grow-by-3KB save"
        );
    }

    #[test]
    fn test_replace_content_large_shrink_is_bulk_and_fast() {
        // End-to-end: shrinking a large document to a small one must take the O(n)
        // bulk path, not the O(N·D) char diff. `old` is kept modest so that, if
        // this regresses, the failing run is seconds (not minutes).
        let para = "- backlog item about `relay` sync and `daemon` startup, plus prose.\n";
        let old = para.repeat(300); // ~20 KB
        let new = para.repeat(8); // ~540 bytes, same vocabulary (low SimHash distance)

        let mut doc = Document::new();
        let agent = doc.register_agent("t").unwrap();
        doc.replace_content(agent, "a", "seed", Boundary::Explicit, &old)
            .unwrap();

        let started = std::time::Instant::now();
        let outcome = doc
            .replace_content(agent, "a", "shrink", Boundary::Explicit, &new)
            .unwrap();
        let elapsed = started.elapsed();

        assert!(
            outcome.was_bulk,
            "large→small shrink must take the bulk path"
        );
        assert_eq!(doc.content(), new, "content must equal the new text");
        assert!(
            elapsed < std::time::Duration::from_secs(3),
            "shrink took {elapsed:?} — quadratic char-diff regression"
        );
    }
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Helper: create a temp dir that lives for the scope.
    fn tmp() -> TempDir {
        tempfile::tempdir().unwrap()
    }

    fn dt_path(dir: &Path) -> PathBuf {
        dir.join("test.dt")
    }

    // ---- basic edit ----

    #[test]
    fn test_edit_records_change() {
        let mut doc = Document::new();
        let agent = doc.register_agent("alice").unwrap();
        doc.edit(agent, "alice", "add greeting", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();

        assert_eq!(doc.content(), "hello");
        assert_eq!(doc.changes().len(), 1);
        assert_eq!(doc.changes()[0].intent, "add greeting");
    }

    #[test]
    fn test_noop_edit_records_nothing() {
        let mut doc = Document::new();
        let agent = doc.register_agent("alice").unwrap();
        doc.edit(agent, "alice", "no-op", Boundary::Auto, |_ctx| Ok(()))
            .unwrap();

        assert_eq!(doc.changes().len(), 0);
    }

    #[test]
    fn test_multi_op_single_change() {
        let mut doc = Document::new();
        let agent = doc.register_agent("alice").unwrap();
        doc.edit(
            agent,
            "alice",
            "build sentence",
            Boundary::Explicit,
            |ctx| {
                ctx.insert(0, "hello")?;
                ctx.insert(5, " world")?;
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(doc.content(), "hello world");
        assert_eq!(doc.changes().len(), 1);
    }

    // ---- save / load ----

    #[test]
    fn test_save_load_roundtrip() {
        let dir = tmp();
        let path = dt_path(dir.path());

        let mut doc = Document::new();
        let agent = doc.register_agent("alice").unwrap();
        doc.edit(agent, "alice", "add greeting", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello world")
        })
        .unwrap();

        doc.save(&path).unwrap();

        let restored = Document::load(&path).unwrap();
        assert_eq!(restored.content(), "hello world");
        assert_eq!(restored.changes().len(), 1);
        assert_eq!(restored.changes()[0].intent, "add greeting");
    }

    #[test]
    fn test_load_tolerates_missing_sidecar() {
        let dir = tmp();
        let path = dt_path(dir.path());

        let mut doc = Document::new();
        let agent = doc.register_agent("alice").unwrap();
        doc.edit(agent, "alice", "init", Boundary::Auto, |ctx| {
            ctx.insert(0, "data")
        })
        .unwrap();

        doc.save(&path).unwrap();

        // Remove the sidecar.
        let sidecar = path.with_extension("dt.changes");
        std::fs::remove_file(&sidecar).unwrap();

        let restored = Document::load(&path).unwrap();
        assert_eq!(restored.content(), "data");
        assert!(restored.changes().is_empty());
    }

    // ---- two-replica convergence (key deliverable) ----

    #[test]
    fn test_two_replicas_converge_with_intents() {
        let mut doc_a = Document::new();
        let agent_a = doc_a.register_agent("alice").unwrap();
        doc_a
            .edit(agent_a, "alice", "seed", Boundary::Explicit, |ctx| {
                ctx.insert(0, "hello")
            })
            .unwrap();

        // Fork: doc_b starts from doc_a's full state.
        let dir = tmp();
        let path = dt_path(dir.path());
        doc_a.save(&path).unwrap();
        let mut doc_b = Document::load(&path).unwrap();
        let agent_b = doc_b.register_agent("bob").unwrap();

        let common = doc_a.local_version();

        // Diverge.
        doc_a
            .edit(
                agent_a,
                "alice",
                "append alice",
                Boundary::Explicit,
                |ctx| ctx.insert(5, " alice"),
            )
            .unwrap();

        doc_b
            .edit(agent_b, "bob", "append bob", Boundary::Explicit, |ctx| {
                ctx.insert(5, " bob")
            })
            .unwrap();

        // Exchange deltas + changes.
        let delta_a = doc_a.encode_since(&common);
        let changes_a = doc_a.changes_since(&common);
        let delta_b = doc_b.encode_since(&common);
        let changes_b = doc_b.changes_since(&common);

        doc_a.merge(&delta_b, &changes_b).unwrap();
        doc_b.merge(&delta_a, &changes_a).unwrap();

        // Content must converge.
        assert_eq!(doc_a.content(), doc_b.content());
        let content = doc_a.content();
        assert!(content.contains("alice"), "missing alice in: {content}");
        assert!(content.contains("bob"), "missing bob in: {content}");

        // Both should have all intent records.
        let a_intents: Vec<&str> = doc_a.changes().iter().map(|c| c.intent.as_str()).collect();
        let b_intents: Vec<&str> = doc_b.changes().iter().map(|c| c.intent.as_str()).collect();
        assert!(a_intents.contains(&"append alice"));
        assert!(a_intents.contains(&"append bob"));
        assert!(b_intents.contains(&"append alice"));
        assert!(b_intents.contains(&"append bob"));
    }

    // ---- three-way merge ----

    #[test]
    fn test_three_way_merge() {
        let mut doc_a = Document::new();
        let agent_a = doc_a.register_agent("alice").unwrap();
        doc_a
            .edit(agent_a, "alice", "seed", Boundary::Explicit, |ctx| {
                ctx.insert(0, "base")
            })
            .unwrap();

        // Fork into B and C.
        let full = doc_a.engine.encode_full();
        let changes_full = doc_a.changes().to_vec();

        let mut doc_b = Document::new();
        doc_b.merge(&full, &changes_full).unwrap();
        let agent_b = doc_b.register_agent("bob").unwrap();

        let mut doc_c = Document::new();
        doc_c.merge(&full, &changes_full).unwrap();
        let agent_c = doc_c.register_agent("carol").unwrap();

        let common = doc_a.local_version();

        // All three diverge.
        doc_a
            .edit(agent_a, "alice", "alice edit", Boundary::Explicit, |ctx| {
                ctx.insert(4, " A")
            })
            .unwrap();
        doc_b
            .edit(agent_b, "bob", "bob edit", Boundary::Explicit, |ctx| {
                ctx.insert(0, "B ")
            })
            .unwrap();
        doc_c
            .edit(agent_c, "carol", "carol edit", Boundary::Explicit, |ctx| {
                ctx.insert(4, " C")
            })
            .unwrap();

        // Merge all into doc_a.
        let delta_b = doc_b.encode_since(&common);
        let delta_c = doc_c.encode_since(&common);
        let changes_b = doc_b.changes_since(&common);
        let changes_c = doc_c.changes_since(&common);

        doc_a.merge(&delta_b, &changes_b).unwrap();
        doc_a.merge(&delta_c, &changes_c).unwrap();

        let content = doc_a.content();
        assert!(content.contains('A'), "missing A in: {content}");
        assert!(content.contains('B'), "missing B in: {content}");
        assert!(content.contains('C'), "missing C in: {content}");
    }

    // ---- encode_since delta ----

    // ---- replace_content ----

    fn replace_doc(old: &str, new: &str) -> (Document, ReplaceOutcome) {
        let mut doc = Document::new();
        let agent = doc.register_agent("test").unwrap();
        if !old.is_empty() {
            doc.edit(agent, "test", "seed", Boundary::Explicit, |ctx| {
                ctx.insert(0, old)
            })
            .unwrap();
        }
        let outcome = doc
            .replace_content(agent, "test", "replace", Boundary::Auto, new)
            .unwrap();
        (doc, outcome)
    }

    #[test]
    fn test_replace_content_basic() {
        let (doc, outcome) = replace_doc("hello world", "hello rust");
        assert_eq!(doc.content(), "hello rust");
        assert!(outcome.ops_applied > 0);
    }

    #[test]
    fn test_replace_content_empty_to_text() {
        let (doc, outcome) = replace_doc("", "hello");
        assert_eq!(doc.content(), "hello");
        assert!(outcome.ops_applied > 0);
    }

    #[test]
    fn test_replace_content_text_to_empty() {
        let (doc, outcome) = replace_doc("hello", "");
        assert_eq!(doc.content(), "");
        assert!(outcome.ops_applied > 0);
    }

    #[test]
    fn test_replace_content_identical() {
        let (doc, outcome) = replace_doc("hello", "hello");
        assert_eq!(doc.content(), "hello");
        assert_eq!(outcome.ops_applied, 0);
        assert!(!outcome.was_bulk);
    }

    #[test]
    fn test_replace_content_emoji() {
        let (doc, outcome) = replace_doc("hello 🌍 world", "hello 🌎 world");
        assert_eq!(doc.content(), "hello 🌎 world");
        assert!(outcome.ops_applied > 0);
    }

    #[test]
    fn test_replace_content_cjk() {
        let (doc, outcome) = replace_doc("hello 世界", "hello 宇宙");
        assert_eq!(doc.content(), "hello 宇宙");
        assert!(outcome.ops_applied > 0);
    }

    #[test]
    fn test_replace_content_mixed_multibyte() {
        let (doc, outcome) = replace_doc("a🎉b世c", "a🎊b宙c");
        assert_eq!(doc.content(), "a🎊b宙c");
        assert!(outcome.ops_applied > 0);
    }

    #[test]
    fn test_replace_content_emoji_sequence() {
        let (doc, outcome) = replace_doc(
            "👨\u{200d}👩\u{200d}👧\u{200d}👦 family",
            "👨\u{200d}👩\u{200d}👧 family",
        );
        assert_eq!(doc.content(), "👨\u{200d}👩\u{200d}👧 family");
        assert!(outcome.ops_applied > 0);
    }

    #[test]
    fn test_replace_content_insert_after_multibyte() {
        let (doc, outcome) = replace_doc("🌍abc", "🌍XYZabc");
        assert_eq!(doc.content(), "🌍XYZabc");
        assert!(outcome.ops_applied > 0);
    }

    #[test]
    fn test_replace_content_delete_around_multibyte() {
        let (doc, outcome) = replace_doc("abc🌍def", "abcdef");
        assert_eq!(doc.content(), "abcdef");
        assert!(outcome.ops_applied > 0);
    }

    #[test]
    fn test_replace_content_multiline_unicode() {
        let (doc, outcome) = replace_doc("line1\n世界\nline3", "line1\n宇宙\nline3");
        assert_eq!(doc.content(), "line1\n宇宙\nline3");
        assert!(outcome.ops_applied > 0);
    }

    // ---- bulk replace tests ----

    #[test]
    fn test_bulk_triggers_on_full_rewrite() {
        // 1000 chars fully replaced with completely different content.
        // SimHash should detect maximum structural difference.
        let old: String = "a".repeat(1000);
        let new: String = "b".repeat(1000);
        let (doc, outcome) = replace_doc(&old, &new);
        assert_eq!(doc.content(), new);
        assert!(outcome.was_bulk, "expected bulk path for full rewrite");
        assert!(outcome.ops_applied <= 2, "bulk should use at most 2 ops");
    }

    #[test]
    fn test_bulk_does_not_trigger_for_small_edit() {
        // 5 chars changed in a 1000-char code-like document — SimHash
        // sees near-identical because diverse trigrams dominate the vote.
        let old: String = "let result = compute(value);\n".repeat(36); // ~1008 chars
        let mut new = old.clone();
        new.replace_range(0..5, "XXXXX");
        let (doc, outcome) = replace_doc(&old, &new);
        assert_eq!(doc.content(), new);
        assert!(!outcome.was_bulk, "small edit should not trigger bulk");
    }

    #[test]
    fn test_bulk_does_not_trigger_for_short_documents() {
        // Both below BULK_MIN_LEN — always use diff path.
        let (doc, outcome) = replace_doc("hello world", "goodbye world");
        assert_eq!(doc.content(), "goodbye world");
        assert!(!outcome.was_bulk, "short documents should not trigger bulk");
    }

    #[test]
    fn test_bulk_does_not_trigger_for_moderate_refactoring() {
        // ~20% of characters changed scattered through a long document.
        // `SimHash` should see enough shared structure to keep the diff path.
        let base = "fn process(data: &[u8]) -> Vec<u8> {\n";
        let old: String = base.repeat(30); // ~1110 chars
        let new = old.replace("process", "handle");
        let (doc, outcome) = replace_doc(&old, &new);
        assert_eq!(doc.content(), new);
        assert!(
            !outcome.was_bulk,
            "moderate refactoring should not trigger bulk"
        );
    }

    #[test]
    fn test_bulk_sets_full_rewrite_flag() {
        let old: String = "x".repeat(1000);
        let new: String = "y".repeat(1000);
        let (doc, outcome) = replace_doc(&old, &new);
        assert!(outcome.was_bulk);
        let last_change = doc.changes().last().expect("should have a change");
        assert!(
            last_change.full_rewrite,
            "bulk path should set full_rewrite"
        );
    }

    #[test]
    fn test_non_bulk_does_not_set_full_rewrite_flag() {
        let (doc, outcome) = replace_doc("hello world", "hello rust");
        assert!(!outcome.was_bulk);
        let last_change = doc.changes().last().expect("should have a change");
        assert!(
            !last_change.full_rewrite,
            "non-bulk path should not set full_rewrite"
        );
    }

    #[test]
    fn test_bulk_empty_to_large() {
        // A from-empty fill past the disparity floor is a majority disparity
        // → bulk, and honestly so: the fill IS a full rewrite, and bulk's
        // insert-all costs exactly the ops (and attribution) the diff path
        // would. (A sub-floor fill diffs instead — same output either way.)
        let new: String = "a".repeat(BULK_DISPARITY_FLOOR + 1000);
        let (doc, outcome) = replace_doc("", &new);
        assert_eq!(doc.content(), new);
        assert!(
            outcome.was_bulk,
            "empty-to-large is a full-rewrite bulk fill"
        );
    }

    #[test]
    fn test_bulk_large_to_empty() {
        // Emptying a document past the disparity floor is a majority
        // disparity → bulk; delete-all is exactly the minimal diff here.
        let old: String = "a".repeat(BULK_DISPARITY_FLOOR + 1000);
        let (doc, outcome) = replace_doc(&old, "");
        assert_eq!(doc.content(), "");
        assert!(
            outcome.was_bulk,
            "large-to-empty is a full-rewrite bulk wipe"
        );
    }

    #[test]
    fn test_bulk_does_not_trigger_for_identifier_rename() {
        // Renaming identifiers across a large file (old→new prefix) is a
        // rename refactoring, not a full rewrite — diff path preserves
        // fine-grained CRDT history.
        use std::fmt::Write;
        let mut old = String::new();
        for i in 0..100 {
            writeln!(old, "let old_var_{i} = compute_old({i});").unwrap();
        }
        let mut new = String::new();
        for i in 0..100 {
            writeln!(new, "let new_var_{i} = compute_new({i});").unwrap();
        }
        let (doc, outcome) = replace_doc(&old, &new);
        assert_eq!(doc.content(), new);
        assert!(
            !outcome.was_bulk,
            "identifier rename should not trigger bulk"
        );
    }

    #[test]
    fn test_bulk_triggers_on_completely_different_content() {
        // Code vs prose — almost no shared character trigrams.
        use std::fmt::Write;
        let mut old = String::new();
        for i in 0..20 {
            writeln!(old, "fn compute_{i}(x: i32) -> i32 {{ x * {i} + 1 }}").unwrap();
        }
        let new = "The quick brown fox jumps over the lazy dog repeatedly.\n".repeat(16);
        let (doc, outcome) = replace_doc(&old, &new);
        assert_eq!(doc.content(), new);
        assert!(
            outcome.was_bulk,
            "completely different content should trigger bulk"
        );
    }

    // ---- encode_since delta ----

    // ---- op-count cap ----

    #[test]
    fn test_merge_rejects_when_already_at_cap() {
        /// Chars per insert. Each CHURN cycle (insert + delete of the same
        /// span) consumes `2 × FILL_BLOCK` ops while the content stays one
        /// block, so the fill is fast AND the doc stays a normal-sized
        /// string (an insert-only fill would build a cap-sized one); a final
        /// pure-insert top-up lands exactly on the cap for any cap value.
        const FILL_BLOCK: usize = 50_000;
        // Fill doc to exactly the cap via churn cycles + a remainder top-up.
        let mut doc = Document::new();
        let agent = doc.register_agent("local").unwrap();
        let fill_text: String = "a".repeat(FILL_BLOCK);
        doc.edit(agent, "local", "seed", Boundary::Auto, |ctx| {
            ctx.insert(0, &fill_text)
        })
        .unwrap();
        while doc.op_count() < crate::MAX_OPS_PER_DOC {
            let missing = crate::MAX_OPS_PER_DOC - doc.op_count();
            if missing < 2 * FILL_BLOCK {
                let text: String = "a".repeat(missing);
                doc.edit(agent, "local", "top-up", Boundary::Auto, |ctx| {
                    ctx.insert(0, &text)
                })
                .unwrap();
            } else {
                let text = fill_text.clone();
                doc.edit(agent, "local", "fill", Boundary::Auto, |ctx| {
                    ctx.insert(FILL_BLOCK, &text)?;
                    ctx.delete(0..FILL_BLOCK)
                })
                .unwrap();
            }
        }
        assert_eq!(doc.op_count(), crate::MAX_OPS_PER_DOC);

        let mut remote = Document::new();
        let remote_agent = remote.register_agent("remote").unwrap();
        remote
            .edit(remote_agent, "remote", "x", Boundary::Auto, |ctx| {
                ctx.insert(0, "x")
            })
            .unwrap();
        let patch = remote.engine.encode_since(&[]);

        let result = doc.merge(&patch, &[]);
        assert!(
            matches!(result, Err(Error::OpCountExceeded { .. })),
            "expected OpCountExceeded at cap, got {result:?}"
        );
        assert_eq!(
            doc.op_count(),
            crate::MAX_OPS_PER_DOC,
            "oplog must not advance"
        );
    }

    #[test]
    fn test_merge_rejects_oversized_patch_by_byte_cap() {
        let mut doc = Document::new();
        let patch = vec![0u8; crate::MAX_PATCH_BYTES + 1];
        let result = doc.merge(&patch, &[]);
        assert!(
            matches!(result, Err(Error::PatchTooLarge { .. })),
            "expected PatchTooLarge, got {result:?}"
        );
        assert_eq!(doc.op_count(), 0);
    }

    /// Build a synthetic [`Change`] with a distinct id for dedup/cap tests.
    fn synthetic_change(id: &str) -> Change {
        new_change(
            "remote",
            "merge",
            Boundary::Auto,
            &[],
            &[],
            0,
            id.to_owned(),
        )
    }

    #[test]
    fn test_merge_dedups_change_by_id() {
        // The same change arriving twice (mesh topology) must be recorded once.
        let mut doc = Document::new();
        let change = synthetic_change("dup-id");

        doc.merge(&[], std::slice::from_ref(&change)).unwrap();
        doc.merge(&[], std::slice::from_ref(&change)).unwrap();

        let with_id = doc.changes().iter().filter(|c| c.id == "dup-id").count();
        assert_eq!(with_id, 1, "duplicate change id must be recorded once");
        assert_eq!(doc.changes().len(), 1);
    }

    #[test]
    fn test_merge_caps_change_count() {
        // `changes` must not grow without bound. Once it exceeds
        // MAX_CHANGES_PER_DOC the oldest are dropped, keeping the newest.
        let mut doc = Document::new();
        let overflow = 100;
        let total = MAX_CHANGES_PER_DOC + overflow;

        let incoming: Vec<Change> = (0..total)
            .map(|i| synthetic_change(&format!("change-{i}")))
            .collect();
        doc.merge(&[], &incoming).unwrap();

        assert!(
            doc.changes().len() <= MAX_CHANGES_PER_DOC,
            "changes len {} must not exceed cap {MAX_CHANGES_PER_DOC}",
            doc.changes().len()
        );
        // Newest survive; oldest were dropped.
        let last_id = format!("change-{}", total - 1);
        assert!(
            doc.changes().iter().any(|c| c.id == last_id),
            "newest change must be retained"
        );
        assert!(
            !doc.changes().iter().any(|c| c.id == "change-0"),
            "oldest change must be dropped"
        );
    }

    #[test]
    fn test_dedup_holds_after_cap_eviction() {
        // After eviction, a change whose id was dropped is no longer
        // considered "seen" — re-merging it re-records it (no phantom dedup),
        // and a still-present id is still deduped.
        let mut doc = Document::new();
        let total = MAX_CHANGES_PER_DOC + 10;
        let incoming: Vec<Change> = (0..total)
            .map(|i| synthetic_change(&format!("c-{i}")))
            .collect();
        doc.merge(&[], &incoming).unwrap();

        let len_before = doc.changes().len();
        // A retained id must still dedup.
        let retained = synthetic_change(&format!("c-{}", total - 1));
        doc.merge(&[], std::slice::from_ref(&retained)).unwrap();
        assert_eq!(
            doc.changes().len(),
            len_before,
            "retained id must still be deduped"
        );
    }

    #[test]
    fn test_merge_accepts_real_delta_patch_from_synced_client() {
        // REGRESSION GUARD: delta encoded from non-empty `since` must NOT be rejected.
        // This is the actual production wire format; any bug here bricks sync.
        //
        // Setup mirrors real sync: peer writes initial content, doc merges the full
        // blob (shared history), then peer appends more and sends only the delta.
        let mut peer = Document::new();
        let peer_agent = peer.register_agent("peer").unwrap();
        peer.edit(peer_agent, "peer", "seed", Boundary::Auto, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();

        // doc receives the full initial state — establishing shared history.
        let mut doc = Document::new();
        let full = peer.engine.encode_full();
        doc.merge(&full, &[]).unwrap();

        // peer adds more; doc should be able to merge the delta (non-empty since).
        let since = peer.engine.local_version();
        peer.edit(peer_agent, "peer", "append", Boundary::Auto, |ctx| {
            ctx.insert(5, " world")
        })
        .unwrap();
        let delta_patch = peer.engine.encode_since(&since); // non-empty since!

        let result = doc.merge(&delta_patch, &[]);
        assert!(
            result.is_ok(),
            "real delta patch must merge, got {result:?}"
        );
        assert!(doc.op_count() > 5);
        assert_eq!(doc.content(), "hello world");
    }

    #[test]
    fn test_merge_post_merge_overshoot_blocks_next_merge() {
        /// Size of each fill block in chars = CRDT ops. Large blocks keep the
        /// test fast: `target / FILL_BLOCK` `edit()` calls instead of one
        /// single-char call per op.
        const FILL_BLOCK: usize = 50_000;
        // Fill doc to just under cap using large string inserts (efficient: one
        // edit per block instead of one edit per op).
        let mut doc = Document::new();
        let agent = doc.register_agent("local").unwrap();
        let fill_text: String = "a".repeat(FILL_BLOCK);
        let target = crate::MAX_OPS_PER_DOC - 100;
        let full_blocks = target / FILL_BLOCK;
        let remainder = target % FILL_BLOCK;
        for _ in 0..full_blocks {
            let text = fill_text.clone();
            doc.edit(agent, "local", "fill", Boundary::Auto, |ctx| {
                ctx.insert(0, &text)
            })
            .unwrap();
        }
        if remainder > 0 {
            let text: String = "a".repeat(remainder);
            doc.edit(agent, "local", "fill", Boundary::Auto, |ctx| {
                ctx.insert(0, &text)
            })
            .unwrap();
        }
        // Verify we're at target before the remote merge.
        assert_eq!(doc.op_count(), target, "fill should reach target op count");

        // Build a remote doc with 200 ops (100 inserts + 100 deletes).
        let mut remote = Document::new();
        let remote_agent = remote.register_agent("remote").unwrap();
        // Single edit with 200 individual ops inside it.
        remote
            .edit(remote_agent, "remote", "fill", Boundary::Auto, |ctx| {
                for _ in 0..200 {
                    ctx.insert(0, "x")?;
                }
                Ok(())
            })
            .unwrap();
        let patch = remote.engine.encode_since(&[]);
        let _ = doc.merge(&patch, &[]);

        if doc.op_count() >= crate::MAX_OPS_PER_DOC {
            let mut extra = Document::new();
            let extra_agent = extra.register_agent("extra").unwrap();
            extra
                .edit(extra_agent, "extra", "y", Boundary::Auto, |ctx| {
                    ctx.insert(0, "y")
                })
                .unwrap();
            let extra_patch = extra.engine.encode_since(&[]);
            let result = doc.merge(&extra_patch, &[]);
            assert!(
                matches!(result, Err(Error::OpCountExceeded { .. })),
                "second merge at/over cap must be rejected"
            );
        }
        let overshoot = doc.op_count().saturating_sub(crate::MAX_OPS_PER_DOC);
        assert!(
            overshoot < crate::MAX_PATCH_BYTES / 5,
            "overshoot {overshoot} must be bounded by MAX_PATCH_BYTES/OP_FLOOR"
        );
    }

    #[test]
    fn test_encode_since_delta() {
        let mut doc = Document::new();
        let agent = doc.register_agent("alice").unwrap();
        doc.edit(agent, "alice", "first", Boundary::Auto, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();

        let v1 = doc.local_version();

        doc.edit(agent, "alice", "second", Boundary::Auto, |ctx| {
            ctx.insert(5, " world")
        })
        .unwrap();

        let delta = doc.encode_since(&v1);
        assert!(!delta.is_empty());

        // Apply the delta to a fresh doc that has v1 state.
        let full_at_v1 = {
            let mut d = Document::new();
            let a = d.register_agent("alice").unwrap();
            d.edit(a, "alice", "first", Boundary::Auto, |ctx| {
                ctx.insert(0, "hello")
            })
            .unwrap();
            d
        };
        let mut receiver = full_at_v1;
        let changes = doc.changes_since(&v1);
        receiver.merge(&delta, &changes).unwrap();
        assert_eq!(receiver.content(), "hello world");
    }

    /// `content_eq` must agree with full-String equality, including multibyte
    /// content and chunk boundaries (a long doc spans several rope chunks).
    #[test]
    fn test_content_at_reconstructs_via_change_span_end() {
        // Prove the timestamp→frontier→content path: Document::content_at with
        // the frontier from change::span_end reproduces the content seen at
        // each edit boundary.
        let mut doc = Document::new();
        let agent = doc.register_agent("t").unwrap();

        doc.edit(agent, "t", "first", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();
        let after_first = crate::change::span_end(&doc.changes()[0]);

        doc.edit(agent, "t", "second", Boundary::Explicit, |ctx| {
            ctx.insert(5, " world")
        })
        .unwrap();
        let after_second = crate::change::span_end(&doc.changes()[1]);

        doc.edit(agent, "t", "third", Boundary::Explicit, |ctx| {
            ctx.delete(0..5)
        })
        .unwrap();

        // Tip is " world" (first 5 chars deleted).
        assert_eq!(doc.content(), " world", "tip content");

        // content_at the end of the first change == "hello"
        assert_eq!(
            doc.content_at(&after_first),
            "hello",
            "content as of first change"
        );

        // content_at the end of the second change == "hello world"
        assert_eq!(
            doc.content_at(&after_second),
            "hello world",
            "content as of second change"
        );

        // content_at empty frontier == empty doc
        assert_eq!(doc.content_at(&[]), "", "empty frontier => empty doc");
    }

    #[test]
    fn test_edit_populates_author_tip_with_authoring_agent() {
        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.edit(alice, "did:alice", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();

        let tip = doc
            .changes()
            .last()
            .expect("one change")
            .author_tip
            .as_ref()
            .expect("author_tip populated at author time");
        assert_eq!(tip.agent, "alice");
    }

    #[test]
    fn test_author_of_roundtrips_through_save_load() {
        let dir = tmp();
        let path = dt_path(dir.path());

        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.edit(alice, "did:alice", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();
        doc.save(&path).unwrap();

        let restored = Document::load(&path).unwrap();
        assert_eq!(restored.author_of("alice"), "did:alice");
        assert_eq!(
            restored.author_of("nobody"),
            "unknown",
            "unmapped agent resolves to unknown, never panics"
        );
    }

    #[test]
    fn test_blame_with_text_carries_line_text_and_author() {
        // alice writes line 1 "ab\n", bob appends line 2 "cd".
        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        let bob = doc.register_agent("bob").unwrap();
        doc.edit(alice, "did:alice", "line 1", Boundary::Explicit, |ctx| {
            ctx.insert(0, "ab\n")
        })
        .unwrap();
        doc.edit(bob, "did:bob", "line 2", Boundary::Explicit, |ctx| {
            ctx.insert(3, "cd")
        })
        .unwrap();
        assert_eq!(doc.content(), "ab\ncd");

        assert_eq!(
            doc.blame_with_text(),
            vec![
                (1, "did:alice".to_owned(), "ab".to_owned()),
                (2, "did:bob".to_owned(), "cd".to_owned()),
            ],
            "line 1 → alice/ab, line 2 → bob/cd"
        );
    }

    #[test]
    fn test_blame_with_text_no_phantom_trailing_line() {
        // A trailing newline must NOT yield a phantom empty line 2.
        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.edit(alice, "did:alice", "line 1", Boundary::Explicit, |ctx| {
            ctx.insert(0, "ab\n")
        })
        .unwrap();
        assert_eq!(doc.content(), "ab\n");

        let rows = doc.blame_with_text();
        assert_eq!(rows.len(), 1, "trailing newline yields exactly one row");
        assert_eq!(rows[0], (1, "did:alice".to_owned(), "ab".to_owned()));
    }

    #[test]
    fn test_content_at_change_agrees_with_span_end_locally() {
        // For a locally-authored change, the portable-tip path and the local
        // span-end path must reconstruct identical content.
        let mut doc = Document::new();
        let agent = doc.register_agent("t").unwrap();
        doc.edit(agent, "did:t", "first", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();
        doc.edit(agent, "did:t", "second", Boundary::Explicit, |ctx| {
            ctx.insert(5, " world")
        })
        .unwrap();

        for change in doc.changes() {
            assert_eq!(
                doc.content_at_change(change),
                doc.content_at(&span_end(change)),
                "both paths agree for a locally-authored change"
            );
        }
    }

    #[test]
    fn test_content_eq_matches_string_equality() {
        let mut doc = Document::new();
        let agent = doc.register_agent("t").unwrap();
        let body = "héllo 🌍 multibyte —\n".repeat(500);
        doc.edit(agent, "did:t", "seed", Boundary::Auto, |ctx| {
            ctx.insert(0, &body)
        })
        .unwrap();

        assert!(doc.content_eq(&doc.content()));
        assert!(doc.content_eq(&body));
        // Same length, different bytes (flip one char mid-doc).
        let mut other = body.clone();
        let mid = other.char_indices().nth(1000).map(|(i, _)| i).unwrap();
        other.replace_range(mid..=mid, "X");
        assert_eq!(other.len(), body.len(), "probe stays same-length");
        assert!(!doc.content_eq(&other));
        // Different length.
        assert!(!doc.content_eq(&body[..body.len() - 1]));
        assert!(!doc.content_eq(""));
        // Empty doc.
        let empty = Document::new();
        assert!(empty.content_eq(""));
        assert!(!empty.content_eq("x"));
    }

    #[test]
    fn test_merge_author_map_first_binding_wins() {
        let mut doc = Document::new();

        doc.merge_author_map(HashMap::from([("a".to_owned(), "did:a".to_owned())]));
        assert_eq!(doc.author_of("a"), "did:a", "first binding is installed");
        assert_eq!(
            doc.author_map().get("a").map(String::as_str),
            Some("did:a"),
            "author_map returns the same binding"
        );

        // A conflicting second binding is a no-op — insert-if-absent.
        doc.merge_author_map(HashMap::from([("a".to_owned(), "did:other".to_owned())]));
        assert_eq!(
            doc.author_of("a"),
            "did:a",
            "first binding wins; the union never overwrites"
        );
    }

    #[test]
    fn test_encode_full_is_not_mistaken_for_an_envelope() {
        // A real, edited document's oplog must never be sniffable as an
        // envelope, and diamond-types must keep its `DMNDTYPS` magic. If a
        // future diamond-types bump changes that prefix, this fails in CI.
        let mut doc = Document::new();
        let alice = doc.register_agent("alice").unwrap();
        doc.edit(alice, "did:alice", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hello world")
        })
        .unwrap();

        let blob = doc.encode_full();
        assert_ne!(
            blob[..4],
            crate::content_envelope::ENVELOPE_MAGIC,
            "a legacy oplog must never be mis-sniffed as a content envelope"
        );
        assert_eq!(
            &blob[..4],
            b"DMND",
            "diamond-types oplogs start with DMNDTYPS"
        );
    }

    #[test]
    fn test_merge_from_base_keeps_a_concurrent_edit_elsewhere() {
        // The defect this whole mechanism exists to remove: a writer that
        // never saw a peer's section must not delete it.
        let mut doc = Document::new();
        let a = doc.register_agent("a").unwrap();
        let base = "# Menu\n\n## Starter\n\n## Main\n\n## Pudding\n";
        doc.replace_content(a, "a", "seed", Boundary::Explicit, base)
            .unwrap();

        // A peer appends while our writer is composing.
        let current = format!("{base}\n## Peter's note\n\nkosher check pending\n");
        doc.replace_content(a, "peer", "peer adds", Boundary::Explicit, &current)
            .unwrap();

        // Our writer, still holding `base`, adds a main course.
        let updated = base.replace("## Main\n", "## Main\n\nrisotto\n");
        let out = doc
            .merge_from_base(
                a,
                "writer",
                "propose main",
                Boundary::Explicit,
                base,
                &updated,
            )
            .unwrap();

        assert!(
            out.refused.is_empty(),
            "nothing conflicts: {:?}",
            out.refused
        );
        let got = doc.content();
        assert!(got.contains("risotto"), "the writer's change landed: {got}");
        assert!(
            got.contains("kosher check pending"),
            "the peer's section survived: {got}"
        );
    }

    #[test]
    fn test_merge_from_base_refuses_only_the_contested_hunk() {
        let mut doc = Document::new();
        let a = doc.register_agent("a").unwrap();
        // Starter and Main are separated by enough untouched lines (well past
        // `merge::CONTEXT_LINES` on both sides) that `hunks()` treats their
        // edits as two independent hunks rather than one that spans both.
        let base =
            "# Menu\n\n## Starter\n\nsoup\n\n## Sides\n\nbread\n\nrice\n\n## Main\n\npasta\n";
        doc.replace_content(a, "a", "seed", Boundary::Explicit, base)
            .unwrap();

        // A peer rewrites the Main section out from under our writer.
        let current = base.replace("pasta\n", "gnocchi\n");
        doc.replace_content(a, "peer", "peer edits main", Boundary::Explicit, &current)
            .unwrap();

        // Our writer changes BOTH sections, holding the old base.
        let updated = base
            .replace("soup\n", "gazpacho\n")
            .replace("pasta\n", "risotto\n");
        let out = doc
            .merge_from_base(
                a,
                "writer",
                "two changes",
                Boundary::Explicit,
                base,
                &updated,
            )
            .unwrap();

        assert_eq!(
            out.refused.len(),
            1,
            "only Main is contested: {:?}",
            out.refused
        );
        // The excerpt names the region, and a markdown hunk's leading context
        // opens on the blank line above a heading — so the first line that
        // carries text is the only one that tells a writer where to look.
        assert_eq!(
            out.refused[0].1, "## Main",
            "the excerpt must name the contested section, not a blank line: {:?}",
            out.refused
        );
        assert_eq!(out.hunks_applied, 1);
        let got = doc.content();
        assert!(
            got.contains("gazpacho"),
            "the uncontested change landed: {got}"
        );
        assert!(
            got.contains("gnocchi"),
            "the peer's version of Main stands: {got}"
        );
    }

    #[test]
    fn test_merge_from_base_refuses_neighbours_of_a_contested_change() {
        // Two edits close enough together that `hunks()` groups them into one
        // hunk share that hunk's fate: if either region moved, the combined
        // hunk cannot be placed and BOTH edits refuse, not just the contested
        // one. This is the conservative direction, not a limitation to work
        // around: nothing the writer wrote is silently dropped, and nothing
        // the peer wrote is silently overwritten. The writer is told the
        // whole hunk didn't land and re-reads before resubmitting.
        let mut doc = Document::new();
        let a = doc.register_agent("a").unwrap();
        let base = "# Menu\n\n## Starter\n\nsoup\n\n## Main\n\npasta\n";
        doc.replace_content(a, "a", "seed", Boundary::Explicit, base)
            .unwrap();

        // A peer rewrites Main. Starter sits only a few lines away.
        let current = base.replace("pasta\n", "gnocchi\n");
        doc.replace_content(a, "peer", "peer edits main", Boundary::Explicit, &current)
            .unwrap();

        // Our writer changes both sections, holding the old base. Because
        // Starter and Main share one hunk here, the writer's Starter edit is
        // a neighbour of the contested Main edit, not a separate change.
        let updated = base
            .replace("soup\n", "gazpacho\n")
            .replace("pasta\n", "risotto\n");
        let out = doc
            .merge_from_base(
                a,
                "writer",
                "two changes, close together",
                Boundary::Explicit,
                base,
                &updated,
            )
            .unwrap();

        assert_eq!(
            out.refused.len(),
            1,
            "one shared hunk, refused as a unit: {:?}",
            out.refused
        );
        assert_eq!(
            out.hunks_applied, 0,
            "the neighbour does not land on its own: {out:?}"
        );
        let got = doc.content();
        assert!(
            !got.contains("gazpacho"),
            "the neighbour refuses along with the contested change: {got}"
        );
        assert!(
            !got.contains("risotto"),
            "the contested change itself does not land: {got}"
        );
        assert!(
            got.contains("gnocchi"),
            "the peer's version stands untouched: {got}"
        );
    }

    #[test]
    fn test_merge_from_base_applies_an_append_with_no_conflict() {
        let mut doc = Document::new();
        let a = doc.register_agent("a").unwrap();
        let base = "# Menu\n\n## Starter\n";
        doc.replace_content(a, "a", "seed", Boundary::Explicit, base)
            .unwrap();

        let out = doc
            .merge_from_base(
                a,
                "writer",
                "add main",
                Boundary::Explicit,
                base,
                &format!("{base}\n## Main\n"),
            )
            .unwrap();

        assert!(out.refused.is_empty());
        assert!(doc.content().contains("## Main"));
    }

    #[test]
    fn test_merge_from_base_is_a_noop_when_nothing_changed() {
        let mut doc = Document::new();
        let a = doc.register_agent("a").unwrap();
        let base = "# Menu\n";
        doc.replace_content(a, "a", "seed", Boundary::Explicit, base)
            .unwrap();
        let before = doc.changes().len();

        let out = doc
            .merge_from_base(a, "writer", "nothing", Boundary::Explicit, base, base)
            .unwrap();

        assert_eq!(out.ops_applied, 0);
        assert_eq!(
            doc.changes().len(),
            before,
            "a no-op must not record a change"
        );
    }

    #[test]
    fn test_merge_from_base_applies_multiple_hunks_in_one_call() {
        // Three widely-separated edits of different shapes landed in one
        // call: a pure insertion, a pure deletion, and a replacement.
        // Applying a hunk shifts every later offset, so the
        // descending-by-`range.start` sort in `merge_from_base` is load-
        // bearing here — a test with only one hunk can't catch an ordering
        // bug that corrupts a later hunk's position.
        let mut doc = Document::new();
        let a = doc.register_agent("a").unwrap();
        let base = "# Menu\n\n## Starter\n\nsoup\n\n## Sides\n\nbread\n\ncheese\n\ncrackers\n\nnapkins\n\nrice\n\n## Main\n\ngreens\n\ncarrots\n\nlentils\n\nonions\n\npasta\n\n## Dessert\n\ncake\n";
        doc.replace_content(a, "a", "seed", Boundary::Explicit, base)
            .unwrap();

        let updated = base
            // Pure insertion: a new line after `soup`.
            .replace("soup\n\n", "soup\n\ntoppings: parmesan\n\n")
            // Pure deletion: `rice` removed outright.
            .replace("rice\n\n", "")
            // Replacement: `pasta` becomes `risotto`.
            .replace("pasta\n\n", "risotto\n\n");

        let out = doc
            .merge_from_base(
                a,
                "writer",
                "three changes",
                Boundary::Explicit,
                base,
                &updated,
            )
            .unwrap();

        assert!(
            out.refused.is_empty(),
            "nothing conflicts: {:?}",
            out.refused
        );
        assert_eq!(out.hunks_applied, 3, "three independent hunks: {out:?}");
        assert_eq!(
            doc.content(),
            updated,
            "content must match exactly, not merely contain each fragment"
        );
    }

    #[test]
    fn test_merge_from_base_applies_a_pure_deletion_as_one_op() {
        // A hunk whose `after` is empty (the writer's base fully removed,
        // nothing put in its place) must issue only the delete: no insert of
        // empty text, so `ops_applied` counts one operation, not two.
        let mut doc = Document::new();
        let a = doc.register_agent("a").unwrap();
        let base = "## Draft note\n\nthis whole section goes\n";
        doc.replace_content(a, "a", "seed", Boundary::Explicit, base)
            .unwrap();

        // A peer appends text the writer never read.
        let current = format!("{base}## Kept\n\nsurrounding text\n");
        doc.replace_content(a, "peer", "peer appends", Boundary::Explicit, &current)
            .unwrap();

        // The writer, holding the old base, drops the whole section it read.
        let out = doc
            .merge_from_base(a, "writer", "drop draft note", Boundary::Explicit, base, "")
            .unwrap();

        assert!(
            out.refused.is_empty(),
            "nothing conflicts: {:?}",
            out.refused
        );
        assert_eq!(out.hunks_applied, 1);
        assert_eq!(out.ops_applied, 1, "delete only, no insert of empty text");
        assert_eq!(
            doc.content(),
            "## Kept\n\nsurrounding text\n",
            "the peer's untouched text survives exactly"
        );
    }

    #[test]
    fn test_merge_from_base_leaves_context_lines_with_their_author() {
        // A hunk carries unchanged context either side of the change. Those
        // lines are not the writer's work and must keep their author: a merge
        // that deletes and re-inserts the whole region reassigns blame for
        // text nobody edited.
        let mut doc = Document::new();
        let seed = doc.register_agent("seed").unwrap();
        let writer = doc.register_agent("writer").unwrap();
        let base = "l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\nl9\n";
        doc.edit(seed, "did:seed", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, base)
        })
        .unwrap();

        let updated = base.replace("l5\n", "l5 amended\n");
        doc.merge_from_base(
            writer,
            "did:writer",
            "one line",
            Boundary::Explicit,
            base,
            &updated,
        )
        .unwrap();

        let strayed: Vec<_> = doc
            .blame_with_text()
            .into_iter()
            .filter(|(_, author, text)| text != "l5 amended" && author != "did:seed")
            .collect();
        assert!(
            strayed.is_empty(),
            "untouched lines kept their author: {strayed:?}"
        );
    }

    #[test]
    fn test_merge_from_base_leaves_a_concurrent_edit_in_the_context_in_place() {
        // The relay and a daemon are separate replicas of one document, so a
        // merge on one racing an ordinary edit on the other is routine. If
        // the merge rewrites its whole hunk, the peer's concurrent edit
        // inside that hunk's untouched context is a delete-vs-edit race and
        // the CRDT relocates the peer's characters to the region's edge.
        let base = "l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\nl9\n";
        let mut doc_a = Document::new();
        let a = doc_a.register_agent("a").unwrap();
        doc_a
            .edit(a, "did:a", "seed", Boundary::Explicit, |ctx| {
                ctx.insert(0, base)
            })
            .unwrap();

        let dir = tmp();
        let path = dt_path(dir.path());
        doc_a.save(&path).unwrap();
        let mut doc_b = Document::load(&path).unwrap();
        let b = doc_b.register_agent("b").unwrap();
        let common = doc_a.local_version();

        // Replica A merges a change to l5. Replica B, concurrently, appends
        // to l3 — inside A's context window, not inside A's change.
        doc_a
            .merge_from_base(
                a,
                "did:a",
                "amend l5",
                Boundary::Explicit,
                base,
                &base.replace("l5\n", "l5 amended\n"),
            )
            .unwrap();
        let l3_end = base.find("l3\n").expect("l3 present") + "l3".len();
        doc_b
            .edit(b, "did:b", "peer note", Boundary::Explicit, |ctx| {
                ctx.insert(l3_end, " (peer note)")
            })
            .unwrap();

        let delta_a = doc_a.encode_since(&common);
        let changes_a = doc_a.changes_since(&common);
        let delta_b = doc_b.encode_since(&common);
        let changes_b = doc_b.changes_since(&common);
        doc_a.merge(&delta_b, &changes_b).unwrap();
        doc_b.merge(&delta_a, &changes_a).unwrap();

        assert_eq!(doc_a.content(), doc_b.content(), "replicas converge");
        assert!(
            doc_a.content().contains("l3 (peer note)\n"),
            "the peer's text stays on its own line: {:?}",
            doc_a.content()
        );
    }

    #[test]
    fn test_merge_from_base_costs_ops_for_the_edit_not_the_hunk() {
        // Sequence numbers are a per-document budget. A merge that rewrites
        // its whole hunk prices a one-line edit at the size of the hunk, and
        // a document whose edits fall within `CONTEXT_LINES` of each other
        // collapses into one hunk spanning everything — bulk rewrite by
        // another name.
        let mut doc = Document::new();
        let a = doc.register_agent("a").unwrap();
        let mut base = String::new();
        for i in 0..500 {
            use std::fmt::Write as _;
            writeln!(base, "line {i}").expect("writing to a String cannot fail");
        }
        doc.replace_content(a, "did:seed", "seed", Boundary::Explicit, &base)
            .unwrap();
        let before = doc.op_count();

        let updated = base.replace("line 250\n", "line 250 amended\n");
        doc.merge_from_base(
            a,
            "did:writer",
            "one line",
            Boundary::Explicit,
            &base,
            &updated,
        )
        .unwrap();

        // The edit itself is 8 characters. The bound is generous enough that
        // a coarse-but-minimal diff still passes and a whole-hunk rewrite
        // (which costs the hunk's ~70 characters twice) cannot.
        let grew = doc.op_count() - before;
        assert!(
            grew <= 32,
            "a one-line edit must cost ops for the edit, not the hunk: {grew}"
        );
        assert_eq!(doc.content(), updated);
    }

    #[test]
    fn test_encode_since_invalid_frontier_degrades_to_full() {
        // A frontier naming times this oplog does not contain — bookkeeping
        // recorded against a DIFFERENT document — must not panic the encode
        // (diamond-types unwraps a failed history lookup on such input, and a
        // multi-element frontier reaches it). The document degrades to the
        // full stream, and changes_since degrades in step so the shipped ops
        // and metadata stay aligned.
        let mut doc = Document::new();
        let agent = doc.register_agent("t").unwrap();
        doc.edit(agent, "t", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, "hi")
        })
        .unwrap();

        let full = doc.encode_since(&[]);
        let degraded = doc.encode_since(&[3, 599]);
        assert_eq!(
            degraded, full,
            "an invalid frontier encodes the full stream"
        );
        assert_eq!(
            doc.changes_since(&[3, 599]).len(),
            doc.changes_since(&[]).len(),
            "metadata degrades in step with the ops"
        );
    }
}
