//! CRDT document — the primary public type for synchronized text.

use std::ops::Range;
use std::path::Path;

use diamond_types::AgentId;
use prost::Message;
use similar::{ChangeTag, TextDiff};

use crate::change::{Boundary, Change, ChangeList, new_change, span_start};
use crate::engine::Engine;
use crate::env::{SharedEnv, system_env};
use crate::error::{Error, Result};

/// Maximum agent name length in UTF-8 bytes (diamond-types hard limit).
pub const MAX_AGENT_NAME_BYTES: usize = 50;

/// The primary public type for kutl-core.
///
/// A `Document` owns an [`Engine`] (the diamond-types CRDT state) and a
/// `Vec<Change>` (the intent-annotated change history per RFD 0002).
#[derive(Debug, Clone)]
pub struct Document {
    pub(crate) engine: Engine,
    changes: Vec<Change>,
    env: SharedEnv,
}

/// Outcome of a [`Document::replace_content`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplaceOutcome {
    /// Number of CRDT operations applied.
    pub ops_applied: usize,
    /// Whether the bulk (delete-all + insert-all) path was taken.
    pub was_bulk: bool,
}

/// Minimum character count (in both old and new) before considering the bulk
/// path. Below this the diff is fast enough that the overhead isn't worth it.
const BULK_MIN_LEN: usize = 500;

/// `SimHash` Hamming-distance threshold (out of 64 bits). When the distance
/// exceeds this value the documents are considered structurally different
/// enough to skip the character-level diff entirely.
///
/// Calibrated so that: small edits produce distance ~0-3, rename-style
/// refactorings produce distance ~10-11, and full replacements (truly
/// different content) produce distance ~30.  Threshold of 20 avoids false
/// positives on moderate refactorings while still catching genuine rewrites.
const BULK_HAMMING_THRESHOLD: u32 = 20;

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

        self.changes.push(new_change(
            author,
            intent,
            boundary,
            &version_before,
            &version_after,
            self.env.now_millis(),
            self.env.gen_id().to_string(),
        ));

        Ok(())
    }

    /// Merge remote operations and their associated changes.
    ///
    /// Deduplicates changes by `id` to prevent metadata bloat in mesh topologies
    /// where the same change may arrive from multiple peers.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PatchTooLarge`] if `op_bytes` exceeds
    /// [`crate::MAX_PATCH_BYTES`]. Returns [`Error::OpCountExceeded`] if
    /// this document is already at or above [`crate::MAX_OPS_PER_DOC`].
    /// See RFD 0063 §3 for the full security rationale.
    pub fn merge(&mut self, op_bytes: &[u8], remote_changes: &[Change]) -> Result<()> {
        // RFD 0063 §3: three-check op-count enforcement.
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
            if !self.changes.iter().any(|c| c.id == rc.id) {
                self.changes.push(rc.clone());
            }
        }
        Ok(())
    }

    /// Persist the document to disk.
    ///
    /// Writes `<path>` as the `.dt` binary blob and
    /// `<path>.changes` as a protobuf-encoded sidecar with intent metadata.
    pub fn save(&self, path: &Path) -> Result<()> {
        let dt_bytes = self.engine.encode_full();
        std::fs::write(path, dt_bytes).map_err(|source| Error::FileWrite {
            path: path.to_owned(),
            source,
        })?;

        let sidecar = path.with_extension(sidecar_extension(path));
        let list = ChangeList {
            changes: self.changes.clone(),
        };
        let proto_bytes = list.encode_to_vec();
        std::fs::write(&sidecar, proto_bytes).map_err(|source| Error::FileWrite {
            path: sidecar,
            source,
        })?;

        Ok(())
    }

    /// Load a document from disk using the system environment.
    ///
    /// Reads the `.dt` binary blob from `path` and the optional
    /// `.changes` protobuf sidecar. Tolerates a missing sidecar.
    pub fn load(path: &Path) -> Result<Self> {
        Self::load_with_env(path, system_env())
    }

    /// Load a document from disk with a custom environment.
    pub fn load_with_env(path: &Path, env: SharedEnv) -> Result<Self> {
        let dt_bytes = std::fs::read(path).map_err(|source| Error::FileRead {
            path: path.to_owned(),
            source,
        })?;
        let engine = Engine::load_from(&dt_bytes)?;

        let sidecar = path.with_extension(sidecar_extension(path));
        let changes = match std::fs::read(&sidecar) {
            Ok(bytes) => {
                let list = ChangeList::decode(bytes.as_slice())?;
                list.changes
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(source) => {
                return Err(Error::FileRead {
                    path: sidecar,
                    source,
                });
            }
        };

        Ok(Self {
            engine,
            changes,
            env,
        })
    }

    /// Encode operations added since `version` as a binary delta.
    #[must_use]
    pub fn encode_since(&self, version: &[usize]) -> Vec<u8> {
        self.engine.encode_since(version)
    }

    /// Return changes not yet seen by `version`.
    ///
    /// A change is "unseen" when `version` does not strictly dominate its
    /// span start — i.e. it is concurrent, equal, or newer. Uses proper
    /// vector-clock dominance rather than lexicographic comparison.
    #[must_use]
    pub fn changes_since(&self, version: &[usize]) -> Vec<Change> {
        self.changes
            .iter()
            .filter(|c| !version_dominates(version, &span_start(c)))
            .cloned()
            .collect()
    }

    /// Encode the full document state as a binary blob.
    #[must_use]
    pub fn encode_full(&self) -> Vec<u8> {
        self.engine.encode_full()
    }

    /// Replace the entire document content with `new_content`.
    ///
    /// Computes a character-level diff between the current content and
    /// `new_content`, then applies minimal insert/delete operations via the
    /// CRDT engine. When the diff indicates a large rewrite (many ops and
    /// majority of characters changed), collapses to a bulk delete-all +
    /// insert-all for performance. Returns a [`ReplaceOutcome`] describing
    /// what happened (0 ops if content is unchanged).
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
        let diff = TextDiff::from_chars(old.as_str(), new_content);
        let mut ops: Vec<DiffAction> = Vec::new();
        let mut cursor: usize = 0;

        for change in diff.iter_all_changes() {
            let char_len = change.value().chars().count();
            match change.tag() {
                ChangeTag::Equal => {
                    cursor += char_len;
                }
                ChangeTag::Delete => {
                    ops.push(DiffAction::Delete(cursor..cursor + char_len));
                }
                ChangeTag::Insert => {
                    ops.push(DiffAction::Insert(cursor, change.value().to_owned()));
                    cursor += char_len;
                }
            }
        }

        let op_count = ops.len();
        self.edit(agent, author, intent, boundary, |ctx| {
            for op in &ops {
                match op {
                    DiffAction::Insert(pos, text) => ctx.insert(*pos, text)?,
                    DiffAction::Delete(range) => ctx.delete(range.clone())?,
                }
            }
            Ok(())
        })?;

        Ok(ReplaceOutcome {
            ops_applied: op_count,
            was_bulk: false,
        })
    }

    /// Return the total number of CRDT operations stored in this document.
    ///
    /// Used by quota enforcement (RFD 0063 §3) to check whether the document
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

/// Internal diff action used by [`Document::replace_content`].
enum DiffAction {
    Insert(usize, String),
    Delete(Range<usize>),
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
    // Short documents: diff is cheap, always use it.
    if old_len < BULK_MIN_LEN || new_len < BULK_MIN_LEN {
        return false;
    }

    let h_old = simhash(old);
    let h_new = simhash(new);
    let distance = (h_old ^ h_new).count_ones();
    distance > BULK_HAMMING_THRESHOLD
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
        // Empty string is below BULK_MIN_LEN → diff path.
        let new: String = "a".repeat(1000);
        let (doc, outcome) = replace_doc("", &new);
        assert_eq!(doc.content(), new);
        assert!(!outcome.was_bulk, "empty-to-large uses diff path");
    }

    #[test]
    fn test_bulk_large_to_empty() {
        // Empty string is below BULK_MIN_LEN → diff path.
        let old: String = "a".repeat(1000);
        let (doc, outcome) = replace_doc(&old, "");
        assert_eq!(doc.content(), "");
        assert!(!outcome.was_bulk, "large-to-empty uses diff path");
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

    // ---- op-count cap (RFD 0063 §3) ----

    #[test]
    fn test_merge_rejects_when_already_at_cap() {
        /// Size of each fill block in chars = CRDT ops. Large blocks keep the
        /// test fast: 20 `edit()` calls instead of 100 000 single-char calls.
        const FILL_BLOCK: usize = 5_000;
        // Fill doc to exactly the cap using large string inserts for speed.
        let mut doc = Document::new();
        let agent = doc.register_agent("local").unwrap();
        let fill_text: String = "a".repeat(FILL_BLOCK);
        let full_blocks = crate::MAX_OPS_PER_DOC / FILL_BLOCK;
        let remainder = crate::MAX_OPS_PER_DOC % FILL_BLOCK;
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
        /// test fast: ~20 `edit()` calls instead of ~100 000 single-char calls.
        const FILL_BLOCK: usize = 5_000;
        // Fill doc to just under cap using large string inserts (efficient: one edit
        // per 5000-char block → ~20 edit calls total instead of ~100k single-char calls).
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
}
