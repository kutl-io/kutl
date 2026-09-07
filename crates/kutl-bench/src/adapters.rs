//! Uniform trait and adapters for benchmarking different CRDT engines.

use std::ops::Range;

use automerge::ReadDoc;
use automerge::transaction::Transactable;
use yrs::{GetString, Text, Transact};

/// Common interface for CRDT text engines under benchmark.
pub trait BenchEngine {
    /// Create a new, empty document.
    fn new() -> Self;

    /// Insert `text` at the given character position.
    fn insert(&mut self, pos: usize, text: &str);

    /// Delete `len` characters starting at `pos`.
    fn delete(&mut self, pos: usize, len: usize);

    /// Return the current document content.
    fn content(&self) -> String;

    /// Return the document length in characters.
    fn len(&self) -> usize;

    /// Return `true` if the document is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// diamond-types (via kutl-core Engine)
// ---------------------------------------------------------------------------

/// Adapter for diamond-types eg-walker engine.
pub struct DtEngine {
    engine: kutl_core::Engine,
    agent: kutl_core::AgentId,
}

impl BenchEngine for DtEngine {
    fn new() -> Self {
        let mut engine = kutl_core::Engine::new();
        let agent = engine.get_or_create_agent("bench");
        Self { engine, agent }
    }

    fn insert(&mut self, pos: usize, text: &str) {
        self.engine.insert(self.agent, pos, text).unwrap();
    }

    fn delete(&mut self, pos: usize, len: usize) {
        self.engine
            .delete(
                self.agent,
                Range {
                    start: pos,
                    end: pos + len,
                },
            )
            .unwrap();
    }

    fn content(&self) -> String {
        self.engine.content()
    }

    fn len(&self) -> usize {
        self.engine.len()
    }
}

// ---------------------------------------------------------------------------
// Yrs (Yjs Rust port)
// ---------------------------------------------------------------------------

/// Adapter for the Yrs CRDT engine.
pub struct YrsEngine {
    doc: yrs::Doc,
    text: yrs::types::text::TextRef,
}

impl BenchEngine for YrsEngine {
    fn new() -> Self {
        let doc = yrs::Doc::new();
        let text = doc.get_or_insert_text("doc");
        Self { doc, text }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn insert(&mut self, pos: usize, text: &str) {
        let mut txn = self.doc.transact_mut();
        self.text.insert(&mut txn, pos as u32, text);
    }

    #[allow(clippy::cast_possible_truncation)]
    fn delete(&mut self, pos: usize, len: usize) {
        let mut txn = self.doc.transact_mut();
        self.text.remove_range(&mut txn, pos as u32, len as u32);
    }

    fn content(&self) -> String {
        let txn = self.doc.transact();
        self.text.get_string(&txn)
    }

    fn len(&self) -> usize {
        let txn = self.doc.transact();
        self.text.len(&txn) as usize
    }
}

// ---------------------------------------------------------------------------
// Automerge
// ---------------------------------------------------------------------------

/// Adapter for the Automerge CRDT engine.
pub struct AutomergeEngine {
    doc: automerge::AutoCommit,
    text_id: automerge::ObjId,
}

impl BenchEngine for AutomergeEngine {
    fn new() -> Self {
        let mut doc = automerge::AutoCommit::new();
        let text_id = doc
            .put_object(automerge::ROOT, "doc", automerge::ObjType::Text)
            .unwrap();
        Self { doc, text_id }
    }

    fn insert(&mut self, pos: usize, text: &str) {
        self.doc.splice_text(&self.text_id, pos, 0, text).unwrap();
    }

    #[allow(clippy::cast_possible_wrap)]
    fn delete(&mut self, pos: usize, len: usize) {
        self.doc
            .splice_text(&self.text_id, pos, len as isize, "")
            .unwrap();
    }

    fn content(&self) -> String {
        self.doc.text(&self.text_id).unwrap()
    }

    fn len(&self) -> usize {
        self.doc.length(&self.text_id)
    }
}

// ---------------------------------------------------------------------------
// Loro
// ---------------------------------------------------------------------------

/// Adapter for the Loro CRDT engine.
///
/// `LoroText` is an Arc-backed handle, so `get_text()` is cheap.
/// We cache it to avoid the lookup on every operation.
pub struct LoroEngine {
    /// Kept alive to anchor the `text` handle.
    _doc: loro::LoroDoc,
    text: loro::LoroText,
}

impl BenchEngine for LoroEngine {
    fn new() -> Self {
        let doc = loro::LoroDoc::new();
        let text = doc.get_text("doc");
        Self { _doc: doc, text }
    }

    fn insert(&mut self, pos: usize, text: &str) {
        self.text.insert(pos, text).unwrap();
    }

    fn delete(&mut self, pos: usize, len: usize) {
        self.text.delete(pos, len).unwrap();
    }

    fn content(&self) -> String {
        self.text.to_string()
    }

    fn len(&self) -> usize {
        self.text.len_unicode()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Run the same smoke test against any `BenchEngine` implementation.
    fn smoke_test<E: BenchEngine>() {
        let mut e = E::new();
        assert!(e.is_empty());

        e.insert(0, "hello");
        assert_eq!(e.content(), "hello");
        assert_eq!(e.len(), 5);

        e.insert(5, " world");
        assert_eq!(e.content(), "hello world");

        e.delete(5, 6);
        assert_eq!(e.content(), "hello");
        assert_eq!(e.len(), 5);
    }

    #[test]
    fn test_dt_engine_smoke() {
        smoke_test::<DtEngine>();
    }

    #[test]
    fn test_yrs_engine_smoke() {
        smoke_test::<YrsEngine>();
    }

    #[test]
    fn test_automerge_engine_smoke() {
        smoke_test::<AutomergeEngine>();
    }

    #[test]
    fn test_loro_engine_smoke() {
        smoke_test::<LoroEngine>();
    }
}
