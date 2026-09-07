//! Before-merge observer that captures document baselines.
//!
//! Stores the pre-merge content of each document so the after-merge
//! observer can diff against it for snippet extraction and mention parsing.

use std::collections::HashMap;
use std::sync::Mutex;

use kutl_core::Document;

use crate::observer::BeforeMergeObserver;

/// Captures document baselines before text merges.
///
/// After-merge consumers (the OSS record materializer and the commercial
/// `AfterMergeEnricher`) retrieve captured baselines to compute snippets
/// and mention diffs.
pub struct BaselineCapture {
    /// Pre-merge content keyed by `(space_id, document_id)`.
    baselines: Mutex<HashMap<(String, String), String>>,
}

impl Default for BaselineCapture {
    fn default() -> Self {
        Self::new()
    }
}

impl BaselineCapture {
    /// Create a new baseline capture store.
    pub fn new() -> Self {
        Self {
            baselines: Mutex::new(HashMap::new()),
        }
    }

    /// Retrieve and remove the captured baseline for a document.
    pub fn take_baseline(&self, space_id: &str, document_id: &str) -> Option<String> {
        self.baselines
            .lock()
            .expect("baseline lock poisoned")
            .remove(&(space_id.to_owned(), document_id.to_owned()))
    }
}

impl BeforeMergeObserver for BaselineCapture {
    fn before_text_merge(&self, space_id: &str, document_id: &str, doc: &Document) {
        let key = (space_id.to_owned(), document_id.to_owned());
        let mut baselines = self.baselines.lock().expect("baseline lock poisoned");
        baselines.entry(key).or_insert_with(|| doc.content());
    }
}

#[cfg(test)]
mod tests {
    use kutl_core::Boundary;

    use super::*;

    #[test]
    fn test_captures_and_returns_baseline() {
        let capture = BaselineCapture::new();
        let mut doc = Document::new();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(agent, "test", "seed", Boundary::Auto, |ctx| {
            ctx.insert(0, "hello")
        })
        .unwrap();
        capture.before_text_merge("space1", "doc1", &doc);
        let baseline = capture.take_baseline("space1", "doc1");
        assert_eq!(baseline, Some("hello".to_owned()));
    }

    #[test]
    fn test_take_removes_baseline() {
        let capture = BaselineCapture::new();
        let doc = Document::new();
        capture.before_text_merge("space1", "doc1", &doc);
        let _ = capture.take_baseline("space1", "doc1");
        assert_eq!(capture.take_baseline("space1", "doc1"), None);
    }

    #[test]
    fn test_keeps_first_capture() {
        let capture = BaselineCapture::new();
        let mut doc = Document::new();
        let agent = doc.register_agent("test").unwrap();
        doc.edit(agent, "test", "first", Boundary::Auto, |ctx| {
            ctx.insert(0, "first")
        })
        .unwrap();
        capture.before_text_merge("space1", "doc1", &doc);
        doc.edit(agent, "test", "second", Boundary::Auto, |ctx| {
            ctx.insert(5, " second")
        })
        .unwrap();
        capture.before_text_merge("space1", "doc1", &doc);
        // First-capture-wins: baseline should be "first", not "first second".
        let baseline = capture.take_baseline("space1", "doc1");
        assert_eq!(baseline, Some("first".to_owned()));
    }

    #[test]
    fn test_missing_baseline_returns_none() {
        let capture = BaselineCapture::new();
        assert_eq!(capture.take_baseline("space1", "doc1"), None);
    }
}
