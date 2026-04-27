//! File-to-CRDT bridge: diff file content into CRDT operations.
//!
//! Delegates to [`Document::replace_content`] for character-level diffing.

use kutl_core::{AgentId, Boundary, Document};

/// Apply a file content change to a document.
///
/// Diffs `doc.content()` against `new_content` at character granularity
/// and translates the diff into insert/delete operations.
pub fn apply_file_change(
    doc: &mut Document,
    agent: AgentId,
    author_did: &str,
    new_content: &str,
) -> kutl_core::Result<()> {
    doc.replace_content(
        agent,
        author_did,
        "file change",
        Boundary::Auto,
        new_content,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Document, AgentId) {
        let mut doc = Document::new();
        let agent = doc.register_agent("test").unwrap();
        (doc, agent)
    }

    fn setup_with_content(content: &str) -> (Document, AgentId) {
        let (mut doc, agent) = setup();
        doc.edit(agent, "test", "seed", Boundary::Explicit, |ctx| {
            ctx.insert(0, content)
        })
        .unwrap();
        (doc, agent)
    }

    #[test]
    fn test_empty_to_content() {
        let (mut doc, agent) = setup();
        apply_file_change(&mut doc, agent, "test", "hello world").unwrap();
        assert_eq!(doc.content(), "hello world");
    }

    #[test]
    fn test_content_to_empty() {
        let (mut doc, agent) = setup_with_content("hello world");
        apply_file_change(&mut doc, agent, "test", "").unwrap();
        assert_eq!(doc.content(), "");
    }

    #[test]
    fn test_partial_edit() {
        let (mut doc, agent) = setup_with_content("hello world");
        apply_file_change(&mut doc, agent, "test", "hello rust").unwrap();
        assert_eq!(doc.content(), "hello rust");
    }

    #[test]
    fn test_noop_same_content() {
        let (mut doc, agent) = setup_with_content("hello");
        let changes_before = doc.changes().len();
        apply_file_change(&mut doc, agent, "test", "hello").unwrap();
        assert_eq!(doc.changes().len(), changes_before);
    }

    #[test]
    fn test_multi_region_changes() {
        let (mut doc, agent) = setup_with_content("aaa bbb ccc");
        apply_file_change(&mut doc, agent, "test", "xxx bbb yyy").unwrap();
        assert_eq!(doc.content(), "xxx bbb yyy");
    }

    #[test]
    fn test_unicode_content() {
        let (mut doc, agent) = setup_with_content("hello 世界");
        apply_file_change(&mut doc, agent, "test", "hello 宇宙").unwrap();
        assert_eq!(doc.content(), "hello 宇宙");
    }

    #[test]
    fn test_insert_at_beginning() {
        let (mut doc, agent) = setup_with_content("world");
        apply_file_change(&mut doc, agent, "test", "hello world").unwrap();
        assert_eq!(doc.content(), "hello world");
    }

    #[test]
    fn test_append_at_end() {
        let (mut doc, agent) = setup_with_content("hello");
        apply_file_change(&mut doc, agent, "test", "hello world").unwrap();
        assert_eq!(doc.content(), "hello world");
    }

    #[test]
    fn test_multiline_edit() {
        let (mut doc, agent) = setup_with_content("line1\nline2\nline3\n");
        apply_file_change(&mut doc, agent, "test", "line1\nmodified\nline3\n").unwrap();
        assert_eq!(doc.content(), "line1\nmodified\nline3\n");
    }

    #[test]
    fn test_concurrent_offline_edits_merge() {
        // A creates "MIDDLE\n"
        let (mut a, a_agent) = setup();
        apply_file_change(&mut a, a_agent, "did:a", "MIDDLE\n").unwrap();

        // B gets A's state (full encode)
        let a_ops = a.encode_full();
        let a_meta = a.changes_since(&[]);
        let mut b = Document::new();
        b.merge(&a_ops, &a_meta).unwrap();

        // A edits offline: prepend "TOP\n"
        apply_file_change(&mut a, a_agent, "did:a", "TOP\nMIDDLE\n").unwrap();

        // B edits offline: append "BOTTOM\n"
        let b_agent = b.register_agent("did:b").unwrap();
        apply_file_change(&mut b, b_agent, "did:b", "MIDDLE\nBOTTOM\n").unwrap();

        // Exchange all ops (encode_since(&[]) = all ops)
        let a_all = a.encode_since(&[]);
        let a_all_meta = a.changes_since(&[]);
        let b_all = b.encode_since(&[]);
        let b_all_meta = b.changes_since(&[]);

        b.merge(&a_all, &a_all_meta).unwrap();
        a.merge(&b_all, &b_all_meta).unwrap();

        let a_content = a.content();
        let b_content = b.content();

        assert!(a_content.contains("TOP"), "A missing TOP: {a_content:?}");
        assert!(
            a_content.contains("MIDDLE"),
            "A missing MIDDLE: {a_content:?}"
        );
        assert!(
            a_content.contains("BOTTOM"),
            "A missing BOTTOM: {a_content:?}"
        );
        assert_eq!(a_content, b_content, "A and B diverged");
    }
}
