//! Document-marker parsers shared by the OSS record materializer and the
//! commercial Redis enricher. Pure: parse content, diff against
//! known sets, emit tracker events. No IO, no Redis — the commercial side
//! owns emission.

pub mod before_merge;
pub mod code_fence;
pub mod comments;
pub mod decisions;
pub mod in_doc_flag;
pub mod materialize;
pub mod stable_id;

/// Golden-vector conformance for the marker grammars. The vectors file is
/// shared with the TypeScript editor recognizers' test suite, so the two
/// implementations can only change together — vectors pin BEHAVIOR, where
/// comparing regex text would miss semantic drift.
#[cfg(test)]
mod grammar_vectors {
    use super::{comments, in_doc_flag};

    fn vectors() -> serde_json::Value {
        let raw = include_str!("../../../../test-vectors/marker-grammar.json");
        serde_json::from_str(raw).expect("marker-grammar.json parses")
    }

    #[test]
    fn test_mention_kind_list_matches_vectors() {
        let v = vectors();
        let kinds: Vec<&str> = v["mention"]["kinds"]
            .as_array()
            .expect("kinds array")
            .iter()
            .map(|k| k.as_str().expect("kind string"))
            .collect();
        let mut ours = in_doc_flag::VALID_KINDS.to_vec();
        let mut theirs = kinds.clone();
        ours.sort_unstable();
        theirs.sort_unstable();
        assert_eq!(ours, theirs, "VALID_KINDS must match the vector kind list");
    }

    #[test]
    fn test_mention_accept_vectors_parse_exactly() {
        let v = vectors();
        for case in v["mention"]["accept"].as_array().expect("accept array") {
            let input = case["input"].as_str().expect("input");
            let parsed = in_doc_flag::parse_in_doc_flags(input);
            assert_eq!(parsed.len(), 1, "exactly one mention parses from {input:?}");
            let flag = parsed.iter().next().expect("one flag");
            assert_eq!(
                flag.display_name,
                case["display"].as_str().expect("display"),
                "{input:?}"
            );
            assert_eq!(flag.kind, case["kind"].as_str().expect("kind"), "{input:?}");
            assert_eq!(
                flag.target_id,
                case["target"].as_str().expect("target"),
                "{input:?}"
            );
            let want_note = case["note"].as_str().map(str::to_owned);
            assert_eq!(flag.message, want_note, "note/message for {input:?}");
        }
    }

    #[test]
    fn test_mention_reject_vectors_do_not_parse() {
        let v = vectors();
        for case in v["mention"]["reject"].as_array().expect("reject array") {
            let input = case["input"].as_str().expect("input");
            assert!(
                in_doc_flag::parse_in_doc_flags(input).is_empty(),
                "must not parse ({}): {input:?}",
                case["reason"].as_str().unwrap_or("")
            );
        }
    }

    #[test]
    fn test_comment_accept_vectors_parse_exactly() {
        let v = vectors();
        for case in v["comment"]["accept"].as_array().expect("accept array") {
            let input = case["input"].as_str().expect("input");
            let parsed = comments::parse_comment_markers(input);
            assert_eq!(parsed.len(), 1, "exactly one comment parses from {input:?}");
            let marker = parsed.iter().next().expect("one marker");
            assert_eq!(
                marker.anchor_text,
                case["anchor"].as_str().expect("anchor"),
                "{input:?}"
            );
            assert_eq!(
                marker.signal_id,
                case["id"].as_str().expect("id"),
                "{input:?}"
            );
        }
    }

    #[test]
    fn test_comment_reject_vectors_do_not_parse() {
        let v = vectors();
        for case in v["comment"]["reject"].as_array().expect("reject array") {
            let input = case["input"].as_str().expect("input");
            assert!(
                comments::parse_comment_markers(input).is_empty(),
                "must not parse ({}): {input:?}",
                case["reason"].as_str().unwrap_or("")
            );
        }
    }
}
