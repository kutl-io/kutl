//! Merging one writer's delta into a document another writer has moved on from.
//!
//! A writer sends the text it read and the text it wants. The difference
//! between those two is what it MEANT; the difference between its text and the
//! document's current state is that plus everyone else's work, and applying
//! the latter deletes theirs. These functions extract the former as hunks and
//! find where each one belongs now.

use std::ops::Range;

use similar::TextDiffConfig;

use crate::document::DIFF_TIMEOUT;

/// Lines of unchanged text kept either side of a change, used to find the
/// change again in text that has moved. Too few and a hunk matches in several
/// places; too many and an unrelated edit nearby makes it match nowhere.
const CONTEXT_LINES: usize = 3;

/// One contiguous change, with the context needed to place it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hunk {
    /// The region as the writer last saw it.
    pub before: String,
    /// What the writer wants that region to become.
    pub after: String,
}

/// Why a hunk could not be placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HunkRefusal {
    /// The region is no longer present — someone else changed it.
    RegionChanged,
    /// The region occurs more than once, so placing it would be a guess.
    RegionAmbiguous,
}

/// Split the difference between `base` and `updated` into placeable hunks.
///
/// Both texts come from a caller over the wire, so the line diff's search is
/// bounded by [`DIFF_TIMEOUT`]: unbounded, its O(N·D) worst case is reachable
/// with two same-length texts sharing a line multiset (measured at seconds for
/// tens of thousands of lines) and this runs synchronously while the document
/// is held. On timeout `similar` finishes with a coarser but still correct
/// diff, so the cost of firing is wider hunks — and therefore possibly more
/// refusals — never a wrong document.
pub fn hunks(base: &str, updated: &str) -> Vec<Hunk> {
    let diff = TextDiffConfig::default()
        .timeout(DIFF_TIMEOUT)
        .diff_lines(base, updated);
    diff.grouped_ops(CONTEXT_LINES)
        .into_iter()
        .map(|group| {
            let mut before = String::new();
            let mut after = String::new();
            for op in group {
                for change in diff.iter_changes(&op) {
                    match change.tag() {
                        similar::ChangeTag::Equal => {
                            before.push_str(change.value());
                            after.push_str(change.value());
                        }
                        similar::ChangeTag::Delete => before.push_str(change.value()),
                        similar::ChangeTag::Insert => after.push_str(change.value()),
                    }
                }
            }
            Hunk { before, after }
        })
        .collect()
}

/// Find where `before` sits in `current`, as a CHARACTER range.
///
/// Character, not byte: the engine addresses text by character, and a byte
/// offset past any multi-byte character would land an edit mid-word.
pub fn locate(current: &str, before: &str) -> Result<Range<usize>, HunkRefusal> {
    if before.is_empty() {
        // A hunk cut from an empty base carries nothing to search for. It can
        // only mean the start, and only while the document is still empty —
        // otherwise where the writer meant its text to go is a guess.
        return if current.is_empty() {
            Ok(0..0)
        } else {
            Err(HunkRefusal::RegionChanged)
        };
    }
    // Scanning advances one character past a match, not past the whole match:
    // a region that occurs twice with overlap ("a\nb\na\n" in "a\nb\na\nb\na\n")
    // is genuinely ambiguous, and resuming past the first match would report
    // it as unique and place the writer's edit at a guess.
    let mut first: Option<usize> = None;
    let mut from = 0;
    while let Some(offset) = current[from..].find(before) {
        let byte_start = from + offset;
        if first.is_some() {
            return Err(HunkRefusal::RegionAmbiguous);
        }
        first = Some(byte_start);
        // The match starts here and `before` is non-empty (the empty case
        // returned above), so there is always a character to step over. A
        // fallback would be a lie either way: any offset other than one
        // character forward either re-finds the same match or steps past a
        // region that could hold the second occurrence, and adding a length to
        // `byte_start` puts `from` beyond the string, so the NEXT slice panics
        // with an out-of-range index instead of anything a reader could act on.
        let matched = current[byte_start..]
            .chars()
            .next()
            .expect("a match of a non-empty needle has a first character");
        from = byte_start + matched.len_utf8();
    }
    let byte_start = first.ok_or(HunkRefusal::RegionChanged)?;
    let start = current[..byte_start].chars().count();
    Ok(start..start + before.chars().count())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "# Menu\n\n## Starter\n\n## Main\n\n## Pudding\n";

    #[test]
    fn test_hunks_of_an_unchanged_document_is_empty() {
        assert!(hunks(BASE, BASE).is_empty());
    }

    #[test]
    fn test_hunks_carries_context_around_a_change() {
        let updated = BASE.replace("## Main\n", "## Main\n\nrisotto\n");
        let h = hunks(BASE, &updated);
        assert_eq!(h.len(), 1, "one contiguous change is one hunk: {h:?}");
        assert!(
            h[0].before.contains("## Main"),
            "before must carry context: {h:?}"
        );
        assert!(
            h[0].after.contains("risotto"),
            "after must carry the addition: {h:?}"
        );
    }

    #[test]
    fn test_locate_finds_a_unique_region() {
        let r = locate(BASE, "## Main\n").expect("unique");
        assert_eq!(
            BASE.chars()
                .skip(r.start)
                .take(r.end - r.start)
                .collect::<String>(),
            "## Main\n"
        );
    }

    #[test]
    fn test_locate_refuses_a_region_that_is_gone() {
        // What a concurrently-rewritten region looks like.
        assert_eq!(
            locate(BASE, "## Dessert\n"),
            Err(HunkRefusal::RegionChanged)
        );
    }

    #[test]
    fn test_locate_refuses_an_ambiguous_region() {
        let doubled = format!("{BASE}{BASE}");
        assert_eq!(
            locate(&doubled, "## Main\n"),
            Err(HunkRefusal::RegionAmbiguous)
        );
    }

    #[test]
    fn test_locate_returns_character_offsets_not_bytes() {
        // The engine indexes by character. A multi-byte prefix makes a byte
        // offset silently wrong, and the resulting edit lands mid-word.
        let doc = "héllo\ntarget\n";
        let r = locate(doc, "target\n").expect("unique");
        assert_eq!(r.start, 6, "chars before target: h é l l o \\n");
    }

    #[test]
    fn test_locate_refuses_a_region_that_overlaps_itself() {
        // Non-overlapping scanning reads this as unique: after matching at 0
        // it resumes past the match and never sees the occurrence at char 4.
        // The region truly sits at both, so placing it is a guess.
        assert_eq!(
            locate("a\nb\na\nb\na\n", "a\nb\na\n"),
            Err(HunkRefusal::RegionAmbiguous)
        );
    }

    #[test]
    fn test_hunks_of_an_addition_to_empty_base_has_no_context() {
        let h = hunks("", "first line\n");
        assert_eq!(h.len(), 1);
        assert!(h[0].before.is_empty(), "nothing to anchor to: {h:?}");
    }
}
