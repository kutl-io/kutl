//! Change metadata types re-exported from the protobuf schema.
//!
//! The canonical definitions live in `kutl-proto`. This module re-exports
//! them and provides convenience constructors.

pub use kutl_proto::sync::{Boundary, ChangeList, ChangeMetadata as Change, VersionSpan};

/// Create a new [`Change`] with the given id and timestamp.
///
/// This is a pure function — callers obtain `timestamp` and `id` from an
/// [`Env`](crate::Env) implementation.
pub fn new_change(
    author: &str,
    intent: &str,
    boundary: Boundary,
    version_start: &[usize],
    version_end: &[usize],
    timestamp: i64,
    id: String,
) -> Change {
    Change {
        id,
        author_did: author.to_owned(),
        intent: intent.to_owned(),
        boundary: boundary.into(),
        version_span: Some(VersionSpan {
            start: version_start.iter().map(|&v| v as u64).collect(),
            end: version_end.iter().map(|&v| v as u64).collect(),
        }),
        timestamp,
        full_rewrite: false,
        change_snippet: String::new(),
    }
}

/// Extract the version span start as `Vec<usize>`, returning an empty vec
/// if the span is missing.
#[allow(clippy::cast_possible_truncation)] // diamond-types versions fit in usize
pub fn span_start(change: &Change) -> Vec<usize> {
    change
        .version_span
        .as_ref()
        .map(|s| s.start.iter().map(|&v| v as usize).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn test_change_fields() {
        let c = new_change(
            "alice",
            "first",
            Boundary::Explicit,
            &[],
            &[0],
            1000,
            "id-1".into(),
        );
        assert_eq!(c.author_did, "alice");
        assert_eq!(c.intent, "first");
        assert_eq!(c.timestamp, 1000);
        assert_eq!(c.id, "id-1");
    }

    #[test]
    fn test_change_proto_roundtrip() {
        let change = new_change(
            "bob",
            "add greeting",
            Boundary::Explicit,
            &[],
            &[4],
            2000,
            "id-2".into(),
        );

        let bytes = change.encode_to_vec();
        let restored = Change::decode(bytes.as_slice()).unwrap();

        assert_eq!(change, restored);
    }

    #[test]
    fn test_boundary_values() {
        assert_eq!(Boundary::Explicit as i32, 1);
        assert_eq!(Boundary::Auto as i32, 2);
        assert_eq!(Boundary::Unspecified as i32, 0);

        // Round-trip through i32
        let val: i32 = Boundary::Explicit.into();
        assert_eq!(Boundary::try_from(val).unwrap(), Boundary::Explicit);
    }

    #[test]
    fn test_version_span_roundtrip() {
        let span = VersionSpan {
            start: vec![1, 2],
            end: vec![3, 4],
        };
        let bytes = span.encode_to_vec();
        let restored = VersionSpan::decode(bytes.as_slice()).unwrap();
        assert_eq!(span, restored);
    }

    #[test]
    fn test_change_list_roundtrip() {
        let changes = vec![
            new_change(
                "alice",
                "first",
                Boundary::Explicit,
                &[],
                &[4],
                100,
                "a".into(),
            ),
            new_change("bob", "second", Boundary::Auto, &[4], &[8], 200, "b".into()),
        ];
        let list = ChangeList {
            changes: changes.clone(),
        };
        let bytes = list.encode_to_vec();
        let restored = ChangeList::decode(bytes.as_slice()).unwrap();
        assert_eq!(restored.changes, changes);
    }
}
