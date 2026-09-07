//! Decoders for the shapes kutl-core's files had before they carried the
//! envelope. The live types carry no knowledge of those shapes: each decoder
//! here reads the old bytes into the current message, names the layout
//! version at which it must be deleted, and is exercised against a fixture
//! captured from the last release that wrote the old shape. The expiry
//! test calls every decoder's `assert_not_expired`: when a decoder's
//! version ships, the test fails until the decoder, its types, and its
//! fixture are gone.

use prost::Message;

use crate::change::ChangeList;
use crate::envelope::{Kind, Legacy};

/// The `<doc>.changes` sidecar before the envelope: a bare `ChangeList`
/// encoding at the same path, with no header.
pub(crate) const CHANGES_V0: Legacy<ChangeList> = Legacy {
    kind: Kind::Changes,
    expires_at_version: 2,
    decode: changes_v0,
};

fn changes_v0(bytes: &[u8]) -> Option<ChangeList> {
    // The envelope reader hands a same-path file to this decoder only when
    // it is at least a magic long, so an empty file never reaches here from
    // disk. The guard is defence in depth behind that filter: an empty
    // `ChangeList` encodes to zero bytes, so without it a zero-length file,
    // which is what a truncated write leaves behind, would decode as a
    // valid empty list instead of being refused as damage.
    if bytes.is_empty() {
        return None;
    }
    ChangeList::decode(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_decoder_has_expired() {
        CHANGES_V0.assert_not_expired();
    }

    #[test]
    fn test_empty_bytes_are_damage_not_an_empty_list() {
        assert!((CHANGES_V0.decode)(b"").is_none());
    }

    /// The fixture is the bare-prost sidecar the last pre-envelope release
    /// wrote for one recorded change; it must keep decoding until the
    /// window closes.
    #[test]
    fn test_changes_v0_fixture_decodes() {
        let bytes = include_bytes!("../legacy-fixtures/v0/doc.md.changes");
        let list = (CHANGES_V0.decode)(bytes).expect("v0 sidecar decodes");
        assert_eq!(list.changes.len(), 1);
        assert_eq!(list.changes[0].intent, "legacy fixture");
        assert_eq!(list.author_by_agent.len(), 1);
    }
}
