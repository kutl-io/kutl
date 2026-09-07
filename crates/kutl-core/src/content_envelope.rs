//! Content-envelope codec: an opaque blob combining a document's CRDT oplog
//! with its change-metadata snapshot ([`ChangeList`]).
//!
//! The relay's content backend stores one opaque blob per document. Storing
//! only the raw diamond-types oplog there would lose the
//! `author_by_agent` map (blame attribution) on relay restart. This envelope
//! wraps the oplog and the [`ChangeList`] together so the map survives.
//!
//! # Layout
//!
//! ```text
//! [ENVELOPE_MAGIC (4)] [u32 LE oplog length (4)] [oplog bytes] [ChangeList proto bytes to end]
//! ```
//!
//! # Fail-safe decode
//!
//! [`decode_content_envelope`] never panics on arbitrary input. Any blob that
//! is not a well-formed envelope — including a bare legacy oplog written before
//! this codec existed — decodes as `{ oplog: <whole blob>, metadata: None }`.
//! This lets a relay that only knows how to READ envelopes coexist with old
//! blobs.

use crate::change::ChangeList;
use prost::Message;

/// Magic prefix identifying a content envelope. Chosen so it can never collide
/// with a diamond-types oplog, which always begins `DMNDTYPS` (see the
/// invariant test in [`crate::document`]).
pub const ENVELOPE_MAGIC: [u8; 4] = *b"KCE1";

/// Fixed header size: 4 magic bytes + a 4-byte little-endian `u32` oplog length.
const HEADER_LEN: usize = 8;

/// The result of decoding a content envelope.
///
/// `metadata` is `Some` only when `blob` was a well-formed envelope; otherwise
/// `oplog` holds the entire input verbatim and `metadata` is `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedEnvelope {
    /// The extracted CRDT oplog bytes.
    pub oplog: Vec<u8>,
    /// The change-metadata snapshot, if the blob was a valid envelope.
    pub metadata: Option<ChangeList>,
}

/// Encode an oplog and its change-metadata snapshot into one opaque blob.
///
/// See the module docs for the byte layout.
#[must_use]
pub fn encode_content_envelope(oplog: &[u8], meta: &ChangeList) -> Vec<u8> {
    // Oplogs are bounded far below `u32::MAX` (the relay's 25 MB flush-envelope
    // ceiling, see [`crate::MAX_PATCH_BYTES`]'s doc), so
    // this conversion never saturates in practice; the guard keeps the encoder
    // total (still produces a decodable-shaped envelope) instead of panicking.
    let oplog_len = u32::try_from(oplog.len()).unwrap_or(u32::MAX);

    let mut out = Vec::with_capacity(HEADER_LEN + oplog.len() + meta.encoded_len());
    out.extend_from_slice(&ENVELOPE_MAGIC);
    out.extend_from_slice(&oplog_len.to_le_bytes());
    out.extend_from_slice(oplog);
    meta.encode(&mut out)
        .expect("Vec<u8> is an infallible prost sink");
    out
}

/// Decode a content envelope, falling back to treating the whole blob as a
/// bare legacy oplog.
///
/// FAIL-SAFE and total: never panics on any input. Returns
/// `metadata: Some(..)` only when every check passes — magic prefix present,
/// length at least [`HEADER_LEN`], the declared oplog length does not overrun
/// the buffer, and the trailing bytes decode as a [`ChangeList`]. In every
/// other case the entire input is returned as `oplog` with `metadata: None`.
#[must_use]
pub fn decode_content_envelope(blob: &[u8]) -> DecodedEnvelope {
    let legacy = || DecodedEnvelope {
        oplog: blob.to_vec(),
        metadata: None,
    };

    if blob.len() < HEADER_LEN || blob[..ENVELOPE_MAGIC.len()] != ENVELOPE_MAGIC {
        return legacy();
    }

    // Bytes [4..8] are the little-endian oplog length. The slice is exactly 4
    // bytes wide (HEADER_LEN - magic == 4), so `try_into` cannot fail.
    let Ok(len_bytes) = <[u8; 4]>::try_from(&blob[ENVELOPE_MAGIC.len()..HEADER_LEN]) else {
        return legacy();
    };
    let oplog_len = u32::from_le_bytes(len_bytes) as usize;

    // Reject a declared length that would overrun the buffer.
    let Some(meta_start) = HEADER_LEN.checked_add(oplog_len) else {
        return legacy();
    };
    if meta_start > blob.len() {
        return legacy();
    }

    let oplog = &blob[HEADER_LEN..meta_start];
    let meta_bytes = &blob[meta_start..];
    match ChangeList::decode(meta_bytes) {
        Ok(list) => DecodedEnvelope {
            oplog: oplog.to_vec(),
            metadata: Some(list),
        },
        Err(_) => legacy(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::Change;

    /// Build a non-trivial `ChangeList` with a couple of changes and a map entry.
    fn sample_changes() -> ChangeList {
        let mut list = ChangeList::default();
        list.changes.push(Change {
            id: "chg-1".to_owned(),
            author_did: "did:alice".to_owned(),
            intent: "first".to_owned(),
            ..Change::default()
        });
        list.changes.push(Change {
            id: "chg-2".to_owned(),
            author_did: "did:bob".to_owned(),
            intent: "second".to_owned(),
            ..Change::default()
        });
        list.author_by_agent
            .insert("agent-1".to_owned(), "did:alice".to_owned());
        list.author_by_agent
            .insert("agent-2".to_owned(), "did:bob".to_owned());
        list
    }

    #[test]
    fn test_envelope_roundtrip() {
        let oplog = b"DMNDTYPS-some-oplog-bytes".to_vec();
        let meta = sample_changes();

        let blob = encode_content_envelope(&oplog, &meta);
        let decoded = decode_content_envelope(&blob);

        assert_eq!(decoded.oplog, oplog, "oplog bytes must round-trip exactly");
        assert_eq!(
            decoded.metadata,
            Some(meta),
            "ChangeList must round-trip equal"
        );
    }

    #[test]
    fn test_legacy_blob_has_no_metadata() {
        // A bare oplog written before this codec existed — no magic prefix.
        let legacy = b"DMNDTYPS....raw diamond-types oplog....".to_vec();
        let decoded = decode_content_envelope(&legacy);

        assert_eq!(decoded.oplog, legacy, "the whole blob is the oplog");
        assert_eq!(decoded.metadata, None, "legacy blobs carry no metadata");
    }

    #[test]
    fn test_decode_is_failsafe_on_corrupt() {
        // (a) too short to even hold a header.
        let short = vec![b'K', b'C', b'E'];
        let decoded = decode_content_envelope(&short);
        assert_eq!(decoded.metadata, None);
        assert_eq!(decoded.oplog, short, "whole 3-byte input is the oplog");

        // (b) magic + a declared length far larger than the remaining bytes.
        let mut overrun = ENVELOPE_MAGIC.to_vec();
        overrun.extend_from_slice(&u32::MAX.to_le_bytes());
        overrun.extend_from_slice(b"only a few bytes follow");
        let overrun_input = overrun.clone();
        let decoded = decode_content_envelope(&overrun);
        assert_eq!(decoded.metadata, None, "overrun length must not panic");
        assert_eq!(
            decoded.oplog, overrun_input,
            "on overrun the whole input is the oplog"
        );

        // (c) magic + a valid length header, but garbage in the ChangeList region.
        let oplog = b"xy".to_vec();
        let mut garbled = ENVELOPE_MAGIC.to_vec();
        let oplog_len = u32::try_from(oplog.len()).unwrap();
        garbled.extend_from_slice(&oplog_len.to_le_bytes());
        garbled.extend_from_slice(&oplog);
        // A proto field tag with an invalid wire type / truncated varint.
        garbled.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff]);
        let decoded = decode_content_envelope(&garbled);
        assert_eq!(
            decoded.metadata, None,
            "undecodable ChangeList region falls back to legacy"
        );
    }

    #[test]
    fn test_decode_never_panics_on_boundary_lengths() {
        // 0-, 3-, and 7-byte blobs (all below HEADER_LEN) must be total.
        for len in [0usize, 3, 7] {
            let blob = vec![0xffu8; len];
            let decoded = decode_content_envelope(&blob);
            assert_eq!(decoded.metadata, None);
            assert_eq!(decoded.oplog, blob);
        }
    }

    #[test]
    fn test_empty_changelist_roundtrips() {
        let oplog = b"oplog".to_vec();
        let meta = ChangeList::default();

        let blob = encode_content_envelope(&oplog, &meta);
        let decoded = decode_content_envelope(&blob);

        assert_eq!(decoded.oplog, oplog);
        assert_eq!(decoded.metadata, Some(ChangeList::default()));
    }
}
