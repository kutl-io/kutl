//! Opaque tokens naming the document version a caller read.
//!
//! A caller receives one from a read and hands it back on a write; nothing
//! outside this module interprets it. That opacity is deliberate — the payload
//! carries the engine's frontier representation, which has no business being
//! visible to callers, and keeping it private means the encoding can change
//! without breaking anyone who only ever echoed it.
//!
//! The token binds to its document. Frontier indices from one document are
//! usually valid on another, so a caller that mixes up two open documents
//! would otherwise check out a plausible wrong base and merge against it with
//! no indication anything was amiss.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};

/// Encoding marker. Present so the payload can change later while tokens
/// minted by an older relay still parse.
const PREFIX: &str = "kv1.";

/// Bytes of document fingerprint carried in the payload. Four is enough to
/// catch a caller confusing two documents, which is what this guards; it is
/// not a security boundary.
const FINGERPRINT_LEN: usize = 4;

/// Bytes of base-content digest carried in the payload.
const DIGEST_LEN: usize = 4;

/// Bits of value packed into each LEB128 byte.
const VARINT_PAYLOAD_BITS: u32 = 7;

/// Mask isolating a LEB128 byte's payload bits.
const VARINT_PAYLOAD_MASK: u8 = 0x7f;

/// Bit marking that a LEB128 byte is followed by another.
const VARINT_CONTINUATION_BIT: u8 = 0x80;

/// Why a token could not be used.
#[derive(Debug, PartialEq, Eq)]
pub enum TokenError {
    /// Not a token this relay could have minted.
    Malformed,
    /// A valid token, but for a different document.
    WrongDocument,
}

fn short_hash(parts: &[&str], out: usize) -> Vec<u8> {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p.as_bytes());
        h.update([0u8]);
    }
    h.finalize()[..out].to_vec()
}

/// Encode `n` as LEB128.
fn put_varint(buf: &mut Vec<u8>, mut n: usize) {
    loop {
        let byte = u8::try_from(n & usize::from(VARINT_PAYLOAD_MASK)).expect("masked to 7 bits");
        n >>= VARINT_PAYLOAD_BITS;
        if n == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | VARINT_CONTINUATION_BIT);
    }
}

/// Decode one LEB128 value, advancing `pos`. `None` on truncation or overflow.
///
/// A plain `<<` silently drops bits shifted past bit 63, so a byte whose
/// payload bits would land there is checked by shifting the result back down
/// and comparing to the input; any mismatch means data was lost and the
/// value is rejected rather than returned truncated.
fn take_varint(bytes: &[u8], pos: &mut usize) -> Option<usize> {
    let mut out: usize = 0;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*pos)?;
        *pos += 1;
        let payload = usize::from(byte & VARINT_PAYLOAD_MASK);
        let shifted = payload.checked_shl(shift)?;
        if shifted >> shift != payload {
            return None;
        }
        out |= shifted;
        if byte & VARINT_CONTINUATION_BIT == 0 {
            return Some(out);
        }
        shift += VARINT_PAYLOAD_BITS;
        if shift >= usize::BITS {
            return None;
        }
    }
}

/// Mint the token describing `content` at `frontier` in this document.
pub fn mint(space_id: &str, document_id: &str, content: &str, frontier: &[usize]) -> String {
    let mut payload = short_hash(&[space_id, document_id], FINGERPRINT_LEN);
    payload.extend_from_slice(&short_hash(&[content], DIGEST_LEN));
    for v in frontier {
        put_varint(&mut payload, *v);
    }
    format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(&payload))
}

fn decode(token: &str) -> Option<Vec<u8>> {
    let body = token.strip_prefix(PREFIX)?;
    let bytes = URL_SAFE_NO_PAD.decode(body).ok()?;
    (bytes.len() >= FINGERPRINT_LEN + DIGEST_LEN).then_some(bytes)
}

/// The base-content digest a token carries, for [`verify_base`].
pub(crate) fn base_digest(token: &str) -> Option<[u8; DIGEST_LEN]> {
    let bytes = decode(token)?;
    bytes[FINGERPRINT_LEN..FINGERPRINT_LEN + DIGEST_LEN]
        .try_into()
        .ok()
}

/// Whether `base_content` is what the token was minted over.
///
/// Frontier indices are local to a relay's oplog. The same indices need not
/// mean the same thing elsewhere, so this turns a token that travelled between
/// relays from a silent wrong-base merge into a refusal.
pub(crate) fn verify_base(token_digest: [u8; DIGEST_LEN], base_content: &str) -> bool {
    short_hash(&[base_content], DIGEST_LEN) == token_digest
}

/// Recover the frontier a token names, rejecting one minted for a different
/// document.
pub fn parse(token: &str, space_id: &str, document_id: &str) -> Result<Vec<usize>, TokenError> {
    let bytes = decode(token).ok_or(TokenError::Malformed)?;
    if bytes[..FINGERPRINT_LEN] != short_hash(&[space_id, document_id], FINGERPRINT_LEN)[..] {
        return Err(TokenError::WrongDocument);
    }
    let mut frontier = Vec::new();
    let mut pos = FINGERPRINT_LEN + DIGEST_LEN;
    while pos < bytes.len() {
        frontier.push(take_varint(&bytes, &mut pos).ok_or(TokenError::Malformed)?);
    }
    Ok(frontier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mint_round_trips_the_frontier() {
        let t = mint("space-a", "doc-1", "hello", &[3, 7]);
        assert_eq!(parse(&t, "space-a", "doc-1").expect("valid"), vec![3, 7]);
    }

    #[test]
    fn test_mint_round_trips_an_empty_frontier() {
        // A document nobody has written yet. The common first-write case,
        // and the one an off-by-one in the varint decoder would break.
        let t = mint("space-a", "doc-1", "", &[]);
        assert_eq!(
            parse(&t, "space-a", "doc-1").expect("valid"),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn test_parse_rejects_a_token_for_another_document() {
        // Frontier indices from one document are very likely valid on
        // another, so without this the caller merges against a plausible
        // wrong base and never learns.
        let t = mint("space-a", "doc-1", "hello", &[3]);
        assert!(matches!(
            parse(&t, "space-a", "doc-2"),
            Err(TokenError::WrongDocument)
        ));
    }

    #[test]
    fn test_parse_rejects_garbage() {
        for bad in ["", "hello", "kv1.", "kv1.!!!!", "kv2.AAAA"] {
            assert!(
                matches!(parse(bad, "space-a", "doc-1"), Err(TokenError::Malformed)),
                "expected {bad:?} to be malformed"
            );
        }
    }

    #[test]
    fn test_verify_base_detects_different_content() {
        let t = mint("space-a", "doc-1", "hello", &[3]);
        let digest = base_digest(&t).expect("valid token");
        assert!(verify_base(digest, "hello"));
        assert!(!verify_base(digest, "goodbye"));
    }

    #[test]
    fn test_parse_rejects_a_frontier_varint_that_overflows_usize() {
        // 10 bytes where the final byte's payload only has one bit of room
        // left (shift 63) but carries all 7 — a decoder that shifts without
        // checking for lost bits would return a truncated value instead of
        // failing.
        let mut bytes = short_hash(&["space-a", "doc-1"], FINGERPRINT_LEN);
        bytes.extend_from_slice(&[0u8; DIGEST_LEN]);
        bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        let token = format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(&bytes));
        assert!(matches!(
            parse(&token, "space-a", "doc-1"),
            Err(TokenError::Malformed)
        ));
    }

    #[test]
    fn test_parse_rejects_a_frontier_varint_truncated_mid_value() {
        // A varint whose last byte still sets the continuation bit: the value
        // continues past the end of the payload. A decoder that ran out of
        // bytes and returned what it had so far would hand back a frontier
        // position the caller never named, and the merge would take its base
        // from there.
        let mut bytes = short_hash(&["space-a", "doc-1"], FINGERPRINT_LEN);
        bytes.extend_from_slice(&[0u8; DIGEST_LEN]);
        bytes.extend_from_slice(&[0x81, 0x82]);
        let token = format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(&bytes));
        assert!(matches!(
            parse(&token, "space-a", "doc-1"),
            Err(TokenError::Malformed)
        ));
    }

    #[test]
    fn test_mint_round_trips_a_frontier_value_near_usize_max() {
        let t = mint("space-a", "doc-1", "hello", &[usize::MAX]);
        assert_eq!(
            parse(&t, "space-a", "doc-1").expect("valid"),
            vec![usize::MAX]
        );
    }
}
