//! Record discipline: canonical bytes, Ed25519 signing, advisory
//! verification.
//!
//! Canonical form = the record `Signal` with `signature` and `attestation`
//! cleared, prost-encoded. Canonical bytes always carry the full DID so
//! storage-side encodings can never perturb verification.

use ed25519_dalek::{SIGNATURE_LENGTH, Signature, Signer, SigningKey, VerifyingKey};
use kutl_proto::sync::{RelayAttestation, Signal, SignatureRole};
use prost::Message;

use crate::error::{Error, Result};

/// Multicodec prefix for an Ed25519 public key in a did:key (0xed 0x01).
const ED25519_MULTICODEC_PREFIX: [u8; 2] = [0xed, 0x01];
/// Length of an Ed25519 public key in bytes.
const ED25519_PUBKEY_LEN: usize = 32;
/// Upper bound on the base58 payload of an Ed25519 did:key (~46 chars
/// for multicodec + 32-byte key; margin for future multicodecs). Bounds
/// the O(n·m) bs58 decode against megabyte-DID CPU exhaustion.
const MAX_DID_KEY_B58_LEN: usize = 64;

/// The canonical signing input: the record with both signature-bearing
/// fields cleared, prost-encoded (deterministic tag-order for non-map
/// messages; the golden-bytes test locks this against prost upgrades).
///
/// `sig_role` stays IN the signed bytes: the role is part of the signed
/// claim, so a valid signature cannot be re-labeled AUTHOR↔MATERIALIZER
/// after the fact.
///
/// Callers that hold `&Signal` pay a full clone when the record is already
/// signed (signature/attestation are set). `sign_record` avoids this clone
/// by operating on `&mut Signal` directly via `canonical_bytes_in_place`.
fn canonical_bytes(record: &Signal) -> Vec<u8> {
    if record.signature.is_empty() && record.attestation.is_none() {
        // Fast path: record is already in canonical form — no clone needed.
        return record.encode_to_vec();
    }
    let mut c = record.clone();
    c.signature = Vec::new();
    c.attestation = None;
    c.encode_to_vec()
}

/// Encode `record` as canonical bytes using a mutable borrow — clears
/// `signature` and `attestation` in place, encodes, then restores them.
/// Called by `sign_record` which already has `&mut Signal`, avoiding the
/// full-clone path in `canonical_bytes`.
fn canonical_bytes_in_place(record: &mut Signal) -> Vec<u8> {
    let saved_sig = std::mem::take(&mut record.signature);
    let saved_att = record.attestation.take();
    let encoded = record.encode_to_vec();
    record.signature = saved_sig;
    record.attestation = saved_att;
    encoded
}

/// Domain separator for the AUTHOR record signing input — prevents
/// cross-protocol signature reuse (the same key signs auth nonces) and versions
/// the scheme so it can evolve over immutable records.
const RECORD_DOMAIN: &[u8] = b"kutl-signal-v1";

/// Author signing input: `RECORD_DOMAIN || u64_le(len(canonical)) || canonical`.
/// Mirrors `attestation_signing_input` (minus the attestation bytes) so both
/// signature slots share one shape.
fn record_signing_input(canonical: &[u8]) -> Vec<u8> {
    let len = u64::try_from(canonical.len()).expect("canonical length cannot exceed u64::MAX");
    let mut input = Vec::with_capacity(RECORD_DOMAIN.len() + size_of::<u64>() + canonical.len());
    input.extend_from_slice(RECORD_DOMAIN);
    input.extend_from_slice(&len.to_le_bytes());
    input.extend_from_slice(canonical);
    input
}

/// Sign `record` in place with `key`, claiming `role`. Sets `sig_role`
/// before computing canonical bytes (the role is part of the signed claim).
///
/// Caller contract: `key` must be the key pair behind `record.actor_did` —
/// `verify_record` checks the signature against the actor's DID.
pub fn sign_record(record: &mut Signal, key: &SigningKey, role: SignatureRole) {
    record.set_sig_role(role);
    // Use the in-place path: we hold &mut Signal so no clone is needed.
    let bytes = canonical_bytes_in_place(record);
    let input = record_signing_input(&bytes);
    record.signature = key.sign(&input).to_bytes().to_vec();
}

/// The relay identities a verifier will honour a `MATERIALIZER` claim from.
///
/// Exists because a `MATERIALIZER` signature is the one role that speaks *for
/// someone else*: `finish_record` sets `actor_did` to the relay's DID while
/// leaving `author_did` as the person the record is attributed to. So a leaked
/// relay key mints records carrying an arbitrary `author_did` that verify
/// cryptographically — which, without this list, [`verify_record`] would answer
/// `SignedValid` to, with no way for the caller to say "but not from *that*
/// relay". `AUTHOR` and `AGENT` signatures need no such list: their `actor_did`
/// IS the signer, so they are self-binding.
///
/// The SAME set anchors relay attestations ([`verify_attestation`]): an
/// attestation is a relay-role claim exactly like a `MATERIALIZER` signature,
/// and a verdict that did not check membership would let any self-signed
/// attestation read as vouched-for.
///
/// Borrows its list so neither the daemon's per-record ingest nor the relay's
/// admission path clones a pin set to check one signature.
#[derive(Debug, Clone, Copy, Default)]
pub struct RelayTrust<'a> {
    relays: &'a [String],
}

impl<'a> RelayTrust<'a> {
    /// Trust no relay's claims.
    ///
    /// The honest setting for a verifier with no pin — a client that has not
    /// completed a handshake, or a test asserting the unbound path. It is NOT a
    /// degraded mode to be avoided: "I do not know which relay to trust" must
    /// answer [`RecordVerification::UnboundMaterializer`] /
    /// [`AttestationVerification::UntrustedRelay`], never a trust verdict, or
    /// the role binding buys nothing.
    #[must_use]
    pub fn none() -> Self {
        Self { relays: &[] }
    }

    /// Trust relay claims from any of `relays`.
    ///
    /// A slice rather than a single DID because rotation is a recorded event,
    /// not an alarm: a client that has seen a relay rotate keeps both
    /// entries, and records signed OR attested under either key stay
    /// verifiable.
    #[must_use]
    pub fn pinned(relays: &'a [String]) -> Self {
        Self { relays }
    }

    /// Whether `did` is one of the trusted relay identities — the single
    /// membership predicate for both the materializer-role check and the
    /// attestation check.
    ///
    /// The empty-`did` refusal is a backstop, not the primary defense: both
    /// callers run only after did:key parsing (which rejects an empty DID)
    /// has succeeded, so an empty `did` cannot normally reach here. It stays
    /// because it costs one comparison and keeps the predicate safe under a
    /// future caller that has not parsed the DID first.
    fn honours(self, did: &str) -> bool {
        !did.is_empty() && self.relays.iter().any(|r| r == did)
    }
}

/// Advisory verification outcome (the three provenance tiers — signed,
/// relay-attested, asserted; membership — "signed by a space member" — is the
/// caller's judgment, layered above).
#[derive(Debug)]
pub enum RecordVerification {
    /// Signature verifies against the `actor_did` key.
    SignedValid,
    /// Signature present but does not verify (or the DID is unparseable).
    SignedInvalid(String),
    /// No signature — tier-3 asserted record. Never an error.
    Unsigned,
    /// The record declares a `canonical_version` this build does not know, so
    /// its signing input cannot be reconstructed.
    ///
    /// Distinct from [`Self::SignedInvalid`] on purpose: "I cannot check this"
    /// is not "this is forged". Conflating them would make any future scheme
    /// bump read as fleet-wide forgery on every unupgraded client, so this
    /// variant logs at debug rather than warn. Where verification ever gates
    /// admission, it MUST fail closed — unknown is not permission.
    Unverifiable {
        /// The version the record declared.
        declared: u32,
        /// The newest version this build can reconstruct.
        supported: u32,
    },
    /// The signature verifies, but it claims `MATERIALIZER` on behalf of a
    /// relay identity this verifier does not trust.
    ///
    /// Distinct from [`Self::SignedInvalid`] because the cryptography is
    /// sound — nothing is malformed and no key failed. What failed is the
    /// *authority*: someone holding a relay key signed a record attributed to
    /// an author who never touched it, and this verifier has not pinned that
    /// relay. Also distinct from [`Self::Unsigned`], which would understate it:
    /// a record in this state is not merely unattested, it carries a claim that
    /// did not check out.
    ///
    /// The verdict a verifier with no pin at all gets for every materialized
    /// record. That is deliberate — see [`RelayTrust::none`].
    UnboundMaterializer {
        /// The relay DID the record's `MATERIALIZER` signature was made under.
        actor_did: String,
    },
}

/// The newest canonical-signing scheme version this build can reconstruct.
///
/// `0` is the original scheme (what an absent `canonical_version` decodes to,
/// covering every record written before the field existed): whole canonical
/// bytes under `kutl-signal-v1` /
/// `kutl-attest-v1`. Bump this only in the same change that teaches
/// `canonical_bytes` (private to this module) a second construction.
pub const SUPPORTED_CANONICAL_VERSION: u32 = 0;

/// Verify a record's signature against its own `actor_did`, **and** bind a
/// `MATERIALIZER` claim to a relay the caller trusts.
///
/// Two questions, in order, because they fail differently:
///
/// 1. *Did this key sign these bytes?* — the signature check against
///    `actor_did`. A failure here is [`RecordVerification::SignedInvalid`].
/// 2. *Was this key entitled to make this claim?* — for `MATERIALIZER` only,
///    since it is the sole role that signs on another author's behalf.
///    A failure here is [`RecordVerification::UnboundMaterializer`].
///
/// The second question is the one a leaked relay key exploits: without it, any
/// relay's key mints records that every peer logs at the top tier,
/// carrying whatever `author_did` the holder chose. `trust` is what closes it —
/// pass [`RelayTrust::none`] when the caller genuinely has no pin, and
/// materialized records will read as unbound rather than as verified.
///
/// The check is ordered signature-first so an unbound verdict always means the
/// bytes really were signed by the DID they name. A caller escalating on
/// `UnboundMaterializer` is then reacting to a real relay key, not to noise.
pub fn verify_record(record: &Signal, trust: RelayTrust<'_>) -> RecordVerification {
    // Version gate first: an unknown scheme means the signing input cannot be
    // reconstructed, so neither "valid" nor "invalid" is an honest answer.
    if record.canonical_version > SUPPORTED_CANONICAL_VERSION {
        return RecordVerification::Unverifiable {
            declared: record.canonical_version,
            supported: SUPPORTED_CANONICAL_VERSION,
        };
    }
    if record.signature.is_empty() {
        return RecordVerification::Unsigned;
    }
    let key = match did_key_verifying_key(&record.actor_did) {
        Ok(k) => k,
        Err(e) => return RecordVerification::SignedInvalid(e.to_string()),
    };
    let sig_bytes: [u8; SIGNATURE_LENGTH] = match record.signature.as_slice().try_into() {
        Ok(b) => b,
        Err(_) => {
            return RecordVerification::SignedInvalid(format!(
                "signature is not {SIGNATURE_LENGTH} bytes"
            ));
        }
    };
    let sig = Signature::from_bytes(&sig_bytes);
    // verify_strict rejects small-order/mixed-order keys and small-order R.
    // No signed record predates this check, so enforcement landing later
    // can never flip a historical verdict.
    let input = record_signing_input(&canonical_bytes(record));
    match key.verify_strict(&input, &sig) {
        Ok(()) => {
            // Role binding. Only MATERIALIZER needs it: AUTHOR and AGENT sign
            // as themselves, so the signature check above already proved the
            // signer is who the record says acted.
            if record.sig_role() == SignatureRole::Materializer && !trust.honours(&record.actor_did)
            {
                return RecordVerification::UnboundMaterializer {
                    actor_did: record.actor_did.clone(),
                };
            }
            RecordVerification::SignedValid
        }
        Err(e) => RecordVerification::SignedInvalid(e.to_string()),
    }
}

/// Encode a verifying key as a did:key string (multicodec 0xed01 +
/// base58btc, z-prefixed). The canonical did:key encoder — `kutl-client`,
/// `kutl-relay`, and `kutlhub-relay` delegate here.
pub fn did_key_encode(key: &VerifyingKey) -> String {
    let mut bytes = Vec::with_capacity(2 + ED25519_PUBKEY_LEN);
    bytes.extend_from_slice(&ED25519_MULTICODEC_PREFIX);
    bytes.extend_from_slice(key.as_bytes());
    format!("did:key:z{}", bs58::encode(bytes).into_string())
}

/// Decode a did:key string into an Ed25519 verifying key.
pub fn did_key_verifying_key(did: &str) -> Result<VerifyingKey> {
    let encoded = did
        .strip_prefix("did:key:z")
        .ok_or_else(|| Error::InvalidDidKey {
            reason: "missing did:key:z prefix".into(),
        })?;
    if encoded.len() > MAX_DID_KEY_B58_LEN {
        return Err(Error::InvalidDidKey {
            reason: "did:key payload too long".into(),
        });
    }
    let bytes = bs58::decode(encoded)
        .into_vec()
        .map_err(|e| Error::InvalidDidKey {
            reason: e.to_string(),
        })?;
    let key_bytes = bytes
        .strip_prefix(ED25519_MULTICODEC_PREFIX.as_slice())
        .ok_or_else(|| Error::InvalidDidKey {
            reason: "not an ed25519 multicodec".into(),
        })?;
    let arr: [u8; ED25519_PUBKEY_LEN] = key_bytes.try_into().map_err(|_| Error::InvalidDidKey {
        reason: "key is not 32 bytes".into(),
    })?;
    VerifyingKey::from_bytes(&arr).map_err(|e| Error::InvalidDidKey {
        reason: e.to_string(),
    })
}

/// Domain separator for the relay-attestation signing input — prevents
/// cross-protocol signature reuse.
const ATTESTATION_DOMAIN: &[u8] = b"kutl-attest-v1";

/// Advisory attestation-verification outcome (the middle provenance tier):
/// a relay countersignature over a record it observed.
#[derive(Debug)]
pub enum AttestationVerification {
    /// The attestation verifies AND its `relay_did` is one of the verifier's
    /// pinned relay identities ([`RelayTrust`]) — a trust verdict, not merely
    /// internal consistency. Rotation is covered by the pin SET: a record
    /// attested under a relay's former key stays `Valid` as long as that key
    /// remains in the verifier's recorded set (`known_relays` keeps rotated
    /// entries).
    Valid,
    /// The attestation is internally consistent — its signature verifies
    /// against the key its own `relay_did` names — but that DID is not one
    /// the verifier trusts. Any key can self-attest, so without membership
    /// this proves only self-consistency; it must never be treated as
    /// vouched-for.
    UntrustedRelay {
        /// The self-claimed relay DID the attestation verified under.
        relay_did: String,
    },
    /// An attestation is present but does not verify (or its DID is
    /// unparseable) — the relay claim is not trustworthy.
    Invalid(String),
    /// No attestation present — tier-3 asserted, never an error.
    None,
    /// The record declares a `canonical_version` this build does not know, so
    /// the attestation's signing input cannot be reconstructed.
    /// Distinct from [`Self::Invalid`]: unknown is not forged. Logs at debug,
    /// and fails closed wherever verification gates admission.
    Unverifiable {
        /// The version the record declared.
        declared: u32,
        /// The newest version this build can reconstruct.
        supported: u32,
    },
}

/// Builds the attestation signing input: domain separator, a u64-LE length
/// prefix on the record's canonical bytes, the canonical bytes, then the
/// attestation encoded with its `signature` field cleared (so `relay_did`
/// and `observed_at_ms` are covered).
fn attestation_signing_input(record: &Signal, attestation: &RelayAttestation) -> Vec<u8> {
    let canonical = canonical_bytes(record);
    let mut cleared = attestation.clone();
    cleared.signature = Vec::new();
    let att_bytes = cleared.encode_to_vec();
    let canonical_len =
        u64::try_from(canonical.len()).expect("canonical bytes length cannot exceed u64::MAX");
    let mut input = Vec::with_capacity(
        ATTESTATION_DOMAIN.len() + size_of::<u64>() + canonical.len() + att_bytes.len(),
    );
    input.extend_from_slice(ATTESTATION_DOMAIN);
    input.extend_from_slice(&canonical_len.to_le_bytes());
    input.extend_from_slice(&canonical);
    input.extend_from_slice(&att_bytes);
    input
}

/// Countersign `record` as `relay_did`, observed at `observed_at_ms`.
///
/// Returns the built [`RelayAttestation`]; the caller sets it on the record.
/// The signing input is: `ATTESTATION_DOMAIN || u64_le(len(canonical)) ||
/// canonical_bytes(record) || encode(attestation with signature cleared)`.
/// This pins both the record identity and the relay metadata (`relay_did` +
/// `observed_at_ms`) to the signature.
pub fn sign_attestation(
    record: &Signal,
    relay_key: &SigningKey,
    relay_did: &str,
    observed_at_ms: i64,
) -> RelayAttestation {
    let mut att = RelayAttestation {
        relay_did: relay_did.to_owned(),
        observed_at_ms,
        signature: Vec::new(),
    };
    let input = attestation_signing_input(record, &att);
    att.signature = relay_key.sign(&input).to_bytes().to_vec();
    att
}

/// Verify a record's optional relay attestation against its own `relay_did`.
///
/// Returns [`AttestationVerification::None`] when no attestation is present
/// (tier-3 asserted — not an error); [`AttestationVerification::Valid`] when
/// the signature verifies AND the attestation's `relay_did` is in `trust`;
/// [`AttestationVerification::UntrustedRelay`] when it verifies under a DID
/// the verifier has not pinned (self-consistency is not trust — any key can
/// self-attest); and [`AttestationVerification::Invalid`] when the signature
/// or DID fails outright.
pub fn verify_attestation(record: &Signal, trust: RelayTrust) -> AttestationVerification {
    // Version gate first, for the same reason as `verify_record`: an unknown
    // scheme means the signing input cannot be rebuilt, so no verdict about
    // the signature itself would be honest.
    if record.canonical_version > SUPPORTED_CANONICAL_VERSION {
        return AttestationVerification::Unverifiable {
            declared: record.canonical_version,
            supported: SUPPORTED_CANONICAL_VERSION,
        };
    }
    let Some(att) = record.attestation.as_ref() else {
        return AttestationVerification::None;
    };
    let key = match did_key_verifying_key(&att.relay_did) {
        Ok(k) => k,
        Err(e) => return AttestationVerification::Invalid(e.to_string()),
    };
    let sig_bytes: [u8; SIGNATURE_LENGTH] = match att.signature.as_slice().try_into() {
        Ok(b) => b,
        Err(_) => {
            return AttestationVerification::Invalid(format!(
                "signature is not {SIGNATURE_LENGTH} bytes"
            ));
        }
    };
    let sig = Signature::from_bytes(&sig_bytes);
    match key.verify_strict(&attestation_signing_input(record, att), &sig) {
        Ok(()) if trust.honours(&att.relay_did) => AttestationVerification::Valid,
        Ok(()) => AttestationVerification::UntrustedRelay {
            relay_did: att.relay_did.clone(),
        },
        Err(e) => AttestationVerification::Invalid(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A record declaring a newer scheme than this build supports verifies as
    /// `Unverifiable`, NOT `SignedInvalid` — "cannot check" is not "forged".
    /// The signature here is deliberately valid for the
    /// current scheme, proving the version gate fires FIRST rather than the
    /// verdict falling out of a failed signature check.
    #[test]
    fn test_verify_record_newer_scheme_is_unverifiable_not_invalid() {
        let key = SigningKey::from_bytes(&[3u8; 32]);
        let mut r = fixed_record();
        r.actor_did = did_key_encode(&key.verifying_key());
        sign_record(&mut r, &key, SignatureRole::Author);
        assert!(matches!(
            verify_record(&r, RelayTrust::none()),
            RecordVerification::SignedValid
        ));

        r.canonical_version = SUPPORTED_CANONICAL_VERSION + 1;
        match verify_record(&r, RelayTrust::none()) {
            RecordVerification::Unverifiable {
                declared,
                supported,
            } => {
                assert_eq!(declared, SUPPORTED_CANONICAL_VERSION + 1);
                assert_eq!(supported, SUPPORTED_CANONICAL_VERSION);
            }
            other => panic!("expected Unverifiable, got {other:?}"),
        }
    }

    /// The same gate guards the attestation slot, and takes precedence over
    /// the absent-attestation case so an unknown scheme never reads as `None`.
    #[test]
    fn test_verify_attestation_newer_scheme_is_unverifiable() {
        let mut r = fixed_record();
        r.canonical_version = SUPPORTED_CANONICAL_VERSION + 1;
        assert!(matches!(
            verify_attestation(&r, RelayTrust::none()),
            AttestationVerification::Unverifiable { .. }
        ));
    }

    /// A record at the supported version is unaffected by the gate: absent
    /// (0) is the original scheme, so every record written before the field
    /// existed still verifies exactly as before.
    #[test]
    fn test_verify_record_absent_version_is_the_original_scheme() {
        let key = SigningKey::from_bytes(&[5u8; 32]);
        let mut r = fixed_record();
        r.actor_did = did_key_encode(&key.verifying_key());
        sign_record(&mut r, &key, SignatureRole::Author);
        assert_eq!(r.canonical_version, 0);
        assert!(matches!(
            verify_record(&r, RelayTrust::none()),
            RecordVerification::SignedValid
        ));
    }
    use ed25519_dalek::SigningKey;
    use kutl_proto::sync::{
        AudienceType, CloseReason, FlagKind, FlagPayload, Hlc, Signal, SignalEventType,
        SignatureRole, signal,
    };

    fn fixed_record() -> Signal {
        let mut s = Signal {
            id: "5e0f7bb2-6db5-4a3e-9c60-111111111111".into(),
            space_id: "065ae81c-c74c-429c-af66-163df73991b4".into(),
            author_did: "did:key:zAlice".into(),
            timestamp: 1_752_400_000_000,
            record_id: "9d0e6c7a-2222-4a3e-9c60-222222222222".into(),
            actor_did: "did:key:zAlice".into(),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// Every canonical-relevant field populated: optional scalars, the
    /// nested `Hlc` message, the `FlagPayload` oneof arm, and all enums
    /// non-default — so nested-message and oneof encodings are pinned too.
    fn full_record() -> Signal {
        // The deprecated audience pair is deliberate: this golden pins the
        // canonical bytes a record using the legacy audience fields encodes
        // to, so it must keep
        // encoding the legacy shape. `audience` stays unset for the same reason.
        #[allow(deprecated)]
        let mut flag = FlagPayload {
            target_did: Some("did:key:zBob".into()),
            message: "needs review".into(),
            ..Default::default()
        };
        flag.set_kind(FlagKind::Question);
        flag.set_audience_type(AudienceType::Space);
        let mut s = Signal {
            id: "5e0f7bb2-6db5-4a3e-9c60-333333333333".into(),
            space_id: "065ae81c-c74c-429c-af66-163df73991b4".into(),
            document_id: Some("7c1d2e3f-4444-4a3e-9c60-444444444444".into()),
            author_did: "did:key:zAlice".into(),
            timestamp: 1_752_400_000_000,
            record_id: "9d0e6c7a-5555-4a3e-9c60-555555555555".into(),
            hlc: Some(Hlc {
                physical_ms: 1_752_400_000_123,
                logical: 7,
                actor: vec![0xab; 16],
            }),
            actor_did: "did:key:zAlice".into(),
            payload: Some(signal::Payload::Flag(flag)),
            ..Default::default()
        };
        s.set_event(SignalEventType::Closed);
        s.set_close_reason(CloseReason::Resolved);
        s.set_sig_role(SignatureRole::Author);
        s
    }

    fn test_key() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }

    /// Renders canonical bytes as lowercase hex for golden comparison.
    fn canonical_hex(record: &Signal) -> String {
        canonical_bytes(record)
            .iter()
            .fold(String::new(), |mut acc, b| {
                use std::fmt::Write as _;
                let _ = write!(acc, "{b:02x}");
                acc
            })
    }

    /// Canonical bytes exclude signature + attestation: signing then
    /// recomputing canonical bytes yields the pre-signing encoding
    /// (role set before the snapshot — it is part of the signed claim).
    #[test]
    fn test_canonical_bytes_exclude_signature_fields() {
        let mut r = fixed_record();
        r.set_sig_role(SignatureRole::Author);
        let before = canonical_bytes(&r);
        sign_record(&mut r, &test_key(), SignatureRole::Author);
        assert!(!r.signature.is_empty());
        assert_eq!(
            canonical_bytes(&r),
            before,
            "signature/attestation must not perturb canonical bytes"
        );
    }

    /// GOLDEN BYTES — locks the canonical encoding across prost upgrades.
    /// Bootstrap: run once with the empty const, paste the printed hex,
    /// re-run green. A failure after a dependency bump means the canonical
    /// encoding rotated — that is a release blocker, not a test to update
    /// casually.
    #[test]
    fn test_canonical_bytes_golden() {
        const GOLDEN_HEX: &str = "0a2435653066376262322d366462352d346133652d396336302d313131313131313131313131122430363561653831632d633734632d343239632d616636362d313633646637333939316234220e6469643a6b65793a7a416c6963652880e8b7998033722439643065366337612d323232322d346133652d396336302d32323232323232323232323278018a010e6469643a6b65793a7a416c696365";
        let hex = canonical_hex(&fixed_record());
        if GOLDEN_HEX.is_empty() {
            println!("GOLDEN_HEX candidate: {hex}");
            panic!("golden constant not yet pinned — paste the printed hex");
        }
        assert_eq!(hex, GOLDEN_HEX);
    }

    /// GOLDEN BYTES, fully-populated fixture — pins the nested-message
    /// (`Hlc`), oneof (`FlagPayload`), optional-scalar, and non-default-enum
    /// encodings that the minimal golden cannot exercise. Same bootstrap
    /// and release-blocker discipline as `test_canonical_bytes_golden`.
    #[test]
    fn test_canonical_bytes_golden_full() {
        const GOLDEN_HEX: &str = "0a2435653066376262322d366462352d346133652d396336302d333333333333333333333333122430363561653831632d633734632d343239632d616636362d3136336466373339393162341a2437633164326533662d343434342d346133652d396336302d343434343434343434343434220e6469643a6b65793a7a416c6963652880e8b79980335220080410021a0c6469643a6b65793a7a426f62220c6e6565647320726576696577722439643065366337612d353535352d346133652d396336302d353535353535353535353535780282011b08fbe8b799803310071a10abababababababababababababababab8a010e6469643a6b65793a7a416c696365900101980101";
        let hex = canonical_hex(&full_record());
        if GOLDEN_HEX.is_empty() {
            println!("GOLDEN_HEX (full) candidate: {hex}");
            panic!("golden constant not yet pinned — paste the printed hex");
        }
        assert_eq!(hex, GOLDEN_HEX);
    }

    /// The author signature is domain-separated: a signature made
    /// over RAW canonical bytes (the old, un-domained input) must NOT verify —
    /// otherwise the scheme could not evolve over immutable records and record
    /// bytes would share a signing input with raw WS-auth nonces.
    #[test]
    fn test_author_signature_is_domain_separated() {
        let key = test_key();
        let mut r = fixed_record();
        r.actor_did = did_key_encode(&key.verifying_key());
        r.set_sig_role(SignatureRole::Author);
        // Sign the RAW canonical bytes the OLD way (no domain separator).
        let raw = canonical_bytes(&r);
        r.signature = key.sign(&raw).to_bytes().to_vec();
        assert!(
            matches!(
                verify_record(&r, RelayTrust::none()),
                RecordVerification::SignedInvalid(_)
            ),
            "a raw-canonical-bytes signature must not verify once the author sig is domain-separated"
        );
    }

    /// Sign → verify round-trips as `SignedValid` for the actor's real key.
    #[test]
    fn test_sign_verify_roundtrip() {
        let key = test_key();
        let mut r = fixed_record();
        r.actor_did = did_key_encode(&key.verifying_key());
        sign_record(&mut r, &key, SignatureRole::Author);
        assert!(matches!(
            verify_record(&r, RelayTrust::none()),
            RecordVerification::SignedValid
        ));
    }

    /// A tampered payload flips verification to `SignedInvalid`.
    #[test]
    fn test_verify_detects_tamper() {
        let key = test_key();
        let mut r = fixed_record();
        r.actor_did = did_key_encode(&key.verifying_key());
        sign_record(&mut r, &key, SignatureRole::Author);
        r.timestamp += 1;
        assert!(matches!(
            verify_record(&r, RelayTrust::none()),
            RecordVerification::SignedInvalid(_)
        ));
    }

    /// Flipping the signed role after signing invalidates the signature —
    /// `sig_role` is part of the signed claim.
    #[test]
    fn test_verify_detects_role_tamper() {
        let key = test_key();
        let mut r = fixed_record();
        r.actor_did = did_key_encode(&key.verifying_key());
        sign_record(&mut r, &key, SignatureRole::Author);
        r.set_sig_role(SignatureRole::Materializer);
        assert!(matches!(
            verify_record(&r, RelayTrust::none()),
            RecordVerification::SignedInvalid(_)
        ));
    }

    /// Key binding — the central claim: a signature made with key A does
    /// not verify when the record claims key B's DID as its actor.
    #[test]
    fn test_verify_detects_wrong_key_reattribution() {
        let key_a = test_key();
        let key_b = SigningKey::from_bytes(&[9u8; 32]);
        let mut r = fixed_record();
        r.actor_did = did_key_encode(&key_a.verifying_key());
        sign_record(&mut r, &key_a, SignatureRole::Author);
        r.actor_did = did_key_encode(&key_b.verifying_key());
        assert!(matches!(
            verify_record(&r, RelayTrust::none()),
            RecordVerification::SignedInvalid(_)
        ));
    }

    /// Empty signature is tier-3 Unsigned, never an error.
    #[test]
    fn test_verify_unsigned_record() {
        assert!(matches!(
            verify_record(&fixed_record(), RelayTrust::none()),
            RecordVerification::Unsigned
        ));
    }

    /// A signature of the wrong length is `SignedInvalid`, never a panic
    /// (and never mistaken for tier-3 Unsigned).
    #[test]
    fn test_verify_rejects_bad_signature_length() {
        let key = test_key();
        let mut r = fixed_record();
        r.actor_did = did_key_encode(&key.verifying_key());
        sign_record(&mut r, &key, SignatureRole::Author);
        for len in [SIGNATURE_LENGTH - 1, SIGNATURE_LENGTH + 1] {
            let mut bad = r.clone();
            bad.signature = vec![0; len];
            assert!(
                matches!(
                    verify_record(&bad, RelayTrust::none()),
                    RecordVerification::SignedInvalid(_)
                ),
                "a {len}-byte signature must be SignedInvalid"
            );
        }
    }

    /// A MATERIALIZER record signed by a relay whose DID is in the trust set
    /// verifies; the SAME record read without that pin does not (a leaked
    /// relay key must not mint pool-wide-valid
    /// history).
    ///
    /// Both directions in one test on purpose: asserting only the rejection
    /// would pass just as well if `verify_record` had been broken outright.
    #[test]
    fn test_materializer_signature_binds_to_the_trusted_relay() {
        let relay_key = SigningKey::from_bytes(&[7u8; 32]);
        let relay_did = did_key_encode(&relay_key.verifying_key());
        let mut r = fixed_record();
        // The shape that makes the role dangerous: the relay signs, but the
        // record stays attributed to a human who never touched this key.
        r.author_did = "did:key:zTheHumanWhoNeverSignedThis".to_owned();
        r.actor_did = relay_did.clone();
        sign_record(&mut r, &relay_key, SignatureRole::Materializer);

        let pinned = [relay_did.clone()];
        assert!(
            matches!(
                verify_record(&r, RelayTrust::pinned(&pinned)),
                RecordVerification::SignedValid
            ),
            "a pinned relay's MATERIALIZER signature must still verify"
        );

        let other = ["did:key:zSomeOtherRelay".to_owned()];
        for (trust, label) in [
            (RelayTrust::none(), "no pin at all"),
            (RelayTrust::pinned(&other), "a different relay pinned"),
        ] {
            match verify_record(&r, trust) {
                RecordVerification::UnboundMaterializer { actor_did } => {
                    assert_eq!(actor_did, relay_did, "the verdict names the signing relay");
                }
                other => panic!("{label}: expected UnboundMaterializer, got {other:?}"),
            }
        }
    }

    /// The binding applies to MATERIALIZER only. AUTHOR and AGENT sign as
    /// themselves — `actor_did` IS the signer — so an empty trust set must not
    /// downgrade them, which would break every client-authored record.
    #[test]
    fn test_self_binding_roles_are_unaffected_by_the_trust_set() {
        for role in [SignatureRole::Author, SignatureRole::Agent] {
            let key = SigningKey::from_bytes(&[9u8; 32]);
            let mut r = fixed_record();
            r.actor_did = did_key_encode(&key.verifying_key());
            sign_record(&mut r, &key, role);
            assert!(
                matches!(
                    verify_record(&r, RelayTrust::none()),
                    RecordVerification::SignedValid
                ),
                "{role:?} must verify with no relay pinned"
            );
        }
    }

    /// An empty pinned DID matches nothing. Guards the daemon's pre-handshake
    /// state, where the pin is `String::new()` — a naive equality check would
    /// bind a record whose `actor_did` was also empty.
    #[test]
    fn test_empty_pin_honours_nothing() {
        let relay_key = SigningKey::from_bytes(&[11u8; 32]);
        let mut r = fixed_record();
        r.actor_did = did_key_encode(&relay_key.verifying_key());
        sign_record(&mut r, &relay_key, SignatureRole::Materializer);

        let empty_pin = [String::new()];
        assert!(
            matches!(
                verify_record(&r, RelayTrust::pinned(&empty_pin)),
                RecordVerification::UnboundMaterializer { .. }
            ),
            "an empty pin entry must never honour a materializer claim"
        );
    }

    /// Malformed did:key inputs are all rejected with `InvalidDidKey`.
    #[test]
    fn test_did_key_verifying_key_rejects_malformed() {
        let short_key = format!(
            "did:key:z{}",
            bs58::encode([ED25519_MULTICODEC_PREFIX.as_slice(), &[0u8; 31]].concat()).into_string()
        );
        let long_key = format!(
            "did:key:z{}",
            bs58::encode([ED25519_MULTICODEC_PREFIX.as_slice(), &[0u8; 33]].concat()).into_string()
        );
        let non_b58 = format!("did:key:z{}", "0OIl".repeat(11));
        let oversized = format!("did:key:z{}", "1".repeat(MAX_DID_KEY_B58_LEN + 1));
        let cases: [(&str, &str); 6] = [
            ("", "empty string"),
            ("did:key:zAlice", "junk payload"),
            (&short_key, "31-byte key"),
            (&long_key, "33-byte key"),
            (&non_b58, "non-base58 characters"),
            (&oversized, "over-length payload"),
        ];
        for (did, label) in cases {
            assert!(
                matches!(did_key_verifying_key(did), Err(Error::InvalidDidKey { .. })),
                "{label} must be rejected: {did:?}"
            );
        }
    }

    /// A record attested under a relay's FORMER key stays Valid as long as
    /// that key remains in the verifier's pinned set — rotation is a
    /// recorded event, not an alarm.
    #[test]
    fn test_attestation_under_former_key_stays_valid_with_rotated_pin_set() {
        let old_key = SigningKey::from_bytes(&[7u8; 32]);
        let old_did = did_key_encode(&old_key.verifying_key());
        let new_key = SigningKey::from_bytes(&[8u8; 32]);
        let new_did = did_key_encode(&new_key.verifying_key());

        let mut r = fixed_record();
        let att = sign_attestation(&r, &old_key, &old_did, 1_752_400_000_123);
        r.attestation = Some(att);

        // The client saw the rotation: both DIDs are in the recorded set.
        let both = [old_did.clone(), new_did.clone()];
        assert!(matches!(
            verify_attestation(&r, RelayTrust::pinned(&both)),
            AttestationVerification::Valid
        ));

        // A verifier that only ever knew the NEW key has no basis to trust
        // the old attestation.
        let only_new = [new_did];
        assert!(matches!(
            verify_attestation(&r, RelayTrust::pinned(&only_new)),
            AttestationVerification::UntrustedRelay { .. }
        ));
    }

    /// did:key round-trip matches the kutl-client encoding convention.
    #[test]
    fn test_did_key_roundtrip() {
        let key = test_key();
        let did = did_key_encode(&key.verifying_key());
        assert!(did.starts_with("did:key:z"));
        let back = did_key_verifying_key(&did).unwrap();
        assert_eq!(back.to_bytes(), key.verifying_key().to_bytes());
    }

    /// A relay attestation over a record's canonical bytes verifies with the
    /// relay's key WHEN that key is pinned, and rejects tampering with
    /// `relay_did`/`observed_at_ms`.
    #[test]
    fn test_attestation_sign_verify_roundtrip() {
        let relay_key = SigningKey::from_bytes(&[9u8; 32]);
        let relay_did = did_key_encode(&relay_key.verifying_key());
        let pin = [relay_did.clone()];
        let mut r = fixed_record();
        let att = sign_attestation(&r, &relay_key, &relay_did, 1_752_400_000_123);
        r.attestation = Some(att);
        assert!(matches!(
            verify_attestation(&r, RelayTrust::pinned(&pin)),
            AttestationVerification::Valid
        ));

        // The SAME attestation without the pin is self-consistency, not
        // trust: any key can self-attest, so it must never read Valid.
        assert!(matches!(
            verify_attestation(&r, RelayTrust::none()),
            AttestationVerification::UntrustedRelay { .. }
        ));

        // Tamper with observed_at_ms after signing -> Invalid.
        if let Some(a) = r.attestation.as_mut() {
            a.observed_at_ms += 1;
        }
        assert!(matches!(
            verify_attestation(&r, RelayTrust::pinned(&pin)),
            AttestationVerification::Invalid(_)
        ));
    }

    /// No attestation is the tier-3 (asserted) case — never an error.
    #[test]
    fn test_verify_attestation_absent_is_none() {
        assert!(matches!(
            verify_attestation(&fixed_record(), RelayTrust::none()),
            AttestationVerification::None
        ));
    }

    /// The domain separator + length prefix bind the record to its
    /// attestation: swapping the attested record's canonical bytes fails.
    #[test]
    fn test_attestation_binds_to_record() {
        let relay_key = SigningKey::from_bytes(&[9u8; 32]);
        let relay_did = did_key_encode(&relay_key.verifying_key());
        let pin = [relay_did.clone()];
        let mut r = fixed_record();
        r.attestation = Some(sign_attestation(&r, &relay_key, &relay_did, 42));
        r.timestamp += 1; // perturb the record's canonical bytes
        assert!(matches!(
            verify_attestation(&r, RelayTrust::pinned(&pin)),
            AttestationVerification::Invalid(_)
        ));
    }
}
