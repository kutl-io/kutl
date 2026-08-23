//! Gate signal event records at ingest — well-formedness is strict at the
//! boundary. `validate_record` admits a record
//! only when it meets the minimum well-formedness criteria required by the
//! segment writer; the fold in the segment writer is a backstop only.
//!
//! The relay-mint paths (create / transition / backfill) build their records
//! via the SHARED [`kutl_signals::authoring::assemble_record`] the client uses
//! — structural byte-parity — then relay-attest via
//! [`attest_on_ingest`]. There is no relay-only record-assembly function.

use kutl_proto::sync::Signal;
use kutl_signals::record::{
    AttestationVerification, RecordVerification, verify_attestation, verify_record,
};
use kutl_signals::segment::MAX_RECORD_BYTES;
use tracing::{debug, warn};

use crate::identity::RelayIdentity;

/// A record failed the ingest well-formedness gate.
///
/// The segment layer has its own hard size check; this gate fires earlier and
/// gives the relay control over whether to halt the emit.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RecordAdmission {
    /// `record_id` is empty — a legacy broadcast shape, not a durable record.
    #[error("record has no record_id")]
    NoRecordId,
    /// The HLC is missing or its actor field is not exactly 16 bytes.
    #[error("record has a missing or malformed hlc")]
    BadHlc,
    /// The encoded record exceeds the admission cap.
    #[error("record exceeds the {MAX_RECORD_BYTES}-byte admission cap")]
    TooLarge,
    /// A transition's note exceeds the body bound.
    ///
    /// The ONE tightened check that runs on the replicated path as well as the
    /// authored one. It belongs here because a size limit is
    /// well-formedness rather than policy, so applying it to a peer's records is
    /// not a retroactive rejection of previously-legal shapes — and because the
    /// fold parks an orphan transition's note in memory, which the replicated
    /// path can reach, so a bound applied only to authored records would leave
    /// the amplification open exactly where untrusted records arrive.
    #[error("transition note exceeds {max} characters, got {len}")]
    NoteTooLong {
        /// The bound that was exceeded.
        max: usize,
        /// The length supplied.
        len: usize,
    },
    /// The record's `space_id` field does not match the authorized space it is
    /// being ingested into. A
    /// member of space A must not be able to push a record carrying space B's
    /// id: reads filter by `space_id`, so an unbound projection is a
    /// cross-tenant injection vector.
    #[error("record space_id {record_space_id:?} does not match authorized space {authorized:?}")]
    ForeignSpace {
        /// The `space_id` field carried by the record.
        record_space_id: String,
        /// The space the caller is authorized for (the URL path space).
        authorized: String,
    },
}

/// The pure replicated-admission core: well-formedness + space binding.
///
/// Shared by [`admit_replicated_record`], which layers advisory provenance
/// logging on top (the re-seed loop needs per-record reject accounting), and
/// by the replicated append seam itself, which re-runs it so that NO caller
/// can reach the log through that seam without the one check that
/// distinguishes it — the cross-tenant space binding. Idempotent and cheap,
/// so the double-run on the re-seed path costs a validate + one compare.
pub(crate) fn check_replicated(
    record: &Signal,
    authorized_space: &str,
) -> Result<(), RecordAdmission> {
    validate_record(record)?;
    if record.space_id != authorized_space {
        return Err(RecordAdmission::ForeignSpace {
            record_space_id: record.space_id.clone(),
            authorized: authorized_space.to_owned(),
        });
    }
    Ok(())
}

/// Admission for records ingested ON BEHALF OF OTHER AUTHORS (re-seed /
/// replication). Integrity rests on space-binding + the caller's
/// `record_id`-collision defense + the tier-1 author signature — NOT on
/// `actor == caller`.
///
/// A re-seeding daemon pushes back records authored by EVERY member it caught
/// up from the relay ("clients are the source of truth"), so
/// binding `actor_did` to the pushing caller here would silently drop every
/// foreign-authored record — permanent multi-member data loss on recovery.
///
/// Runs, in order:
/// 1. [`validate_record`] — well-formedness (`record_id`, HLC, size);
/// 2. the space-binding check — the record's `space_id` MUST equal the
///    `authorized_space` the caller is authorized for (rejects the cross-space
///    injection: a member of space A pushing a record stamped with space B);
/// 3. advisory signature + attestation verification — `verify_record` /
///    `verify_attestation` are CALLED and their tier LOGGED, but a bad
///    signature does NOT reject (the tiers are advisory; if verification ever
///    becomes enforcing, this is the one seam that flips to rejecting).
///
/// Returns `Err(RecordAdmission)` only for a well-formedness or space-binding
/// failure; a signature that fails to verify still returns `Ok(())`. There is
/// deliberately NO actor-binding check here.
pub(crate) fn admit_replicated_record(
    record: &Signal,
    authorized_space: &str,
    trust: kutl_signals::RelayTrust<'_>,
) -> Result<(), RecordAdmission> {
    check_replicated(record, authorized_space)?;
    // Advisory tier logging — never rejects (the seam that would flip if
    // verification ever becomes enforcing).
    match verify_record(record, trust) {
        RecordVerification::SignedValid => {
            debug!(record_id = %record.record_id, signal_id = %record.id, "ingest: record signature valid (tier-1)");
        }
        RecordVerification::SignedInvalid(reason) => {
            // Advisory only: a bad signature is logged but not rejected.
            // This is the one seam that changes if enforcement ever lands.
            warn!(record_id = %record.record_id, signal_id = %record.id, %reason, "ingest: record signature INVALID (advisory — not rejecting)");
        }
        RecordVerification::Unsigned => {
            debug!(record_id = %record.record_id, signal_id = %record.id, "ingest: record unsigned (tier-3 asserted)");
        }
        // Deliberately debug, not warn: an unrecognized scheme version means we
        // cannot rebuild the signing input, which is not evidence of forgery.
        // Warning here would make a future scheme bump look
        // like a fleet-wide attack on every relay that had not upgraded yet.
        // If this seam ever flips to enforcing, this arm must reject
        // alongside `SignedInvalid` — unknown is not permission.
        RecordVerification::Unverifiable {
            declared,
            supported,
        } => {
            debug!(record_id = %record.record_id, signal_id = %record.id, declared, supported, "ingest: record signing scheme newer than this build (unverifiable, not rejected)");
        }
        // A record materialized under a relay identity that is not this
        // relay's. On a single-relay pool that is the leaked-key shape,
        // so it warns; it is still admitted, because this seam stays advisory
        // and a client re-seeding history it legitimately caught up from a
        // predecessor relay would otherwise lose it.
        RecordVerification::UnboundMaterializer { actor_did } => {
            warn!(record_id = %record.record_id, signal_id = %record.id, materializer_did = %actor_did, "ingest: record signed by an unrecognized relay identity (advisory — not rejecting)");
        }
    }
    match verify_attestation(record, trust) {
        AttestationVerification::Valid => {
            debug!(record_id = %record.record_id, signal_id = %record.id, "ingest: relay attestation valid (tier-2)");
        }
        // Same class as `UnboundMaterializer` above: nothing failed to
        // verify — this is a well-formed claim of authority from a relay
        // identity we have not recorded. Advisory warn; still admitted.
        AttestationVerification::UntrustedRelay { relay_did } => {
            warn!(record_id = %record.record_id, signal_id = %record.id, %relay_did, "ingest: relay attestation from an unrecorded relay identity (advisory — not rejecting)");
        }
        AttestationVerification::Invalid(reason) => {
            warn!(record_id = %record.record_id, signal_id = %record.id, %reason, "ingest: relay attestation INVALID (advisory — not rejecting)");
        }
        AttestationVerification::None => {}
        // Same reasoning as the record arm above: debug, and fail closed when
        // this seam ever gates admission.
        AttestationVerification::Unverifiable {
            declared,
            supported,
        } => {
            debug!(record_id = %record.record_id, signal_id = %record.id, declared, supported, "ingest: attestation scheme newer than this build (unverifiable, not rejected)");
        }
    }
    Ok(())
}

/// Validate a record at the ingest boundary.
///
/// Returns `Ok(())` iff the record is admissible for segment append.
/// Rejects records that are missing a `record_id`, have a missing or
/// malformed HLC, or exceed `MAX_RECORD_BYTES` in encoded form.
pub(crate) fn validate_record(record: &Signal) -> Result<(), RecordAdmission> {
    if record.record_id.is_empty() {
        return Err(RecordAdmission::NoRecordId);
    }
    match record.hlc.as_ref() {
        Some(h) if h.actor.len() == 16 => {}
        _ => return Err(RecordAdmission::BadHlc),
    }
    // `encoded_len` is O(fields) with no heap allocation.
    if prost::Message::encoded_len(record) > MAX_RECORD_BYTES {
        return Err(RecordAdmission::TooLarge);
    }
    // The transition note, bounded on BOTH admission paths — see
    // `RecordAdmission::NoteTooLong`. The 16 MiB record cap above is a different
    // rule and far too loose to stop the fold parking a large note per orphan.
    if let Some(kutl_proto::sync::signal::Payload::Transition(t)) = record.payload.as_ref() {
        let len = t.note.chars().count();
        if len > kutl_signals::authoring::MAX_BODY_CHARS {
            return Err(RecordAdmission::NoteTooLong {
                max: kutl_signals::authoring::MAX_BODY_CHARS,
                len,
            });
        }
    }
    Ok(())
}

/// Attach a relay attestation (an `observed_at` anchor) to a signal record on
/// ingest, WITHOUT disturbing the author signature (the `signature` and
/// `attestation` slots are independent). The
/// server-observed timestamp is the only anchor a future enforcer can bound a
/// backdated client HLC against, and it cannot be recovered for records already
/// written — so it is attached at ingest.
///
/// The relay-mint create/transition/backfill paths (which assemble via
/// [`kutl_signals::authoring::assemble_record`] and then attest) and the
/// re-seed path (via [`attest_reseeded_record`], which preserves this relay's
/// own prior anchor and never re-stamps a legacy member tier-1 signature)
/// share this one attestation step.
///
/// No-op when the relay has no signing identity (tier-3 stays tier-3). It
/// touches ONLY the attestation slot: an author signature over the record's
/// canonical bytes is unaffected (canonical bytes exclude both
/// signature-bearing fields).
///
/// TRUST TIER: a relay-mint record's `actor_did`
/// is a **relay-ASSERTED** authorship claim carrying **NO member signature** —
/// the empty `signature` slot means "who wrote it" is relay-vouched, not
/// member-proven, and its ONLY cryptographic proof is this attestation (tier-2,
/// when an identity is present). Contrast legacy tier-1 records (client-signed
/// under the retired edge-signing scheme, still ingested via re-seed), where
/// the member's own signature proves authorship directly.
pub(crate) fn attest_on_ingest(
    record: &mut Signal,
    relay_identity: Option<&RelayIdentity>,
    observed_at_ms: i64,
) {
    if let Some(id) = relay_identity {
        let att =
            kutl_signals::sign_attestation(record, id.signing_key(), id.did(), observed_at_ms);
        record.attestation = Some(att);
    }
}

/// True iff `record` already carries THIS relay's OWN valid attestation — its
/// attestation verifies under a trust set containing exactly
/// `relay_identity`'s own DID.
///
/// An attestation that answers `Valid` against that one-DID pin could only
/// have been produced by this relay's own signing key — so this is a sound
/// "is this my own past anchor?" check. A foreign, forged, invalid, or
/// absent attestation returns `false`.
pub(crate) fn is_own_valid_attestation(record: &Signal, relay_identity: &RelayIdentity) -> bool {
    if record.attestation.is_none() {
        return false;
    }
    let own = [relay_identity.did().to_owned()];
    matches!(
        verify_attestation(record, kutl_signals::RelayTrust::pinned(&own)),
        AttestationVerification::Valid
    )
}

/// Attest a RE-SEEDED record on ingest, PRESERVING this relay's own past
/// `observed_at` anchor through recovery.
///
/// If `record` already carries THIS relay's own valid attestation (see
/// [`is_own_valid_attestation`]) it is left untouched — that attestation is our
/// own anchor from the record's first ingest, and re-minting it at recovery
/// time would destroy the durable anchor the backdated-HLC defense bounds a
/// client HLC against. Any OTHER attestation (foreign, forged, invalid, or
/// absent) is re-minted fresh via [`attest_on_ingest`], so a forger cannot
/// pre-fill a bogus anchor and have it preserved.
///
/// Re-seed ONLY: the direct-authoring routes always [`attest_on_ingest`] (the
/// client never supplies a legitimate attestation there). The broader question
/// of which PEER relays' attestations to trust on re-seed is deliberately
/// unresolved while verification stays advisory.
pub(crate) fn attest_reseeded_record(
    record: &mut Signal,
    relay_identity: Option<&RelayIdentity>,
    observed_at_ms: i64,
) {
    let preserve_own = relay_identity.is_some_and(|id| is_own_valid_attestation(record, id));
    if !preserve_own {
        attest_on_ingest(record, relay_identity, observed_at_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kutl_proto::sync::{Hlc, SignatureRole};
    use tempfile::TempDir;

    /// A canonical 16-byte actor for records built directly in tests.
    const ACTOR_16: [u8; 16] = [0u8; 16];

    fn minimal_record() -> Signal {
        Signal {
            record_id: uuid::Uuid::new_v4().to_string(),
            hlc: Some(Hlc {
                physical_ms: 1,
                logical: 0,
                actor: ACTOR_16.to_vec(),
            }),
            ..Default::default()
        }
    }

    // -----------------------------------------------------------------------
    // relay-mint HLC ordering (records assembled via `assemble_record`)
    // -----------------------------------------------------------------------

    /// A CREATED intent for the strict-order test. Payload-less on purpose — the
    /// test is about HLC ordering, not shape — which is why it goes through
    /// `rematerialize` rather than a per-kind builder.
    fn created_intent() -> kutl_signals::authoring::SignalIntent {
        kutl_signals::authoring::SignalIntent::rematerialize(
            kutl_signals::authoring::RecordEnvelope {
                space_id: "5171e0a1-1111-4000-8000-000000000004".into(),
                document_id: None,
                signal_id: "sig".into(),
                timestamp: 0,
            },
            None,
        )
    }

    /// Two records assembled from ONE monotonic clock at the SAME wall-ms are
    /// strictly HLC-ordered (the second dominates the first via the logical
    /// counter), so the fold never falls back to a random `record_id` tiebreak.
    /// This pre-empts close→reopen ordering flakiness in the same millisecond —
    /// the relay-mint paths ([`stamp_and_append_created`] /
    /// [`build_and_append_transition`]) tick this same shared clock and pass the
    /// HLC into `assemble_record`.
    #[test]
    fn test_relay_mint_strict_order_within_same_ms() {
        /// Both stamps use the same wall-ms; ordering must come from the clock.
        const SAME_MS: u64 = 1_000;

        // Emulate the relay's single monotonic clock across two assemblies.
        let mut clock = kutl_core::HlcClock::new(kutl_core::ActorId(uuid::Uuid::from_u128(7)));
        let intent = created_intent();
        let first = kutl_signals::authoring::assemble_record(
            &intent,
            "did:k:z",
            clock.tick(SAME_MS).into(),
        );
        let second = kutl_signals::authoring::assemble_record(
            &intent,
            "did:k:z",
            clock.tick(SAME_MS).into(),
        );

        let h1 = first.hlc.expect("first hlc");
        let h2 = second.hlc.expect("second hlc");
        assert_eq!(h1.physical_ms, SAME_MS);
        assert_eq!(h2.physical_ms, SAME_MS);
        // Strict order: same physical_ms, second's logical strictly greater.
        assert!(
            (h1.physical_ms, h1.logical) < (h2.physical_ms, h2.logical),
            "second record must strictly dominate the first: {h1:?} vs {h2:?}"
        );
        assert!(h2.logical > h1.logical, "logical counter must advance");
    }

    // -----------------------------------------------------------------------
    // attest_on_ingest — attach the relay `observed_at` anchor without
    // disturbing an author signature (shared by client + relay-mint paths)
    // -----------------------------------------------------------------------

    /// With a relay identity, `attest_on_ingest` attaches a tier-2 attestation
    /// naming the relay's own DID and does NOT set an author signature/role —
    /// the attestation slot is orthogonal to the `sig_role`/`signature` fields.
    /// This is the relay-mint attestation branch (client author signature absent).
    #[test]
    fn test_attest_on_ingest_relay_mint_tier2() {
        let dir = TempDir::new().unwrap();
        let id = crate::identity::RelayIdentity::load_or_generate(dir.path()).unwrap();

        let mut r = minimal_record();
        r.actor_did = "did:key:zActor".into();
        attest_on_ingest(&mut r, Some(&id), 100);

        let att = r
            .attestation
            .as_ref()
            .expect("attestation must be present with identity");
        assert_eq!(att.relay_did, id.did(), "attestation names the relay DID");
        assert!(!att.signature.is_empty());
        // sig_role/signature stay at proto defaults — the relay attestation is
        // orthogonal to the author-signature fields.
        assert_eq!(r.sig_role(), SignatureRole::Unspecified);
        assert!(r.signature.is_empty());
    }

    /// `attest_on_ingest` attaches a valid relay attestation carrying the
    /// server-observed timestamp WITHOUT disturbing the client's tier-1 author
    /// signature — the two signature slots are independent.
    #[test]
    fn test_attest_on_ingest_adds_attestation_preserving_author_signature() {
        /// The server-observed ingest time this test pins on the attestation.
        const OBSERVED_AT_MS: i64 = 1234;

        // A client-signed record: sign with the actor's own key so
        // `verify_record` is `SignedValid` before ingest.
        let author_key = ed25519_dalek::SigningKey::from_bytes(&[3u8; 32]);
        let mut r = minimal_record();
        r.actor_did = kutl_signals::did_key_encode(&author_key.verifying_key());
        kutl_signals::sign_record(&mut r, &author_key, SignatureRole::Author);
        assert!(
            matches!(
                verify_record(&r, kutl_signals::RelayTrust::none()),
                RecordVerification::SignedValid
            ),
            "precondition: the record must be tier-1 SignedValid before ingest"
        );
        let author_sig_before = r.signature.clone();

        // Attest on ingest with a real relay identity.
        let dir = TempDir::new().unwrap();
        let id = crate::identity::RelayIdentity::load_or_generate(dir.path()).unwrap();
        attest_on_ingest(&mut r, Some(&id), OBSERVED_AT_MS);

        // The attestation is present, carries the observed_at anchor, and verifies.
        let att = r
            .attestation
            .as_ref()
            .expect("attestation must be attached on ingest");
        assert_eq!(att.observed_at_ms, OBSERVED_AT_MS);
        let own = [id.did().to_owned()];
        assert!(matches!(
            verify_attestation(&r, kutl_signals::RelayTrust::pinned(&own)),
            AttestationVerification::Valid
        ));

        // INDEPENDENCE INVARIANT: the author signature is byte-unchanged and
        // still verifies — attesting must never disturb the tier-1 signature.
        assert_eq!(
            r.signature, author_sig_before,
            "the author signature must be byte-unchanged by attestation"
        );
        assert!(
            matches!(
                verify_record(&r, kutl_signals::RelayTrust::none()),
                RecordVerification::SignedValid
            ),
            "the author signature must still verify after attestation"
        );
    }

    /// With no relay identity, `attest_on_ingest` is a no-op — tier-3 records
    /// stay tier-3 (no attestation attached).
    #[test]
    fn test_attest_on_ingest_no_identity_is_noop() {
        let mut r = minimal_record();
        attest_on_ingest(&mut r, None, 1);
        assert!(
            r.attestation.is_none(),
            "no identity means no attestation (tier-3 stays tier-3)"
        );
    }

    // -----------------------------------------------------------------------
    // validate_record
    // -----------------------------------------------------------------------

    /// A fully-stamped record passes validation.
    #[test]
    fn test_validate_admissible_record_ok() {
        let r = minimal_record();
        assert!(validate_record(&r).is_ok());
    }

    /// Empty `record_id` is rejected with `NoRecordId`.
    #[test]
    fn test_validate_rejects_empty_record_id() {
        let mut r = minimal_record();
        r.record_id = String::new();
        assert!(matches!(
            validate_record(&r),
            Err(RecordAdmission::NoRecordId)
        ));
    }

    /// Missing HLC is rejected with `BadHlc`.
    #[test]
    fn test_validate_rejects_missing_hlc() {
        let mut r = minimal_record();
        r.hlc = None;
        assert!(matches!(validate_record(&r), Err(RecordAdmission::BadHlc)));
    }

    /// An HLC with a 15-byte actor is rejected with `BadHlc`.
    #[test]
    fn test_validate_rejects_bad_actor_len() {
        let mut r = minimal_record();
        r.hlc = Some(Hlc {
            physical_ms: 1,
            logical: 0,
            actor: vec![0u8; 15],
        });
        assert!(matches!(validate_record(&r), Err(RecordAdmission::BadHlc)));
    }

    // -----------------------------------------------------------------------
    // admit_replicated_record — re-seed / replication (NO actor binding)
    // -----------------------------------------------------------------------

    /// A FOREIGN-authored record (its `actor_did` is some other member, not the
    /// pushing caller) is admitted for replication — `admit_replicated_record`
    /// deliberately does NOT actor-bind, because a re-seeding daemon relays
    /// records authored by every member it caught up.
    #[test]
    fn test_admit_replicated_record_admits_foreign_author() {
        let mut r = minimal_record();
        r.space_id = "5171e0a1-1111-4000-8000-000000000004".into();
        r.actor_did = "did:key:zFOREIGN".into();

        // Replication gate: foreign author admitted (space-binding satisfied).
        assert!(
            admit_replicated_record(&r, &r.space_id.clone(), kutl_signals::RelayTrust::none())
                .is_ok(),
            "a foreign-authored record must be admitted for replication"
        );
    }

    /// The replication gate STILL enforces space-binding — a record stamped with
    /// a foreign `space_id` is rejected `ForeignSpace` even on the re-seed path
    /// (the cross-tenant injection defense is orthogonal to actor-binding).
    #[test]
    fn test_admit_replicated_record_still_space_binds() {
        let mut r = minimal_record();
        r.space_id = "spOTHER".into();
        r.actor_did = "did:key:zFOREIGN".into();
        let err = admit_replicated_record(&r, "spAUTHORIZED", kutl_signals::RelayTrust::none())
            .expect_err("a foreign space_id must be rejected even on replication");
        assert!(
            matches!(&err, RecordAdmission::ForeignSpace { .. }),
            "expected ForeignSpace, got: {err:?}"
        );
    }

    /// A record whose encoded size exceeds `MAX_RECORD_BYTES` is rejected with `TooLarge`.
    #[test]
    fn test_validate_rejects_oversized_record() {
        let mut r = minimal_record();
        // Stuff the actor_did field to push encoded size over the cap.
        r.actor_did = "x".repeat(MAX_RECORD_BYTES + 1);
        assert!(matches!(
            validate_record(&r),
            Err(RecordAdmission::TooLarge)
        ));
    }

    /// The cap is INCLUSIVE: a record whose encoded size is EXACTLY
    /// `MAX_RECORD_BYTES` is admitted (`>`, not `>=`). Pins the boundary so a
    /// one-byte drift in the comparison can't silently reject a max-size record.
    #[test]
    fn test_validate_accepts_record_at_exact_cap() {
        let mut r = minimal_record();
        // Binary-search the actor_did length to hit the exact encoded size
        // (encoded_len is monotonic in the field length, +1 per byte in this
        // range — no varint boundary near 16 MiB).
        let (mut lo, mut hi) = (0usize, MAX_RECORD_BYTES);
        while lo < hi {
            let mid = usize::midpoint(lo, hi);
            r.actor_did = "x".repeat(mid);
            if prost::Message::encoded_len(&r) < MAX_RECORD_BYTES {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        r.actor_did = "x".repeat(lo);
        assert_eq!(
            prost::Message::encoded_len(&r),
            MAX_RECORD_BYTES,
            "test setup: could not size the record to exactly the cap"
        );
        assert!(
            validate_record(&r).is_ok(),
            "a record of exactly MAX_RECORD_BYTES must be admitted (inclusive cap)"
        );
    }
}
