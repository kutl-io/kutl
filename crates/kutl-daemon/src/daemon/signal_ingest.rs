//! Signal-record ingest for [`SpaceWorker`]: the driver
//! edge that turns a broadcast [`SyncEvent::SignalRecord`] into a durable
//! append into the space's local segments plus a catch-up cursor advance,
//! under the store's single-writer `flock`.
//!
//! Verification is ADVISORY: every well-formed record is
//! appended regardless of its signature/attestation tier; the tier is only
//! LOGGED (enforcement is deferred). The one record shape NOT appended is a
//! legacy bare broadcast (empty `record_id`) — not a durable record — which is
//! dropped before any IO, mirroring `SpaceSignalState::apply`'s rule.

use anyhow::{Context, Result};
use kutl_proto::sync::Signal;
use kutl_signals::{
    AttestationVerification, RecordVerification, verify_attestation, verify_record,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::signal_store::DaemonSignalStore;

use super::SpaceWorker;

impl SpaceWorker {
    /// Ingest one broadcast signal record into this space's local segments and
    /// advance the catch-up cursor to its HLC.
    ///
    /// Steps: drop a legacy bare broadcast (empty `record_id`); verify the
    /// record + attestation and log the resulting tier (ADVISORY — the record
    /// is ALWAYS appended, never rejected); append via the lazily-opened
    /// [`DaemonSignalStore`]; then persist the cursor. A duplicate `record_id`
    /// is a no-op — the ingest-side `seen` set dedups on set-union (seeded from
    /// the segments on disk), so a re-append never doubles the record (the
    /// ingest test asserts this).
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be opened, the append fails, the
    /// space id is not a UUID (so no segment directory can be keyed), or the
    /// cursor cannot be persisted. Verification NEVER produces an error — an
    /// invalid signature only downgrades the logged tier.
    pub(super) fn ingest_signal_record(&mut self, record: &Signal) -> Result<()> {
        // A legacy bare broadcast (pre-records sender) carries no `record_id`;
        // it is NOT a durable record. Drop it before any IO, mirroring
        // `SpaceSignalState::apply`'s empty-`record_id` rule.
        if record.record_id.is_empty() {
            debug!(
                signal_id = %record.id,
                "dropping legacy bare signal broadcast (empty record_id)"
            );
            return Ok(());
        }

        self.ensure_signal_store()?;

        // Set-union dedup on `record_id` (mirrors the fold's `seen` set and the
        // relay's re-seed collision check): a `record_id` already persisted is
        // an idempotent no-op — appending it again would bloat the segments and
        // inflate a later re-seed with records the relay already holds. `insert`
        // returns `false` for a known id. Checked BEFORE verify/append so a
        // duplicate broadcast does no work at all.
        if !self.seen_record_ids.insert(record.record_id.clone()) {
            debug!(
                record_id = %record.record_id,
                "duplicate signal record — already persisted, skipping append"
            );
            return Ok(());
        }

        // ADVISORY verification: log the tier, never
        // reject. Computed BEFORE the append so the log reflects what landed.
        self.log_record_tier(record);

        let store = self
            .signal_store
            .as_mut()
            .expect("signal store opened by ensure_signal_store");
        if let Err(e) = store.append(record) {
            // The append failed: roll the id back out of the seen set so a retry
            // (the record re-broadcast) is not mistaken for a duplicate and
            // silently dropped, which would strand it out of the segments.
            self.seen_record_ids.remove(&record.record_id);
            return Err(e).context("appending signal record to local segments");
        }

        // Advance the cursor to this record's HLC after the append lands. A
        // record with no HLC leaves the cursor untouched — nothing to advance
        // to (the fold tolerates it; catch-up simply re-overlaps, idempotent).
        if let Some(ref hlc) = record.hlc {
            let dir = store.dir().to_path_buf();
            kutl_signals::catchup::save_cursor(&dir, hlc)
                .with_context(|| format!("saving signal cursor at {}", dir.display()))?;
        }

        Ok(())
    }

    /// Ensure this space's [`DaemonSignalStore`] is open (taking its
    /// single-writer `flock`) and its `record_id` dedup set is seeded from the
    /// segments already on disk. Idempotent — the store and lock are held for
    /// the worker's lifetime, so this loads the existing records at most once.
    ///
    /// # Errors
    ///
    /// Returns an error if `config.space_id` is not a UUID (no segment
    /// directory can be keyed), the store cannot be opened (lock held by
    /// another writer, or the directory cannot be created), or the existing
    /// segments cannot be loaded to seed the dedup set.
    pub(super) fn ensure_signal_store(&mut self) -> Result<()> {
        if self.signal_store.is_some() {
            return Ok(());
        }
        let space_id = Uuid::parse_str(&self.config.space_id).with_context(|| {
            format!(
                "space id {} is not a uuid; cannot key its signal segments",
                self.config.space_id
            )
        })?;
        let store = DaemonSignalStore::open(&self.config.space_root, space_id)
            .context("opening the space signal store")?;
        // Seed the dedup set from what is already persisted so a restart does
        // not re-append records it already holds (catch-up overlap is
        // idempotent by construction, but re-appending would bloat the
        // segments). One-time O(R) load, amortized over the session.
        let loaded = store
            .load()
            .context("loading existing signal segments to seed the dedup set")?;
        for r in &loaded.records {
            if !r.record_id.is_empty() {
                self.seen_record_ids.insert(r.record_id.clone());
            }
        }
        self.signal_store = Some(store);
        Ok(())
    }

    /// Log the advisory verification tier for `record`,
    /// anchoring both the `MATERIALIZER` signature claim and any
    /// relay attestation against the SAME recorded trust set
    /// ([`kutl_signals::RelayTrust`] over `known_relay_dids`). Never rejects —
    /// the caller appends the record regardless. The six outcomes it logs:
    ///
    /// - `signed-valid`   — the record signature verifies against `actor_did`
    ///   (a `MATERIALIZER` claim additionally requires a recorded relay).
    /// - `relay-attested` — the attestation verifies under a recorded relay
    ///   identity (a trusted countersignature; former keys stay recorded, so
    ///   rotation does not orphan old attestations).
    /// - `asserted`       — unsigned and unattested (tier 3), the expected
    ///   shape for a plain broadcast; logged at debug.
    /// - `unbound-materializer` — the signature verifies but claims
    ///   `MATERIALIZER` for a relay identity not in the recorded set.
    ///   `warn`, still appended.
    /// - `unverifiable`   — a newer signing scheme than this build knows;
    ///   "cannot check", not "forged". Logged at debug.
    /// - `invalid-advisory` — a signature/attestation is present but does not
    ///   verify, or an attestation verifies under an unrecorded relay.
    ///   Logged at `warn` (attention-worthy) but still appended.
    fn log_record_tier(&self, record: &Signal) {
        // Role-aware: relay claims are honoured only
        // from a relay recorded in `known_relays.toml`. Deliberately NOT
        // `pinned_relay_did` — that is whatever the current connection claimed,
        // so trusting it would let the party being checked establish its own
        // pin. The persisted set also carries pre-rotation keys, without which
        // every record signed or attested before a rotation would stop
        // verifying.
        let trust = kutl_signals::RelayTrust::pinned(&self.known_relay_dids);
        let signature = verify_record(record, trust);
        let attestation = verify_attestation(record, trust);

        match (&signature, &attestation) {
            (RecordVerification::SignedValid, _) => {
                debug!(
                    signal_id = %record.id,
                    record_id = %record.record_id,
                    tier = "signed-valid",
                    "ingesting signal record"
                );
            }
            (RecordVerification::Unsigned, AttestationVerification::Valid) => {
                debug!(
                    signal_id = %record.id,
                    record_id = %record.record_id,
                    tier = "relay-attested",
                    "ingesting signal record"
                );
            }
            (RecordVerification::Unsigned, AttestationVerification::None) => {
                debug!(
                    signal_id = %record.id,
                    record_id = %record.record_id,
                    tier = "asserted",
                    "ingesting signal record"
                );
            }
            // A relay key signed this record on some author's behalf, and it is
            // not a relay we pinned. Its OWN tier rather than
            // folding into `invalid-advisory`, because the distinction is the
            // whole point: nothing here is malformed and no key failed, so the
            // record is not "invalid" — it is a well-formed claim of authority
            // we cannot place. Kept at warn: this is precisely the shape a
            // leaked relay key produces, and a client that has simply not seen a
            // rotation yet should also surface it (the rotation entry is what
            // silences it — the recorded set works like SSH's known_hosts).
            //
            // MUST precede the catch-all — see the `Unverifiable` note below for
            // what falling through costs.
            (RecordVerification::UnboundMaterializer { actor_did }, _) => {
                warn!(
                    signal_id = %record.id,
                    record_id = %record.record_id,
                    tier = "unbound-materializer",
                    materializer_did = %actor_did,
                    pinned_relay_did = %self.pinned_relay_did,
                    "ingesting a record signed by an unpinned relay identity (advisory, appended)"
                );
            }
            // Newer signing scheme than this build knows: we cannot rebuild the
            // signing input, so this is "cannot check", not
            // "forged". MUST precede the catch-all below — falling through would
            // log `invalid-advisory` at warn and make a routine scheme bump
            // read as a fleet-wide attack on every client yet to upgrade.
            (
                RecordVerification::Unverifiable {
                    declared,
                    supported,
                },
                _,
            )
            | (
                _,
                AttestationVerification::Unverifiable {
                    declared,
                    supported,
                },
            ) => {
                debug!(
                    signal_id = %record.id,
                    record_id = %record.record_id,
                    tier = "unverifiable",
                    declared,
                    supported,
                    "ingesting signal record signed under a newer scheme (appended, not verified)"
                );
            }
            // A signature or attestation is present but does not verify or does
            // not pin to the adopted relay — attention-worthy, still appended.
            _ => {
                warn!(
                    signal_id = %record.id,
                    record_id = %record.record_id,
                    tier = "invalid-advisory",
                    ?signature,
                    ?attestation,
                    "ingesting signal record with an unverified provenance (advisory, appended)"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use kutl_proto::sync::{Hlc, Signal, SignalEventType, SignatureRole};
    use kutl_signals::segment::SegmentStore;
    use kutl_signals::{did_key_encode, sign_attestation, sign_record};
    use uuid::Uuid;

    use crate::daemon::tests::test_worker;

    /// A UUID space id (the production shape). `test_worker` defaults to
    /// `"test-space"`, which is NOT a UUID and cannot key a segment directory —
    /// ingest tests override it here.
    const TEST_SPACE_UUID: u128 = 0x1234_5678_9abc_def0_1234_5678_9abc_def0;

    /// Build a minimal CREATED record with the given HLC physical time.
    fn record(space: Uuid, sig: &str, rec: &str, ms: u64) -> Signal {
        let mut s = Signal {
            id: sig.into(),
            space_id: space.to_string(),
            record_id: rec.into(),
            author_did: "did:key:zSomeAuthor".into(),
            actor_did: "did:key:zSomeAuthor".into(),
            hlc: Some(Hlc {
                physical_ms: ms,
                logical: 0,
                actor: vec![0u8; 16],
            }),
            ..Default::default()
        };
        s.set_event(SignalEventType::Created);
        s
    }

    /// A worker whose `space_id` is a real UUID so `DaemonSignalStore` can key
    /// its segment directory.
    fn uuid_worker(root: std::path::PathBuf) -> (crate::daemon::SpaceWorker, Uuid) {
        let mut worker = test_worker(root);
        let space = Uuid::from_u128(TEST_SPACE_UUID);
        worker.config.space_id = space.to_string();
        (worker, space)
    }

    /// The per-space segment directory a `DaemonSignalStore` keys for `space`.
    fn space_dir(root: &std::path::Path, space: Uuid) -> std::path::PathBuf {
        root.join(".kutl").join("signals").join(space.to_string())
    }

    /// Ingest appends the record to local segments AND advances the cursor to
    /// its HLC.
    #[test]
    fn test_ingest_appends_record_and_advances_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let (mut worker, space) = uuid_worker(dir.path().to_path_buf());

        worker
            .ingest_signal_record(&record(space, "sig-a", "rec-a", 42))
            .expect("ingest ok");

        let seg_dir = space_dir(dir.path(), space);
        let loaded = SegmentStore::load(&seg_dir).expect("load segments");
        assert_eq!(loaded.records.len(), 1, "the record was appended");
        assert_eq!(loaded.records[0].record_id, "rec-a");

        let cursor = kutl_signals::catchup::load_cursor(&seg_dir)
            .expect("load cursor")
            .expect("cursor was written");
        assert_eq!(cursor.physical_ms, 42, "cursor advanced to the record HLC");
    }

    /// A duplicate `record_id` is a no-op — the segment/fold layer dedups on
    /// set-union, so a re-append does not double the record.
    #[test]
    fn test_ingest_duplicate_record_id_is_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let (mut worker, space) = uuid_worker(dir.path().to_path_buf());

        let rec = record(space, "sig-a", "dup", 7);
        worker.ingest_signal_record(&rec).expect("first ingest ok");
        worker.ingest_signal_record(&rec).expect("second ingest ok");

        let seg_dir = space_dir(dir.path(), space);
        let loaded = SegmentStore::load(&seg_dir).expect("load segments");
        let dups = loaded
            .records
            .iter()
            .filter(|r| r.record_id == "dup")
            .count();
        assert_eq!(dups, 1, "a duplicate record_id folds to one record");
    }

    /// A record with an EMPTY `record_id` (a legacy bare broadcast) is dropped:
    /// nothing is appended and no cursor is written.
    #[test]
    fn test_ingest_empty_record_id_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let (mut worker, space) = uuid_worker(dir.path().to_path_buf());

        worker
            .ingest_signal_record(&record(space, "sig-a", "", 5))
            .expect("ingest ok (dropped)");

        let seg_dir = space_dir(dir.path(), space);
        // The drop happens before any IO, so the store never opens: the segment
        // directory is never created and nothing lands.
        let loaded = SegmentStore::load(&seg_dir).expect("load segments");
        assert_eq!(
            loaded.records.len(),
            0,
            "an empty-record_id broadcast appends nothing"
        );
        assert!(
            kutl_signals::catchup::load_cursor(&seg_dir)
                .expect("load cursor")
                .is_none(),
            "an empty-record_id broadcast writes no cursor"
        );
    }

    /// A record whose attestation `relay_did` matches the pinned relay DID is
    /// STILL appended (advisory), AND so is one with a mismatched pin. This
    /// asserts the invariant "the record ALWAYS lands regardless of tier"; the
    /// tier itself is verified structurally by exercising both the attested and
    /// the mismatched paths through `log_record_tier`.
    #[test]
    fn test_ingest_appends_regardless_of_attestation_pin() {
        let dir = tempfile::tempdir().unwrap();
        let (mut worker, space) = uuid_worker(dir.path().to_path_buf());

        // A relay identity we will pin, and a DIFFERENT one that will not match.
        let relay_key = SigningKey::from_bytes(&[7u8; 32]);
        let relay_did = did_key_encode(&relay_key.verifying_key());
        worker.pinned_relay_did = relay_did.clone();

        // Record 1: attested by the pinned relay → tier "relay-attested".
        let mut attested = record(space, "sig-attested", "rec-att", 10);
        let att = sign_attestation(&attested, &relay_key, &relay_did, 1);
        attested.attestation = Some(att);
        worker
            .ingest_signal_record(&attested)
            .expect("attested ingest ok");

        // Record 2: attested by a DIFFERENT relay → pin mismatch, "invalid-advisory".
        let other_key = SigningKey::from_bytes(&[9u8; 32]);
        let other_did = did_key_encode(&other_key.verifying_key());
        let mut mismatched = record(space, "sig-mismatch", "rec-mis", 11);
        let bad_att = sign_attestation(&mismatched, &other_key, &other_did, 1);
        mismatched.attestation = Some(bad_att);
        worker
            .ingest_signal_record(&mismatched)
            .expect("mismatched ingest ok (advisory, never rejects)");

        // Record 3: a validly SIGNED record → tier "signed-valid".
        let author_key = SigningKey::from_bytes(&[3u8; 32]);
        let author_did = did_key_encode(&author_key.verifying_key());
        let mut signed = record(space, "sig-signed", "rec-signed", 12);
        signed.author_did = author_did.clone();
        signed.actor_did = author_did;
        sign_record(&mut signed, &author_key, SignatureRole::Author);
        worker
            .ingest_signal_record(&signed)
            .expect("signed ingest ok");

        // ALL THREE land regardless of tier — the advisory invariant.
        let seg_dir = space_dir(dir.path(), space);
        let loaded = SegmentStore::load(&seg_dir).expect("load segments");
        assert_eq!(
            loaded.records.len(),
            3,
            "every record is appended regardless of verification tier"
        );
    }

    /// A MATERIALIZER record is honoured when its relay is in the recorded
    /// trust set, and lands as `unbound-materializer` when it is not — and
    /// lands EITHER WAY. The advisory invariant has to survive
    /// this tier, or role-aware verification would silently drop
    /// the records it is meant only to label.
    #[test]
    fn test_ingest_appends_materialized_records_bound_or_not() {
        let dir = tempfile::tempdir().unwrap();
        let (mut worker, space) = uuid_worker(dir.path().to_path_buf());

        let relay_key = SigningKey::from_bytes(&[13u8; 32]);
        let relay_did = did_key_encode(&relay_key.verifying_key());

        // A record the relay signed on a human's behalf — the MATERIALIZER
        // shape, where `author_did` and `actor_did` deliberately differ.
        let materialized = |signal_id: &str, record_id: &str, ms: u64| {
            let mut r = record(space, signal_id, record_id, ms);
            r.author_did = "did:key:zTheHuman".to_owned();
            r.actor_did = relay_did.clone();
            sign_record(&mut r, &relay_key, SignatureRole::Materializer);
            r
        };

        // Trust set empty: the relay is not recorded, so this is unbound.
        assert!(worker.known_relay_dids.is_empty());
        worker
            .ingest_signal_record(&materialized("sig-unbound", "rec-unbound", 20))
            .expect("an unbound materialized record must still be appended");

        // Now record the relay, as a handshake against a known relay would.
        worker.known_relay_dids = vec![relay_did.clone()];
        worker
            .ingest_signal_record(&materialized("sig-bound", "rec-bound", 21))
            .expect("a bound materialized record is appended");

        let seg_dir = space_dir(dir.path(), space);
        let loaded = SegmentStore::load(&seg_dir).expect("load segments");
        assert_eq!(
            loaded.records.len(),
            2,
            "trust affects the logged tier, never whether the record lands"
        );
    }
}
