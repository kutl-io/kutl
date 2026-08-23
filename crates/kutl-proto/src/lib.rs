//! kutl-proto — generated protobuf types for the kutl sync protocol.

pub mod protocol;
pub mod vocab;

pub mod sync {
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/kutl.sync.v1.rs"));
}

#[cfg(test)]
mod tests {
    use crate::sync::{
        AudienceType, CloseReason, FlagKind, FlagPayload, Hlc, RelayAttestation, Signal,
        SignalEventType, SignatureRole, signal,
    };
    use prost::Message;

    /// The record-envelope fields round-trip through prost encoding and
    /// default correctly for legacy (unset) senders.
    #[test]
    fn test_signal_record_fields_roundtrip() {
        let mut s = Signal {
            id: "ab12".into(),
            space_id: "94b97b29-5a98-4a13-81e0-f766d259cd39".into(),
            record_id: "7f3e".into(),
            actor_did: "did:key:zExample".into(),
            hlc: Some(Hlc {
                physical_ms: 42,
                logical: 7,
                actor: vec![0u8; 16],
            }),
            signature: vec![1, 2, 3],
            attestation: Some(RelayAttestation {
                relay_did: "did:key:zRelay".into(),
                observed_at_ms: 99,
                signature: vec![9],
            }),
            ..Default::default()
        };
        s.set_event(SignalEventType::Closed);
        s.set_sig_role(SignatureRole::Author);
        s.set_close_reason(CloseReason::Resolved);
        let bytes = s.encode_to_vec();
        let back = Signal::decode(bytes.as_slice()).unwrap();
        assert_eq!(back.event(), SignalEventType::Closed);
        assert_eq!(back.sig_role(), SignatureRole::Author);
        assert_eq!(back.close_reason(), CloseReason::Resolved);
        assert_eq!(back.record_id, "7f3e");
        assert_eq!(back.signature, vec![1, 2, 3]);
        let hlc = back.hlc.expect("hlc should survive decode");
        assert_eq!(hlc.physical_ms, 42);
        assert_eq!(hlc.logical, 7);
        assert_eq!(hlc.actor, vec![0u8; 16]);
        let att = back.attestation.expect("attestation should survive decode");
        assert_eq!(att.relay_did, "did:key:zRelay");
        assert_eq!(att.observed_at_ms, 99);
        assert_eq!(att.signature, vec![9]);

        // Legacy wire shape: an old sender sets only fields 1-5 + a payload;
        // proto3 omits defaults on the wire, so these bytes are exactly what
        // a pre-records sender emits. Record fields must decode as unset.
        //
        // The deprecated audience pair is the whole point of this fixture — it
        // is what a legacy sender emits — so the allow is permanent here.
        #[allow(deprecated)]
        let legacy = Signal {
            id: "old-1".into(),
            space_id: "94b97b29-5a98-4a13-81e0-f766d259cd39".into(),
            document_id: Some("doc-1".into()),
            author_did: "did:key:zOld".into(),
            timestamp: 1_700_000_000_000,
            payload: Some(signal::Payload::Flag(FlagPayload {
                kind: FlagKind::Info.into(),
                audience_type: AudienceType::Space.into(),
                target_did: None,
                message: "hello".into(),
                audience: None,
                anchor_text: None,
            })),
            ..Default::default()
        };
        let legacy_bytes = legacy.encode_to_vec();
        let legacy_back = Signal::decode(legacy_bytes.as_slice()).unwrap();
        assert!(legacy_back.record_id.is_empty());
        assert_eq!(legacy_back.event(), SignalEventType::Unspecified);
        assert!(legacy_back.signature.is_empty());
        assert!(legacy_back.hlc.is_none());
        assert!(legacy_back.attestation.is_none());
    }
}
