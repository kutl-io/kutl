//! Relay-specific protocol helpers built on top of `kutl_proto::protocol`.
//!
//! Re-exports shared encode/decode and version constants, and adds
//! relay-only envelope builders (handshake ack, error).

pub use kutl_proto::protocol::{
    ABSOLUTE_BLOB_MAX, MIN_SUPPORTED_PROTOCOL_MAJOR, PROTOCOL_VERSION_MAJOR,
    PROTOCOL_VERSION_MINOR, blob_ops_envelope, decode_envelope, encode_envelope, is_blob_mode,
    presence_update_envelope, register_document_ack_envelope, rename_document_ack_envelope,
    stale_subscriber_envelope, subscribe_status_envelope, sync_ops_envelope,
    transfer_space_ownership_ack_envelope, unregister_document_ack_envelope,
    unregister_space_ack_envelope,
};

use kutl_proto::sync::{self, ErrorCode, HandshakeAck, SyncEnvelope, sync_envelope::Payload};

use crate::config::RelayConfig;

/// Capability advertised in `HandshakeAck.features` to signal that this relay
/// supports the signal-records catch-up + re-seed HTTP surface.
///
/// Advertised ONLY when the relay has a record log. A log-less relay (a
/// standalone test or sim actor) cannot serve catch-up — its pages are
/// permanently empty — so it must not claim the capability. The OSS binary
/// always has one, and so does kutlhub.
///
/// This capability covers **reads only**. Accepting client-pushed history is
/// [`SIGNAL_RESEED_CAPABILITY`], a separate claim — see there for why the two
/// had to split.
pub const SIGNAL_RECORDS_CAPABILITY: &str = "signal-records";

/// Capability advertised in `HandshakeAck.features` when this relay accepts
/// **client-pushed** signal history (the `SignalReseed` WS frame).
///
/// A separate claim from [`SIGNAL_RECORDS_CAPABILITY`] because *serving*
/// catch-up and *accepting* history must be independently deniable: under a
/// single flag, any relay with a record log (which it needs to serve catch-up)
/// would also be open to accepting pushed history. A relay's signing key signs
/// records, so a leaked key mints pool-wide-valid history; refusing re-seed on
/// the deployment holding the most attractive key closes one of the two loops
/// that history could arrive through.
///
/// Requires a record log — there is nowhere to append without one — so it is
/// never advertised alone.
pub const SIGNAL_RESEED_CAPABILITY: &str = "signal-reseed";

/// Build a `HandshakeAck` envelope from relay config.
///
/// `relay_did` is the relay's `did:key` signing identity. Pass
/// `Some(did)` when an identity is present; `None` on the storeless host relay
/// (kutlhub — no data dir) and standalone test actors, which degrade gracefully
/// to asserted (tier-3) records — the ack will carry an empty string so clients
/// can distinguish "relay present but no identity" from a future explicit
/// opt-out.
///
/// `supports_signal_records` is `true` only when the relay has a record log.
/// When `false`, neither [`SIGNAL_RECORDS_CAPABILITY`] nor
/// [`SIGNAL_RESEED_CAPABILITY`] is advertised — a log-less relay can neither
/// page history nor store any.
///
/// Re-seed is the narrower claim: it needs the log **and** `config.accepts_reseed`,
/// so a relay can serve catch-up while refusing to be written to.
/// Derived from config here rather than taken as a second parameter,
/// because two adjacent bools at a call site is exactly how they get swapped.
pub fn wrap_handshake_ack(
    config: &RelayConfig,
    relay_did: Option<&str>,
    supports_signal_records: bool,
) -> SyncEnvelope {
    let mut features = Vec::new();
    if supports_signal_records {
        features.push(SIGNAL_RECORDS_CAPABILITY.to_owned());
        if config.accepts_reseed {
            features.push(SIGNAL_RESEED_CAPABILITY.to_owned());
        }
    }
    SyncEnvelope {
        payload: Some(Payload::HandshakeAck(HandshakeAck {
            protocol_version_major: PROTOCOL_VERSION_MAJOR,
            protocol_version_minor: PROTOCOL_VERSION_MINOR,
            min_supported_major: MIN_SUPPORTED_PROTOCOL_MAJOR,
            relay_name: config.relay_name.clone(),
            features,
            relay_did: relay_did.unwrap_or("").to_owned(),
        })),
    }
}

/// Build an `Error` envelope.
pub fn wrap_error(code: ErrorCode, message: impl Into<String>) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::Error(sync::Error {
            code: code.into(),
            message: message.into(),
            ..Default::default()
        })),
    }
}

/// Build an `Error` envelope scoped to one document. The identifiers ride
/// as STRUCTURED fields alongside the human-facing message: clients act on
/// the fields, so the prose can be reworded freely — text meant for people
/// is never a contract.
pub fn wrap_error_for_document(
    code: ErrorCode,
    message: impl Into<String>,
    space_id: impl Into<String>,
    document_id: impl Into<String>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::Error(sync::Error {
            code: code.into(),
            message: message.into(),
            space_id: space_id.into(),
            document_id: document_id.into(),
            ..Default::default()
        })),
    }
}

/// Build an `Error` envelope with a structured details payload.
pub fn wrap_error_with_details(
    code: ErrorCode,
    message: impl Into<String>,
    details: String,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::Error(sync::Error {
            code: code.into(),
            message: message.into(),
            details,
            ..Default::default()
        })),
    }
}

/// Machine-readable reason a quota was hit. Rendered into `Error.details` as
/// JSON so clients can pick cap-specific copy without string-matching the
/// human-readable message.
#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapKind {
    /// Per-file/patch byte cap (tier `per_blob_size` or `MAX_PATCH_BYTES`).
    BlobSize,
    /// Account-level total storage cap (tier `storage_bytes_total`).
    StorageTotal,
    /// Per-document op-count cap (`MAX_OPS_PER_DOC`).
    OpCount,
}

/// Structured payload for `Error.details` when `code == QUOTA_EXCEEDED`.
///
/// Encoded via `serde_json` into a JSON string so clients can parse it
/// with their own decoders. No `format!`-based construction — that would
/// open an injection surface if more fields are added later.
#[derive(Debug, Clone, serde::Serialize)]
struct QuotaDetails {
    /// Which cap was hit.
    cap_kind: CapKind,
    /// Configured cap value.
    limit: i64,
    /// Current usage (omitted when not known, e.g. for per-patch caps).
    #[serde(skip_serializing_if = "Option::is_none")]
    used: Option<i64>,
}

/// Build a `QUOTA_EXCEEDED` error envelope with structured details.
///
/// The `cap_kind` field is the stable machine-readable reason. Clients
/// should dispatch copy on `cap_kind` rather than matching on `message`.
pub fn wrap_quota_exceeded(cap_kind: CapKind, limit: i64, used: Option<i64>) -> SyncEnvelope {
    let details = serde_json::to_string(&QuotaDetails {
        cap_kind,
        limit,
        used,
    })
    .expect("quota details are owned primitives, serde_json never fails");
    wrap_error_with_details(ErrorCode::QuotaExceeded, "quota exceeded", details)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let envelope =
            kutl_proto::protocol::handshake_envelope("9f86d081-884c-4d65-8a2f-eaa0c55ad015");
        let bytes = encode_envelope(&envelope);
        let decoded = decode_envelope(&bytes).unwrap();
        assert_eq!(envelope, decoded);
    }

    #[test]
    fn test_wrap_handshake_ack() {
        let config = RelayConfig {
            relay_name: "test-relay".into(),
            ..Default::default()
        };
        let envelope = wrap_handshake_ack(&config, None, false);
        match envelope.payload {
            Some(Payload::HandshakeAck(ack)) => {
                assert_eq!(ack.relay_name, "test-relay");
                assert_eq!(ack.protocol_version_major, PROTOCOL_VERSION_MAJOR);
                assert_eq!(ack.protocol_version_minor, PROTOCOL_VERSION_MINOR);
                assert_eq!(ack.relay_did, "", "no identity → empty relay_did");
            }
            _ => panic!("expected HandshakeAck"),
        }
    }

    /// The handshake ack advertises the `"signal-records"` capability ONLY when
    /// the relay has a durable signal store — otherwise catch-up returns
    /// permanently-empty pages and re-seed rejects every record, so a storeless
    /// relay must NOT advertise.
    #[test]
    fn test_ack_advertises_signal_records_only_with_store() {
        let config = RelayConfig {
            relay_name: "test-relay".into(),
            ..Default::default()
        };
        // With a durable store: advertised, regardless of identity presence.
        for relay_did in [None, Some("did:key:zRelay")] {
            let ack = wrap_handshake_ack(&config, relay_did, true);
            match ack.payload {
                Some(Payload::HandshakeAck(ack)) => assert!(
                    ack.features.iter().any(|f| f == SIGNAL_RECORDS_CAPABILITY),
                    "with a store, features must advertise {SIGNAL_RECORDS_CAPABILITY:?}, got {:?}",
                    ack.features
                ),
                _ => panic!("expected HandshakeAck"),
            }
        }
    }

    /// A storeless relay (one that never constructs a `SignalStore`, e.g. a
    /// standalone test actor) MUST NOT advertise the capability — the
    /// catch-up/re-seed surface would be permanently non-functional.
    #[test]
    fn test_ack_omits_signal_records_without_store() {
        let config = RelayConfig {
            relay_name: "test-relay".into(),
            ..Default::default()
        };
        for relay_did in [None, Some("did:key:zRelay")] {
            let ack = wrap_handshake_ack(&config, relay_did, false);
            match ack.payload {
                Some(Payload::HandshakeAck(ack)) => assert!(
                    !ack.features.iter().any(|f| f == SIGNAL_RECORDS_CAPABILITY),
                    "without a store, features must NOT advertise {SIGNAL_RECORDS_CAPABILITY:?}, got {:?}",
                    ack.features
                ),
                _ => panic!("expected HandshakeAck"),
            }
        }
    }

    /// The handshake ack advertises the relay DID when an identity is present
    /// and leaves it empty when absent (graceful degrade).
    #[test]
    fn test_handshake_ack_carries_relay_did_when_present() {
        let config = RelayConfig {
            relay_name: "test-relay".into(),
            ..Default::default()
        };

        let ack_with_did = wrap_handshake_ack(&config, Some("did:key:zRelay"), true);
        match ack_with_did.payload {
            Some(Payload::HandshakeAck(ack)) => {
                assert_eq!(ack.relay_did, "did:key:zRelay");
            }
            _ => panic!("expected HandshakeAck"),
        }

        let ack_absent = wrap_handshake_ack(&config, None, true);
        match ack_absent.payload {
            Some(Payload::HandshakeAck(ack)) => {
                assert!(
                    ack.relay_did.is_empty(),
                    "absent identity → empty relay_did"
                );
            }
            _ => panic!("expected HandshakeAck"),
        }
    }

    #[test]
    fn test_wrap_error() {
        let envelope = wrap_error(ErrorCode::InvalidMessage, "bad frame");
        match envelope.payload {
            Some(Payload::Error(e)) => {
                assert_eq!(e.code, i32::from(ErrorCode::InvalidMessage));
                assert_eq!(e.message, "bad frame");
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn test_wrap_quota_exceeded_serializes_cap_kind() {
        let envelope = wrap_quota_exceeded(CapKind::StorageTotal, 1_000_000, Some(999_999));
        match envelope.payload {
            Some(Payload::Error(e)) => {
                assert_eq!(e.code, i32::from(ErrorCode::QuotaExceeded));
                assert_eq!(e.message, "quota exceeded");
                // snake_case variant rendering.
                assert!(e.details.contains("\"cap_kind\":\"storage_total\""));
                assert!(e.details.contains("\"limit\":1000000"));
                assert!(e.details.contains("\"used\":999999"));
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn test_wrap_quota_exceeded_omits_used_when_none() {
        let envelope = wrap_quota_exceeded(CapKind::BlobSize, 5_000_000, None);
        match envelope.payload {
            Some(Payload::Error(e)) => {
                assert!(e.details.contains("\"cap_kind\":\"blob_size\""));
                assert!(!e.details.contains("\"used\""));
            }
            _ => panic!("expected Error"),
        }
    }

    /// Pin the advertised capability strings to the cross-language contract
    /// file (`oss/proto/kutl/sync/v1/contract.json`), the rendezvous every
    /// stack's copy is gated against.
    #[test]
    fn test_capability_strings_match_contract_file() {
        let raw = include_str!("../../../proto/kutl/sync/v1/contract.json");
        let contract: serde_json::Value = serde_json::from_str(raw).expect("contract.json parses");
        assert_eq!(
            contract["capability_signal_records"],
            serde_json::json!(SIGNAL_RECORDS_CAPABILITY)
        );
        assert_eq!(
            contract["capability_signal_reseed"],
            serde_json::json!(SIGNAL_RESEED_CAPABILITY)
        );
    }
}
