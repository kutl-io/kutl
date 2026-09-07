//! Relay-specific protocol helpers built on top of `kutl_proto::protocol`.
//!
//! Re-exports shared encode/decode and version constants, and adds the
//! relay-only builders: the two handshake acks, and the refusal record every
//! typed reply carries.

pub use kutl_proto::protocol::{
    ABSOLUTE_BLOB_MAX, MIN_SUPPORTED_PROTOCOL_MAJOR, PROTOCOL_VERSION_MAJOR,
    PROTOCOL_VERSION_MINOR, WS_MESSAGE_MAX, blob_ops_envelope, decode_envelope, encode_envelope,
    is_blob_mode, presence_update_envelope, register_document_ack_envelope,
    rename_document_ack_envelope, stale_subscriber_envelope, subscribe_status_envelope,
    sync_ops_envelope, sync_ops_rejected_envelope, transfer_space_ownership_ack_envelope,
    unregister_document_ack_envelope, unregister_space_ack_envelope,
};

use kutl_proto::sync::{self, ErrorCode, HandshakeAck, SyncEnvelope, sync_envelope::Payload};

use crate::config::RelayConfig;

/// The signal-reseed capability string, advertised when this relay accepts
/// **client-pushed** signal history (the `SignalReseed` WS frame).
///
/// Serving catch-up needs no claim (every relay answers a subscribe with a
/// page, empty without a record log); accepting history does, and it is
/// deniable on its own: a relay's signing key signs records, so a leaked key
/// mints pool-wide-valid history, and refusing re-seed on the deployment
/// holding the most attractive key closes one of the two loops that history
/// could arrive through.
///
/// Requires a record log — there is nowhere to append without one — and
/// `config.accepts_reseed`.
pub use kutl_proto::protocol::SIGNAL_RESEED_CAPABILITY;

/// Build a `HandshakeAck` envelope from relay config.
///
/// `relay_did` is the relay's `did:key` signing identity. Pass
/// `Some(did)` when an identity is present; `None` on standalone test actors
/// and the in-memory test relay, which degrade gracefully
/// to asserted (tier-3) records — the ack will carry an empty string so clients
/// can distinguish "relay present but no identity" from a future explicit
/// opt-out.
///
/// `has_record_log` gates [`SIGNAL_RESEED_CAPABILITY`]: a relay without a
/// log has nowhere to store pushed history. Consent is the other half,
/// `config.accepts_reseed`, so a relay can serve catch-up while refusing to
/// be written to. Consent is read from config here rather than taken as a
/// second parameter, because two adjacent bools at a call site is exactly how
/// they get swapped.
pub fn wrap_handshake_ack(
    config: &RelayConfig,
    relay_did: Option<&str>,
    has_record_log: bool,
) -> SyncEnvelope {
    let mut features = vec![kutl_proto::protocol::BARRIER_CAPABILITY.to_owned()];
    if has_record_log && config.accepts_reseed {
        features.push(SIGNAL_RESEED_CAPABILITY.to_owned());
    }
    SyncEnvelope {
        payload: Some(Payload::HandshakeAck(HandshakeAck {
            protocol_version_major: PROTOCOL_VERSION_MAJOR,
            protocol_version_minor: PROTOCOL_VERSION_MINOR,
            min_supported_major: MIN_SUPPORTED_PROTOCOL_MAJOR,
            relay_name: config.relay_name.clone(),
            features,
            relay_did: relay_did.unwrap_or("").to_owned(),
            error: None,
        })),
    }
}

/// Build the `HandshakeAck` that refuses a handshake: the version fields a
/// client below the floor reads its own verdict from, and `error` for one
/// that reads the refusal. The connection closes after it.
pub fn wrap_refused_handshake_ack(config: &RelayConfig, error: sync::Error) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::HandshakeAck(HandshakeAck {
            protocol_version_major: PROTOCOL_VERSION_MAJOR,
            protocol_version_minor: PROTOCOL_VERSION_MINOR,
            min_supported_major: MIN_SUPPORTED_PROTOCOL_MAJOR,
            relay_name: config.relay_name.clone(),
            features: Vec::new(),
            relay_did: String::new(),
            error: Some(error),
        })),
    }
}

/// The refusal record a typed reply carries: `code` is the verdict a client
/// branches on, `message` is display text. Every refusal the relay sends is
/// built here, so the shape cannot drift between carriers.
pub fn refusal(code: ErrorCode, message: impl Into<String>) -> sync::Error {
    sync::Error {
        code: code.into(),
        message: message.into(),
        ..Default::default()
    }
}

/// [`refusal`] with a structured `details` payload.
pub fn refusal_with_details(
    code: ErrorCode,
    message: impl Into<String>,
    details: String,
) -> sync::Error {
    sync::Error {
        code: code.into(),
        message: message.into(),
        details,
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

/// A `QUOTA_EXCEEDED` refusal with structured details.
///
/// The `cap_kind` field is the stable machine-readable reason. Clients
/// should dispatch copy on `cap_kind` rather than matching on `message`.
pub fn quota_exceeded_refusal(cap_kind: CapKind, limit: i64, used: Option<i64>) -> sync::Error {
    let details = serde_json::to_string(&QuotaDetails {
        cap_kind,
        limit,
        used,
    })
    .expect("quota details are owned primitives, serde_json never fails");
    refusal_with_details(ErrorCode::QuotaExceeded, "quota exceeded", details)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one refusal shape: the verdict is the code, the details ride the
    /// record, and a quota refusal carries its cap as structured JSON.
    #[test]
    fn test_refusal_builders_carry_the_verdict_and_details() {
        let plain = refusal(ErrorCode::InvalidMessage, "bad frame");
        assert_eq!(plain.code(), ErrorCode::InvalidMessage);
        assert_eq!(plain.message, "bad frame");
        assert!(plain.details.is_empty());
        let quota = quota_exceeded_refusal(CapKind::OpCount, 7, Some(3));
        assert_eq!(quota.code(), ErrorCode::QuotaExceeded);
        let details: serde_json::Value = serde_json::from_str(&quota.details).unwrap();
        assert_eq!(details["cap_kind"], "op_count");
        assert_eq!(details["limit"], 7);
        assert_eq!(details["used"], 3);
        let unknown_usage = quota_exceeded_refusal(CapKind::BlobSize, 5_000_000, None);
        assert!(!unknown_usage.details.contains("used"));
    }

    /// A refused handshake is an ack: the version floor for a client that
    /// reads its own verdict, the error for one that reads the refusal.
    #[test]
    fn test_refused_handshake_ack_carries_floor_and_error() {
        let config = RelayConfig::default();
        let envelope =
            wrap_refused_handshake_ack(&config, refusal(ErrorCode::AuthFailed, "rejected"));
        match envelope.payload {
            Some(Payload::HandshakeAck(ack)) => {
                assert_eq!(ack.min_supported_major, MIN_SUPPORTED_PROTOCOL_MAJOR);
                assert!(ack.features.is_empty());
                assert_eq!(ack.error.unwrap().code(), ErrorCode::AuthFailed);
            }
            other => panic!("expected a HandshakeAck, got {other:?}"),
        }
    }

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

    /// `signal-reseed` is advertised only with a record log AND consent: a
    /// relay without a log has nowhere to store pushed history, and one with a
    /// log serves catch-up regardless of whether it accepts any.
    #[test]
    fn test_ack_advertises_reseed_only_with_a_log_and_consent() {
        let consenting = RelayConfig {
            relay_name: "test-relay".into(),
            accepts_reseed: true,
            ..Default::default()
        };
        let refusing = RelayConfig {
            relay_name: "test-relay".into(),
            accepts_reseed: false,
            ..Default::default()
        };
        let advertises = |envelope: SyncEnvelope| match envelope.payload {
            Some(Payload::HandshakeAck(ack)) => {
                ack.features.iter().any(|f| f == SIGNAL_RESEED_CAPABILITY)
            }
            _ => panic!("expected HandshakeAck"),
        };
        assert!(advertises(wrap_handshake_ack(&consenting, None, true)));
        assert!(!advertises(wrap_handshake_ack(&consenting, None, false)));
        assert!(!advertises(wrap_handshake_ack(&refusing, None, true)));
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
    fn test_capability_strings_match_contract_file() {
        let raw = include_str!("../../../proto/kutl/sync/v1/contract.json");
        let contract: serde_json::Value = serde_json::from_str(raw).expect("contract.json parses");
        assert_eq!(
            contract["capability_signal_reseed"],
            serde_json::json!(SIGNAL_RESEED_CAPABILITY)
        );
        assert_eq!(
            contract["capability_barrier"],
            serde_json::json!(kutl_proto::protocol::BARRIER_CAPABILITY)
        );
    }
}
