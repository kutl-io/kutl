//! Relay-specific protocol helpers built on top of `kutl_proto::protocol`.
//!
//! Re-exports shared encode/decode and version constants, and adds
//! relay-only envelope builders (handshake ack, error).

pub use kutl_proto::protocol::{
    ABSOLUTE_BLOB_MAX, PROTOCOL_VERSION_MAJOR, PROTOCOL_VERSION_MINOR, blob_ops_envelope,
    decode_envelope, encode_envelope, is_blob_mode, is_text_mode, presence_update_envelope,
    signal_flag_envelope, stale_subscriber_envelope, subscribe_status_envelope, sync_ops_envelope,
};

use kutl_proto::sync::{self, ErrorCode, HandshakeAck, SyncEnvelope, sync_envelope::Payload};

use crate::config::RelayConfig;

/// Build a `HandshakeAck` envelope from relay config.
pub fn wrap_handshake_ack(config: &RelayConfig) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::HandshakeAck(HandshakeAck {
            protocol_version_major: PROTOCOL_VERSION_MAJOR,
            protocol_version_minor: PROTOCOL_VERSION_MINOR,
            min_supported_major: PROTOCOL_VERSION_MAJOR,
            relay_name: config.relay_name.clone(),
            features: Vec::new(),
        })),
    }
}

/// Build an `Error` envelope.
pub fn wrap_error(code: ErrorCode, message: impl Into<String>) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::Error(sync::Error {
            code: code.into(),
            message: message.into(),
            details: String::new(),
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
        })),
    }
}

/// Machine-readable reason a quota was hit. Rendered into `Error.details` as
/// JSON so clients can pick cap-specific copy without string-matching the
/// human-readable message. See RFD 0063.
#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapKind {
    /// Per-file/patch byte cap (tier `per_blob_size` or `MAX_PATCH_BYTES`).
    BlobSize,
    /// Account-level total storage cap (tier `storage_bytes_total`).
    StorageTotal,
    /// Per-document op-count cap (`MAX_OPS_PER_DOC`).
    OpCount,
    /// Per-account document-count cap (tier `documents_per_account`).
    DocCount,
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
        let envelope = kutl_proto::protocol::handshake_envelope("test");
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
        let envelope = wrap_handshake_ack(&config);
        match envelope.payload {
            Some(Payload::HandshakeAck(ack)) => {
                assert_eq!(ack.relay_name, "test-relay");
                assert_eq!(ack.protocol_version_major, PROTOCOL_VERSION_MAJOR);
                assert_eq!(ack.protocol_version_minor, PROTOCOL_VERSION_MINOR);
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
}
