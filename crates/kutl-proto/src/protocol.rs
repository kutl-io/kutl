//! Shared protocol helpers for the kutl sync wire format.
//!
//! Provides encode/decode for `SyncEnvelope`, protocol version constants,
//! and builder functions for common envelope types. Used by both the relay
//! and the daemon client.

use crate::sync::{
    self, ContentMode, Handshake, JoinSpace, RegisterDocument, RenameDocument, ResolveSpace,
    Subscribe, SyncEnvelope, UnregisterDocument, Unsubscribe, sync_envelope::Payload,
};
use prost::Message;

/// Current protocol major version.
pub const PROTOCOL_VERSION_MAJOR: u32 = 0;
/// Current protocol minor version.
pub const PROTOCOL_VERSION_MINOR: u32 = 1;
/// Absolute upper bound on a single blob upload, enforced at the WebSocket
/// codec layer. Bounds per-upload memory use on the relay; real per-tier
/// limits are smaller and enforced via `QuotaBackend::load_space_quota`.
pub const ABSOLUTE_BLOB_MAX: usize = 25 * 1024 * 1024;

/// Encode a `SyncEnvelope` to bytes for a binary WebSocket frame.
pub fn encode_envelope(envelope: &SyncEnvelope) -> Vec<u8> {
    envelope.encode_to_vec()
}

/// Decode bytes into a `SyncEnvelope`.
pub fn decode_envelope(bytes: &[u8]) -> Result<SyncEnvelope, prost::DecodeError> {
    SyncEnvelope::decode(bytes)
}

/// Encode bytes as lowercase hex.
pub fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// Build a `SyncOps` envelope for text content.
pub fn sync_ops_envelope(
    space_id: &str,
    document_id: &str,
    ops: Vec<u8>,
    metadata: Vec<sync::ChangeMetadata>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::SyncOps(sync::SyncOps {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            parents: Vec::new(),
            ops,
            metadata,
            sequence_number: 0,
            is_last: true,
            content_mode: 0,
            content_hash: Vec::new(),
            blob_size: 0,
            blob_ref_url: String::new(),
        })),
    }
}

/// Build a `SyncOps` envelope for binary blob content.
pub fn blob_ops_envelope(
    space_id: &str,
    document_id: &str,
    blob_bytes: Vec<u8>,
    content_hash: Vec<u8>,
    metadata: Option<sync::ChangeMetadata>,
) -> SyncEnvelope {
    #[allow(clippy::cast_possible_wrap)]
    let blob_size = blob_bytes.len() as i64;
    SyncEnvelope {
        payload: Some(Payload::SyncOps(sync::SyncOps {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            parents: Vec::new(),
            ops: blob_bytes,
            metadata: metadata.into_iter().collect(),
            sequence_number: 0,
            is_last: true,
            content_mode: ContentMode::Blob.into(),
            content_hash,
            blob_size,
            blob_ref_url: String::new(),
        })),
    }
}

/// Returns `true` if the `SyncOps` carries binary blob content.
pub fn is_blob_mode(sync_ops: &sync::SyncOps) -> bool {
    matches!(
        ContentMode::try_from(sync_ops.content_mode),
        Ok(ContentMode::Blob | ContentMode::BlobRef)
    )
}

/// Returns `true` if the `SyncOps` carries text/CRDT content.
pub fn is_text_mode(sync_ops: &sync::SyncOps) -> bool {
    !is_blob_mode(sync_ops)
}

/// Build a client `Handshake` envelope (no auth token).
pub fn handshake_envelope(client_name: &str) -> SyncEnvelope {
    handshake_envelope_with_token(client_name, "", "")
}

/// Build a client `Handshake` envelope with an auth token and optional display name.
pub fn handshake_envelope_with_token(
    client_name: &str,
    auth_token: &str,
    display_name: &str,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::Handshake(Handshake {
            protocol_version_major: PROTOCOL_VERSION_MAJOR,
            protocol_version_minor: PROTOCOL_VERSION_MINOR,
            client_name: client_name.to_owned(),
            features: Vec::new(),
            auth_token: auth_token.to_owned(),
            display_name: display_name.to_owned(),
        })),
    }
}

/// Build a `Subscribe` envelope.
pub fn subscribe_envelope(space_id: &str, document_id: &str) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::Subscribe(Subscribe {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
        })),
    }
}

/// Build an `Unsubscribe` envelope.
pub fn unsubscribe_envelope(space_id: &str, document_id: &str) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::Unsubscribe(Unsubscribe {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
        })),
    }
}

/// Build a `Signal` envelope containing a flag payload.
#[allow(clippy::too_many_arguments)]
pub fn signal_flag_envelope(
    id: &str,
    space_id: &str,
    document_id: &str,
    author_did: &str,
    kind: i32,
    audience_type: i32,
    message: &str,
    target_did: &str,
    timestamp: i64,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::Signal(sync::Signal {
            id: id.to_owned(),
            space_id: space_id.to_owned(),
            document_id: Some(document_id.to_owned()),
            author_did: author_did.to_owned(),
            timestamp,
            payload: Some(sync::signal::Payload::Flag(sync::FlagPayload {
                kind,
                audience_type,
                target_did: Some(target_did.to_owned()),
                message: message.to_owned(),
            })),
        })),
    }
}

/// Build a `PresenceUpdate` envelope for relay.
pub fn presence_update_envelope(
    space_id: &str,
    document_id: &str,
    participant_did: &str,
    cursor_pos: u32,
    custom_data: Vec<u8>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::PresenceUpdate(sync::PresenceUpdate {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            participant_did: participant_did.to_owned(),
            cursor_pos,
            custom_data,
        })),
    }
}

/// Source provenance bundle carried on a `RegisterDocument` envelope.
///
/// The daemon path populates `originally_created_at_ms` only. The ingestion
/// worker populates all six. Internal callers (MCP, presence, tests) use
/// `..Default::default()`.
///
/// Bundled so the envelope builder doesn't grow a positional argument per
/// provenance field — and so adding a seventh later doesn't touch every
/// caller.
#[derive(Debug, Default, Clone)]
pub struct RegisterDocumentMetadata {
    /// Filesystem birthtime in Unix millis. Set on the daemon path
    /// from `stat`'s creation-time field. `None` otherwise.
    pub originally_created_at_ms: Option<i64>,
    /// `SourceKind` enum value (proto-encoded as `u32`). `None` on
    /// the daemon path; the DB default of 0 (native) applies.
    pub source_kind: Option<u32>,
    /// Source identifier (e.g. Notion page id). `None` on the daemon path.
    pub source_id: Option<String>,
    /// Canonical URL at the source. `None` on the daemon path.
    pub source_url: Option<String>,
    /// Ingestion job id (UUID string). `None` on the daemon path.
    pub ingestion_job_id: Option<String>,
    /// Author label as reported by the source. `None` on the daemon path.
    pub source_author_display: Option<String>,
}

/// Build a `RegisterDocument` envelope.
///
/// `rfd0081` carries source provenance — `Default::default()` is fine
/// for callers that have none.
pub fn register_document_envelope(
    space_id: &str,
    document_id: &str,
    path: &str,
    metadata: Option<sync::ChangeMetadata>,
    rfd0081: RegisterDocumentMetadata,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::RegisterDocument(RegisterDocument {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            path: path.to_owned(),
            metadata,
            originally_created_at_ms: rfd0081.originally_created_at_ms,
            source_kind: rfd0081.source_kind,
            source_id: rfd0081.source_id,
            source_url: rfd0081.source_url,
            ingestion_job_id: rfd0081.ingestion_job_id,
            source_author_display: rfd0081.source_author_display,
            // RFD 0042 amendment 2026-05-24: UX-only metadata threaded
            // through to the kutlhub-relay's `documents` mirror INSERT.
            // Daemon-path callers leave these absent; the UX server's
            // CreateDocument handler populates them in Phase 3.
            title: None,
            content_type: None,
            // RFD 0042 amendment 2026-05-24 follow-up: UX-only convert-
            // path metadata, populated only by web/ingestion-worker
            // convert flows. Daemon-path callers leave these absent.
            converted_from_id: None,
            converted_from_filename: None,
            size_bytes: None,
        })),
    }
}

/// Build a `RenameDocument` envelope.
pub fn rename_document_envelope(
    space_id: &str,
    document_id: &str,
    old_path: &str,
    new_path: &str,
    metadata: Option<sync::ChangeMetadata>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::RenameDocument(RenameDocument {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            old_path: old_path.to_owned(),
            new_path: new_path.to_owned(),
            metadata,
        })),
    }
}

/// Build an `UnregisterDocument` envelope.
pub fn unregister_document_envelope(
    space_id: &str,
    document_id: &str,
    metadata: Option<sync::ChangeMetadata>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::UnregisterDocument(UnregisterDocument {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            metadata,
        })),
    }
}

/// Build a `JoinSpace` envelope.
pub fn join_space_envelope(code: &str) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::JoinSpace(JoinSpace {
            code: code.to_owned(),
        })),
    }
}

/// Build a `ResolveSpace` envelope.
pub fn resolve_space_envelope(owner: &str, slug: &str) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::ResolveSpace(ResolveSpace {
            owner: owner.to_owned(),
            slug: slug.to_owned(),
        })),
    }
}

/// Build a `ListSpaceDocuments` envelope.
pub fn list_space_documents_envelope(space_id: &str) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::ListSpaceDocuments(
            crate::sync::ListSpaceDocuments {
                space_id: space_id.to_owned(),
            },
        )),
    }
}

/// Build a `SubscribeStatus` envelope.
///
/// Sent once per subscribe to communicate the relay's read
/// classification — Found, Empty, Inconsistent, or Unavailable.
/// Clients gate editor writability on the `LoadStatus`.
pub fn subscribe_status_envelope(
    space_id: &str,
    document_id: &str,
    load_status: sync::LoadStatus,
    expected_version: u64,
    reason: String,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::SubscribeStatus(sync::SubscribeStatus {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            load_status: load_status.into(),
            expected_version,
            reason,
        })),
    }
}

/// Build a `RegisterDocumentAck` envelope (RFD 0042 amendment 2026-05-24).
///
/// Emitted by the relay after `register_document_internal` returns Ok —
/// i.e. after both the in-memory registry insert AND the
/// `RegistryBackend::save_entry` mirror have committed. The presence of
/// `entry` is the relay's promise of durable, cross-store persistence.
///
/// On failure, callers pass `entry = None` and populate `error` with a
/// structured `sync::Error` matching the `send_error` shape.
pub fn register_document_ack_envelope(
    space_id: &str,
    document_id: &str,
    success: bool,
    entry: Option<sync::RegistryEntry>,
    error: Option<sync::Error>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::RegisterDocumentAck(sync::RegisterDocumentAck {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            entry,
            error,
            success,
        })),
    }
}

/// Build a `RenameDocumentAck` envelope (RFD 0042 amendment 2026-05-24).
///
/// Emitted after both the in-memory rename AND the
/// `RegistryBackend::save_entry` mirror have committed. No `entry`
/// payload — the caller already knows the new path it requested.
pub fn rename_document_ack_envelope(
    space_id: &str,
    document_id: &str,
    success: bool,
    error: Option<sync::Error>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::RenameDocumentAck(sync::RenameDocumentAck {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            error,
            success,
        })),
    }
}

/// Build an `UnregisterDocumentAck` envelope (RFD 0042 amendment 2026-05-24).
///
/// Emitted after both the in-memory soft-delete AND
/// `RegistryBackend::delete_entry` (which also propagates `deleted_at`
/// to the mirrored `documents` row) have committed.
pub fn unregister_document_ack_envelope(
    space_id: &str,
    document_id: &str,
    success: bool,
    error: Option<sync::Error>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::UnregisterDocumentAck(
            sync::UnregisterDocumentAck {
                space_id: space_id.to_owned(),
                document_id: document_id.to_owned(),
                error,
                success,
            },
        )),
    }
}

/// Build an `UnregisterSpaceAck` envelope (RFD 0042 amendment
/// 2026-05-24 — B-full follow-up).
///
/// `document_ids` carries the IDs of documents the relay successfully
/// unregistered in this bulk call. Empty when `success = false`.
pub fn unregister_space_ack_envelope(
    space_id: &str,
    document_ids: Vec<String>,
    success: bool,
    error: Option<sync::Error>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::UnregisterSpaceAck(sync::UnregisterSpaceAck {
            space_id: space_id.to_owned(),
            document_ids,
            error,
            success,
        })),
    }
}

/// Build a `TransferSpaceOwnershipAck` envelope (RFD 0042 amendment
/// 2026-05-24 — B-full follow-up).
pub fn transfer_space_ownership_ack_envelope(
    space_id: &str,
    transferred_count: u32,
    success: bool,
    error: Option<sync::Error>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::TransferSpaceOwnershipAck(
            sync::TransferSpaceOwnershipAck {
                space_id: space_id.to_owned(),
                transferred_count,
                error,
                success,
            },
        )),
    }
}

/// Build a `StaleSubscriber` envelope.
pub fn stale_subscriber_envelope(
    space_id: &str,
    document_id: &str,
    reason: sync::StaleReason,
    relay_drops: u64,
    relay_ops_count: u64,
    timestamp_ms: i64,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::StaleSubscriber(sync::StaleSubscriber {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            reason: reason.into(),
            relay_drops,
            relay_ops_count,
            timestamp_ms,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let envelope = handshake_envelope("test-client");
        let bytes = encode_envelope(&envelope);
        let decoded = decode_envelope(&bytes).unwrap();
        assert_eq!(envelope, decoded);
    }

    #[test]
    fn test_sync_ops_envelope() {
        let envelope = sync_ops_envelope("space", "doc", vec![1, 2, 3], Vec::new());
        match envelope.payload {
            Some(Payload::SyncOps(ops)) => {
                assert_eq!(ops.space_id, "space");
                assert_eq!(ops.document_id, "doc");
                assert_eq!(ops.ops, vec![1, 2, 3]);
            }
            _ => panic!("expected SyncOps"),
        }
    }

    #[test]
    fn test_subscribe_envelope() {
        let envelope = subscribe_envelope("space", "doc");
        match envelope.payload {
            Some(Payload::Subscribe(sub)) => {
                assert_eq!(sub.space_id, "space");
                assert_eq!(sub.document_id, "doc");
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn test_blob_ops_envelope() {
        let blob = vec![0xFF, 0xFE, 0x00, 0x01];
        let hash = vec![0xAA; 32];
        let envelope = blob_ops_envelope("space", "img.png", blob.clone(), hash.clone(), None);
        match envelope.payload {
            Some(Payload::SyncOps(ops)) => {
                assert_eq!(ops.space_id, "space");
                assert_eq!(ops.document_id, "img.png");
                assert_eq!(ops.ops, blob);
                assert_eq!(ops.content_hash, hash);
                assert_eq!(ops.blob_size, 4);
                assert_eq!(ops.content_mode, i32::from(sync::ContentMode::Blob));
            }
            _ => panic!("expected SyncOps"),
        }
    }

    #[test]
    fn test_is_blob_mode() {
        let text_ops = sync::SyncOps {
            content_mode: sync::ContentMode::Unspecified.into(),
            ..Default::default()
        };
        assert!(!is_blob_mode(&text_ops));
        assert!(is_text_mode(&text_ops));

        let text_explicit = sync::SyncOps {
            content_mode: sync::ContentMode::Text.into(),
            ..Default::default()
        };
        assert!(!is_blob_mode(&text_explicit));
        assert!(is_text_mode(&text_explicit));

        let blob_ops = sync::SyncOps {
            content_mode: sync::ContentMode::Blob.into(),
            ..Default::default()
        };
        assert!(is_blob_mode(&blob_ops));
        assert!(!is_text_mode(&blob_ops));

        let blob_ref_ops = sync::SyncOps {
            content_mode: sync::ContentMode::BlobRef.into(),
            ..Default::default()
        };
        assert!(is_blob_mode(&blob_ref_ops));
        assert!(!is_text_mode(&blob_ref_ops));
    }

    #[test]
    fn test_blob_ops_roundtrip() {
        let blob = vec![0xFF; 64];
        let hash = vec![0xBB; 32];
        let envelope = blob_ops_envelope("p", "d", blob.clone(), hash.clone(), None);
        let bytes = encode_envelope(&envelope);
        let decoded = decode_envelope(&bytes).unwrap();
        match decoded.payload {
            Some(Payload::SyncOps(ops)) => {
                assert_eq!(ops.ops, blob);
                assert_eq!(ops.content_hash, hash);
                assert!(is_blob_mode(&ops));
            }
            _ => panic!("expected SyncOps"),
        }
    }

    #[test]
    fn test_signal_flag_envelope() {
        let envelope = signal_flag_envelope(
            "ar-1",
            "space-1",
            "doc-1",
            "did:key:author",
            i32::from(sync::FlagKind::ReviewRequested),
            i32::from(sync::AudienceType::Space),
            "please review",
            "",
            1_700_000_000_000,
        );
        match &envelope.payload {
            Some(Payload::Signal(s)) => {
                assert_eq!(s.id, "ar-1");
                assert_eq!(s.space_id, "space-1");
                match &s.payload {
                    Some(sync::signal::Payload::Flag(f)) => {
                        assert_eq!(f.kind, i32::from(sync::FlagKind::ReviewRequested));
                        assert_eq!(f.message, "please review");
                    }
                    _ => panic!("expected flag payload"),
                }
            }
            _ => panic!("expected Signal"),
        }
    }

    #[test]
    fn test_signal_flag_roundtrip() {
        let envelope = signal_flag_envelope(
            "ar-2",
            "space-2",
            "doc-2",
            "did:key:bob",
            i32::from(sync::FlagKind::Blocked),
            i32::from(sync::AudienceType::Participant),
            "stuck on this",
            "did:key:alice",
            1_700_000_001_000,
        );
        let bytes = encode_envelope(&envelope);
        let decoded = decode_envelope(&bytes).unwrap();
        assert_eq!(envelope, decoded);
    }

    #[test]
    fn test_stale_subscriber_envelope() {
        let envelope = stale_subscriber_envelope(
            "space",
            "doc",
            sync::StaleReason::ChannelFull,
            1,
            500,
            1_700_000_000_000,
        );
        match envelope.payload {
            Some(Payload::StaleSubscriber(s)) => {
                assert_eq!(s.space_id, "space");
                assert_eq!(s.document_id, "doc");
                assert_eq!(s.reason, i32::from(sync::StaleReason::ChannelFull));
                assert_eq!(s.relay_drops, 1);
                assert_eq!(s.relay_ops_count, 500);
                assert_eq!(s.timestamp_ms, 1_700_000_000_000);
            }
            _ => panic!("expected StaleSubscriber"),
        }
    }

    #[test]
    fn test_stale_subscriber_roundtrip() {
        let envelope = stale_subscriber_envelope(
            "space",
            "doc",
            sync::StaleReason::ChannelClosed,
            0,
            100,
            1_700_000_000_000,
        );
        let bytes = encode_envelope(&envelope);
        let decoded = decode_envelope(&bytes).unwrap();
        assert_eq!(envelope, decoded);
    }
}
