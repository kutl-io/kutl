//! Shared protocol helpers for the kutl sync wire format.
//!
//! Provides encode/decode for `SyncEnvelope`, protocol version constants,
//! and builder functions for common envelope types. Used by both the relay
//! and the daemon client.

use crate::sync::{
    self, ContentMode, Handshake, Hlc, JoinSpace, RegisterDocument, RenameDocument, ResolveSpace,
    Subscribe, SubscribeSignals, SyncEnvelope, UnregisterDocument, Unsubscribe,
    sync_envelope::Payload,
};
use prost::Message;

/// Current protocol major version.
///
/// A relay refuses any client below this major (see `HandshakeAck`'s
/// `min_supported_major`): the frames such a client depends on are gone, so
/// letting it connect buys a session that silently receives nothing rather
/// than an error it can act on.
pub const PROTOCOL_VERSION_MAJOR: u32 = 1;
/// Current protocol minor version. Resets with each major.
pub const PROTOCOL_VERSION_MINOR: u32 = 0;
/// Oldest protocol major a relay still speaks.
///
/// One constant for both halves of the contract: the relay advertises it as
/// `HandshakeAck.min_supported_major` AND refuses connections below it. Two
/// constants would let the promise and the enforcement drift.
///
/// Equal to [`PROTOCOL_VERSION_MAJOR`] today — no relay speaks more than one
/// major. Lowering it is how a relay would serve a transition period.
pub const MIN_SUPPORTED_PROTOCOL_MAJOR: u32 = PROTOCOL_VERSION_MAJOR;
/// Absolute upper bound on a single blob upload, enforced at the WebSocket
/// codec layer. Bounds per-upload memory use on the relay; real per-tier
/// limits are smaller and enforced via `QuotaBackend::load_space_quota`.
pub const ABSOLUTE_BLOB_MAX: usize = 25 * 1024 * 1024;

/// Check a received `HandshakeAck`'s version fields, both directions.
///
/// The ONE version gate for every client-side handshake consumer — daemon
/// sync client, one-shot CLI connections, the watch/MCP relay session, and
/// the library sync client all call this on the ack. A single function is
/// the point: the check exists precisely because per-site copies drift, and
/// a site with no check silently syncs nothing against a relay that speaks
/// a different major.
///
/// The first arm fires only against a relay that advertises a floor without
/// enforcing it (a current relay refuses too-old clients before acking).
/// The second arm is the one no relay-side gate can cover: an OLD relay —
/// mid-rollout, or an unupgraded self-hosted one — accepts the connection
/// and would then ignore frames this client depends on.
pub fn verify_ack_versions(ack: &sync::HandshakeAck) -> Result<(), String> {
    if PROTOCOL_VERSION_MAJOR < ack.min_supported_major {
        return Err(format!(
            "relay requires protocol version {} or later (this client speaks {}); upgrade kutl",
            ack.min_supported_major, PROTOCOL_VERSION_MAJOR
        ));
    }
    if ack.protocol_version_major < PROTOCOL_VERSION_MAJOR {
        return Err(format!(
            "relay speaks protocol version {}, this client requires {}; upgrade the relay",
            ack.protocol_version_major, PROTOCOL_VERSION_MAJOR
        ));
    }
    Ok(())
}

/// A relay's refusal of a handshake, read from its `Error` frame.
///
/// `auth_failed` is the relay's OWN verdict — `ErrorCode::AuthFailed`, meaning
/// the bearer presented was refused — as opposed to any other refusal, which no
/// credential change can fix. `message` is display text; its wording is not a
/// contract and must never be parsed.
pub struct HandshakeRefusal {
    /// Display text: the relay's message, with its `details` appended when set.
    pub message: String,
    /// The relay answered `ErrorCode::AuthFailed`.
    pub auth_failed: bool,
}

/// Classify a relay's handshake-phase `Error` frame.
///
/// The ONE reader of the refusal verdict, for the same reason
/// [`verify_ack_versions`] is the one version gate: there are four client-side
/// handshake implementations, and a per-site copy of "what counts as an auth
/// rejection" drifts. A site that reads the message text instead of the code
/// gets it wrong the first time a relay rewords an error.
///
/// Callers branch on `auth_failed` and render `message`; nothing downstream
/// inspects the prose.
pub fn handshake_refusal(e: &sync::Error) -> HandshakeRefusal {
    let message = if e.details.is_empty() {
        e.message.clone()
    } else {
        format!("{} ({})", e.message, e.details)
    };
    HandshakeRefusal {
        message,
        auth_failed: sync::ErrorCode::try_from(e.code) == Ok(sync::ErrorCode::AuthFailed),
    }
}

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
///
/// `author_by_agent_snapshot` carries the uncapped agent→author-DID map on the
/// catch-up (join) envelope so a fresh subscriber can install bindings the
/// FIFO-capped per-change `metadata` no longer carries (blame durability).
/// Incremental broadcast deltas pass an empty map — their per-change
/// metadata already carries the bindings the receiver needs.
pub fn sync_ops_envelope<S: std::hash::BuildHasher>(
    space_id: &str,
    document_id: &str,
    ops: Vec<u8>,
    metadata: Vec<sync::ChangeMetadata>,
    author_by_agent_snapshot: std::collections::HashMap<String, String, S>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::SyncOps(sync::SyncOps {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            ops,
            metadata,
            content_mode: 0,
            content_hash: Vec::new(),
            blob_size: 0,
            blob_ref_url: String::new(),
            // Collect into the prost field's default-hasher map (accepting any
            // caller hasher keeps the builder hasher-agnostic, clippy-pedantic).
            author_by_agent_snapshot: author_by_agent_snapshot.into_iter().collect(),
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
            ops: blob_bytes,
            metadata: metadata.into_iter().collect(),
            content_mode: ContentMode::Blob.into(),
            content_hash,
            blob_size,
            blob_ref_url: String::new(),
            // Blobs carry no text metadata and no author snapshot.
            author_by_agent_snapshot: std::collections::HashMap::new(),
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

/// Build a `SubscribeSignals` envelope — join a space's SIGNAL stream.
///
/// Distinct from [`subscribe_envelope`], which joins a DOCUMENT. Joining the
/// signal stream requires no document subscription: a signals-only client
/// (e.g. `kutl watch`) enters the recipient set directly instead of
/// subscribing to a sentinel document it has no interest in.
///
/// `cursor` resumes from a prior page; `None` starts from the beginning.
#[must_use]
pub fn subscribe_signals_envelope(space_id: &str, cursor: Option<Hlc>) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::SubscribeSignals(SubscribeSignals {
            space_id: space_id.to_owned(),
            cursor,
        })),
    }
}

/// Build a `SubmitFlag` envelope — the authored flag door.
///
/// Arguments, not a record: the relay mints every identity field, so a caller
/// cannot assert one. `target_did` present addresses the flag to that
/// participant; absent addresses it to the space — the same derivation every
/// other door performs, so an `Audience` with `to` unset is not expressible.
#[must_use]
pub fn submit_flag_envelope(
    client_ref: &str,
    space_id: &str,
    document_id: Option<&str>,
    kind: sync::FlagKind,
    message: &str,
    target_did: Option<&str>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::SubmitFlag(sync::SubmitFlag {
            client_ref: client_ref.to_owned(),
            space_id: space_id.to_owned(),
            document_id: document_id.map(str::to_owned),
            signal_id: None,
            kind: i32::from(kind),
            message: message.to_owned(),
            audience: Some(audience_for(target_did)),
        })),
    }
}

/// Build a `SubmitReply` envelope. A reply carries no audience: it inherits its
/// parent's, transitively.
#[must_use]
pub fn submit_reply_envelope(
    client_ref: &str,
    space_id: &str,
    parent_signal_id: &str,
    parent_reply_id: Option<&str>,
    body: &str,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::SubmitReply(sync::SubmitReply {
            client_ref: client_ref.to_owned(),
            space_id: space_id.to_owned(),
            parent_signal_id: parent_signal_id.to_owned(),
            parent_reply_id: parent_reply_id.map(str::to_owned),
            body: body.to_owned(),
        })),
    }
}

/// Build a `SubmitTransition` envelope — close or reopen.
///
/// `event` accepts CLOSED and REOPENED only; TOMBSTONED is not authorable and
/// the relay refuses it. The note rides the transition record rather than a
/// transient observer event, so it survives a rebuild from segments.
#[must_use]
pub fn submit_transition_envelope(
    client_ref: &str,
    space_id: &str,
    signal_id: &str,
    event: sync::SignalEventType,
    close_reason: Option<sync::CloseReason>,
    note: Option<&str>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::SubmitTransition(sync::SubmitTransition {
            client_ref: client_ref.to_owned(),
            space_id: space_id.to_owned(),
            signal_id: signal_id.to_owned(),
            event: i32::from(event),
            close_reason: close_reason.map(i32::from),
            note: note.map(str::to_owned),
        })),
    }
}

/// Build a `SignalReseed` envelope — replication push.
///
/// Carries whole RECORDS, unlike the authored frames above: replication moves
/// records that another relay already minted, and re-stamping them would
/// destroy the history being replicated.
#[must_use]
pub fn signal_reseed_envelope(
    client_ref: &str,
    space_id: &str,
    records: Vec<sync::Signal>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::SignalReseed(sync::SignalReseed {
            records,
            space_id: space_id.to_owned(),
            client_ref: client_ref.to_owned(),
        })),
    }
}

/// The typed `Audience` for an authored signal, derived from whether a target
/// was named. Shared by the builders above so the two cannot disagree.
fn audience_for(target_did: Option<&str>) -> sync::Audience {
    let to = match target_did {
        Some(did) => sync::audience::To::Participant(sync::audience::Participant {
            did: did.to_owned(),
        }),
        None => sync::audience::To::Space(sync::audience::Space {}),
    };
    sync::Audience { to: Some(to) }
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
/// `provenance` carries source provenance — `Default::default()` is fine
/// for callers that have none.
pub fn register_document_envelope(
    space_id: &str,
    document_id: &str,
    path: &str,
    metadata: Option<sync::ChangeMetadata>,
    provenance: RegisterDocumentMetadata,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::RegisterDocument(RegisterDocument {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            path: path.to_owned(),
            metadata,
            originally_created_at_ms: provenance.originally_created_at_ms,
            source_kind: provenance.source_kind,
            source_id: provenance.source_id,
            source_url: provenance.source_url,
            ingestion_job_id: provenance.ingestion_job_id,
            source_author_display: provenance.source_author_display,
            // UX-only metadata threaded through to the commercial relay's
            // `documents` mirror INSERT. Daemon-path callers leave these
            // absent; the web UX server populates them on document creation.
            title: None,
            content_type: None,
            // UX-only convert-path metadata, populated only by
            // web/ingestion-worker convert flows. Daemon-path callers
            // leave these absent.
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
    rename_causal_floor: Option<sync::Hlc>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::RenameDocument(RenameDocument {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            old_path: old_path.to_owned(),
            new_path: new_path.to_owned(),
            metadata,
            rename_causal_floor,
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

/// The fail-closed reading of a `SubscribeStatus` frame, shared by every
/// subscribing client.
///
/// `Some(reason)` when the relay classified the document's read as
/// compromised (`Inconsistent` / `Unavailable`) — the relay short-circuits
/// and no catch-up `SyncOps` follows, so a subscriber that ignores the
/// frame cannot distinguish "empty document" from "wounded slot" and
/// silently treats damage as emptiness. `None` for
/// Found/Empty/Unspecified, where catch-up (or genuine emptiness) follows
/// normally. One classifier for all consumers: per-client readings of
/// this frame drift, and a consumer with no reading fails open.
pub fn subscribe_status_failure(s: &sync::SubscribeStatus) -> Option<String> {
    match sync::LoadStatus::try_from(s.load_status) {
        Ok(sync::LoadStatus::Inconsistent) => Some(format!(
            "relay classified the read as inconsistent (witness expected_version {}, backend returned no content)",
            s.expected_version
        )),
        Ok(sync::LoadStatus::Unavailable) => Some(format!(
            "relay classified the read as unavailable ({})",
            s.reason
        )),
        _ => None,
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

/// Build a `RegisterDocumentAck` envelope.
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
    hlc: Option<sync::Hlc>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::RegisterDocumentAck(sync::RegisterDocumentAck {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            entry,
            error,
            success,
            hlc,
        })),
    }
}

/// Build a `RenameDocumentAck` envelope.
///
/// Emitted after both the in-memory rename AND the `RegistryBackend::save_entry`
/// mirror have committed. On success, `entry` carries the persisted snapshot
/// whose `path` is the post-arbitration effective path — the renamer
/// reconciles its local file to it (a rename that lost a path collision is
/// conflict-copied, so the effective path differs from the requested one).
pub fn rename_document_ack_envelope(
    space_id: &str,
    document_id: &str,
    success: bool,
    entry: Option<sync::RegistryEntry>,
    error: Option<sync::Error>,
    hlc: Option<sync::Hlc>,
) -> SyncEnvelope {
    SyncEnvelope {
        payload: Some(Payload::RenameDocumentAck(sync::RenameDocumentAck {
            space_id: space_id.to_owned(),
            document_id: document_id.to_owned(),
            error,
            success,
            entry,
            hlc,
        })),
    }
}

/// Build an `UnregisterDocumentAck` envelope.
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

/// Build an `UnregisterSpaceAck` envelope.
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

/// Build a `TransferSpaceOwnershipAck` envelope.
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
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let envelope = handshake_envelope("d5fe8251-3196-4a97-8d81-66092b9a47dc");
        let bytes = encode_envelope(&envelope);
        let decoded = decode_envelope(&bytes).unwrap();
        assert_eq!(envelope, decoded);
    }

    #[test]
    fn test_sync_ops_envelope() {
        let envelope = sync_ops_envelope("space", "doc", vec![1, 2, 3], Vec::new(), HashMap::new());
        match envelope.payload {
            Some(Payload::SyncOps(ops)) => {
                assert_eq!(ops.space_id, "space");
                assert_eq!(ops.document_id, "doc");
                assert_eq!(ops.ops, vec![1, 2, 3]);
                assert!(
                    ops.author_by_agent_snapshot.is_empty(),
                    "no snapshot when none is passed"
                );
            }
            _ => panic!("expected SyncOps"),
        }
    }

    #[test]
    fn test_sync_ops_envelope_carries_author_snapshot() {
        let mut snapshot = HashMap::new();
        snapshot.insert("agent-x".to_owned(), "did:key:x".to_owned());
        let envelope = sync_ops_envelope("space", "doc", vec![1], Vec::new(), snapshot);
        match envelope.payload {
            Some(Payload::SyncOps(ops)) => {
                assert_eq!(
                    ops.author_by_agent_snapshot.get("agent-x"),
                    Some(&"did:key:x".to_owned()),
                    "catch-up snapshot is set on the envelope"
                );
            }
            _ => panic!("expected SyncOps"),
        }
    }

    #[test]
    fn test_subscribe_envelope() {
        let envelope = subscribe_envelope("3f49dbbf-e051-4b20-8c03-8923424fedf8", "doc");
        match envelope.payload {
            Some(Payload::Subscribe(sub)) => {
                assert_eq!(sub.space_id, "3f49dbbf-e051-4b20-8c03-8923424fedf8");
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

        let text_explicit = sync::SyncOps {
            content_mode: sync::ContentMode::Text.into(),
            ..Default::default()
        };
        assert!(!is_blob_mode(&text_explicit));

        let blob_ops = sync::SyncOps {
            content_mode: sync::ContentMode::Blob.into(),
            ..Default::default()
        };
        assert!(is_blob_mode(&blob_ops));

        let blob_ref_ops = sync::SyncOps {
            content_mode: sync::ContentMode::BlobRef.into(),
            ..Default::default()
        };
        assert!(is_blob_mode(&blob_ref_ops));
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

    fn ack(protocol_version_major: u32, min_supported_major: u32) -> sync::HandshakeAck {
        sync::HandshakeAck {
            protocol_version_major,
            min_supported_major,
            ..Default::default()
        }
    }

    /// The refusal verdict is read from the relay's `ErrorCode`, never from its
    /// prose — a refused bearer and a refused protocol major arrive as the same
    /// frame and only the code separates them.
    #[test]
    fn test_handshake_refusal_reads_the_code_not_the_message() {
        let err = |code: sync::ErrorCode, message: &str| sync::Error {
            code: code.into(),
            message: message.to_owned(),
            ..Default::default()
        };
        // Identical text under two codes: only the code may decide.
        assert!(handshake_refusal(&err(sync::ErrorCode::AuthFailed, "rejected")).auth_failed);
        assert!(!handshake_refusal(&err(sync::ErrorCode::VersionMismatch, "rejected")).auth_failed);
        // Text that merely READS like an auth failure is not one.
        assert!(
            !handshake_refusal(&err(sync::ErrorCode::QuotaExceeded, "token not found")).auth_failed
        );
    }

    /// `details` rides the surfaced text, so a quota refusal names which cap it
    /// hit instead of logging a bare "quota exceeded".
    #[test]
    fn test_handshake_refusal_folds_in_details() {
        let plain = handshake_refusal(&sync::Error {
            code: sync::ErrorCode::QuotaExceeded.into(),
            message: "quota exceeded".to_owned(),
            ..Default::default()
        });
        assert_eq!(plain.message, "quota exceeded");

        let detailed = handshake_refusal(&sync::Error {
            code: sync::ErrorCode::QuotaExceeded.into(),
            message: "quota exceeded".to_owned(),
            details: r#"{"cap_kind":"op_count"}"#.to_owned(),
            ..Default::default()
        });
        assert_eq!(
            detailed.message,
            r#"quota exceeded ({"cap_kind":"op_count"})"#
        );
    }

    #[test]
    fn test_verify_ack_versions_accepts_matching_relay() {
        assert!(
            verify_ack_versions(&ack(PROTOCOL_VERSION_MAJOR, MIN_SUPPORTED_PROTOCOL_MAJOR)).is_ok()
        );
    }

    #[test]
    fn test_verify_ack_versions_refuses_old_relay() {
        let err = verify_ack_versions(&ack(PROTOCOL_VERSION_MAJOR - 1, 0)).unwrap_err();
        assert!(err.contains("upgrade the relay"), "{err}");
    }

    #[test]
    fn test_verify_ack_versions_refuses_when_client_below_floor() {
        let err = verify_ack_versions(&ack(PROTOCOL_VERSION_MAJOR + 1, PROTOCOL_VERSION_MAJOR + 1))
            .unwrap_err();
        assert!(err.contains("upgrade kutl"), "{err}");
    }

    /// Pin this crate's wire constants to the cross-language contract file
    /// (`oss/proto/kutl/sync/v1/contract.json`) — the rendezvous the
    /// TypeScript side is pinned to by its own gate. Drift on either side
    /// breaks that side's build instead of shipping a mismatch.
    #[test]
    fn test_constants_match_contract_file() {
        let raw = include_str!("../../../proto/kutl/sync/v1/contract.json");
        let contract: serde_json::Value = serde_json::from_str(raw).expect("contract.json parses");
        assert_eq!(
            contract["protocol_version_major"],
            serde_json::json!(PROTOCOL_VERSION_MAJOR)
        );
        assert_eq!(
            contract["protocol_version_minor"],
            serde_json::json!(PROTOCOL_VERSION_MINOR)
        );
        assert_eq!(
            contract["min_supported_protocol_major"],
            serde_json::json!(MIN_SUPPORTED_PROTOCOL_MAJOR)
        );
        assert_eq!(
            contract["absolute_blob_max"],
            serde_json::json!(ABSOLUTE_BLOB_MAX)
        );
    }
}
