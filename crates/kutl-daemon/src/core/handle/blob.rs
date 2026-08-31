//! Blob family: the local blob-change half, the remote blob LWW merge, and the
//! pure LWW acceptance rule.

use kutl_proto::sync::ChangeMetadata;
use tracing::debug;

use super::helpers::{is_untracked_uuid, stamp_metadata};
use crate::blob_state::{BlobState, sha256_bytes, sha256_hex};
use crate::core::{Effect, EventStamp, SpaceState};

/// The LOCAL blob content half — the pure port of the former
/// `handle_blob_change`, minus register/subscribe (the shared mint owns those)
/// and minus the size cap (enforced at the classify edge, where the bytes are
/// read). Hash-compares against the LWW record, ships the full bytes, and
/// updates the record; `Effect::SaveBlobState` persists it.
pub(super) fn blob_change(
    state: &mut SpaceState,
    document_id: &str,
    rel: &std::path::Path,
    content: &[u8],
    stamp: EventStamp,
) -> Vec<Effect> {
    let hash_hex = sha256_hex(content);
    if let Some(existing) = state.blob_state.get(rel)
        && existing.hash == hash_hex
    {
        // Unchanged bytes (e.g. a touch or an echo): nothing to ship.
        return Vec::new();
    }
    let (meta, _hlc) = stamp_metadata(state, "file change", stamp);
    let timestamp = meta.timestamp;
    state.blob_state.insert(
        rel.to_path_buf(),
        BlobState {
            hash: hash_hex,
            timestamp,
        },
    );
    vec![
        Effect::SendOps {
            document_id: document_id.to_owned(),
            ops: content.to_vec(),
            metadata: vec![meta],
            content_mode: i32::from(kutl_proto::sync::ContentMode::Blob),
            content_hash: sha256_bytes(content),
        },
        Effect::SaveBlobState,
    ]
}

/// The REMOTE blob merge — the pure port of the former `handle_remote_blob`.
/// The LWW decision is [`blob_lww_accepts`]; an accepted blob writes through
/// the SHARED `Effect::WriteFile` funnel, so echo suppression and the
/// shadow/inode folds ride the write ACK exactly like text (the hand-rolled
/// folds the imperative handler carried are gone). The path resolves
/// prefer-shadow (`SpaceState::doc_disk_path`) — a displaced blob's bytes land
/// at its conflict path, not the identity path.
pub(super) fn merge_remote_blob(
    state: &mut SpaceState,
    document_id: &str,
    ops: &[u8],
    metadata: &[ChangeMetadata],
) -> Vec<Effect> {
    // Empty ops are a catch-up signal for documents with no content yet.
    if ops.is_empty() {
        return Vec::new();
    }
    // Same §1.5 hazard as the text path: an op for an untracked UUID must not
    // materialize; content is re-delivered by the post-register catch-up.
    if is_untracked_uuid(state, document_id) {
        debug!(%document_id, "skipping remote blob op for untracked document");
        return Vec::new();
    }
    let Some(rel) = state.doc_disk_path(document_id) else {
        debug!(%document_id, "no local path for remote blob op; ignoring");
        return Vec::new();
    };
    let remote_timestamp = metadata.first().map_or(0, |m| m.timestamp);
    let remote_hash = sha256_hex(ops);
    if !blob_lww_accepts(state.blob_state.get(&rel), remote_timestamp, &remote_hash) {
        debug!(
            %document_id,
            rel = %rel.display(),
            remote_timestamp,
            "gate drop: stale remote blob (LWW)"
        );
        return Vec::new();
    }
    state.blob_state.insert(
        rel.clone(),
        BlobState {
            hash: remote_hash,
            timestamp: remote_timestamp,
        },
    );
    vec![
        Effect::WriteFile {
            rel,
            content: ops.to_vec(),
        },
        Effect::SaveBlobState,
    ]
}

/// The pure blob LWW rule (the allowed blob residue): a newer timestamp wins;
/// equal timestamps tiebreak on the lexicographically GREATER content hash, so
/// two daemons writing different blobs in the same millisecond converge on one
/// of them instead of mutually rejecting (permanent divergence). Exactly the
/// former `handle_remote_blob` acceptance, inverted from its reject form.
fn blob_lww_accepts(
    existing: Option<&BlobState>,
    remote_timestamp: i64,
    remote_hash: &str,
) -> bool {
    existing.is_none_or(|e| {
        remote_timestamp > e.timestamp
            || (remote_timestamp == e.timestamp && remote_hash > e.hash.as_str())
    })
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::core::handle::test_support::{doc_id, register_identity_only, st};
    use crate::core::{DaemonCore, Event};

    // ── blob LWW (Task 6b: the blob path on the core) ──

    /// The pure LWW rule's full truth table: newer wins, older loses, equal
    /// timestamps tiebreak on the lexicographically greater hash (equal hash
    /// = the same bytes = reject the redundant write), absent state accepts.
    #[test]
    fn test_blob_lww_accepts_truth_table() {
        let existing = BlobState {
            hash: "bb".into(),
            timestamp: 100,
        };
        assert!(blob_lww_accepts(None, 0, "aa"), "no prior state accepts");
        assert!(blob_lww_accepts(Some(&existing), 101, "aa"), "newer wins");
        assert!(!blob_lww_accepts(Some(&existing), 99, "zz"), "older loses");
        assert!(
            blob_lww_accepts(Some(&existing), 100, "cc"),
            "equal ts, greater hash wins"
        );
        assert!(
            !blob_lww_accepts(Some(&existing), 100, "aa"),
            "equal ts, lesser hash loses"
        );
        assert!(
            !blob_lww_accepts(Some(&existing), 100, "bb"),
            "equal ts, equal hash (same bytes) is a redundant write"
        );
    }

    /// An accepted remote blob writes through the SHARED `WriteFile` funnel
    /// (suppress hash inline; shadow/inode folds ride the ACK) and persists
    /// the LWW record via `SaveBlobState` — the former hand-rolled folds of
    /// `handle_remote_blob` are gone.
    #[test]
    fn test_remote_blob_accept_writes_via_shared_funnel() {
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "img.png");
        let bytes = b"\x89PNG-bytes".to_vec();
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id.clone(),
                ops: bytes.clone(),
                metadata: vec![ChangeMetadata {
                    timestamp: 500,
                    ..Default::default()
                }],
                content_mode: i32::from(kutl_proto::sync::ContentMode::Blob),
                local_content: None,
                author_by_agent_snapshot: std::collections::HashMap::new(),
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::WriteFile { rel, content }
                    if rel.as_path() == Path::new("img.png") && *content == bytes
            )),
            "accepted blob writes via the shared funnel, got {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(e, Effect::SaveBlobState)),
            "the LWW record persists via SaveBlobState"
        );
        let rec = s
            .blob_state
            .get(Path::new("img.png"))
            .expect("LWW recorded");
        assert_eq!(rec.timestamp, 500);
    }

    /// A stale remote blob (older timestamp) is gated out: no effects, the
    /// recorded LWW state unchanged.
    #[test]
    fn test_remote_blob_stale_is_dropped_by_lww_gate() {
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "img.png");
        s.blob_state.insert(
            PathBuf::from("img.png"),
            BlobState {
                hash: "ff".into(),
                timestamp: 1_000,
            },
        );
        let effects = DaemonCore::handle(
            &mut s,
            Event::RemoteOps {
                document_id: id,
                ops: b"old".to_vec(),
                metadata: vec![ChangeMetadata {
                    timestamp: 999,
                    ..Default::default()
                }],
                content_mode: i32::from(kutl_proto::sync::ContentMode::Blob),
                local_content: None,
                author_by_agent_snapshot: std::collections::HashMap::new(),
                stamp: EventStamp {
                    wall_ms: 50,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects.is_empty(),
            "a stale blob is gated out, got {effects:?}"
        );
        assert_eq!(
            s.blob_state.get(Path::new("img.png")).map(|b| b.timestamp),
            Some(1_000),
            "the LWW record is unchanged"
        );
    }

    /// A LOCAL binary create takes the SHARED mint (identity, shadow fold,
    /// register, explicit subscribe, persist) and ships the bytes as a blob
    /// `SendOps` — content mode only picks the content rule.
    #[test]
    fn test_local_binary_create_shares_the_mint_and_ships_blob() {
        let mut s = st();
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("img.png"),
                content: Some(vec![0x89, b'P', b'N', b'G', 0xFF]),
                stamp: EventStamp {
                    wall_ms: 100,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::RegisterDocument { path, .. } if path == "img.png")),
            "binary create registers via the shared mint, got {effects:?}"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Subscribe { .. })),
            "binary create subscribes explicitly at mint"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::SendOps { content_mode, ops, .. }
                    if *content_mode == i32::from(kutl_proto::sync::ContentMode::Blob)
                        && !ops.is_empty()
            )),
            "binary create ships its bytes as a blob SendOps"
        );
        assert!(
            s.blob_state.get(Path::new("img.png")).is_some(),
            "the local LWW record is written at mint"
        );
        // An unchanged re-save (same bytes) ships nothing.
        let effects = DaemonCore::handle(
            &mut s,
            Event::FileModified {
                rel: PathBuf::from("img.png"),
                content: Some(vec![0x89, b'P', b'N', b'G', 0xFF]),
                stamp: EventStamp {
                    wall_ms: 200,
                    origin_hlc: None,
                },
            },
        );
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::SendOps { .. })),
            "an unchanged blob re-save ships nothing, got {effects:?}"
        );
    }
}
