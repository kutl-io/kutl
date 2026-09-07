//! Blob family: the local blob-change half, the remote blob LWW merge, and the
//! pure LWW acceptance rule.

use kutl_proto::sync::ChangeMetadata;
use tracing::debug;

use super::helpers::{is_untracked_uuid, stamp_metadata};
use crate::blob_state::{BlobState, HashedContent};
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
    digest: Vec<u8>,
    stamp: EventStamp,
) -> Vec<Effect> {
    // `digest` is the caller's one pass over `content`: the identity record
    // took its hex, the LWW record takes it here, and the wire carries the
    // raw bytes.
    let hash_hex = kutl_proto::protocol::hex_encode(&digest);
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
            content_hash: digest,
        },
        Effect::SaveBlobState,
    ]
}

/// The REMOTE blob merge — the pure port of the former `handle_remote_blob`.
/// The LWW decision is [`blob_lww_accepts`]; an accepted blob writes through
/// the SHARED `Effect::WriteFile` funnel, so echo suppression and the
/// shadow/inode folds ride the write ACK exactly like text. The path resolves
/// prefer-shadow (`SpaceState::doc_disk_path`) — a displaced blob's bytes land
/// at its conflict path, not the identity path.
///
/// The accepted LWW record is staged in `pending_blob_state`, not committed:
/// `DaemonCore::apply_effect_result` moves it into `blob_state` when the
/// shell reports the write landed. A write the shell refuses (a read-only
/// file, a full disk) therefore leaves the committed record on the previous
/// bytes, and the relay's next redelivery of the same bytes passes the gate
/// and retries the write; a record committed here would gate that redelivery
/// out as redundant and keep the stale file forever. `SaveBlobState` after a
/// refused write rewrites the unchanged map.
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
    // Same hazard as the text path: an op for a UUID this replica holds no
    // path for (deleted locally, or not yet registered) must not materialize
    // a file; content is re-delivered by the post-register catch-up.
    if is_untracked_uuid(state, document_id) {
        debug!(%document_id, "skipping remote blob op for untracked document");
        return Vec::new();
    }
    let Some(rel) = state.doc_disk_path(document_id) else {
        debug!(%document_id, "no local path for remote blob op; ignoring");
        return Vec::new();
    };
    let remote_timestamp = metadata.first().map_or(0, |m| m.timestamp);
    // Hashed from the bytes this replica holds, never taken from the wire:
    // the equal-timestamp tie-break compares hashes, and a forwarded hash
    // could split replicas that hold the same bytes. The write funnel keys
    // its echo suppression on this same digest.
    let content = HashedContent::new(ops.to_vec());
    let remote_hash = content.hex();
    if !blob_lww_accepts(state.blob_state.get(&rel), remote_timestamp, &remote_hash) {
        debug!(
            %document_id,
            rel = %rel.display(),
            remote_timestamp,
            "gate drop: stale remote blob (LWW)"
        );
        return Vec::new();
    }
    state.pending_blob_state.insert(
        rel.clone(),
        BlobState {
            hash: remote_hash,
            timestamp: remote_timestamp,
        },
    );
    vec![Effect::WriteFile { rel, content }, Effect::SaveBlobState]
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
    use crate::core::{DaemonCore, EffectResult, Event};

    // ── blob LWW ──

    /// A remote blob delivery at `timestamp` for `id`.
    fn remote_blob(id: &str, bytes: &[u8], timestamp: i64) -> Event {
        Event::RemoteOps {
            document_id: id.to_owned(),
            ops: bytes.to_vec(),
            metadata: vec![ChangeMetadata {
                timestamp,
                ..Default::default()
            }],
            content_mode: i32::from(kutl_proto::sync::ContentMode::Blob),
            local_content: None,
            author_by_agent_snapshot: std::collections::HashMap::new(),
            stamp: EventStamp {
                wall_ms: 50,
                origin_hlc: None,
            },
        }
    }

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
    /// the LWW record via `SaveBlobState`; the record itself is staged until
    /// the write's `FileWritten` fold commits it.
    #[test]
    fn test_remote_blob_accept_writes_via_shared_funnel() {
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "img.png");
        let bytes = b"\x89PNG-bytes".to_vec();
        let effects = DaemonCore::handle(&mut s, remote_blob(&id, &bytes, 500));
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::WriteFile { rel, content }
                    if rel.as_path() == Path::new("img.png") && content.bytes() == bytes
            )),
            "accepted blob writes via the shared funnel, got {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(e, Effect::SaveBlobState)),
            "the LWW record persists via SaveBlobState"
        );
        assert!(
            s.blob_state.get(Path::new("img.png")).is_none(),
            "nothing is committed before the write lands"
        );
        assert_eq!(
            s.pending_blob_state
                .get(Path::new("img.png"))
                .map(|b| b.timestamp),
            Some(500),
            "the accepted record is staged for the write's fold"
        );
        DaemonCore::apply_effect_result(
            &mut s,
            EffectResult::FileWritten {
                rel: PathBuf::from("img.png"),
            },
        );
        let rec = s
            .blob_state
            .get(Path::new("img.png"))
            .expect("LWW recorded once the write landed");
        assert_eq!(rec.timestamp, 500);
    }

    /// The LWW record commits only when the write lands. While no
    /// `FileWritten` fold has arrived — the shell refused the write — the
    /// committed record is unchanged and a redelivery of the same bytes still
    /// passes the gate, so the relay's next catch-up retries the write
    /// instead of finding it recorded as synced and gated out forever. The
    /// fold then commits the staged record, after which the same bytes are
    /// the redundant write they should be.
    #[test]
    fn test_remote_blob_record_commits_only_on_landed_write() {
        let mut s = st();
        let id = doc_id();
        register_identity_only(&mut s, &id, "img.png");
        s.blob_state.insert(
            PathBuf::from("img.png"),
            BlobState {
                hash: "aa".into(),
                timestamp: 100,
            },
        );
        let bytes = b"\x89PNG-newer".to_vec();
        let writes = |effects: &[Effect]| {
            effects.iter().any(|e| {
                matches!(
                    e,
                    Effect::WriteFile { rel, content }
                        if rel.as_path() == Path::new("img.png") && content.bytes() == bytes
                )
            })
        };

        let first = DaemonCore::handle(&mut s, remote_blob(&id, &bytes, 500));
        assert!(writes(&first), "the newer blob is accepted, got {first:?}");
        assert_eq!(
            s.blob_state.get(Path::new("img.png")).map(|b| b.timestamp),
            Some(100),
            "no write landed: the committed record is unchanged"
        );

        // The write was refused (no fold); the same bytes are delivered again.
        let again = DaemonCore::handle(&mut s, remote_blob(&id, &bytes, 500));
        assert!(
            writes(&again),
            "a redelivery is accepted while the record is uncommitted, got {again:?}"
        );

        DaemonCore::apply_effect_result(
            &mut s,
            EffectResult::FileWritten {
                rel: PathBuf::from("img.png"),
            },
        );
        let rec = s
            .blob_state
            .get(Path::new("img.png"))
            .expect("the fold committed the record");
        assert_eq!(rec.timestamp, 500);
        assert_eq!(rec.hash, HashedContent::new(bytes.clone()).hex());
        assert!(
            s.pending_blob_state.is_empty(),
            "the fold consumed the staged record"
        );

        let third = DaemonCore::handle(&mut s, remote_blob(&id, &bytes, 500));
        assert!(
            third.is_empty(),
            "once committed, the same bytes are a redundant write, got {third:?}"
        );
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
        let effects = DaemonCore::handle(&mut s, remote_blob(&id, b"old", 999));
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
