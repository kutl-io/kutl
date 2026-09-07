//! Shared test fixtures for the `handle` child modules' test mods.

use std::path::PathBuf;

use kutl_proto::sync::ChangeMetadata;
use uuid::Uuid;

use super::{DaemonCore, register_identity};
use crate::core::{Event, EventStamp, SpaceState};

pub(super) fn st() -> SpaceState {
    SpaceState::new_for_test("space-1".into(), PathBuf::from("/tmp/x"), "did:a".into())
}

/// Register a remote doc at `path` and SIMULATE its placement landing, but with
/// NO file on disk yet (identity claimed, shadow occupant UNSET) — a
/// registered+subscribed doc whose content has not streamed in. The first-
/// materialization case where a remote merge must WRITE the file. The on-disk
/// variant is [`track_remote`] (which also marks the shadow `Tracked`).
///
/// Identity follows placement now (carried concern 1), so a bare
/// `RemoteRegister` no longer claims identity; this drives the place explicitly.
pub(super) fn register_identity_only(s: &mut SpaceState, id: &str, path: &str) {
    let actor = s.hlc.last().actor;
    let _ = DaemonCore::handle(
        s,
        Event::RemoteRegister {
            document_id: id.to_owned(),
            path: path.to_owned(),
            stamp: EventStamp {
                wall_ms: 10,
                origin_hlc: Some(hlc(actor, 10)),
            },
        },
    );
    register_identity(
        s,
        &PathBuf::from(path),
        id.to_owned(),
        /* confirmed */ true,
    );
}

pub(super) fn hlc(actor: kutl_core::hlc::ActorId, ms: u64) -> kutl_core::Hlc {
    kutl_core::Hlc {
        physical_ms: ms,
        logical: 0,
        actor,
    }
}

/// A valid UUID document id (the lattice keys by `Uuid`, so the lifecycle
/// arms only fold records for parseable ids).
pub(super) fn doc_id() -> String {
    "11111111-1111-4111-8111-111111111111".to_owned()
}

/// Track a remote document at `rel` (register places + subscribes it) and
/// reflect that placement in the shadow so the cascade does not re-subscribe
/// and `should_skip_remote_write` sees the file "on disk".
pub(super) fn track_remote(s: &mut SpaceState, id: &str, rel: &str, ms: u64) {
    let actor = s.hlc.last().actor;
    let _ = DaemonCore::handle(
        s,
        Event::RemoteRegister {
            document_id: id.to_owned(),
            path: rel.to_owned(),
            stamp: EventStamp {
                wall_ms: ms,
                origin_hlc: Some(hlc(actor, ms)),
            },
        },
    );
    // SIMULATE the placement landing (`GuardedPlace(Register)` ACK): identity
    // follows placement now (carried concern 1), so a bare `RemoteRegister` no
    // longer claims `file_identity`/`uuid_to_path` — the shell does on the place.
    // Claim it + mark the shadow `Tracked` (the file is materialized on disk).
    let path = PathBuf::from(rel);
    register_identity(s, &path, id.to_owned(), /* confirmed */ true);
    s.shadow.set_tracked(&path, Uuid::parse_str(id).unwrap());
}

/// Encode a fresh remote document's full op stream + metadata for a merge —
/// the wire payload a peer's `RemoteOps` would carry.
pub(super) fn remote_ops_for(content: &str) -> (Vec<u8>, Vec<ChangeMetadata>) {
    let mut doc = kutl_core::Document::new();
    let agent = doc.register_agent("peer").unwrap();
    crate::bridge::apply_file_change(&mut doc, agent, "did:peer", content).unwrap();
    doc.delta_since(&[])
}

/// Fill a doc's CRDT to exactly [`kutl_core::MAX_OPS_PER_DOC`] via large
/// block inserts (ops are per-character; blocks keep this fast).
pub(super) fn fill_doc_to_op_cap(s: &mut SpaceState, id: &str) {
    /// Chars per insert; each CHURN cycle (insert + delete of the same span)
    /// consumes `2 × FILL_BLOCK` ops while the CONTENT stays one block —
    /// tests after the fill read/diff the content, and an insert-only fill
    /// would hand them a cap-sized (multi-MB) string.
    const FILL_BLOCK: usize = 50_000;
    let doc = s.load_or_create_doc(id);
    let agent = doc.register_agent("filler").unwrap();
    let block = "a".repeat(FILL_BLOCK);
    doc.edit(agent, "filler", "seed", kutl_core::Boundary::Auto, |ctx| {
        ctx.insert(0, &block)
    })
    .unwrap();
    while doc.op_count() < kutl_core::MAX_OPS_PER_DOC {
        doc.edit(agent, "filler", "fill", kutl_core::Boundary::Auto, |ctx| {
            ctx.insert(FILL_BLOCK, &block)?;
            ctx.delete(0..FILL_BLOCK)
        })
        .unwrap();
    }
    assert!(doc.op_count() >= kutl_core::MAX_OPS_PER_DOC);
}
