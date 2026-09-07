//! Decoders for the shapes the daemon's files had before they carried the
//! envelope. Each decoder reads the old bytes into the current message,
//! names the layout version at which it must be deleted, and is exercised
//! against a fixture captured from the last release that wrote the old
//! shape. The live types know nothing of these shapes. The expiry test
//! calls every decoder's `assert_not_expired`: when a decoder's version
//! ships, the test fails until the decoder, its types, and its fixture are
//! gone.

use std::collections::HashMap;

use kutl_core::envelope::{Kind, Legacy};
use kutl_proto::daemon::{BlobEntry, BlobState};
use serde::Deserialize;

/// `.kutl/blob-state.json`, the pretty-printed JSON map that preceded
/// `blob-state.ksnap`.
pub(crate) const BLOB_STATE_V0: Legacy<BlobState> = Legacy {
    kind: Kind::BlobState,
    expires_at_version: 2,
    decode: blob_state_v0,
};

/// The v0 shape, frozen: relative path → `{hash, timestamp}`.
#[derive(Deserialize)]
struct BlobStateJsonV0 {
    #[serde(default)]
    entries: HashMap<String, BlobEntryJsonV0>,
}

#[derive(Deserialize)]
struct BlobEntryJsonV0 {
    hash: String,
    timestamp: i64,
}

fn blob_state_v0(bytes: &[u8]) -> Option<BlobState> {
    let v0: BlobStateJsonV0 = serde_json::from_slice(bytes).ok()?;
    Some(BlobState {
        entries: v0
            .entries
            .into_iter()
            .map(|(path, e)| {
                (
                    path,
                    BlobEntry {
                        hash: e.hash,
                        timestamp: e.timestamp,
                    },
                )
            })
            .collect(),
    })
}

// ---- state.json and identity.log: the daemon's identity state before the envelope ----

use kutl_proto::daemon::{
    DaemonState as DaemonStateProto, DocEntry as DocEntryProto, HlcFloor as HlcFloorProto,
    IdentityRecord, IdentityRemoval, IdentitySnapshot, RegisterHlc as RegisterHlcProto,
    WriteIntent, identity_record,
};

/// `.kutl/state.json`, the pretty-printed JSON snapshot that preceded
/// `state.ksnap`. Two shapes were ever written: the field-for-field map
/// below, and before it a bare path → id map with a parallel list of
/// relay-confirmed ids.
pub(crate) const STATE_V0: Legacy<DaemonStateProto> = Legacy {
    kind: Kind::State,
    expires_at_version: 2,
    decode: state_v0,
};

/// `.kutl/identity.log`, the JSON Lines journal that preceded
/// `identity.klog`: one object per line, a snapshot or removal carrying
/// `entry`, or a write intent carrying only `path` and `pending_write`.
/// Replay stopped at the first unparseable line, and so does this decoder.
pub(crate) const IDENTITY_LOG_V0: Legacy<Vec<IdentityRecord>> = Legacy {
    kind: Kind::IdentityLog,
    expires_at_version: 2,
    decode: identity_log_v0,
};

#[derive(Deserialize)]
struct DocEntryJsonV0 {
    id: String,
    confirmed: bool,
    #[serde(default)]
    inode: Option<u64>,
    #[serde(default)]
    last_written_hash: Option<String>,
}

#[derive(Deserialize)]
struct HlcFloorJsonV0 {
    physical_ms: u64,
    logical: u32,
}

#[derive(Deserialize)]
struct RegisterHlcJsonV0 {
    physical_ms: u64,
    logical: u32,
    actor: String,
}

#[derive(Deserialize)]
struct DaemonStateJsonV0 {
    #[serde(default)]
    documents: HashMap<String, DocEntryJsonV0>,
    #[serde(default)]
    device_id: Option<String>,
    #[serde(default)]
    hlc_floor: Option<HlcFloorJsonV0>,
    #[serde(default)]
    register_hlc: HashMap<String, RegisterHlcJsonV0>,
    #[serde(default)]
    pending_writes: HashMap<String, String>,
    #[serde(default)]
    at_op_cap: Vec<String>,
    #[serde(default)]
    approaching_op_cap: Vec<String>,
}

/// The shape before `DaemonStateJsonV0`: path → id, plus the ids the relay
/// had confirmed.
#[derive(Deserialize)]
struct DaemonStateJsonPreV0 {
    documents: HashMap<String, String>,
    #[serde(default)]
    remote_document_ids: Vec<String>,
}

#[derive(Deserialize)]
struct IdentityJournalLineJsonV0 {
    path: String,
    #[serde(default)]
    entry: Option<DocEntryJsonV0>,
    #[serde(default)]
    register_hlc: Option<RegisterHlcJsonV0>,
    #[serde(default)]
    hlc_floor: Option<HlcFloorJsonV0>,
    #[serde(default)]
    pending_write: Option<String>,
}

impl From<DocEntryJsonV0> for DocEntryProto {
    fn from(e: DocEntryJsonV0) -> Self {
        Self {
            id: e.id,
            confirmed: e.confirmed,
            inode: e.inode,
            last_written_hash: e.last_written_hash,
        }
    }
}

impl From<HlcFloorJsonV0> for HlcFloorProto {
    fn from(f: HlcFloorJsonV0) -> Self {
        Self {
            physical_ms: f.physical_ms,
            logical: f.logical,
        }
    }
}

impl From<RegisterHlcJsonV0> for RegisterHlcProto {
    fn from(r: RegisterHlcJsonV0) -> Self {
        Self {
            physical_ms: r.physical_ms,
            logical: r.logical,
            actor: r.actor,
        }
    }
}

fn state_v0(bytes: &[u8]) -> Option<DaemonStateProto> {
    if let Ok(v0) = serde_json::from_slice::<DaemonStateJsonV0>(bytes) {
        return Some(DaemonStateProto {
            documents: v0
                .documents
                .into_iter()
                .map(|(p, e)| (p, e.into()))
                .collect(),
            device_id: v0.device_id,
            hlc_floor: v0.hlc_floor.map(Into::into),
            register_hlc: v0
                .register_hlc
                .into_iter()
                .map(|(id, r)| (id, r.into()))
                .collect(),
            pending_writes: v0.pending_writes,
            at_op_cap: v0.at_op_cap,
            approaching_op_cap: v0.approaching_op_cap,
        });
    }
    let pre: DaemonStateJsonPreV0 = serde_json::from_slice(bytes).ok()?;
    let confirmed: std::collections::HashSet<String> =
        pre.remote_document_ids.into_iter().collect();
    Some(DaemonStateProto {
        documents: pre
            .documents
            .into_iter()
            .map(|(path, id)| {
                let is_confirmed = confirmed.contains(&id);
                (
                    path,
                    DocEntryProto {
                        id,
                        confirmed: is_confirmed,
                        inode: None,
                        last_written_hash: None,
                    },
                )
            })
            .collect(),
        ..Default::default()
    })
}

fn identity_log_v0(bytes: &[u8]) -> Option<Vec<IdentityRecord>> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut records = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let Ok(v0) = serde_json::from_str::<IdentityJournalLineJsonV0>(line) else {
            // The pre-envelope rule: replay stops at the first unparseable
            // line, keeping everything before it.
            break;
        };
        let kind = if let Some(hash) = v0.pending_write {
            identity_record::Kind::WriteIntent(WriteIntent { hash })
        } else if let Some(entry) = v0.entry {
            identity_record::Kind::Snapshot(IdentitySnapshot {
                entry: Some(entry.into()),
                register_hlc: v0.register_hlc.map(Into::into),
            })
        } else {
            identity_record::Kind::Removal(IdentityRemoval {})
        };
        records.push(IdentityRecord {
            path: v0.path,
            kind: Some(kind),
            hlc_floor: v0.hlc_floor.map(Into::into),
        });
    }
    Some(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_decoder_has_expired() {
        BLOB_STATE_V0.assert_not_expired();
        STATE_V0.assert_not_expired();
        IDENTITY_LOG_V0.assert_not_expired();
    }

    /// The fixture is the file the last pre-envelope release wrote for one
    /// synced blob; it must keep decoding until the window closes.
    #[test]
    fn test_blob_state_v0_fixture_decodes() {
        let bytes = include_bytes!("../legacy-fixtures/v0/blob-state.json");
        let state = (BLOB_STATE_V0.decode)(bytes).expect("v0 blob state decodes");
        let entry = state
            .entries
            .get("image.png")
            .expect("the fixture's one entry");
        assert_eq!(entry.timestamp, 12345);
        assert_eq!(entry.hash.len(), 64);
    }

    /// The fixture is the `state.json` the last pre-envelope release wrote:
    /// one confirmed document with an inode and a written hash, a device
    /// id, a clock floor, a register stamp, a pending write, and an op-cap
    /// flag; every field must come through.
    #[test]
    fn test_state_v0_fixture_decodes() {
        let bytes = include_bytes!("../legacy-fixtures/v0/state.json");
        let state = (STATE_V0.decode)(bytes).expect("v0 state decodes");
        let entry = state
            .documents
            .get("doc.md")
            .expect("the fixture's document");
        assert_eq!(entry.id, "0f0f0f0f-0000-4000-8000-000000000001");
        assert!(entry.confirmed);
        assert_eq!(entry.inode, Some(4242));
        assert_eq!(entry.last_written_hash.as_deref().map(str::len), Some(64));
        assert_eq!(
            state.device_id.as_deref(),
            Some("0f0f0f0f-0000-4000-8000-0000000000dd")
        );
        assert_eq!(
            state.hlc_floor,
            Some(HlcFloorProto {
                physical_ms: 1_700_000_000_000,
                logical: 3
            })
        );
        assert_eq!(
            state
                .register_hlc
                .get("0f0f0f0f-0000-4000-8000-000000000001"),
            Some(&RegisterHlcProto {
                physical_ms: 1_699_999_999_000,
                logical: 1,
                actor: "0f0f0f0f-0000-4000-8000-0000000000dd".to_owned()
            }),
            "the register stamp comes through whole, actor included"
        );
        assert_eq!(
            state.pending_writes.get("pending.md").map(String::as_str),
            Some("ab".repeat(32).as_str())
        );
        assert_eq!(
            state.at_op_cap,
            vec!["0f0f0f0f-0000-4000-8000-000000000001"]
        );
    }

    /// The shape before that: a bare path → id map with a parallel list of
    /// relay-confirmed ids.
    #[test]
    fn test_state_pre_v0_fixture_decodes() {
        let bytes = include_bytes!("../legacy-fixtures/v0/state-pre-v0.json");
        let state = (STATE_V0.decode)(bytes).expect("pre-v0 state decodes");
        assert!(state.documents.get("a.md").expect("a.md").confirmed);
        assert!(!state.documents.get("b.md").expect("b.md").confirmed);
        assert!(state.device_id.is_none());
    }

    /// The fixture is an `identity.log` tail the last pre-envelope release
    /// left: a write intent, a snapshot, a removal, then a torn line.
    #[test]
    fn test_identity_log_v0_fixture_decodes_and_stops_at_the_tear() {
        let bytes = include_bytes!("../legacy-fixtures/v0/identity.log");
        let records = (IDENTITY_LOG_V0.decode)(bytes).expect("v0 journal decodes");
        assert_eq!(
            records.len(),
            3,
            "three whole lines, the torn fourth dropped"
        );
        assert!(matches!(
            records[0].kind,
            Some(identity_record::Kind::WriteIntent(_))
        ));
        assert!(matches!(
            records[1].kind,
            Some(identity_record::Kind::Snapshot(_))
        ));
        assert!(matches!(
            records[2].kind,
            Some(identity_record::Kind::Removal(_))
        ));
        assert_eq!(records[0].path, "doc.md");
    }
}
