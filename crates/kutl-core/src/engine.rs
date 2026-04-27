//! Diamond-types engine wrapper for CRDT operations.

use std::ops::Range;

use diamond_types::AgentId;
use diamond_types::list::encoding::{ENCODE_FULL, ENCODE_PATCH};
use diamond_types::list::{Branch, OpLog};

use crate::error::{Error, Result};

/// Thin wrapper around diamond-types' `OpLog` and `Branch`.
///
/// `Engine` owns the operation log (the full history of edits) and a
/// branch (a materialised snapshot of the document at the current tip).
#[derive(Debug, Clone)]
pub struct Engine {
    oplog: OpLog,
    branch: Branch,
}

impl Engine {
    /// Create a new, empty engine.
    #[must_use]
    pub fn new() -> Self {
        let oplog = OpLog::new();
        let branch = Branch::new_at_tip(&oplog);
        Self { oplog, branch }
    }

    /// Return an existing agent id or create a new one.
    pub fn get_or_create_agent(&mut self, name: &str) -> AgentId {
        self.oplog.get_or_create_agent_id(name)
    }

    /// Return the current document content as a `String`.
    #[must_use]
    pub fn content(&self) -> String {
        self.branch.content().to_string()
    }

    /// Return the document length in unicode characters.
    #[must_use]
    pub fn len(&self) -> usize {
        self.branch.len()
    }

    /// Return `true` if the document is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.branch.is_empty()
    }

    /// Insert `text` at character position `pos`.
    ///
    /// Returns the number of characters inserted.
    pub fn insert(&mut self, agent: AgentId, pos: usize, text: &str) -> Result<usize> {
        let doc_len = self.branch.len();
        if pos > doc_len {
            return Err(Error::OutOfBounds { pos, len: doc_len });
        }
        self.branch.insert(&mut self.oplog, agent, pos, text);
        Ok(text.chars().count())
    }

    /// Delete the character range `start..end`.
    ///
    /// Returns the number of characters deleted.
    pub fn delete(&mut self, agent: AgentId, range: Range<usize>) -> Result<usize> {
        let doc_len = self.branch.len();
        if range.start >= range.end {
            return Err(Error::InvalidRange {
                start: range.start,
                end: range.end,
                len: doc_len,
            });
        }
        if range.end > doc_len {
            return Err(Error::InvalidRange {
                start: range.start,
                end: range.end,
                len: doc_len,
            });
        }
        let count = range.end - range.start;
        self.branch.delete(&mut self.oplog, agent, range);
        Ok(count)
    }

    /// Return the local version (a frontier of the operation DAG).
    #[must_use]
    pub fn local_version(&self) -> Vec<usize> {
        self.oplog.local_version_ref().to_vec()
    }

    /// Encode the full operation log to a binary blob (`.dt` format).
    #[must_use]
    pub fn encode_full(&self) -> Vec<u8> {
        self.oplog.encode(ENCODE_FULL)
    }

    /// Encode a delta containing only operations since `from_version`.
    #[must_use]
    pub fn encode_since(&self, from_version: &[usize]) -> Vec<u8> {
        self.oplog.encode_from(ENCODE_PATCH, from_version)
    }

    /// Load an engine from a full `.dt` binary blob.
    pub fn load_from(bytes: &[u8]) -> Result<Self> {
        let oplog = OpLog::load_from(bytes).map_err(Error::Decode)?;
        let branch = Branch::new_at_tip(&oplog);
        Ok(Self { oplog, branch })
    }

    /// Return the total number of CRDT operations in the log.
    #[must_use]
    pub fn op_count(&self) -> usize {
        self.oplog.len()
    }

    /// Merge a remote binary blob (full or delta) into this engine.
    ///
    /// Empty bytes are a no-op — the relay sends empty ops as catch-up for
    /// documents that exist but have no content yet.
    pub fn merge_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.oplog.decode_and_add(bytes).map_err(Error::Decode)?;
        self.branch
            .merge(&self.oplog, self.oplog.local_version_ref());
        Ok(())
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_engine_is_empty() {
        let engine = Engine::new();
        assert!(engine.is_empty());
        assert_eq!(engine.len(), 0);
        assert_eq!(engine.content(), "");
    }

    #[test]
    fn test_insert_and_content() {
        let mut engine = Engine::new();
        let agent = engine.get_or_create_agent("alice");
        engine.insert(agent, 0, "hello").unwrap();
        assert_eq!(engine.content(), "hello");
        assert_eq!(engine.len(), 5);
        assert!(!engine.is_empty());
    }

    #[test]
    fn test_insert_at_end() {
        let mut engine = Engine::new();
        let agent = engine.get_or_create_agent("alice");
        engine.insert(agent, 0, "hello").unwrap();
        engine.insert(agent, 5, " world").unwrap();
        assert_eq!(engine.content(), "hello world");
    }

    #[test]
    fn test_insert_out_of_bounds() {
        let mut engine = Engine::new();
        let agent = engine.get_or_create_agent("alice");
        let err = engine.insert(agent, 1, "x").unwrap_err();
        assert!(matches!(err, Error::OutOfBounds { pos: 1, len: 0 }));
    }

    #[test]
    fn test_delete() {
        let mut engine = Engine::new();
        let agent = engine.get_or_create_agent("alice");
        engine.insert(agent, 0, "hello world").unwrap();
        let deleted = engine.delete(agent, 5..11).unwrap();
        assert_eq!(deleted, 6);
        assert_eq!(engine.content(), "hello");
    }

    #[test]
    fn test_delete_invalid_range() {
        let mut engine = Engine::new();
        let agent = engine.get_or_create_agent("alice");
        engine.insert(agent, 0, "hello").unwrap();

        // Empty range
        let err = engine.delete(agent, 2..2).unwrap_err();
        assert!(matches!(err, Error::InvalidRange { .. }));

        // Reversed range — construct dynamically to avoid clippy::reversed_empty_ranges.
        let (start, end) = (3, 1);
        let err = engine.delete(agent, start..end).unwrap_err();
        assert!(matches!(err, Error::InvalidRange { .. }));

        // Past end
        let err = engine.delete(agent, 0..10).unwrap_err();
        assert!(matches!(err, Error::InvalidRange { .. }));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut engine = Engine::new();
        let agent = engine.get_or_create_agent("alice");
        engine.insert(agent, 0, "hello world").unwrap();

        let bytes = engine.encode_full();
        let restored = Engine::load_from(&bytes).unwrap();
        assert_eq!(restored.content(), "hello world");
    }

    #[test]
    fn test_version_advances() {
        let mut engine = Engine::new();
        let v0 = engine.local_version();

        let agent = engine.get_or_create_agent("alice");
        engine.insert(agent, 0, "hi").unwrap();
        let v1 = engine.local_version();

        assert_ne!(v0, v1);
    }

    #[test]
    fn test_concurrent_merge() {
        // Two engines diverge from a common ancestor, then converge.
        let mut engine_a = Engine::new();
        let agent_a = engine_a.get_or_create_agent("alice");
        engine_a.insert(agent_a, 0, "hello").unwrap();

        // Fork: engine_b starts from the same state as engine_a.
        let bytes_full = engine_a.encode_full();
        let mut engine_b = Engine::load_from(&bytes_full).unwrap();
        let agent_b = engine_b.get_or_create_agent("bob");

        // Record the common version before diverging.
        let common = engine_a.local_version();

        // Diverge.
        engine_a.insert(agent_a, 5, " alice").unwrap();
        engine_b.insert(agent_b, 5, " bob").unwrap();

        assert_ne!(engine_a.content(), engine_b.content());

        // Exchange deltas.
        let delta_a = engine_a.encode_since(&common);
        let delta_b = engine_b.encode_since(&common);

        engine_a.merge_bytes(&delta_b).unwrap();
        engine_b.merge_bytes(&delta_a).unwrap();

        // After merge, both must converge to the same content.
        assert_eq!(engine_a.content(), engine_b.content());
        // Content should contain both edits.
        let content = engine_a.content();
        assert!(content.contains("alice"), "missing alice in: {content}");
        assert!(content.contains("bob"), "missing bob in: {content}");
    }

    #[test]
    fn test_merge_full_blob() {
        let mut engine_a = Engine::new();
        let agent_a = engine_a.get_or_create_agent("alice");
        engine_a.insert(agent_a, 0, "hello").unwrap();

        let mut engine_b = Engine::new();
        let full = engine_a.encode_full();
        engine_b.merge_bytes(&full).unwrap();
        assert_eq!(engine_b.content(), "hello");
    }

    /// Reproduces the stress-test scenario: relay has ops from multiple agents;
    /// a client has only its own ops. Encoding the relay with
    /// `encode_since(&[])` (PATCH from empty) and merging into the partial
    /// client must succeed.
    #[test]
    fn test_patch_from_empty_into_partial_doc() {
        // Relay receives ops from alice and bob independently.
        let mut engine_relay = Engine::new();
        let relay_alice = engine_relay.get_or_create_agent("alice");
        let relay_bob = engine_relay.get_or_create_agent("bob");
        engine_relay.insert(relay_alice, 0, "hello").unwrap();
        engine_relay.insert(relay_bob, 5, " world").unwrap();

        // Client has only alice's ops (never received bob's).
        let mut engine_client = Engine::new();
        let client_alice = engine_client.get_or_create_agent("alice");
        engine_client.insert(client_alice, 0, "hello").unwrap();

        // Relay sends catch-up as PATCH from empty version.
        let patch_from_empty = engine_relay.encode_since(&[]);
        let result = engine_client.merge_bytes(&patch_from_empty);

        // This must succeed — encode_since(&[]) should be self-contained.
        assert!(
            result.is_ok(),
            "merge of encode_since(&[]) into partial doc failed: {:?}",
            result.err()
        );
        assert_eq!(engine_client.content(), engine_relay.content());
    }

    /// More realistic stress-test scenario: relay receives ops from clients
    /// via `merge_bytes` (not direct insert), then sends catch-up to a client.
    #[test]
    fn test_patch_from_empty_into_partial_doc_via_merge() {
        // Client A creates ops.
        let mut client_a = Engine::new();
        let agent_a = client_a.get_or_create_agent("alice");
        for i in 0..50 {
            let pos = client_a.len();
            client_a.insert(agent_a, pos, &format!("a{i}")).unwrap();
        }

        // Client B creates ops independently.
        let mut client_b = Engine::new();
        let agent_b = client_b.get_or_create_agent("bob");
        for i in 0..50 {
            let pos = client_b.len();
            client_b.insert(agent_b, pos, &format!("b{i}")).unwrap();
        }

        // Relay starts empty, receives ops from both clients.
        let mut relay = Engine::new();
        let ops_a = client_a.encode_since(&[]);
        let ops_b = client_b.encode_since(&[]);
        relay.merge_bytes(&ops_a).unwrap();
        relay.merge_bytes(&ops_b).unwrap();

        // Client A has only its own ops. Relay sends catch-up.
        let catch_up_patch = relay.encode_since(&[]);
        let result = client_a.merge_bytes(&catch_up_patch);
        assert!(
            result.is_ok(),
            "patch from empty via merge failed: {:?}",
            result.err()
        );
        assert_eq!(client_a.content(), relay.content());

        // Also test with encode_full.
        let mut client_a2 = Engine::new();
        let agent_a2 = client_a2.get_or_create_agent("alice");
        for i in 0..50 {
            let pos = client_a2.len();
            client_a2.insert(agent_a2, pos, &format!("a{i}")).unwrap();
        }
        let catch_up_full = relay.encode_full();
        let result = client_a2.merge_bytes(&catch_up_full);
        assert!(
            result.is_ok(),
            "full encode via merge failed: {:?}",
            result.err()
        );
        assert_eq!(client_a2.content(), relay.content());
    }

    /// Realistic stress-test scenario: 10 clients each with 100 ops,
    /// relay receives all ops via merge, some merges are partial.
    /// Then relay sends `encode_since(&[])` catch-up to a client.
    #[test]
    fn test_patch_from_empty_stress_simulation() {
        const NUM_CLIENTS: usize = 10;
        const OPS_PER_CLIENT: usize = 100;

        // Create independent client engines.
        let mut clients: Vec<Engine> = Vec::new();
        for i in 0..NUM_CLIENTS {
            let mut engine = Engine::new();
            let agent = engine.get_or_create_agent(&format!("client-{i}"));
            for j in 0..OPS_PER_CLIENT {
                let pos = engine.len();
                engine.insert(agent, pos, &format!("c{i}o{j}")).unwrap();
            }
            clients.push(engine);
        }

        // Relay starts empty, receives ops from all clients.
        let mut relay = Engine::new();
        for client in &clients {
            let ops = client.encode_since(&[]);
            relay.merge_bytes(&ops).unwrap();
        }

        // Client 0 simulates having received SOME relay messages (50%).
        // Pre-encode ops from clients 1-4 before borrowing client 0 mutably.
        let relay_blobs: Vec<Vec<u8>> = (1..5).map(|i| clients[i].encode_since(&[])).collect();
        for ops in &relay_blobs {
            clients[0].merge_bytes(ops).unwrap();
        }
        // Now client0 has: own 100 ops + clients 1-4's 400 ops = 500 ops
        // Missing: clients 5-9's 500 ops

        // Relay sends catch-up as PATCH from empty.
        let patch_from_empty = relay.encode_since(&[]);
        let result = clients[0].merge_bytes(&patch_from_empty);
        assert!(
            result.is_ok(),
            "stress simulation patch merge failed: {:?}",
            result.err()
        );
        assert_eq!(clients[0].content(), relay.content());
    }

    /// Same scenario but with `encode_full()` — should always work.
    #[test]
    fn test_full_encode_into_partial_doc() {
        let mut engine_relay = Engine::new();
        let relay_alice = engine_relay.get_or_create_agent("alice");
        let relay_bob = engine_relay.get_or_create_agent("bob");
        engine_relay.insert(relay_alice, 0, "hello").unwrap();
        engine_relay.insert(relay_bob, 5, " world").unwrap();

        let mut engine_client = Engine::new();
        let client_alice = engine_client.get_or_create_agent("alice");
        engine_client.insert(client_alice, 0, "hello").unwrap();

        let full = engine_relay.encode_full();
        let result = engine_client.merge_bytes(&full);
        assert!(
            result.is_ok(),
            "merge of encode_full() into partial doc failed: {:?}",
            result.err()
        );
        assert_eq!(engine_client.content(), engine_relay.content());
    }
}
