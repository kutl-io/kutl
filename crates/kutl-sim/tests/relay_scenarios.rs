//! Integration tests for relay-mediated simulation scenarios.
//!
//! Each test exercises the full daemon → relay → daemon sync path using
//! [`RelaySimulation`], which drives a real [`Relay`] actor synchronously.

use kutl_sim::link::LinkConfig;
use kutl_sim::relay_sim::RelaySimulation;

const SPACE: &str = "test-space";
const DOC: &str = "test-doc";
const LOWER: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
const UPPER: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

// ---------------------------------------------------------------------------
// Basic two-daemon convergence
// ---------------------------------------------------------------------------

#[test]
fn test_two_daemons_converge() {
    let mut sim = RelaySimulation::new(1);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();

    sim.insert(a, 0, "hello");
    sim.run_until_quiescent();

    // Bob should have received Alice's edit via the relay.
    assert_eq!(sim.content(a), "hello");
    assert_eq!(sim.content(b), "hello");
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Late joiner catches up
// ---------------------------------------------------------------------------

#[test]
fn test_late_joiner_catches_up() {
    let mut sim = RelaySimulation::new(2);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();

    sim.insert(a, 0, "hello");
    sim.run_until_quiescent();

    // Bob joins after Alice has already edited.
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();
    sim.run_until_quiescent();

    // Bob should have received the catch-up.
    assert_eq!(sim.content(b), "hello");
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Concurrent edits converge
// ---------------------------------------------------------------------------

#[test]
fn test_concurrent_edits_converge() {
    let mut sim = RelaySimulation::new(3);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();

    // Both edit concurrently (before any ticks deliver messages).
    sim.insert(a, 0, "alice");
    sim.insert(b, 0, "bob");

    sim.run_until_quiescent();

    let content = sim.content(a);
    assert!(content.contains("alice"), "missing alice in: {content}");
    assert!(content.contains("bob"), "missing bob in: {content}");
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Network partition and heal
// ---------------------------------------------------------------------------

#[test]
fn test_partition_and_heal() {
    let mut sim = RelaySimulation::new(4);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();

    // Partition Alice from the relay.
    sim.partition(a);

    // Alice edits while partitioned — ops are dropped by the network.
    sim.insert(a, 0, "offline");
    sim.run_until_quiescent();

    // Bob should NOT have Alice's edit.
    assert_eq!(sim.content(b), "");

    // Heal and resync.
    sim.heal(a);
    sim.sync_daemon(a);
    sim.run_until_quiescent();

    assert_eq!(sim.content(a), "offline");
    assert_eq!(sim.content(b), "offline");
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Bidirectional partition with concurrent edits
// ---------------------------------------------------------------------------

#[test]
fn test_partition_concurrent_edits() {
    let mut sim = RelaySimulation::new(5);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();

    // Partition both.
    sim.partition(a);
    sim.partition(b);

    sim.insert(a, 0, "A");
    sim.insert(b, 0, "B");
    sim.run_until_quiescent();

    // Neither should have the other's edit.
    assert_eq!(sim.content(a), "A");
    assert_eq!(sim.content(b), "B");

    // Heal and resync.
    sim.heal_all();
    sim.sync_all_daemons();
    sim.run_until_quiescent();

    let content = sim.content(a);
    assert!(content.contains('A'), "missing A in: {content}");
    assert!(content.contains('B'), "missing B in: {content}");
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Large document stress test
// ---------------------------------------------------------------------------

#[test]
fn test_large_document_stress() {
    let mut sim = RelaySimulation::new(6);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();

    // Alice inserts 500 characters.
    for i in 0..500 {
        let ch = LOWER[i % 26] as char;
        sim.insert(a, i, &ch.to_string());
    }
    sim.run_until_quiescent();

    // Bob inserts 500 more characters.
    let bob_start = sim.content(b).len();
    for i in 0..500 {
        let ch = UPPER[i % 26] as char;
        sim.insert(b, bob_start + i, &ch.to_string());
    }
    sim.run_until_quiescent();

    assert_eq!(sim.content(a).len(), 1000);
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Many daemons fan-out
// ---------------------------------------------------------------------------

#[test]
fn test_many_daemons_fan_out() {
    let mut sim = RelaySimulation::new(7);

    let daemons: Vec<_> = (0..6)
        .map(|i| sim.add_daemon(&format!("peer-{i}"), SPACE, DOC).unwrap())
        .collect();

    // Each daemon inserts its name at the end.
    for (i, &d) in daemons.iter().enumerate() {
        let pos = sim.content(d).len();
        sim.insert(d, pos, &format!("p{i}"));
        sim.run_until_quiescent();
    }

    let content = sim.content(daemons[0]);
    for i in 0..6 {
        assert!(
            content.contains(&format!("p{i}")),
            "missing p{i} in: {content}"
        );
    }
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Interleaved edits from multiple daemons
// ---------------------------------------------------------------------------

#[test]
fn test_interleaved_edits() {
    let mut sim = RelaySimulation::new(8);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();
    let c = sim.add_daemon("carol", SPACE, DOC).unwrap();

    sim.insert(a, 0, "hello");
    sim.run_until_quiescent();

    sim.insert(b, 5, " world");
    sim.run_until_quiescent();

    sim.insert(c, 0, ">> ");
    sim.run_until_quiescent();

    let content = sim.content(a);
    assert!(content.contains("hello"), "missing hello in: {content}");
    assert!(content.contains("world"), "missing world in: {content}");
    assert!(content.contains(">>"), "missing >> in: {content}");
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Delete operations
// ---------------------------------------------------------------------------

#[test]
fn test_delete_operations() {
    let mut sim = RelaySimulation::new(9);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();

    sim.insert(a, 0, "hello world");
    sim.run_until_quiescent();

    // Delete " world" from Bob's copy.
    sim.delete(b, 5, 6);
    sim.run_until_quiescent();

    assert_eq!(sim.content(a), "hello");
    assert_eq!(sim.content(b), "hello");
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Link latency
// ---------------------------------------------------------------------------

#[test]
fn test_link_latency() {
    let mut sim = RelaySimulation::new(10);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();

    // Add latency on Alice → relay link.
    sim.configure_link_to_relay(
        a,
        LinkConfig {
            latency: 5..10,
            loss: 0.0,
            reorder: 0.0,
        },
    );

    sim.insert(a, 0, "delayed");

    // After 1 tick, message should still be in transit.
    sim.tick();
    assert_eq!(sim.content(b), "");

    // After enough ticks, it should arrive.
    sim.run_until_quiescent();
    assert_eq!(sim.content(b), "delayed");
    sim.assert_converged();
}

// ---------------------------------------------------------------------------
// Multiple documents
// ---------------------------------------------------------------------------

#[test]
fn test_multiple_documents() {
    let mut sim = RelaySimulation::new(11);
    let a1 = sim.add_daemon("alice-code", SPACE, "code.rs").unwrap();
    let b1 = sim.add_daemon("bob-code", SPACE, "code.rs").unwrap();
    let a2 = sim.add_daemon("alice-readme", SPACE, "README.md").unwrap();
    let b2 = sim.add_daemon("bob-readme", SPACE, "README.md").unwrap();

    sim.insert(a1, 0, "fn main() {}");
    sim.insert(a2, 0, "# Hello");
    sim.run_until_quiescent();

    assert_eq!(sim.content(b1), "fn main() {}");
    assert_eq!(sim.content(b2), "# Hello");

    // Edits to one doc don't affect the other.
    sim.insert(b1, 12, "\n");
    sim.run_until_quiescent();

    assert_eq!(sim.content(a2), "# Hello");
}
