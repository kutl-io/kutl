//! Intensive relay simulation stress tests.
//!
//! Gated behind the `stress` feature flag. Run with:
//! ```sh
//! cargo test -p kutl-sim --features stress
//! ```

#![cfg(feature = "stress")]

use kutl_sim::relay_fuzz::{RelayFuzzConfig, run_relay_fuzz};
use kutl_sim::relay_sim::RelaySimulation;
use rand::RngExt;

const SPACE: &str = "stress-space";
const DOC: &str = "stress-doc";

// ---------------------------------------------------------------------------
// Many daemons convergence
// ---------------------------------------------------------------------------

/// 20 daemons each insert 100 characters, then verify convergence.
#[test]
fn stress_many_daemons_convergence() {
    let mut sim = RelaySimulation::new(42);

    let daemon_count = 20;
    let edits_per_daemon = 100;
    let mut ids = Vec::with_capacity(daemon_count);
    for i in 0..daemon_count {
        ids.push(sim.add_daemon(&format!("d-{i}"), SPACE, DOC).unwrap());
    }

    // Each daemon inserts a batch of characters.
    for &id in &ids {
        for j in 0..edits_per_daemon {
            let pos = sim.document_size(id);
            sim.insert(id, pos, &format!("{id}:{j} "));
        }
        sim.sync_daemon(id);
        sim.run_until_quiescent();
    }

    // Final convergence round.
    sim.sync_all_daemons();
    sim.run_until_quiescent();
    sim.assert_converged();

    let size = sim.document_size(ids[0]);
    eprintln!(
        "many daemons: {daemon_count} daemons, {edits_per_daemon} edits each, final size = {size} chars"
    );
    assert!(size > 0, "document should not be empty");
}

// ---------------------------------------------------------------------------
// Large document
// ---------------------------------------------------------------------------

/// Build a 100KB+ document through 2 daemons alternating edits.
#[test]
fn stress_large_document() {
    let mut sim = RelaySimulation::new(43);
    let a = sim.add_daemon("alice", SPACE, DOC).unwrap();
    let b = sim.add_daemon("bob", SPACE, DOC).unwrap();

    let target_size = 100_000; // 100KB in chars
    let chunk = "abcdefghij"; // 10 chars per insert
    let mut total_edits = 0;

    while sim.document_size(a) < target_size {
        let writer = if total_edits % 2 == 0 { a } else { b };
        let pos = sim.document_size(writer);
        sim.insert(writer, pos, chunk);
        total_edits += 1;

        // Sync every 100 edits to keep versions manageable.
        if total_edits % 100 == 0 {
            sim.sync_all_daemons();
            sim.run_until_quiescent();
        }
    }

    sim.sync_all_daemons();
    sim.run_until_quiescent();
    sim.assert_converged();

    let size = sim.document_size(a);
    eprintln!("large doc: {total_edits} edits, final size = {size} chars");
    assert!(
        size >= target_size,
        "document should be at least {target_size} chars, got {size}"
    );
}

// ---------------------------------------------------------------------------
// Many documents
// ---------------------------------------------------------------------------

/// 50 documents, 2 daemons each, verify isolation and convergence.
#[test]
fn stress_many_documents() {
    let doc_count = 50;
    let edits_per_doc = 20;

    // Each document gets its own simulation (relay tracks by space+doc key).
    let mut sim = RelaySimulation::new(44);

    // Create daemon pairs for each document.
    let mut pairs: Vec<(usize, usize)> = Vec::new();
    for i in 0..doc_count {
        let doc_id = format!("doc-{i}");
        let a = sim.add_daemon(&format!("a-{i}"), SPACE, &doc_id).unwrap();
        let b = sim.add_daemon(&format!("b-{i}"), SPACE, &doc_id).unwrap();
        pairs.push((a, b));
    }

    // Each pair edits its document.
    for (i, &(a, b)) in pairs.iter().enumerate() {
        for j in 0..edits_per_doc {
            let writer = if j % 2 == 0 { a } else { b };
            let pos = sim.document_size(writer);
            sim.insert(writer, pos, &format!("d{i}e{j} "));
        }
    }

    // Sync all and converge.
    sim.sync_all_daemons();
    sim.run_until_quiescent();

    // Verify each pair converged (can't use assert_converged since it checks ALL daemons).
    for (i, &(a, b)) in pairs.iter().enumerate() {
        let ca = sim.content(a);
        let cb = sim.content(b);
        assert_eq!(ca, cb, "document {i} diverged: {ca:?} vs {cb:?}");
    }

    eprintln!("many docs: {doc_count} documents, {edits_per_doc} edits each");
}

// ---------------------------------------------------------------------------
// Burst concurrent edits
// ---------------------------------------------------------------------------

/// 10 daemons all insert simultaneously before any tick delivery.
#[test]
fn stress_burst_concurrent_edits() {
    let mut sim = RelaySimulation::new(45);
    let daemon_count = 10;
    let burst_size = 50;
    let mut ids = Vec::with_capacity(daemon_count);
    for i in 0..daemon_count {
        ids.push(sim.add_daemon(&format!("burst-{i}"), SPACE, DOC).unwrap());
    }

    // All daemons insert without any ticks between them.
    for round in 0..burst_size {
        for &id in &ids {
            sim.insert(id, 0, &format!("r{round}d{id}"));
        }
    }

    // Now let everything propagate.
    sim.sync_all_daemons();
    sim.run_until_quiescent();
    sim.sync_all_daemons();
    sim.run_until_quiescent();
    sim.assert_converged();

    let size = sim.document_size(ids[0]);
    eprintln!("burst: {daemon_count} daemons, {burst_size} rounds, final size = {size} chars");
    assert!(size > 0);
}

// ---------------------------------------------------------------------------
// Disconnect/reconnect convergence
// ---------------------------------------------------------------------------

/// 8 daemons editing with random disconnects and reconnects — verify convergence.
#[test]
fn stress_disconnect_reconnect_convergence() {
    use rand::SeedableRng;
    use rand_chacha::ChaChaRng;

    let mut sim = RelaySimulation::new(46);
    let mut rng = ChaChaRng::seed_from_u64(46);
    let daemon_count = 8;
    let rounds = 200;
    let mut ids = Vec::with_capacity(daemon_count);
    for i in 0..daemon_count {
        ids.push(sim.add_daemon(&format!("dc-{i}"), SPACE, DOC).unwrap());
    }

    for round in 0..rounds {
        // Each round: some daemons edit, some disconnect, some reconnect.
        for &id in &ids {
            if !sim.is_connected(id) {
                // 30% chance to reconnect.
                if rng.random_range(0u32..100) < 30 {
                    sim.reconnect_daemon(id);
                    sim.run_until_quiescent();
                }
                continue;
            }

            // 10% chance to disconnect.
            if rng.random_range(0u32..100) < 10 {
                sim.disconnect_daemon(id);
                continue;
            }

            // Otherwise, insert text.
            let pos = sim.document_size(id);
            sim.insert(id, pos, &format!("r{round}d{id} "));
        }

        // Sync and propagate periodically.
        if round % 10 == 0 {
            for &id in &ids {
                if sim.is_connected(id) {
                    sim.sync_daemon(id);
                }
            }
            sim.run_until_quiescent();
        }
    }

    // Reconnect all and converge.
    for &id in &ids {
        if !sim.is_connected(id) {
            sim.reconnect_daemon(id);
        }
    }
    sim.sync_all_daemons();
    sim.run_until_quiescent();
    sim.sync_all_daemons();
    sim.run_until_quiescent();
    sim.assert_converged();

    let size = sim.document_size(ids[0]);
    eprintln!(
        "disconnect/reconnect: {daemon_count} daemons, {rounds} rounds, final size = {size} chars"
    );
    assert!(size > 0);
}

// ---------------------------------------------------------------------------
// Network adversity convergence
// ---------------------------------------------------------------------------

/// 6 daemons editing with lossy upstream links — verify eventual convergence.
///
/// Only the daemon→relay direction is lossy (simulating unreliable client uploads).
/// The relay→daemon direction stays reliable (production uses TCP/WebSocket which
/// guarantees delivery after the relay's `try_send` succeeds).
#[test]
fn stress_lossy_upstream_convergence() {
    use kutl_sim::link::LinkConfig;

    let mut sim = RelaySimulation::new(47);
    let daemon_count = 6;
    let edits_per_daemon = 50;
    let mut ids = Vec::with_capacity(daemon_count);
    for i in 0..daemon_count {
        ids.push(sim.add_daemon(&format!("lossy-{i}"), SPACE, DOC).unwrap());
    }

    // Configure first 3 daemons with lossy/laggy upstream links only.
    for &id in &ids[..3] {
        sim.configure_link_to_relay(
            id,
            LinkConfig {
                loss: 0.3, // 30% packet loss on daemon→relay
                latency: 2..5,
                reorder: 0.2,
            },
        );
    }

    // All daemons edit.
    for round in 0..edits_per_daemon {
        for &id in &ids {
            let pos = sim.document_size(id);
            sim.insert(id, pos, &format!("r{round}d{id} "));
        }
        // Sync and tick after each round.
        sim.sync_all_daemons();
        sim.run_until_quiescent();
    }

    // Remove adversity and do multiple convergence rounds.
    for &id in &ids[..3] {
        sim.configure_link_to_relay(id, LinkConfig::default());
    }

    for _ in 0..5 {
        sim.sync_all_daemons();
        sim.run_until_quiescent();
    }

    sim.assert_converged();

    let size = sim.document_size(ids[0]);
    eprintln!(
        "lossy upstream: {daemon_count} daemons, {edits_per_daemon} edits each, final size = {size} chars"
    );
    assert!(size > 0);
}

// ---------------------------------------------------------------------------
// Partition and heal
// ---------------------------------------------------------------------------

/// 4 daemons, 2 disconnect mid-editing, edit in isolation, then reconnect
/// and converge. Uses disconnect/reconnect (not partition) to properly
/// reset version tracking.
#[test]
fn stress_disconnect_edit_reconnect_convergence() {
    let mut sim = RelaySimulation::new(48);
    let mut ids = Vec::new();
    for i in 0..4 {
        ids.push(sim.add_daemon(&format!("part-{i}"), SPACE, DOC).unwrap());
    }

    // Phase 1: all daemons edit together.
    for round in 0..20 {
        for &id in &ids {
            let pos = sim.document_size(id);
            sim.insert(id, pos, &format!("p1r{round}d{id} "));
        }
        sim.sync_all_daemons();
        sim.run_until_quiescent();
    }

    // Phase 2: disconnect daemons 0 and 1 — they keep editing locally.
    sim.disconnect_daemon(ids[0]);
    sim.disconnect_daemon(ids[1]);

    for round in 0..30 {
        // Disconnected daemons edit locally.
        for &id in &ids[..2] {
            let pos = sim.document_size(id);
            sim.insert(id, pos, &format!("iso{round}d{id} "));
        }
        // Connected daemons edit and sync normally.
        for &id in &ids[2..] {
            let pos = sim.document_size(id);
            sim.insert(id, pos, &format!("con{round}d{id} "));
        }
        for &id in &ids[2..] {
            sim.sync_daemon(id);
        }
        sim.run_until_quiescent();
    }

    // Phase 3: reconnect and converge.
    sim.reconnect_daemon(ids[0]);
    sim.reconnect_daemon(ids[1]);
    for _ in 0..5 {
        sim.sync_all_daemons();
        sim.run_until_quiescent();
    }
    sim.assert_converged();

    let size = sim.document_size(ids[0]);
    eprintln!("disconnect/edit/reconnect: 4 daemons, final size = {size} chars");
    assert!(size > 0);
}

// ---------------------------------------------------------------------------
// Extended relay fuzz
// ---------------------------------------------------------------------------

/// Relay fuzz with 2000 ticks and 8 daemons.
#[test]
fn stress_extended_relay_fuzz() {
    let config = RelayFuzzConfig {
        ticks: 2000,
        min_daemons: 6,
        max_daemons: 8,
        disconnects: true,
    };

    let seed_count: u64 = std::env::var("KUTL_SIM_FUZZ_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);

    for seed in 0..seed_count {
        let result = run_relay_fuzz(seed, &config);
        eprintln!(
            "extended fuzz seed={seed}: daemons={}, edits={}, content_len={}",
            result.daemon_count,
            result.edit_count,
            result.final_content.len(),
        );
    }
}
