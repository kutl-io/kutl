//! Basic simulation convergence scenarios.

use kutl_core::Boundary;
use kutl_sim::Simulation;

#[test]
fn two_peer_convergence() {
    let mut sim = Simulation::new(1);
    let alice = sim.add_peer("alice").unwrap();
    let bob = sim.add_peer("bob").unwrap();

    sim.edit(alice, "init", Boundary::Explicit, |ctx| {
        ctx.insert(0, "hello world")
    })
    .unwrap();

    sim.sync(alice, bob);
    sim.run_until_quiescent();

    sim.assert_converged();
    sim.assert_content(alice, "hello world");
    sim.assert_content(bob, "hello world");
}

#[test]
fn concurrent_edits_converge() {
    let mut sim = Simulation::new(2);
    let alice = sim.add_peer("alice").unwrap();
    let bob = sim.add_peer("bob").unwrap();

    // Both start from empty and edit independently.
    sim.edit(alice, "alice writes", Boundary::Explicit, |ctx| {
        ctx.insert(0, "AAA")
    })
    .unwrap();

    sim.edit(bob, "bob writes", Boundary::Explicit, |ctx| {
        ctx.insert(0, "BBB")
    })
    .unwrap();

    // Sync both directions.
    sim.sync(alice, bob);
    sim.sync(bob, alice);
    sim.run_until_quiescent();

    sim.assert_converged();
    let content = sim.content(alice);
    assert!(content.contains("AAA"), "missing AAA in: {content}");
    assert!(content.contains("BBB"), "missing BBB in: {content}");
}

#[test]
fn three_peer_fan_out() {
    let mut sim = Simulation::new(3);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();
    let c = sim.add_peer("carol").unwrap();

    sim.edit(a, "seed", Boundary::Explicit, |ctx| ctx.insert(0, "hello"))
        .unwrap();

    // Fan out from A to B and C.
    sim.sync(a, b);
    sim.sync(a, c);
    sim.run_until_quiescent();

    sim.assert_converged();
    sim.assert_content(a, "hello");
    sim.assert_content(b, "hello");
    sim.assert_content(c, "hello");
}

#[test]
fn three_peer_chain() {
    let mut sim = Simulation::new(4);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();
    let c = sim.add_peer("carol").unwrap();

    sim.edit(a, "seed", Boundary::Explicit, |ctx| ctx.insert(0, "chain"))
        .unwrap();

    // A → B → C (relay through B).
    sim.sync(a, b);
    sim.run_until_quiescent();
    sim.sync(b, c);
    sim.run_until_quiescent();

    sim.assert_converged();
    sim.assert_content(c, "chain");
}

#[test]
fn seed_reproducibility() {
    // Run the same scenario twice with the same seed and verify identical results.
    fn run(seed: u64) -> String {
        let mut sim = Simulation::new(seed);
        let alice = sim.add_peer("alice").unwrap();
        let bob = sim.add_peer("bob").unwrap();

        sim.edit(alice, "alice writes", Boundary::Explicit, |ctx| {
            ctx.insert(0, "AAA")
        })
        .unwrap();

        sim.edit(bob, "bob writes", Boundary::Explicit, |ctx| {
            ctx.insert(0, "BBB")
        })
        .unwrap();

        sim.sync(alice, bob);
        sim.sync(bob, alice);
        sim.run_until_quiescent();

        sim.assert_converged();
        sim.content(alice)
    }

    let result_a = run(42);
    let result_b = run(42);
    assert_eq!(
        result_a, result_b,
        "same seed must produce identical results"
    );
}

#[test]
fn intent_preserved_through_sync() {
    let mut sim = Simulation::new(5);
    let alice = sim.add_peer("alice").unwrap();
    let bob = sim.add_peer("bob").unwrap();

    sim.edit(alice, "add greeting", Boundary::Explicit, |ctx| {
        ctx.insert(0, "hello")
    })
    .unwrap();

    sim.sync(alice, bob);
    sim.run_until_quiescent();

    // Bob should have the change with the intent metadata.
    let bob_changes = sim.changes(bob);
    let intents: Vec<&str> = bob_changes.iter().map(|c| c.intent.as_str()).collect();
    assert!(
        intents.contains(&"add greeting"),
        "intent 'add greeting' not found in bob's changes: {intents:?}",
    );
}
