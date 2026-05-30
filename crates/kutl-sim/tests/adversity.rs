//! Network adversity tests: partitions, reordering, loss, latency.

use kutl_core::Boundary;
use kutl_sim::Simulation;
use kutl_sim::link::LinkConfig;

#[test]
fn partition_then_heal_converges() {
    let mut sim = Simulation::new(100);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();
    let _c = sim.add_peer("carol").unwrap();

    // Seed some content.
    sim.edit(a, "seed", Boundary::Explicit, |ctx| ctx.insert(0, "hello"))
        .unwrap();
    sim.sync_all();
    sim.run_until_quiescent();

    // Partition alice from bob.
    sim.partition(a, b);

    // Both edit independently.
    sim.edit(a, "alice edit", Boundary::Explicit, |ctx| {
        ctx.insert(5, " alice")
    })
    .unwrap();
    sim.edit(b, "bob edit", Boundary::Explicit, |ctx| {
        ctx.insert(5, " bob")
    })
    .unwrap();

    // Sync: alice→bob should be dropped due to partition.
    sim.sync(a, b);
    sim.sync(b, a);
    sim.run_until_quiescent();

    // Alice and bob should have diverged.
    assert_ne!(sim.content(a), sim.content(b));

    // Heal and re-sync.
    sim.heal(a, b);
    sim.sync_all();
    sim.run_until_quiescent();
    sim.sync_all();
    sim.run_until_quiescent();

    sim.assert_converged();
    let content = sim.content(a);
    assert!(content.contains("alice"), "missing alice in: {content}");
    assert!(content.contains("bob"), "missing bob in: {content}");
}

#[test]
fn message_reordering_still_converges() {
    let mut sim = Simulation::new(101);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();

    // High reorder probability on both directions.
    let reorder_config = LinkConfig {
        latency: 0..5,
        loss: 0.0,
        reorder: 0.9,
    };
    sim.configure_link(a, b, reorder_config.clone());
    sim.configure_link(b, a, reorder_config);

    sim.edit(a, "write aaa", Boundary::Explicit, |ctx| {
        ctx.insert(0, "AAA")
    })
    .unwrap();
    sim.edit(b, "write bbb", Boundary::Explicit, |ctx| {
        ctx.insert(0, "BBB")
    })
    .unwrap();

    // Multiple sync rounds to overcome reordering.
    for _ in 0..5 {
        sim.sync_all();
        sim.run_until_quiescent();
    }

    sim.assert_converged();
    let content = sim.content(a);
    assert!(content.contains("AAA"), "missing AAA in: {content}");
    assert!(content.contains("BBB"), "missing BBB in: {content}");
}

#[test]
fn message_loss_with_retransmit() {
    let mut sim = Simulation::new(102);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();

    // 20% loss in both directions.
    let lossy_config = LinkConfig {
        latency: 0..1,
        loss: 0.2,
        reorder: 0.0,
    };
    sim.configure_link(a, b, lossy_config.clone());
    sim.configure_link(b, a, lossy_config);

    sim.edit(a, "write aaa", Boundary::Explicit, |ctx| {
        ctx.insert(0, "AAA")
    })
    .unwrap();
    sim.edit(b, "write bbb", Boundary::Explicit, |ctx| {
        ctx.insert(0, "BBB")
    })
    .unwrap();

    // Many sync rounds to overcome 20% loss.
    for _ in 0..20 {
        sim.sync_all();
        sim.run_until_quiescent();
    }

    sim.assert_converged();
    let content = sim.content(a);
    assert!(content.contains("AAA"), "missing AAA in: {content}");
    assert!(content.contains("BBB"), "missing BBB in: {content}");
}

#[test]
fn latency_does_not_prevent_convergence() {
    let mut sim = Simulation::new(103);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();

    // Large latency range.
    let latency_config = LinkConfig {
        latency: 50..200,
        loss: 0.0,
        reorder: 0.0,
    };
    sim.configure_link(a, b, latency_config.clone());
    sim.configure_link(b, a, latency_config);

    sim.edit(a, "write aaa", Boundary::Explicit, |ctx| {
        ctx.insert(0, "AAA")
    })
    .unwrap();
    sim.edit(b, "write bbb", Boundary::Explicit, |ctx| {
        ctx.insert(0, "BBB")
    })
    .unwrap();

    sim.sync_all();
    sim.run_until_quiescent();

    sim.assert_converged();
    let content = sim.content(a);
    assert!(content.contains("AAA"), "missing AAA in: {content}");
    assert!(content.contains("BBB"), "missing BBB in: {content}");
}

#[test]
fn asymmetric_link_config() {
    let mut sim = Simulation::new(104);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();

    // High latency a→b, low latency b→a.
    sim.configure_link(
        a,
        b,
        LinkConfig {
            latency: 100..200,
            loss: 0.0,
            reorder: 0.0,
        },
    );
    sim.configure_link(
        b,
        a,
        LinkConfig {
            latency: 0..1,
            loss: 0.0,
            reorder: 0.0,
        },
    );

    sim.edit(a, "write aaa", Boundary::Explicit, |ctx| {
        ctx.insert(0, "AAA")
    })
    .unwrap();
    sim.edit(b, "write bbb", Boundary::Explicit, |ctx| {
        ctx.insert(0, "BBB")
    })
    .unwrap();

    sim.sync_all();
    sim.run_until_quiescent();

    sim.assert_converged();
    let content = sim.content(a);
    assert!(content.contains("AAA"), "missing AAA in: {content}");
    assert!(content.contains("BBB"), "missing BBB in: {content}");
}
