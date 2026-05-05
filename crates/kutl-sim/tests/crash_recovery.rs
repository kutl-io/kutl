//! Crash recovery tests: peers crash and restart from persisted snapshots.

use kutl_core::Boundary;
use kutl_sim::Simulation;

#[test]
fn crash_and_restart_preserves_content() {
    let mut sim = Simulation::new(200);
    let a = sim.add_peer("alice").unwrap();

    sim.edit(a, "write hello", Boundary::Explicit, |ctx| {
        ctx.insert(0, "hello")
    })
    .unwrap();

    sim.crash(a);
    assert!(sim.is_crashed(a));

    sim.restart(a);
    assert!(!sim.is_crashed(a));
    sim.assert_content(a, "hello");
}

#[test]
fn crash_and_restart_can_continue_editing() {
    let mut sim = Simulation::new(201);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();

    sim.edit(a, "init", Boundary::Explicit, |ctx| ctx.insert(0, "hello"))
        .unwrap();
    sim.sync_all();
    sim.run_until_quiescent();

    // Crash alice, bob edits.
    sim.crash(a);
    sim.edit(b, "bob edit", Boundary::Explicit, |ctx| {
        ctx.insert(5, " bob")
    })
    .unwrap();

    // Restart alice, she edits.
    sim.restart(a);
    sim.edit(a, "alice edit", Boundary::Explicit, |ctx| {
        ctx.insert(5, " alice")
    })
    .unwrap();

    // Sync and converge.
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
fn crash_during_active_sync() {
    let mut sim = Simulation::new(202);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();

    sim.edit(a, "init", Boundary::Explicit, |ctx| ctx.insert(0, "hello"))
        .unwrap();

    // Alice syncs to bob (message in flight).
    sim.sync(a, b);

    // Crash bob before delivery — in-flight messages get dropped.
    sim.crash(b);
    sim.run_until_quiescent();

    // Bob restarts and should have empty doc (never received the message).
    sim.restart(b);

    // Re-sync.
    sim.sync_all();
    sim.run_until_quiescent();

    sim.assert_converged();
    sim.assert_content(a, "hello");
    sim.assert_content(b, "hello");
}

#[test]
fn crash_clears_sync_cursors() {
    let mut sim = Simulation::new(203);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();

    sim.edit(a, "init", Boundary::Explicit, |ctx| ctx.insert(0, "hello"))
        .unwrap();
    sim.sync_all();
    sim.run_until_quiescent();

    // Bob has "hello". Crash and restart bob.
    sim.crash(b);
    sim.restart(b);

    // Alice edits more.
    sim.edit(a, "add world", Boundary::Explicit, |ctx| {
        ctx.insert(5, " world")
    })
    .unwrap();

    // Sync should correctly send the new operations (cursors were cleared).
    sim.sync_all();
    sim.run_until_quiescent();

    sim.assert_converged();
    let content = sim.content(b);
    assert!(
        content.contains("world"),
        "missing world in: {content} — sync cursors may not have been cleared"
    );
}

#[test]
fn multiple_crashes_and_restarts() {
    let mut sim = Simulation::new(204);
    let a = sim.add_peer("alice").unwrap();
    let _b = sim.add_peer("bob").unwrap();

    sim.edit(a, "write v1", Boundary::Explicit, |ctx| ctx.insert(0, "v1"))
        .unwrap();

    // Three crash/restart cycles.
    for i in 0..3 {
        sim.crash(a);
        sim.restart(a);
        let text = format!(" r{i}");
        let doc_len = sim.content(a).len();
        sim.edit(
            a,
            &format!("edit after restart {i}"),
            Boundary::Explicit,
            |ctx| ctx.insert(doc_len, &text),
        )
        .unwrap();
    }

    sim.sync_all();
    sim.run_until_quiescent();

    sim.assert_converged();
    let content = sim.content(a);
    assert!(content.contains("v1"), "missing v1 in: {content}");
    assert!(content.contains("r0"), "missing r0 in: {content}");
    assert!(content.contains("r1"), "missing r1 in: {content}");
    assert!(content.contains("r2"), "missing r2 in: {content}");
}

#[test]
fn all_peers_crash_and_restart() {
    let mut sim = Simulation::new(205);
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();
    let c = sim.add_peer("carol").unwrap();

    sim.edit(a, "alice init", Boundary::Explicit, |ctx| {
        ctx.insert(0, "AAA")
    })
    .unwrap();
    sim.edit(b, "bob init", Boundary::Explicit, |ctx| {
        ctx.insert(0, "BBB")
    })
    .unwrap();
    sim.edit(c, "carol init", Boundary::Explicit, |ctx| {
        ctx.insert(0, "CCC")
    })
    .unwrap();

    // Crash everyone.
    sim.crash(a);
    sim.crash(b);
    sim.crash(c);

    // Restart everyone.
    sim.restart_all_crashed();

    // Sync and converge.
    sim.sync_all();
    sim.run_until_quiescent();
    sim.sync_all();
    sim.run_until_quiescent();

    sim.assert_converged();
    let content = sim.content(a);
    assert!(content.contains("AAA"), "missing AAA in: {content}");
    assert!(content.contains("BBB"), "missing BBB in: {content}");
    assert!(content.contains("CCC"), "missing CCC in: {content}");
}
