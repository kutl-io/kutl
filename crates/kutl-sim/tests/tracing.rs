//! Tests for the simulation tracing system.

use kutl_core::Boundary;
use kutl_sim::Simulation;
use kutl_sim::trace::TraceEvent;

#[test]
fn trace_records_events() {
    let mut sim = Simulation::new(300).with_tracing();
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();

    sim.edit(a, "init", Boundary::Explicit, |ctx| ctx.insert(0, "hello"))
        .unwrap();
    sim.sync(a, b);
    sim.partition(a, b);
    sim.heal(a, b);

    let log = sim.trace().unwrap();
    let events = log.events();

    // Should have: Edit, MessageSent, Partition, Heal
    assert!(
        events.len() >= 4,
        "expected at least 4 events, got {}",
        events.len()
    );

    // Check event types.
    assert!(matches!(&events[0], TraceEvent::Edit { peer: 0, .. }));
    assert!(matches!(
        &events[1],
        TraceEvent::MessageSent { from: 0, to: 1, .. }
    ));
    assert!(matches!(
        &events[2],
        TraceEvent::Partition { a: 0, b: 1, .. }
    ));
    assert!(matches!(&events[3], TraceEvent::Heal { a: 0, b: 1, .. }));
}

#[test]
fn trace_last_n() {
    let mut sim = Simulation::new(301).with_tracing();
    let a = sim.add_peer("alice").unwrap();

    for i in 0..10 {
        sim.edit(a, &format!("edit-{i}"), Boundary::Auto, |ctx| {
            ctx.insert(0, "x")
        })
        .unwrap();
    }

    let log = sim.trace().unwrap();
    assert_eq!(log.last_n(3).len(), 3);
    assert_eq!(log.last_n(100).len(), 10);
    assert_eq!(log.last_n(0).len(), 0);
}

#[test]
fn trace_json_output() {
    let mut sim = Simulation::new(302).with_tracing();
    let a = sim.add_peer("alice").unwrap();
    let b = sim.add_peer("bob").unwrap();

    sim.edit(a, "init", Boundary::Explicit, |ctx| ctx.insert(0, "hello"))
        .unwrap();
    sim.sync(a, b);
    sim.run_until_quiescent();

    let log = sim.trace().unwrap();
    let json = log.to_json().unwrap();

    // Should be valid JSON.
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(parsed.is_array());
    let arr = parsed.as_array().unwrap();
    assert!(!arr.is_empty());
}

#[test]
fn trace_file_written() {
    let mut sim = Simulation::new(303).with_tracing();
    let a = sim.add_peer("alice").unwrap();

    sim.edit(a, "init", Boundary::Explicit, |ctx| ctx.insert(0, "hello"))
        .unwrap();

    let log = sim.trace().unwrap();
    let path = log.write_to_file(303).unwrap();

    assert!(path.exists());
    assert!(path.to_string_lossy().contains("sim-traces"));
    assert!(path.to_string_lossy().contains("303.json"));

    // Verify contents are valid JSON.
    let contents = std::fs::read_to_string(&path).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert!(parsed.is_array());

    // Clean up.
    let _ = std::fs::remove_file(&path);
}
