//! Criterion benchmarks for relay-mediated sync throughput.
//!
//! Uses [`RelaySimulation`] to measure full end-to-end sync performance
//! (encode → network → relay merge → relay → decode) with zero-latency
//! network links for pure throughput measurement.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kutl_sim::relay_sim::RelaySimulation;

const SPACE: &str = "bench-space";
const DOC: &str = "bench-doc";

// ---------------------------------------------------------------------------
// Single-pair sync: one writer, one reader, N ops each followed by quiescence
// ---------------------------------------------------------------------------

fn bench_single_pair_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_pair_sync");

    for &n in &[100, 500, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut sim = RelaySimulation::new(42);
                let alice = sim.add_daemon("alice", SPACE, DOC).unwrap();
                let _bob = sim.add_daemon("bob", SPACE, DOC).unwrap();

                for i in 0..n {
                    sim.insert(alice, i, "x");
                    sim.run_until_quiescent();
                }

                sim.assert_converged();
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Fan-out: one writer, N readers
// ---------------------------------------------------------------------------

fn bench_fan_out(c: &mut Criterion) {
    let mut group = c.benchmark_group("fan_out");

    let ops = 100;

    for &readers in &[2, 5, 10, 20] {
        group.bench_with_input(
            BenchmarkId::new("readers", readers),
            &readers,
            |b, &readers| {
                b.iter(|| {
                    let mut sim = RelaySimulation::new(42);
                    let writer = sim.add_daemon("writer", SPACE, DOC).unwrap();
                    for i in 0..readers {
                        sim.add_daemon(&format!("reader-{i}"), SPACE, DOC).unwrap();
                    }

                    for i in 0..ops {
                        sim.insert(writer, i, "x");
                        sim.run_until_quiescent();
                    }

                    sim.assert_converged();
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Convergence time: N daemons with concurrent edits
// ---------------------------------------------------------------------------

fn bench_convergence(c: &mut Criterion) {
    let mut group = c.benchmark_group("convergence");

    let edits_per_daemon = 20;

    for &daemon_count in &[3, 5, 10] {
        group.bench_with_input(
            BenchmarkId::new("daemons", daemon_count),
            &daemon_count,
            |b, &daemon_count| {
                b.iter(|| {
                    let mut sim = RelaySimulation::new(42);
                    let mut ids = Vec::with_capacity(daemon_count);
                    for i in 0..daemon_count {
                        ids.push(sim.add_daemon(&format!("d-{i}"), SPACE, DOC).unwrap());
                    }

                    // All daemons make concurrent edits (no ticks between rounds).
                    for round in 0..edits_per_daemon {
                        for &id in &ids {
                            let pos = sim.document_size(id);
                            sim.insert(id, pos, &format!("r{round}d{id}"));
                        }
                    }

                    // Measure convergence.
                    sim.sync_all_daemons();
                    sim.run_until_quiescent();
                    sim.sync_all_daemons();
                    sim.run_until_quiescent();
                    sim.assert_converged();
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Batched sync: writer accumulates N ops, single sync
// ---------------------------------------------------------------------------

fn bench_batched_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("batched_sync");

    for &batch in &[100, 500, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.iter(|| {
                let mut sim = RelaySimulation::new(42);
                let alice = sim.add_daemon("alice", SPACE, DOC).unwrap();
                let _bob = sim.add_daemon("bob", SPACE, DOC).unwrap();

                // Alice accumulates many edits without any network delivery.
                for i in 0..batch {
                    sim.insert(alice, i, "x");
                }

                // Single sync cycle.
                sim.sync_all_daemons();
                sim.run_until_quiescent();
                sim.assert_converged();
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_pair_sync,
    bench_fan_out,
    bench_convergence,
    bench_batched_sync,
);
criterion_main!(benches);
