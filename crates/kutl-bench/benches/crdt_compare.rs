//! Criterion benchmarks comparing CRDT engines.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kutl_bench::adapters::{AutomergeEngine, BenchEngine, DtEngine, LoroEngine, YrsEngine};
use kutl_bench::workload::{self, EditOp, apply_ops};

/// Operation counts for sequential-insert benchmarks.
/// Goes higher (100k) because sequential appends are the cheapest workload.
const SEQUENTIAL_SIZES: &[usize] = &[1_000, 10_000, 100_000];

/// Operation counts for random-position and mixed benchmarks.
const STANDARD_SIZES: &[usize] = &[1_000, 10_000, 50_000];

/// RNG seed for random-insert benchmarks.
const RANDOM_INSERT_SEED: u64 = 42;

/// RNG seed for mixed-edit benchmarks.
const MIXED_EDIT_SEED: u64 = 99;

/// Register benchmarks for all four engines at a given size and workload.
macro_rules! bench_all_engines {
    ($group:expr, $n:expr, $ops:expr) => {
        $group.bench_with_input(BenchmarkId::new("diamond-types", $n), &$ops, |b, ops| {
            b.iter(|| run_workload::<DtEngine>(ops));
        });
        $group.bench_with_input(BenchmarkId::new("yrs", $n), &$ops, |b, ops| {
            b.iter(|| run_workload::<YrsEngine>(ops));
        });
        $group.bench_with_input(BenchmarkId::new("automerge", $n), &$ops, |b, ops| {
            b.iter(|| run_workload::<AutomergeEngine>(ops));
        });
        $group.bench_with_input(BenchmarkId::new("loro", $n), &$ops, |b, ops| {
            b.iter(|| run_workload::<LoroEngine>(ops));
        });
    };
}

/// Apply a pre-generated workload to a fresh engine.
fn run_workload<E: BenchEngine>(ops: &[EditOp]) {
    std::hint::black_box(apply_ops::<E>(ops));
}

// ---------------------------------------------------------------------------
// Benchmark groups
// ---------------------------------------------------------------------------

fn bench_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_insert");

    for &n in SEQUENTIAL_SIZES {
        let ops = workload::sequential_chars(n);
        bench_all_engines!(group, n, ops);
    }

    group.finish();
}

fn bench_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_insert");

    for &n in STANDARD_SIZES {
        let ops = workload::random_inserts(n, RANDOM_INSERT_SEED);
        bench_all_engines!(group, n, ops);
    }

    group.finish();
}

fn bench_front(c: &mut Criterion) {
    let mut group = c.benchmark_group("front_insert");

    for &n in STANDARD_SIZES {
        let ops = workload::front_insert(n);
        bench_all_engines!(group, n, ops);
    }

    group.finish();
}

fn bench_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_edit");

    for &n in STANDARD_SIZES {
        let ops = workload::mixed_edit(n, MIXED_EDIT_SEED);
        bench_all_engines!(group, n, ops);
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential,
    bench_random,
    bench_front,
    bench_mixed
);
criterion_main!(benches);
