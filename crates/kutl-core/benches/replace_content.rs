//! Benchmarks for `Document::replace_content()`.
//!
//! Measures the diff-then-apply-to-CRDT path that kutl-daemon and kutl-relay
//! both delegate to. Scenarios cover small edits in large documents (the
//! common case) through to full rewrites.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kutl_core::{Boundary, Document};
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;

/// RNG seed for reproducible workloads.
const SEED: u64 = 42;

/// Document sizes (characters) to benchmark against.
const DOC_SIZES: &[usize] = &[100, 1_000, 10_000];

/// Number of characters changed in the "small edit" scenario.
const SMALL_EDIT_CHARS: usize = 10;

/// Alphabet for generated content.
const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz \n";

/// Generate a deterministic string of `len` characters.
fn gen_text(rng: &mut StdRng, len: usize) -> String {
    (0..len)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect()
}

/// Create a `Document` pre-populated with `content`.
fn doc_with_content(content: &str) -> (Document, kutl_core::AgentId) {
    let mut doc = Document::new();
    let agent = doc.register_agent("bench").unwrap();
    doc.edit(agent, "bench", "seed", Boundary::Auto, |ctx| {
        ctx.insert(0, content)
    })
    .unwrap();
    (doc, agent)
}

// ---------------------------------------------------------------------------
// Scenario 1: Small edit in the middle of a document
// Simulates a typical file save where a few characters changed.
// ---------------------------------------------------------------------------

fn bench_small_edit(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace_content/small_edit");

    for &size in DOC_SIZES {
        let mut rng = StdRng::seed_from_u64(SEED);
        let original = gen_text(&mut rng, size);

        // Replace SMALL_EDIT_CHARS in the middle with different text.
        let mid = size / 2;
        let end = (mid + SMALL_EDIT_CHARS).min(size);
        let replacement = gen_text(&mut rng, end - mid);
        let mut new_content = original.clone();
        // Safe because our alphabet is ASCII.
        new_content.replace_range(mid..end, &replacement);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || doc_with_content(&original),
                |(mut doc, agent)| {
                    doc.replace_content(agent, "bench", "small edit", Boundary::Auto, &new_content)
                        .unwrap()
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 2: Append a line at the end
// Simulates adding a new line to a file.
// ---------------------------------------------------------------------------

fn bench_append_line(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace_content/append_line");

    for &size in DOC_SIZES {
        let mut rng = StdRng::seed_from_u64(SEED);
        let original = gen_text(&mut rng, size);
        let new_content = format!("{original}\nnew line appended here\n");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || doc_with_content(&original),
                |(mut doc, agent)| {
                    doc.replace_content(agent, "bench", "append", Boundary::Auto, &new_content)
                        .unwrap()
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 3: Scattered multi-region edits
// Simulates a refactoring where several non-adjacent regions change.
// ---------------------------------------------------------------------------

/// Number of scattered edit regions.
const SCATTER_REGIONS: usize = 5;

/// Characters replaced per region.
const SCATTER_REGION_SIZE: usize = 8;

fn bench_scattered_edits(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace_content/scattered_edits");

    for &size in DOC_SIZES {
        if size < SCATTER_REGIONS * SCATTER_REGION_SIZE * 3 {
            continue; // skip sizes too small for scattered edits
        }

        let mut rng = StdRng::seed_from_u64(SEED);
        let original = gen_text(&mut rng, size);

        // Edit SCATTER_REGIONS evenly-spaced regions.
        let mut new_content = original.clone();
        let spacing = size / (SCATTER_REGIONS + 1);
        for i in 1..=SCATTER_REGIONS {
            let start = i * spacing;
            let end = (start + SCATTER_REGION_SIZE).min(size);
            let replacement = gen_text(&mut rng, end - start);
            new_content.replace_range(start..end, &replacement);
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || doc_with_content(&original),
                |(mut doc, agent)| {
                    doc.replace_content(agent, "bench", "scatter", Boundary::Auto, &new_content)
                        .unwrap()
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 4: Agent-style rename refactoring
// Simulates renaming identifiers across a code file — same structure, different
// names. Produces many scattered small edits across shared boilerplate.
// This stays on the diff path (SimHash distance is moderate, not extreme).
// ---------------------------------------------------------------------------

/// Number of functions in the generated code-like content.
const CODE_FUNCTIONS: &[usize] = &[5, 50, 200];

/// Generate a code-like file with `n` functions.  Each function uses `prefix`
/// in its identifiers so we can create a "rewritten" variant by swapping the
/// prefix.
fn gen_code_file(n: usize, prefix: &str) -> String {
    use std::fmt::Write;
    let mut buf = String::new();
    buf.push_str("use std::collections::HashMap;\n\n");
    for i in 0..n {
        write!(
            buf,
            "fn {prefix}_func_{i}({prefix}_arg: i32) -> i32 {{\n\
             \x20   let {prefix}_result = {prefix}_arg * {i} + 1;\n\
             \x20   println!(\"{{}} = {{}}\", \"{prefix}_func_{i}\", {prefix}_result);\n\
             \x20   {prefix}_result\n\
             }}\n\n"
        )
        .unwrap();
    }
    buf
}

fn bench_agent_rewrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace_content/agent_rewrite");

    for &n in CODE_FUNCTIONS {
        let original = gen_code_file(n, "old");
        let rewritten = gen_code_file(n, "new");
        let char_count = original.chars().count();

        group.bench_with_input(
            BenchmarkId::new("chars", char_count),
            &char_count,
            |b, _| {
                b.iter_batched(
                    || doc_with_content(&original),
                    |(mut doc, agent)| {
                        doc.replace_content(
                            agent,
                            "bench",
                            "agent rewrite",
                            Boundary::Auto,
                            &rewritten,
                        )
                        .unwrap()
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 5: Full rewrite (completely different content)
// Simulates replacing a file with structurally different code. This is the
// target for the SimHash bulk-replace optimisation: the pre-diff check
// detects that content is very different and skips the O(n*m) Myers diff.
// ---------------------------------------------------------------------------

/// Line counts for the full rewrite benchmark.
const REWRITE_LINE_COUNTS: &[usize] = &[20, 100, 500];

/// Prose line used as the replacement content (completely different trigrams
/// from code).
const PROSE_LINE: &str = "The quick brown fox jumps over the lazy dog repeatedly.\n";

fn bench_full_rewrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace_content/full_rewrite");

    for &n in REWRITE_LINE_COUNTS {
        let original = gen_code_file(n, "old");
        let replacement = PROSE_LINE.repeat(n);
        let char_count = original.chars().count();

        group.bench_with_input(
            BenchmarkId::new("chars", char_count),
            &char_count,
            |b, _| {
                b.iter_batched(
                    || doc_with_content(&original),
                    |(mut doc, agent)| {
                        doc.replace_content(
                            agent,
                            "bench",
                            "full rewrite",
                            Boundary::Auto,
                            &replacement,
                        )
                        .unwrap()
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 6: No-op (identical content)
// Should be near-zero cost — early return path.
// ---------------------------------------------------------------------------

fn bench_noop(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace_content/noop");

    for &size in DOC_SIZES {
        let mut rng = StdRng::seed_from_u64(SEED);
        let content = gen_text(&mut rng, size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || doc_with_content(&content),
                |(mut doc, agent)| {
                    doc.replace_content(agent, "bench", "noop", Boundary::Auto, &content)
                        .unwrap()
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 7: Unicode content — multi-byte characters
// Ensures no hidden penalty from char vs byte arithmetic.
// ---------------------------------------------------------------------------

fn bench_unicode_edit(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace_content/unicode_edit");

    // Build a document with mixed ASCII + CJK + emoji.
    let base_unit = "hello 世界 🌍 test 宇宙 🎉 ";
    for &repeats in &[10, 100, 500] {
        let original: String = base_unit.repeat(repeats);
        let char_count = original.chars().count();

        // Replace a CJK/emoji region in the middle.
        let chars: Vec<char> = original.chars().collect();
        let mid = char_count / 2;
        let end = (mid + 10).min(char_count);
        let mut new_chars = chars.clone();
        for ch in &mut new_chars[mid..end] {
            *ch = '★';
        }
        let new_content: String = new_chars.into_iter().collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(char_count),
            &char_count,
            |b, _| {
                b.iter_batched(
                    || doc_with_content(&original),
                    |(mut doc, agent)| {
                        doc.replace_content(agent, "bench", "unicode", Boundary::Auto, &new_content)
                            .unwrap()
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_small_edit,
    bench_append_line,
    bench_scattered_edits,
    bench_agent_rewrite,
    bench_full_rewrite,
    bench_noop,
    bench_unicode_edit,
);
criterion_main!(benches);
