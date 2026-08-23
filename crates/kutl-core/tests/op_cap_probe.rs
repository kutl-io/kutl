//! Op-count degradation probe — decision support for sizing `MAX_OPS_PER_DOC`.
//!
//! Builds one churn-shaped document (content plateaus at a realistic file
//! size while history grows, the shape long-lived synced docs actually have)
//! and reports, at increasing op counts, the costs that scale with HISTORY
//! rather than content:
//!
//! - `encode_full` time + encoded bytes — the per-save disk cost every
//!   debounced edit pays, and the catch-up frame a fresh joiner downloads;
//! - save+load round-trip — daemon startup / relay cold-load;
//! - a small edit via `replace_content` — the steady-state authoring path;
//! - `blame_with_text` — the full-oplog replay read;
//! - a fresh peer merging the full encode — the relay cold-load / joiner
//!   ingest path, which also shows where `MAX_PATCH_BYTES` starts rejecting
//!   legitimate frames (the gate is sized to the cap, so it must move with
//!   any cap change).
//!
//! Off the normal gate: set `KUTL_OP_PROBE=1` to run —
//! `KUTL_OP_PROBE=1 cargo test -p kutl-core --test op_cap_probe -- --nocapture`.
//! Numbers are same-box relative (compare levels within one run, not across
//! sessions).

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use kutl_core::{Boundary, Document};

/// Content plateau the churn cycles maintain — a realistic hot-doc size (the
/// wedged production doc was ~40KB).
const CONTENT_CHARS: usize = 24_000;

/// Ops consumed per churn cycle: one insert of [`CONTENT_CHARS`] plus one
/// delete of the same span (dt sequence numbers are per character).
const OPS_PER_CYCLE: usize = 2 * CONTENT_CHARS;

/// Op-count levels to report at. Spans the old cap (100K was two levels below
/// the first), the current 1M, and past a candidate 10M so the curve's shape
/// at the candidate is interpolated from data, not extrapolated.
const LEVELS: &[usize] = &[
    250_000, 500_000, 1_000_000, 2_000_000, 4_000_000, 8_000_000, 16_000_000,
];

/// Chars changed by the steady-state small-edit measurement.
const SMALL_EDIT_CHARS: usize = 100;

/// Deterministic word-ish text for cycle `i` — varied enough that the encoded
/// oplog isn't a degenerate RLE/LZ4 best case (real content compresses far
/// worse than a repeated single char).
fn cycle_block(i: usize) -> String {
    let mut s = String::with_capacity(CONTENT_CHARS + 64);
    let mut x = (i as u64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    while s.chars().count() < CONTENT_CHARS {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let _ = write!(s, "word{:x} edit{} relay daemon sync ", x >> 40, i);
    }
    s.truncate(
        s.char_indices()
            .nth(CONTENT_CHARS)
            .map_or(s.len(), |(b, _)| b),
    );
    s
}

/// A duration as fractional milliseconds for the report lines.
fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[test]
fn probe_op_count_degradation() {
    if std::env::var("KUTL_OP_PROBE").is_err() {
        eprintln!("skipping op-count degradation probe (set KUTL_OP_PROBE=1 to run)");
        return;
    }

    let dir = tempfile::tempdir().expect("probe tempdir");
    let dt_path = dir.path().join("probe.dt");

    let mut doc = Document::new();
    let agent = doc.register_agent("probe").expect("agent");
    let seed = cycle_block(0);
    doc.edit(agent, "did:probe", "seed", Boundary::Auto, |ctx| {
        ctx.insert(0, &seed)
    })
    .expect("seed");

    eprintln!(
        "OPPROBE content plateau ~{CONTENT_CHARS} chars, {OPS_PER_CYCLE} ops/cycle; \
         columns: ops | encode_full ms / bytes | save+load ms | small-edit ms | blame ms | fresh-merge ms"
    );

    let mut cycle = 1usize;
    for &level in LEVELS {
        // Churn up to the level: append a fresh block, delete the oldest span.
        while doc.op_count() < level {
            let block = cycle_block(cycle);
            let len = doc.content().chars().count();
            doc.edit(agent, "did:probe", "churn", Boundary::Auto, |ctx| {
                ctx.insert(len, &block)?;
                ctx.delete(0..CONTENT_CHARS)
            })
            .expect("churn edit");
            cycle += 1;
        }
        let ops = doc.op_count();

        let t = Instant::now();
        let encoded = doc.encode_full();
        let encode_ms = ms(t.elapsed());

        let t = Instant::now();
        doc.save(&dt_path).expect("save");
        let loaded = Document::load(&dt_path).expect("load");
        let save_load_ms = ms(t.elapsed());
        assert_eq!(loaded.op_count(), ops, "round-trip preserves history");

        // Steady-state authoring: change SMALL_EDIT_CHARS in the middle.
        let content = doc.content();
        let mid = content
            .char_indices()
            .nth(content.chars().count() / 2)
            .map_or(0, |(b, _)| b);
        let mut edited = content.clone();
        edited.replace_range(mid..mid, &"x".repeat(SMALL_EDIT_CHARS));
        let t = Instant::now();
        let outcome = doc
            .replace_content(agent, "did:probe", "small edit", Boundary::Auto, &edited)
            .expect("small edit");
        let small_edit_ms = ms(t.elapsed());
        assert!(!outcome.was_bulk, "a 100-char edit must take the diff path");

        let t = Instant::now();
        let rows = doc.blame_with_text();
        let blame_ms = ms(t.elapsed());
        assert!(!rows.is_empty(), "blame produced rows");

        // Fresh-peer ingest of the full encode — the relay cold-load / joiner
        // path. MAX_PATCH_BYTES gates this; a rejection here is the coupling
        // datum, not a probe failure.
        let t = Instant::now();
        let fresh_merge = match Document::new().merge(&encoded, &[]) {
            Ok(()) => format!("{:.1}", ms(t.elapsed())),
            Err(e) => format!("REJECTED ({e})"),
        };

        eprintln!(
            "OPPROBE {ops:>10} | {encode_ms:>8.1} ms / {encoded_bytes:>10} B | \
             {save_load_ms:>8.1} | {small_edit_ms:>7.1} | {blame_ms:>8.1} | {fresh_merge}",
            encoded_bytes = encoded.len(),
        );
    }
}
