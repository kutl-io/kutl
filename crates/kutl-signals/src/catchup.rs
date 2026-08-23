//! HLC cursor bookkeeping for the HTTP catch-up exchange. Both ends persist
//! a high-water mark per space; catch-up streams records strictly above it.

use std::io;
use std::path::Path;

use kutl_proto::sync::Signal;

use crate::error::{Error, Result};
use crate::fold::proto_hlc_to_core;

/// Cursor filename inside a space's signals directory.
pub const CURSOR_FILE_NAME: &str = "cursor.json";

/// Serialized cursor shape (proto `Hlc` fields, JSON for debuggability —
/// the cursor is bookkeeping, not a record; it is NOT part of the
/// canonical storage format).
#[derive(serde::Serialize, serde::Deserialize)]
struct CursorFile {
    physical_ms: u64,
    logical: u32,
    /// 16-byte actor UUID. A proto actor field that is not exactly 16 bytes
    /// is stored as all-zeros (nil actor) — consistent with the fold's
    /// nil-actor rule for malformed HLC bytes.
    actor: [u8; 16],
}

/// Load the persisted high-water mark.
///
/// Returns `None` when no cursor file exists OR when it fails to parse —
/// either way catch-up streams from the zero clock (i.e., all records).
/// An unparseable cursor (e.g. truncated on unclean shutdown, consistent
/// with the documented no-fsync posture) must not wedge catch-up;
/// from-zero is the safe direction: re-overlap only, idempotent union
/// downstream, no data loss by construction.
///
/// Uses open-then-read rather than `.exists()` to avoid the TOCTOU window
/// between the existence check and the open.
pub fn load_cursor(dir: &Path) -> Result<Option<kutl_proto::sync::Hlc>> {
    let path = dir.join(CURSOR_FILE_NAME);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Error::CursorRead { path, source: e }),
    };
    let Ok(c) = serde_json::from_slice::<CursorFile>(&bytes) else {
        // unparseable cursor = missing cursor (see doc comment above)
        return Ok(None);
    };
    Ok(Some(kutl_proto::sync::Hlc {
        physical_ms: c.physical_ms,
        logical: c.logical,
        actor: c.actor.to_vec(),
    }))
}

/// Persist the high-water mark atomically (write tmp + rename).
///
/// **MONOTONIC: a cursor never moves backwards.** A resume point that can
/// regress is not a resume point — and it cannot merely regress, it can
/// OVERSHOOT, which is unrecoverable. [`page`] and [`records_after`] both
/// resume STRICTLY after the stored `physical_ms`, so a cursor advanced past
/// records that were never ingested filters them out on this and every future
/// connect; nothing re-fetches them and only deleting the file recovers.
///
/// Callers write this per record, and their records do not always arrive in
/// HLC order: the daemon ingests backlog pages and live broadcasts through one
/// event loop, so a live record stamped `now` can land between two pages of
/// older history. The guard belongs to the cursor rather than to each caller —
/// that is what makes it hold for the CLI's `--fetch` path too.
///
/// A regressing write is a silent no-op, deliberately: the caller has nothing
/// useful to do about it, and the stored value is already the better one.
///
/// Durability note: neither the tmp write nor the rename is fsynced. A
/// cursor loss on an unclean shutdown causes harmless catch-up overlap —
/// the downstream union is idempotent — so the overhead of fsync is
/// unwarranted here.
pub fn save_cursor(dir: &Path, hlc: &kutl_proto::sync::Hlc) -> Result<()> {
    // Compare on the FULL HLC order, not just physical_ms: two records can
    // share a millisecond and the later one must still advance the cursor, or a
    // same-ms group would be re-fetched forever.
    if let Some(stored) = load_cursor(dir)?
        && (stored.physical_ms, stored.logical, stored.actor.clone())
            >= (hlc.physical_ms, hlc.logical, hlc.actor.clone())
    {
        return Ok(());
    }
    // A proto actor field that is not exactly 16 bytes is stored as
    // all-zeros (nil actor), consistent with the fold's nil-actor rule.
    let actor: [u8; 16] = hlc.actor.as_slice().try_into().unwrap_or([0u8; 16]);
    let c = CursorFile {
        physical_ms: hlc.physical_ms,
        logical: hlc.logical,
        actor,
    };
    let tmp = dir.join(format!("{CURSOR_FILE_NAME}.tmp"));
    let dest = dir.join(CURSOR_FILE_NAME);
    std::fs::write(&tmp, serde_json::to_vec(&c)?).map_err(|e| Error::CursorWrite {
        path: tmp.clone(),
        source: e,
    })?;
    std::fs::rename(&tmp, &dest).map_err(|e| Error::CursorWrite {
        path: dest,
        source: e,
    })?;
    Ok(())
}

/// One page of catch-up records.
///
/// Produced by [`page`]. Callers MUST persist the page before advancing their
/// cursor to `high_water` — advancing past an unpersisted page forms a hole
/// that idempotent union cannot heal.
pub struct CatchUpPage<'a> {
    /// Records in the shared HLC total order `(hlc, actor_did, record_id)`, a
    /// prefix of the sorted-filtered set covering only COMPLETE `physical_ms`
    /// groups. At most `max` in the common case, but a single `physical_ms`
    /// group larger than `max` OVERFLOWS whole (the soft cap that guarantees
    /// progress) — so a page may exceed `max` when one ms group demands it.
    pub records: Vec<&'a Signal>,
    /// The coarse `physical_ms` of the last emitted group (as an `Hlc` with
    /// `logical = 0`, `actor = <nil>`), or `since` when `records` is empty.
    /// Advance the cursor here after persisting `records`; resuming at this
    /// `physical_ms` skips the whole emitted group (see [`records_after`]).
    pub high_water: Option<kutl_proto::sync::Hlc>,
    /// `true` when further records exist beyond this page (i.e., a next call
    /// resuming at `high_water.physical_ms` will return at least one record).
    pub more: bool,
}

/// Return one catch-up page: records strictly after the coarse `physical_ms`
/// floor in `since`, sorted by the shared HLC total order
/// `(hlc, actor_did, record_id)`, emitting only COMPLETE `physical_ms` groups
/// up to `max`.
///
/// **Gap-free-at-`physical_ms`-granularity contract.** The GET catch-up cursor
/// is a coarse `physical_ms` value — the URL carries no logical/actor part, so
/// a client resumes with `Hlc { physical_ms, logical: 0, actor: <nil> }`. To
/// advance the cursor without dropping or re-serving records, a page NEVER
/// splits a `physical_ms` group across the `max` boundary:
///
/// - After sorting, cut near `max`, then TRIM back to the last COMPLETE
///   `physical_ms` boundary. Every earlier `physical_ms` group is fully
///   present (the slice is contiguous and sorted), so it is safe to emit and to
///   advance the cursor past.
/// - When a SINGLE `physical_ms` group alone exceeds `max`, OVERFLOW: emit the
///   whole group anyway (a soft cap — the only way to guarantee forward
///   progress). This is the same gap-free keyset advance the `SQLite`
///   change-feed uses (`trim_to_complete_groups` / `PageTrim::Overflow`).
///
/// Two properties follow:
/// - **No same-`physical_ms` livelock.** The client resumes at
///   `high_water.physical_ms` and [`records_after`] filters `physical_ms >
///   cursor` — strictly PAST the emitted group. A cluster of >`max` records at
///   one `physical_ms` is drained in one overflow page, not re-served forever.
/// - **No equal-HLC hole.** All records sharing a full HLC necessarily share a
///   `physical_ms`, so a complete-group page never splits an equal-HLC run
///   across a boundary.
///
/// **Correctness invariant**: sorting happens BEFORE cutting and computing
/// `high_water`, and the cut is trimmed to a complete `physical_ms` boundary.
///
/// `since = None` is a from-zero page: yields ALL records (including
/// missing-HLC ones, which sort first as the zero clock). `since = Some(floor)`
/// yields records whose `physical_ms` is strictly greater than
/// `floor.physical_ms`; missing-HLC records fold to `physical_ms = 0` and are
/// therefore never "after" any `Some(floor)`.
///
/// `max = 0` is legal: it emits the FIRST complete `physical_ms` group (the
/// overflow floor guarantees progress even at `max = 0` — a zero-record page
/// with more records pending would livelock), with `more` reflecting whether
/// records remain beyond it.
pub fn page<'a>(
    records: &'a [Signal],
    since: Option<&kutl_proto::sync::Hlc>,
    max: usize,
) -> CatchUpPage<'a> {
    // 1. Filter to records strictly after the coarse `physical_ms` floor.
    let mut filtered: Vec<&'a Signal> = records_after(records, since).collect();

    // 2. Sort by the full total order (hlc, actor_did, record_id) — identical
    //    to the fold's LWW key — BEFORE cutting.  The sort key uses
    //    proto_hlc_to_core so the ordering is byte-for-byte the same as the
    //    fold; missing HLCs sort first (zero clock).
    filtered.sort_by(|a, b| {
        let ka = (
            proto_hlc_to_core(a.hlc.as_ref()),
            a.actor_did.as_str(),
            a.record_id.as_str(),
        );
        let kb = (
            proto_hlc_to_core(b.hlc.as_ref()),
            b.actor_did.as_str(),
            b.record_id.as_str(),
        );
        ka.cmp(&kb)
    });

    // 3. Emit only complete `physical_ms` groups. `cut` is the number of
    //    records to keep so that no `physical_ms` group is split across the
    //    page boundary. `more` is whether any records remain past the cut.
    let ms_of = |r: &Signal| proto_hlc_to_core(r.hlc.as_ref()).physical_ms;
    let cut = complete_group_cut(&filtered, max, ms_of);
    let more = cut < filtered.len();
    let page_records: Vec<&'a Signal> = filtered.into_iter().take(cut).collect();

    // 4. high_water = the coarse `physical_ms` of the last emitted group, or
    //    `since` when the page is empty (do not retreat the cursor). The client
    //    resumes at this `physical_ms`; the filter's strict-> then advances past
    //    the whole emitted group.
    let high_water = page_records
        .last()
        .map(|r| kutl_proto::sync::Hlc {
            physical_ms: ms_of(r),
            logical: 0,
            actor: vec![0u8; 16],
        })
        .or_else(|| since.cloned());

    CatchUpPage {
        records: page_records,
        high_water,
        more,
    }
}

/// Number of leading records to emit so that no `physical_ms` group is split
/// across the `max` page boundary — the gap-free keyset advance (mirrors the
/// relay's `SQLite`-side `trim_to_complete_groups` / `PageTrim::Overflow`).
///
/// `filtered` is sorted ascending by the total order (so `physical_ms` is
/// non-decreasing). Returns a cut index into `filtered`:
/// - fewer than `max` records total → keep everything (nothing straddles a
///   non-existent next page);
/// - otherwise trim back to the last COMPLETE `physical_ms` boundary at or
///   before `max`;
/// - if that trim would keep nothing (a single `physical_ms` group already
///   exceeds `max`, or `max == 0`), OVERFLOW: emit that whole first group so
///   the cursor can always advance (a zero-record page with records pending
///   would livelock).
fn complete_group_cut(filtered: &[&Signal], max: usize, ms_of: impl Fn(&Signal) -> u64) -> usize {
    // Short set: everything past the cursor is present; nothing can straddle a
    // (non-existent) next page.
    if filtered.len() <= max {
        return filtered.len();
    }

    // The `physical_ms` group straddling the `max` boundary is (possibly)
    // incomplete. Trim back to just before it: keep every record whose
    // `physical_ms` is strictly less than the boundary group's.
    let boundary_ms = ms_of(filtered[max]);
    let trimmed = filtered
        .iter()
        .take_while(|r| ms_of(r) < boundary_ms)
        .count();
    if trimmed > 0 {
        return trimmed;
    }

    // Overflow: the very first group (which starts at index 0) already spans the
    // whole `max` window — emit it whole so the cursor advances past it. Its
    // `physical_ms` is `filtered[0]`'s; extend the cut to the end of that group.
    let first_ms = ms_of(filtered[0]);
    filtered.iter().take_while(|r| ms_of(r) == first_ms).count()
}

/// Records strictly after the COARSE `physical_ms` floor carried by `since`
/// (the catch-up page filter).
///
/// A catch-up resume floor can be COARSE: a cursor that survives persistence
/// or a wire round-trip may carry only `physical_ms`, rebuilt as
/// `Hlc { physical_ms, logical: 0, actor: <nil> }`. To advance PAST a whole
/// emitted `physical_ms` group (and never re-serve it), the filter compares on
/// `physical_ms` ALONE: `record.physical_ms > since.physical_ms`. This pairs
/// with [`page`], which only ever emits COMPLETE `physical_ms` groups, so a
/// strictly-greater-by-ms resume drops nothing that was not already delivered.
/// A full-HLC strictly-after comparison would instead re-include the just-
/// emitted `physical_ms = T` group under the coarse floor `(T, 0, nil)` (every
/// record at T sorts after it), which cannot advance the cursor — the livelock.
///
/// OUTPUT PRESERVES INPUT ORDER — it is a filter, not a sort, and
/// `SegmentStore::records` is a non-semantic multiset order. Pagers MUST
/// sort by the shared HLC total order before cutting a page and advancing
/// `high_water`.
///
/// `since = None` means from-zero: ALL records are yielded, including those
/// with a missing HLC (which otherwise fold as the zero clock, `physical_ms =
/// 0`, and would be excluded by any `Some(floor)` at or above zero). This is
/// the type-enforced version of the from-zero intent — a `Some(&zero_hlc)`
/// would silently drop missing-HLC records because `0 > 0` is false.
///
/// `since = Some(floor)` yields records whose `physical_ms` is strictly greater
/// than `floor.physical_ms`. Records at exactly `floor.physical_ms` (any
/// logical/actor) are excluded — they belong to the group that already
/// advanced the cursor. Records with a missing HLC fold to `physical_ms = 0`
/// and are therefore never "after" a `Some(floor)` — they are only delivered
/// in from-zero catch-ups (`None`).
///
/// COARSE-CURSOR TRADE-OFF: a record whose
/// `physical_ms` is at or below an already-advanced subscriber cursor is
/// unreachable via this filter — it surfaces only on a fresh from-zero catch-up
/// (`None`). This matters for the re-seed path (`handle_signal_reseed`), which
/// ingests records WITHOUT broadcasting them: a re-seeded record below a live
/// cursor is only re-served from zero, never live.
/// The prefix a paged `RecordLog` fetch must return so that [`page`] behaves
/// exactly as if it had been handed the whole space.
///
/// A record log that pages cannot simply `LIMIT n`: [`page`] emits only
/// COMPLETE `physical_ms` groups, and it decides completeness from the slice in
/// front of it. Hand it a slice cut mid-group and it reads that group as
/// finished, emits it, and advances the cursor past records it never sent —
/// a hole, and a silent one, since every frame involved looks well-formed.
///
/// So the rule is `limit` records **plus the remainder of the group the
/// `limit`-th record belongs to**. Callers ask for one more than the page size
/// they intend to serve, which is what lets [`page`] compute `more` honestly:
/// its `more` is `cut < filtered.len()`, so the slice has to contain at least
/// one record beyond the page for "there is more" to be distinguishable from
/// "that was everything".
///
/// Defined here, next to [`page`], because it is the same boundary rule read
/// from the other side — and because two substrates implement it, one in Rust
/// over a sequential file read and one in SQL over a keyset index. A shared
/// definition is what a differential test can hold them both to.
pub fn take_with_complete_boundary<'a>(
    records: &'a [Signal],
    since: Option<&kutl_proto::sync::Hlc>,
    limit: usize,
) -> Vec<&'a Signal> {
    let ms_of = |r: &Signal| proto_hlc_to_core(r.hlc.as_ref()).physical_ms;
    let mut filtered: Vec<&'a Signal> = records_after(records, since).collect();
    filtered.sort_by(|a, b| {
        let ka = (
            proto_hlc_to_core(a.hlc.as_ref()),
            a.actor_did.as_str(),
            a.record_id.as_str(),
        );
        let kb = (
            proto_hlc_to_core(b.hlc.as_ref()),
            b.actor_did.as_str(),
            b.record_id.as_str(),
        );
        ka.cmp(&kb)
    });
    if filtered.len() <= limit {
        return filtered;
    }
    // Extend past `limit` to the end of the boundary group. At `limit = 0` the
    // boundary is the FIRST group, which is what the pager's overflow floor
    // needs: a zero-record page while records remain would livelock.
    //
    // `end >= limit` holds by construction — every record before `limit` shares
    // or precedes `boundary_ms`, so the first record past it cannot come
    // earlier — which is why there is no clamp here.
    let boundary_ms = ms_of(filtered[limit.saturating_sub(1)]);
    let end = filtered
        .iter()
        .position(|r| ms_of(r) > boundary_ms)
        .unwrap_or(filtered.len());
    // ...then ONE record further, and the `+ 1` is not an off-by-one guard.
    // `page` reports `more` as "the slice held records past the cut", and its
    // cut is not bounded by `limit`: a single `physical_ms` group larger than
    // the page size OVERFLOWS and is emitted whole. So a slice ending at the
    // group boundary can be entirely consumed by one page, leaving `more` false
    // while the log still holds records — the client stops paging and never
    // learns the rest exists. The differential test catches this at `max = 0`,
    // where every page overflows by construction.
    //
    // The extra record is never emitted: `end > limit > cut` in the ordinary
    // case and `cut == end` in the overflow case, so it only ever serves as
    // evidence that paging should continue.
    filtered.truncate(end + 1);
    filtered
}

pub fn records_after<'a>(
    records: &'a [Signal],
    since: Option<&kutl_proto::sync::Hlc>,
) -> impl Iterator<Item = &'a Signal> {
    let floor_ms = since.map(|h| h.physical_ms);
    records.iter().filter(move |r| {
        match floor_ms {
            // From-zero: yield everything, including missing-HLC records.
            None => true,
            // Strictly-after by physical_ms: the coarse resume key the GET
            // cursor carries. A missing HLC folds to physical_ms = 0, so it is
            // never after any floor ≥ 0.
            Some(f) => proto_hlc_to_core(r.hlc.as_ref()).physical_ms > f,
        }
    })
}

/// The floor to RESUME catch-up from, given a persisted cursor. `None`
/// means resume from zero.
///
/// A persisted cursor can sit MID-GROUP: live ingest advances it to each
/// broadcast record's HLC, and an eviction or restart can land between two
/// records sharing one `physical_ms`. The serve filter is coarse
/// ([`records_after`]: strictly-after by `physical_ms` alone), which is
/// sound only for the complete-group boundaries a page's high-water names —
/// a mid-group floor silently excludes the rest of its own millisecond, and
/// because every later cursor is at least as large, that gap never heals
/// short of a from-zero re-walk. Resuming one millisecond EARLIER re-serves
/// the cursor's whole group instead: the overlap is idempotent (ingest
/// set-unions on `record_id`); a lost sibling is permanent.
///
/// A cursor at `physical_ms == 0` resumes from zero rather than from a
/// floor of 0 — a zero floor would still exclude the ms-0 group the cursor
/// may have split.
///
/// If [`records_after`] ever becomes full-precision (strict-after over the
/// total record order), this rewind becomes unnecessary — retire them
/// together or not at all.
#[must_use]
pub fn resume_floor(cursor: &kutl_proto::sync::Hlc) -> Option<kutl_proto::sync::Hlc> {
    if cursor.physical_ms == 0 {
        return None;
    }
    Some(kutl_proto::sync::Hlc {
        physical_ms: cursor.physical_ms - 1,
        logical: 0,
        actor: vec![0u8; 16],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn hlc(ms: u64, logical: u32) -> kutl_proto::sync::Hlc {
        kutl_proto::sync::Hlc {
            physical_ms: ms,
            logical,
            actor: vec![0u8; 16],
        }
    }

    /// The cursor NEVER moves backwards, and overshooting is why.
    ///
    /// The daemon ingests backlog pages and live broadcasts through one event
    /// loop, so a live record stamped `now` can land between two pages of older
    /// history. Without this guard that write sticks: `page`/`records_after`
    /// resume strictly after the stored `physical_ms`, so the un-ingested
    /// remainder of the backlog is filtered out on every future connect and
    /// only deleting `cursor.json` recovers it.
    #[test]
    fn test_cursor_never_regresses_so_a_late_write_cannot_overshoot() {
        let dir = TempDir::new().unwrap();

        // A backlog page lands, then a LIVE record stamped far later.
        save_cursor(dir.path(), &hlc(100, 0)).unwrap();
        save_cursor(dir.path(), &hlc(9_000, 0)).unwrap();
        assert_eq!(
            load_cursor(dir.path()).unwrap().unwrap().physical_ms,
            9_000,
            "a forward write advances"
        );

        // The next backlog page — older than the live record — must NOT move the
        // cursor back, and equally must not be able to move it forward past
        // itself later.
        save_cursor(dir.path(), &hlc(200, 0)).unwrap();
        assert_eq!(
            load_cursor(dir.path()).unwrap().unwrap().physical_ms,
            9_000,
            "an older write is a silent no-op, never a regression"
        );

        // Same millisecond, higher logical: this MUST advance, or a same-ms
        // group would be re-fetched forever.
        save_cursor(dir.path(), &hlc(9_000, 1)).unwrap();
        let c = load_cursor(dir.path()).unwrap().unwrap();
        assert_eq!(
            (c.physical_ms, c.logical),
            (9_000, 1),
            "the full HLC orders the comparison, not physical_ms alone"
        );

        // And an equal write is a no-op rather than an error.
        save_cursor(dir.path(), &hlc(9_000, 1)).unwrap();
        assert_eq!(
            load_cursor(dir.path()).unwrap().unwrap().logical,
            1,
            "an equal write leaves the stored value alone"
        );
    }

    #[test]
    fn test_cursor_roundtrip_and_missing_defaults_to_zero() {
        let dir = TempDir::new().unwrap();
        assert!(
            load_cursor(dir.path()).unwrap().is_none(),
            "no cursor file = from zero"
        );
        save_cursor(dir.path(), &hlc(42, 7)).unwrap();
        let c = load_cursor(dir.path()).unwrap().unwrap();
        assert_eq!((c.physical_ms, c.logical), (42, 7));
    }

    #[test]
    fn test_save_cursor_no_partial_file_after_success() {
        let dir = TempDir::new().unwrap();
        save_cursor(dir.path(), &hlc(1, 0)).unwrap();
        save_cursor(dir.path(), &hlc(2, 0)).unwrap();
        assert!(!dir.path().join(format!("{CURSOR_FILE_NAME}.tmp")).exists());
        assert_eq!(load_cursor(dir.path()).unwrap().unwrap().physical_ms, 2);
    }

    #[test]
    fn test_load_cursor_corrupt_file_means_from_zero() {
        let dir = TempDir::new().unwrap();
        // A truncated/garbage cursor (unclean shutdown under the no-fsync
        // posture) must not wedge catch-up — it reads as "no cursor".
        std::fs::write(dir.path().join(CURSOR_FILE_NAME), b"{\"physical_ms\":4").unwrap();
        assert!(
            load_cursor(dir.path()).unwrap().is_none(),
            "unparseable cursor = from-zero catch-up, not an error"
        );
    }

    #[test]
    fn test_load_cursor_propagates_non_notfound_io_error() {
        let dir = TempDir::new().unwrap();
        // A genuine IO fault reading the cursor (NOT a missing file, NOT an
        // unparseable one) must SURFACE — otherwise a permission/hardware fault
        // masquerades as "no cursor" and silently rewinds catch-up to zero.
        // Reading a path that is a directory fails with a non-NotFound io error.
        std::fs::create_dir(dir.path().join(CURSOR_FILE_NAME)).unwrap();
        let err = load_cursor(dir.path())
            .expect_err("a non-NotFound io error must propagate, not read as from-zero");
        assert!(
            matches!(err, Error::CursorRead { .. }),
            "expected CursorRead, got {err:?}"
        );
    }

    #[test]
    fn test_records_after_filters_by_hlc_order() {
        use kutl_proto::sync::Signal;
        let recs: Vec<Signal> = (1..=5u64)
            .map(|ms| Signal {
                record_id: format!("r{ms}"),
                hlc: Some(hlc(ms, 0)),
                ..Default::default()
            })
            .collect();
        let after: Vec<_> = records_after(&recs, Some(&hlc(3, 0)))
            .map(|r| r.record_id.clone())
            .collect();
        assert_eq!(after, vec!["r4", "r5"]);
    }

    #[test]
    fn test_records_after_coarse_physical_ms_boundary_excludes_whole_ms_group() {
        use kutl_proto::sync::Signal;
        // The GET cursor is a COARSE physical_ms floor. A resume at
        // physical_ms=3 excludes the ENTIRE physical_ms=3 group — every record
        // at ms=3 (whatever its logical/actor) belongs to the group that
        // advanced the cursor. `page` only ever emits COMPLETE physical_ms
        // groups, so this drops nothing that was not already delivered — and it
        // is what lets the cursor advance PAST a same-ms cluster (no livelock).
        let at_cursor = Signal {
            record_id: "at".into(),
            hlc: Some(hlc(3, 0)),
            ..Default::default()
        };
        // A record with no HLC folds to physical_ms = 0 — excluded above zero.
        let no_hlc = Signal {
            record_id: "none".into(),
            hlc: None,
            ..Default::default()
        };
        // A record one LOGICAL tick above the cursor at the SAME physical_ms is
        // part of the ms=3 group and is EXCLUDED (coarse floor). A full-HLC
        // "strictly-after" would instead include it — the very re-inclusion
        // that livelocked a same-ms cluster.
        let logical_tick = Signal {
            record_id: "tick".into(),
            hlc: Some(hlc(3, 1)),
            ..Default::default()
        };
        // A record at a strictly-greater physical_ms IS included.
        let next_ms = Signal {
            record_id: "next".into(),
            hlc: Some(hlc(4, 0)),
            ..Default::default()
        };
        let recs = vec![at_cursor, no_hlc, logical_tick, next_ms];
        let after: Vec<_> = records_after(&recs, Some(&hlc(3, 0)))
            .map(|r| r.record_id.clone())
            .collect();
        assert_eq!(
            after,
            vec!["next"],
            "coarse physical_ms floor excludes the whole ms=3 group; only ms>3 is after"
        );
    }

    /// `records_after(recs, None)` is a from-zero catch-up: yields ALL
    /// records, including those with a missing HLC that a `Some(zero_hlc)`
    /// floor would silently drop (zero > zero is false).
    #[test]
    fn test_records_after_none_yields_all_including_missing_hlc() {
        use kutl_proto::sync::Signal;
        let has_hlc = Signal {
            record_id: "has-hlc".into(),
            hlc: Some(hlc(5, 0)),
            ..Default::default()
        };
        let no_hlc = Signal {
            record_id: "no-hlc".into(),
            hlc: None,
            ..Default::default()
        };
        let recs = vec![has_hlc, no_hlc];
        let mut ids: Vec<_> = records_after(&recs, None)
            .map(|r| r.record_id.clone())
            .collect();
        ids.sort();
        assert_eq!(
            ids,
            vec!["has-hlc", "no-hlc"],
            "from-zero (None) must yield every record including missing-HLC ones"
        );
    }

    // ── page() tests ──────────────────────────────────────────────────────────

    /// Build a Signal with explicit HLC and distinct `actor`/`record_id` for
    /// tie-breaking.
    fn sig(ms: u64, logical: u32, actor_suffix: &str, record_suffix: &str) -> Signal {
        Signal {
            record_id: format!("rec-{record_suffix}"),
            actor_did: format!("did:key:zActor{actor_suffix}"),
            hlc: Some(hlc(ms, logical)),
            ..Default::default()
        }
    }

    /// GUARD (mid-group resume): a cursor persisted from a LIVE record can
    /// split its own `physical_ms` group, and the coarse serve filter then
    /// excludes the unseen rest of that group forever — the permanent-gap
    /// shape. `resume_floor` rewinds one millisecond so the whole group is
    /// re-served; overlap is idempotent, a lost sibling is not.
    ///
    /// The first half of this test PINS the hazard itself: if
    /// [`records_after`] ever becomes full-precision, that assertion fails —
    /// the cue that `resume_floor` can be retired with it (they are one
    /// contract; see `resume_floor`'s doc).
    #[test]
    fn test_resume_floor_recovers_the_mid_group_sibling() {
        // Two records in one millisecond group: the subscriber received r1,
        // persisted its full HLC as the cursor, and was evicted before r2.
        let records = vec![sig(5, 0, "A", "r1"), sig(5, 1, "A", "r2")];
        let mid_group_cursor = hlc(5, 0);

        // The hazard, pinned: resuming from the raw cursor loses r2.
        let naive = page(&records, Some(&mid_group_cursor), 10);
        assert!(
            naive.records.is_empty(),
            "the coarse filter excludes the cursor's own group — this pin \
             failing means records_after went full-precision and resume_floor \
             should be retired with it: {:?}",
            naive.records
        );

        // The rescue: the rewound floor re-serves the whole group.
        let floor = resume_floor(&mid_group_cursor);
        let rescued = page(&records, floor.as_ref(), 10);
        let ids: Vec<&str> = rescued
            .records
            .iter()
            .map(|r| r.record_id.as_str())
            .collect();
        assert_eq!(
            ids,
            vec!["rec-r1", "rec-r2"],
            "resuming one ms early re-serves the split group; ingest dedups r1"
        );
    }

    /// `resume_floor` edges: a zero-ms cursor resumes from zero (a floor of 0
    /// would still exclude the ms-0 group), and a ms-1 cursor floors at 0
    /// (which serves ms 1 onward — the cursor's own group included).
    #[test]
    fn test_resume_floor_zero_and_one_ms_edges() {
        assert!(
            resume_floor(&hlc(0, 3)).is_none(),
            "a ms-0 cursor must resume from zero, not from a floor of 0"
        );
        let floor = resume_floor(&hlc(1, 3)).expect("ms-1 cursor floors");
        assert_eq!(floor.physical_ms, 0);
        assert_eq!(floor.logical, 0, "the floor is synthetic, not a record HLC");

        // The floored resume actually includes the ms-1 group.
        let records = vec![sig(1, 0, "A", "a"), sig(1, 7, "A", "b")];
        let rescued = page(&records, Some(&floor), 10);
        assert_eq!(rescued.records.len(), 2);
    }

    /// **Paging through a bounded fetch must be indistinguishable from paging
    /// through the whole space.** The differential test the two `RecordLog`
    /// substrates are held to.
    ///
    /// `take_with_complete_boundary` exists so a database can serve a page with
    /// a keyset scan instead of fetching every record a space has ever held. The
    /// risk in that is not performance, it is silence: cut a `physical_ms` group
    /// in half and `page` reads the half as a finished group, emits it, and
    /// advances the cursor past records it never sent. Nothing errors. The
    /// client simply never learns those records exist.
    ///
    /// So rather than assert properties of one page, this drives a full
    /// cursor-following loop both ways and requires the transcripts to be
    /// identical — same records, same order, same `more`, same high-water at
    /// every step. Corpora are chosen for the boundaries that rule exists for.
    #[test]
    fn test_paging_a_bounded_fetch_matches_paging_the_whole_space() {
        // Each corpus names the boundary it probes.
        let corpora: Vec<(&str, Vec<Signal>)> = vec![
            ("empty", vec![]),
            ("one record", vec![sig(10, 0, "a", "a")]),
            (
                // The case a plain LIMIT breaks: a group straddling the cut.
                "a group larger than the page, straddling the boundary",
                (0..7).map(|i| sig(10, i, "a", &format!("g{i}"))).collect(),
            ),
            (
                // Every record its own group — the cut lands cleanly every time,
                // which is where an off-by-one in `more` hides instead.
                "one record per millisecond",
                (0..9_u32)
                    .map(|i| sig(u64::from(i) + 1, 0, "a", &format!("s{i}")))
                    .collect(),
            ),
            (
                // Groups of two with the page size odd, so the cut lands
                // mid-group on alternating pages.
                "even groups against an odd page size",
                (0..12_u32)
                    .map(|i| sig(u64::from(i / 2) + 1, i % 2, "a", &format!("p{i}")))
                    .collect(),
            ),
            (
                // Missing-HLC records fold to physical_ms 0 and are reachable
                // only from zero — they must not be silently dropped by the
                // bounded fetch on the FIRST page, where they do belong.
                "missing-HLC records mixed with stamped ones",
                vec![
                    Signal {
                        record_id: "rec-none-1".into(),
                        actor_did: "did:key:zActorA".into(),
                        hlc: None,
                        ..Default::default()
                    },
                    sig(5, 0, "b", "b5"),
                    Signal {
                        record_id: "rec-none-2".into(),
                        actor_did: "did:key:zActorC".into(),
                        hlc: None,
                        ..Default::default()
                    },
                    sig(6, 0, "d", "d6"),
                ],
            ),
        ];

        // Page sizes bracketing the group sizes above: smaller than a group,
        // equal to one, and larger than the whole corpus.
        for max in [0_usize, 1, 2, 3, 5, 100] {
            for (name, records) in &corpora {
                // The whole-space read ignores both cursor and limit — that is what makes
                // it the reference: `page` alone decides the page.
                let whole = drain(records, max, |recs, _, _| recs.to_vec());
                let bounded = drain(records, max, |recs, since, limit| {
                    take_with_complete_boundary(recs, since, limit)
                        .into_iter()
                        .cloned()
                        .collect()
                });
                assert_eq!(
                    whole, bounded,
                    "corpus {name:?} at max={max}: a bounded fetch paged differently \
                     from the whole space"
                );
            }
        }
    }

    /// Follow the cursor to exhaustion, recording what each page emitted.
    ///
    /// `fetch` stands in for the record log: either "hand back everything" (the
    /// whole-space read) or "hand back the bounded prefix". Returns one entry
    /// per page so a divergence reports WHICH page differed, not just that the
    /// totals did.
    fn drain(
        records: &[Signal],
        max: usize,
        fetch: impl Fn(&[Signal], Option<&kutl_proto::sync::Hlc>, usize) -> Vec<Signal>,
    ) -> Vec<(Vec<String>, bool, Option<u64>)> {
        let mut cursor: Option<kutl_proto::sync::Hlc> = None;
        let mut pages = Vec::new();
        // A page that emits nothing while claiming more would loop forever; the
        // bound turns that livelock into a readable failure instead of a hang.
        for _ in 0..64 {
            let fetched = fetch(records, cursor.as_ref(), max + 1);
            let page = page(&fetched, cursor.as_ref(), max);
            let ids: Vec<String> = page.records.iter().map(|r| r.record_id.clone()).collect();
            let more = page.more;
            pages.push((ids, more, page.high_water.as_ref().map(|h| h.physical_ms)));
            cursor = page.high_water;
            if !more {
                return pages;
            }
        }
        panic!("paging did not terminate in 64 pages — a livelock, not a slow drain");
    }

    /// `page(None, max)` returns ALL records sorted by the full total order.
    #[test]
    fn test_page_from_zero_returns_all_sorted() {
        use kutl_proto::sync::Signal;
        // Intentionally unsorted input (descending ms).
        let recs = vec![
            sig(5, 0, "c", "5c"),
            sig(1, 0, "a", "1a"),
            sig(3, 0, "b", "3b"),
            // missing HLC — sorts first (zero clock)
            Signal {
                record_id: "rec-no-hlc".into(),
                actor_did: "did:key:zActorZ".into(),
                hlc: None,
                ..Default::default()
            },
        ];
        let p = page(&recs, None, 100);
        assert_eq!(p.records.len(), 4, "all records returned");
        assert!(!p.more);
        // Missing-HLC record sorts first (zero clock < any real stamp).
        assert_eq!(p.records[0].record_id, "rec-no-hlc");
        assert_eq!(p.records[1].record_id, "rec-1a");
        assert_eq!(p.records[2].record_id, "rec-3b");
        assert_eq!(p.records[3].record_id, "rec-5c");
        // high_water == the page's max HLC.
        assert_eq!(
            p.high_water.as_ref().map(|h| h.physical_ms),
            Some(5),
            "high_water is the max HLC of the page"
        );
    }

    /// `page` with an empty result preserves the cursor (`high_water == since`).
    #[test]
    fn test_page_empty_high_water_equals_since() {
        let recs: Vec<Signal> = vec![sig(1, 0, "a", "1")];
        let floor = hlc(5, 0);
        // All records are at ms=1, floor is ms=5 → nothing strictly after.
        let p = page(&recs, Some(&floor), 100);
        assert!(p.records.is_empty());
        assert!(!p.more);
        // high_water must be since, not regressed to None.
        assert_eq!(
            p.high_water.as_ref().map(|h| h.physical_ms),
            Some(5),
            "empty page preserves the cursor at since"
        );
    }

    /// `page` with `since = None` and no records returns `high_water = None`.
    #[test]
    fn test_page_empty_from_zero_high_water_is_none() {
        let recs: Vec<Signal> = vec![];
        let p = page(&recs, None, 100);
        assert!(p.records.is_empty());
        assert!(!p.more);
        assert!(p.high_water.is_none());
    }

    /// Multi-page walk reassembles the full set exactly once, in order,
    /// with no overlap and no hole — the key monotonic-paging invariant.
    ///
    /// Feed records in a SHUFFLED (non-HLC) order to prove sorting happens
    /// before cutting.
    #[test]
    fn test_catch_up_page_advances_high_water_monotonically() {
        // Page size 3 → 4 pages (3+3+3+1). Const declared first so it
        // precedes all `let` statements (clippy::items_after_statements).
        const PAGE_SIZE: usize = 3;

        // 10 records with varied (ms, logical, actor) so the total-order
        // sort key is unambiguous.
        let mut recs: Vec<Signal> = vec![
            sig(3, 0, "a", "3a0"),
            sig(1, 0, "b", "1b0"),
            sig(7, 1, "a", "7a1"),
            sig(2, 0, "c", "2c0"),
            sig(5, 0, "b", "5b0"),
            sig(4, 0, "a", "4a0"),
            sig(7, 0, "a", "7a0"), // same ms as 7a1, lower logical → sorts before
            sig(6, 0, "b", "6b0"),
            sig(1, 0, "a", "1a0"), // same ms as 1b0; actor "a" < "b" → sorts before
            sig(9, 0, "c", "9c0"),
        ];

        // Shuffle deterministically (reverse order to guarantee ≠ HLC order).
        recs.reverse();

        // Expected full order (by (ms, logical, actor, record)):
        // 1a0, 1b0, 2c0, 3a0, 4a0, 5b0, 6b0, 7a0, 7a1, 9c0
        let expected_order: Vec<&str> = vec![
            "rec-1a0", "rec-1b0", "rec-2c0", "rec-3a0", "rec-4a0", "rec-5b0", "rec-6b0", "rec-7a0",
            "rec-7a1", "rec-9c0",
        ];
        let mut cursor: Option<kutl_proto::sync::Hlc> = None;
        let mut all_seen: Vec<String> = Vec::new();

        loop {
            let p = page(&recs, cursor.as_ref(), PAGE_SIZE);
            assert!(
                p.records.len() <= PAGE_SIZE,
                "page must not exceed max_records"
            );
            if p.records.is_empty() {
                break;
            }
            // Verify intra-page order is strictly ascending by the total order.
            for w in p.records.windows(2) {
                let ka = (
                    proto_hlc_to_core(w[0].hlc.as_ref()),
                    w[0].actor_did.as_str(),
                    w[0].record_id.as_str(),
                );
                let kb = (
                    proto_hlc_to_core(w[1].hlc.as_ref()),
                    w[1].actor_did.as_str(),
                    w[1].record_id.as_str(),
                );
                assert!(
                    ka < kb,
                    "records within a page must be strictly ordered by (hlc, actor, record_id)"
                );
            }
            for r in &p.records {
                all_seen.push(r.record_id.clone());
            }
            // high_water is the last record's HLC (page's max).
            let hw = p
                .high_water
                .clone()
                .expect("non-empty page must have high_water");
            // Advance cursor.
            cursor = Some(hw);
            if !p.more {
                break;
            }
        }

        // Exact reassembly: all 10 records, in order, exactly once.
        assert_eq!(
            all_seen,
            expected_order
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            "multi-page walk must reassemble the full set in order with no overlap or hole"
        );
    }

    /// `more` is `true` exactly when records exist beyond the cut.
    #[test]
    fn test_page_more_flag() {
        let recs: Vec<Signal> = (1u64..=5)
            .map(|ms| sig(ms, 0, "a", &ms.to_string()))
            .collect();
        // max = 3, 5 records total → more after first page.
        let p1 = page(&recs, None, 3);
        assert_eq!(p1.records.len(), 3);
        assert!(p1.more, "more=true when records remain after the cut");

        // Advance and fetch the rest.
        let hw = p1.high_water.clone().unwrap();
        let p2 = page(&recs, Some(&hw), 3);
        assert_eq!(p2.records.len(), 2);
        assert!(!p2.more, "more=false on the last page");
    }

    /// A single `physical_ms` group LARGER than `max` OVERFLOWS: the whole group
    /// is emitted in one page (a soft cap) so the coarse cursor can advance past
    /// it. Trimming would keep zero records and livelock.
    #[test]
    fn test_page_overflows_single_ms_group_larger_than_max() {
        /// Page size.
        const MAX: usize = 3;
        /// Records sharing one `physical_ms`, strictly greater than `MAX`.
        const CLUSTER: usize = 8;
        let mut recs: Vec<Signal> = (0..CLUSTER)
            .map(|i| sig(100, u32::try_from(i).expect("small"), "a", &format!("c{i}")))
            .collect();
        recs.push(sig(200, 0, "a", "after"));

        // First page: the whole ms=100 group overflows (all 8), more=true.
        let p1 = page(&recs, None, MAX);
        assert_eq!(
            p1.records.len(),
            CLUSTER,
            "the >max single-ms group is emitted whole (overflow), not truncated"
        );
        assert!(
            p1.records
                .iter()
                .all(|r| r.hlc.as_ref().unwrap().physical_ms == 100)
        );
        assert!(
            p1.more,
            "a record at ms=200 remains beyond the overflow group"
        );
        assert_eq!(p1.high_water.as_ref().unwrap().physical_ms, 100);

        // Resume at the coarse floor: advances PAST ms=100, delivers ms=200.
        let hw = p1.high_water.clone().unwrap();
        let p2 = page(&recs, Some(&hw), MAX);
        let ids2: Vec<&str> = p2.records.iter().map(|r| r.record_id.as_str()).collect();
        assert_eq!(
            ids2,
            vec!["rec-after"],
            "cursor advanced past the ms=100 cluster"
        );
        assert!(!p2.more);
    }

    /// A page cut trims back to the last COMPLETE `physical_ms` boundary rather
    /// than splitting a group across the `max` limit.
    #[test]
    fn test_page_trims_to_complete_ms_boundary() {
        // ms=1 (one record), ms=2 (three records) — max=2 would cut inside ms=2.
        let recs: Vec<Signal> = vec![
            sig(1, 0, "a", "1a"),
            sig(2, 0, "a", "2a"),
            sig(2, 1, "a", "2b"),
            sig(2, 2, "a", "2c"),
        ];
        // max=2 would naively emit [1a, 2a] and split the ms=2 group. The trim
        // keeps only the complete ms=1 group.
        let p1 = page(&recs, None, 2);
        let ids1: Vec<&str> = p1.records.iter().map(|r| r.record_id.as_str()).collect();
        assert_eq!(
            ids1,
            vec!["rec-1a"],
            "trimmed back to the complete ms=1 group"
        );
        assert!(p1.more);
        assert_eq!(p1.high_water.as_ref().unwrap().physical_ms, 1);

        // Next page overflows the whole ms=2 group (3 > max=2).
        let hw = p1.high_water.clone().unwrap();
        let p2 = page(&recs, Some(&hw), 2);
        let ids2: Vec<&str> = p2.records.iter().map(|r| r.record_id.as_str()).collect();
        assert_eq!(
            ids2,
            vec!["rec-2a", "rec-2b", "rec-2c"],
            "the whole ms=2 group"
        );
        assert!(!p2.more);
    }

    /// `max = 0` still makes progress: it emits the FIRST complete `physical_ms`
    /// group (overflow floor). A zero-record page with records pending would
    /// livelock.
    #[test]
    fn test_page_max_zero_still_makes_progress() {
        let recs: Vec<Signal> = vec![
            sig(1, 0, "a", "1a"),
            sig(1, 1, "a", "1b"),
            sig(2, 0, "a", "2a"),
        ];
        let p = page(&recs, None, 0);
        let ids: Vec<&str> = p.records.iter().map(|r| r.record_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["rec-1a", "rec-1b"],
            "max=0 emits the first ms group whole"
        );
        assert!(p.more);
        assert_eq!(p.high_water.as_ref().unwrap().physical_ms, 1);
    }

    // ── adversarial paging-to-exhaustion ──────────────────────────────────────

    /// Rebuild the COARSE `physical_ms` resume floor a lossy cursor carries:
    /// `Hlc { physical_ms, logical: 0, actor: <nil> }` — the shape a cursor
    /// takes when only its millisecond survives persistence or a wire
    /// round-trip. The pager must make progress under this lossy cursor, not
    /// only under a full-precision one.
    fn coarse_floor(physical_ms: u64) -> kutl_proto::sync::Hlc {
        kutl_proto::sync::Hlc {
            physical_ms,
            logical: 0,
            actor: vec![0u8; 16],
        }
    }

    /// Page a record set to exhaustion the way the HTTP client does: resume with
    /// the coarse `physical_ms` floor rebuilt from the prior page's
    /// `high_water.physical_ms`. Returns the de-duplicated union of every
    /// emitted `record_id` (the coarse cursor may legitimately re-overlap, which
    /// idempotent union heals) and the number of iterations consumed.
    ///
    /// Caps the loop at `iteration_cap`: a livelock (a page that repeats forever
    /// because the cursor cannot advance past a same-`physical_ms` cluster) must
    /// FAIL the test, not hang it — so the caller asserts termination BELOW the
    /// cap.
    fn drain_like_http_client(
        records: &[Signal],
        max: usize,
        iteration_cap: usize,
    ) -> (std::collections::BTreeSet<String>, usize) {
        let mut union: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut floor: Option<kutl_proto::sync::Hlc> = None;
        let mut iterations = 0usize;
        loop {
            iterations += 1;
            let p = page(records, floor.as_ref(), max);
            for r in &p.records {
                union.insert(r.record_id.clone());
            }
            if !p.more {
                break;
            }
            // Resume EXACTLY as the HTTP client does: coarse physical_ms floor
            // rebuilt from the page's high_water. A full-precision cursor is
            // never carried on the GET wire.
            let hw_ms = p.high_water.as_ref().map_or(0, |h| h.physical_ms);
            floor = Some(coarse_floor(hw_ms));
            if iterations >= iteration_cap {
                // Do not loop forever — return with `more` still true so the
                // caller's below-cap assertion fails loudly as a livelock.
                break;
            }
        }
        (union, iterations)
    }

    /// A tiny deterministic PRNG (`SplitMix64`) — the repo bans nondeterministic
    /// test randomness, and `kutl-signals` carries no `rand` dependency. Seeded
    /// from a fixed constant so a failure reproduces byte-for-byte.
    struct SplitMix64(u64);
    impl SplitMix64 {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        /// Uniform in `[0, n)` (n > 0).
        fn below(&mut self, n: u64) -> u64 {
            self.next_u64() % n
        }
    }

    /// Build an adversarial record set that deliberately stresses the exact
    /// fields the pager tiebreaks on:
    ///  (a) `physical_ms` CLUSTERS larger than the page size — one wall-ms with
    ///      `cluster` records, only `logical` advancing (the merge-materializer
    ///      / legacy-backfill shape that livelocks a coarse cursor), and
    ///  (b) full-HLC TIES — ≥2 records with identical `(physical_ms, logical,
    ///      actor)` but distinct `record_id` (the equal-HLC hole a page cut
    ///      inside an equal-HLC run would form).
    /// Every `record_id` is unique so the union size equals the record count.
    fn adversarial_records(seed: u64, cluster: usize) -> Vec<Signal> {
        let mut rng = SplitMix64(seed);
        let mut recs: Vec<Signal> = Vec::new();
        let mut next_id = 0u64;
        let mut mint = |ms: u64, logical: u32, actor_byte: u8| {
            let id = next_id;
            next_id += 1;
            let mut actor = vec![0u8; 16];
            actor[0] = actor_byte;
            Signal {
                record_id: format!("rec-{id:06}"),
                actor_did: format!("did:key:zA{actor_byte}"),
                hlc: Some(kutl_proto::sync::Hlc {
                    physical_ms: ms,
                    logical,
                    actor,
                }),
                ..Default::default()
            }
        };

        // (a) A same-physical_ms cluster LARGER than the page size: T=1000,
        //     `cluster` records, only the logical counter advancing.
        for logical in 0..cluster {
            recs.push(mint(1000, logical.try_into().expect("cluster fits u32"), 1));
        }
        // (b) Full-HLC ties at a DIFFERENT ms: three records sharing exactly
        //     (2000, 5, actor=7) but distinct record_ids — an equal-HLC run.
        for _ in 0..3 {
            recs.push(mint(2000, 5, 7));
        }
        // A scattering of ordinary records at random earlier/later ms so the
        // clusters are not the only groups and the sort has real work.
        for _ in 0..20 {
            let ms = rng.below(3000);
            let logical = u32::try_from(rng.below(4)).expect("small logical");
            let actor_byte = u8::try_from(rng.below(3) + 10).expect("small actor byte");
            recs.push(mint(ms, logical, actor_byte));
        }

        // Shuffle deterministically (Fisher–Yates) so input order ≠ HLC order.
        for i in (1..recs.len()).rev() {
            let bound = u64::try_from(i).expect("index fits u64") + 1;
            let j = usize::try_from(rng.below(bound)).expect("index fits usize");
            recs.swap(i, j);
        }
        recs
    }

    /// The equal-HLC hole under a FULL-HLC resume. Two records share exactly
    /// `(physical_ms, logical, actor)` but distinct `record_id`; a page cut
    /// lands between them. `high_water` is HLC-only, so resuming
    /// strictly-after that HLC permanently skips the un-emitted equal-HLC
    /// sibling. Paging to exhaustion — resuming with the page's own
    /// `high_water` — must still reassemble BOTH records.
    #[test]
    fn test_page_to_exhaustion_no_equal_hlc_hole_full_precision_resume() {
        /// One record per page so the cut lands inside the equal-HLC pair.
        const MAX: usize = 1;
        // Two records at the SAME full HLC (5000, 5, actor=9), distinct ids.
        let mut actor = vec![0u8; 16];
        actor[0] = 9;
        let tie_a = Signal {
            record_id: "rec-tie-a".into(),
            actor_did: "did:key:zTie".into(),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: 5000,
                logical: 5,
                actor: actor.clone(),
            }),
            ..Default::default()
        };
        let tie_b = Signal {
            record_id: "rec-tie-b".into(),
            actor_did: "did:key:zTie".into(),
            hlc: Some(kutl_proto::sync::Hlc {
                physical_ms: 5000,
                logical: 5,
                actor,
            }),
            ..Default::default()
        };
        let later = sig(5001, 0, "z", "later");
        let recs = vec![tie_a, tie_b, later];
        let expected: std::collections::BTreeSet<String> =
            recs.iter().map(|r| r.record_id.clone()).collect();

        // Drain resuming with the page's OWN high_water (full-HLC precision) —
        // the mode that exposes the equal-HLC hole.
        let mut union: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut floor: Option<kutl_proto::sync::Hlc> = None;
        for _ in 0..(recs.len() + 10) {
            let p = page(&recs, floor.as_ref(), MAX);
            for r in &p.records {
                union.insert(r.record_id.clone());
            }
            if !p.more {
                break;
            }
            floor = p.high_water;
        }
        assert_eq!(
            union, expected,
            "an equal-HLC run must not drop a sibling across a page boundary \
             (the 'can never form a hole' claim is false for a full-HLC cursor)"
        );
    }

    /// Adversarial generator. Paging to exhaustion the way the
    /// HTTP client does (coarse `physical_ms` resume) must yield EXACTLY the
    /// full record set — no duplicate loss, no hole — AND TERMINATE. A livelock
    /// (cursor stuck on a >max same-`physical_ms` cluster) fails via the
    /// iteration-cap assertion; a hole fails via the missing `record_id`.
    #[test]
    fn test_page_to_exhaustion_no_livelock_no_hole() {
        /// Page size deliberately SMALLER than the same-ms cluster so a coarse
        /// cursor that cannot advance past the cluster livelocks.
        const MAX: usize = 5;
        /// Same-`physical_ms` cluster size, strictly greater than `MAX`.
        const CLUSTER: usize = 3 * MAX;

        // A fixed set of seeds — deterministic, reproduces byte-for-byte.
        for seed in [1u64, 2, 3, 7, 42, 1234, 99_999] {
            let recs = adversarial_records(seed, CLUSTER);
            let total = recs.len();
            let expected: std::collections::BTreeSet<String> =
                recs.iter().map(|r| r.record_id.clone()).collect();

            // Cap iterations well above the theoretical page count. If the pager
            // livelocks, it hits the cap with `more` still true and returns here
            // — the assertion below then fails as a livelock.
            let iteration_cap = total + 10;
            let (union, iterations) = drain_like_http_client(&recs, MAX, iteration_cap);

            assert!(
                iterations < iteration_cap,
                "seed {seed}: paging did not terminate within {iteration_cap} iterations \
                 (livelock on a same-physical_ms cluster larger than max={MAX})"
            );
            assert_eq!(
                union, expected,
                "seed {seed}: paging to exhaustion must reassemble EXACTLY the full set \
                 (no hole from an equal-HLC page-boundary split)"
            );
            assert_eq!(
                union.len(),
                total,
                "seed {seed}: every record_id is reachable exactly once in the union"
            );
        }
    }
}
