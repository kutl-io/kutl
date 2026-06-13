//! Liveness harness for the pure `DaemonCore` driven through `kutl-sim`'s
//! `DaemonSim`.
//!
//! This is the red→green proof that the daemon's event loop is deadlock-free
//! under bulk load. The in-process integration relay can't reproduce the
//! original deadlock (it evicts a full subscriber rather than blocking), so this
//! sim — which models the four bounded channels as capacity-`VecDeque`s — IS the
//! reproduction:
//!
//! - the SHIPPED topology ([`DaemonSim::new`]: unbounded outbound + intake gate)
//!   reaches quiescence;
//! - the OLD blocking topology ([`DaemonSim::new_blocking`]: a bounded `sync_cmd`
//!   whose full state stalls the inbound drain) FIXPOINTS — the reproduced
//!   bulk-load deadlock.
//!
//! The proptest property is the runtime analogue of the lattice's confluence
//! proptest (`arbitration_is_order_independent`, `registry.rs`): for any event
//! interleaving and any schedule, the core reaches quiescence.
//!
//! The bulk seed drives **local file creations** (`Event::FileModified`), not
//! remote registers — the `cp -R` repro from the findings doc. A local create is
//! the path that emits an outbound `RegisterDocument` to `sync_cmd` and stays
//! `confirmed:false` until the relay's `LifecycleAck` comes back; that
//! outbound→ack round-trip through the bounded channels is what the original
//! deadlock wedged. A remote register, by contrast, is confirmed-on-arrival and
//! emits no outbound command, so it cannot reproduce the cycle.

use std::path::PathBuf;

use kutl_daemon::core::{Event, EventStamp};
use kutl_sim::daemon_sim::DaemonSim;
use proptest::prelude::*;

/// Two remote registers at distinct, free paths: gamma must emit a
/// `GuardedPlace(Register)` for each (the `materialize_at` untracked fork,
/// `daemon.rs:2548`), the sim tracks + subscribes both, and the system reaches
/// quiescence. A register places NO bytes — the subscribe streams content — so the
/// disk stays empty.
#[test]
fn test_two_register_no_collision_places_both() {
    /// A budget far above the handful of steps two registers + acks need.
    const MAX_STEPS: usize = 10_000;
    const ID_A: &str = "11111111-1111-1111-1111-111111111111";
    const ID_B: &str = "22222222-2222-2222-2222-222222222222";

    let mut sim = DaemonSim::new(1);
    sim.feed(Event::RemoteRegister {
        document_id: ID_A.into(),
        path: "a.md".into(),
        stamp: EventStamp {
            wall_ms: 10,
            origin_hlc: Some(sim.hlc_for("a", 10)),
        },
    });
    sim.feed(Event::RemoteRegister {
        document_id: ID_B.into(),
        path: "b.md".into(),
        stamp: EventStamp {
            wall_ms: 11,
            origin_hlc: Some(sim.hlc_for("b", 11)),
        },
    });
    sim.drain_to_quiescence_or_fail(MAX_STEPS);
    assert!(
        sim.is_quiescent(),
        "two distinct-path registers did not converge"
    );
    assert_eq!(
        sim.disk_path_count(),
        0,
        "register-only places no bytes; subscribe streams content"
    );
    assert!(sim.is_tracked(ID_A), "doc a was not placed by the cascade");
    assert!(sim.is_tracked(ID_B), "doc b was not placed by the cascade");
}

/// A bulk seed through cap-64 channels. The findings-doc deadlock repro,
/// reinstated as sim state. On the reserve-before-commit (shipped) driver the sim
/// reaches quiescence; a blocking-send model fixpoints (the reproduced deadlock),
/// proving the sim catches it.
#[test]
fn test_bulk_seed_many_documents_does_not_deadlock() {
    /// Enough docs to SATURATE the relay's inbound queue (cap 256), which is what
    /// closes the blocking model's circular wait: gamma emits O(1) outbound
    /// commands per doc (one Register + one Subscribe), so the seed must exceed
    /// ~128 docs to cross that threshold. (The original deadlock is a saturation
    /// circular wait independent of volume; this just feeds enough to reach it.)
    const DOCS: usize = 200;
    /// A budget far above what a healthy drain needs (each doc is a handful of
    /// steps), so exhausting it means a genuine fixpoint, not a slow drain.
    const MAX_STEPS: usize = 200_000;

    // ── shipped shape: must reach quiescence ──
    let mut sim = DaemonSim::new(0x0BAD_C0DE);
    for i in 0..DOCS {
        sim.feed(local_create(i));
    }
    sim.drain_to_quiescence_or_fail(MAX_STEPS);
    assert!(
        sim.is_quiescent(),
        "bulk seed did not reach quiescence (deadlock)"
    );
    assert_eq!(
        sim.unconfirmed_count(),
        0,
        "a doc was stranded confirmed:false after the shipped drain"
    );

    // ── blocking shape: must FIXPOINT (the reproduced deadlock) ──
    let mut blocking = DaemonSim::new_blocking(0x0BAD_C0DE);
    for i in 0..DOCS {
        blocking.feed(local_create(i));
    }
    let reached = blocking.try_drain_to_quiescence(MAX_STEPS);
    assert!(
        !reached,
        "blocking-send model must fixpoint (the reproduced deadlock); it instead \
         reached quiescence, so the sim does NOT prove the property"
    );
}

/// More than 64 concurrent register/rename/delete + a remote-op flood. Backlog
/// must drain to 0; no doc stranded `confirmed:false`.
#[test]
fn test_bulk_op_mix_under_load_drains_to_zero() {
    /// More than the cap-64 channel so the mix genuinely crosses the threshold.
    const N: usize = 80;
    /// Generous step budget for the heavier op mix (renames/deletes/merges each
    /// emit several effects).
    const MAX_STEPS: usize = 60_000;

    let mut sim = DaemonSim::new(0x0000_001A);

    // Seed N registrations.
    for i in 0..N {
        sim.feed(local_create(i));
    }
    // Interleave renames, deletes, and a remote-op flood on the same ids.
    for i in 0..N {
        match i % 3 {
            0 => sim.feed(Event::RemoteRename {
                document_id: doc_id(i),
                old_path: format!("f{i}.md"),
                new_path: format!("r{i}.md"),
                rename_causal_floor: None,
                stamp: stamp((N + i) as u64),
            }),
            1 => sim.feed(Event::RemoteUnregister {
                document_id: doc_id(i),
                stamp: stamp((2 * N + i) as u64),
            }),
            _ => sim.feed(Event::RemoteOps {
                document_id: doc_id(i),
                ops: vec![],
                metadata: vec![],
                content_mode: 0,
                local_content: None,
                stamp: stamp((3 * N + i) as u64),
            }),
        }
    }

    sim.drain_to_quiescence_or_fail(MAX_STEPS);
    assert!(
        sim.is_quiescent(),
        "op-mix under load did not drain (deadlock)"
    );
    assert_eq!(
        sim.unconfirmed_count(),
        0,
        "a doc was stranded confirmed:false"
    );
}

/// TDD case 6 (graft 2): a brand-new doc onto an untracked-occupied path
/// DEFERS; a previously-seen doc onto its own orphan REVIVES (places). Both in
/// one suite, so the exemption can't drift to always-on or always-off.
/// Ports the `defer_if_occupied(exempt_revival)` split (`daemon.rs:505`).
#[test]
fn test_revival_exemption_does_not_over_fire() {
    let mut sim = DaemonSim::new(2);
    // An untracked local file already holds "shared.md".
    sim.put_untracked_file("shared.md", b"local bytes");

    // Brand-new remote doc (never seen) registers onto the occupied path: defer.
    let newcomer = "33333333-3333-3333-3333-333333333333";
    sim.feed(Event::RemoteRegister {
        document_id: newcomer.into(),
        path: "shared.md".into(),
        stamp: EventStamp {
            wall_ms: 20,
            origin_hlc: Some(sim.hlc_for("c", 20)),
        },
    });
    sim.run_one_handle();
    assert!(
        sim.is_deferred(newcomer),
        "brand-new doc onto untracked-held path must defer"
    );
    assert!(
        !sim.is_tracked(newcomer),
        "deferred doc is not placed (no clobber of local bytes)"
    );

    // A previously-seen doc (we applied a prior lifecycle op) revives onto its
    // OWN orphaned path — exempt from deferral.
    let known = "44444444-4444-4444-4444-444444444444";
    sim.seed_prior_lifecycle(known, sim.hlc_for("d", 5)); // makes has_applied_lifecycle true
    sim.put_untracked_file("revive.md", b"its own orphan");
    sim.feed(Event::RemoteRegister {
        document_id: known.into(),
        path: "revive.md".into(),
        stamp: EventStamp {
            wall_ms: 25,
            origin_hlc: Some(sim.hlc_for("d", 25)),
        },
    });
    sim.run_one_handle();
    assert!(!sim.is_deferred(known), "a revival is exempt from deferral");
    assert!(sim.is_tracked(known), "revival places onto its own orphan");
}

/// TDD case 5 (`32b0db0b` class): a previously-seen doc revives onto its own
/// orphaned path; it places (exempt), is not deferred. The single-doc analogue
/// of case 6's revival half, driven to full quiescence.
#[test]
fn test_revival_onto_own_orphan_places() {
    /// Budget far above the handful of steps one revival + its drain needs.
    const MAX_STEPS: usize = 10_000;
    let mut sim = DaemonSim::new(4);
    let doc = "66666666-6666-6666-6666-666666666666";
    sim.seed_prior_lifecycle(doc, sim.hlc_for("f", 1));
    sim.put_untracked_file("orphan.md", b"its own content");
    sim.feed(Event::RemoteRegister {
        document_id: doc.into(),
        path: "orphan.md".into(),
        stamp: EventStamp {
            wall_ms: 40,
            origin_hlc: Some(sim.hlc_for("f", 40)),
        },
    });
    sim.drain_to_quiescence_or_fail(MAX_STEPS);
    assert!(!sim.is_deferred(doc), "a revival is never deferred");
    assert!(
        sim.is_tracked(doc),
        "the revival places onto its own orphan"
    );
}

/// TDD case 9: a deferred doc gets `RemoteUnregister`, THEN the occupant vacates.
/// Because unregister removes it from `known_records` (tombstone), the next
/// reconcile simply doesn't consider it — no resurrection (the `37093896` class
/// is structural, not a guard the cascade has to remember to apply).
#[test]
fn test_unregistered_deferred_doc_is_not_resurrected() {
    /// Budget far above the steps the defer→unregister→vacate sequence needs.
    const MAX_STEPS: usize = 10_000;
    let mut sim = DaemonSim::new(3);
    sim.put_untracked_file("c.md", b"occupant");
    let doc = "55555555-5555-5555-5555-555555555555";
    sim.feed(Event::RemoteRegister {
        document_id: doc.into(),
        path: "c.md".into(),
        stamp: EventStamp {
            wall_ms: 30,
            origin_hlc: Some(sim.hlc_for("e", 30)),
        },
    });
    sim.run_one_handle();
    assert!(
        sim.is_deferred(doc),
        "the brand-new doc deferred onto the occupant"
    );
    // Precondition for the contrast below: while alive, the doc IS a placement
    // candidate (an alive record the cascade derives a mover for).
    assert!(
        sim.is_alive_record(doc),
        "a deferred-but-alive doc is still a cascade placement candidate"
    );

    // The relay unregisters the deferred doc.
    sim.feed(Event::RemoteUnregister {
        document_id: doc.into(),
        stamp: EventStamp {
            wall_ms: 31,
            origin_hlc: Some(sim.hlc_for("e", 31)),
        },
    });
    sim.run_one_handle();
    assert!(
        !sim.is_deferred(doc),
        "unregister clears the derived deferral (the doc left the desired set)"
    );
    // THE STRUCTURAL FIX (37093896): unregister tombstones the doc in
    // `known_records`, so `desired_assignment` excludes it — it can never again be
    // a mover, no matter when or how often the cascade re-runs. This is what makes
    // resurrection IMPOSSIBLE rather than merely guarded-against.
    assert!(
        !sim.is_alive_record(doc),
        "the unregistered doc is a tombstone — not an alive placement candidate"
    );

    // The occupant vacates (a local rm of the untracked file). Even though the
    // path is now free, the unregistered doc must NOT resurrect — it is no longer
    // an alive record, so the cascade never makes it a mover.
    sim.feed(Event::UntrackedFileRemoved {
        rel: "c.md".into(),
        stamp: stamp(32),
    });
    sim.drain_to_quiescence_or_fail(MAX_STEPS);
    assert!(
        !sim.is_tracked(doc),
        "an unregistered doc must NOT resurrect when its path frees"
    );
    assert!(
        !sim.is_alive_record(doc),
        "the tombstone survives the path freeing — still no placement candidate"
    );
}

/// TDD case 3 (graft 1, FS-1): a local untracked create (event still in
/// debounce) races a remote register for the SAME path. `GuardedPlace`'s atomic
/// stat must catch the occupant → the remote doc defers (no silent overwrite of
/// the local bytes). Closes the daemon.rs:303-305 conflation.
#[test]
fn test_fs1_untracked_create_races_register_no_clobber() {
    let mut sim = DaemonSim::new(5);
    let doc = "77777777-7777-7777-7777-777777777777";
    // Remote register arrives FIRST in the queue; the core decides to place.
    sim.feed(Event::RemoteRegister {
        document_id: doc.into(),
        path: "race.md".into(),
        stamp: EventStamp {
            wall_ms: 50,
            origin_hlc: Some(sim.hlc_for("g", 50)),
        },
    });
    // But a local untracked file lands on disk in the GuardedPlace window
    // (its watcher event has not yet been delivered to the core).
    sim.put_untracked_file_after_decide("race.md", b"local content");
    sim.drain_to_quiescence_or_fail(10_000);
    // The atomic stat saw the occupant: the doc deferred, local bytes intact.
    assert!(
        sim.is_deferred(doc),
        "GuardedPlace must defer on an unexpected untracked occupant"
    );
    assert_eq!(
        sim.disk_content("race.md").as_deref(),
        Some(&b"local content"[..]),
        "local bytes must not be clobbered"
    );
}

/// TDD case 10: inject a local create between the core's decision and the
/// shell's mutation; `GuardedPlace`'s atomic stat catches it → defer (conflict-
/// copy on the relay's next move), NOT clobber. Same mechanism as case 3 but
/// asserts the atomicity directly: the place must NOT happen in the same step
/// the occupant appears.
#[test]
fn test_guarded_place_atomicity_catches_mid_decision_create() {
    let mut sim = DaemonSim::new(6);
    let doc = "88888888-8888-8888-8888-888888888888";
    sim.feed(Event::RemoteRegister {
        document_id: doc.into(),
        path: "atomic.md".into(),
        stamp: EventStamp {
            wall_ms: 60,
            origin_hlc: Some(sim.hlc_for("h", 60)),
        },
    });
    sim.arm_disk_write_in_guarded_window("atomic.md", b"slipped in");
    sim.drain_to_quiescence_or_fail(10_000);
    assert!(
        sim.is_deferred(doc),
        "atomic stat must catch the mid-decision create"
    );
    assert_eq!(
        sim.disk_content("atomic.md").as_deref(),
        Some(&b"slipped in"[..])
    );
}

/// TDD case 4 (graft 1 at startup): drive a startup register BEFORE any file
/// scan, with an offline-created untracked file already on disk. The maps are
/// empty (daemon.rs:1029 before 1040 before 1043), so only the live stat can
/// see the file: the remote doc defers, the scan mints a fresh uuid, both
/// survive.
#[test]
fn test_fs2_startup_offline_create_defers_remote() {
    let mut sim = DaemonSim::new(7);
    let remote = "99999999-9999-9999-9999-999999999999";
    // Startup: the maps are EMPTY (daemon.rs:1029 before 1040 before 1043), so the
    // offline file is unknown to the core. SpaceDocuments lists the remote doc at
    // the SAME path; it tries to place, and only the GuardedPlace LIVE STAT sees the
    // offline file in the place window — the model of an offline-created file the
    // scan has not yet adopted. (No no-probe fast path: the stat runs at startup
    // too, contract §3.)
    sim.put_untracked_file_after_decide("offline.md", b"made while offline");
    sim.feed(Event::RemoteRegister {
        document_id: remote.into(),
        path: "offline.md".into(),
        stamp: EventStamp {
            wall_ms: 1,
            origin_hlc: Some(sim.hlc_for("i", 1)),
        },
    });
    sim.drain_to_quiescence_or_fail(10_000);
    assert!(
        sim.is_deferred(remote),
        "startup register onto an offline-held path must defer (the live stat caught it)"
    );
    // The scan now mints a FRESH, distinct uuid for the offline file and adopts it
    // — the real `initial_file_scan` never reuses a deferred register's claim
    // (daemon.rs:2862-2911: a deferred register never reaches `place_register`/
    // `register_identity`). Both documents now exist; the collision conflict-copies.
    let local = sim.scan_mints_uuid_for("offline.md");
    sim.drain_to_quiescence_or_fail(10_000);
    assert_ne!(
        local, remote,
        "the scan must mint a DISTINCT id — the two authors are not conflated (no merge)"
    );
    assert!(
        sim.is_tracked(&local),
        "the scanned offline file is adopted as its own tracked document"
    );
    assert!(
        sim.is_alive_record(remote),
        "the deferred remote doc still survives as an alive record (conflict-copied, not merged)"
    );
}

/// TDD case 8 (graft 3): the shell FAILS a rename (ENOSPC). The shadow must be
/// left UNCHANGED so the next reconcile re-derives and retries; the target is
/// not permanently deferred, and the doc is not lost. Mirrors the funnel
/// log-and-swallow self-heal (`daemon.rs:2516`).
#[test]
fn test_failed_rename_self_heals() {
    let mut sim = DaemonSim::new(8);
    let doc = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    // Doc tracked at old.md; relay renames it to new.md.
    sim.register_and_place(doc, "old.md", b"content");
    sim.fail_next_fs_effect(); // the shell will fail the upcoming rename
    sim.feed(Event::RemoteRename {
        document_id: doc.into(),
        old_path: "old.md".into(),
        new_path: "new.md".into(),
        rename_causal_floor: None,
        stamp: EventStamp {
            wall_ms: 70,
            origin_hlc: Some(sim.hlc_for("j", 70)),
        },
    });
    sim.run_one_handle();
    // Shadow unchanged: still at old.md (the failure folded nothing), and
    // NOTHING retries while the space is idle — the sim now models the real
    // driver faithfully (a failed fs effect schedules no retry; the old
    // auto-retry park was the `ba966512` faking class).
    assert_eq!(
        sim.shadow_path(doc).as_deref(),
        Some("old.md"),
        "a failed rename must not advance the shadow"
    );
    // The periodic tick is the guaranteed retry driver: its reconcile
    // re-derives the mover (no longer failing) and converges.
    sim.feed(Event::MetricsTick {
        stamp: EventStamp {
            wall_ms: 71,
            origin_hlc: None,
        },
    });
    sim.drain_to_quiescence_or_fail(10_000);
    assert_eq!(
        sim.shadow_path(doc).as_deref(),
        Some("new.md"),
        "the tick-driven retry converges"
    );
    assert!(sim.is_quiescent());
}

/// ENOSPC arm (A4e, 2026-06-11): the shell FAILS the conflict-copy
/// DISPLACEMENT write (the loser's `GuardedPlace(Rename)` to its conflict
/// path). Graft 3 leaves the shadow unchanged so a later reconcile re-derives
/// and retries — and the PERIODIC TICK is the guaranteed retry driver (added
/// with this test). Without it an IDLE space stays diverged forever after a
/// swallowed write: the sim models the real driver faithfully now (a failed
/// fs effect schedules NOTHING — the old auto-retry park was the `ba966512`
/// faking class and hid exactly this gap).
#[test]
fn test_failed_displacement_write_heals_on_tick() {
    /// Far above any stamp the sim clock produced during setup, so the
    /// winner's register strictly dominates (arbitrate's `max_by (path_hlc,
    /// id)` takes the path).
    const DOMINANT_MS: u64 = 1_000_000;
    let mut sim = DaemonSim::new(12);
    let loser = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
    let winner = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
    sim.register_and_place(loser, "x.md", b"loser bytes");

    // The winner registers at the same path with a dominant stamp; the
    // cascade derives the loser's displacement to its conflict path. Fail
    // that move.
    sim.fail_next_fs_effect();
    sim.feed(Event::RemoteRegister {
        document_id: winner.into(),
        path: "x.md".into(),
        stamp: EventStamp {
            wall_ms: DOMINANT_MS,
            origin_hlc: Some(sim.hlc_for("w", DOMINANT_MS)),
        },
    });
    sim.run_one_handle();
    sim.drain_to_quiescence_or_fail(10_000);

    // DIVERGED at rest: the swallowed displacement left the loser at x.md and
    // nothing retries it (no events arrive; the real driver schedules no
    // retry). This is the pre-fix stranded state, held by the sim honestly.
    let conflict = kutl_core::lattice::conflict_path(
        "x.md",
        &kutl_core::Uuid::parse_str(loser).expect("loser uuid"),
    );
    assert_eq!(
        sim.shadow_path(loser).as_deref(),
        Some("x.md"),
        "a failed displacement must not advance the shadow, and nothing\
         retries it while the space is idle"
    );
    assert!(
        !sim.disk_path_exists(&conflict),
        "no conflict file exists while the space is idle"
    );

    // The metrics tick drives the cascade back to its fixpoint: the loser's
    // mover is re-derived (shadow still at x.md, desired = conflict path) and
    // the retry lands.
    sim.feed(Event::MetricsTick {
        stamp: EventStamp {
            wall_ms: DOMINANT_MS + 1,
            origin_hlc: None,
        },
    });
    sim.drain_to_quiescence_or_fail(10_000);
    assert!(sim.is_quiescent());
    assert_eq!(
        sim.disk_content(&conflict).as_deref(),
        Some(b"loser bytes".as_ref()),
        "the loser's bytes survive at its conflict path"
    );
    assert_eq!(
        sim.shadow_path(loser).as_deref(),
        Some(conflict.as_str()),
        "the tick-driven retry lands the displacement"
    );
}

/// TDD case 11 (locks the `5dd87257` class): a gamma-computed rename emits the
/// SAME two suppressions in the SAME order the funnel does (removal-half
/// `(old, None)` FIRST, write-half `(new, Some(hash))` SECOND —
/// `daemon.rs:3837-3838` / `1796-1797`). gamma emits the rename as a
/// `GuardedPlace(Rename)`; the shell applying it records the pairing, so the
/// order is a property of the shared funnel, not a gamma-private reordering.
///
/// Then the two halves of the catastrophic invariant:
/// - own write IS suppressed: a daemon-echo create at the new path carrying the
///   renamed bytes is swallowed (hash matches the write-half suppression);
/// - peer recreate NOT swallowed: a genuine create at the old path with
///   DIFFERENT bytes survives (it is not a removal echo, and its hash differs
///   from any write echo), so the file is tracked — no §1.3/§4.2 strand.
#[test]
fn test_rename_effect_suppression_pairing_and_order() {
    let mut sim = DaemonSim::new(10);
    let doc = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    sim.register_and_place(doc, "from.md", b"renamed bytes");
    sim.feed(Event::RemoteRename {
        document_id: doc.into(),
        old_path: "from.md".into(),
        new_path: "to.md".into(),
        rename_causal_floor: None,
        stamp: EventStamp {
            wall_ms: 90,
            origin_hlc: Some(sim.hlc_for("l", 90)),
        },
    });
    sim.run_one_handle();
    // The shell applied the rename via the funnel order: removal of `from.md`
    // suppressed FIRST (hash None), then the write of `to.md` suppressed with
    // the renamed content hash.
    let supp = sim.suppressions_in_order();
    assert_eq!(
        supp.first().map(|(p, h)| (p.clone(), h.clone())),
        Some((PathBuf::from("from.md"), None)),
        "removal half first (old path, no hash)"
    );
    assert_eq!(
        supp.get(1).map(|(p, _)| p.clone()),
        Some(PathBuf::from("to.md")),
        "write half second (new path)"
    );
    assert!(
        supp.get(1).is_some_and(|(_, h)| h.is_some()),
        "write half carries the renamed content hash"
    );

    // own write IS suppressed: the daemon's own rename-write echo at `to.md`
    // carrying the renamed bytes matches the write-half hash → swallowed, NOT
    // re-ingested as a fresh edit (no op amplification / phantom conflict-copy).
    sim.feed_local_create("to.md", b"renamed bytes");
    sim.run_one_handle();
    assert!(
        sim.last_local_create_was_suppressed(),
        "the daemon's own write echo must be suppressed (hash matches the write half)"
    );

    // peer recreate NOT swallowed: a genuine recreate of `from.md` with
    // DIFFERENT bytes in the same window is not consumed by the removal-half
    // suppression (a `None` half is not a write echo), so the file is tracked.
    sim.feed_local_create("from.md", b"peer recreate");
    sim.run_one_handle();
    assert!(
        !sim.last_local_create_was_suppressed(),
        "a genuine recreate (different bytes) is NOT a write echo — not swallowed"
    );
    assert!(
        sim.is_tracked_at("from.md"),
        "the genuine recreate is materialized, not swallowed by the removal echo"
    );
}

/// TDD case 12: a doc deferred behind an untracked occupant eventually
/// materializes once the occupant is removed locally — confirm a reconcile is
/// actually RE-TRIGGERED by `UntrackedFileRemoved` (the deferral drains, it is
/// not a permanent strand / liveness bug).
#[test]
fn test_deferred_doc_materializes_on_untracked_removal() {
    let mut sim = DaemonSim::new(11);
    let doc = "dddddddd-dddd-dddd-dddd-dddddddddddd";
    sim.put_untracked_file("held.md", b"occupant");
    sim.feed(Event::RemoteRegister {
        document_id: doc.into(),
        path: "held.md".into(),
        stamp: EventStamp {
            wall_ms: 100,
            origin_hlc: Some(sim.hlc_for("m", 100)),
        },
    });
    sim.run_one_handle();
    assert!(
        sim.is_deferred(doc),
        "deferred behind the untracked occupant"
    );
    // Local rm of the untracked file → the path frees.
    sim.feed(Event::UntrackedFileRemoved {
        rel: "held.md".into(),
        stamp: stamp(101),
    });
    sim.drain_to_quiescence_or_fail(10_000);
    assert!(!sim.is_deferred(doc), "deferral drained");
    assert!(
        sim.is_tracked(doc),
        "the doc materialized once the path freed"
    );
}

/// A bounded, sim-domain daemon event for the liveness property. Mirrors the
/// lattice proptest's `arb_event` (`registry.rs`) over a small domain (3 ids × 3
/// paths × bounded HLCs) so collisions and revivals hit densely.
///
/// Every remote event carries an origin HLC, exactly as the wire events do
/// (`client.rs:394`/`hlc_of`): the lattice elects a doc's path from the latest of
/// its register/rename HLCs, so a real remote rename MUST carry one for
/// arbitration to move the path. (Feeding `origin_hlc: None` would exercise only
/// the pre-HLC fallback, where a rename moves the file on disk but the lattice
/// path does not update — a degenerate path the real daemon never takes.)
fn arb_daemon_event() -> impl Strategy<Value = Event> {
    let id_s = 0u8..3; // d0,d1,d2
    let path_s = 0u8..3; // f0,f1,f2
    let hlc_s = (1u64..6, 0u8..3).prop_map(|(physical_ms, a)| stamp_hlc(physical_ms, a));
    prop_oneof![
        (id_s.clone(), path_s.clone(), hlc_s.clone()).prop_map(|(i, p, st)| {
            Event::RemoteRegister {
                document_id: uuid_id(i),
                path: format!("f{p}.md"),
                stamp: st,
            }
        }),
        (id_s.clone(), path_s, hlc_s.clone()).prop_map(|(i, p, st)| Event::RemoteRename {
            document_id: uuid_id(i),
            old_path: format!("f{i}.md"),
            new_path: format!("f{p}.md"),
            rename_causal_floor: None,
            stamp: st,
        }),
        (id_s, hlc_s).prop_map(|(i, st)| Event::RemoteUnregister {
            document_id: uuid_id(i),
            stamp: st,
        }),
    ]
}

/// An `EventStamp` carrying an origin HLC at `(physical_ms, actor)` — the wire
/// shape of a remote lifecycle event.
fn stamp_hlc(physical_ms: u64, actor: u8) -> EventStamp {
    let mut b = [0u8; 16];
    b[15] = actor;
    EventStamp {
        wall_ms: physical_ms,
        origin_hlc: Some(kutl_core::Hlc {
            physical_ms,
            logical: 0,
            actor: kutl_core::hlc::ActorId(uuid::Uuid::from_bytes(b)),
        }),
    }
}

proptest! {
    /// LIVENESS: for any event interleaving and any bounded schedule, the sim
    /// reaches quiescence with all docs confirmed and converged. The runtime
    /// analogue of the lattice's confluence property — the deadlock becomes a
    /// falsifiable invariant.
    #[test]
    fn daemon_core_reaches_quiescence(
        events in prop::collection::vec(arb_daemon_event(), 1..40),
        schedule in prop::collection::vec(any::<prop::sample::Index>(), 0..400),
    ) {
        /// Budget far above the worst-case drain for ≤40 events.
        const MAX_STEPS: usize = 100_000;
        let mut sim = DaemonSim::new(0x11FE);
        for ev in &events {
            sim.feed(ev.clone());
        }
        for idx in &schedule {
            sim.step_scheduled(idx.index(usize::MAX));
        }
        sim.drain_to_quiescence_or_fail(MAX_STEPS);
        prop_assert!(sim.is_quiescent(), "core did not reach quiescence (deadlock)");
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// A deterministic UUID document id for index `i` (the lattice keys by `Uuid`, so
/// the lifecycle arms only fold records for parseable ids).
fn doc_id(i: usize) -> String {
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&(i as u64).to_le_bytes());
    uuid::Uuid::from_bytes(b).to_string()
}

/// A small-domain UUID document id for the proptest strategy (ids 0..3).
fn uuid_id(i: u8) -> String {
    doc_id(usize::from(i))
}

/// A local file creation for doc index `i` at `file-{i}.md` — the `cp -R` repro
/// path. The core mints a new id, emits `RegisterDocument` to the relay, and the
/// doc stays `confirmed:false` until the relay's `LifecycleAck` returns. This is
/// the outbound→ack round-trip the original deadlock wedged.
fn local_create(i: usize) -> Event {
    Event::FileModified {
        rel: PathBuf::from(format!("file-{i}.md")),
        content: Some(format!("# Document {i}\n\nbody {i}\n").into_bytes()),
        stamp: stamp(i as u64),
    }
}

/// TDD case 7 (FS-12, the §7.1 / `f_conflicts` conform): an out-of-band local `mv`
/// relocated the file; the remote rename is dequeued first. The daemon shadow is
/// BLIND to the user rename (`relocate_out_of_band` does not advance it). The §1.3
/// drain edge (`model_disk_edge_for`, the twin of `drain_relocated_local_rename`)
/// first surfaces the local rename — it reaches the relay with its honest
/// pre-observation stamp instead of being silently destroyed — and the remote
/// rename, stamped by a +5s-skewed peer so it strictly dominates, then conforms the
/// relocated file by inode (`apply_guarded_place`, the twin of `place_now`'s
/// missing-source conform). One file at the authoritative target, no duplicate, no
/// orphan, and BOTH renames observed by the relay. This is the regression
/// `41add2a5` fixed at the e2e altitude, now a fast deterministic sim guard.
/// (Modelling the relocation faithfully is what proved graft 4 / `ProbePath` dead —
/// `shadow_inode`/`shadow_path` always move together in the real daemon, so the
/// inode-discrepancy probe never fires; graft 4 was since removed.)
#[test]
fn test_fs12_conform_vs_concurrent_relocation() {
    let mut sim = DaemonSim::new(9);
    let doc = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    sim.register_and_place(doc, "old.md", b"payload");
    // Out-of-band local rename relocated the file to moved.md (watcher event
    // NOT yet delivered), carrying the same inode; shadow still says old.md.
    sim.relocate_out_of_band("old.md", "moved.md");
    // Remote rename to dest.md dequeues first, stamped by a +5s-skewed peer so
    // it strictly dominates the drained local rename (stamped at wall 80) in
    // every interleaving — the §7.1 shape, deterministic by construction.
    sim.feed(Event::RemoteRename {
        document_id: doc.into(),
        old_path: "old.md".into(),
        new_path: "dest.md".into(),
        rename_causal_floor: None,
        stamp: EventStamp {
            wall_ms: 80,
            origin_hlc: Some(sim.hlc_for("k", 5_080)),
        },
    });
    sim.drain_to_quiescence_or_fail(10_000);
    // The drained local rename reached the relay (no silent §1.3 drop), even
    // though it loses the arbitration.
    assert!(
        sim.renames_relayed()
            .iter()
            .any(|(id, path)| id == doc && path == "moved.md"),
        "the concurrent local rename must reach the relay, got {:?}",
        sim.renames_relayed()
    );
    // Exactly one file at the authoritative dest, conformed from the relocated
    // inode — no duplicate (the materialize-from-CRDT branch must NOT fire).
    assert!(
        sim.disk_path_exists("dest.md"),
        "conformed to the authoritative path"
    );
    assert!(
        !sim.disk_path_exists("moved.md"),
        "the relocated file was conformed, not duplicated"
    );
    assert_eq!(
        sim.disk_content("dest.md").as_deref(),
        Some(&b"payload"[..])
    );
}

/// §4.2 — a DELETE must vacate the shadow occupant, so a foreign recreate at
/// the tombstoned path makes the next register DEFER instead of place-adopting
/// the recreate's bytes. The deterministic guard for the concurrent-recreate
/// merge corruption: with the removal fold missing, the stale `Tracked`
/// occupant makes the place's stat report "not untracked" (the FS-1 guard is
/// blind to the recreate), the register places cleanly, claims the path, and
/// the local recreate is merged into the remote doc as a "pending edit" — one
/// doc with concatenated bodies instead of canonical + conflict-copy.
#[test]
fn test_sec42_recreate_after_delete_defers_remote_register() {
    let mut sim = DaemonSim::new(13);
    let old_doc = "dddddddd-dddd-dddd-dddd-dddddddddddd";
    sim.register_and_place(old_doc, "doc.md", b"base\n");

    // The LOCAL delete (the user's `rm`, observed by the watcher) — the e2e
    // failure's path: the deleter's own shadow must vacate the occupant. The
    // remote-unregister twin vacates inline in `handle_remote_unregister`;
    // this is the local arm (`handle_file_removed`), which has no
    // RemoveFile/ACK round-trip (the file is already gone).
    sim.feed(Event::FileRemoved {
        rel: PathBuf::from("doc.md"),
        stamp: stamp(50),
    });
    sim.drain_to_quiescence_or_fail(10_000);
    assert!(
        !sim.disk_path_exists("doc.md"),
        "the delete removed the file"
    );

    // A local recreate lands at the tombstoned path INSIDE the next place's
    // critical section (on disk before the stat, invisible to the shadow) —
    // the §4.2 race timing.
    sim.put_untracked_file_after_decide("doc.md", b"base\nREVIVE-LOCAL\n");

    // A peer's brand-new doc registers at the same path. It must DEFER behind
    // the untracked recreate (no revival exemption — never-seen id), NOT place.
    let new_doc = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
    sim.feed(Event::RemoteRegister {
        document_id: new_doc.into(),
        path: "doc.md".into(),
        stamp: EventStamp {
            wall_ms: 60,
            origin_hlc: Some(sim.hlc_for("e", 60)),
        },
    });
    sim.run_one_handle();

    assert!(
        sim.is_deferred(new_doc),
        "a brand-new register onto a foreign recreate must defer"
    );
    assert!(
        !sim.is_tracked(new_doc),
        "the deferred doc must not claim the recreate's path"
    );
    assert_eq!(
        sim.disk_content("doc.md").as_deref(),
        Some(&b"base\nREVIVE-LOCAL\n"[..]),
        "the local recreate's bytes are untouched"
    );
}

/// §1.3, loser direction: the incoming remote rename's origin stamp is OLDER than
/// the drained local rename's, so after the drain surfaces the local rename the
/// freshness gate drops the remote one — the file stays at the user's path, and
/// the local rename (the cluster-wide winner) reached the relay. Without the
/// drain edge this scenario silently lost one of the two concurrent renames: the
/// conform relocated the file first and the local rename never reached the relay,
/// leaving the winner decided by broadcast timing instead of the lattice.
#[test]
fn test_sec13_stale_remote_rename_loses_to_drained_local_rename() {
    let mut sim = DaemonSim::new(11);
    let doc = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    sim.register_and_place(doc, "old.md", b"payload");
    sim.relocate_out_of_band("old.md", "moved.md");
    // The remote rename's origin stamp (wall 10) predates the drained local
    // rename (stamped at wall 80): the gate must drop it after the drain.
    sim.feed(Event::RemoteRename {
        document_id: doc.into(),
        old_path: "old.md".into(),
        new_path: "dest.md".into(),
        rename_causal_floor: None,
        stamp: EventStamp {
            wall_ms: 80,
            origin_hlc: Some(sim.hlc_for("k", 10)),
        },
    });
    sim.drain_to_quiescence_or_fail(10_000);
    assert!(
        sim.renames_relayed()
            .iter()
            .any(|(id, path)| id == doc && path == "moved.md"),
        "the local rename must reach the relay, got {:?}",
        sim.renames_relayed()
    );
    assert!(
        sim.disk_path_exists("moved.md"),
        "the stale remote rename must not move the locally-renamed file"
    );
    assert!(
        !sim.disk_path_exists("dest.md"),
        "the dropped remote rename must not materialize its path"
    );
    assert_eq!(
        sim.disk_content("moved.md").as_deref(),
        Some(&b"payload"[..])
    );
}

/// An `EventStamp` at virtual wall `ms`, no origin HLC (the sim feeds events
/// without a peer HLC; the relay model assigns deterministic stamps).
fn stamp(ms: u64) -> EventStamp {
    EventStamp {
        wall_ms: ms,
        origin_hlc: None,
    }
}

/// The third-observer floored-rename drop (the combined-stressors lost
/// rename): this daemon PLACED a doc whose remote register carried a
/// clock-skewed future stamp (folded into the lifecycle watermark); a peer
/// then renames it with a stamp BELOW that watermark but floored at the
/// register it observed. The freshness gate must apply `path_priority`'s
/// supersession rule (`renamed >= applied OR floor >= applied`) — without the
/// floor arm the rename is silently dropped here while registrant and renamer
/// both apply it, splitting the cluster on the doc's path.
#[test]
fn test_floored_cross_rename_lands_on_third_observer() {
    let mut sim = DaemonSim::new(17);
    let doc = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
    // The skewed registrant's stamp: far in this observer's future.
    let register = sim.hlc_for("registrant", 5_000);
    sim.feed(Event::RemoteRegister {
        document_id: doc.into(),
        path: "skew.md".into(),
        stamp: EventStamp {
            wall_ms: 1,
            origin_hlc: Some(register),
        },
    });
    sim.drain_to_quiescence_or_fail(10_000);
    assert!(
        sim.is_tracked(doc),
        "precondition: the observer placed the registered doc"
    );

    // The peer's cross-worker rename: stamped below the watermark, floored at it.
    sim.feed(Event::RemoteRename {
        document_id: doc.into(),
        old_path: "skew.md".into(),
        new_path: "moved.md".into(),
        rename_causal_floor: Some(register),
        stamp: EventStamp {
            wall_ms: 2,
            origin_hlc: Some(sim.hlc_for("renamer", 2)),
        },
    });
    sim.drain_to_quiescence_or_fail(10_000);
    assert_eq!(
        sim.shadow_path(doc).as_deref(),
        Some("moved.md"),
        "the floored rename must re-place the doc at its new path on the observer"
    );
}

/// The `f_conflicts` create/create strand (the gamma default-flip blocker): a
/// peer's register races the local create's debounce window — the `GuardedPlace`
/// atomic stat catches the unflushed file and the peer doc defers behind the
/// `Occupant::Untracked` marker (FS-1, the correct half). When the local
/// create's watcher event THEN mints its own doc at the path, the stale marker
/// must not defer the minted doc on its own file: it must place AND subscribe.
/// A local create's `Subscribe` rides its place ACK (`place_now`, daemon.rs:2405)
/// — in the captured e2e failure the observer's winning doc was never
/// subscribed, so the peer's post-conflict edit was never delivered and the
/// workers diverged silently (same file keys, different `dup.md` content).
#[test]
fn test_create_create_register_race_subscribes_minted_doc() {
    let mut sim = DaemonSim::new(13);
    let peer = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    // The peer's register arrives BEFORE the local create's watcher flush; the
    // unflushed create lands inside the GuardedPlace window, so the atomic stat
    // catches it and the peer defers.
    sim.feed(Event::RemoteRegister {
        document_id: peer.into(),
        path: "dup.md".into(),
        stamp: EventStamp {
            wall_ms: 1,
            origin_hlc: Some(sim.hlc_for("peer", 1)),
        },
    });
    sim.put_untracked_file_after_decide("dup.md", b"from-local");
    sim.drain_to_quiescence_or_fail(10_000);
    assert!(
        sim.is_deferred(peer),
        "precondition: the peer doc defers behind the untracked occupant"
    );

    // The local create's watcher event now flushes and mints its own doc — with
    // a LATER stamp than the peer's register, so the minted doc wins dup.md
    // (the captured e2e ordering: the observer's register went out after the
    // peer's arrived, and won).
    sim.feed_local_create("dup.md", b"from-local");
    sim.drain_to_quiescence_or_fail(10_000);

    let minted = sim.doc_id_at("dup.md").expect("the mint claims dup.md");
    assert!(
        !sim.is_deferred(&minted),
        "the minted doc must not defer on its own file (stale Untracked marker)"
    );
    assert!(
        sim.is_tracked(&minted),
        "the minted doc must be placed (shadow holds it)"
    );
    assert!(
        sim.subscribed_docs().contains(&minted),
        "the minted doc must subscribe — without it the winner-doc edit stream never arrives"
    );
    // The losing peer re-derives to its conflict sibling; both docs survive.
    assert!(
        sim.is_alive_record(peer),
        "the deferred peer record survives the collision"
    );
    let peer_path = sim
        .shadow_path(peer)
        .expect("the peer doc places once the marker resolves");
    assert!(
        peer_path.contains(kutl_core::lattice::CONFLICT_INFIX),
        "the losing peer register lands at its conflict sibling, got {peer_path}"
    );
}
