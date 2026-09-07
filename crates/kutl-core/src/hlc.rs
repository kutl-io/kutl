//! Hybrid logical clock (HLC) — causal order in a single comparable scalar.
//!
//! Lifecycle ops are ordered by HLC, not wall-clock, so convergence is
//! reproducible (skew-resistant) and offline-correct (a stale offline op
//! carries its *original* time and cannot clobber recent ops by virtue of a
//! late rejoin).
//!
//! The clock is **pure**: wall time is injected by the caller, matching
//! kutl-core's no-ambient-clock convention (`env::now_ms`), so the algorithm is
//! deterministic and property-testable.

use uuid::Uuid;

/// Per-device actor identity, the final HLC tiebreaker.
///
/// MUST be distinct per physical installation — **not** per account/DID. One
/// user with two devices needs two `ActorId`s, otherwise concurrent edits from
/// the two devices can produce identical HLCs and the order is no longer total.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActorId(pub Uuid);

/// A hybrid logical clock timestamp.
///
/// Total order is lexicographic `(physical_ms, logical, actor)` — field
/// declaration order, on which `derive(Ord)` relies. The generating clock
/// guarantees: if event A causally precedes B (B's clock observed A), then
/// `A < B`. Genuinely concurrent events get an arbitrary-but-deterministic
/// order (physical time dominates; `logical` breaks same-millisecond ties;
/// `actor` breaks same-`(physical, logical)` ties across devices).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hlc {
    /// Physical time component (millis since epoch), keeping stamps near wall
    /// clock so they remain human-interpretable and roughly time-ordered.
    pub physical_ms: u64,
    /// Logical counter, incremented to disambiguate events within one
    /// millisecond (or while the physical clock is stalled / regressing).
    pub logical: u32,
    /// Originating device, the deterministic tiebreaker for concurrent stamps.
    pub actor: ActorId,
}

impl Hlc {
    /// The liveness touch for a content edit observed at wall time `physical_ms`.
    ///
    /// Content edits carry only a physical timestamp (no logical/device part),
    /// so this is how a content edit enters the lifecycle order as an
    /// edit-revives "touch" (see the lattice-lifecycle design). The rule it
    /// encodes — *a concurrent or later edit beats a delete* — falls out of
    /// the lexicographic order:
    ///
    /// - **Strictly-later delete wins:** a delete at a greater `physical_ms`
    ///   dominates this touch (physical time is compared first), so a stale
    ///   offline edit cannot revive past a causally-later cluster delete.
    /// - **Concurrent (same-`physical_ms`) edit wins:** `logical = u32::MAX`
    ///   makes this touch outrank any real lifecycle stamp at the same
    ///   millisecond (genuine logical counters are tiny), so an edit racing a
    ///   delete in the same millisecond revives the document.
    ///
    /// The actor is nil: a content edit has no HLC device identity, and the
    /// `logical` ceiling already settles same-millisecond ties before the actor
    /// is consulted.
    #[must_use]
    pub fn physical_touch(physical_ms: u64) -> Self {
        Self {
            physical_ms,
            logical: u32::MAX,
            actor: ActorId(Uuid::nil()),
        }
    }
}

/// Stateful HLC generator for one actor (device).
///
/// Holds the last-emitted stamp; `tick` produces a stamp for a local event and
/// `recv` advances the clock past an observed remote stamp. Both take the
/// current wall time as a parameter (no ambient clock).
#[derive(Clone, Debug)]
pub struct HlcClock {
    actor: ActorId,
    last: Hlc,
}

impl HlcClock {
    /// A fresh clock for `actor`, starting below any real stamp.
    #[must_use]
    pub fn new(actor: ActorId) -> Self {
        Self {
            actor,
            last: Hlc {
                physical_ms: 0,
                logical: 0,
                actor,
            },
        }
    }

    /// Restore a clock seeded at `floor`, so a restart never emits a stamp at or
    /// below one already observed before the restart (monotonic restart). The
    /// next `tick`/`recv` is strictly greater than `floor`.
    #[must_use]
    pub fn restore(actor: ActorId, floor: Hlc) -> Self {
        Self {
            actor,
            last: Hlc {
                physical_ms: floor.physical_ms,
                logical: floor.logical,
                actor,
            },
        }
    }

    /// The last stamp this clock emitted (its current high-water mark).
    #[must_use]
    pub fn last(&self) -> Hlc {
        self.last
    }

    /// Generate a stamp for a local event at wall time `wall_ms`.
    ///
    /// Strictly greater than every prior stamp from this clock, even if the
    /// wall clock stalled or regressed (the logical counter advances instead).
    pub fn tick(&mut self, wall_ms: u64) -> Hlc {
        let candidate = wall_ms.max(self.last.physical_ms);
        let (physical_ms, logical) = if candidate == self.last.physical_ms {
            bump_logical(candidate, self.last.logical)
        } else {
            (candidate, 0)
        };
        self.last = Hlc {
            physical_ms,
            logical,
            actor: self.actor,
        };
        self.last
    }

    /// Advance the clock on observing `remote`, returning the new local stamp.
    ///
    /// The result strictly dominates both the prior local stamp and `remote`,
    /// so an op stamped after receiving `remote` is causally ordered after it.
    pub fn recv(&mut self, remote: Hlc, wall_ms: u64) -> Hlc {
        let candidate = wall_ms.max(self.last.physical_ms).max(remote.physical_ms);
        let (physical_ms, logical) =
            if candidate == self.last.physical_ms && candidate == remote.physical_ms {
                bump_logical(candidate, self.last.logical.max(remote.logical))
            } else if candidate == self.last.physical_ms {
                bump_logical(candidate, self.last.logical)
            } else if candidate == remote.physical_ms {
                bump_logical(candidate, remote.logical)
            } else {
                (candidate, 0)
            };
        self.last = Hlc {
            physical_ms,
            logical,
            actor: self.actor,
        };
        self.last
    }
}

/// Advance the logical counter, rolling `physical_ms` forward by 1ms if it would
/// overflow `u32`. This keeps strict monotonicity *total* (never panicking, never
/// wrapping below the prior stamp) instead of merely asserting it. `u32::MAX`
/// (~4.3e9) events sharing a single physical millisecond is unreachable in
/// practice; rolling forward is the standard HLC remedy. `physical_ms` itself
/// saturates at `u64::MAX` (year ~584 million — far past any real clock).
fn bump_logical(physical_ms: u64, base_logical: u32) -> (u64, u32) {
    match base_logical.checked_add(1) {
        Some(logical) => (physical_ms, logical),
        None => (physical_ms.saturating_add(1), 0),
    }
}

impl From<Hlc> for kutl_proto::sync::Hlc {
    fn from(h: Hlc) -> Self {
        Self {
            physical_ms: h.physical_ms,
            logical: h.logical,
            actor: h.actor.0.as_bytes().to_vec(),
        }
    }
}

/// A wire `Hlc` whose `actor` field is not a 16-byte UUID.
#[derive(Debug, thiserror::Error)]
#[error("hlc actor must be 16 bytes, got {0}")]
pub struct InvalidHlcActor(pub usize);

impl TryFrom<kutl_proto::sync::Hlc> for Hlc {
    type Error = InvalidHlcActor;

    fn try_from(p: kutl_proto::sync::Hlc) -> Result<Self, Self::Error> {
        let bytes: [u8; 16] = p
            .actor
            .as_slice()
            .try_into()
            .map_err(|_| InvalidHlcActor(p.actor.len()))?;
        Ok(Self {
            physical_ms: p.physical_ms,
            logical: p.logical,
            actor: ActorId(Uuid::from_bytes(bytes)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn actor(n: u8) -> ActorId {
        let mut b = [0u8; 16];
        b[15] = n;
        ActorId(Uuid::from_bytes(b))
    }

    // --- total order ---

    #[test]
    fn test_order_physical_dominates() {
        let a = Hlc {
            physical_ms: 5,
            logical: 9,
            actor: actor(9),
        };
        let b = Hlc {
            physical_ms: 6,
            logical: 0,
            actor: actor(0),
        };
        assert!(a < b, "higher physical wins regardless of logical/actor");
    }

    #[test]
    fn test_order_logical_breaks_physical_tie() {
        let a = Hlc {
            physical_ms: 5,
            logical: 1,
            actor: actor(9),
        };
        let b = Hlc {
            physical_ms: 5,
            logical: 2,
            actor: actor(0),
        };
        assert!(a < b, "same physical → higher logical wins");
    }

    #[test]
    fn test_order_actor_breaks_remaining_tie() {
        let a = Hlc {
            physical_ms: 5,
            logical: 1,
            actor: actor(1),
        };
        let b = Hlc {
            physical_ms: 5,
            logical: 1,
            actor: actor(2),
        };
        assert!(
            a < b,
            "same physical+logical → actor breaks the tie deterministically"
        );
    }

    // --- physical_touch (content edit → liveness touch, catalog §1.5/§3.4) ---

    #[test]
    fn test_physical_touch_concurrent_edit_beats_delete() {
        // A delete and an edit in the SAME millisecond: the edit (touch) wins —
        // "a concurrent edit beats the delete".
        let delete = Hlc {
            physical_ms: 100,
            logical: 3,
            actor: actor(7),
        };
        let touch = Hlc::physical_touch(100);
        assert!(touch > delete, "same-ms edit-touch outranks the delete");
    }

    #[test]
    fn test_physical_touch_loses_to_strictly_later_delete() {
        // A stale (offline) edit at an older physical time loses to a
        // causally-later delete — it cannot revive past it.
        let touch = Hlc::physical_touch(50);
        let later_delete = Hlc {
            physical_ms: 100,
            logical: 0,
            actor: actor(1),
        };
        assert!(
            touch < later_delete,
            "older edit loses to a strictly-later delete"
        );
    }

    #[test]
    fn test_physical_touch_beats_strictly_earlier_delete() {
        let earlier_delete = Hlc {
            physical_ms: 50,
            logical: 9,
            actor: actor(9),
        };
        let touch = Hlc::physical_touch(100);
        assert!(touch > earlier_delete, "newer edit beats an earlier delete");
    }

    // --- tick monotonicity ---

    #[test]
    fn test_tick_strictly_increases_even_when_wall_stalls() {
        let mut c = HlcClock::new(actor(1));
        let t1 = c.tick(100);
        let t2 = c.tick(100); // wall stalled at the same ms
        let t3 = c.tick(50); // wall regressed
        assert!(t1 < t2, "stalled wall → logical advances");
        assert!(t2 < t3, "regressed wall → logical advances, physical held");
        assert_eq!(t3.physical_ms, 100);
        assert_eq!(t3.logical, 2);
    }

    #[test]
    fn test_tick_resets_logical_on_physical_advance() {
        let mut c = HlcClock::new(actor(1));
        c.tick(100);
        c.tick(100);
        let t = c.tick(101);
        assert_eq!(t.physical_ms, 101);
        assert_eq!(t.logical, 0, "physical advanced → logical resets");
    }

    // --- recv causality ---

    #[test]
    fn test_recv_dominates_both_local_and_remote() {
        let mut c = HlcClock::new(actor(1));
        let local = c.tick(100);
        let remote = Hlc {
            physical_ms: 100,
            logical: 5,
            actor: actor(2),
        };
        let after = c.recv(remote, 100);
        assert!(after > local, "recv dominates the prior local stamp");
        assert!(after > remote, "recv dominates the observed remote stamp");
    }

    #[test]
    fn test_recv_then_tick_is_causally_after_remote() {
        // The load-bearing property: an op emitted after observing a remote op
        // is ordered strictly after it.
        let mut c = HlcClock::new(actor(1));
        let remote = Hlc {
            physical_ms: 200,
            logical: 3,
            actor: actor(2),
        };
        c.recv(remote, 10); // local wall behind the remote
        let next = c.tick(10);
        assert!(
            next > remote,
            "tick after recv is causally after the remote"
        );
    }

    // --- monotonic restart ---

    #[test]
    fn test_restore_never_emits_at_or_below_floor() {
        let floor = Hlc {
            physical_ms: 500,
            logical: 7,
            actor: actor(1),
        };
        let mut c = HlcClock::restore(actor(1), floor);
        let next = c.tick(10); // wall well below the persisted floor
        assert!(
            next > floor,
            "a restored clock cannot emit at or below its floor"
        );
    }

    // --- property tests ---

    fn arb_hlc() -> impl Strategy<Value = Hlc> {
        (0u64..50, 0u32..5, 0u8..3).prop_map(|(physical_ms, logical, a)| Hlc {
            physical_ms,
            logical,
            actor: actor(a),
        })
    }

    proptest! {
        /// The order is total and antisymmetric: distinct stamps compare one way.
        #[test]
        fn order_is_total(a in arb_hlc(), b in arb_hlc()) {
            if a == b {
                prop_assert_eq!(a.cmp(&b), std::cmp::Ordering::Equal);
            } else {
                prop_assert_ne!(a.cmp(&b), std::cmp::Ordering::Equal);
                prop_assert_eq!(a.cmp(&b), b.cmp(&a).reverse());
            }
        }

        /// Lattice join (max) is commutative, associative, idempotent — so HLC
        /// fields can be field-maxed in a DocRecord merge order-independently.
        #[test]
        fn max_join_laws(a in arb_hlc(), b in arb_hlc(), c in arb_hlc()) {
            prop_assert_eq!(a.max(b), b.max(a));
            prop_assert_eq!(a.max(b).max(c), a.max(b.max(c)));
            prop_assert_eq!(a.max(a), a);
            prop_assert!(a.max(b) >= a);
            prop_assert!(a.max(b) >= b);
        }

        /// A run of ticks from one clock is strictly increasing under any
        /// adversarial wall-time sequence (stalls, regressions, jumps).
        #[test]
        fn tick_run_is_strictly_increasing(walls in prop::collection::vec(0u64..1000, 1..30)) {
            let mut c = HlcClock::new(actor(1));
            let mut prev = c.tick(walls[0]);
            for &w in &walls[1..] {
                let next = c.tick(w);
                prop_assert!(next > prev, "tick must strictly increase: {:?} !> {:?}", next, prev);
                prev = next;
            }
        }

        /// recv always produces a stamp strictly greater than both inputs.
        #[test]
        fn recv_dominates(remote in arb_hlc(), wall in 0u64..1000, pre in 0u64..1000) {
            let mut c = HlcClock::new(actor(1));
            let local = c.tick(pre);
            let after = c.recv(remote, wall);
            prop_assert!(after > local);
            prop_assert!(after > remote);
        }

        /// core ↔ proto conversion round-trips losslessly — inclusive ranges so
        /// the all-ones bit patterns (most likely to expose a truncating cast)
        /// are exercised.
        #[test]
        fn proto_round_trip(physical_ms in 0u64..=u64::MAX, logical in 0u32..=u32::MAX, a in 0u8..=255) {
            let original = Hlc { physical_ms, logical, actor: actor(a) };
            let wire: kutl_proto::sync::Hlc = original.into();
            let back = Hlc::try_from(wire).expect("16-byte actor round-trips");
            prop_assert_eq!(original, back);
        }
    }

    #[test]
    fn test_logical_saturation_rolls_physical_forward_no_panic() {
        // A clock seeded at logical = u32::MAX on a fixed physical, ticked with a
        // stalled/regressed wall clock, must not overflow: physical rolls forward
        // 1ms and logical resets, preserving strict monotonicity.
        let floor = Hlc {
            physical_ms: 100,
            logical: u32::MAX,
            actor: actor(1),
        };
        let mut c = HlcClock::restore(actor(1), floor);
        let next = c.tick(50); // wall behind; naive logical+1 would overflow
        assert!(next > floor, "monotonic across the saturation boundary");
        assert_eq!(next.physical_ms, 101);
        assert_eq!(next.logical, 0);
    }

    #[test]
    fn test_recv_saturation_rolls_physical_forward_no_panic() {
        let mut c = HlcClock::restore(
            actor(1),
            Hlc {
                physical_ms: 100,
                logical: u32::MAX,
                actor: actor(1),
            },
        );
        // remote at the same physical with max logical — the three-way tie path.
        let remote = Hlc {
            physical_ms: 100,
            logical: u32::MAX,
            actor: actor(2),
        };
        let after = c.recv(remote, 50);
        assert!(after > remote);
        assert_eq!(after.physical_ms, 101);
        assert_eq!(after.logical, 0);
    }

    #[test]
    fn test_try_from_rejects_wrong_actor_len() {
        let bad = kutl_proto::sync::Hlc {
            physical_ms: 1,
            logical: 0,
            actor: vec![0u8; 8], // not 16
        };
        assert!(Hlc::try_from(bad).is_err());
    }
}
