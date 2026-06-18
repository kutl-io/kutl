//! `DaemonCore::handle`: the pure, sans-IO event dispatch.
//!
//! Ports the arms of `handle_file_event` and
//! `handle_sync_event` (daemon.rs) so each former `self.foo().await` that
//! produced an IO/channel side effect now **pushes an [`Effect`]** instead.
//! The handler families live in the child modules (`local_events`,
//! `remote_content`, `remote_lifecycle`, `blob`, `helpers`); the load-bearing
//! HLC gating asymmetry is documented in `remote_lifecycle`'s module doc.
//!
//! Purity: nothing here `.await`s, touches `std::fs`, holds an `mpsc`, or reads a
//! clock. All time is injected via [`EventStamp`](crate::core::EventStamp);
//! the suppress-echo hash is
//! computed in-core and carried inline in [`Effect::WriteFile`] (Cycle B closed
//! by construction — there is no separate `suppress_tx.send().await`).

use tracing::debug;
use uuid::Uuid;

use crate::core::shadow::casefold;
use crate::core::{Effect, EffectResult, Event, SpaceState, reconcile_placement};

mod blob;
mod helpers;
mod local_events;
mod remote_content;
mod remote_lifecycle;
#[cfg(test)]
mod test_support;

// Re-exported for qualified-path callers outside the crate: kutl-sim's shell
// claims/re-keys identity at its place edges via
// `kutl_daemon::core::handle::{register_identity, move_identity}`.
pub use helpers::{move_identity, register_identity};

use local_events::{handle_file_modified, handle_file_removed, handle_file_renamed};
use remote_content::handle_remote_ops;
use remote_lifecycle::{
    handle_lifecycle_ack, handle_remote_register, handle_remote_rename, handle_remote_unregister,
};

/// The pure per-space sync core. Holds no state of its own — it is a namespace
/// for the two pure transition functions over [`SpaceState`].
pub struct DaemonCore;

impl DaemonCore {
    /// Apply one event to the state, returning the effects to execute.
    ///
    /// No `.await`, no `std::fs`, no `mpsc`, no clock read — every impurity is in
    /// the returned `Vec<Effect>` or injected via the event's
    /// [`EventStamp`](crate::core::EventStamp).
    // The driver-handled and cascade-probe arm groups are deliberately kept
    // separate even though both return `vec![]` today: they are inert for
    // different reasons (driver-owned session/startup events vs. Phase 4 cascade
    // feedbacks the non-cascade handlers never emit). Merging them would erase
    // that intent.
    #[allow(clippy::match_same_arms)]
    #[must_use]
    pub fn handle(state: &mut SpaceState, event: Event) -> Vec<Effect> {
        // graft 2: the revival exemption is PER-PASS — a doc is exempt from
        // path-collision deferral only for the reconcile its own register triggers
        // (mirrors the former imperative `is_revival`'s per-call scope). Clear it at the
        // top of every handle so a stale exemption can't leak into an unrelated
        // later pass and let a brand-new doc clobber an untracked occupant.
        state.exempt_revival.clear();
        debug!(event = event.name(), "core event");
        match event {
            // ── local file events (ports handle_file_event, daemon.rs) ──
            Event::FileModified {
                rel,
                content,
                stamp,
            } => handle_file_modified(state, &rel, content.as_deref(), stamp),
            Event::FileRemoved { rel, stamp } => handle_file_removed(state, &rel, stamp),
            Event::FileRenamed { old, new, stamp } => handle_file_renamed(state, &old, &new, stamp),

            // ── remote sync events (ports handle_sync_event, daemon.rs) ──
            Event::RemoteOps {
                document_id,
                ops,
                metadata,
                content_mode,
                local_content,
                stamp,
            } => handle_remote_ops(
                state,
                &document_id,
                &ops,
                &metadata,
                content_mode,
                local_content.as_deref(),
                stamp,
            ),

            // ── timers (driver-injected) ──
            // MetricsTick refreshes the per-space gauges (`emit_periodic_metrics`,
            // daemon/effects.rs) AND
            // drives the placement cascade to its fixpoint: graft 3 leaves the
            // shadow unchanged on a failed disk effect precisely so "the next
            // reconcile retries" — but without a guaranteed driver an idle
            // space stays diverged after a swallowed write (ENOSPC) until some
            // unrelated event arrives. At the fixpoint (shadow == desired) the
            // reconcile emits nothing, so the tick costs one O(docs)
            // projection per interval.
            Event::MetricsTick { .. } => {
                let mut effects = vec![Effect::EmitMetrics];
                effects.extend(reconcile_placement(state));
                effects
            }

            // ── lifecycle arms (preserve the freshness-gating asymmetry):
            //    register is fold-only (NOT gated — a revival must always apply);
            //    rename/unregister/ack are gated by lifecycle_event_is_fresh.
            Event::RemoteRegister {
                document_id,
                path,
                stamp,
            } => handle_remote_register(state, &document_id, &path, stamp),
            Event::RemoteRename {
                document_id,
                old_path,
                new_path,
                rename_causal_floor,
                stamp,
            } => handle_remote_rename(
                state,
                &document_id,
                &old_path,
                &new_path,
                rename_causal_floor,
                stamp,
            ),
            Event::RemoteUnregister { document_id, stamp } => {
                handle_remote_unregister(state, &document_id, stamp)
            }
            Event::LifecycleAck {
                document_id,
                effective_path,
                stamp,
            } => handle_lifecycle_ack(state, &document_id, effective_path.as_deref(), stamp),

            // ── driver-handled: the core does not act on these. Connected/
            //    Disconnected drive session lifecycle (kept in the driver, §7);
            //    SpaceDocuments is consumed at startup.
            Event::Connected | Event::Disconnected | Event::SpaceDocuments { .. } => vec![],

            // ── cascade-probe feedbacks: Phase 4 grafts. graft 1's
            //    UntrackedFileObserved is the shell's report that its atomic stat
            //    found an unexpected untracked file when it went to place — mark the
            //    path Occupant::Untracked in the shadow and re-derive placement so
            //    the doc whose target this is now DEFERS instead of clobbering the
            //    local bytes (closing the `get_or_create_uuid` two-author conflation).
            Event::UntrackedFileObserved { rel, stamp: _ } => {
                state
                    .shadow
                    .shadow_occupant
                    .insert(casefold(&rel), crate::core::Occupant::Untracked);
                reconcile_placement(state)
            }
            // The shell observed `rel`'s untracked occupant vacate (a local `rm`):
            // clear the `Occupant::Untracked` mark and re-run reconcile so a doc
            // that DEFERRED behind it (graft 2 predicate now false) materializes.
            // Without this re-trigger the deferral is a permanent strand (the
            // liveness bug TDD case 12 guards). The shell removed the on-disk bytes
            // at its edge; the core only owns the shadow + placement re-derivation.
            Event::UntrackedFileRemoved { rel, stamp: _ } => {
                state.shadow.shadow_occupant.remove(&casefold(&rel));
                reconcile_placement(state)
            }
        }
    }

    /// Re-derive placement without an incoming event — the explicit reconcile
    /// entry point for callers outside the event stream: the TWIN-predicate
    /// equivalence test drives the defer arm through it, and a shell can use
    /// it after a failed disk effect (graft-3 self-heal) without minting a
    /// dummy event. In the live driver the periodic `MetricsTick` already
    /// drives `reconcile_placement` through `handle`.
    pub fn reconcile(state: &mut SpaceState) -> Vec<Effect> {
        reconcile_placement(state)
    }

    /// Fold a shell's success report into the shadow (graft 3).
    ///
    /// Called by the driver after a landed disk effect; a FAILURE feeds nothing,
    /// so the shadow reflects only landed ops and the next reconcile re-derives +
    /// retries (preserving today's stateless re-stat self-heal — the log-and-swallow
    /// contract of the driver's disk funnels, formerly the procedural disk-apply
    /// paths). Mirrors the `file_identity`/`refresh_inode`
    /// bookkeeping the funnels do on success (daemon/effects.rs): a write records the
    /// path as `Tracked` by the doc currently at `rel` (resolved from
    /// `file_identity`) and folds the post-op inode; a rename moves the occupant +
    /// inode from `old` to `new`.
    pub fn apply_effect_result(state: &mut SpaceState, result: EffectResult) {
        debug!(?result, "shadow fold");
        match result {
            EffectResult::FileWritten { rel, inode } => {
                if let Some(id) = state
                    .file_identity
                    .get(&rel)
                    .and_then(|fi| Uuid::parse_str(&fi.document_uuid).ok())
                {
                    state.shadow.set_tracked(&rel, id);
                }
                if let Some(inode) = inode {
                    state.shadow.set_inode(inode, &rel);
                }
            }
            EffectResult::RenameApplied { old, new, inode } => {
                // Vacate the old path and re-key the occupant onto the new path.
                let id = state
                    .file_identity
                    .get(&new)
                    .and_then(|fi| Uuid::parse_str(&fi.document_uuid).ok());
                state.shadow.rename_fold(&old, &new, id, inode);
            }
            EffectResult::FileRemoved { rel } => {
                state.shadow.remove_fold(&rel);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::test_support::{doc_id, hlc, st, track_remote};
    use super::*;
    use crate::core::EventStamp;

    #[test]
    fn test_metrics_tick_emits_emit_metrics_effect() {
        // MetricsTick (the driver's `emit_periodic_metrics`) folds to a pure
        // Effect::EmitMetrics.
        let mut s = st();
        let effects = DaemonCore::handle(
            &mut s,
            Event::MetricsTick {
                stamp: EventStamp {
                    wall_ms: 10,
                    origin_hlc: None,
                },
            },
        );
        assert!(effects.iter().any(|e| matches!(e, Effect::EmitMetrics)));
    }

    #[test]
    fn test_untracked_file_observed_marks_occupant_and_defers_mover() {
        // graft 1: the shell reports its atomic stat found an untracked file at the
        // target a non-revival mover wanted. The arm marks the path
        // Occupant::Untracked and re-derives placement → the mover now DEFERS
        // (graft 2 predicate) instead of clobbering. Closes the
        // `get_or_create_uuid` two-author conflation.
        let mut s = st();
        let id = doc_id();
        // A non-revival remote register makes the doc an alive mover onto "race.md"
        // with NO shadow occupant yet (the core believed the path free).
        let actor = s.hlc.last().actor;
        let _ = DaemonCore::handle(
            &mut s,
            Event::RemoteRegister {
                document_id: id.clone(),
                path: "race.md".into(),
                stamp: EventStamp {
                    wall_ms: 10,
                    origin_hlc: Some(hlc(actor, 10)),
                },
            },
        );
        assert!(
            !s.deferred.contains_key(&id),
            "with the path believed free, the doc is a plain mover, not deferred"
        );
        // The shell's stat found an untracked file in the place window.
        let effects = DaemonCore::handle(
            &mut s,
            Event::UntrackedFileObserved {
                rel: PathBuf::from("race.md"),
                stamp: EventStamp {
                    wall_ms: 11,
                    origin_hlc: None,
                },
            },
        );
        assert_eq!(
            s.shadow
                .shadow_occupant
                .get(&casefold(&PathBuf::from("race.md"))),
            Some(&crate::core::Occupant::Untracked),
            "the observed path must be marked Occupant::Untracked"
        );
        assert!(
            s.deferred.contains_key(&id),
            "the mover must now defer onto the untracked occupant, got {effects:?}"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::GuardedPlace { .. })),
            "a deferred doc emits no place, got {effects:?}"
        );
    }

    #[test]
    fn test_apply_effect_result_file_written_records_shadow_tracked_and_inode() {
        // Graft 3: a landed write folds the path into the shadow as Tracked by the
        // doc at that path, plus the post-op inode.
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        // Clear the shadow occupant the helper set so we can observe the fold.
        s.shadow
            .shadow_occupant
            .remove(&casefold(&PathBuf::from("a.md")));
        DaemonCore::apply_effect_result(
            &mut s,
            EffectResult::FileWritten {
                rel: PathBuf::from("a.md"),
                inode: Some(77),
            },
        );
        let uid = Uuid::parse_str(&id).unwrap();
        assert_eq!(
            s.shadow
                .shadow_occupant
                .get(&casefold(&PathBuf::from("a.md"))),
            Some(&crate::core::Occupant::Tracked(uid))
        );
        assert_eq!(
            s.shadow.shadow_path_inode.get(&PathBuf::from("a.md")),
            Some(&77)
        );
    }

    #[test]
    fn test_apply_effect_result_rename_applied_moves_shadow_occupant() {
        // Graft 3: a landed rename vacates `old` in the shadow and re-keys the
        // occupant + inode onto `new`.
        let mut s = st();
        let id = doc_id();
        track_remote(&mut s, &id, "a.md", 10);
        // Move identity so file_identity resolves the doc at the new path.
        move_identity(&mut s, &PathBuf::from("a.md"), PathBuf::from("b.md"), &id);
        DaemonCore::apply_effect_result(
            &mut s,
            EffectResult::RenameApplied {
                old: PathBuf::from("a.md"),
                new: PathBuf::from("b.md"),
                inode: Some(88),
            },
        );
        assert!(
            !s.shadow
                .shadow_occupant
                .contains_key(&casefold(&PathBuf::from("a.md"))),
            "the old path must be vacated in the shadow"
        );
        let uid = Uuid::parse_str(&id).unwrap();
        assert_eq!(
            s.shadow
                .shadow_occupant
                .get(&casefold(&PathBuf::from("b.md"))),
            Some(&crate::core::Occupant::Tracked(uid))
        );
        assert_eq!(
            s.shadow.shadow_path_inode.get(&PathBuf::from("b.md")),
            Some(&88)
        );
    }
}
