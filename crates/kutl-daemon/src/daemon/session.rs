//! Session runtime for [`SpaceWorker`]: the reconnect-with-backoff outer loop
//! (`run`/`run_once`), per-session setup and teardown (`run_session`), and the
//! single-intake event loop (`event_loop`/`next_event`) — its [`LoopInput`]
//! work units, the [`IntakeGate`] file-event backpressure gate, and the
//! backlog-health watchdog classification.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::client::{self, SyncCommand, SyncEvent};
use crate::core::{DaemonCore, Event};
use crate::watcher::{FileEvent, FileWatcher, Suppression};

use super::SpaceWorker;

/// One unit of work pulled by [`SpaceWorker::next_event`] from the merged intake.
///
/// The single-intake loop turns each wire event into exactly one of these: a pure
/// core [`Event`] (drained through `handle` + `apply_effect`), an imperative
/// file/sync event for a path the pure core does not cover yet (binary blobs,
/// local inode/overwrite rename detection, the lifecycle disk-reconcile cascade),
/// or a control signal (shutdown/disconnect/closed/activity-only).
pub(super) enum LoopInput {
    /// A pure core event plus whether it counts as sync progress (`activity`):
    /// a metrics tick is not user activity and must not keep a one-shot session
    /// alive or advance the progress gauge.
    Core { event: Event, activity: bool },
    /// A file event served at the driver edge because it needs LIVE disk
    /// reads the pure core cannot do: the inode/overwrite rename detections
    /// and the directory-shaped expansions.
    ImperativeFile(FileEvent),
    /// A sync event served at the driver edge: only `StaleSubscriber`
    /// recovery — every content and lifecycle event is core-routed.
    ImperativeSync(SyncEvent),
    /// Cancellation or the one-shot idle timeout fired — shut down.
    Shutdown,
    /// The relay reported a disconnect — end the session and reconnect.
    Disconnected,
    /// All intake channels closed — break the loop.
    ChannelsClosed,
    /// An event was consumed that produces no core work (e.g. a file gone before
    /// the edge read, a `Connected`/`SpaceDocuments` notice) but still counts as
    /// session activity for the one-shot idle reset.
    Activity,
}

/// Outcome of a single session, used to decide whether to reconnect or exit.
enum SessionOutcome {
    /// Relay disconnected or channels closed — reconnect.
    Disconnected,
    /// Cancellation requested — shut down gracefully.
    Shutdown,
    /// Relay explicitly rejected authentication — do not retry.
    AuthRejected(String),
}

/// Maximum reconnect backoff delay.
const MAX_BACKOFF: Duration = kutl_core::std_duration(kutl_core::SignedDuration::from_secs(30));
/// Initial reconnect delay.
const INITIAL_BACKOFF: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_millis(500));
/// Multiplier applied to the reconnect backoff after each failed attempt
/// (exponential growth, capped at [`MAX_BACKOFF`]).
const BACKOFF_MULTIPLIER: u32 = 2;
/// Channel capacity for sync events and file events.
pub(super) const CHANNEL_CAPACITY: usize = 64;

/// Intake high-water mark for outstanding ack-bearing commands. The outbound
/// relay channels (`sync_cmd`, `suppress`) are unbounded so the single loop task
/// never blocks producing (the deadlock cycles closed in
/// `2026-06-08-daemon-sync-loop-deadlock-findings.md`); this bounds the only
/// thing that can grow unbounded — the loop racing ahead of a slow relay. When
/// the [`IntakeGate`] depth (incremented per ack-bearing command, decremented
/// per ack) reaches this watermark, the `file_event` intake arm is disabled in
/// [`SpaceWorker::next_event`] so the loop keeps draining `sync_event` (acks
/// lower the backlog) until it falls back under the mark. NEVER gate
/// `sync_event`/ack consumption — that would self-deadlock, since acks are what
/// lower the backlog. Tunable: large enough that a healthy bursty relay never
/// throttles, small enough to bound the outbound queue's memory.
const SYNC_BACKLOG_HIGH_WATER: u64 = 256;

/// Idle timeout for one-shot sync mode: exit after this much inactivity.
const ONE_SHOT_IDLE_TIMEOUT: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(2));

/// How often the event loop refreshes the per-space `/metrics` gauges (queue
/// depth, blob backlog, progress staleness). `tokio::time::interval` fires its
/// first tick immediately, so the gauges register as soon as a session starts;
/// thereafter they refresh on this cadence so the staleness gauge keeps
/// climbing while the daemon is idle.
const METRICS_EMIT_INTERVAL: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(10));

/// How long the sync loop may show in-flight unacked lifecycle commands with no
/// forward progress before the watchdog escalates to ERROR. Chosen as 3× the
/// 10s metrics cadence so a single slow tick never trips it — only a genuine
/// stall (a stranded `confirmed:false` doc) survives this window.
const STALE_PROGRESS_THRESHOLD: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(30));

/// Backlog depth at or above which the watchdog WARNs even while progress is
/// still being made — a backlog this deep is draining but worth investigating.
/// Set to the bounded inbound channel capacity so it fires only when the loop
/// is running a full channel behind, never on normal bursty traffic.
///
/// `CHANNEL_CAPACITY` is 64 (fits i64 exactly; the cast cannot wrap).
#[allow(clippy::cast_possible_wrap)]
const BACKLOG_WARN_DEPTH: u64 = CHANNEL_CAPACITY as u64;

/// The watchdog's read of sync health from the backlog depth and progress staleness.
///
/// `Stalled` outranks `DeepButMoving`: a backlog that is both deep AND stale
/// is the real wedge and must escalate to ERROR, not stay at WARN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BacklogHealth {
    /// Nothing in flight, or draining within normal bounds — no log.
    Healthy,
    /// Backlog is at or above [`BACKLOG_WARN_DEPTH`] but progress is recent — WARN.
    DeepButMoving,
    /// Positive backlog with no progress for ≥ [`STALE_PROGRESS_THRESHOLD`] — ERROR.
    Stalled,
}

/// Pure classification of sync health. No clock read — `stale_for` is passed in
/// (`last_progress.elapsed()` at the call site) so this stays unit-testable with
/// no clock or IO. `Stalled` outranks `DeepButMoving`: a deep AND stale backlog
/// is the real wedge.
pub(super) fn classify_backlog(backlog: u64, stale_for: Duration) -> BacklogHealth {
    if backlog > 0 && stale_for >= STALE_PROGRESS_THRESHOLD {
        BacklogHealth::Stalled
    } else if backlog >= BACKLOG_WARN_DEPTH {
        BacklogHealth::DeepButMoving
    } else {
        BacklogHealth::Healthy
    }
}

/// In-flight ack-bearing relay commands — the single owner of the intake-gate
/// arithmetic. Counts a command at send ([`IntakeGate::track`], called by
/// `send_cmd` for every outbound command), clears one per ack
/// ([`IntakeGate::ack`], the classify edge + the startup `SpaceDocuments`
/// response), and answers the gate question ([`IntakeGate::intake_open`]).
///
/// The pure core knows nothing of this counter (a driver concern, per the
/// sans-IO redesign). Splitting the two halves across layers is exactly the
/// gamma bulk-move-stall / intake-gate-latch deadlock class: an ack path that
/// skips its decrement pins the depth at [`SYNC_BACKLOG_HIGH_WATER`] and
/// starves the watcher forever. One owner, one increment site, one decrement
/// primitive.
#[derive(Debug, Default)]
pub(super) struct IntakeGate {
    /// Ack-bearing commands sent but not yet acked. Unsigned: the zero floor
    /// is structural, not a `.max(0)` repeated at every read site.
    in_flight: u64,
}

impl IntakeGate {
    /// Whether `cmd` expects a lifecycle/list ack from the relay.
    fn is_ack_bearing(cmd: &SyncCommand) -> bool {
        matches!(
            cmd,
            SyncCommand::RegisterDocument { .. }
                | SyncCommand::RenameDocument { .. }
                | SyncCommand::UnregisterDocument { .. }
                | SyncCommand::ListSpaceDocuments { .. }
        )
    }

    /// Count `cmd` iff it is ack-bearing — the ONLY increment site.
    pub(super) fn track(&mut self, cmd: &SyncCommand) {
        if Self::is_ack_bearing(cmd) {
            self.in_flight += 1;
        }
    }

    /// Clear one in-flight entry on its ack — the ONLY decrement primitive.
    /// Saturating: a duplicate or unexpected ack stays at zero rather than
    /// underflowing into a false all-clear for a later genuine stall.
    pub(super) fn ack(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    /// A fresh session has no outstanding commands (the previous session's
    /// in-flight commands died with its dropped channel).
    fn reset(&mut self) {
        self.in_flight = 0;
    }

    /// Current depth, for the `/metrics` gauge + watchdog classification.
    pub(super) fn depth(&self) -> u64 {
        self.in_flight
    }

    /// INTAKE GATE: whether the `file_event` arm may run — depth below the
    /// high water. NEVER gate `sync_event`/ack consumption on this (acks are
    /// what lower the depth; gating them self-deadlocks).
    fn intake_open(&self) -> bool {
        self.in_flight < SYNC_BACKLOG_HIGH_WATER
    }
}

impl SpaceWorker {
    /// Run the daemon event loop with automatic reconnection.
    ///
    /// Returns `Ok(())` on graceful shutdown (cancellation).
    pub async fn run(mut self) -> Result<()> {
        // Pre-flight: case-variant duplicates are bad state, not transient.
        // Run once before any session so we never enter the retry loop.
        if let Err(err) = crate::case_collision::detect_case_collisions(&self.config.space_root) {
            error!(
                space_id = %self.config.space_id,
                "{}",
                err.format_user_message()
            );
            return Err(err.into());
        }

        if self.config.one_shot {
            return self.run_once().await;
        }

        let mut backoff = INITIAL_BACKOFF;
        let mut is_reconnect = false;

        loop {
            if is_reconnect {
                crate::metrics_calls::record_relay_reconnect(&self.config.relay_url);
            }
            // Track whether this session ever reached the Connected state.
            // We only emit `connected=false` after a previous `true` so the
            // gauge never publishes "disconnected" for a connection that
            // never happened.
            let mut was_connected = false;
            let outcome = self.run_session(false, &mut was_connected).await;
            if was_connected {
                crate::metrics_calls::record_relay_connected(&self.config.relay_url, false);
            }
            is_reconnect = true;
            match outcome {
                Ok(SessionOutcome::Disconnected) => {
                    info!("session ended cleanly, reconnecting");
                    backoff = INITIAL_BACKOFF;
                    tokio::select! {
                        () = tokio::time::sleep(INITIAL_BACKOFF) => {}
                        () = self.config.cancel.cancelled() => {
                            info!("cancelled during reconnect backoff");
                            return Ok(());
                        }
                    }
                }
                Ok(SessionOutcome::Shutdown) => {
                    info!("received shutdown signal");
                    return Ok(());
                }
                Ok(SessionOutcome::AuthRejected(msg)) => {
                    return Err(anyhow::anyhow!("authentication rejected by relay: {msg}"));
                }
                Err(e) => {
                    warn!(error = %e, ?backoff, "session failed, reconnecting");
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = self.config.cancel.cancelled() => {
                            info!("cancelled during reconnect backoff");
                            return Ok(());
                        }
                    }
                    backoff = (backoff * BACKOFF_MULTIPLIER).min(MAX_BACKOFF);
                }
            }
        }
    }

    /// Run a single sync pass: connect, push local state, pull remote state, exit.
    async fn run_once(&mut self) -> Result<()> {
        // One-shot mode never reconnects, so the gauge transition isn't
        // observable; we still pass a sink to keep the signature uniform.
        let mut was_connected = false;
        match self.run_session(true, &mut was_connected).await? {
            SessionOutcome::Shutdown => info!("sync complete"),
            SessionOutcome::Disconnected => warn!("relay disconnected before sync completed"),
            SessionOutcome::AuthRejected(msg) => {
                anyhow::bail!("authentication rejected by relay: {msg}");
            }
        }
        Ok(())
    }

    /// Resolve a bearer token from stored credentials or DID challenge-response.
    ///
    /// Stored file credentials are only used if their `relay_url` matches
    /// the current relay. This prevents stale tokens from a different relay
    /// causing infinite auth-rejection retry loops.
    async fn resolve_auth_token(&self) -> String {
        // Priority 1: KUTL_TOKEN env var (explicit override, always trusted).
        if let Ok(token) = std::env::var(kutl_client::credentials::TOKEN_ENV_VAR)
            && !token.is_empty()
        {
            info!("using token from environment");
            return token;
        }

        // Priority 2: stored credentials, only if relay URL matches.
        let creds_path = kutl_client::credentials::default_credentials_path().ok();
        if let Some(path) = creds_path.as_deref()
            && let Ok(Some(creds)) = kutl_client::credentials::StoredCredentials::load(path)
            && creds.relay_url == self.config.relay_url
        {
            info!("using stored token");
            return creds.token;
        }
        if let Some(ref signing_key) = self.config.signing_key {
            return kutl_client::authenticate(
                &self.config.relay_url,
                &self.config.author_did,
                signing_key,
            )
            .await
            .unwrap_or_else(|e| {
                error!(error = %e, "authentication failed, proceeding without token");
                String::new()
            });
        }
        String::new()
    }

    /// Run a single session: authenticate, connect, relay, return on disconnect.
    ///
    /// `was_connected` is set to `true` when the relay reports Connected,
    /// letting the caller decide whether to emit the matching `false`
    /// transition for the connection-state gauge.
    async fn run_session(
        &mut self,
        one_shot: bool,
        was_connected: &mut bool,
    ) -> Result<SessionOutcome> {
        let auth_token = self.resolve_auth_token().await;

        // A fresh session is itself activity, and the previous session's
        // in-flight blob sends went away with its dropped channel — reset both
        // metric sources so the gauges reflect this session, not a stale one.
        self.last_progress = Instant::now();
        self.blob_backlog.store(0, Ordering::Relaxed);
        // A fresh session starts with no outstanding commands; the previous
        // session's in-flight commands went away with its dropped channel.
        self.intake.reset();

        // The two OUTBOUND channels are UNBOUNDED so the single loop task never
        // blocks producing: a bounded `.send().await` from inside the loop —
        // while a peer is itself blocked sending back on a channel the loop must
        // drain — is the circular wait that deadlocked the sync loop. Acks and
        // commands must never be dropped (a lost ack leaves a doc stuck
        // `confirmed:false` forever), so try-send-and-drop is not an option;
        // backpressure is kept at INTAKE instead (the `file_event` gate, see
        // `next_event`). The two INBOUND channels stay bounded — the loop drains
        // them, so they impose normal backpressure on the watcher/relay.
        let (sync_cmd_tx, sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (sync_event_tx, mut sync_event_rx) = mpsc::channel::<SyncEvent>(CHANNEL_CAPACITY);
        let (file_event_tx, mut file_event_rx) = mpsc::channel::<FileEvent>(CHANNEL_CAPACITY);
        let (suppress_tx, suppress_rx) = mpsc::unbounded_channel::<Suppression>();

        // Spawn the WS client.
        let relay_url = self.config.relay_url.clone();
        let space_id = self.config.space_id.clone();
        let client_name = self.config.author_did.clone();
        let display_name = self.config.display_name.clone();
        let blob_backlog = self.blob_backlog.clone();
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client::run_client(
                &relay_url,
                &space_id,
                &client_name,
                &auth_token,
                &display_name,
                sync_cmd_rx,
                sync_event_tx,
                blob_backlog,
            )
            .await
            {
                error!(error = %e, "sync client error");
                crate::metrics_calls::record_error(crate::metrics_calls::error_category::RELAY);
            }
        });

        // Wait for connection before subscribing.
        match sync_event_rx.recv().await {
            Some(SyncEvent::Connected) => {
                info!("connected to relay");
                *was_connected = true;
                crate::metrics_calls::record_relay_connected(&self.config.relay_url, true);
            }
            Some(SyncEvent::AuthRejected(msg)) => {
                return Ok(SessionOutcome::AuthRejected(msg));
            }
            other => {
                anyhow::bail!("expected Connected event, got: {other:?}");
            }
        }

        // Discover remote documents, reconcile with local state, and execute.
        // MUST complete before the watcher starts: reconciliation mutates the
        // space (offline-rename conforms, deletions) WITHOUT registering echo
        // suppressions — `reconcile_offline_renames` relies on no watcher
        // running.
        self.startup_reconciliation(&sync_cmd_tx, &suppress_tx, &mut sync_event_rx)
            .await?;

        // Start the file watcher BEFORE the initial scan (space_root is already
        // canonical from caller). The scan only READS the space (its disk
        // writes are `.kutl` sidecars, ignored paths), so no suppression is
        // needed — and a file landing in a directory the scan's walkdir has
        // already passed (a `cp -R` still running as the daemon starts) is
        // caught by the watcher instead of staying silently invisible until
        // the next restart. Events observed during the scan queue in the
        // bounded channel until the loop starts; a file both scanned and
        // event-delivered is processed twice idempotently (the second pass
        // sees content == CRDT and skips).
        let mut watcher = FileWatcher::new(&self.config.space_root, file_event_tx, suppress_rx)?;
        let watcher_handle = tokio::spawn(async move {
            watcher.run().await;
        });

        // Scan all files on disk. For files already tracked by reconciliation,
        // this diffs file content against the CRDT and sends ops for any
        // offline edits. For new files, this registers and subscribes.
        //
        // This must run BEFORE processing remote catch-up so that local file
        // edits are applied to the CRDT against the pre-sync baseline. The
        // subsequent event loop merges remote ops, and diamond-types handles
        // the concurrent position transforms correctly.
        self.initial_file_scan(&sync_cmd_tx, &suppress_tx, &mut sync_event_rx);

        // Seed the in-memory disk shadow from the tracked-on-disk truth BEFORE the
        // event loop runs, so the pure core's shadow-based "file present?"/inode
        // checks match live disk on the first event (see `seed_shadow`).
        self.seed_shadow();

        if let Some(ref notify) = self.config.ready {
            notify.notify_one();
        }

        // Main event loop.
        let result = self
            .event_loop(
                &mut file_event_rx,
                &mut sync_event_rx,
                &sync_cmd_tx,
                &suppress_tx,
                one_shot,
            )
            .await;

        watcher_handle.abort();
        client_handle.abort();

        // Persist the HLC floor advanced by the stamps this session emitted (and
        // remote stamps it observed), so the next process seeds its clock above
        // them. The other `save_state` call sites fire from `register_identity` /
        // `confirm` / `move_identity`, which all run BEFORE `make_metadata` ticks
        // the clock — so without this, a one-shot `kutl sync` left the persisted
        // floor at its pre-session value (`{0,0}` on first run), and the next
        // sync stamped its offline delete/rename at zero and lost arbitration at
        // the relay. See `2026-06-05-offline-delete-rename-floor-design.md`.
        self.save_state();

        result
    }

    /// Core single-intake loop: pull ONE [`LoopInput`] at a time via
    /// [`Self::next_event`], run the pure [`DaemonCore::handle`] for the events the
    /// core covers, and drain the returned `Vec<Effect>` through
    /// [`Self::apply_effect`]. The unported edge paths (binary blobs, local
    /// inode/overwrite rename detection, and the lifecycle disk-reconcile cascade)
    /// stay on their imperative driver helpers until Phase 4's gamma cascade lands
    /// — they reach the loop as [`LoopInput::ImperativeFile`] /
    /// [`LoopInput::ImperativeSync`] so the single intake is preserved.
    ///
    /// In one-shot mode, exits after [`ONE_SHOT_IDLE_TIMEOUT`] of inactivity (no
    /// events received), indicating the initial sync exchange is complete.
    ///
    /// BACKPRESSURE: the outbound relay channel (`sync_cmd`) is UNBOUNDED, so a
    /// producing send never parks the loop — the loop is a pure consumer that
    /// hands work off without awaiting (the sans-IO discipline the redesign makes
    /// explicit; it closes the deadlock cycles). The only thing left to bound is
    /// the loop racing ahead of a slow relay, handled at INTAKE by the
    /// `file_event` gate in [`Self::next_event`]. Effect ordering within one event
    /// is preserved verbatim from the imperative path (suppress-before-fs-write;
    /// removal-half-before-write-half on a rename).
    async fn event_loop(
        &mut self,
        file_event_rx: &mut mpsc::Receiver<FileEvent>,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
        one_shot: bool,
    ) -> Result<SessionOutcome> {
        // The idle timer is only polled when `one_shot` is true (via the select
        // guard). In persistent mode it's never polled, so no system timer is
        // registered and the cost is just the struct on the stack.
        let idle_timeout = tokio::time::sleep(ONE_SHOT_IDLE_TIMEOUT);
        tokio::pin!(idle_timeout);

        // Refreshes the per-space gauges; the first tick fires immediately so
        // they register as soon as the session is up. Independent of the idle
        // timer — a metrics tick is not user activity and must not keep a
        // one-shot session alive.
        let mut metrics_tick = tokio::time::interval(METRICS_EMIT_INTERVAL);

        loop {
            let input = self
                .next_event(
                    file_event_rx,
                    sync_event_rx,
                    &mut metrics_tick,
                    idle_timeout.as_mut(),
                    one_shot,
                )
                .await;

            match input {
                LoopInput::Shutdown => {
                    // Graceful stop: persist any coalesced SaveState before exiting.
                    if self.state_dirty {
                        self.save_state();
                    }
                    return Ok(SessionOutcome::Shutdown);
                }
                LoopInput::Disconnected => {
                    info!("disconnected from relay");
                    if self.state_dirty {
                        self.save_state();
                    }
                    return Ok(SessionOutcome::Disconnected);
                }
                LoopInput::ChannelsClosed => break,
                LoopInput::Activity => {
                    if one_shot {
                        idle_timeout
                            .as_mut()
                            .reset(tokio::time::Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
                    }
                }
                LoopInput::Core { event, activity } => {
                    if one_shot && activity {
                        idle_timeout
                            .as_mut()
                            .reset(tokio::time::Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
                    }
                    // §1.3: surface an undrained concurrent LOCAL rename FIRST
                    // for any core event whose apply would destroy its evidence
                    // (see `drain_before_core_event`).
                    self.drain_before_core_event(&event, sync_cmd_tx, suppress_tx);
                    // PURE decision region: no IO, no clock read, no channel.
                    let effects = DaemonCore::handle(&mut self.state, event);
                    let mut errored = false;
                    for eff in effects {
                        let effect_name = eff.name();
                        if let Err(e) = self.apply_effect(eff, sync_cmd_tx, suppress_tx) {
                            error!(error = %e, effect = effect_name, "error applying effect");
                            crate::metrics_calls::record_error(
                                crate::metrics_calls::error_category::SYNC_EVENT,
                            );
                            errored = true;
                            break;
                        }
                    }
                    if !errored && activity {
                        self.last_progress = Instant::now();
                    }
                }
                LoopInput::ImperativeFile(file_event) => {
                    if one_shot {
                        idle_timeout
                            .as_mut()
                            .reset(tokio::time::Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
                    }
                    if let Err(e) = self.handle_file_event(file_event, sync_cmd_tx, suppress_tx) {
                        error!(error = %e, "error handling file event");
                        crate::metrics_calls::record_error(
                            crate::metrics_calls::error_category::FILE_EVENT,
                        );
                    } else {
                        self.last_progress = Instant::now();
                    }
                }
                LoopInput::ImperativeSync(sync_event) => {
                    if one_shot {
                        idle_timeout
                            .as_mut()
                            .reset(tokio::time::Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
                    }
                    if let Err(e) = self.handle_sync_event(sync_event, suppress_tx, sync_cmd_tx) {
                        error!(error = %e, "error handling sync event");
                        crate::metrics_calls::record_error(
                            crate::metrics_calls::error_category::SYNC_EVENT,
                        );
                    } else {
                        self.last_progress = Instant::now();
                    }
                }
            }
            // Coalesced persist: now that this event is fully handled, write a
            // pending `Effect::SaveState` ONLY if the intake is drained — so a bulk
            // materialization does one save when the burst ends, not one O(docs)
            // rewrite per doc (the N=1000 stage-1 regression).
            self.flush_state_if_caught_up(file_event_rx, sync_event_rx);
        }
        // The intake closed (ChannelsClosed): persist any coalesced state.
        if self.state_dirty {
            self.save_state();
        }
        Ok(SessionOutcome::Disconnected)
    }

    /// Pull ONE [`LoopInput`] from the merged intake (file events, sync events,
    /// the metrics tick, the one-shot idle timer, and cancellation), translating
    /// each wire event to the loop's input form.
    ///
    /// EDGE READS happen here, OFF the pure core: a `FileEvent::Modified` reads the
    /// file bytes (the `std::fs::read_to_string` that was at `daemon.rs:1721`); a
    /// text `RemoteOps` on a tracked doc ALSO reads the current on-disk text so the
    /// core can incorporate a pending local edit before merge (FS-5). The wire enum
    /// fields are renamed to the core's (`rel_path`→`rel`, `old_path`→`old`,
    /// `new_path`→`new`); `SyncEvent::Document*` map to `Remote*`;
    /// `AuthRejected`/`Error` map to `RelayError`; the timers map to
    /// `MetricsTick`/`IdleTimeout`.
    ///
    /// The select is biased toward a still-debouncing local file event;
    /// correctness never depends on cross-channel order (the cascade is confluent),
    /// only liveness does.
    ///
    /// INTAKE GATE: the `file_event` arm is disabled while the gate depth is at or
    /// over [`SYNC_BACKLOG_HIGH_WATER`]. With the outbound channels unbounded the
    /// loop can race ahead of a slow relay and grow the outbound queue without
    /// limit; throttling the FILE-EVENT intake (the source of new outbound work)
    /// bounds it. `sync_event`/ack consumption is NEVER gated — acks are exactly
    /// what lowers the backlog, so gating them would self-deadlock; with
    /// `file_event` paused the loop keeps draining acks until the backlog falls
    /// back under the mark and `file_event` re-enables.
    async fn next_event(
        &mut self,
        file_event_rx: &mut mpsc::Receiver<FileEvent>,
        sync_event_rx: &mut mpsc::Receiver<SyncEvent>,
        metrics_tick: &mut tokio::time::Interval,
        idle_timeout: std::pin::Pin<&mut tokio::time::Sleep>,
        one_shot: bool,
    ) -> LoopInput {
        // Process events buffered during startup FIRST, in arrival order, before
        // any live channel event — see `startup_buffer`. This both preserves
        // local-before-remote (the scan's local edits already applied) and lets
        // the scan keep the bounded `sync_event` channel drained so the WS read
        // loop never blocks on a full channel mid-startup.
        if let Some(event) = self.startup_buffer.pop_front() {
            return self.classify_sync_event(event);
        }
        let intake_open = self.intake.intake_open();
        tokio::select! {
            biased;
            () = self.config.cancel.cancelled() => LoopInput::Shutdown,
            Some(file_event) = file_event_rx.recv(), if intake_open => {
                self.classify_file_event(file_event)
            }
            Some(sync_event) = sync_event_rx.recv() => self.classify_sync_event(sync_event),
            _ = metrics_tick.tick() => LoopInput::Core {
                event: Event::MetricsTick { stamp: self.stamp(None) },
                activity: false,
            },
            () = idle_timeout, if one_shot => {
                info!("sync idle timeout, exiting");
                LoopInput::Shutdown
            }
            else => LoopInput::ChannelsClosed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::daemon::tests::test_worker;

    // --- intake-gate round-trip tests (PRODUCTION paths, not closures) ---
    //
    // The old tests replicated the increment/decrement arithmetic in local
    // closures, so a real leak (an ack path that skips the edge decrement)
    // passed the suite. These drive the production sites: `send_cmd` for the
    // increment, the `classify_sync_event` ack edge for the decrement.

    /// N ack-bearing sends through the PRODUCTION `send_cmd` followed by N
    /// acks through the PRODUCTION classify edge must return the gate to zero
    /// depth. A residue is a leak: the watchdog then false-alarms forever, and
    /// a leak in this direction latches the intake gate shut (the gamma
    /// bulk-move stall class). Deleting the classify-edge `ack()` makes this
    /// fail.
    #[test]
    fn test_intake_gate_round_trip_through_production_paths() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (sync_cmd_tx, _sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();

        // Three ack-bearing sends + one non-ack-bearing (Subscribe must not
        // count) through the production increment site.
        worker
            .send_cmd(
                &sync_cmd_tx,
                SyncCommand::RegisterDocument {
                    space_id: "s".into(),
                    document_id: "doc-a".into(),
                    path: "a.md".into(),
                    metadata: None,
                    originally_created_at_ms: None,
                },
            )
            .unwrap();
        worker
            .send_cmd(
                &sync_cmd_tx,
                SyncCommand::UnregisterDocument {
                    space_id: "s".into(),
                    document_id: "doc-b".into(),
                    metadata: None,
                },
            )
            .unwrap();
        worker
            .send_cmd(
                &sync_cmd_tx,
                SyncCommand::ListSpaceDocuments {
                    space_id: "s".into(),
                },
            )
            .unwrap();
        worker
            .send_cmd(
                &sync_cmd_tx,
                SyncCommand::Subscribe {
                    document_id: "doc-a".into(),
                },
            )
            .unwrap();
        assert_eq!(
            worker.intake.depth(),
            3,
            "three ack-bearing sends count; Subscribe does not"
        );

        // Three acks through the production decrement site (the classify edge;
        // a failed ack — no effective path — still clears its backlog entry).
        for _ in 0..3 {
            let _ = worker.classify_sync_event(SyncEvent::LifecycleAck {
                document_id: "doc-a".into(),
                effective_path: None,
                hlc: None,
            });
        }
        assert_eq!(
            worker.intake.depth(),
            0,
            "every ack-bearing command is balanced by exactly one ack"
        );

        // A duplicate/unexpected ack through the SAME production path stays at
        // zero (saturating) — no false all-clear for a later genuine stall.
        let _ = worker.classify_sync_event(SyncEvent::LifecycleAck {
            document_id: "doc-a".into(),
            effective_path: None,
            hlc: None,
        });
        assert_eq!(worker.intake.depth(), 0, "extra ack must not underflow");
    }

    /// The gate closes exactly at the high water and a single ack reopens it.
    #[test]
    fn test_intake_gate_closes_at_high_water_and_reopens_on_ack() {
        let mut gate = IntakeGate::default();
        let cmd = SyncCommand::ListSpaceDocuments {
            space_id: "s".into(),
        };
        for _ in 0..SYNC_BACKLOG_HIGH_WATER {
            gate.track(&cmd);
        }
        assert!(!gate.intake_open(), "gate closes at the high water");
        gate.ack();
        assert!(gate.intake_open(), "one ack reopens the gate");
    }

    // --- watchdog classifier tests ---

    /// A positive backlog that has not made progress for at least
    /// `STALE_PROGRESS_THRESHOLD` must classify as `Stalled` (→ ERROR log).
    #[test]
    fn test_classify_backlog_stalled_emits_error() {
        let h = classify_backlog(1, STALE_PROGRESS_THRESHOLD);
        assert_eq!(h, BacklogHealth::Stalled);
    }

    /// A deep backlog that is still within the stale threshold classifies as
    /// `DeepButMoving` (→ WARN log). Progress is moving; only the depth warrants
    /// attention.
    #[test]
    fn test_classify_backlog_deep_but_moving_warns() {
        let recent = STALE_PROGRESS_THRESHOLD
            .checked_sub(Duration::from_secs(1))
            .expect("threshold is 30s, so subtracting 1s cannot underflow");
        let h = classify_backlog(BACKLOG_WARN_DEPTH, recent);
        assert_eq!(h, BacklogHealth::DeepButMoving);
    }

    /// A backlog of 0 is always `Healthy` regardless of elapsed time. A backlog
    /// below `BACKLOG_WARN_DEPTH` with recent progress is also `Healthy`.
    #[test]
    fn test_classify_backlog_healthy_when_small_and_recent() {
        assert_eq!(classify_backlog(0, Duration::ZERO), BacklogHealth::Healthy);
        assert_eq!(
            classify_backlog(BACKLOG_WARN_DEPTH - 1, Duration::ZERO),
            BacklogHealth::Healthy
        );
    }

    /// `Stalled` outranks `DeepButMoving`: a backlog that is both deep AND stale
    /// is the real wedge and must escalate to ERROR, not stay at WARN.
    #[test]
    fn test_classify_backlog_stalled_outranks_deep() {
        let h = classify_backlog(BACKLOG_WARN_DEPTH + 5, STALE_PROGRESS_THRESHOLD);
        assert_eq!(h, BacklogHealth::Stalled);
    }
}
