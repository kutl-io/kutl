//! Session runtime for [`SpaceWorker`]: the reconnect-with-backoff outer loop
//! (`run`/`run_once`), per-session setup and teardown (`run_session`), and the
//! single-intake event loop (`event_loop`/`next_event`) — its [`LoopInput`]
//! work units, the [`IntakeGate`] file-event backpressure gate, and the
//! backlog-health watchdog classification.

use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use kutl_client::credentials;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::client::{self, SyncCommand, SyncEvent};
use crate::core::{DaemonCore, Effect, Event};
use crate::watcher::{FileEvent, FileWatcher, Suppression};

use super::SpaceWorker;

/// One unit of work pulled by [`SpaceWorker::next_event`] from the merged intake.
///
/// The single-intake loop turns each wire event into exactly one of these: a pure
/// core [`Event`] (drained through `handle` + `apply_effect`), an imperative
/// file/sync event served at the driver edge because it needs live disk the pure
/// core cannot do (binary blobs, local inode/overwrite rename detection), or a
/// control signal (shutdown/disconnect/closed/activity-only).
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
    /// A broadcast signal record to ingest into the space's local
    /// segments. Served at the driver edge — appending, cursor
    /// bookkeeping, and the single-writer `flock` are IO the pure core cannot
    /// do. Handled by [`SpaceWorker::ingest_signal_record`]. Boxed: a `Signal`
    /// dwarfs the other variants.
    SignalIngest(Box<kutl_proto::sync::Signal>),
    /// A backlog page answering a `SubscribeSignals`. Ingested
    /// record-by-record like a broadcast, then the walk continues if `more`.
    SignalPage(Box<kutl_proto::sync::SignalPage>),
    /// A `SignalAck`, correlated by `client_ref`. Releases the next chunk of a
    /// re-seed push in flight, or — when it matches nothing this worker is
    /// waiting on — is a submit outcome, whose failures are reported.
    SignalAck {
        client_ref: String,
        success: bool,
        error: Option<String>,
    },
    /// The pacing floor behind an armed placement debt elapsed — no core
    /// work of its own; the loop tail's drain-edge probe pays the debt.
    ReconcileDue,
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

/// The event loop's wake-up for paced placement debt: a timer plus whether
/// it is armed. Polled only while armed (the select guard), so an idle loop
/// registers no timer for it.
struct ReconcileWakeup<'a> {
    sleep: std::pin::Pin<&'a mut tokio::time::Sleep>,
    armed: bool,
}

impl ReconcileWakeup<'_> {
    /// Arm the timer for `due`, or disarm it when there is nothing to wake
    /// for (no debt, or debt the next drained edge pays anyway).
    fn arm(&mut self, due: Option<Instant>) {
        match due {
            Some(at) => {
                self.sleep.as_mut().reset(at);
                self.armed = true;
            }
            None => self.armed = false,
        }
    }
}

/// Outcome of a single session, used to decide whether to reconnect or exit.
enum SessionOutcome {
    /// Relay disconnected or channels closed — reconnect.
    Disconnected,
    /// Cancellation requested — shut down gracefully.
    Shutdown,
    /// The relay refused the handshake — do not retry. Covers a rejected
    /// token AND a protocol-version mismatch in either direction; all of them
    /// are terminal for this binary against this relay, and none improves by
    /// reconnecting.
    ///
    /// Already carries its remedy: the session composes one at the edge that
    /// knows which credential slot was presented, so both the one-shot and the
    /// persistent surface report an identical, actionable line.
    HandshakeRejected(String),
}

/// How a session reaches its relay.
pub(crate) enum SessionLink {
    /// The relay at `config.relay_url`: authenticate, then a WebSocket
    /// carried by [`client::run_client`].
    WebSocket,
    /// An in-process link a test drives. Each session hands its channel ends
    /// to the harness, which plays the relay's side of the wire one frame at
    /// a time, so the interleaving of daemon and relay steps is the test's
    /// to choose. No socket, no bearer, no client task.
    #[cfg(test)]
    InProcess(mpsc::UnboundedSender<super::interleave::SessionEnds>),
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
/// never blocks producing (a bounded outbound send from inside the loop is a
/// circular-wait deadlock — see the channel setup in `run_session`); this bounds the only
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

/// Push the one-shot idle deadline out, because the loop just did work.
///
/// Five event arms need this and each spelled it out, which is five places for
/// the deadline semantics to drift and five arms' worth of length in a loop
/// already at its line budget. A no-op in persistent mode, where there is no
/// deadline to bump.
fn bump_idle_deadline(one_shot: bool, idle_timeout: std::pin::Pin<&mut tokio::time::Sleep>) {
    if one_shot {
        idle_timeout.reset(Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
    }
}

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

/// Minimum spacing between paced full placement passes. One shared floor
/// behind both pacing decisions — the `intake_backlogged` flag (which the
/// core's per-event reconcile tails obey) and the drain-edge probe — via
/// `last_reconcile_pass`, stamped when a pacing window closes.
///
/// `reconcile_placement` projects the FULL registry (`desired_assignment`
/// is O(records)), and the drained-burst test alone cannot see a network
/// TRICKLE: a peer's bulk operation arriving op by op leaves the intake
/// empty after every event, so every event ran the O(records) pass — the
/// receiver-side c·N term reintroduced on any real network (the loopback
/// bench never sees it because its intake stays backlogged). One pass per
/// interval bounds it; debt armed within the floor is paid by the next
/// open window's tail, the drain-edge probe, or at latest the ungated
/// metrics-tick pass (the idle fixpoint driver). The same trickle shape
/// motivated the state-save floor (`MIN_STATE_SAVE_INTERVAL`).
///
/// Sized to the watcher's batch window: a document a peer just created is
/// placed at most one such window after its register arrives, the same
/// delay its own machine's watcher added before shipping it, and for a new
/// document that placement is when its bytes start moving (it is not
/// subscribed until placed, so the live relay passes it by). A full-rate
/// trickle in a large space then runs at most one pass per window; the
/// pass duration is logged at debug so that bound can be read off a run.
pub(super) const RECONCILE_PASS_MIN_INTERVAL: Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_millis(200));

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
/// The pure core knows nothing of this counter (a driver concern; the core is
/// sans-IO). Splitting the two halves across layers is exactly the
/// bulk-move-stall / intake-gate-latch deadlock class: an ack path that
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
                Ok(SessionOutcome::HandshakeRejected(msg)) => {
                    return Err(anyhow::anyhow!("{msg}"));
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
    ///
    /// Fails LOUDLY when the pass cannot honestly claim success: the relay
    /// rejected operations, commands or blob uploads were still undelivered at
    /// the idle exit, or the relay disconnected mid-sync. Local state is
    /// preserved in every case and the next sync retries — but returning `Ok`
    /// over any of them (and letting the CLI print a clean completion) is how
    /// a wedged space stays silently stale.
    async fn run_once(&mut self) -> Result<()> {
        // One-shot mode never reconnects, so the gauge transition isn't
        // observable; we still pass a sink to keep the signature uniform.
        let mut was_connected = false;
        match self.run_session(true, &mut was_connected).await? {
            SessionOutcome::Shutdown => {}
            SessionOutcome::Disconnected => {
                anyhow::bail!(
                    "sync incomplete: the relay disconnected before the sync finished; \
                     local changes are preserved and will retry on the next sync"
                );
            }
            SessionOutcome::HandshakeRejected(msg) => {
                anyhow::bail!("{msg}");
            }
        }
        if let Some(reason) = self.one_shot_incomplete() {
            anyhow::bail!(
                "sync incomplete: {reason}; local changes are preserved and will \
                 retry on the next sync"
            );
        }
        info!("sync complete");
        Ok(())
    }

    /// Why the finished one-shot pass cannot claim success — `None` when it can.
    ///
    /// Checked after the idle exit, most-diagnostic first: relay rejections
    /// name their cause (e.g. a quota refusal), while undelivered commands or
    /// blob uploads only say the relay stopped answering before the backlog
    /// drained.
    fn one_shot_incomplete(&self) -> Option<String> {
        if self.relay_errors > 0 {
            let last = self.last_relay_error.as_deref().unwrap_or("unknown error");
            return Some(format!(
                "the relay rejected {} operation(s) (last error: {last})",
                self.relay_errors
            ));
        }
        let unacked = self.intake.depth();
        if unacked > 0 {
            return Some(format!(
                "{unacked} command(s) were still awaiting relay acknowledgment at exit"
            ));
        }
        let blobs = self.blob_backlog.load(Ordering::Relaxed);
        if blobs > 0 {
            return Some(format!("{blobs} blob upload(s) were still queued at exit"));
        }
        None
    }

    /// Resolve a bearer token from stored credentials or DID challenge-response,
    /// reporting which credential slot supplied it.
    ///
    /// The chain itself lives in `kutl_client::credentials` and is shared with
    /// the one-shot CLI paths, so both reach the same verdict about the same
    /// slots — including its rule that a stored token minted by a DIFFERENT
    /// relay does not apply, which keeps a stale credential from masking a
    /// working local identity. A `None` source means no stored token applied and
    /// the bearer was minted here from the machine's did:key identity.
    ///
    /// Fails LOUDLY rather than returning an empty token: under mandatory relay
    /// auth a tokenless handshake is rejected relay-side with an
    /// opaque error, so a mis-provisioned daemon would otherwise connect-but-
    /// never-sync silently. Both the no-signing-identity case and a
    /// challenge-response failure return an actionable [`Err`]; the caller
    /// surfaces it at `error` level and backs off/retries (the identity may
    /// appear, or the relay may recover).
    async fn resolve_auth_token(&mut self) -> Result<String> {
        let creds_path = credentials::default_credentials_path().ok();
        if let Some((token, source)) =
            credentials::resolve_token_for(&self.config.relay_url, creds_path.as_deref())
        {
            info!(?source, "using stored token");
            return Ok(token);
        }

        // No stored token applies: did:key challenge-response against the relay.
        let Some(signing_key) = self.config.signing_key.as_ref() else {
            anyhow::bail!(
                "cannot authenticate to the relay: no signing identity found (run `kutl init`)"
            );
        };
        kutl_client::authenticate(&self.config.relay_url, &self.config.author_did, signing_key)
            .await
            .map_err(|e| anyhow::anyhow!("authentication to the relay failed: {e}"))
    }

    /// Resolve this session's bearer, surfacing a resolve failure before it
    /// propagates.
    ///
    /// A resolve failure is not a transient socket hiccup — it is a
    /// provisioning/identity problem the operator must fix. Surface it at
    /// `error` with the remedy, then propagate so the outer loop backs off
    /// and retries (the identity may appear / the relay may recover) rather
    /// than connecting tokenless and silently never syncing.
    async fn session_auth_token(&mut self) -> Result<String> {
        match self.resolve_auth_token().await {
            Ok(token) => Ok(token),
            Err(e) => {
                error!(
                    space_id = %self.config.space_id,
                    relay_url = %self.config.relay_url,
                    error = %e,
                    "relay authentication could not be established; not connecting tokenless"
                );
                Err(e)
            }
        }
    }

    /// Open this session's link to the relay over the channel ends the loop
    /// will use: authenticate and spawn the WebSocket client, whose task
    /// handle is returned for teardown, or hand the ends to the in-process
    /// harness, which needs no task.
    async fn open_session_link(
        &mut self,
        sync_cmd_rx: mpsc::UnboundedReceiver<SyncCommand>,
        sync_event_tx: mpsc::Sender<SyncEvent>,
    ) -> Result<Option<tokio::task::JoinHandle<()>>> {
        match &self.link {
            SessionLink::WebSocket => {
                let auth_token = self.session_auth_token().await?;
                let relay_url = self.config.relay_url.clone();
                let space_id = self.config.space_id.clone();
                let client_name = self.config.author_did.clone();
                let display_name = self.config.display_name.clone();
                let blob_backlog = self.blob_backlog.clone();
                Ok(Some(tokio::spawn(async move {
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
                        crate::metrics_calls::record_error(
                            crate::metrics_calls::error_category::RELAY,
                        );
                    }
                })))
            }
            #[cfg(test)]
            SessionLink::InProcess(harness) => {
                harness
                    .send(super::interleave::SessionEnds {
                        cmd_rx: sync_cmd_rx,
                        event_tx: sync_event_tx,
                        blob_backlog: self.blob_backlog.clone(),
                    })
                    .map_err(|_| anyhow::anyhow!("the in-process relay harness is gone"))?;
                Ok(None)
            }
        }
    }

    /// The user-facing line for a handshake the relay refused.
    ///
    /// `auth_failed` is the relay's own verdict, read from its `ErrorCode` — the
    /// message text is never parsed. On an auth refusal the line names the
    /// credential slot that supplied the bearer and the command that changes it;
    /// a refusal the reader cannot act on is the failure mode, and the relay's
    /// own "token not found" names neither what was sent nor how to clear it.
    /// Every other refusal (a protocol major neither side can bridge) already
    /// states its own remedy and is passed through.
    ///
    /// Shared by the connect gate and both mid-loop surfaces so a refusal reads
    /// the same wherever it is observed.
    pub(super) fn handshake_rejection_detail(&self, message: &str, auth_failed: bool) -> String {
        if auth_failed {
            credentials::refused_token_remedy(&self.config.relay_url)
        } else {
            format!("relay refused the connection: {message}")
        }
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
        // A fresh session is itself activity, and the previous session's
        // in-flight blob sends went away with its dropped channel — reset both
        // metric sources so the gauges reflect this session, not a stale one.
        self.last_progress = Instant::now();
        self.blob_backlog.store(0, Ordering::Relaxed);
        // A fresh session starts with no outstanding commands; the previous
        // session's in-flight commands went away with its dropped channel.
        self.intake.reset();
        // Events a dead session parked for its loop belong to that
        // connection. This session re-lists and re-subscribes: the snapshot
        // subsumes any lifecycle frame the old one had not applied, and a
        // signal record it had not ingested is behind the persisted cursor
        // (ingest is what advances it), so the new walk re-serves it from a
        // relay that keeps records; a relay without a record log never held
        // it to re-serve.
        self.startup_buffer.clear();
        // And no relay rejections yet — the tally is per-session (it feeds the
        // one-shot completion check, which judges only the pass it ran).
        self.relay_errors = 0;
        self.last_relay_error = None;
        // A records-outage retry chain belongs to the session that armed it.
        self.signal_records_retry = None;
        if let Some(task) = self.signal_records_retry_task.take() {
            task.abort();
        }

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

        let client_handle = self.open_session_link(sync_cmd_rx, sync_event_tx).await?;

        // Wait for connection before subscribing.
        match sync_event_rx.recv().await {
            Some(SyncEvent::Connected {
                relay_did,
                accepts_reseed,
            }) => {
                info!("connected to relay");
                *was_connected = true;
                // Pin the relay's advertised signing identity for this session
                // so ingested signal-record attestations can be checked against
                // it (advisory — never rejects). Empty means the relay
                // advertised no identity (graceful degrade).
                self.pinned_relay_did = relay_did;
                // …and reconcile it against the DURABLE record.
                // A pin the connection itself establishes is no pin at all, so
                // the set that gates MATERIALIZER trust comes from disk.
                // Advisory: a failure to read or record leaves the trust set
                // empty, which downgrades materialized records to
                // `unbound-materializer` rather than taking down the session.
                self.reconcile_known_relay().await;
                // Whether the PUSH half of catch-up runs this session.
                // Held on the worker rather than passed down, because
                // `catch_up_signals` is also reached from the test seam.
                self.relay_accepts_reseed = accepts_reseed;
                crate::metrics_calls::record_relay_connected(&self.config.relay_url, true);
            }
            Some(SyncEvent::HandshakeRejected {
                message,
                auth_failed,
            }) => {
                return Ok(SessionOutcome::HandshakeRejected(
                    self.handshake_rejection_detail(&message, auth_failed),
                ));
            }
            other => {
                anyhow::bail!("expected Connected event, got: {other:?}");
            }
        }

        // Listen to the space FIRST. `SubscribeSignals` is what makes this
        // daemon present in the space and enrols it in lifecycle broadcasts,
        // so it goes out before the snapshot: a peer's register or rename the
        // relay processes after the subscribe is delivered, and one it
        // processed before is in the snapshot. Its answers (the first backlog
        // page, or a stale-stream notice) may arrive while discovery drains
        // for its own reply; discovery parks them for the loop. Unconditional:
        // a relay without a record log answers with an empty page, a local
        // store that will not open costs only the catch-up floor, and
        // listening is presence whether or not there is a backlog to walk.
        self.start_signal_catch_up(&sync_cmd_tx);

        // Discover remote documents, reconcile with local state, and execute.
        // MUST complete before the watcher starts: the offline-rename pre-pass
        // (`reconcile_offline_renames`) moves and removes files with bare fs
        // calls and relies on no watcher running; the truth-table actions
        // register their echo suppressions, which the watcher consumes once
        // it starts.
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
        let mut watcher = FileWatcher::new(
            &self.config.space_root,
            file_event_tx,
            suppress_rx,
            self.config.poll_watcher,
        )?;
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
        // Identities minted by the scan are journaled before the loop starts —
        // a crash during a long first sync must not forget them.
        self.drain_identity_journal();

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
        if let Some(client_handle) = client_handle {
            client_handle.abort();
        }

        // Persist the HLC floor advanced by the stamps this session emitted (and
        // remote stamps it observed), so the next process seeds its clock above
        // them. The identity paths (`register_identity` / `move_identity` /
        // `unregister_identity`) only MARK state dirty for the coalesced flush,
        // and any flush that ran fired BEFORE `make_metadata` ticked the clock —
        // so without this save, a one-shot `kutl sync` would leave the persisted
        // floor at its pre-session value (`{0,0}` on first run), and the next
        // sync would stamp its offline delete/rename at zero and lose
        // arbitration at the relay.
        self.save_state();

        result
    }

    /// Core single-intake loop: pull ONE [`LoopInput`] at a time via
    /// [`Self::next_event`], run the pure [`DaemonCore::handle`] for the events the
    /// core covers, and drain the returned `Vec<Effect>` through
    /// [`Self::apply_effect`]. The two driver-edge paths (binary blobs and
    /// local inode/overwrite rename detection) stay on their imperative driver
    /// helpers because they need live disk the pure core cannot touch — they
    /// reach the loop as [`LoopInput::ImperativeFile`] /
    /// [`LoopInput::ImperativeSync`] so the single intake is preserved.
    ///
    /// In one-shot mode, exits after [`ONE_SHOT_IDLE_TIMEOUT`] of inactivity (no
    /// events received), indicating the initial sync exchange is complete.
    ///
    /// BACKPRESSURE: the outbound relay channel (`sync_cmd`) is UNBOUNDED, so a
    /// producing send never parks the loop — the loop is a pure consumer that
    /// hands work off without awaiting (the sans-IO discipline;
    /// it closes the deadlock cycles). The only thing left to bound is
    /// the loop racing ahead of a slow relay, handled at INTAKE by the
    /// `file_event` gate in [`Self::next_event`]. Effect ordering within one
    /// event is fixed: suppress-before-fs-write;
    /// removal-half-before-write-half on a rename.
    /// Reconcile the relay identity this session was handed against the durable
    /// record at `$KUTL_HOME/known_relays.toml`, and adopt the resulting trust
    /// set.
    ///
    /// Advisory throughout — the outcome is logged and the set adopted, never
    /// enforced. A rotation gets a `warn` because a relay identity changing is
    /// exactly what a substituted relay looks like, but the connection is not
    /// refused: document sync does not depend on this identity, and taking it
    /// down over a signing-key change would be a wildly disproportionate
    /// failure. An error reading or writing the record leaves the trust set
    /// empty, which reads materialized records as `unbound-materializer` — the
    /// safe direction, since it withholds trust rather than granting it.
    async fn reconcile_known_relay(&mut self) {
        use kutl_client::RelayPinOutcome;

        let relay_url = self.config.relay_url.clone();
        let relay_did = self.pinned_relay_did.clone();
        let path = self.config.known_relays_path.clone();
        // OFF the async runtime. `observe` takes a blocking exclusive `flock`
        // and does synchronous file I/O; run inline it parks a tokio worker
        // thread while it waits, and a process running many workers can park
        // enough of them to stall the runtime.
        let observed = tokio::task::spawn_blocking({
            let relay_url = relay_url.clone();
            move || kutl_client::KnownRelays::observe_at(path.as_deref(), &relay_url, &relay_did)
        })
        .await;
        let observed = match observed {
            Ok(result) => result,
            Err(e) => {
                warn!(%relay_url, error = %e, "relay identity reconcile task failed");
                self.known_relay_dids.clear();
                return;
            }
        };
        let relay_url = &relay_url;
        match observed {
            Ok((trusted, outcome)) => {
                match &outcome {
                    RelayPinOutcome::NoIdentity => {
                        debug!(%relay_url, "relay advertises no signing identity");
                    }
                    RelayPinOutcome::FirstUse => {
                        info!(
                            %relay_url,
                            relay_did = %self.pinned_relay_did,
                            "recorded this relay's signing identity on first use"
                        );
                    }
                    RelayPinOutcome::Known => {}
                    RelayPinOutcome::Rotated { previous } => {
                        warn!(
                            %relay_url,
                            relay_did = %self.pinned_relay_did,
                            ?previous,
                            "RELAY SIGNING IDENTITY CHANGED — recorded as a rotation. \
                             If you did not rotate this relay's key, its records are \
                             being signed by something else"
                        );
                    }
                }
                self.known_relay_dids = trusted;
            }
            Err(e) => {
                warn!(
                    %relay_url,
                    error = %e,
                    "could not reconcile the relay identity record; \
                     materialized records will read as unverified this session"
                );
                self.known_relay_dids.clear();
            }
        }
    }

    /// Ingest one broadcast record, logging and counting a failure rather than
    /// propagating it — a bad record must not tear down the session.
    fn ingest_one_signal(&mut self, record: &kutl_proto::sync::Signal) {
        if let Err(e) = self.ingest_signal_record(record) {
            error!(error = %e, "error ingesting signal record");
            crate::metrics_calls::record_error(crate::metrics_calls::error_category::SYNC_EVENT);
        } else {
            self.last_progress = Instant::now();
        }
    }

    /// Ingest one backlog page, then continue the walk or finish it.
    ///
    /// The cursor carried forward is the RELAY's high-water for the page it
    /// just sent, never re-derived from the last record: a page is `max`
    /// records PLUS the whole trailing group sharing its `physical_ms`, so
    /// re-deriving would land mid-millisecond and lose the remainder on resume.
    /// That boundary rule is the only thing standing between a resume and a
    /// permanent gap — ingest is idempotent and the fold is order-independent,
    /// so every other way a page can go wrong costs work, not correctness.
    fn handle_signal_page(
        &mut self,
        page: &kutl_proto::sync::SignalPage,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
    ) {
        // ABORT the page on the first failed ingest, and do NOT walk on. The
        // cursor's monotonic guard cannot cover this case: record N failing and
        // record N+1 succeeding advances the cursor legitimately FORWARD past N,
        // so N is permanently skipped without anything having regressed. Leaving
        // the cursor where it was means the next re-subscribe re-serves this
        // page; the whole ingest path is idempotent (dedup on `record_id`), so
        // the only cost of the retry is the work.
        let mut ingested = 0_usize;
        for record in &page.records {
            if let Err(e) = self.ingest_signal_record(record) {
                error!(error = %e, "error ingesting a signal backlog record; abandoning the page");
                crate::metrics_calls::record_error(
                    crate::metrics_calls::error_category::SYNC_EVENT,
                );
                if ingested > 0 {
                    self.last_progress = Instant::now();
                }
                return;
            }
            ingested += 1;
        }
        if ingested > 0 {
            self.last_progress = Instant::now();
        }
        if page.more {
            let _ = sync_cmd_tx.send(SyncCommand::SubscribeSignals {
                space_id: self.config.space_id.clone(),
                cursor: page.cursor.clone(),
            });
        } else if let Err(e) = self.reseed_after_catch_up(sync_cmd_tx, page.cursor.as_ref()) {
            // Advisory, as the inline pull's push half was: the pull succeeded,
            // and a failed push retries on the next reconnect.
            warn!(error = %e, "signal re-seed after catch-up failed (advisory)");
        }
    }

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

        // Wakes the loop when the pacing floor behind an armed placement
        // debt elapses, so a trickled event's placement is paid one floor
        // interval later at the latest, never left to the metrics tick.
        let reconcile_sleep = tokio::time::sleep(RECONCILE_PASS_MIN_INTERVAL);
        tokio::pin!(reconcile_sleep);
        let mut reconcile_wakeup = ReconcileWakeup {
            sleep: reconcile_sleep,
            armed: false,
        };

        loop {
            let input = self
                .next_event(
                    file_event_rx,
                    sync_event_rx,
                    &mut metrics_tick,
                    idle_timeout.as_mut(),
                    &mut reconcile_wakeup,
                    one_shot,
                )
                .await;

            // Intake-burst signal, set BEFORE dispatch so every core arm sees
            // it: while more events are already queued, the core's per-event
            // reconcile tails return empty and the full placement recompute
            // runs once when the burst drains — the reconcile twin of the
            // coalesced `SaveState` flush below (same drained-intake test).
            // The pacing floor rides the same flag: a network trickle leaves
            // the intake empty after EVERY event, so drained-alone would run
            // the O(records) tail per event (see
            // [`RECONCILE_PASS_MIN_INTERVAL`]). A genuine backlog is
            // remembered so the drain-edge probe can bypass the floor for
            // burst debt.
            let drained = self.intake_drained(file_event_rx, sync_event_rx);
            if !drained {
                self.burst_seen_since_pass = true;
            }
            self.state.intake_backlogged = !drained || !self.reconcile_floor_elapsed();

            match input {
                // The tail below pays the debt and re-arms or disarms the timer.
                LoopInput::ReconcileDue => {}
                LoopInput::Shutdown => return Ok(SessionOutcome::Shutdown),
                LoopInput::Disconnected => {
                    info!("disconnected from relay");
                    return Ok(SessionOutcome::Disconnected);
                }
                LoopInput::ChannelsClosed => break,
                LoopInput::Activity => {
                    bump_idle_deadline(one_shot, idle_timeout.as_mut());
                }
                LoopInput::Core { event, activity } => {
                    if one_shot && activity {
                        idle_timeout
                            .as_mut()
                            .reset(Instant::now() + ONE_SHOT_IDLE_TIMEOUT);
                    }
                    // The metrics tick doubles as the deferred-empty revisit
                    // heartbeat: a file that really was emptied raises no
                    // further event, so its second observation has to ride a
                    // timer this loop already owns.
                    if matches!(event, Event::MetricsTick { .. }) {
                        self.revisit_pending_empties(sync_cmd_tx, suppress_tx);
                    }
                    self.apply_core_input(event, activity, sync_cmd_tx, suppress_tx);
                }
                LoopInput::ImperativeFile(file_event) => {
                    bump_idle_deadline(one_shot, idle_timeout.as_mut());
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
                    bump_idle_deadline(one_shot, idle_timeout.as_mut());
                    if let Err(e) = self.handle_sync_event(sync_event, suppress_tx, sync_cmd_tx) {
                        error!(error = %e, "error handling sync event");
                        crate::metrics_calls::record_error(
                            crate::metrics_calls::error_category::SYNC_EVENT,
                        );
                    } else {
                        self.last_progress = Instant::now();
                    }
                }
                LoopInput::SignalPage(page) => {
                    bump_idle_deadline(one_shot, idle_timeout.as_mut());
                    self.handle_signal_page(&page, sync_cmd_tx);
                }
                LoopInput::SignalAck {
                    client_ref,
                    success,
                    error,
                } => {
                    // An ack is WORK: it advances (or refuses) the chunked
                    // re-seed walk, and each one grants a fresh idle window so
                    // the walk's length never has to fit inside one.
                    bump_idle_deadline(one_shot, idle_timeout.as_mut());
                    self.route_signal_ack(&client_ref, success, error.as_deref(), sync_cmd_tx);
                }
                LoopInput::SignalIngest(record) => {
                    bump_idle_deadline(one_shot, idle_timeout.as_mut());
                    // `record` is a `Box<Signal>`; `&record` derefs to `&Signal`.
                    self.ingest_one_signal(&record);
                }
            }
            self.loop_tail(file_event_rx, sync_event_rx, sync_cmd_tx, suppress_tx);
            reconcile_wakeup.arm(self.reconcile_wakeup_at(file_event_rx, sync_event_rx));
        }
        // The intake closed (ChannelsClosed): the same exit as every other.
        // Every exit lands in `run_session`'s unconditional save; an armed
        // placement debt is not flushed here, the next session's first
        // metrics tick runs the ungated pass over the same persisted state.
        Ok(SessionOutcome::Disconnected)
    }

    /// Handle a [`LoopInput::Core`] work unit: surface an undrained concurrent
    /// local op (rename, delete or edit) first, run the PURE [`DaemonCore::handle`] decision,
    /// then drain the returned effects through [`Self::apply_effect`]. Records
    /// sync progress on `activity` unless an effect errored. Extracted from
    /// [`Self::event_loop`] so the loop's match stays legible.
    pub(super) fn apply_core_input(
        &mut self,
        event: Event,
        activity: bool,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) {
        // Surface an undrained concurrent LOCAL op (rename, delete or edit)
        // FIRST for any core event whose apply would destroy its evidence
        // (`drain_before_core_event`).
        self.drain_before_core_event(&event, sync_cmd_tx, suppress_tx);
        // PURE decision region: no IO, no clock read, no channel.
        let effects = DaemonCore::handle(&mut self.state, event);
        let errored = self.apply_effects(effects, sync_cmd_tx, suppress_tx);
        if !errored && activity {
            self.last_progress = Instant::now();
        }
    }

    /// Drain a core decision's effects through [`Self::apply_effect`], logging
    /// each failure and applying the rest. Returns whether any failed.
    ///
    /// Every effect in a batch is applied even after one fails: the core
    /// emits them as independent consequences of one decision, and stopping
    /// early would drop a `SaveDoc` queued behind a `SendOps` whose channel
    /// has closed, leaving the sidecar behind the CRDT with the recorded
    /// disk hash still claiming the file is known, which a restart's restore
    /// branch would then overwrite from the stale sidecar.
    fn apply_effects(
        &mut self,
        effects: Vec<Effect>,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) -> bool {
        let mut failed = false;
        for eff in effects {
            let effect_name = eff.name();
            if let Err(e) = self.apply_effect(eff, sync_cmd_tx, suppress_tx) {
                error!(error = %e, effect = effect_name, "error applying effect");
                crate::metrics_calls::record_error(
                    crate::metrics_calls::error_category::SYNC_EVENT,
                );
                failed = true;
            }
        }
        failed
    }

    /// The loop tail every input runs, in a load-bearing order: reconcile
    /// first (a skipped-gate placement pass on the drained edge mutates
    /// identities), then journal, then flush. Draining BEFORE the flush
    /// means every delta this input produced, the reconcile pass's included,
    /// is on disk before the save attempt, and a drain that trips the
    /// force-save line budget is honored by the flush in the SAME iteration.
    /// A successful save then truncates the journal, so the drained edge
    /// ends clean: fresh snapshot, empty journal.
    fn loop_tail(
        &mut self,
        file_event_rx: &mpsc::Receiver<FileEvent>,
        sync_event_rx: &mpsc::Receiver<SyncEvent>,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) {
        self.reconcile_if_caught_up(file_event_rx, sync_event_rx, sync_cmd_tx, suppress_tx);
        // Close the pacing window AFTER the probe: this iteration was
        // permitted to run a reconcile pass (a core tail, or the probe
        // above, which stamped for itself already), so the next window
        // opens a full interval from now. Stamping before the probe would
        // make the probe read its OWN iteration's stamp and skip the
        // drain-edge pass. A permitted window also ends any remembered
        // burst: the probe just had an unblocked shot at the debt.
        if !self.state.intake_backlogged {
            self.last_reconcile_pass = Some(Instant::now());
            self.burst_seen_since_pass = false;
        }
        // Crash durability for whatever this input changed: journal the
        // pending identity deltas (O(changes), usually empty). The
        // core-routed mutations (remote materializations, acks) have no
        // inline drain of their own.
        self.drain_identity_journal();
        // Coalesced persist: write a pending `Effect::SaveState` ONLY if the
        // intake is drained, so a bulk materialization does one save when
        // the burst ends, not one O(docs) rewrite per doc.
        self.flush_state_if_caught_up(file_event_rx, sync_event_rx);
    }

    /// When the loop must wake to pay an armed placement debt: `None` with
    /// no debt, or with debt this loop cannot pay yet; now, for debt that
    /// bypasses the floor (a capped pass's remainder, burst debt), so the
    /// next iteration pays it even when no further input arrives; otherwise
    /// the end of the current floor window.
    ///
    /// Agrees with [`Self::reconcile_if_caught_up`], the probe that pays: an
    /// undrained intake is debt the probe refuses, so no wakeup is armed for
    /// it. Arming one anyway sets a deadline that is already elapsed and that
    /// every refused iteration re-arms, so the loop spins through no-op
    /// wakeups for as long as the intake stays undrained: the steady state of
    /// a bulk burst, where the file-event intake gate latches shut on queued
    /// events until the relay's acks reopen it. That input is the wakeup
    /// instead: an ack, a queued file event, or a buffered startup event each
    /// runs the loop tail and re-arms here.
    fn reconcile_wakeup_at(
        &self,
        file_event_rx: &mpsc::Receiver<FileEvent>,
        sync_event_rx: &mpsc::Receiver<SyncEvent>,
    ) -> Option<Instant> {
        if !self.state.placement_dirty || !self.intake_drained(file_event_rx, sync_event_rx) {
            return None;
        }
        if self.burst_seen_since_pass || self.state.placement_truncated {
            return Some(Instant::now());
        }
        let last = self.last_reconcile_pass?;
        Some(last + RECONCILE_PASS_MIN_INTERVAL)
    }

    /// Every queued-but-undispatched input drained — the loop is caught up.
    /// The ONE definition of "drained" behind every burst-coalescing decision
    /// (the `intake_backlogged` flag, the coalesced state flush, the
    /// drain-edge reconcile), so the gates can never disagree about when a
    /// burst has ended. The startup buffer counts as intake: events parked
    /// there during the initial scan are dispatched before any live channel
    /// event, so a non-empty buffer means the loop is still mid-burst even
    /// while the channels themselves sit empty — a buffer-blind test made
    /// the gates fire (and pay O(docs) work) once per buffered event.
    pub(super) fn intake_drained(
        &self,
        file_event_rx: &mpsc::Receiver<FileEvent>,
        sync_event_rx: &mpsc::Receiver<SyncEvent>,
    ) -> bool {
        self.startup_buffer.is_empty() && file_event_rx.is_empty() && sync_event_rx.is_empty()
    }

    /// Whether the pacing floor has elapsed — the ONE clock read behind the
    /// `intake_backlogged` computation and the drain-edge probe, so the two
    /// pacing decisions cannot drift apart.
    fn reconcile_floor_elapsed(&self) -> bool {
        self.last_reconcile_pass
            .is_none_or(|last| last.elapsed() >= RECONCILE_PASS_MIN_INTERVAL)
    }

    /// Drain-edge reconcile: run one placement pass the moment the intake
    /// empties while a skipped-gate debt is armed (`placement_dirty`). The
    /// per-event reconcile tails are gated on a drained intake, and a burst's
    /// TRAILING inputs are often tail-less (suppression echoes, imperative
    /// acks) or see one another still queued — so the ack-redriven cascade can
    /// end a burst with deferred placements and no tail left to run them,
    /// stranding the stragglers until the 10s metrics tick (measured: the
    /// final ~2% of a bulk add waited ~7s on the tick). Runs after every loop
    /// input, next to the coalesced-save flush it mirrors; the dirty flag
    /// clears on every actual pass, so a drained burst pays exactly one.
    ///
    /// Shares the pacing floor with the `intake_backlogged` computation
    /// (see [`RECONCILE_PASS_MIN_INTERVAL`]): a network trickle leaves the
    /// intake empty after every event, and without the floor this probe
    /// would run the O(records) pass per trickled event — the exact
    /// per-event cost the burst gate exists to avoid. BURST debt bypasses
    /// the floor (`burst_seen_since_pass`): a chunked bulk cascade drains
    /// between ack floods faster than the floor, and pacing those edges
    /// would stall every chunk on the metrics tick. The floor check runs
    /// BEFORE the loop tail's window-close stamp, so a trickle edge whose
    /// floor has elapsed also pays immediately.
    fn reconcile_if_caught_up(
        &mut self,
        file_event_rx: &mpsc::Receiver<FileEvent>,
        sync_event_rx: &mpsc::Receiver<SyncEvent>,
        sync_cmd_tx: &mpsc::UnboundedSender<SyncCommand>,
        suppress_tx: &mpsc::UnboundedSender<Suppression>,
    ) {
        if !self.state.placement_dirty
            || !self.intake_drained(file_event_rx, sync_event_rx)
            || !(self.reconcile_floor_elapsed()
                || self.burst_seen_since_pass
                || self.state.placement_truncated)
        {
            return;
        }
        let started = Instant::now();
        self.last_reconcile_pass = Some(started);
        self.burst_seen_since_pass = false;
        self.state.intake_backlogged = false;
        let effects = crate::core::reconcile_placement(&mut self.state);
        debug!(
            records = self.state.known_records.records().count(),
            elapsed_us = started.elapsed().as_micros(),
            "placement pass"
        );
        if self.apply_effects(effects, sync_cmd_tx, suppress_tx) {
            // A place that failed to land left the shadow unchanged, so the
            // doc is still a mover; keep the debt armed so the next window
            // retries it rather than waiting for the metrics tick.
            self.state.placement_dirty = true;
        }
    }

    /// Pull ONE [`LoopInput`] from the merged intake (file events, sync events,
    /// the metrics tick, the one-shot idle timer, and cancellation), translating
    /// each wire event to the loop's input form.
    ///
    /// EDGE READS happen here, OFF the pure core: a `FileEvent::Modified` reads the
    /// file bytes; a
    /// text `RemoteOps` on a tracked doc ALSO reads the current on-disk text so the
    /// core can incorporate a pending local edit before merge. The wire enum
    /// fields are renamed to the core's (`rel_path`→`rel`, `old_path`→`old`,
    /// `new_path`→`new`); `SyncEvent::Document*` map to `Remote*`;
    /// `HandshakeRejected`/`Error` map to `RelayError`; the timers map to
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
        reconcile_wakeup: &mut ReconcileWakeup<'_>,
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
            () = reconcile_wakeup.sleep.as_mut(), if reconcile_wakeup.armed => {
                LoopInput::ReconcileDue
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
    // Tests that replicate the increment/decrement arithmetic in local
    // closures would let a real leak (an ack path that skips the edge
    // decrement) pass the suite. These drive the production sites: `send_cmd`
    // for the increment, the `classify_sync_event` ack edge for the decrement.

    /// N ack-bearing sends through the PRODUCTION `send_cmd` followed by N
    /// acks through the PRODUCTION classify edge must return the gate to zero
    /// depth. A residue is a leak: the watchdog then false-alarms forever, and
    /// a leak in this direction latches the intake gate shut (the
    /// bulk-move-stall class). Deleting the classify-edge `ack()` makes this
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
                    space_id: "043a7187-74c5-42bd-8a25-adbeb1bfcd5c".into(),
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
                    space_id: "043a7187-74c5-42bd-8a25-adbeb1bfcd5c".into(),
                    document_id: "doc-b".into(),
                    metadata: None,
                },
            )
            .unwrap();
        worker
            .send_cmd(
                &sync_cmd_tx,
                SyncCommand::ListSpaceDocuments {
                    space_id: "043a7187-74c5-42bd-8a25-adbeb1bfcd5c".into(),
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
            space_id: "043a7187-74c5-42bd-8a25-adbeb1bfcd5c".into(),
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

    // --- one-shot completion-honesty tests ---

    /// A clean pass (no rejections, nothing in flight, no queued blobs) reports
    /// nothing — the one-shot may claim success.
    #[test]
    fn test_one_shot_incomplete_none_when_clean() {
        let dir = tempfile::tempdir().unwrap();
        let worker = test_worker(dir.path().to_path_buf());
        assert_eq!(worker.one_shot_incomplete(), None);
    }

    /// Relay error frames recorded through the PRODUCTION classify edge must
    /// surface in the completion check, with the count and the last message. A
    /// quota-wedged one-shot must fail — every rejected operation is work the
    /// relay did not apply, so "complete" would be a lie the operator only
    /// discovers as silent staleness.
    #[test]
    fn test_one_shot_incomplete_reports_relay_rejections() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        for _ in 0..2 {
            let _ = worker.classify_sync_event(SyncEvent::Error {
                message: "quota exceeded ({\"cap_kind\":\"op_count\"})".into(),
                auth_failed: false,
            });
        }
        let reason = worker
            .one_shot_incomplete()
            .expect("relay rejections mean the pass is incomplete");
        assert!(
            reason.contains("rejected 2 operation(s)"),
            "the failure counts the rejections: {reason}"
        );
        assert!(
            reason.contains("quota exceeded"),
            "the failure names the relay's cause: {reason}"
        );
    }

    /// Ack-bearing commands still in flight at the idle exit mean the relay
    /// stopped answering before the backlog drained — not success.
    #[test]
    fn test_one_shot_incomplete_reports_unacked_commands() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (sync_cmd_tx, _sync_cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        worker
            .send_cmd(
                &sync_cmd_tx,
                SyncCommand::ListSpaceDocuments {
                    space_id: "043a7187-74c5-42bd-8a25-adbeb1bfcd5c".into(),
                },
            )
            .unwrap();
        let reason = worker
            .one_shot_incomplete()
            .expect("an unacked command means the pass is incomplete");
        assert!(
            reason.contains("1 command(s)"),
            "the failure counts the unacked commands: {reason}"
        );
    }

    /// Blob uploads still queued at the idle exit are undelivered bytes — not
    /// success.
    #[test]
    fn test_one_shot_incomplete_reports_queued_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let worker = test_worker(dir.path().to_path_buf());
        worker.blob_backlog.store(3, Ordering::Relaxed);
        let reason = worker
            .one_shot_incomplete()
            .expect("queued blobs mean the pass is incomplete");
        assert!(
            reason.contains("3 blob upload(s)"),
            "the failure counts the queued blobs: {reason}"
        );
    }

    /// Events parked in the startup buffer are undispatched intake: the ONE
    /// drained test must say "not drained" while any are queued, or the
    /// coalescing gates (flush, reconcile probe, `intake_backlogged`) fire
    /// once per buffered event — the burst they exist to coalesce.
    #[test]
    fn test_intake_drained_counts_startup_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (_fe_tx, fe_rx) = mpsc::channel::<FileEvent>(4);
        let (_se_tx, se_rx) = mpsc::channel::<SyncEvent>(4);
        assert!(worker.intake_drained(&fe_rx, &se_rx));

        worker.startup_buffer.push_back(SyncEvent::Disconnected);
        assert!(
            !worker.intake_drained(&fe_rx, &se_rx),
            "a buffered startup event is undispatched intake"
        );
        // The coalesced flush obeys the same test: dirty state must NOT
        // persist mid-buffer.
        worker.state_dirty = true;
        worker.flush_state_if_caught_up(&fe_rx, &se_rx);
        assert!(
            worker.state_dirty,
            "the flush must wait for the startup buffer to drain"
        );

        worker.startup_buffer.clear();
        worker.flush_state_if_caught_up(&fe_rx, &se_rx);
        assert!(!worker.state_dirty, "drained buffer: the flush proceeds");
    }

    /// The drain-edge probe honors the pacing floor: a pass stamps the
    /// window shut, an immediately re-armed debt stays armed within the
    /// floor (a trickle must not run the O(records) pass per event), and an
    /// elapsed floor lets the next probe pay it.
    #[test]
    fn test_reconcile_probe_honors_pacing_floor() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (_fe_tx, fe_rx) = mpsc::channel::<FileEvent>(4);
        let (_se_tx, se_rx) = mpsc::channel::<SyncEvent>(4);
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<SyncCommand>();
        let (sup_tx, _sup_rx) = mpsc::unbounded_channel::<Suppression>();

        // Construction seeds the floor open: the first probe pays the debt.
        worker.state.placement_dirty = true;
        worker.reconcile_if_caught_up(&fe_rx, &se_rx, &cmd_tx, &sup_tx);
        assert!(
            !worker.state.placement_dirty,
            "the first probe after construction pays the armed debt"
        );

        // Re-armed within the floor: the probe must hold.
        worker.state.placement_dirty = true;
        worker.reconcile_if_caught_up(&fe_rx, &se_rx, &cmd_tx, &sup_tx);
        assert!(
            worker.state.placement_dirty,
            "within the floor the debt stays armed — no per-trickle-event pass"
        );

        // BURST debt bypasses the floor: a chunked bulk cascade drains
        // between ack floods faster than the floor, and pacing those edges
        // would stall every chunk on the metrics tick.
        worker.state.placement_dirty = true;
        worker.burst_seen_since_pass = true;
        worker.reconcile_if_caught_up(&fe_rx, &se_rx, &cmd_tx, &sup_tx);
        assert!(
            !worker.state.placement_dirty,
            "burst debt pays at the drain edge regardless of the floor"
        );
        assert!(
            !worker.burst_seen_since_pass,
            "the paying pass consumes the burst memory"
        );

        // Floor elapsed (forced: no window ever closed): the next
        // trickle-armed probe pays.
        worker.state.placement_dirty = true;
        worker.last_reconcile_pass = None;
        worker.reconcile_if_caught_up(&fe_rx, &se_rx, &cmd_tx, &sup_tx);
        assert!(
            !worker.state.placement_dirty,
            "an elapsed floor lets the probe pay the debt"
        );
    }

    /// The wakeup is armed only for debt the probe would pay. With the
    /// intake undrained (a queued file event the closed intake gate holds
    /// back) the loop parks instead of arming a deadline that is already
    /// elapsed and that every refused iteration would re-arm: a spin for the
    /// whole gap between relay acks. Both the burst branch and the
    /// floor-paced branch obey the rule.
    #[test]
    fn test_reconcile_wakeup_at_parks_while_intake_undrained() {
        let dir = tempfile::tempdir().unwrap();
        let mut worker = test_worker(dir.path().to_path_buf());
        let (fe_tx, mut fe_rx) = mpsc::channel::<FileEvent>(4);
        let (_se_tx, se_rx) = mpsc::channel::<SyncEvent>(4);
        let queued = || FileEvent::Modified {
            rel_path: std::path::PathBuf::from("queued.md"),
        };

        // Burst debt on a drained intake: due now.
        worker.state.placement_dirty = true;
        worker.burst_seen_since_pass = true;
        assert!(
            worker.reconcile_wakeup_at(&fe_rx, &se_rx).is_some(),
            "burst debt on a drained intake arms an immediate wakeup"
        );

        // The same debt behind a queued file event: nothing is armed; the
        // event (or the ack that reopens the gate for it) is the wakeup.
        fe_tx.try_send(queued()).unwrap();
        assert!(
            worker.reconcile_wakeup_at(&fe_rx, &se_rx).is_none(),
            "an undrained intake is debt the probe refuses; arming would spin"
        );

        // Floor-paced debt (no burst memory) follows the same rule.
        worker.burst_seen_since_pass = false;
        worker.last_reconcile_pass = Some(Instant::now());
        assert!(
            worker.reconcile_wakeup_at(&fe_rx, &se_rx).is_none(),
            "floor-paced debt behind an undrained intake arms nothing either"
        );

        // Drained again: the floor's end is armed.
        fe_rx.try_recv().unwrap();
        assert!(
            worker.reconcile_wakeup_at(&fe_rx, &se_rx).is_some(),
            "once the intake drains, floor-paced debt arms the window's end"
        );
    }
}
