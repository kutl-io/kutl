//! Signal-record catch-up + re-seed on (re)connect for
//! [`SpaceWorker`]: the driver edge that, once a session's handshake ack
//! advertises the `signal-records` capability, pulls the relay's durable
//! records this daemon lacks and pushes back any this daemon holds that the
//! relay lacks.
//!
//! Direction:
//! - **Pull:** GET records `since` the local high-water cursor, ingesting each
//!   via [`SpaceWorker::ingest_signal_record`] (which handles append + cursor +
//!   dedup + advisory verify — never re-implemented here). Pages until the
//!   relay reports no more.
//! - **Push (re-seed):** if the relay's returned `high_water` is BELOW the
//!   local high-water, this daemon holds records above it — re-seed them
//!   (`records_after(local, relay_high_water)`), so the relay (and, through it,
//!   other joiners) recover them: clients are the source of truth.
//!
//! All catch-up is ADVISORY to the session: a failure is surfaced to the caller
//! to log, and the session proceeds — live broadcasts still arrive, and the
//! next reconnect retries.

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use super::SpaceWorker;

/// Records per `SignalReseed` frame (the protocol caps a frame at 100
/// records).
///
/// Well under the relay's `MAX_RESEED_BATCH` of 10,000, which is a refusal
/// threshold rather than a target: a single frame above it is rejected
/// outright, so a daemon that pushed its whole surplus in one frame would lose
/// its ENTIRE re-seed exactly when it had been offline longest — chunking is
/// what keeps client-held recovery reachable for the deployments that need it
/// most.
const RESEED_CHUNK_SIZE: usize = 100;

impl SpaceWorker {
    /// Join the space's signal stream and start walking the backlog.
    ///
    /// **Kicks off; it does not drain.** The relay answers one `SignalPage` per
    /// `SubscribeSignals`, so the walk lives in the session
    /// loop's `LoopInput::SignalPage` arm, which ingests a page and re-sends
    /// with the returned cursor until `more` is false.
    ///
    /// **Subscribing is also the only way SPACE-audience signals arrive at
    /// all.** The relay broadcasts space-audience signals to
    /// `signal_subscribers` only — there is no document-subscriber union — so
    /// a connection that never sends this frame
    /// receives no space-audience broadcast.
    ///
    /// Resumes from the persisted cursor so a restart does not re-fetch from
    /// zero. A missing or corrupt cursor degrades to from-zero, which is safe:
    /// ingest dedups on `record_id` and the fold is order-independent, so
    /// over-fetching costs work and never correctness.
    ///
    /// # Errors
    ///
    /// Returns an error if the signal store cannot be opened or its cursor
    /// cannot be read.
    pub(super) fn start_signal_catch_up(
        &mut self,
        sync_cmd_tx: &tokio::sync::mpsc::UnboundedSender<crate::client::SyncCommand>,
    ) -> Result<()> {
        // Open the store now (idempotent) so the cursor read and every later
        // ingest run under its held flock.
        self.ensure_signal_store()?;
        let dir = self
            .signal_store
            .as_ref()
            .expect("signal store opened by ensure_signal_store")
            .dir()
            .to_path_buf();
        // Resume one millisecond BEFORE the persisted cursor. Live ingest
        // advances the cursor to each broadcast record's HLC, so it can sit
        // mid-`physical_ms`-group — and the relay's serve filter is coarse,
        // which would exclude the unseen rest of that group forever. The
        // rewound floor re-serves the whole group; ingest dedups the overlap.
        // See `kutl_signals::catchup::resume_floor`.
        let since = kutl_signals::catchup::load_cursor(&dir)
            .context("loading the signal catch-up cursor")?
            .and_then(|c| kutl_signals::catchup::resume_floor(&c));

        let _ = sync_cmd_tx.send(crate::client::SyncCommand::SubscribeSignals {
            space_id: self.config.space_id.clone(),
            cursor: since,
        });
        Ok(())
    }

    /// Push the surplus this daemon holds above the relay's high-water, once
    /// the backlog walk has finished.
    ///
    /// Called from the `SignalPage` arm when `more` is false, because that is
    /// when the relay's high-water is known.
    ///
    /// # Errors
    ///
    /// Returns an error if the local segments cannot be loaded or the push
    /// fails.
    pub(super) fn reseed_after_catch_up(
        &mut self,
        sync_cmd_tx: &tokio::sync::mpsc::UnboundedSender<crate::client::SyncCommand>,
        relay_high_water: Option<&kutl_proto::sync::Hlc>,
    ) -> Result<()> {
        let space_id = self.config.space_id.clone();
        // Only against a relay that accepts client-pushed
        // history. A relay can serve the pull half and refuse this one —
        // kutlhub does — and pushing anyway earns a rejection that would
        // surface as a catch-up failure even though the pull fully succeeded.
        if !self.relay_accepts_reseed {
            debug!(
                %space_id,
                "signal re-seed: relay does not accept client-pushed history; skipping the push"
            );
            return Ok(());
        }
        self.reseed_surplus(sync_cmd_tx, &space_id, relay_high_water)
    }

    /// Re-seed the relay with records this daemon's segments hold that sort
    /// STRICTLY above `relay_high_water` — the surplus the relay lacks. A no-op
    /// when there is no surplus.
    ///
    /// # Errors
    ///
    /// Returns an error if the local segments cannot be loaded or the re-seed
    /// POST fails.
    fn reseed_surplus(
        &mut self,
        sync_cmd_tx: &tokio::sync::mpsc::UnboundedSender<crate::client::SyncCommand>,
        space_id: &str,
        relay_high_water: Option<&kutl_proto::sync::Hlc>,
    ) -> Result<()> {
        let loaded = self
            .signal_store
            .as_ref()
            .expect("signal store opened by ensure_signal_store")
            .load()
            .context("loading local signal segments for re-seed")?;

        // Records STRICTLY above the relay's high-water are the surplus. From a
        // relay-high-water of `None` (empty relay) this yields everything —
        // exactly the fresh-relay re-seed.
        let surplus: Vec<kutl_proto::sync::Signal> =
            kutl_signals::catchup::records_after(&loaded.records, relay_high_water)
                .cloned()
                .collect();

        if surplus.is_empty() {
            debug!(%space_id, "signal re-seed: no surplus records to push");
            return Ok(());
        }

        let count = surplus.len();
        let chunks: std::collections::VecDeque<Vec<kutl_proto::sync::Signal>> = surplus
            .chunks(RESEED_CHUNK_SIZE)
            .map(<[kutl_proto::sync::Signal]>::to_vec)
            .collect();
        let total = chunks.len();
        info!(
            %space_id,
            surplus = count,
            chunks = total,
            "signal re-seed starting"
        );
        self.reseed = Some(crate::daemon::ReseedPush::new(chunks, total));
        self.send_next_reseed_chunk(sync_cmd_tx, space_id);
        Ok(())
    }

    /// Send the next queued re-seed chunk, or finish the push when the queue is
    /// empty.
    pub(super) fn send_next_reseed_chunk(
        &mut self,
        sync_cmd_tx: &tokio::sync::mpsc::UnboundedSender<crate::client::SyncCommand>,
        space_id: &str,
    ) {
        let Some(push) = self.reseed.as_mut() else {
            return;
        };
        let Some(records) = push.take_next() else {
            info!(
                %space_id,
                chunks = push.total_chunks(),
                "signal re-seed complete"
            );
            self.reseed = None;
            return;
        };
        let client_ref = uuid::Uuid::new_v4().to_string();
        push.mark_in_flight(client_ref.clone());
        let _ = sync_cmd_tx.send(crate::client::SyncCommand::SignalReseed {
            space_id: space_id.to_owned(),
            client_ref,
            records,
        });
    }

    /// Route a `SignalAck` to the re-seed walk, or report it as a submit
    /// failure.
    ///
    /// The two cases are told apart by `client_ref`, and the `else` branch is
    /// the one that matters: an ack matching nothing in flight is a SUBMIT
    /// outcome, and dropping its failure would let this daemon believe history
    /// it holds reached the relay when it did not.
    pub(super) fn route_signal_ack(
        &mut self,
        client_ref: &str,
        success: bool,
        error: Option<&str>,
        sync_cmd_tx: &tokio::sync::mpsc::UnboundedSender<crate::client::SyncCommand>,
    ) {
        let outcome = if success {
            Ok(())
        } else {
            Err(error.unwrap_or("no reason given"))
        };
        if self.reseed_ack_is_ours(client_ref) {
            self.handle_reseed_ack(client_ref, outcome, sync_cmd_tx);
        } else if let Err(reason) = outcome {
            tracing::error!(reason, "relay refused a signal submit");
            crate::metrics_calls::record_error(crate::metrics_calls::error_category::SYNC_EVENT);
        }
    }

    /// Does this ack belong to the re-seed push in flight?
    ///
    /// Asked before dispatch so the session can tell a re-seed ack from a submit
    /// ack: an unmatched failure is a submit refusal that must still be
    /// reported, and silently treating every ack as the walk's would swallow it.
    pub(super) fn reseed_ack_is_ours(&self, client_ref: &str) -> bool {
        self.reseed
            .as_ref()
            .is_some_and(|p| p.is_in_flight(client_ref))
    }

    /// A `SignalAck` arrived; advance or abandon the re-seed walk.
    ///
    /// Correlated on `client_ref`, so an ack for anything else — an authored
    /// submit, a stale chunk from a connection that has since dropped — cannot
    /// advance this push.
    ///
    /// A refusal ABANDONS the whole push rather than skipping to the next chunk,
    /// mirroring what [`SpaceWorker::handle_signal_page`] does in the pull
    /// direction. The reasoning is the same in both: continuing past a chunk the
    /// peer did not accept replicates history with a hole in it, and a hole is
    /// exactly what neither end can detect afterwards. The next reconnect
    /// re-derives the surplus and starts over; every record is idempotent on
    /// `record_id`, so the only cost of retrying is the work.
    pub(super) fn handle_reseed_ack(
        &mut self,
        client_ref: &str,
        outcome: Result<(), &str>,
        sync_cmd_tx: &tokio::sync::mpsc::UnboundedSender<crate::client::SyncCommand>,
    ) {
        let Some(push) = self.reseed.as_ref() else {
            return;
        };
        if !push.is_in_flight(client_ref) {
            return;
        }
        if let Err(reason) = outcome {
            warn!(
                space_id = %self.config.space_id,
                reason,
                "relay refused a re-seed chunk; abandoning the push (retries on reconnect)"
            );
            self.reseed = None;
            return;
        }
        let space_id = self.config.space_id.clone();
        self.send_next_reseed_chunk(sync_cmd_tx, &space_id);
    }
}
