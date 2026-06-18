//! Stress test scenarios.

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use kutl_core::{SignedDuration, elapsed_ms, std_duration};
use tracing::info;

use crate::client::StressClient;
use crate::config::StressConfig;
use crate::report::Report;

/// Nanoseconds per second, used for rate interval calculation.
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// How often (in ops) `throttle_read` clients drain during the edit phase.
const THROTTLE_DRAIN_INTERVAL: usize = 10;

/// How often (in ops) `large_doc` drains incoming messages to avoid
/// unbounded channel growth.
const LARGE_DOC_DRAIN_INTERVAL: usize = 100;

/// Fraction of total ops after which `pause_read` clients freeze.
const PAUSE_TRIGGER_FRACTION_NUM: usize = 1;
/// Denominator for pause trigger fraction (triggers at 1/3 of ops sent).
const PAUSE_TRIGGER_FRACTION_DEN: usize = 3;

/// Run the configured scenario and return a report.
pub async fn run(config: &StressConfig) -> Result<Report> {
    match config.scenario.as_str() {
        "sustained" => sustained(config).await,
        "burst" => burst(config).await,
        "large_doc" => large_doc(config).await,
        "many_docs" => many_docs(config).await,
        "slow_client" => slow_client(config).await,
        "pause_read" => pause_read(config).await,
        "throttle_read" => throttle_read(config).await,
        "churner" => churner(config).await,
        "chaos" => chaos(config).await,
        other => bail!("unknown scenario: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build a rate-limiting interval from ops-per-second config.
/// Returns `None` if `ops_per_sec` is 0 (unlimited).
#[allow(clippy::cast_precision_loss)]
fn rate_interval(ops_per_sec: usize) -> Option<tokio::time::Interval> {
    if ops_per_sec == 0 {
        return None;
    }
    let period_nanos = NANOS_PER_SEC / ops_per_sec as u64;
    Some(tokio::time::interval(Duration::from_nanos(period_nanos)))
}

/// Connect `client_count` clients to the same document.
async fn connect_all(config: &StressConfig) -> Result<Vec<StressClient>> {
    let mut clients = Vec::with_capacity(config.client_count);
    for i in 0..config.client_count {
        let client = StressClient::connect(
            &config.relay_url,
            &format!("stress-{i}"),
            &config.space_id,
            &config.doc_id,
        )
        .await?;
        clients.push(client);
    }
    Ok(clients)
}

/// Drain all clients, check convergence, measure timing, and close connections.
///
/// Returns `(convergence_time, converged, final_doc_size)`.
async fn converge_and_close(mut clients: Vec<StressClient>) -> Result<(Duration, bool, usize)> {
    let conv_start = Instant::now();
    for client in &mut clients {
        client.drain_until_quiet().await?;
    }
    let convergence_time = conv_start.elapsed();
    let reference = clients[0].content();
    let converged = clients.iter().all(|c| c.content() == reference);
    let final_doc_size = clients[0].doc_size();
    for client in clients {
        client.close().await?;
    }
    Ok((convergence_time, converged, final_doc_size))
}

/// Collect eviction stats and close all clients.
fn eviction_stats(clients: &[StressClient]) -> (u32, u32, u32) {
    let total: u32 = clients.iter().map(StressClient::resubscribe_count).sum();
    // When no degraded split is relevant, degraded/healthy are 0/total.
    (total, 0, total)
}

/// Collect eviction stats split by degraded (first `n`) vs healthy.
fn eviction_stats_split(clients: &[StressClient], degraded_count: usize) -> (u32, u32, u32) {
    let degraded: u32 = clients[..degraded_count]
        .iter()
        .map(StressClient::resubscribe_count)
        .sum();
    let healthy: u32 = clients[degraded_count..]
        .iter()
        .map(StressClient::resubscribe_count)
        .sum();
    (degraded + healthy, degraded, healthy)
}

// ---------------------------------------------------------------------------
// sustained: N clients send edits at configured rate, measure latency
// ---------------------------------------------------------------------------

async fn sustained(config: &StressConfig) -> Result<Report> {
    info!(
        clients = config.client_count,
        ops = config.ops_per_client,
        "starting sustained scenario"
    );

    let mut clients = connect_all(config).await?;
    let mut latencies: Vec<f64> = Vec::new();
    let mut interval = rate_interval(config.ops_per_sec);

    let start = Instant::now();

    for op_idx in 0..config.ops_per_client {
        for (i, client) in clients.iter_mut().enumerate() {
            if let Some(iv) = &mut interval {
                iv.tick().await;
            }
            let op_start = Instant::now();
            let pos = client.doc_size();
            client.insert(pos, &format!("c{i}o{op_idx}")).await?;
            latencies.push(elapsed_ms(op_start.elapsed()));
        }
    }

    let edit_duration = start.elapsed();

    let (convergence_time, converged, final_doc_size) = converge_and_close(clients).await?;

    latencies.sort_by(f64::total_cmp);
    let total_ops = config.client_count * config.ops_per_client;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let duration_ms = elapsed_ms(edit_duration) as u64;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let convergence_time_ms = elapsed_ms(convergence_time) as u64;

    Ok(Report::new(
        "sustained",
        config.client_count,
        total_ops,
        duration_ms,
        convergence_time_ms,
        &latencies,
        final_doc_size,
        converged,
    ))
}

// ---------------------------------------------------------------------------
// burst: all clients send M edits simultaneously, measure convergence
// ---------------------------------------------------------------------------

async fn burst(config: &StressConfig) -> Result<Report> {
    info!(
        clients = config.client_count,
        ops = config.ops_per_client,
        "starting burst scenario"
    );

    let mut clients = connect_all(config).await?;

    let start = Instant::now();

    // Clients insert sequentially (one after another) with no rate limiting
    // or draining between ops — a sequential flood that saturates the relay
    // as fast as the async runtime allows.
    for (i, client) in clients.iter_mut().enumerate() {
        for op_idx in 0..config.ops_per_client {
            let pos = client.doc_size();
            client.insert(pos, &format!("c{i}o{op_idx}")).await?;
        }
    }
    let edit_duration = start.elapsed();

    let (convergence_time, converged, final_doc_size) = converge_and_close(clients).await?;

    let total_ops = config.client_count * config.ops_per_client;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let duration_ms = elapsed_ms(edit_duration) as u64;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let convergence_time_ms = elapsed_ms(convergence_time) as u64;

    Ok(Report::new(
        "burst",
        config.client_count,
        total_ops,
        duration_ms,
        convergence_time_ms,
        &[],
        final_doc_size,
        converged,
    ))
}

// ---------------------------------------------------------------------------
// large_doc: grow document to target size, measure per-op latency
// ---------------------------------------------------------------------------

async fn large_doc(config: &StressConfig) -> Result<Report> {
    info!(
        clients = config.client_count,
        ops = config.ops_per_client,
        "starting large_doc scenario"
    );

    let mut clients = connect_all(config).await?;
    let mut latencies: Vec<f64> = Vec::new();

    let start = Instant::now();
    let total_ops = config.ops_per_client;

    // Only first client writes; others just receive.
    let writer = &mut clients[0];
    for op_idx in 0..total_ops {
        let op_start = Instant::now();
        let pos = writer.doc_size();
        writer
            .insert(pos, "abcdefghij") // 10 chars per op
            .await?;
        latencies.push(elapsed_ms(op_start.elapsed()));

        // Drain periodically.
        if op_idx % LARGE_DOC_DRAIN_INTERVAL == LARGE_DOC_DRAIN_INTERVAL - 1 {
            writer.drain_incoming().await?;
        }
    }
    let edit_duration = start.elapsed();

    let (convergence_time, converged, final_doc_size) = converge_and_close(clients).await?;

    latencies.sort_by(f64::total_cmp);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let duration_ms = elapsed_ms(edit_duration) as u64;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let convergence_time_ms = elapsed_ms(convergence_time) as u64;

    Ok(Report::new(
        "large_doc",
        config.client_count,
        total_ops,
        duration_ms,
        convergence_time_ms,
        &latencies,
        final_doc_size,
        converged,
    ))
}

// ---------------------------------------------------------------------------
// many_docs: each client subscribes to a unique doc, measures relay overhead
// ---------------------------------------------------------------------------

async fn many_docs(config: &StressConfig) -> Result<Report> {
    info!(
        clients = config.client_count,
        ops = config.ops_per_client,
        "starting many_docs scenario"
    );

    // Each client gets its own document.
    let mut clients = Vec::with_capacity(config.client_count);
    for i in 0..config.client_count {
        let doc_id = format!("{}-{i}", config.doc_id);
        let client = StressClient::connect(
            &config.relay_url,
            &format!("stress-{i}"),
            &config.space_id,
            &doc_id,
        )
        .await?;
        clients.push(client);
    }

    let start = Instant::now();
    let mut latencies: Vec<f64> = Vec::new();

    for op_idx in 0..config.ops_per_client {
        for (i, client) in clients.iter_mut().enumerate() {
            let op_start = Instant::now();
            let pos = client.doc_size();
            client.insert(pos, &format!("c{i}o{op_idx}")).await?;
            latencies.push(elapsed_ms(op_start.elapsed()));
        }
    }
    let edit_duration = start.elapsed();

    for client in &mut clients {
        client.drain_until_quiet().await?;
    }

    let final_doc_size = clients[0].doc_size();

    for client in clients {
        client.close().await?;
    }

    latencies.sort_by(f64::total_cmp);
    let total_ops = config.client_count * config.ops_per_client;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let duration_ms = elapsed_ms(edit_duration) as u64;

    Ok(Report::new(
        "many_docs",
        config.client_count,
        total_ops,
        duration_ms,
        0, // each doc has one writer, always converged
        &latencies,
        final_doc_size,
        true,
    ))
}

// ---------------------------------------------------------------------------
// slow_client: N clients, K degraded — test relay isolation
// ---------------------------------------------------------------------------

async fn slow_client(config: &StressConfig) -> Result<Report> {
    info!(
        clients = config.client_count,
        ops = config.ops_per_client,
        degraded = config.degraded_count,
        "starting slow_client scenario"
    );

    let mut clients = connect_all(config).await?;
    let degraded_count = config.degraded_count.min(config.client_count);

    let start = Instant::now();

    // All clients send ops at rate. Degraded clients (first K) never drain
    // during the edit phase — their outbound channels will fill, triggering eviction.
    let mut interval = rate_interval(config.ops_per_sec);

    for op_idx in 0..config.ops_per_client {
        for (i, client) in clients.iter_mut().enumerate() {
            if let Some(iv) = interval.as_mut() {
                iv.tick().await;
            }
            let pos = client.doc_size();
            client.insert(pos, &format!("c{i}o{op_idx}")).await?;
        }

        // Only healthy clients (index >= degraded_count) drain during edits.
        for client in &mut clients[degraded_count..] {
            client.drain_incoming().await?;
        }
    }
    let edit_duration = start.elapsed();

    // Collect eviction stats before draining (resubscribe counters are stable now).
    let (total_resubs, degraded_resubs, healthy_resubs) =
        eviction_stats_split(&clients, degraded_count);

    info!(
        degraded_resubscribes = degraded_resubs,
        healthy_resubscribes = healthy_resubs,
        "slow_client eviction stats"
    );

    // Degraded clients should have been evicted at least once.
    if degraded_count > 0 && degraded_resubs == 0 {
        tracing::warn!(
            "no degraded clients were evicted — test may not be exercising relay resilience"
        );
    }

    // Healthy clients should NOT have been evicted.
    if healthy_resubs > 0 {
        tracing::warn!(
            healthy_resubscribes = healthy_resubs,
            "healthy clients were evicted — relay may not be isolating slow clients"
        );
    }

    // ALL clients drain (including degraded ones, which now recover).
    let (convergence_time, converged, final_doc_size) = converge_and_close(clients).await?;

    let total_ops = config.client_count * config.ops_per_client;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let duration_ms = elapsed_ms(edit_duration) as u64;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let convergence_time_ms = elapsed_ms(convergence_time) as u64;

    Ok(Report::new(
        "slow_client",
        config.client_count,
        total_ops,
        duration_ms,
        convergence_time_ms,
        &[],
        final_doc_size,
        converged,
    )
    .with_eviction_stats(total_resubs, degraded_resubs, healthy_resubs))
}

// ---------------------------------------------------------------------------
// pause_read: client freezes reading → relay evicts → auto-recovery
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn pause_read(config: &StressConfig) -> Result<Report> {
    let degraded_count = config.degraded_count.min(config.client_count);
    let pause_duration = std_duration(SignedDuration::from_secs(
        i64::try_from(config.pause_duration_secs).unwrap_or(i64::MAX),
    ));

    info!(
        clients = config.client_count,
        ops = config.ops_per_client,
        degraded = degraded_count,
        pause_secs = config.pause_duration_secs,
        "starting pause_read scenario"
    );

    let mut clients = connect_all(config).await?;
    let mut interval = rate_interval(config.ops_per_sec);
    let pause_after =
        config.ops_per_client * PAUSE_TRIGGER_FRACTION_NUM / PAUSE_TRIGGER_FRACTION_DEN;

    let start = Instant::now();

    // Phase 1: all clients send, degraded clients never drain.
    // This is identical to slow_client — relay channel starts filling.
    for op_idx in 0..pause_after {
        for (i, client) in clients.iter_mut().enumerate() {
            if let Some(iv) = interval.as_mut() {
                iv.tick().await;
            }
            let pos = client.doc_size();
            client.insert(pos, &format!("c{i}o{op_idx}")).await?;
        }

        // Only healthy clients drain.
        for client in &mut clients[degraded_count..] {
            client.drain_incoming().await?;
        }
    }

    // Phase 2: degraded clients freeze completely (no send, no read)
    // while healthy clients blast ops. Skip draining here — the 50ms
    // drain timeout would limit throughput and prevent channel fill.
    // Healthy clients catch up in phase 2b and phase 3.
    info!(
        op = pause_after,
        "degraded clients freezing for {pause_duration:?}"
    );
    let pause_deadline = tokio::time::Instant::now() + pause_duration;
    let mut op_idx = pause_after;
    while tokio::time::Instant::now() < pause_deadline && op_idx < config.ops_per_client {
        for (i, client) in clients[degraded_count..].iter_mut().enumerate() {
            if let Some(iv) = interval.as_mut() {
                iv.tick().await;
            }
            let pos = client.doc_size();
            let healthy_idx = degraded_count + i;
            client
                .insert(pos, &format!("c{healthy_idx}o{op_idx}"))
                .await?;
        }
        op_idx += 1;
    }
    info!(
        ops_sent_during_pause = op_idx - pause_after,
        "pause phase complete"
    );

    // Phase 2b: remaining ops — all clients send, degraded still don't drain.
    for op_idx in op_idx..config.ops_per_client {
        for (i, client) in clients.iter_mut().enumerate() {
            if let Some(iv) = interval.as_mut() {
                iv.tick().await;
            }
            let pos = client.doc_size();
            client.insert(pos, &format!("c{i}o{op_idx}")).await?;
        }
        for client in &mut clients[degraded_count..] {
            client.drain_incoming().await?;
        }
    }
    let edit_duration = start.elapsed();

    // Collect eviction stats before draining (resubscribe counters are stable now).
    let (total_resubs, degraded_resubs, healthy_resubs) =
        eviction_stats_split(&clients, degraded_count);

    info!(
        degraded_resubscribes = degraded_resubs,
        healthy_resubscribes = healthy_resubs,
        "pause_read eviction stats"
    );

    // Phase 3: ALL clients drain — degraded clients recover via auto-resubscribe.
    let (convergence_time, converged, final_doc_size) = converge_and_close(clients).await?;

    let total_ops = config.client_count * config.ops_per_client;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let duration_ms = elapsed_ms(edit_duration) as u64;

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let convergence_time_ms = elapsed_ms(convergence_time) as u64;

    Ok(Report::new(
        "pause_read",
        config.client_count,
        total_ops,
        duration_ms,
        convergence_time_ms,
        &[],
        final_doc_size,
        converged,
    )
    .with_eviction_stats(total_resubs, degraded_resubs, healthy_resubs))
}

// ---------------------------------------------------------------------------
// throttle_read: client reads at reduced speed → sustained backpressure
// ---------------------------------------------------------------------------

async fn throttle_read(config: &StressConfig) -> Result<Report> {
    let degraded_count = config.degraded_count.min(config.client_count);

    info!(
        clients = config.client_count,
        ops = config.ops_per_client,
        degraded = degraded_count,
        drain_interval = THROTTLE_DRAIN_INTERVAL,
        "starting throttle_read scenario"
    );

    let mut clients = connect_all(config).await?;
    let mut interval = rate_interval(config.ops_per_sec);

    let start = Instant::now();

    for op_idx in 0..config.ops_per_client {
        for (i, client) in clients.iter_mut().enumerate() {
            if let Some(iv) = interval.as_mut() {
                iv.tick().await;
            }
            let pos = client.doc_size();
            client.insert(pos, &format!("c{i}o{op_idx}")).await?;
        }

        // Degraded clients drain only every THROTTLE_DRAIN_INTERVAL ops (1/10 rate).
        // Single drain_incoming per cycle — no blocking sleep that would stall
        // healthy clients.
        if op_idx % THROTTLE_DRAIN_INTERVAL == THROTTLE_DRAIN_INTERVAL - 1 {
            for client in &mut clients[..degraded_count] {
                client.drain_incoming().await?;
            }
        }

        // Healthy clients drain normally every op.
        for client in &mut clients[degraded_count..] {
            client.drain_incoming().await?;
        }
    }
    let edit_duration = start.elapsed();

    // Collect eviction stats before draining (resubscribe counters are stable now).
    let (total_resubs, degraded_resubs, healthy_resubs) =
        eviction_stats_split(&clients, degraded_count);

    info!(
        degraded_resubscribes = degraded_resubs,
        healthy_resubscribes = healthy_resubs,
        "throttle_read eviction stats"
    );

    // ALL clients drain.
    let (convergence_time, converged, final_doc_size) = converge_and_close(clients).await?;

    let total_ops = config.client_count * config.ops_per_client;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let duration_ms = elapsed_ms(edit_duration) as u64;

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let convergence_time_ms = elapsed_ms(convergence_time) as u64;

    Ok(Report::new(
        "throttle_read",
        config.client_count,
        total_ops,
        duration_ms,
        convergence_time_ms,
        &[],
        final_doc_size,
        converged,
    )
    .with_eviction_stats(total_resubs, degraded_resubs, healthy_resubs))
}

// ---------------------------------------------------------------------------
// churner: rapid connect/disconnect cycles — relay memory cleanup
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn churner(config: &StressConfig) -> Result<Report> {
    let stable_count = config.client_count / 2;
    let churner_count = config.client_count - stable_count;
    let churn_delay = std_duration(SignedDuration::from_millis(
        i64::try_from(config.churn_delay_ms).unwrap_or(i64::MAX),
    ));
    // `checked_div` returns `None` when `churn_cycles == 0`; fall back to
    // the full per-client op budget so degenerate configs still complete.
    let ops_per_cycle = config
        .ops_per_client
        .checked_div(config.churn_cycles)
        .unwrap_or(config.ops_per_client);

    info!(
        stable = stable_count,
        churners = churner_count,
        cycles = config.churn_cycles,
        ops_per_cycle,
        "starting churner scenario"
    );

    // Connect stable clients (stay connected for the whole test).
    let mut stable_clients = Vec::with_capacity(stable_count);
    for i in 0..stable_count {
        let client = StressClient::connect(
            &config.relay_url,
            &format!("stable-{i}"),
            &config.space_id,
            &config.doc_id,
        )
        .await?;
        stable_clients.push(client);
    }

    let start = Instant::now();
    let mut total_reconnects: u32 = 0;

    // Stable clients send all their ops first (with draining).
    for op_idx in 0..config.ops_per_client {
        for (i, client) in stable_clients.iter_mut().enumerate() {
            let pos = client.doc_size();
            client.insert(pos, &format!("s{i}o{op_idx}")).await?;
        }
        for client in &mut stable_clients {
            client.drain_incoming().await?;
        }
    }

    // Churner clients: connect → send → close → sleep → reconnect.
    for cycle in 0..config.churn_cycles {
        let mut churners = Vec::with_capacity(churner_count);
        for i in 0..churner_count {
            let client = StressClient::connect(
                &config.relay_url,
                &format!("churner-{i}-c{cycle}"),
                &config.space_id,
                &config.doc_id,
            )
            .await?;
            churners.push(client);
        }

        // Send ops for this cycle.
        for op_idx in 0..ops_per_cycle {
            for (i, client) in churners.iter_mut().enumerate() {
                let pos = client.doc_size();
                client
                    .insert(pos, &format!("ch{i}c{cycle}o{op_idx}"))
                    .await?;
            }
        }

        // Close all churners.
        for client in churners {
            client.close().await?;
            total_reconnects += 1;
        }

        tokio::time::sleep(churn_delay).await;
    }

    let edit_duration = start.elapsed();

    // Collect eviction stats before draining (resubscribe counters are stable now).
    let (total_resubs, _, _) = eviction_stats(&stable_clients);

    // Stable clients drain — they should all agree.
    let (convergence_time, converged, final_doc_size) = converge_and_close(stable_clients).await?;

    info!(
        total_reconnects,
        total_resubscribes = total_resubs,
        converged,
        "churner stats"
    );

    let total_ops =
        stable_count * config.ops_per_client + churner_count * ops_per_cycle * config.churn_cycles;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let duration_ms = elapsed_ms(edit_duration) as u64;

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let convergence_time_ms = elapsed_ms(convergence_time) as u64;

    Ok(Report::new(
        "churner",
        config.client_count,
        total_ops,
        duration_ms,
        convergence_time_ms,
        &[],
        final_doc_size,
        converged,
    )
    .with_resubscribes(total_resubs)
    .with_reconnects(total_reconnects))
}

// ---------------------------------------------------------------------------
// chaos: relay pod killed mid-test → clients reconnect after restart
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn chaos(config: &StressConfig) -> Result<Report> {
    let kill_interval = std_duration(SignedDuration::from_secs(
        i64::try_from(config.chaos_interval_secs).unwrap_or(i64::MAX),
    ));

    info!(
        clients = config.client_count,
        ops = config.ops_per_client,
        kills = config.chaos_kills,
        interval_secs = config.chaos_interval_secs,
        label = %config.relay_pod_label,
        "starting chaos scenario"
    );

    // Spawn background pod killer.
    let kills = config.chaos_kills;
    let label = config.relay_pod_label.clone();
    let kill_handle = tokio::spawn(async move {
        let mut completed_kills: u32 = 0;
        for kill_idx in 0..kills {
            tokio::time::sleep(kill_interval).await;
            info!(kill = kill_idx, "killing relay pod");
            let status = tokio::process::Command::new("kubectl")
                .args(["delete", "pod", "-l", &label, "--grace-period=0", "--force"])
                .status()
                .await;
            match status {
                Ok(s) if s.success() => {
                    completed_kills += 1;
                    info!(kill = kill_idx, "relay pod killed");
                }
                Ok(s) => {
                    tracing::warn!(kill = kill_idx, code = ?s.code(), "kubectl delete failed");
                }
                Err(e) => {
                    tracing::warn!(kill = kill_idx, error = %e, "kubectl not available");
                }
            }
        }
        completed_kills
    });

    // Main loop: send ops with error-tolerant reconnection.
    let mut clients = connect_all(config).await?;
    let relay_url = config.relay_url.clone();
    let space_id = config.space_id.clone();
    let doc_id = config.doc_id.clone();

    let start = Instant::now();
    let mut total_reconnects: u32 = 0;

    for op_idx in 0..config.ops_per_client {
        // Index-based loop: we may replace clients[i] on reconnect.
        #[allow(clippy::needless_range_loop)]
        for i in 0..clients.len() {
            let pos = clients[i].doc_size();
            let result = clients[i].insert(pos, &format!("c{i}o{op_idx}")).await;

            if result.is_err() {
                info!(client = i, op = op_idx, "send failed, reconnecting");
                // Reconnect with a fresh client.
                match StressClient::connect(
                    &relay_url,
                    &format!("stress-{i}-r{total_reconnects}"),
                    &space_id,
                    &doc_id,
                )
                .await
                {
                    Ok(new_client) => {
                        clients[i] = new_client;
                        total_reconnects += 1;
                    }
                    Err(e) => {
                        tracing::warn!(client = i, error = %e, "reconnect failed, will retry next op");
                    }
                }
            }
        }

        // Drain healthy clients.
        for client in &mut clients {
            let _ = client.drain_incoming().await;
        }
    }
    let edit_duration = start.elapsed();

    // Wait for killer to finish.
    let relay_restarts = kill_handle.await.unwrap_or(0);

    // Try convergence (may not converge if relay lost state).
    let conv_start = Instant::now();
    for client in &mut clients {
        let _ = client.drain_until_quiet().await;
    }
    let convergence_time = conv_start.elapsed();

    let reference = clients[0].content();
    let converged = clients.iter().all(|c| c.content() == reference);
    let final_doc_size = clients[0].doc_size();
    let (total_resubs, _, _) = eviction_stats(&clients);

    info!(
        relay_restarts,
        total_reconnects,
        total_resubscribes = total_resubs,
        converged,
        "chaos stats"
    );

    for client in clients {
        let _ = client.close().await;
    }

    let total_ops = config.client_count * config.ops_per_client;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let duration_ms = elapsed_ms(edit_duration) as u64;

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let convergence_time_ms = elapsed_ms(convergence_time) as u64;

    Ok(Report::new(
        "chaos",
        config.client_count,
        total_ops,
        duration_ms,
        convergence_time_ms,
        &[],
        final_doc_size,
        converged,
    )
    .with_resubscribes(total_resubs)
    .with_reconnects(total_reconnects)
    .with_relay_restarts(relay_restarts))
}
