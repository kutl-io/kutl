//! Test-support constructors for embedding a relay in consumer test suites
//! (behind the off-by-default `testing` feature).
//!
//! Everything here builds the IN-MEMORY relay shape: no data dir and no
//! backends unless a test injects one, so registries live in memory and
//! nothing persists. No production relay boots this way: the OSS binary
//! requires a data dir ([`crate::build_app`]) and the hosted relay injects
//! its own backends. It shares one trait with the hosted relay, no space
//! backend, which is why tests that pin the no-space-backend behaviour
//! (implicit create on first touch, list-spaces-from-registries) construct
//! it through this seam rather than each spelling out the full backend
//! parameter list.
//!
//! Dev-only by contract: consumers enable the feature only via a
//! dev-dependency, and no production code path is gated on it.

use std::sync::Arc;

use axum::Router;
use tokio::task::JoinHandle;

use crate::blob_backend::BlobBackend;
use crate::config::RelayConfig;
use crate::content_backend::ContentBackend;
use crate::mcp_tools;
use crate::observer;
use crate::quota_backend::QuotaBackend;
use crate::relay::{ConnId, RelayCommand};

/// Capacity of a connection's bounded control lane (lifecycle broadcasts,
/// displacement corrections), as the connection task sizes it. A harness
/// that plays the connection task itself sizes its lanes from here and from
/// `RelayConfig::outbound_capacity` (the data lane), so eviction and pause
/// behave as they do behind a socket.
pub const CTRL_LANE_CAPACITY: usize = crate::conn::CTRL_CAPACITY;

/// The actor command a client frame is — the mapping the connection task
/// applies to every inbound frame, for a harness that drives
/// [`Relay::process_command`](crate::relay::Relay::process_command) directly.
///
/// # Errors
///
/// Returns an error for an empty envelope or a server-only payload, as the
/// connection task would.
pub fn client_command(
    conn_id: ConnId,
    envelope: kutl_proto::sync::SyncEnvelope,
) -> anyhow::Result<RelayCommand> {
    crate::conn::command_from_envelope(conn_id, envelope)
}

/// The backends a test can vary on an in-memory relay. Everything not listed
/// is pinned to the in-memory defaults (`None` backends, tracing observer,
/// no-op providers) — add a field only when a real call site needs to vary
/// it, never speculatively.
#[derive(Default)]
pub struct TestBackends {
    /// Content persistence; `None` keeps documents in memory only.
    pub content: Option<Arc<dyn ContentBackend>>,
    /// Blob persistence; `None` is the in-memory default (no blob store).
    pub blob: Option<Arc<dyn BlobBackend>>,
    /// Quota enforcement; `None` means nothing is metered.
    pub quota: Option<Arc<dyn QuotaBackend>>,
}

/// Build the in-memory relay app with `backends`, returning the router plus
/// the relay-actor and optional flush task handles.
///
/// Listener-agnostic on purpose: a harness that manages its own listener
/// (fixed port with `SO_REUSEADDR`, restart-in-place) serves the returned
/// router itself; simple tests bind an ephemeral port and
/// `axum::serve(listener, app)`.
///
/// # Panics
///
/// Panics if router construction fails — test-support code fails loudly.
pub fn build_in_memory_app(
    config: RelayConfig,
    backends: TestBackends,
) -> (Router, JoinHandle<()>, Option<JoinHandle<()>>) {
    crate::build_app_with_backends(
        config,
        None, // session backend
        None, // PAT backend
        None, // membership backend
        None, // registry backend
        None, // space backend
        backends.content,
        backends.blob,
        None, // invite backend
        None, // change backend
        backends.quota,
        None, // record log — signal appends are no-ops
        Arc::new(observer::TracingObserver),
        Arc::new(observer::NoopBeforeMergeObserver),
        None, // host feed publisher — the materializer runs regardless
        Arc::new(mcp_tools::NoopToolProvider),
        Arc::new(mcp_tools::DefaultInstructionsProvider),
    )
    .expect("build in-memory relay app")
}
