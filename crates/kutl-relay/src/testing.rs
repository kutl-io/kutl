//! Test-support constructors for embedding a relay in consumer test suites
//! (behind the off-by-default `testing` feature).
//!
//! Everything here builds the STORELESS relay shape: no data dir, no
//! space/registry/change backends, in-memory registries, no persistence.
//! The OSS binary never boots this way — [`crate::build_app`] requires a
//! data dir — the shape exists for host relays that supply their own
//! backends. Tests that pin the in-memory implicit-create /
//! list-spaces-from-registries behaviour construct it explicitly through
//! this seam rather than each spelling out the full backend parameter list.
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

/// The backends a test can vary on a storeless relay. Everything not listed
/// is pinned to the storeless defaults (`None` backends, tracing observer,
/// no-op providers) — add a field only when a real call site needs to vary
/// it, never speculatively.
#[derive(Default)]
pub struct TestBackends {
    /// Content persistence; `None` keeps documents in memory only.
    pub content: Option<Arc<dyn ContentBackend>>,
    /// Blob persistence; `None` is the storeless default (no blob store).
    pub blob: Option<Arc<dyn BlobBackend>>,
    /// Quota enforcement; `None` means nothing is metered.
    pub quota: Option<Arc<dyn QuotaBackend>>,
}

/// Build the storeless relay app with `backends`, returning the router plus
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
pub fn build_storeless_app(
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
        None, // record log — storeless: signal appends are no-ops
        Arc::new(observer::TracingObserver),
        Arc::new(observer::NoopBeforeMergeObserver),
        None, // host feed publisher — the materializer runs regardless
        Arc::new(mcp_tools::NoopToolProvider),
        Arc::new(mcp_tools::DefaultInstructionsProvider),
    )
    .expect("build storeless relay app")
}
