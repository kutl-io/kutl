//! The relay's `GET /spaces/{space_id}/changes` client, used by
//! `kutl space feed`.
//!
//! `/changes` is the ONLY route this client fronts. Everything else rides the
//! WebSocket: authoring goes to the submit frames, catch-up to
//! `SubscribeSignals`, replication to `SignalReseed` — a new relay capability
//! is a new FRAME, never a new route.
//!
//! The request carries the space bearer token (`kutl_`-prefixed) in an
//! `Authorization: Bearer` header, resolved by the relay per-request.
//!
//! ## async vs blocking
//!
//! This client is ASYNC (`reqwest::Client`). The sync CLI drives it by blocking
//! on a `tokio` runtime at its call site rather than this module carrying a
//! blocking variant — one runtime per one-shot CLI invocation is cheap and
//! keeps a single implementation. (The daemon was the other caller and no
//! longer uses this module at all; its catch-up rides the socket.)

use anyhow::{Context, Result};
use kutl_proto::sync::{RegistryEntry, Signal};
use serde::{Deserialize, Serialize};

/// The lifecycle event a transition applies to a signal: closed (with a
/// reason) or reopened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransitionEvent {
    /// Close the signal (carries a [`TransitionRequest::reason`]).
    Closed,
    /// Reopen a previously-closed signal.
    Reopened,
}

/// A caller's transition intent, built by the CLI's close/resolve/reopen
/// verbs and translated into the relay's `SubmitTransition` WS frame.
/// `reason` applies to a close (resolved/declined/withdrawn).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionRequest {
    /// The transition to apply (close or reopen).
    pub event: TransitionEvent,
    /// The close reason (only meaningful when `event` is
    /// [`TransitionEvent::Closed`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// One page of the space activity feed, decoded from the relay's
/// `GET /spaces/{id}/changes` JSON response.
///
/// The relay serializes [`ChangesResponse`] (`kutl_relay::change_backend`) as
/// JSON; this struct mirrors its shape using the shared proto types, which
/// derive `serde::Deserialize` via the prost-build config. Unknown fields in
/// the relay's JSON (e.g. relay-internal provenance not in the proto schema)
/// are silently ignored by `serde_json`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct FeedPage {
    /// Signals since the last checkpoint. Empty when none.
    #[serde(default)]
    pub signals: Vec<Signal>,
    /// Documents registered, renamed, or deleted since the last checkpoint.
    /// Empty when none.
    #[serde(default)]
    pub document_changes: Vec<RegistryEntry>,
    /// Opaque checkpoint for paging. Pass this as `checkpoint` on the next
    /// call to continue from this position. Empty string on an empty relay or
    /// when no change backend is configured.
    #[serde(default)]
    pub checkpoint: String,
}

/// A shared async HTTP client for a relay's signal-records surface.
///
/// Holds a reqwest client and the relay's HTTP base URL, derived from the WS
/// URL — construct via [`SignalCatchUpClient::from_ws_url`], which reuses
/// [`crate::url::ws_url_to_http`].
#[derive(Debug, Clone)]
pub struct SignalCatchUpClient {
    http: reqwest::Client,
    /// Relay HTTP base with no trailing slash, e.g. `http://host:9100`.
    base: String,
}

impl SignalCatchUpClient {
    /// Construct from a WebSocket relay URL, deriving the HTTP base via
    /// [`crate::url::ws_url_to_http`] (`ws://→http://`, `wss://→https://`,
    /// strips a trailing `/ws`). Reuses the shared mapping — never hand-rolled.
    #[must_use]
    pub fn from_ws_url(ws_url: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: crate::url::ws_url_to_http(ws_url),
        }
    }

    /// `GET /spaces/{space_id}/changes?checkpoint=<cp>` — one page of the
    /// space activity feed (edits + signals) from the relay.
    ///
    /// `checkpoint` pages deterministically (rewind-to-position): pass `None`
    /// for the first page and the returned [`FeedPage::checkpoint`] for the
    /// next. The relay degrades to an empty [`FeedPage`] when no change
    /// backend is configured; this method treats that the same as a valid
    /// (but empty) response.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails to send, the relay returns a
    /// non-success status, or the JSON body does not decode as [`FeedPage`].
    pub async fn get_changes(
        &self,
        space_id: &str,
        token: &str,
        checkpoint: Option<&str>,
    ) -> Result<FeedPage> {
        let base_url = format!("{}/spaces/{space_id}/changes", self.base);
        let url = match checkpoint {
            Some(cp) => format!("{base_url}?checkpoint={cp}"),
            None => base_url,
        };

        let resp = self
            .http
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("requesting space feed from {url}"))?;

        let resp = ensure_success(resp, "space feed")
            .await
            .with_context(|| format!("space feed rejected by {url}"))?;
        resp.json::<FeedPage>()
            .await
            .context("decoding space feed response")
    }
}

/// Turn a non-success HTTP response into an error that includes the status and
/// the relay's reason body (the routes always return a non-empty reason, never
/// a bare status code), so callers surface WHY a request was
/// rejected. A success response passes through unchanged.
async fn ensure_success(resp: reqwest::Response, op: &str) -> Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let reason = resp.text().await.unwrap_or_default();
    anyhow::bail!("{op} returned {status}: {reason}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::extract::{Path, Query, State};
    use axum::http::HeaderMap;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use reqwest::header;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    /// A bearer token the stub asserts each request carried.
    const TEST_TOKEN: &str = "kutl_test_bearer";

    /// What the stub server captured about the last request it served, so the
    /// test can assert the client sent the right bearer and query param.
    ///
    /// Only fields a test actually asserts belong here — a capture field
    /// nothing writes reads like coverage that exists.
    #[derive(Default)]
    struct Captured {
        changes_auth: Option<String>,
        changes_checkpoint: Option<String>,
    }

    type SharedCapture = Arc<Mutex<Captured>>;

    /// Stub handler for `GET /spaces/{space_id}/changes` — returns a canned
    /// JSON [`FeedPage`] body with one `document_changes` entry and the
    /// `checkpoint` the test asserts `get_changes` decoded correctly.
    async fn get_changes_stub(
        State(cap): State<SharedCapture>,
        Path(_space_id): Path<String>,
        Query(q): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        {
            let mut c = cap.lock().unwrap();
            c.changes_auth = headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            c.changes_checkpoint = q.get("checkpoint").cloned();
        }
        axum::Json(serde_json::json!({
            "signals": [],
            "document_changes": [
                {
                    "document_id": "doc-feed-1",
                    "path": "notes/feed.md",
                    "created_by": "did:key:test",
                    "created_at": 1_000_000_i64,
                    "edited_at": 2_000_000_i64
                }
            ],
            "checkpoint": "cp-next"
        }))
    }

    /// Spawn the stub relay on an ephemeral port; return its `ws://` URL (so the
    /// test also exercises `from_ws_url`'s ws→http derivation) and the capture.
    async fn spawn_stub() -> (String, SharedCapture) {
        let cap: SharedCapture = Arc::new(Mutex::new(Captured::default()));
        // `/changes` is the only route the client under test calls.
        let app = Router::new()
            .route("/spaces/{space_id}/changes", get(get_changes_stub))
            .with_state(Arc::clone(&cap));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("ws://{addr}/ws"), cap)
    }

    /// `get_changes` sends the bearer, omits the `checkpoint` query key on the
    /// first call, decodes the JSON [`FeedPage`], and carries the returned
    /// checkpoint so the caller can page.
    #[tokio::test]
    async fn test_get_changes_decodes_feed_page_and_sends_bearer() {
        let (ws_url, cap) = spawn_stub().await;
        let client = SignalCatchUpClient::from_ws_url(&ws_url);

        let page = client
            .get_changes("space-E", TEST_TOKEN, None)
            .await
            .expect("get_changes ok");

        // The stub returns one document_change entry and an empty signals list.
        assert_eq!(page.signals.len(), 0);
        assert_eq!(page.document_changes.len(), 1);
        assert_eq!(page.document_changes[0].document_id, "doc-feed-1");
        assert_eq!(page.document_changes[0].path, "notes/feed.md");
        assert_eq!(page.document_changes[0].edited_at, Some(2_000_000_i64));
        assert_eq!(page.checkpoint, "cp-next");

        let c = cap.lock().unwrap();
        assert_eq!(
            c.changes_auth.as_deref(),
            Some("Bearer kutl_test_bearer"),
            "bearer must be forwarded"
        );
        assert!(
            c.changes_checkpoint.is_none(),
            "first-page call must omit the checkpoint query key"
        );
    }

    /// A paged `get_changes` call (with a prior `checkpoint`) includes the
    /// `?checkpoint=<cp>` query parameter so the relay can resume from that
    /// position.
    #[tokio::test]
    async fn test_get_changes_with_checkpoint_sends_query_param() {
        let (ws_url, cap) = spawn_stub().await;
        let client = SignalCatchUpClient::from_ws_url(&ws_url);

        client
            .get_changes("space-F", TEST_TOKEN, Some("cp-abc"))
            .await
            .expect("paged get_changes ok");

        let c = cap.lock().unwrap();
        assert_eq!(
            c.changes_checkpoint.as_deref(),
            Some("cp-abc"),
            "checkpoint must be forwarded as a query param"
        );
    }
}
