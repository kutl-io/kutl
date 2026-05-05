//! Stress test client — thin wrapper around [`kutl_client::SyncClient`].
//!
//! Delegates all WebSocket protocol handling, auto-recovery from stale
//! subscriptions, and rate-limit backoff to the shared client library.

use std::time::Duration;

use anyhow::Result;
use kutl_client::{SyncClient, SyncEvent};
use kutl_core::{Document, SignedDuration, std_duration};

/// Timeout for draining available incoming messages (short poll).
const DRAIN_TIMEOUT: Duration = std_duration(SignedDuration::from_millis(50));

/// Timeout for waiting until quiescent (no new messages).
const QUIESCENT_TIMEOUT: Duration = std_duration(SignedDuration::from_millis(200));

/// A connected stress-test client with its own CRDT document.
///
/// Wraps a [`SyncClient`] for protocol handling and a [`Document`]
/// for local CRDT state.
pub struct StressClient {
    inner: SyncClient,
    /// The local CRDT document.
    pub doc: Document,
    /// The registered agent id for this client.
    pub agent: kutl_core::AgentId,
    space_id: String,
    doc_id: String,
    /// Version at last send (for delta encoding).
    last_sent: Vec<usize>,
}

impl StressClient {
    /// Connect, handshake, and subscribe. Returns a ready-to-use client.
    pub async fn connect(
        relay_url: &str,
        client_name: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Self> {
        let mut inner = SyncClient::connect(relay_url, client_name).await?;

        let mut doc = Document::new();
        let agent = doc.register_agent(client_name)?;

        // Subscribe and apply any catch-up ops.
        if let Some(catch_up) = inner.subscribe(space_id, doc_id).await? {
            doc.merge(&catch_up.ops, &catch_up.metadata)?;
        }

        Ok(Self {
            inner,
            doc,
            agent,
            space_id: space_id.to_owned(),
            doc_id: doc_id.to_owned(),
            last_sent: Vec::new(),
        })
    }

    /// Insert text at the given character position and send ops to the relay.
    pub async fn insert(&mut self, pos: usize, text: &str) -> Result<()> {
        let agent = self.agent;
        self.doc.edit(
            agent,
            "stress",
            "insert",
            kutl_core::Boundary::Auto,
            |ctx| ctx.insert(pos, text),
        )?;
        self.send_pending_ops().await
    }

    /// Drain all available incoming messages, merging ops into our document.
    pub async fn drain_incoming(&mut self) -> Result<usize> {
        self.drain(DRAIN_TIMEOUT).await
    }

    /// Receive all incoming messages until quiescent (no new messages for 200ms).
    pub async fn drain_until_quiet(&mut self) -> Result<usize> {
        let mut count = 0;
        loop {
            match self.inner.recv_timeout(QUIESCENT_TIMEOUT).await? {
                Some(SyncEvent::Ops(ops)) => {
                    self.doc.merge(&ops.ops, &ops.metadata)?;
                    count += 1;
                }
                Some(_) => {}
                None => break,
            }
        }
        Ok(count)
    }

    /// Get the current document content.
    pub fn content(&self) -> String {
        self.doc.content()
    }

    /// Get document size in characters.
    pub fn doc_size(&self) -> usize {
        self.doc.len()
    }

    /// Number of auto-resubscribes triggered by stale subscriber evictions.
    pub fn resubscribe_count(&self) -> u32 {
        self.inner.resubscribe_count()
    }

    /// Close the underlying WebSocket connection.
    pub async fn close(self) -> Result<()> {
        self.inner.close().await
    }

    /// Encode and send any ops produced since the last send.
    async fn send_pending_ops(&mut self) -> Result<()> {
        let ops = self.doc.encode_since(&self.last_sent);
        if ops.is_empty() {
            return Ok(());
        }
        let metadata = self.doc.changes_since(&self.last_sent);
        self.last_sent = self.doc.local_version();

        self.inner
            .send_ops(&self.space_id, &self.doc_id, ops, metadata)
            .await
    }

    /// Drain incoming messages for up to `timeout`. Returns count of ops merged.
    async fn drain(&mut self, timeout: Duration) -> Result<usize> {
        let mut count = 0;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match self.inner.recv_timeout(remaining).await? {
                Some(SyncEvent::Ops(ops)) => {
                    self.doc.merge(&ops.ops, &ops.metadata)?;
                    count += 1;
                }
                Some(_) => {}
                None => break,
            }
        }
        Ok(count)
    }
}
