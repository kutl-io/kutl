//! Backend trait for querying changes in a space.
//!
//! Implementations query existing data rather than maintaining a parallel
//! event log. The OSS relay uses `SQLite`; kutlhub-relay uses Postgres.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::registry::RegistryEntry;

/// Response from [`ChangeBackend::get_changes`].
///
/// Structured as a map of arrays — each section is always present but may
/// be empty depending on what changed and the backend's capabilities.
#[derive(Debug, Default, Serialize)]
pub struct ChangesResponse {
    /// Signals since last check. Uses the proto type directly
    /// (serde derives added to kutl-proto via prost-build config).
    pub signals: Vec<kutl_proto::sync::Signal>,
    /// Documents registered, renamed, or deleted since last check.
    /// Uses the existing [`RegistryEntry`] type from the document registry.
    pub document_changes: Vec<RegistryEntry>,
    /// Opaque checkpoint for failure recovery. The agent can pass this back
    /// on the next call to replay from this point if a previous call failed.
    pub checkpoint: String,
}

/// Errors from change backend operations.
#[derive(Debug, thiserror::Error)]
pub enum ChangeError {
    /// A database-level error occurred.
    #[error("database error: {0}")]
    Db(String),
    /// The requested resource was not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// An unexpected internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<sqlx::Error> for ChangeError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e.to_string())
    }
}

/// Full signal detail returned by [`ChangeBackend::get_signal_detail`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalDetail {
    /// Signal identifier.
    pub id: String,
    /// Space the signal belongs to.
    pub space_id: String,
    /// Document the signal is attached to, if any.
    pub document_id: Option<String>,
    /// DID of the signal author.
    pub author_did: String,
    /// Signal type discriminator (e.g. `"flag"`, `"chat"`, `"reply"`).
    pub signal_type: String,
    /// Creation time as milliseconds since epoch.
    pub timestamp: i64,
    /// Flag kind string, for flag signals.
    pub flag_kind: Option<String>,
    /// Audience scope, for flag signals.
    pub audience: Option<String>,
    /// Target DID, for directed signals.
    pub target_did: Option<String>,
    /// Human-readable message, for flag signals.
    pub message: Option<String>,
    /// Close time as milliseconds since epoch, if the signal has been closed.
    pub closed_at: Option<i64>,
    /// Topic string, for chat signals.
    pub topic: Option<String>,
    /// Parent signal identifier, for reply signals.
    pub parent_signal_id: Option<String>,
    /// Parent reply identifier, for nested replies.
    pub parent_reply_id: Option<String>,
    /// Body text, for reply signals.
    pub body: Option<String>,
    /// Nested replies for the detail view.
    pub replies: Vec<SignalReplyDetail>,
    /// Nested reactions for the detail view.
    pub reactions: Vec<SignalReactionDetail>,
}

/// A reply in a signal detail response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalReplyDetail {
    /// Reply identifier.
    pub id: String,
    /// Parent reply identifier, for nested replies.
    pub parent_reply_id: Option<String>,
    /// DID of the reply author.
    pub author_did: String,
    /// Body text of the reply.
    pub body: String,
    /// Creation time as milliseconds since epoch.
    pub created_at: i64,
}

/// A reaction in a signal detail response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalReactionDetail {
    /// DID of the actor who reacted.
    pub actor_did: String,
    /// Emoji string for the reaction.
    pub emoji: String,
    /// Creation time as milliseconds since epoch.
    pub created_at: i64,
}

/// Parse a checkpoint string of the form `"{signal_cursor}:{reg_cursor}"`.
pub fn parse_checkpoint(cp: &str) -> Option<(i64, i64)> {
    let (a, r) = cp.split_once(':')?;
    Some((a.parse().ok()?, r.parse().ok()?))
}

/// Resolve cursor positions, handling checkpoint-based rewind.
///
/// Given the current cursor values and the checkpoint ring (prev1, prev2),
/// determines which cursors to use based on the optional checkpoint parameter.
pub fn resolve_cursors(
    checkpoint: Option<&str>,
    current: (i64, i64),
    prev1: (i64, i64),
    prev2: (i64, i64),
    stored_checkpoint: &str,
) -> (i64, i64) {
    let Some(cp) = checkpoint else {
        return current;
    };
    let Some(parsed) = parse_checkpoint(cp) else {
        return current;
    };

    // Match against stored checkpoint string.
    if parse_checkpoint(stored_checkpoint) == Some(parsed) {
        return parsed;
    }
    // Match against prev1 ring slot.
    if prev1 == parsed {
        return parsed;
    }
    // Match against prev2 ring slot.
    if prev2 == parsed {
        return parsed;
    }

    current
}

/// Backend for querying changes in a space.
///
/// Implementations query existing data (signals, document registry)
/// rather than maintaining a parallel event log.
#[async_trait]
pub trait ChangeBackend: Send + Sync {
    /// Get changes since this DID's last check for the given space.
    ///
    /// Auto-advances the cursor for the next call. If `checkpoint` is
    /// provided, rewinds to that position first for failure recovery.
    async fn get_changes(
        &self,
        did: &str,
        space_id: &str,
        checkpoint: Option<&str>,
    ) -> Result<ChangesResponse, ChangeError>;

    /// Record a new signal (flag, chat, or reply).
    ///
    /// `id` is a relay-generated UUID string that becomes the canonical identifier
    /// for this signal. Both backends must INSERT it explicitly so the MCP tool
    /// and `get_changes` proto can return a stable id to callers.
    #[allow(clippy::too_many_arguments)]
    async fn record_signal(
        &self,
        id: &str,
        space_id: &str,
        document_id: Option<&str>,
        author_did: &str,
        signal_type: &str,
        timestamp: i64,
        flag_kind: Option<i32>,
        audience: Option<i32>,
        message: Option<&str>,
        target_did: Option<&str>,
        topic: Option<&str>,
        parent_signal_id: Option<&str>,
        parent_reply_id: Option<&str>,
        body: Option<&str>,
    ) -> Result<(), ChangeError>;

    /// Fetch full signal detail with replies and reactions.
    async fn get_signal_detail(
        &self,
        space_id: &str,
        signal_id: &str,
    ) -> Result<SignalDetail, ChangeError>;

    /// Prune data older than the retention window.
    ///
    /// Called periodically by the relay's reaper task. Implementations define
    /// their own retention policy. Returns the number of rows pruned.
    async fn prune(&self, now_ms: i64) -> Result<u64, ChangeError>;
}
