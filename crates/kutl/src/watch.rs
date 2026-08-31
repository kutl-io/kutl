//! Stdio MCP server for `kutl mcp serve`.
//!
//! Reads JSON-RPC 2.0 requests from stdin, handles them, and writes
//! responses to stdout. Pushes channel notifications from the relay
//! connection as signals (flags) arrive.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use kutl_client::SpaceConfig;
use kutl_proto::protocol::{
    decode_envelope, encode_envelope, handshake_envelope_with_token, subscribe_signals_envelope,
};
use kutl_proto::sync::{self, sync_envelope::Payload};
use kutl_relay::mcp::{
    INVALID_PARAMS, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, MCP_PROTOCOL_VERSION,
    METHOD_NOT_FOUND, PARSE_ERROR, ToolCallParams, ToolCallResult, ToolDefinition,
};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite;
use tokio_util::sync::CancellationToken;

use crate::watch_tools::RelayProxy;

/// Capacity of the internal notification channel.
const NOTIFY_CHANNEL_CAPACITY: usize = 64;

/// Capacity of the event broadcast. A subscriber that falls this far behind
/// is told it lagged and resynchronises from the relay rather than silently
/// missing activity.
const EVENT_BROADCAST_CAPACITY: usize = 64;

/// Capacity of the queue feeding the single stdout writer. Bounds how many
/// serialized frames can be in flight when the reader on the other end of the
/// pipe is slow.
const OUTGOING_FRAME_CAPACITY: usize = 64;

/// How long shutdown waits for requests already being handled.
///
/// Requests are handled off the event loop, so EOF on stdin can arrive while a
/// response is still being produced — a caller that writes one frame and
/// closes would otherwise be answered with silence. Bounded because a caller
/// blocked in `wait_for_changes` has no answer coming and must not hold the
/// session open until its own deadline.
const SHUTDOWN_DRAIN: std::time::Duration = std::time::Duration::from_secs(5);

/// Delay between relay reconnection attempts.
const RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// Maximum consecutive reconnection failures before giving up.
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// A session failure that repeats identically on every retry: a rejected
/// credential, or a protocol major this client/relay pairing cannot
/// bridge. The reconnect ladder stops on these immediately and surfaces
/// the message as the remedy — retrying a deterministic refusal burns the
/// budget without ever telling the user what to fix.
#[derive(Debug)]
struct TerminalSessionError(String);

impl std::fmt::Display for TerminalSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TerminalSessionError {}

/// Initial retry delay for relay proxy reconnection.
const PROXY_INITIAL_RETRY: std::time::Duration = std::time::Duration::from_secs(5);

/// Maximum retry delay for relay proxy (exponential backoff cap).
const PROXY_MAX_RETRY: std::time::Duration = std::time::Duration::from_mins(5);

/// Number of initial connection attempts when resolving the relay proxy at startup.
const INITIAL_PROXY_CONNECT_ATTEMPTS: u32 = 3;

/// Delay between initial relay proxy connection attempts at startup.
const INITIAL_PROXY_CONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Instructions text sent to the agent during MCP initialization.
///
/// Composition: the operational briefing, then the KFM dialect spec —
/// the same `oss/docs/kutl-markdown.md` the relay concatenates onto its
/// own instructions and `kutl init` writes into AGENTS.md. An MCP-only
/// agent reads no AGENTS.md, so this string is the only path by which
/// the dialect reaches it, and sharing the file is what keeps the three
/// surfaces from teaching three subtly different grammars.
pub(crate) const SERVER_INSTRUCTIONS: &str = concat!(
    "\
kutl is a collaborative document editor. Participants edit markdown documents \
together in real-time within shared spaces.\n\n\
## How to discover work\n\n\
Three ways, in the order worth reaching for:\n\
- **Wait (block):** Call `wait_for_changes` with your space_id. It returns \
  the moment a signal reaches you or someone edits a document, and answers \
  empty if nothing happens before `timeout_seconds`. This is how to stay with \
  a piece of work that depends on other participants: waiting costs nothing, \
  and you are still there when they answer.\n\
- **Poll (pull):** Call `get_changes` with a space_id for what has happened \
  since your last check. You receive flags addressed to you and flags \
  addressed to the space, never one naming a different participant; replies, \
  chats and decisions name nobody, so every one of them in the space reaches \
  you — including on threads you have not spoken in. Use it when you want a \
  snapshot now rather than the next thing to happen.\n\
- **Channel events (push):** Some clients deliver activity as unsolicited \
  channel events. Two shapes ride the same notification, told apart by their \
  metadata. A signal: content is the flag's message or the reply's body, and \
  the metadata carries `space_id`, `space_name`, `record` \
  (flag/reply/chat/decision), `document` (the document id, empty when the \
  record hangs off no document) and `sender` (the author's DID), plus `kind` \
  (info/question/etc.) on a flag and `parent_signal_id` on a reply. A document \
  change: content is a one-line summary, and the metadata carries \
  `type` (`document_changed`), `space_id`, `document_id`, `author_did` and \
  `intent` — no `record` and no `sender`. Not every client acts on either, so \
  do not rely on one arriving — `wait_for_changes` covers the same activity \
  and works everywhere.\n\n\
Keep calling one of the first two for as long as you work — not only when you \
are blocked. Messages arrive while you work, and nothing delivers them: you \
are the only one who will go and get them. A message you never asked for is a \
message you never saw, and re-reading the document will not show it to you. \
Check between steps. An agent that queries once at the start and then works \
straight through finishes without ever learning what was said to it.\n\n\
Work that depends on someone else is not finished when you have said your \
part. If a decision needs another participant, wait for them rather than \
ending your turn — once you stop, nothing will wake you.\n\n\
Participants address you directly, too. A flag naming you is a question with \
your name on it, and it travels the signal stream, not the document body. \
Answering is part of the work, not a courtesy: reply with `create_reply` on \
that flag's signal_id. Someone who addressed you and heard nothing cannot \
tell whether you disagreed or never looked.\n\n\
To reach a space other than the one below, `list_spaces` shows the spaces \
registered on this machine and `subscribe_space` attaches to one. Registered \
is not the same as permitted: whether you may read or write a space is settled \
by the relay when you call a tool against it.\n\n\
## Decisions\n\n\
Decisions are `## ?` / `## =` headings — the dialect reference below \
carries the grammar and the rules for keeping a decision's identity \
across a resolve. Through this server, `close_flag` (reason `resolved`) \
performs the marker flip for you, safely.\n\n\
## Working in documents\n\n\
Read with `read_document`, then edit with `edit_document`, passing back the \
`version` the read gave you as `base_version` along with the full content you \
want. The relay compares your content against the text you read, so it applies \
only what YOU changed and keeps whatever others added meanwhile.\n\n\
Give a short `intent` describing your edit — it is what other participants see \
when they are notified of it.\n\n\
If a participant rewrote a region you also changed, or that region now sits in \
more than one place, the region comes back in `hunks_refused` and the rest of \
your edit lands. A refused region is not a failed edit: read the document again \
and reapply that region. Changes close together in the text travel as one \
region, so contesting one of them refuses both.\n\n\
The result carries no version. Every edit starts with a read — content you did \
not read is content you would delete.\n\n",
    "---\n\n",
    include_str!("../../../docs/kutl-markdown.md"),
);

/// Appended to [`SERVER_INSTRUCTIONS`] when the local space's root sits
/// inside a git repository. AGENTS.md normally carries this contract,
/// but an MCP-only agent never reads AGENTS.md — this is the only path
/// by which it reaches one.
const GIT_SURFACE_INSTRUCTIONS: &str = concat!(
    "\n\n",
    "## Working in a git-wrapped space\n\n",
    "This space's files are mirrored into the surrounding git tree by `kutl ",
    "surface`. The canonical copies live in the space; the git tree only ",
    "sees a mirror. A file whose first line reads:\n\n",
    "```\n",
    kutl_client::surface_sentinel_header!(),
    "\n```\n\n",
    "is that mirror. Direct edits to it are overwritten the next time `kutl ",
    "surface` runs. Edit through the space instead: for you, that means the ",
    "kutl document tools, not the git working tree.\n\n",
    "`kutl surface` is a logical-commit boundary, not an edit-time hook: run ",
    "it once when a chunk of work is done, then commit.",
);

/// A channel notification to push to the connected agent.
pub struct ChannelEvent {
    /// Human-readable event content.
    pub content: String,
    /// Structured metadata about the event.
    pub meta: serde_json::Map<String, Value>,
}

/// Something that happened in a subscribed space, from either of the two
/// sources this server watches: signals off the relay's WebSocket lane and
/// document changes off its notification stream.
///
/// One type for both so a consumer that waits for activity cannot accidentally
/// serve half of it. An agent woken by flags but never by a peer's edit looks
/// like it works while missing the half of a collaboration where someone
/// writes their contribution down.
#[derive(Clone, Debug)]
enum AgentEvent {
    /// A flag delivered to this agent.
    Signal {
        /// The flag's message.
        content: String,
        /// Structured metadata; carries the author under `sender`.
        meta: serde_json::Map<String, Value>,
    },
    /// A participant registered, renamed, deleted, or edited a document.
    DocumentChanged {
        /// Space the change belongs to.
        space_id: String,
        /// Document that changed.
        document_id: String,
        /// DID of the author that produced the change.
        author_did: String,
        /// Lifecycle intent string from the relay.
        intent: String,
    },
}

impl AgentEvent {
    /// DID of whoever caused the event, when the source records one.
    ///
    /// The two sources spell it differently — a flag keeps it in its metadata
    /// map, a document change in a field — so every caller that has to
    /// recognize its own activity reads it through here.
    fn author_did(&self) -> Option<&str> {
        match self {
            Self::Signal { meta, .. } => meta.get("sender").and_then(Value::as_str),
            Self::DocumentChanged { author_did, .. } => Some(author_did.as_str()),
        }
    }

    /// Space the event happened in, when the source records one.
    ///
    /// Same split as [`Self::author_did`]: a flag keeps it in its metadata
    /// map, a document change in a field. Read through here by anything that
    /// has to tell one space's activity from another's — a caller blocked on
    /// one space and woken by another reads the empty answer that follows as
    /// its own wait expiring, and stops waiting.
    fn space_id(&self) -> Option<&str> {
        match self {
            Self::Signal { meta, .. } => meta.get("space_id").and_then(Value::as_str),
            Self::DocumentChanged { space_id, .. } => Some(space_id.as_str()),
        }
    }

    /// The `claude/channel` notification frame for this event.
    fn channel_notification(&self) -> JsonRpcNotification {
        let params = match self {
            Self::Signal { content, meta } => serde_json::json!({
                "content": content,
                "meta": meta,
            }),
            Self::DocumentChanged {
                space_id,
                document_id,
                author_did,
                intent,
            } => serde_json::json!({
                "content": format!(
                    "{} edited {document_id} ({intent})",
                    author_did.rsplit(':').next().unwrap_or("?"),
                ),
                "meta": {
                    "type": "document_changed",
                    "space_id": space_id,
                    "document_id": document_id,
                    "author_did": author_did,
                    "intent": intent,
                },
            }),
        };
        JsonRpcNotification {
            jsonrpc: "2.0".into(),
            method: "notifications/claude/channel".into(),
            params,
        }
    }
}

impl From<crate::watch_tools::DocumentChangedParams> for AgentEvent {
    fn from(e: crate::watch_tools::DocumentChangedParams) -> Self {
        Self::DocumentChanged {
            space_id: e.space_id,
            document_id: e.document_id,
            author_did: e.author_did,
            intent: e.intent,
        }
    }
}

impl From<ChannelEvent> for AgentEvent {
    fn from(e: ChannelEvent) -> Self {
        Self::Signal {
            content: e.content,
            meta: e.meta,
        }
    }
}

/// The space this server subscribed to at startup, as the agent needs to know
/// it.
///
/// Every space-scoped tool takes a `space_id`, and an agent reaching the
/// server over stdio sees neither the working directory it was launched in nor
/// anything the process writes to stderr. Told nothing, it has no first move.
#[derive(Clone, Debug)]
struct SpaceHint {
    /// Canonical id — what the tools take.
    id: String,
    /// Human-readable name, for talking about the space.
    name: String,
}

/// A subscription to a single space on a relay.
///
/// Holds the cancellation token used to shut down the relay listener
/// task when the space is unsubscribed.
struct SpaceSubscription {
    /// Token to cancel the relay listener task.
    cancel: CancellationToken,
}

/// Mutable session state shared between the main loop and tool handlers.
struct WatchState {
    /// Active relay subscriptions, keyed by canonical `space_id`.
    ///
    /// The key is always the canonical `SpaceConfig::space_id` (never the
    /// human-readable name), so that subscribing by name and by id resolve to
    /// the same entry. Auto-subscribe and the tool handlers must agree on this
    /// key or a space could be listened to twice (every flag delivered twice)
    /// and unsubscribe-by-name would miss.
    subscriptions: HashMap<String, SpaceSubscription>,
    /// Agent DID for flag targeting, filtering, and authz-error enrichment.
    did: String,
    /// Auth token for relay handshakes.
    auth_token: String,
    /// Sender for pushing channel events to stdout.
    notify_tx: mpsc::Sender<ChannelEvent>,
    /// Whether the local space's root sits inside a git repository.
    /// Computed once at startup; `false` when no local space was found.
    /// Drives whether `initialize` appends the git-surface contract to
    /// its instructions.
    in_git_repo: bool,
    /// The space auto-subscribed at startup, named in the `initialize`
    /// instructions so the agent has somewhere to start. `None` when no local
    /// space was found.
    space_hint: Option<SpaceHint>,
}

/// Startup state returned by [`setup`].
struct SetupResult {
    state: WatchState,
    local_space: Option<(SpaceConfig, std::path::PathBuf)>,
    relay_proxy: Option<RelayProxy>,
    relay_tools: Vec<ToolDefinition>,
    auth_token: String,
    notify_rx: mpsc::Receiver<ChannelEvent>,
    doc_stream: Option<crate::watch_tools::NotificationStream>,
}

/// The agent principal `mcp serve` authenticates as: the agent's
/// own `did:key` [`Identity`](kutl_client::Identity) and the bearer token minted
/// by the did:key challenge-response flow against the space's relay.
///
/// Distinct from the human `identity.json`. The DID drives flag targeting,
/// filtering, and authz-error enrichment; the token is used for relay handshakes
/// and the proxy lane. Cloneable (the identity is behind an `Arc`).
#[derive(Clone)]
pub struct AgentContext {
    /// The agent's own identity (signing key + did).
    identity: std::sync::Arc<kutl_client::Identity>,
    /// The agent-bound bearer token from the did:key challenge flow (empty when
    /// there is no local space/relay to authenticate against).
    token: String,
}

/// Load the agent key and authenticate as the agent against
/// `local_space`'s relay. Returns the agent identity + its
/// agent-bound bearer token.
///
/// With no local space there is no relay to authenticate against, so the token
/// is empty; the identity is still loaded (its did drives flag targeting and,
/// once a space is subscribed, authoring).
///
/// # Errors
///
/// Returns an error if the agent keyfile is absent/unreadable, or the did:key
/// challenge flow against the relay fails.
async fn build_agent_context(
    agent_name: &str,
    local_space: Option<&SpaceConfig>,
) -> Result<AgentContext> {
    let identity = load_agent_identity(agent_name)?;
    eprintln!(
        "kutl mcp: agent identity loaded ({}, agent {agent_name})",
        identity.did
    );

    let token = match local_space {
        Some(config) => {
            let signing_key = identity
                .decode_signing_key()
                .context("decoding the agent signing key for relay authentication")?;
            kutl_client::authenticate(&config.relay_url, &identity.did, &signing_key)
                .await
                .context("authenticating as the agent against the relay")?
        }
        None => String::new(),
    };

    Ok(AgentContext {
        identity: std::sync::Arc::new(identity),
        token,
    })
}

/// Discover space, connect relay, subscribe to events.
async fn setup(agent_name: &str) -> Result<SetupResult> {
    let (notify_tx, notify_rx) = mpsc::channel::<ChannelEvent>(NOTIFY_CHANNEL_CAPACITY);

    let search_root = std::env::var("KUTL_HOME").map_or_else(
        |_| std::env::current_dir().expect("failed to get working directory"),
        std::path::PathBuf::from,
    );
    let local_space = kutl_client::space_config::discover_space(&search_root);

    // Load the tool-held agent key and authenticate as the agent against the
    // served space's relay. The ENTIRE session is the agent principal:
    // its did drives flag targeting/filtering, and its token both the
    // read-only proxy handshakes and the authoring writes — the relay authors
    // and attests records under that DID; the key itself only
    // signs the auth challenge, never records.
    let agent = build_agent_context(agent_name, local_space.as_ref().map(|(c, _)| c)).await?;
    let did = agent.identity.did.clone();
    let auth_token = agent.token.clone();

    let in_git_repo = local_space
        .as_ref()
        .is_some_and(|(_, space_root)| kutl_client::find_git_repo_root(space_root).is_some());

    let mut state = WatchState {
        subscriptions: HashMap::new(),
        did: did.clone(),
        auth_token: auth_token.clone(),
        notify_tx,
        in_git_repo,
        space_hint: None,
    };

    if let Some((ref config, ref space_root)) = local_space {
        let display_name = kutl_client::KutlspaceConfig::display_name(space_root, &config.space_id);
        eprintln!(
            "kutl mcp: auto-subscribed to space {} ({})",
            display_name, config.space_id
        );
        // The same fact the line above tells the operator, kept where the
        // handshake can tell the agent. Only stderr reaches the operator; only
        // the handshake reaches the agent.
        state.space_hint = Some(SpaceHint {
            id: config.space_id.clone(),
            name: display_name.clone(),
        });

        let cancel = CancellationToken::new();
        spawn_relay_listener(
            config,
            space_root,
            &did,
            &auth_token,
            state.notify_tx.clone(),
            cancel.clone(),
        );

        state
            .subscriptions
            .insert(config.space_id.clone(), SpaceSubscription { cancel });
    } else {
        eprintln!("kutl mcp: no local space found. Use list_spaces and subscribe_space tools.");
    }

    let local_space_ref = local_space.as_ref().map(|(c, _)| c);

    let (relay_proxy, relay_tools) =
        connect_relay_proxy_with_retry(local_space_ref, &auth_token, &did).await;

    if local_space_ref.is_some() && relay_proxy.is_none() {
        anyhow::bail!(
            "failed to connect to relay MCP endpoint after 3 attempts. \
             check that the relay is running and reachable"
        );
    }

    // Document-change notifications from the relay. Always subscribed: both
    // the push emitter and the blocking read consume these, and a server that
    // is not subscribed can answer neither.
    let doc_stream = relay_proxy
        .as_ref()
        .map(RelayProxy::subscribe_notifications);

    Ok(SetupResult {
        state,
        local_space,
        relay_proxy,
        relay_tools,
        auth_token,
        notify_rx,
        doc_stream,
    })
}

/// Receive from an optional doc-change channel (for `tokio::select!`).
async fn recv_doc_change(
    rx: &mut Option<mpsc::Receiver<crate::watch_tools::DocumentChangedParams>>,
) -> Option<crate::watch_tools::DocumentChangedParams> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Own stdout for the session, writing one newline-delimited frame at a time.
///
/// Requests are handled concurrently, so without a single writer two handlers
/// could interleave halves of their frames into one unparseable line.
async fn stdout_writer(mut frames: mpsc::Receiver<String>) {
    let mut stdout = tokio::io::stdout();
    while let Some(frame) = frames.recv().await {
        if stdout.write_all(frame.as_bytes()).await.is_err()
            || stdout.write_all(b"\n").await.is_err()
            || stdout.flush().await.is_err()
        {
            // The far end of the pipe is gone. Nothing left to say.
            break;
        }
    }
}

/// Serialize `value` and hand it to the stdout writer as one frame.
async fn send_json_frame<T: serde::Serialize>(
    frames: &mpsc::Sender<String>,
    value: &T,
    ctx: &'static str,
) {
    match serde_json::to_string(value) {
        // A closed writer means stdout is gone. Not reported here: the main
        // loop reaches EOF on stdin and shuts the session down on its own.
        Ok(json) => drop(frames.send(json).await),
        Err(e) => eprintln!("kutl mcp: {ctx}: {e}"),
    }
}

/// Publish an event to everything that cares: the blocking readers waiting on
/// the broadcast, then the push frame on stdout.
async fn publish_event(
    events: &broadcast::Sender<AgentEvent>,
    frames: &mpsc::Sender<String>,
    event: AgentEvent,
) {
    let notification = event.channel_notification();
    // No subscribers is the ordinary case — nothing is blocked in
    // `wait_for_changes` right now. The push frame below still goes out.
    drop(events.send(event));
    send_json_frame(frames, &notification, "failed to serialize notification").await;
}

/// Everything a spawned request handler needs to answer one request.
///
/// Cheap to clone per request: the tool list and proxy sit behind `Arc`s that
/// the reconnect path swaps wholesale, and the mutable session state behind a
/// mutex no handler holds across an await.
#[derive(Clone)]
struct RequestContext {
    /// Tool definitions fetched from the relay.
    relay_tools: Arc<Vec<ToolDefinition>>,
    /// The relay MCP session, when connected.
    relay_proxy: Option<Arc<RelayProxy>>,
    /// Session state. `None` in unit tests, where nothing mutates it.
    state: Option<Arc<Mutex<WatchState>>>,
    /// Event broadcast the blocking read subscribes to.
    events: broadcast::Sender<AgentEvent>,
}

impl RequestContext {
    /// Borrow the session state.
    ///
    /// Poisoning means a handler panicked mid-mutation. The guarded map decides
    /// which relay listeners exist, and a wrong answer there means either
    /// silence or every flag delivered twice, so the session does not continue
    /// past it.
    fn lock_state(&self) -> Option<std::sync::MutexGuard<'_, WatchState>> {
        self.state
            .as_ref()
            .map(|s| s.lock().expect("watch state lock poisoned"))
    }

    /// The agent's own DID, cloned so no lock is held across an await.
    fn did(&self) -> Option<String> {
        self.lock_state().map(|s| s.did.clone())
    }
}

/// Run the `kutl mcp serve` MCP server.
///
/// `agent_name` selects the tool-held agent key: the whole session
/// authenticates and authors as that agent principal.
pub async fn run(agent_name: &str) -> Result<()> {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    let SetupResult {
        state,
        local_space: local_space_with_root,
        relay_proxy,
        relay_tools,
        auth_token,
        mut notify_rx,
        doc_stream,
    } = setup(agent_name).await?;

    let local_space_ref = local_space_with_root.as_ref().map(|(c, _)| c);
    let did = state.did.clone();
    let state = Arc::new(Mutex::new(state));
    let relay_proxy = relay_proxy.map(Arc::new);

    let (frames_tx, frames_rx) = mpsc::channel::<String>(OUTGOING_FRAME_CAPACITY);
    let writer = tokio::spawn(stdout_writer(frames_rx));
    let (events_tx, _) = broadcast::channel::<AgentEvent>(EVENT_BROADCAST_CAPACITY);
    let mut handlers = tokio::task::JoinSet::new();

    let mut relay = RelayLink {
        next_retry_at: relay_proxy
            .is_none()
            .then(|| tokio::time::Instant::now() + PROXY_INITIAL_RETRY),
        proxy: relay_proxy,
        tools: Arc::new(relay_tools),
        doc_changes: None,
        doc_stream_established: None,
        retry_delay: PROXY_INITIAL_RETRY,
        next_stream_retry_at: None,
        stream_retry_delay: PROXY_INITIAL_RETRY,
    };
    if let Some(stream) = doc_stream {
        relay.hold_stream(stream);
    }

    loop {
        // Recomputed every iteration: each arm below can arm, advance, or
        // clear a ladder, and the sleep must follow it.
        let retry_at = relay.retry_deadline();
        tokio::select! {
            // A due retry fires on its own clock, not on the next request:
            // an agent parked in a blocking read sends nothing, and the arm
            // below that re-subscribes on stdin traffic never runs for it.
            () = tokio::time::sleep_until(retry_at.unwrap_or_else(far_future)),
                if retry_at.is_some() =>
            {
                maybe_reconnect_proxy(&mut relay, local_space_ref, &auth_token, &did).await;
            }
            line = lines.next_line() => {
                let Some(line) = line.context("stdin read error")? else {
                    break; // EOF — the agent closed the session
                };
                if line.trim().is_empty() {
                    continue;
                }
                maybe_reconnect_proxy(&mut relay, local_space_ref, &auth_token, &did).await;
                // Handled off the loop: a request that waits for an event must
                // not stop the loop that delivers it. Responses carry their
                // request's id, so completing out of order is well-formed.
                let ctx = RequestContext {
                    relay_tools: Arc::clone(&relay.tools),
                    relay_proxy: relay.proxy.clone(),
                    state: Some(Arc::clone(&state)),
                    events: events_tx.clone(),
                };
                let frames = frames_tx.clone();
                handlers.spawn(async move {
                    if let Some(resp) = handle_request(&line, &ctx).await {
                        send_json_frame(&frames, &resp, "failed to serialize response").await;
                    }
                });
            }
            Some(event) = notify_rx.recv() => {
                publish_event(&events_tx, &frames_tx, event.into()).await;
            }
            doc_event = recv_doc_change(&mut relay.doc_changes) => {
                match doc_event {
                    Some(event) => publish_event(&events_tx, &frames_tx, event.into()).await,
                    // The stream ended. Clearing the receiver is what stops
                    // this arm re-firing: `recv_doc_change` parks forever on a
                    // `None` receiver. Recovery is the retry arm above — it
                    // sleeps on the ladder this call arms, so the stream comes
                    // back on its own clock even if no request ever arrives.
                    None => relay.stream_ended(),
                }
            }
        }
    }

    // Let requests already in flight finish and reach stdout. Stragglers past
    // the grace period are abandoned when the set drops.
    let _ = tokio::time::timeout(SHUTDOWN_DRAIN, async {
        while handlers.join_next().await.is_some() {}
    })
    .await;
    drop(handlers);

    // Dropping every sender is what tells the writer there is nothing more;
    // it then finishes the queue and returns.
    drop(frames_tx);
    let _ = writer.await;

    // Cancel all relay listener tasks on exit.
    let subscriptions = std::mem::take(
        &mut state
            .lock()
            .expect("watch state lock poisoned")
            .subscriptions,
    );
    for (_, sub) in subscriptions {
        sub.cancel.cancel();
    }

    Ok(())
}

/// A stand-in instant for a `sleep_until` whose select-arm guard is false —
/// never awaited, but `tokio::select!` still constructs the future. Far
/// enough out that even a pathological guard slip only oversleeps.
fn far_future() -> tokio::time::Instant {
    /// One day, in seconds.
    const ONE_DAY_SECS: u64 = 86_400;
    tokio::time::Instant::now() + std::time::Duration::from_secs(ONE_DAY_SECS)
}

/// The default agent name when neither `--agent` nor `KUTL_AGENT` selects one.
const DEFAULT_AGENT_NAME: &str = "default";

/// Resolve the agent name for `mcp serve` from the precedence chain
/// `--agent <name>` (`explicit`) -> `KUTL_AGENT` env -> [`DEFAULT_AGENT_NAME`].
///
/// The agent is the tool-held principal: `mcp serve` authenticates
/// with THIS agent's key — cryptographically distinct from the human
/// `identity.json` — and the relay authors signal records under the agent's
/// DID.
pub fn resolve_agent_name(explicit: Option<&str>) -> String {
    explicit
        .map(str::to_owned)
        .or_else(|| std::env::var("KUTL_AGENT").ok())
        .unwrap_or_else(|| DEFAULT_AGENT_NAME.to_owned())
}

/// Load the agent [`Identity`](kutl_client::Identity) for `name` from
/// `agent_identity_path(name)` (`$KUTL_HOME/agents/<name>.json`).
///
/// The keyfile is provisioned by `kutl agent create --name <name>`; the error
/// carries that hint when the keyfile is absent so the operator knows how to fix
/// it.
///
/// # Errors
///
/// Returns an error if `name` is malformed (path separators / traversal), or the
/// keyfile is missing or unreadable.
fn load_agent_identity(name: &str) -> Result<kutl_client::Identity> {
    let path = kutl_client::agent_identity_path(name)
        .with_context(|| format!("resolving the agent keyfile path for {name}"))?;
    if !path.exists() {
        anyhow::bail!(
            "no agent keyfile at {} — run `kutl agent create --name {name}` first",
            path.display()
        );
    }
    kutl_client::Identity::load(&path)
        .with_context(|| format!("loading the agent keyfile at {}", path.display()))
}

/// Connect a relay proxy for tool dispatch, if a local space is available.
///
/// Returns `(Some(proxy), tools)` on success, `(None, [])` if no space
/// is configured or the relay is unreachable.
async fn connect_relay_proxy(
    local_space: Option<&SpaceConfig>,
    auth_token: &str,
    did: &str,
) -> (Option<RelayProxy>, Vec<ToolDefinition>) {
    let Some(config) = local_space else {
        return (None, Vec::new());
    };

    match RelayProxy::connect(&config.relay_url, auth_token, did).await {
        Ok((proxy, tools)) => {
            eprintln!(
                "kutl mcp: relay proxy connected ({} tools available)",
                tools.len()
            );
            (Some(proxy), tools)
        }
        Err(e) => {
            eprintln!(
                "kutl mcp: relay proxy unavailable: {e:#}. \
                 Relay tools (get_changes, read_document, etc.) will not be \
                 available. Will retry on next tool call."
            );
            (None, Vec::new())
        }
    }
}

/// Connect the relay proxy with startup retries.
///
/// The relay may still be initializing when `kutl mcp serve` starts. Retry up
/// to 3 times with a 2-second delay to avoid agents starting with zero tools.
async fn connect_relay_proxy_with_retry(
    local_space: Option<&SpaceConfig>,
    auth_token: &str,
    did: &str,
) -> (Option<RelayProxy>, Vec<ToolDefinition>) {
    let mut result = connect_relay_proxy(local_space, auth_token, did).await;
    for attempt in 1..=INITIAL_PROXY_CONNECT_ATTEMPTS {
        if result.0.is_some() {
            break;
        }
        eprintln!(
            "kutl mcp: relay proxy not ready, retry {attempt}/{max} in {delay}s...",
            max = INITIAL_PROXY_CONNECT_ATTEMPTS,
            delay = INITIAL_PROXY_CONNECT_DELAY.as_secs(),
        );
        tokio::time::sleep(INITIAL_PROXY_CONNECT_DELAY).await;
        result = connect_relay_proxy(local_space, auth_token, did).await;
    }
    result
}

/// Everything that lives and dies with one relay MCP session.
///
/// Grouped because they change together and only together: a session's tool
/// list, its notification stream, and its place in the reconnect ladder are
/// all invalidated the moment the session is. Held apart, a path can update
/// some and miss others, and the failure that produces — a stream still
/// pointing at a dead session — is silent rather than loud.
struct RelayLink {
    /// The relay MCP session, when connected.
    proxy: Option<Arc<RelayProxy>>,
    /// Tool definitions fetched from that session.
    tools: Arc<Vec<ToolDefinition>>,
    /// Document-change notifications off that session's SSE stream.
    doc_changes: Option<mpsc::Receiver<crate::watch_tools::DocumentChangedParams>>,
    /// Whether the relay ever accepted the stream `doc_changes` reads. Held
    /// alongside the receiver because a closed channel alone cannot say
    /// whether the session died or the stream never opened.
    doc_stream_established: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// When the next reconnect may be attempted. `None` while connected.
    next_retry_at: Option<tokio::time::Instant>,
    /// Backoff for the next FAILED attempt (5s -> 10s -> ... -> 5min cap).
    retry_delay: std::time::Duration,
    /// When the notification stream alone may be re-subscribed, for a session
    /// whose tool lane is healthy and whose stream the relay would not open.
    /// `None` while a stream is in hand.
    next_stream_retry_at: Option<tokio::time::Instant>,
    /// Backoff for the next failed STREAM subscribe, on the same ladder as
    /// [`Self::retry_delay`] and kept separately so a refused stream does not
    /// pace the session's own reconnects.
    stream_retry_delay: std::time::Duration,
}

impl RelayLink {
    /// Take the stream a fresh subscribe produced.
    fn hold_stream(&mut self, stream: crate::watch_tools::NotificationStream) {
        self.doc_stream_established = Some(Arc::clone(&stream.established));
        self.doc_changes = Some(stream.events);
        self.next_stream_retry_at = None;
    }

    /// The document-change stream closed. Decide what that says about the
    /// session behind it.
    ///
    /// A stream that was LIVE and then ended says the session is gone — the
    /// commonest cause being the relay reaping it after its idle TTL. The
    /// session id is immutable, so every later call on that proxy fails the
    /// same way forever and only a new session recovers.
    ///
    /// A stream that NEVER opened says nothing about the session. `GET /mcp`
    /// can be refused where `POST /mcp` is not — a gateway that 404s the
    /// stream, a proxy that will not carry SSE, an auth path that admits tool
    /// calls and rejects the stream — and reqwest reports that as a body
    /// ending immediately, which arrives here identically. Replacing the
    /// session then would discard a working tool lane and mint a fresh one per
    /// request, each abandoned the same way. The stream alone is retried, on
    /// its own ladder.
    ///
    /// The receiver is cleared either way: a closed channel is ready at once,
    /// so leaving it in place spins the loop that selects on it.
    fn stream_ended(&mut self) {
        let was_live = self
            .doc_stream_established
            .take()
            .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire));
        self.doc_changes = None;

        if was_live {
            self.stream_retry_delay = PROXY_INITIAL_RETRY;
            self.tear_down();
            return;
        }
        eprintln!(
            "kutl mcp: the relay would not open a notification stream for this session. \
             Tool calls are unaffected; document wakes are off until it does. \
             Retrying in {}s.",
            self.stream_retry_delay.as_secs()
        );
        self.next_stream_retry_at = Some(tokio::time::Instant::now() + self.stream_retry_delay);
        self.stream_retry_delay = (self.stream_retry_delay * 2).min(PROXY_MAX_RETRY);
    }

    /// The earliest armed retry instant — session reconnect or stream-only
    /// re-subscribe — or `None` when neither ladder is armed.
    ///
    /// The serve loop sleeps on this so a due retry fires even when no
    /// request arrives. An agent parked in a blocking read sends nothing on
    /// stdin, and a ladder driven only by incoming requests would leave it
    /// deaf to document changes until its own timeout — push latency
    /// silently degraded to poll latency.
    fn retry_deadline(&self) -> Option<tokio::time::Instant> {
        // The stream ladder is live only while no stream is in hand,
        // mirroring `maybe_resubscribe_stream`'s no-op condition.
        let stream_retry = if self.doc_changes.is_none() {
            self.next_stream_retry_at
        } else {
            None
        };
        match (self.next_retry_at, stream_retry) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Re-subscribe the notification stream on a session that still works,
    /// once its backoff has elapsed. A no-op while a stream is in hand.
    fn maybe_resubscribe_stream(&mut self) {
        if self.doc_changes.is_some() {
            return;
        }
        let Some(at) = self.next_stream_retry_at else {
            return;
        };
        if tokio::time::Instant::now() < at {
            return;
        }
        let Some(proxy) = self.proxy.as_ref() else {
            return;
        };
        let stream = proxy.subscribe_notifications();
        self.hold_stream(stream);
    }

    /// Drop the session and arm the reconnect ladder.
    ///
    /// Nothing else detects a dead session: proxied calls surface the failure
    /// one at a time to whoever made them, and the stream is subscribed once
    /// per session, so without this the server goes permanently deaf to
    /// document changes while still answering tool calls.
    ///
    /// Armed for the next attempt immediately rather than after a delay: the
    /// reconnect runs on the next incoming request, and the backoff exists to
    /// space out failed attempts, not to sit out a healthy relay. The tool
    /// list is left standing — the relay still offers those tools, and a call
    /// made before the reconnect is answered with the not-connected remedy
    /// rather than by pretending the tool does not exist.
    fn tear_down(&mut self) {
        if self.proxy.is_none() {
            return;
        }
        eprintln!(
            "kutl mcp: relay notification stream ended; the session is gone. \
             Reconnecting on the next tool call."
        );
        self.proxy = None;
        self.doc_changes = None;
        self.doc_stream_established = None;
        self.next_retry_at = Some(tokio::time::Instant::now());
    }
}

/// Attempt relay proxy reconnection if disconnected and backoff has elapsed.
///
/// Uses exponential backoff (5s -> 10s -> 20s -> ... -> 5min cap) to avoid
/// flooding the relay with connection attempts.
///
/// A reconnect mints a new relay session, so the document-change stream is
/// re-subscribed here too: the old session's SSE channel belonged to a session
/// id that no longer exists, and a server that is not subscribed can neither
/// push document changes nor answer a blocking read with one.
async fn maybe_reconnect_proxy(
    relay: &mut RelayLink,
    local_space: Option<&SpaceConfig>,
    auth_token: &str,
    did: &str,
) {
    if relay.proxy.is_some() {
        // A session whose stream was refused keeps working for tool calls, so
        // the stream is retried without the session being replaced.
        relay.maybe_resubscribe_stream();
        return;
    }
    let Some(retry_time) = relay.next_retry_at else {
        return;
    };
    if tokio::time::Instant::now() < retry_time {
        return;
    }

    let (new_proxy, new_tools) = connect_relay_proxy(local_space, auth_token, did).await;
    if let Some(proxy) = new_proxy {
        relay.hold_stream(proxy.subscribe_notifications());
        relay.proxy = Some(Arc::new(proxy));
        relay.tools = Arc::new(new_tools);
        relay.next_retry_at = None;
        relay.retry_delay = PROXY_INITIAL_RETRY;
    } else {
        relay.retry_delay = (relay.retry_delay * 2).min(PROXY_MAX_RETRY);
        relay.next_retry_at = Some(tokio::time::Instant::now() + relay.retry_delay);
    }
}

// ---------------------------------------------------------------------------
// Watch-local tool handlers
// ---------------------------------------------------------------------------

/// Spawn a relay listener task for the given space config.
///
/// Loads `.kutlspace` from `space_root` once at startup to resolve a
/// human-readable display name for MCP notifications. Falls back to
/// `space_id` when `.kutlspace` is absent.
fn spawn_relay_listener(
    config: &SpaceConfig,
    space_root: &std::path::Path,
    did: &str,
    auth_token: &str,
    notify_tx: mpsc::Sender<ChannelEvent>,
    cancel: CancellationToken,
) {
    let display_name = kutl_client::KutlspaceConfig::display_name(space_root, &config.space_id);
    tokio::spawn(relay_listener(
        config.relay_url.clone(),
        config.space_id.clone(),
        auth_token.to_owned(),
        did.to_owned(),
        display_name,
        notify_tx,
        cancel,
    ));
}

/// Search the global space registry for a space matching `space_name_or_id`
/// (matching against either `space_id` from `.kutl/space.json` or `space_name`
/// from `.kutlspace`).
fn resolve_space_config(space_name_or_id: &str) -> Option<(SpaceConfig, std::path::PathBuf)> {
    let path = kutl_client::space_registry::registry_path()
        .map_err(|e| {
            eprintln!("kutl mcp: failed to resolve spaces.json path: {e}");
        })
        .ok()?;
    let registry = kutl_client::space_registry::SpaceRegistry::load(&path)
        .map_err(|e| {
            eprintln!("kutl mcp: failed to load space registry: {e}");
        })
        .ok()?;
    for space_path in &registry.spaces {
        let root = std::path::Path::new(space_path);
        let Ok(config) = SpaceConfig::load(root) else {
            continue;
        };
        if config.space_id == space_name_or_id {
            return Some((config, root.to_path_buf()));
        }
        if let Ok(Some(ks)) = kutl_client::KutlspaceConfig::load(root)
            && ks.space_name == space_name_or_id
        {
            return Some((config, root.to_path_buf()));
        }
    }
    None
}

/// Dispatch a watch-local tool call.
///
/// When the context carries no session state (unit tests),
/// subscription-mutating tools return an error indicating state is
/// unavailable.
async fn handle_local_tool(name: &str, args: &Value, ctx: &RequestContext) -> ToolCallResult {
    match name {
        "list_spaces" => handle_list_spaces(),
        "subscribe_space" => with_space_argument(args, |space| match ctx.lock_state() {
            Some(mut s) => handle_subscribe_space(&mut s, space),
            None => ToolCallResult::error("state unavailable in test context"),
        }),
        "unsubscribe_space" => with_space_argument(args, |space| match ctx.lock_state() {
            Some(mut s) => handle_unsubscribe_space(&mut s, space),
            None => ToolCallResult::error("state unavailable in test context"),
        }),
        "wait_for_changes" => handle_wait_for_changes(args, ctx).await,
        _ => ToolCallResult::error(format!("unknown local tool: {name}")),
    }
}

/// How long `wait_for_changes` blocks when the caller names no timeout.
const WAIT_DEFAULT_TIMEOUT_SECS: u64 = 60;

/// Ceiling on how long `wait_for_changes` blocks. Clients impose their own
/// request deadlines; blocking past one turns an ordinary empty answer into an
/// error the caller has to handle.
const WAIT_MAX_TIMEOUT_SECS: u64 = 600;

/// The relay tool `wait_for_changes` blocks around and answers with.
const CHANGES_TOOL: &str = "get_changes";

/// Block until an event the caller did not cause arrives in the space it is
/// waiting on, or `deadline` passes. `true` means something happened.
///
/// Takes the receiver by reference and the deadline absolutely, so a caller
/// that has to block again after an empty read resumes the same subscription
/// — no gap for an event to fall through — without extending its own deadline.
///
/// Two kinds of event are skipped. The caller's own: waking an agent on its
/// own edit produces a loop that looks like liveness. And another space's: the
/// read that follows the wake is scoped to one space, so an event from
/// elsewhere produces an empty answer indistinguishable from the deadline
/// passing, and the agent concludes its wait expired.
async fn wait_for_event(
    events: &mut broadcast::Receiver<AgentEvent>,
    deadline: tokio::time::Instant,
    self_did: &str,
    space_id: &str,
) -> bool {
    loop {
        // The window is absolute, and `timeout_at` alone does not bound it: an
        // event already queued makes the receive resolve without the timer
        // being consulted, so a backlog is delivered — and each filtered one
        // buys another pass — after the deadline has gone. Checked here, the
        // loop cannot run outside the window the caller asked for.
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        match tokio::time::timeout_at(deadline, events.recv()).await {
            // Deadline, or the server is shutting down. Either way there is
            // nothing to report, which is an ordinary answer.
            Err(_) | Ok(Err(broadcast::error::RecvError::Closed)) => return false,
            // Too far behind to say what was missed. Treat it as activity —
            // the caller re-reads the space and resynchronises.
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => return true,
            Ok(Ok(event)) => {
                let is_mine = event.author_did().is_some_and(|did| did == self_did);
                // An event that names no space, or a caller that named none,
                // leaves nothing to compare — wake, and let the read decide.
                // Scoping on an unknown is how activity goes missing.
                let is_elsewhere =
                    !space_id.is_empty() && event.space_id().is_some_and(|space| space != space_id);
                if !is_mine && !is_elsewhere {
                    return true;
                }
            }
        }
    }
}

/// Whether a `get_changes` result reports any activity.
///
/// The relay answers with `{ signals, document_changes, checkpoint }`; a
/// response whose two arrays are both empty means nothing happened. Anything
/// unparseable counts as activity so a shape change surfaces to the caller
/// rather than being swallowed as silence.
fn changes_are_present(result: &ToolCallResult) -> bool {
    let Some(text) = result.content.first().map(|c| c.text.as_str()) else {
        return true;
    };
    let Ok(payload) = serde_json::from_str::<Value>(text) else {
        return true;
    };
    ["signals", "document_changes"].iter().any(|section| {
        payload
            .get(*section)
            .and_then(Value::as_array)
            .is_none_or(|entries| !entries.is_empty())
    })
}

/// The space a `wait_for_changes` call is scoped to, as the wake filter needs
/// it: the canonical `space_id`. Empty when the caller named no space.
///
/// The argument may be a name. The read this call wraps resolves a space by
/// name or by id, `subscribe_space` documents its argument as "owner/name or
/// `space_id`", and `list_spaces` hands the agent both spellings — so a caller
/// naming the space gets a working read. Every event, though, carries the
/// canonical id: the signal listener stamps the configured `space_id` and the
/// relay stamps its canonicalized id on the document-change frame. Comparing a
/// name against those matches nothing, and the caller then blocks its whole
/// timeout and is answered empty — byte-identical to "nothing happened".
///
/// An argument the local registry does not claim is kept verbatim rather than
/// dropped: a space this server holds no registration for is still readable
/// through the relay, and an unresolvable argument that IS the canonical id
/// must go on scoping the wake. Dropping it would widen the wake to every
/// space the session hears from — the failure this scope exists to remove.
fn wait_scope(args: &Value) -> String {
    let requested = args
        .get("space_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if requested.is_empty() {
        return String::new();
    }
    resolve_space_config(requested)
        .map_or_else(|| requested.to_owned(), |(config, _root)| config.space_id)
}

/// Block until something happens in the space, then answer with it.
///
/// Reads first, so activity that arrived while the caller was busy is returned
/// immediately instead of being waited past. Only a quiet space blocks, and a
/// block that reaches its deadline answers empty — an ordinary result, not a
/// failure.
async fn handle_wait_for_changes(args: &Value, ctx: &RequestContext) -> ToolCallResult {
    let Some(proxy) = ctx.relay_proxy.as_ref() else {
        return ToolCallResult::error(NOT_CONNECTED_REMEDY);
    };
    let timeout_secs = args
        .get("timeout_seconds")
        .and_then(Value::as_u64)
        .unwrap_or(WAIT_DEFAULT_TIMEOUT_SECS)
        .min(WAIT_MAX_TIMEOUT_SECS);
    let self_did = ctx.did().unwrap_or_default();
    let space_id = wait_scope(args);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

    // Subscribe before reading. An event landing between the read and the
    // block would otherwise fall in the gap: absent from the answer, and gone
    // by the time anyone is listening.
    let mut events = ctx.events.subscribe();

    let changes_args = serde_json::json!({
        "space_id": args.get("space_id").cloned().unwrap_or(Value::Null),
    });
    let mut latest = proxy.call_tool(CHANGES_TOOL, &changes_args).await;
    loop {
        if latest.is_error || changes_are_present(&latest) {
            return latest;
        }
        // A wake the read cannot account for is not an answer. The relay's
        // document-change fanout reaches every session it holds, so a wake can
        // come from a space this read is not scoped to; returning the empty
        // payload then is byte-identical to the deadline answer, and the agent
        // reads it as its wait having expired. Block again on the same
        // subscription, against the SAME deadline, so the caller is never held
        // past the window it asked for and a wake it cannot use costs it
        // nothing.
        if !wait_for_event(&mut events, deadline, &self_did, &space_id).await {
            return latest;
        }
        latest = proxy.call_tool(CHANGES_TOOL, &changes_args).await;
    }
}

/// Run `f` with the call's required `space` argument, or reject the call.
fn with_space_argument(args: &Value, f: impl FnOnce(&str) -> ToolCallResult) -> ToolCallResult {
    let space = args
        .get("space")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if space.is_empty() {
        return ToolCallResult::error("missing required argument: space");
    }
    f(space)
}

/// List all spaces registered in `$KUTL_HOME/spaces.json`.
fn handle_list_spaces() -> ToolCallResult {
    let registry_path = match kutl_client::space_registry::registry_path() {
        Ok(p) => p,
        Err(e) => return ToolCallResult::error(format!("failed to find space registry: {e}")),
    };

    let registry = match kutl_client::space_registry::SpaceRegistry::load(&registry_path) {
        Ok(r) => r,
        Err(e) => return ToolCallResult::error(format!("failed to load space registry: {e}")),
    };

    let spaces: Vec<Value> = registry
        .spaces
        .iter()
        .filter_map(|path| {
            let root = std::path::Path::new(path);
            let config = SpaceConfig::load(root).ok()?;
            let space_name = kutl_client::KutlspaceConfig::display_name(root, &config.space_id);
            Some(serde_json::json!({
                "space_name": space_name,
                "space_id": config.space_id,
            }))
        })
        .collect();

    ToolCallResult::text(
        serde_json::to_string_pretty(&spaces)
            .unwrap_or_else(|e| format!("failed to serialize space list: {e}")),
    )
}

/// Subscribe to a space by name or ID, spawning a relay listener.
///
/// Validates the new space's AGENTS.md anchor before subscribing. This
/// is per-subscription — a broken AGENTS.md in one space rejects only
/// that subscription, leaving the daemon and all existing subscriptions
/// intact.
fn handle_subscribe_space(state: &mut WatchState, space: &str) -> ToolCallResult {
    // Resolve to the canonical space_id FIRST so the subscription check and
    // insert use the same key the auto-subscribe path uses. Checking the raw
    // `space` argument (which may be a name) would miss an existing
    // id-keyed subscription and spawn a duplicate relay listener.
    let Some((config, root)) = resolve_space_config(space) else {
        return ToolCallResult::error(format!("space {space} not found in local registry"));
    };

    if state.subscriptions.contains_key(&config.space_id) {
        return ToolCallResult::text(format!("already subscribed to {space}"));
    }

    // Check the new space's AGENTS.md anchor before subscribing. Error paths
    // return before inserting into `state.subscriptions` — the subscription is
    // not added on failure.
    //
    // Absent and stale-compatible warn; only an incompatible block refuses,
    // matching what the server does for the space it is launched in. AGENTS.md
    // carries guidance, not access: refusing here would leave a space
    // reachable at launch and unreachable by name.
    let anchor = crate::agents_md::anchor_for(&root);
    let anchor_warning = match crate::agents_md::check_at_repo_root(&anchor) {
        Ok(crate::agents_md::CheckOutcome::Current) => None,
        Ok(crate::agents_md::CheckOutcome::StaleCompatible { sentinel }) => Some(format!(
            "warning: AGENTS.md kutl block at {} was generated by v{sentinel}; \
             current is v{}; run `kutl space apply` to refresh",
            anchor.join("AGENTS.md").display(),
            env!("CARGO_PKG_VERSION"),
        )),
        Ok(crate::agents_md::CheckOutcome::StaleIncompatible { sentinel }) => {
            return ToolCallResult::error(format!(
                "AGENTS.md kutl block at {} was generated by v{sentinel} which is \
                 incompatible with the running v{}; run `kutl space apply` and \
                 re-attempt subscribe_space",
                anchor.join("AGENTS.md").display(),
                env!("CARGO_PKG_VERSION"),
            ));
        }
        Ok(crate::agents_md::CheckOutcome::Absent) => Some(format!(
            "warning: no agent instructions found at {}; run `kutl space apply` to write them",
            anchor.join("AGENTS.md").display(),
        )),
        Err(e) => {
            return ToolCallResult::error(format!(
                "failed to check AGENTS.md at {}: {e}",
                anchor.display()
            ));
        }
    };

    let display_name = kutl_client::KutlspaceConfig::display_name(&root, &config.space_id);
    eprintln!(
        "kutl mcp: subscribed to space {} ({})",
        display_name, config.space_id
    );

    let cancel = CancellationToken::new();
    spawn_relay_listener(
        &config,
        &root,
        &state.did,
        &state.auth_token,
        state.notify_tx.clone(),
        cancel.clone(),
    );

    state
        .subscriptions
        .insert(config.space_id.clone(), SpaceSubscription { cancel });

    let mut response = format!("subscribed to {space}");
    if let Some(warning) = anchor_warning {
        response.push('\n');
        response.push_str(&warning);
    }
    ToolCallResult::text(response)
}

/// Cancel and remove a space subscription.
///
/// Resolves the argument (name or id) to the canonical `space_id` before
/// removing, so unsubscribe-by-name reaches an id-keyed subscription. Falls
/// back to the raw argument when the space cannot be resolved (e.g. it was
/// removed from the registry) so a stale subscription can still be cleared.
fn handle_unsubscribe_space(state: &mut WatchState, space: &str) -> ToolCallResult {
    let key = resolve_space_config(space).map_or_else(|| space.to_string(), |(c, _)| c.space_id);
    match state.subscriptions.remove(&key) {
        Some(sub) => {
            sub.cancel.cancel();
            ToolCallResult::text(format!("unsubscribed from {space}"))
        }
        None => ToolCallResult::error(format!("not subscribed to {space}")),
    }
}

/// Parse and dispatch a single JSON-RPC request line.
///
/// Returns `None` for notifications (no id), `Some(response)` for
/// requests that require a reply. Tool calls to relay-provided tools
/// are proxied through the context's relay session when connected.
async fn handle_request(line: &str, ctx: &RequestContext) -> Option<JsonRpcResponse> {
    let req: JsonRpcRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            return Some(JsonRpcResponse::error(
                Value::Null,
                PARSE_ERROR,
                format!("invalid JSON: {e}"),
            ));
        }
    };

    let id = req.id.clone().unwrap_or(Value::Null);

    match req.method.as_str() {
        "initialize" => {
            let (in_git_repo, space) = ctx
                .lock_state()
                .map_or((false, None), |s| (s.in_git_repo, s.space_hint.clone()));
            Some(handle_initialize(id, in_git_repo, space.as_ref()))
        }
        "notifications/initialized" => None,
        "ping" => Some(JsonRpcResponse::success(id, serde_json::json!({}))),
        "tools/list" => Some(handle_tools_list(id, &ctx.relay_tools)),
        "tools/call" => Some(handle_tools_call(id, &req.params, ctx).await),
        _ => Some(JsonRpcResponse::error(
            id,
            METHOD_NOT_FOUND,
            "method not found",
        )),
    }
}

/// Handle the `initialize` handshake.
///
/// Returns server info with `tools` and `claude/channel` capabilities,
/// plus instructions guiding the agent on how to respond to events. When
/// `in_git_repo` is true, the git-surface contract
/// ([`GIT_SURFACE_INSTRUCTIONS`]) is appended.
///
/// `space` names the space this server already subscribed to. It is the
/// agent's only route to that id: every space-scoped tool requires one, and an
/// agent on the other end of a pipe sees neither the working directory nor
/// anything written to stderr.
fn handle_initialize(id: Value, in_git_repo: bool, space: Option<&SpaceHint>) -> JsonRpcResponse {
    let mut instructions = SERVER_INSTRUCTIONS.to_owned();
    if let Some(space) = space {
        use std::fmt::Write as _;
        let _ = write!(
            instructions,
            "\n\n## Your space\n\n\
             You are already subscribed to **{}**. Pass this `space_id` to \
             every space-scoped tool:\n\n\
             ```\n{}\n```\n\n\
             Nothing further is needed to start reading or waiting.",
            space.name, space.id,
        );
    }
    if in_git_repo {
        instructions.push_str(GIT_SURFACE_INSTRUCTIONS);
    }
    let result = serde_json::json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": {
            "tools": {},
            "experimental": {
                "claude/channel": {}
            }
        },
        "serverInfo": {
            "name": "kutl-watch",
            "version": env!("CARGO_PKG_VERSION")
        },
        "instructions": instructions
    });
    JsonRpcResponse::success(id, result)
}

/// Handle `tools/list` — return watch-local and relay-proxied tool definitions.
///
/// Relay tools whose names collide with `WATCH_LOCAL_TOOLS` are dropped: the
/// local implementation takes precedence and MCP requires unique tool names.
fn handle_tools_list(id: Value, relay_tools: &[ToolDefinition]) -> JsonRpcResponse {
    let mut tools = watch_tool_definitions();
    tools.extend(
        relay_tools
            .iter()
            .filter(|t| !WATCH_LOCAL_TOOLS.contains(&t.name.as_str()))
            .cloned(),
    );
    JsonRpcResponse::success(id, serde_json::json!({ "tools": tools }))
}

/// Watch-local tool names that are handled directly, not proxied.
const WATCH_LOCAL_TOOLS: &[&str] = &[
    "list_spaces",
    "subscribe_space",
    "unsubscribe_space",
    "wait_for_changes",
];

/// What to tell a caller whose tool needs the relay while there is no relay
/// session. Every such path says the same thing, in the caller's terms.
const NOT_CONNECTED_REMEDY: &str = "not connected to the relay. The relay may be unreachable or \
     authentication may have failed. Ask the user to verify: \
     (1) the relay is running, \
     (2) credentials are valid (run `kutl auth login` to refresh), \
     (3) the space is configured correctly in .kutl/space.json";

/// Handle `tools/call` — dispatch to local handler or relay proxy.
async fn handle_tools_call(id: Value, params: &Value, ctx: &RequestContext) -> JsonRpcResponse {
    let call: ToolCallParams = match serde_json::from_value(params.clone()) {
        Ok(c) => c,
        Err(e) => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                format!("invalid tool call params: {e}"),
            );
        }
    };

    let result = if WATCH_LOCAL_TOOLS.contains(&call.name.as_str()) {
        handle_local_tool(&call.name, &call.arguments, ctx).await
    } else if ctx.relay_tools.iter().any(|t| t.name == call.name) {
        // Known relay tool — proxy through the relay. Signal tools
        // (`create_flag`/`create_reply`/`close_flag`/`reopen_flag`) flow
        // through this same lane: the relay authors them relay-mint with
        // `author_did` == the authenticated session DID.
        if let Some(proxy) = ctx.relay_proxy.as_ref() {
            let did = ctx.did();
            let result = proxy.call_tool(&call.name, &call.arguments).await;
            // The relay's authz rejection for a proxied mutation is a terse
            // "not a member of space {id}" with no pointer to the fix.
            // Enrich it with `authorized_keys` guidance so the operator
            // knows what to add.
            enrich_proxied_authz_error(result, did.as_deref(), &call.arguments)
        } else {
            ToolCallResult::error(NOT_CONNECTED_REMEDY)
        }
    } else {
        ToolCallResult::error(format!("unknown tool: {}", call.name))
    };

    JsonRpcResponse::success(
        id,
        serde_json::to_value(&result).expect("ToolCallResult is always serializable"),
    )
}

/// The relay's HTTP reason for an unauthorized agent DID (403 from the relay's
/// `McpError::NotAuthorized`). All proxied tool calls share this shape.
const NOT_AUTHORIZED_REASON: &str = "not authorized";

/// The HTTP status the relay returns when the agent DID is not authorized for
/// the space (or not in `authorized_keys`).
const FORBIDDEN_STATUS: &str = "403";

/// The relay's `McpError::NotAuthorized` message stem (`"not a member of space
/// {id}"`, see `kutl_relay::relay::mcp`). All proxied tools surface this shape
/// on authz failure — signal tools included, since they flow through the
/// proxy lane.
const NOT_A_MEMBER_REASON: &str = "not a member";

/// Whether `err_display` is a relay AUTHORIZATION failure — a 403 / "not
/// authorized" / "not a member of space" from a proxied tool call.
/// A 401 (invalid/expired token) is deliberately NOT matched: that's a
/// bad-token problem, not a missing-authorization one.
fn is_authz_failure(err_display: &str) -> bool {
    err_display.contains(NOT_AUTHORIZED_REASON)
        || err_display.contains(FORBIDDEN_STATUS)
        || err_display.contains(NOT_A_MEMBER_REASON)
}

/// If `err_display` is a relay AUTHORIZATION failure (see [`is_authz_failure`]),
/// return actionable operator guidance that echoes the agent `did` and the exact
/// space-scoped `authorized_keys` line to add — otherwise `None`, so non-authz
/// errors are neither swallowed nor mis-described.
///
/// All proxied tool calls — including signal tools, which are relay-minted
/// — surface the relay's terse "not a member of space {id}" on authz failure.
/// This maps that specific failure to a copy-pasteable remedy. A 401
/// (invalid/expired token) is deliberately NOT matched.
fn authz_failure_guidance(err_display: &str, did: &str, space_id: &str) -> Option<String> {
    if !is_authz_failure(err_display) {
        return None;
    }
    // We know the real space uuid here, so hand the operator a fully-resolved
    // scoped line (placeholder already substituted) plus the bare-DID
    // all-spaces alternative.
    let scoped = crate::agent::scoped_authorized_keys_line(did, "mcp-agent")
        .replace(crate::agent::SPACE_UUID_PLACEHOLDER, space_id);
    Some(format!(
        "the relay rejected this agent as not authorized. add the agent DID to the \
         relay authorized_keys, then retry:\n  # scoped to this space:\n  {scoped}\n  \
         # or grant ALL spaces forever with a bare DID line:\n  {did}"
    ))
}

/// Enrich a proxied relay tool's authz-failure result with `authorized_keys`
/// guidance. Applies to all proxied tools — document mutations
/// AND signal tools (relay-minted).
///
/// A proxied mutation that the caller is not authorized for comes back as a
/// terse `ToolCallResult` (its text is the relay's "not a member of space {id}")
/// with no pointer to the fix.
/// When the result is an authz failure and we have both the agent DID (off
/// [`WatchState`]) and the call's `space_id` argument, append the copy-pasteable
/// remedy. Non-authz errors, successes, and calls missing a `space_id` pass
/// through unchanged — the guidance needs a space to scope the example line.
fn enrich_proxied_authz_error(
    result: ToolCallResult,
    did: Option<&str>,
    arguments: &Value,
) -> ToolCallResult {
    if !result.is_error {
        return result;
    }
    let Some(did) = did else {
        return result;
    };
    let Some(space_id) = arguments.get("space_id").and_then(Value::as_str) else {
        return result;
    };
    let text = result
        .content
        .first()
        .map(|c| c.text.as_str())
        .unwrap_or_default();
    match authz_failure_guidance(text, did, space_id) {
        Some(guidance) => ToolCallResult::error(format!("{text}\n\n{guidance}")),
        None => result,
    }
}

/// Tool definitions for watch-specific tools.
///
/// These are the local tools that `kutl mcp serve` provides directly.
/// Relay-proxied tools are fetched dynamically and merged in
/// [`handle_tools_list`].
fn watch_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "list_spaces".into(),
            description: "List locally-registered kutl spaces. \
                          Returns an array of {space_name, space_id} objects."
                .into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "subscribe_space".into(),
            description: "Subscribe to a space for the current session. \
                          Subscribing does not deliver anything on its own — \
                          some clients surface incoming flags as unsolicited \
                          channel events, many never do. To see flags \
                          targeting you, call `wait_for_changes` (blocks \
                          until something happens) or `get_changes` \
                          (snapshot now), and keep calling; that path works \
                          in every client."
                .into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "space": {
                        "type": "string",
                        "description": "Space identifier (owner/name or space_id)."
                    }
                },
                "required": ["space"]
            }),
        },
        ToolDefinition {
            name: "unsubscribe_space".into(),
            description: "Stop watching a space.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "space": {
                        "type": "string",
                        "description": "Space identifier to unsubscribe from."
                    }
                },
                "required": ["space"]
            }),
        },
        ToolDefinition {
            name: "wait_for_changes".into(),
            description: "Wait until something new arrives for you in this \
                          space, then return it — the blocking form of your \
                          inbox. Returns the same \
                          `{ signals, document_changes, checkpoint }` as \
                          `get_changes`, scoped the same way (flags addressed \
                          to you or to the space, never ones naming another \
                          participant), and advances the same cursor.\n\n\
                          Answers immediately when activity is already \
                          waiting. Otherwise it blocks until a signal reaches \
                          you or someone edits a document, or until \
                          `timeout_seconds` elapses — whichever comes first. \
                          An empty result means nothing happened in that \
                          window; call again.\n\n\
                          Prefer this over calling `get_changes` on a timer: \
                          it returns the moment something occurs rather than \
                          on the next tick, and costs nothing while it waits. \
                          Your own edits do not wake it.\n\n\
                          It reports arrivals, so a signal that was already \
                          open before your cursor never comes back from it — \
                          `list_signals` is how you ask what is currently \
                          open in the space."
                .into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "space_id": {
                        "type": "string",
                        "description": "The space to wait on."
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": format!(
                            "How long to wait before answering empty. \
                             Defaults to {WAIT_DEFAULT_TIMEOUT_SECS}, \
                             capped at {WAIT_MAX_TIMEOUT_SECS}."
                        ),
                    }
                },
                "required": ["space_id"]
            }),
        },
    ]
}

// ---------------------------------------------------------------------------
// Space discovery
// ---------------------------------------------------------------------------

// Space discovery via kutl_client::space_config::discover_space.

// ---------------------------------------------------------------------------
// Relay WebSocket listener
// ---------------------------------------------------------------------------

/// Long-running relay listener task.
///
/// Connects to the relay, subscribes to a sentinel document to receive
/// space-wide signal broadcasts, and forwards matching flag events
/// through `notify_tx`. Reconnects automatically on disconnect.
async fn relay_listener(
    relay_url: String,
    space_id: String,
    auth_token: String,
    agent_did: String,
    display_name: String,
    notify_tx: mpsc::Sender<ChannelEvent>,
    cancel: CancellationToken,
) {
    let mut attempts: u32 = 0;
    let params = SessionParams {
        relay_url: &relay_url,
        space_id: &space_id,
        auth_token: &auth_token,
        agent_did: &agent_did,
        display_name: &display_name,
        notify_tx: &notify_tx,
        cancel: &cancel,
    };

    loop {
        if cancel.is_cancelled() {
            return;
        }

        // Set by `relay_session` once the handshake + subscribe succeed. A
        // session that reached this point is a *consecutive*-failure reset
        // point: only back-to-back connect failures should accumulate toward
        // the give-up threshold (otherwise a long-lived watch that drops once
        // per day would eventually exhaust the budget over its lifetime).
        let connected = std::sync::atomic::AtomicBool::new(false);

        match relay_session(&params, &connected).await {
            Ok(()) => {
                // Clean disconnect (cancel or EOF).
                return;
            }
            Err(e) => {
                if let Some(terminal) = e.downcast_ref::<TerminalSessionError>() {
                    // Deterministic refusal: every retry gets the same
                    // answer. Surface the remedy and stop.
                    eprintln!("kutl mcp: relay refused the session: {terminal} (not retrying)");
                    return;
                }
                let was_connected = connected.load(std::sync::atomic::Ordering::Relaxed);
                attempts = next_reconnect_attempts(attempts, was_connected);
                eprintln!(
                    "kutl mcp: relay connection lost ({e:#}), attempt {attempts}/{MAX_RECONNECT_ATTEMPTS}"
                );
                if attempts >= MAX_RECONNECT_ATTEMPTS {
                    eprintln!(
                        "kutl mcp: giving up on relay connection after {attempts} consecutive attempts"
                    );
                    return;
                }
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(RECONNECT_DELAY) => {}
                }
            }
        }
    }
}

/// Compute the next consecutive-failure count after a session ends in error.
///
/// If the session reached a connected/subscribed state, the streak resets to a
/// single fresh failure (`1`); otherwise the prior count is incremented. Only
/// truly consecutive connect failures (no successful session in between)
/// accumulate toward [`MAX_RECONNECT_ATTEMPTS`].
fn next_reconnect_attempts(prior: u32, connected_this_session: bool) -> u32 {
    if connected_this_session {
        1
    } else {
        prior.saturating_add(1)
    }
}

/// Immutable connection context for a single relay session.
///
/// Bundled into one struct so [`relay_session`] (and the reconnect loop that
/// drives it) pass a single borrow rather than a long argument list.
struct SessionParams<'a> {
    relay_url: &'a str,
    space_id: &'a str,
    auth_token: &'a str,
    agent_did: &'a str,
    display_name: &'a str,
    notify_tx: &'a mpsc::Sender<ChannelEvent>,
    cancel: &'a CancellationToken,
}

/// Run a single relay session: connect, handshake, subscribe, read loop.
///
/// Returns `Ok(())` on clean shutdown (cancellation), `Err` on connection
/// failures that should trigger reconnection.
async fn relay_session(
    params: &SessionParams<'_>,
    connected: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    let &SessionParams {
        relay_url,
        space_id,
        auth_token,
        ..
    } = params;

    let (ws, _) = tokio_tungstenite::connect_async(relay_url)
        .await
        .context("failed to connect to relay")?;

    let (mut ws_sink, mut ws_stream) = ws.split();

    // Handshake with auth token.
    let hs = handshake_envelope_with_token("2b6c76d3-124f-428b-82fc-896c484df5fd", auth_token, "");
    let hs_bytes = encode_envelope(&hs);
    ws_sink
        .send(tungstenite::Message::Binary(hs_bytes.into()))
        .await
        .context("failed to send handshake")?;

    // Wait for HandshakeAck.
    let ack_msg = ws_stream
        .next()
        .await
        .context("connection closed before handshake ack")?
        .context("ws error during handshake")?;

    match ack_msg {
        tungstenite::Message::Binary(bytes) => {
            let envelope = decode_envelope(&bytes).context("failed to decode handshake ack")?;
            match envelope.payload {
                Some(Payload::HandshakeAck(ack)) => {
                    if let Err(e) = kutl_proto::protocol::verify_ack_versions(&ack) {
                        // A version gap does not heal by retrying.
                        return Err(TerminalSessionError(e).into());
                    }
                }
                Some(Payload::Error(e)) => {
                    // A handshake-phase refusal (rejected token, refused
                    // protocol) is deterministic: identical on every retry.
                    // Read the relay's code, never its prose — an auth refusal
                    // names the credential slot to change, and every other
                    // refusal already states its own remedy.
                    let refusal = kutl_proto::protocol::handshake_refusal(&e);
                    let detail = if refusal.auth_failed {
                        kutl_client::credentials::refused_token_remedy(relay_url)
                    } else {
                        format!("relay refused the connection: {}", refusal.message)
                    };
                    return Err(TerminalSessionError(detail).into());
                }
                other => {
                    anyhow::bail!("unexpected handshake response: {other:?}");
                }
            }
        }
        other => {
            anyhow::bail!("expected binary handshake ack, got {other:?}");
        }
    }

    eprintln!("kutl mcp: connected to relay at {relay_url}");

    // Join the space's SIGNAL stream directly: signal interest is its own
    // subscription, never piggybacked on a document subscription.
    let sub = subscribe_signals_envelope(space_id, None);
    let sub_bytes = encode_envelope(&sub);
    ws_sink
        .send(tungstenite::Message::Binary(sub_bytes.into()))
        .await
        .context("failed to send subscribe")?;

    // Handshake acked and subscribe sent: this session is connected. A
    // subsequent disconnect resets the consecutive-failure streak rather than
    // accumulating toward the lifetime give-up threshold.
    connected.store(true, std::sync::atomic::Ordering::Relaxed);

    session_read_loop(params, &mut ws_sink, &mut ws_stream).await
}

/// The WS sink half of a relay session's split socket.
type SessionSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tungstenite::Message,
>;
/// The WS stream half of a relay session's split socket.
type SessionStream = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

/// A connected session's read loop: deliver matching flags, heal a
/// signal-stream eviction in place by re-subscribing, and bail on
/// session-fatal frames so the reconnect ladder rebuilds the session.
async fn session_read_loop(
    params: &SessionParams<'_>,
    ws_sink: &mut SessionSink,
    ws_stream: &mut SessionStream,
) -> Result<()> {
    let &SessionParams {
        space_id,
        agent_did,
        display_name,
        notify_tx,
        cancel,
        ..
    } = params;

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            msg = ws_stream.next() => {
                let Some(msg) = msg else {
                    anyhow::bail!("relay connection closed");
                };
                let msg = msg.context("ws read error")?;

                let bytes = match msg {
                    tungstenite::Message::Binary(b) => b,
                    tungstenite::Message::Close(_) => {
                        anyhow::bail!("relay sent close frame");
                    }
                    _ => continue,
                };

                let envelope = match decode_envelope(&bytes) {
                    Ok(e) => e,
                    Err(e) => {
                        eprintln!("kutl mcp: failed to decode envelope: {e:#}");
                        continue;
                    }
                };

                match envelope.payload {
                    // Every record kind arrives here; what this agent is woken
                    // for is decided in one place, off the loop.
                    Some(Payload::Signal(ref signal)) => {
                        deliver_signal_frame(signal, agent_did, space_id, display_name, notify_tx);
                    }
                    // The relay EVICTED this session from the space's signal
                    // recipient set (its lane overflowed, or signals yielded
                    // to document traffic); the notice is the only cue that
                    // re-subscribing is required. Ignoring it leaves the
                    // session connected but deaf to flags forever.
                    Some(Payload::StaleSignalStream(ref s)) => {
                        eprintln!(
                            "kutl mcp: signal stream evicted ({}); re-subscribing",
                            s.reason
                        );
                        let sub = subscribe_signals_envelope(space_id, None);
                        ws_sink
                            .send(tungstenite::Message::Binary(encode_envelope(&sub).into()))
                            .await
                            .context("failed to re-subscribe after signal-stream eviction")?;
                    }
                    // A mid-session Error frame is fatal to this session
                    // (the relay tears the connection down after sending
                    // it). Bail so the reconnect ladder rebuilds — and a
                    // deterministic refusal then stops terminally at the
                    // next handshake instead of being swallowed here.
                    Some(Payload::Error(ref e)) => {
                        anyhow::bail!("relay error: {}", e.message);
                    }
                    // All other payloads (SyncOps, StaleSubscriber, etc.)
                    // are ignored — the watch command only cares about
                    // signals and its own stream's health.
                    _ => {}
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Signal filtering
// ---------------------------------------------------------------------------

/// Send `signal` to the agent as a channel event, when it is activity this
/// agent should be woken for.
///
/// The whole wake decision lives here rather than in the read loop's match, so
/// there is one place to reason about — and to test — what reaches an agent.
fn deliver_signal_frame(
    signal: &sync::Signal,
    agent_did: &str,
    space_id: &str,
    display_name: &str,
    notify_tx: &mpsc::Sender<ChannelEvent>,
) {
    if !should_deliver_signal(signal, agent_did) {
        return;
    }
    let Some(event) = build_signal_channel_event(signal, space_id, display_name) else {
        return;
    };
    if let Err(e) = notify_tx.try_send(event) {
        eprintln!("kutl mcp: dropped signal event (channel full): {e}");
    }
}

/// Determine whether an incoming signal should be delivered to this agent.
///
/// # The wake gate is narrower than the feed on purpose
///
/// The change feed pages in last-activity order, so a close, reopen, or edit
/// re-surfaces its signal to `get_changes` — a woken agent would no longer
/// read empty. The gate still wakes only on the record that MINTS a signal,
/// as a chosen scope rather than a mechanical constraint: a blocked agent is
/// waiting to be spoken to, and waking it for every lifecycle transition on
/// signals it has already seen would turn state churn into interrupts. The
/// test is the record's EVENT, not its payload — an EDITED carries an
/// ordinary flag payload and would slip a payload-shaped gate.
///
/// # What this surface adds on its own
///
/// Self-authored records are never echoed back — waking an agent on its own
/// write produces a loop that looks like liveness. That rule is push-only, and
/// deliberately not symmetric: [`kutl_relay::change_backend::signal_reaches`]
/// carries no notion of authorship, so `get_changes` does return your own
/// space-wide records even though none of them ever woke you. Reach itself is
/// that shared function, so the two paths cannot disagree about who a record
/// is for.
fn should_deliver_signal(signal: &sync::Signal, agent_did: &str) -> bool {
    if !mints_a_signal(signal) {
        return false;
    }
    if !agent_did.is_empty() && signal.author_did == agent_did {
        return false;
    }
    kutl_relay::change_backend::signal_reaches(signal, agent_did)
}

/// Whether this record is the one that brings a signal into being.
///
/// `UNSPECIFIED` counts as CREATED: a record predating the event field is a
/// CREATED by construction, and the fold and the projections read it that way
/// too.
fn mints_a_signal(signal: &sync::Signal) -> bool {
    matches!(
        signal.event(),
        sync::SignalEventType::Created | sync::SignalEventType::Unspecified
    )
}

/// Build a [`ChannelEvent`] from a signal this agent receives, or `None` when
/// the record has no nameable kind.
///
/// Every kind an agent can act on produces one, not only flags — a reply
/// carries the substance of most conversations. A record whose kind this build
/// cannot name (a legacy or future payload) produces nothing: the alternative
/// is a `record` field carrying a discriminator outside its own documented set,
/// which a consumer has to guess at. Silence is the safer of the two, and the
/// caller re-reads through `get_changes` regardless.
///
/// The metadata always includes both `space_id` (canonical UUID, stable
/// across renames, suitable for programmatic lookup) and `space_name`
/// (human-readable, may change). Consumers should display `space_name` and
/// reference `space_id` for any programmatic action. `record` is the record
/// kind; `kind` is a flag's intent and is present on flags only — the two are
/// orthogonal axes and collapsing them is the common mistake. `document` is
/// empty for a record attached to no document, which is every reply and any
/// space-level flag.
fn build_signal_channel_event(
    signal: &sync::Signal,
    space_id: &str,
    space_name: &str,
) -> Option<ChannelEvent> {
    let record = kutl_signals::summary::kind_of(signal)?;

    let mut meta = serde_json::Map::new();
    meta.insert("space_id".into(), Value::String(space_id.to_owned()));
    meta.insert("space_name".into(), Value::String(space_name.to_owned()));
    meta.insert(
        "document".into(),
        Value::String(signal.document_id.clone().unwrap_or_default()),
    );
    meta.insert("sender".into(), Value::String(signal.author_did.clone()));
    meta.insert("record".into(), Value::String(record.label().to_owned()));

    // The body each kind puts in front of a reader.
    let content = match &signal.payload {
        Some(sync::signal::Payload::Flag(flag)) => {
            meta.insert("kind".into(), Value::String(flag_kind_name(flag.kind)));
            flag.message.clone()
        }
        Some(sync::signal::Payload::Reply(reply)) => {
            meta.insert(
                "parent_signal_id".into(),
                Value::String(reply.parent_signal_id.clone()),
            );
            reply.body.clone()
        }
        Some(sync::signal::Payload::Chat(chat)) => chat.topic.clone().unwrap_or_default(),
        Some(sync::signal::Payload::Decision(decision)) => decision.title.clone(),
        // `kind_of` already answered `None` for these, so the `?` above
        // returned. Unreachable, and spelled out rather than caught by a
        // wildcard so a new payload arm has to be answered for here too.
        Some(sync::signal::Payload::Transition(_)) | None => String::new(),
    };

    Some(ChannelEvent { content, meta })
}

/// Convert a `FlagKind` i32 to a human-readable string.
///
/// The names come from the shared vocabulary, but the fallback deliberately
/// does not: an unrecognized or unset discriminant reads `unknown` rather than
/// the vocabulary's `info`, because a kind this build predates must not reach
/// an agent labelled as chatter it can ignore.
fn flag_kind_name(kind: i32) -> String {
    match sync::FlagKind::try_from(kind) {
        Ok(sync::FlagKind::Unspecified) | Err(_) => "unknown".to_owned(),
        Ok(known) => kutl_proto::vocab::flag_kind_to_str(i32::from(known)).to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::SPACE_UUID_PLACEHOLDER;

    /// A request context with `relay_tools` advertised but no relay session
    /// and no session state — the unit-test scenario.
    fn test_context(relay_tools: Vec<ToolDefinition>) -> RequestContext {
        let (events, _) = broadcast::channel(EVENT_BROADCAST_CAPACITY);
        RequestContext {
            relay_tools: Arc::new(relay_tools),
            relay_proxy: None,
            state: None,
            events,
        }
    }

    /// No relay tools, no proxy, no state — the default test scenario.
    async fn test_handle(line: &str) -> Option<JsonRpcResponse> {
        handle_request(line, &test_context(Vec::new())).await
    }

    /// A document change in [`WAKE_SPACE`] authored by `did`.
    fn edit_by(did: &str) -> AgentEvent {
        edit_in(WAKE_SPACE, did)
    }

    /// A document change in `space` authored by `did`.
    fn edit_in(space: &str, did: &str) -> AgentEvent {
        AgentEvent::DocumentChanged {
            space_id: space.into(),
            document_id: "d".into(),
            author_did: did.into(),
            intent: "edit".into(),
        }
    }

    /// A deadline `secs` from now, for the tests that drive `wait_for_event`
    /// directly.
    fn deadline_in(secs: u64) -> tokio::time::Instant {
        tokio::time::Instant::now() + std::time::Duration::from_secs(secs)
    }

    #[tokio::test]
    async fn test_wait_for_event_wakes_on_a_peer_event() {
        let (tx, _) = broadcast::channel::<AgentEvent>(EVENT_BROADCAST_CAPACITY);
        let mut rx = tx.subscribe();
        let waiting = tokio::spawn(async move {
            wait_for_event(&mut rx, deadline_in(5), "did:key:me", WAKE_SPACE).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send(edit_by("did:key:peer"))
            .expect("a subscriber is waiting");
        assert!(
            waiting.await.expect("the waiter joins"),
            "a peer's edit must wake the call"
        );
    }

    /// The space the wake-path tests run in.
    const WAKE_SPACE: &str = "475e1f0c-14a9-4b83-8b73-95c80cfd166d";

    /// A second space this server also holds a subscription to. Nothing about
    /// a caller blocked on [`WAKE_SPACE`] should notice its traffic.
    const OTHER_SPACE: &str = "9c1f4a70-6d2b-4c1e-90a8-2f6b1d3e4c55";

    #[tokio::test]
    async fn test_wait_for_event_ignores_another_spaces_edit() {
        // One session can hold several subscriptions, and the relay's
        // document-change fanout reaches every MCP session it knows. A wake
        // from a space the caller is not reading hands back the same empty
        // payload as a timeout, and the agent stops waiting.
        let (tx, _) = broadcast::channel::<AgentEvent>(EVENT_BROADCAST_CAPACITY);
        let mut rx = tx.subscribe();
        let waiting = tokio::spawn(async move {
            wait_for_event(&mut rx, deadline_in(1), "did:key:me", WAKE_SPACE).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send(edit_in(OTHER_SPACE, "did:key:peer"))
            .expect("a subscriber is waiting");
        assert!(
            !waiting.await.expect("the waiter joins"),
            "an edit in another space must not wake a caller reading this one"
        );
    }

    #[tokio::test]
    async fn test_wait_for_event_ignores_another_spaces_signal() {
        // The signal half carries its space in the channel metadata rather
        // than a field, so the scope check has to see through both shapes.
        let (tx, _) = broadcast::channel::<AgentEvent>(EVENT_BROADCAST_CAPACITY);
        let mut rx = tx.subscribe();
        let waiting = tokio::spawn(async move {
            wait_for_event(&mut rx, deadline_in(1), "did:key:me", WAKE_SPACE).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut meta = serde_json::Map::new();
        meta.insert("space_id".into(), Value::String(OTHER_SPACE.into()));
        meta.insert("sender".into(), Value::String("did:key:peer".into()));
        tx.send(AgentEvent::Signal {
            content: "hi".into(),
            meta,
        })
        .expect("a subscriber is waiting");
        assert!(
            !waiting.await.expect("the waiter joins"),
            "a flag in another space must not wake a caller reading this one"
        );
    }

    /// A caller may name the space rather than id it — the read resolves both,
    /// `subscribe_space` documents its argument as "owner/name or `space_id`",
    /// and `list_spaces` hands the agent both spellings. Every event carries
    /// the canonical id, so the wake filter has to compare canonical to
    /// canonical: a name compared verbatim matches nothing, and the caller
    /// blocks its whole timeout and is answered empty, which is exactly what
    /// "nothing happened" looks like.
    #[tokio::test]
    #[serial_test::serial] // mutates process-global KUTL_HOME
    async fn test_wait_scope_canonicalizes_a_space_named_by_name() {
        let (kutl_home, _space_dir) =
            register_named_space_with_valid_anchor(WAKE_SPACE, "dinner-club");

        unsafe { std::env::set_var("KUTL_HOME", kutl_home.path()) };
        let scope = wait_scope(&serde_json::json!({ "space_id": "dinner-club" }));
        unsafe { std::env::remove_var("KUTL_HOME") };
        assert_eq!(scope, WAKE_SPACE, "the name must resolve to the space id");

        let (tx, _) = broadcast::channel::<AgentEvent>(EVENT_BROADCAST_CAPACITY);
        let mut rx = tx.subscribe();
        let waiting = tokio::spawn(async move {
            wait_for_event(&mut rx, deadline_in(5), "did:key:me", &scope).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send(edit_in(WAKE_SPACE, "did:key:peer"))
            .expect("a subscriber is waiting");
        assert!(
            waiting.await.expect("the waiter joins"),
            "a caller who named the space must still be woken by its events"
        );
    }

    /// An argument nothing in the local registry claims stays as it was. A
    /// space this server holds no registration for is still readable through
    /// the relay, and dropping the scope would widen the wake to every space
    /// the session hears from.
    #[tokio::test]
    #[serial_test::serial] // mutates process-global KUTL_HOME
    async fn test_wait_scope_keeps_an_unresolvable_argument() {
        let (kutl_home, _space_dir) =
            register_named_space_with_valid_anchor(WAKE_SPACE, "dinner-club");

        unsafe { std::env::set_var("KUTL_HOME", kutl_home.path()) };
        let scope = wait_scope(&serde_json::json!({ "space_id": OTHER_SPACE }));
        let unnamed = wait_scope(&serde_json::json!({}));
        unsafe { std::env::remove_var("KUTL_HOME") };

        assert_eq!(
            scope, OTHER_SPACE,
            "an unregistered id still scopes the wake"
        );
        assert!(unnamed.is_empty(), "naming no space scopes nothing");
    }

    /// A link holding a session and a document-change stream that has just
    /// closed, with `established` saying whether the relay ever served it.
    fn link_with_closed_stream(established: bool) -> RelayLink {
        let (_tx, rx) = mpsc::channel(1);
        RelayLink {
            proxy: Some(Arc::new(RelayProxy::detached())),
            tools: Arc::new(Vec::new()),
            doc_changes: Some(rx),
            doc_stream_established: Some(Arc::new(std::sync::atomic::AtomicBool::new(established))),
            next_retry_at: None,
            retry_delay: PROXY_INITIAL_RETRY,
            next_stream_retry_at: None,
            stream_retry_delay: PROXY_INITIAL_RETRY,
        }
    }

    /// A stream the relay never opened says nothing about the session behind
    /// it. `GET /mcp` can be refused where `POST /mcp` is not, and the tool
    /// lane keeps working — so the session stays, and only the stream is
    /// retried. Replacing it here would abandon a working session and mint a
    /// fresh one per request, each abandoned the same way.
    #[tokio::test]
    async fn test_stream_ended_keeps_a_session_whose_stream_never_opened() {
        let mut relay = link_with_closed_stream(false);
        relay.stream_ended();

        assert!(
            relay.proxy.is_some(),
            "a session whose tool lane works must not be discarded"
        );
        assert!(
            relay.next_retry_at.is_none(),
            "the session reconnect ladder must not be armed"
        );
        assert!(
            relay.doc_changes.is_none(),
            "the closed receiver must be cleared or the serving loop spins on it"
        );
        assert!(
            relay.next_stream_retry_at.is_some(),
            "the stream alone must be scheduled for another attempt"
        );
    }

    /// A stream that WAS live and then ended is the relay having reaped the
    /// session. The session id is immutable, so only a new session recovers —
    /// this is the recovery the stream watch exists for and it must keep
    /// firing.
    #[tokio::test]
    async fn test_stream_ended_replaces_a_session_whose_stream_was_live() {
        let mut relay = link_with_closed_stream(true);
        relay.stream_ended();

        assert!(relay.proxy.is_none(), "a dead session must be dropped");
        assert!(
            relay.next_retry_at.is_some(),
            "the reconnect ladder must be armed"
        );
        assert!(relay.doc_changes.is_none(), "the stream goes with it");
    }

    /// The stream retry waits out its backoff and then fires, without the
    /// session being touched either way.
    #[tokio::test]
    async fn test_maybe_resubscribe_stream_waits_for_its_backoff() {
        let mut relay = link_with_closed_stream(false);
        relay.stream_ended();

        relay.maybe_resubscribe_stream();
        assert!(
            relay.doc_changes.is_none(),
            "a retry before the backoff elapses is not due"
        );

        relay.next_stream_retry_at = Some(tokio::time::Instant::now());
        relay.maybe_resubscribe_stream();
        assert!(
            relay.doc_changes.is_some(),
            "a due retry must re-subscribe the stream"
        );
        assert!(
            relay.proxy.is_some(),
            "re-subscribing must not disturb the session"
        );
    }

    /// The serve loop sleeps on [`RelayLink::retry_deadline`] so a due retry
    /// fires even when no request arrives. An agent parked in a blocking read
    /// sends nothing on stdin; a ladder driven only by incoming requests
    /// leaves it deaf to document changes until its own timeout.
    #[tokio::test]
    async fn test_retry_deadline_is_unarmed_while_the_stream_is_healthy() {
        let relay = link_with_closed_stream(true);
        assert!(
            relay.retry_deadline().is_none(),
            "a connected link with a live stream has nothing to retry"
        );
    }

    #[tokio::test]
    async fn test_retry_deadline_arms_when_a_live_stream_dies() {
        let mut relay = link_with_closed_stream(true);
        relay.stream_ended();
        assert!(
            relay.retry_deadline().is_some(),
            "a dead session must arm the deadline the loop sleeps on"
        );
    }

    #[tokio::test]
    async fn test_retry_deadline_arms_for_a_refused_stream_and_clears_on_hold() {
        let mut relay = link_with_closed_stream(false);
        relay.stream_ended();
        assert!(
            relay.retry_deadline().is_some(),
            "a refused stream's backoff must arm the deadline"
        );

        let (_tx, rx) = mpsc::channel(1);
        relay.hold_stream(crate::watch_tools::NotificationStream {
            events: rx,
            established: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        assert!(
            relay.retry_deadline().is_none(),
            "a stream in hand disarms the deadline"
        );
    }

    #[test]
    fn test_agent_event_space_id_reads_both_shapes() {
        let mut meta = serde_json::Map::new();
        meta.insert("space_id".into(), Value::String(OTHER_SPACE.into()));
        let signal = AgentEvent::Signal {
            content: "hi".into(),
            meta,
        };
        assert_eq!(signal.space_id(), Some(OTHER_SPACE));
        assert_eq!(
            edit_in(WAKE_SPACE, "did:key:bob").space_id(),
            Some(WAKE_SPACE)
        );
        assert_eq!(
            AgentEvent::Signal {
                content: "hi".into(),
                meta: serde_json::Map::new(),
            }
            .space_id(),
            None,
            "an event that records no space is not attributable to one"
        );
    }

    /// Run one relay signal frame through the real wake path and report whether
    /// a caller blocked in `wait_for_changes` is woken by it.
    ///
    /// The whole chain the session runs, not a reconstruction of it: the read
    /// loop's delivery call, the notify channel it sends on, the
    /// `notify_rx` → [`publish_event`] hop the main loop performs, and the
    /// broadcast [`wait_for_event`] blocks on. A test that built an event
    /// itself and pushed it onto a fresh broadcast would assert on its own
    /// wiring and pass no matter what the read loop decides.
    async fn frame_wakes_a_blocked_caller(signal: &sync::Signal, self_did: &str) -> bool {
        let (notify_tx, mut notify_rx) = mpsc::channel::<ChannelEvent>(NOTIFY_CHANNEL_CAPACITY);
        let (events_tx, _) = broadcast::channel::<AgentEvent>(EVENT_BROADCAST_CAPACITY);
        let (frames_tx, _frames_rx) = mpsc::channel::<String>(OUTGOING_FRAME_CAPACITY);

        let mut rx = events_tx.subscribe();
        let self_did_owned = self_did.to_owned();
        let waiting = tokio::spawn(async move {
            wait_for_event(&mut rx, deadline_in(1), &self_did_owned, WAKE_SPACE).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        deliver_signal_frame(signal, self_did, WAKE_SPACE, "team-space", &notify_tx);

        // The main loop's `Some(event) = notify_rx.recv()` arm. `try_recv`
        // rather than `recv`: nothing else will ever send, so a frame the read
        // loop declined must not park this test until the wait times out.
        if let Ok(event) = notify_rx.try_recv() {
            publish_event(&events_tx, &frames_tx, event.into()).await;
        }

        waiting.await.expect("the waiter joins")
    }

    /// A peer's reply wakes a blocked caller. Replies carry the substance of a
    /// conversation, so a wait that sleeps through them answers on its own
    /// timeout and reads, to whoever wrote the reply, as being ignored.
    #[tokio::test]
    async fn test_reply_frame_wakes_a_blocked_caller() {
        assert!(
            frame_wakes_a_blocked_caller(
                &make_reply_signal("did:key:peer", "sig-parent"),
                "did:key:me"
            )
            .await,
            "a peer's reply must wake the call"
        );
    }

    /// The self-filter holds for every kind: an agent is not woken by its own
    /// answer, which would make a group of agents cascade in a loop that reads
    /// as liveness.
    #[tokio::test]
    async fn test_self_authored_reply_frame_does_not_wake() {
        assert!(
            !frame_wakes_a_blocked_caller(
                &make_reply_signal("did:key:me", "sig-parent"),
                "did:key:me"
            )
            .await,
            "the caller's own reply must not wake it"
        );
    }

    /// A lifecycle transition must NOT wake a blocked caller.
    ///
    /// The relay broadcasts every close, reopen, and tombstone space-wide, and
    /// they address nobody, so reach alone would hand them to everyone. But
    /// `get_changes` pages on the signal row's CREATED timestamp, which a
    /// transition leaves untouched — so the read that follows the wake returns
    /// nothing, the agent reads that as "my wait timed out", and it stops
    /// waiting. Waking here would reintroduce the exact silence the wake path
    /// exists to prevent.
    #[tokio::test]
    async fn test_transition_frame_does_not_wake_a_blocked_caller() {
        for event in [
            sync::SignalEventType::Closed,
            sync::SignalEventType::Reopened,
            sync::SignalEventType::Tombstoned,
        ] {
            let transition = make_transition_signal("did:key:peer", event);
            // Asserted at the gate as well as end-to-end. A transition also
            // carries no nameable record kind, so the event builder declines it
            // independently — without this line the test would still pass with
            // the cursor invariant deleted, and would be pinning the wrong
            // mechanism.
            assert!(
                !should_deliver_signal(&transition, "did:key:me"),
                "{event:?} must be refused for crossing no cursor, not incidentally"
            );
            assert!(
                !frame_wakes_a_blocked_caller(&transition, "did:key:me").await,
                "{event:?} must not wake a caller whose next read would be empty"
            );
        }
    }

    /// An EDITED record is the same trap as a transition: it carries an
    /// ordinary flag payload, so a payload-shaped rule would wake on it, and it
    /// leaves the row's timestamp alone exactly as a close does.
    #[tokio::test]
    async fn test_edited_frame_does_not_wake_a_blocked_caller() {
        let mut edited = make_flag_signal(
            "did:key:peer",
            "",
            sync::AudienceType::Space,
            sync::FlagKind::Question,
        );
        edited.set_event(sync::SignalEventType::Edited);

        assert!(
            !frame_wakes_a_blocked_caller(&edited, "did:key:me").await,
            "an edit never crosses the cursor, so it must not wake"
        );
    }

    /// The caller's window is absolute.
    ///
    /// An event already sitting in the channel makes `timeout_at` resolve
    /// without the timer ever being consulted, so a backlog can carry this
    /// loop past the deadline it was handed — once per queued event that gets
    /// filtered, and then a wake delivered outside the window the caller asked
    /// for.
    #[tokio::test]
    async fn test_wait_for_event_does_not_wake_past_its_deadline() {
        let (tx, _) = broadcast::channel::<AgentEvent>(EVENT_BROADCAST_CAPACITY);
        let mut rx = tx.subscribe();
        // Queued before the wait begins: one the filter drops, then one that
        // would otherwise wake.
        tx.send(edit_by("did:key:me")).expect("a subscriber exists");
        tx.send(edit_by("did:key:peer"))
            .expect("a subscriber exists");

        let gone = tokio::time::Instant::now() - std::time::Duration::from_secs(1);
        assert!(
            !wait_for_event(&mut rx, gone, "did:key:me", WAKE_SPACE).await,
            "a deadline already passed must not be extended by a queued event"
        );
    }

    #[tokio::test]
    async fn test_wait_for_event_returns_false_at_timeout() {
        let (tx, _) = broadcast::channel::<AgentEvent>(EVENT_BROADCAST_CAPACITY);
        assert!(
            !wait_for_event(
                &mut tx.subscribe(),
                deadline_in(1),
                "did:key:me",
                WAKE_SPACE
            )
            .await,
            "a timeout is an empty answer, never an error"
        );
    }

    #[tokio::test]
    async fn test_wait_for_event_ignores_self_authored_edit() {
        // Without this an agent wakes on its own edit, and a group of them
        // cascade in a loop that reads as liveness.
        let (tx, _) = broadcast::channel::<AgentEvent>(EVENT_BROADCAST_CAPACITY);
        let mut rx = tx.subscribe();
        let waiting = tokio::spawn(async move {
            wait_for_event(&mut rx, deadline_in(1), "did:key:me", WAKE_SPACE).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send(edit_by("did:key:me"))
            .expect("a subscriber is waiting");
        assert!(
            !waiting.await.expect("the waiter joins"),
            "the caller's own edit must not wake it"
        );
    }

    #[test]
    fn test_agent_event_author_did_reads_both_shapes() {
        // The two sources carry the author differently: a flag keeps it in the
        // channel meta map under `sender`, a document change in a field. The
        // self-filter reads one accessor, so it has to see through both.
        let mut meta = serde_json::Map::new();
        meta.insert("sender".into(), Value::String("did:key:alice".into()));
        let signal = AgentEvent::Signal {
            content: "hi".into(),
            meta,
        };
        assert_eq!(signal.author_did(), Some("did:key:alice"));

        let edit = AgentEvent::DocumentChanged {
            space_id: "s".into(),
            document_id: "d".into(),
            author_did: "did:key:bob".into(),
            intent: "edit".into(),
        };
        assert_eq!(edit.author_did(), Some("did:key:bob"));

        let anonymous = AgentEvent::Signal {
            content: "hi".into(),
            meta: serde_json::Map::new(),
        };
        assert_eq!(anonymous.author_did(), None);
    }

    #[tokio::test]
    async fn test_handle_request_initialize() {
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","clientInfo":{"name":"test","version":"1.0"}}}"#;
        let resp = test_handle(req)
            .await
            .expect("initialize should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert_eq!(result["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert!(result["capabilities"]["tools"].is_object());
        assert!(result["capabilities"]["experimental"]["claude/channel"].is_object());
        assert_eq!(result["serverInfo"]["name"], "kutl-watch");
        assert!(result["instructions"].is_string());
    }

    #[test]
    fn test_handle_initialize_names_the_subscribed_space() {
        // The server subscribes to the local space at startup and says so on
        // stderr, which no MCP client shows the model. Without the id in the
        // handshake an agent has nothing to pass to any space-scoped tool and
        // resorts to guessing names.
        let hint = SpaceHint {
            id: "4ba224d8-528c-4ef1-852f-800e32237f05".into(),
            name: "smoke".into(),
        };
        let resp = handle_initialize(Value::Null, false, Some(&hint));
        let result = resp.result.expect("should have result");
        let instructions = result["instructions"]
            .as_str()
            .expect("instructions should be a string");
        assert!(
            instructions.contains("4ba224d8-528c-4ef1-852f-800e32237f05"),
            "the handshake must carry the space id, got:\n{instructions}"
        );
        assert!(
            instructions.contains("smoke"),
            "the handshake should name the space too, got:\n{instructions}"
        );
    }

    #[test]
    fn test_handle_initialize_without_a_space_promises_nothing() {
        // With no local space there is no id to hand over, and claiming one
        // would send the agent chasing a space that does not exist.
        let resp = handle_initialize(Value::Null, false, None);
        let result = resp.result.expect("should have result");
        let instructions = result["instructions"]
            .as_str()
            .expect("instructions should be a string");
        assert!(
            !instructions.contains("You are already subscribed"),
            "no space means no subscription claim, got:\n{instructions}"
        );
    }

    #[test]
    fn test_handle_initialize_appends_git_surface_section_when_in_git_repo() {
        let resp = handle_initialize(Value::Null, true, None);
        let result = resp.result.expect("should have result");
        let instructions = result["instructions"]
            .as_str()
            .expect("instructions should be a string");
        assert!(
            instructions.contains(kutl_client::surface::SURFACE_SENTINEL_HEADER),
            "expected git-surface section, got:\n{instructions}"
        );
        // Prose immediately surrounding the sentinel, and the trailing
        // sentence, must survive the `concat!` build byte-for-byte.
        assert!(instructions.contains(
            "This space's files are mirrored into the surrounding git tree by `kutl surface`."
        ));
        assert!(instructions.ends_with("it once when a chunk of work is done, then commit."));
    }

    #[test]
    fn test_handle_initialize_omits_git_surface_section_when_not_in_git_repo() {
        let resp = handle_initialize(Value::Null, false, None);
        let result = resp.result.expect("should have result");
        let instructions = result["instructions"]
            .as_str()
            .expect("instructions should be a string");
        assert!(!instructions.contains("surfaced from kutl space"));
        // The base briefing and the KFM dialect reference it carries are
        // present either way — only the git-surface section is conditional.
        assert!(instructions.contains("## Decisions"));
        assert!(instructions.contains("# kutl-flavored markdown (KFM)"));
    }

    #[tokio::test]
    async fn test_handle_request_notification_returns_none() {
        let req = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        assert!(test_handle(req).await.is_none());
    }

    #[tokio::test]
    async fn test_handle_request_ping() {
        let req = r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#;
        let resp = test_handle(req)
            .await
            .expect("ping should return a response");
        assert!(resp.error.is_none());
        assert_eq!(resp.result, Some(serde_json::json!({})));
    }

    #[tokio::test]
    async fn test_handle_request_tools_list_no_relay() {
        let req = r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/list should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        let tools = result["tools"]
            .as_array()
            .expect("tools should be an array");
        assert_eq!(tools.len(), WATCH_LOCAL_TOOLS.len());

        let names: Vec<&str> = tools
            .iter()
            .map(|t| t["name"].as_str().expect("tool should have a name"))
            .collect();
        for local in WATCH_LOCAL_TOOLS {
            assert!(names.contains(local), "{local} should be advertised");
        }
    }

    #[tokio::test]
    async fn test_handle_request_tools_list_with_relay_tools() {
        let relay_tools = vec![ToolDefinition {
            name: "read_document".into(),
            description: "Read a document.".into(),
            input_schema: serde_json::json!({"type": "object", "properties": {}}),
        }];
        let req = r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#;
        let resp = handle_request(req, &test_context(relay_tools))
            .await
            .expect("tools/list should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        let tools = result["tools"]
            .as_array()
            .expect("tools should be an array");
        // every watch-local tool, plus the one relay tool
        assert_eq!(tools.len(), WATCH_LOCAL_TOOLS.len() + 1);

        let names: Vec<&str> = tools
            .iter()
            .map(|t| t["name"].as_str().expect("tool should have a name"))
            .collect();
        assert!(names.contains(&"read_document"));
    }

    /// When the relay also advertises `list_spaces`, `tools/list` must
    /// return exactly ONE entry for it (no duplicates — MCP requires unique names).
    #[tokio::test]
    async fn test_handle_request_tools_list_dedup_colliding_relay_tool() {
        let relay_tools = vec![
            ToolDefinition {
                name: "list_spaces".into(),
                description: "Relay copy — must be suppressed.".into(),
                input_schema: serde_json::json!({"type": "object", "properties": {}}),
            },
            ToolDefinition {
                name: "read_document".into(),
                description: "Read a document.".into(),
                input_schema: serde_json::json!({"type": "object", "properties": {}}),
            },
        ];
        let req = r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#;
        let resp = handle_request(req, &test_context(relay_tools))
            .await
            .expect("tools/list should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        let tools = result["tools"]
            .as_array()
            .expect("tools should be an array");

        let list_spaces_count = tools
            .iter()
            .filter(|t| t["name"].as_str() == Some("list_spaces"))
            .count();
        assert_eq!(
            list_spaces_count, 1,
            "list_spaces must appear exactly once (no relay dupe); got {list_spaces_count} in {tools:?}"
        );
        // Non-colliding relay tool must still be present.
        let names: Vec<&str> = tools
            .iter()
            .map(|t| t["name"].as_str().expect("tool should have a name"))
            .collect();
        assert!(
            names.contains(&"read_document"),
            "non-colliding relay tool missing: {names:?}"
        );
    }

    /// `list_spaces` must NOT include `path` in its per-space objects —
    /// the host filesystem path is useless to an agent and should not be leaked.
    #[tokio::test]
    async fn test_handle_list_spaces_no_path_field() {
        // Call handle_list_spaces directly; parse whatever the registry returns
        // (may be an empty array in CI — that's fine, the assertion still holds
        // because it checks every element that IS returned).
        let result = handle_list_spaces();
        let text = result.content.first().map_or("[]", |c| c.text.as_str());
        // If the text starts with "failed" we're in an environment with no
        // registry — skip the structural assertion.
        if text.starts_with("failed") {
            return;
        }
        let spaces: Vec<serde_json::Value> =
            serde_json::from_str(text).expect("list_spaces returns a JSON array");
        for space in &spaces {
            assert!(
                space.get("path").is_none(),
                "list_spaces must not include 'path'; got element: {space}"
            );
            assert!(
                space.get("space_id").is_some(),
                "list_spaces must include 'space_id'; got element: {space}"
            );
        }
    }

    #[tokio::test]
    async fn test_handle_request_list_spaces() {
        // list_spaces does not require state — it reads the global registry.
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_spaces","arguments":{}}}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        // Either succeeds with a JSON array, or fails because $HOME is unavailable
        // in the test environment — both are acceptable non-panic outcomes.
        assert!(resp.result.is_some());
    }

    #[tokio::test]
    async fn test_handle_request_subscribe_space_no_state() {
        // subscribe_space without state returns an informative error.
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"subscribe_space","arguments":{"space":"my-space"}}}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert!(result["isError"].as_bool().expect("should have isError"));
        assert!(
            result["content"][0]["text"]
                .as_str()
                .expect("should have text")
                .contains("state unavailable")
        );
    }

    #[tokio::test]
    async fn test_handle_request_unsubscribe_space_no_state() {
        // unsubscribe_space without state returns an informative error.
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"unsubscribe_space","arguments":{"space":"my-space"}}}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert!(result["isError"].as_bool().expect("should have isError"));
        assert!(
            result["content"][0]["text"]
                .as_str()
                .expect("should have text")
                .contains("state unavailable")
        );
    }

    #[tokio::test]
    async fn test_handle_request_unknown_tool() {
        let req = r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"bogus","arguments":{}}}"#;
        let resp = test_handle(req)
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert!(result["isError"].as_bool().expect("should have isError"));
        assert!(
            result["content"][0]["text"]
                .as_str()
                .expect("should have text")
                .contains("unknown tool")
        );
    }

    #[tokio::test]
    async fn test_handle_request_relay_tool_without_proxy() {
        let relay_tools = vec![ToolDefinition {
            name: "read_document".into(),
            description: "Read a document.".into(),
            input_schema: serde_json::json!({"type": "object", "properties": {}}),
        }];
        let req = r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"read_document","arguments":{}}}"#;
        let resp = handle_request(req, &test_context(relay_tools))
            .await
            .expect("tools/call should return a response");
        assert!(resp.error.is_none());
        let result = resp.result.expect("should have result");
        assert!(result["isError"].as_bool().expect("should have isError"));
        assert!(
            result["content"][0]["text"]
                .as_str()
                .expect("should have text")
                .contains("not connected to the relay")
        );
    }

    #[tokio::test]
    async fn test_handle_request_unknown_method() {
        let req = r#"{"jsonrpc":"2.0","id":6,"method":"bogus/method"}"#;
        let resp = test_handle(req)
            .await
            .expect("unknown method should return error");
        assert!(resp.error.is_some());
        let err = resp.error.expect("should have error");
        assert_eq!(err.code, METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn test_handle_request_invalid_json() {
        let resp = test_handle("not json")
            .await
            .expect("invalid JSON should return error");
        assert!(resp.error.is_some());
        let err = resp.error.expect("should have error");
        assert_eq!(err.code, PARSE_ERROR);
    }

    #[tokio::test]
    async fn test_handle_request_invalid_tool_call_params() {
        let req = r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":"bad"}"#;
        let resp = test_handle(req)
            .await
            .expect("bad params should return error");
        assert!(resp.error.is_some());
        let err = resp.error.expect("should have error");
        assert_eq!(err.code, INVALID_PARAMS);
    }

    // --- Space discovery tests ---

    /// Path to a non-existent `spaces.json` inside the given tempdir,
    /// for use as the Strategy-2 registry override in space-discovery
    /// tests. Pointing Strategy 2 at a guaranteed-missing file makes
    /// it fail deterministically regardless of `$KUTL_HOME` or
    /// concurrent tests' env-var pollution.
    fn isolated_registry(dir: &tempfile::TempDir) -> std::path::PathBuf {
        dir.path().join("isolated-spaces.json")
    }

    #[test]
    fn test_discover_space_found() {
        let dir = tempfile::TempDir::new().unwrap();
        let space_root = dir.path();
        let config = SpaceConfig {
            space_id: "3314f713-09a4-40c6-8910-0a2ea70c5c53".into(),
            relay_url: "ws://localhost:9100/ws".into(),
        };
        config.save(space_root).unwrap();

        let found = kutl_client::space_config::discover_space_with_registry(
            space_root,
            Some(&isolated_registry(&dir)),
        );
        assert!(found.is_some());
        let (found_config, found_root) = found.unwrap();
        assert_eq!(
            found_config.space_id,
            "3314f713-09a4-40c6-8910-0a2ea70c5c53"
        );
        assert_eq!(found_root, space_root);
    }

    #[test]
    fn test_discover_space_no_walk_up() {
        // discover_space should NOT walk up to ancestor directories.
        // This guards Strategy 1 — Strategy 2 (`spaces.json` lookup) is
        // bypassed via the override so the test does not need to touch
        // `$KUTL_HOME` and cannot race with sibling tests' env state.
        let dir = tempfile::TempDir::new().unwrap();
        let space_root = dir.path();
        let config = SpaceConfig {
            space_id: "3314f713-09a4-40c6-8910-0a2ea70c5c53".into(),
            relay_url: "ws://localhost:9100/ws".into(),
        };
        config.save(space_root).unwrap();

        let subdir = space_root.join("src").join("deep");
        std::fs::create_dir_all(&subdir).unwrap();

        let found = kutl_client::space_config::discover_space_with_registry(
            &subdir,
            Some(&isolated_registry(&dir)),
        );
        assert!(found.is_none(), "should not walk up to parent");
    }

    #[test]
    fn test_discover_space_not_found() {
        // Use the registry override so Strategy 2 fails deterministically
        // — without it, a populated `$KUTL_HOME/spaces.json` on the host
        // (or pollution from a concurrent test that touched KUTL_HOME)
        // could cause this test to find an unrelated workspace.
        let dir = tempfile::TempDir::new().unwrap();
        let found = kutl_client::space_config::discover_space_with_registry(
            dir.path(),
            Some(&isolated_registry(&dir)),
        );
        assert!(found.is_none());
    }

    // --- Flag filtering tests ---

    fn make_flag_signal(
        author: &str,
        target: &str,
        audience: sync::AudienceType,
        kind: sync::FlagKind,
    ) -> sync::Signal {
        // The deprecated audience pair is the shape under test: these fixtures
        // stand in for records already on disk.
        #[allow(deprecated)]
        let flag = sync::FlagPayload {
            kind: i32::from(kind),
            audience_type: i32::from(audience),
            target_did: if target.is_empty() {
                None
            } else {
                Some(target.into())
            },
            message: "test message".into(),
            audience: None,
            anchor_text: None,
        };
        sync::Signal {
            id: String::new(),
            space_id: "475e1f0c-14a9-4b83-8b73-95c80cfd166d".into(),
            document_id: Some("doc-1".into()),
            author_did: author.into(),
            timestamp: 0,
            payload: Some(sync::signal::Payload::Flag(flag)),
            ..Default::default()
        }
    }

    /// A reply record authored by `author`, hanging off `parent`.
    fn make_reply_signal(author: &str, parent: &str) -> sync::Signal {
        let mut signal = sync::Signal {
            id: "reply-1".into(),
            space_id: "475e1f0c-14a9-4b83-8b73-95c80cfd166d".into(),
            // Production mints a reply with no document: it hangs off its
            // parent signal, not off a document. The fixture matches, so a
            // test can see what a consumer actually gets in `document`.
            document_id: None,
            author_did: author.into(),
            timestamp: 0,
            payload: Some(sync::signal::Payload::Reply(sync::ReplyPayload {
                parent_signal_id: parent.into(),
                body: "the substantive answer".into(),
                ..Default::default()
            })),
            ..Default::default()
        };
        signal.set_event(sync::SignalEventType::Created);
        signal
    }

    /// A lifecycle transition on an existing signal — the shape the relay
    /// broadcasts space-wide on every close, reopen, and tombstone.
    fn make_transition_signal(author: &str, event: sync::SignalEventType) -> sync::Signal {
        let mut signal = sync::Signal {
            id: "sig-parent".into(),
            space_id: "475e1f0c-14a9-4b83-8b73-95c80cfd166d".into(),
            document_id: Some("doc-1".into()),
            author_did: author.into(),
            timestamp: 0,
            payload: Some(sync::signal::Payload::Transition(
                sync::TransitionPayload::default(),
            )),
            ..Default::default()
        };
        signal.set_event(event);
        signal
    }

    #[test]
    fn test_filter_rejects_self_authored() {
        let signal = make_flag_signal(
            "did:key:me",
            "",
            sync::AudienceType::Space,
            sync::FlagKind::Info,
        );
        assert!(!should_deliver_signal(&signal, "did:key:me"));
    }

    #[test]
    fn test_filter_accepts_space_audience() {
        let signal = make_flag_signal(
            "did:key:other",
            "",
            sync::AudienceType::Space,
            sync::FlagKind::Question,
        );
        assert!(should_deliver_signal(&signal, "did:key:me"));
    }

    #[test]
    fn test_filter_accepts_agent_audiences() {
        for audience in [
            sync::AudienceType::AgentOwners,
            sync::AudienceType::AgentEditors,
            sync::AudienceType::AgentViewers,
        ] {
            let signal = make_flag_signal(
                "did:key:other",
                "",
                audience,
                sync::FlagKind::ReviewRequested,
            );
            assert!(
                should_deliver_signal(&signal, "did:key:me"),
                "should accept {audience:?}"
            );
        }
    }

    #[test]
    fn test_filter_participant_matching_target() {
        let signal = make_flag_signal(
            "did:key:other",
            "did:key:me",
            sync::AudienceType::Participant,
            sync::FlagKind::Blocked,
        );
        assert!(should_deliver_signal(&signal, "did:key:me"));
    }

    #[test]
    fn test_filter_participant_wrong_target() {
        let signal = make_flag_signal(
            "did:key:other",
            "did:key:someone-else",
            sync::AudienceType::Participant,
            sync::FlagKind::Blocked,
        );
        assert!(!should_deliver_signal(&signal, "did:key:me"));
    }

    /// The retired role audiences all deliver.
    ///
    /// All six retired role audiences (`human_*` and `agent_*`) deliver as
    /// space-wide — no human/agent distinction is carried for records nobody
    /// can write anymore. Safe because audience is a delivery filter
    /// rather than an access boundary — the agent already has the record
    /// synced; audience only decides whether it is surfaced.
    #[test]
    fn test_filter_delivers_retired_role_audiences() {
        for audience in [
            sync::AudienceType::HumanOwners,
            sync::AudienceType::HumanEditors,
            sync::AudienceType::HumanViewers,
            sync::AudienceType::AgentOwners,
            sync::AudienceType::AgentEditors,
            sync::AudienceType::AgentViewers,
        ] {
            let signal = make_flag_signal("did:key:other", "", audience, sync::FlagKind::Info);
            assert!(
                should_deliver_signal(&signal, "did:key:me"),
                "{audience:?} should deliver as space-wide after the cutover"
            );
        }
    }

    #[test]
    fn test_filter_rejects_unspecified_audience() {
        let signal = make_flag_signal(
            "did:key:other",
            "",
            sync::AudienceType::Unspecified,
            sync::FlagKind::Info,
        );
        assert!(!should_deliver_signal(&signal, "did:key:me"));
    }

    #[test]
    fn test_filter_with_empty_did_accepts_space() {
        let signal = make_flag_signal(
            "did:key:other",
            "",
            sync::AudienceType::Space,
            sync::FlagKind::Info,
        );
        assert!(should_deliver_signal(&signal, ""));
    }

    #[test]
    fn test_filter_with_empty_did_rejects_participant() {
        let signal = make_flag_signal(
            "did:key:other",
            "",
            sync::AudienceType::Participant,
            sync::FlagKind::Info,
        );
        assert!(!should_deliver_signal(&signal, ""));
    }

    // --- Reconnect attempt accounting tests ---

    #[test]
    fn test_next_reconnect_attempts_accumulates_when_never_connected() {
        // Back-to-back connect failures (handshake never succeeded) accumulate.
        let mut attempts = 0;
        for expected in 1..=5 {
            attempts = next_reconnect_attempts(attempts, false);
            assert_eq!(attempts, expected);
        }
    }

    #[test]
    fn test_next_reconnect_attempts_resets_after_connected_session() {
        // A session that connected before dropping resets the streak to 1, so
        // a long-lived watch that drops once per day never exhausts the budget.
        let attempts = next_reconnect_attempts(9, true);
        assert_eq!(
            attempts, 1,
            "a connected session resets the consecutive-failure streak"
        );
    }

    #[test]
    fn test_next_reconnect_attempts_below_threshold_after_intermittent_drops() {
        // Connect-drop-connect-drop... never reaches the give-up threshold.
        let mut attempts = 0;
        for _ in 0..100 {
            attempts = next_reconnect_attempts(attempts, true);
            assert!(attempts < MAX_RECONNECT_ATTEMPTS);
        }
    }

    // --- Flag kind name tests ---

    #[test]
    fn test_flag_kind_names() {
        assert_eq!(flag_kind_name(i32::from(sync::FlagKind::Info)), "info");
        assert_eq!(
            flag_kind_name(i32::from(sync::FlagKind::Completed)),
            "completed"
        );
        assert_eq!(
            flag_kind_name(i32::from(sync::FlagKind::ReviewRequested)),
            "review_requested"
        );
        assert_eq!(
            flag_kind_name(i32::from(sync::FlagKind::Question)),
            "question"
        );
        assert_eq!(
            flag_kind_name(i32::from(sync::FlagKind::Blocked)),
            "blocked"
        );
        assert_eq!(
            flag_kind_name(i32::from(sync::FlagKind::Comment)),
            "comment"
        );
        assert_eq!(flag_kind_name(99), "unknown");
    }

    // --- Channel event construction tests ---

    #[test]
    fn test_build_flag_channel_event() {
        let signal = make_flag_signal(
            "did:key:alice",
            "did:key:bob",
            sync::AudienceType::Participant,
            sync::FlagKind::Question,
        );
        let event = build_signal_channel_event(&signal, "abc123def456", "team-space")
            .expect("a flag has a nameable record kind");

        assert_eq!(event.content, "test message");
        assert_eq!(event.meta["space_id"], "abc123def456");
        assert_eq!(event.meta["space_name"], "team-space");
        // No single "space" key — consumers use space_id (canonical) and
        // space_name (display).
        assert!(event.meta.get("space").is_none());
        assert_eq!(event.meta["kind"], "question");
        assert_eq!(event.meta["record"], "flag");
        assert_eq!(event.meta["document"], "doc-1");
        assert_eq!(event.meta["sender"], "did:key:alice");
    }

    /// A reply produces an event too. It carries the reply's body and names
    /// the signal it hangs off, and its `record` says `reply` while `kind` —
    /// a flag's intent — is absent.
    ///
    /// `document` is empty, because a reply hangs off its parent signal rather
    /// than a document. A consumer reading it as a document id gets a string
    /// that names nothing, so the metadata contract says so.
    #[test]
    fn test_build_reply_channel_event() {
        let signal = make_reply_signal("did:key:alice", "sig-parent");
        let event = build_signal_channel_event(&signal, "abc123def456", "team-space")
            .expect("a reply has a nameable record kind");

        assert_eq!(event.content, "the substantive answer");
        assert_eq!(event.meta["record"], "reply");
        assert_eq!(event.meta["parent_signal_id"], "sig-parent");
        assert_eq!(event.meta["sender"], "did:key:alice");
        assert_eq!(
            event.meta["document"], "",
            "a reply carries no document, and the field says so rather than lying"
        );
        assert!(
            event.meta.get("kind").is_none(),
            "flag intent is a flag-only axis"
        );
    }

    /// A record whose kind cannot be named produces no event at all.
    ///
    /// `record` advertises a closed set — flag, reply, chat, decision — and a
    /// consumer that switches on it has no arm for anything else. Emitting a
    /// value outside that set asks every reader to guess; emitting nothing asks
    /// nothing of them, and the caller re-reads through `get_changes` anyway.
    #[test]
    fn test_build_channel_event_declines_an_unnameable_record() {
        let mut payloadless = make_reply_signal("did:key:alice", "sig-parent");
        payloadless.payload = None;

        assert!(
            build_signal_channel_event(&payloadless, "abc123def456", "team-space").is_none(),
            "no nameable kind means no event"
        );
    }

    // --- subscribe_space anchor validation tests ---

    /// Build a minimal `WatchState` for unit tests. The notify channel is
    /// unbounded-capacity so tests never block; the receiver is dropped
    /// immediately (sends succeed but nobody reads them).
    fn make_test_watch_state() -> WatchState {
        let (notify_tx, _rx) = mpsc::channel::<ChannelEvent>(16);
        WatchState {
            subscriptions: HashMap::new(),
            did: "did:key:test".into(),
            auth_token: "test-token".into(),
            notify_tx,
            in_git_repo: false,
            space_hint: None,
        }
    }

    /// Register a space at `space_root` in a fresh `KUTL_HOME` tempdir and
    /// return both `TempDir`s so the caller keeps them alive for the test.
    fn register_space_in_tempdir(space_id: &str) -> (tempfile::TempDir, tempfile::TempDir) {
        let kutl_home = tempfile::TempDir::new().unwrap();
        let space_dir = tempfile::TempDir::new().unwrap();

        // Write the space config.
        let config = SpaceConfig {
            space_id: space_id.to_string(),
            relay_url: "ws://localhost:9100/ws".into(),
        };
        config.save(space_dir.path()).unwrap();

        // Write the registry.
        let registry = serde_json::json!({
            "spaces": [space_dir.path().to_str().unwrap()]
        });
        std::fs::write(
            kutl_home.path().join("spaces.json"),
            serde_json::to_string(&registry).unwrap(),
        )
        .unwrap();

        (kutl_home, space_dir)
    }

    /// Register a space with a human-readable `.kutlspace` name and a current
    /// (valid) AGENTS.md anchor so `handle_subscribe_space` reaches the
    /// insert step. Returns the two `TempDir`s (kept alive by the caller).
    fn register_named_space_with_valid_anchor(
        space_id: &str,
        space_name: &str,
    ) -> (tempfile::TempDir, tempfile::TempDir) {
        let (kutl_home, space_dir) = register_space_in_tempdir(space_id);
        // Human-readable name so subscribe-by-name resolves to this space_id.
        kutl_client::KutlspaceConfig {
            space_name: space_name.to_string(),
            surface: None,
        }
        .save(space_dir.path())
        .unwrap();
        // Current-version AGENTS.md so the anchor check returns Current.
        let block = crate::agents_md::render(env!("CARGO_PKG_VERSION"));
        std::fs::write(space_dir.path().join("AGENTS.md"), &block).unwrap();
        (kutl_home, space_dir)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_subscribe_by_id_then_name_is_deduplicated() {
        // subscribing by id then by the human-readable name must not spawn
        // a second listener — both resolve to the same canonical space_id key.
        let space_id = "a9b0e348-cf73-4fc0-8494-ea67b0ef012c";
        let space_name = "dedup-team";
        let (kutl_home, _space_dir) = register_named_space_with_valid_anchor(space_id, space_name);

        unsafe { std::env::set_var("KUTL_HOME", kutl_home.path()) };
        let mut state = make_test_watch_state();

        let first = handle_subscribe_space(&mut state, space_id);
        let second = handle_subscribe_space(&mut state, space_name);

        unsafe { std::env::remove_var("KUTL_HOME") };

        assert!(!first.is_error, "first subscribe should succeed");
        assert!(
            second.content[0].text.contains("already subscribed"),
            "subscribe-by-name after subscribe-by-id should report already subscribed, got: {}",
            second.content[0].text
        );
        assert_eq!(
            state.subscriptions.len(),
            1,
            "only one canonical subscription should exist"
        );
        assert!(
            state.subscriptions.contains_key(space_id),
            "subscription must be keyed by canonical space_id"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_unsubscribe_by_name_removes_id_keyed_subscription() {
        // unsubscribe by the human-readable name must reach an id-keyed
        // subscription (created by auto-subscribe / subscribe-by-id).
        let space_id = "a1ec0018-5871-4b4e-831a-d577c1e368c9";
        let space_name = "unsub-team";
        let (kutl_home, _space_dir) = register_named_space_with_valid_anchor(space_id, space_name);

        unsafe { std::env::set_var("KUTL_HOME", kutl_home.path()) };
        let mut state = make_test_watch_state();

        let sub = handle_subscribe_space(&mut state, space_id);
        let unsub = handle_unsubscribe_space(&mut state, space_name);

        unsafe { std::env::remove_var("KUTL_HOME") };

        assert!(!sub.is_error, "subscribe should succeed");
        assert!(
            !unsub.is_error,
            "unsubscribe-by-name should succeed, got: {}",
            unsub.content[0].text
        );
        assert!(
            state.subscriptions.is_empty(),
            "subscription should be removed after unsubscribe-by-name"
        );
    }

    #[tokio::test]
    #[serial_test::serial] // mutates process-global KUTL_HOME; serialize with the other KUTL_HOME tests
    async fn test_handle_subscribe_space_rejects_stale_incompatible_anchor() {
        // Space directory has an AGENTS.md with a sentinel below
        // MIN_COMPATIBLE_KUTL_VERSION (0.0.1 < 0.1.5).
        let space_id = "31eb308f-3880-4062-8405-d534c4bff01b";
        let (kutl_home, space_dir) = register_space_in_tempdir(space_id);

        // Write a stale-incompatible AGENTS.md (v=0.0.1) in the space dir.
        // anchor_for returns space_dir itself since it has no git repo.
        let block = crate::agents_md::render("0.0.1");
        std::fs::write(space_dir.path().join("AGENTS.md"), &block).unwrap();

        unsafe { std::env::set_var("KUTL_HOME", kutl_home.path()) };
        let mut state = make_test_watch_state();
        let result = handle_subscribe_space(&mut state, space_id);
        unsafe { std::env::remove_var("KUTL_HOME") };

        assert!(
            result.is_error,
            "stale-incompatible anchor must return an error"
        );
        let text = &result.content[0].text;
        assert!(
            text.contains("incompatible"),
            "error message should mention incompatible, got: {text}"
        );
        assert!(
            text.contains("kutl space apply"),
            "error message should suggest kutl space apply, got: {text}"
        );
        // Subscription must NOT have been added.
        assert!(
            !state.subscriptions.contains_key(space_id),
            "subscription must not be added on error"
        );
    }

    #[tokio::test]
    #[serial_test::serial] // mutates process-global KUTL_HOME; serialize with the other KUTL_HOME tests
    async fn test_handle_subscribe_space_warns_on_absent_anchor() {
        // Space directory exists but has no AGENTS.md at all. Startup treats
        // this as a warning and serves the space anyway; subscribing to a
        // second space is the same condition and answers the same way.
        // Refusing here would make a space reachable at launch and
        // unreachable by name, for a file that carries guidance, not access.
        let space_id = "bf6a71b2-792d-445f-826b-75faee612a02";
        let (kutl_home, _space_dir) = register_space_in_tempdir(space_id);
        // No AGENTS.md written — absent case.

        unsafe { std::env::set_var("KUTL_HOME", kutl_home.path()) };
        let mut state = make_test_watch_state();
        let result = handle_subscribe_space(&mut state, space_id);
        unsafe { std::env::remove_var("KUTL_HOME") };

        assert!(!result.is_error, "an absent anchor must not refuse");
        let text = &result.content[0].text;
        assert!(
            text.contains("no agent instructions"),
            "the answer should still name the absent instructions, got: {text}"
        );
        assert!(
            text.contains("kutl space apply"),
            "the answer should suggest kutl space apply, got: {text}"
        );
        assert!(
            state.subscriptions.contains_key(space_id),
            "the subscription must be added despite the warning"
        );
    }

    // test_handle_subscribe_space_warns_on_stale_compatible_anchor is omitted:
    // StaleCompatible is unreachable whenever MIN_COMPATIBLE_KUTL_VERSION equals
    // this crate's version, because then every sentinel older than the running
    // binary is also below the minimum, collapsing StaleCompatible →
    // StaleIncompatible. They are two independent constants that happen to be
    // equal today; the band reopens the moment a release ships without moving
    // the minimum, and this test becomes writable then.

    // --- Agent credential bootstrap tests ---

    /// The explicit `--agent` flag wins over `KUTL_AGENT`; `KUTL_AGENT` wins
    /// over the default; nothing set falls back to `default`.
    #[test]
    #[serial_test::serial] // mutates process-global KUTL_AGENT
    fn test_resolve_agent_name_precedence() {
        // SAFETY: env-mutating test serialized via `#[serial]`.
        unsafe { std::env::remove_var("KUTL_AGENT") };
        assert_eq!(
            resolve_agent_name(None),
            DEFAULT_AGENT_NAME,
            "nothing set -> default"
        );
        assert_eq!(
            resolve_agent_name(Some("flagged")),
            "flagged",
            "explicit flag with no env -> flag"
        );

        // SAFETY: env-mutating test serialized via `#[serial]`.
        unsafe { std::env::set_var("KUTL_AGENT", "from-env") };
        assert_eq!(
            resolve_agent_name(None),
            "from-env",
            "KUTL_AGENT wins over the default"
        );
        assert_eq!(
            resolve_agent_name(Some("flagged")),
            "flagged",
            "the explicit flag wins over KUTL_AGENT"
        );
        // SAFETY: env-mutating test serialized via `#[serial]`.
        unsafe { std::env::remove_var("KUTL_AGENT") };
    }

    /// The credential bootstrap loads the AGENT keyfile from
    /// `agent_identity_path(name)` (NOT `identity.json`) and exposes its DID +
    /// signing key to the authoring layer — the agent principal `mcp serve`
    /// authors as.
    #[test]
    #[serial_test::serial] // mutates process-global KUTL_HOME
    fn test_load_agent_identity_loads_agent_keyfile_not_human() {
        let kutl_home = tempfile::TempDir::new().unwrap();
        // SAFETY: env-mutating test serialized via `#[serial]`.
        unsafe { std::env::set_var("KUTL_HOME", kutl_home.path()) };

        // A DISTINCT human identity.json must NOT be what the agent bootstrap
        // loads — write one and prove the agent DID differs from it.
        let human = kutl_client::Identity::generate();
        human
            .save(&kutl_client::default_identity_path().unwrap())
            .unwrap();

        // Provision the agent keyfile the way `kutl agent create` does.
        let agent = kutl_client::Identity::generate();
        let agent_path = kutl_client::agent_identity_path("claude-laptop").unwrap();
        agent.save(&agent_path).unwrap();

        let loaded = load_agent_identity("claude-laptop").expect("agent keyfile loads");
        // SAFETY: env-mutating test serialized via `#[serial]`.
        unsafe { std::env::remove_var("KUTL_HOME") };

        assert_eq!(
            loaded.did, agent.did,
            "loads the AGENT did, not the human's"
        );
        assert_ne!(
            loaded.did, human.did,
            "the agent principal is cryptographically distinct from the human"
        );
        // The signing key is usable by the authoring layer (decodes to the
        // agent's own key pair — the DID it will sign records against).
        let key = loaded
            .decode_signing_key()
            .expect("agent signing key decodes");
        assert_eq!(
            kutl_signals::did_key_encode(&key.verifying_key()),
            agent.did,
            "the exposed signing key matches the agent DID"
        );
    }

    /// An absent agent keyfile yields a clear error carrying the
    /// `kutl agent create` hint.
    #[test]
    #[serial_test::serial] // mutates process-global KUTL_HOME
    fn test_load_agent_identity_missing_keyfile_hints_create() {
        let kutl_home = tempfile::TempDir::new().unwrap();
        // SAFETY: env-mutating test serialized via `#[serial]`.
        unsafe { std::env::set_var("KUTL_HOME", kutl_home.path()) };

        let err = load_agent_identity("nope").expect_err("missing keyfile is an error");
        // SAFETY: env-mutating test serialized via `#[serial]`.
        unsafe { std::env::remove_var("KUTL_HOME") };

        let msg = format!("{err:#}");
        assert!(
            msg.contains("kutl agent create --name nope"),
            "error must hint the create command, got: {msg}"
        );
    }

    #[test]
    fn test_authz_guidance_enriches_a_403_not_authorized() {
        // The relay returns 403 "not authorized" when the agent DID is absent
        // from authorized_keys (or not scoped to this space). The guidance must
        // echo the DID and the space-scoped authorized_keys line so the operator
        // can fix it, replacing the placeholder with the space uuid we know.
        let did = "did:key:z6MkAgentNeedsAuthorizing";
        let space = "11111111-2222-3333-4444-555555555555";
        let err = "transmitting: signed signal create rejected: signed signal create returned 403 Forbidden: not authorized";
        let guidance = authz_failure_guidance(err, did, space)
            .expect("a 403 not-authorized error must produce guidance");
        assert!(
            guidance.contains(did),
            "guidance must echo the DID: {guidance}"
        );
        assert!(
            guidance.contains("authorized_keys"),
            "guidance must name authorized_keys: {guidance}"
        );
        // The scope must be the REAL space uuid (we know it here), not the
        // placeholder — the operator has nothing left to substitute.
        assert!(
            guidance.contains(&format!("scope={space}")),
            "guidance must carry the real space uuid in the scope: {guidance}"
        );
        assert!(
            !guidance.contains(SPACE_UUID_PLACEHOLDER),
            "guidance must not leave the placeholder in place: {guidance}"
        );
    }

    #[test]
    fn test_authz_guidance_matches_the_bare_not_authorized_reason() {
        // Match on the relay's reason string too, not only the numeric status,
        // so the mapping is robust to context re-wrapping.
        let did = "did:key:z6MkAnotherAgent";
        let space = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let guidance = authz_failure_guidance("relay said: not authorized", did, space)
            .expect("the not-authorized reason must produce guidance");
        assert!(guidance.contains(did));
    }

    #[test]
    fn test_authz_guidance_ignores_non_authz_errors() {
        // A 400 record-rejected, a 404, a transport error, etc. are NOT authz
        // failures — the mapper must return None so the original error is not
        // swallowed or mis-described as an authorization problem.
        let did = "did:key:z6MkAgent";
        let space = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        assert!(
            authz_failure_guidance("returned 400 Bad Request: record rejected", did, space)
                .is_none(),
            "a 400 must not be treated as an authz failure"
        );
        assert!(
            authz_failure_guidance("returned 404 Not Found: signal not found", did, space)
                .is_none(),
            "a 404 must not be treated as an authz failure"
        );
        assert!(
            authz_failure_guidance(
                "error sending request for url: connection refused",
                did,
                space
            )
            .is_none(),
            "a transport error must not be treated as an authz failure"
        );
    }
}
