//! kutl CLI — collaborative text synchronization tool.

mod agent;
mod agents_md;
mod daemon_mgmt;
mod dirs;
mod identity;
mod join_auth;
mod signals;
mod space;
mod status;
mod supervisor;
mod watch;
mod watch_tools;

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use jiff::Timestamp;

use kutl_core::{Boundary, Change, Document};

/// How a user updates kutl (it ships via the `kutl-io/homebrew-tap`).
const UPDATE_HINT: &str = "kutl is installed via Homebrew — run `brew upgrade kutl` to update.";

#[derive(Parser)]
#[command(
    name = "kutl",
    version,
    about = "kutl — keep a folder of text documents in sync across people and agents"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    // --- blessed top-level shortcuts (also valid as `kutl space <verb>`) ---
    /// Create a new space — a synced folder of documents — in the current
    /// directory (shortcut for `kutl space init`).
    Init(InitArgs),
    /// Join an existing space in the current directory (shortcut for
    /// `kutl space join`).
    Join(JoinArgs),
    /// Sync the current space once, then exit (shortcut for `kutl space sync`).
    Sync(DirArgs),
    /// Copy this space's documents out to its configured surface target,
    /// e.g. into the enclosing git repo (shortcut for `kutl space surface`).
    Surface(DirArgs),
    /// Show everything at a glance: the background sync daemon, every
    /// registered space, their relays, and your identity.
    Status(StatusArgs),
    // --- entities ---
    /// Manage spaces — the synced folders kutl keeps in step.
    Space(SpaceCli),
    /// Manage the daemon — the background process that watches and syncs
    /// your spaces.
    Daemon(DaemonCli),
    /// Authenticate with a relay — the server your spaces sync through.
    Auth(AuthCli),
    /// Set or read your identity details (name, email).
    Config(ConfigCli),
    /// Document history: change log, per-line authorship, point-in-time restore.
    Document(DocumentCli),
    /// Signals — how people ask each other for things and settle them: create a
    /// question or a review for someone, reply to one you have been sent, and
    /// close it once it is answered. Flags, chats, and decisions attach to a
    /// space or one of its documents.
    ///
    /// The verbs named here are the real ones, deliberately. A description that
    /// reaches for a natural synonym teaches a command that does not exist —
    /// this line said "raise" for months, and three demo runs in a row spent a
    /// turn discovering there is no `kutl signal raise`.
    Signal(SignalCli),
    /// Provision keypairs for tool-held agents (e.g. the MCP server).
    Agent(agent::AgentCli),
    /// Run the Model Context Protocol (MCP) server that gives agents access.
    Mcp(McpCli),
    /// Show how to update kutl.
    Update,
}

/// Shared arguments for `kutl init` and `kutl space init`.
#[derive(clap::Args, Clone)]
struct InitArgs {
    /// Relay WebSocket URL.
    #[arg(long, default_value = space::DEFAULT_RELAY_URL)]
    relay: String,
    /// Human-readable space name (auto-generated if omitted).
    #[arg(long)]
    name: Option<String>,
    /// Target directory (defaults to current directory).
    #[arg(long)]
    dir: Option<PathBuf>,
    /// Inside a git repo, create the space in this subfolder.
    ///
    /// Defaults to "kutl" with an interactive prompt to override.
    #[arg(long)]
    subfolder: Option<String>,
}

/// Shared arguments for `kutl join` and `kutl space join`.
///
/// Accepts three target forms:
///   - A full invite URL (`https://...`) — relay extracted from the URL
///   - An owner/slug identifier (`alice/my-project`) — connects via the
///     hosted relay
///   - A bare name — resolves via the local relay
///
/// If invoked inside a folder containing `.kutlspace`, the `space_name`
/// from that file is used and the argument may be omitted.
#[derive(clap::Args, Clone)]
struct JoinArgs {
    /// Space target: invite URL, owner/slug, or bare name.
    target: Option<String>,
    /// Relay WebSocket URL (optional; inferred from target when possible).
    #[arg(long)]
    relay: Option<String>,
    /// Target directory (defaults to current directory).
    #[arg(long)]
    dir: Option<PathBuf>,
    /// Inside a git repo, create the space in this subfolder.
    #[arg(long)]
    subfolder: Option<String>,
}

/// Shared arguments for commands that take only a target directory
/// (`sync`, `surface`, `space apply`).
#[derive(clap::Args, Clone)]
struct DirArgs {
    /// Target directory (defaults to current directory).
    #[arg(long)]
    dir: Option<PathBuf>,
}

/// Shared arguments for read commands that select an output format.
#[derive(clap::Args, Clone)]
struct StatusArgs {
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::default())]
    format: OutputFormat,
}

/// Space entity: `kutl space <verb>`.
#[derive(clap::Args)]
struct SpaceCli {
    #[command(subcommand)]
    action: SpaceAction,
}

#[derive(Subcommand)]
enum SpaceAction {
    /// Initialize a new kutl space in a directory.
    Init(InitArgs),
    /// Join an existing space.
    Join(JoinArgs),
    /// Sync once: push local changes and pull remote changes, then exit.
    Sync(DirArgs),
    /// Copy this space's documents out to its configured surface target,
    /// e.g. into the enclosing git repo.
    Surface(DirArgs),
    /// List the spaces registered on this client.
    List(StatusArgs),
    /// Show the health of the space you are standing in, plus its relay.
    /// (Use bare `kutl status` for the all-spaces view.)
    Status(StatusArgs),
    /// List who may act in this space — the authorized roster, whether or
    /// not each participant is connected right now. Presence is a status,
    /// not a filter: you address someone precisely because they are away.
    Participants(StatusArgs),
    /// Regenerate kutl's managed AGENTS.md block to match this binary.
    ///
    /// Refreshes the kutl-managed section of AGENTS.md against the running
    /// binary's instructions template. Use this when AGENTS.md was
    /// generated by an older kutl version and is out of date. Anchors at
    /// the local kutl space's project root (the git repo root if any,
    /// otherwise the space directory). Bails if no kutl space is present
    /// at or under the anchor.
    Apply(DirArgs),
    /// Show this space's activity feed — edits and signals, newest first.
    Feed(signals::FeedArgs),
    /// Reserved: space-scoped settings — get/set/list (not yet built).
    Config {
        /// Accepted and ignored while reserved (e.g. `get`, `set KEY VALUE`, `list`).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Forget the space you are standing in on this machine: remove it from
    /// this client's registry. The working tree (including its `.kutl/`
    /// state) is kept, and your membership on the relay is untouched (leaving
    /// server-side is a relay operation) — `kutl init` in the folder
    /// re-attaches it.
    Leave,
    /// Reserved: delete a space (not yet built).
    Delete,
}

/// Document entity: `kutl document <verb>`.
#[derive(clap::Args)]
struct DocumentCli {
    #[command(subcommand)]
    action: DocumentAction,
}

#[derive(Subcommand)]
enum DocumentAction {
    /// Show the change history of a document.
    Log {
        /// Path to the document's working-tree file.
        path: PathBuf,
        #[arg(long, value_enum, default_value_t = OutputFormat::default())]
        format: OutputFormat,
    },
    /// Show per-line authorship of a text document (git-blame for a CRDT).
    Blame {
        /// Path to the document's working-tree file.
        path: PathBuf,
        #[arg(long, value_enum, default_value_t = OutputFormat::default())]
        format: OutputFormat,
    },
    /// Restore a document to an earlier point in time (Google-Docs style):
    /// reconstruct its content as of `--at <time>` or `--to <change-id>` and
    /// re-assert it as a new forward edit. Text documents only.
    Restore(RestoreArgs),
}

/// Arguments for `kutl document restore`.
///
/// Restore reconstructs the document's content as of the selected point and
/// writes it back to the working file. A running daemon then diffs the
/// rewritten file forward as a new edit — so a concurrent editor's later ops
/// merge rather than being destroyed (read-your-writes needs the daemon; same
/// caveat class as signal records).
#[derive(clap::Args, Clone)]
struct RestoreArgs {
    /// Path to the document's working-tree file.
    path: PathBuf,
    /// Restore to the newest change at or before this time — an RFC3339
    /// timestamp (2026-07-18T15:00:00Z) or a civil date/datetime
    /// (2026-07-18 or 2026-07-18T15:00), the latter read in the system time
    /// zone. Mutually exclusive with --to. (Natural-language times like
    /// "yesterday 3pm" are a future nicety.)
    #[arg(long, conflicts_with = "to")]
    at: Option<String>,
    /// Restore to the exact change with this id (as shown by `kutl document
    /// log`). Mutually exclusive with --at.
    #[arg(long)]
    to: Option<String>,
}

/// Signal entity: `kutl signal <verb>`.
#[derive(clap::Args)]
struct SignalCli {
    #[command(subcommand)]
    action: SignalAction,
}

/// How to address a flag from this surface. The neutral half — why you would
/// create one at all — is [`kutl_relay::mcp_tools::CREATE_GUIDANCE`], which the
/// MCP `create_flag` tool description carries too.
const CLI_RAISE_INVOCATION: &str = "\
From here: `--audience space` is the default and the usual choice — it puts the flag in front \
of everyone in the space. `--audience participant --to <name>` narrows it to one recipient, \
naming them the way people do; `--to` also takes a DID if you have one. A name that matches \
two participants is refused rather than guessed at, and someone the relay knows no name for is \
still reachable by DID.";

/// The reply verb's own invocation text; the neutral half is
/// [`kutl_relay::mcp_tools::REPLY_GUIDANCE`].
const CLI_REPLY_INVOCATION: &str = "\
From here: the parent is a signal id (a unique prefix of 4+ characters works). Use \
`--parent-reply` to nest under an existing reply instead of the signal itself. Replies do not \
appear locally until the daemon ingests them, or until the next `kutl signal list --fetch`.";

#[derive(Subcommand)]
enum SignalAction {
    /// Create a signal — a flag — in the space you are standing in.
    #[command(long_about = format!(
        "{}\n\n{CLI_RAISE_INVOCATION}",
        kutl_relay::mcp_tools::CREATE_GUIDANCE
    ))]
    Create(SignalCreateArgs),
    /// List signals: a state query — what is open in the space right now.
    ///
    /// Not an inbox: nothing is pushed or marked read, so run it again
    /// whenever the current picture matters. Rows addressed to a
    /// participant carry `→ <did>`; unmarked rows are space-wide. Lists
    /// the current space (all registered spaces when run outside one)
    /// from the local mirror; `--fetch` pulls from the relay first.
    List(SignalListArgs),
    /// Show one signal's detail and its full transition history.
    View {
        /// Signal id to inspect (a unique prefix of 4+ characters works).
        id: String,
        /// Output format.
        #[arg(long, value_enum, default_value_t)]
        format: OutputFormat,
        /// Pull the latest records from the relay before reading. When the
        /// running daemon holds the store (it is already live-syncing), the
        /// pull is skipped with a note and the local copy is read.
        #[arg(long)]
        fetch: bool,
    },
    /// Reply to a signal via the relay.
    #[command(long_about = format!(
        "{}\n\n{CLI_REPLY_INVOCATION}",
        kutl_relay::mcp_tools::REPLY_GUIDANCE
    ))]
    Reply {
        /// The signal being replied to (a unique id prefix of 4+ characters
        /// works).
        parent_signal_id: String,
        /// The reply body.
        #[arg(long)]
        message: String,
        /// Reply to an existing reply instead of directly to the signal,
        /// nesting this one inside that thread (full reply id).
        #[arg(long)]
        parent_reply: Option<String>,
    },
    /// Close a signal via the relay (append a CLOSED transition record).
    #[command(long_about = format!(
        "{}\n\nDecisions are document edits: closing a decision flips its `## ?` heading to \
         `## =` in the document as your edit, and the close record follows from that edit. \
         Only `--reason resolved` applies to a decision; remove the heading to withdraw it.",
        kutl_relay::mcp_tools::CLOSE_GUIDANCE
    ))]
    Close {
        /// Signal id to close (a unique prefix of 4+ characters works).
        id: String,
        /// Close reason (defaults to `resolved`).
        #[arg(long, value_enum)]
        reason: Option<signals::CloseReasonArg>,
    },
    /// Reopen a previously-closed signal via the relay.
    #[command(long_about = format!(
        "{}\n\nDecisions are document edits: reopening a decision flips its `## =` heading \
         back to `## ?` in the document as your edit.",
        kutl_relay::mcp_tools::REOPEN_GUIDANCE
    ))]
    Reopen {
        /// Signal id to reopen (a unique prefix of 4+ characters works).
        id: String,
    },
    /// Resolve a signal — sugar for `close --reason resolved`.
    ///
    /// Decisions are document edits: resolving a decision flips its `## ?`
    /// heading to `## =` in the document as your edit, and the close record
    /// follows from that edit.
    Resolve {
        /// Signal id to resolve (a unique prefix of 4+ characters works).
        id: String,
    },
}

/// Arguments for `kutl signal create`.
#[derive(clap::Args, Clone)]
pub(crate) struct SignalCreateArgs {
    /// What kind of attention this warrants. `comment` is listed but not
    /// creatable here — it needs the inline marker the editor/MCP flow binds.
    #[arg(long, value_enum, default_value_t = signals::FlagKindArg::Info)]
    pub(crate) kind: signals::FlagKindArg,
    /// The flag's message.
    #[arg(long)]
    pub(crate) message: String,
    /// Attach the flag to a document (path within the space, relative to the
    /// space root). Omit for a space-level flag.
    #[arg(long)]
    pub(crate) doc: Option<String>,
    /// Audience: `space` (default, a broadcast) or `participant`.
    ///
    /// The six group audiences (`human_owners`, `agent_editors`, …) are legacy
    /// values that cannot be authored — they never had filtering behind them on
    /// any surface but the agent watch, and the typed audience has no arm for
    /// them. Existing signals that carry one still read back fine.
    #[arg(long, default_value = "space")]
    pub(crate) audience: String,
    /// Who to reach — required when --audience is `participant`, forbidden when
    /// --audience is `space`.
    ///
    /// Takes a participant's name, or their DID. A name is matched exactly and
    /// resolved by the relay; one that names two participants is refused rather
    /// than guessed at. Someone with no name configured is reachable by DID.
    #[arg(long = "to", value_name = "NAME|DID")]
    pub(crate) to: Option<String>,
}

/// The mutually-informative status selector for `kutl signal list`.
///
/// Flattened into [`SignalListArgs`] so the three flags stay top-level while
/// keeping the parent under clippy's bool-count threshold. Nothing selected
/// defaults to open only; `--all` (or `--open --closed`) shows open + closed.
/// Tombstoned signals are never shown regardless.
#[derive(clap::Args, Clone)]
struct StatusFlags {
    /// Show only open signals (the default).
    #[arg(long)]
    open: bool,
    /// Show only closed signals.
    #[arg(long)]
    closed: bool,
    /// Show both open and closed signals.
    #[arg(long)]
    all: bool,
}

/// Arguments for `kutl signal list`.
#[derive(clap::Args, Clone)]
struct SignalListArgs {
    /// Restrict to signals attached to this document (working-tree path or id).
    #[arg(long)]
    doc: Option<String>,
    /// Filter by record type: flag, chat, decision, reply. (The `--flag-kind`
    /// flag filters by the flag's intent — info/question/blocked/… — instead;
    /// the two axes are orthogonal.)
    #[arg(long, value_enum)]
    kind: Option<signals::SignalKind>,
    /// Filter by the flag's intent — what kind of attention it asks for. (The
    /// `--kind` flag filters by record type — flag/chat/decision/reply —
    /// instead; the two axes are orthogonal.)
    #[arg(long, value_enum)]
    flag_kind: Option<signals::FlagKindArg>,
    /// Status selector (`--open` / `--closed` / `--all`).
    #[command(flatten)]
    status: StatusFlags,
    /// Output format.
    #[arg(long, value_enum, default_value_t)]
    format: OutputFormat,
    /// Pull the latest records from the relay before reading. When the
    /// running daemon holds the store (it is already live-syncing), the
    /// pull is skipped with a note and the local copy is read.
    #[arg(long)]
    fetch: bool,
}

/// MCP entity: `kutl mcp <verb>`.
#[derive(clap::Args)]
struct McpCli {
    #[command(subcommand)]
    action: McpAction,
}

#[derive(Subcommand)]
enum McpAction {
    /// Run the MCP server over stdio (the agent entry point).
    ///
    /// Serves as a Claude Code channel and MCP tool server over stdio.
    Serve {
        /// The tool-held agent key to authenticate as: it signs the
        /// relay's did:key challenge, and the relay authors signal records
        /// under that DID. Resolves to `--agent` -> `KUTL_AGENT`
        /// env -> `default`; load the keyfile from
        /// `$KUTL_HOME/agents/<name>.toml` (provision it with
        /// `kutl agent create --name <name>`).
        #[arg(long)]
        agent: Option<String>,
    },
}

/// Output format selector for read commands.
#[derive(clap::ValueEnum, Clone, Copy, Debug, Default)]
pub(crate) enum OutputFormat {
    /// Human-readable summary (default).
    #[default]
    Human,
    /// Machine-readable JSON (stable schema).
    Json,
}

/// Daemon entity: `kutl daemon <verb>`.
#[derive(clap::Args)]
struct DaemonCli {
    #[command(subcommand)]
    action: DaemonAction,
}

#[derive(Subcommand)]
enum DaemonAction {
    /// Run the daemon in the foreground (watches all registered spaces).
    Run,
    /// Start the daemon as a background process.
    Start,
    /// Stop the running daemon.
    Stop,
    /// Show focused daemon status.
    Status(StatusArgs),
}

/// Config entity: `kutl config <verb>`.
#[derive(clap::Args)]
struct ConfigCli {
    #[command(subcommand)]
    action: ConfigAction,
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Set a configuration value.
    Set {
        /// Field name (name, email).
        key: String,
        /// New value.
        value: String,
    },
    /// Read a configuration value, or all values when no key is given.
    Get {
        /// Field name (name, email). Omit to read all set keys.
        key: Option<String>,
        /// Output format.
        #[arg(long, value_enum, default_value_t)]
        format: OutputFormat,
    },
    /// List all set configuration values.
    List(StatusArgs),
}

/// Auth entity: `kutl auth <verb>`.
#[derive(clap::Args)]
struct AuthCli {
    #[command(subcommand)]
    action: AuthAction,
}

#[derive(Subcommand)]
enum AuthAction {
    /// Log in to a kutl relay.
    Login {
        /// Personal Access Token (skip browser auth).
        #[arg(long)]
        token: Option<String>,
        /// Relay URL (default: read from auth.toml or use default).
        #[arg(long)]
        relay: Option<String>,
    },
    /// Store a personal access token.
    Token {
        /// The token value (e.g. `kutl_abc123`...).
        token: String,
        /// Relay URL to associate with this token.
        #[arg(long)]
        relay: Option<String>,
    },
    /// Log out and delete stored credentials.
    Logout,
    /// Show focused authentication status.
    Status(StatusArgs),
}

/// Install a process-default rustls [`CryptoProvider`] before any TLS is
/// attempted.
///
/// The dependency tree pulls in both `ring` and `aws-lc-rs` (via reqwest,
/// tokio-tungstenite, and friends). rustls 0.23 refuses to auto-select a
/// provider when more than one is present and panics on first use of the
/// process-default provider with "could not automatically determine the
/// process-level `CryptoProvider`". The `reqwest` paths (auth login, relay-policy
/// fetch) build their config with an explicit provider and are unaffected, but
/// the `tokio-tungstenite` relay connection relies on the process default — so
/// `kutl join`/`sync` panic on the first `wss://` handshake without this call.
/// Idempotent: if a provider is already installed the `Err` is ignored.
fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Report a reserved-but-unbuilt command clearly, then exit non-zero.
///
/// The top-level `main` returns `Result<()>`, so returning this error from a
/// dispatch arm prints the message to stderr and exits with a failing status —
/// distinguishing a deliberately-reserved command from a plain typo (which clap
/// rejects with an opaque "unrecognized subcommand").
fn reserved_command(name: &str) -> anyhow::Error {
    anyhow::anyhow!("`kutl {name}` is reserved but not yet built")
}

#[tokio::main]
async fn main() -> Result<()> {
    install_crypto_provider();
    let cli = Cli::parse();

    match cli.command {
        // Blessed shortcuts → same handler as the canonical space verb.
        Command::Init(a) => {
            cmd_init(&a.relay, a.name.as_deref(), a.dir, a.subfolder.as_deref()).await
        }
        Command::Join(a) => {
            cmd_join(
                a.target.as_deref(),
                a.relay.as_deref(),
                a.dir,
                a.subfolder.as_deref(),
            )
            .await
        }
        Command::Sync(a) => cmd_sync(a.dir).await,
        Command::Surface(a) => cmd_surface(a.dir),
        Command::Status(a) => cmd_status(a.format).await,

        Command::Space(SpaceCli { action }) => match action {
            SpaceAction::Init(a) => {
                cmd_init(&a.relay, a.name.as_deref(), a.dir, a.subfolder.as_deref()).await
            }
            SpaceAction::Join(a) => {
                cmd_join(
                    a.target.as_deref(),
                    a.relay.as_deref(),
                    a.dir,
                    a.subfolder.as_deref(),
                )
                .await
            }
            SpaceAction::Sync(a) => cmd_sync(a.dir).await,
            SpaceAction::Surface(a) => cmd_surface(a.dir),
            SpaceAction::List(a) => cmd_space_list(a.format),
            SpaceAction::Status(a) => cmd_space_status(a.format).await,
            SpaceAction::Participants(a) => cmd_space_participants(a.format).await,
            SpaceAction::Apply(a) => cmd_space_apply(a.dir),
            SpaceAction::Feed(args) => signals::cmd_space_feed(args).await,
            SpaceAction::Config { .. } => Err(reserved_command("space config")),
            SpaceAction::Leave => cmd_space_leave(),
            SpaceAction::Delete => Err(reserved_command("space delete")),
        },
        Command::Document(DocumentCli { action }) => match action {
            DocumentAction::Log { path, format } => cmd_log(&path, format),
            DocumentAction::Blame { path, format } => cmd_blame(&path, format),
            DocumentAction::Restore(args) => cmd_restore(&args),
        },
        Command::Signal(SignalCli { action }) => match action {
            SignalAction::Create(args) => signals::cmd_signal_create(args).await,
            SignalAction::List(args) => signals::cmd_signal_list(args).await,
            SignalAction::View { id, format, fetch } => {
                signals::cmd_signal_view(&id, format, fetch).await
            }
            SignalAction::Reply {
                parent_signal_id,
                message,
                parent_reply,
            } => {
                signals::cmd_signal_reply(&parent_signal_id, &message, parent_reply.as_deref())
                    .await
            }
            SignalAction::Close { id, reason } => signals::cmd_signal_close(&id, reason).await,
            SignalAction::Reopen { id } => signals::cmd_signal_reopen(&id).await,
            SignalAction::Resolve { id } => signals::cmd_signal_resolve(&id).await,
        },
        Command::Agent(cli) => agent::run(cli),
        Command::Mcp(McpCli { action }) => match action {
            McpAction::Serve { agent } => cmd_mcp_serve(agent.as_deref()).await,
        },
        Command::Daemon(DaemonCli { action }) => cmd_daemon(action).await,
        Command::Auth(AuthCli { action }) => cmd_auth(action).await,
        Command::Config(ConfigCli { action }) => cmd_config(action),
        Command::Update => {
            println!("{UPDATE_HINT}");
            Ok(())
        }
    }
}

/// Save a space configuration and register it globally.
///
/// Uses `dir` if provided, otherwise falls back to the current directory.
/// Returns the space root path. Fails if `.kutl/space.toml` already exists.
fn print_join_success(
    header: &str,
    space_id: &str,
    space_name: &str,
    relay_url: &str,
    space_root: &std::path::Path,
) {
    println!("{header}");
    println!("  space_id:   {space_id}");
    println!("  space_name: {space_name}");
    println!("  relay:      {relay_url}");
    println!(
        "  config:     {}",
        space::SpaceConfig::path(space_root).display()
    );
    println!();
    println!("Run `kutl daemon start` to begin syncing.");
}

fn save_space_config(
    space_id: &str,
    space_name: &str,
    relay_url: &str,
    dir: Option<PathBuf>,
) -> Result<std::path::PathBuf> {
    let space_root = resolve_dir(dir)?;
    if space::SpaceConfig::is_joined(&space_root) {
        let existing = space::SpaceConfig::load(&space_root)?;
        if existing.space_id == space_id {
            // The SAME space is already configured here — a tree kept by
            // `kutl space leave`, or a repeated join. Re-attaching is just
            // re-registering on this client (idempotent), never a rewrite.
            register_space_root(&space_root)?;
            println!("Re-attached the existing space in this directory.");
            return Ok(space_root);
        }
        println!("A DIFFERENT space is already initialized in this directory.");
        println!("  space_id:   {}", existing.space_id);
        println!("  relay:      {}", existing.relay_url);
        if let Some(ks) = kutl_client::KutlspaceConfig::load(&space_root)? {
            println!("  space_name: {}", ks.space_name);
        }
        anyhow::bail!("remove .kutl/space.toml first to reinitialize");
    }

    // Write the team-wide .kutlspace and the kutl-managed .gitignore FIRST.
    // Both are git-tracked and not synced via the relay. The
    // sentinel file `.kutl/space.toml` is written last so partial failures
    // (during these writes) leave the space re-initializable: re-running
    // `kutl init` will overwrite the partial state cleanly.
    let kutlspace = kutl_client::KutlspaceConfig {
        space_name: space_name.to_owned(),
        surface: None,
    };
    kutlspace.save(&space_root)?;
    kutl_client::write_space_gitignore(&space_root)?;

    // Now write the sentinel. After this point the space is "initialized"
    // and `kutl init` will refuse to re-run without manual cleanup.
    let config = space::SpaceConfig {
        space_id: space_id.to_owned(),
        relay_url: relay_url.to_owned(),
    };
    config.save(&space_root)?;

    register_space_root(&space_root)?;

    Ok(space_root)
}

/// Register a space root in this client's registry (idempotent) and nudge a
/// running daemon to pick it up.
fn register_space_root(space_root: &std::path::Path) -> Result<()> {
    let path_str = space_root.display().to_string();
    space::SpaceRegistry::update(|registry| {
        registry.add(&path_str);
    })?;

    // Notify the running daemon (if any) to pick up the space.
    if let Err(e) = daemon_mgmt::signal_reload() {
        tracing::debug!(error = %e, "could not signal daemon reload");
    }
    Ok(())
}

/// Default subfolder name when initializing inside a git repo.
const DEFAULT_KUTL_SUBFOLDER: &str = "kutl";

/// Prompt the user for the subfolder name to use inside a git repo.
///
/// Defaults to [`DEFAULT_KUTL_SUBFOLDER`]. Reads one line from stdin. If
/// stdin is empty (e.g. piped from `/dev/null` in CI), uses the default.
fn prompt_subfolder_name(repo_root: &std::path::Path) -> String {
    use std::io::{self, BufRead, Write};

    eprintln!(
        "Detected git repo at {}. kutl spaces live in a",
        repo_root.display()
    );
    eprintln!("dedicated subfolder so they do not conflict with git operations.");
    eprint!("Subfolder name [{DEFAULT_KUTL_SUBFOLDER}]: ");
    io::stderr().flush().ok();

    let stdin = io::stdin();
    let mut line = String::new();
    if stdin.lock().read_line(&mut line).is_err() || line.trim().is_empty() {
        return DEFAULT_KUTL_SUBFOLDER.to_owned();
    }
    line.trim().to_owned()
}

/// Return `true` if any kutl space is already initialized under `repo_root`.
///
/// Checks the repo root itself (bare-space case) and all immediate
/// subdirectories (the default subfolder layout). Used by the
/// `--update` short-circuit to detect whether an existing space is present
/// without needing to know the exact subfolder name.
///
/// **Scope is one level by design.** `.kutlspace` is placed either at
/// the repo root (non-git or bare-space init) or in exactly one chosen
/// subfolder (`repo_root/<subfolder>/.kutlspace`). Recursing deeper would
/// pick up unrelated kutl spaces nested in other projects' working trees
/// (e.g. a vendored dependency, a sibling spike) and produce surprising
/// `--update` behavior — refreshing AGENTS.md against an "anchor" that isn't
/// the user's. Do not "fix" this into a recursive walk.
fn has_any_space_under(repo_root: &std::path::Path) -> bool {
    // Check the repo root itself (bare-space or non-git init).
    if repo_root.join(".kutlspace").exists() {
        return true;
    }
    // Scan immediate subdirectories for a .kutlspace file.
    let Ok(entries) = std::fs::read_dir(repo_root) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() && path.join(".kutlspace").exists() {
            return true;
        }
    }
    false
}

/// Report the result of an AGENTS.md managed-block write to the user via stderr.
///
/// Covers all [`agents_md::ApplyOutcome`] variants: silent on `AlreadyCurrent`,
/// informational on `Created` / `Appended` / `Replaced`, and a nudge on `Stale`
/// variants so users know to run `kutl space apply`.
fn report_agents_md_outcome(path: &std::path::Path, outcome: &agents_md::ApplyOutcome) {
    use agents_md::{ApplyOutcome, Staleness};
    match outcome {
        ApplyOutcome::Created => {
            eprintln!("wrote agent instructions to {}", path.display());
        }
        ApplyOutcome::Appended => {
            eprintln!(
                "appended kutl-managed section to existing {}",
                path.display()
            );
        }
        ApplyOutcome::Replaced => {
            eprintln!("refreshed kutl-managed section in {}", path.display());
        }
        ApplyOutcome::AlreadyCurrent => {
            // Quiet: nothing to say.
        }
        ApplyOutcome::Stale {
            sentinel,
            staleness,
        } => match staleness {
            Staleness::StaleCompatible => {
                eprintln!(
                    "note: kutl-managed section in {} was generated by v{}; current is v{}; run `kutl space apply` to refresh",
                    path.display(),
                    sentinel,
                    env!("CARGO_PKG_VERSION"),
                );
            }
            Staleness::StaleIncompatible => {
                eprintln!(
                    "warning: kutl-managed section in {} was generated by v{} which is incompatible with the running v{}; run `kutl space apply` to refresh",
                    path.display(),
                    sentinel,
                    env!("CARGO_PKG_VERSION"),
                );
            }
            Staleness::Current => {}
        },
    }
}

async fn cmd_init(
    relay_url: &str,
    name: Option<&str>,
    dir: Option<PathBuf>,
    subfolder: Option<&str>,
) -> Result<()> {
    // Reject a malformed relay URL up front, before we touch the filesystem or
    // the relay — otherwise a bad value (e.g. `not-a-url`, or an `http://`
    // scheme) would persist in `space.toml` and only fail opaquely at connect.
    kutl_client::validate_relay_url(relay_url)?;

    let initial_root = resolve_dir(dir.clone())?;

    // If we're inside a git repo, route the space into a subfolder so it
    // does not share files with the git working tree.
    let space_root = if let Some(repo_root) = kutl_client::find_git_repo_root(&initial_root) {
        let chosen = match subfolder {
            Some(s) => s.to_owned(),
            None => prompt_subfolder_name(&repo_root),
        };
        let candidate = repo_root.join(&chosen);
        if space::SpaceConfig::is_marked(&candidate) {
            anyhow::bail!(
                "space already exists at {} — pick a different --subfolder or remove it first",
                candidate.display()
            );
        }
        std::fs::create_dir_all(&candidate)
            .with_context(|| format!("failed to create {}", candidate.display()))?;
        candidate.canonicalize()?
    } else {
        initial_root.clone()
    };

    // Ensure identity exists.
    identity::load_or_generate()?;
    let id_path = identity::default_identity_path()?;
    println!("Identity: {}", id_path.display());

    // Check for existing space. Re-register it on this client (idempotent)
    // so an init in a tree kept by `kutl space leave` re-attaches instead of
    // silently leaving the registry stale.
    if space::SpaceConfig::is_joined(&space_root) {
        let existing = space::SpaceConfig::load(&space_root)?;
        register_space_root(&space_root)?;
        println!("\nSpace already initialized.");
        println!("  space_id:   {}", existing.space_id);
        println!("  relay:      {}", existing.relay_url);
        if let Some(ks) = kutl_client::KutlspaceConfig::load(&space_root)? {
            println!("  space_name: {}", ks.space_name);
        }
        return Ok(());
    }

    // Determine space name.
    let space_name = match name {
        Some(n) => n.to_owned(),
        None => space::generate_space_name(),
    };

    // Try to register with the relay.
    let (space_id, registered) = match register_with_relay(relay_url, &space_name).await {
        Ok(resp) => (resp.space_id, true),
        Err(RegisterError::NameConflict) => {
            anyhow::bail!(
                "space name '{space_name}' is already taken on the relay — try a different --name"
            );
        }
        Err(RegisterError::Unreachable(reason)) => {
            eprintln!("warning: relay unreachable ({reason}), initializing local-only space");
            (space::generate_space_id(), false)
        }
        Err(RegisterError::Other(msg)) => {
            eprintln!("warning: relay registration failed ({msg}), initializing local-only space");
            (space::generate_space_id(), false)
        }
    };

    // Fail loudly on case-variant duplicates before we write any config.
    if let Err(err) = kutl_daemon::case_collision::detect_case_collisions(&space_root) {
        eprintln!("{}", err.format_user_message());
        anyhow::bail!("space root contains case-variant duplicates");
    }

    save_space_config(&space_id, &space_name, relay_url, Some(space_root.clone()))?;

    println!("\nInitialized kutl space.");
    println!("  space_id:   {space_id}");
    println!("  space_name: {space_name}");
    println!("  relay:      {relay_url}");
    if registered {
        println!("  registered: yes");
    } else {
        println!("  registered: no (local-only, re-run init when relay is available)");
    }
    println!(
        "  config:     {}",
        space::SpaceConfig::path(&space_root).display()
    );

    println!("\nNext steps:");
    println!("  Start syncing:  kutl daemon start");
    println!("  Or sync once:   kutl sync --dir {}", space_root.display());
    println!("  Check status:   kutl status");

    // Write the kutl-managed section into the repo-root AGENTS.md when
    // inside a git repo, or at the chosen project root otherwise. Non-git
    // installs still get the contract written next to their .kutlspace.
    // Refresh of an already-written block belongs to `kutl space apply`, not
    // `kutl init`.
    let agents_anchor = agents_md::anchor_for(&initial_root);
    let agents_path = agents_anchor.join("AGENTS.md");
    let outcome = agents_md::apply_block(
        &agents_path,
        env!("CARGO_PKG_VERSION"),
        agents_md::ApplyMode::Default,
    )?;
    report_agents_md_outcome(&agents_path, &outcome);

    Ok(())
}

/// Implementation of `kutl space apply`: refresh the kutl-managed AGENTS.md
/// section against the running binary's instructions template.
///
/// Anchors at the local kutl space's project root (git repo root if
/// any, otherwise the space directory). Bails when no kutl space is
/// present at or under the anchor — `kutl space apply` is a refresh-only
/// path, never a create path.
fn cmd_space_apply(dir: Option<PathBuf>) -> Result<()> {
    let initial_root = resolve_dir(dir)?;
    let agents_anchor = agents_md::anchor_for(&initial_root);
    if !has_any_space_under(&agents_anchor) {
        anyhow::bail!(
            "no kutl space found at or under {} — run `kutl init` to create one or `kutl join` to join an existing one",
            agents_anchor.display(),
        );
    }
    let agents_path = agents_anchor.join("AGENTS.md");
    let outcome = agents_md::apply_block(
        &agents_path,
        env!("CARGO_PKG_VERSION"),
        agents_md::ApplyMode::ForceUpdate,
    )?;
    report_agents_md_outcome(&agents_path, &outcome);
    Ok(())
}

// ---------------------------------------------------------------------------
// Relay registration
// ---------------------------------------------------------------------------

/// Timeout for HTTP requests to the relay.
const RELAY_REQUEST_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(5));

/// Timeout for long-running HTTP requests (device flow polling).
const DEVICE_FLOW_REQUEST_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(30));

/// Successful registration response from the relay.
#[derive(serde::Deserialize)]
struct RegisterResponse {
    space_id: String,
}

/// Errors from relay registration.
enum RegisterError {
    /// The space name is already taken (HTTP 409).
    NameConflict,
    /// The relay is unreachable (network error or timeout).
    Unreachable(String),
    /// Any other relay error.
    Other(String),
}

/// Register a space with the relay via `POST /spaces/register`.
///
/// Converts the WebSocket URL to an HTTP URL and posts the registration
/// request. Returns the relay-assigned space ID on success.
async fn register_with_relay(
    relay_url: &str,
    name: &str,
) -> Result<RegisterResponse, RegisterError> {
    let base = kutl_client::ws_url_to_http(relay_url);
    let url = format!("{base}/spaces/register");

    let body = serde_json::json!({ "name": name });

    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&body)
        .timeout(RELAY_REQUEST_TIMEOUT)
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => {
            let data: RegisterResponse = r
                .json()
                .await
                .map_err(|e| RegisterError::Other(format!("failed to parse response: {e}")))?;
            Ok(data)
        }
        Ok(r) if r.status() == reqwest::StatusCode::CONFLICT => Err(RegisterError::NameConflict),
        Ok(r) => {
            let status = r.status();
            let text = r
                .text()
                .await
                .unwrap_or_else(|e| format!("(failed to read response: {e})"));
            Err(RegisterError::Other(register_error_message(
                relay_url, status, &text,
            )))
        }
        Err(e) => Err(RegisterError::Unreachable(e.to_string())),
    }
}

/// Build a human-readable registration-failure message from the relay URL, the
/// HTTP status, and the response body.
///
/// A relay that answers a non-conflict status with an empty body (e.g. a bare
/// 404 from a server that has no registration endpoint) would otherwise surface
/// as a blank error string. When the body is empty, substitute a message that
/// names the status and relay URL and hints at the likely cause.
fn register_error_message(relay_url: &str, status: reqwest::StatusCode, body: &str) -> String {
    if body.trim().is_empty() {
        format!(
            "relay at {relay_url} returned {status} with no body — check the relay URL is correct and running a registration endpoint"
        )
    } else {
        format!("relay at {relay_url} returned {status}: {}", body.trim())
    }
}

// ---------------------------------------------------------------------------
// DID authentication
// ---------------------------------------------------------------------------

/// Load the signing key from the default identity file.
///
/// Returns `(did, signing_key, display_name)`.
fn load_signing_key() -> Result<(String, ed25519_dalek::SigningKey, Option<String>)> {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    let path = identity::default_identity_path()?;
    let id = identity::Identity::load(&path).context(
        "no identity found — run `kutl init` to create a space or `kutl join` to join one",
    )?;

    let key_bytes = URL_SAFE_NO_PAD
        .decode(&id.private_key)
        .context("failed to decode private key")?;
    let key_array: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid private key length"))?;
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&key_array);

    Ok((id.did, signing_key, id.display_name))
}

// ---------------------------------------------------------------------------
// Device flow
// ---------------------------------------------------------------------------

/// Response from `POST /auth/device` — initiates the device authorization flow.
#[derive(serde::Deserialize)]
struct DeviceCodeResponse {
    /// Opaque code the CLI polls with.
    device_code: String,
    /// Short code the user enters in the browser.
    user_code: String,
    /// URL the user visits to enter the code.
    verification_url: String,
    /// Minimum polling interval in seconds.
    interval: u32,
}

/// Successful response from `POST /auth/device/token`.
#[derive(serde::Deserialize)]
struct DeviceTokenResponse {
    /// Bearer token for API access.
    token: String,
    /// Account ID on the relay.
    account_id: String,
    /// Human-readable display name.
    display_name: String,
    /// Relay URL the token is valid for.
    relay_url: String,
}

/// Build [`StoredCredentials`](kutl_client::StoredCredentials) from a device-flow
/// token response.
///
/// Normalizes the relay base URL to a ws(s) scheme via
/// [`kutl_client::http_url_to_ws`] so it matches the wss URL the daemon compares
/// against when reusing the stored token. The device-flow endpoint returns an
/// http(s) base; without this conversion the daemon's scheme-equality gate
/// silently rejects the otherwise-valid token.
fn device_credentials_from_token(
    token_resp: DeviceTokenResponse,
) -> kutl_client::StoredCredentials {
    kutl_client::StoredCredentials {
        token: token_resp.token,
        relay_url: kutl_client::http_url_to_ws(&token_resp.relay_url),
        account_id: token_resp.account_id,
        display_name: token_resp.display_name,
    }
}

/// HTTP 428 — authorization pending (user hasn't completed browser flow yet).
const HTTP_PRECONDITION_REQUIRED: u16 = 428;

/// HTTP 410 — device code expired.
const HTTP_GONE: u16 = 410;

// ---------------------------------------------------------------------------
// Join
// ---------------------------------------------------------------------------

/// Default OSS relay WebSocket URL (local development).
const DEFAULT_OSS_RELAY_URL: &str = space::DEFAULT_RELAY_URL;

/// How a `kutl join` target was interpreted.
enum JoinTarget {
    /// A full invite URL (`https://...`).
    InviteUrl(String),
    /// A `owner/slug` namespace identifier.
    OwnerSlug(String),
    /// A bare space name for OSS relay resolution.
    BareName(String),
}

/// Classify a raw join target string.
fn parse_join_target(target: &str) -> JoinTarget {
    if target.starts_with("http://") || target.starts_with("https://") {
        JoinTarget::InviteUrl(target.to_owned())
    } else if target.contains('/') {
        JoinTarget::OwnerSlug(target.to_owned())
    } else {
        JoinTarget::BareName(target.to_owned())
    }
}

/// Response from `GET /invites/{code}`.
#[derive(serde::Deserialize)]
struct InviteResponse {
    space_id: String,
    space_name: String,
}

/// Response from `GET /spaces/resolve`.
#[derive(serde::Deserialize)]
struct ResolveResponse {
    space_id: String,
    name: String,
}

/// Parse an invite URL into its `(code, http_origin)`. The code is the last
/// non-empty path segment (tolerating a trailing slash); the origin is the
/// scheme + host + optional port the relay is reached at. Shared by every
/// invite-redemption path so the parse rules cannot drift between them.
fn parse_invite_url(invite_url: &str) -> Result<(String, String)> {
    let parsed = reqwest::Url::parse(invite_url)
        .with_context(|| format!("invalid invite URL: {invite_url}"))?;
    let code = parsed
        .path_segments()
        .and_then(|seg| seg.rev().find(|s| !s.is_empty()))
        .map(str::to_owned)
        .with_context(|| format!("invite URL has no path segment: {invite_url}"))?;
    let origin = parsed.origin().ascii_serialization();
    Ok((code, origin))
}

/// Join a space via an invite URL.
///
/// Parses the relay host from the URL, fetches invite metadata via
/// `GET /invites/{code}`, and saves the space config.
async fn join_via_invite_url(invite_url: &str, dir: Option<PathBuf>) -> Result<()> {
    let (code, origin) = parse_invite_url(invite_url)?;

    // Hit the relay's invite resolution endpoint.
    let endpoint = format!("{origin}/invites/{code}");
    let client = reqwest::Client::new();
    let resp = client
        .get(&endpoint)
        .header(reqwest::header::ACCEPT, "application/json")
        .timeout(RELAY_REQUEST_TIMEOUT)
        .send()
        .await
        .context("failed to reach relay for invite resolution")?;

    match resp.status() {
        reqwest::StatusCode::OK => {}
        reqwest::StatusCode::NOT_FOUND => {
            anyhow::bail!("invite not found or expired: {code}");
        }
        status => {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("(failed to read response: {e})"));
            anyhow::bail!("relay returned {status}: {body}");
        }
    }

    let invite: InviteResponse = resp
        .json()
        .await
        .context("failed to parse invite response")?;

    // Convert the HTTP origin to a WebSocket relay URL.
    let relay_url = kutl_client::http_url_to_ws(&origin);

    let space_root = save_space_config(&invite.space_id, &invite.space_name, &relay_url, dir)?;

    print_join_success(
        &format!("Joined space {} via invite.", invite.space_name),
        &invite.space_id,
        &invite.space_name,
        &relay_url,
        &space_root,
    );

    Ok(())
}

/// Redeem an invite over the authenticated WS `JoinSpace` op (the
/// `MEMBERSHIP_GRANT` model): the relay creates an account-scoped membership for
/// the authenticated caller. Used when the relay advertises `MEMBERSHIP_GRANT`;
/// `CAPABILITY_URL` relays use the anonymous [`join_via_invite_url`].
async fn join_via_authenticated_invite(
    invite_url: &str,
    relay_url: &str,
    dir: Option<PathBuf>,
) -> Result<()> {
    // Only the code comes from the invite URL; the relay endpoint comes from the
    // discovered policy, so a front-door invite URL on the bare origin
    // still connects to the right relay.
    let (code, _origin) = parse_invite_url(invite_url)?;

    // MEMBERSHIP_GRANT redemption is authenticated: resolve an account session.
    let token = kutl_client::resolve_or_authenticate(relay_url).await?;

    // Redeem over the authenticated WS JoinSpace op; the server-side
    // accept_invitation creates the membership.
    let mut conn = AsyncRelayConn::connect(relay_url, &token).await?;
    let req = kutl_proto::protocol::join_space_envelope(&code);
    let resp = conn.request(&req).await?;
    let result = match resp.payload {
        Some(kutl_proto::sync::sync_envelope::Payload::JoinSpaceResult(r)) => {
            if let Some(e) = r.error {
                anyhow::bail!("invite redemption failed: {}", e.message);
            }
            r
        }
        other => anyhow::bail!("unexpected response from relay: {other:?}"),
    };
    conn.close().await?;

    let space_root = save_space_config(&result.space_id, &result.space_name, relay_url, dir)?;
    print_join_success(
        &format!("Joined space {} via invite.", result.space_name),
        &result.space_id,
        &result.space_name,
        relay_url,
        &space_root,
    );

    Ok(())
}

/// Number of parts expected when splitting an `owner/slug` space identifier.
const SPACE_IDENTIFIER_PARTS: usize = 2;

/// Join a hosted space by `owner/slug` via WebSocket `ResolveSpace` RPC.
async fn join_via_owner_slug(space_arg: &str, relay_url: &str, dir: Option<PathBuf>) -> Result<()> {
    let parts: Vec<&str> = space_arg.splitn(SPACE_IDENTIFIER_PARTS, '/').collect();
    if parts.len() != SPACE_IDENTIFIER_PARTS || parts[0].is_empty() || parts[1].is_empty() {
        anyhow::bail!(
            "invalid space identifier '{space_arg}' — expected format: owner/slug (e.g. alice/my-project)"
        );
    }
    let owner = parts[0];
    let slug = parts[1];

    // Resolve auth token.
    let token = kutl_client::resolve_or_authenticate(relay_url).await?;

    // Connect to relay via async WebSocket.
    let mut conn = AsyncRelayConn::connect(relay_url, &token).await?;

    // Send ResolveSpace request.
    let req = kutl_proto::protocol::resolve_space_envelope(owner, slug);
    let resp = conn.request(&req).await?;

    // Extract ResolveSpaceResult.
    let result = match resp.payload {
        Some(kutl_proto::sync::sync_envelope::Payload::ResolveSpaceResult(r)) => {
            if let Some(e) = r.error {
                // Map the relay's ErrorCode: an auth failure is explicit;
                // not-found and not-authorized collapse into one non-leaking
                // message — rather than blanket-labeling all errors "space
                // not found".
                let msg = join_auth::map_join_error(e.code, space_arg, relay_url);
                anyhow::bail!("{msg}");
            }
            r
        }
        other => {
            anyhow::bail!("unexpected response from relay: {other:?}");
        }
    };

    conn.close().await?;

    // Determine effective relay URL: use the one from the result if non-empty,
    // otherwise fall back to the relay we connected to.
    let effective_relay = if result.relay_url.is_empty() {
        relay_url.to_owned()
    } else {
        result.relay_url
    };

    let space_root =
        save_space_config(&result.space_id, &result.space_name, &effective_relay, dir)?;

    print_join_success(
        &format!("Joined space {owner}/{slug}."),
        &result.space_id,
        &result.space_name,
        &effective_relay,
        &space_root,
    );

    Ok(())
}

/// Join a space on an OSS relay by bare name via `GET /spaces/resolve?name=...`.
async fn join_via_bare_name(name: &str, relay_url: &str, dir: Option<PathBuf>) -> Result<()> {
    let base = kutl_client::ws_url_to_http(relay_url);
    let url = format!("{base}/spaces/resolve?name={}", urlencoding::encode(name));

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .timeout(RELAY_REQUEST_TIMEOUT)
        .send()
        .await
        .context("failed to reach relay — is it running?")?;

    match resp.status() {
        reqwest::StatusCode::OK => {}
        reqwest::StatusCode::NOT_FOUND => {
            anyhow::bail!("space not found: {name}");
        }
        status => {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("(failed to read response: {e})"));
            anyhow::bail!("relay returned {status}: {body}");
        }
    }

    let body: ResolveResponse = resp
        .json()
        .await
        .context("failed to parse resolve response")?;

    let space_root = save_space_config(&body.space_id, &body.name, relay_url, dir)?;

    print_join_success(
        &format!("Joined space {name}."),
        &body.space_id,
        &body.name,
        relay_url,
        &space_root,
    );

    Ok(())
}

/// The default WebSocket relay URL for an `owner/slug` or bare-name join target,
/// honoring an explicit `--relay` override. Single source of truth so the
/// pre-connect policy fetch and the join itself always target the same relay.
/// (Invite URLs carry their own origin and never consult this.)
fn default_relay_for_target<'a>(target: &JoinTarget, relay_override: Option<&'a str>) -> &'a str {
    match target {
        JoinTarget::OwnerSlug(_) => {
            relay_override.unwrap_or(kutl_client::DEFAULT_KUTLHUB_RELAY_URL)
        }
        JoinTarget::BareName(_) | JoinTarget::InviteUrl(_) => {
            relay_override.unwrap_or(DEFAULT_OSS_RELAY_URL)
        }
    }
}

/// HTTP base URL to fetch the relay policy from, for a given join target.
/// `owner/slug` and bare-name resolve to the per-form default relay
/// (or the override); an invite URL carries its own origin.
fn relay_http_base_for_target(target: &JoinTarget, relay_override: Option<&str>) -> Result<String> {
    let base = match target {
        JoinTarget::InviteUrl(url) => {
            let (_code, origin) = parse_invite_url(url)?;
            origin
        }
        JoinTarget::OwnerSlug(_) | JoinTarget::BareName(_) => {
            kutl_client::ws_url_to_http(default_relay_for_target(target, relay_override))
        }
    };
    Ok(base)
}

/// Fetch the relay's advertised onboarding policy over HTTP,
/// pre-connect. Returns `None` if the relay does not serve `/relay-policy`
/// (an older relay) or is unreachable, so the caller falls back to legacy
/// behavior rather than blocking the join.
async fn fetch_relay_policy(http_base: &str) -> Option<kutl_proto::sync::RelayPolicy> {
    let url = format!("{}/relay-policy", http_base.trim_end_matches('/'));
    let resp = reqwest::Client::new()
        .get(&url)
        .timeout(RELAY_REQUEST_TIMEOUT)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<kutl_proto::sync::RelayPolicy>().await.ok()
}

/// Apply the lazy, policy-gated identity check before any DID is
/// minted: given the relay's advertised policy and the local auth state, either
/// bail with an actionable error, run the inline device flow, or proceed. A
/// no-op when no policy was served (older/unreachable relay). The caller
/// applies it only to a join whose own calls carry a bearer (an
/// `owner/slug` resolve over the authenticated socket, a `MEMBERSHIP_GRANT`
/// invite); a capability-URL or bare-name join resolves anonymously, and
/// gating it would demand a token nothing validates.
async fn apply_join_policy_gate(
    relay_policy: Option<&(String, kutl_proto::sync::RelayPolicy)>,
) -> Result<()> {
    let Some((relay_base, policy)) = relay_policy else {
        return Ok(());
    };
    let has_token = kutl_client::credentials::default_credentials_path()
        .ok()
        .and_then(|p| kutl_client::credentials::resolve_token(Some(&p)))
        .is_some();
    let is_interactive = {
        use std::io::IsTerminal as _;
        std::io::stdin().is_terminal()
    };
    // Where to authenticate: the policy's advertised auth_url (the relay HTTP
    // base behind a proxy front door) when set, else the discovery base. This
    // matters for a bare-origin invite URL, whose discovery base is the front
    // door root — the device flow must post under /relay, not collide with the
    // front door's own /auth/* routes. Anti-redirect: only honor auth_url when
    // it stays on the origin the user already targeted.
    let auth_base = if policy.auth_url.is_empty() {
        relay_base.as_str()
    } else {
        if !join_auth::same_origin(&policy.auth_url, relay_base) {
            anyhow::bail!(
                "relay advertised a cross-origin auth_url {} for {relay_base} — refusing to authenticate",
                policy.auth_url
            );
        }
        policy.auth_url.as_str()
    };
    match join_auth::decide_join_auth(policy.auth_model(), has_token, is_interactive) {
        // Account-required, no credentials, non-interactive (CI/agents):
        // stop with an actionable error before minting an unusable DID.
        join_auth::JoinAuthAction::ErrorNotAuthenticated => {
            anyhow::bail!(
                "not authenticated to {auth_base} — run `kutl auth login --relay {auth_base}` first"
            );
        }
        // Account-required, no credentials, interactive: offer to sign in,
        // then run the device flow inline (opens the browser) so the join is
        // a single command — the policy's next=AUTHENTICATE directive.
        join_auth::JoinAuthAction::PromptThenDeviceFlow => {
            use std::io::Write as _;
            eprint!("Sign in to {auth_base} to join? [Y/n] ");
            std::io::stderr().flush().ok();
            let mut answer = String::new();
            std::io::stdin().read_line(&mut answer)?;
            if join_auth::affirmative_default_yes(&answer) {
                cmd_auth_login_device_flow(auth_base).await?;
            } else {
                anyhow::bail!(
                    "sign-in declined; run `kutl auth login --relay {auth_base}` to authenticate"
                );
            }
        }
        join_auth::JoinAuthAction::UseStoredToken | join_auth::JoinAuthAction::Proceed => {}
    }
    Ok(())
}

/// Dispatch `kutl join` to the appropriate handler based on target form.
async fn cmd_join(
    target: Option<&str>,
    relay: Option<&str>,
    dir: Option<PathBuf>,
    subfolder: Option<&str>,
) -> Result<()> {
    // Resolve the target. If missing, look for .kutlspace in the current
    // (or --dir) directory and use space_name from it.
    let resolved_target: String = if let Some(t) = target {
        t.to_owned()
    } else {
        let here = resolve_dir(dir.clone())?;
        let ks = kutl_client::KutlspaceConfig::load(&here)?.ok_or_else(|| {
            anyhow::anyhow!(
                "no target specified and no .kutlspace found in {}",
                here.display()
            )
        })?;
        ks.space_name
    };

    // Resolve subfolder if we're inside a git repo with no existing subfolder.
    let effective_dir = resolve_join_directory(dir, subfolder)?;

    // Fail loudly on case-variant duplicates before any handler writes config.
    // `effective_dir` is the directory the handlers will write `.kutlspace` into;
    // empty subfolders pass the check trivially.
    let check_target: PathBuf = match effective_dir.as_ref() {
        Some(t) => t.clone(),
        None => std::env::current_dir().context("failed to determine current directory")?,
    };
    // Canonical, so the git-root anchor below (itself canonical) compares
    // equal to a relative or symlinked `--dir` naming the same directory.
    let check_target = check_target.canonicalize().unwrap_or(check_target);
    if let Err(err) = kutl_daemon::case_collision::detect_case_collisions(&check_target) {
        eprintln!("{}", err.format_user_message());
        anyhow::bail!("join target contains case-variant duplicates");
    }

    // Lazy, policy-gated identity: learn the relay's advertised auth
    // model before minting a DID, so an account-required relay fails clearly
    // instead of silently creating an unusable identity and then reporting a
    // misleading "space not found". Best-effort: if the relay serves no policy
    // (older relay) or is unreachable, fall through to the legacy flow.
    let join_target = parse_join_target(&resolved_target);
    let relay_policy = match relay_http_base_for_target(&join_target, relay) {
        Ok(http_base) => fetch_relay_policy(&http_base).await.map(|p| (http_base, p)),
        Err(_) => None,
    };
    // MEMBERSHIP_GRANT relays redeem invites over the authenticated WS
    // JoinSpace op (creates an account-scoped membership); CAPABILITY_URL and
    // older relays use the anonymous HTTP path. Driven by the advertised
    // InviteModel, never the host.
    let membership_grant = relay_policy
        .as_ref()
        .is_some_and(|(_, p)| p.invite_model() == kutl_proto::sync::InviteModel::MembershipGrant);
    // The gate guards a join whose own calls carry a bearer; an anonymous
    // resolve (capability URL, bare name) proceeds and authenticates at sync
    // time like any did:key client.
    let join_needs_bearer = match &join_target {
        JoinTarget::InviteUrl(_) => membership_grant,
        JoinTarget::OwnerSlug(_) => true,
        JoinTarget::BareName(_) => false,
    };
    if join_needs_bearer {
        apply_join_policy_gate(relay_policy.as_ref()).await?;
    }

    // Ensure the joiner has a DID identity. Symmetric with `cmd_init`:
    // a fully-provisioned join writes identity at `$KUTL_HOME/identity.toml`
    // so the next sync / daemon / DID-auth call doesn't bail with
    // "no identity found — run `kutl init` first". Without this call,
    // joiners would hit that error on their first `kutl sync` or `kutl daemon
    // start` — told to run init, which is the wrong command for
    // someone joining an existing space.
    //
    // Provisioning happens before the network round-trip so a join
    // against an unreachable relay still leaves the user with a usable
    // identity.
    identity::load_or_generate()?;
    let id_path = identity::default_identity_path()?;
    println!("Identity: {}", id_path.display());

    // The kutl-managed AGENTS.md section, at the git repo root when the
    // space is a subfolder of a repo: that file is outside the space, so
    // nothing else writes it. When the anchor IS the space root the file is
    // one the space syncs, and the owner's copy (written by their init)
    // arrives with the first sync; a copy written here first would meet it
    // as an offline-created collision and land as a conflict copy. Default
    // mode only — `kutl join` has no `--update` flag.
    let agents_anchor = agents_md::anchor_for(&check_target);
    if agents_anchor == check_target {
        eprintln!(
            "AGENTS.md arrives with the space's first sync; run `kutl space apply` if it is missing"
        );
    } else {
        let agents_path = agents_anchor.join("AGENTS.md");
        let outcome = agents_md::apply_block(
            &agents_path,
            env!("CARGO_PKG_VERSION"),
            agents_md::ApplyMode::Default,
        )?;
        report_agents_md_outcome(&agents_path, &outcome);
    }

    // Resolved once so the pre-connect policy fetch and the join target the same
    // relay (unused for invite URLs, which carry their own origin).
    let target_relay = default_relay_for_target(&join_target, relay);
    // The ws endpoint to actually connect to: follow the policy's advertised
    // relay_endpoint (a proxy front door) when set, else the http base the policy
    // was fetched from, else — if no policy was served — the per-form default.
    let effective_ws = match &relay_policy {
        Some((http_base, policy)) => join_auth::effective_relay_endpoint(Some(policy), http_base)?,
        None => target_relay.to_owned(),
    };
    match join_target {
        JoinTarget::InviteUrl(url) => {
            if relay.is_some() {
                eprintln!("warning: --relay is ignored when joining via an invite URL");
            }
            if membership_grant {
                join_via_authenticated_invite(&url, &effective_ws, effective_dir).await
            } else {
                join_via_invite_url(&url, effective_dir).await
            }
        }
        JoinTarget::OwnerSlug(spec) => {
            join_via_owner_slug(&spec, &effective_ws, effective_dir).await
        }
        JoinTarget::BareName(name) => join_via_bare_name(&name, &effective_ws, effective_dir).await,
    }
}

/// Resolve the directory `kutl join` will use, applying the git subfolder rule.
///
/// - If the user passed `--dir`, that wins as-is.
/// - Otherwise, if the current directory contains an existing `.kutlspace`,
///   use it directly (the user is rejoining a kutl-marked folder).
/// - Otherwise, if the current directory is inside a git repo and has no
///   existing `.kutlspace`, create a subfolder (default = the resolved
///   `target` name) and return its path.
/// - Otherwise, return `None` so the join handlers fall back to cwd.
fn resolve_join_directory(
    dir: Option<PathBuf>,
    subfolder: Option<&str>,
) -> Result<Option<PathBuf>> {
    if dir.is_some() {
        return Ok(dir);
    }
    let here = std::env::current_dir().context("failed to determine current directory")?;
    if space::SpaceConfig::is_marked(&here) {
        // Already inside a kutl-marked folder; join in place.
        return Ok(Some(here));
    }
    let Some(repo_root) = kutl_client::find_git_repo_root(&here) else {
        return Ok(None);
    };
    // No --subfolder given inside a git repo: prompt with the default name,
    // exactly like `kutl init`. Never derive the folder from the raw join
    // target — `kutl join owner/slug` would silently create a nested
    // `owner/slug/` folder with no prompt.
    let chosen = match subfolder {
        Some(s) => s.to_owned(),
        None => prompt_subfolder_name(&repo_root),
    };
    let candidate = repo_root.join(&chosen);
    if space::SpaceConfig::is_marked(&candidate) {
        anyhow::bail!(
            "space already exists at {} — pick a different --subfolder or `cd` into it first",
            candidate.display()
        );
    }
    std::fs::create_dir_all(&candidate)
        .with_context(|| format!("failed to create {}", candidate.display()))?;
    Ok(Some(candidate.canonicalize()?))
}

/// Implementation of `kutl surface`.
///
/// Copies the space's document content into the surface target, lifting it
/// out of the kutl folder into the git working tree.
fn cmd_surface(dir: Option<PathBuf>) -> Result<()> {
    let space_root = resolve_space_root(dir)?;

    let kutlspace = kutl_client::KutlspaceConfig::load(&space_root)?.ok_or_else(|| {
        anyhow::anyhow!(
            "no .kutlspace found at {} — surface requires a kutl-marked folder",
            space_root.display()
        )
    })?;

    let surface_cfg = kutlspace.surface.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "no [surface] target configured in .kutlspace — add a `[surface]` section with `target = \"../\"` to enable surfacing"
        )
    })?;

    let target = kutl_client::resolve_surface_target(&space_root, &surface_cfg.target)?;
    let files = kutl_client::enumerate_surface_files(&space_root)?;

    if files.is_empty() {
        println!("No documents to surface.");
        return Ok(());
    }

    let plural = if files.len() == 1 { "" } else { "s" };
    println!(
        "Surfacing {} file{plural} from {} to {}:",
        files.len(),
        space_root.display(),
        target.display()
    );
    for file in &files {
        println!("  {}", file.rel_path.display());
    }

    let copied = kutl_client::copy_surface_files(&space_root, &target, &files)?;
    let copied_plural = if copied == 1 { "" } else { "s" };
    println!("Surfaced {copied} file{copied_plural}.");
    Ok(())
}

/// Print client diagnostic status (daemon liveness, registered spaces, relay
/// reachability, identity).
///
/// Works whether the daemon is running or not — all data is from disk
/// (`$KUTL_HOME`) and direct relay reachability probes; no daemon IPC.
async fn cmd_status(format: OutputFormat) -> Result<()> {
    let kutl_home = kutl_client::kutl_home()?;
    let mut snapshot = status::collect_static(&kutl_home)?;
    status::probe_relays(&mut snapshot.relays).await;
    status::reconcile_space_health(&mut snapshot);

    match format {
        OutputFormat::Json => {
            let out = serde_json::to_string_pretty(&snapshot)?;
            println!("{out}");
        }
        OutputFormat::Human => {
            print!("{}", status::render_human(&snapshot));
        }
    }
    Ok(())
}

/// List the spaces registered on this client.
///
/// Reads the static status snapshot (no daemon IPC, no relay probe) and
/// renders just the registered-spaces slice. `--format json` emits the
/// stable `spaces` array from the status schema.
fn cmd_space_list(format: OutputFormat) -> Result<()> {
    let kutl_home = kutl_client::kutl_home()?;
    let snapshot = status::collect_static(&kutl_home)?;

    match format {
        OutputFormat::Json => {
            let out = serde_json::to_string_pretty(&snapshot.spaces)?;
            println!("{out}");
        }
        OutputFormat::Human => {
            print!("{}", status::render_space_list(&snapshot));
        }
    }
    Ok(())
}

/// Handle `kutl space leave`: forget the space enclosing the current
/// directory on this machine — remove it from the client registry.
///
/// Deliberately minimal and reversible: the working tree (including its
/// `.kutl/` state) stays on disk, and relay-side membership is untouched
/// (removing it is a relay operation) — `kutl join` in the folder
/// re-attaches everything. Refuses while the daemon runs: it would keep
/// live-syncing the space it no longer should know about.
fn cmd_space_leave() -> Result<()> {
    let root = require_cwd_space()?;
    if let Some(pid) = daemon_mgmt::stale_pid_check()? {
        anyhow::bail!(
            "the daemon (pid {pid}) is running and live-syncing this space — run `kutl daemon stop`, then `kutl space leave` again"
        );
    }

    let mut removed = false;
    space::SpaceRegistry::update(|registry| {
        let before = registry.spaces.len();
        registry
            .spaces
            .retain(|p| std::path::Path::new(p) != root.as_path());
        removed = registry.spaces.len() != before;
    })?;
    if !removed {
        println!(
            "the space at {} is not registered on this client — nothing to forget",
            root.display()
        );
        return Ok(());
    }

    println!(
        "left the space at {} (forgotten on this client)",
        root.display()
    );
    println!("  kept: the working tree and its files, including .kutl/ state");
    println!("  kept: your membership on the relay (leaving server-side is a relay operation)");
    println!("re-attach any time by running `kutl init` in that folder.");
    Ok(())
}

/// Show focused status for the space enclosing the current directory.
///
/// Collects the static snapshot, scopes it to the cwd space and the relay it
/// references, probes reachability, and renders that slice — omitting the
/// daemon and identity sections the aggregate `kutl status` carries. The
/// multi-space roll-up lives in bare `kutl status`. `--format json` emits
/// `{ spaces, relays }` (the arrays hold the one scoped space and its relay).
async fn cmd_space_status(format: OutputFormat) -> Result<()> {
    let root = require_cwd_space()
        .context("`kutl space status` reports the space you are standing in (use `kutl status` for the all-spaces view)")?;
    let kutl_home = kutl_client::kutl_home()?;
    let mut snapshot = status::collect_static(&kutl_home)?;
    snapshot
        .spaces
        .retain(|s| std::path::Path::new(&s.path) == root);
    if snapshot.spaces.is_empty() {
        anyhow::bail!(
            "the space at {} is not in this client's registry — run `kutl join` there to register it",
            root.display()
        );
    }
    let relay_urls: std::collections::HashSet<&str> = snapshot
        .spaces
        .iter()
        .map(|s| s.relay_url.as_str())
        .collect();
    snapshot
        .relays
        .retain(|r| relay_urls.contains(r.url.as_str()));
    status::probe_relays(&mut snapshot.relays).await;
    status::reconcile_space_health(&mut snapshot);

    match format {
        OutputFormat::Json => {
            let out = serde_json::to_string_pretty(&serde_json::json!({
                "spaces": snapshot.spaces,
                "relays": snapshot.relays,
            }))?;
            println!("{out}");
        }
        OutputFormat::Human => {
            print!("{}", status::render_space_status(&snapshot));
        }
    }
    Ok(())
}

/// One entry of the relay's `list_participants` answer.
///
/// Field names match the wire response verbatim (`did`, `name`,
/// `connection_type`) so `--format json` can re-serialize this struct
/// unchanged instead of re-deriving the shape.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct Participant {
    /// DID of the participant.
    did: String,
    /// What people call this DID, when the relay knows a name. `None` is
    /// legal — an unnamed entry is still an actor, reachable by DID.
    name: Option<String>,
    /// `"websocket"`, `"mcp"`, or `"offline"` — see [`render_participants_human`]
    /// for how this collapses for a human reader.
    connection_type: String,
}

/// Render the participant roster the relay's response deserialized to, for a
/// machine reader: the raw list, DIDs and all — `--format json` is where a
/// caller that needs to address someone by DID looks.
fn render_participants_json(participants: &[Participant]) -> Result<String> {
    serde_json::to_string_pretty(participants).context("rendering the participant list as json")
}

/// Render the participant roster for a human reader: one line per
/// participant, name first, presence second.
///
/// `connection_type` collapses to `online` for `"websocket"` or `"mcp"` and
/// `offline` for anything else — a human addressing someone does not care
/// which door they came in, only whether they're reachable right now. Rows
/// sort online-first, then by name, so who can answer immediately is at the
/// top. A participant the relay has no name for renders as `(unnamed)`; no
/// DID is shown here — that's what `--format json` is for. An empty roster
/// renders as `no participants`.
fn render_participants_human(participants: &[Participant]) -> String {
    let mut out = String::new();
    if participants.is_empty() {
        let _ = writeln!(out, "no participants");
        return out;
    }

    let mut rows: Vec<(&str, bool)> = participants
        .iter()
        .map(|p| {
            let name = p.name.as_deref().unwrap_or("(unnamed)");
            let online = matches!(p.connection_type.as_str(), "websocket" | "mcp");
            (name, online)
        })
        .collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

    for (name, online) in rows {
        let presence = if online { "online" } else { "offline" };
        let _ = writeln!(out, "{name}  {presence}");
    }
    out
}

/// Handle `kutl space participants`.
///
/// Asks the relay's `list_participants` MCP tool for the authorized actor
/// set of the space you are standing in and renders it. Answers who may act
/// here, whether or not they are connected right now — so you know who to
/// address versus who to leave a signal for.
///
/// # Errors
///
/// Returns an error if the current directory is not inside a space, the
/// space config cannot be loaded, the relay is unreachable or rejects
/// authentication, or the relay's response cannot be parsed.
async fn cmd_space_participants(format: OutputFormat) -> Result<()> {
    let root = require_cwd_space()
        .context("`kutl space participants` reports the roster of the space you are standing in")?;
    let config = kutl_client::SpaceConfig::load(&root)
        .with_context(|| format!("loading the space config at {}", root.display()))?;

    let token = kutl_client::resolve_or_authenticate(&config.relay_url)
        .await
        .context("authenticating to the relay for the participants list")?;

    let (proxy, _tools) = crate::watch_tools::RelayProxy::connect(&config.relay_url, &token, "")
        .await
        .context("asking the relay who is in this space")?;
    let participants: Vec<Participant> = proxy
        .call_tool_json(
            "list_participants",
            &serde_json::json!({ "space_id": config.space_id }),
            "the relay could not list participants",
            "reading the relay's participant list",
        )
        .await?;

    match format {
        OutputFormat::Json => println!("{}", render_participants_json(&participants)?),
        OutputFormat::Human => print!("{}", render_participants_human(&participants)),
    }
    Ok(())
}

/// Show focused daemon status: liveness and PID, plus the desktop tray when
/// running.
///
/// No relay probe (daemon liveness is a pure local PID-file read). `--format
/// json` emits both the `daemon` and `desktop` sub-structs from the status
/// schema, matching what the human render shows.
fn cmd_daemon_status(format: OutputFormat) -> Result<()> {
    let kutl_home = kutl_client::kutl_home()?;
    let snapshot = status::collect_static(&kutl_home)?;

    match format {
        OutputFormat::Json => {
            let out = serde_json::to_string_pretty(&serde_json::json!({
                "daemon": snapshot.daemon,
                "desktop": snapshot.desktop,
            }))?;
            println!("{out}");
        }
        OutputFormat::Human => {
            print!("{}", status::render_daemon_status(&snapshot));
        }
    }
    Ok(())
}

/// Show focused authentication status: the DID identity and any relay-bound
/// credentials.
///
/// No relay probe. `--format json` emits the `identity` sub-struct from the
/// status schema (`null` when no identity is provisioned).
fn cmd_auth_status(format: OutputFormat) -> Result<()> {
    let kutl_home = kutl_client::kutl_home()?;
    let snapshot = status::collect_static(&kutl_home)?;

    match format {
        OutputFormat::Json => {
            let out = serde_json::to_string_pretty(&snapshot.identity)?;
            println!("{out}");
        }
        OutputFormat::Human => {
            print!("{}", status::render_auth_status(&snapshot));
        }
    }
    Ok(())
}

async fn cmd_sync(dir: Option<PathBuf>) -> Result<()> {
    // Interactive one-shot: keep stdout clean for the progress lines below and
    // send diagnostics to stderr, quiet by default (opt in with KUTL_LOG=info).
    kutl_relay::telemetry::init_cli_tracing();

    let space_root = resolve_space_root(dir)?;
    // Two sync actors on one tree re-mint each other's materializations
    // (see daemon_mgmt::refuse_if_daemon_watches) — when the daemon
    // already watches this space, the one-shot must not run beside it.
    daemon_mgmt::refuse_if_daemon_watches(&space_root)?;
    let daemon_config = build_daemon_config(&space_root, true)?;
    let cancel = daemon_config.cancel.clone();

    // Install signal handlers that cancel the worker.
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
        cancel.cancel();
    });

    println!("Syncing space {} ...", daemon_config.space_id);

    kutl_daemon::run(daemon_config).await?;

    println!("Sync complete.");
    Ok(())
}

/// Locate the innermost kutl space enclosing `start`, if any.
///
/// `Ok(Some(root))` — a joined space; `Ok(None)` — not inside any
/// kutl-marked tree; `Err` — the innermost marker is a `.kutlspace`
/// folder not joined on this machine. Erring there (instead of walking
/// past it to an enclosing joined space) keeps a verb from silently
/// acting on a DIFFERENT space than the one the user is standing in.
fn enclosing_space(start: &std::path::Path) -> Result<Option<PathBuf>> {
    require_joined(kutl_client::find_space_root_upward(start))
}

/// [`enclosing_space`] for an EXPLICITLY-NAMED directory (a `--dir`
/// argument): the `$KUTL_HOME` workspace ceiling does not apply. The
/// ceiling exists to stop AMBIENT cwd resolution from binding a process
/// to whatever space its cwd happens to sit inside; a directory the
/// caller named outright is a declaration, not an accident of cwd.
fn enclosing_space_explicit(start: &std::path::Path) -> Result<Option<PathBuf>> {
    require_joined(kutl_client::find_space_root_upward_bounded(start, None))
}

/// Shared joined-check for the two resolvers above: a found root must be a
/// joined space, and an unjoined `.kutlspace` marker errs rather than
/// being walked past (acting on an enclosing DIFFERENT space instead of
/// the one the user named or stands in).
fn require_joined(found: Option<PathBuf>) -> Result<Option<PathBuf>> {
    match found {
        None => Ok(None),
        Some(root) => {
            if space::SpaceConfig::is_joined(&root) {
                Ok(Some(root))
            } else {
                anyhow::bail!(
                    "{} is a kutl space folder that is not joined on this machine — run `kutl join` there first",
                    root.display()
                )
            }
        }
    }
}

/// The space enclosing the current directory, or `Ok(None)` outside one.
///
/// The cwd-first anchor shared by the signal verbs, the feed, and the
/// focused space status: commands operate on the space you are standing
/// in, from any depth inside it.
pub(crate) fn cwd_enclosing_space() -> Result<Option<PathBuf>> {
    let cwd = std::env::current_dir().context("failed to determine current directory")?;
    enclosing_space(&cwd)
}

/// The space enclosing the current directory, required — for verbs that
/// only make sense inside one (authoring, transitions, the feed).
pub(crate) fn require_cwd_space() -> Result<PathBuf> {
    cwd_enclosing_space()?.ok_or_else(|| {
        anyhow::anyhow!(
            "not inside a kutl space — `cd` into one, or run `kutl init`/`kutl join` to create one here"
        )
    })
}

/// Resolve a space root from an optional starting directory (defaults to
/// cwd), walking up to the innermost enclosing space so any subdirectory
/// of a space works — the same cwd-first rule as git.
fn resolve_space_root(dir: Option<PathBuf>) -> Result<PathBuf> {
    let (start, explicit) = match dir {
        Some(p) => (
            p.canonicalize()
                .with_context(|| format!("invalid space path: {}", p.display()))?,
            true,
        ),
        None => (
            std::env::current_dir().context("failed to determine current directory")?,
            false,
        ),
    };
    let found = if explicit {
        enclosing_space_explicit(&start)?
    } else {
        enclosing_space(&start)?
    };
    let root = found.ok_or_else(|| {
        anyhow::anyhow!(
            "no kutl space at or above {} — run `kutl init` to create one or `kutl join` to join an existing one",
            start.display()
        )
    })?;
    // Verify the config is readable here, where the failure is attributable.
    space::SpaceConfig::load(&root)?;
    Ok(root)
}

/// Resolve a target directory from an optional `--dir` argument.
///
/// Creates the directory if it doesn't exist. Falls back to cwd if `None`.
fn resolve_dir(dir: Option<PathBuf>) -> Result<PathBuf> {
    match dir {
        Some(p) => {
            if !p.exists() {
                std::fs::create_dir_all(&p)
                    .with_context(|| format!("failed to create directory: {}", p.display()))?;
            }
            p.canonicalize()
                .with_context(|| format!("invalid directory: {}", p.display()))
        }
        None => std::env::current_dir().context("failed to determine current directory"),
    }
}

/// Build a `SpaceWorkerConfig` from a resolved space root.
fn build_daemon_config(
    space_root: &Path,
    one_shot: bool,
) -> Result<kutl_daemon::SpaceWorkerConfig> {
    let (did, signing_key, display_name) = load_signing_key()?;
    kutl_daemon::SpaceWorkerConfig::for_space(
        space_root,
        &did,
        Some(&signing_key),
        &display_name.unwrap_or_default(),
        one_shot,
    )
}

/// Run the global daemon supervisor in the foreground.
async fn cmd_daemon_run() -> Result<()> {
    kutl_relay::telemetry::init_tracing("cli");
    // Foreground daemons hold the same one-daemon-per-home claim the
    // `daemon start` flow enforces. Unclaimed, this process is invisible
    // to `daemon start`/`stop`/`status`, and the second daemon a later
    // `daemon start` then spawns turns every materialization into a
    // re-minted edit (see daemon_mgmt::claim_foreground).
    daemon_mgmt::claim_foreground()?;
    let (did, signing_key, display_name) = load_signing_key()?;

    // Install Prometheus recorder + start /metrics server before any
    // metric facade calls run.
    kutl_daemon::install_metrics_and_serve().await?;

    supervisor::run(did, Some(signing_key), display_name.unwrap_or_default()).await
}

async fn cmd_daemon(action: DaemonAction) -> Result<()> {
    match action {
        DaemonAction::Run => cmd_daemon_run().await,
        DaemonAction::Start => {
            if let Some(pid) = daemon_mgmt::stale_pid_check()? {
                println!("Daemon is already running (PID {pid})");
                return Ok(());
            }

            let pid = daemon_mgmt::spawn_daemon()?;
            daemon_mgmt::write_pid(pid)?;

            println!("Daemon started (PID {pid})");
            println!("  log: {}", daemon_mgmt::log_path()?.display());

            Ok(())
        }
        DaemonAction::Stop => {
            let msg = daemon_mgmt::stop_daemon()?;
            println!("{msg}");
            Ok(())
        }
        DaemonAction::Status(args) => cmd_daemon_status(args.format),
    }
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Dispatch `kutl auth` to the appropriate subcommand handler.
async fn cmd_auth(action: AuthAction) -> Result<()> {
    match action {
        AuthAction::Login { token, relay } => cmd_auth_login(token, relay).await,
        AuthAction::Token { token, relay } => cmd_auth_token(&token, relay),
        AuthAction::Logout => cmd_auth_logout(),
        AuthAction::Status(args) => cmd_auth_status(args.format),
    }
}

/// Store a personal access token in the credentials file.
fn cmd_auth_token(token: &str, relay: Option<String>) -> Result<()> {
    let relay_url = relay.unwrap_or_else(|| kutl_client::DEFAULT_KUTLHUB_RELAY_URL.to_owned());
    let creds = kutl_client::StoredCredentials {
        token: token.to_owned(),
        relay_url: relay_url.clone(),
        account_id: String::new(),
        display_name: String::new(),
    };
    let path = kutl_client::credentials::default_credentials_path()?;
    creds.save(&path)?;
    println!("Token saved.");
    println!("  relay: {relay_url}");
    println!("Use `kutl join <owner/space>` to connect a space.");
    Ok(())
}

/// Handle `kutl auth login`: store a PAT directly or run the device flow.
///
/// `--relay` defaults to the hosted-deployment URL (not the local-dev
/// relay). Auth tokens come from the hosted web UI (PATs) or its OAuth
/// device flow; self-hosted relays use DID auth and don't need this
/// command. Pass `--relay` explicitly to override.
async fn cmd_auth_login(token: Option<String>, relay: Option<String>) -> Result<()> {
    let relay_url = relay.unwrap_or_else(|| kutl_client::DEFAULT_KUTLHUB_RELAY_URL.to_owned());
    if let Some(pat) = token {
        // Direct PAT login — store it.
        let creds = kutl_client::StoredCredentials {
            token: pat,
            relay_url,
            // Account info not available from a bare PAT.
            // Will be populated on first use when relay validates the token.
            account_id: String::new(),
            display_name: String::new(),
        };
        let path = kutl_client::credentials::default_credentials_path()?;
        creds.save(&path)?;
        println!("Token saved to {}.", path.display());
        println!("  relay: {}", creds.relay_url);
        Ok(())
    } else {
        cmd_auth_login_device_flow(&relay_url).await
    }
}

/// Run the OAuth device flow against the relay and save the resulting credentials.
async fn cmd_auth_login_device_flow(relay_url: &str) -> Result<()> {
    let base = kutl_client::ws_url_to_http(relay_url);
    let client = reqwest::Client::new();

    // Step 1: Request device code.
    let resp = client
        .post(format!("{base}/auth/device"))
        .timeout(DEVICE_FLOW_REQUEST_TIMEOUT)
        .send()
        .await
        .context("failed to initiate device flow — is the relay reachable?")?;

    if !resp.status().is_success() {
        anyhow::bail!(
            "device flow initiation failed: {}",
            resp.text()
                .await
                .unwrap_or_else(|e| format!("(failed to read response: {e})"))
        );
    }

    let device: DeviceCodeResponse = resp
        .json()
        .await
        .context("failed to parse device code response")?;

    // Step 2: Display instructions.
    println!("Open this URL in your browser:");
    println!("  {}", device.verification_url);
    println!();
    println!("Enter code: {}", device.user_code);
    println!();

    // Step 3: Try to open browser automatically.
    if let Err(e) = open::that(&device.verification_url) {
        eprintln!("warning: could not open browser automatically: {e}");
    }

    // Step 4: Poll for token.
    println!("Waiting for authorization...");
    let poll_interval = std::time::Duration::from_secs(u64::from(device.interval));
    loop {
        tokio::time::sleep(poll_interval).await;

        let resp = client
            .post(format!("{base}/auth/device/token"))
            .json(&serde_json::json!({"device_code": device.device_code}))
            .timeout(DEVICE_FLOW_REQUEST_TIMEOUT)
            .send()
            .await
            .context("failed to poll device token endpoint")?;

        let status = resp.status().as_u16();

        if resp.status().is_success() {
            let token_resp: DeviceTokenResponse = resp
                .json()
                .await
                .context("failed to parse device token response")?;

            let creds = device_credentials_from_token(token_resp);
            let path = kutl_client::credentials::default_credentials_path()?;
            creds.save(&path)?;

            println!("Authenticated as {}.", creds.display_name);
            println!("  credentials saved to {}", path.display());

            return Ok(());
        }

        if status == HTTP_PRECONDITION_REQUIRED {
            // Authorization pending — keep polling.
            continue;
        }

        if status == HTTP_GONE {
            anyhow::bail!("device code expired — run `kutl auth login` again to restart");
        }

        // Unexpected status.
        anyhow::bail!(
            "unexpected response from device token endpoint (HTTP {status}): {}",
            resp.text()
                .await
                .unwrap_or_else(|e| format!("(failed to read response: {e})"))
        );
    }
}

/// Delete stored credentials and log out. Names the spelling that was on
/// disk (a `.json` written before the TOML move included), read before the
/// deletion because nothing is left to name afterwards.
fn cmd_auth_logout() -> Result<()> {
    let path = kutl_client::credentials::default_credentials_path()?;
    if let Some(found) = kutl_client::text_file::existing(&path) {
        kutl_client::credentials::delete_credentials(&path)?;
        println!("Credentials removed from {}.", found.display());
    } else {
        println!("No stored credentials found.");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Async WebSocket helpers
// ---------------------------------------------------------------------------
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

/// Timeout for the async WebSocket connection and handshake.
const WS_CONNECT_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(10));

/// Timeout for receiving a single relay response.
const WS_RECV_TIMEOUT: std::time::Duration =
    kutl_core::std_duration(kutl_core::SignedDuration::from_secs(10));

/// An async WebSocket connection for single request-response exchanges.
///
/// Used by CLI commands that need a brief relay interaction (e.g. `join`
/// with an owner/slug target). Wraps `tokio-tungstenite` and handles the
/// handshake internally.
struct AsyncRelayConn {
    ws: kutl_client::WsStream,
}

impl AsyncRelayConn {
    /// Connect, perform handshake with an auth token, and return the connection.
    /// The whole connect-and-handshake sits under [`WS_CONNECT_TIMEOUT`].
    async fn connect(relay_url: &str, auth_token: &str) -> Result<Self> {
        let connected = tokio::time::timeout(
            WS_CONNECT_TIMEOUT,
            kutl_client::connect_and_handshake(
                relay_url,
                "62457a72-e8ec-467c-8440-5dacabbc1fe7",
                auth_token,
                "",
            ),
        )
        .await
        .context("relay connection timed out")?;
        // Read the relay's code, never its prose — an auth refusal names the
        // credential slot to change, and every other refusal already states
        // its own remedy.
        let (ws, _ack) = match connected {
            Ok(ok) => ok,
            Err(kutl_client::HandshakeError::Refused {
                auth_failed: true, ..
            }) => anyhow::bail!(kutl_client::credentials::refused_token_remedy(relay_url)),
            Err(kutl_client::HandshakeError::Refused { message, .. }) => {
                anyhow::bail!("relay refused the connection: {message}")
            }
            Err(kutl_client::HandshakeError::VersionGap(gap)) => anyhow::bail!(gap),
            Err(kutl_client::HandshakeError::Transport(e)) => return Err(e),
        };

        Ok(Self { ws })
    }

    /// Send an envelope and wait for the next response envelope.
    async fn request(
        &mut self,
        envelope: &kutl_proto::sync::SyncEnvelope,
    ) -> Result<kutl_proto::sync::SyncEnvelope> {
        let bytes = kutl_proto::protocol::encode_envelope(envelope);
        self.ws
            .send(Message::Binary(bytes.into()))
            .await
            .context("failed to send message")?;

        Self::recv_envelope(&mut self.ws).await
    }

    /// Receive and decode the next binary envelope from the stream.
    async fn recv_envelope(
        ws: &mut kutl_client::WsStream,
    ) -> Result<kutl_proto::sync::SyncEnvelope> {
        tokio::time::timeout(WS_RECV_TIMEOUT, async {
            loop {
                let msg = ws
                    .next()
                    .await
                    .context("connection closed by relay")?
                    .context("failed to read from relay")?;
                match msg {
                    Message::Binary(bytes) => {
                        return kutl_proto::protocol::decode_envelope(&bytes)
                            .context("failed to decode envelope");
                    }
                    Message::Close(_) => anyhow::bail!("connection closed by relay"),
                    _ => {} // skip ping/pong/text
                }
            }
        })
        .await
        .context("relay response timed out")?
    }

    /// Gracefully close the connection.
    async fn close(mut self) -> Result<()> {
        self.ws.close(None).await.ok();
        Ok(())
    }
}

/// The `name` config key — maps to [`identity::Identity::display_name`].
const CONFIG_KEY_NAME: &str = "name";
/// The `email` config key — maps to [`identity::Identity::email`].
const CONFIG_KEY_EMAIL: &str = "email";
/// Every user-settable identity config key, in display order. Single source
/// of truth for `set`, `get`, and `list`.
const CONFIG_KEYS: [&str; 2] = [CONFIG_KEY_NAME, CONFIG_KEY_EMAIL];

/// Read one identity config value by key, or `None` when the key is valid but
/// unset. Errors when the key is not a recognized config key.
fn config_value(id: &identity::Identity, key: &str) -> Result<Option<String>> {
    match key {
        CONFIG_KEY_NAME => Ok(id.display_name.clone()),
        CONFIG_KEY_EMAIL => Ok(id.email.clone()),
        _ => anyhow::bail!(
            "unknown config key: {key} — valid keys are {}",
            CONFIG_KEYS.join(", ")
        ),
    }
}

/// Render all set identity config keys as `key = value` lines, sorted by
/// [`CONFIG_KEYS`] order. Unset keys are omitted. Returns a short notice when
/// nothing is set.
fn render_config_list(id: &identity::Identity) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for key in CONFIG_KEYS {
        // `config_value` only fails on an unknown key; `CONFIG_KEYS` are all
        // known, so a value lookup here cannot error.
        if let Ok(Some(value)) = config_value(id, key) {
            let _ = writeln!(out, "{key} = {value}");
        }
    }
    if out.is_empty() {
        out.push_str("no config values set\n");
    }
    out
}

/// Handle `kutl config`: read and mutate identity fields.
///
/// `set` mutates a field; `get`/`list` read them (symmetric read side). The
/// aggregate `kutl status` / `kutl auth status` also surface identity, but
/// `config get`/`list` are the focused key/value read path.
fn cmd_config(action: ConfigAction) -> Result<()> {
    let path = identity::default_identity_path()?;
    let load_identity = || {
        identity::Identity::load(&path).context(
            "no identity found — run `kutl init` to create a space or `kutl join` to join one",
        )
    };

    match action {
        ConfigAction::Set { key, value } => {
            let mut id = load_identity()?;

            match key.as_str() {
                CONFIG_KEY_NAME => id.display_name = Some(value.clone()),
                CONFIG_KEY_EMAIL => id.email = Some(value.clone()),
                _ => anyhow::bail!(
                    "unknown config key: {key} — valid keys are {}",
                    CONFIG_KEYS.join(", ")
                ),
            }

            id.save(&path)?;
            println!("Set {key} = {value}");
            Ok(())
        }
        ConfigAction::Get {
            key: Some(key),
            format,
        } => {
            let id = load_identity()?;
            let value = config_value(&id, &key)?;
            match format {
                // One-key object, self-describing for pipelines; an unset key
                // is null (still exit 0 — absence is a value, not an error).
                OutputFormat::Json => {
                    println!("{}", serde_json::json!({ key.as_str(): value }));
                }
                OutputFormat::Human => match value {
                    Some(value) => println!("{value}"),
                    None => println!("{key} is not set"),
                },
            }
            Ok(())
        }
        // `get` with no key behaves like `list`: show every key.
        ConfigAction::Get { key: None, format } | ConfigAction::List(StatusArgs { format }) => {
            let id = load_identity()?;
            match format {
                // Every valid key, unset ones as null — a stable shape that
                // does not grow/shrink with what happens to be set.
                OutputFormat::Json => {
                    let mut map = serde_json::Map::new();
                    for key in CONFIG_KEYS {
                        let value = config_value(&id, key)?;
                        map.insert(key.to_owned(), serde_json::json!(value));
                    }
                    println!("{}", serde_json::Value::Object(map));
                }
                OutputFormat::Human => print!("{}", render_config_list(&id)),
            }
            Ok(())
        }
    }
}

/// The space and document a `kutl document <verb>` target resolves to: the
/// internal history store to read, plus the space context for the document's
/// signal records.
#[derive(Debug)]
struct DocumentTarget {
    /// The internal `.dt` store holding the document's change history.
    dt_path: PathBuf,
    /// Root of the space that tracks the document.
    space_root: PathBuf,
    /// The tracking space's id.
    space_id: String,
    /// The tracked document's id.
    document_id: String,
}

/// Resolve a `kutl document <verb>` argument — the document's working-tree
/// path — to its tracked identity. The one resolver shared by
/// `log`/`blame`/`restore`.
///
/// The file's own location names its space (the enclosing-space walk, exactly
/// like every other space-scoped verb); the space's persisted state then maps
/// the space-relative path to the document id. `.dt` files are kutl's internal
/// storage, not part of the command contract — one is rejected with a pointer
/// at the working-tree path.
fn resolve_document_target(arg: &std::path::Path) -> Result<DocumentTarget> {
    if arg.extension().is_some_and(|e| e == "dt") {
        anyhow::bail!(
            "pass the document's working-tree path (e.g. notes/plan.md) — .dt files are kutl's internal storage"
        );
    }

    // The file itself need not exist on disk (a tracked doc may be locally
    // deleted), so absolutize against the cwd rather than canonicalizing.
    let abs = absolute_path(arg)?;
    // Walk up from the deepest EXISTING ancestor: the enclosing-space walk
    // needs a real directory to start from.
    let mut start = abs.as_path();
    while !start.exists() {
        start = start.parent().ok_or_else(|| {
            anyhow::anyhow!("no existing ancestor directory for {}", arg.display())
        })?;
    }
    let space_root = enclosing_space(start)?.ok_or_else(|| {
        anyhow::anyhow!(
            "{} is not inside a kutl space — document history lives in the space that tracks the file",
            arg.display()
        )
    })?;
    let config = space::SpaceConfig::load(&space_root)?;

    let rel_str = signals::normalize_space_rel_path(&space_root, &abs.to_string_lossy())?;
    let state = kutl_daemon::state::DaemonState::load_readonly(&space_root.join(".kutl"));
    let Some(entry) = state.documents.get(&rel_str) else {
        anyhow::bail!(
            "'{rel_str}' is not tracked in this space yet — a running daemon (or `kutl sync`) registers new files"
        );
    };
    let dt_path = space_root
        .join(".kutl")
        .join("docs")
        .join(format!("{id}.dt", id = entry.id));
    Ok(DocumentTarget {
        dt_path,
        space_root,
        space_id: config.space_id,
        document_id: entry.id.clone(),
    })
}

/// Absolutize a path against the current directory without requiring it to
/// exist (unlike `canonicalize`), then canonicalize the existing ancestor so
/// the space-root prefix match is symlink-stable.
fn absolute_path(arg: &std::path::Path) -> Result<PathBuf> {
    let joined = if arg.is_absolute() {
        arg.to_path_buf()
    } else {
        std::env::current_dir()
            .context("failed to determine current directory")?
            .join(arg)
    };
    // Canonicalize the deepest existing ancestor, then re-append the remainder,
    // so a not-yet-existing working file still resolves under its space root.
    let mut existing = joined.as_path();
    let mut tail = PathBuf::new();
    let canon_ancestor = loop {
        if let Ok(c) = existing.canonicalize() {
            break c;
        }
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                tail = std::path::Path::new(name).join(&tail);
                existing = parent;
            }
            _ => return Ok(joined),
        }
    };
    Ok(canon_ancestor.join(tail))
}

/// One rendered log block keyed by its wall-clock timestamp — the unit the
/// timeline merges. Changes and signal records share this shape so a single
/// descending sort interleaves them like `git log`.
pub(crate) struct LogBlock {
    pub(crate) timestamp_ms: i64,
    /// Chronological tiebreaker for entries sharing a millisecond: higher =
    /// more recent. Changes carry their position in the change list; signals
    /// sort after a same-ms change (they are appended). Keeps a busy same-ms
    /// burst in a stable "newest first" order the raw timestamp cannot.
    pub(crate) seq: usize,
    pub(crate) text: String,
}

/// Print the change history of a document, interleaved with its signal records.
///
/// Takes the document's working-tree path. Change entries and signal records
/// are merged into one timeline, most-recent-first. `--format json` emits
/// `{ changes, signals }` — each change row carries the change `id` (the
/// input `restore --to` takes) plus author/timestamp/boundary/intent, and
/// `signals` reuses the `signal list` view schema.
fn cmd_log(path: &std::path::Path, format: OutputFormat) -> Result<()> {
    let target = resolve_document_target(path)?;
    let doc = Document::load_readonly(&target.dt_path)
        .with_context(|| format!("failed to load {}", target.dt_path.display()))?;
    let changes = doc.changes();

    if matches!(format, OutputFormat::Json) {
        // Newest-first, mirroring the human timeline.
        let rows: Vec<serde_json::Value> = changes
            .iter()
            .rev()
            .map(|c| {
                serde_json::json!({
                    "id": c.id,
                    "author": c.author_did,
                    "timestamp_ms": c.timestamp,
                    "boundary": boundary_label(c.boundary),
                    "intent": (!c.intent.is_empty()).then_some(c.intent.as_str()),
                })
            })
            .collect();
        let signal_views = signals::document_signal_views(
            &target.space_root,
            &target.space_id,
            &target.document_id,
        )?;
        let payload = serde_json::json!({ "changes": rows, "signals": signal_views });
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    // Changes get ascending `seq` (their chronological position); signals
    // slot in after with a `seq` past every change so a same-ms change is
    // listed above its signal.
    let mut blocks: Vec<LogBlock> = changes
        .iter()
        .enumerate()
        .map(|(seq, change)| render_change_block(seq, change))
        .collect();

    let signals =
        signals::document_log_signals(&target.space_root, &target.space_id, &target.document_id)?;
    blocks.extend(signals.into_iter().map(|s| LogBlock {
        timestamp_ms: s.timestamp_ms,
        seq: changes.len(),
        text: s.block,
    }));

    if blocks.is_empty() {
        println!("no changes recorded for {}", path.display());
        return Ok(());
    }

    // Most recent first, like git log: by timestamp, then `seq`, both
    // descending — so a same-ms burst keeps its chronological "newest first".
    blocks.sort_by(|a, b| {
        b.timestamp_ms
            .cmp(&a.timestamp_ms)
            .then_with(|| b.seq.cmp(&a.seq))
    });
    for (i, block) in blocks.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("{}", block.text);
    }

    Ok(())
}

/// The stable lowercase label for a change's boundary discriminant. Shared
/// by the human log block and the JSON row so the vocabulary cannot drift.
fn boundary_label(boundary: i32) -> &'static str {
    match Boundary::try_from(boundary) {
        Ok(Boundary::Explicit) => "explicit",
        Ok(Boundary::Auto) => "auto",
        _ => "unspecified",
    }
}

/// One `document blame` row: a line's number, its author DID, and its text.
///
/// The stable `--format json` schema — an array of these, one per line.
#[derive(serde::Serialize)]
struct BlameRow {
    line: usize,
    author: String,
    text: String,
}

/// Print the per-line authorship of a text document (git-blame for a CRDT).
///
/// Takes the document's working-tree path, mirroring `kutl document log`.
/// Each line is attributed to the durable author DID of its first character
/// (git-blame convention). A blob/binary document has no per-character oplog,
/// so blame is text-only — rejected with the same wording `document restore`
/// uses.
fn cmd_blame(path: &std::path::Path, format: OutputFormat) -> Result<()> {
    let target = resolve_document_target(path)?;
    let mut doc = Document::load_readonly(&target.dt_path)
        .with_context(|| format!("failed to load {}", target.dt_path.display()))?;

    // A blob (binary) document carries no change history: blame is text-only.
    if doc.changes().is_empty() {
        anyhow::bail!("no recorded change history to blame — text documents only");
    }

    // Recover attribution for history recorded before durable author
    // bindings existed: the change log's authors + spans re-bind those
    // agents in memory for this render.
    doc.backfill_author_bindings();

    let rows = doc.blame_with_text();
    // Attribution can still be genuinely missing (edits whose changes were
    // never recorded or were evicted). Say so rather than letting a wall of
    // "unknown" read as the command being broken.
    let unknown = rows.iter().filter(|(_, a, _)| a == "unknown").count();
    if unknown > 0 {
        eprintln!(
            "note: {unknown} of {} line(s) have no recorded authorship (their edits \
             predate recorded attribution)",
            rows.len()
        );
    }
    match format {
        OutputFormat::Human => {
            for (line, author, text) in &rows {
                println!("{author}\t{line}\t{text}");
            }
        }
        OutputFormat::Json => {
            let rows: Vec<BlameRow> = rows
                .into_iter()
                .map(|(line, author, text)| BlameRow { line, author, text })
                .collect();
            let out = serde_json::to_string_pretty(&rows)?;
            println!("{out}");
        }
    }
    Ok(())
}

/// Render one change as a `LogBlock` (the `change <id>` entry `cmd_log` printed
/// inline before signals were interleaved). `seq` is the change's position in
/// the chronological change list (the same-ms tiebreaker).
fn render_change_block(seq: usize, change: &Change) -> LogBlock {
    let mut text = String::new();
    let _ = writeln!(text, "change {}", change.id);
    let _ = writeln!(text, "Author: {}", change.author_did);
    let _ = writeln!(text, "Date:   {}", format_timestamp(change.timestamp));
    let _ = writeln!(text, "Boundary: {}", boundary_label(change.boundary));
    if let Some(span) = &change.version_span {
        let _ = writeln!(text, "Span:   {:?} → {:?}", span.start, span.end);
    }
    let _ = writeln!(text);
    let _ = write!(text, "    {}", change.intent);
    LogBlock {
        timestamp_ms: change.timestamp,
        seq,
        text,
    }
}

/// Format a Unix-millis timestamp as a human-readable UTC string.
pub(crate) fn format_timestamp(millis: i64) -> String {
    match Timestamp::from_millisecond(millis) {
        Ok(ts) => ts.strftime("%Y-%m-%d %H:%M:%S%.3f UTC").to_string(),
        Err(_) => format!("{millis}ms (invalid timestamp)"),
    }
}

/// `kutl document restore` — reconstruct a text document as of a chosen point
/// and re-assert it as a forward edit.
///
/// The content as of `--at <time>` / `--to <change-id>` is materialized from
/// the oplog and written back to the working file; a running daemon then diffs
/// it forward as a new edit (never a history rewrite), so a concurrent editor's
/// later ops merge rather than being destroyed. This means the restore only
/// propagates once the daemon runs — the same read-your-writes caveat the
/// signal records carry. Text documents only: a blob (binary) document has no
/// per-version oplog to reconstruct from.
///
/// Restore points beyond the change sidecar's FIFO retention (100k entries)
/// are unavailable: the oplog retains the ops but the sidecar — the
/// timestamp→frontier index — has evicted the entry (a documented v1 limit).
fn cmd_restore(args: &RestoreArgs) -> Result<()> {
    // The positional arg is the working file the daemon watches and we write
    // back to; the shared document resolver rejects internal `.dt` paths.
    // A blob document has no text oplog, so loading its history is the
    // reliable text/blob signal at this layer.
    let restored = reconstruct_restore(args)?;

    // Re-assert the historical content as a forward edit by rewriting the
    // working file in place. The daemon's watcher character-diffs it into
    // minimal ops at the CURRENT tip — convergence-safe, never a rollback.
    kutl_core::fs::write_atomic(&args.path, restored.content.as_bytes())
        .with_context(|| format!("writing the restored content to {}", args.path.display()))?;

    println!(
        "restored {path} to {label} — a running daemon will sync it forward as a new edit",
        path = args.path.display(),
        label = restored.label
    );
    Ok(())
}

/// The reconstructed restore point: the as-of content plus a human label for
/// the selected change (for the confirmation line and testability).
struct RestorePoint {
    /// The reconstructed content as of the selected change's frontier.
    content: String,
    /// A human label naming the selected point (e.g. `change <id>`).
    label: String,
}

/// Resolve + select + reconstruct — the pure core of `cmd_restore` (the disk
/// write is a thin wrapper around this). Loads the document's `.dt` sidecar,
/// selects the target change per `--at`/`--to`, and materializes the content as
/// of that change's frontier.
fn reconstruct_restore(args: &RestoreArgs) -> Result<RestorePoint> {
    let target = resolve_document_target(&args.path)
        .with_context(|| format!("resolving the document at {}", args.path.display()))?;

    // A blob document has no text oplog: loading fails (no readable oplog) or
    // yields an empty change history. Either way, restore is text-only.
    let doc = kutl_core::Document::load_readonly(&target.dt_path).with_context(|| {
        format!(
            "loading the change history for {} — restore is for text documents only (a binary/blob document or one without a .dt oplog has no per-version history)",
            args.path.display()
        )
    })?;

    reconstruct_from_doc(&doc, args.at.as_deref(), args.to.as_deref())
        .with_context(|| format!("restoring {}", args.path.display()))
}

/// Select the target change and reconstruct its as-of content from an already
/// loaded document — the pure heart of restore (no disk resolution), so the
/// select→frontier→content path is unit-testable.
fn reconstruct_from_doc(
    doc: &kutl_core::Document,
    at: Option<&str>,
    to: Option<&str>,
) -> Result<RestorePoint> {
    let changes = doc.changes();
    if changes.is_empty() {
        anyhow::bail!("no recorded change history to restore from — text documents only");
    }

    let change = select_restore_point(changes, at, to)?;
    Ok(RestorePoint {
        content: doc.content_at_change(change),
        label: format!("change {id}", id = change.id),
    })
}

/// Pick the change to restore to. `--to <id>` selects exactly; `--at <time>`
/// selects the newest change whose timestamp is at or before the parsed
/// instant. Exactly one selector is required.
fn select_restore_point<'a>(
    changes: &'a [Change],
    at: Option<&str>,
    to: Option<&str>,
) -> Result<&'a Change> {
    match (at, to) {
        (Some(_), Some(_)) => anyhow::bail!("pass only one of --at or --to"),
        (None, None) => anyhow::bail!(
            "pass --at <time> or --to <change-id> — see `kutl document log` for change ids"
        ),
        (None, Some(id)) => changes
            .iter()
            .find(|c| c.id == id)
            .ok_or_else(|| anyhow::anyhow!("no change with id {id} — see `kutl document log`")),
        (Some(when), None) => {
            let t = parse_at_time_to_millis(when)
                .with_context(|| format!("parsing --at time {when}"))?;
            changes
                .iter()
                .filter(|c| c.timestamp <= t)
                // Newest change at or before T; break ties on the change id so
                // a same-millisecond burst selects deterministically.
                .max_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)))
                .ok_or_else(|| anyhow::anyhow!("no change at or before {when}"))
        }
    }
}

/// Parse a `--at` restore time to Unix milliseconds — the inverse of
/// [`format_timestamp`]. Accepts an RFC3339 timestamp (e.g.
/// `2026-07-18T15:00:00Z`), or a civil date/datetime (`2026-07-18` or
/// `2026-07-18T15:00`) interpreted in the system time zone. No inline epoch
/// math — all conversion goes through `jiff`. Natural-language times are out of
/// scope for v1.
fn parse_at_time_to_millis(when: &str) -> Result<i64> {
    use std::str::FromStr as _;

    // 1) RFC3339 instant (has an explicit offset or `Z`) → absolute.
    if let Ok(ts) = Timestamp::from_str(when) {
        return Ok(ts.as_millisecond());
    }

    // 2) Civil datetime (no offset) → resolve in the system time zone.
    let zone = jiff::tz::TimeZone::system();
    if let Ok(dt) = jiff::civil::DateTime::from_str(when) {
        let zoned = dt
            .to_zoned(zone.clone())
            .with_context(|| format!("resolving {when} in the system time zone"))?;
        return Ok(zoned.timestamp().as_millisecond());
    }

    // 3) Civil date (midnight) → resolve in the system time zone.
    if let Ok(date) = jiff::civil::Date::from_str(when) {
        let zoned = date
            .to_zoned(zone)
            .with_context(|| format!("resolving {when} in the system time zone"))?;
        return Ok(zoned.timestamp().as_millisecond());
    }

    anyhow::bail!(
        "could not parse {when} as an RFC3339 timestamp (2026-07-18T15:00:00Z) or a civil date/datetime (2026-07-18 or 2026-07-18T15:00)"
    )
}

async fn cmd_mcp_serve(agent: Option<&str>) -> Result<()> {
    // Agent-facing commands inspect the kutl-managed AGENTS.md block on
    // startup and warn or refuse based on its sentinel version. The
    // check anchors at the local space's
    // anchor (git root if any, else .kutlspace-parent), not the
    // working-directory's git root, because the daemon serves the
    // local space regardless of where the user invoked `mcp serve` from.
    let working_dir = std::env::var("KUTL_HOME").map_or_else(
        |_| std::env::current_dir().expect("failed to get working directory"),
        std::path::PathBuf::from,
    );
    if let Some((_config, space_root)) = kutl_client::space_config::discover_space(&working_dir) {
        let anchor = agents_md::anchor_for(&space_root);
        match agents_md::check_at_repo_root(&anchor)? {
            agents_md::CheckOutcome::Current => {}
            agents_md::CheckOutcome::StaleCompatible { sentinel } => {
                // Higher severity than `cmd_init`'s `note:` for the same
                // condition: `kutl mcp serve` is the agent-facing surface, so
                // a stale block actively affects downstream agent
                // behavior rather than just being a future-PR concern.
                eprintln!(
                    "warning: AGENTS.md kutl block was generated by v{sentinel}; current is v{}; run `kutl space apply` to refresh",
                    env!("CARGO_PKG_VERSION"),
                );
            }
            agents_md::CheckOutcome::StaleIncompatible { sentinel } => {
                agents_md::handle_incompatible(&anchor, &sentinel)?;
            }
            agents_md::CheckOutcome::Absent => {
                // Non-fatal: this server's stdin carries JSON-RPC
                // frames, never a terminal, so a TTY-gated
                // prompt-or-refuse would refuse every launch.
                agents_md::warn_absent(&anchor);
            }
        }
    }
    let agent_name = watch::resolve_agent_name(agent);
    watch::run(&agent_name).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_shortcut_and_canonical_parse_identically() {
        use clap::Parser;
        let short = Cli::try_parse_from(["kutl", "init", "--relay", "ws://x"]).unwrap();
        let long = Cli::try_parse_from(["kutl", "space", "init", "--relay", "ws://x"]).unwrap();
        // Both must resolve to the same InitArgs.
        let Command::Init(a) = short.command else {
            panic!("shortcut not Init");
        };
        let Command::Space(SpaceCli {
            action: SpaceAction::Init(b),
        }) = long.command
        else {
            panic!("canonical not space init");
        };
        assert_eq!(a.relay, b.relay);
        assert_eq!(a.relay, "ws://x");
    }

    #[test]
    fn test_register_error_message_empty_body_names_status_and_relay() {
        let relay = "ws://relay.example.com:9100/ws";
        let msg = register_error_message(relay, reqwest::StatusCode::NOT_FOUND, "");
        assert!(!msg.is_empty(), "message must not be empty");
        assert!(
            msg.contains(relay),
            "message must name the relay url: {msg}"
        );
        assert!(
            msg.contains("404"),
            "message must name the status code: {msg}"
        );
        assert!(
            msg.to_lowercase().contains("registration endpoint"),
            "message must hint at the likely cause: {msg}"
        );
    }

    #[test]
    fn test_register_error_message_whitespace_body_treated_as_empty() {
        let msg =
            register_error_message("ws://x:9100/ws", reqwest::StatusCode::NOT_FOUND, "   \n  ");
        assert!(
            msg.contains("with no body"),
            "whitespace body is empty: {msg}"
        );
    }

    #[test]
    fn test_register_error_message_preserves_nonempty_body() {
        let msg = register_error_message(
            "ws://x:9100/ws",
            reqwest::StatusCode::BAD_REQUEST,
            "name too long",
        );
        assert!(
            msg.contains("400"),
            "message must name the status code: {msg}"
        );
        assert!(
            msg.contains("name too long"),
            "message must include the body: {msg}"
        );
    }

    #[test]
    fn test_new_grammar_parses() {
        use clap::Parser;
        for argv in [
            vec!["kutl", "space", "list"],
            vec!["kutl", "space", "status"],
            vec!["kutl", "space", "apply"],
            vec!["kutl", "document", "log", "notes/a.md"],
            vec!["kutl", "document", "log", "notes/a.md", "--format", "json"],
            vec!["kutl", "mcp", "serve"],
            vec!["kutl", "daemon", "status"],
            vec!["kutl", "auth", "status"],
            vec!["kutl", "config", "get"],
            vec!["kutl", "config", "get", "name"],
            vec!["kutl", "config", "get", "name", "--format", "json"],
            vec!["kutl", "config", "list"],
            vec!["kutl", "config", "list", "--format", "json"],
            vec!["kutl", "agent", "list", "--format", "json"],
            vec!["kutl", "surface"],
            vec!["kutl", "space", "surface"],
            vec!["kutl", "signal", "list"],
            vec!["kutl", "signal", "view", "sig-1"],
        ] {
            assert!(
                Cli::try_parse_from(argv.clone()).is_ok(),
                "should parse: {argv:?}"
            );
        }
    }

    #[test]
    fn test_signal_commands_parse_with_all_flags() {
        use clap::Parser;

        // `list` accepts every filter/format/fetch flag.
        for argv in [
            vec!["kutl", "signal", "list"],
            vec!["kutl", "signal", "list", "--open"],
            vec!["kutl", "signal", "list", "--closed"],
            vec!["kutl", "signal", "list", "--all"],
            vec!["kutl", "signal", "list", "--doc", "notes/a.md"],
            vec!["kutl", "signal", "list", "--kind", "flag"],
            vec!["kutl", "signal", "list", "--kind", "chat"],
            vec!["kutl", "signal", "list", "--kind", "decision"],
            vec!["kutl", "signal", "list", "--kind", "reply"],
            vec!["kutl", "signal", "list", "--format", "json"],
            vec!["kutl", "signal", "list", "--fetch"],
            vec!["kutl", "signal", "view", "sig-1", "--format", "json"],
            vec!["kutl", "signal", "view", "sig-1", "--fetch"],
        ] {
            assert!(
                Cli::try_parse_from(argv.clone()).is_ok(),
                "should parse: {argv:?}"
            );
        }

        // An unknown --kind is rejected at parse time (ValueEnum).
        assert!(
            Cli::try_parse_from(["kutl", "signal", "list", "--kind", "bogus"]).is_err(),
            "unknown kind must be rejected"
        );

        // `--space` is gone: signal verbs are cwd-scoped, not selector-driven.
        assert!(
            Cli::try_parse_from(["kutl", "signal", "list", "--space", "my-space"]).is_err(),
            "--space must be rejected"
        );

        // `list --kind flag` parses into the expected SignalKind.
        let cli = Cli::try_parse_from(["kutl", "signal", "list", "--kind", "flag"]).unwrap();
        let Command::Signal(SignalCli {
            action: SignalAction::List(args),
        }) = cli.command
        else {
            panic!("not a signal list command");
        };
        assert_eq!(args.kind, Some(signals::SignalKind::Flag));
    }

    #[test]
    fn test_signal_flag_kind_flags_are_value_enums() {
        use clap::Parser;

        // Every creatable kind parses, and lands as the typed variant.
        let cli = Cli::try_parse_from([
            "kutl",
            "signal",
            "create",
            "--kind",
            "review_requested",
            "--message",
            "look at this",
        ])
        .expect("review_requested must parse");
        let Command::Signal(SignalCli {
            action: SignalAction::Create(args),
        }) = cli.command
        else {
            panic!("not a signal create command");
        };
        assert_eq!(args.kind, signals::FlagKindArg::ReviewRequested);

        // A misspelled kind dies at parse time, not after the relay round-trip.
        assert!(
            Cli::try_parse_from([
                "kutl",
                "signal",
                "create",
                "--kind",
                "bogus",
                "--message",
                "m",
            ])
            .is_err(),
            "unknown --kind must be rejected"
        );

        // `list --flag-kind` filters the same vocabulary, and `comment` is
        // filterable there even though it is not creatable.
        for kind in kutl_proto::vocab::flag_kind_names(kutl_proto::vocab::FLAG_KINDS) {
            assert!(
                Cli::try_parse_from(["kutl", "signal", "list", "--flag-kind", kind]).is_ok(),
                "should parse --flag-kind {kind}"
            );
        }
        assert!(
            Cli::try_parse_from(["kutl", "signal", "list", "--flag-kind", "bogus"]).is_err(),
            "unknown --flag-kind must be rejected"
        );
    }

    #[test]
    fn test_signal_transition_commands_parse() {
        use clap::Parser;

        // All three transition verbs and their flags parse.
        for argv in [
            vec!["kutl", "signal", "close", "sig-1"],
            vec!["kutl", "signal", "close", "sig-1", "--reason", "resolved"],
            vec!["kutl", "signal", "close", "sig-1", "--reason", "declined"],
            vec!["kutl", "signal", "close", "sig-1", "--reason", "withdrawn"],
            vec!["kutl", "signal", "reopen", "sig-1"],
            vec!["kutl", "signal", "resolve", "sig-1"],
        ] {
            assert!(
                Cli::try_parse_from(argv.clone()).is_ok(),
                "should parse: {argv:?}"
            );
        }

        // An unknown --reason is rejected at parse time (ValueEnum).
        assert!(
            Cli::try_parse_from(["kutl", "signal", "close", "sig-1", "--reason", "bogus"]).is_err(),
            "unknown reason must be rejected"
        );

        // `--space` is gone from the transition verbs: cwd-scoped only.
        for argv in [
            vec!["kutl", "signal", "close", "sig-1", "--space", "my-space"],
            vec!["kutl", "signal", "reopen", "sig-1", "--space", "my-space"],
            vec!["kutl", "signal", "resolve", "sig-1", "--space", "my-space"],
        ] {
            assert!(
                Cli::try_parse_from(argv.clone()).is_err(),
                "--space must be rejected: {argv:?}"
            );
        }

        // `close --reason declined` parses into the expected action.
        let cli = Cli::try_parse_from(["kutl", "signal", "close", "sig-9", "--reason", "declined"])
            .unwrap();
        let Command::Signal(SignalCli {
            action: SignalAction::Close { id, reason },
        }) = cli.command
        else {
            panic!("not a signal close command");
        };
        assert_eq!(id, "sig-9");
        assert_eq!(reason, Some(signals::CloseReasonArg::Declined));

        // `reopen` parses into the expected action.
        let cli = Cli::try_parse_from(["kutl", "signal", "reopen", "sig-2"]).unwrap();
        let Command::Signal(SignalCli {
            action: SignalAction::Reopen { id },
        }) = cli.command
        else {
            panic!("not a signal reopen command");
        };
        assert_eq!(id, "sig-2");

        // `resolve` parses into the expected action.
        let cli = Cli::try_parse_from(["kutl", "signal", "resolve", "sig-3"]).unwrap();
        let Command::Signal(SignalCli {
            action: SignalAction::Resolve { id },
        }) = cli.command
        else {
            panic!("not a signal resolve command");
        };
        assert_eq!(id, "sig-3");
    }

    #[test]
    fn test_agent_commands_parse() {
        use clap::Parser;

        // `agent create --name claude-laptop` parses into the Create action.
        let cli =
            Cli::try_parse_from(["kutl", "agent", "create", "--name", "claude-laptop"]).unwrap();
        let Command::Agent(agent::AgentCli {
            action: agent::AgentAction::Create { name },
        }) = cli.command
        else {
            panic!("not an agent create command");
        };
        assert_eq!(name, "claude-laptop");

        // `agent list` parses into the List action.
        let cli = Cli::try_parse_from(["kutl", "agent", "list"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Agent(agent::AgentCli {
                action: agent::AgentAction::List { .. },
            })
        ));

        // `create` requires --name.
        assert!(
            Cli::try_parse_from(["kutl", "agent", "create"]).is_err(),
            "create must require --name"
        );
    }

    #[test]
    fn test_removed_bare_forms_error() {
        use clap::Parser;
        // `update`/`upgrade` are real commands (they parse and exit 0), so they
        // do not belong in this removed list. The `relay` noun is gone: the
        // relay's operator interface is its config + authorized_keys file
        // (git-ops friendly), not CLI verbs.
        for argv in [
            vec!["kutl", "watch"],
            vec!["kutl", "log", "a.dt"],
            vec!["kutl", "relay", "authorize"],
        ] {
            assert!(
                Cli::try_parse_from(argv.clone()).is_err(),
                "should be removed: {argv:?}"
            );
        }
    }

    /// An explicit `--dir` target is exempt from the `$KUTL_HOME` ceiling:
    /// the ceiling stops AMBIENT cwd resolution from binding to whatever
    /// space the process sits inside, but a directory the caller named
    /// outright must resolve even when it lies outside the declared
    /// workspace (the demo/sim layout: `KUTL_HOME` is a config dir, the
    /// space a sibling).
    #[test]
    #[serial_test::serial]
    fn test_resolve_space_root_explicit_dir_ignores_kutl_home_ceiling() {
        let outer = tempfile::TempDir::new().unwrap();
        let home = outer.path().join("homes/viewer");
        std::fs::create_dir_all(&home).unwrap();
        // SAFETY: #[serial] ensures no other test mutates env vars in parallel.
        unsafe {
            std::env::set_var("KUTL_HOME", &home);
        }
        let workspace = outer.path().join("workspace");
        kutl_client::SpaceConfig {
            space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
            relay_url: "ws://127.0.0.1:9/ws".into(),
        }
        .save(&workspace)
        .unwrap();

        // Explicit dir: resolves despite sitting outside $KUTL_HOME.
        let root = resolve_space_root(Some(workspace.clone())).unwrap();
        assert_eq!(root, workspace.canonicalize().unwrap());

        // The ambient resolver still fences the same tree out — the
        // ceiling's actual job is untouched.
        assert!(
            enclosing_space(&workspace.canonicalize().unwrap())
                .unwrap()
                .is_none(),
            "ambient resolution must still respect the ceiling"
        );
    }

    /// The shared document resolver takes working-tree paths only: an internal
    /// `.dt` path is rejected with a pointer at the working-tree path.
    #[test]
    fn test_resolve_document_target_rejects_dt_paths() {
        let err = resolve_document_target(std::path::Path::new("notes/plan.dt")).unwrap_err();
        assert!(
            err.to_string().contains("internal storage"),
            "should say .dt is internal: {err}"
        );
        assert!(
            err.to_string().contains("working-tree path"),
            "should point at the working-tree path: {err}"
        );
    }

    /// The shared document resolver maps a working-tree path to its tracked
    /// document via the enclosing space's persisted state, and errors clearly
    /// on an untracked path.
    ///
    /// `#[serial]` + an explicit `KUTL_HOME` above the space: a set
    /// `KUTL_HOME` is a hard resolution boundary, and the serial env-mutating
    /// tests in this binary leave one dangling — an ambient value pointing
    /// elsewhere would fence this test's space out.
    #[test]
    #[serial_test::serial]
    fn test_resolve_document_target_resolves_tracked_and_rejects_untracked() {
        use kutl_daemon::state::{DaemonState, DocEntry};

        let boundary = tempfile::TempDir::new().unwrap();
        // SAFETY: #[serial] ensures no other test mutates env vars in parallel.
        unsafe {
            std::env::set_var("KUTL_HOME", boundary.path());
        }
        let space_dir = boundary.path().join("space");
        kutl_client::SpaceConfig {
            space_id: "be18b85f-77fc-424d-8379-acf19e8a1ce6".into(),
            relay_url: "ws://127.0.0.1:9/ws".into(),
        }
        .save(&space_dir)
        .unwrap();
        let kutl_dir = space_dir.join(".kutl");

        let doc_uuid = uuid::Uuid::from_u128(0xfeed_f00d).to_string();
        let mut state = DaemonState::default();
        state.documents.insert(
            "notes/plan.md".to_owned(),
            DocEntry {
                id: doc_uuid.clone(),
                confirmed: true,
                inode: None,
                last_written_hash: None,
            },
        );
        state.save(&kutl_dir).unwrap();
        std::fs::create_dir_all(space_dir.join("notes")).unwrap();
        std::fs::write(space_dir.join("notes/plan.md"), "hello\n").unwrap();

        // A tracked file resolves to its document id and internal store,
        // from an absolute path (no cwd dependency in the test).
        let target = resolve_document_target(&space_dir.join("notes/plan.md")).unwrap();
        assert_eq!(target.document_id, doc_uuid);
        assert_eq!(target.space_id, "be18b85f-77fc-424d-8379-acf19e8a1ce6");
        assert!(
            target
                .dt_path
                .ends_with(format!(".kutl/docs/{doc_uuid}.dt")),
            "dt path derives from the id: {}",
            target.dt_path.display()
        );

        // An untracked sibling errors with the tracking remedy.
        std::fs::write(space_dir.join("notes/other.md"), "x\n").unwrap();
        let err = resolve_document_target(&space_dir.join("notes/other.md")).unwrap_err();
        assert!(
            err.to_string().contains("not tracked"),
            "should say untracked: {err}"
        );
    }

    /// Reserved verbs must PARSE (clap accepts the args) so that the dispatch arm
    /// — not a raw clap error — produces the uniform "not yet built"
    /// message. This test pins the parse-side contract for every reserved shape
    /// (incl. the new path/trailing-arg forms) and asserts the message markers
    /// directly below. `journey_reserved` in `cli-uxr` additionally proves the
    /// end-to-end message + exit code for the arg-free forms it covers.
    #[test]
    fn test_reserved_verbs_parse_with_optional_args() {
        use clap::Parser;

        // All of these must parse successfully — the core regression being guarded.
        // `document blame` requires a path, so only the path-bearing form
        // parses; the still-reserved space verbs cover the
        // arg-free/optional-arg shapes.
        for argv in [
            vec!["kutl", "document", "blame", "foo.md"],
            vec!["kutl", "space", "config"],
            vec!["kutl", "space", "config", "get"],
            vec!["kutl", "space", "config", "set", "k", "v"],
            vec!["kutl", "space", "delete"],
        ] {
            assert!(
                Cli::try_parse_from(argv.clone()).is_ok(),
                "reserved verb should parse: {argv:?}"
            );
        }

        // The reserved_command error message must contain the marker strings
        // that the cli-uxr journey test asserts on.
        let msg = reserved_command("document blame").to_string();
        assert!(
            msg.contains("not yet built"),
            "reserved_command must say 'not yet built', got: {msg}"
        );
        assert!(
            msg.contains("reserved"),
            "reserved_command must say 'reserved', got: {msg}"
        );
    }

    #[test]
    fn test_update_parses_and_upgrade_alias_is_gone() {
        use clap::Parser;
        assert!(
            Cli::try_parse_from(["kutl", "update"]).is_ok(),
            "update must parse"
        );
        // One verb for one job: the text-only `upgrade` duplicate is gone.
        assert!(
            Cli::try_parse_from(["kutl", "upgrade"]).is_err(),
            "upgrade alias must be rejected"
        );
        assert!(
            UPDATE_HINT.contains("brew upgrade"),
            "hint names the brew self-update path: {UPDATE_HINT}"
        );
    }

    #[test]
    fn test_format_json_selected_on_read_commands() {
        use clap::Parser;

        // Extract the `--format` value each read command parses to.
        fn parse_format(argv: &[&str]) -> OutputFormat {
            let cli = Cli::try_parse_from(argv.to_vec())
                .unwrap_or_else(|e| panic!("should parse {argv:?}: {e}"));
            let status_args: StatusArgs = match cli.command {
                Command::Status(a)
                | Command::Space(SpaceCli {
                    action:
                        SpaceAction::List(a) | SpaceAction::Status(a) | SpaceAction::Participants(a),
                })
                | Command::Daemon(DaemonCli {
                    action: DaemonAction::Status(a),
                })
                | Command::Auth(AuthCli {
                    action: AuthAction::Status(a),
                }) => a,
                _ => panic!("not a read command: {argv:?}"),
            };
            status_args.format
        }

        // `--format json` selects Json on every read command.
        for argv in [
            vec!["kutl", "status", "--format", "json"],
            vec!["kutl", "space", "list", "--format", "json"],
            vec!["kutl", "space", "status", "--format", "json"],
            vec!["kutl", "space", "participants", "--format", "json"],
            vec!["kutl", "daemon", "status", "--format", "json"],
            vec!["kutl", "auth", "status", "--format", "json"],
        ] {
            assert!(
                matches!(parse_format(&argv), OutputFormat::Json),
                "expected Json for {argv:?}"
            );
        }

        // Omitting `--format` defaults to Human.
        for argv in [
            vec!["kutl", "status"],
            vec!["kutl", "space", "list"],
            vec!["kutl", "space", "participants"],
            vec!["kutl", "auth", "status"],
        ] {
            assert!(
                matches!(parse_format(&argv), OutputFormat::Human),
                "expected Human default for {argv:?}"
            );
        }
    }

    /// Fixture roster mirroring a real `list_participants` response: two
    /// named actors (one online over each door, one offline), and one
    /// unnamed but reachable-by-DID actor.
    fn participants_fixture() -> Vec<Participant> {
        vec![
            Participant {
                did: "did:key:zAlice".into(),
                name: Some("boris-demo/alice".into()),
                connection_type: "offline".into(),
            },
            Participant {
                did: "did:key:zRay".into(),
                name: Some("boris-demo/ray".into()),
                connection_type: "mcp".into(),
            },
            Participant {
                did: "did:key:zGhost".into(),
                name: None,
                connection_type: "websocket".into(),
            },
            Participant {
                did: "did:key:zBoris".into(),
                name: Some("boris-demo/boris".into()),
                connection_type: "offline".into(),
            },
        ]
    }

    #[test]
    fn test_render_participants_human_sorts_online_first_then_name() {
        let rendered = render_participants_human(&participants_fixture());
        let lines: Vec<&str> = rendered.lines().collect();

        // Online participants (mcp and websocket both collapse to "online")
        // sort before offline ones; within each group, alphabetically by name.
        assert_eq!(
            lines,
            vec![
                "(unnamed)  online",
                "boris-demo/ray  online",
                "boris-demo/alice  offline",
                "boris-demo/boris  offline",
            ],
            "rendered:\n{rendered}"
        );
    }

    #[test]
    fn test_render_participants_human_unnamed_has_no_did() {
        let rendered = render_participants_human(&participants_fixture());
        assert!(
            !rendered.contains("did:key:zGhost"),
            "human output must never show a DID: {rendered}"
        );
        assert!(
            rendered.contains("(unnamed)  online"),
            "an unnamed participant falls back to '(unnamed)': {rendered}"
        );
    }

    #[test]
    fn test_render_participants_human_empty_roster() {
        assert_eq!(render_participants_human(&[]), "no participants\n");
    }

    #[test]
    fn test_render_participants_json_preserves_did_name_connection_type() {
        let json = render_participants_json(&participants_fixture()).expect("valid json");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parses as json");
        let entries = value.as_array().expect("top-level array");
        assert_eq!(entries.len(), 4);

        let ray = entries
            .iter()
            .find(|e| e["did"] == "did:key:zRay")
            .expect("ray is present");
        assert_eq!(ray["name"], "boris-demo/ray");
        assert_eq!(
            ray["connection_type"], "mcp",
            "connection_type stays uncollapsed in json"
        );

        let ghost = entries
            .iter()
            .find(|e| e["did"] == "did:key:zGhost")
            .expect("ghost is present");
        assert_eq!(
            ghost["name"],
            serde_json::Value::Null,
            "an unnamed participant's DID is still present in json"
        );
    }

    #[test]
    fn test_render_participants_json_empty_roster_is_empty_array() {
        let json = render_participants_json(&[]).expect("valid json");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parses as json");
        assert_eq!(value, serde_json::json!([]));
    }

    #[test]
    fn test_typo_suggests_but_removed_errors_plainly() {
        use clap::Parser;

        // `Cli` doesn't derive `Debug`, so `unwrap_err()` is unavailable —
        // pull the clap error out via `err()` instead.
        let parse_err = |argv: &[&str]| -> String {
            Cli::try_parse_from(argv.to_vec())
                .err()
                .expect("removed/typo command should fail to parse")
                .to_string()
        };

        // A near-miss of a real command triggers clap's built-in suggestion.
        // clap 4.6 emits: "unrecognized subcommand 'statuss'" plus a
        // "tip: a similar subcommand exists: 'status'" line.
        let msg = parse_err(&["kutl", "statuss"]);
        assert!(
            msg.contains("status"),
            "typo should suggest `status`; got: {msg}"
        );
        assert!(
            msg.contains("similar subcommand exists"),
            "typo should surface clap's built-in suggestion; got: {msg}"
        );

        // A removed bare command is just unrecognized — no compat table, no
        // did-you-mean hint. `log` is not a near-miss of any
        // remaining command, so clap offers no suggestion: a clean break.
        let lower = parse_err(&["kutl", "log", "a.dt"]).to_lowercase();
        assert!(
            lower.contains("unrecognized") || lower.contains("subcommand"),
            "removed command should error as unrecognized; got: {lower}"
        );
        assert!(
            !lower.contains("similar subcommand exists"),
            "removed command must not carry a compat/suggestion hint; got: {lower}"
        );
    }

    #[test]
    fn test_install_crypto_provider_sets_process_default() {
        // Regression guard for the rustls 0.23 "could not automatically
        // determine the process-level CryptoProvider" panic that hit the first
        // wss:// relay connection (the tokio-tungstenite path in `kutl join`).
        // Installing a provider at startup is what keeps join/sync from
        // panicking on TLS.
        install_crypto_provider();
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_some(),
            "a process-default rustls CryptoProvider must be installed after init"
        );
    }

    #[test]
    fn test_device_credentials_normalizes_relay_url_to_ws() {
        // The device-flow endpoint returns an http(s) base; the stored relay_url
        // must be a ws(s) URL so the daemon's scheme-equality token-reuse gate
        // matches.
        let resp = DeviceTokenResponse {
            token: "kutl_abc".into(),
            account_id: "acct-1".into(),
            display_name: "Alice".into(),
            relay_url: "https://relay.example.com".into(),
        };
        let creds = device_credentials_from_token(resp);
        assert!(
            creds.relay_url.starts_with("ws://") || creds.relay_url.starts_with("wss://"),
            "relay_url should have a ws(s) scheme, got: {}",
            creds.relay_url
        );
    }

    #[test]
    fn test_device_credentials_relay_url_matches_kutlhub_connect_url() {
        // The daemon reuses a stored device-flow token only when creds.relay_url
        // byte-equals the connect URL (DEFAULT_KUTLHUB_RELAY_URL). The gateway
        // rewrites the device-token relay_url to that front-door ws URL, so the
        // stored credential must round-trip to exactly it — otherwise every
        // kutlhub login silently falls back to DID auth. Pin the invariant.
        let resp = DeviceTokenResponse {
            token: "kutl_abc".into(),
            account_id: "acct-1".into(),
            display_name: "Alice".into(),
            relay_url: kutl_client::DEFAULT_KUTLHUB_RELAY_URL.to_owned(),
        };
        let creds = device_credentials_from_token(resp);
        assert_eq!(creds.relay_url, kutl_client::DEFAULT_KUTLHUB_RELAY_URL);
    }

    #[test]
    fn test_parse_invite_url_extracts_code_and_origin() {
        let (code, origin) = parse_invite_url("https://relay.example.com/join/abc123").unwrap();
        assert_eq!(code, "abc123");
        assert_eq!(origin, "https://relay.example.com");
    }

    #[test]
    fn test_parse_invite_url_tolerates_trailing_slash() {
        // Browsers and copy buttons routinely append a trailing slash; the code
        // is still the last NON-empty segment.
        let (code, _) = parse_invite_url("https://relay.example.com/join/abc123/").unwrap();
        assert_eq!(code, "abc123");
    }

    #[test]
    fn test_parse_invite_url_preserves_port() {
        let (code, origin) = parse_invite_url("http://127.0.0.1:9100/join/xyz").unwrap();
        assert_eq!(code, "xyz");
        assert_eq!(origin, "http://127.0.0.1:9100");
    }

    #[test]
    fn test_parse_invite_url_rejects_missing_code() {
        assert!(parse_invite_url("https://relay.example.com/").is_err());
    }

    #[test]
    fn test_parse_join_target_invite_url_https() {
        assert!(matches!(
            parse_join_target("https://example.com/invites/abc123"),
            JoinTarget::InviteUrl(_)
        ));
    }

    #[test]
    fn test_parse_join_target_invite_url_http() {
        assert!(matches!(
            parse_join_target("http://localhost:8080/invites/xyz"),
            JoinTarget::InviteUrl(_)
        ));
    }

    #[test]
    fn test_parse_join_target_owner_slug() {
        assert!(matches!(
            parse_join_target("alice/my-project"),
            JoinTarget::OwnerSlug(_)
        ));
    }

    #[test]
    fn test_parse_join_target_owner_slug_with_nested_slash() {
        // Only the presence of '/' matters for classification.
        assert!(matches!(
            parse_join_target("alice/deep/path"),
            JoinTarget::OwnerSlug(_)
        ));
    }

    #[test]
    fn test_parse_join_target_bare_name() {
        assert!(matches!(
            parse_join_target("my-space"),
            JoinTarget::BareName(_)
        ));
    }

    #[test]
    fn test_parse_join_target_bare_name_no_slash() {
        assert!(matches!(
            parse_join_target("projectname"),
            JoinTarget::BareName(_)
        ));
    }

    mod has_any_space_under_tests {
        use super::*;
        use tempfile::TempDir;

        #[test]
        fn returns_true_when_kutlspace_at_root() {
            let dir = TempDir::new().unwrap();
            std::fs::write(dir.path().join(".kutlspace"), "").unwrap();
            assert!(has_any_space_under(dir.path()));
        }

        #[test]
        fn returns_true_when_kutlspace_in_immediate_subfolder() {
            let dir = TempDir::new().unwrap();
            let sub = dir.path().join("my-space");
            std::fs::create_dir(&sub).unwrap();
            std::fs::write(sub.join(".kutlspace"), "").unwrap();
            assert!(has_any_space_under(dir.path()));
        }

        #[test]
        fn returns_false_when_empty() {
            let dir = TempDir::new().unwrap();
            assert!(!has_any_space_under(dir.path()));
        }

        #[test]
        fn returns_false_when_kutlspace_two_levels_deep() {
            // `.kutlspace` belongs at most one level under the anchor. A
            // `.kutlspace` nested deeper belongs to some other project's
            // working tree and must not count.
            let dir = TempDir::new().unwrap();
            let nested = dir.path().join("vendor").join("inner");
            std::fs::create_dir_all(&nested).unwrap();
            std::fs::write(nested.join(".kutlspace"), "").unwrap();
            assert!(!has_any_space_under(dir.path()));
        }

        #[test]
        fn returns_false_when_path_does_not_exist() {
            let dir = TempDir::new().unwrap();
            let missing = dir.path().join("does-not-exist");
            assert!(!has_any_space_under(&missing));
        }
    }

    mod cmd_space_apply_bail_tests {
        use super::*;
        use tempfile::TempDir;

        #[test]
        fn apply_outside_git_with_no_space_bails() {
            // Outside any git repo, `kutl space apply` against a directory
            // with no .kutlspace must bail rather than fall through to
            // anything else — apply is refresh-only.
            let dir = TempDir::new().unwrap();
            let err = cmd_space_apply(Some(dir.path().to_path_buf()))
                .expect_err("expected bail when apply has no space");
            let msg = format!("{err:#}");
            assert!(
                msg.contains("no kutl space found"),
                "unexpected error message: {msg}",
            );
            assert!(
                msg.contains("kutl init") || msg.contains("kutl join"),
                "error should suggest init or join: {msg}",
            );
        }
    }

    mod save_space_config_tests {
        use super::*;
        use serial_test::serial;
        use tempfile::TempDir;

        #[test]
        #[serial]
        fn test_save_space_config_creates_all_files() {
            // Isolate KUTL_HOME so SpaceRegistry::update and signal_reload don't
            // touch the developer's real state.
            let home = TempDir::new().unwrap();
            // SAFETY: #[serial] ensures no other test mutates env vars in parallel.
            unsafe {
                std::env::set_var("KUTL_HOME", home.path());
            }

            let dir = TempDir::new().unwrap();
            let space_root = save_space_config(
                "abc123",
                "calm-eagle-0f1a",
                "ws://localhost:9100/ws",
                Some(dir.path().to_path_buf()),
            )
            .unwrap();

            // .kutl/space.toml
            assert!(kutl_client::SpaceConfig::path(&space_root).exists());

            // .kutlspace (has space_name)
            let ks = kutl_client::KutlspaceConfig::load(&space_root)
                .unwrap()
                .unwrap();
            assert_eq!(ks.space_name, "calm-eagle-0f1a");
            assert!(ks.surface.is_none());

            // .gitignore should match GITIGNORE_CONTENTS exactly.
            let gi = std::fs::read_to_string(space_root.join(".gitignore")).unwrap();
            assert_eq!(gi, kutl_client::space_gitignore::GITIGNORE_CONTENTS);
        }

        #[test]
        #[serial]
        fn test_save_space_config_in_subfolder_inside_git_repo() {
            // Isolate KUTL_HOME so SpaceRegistry::update doesn't touch real state.
            let home = TempDir::new().unwrap();
            // SAFETY: #[serial] ensures no other test mutates env vars in parallel.
            unsafe {
                std::env::set_var("KUTL_HOME", home.path());
            }

            // Set up a git repo with a kutl subfolder.
            let dir = TempDir::new().unwrap();
            std::fs::create_dir(dir.path().join(".git")).unwrap();
            let subfolder = dir.path().join("kutl");

            // Use save_space_config directly with the explicit subfolder path,
            // simulating what cmd_init does after resolving the subfolder.
            let space_root = save_space_config(
                "abc123",
                "calm-eagle-0f1a",
                "ws://localhost:9100/ws",
                Some(subfolder.clone()),
            )
            .unwrap();

            let canonical_dir = dir.path().canonicalize().unwrap();
            assert!(space_root.starts_with(&canonical_dir));
            assert!(space_root.join(".kutlspace").exists());
            assert!(space_root.join(".gitignore").exists());
            assert!(kutl_client::SpaceConfig::path(&space_root).exists());
        }

        #[test]
        fn test_resolve_join_directory_explicit_dir_wins() {
            let dir = TempDir::new().unwrap();
            // When --dir is provided, it always wins, regardless of git or .kutlspace.
            let result = resolve_join_directory(Some(dir.path().to_path_buf()), None).unwrap();
            assert_eq!(result, Some(dir.path().to_path_buf()));
        }

        #[test]
        #[serial]
        fn test_cmd_surface_copies_files() {
            // Isolate KUTL_HOME so SpaceRegistry::update doesn't touch real state.
            let home = TempDir::new().unwrap();
            // SAFETY: #[serial] ensures no other test mutates env vars in parallel.
            unsafe {
                std::env::set_var("KUTL_HOME", home.path());
            }

            // The space lives INSIDE the declared KUTL_HOME: a set KUTL_HOME
            // is a hard resolution boundary and cmd_surface's cwd-first
            // resolution refuses spaces outside it.
            let parent = home.path().join("work");
            std::fs::create_dir(&parent).unwrap();
            let space = parent.join("kutl");
            std::fs::create_dir(&space).unwrap();

            // Initialize the space using save_space_config.
            save_space_config(
                "abc123",
                "test-space",
                "ws://localhost:9100/ws",
                Some(space.clone()),
            )
            .unwrap();

            // Add a [surface] section to .kutlspace by re-saving with surface set.
            let updated = kutl_client::KutlspaceConfig {
                space_name: "test-space".into(),
                surface: Some(kutl_client::SurfaceConfig {
                    target: "../".into(),
                }),
            };
            updated.save(&space).unwrap();

            // Add a document.
            std::fs::create_dir_all(space.join("specs")).unwrap();
            std::fs::write(space.join("specs/foo.md"), "hello").unwrap();

            // Surface it.
            cmd_surface(Some(space.clone())).unwrap();

            // Check that the file landed in the parent with the surface sentinel.
            let out = std::fs::read_to_string(parent.join("specs/foo.md")).unwrap();
            assert!(
                out.starts_with(kutl_client::surface::SURFACE_SENTINEL_HEADER),
                "sentinel missing from surfaced file: {out}"
            );
            assert!(out.contains("hello"));
            // The .kutlspace must NOT have been copied.
            assert!(!parent.join(".kutlspace").exists());
        }

        #[test]
        fn test_config_get_and_list_read_side() {
            // Build an identity with both keys set and confirm the read-side
            // helpers report them — symmetric with what `config set` writes.
            let id = identity::Identity {
                did: "did:key:zTest".into(),
                private_key: "unused".into(),
                created_at: "2026-01-01T00:00:00Z".into(),
                display_name: Some("Ada Lovelace".into()),
                email: Some("ada@example.com".into()),
            };

            // `config get name` returns the display name.
            assert_eq!(
                config_value(&id, "name").unwrap(),
                Some("Ada Lovelace".to_owned())
            );
            // `config get email` returns the email.
            assert_eq!(
                config_value(&id, "email").unwrap(),
                Some("ada@example.com".to_owned())
            );
            // An unknown key errors with context, not a bare failure.
            let err = config_value(&id, "nope").unwrap_err().to_string();
            assert!(
                err.contains("unknown config key") && err.contains("nope"),
                "unexpected error: {err}"
            );

            // `config list` / `config get` (no key) show every set key.
            let listed = render_config_list(&id);
            assert!(listed.contains("name = Ada Lovelace"), "got:\n{listed}");
            assert!(listed.contains("email = ada@example.com"), "got:\n{listed}");
        }

        #[test]
        fn test_config_list_reports_unset_keys() {
            // A fresh identity has no name/email set; both `get` and `list`
            // must report that clearly rather than printing blank lines.
            let id = identity::Identity {
                did: "did:key:zTest".into(),
                private_key: "unused".into(),
                created_at: "2026-01-01T00:00:00Z".into(),
                display_name: None,
                email: None,
            };
            assert_eq!(config_value(&id, "name").unwrap(), None);
            let listed = render_config_list(&id);
            assert!(listed.contains("no config values set"), "got:\n{listed}");
        }

        #[test]
        #[serial]
        fn test_cmd_surface_errors_when_surface_section_missing() {
            let home = TempDir::new().unwrap();
            // SAFETY: #[serial] ensures no other test mutates env vars in parallel.
            unsafe {
                std::env::set_var("KUTL_HOME", home.path());
            }

            // Inside the declared KUTL_HOME (the hard resolution boundary).
            let parent = home.path().join("work");
            std::fs::create_dir(&parent).unwrap();
            let space = parent.join("kutl");
            std::fs::create_dir(&space).unwrap();

            // Initialize a space without a [surface] section.
            save_space_config(
                "abc123",
                "test-space",
                "ws://localhost:9100/ws",
                Some(space.clone()),
            )
            .unwrap();

            let err = cmd_surface(Some(space.clone())).unwrap_err().to_string();
            assert!(
                err.contains("no [surface] target configured"),
                "expected missing-surface-section error, got: {err}"
            );
        }
    }

    mod blame_tests {
        use super::*;
        use kutl_core::{Boundary, Document};

        /// `blame_with_text` on a two-agent document serializes to the stable
        /// `--format json` array shape: one `{line, author, text}` object per
        /// line, attributed to the first character's author DID.
        #[test]
        fn test_blame_json_shape() {
            let mut doc = Document::new();
            let alice = doc.register_agent("alice").unwrap();
            let bob = doc.register_agent("bob").unwrap();
            doc.edit(alice, "did:alice", "line 1", Boundary::Explicit, |ctx| {
                ctx.insert(0, "ab\n")
            })
            .unwrap();
            doc.edit(bob, "did:bob", "line 2", Boundary::Explicit, |ctx| {
                ctx.insert(3, "cd")
            })
            .unwrap();
            assert_eq!(doc.content(), "ab\ncd");

            let rows: Vec<BlameRow> = doc
                .blame_with_text()
                .into_iter()
                .map(|(line, author, text)| BlameRow { line, author, text })
                .collect();
            let json = serde_json::to_string(&rows).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

            let expected = serde_json::json!([
                { "line": 1, "author": "did:alice", "text": "ab" },
                { "line": 2, "author": "did:bob", "text": "cd" },
            ]);
            assert_eq!(parsed, expected);
        }
    }

    mod restore_tests {
        use super::*;
        use kutl_core::{Boundary, Document, new_change};
        use tempfile::TempDir;

        /// Build three synthetic changes with distinct ids and timestamps. Only
        /// `id` and `timestamp` matter for `select_restore_point`; the version
        /// spans are placeholders.
        fn sample_changes() -> Vec<Change> {
            vec![
                new_change(
                    "a",
                    "first",
                    Boundary::Explicit,
                    &[],
                    &[0],
                    1_000,
                    "c1".into(),
                ),
                new_change(
                    "a",
                    "second",
                    Boundary::Explicit,
                    &[0],
                    &[1],
                    2_000,
                    "c2".into(),
                ),
                new_change(
                    "a",
                    "third",
                    Boundary::Explicit,
                    &[1],
                    &[2],
                    3_000,
                    "c3".into(),
                ),
            ]
        }

        #[test]
        fn test_select_to_exact_id() {
            let changes = sample_changes();
            let c = select_restore_point(&changes, None, Some("c2")).unwrap();
            assert_eq!(c.id, "c2");
        }

        #[test]
        fn test_select_to_bad_id_errors() {
            let changes = sample_changes();
            let err =
                select_restore_point(&changes, None, Some("nope")).expect_err("bad id must error");
            let msg = err.to_string();
            assert!(msg.contains("no change with id nope"), "got: {msg}");
            assert!(
                msg.contains("kutl document log"),
                "should point at log: {msg}"
            );
        }

        #[test]
        fn test_select_at_picks_newest_at_or_before() {
            let changes = sample_changes();
            // 2500ms is after c2 (2000) but before c3 (3000) → newest ≤ T is c2.
            let between = "1970-01-01T00:00:02.500Z"; // 2500ms since epoch
            let c = select_restore_point(&changes, Some(between), None).unwrap();
            assert_eq!(c.id, "c2", "newest change at or before 2500ms is c2");
        }

        #[test]
        fn test_select_at_exact_boundary_includes_that_change() {
            let changes = sample_changes();
            // Exactly c2's timestamp (2000ms) → c2 is included (<=).
            let at = "1970-01-01T00:00:02.000Z";
            let c = select_restore_point(&changes, Some(at), None).unwrap();
            assert_eq!(c.id, "c2");
        }

        #[test]
        fn test_select_at_before_all_errors() {
            let changes = sample_changes();
            // Before the first change (1000ms) → nothing at or before.
            let at = "1970-01-01T00:00:00.500Z";
            let err =
                select_restore_point(&changes, Some(at), None).expect_err("no match must error");
            assert!(
                err.to_string().contains("no change at or before"),
                "got: {err}"
            );
        }

        #[test]
        fn test_select_both_errors() {
            let changes = sample_changes();
            let err = select_restore_point(&changes, Some("2026-01-01"), Some("c1"))
                .expect_err("both selectors must error");
            assert!(
                err.to_string().contains("only one of --at or --to"),
                "got: {err}"
            );
        }

        #[test]
        fn test_select_neither_errors() {
            let changes = sample_changes();
            let err =
                select_restore_point(&changes, None, None).expect_err("no selector must error");
            let msg = err.to_string();
            assert!(
                msg.contains("--at") && msg.contains("--to"),
                "hint both flags: {msg}"
            );
        }

        #[test]
        fn test_parse_at_rfc3339_utc() {
            // 2026-07-18T15:00:00Z → its Unix-millis, checked by round-tripping
            // through jiff (no hand-computed epoch constant).
            use std::str::FromStr as _;
            let expected = Timestamp::from_str("2026-07-18T15:00:00Z")
                .unwrap()
                .as_millisecond();
            assert_eq!(
                parse_at_time_to_millis("2026-07-18T15:00:00Z").unwrap(),
                expected
            );
        }

        #[test]
        fn test_parse_at_rfc3339_with_offset() {
            use std::str::FromStr as _;
            let expected = Timestamp::from_str("2026-07-18T15:00:00-04:00")
                .unwrap()
                .as_millisecond();
            assert_eq!(
                parse_at_time_to_millis("2026-07-18T15:00:00-04:00").unwrap(),
                expected
            );
        }

        #[test]
        fn test_parse_at_civil_datetime_in_system_zone() {
            use std::str::FromStr as _;
            // A civil datetime (no offset) resolves in the system zone — assert
            // via the same jiff path so the test is zone-independent.
            let expected = jiff::civil::DateTime::from_str("2026-07-18T15:00")
                .unwrap()
                .to_zoned(jiff::tz::TimeZone::system())
                .unwrap()
                .timestamp()
                .as_millisecond();
            assert_eq!(
                parse_at_time_to_millis("2026-07-18T15:00").unwrap(),
                expected
            );
        }

        #[test]
        fn test_parse_at_civil_date_in_system_zone() {
            use std::str::FromStr as _;
            let expected = jiff::civil::Date::from_str("2026-07-18")
                .unwrap()
                .to_zoned(jiff::tz::TimeZone::system())
                .unwrap()
                .timestamp()
                .as_millisecond();
            assert_eq!(parse_at_time_to_millis("2026-07-18").unwrap(), expected);
        }

        #[test]
        fn test_parse_at_garbage_errors() {
            let err = parse_at_time_to_millis("yesterday 3pm").expect_err("garbage must error");
            assert!(err.to_string().contains("could not parse"), "got: {err}");
        }

        /// Author a 3-edit text document to disk, load it back, and assert
        /// `reconstruct_from_doc` reproduces the historical content at each
        /// selected change — the full timestamp/id → frontier → content path.
        #[test]
        fn test_reconstruct_round_trip() {
            let dir = TempDir::new().unwrap();
            let dt_path = dir.path().join("doc.dt");

            let mut doc = Document::new();
            let agent = doc.register_agent("t").unwrap();
            doc.edit(agent, "t", "first", Boundary::Explicit, |ctx| {
                ctx.insert(0, "hello")
            })
            .unwrap();
            doc.edit(agent, "t", "second", Boundary::Explicit, |ctx| {
                ctx.insert(5, " world")
            })
            .unwrap();
            doc.edit(agent, "t", "third", Boundary::Explicit, |ctx| {
                ctx.delete(0..5)
            })
            .unwrap();
            doc.save(&dt_path).unwrap();

            // Read the real ids the author env minted for each change.
            let (id0, id1, id2) = {
                let c = doc.changes();
                (c[0].id.clone(), c[1].id.clone(), c[2].id.clone())
            };

            let loaded = Document::load(&dt_path).unwrap();
            assert_eq!(loaded.content(), " world", "tip content after 3 edits");

            // Restore to the first change → "hello".
            let r0 = reconstruct_from_doc(&loaded, None, Some(&id0)).unwrap();
            assert_eq!(r0.content, "hello", "content as of first change");
            assert!(
                r0.label.contains(&id0),
                "label names the change: {}",
                r0.label
            );

            // Restore to the second change → "hello world".
            let r1 = reconstruct_from_doc(&loaded, None, Some(&id1)).unwrap();
            assert_eq!(r1.content, "hello world", "content as of second change");

            // Restore to the third (tip) change → " world".
            let r2 = reconstruct_from_doc(&loaded, None, Some(&id2)).unwrap();
            assert_eq!(r2.content, " world", "content as of third change");
        }

        /// A `.dt` positional is rejected by the shared document resolver with
        /// the uniform pointer at the working-tree path (before any IO).
        #[test]
        fn test_dt_positional_rejected() {
            let args = RestoreArgs {
                path: PathBuf::from("something.dt"),
                at: None,
                to: Some("c1".into()),
            };
            let err = cmd_restore(&args).expect_err(".dt path must be rejected");
            let msg = format!("{err:#}");
            assert!(
                msg.contains("internal storage") && msg.contains("working-tree path"),
                "got: {msg}"
            );
        }

        /// A restore that grows the change history by one entry (a forward
        /// edit), not a truncation, is restore's core safety property. Model it
        /// locally: reconstruct old content, then re-apply it via
        /// `replace_content` and assert `changes()` GREW rather than shrank.
        #[test]
        fn test_restore_is_forward_edit_not_truncation() {
            let mut doc = Document::new();
            let agent = doc.register_agent("t").unwrap();
            doc.edit(agent, "t", "first", Boundary::Explicit, |ctx| {
                ctx.insert(0, "hello")
            })
            .unwrap();
            doc.edit(agent, "t", "second", Boundary::Explicit, |ctx| {
                ctx.insert(5, " world")
            })
            .unwrap();
            let before = doc.changes().len();
            let first_id = doc.changes()[0].id.clone();

            // Reconstruct the as-of-first content and re-assert it as a forward
            // edit (what the daemon's watcher does to the rewritten file).
            let restored = reconstruct_from_doc(&doc, None, Some(&first_id)).unwrap();
            assert_eq!(restored.content, "hello");
            doc.replace_content(agent, "t", "restore", Boundary::Auto, &restored.content)
                .unwrap();

            assert_eq!(doc.content(), "hello", "tip reflects the restored content");
            assert!(
                doc.changes().len() > before,
                "restore appends a forward edit (grew {} -> {}), never truncates",
                before,
                doc.changes().len()
            );
        }

        /// REGRESSION: restoring to a change authored by a *remote* peer must
        /// reconstruct that change's content, not garbage.
        ///
        /// A change's `version_span.end` is a frontier of the AUTHORING
        /// replica's LOCAL times. Once merged into a peer, those indices name
        /// different ops on the peer, so a naive `content_at(&span_end(change))`
        /// checks out at a foreign frontier and reconstructs the wrong
        /// content. The portable `author_tip` resolves back to the peer's local
        /// time, so `content_at_change` (which restore uses) cuts correctly.
        #[test]
        fn test_reconstruct_remote_authored_change() {
            // Doc A authors "hello".
            let mut a = Document::new();
            let a_agent = a.register_agent("a").unwrap();
            a.edit(a_agent, "did:a", "seed", Boundary::Explicit, |ctx| {
                ctx.insert(0, "hello")
            })
            .unwrap();
            let a_change_id = a.changes().last().unwrap().id.clone();
            let a_ops = a.encode_since(&[]);
            let a_changes = a.changes_since(&[]);

            // Doc B authors its own "world", THEN merges A's change — so B's
            // local time indices differ from A's (B has its own ops at low
            // indices, A's change lands at higher local times after the merge).
            let mut b = Document::new();
            let b_agent = b.register_agent("b").unwrap();
            b.edit(b_agent, "did:b", "own", Boundary::Explicit, |ctx| {
                ctx.insert(0, "world")
            })
            .unwrap();
            b.merge(&a_ops, &a_changes).unwrap();

            // The merged change carries A's portable author tip.
            let merged = b
                .changes()
                .iter()
                .find(|c| c.id == a_change_id)
                .expect("A's change merged into B");
            assert_eq!(
                merged.author_tip.as_ref().map(|t| t.agent.as_str()),
                Some("a"),
                "merged change pins A's portable author tip"
            );

            // Restoring B to A's change reconstructs A's content ("hello"),
            // NOT whatever B's local span-end indices would have pointed at.
            let restored = reconstruct_from_doc(&b, None, Some(&a_change_id)).unwrap();
            assert_eq!(
                restored.content, "hello",
                "restore to a remote-authored change reconstructs its content via the portable tip"
            );
        }
    }
}
