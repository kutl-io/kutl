//! kutl CLI — collaborative text synchronization tool.

mod agents_md;
mod daemon_mgmt;
mod dirs;
mod identity;
mod space;
mod status;
mod supervisor;
mod watch;
mod watch_tools;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use jiff::Timestamp;

use kutl_core::{Boundary, Document};

#[derive(Parser)]
#[command(name = "kutl", version, about = "kutl — collaborative text sync tool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show the change history of a document.
    Log {
        /// Path to the .dt document file.
        path: PathBuf,
    },
    /// Initialize a new kutl space in a directory.
    Init {
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
        /// Defaults to "kutl" with an interactive prompt to override.
        #[arg(long)]
        subfolder: Option<String>,
        /// Re-render the kutl-managed section of AGENTS.md in place, even
        /// when one already exists. Content outside the markers is
        /// preserved.
        #[arg(long)]
        update: bool,
    },
    /// Sync once: push local changes and pull remote changes, then exit.
    Sync {
        /// Target directory (defaults to current directory).
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    /// Daemon management commands.
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
    /// View or update identity configuration.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Authenticate with a kutl relay.
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
    /// Join an existing space.
    ///
    /// Accepts three forms:
    ///   - A full invite URL (`https://...`) — relay extracted from the URL
    ///   - An owner/slug identifier (`alice/my-project`) — connects via kutlhub
    ///   - A bare name — resolves via the OSS relay
    ///
    /// If invoked inside a folder containing `.kutlspace`, the `space_name`
    /// from that file is used and the argument may be omitted.
    Join {
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
    },
    /// Watch a space for signals. Serves as a Claude Code channel
    /// and MCP tool server over stdio.
    Watch {
        /// Disable push notifications (SSE + WS listener). Agents discover
        /// changes only via `get_changes` polling.
        #[arg(long)]
        poll_only: bool,
    },
    /// Copy this space's documents into the parent directory (or configured
    /// surface target). Per RFD 0060, kutl is the source of truth for docs;
    /// surface always overwrites the target.
    Surface {
        /// Target directory (defaults to current directory's space root).
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    /// Show client diagnostic status: daemon, registered spaces, relays,
    /// identity. Per RFD 0074. Works whether the daemon is running or not.
    Status {
        /// Output format.
        #[arg(long, value_enum, default_value_t = StatusFormat::default())]
        format: StatusFormat,
    },
}

/// Output format selector for `kutl status`.
#[derive(clap::ValueEnum, Clone, Copy, Debug, Default)]
enum StatusFormat {
    /// Human-readable summary (default).
    #[default]
    Human,
    /// Machine-readable JSON (stable schema).
    Json,
}

#[derive(Subcommand)]
enum DaemonAction {
    /// Run the daemon in the foreground (watches all registered spaces).
    Run,
    /// Start the daemon as a background process.
    Start,
    /// Stop the running daemon.
    Stop,
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
    /// Show current identity configuration.
    Show,
}

#[derive(Subcommand)]
enum AuthAction {
    /// Log in to a kutl relay.
    Login {
        /// Personal Access Token (skip browser auth).
        #[arg(long)]
        token: Option<String>,
        /// Relay URL (default: read from auth.json or use default).
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Log { path } => cmd_log(&path),
        Command::Init {
            relay,
            name,
            dir,
            subfolder,
            update,
        } => cmd_init(&relay, name.as_deref(), dir, subfolder.as_deref(), update).await,
        Command::Sync { dir } => cmd_sync(dir).await,
        Command::Daemon { action } => cmd_daemon(action).await,
        Command::Config { action } => cmd_config(action),
        Command::Auth { action } => cmd_auth(action).await,
        Command::Join {
            target,
            relay,
            dir,
            subfolder,
        } => {
            cmd_join(
                target.as_deref(),
                relay.as_deref(),
                dir,
                subfolder.as_deref(),
            )
            .await
        }
        Command::Watch { poll_only } => cmd_watch(poll_only).await,
        Command::Surface { dir } => cmd_surface(dir),
        Command::Status { format } => cmd_status(format).await,
    }
}

/// Save a space configuration and register it globally.
///
/// Uses `dir` if provided, otherwise falls back to the current directory.
/// Returns the space root path. Fails if `.kutl/space.json` already exists.
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
        space_root.join(".kutl/space.json").display()
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
    let dotdir = space_root.join(".kutl");
    if dotdir.join("space.json").exists() {
        let existing = space::SpaceConfig::load(&space_root)?;
        println!("Space already initialized in this directory.");
        println!("  space_id:   {}", existing.space_id);
        println!("  relay:      {}", existing.relay_url);
        if let Some(ks) = kutl_client::KutlspaceConfig::load(&space_root)? {
            println!("  space_name: {}", ks.space_name);
        }
        anyhow::bail!("remove .kutl/space.json first to reinitialize");
    }

    // Write the team-wide .kutlspace and the kutl-managed .gitignore FIRST.
    // Both are git-tracked and not synced via the relay (RFD 0060). The
    // sentinel file `.kutl/space.json` is written last so partial failures
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

    let path_str = space_root.display().to_string();
    space::SpaceRegistry::update(|registry| {
        registry.add(&path_str);
    })?;

    // Notify the running daemon (if any) to pick up the new space.
    if let Err(e) = daemon_mgmt::signal_reload() {
        tracing::debug!(error = %e, "could not signal daemon reload");
    }

    Ok(space_root)
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
        "Detected git repo at {}. Per RFD 0060, kutl spaces live in a",
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
/// subdirectories (the default RFD 0060 subfolder layout). Used by the
/// `--update` short-circuit to detect whether an existing space is present
/// without needing to know the exact subfolder name.
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
/// variants so users know to run `kutl init --update`.
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
                    "note: kutl-managed section in {} was generated by v{}; current is v{}; run `kutl init --update` to refresh",
                    path.display(),
                    sentinel,
                    env!("CARGO_PKG_VERSION"),
                );
            }
            Staleness::StaleIncompatible => {
                eprintln!(
                    "warning: kutl-managed section in {} was generated by v{} which is incompatible with the running v{}; run `kutl init --update` to refresh",
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
    update: bool,
) -> Result<()> {
    let initial_root = resolve_dir(dir.clone())?;

    // `--update` is allowed as a refresh-only invocation when the space is
    // already initialized: refresh AGENTS.md and exit. This is the supported
    // path for "the kutl-managed AGENTS.md block is stale, please update".
    //
    // If we are inside a git repo, we check whether any kutl space exists
    // under the repo root (by scanning for `.kutlspace` one level down, the
    // default subfolder layout from RFD 0060, or directly at initial_root).
    // If a space is found, we short-circuit to a refresh-only run. If not,
    // we fall through to the full init path, which will also write AGENTS.md
    // with ForceUpdate at the end.
    if update {
        let agents_anchor = agents_md::anchor_for(&initial_root);
        if has_any_space_under(&agents_anchor) {
            let agents_path = agents_anchor.join("AGENTS.md");
            let outcome = agents_md::apply_block(
                &agents_path,
                env!("CARGO_PKG_VERSION"),
                agents_md::ApplyMode::ForceUpdate,
            )?;
            report_agents_md_outcome(&agents_path, &outcome);
            return Ok(());
        }
        // No existing space found — fall through to the regular init path,
        // which will also call apply_block with ForceUpdate at the end.
    }

    // If we're inside a git repo, route the space into a subfolder so it
    // does not share files with the git working tree (RFD 0060).
    let space_root = if let Some(repo_root) = kutl_client::find_git_repo_root(&initial_root) {
        let chosen = match subfolder {
            Some(s) => s.to_owned(),
            None => prompt_subfolder_name(&repo_root),
        };
        let candidate = repo_root.join(&chosen);
        if candidate.join(".kutlspace").exists() || candidate.join(".kutl/space.json").exists() {
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
    let id = identity::load_or_generate()?;
    let id_path = identity::default_identity_path()?;
    println!("Identity: {}", id.did);
    println!("  stored: {}", id_path.display());

    // Check for existing space.
    let dotdir = space_root.join(".kutl");
    if dotdir.join("space.json").exists() {
        let existing = space::SpaceConfig::load(&space_root)?;
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
    println!("  config:     {}", dotdir.join("space.json").display());

    // RFD 0075 §AGENTS.md: write the kutl-managed section into the repo-root
    // AGENTS.md when inside a git repo, or at the chosen project root otherwise.
    // Non-git installs still get the contract written next to their .kutlspace.
    let agents_anchor = agents_md::anchor_for(&initial_root);
    let agents_path = agents_anchor.join("AGENTS.md");
    let mode = if update {
        agents_md::ApplyMode::ForceUpdate
    } else {
        agents_md::ApplyMode::Default
    };
    let outcome = agents_md::apply_block(&agents_path, env!("CARGO_PKG_VERSION"), mode)?;
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
            let text = r
                .text()
                .await
                .unwrap_or_else(|e| format!("(failed to read response: {e})"));
            Err(RegisterError::Other(text))
        }
        Err(e) => Err(RegisterError::Unreachable(e.to_string())),
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
    let id =
        identity::Identity::load(&path).context("no identity found — run `kutl init` first")?;

    let key_bytes = URL_SAFE_NO_PAD
        .decode(&id.private_key)
        .context("failed to decode private key")?;
    let key_array: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid private key length"))?;
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&key_array);

    Ok((id.did, signing_key, id.display_name))
}

/// Resolve a bearer token, falling back to DID challenge-response.
///
/// Checks the token resolution chain (env var, auth.json) first.
/// If no stored token is found, performs a full DID challenge-response
/// against the relay via `kutl_client::authenticate` and returns the token.
async fn resolve_or_authenticate(relay_url: &str) -> Result<String> {
    let creds_path = kutl_client::credentials::default_credentials_path()?;
    if let Some(token) = kutl_client::credentials::resolve_token(Some(&creds_path)) {
        return Ok(token);
    }
    // Fallback: DID challenge-response.
    let (did, signing_key, _) = load_signing_key()?;
    kutl_client::authenticate(relay_url, &did, &signing_key).await
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
    /// Account ID on kutlhub.
    account_id: String,
    /// Human-readable display name.
    display_name: String,
    /// Relay URL the token is valid for.
    relay_url: String,
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
    /// A `owner/slug` kutlhub namespace identifier.
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

/// Join a space via an invite URL.
///
/// Parses the relay host from the URL, fetches invite metadata via
/// `GET /invites/{code}`, and saves the space config.
async fn join_via_invite_url(
    invite_url: &str,
    relay_override: Option<&str>,
    dir: Option<PathBuf>,
) -> Result<()> {
    if relay_override.is_some() {
        eprintln!("warning: --relay is ignored when joining via an invite URL");
    }

    // Extract the invite code (last non-empty path segment).
    let parsed = reqwest::Url::parse(invite_url)
        .with_context(|| format!("invalid invite URL: {invite_url}"))?;
    let code = parsed
        .path_segments()
        .and_then(|mut seg| seg.next_back().filter(|s| !s.is_empty()))
        .with_context(|| format!("invite URL has no path segment: {invite_url}"))?;

    // Derive the HTTP base URL (scheme + host + optional port).
    let origin = parsed.origin().ascii_serialization();

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

/// Number of parts expected when splitting an `owner/slug` space identifier.
const SPACE_IDENTIFIER_PARTS: usize = 2;

/// Join a kutlhub space by `owner/slug` via WebSocket `ResolveSpace` RPC.
async fn join_via_owner_slug(
    space_arg: &str,
    relay_override: Option<&str>,
    dir: Option<PathBuf>,
) -> Result<()> {
    let relay_url = relay_override.unwrap_or(kutl_client::DEFAULT_KUTLHUB_RELAY_URL);

    let parts: Vec<&str> = space_arg.splitn(SPACE_IDENTIFIER_PARTS, '/').collect();
    if parts.len() != SPACE_IDENTIFIER_PARTS || parts[0].is_empty() || parts[1].is_empty() {
        anyhow::bail!(
            "invalid space identifier '{space_arg}' — expected format: owner/slug (e.g. alice/my-project)"
        );
    }
    let owner = parts[0];
    let slug = parts[1];

    // Resolve auth token.
    let token = resolve_or_authenticate(relay_url).await?;

    // Connect to relay via async WebSocket.
    let mut conn = AsyncRelayConn::connect(relay_url, &token).await?;

    // Send ResolveSpace request.
    let req = kutl_proto::protocol::resolve_space_envelope(owner, slug);
    let resp = conn.request(&req).await?;

    // Extract ResolveSpaceResult.
    let result = match resp.payload {
        Some(kutl_proto::sync::sync_envelope::Payload::ResolveSpaceResult(r)) => r,
        Some(kutl_proto::sync::sync_envelope::Payload::Error(e)) => {
            anyhow::bail!("space not found: {}", e.message);
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
async fn join_via_bare_name(
    name: &str,
    relay_override: Option<&str>,
    dir: Option<PathBuf>,
) -> Result<()> {
    let relay_url = relay_override.unwrap_or(DEFAULT_OSS_RELAY_URL);
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
    let effective_dir = resolve_join_directory(dir, &resolved_target, subfolder)?;

    // Fail loudly on case-variant duplicates before any handler writes config.
    // `effective_dir` is the directory the handlers will write `.kutlspace` into;
    // empty subfolders pass the check trivially.
    let check_target: PathBuf = match effective_dir.as_ref() {
        Some(t) => t.clone(),
        None => std::env::current_dir().context("failed to determine current directory")?,
    };
    if let Err(err) = kutl_daemon::case_collision::detect_case_collisions(&check_target) {
        eprintln!("{}", err.format_user_message());
        anyhow::bail!("join target contains case-variant duplicates");
    }

    // RFD 0075 §AGENTS.md: write the kutl-managed section before the network
    // round-trip so the file is present even when the relay is unreachable.
    // Default mode only — `kutl join` has no `--update` flag. Anchors to the
    // git repo root if inside a git repo, otherwise the chosen project dir.
    let agents_anchor = agents_md::anchor_for(&check_target);
    let agents_path = agents_anchor.join("AGENTS.md");
    let outcome = agents_md::apply_block(
        &agents_path,
        env!("CARGO_PKG_VERSION"),
        agents_md::ApplyMode::Default,
    )?;
    report_agents_md_outcome(&agents_path, &outcome);

    match parse_join_target(&resolved_target) {
        JoinTarget::InviteUrl(url) => join_via_invite_url(&url, relay, effective_dir).await,
        JoinTarget::OwnerSlug(spec) => join_via_owner_slug(&spec, relay, effective_dir).await,
        JoinTarget::BareName(name) => join_via_bare_name(&name, relay, effective_dir).await,
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
    target: &str,
    subfolder: Option<&str>,
) -> Result<Option<PathBuf>> {
    if dir.is_some() {
        return Ok(dir);
    }
    let here = std::env::current_dir().context("failed to determine current directory")?;
    if here.join(".kutlspace").exists() {
        // Already inside a kutl-marked folder; join in place.
        return Ok(Some(here));
    }
    let Some(repo_root) = kutl_client::find_git_repo_root(&here) else {
        return Ok(None);
    };
    let chosen = match subfolder {
        Some(s) => s.to_owned(),
        None => target.to_owned(),
    };
    let candidate = repo_root.join(&chosen);
    if candidate.join(".kutlspace").exists() || candidate.join(".kutl/space.json").exists() {
        anyhow::bail!(
            "space already exists at {} — pick a different --subfolder or `cd` into it first",
            candidate.display()
        );
    }
    std::fs::create_dir_all(&candidate)
        .with_context(|| format!("failed to create {}", candidate.display()))?;
    Ok(Some(candidate.canonicalize()?))
}

/// Implementation of `kutl surface` (RFD 0060).
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
/// reachability, identity). Per RFD 0074.
///
/// Works whether the daemon is running or not — all data is from disk
/// (`$KUTL_HOME`) and direct relay reachability probes; no daemon IPC.
async fn cmd_status(format: StatusFormat) -> Result<()> {
    let kutl_home = kutl_client::kutl_home()?;
    let mut snapshot = status::collect_static(&kutl_home)?;
    status::probe_relays(&mut snapshot.relays).await;

    match format {
        StatusFormat::Json => {
            let out = serde_json::to_string_pretty(&snapshot)?;
            println!("{out}");
        }
        StatusFormat::Human => {
            print!("{}", status::render_human(&snapshot));
        }
    }
    Ok(())
}

async fn cmd_sync(dir: Option<PathBuf>) -> Result<()> {
    kutl_relay::telemetry::init_tracing("cli");

    let space_root = resolve_space_root(dir)?;
    let daemon_config = build_daemon_config(space_root, true)?;
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

/// Resolve and canonicalize a space root from an optional path argument.
fn resolve_space_root(space: Option<PathBuf>) -> Result<PathBuf> {
    let space_root = match space {
        Some(p) => p,
        None => std::env::current_dir().context("failed to determine current directory")?,
    };
    let space_root = space_root
        .canonicalize()
        .with_context(|| format!("invalid space path: {}", space_root.display()))?;

    // Verify space is initialized.
    space::SpaceConfig::load(&space_root)
        .context("space not initialized — run `kutl init` first")?;

    Ok(space_root)
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
    space_root: PathBuf,
    one_shot: bool,
) -> Result<kutl_daemon::SpaceWorkerConfig> {
    let config = space::SpaceConfig::load(&space_root)?;
    let (did, signing_key, display_name) = load_signing_key()?;
    Ok(kutl_daemon::SpaceWorkerConfig {
        space_root,
        author_did: did,
        relay_url: config.relay_url,
        space_id: config.space_id,
        signing_key: Some(signing_key),
        one_shot,
        display_name: display_name.unwrap_or_default(),
        ready: None,
        cancel: tokio_util::sync::CancellationToken::new(),
    })
}

/// Run the global daemon supervisor in the foreground.
async fn cmd_daemon_run() -> Result<()> {
    kutl_relay::telemetry::init_tracing("cli");
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
/// `--relay` defaults to the kutlhub URL (not the local-dev relay). Auth
/// tokens come from kutlhub's web UI (PATs) or kutlhub's OAuth device flow;
/// self-hosted relays use DID auth and don't need this command. Pass
/// `--relay` explicitly to override.
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

            let creds = kutl_client::StoredCredentials {
                token: token_resp.token,
                relay_url: token_resp.relay_url,
                account_id: token_resp.account_id,
                display_name: token_resp.display_name.clone(),
            };
            let path = kutl_client::credentials::default_credentials_path()?;
            creds.save(&path)?;

            println!("Authenticated as {}.", token_resp.display_name);
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

/// Delete stored credentials and log out.
fn cmd_auth_logout() -> Result<()> {
    let path = kutl_client::credentials::default_credentials_path()?;
    if path.exists() {
        kutl_client::credentials::delete_credentials(&path)?;
        println!("Credentials removed from {}.", path.display());
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
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

impl AsyncRelayConn {
    /// Connect, perform handshake with an auth token, and return the connection.
    async fn connect(relay_url: &str, auth_token: &str) -> Result<Self> {
        let (mut ws, _) = tokio::time::timeout(
            WS_CONNECT_TIMEOUT,
            tokio_tungstenite::connect_async(relay_url),
        )
        .await
        .context("relay connection timed out")?
        .context("failed to connect to relay")?;

        let hs = kutl_proto::protocol::handshake_envelope_with_token("kutl", auth_token, "");
        let bytes = kutl_proto::protocol::encode_envelope(&hs);
        ws.send(Message::Binary(bytes.into()))
            .await
            .context("failed to send handshake")?;

        let ack = Self::recv_envelope(&mut ws).await?;
        match ack.payload {
            Some(kutl_proto::sync::sync_envelope::Payload::HandshakeAck(_)) => {}
            Some(kutl_proto::sync::sync_envelope::Payload::Error(e)) => {
                anyhow::bail!("handshake rejected: {}", e.message);
            }
            other => anyhow::bail!("expected HandshakeAck, got {other:?}"),
        }

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
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
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

/// Handle `kutl config`: show or update identity fields.
fn cmd_config(action: ConfigAction) -> Result<()> {
    let path = identity::default_identity_path()?;

    match action {
        ConfigAction::Set { key, value } => {
            let mut id = identity::Identity::load(&path)
                .context("no identity found — run `kutl init` first")?;

            match key.as_str() {
                "name" => id.display_name = Some(value.clone()),
                "email" => id.email = Some(value.clone()),
                _ => anyhow::bail!("unknown config key: {key}"),
            }

            id.save(&path)?;
            println!("Set {key} = {value}");
            Ok(())
        }
        ConfigAction::Show => {
            let id = identity::Identity::load(&path)
                .context("no identity found — run `kutl init` first")?;

            println!("DID:   {}", id.did);
            println!("Name:  {}", id.display_name.as_deref().unwrap_or("not set"));
            println!("Email: {}", id.email.as_deref().unwrap_or("not set"));
            Ok(())
        }
    }
}

/// Print the change history of a `.dt` document file.
fn cmd_log(path: &std::path::Path) -> Result<()> {
    let doc = Document::load(path).with_context(|| format!("failed to load {}", path.display()))?;
    let changes = doc.changes();

    if changes.is_empty() {
        println!("no changes recorded for {}", path.display());
        return Ok(());
    }

    // Print most recent first, like git log.
    for (i, change) in changes.iter().enumerate().rev() {
        if i < changes.len() - 1 {
            println!();
        }

        println!("change {}", change.id);
        println!("Author: {}", change.author_did);
        println!("Date:   {}", format_timestamp(change.timestamp));
        println!(
            "Boundary: {}",
            match Boundary::try_from(change.boundary) {
                Ok(Boundary::Explicit) => "explicit",
                Ok(Boundary::Auto) => "auto",
                _ => "unspecified",
            }
        );

        if let Some(span) = &change.version_span {
            println!("Span:   {:?} → {:?}", span.start, span.end);
        }

        println!();
        println!("    {}", change.intent);
    }

    Ok(())
}

/// Format a Unix-millis timestamp as a human-readable UTC string.
fn format_timestamp(millis: i64) -> String {
    match Timestamp::from_millisecond(millis) {
        Ok(ts) => ts.strftime("%Y-%m-%d %H:%M:%S%.3f UTC").to_string(),
        Err(_) => format!("{millis}ms (invalid timestamp)"),
    }
}

async fn cmd_watch(poll_only: bool) -> Result<()> {
    // RFD 0075 §staleness check: agent-facing commands inspect the
    // kutl-managed AGENTS.md block on startup and warn or refuse based
    // on its sentinel version. The check anchors at the local space's
    // anchor (git root if any, else .kutlspace-parent), not the
    // working-directory's git root, because the daemon serves the
    // local space regardless of where the user invoked watch from.
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
                // condition: `kutl watch` is the agent-facing surface, so
                // a stale block actively affects downstream agent
                // behavior rather than just being a future-PR concern.
                eprintln!(
                    "warning: AGENTS.md kutl block was generated by v{sentinel}; current is v{}; run `kutl init --update` to refresh",
                    env!("CARGO_PKG_VERSION"),
                );
            }
            agents_md::CheckOutcome::StaleIncompatible { sentinel } => {
                agents_md::handle_incompatible(&anchor, &sentinel)?;
            }
            agents_md::CheckOutcome::Absent => {
                agents_md::handle_absent(&anchor)?;
            }
        }
    }
    watch::run(poll_only).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_join_target_invite_url_https() {
        assert!(matches!(
            parse_join_target("https://kutlhub.com/invites/abc123"),
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

            // .kutl/space.json
            assert!(space_root.join(".kutl/space.json").exists());

            // .kutlspace (has space_name)
            let ks = kutl_client::KutlspaceConfig::load(&space_root)
                .unwrap()
                .unwrap();
            assert_eq!(ks.space_name, "calm-eagle-0f1a");
            assert!(ks.surface.is_none());

            // .gitignore should match GITIGNORE_CONTENTS exactly.
            let gi = std::fs::read_to_string(space_root.join(".gitignore")).unwrap();
            assert_eq!(gi, kutl_client::GITIGNORE_CONTENTS);
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
            assert!(space_root.join(".kutl/space.json").exists());
        }

        #[test]
        fn test_resolve_join_directory_explicit_dir_wins() {
            let dir = TempDir::new().unwrap();
            // When --dir is provided, it always wins, regardless of git or .kutlspace.
            let result =
                resolve_join_directory(Some(dir.path().to_path_buf()), "some-target", None)
                    .unwrap();
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

            let parent = TempDir::new().unwrap();
            let space = parent.path().join("kutl");
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
            let out = std::fs::read_to_string(parent.path().join("specs/foo.md")).unwrap();
            assert!(
                out.starts_with(kutl_client::surface::SURFACE_SENTINEL_HEADER),
                "sentinel missing from surfaced file: {out}"
            );
            assert!(out.contains("hello"));
            // The .kutlspace must NOT have been copied.
            assert!(!parent.path().join(".kutlspace").exists());
        }

        #[test]
        #[serial]
        fn test_cmd_surface_errors_when_surface_section_missing() {
            let home = TempDir::new().unwrap();
            // SAFETY: #[serial] ensures no other test mutates env vars in parallel.
            unsafe {
                std::env::set_var("KUTL_HOME", home.path());
            }

            let parent = TempDir::new().unwrap();
            let space = parent.path().join("kutl");
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
}
