//! `kutl agent create` / `kutl agent list` — provision tool-held agent
//! keypairs.
//!
//! An agent is a distinct `did:key` principal whose TOOL process (e.g. the
//! `kutl mcp serve` bridge), not the LLM, holds the keyfile — SSH-shaped. The
//! keyfile is an ordinary [`Identity`] stored at `$KUTL_HOME/agents/<name>.toml`;
//! the operator adds the printed `did:key` to the relay's `authorized_keys`
//! (typically scoped to a single space) to grant it access.

use std::path::Path;

use anyhow::{Context, Result};
use kutl_client::{Identity, agent_identity_path, kutl_home};

/// The literal `scope=` value in the printed `authorized_keys` template that the
/// operator must replace with a real space uuid before the line is valid.
///
/// The relay's `authorized_keys` parser fails CLOSED on a malformed
/// option (an empty/whitespace scope, a non-`key=value` token, an unrecognized
/// key), so a copy-paste of the template with this placeholder still present
/// would parse the literal string `<SPACE_UUID>` as the sole scoped space and
/// silently deny every real space — hence the loud "replace" instruction next to
/// it.
pub(crate) const SPACE_UUID_PLACEHOLDER: &str = "<SPACE_UUID>";

/// Build the space-scoped `authorized_keys` line for `did`, labelled with the
/// agent `name`.
///
/// The result is a grammar-valid line EXCEPT for the [`SPACE_UUID_PLACEHOLDER`]
/// scope value, which the operator replaces with a real space uuid. Shape:
/// `<did> scope=<SPACE_UUID> name=<name>`. Shared by `kutl agent create`'s hint
/// and the `mcp serve` authz-failure guidance so the two cannot drift.
///
/// `name=` is the field participants address the agent by, so the name given
/// here is the one they will type — not an operator's private note, which is
/// what `notes=` on the same line is for.
pub(crate) fn scoped_authorized_keys_line(did: &str, name: &str) -> String {
    format!("{did} scope={SPACE_UUID_PLACEHOLDER} name={name}")
}

/// Agent entity: `kutl agent <verb>`.
#[derive(clap::Args)]
pub struct AgentCli {
    #[command(subcommand)]
    pub action: AgentAction,
}

/// The `kutl agent` verbs.
#[derive(clap::Subcommand)]
pub enum AgentAction {
    /// Provision a new tool-held agent keypair.
    Create {
        /// Agent name (alphanumeric/-/_); the keyfile is stored at
        /// `$KUTL_HOME/agents/<name>.toml`.
        #[arg(long)]
        name: String,
    },
    /// List provisioned agent keypairs.
    List {
        /// Output format.
        #[arg(long, value_enum, default_value_t)]
        format: crate::OutputFormat,
    },
}

/// Dispatch a `kutl agent` invocation.
pub fn run(cli: AgentCli) -> Result<()> {
    match cli.action {
        AgentAction::Create { name } => create(&name),
        AgentAction::List { format } => list(format),
    }
}

/// Provision a new agent keypair, refusing to clobber an existing keyfile.
///
/// Prints the agent's `did:key` to stdout and, to stderr, the line to add to
/// the relay's `authorized_keys` (scoped to a space).
fn create(name: &str) -> Result<()> {
    let path = agent_identity_path(name)?;
    if kutl_client::text_file::exists(&path) {
        anyhow::bail!(
            "agent keyfile already exists at {} (refusing to overwrite)",
            path.display()
        );
    }

    let identity = Identity::generate();
    identity
        .save(&path)
        .with_context(|| format!("failed to save agent keyfile to {}", path.display()))?;

    println!("{}", identity.did);
    eprintln!(
        "\nadd the agent to the relay authorized_keys.\n\
         # scoped to one space — replace {placeholder} with a real space uuid\n\
         # (a malformed line fails CLOSED and silently denies the agent):\n\
         {scoped}\n\
         # or grant ALL spaces forever with a bare DID line (no options):\n\
         {did}",
        placeholder = SPACE_UUID_PLACEHOLDER,
        scoped = scoped_authorized_keys_line(&identity.did, name),
        did = identity.did,
    );
    Ok(())
}

/// List provisioned agent keypairs sorted by name — `name  did` lines, or a
/// `[{name, did}]` array for `--format json`.
fn list(format: crate::OutputFormat) -> Result<()> {
    let dir = kutl_home()?.join("agents");
    let agents = read_agents(&dir)?;
    match format {
        crate::OutputFormat::Json => {
            let rows: Vec<serde_json::Value> = agents
                .iter()
                .map(|(name, did)| serde_json::json!({ "name": name, "did": did }))
                .collect();
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        crate::OutputFormat::Human => {
            for (name, did) in agents {
                println!("{name}  {did}");
            }
        }
    }
    Ok(())
}

/// Enumerate `<dir>/*.toml` agent keyfiles (and pre-move `*.json` ones,
/// which load through the fallback and are rewritten), returning
/// `(name, did)` pairs sorted by name: the stems are deduped through a
/// `BTreeSet`, so `<name>.toml` and a pre-move `<name>.json` collapse to
/// one entry and iteration is already name-ordered.
///
/// A missing directory yields an empty list (no agents provisioned yet).
fn read_agents(dir: &Path) -> Result<Vec<(String, String)>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to read {}", dir.display()));
        }
    };

    let mut names = std::collections::BTreeSet::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
        let path = entry.path();
        if !kutl_client::text_file::is_text_file(&path) {
            continue;
        }
        if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
            names.insert(name.to_owned());
        }
    }
    let mut agents = Vec::new();
    for name in names {
        let path = dir.join(format!("{name}.toml"));
        let identity = Identity::load(&path)
            .with_context(|| format!("failed to load agent keyfile {}", path.display()))?;
        agents.push((name, identity.did));
    }
    Ok(agents)
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tempfile::TempDir;

    use super::*;

    /// Point `$KUTL_HOME` at a temp dir for the duration of a serialized test.
    fn with_kutl_home(dir: &TempDir) {
        // SAFETY: env-mutating tests are serialized via `#[serial]`.
        unsafe { std::env::set_var("KUTL_HOME", dir.path()) };
    }

    fn clear_kutl_home() {
        // SAFETY: env-mutating tests are serialized via `#[serial]`.
        unsafe { std::env::remove_var("KUTL_HOME") };
    }

    #[test]
    #[serial]
    fn test_create_writes_keyfile_and_returns_did() {
        let dir = TempDir::new().unwrap();
        with_kutl_home(&dir);

        create("claude-laptop").unwrap();
        let path = agent_identity_path("claude-laptop").unwrap();
        let identity = Identity::load(&path).unwrap();
        clear_kutl_home();

        assert!(path.exists(), "keyfile must be written");
        assert!(
            identity.did.starts_with("did:key:z6Mk"),
            "got: {}",
            identity.did
        );
    }

    #[test]
    #[serial]
    fn test_create_refuses_to_clobber_existing_keyfile() {
        let dir = TempDir::new().unwrap();
        with_kutl_home(&dir);

        create("claude-laptop").unwrap();
        let result = create("claude-laptop");
        clear_kutl_home();

        assert!(result.is_err(), "second create must refuse to overwrite");
    }

    #[test]
    #[serial]
    fn test_read_agents_enumerates_keyfiles() {
        let dir = TempDir::new().unwrap();
        with_kutl_home(&dir);
        create("alpha").unwrap();
        create("beta").unwrap();
        let agents_dir = kutl_home().unwrap().join("agents");
        let agents = read_agents(&agents_dir).unwrap();
        clear_kutl_home();

        assert_eq!(agents.len(), 2);
        let names: Vec<&str> = agents.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"alpha"));
        assert!(names.contains(&"beta"));
    }

    #[test]
    fn test_read_agents_missing_dir_is_empty() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("agents");
        assert!(read_agents(&missing).unwrap().is_empty());
    }

    #[test]
    fn test_scoped_authorized_keys_line_is_a_parseable_template() {
        // The scoped template must be a VALID authorized_keys line per the
        // grammar: DID, then `scope=<uuid> name=<name>`. The
        // <SPACE_UUID> placeholder is the one token the operator replaces.
        let did = "did:key:z6MkTestAgentPrincipalXYZ";
        let line = scoped_authorized_keys_line(did, "claude-laptop");
        assert!(
            line.starts_with(did),
            "line must start with the real DID: {line}"
        );
        assert!(
            line.contains("scope=<SPACE_UUID>"),
            "line must carry the replace-me scope placeholder: {line}"
        );
        assert!(
            line.contains("name=claude-laptop"),
            "line must carry the agent name in the addressable field: {line}"
        );
        // Exactly three whitespace-separated tokens (DID + two key=value options),
        // so a copy-paste (with <SPACE_UUID> replaced) parses cleanly.
        let tokens: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(
            tokens,
            vec![did, "scope=<SPACE_UUID>", "name=claude-laptop"]
        );
    }

    #[test]
    #[serial]
    fn test_create_hint_echoes_the_real_did() {
        // The printed hint must reference the agent's actual DID (not a
        // placeholder), so a copy-paste authorizes the right principal.
        let dir = TempDir::new().unwrap();
        with_kutl_home(&dir);
        create("claude-laptop").unwrap();
        let path = agent_identity_path("claude-laptop").unwrap();
        let did = Identity::load(&path).unwrap().did;
        clear_kutl_home();

        let line = scoped_authorized_keys_line(&did, "claude-laptop");
        assert!(
            line.starts_with(&did),
            "hint must echo the real DID: {line}"
        );
    }
}
