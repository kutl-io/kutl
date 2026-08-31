//! Shared sync client library for kutl.
//!
//! Provides a `SyncClient` for WebSocket sync, credential storage,
//! DID authentication, URL utilities, identity management, and
//! space configuration.

mod bounds;
mod connection;
pub mod credentials;
mod did_auth;
pub mod dirs;
mod file_lock;
pub use file_lock::try_lock_exclusive;
pub mod git_detect;
pub mod identity;
pub mod known_relays;
pub mod kutlspace;
pub mod pid;
mod recovery;
mod secret_file;
pub mod signal_catchup;
pub mod space_config;
pub mod space_gitignore;
pub mod space_registry;
pub mod surface;
mod url;

/// Default relay URL for the hosted production deployment.
///
/// Points at the ux-server front door's relay namespace (`/relay/ws`), not the
/// internal relay directly — the relay is not publicly reachable. The HTTP base
/// derived from this (`https://kutlhub.com/relay`) is where the CLI fetches the
/// policy, runs the device flow, and resolves spaces.
pub const DEFAULT_KUTLHUB_RELAY_URL: &str = "wss://kutlhub.com/relay/ws";

pub use connection::{RegisterDocumentMetadata, SyncClient, SyncEvent};
pub use credentials::StoredCredentials;
pub use did_auth::{authenticate, resolve_or_authenticate};
pub use dirs::{agent_identity_path, kutl_home};
pub use git_detect::{find_git_repo_root, find_git_repo_root_bounded};
pub use identity::{Identity, default_identity_path, load_or_generate};
pub use known_relays::{KnownRelays, RelayPinOutcome, known_relays_path};
pub use kutlspace::{KutlspaceConfig, SurfaceConfig};
pub use pid::{
    is_process_alive, read_pid_file, read_pid_file_alive, remove_pid_file, send_signal,
    write_pid_file,
};
pub use signal_catchup::{FeedPage, SignalCatchUpClient, TransitionEvent, TransitionRequest};
pub use space_config::{
    DEFAULT_RELAY_URL, SpaceConfig, find_space_root_upward, find_space_root_upward_bounded,
};
pub use space_gitignore::write_space_gitignore;
pub use space_registry::{SpaceRegistry, generate_space_id, generate_space_name, registry_path};
pub use surface::{
    SurfaceFile, copy_surface_files, enumerate_surface_files, resolve_surface_target,
};
pub use url::{
    http_url_to_ws, normalize_relay_url, relay_url_to_http, validate_relay_url, ws_url_to_http,
};
