//! Shared sync client library for kutl.
//!
//! Provides a `SyncClient` for WebSocket sync, credential storage,
//! DID authentication, URL utilities, identity management, and
//! space configuration.

mod connection;
pub mod credentials;
mod did_auth;
pub mod dirs;
pub mod git_detect;
pub mod identity;
pub mod kutlspace;
pub mod pid;
mod recovery;
pub mod space_config;
pub mod space_gitignore;
pub mod space_registry;
pub mod surface;
mod url;

/// Default relay URL for kutlhub.com production.
pub const DEFAULT_KUTLHUB_RELAY_URL: &str = "wss://relay.kutlhub.com/ws";

pub use connection::{SyncClient, SyncEvent};
pub use credentials::StoredCredentials;
pub use did_auth::authenticate;
pub use dirs::kutl_home;
pub use git_detect::find_git_repo_root;
pub use identity::{Identity, default_identity_path, load_or_generate};
pub use kutlspace::{KUTLSPACE_FILENAME, KutlspaceConfig, SurfaceConfig};
pub use pid::{
    is_process_alive, read_pid_file, read_pid_file_alive, remove_pid_file, send_signal,
    write_pid_file,
};
pub use recovery::RecoveryConfig;
pub use space_config::{DEFAULT_RELAY_URL, SpaceConfig};
pub use space_gitignore::{GITIGNORE_CONTENTS, GITIGNORE_FILENAME, write_space_gitignore};
pub use space_registry::{SpaceRegistry, generate_space_id, generate_space_name, registry_path};
pub use surface::{
    SurfaceFile, copy_surface_files, enumerate_surface_files, resolve_surface_target,
};
pub use url::{http_url_to_ws, normalize_relay_url, relay_url_to_http, ws_url_to_http};
