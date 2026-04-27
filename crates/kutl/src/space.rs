//! Space configuration and registry.
//!
//! Re-exports from [`kutl_client`] for use within the CLI binary crate.

pub use kutl_client::space_config::{DEFAULT_RELAY_URL, SpaceConfig};
pub use kutl_client::space_registry::{
    SpaceRegistry, generate_space_id, generate_space_name, registry_path,
};
