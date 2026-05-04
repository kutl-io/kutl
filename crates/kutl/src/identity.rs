//! DID identity generation and persistence.
//!
//! Re-exports from [`kutl_client::identity`] for use within the CLI binary crate.

pub use kutl_client::identity::{Identity, default_identity_path, load_or_generate};
