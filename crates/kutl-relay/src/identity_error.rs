//! Error type for relay identity generation and persistence.

/// Errors that can occur when loading or generating the relay signing identity.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// Failed to read the identity file.
    #[error("failed to read identity file {0}: {1}")]
    Read(String, #[source] std::io::Error),

    /// The identity file is absent from an explicitly configured identity
    /// directory.
    ///
    /// Only [`RelayIdentity::load`] raises this; the `load_or_generate` path
    /// treats absence as a normal first start. The distinction is the whole
    /// point of having two constructors — see that method's docs.
    ///
    /// [`RelayIdentity::load`]: crate::identity::RelayIdentity::load
    #[error(
        "identity file {0} is absent, and an explicitly configured identity is never generated"
    )]
    Absent(String),

    /// Failed to parse the identity file as JSON.
    #[error("failed to parse identity file {0}: {1}")]
    Parse(String, #[source] serde_json::Error),

    /// Failed to decode the private key from base64url.
    #[error("failed to decode private key: {0}")]
    DecodeKey(String),

    /// The DID stored in the identity file does not match the key.
    #[error(
        "stored did {stored:?} does not match key-derived did {derived:?}; identity file may be corrupt"
    )]
    DidMismatch { stored: String, derived: String },

    /// Failed to create the data directory.
    #[error("failed to create data directory {0}: {1}")]
    CreateDir(String, #[source] std::io::Error),

    /// Failed to write the identity file.
    #[error("failed to write identity file {0}: {1}")]
    Write(String, #[source] std::io::Error),

    /// Failed to serialize the identity to JSON.
    #[error("failed to serialize identity: {0}")]
    Serialize(String),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, IdentityError>;
