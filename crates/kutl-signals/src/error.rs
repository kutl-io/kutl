//! Error types for kutl-signals operations.

use std::path::PathBuf;

/// Errors produced by kutl-signals operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A segment file could not be read.
    #[error("failed to read segment {path}")]
    SegmentRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A segment file could not be written.
    #[error("failed to write segment {path}")]
    SegmentWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A sealed segment failed its integrity check and was quarantined.
    #[error("segment {path} failed integrity check: {reason}")]
    SegmentCorrupt { path: PathBuf, reason: String },

    /// Another writer holds the space's segment lock.
    #[error("segment directory {path} is locked by another writer")]
    Locked { path: PathBuf },

    /// A record exceeded the maximum allowed size and was not written.
    #[error("record of {bytes} bytes exceeds max {max}")]
    RecordTooLarge { bytes: usize, max: usize },

    /// A cursor file could not be read.
    #[error("failed to read cursor {path}")]
    CursorRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A cursor file could not be written.
    #[error("failed to write cursor {path}")]
    CursorWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A cursor file failed JSON (de)serialization.
    #[error("cursor json error: {0}")]
    CursorSerde(#[from] serde_json::Error),

    /// A record failed protobuf decoding.
    #[error("record decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    /// A DID string could not be parsed into a verifying key.
    #[error("invalid did:key: {reason}")]
    InvalidDidKey { reason: String },
}

/// Convenience alias used across the crate.
pub type Result<T> = std::result::Result<T, Error>;
