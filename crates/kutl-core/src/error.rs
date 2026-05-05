//! Error types for kutl-core operations.

use std::path::PathBuf;

use diamond_types::list::encoding::encode_tools::ParseError;

/// Errors produced by kutl-core operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to decode a diamond-types binary blob.
    #[error("decode error: {0}")]
    Decode(#[from] ParseError),

    /// Failed to read a file from disk.
    #[error("failed to read {path}")]
    FileRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Failed to write a file to disk.
    #[error("failed to write {path}")]
    FileWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Failed to decode protobuf change metadata.
    #[error("change decode error: {0}")]
    ChangeDecode(#[from] prost::DecodeError),

    /// An edit position exceeds the current document length.
    #[error("position {pos} out of bounds for document of length {len}")]
    OutOfBounds { pos: usize, len: usize },

    /// A delete range is invalid (start >= end or end > len).
    #[error("invalid range {start}..{end} for document of length {len}")]
    InvalidRange {
        start: usize,
        end: usize,
        len: usize,
    },

    /// Agent name exceeds the diamond-types byte limit.
    #[error("agent name is {len} bytes, exceeds {max}-byte limit")]
    AgentNameTooLong { len: usize, max: usize },

    /// Incoming merge patch exceeds the per-merge byte cap.
    ///
    /// See [`crate::MAX_PATCH_BYTES`] and RFD 0063 §3.
    #[error("merge patch size {size} exceeds cap {cap}")]
    PatchTooLarge { size: usize, cap: usize },

    /// Document op-count is at or above the per-document cap.
    ///
    /// See [`crate::MAX_OPS_PER_DOC`] and RFD 0063 §3.
    #[error("document operation count {current} at/above cap {cap}")]
    OpCountExceeded { current: usize, cap: usize },
}

/// Convenience alias used throughout kutl-core.
pub type Result<T> = std::result::Result<T, Error>;
