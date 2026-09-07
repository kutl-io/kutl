//! Error types for kutl-core operations.

use std::any::Any;
use std::path::PathBuf;

/// Errors produced by kutl-core operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to decode a diamond-types binary blob.
    ///
    /// Boxed rather than named: diamond-types keeps its parse error in a
    /// private module. The typed value lives on inside the box, so the
    /// message and source chain are the decoder's own; only matching on its
    /// variants is off the table, and nothing does. The conversion has one
    /// site, the engine's panic guard.
    #[error("decode error: {0}")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The engine panicked while applying the given bytes; the panic was
    /// caught at the boundary and `message` is its payload.
    ///
    /// The engine that raised this must be discarded. A panic skips the
    /// decoder's own rollback, so the operation log may be partially
    /// applied; the caller reloads the document from durable storage.
    #[error("engine panicked: {message}")]
    EnginePanicked { message: String },

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
    /// See [`crate::MAX_PATCH_BYTES`].
    #[error("merge patch size {size} exceeds cap {cap}")]
    PatchTooLarge { size: usize, cap: usize },

    /// Document op-count is at or above the per-document cap.
    ///
    /// See [`crate::MAX_OPS_PER_DOC`].
    #[error("document operation count {current} at/above cap {cap}")]
    OpCountExceeded { current: usize, cap: usize },
}

/// Convenience alias used throughout kutl-core.
pub type Result<T> = std::result::Result<T, Error>;

/// Text shown when a panic payload is neither a `&str` nor a `String`.
const OPAQUE_PANIC_PAYLOAD: &str = "non-string panic payload";

/// The human-readable message carried by a panic payload.
///
/// `panic!` with a literal stores a `&str`; `panic!` with arguments stores a
/// `String`; `panic_any` stores anything. This is the one reader for all of
/// them, shared by the engine's panic guard and the services' panic hook.
#[must_use]
pub fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        OPAQUE_PANIC_PAYLOAD.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_panic_payload_message_reads_each_payload_kind() {
        let literal = std::panic::catch_unwind(|| panic!("literal")).unwrap_err();
        assert_eq!(panic_payload_message(literal.as_ref()), "literal");

        let formatted = std::panic::catch_unwind(|| panic!("formatted {}", 7)).unwrap_err();
        assert_eq!(panic_payload_message(formatted.as_ref()), "formatted 7");

        let opaque = std::panic::catch_unwind(|| std::panic::panic_any(42_u8)).unwrap_err();
        assert_eq!(panic_payload_message(opaque.as_ref()), OPAQUE_PANIC_PAYLOAD);
    }

    #[test]
    fn test_decode_error_displays_the_boxed_source() {
        #[derive(Debug)]
        struct Inner;
        impl std::fmt::Display for Inner {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("bad magic")
            }
        }
        impl std::error::Error for Inner {}

        let err = Error::Decode(Box::new(Inner));
        assert_eq!(err.to_string(), "decode error: bad magic");
        assert!(std::error::Error::source(&err).is_some());
    }
}
