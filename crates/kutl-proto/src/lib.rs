//! kutl-proto — generated protobuf types for the kutl sync protocol.

pub mod protocol;

pub mod sync {
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/kutl.sync.v1.rs"));
}
