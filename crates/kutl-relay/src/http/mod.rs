//! HTTP handlers mounted beside the WebSocket sync surface.
//!
//! Each submodule owns one route family. Authentication for these routes
//! resolves a bearer token to a DID per-request via
//! [`crate::mcp_handler::authenticate_request`], then authorizes space
//! membership inside the relay actor — the same rejection class the
//! WebSocket path uses.

pub mod changes;
