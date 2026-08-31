//! kutl-cli-uxr — user-lens integration harness for the kutl CLI + MCP surface.
//!
//! Spawns the real `kutl` and `kutl-relay` binaries (and drives `kutl mcp
//! serve` over JSON-RPC stdio) to exercise end-to-end user journeys with hard
//! assertions. The OSS analog of the kutlhub Playwright UXR suite.

pub mod harness;
