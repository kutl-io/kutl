//! Shared test harness: binary resolution, relay process, CLI runner, MCP session.

pub mod binaries;
pub mod cli;
pub mod daemon;
pub mod journey;
pub mod mcp;
pub mod relay;

pub use cli::TestHome;
pub use daemon::DaemonProcess;
pub use mcp::McpSession;
pub use relay::RelayProcess;
