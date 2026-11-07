mod mcp;
use mcp::{MCP_SECURITY_NOTICE, McpArgs};

use clap::Subcommand;

use super::interface::{Dispatch, Runnable};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run DBFlux as an MCP (Model Context Protocol) server for AI clients.
    #[command(after_help = MCP_SECURITY_NOTICE)]
    Mcp(McpArgs),
}

impl Runnable for Command {
    fn run(self) -> Dispatch {
        match self {
            Command::Mcp(args) => args.run(),
        }
    }
}
