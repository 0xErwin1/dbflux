use clap::Args;
use std::path::PathBuf;

use crate::interface::{Dispatch, Runnable};
use dbflux_app::mcp_command::run_mcp_command;

#[derive(Debug, Args)]
pub struct McpArgs {
    /// Identifier for this AI client; must match a trusted client in MCP settings.
    #[arg(long)]
    pub client_id: String,

    /// Override config directory (default: ~/.local/share/dbflux).
    #[arg(long)]
    pub config_dir: Option<PathBuf>,
}

impl Runnable for McpArgs {
    fn run(self) -> Dispatch {
        Dispatch::Exit(run_mcp_command(self.client_id, self.config_dir))
    }
}

pub(super) const MCP_SECURITY_NOTICE: &str = "\
Example:
  dbflux mcp --client-id claude-desktop

Security notice:
  Authentication is process-identity only. --client-id is a shared identifier,
  not a cryptographic credential. Any local process that knows the client ID
  can connect. Do not expose this server beyond localhost without an additional
  authentication layer (P3 roadmap item).";
