mod connection_cache;
mod error_messages;
mod governance;
mod handlers;
mod server;
mod state;

use std::path::PathBuf;
use rmcp::{ServiceExt, transport::stdio};

use crate::state::ServerState;
use crate::server::DbFluxServer;

#[derive(Debug, Clone)]
pub struct McpServerArgs {
    pub client_id: String,
    pub config_dir: Option<PathBuf>,
}

pub async fn run_mcp_server(args: McpServerArgs) -> anyhow::Result<()> {
    // Setup logging to stderr (important: don't pollute stdout!)
    // Note: env_logger writes to stderr by default, so we don't need to specify it
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .init();

    log::info!("dbflux-mcp-server starting, client_id={}", args.client_id);

    // Initialize state
    let state = ServerState::new(args.client_id.clone(), args.config_dir)
        .map_err(|e| anyhow::anyhow!("Failed to initialize MCP server: {}", e))?;

    log::info!("dbflux-mcp-server initialized");

    // Create server
    let server = DbFluxServer::new(state);

    // Serve over stdio transport
    let service = server.serve(stdio()).await?;

    log::info!("dbflux-mcp-server ready");

    // Wait for completion
    service.waiting().await?;

    log::info!("dbflux-mcp-server shutting down");
    Ok(())
}
