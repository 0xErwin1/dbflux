pub mod connection_cache;
mod error_messages;
pub mod governance;
mod handlers;
mod helper;
pub mod server;
pub mod state;
pub mod tools;

use rmcp::{ServiceExt, transport::stdio};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use crate::server::DbFluxServer;
use crate::state::ServerState;

#[derive(Debug, Clone)]
pub struct McpServerArgs {
    pub client_id: String,
    pub config_dir: Option<PathBuf>,
}

pub async fn run_mcp_server(args: McpServerArgs) -> anyhow::Result<()> {
    // Setup logging to file (don't pollute stdout with logs)
    let log_path = std::env::temp_dir().join("dbflux_mcp.log");
    let log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&log_path)
        .map_err(|e| anyhow::anyhow!("Failed to open log file {}: {}", log_path.display(), e))?;

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug"))
        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {}: {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.target(),
                record.args()
            )
        })
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
