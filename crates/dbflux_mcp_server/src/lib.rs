pub mod connection_cache;
mod error_messages;
pub mod governance;
mod handlers;
mod helper;
pub mod server;
pub mod state;
pub mod tools;

use std::path::PathBuf;
use std::sync::Arc;

use rmcp::{ServiceExt, transport::stdio};

use crate::server::DbFluxServer;
use crate::state::ServerState;

#[derive(Debug, Clone)]
pub struct McpServerArgs {
    pub client_id: String,
    pub config_dir: Option<PathBuf>,
}

pub async fn run_mcp_server(args: McpServerArgs) -> anyhow::Result<()> {
    use dbflux_core::observability::tracing_bridge::{BridgeConfig, FmtWriter, init_tracing};

    let log_path = resolve_mcp_log_path();
    let fmt_writer = FmtWriter::NonBlockingFile(log_path);

    let bridge_config = BridgeConfig {
        include_audit_layer: true,
        fmt_writer,
        env_filter_default: "debug,hyper=warn",
        ..BridgeConfig::default()
    };

    let bridge_handle = match init_tracing(bridge_config) {
        Ok(h) => Some(h),
        Err(err) => {
            eprintln!("dbflux-mcp-server: failed to initialize tracing: {err}");
            None
        }
    };

    log::info!("dbflux-mcp-server starting, client_id={}", args.client_id);

    let state = ServerState::new(args.client_id.clone(), args.config_dir)
        .map_err(|e| anyhow::anyhow!("Failed to initialize MCP server: {}", e))?;

    // Install the audit sink into the bridge so tracing events are routed
    // to the audit database.  The clone shares the underlying SQLite
    // connection with the service held by McpRuntime.
    if let Some(ref handle) = bridge_handle {
        let audit_clone = state.runtime.blocking_read().audit_service().clone();
        if let Err(err) = handle.install_sink(Arc::new(audit_clone)) {
            log::warn!("Failed to install audit bridge sink: {err}");
        }
    }

    log::info!("dbflux-mcp-server initialized");

    let server = DbFluxServer::new(state);

    let service = server.serve(stdio()).await?;

    log::info!("dbflux-mcp-server ready");

    service.waiting().await?;

    log::info!("dbflux-mcp-server shutting down");

    if let Some(handle) = bridge_handle {
        use dbflux_core::observability::tracing_bridge::ShutdownError;
        match handle.shutdown() {
            Ok(()) => {}
            Err(ShutdownError::DrainTimeout {
                remaining_in_flight,
            }) => {
                eprintln!(
                    "dbflux-mcp-server: audit bridge shutdown timed out, dropped {} in-flight events",
                    remaining_in_flight
                );
            }
            Err(ShutdownError::JoinPanic) => {
                eprintln!("dbflux-mcp-server: audit bridge drain thread panicked during shutdown");
            }
        }
    }

    Ok(())
}

fn resolve_mcp_log_path() -> PathBuf {
    if let Ok(data_dir) = dbflux_storage::paths::data_dir() {
        let logs_dir = data_dir.join("logs");
        return logs_dir.join("dbflux_mcp.log");
    }
    std::env::temp_dir().join("dbflux_mcp.log")
}
