use std::path::PathBuf;

/// Runs the MCP server on stdio until the client disconnects; returns the exit code.
#[cfg(feature = "mcp")]
pub fn run_mcp_command(client_id: String, config_dir: Option<PathBuf>) -> i32 {
    let mcp_args = dbflux_mcp_server::McpServerArgs {
        client_id,
        config_dir,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to create tokio runtime: {}", e);
            return 1;
        }
    };

    match rt.block_on(dbflux_mcp_server::run_mcp_server(mcp_args)) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("MCP server error: {}", e);
            1
        }
    }
}

/// Reports that this build lacks MCP support; always returns exit code 1.
#[cfg(not(feature = "mcp"))]
pub fn run_mcp_command(_client_id: String, _config_dir: Option<PathBuf>) -> i32 {
    eprintln!("Error: MCP support not compiled into this build.");
    eprintln!();
    eprintln!("To enable MCP functionality, rebuild with:");
    eprintln!("  cargo build --features mcp");
    eprintln!();
    eprintln!("Or use the default build (MCP included):");
    eprintln!("  cargo build");
    1
}
