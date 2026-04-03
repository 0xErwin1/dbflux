#[cfg(feature = "mcp")]
pub fn run_mcp_command(args: &[String]) -> i32 {
    match parse_mcp_args(args) {
        Ok(mcp_args) => {
            // Create tokio runtime for async MCP server
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("Failed to create tokio runtime: {}", e);
                    return 1;
                }
            };

            // Run async server
            match rt.block_on(dbflux_mcp_server::run_mcp_server(mcp_args)) {
                Ok(_) => 0,
                Err(e) => {
                    eprintln!("MCP server error: {}", e);
                    1
                }
            }
        }
        Err(e) => {
            eprintln!("{}", e);
            print_mcp_help();
            1
        }
    }
}

#[cfg(not(feature = "mcp"))]
pub fn run_mcp_command(_args: &[String]) -> i32 {
    eprintln!("Error: MCP support not compiled into this build.");
    eprintln!();
    eprintln!("To enable MCP functionality, rebuild with:");
    eprintln!("  cargo build --features mcp");
    eprintln!();
    eprintln!("Or use the default build (MCP included):");
    eprintln!("  cargo build");
    1
}

#[cfg(feature = "mcp")]
fn parse_mcp_args(args: &[String]) -> Result<dbflux_mcp_server::McpServerArgs, String> {
    use std::path::PathBuf;

    let mut client_id = None;
    let mut config_dir = None;
    let mut iter = args.iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--client-id" => {
                client_id = iter.next().map(|s| s.to_string());
            }
            "--config-dir" => {
                config_dir = iter.next().map(PathBuf::from);
            }
            "--help" | "-h" => {
                print_mcp_help();
                std::process::exit(0);
            }
            other => {
                return Err(format!("Unknown argument: {}", other));
            }
        }
    }

    let client_id = client_id.ok_or("--client-id is required".to_string())?;

    Ok(dbflux_mcp_server::McpServerArgs {
        client_id,
        config_dir,
    })
}

fn print_mcp_help() {
    eprintln!("Usage: dbflux mcp --client-id <id> [options]");
    eprintln!();
    eprintln!("Run DBFlux as an MCP (Model Context Protocol) server for AI clients.");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --client-id <id>      Identifier for this AI client (required)");
    eprintln!("                        Must match a trusted client in MCP settings");
    eprintln!("  --config-dir <path>   Override config directory (default: ~/.config/dbflux)");
    eprintln!("  --help, -h            Show this help message");
    eprintln!();
    eprintln!("Example:");
    eprintln!("  dbflux mcp --client-id claude-desktop");
}
