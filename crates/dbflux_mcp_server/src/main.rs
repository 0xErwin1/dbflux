mod bootstrap;
mod connection_cache;
mod handlers;
mod server;
mod transport;

use std::path::PathBuf;
use std::process;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let args = parse_args();

    let mut state = bootstrap::init(args.client_id, args.config_dir).unwrap_or_else(|e| {
        eprintln!("Error: Failed to initialize MCP server: {e}");
        process::exit(1);
    });

    log::info!("dbflux-mcp-server started, client_id={}", state.client_id);

    let mut reader = transport::stdin_reader();
    let mut writer = std::io::stdout();

    if let Err(e) = server::run(&mut state, &mut reader, &mut writer) {
        log::error!("Server exited with error: {e}");
        process::exit(1);
    }
}

struct Args {
    client_id: String,
    config_dir: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut client_id = None;
    let mut config_dir = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--client-id" => {
                client_id = args.next();
            }
            "--config-dir" => {
                config_dir = args.next().map(PathBuf::from);
            }
            "--help" | "-h" => {
                eprintln!("Usage: dbflux-mcp-server --client-id <id> [--config-dir <path>]");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --client-id <id>      Identifier for this AI client (required)");
                eprintln!(
                    "  --config-dir <path>   Override config directory (default: ~/.config/dbflux)"
                );
                process::exit(0);
            }
            other => {
                eprintln!("Error: Unknown argument: {other}");
                eprintln!("Run with --help for usage.");
                process::exit(1);
            }
        }
    }

    Args {
        client_id: client_id.unwrap_or_else(|| {
            eprintln!("Error: --client-id is required");
            process::exit(1);
        }),
        config_dir,
    }
}
