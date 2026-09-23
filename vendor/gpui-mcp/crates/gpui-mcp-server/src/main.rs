//! MCP stdio entry point for instrumented GPUI applications.

mod capture;
mod client;
mod recording;
mod tools;

use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Parser;
use gpui_mcp_protocol::AppId;
use rmcp::{ServiceExt as _, transport::stdio};
use tracing_subscriber::EnvFilter;

use client::{BridgeRegistry, default_runtime_root};
use recording::ArtifactStore;
use tools::GpuiMcp;

#[derive(Debug, Parser)]
#[command(
    name = "gpui-mcp",
    version,
    about = "Connect MCP clients to instrumented GPUI applications"
)]
struct Args {
    /// Restrict discovery to one exact endpoint descriptor.
    #[arg(long, value_name = "PATH")]
    endpoint: Option<PathBuf>,

    /// Restrict discovery to this Rust-configured application identifier.
    /// Normal MCP setup does not need this option.
    #[arg(long, value_name = "ID", conflicts_with = "endpoint")]
    app_id: Option<AppId>,

    /// Override the private endpoint discovery directory.
    #[arg(long, value_name = "DIR", conflicts_with = "endpoint")]
    endpoint_dir: Option<PathBuf>,

    /// Directory for recording artifacts. Defaults to a private session directory.
    #[arg(long, value_name = "DIR")]
    artifact_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let args = Args::parse();
    let registry = BridgeRegistry::new(args.endpoint, args.app_id, args.endpoint_dir);
    let artifact_dir = match args.artifact_dir {
        Some(directory) => directory,
        None => default_runtime_root()?
            .join("artifacts")
            .join(format!("server-{}", std::process::id())),
    };
    let artifacts = ArtifactStore::open(artifact_dir).map_err(anyhow::Error::msg)?;
    tracing::info!(
        artifact_dir = %artifacts.directory().display(),
        "started GPUI MCP server"
    );

    let service = GpuiMcp::new(registry, artifacts)
        .serve(stdio())
        .await
        .context("could not start MCP stdio transport")?;
    service
        .waiting()
        .await
        .context("MCP stdio service stopped unexpectedly")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::Args;

    #[test]
    fn accepts_default_discovery() {
        assert!(Args::try_parse_from(["gpui-mcp"]).is_ok());
    }

    #[test]
    fn validates_application_ids() {
        assert!(Args::try_parse_from(["gpui-mcp", "--app-id", "valid.app"]).is_ok());
        assert!(Args::try_parse_from(["gpui-mcp", "--app-id", "../invalid"]).is_err());
    }
}
