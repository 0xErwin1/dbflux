use std::path::PathBuf;

use dbflux_cli::*;


fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("dbflux").chain(args.iter().copied()))
}

#[test]
fn mcp_requires_client_id() {
    assert!(parse(&["mcp"]).is_err());
}

#[test]
fn mcp_parses_options() {
    let cli = parse(&["mcp", "--client-id", "claude", "--config-dir", "/tmp/x"]).unwrap();
    let Some(Command::Mcp(args)) = cli.command else {
        panic!("expected mcp subcommand");
    };
    assert_eq!(args.client_id, "claude");
    assert_eq!(args.config_dir, Some(PathBuf::from("/tmp/x")));
}

#[test]
fn gui_flag_runs_gui_in_process() {
    assert_eq!(parse(&["--gui"]).unwrap().run(), Dispatch::RunGui);
}

#[test]
fn path_cannot_be_combined_with_subcommand() {
    assert!(parse(&["script.sql", "mcp", "--client-id", "x"]).is_err());
}

#[test]
fn unknown_argument_is_rejected() {
    assert!(parse(&["--bogus"]).is_err());
}