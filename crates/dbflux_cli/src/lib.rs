//! Command-line surface of the `dbflux` binary.
//!
//! Owns argument parsing and every command that does not need a window.
//! Launching the GUI is left to the binary through [`Dispatch::RunGui`],
//! which keeps gpui out of this crate.

mod commands;
mod launcher;
mod interface;

use commands::Command;
pub use interface::{Dispatch, Runnable};

pub use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "dbflux",
    version,
    about,
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    /// Internal: run the GUI in this process (used when spawning).
    #[arg(long, hide = true)]
    pub gui: bool,

    /// Script to open in the running instance.
    #[arg(value_name = "PATH")]
    pub path: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Cli {
    pub fn run(self) -> Dispatch {
        if let Some(command) = self.command {
            return command.run();
        }

        if self.gui {
            return Dispatch::RunGui;
        }

        if self.path.is_some() {
            return Dispatch::Exit(launcher::open_or_spawn(self.path));
        }

        if launcher::focus_running_instance() {
            Dispatch::Exit(0)
        } else {
            Dispatch::RunGui
        }
    }
}
