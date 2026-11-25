use std::time::Duration;

use super::{DEFAULT_POLL_INTERVAL, WatchOptions, run_watch};

pub const WATCH_HELP: &str = "\
Usage: dbflux mcp approvals watch [--pending-id <id>] [--client-id <id>] [--timeout <secs>]

Watch the shared approval queue (dbflux.db) and print one JSON line per event
to stdout, flushed after every line. Meant to run as a background command that
an agent reads line by line.

Options:
  --pending-id <id>   Wait until this request is decided, print one line, exit
  --client-id <id>    Only report requests made by this client (actor_id)
  --timeout <secs>    Stop waiting after this many seconds (default: no limit)
  --help, -h          Show this help message

Without --pending-id the watcher streams until --timeout (or forever). It
reports the requests already pending at start and every later one.
It reports a decision only for a request it already announced as pending:
a request created and decided within one poll (about a second) produces no
line. Use --pending-id to follow a specific request.

Events (JSON, one object per line):
  pending     A request is waiting for a decision
  approved    A person approved it
  rejected    A person rejected it; \"reason\" holds what they wrote, if anything
  expired     Nobody decided it in time, or an approval outlived its time to live
  not_found   --pending-id only: unknown, another client's, or already used
  still_pending  --pending-id only: --timeout elapsed before a decision

Every line has \"event\" and \"pending_id\". Events other than not_found and
still_pending also carry \"tool_id\", \"actor_id\" and \"connection_id\".

Exit codes:
  0     --pending-id: decided as approved, rejected or expired
        stream mode: --timeout elapsed
  1     --pending-id: not_found; or an error (bad arguments, the shared store
        cannot be opened or read)
  124   --pending-id: --timeout elapsed while still pending

Example:
  dbflux mcp approvals watch --pending-id 0190a1b2-... --timeout 600
";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchArgs {
    Help,
    Watch {
        pending_id: Option<String>,
        client_id: Option<String>,
        timeout_secs: Option<u64>,
    },
}

/// Parses the arguments after `approvals`.
pub fn parse_watch_args(args: &[String]) -> Result<WatchArgs, String> {
    let mut iter = args.iter();

    match iter.next().map(String::as_str) {
        Some("watch") => {}
        Some("--help" | "-h") => return Ok(WatchArgs::Help),
        Some(other) => return Err(format!("Unknown approvals command: {other}")),
        None => return Err("Missing approvals command (expected: watch)".to_string()),
    }

    let mut pending_id = None;
    let mut client_id = None;
    let mut timeout_secs = None;

    while let Some(arg) = iter.next() {
        let mut value = |name: &str| {
            iter.next()
                .cloned()
                .ok_or_else(|| format!("{name} requires a value"))
        };

        match arg.as_str() {
            "--pending-id" => pending_id = Some(value("--pending-id")?),
            "--client-id" => client_id = Some(value("--client-id")?),
            "--timeout" => {
                let raw = value("--timeout")?;
                timeout_secs = Some(
                    raw.parse::<u64>()
                        .map_err(|_| format!("--timeout expects whole seconds, got '{raw}'"))?,
                );
            }
            "--help" | "-h" => return Ok(WatchArgs::Help),
            other => return Err(format!("Unknown argument: {other}")),
        }
    }

    Ok(WatchArgs::Watch {
        pending_id,
        client_id,
        timeout_secs,
    })
}

/// Entry point of `dbflux mcp approvals ...`; `args` follow `approvals`.
/// Returns the process exit code.
pub fn run_approvals_command(args: &[String]) -> i32 {
    let (pending_id, client_id, timeout_secs) = match parse_watch_args(args) {
        Ok(WatchArgs::Help) => {
            print!("{WATCH_HELP}");
            return 0;
        }
        Ok(WatchArgs::Watch {
            pending_id,
            client_id,
            timeout_secs,
        }) => (pending_id, client_id, timeout_secs),
        Err(error) => {
            eprintln!("{error}");
            eprintln!();
            eprint!("{WATCH_HELP}");
            return 1;
        }
    };

    let store = match crate::state::open_shared_pending_store() {
        Ok(store) => store,
        Err(error) => {
            eprintln!("dbflux mcp approvals watch: {error}");
            return 1;
        }
    };

    let options = WatchOptions {
        pending_id,
        client_id,
        timeout: timeout_secs.map(Duration::from_secs),
        poll_interval: DEFAULT_POLL_INTERVAL,
    };

    run_watch(store.as_ref(), &options, &mut std::io::stdout().lock())
}
