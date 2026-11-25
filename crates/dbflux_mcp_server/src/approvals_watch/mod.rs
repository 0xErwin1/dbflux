//! `dbflux mcp approvals watch`: reports approval decisions from the shared
//! approval store to a process that runs in the background, such as an agent's
//! monitor, one JSON line per event.

mod cli;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::io::Write;
use std::time::{Duration, Instant};

use dbflux_approval::{
    ApprovalOutcome, PendingExecution, PendingExecutionStore, PendingStoreError, classify_execution,
};
use serde_json::{Value, json};
use uuid::Uuid;

pub use cli::{WATCH_HELP, WatchArgs, parse_watch_args, run_approvals_command};

/// Exit code of `--pending-id` mode when the timeout elapsed before a decision.
pub const EXIT_STILL_PENDING: i32 = 124;

const EXIT_OK: i32 = 0;
const EXIT_FAILURE: i32 = 1;

/// How often the store is re-read.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEventKind {
    Pending,
    Approved,
    Rejected,
    Expired,
    NotFound,
}

impl WatchEventKind {
    fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
            Self::NotFound => "not_found",
        }
    }
}

/// One line of output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchEvent {
    pub kind: WatchEventKind,
    pub pending_id: String,
    pub execution: Option<PendingExecution>,
}

impl WatchEvent {
    fn for_execution(kind: WatchEventKind, execution: &PendingExecution) -> Self {
        Self {
            kind,
            pending_id: execution.id.to_string(),
            execution: Some(execution.clone()),
        }
    }

    pub fn to_json(&self) -> Value {
        let mut line = json!({
            "event": self.kind.name(),
            "pending_id": self.pending_id,
        });

        if let Some(execution) = &self.execution {
            line["tool_id"] = json!(execution.plan.tool_id);
            line["actor_id"] = json!(execution.plan.actor_id);
            line["connection_id"] = json!(execution.plan.connection_id);

            if self.kind == WatchEventKind::Rejected
                && let Some(reason) = &execution.rejection_reason
            {
                line["reason"] = json!(reason);
            }
        }

        line
    }
}

fn event_kind(outcome: ApprovalOutcome) -> Option<WatchEventKind> {
    match outcome {
        ApprovalOutcome::Pending => Some(WatchEventKind::Pending),
        ApprovalOutcome::Approved => Some(WatchEventKind::Approved),
        ApprovalOutcome::Rejected => Some(WatchEventKind::Rejected),
        ApprovalOutcome::Expired => Some(WatchEventKind::Expired),
        // Used by the call it authorized, so nobody can still decide it.
        ApprovalOutcome::Consumed => None,
    }
}

fn matches_client(execution: &PendingExecution, client_id: Option<&str>) -> bool {
    client_id.is_none_or(|client| execution.plan.actor_id == client)
}

/// Events the stream mode emits for one poll. `tracked` holds the requests
/// already announced as pending and still undecided; it starts empty, so
/// requests that were pending when the watcher started are announced too.
pub fn poll_events(
    store: &dyn PendingExecutionStore,
    tracked: &mut HashSet<Uuid>,
    client_id: Option<&str>,
    now_ms: i64,
) -> Result<Vec<WatchEvent>, PendingStoreError> {
    let mut events = Vec::new();
    let pending = store.list_pending()?;
    let still_pending: HashSet<Uuid> = pending.iter().map(|execution| execution.id).collect();

    for execution in pending
        .iter()
        .filter(|execution| matches_client(execution, client_id))
    {
        if tracked.insert(execution.id) {
            events.push(WatchEvent::for_execution(
                WatchEventKind::Pending,
                execution,
            ));
        }
    }

    // A request that left the pending list was decided, expired, or purged.
    let left: Vec<Uuid> = tracked.difference(&still_pending).copied().collect();
    for id in left {
        let Some(execution) = store.get_execution(id)? else {
            tracked.remove(&id);
            continue;
        };

        match event_kind(classify_execution(&execution, now_ms)) {
            Some(WatchEventKind::Pending) => {}
            Some(kind) => {
                tracked.remove(&id);
                events.push(WatchEvent::for_execution(kind, &execution));
            }
            None => {
                tracked.remove(&id);
            }
        }
    }

    Ok(events)
}

/// The event that describes `pending_id` now, as `--pending-id` mode reports
/// it. A request of another client, an unknown id and a used one are all
/// `not_found`.
pub fn single_event(
    store: &dyn PendingExecutionStore,
    pending_id: &str,
    client_id: Option<&str>,
    now_ms: i64,
) -> Result<WatchEvent, PendingStoreError> {
    let not_found = || WatchEvent {
        kind: WatchEventKind::NotFound,
        pending_id: pending_id.to_string(),
        execution: None,
    };

    let Ok(id) = pending_id.parse::<Uuid>() else {
        return Ok(not_found());
    };

    let Some(execution) = store
        .get_execution(id)?
        .filter(|execution| matches_client(execution, client_id))
    else {
        return Ok(not_found());
    };

    Ok(match event_kind(classify_execution(&execution, now_ms)) {
        Some(kind) => WatchEvent::for_execution(kind, &execution),
        None => not_found(),
    })
}

#[derive(Debug, Clone)]
pub struct WatchOptions {
    pub pending_id: Option<String>,
    pub client_id: Option<String>,
    pub timeout: Option<Duration>,
    pub poll_interval: Duration,
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Writes `line` and flushes, so a reader sees it at once. `Err` carries the
/// exit code to stop with.
fn emit(out: &mut dyn Write, line: &Value) -> Result<(), i32> {
    writeln!(out, "{line}")
        .and_then(|()| out.flush())
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::BrokenPipe {
                // The reader went away; there is nobody left to tell.
                EXIT_OK
            } else {
                eprintln!("dbflux mcp approvals watch: failed to write output: {error}");
                EXIT_FAILURE
            }
        })
}

/// Runs the watcher against `store`, writing JSON lines to `out`, and returns
/// the process exit code.
pub fn run_watch(
    store: &dyn PendingExecutionStore,
    options: &WatchOptions,
    out: &mut dyn Write,
) -> i32 {
    let result = match &options.pending_id {
        Some(pending_id) => watch_single(store, pending_id, options, out),
        None => watch_stream(store, options, out),
    };

    result.unwrap_or_else(|code| code)
}

fn timed_out(started: Instant, timeout: Option<Duration>) -> bool {
    timeout.is_some_and(|timeout| started.elapsed() >= timeout)
}

fn sleep_until_next_poll(started: Instant, options: &WatchOptions) {
    let wait = match options.timeout {
        Some(timeout) => options
            .poll_interval
            .min(timeout.saturating_sub(started.elapsed())),
        None => options.poll_interval,
    };
    std::thread::sleep(wait);
}

fn store_failure(error: PendingStoreError) -> i32 {
    eprintln!("dbflux mcp approvals watch: failed to read the approval store: {error}");
    EXIT_FAILURE
}

fn watch_single(
    store: &dyn PendingExecutionStore,
    pending_id: &str,
    options: &WatchOptions,
    out: &mut dyn Write,
) -> Result<i32, i32> {
    let started = Instant::now();

    loop {
        let event = single_event(
            store,
            pending_id,
            options.client_id.as_deref(),
            now_epoch_ms(),
        )
        .map_err(store_failure)?;

        match event.kind {
            WatchEventKind::Pending => {}
            WatchEventKind::NotFound => {
                emit(out, &event.to_json())?;
                return Ok(EXIT_FAILURE);
            }
            _ => {
                emit(out, &event.to_json())?;
                return Ok(EXIT_OK);
            }
        }

        if timed_out(started, options.timeout) {
            emit(
                out,
                &json!({ "event": "still_pending", "pending_id": pending_id }),
            )?;
            return Ok(EXIT_STILL_PENDING);
        }

        sleep_until_next_poll(started, options);
    }
}

fn watch_stream(
    store: &dyn PendingExecutionStore,
    options: &WatchOptions,
    out: &mut dyn Write,
) -> Result<i32, i32> {
    let started = Instant::now();
    let mut tracked = HashSet::new();

    loop {
        let events = poll_events(
            store,
            &mut tracked,
            options.client_id.as_deref(),
            now_epoch_ms(),
        )
        .map_err(store_failure)?;

        for event in &events {
            emit(out, &event.to_json())?;
        }

        if timed_out(started, options.timeout) {
            return Ok(EXIT_OK);
        }

        sleep_until_next_poll(started, options);
    }
}
