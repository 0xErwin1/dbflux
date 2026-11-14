use std::future::Future;
use std::time::Duration;

use dbflux_core::{CancelToken, Connection, PipelineState, StateWatcher};
use tokio::task::JoinHandle;

use crate::error_messages;

/// Budget for a pipeline stage that does not wait on a human (auth refresh, value
/// resolution, opening an SSM tunnel).
const STAGE_BUDGET: Duration = Duration::from_secs(30);

/// An SSO login waits for the user to finish the flow in a browser.
const LOGIN_BUDGET: Duration = Duration::from_secs(300);

/// Driver connects run their own network handshake and may include server-side
/// authentication, so they get more room than an ordinary stage.
const DRIVER_CONNECT_BUDGET: Duration = Duration::from_secs(60);

/// How long each part of an MCP connect may stall before the call fails.
#[derive(Debug, Clone)]
pub(crate) struct ConnectDeadlines {
    pub(crate) stage: Duration,
    pub(crate) login: Duration,
    pub(crate) driver_connect: Duration,
}

impl Default for ConnectDeadlines {
    fn default() -> Self {
        Self {
            stage: STAGE_BUDGET,
            login: LOGIN_BUDGET,
            driver_connect: DRIVER_CONNECT_BUDGET,
        }
    }
}

impl ConnectDeadlines {
    fn budget_for(&self, state: &PipelineState) -> Duration {
        match state {
            PipelineState::WaitingForLogin { .. } => self.login,
            _ => self.stage,
        }
    }
}

/// Runs `future` until it finishes, failing when the pipeline stays in one
/// `PipelineState` longer than that state's budget.
///
/// The budget restarts on every state change, so a pipeline that keeps making
/// progress is never cut off. On timeout the token is cancelled and the task is
/// aborted, which drops anything the pipeline had already opened.
pub(crate) async fn run_with_stage_deadline<F, T>(
    future: F,
    mut state_watcher: StateWatcher,
    deadlines: &ConnectDeadlines,
    cancel: &CancelToken,
) -> Result<T, String>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // Spawned so the deadline still fires when a stage blocks its worker thread.
    let mut task = tokio::spawn(future);
    let mut watching = true;

    loop {
        let state = state_watcher.borrow_and_update().clone();
        let budget = deadlines.budget_for(&state);
        let state_changed = async {
            if watching {
                state_watcher.changed().await.is_ok()
            } else {
                std::future::pending().await
            }
        };

        tokio::select! {
            joined = &mut task => {
                return joined
                    .map_err(|error| format!("Connection pipeline task failed: {error}"));
            }
            changed = state_changed => {
                // A dropped sender means no more stage updates: keep timing the
                // last one until the task finishes.
                watching = changed;
            }
            () = tokio::time::sleep(budget) => {
                cancel.cancel();
                task.abort();
                return Err(error_messages::connect_stage_timeout(budget, &state.to_string()));
            }
        }
    }
}

/// Runs a blocking driver connect, failing once it takes longer than `budget`.
///
/// A blocking connect cannot be interrupted, so its thread keeps running after
/// the timeout; a connection it produces late is closed instead of leaked.
pub(crate) async fn connect_with_deadline<F>(
    connect: F,
    budget: Duration,
    driver_name: &str,
) -> Result<Box<dyn Connection>, String>
where
    F: FnOnce() -> Result<Box<dyn Connection>, String> + Send + 'static,
{
    let mut task = tokio::task::spawn_blocking(connect);

    match tokio::time::timeout(budget, &mut task).await {
        Ok(joined) => joined.map_err(|error| format!("Blocking task failed: {error}"))?,
        Err(_elapsed) => {
            tokio::spawn(close_late_connection(task, driver_name.to_string()));
            let stage = PipelineState::Connecting {
                driver_name: driver_name.to_string(),
            };
            Err(error_messages::connect_stage_timeout(
                budget,
                &stage.to_string(),
            ))
        }
    }
}

async fn close_late_connection(
    task: JoinHandle<Result<Box<dyn Connection>, String>>,
    driver_name: String,
) {
    let mut connection = match task.await {
        Ok(Ok(connection)) => connection,
        Ok(Err(_connect_error)) => return,
        Err(error) => {
            log::warn!("Timed-out {driver_name} connect task failed: {error}");
            return;
        }
    };

    let closed =
        tokio::task::spawn_blocking(move || connection.close().map_err(|error| error.to_string()));
    match closed.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            log::warn!("Failed to close timed-out {driver_name} connection: {error}");
        }
        Err(error) => {
            log::warn!("Closing timed-out {driver_name} connection failed: {error}");
        }
    }
}

#[cfg(test)]
mod tests;
