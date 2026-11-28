//! Test-only stand-in for work that owns a connection or database target.

use dbflux_core::{CancelPolicy, TaskHandle, TaskKind, TaskRequest, TaskTarget};
use dbflux_ui_base::AppStateEntity;

/// Starts a task that claims `target`, as a connect or a database switch
/// does, without the duplicate check: tests use it to stand for a claim that
/// another session or another component holds. Settle it through the task
/// manager (`complete_task`) to release the claim.
pub(crate) fn claim_target(state: &mut AppStateEntity, target: TaskTarget) -> TaskHandle {
    state.start_task_handle(
        TaskRequest::new(TaskKind::SwitchDatabase, "test claim")
            .target(target)
            .policy(CancelPolicy::WorkerAcknowledged)
            .claims_target(),
    )
}
