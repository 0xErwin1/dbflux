//! Supersede by key and atomic dedup by target.
//!
//! Same-kind rule for dedup: two kinds are in the same group when they are
//! equal, except `TaskKind::Hook { .. }`, where every phase is one group
//! (a profile runs one hook phase at a time). Only tasks in flight (`Running`
//! or `Cancelling`) count; `Pending` stages never block a start, and a
//! request without a target or a `pending()` request is never refused.
//!
//! `try_start` checks for a busy target first; a request that also carries a
//! `supersede_key` supersedes only after that check passes.

use std::fmt;

use super::{TaskHandle, TaskId, TaskKind, TaskManager, TaskRequest, TaskTarget};

/// A start refused because a task of the same kind and target is in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskBusy {
    pub existing: TaskId,
}

impl fmt::Display for TaskBusy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "task {} is already running for this target",
            self.existing
        )
    }
}

impl std::error::Error for TaskBusy {}

fn same_kind_group(left: TaskKind, right: TaskKind) -> bool {
    match (left, right) {
        (TaskKind::Hook { .. }, TaskKind::Hook { .. }) => true,
        _ => left == right,
    }
}

impl TaskManager {
    /// Starts the task unless one of the same kind group and target is in
    /// flight; the check and the registration happen under one `&mut self`.
    pub fn try_start(&mut self, request: TaskRequest) -> Result<TaskHandle, TaskBusy> {
        if !request.pending
            && let Some(target) = &request.target
            && let Some(existing) = self.busy_task(request.kind, target)
        {
            return Err(TaskBusy { existing });
        }

        Ok(self.start_handle(request))
    }

    /// Whether a task of `kind`'s group is in flight for `target`.
    pub fn is_busy(&self, kind: TaskKind, target: &TaskTarget) -> bool {
        self.busy_task(kind, target).is_some()
    }

    fn busy_task(&self, kind: TaskKind, target: &TaskTarget) -> Option<TaskId> {
        self.tasks
            .values()
            .filter(|task| task.status.is_in_flight())
            .filter(|task| task.target.as_ref() == Some(target))
            .filter(|task| same_kind_group(task.kind, kind))
            .min_by_key(|task| task.sequence)
            .map(|task| task.id)
    }

    /// Requests cancel on every non-terminal task with `key`, oldest first.
    pub(super) fn supersede_matching(&mut self, key: Option<&str>) -> Vec<TaskId> {
        let Some(key) = key else {
            return Vec::new();
        };

        let mut older: Vec<_> = self
            .tasks
            .values()
            .filter(|task| !task.status.is_terminal())
            .filter(|task| task.supersede_key.as_deref() == Some(key))
            .map(|task| (task.sequence, task.id))
            .collect();
        older.sort_unstable();

        let ids: Vec<TaskId> = older.into_iter().map(|(_, id)| id).collect();
        for id in &ids {
            self.request_cancel(*id);
        }
        ids
    }
}
