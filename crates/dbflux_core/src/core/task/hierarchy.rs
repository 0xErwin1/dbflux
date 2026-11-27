//! Parent/child tasks: cascade cancel and how a parent settles.
//!
//! Rules:
//! - Cancelling a task first cancels every non-terminal child, depth-first.
//!   `Pending` children become `Skipped`.
//! - A parent whose own cancel would read `Cancelled` while a child is still
//!   `Running` or `Cancelling` (a child that cannot stop, or is waiting for
//!   its worker) becomes `Cancelling` instead, and turns `Cancelled` by
//!   itself once every child is terminal.
//! - `acknowledge_cancelled` on a parent is refused while a child is in
//!   flight.
//! - Completing or failing a parent never touches its children: they finish
//!   on their own.

use super::{CancelRequestOutcome, TaskId, TaskManager, TaskSnapshot, TaskStatus};

impl TaskManager {
    /// Children of `id` in start order, whatever their status.
    pub fn children_of(&self, id: TaskId) -> Vec<TaskSnapshot> {
        self.child_ids(id)
            .into_iter()
            .filter_map(|child| self.get(child))
            .collect()
    }

    /// Moves a `Pending` task to `Running`. Returns `false` and changes
    /// nothing for any other status or an unknown task. The elapsed time
    /// counts from here.
    pub fn activate(&mut self, id: TaskId) -> bool {
        match self.tasks.get_mut(&id) {
            Some(task) if task.status == TaskStatus::Pending => {
                task.status = TaskStatus::Running;
                task.started_at = std::time::Instant::now();
                true
            }
            _ => false,
        }
    }

    pub(super) fn child_ids(&self, id: TaskId) -> Vec<TaskId> {
        let mut children: Vec<_> = self
            .tasks
            .values()
            .filter(|task| task.parent_id == Some(id))
            .map(|task| (task.sequence, task.id))
            .collect();
        children.sort_unstable();
        children.into_iter().map(|(_, child)| child).collect()
    }

    pub(super) fn has_in_flight_children(&self, id: TaskId) -> bool {
        self.tasks
            .values()
            .any(|task| task.parent_id == Some(id) && task.status.is_in_flight())
    }

    /// Applies the honest-parent rule after the parent's own cancel request.
    pub(super) fn reconcile_with_children(
        &mut self,
        id: TaskId,
        own: CancelRequestOutcome,
    ) -> CancelRequestOutcome {
        if own != CancelRequestOutcome::Cancelled || !self.has_in_flight_children(id) {
            return own;
        }

        if let Some(task) = self.tasks.get_mut(&id) {
            task.status = TaskStatus::Cancelling;
            task.completed_at = None;
            task.settles_with_children = true;
        }
        CancelRequestOutcome::Cancelling
    }

    /// Settles the ancestors of a task that just became terminal: a parent
    /// waiting only for its children turns `Cancelled` once none is in flight.
    pub(super) fn settle_ancestors(&mut self, id: TaskId) {
        let mut current = self.tasks.get(&id).and_then(|task| task.parent_id);

        while let Some(parent_id) = current {
            if self.has_in_flight_children(parent_id) {
                return;
            }

            let Some(parent) = self.tasks.get_mut(&parent_id) else {
                return;
            };

            if parent.status != TaskStatus::Cancelling || !parent.settles_with_children {
                return;
            }

            parent.finish(TaskStatus::Cancelled);
            current = parent.parent_id;
        }
    }
}
