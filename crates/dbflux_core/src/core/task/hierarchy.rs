//! Parent/child tasks: cascade cancel and how a parent settles.
//!
//! Rules:
//! - Cancelling a task first cancels every non-terminal descendant,
//!   depth-first, passing through terminal intermediates. `Pending`
//!   descendants become `Skipped`. A direct cancel of a terminal task is a
//!   no-op that leaves its subtree alone.
//! - A child registered under a cancelling or cancelled parent inherits the
//!   cancellation (see `TaskRequest::parent`).
//! - A parent whose own cancel would read `Cancelled` while a descendant is still
//!   `Running` or `Cancelling` (a descendant that cannot stop, or is waiting for
//!   its worker) becomes `Cancelling` instead, and turns `Cancelled` by
//!   itself once every descendant is terminal.
//! - `acknowledge_cancelled` on a parent is refused while a descendant is in
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

    /// Whether any task below `id` is in flight, however many terminal
    /// intermediates lie between them. A finished parent whose child still
    /// runs (a detached hook that outlives its connect) must stay listed.
    pub fn has_in_flight_descendants(&self, id: TaskId) -> bool {
        let mut pending = vec![id];

        while let Some(current) = pending.pop() {
            for task in self.tasks.values() {
                if task.parent_id != Some(current) {
                    continue;
                }

                if task.status.is_in_flight() {
                    return true;
                }
                pending.push(task.id);
            }
        }

        false
    }

    /// Applies the honest-parent rule after the parent's own cancel request.
    pub(super) fn reconcile_with_children(
        &mut self,
        id: TaskId,
        own: CancelRequestOutcome,
    ) -> CancelRequestOutcome {
        if own != CancelRequestOutcome::Cancelled || !self.has_in_flight_descendants(id) {
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
            if self.has_in_flight_descendants(parent_id) {
                return;
            }

            let Some(parent) = self.tasks.get_mut(&parent_id) else {
                return;
            };

            if parent.status == TaskStatus::Cancelling && parent.settles_with_children {
                parent.finish(TaskStatus::Cancelled);
            } else if !parent.status.is_terminal() {
                return;
            }

            current = parent.parent_id;
        }
    }
}
