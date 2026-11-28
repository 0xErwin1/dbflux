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
//! - A child registered below a completed intermediate of a cancelled ancestor
//!   inherits the cancellation too: the whole chain is checked.
//! - `acknowledge_cancelled` on a parent while a descendant is in flight
//!   records the confirmation and keeps the parent `Cancelling`; the last
//!   descendant to stop settles it. A parent that never started (`Pending`)
//!   settles as `Skipped`, not `Cancelled`.
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
    /// intermediates lie between them.
    pub(super) fn has_in_flight_descendants(&self, id: TaskId) -> bool {
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

    /// Whether `parent` or any task above it was cancelled, however many
    /// terminal intermediates lie between them.
    pub(super) fn has_cancelled_ancestor(&self, parent: Option<TaskId>) -> bool {
        let mut current = parent;
        let mut remaining = self.tasks.len();

        while let Some(id) = current {
            let Some(task) = self.tasks.get(&id) else {
                return false;
            };

            if matches!(
                task.status,
                TaskStatus::Cancelling | TaskStatus::Cancelled | TaskStatus::Skipped
            ) {
                return true;
            }

            if remaining == 0 {
                return false;
            }
            remaining -= 1;
            current = task.parent_id;
        }

        false
    }

    /// Applies the honest-parent rule after the parent's own cancel request.
    /// `was_pending` keeps a stage that never started reading `Skipped` once
    /// its children stopped.
    pub(super) fn reconcile_with_children(
        &mut self,
        id: TaskId,
        own: CancelRequestOutcome,
        was_pending: bool,
    ) -> CancelRequestOutcome {
        if own != CancelRequestOutcome::Cancelled || !self.has_in_flight_descendants(id) {
            return own;
        }

        if let Some(task) = self.tasks.get_mut(&id) {
            task.status = TaskStatus::Cancelling;
            task.completed_at = None;
            task.settles_with_children = true;
            if was_pending {
                task.settles_as = TaskStatus::Skipped;
            }
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
                let settled = parent.settles_as.clone();
                parent.finish(settled);
            } else if !parent.status.is_terminal() {
                return;
            }

            current = parent.parent_id;
        }
    }
}
