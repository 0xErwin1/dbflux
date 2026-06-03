use dbflux_core::{CancelToken, TaskId, TaskKind, TaskSlot, TaskTarget};
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::*;
use uuid::Uuid;

/// A cloneable cancellation handle for a visual mutation execution.
///
/// Wraps the `CancelToken` returned by `start_task_for_target` so that the task
/// manager's cancel path (triggered by the task chip UI) and the executor's poll
/// point (`is_cancelled`) share the same underlying flag. This ensures that
/// `DocumentTaskRunner::cancel_mutation` actually stops the executor loop.
#[derive(Clone)]
pub struct MutationCancelHandle {
    token: CancelToken,
}

impl Default for MutationCancelHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl MutationCancelHandle {
    pub fn new() -> Self {
        Self {
            token: CancelToken::new(),
        }
    }

    /// Build a handle from an existing token (used by `start_mutation` to share
    /// the task manager's cancel flag with the executor loop).
    fn from_token(token: CancelToken) -> Self {
        Self { token }
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }
}

#[cfg(test)]
mod tests {
    // Import only what we need — avoid `use gpui::*` which triggers macro recursion.
    use dbflux_core::CancelToken;

    use super::MutationCancelHandle;

    // T-21 — [RED] Tests for MutationCancelHandle (spec J-1, J-2, DR-15.1)

    #[test]
    fn j1_new_handle_is_not_cancelled() {
        // We test the standalone handle directly since start_mutation requires GPUI context.
        let handle = MutationCancelHandle::new();
        assert!(
            !handle.is_cancelled(),
            "new handle must start as not-cancelled"
        );
    }

    #[test]
    fn j2_cancel_flips_is_cancelled_to_true() {
        let handle = MutationCancelHandle::new();
        assert!(!handle.is_cancelled());
        handle.cancel();
        assert!(handle.is_cancelled());
    }

    #[test]
    fn mutation_cancel_handle_clone_shares_state() {
        let handle = MutationCancelHandle::new();
        let clone = handle.clone();
        handle.cancel();
        assert!(
            clone.is_cancelled(),
            "clone must see cancellation from original"
        );
    }

    // F-R2-2: DR-15.1 — cancellation via the task manager's CancelToken must propagate
    // to the MutationCancelHandle that the executor polls.
    //
    // `start_mutation` now wraps the token returned by `start_task_for_target` into the
    // handle via `from_token`. This test proves the shared-state invariant without
    // requiring a GPUI context: cancelling the original token must flip the handle.
    #[test]
    fn cancel_mutation_via_task_runner_flips_executor_handle() {
        let token = CancelToken::new();
        let handle = MutationCancelHandle::from_token(token.clone());

        assert!(!handle.is_cancelled(), "handle must start as not-cancelled");

        token.cancel();

        assert!(
            handle.is_cancelled(),
            "cancelling the CancelToken must flip is_cancelled() on the handle (DR-15.1)"
        );
    }

    // Inverse: cancelling the handle must also flip the shared token.
    #[test]
    fn cancel_on_handle_also_cancels_shared_token() {
        let token = CancelToken::new();
        let handle = MutationCancelHandle::from_token(token.clone());

        handle.cancel();

        assert!(
            token.is_cancelled(),
            "cancelling the handle must propagate to the shared CancelToken"
        );
    }
}

pub struct DocumentTaskRunner {
    primary: TaskSlot,
    app_state: Entity<AppStateEntity>,
    profile_id: Option<Uuid>,
}

impl DocumentTaskRunner {
    pub fn new(app_state: Entity<AppStateEntity>) -> Self {
        Self {
            primary: TaskSlot::new(),
            app_state,
            profile_id: None,
        }
    }

    pub fn set_profile_id(&mut self, profile_id: Uuid) {
        self.profile_id = Some(profile_id);
    }

    pub fn clear_profile_id(&mut self) {
        self.profile_id = None;
    }

    fn default_target(&self) -> Option<TaskTarget> {
        self.profile_id.map(|profile_id| TaskTarget {
            profile_id,
            database: None,
        })
    }

    // -- Primary slot (reads — auto-cancel-previous) --

    pub fn start_primary(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        self.start_primary_for_target(kind, description, self.default_target(), cx)
    }

    pub fn start_primary_for_target(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        target: Option<TaskTarget>,
        cx: &mut App,
    ) -> (TaskId, CancelToken) {
        let (task_id, cancel_token) = self.app_state.update(cx, |state, _cx| {
            state.start_task_for_target(kind, description, target)
        });

        if let Some(old_id) = self.primary.start(task_id, cancel_token.clone()) {
            self.app_state.update(cx, |state, cx| {
                state.tasks_mut().cancel(old_id);
                cx.emit(AppStateChanged);
            });
        }

        (task_id, cancel_token)
    }

    pub fn complete_primary(&mut self, task_id: TaskId, cx: &mut App) {
        if self.primary.take_if(task_id).is_some() {
            self.app_state.update(cx, |state, cx| {
                state.complete_task(task_id);
                cx.emit(AppStateChanged);
            });
        }
    }

    pub fn fail_primary(&mut self, task_id: TaskId, error: impl Into<String>, cx: &mut App) {
        if self.primary.take_if(task_id).is_some() {
            self.app_state.update(cx, |state, cx| {
                state.fail_task(task_id, error);
                cx.emit(AppStateChanged);
            });
        }
    }

    pub fn cancel_primary(&mut self, cx: &mut App) -> bool {
        if let Some(old_id) = self.primary.cancel() {
            self.app_state.update(cx, |state, cx| {
                state.tasks_mut().cancel(old_id);
                cx.emit(AppStateChanged);
            });
            return true;
        }
        false
    }

    pub fn clear_primary(&mut self, task_id: TaskId) {
        let _ = self.primary.take_if(task_id);
    }

    pub fn is_primary_active(&self) -> bool {
        self.primary.is_active()
    }

    #[allow(dead_code)]
    pub fn primary_token(&self) -> Option<&CancelToken> {
        self.primary.active_token()
    }

    // -- Mutation slot (writes — independent, no auto-cancel) --

    pub fn start_mutation(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        cx: &mut App,
    ) -> (TaskId, MutationCancelHandle) {
        let (task_id, cancel_token) = self.app_state.update(cx, |state, _cx| {
            state.start_task_for_target(kind, description, self.default_target())
        });

        let handle = MutationCancelHandle::from_token(cancel_token);
        (task_id, handle)
    }

    pub fn cancel_mutation(&mut self, task_id: TaskId, cx: &mut App) {
        self.app_state.update(cx, |state, cx| {
            state.tasks_mut().cancel(task_id);
            cx.emit(AppStateChanged);
        });
    }

    pub fn complete_mutation(&mut self, task_id: TaskId, cx: &mut App) {
        self.app_state.update(cx, |state, cx| {
            state.complete_task(task_id);
            cx.emit(AppStateChanged);
        });
    }

    pub fn fail_mutation(&mut self, task_id: TaskId, error: impl Into<String>, cx: &mut App) {
        self.app_state.update(cx, |state, cx| {
            state.fail_task(task_id, error);
            cx.emit(AppStateChanged);
        });
    }
}
