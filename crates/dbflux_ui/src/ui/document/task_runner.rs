use crate::app::{AppStateChanged, AppStateEntity};
use dbflux_core::{CancelToken, TaskId, TaskKind, TaskSlot, TaskTarget};
use gpui::*;
use uuid::Uuid;

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
    ) -> (TaskId, CancelToken) {
        self.app_state.update(cx, |state, _cx| {
            state.start_task_for_target(kind, description, self.default_target())
        })
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
