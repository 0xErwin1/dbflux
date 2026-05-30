//! `InspectorPanel` — a tabular live-view document that executes an
//! `InstanceInspectorQuery` against the connection and displays the result
//! as a data grid (e.g. process list, active sessions).
//!
//! Unlike `ChartDocument`, `InspectorPanel` issues queries with no time window.
//! The source context is always `ExecutionSourceContext::InstanceInspectorQuery`.

pub mod pane;

use super::handle::DocumentEvent;
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use crate::refresh::MIN_REFRESH_FLOOR_SECS;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::result_panel::ResultPanel;
use dbflux_core::{
    ExecutionContext, ExecutionSourceContext, QueryRequest, QueryResult, RefreshPolicy,
};
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::{App, Context, Entity, EventEmitter, FocusHandle, Task, Window};
use std::sync::Arc;
use uuid::Uuid;

/// A pending query result from the background task runner.
struct PendingResult {
    task_id: dbflux_core::TaskId,
    result: Result<QueryResult, dbflux_core::DbError>,
}

/// Live tabular inspector panel tied to a specific instance-metric inspector.
///
/// Executes `InstanceInspectorQuery` against the profile's connection and renders
/// the result as a data grid. No time-window fields are used — inspectors always
/// reflect the live state of the database.
pub struct InspectorPanel {
    id: DocumentId,
    state: DocumentState,

    profile_id: Uuid,
    metric_id: String,

    result: Option<Arc<QueryResult>>,
    last_error: Option<String>,

    refresh_policy: RefreshPolicy,
    _timer: Option<Task<()>>,

    runner: DocumentTaskRunner,
    app_state: Entity<AppStateEntity>,
    pending_result: Option<PendingResult>,

    focus_handle: FocusHandle,

    #[allow(dead_code)]
    pub(super) result_panel: Option<Entity<ResultPanel>>,
}

impl EventEmitter<DocumentEvent> for InspectorPanel {}

impl InspectorPanel {
    pub fn new(
        profile_id: Uuid,
        metric_id: String,
        app_state: Entity<AppStateEntity>,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut runner = DocumentTaskRunner::new(app_state.clone());
        runner.set_profile_id(profile_id);

        Self {
            id: DocumentId::new(),
            state: DocumentState::Clean,
            profile_id,
            metric_id,
            result: None,
            last_error: None,
            refresh_policy: RefreshPolicy::Manual,
            _timer: None,
            runner,
            app_state,
            pending_result: None,
            focus_handle: cx.focus_handle(),
            result_panel: None,
        }
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.metric_id.clone()
    }

    pub fn state(&self) -> DocumentState {
        self.state
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        Some(self.profile_id)
    }

    pub fn active_context(&self) -> ContextId {
        ContextId::Global
    }

    pub fn change_summary(&self, _cx: &App) -> Option<String> {
        None
    }

    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        self.refresh_policy = policy;
        self.update_timer(cx);
    }

    pub fn set_active_tab(&mut self, _active: bool) {}

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    pub fn dispatch_command(
        &mut self,
        _cmd: Command,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> bool {
        false
    }

    pub fn metric_id(&self) -> &str {
        &self.metric_id
    }

    /// Schedule a fresh inspector snapshot fetch.
    ///
    /// Builds an `InstanceInspectorQuery` execution context (no time window)
    /// and dispatches through the connection. The query text is empty — the
    /// driver dispatches based solely on the source context variant.
    pub fn request_reexec(&mut self, cx: &mut Context<Self>) {
        let Some(profile_id) = Some(self.profile_id) else {
            return;
        };

        let conn = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                self.last_error = Some("Connection not found".to_string());
                self.state = DocumentState::Error;
                cx.notify();
                return;
            };
            match connected.resolve_connection_for_execution(None) {
                Ok(c) => c,
                Err(e) => {
                    self.last_error = Some(format!("Connection error: {:?}", e));
                    self.state = DocumentState::Error;
                    cx.notify();
                    return;
                }
            }
        };

        let request = build_inspector_request(&self.metric_id);
        let (task_id, cancel_token) = self.runner.start_primary(
            dbflux_core::TaskKind::Query,
            format!("Inspector: {}", self.metric_id),
            cx,
        );

        self.state = DocumentState::Loading;
        cx.notify();

        let conn_cleanup = conn.clone();
        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&request) });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    if let Err(e) = conn_cleanup.cleanup_after_cancel() {
                        log::warn!("[inspector] cleanup after cancel failed: {}", e);
                    }
                    return;
                }

                let Some(entity) = this.upgrade() else { return };
                entity.update(cx, |panel, cx| {
                    panel.pending_result = Some(PendingResult { task_id, result });
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    fn apply_result(&mut self, pending: PendingResult, cx: &mut Context<Self>) {
        self.runner.complete_primary(pending.task_id, cx);

        match pending.result {
            Ok(result) => {
                self.state = DocumentState::Clean;
                self.last_error = None;
                self.result = Some(Arc::new(result));
            }
            Err(err) => {
                self.state = DocumentState::Error;
                self.last_error = Some(err.to_string());
            }
        }

        cx.notify();
    }

    fn update_timer(&mut self, cx: &mut Context<Self>) {
        self._timer = None;

        let duration = match self.refresh_policy {
            RefreshPolicy::Manual => return,
            RefreshPolicy::Interval { every_secs } => {
                let clamped = every_secs.max(MIN_REFRESH_FLOOR_SECS as u32);
                std::time::Duration::from_secs(clamped as u64)
            }
        };

        self._timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(duration).await;

                let _ = cx.update(|cx| {
                    let Some(entity) = this.upgrade() else {
                        return;
                    };
                    entity.update(cx, |panel, cx| {
                        panel.request_reexec(cx);
                    });
                });
            }
        }));
    }
}

impl Render for InspectorPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::div;

        if let Some(pending) = self.pending_result.take() {
            self.apply_result(pending, cx);
        }

        div()
            .size_full()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .text_sm()
            .child(format!("Inspector: {}", self.metric_id))
    }
}

/// Build the `ExecutionSourceContext` for an instance inspector snapshot.
///
/// Always constructs `InstanceInspectorQuery` — no time-window fields are
/// present on this variant by design, ensuring callers cannot accidentally
/// inject time bounds.
pub(crate) fn build_inspector_context(metric_id: &str) -> ExecutionSourceContext {
    ExecutionSourceContext::InstanceInspectorQuery {
        metric_id: metric_id.to_string(),
    }
}

/// Build a `QueryRequest` that dispatches an `InstanceInspectorQuery`.
///
/// The `sql` field is empty — the driver routes exclusively on the execution
/// source context variant.
fn build_inspector_request(metric_id: &str) -> QueryRequest {
    QueryRequest {
        execution_context: Some(ExecutionContext {
            source: Some(build_inspector_context(metric_id)),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use dbflux_core::ExecutionSourceContext;

    /// REQ-DOC-3, REQ-UI-2: `build_inspector_context` must produce an
    /// `InstanceInspectorQuery` with exactly the given `metric_id` and no
    /// time-window fields.
    #[test]
    fn build_inspector_context_produces_instance_inspector_query() {
        let metric_id = "pg.activity".to_string();

        let ctx = super::build_inspector_context(&metric_id);

        match ctx {
            ExecutionSourceContext::InstanceInspectorQuery { metric_id: ref id } => {
                assert_eq!(
                    id, &metric_id,
                    "metric_id must round-trip through the context"
                );
            }
            other => panic!("expected InstanceInspectorQuery, got {:?}", other),
        }
    }

    /// REQ-DOC-3: The inspector context must NOT carry `start_ms` or `end_ms`
    /// fields. This test pins the structural invariant at the type level by
    /// attempting to destructure with those fields (which must not compile).
    #[test]
    fn inspector_query_context_has_no_time_window_fields() {
        let ctx = ExecutionSourceContext::InstanceInspectorQuery {
            metric_id: "pg.locks".to_string(),
        };

        let ExecutionSourceContext::InstanceInspectorQuery { metric_id } = ctx else {
            panic!("expected InstanceInspectorQuery");
        };

        assert_eq!(
            metric_id, "pg.locks",
            "metric_id must match the constructed value"
        );
    }
}
