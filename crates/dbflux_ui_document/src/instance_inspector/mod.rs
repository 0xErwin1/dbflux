//! `InspectorPanel` — a tabular live-view document that executes an
//! `InstanceInspectorQuery` against the connection and displays the result
//! as a data grid (e.g. process list, active sessions).
//!
//! Unlike `ChartDocument`, `InspectorPanel` issues queries with no time window.
//! The source context is always `ExecutionSourceContext::InstanceInspectorQuery`.

pub mod pane;

use super::DataGridPanel;
use super::data_grid_panel::DataGridEvent;
use super::handle::DocumentEvent;
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use crate::refresh::MIN_REFRESH_FLOOR_SECS;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text, overlay_bg, surface_panel};
use dbflux_components::result_panel::{ResultPanel, ViewHandle};
use dbflux_components::tokens::{Radii, Spacing};
use dbflux_core::{
    ExecutionContext, ExecutionSourceContext, QueryRequest, QueryResult, RefreshPolicy, Value,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::AsyncUpdateResultExt;
use gpui::prelude::*;
use gpui::{App, Context, Entity, EventEmitter, FocusHandle, Subscription, Task, Window};
use gpui_component::ActiveTheme;
use std::sync::Arc;
use uuid::Uuid;

/// A pending query result from the background task runner.
struct PendingResult {
    task_id: dbflux_core::TaskId,
    result: Result<QueryResult, dbflux_core::DbError>,
}

/// State held while the kill-confirmation modal is visible.
struct PendingKillConfirm {
    action_id: String,
    action_label: String,
    row_values: Vec<Value>,
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

    /// `DataGridPanel` entity that owns the rendered result table.
    ///
    /// Instantiated lazily on the first render after a successful fetch, because
    /// `DataGridPanel::new_for_result` requires a `Window` reference (for its
    /// `InputState` fields) that is only available during `render()`.
    data_grid: Option<Entity<DataGridPanel>>,

    /// Buffered result waiting for the first render to create `data_grid`.
    ///
    /// Written by `apply_result` when no grid exists yet; consumed and cleared
    /// by `render()` which uses it to construct `DataGridPanel::new_for_result`.
    pending_grid_result: Option<Arc<QueryResult>>,

    /// Chrome-row host built lazily alongside `data_grid` on first render.
    pub(super) result_panel: Option<Entity<ResultPanel>>,

    /// Subscription kept alive while `data_grid` exists, routing
    /// `DataGridEvent::RowActionRequested` events to this panel.
    _data_grid_subscription: Option<Subscription>,

    /// State while the kill-confirmation modal is visible.
    pending_kill_confirm: Option<PendingKillConfirm>,
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
            data_grid: None,
            pending_grid_result: None,
            result_panel: None,
            _data_grid_subscription: None,
            pending_kill_confirm: None,
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

    /// Returns `true` if the panel has received at least one successful result.
    #[cfg(test)]
    pub(crate) fn has_result(&self) -> bool {
        self.result.is_some()
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

                let arc = Arc::new(result);

                if let Some(grid) = &self.data_grid {
                    grid.update(cx, |g, cx| g.set_result((*arc).clone(), cx));
                } else {
                    self.pending_grid_result = Some(arc.clone());
                }

                self.result = Some(arc);
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

impl InspectorPanel {
    fn cancel_kill_action(&mut self, cx: &mut Context<Self>) {
        self.pending_kill_confirm = None;
        cx.notify();
    }

    fn confirm_kill_action(&mut self, cx: &mut Context<Self>) {
        let Some(confirm) = self.pending_kill_confirm.take() else {
            return;
        };

        let profile_id = self.profile_id;
        let metric_id = self.metric_id.clone();
        let action_id = confirm.action_id.clone();
        let row_values = confirm.row_values.clone();
        let action_label = confirm.action_label.clone();
        let audit_service = self.app_state.read(cx).audit_service().clone();

        let connection: Option<Arc<dyn dbflux_core::Connection>> = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|c| c.resolve_connection_for_execution(None).ok());

        cx.notify();

        let Some(conn) = connection else {
            log::warn!(
                "[inspector kill] connection not found for profile {}",
                profile_id
            );
            return;
        };

        cx.spawn(async move |_this, _cx| {
            let catalog = conn.instance_catalog();
            let result = match catalog {
                Some(cat) => {
                    cat.execute_row_action(&metric_id, &action_id, &row_values)
                        .await
                }
                None => Err(dbflux_core::DbError::NotSupported(
                    "driver does not support instance catalog".to_string(),
                )),
            };

            emit_kill_audit(
                &audit_service,
                &metric_id,
                &action_id,
                &action_label,
                profile_id,
                &result,
            );

            if let Err(e) = result {
                log::warn!("[inspector kill] row action '{}' failed: {}", action_id, e);
            }
        })
        .detach();
    }

    fn render_kill_confirm_modal(
        &self,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        use gpui::div;

        let btn_hover = theme.muted;

        let action_label = self
            .pending_kill_confirm
            .as_ref()
            .map(|c| c.action_label.clone())
            .unwrap_or_else(|| "Kill".to_string());

        let title = format!("{}?", action_label);
        let description =
            "This action will terminate the selected operation. It cannot be undone.".to_string();

        div()
            .id("kill-confirm-overlay")
            .absolute()
            .inset_0()
            .bg(overlay_bg(theme))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(gpui::MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .child(
                surface_panel(cx)
                    .rounded(Radii::MD)
                    .min_w(gpui::px(300.0))
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .p(Spacing::MD)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                Icon::new(AppIcon::TriangleAlert)
                                    .medium()
                                    .color(theme.warning),
                            )
                            .child(Text::heading(title)),
                    )
                    .child(Text::muted(description))
                    .child(
                        div()
                            .flex()
                            .justify_end()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .id("kill-cancel-btn")
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .px(Spacing::SM)
                                    .py(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.secondary)
                                    .hover(|d| d.bg(btn_hover))
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.cancel_kill_action(cx);
                                    }))
                                    .child(
                                        Icon::new(AppIcon::X).small().color(theme.muted_foreground),
                                    )
                                    .child(Text::caption("Cancel")),
                            )
                            .child(
                                div()
                                    .id("kill-confirm-btn")
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .px(Spacing::SM)
                                    .py(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.danger)
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(|this, _, _window, cx| {
                                        this.confirm_kill_action(cx);
                                    }))
                                    .child(
                                        Icon::new(AppIcon::Delete).small().color(theme.background),
                                    )
                                    .child(Text::caption("Confirm").color(theme.background)),
                            ),
                    ),
            )
    }
}

impl Render for InspectorPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::div;

        if let Some(pending) = self.pending_result.take() {
            self.apply_result(pending, cx);
        }

        // Lazily construct the DataGridPanel + ResultPanel when the first
        // successful result arrives. Both require a Window reference (for
        // InputState / TimeRangePanel internals), so they cannot be created
        // outside render().
        if self.data_grid.is_none() && self.pending_grid_result.is_some() {
            let result = self.pending_grid_result.take().unwrap();
            let profile_id = self.profile_id;
            let app_state = self.app_state.clone();
            let metric_id = self.metric_id.clone();

            // Capture a connection clone for the row-action provider.
            let connection_for_actions: Option<Arc<dyn dbflux_core::Connection>> = self
                .app_state
                .read(cx)
                .connections()
                .get(&profile_id)
                .and_then(|c| c.resolve_connection_for_execution(None).ok());

            let grid = cx.new(|cx| {
                let mut panel = DataGridPanel::new_for_result(
                    result,
                    metric_id.clone(),
                    Some(profile_id),
                    app_state,
                    window,
                    cx,
                );

                if let Some(conn) = connection_for_actions {
                    panel.set_row_action_provider(Arc::new(move |mid| {
                        conn.instance_catalog()
                            .map(|cat| cat.row_actions(mid))
                            .unwrap_or_default()
                    }));
                }

                panel
            });

            let subscription = cx.subscribe(&grid, |this, _grid, event: &DataGridEvent, cx| {
                if let DataGridEvent::RowActionRequested {
                    action_id,
                    action_label,
                    row_values,
                    ..
                } = event
                {
                    this.pending_kill_confirm = Some(PendingKillConfirm {
                        action_id: action_id.clone(),
                        action_label: action_label.clone(),
                        row_values: row_values.clone(),
                    });
                    cx.notify();
                }
            });

            let view_handle = DataGridPanel::into_view_handle(grid.clone(), cx);
            let panel = cx.new(|cx| ResultPanel::new(view_handle, cx));

            self.data_grid = Some(grid);
            self.result_panel = Some(panel);
            self._data_grid_subscription = Some(subscription);
        }

        let focus_handle = self.focus_handle.clone();

        if let Some(result_panel) = self.result_panel.as_ref().cloned() {
            let theme = cx.theme().clone();
            let has_kill_confirm = self.pending_kill_confirm.is_some();

            return div()
                .size_full()
                .relative()
                .track_focus(&focus_handle)
                .child(result_panel)
                .when(has_kill_confirm, |d| {
                    d.child(self.render_kill_confirm_modal(&theme, cx))
                })
                .into_any();
        }

        // No result yet — show a loading or error placeholder.
        let msg = if let Some(err) = &self.last_error {
            format!("Error: {err}")
        } else if self.state == DocumentState::Loading {
            "Loading…".to_string()
        } else {
            "No data. Connect and click Refresh.".to_string()
        };

        div()
            .size_full()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .text_sm()
            .track_focus(&focus_handle)
            .child(msg)
            .into_any()
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

fn emit_kill_audit(
    audit_service: &dbflux_audit::AuditService,
    metric_id: &str,
    action_id: &str,
    action_label: &str,
    profile_id: Uuid,
    result: &Result<(), dbflux_core::DbError>,
) {
    use dbflux_core::chrono::Utc;
    use dbflux_core::observability::{
        EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
    };

    let (severity, outcome, action_key) = match result {
        Ok(()) => (
            EventSeverity::Info,
            EventOutcome::Success,
            "inspector_row_action",
        ),
        Err(_) => (
            EventSeverity::Error,
            EventOutcome::Failure,
            "inspector_row_action_failed",
        ),
    };

    let summary = match result {
        Ok(()) => format!(
            "Inspector row action '{}' ({}) executed on inspector '{}' (profile {})",
            action_label, action_id, metric_id, profile_id
        ),
        Err(e) => format!(
            "Inspector row action '{}' ({}) failed on '{}' (profile {}): {}",
            action_label, action_id, metric_id, profile_id, e
        ),
    };

    let event = EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::Query,
        outcome,
    )
    .with_action(action_key.to_string())
    .with_summary(summary)
    .with_actor_id("ui:user");

    if let Err(e) = audit_service.record(event) {
        log::warn!("[inspector kill] failed to record audit event: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// BF4: after `apply_result` is called with a successful `QueryResult`,
    /// `has_result()` must return `true` and `pending_grid_result` must be
    /// populated (since no `data_grid` exists yet in a non-rendered panel).
    ///
    /// Uses `#[gpui::test]` because `apply_result` calls
    /// `runner.complete_primary` which requires a GPUI `App` context.
    #[gpui::test]
    fn apply_result_sets_result_and_pending_grid(cx: &mut gpui::TestAppContext) {
        use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, QueryResultShape};
        use dbflux_storage::bootstrap::StorageRuntime;
        use std::time::Duration;

        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| dbflux_ui_base::toast::ToastHost::new());
            cx.set_global(dbflux_ui_base::toast::ToastGlobal { host });
        });

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = StorageRuntime::in_memory().expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime)
            })
        });

        let profile_id = uuid::Uuid::new_v4();
        let panel: Entity<super::InspectorPanel> = cx.update(|cx| {
            cx.new(|cx| {
                super::InspectorPanel::new(profile_id, "pg.activity".to_string(), app_state, cx)
            })
        });

        let synthetic_result = QueryResult {
            shape: QueryResultShape::Table,
            columns: vec![ColumnMeta {
                name: "pid".to_string(),
                type_name: "int4".to_string(),
                kind: ColumnKind::Integer,
                nullable: true,
                is_primary_key: false,
            }],
            rows: vec![vec![dbflux_core::Value::Int(42)]],
            affected_rows: None,
            execution_time: Duration::ZERO,
            text_body: None,
            raw_bytes: None,
            next_page_token: None,
            resolved_window: None,
            metadata_extra: None,
            additional_results: Vec::new(),
        };

        cx.update(|cx| {
            panel.update(cx, |p, cx| {
                let task_id = uuid::Uuid::nil();
                p.apply_result(
                    super::PendingResult {
                        task_id,
                        result: Ok(synthetic_result),
                    },
                    cx,
                );
            });
        });

        cx.update(|cx| {
            let p = panel.read(cx);
            assert!(
                p.has_result(),
                "apply_result with Ok must set result to Some"
            );
            assert!(
                p.pending_grid_result.is_some(),
                "apply_result must populate pending_grid_result when data_grid is None"
            );
        });
    }
}
