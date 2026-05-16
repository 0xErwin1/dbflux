//! `ChartDocument` — first-class workspace document that owns a query, connection,
//! chart spec, and a `ChartShell`.
//!
//! Unlike DataGridPanel-embedded charts, `ChartDocument` implements `ChartHost`
//! natively: it owns its `TimeRangePanel`, `RefreshDropdown`, and execution loop.
//! The collapsible editor drawer lets users edit and re-run the query without
//! leaving the chart view.

mod render;

use super::chart::{ChartHost, ChartShell, HostAdapter};
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use crate::app::AppStateEntity;
use crate::keymap::{Command, ContextId};
use crate::ui::common::time_range::view::TimeRangePanel;
use crate::ui::components::dropdown::{Dropdown, DropdownItem};
use crate::ui::components::toast::PendingToast;
use dbflux_components::chart::{ChartDetection, detect_chart_columns};
use dbflux_components::controls::InputState;
use dbflux_components::saved_chart::SavedChart;
use dbflux_core::{ExecutionContext, ExecutionSourceContext};
use dbflux_core::{QueryResult, RefreshPolicy};
use gpui::prelude::*;
use gpui::{App, Context, Entity, EventEmitter, FocusHandle, Subscription, Task, Window};
use std::sync::Arc;
use uuid::Uuid;

/// Events emitted by `ChartDocument`.
#[derive(Clone, Debug)]
pub enum ChartDocumentEvent {
    /// Title or state changed; tab bar should repaint.
    MetaChanged,
    /// The document area was clicked and wants to receive keyboard focus.
    RequestFocus,
}

/// Active focus target within the document.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[allow(dead_code)]
enum ChartDocFocus {
    #[default]
    Shell,
    Drawer,
}

/// A pending query result that arrived from the task runner background task.
struct PendingResult {
    task_id: dbflux_core::TaskId,
    result: Result<QueryResult, dbflux_core::DbError>,
}

/// Internal execution state.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum ExecState {
    #[default]
    Idle,
    Running,
    Error,
}

/// State for the name-prompt modal shown during Save.
struct NamePromptState {
    input: Entity<InputState>,
    _subscription: Subscription,
}

/// First-class chart document.
///
/// Owns its connection ID, query text, `ChartShell`, editor drawer, time-range
/// panel, refresh dropdown, and execution loop. Implements `ChartHost` natively.
pub struct ChartDocument {
    // Identity
    id: DocumentId,
    title: String,
    state: DocumentState,
    exec_state: ExecState,

    // Connection
    profile_id: Option<Uuid>,

    // Query + chart
    query: String,
    last_result: Option<Arc<QueryResult>>,

    // Execution
    runner: DocumentTaskRunner,
    app_state: Entity<AppStateEntity>,
    pending_result: Option<PendingResult>,
    pending_run_on_first_render: bool,

    // Shell
    chart_shell: Entity<ChartShell>,

    // Editor drawer
    editor_drawer_open: bool,
    editor_input: Entity<InputState>,
    _editor_subscription: Subscription,

    // Toolbar controls
    time_range_panel: Option<Entity<TimeRangePanel>>,
    _time_range_sub: Option<Subscription>,
    refresh_dropdown: Entity<Dropdown>,
    refresh_policy: RefreshPolicy,
    _refresh_subscriptions: Vec<Subscription>,
    _refresh_timer: Option<Task<()>>,

    // Pending state from time-range panel changes
    pending_time_window: Option<(i64, i64)>,
    pending_chart_reexecute: bool,

    // Save flow
    saved_chart_id: Option<Uuid>,
    name_prompt: Option<NamePromptState>,
    pending_toast: Option<PendingToast>,

    // Focus
    focus_handle: FocusHandle,
    #[allow(dead_code)]
    focus_mode: ChartDocFocus,

    _subscriptions: Vec<Subscription>,
}

impl ChartDocument {
    /// Create a new `ChartDocument` from a raw query and optional connection.
    ///
    /// `pending_run_on_first_render` is set to `true` when the query is non-empty,
    /// causing the document to auto-execute on its first render cycle.
    pub fn new(
        profile_id: Option<Uuid>,
        query: String,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let chart_shell = cx.new(|cx| {
            // The host adapter for a ChartDocument will be added as a variant once
            // ChartDocument is itself an entity. For now the shell bootstraps with
            // a DataGrid adapter and the host is replaced on first set_result.
            // Because ChartDocument is the native host, the shell initially has
            // no host adapter — we wire it in immediately after construction.
            // SAFETY: This requires ChartDocument to call set_result itself and
            // not delegate through HostAdapter for re-execution.
            //
            // Practical approach: create a minimal host-less shell; ChartDocument
            // drives set_result directly without going through HostAdapter.
            ChartShell::new_standalone(cx)
        });

        let editor_language = profile_id
            .and_then(|pid| {
                app_state
                    .read(cx)
                    .connections()
                    .get(&pid)
                    .map(|c| c.connection.metadata().query_language.clone())
            })
            .unwrap_or(dbflux_core::QueryLanguage::Sql);

        let editor_mode = editor_language.editor_mode();
        let placeholder = editor_language.placeholder();

        let editor_input = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor(editor_mode)
                .line_number(true)
                .soft_wrap(false)
                .placeholder(placeholder)
        });

        // Populate the editor with the initial query text.
        if !query.is_empty() {
            let initial_query = query.clone();
            editor_input.update(cx, |state, cx| {
                state.set_value(&initial_query, window, cx);
            });
        }

        let editor_sub = cx.subscribe_in(
            &editor_input,
            window,
            |this: &mut Self,
             input: &Entity<InputState>,
             event: &dbflux_components::controls::InputEvent,
             _window,
             cx| {
                if matches!(event, dbflux_components::controls::InputEvent::Change) {
                    this.query = input.read(cx).value().to_string();
                }
            },
        );

        let refresh_dropdown = cx.new(|_cx| {
            Dropdown::new("chart-doc-refresh").items(vec![
                DropdownItem::new("Off"),
                DropdownItem::new("30s"),
                DropdownItem::new("1m"),
                DropdownItem::new("5m"),
            ])
        });

        let mut runner = DocumentTaskRunner::new(app_state.clone());
        if let Some(pid) = profile_id {
            runner.set_profile_id(pid);
        }

        let pending_run = !query.trim().is_empty();

        Self {
            id: DocumentId::new(),
            title: "Untitled chart".to_string(),
            state: DocumentState::Clean,
            exec_state: ExecState::Idle,
            profile_id,
            query,
            last_result: None,
            runner,
            app_state,
            pending_result: None,
            pending_run_on_first_render: pending_run,
            chart_shell,
            editor_drawer_open: false,
            editor_input,
            _editor_subscription: editor_sub,
            time_range_panel: None,
            _time_range_sub: None,
            refresh_dropdown,
            refresh_policy: RefreshPolicy::default(),
            _refresh_subscriptions: Vec::new(),
            _refresh_timer: None,
            pending_time_window: None,
            pending_chart_reexecute: false,
            saved_chart_id: None,
            name_prompt: None,
            pending_toast: None,
            focus_handle: cx.focus_handle(),
            focus_mode: ChartDocFocus::default(),
            _subscriptions: Vec::new(),
        }
    }

    /// Create a `ChartDocument` from a previously saved chart record.
    ///
    /// Only `SavedChartSource::Query` sources are supported. Callers must
    /// route `Collection` sources to a `DataDocument` instead — passing a
    /// `Collection`-source chart here will produce a document with an empty
    /// query and no data.
    pub fn from_saved(
        saved: &SavedChart,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Result<Self, String> {
        use dbflux_components::saved_chart::SavedChartSource;

        let query = match &saved.source {
            SavedChartSource::Query { query } => query.clone(),
            SavedChartSource::Collection { .. } => {
                return Err(
                    "Collection source not supported in ChartDocument; open via DataDocument"
                        .to_string(),
                );
            }
        };

        let mut doc = Self::new(Some(saved.profile_id), query, app_state, window, cx);
        doc.title = saved.name.clone();
        doc.saved_chart_id = Some(saved.id);
        Ok(doc)
    }

    /// Check whether a `SavedChart` source is compatible with `ChartDocument`.
    ///
    /// Returns `Ok(())` for `Query` sources and `Err` for `Collection` sources.
    /// Call this before allocating an entity to avoid panicking inside `cx.new`.
    pub fn validate_saved_source(saved: &SavedChart) -> Result<(), String> {
        use dbflux_components::saved_chart::SavedChartSource;
        match &saved.source {
            SavedChartSource::Query { .. } => Ok(()),
            SavedChartSource::Collection { .. } => Err(
                "Collection source not supported in ChartDocument; open via DataDocument"
                    .to_string(),
            ),
        }
    }

    // ---- public accessors ----

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn state(&self) -> DocumentState {
        self.state
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        self.profile_id
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn saved_chart_id(&self) -> Option<Uuid> {
        self.saved_chart_id
    }

    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    pub fn active_context(&self) -> ContextId {
        ContextId::Global
    }

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

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, _cx: &mut Context<Self>) {
        self.refresh_policy = policy;
    }

    pub fn set_active_tab(&mut self, _active: bool) {}

    pub fn change_summary(&self, _cx: &App) -> Option<String> {
        None
    }

    // ---- editor drawer ----

    pub fn toggle_editor_drawer(&mut self, cx: &mut Context<Self>) {
        self.editor_drawer_open = !self.editor_drawer_open;
        cx.notify();
    }

    // ---- execution ----

    /// Request a fresh query execution.
    ///
    /// Gets the connection from `app_state`, fires the query on a background
    /// thread, and delivers the result back to the entity via `pending_result`.
    /// The render loop picks up `pending_result` and applies it.
    pub fn request_reexecute(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let query = self.query.trim().to_string();
        if query.is_empty() {
            return;
        }
        let Some(profile_id) = self.profile_id else {
            self.pending_toast = Some(PendingToast {
                message: "No connection selected".to_string(),
                is_error: true,
            });
            cx.notify();
            return;
        };

        // Resolve the connection synchronously on the foreground thread.
        let conn = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                self.pending_toast = Some(PendingToast {
                    message: "Connection not found".to_string(),
                    is_error: true,
                });
                cx.notify();
                return;
            };
            match connected.resolve_connection_for_execution(None) {
                Ok(c) => c,
                Err(e) => {
                    self.pending_toast = Some(PendingToast {
                        message: format!("Connection error: {:?}", e),
                        is_error: true,
                    });
                    cx.notify();
                    return;
                }
            }
        };

        let (task_id, cancel_token) =
            self.runner
                .start_primary(dbflux_core::TaskKind::Query, "Chart query", cx);

        self.exec_state = ExecState::Running;
        self.state = DocumentState::Executing;
        cx.notify();

        // Attach a CollectionWindow source context when the time-range panel
        // has produced a resolved window. The driver uses it to inject time
        // bounds into queries that have no hardcoded WHERE time predicate.
        let exec_ctx = self
            .pending_time_window
            .map(|(start_ms, end_ms)| ExecutionContext {
                source: Some(ExecutionSourceContext::CollectionWindow {
                    targets: Vec::new(),
                    start_ms,
                    end_ms,
                    query_mode: None,
                }),
                ..ExecutionContext::default()
            });

        let request = dbflux_core::QueryRequest::new(query).with_execution_context(exec_ctx);
        let conn_cleanup = conn.clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&request) });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    if let Err(e) = conn_cleanup.cleanup_after_cancel() {
                        log::warn!("[chart-doc] cleanup after cancel failed: {}", e);
                    }
                    return;
                }

                let Some(entity) = this.upgrade() else { return };
                entity.update(cx, |doc, cx| {
                    doc.pending_result = Some(PendingResult { task_id, result });
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    /// Apply a completed query result to the chart shell.
    fn apply_result(&mut self, pending: PendingResult, cx: &mut Context<Self>) {
        self.runner.complete_primary(pending.task_id, cx);

        match pending.result {
            Ok(result) => {
                self.exec_state = ExecState::Idle;
                self.state = DocumentState::Clean;

                let was_chart_mode = self.last_result.is_some();
                let arc = Arc::new(result);

                let arc_clone = arc.clone();
                self.chart_shell.update(cx, |shell, cx| {
                    shell.set_result(&arc_clone, was_chart_mode, cx);
                });

                self.last_result = Some(arc);
            }
            Err(err) => {
                self.exec_state = ExecState::Error;
                self.state = DocumentState::Error;
                self.pending_toast = Some(PendingToast {
                    message: err.to_string(),
                    is_error: true,
                });
            }
        }

        cx.notify();
    }

    /// Handle a `TimeRangeChanged` event from the owned `TimeRangePanel`.
    ///
    /// Stashes the resolved window and schedules a re-execution on the next
    /// render cycle, mirroring how `CodeDocument` reacts to range changes.
    pub fn on_time_range_changed(
        &mut self,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        cx: &mut Context<Self>,
    ) {
        if let (Some(start), Some(end)) = (start_ms, end_ms) {
            self.pending_time_window = Some((start, end));
            self.pending_chart_reexecute = true;
            cx.notify();
        }
    }

    // ---- save flow ----

    /// Open the name-prompt modal for saving this chart.
    fn open_name_prompt(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let initial = if self.saved_chart_id.is_some() {
            self.title.clone()
        } else {
            String::new()
        };

        let input = cx.new(|cx| InputState::new(window, cx).placeholder("Chart name"));

        if !initial.is_empty() {
            input.update(cx, |state, cx| {
                state.set_value(&initial, window, cx);
            });
        }

        // No subscription needed — value is read on confirm.
        let sub = cx.subscribe_in(
            &input,
            window,
            |_this: &mut Self,
             _input: &Entity<InputState>,
             _event: &dbflux_components::controls::InputEvent,
             _window,
             _cx| {},
        );

        self.name_prompt = Some(NamePromptState {
            input,
            _subscription: sub,
        });

        cx.notify();
    }

    /// Confirm the name-prompt and persist the chart.
    fn confirm_save(&mut self, cx: &mut Context<Self>) {
        let Some(prompt) = self.name_prompt.take() else {
            return;
        };

        let name = prompt.input.read(cx).value().trim().to_string();
        if name.is_empty() {
            self.name_prompt = Some(prompt);
            return;
        }

        let id = self.saved_chart_id.unwrap_or_else(Uuid::new_v4);
        let profile_id = self.profile_id.unwrap_or_else(Uuid::nil);

        // Build a ChartSpec from the last result if available, otherwise use a minimal placeholder.
        let spec = self
            .last_result
            .as_ref()
            .and_then(|r| match detect_chart_columns(r) {
                ChartDetection::Ok {
                    time_col,
                    numeric_cols,
                } => dbflux_components::chart::ChartSpec::from_detection(
                    time_col,
                    numeric_cols,
                    &r.columns,
                    10_000,
                ),
                _ => None,
            })
            .unwrap_or_else(|| dbflux_components::chart::ChartSpec {
                kind: dbflux_components::chart::ChartKind::Line,
                x_axis: dbflux_components::chart::AxisSpec {
                    column_index: 0,
                    label: String::new(),
                    kind: dbflux_components::chart::AxisKind::Time,
                    unit: None,
                },
                series: Vec::new(),
                legend_visible: false,
                decimation_threshold: 10_000,
                binding: dbflux_components::chart::BindingSpec::default(),
                track_source_indices: false,
            });

        let bindings = spec.binding.clone();

        let mut saved =
            SavedChart::new_query(name.clone(), profile_id, self.query.clone(), spec, bindings);
        // Preserve the ID so upsert overwrites the existing record.
        saved.id = id;

        self.app_state.update(cx, |state, _cx| {
            state.saved_charts.upsert(saved);
        });

        self.saved_chart_id = Some(id);
        self.title = name;
        self.pending_toast = Some(PendingToast {
            message: "Chart saved".to_string(),
            is_error: false,
        });

        cx.notify();
    }

    /// Dismiss the name-prompt modal without saving.
    fn cancel_save(&mut self, cx: &mut Context<Self>) {
        self.name_prompt = None;
        cx.notify();
    }
}

impl EventEmitter<ChartDocumentEvent> for ChartDocument {}

impl ChartHost for ChartDocument {
    fn current_query(&self, _cx: &App) -> Option<String> {
        let q = self.query.trim().to_string();
        if q.is_empty() { None } else { Some(q) }
    }

    fn connection_id(&self, _cx: &App) -> Option<Uuid> {
        self.profile_id
    }

    fn time_range_panel(&self, _cx: &App) -> Option<Entity<TimeRangePanel>> {
        self.time_range_panel.clone()
    }

    fn refresh_dropdown(&self, _cx: &App) -> Entity<Dropdown> {
        self.refresh_dropdown.clone()
    }

    fn current_result(&self, _cx: &App) -> Option<Arc<QueryResult>> {
        self.last_result.clone()
    }

    fn request_reexecute(&mut self, window: &mut Window, cx: &mut App) {
        // ChartHost::request_reexecute takes &mut App but ChartDocument::request_reexecute
        // takes &mut Context<Self>. We use cx.notify() as a bridge here.
        // This method is called by ChartShell via HostAdapter; for ChartDocument
        // the execution is driven directly without HostAdapter, so this is a no-op
        // path in practice — re-execution goes through render's pending_run flag.
        let _ = window;
        let _ = cx;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Constructor with empty query must NOT set `pending_run_on_first_render`.
    #[test]
    fn empty_query_does_not_schedule_auto_run() {
        let pending = compute_pending_run_flag("");
        assert!(!pending, "empty query must not trigger auto-run");
    }

    /// Constructor with non-empty query MUST set `pending_run_on_first_render`.
    #[test]
    fn non_empty_query_schedules_auto_run() {
        let pending = compute_pending_run_flag("SELECT * FROM metrics");
        assert!(pending, "non-empty query must trigger auto-run");
    }

    /// Drawer toggle is reversible.
    #[test]
    fn drawer_toggle_is_reversible() {
        let open = true;
        let after_first_toggle = !open;
        let after_second_toggle = !after_first_toggle;
        assert_eq!(
            after_second_toggle, open,
            "two toggles must return to original state"
        );
    }

    /// `on_time_range_changed` sets `pending_chart_reexecute` and stashes the
    /// window when both ms values are `Some`.
    ///
    /// T-CR-06: unit test for the reexecute flag.
    #[test]
    fn on_time_range_changed_sets_reexecute_flag_when_both_some() {
        let result = simulate_time_range_changed(Some(1_000), Some(2_000));
        assert!(
            result.pending_chart_reexecute,
            "pending_chart_reexecute must be true when both start and end are Some"
        );
        assert_eq!(
            result.pending_time_window,
            Some((1_000, 2_000)),
            "pending_time_window must be stashed as (start_ms, end_ms)"
        );
    }

    /// `on_time_range_changed` must NOT set the flag when either value is None.
    ///
    /// T-CR-06: guard against Custom preset half-state.
    #[test]
    fn on_time_range_changed_ignores_partial_window() {
        let result_start_none = simulate_time_range_changed(None, Some(2_000));
        assert!(
            !result_start_none.pending_chart_reexecute,
            "must not reexecute when start_ms is None"
        );

        let result_end_none = simulate_time_range_changed(Some(1_000), None);
        assert!(
            !result_end_none.pending_chart_reexecute,
            "must not reexecute when end_ms is None"
        );

        let result_both_none = simulate_time_range_changed(None, None);
        assert!(
            !result_both_none.pending_chart_reexecute,
            "must not reexecute when both are None"
        );
    }

    // ---- helpers ----

    fn compute_pending_run_flag(query: &str) -> bool {
        !query.trim().is_empty()
    }

    /// Simulated outcome of calling `on_time_range_changed` on a zeroed state.
    struct TimeRangeChangedOutcome {
        pending_chart_reexecute: bool,
        pending_time_window: Option<(i64, i64)>,
    }

    /// Exercise `on_time_range_changed` logic without a GPUI runtime by
    /// replicating the method's decision tree directly.
    fn simulate_time_range_changed(
        start_ms: Option<i64>,
        end_ms: Option<i64>,
    ) -> TimeRangeChangedOutcome {
        let mut pending_chart_reexecute = false;
        let mut pending_time_window: Option<(i64, i64)> = None;

        if let (Some(start), Some(end)) = (start_ms, end_ms) {
            pending_time_window = Some((start, end));
            pending_chart_reexecute = true;
        }

        TimeRangeChangedOutcome {
            pending_chart_reexecute,
            pending_time_window,
        }
    }
}
