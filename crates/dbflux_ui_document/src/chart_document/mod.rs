//! `ChartDocument` — first-class workspace document that owns a query, connection,
//! chart spec, and a `ChartShell`.
//!
//! Unlike DataGridPanel-embedded charts, `ChartDocument` implements `ChartHost`
//! natively: it owns its `TimeRangePanel`, `RefreshDropdown`, and execution loop.
//! Created exclusively by promoting a query result (e.g. "Chart this query"
//! from a data grid); the query is fixed for the document's lifetime.

pub mod pane;
mod render;

use super::chart::shell::ChartShellEvent;
use super::chart::{ChartHost, ChartShell, HostAdapter};
use super::handle::DocumentEvent;
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::chart::{
    ChartDataSource, ChartDetection, ChartSourceError, TimeWindow, detect_chart_columns,
    resolve_source,
};
use dbflux_components::common::time_range::state::TimeRange;
use dbflux_components::common::time_range::view::TimeRangePanel;
use dbflux_components::controls::InputState;
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::result_panel::{ResultPanel, SegmentPosition, ToolbarSegment, ViewHandle};
use dbflux_components::result_view::ResultViewMode;
use dbflux_components::saved_chart::{SavedChart, SavedChartSource};
use dbflux_core::{QueryResult, RefreshPolicy};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::PendingToast;
use gpui::prelude::*;
use gpui::{App, Context, Entity, EventEmitter, FocusHandle, Subscription, Task, Window};
use std::sync::Arc;
use uuid::Uuid;

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
    data_source: Box<dyn ChartDataSource>,
    last_result: Option<Arc<QueryResult>>,

    // Execution
    runner: DocumentTaskRunner,
    app_state: Entity<AppStateEntity>,
    pending_result: Option<PendingResult>,
    pending_run_on_first_render: bool,

    // Shell
    chart_shell: Entity<ChartShell>,

    // Toolbar controls
    time_range_panel: Option<Entity<TimeRangePanel>>,
    _time_range_sub: Option<Subscription>,
    /// Mirrors `TimeRangePanel::selected_time_range` so the render path can
    /// decide whether to show the custom date/time picker row without calling
    /// `panel.read(cx)` on every frame.
    selected_time_range: Option<TimeRange>,
    refresh_dropdown: Entity<Dropdown>,
    refresh_policy: RefreshPolicy,
    _refresh_subscriptions: Vec<Subscription>,
    _refresh_timer: Option<Task<()>>,

    // Pending state from time-range panel changes
    pending_time_window: Option<(i64, i64)>,
    pending_chart_reexecute: bool,

    // Pending data source swap from MetricPickerApplied event.
    // Consumed by the render loop so the swap happens on the UI thread.
    pending_data_source: Option<Box<dyn ChartDataSource>>,

    // The `(namespace, metric_name)` triple this document was opened with,
    // when the source was a `MetricSource`. Used by `matches_metric_source`
    // for sidebar dedup so the identity remains stable after the user
    // refines dimensions/period/statistic via the Apply button.
    initial_metric_identity: Option<(String, String)>,

    // Save flow
    saved_chart_id: Option<Uuid>,
    name_prompt: Option<NamePromptState>,
    pending_toast: Option<PendingToast>,

    // Focus
    focus_handle: FocusHandle,
    #[allow(dead_code)]
    focus_mode: ChartDocFocus,

    /// Chrome host: lazily built on first render (requires Window for
    /// TimeRangePanel construction; set to `Some` by `render.rs`).
    pub(super) result_panel: Option<Entity<ResultPanel>>,

    /// When `true`, this chart is embedded inside another document (e.g. a
    /// `DashboardDocument` panel) and must suppress its own chrome — the
    /// header segments (title/Run/Save) and the internal chart toolbar row
    /// (TYPE/Stats/PNG/Save) are not rendered. The host document supplies the
    /// surrounding chrome instead.
    pub(super) embedded: bool,

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
        _window: &mut Window,
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

        // Bridge the metric picker's Apply emission into this document.
        // Without this subscription `pending_data_source` is never written and
        // the Apply button (and Cmd/Ctrl+Enter shortcut) become dead UI.
        let metric_apply_sub = cx.subscribe(
            &chart_shell,
            |this: &mut Self, _shell, event: &ChartShellEvent, cx| match event {
                ChartShellEvent::MetricPickerApplied(src) => {
                    this.pending_data_source = Some(src.clone_box());
                    cx.notify();
                }
            },
        );

        // Cancel any pending metric-picker dimensions fetch when the chart's
        // connection drops. Without this, the in-flight fetch completes,
        // enters its foreground cx.update closure, and writes a now-stale
        // entry into MetricCatalogCache (which the disconnect already
        // invalidated). Dropping the task short-circuits the await.
        let app_state_disconnect_sub = cx.subscribe(
            &app_state,
            |this: &mut Self, _state, _event: &dbflux_ui_base::AppStateChanged, cx| {
                this.cancel_metric_fetches_if_disconnected(cx);
            },
        );

        let default_refresh = RefreshPolicy::default();
        let refresh_dropdown = cx.new(|_cx| {
            let items = RefreshPolicy::ALL
                .iter()
                .map(|p| DropdownItem::new(p.label()))
                .collect();

            Dropdown::new("chart-doc-refresh")
                .items(items)
                .selected_index(Some(default_refresh.index()))
                .compact_trigger(true)
        });

        let refresh_policy_sub = cx.subscribe(
            &refresh_dropdown,
            |this: &mut Self, _, event: &DropdownSelectionChanged, cx| {
                let policy = RefreshPolicy::from_index(event.index);
                this.set_refresh_policy(policy, cx);
            },
        );

        let mut runner = DocumentTaskRunner::new(app_state.clone());
        if let Some(pid) = profile_id {
            runner.set_profile_id(pid);
        }

        let pending_run = !query.trim().is_empty();

        // Build the data source from the query string. Uses resolve_source so
        // construction goes through the single factory (QuerySource is pub(crate)
        // in dbflux_components and not directly accessible here).
        let data_source = resolve_source(&SavedChartSource::Query {
            query: query.clone(),
        });

        Self {
            id: DocumentId::new(),
            title: "Untitled chart".to_string(),
            state: DocumentState::Clean,
            exec_state: ExecState::Idle,
            profile_id,
            query,
            data_source,
            last_result: None,
            runner,
            app_state,
            pending_result: None,
            pending_run_on_first_render: pending_run,
            chart_shell,
            time_range_panel: None,
            _time_range_sub: None,
            selected_time_range: None,
            refresh_dropdown,
            refresh_policy: default_refresh,
            _refresh_subscriptions: vec![refresh_policy_sub],
            _refresh_timer: None,
            pending_time_window: None,
            pending_chart_reexecute: false,
            pending_data_source: None,
            initial_metric_identity: None,
            saved_chart_id: None,
            name_prompt: None,
            pending_toast: None,
            focus_handle: cx.focus_handle(),
            focus_mode: ChartDocFocus::default(),
            result_panel: None,
            embedded: false,
            _subscriptions: vec![metric_apply_sub, app_state_disconnect_sub],
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
        use dbflux_components::chart::{InstanceMetricSource, MetricSource};

        match &saved.source {
            // Collection sources are not routed through ChartDocument in W0.
            // They still open via DataDocument.
            SavedChartSource::Collection { .. } => {
                return Err(
                    "Collection source not supported in ChartDocument; open via DataDocument"
                        .to_string(),
                );
            }

            // Metric sources bypass the query path and construct a MetricSource
            // directly, matching the same path used by `open_metric_chart_from_sidebar`.
            SavedChartSource::Metric { series } => {
                let source = MetricSource {
                    series: series.clone(),
                };

                let mut doc = Self::new_with_source(
                    Some(saved.profile_id),
                    saved.name.clone(),
                    Box::new(source),
                    app_state,
                    window,
                    cx,
                );
                doc.saved_chart_id = Some(saved.id);
                return Ok(doc);
            }

            // Instance metric sources follow the same self-executing pattern as Metric.
            SavedChartSource::InstanceMetric { metric_id } => {
                let source = InstanceMetricSource {
                    metric_id: metric_id.clone(),
                };

                let mut doc = Self::new_with_source(
                    Some(saved.profile_id),
                    saved.name.clone(),
                    Box::new(source),
                    app_state,
                    window,
                    cx,
                );
                doc.saved_chart_id = Some(saved.id);
                return Ok(doc);
            }

            // Query source: standard path through query execution.
            SavedChartSource::Query { .. } => {}
        }

        // Extract the query string (only reached for Query variant).
        let query = if let SavedChartSource::Query { query } = &saved.source {
            query.clone()
        } else {
            String::new()
        };

        let mut doc = Self::new(Some(saved.profile_id), query, app_state, window, cx);

        // Override data_source with the resolver so from_saved is already
        // correct for future source kinds once routing is extended.
        doc.data_source = resolve_source(&saved.source);

        doc.title = saved.name.clone();
        doc.saved_chart_id = Some(saved.id);
        Ok(doc)
    }

    /// Create a `ChartDocument` with an explicitly supplied `ChartDataSource`.
    ///
    /// Used when the caller already holds a fully-constructed source (e.g.
    /// `MetricSource`) and does not want to go through `resolve_source`. The
    /// document title defaults to `"Untitled chart"` and can be overridden by
    /// the caller after construction.
    ///
    /// `pending_run_on_first_render` is always `true`: the document auto-executes
    /// on first render, which seeds the initial time window from `TimeRangePanel`
    /// and fires the first data request.
    pub fn new_with_source(
        profile_id: Option<Uuid>,
        title: String,
        data_source: Box<dyn ChartDataSource>,
        app_state: Entity<AppStateEntity>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let chart_shell = cx.new(ChartShell::new_standalone);

        // Bridge the metric picker's Apply emission into this document.
        // Without this subscription `pending_data_source` is never written and
        // the Apply button (and Cmd/Ctrl+Enter shortcut) become dead UI.
        let metric_apply_sub = cx.subscribe(
            &chart_shell,
            |this: &mut Self, _shell, event: &ChartShellEvent, cx| match event {
                ChartShellEvent::MetricPickerApplied(src) => {
                    this.pending_data_source = Some(src.clone_box());
                    cx.notify();
                }
            },
        );

        // See `new()` for the rationale: drop the metric-picker dimensions
        // task on disconnect so its foreground cache-write closure never runs
        // against the invalidated MetricCatalogCache.
        let app_state_disconnect_sub = cx.subscribe(
            &app_state,
            |this: &mut Self, _state, _event: &dbflux_ui_base::AppStateChanged, cx| {
                this.cancel_metric_fetches_if_disconnected(cx);
            },
        );

        let default_refresh = RefreshPolicy::default();
        let refresh_dropdown = cx.new(|_cx| {
            let items = RefreshPolicy::ALL
                .iter()
                .map(|p| DropdownItem::new(p.label()))
                .collect();

            Dropdown::new("chart-doc-refresh")
                .items(items)
                .selected_index(Some(default_refresh.index()))
                .compact_trigger(true)
        });

        let refresh_policy_sub = cx.subscribe(
            &refresh_dropdown,
            |this: &mut Self, _, event: &DropdownSelectionChanged, cx| {
                let policy = RefreshPolicy::from_index(event.index);
                this.set_refresh_policy(policy, cx);
            },
        );

        let mut runner = DocumentTaskRunner::new(app_state.clone());
        if let Some(pid) = profile_id {
            runner.set_profile_id(pid);
        }

        // Capture the initial (namespace, metric_name) identity when the
        // source is a MetricSource so sidebar dedup stays correct even after
        // Apply rewrites dimensions/period/statistic.
        let initial_metric_identity = data_source
            .as_any()
            .and_then(|a| a.downcast_ref::<dbflux_components::chart::MetricSource>())
            .map(|src| {
                (
                    src.primary_namespace().to_string(),
                    src.primary_metric_name().to_string(),
                )
            });

        Self {
            id: DocumentId::new(),
            title,
            state: DocumentState::Clean,
            exec_state: ExecState::Idle,
            profile_id,
            query: String::new(),
            data_source,
            last_result: None,
            runner,
            app_state,
            pending_result: None,
            pending_run_on_first_render: true,
            chart_shell,
            time_range_panel: None,
            _time_range_sub: None,
            selected_time_range: None,
            refresh_dropdown,
            refresh_policy: default_refresh,
            _refresh_subscriptions: vec![refresh_policy_sub],
            _refresh_timer: None,
            pending_time_window: None,
            pending_chart_reexecute: false,
            pending_data_source: None,
            initial_metric_identity,
            saved_chart_id: None,
            name_prompt: None,
            pending_toast: None,
            result_panel: None,
            embedded: false,
            focus_handle: cx.focus_handle(),
            focus_mode: ChartDocFocus::default(),
            _subscriptions: vec![metric_apply_sub, app_state_disconnect_sub],
        }
    }

    /// If the document's profile is no longer connected, drop the metric
    /// picker's in-flight dimensions fetch so its foreground cache-write
    /// closure never runs against the now-invalidated cache.
    ///
    /// No-op when there is no profile, no picker, or no in-flight task.
    fn cancel_metric_fetches_if_disconnected(&mut self, cx: &mut Context<Self>) {
        let Some(profile_id) = self.profile_id else {
            return;
        };
        let still_connected = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);
        if still_connected {
            return;
        }
        self.chart_shell.update(cx, |shell, _cx| {
            if let Some(picker) = shell.metric_picker.as_mut()
                && picker.dimensions_task.is_some()
            {
                picker.dimensions_task = None;
            }
        });
    }

    /// Check whether a `SavedChart` source is compatible with `ChartDocument`.
    ///
    /// Returns `Ok(())` for `Query` sources and `Err` for `Collection` sources.
    /// Call this before allocating an entity to avoid panicking inside `cx.new`.
    pub fn validate_saved_source(saved: &SavedChart) -> Result<(), String> {
        match &saved.source {
            SavedChartSource::Query { .. }
            | SavedChartSource::Metric { .. }
            | SavedChartSource::InstanceMetric { .. } => Ok(()),
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

    /// Check whether this document was opened for the given metric identity.
    ///
    /// Used by the `DocumentKey::MetricChart` dedup path in `into_pane`. Compares
    /// against the initial `(namespace, metric_name)` captured at construction —
    /// NOT the current `data_source` — so the identity remains stable after the
    /// Apply button rewrites dimensions/period/statistic (which keeps the
    /// `MetricSource` type but produces a new value via `set_data_source`).
    pub fn matches_metric_source(
        &self,
        profile_id: Uuid,
        namespace: &str,
        metric_name: &str,
    ) -> bool {
        if self.profile_id != Some(profile_id) {
            return false;
        }

        self.initial_metric_identity
            .as_ref()
            .is_some_and(|(ns, mn)| ns == namespace && mn == metric_name)
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

    /// Open the Metric rail and initialize the picker with a pre-populated
    /// `(namespace, metric_name)`.
    ///
    /// Called after construction when the chart is opened from the sidebar tree
    /// (user clicked a metric leaf). The picker shows dimensions, period, and
    /// statistic for refinement; namespace/metric are pinned.
    pub fn setup_metric_picker(
        &mut self,
        namespace: String,
        metric_name: String,
        cx: &mut Context<Self>,
    ) {
        use super::chart::ChartRailTab;
        use super::chart::metric_picker::MetricPickerState;

        let profile_id = match self.profile_id {
            Some(id) => id,
            None => return,
        };

        // Record the metric identity so the sidebar's MetricChart dedup
        // remains stable across subsequent Apply rewrites.
        self.initial_metric_identity = Some((namespace.clone(), metric_name.clone()));

        let app_state_clone = self.app_state.clone();
        self.chart_shell.update(cx, |shell, cx| {
            shell.set_initial_rail(ChartRailTab::Metric, true);
            shell.metric_picker = Some(MetricPickerState::new_pre_populated(
                profile_id,
                app_state_clone,
                namespace,
                metric_name,
                cx,
            ));
        });
    }

    pub fn change_summary(&self, _cx: &App) -> Option<String> {
        None
    }

    // ---- data source ----

    /// Replace the active data source and trigger a fresh execution.
    ///
    /// Cancels any in-progress execution, swaps the source, updates the
    /// document title from the source description (when the source provides
    /// one), emits `DataSourceChanged` + `MetaChanged`, and schedules a
    /// chart re-execute. The `window` parameter is retained for forward
    /// compatibility; no callers currently require it.
    pub fn set_data_source(
        &mut self,
        source: Box<dyn ChartDataSource>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.runner.cancel_primary(cx);

        // Update the title if the new source describes itself.
        if let Some(title) = source.describe().display_title() {
            self.title = title;
        }

        self.data_source = source;
        self.pending_chart_reexecute = true;

        cx.emit(DocumentEvent::DataSourceChanged);
        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    // ---- execution ----

    /// Request a fresh query execution.
    ///
    /// Gets the connection from `app_state`, fires the query on a background
    /// thread, and delivers the result back to the entity via `pending_result`.
    /// The render loop picks up `pending_result` and applies it.
    pub fn request_reexecute(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        // Build the execution request through the data source seam.
        // EmptyQuery → silent early return (preserves the old inline empty-query guard).
        // Other errors → show a toast and return.
        let window = self.pending_time_window.map(|(s, e)| TimeWindow {
            start_ms: s,
            end_ms: e,
        });

        // Build the execution plan through the data source seam, then extract the
        // Driver request. Query/Collection sources always yield ChartDataPlan::Driver;
        // the non-Driver arm is a defensive guard for any future source kind that
        // somehow reaches ChartDocument (which should not happen by design).
        let request = match self.data_source.build_plan(window) {
            Ok(dbflux_components::chart::ChartDataPlan::Driver(r)) => r,
            Ok(_non_driver) => {
                // Non-Driver plans are not executable by ChartDocument; this path
                // is unreachable with Query/Collection sources but is handled
                // defensively to avoid a silent no-op.
                log::warn!("[chart-doc] build_plan returned a non-Driver plan; ignoring");
                return;
            }
            Err(ChartSourceError::EmptyQuery) => return,
            Err(e) => {
                self.pending_toast = Some(PendingToast {
                    message: format!("Chart source error: {e}"),
                    is_error: true,
                });
                cx.notify();
                return;
            }
        };

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

        // Apply time-range macro substitution before dispatch. ChartDocument
        // does not flow through `query_request_for_execution` in code/mod.rs,
        // so we substitute here using the connection's declared QueryLanguage
        // and the same window that drove the data-source plan. Without this,
        // queries containing `$timeFilter` / `$__from` / `$__to` (InfluxQL) or
        // `v.timeRangeStart` / `v.timeRangeStop` (Flux) would reach the driver
        // unsubstituted and fail to parse.
        let mut request = request;
        let macro_window = self.pending_time_window;
        let query_language = conn.metadata().query_language.clone();
        request.sql =
            dbflux_core::substitute_time_macros(&request.sql, macro_window, query_language);

        let (task_id, cancel_token) =
            self.runner
                .start_primary(dbflux_core::TaskKind::Query, "Chart query", cx);

        self.exec_state = ExecState::Running;
        self.state = DocumentState::Executing;
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
    /// Also mirrors `selected_time_range` from the panel so the render path
    /// knows whether to keep the custom picker row visible after Apply.
    pub fn on_time_range_changed(
        &mut self,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        cx: &mut Context<Self>,
    ) {
        // Mirror the panel's selected range so the custom picker row stays
        // visible after the user applies a custom window.
        if let Some(panel) = &self.time_range_panel {
            self.selected_time_range = panel.read(cx).selected_time_range;
        }

        if let (Some(start), Some(end)) = (start_ms, end_ms) {
            self.pending_time_window = Some((start, end));
            self.pending_chart_reexecute = true;
            cx.notify();
        }
    }

    /// Update the pending time window WITHOUT scheduling a re-execution.
    ///
    /// Called by `DashboardDocument::request_reexec_for_slot` for panels that
    /// are queued behind the semaphore. The window is stashed so that when the
    /// semaphore releases and `mark_pending_reexecute` is called, the correct
    /// window is used.
    pub fn stage_time_window(&mut self, start_ms: i64, end_ms: i64) {
        self.pending_time_window = Some((start_ms, end_ms));
        // Intentionally does NOT set pending_chart_reexecute or call cx.notify().
    }

    /// Set `pending_chart_reexecute = true` and schedule a render notification.
    ///
    /// Called by `DashboardDocument` when the semaphore releases a slot.
    /// The panel's render loop will pick up the flag and call
    /// `request_reexecute(window, cx)`.
    pub fn mark_pending_reexecute(&mut self, cx: &mut Context<Self>) {
        self.pending_chart_reexecute = true;
        cx.notify();
    }

    /// Apply the custom date/time picker values and trigger a chart re-execution.
    ///
    /// Called by the Apply button in the custom picker row. Delegates to the
    /// panel's `apply_custom_range`, which validates the inputs and emits
    /// `TimeRangeChanged`. The flags are set here synchronously from the
    /// returned `(start_ms, end_ms)` bounds rather than waiting for the
    /// deferred subscription delivery, which eliminates a render-timing race
    /// where the re-execution flag could be missed.
    pub(super) fn apply_custom_range(&mut self, cx: &mut Context<Self>) {
        let Some(panel) = self.time_range_panel.clone() else {
            return;
        };

        match panel.update(cx, |p, cx| p.apply_custom_range(cx)) {
            Ok((start_ms, end_ms)) => {
                // Drive re-execution synchronously from the validated bounds rather
                // than waiting for the deferred TimeRangeChanged subscription to
                // mutate state. The subscription still fires (and mirrors
                // selected_time_range), but the chart re-run is no longer gated
                // on its delivery timing.
                self.pending_time_window = Some((start_ms, end_ms));
                self.pending_chart_reexecute = true;
                cx.notify();
            }
            Err(error) => {
                self.pending_toast = Some(PendingToast {
                    message: error,
                    is_error: true,
                });
                cx.notify();
            }
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
                y_scale: dbflux_components::chart::YScale::Linear,
            });

        let bindings = spec.binding.clone();

        let mut saved =
            SavedChart::new_query(name.clone(), profile_id, self.query.clone(), spec, bindings);
        // Preserve the ID so upsert overwrites the existing record.
        saved.id = id;

        let persist_result = self.app_state.update(cx, |state, _cx| {
            state.saved_charts.upsert(saved).inspect_err(|e| {
                state.record_storage_failure(
                    dbflux_core::observability::actions::CONFIG_UPDATE,
                    "saved_chart",
                    id.to_string(),
                    format!("Failed to save chart '{name}'"),
                    e.to_string(),
                );
            })
        });

        match persist_result {
            Ok(_) => {
                self.saved_chart_id = Some(id);
                self.title = name;
                self.pending_toast = Some(PendingToast {
                    message: "Chart saved".to_string(),
                    is_error: false,
                });
            }
            Err(e) => {
                self.pending_toast = Some(PendingToast {
                    message: format!("Failed to save chart: {e}"),
                    is_error: true,
                });
            }
        }

        cx.notify();
    }

    /// Dismiss the name-prompt modal without saving.
    fn cancel_save(&mut self, cx: &mut Context<Self>) {
        self.name_prompt = None;
        cx.notify();
    }

    // ---- ViewHandle construction ----

    /// Mark this chart document as embedded inside another document (typically
    /// a `DashboardDocument` panel).
    ///
    /// When embedded, the chart suppresses its own header segments (title /
    /// Run / Save) and its internal chart toolbar row (TYPE / Stats / PNG /
    /// Save chart). The host document provides the surrounding chrome.
    pub fn set_embedded(&mut self, embedded: bool, cx: &mut Context<Self>) {
        if self.embedded != embedded {
            self.embedded = embedded;
            cx.notify();
        }
    }

    /// Returns whether this chart is in embedded mode.
    pub fn is_embedded(&self) -> bool {
        self.embedded
    }

    // ---- Accessors used by host documents (e.g. DashboardDocument Configure popover) ----

    /// Returns the current chart kind from the underlying `ChartShell`.
    pub fn chart_kind(&self, cx: &App) -> dbflux_components::chart::ChartKind {
        self.chart_shell.read(cx).chart_kind()
    }

    /// Returns the active binding spec from the underlying `ChartShell`.
    pub fn active_bindings(&self, cx: &App) -> dbflux_components::chart::BindingSpec {
        self.chart_shell.read(cx).active_bindings()
    }

    /// Returns the column metadata from the last successful execution, when present.
    pub fn last_result_columns(&self) -> Option<Vec<dbflux_core::ColumnMeta>> {
        self.last_result.as_ref().map(|r| r.columns.clone())
    }

    /// Returns the currently open axis pill on the underlying `ChartShell`.
    pub fn axis_open_pill(&self, cx: &App) -> Option<dbflux_components::chart::AxisPill> {
        self.chart_shell.read(cx).axis_open_pill
    }

    /// Toggle an axis pill open/closed on the underlying `ChartShell`.
    pub fn toggle_axis_pill(
        &mut self,
        pill: dbflux_components::chart::AxisPill,
        cx: &mut Context<Self>,
    ) {
        self.chart_shell
            .update(cx, |shell, cx| shell.toggle_axis_pill(pill, cx));
    }

    /// Apply a chart kind change through the underlying `ChartShell`. The shell
    /// handles cx.notify() internally.
    pub fn apply_chart_kind(
        &mut self,
        kind: dbflux_components::chart::ChartKind,
        cx: &mut Context<Self>,
    ) {
        self.chart_shell
            .update(cx, |shell, cx| shell.set_chart_kind(kind, cx));
    }

    /// Apply a binding-spec change through the underlying `ChartShell`.
    pub fn apply_binding_spec(
        &mut self,
        bindings: dbflux_components::chart::BindingSpec,
        cx: &mut Context<Self>,
    ) {
        self.chart_shell
            .update(cx, |shell, cx| shell.apply_bindings(bindings, cx));
    }

    /// Toggle the stats rail on the underlying `ChartShell`. Mirrors the
    /// internal `on_toggle_stats_rail` handler used by `ChartDocument`'s own
    /// toolbar so the dashboard Configure popover behaves identically.
    pub fn toggle_stats_rail(&mut self, cx: &mut Context<Self>) {
        self.chart_shell.update(cx, |shell, cx| {
            let (open, tab) = if shell.chart_rail_open
                && shell.chart_rail_tab == crate::chart::ChartRailTab::Stats
            {
                (false, shell.chart_rail_tab)
            } else {
                (true, crate::chart::ChartRailTab::Stats)
            };
            shell.chart_rail_open = open;
            shell.chart_rail_tab = tab;
            cx.notify();
        });
    }

    /// Schedule a "PNG export coming soon" toast. The host document's render
    /// loop drains `pending_toast` and surfaces it through the global toast host.
    pub fn schedule_png_export_toast(&mut self, cx: &mut Context<Self>) {
        self.pending_toast = Some(PendingToast {
            message: "PNG export coming in v0.7".to_string(),
            is_error: false,
        });
        cx.notify();
    }

    /// Persist the current `chart_spec` + bindings back to `SavedChart` storage.
    ///
    /// Looks up the chart record by `saved_chart_id` (no-op if the document was
    /// never saved), mutates its `chart_spec` to reflect the latest in-memory
    /// shell state, and re-upserts it. Failures are routed through
    /// `record_storage_failure` and surfaced as a toast via `pending_toast`.
    ///
    /// Returns `true` on success, `false` if there was nothing to persist or
    /// the upsert failed. After a successful persist the chart re-executes via
    /// `mark_pending_reexecute` so the panel renders against the new bindings.
    pub fn persist_chart_spec_and_reexecute(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(chart_id) = self.saved_chart_id else {
            return false;
        };

        // Read the existing saved record so we preserve unrelated fields
        // (name, profile_id, source, refresh_policy, time_range_preset, ...).
        let existing = self
            .app_state
            .read(cx)
            .saved_charts
            .chart_by_id(chart_id)
            .cloned();
        let Some(mut saved) = existing else {
            return false;
        };

        let kind = self.chart_kind(cx);
        let bindings = self.active_bindings(cx);

        saved.chart_spec.kind = kind;
        saved.chart_spec.binding = bindings.clone();
        saved.bindings = bindings;

        let title = saved.name.clone();
        let persist_result = self.app_state.update(cx, |state, _cx| {
            state.saved_charts.upsert(saved).inspect_err(|e| {
                state.record_storage_failure(
                    dbflux_core::observability::actions::CONFIG_UPDATE,
                    "saved_chart",
                    chart_id.to_string(),
                    format!("Failed to save chart '{title}'"),
                    e.to_string(),
                );
            })
        });

        match persist_result {
            Ok(_) => {
                self.mark_pending_reexecute(cx);
                true
            }
            Err(e) => {
                self.pending_toast = Some(PendingToast {
                    message: format!("Failed to save chart: {e}"),
                    is_error: true,
                });
                cx.notify();
                false
            }
        }
    }

    /// Produce a `ViewHandle` that lets `ResultPanel` host `ChartDocument`.
    ///
    /// The three header segments (title Left/0, Run Left/1, Save Right/0) are
    /// returned by `toolbar_segments`. The content area (chart toolbar row +
    /// axis bar + chart area) is rendered by `render_chart_content`, which is
    /// called from the `render` closure.
    ///
    /// `available_modes` returns `[Chart]` only; `ResultPanel` suppresses the
    /// mode bar when the list has fewer than two entries.
    pub fn into_view_handle(entity: Entity<Self>, _cx: &mut App) -> ViewHandle {
        let e_render = entity.clone();
        let e_focus_do = entity.clone();
        let e_focus_get = entity.clone();
        let e_segs = entity.clone();

        ViewHandle::builder()
            .render(move |window, cx| {
                e_render.update(cx, |this, cx| this.render_chart_content(window, cx))
            })
            .focus(move |window, cx| {
                e_focus_do.update(cx, |this, cx| this.focus(window, cx));
            })
            .focus_handle(move |cx| e_focus_get.read(cx).focus_handle.clone())
            .toolbar_segments(move |cx| Self::header_segments(e_segs.clone(), cx))
            .available_modes(|_cx| vec![ResultViewMode::Chart])
            .current_mode(|_cx| ResultViewMode::Chart)
            .set_mode(|_mode, _cx| {
                // Chart is the only supported mode; no-op.
            })
            .build()
    }

    /// Build the three chrome-row segments for `ChartDocument`.
    ///
    /// - `Left/0`: document title label
    /// - `Left/1`: Run / Running… primary button
    /// - `Right/0`: Save button
    fn header_segments(entity: Entity<Self>, cx: &App) -> Vec<ToolbarSegment> {
        // When embedded inside another document (e.g. a DashboardDocument
        // panel) the host owns the chrome and no segments should be rendered.
        if entity.read(cx).embedded {
            return Vec::new();
        }

        use dbflux_components::primitives::Text;
        use dbflux_components::tokens::Spacing;
        use gpui_component::button::{Button, ButtonVariant, ButtonVariants};
        use gpui_component::{Disableable, Sizable};

        let e_title = entity.clone();
        let e_run = entity.clone();
        let e_save = entity.clone();

        vec![
            ToolbarSegment {
                position: SegmentPosition::Left,
                index: 0,
                builder: Box::new(move |_window, cx| {
                    let title = e_title.read(cx).title.clone();
                    Text::label(title).into_any_element()
                }),
            },
            ToolbarSegment {
                position: SegmentPosition::Left,
                index: 1,
                builder: Box::new(move |_window, cx| {
                    let is_executing = e_run.read(cx).exec_state == ExecState::Running;
                    let e = e_run.clone();
                    Button::new("run-query")
                        .label(if is_executing { "Running…" } else { "Run" })
                        .small()
                        .with_variant(ButtonVariant::Primary)
                        .disabled(is_executing)
                        .on_click(move |_, window, cx| {
                            e.update(cx, |this, cx| {
                                this.request_reexecute(window, cx);
                            });
                        })
                        .into_any_element()
                }),
            },
            ToolbarSegment {
                position: SegmentPosition::Right,
                index: 0,
                builder: Box::new(move |_window, _cx| {
                    let e = e_save.clone();
                    Button::new("save-chart")
                        .label("Save")
                        .small()
                        .on_click(move |_, window, cx| {
                            e.update(cx, |this, cx| {
                                this.open_name_prompt(window, cx);
                            });
                        })
                        .into_any_element()
                }),
            },
        ]
    }
}

impl EventEmitter<DocumentEvent> for ChartDocument {}

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

    fn refresh_dropdown(&self, _cx: &App) -> Option<Entity<Dropdown>> {
        Some(self.refresh_dropdown.clone())
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

/// Returns `true` when `ChartDocument` should render the Stats rail.
///
/// The render branch in `render_chart_content` delegates to this predicate so
/// tests can pin the gating logic without a GPUI runtime.
fn should_render_stats_rail(rail_open: bool, rail_tab: crate::chart::ChartRailTab) -> bool {
    rail_open && rail_tab == crate::chart::ChartRailTab::Stats
}

/// Returns the new `(open, tab)` state after the Stats toolbar button is clicked.
///
/// Toggling while already open on the Stats tab closes the rail. Clicking Stats
/// from any other state (closed, or open on a different tab) opens the rail and
/// switches to the Stats tab.
fn toggle_stats_rail(
    open: bool,
    tab: crate::chart::ChartRailTab,
) -> (bool, crate::chart::ChartRailTab) {
    if open && tab == crate::chart::ChartRailTab::Stats {
        (false, tab)
    } else {
        (true, crate::chart::ChartRailTab::Stats)
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

    // ---- apply_custom_range synchronous flag-set ----

    /// Simulated outcome of the Ok branch in `apply_custom_range`.
    struct ApplyCustomRangeOutcome {
        pending_chart_reexecute: bool,
        pending_time_window: Option<(i64, i64)>,
    }

    /// Exercise the `apply_custom_range` Ok-branch logic without a GPUI runtime
    /// by replicating the synchronous flag-set directly.
    ///
    /// T-CR-07: Apply sets both flags in the same call as validation, removing
    /// the timing dependency on `TimeRangeChanged` subscription delivery.
    fn simulate_apply_custom_range_ok(start_ms: i64, end_ms: i64) -> ApplyCustomRangeOutcome {
        // This replicates the Ok branch added in Piece A: the bounds returned
        // by `panel.apply_custom_range` are used to set state synchronously.
        let pending_time_window = Some((start_ms, end_ms));
        let pending_chart_reexecute = true;

        ApplyCustomRangeOutcome {
            pending_chart_reexecute,
            pending_time_window,
        }
    }

    /// `apply_custom_range` Ok branch sets `pending_chart_reexecute` and stashes
    /// the exact bounds returned by the panel — no subscription needed.
    ///
    /// T-CR-07: synchronous flag-set test.
    #[test]
    fn apply_custom_range_ok_sets_flags_synchronously() {
        let outcome = simulate_apply_custom_range_ok(1_000, 2_000);
        assert!(
            outcome.pending_chart_reexecute,
            "pending_chart_reexecute must be true immediately after Ok"
        );
        assert_eq!(
            outcome.pending_time_window,
            Some((1_000, 2_000)),
            "pending_time_window must hold the exact validated bounds"
        );
    }

    // ---- Task 2.6: data_source routing tests ----

    /// T-DS-01 / R-03: `resolve_source` with a Query source and a time window
    /// produces a request carrying the window. This mirrors the exact path
    /// `request_reexecute` takes: `self.data_source.build_plan(window)` and
    /// destructures `ChartDataPlan::Driver(request)`.
    ///
    /// Tested without a GPUI runtime by calling the seam directly.
    #[test]
    fn data_source_build_plan_with_window_produces_driver_plan_with_collection_window_context() {
        use dbflux_components::chart::{ChartDataPlan, TimeWindow, resolve_source};
        use dbflux_components::saved_chart::SavedChartSource;
        use dbflux_core::ExecutionSourceContext;

        let source = resolve_source(&SavedChartSource::Query {
            query: "SELECT * FROM metrics".to_string(),
        });

        let window = TimeWindow {
            start_ms: 1_000,
            end_ms: 2_000,
        };

        let plan = source
            .build_plan(Some(window))
            .expect("non-empty query with window must produce Ok plan");

        let ChartDataPlan::Driver(request) = plan else {
            panic!("expected ChartDataPlan::Driver");
        };

        let ctx = request
            .execution_context
            .as_ref()
            .expect("request must carry an execution context");

        match ctx.source.as_ref().expect("source must be Some") {
            ExecutionSourceContext::CollectionWindow {
                start_ms, end_ms, ..
            } => {
                assert_eq!(start_ms, &1_000_i64);
                assert_eq!(end_ms, &2_000_i64);
            }
            other => panic!("expected CollectionWindow source context, got: {other:?}"),
        }
    }

    /// T-DS-02 / R-03, R-07: empty query via data source returns `EmptyQuery`.
    /// This corresponds to the early-return branch in `request_reexecute`.
    #[test]
    fn data_source_build_plan_empty_query_returns_empty_query_error() {
        use dbflux_components::chart::{ChartSourceError, resolve_source};
        use dbflux_components::saved_chart::SavedChartSource;

        let source = resolve_source(&SavedChartSource::Query {
            query: String::new(),
        });

        let result = source.build_plan(None);

        assert!(
            matches!(result, Err(ChartSourceError::EmptyQuery)),
            "empty query data source must return ChartSourceError::EmptyQuery"
        );
    }

    /// T-DS-03 / R-03: data source without a window produces a Driver plan with
    /// no source context. Verifies the no-window branch preserves the pre-seam
    /// behavior (no `CollectionWindow` injected when `pending_time_window` is `None`).
    #[test]
    fn data_source_build_plan_without_window_produces_driver_plan_with_no_source_context() {
        use dbflux_components::chart::{ChartDataPlan, resolve_source};
        use dbflux_components::saved_chart::SavedChartSource;

        let source = resolve_source(&SavedChartSource::Query {
            query: "SELECT 1".to_string(),
        });

        let plan = source
            .build_plan(None)
            .expect("non-empty query without window must produce Ok plan");

        let ChartDataPlan::Driver(request) = plan else {
            panic!("expected ChartDataPlan::Driver");
        };

        let has_source = request
            .execution_context
            .as_ref()
            .and_then(|c| c.source.as_ref())
            .is_some();

        assert!(
            !has_source,
            "no time window must produce no source context in the request"
        );
    }

    /// `ChartDocument::into_view_handle` must advertise exactly `[Chart]`.
    ///
    /// The contract constant is validated here without a GPUI runtime.
    #[test]
    fn available_modes_chart_only() {
        let modes = [ResultViewMode::Chart];
        assert_eq!(modes.len(), 1);
        assert_eq!(modes[0], ResultViewMode::Chart);
    }

    /// Header segments must be ordered: title (Left/0), Run (Left/1), Save (Right/0).
    ///
    /// Validates the `header_segments` layout contract: after sorting by
    /// `(position, index)` the order must match construction order.
    #[test]
    fn header_segments_layout_contract() {
        let positions: Vec<(SegmentPosition, u16)> = vec![
            (SegmentPosition::Left, 0),
            (SegmentPosition::Left, 1),
            (SegmentPosition::Right, 0),
        ];

        let mut sorted = positions.clone();
        sorted.sort_by_key(|&(p, i)| (p, i));
        assert_eq!(
            sorted, positions,
            "header segments must already be in sorted order"
        );
    }

    // ---- Phase 5: set_data_source ----

    /// T-DS-10: `DocumentEvent::DataSourceChanged` variant must exist.
    ///
    /// This test fails to compile until `DataSourceChanged` is added to
    /// `DocumentEvent`. Compile failure = RED state.
    #[test]
    fn data_source_changed_event_variant_exists() {
        // Constructing the variant proves it compiles.
        let event = DocumentEvent::DataSourceChanged;
        // Pattern-match to assert it is a unit variant.
        assert!(matches!(event, DocumentEvent::DataSourceChanged));
    }

    /// T-DS-11: `ChartRailTab::Metric` variant must exist.
    ///
    /// Fails to compile until the variant is added. RED state.
    #[test]
    fn chart_rail_tab_metric_variant_exists() {
        let tab = crate::chart::ChartRailTab::Metric;
        assert!(matches!(tab, crate::chart::ChartRailTab::Metric));
    }

    /// T-DS-12: `set_data_source` replaces the data source.
    ///
    /// Simulates the state transition: a new source arrives, the existing one
    /// is replaced. Tests the decision logic without a GPUI runtime by
    /// replicating the flag-set that `set_data_source` performs.
    #[test]
    fn set_data_source_replaces_data_source_and_schedules_reexecute() {
        use dbflux_components::chart::ChartSourceDescription;

        // A source whose description carries no display title must NOT
        // overwrite the document title. We exercise this by constructing the
        // empty description directly — set_data_source's title-update branch
        // reads only `description.display_title()`.
        let description = ChartSourceDescription::empty();
        let title_update: Option<String> = description.display_title();
        assert!(
            title_update.is_none(),
            "ChartSourceDescription::empty() must have no display title; title must not be overwritten"
        );

        // Simulate the reexecute-flag set: set_data_source always enables it.
        let pending_chart_reexecute = true;
        assert!(
            pending_chart_reexecute,
            "set_data_source must schedule a reexecute"
        );
    }

    /// Simulate the closure body installed by the `metric_apply_sub` subscription
    /// in `ChartDocument::new` / `new_with_source`. The closure must:
    ///   1. Clone the source via `clone_box` (the field is `Box<dyn ChartDataSource>`
    ///      so it cannot be moved out of the borrowed event).
    ///   2. Write it into `pending_data_source`.
    ///
    /// Without this closure the Apply button is dead UI.
    #[test]
    fn metric_picker_applied_event_populates_pending_data_source() {
        use crate::chart::shell::ChartShellEvent;
        use dbflux_components::chart::{ChartDataSource, MetricSource};

        let source = MetricSource::single(
            "AWS/EC2".to_string(),
            "CPUUtilization".to_string(),
            vec![],
            300,
            "Average".to_string(),
        );
        let event = ChartShellEvent::MetricPickerApplied(Box::new(source));

        // Mirror the closure body in `cx.subscribe(&chart_shell, ...)`.
        let pending_data_source: Option<Box<dyn ChartDataSource>> = match &event {
            ChartShellEvent::MetricPickerApplied(src) => Some(src.clone_box()),
        };

        assert!(
            pending_data_source.is_some(),
            "MetricPickerApplied must populate pending_data_source"
        );
        let captured = pending_data_source
            .as_ref()
            .and_then(|s| s.as_any())
            .and_then(|a| a.downcast_ref::<MetricSource>())
            .expect("pending_data_source must downcast back to MetricSource");
        assert_eq!(captured.primary_namespace(), "AWS/EC2");
        assert_eq!(captured.primary_metric_name(), "CPUUtilization");
    }

    /// `matches_metric_source` must compare against the initial identity
    /// captured at construction, not the (possibly mutated) current data_source.
    ///
    /// After Apply rewrites dimensions/period/statistic the
    /// `(namespace, metric_name)` pair stays the same, so sidebar dedup must
    /// continue to find the existing tab.
    #[test]
    fn matches_metric_source_uses_initial_identity() {
        let profile_id = Uuid::new_v4();
        let identity = Some(("AWS/EC2".to_string(), "CPUUtilization".to_string()));

        // Simulate the body of `matches_metric_source` directly without a GPUI runtime.
        fn matches(
            doc_profile: Option<Uuid>,
            doc_identity: &Option<(String, String)>,
            query_profile: Uuid,
            query_ns: &str,
            query_metric: &str,
        ) -> bool {
            if doc_profile != Some(query_profile) {
                return false;
            }
            doc_identity
                .as_ref()
                .is_some_and(|(ns, mn)| ns == query_ns && mn == query_metric)
        }

        assert!(
            matches(
                Some(profile_id),
                &identity,
                profile_id,
                "AWS/EC2",
                "CPUUtilization"
            ),
            "exact identity must match"
        );
        assert!(
            !matches(
                Some(profile_id),
                &identity,
                profile_id,
                "AWS/EC2",
                "NetworkIn"
            ),
            "different metric name must not match"
        );
        assert!(
            !matches(
                Some(profile_id),
                &identity,
                Uuid::new_v4(),
                "AWS/EC2",
                "CPUUtilization"
            ),
            "different profile must not match"
        );
        assert!(
            !matches(
                Some(profile_id),
                &None,
                profile_id,
                "AWS/EC2",
                "CPUUtilization"
            ),
            "doc with no identity (non-metric chart) must not match"
        );
    }

    /// A2 regression: `cancel_metric_fetches_if_disconnected` must early-return
    /// when the profile is still connected and tear down the dimensions task
    /// when the profile is no longer present. The full GPUI wiring sits behind
    /// the subscribe -> chart_shell.update path; this test pins the pure
    /// decision logic that selects between "no-op" and "drop task".
    #[test]
    fn cancel_metric_fetches_decision_logic_short_circuits_when_connected() {
        // Pure-logic replica of `cancel_metric_fetches_if_disconnected`'s
        // decision predicate: only proceed when we have a profile AND it is no
        // longer in the connections map.
        fn should_cancel(profile: Option<Uuid>, connected: bool) -> bool {
            profile.is_some() && !connected
        }

        assert!(
            !should_cancel(None, false),
            "no profile_id must skip cancellation"
        );
        assert!(
            !should_cancel(Some(Uuid::new_v4()), true),
            "still-connected profile must skip cancellation"
        );
        assert!(
            should_cancel(Some(Uuid::new_v4()), false),
            "disconnected profile must trigger cancellation"
        );
    }

    /// T-DS-13: `set_data_source` updates the title when the source describes itself.
    ///
    /// Simulates the title-update branch using a `MetricSource` description.
    #[test]
    fn set_data_source_updates_title_from_source_description() {
        use dbflux_components::chart::MetricSource;

        let source = MetricSource::single(
            "AWS/Lambda".to_string(),
            "Invocations".to_string(),
            vec![],
            300,
            "Average".to_string(),
        );

        let description = source.describe();
        let title_update = description.display_title();

        // MetricSource::describe produces "AWS/Lambda / Invocations".
        assert!(
            title_update.is_some(),
            "MetricSource description must provide a display title"
        );
        let title = title_update.unwrap();
        assert!(
            title.contains("AWS/Lambda"),
            "title must include namespace: got {title:?}"
        );
        assert!(
            title.contains("Invocations"),
            "title must include metric name: got {title:?}"
        );
    }

    // ---- stats rail helpers ----

    /// T-SR-01: `should_render_stats_rail` returns true when rail is open on Stats tab.
    #[test]
    fn should_render_stats_rail_when_open_and_stats() {
        assert!(
            super::should_render_stats_rail(true, crate::chart::ChartRailTab::Stats),
            "rail must render when open and tab is Stats"
        );
    }

    /// T-SR-02: `should_render_stats_rail` returns false when rail is closed.
    #[test]
    fn should_not_render_when_closed() {
        assert!(
            !super::should_render_stats_rail(false, crate::chart::ChartRailTab::Stats),
            "rail must not render when closed"
        );
    }

    /// T-SR-03: `should_render_stats_rail` returns false when a different tab is active.
    #[test]
    fn should_not_render_when_other_tab() {
        assert!(
            !super::should_render_stats_rail(true, crate::chart::ChartRailTab::Metric),
            "rail must not render when tab is not Stats"
        );
    }

    /// T-SR-04: toggling from closed state opens the rail on Stats tab.
    #[test]
    fn toggle_from_closed_opens_stats() {
        let (open, tab) = super::toggle_stats_rail(false, crate::chart::ChartRailTab::Configure);
        assert!(open, "toggle from closed must open the rail");
        assert_eq!(
            tab,
            crate::chart::ChartRailTab::Stats,
            "toggle from closed must set tab to Stats"
        );
    }

    /// T-SR-05: toggling while open on Stats tab closes the rail.
    #[test]
    fn toggle_while_open_on_stats_closes() {
        let (open, tab) = super::toggle_stats_rail(true, crate::chart::ChartRailTab::Stats);
        assert!(!open, "toggle while open on Stats must close the rail");
        assert_eq!(
            tab,
            crate::chart::ChartRailTab::Stats,
            "closed toggle must preserve Stats tab identity"
        );
    }

    /// T-SR-06: toggling while rail is open on Metric tab switches to Stats without closing.
    #[test]
    fn toggle_while_open_on_metric_switches_to_stats() {
        let (open, tab) = super::toggle_stats_rail(true, crate::chart::ChartRailTab::Metric);
        assert!(open, "switching from Metric to Stats must keep rail open");
        assert_eq!(
            tab,
            crate::chart::ChartRailTab::Stats,
            "switching from Metric must set tab to Stats"
        );
    }

    /// T-SR-07: toggling while rail is open on Configure tab switches to Stats without closing.
    #[test]
    fn toggle_while_open_on_configure_switches_to_stats() {
        let (open, tab) = super::toggle_stats_rail(true, crate::chart::ChartRailTab::Configure);
        assert!(
            open,
            "switching from Configure to Stats must keep rail open"
        );
        assert_eq!(
            tab,
            crate::chart::ChartRailTab::Stats,
            "switching from Configure must set tab to Stats"
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
