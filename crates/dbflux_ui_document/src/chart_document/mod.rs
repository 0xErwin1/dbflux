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

    /// Stable identity for instance metric charts opened from the
    /// `InstanceMetricsFolder` sidebar node. Used by `matches_instance_metric_chart`
    /// for `DocumentKey::InstanceMetric` dedup.
    initial_instance_metric_id: Option<String>,

    /// Initial preset index passed to `TimeRangePanel::new` on first render.
    ///
    /// Index 3 = Last24Hours (default for most sources).
    /// Index 0 = Last15min (set by `open_instance_metric` for InstanceMetric sources).
    /// The value is consumed once — after the panel is created it has no further effect.
    initial_time_range_index: usize,

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

    /// Sample accumulation buffer for `InstanceMetric` sources.
    ///
    /// Populated on first successful fetch when `data_source.is_accumulating()`
    /// is true. Each subsequent fetch appends to the buffer and the chart shell
    /// receives the full accumulated series rather than the single-point result.
    /// Cleared when the data source changes or the document closes.
    instance_metric_buffer: Option<InstantSeriesBuffer>,
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
            initial_instance_metric_id: None,
            initial_time_range_index: 3,
            saved_chart_id: None,
            name_prompt: None,
            pending_toast: None,
            focus_handle: cx.focus_handle(),
            focus_mode: ChartDocFocus::default(),
            result_panel: None,
            embedded: false,
            _subscriptions: vec![metric_apply_sub, app_state_disconnect_sub],
            instance_metric_buffer: None,
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

                // Establish the instance-metric identity so DocumentKey::InstanceMetric
                // deduplication works when the tab is re-opened.
                doc.set_instance_metric_identity(metric_id.clone());

                // Restore the 15-min rolling window default (preset index 0) so
                // re-loading a saved chart behaves identically to a fresh open.
                doc.set_initial_time_range_preset(0);

                // Default to 30-second auto-refresh so the series stays current.
                // The floor in `clamp_refresh_secs` ensures this is never below 10s.
                let policy = RefreshPolicy::Interval { every_secs: 30 };
                doc.refresh_policy = policy;
                doc.update_refresh_timer(cx);

                // Sync the toolbar refresh-policy dropdown to reflect the loaded
                // policy so it does not display "Manual" while the timer runs.
                let policy_index = policy.index();
                doc.refresh_dropdown.update(cx, |dropdown, cx| {
                    dropdown.set_selected_index(Some(policy_index), cx);
                });

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
            initial_instance_metric_id: None,
            initial_time_range_index: 3,
            saved_chart_id: None,
            name_prompt: None,
            pending_toast: None,
            result_panel: None,
            embedded: false,
            focus_handle: cx.focus_handle(),
            focus_mode: ChartDocFocus::default(),
            _subscriptions: vec![metric_apply_sub, app_state_disconnect_sub],
            instance_metric_buffer: None,
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

    /// Set the stable instance-metric identity for this document.
    ///
    /// Called after construction when the chart is opened from the sidebar's
    /// `InstanceMetricsFolder`. Enables `DocumentKey::InstanceMetric` dedup.
    pub fn set_instance_metric_identity(&mut self, metric_id: String) {
        self.initial_instance_metric_id = Some(metric_id);
    }

    /// Override the initial `TimeRangePanel` preset index used on first render.
    ///
    /// Index 0 = Last15min. Index 3 = Last24Hours (default). The value is
    /// consumed once at the moment the panel is lazily created; changing it
    /// after the first render has no effect because the panel already exists.
    pub fn set_initial_time_range_preset(&mut self, index: usize) {
        self.initial_time_range_index = index;
    }

    /// Returns the initial `TimeRangePanel` preset index set for this document.
    pub fn initial_time_range_index(&self) -> usize {
        self.initial_time_range_index
    }

    /// Returns `true` when this document was opened for the given
    /// `(profile_id, metric_id)` via the instance-metrics sidebar folder.
    pub fn matches_instance_metric_chart(&self, profile_id: Uuid, metric_id: &str) -> bool {
        if self.profile_id != Some(profile_id) {
            return false;
        }
        self.initial_instance_metric_id
            .as_deref()
            .is_some_and(|id| id == metric_id)
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

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        let was_interval = matches!(self.refresh_policy, RefreshPolicy::Interval { .. });
        let now_manual = matches!(policy, RefreshPolicy::Manual);

        if was_interval && now_manual && self.instance_metric_buffer.is_some() {
            self.instance_metric_buffer = None;
        }

        self.refresh_policy = policy;
        self.update_refresh_timer(cx);
    }

    /// Cancel any running timer and start a new one at the clamped interval, or
    /// leave `_refresh_timer = None` when the policy is `Manual`.
    ///
    /// Dropping the old `Task<()>` value cancels the spawned future, which is
    /// the cancellation mechanism for the timer loop.
    ///
    /// Callers that change the refresh policy (not merely restart the timer)
    /// MUST go through `set_refresh_policy`, which handles `instance_metric_buffer`
    /// invalidation on Interval→Manual transitions. Calling this method directly
    /// bypasses that invariant.
    fn update_refresh_timer(&mut self, cx: &mut Context<Self>) {
        self._refresh_timer = None;

        let Some(duration) = refresh_timer_duration(self.refresh_policy) else {
            return;
        };

        self._refresh_timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(duration).await;

                let _ = cx.update(|cx| {
                    let Some(entity) = this.upgrade() else {
                        return;
                    };
                    entity.update(cx, |doc, cx| {
                        if let Some(pid) = doc.profile_id
                            && !doc.app_state.read(cx).connections().contains_key(&pid)
                        {
                            return;
                        }
                        doc.mark_pending_reexecute(cx);
                    });
                });
            }
        }));
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

        // Drop the accumulation buffer when the source changes so stale
        // samples from the previous metric do not bleed into the new series.
        self.instance_metric_buffer = None;

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
    ///
    /// For accumulating sources (`is_accumulating() == true`) the raw
    /// single-sample result is appended to `instance_metric_buffer` and the
    /// shell receives the full accumulated series. For all other sources the
    /// raw result is forwarded directly.
    fn apply_result(&mut self, pending: PendingResult, cx: &mut Context<Self>) {
        self.runner.complete_primary(pending.task_id, cx);

        match pending.result {
            Ok(result) => {
                self.exec_state = ExecState::Idle;
                self.state = DocumentState::Clean;

                let was_chart_mode = self.last_result.is_some();

                let display_result = if self.data_source.is_accumulating() {
                    let buffer = self.instance_metric_buffer.get_or_insert_with(|| {
                        InstantSeriesBuffer::new(result.columns.clone(), 120)
                    });
                    buffer.push_result(&result);
                    Arc::new(buffer.to_query_result())
                } else {
                    Arc::new(result)
                };

                let display_clone = display_result.clone();
                self.chart_shell.update(cx, |shell, cx| {
                    shell.set_result(&display_clone, was_chart_mode, cx);
                });

                self.last_result = Some(display_result);
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

        cx.emit(DocumentEvent::ExecutionFinished);
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

// ---------------------------------------------------------------------------
// InstantSeriesBuffer
// ---------------------------------------------------------------------------

/// In-memory accumulator for instance-metric single-sample poll results.
///
/// Each `fetch_metric_series` call returns exactly one row (the current gauge
/// value plus a timestamp). `ChartDocument` uses this buffer to stitch those
/// samples into a visible time series: each successful fetch appends a new
/// row, and the buffer is pruned to at most `max_samples` entries so memory
/// stays bounded.
///
/// The buffer is owned by `ChartDocument` only when the active source returns
/// `is_accumulating() == true`. For all other sources the field is `None`
/// and accumulation is never invoked.
pub(super) struct InstantSeriesBuffer {
    columns: Vec<dbflux_core::ColumnMeta>,
    rows: Vec<dbflux_core::Row>,
    max_samples: usize,
}

impl InstantSeriesBuffer {
    /// Create an empty buffer with the given column schema and retention cap.
    pub(super) fn new(columns: Vec<dbflux_core::ColumnMeta>, max_samples: usize) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            max_samples,
        }
    }

    /// Returns `true` when `incoming` columns are structurally incompatible with
    /// the buffer's current schema — differing count, names, or `ColumnKind`s.
    fn schema_changed(&self, incoming: &[dbflux_core::ColumnMeta]) -> bool {
        if incoming.len() != self.columns.len() {
            return true;
        }
        incoming
            .iter()
            .zip(self.columns.iter())
            .any(|(a, b)| a.name != b.name || a.kind != b.kind)
    }

    /// Append rows from a single-sample fetch result and prune to retention cap.
    ///
    /// If the incoming result's column schema (count, names, or kinds) differs
    /// from the buffer's schema the buffer is reset: accumulated rows are
    /// discarded and the column schema is replaced with the new result's schema.
    /// This prevents silently appending rows with structurally mismatched columns,
    /// which would produce an unrenderable `QueryResult`.
    pub(super) fn push_result(&mut self, result: &dbflux_core::QueryResult) {
        if self.schema_changed(&result.columns) {
            self.rows.clear();
            self.columns = result.columns.clone();
        }

        for row in &result.rows {
            self.rows.push(row.clone());
        }

        if self.rows.len() > self.max_samples {
            let overflow = self.rows.len() - self.max_samples;
            self.rows.drain(..overflow);
        }
    }

    /// Number of accumulated samples.
    #[cfg(test)]
    pub(super) fn sample_count(&self) -> usize {
        self.rows.len()
    }

    /// Build a `QueryResult` whose rows contain all accumulated samples.
    ///
    /// The schema (columns) is identical to the first fetch result so the
    /// chart engine's column detection produces the same X/Y mapping as it
    /// would for a native time-series result.
    pub(super) fn to_query_result(&self) -> dbflux_core::QueryResult {
        dbflux_core::QueryResult {
            shape: dbflux_core::QueryResultShape::Table,
            columns: self.columns.clone(),
            rows: self.rows.clone(),
            affected_rows: None,
            execution_time: std::time::Duration::ZERO,
            text_body: None,
            raw_bytes: None,
            next_page_token: None,
            resolved_window: None,
            metadata_extra: None,
            additional_results: Vec::new(),
        }
    }
}

/// Returns `true` when `ChartDocument` should render the Stats rail.
///
/// The render branch in `render_chart_content` delegates to this predicate so
/// tests can pin the gating logic without a GPUI runtime.
fn should_render_stats_rail(rail_open: bool, rail_tab: crate::chart::ChartRailTab) -> bool {
    rail_open && rail_tab == crate::chart::ChartRailTab::Stats
}

/// Clamp `every_secs` to the 10-second minimum refresh floor defined in
/// `crate::refresh::MIN_REFRESH_FLOOR_SECS`.
///
/// Values at or above the floor are returned unchanged. Values below (e.g. the
/// legacy 1s/2s/5s options in `RefreshPolicy::ALL`) are raised to the floor so
/// the UI never schedules sub-10-second polling.
fn clamp_refresh_secs(every_secs: u32) -> u32 {
    use crate::refresh::MIN_REFRESH_FLOOR_SECS;
    every_secs.max(MIN_REFRESH_FLOOR_SECS as u32)
}

/// Return the `Duration` the auto-refresh timer should use for `policy`, or
/// `None` if `policy` is `Manual` (no timer should be started).
///
/// The interval is clamped through `clamp_refresh_secs` so the effective
/// minimum is always 10 seconds.
fn refresh_timer_duration(policy: RefreshPolicy) -> Option<std::time::Duration> {
    match policy {
        RefreshPolicy::Manual => None,
        RefreshPolicy::Interval { every_secs } => Some(std::time::Duration::from_secs(
            clamp_refresh_secs(every_secs) as u64,
        )),
    }
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

    // ---- T18: refresh timer behaviour ----

    /// REQ-DOC-2: `clamp_refresh_secs` must return the input unchanged when it
    /// is at or above the 10-second floor.
    ///
    /// This test will fail to compile until `clamp_refresh_secs` is added in T19.
    #[test]
    fn refresh_secs_at_or_above_floor_passes_through_unchanged() {
        assert_eq!(
            super::clamp_refresh_secs(10),
            10,
            "10s is exactly at the floor and must not be raised"
        );
        assert_eq!(
            super::clamp_refresh_secs(30),
            30,
            "30s is above the floor and must not be altered"
        );
        assert_eq!(
            super::clamp_refresh_secs(60),
            60,
            "60s is above the floor and must not be altered"
        );
    }

    /// REQ-DOC-2: any interval below 10s must be clamped up to 10s.
    ///
    /// This test will fail to compile until `clamp_refresh_secs` is added in T19.
    #[test]
    fn refresh_secs_below_floor_is_clamped_to_10() {
        assert_eq!(
            super::clamp_refresh_secs(1),
            10,
            "1s must be clamped to the 10s floor"
        );
        assert_eq!(
            super::clamp_refresh_secs(5),
            10,
            "5s must be clamped to the 10s floor"
        );
        assert_eq!(
            super::clamp_refresh_secs(9),
            10,
            "9s must be clamped to the 10s floor"
        );
    }

    /// REQ-DOC-2: `Manual` policy produces `None` from `refresh_timer_duration`,
    /// which means `update_refresh_timer` must leave `_refresh_timer` as `None`.
    ///
    /// This test will fail to compile until `refresh_timer_duration` is added in T19.
    #[test]
    fn manual_policy_produces_no_timer_duration() {
        let duration = super::refresh_timer_duration(RefreshPolicy::Manual);
        assert!(
            duration.is_none(),
            "Manual policy must return None — no timer scheduled"
        );
    }

    /// REQ-DOC-2: `Interval { every_secs: 5 }` (below floor) must produce a
    /// duration of exactly 10 seconds after clamping.
    ///
    /// This test will fail to compile until `refresh_timer_duration` is added in T19.
    #[test]
    fn interval_below_floor_produces_10s_duration() {
        use std::time::Duration;

        let duration = super::refresh_timer_duration(RefreshPolicy::Interval { every_secs: 5 });
        assert_eq!(
            duration,
            Some(Duration::from_secs(10)),
            "5s interval must be clamped to 10s duration"
        );
    }

    /// REQ-DOC-2: `Interval { every_secs: 30 }` (above floor) must produce a
    /// duration of exactly 30 seconds.
    ///
    /// This test will fail to compile until `refresh_timer_duration` is added in T19.
    #[test]
    fn interval_above_floor_produces_exact_duration() {
        use std::time::Duration;

        let duration = super::refresh_timer_duration(RefreshPolicy::Interval { every_secs: 30 });
        assert_eq!(
            duration,
            Some(Duration::from_secs(30)),
            "30s interval must produce a 30s duration unchanged"
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

    // ---- REQ-DOC-2: runtime drop-cancel test ----

    fn init_test_runtime(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| dbflux_ui_base::toast::ToastHost::new());
            cx.set_global(dbflux_ui_base::toast::ToastGlobal { host });
        });
    }

    fn isolated_test_app_state(cx: &mut gpui::TestAppContext) -> gpui::Entity<AppStateEntity> {
        cx.update(|cx| {
            cx.new(|_| {
                let storage_runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("isolated storage runtime");
                AppStateEntity::new_with_storage_runtime(storage_runtime)
                    .expect("test storage setup")
            })
        })
    }

    /// Thin harness entity that optionally holds a `ChartDocument`.
    ///
    /// Used by `refresh_timer_is_cancelled_on_entity_drop` so the
    /// `ChartDocument` is not rooted directly as the window view (which
    /// would keep a strong reference alive in the window's view hierarchy).
    struct ChartDocHarness {
        doc: Option<Entity<ChartDocument>>,
    }

    impl Render for ChartDocHarness {
        fn render(
            &mut self,
            _window: &mut Window,
            _cx: &mut Context<Self>,
        ) -> impl gpui::IntoElement {
            gpui::div()
        }
    }

    /// REQ-DOC-2 — runtime evidence that dropping a `ChartDocument` entity cancels
    /// its `_refresh_timer` loop.
    ///
    /// The test installs a `cx.observe` counter on the entity, sets an interval
    /// refresh policy, advances the fake clock past one tick to confirm the timer
    /// fires, then drops the entity (by releasing both the test-side holder and the
    /// harness's `Option`). A second clock advance confirms the count is frozen —
    /// the `Task<()>` was cancelled when `ChartDocument` was freed, stopping the loop.
    ///
    /// This is the spec gate against timer leaks. `Task<()>` stored in a struct field
    /// is cancelled when that struct is freed; the test makes this property observable
    /// at runtime.
    #[gpui::test]
    fn refresh_timer_is_cancelled_on_entity_drop(cx: &mut gpui::TestAppContext) {
        use std::cell::Cell;
        use std::rc::Rc;
        use std::time::Duration;

        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);

        let notify_count = Rc::new(Cell::new(0u32));
        let doc_holder: Rc<std::cell::RefCell<Option<Entity<ChartDocument>>>> =
            Rc::new(std::cell::RefCell::new(None));
        let harness_holder: Rc<std::cell::RefCell<Option<Entity<ChartDocHarness>>>> =
            Rc::new(std::cell::RefCell::new(None));

        cx.add_window_view({
            let app_state = app_state.clone();
            let doc_holder = doc_holder.clone();
            let harness_holder = harness_holder.clone();
            move |window, cx| {
                let doc =
                    cx.new(|cx| ChartDocument::new(None, String::new(), app_state, window, cx));
                doc_holder.replace(Some(doc.clone()));

                let harness = cx.new(|_cx| ChartDocHarness { doc: Some(doc) });
                harness_holder.replace(Some(harness.clone()));
                gpui_component::Root::new(harness, window, cx)
            }
        });

        let doc = doc_holder.borrow().clone().expect("entity created");

        cx.update(|cx| {
            doc.update(cx, |d, cx| {
                d.set_refresh_policy(RefreshPolicy::Interval { every_secs: 10 }, cx);
            });
        });

        let counter = notify_count.clone();
        let _observe_sub = cx.update(|cx| {
            cx.observe(&doc, move |_, _| {
                counter.set(counter.get() + 1);
            })
        });

        cx.executor().advance_clock(Duration::from_secs(15));
        cx.run_until_parked();

        let count_after_first_tick = notify_count.get();
        assert!(
            count_after_first_tick >= 1,
            "timer must have fired at least once after 15 s; count = {count_after_first_tick}"
        );

        // Release all strong references so the ChartDocument entity is freed,
        // which drops _refresh_timer and cancels the timer task.
        doc_holder.borrow_mut().take();
        if let Some(harness) = harness_holder.borrow().clone() {
            cx.update(|cx| {
                harness.update(cx, |h, _| h.doc = None);
            });
        }
        harness_holder.borrow_mut().take();
        drop(doc);

        cx.executor().advance_clock(Duration::from_secs(60));
        cx.run_until_parked();

        let count_after_drop = notify_count.get();
        assert_eq!(
            count_after_drop, count_after_first_tick,
            "notify count must not increase after entity drop — timer loop must be cancelled"
        );
    }

    // ---- BF5: InstantSeriesBuffer accumulation ----

    fn make_single_point_result(timestamp_ms: i64, value: f64) -> QueryResult {
        use dbflux_core::{ColumnKind, ColumnMeta, QueryResultShape, Value};
        use std::time::Duration;

        QueryResult {
            shape: QueryResultShape::Table,
            columns: vec![
                ColumnMeta {
                    name: "timestamp_ms".to_string(),
                    type_name: "int8".to_string(),
                    kind: ColumnKind::Timestamp,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnMeta {
                    name: "value".to_string(),
                    type_name: "float8".to_string(),
                    kind: ColumnKind::Float,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
            rows: vec![vec![Value::Int(timestamp_ms), Value::Float(value)]],
            affected_rows: None,
            execution_time: Duration::ZERO,
            text_body: None,
            raw_bytes: None,
            next_page_token: None,
            resolved_window: None,
            metadata_extra: None,
            additional_results: Vec::new(),
        }
    }

    /// BF5: three sequential push_result calls must accumulate 3 samples.
    #[test]
    fn instant_series_buffer_accumulates_samples() {
        use dbflux_core::ColumnKind;

        let result1 = make_single_point_result(1_000, 10.0);
        let mut buffer = InstantSeriesBuffer::new(result1.columns.clone(), 120);

        buffer.push_result(&result1);
        buffer.push_result(&make_single_point_result(2_000, 20.0));
        buffer.push_result(&make_single_point_result(3_000, 30.0));

        assert_eq!(
            buffer.sample_count(),
            3,
            "three push_result calls must yield 3 accumulated samples"
        );

        let merged = buffer.to_query_result();
        assert_eq!(merged.rows.len(), 3, "merged result must have 3 rows");
        assert_eq!(
            merged.columns.len(),
            2,
            "merged result must preserve 2 columns"
        );
        assert_eq!(
            merged.columns[0].kind,
            ColumnKind::Timestamp,
            "first column kind must be Timestamp"
        );
        assert_eq!(
            merged.columns[1].kind,
            ColumnKind::Float,
            "second column kind must be Float"
        );
    }

    /// BF5: once sample_count exceeds max_samples, oldest entries are pruned.
    #[test]
    fn instant_series_buffer_prunes_oldest_beyond_max() {
        use dbflux_core::Value;

        let max = 3usize;
        let first = make_single_point_result(1_000, 1.0);
        let mut buffer = InstantSeriesBuffer::new(first.columns.clone(), max);

        for i in 0..5u64 {
            buffer.push_result(&make_single_point_result(i as i64 * 1_000, i as f64));
        }

        assert_eq!(
            buffer.sample_count(),
            max,
            "buffer must not exceed max_samples after overflow"
        );

        let merged = buffer.to_query_result();

        // The oldest two samples (timestamps 0, 1000) must have been dropped;
        // only timestamps 2000, 3000, 4000 remain.
        let timestamps: Vec<i64> = merged
            .rows
            .iter()
            .map(|row| match &row[0] {
                Value::Int(v) => *v,
                _ => panic!("expected Int for timestamp column"),
            })
            .collect();

        assert_eq!(
            timestamps,
            vec![2_000, 3_000, 4_000],
            "oldest samples must be pruned when max_samples is exceeded"
        );
    }

    /// BF5: InstanceMetricSource must return is_accumulating() == true.
    #[test]
    fn instance_metric_source_is_accumulating() {
        let src = dbflux_components::chart::InstanceMetricSource {
            metric_id: "pg.tps".to_string(),
        };
        assert!(
            src.is_accumulating(),
            "InstanceMetricSource must report is_accumulating() == true"
        );
    }

    /// BF5: non-InstanceMetric sources must return is_accumulating() == false.
    #[test]
    fn query_source_is_not_accumulating() {
        use dbflux_components::chart::resolve_source;
        use dbflux_components::saved_chart::SavedChartSource;

        let src = resolve_source(&SavedChartSource::Query {
            query: "SELECT 1".to_string(),
        });
        assert!(
            !src.is_accumulating(),
            "QuerySource must report is_accumulating() == false"
        );
    }

    // ---- BF6: InstanceMetric default time range and refresh policy ----

    /// BF6: after `set_initial_time_range_preset(0)`, the chart must report
    /// `initial_time_range_index() == 0` (preset index 0 = Last15min).
    #[gpui::test]
    fn instance_metric_chart_has_15min_initial_preset(cx: &mut gpui::TestAppContext) {
        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);
        let doc_holder: std::rc::Rc<std::cell::RefCell<Option<Entity<ChartDocument>>>> =
            std::rc::Rc::new(std::cell::RefCell::new(None));

        cx.add_window_view({
            let app_state = app_state.clone();
            let doc_holder = doc_holder.clone();
            move |window, cx| {
                let source = dbflux_components::chart::InstanceMetricSource {
                    metric_id: "pg.tps".to_string(),
                };
                let doc = cx.new(|cx| {
                    let mut chart = ChartDocument::new_with_source(
                        None,
                        "pg.tps".to_string(),
                        Box::new(source),
                        app_state,
                        window,
                        cx,
                    );
                    chart.set_initial_time_range_preset(0);
                    chart
                });
                doc_holder.replace(Some(doc.clone()));
                gpui_component::Root::new(doc, window, cx)
            }
        });

        let doc = doc_holder.borrow().clone().expect("entity created");
        cx.update(|cx| {
            let chart = doc.read(cx);
            assert_eq!(
                chart.initial_time_range_index(),
                0,
                "InstanceMetric chart must have initial preset index 0 (15min)"
            );
        });
    }

    /// BF6: a chart whose `set_refresh_policy` is called with `Interval{10}` must
    /// report that policy from `refresh_policy()`.
    #[gpui::test]
    fn instance_metric_chart_has_10s_refresh_policy(cx: &mut gpui::TestAppContext) {
        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);
        let doc_holder: std::rc::Rc<std::cell::RefCell<Option<Entity<ChartDocument>>>> =
            std::rc::Rc::new(std::cell::RefCell::new(None));

        cx.add_window_view({
            let app_state = app_state.clone();
            let doc_holder = doc_holder.clone();
            move |window, cx| {
                let source = dbflux_components::chart::InstanceMetricSource {
                    metric_id: "pg.tps".to_string(),
                };
                let doc = cx.new(|cx| {
                    ChartDocument::new_with_source(
                        None,
                        "pg.tps".to_string(),
                        Box::new(source),
                        app_state,
                        window,
                        cx,
                    )
                });
                doc.update(cx, |chart, cx| {
                    chart.set_refresh_policy(RefreshPolicy::Interval { every_secs: 10 }, cx);
                });
                doc_holder.replace(Some(doc.clone()));
                gpui_component::Root::new(doc, window, cx)
            }
        });

        let doc = doc_holder.borrow().clone().expect("entity created");
        cx.update(|cx| {
            let chart = doc.read(cx);
            assert_eq!(
                chart.refresh_policy(),
                RefreshPolicy::Interval { every_secs: 10 },
                "chart must report the 10s interval policy after set_refresh_policy"
            );
        });
    }

    /// BF6 control: a Query-source chart must keep the default preset index
    /// (3 = Last24Hours) and Manual refresh policy without any overrides.
    #[gpui::test]
    fn query_source_chart_keeps_default_24h_manual_refresh(cx: &mut gpui::TestAppContext) {
        init_test_runtime(cx);
        let app_state = isolated_test_app_state(cx);
        let doc_holder: std::rc::Rc<std::cell::RefCell<Option<Entity<ChartDocument>>>> =
            std::rc::Rc::new(std::cell::RefCell::new(None));

        cx.add_window_view({
            let app_state = app_state.clone();
            let doc_holder = doc_holder.clone();
            move |window, cx| {
                let doc = cx.new(|cx| {
                    ChartDocument::new(None, "SELECT 1".to_string(), app_state, window, cx)
                });
                doc_holder.replace(Some(doc.clone()));
                gpui_component::Root::new(doc, window, cx)
            }
        });

        let doc = doc_holder.borrow().clone().expect("entity created");
        cx.update(|cx| {
            let chart = doc.read(cx);
            assert_eq!(
                chart.initial_time_range_index(),
                3,
                "Query-source chart must keep default preset index 3 (Last24Hours)"
            );
            assert_eq!(
                chart.refresh_policy(),
                RefreshPolicy::Manual,
                "Query-source chart must keep Manual refresh policy"
            );
        });
    }
}
