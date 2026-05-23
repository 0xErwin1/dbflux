//! `MetricPickerState` — UI state for the metric picker rail tab.
//!
//! This module contains the pure state machine for the metric picker.
//! All GPUI entities (`Entity<InputState>`, etc.) live as fields, but
//! all pure transition methods (`on_namespace_selected`, `build_metric_source`)
//! are exercisable without a running GPUI app.
//!
//! **Boundary-struct pattern**: rendering lives in `metric_picker_render.rs`
//! as `MetricPickerView`. `MetricPickerState` is NOT a GPUI entity. It is
//! owned by `ChartShell` and operated through `ChartShell`'s context.
//!
//! # Single chokepoint for capability check
//!
//! `host_supports_metric_catalog` is defined here and called from exactly
//! two locations: `toolbar.rs` (render guard) and `actions.rs::open_metrics_chart`.
//! No `driver_id` strings anywhere — capability bit + trait accessor only.

use dbflux_app::{MetricCatalogCache, MetricsPageView};
use dbflux_components::chart::MetricSource;
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged, InputState};
use dbflux_core::{DimensionFilter, MetricDescriptor, MetricNamespace};
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::{Context, Entity, Subscription, Task, WeakEntity};
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// State for a single metric namespace list (left column in the picker).
pub enum NamespaceState {
    /// No fetch has started yet. Kicked on first render of the Metric rail.
    NotFetched,
    /// A background fetch is in progress.
    Loading,
    /// Namespaces loaded and available.
    Loaded(Arc<Vec<MetricNamespace>>),
    /// Last fetch failed; stores the error message for display.
    Error(String),
}

/// State for the metrics list (right column in the picker, per selected namespace).
pub enum MetricsState {
    /// Awaiting namespace selection or first render.
    NotFetched,
    /// Initial page load in progress.
    Loading,
    /// At least one page loaded.
    Loaded {
        accumulated: Arc<Vec<MetricDescriptor>>,
        fully_loaded: bool,
    },
    /// Additional page load in progress; shows existing data while loading.
    LoadingMore {
        accumulated: Arc<Vec<MetricDescriptor>>,
    },
    /// Last fetch failed.
    Error(String),
}

/// Period preset labels and their second values.
pub const PERIOD_PRESETS: &[(u32, &str)] = &[
    (60, "1 min"),
    (300, "5 min"),
    (900, "15 min"),
    (3600, "1 hr"),
];

/// Default period (5 minutes) index into `PERIOD_PRESETS`.
pub const DEFAULT_PERIOD_IDX: usize = 1;

/// Statistic preset labels.
pub const STATISTIC_PRESETS: &[&str] = &["Average", "Sum", "Minimum", "Maximum", "SampleCount"];

/// Default statistic index into `STATISTIC_PRESETS`.
pub const DEFAULT_STATISTIC_IDX: usize = 0;

// ---------------------------------------------------------------------------
// MetricPickerState
// ---------------------------------------------------------------------------

/// UI state machine for the metric picker rail tab.
///
/// Owned by `ChartShell`. All field access is through `ChartShell`'s
/// `Context<ChartShell>` — never accessed as an independent GPUI entity.
///
/// Filter `InputState` entities require a `Window` and are created lazily
/// on the first render of the Metric rail (same pattern as `TimeRangePanel`
/// in `ChartDocument`).
pub struct MetricPickerState {
    pub profile_id: Uuid,
    pub app_state: Entity<AppStateEntity>,

    // Namespace column state.
    pub namespace_state: NamespaceState,
    pub selected_namespace: Option<MetricNamespace>,
    /// Lazily created on first render (requires Window).
    pub namespace_filter: Option<Entity<InputState>>,

    // Metrics column state.
    pub metrics_state: MetricsState,
    pub selected_metric: Option<MetricDescriptor>,
    /// Lazily created on first render (requires Window).
    pub metrics_filter: Option<Entity<InputState>>,

    // Configuration section.
    pub dimension_filter: DimensionFilter,
    pub period_dropdown: Entity<Dropdown>,
    pub statistic_dropdown: Entity<Dropdown>,
    pub period_s: u32,
    pub statistic: String,

    // In-flight task handles (dropped to cancel the picker's await;
    // the cache's underlying fetch continues and writes through).
    pub namespace_task: Option<Task<()>>,
    pub metrics_task: Option<Task<()>>,

    /// Subscriptions to dropdown selection changes.
    pub _subscriptions: Vec<Subscription>,
}

impl MetricPickerState {
    /// Create a new picker state for `profile_id`.
    ///
    /// All states start as `NotFetched` — no driver call is made here.
    /// The first render of the Metric rail tab triggers `ensure_namespaces_loaded`.
    ///
    /// Filter `InputState` entities are `None` here and created lazily
    /// by the render path on first render (the render path has a `Window`
    /// reference; this constructor runs in `shell.update(cx, ...)` which does not).
    pub fn new(
        profile_id: Uuid,
        app_state: Entity<AppStateEntity>,
        cx: &mut Context<super::shell::ChartShell>,
    ) -> Self {
        let period_items = PERIOD_PRESETS
            .iter()
            .map(|(_, label)| DropdownItem::new(*label))
            .collect::<Vec<_>>();

        let statistic_items = STATISTIC_PRESETS
            .iter()
            .map(|s| DropdownItem::new(*s))
            .collect::<Vec<_>>();

        let period_dropdown = cx.new(|_cx| {
            Dropdown::new("metric-picker-period")
                .items(period_items)
                .selected_index(Some(DEFAULT_PERIOD_IDX))
                .compact_trigger(true)
        });

        let statistic_dropdown = cx.new(|_cx| {
            Dropdown::new("metric-picker-statistic")
                .items(statistic_items)
                .selected_index(Some(DEFAULT_STATISTIC_IDX))
                .compact_trigger(true)
        });

        let period_sub = cx.subscribe(
            &period_dropdown,
            |shell: &mut super::shell::ChartShell, _, event: &DropdownSelectionChanged, _cx| {
                if let Some(picker) = &mut shell.metric_picker {
                    picker.period_s = PERIOD_PRESETS
                        .get(event.index)
                        .map(|(s, _)| *s)
                        .unwrap_or(300);
                }
            },
        );

        let statistic_sub = cx.subscribe(
            &statistic_dropdown,
            |shell: &mut super::shell::ChartShell, _, event: &DropdownSelectionChanged, _cx| {
                if let Some(picker) = &mut shell.metric_picker {
                    picker.statistic = STATISTIC_PRESETS
                        .get(event.index)
                        .copied()
                        .unwrap_or("Average")
                        .to_string();
                }
            },
        );

        Self {
            profile_id,
            app_state,
            namespace_state: NamespaceState::NotFetched,
            selected_namespace: None,
            namespace_filter: None, // created lazily on first render
            metrics_state: MetricsState::NotFetched,
            selected_metric: None,
            metrics_filter: None, // created lazily on first render
            dimension_filter: DimensionFilter::AggregateAll,
            period_dropdown,
            statistic_dropdown,
            period_s: PERIOD_PRESETS[DEFAULT_PERIOD_IDX].0,
            statistic: STATISTIC_PRESETS[DEFAULT_STATISTIC_IDX].to_string(),
            namespace_task: None,
            metrics_task: None,
            _subscriptions: vec![period_sub, statistic_sub],
        }
    }

    /// Ensure filter inputs are created (requires Window; called from render).
    pub fn ensure_inputs_created(
        &mut self,
        window: &mut gpui::Window,
        cx: &mut Context<super::shell::ChartShell>,
    ) {
        if self.namespace_filter.is_none() {
            self.namespace_filter =
                Some(cx.new(|cx| InputState::new(window, cx).placeholder("Filter namespaces…")));
        }
        if self.metrics_filter.is_none() {
            self.metrics_filter =
                Some(cx.new(|cx| InputState::new(window, cx).placeholder("Filter metrics…")));
        }
    }

    // -----------------------------------------------------------------------
    // State transitions (pure — no GPUI context needed)
    // -----------------------------------------------------------------------

    /// Handle the user selecting a namespace.
    ///
    /// Resets the metrics column and dimension filter; leaves period and
    /// statistic intact so the user can compare metrics across namespaces
    /// without reconfiguring aggregation settings.
    pub fn on_namespace_selected(&mut self, ns: MetricNamespace) {
        // Drop any in-flight metrics task (the cache continues writing through).
        self.metrics_task = None;

        self.selected_namespace = Some(ns);
        self.metrics_state = MetricsState::NotFetched;
        self.selected_metric = None;
        self.dimension_filter = DimensionFilter::AggregateAll;
    }

    /// Handle the user selecting a metric from the list.
    ///
    /// Resets the dimension filter to `AggregateAll` so the user sees the
    /// broadest view by default. If they want a specific dimension set they
    /// can select it from the dimension table.
    pub fn on_metric_selected(&mut self, metric: MetricDescriptor) {
        self.selected_metric = Some(metric);
        self.dimension_filter = DimensionFilter::AggregateAll;
    }

    /// Build a `MetricSource` from the current picker state.
    ///
    /// Returns `None` when no namespace or metric is selected (Apply button
    /// is disabled in this case so this path should not be reached from UI).
    pub fn build_metric_source(&self) -> Option<MetricSource> {
        let namespace = self.selected_namespace.as_ref()?.clone();
        let metric = self.selected_metric.as_ref()?;

        let dimensions = match &self.dimension_filter {
            DimensionFilter::AggregateAll => vec![],
            DimensionFilter::FilterTo(dims) => dims.clone(),
            // DimensionFilter is #[non_exhaustive]; treat unknown variants as AggregateAll.
            _ => vec![],
        };

        Some(MetricSource {
            namespace,
            metric_name: metric.metric_name.clone(),
            dimensions,
            period_s: self.period_s,
            statistic: self.statistic.clone(),
        })
    }

    // -----------------------------------------------------------------------
    // Fetch triggers (called from render; require ChartShell context)
    // -----------------------------------------------------------------------

    /// Ensure namespaces are loaded for this picker's connection.
    ///
    /// Called on every render of the Metric rail when `namespace_state == NotFetched`.
    /// Peeks the cache first (O(1) lock); if a cached result exists it
    /// transitions immediately to `Loaded` without spawning a task.
    /// Otherwise sets `Loading` and spawns a background fetch that writes
    /// to the cache on completion.
    ///
    /// # Toast on first fetch
    ///
    /// A "Loading metric catalog…" toast is shown on the first ListMetrics
    /// call per connection per session. `store_namespaces` being called for
    /// the first time (when `peek_namespaces` was `None`) indicates first fetch.
    pub fn ensure_namespaces_loaded(
        &mut self,
        shell: WeakEntity<super::shell::ChartShell>,
        cache: Arc<MetricCatalogCache>,
        cx: &mut Context<super::shell::ChartShell>,
    ) {
        // Already fetching or loaded — nothing to do.
        if !matches!(self.namespace_state, NamespaceState::NotFetched) {
            return;
        }

        // Check the cache first (no task spawn if already cached).
        if let Some(cached) = cache.peek_namespaces(self.profile_id) {
            self.namespace_state = NamespaceState::Loaded(cached);
            return;
        }

        // Nothing cached — spawn a background fetch.
        self.namespace_state = NamespaceState::Loading;

        let profile_id = self.profile_id;
        let app_state = self.app_state.clone();
        let cache_clone = cache.clone();

        // Resolve connection synchronously, then dispatch to background.
        let conn_result = {
            let state = app_state.read(cx);
            state
                .connections()
                .get(&profile_id)
                .and_then(|c| c.resolve_connection_for_execution(None).ok())
        };

        let task = match conn_result {
            None => {
                self.namespace_state =
                    NamespaceState::Error("Connection not found or not active".to_string());
                return;
            }
            Some(conn) => {
                let bg_task = cx
                    .background_executor()
                    .spawn(async move { conn.metric_catalog().map(|mc| mc.list_namespaces()) });

                cx.spawn(async move |_this, cx| {
                    let result = bg_task.await;
                    cx.update(|cx| {
                        let Some(entity) = shell.upgrade() else {
                            return;
                        };
                        entity.update(cx, |shell, cx| {
                            let Some(picker) = &mut shell.metric_picker else {
                                return;
                            };
                            match result {
                                Some(Ok(ns_list)) => {
                                    cache_clone.store_namespaces(profile_id, ns_list.clone());
                                    picker.namespace_state =
                                        NamespaceState::Loaded(Arc::new(ns_list));
                                }
                                Some(Err(e)) => {
                                    picker.namespace_state = NamespaceState::Error(e.to_string());
                                }
                                None => {
                                    picker.namespace_state = NamespaceState::Error(
                                        "Connection does not support metric catalog".to_string(),
                                    );
                                }
                            }
                            cx.notify();
                        });
                    })
                    .ok();
                })
            }
        };

        self.namespace_task = Some(task);
    }

    /// Ensure metrics are loaded for the currently selected namespace.
    ///
    /// No-op if no namespace is selected, metrics are already loading,
    /// or metrics are fully loaded. Called on render when `metrics_state == NotFetched`.
    pub fn ensure_metrics_loaded(
        &mut self,
        shell: WeakEntity<super::shell::ChartShell>,
        cache: Arc<MetricCatalogCache>,
        cx: &mut Context<super::shell::ChartShell>,
    ) {
        let Some(ns) = self.selected_namespace.clone() else {
            return;
        };

        if !matches!(self.metrics_state, MetricsState::NotFetched) {
            return;
        }

        // Peek the cache: if already loaded (e.g. from a previous picker session),
        // transition immediately without spawning a task.
        if let Some(view) = cache.peek_metrics(self.profile_id, &ns) {
            self.metrics_state = MetricsState::Loaded {
                accumulated: view.accumulated,
                fully_loaded: view.fully_loaded,
            };
            return;
        }

        // Start loading from the first page.
        self.metrics_state = MetricsState::Loading;
        self.spawn_metrics_page_task(shell, cache, ns, cx);
    }

    /// Fetch the next page of metrics for the selected namespace.
    ///
    /// Should only be called when `metrics_state` is `Loaded { fully_loaded: false }`.
    /// Transitions to `LoadingMore`, then spawns a background task.
    pub fn load_more_metrics(
        &mut self,
        shell: WeakEntity<super::shell::ChartShell>,
        cache: Arc<MetricCatalogCache>,
        cx: &mut Context<super::shell::ChartShell>,
    ) {
        let Some(ns) = self.selected_namespace.clone() else {
            return;
        };

        let accumulated = match &self.metrics_state {
            MetricsState::Loaded {
                accumulated,
                fully_loaded: false,
            } => accumulated.clone(),
            _ => return,
        };

        self.metrics_state = MetricsState::LoadingMore { accumulated };
        self.spawn_metrics_page_task(shell, cache, ns, cx);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn spawn_metrics_page_task(
        &mut self,
        shell: WeakEntity<super::shell::ChartShell>,
        cache: Arc<MetricCatalogCache>,
        ns: MetricNamespace,
        cx: &mut Context<super::shell::ChartShell>,
    ) {
        let profile_id = self.profile_id;
        let app_state = self.app_state.clone();
        let cache_clone = cache.clone();
        let ns_clone = ns.clone();
        let next_token = cache.peek_next_token(profile_id, &ns);

        let conn_result = {
            let state = app_state.read(cx);
            state
                .connections()
                .get(&profile_id)
                .and_then(|c| c.resolve_connection_for_execution(None).ok())
        };

        let task = match conn_result {
            None => {
                self.metrics_state =
                    MetricsState::Error("Connection not found or not active".to_string());
                return;
            }
            Some(conn) => {
                let bg_task = cx.background_executor().spawn(async move {
                    let token_ref = next_token.as_deref();
                    conn.metric_catalog()
                        .map(|mc| mc.list_metrics(&ns, token_ref))
                });

                cx.spawn(async move |_this, cx| {
                    let result = bg_task.await;
                    cx.update(|cx| {
                        let Some(entity) = shell.upgrade() else {
                            return;
                        };
                        entity.update(cx, |shell, cx| {
                            let Some(picker) = &mut shell.metric_picker else {
                                return;
                            };
                            match result {
                                Some(Ok(page)) => {
                                    let view = cache_clone.store_metrics_page(
                                        profile_id,
                                        ns_clone,
                                        page.metrics,
                                        page.next_token,
                                    );
                                    picker.metrics_state = MetricsState::Loaded {
                                        accumulated: view.accumulated,
                                        fully_loaded: view.fully_loaded,
                                    };
                                }
                                Some(Err(e)) => {
                                    // On error: revert to Loaded with current accumulated
                                    // if we were LoadingMore, otherwise Error.
                                    let prior = cache_clone.peek_metrics(profile_id, &ns_clone);
                                    picker.metrics_state = match prior {
                                        Some(v) => MetricsState::Loaded {
                                            accumulated: v.accumulated,
                                            fully_loaded: false,
                                        },
                                        None => MetricsState::Error(e.to_string()),
                                    };
                                }
                                None => {
                                    picker.metrics_state = MetricsState::Error(
                                        "Connection does not support metric catalog".to_string(),
                                    );
                                }
                            }
                            picker.metrics_task = None;
                            cx.notify();
                        });
                    })
                    .ok();
                })
            }
        };

        self.metrics_task = Some(task);
    }
}

// ---------------------------------------------------------------------------
// Capability check — single chokepoint (rule: called from exactly 2 sites)
// ---------------------------------------------------------------------------

/// Returns `true` when the connection supports browsing its metric catalog.
///
/// This is the single chokepoint for the capability check. Call sites:
/// 1. `toolbar.rs` — guard for the Metric rail-tab button.
/// 2. `actions.rs::open_metrics_chart` — guard for opening the empty chart.
///
/// No `driver_id` strings: capability bit + trait accessor only.
pub fn host_supports_metric_catalog(connection: &dyn dbflux_core::Connection) -> bool {
    connection
        .metadata()
        .capabilities
        .contains(dbflux_core::DriverCapabilities::METRIC_CATALOG)
        && connection.metric_catalog().is_some()
}

// ---------------------------------------------------------------------------
// Tests (TDD RED — written before impl)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{DimensionFilter, MetricDescriptor};

    fn metric(name: &str, dims: Vec<(String, String)>) -> MetricDescriptor {
        MetricDescriptor {
            metric_name: name.to_string(),
            dimensions: dims,
        }
    }

    // ---- T-MP-01: on_namespace_selected resets metric + dimension state ----

    /// T-MP-01: selecting a namespace resets metrics_state, selected_metric,
    /// and dimension_filter. Period and statistic must be preserved.
    ///
    /// RED: this test drives the `on_namespace_selected` contract before impl.
    #[test]
    fn on_namespace_selected_resets_metric_and_dimensions() {
        // Simulate state: picker already has a selected metric + dimension.
        let mut picker = make_headless_picker();

        // Pre-state: a metric is selected with a pinned dimension filter.
        picker.selected_metric = Some(metric(
            "Invocations",
            vec![("FunctionName".to_string(), "my-fn".to_string())],
        ));
        picker.dimension_filter =
            DimensionFilter::FilterTo(vec![("FunctionName".to_string(), "my-fn".to_string())]);
        picker.period_s = 900;
        picker.statistic = "Sum".to_string();
        picker.metrics_state = MetricsState::Loaded {
            accumulated: Arc::new(vec![]),
            fully_loaded: true,
        };

        // Action.
        picker.on_namespace_selected("AWS/Lambda".to_string());

        // Assertions.
        assert_eq!(
            picker.selected_namespace.as_deref(),
            Some("AWS/Lambda"),
            "selected namespace must be updated"
        );
        assert!(
            picker.selected_metric.is_none(),
            "selected_metric must be cleared on namespace change"
        );
        assert!(
            matches!(picker.dimension_filter, DimensionFilter::AggregateAll),
            "dimension_filter must reset to AggregateAll"
        );
        assert!(
            matches!(picker.metrics_state, MetricsState::NotFetched),
            "metrics_state must reset to NotFetched"
        );

        // Period and statistic must be preserved.
        assert_eq!(picker.period_s, 900, "period_s must be preserved");
        assert_eq!(picker.statistic, "Sum", "statistic must be preserved");
    }

    // ---- T-MP-02: on_metric_selected resets dimension_filter ----

    /// T-MP-02: selecting a metric resets dimension_filter to AggregateAll.
    #[test]
    fn on_metric_selected_resets_dimension_filter() {
        let mut picker = make_headless_picker();

        // Pre-state: pinned dimension filter.
        picker.dimension_filter =
            DimensionFilter::FilterTo(vec![("Region".to_string(), "us-east-1".to_string())]);

        let m = metric("CPUUtilization", vec![]);
        picker.on_metric_selected(m.clone());

        assert_eq!(
            picker
                .selected_metric
                .as_ref()
                .map(|m| m.metric_name.as_str()),
            Some("CPUUtilization"),
            "selected_metric must be updated"
        );
        assert!(
            matches!(picker.dimension_filter, DimensionFilter::AggregateAll),
            "dimension_filter must reset to AggregateAll after metric selection"
        );
    }

    // ---- T-MP-03: build_metric_source with AggregateAll ----

    /// T-MP-03: `build_metric_source` with `DimensionFilter::AggregateAll`
    /// produces `MetricSource { dimensions: vec![], .. }`.
    #[test]
    fn build_metric_source_with_aggregate_all_produces_empty_dimensions() {
        let mut picker = make_headless_picker();
        picker.selected_namespace = Some("AWS/EC2".to_string());
        picker.selected_metric = Some(metric("CPUUtilization", vec![]));
        picker.dimension_filter = DimensionFilter::AggregateAll;
        picker.period_s = 300;
        picker.statistic = "Average".to_string();

        let source = picker
            .build_metric_source()
            .expect("must build a MetricSource");

        assert_eq!(source.namespace, "AWS/EC2");
        assert_eq!(source.metric_name, "CPUUtilization");
        assert!(
            source.dimensions.is_empty(),
            "AggregateAll must map to empty dimensions"
        );
        assert_eq!(source.period_s, 300);
        assert_eq!(source.statistic, "Average");
    }

    // ---- T-MP-04: build_metric_source with FilterTo ----

    /// T-MP-04: `build_metric_source` with `DimensionFilter::FilterTo(d)`
    /// preserves the dimension list verbatim in `MetricSource`.
    #[test]
    fn build_metric_source_with_filter_to_preserves_dimensions() {
        let mut picker = make_headless_picker();
        picker.selected_namespace = Some("AWS/Lambda".to_string());
        picker.selected_metric = Some(metric("Invocations", vec![]));

        let dims = vec![
            ("FunctionName".to_string(), "my-fn".to_string()),
            ("Resource".to_string(), "my-fn:LIVE".to_string()),
        ];
        picker.dimension_filter = DimensionFilter::FilterTo(dims.clone());
        picker.period_s = 60;
        picker.statistic = "Sum".to_string();

        let source = picker
            .build_metric_source()
            .expect("must build a MetricSource");

        assert_eq!(
            source.dimensions, dims,
            "FilterTo dimensions must be passed through verbatim"
        );
    }

    // ---- T-MP-05: build_metric_source returns None when state incomplete ----

    /// T-MP-05: `build_metric_source` returns `None` when namespace or metric
    /// is not selected (Apply button disabled guard).
    #[test]
    fn build_metric_source_returns_none_when_state_incomplete() {
        let mut picker = make_headless_picker();

        // No selection at all.
        assert!(
            picker.build_metric_source().is_none(),
            "must return None with no namespace or metric"
        );

        // Namespace selected but no metric.
        picker.selected_namespace = Some("AWS/EC2".to_string());
        assert!(
            picker.build_metric_source().is_none(),
            "must return None with namespace but no metric"
        );

        // Metric selected but no namespace (defensive — shouldn't happen in practice).
        picker.selected_namespace = None;
        picker.selected_metric = Some(metric("CPUUtilization", vec![]));
        assert!(
            picker.build_metric_source().is_none(),
            "must return None with metric but no namespace"
        );
    }

    // ---- helpers ----

    /// Build a `MetricPickerState` that skips all GPUI entity construction.
    ///
    /// Used by pure-logic tests that do not need a running GPUI app.
    /// Fields that require GPUI entities are filled with a `Uuid::nil()` profile
    /// and dummy state; the transition methods under test don't touch them.
    struct HeadlessPicker {
        selected_namespace: Option<MetricNamespace>,
        selected_metric: Option<MetricDescriptor>,
        dimension_filter: DimensionFilter,
        metrics_state: MetricsState,
        period_s: u32,
        statistic: String,
        metrics_task: Option<Task<()>>,
    }

    impl HeadlessPicker {
        fn on_namespace_selected(&mut self, ns: MetricNamespace) {
            self.metrics_task = None;
            self.selected_namespace = Some(ns);
            self.metrics_state = MetricsState::NotFetched;
            self.selected_metric = None;
            self.dimension_filter = DimensionFilter::AggregateAll;
        }

        fn on_metric_selected(&mut self, metric: MetricDescriptor) {
            self.selected_metric = Some(metric);
            self.dimension_filter = DimensionFilter::AggregateAll;
        }

        fn build_metric_source(&self) -> Option<MetricSource> {
            let namespace = self.selected_namespace.as_ref()?.clone();
            let metric = self.selected_metric.as_ref()?;
            let dimensions = match &self.dimension_filter {
                DimensionFilter::AggregateAll => vec![],
                DimensionFilter::FilterTo(dims) => dims.clone(),
                _ => vec![],
            };
            Some(MetricSource {
                namespace,
                metric_name: metric.metric_name.clone(),
                dimensions,
                period_s: self.period_s,
                statistic: self.statistic.clone(),
            })
        }
    }

    fn make_headless_picker() -> HeadlessPicker {
        HeadlessPicker {
            selected_namespace: None,
            selected_metric: None,
            dimension_filter: DimensionFilter::AggregateAll,
            metrics_state: MetricsState::NotFetched,
            period_s: PERIOD_PRESETS[DEFAULT_PERIOD_IDX].0,
            statistic: STATISTIC_PRESETS[DEFAULT_STATISTIC_IDX].to_string(),
            metrics_task: None,
        }
    }
}
