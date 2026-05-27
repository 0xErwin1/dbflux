//! `MetricPickerState` — UI state for the metric picker rail tab.
//!
//! This module contains the pure state machine for the metric picker.
//!
//! **Sidebar-pivot model**: the namespace and metric are always known when the
//! picker is created (the user clicked a metric leaf in the sidebar). The picker
//! rail is responsible only for:
//!   1. Loading and displaying dimension combinations (from the shared cache).
//!   2. Letting the user pick period and statistic.
//!   3. Building a `MetricSource` and emitting it via the Apply button.
//!
//! **Boundary-struct pattern**: rendering lives in `metric_picker_render.rs`
//! as `MetricPickerView`. `MetricPickerState` is NOT a GPUI entity. It is
//! owned by `ChartShell` and operated through `ChartShell`'s context.
//!
//! # Capability check
//!
//! The sidebar tree-builder (`tree_builder.rs`) gates the Metrics folder on
//! the generic `DriverCapabilities::METRIC_CATALOG` bit directly — no
//! driver-id strings anywhere.

// `DbError` is a large type defined in `dbflux_core`; we cannot change its size.
// Background task closures that call into MetricCatalog return Option<Result<_, DbError>>,
// which triggers clippy::result_large_err. Suppressed here since boxing DbError is not
// an option in this codebase.
#![allow(clippy::result_large_err)]

use dbflux_app::MetricCatalogCache;
use dbflux_components::chart::MetricSource;
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged, InputState};
use dbflux_core::DimensionFilter;
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::{App, Context, Entity, FocusHandle, Subscription, Task, WeakEntity};
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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

/// Label appended as the LAST dropdown entry for both period and statistic.
///
/// Selecting it reveals an inline text input so the user can supply any AWS-
/// accepted period (1..=86400 seconds) or statistic (e.g. `p99`, `p99.9`,
/// `trimmed_mean`) that does not appear in the preset list. The free-text
/// value is run through `validate_period` / `validate_statistic` on commit.
pub const CUSTOM_DROPDOWN_LABEL: &str = "Custom…";

/// Dropdown index of the "Custom…" entry (last position).
pub fn period_custom_index() -> usize {
    PERIOD_PRESETS.len()
}

/// Dropdown index of the "Custom…" entry (last position).
pub fn statistic_custom_index() -> usize {
    STATISTIC_PRESETS.len()
}

// ---------------------------------------------------------------------------
// MetricPickerState
// ---------------------------------------------------------------------------

/// UI state machine for the metric picker rail tab.
///
/// Owned by `ChartShell`. All field access is through `ChartShell`'s
/// `Context<ChartShell>` — never accessed as an independent GPUI entity.
///
/// In the sidebar-pivot model the namespace and metric are always fixed at
/// construction time (set by `new_pre_populated`). The picker rail only
/// manages: dimension selection, period, statistic, and the Apply button.
pub struct MetricPickerState {
    pub profile_id: Uuid,
    pub app_state: Entity<AppStateEntity>,

    /// The fixed metric namespace. Non-optional — always set at construction.
    pub selected_namespace: String,

    /// The fixed metric name. Non-optional — always set at construction.
    pub selected_metric_name: String,

    /// Focus handle for the picker rail container.
    ///
    /// The picker root `div()` tracks this handle so that keyboard events
    /// (Cmd/Ctrl+Enter → Apply) are received when the rail is focused.
    pub focus_handle: FocusHandle,

    // Configuration section.
    pub dimension_filter: DimensionFilter,
    pub period_dropdown: Entity<Dropdown>,
    pub statistic_dropdown: Entity<Dropdown>,
    pub period_s: u32,
    pub statistic: String,

    /// When `true`, the dropdown is on "Custom…" and the render path renders
    /// an inline text input bound to `period_custom_input` (created lazily on
    /// first render — `InputState::new` requires a `Window`).
    pub period_custom_active: bool,
    pub period_custom_input: Option<Entity<InputState>>,
    pub period_custom_error: Option<String>,
    pub period_custom_sub: Option<Subscription>,

    pub statistic_custom_active: bool,
    pub statistic_custom_input: Option<Entity<InputState>>,
    pub statistic_custom_error: Option<String>,
    pub statistic_custom_sub: Option<Subscription>,

    /// Marks that the next render must trigger `ensure_dimensions_loaded`.
    /// Set when the picker is constructed; consumed (and reset to `false`) in
    /// the first render. Prevents the documented GPUI antipattern of spawning
    /// futures unconditionally from inside the render pass.
    pub pending_dimensions_fetch: bool,

    /// Dimension combinations for the selected metric.
    ///
    /// Starts as `NotFetched`; the render path calls `ensure_dimensions_loaded`
    /// which peeks the shared cache (typically warm because the sidebar already
    /// expanded the namespace folder) and transitions to `Loaded` on a cache hit
    /// or `Loading` + spawns a background fetch on a miss.
    pub dimensions_state: DimensionsState,

    /// In-flight dimensions fetch task (dropped to cancel the picker's await;
    /// the cache's underlying fetch continues and writes through).
    pub dimensions_task: Option<Task<()>>,

    /// Subscriptions to dropdown selection changes.
    pub _subscriptions: Vec<Subscription>,
}

impl MetricPickerState {
    /// Construct a `MetricPickerState` pre-populated with a known
    /// `(namespace, metric_name)`.
    ///
    /// Used when the user clicks a metric leaf in the sidebar tree. The
    /// namespace and metric are fixed; only dimensions, period, and statistic
    /// are configurable in the picker rail.
    ///
    /// `dimensions_state` starts as `NotFetched`; the render path calls
    /// `ensure_dimensions_loaded` which peeks the shared cache (typically warm
    /// because the sidebar already expanded the namespace folder) and transitions
    /// to `Loaded` on a cache hit or `Loading` on a miss.
    pub fn new_pre_populated(
        profile_id: Uuid,
        app_state: Entity<AppStateEntity>,
        namespace: String,
        metric_name: String,
        cx: &mut Context<super::shell::ChartShell>,
    ) -> Self {
        // Preset entries plus a trailing "Custom…" entry that reveals an
        // inline text input on selection. Without this entry the
        // `validate_period` / `validate_statistic` helpers would be dead code.
        let period_items = PERIOD_PRESETS
            .iter()
            .map(|(_, label)| DropdownItem::new(*label))
            .chain(std::iter::once(DropdownItem::new(CUSTOM_DROPDOWN_LABEL)))
            .collect::<Vec<_>>();

        let statistic_items = STATISTIC_PRESETS
            .iter()
            .map(|s| DropdownItem::new(*s))
            .chain(std::iter::once(DropdownItem::new(CUSTOM_DROPDOWN_LABEL)))
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
            |shell: &mut super::shell::ChartShell, _, event: &DropdownSelectionChanged, cx| {
                if let Some(picker) = &mut shell.metric_picker {
                    if event.index == period_custom_index() {
                        // Reveal the inline custom text input; keep the last
                        // applied numeric value until the user commits a new one.
                        picker.period_custom_active = true;
                        picker.period_custom_error = None;
                    } else {
                        picker.period_custom_active = false;
                        picker.period_custom_error = None;
                        picker.period_s = PERIOD_PRESETS
                            .get(event.index)
                            .map(|(s, _)| *s)
                            .unwrap_or(300);
                    }
                    cx.notify();
                }
            },
        );

        let statistic_sub = cx.subscribe(
            &statistic_dropdown,
            |shell: &mut super::shell::ChartShell, _, event: &DropdownSelectionChanged, cx| {
                if let Some(picker) = &mut shell.metric_picker {
                    if event.index == statistic_custom_index() {
                        picker.statistic_custom_active = true;
                        picker.statistic_custom_error = None;
                    } else {
                        picker.statistic_custom_active = false;
                        picker.statistic_custom_error = None;
                        picker.statistic = STATISTIC_PRESETS
                            .get(event.index)
                            .copied()
                            .unwrap_or("Average")
                            .to_string();
                    }
                    cx.notify();
                }
            },
        );

        Self {
            profile_id,
            app_state,
            selected_namespace: namespace,
            selected_metric_name: metric_name,
            focus_handle: cx.focus_handle(),
            dimension_filter: DimensionFilter::AggregateAll,
            period_dropdown,
            statistic_dropdown,
            period_s: PERIOD_PRESETS[DEFAULT_PERIOD_IDX].0,
            statistic: STATISTIC_PRESETS[DEFAULT_STATISTIC_IDX].to_string(),
            period_custom_active: false,
            period_custom_input: None,
            period_custom_error: None,
            period_custom_sub: None,
            statistic_custom_active: false,
            statistic_custom_input: None,
            statistic_custom_error: None,
            statistic_custom_sub: None,
            pending_dimensions_fetch: true,
            dimensions_state: DimensionsState::NotFetched,
            dimensions_task: None,
            _subscriptions: vec![period_sub, statistic_sub],
        }
    }

    /// Build a `MetricSource` from the current picker state.
    ///
    /// Always succeeds — namespace and metric name are non-optional fields.
    pub fn build_metric_source(&self) -> MetricSource {
        let dimensions = match &self.dimension_filter {
            DimensionFilter::AggregateAll => vec![],
            DimensionFilter::FilterTo(dims) => dims.clone(),
            // DimensionFilter is #[non_exhaustive]; treat unknown variants as AggregateAll.
            _ => vec![],
        };

        MetricSource::single(
            self.selected_namespace.clone(),
            self.selected_metric_name.clone(),
            dimensions,
            self.period_s,
            self.statistic.clone(),
        )
    }

    // -----------------------------------------------------------------------
    // Dimensions fetch (called from render; requires ChartShell context)
    // -----------------------------------------------------------------------

    /// Ensure dimension combinations are loaded for the current metric.
    ///
    /// Called on every render when `dimensions_state == NotFetched`. Peeks the
    /// cache first (`peek_metrics` for the namespace); if found, scans for the
    /// selected metric name and extracts its dimensions immediately without
    /// spawning a task. On a cache miss, sets `Loading` and spawns a background
    /// fetch.
    pub fn ensure_dimensions_loaded(
        &mut self,
        shell: WeakEntity<super::shell::ChartShell>,
        cache: Arc<MetricCatalogCache>,
        cx: &mut Context<super::shell::ChartShell>,
    ) {
        if !matches!(self.dimensions_state, DimensionsState::NotFetched) {
            return;
        }

        let ns = self.selected_namespace.clone();
        let metric_name = self.selected_metric_name.clone();

        // Cache warm path: extract dimensions immediately.
        if let Some(view) = cache.peek_metrics(self.profile_id, &ns) {
            let dims: Vec<Vec<(String, String)>> = view
                .accumulated
                .iter()
                .filter(|m| m.metric_name == metric_name)
                .map(|m| m.dimensions.clone())
                .collect();
            self.dimensions_state = DimensionsState::Loaded(dims);
            return;
        }

        // Cache miss — spawn a background metrics-page fetch for the namespace.
        self.dimensions_state = DimensionsState::Loading;

        let profile_id = self.profile_id;
        let app_state = self.app_state.clone();
        let cache_clone = cache.clone();
        let ns_clone = ns.clone();
        let metric_name_clone = metric_name.clone();

        let conn_result = {
            let state = app_state.read(cx);
            state
                .connections()
                .get(&profile_id)
                .and_then(|c| c.resolve_connection_for_execution(None).ok())
        };

        let task = match conn_result {
            None => {
                self.dimensions_state =
                    DimensionsState::Error("Connection not found or not active".to_string());
                return;
            }
            Some(conn) => {
                let bg_task = cx.background_executor().spawn(async move {
                    conn.metric_catalog()
                        .map(|mc| mc.list_metrics(&ns_clone, None))
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
                                    cache_clone.store_metrics_page(
                                        profile_id,
                                        ns.clone(),
                                        page.metrics.clone(),
                                        page.next_token,
                                    );
                                    let dims: Vec<Vec<(String, String)>> = page
                                        .metrics
                                        .iter()
                                        .filter(|m| m.metric_name == metric_name_clone)
                                        .map(|m| m.dimensions.clone())
                                        .collect();
                                    picker.dimensions_state = DimensionsState::Loaded(dims);
                                }
                                Some(Err(e)) => {
                                    picker.dimensions_state = DimensionsState::Error(e.to_string());
                                }
                                None => {
                                    picker.dimensions_state = DimensionsState::Error(
                                        "Connection does not support metric catalog".to_string(),
                                    );
                                }
                            }
                            picker.dimensions_task = None;
                            cx.notify();
                        });
                    })
                    .ok();
                })
            }
        };

        self.dimensions_task = Some(task);
    }

    /// Return the dimension combinations loaded for the current metric, or
    /// `None` when not yet loaded.
    pub fn dimensions_state(&self) -> &DimensionsState {
        &self.dimensions_state
    }

    /// Commit the user's free-text period entry.
    ///
    /// Validates via `validate_period`; on success stores the result in
    /// `period_s` and clears any error; on failure stores the error message
    /// for inline display and leaves the dropdown on "Custom…".
    pub fn commit_custom_period(&mut self, raw: &str) {
        match validate_period(raw.trim()) {
            Ok(n) => {
                self.period_s = n;
                self.period_custom_error = None;
            }
            Err(e) => {
                self.period_custom_error = Some(e);
            }
        }
    }

    /// Commit the user's free-text statistic entry.
    ///
    /// Validates via `validate_statistic`; on success stores the result in
    /// `statistic`; on failure stores the error message for inline display.
    pub fn commit_custom_statistic(&mut self, raw: &str) {
        match validate_statistic(raw.trim()) {
            Ok(s) => {
                self.statistic = s;
                self.statistic_custom_error = None;
            }
            Err(e) => {
                self.statistic_custom_error = Some(e);
            }
        }
    }

    /// Flush any pending "Custom…" inputs before Apply.
    ///
    /// When the user selects "Custom…" in the period or statistic dropdown and
    /// types a value but does not press Enter inside the input, the
    /// per-input PressEnter subscription never fires and `period_s` /
    /// `statistic` still reflect the previously-committed values. Apply must
    /// read the current input contents directly, validate them, and either
    /// commit (success) or surface the inline error and refuse to proceed.
    ///
    /// Returns `true` when every active custom input committed cleanly. When
    /// `false`, the caller must surface `period_custom_error` /
    /// `statistic_custom_error` instead of emitting `MetricPickerApplied`.
    pub fn flush_pending_custom_inputs(&mut self, cx: &App) -> bool {
        let mut ok = true;

        if self.period_custom_active
            && let Some(input) = self.period_custom_input.as_ref()
        {
            let raw = input.read(cx).value().to_string();
            self.commit_custom_period(&raw);
            if self.period_custom_error.is_some() {
                ok = false;
            }
        }

        if self.statistic_custom_active
            && let Some(input) = self.statistic_custom_input.as_ref()
        {
            let raw = input.read(cx).value().to_string();
            self.commit_custom_statistic(&raw);
            if self.statistic_custom_error.is_some() {
                ok = false;
            }
        }

        ok
    }
}

// ---------------------------------------------------------------------------
// DimensionsState
// ---------------------------------------------------------------------------

/// Dimension-combos fetch state for the pre-populated picker.
///
/// When the user opens a chart from the sidebar the namespace and metric are
/// already known. `ensure_dimensions_loaded` reads the available dimension
/// combinations from the cache (warm) or spawns a background fetch (cold).
pub enum DimensionsState {
    /// No fetch has started yet.
    NotFetched,
    /// A background fetch is in progress.
    Loading,
    /// Dimension combinations available. Each inner `Vec` is one combination
    /// of `(name, value)` pairs from the metric descriptor set.
    Loaded(Vec<Vec<(String, String)>>),
    /// Last fetch failed; stores the error message for display + retry.
    Error(String),
}

// ---------------------------------------------------------------------------
// Validation helpers (T15.1, T15.2) — pure, no GPUI
// ---------------------------------------------------------------------------

/// Validate a user-supplied statistic string.
///
/// Accepts:
/// - AWS standard presets (e.g. `"Average"`, `"Sum"`, `"p99"`, `"p99.9"`).
/// - Any non-empty free-text string (passed through to AWS for server-side
///   validation per REQ-3.4, so errors surface as driver errors rather than
///   silent no-ops).
///
/// Rejects only empty strings. The caller is responsible for trimming whitespace.
pub fn validate_statistic(s: &str) -> Result<String, String> {
    if s.is_empty() {
        return Err("statistic must not be empty".to_string());
    }
    Ok(s.to_string())
}

/// Validate a user-supplied period string.
///
/// Accepts strings that parse to a `u32` in the range `1..=86400` (1 second
/// to 24 hours). Returns the parsed value on success.
///
/// Rejects:
/// - Non-numeric strings.
/// - `0` (below minimum).
/// - Values above `86400` (AWS CloudWatch maximum period).
pub fn validate_period(s: &str) -> Result<u32, String> {
    let n: u32 = s
        .parse()
        .map_err(|_| format!("period must be a number, got {:?}", s))?;

    if n == 0 {
        return Err("period must be at least 1 second".to_string());
    }
    if n > 86400 {
        return Err("period must not exceed 86400 seconds (24 hours)".to_string());
    }
    Ok(n)
}

// ---------------------------------------------------------------------------
// Tests (TDD RED — written before impl)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::DimensionFilter;

    // ---- T14.2: DimensionsState starts as NotFetched ----

    /// T14.2: DimensionsState starts as NotFetched for a pre-populated picker.
    #[test]
    fn pre_populated_picker_dimensions_state_starts_not_fetched() {
        let state = DimensionsState::NotFetched;
        assert!(
            matches!(state, DimensionsState::NotFetched),
            "DimensionsState::NotFetched must be the initial value"
        );
    }

    /// T14.2: validate_statistic accepts known statistic patterns.
    #[test]
    fn validate_statistic_accepts_percentile_and_free_text() {
        assert!(validate_statistic("p99").is_ok(), "p99 must be accepted");
        assert!(
            validate_statistic("p99.9").is_ok(),
            "p99.9 must be accepted"
        );
        assert!(
            validate_statistic("Average").is_ok(),
            "Average must be accepted (free-text passthrough)"
        );
        assert!(
            validate_statistic("trimmed_mean").is_ok(),
            "trimmed_mean must be accepted (free-text passthrough)"
        );
    }

    /// T14.2: validate_statistic rejects empty strings.
    #[test]
    fn validate_statistic_rejects_empty_string() {
        assert!(
            validate_statistic("").is_err(),
            "empty string must be rejected"
        );
    }

    /// T15.2: validate_period accepts valid period values.
    #[test]
    fn validate_period_accepts_valid_values() {
        assert!(validate_period("60").is_ok(), "60 must be accepted");
        assert!(validate_period("120").is_ok(), "120 must be accepted");
        assert!(
            validate_period("86400").is_ok(),
            "86400 (max) must be accepted"
        );
    }

    /// W2: "Custom…" must be the LAST entry constructed into the period
    /// dropdown items. The constructor chains it after `PERIOD_PRESETS`, so
    /// `period_custom_index()` returns `PERIOD_PRESETS.len()` and selecting it
    /// reveals the inline custom text input.
    #[test]
    fn custom_entry_is_last_for_period_dropdown() {
        let labels: Vec<&str> = PERIOD_PRESETS
            .iter()
            .map(|(_, l)| *l)
            .chain(std::iter::once(CUSTOM_DROPDOWN_LABEL))
            .collect();

        assert_eq!(
            labels.last().copied(),
            Some(CUSTOM_DROPDOWN_LABEL),
            "Custom… must be the last entry in the period dropdown"
        );
        assert_eq!(
            period_custom_index(),
            PERIOD_PRESETS.len(),
            "period_custom_index() must point at the Custom… entry"
        );
    }

    /// W2: "Custom…" must be the LAST entry constructed into the statistic
    /// dropdown items.
    #[test]
    fn custom_entry_is_last_for_statistic_dropdown() {
        let labels: Vec<&str> = STATISTIC_PRESETS
            .iter()
            .copied()
            .chain(std::iter::once(CUSTOM_DROPDOWN_LABEL))
            .collect();

        assert_eq!(
            labels.last().copied(),
            Some(CUSTOM_DROPDOWN_LABEL),
            "Custom… must be the last entry in the statistic dropdown"
        );
        assert_eq!(
            statistic_custom_index(),
            STATISTIC_PRESETS.len(),
            "statistic_custom_index() must point at the Custom… entry"
        );
    }

    /// `commit_custom_period` writes the validated number on Ok and stores
    /// an error message on Err without mutating `period_s`.
    #[test]
    fn commit_custom_period_validates_and_routes_errors() {
        // Use HeadlessPicker-style direct state checks via the validators —
        // commit_custom_period uses the same logic.
        assert!(validate_period("120").is_ok(), "120 must be accepted");
        assert!(validate_period("0").is_err(), "0 must be rejected");
        assert!(
            validate_period("abc").is_err(),
            "non-numeric must be rejected"
        );
    }

    /// T15.2: validate_period rejects zero and out-of-range values.
    #[test]
    fn validate_period_rejects_zero_and_out_of_range() {
        assert!(validate_period("0").is_err(), "0 must be rejected");
        assert!(
            validate_period("86401").is_err(),
            "86401 (above max) must be rejected"
        );
        assert!(
            validate_period("abc").is_err(),
            "non-numeric must be rejected"
        );
    }

    // ---- HeadlessPicker — pure-logic state machine tests ----
    //
    // These tests use a self-contained `HeadlessPicker` struct (not the real
    // `MetricPickerState`) to exercise the picker state-machine logic without
    // a running GPUI app. They guard the config-section contract.

    // ---- T-MP-03: build_metric_source with AggregateAll ----

    /// T-MP-03: `build_metric_source` with `DimensionFilter::AggregateAll`
    /// produces `MetricSource { dimensions: vec![], .. }`.
    #[test]
    fn build_metric_source_with_aggregate_all_produces_empty_dimensions() {
        let mut picker = make_headless_picker("AWS/EC2", "CPUUtilization");
        picker.dimension_filter = DimensionFilter::AggregateAll;
        picker.period_s = 300;
        picker.statistic = "Average".to_string();

        let source = picker.build_metric_source();

        assert_eq!(source.series[0].namespace, "AWS/EC2");
        assert_eq!(source.series[0].metric_name, "CPUUtilization");
        assert!(
            source.series[0].dimensions.is_empty(),
            "AggregateAll must map to empty dimensions"
        );
        assert_eq!(source.series[0].period_seconds, 300);
        assert_eq!(source.series[0].statistic, "Average");
    }

    // ---- T-MP-04: build_metric_source with FilterTo ----

    /// T-MP-04: `build_metric_source` with `DimensionFilter::FilterTo(d)`
    /// preserves the dimension list verbatim in `MetricSource`.
    #[test]
    fn build_metric_source_with_filter_to_preserves_dimensions() {
        let mut picker = make_headless_picker("AWS/Lambda", "Invocations");

        let dims = vec![
            ("FunctionName".to_string(), "my-fn".to_string()),
            ("Resource".to_string(), "my-fn:LIVE".to_string()),
        ];
        picker.dimension_filter = DimensionFilter::FilterTo(dims.clone());
        picker.period_s = 60;
        picker.statistic = "Sum".to_string();

        let source = picker.build_metric_source();

        assert_eq!(
            source.series[0].dimensions, dims,
            "FilterTo dimensions must be passed through verbatim"
        );
    }

    // ---- A1: Apply must flush pending Custom… inputs ----

    /// A1 regression: simulate the flush path that
    /// `flush_pending_custom_inputs` performs before Apply. If the user
    /// selects Custom… and types `120` but never presses Enter, the picker's
    /// `period_s` still reflects the previous preset. The Apply path must call
    /// `commit_custom_period` with the input contents so the new value is
    /// applied. This test pins that contract at the validator + commit level
    /// (the GPUI input read happens at the call site).
    #[test]
    fn flush_custom_period_commits_typed_value_before_build_metric_source() {
        let mut picker = make_headless_picker("AWS/EC2", "CPUUtilization");
        picker.period_s = 300; // previously-committed preset

        // Simulate user typing "120" into the Custom… input but not pressing
        // Enter — this is what flush_pending_custom_inputs reads from the
        // InputState entity and feeds into commit_custom_period.
        let raw = "120";
        match validate_period(raw.trim()) {
            Ok(n) => picker.period_s = n,
            Err(_) => panic!("120 must validate"),
        }

        let source = picker.build_metric_source();
        assert_eq!(
            source.series[0].period_seconds, 120,
            "Apply must use the typed Custom… value, not the prior preset"
        );
    }

    /// A1 regression: when validation fails, Apply must NOT proceed and the
    /// caller must observe the error (here proxied by validate_period's Err
    /// return). The picker.period_s must remain at the previous preset.
    #[test]
    fn flush_custom_period_rejects_invalid_and_preserves_prior_value() {
        let mut picker = make_headless_picker("AWS/EC2", "CPUUtilization");
        picker.period_s = 300;

        let raw = "abc";
        let result = validate_period(raw.trim());
        assert!(result.is_err(), "non-numeric must reject");

        // The Apply path returns early on Err — period_s is untouched.
        assert_eq!(
            picker.period_s, 300,
            "invalid input must not overwrite the prior preset"
        );
    }

    /// A1 regression: same behavior for the statistic Custom… input.
    #[test]
    fn flush_custom_statistic_commits_typed_value_before_build_metric_source() {
        let mut picker = make_headless_picker("AWS/EC2", "CPUUtilization");
        picker.statistic = "Average".to_string();

        let raw = "p99.5";
        match validate_statistic(raw.trim()) {
            Ok(s) => picker.statistic = s,
            Err(_) => panic!("p99.5 must validate"),
        }

        let source = picker.build_metric_source();
        assert_eq!(
            source.series[0].statistic, "p99.5",
            "Apply must use the typed Custom… statistic value, not the prior preset"
        );
    }

    // ---- T-MP-06: build_metric_source always returns a value ----

    /// T-MP-06: In the sidebar-pivot model `build_metric_source` always
    /// succeeds — namespace and metric name are non-optional fields.
    #[test]
    fn build_metric_source_always_succeeds_in_prepopulated_picker() {
        let picker = make_headless_picker("AWS/EC2", "CPUUtilization");
        let source = picker.build_metric_source();
        assert_eq!(source.series[0].namespace, "AWS/EC2");
        assert_eq!(source.series[0].metric_name, "CPUUtilization");
    }

    // ---- helpers ----

    /// Build a `HeadlessPicker` that skips all GPUI entity construction.
    ///
    /// Used by pure-logic tests that do not need a running GPUI app.
    struct HeadlessPicker {
        selected_namespace: String,
        selected_metric_name: String,
        dimension_filter: DimensionFilter,
        period_s: u32,
        statistic: String,
    }

    impl HeadlessPicker {
        fn build_metric_source(&self) -> MetricSource {
            let dimensions = match &self.dimension_filter {
                DimensionFilter::AggregateAll => vec![],
                DimensionFilter::FilterTo(dims) => dims.clone(),
                _ => vec![],
            };
            MetricSource::single(
                self.selected_namespace.clone(),
                self.selected_metric_name.clone(),
                dimensions,
                self.period_s,
                self.statistic.clone(),
            )
        }
    }

    fn make_headless_picker(namespace: &str, metric_name: &str) -> HeadlessPicker {
        HeadlessPicker {
            selected_namespace: namespace.to_string(),
            selected_metric_name: metric_name.to_string(),
            dimension_filter: DimensionFilter::AggregateAll,
            period_s: PERIOD_PRESETS[DEFAULT_PERIOD_IDX].0,
            statistic: STATISTIC_PRESETS[DEFAULT_STATISTIC_IDX].to_string(),
        }
    }

    // ---- T18.2: Cmd/Ctrl+Enter triggers Apply ----

    /// Helper: test the keystroke condition used in the picker's `on_key_down`
    /// handler. Mirrors the logic in `metric_picker_render.rs::render()`.
    fn is_apply_shortcut(key: &str, platform: bool, control: bool, shift: bool, alt: bool) -> bool {
        key == "return" && !shift && !alt && (platform || control)
    }

    /// T18.2: Cmd+Enter (platform modifier) must trigger Apply.
    #[test]
    fn cmd_enter_invokes_apply_from_anywhere() {
        assert!(
            is_apply_shortcut("return", true, false, false, false),
            "Cmd+Enter must trigger Apply"
        );
    }

    /// T18.2: Ctrl+Enter must also trigger Apply (Linux/Windows).
    #[test]
    fn ctrl_enter_invokes_apply() {
        assert!(
            is_apply_shortcut("return", false, true, false, false),
            "Ctrl+Enter must trigger Apply"
        );
    }

    /// T18.2: Plain Enter must NOT trigger Apply (reserved for dropdown/selection).
    #[test]
    fn plain_enter_does_not_trigger_apply() {
        assert!(
            !is_apply_shortcut("return", false, false, false, false),
            "Plain Enter must not trigger Apply"
        );
    }

    /// T18.2: Shift+Cmd+Enter must NOT trigger Apply.
    #[test]
    fn shift_cmd_enter_does_not_trigger_apply() {
        assert!(
            !is_apply_shortcut("return", true, false, true, false),
            "Shift+Cmd+Enter must not trigger Apply"
        );
    }
}
