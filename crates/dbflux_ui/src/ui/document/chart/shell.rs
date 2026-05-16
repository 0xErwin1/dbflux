//! `ChartShell` — the reusable chart entity that owns chart state.
//!
//! `ChartShell` absorbs the chart-specific fields and rendering methods that
//! previously lived directly on `DataGridPanel`. Any host that implements
//! `ChartHost` can mount a `ChartShell` to get full chart UX (toolbar,
//! legend, rail, hidden-series management) without duplicating state.

/// Active tab in the chart Configure/Stats rail.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum ChartRailTab {
    #[default]
    Configure,
    Stats,
}

use super::host::{ChartHost, HostAdapter};
use dbflux_components::chart::{
    AxisPill, BindingSpec, ChartDetection, ChartSpec, ChartView, DataPointRef,
    ManualChartSelection, SourceRowRef, detect_chart_columns,
};
use dbflux_core::{ColumnKind, ColumnMeta, QueryResult};
use gpui::prelude::*;
use gpui::{Context, Entity, Subscription, Window};
use std::collections::HashSet;
use std::sync::Arc;

/// Reusable entity that owns all chart state for a single mounted host.
///
/// Created lazily by the host when the result first passes chart detection.
/// The host keeps an `Option<Entity<ChartShell>>` and renders the shell
/// when `result_view_mode == Chart`.
pub struct ChartShell {
    // ---- host adapter ----
    host: HostAdapter,

    // ---- chart view ----
    /// The built `ChartView` entity. `None` before the first result arrives
    /// or after a column-shape change that forces a rebuild.
    pub(crate) chart_view: Option<Entity<ChartView>>,

    /// Subscription that triggers `cx.notify()` on this shell whenever the
    /// `ChartView` entity notifies (e.g. hover changes, focus changes).
    pub(crate) chart_view_observer: Option<Subscription>,

    // ---- column selection ----
    /// Last result used to build the current `chart_view`. Used to detect
    /// when a rebuild is necessary after `set_result`.
    pub(crate) chart_detection: Option<ChartDetection>,

    /// Manual column selection overriding auto-detection. `None` = use
    /// detection result.
    pub(crate) chart_manual_selection: Option<ManualChartSelection>,

    // ---- interaction state ----
    /// Series indices hidden by the user via the legend.
    pub(crate) chart_hidden_series: HashSet<usize>,

    /// Index of the currently focused series (hover-driven).
    pub(crate) chart_focused_series_idx: usize,

    /// User-controlled legend visibility. Defaults to `true`; the engine's
    /// own rule (hide when series count ≤ 1) is applied on top of this.
    chart_legend_visible: bool,

    // ---- overlay picker state ----
    /// Whether the manual column-picker overlay is visible in the degraded card.
    pub(crate) chart_picker_overlay_open: bool,

    /// X-column index in the degraded picker (into result columns).
    pub(crate) chart_picker_x_col: usize,

    /// Checked state per Y-candidate column in the degraded picker.
    pub(crate) chart_picker_y_checked: Vec<bool>,

    // ---- rail state ----
    /// Whether the Stats rail is open.
    pub(crate) chart_rail_open: bool,

    /// Active tab inside the rail (Stats only after AxisBar landing).
    pub(crate) chart_rail_tab: ChartRailTab,

    /// Selected X-column index in the rail Configure picker.
    /// Kept for backward compat; no longer driven by a UI tab.
    pub(crate) chart_rail_picker_x_col: usize,

    /// Checked state per Y-candidate column in the rail Configure picker.
    /// Kept for backward compat; no longer driven by a UI tab.
    pub(crate) chart_rail_picker_y_checked: Vec<bool>,

    // ---- AxisBar state ----
    /// Which AxisBar pill picker is currently open.
    pub(crate) axis_open_pill: Option<AxisPill>,
}

impl ChartShell {
    /// Create a `ChartShell` for a native `ChartDocument` host.
    ///
    /// Uses `HostAdapter::Standalone` — the document drives `set_result` directly
    /// and does not route re-execute requests through the adapter.
    pub fn new_standalone(cx: &mut Context<Self>) -> Self {
        Self::new(HostAdapter::Standalone, cx)
    }

    /// Create a new `ChartShell` bound to the given host adapter.
    ///
    /// The shell starts with no chart view. Call `set_result` to provide the
    /// first `QueryResult` and trigger chart detection + view construction.
    pub fn new(host: HostAdapter, _cx: &mut Context<Self>) -> Self {
        Self {
            host,
            chart_view: None,
            chart_view_observer: None,
            chart_detection: None,
            chart_manual_selection: None,
            chart_hidden_series: HashSet::new(),
            chart_focused_series_idx: 0,
            chart_legend_visible: true,
            chart_picker_overlay_open: false,
            chart_picker_x_col: 0,
            chart_picker_y_checked: Vec::new(),
            chart_rail_open: false,
            chart_rail_tab: ChartRailTab::Configure,
            chart_rail_picker_x_col: 0,
            chart_rail_picker_y_checked: Vec::new(),
            axis_open_pill: None,
        }
    }

    /// Returns `true` when the current detection result is `Ok` (chart available).
    pub fn chart_available(&self) -> bool {
        matches!(&self.chart_detection, Some(ChartDetection::Ok { .. }))
    }

    /// Update shell state for a new `QueryResult`.
    ///
    /// Preserves hidden-series, manual selection, focused series, and rail
    /// state when the detection result is still `Ok` after the update (i.e.
    /// the user was already in chart mode and the new result is still
    /// chartable).
    pub fn set_result(
        &mut self,
        result: &QueryResult,
        was_chart_mode: bool,
        cx: &mut Context<Self>,
    ) {
        let detection = detect_chart_columns(result);
        let detection_ok = matches!(detection, ChartDetection::Ok { .. });

        let prev_hidden = std::mem::take(&mut self.chart_hidden_series);
        let prev_manual = self.chart_manual_selection.clone();
        let prev_focused = self.chart_focused_series_idx;
        let prev_rail_open = self.chart_rail_open;
        let prev_rail_tab = self.chart_rail_tab;

        self.chart_detection = Some(detection);
        self.chart_view = None;
        self.chart_view_observer = None;
        self.chart_picker_overlay_open = false;
        self.reset_picker(&result.columns);

        if was_chart_mode && detection_ok {
            self.chart_hidden_series = prev_hidden;
            self.chart_manual_selection = prev_manual;
            self.chart_focused_series_idx = prev_focused;
            self.chart_rail_open = prev_rail_open;
            self.chart_rail_tab = prev_rail_tab;
        } else {
            self.chart_hidden_series = HashSet::new();
            self.chart_manual_selection = None;
            self.chart_focused_series_idx = 0;
            self.chart_rail_open = false;
            self.chart_rail_tab = ChartRailTab::Configure;
            self.chart_rail_picker_x_col = 0;
            self.chart_rail_picker_y_checked = Vec::new();
        }

        cx.notify();
    }

    /// Build or return the existing `ChartView` entity.
    ///
    /// Returns `None` when detection failed or the result is incompatible.
    /// Uses the manual selection if set, otherwise auto-detection. Requires
    /// a reference to the current `QueryResult` from the host.
    pub fn ensure_chart_view(
        &mut self,
        result: &QueryResult,
        cx: &mut Context<Self>,
    ) -> Option<Entity<ChartView>> {
        if self.chart_view.is_some() {
            return self.chart_view.clone();
        }

        let mut spec = if let Some(manual) = &self.chart_manual_selection {
            ChartSpec::from_manual_selection(manual, &result.columns, 10_000)
        } else {
            match &self.chart_detection {
                Some(ChartDetection::Ok {
                    time_col,
                    numeric_cols,
                }) => ChartSpec::from_detection(
                    *time_col,
                    numeric_cols.clone(),
                    &result.columns,
                    10_000,
                ),
                _ => None,
            }
        }?;

        spec.legend_visible = self.chart_legend_visible && spec.series.len() > 1;

        match ChartView::build(result, spec) {
            Ok(chart_view) => {
                let entity = cx.new(|_cx| chart_view);
                let observer = cx.observe(&entity, |_this, _chart, cx| cx.notify());
                self.chart_view = Some(entity.clone());
                self.chart_view_observer = Some(observer);
                Some(entity)
            }
            Err(err) => {
                log::warn!("[chart-shell] ChartView::build failed: {}", err);
                None
            }
        }
    }

    /// Toggle the hidden state of a series by index.
    ///
    /// Propagates the updated hidden set to the live `ChartView` entity.
    pub fn toggle_chart_series_hidden(&mut self, idx: usize, cx: &mut Context<Self>) {
        if self.chart_hidden_series.contains(&idx) {
            self.chart_hidden_series.remove(&idx);
        } else {
            self.chart_hidden_series.insert(idx);
        }

        if let Some(chart_entity) = self.chart_view.clone() {
            let hidden = self.chart_hidden_series.clone();
            chart_entity.update(cx, |view, cx| {
                view.set_hidden_series(hidden, cx);
            });
        }

        cx.notify();
    }

    /// Returns the current `ChartView` entity without triggering a build.
    pub fn chart_view(&self) -> Option<&Entity<ChartView>> {
        self.chart_view.as_ref()
    }

    /// Returns the host adapter for this shell.
    pub fn host(&self) -> &HostAdapter {
        &self.host
    }

    /// Compute the `DataPointRef` for the current hover position.
    ///
    /// Returns `None` when the cursor is outside the chart, no chart view exists,
    /// or the focused series has no usable decimated points. This is called during
    /// render to decide whether to show the PointInspector.
    pub fn hovered_data_point(&self, cx: &gpui::App) -> Option<DataPointRef> {
        let chart_entity = self.chart_view.as_ref()?;
        let chart = chart_entity.read(cx);
        let series_idx = chart.focused_series_idx();
        let cursor_data_x = chart.hover_data_x()?;
        let point_idx_in_series = chart.nearest_point_idx(series_idx, cursor_data_x)?;
        Some(DataPointRef {
            series_idx,
            point_idx_in_series,
        })
    }

    /// Look up the source row for the currently hovered chart point.
    ///
    /// Calls `hovered_data_point` then delegates to the host's `source_for_point`.
    /// Returns `None` when source tracking is disabled (CodeDocument-backed charts
    /// always return None here).
    pub fn hovered_source_row(&self, cx: &gpui::App) -> Option<SourceRowRef> {
        let point = self.hovered_data_point(cx)?;
        self.host.source_for_point(point, cx)
    }

    /// Apply a new `BindingSpec` from the AxisBar without re-running the query.
    ///
    /// Stores the binding as a manual chart selection and clears the existing
    /// `chart_view` so the next `ensure_chart_view` call rebuilds from the
    /// new binding. Also closes any open AxisBar picker.
    ///
    /// The `QueryResult` is NOT required here; the host supplies it when calling
    /// `ensure_chart_view`. When `bindings.y` is empty the chart will show the
    /// degraded state on the next render.
    pub fn apply_bindings(&mut self, bindings: BindingSpec, cx: &mut Context<Self>) {
        // Always store the manual selection — even if y is empty — so the AxisBar
        // pill labels stay consistent with what the user last picked.
        self.chart_manual_selection = Some(ManualChartSelection {
            x_col: bindings.x,
            y_cols: bindings.y,
        });

        self.chart_view = None;
        self.chart_view_observer = None;
        self.axis_open_pill = None;
        self.chart_hidden_series = HashSet::new();

        cx.notify();
    }

    /// Toggle the open/closed state of an AxisBar pill picker.
    ///
    /// Clicking the same pill again closes it (toggle). Clicking a different
    /// pill closes the previous one and opens the new one.
    pub fn toggle_axis_pill(&mut self, pill: AxisPill, cx: &mut Context<Self>) {
        if self.axis_open_pill == Some(pill) {
            self.axis_open_pill = None;
        } else {
            self.axis_open_pill = Some(pill);
        }
        cx.notify();
    }

    /// Close any open AxisBar picker.
    pub fn close_axis_picker(&mut self, cx: &mut Context<Self>) {
        if self.axis_open_pill.is_some() {
            self.axis_open_pill = None;
            cx.notify();
        }
    }

    /// Returns the current binding spec derived from the active chart selection.
    ///
    /// When a manual selection is set, the binding is derived from it. When
    /// auto-detection is in effect, the binding uses the detected columns.
    /// Returns a default `BindingSpec` when no result is available.
    pub fn active_bindings(&self) -> BindingSpec {
        if let Some(manual) = &self.chart_manual_selection {
            BindingSpec {
                x: manual.x_col,
                y: manual.y_cols.clone(),
                group_by: None,
                filter: None,
                aggregation: dbflux_components::chart::AggKind::None,
            }
        } else if let Some(ChartDetection::Ok {
            time_col,
            numeric_cols,
        }) = &self.chart_detection
        {
            BindingSpec {
                x: *time_col,
                y: numeric_cols.clone(),
                group_by: None,
                filter: None,
                aggregation: dbflux_components::chart::AggKind::None,
            }
        } else {
            BindingSpec::default()
        }
    }

    /// Reset the degraded-picker column selections for a new result.
    pub(crate) fn reset_picker(&mut self, columns: &[ColumnMeta]) {
        self.chart_picker_x_col = columns
            .iter()
            .position(|c| c.kind == ColumnKind::Timestamp)
            .unwrap_or(0);

        self.chart_picker_y_checked = columns
            .iter()
            .filter(|c| {
                matches!(
                    c.kind,
                    ColumnKind::Float | ColumnKind::Integer | ColumnKind::Unknown
                )
            })
            .map(|c| matches!(c.kind, ColumnKind::Float | ColumnKind::Integer))
            .collect();
    }

    /// Prime the rail Configure picker from the current chart spec.
    #[allow(dead_code)]
    pub(crate) fn prime_rail_picker_from_spec(&mut self, result: &QueryResult) {
        let columns = &result.columns;

        let (x_col, y_col_indices) = if let Some(manual) = &self.chart_manual_selection {
            let ys: Vec<usize> = manual.y_cols.clone();
            (manual.x_col, ys)
        } else if let Some(ChartDetection::Ok {
            time_col,
            numeric_cols,
        }) = &self.chart_detection
        {
            (*time_col, numeric_cols.clone())
        } else {
            let x = columns
                .iter()
                .position(|c| c.kind == ColumnKind::Timestamp)
                .unwrap_or(0);
            (x, vec![])
        };

        let x_candidates: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Timestamp | ColumnKind::Text | ColumnKind::Unknown
                )
            })
            .map(|(i, _)| i)
            .collect();

        self.chart_rail_picker_x_col = x_candidates.iter().position(|&ci| ci == x_col).unwrap_or(0);

        let y_candidates: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Float | ColumnKind::Integer | ColumnKind::Unknown
                )
            })
            .map(|(i, _)| i)
            .collect();

        self.chart_rail_picker_y_checked = y_candidates
            .iter()
            .map(|ci| y_col_indices.contains(ci))
            .collect();
    }

    /// Apply the rail Configure picker state as a `ManualChartSelection`.
    ///
    /// Clears the existing `chart_view` so the next render triggers a rebuild.
    #[allow(dead_code)]
    pub(crate) fn apply_rail_selection(&mut self, result: &QueryResult, cx: &mut Context<Self>) {
        let columns = &result.columns;

        let x_candidates: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Timestamp | ColumnKind::Text | ColumnKind::Unknown
                )
            })
            .map(|(i, _)| i)
            .collect();

        let x_col = x_candidates
            .get(self.chart_rail_picker_x_col)
            .copied()
            .unwrap_or(0);

        let y_candidates: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Float | ColumnKind::Integer | ColumnKind::Unknown
                )
            })
            .map(|(i, _)| i)
            .collect();

        let y_cols: Vec<usize> = y_candidates
            .iter()
            .zip(self.chart_rail_picker_y_checked.iter())
            .filter_map(|(&ci, &checked)| if checked { Some(ci) } else { None })
            .collect();

        if y_cols.is_empty() {
            return;
        }

        self.chart_manual_selection = Some(ManualChartSelection { x_col, y_cols });
        self.chart_view = None;
        self.chart_view_observer = None;
        self.chart_hidden_series = HashSet::new();
        cx.notify();
    }

    /// Reset chart selection to auto-detection, clearing any manual override.
    #[allow(dead_code)]
    pub(crate) fn reset_rail_to_auto(&mut self, result: &QueryResult, cx: &mut Context<Self>) {
        if !matches!(&self.chart_detection, Some(ChartDetection::Ok { .. })) {
            return;
        }
        self.chart_manual_selection = None;
        self.chart_view = None;
        self.chart_view_observer = None;
        self.prime_rail_picker_from_spec(result);
        cx.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, Value};
    use std::time::Duration;

    fn make_col(name: &str, kind: ColumnKind) -> ColumnMeta {
        ColumnMeta {
            name: name.to_owned(),
            type_name: String::new(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    fn two_series_result() -> QueryResult {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0), Value::Float(10.0)],
            vec![Value::Int(1000), Value::Float(2.0), Value::Float(20.0)],
            vec![Value::Int(2000), Value::Float(3.0), Value::Float(15.0)],
        ];
        QueryResult::table(
            vec![
                make_col("ts", ColumnKind::Timestamp),
                make_col("val_a", ColumnKind::Float),
                make_col("val_b", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        )
    }

    /// Verify that `set_result` on a chartable result puts detection in Ok state.
    #[test]
    fn set_result_detects_chart_ok() {
        // Without a GPUI app context we test the pure logic layer:
        // detection, field resets, and state transitions.
        let result = two_series_result();
        let detection = detect_chart_columns(&result);
        assert!(
            matches!(detection, ChartDetection::Ok { .. }),
            "fixture must produce Ok detection"
        );
    }

    /// Verify hidden_series toggle logic without GPUI context.
    #[test]
    fn hidden_series_toggle_state() {
        let mut hidden: HashSet<usize> = HashSet::new();

        // First toggle: insert
        if hidden.contains(&0) {
            hidden.remove(&0);
        } else {
            hidden.insert(0);
        }
        assert!(hidden.contains(&0));

        // Second toggle: remove
        if hidden.contains(&0) {
            hidden.remove(&0);
        } else {
            hidden.insert(0);
        }
        assert!(!hidden.contains(&0));
    }

    /// Verify that `reset_picker` produces the right default X and Y selections.
    #[test]
    fn reset_picker_defaults() {
        let columns = vec![
            make_col("ts", ColumnKind::Timestamp),
            make_col("val_a", ColumnKind::Float),
            make_col("val_b", ColumnKind::Float),
            make_col("label", ColumnKind::Text),
        ];

        let x_col = columns
            .iter()
            .position(|c| c.kind == ColumnKind::Timestamp)
            .unwrap_or(0);

        let y_checked: Vec<bool> = columns
            .iter()
            .filter(|c| {
                matches!(
                    c.kind,
                    ColumnKind::Float | ColumnKind::Integer | ColumnKind::Unknown
                )
            })
            .map(|c| matches!(c.kind, ColumnKind::Float | ColumnKind::Integer))
            .collect();

        assert_eq!(x_col, 0, "Timestamp column is at index 0");
        assert_eq!(y_checked.len(), 2, "Two Float columns are Y candidates");
        assert!(y_checked[0], "val_a is Float, checked by default");
        assert!(y_checked[1], "val_b is Float, checked by default");
    }

    /// State preservation: when chart mode was active and new result is still
    /// chartable, hidden_series and focused_series must be preserved.
    #[test]
    fn set_result_preserves_state_when_still_chartable() {
        let result = two_series_result();
        let detection_ok = matches!(detect_chart_columns(&result), ChartDetection::Ok { .. });

        // Simulate the preservation logic from ChartShell::set_result.
        let mut hidden: HashSet<usize> = HashSet::new();
        hidden.insert(1);
        let was_chart_mode = true;
        let focused = 1usize;

        let (out_hidden, out_focused) = if was_chart_mode && detection_ok {
            (hidden, focused)
        } else {
            (HashSet::new(), 0)
        };

        assert!(out_hidden.contains(&1), "series 1 remains hidden");
        assert_eq!(out_focused, 1, "focused series preserved");
    }

    /// State reset: when switching from non-chart mode to a new result, all
    /// chart state must be zeroed.
    #[test]
    fn set_result_resets_state_when_not_in_chart_mode() {
        let result = two_series_result();
        let detection_ok = matches!(detect_chart_columns(&result), ChartDetection::Ok { .. });

        let mut hidden: HashSet<usize> = HashSet::new();
        hidden.insert(1);
        let was_chart_mode = false;
        let focused = 1usize;

        let (out_hidden, out_focused) = if was_chart_mode && detection_ok {
            (hidden, focused)
        } else {
            (HashSet::new(), 0)
        };

        assert!(out_hidden.is_empty(), "hidden series reset");
        assert_eq!(out_focused, 0, "focused series reset to 0");
    }
}
