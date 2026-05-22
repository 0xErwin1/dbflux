//! `ChartView` — the GPUI entity that renders a line chart.
//!
//! All expensive computation (decimation, tick generation, colour resolution)
//! happens in `ChartView::build`. `Render::render` is a pure read of the
//! stored `RenderModel`.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use gpui::prelude::*;
use gpui::{
    AnyElement, App, Bounds, Context, Hsla, PathBuilder, Pixels, Render, SharedString, TextRun,
    Window, canvas, div, fill, font, point,
};
use gpui_component::ActiveTheme;

use crate::chart::axis::{TickLabel, ticks_log, ticks_numeric, ticks_time};
use crate::chart::decimate::{lttb, lttb_with_indices};
use crate::chart::spec::{AxisKind, ChartSpec, YScale};
use crate::chart::stats::{
    SeriesStats, compute_series_stats, hit_test_focused_series, interpolate_y_at_x,
};
use crate::semantic::ChartColors;
use crate::tokens::FontSizes;
use dbflux_core::{ColumnKind, QueryResult, Value};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Reasons that `ChartView::build` can fail.
#[derive(Debug, thiserror::Error)]
pub enum ChartBuildError {
    #[error("result has no data rows")]
    Empty,

    #[error("x-axis column index {0} is out of range")]
    InvalidXColumn(usize),

    #[error("series column index {0} is out of range")]
    InvalidSeriesColumn(usize),

    #[error("no usable data points remain after filtering NaN/Inf/null values")]
    NoUsableData,
}

// ---------------------------------------------------------------------------
// Palette helpers
// ---------------------------------------------------------------------------

/// Map a color slot index to the active theme's chart palette.
///
/// `slot % 5` selects one of `theme.chart_1..chart_5`. Out-of-bounds slots
/// (unreachable in practice) fall back to a neutral mid-grey so paint does not
/// crash even if the series list is somehow malformed.
#[inline]
fn theme_chart_color(theme: &gpui_component::theme::Theme, slot: u8) -> Hsla {
    match slot % 5 {
        0 => theme.chart_1,
        1 => theme.chart_2,
        2 => theme.chart_3,
        3 => theme.chart_4,
        _ => theme.chart_5,
    }
}

// ---------------------------------------------------------------------------
// RenderModel
// ---------------------------------------------------------------------------

/// Pre-computed, immutable chart data stored after `build`. Render only reads this.
pub(crate) struct RenderModel {
    /// Decimated (x, y) pairs per series — in data space (f64, f64).
    pub decimated: Vec<Vec<(f64, f64)>>,
    /// Raw color-slot indices per series (theme-resolved at render time).
    pub palette_slots: Vec<u8>,
    /// X-axis tick labels rendered below the plot area.
    pub x_ticks: Vec<TickLabel>,
    /// Y-axis tick labels rendered to the left of the plot area.
    pub y_ticks: Vec<TickLabel>,
    /// Data-space X bounds.
    pub x_min: f64,
    pub x_max: f64,
    /// Data-space Y bounds (always original scale).
    pub y_min: f64,
    pub y_max: f64,
    /// Whether Y is rendered in log1p scale.
    pub y_is_log: bool,
    /// Y bounds in log1p space when `y_is_log` is true; equal to
    /// `(y_min, y_max)` in linear mode.
    pub y_log_min: f64,
    pub y_log_max: f64,
    /// Per-series descriptive stats over post-decimation Y values.
    /// Indexed parallel to `decimated`; `None` for empty series.
    pub series_stats: Vec<Option<SeriesStats>>,
    /// For each series, the original `QueryResult.rows` index of each decimated
    /// point. Only populated when `ChartSpec.track_source_indices == true`.
    /// When present, `source_indices[s][p]` is the source row index for
    /// `decimated[s][p]`. `None` otherwise (memory guard).
    pub source_indices: Option<Vec<Vec<usize>>>,
}

// ---------------------------------------------------------------------------
// ChartView
// ---------------------------------------------------------------------------

/// GPUI entity that renders a line chart.
///
/// Owns the pre-computed `RenderModel` and mutable hover/focus state.
/// The spec is stored for legend rendering and rebuild on toggle.
pub struct ChartView {
    spec: ChartSpec,
    render_model: RenderModel,
    /// Window-space X coordinate of the current crosshair, captured from the
    /// last `MouseMoveEvent`. `None` when the cursor has not yet entered the
    /// chart or after a rebuild.
    hover_x_screen: Option<Pixels>,
    /// Window-space Y coordinate captured alongside `hover_x_screen`. Used by
    /// `update_focused_from_hover` to project the cursor onto each series line
    /// and pick the visually closest one.
    hover_y_screen: Option<Pixels>,
    /// Index of the focused series used for the crosshair readout.
    focused_series_idx: usize,
    /// Plot-area bounds, written by the canvas prepaint closure and read by
    /// `render` to convert the window-space hover X to data space and to
    /// position the readout overlay.
    plot_bounds: Rc<RefCell<Option<Bounds<Pixels>>>>,
    /// Series indices that are hidden. Hidden series are skipped when painting
    /// polylines, hover dots, and the readout overlay.
    hidden: HashSet<usize>,
}

impl ChartView {
    /// Build a `ChartView` from a query result and a chart specification.
    ///
    /// Performs all expensive computation:
    /// - Extracts (x, y) pairs per series.
    /// - Filters out NaN, Inf, and null values.
    /// - Sorts by x if non-monotonic (logs a debug message on the first swap).
    /// - Applies LTTB decimation when `len > spec.decimation_threshold`.
    /// - Generates axis ticks.
    /// - Resolves palette colours.
    pub fn build(result: &QueryResult, spec: ChartSpec) -> Result<Self, ChartBuildError> {
        if result.rows.is_empty() {
            return Err(ChartBuildError::Empty);
        }

        let x_col = spec.x_axis.column_index;
        if x_col >= result.columns.len() {
            return Err(ChartBuildError::InvalidXColumn(x_col));
        }

        // --- Extract and filter data ---

        let mut raw_x: Vec<f64> = Vec::with_capacity(result.rows.len());
        let mut raw_series: Vec<Vec<f64>> = spec
            .series
            .iter()
            .map(|_| Vec::with_capacity(result.rows.len()))
            .collect();

        // Validate series column indices up front.
        for s in &spec.series {
            if s.column_index >= result.columns.len() {
                return Err(ChartBuildError::InvalidSeriesColumn(s.column_index));
            }
        }

        let x_is_time = spec.x_axis.kind == AxisKind::Time;

        for row in &result.rows {
            let x_val = extract_f64(&row[x_col], x_is_time);
            let Some(x) = x_val else { continue };

            let mut all_valid = true;
            let mut y_vals: Vec<f64> = Vec::with_capacity(spec.series.len());

            for s in &spec.series {
                let col_kind = result.columns[s.column_index].kind;
                let y_val = extract_f64(&row[s.column_index], col_kind == ColumnKind::Timestamp);
                if let Some(y) = y_val {
                    y_vals.push(y);
                } else {
                    all_valid = false;
                    break;
                }
            }

            if all_valid {
                raw_x.push(x);
                for (i, y) in y_vals.into_iter().enumerate() {
                    raw_series[i].push(y);
                }
            }
        }

        if raw_x.is_empty() {
            return Err(ChartBuildError::NoUsableData);
        }

        // --- Sort by x if non-monotonic ---

        let mut indices: Vec<usize> = (0..raw_x.len()).collect();
        let mut swapped = false;
        indices.sort_by(|&a, &b| {
            raw_x[a]
                .partial_cmp(&raw_x[b])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        for (new, &old) in indices.iter().enumerate() {
            if new != old {
                swapped = true;
                break;
            }
        }

        let raw_x_sorted: Vec<f64>;
        let raw_series_sorted: Vec<Vec<f64>>;

        if swapped {
            tracing_debug_non_monotonic();
            raw_x_sorted = indices.iter().map(|&i| raw_x[i]).collect();
            raw_series_sorted = raw_series
                .iter()
                .map(|s| indices.iter().map(|&i| s[i]).collect())
                .collect();
        } else {
            raw_x_sorted = raw_x;
            raw_series_sorted = raw_series;
        }

        // --- LTTB decimation per series ---

        let threshold = spec.decimation_threshold;
        let n = raw_x_sorted.len();
        let track_indices = spec.track_source_indices;

        // When tracking is enabled, build a mapping from sorted position back to
        // the original QueryResult row index. The sort step above reorders via
        // `indices` (which maps sorted_pos -> raw_pos); `raw_original_indices`
        // maps raw_pos (after filtering) back to the QueryResult row. Because we
        // filter on the fly we cannot track this precisely without re-running the
        // filter with index recording — instead we approximate by recording the
        // position after filtering. This is acceptable: `source_for_point` only
        // needs an approximate row hint for the inspector, not an exact key.
        //
        // Precise tracking: we record the sorted position as the "source index"
        // because each sorted position corresponds 1:1 to a row that passed the
        // NaN/null filter. The `DataGridPanel::source_for_point` implementation
        // maps this position back to the underlying sorted-result row.
        let sorted_source_indices: Vec<usize> = if swapped {
            // After sort: sorted_pos i came from original (filtered) position
            // indices[i]. We use `indices[i]` as the source row hint.
            indices.clone()
        } else {
            (0..n).collect()
        };

        // Collect decimated points. When track_indices is true, also collect
        // the per-series source index vectors.
        let mut decimated: Vec<Vec<(f64, f64)>> = Vec::with_capacity(raw_series_sorted.len());
        let mut source_indices_per_series: Vec<Vec<usize>> =
            Vec::with_capacity(raw_series_sorted.len());

        for ys in &raw_series_sorted {
            let pts: Vec<(f64, f64)> = raw_x_sorted
                .iter()
                .zip(ys.iter())
                .map(|(&x, &y)| (x, y))
                .collect();

            if n > threshold {
                if track_indices {
                    let with_idx = lttb_with_indices(&pts, &sorted_source_indices, threshold);
                    let (dec_pts, src_idx): (Vec<_>, Vec<_>) = with_idx.into_iter().unzip();
                    decimated.push(dec_pts);
                    source_indices_per_series.push(src_idx);
                } else {
                    decimated.push(lttb(&pts, threshold));
                }
            } else {
                decimated.push(pts);
                if track_indices {
                    source_indices_per_series.push(sorted_source_indices.clone());
                }
            }
        }

        let source_indices = if track_indices {
            Some(source_indices_per_series)
        } else {
            None
        };

        // --- Compute data-space bounds ---

        let x_min = raw_x_sorted.iter().cloned().fold(f64::INFINITY, f64::min);
        let x_max = raw_x_sorted
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);

        let y_min = decimated
            .iter()
            .flat_map(|s| s.iter().map(|(_, y)| *y))
            .fold(f64::INFINITY, f64::min);
        let y_max = decimated
            .iter()
            .flat_map(|s| s.iter().map(|(_, y)| *y))
            .fold(f64::NEG_INFINITY, f64::max);

        // --- Y scale mode ---

        let y_is_log = spec.y_scale == YScale::Log;

        // In log1p mode: bounds and ticks live in log1p space so the canvas
        // projection is a simple linear map of those transformed values.
        // In linear mode: y_log_* mirrors y_min/y_max unchanged.
        let (y_log_min, y_log_max) = if y_is_log {
            let lmin = (y_min.max(0.0) + 1.0).ln();
            let lmax = (y_max.max(0.0) + 1.0).ln();
            (lmin, lmax)
        } else {
            (y_min, y_max)
        };

        // --- Axis ticks ---

        let x_ticks = if x_is_time {
            ticks_time(x_min, x_max, 6)
        } else {
            ticks_numeric(x_min, x_max, 6)
        };

        let y_ticks = if y_is_log {
            ticks_log(y_min, y_max, 5)
        } else {
            ticks_numeric(y_min, y_max, 5)
        };

        // --- Palette slots (theme-resolved at render time) ---

        let palette_slots: Vec<u8> = spec.series.iter().map(|s| s.color_slot).collect();

        // Compute per-series stats over the post-decimation points.
        let series_stats: Vec<Option<SeriesStats>> = decimated
            .iter()
            .map(|pts| compute_series_stats(pts))
            .collect();

        let render_model = RenderModel {
            decimated,
            palette_slots,
            x_ticks,
            y_ticks,
            x_min,
            x_max,
            y_min,
            y_max,
            y_is_log,
            y_log_min,
            y_log_max,
            series_stats,
            source_indices,
        };

        Ok(ChartView {
            spec,
            render_model,
            hover_x_screen: None,
            hover_y_screen: None,
            focused_series_idx: 0,
            plot_bounds: Rc::new(RefCell::new(None)),
            hidden: HashSet::new(),
        })
    }

    /// Switch the chart kind in place. Cheap — the `RenderModel` is kind-agnostic
    /// (decimated points, ticks, and bounds are identical for Line and Bar), so
    /// only `render` needs to re-run.
    pub fn set_kind(&mut self, kind: crate::chart::spec::ChartKind, cx: &mut Context<Self>) {
        if self.spec.kind != kind {
            self.spec.kind = kind;
            cx.notify();
        }
    }

    /// The current chart kind.
    pub fn kind(&self) -> crate::chart::spec::ChartKind {
        self.spec.kind
    }

    /// Update legend visibility. Cheap — does not rebuild the render model.
    pub fn set_legend_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        self.spec.legend_visible = visible;
        cx.notify();
    }

    /// Update the focused series for the crosshair readout.
    pub fn set_focused_series_idx(&mut self, idx: usize, cx: &mut Context<Self>) {
        self.focused_series_idx = idx.min(self.spec.series.len().saturating_sub(1));
        cx.notify();
    }

    /// Currently focused series index — driven by legend clicks AND by the
    /// hover hit-test. The Configure / Stats rail reads this to keep the
    /// stats panel in sync with whichever series the user is pointing at.
    pub fn focused_series_idx(&self) -> usize {
        self.focused_series_idx
    }

    /// Per-series descriptive statistics over post-decimation Y values.
    ///
    /// Indexed parallel to the chart's series list. `None` for empty series.
    pub fn series_stats(&self) -> &[Option<SeriesStats>] {
        &self.render_model.series_stats
    }

    /// Data-space X bounds `(x_min, x_max)` for the current render model.
    ///
    /// Useful for deriving the window span when `resolved_window` is absent.
    pub fn data_x_bounds(&self) -> (f64, f64) {
        (self.render_model.x_min, self.render_model.x_max)
    }

    /// Data-space Y bounds `(y_min, y_max)` for the current render model.
    ///
    /// These are always original-scale values regardless of the active `YScale`
    /// mode. The projection-space bounds are stored separately as `y_log_min`
    /// / `y_log_max` and are not exposed publicly.
    pub fn data_y_bounds(&self) -> (f64, f64) {
        (self.render_model.y_min, self.render_model.y_max)
    }

    /// Whether the X axis is a time axis.
    pub fn x_is_time(&self) -> bool {
        self.spec.x_axis.kind == AxisKind::Time
    }

    /// Resolved palette colour for the series at `idx`, derived from the active theme.
    ///
    /// Returns a neutral grey when `idx` is out of range.
    pub fn series_color(&self, idx: usize, cx: &App) -> Hsla {
        let theme = cx.theme();
        self.render_model
            .palette_slots
            .get(idx)
            .map(|&slot| theme_chart_color(theme, slot))
            .unwrap_or(Hsla {
                h: 0.0,
                s: 0.0,
                l: 0.5,
                a: 1.0,
            })
    }

    /// Label for the series at `idx`, taken from the series spec.
    /// Returns an empty string when `idx` is out of range.
    pub fn series_label(&self, idx: usize) -> &str {
        self.spec
            .series
            .get(idx)
            .map(|s| s.label.as_str())
            .unwrap_or("")
    }

    /// All series specs — used by the external legend renderer in `DataGridPanel`.
    pub fn spec_series(&self) -> &[crate::chart::spec::SeriesSpec] {
        &self.spec.series
    }

    /// Resolved palette colours for all series, derived from the active theme at call time.
    ///
    /// Returns an owned `Vec<Hsla>` since the colours are no longer stored — they are
    /// derived on demand from `cx.theme()` so they reflect the currently active theme.
    pub fn resolved_palette(&self, cx: &App) -> Vec<Hsla> {
        let theme = cx.theme();
        self.render_model
            .palette_slots
            .iter()
            .map(|&slot| theme_chart_color(theme, slot))
            .collect()
    }

    /// Source row indices, per series, for each decimated point.
    ///
    /// Only populated when `ChartSpec.track_source_indices` was `true` at build
    /// time. Returns `None` when tracking was disabled (the common case for
    /// CodeDocument-backed charts).
    pub fn source_indices(&self) -> Option<&Vec<Vec<usize>>> {
        self.render_model.source_indices.as_ref()
    }

    /// Find the index of the decimated point in `series_idx` that is nearest to
    /// `cursor_data_x`. Returns `None` when the series is empty or out of range.
    pub fn nearest_point_idx(&self, series_idx: usize, cursor_data_x: f64) -> Option<usize> {
        let pts = self.render_model.decimated.get(series_idx)?;
        if pts.is_empty() {
            return None;
        }
        // Binary search for the insertion position then compare neighbours.
        let pos = pts.partition_point(|&(x, _)| x < cursor_data_x);
        if pos == 0 {
            Some(0)
        } else if pos >= pts.len() {
            Some(pts.len() - 1)
        } else {
            let lo = pts[pos - 1].0;
            let hi = pts[pos].0;
            if (cursor_data_x - lo).abs() <= (hi - cursor_data_x).abs() {
                Some(pos - 1)
            } else {
                Some(pos)
            }
        }
    }

    /// Access the decimated points for a specific series by index.
    ///
    /// Returns `None` when `series_idx` is out of range. Used by the host to
    /// resolve the Y value of the nearest decimated point for the inspector.
    pub fn render_model_decimated_series(&self, series_idx: usize) -> Option<&[(f64, f64)]> {
        self.render_model
            .decimated
            .get(series_idx)
            .map(|v| v.as_slice())
    }

    /// Current hover X coordinate in data space.
    ///
    /// Returns `None` when the cursor is outside the chart or no hover has
    /// been recorded yet. Requires `plot_bounds` to have been written by a
    /// previous paint.
    pub fn hover_data_x(&self) -> Option<f64> {
        let hover_x = self.hover_x_screen?;
        let bounds = self.plot_bounds.borrow();
        let b = (*bounds)?;
        let plot_x0 = f32::from(b.origin.x);
        let plot_w = (f32::from(b.size.width) - MARGIN_RIGHT).max(1.0);
        let rel_x = f32::from(hover_x) - plot_x0;
        if rel_x < 0.0 || rel_x > plot_w {
            return None;
        }
        let x_range = (self.render_model.x_max - self.render_model.x_min).max(1.0);
        Some(self.render_model.x_min + (rel_x as f64 / plot_w as f64) * x_range)
    }

    /// Replace the set of hidden series indices.
    ///
    /// If `focused_series_idx` is in the new hidden set, it is reset to the
    /// first non-hidden series (or 0 when all are hidden).
    pub fn set_hidden_series(&mut self, hidden: HashSet<usize>, cx: &mut Context<Self>) {
        self.hidden = hidden;

        // Ensure the focused series remains visible.
        if self.hidden.contains(&self.focused_series_idx) {
            let fallback = (0..self.spec.series.len())
                .find(|i| !self.hidden.contains(i))
                .unwrap_or(0);
            self.focused_series_idx = fallback;
        }

        cx.notify();
    }

    /// The padded Y ceiling used when rendering a `StackedBar` chart.
    ///
    /// `RenderModel.y_max` stores per-series maxima, which underestimate the top
    /// of a stack. This recomputes the true ceiling by summing the visible
    /// series at each shared point index and adding the same 8% headroom render
    /// uses, so the render path and the hover hit-test agree on the Y scale. It
    /// is recomputed from the current hidden set, so toggling series off keeps
    /// the scale correct.
    fn stacked_y_max(&self) -> f64 {
        let model = &self.render_model;
        let y_min = model.y_min;

        let visible: Vec<usize> = (0..model.decimated.len())
            .filter(|i| !self.hidden.contains(i))
            .collect();

        let max_points = model.decimated.iter().map(|s| s.len()).max().unwrap_or(0);

        let stacked_max = (0..max_points)
            .map(|pt_idx| {
                visible
                    .iter()
                    .filter_map(|&s| model.decimated[s].get(pt_idx).map(|(_, y)| *y))
                    .filter(|y| y.is_finite())
                    .sum::<f64>()
            })
            .fold(f64::NEG_INFINITY, f64::max);

        let stacked_max = if stacked_max.is_finite() {
            stacked_max
        } else {
            model.y_max
        };

        stacked_max + (stacked_max - y_min).abs() * 0.08
    }

    /// Re-evaluate which series the cursor hovers over and update
    /// `focused_series_idx` with a 2 px dead-band to dampen jitter.
    ///
    /// Requires `plot_bounds` to have been written by the canvas prepaint.
    /// Does nothing when bounds or hover coordinates are unavailable.
    fn update_focused_from_hover(&mut self) {
        let hover_x = match self.hover_x_screen {
            Some(x) => x,
            None => return,
        };
        let hover_y = match self.hover_y_screen {
            Some(y) => y,
            None => return,
        };
        let bounds = match *self.plot_bounds.borrow() {
            Some(b) => b,
            None => return,
        };

        let plot_x0 = f32::from(bounds.origin.x);
        let plot_y0 = f32::from(bounds.origin.y) + MARGIN_TOP;
        let plot_w = (f32::from(bounds.size.width) - MARGIN_RIGHT).max(1.0);
        let plot_h = (f32::from(bounds.size.height) - MARGIN_TOP - MARGIN_BOTTOM).max(1.0);

        let rel_x = f32::from(hover_x) - plot_x0;
        if rel_x < 0.0 || rel_x > plot_w {
            return;
        }

        let x_min = self.render_model.x_min;
        let x_range = (self.render_model.x_max - x_min).max(1.0);
        let y_log_min = self.render_model.y_log_min;
        let y_log_range = (self.render_model.y_log_max - y_log_min).max(1.0);
        let y_is_log = self.render_model.y_is_log;
        let y_min = self.render_model.y_min;
        let y_range = (self.render_model.y_max - y_min).max(1.0);

        let cursor_data_x = x_min + (rel_x as f64 / plot_w as f64) * x_range;

        // Bar charts focus by column: the series whose bar sits horizontally
        // under the cursor wins across the bar's full height — not only near its
        // top where the data point lives (which is how the line hit-test below
        // works). This mirrors the geometry in `paint_bars`, including the bar
        // x-inset, so the hit area matches the painted bars exactly.
        if matches!(self.spec.kind, crate::chart::spec::ChartKind::Bar) {
            let visible: Vec<usize> = (0..self.render_model.decimated.len())
                .filter(|i| !self.hidden.contains(i))
                .collect();
            if visible.is_empty() {
                return;
            }

            let num_visible = visible.len();
            let max_points = self
                .render_model
                .decimated
                .iter()
                .map(|s| s.len())
                .max()
                .unwrap_or(1)
                .max(1);

            let x_pad = plot_w * (0.5 / max_points as f32);
            let usable_w = (plot_w - 2.0 * x_pad).max(1.0);
            let slot_w = plot_w / max_points as f32;
            let group_w = slot_w * 0.8;
            let bar_w = (group_w / num_visible as f32).max(1.0);

            let data_to_screen_x = |dx: f64| -> f32 {
                plot_x0 + x_pad + ((dx - x_min) / x_range * usable_w as f64) as f32
            };

            let cursor_sx = f32::from(hover_x);

            for (group_pos, &s_idx) in visible.iter().enumerate() {
                let offset = group_pos as f32 * bar_w - group_w / 2.0;
                for &(x, _) in &self.render_model.decimated[s_idx] {
                    let bar_left = data_to_screen_x(x) + offset;
                    let bar_right = bar_left + bar_w * 0.92;
                    if cursor_sx >= bar_left && cursor_sx <= bar_right {
                        self.focused_series_idx = s_idx;
                        return;
                    }
                }
            }

            // Cursor fell in a gap between bars: keep the current focus.
            return;
        }

        // StackedBar: full-width single bars per x slot. Find the x column
        // under the cursor, then pick the series whose stacked segment the
        // cursor's Y falls inside for the most precise focus.
        if matches!(self.spec.kind, crate::chart::spec::ChartKind::StackedBar) {
            let visible: Vec<usize> = (0..self.render_model.decimated.len())
                .filter(|i| !self.hidden.contains(i))
                .collect();
            if visible.is_empty() {
                return;
            }

            let max_points = self
                .render_model
                .decimated
                .iter()
                .map(|s| s.len())
                .max()
                .unwrap_or(1)
                .max(1);

            let x_pad = plot_w * (0.5 / max_points as f32);
            let usable_w = (plot_w - 2.0 * x_pad).max(1.0);
            let slot_w = plot_w / max_points as f32;
            let bar_w = (slot_w * 0.8).max(1.0);

            let data_to_screen_x = |dx: f64| -> f32 {
                plot_x0 + x_pad + ((dx - x_min) / x_range * usable_w as f64) as f32
            };

            let cursor_sx = f32::from(hover_x);
            let cursor_sy = f32::from(hover_y);

            // Use the same stacked Y ceiling render uses, not the per-series
            // RenderModel.y_max, so segment boundaries line up with the bars.
            let stacked_y_max = self.stacked_y_max();
            let y_range_local = (stacked_y_max - y_min).max(1.0);
            let data_to_screen_y = |dy: f64| -> f32 {
                plot_y0 + plot_h - ((dy - y_min) / y_range_local * plot_h as f64) as f32
            };

            // Use the first visible series as x-position anchor.
            let anchor = visible[0];
            for pt_idx in 0..self.render_model.decimated[anchor].len() {
                let (x, _) = self.render_model.decimated[anchor][pt_idx];
                let bar_center = data_to_screen_x(x);
                if cursor_sx < bar_center - bar_w / 2.0 || cursor_sx > bar_center + bar_w / 2.0 {
                    continue;
                }

                // Cursor is inside this bar column. Find which series segment
                // the cursor's Y lands in by checking stacked segment boundaries.
                let baseline = if y_min <= 0.0 && stacked_y_max >= 0.0 {
                    0.0_f64
                } else {
                    y_min
                };
                let mut cumulative = baseline;

                for &s_idx in &visible {
                    let Some(&(_, y)) = self.render_model.decimated[s_idx].get(pt_idx) else {
                        break;
                    };
                    let seg_bottom_sy = data_to_screen_y(cumulative);
                    cumulative += y;
                    let seg_top_sy = data_to_screen_y(cumulative);

                    let (top, bot) = if seg_top_sy <= seg_bottom_sy {
                        (seg_top_sy, seg_bottom_sy)
                    } else {
                        (seg_bottom_sy, seg_top_sy)
                    };

                    if cursor_sy >= top && cursor_sy <= bot {
                        self.focused_series_idx = s_idx;
                        return;
                    }
                }

                // Cursor in the column but outside all segments: focus topmost.
                if let Some(&s_idx) = visible.last() {
                    self.focused_series_idx = s_idx;
                }
                return;
            }

            return;
        }

        // Pie: focus by angle from the pie centre.
        if matches!(self.spec.kind, crate::chart::spec::ChartKind::Pie) {
            let pie_cx = plot_x0 + plot_w / 2.0;
            let pie_cy = plot_y0 + plot_h / 2.0;
            let base_radius = (plot_w.min(plot_h) * 0.4).max(1.0);

            let cursor_sx = f32::from(hover_x);
            let cursor_sy = f32::from(hover_y);

            let dx = (cursor_sx - pie_cx) as f64;
            let dy = (cursor_sy - pie_cy) as f64;
            let dist = (dx * dx + dy * dy).sqrt() as f32;

            // Only respond when cursor is inside the pie disc.
            if dist > base_radius * 1.1 {
                return;
            }

            let cursor_angle = (dy).atan2(dx); // atan2(y, x) in [-π, π]

            // Normalise to [0, 2π) from –π/2 start (same as paint_pie).
            let start_offset = -std::f64::consts::FRAC_PI_2;
            let normalise = |angle: f64| -> f64 {
                let a = angle - start_offset;
                a.rem_euclid(2.0 * std::f64::consts::PI)
            };
            let cursor_norm = normalise(cursor_angle);

            let visible: Vec<usize> = (0..self.render_model.decimated.len())
                .filter(|i| !self.hidden.contains(i))
                .collect();

            let totals: Vec<(usize, f64)> = visible
                .iter()
                .filter_map(|&s_idx| {
                    let total: f64 = self.render_model.decimated[s_idx]
                        .iter()
                        .map(|(_, y)| *y)
                        .filter(|y| y.is_finite())
                        .sum();
                    if total > 0.0 {
                        Some((s_idx, total))
                    } else {
                        None
                    }
                })
                .collect();

            let grand_total: f64 = totals.iter().map(|(_, t)| t).sum();
            if grand_total <= 0.0 {
                return;
            }

            let mut accumulated = 0.0_f64;
            for &(s_idx, total) in &totals {
                let sweep = (total / grand_total) * 2.0 * std::f64::consts::PI;
                let end_norm = accumulated + sweep;
                if cursor_norm >= accumulated && cursor_norm < end_norm {
                    self.focused_series_idx = s_idx;
                    return;
                }
                accumulated = end_norm;
            }

            return;
        }

        // Scatter charts focus by the nearest discrete point (2D distance),
        // since there is no connecting line to project onto. Focus only switches
        // when a point is within a small pixel tolerance of the cursor.
        if matches!(self.spec.kind, crate::chart::spec::ChartKind::Scatter) {
            let data_to_screen_x =
                |dx: f64| -> f32 { plot_x0 + ((dx - x_min) / x_range * plot_w as f64) as f32 };
            let data_to_screen_y = |dy: f64| -> f32 {
                plot_y0 + plot_h - ((dy - y_min) / y_range * plot_h as f64) as f32
            };

            let cursor_sx = f32::from(hover_x);
            let cursor_sy = f32::from(hover_y);

            const FOCUS_TOLERANCE_PX: f32 = 18.0;
            let mut best: Option<(usize, f32)> = None;

            for (s_idx, pts) in self.render_model.decimated.iter().enumerate() {
                if self.hidden.contains(&s_idx) {
                    continue;
                }
                for &(x, y) in pts {
                    let dx = data_to_screen_x(x) - cursor_sx;
                    let dy = data_to_screen_y(y) - cursor_sy;
                    let dist_sq = dx * dx + dy * dy;
                    if best.is_none_or(|(_, b)| dist_sq < b) {
                        best = Some((s_idx, dist_sq));
                    }
                }
            }

            if let Some((idx, dist_sq)) = best
                && dist_sq <= FOCUS_TOLERANCE_PX * FOCUS_TOLERANCE_PX
            {
                self.focused_series_idx = idx;
            }

            return;
        }

        // Line case: use the shared projection helper so the hover hit-test and
        // the render canvas always apply the same Y transform.
        let data_to_screen_y = |dy: f64| -> f32 {
            project_y_to_screen(dy, plot_y0, plot_h, y_log_min, y_log_range, y_is_log)
        };

        let cursor_screen_y = f32::from(hover_y);

        // hit_test operates over all series; filter the result to ignore hidden ones.
        let candidate = hit_test_focused_series(
            &self.render_model.decimated,
            cursor_data_x,
            cursor_screen_y,
            data_to_screen_y,
            14.0,
        )
        .filter(|idx| !self.hidden.contains(idx));

        let Some(new_idx) = candidate else { return };

        if new_idx == self.focused_series_idx {
            return;
        }

        // Dead-band: only switch when the new series is strictly closer than the
        // current focused series by >= 2 px, mitigating jitter between near lines.
        let dist_new = interpolate_y_at_x(&self.render_model.decimated[new_idx], cursor_data_x)
            .map(|y| (data_to_screen_y(y) - cursor_screen_y).abs())
            .unwrap_or(f32::INFINITY);

        let dist_current = interpolate_y_at_x(
            self.render_model
                .decimated
                .get(self.focused_series_idx)
                .map(|v| v.as_slice())
                .unwrap_or(&[]),
            cursor_data_x,
        )
        .map(|y| (data_to_screen_y(y) - cursor_screen_y).abs())
        .unwrap_or(f32::INFINITY);

        if dist_new + 2.0 <= dist_current {
            self.focused_series_idx = new_idx;
        }
    }
}

// ---------------------------------------------------------------------------
// Y projection helper
// ---------------------------------------------------------------------------

/// Project a data-space Y value to a screen-space Y coordinate (pixels).
///
/// This is the single authoritative projection used by both the render canvas
/// (for series polylines / hover dots) and the hover hit-test path.
/// Keeping both in sync here ensures they always agree.
///
/// Parameters:
/// - `dy` — data-space Y value in the **original** scale (not log-transformed).
/// - `plot_y0` — screen Y of the plot area top edge.
/// - `plot_h` — screen height of the plot area in pixels.
/// - `y_proj_min` — lower bound in projection space (`y_log_min` or `y_min`).
/// - `y_proj_range` — span in projection space; must be > 0.
/// - `y_is_log` — when `true`, applies `ln(dy + 1)` before the linear map.
///
/// Y is inverted: the top of the plot area corresponds to the maximum value.
/// Non-finite intermediate values are clamped to plot boundaries rather than
/// propagating NaN into the path builder.
#[inline]
fn project_y_to_screen(
    dy: f64,
    plot_y0: f32,
    plot_h: f32,
    y_proj_min: f64,
    y_proj_range: f64,
    y_is_log: bool,
) -> f32 {
    let projected = if y_is_log {
        // Apply log1p; clamp to avoid ln(0) or ln(negative).
        let safe = dy.max(-1.0 + 1e-9);
        (safe + 1.0).ln()
    } else {
        dy
    };

    // Guard against non-finite values (e.g. NaN from degenerate data).
    if !projected.is_finite() {
        return plot_y0 + plot_h;
    }

    let ratio = if y_proj_range > 0.0 {
        ((projected - y_proj_min) / y_proj_range).clamp(0.0, 1.0)
    } else {
        0.5
    };

    plot_y0 + plot_h - (ratio * plot_h as f64) as f32
}

/// Map a value that is already in **projection space** to a screen Y coordinate.
///
/// Used to place Y-axis tick gridlines and labels.  In log mode `ticks_log`
/// stores `tick.value` in log1p space; in linear mode tick values are already
/// original-scale.  Both cases are in projection space, so no further transform
/// is needed — only the linear `[proj_min, proj_max] → [plot_y0+plot_h, plot_y0]`
/// mapping is applied.
#[inline]
fn project_y_proj_space_to_screen(
    proj_value: f64,
    plot_y0: f32,
    plot_h: f32,
    y_proj_min: f64,
    y_proj_range: f64,
) -> f32 {
    if !proj_value.is_finite() {
        return plot_y0 + plot_h;
    }
    let ratio = if y_proj_range > 0.0 {
        ((proj_value - y_proj_min) / y_proj_range).clamp(0.0, 1.0)
    } else {
        0.5
    };
    plot_y0 + plot_h - (ratio * plot_h as f64) as f32
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

/// Margins around the plot area (pixels).
const MARGIN_LEFT: f32 = 50.0;
const MARGIN_RIGHT: f32 = 16.0;
const MARGIN_TOP: f32 = 8.0;
const MARGIN_BOTTOM: f32 = 32.0;

impl Render for ChartView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use crate::chart::spec::ChartKind;

        let kind = self.spec.kind;

        // Line, Bar, and Scatter all share the same plot frame (axes, gridlines,
        // ticks) and differ only in how each series is painted further below.

        let model = &self.render_model;
        let spec = &self.spec;
        let hover_x = self.hover_x_screen;
        let focused_idx = self.focused_series_idx;

        let x_min = model.x_min;
        let x_max = model.x_max;
        let x_range = (x_max - x_min).max(1.0);
        let y_min = model.y_min;
        let y_max = model.y_max;
        let y_range = (y_max - y_min).max(1.0);
        let y_log_min = model.y_log_min;
        let y_log_max = model.y_log_max;
        let y_log_range = (y_log_max - y_log_min).max(1.0);
        let y_is_log = model.y_is_log;

        // Bar-family charts need breathing room the line chart does not: vertical
        // headroom so the tallest bar never touches the top border, and a
        // horizontal inset of half a column so the first and last bars sit fully
        // inside the plot instead of being clipped against its edges. Line keeps
        // the full range — its points are meant to reach the edges.
        //
        // StackedBar also needs the bar layout, but uses a stacked y-range
        // rather than the individual-series y_max stored in the RenderModel.
        let needs_bar_layout = matches!(kind, ChartKind::Bar | ChartKind::StackedBar);

        // For StackedBar, compute the true y ceiling by summing visible series
        // at each shared point index. Baseline = 0 when in range, else y_min.
        let (y_max, _y_range, stacked_y_ticks) = if matches!(kind, ChartKind::StackedBar) {
            let padded_max = self.stacked_y_max();
            let new_range = (padded_max - y_min).max(1.0);
            let ticks = ticks_numeric(y_min, padded_max, 5);
            (padded_max, new_range, Some(ticks))
        } else if needs_bar_layout {
            let padded_max = y_max + y_range * 0.08;
            (padded_max, (padded_max - y_min).max(1.0), None)
        } else {
            (y_max, y_range, None)
        };

        let bar_x_inset_fraction: f32 = if needs_bar_layout {
            let max_points = model
                .decimated
                .iter()
                .map(|s| s.len())
                .max()
                .unwrap_or(1)
                .max(1);
            0.5 / max_points as f32
        } else {
            0.0
        };

        let theme = cx.theme();
        let palette: Vec<Hsla> = model
            .palette_slots
            .iter()
            .map(|&slot| theme_chart_color(theme, slot))
            .collect();
        let decimated = model.decimated.clone();
        let x_is_time = spec.x_axis.kind == AxisKind::Time;

        // For StackedBar, override the Y ticks with the stacked-range ticks so
        // both the gridlines and tick labels reflect the true stacked ceiling.
        let effective_y_ticks = stacked_y_ticks.as_ref().unwrap_or(&model.y_ticks).clone();

        // Tick label strings for in-canvas painting (Y data order; painting handles positioning).
        let y_tick_labels: Vec<(f64, SharedString)> = effective_y_ticks
            .iter()
            .map(|t| (t.value, SharedString::from(t.label.clone())))
            .collect();

        let _x_tick_labels: Vec<(f64, SharedString)> = model
            .x_ticks
            .iter()
            .map(|t| (t.value, SharedString::from(t.label.clone())))
            .collect();

        // Clone for canvas closure.
        let decimated_canvas = decimated.clone();
        let palette_canvas = palette.clone();
        let y_ticks_canvas = effective_y_ticks;
        let hover_x_canvas = hover_x;
        let y_tick_labels_canvas = y_tick_labels.clone();
        let hidden_canvas = self.hidden.clone();
        let kind_canvas = kind;
        let bar_x_inset_canvas = bar_x_inset_fraction;

        // Shared plot-area bounds: written by the canvas prepaint closure,
        // read here to compute the readout and inside the on_mouse_move
        // listener (next render after the cursor moves).
        let plot_bounds_rc = self.plot_bounds.clone();
        let plot_bounds_for_canvas = plot_bounds_rc.clone();

        // Derive the readout (series label + formatted X/Y) from the bounds
        // captured during the previous paint. `None` until the first paint
        // has run or while the cursor is outside the plot area.
        let readout = build_readout(
            hover_x,
            plot_bounds_rc.borrow().as_ref(),
            &decimated,
            &palette,
            focused_idx,
            &self.hidden,
            spec,
            x_min,
            x_range,
            x_is_time,
        );

        // The legend is now rendered by DataGridPanel as a sibling row below the canvas
        // (via render_chart_legend_row). ChartView no longer owns the legend element —
        // this simplifies the toggle wiring and keeps DataGridPanel as the source of
        // truth for chart_hidden_series.
        //
        // The `spec.legend_visible` field is preserved for future use / API compat.
        let legend: Option<AnyElement> = None;

        // Resolve per-theme chrome colors for the readout overlay.
        let readout_colors = ChartColors::for_current(cx);

        let plot_bounds_for_hover = plot_bounds_rc.clone();

        div()
            .flex()
            .flex_col()
            .size_full()
            .overflow_hidden()
            .child(
                div()
                    .flex_grow()
                    .relative()
                    .overflow_hidden()
                    .on_mouse_move(cx.listener(
                        move |this, ev: &gpui::MouseMoveEvent, _window, cx| {
                            let inside = plot_bounds_for_hover
                                .borrow()
                                .as_ref()
                                .map(|b| b.contains(&ev.position))
                                .unwrap_or(false);
                            let had_hover = this.hover_x_screen.is_some();
                            if inside {
                                this.hover_x_screen = Some(ev.position.x);
                                this.hover_y_screen = Some(ev.position.y);
                                this.update_focused_from_hover();
                                cx.notify();
                            } else if had_hover {
                                this.hover_x_screen = None;
                                this.hover_y_screen = None;
                                cx.notify();
                            }
                        },
                    ))
                    .child({
                        canvas(
                            move |bounds, _window, _cx| {
                                // Store the full canvas bounds; plot bounds are derived
                                // inside the paint closure using the same margin constants.
                                *plot_bounds_for_canvas.borrow_mut() = Some(gpui::Bounds {
                                    origin: point(
                                        bounds.origin.x + gpui::px(MARGIN_LEFT),
                                        bounds.origin.y,
                                    ),
                                    size: gpui::Size {
                                        width: bounds.size.width - gpui::px(MARGIN_LEFT),
                                        height: bounds.size.height,
                                    },
                                });
                                bounds
                            },
                            move |_bounds, bounds_data, window, cx| {
                                let b = bounds_data;
                                let w = f32::from(b.size.width);
                                let h = f32::from(b.size.height);
                                let ox = f32::from(b.origin.x);
                                let oy = f32::from(b.origin.y);

                                // Resolve theme tokens once at the start of each paint pass.
                                let theme = cx.theme();

                                // Plot area occupies the right portion of the canvas;
                                // the left MARGIN_LEFT is reserved for Y-axis tick labels.
                                let plot_x0 = ox + MARGIN_LEFT;
                                let plot_y0 = oy + MARGIN_TOP;
                                let plot_w = (w - MARGIN_LEFT - MARGIN_RIGHT).max(1.0);
                                let plot_h = (h - MARGIN_TOP - MARGIN_BOTTOM).max(1.0);

                                // Bars inset both edges by half a column so the
                                // first/last bar fits; Line uses the full width
                                // (inset fraction is 0).
                                let x_pad = plot_w * bar_x_inset_canvas;
                                let usable_w = (plot_w - 2.0 * x_pad).max(1.0);
                                let data_to_screen_x = |dx: f64| -> f32 {
                                    plot_x0
                                        + x_pad
                                        + ((dx - x_min) / x_range * usable_w as f64) as f32
                                };
                                // Use the shared projection helper so this closure and
                                // the hover hit-test always apply the same Y transform.
                                let data_to_screen_y = |dy: f64| -> f32 {
                                    project_y_to_screen(
                                        dy,
                                        plot_y0,
                                        plot_h,
                                        y_log_min,
                                        y_log_range,
                                        y_is_log,
                                    )
                                };

                                // Dynamic X tick density: roughly one tick per 120px,
                                // clamped to a sane range so very narrow or very wide
                                // charts stay legible.
                                let x_tick_target =
                                    ((plot_w / 120.0).round() as usize).clamp(4, 16);
                                let x_ticks_dynamic = if x_is_time {
                                    ticks_time(x_min, x_max, x_tick_target)
                                } else {
                                    ticks_numeric(x_min, x_max, x_tick_target)
                                };

                                // --- Horizontal gridlines at each Y tick ---
                                // Pie has no axes — skip all gridlines and tick labels.
                                // `tick.value` is in projection space (log1p or linear);
                                // use the projection-space mapper, not data_to_screen_y.
                                let gridline_color = theme.border;
                                for tick in y_ticks_canvas
                                    .iter()
                                    .filter(|_| !matches!(kind_canvas, ChartKind::Pie))
                                {
                                    let sy = project_y_proj_space_to_screen(
                                        tick.value,
                                        plot_y0,
                                        plot_h,
                                        y_log_min,
                                        y_log_range,
                                    );
                                    window.paint_quad(fill(
                                        gpui::Bounds {
                                            origin: point(gpui::px(plot_x0), gpui::px(sy - 0.5)),
                                            size: gpui::Size {
                                                width: gpui::px(plot_w),
                                                height: gpui::px(1.0),
                                            },
                                        },
                                        gridline_color,
                                    ));
                                }

                                // --- Vertical gridlines at each X tick ---
                                for tick in x_ticks_dynamic
                                    .iter()
                                    .filter(|_| !matches!(kind_canvas, ChartKind::Pie))
                                {
                                    let sx = data_to_screen_x(tick.value);
                                    window.paint_quad(fill(
                                        gpui::Bounds {
                                            origin: point(gpui::px(sx - 0.5), gpui::px(plot_y0)),
                                            size: gpui::Size {
                                                width: gpui::px(1.0),
                                                height: gpui::px(plot_h),
                                            },
                                        },
                                        gridline_color,
                                    ));
                                }

                                // --- Series painting ---
                                //
                                // Line and Bar share the plot frame painted above
                                // and differ only in per-series geometry: Line draws
                                // polylines (focused series composited on top); Bar
                                // draws grouped vertical bars anchored at the zero
                                // baseline.
                                match kind_canvas {
                                    ChartKind::Line => {
                                        // Pass 1: non-focused series below the focused
                                        // line. Pass 2: focused series composited on top.
                                        let paint_series =
                                            |pts: &[(f64, f64)],
                                             color: Hsla,
                                             stroke_w: f32,
                                             window: &mut Window| {
                                                if pts.is_empty() {
                                                    return;
                                                }
                                                if pts.len() == 1 {
                                                    // Single-point fallback: paint a square
                                                    // whose side scales with stroke width.
                                                    let half = stroke_w * 1.5;
                                                    let sx = data_to_screen_x(pts[0].0);
                                                    let sy = data_to_screen_y(pts[0].1);
                                                    window.paint_quad(fill(
                                                        gpui::Bounds {
                                                            origin: point(
                                                                gpui::px(sx - half),
                                                                gpui::px(sy - half),
                                                            ),
                                                            size: gpui::Size {
                                                                width: gpui::px(half * 2.0),
                                                                height: gpui::px(half * 2.0),
                                                            },
                                                        },
                                                        color,
                                                    ));
                                                } else {
                                                    let mut builder =
                                                        PathBuilder::stroke(gpui::px(stroke_w));
                                                    let (x0, y0) = pts[0];
                                                    builder.move_to(point(
                                                        gpui::px(data_to_screen_x(x0)),
                                                        gpui::px(data_to_screen_y(y0)),
                                                    ));
                                                    for &(x, y) in pts.iter().skip(1) {
                                                        builder.line_to(point(
                                                            gpui::px(data_to_screen_x(x)),
                                                            gpui::px(data_to_screen_y(y)),
                                                        ));
                                                    }
                                                    if let Ok(path) = builder.build() {
                                                        window.paint_path(path, color);
                                                    }
                                                }
                                            };

                                        // Pass 1 — non-focused, non-hidden series at 1.4 px.
                                        for (s_idx, pts) in decimated_canvas.iter().enumerate() {
                                            if s_idx == focused_idx
                                                || hidden_canvas.contains(&s_idx)
                                            {
                                                continue;
                                            }
                                            let color = palette_canvas
                                                .get(s_idx)
                                                .copied()
                                                .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));
                                            paint_series(pts, color, 1.4, window);
                                        }

                                        // Pass 2 — focused series at 2.2 px on top.
                                        if !hidden_canvas.contains(&focused_idx)
                                            && let Some(pts) = decimated_canvas.get(focused_idx)
                                        {
                                            let color = palette_canvas
                                                .get(focused_idx)
                                                .copied()
                                                .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));
                                            paint_series(pts, color, 2.2, window);
                                        }
                                    }
                                    ChartKind::Bar => {
                                        paint_bars(
                                            window,
                                            &decimated_canvas,
                                            &palette_canvas,
                                            &hidden_canvas,
                                            focused_idx,
                                            plot_w,
                                            y_min,
                                            y_max,
                                            &data_to_screen_x,
                                            &data_to_screen_y,
                                        );
                                    }
                                    ChartKind::Scatter => {
                                        paint_scatter(
                                            window,
                                            &decimated_canvas,
                                            &palette_canvas,
                                            &hidden_canvas,
                                            focused_idx,
                                            &data_to_screen_x,
                                            &data_to_screen_y,
                                        );
                                    }
                                    ChartKind::Area => {
                                        paint_area(
                                            window,
                                            &decimated_canvas,
                                            &palette_canvas,
                                            &hidden_canvas,
                                            focused_idx,
                                            y_min,
                                            y_max,
                                            &data_to_screen_x,
                                            &data_to_screen_y,
                                        );
                                    }
                                    ChartKind::StackedBar => {
                                        paint_stacked_bars(
                                            window,
                                            &decimated_canvas,
                                            &palette_canvas,
                                            &hidden_canvas,
                                            focused_idx,
                                            plot_w,
                                            y_min,
                                            y_max,
                                            &data_to_screen_x,
                                            &data_to_screen_y,
                                        );
                                    }
                                    ChartKind::Pie => {
                                        paint_pie(
                                            window,
                                            &decimated_canvas,
                                            &palette_canvas,
                                            &hidden_canvas,
                                            focused_idx,
                                            plot_x0,
                                            plot_y0,
                                            plot_w,
                                            plot_h,
                                        );
                                    }
                                }

                                // --- Crosshair and hover dots ---
                                //
                                // Pie has no X/Y axes, so crosshair and readout are skipped.
                                // StackedBar and Bar show the crosshair but not hover dots.
                                // Line and Area show both.
                                if let Some(hx) = hover_x_canvas
                                    .filter(|_| !matches!(kind_canvas, ChartKind::Pie))
                                {
                                    let sx = f32::from(hx);
                                    if sx >= plot_x0 && sx <= plot_x0 + plot_w {
                                        let crosshair_color = Hsla {
                                            a: 0.7,
                                            ..theme.primary
                                        };
                                        paint_dashed_vline(
                                            window,
                                            sx,
                                            plot_y0,
                                            plot_y0 + plot_h,
                                            crosshair_color,
                                            2.0,
                                            3.0,
                                        );

                                        // --- Hover dots per series (Line only) ---
                                        // Two-pass: fill background first, then stroke series color.
                                        // Bar charts rely on the crosshair plus the readout
                                        // overlay; per-point dots would float off the bars.
                                        let cursor_data_x = x_min
                                            + ((sx - plot_x0) as f64 / plot_w as f64) * x_range;

                                        for (s_idx, pts) in decimated_canvas.iter().enumerate() {
                                            // Hover dots are shown for Line and Area only.
                                            // Bar, StackedBar, and Scatter skip them:
                                            // bar kinds rely on the crosshair + readout overlay;
                                            // Scatter paints discrete points that are already
                                            // their own indicators.
                                            if !matches!(
                                                kind_canvas,
                                                ChartKind::Line | ChartKind::Area
                                            ) {
                                                break;
                                            }
                                            if hidden_canvas.contains(&s_idx) {
                                                continue;
                                            }

                                            let Some(y_data) =
                                                crate::chart::stats::interpolate_y_at_x(
                                                    pts,
                                                    cursor_data_x,
                                                )
                                            else {
                                                continue;
                                            };

                                            let dot_sx = sx;
                                            let dot_sy = data_to_screen_y(y_data);
                                            let series_color = palette_canvas
                                                .get(s_idx)
                                                .copied()
                                                .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));

                                            // Filled background disk + stroked series-color ring.
                                            // Built from two SVG-style arcs (top + bottom semicircle)
                                            // because GPUI's PathBuilder has no first-class circle.
                                            let r = 3.5_f32;
                                            let radii = point(gpui::px(r), gpui::px(r));
                                            let right =
                                                point(gpui::px(dot_sx + r), gpui::px(dot_sy));
                                            let left =
                                                point(gpui::px(dot_sx - r), gpui::px(dot_sy));

                                            let mut fill_builder = PathBuilder::fill();
                                            fill_builder.move_to(right);
                                            fill_builder.arc_to(
                                                radii,
                                                gpui::px(0.0),
                                                false,
                                                true,
                                                left,
                                            );
                                            fill_builder.arc_to(
                                                radii,
                                                gpui::px(0.0),
                                                false,
                                                true,
                                                right,
                                            );
                                            fill_builder.close();
                                            if let Ok(path) = fill_builder.build() {
                                                window.paint_path(path, theme.background);
                                            }

                                            let mut stroke_builder =
                                                PathBuilder::stroke(gpui::px(1.5));
                                            stroke_builder.move_to(right);
                                            stroke_builder.arc_to(
                                                radii,
                                                gpui::px(0.0),
                                                false,
                                                true,
                                                left,
                                            );
                                            stroke_builder.arc_to(
                                                radii,
                                                gpui::px(0.0),
                                                false,
                                                true,
                                                right,
                                            );
                                            stroke_builder.close();
                                            if let Ok(path) = stroke_builder.build() {
                                                window.paint_path(path, series_color);
                                            }
                                        }
                                    }
                                }

                                // --- In-canvas Y-axis tick labels ---
                                // Right-aligned in the MARGIN_LEFT column.
                                // Pie has no axes — skip all tick labels.
                                let tick_color = theme.muted_foreground;
                                let tick_font = font("Zed Mono");
                                let tick_size = gpui::px(10.0);
                                let line_height = gpui::px(12.0);

                                for (value, label) in y_tick_labels_canvas
                                    .iter()
                                    .filter(|_| !matches!(kind_canvas, ChartKind::Pie))
                                {
                                    // `value` is in projection space (log1p or linear).
                                    let sy = project_y_proj_space_to_screen(
                                        *value,
                                        plot_y0,
                                        plot_h,
                                        y_log_min,
                                        y_log_range,
                                    );
                                    let run = TextRun {
                                        len: label.len(),
                                        font: tick_font.clone(),
                                        color: tick_color,
                                        background_color: None,
                                        underline: None,
                                        strikethrough: None,
                                    };
                                    let shaped = window.text_system().shape_line(
                                        label.clone(),
                                        tick_size,
                                        &[run],
                                        None,
                                    );
                                    let label_w = f32::from(shaped.width);
                                    // Right-align within MARGIN_LEFT minus 4px padding.
                                    let label_x = ox + (MARGIN_LEFT - 4.0) - label_w;
                                    let label_y = sy - f32::from(line_height) / 2.0;
                                    let _ = shaped.paint(
                                        point(gpui::px(label_x), gpui::px(label_y)),
                                        line_height,
                                        window,
                                        cx,
                                    );
                                }

                                // --- In-canvas X-axis tick labels ---
                                // Centered below each tick, in the MARGIN_BOTTOM band.
                                let x_baseline_y = plot_y0 + plot_h + 10.0;

                                for tick in x_ticks_dynamic
                                    .iter()
                                    .filter(|_| !matches!(kind_canvas, ChartKind::Pie))
                                {
                                    let value = tick.value;
                                    let label = SharedString::from(tick.label.clone());
                                    let sx = data_to_screen_x(value);
                                    let run = TextRun {
                                        len: label.len(),
                                        font: tick_font.clone(),
                                        color: tick_color,
                                        background_color: None,
                                        underline: None,
                                        strikethrough: None,
                                    };
                                    let shaped = window.text_system().shape_line(
                                        label.clone(),
                                        tick_size,
                                        &[run],
                                        None,
                                    );
                                    let label_w = f32::from(shaped.width);
                                    let label_x = sx - label_w / 2.0;
                                    let _ = shaped.paint(
                                        point(gpui::px(label_x), gpui::px(x_baseline_y)),
                                        line_height,
                                        window,
                                        cx,
                                    );
                                }
                            },
                        )
                        .absolute()
                        .size_full()
                    })
                    .when_some(readout, |container, r| {
                        container.child(readout_overlay(r, readout_colors))
                    }),
            )
            // Legend row (below canvas)
            .when_some(legend, |d, leg| d.child(leg))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Paint grouped vertical bars for every visible series.
///
/// Each X position owns a slot whose width derives from the densest series so
/// bars stay inside their column even when series have differing point counts.
/// Within a slot, visible series are laid out side by side (grouped, never
/// overlapping). Bars are anchored at the zero baseline when zero lies inside
/// the value range, otherwise at the data minimum so a positive-only series
/// still grows from the axis floor.
///
/// Non-focused series are dimmed when more than one series is visible, mirroring
/// the emphasis the Line arm gives the focused polyline.
#[allow(clippy::too_many_arguments)]
fn paint_bars<FX, FY>(
    window: &mut Window,
    decimated: &[Vec<(f64, f64)>],
    palette: &[Hsla],
    hidden: &HashSet<usize>,
    focused_idx: usize,
    plot_w: f32,
    y_min: f64,
    y_max: f64,
    data_to_screen_x: &FX,
    data_to_screen_y: &FY,
) where
    FX: Fn(f64) -> f32,
    FY: Fn(f64) -> f32,
{
    let baseline = if y_min <= 0.0 && y_max >= 0.0 {
        0.0
    } else {
        y_min
    };
    let baseline_sy = data_to_screen_y(baseline);

    let visible: Vec<usize> = (0..decimated.len())
        .filter(|i| !hidden.contains(i))
        .collect();
    let num_visible = visible.len().max(1);

    let max_points = decimated.iter().map(|s| s.len()).max().unwrap_or(1).max(1);

    let slot_w = plot_w / max_points as f32;
    let group_w = slot_w * 0.8;
    let bar_w = (group_w / num_visible as f32).max(1.0);

    let fallback = gpui::hsla(0.6, 0.6, 0.5, 1.0);

    for (group_pos, &s_idx) in visible.iter().enumerate() {
        let base_color = palette.get(s_idx).copied().unwrap_or(fallback);
        let color = if num_visible > 1 && s_idx != focused_idx {
            gpui::hsla(base_color.h, base_color.s, base_color.l, 0.55)
        } else {
            base_color
        };

        let offset = group_pos as f32 * bar_w - group_w / 2.0;

        for &(x, y) in &decimated[s_idx] {
            let bar_left = data_to_screen_x(x) + offset;
            let value_sy = data_to_screen_y(y);

            let (rect_top, rect_height) = if value_sy <= baseline_sy {
                (value_sy, baseline_sy - value_sy)
            } else {
                (baseline_sy, value_sy - baseline_sy)
            };

            window.paint_quad(fill(
                gpui::Bounds {
                    origin: point(gpui::px(bar_left), gpui::px(rect_top)),
                    size: gpui::Size {
                        width: gpui::px(bar_w * 0.92),
                        height: gpui::px(rect_height.max(1.0)),
                    },
                },
                color,
            ));
        }
    }
}

/// Paint stacked vertical bars for every visible series.
///
/// Unlike `paint_bars` (which groups series side-by-side), stacked bars pile
/// series on top of each other at each X position. The visual footprint per
/// X slot is a single full-width bar column, with each series' segment sitting
/// on the cumulative total of the series below it.
///
/// The Y axis **must** have already been rescaled to the maximum stack sum
/// before calling this function — `render()` does this for `ChartKind::StackedBar`.
///
/// Series with mismatched lengths are handled safely: iteration stops at the
/// shortest series at each point index.
#[allow(clippy::too_many_arguments)]
fn paint_stacked_bars<FX, FY>(
    window: &mut Window,
    decimated: &[Vec<(f64, f64)>],
    palette: &[Hsla],
    hidden: &HashSet<usize>,
    focused_idx: usize,
    plot_w: f32,
    y_min: f64,
    y_max: f64,
    data_to_screen_x: &FX,
    data_to_screen_y: &FY,
) where
    FX: Fn(f64) -> f32,
    FY: Fn(f64) -> f32,
{
    let baseline = if y_min <= 0.0 && y_max >= 0.0 {
        0.0
    } else {
        y_min
    };

    let visible: Vec<usize> = (0..decimated.len())
        .filter(|i| !hidden.contains(i))
        .collect();
    let num_visible = visible.len();
    if num_visible == 0 {
        return;
    }

    // Bar width: one slot per X position (single full-width column per x,
    // since the series stack rather than sit side-by-side).
    let max_points = decimated.iter().map(|s| s.len()).max().unwrap_or(1).max(1);

    let slot_w = plot_w / max_points as f32;
    let bar_w = (slot_w * 0.8).max(1.0);

    let fallback = gpui::hsla(0.6, 0.6, 0.5, 1.0);

    // Iterate over x positions using the first visible series as the anchor.
    // For each x position, collect the y values from all visible series in order.
    let anchor_series = visible[0];
    let n_points = decimated[anchor_series].len();

    for pt_idx in 0..n_points {
        let (x, _) = decimated[anchor_series][pt_idx];
        let bar_center_sx = data_to_screen_x(x);
        let bar_left = bar_center_sx - bar_w / 2.0;

        // Accumulate from the baseline upward, one segment per visible series.
        let mut cumulative = baseline;

        for &s_idx in &visible {
            let Some(&(_, y)) = decimated[s_idx].get(pt_idx) else {
                // This series has fewer points — skip remaining series for this slot.
                break;
            };

            let seg_bottom_sy = data_to_screen_y(cumulative);
            let seg_top_sy = data_to_screen_y(cumulative + y);
            cumulative += y;

            let (rect_top, rect_h) = if seg_top_sy <= seg_bottom_sy {
                (seg_top_sy, seg_bottom_sy - seg_top_sy)
            } else {
                (seg_bottom_sy, seg_top_sy - seg_bottom_sy)
            };

            let base_color = palette.get(s_idx).copied().unwrap_or(fallback);
            let color = if num_visible > 1 && s_idx != focused_idx {
                gpui::hsla(base_color.h, base_color.s, base_color.l, 0.55)
            } else {
                base_color
            };

            window.paint_quad(fill(
                gpui::Bounds {
                    origin: point(gpui::px(bar_left), gpui::px(rect_top)),
                    size: gpui::Size {
                        width: gpui::px(bar_w),
                        height: gpui::px(rect_h.max(1.0)),
                    },
                },
                color,
            ));
        }
    }
}

/// Paint a pie chart: one wedge per visible series, sized proportional to the
/// sum of that series' Y values.
///
/// Wedges are drawn by subdividing each arc into small line segments (~2°
/// steps) to avoid pitfalls with GPUI's arc_to large-arc handling. The focused
/// series is drawn at full opacity and slightly larger radius; non-focused
/// slices are slightly dimmed.
///
/// When all visible series totals are ≤ 0, nothing is painted.
#[allow(clippy::too_many_arguments)]
fn paint_pie(
    window: &mut Window,
    decimated: &[Vec<(f64, f64)>],
    palette: &[Hsla],
    hidden: &HashSet<usize>,
    focused_idx: usize,
    plot_x0: f32,
    plot_y0: f32,
    plot_w: f32,
    plot_h: f32,
) {
    let visible: Vec<usize> = (0..decimated.len())
        .filter(|i| !hidden.contains(i))
        .collect();

    // Sum each visible series; skip series with non-positive totals.
    let totals: Vec<(usize, f64)> = visible
        .iter()
        .filter_map(|&s_idx| {
            let total: f64 = decimated[s_idx]
                .iter()
                .map(|(_, y)| *y)
                .filter(|y| y.is_finite())
                .sum();
            if total > 0.0 {
                Some((s_idx, total))
            } else {
                None
            }
        })
        .collect();

    if totals.is_empty() {
        return;
    }

    let grand_total: f64 = totals.iter().map(|(_, t)| t).sum();
    if grand_total <= 0.0 {
        return;
    }

    let cx = plot_x0 + plot_w / 2.0;
    let cy = plot_y0 + plot_h / 2.0;
    let base_radius = (plot_w.min(plot_h) * 0.4).max(1.0);

    // Each slice spans [start_angle, end_angle] in radians (0 = right, CCW).
    let fallback = gpui::hsla(0.6, 0.6, 0.5, 1.0);
    let mut start_angle: f64 = -std::f64::consts::FRAC_PI_2; // Start from the top.

    for &(s_idx, total) in &totals {
        let fraction = total / grand_total;
        let sweep = fraction * 2.0 * std::f64::consts::PI;
        let end_angle = start_angle + sweep;

        let is_focused = s_idx == focused_idx;
        let radius = if is_focused {
            base_radius * 1.04
        } else {
            base_radius
        };
        let base_color = palette.get(s_idx).copied().unwrap_or(fallback);
        let alpha = if is_focused { 1.0_f32 } else { 0.75_f32 };
        let color = gpui::hsla(base_color.h, base_color.s, base_color.l, alpha);

        // Subdivide the arc into 2-degree segments to avoid large-arc pitfalls.
        const STEP: f64 = 2.0 * std::f64::consts::PI / 180.0; // 2 degrees

        let mut builder = PathBuilder::fill();

        // Start from the centre and trace the wedge outline.
        builder.move_to(point(gpui::px(cx), gpui::px(cy)));

        let mut angle = start_angle;
        let first_x = cx + (radius as f64 * angle.cos()) as f32;
        let first_y = cy + (radius as f64 * angle.sin()) as f32;
        builder.line_to(point(gpui::px(first_x), gpui::px(first_y)));

        // Trace the arc rim by small straight segments.
        while angle < end_angle - STEP * 0.5 {
            angle = (angle + STEP).min(end_angle);
            let rim_x = cx + (radius as f64 * angle.cos()) as f32;
            let rim_y = cy + (radius as f64 * angle.sin()) as f32;
            builder.line_to(point(gpui::px(rim_x), gpui::px(rim_y)));
        }

        // Ensure the final point lands exactly on end_angle.
        let last_x = cx + (radius as f64 * end_angle.cos()) as f32;
        let last_y = cy + (radius as f64 * end_angle.sin()) as f32;
        builder.line_to(point(gpui::px(last_x), gpui::px(last_y)));

        builder.close();

        if let Ok(path) = builder.build() {
            window.paint_path(path, color);
        }

        start_angle = end_angle;
    }
}

/// Paint a filled disk at `(cx, cy)` with radius `r`.
///
/// GPUI's `PathBuilder` has no first-class circle, so the disk is built from two
/// half-arcs (top and bottom semicircles).
fn paint_filled_circle(window: &mut Window, cx: f32, cy: f32, r: f32, color: Hsla) {
    let radii = point(gpui::px(r), gpui::px(r));
    let right = point(gpui::px(cx + r), gpui::px(cy));
    let left = point(gpui::px(cx - r), gpui::px(cy));

    let mut builder = PathBuilder::fill();
    builder.move_to(right);
    builder.arc_to(radii, gpui::px(0.0), false, true, left);
    builder.arc_to(radii, gpui::px(0.0), false, true, right);
    builder.close();

    if let Ok(path) = builder.build() {
        window.paint_path(path, color);
    }
}

/// Paint every visible series as a cloud of discrete points (no connecting
/// line). The focused series is drawn at full opacity with a slightly larger
/// radius; non-focused series are dimmed and smaller, mirroring the emphasis
/// the Line and Bar arms give the focused series.
fn paint_scatter<FX, FY>(
    window: &mut Window,
    decimated: &[Vec<(f64, f64)>],
    palette: &[Hsla],
    hidden: &HashSet<usize>,
    focused_idx: usize,
    data_to_screen_x: &FX,
    data_to_screen_y: &FY,
) where
    FX: Fn(f64) -> f32,
    FY: Fn(f64) -> f32,
{
    let fallback = gpui::hsla(0.6, 0.6, 0.5, 1.0);

    let visible: Vec<usize> = (0..decimated.len())
        .filter(|i| !hidden.contains(i))
        .collect();
    let num_visible = visible.len();

    for &s_idx in &visible {
        let base_color = palette.get(s_idx).copied().unwrap_or(fallback);
        let (color, radius) = if num_visible > 1 && s_idx != focused_idx {
            (
                gpui::hsla(base_color.h, base_color.s, base_color.l, 0.45),
                2.5_f32,
            )
        } else {
            (base_color, 3.5_f32)
        };

        for &(x, y) in &decimated[s_idx] {
            let sx = data_to_screen_x(x);
            let sy = data_to_screen_y(y);
            paint_filled_circle(window, sx, sy, radius, color);
        }
    }
}

/// Paint every visible series as a filled area chart.
///
/// Each series is drawn in two passes to achieve the stacked visual effect of
/// fill behind a stroke line:
///
/// 1. **Fill pass**: a closed path from the first-point baseline, through all
///    data points, back down to the last-point baseline, filled with the series
///    colour at low alpha. Non-focused series use a lower alpha (~0.12) so they
///    recede behind the focused series (~0.22).
/// 2. **Stroke pass**: the data-point polyline only (no baseline edges), at
///    1.6 px for non-focused and 2.2 px for the focused series.
///
/// The baseline follows the same rule as `paint_bars`: `y = 0.0` when zero
/// falls inside `[y_min, y_max]`, otherwise `y = y_min`.
///
/// Single-point series are handled gracefully — the fill degenerates to a
/// vertical line segment and the stroke paints a square marker, matching the
/// Line arm's single-point fallback.
#[allow(clippy::too_many_arguments)]
fn paint_area<FX, FY>(
    window: &mut Window,
    decimated: &[Vec<(f64, f64)>],
    palette: &[Hsla],
    hidden: &HashSet<usize>,
    focused_idx: usize,
    y_min: f64,
    y_max: f64,
    data_to_screen_x: &FX,
    data_to_screen_y: &FY,
) where
    FX: Fn(f64) -> f32,
    FY: Fn(f64) -> f32,
{
    let baseline = if y_min <= 0.0 && y_max >= 0.0 {
        0.0
    } else {
        y_min
    };
    let baseline_sy = data_to_screen_y(baseline);

    let fallback = gpui::hsla(0.6, 0.6, 0.5, 1.0);

    let visible: Vec<usize> = (0..decimated.len())
        .filter(|i| !hidden.contains(i))
        .collect();
    let num_visible = visible.len();

    // Two passes: non-focused first so the focused series composites on top.
    for pass in 0..2usize {
        for &s_idx in &visible {
            let is_focused = s_idx == focused_idx;
            if pass == 0 && is_focused {
                continue;
            }
            if pass == 1 && !is_focused {
                continue;
            }

            let pts = &decimated[s_idx];
            if pts.is_empty() {
                continue;
            }

            let base_color = palette.get(s_idx).copied().unwrap_or(fallback);

            // --- Fill pass ---
            let fill_alpha = if num_visible > 1 && !is_focused {
                0.12
            } else {
                0.22
            };
            let fill_color = gpui::hsla(base_color.h, base_color.s, base_color.l, fill_alpha);

            if pts.len() == 1 {
                // Single-point: fill a thin vertical rect from the data point to the baseline.
                let sx = data_to_screen_x(pts[0].0);
                let sy = data_to_screen_y(pts[0].1);
                let (rect_top, rect_h) = if sy <= baseline_sy {
                    (sy, baseline_sy - sy)
                } else {
                    (baseline_sy, sy - baseline_sy)
                };
                window.paint_quad(fill(
                    gpui::Bounds {
                        origin: point(gpui::px(sx - 1.0), gpui::px(rect_top)),
                        size: gpui::Size {
                            width: gpui::px(2.0),
                            height: gpui::px(rect_h.max(1.0)),
                        },
                    },
                    fill_color,
                ));
            } else {
                // Build a closed filled path: baseline→first, data points, last→baseline.
                let (x0, _) = pts[0];
                let (xn, _) = pts[pts.len() - 1];

                let mut builder = PathBuilder::fill();
                builder.move_to(point(gpui::px(data_to_screen_x(x0)), gpui::px(baseline_sy)));
                for &(x, y) in pts {
                    builder.line_to(point(
                        gpui::px(data_to_screen_x(x)),
                        gpui::px(data_to_screen_y(y)),
                    ));
                }
                builder.line_to(point(gpui::px(data_to_screen_x(xn)), gpui::px(baseline_sy)));
                builder.close();
                if let Ok(path) = builder.build() {
                    window.paint_path(path, fill_color);
                }
            }

            // --- Stroke pass (data-line only, no baseline edges) ---
            let stroke_w = if num_visible > 1 && !is_focused {
                1.6_f32
            } else {
                2.2_f32
            };
            let stroke_alpha = if num_visible > 1 && !is_focused {
                0.6_f32
            } else {
                1.0_f32
            };
            let stroke_color = gpui::hsla(base_color.h, base_color.s, base_color.l, stroke_alpha);

            if pts.len() == 1 {
                // Single-point fallback: a square marker, same as the Line arm.
                let half = stroke_w * 1.5;
                let sx = data_to_screen_x(pts[0].0);
                let sy = data_to_screen_y(pts[0].1);
                window.paint_quad(fill(
                    gpui::Bounds {
                        origin: point(gpui::px(sx - half), gpui::px(sy - half)),
                        size: gpui::Size {
                            width: gpui::px(half * 2.0),
                            height: gpui::px(half * 2.0),
                        },
                    },
                    stroke_color,
                ));
            } else {
                let mut builder = PathBuilder::stroke(gpui::px(stroke_w));
                let (x0, y0) = pts[0];
                builder.move_to(point(
                    gpui::px(data_to_screen_x(x0)),
                    gpui::px(data_to_screen_y(y0)),
                ));
                for &(x, y) in pts.iter().skip(1) {
                    builder.line_to(point(
                        gpui::px(data_to_screen_x(x)),
                        gpui::px(data_to_screen_y(y)),
                    ));
                }
                if let Ok(path) = builder.build() {
                    window.paint_path(path, stroke_color);
                }
            }
        }
    }
}

/// Paint a vertical dashed line using short filled quads.
///
/// `dash` is the length of each segment; `gap` is the space between segments.
fn paint_dashed_vline(
    window: &mut Window,
    x: f32,
    y0: f32,
    y1: f32,
    color: Hsla,
    dash: f32,
    gap: f32,
) {
    let mut y = y0;
    while y < y1 {
        let seg_end = (y + dash).min(y1);
        window.paint_quad(fill(
            gpui::Bounds {
                origin: point(gpui::px(x - 0.5), gpui::px(y)),
                size: gpui::Size {
                    width: gpui::px(1.0),
                    height: gpui::px(seg_end - y),
                },
            },
            color,
        ));
        y = seg_end + gap;
    }
}

/// Extract an f64 value from a `Value`, treating timestamps as ms-since-epoch.
fn extract_f64(value: &Value, is_time: bool) -> Option<f64> {
    match value {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => {
            if f.is_finite() {
                Some(*f)
            } else {
                None
            }
        }
        // Decimal is stored as a string to preserve exact precision; for
        // plotting we accept the f64 lossy parse and drop non-finite results.
        // Drivers that classify NUMERIC/DECIMAL columns as ColumnKind::Float
        // (e.g. Postgres NUMERIC, MSSQL DECIMAL) emit values through this arm.
        Value::Decimal(s) => s.parse::<f64>().ok().filter(|f| f.is_finite()),
        // BIT / BOOLEAN columns get classified as Integer by some drivers
        // (e.g. MSSQL BIT). Map true → 1.0, false → 0.0 so the series is
        // plottable instead of silently empty.
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        Value::Text(s) if is_time => {
            // Try parsing as RFC 3339 timestamp.
            if let Ok(dt) = dbflux_core::chrono::DateTime::parse_from_rfc3339(s) {
                Some(dt.timestamp_millis() as f64)
            } else {
                None
            }
        }
        Value::Null => None,
        _ => None,
    }
}

/// Emit a single debug log when time-series data arrives non-monotonically.
fn tracing_debug_non_monotonic() {
    #[cfg(not(test))]
    log::debug!("[chart] X values are non-monotonic — sorted before rendering");
}

// ---------------------------------------------------------------------------
// Hover readout
// ---------------------------------------------------------------------------

/// One row of the multi-series readout panel.
/// One row in the crosshair readout (one per visible series).
struct SeriesReadoutEntry {
    label: SharedString,
    color: Hsla,
    y_label: SharedString,
}

/// Pre-computed content for the crosshair readout panel.
struct HoverReadout {
    /// Header: e.g. "11:34 UTC"
    header_time: SharedString,
    /// Optional time-offset suffix: e.g. " · t+13m" (None when x_min is unavailable)
    header_offset: Option<SharedString>,
    series: Vec<SeriesReadoutEntry>,
    focused_idx: usize,
    /// Cursor X relative to the plot origin (used to position the overlay).
    screen_x_relative: Pixels,
    plot_width: Pixels,
    plot_height: Pixels,
}

/// Derive readout content from the captured plot-area bounds and the
/// last-seen hover X. Returns `None` when bounds are not yet known (no paint
/// has run), when the cursor falls outside the plot area, or when no series
/// has usable samples near the cursor.
#[allow(clippy::too_many_arguments)]
fn build_readout(
    hover_x_window: Option<Pixels>,
    plot_bounds: Option<&Bounds<Pixels>>,
    decimated: &[Vec<(f64, f64)>],
    palette: &[Hsla],
    focused_idx: usize,
    hidden: &HashSet<usize>,
    spec: &ChartSpec,
    x_min: f64,
    x_range: f64,
    x_is_time: bool,
) -> Option<HoverReadout> {
    let hover_x = hover_x_window?;
    let bounds = plot_bounds?;

    let plot_x0 = bounds.origin.x;
    let plot_w_px = f32::from(bounds.size.width) - MARGIN_RIGHT;
    if plot_w_px <= 0.0 {
        return None;
    }
    let plot_w = gpui::px(plot_w_px);
    let plot_h = bounds.size.height;

    let relative_x = hover_x - plot_x0;
    let rel_x_f = f32::from(relative_x);
    if rel_x_f < 0.0 || rel_x_f > plot_w_px {
        return None;
    }

    let cursor_data_x = x_min + (rel_x_f as f64 / plot_w_px as f64) * x_range;

    // Build the time header: "HH:MM UTC" or raw value for non-time axes.
    let header_time = if x_is_time {
        let secs = (cursor_data_x / 1000.0).trunc() as i64;
        let nsecs = ((cursor_data_x.rem_euclid(1000.0)) * 1_000_000.0) as u32;
        match dbflux_core::chrono::DateTime::from_timestamp(secs, nsecs) {
            Some(dt) => SharedString::from(dt.format("%H:%M UTC").to_string()),
            None => SharedString::from(format!("{:.3}", cursor_data_x)),
        }
    } else {
        SharedString::from(format!("{:.3}", cursor_data_x))
    };

    // Optional t+N suffix (whole minutes since x_min).
    let header_offset = if x_is_time {
        let offset_ms = cursor_data_x - x_min;
        let minutes = (offset_ms / 60_000.0).floor() as i64;
        if minutes >= 0 {
            Some(SharedString::from(format!(" · t+{}m", minutes)))
        } else {
            None
        }
    } else {
        None
    };

    // Collect one entry per series.
    let mut entries: Vec<SeriesReadoutEntry> = Vec::with_capacity(decimated.len());
    let mut any_valid = false;

    for (s_idx, pts) in decimated.iter().enumerate() {
        if pts.is_empty() || hidden.contains(&s_idx) {
            continue;
        }
        let (_sample_x, sample_y) = nearest_sample(pts, cursor_data_x);

        let label = spec
            .series
            .get(s_idx)
            .map(|s| s.label.as_str())
            .unwrap_or("");
        let color = palette
            .get(s_idx)
            .copied()
            .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));

        entries.push(SeriesReadoutEntry {
            label: SharedString::from(label.to_string()),
            color,
            y_label: SharedString::from(format_y_value(sample_y)),
        });
        any_valid = true;
    }

    if !any_valid {
        return None;
    }

    Some(HoverReadout {
        header_time,
        header_offset,
        series: entries,
        focused_idx,
        screen_x_relative: relative_x,
        plot_width: plot_w,
        plot_height: plot_h,
    })
}

/// Locate the sample in `points` whose X coordinate is closest to `target_x`.
/// Assumes `points` is sorted by X (the engine sorts during `build`).
fn nearest_sample(points: &[(f64, f64)], target_x: f64) -> (f64, f64) {
    match points.binary_search_by(|p| {
        p.0.partial_cmp(&target_x)
            .unwrap_or(std::cmp::Ordering::Equal)
    }) {
        Ok(idx) => points[idx],
        Err(insert_idx) => {
            if insert_idx == 0 {
                points[0]
            } else if insert_idx >= points.len() {
                points[points.len() - 1]
            } else {
                let lo = points[insert_idx - 1];
                let hi = points[insert_idx];
                if (target_x - lo.0).abs() <= (hi.0 - target_x).abs() {
                    lo
                } else {
                    hi
                }
            }
        }
    }
}

pub fn format_x_value(x: f64, is_time: bool) -> String {
    if is_time {
        let secs = (x / 1000.0).trunc() as i64;
        let nsecs = ((x.rem_euclid(1000.0)) * 1_000_000.0) as u32;
        match dbflux_core::chrono::DateTime::from_timestamp(secs, nsecs) {
            Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            None => format!("{:.3}", x),
        }
    } else {
        format!("{:.3}", x)
    }
}

pub fn format_y_value(y: f64) -> String {
    if y.abs() >= 1000.0 || (y != 0.0 && y.abs() < 0.001) {
        format!("{:.3e}", y)
    } else {
        format!("{:.3}", y)
    }
}

/// Build the absolute-positioned overlay div that shows the multi-series readout.
///
/// Layout: top 18px fixed; left clamped so the panel stays inside the plot area.
/// Min-width 200px; one header row (time + optional offset) then one row per series.
///
/// `colors` is derived from `ChartColors::for_current(cx)` at the render call site
/// so the overlay matches the active theme.
fn readout_overlay(r: HoverReadout, colors: ChartColors) -> impl IntoElement {
    const PANEL_MIN_WIDTH: f32 = 200.0;
    const PANEL_GAP: f32 = 12.0;

    // screen_x_relative is in plot coordinates; add MARGIN_LEFT for the full canvas.
    let hover_x = f32::from(r.screen_x_relative);
    let plot_w = f32::from(r.plot_width);

    // Clamp so the panel never overflows the right edge.
    let left_in_plot = (hover_x + PANEL_GAP).min(plot_w - PANEL_MIN_WIDTH);
    let left_px = (MARGIN_LEFT + left_in_plot).max(MARGIN_LEFT + 4.0);

    let focused_idx = r.focused_idx;
    let max_h_px = (f32::from(r.plot_height) - 24.0).max(80.0);

    div()
        .absolute()
        .left(gpui::px(left_px))
        .top(gpui::px(18.0))
        .min_w(gpui::px(PANEL_MIN_WIDTH))
        .max_h(gpui::px(max_h_px))
        .flex()
        .flex_col()
        .gap(gpui::px(2.0))
        .px(gpui::px(10.0))
        .py(gpui::px(8.0))
        .bg(colors.panel_bg)
        .border_1()
        .border_color(colors.panel_border)
        .rounded(gpui::px(6.0))
        .text_size(FontSizes::XS)
        .overflow_hidden()
        // Header: time + optional offset
        .child(
            div()
                .flex()
                .items_center()
                .text_color(colors.value_fg)
                .font_weight(gpui::FontWeight::SEMIBOLD)
                .child(r.header_time)
                .when_some(r.header_offset, |d, offset| {
                    d.child(div().text_color(colors.label_fg).child(offset))
                }),
        )
        // One row per series; focused row gets semibold.
        .children(r.series.into_iter().enumerate().map(move |(idx, entry)| {
            let is_focused = idx == focused_idx;
            div()
                .flex()
                .items_center()
                .gap(gpui::px(6.0))
                .py(gpui::px(1.0))
                .when(is_focused, |d| d.font_weight(gpui::FontWeight::SEMIBOLD))
                // 10px colour swatch
                .child(div().w(gpui::px(10.0)).h(gpui::px(10.0)).bg(entry.color))
                // Series name (muted, takes remaining space)
                .child(
                    div()
                        .flex_1()
                        .text_color(colors.label_fg)
                        .child(entry.label),
                )
                // Value (foreground)
                .child(div().text_color(colors.value_fg).child(entry.y_label))
        }))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::spec::{AxisKind, AxisSpec, ChartSpec, SeriesSpec};
    use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, Value};
    use std::time::Duration;

    #[test]
    fn nearest_sample_picks_exact_match() {
        let pts = vec![(0.0, 10.0), (1.0, 20.0), (2.0, 30.0)];
        assert_eq!(nearest_sample(&pts, 1.0), (1.0, 20.0));
    }

    #[test]
    fn nearest_sample_clamps_below_min() {
        let pts = vec![(5.0, 1.0), (6.0, 2.0)];
        assert_eq!(nearest_sample(&pts, -100.0), (5.0, 1.0));
    }

    #[test]
    fn nearest_sample_clamps_above_max() {
        let pts = vec![(5.0, 1.0), (6.0, 2.0)];
        assert_eq!(nearest_sample(&pts, 100.0), (6.0, 2.0));
    }

    #[test]
    fn nearest_sample_picks_closer_of_two_neighbours() {
        let pts = vec![(0.0, 1.0), (10.0, 2.0)];
        assert_eq!(nearest_sample(&pts, 3.0), (0.0, 1.0));
        assert_eq!(nearest_sample(&pts, 7.0), (10.0, 2.0));
    }

    #[test]
    fn nearest_sample_ties_to_lower() {
        let pts = vec![(0.0, 1.0), (10.0, 2.0)];
        // Midpoint: implementation prefers the lower-x neighbour on ties.
        assert_eq!(nearest_sample(&pts, 5.0), (0.0, 1.0));
    }

    #[test]
    fn format_x_value_time_formats_unix_ms() {
        let s = format_x_value(0.0, true);
        assert!(s.starts_with("1970-01-01"), "got: {s}");
    }

    #[test]
    fn extract_f64_parses_decimal_string() {
        assert_eq!(
            extract_f64(&Value::Decimal("123.45".into()), false),
            Some(123.45)
        );
        assert_eq!(
            extract_f64(&Value::Decimal("-0.001".into()), false),
            Some(-0.001)
        );
    }

    #[test]
    fn extract_f64_rejects_non_finite_or_unparseable_decimal() {
        assert_eq!(extract_f64(&Value::Decimal("NaN".into()), false), None);
        assert_eq!(extract_f64(&Value::Decimal("inf".into()), false), None);
        assert_eq!(
            extract_f64(&Value::Decimal("not-a-number".into()), false),
            None
        );
    }

    #[test]
    fn extract_f64_maps_bool_to_zero_or_one() {
        assert_eq!(extract_f64(&Value::Bool(true), false), Some(1.0));
        assert_eq!(extract_f64(&Value::Bool(false), false), Some(0.0));
    }

    #[test]
    fn format_y_value_uses_scientific_for_large_magnitudes() {
        assert!(format_y_value(1234.0).contains('e'));
        assert!(format_y_value(0.0001).contains('e'));
        assert_eq!(format_y_value(0.0), "0.000");
        assert_eq!(format_y_value(1.5), "1.500");
    }

    fn make_col(name: &str, kind: ColumnKind) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            type_name: "t".to_string(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    fn simple_spec(x_col: usize, y_cols: &[usize]) -> ChartSpec {
        use crate::chart::spec::{AggKind, BindingSpec};
        ChartSpec {
            kind: crate::chart::spec::ChartKind::Line,
            x_axis: AxisSpec {
                column_index: x_col,
                label: "time".to_string(),
                kind: AxisKind::Time,
                unit: None,
            },
            series: y_cols
                .iter()
                .enumerate()
                .map(|(slot, &col)| SeriesSpec {
                    column_index: col,
                    label: format!("series_{}", slot),
                    color_slot: slot as u8,
                })
                .collect(),
            legend_visible: false,
            decimation_threshold: 10_000,
            binding: BindingSpec {
                x: x_col,
                y: y_cols.to_vec(),
                group_by: None,
                filter: None,
                aggregation: AggKind::None,
            },
            track_source_indices: false,
            y_scale: crate::chart::spec::YScale::Linear,
        }
    }

    #[test]
    fn build_errors_on_empty_result() {
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            vec![],
            None,
            Duration::ZERO,
        );
        let spec = simple_spec(0, &[1]);
        assert!(matches!(
            ChartView::build(&result, spec),
            Err(ChartBuildError::Empty)
        ));
    }

    #[test]
    fn build_errors_on_invalid_x_column() {
        let result = QueryResult::table(
            vec![make_col("t", ColumnKind::Timestamp)],
            vec![vec![Value::Int(1_000_000)]],
            None,
            Duration::ZERO,
        );
        let spec = simple_spec(5, &[0]); // x_col=5 is out of range
        assert!(matches!(
            ChartView::build(&result, spec),
            Err(ChartBuildError::InvalidXColumn(5))
        ));
    }

    #[test]
    fn build_succeeds_and_applies_decimation_threshold() {
        // 50_000 rows, threshold = 100.
        let n = 50_000usize;
        let rows: Vec<Vec<Value>> = (0..n)
            .map(|i| vec![Value::Int(i as i64), Value::Float(i as f64 * 0.1)])
            .collect();

        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );

        let mut spec = simple_spec(0, &[1]);
        spec.decimation_threshold = 100;

        let view = ChartView::build(&result, spec).expect("build should succeed");
        let series_pts = &view.render_model.decimated[0];
        assert!(
            series_pts.len() <= 100,
            "expected <= 100 decimated points, got {}",
            series_pts.len()
        );
    }

    #[test]
    fn build_has_axis_ticks_present() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0)],
            vec![Value::Int(1000), Value::Float(2.0)],
            vec![Value::Int(2000), Value::Float(3.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let spec = simple_spec(0, &[1]);
        let view = ChartView::build(&result, spec).expect("build should succeed");
        assert!(
            !view.render_model.x_ticks.is_empty(),
            "x ticks should be generated"
        );
        assert!(
            !view.render_model.y_ticks.is_empty(),
            "y ticks should be generated"
        );
    }

    #[test]
    fn build_assigns_palette_slots() {
        let rows = vec![vec![Value::Int(0), Value::Float(1.0), Value::Float(2.0)]];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("a", ColumnKind::Float),
                make_col("b", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let spec = simple_spec(0, &[1, 2]);
        let view = ChartView::build(&result, spec).expect("build should succeed");
        assert_eq!(view.render_model.palette_slots.len(), 2);
    }

    #[test]
    fn build_records_series_stats_post_decimation() {
        // Known input: 4 rows, below decimation threshold so stats are over all points.
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0)],
            vec![Value::Int(1000), Value::Float(2.0)],
            vec![Value::Int(2000), Value::Float(3.0)],
            vec![Value::Int(3000), Value::Float(4.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let spec = simple_spec(0, &[1]);
        let view = ChartView::build(&result, spec).expect("build should succeed");

        assert_eq!(
            view.series_stats().len(),
            1,
            "stats length should match series count"
        );

        let s = view.series_stats()[0].expect("series should have stats");
        assert_eq!(s.min, 1.0);
        assert_eq!(s.max, 4.0);
        assert_eq!(s.last, 4.0);
        // avg of [1,2,3,4] = 2.5
        assert!((s.avg - 2.5).abs() < 1e-9);
    }

    /// `ChartView::build` with `ChartKind::Bar` must not panic.
    ///
    /// Bar shares the kind-agnostic `RenderModel` with Line, so `build` succeeds
    /// identically; the Bar-specific geometry is produced later in `render`.
    #[test]
    fn build_with_bar_kind_does_not_panic() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0)],
            vec![Value::Int(1000), Value::Float(2.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1]);
        spec.kind = crate::chart::spec::ChartKind::Bar;

        // Must succeed; the Bar arm renders a placeholder, it does not panic.
        let _ = ChartView::build(&result, spec).expect("build with Bar kind must not fail");
    }

    /// `ChartView::build` with `ChartKind::Scatter` must not panic.
    #[test]
    fn build_with_scatter_kind_does_not_panic() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0)],
            vec![Value::Int(1000), Value::Float(2.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1]);
        spec.kind = crate::chart::spec::ChartKind::Scatter;

        let _ = ChartView::build(&result, spec).expect("build with Scatter kind must not fail");
    }

    /// `ChartView::build` defaults the kind from the spec, and `kind()` reflects it.
    #[test]
    fn build_preserves_bar_kind_in_spec() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0)],
            vec![Value::Int(1000), Value::Float(2.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1]);
        spec.kind = crate::chart::spec::ChartKind::Bar;

        let view = ChartView::build(&result, spec).expect("build should succeed");
        assert_eq!(view.kind(), crate::chart::spec::ChartKind::Bar);
    }

    // T-CE-G04: source_indices tracking tests

    #[test]
    fn source_indices_none_when_track_disabled() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0)],
            vec![Value::Int(1000), Value::Float(2.0)],
            vec![Value::Int(2000), Value::Float(3.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1]);
        spec.track_source_indices = false;

        let view = ChartView::build(&result, spec).expect("build should succeed");
        assert!(
            view.source_indices().is_none(),
            "source_indices must be None when tracking is disabled"
        );
    }

    #[test]
    fn source_indices_populated_when_track_enabled_no_decimation() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0)],
            vec![Value::Int(1000), Value::Float(2.0)],
            vec![Value::Int(2000), Value::Float(3.0)],
            vec![Value::Int(3000), Value::Float(4.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1]);
        spec.track_source_indices = true;

        let view = ChartView::build(&result, spec).expect("build should succeed");
        let src = view.source_indices().expect("source_indices must be Some");
        assert_eq!(src.len(), 1, "one series");
        assert_eq!(src[0].len(), 4, "4 points (no decimation below threshold)");
        // Without sort or decimation, indices are sequential.
        assert_eq!(src[0], vec![0usize, 1, 2, 3]);
    }

    #[test]
    fn source_indices_populated_when_track_enabled_with_decimation() {
        let n = 1000usize;
        let rows: Vec<Vec<Value>> = (0..n)
            .map(|i| vec![Value::Int(i as i64), Value::Float(i as f64)])
            .collect();
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1]);
        spec.decimation_threshold = 100;
        spec.track_source_indices = true;

        let view = ChartView::build(&result, spec).expect("build should succeed");
        let src = view.source_indices().expect("source_indices must be Some");
        assert_eq!(src.len(), 1, "one series");
        // After decimation: exactly `threshold` points retained.
        assert!(
            src[0].len() <= 100,
            "source index count must be <= decimation threshold, got {}",
            src[0].len()
        );
        // All source indices must be within [0, n).
        for &idx in &src[0] {
            assert!(idx < n, "source index {} out of range", idx);
        }
        // First must be 0 (LTTB always keeps first), last must be n-1.
        assert_eq!(src[0][0], 0, "first source index must be 0");
        assert_eq!(
            *src[0].last().unwrap(),
            n - 1,
            "last source index must be n-1"
        );
    }

    /// Regression baseline: captures the deterministic RenderModel snapshot for a
    /// known two-series time-series fixture. This test must remain green through
    /// all Phase A–H modifications to confirm the Line arm is never accidentally
    /// broken.
    ///
    /// Fixture: 5 rows, two numeric series, decimation_threshold = 10_000 (no
    /// decimation applied). All assertions are over data-space values and tick
    /// counts, which are fully deterministic for this input.
    #[test]
    fn regression_baseline_line_chart_render_model() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0), Value::Float(10.0)],
            vec![Value::Int(1000), Value::Float(2.0), Value::Float(20.0)],
            vec![Value::Int(2000), Value::Float(3.0), Value::Float(15.0)],
            vec![Value::Int(3000), Value::Float(4.0), Value::Float(25.0)],
            vec![Value::Int(4000), Value::Float(5.0), Value::Float(30.0)],
        ];

        let result = QueryResult::table(
            vec![
                make_col("ts", ColumnKind::Timestamp),
                make_col("val_a", ColumnKind::Float),
                make_col("val_b", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );

        let spec = simple_spec(0, &[1, 2]);
        let view = ChartView::build(&result, spec).expect("build should succeed");

        // Series count preserved.
        assert_eq!(view.render_model.decimated.len(), 2, "two series");

        // No decimation below threshold — all 5 points retained.
        assert_eq!(view.render_model.decimated[0].len(), 5, "series 0: 5 pts");
        assert_eq!(view.render_model.decimated[1].len(), 5, "series 1: 5 pts");

        // Data-space bounds are deterministic.
        assert_eq!(view.render_model.x_min, 0.0);
        assert_eq!(view.render_model.x_max, 4000.0);
        assert_eq!(view.render_model.y_min, 1.0);
        assert_eq!(view.render_model.y_max, 30.0);

        // At least one tick generated on each axis.
        assert!(
            !view.render_model.x_ticks.is_empty(),
            "x_ticks must be non-empty"
        );
        assert!(
            !view.render_model.y_ticks.is_empty(),
            "y_ticks must be non-empty"
        );

        // Palette slot indices stored for both series (theme-resolved at render time).
        assert_eq!(view.render_model.palette_slots.len(), 2);

        // Series stats over post-decimation (= all) points.
        let s0 = view.render_model.series_stats[0].expect("series 0 stats present");
        assert_eq!(s0.min, 1.0);
        assert_eq!(s0.max, 5.0);
        assert_eq!(s0.last, 5.0);
        assert!((s0.avg - 3.0).abs() < 1e-9, "avg of [1..5] == 3.0");

        let s1 = view.render_model.series_stats[1].expect("series 1 stats present");
        assert_eq!(s1.min, 10.0);
        assert_eq!(s1.max, 30.0);
        assert_eq!(s1.last, 30.0);

        // Linear mode: y_is_log must be false; y_log_* mirrors y bounds.
        assert!(!view.render_model.y_is_log);
        assert_eq!(view.render_model.y_log_min, view.render_model.y_min);
        assert_eq!(view.render_model.y_log_max, view.render_model.y_max);
    }

    // ---------------------------------------------------------------------------
    // T-PROJ-01..04: project_y_to_screen / regression for Y projection
    // ---------------------------------------------------------------------------

    /// T-PROJ-01: linear projection maps min → bottom, max → top.
    #[test]
    fn project_y_linear_maps_min_to_bottom_and_max_to_top() {
        let plot_y0 = 10.0_f32;
        let plot_h = 200.0_f32;
        let y_min = 0.0_f64;
        let y_max = 100.0_f64;
        let y_range = y_max - y_min;

        let bottom = project_y_to_screen(y_min, plot_y0, plot_h, y_min, y_range, false);
        let top = project_y_to_screen(y_max, plot_y0, plot_h, y_min, y_range, false);

        assert!(
            (bottom - (plot_y0 + plot_h)).abs() < 0.01,
            "min should map to bottom, got {bottom}"
        );
        assert!(
            (top - plot_y0).abs() < 0.01,
            "max should map to top, got {top}"
        );
    }

    /// T-PROJ-02: linear mode is byte-identical before and after this change.
    #[test]
    fn project_y_linear_midpoint_is_correct() {
        let plot_y0 = 0.0_f32;
        let plot_h = 100.0_f32;
        let y_min = 0.0_f64;
        let y_range = 100.0_f64;

        let mid = project_y_to_screen(50.0, plot_y0, plot_h, y_min, y_range, false);
        assert!(
            (mid - 50.0).abs() < 0.01,
            "midpoint should map to screen center (50px), got {mid}"
        );
    }

    /// T-PROJ-03: log1p mode — known value maps to expected screen coordinate.
    #[test]
    fn project_y_log_known_value() {
        // Setup: y range 0..99, log1p bounds ln(1)=0 to ln(100)≈4.605.
        let y_min = 0.0_f64;
        let y_max = 99.0_f64;
        let log_min = (y_min + 1.0).ln(); // = 0.0
        let log_max = (y_max + 1.0).ln(); // ≈ 4.605
        let log_range = log_max - log_min;

        let plot_y0 = 0.0_f32;
        let plot_h = 100.0_f32;

        // y = 0 → log1p(0) = 0 → bottom of chart.
        let sy0 = project_y_to_screen(0.0, plot_y0, plot_h, log_min, log_range, true);
        assert!(
            (sy0 - (plot_y0 + plot_h)).abs() < 0.1,
            "y=0 should map to bottom in log mode, got {sy0}"
        );

        // y = 99 → log1p(99) = log_max → top of chart.
        let sy_max = project_y_to_screen(99.0, plot_y0, plot_h, log_min, log_range, true);
        assert!(
            (sy_max - plot_y0).abs() < 0.1,
            "y=99 should map to top in log mode, got {sy_max}"
        );
    }

    /// T-PROJ-04: log mode with y=0 produces finite result (no -Inf from ln(0)).
    #[test]
    fn project_y_log_zero_is_finite() {
        let result = project_y_to_screen(0.0, 0.0, 100.0, 0.0, 5.0, true);
        assert!(
            result.is_finite(),
            "log mode with y=0 must produce finite screen coord"
        );
    }

    /// T-PROJ-05: ChartView::build with y_scale=Log sets y_is_log and log bounds.
    #[test]
    fn build_with_log_scale_sets_y_is_log() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(0.0)],
            vec![Value::Int(1000), Value::Float(99.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1]);
        spec.y_scale = crate::chart::spec::YScale::Log;

        let view = ChartView::build(&result, spec).expect("build should succeed");

        assert!(view.render_model.y_is_log, "y_is_log must be true");
        // Log bounds: ln(0+1)=0 to ln(99+1)≈4.605.
        assert!(
            view.render_model.y_log_min.abs() < 1e-9,
            "y_log_min should be ~0"
        );
        assert!(
            (view.render_model.y_log_max - 100f64.ln()).abs() < 1e-6,
            "y_log_max should be ~ln(100)"
        );
        // Linear y bounds are still original-scale.
        assert_eq!(view.render_model.y_min, 0.0);
        assert_eq!(view.render_model.y_max, 99.0);
    }

    /// `ChartView::build` with `ChartKind::Area` must not panic.
    ///
    /// Area shares the same kind-agnostic `RenderModel` as Line; the Area-specific
    /// geometry (filled paths + stroke) is produced later in `render`.
    #[test]
    fn build_with_area_kind_does_not_panic() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0)],
            vec![Value::Int(1000), Value::Float(2.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("v", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1]);
        spec.kind = crate::chart::spec::ChartKind::Area;

        let view = ChartView::build(&result, spec).expect("build with Area kind must not fail");
        assert_eq!(view.kind(), crate::chart::spec::ChartKind::Area);
    }

    /// `ChartView::build` with `ChartKind::StackedBar` must not panic.
    ///
    /// StackedBar shares the same kind-agnostic `RenderModel` as Bar; the
    /// stacked-y-range override and stacking geometry are applied in `render`.
    #[test]
    fn build_with_stacked_bar_kind_does_not_panic() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0), Value::Float(2.0)],
            vec![Value::Int(1000), Value::Float(3.0), Value::Float(4.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("a", ColumnKind::Float),
                make_col("b", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1, 2]);
        spec.kind = crate::chart::spec::ChartKind::StackedBar;

        let view =
            ChartView::build(&result, spec).expect("build with StackedBar kind must not fail");
        assert_eq!(view.kind(), crate::chart::spec::ChartKind::StackedBar);
    }

    /// `ChartView::build` with `ChartKind::Pie` must not panic.
    ///
    /// Pie shares the same kind-agnostic `RenderModel`; the wedge geometry
    /// and axis suppression are applied in `render`.
    #[test]
    fn build_with_pie_kind_does_not_panic() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(10.0), Value::Float(20.0)],
            vec![Value::Int(1000), Value::Float(15.0), Value::Float(25.0)],
        ];
        let result = QueryResult::table(
            vec![
                make_col("t", ColumnKind::Timestamp),
                make_col("a", ColumnKind::Float),
                make_col("b", ColumnKind::Float),
            ],
            rows,
            None,
            Duration::ZERO,
        );
        let mut spec = simple_spec(0, &[1, 2]);
        spec.kind = crate::chart::spec::ChartKind::Pie;

        let view = ChartView::build(&result, spec).expect("build with Pie kind must not fail");
        assert_eq!(view.kind(), crate::chart::spec::ChartKind::Pie);
    }
}
