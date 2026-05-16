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
    AnyElement, Bounds, Context, Hsla, PathBuilder, Pixels, Render, SharedString, TextRun, Window,
    canvas, div, fill, font, point,
};

use crate::chart::axis::{TickLabel, ticks_numeric, ticks_time};
use crate::chart::decimate::lttb;
use crate::chart::spec::{AxisKind, ChartSpec};
use crate::chart::stats::{
    SeriesStats, compute_series_stats, hit_test_focused_series, interpolate_y_at_x,
};
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
// Palette
// ---------------------------------------------------------------------------

/// Design-aligned palette: exact Ayu Dark chart tokens from `tokens.css`.
///
/// HSL values computed from the hex palette:
///   #59C2FF → h=0.578, s=1.0,  l=0.673   (chart-1 cyan)
///   #AAD94C → h=0.228, s=0.673, l=0.572  (chart-2 lime)
///   #FFB454 → h=0.097, s=1.0,  l=0.666   (chart-3 amber / primary)
///   #F07178 → h=0.985, s=0.819, l=0.694  (chart-4 rose)
///   #D2A6FF → h=0.758, s=1.0,  l=0.826   (chart-5 lavender)
pub const CHART_PALETTE: &[Hsla] = &[
    Hsla {
        h: 0.578,
        s: 1.0,
        l: 0.673,
        a: 1.0,
    }, // #59C2FF chart-1 cyan
    Hsla {
        h: 0.228,
        s: 0.673,
        l: 0.572,
        a: 1.0,
    }, // #AAD94C chart-2 lime
    Hsla {
        h: 0.097,
        s: 1.0,
        l: 0.666,
        a: 1.0,
    }, // #FFB454 chart-3 amber
    Hsla {
        h: 0.985,
        s: 0.819,
        l: 0.694,
        a: 1.0,
    }, // #F07178 chart-4 rose
    Hsla {
        h: 0.758,
        s: 1.0,
        l: 0.826,
        a: 1.0,
    }, // #D2A6FF chart-5 lavender
];

/// Accent cyan — #95E6CB (min/max/avg stat values in the Stats dock).
pub const CHART_ACCENT_CYAN: Hsla = Hsla {
    h: 0.444,
    s: 0.618,
    l: 0.741,
    a: 1.0,
};

/// Accent primary — #FFB454 (p99 stat value, matches theme primary).
pub const CHART_ACCENT_PRIMARY: Hsla = Hsla {
    h: 0.097,
    s: 1.0,
    l: 0.666,
    a: 1.0,
};

// ---------------------------------------------------------------------------
// RenderModel
// ---------------------------------------------------------------------------

/// Pre-computed, immutable chart data stored after `build`. Render only reads this.
pub(crate) struct RenderModel {
    /// Decimated (x, y) pairs per series — in data space (f64, f64).
    pub decimated: Vec<Vec<(f64, f64)>>,
    /// Resolved palette colour per series.
    pub palette_colors: Vec<Hsla>,
    /// X-axis tick labels rendered below the plot area.
    pub x_ticks: Vec<TickLabel>,
    /// Y-axis tick labels rendered to the left of the plot area.
    pub y_ticks: Vec<TickLabel>,
    /// Data-space X bounds.
    pub x_min: f64,
    pub x_max: f64,
    /// Data-space Y bounds.
    pub y_min: f64,
    pub y_max: f64,
    /// Per-series descriptive stats over post-decimation Y values.
    /// Indexed parallel to `decimated`; `None` for empty series.
    pub series_stats: Vec<Option<SeriesStats>>,
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

        let decimated: Vec<Vec<(f64, f64)>> = raw_series_sorted
            .iter()
            .map(|ys| {
                let pts: Vec<(f64, f64)> = raw_x_sorted
                    .iter()
                    .zip(ys.iter())
                    .map(|(&x, &y)| (x, y))
                    .collect();
                if n > threshold {
                    lttb(&pts, threshold)
                } else {
                    pts
                }
            })
            .collect();

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

        // --- Axis ticks ---

        let x_ticks = if x_is_time {
            ticks_time(x_min, x_max, 6)
        } else {
            ticks_numeric(x_min, x_max, 6)
        };

        let y_ticks = ticks_numeric(y_min, y_max, 5);

        // --- Palette colours ---

        let palette_colors: Vec<Hsla> = spec
            .series
            .iter()
            .map(|s| {
                let idx = s.color_slot as usize % CHART_PALETTE.len();
                CHART_PALETTE[idx]
            })
            .collect();

        // Compute per-series stats over the post-decimation points.
        let series_stats: Vec<Option<SeriesStats>> = decimated
            .iter()
            .map(|pts| compute_series_stats(pts))
            .collect();

        let render_model = RenderModel {
            decimated,
            palette_colors,
            x_ticks,
            y_ticks,
            x_min,
            x_max,
            y_min,
            y_max,
            series_stats,
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

    /// Whether the X axis is a time axis.
    pub fn x_is_time(&self) -> bool {
        self.spec.x_axis.kind == AxisKind::Time
    }

    /// Resolved palette colour for the series at `idx`. Returns a neutral grey
    /// when `idx` is out of range.
    pub fn series_color(&self, idx: usize) -> Hsla {
        self.render_model
            .palette_colors
            .get(idx)
            .copied()
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

    /// Resolved palette colours, indexed parallel to `spec_series()`.
    pub fn palette_colors(&self) -> &[Hsla] {
        &self.render_model.palette_colors
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
        let y_min = self.render_model.y_min;
        let y_range = (self.render_model.y_max - y_min).max(1.0);

        let cursor_data_x = x_min + (rel_x as f64 / plot_w as f64) * x_range;

        let data_to_screen_y =
            |dy: f64| -> f32 { plot_y0 + plot_h - ((dy - y_min) / y_range * plot_h as f64) as f32 };

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
// Render
// ---------------------------------------------------------------------------

/// Margins around the plot area (pixels).
const MARGIN_LEFT: f32 = 50.0;
const MARGIN_RIGHT: f32 = 16.0;
const MARGIN_TOP: f32 = 8.0;
const MARGIN_BOTTOM: f32 = 32.0;

impl Render for ChartView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let model = &self.render_model;
        let spec = &self.spec;
        let hover_x = self.hover_x_screen;
        let focused_idx = self.focused_series_idx;

        let x_min = model.x_min;
        let x_max = model.x_max;
        let y_min = model.y_min;
        let y_max = model.y_max;
        let x_range = (x_max - x_min).max(1.0);
        let y_range = (y_max - y_min).max(1.0);

        let palette = model.palette_colors.clone();
        let decimated = model.decimated.clone();
        let x_is_time = spec.x_axis.kind == AxisKind::Time;

        // Tick label strings for in-canvas painting (Y data order; painting handles positioning).
        let y_tick_labels: Vec<(f64, SharedString)> = model
            .y_ticks
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
        let y_ticks_canvas = model.y_ticks.clone();
        let hover_x_canvas = hover_x;
        let y_tick_labels_canvas = y_tick_labels.clone();
        let hidden_canvas = self.hidden.clone();

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

                                // Plot area occupies the right portion of the canvas;
                                // the left MARGIN_LEFT is reserved for Y-axis tick labels.
                                let plot_x0 = ox + MARGIN_LEFT;
                                let plot_y0 = oy + MARGIN_TOP;
                                let plot_w = (w - MARGIN_LEFT - MARGIN_RIGHT).max(1.0);
                                let plot_h = (h - MARGIN_TOP - MARGIN_BOTTOM).max(1.0);

                                let data_to_screen_x = |dx: f64| -> f32 {
                                    plot_x0 + ((dx - x_min) / x_range * plot_w as f64) as f32
                                };
                                let data_to_screen_y = |dy: f64| -> f32 {
                                    // Y is inverted: top = y_max, bottom = y_min.
                                    plot_y0 + plot_h
                                        - ((dy - y_min) / y_range * plot_h as f64) as f32
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
                                for tick in &y_ticks_canvas {
                                    let sy = data_to_screen_y(tick.value);
                                    window.paint_quad(fill(
                                        gpui::Bounds {
                                            origin: point(gpui::px(plot_x0), gpui::px(sy - 0.5)),
                                            size: gpui::Size {
                                                width: gpui::px(plot_w),
                                                height: gpui::px(1.0),
                                            },
                                        },
                                        gpui::hsla(0.0, 0.0, 0.5, 0.18),
                                    ));
                                }

                                // --- Vertical gridlines at each X tick ---
                                for tick in &x_ticks_dynamic {
                                    let sx = data_to_screen_x(tick.value);
                                    window.paint_quad(fill(
                                        gpui::Bounds {
                                            origin: point(gpui::px(sx - 0.5), gpui::px(plot_y0)),
                                            size: gpui::Size {
                                                width: gpui::px(1.0),
                                                height: gpui::px(plot_h),
                                            },
                                        },
                                        gpui::hsla(0.0, 0.0, 0.5, 0.18),
                                    ));
                                }

                                // --- Series polylines (two-pass) ---
                                //
                                // Pass 1: all non-focused series at 2.0 px so they
                                // render below the focused line.
                                // Pass 2: focused series at 2.8 px, composited on top.
                                let paint_series =
                                    |pts: &[(f64, f64)],
                                     color: Hsla,
                                     stroke_w: f32,
                                     window: &mut Window| {
                                        if pts.is_empty() {
                                            return;
                                        }
                                        if pts.len() == 1 {
                                            // Single-point fallback: paint a square whose
                                            // side scales with stroke width.
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
                                    if s_idx == focused_idx || hidden_canvas.contains(&s_idx) {
                                        continue;
                                    }
                                    let color = palette_canvas
                                        .get(s_idx)
                                        .copied()
                                        .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));
                                    paint_series(pts, color, 1.4, window);
                                }

                                // Pass 2 — focused series at 2.2 px (composited on top).
                                // Skip if the focused series is hidden.
                                if !hidden_canvas.contains(&focused_idx)
                                    && let Some(pts) = decimated_canvas.get(focused_idx)
                                {
                                    let color = palette_canvas
                                        .get(focused_idx)
                                        .copied()
                                        .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));
                                    paint_series(pts, color, 2.2, window);
                                }

                                // --- Crosshair (dashed, amber #FFB454 at 0.7 opacity) ---
                                if let Some(hx) = hover_x_canvas {
                                    let sx = f32::from(hx);
                                    if sx >= plot_x0 && sx <= plot_x0 + plot_w {
                                        paint_dashed_vline(
                                            window,
                                            sx,
                                            plot_y0,
                                            plot_y0 + plot_h,
                                            gpui::hsla(0.097, 1.0, 0.666, 0.7),
                                            2.0,
                                            3.0,
                                        );

                                        // --- Hover dots per series ---
                                        // Two-pass: fill background first, then stroke series color.
                                        let cursor_data_x = x_min
                                            + ((sx - plot_x0) as f64 / plot_w as f64) * x_range;

                                        for (s_idx, pts) in decimated_canvas.iter().enumerate() {
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
                                                window.paint_path(
                                                    path,
                                                    gpui::hsla(0.56, 0.17, 0.07, 1.0),
                                                );
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
                                let tick_color = gpui::hsla(0.0, 0.0, 0.55, 1.0);
                                let tick_font = font("Zed Mono");
                                let tick_size = gpui::px(10.0);
                                let line_height = gpui::px(12.0);

                                for (value, label) in &y_tick_labels_canvas {
                                    let sy = data_to_screen_y(*value);
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

                                for tick in &x_ticks_dynamic {
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
                    .when_some(readout, |container, r| container.child(readout_overlay(r))),
            )
            // Legend row (below canvas)
            .when_some(legend, |d, leg| d.child(leg))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
fn readout_overlay(r: HoverReadout) -> impl IntoElement {
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
        .bg(gpui::hsla(0.0, 0.0, 0.09, 1.0))
        .border_1()
        .border_color(gpui::hsla(0.0, 0.0, 1.0, 0.18))
        .rounded(gpui::px(6.0))
        .text_size(FontSizes::XS)
        .overflow_hidden()
        // Header: time + optional offset
        .child(
            div()
                .flex()
                .items_center()
                .text_color(gpui::hsla(0.0, 0.0, 0.85, 1.0))
                .font_weight(gpui::FontWeight::SEMIBOLD)
                .child(r.header_time)
                .when_some(r.header_offset, |d, offset| {
                    d.child(
                        div()
                            .text_color(gpui::hsla(0.0, 0.0, 0.55, 1.0))
                            .child(offset),
                    )
                }),
        )
        // One row per series; focused row gets semibold + slightly brighter bg.
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
                        .text_color(gpui::hsla(0.0, 0.0, 0.55, 1.0))
                        .child(entry.label),
                )
                // Value (foreground)
                .child(
                    div()
                        .text_color(gpui::hsla(0.0, 0.0, 0.95, 1.0))
                        .child(entry.y_label),
                )
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
    fn build_assigns_palette_colors() {
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
        assert_eq!(view.render_model.palette_colors.len(), 2);
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

        // Palette colours resolved for both series.
        assert_eq!(view.render_model.palette_colors.len(), 2);

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
    }
}
