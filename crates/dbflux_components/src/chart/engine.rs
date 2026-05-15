//! `ChartView` — the GPUI entity that renders a line chart.
//!
//! All expensive computation (decimation, tick generation, colour resolution)
//! happens in `ChartView::build`. `Render::render` is a pure read of the
//! stored `RenderModel`.

use std::cell::RefCell;
use std::rc::Rc;

use gpui::prelude::*;
use gpui::{
    App, Bounds, Context, Hsla, PathBuilder, Pixels, Render, SharedString, Window, canvas, div,
    fill, point,
};

use crate::chart::axis::{TickLabel, ticks_numeric, ticks_time};
use crate::chart::decimate::lttb;
use crate::chart::legend::legend_element;
use crate::chart::spec::{AxisKind, ChartSpec};
use crate::chart::stats::{SeriesStats, compute_series_stats, hit_test_focused_series, interpolate_y_at_x};
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

/// Default palette for chart series (HSLA, 1.0 alpha).
pub const CHART_PALETTE: &[Hsla] = &[
    Hsla {
        h: 0.60,
        s: 0.70,
        l: 0.55,
        a: 1.0,
    }, // blue
    Hsla {
        h: 0.10,
        s: 0.80,
        l: 0.55,
        a: 1.0,
    }, // orange
    Hsla {
        h: 0.37,
        s: 0.55,
        l: 0.45,
        a: 1.0,
    }, // green
    Hsla {
        h: 0.00,
        s: 0.70,
        l: 0.55,
        a: 1.0,
    }, // red
    Hsla {
        h: 0.80,
        s: 0.55,
        l: 0.55,
        a: 1.0,
    }, // purple
    Hsla {
        h: 0.13,
        s: 0.75,
        l: 0.45,
        a: 1.0,
    }, // amber
];

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

        let data_to_screen_y = |dy: f64| -> f32 {
            plot_y0 + plot_h - ((dy - y_min) / y_range * plot_h as f64) as f32
        };

        let cursor_screen_y = f32::from(hover_y);

        let candidate = hit_test_focused_series(
            &self.render_model.decimated,
            cursor_data_x,
            cursor_screen_y,
            data_to_screen_y,
            14.0,
        );

        let Some(new_idx) = candidate else { return };

        if new_idx == self.focused_series_idx {
            return;
        }

        // Dead-band: only switch when the new series is strictly closer than the
        // current focused series by >= 2 px, mitigating jitter between near lines.
        let dist_new = interpolate_y_at_x(
            &self.render_model.decimated[new_idx],
            cursor_data_x,
        )
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

        // Clone for canvas closure.
        let decimated_canvas = decimated.clone();
        let palette_canvas = palette.clone();
        let y_ticks_canvas = model.y_ticks.clone();
        let hover_x_canvas = hover_x;

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
            spec,
            x_min,
            x_range,
            x_is_time,
        );

        // Y-axis labels (rendered reversed: highest value at top, lowest at bottom).
        // `justify_between` distributes them evenly in the label column, matching
        // the evenly-spaced gridlines produced by the nice-tick algorithm.
        let y_label_texts: Vec<SharedString> = model
            .y_ticks
            .iter()
            .rev()
            .map(|t| SharedString::from(t.label.clone()))
            .collect();

        // X-axis labels (left-to-right, matching data order).
        let x_label_texts: Vec<SharedString> = model
            .x_ticks
            .iter()
            .map(|t| SharedString::from(t.label.clone()))
            .collect();

        // Legend element (built outside the canvas closure so it can be a div child).
        let legend = if spec.legend_visible && spec.series.len() > 1 {
            let entity = cx.entity().clone();
            Some(legend_element(
                &spec.series,
                &palette,
                focused_idx,
                Some(move |idx: usize, _window: &mut Window, cx: &mut App| {
                    entity.update(cx, |this, cx| {
                        this.set_focused_series_idx(idx, cx);
                    });
                }),
            ))
        } else {
            None
        };

        div()
            .flex()
            .flex_col()
            .size_full()
            // Chart body: Y-label column + plot area side by side.
            .child(
                div()
                    .flex()
                    .flex_grow()
                    // Y-axis label column — sits in the left margin.
                    .child(
                        div()
                            .w(gpui::px(MARGIN_LEFT))
                            .flex_shrink_0()
                            // Inset by MARGIN_TOP at the top so labels align with
                            // the plot area, not the full container height.
                            .pt(gpui::px(MARGIN_TOP))
                            .flex()
                            .flex_col()
                            .justify_between()
                            .overflow_hidden()
                            .children(y_label_texts.into_iter().map(|label| {
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_end()
                                    .pr(gpui::px(4.0))
                                    .text_size(FontSizes::XS)
                                    .text_color(gpui::hsla(0.0, 0.0, 0.55, 1.0))
                                    .child(label)
                            })),
                    )
                    // Plot area: canvas (absolute, size_full) inside a relative container.
                    .child(
                        div()
                            .flex_grow()
                            .relative()
                            .on_mouse_move(cx.listener(
                                move |this, ev: &gpui::MouseMoveEvent, _window, cx| {
                                    this.hover_x_screen = Some(ev.position.x);
                                    this.hover_y_screen = Some(ev.position.y);
                                    this.update_focused_from_hover();
                                    cx.notify();
                                },
                            ))
                            .child({
                                canvas(
                                    move |bounds, _window, _cx| {
                                        *plot_bounds_for_canvas.borrow_mut() = Some(bounds);
                                        bounds
                                    },
                                    move |_bounds, bounds_data, window, _cx| {
                                        let b = bounds_data;
                                        let w = f32::from(b.size.width);
                                        let h = f32::from(b.size.height);
                                        let ox = f32::from(b.origin.x);
                                        let oy = f32::from(b.origin.y);

                                        // Plot area in screen space (no left margin here;
                                        // the Y-label column already occupies MARGIN_LEFT).
                                        let plot_x0 = ox;
                                        let plot_y0 = oy + MARGIN_TOP;
                                        let plot_w = (w - MARGIN_RIGHT).max(1.0);
                                        let plot_h = (h - MARGIN_TOP - MARGIN_BOTTOM).max(1.0);

                                        let data_to_screen_x = |dx: f64| -> f32 {
                                            plot_x0
                                                + ((dx - x_min) / x_range * plot_w as f64) as f32
                                        };
                                        let data_to_screen_y = |dy: f64| -> f32 {
                                            // Y is inverted: top = y_max, bottom = y_min.
                                            plot_y0 + plot_h
                                                - ((dy - y_min) / y_range * plot_h as f64) as f32
                                        };

                                        // --- Horizontal gridlines at each Y tick ---
                                        for tick in &y_ticks_canvas {
                                            let sy = data_to_screen_y(tick.value);
                                            window.paint_quad(fill(
                                                gpui::Bounds {
                                                    origin: point(
                                                        gpui::px(plot_x0),
                                                        gpui::px(sy - 0.5),
                                                    ),
                                                    size: gpui::Size {
                                                        width: gpui::px(plot_w),
                                                        height: gpui::px(1.0),
                                                    },
                                                },
                                                gpui::hsla(0.0, 0.0, 0.5, 0.2),
                                            ));
                                        }

                                        // --- Series polylines (two-pass) ---
                                        //
                                        // Pass 1: all non-focused series at 2.0 px so they
                                        // render below the focused line.
                                        // Pass 2: focused series at 2.8 px, composited on top.
                                        let paint_series = |pts: &[(f64, f64)],
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

                                        // Pass 1 — non-focused series at 2.0 px.
                                        for (s_idx, pts) in decimated_canvas.iter().enumerate() {
                                            if s_idx == focused_idx {
                                                continue;
                                            }
                                            let color = palette_canvas
                                                .get(s_idx)
                                                .copied()
                                                .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));
                                            paint_series(pts, color, 2.0, window);
                                        }

                                        // Pass 2 — focused series at 2.8 px (composited on top).
                                        if let Some(pts) = decimated_canvas.get(focused_idx) {
                                            let color = palette_canvas
                                                .get(focused_idx)
                                                .copied()
                                                .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));
                                            paint_series(pts, color, 2.8, window);
                                        }

                                        // --- Crosshair ---
                                        if let Some(hx) = hover_x_canvas {
                                            let sx = f32::from(hx);
                                            if sx >= plot_x0 && sx <= plot_x0 + plot_w {
                                                window.paint_quad(fill(
                                                    gpui::Bounds {
                                                        origin: point(
                                                            gpui::px(sx - 0.5),
                                                            gpui::px(plot_y0),
                                                        ),
                                                        size: gpui::Size {
                                                            width: gpui::px(1.0),
                                                            height: gpui::px(plot_h),
                                                        },
                                                    },
                                                    gpui::hsla(0.0, 0.0, 0.7, 0.5),
                                                ));
                                            }
                                        }
                                    },
                                )
                                .absolute()
                                .size_full()
                            })
                            .when_some(readout, |container, r| container.child(readout_overlay(r))),
                    ),
            )
            // X-axis label row — sits below the chart body, inset by the Y-label column width.
            .child(
                div()
                    .h(gpui::px(MARGIN_BOTTOM))
                    .flex_shrink_0()
                    // Left padding equal to the Y-label column so labels start at plot_x0.
                    .pl(gpui::px(MARGIN_LEFT))
                    .flex()
                    .items_start()
                    .justify_between()
                    .overflow_hidden()
                    .children(x_label_texts.into_iter().map(|label| {
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(gpui::hsla(0.0, 0.0, 0.55, 1.0))
                            .child(label)
                    })),
            )
            // Legend row (below X labels)
            .when_some(legend, |d, leg| d.child(leg))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

/// Pre-computed content for the crosshair readout panel.
///
/// `screen_x_relative` is the cursor X in plot-area-local coordinates and is
/// used to anchor the overlay; the panel itself is clamped to stay inside the
/// plot area when the cursor is close to the right edge.
struct HoverReadout {
    series_label: SharedString,
    series_color: Hsla,
    x_label: SharedString,
    y_label: SharedString,
    screen_x_relative: Pixels,
    plot_width: Pixels,
}

/// Derive readout content from the captured plot-area bounds and the
/// last-seen hover X. Returns `None` when bounds are not yet known (no paint
/// has run), when the cursor falls outside the plot area, or when the
/// focused series has no usable samples.
#[allow(clippy::too_many_arguments)]
fn build_readout(
    hover_x_window: Option<Pixels>,
    plot_bounds: Option<&Bounds<Pixels>>,
    decimated: &[Vec<(f64, f64)>],
    palette: &[Hsla],
    focused_idx: usize,
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

    let relative_x = hover_x - plot_x0;
    let rel_x_f = f32::from(relative_x);
    if rel_x_f < 0.0 || rel_x_f > plot_w_px {
        return None;
    }

    let cursor_data_x = x_min + (rel_x_f as f64 / plot_w_px as f64) * x_range;

    let series_pts = decimated.get(focused_idx)?;
    if series_pts.is_empty() {
        return None;
    }

    let (sample_x, sample_y) = nearest_sample(series_pts, cursor_data_x);

    let series_spec = spec.series.get(focused_idx)?;

    let series_color = palette
        .get(focused_idx)
        .copied()
        .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));

    Some(HoverReadout {
        series_label: SharedString::from(series_spec.label.clone()),
        series_color,
        x_label: SharedString::from(format_x_value(sample_x, x_is_time)),
        y_label: SharedString::from(format_y_value(sample_y)),
        screen_x_relative: relative_x,
        plot_width: plot_w,
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

fn format_x_value(x: f64, is_time: bool) -> String {
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

fn format_y_value(y: f64) -> String {
    if y.abs() >= 1000.0 || (y != 0.0 && y.abs() < 0.001) {
        format!("{:.3e}", y)
    } else {
        format!("{:.3}", y)
    }
}

/// Build the absolute-positioned overlay div that shows the readout. Clamps
/// to the left side of the crosshair when the cursor is past the midpoint of
/// the plot so the panel never falls outside the chart.
fn readout_overlay(r: HoverReadout) -> impl IntoElement {
    const PANEL_GAP: f32 = 8.0;
    const PANEL_ESTIMATED_WIDTH: f32 = 170.0;

    let hover_x = f32::from(r.screen_x_relative);
    let plot_w = f32::from(r.plot_width);
    let flip_to_left = hover_x + PANEL_GAP + PANEL_ESTIMATED_WIDTH > plot_w;

    let left_px = if flip_to_left {
        (hover_x - PANEL_GAP - PANEL_ESTIMATED_WIDTH).max(0.0)
    } else {
        hover_x + PANEL_GAP
    };

    div()
        .absolute()
        .left(gpui::px(left_px))
        .top(gpui::px(MARGIN_TOP + 4.0))
        .flex()
        .flex_col()
        .gap_1()
        .px_2()
        .py_1()
        .bg(gpui::hsla(0.0, 0.0, 0.1, 0.92))
        .text_size(FontSizes::XS)
        .text_color(gpui::hsla(0.0, 0.0, 0.95, 1.0))
        .child(
            div()
                .flex()
                .items_center()
                .gap_2()
                .child(div().w(gpui::px(8.0)).h(gpui::px(8.0)).bg(r.series_color))
                .child(r.series_label),
        )
        .child(div().child(r.x_label))
        .child(div().child(r.y_label))
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
}
