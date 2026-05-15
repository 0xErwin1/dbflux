//! `ChartView` — the GPUI entity that renders a line chart.
//!
//! All expensive computation (decimation, tick generation, colour resolution)
//! happens in `ChartView::build`. `Render::render` is a pure read of the
//! stored `RenderModel`.

use gpui::prelude::*;
use gpui::{App, Context, Hsla, PathBuilder, Pixels, Render, Window, canvas, div, fill, point};

use crate::chart::axis::{TickLabel, ticks_numeric, ticks_time};
use crate::chart::decimate::lttb;
use crate::chart::legend::legend_element;
use crate::chart::spec::{AxisKind, ChartSpec};
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
    Hsla { h: 0.60, s: 0.70, l: 0.55, a: 1.0 }, // blue
    Hsla { h: 0.10, s: 0.80, l: 0.55, a: 1.0 }, // orange
    Hsla { h: 0.37, s: 0.55, l: 0.45, a: 1.0 }, // green
    Hsla { h: 0.00, s: 0.70, l: 0.55, a: 1.0 }, // red
    Hsla { h: 0.80, s: 0.55, l: 0.55, a: 1.0 }, // purple
    Hsla { h: 0.13, s: 0.75, l: 0.45, a: 1.0 }, // amber
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
    /// X-axis tick labels (reserved for future tick-label overlay).
    #[allow(dead_code)]
    pub x_ticks: Vec<TickLabel>,
    /// Y-axis tick labels.
    pub y_ticks: Vec<TickLabel>,
    /// Data-space X bounds.
    pub x_min: f64,
    pub x_max: f64,
    /// Data-space Y bounds.
    pub y_min: f64,
    pub y_max: f64,
    /// Whether the X axis is time-formatted (reserved for tick formatting).
    #[allow(dead_code)]
    pub x_is_time: bool,
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
    /// Screen-space X coordinate of the current crosshair, relative to the
    /// canvas origin. `None` when the cursor is outside the chart.
    hover_x_screen: Option<Pixels>,
    /// Index of the focused series used for the crosshair readout.
    focused_series_idx: usize,
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
        indices.sort_by(|&a, &b| raw_x[a].partial_cmp(&raw_x[b]).unwrap_or(std::cmp::Ordering::Equal));
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
        let x_max = raw_x_sorted.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

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

        let render_model = RenderModel {
            decimated,
            palette_colors,
            x_ticks,
            y_ticks,
            x_min,
            x_max,
            y_min,
            y_max,
            x_is_time,
        };

        Ok(ChartView {
            spec,
            render_model,
            hover_x_screen: None,
            focused_series_idx: 0,
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

        // Clone for canvas closure.
        let decimated_canvas = decimated.clone();
        let palette_canvas = palette.clone();
        let y_ticks_canvas = model.y_ticks.clone();
        let hover_x_canvas = hover_x;

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
            // Main chart canvas
            .child(
                div()
                    .flex_grow()
                    .relative()
                    .on_mouse_move(cx.listener(
                        move |this, ev: &gpui::MouseMoveEvent, _window, cx| {
                            this.hover_x_screen = Some(ev.position.x);
                            cx.notify();
                        },
                    ))
                    .child({
                        canvas(
                            move |bounds, _window, _cx| bounds,
                            move |_bounds, bounds_data, window, _cx| {
                                let b = bounds_data;
                                let w = f32::from(b.size.width);
                                let h = f32::from(b.size.height);
                                let ox = f32::from(b.origin.x);
                                let oy = f32::from(b.origin.y);

                                // Plot area in screen space.
                                let plot_x0 = ox + MARGIN_LEFT;
                                let plot_y0 = oy + MARGIN_TOP;
                                let plot_w = (w - MARGIN_LEFT - MARGIN_RIGHT).max(1.0);
                                let plot_h = (h - MARGIN_TOP - MARGIN_BOTTOM).max(1.0);

                                let data_to_screen_x = |dx: f64| -> f32 {
                                    plot_x0 + ((dx - x_min) / x_range * plot_w as f64) as f32
                                };
                                let data_to_screen_y = |dy: f64| -> f32 {
                                    // Y is inverted: top = y_max, bottom = y_min.
                                    plot_y0 + plot_h - ((dy - y_min) / y_range * plot_h as f64) as f32
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

                                // --- Series polylines ---
                                for (s_idx, pts) in decimated_canvas.iter().enumerate() {
                                    if pts.is_empty() {
                                        continue;
                                    }
                                    let color = palette_canvas
                                        .get(s_idx)
                                        .copied()
                                        .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));

                                    if pts.len() == 1 {
                                        // Single-point fallback: paint a 3×3 square.
                                        let sx = data_to_screen_x(pts[0].0);
                                        let sy = data_to_screen_y(pts[0].1);
                                        window.paint_quad(fill(
                                            gpui::Bounds {
                                                origin: point(gpui::px(sx - 1.5), gpui::px(sy - 1.5)),
                                                size: gpui::Size {
                                                    width: gpui::px(3.0),
                                                    height: gpui::px(3.0),
                                                },
                                            },
                                            color,
                                        ));
                                    } else {
                                        let mut builder = PathBuilder::stroke(gpui::px(1.5));
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
                                }

                                // --- Crosshair ---
                                if let Some(hx) = hover_x_canvas {
                                    let sx = f32::from(hx);
                                    if sx >= plot_x0 && sx <= plot_x0 + plot_w {
                                        window.paint_quad(fill(
                                            gpui::Bounds {
                                                origin: point(gpui::px(sx - 0.5), gpui::px(plot_y0)),
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
            )
            // Legend row (below chart)
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
            if f.is_finite() { Some(*f) } else { None }
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
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::spec::{AxisKind, AxisSpec, ChartSpec, SeriesSpec};
    use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, Value};
    use std::time::Duration;

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
            vec![make_col("t", ColumnKind::Timestamp), make_col("v", ColumnKind::Float)],
            vec![],
            None,
            Duration::ZERO,
        );
        let spec = simple_spec(0, &[1]);
        assert!(matches!(ChartView::build(&result, spec), Err(ChartBuildError::Empty)));
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
            vec![make_col("t", ColumnKind::Timestamp), make_col("v", ColumnKind::Float)],
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
            vec![make_col("t", ColumnKind::Timestamp), make_col("v", ColumnKind::Float)],
            rows,
            None,
            Duration::ZERO,
        );
        let spec = simple_spec(0, &[1]);
        let view = ChartView::build(&result, spec).expect("build should succeed");
        assert!(!view.render_model.x_ticks.is_empty(), "x ticks should be generated");
        assert!(!view.render_model.y_ticks.is_empty(), "y ticks should be generated");
    }

    #[test]
    fn build_assigns_palette_colors() {
        let rows = vec![
            vec![Value::Int(0), Value::Float(1.0), Value::Float(2.0)],
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
        let spec = simple_spec(0, &[1, 2]);
        let view = ChartView::build(&result, spec).expect("build should succeed");
        assert_eq!(view.render_model.palette_colors.len(), 2);
    }
}
