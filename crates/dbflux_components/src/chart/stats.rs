//! Per-series statistics and hit-test helpers for the line-chart engine.
//!
//! All computations operate on the **post-decimation** `(x, y)` pairs stored in
//! `RenderModel`. This matches what the user sees on screen — a future change
//! can offer raw-data stats as a configurable switch.

// ---------------------------------------------------------------------------
// SeriesStats
// ---------------------------------------------------------------------------

/// Descriptive statistics computed over the Y values of a single chart series.
///
/// Computed over post-decimation data (matches the rendered view).
/// `None` is returned by `compute_series_stats` when the input is empty.
#[derive(Debug, Clone, Copy)]
pub struct SeriesStats {
    pub min: f64,
    pub max: f64,
    pub avg: f64,
    /// 50th-percentile (median) — nearest-rank method.
    pub p50: f64,
    /// 95th-percentile — nearest-rank method.
    pub p95: f64,
    /// 99th-percentile — nearest-rank method.
    pub p99: f64,
    /// Last Y value in the series (rightmost point).
    pub last: f64,
}

/// Compute descriptive statistics over a slice of `(x, y)` points.
///
/// Returns `None` when `points` is empty.
/// Percentiles use the nearest-rank method:
/// `index = ((p / 100) * (n - 1)).round() as usize`.
pub fn compute_series_stats(points: &[(f64, f64)]) -> Option<SeriesStats> {
    if points.is_empty() {
        return None;
    }

    let ys: Vec<f64> = points.iter().map(|(_, y)| *y).collect();
    let n = ys.len();

    let min = ys.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let avg = ys.iter().sum::<f64>() / n as f64;
    let last = ys[n - 1];

    // Sort a copy for percentile computation.
    let mut sorted = ys.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let percentile = |p: f64| -> f64 {
        let idx = ((p / 100.0) * (n as f64 - 1.0)).round() as usize;
        sorted[idx.min(n - 1)]
    };

    Some(SeriesStats {
        min,
        max,
        avg,
        p50: percentile(50.0),
        p95: percentile(95.0),
        p99: percentile(99.0),
        last,
    })
}

// ---------------------------------------------------------------------------
// Interpolation
// ---------------------------------------------------------------------------

/// Linear interpolation of Y at `target_x` using a sorted slice of `(x, y)` points.
///
/// - Returns `None` when `points` is empty.
/// - Clamps to the first point's Y when `target_x` is before the first point.
/// - Clamps to the last point's Y when `target_x` is after the last point.
/// - Linearly interpolates between the bracketing points otherwise.
pub fn interpolate_y_at_x(points: &[(f64, f64)], target_x: f64) -> Option<f64> {
    if points.is_empty() {
        return None;
    }

    if target_x <= points[0].0 {
        return Some(points[0].1);
    }

    let last_idx = points.len() - 1;
    if target_x >= points[last_idx].0 {
        return Some(points[last_idx].1);
    }

    // Binary-search for the bracketing pair.
    let insert = points.partition_point(|p| p.0 <= target_x);

    // `insert` is in range [1, last_idx] because we handled the edge cases above.
    let lo = points[insert - 1];
    let hi = points[insert];

    let span = hi.0 - lo.0;
    if span == 0.0 {
        return Some(lo.1);
    }

    let t = (target_x - lo.0) / span;
    Some(lo.1 + t * (hi.1 - lo.1))
}

// ---------------------------------------------------------------------------
// Hit-test
// ---------------------------------------------------------------------------

/// Returns the index of the series whose interpolated Y at `cursor_data_x`
/// projects closest in screen space to `cursor_screen_y`, within `tolerance_px`.
///
/// Returns `None` when no series falls within `tolerance_px` of the cursor.
///
/// `data_to_screen_y` converts a data-space Y value to a screen-space Y pixel.
pub fn hit_test_focused_series(
    decimated: &[Vec<(f64, f64)>],
    cursor_data_x: f64,
    cursor_screen_y: f32,
    data_to_screen_y: impl Fn(f64) -> f32,
    tolerance_px: f32,
) -> Option<usize> {
    let mut best_idx: Option<usize> = None;
    let mut best_dist = f32::INFINITY;

    for (idx, pts) in decimated.iter().enumerate() {
        if let Some(y_data) = interpolate_y_at_x(pts, cursor_data_x) {
            let y_screen = data_to_screen_y(y_data);
            let dist = (y_screen - cursor_screen_y).abs();
            if dist < best_dist {
                best_dist = dist;
                best_idx = Some(idx);
            }
        }
    }

    if best_dist <= tolerance_px {
        best_idx
    } else {
        None
    }
}

/// Format a duration given in milliseconds as a human-readable string.
///
/// Examples: "12s", "1h 23m", "45m 30s", "500ms".
pub fn format_span(ms: f64) -> String {
    if ms < 1_000.0 {
        return format!("{:.0}ms", ms);
    }

    let total_secs = (ms / 1_000.0) as u64;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if hours > 0 && mins > 0 {
        format!("{}h {}m", hours, mins)
    } else if hours > 0 {
        format!("{}h", hours)
    } else if mins > 0 && secs > 0 {
        format!("{}m {}s", mins, secs)
    } else if mins > 0 {
        format!("{}m", mins)
    } else {
        format!("{}s", total_secs)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- compute_series_stats ---

    #[test]
    fn compute_series_stats_basic() {
        let pts = vec![(0.0, 1.0), (1.0, 2.0), (2.0, 3.0), (3.0, 4.0)];
        let s = compute_series_stats(&pts).expect("should succeed");
        assert_eq!(s.min, 1.0);
        assert_eq!(s.max, 4.0);
        assert_eq!(s.avg, 2.5);
        assert_eq!(s.last, 4.0);
        // p50 nearest-rank: index = round(0.5 * 3) = 2 -> sorted[2] = 3.0
        assert_eq!(s.p50, 3.0);
    }

    #[test]
    fn compute_series_stats_empty() {
        assert!(compute_series_stats(&[]).is_none());
    }

    #[test]
    fn compute_series_stats_single_point() {
        let pts = vec![(0.0, 42.0)];
        let s = compute_series_stats(&pts).expect("should succeed");
        assert_eq!(s.min, 42.0);
        assert_eq!(s.max, 42.0);
        assert_eq!(s.avg, 42.0);
        assert_eq!(s.p50, 42.0);
        assert_eq!(s.p95, 42.0);
        assert_eq!(s.p99, 42.0);
        assert_eq!(s.last, 42.0);
    }

    // --- interpolate_y_at_x ---

    #[test]
    fn interpolate_y_at_x_exact_match() {
        let pts = vec![(0.0, 10.0), (1.0, 20.0), (2.0, 30.0)];
        assert_eq!(interpolate_y_at_x(&pts, 1.0), Some(20.0));
    }

    #[test]
    fn interpolate_y_at_x_bracket_midpoint() {
        let pts = vec![(0.0, 0.0), (2.0, 10.0)];
        // midpoint: t=0.5 -> 5.0
        assert_eq!(interpolate_y_at_x(&pts, 1.0), Some(5.0));
    }

    #[test]
    fn interpolate_y_at_x_before_first() {
        let pts = vec![(5.0, 100.0), (10.0, 200.0)];
        assert_eq!(interpolate_y_at_x(&pts, 0.0), Some(100.0));
    }

    #[test]
    fn interpolate_y_at_x_after_last() {
        let pts = vec![(5.0, 100.0), (10.0, 200.0)];
        assert_eq!(interpolate_y_at_x(&pts, 99.0), Some(200.0));
    }

    #[test]
    fn interpolate_y_at_x_empty() {
        assert!(interpolate_y_at_x(&[], 5.0).is_none());
    }

    // --- hit_test_focused_series ---

    #[test]
    fn hit_test_picks_closest_series() {
        // Three horizontal lines at y = 10, 20, 30 (data space = screen space).
        let series_0 = vec![(0.0, 10.0), (10.0, 10.0)];
        let series_1 = vec![(0.0, 20.0), (10.0, 20.0)];
        let series_2 = vec![(0.0, 30.0), (10.0, 30.0)];
        let decimated = vec![series_0, series_1, series_2];

        // Cursor near y=20 line in screen space (identity mapping).
        let result =
            hit_test_focused_series(&decimated, 5.0, 20.0_f32, |y_data| y_data as f32, 14.0);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn hit_test_returns_none_outside_tolerance() {
        let series_0 = vec![(0.0, 10.0), (10.0, 10.0)];
        let decimated = vec![series_0];

        // Cursor is 50px away — exceeds tolerance of 14px.
        let result =
            hit_test_focused_series(&decimated, 5.0, 60.0_f32, |y_data| y_data as f32, 14.0);
        assert!(result.is_none());
    }

    // --- format_span ---

    #[test]
    fn format_span_milliseconds() {
        assert_eq!(format_span(500.0), "500ms");
    }

    #[test]
    fn format_span_seconds() {
        assert_eq!(format_span(12_000.0), "12s");
    }

    #[test]
    fn format_span_minutes_and_seconds() {
        assert_eq!(format_span(90_000.0), "1m 30s");
    }

    #[test]
    fn format_span_hours_and_minutes() {
        assert_eq!(format_span(5_100_000.0), "1h 25m");
    }
}
