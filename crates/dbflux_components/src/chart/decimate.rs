//! LTTB (Largest-Triangle-Three-Buckets) downsampling for chart series.
//!
//! LTTB preserves the visual shape of a series better than uniform sampling
//! because it keeps the point in each bucket that forms the largest triangle
//! with its neighbours, maximising apparent visual area.

/// Downsample `points` to at most `target` points using the LTTB algorithm.
///
/// Invariants:
/// - Always includes the first and last input points when `points.len() >= 2`.
/// - Returns the input unchanged (clone) when `target >= points.len()`.
/// - Returns `[first, last]` when `target < 3` and `points.len() >= 2`.
/// - Returns a clone of `points` when `points.len() <= 1`.
///
/// Inputs are `(x, y)` pairs in f64. Callers are responsible for converting
/// integer timestamps or integer values before passing them here.
pub fn lttb(points: &[(f64, f64)], target: usize) -> Vec<(f64, f64)> {
    let n = points.len();

    if n <= 1 || target >= n {
        return points.to_vec();
    }

    if target < 3 {
        return vec![points[0], points[n - 1]];
    }

    let mut sampled = Vec::with_capacity(target);

    // Always include first point.
    sampled.push(points[0]);

    // The middle points are split into (target - 2) equal-width buckets.
    let bucket_count = target - 2;
    // Each bucket covers this many raw points.
    let every = (n - 2) as f64 / bucket_count as f64;

    let mut a = 0usize; // index of the last selected point

    for i in 0..bucket_count {
        // Range of the current bucket (B).
        let b_start = (((i + 1) as f64) * every + 1.0) as usize;
        let b_end = (((i + 2) as f64) * every + 1.0).min(n as f64) as usize;
        let b_end = b_end.min(n); // clamp

        // Range of the next bucket (C) used to compute the average point.
        let c_start = b_end;
        let c_end = ((((i + 2) as f64) * every + 1.0).min(n as f64)) as usize;
        let c_end = c_end.min(n);

        // Compute average of next bucket as the virtual third point.
        let (avg_x, avg_y, count) = if c_start < c_end {
            let mut sum_x = 0.0f64;
            let mut sum_y = 0.0f64;
            let count = (c_end - c_start) as f64;
            for &(x, y) in &points[c_start..c_end] {
                sum_x += x;
                sum_y += y;
            }
            (sum_x / count, sum_y / count, count)
        } else {
            // Edge case: last bucket averages to the final point.
            let last = points[n - 1];
            (last.0, last.1, 1.0f64)
        };
        let _ = count;

        // Point A is the last selected point.
        let (ax, ay) = points[a];

        // Find the point in bucket B that forms the largest triangle with A and avg(C).
        let mut max_area = -1.0f64;
        let mut best = b_start.min(n - 1);

        for j in b_start..b_end {
            let (bx, by) = points[j.min(n - 1)];
            // Triangle area (×2, sign does not matter — we want the maximum).
            let area = ((ax - avg_x) * (by - ay) - (ax - bx) * (avg_y - ay)).abs();
            if area > max_area {
                max_area = area;
                best = j;
            }
        }

        sampled.push(points[best.min(n - 1)]);
        a = best;
    }

    // Always include last point.
    sampled.push(points[n - 1]);

    sampled
}

/// Downsample `points` to at most `target` points using LTTB, also returning
/// the original source index for each selected point.
///
/// Returns a `Vec` of `(point, source_index)` pairs. The `source_index` maps
/// back to the original row in the unsorted `QueryResult` (the caller must
/// account for any prior sort reindexing via `original_indices`).
///
/// Shares the same invariants as `lttb`.
pub fn lttb_with_indices(
    points: &[(f64, f64)],
    original_indices: &[usize],
    target: usize,
) -> Vec<((f64, f64), usize)> {
    let n = points.len();
    assert_eq!(
        points.len(),
        original_indices.len(),
        "points and original_indices must have the same length"
    );

    if n == 0 {
        return vec![];
    }

    if n == 1 {
        return vec![(points[0], original_indices[0])];
    }

    if target >= n {
        return points
            .iter()
            .zip(original_indices.iter())
            .map(|(&p, &i)| (p, i))
            .collect();
    }

    if target < 3 {
        return vec![
            (points[0], original_indices[0]),
            (points[n - 1], original_indices[n - 1]),
        ];
    }

    let mut sampled = Vec::with_capacity(target);

    sampled.push((points[0], original_indices[0]));

    let bucket_count = target - 2;
    let every = (n - 2) as f64 / bucket_count as f64;

    let mut a = 0usize;

    for i in 0..bucket_count {
        let b_start = (((i + 1) as f64) * every + 1.0) as usize;
        let b_end = (((i + 2) as f64) * every + 1.0).min(n as f64) as usize;
        let b_end = b_end.min(n);

        let c_start = b_end;
        let c_end = ((((i + 2) as f64) * every + 1.0).min(n as f64)) as usize;
        let c_end = c_end.min(n);

        let (avg_x, avg_y) = if c_start < c_end {
            let mut sum_x = 0.0f64;
            let mut sum_y = 0.0f64;
            let count = (c_end - c_start) as f64;
            for &(x, y) in &points[c_start..c_end] {
                sum_x += x;
                sum_y += y;
            }
            (sum_x / count, sum_y / count)
        } else {
            let last = points[n - 1];
            (last.0, last.1)
        };

        let (ax, ay) = points[a];

        let mut max_area = -1.0f64;
        let mut best = b_start.min(n - 1);

        for j in b_start..b_end {
            let (bx, by) = points[j.min(n - 1)];
            let area = ((ax - avg_x) * (by - ay) - (ax - bx) * (avg_y - ay)).abs();
            if area > max_area {
                max_area = area;
                best = j;
            }
        }

        sampled.push((points[best.min(n - 1)], original_indices[best.min(n - 1)]));
        a = best;
    }

    sampled.push((points[n - 1], original_indices[n - 1]));

    sampled
}

// ---------------------------------------------------------------------------
// Unit tests (strict TDD)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::PI;

    #[test]
    fn lttb_identity_when_target_ge_len() {
        let pts: Vec<(f64, f64)> = (0..5).map(|i| (i as f64, i as f64)).collect();
        assert_eq!(lttb(&pts, 5), pts);
        assert_eq!(lttb(&pts, 10), pts);
    }

    #[test]
    fn lttb_returns_endpoints_when_target_lt_3() {
        let pts: Vec<(f64, f64)> = (0..10).map(|i| (i as f64, i as f64)).collect();
        let result = lttb(&pts, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], pts[0]);
        assert_eq!(result[result.len() - 1], pts[pts.len() - 1]);
    }

    #[test]
    fn lttb_preserves_first_and_last() {
        let pts: Vec<(f64, f64)> = (0..100).map(|i| (i as f64, (i as f64).sin())).collect();
        let result = lttb(&pts, 20);
        assert_eq!(result.len(), 20, "should return exactly target points");
        assert_eq!(result[0], pts[0], "first point must be preserved");
        assert_eq!(
            result[result.len() - 1],
            pts[pts.len() - 1],
            "last point must be preserved"
        );
    }

    #[test]
    fn lttb_shape_preservation() {
        // A full sine wave over 1000 points → downsample to 50.
        // All 4 extrema (2 peaks + 2 troughs within the range) should survive
        // because they are the points that maximise triangle area.
        let n = 1000usize;
        let pts: Vec<(f64, f64)> = (0..n)
            .map(|i| {
                let x = i as f64;
                // Two full cycles so we get 4 extrema.
                let y = (2.0 * PI * x / n as f64 * 2.0).sin();
                (x, y)
            })
            .collect();

        let result = lttb(&pts, 50);

        // Count how many sampled points are close to a local extremum (|y| > 0.95).
        let extrema_count = result.iter().filter(|&&(_, y)| y.abs() > 0.95).count();
        assert!(
            extrema_count >= 4,
            "expected at least 4 extrema preserved, got {}",
            extrema_count
        );
    }

    // T-CE-G04: lttb_with_indices tests

    #[test]
    fn lttb_with_indices_identity_when_target_ge_len() {
        let pts: Vec<(f64, f64)> = (0..5).map(|i| (i as f64, i as f64)).collect();
        let idx: Vec<usize> = (0..5).collect();
        let result = lttb_with_indices(&pts, &idx, 10);
        assert_eq!(result.len(), 5);
        for (i, ((pt, src_idx), orig_pt)) in result.iter().zip(pts.iter()).enumerate() {
            assert_eq!(pt, orig_pt);
            assert_eq!(*src_idx, i);
        }
    }

    #[test]
    fn lttb_with_indices_preserves_source_indices_when_decimating() {
        // 10 points, target=5 — indices must map to the originals selected by LTTB.
        let pts: Vec<(f64, f64)> = (0..10).map(|i| (i as f64, (i as f64).sin())).collect();
        let idx: Vec<usize> = (0..10).collect();
        let result = lttb_with_indices(&pts, &idx, 5);
        assert_eq!(result.len(), 5);
        // First and last must be the original first/last.
        assert_eq!(result[0].1, 0, "first source index must be 0");
        assert_eq!(result[4].1, 9, "last source index must be 9");
        // Every source index must be within the original range.
        for &(_, src) in &result {
            assert!(src < 10, "source index out of range: {}", src);
        }
    }

    #[test]
    fn lttb_with_indices_custom_index_mapping() {
        // Simulate a sorted-then-remapped dataset: original indices 5,3,7,1,9.
        let pts = vec![(0.0, 1.0), (1.0, 2.0), (2.0, 3.0), (3.0, 2.0), (4.0, 1.0)];
        let idx = vec![5usize, 3, 7, 1, 9];
        let result = lttb_with_indices(&pts, &idx, 3);
        assert_eq!(result.len(), 3);
        // First must map to source 5, last to source 9.
        assert_eq!(result[0].1, 5);
        assert_eq!(result[2].1, 9);
    }

    #[test]
    fn lttb_with_indices_handles_empty_input() {
        let pts: Vec<(f64, f64)> = vec![];
        let idx: Vec<usize> = vec![];
        let result = lttb_with_indices(&pts, &idx, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn lttb_with_indices_single_point() {
        let pts = vec![(0.0, 1.0)];
        let idx = vec![42usize];
        let result = lttb_with_indices(&pts, &idx, 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, (0.0, 1.0));
        assert_eq!(result[0].1, 42);
    }
}
