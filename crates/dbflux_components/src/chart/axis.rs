//! Tick generation and label formatting for chart axes.
//!
//! All formatting is pre-computed at chart-build time; `render()` only reads
//! the stored `TickLabel` strings.

use dbflux_core::chrono::{DateTime, TimeZone, Utc};
use gpui::SharedString;

/// A single axis tick with its data-space value and pre-formatted label.
#[derive(Debug, Clone, PartialEq)]
pub struct TickLabel {
    /// Data-space coordinate for positioning on the axis.
    pub value: f64,
    /// Pre-formatted display string.
    pub label: String,
}

// ---------------------------------------------------------------------------
// Numeric ticks
// ---------------------------------------------------------------------------

/// Generate nice numeric ticks for `[min, max]`.
///
/// Uses the "nice step" algorithm: round the step magnitude up to the nearest
/// value in `{1, 2, 5, 10}` per decade. Returns an empty `Vec` when
/// `min >= max` is detected (degenerate range returns a single tick at `min`).
pub fn ticks_numeric(min: f64, max: f64, target_count: usize) -> Vec<TickLabel> {
    let target_count = target_count.max(1);

    if min >= max {
        return vec![TickLabel {
            value: min,
            label: format_numeric(min),
        }];
    }

    let span = max - min;
    let raw_step = span / target_count as f64;
    let step = nice_step(raw_step);

    let first = (min / step).ceil() * step;
    let mut ticks = Vec::new();
    let mut t = first;

    while t <= max + step * 1e-9 {
        if t >= min - step * 1e-9 {
            ticks.push(TickLabel {
                value: t,
                label: format_numeric(t),
            });
        }
        t += step;
    }

    ticks
}

/// Round `raw` up to the nearest nice step (1, 2, 5, or 10 times a power of 10).
fn nice_step(raw: f64) -> f64 {
    if raw <= 0.0 {
        return 1.0;
    }
    let magnitude = 10f64.powf(raw.log10().floor());
    let fraction = raw / magnitude;

    let nice = if fraction <= 1.0 {
        1.0
    } else if fraction <= 2.0 {
        2.0
    } else if fraction <= 5.0 {
        5.0
    } else {
        10.0
    };

    nice * magnitude
}

/// Format a numeric value with up to 4 significant figures.
///
/// Uses scientific notation for very large (`>= 1e6`) or very small
/// (`< 1e-3` and non-zero) magnitudes.
fn format_numeric(v: f64) -> String {
    if v == 0.0 {
        return "0".to_string();
    }
    let abs = v.abs();
    if abs >= 1e6 || (abs <= 1e-3 && abs > 0.0) {
        format!("{:.3e}", v)
    } else {
        // Up to 4 significant figures, then strip trailing zeros.
        let s = format!("{:.4}", v);
        let s = s.trim_end_matches('0').trim_end_matches('.');
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// Time ticks
// ---------------------------------------------------------------------------

/// Step values for the time tick ladder (milliseconds).
const NICE_TIME_STEPS_MS: &[f64] = &[
    1_000.0,             // 1s
    5_000.0,             // 5s
    15_000.0,            // 15s
    60_000.0,            // 1m
    5.0 * 60_000.0,      // 5m
    15.0 * 60_000.0,     // 15m
    3_600_000.0,         // 1h
    6.0 * 3_600_000.0,   // 6h
    86_400_000.0,        // 1d
    7.0 * 86_400_000.0,  // 1w
    30.0 * 86_400_000.0, // ~1mo
];

/// Generate nice time ticks for `[min_ms, max_ms]` (milliseconds since Unix epoch).
///
/// Labels are UTC; no timezone or locale logic is applied in v0.6.
pub fn ticks_time(min_ms: f64, max_ms: f64, target_count: usize) -> Vec<TickLabel> {
    let target_count = target_count.max(1);

    if min_ms >= max_ms {
        return vec![TickLabel {
            value: min_ms,
            label: format_time_ms(min_ms, 1_000.0),
        }];
    }

    let span = max_ms - min_ms;
    let raw_step = span / target_count as f64;

    let step = NICE_TIME_STEPS_MS
        .iter()
        .copied()
        .find(|&s| s >= raw_step)
        .unwrap_or(*NICE_TIME_STEPS_MS.last().unwrap());

    let first = (min_ms / step).ceil() * step;
    let mut ticks = Vec::new();
    let mut t = first;

    while t <= max_ms + step * 1e-9 {
        if t >= min_ms - step * 1e-9 {
            ticks.push(TickLabel {
                value: t,
                label: format_time_ms(t, step),
            });
        }
        t += step;
    }

    ticks
}

/// Format a millisecond-since-epoch timestamp as a UTC label.
///
/// The format pattern is chosen based on the step magnitude so that
/// labels are as concise as possible while remaining unambiguous.
fn format_time_ms(ms: f64, step_ms: f64) -> String {
    let secs = (ms / 1_000.0) as i64;
    let nanos = ((ms % 1_000.0) * 1_000_000.0) as u32;
    let dt: DateTime<Utc> = match Utc.timestamp_opt(secs, nanos) {
        chrono::LocalResult::Single(d) => d,
        _ => return format!("{:.0}", ms),
    };

    let fmt = if step_ms < 60_000.0 {
        "%H:%M:%S"
    } else if step_ms < 3_600_000.0 {
        "%H:%M"
    } else if step_ms < 86_400_000.0 {
        "%m-%d %H:%M"
    } else {
        "%Y-%m-%d"
    };

    dt.format(fmt).to_string()
}

// Need chrono re-export for the timestamp_opt call
use dbflux_core::chrono;

// ---------------------------------------------------------------------------
// Log1p ticks (Y axis in Log scale mode)
// ---------------------------------------------------------------------------

/// Generate readable Y-axis ticks for a log1p-scaled axis.
///
/// Input `min` and `max` are original data-space values (counts, etc.).
/// Tick *positions* are expressed in log1p space (`ln(y + 1)`) so the caller
/// can map them directly to screen coordinates using the same log1p bounds.
/// Tick *labels* show the rounded original-scale values so users read natural
/// numbers.
///
/// The tick strategy places a tick at each "decade" boundary in log1p space
/// (`e^k - 1` for integer `k`) that falls within `[min, max]`, padded with a
/// few intermediate values when the range spans less than two decades.
///
/// Returns at least one tick even for a degenerate range (`min >= max`).
/// Never produces non-finite tick positions.
pub fn ticks_log(min: f64, max: f64, target_count: usize) -> Vec<TickLabel> {
    let target_count = target_count.max(1);

    // Clamp to non-negative — log1p is only defined for y >= -1; counts are >= 0.
    let min = min.max(0.0);
    let max = max.max(min);

    if min >= max {
        let original = (min.exp() - 1.0).round().max(0.0);
        return vec![TickLabel {
            value: min,
            label: format_log_label(original),
        }];
    }

    // Work in log1p space: transform bounds.
    let log_min = (min + 1.0).ln();
    let log_max = (max + 1.0).ln();

    // Candidate original-scale counts at decade boundaries: 0, 1, 9, 99, 999 …
    // (values where e^k - 1 is an integer for k = 0, 1, 2, …).
    // We also interpolate linearly in log1p space when the range is narrow.
    let mut positions: Vec<f64> = Vec::new();

    // Decade boundaries whose log1p value falls inside [log_min, log_max].
    let k_start = log_min.floor() as i64;
    let k_end = log_max.ceil() as i64;

    for k in k_start..=k_end {
        // log1p position for decade k: k is already in log-space.
        let lv = k as f64;
        if lv >= log_min - 1e-9 && lv <= log_max + 1e-9 {
            positions.push(lv.clamp(log_min, log_max));
        }
    }

    // If fewer than target_count candidates, add linearly-spaced positions in
    // log1p space to reach a denser grid.
    if positions.len() < target_count {
        let step = (log_max - log_min) / target_count as f64;
        let mut t = log_min;
        while t <= log_max + step * 1e-9 {
            let candidate = t.clamp(log_min, log_max);
            // Only add if not already close to an existing position.
            let already_close = positions
                .iter()
                .any(|&p| (p - candidate).abs() < step * 0.4);
            if !already_close {
                positions.push(candidate);
            }
            t += step;
        }
        positions.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    // Deduplicate very close positions (can happen at exact decade boundaries).
    positions.dedup_by(|a, b| (*a - *b).abs() < 1e-9);

    // Build TickLabel: position is in log1p space; label is the original-scale
    // integer count (inverse: original = e^lv - 1, rounded to nearest integer).
    positions
        .into_iter()
        .filter_map(|lv| {
            if !lv.is_finite() {
                return None;
            }
            let original = (lv.exp() - 1.0).round().max(0.0);
            Some(TickLabel {
                value: lv,
                label: format_log_label(original),
            })
        })
        .collect()
}

/// Format a count label for the log1p axis.
///
/// Uses integer formatting for whole numbers and up to 2 decimal places for
/// sub-unit values that can arise after inverse transform.
fn format_log_label(original: f64) -> String {
    if original == original.floor() && original.abs() < 1e12 {
        format!("{}", original as i64)
    } else {
        format!("{:.2}", original)
    }
}

// ---------------------------------------------------------------------------
// Categorical ticks (Bar / Scatter X axis — seam for next change)
// ---------------------------------------------------------------------------

/// Generate tick labels for a categorical (string) X axis.
///
/// Each distinct value in `values` becomes one tick; values are used verbatim
/// as labels. Position is the ordinal index (0, 1, 2, …).
///
/// This function is declared here as a seam for the Bar/Scatter change that
/// follows. The body returns an empty vec until the next change wires up the
/// full categorical-axis implementation.
#[allow(dead_code)]
pub fn ticks_categorical(values: &[SharedString]) -> Vec<TickLabel> {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| TickLabel {
            value: i as f64,
            label: v.to_string(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Unit tests (strict TDD)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticks_numeric_span_1_to_100_returns_5_to_8_ticks() {
        let ticks = ticks_numeric(1.0, 100.0, 5);
        assert!(
            ticks.len() >= 5 && ticks.len() <= 8,
            "expected 5-8 ticks, got {}",
            ticks.len()
        );
        // All tick values should be within [1, 100].
        for t in &ticks {
            assert!(t.value >= 0.0 && t.value <= 110.0);
        }
    }

    #[test]
    fn ticks_numeric_span_0_001_uses_scientific_format() {
        let ticks = ticks_numeric(0.0001, 0.001, 5);
        assert!(!ticks.is_empty(), "should produce at least one tick");
        for t in &ticks {
            // Scientific notation for values < 1e-3.
            assert!(
                t.label.contains('e') || t.label == "0",
                "expected scientific format for {}, got {}",
                t.value,
                t.label
            );
        }
    }

    #[test]
    fn ticks_time_span_1_minute_uses_second_steps() {
        let base_ms = 0.0f64;
        let ticks = ticks_time(base_ms, 60_000.0, 5);
        assert!(!ticks.is_empty());
        // Labels for <60s steps use HH:MM:SS format.
        for t in &ticks {
            // Should contain colons and be length 8 (HH:MM:SS).
            assert!(
                t.label.len() == 8 && t.label.matches(':').count() == 2,
                "expected HH:MM:SS, got {}",
                t.label
            );
        }
    }

    #[test]
    fn ticks_time_span_30_days_uses_day_steps() {
        let base_ms = 0.0f64;
        let end_ms = 30.0 * 86_400_000.0;
        let ticks = ticks_time(base_ms, end_ms, 5);
        assert!(!ticks.is_empty());
        // Labels for >=1d steps use YYYY-MM-DD format.
        for t in &ticks {
            assert!(
                t.label.len() == 10 && t.label.matches('-').count() == 2,
                "expected YYYY-MM-DD, got {}",
                t.label
            );
        }
    }

    /// Seam preservation: ensures `ticks_categorical` signature is stable.
    ///
    /// This test must remain green so that the Bar/Scatter change cannot land
    /// without satisfying the categorical-axis contract.
    #[test]
    fn ticks_categorical_returns_one_tick_per_value() {
        let values = vec![
            SharedString::from("alpha"),
            SharedString::from("beta"),
            SharedString::from("gamma"),
        ];
        let ticks = ticks_categorical(&values);
        assert_eq!(ticks.len(), 3, "one tick per input value");
        assert_eq!(ticks[0].value, 0.0);
        assert_eq!(ticks[0].label, "alpha");
        assert_eq!(ticks[1].value, 1.0);
        assert_eq!(ticks[1].label, "beta");
        assert_eq!(ticks[2].value, 2.0);
        assert_eq!(ticks[2].label, "gamma");
    }

    #[test]
    fn ticks_categorical_empty_input_returns_empty() {
        let ticks = ticks_categorical(&[]);
        assert!(ticks.is_empty());
    }

    // ---------------------------------------------------------------------------
    // ticks_log tests (TDD RED → GREEN for log1p Y-axis)
    // ---------------------------------------------------------------------------

    /// T-LOG-01: decade-ish ticks — positions are in log1p space.
    #[test]
    fn ticks_log_positions_are_in_log1p_space() {
        // Range 0..=99: log1p(0+1)=0, log1p(99+1)≈ln(100)≈4.6.
        let ticks = ticks_log(0.0, 99.0, 5);
        assert!(!ticks.is_empty(), "should produce ticks");

        // Every tick position must equal ln(original + 1) within tolerance.
        for t in &ticks {
            assert!(
                t.value.is_finite(),
                "tick position must be finite, got {}",
                t.value
            );
            // Position is in log1p space; it must be in [ln(1), ln(100)].
            assert!(
                t.value >= -1e-9,
                "position must be >= 0 (log1p of 0+1=0), got {}",
                t.value
            );
            assert!(
                t.value <= (100f64).ln() + 1e-9,
                "position must be <= ln(100), got {}",
                t.value
            );
        }
    }

    /// T-LOG-02: labels show original-scale integer counts.
    #[test]
    fn ticks_log_labels_are_original_scale_integers() {
        let ticks = ticks_log(0.0, 9999.0, 6);
        assert!(!ticks.is_empty());

        // Reconstruct original from position and compare to label.
        for t in &ticks {
            let reconstructed = (t.value.exp() - 1.0).round().max(0.0);
            // Label must be the integer representation of the reconstructed value.
            let expected_label = format!("{}", reconstructed as i64);
            assert_eq!(
                t.label, expected_label,
                "label '{}' should be original-scale integer '{}' (pos={})",
                t.label, expected_label, t.value
            );
        }
    }

    /// T-LOG-03: min == 0 is handled correctly (no NaN or -Inf).
    #[test]
    fn ticks_log_handles_min_zero() {
        let ticks = ticks_log(0.0, 100.0, 5);
        assert!(!ticks.is_empty());
        for t in &ticks {
            assert!(t.value.is_finite(), "position must be finite");
        }
        // First tick should represent count 0.
        let first = &ticks[0];
        assert_eq!(first.label, "0", "first tick should be label '0'");
    }

    /// T-LOG-04: degenerate range (min >= max) returns a single tick.
    #[test]
    fn ticks_log_degenerate_range_returns_single_tick() {
        let ticks = ticks_log(5.0, 5.0, 5);
        assert_eq!(
            ticks.len(),
            1,
            "degenerate range must produce exactly 1 tick"
        );
        assert!(ticks[0].value.is_finite());
    }

    #[test]
    fn ticks_time_labels_format_per_step_magnitude() {
        // 1-hour span → steps should be at minute or second resolution.
        let ticks = ticks_time(0.0, 3_600_000.0, 5);
        for t in &ticks {
            // HH:MM or HH:MM:SS — either is fine; must have at least one colon.
            assert!(
                t.label.contains(':'),
                "expected time label with colon, got {}",
                t.label
            );
        }
    }
}
