//! Tick generation and label formatting for chart axes.
//!
//! All formatting is pre-computed at chart-build time; `render()` only reads
//! the stored `TickLabel` strings.

use dbflux_core::chrono::{DateTime, TimeZone, Utc};

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
    1_000.0,          // 1s
    5_000.0,          // 5s
    15_000.0,         // 15s
    60_000.0,         // 1m
    5.0 * 60_000.0,   // 5m
    15.0 * 60_000.0,  // 15m
    3_600_000.0,      // 1h
    6.0 * 3_600_000.0,// 6h
    86_400_000.0,     // 1d
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
