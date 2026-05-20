//! Time range selection state and timestamp helpers.
//!
//! Reusable types extracted from the audit document so they can be embedded
//! in any query-panel that needs a "time window" control (e.g. InfluxDB editor).

use dbflux_core::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

/// Quick-select presets for a time window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeRange {
    Last15min,
    LastHour,
    Last6Hours,
    #[default]
    Last24Hours,
    Last7Days,
    Custom,
}

/// Controls how timestamps are displayed and how custom date strings are parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampDisplayMode {
    #[default]
    Local,
    Utc,
}

impl TimeRange {
    /// Returns `(start_ms, end_ms)` for this preset.
    ///
    /// `Custom` returns `(None, None)` — the caller must supply absolute bounds
    /// through the date picker.
    pub fn to_filter_values(self) -> (Option<i64>, Option<i64>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match self {
            TimeRange::Last15min => (Some(now - 15 * 60_000), None),
            TimeRange::LastHour => (Some(now - 60 * 60_000), None),
            TimeRange::Last6Hours => (Some(now - 6 * 60 * 60_000), None),
            TimeRange::Last24Hours => (Some(now - 24 * 60 * 60_000), None),
            TimeRange::Last7Days => (Some(now - 7 * 24 * 60 * 60_000), None),
            TimeRange::Custom => (None, None),
        }
    }
}

/// Format an epoch-millisecond timestamp for display.
pub fn format_timestamp_ms(ms: i64, mode: TimestampDisplayMode) -> String {
    let Some(utc) = DateTime::<Utc>::from_timestamp_millis(ms) else {
        return ms.to_string();
    };

    match mode {
        TimestampDisplayMode::Local => utc
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string(),
        TimestampDisplayMode::Utc => utc.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
    }
}

/// Convert a `NaiveDate` + hour/minute pair into an epoch-millisecond value.
///
/// Interprets the inputs according to `mode` — UTC leaves the value
/// unambiguous; Local can fail on DST transitions.
pub fn timestamp_from_date_time(
    date: NaiveDate,
    hour: u32,
    minute: u32,
    mode: TimestampDisplayMode,
) -> Result<i64, String> {
    let time = NaiveTime::from_hms_opt(hour, minute, 0)
        .ok_or_else(|| "Time selection is invalid".to_string())?;
    let naive = NaiveDateTime::new(date, time);

    let timestamp = match mode {
        TimestampDisplayMode::Utc => Utc.from_utc_datetime(&naive).timestamp_millis(),
        TimestampDisplayMode::Local => Local
            .from_local_datetime(&naive)
            .single()
            .ok_or_else(|| "Local time is ambiguous or invalid".to_string())?
            .timestamp_millis(),
    };

    Ok(timestamp)
}

/// Validate and convert split date-picker fields into a `(start_ms, end_ms)` pair.
///
/// Returns an error when the resulting start is after the resulting end.
#[allow(clippy::too_many_arguments)]
pub fn validate_custom_range_parts(
    start_date: NaiveDate,
    start_hour: u32,
    start_minute: u32,
    end_date: NaiveDate,
    end_hour: u32,
    end_minute: u32,
    mode: TimestampDisplayMode,
) -> Result<(i64, i64), String> {
    let start_ms = timestamp_from_date_time(start_date, start_hour, start_minute, mode)
        .map_err(|error| format!("Invalid start time: {error}"))?;
    let end_ms = timestamp_from_date_time(end_date, end_hour, end_minute, mode)
        .map_err(|error| format!("Invalid end time: {error}"))?;

    if start_ms > end_ms {
        return Err("Start time must be before end time".to_string());
    }

    Ok((start_ms, end_ms))
}

#[cfg(test)]
mod tests {
    use super::{
        TimeRange, TimestampDisplayMode, format_timestamp_ms, validate_custom_range_parts,
    };
    use dbflux_core::chrono::NaiveDate;

    #[test]
    fn time_range_presets_map_to_expected_windows() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let cases = [
            (TimeRange::Last15min, 15 * 60_000),
            (TimeRange::LastHour, 60 * 60_000),
            (TimeRange::Last6Hours, 6 * 60 * 60_000),
            (TimeRange::Last24Hours, 24 * 60 * 60_000),
            (TimeRange::Last7Days, 7 * 24 * 60 * 60_000),
        ];

        for (range, expected_ms) in cases {
            let (start_ms, end_ms) = range.to_filter_values();
            let actual_ms = now - start_ms.expect("preset should set start");

            assert!(end_ms.is_none());
            assert!((expected_ms - actual_ms).abs() < 1000);
        }
    }

    #[test]
    fn utc_timestamp_format_includes_date_and_time() {
        let ms = 1_777_034_096_000;
        let formatted = format_timestamp_ms(ms, TimestampDisplayMode::Utc);

        assert_eq!(formatted, "2026-04-24 12:34:56.000");
    }

    #[test]
    fn custom_range_parts_require_ordered_date_time_values() {
        let start = NaiveDate::from_ymd_opt(2026, 4, 24).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 4, 24).unwrap();

        let result =
            validate_custom_range_parts(start, 12, 35, end, 12, 34, TimestampDisplayMode::Utc);

        assert_eq!(
            result,
            Err("Start time must be before end time".to_string())
        );
    }
}
