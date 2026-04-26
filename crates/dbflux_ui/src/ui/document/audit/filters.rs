//! Audit event filters.
//!
//! Provides filter state management for the audit event viewer.

use dbflux_core::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use dbflux_core::observability::{
    AuditQuerySource, EventActorType, EventCategory, EventOutcome, EventSeverity, EventSourceId,
};

/// Filter state for audit event queries.
///
/// All fields are optional - `None` means no filter applied (show all).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditFilters {
    /// Start of time range (epoch ms). `None` = no lower bound.
    pub start_ms: Option<i64>,
    /// End of time range (epoch ms). `None` = no upper bound.
    pub end_ms: Option<i64>,
    /// Filter by severity level.
    pub level: Option<EventSeverity>,
    /// Filter by multiple severity levels (OR'd together). Takes precedence over `level`.
    pub levels: Option<Vec<EventSeverity>>,
    /// Filter by event category.
    pub category: Option<EventCategory>,
    /// Filter by multiple event categories (OR'd together). Takes precedence over `category`.
    pub categories: Option<Vec<EventCategory>>,
    /// Filter by event source ID (where event originated: ui, mcp, hook, etc.).
    pub source: Option<EventSourceId>,
    /// Filter by outcome.
    pub outcome: Option<EventOutcome>,
    /// Filter by multiple outcomes (OR'd together). Takes precedence over `outcome`.
    pub outcomes: Option<Vec<EventOutcome>>,
    /// Free-text search across summary, action, error_message, details.
    pub free_text: Option<String>,
    /// Filter by actor ID (partial match).
    pub actor: Option<String>,
    /// Filter by actor type (user, system, mcp_client, etc.).
    pub actor_type: Option<EventActorType>,
    /// Filter by connection ID.
    pub connection_id: Option<String>,
    /// Filter by driver ID.
    pub driver_id: Option<String>,
    /// Event source location for querying (internal SQLite, CloudWatch, Loki).
    pub event_source: AuditQuerySource,
    /// Filter by correlation_id to find related events (audit trail).
    pub correlation_id: Option<String>,
}

/// Time range option for quick selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(dead_code)]
pub enum TimeRange {
    Last5min,
    Last30min,
    LastHour,
    Last3Hours,
    #[default]
    Last12Hours,
    Custom,
}

/// Timestamp display and custom date parsing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampDisplayMode {
    #[default]
    Local,
    Utc,
}

impl TimeRange {
    /// Returns (start_ms, end_ms) tuple for this time range.
    pub fn to_filter_values(self) -> (Option<i64>, Option<i64>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match self {
            TimeRange::Last5min => (Some(now - 5 * 60 * 1000), None),
            TimeRange::Last30min => (Some(now - 30 * 60 * 1000), None),
            TimeRange::LastHour => (Some(now - 60 * 60 * 1000), None),
            TimeRange::Last3Hours => (Some(now - 3 * 60 * 60 * 1000), None),
            TimeRange::Last12Hours => (Some(now - 12 * 60 * 60 * 1000), None),
            TimeRange::Custom => (None, None),
        }
    }
}

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
            (TimeRange::Last5min, 5 * 60 * 1000),
            (TimeRange::Last30min, 30 * 60 * 1000),
            (TimeRange::LastHour, 60 * 60 * 1000),
            (TimeRange::Last3Hours, 3 * 60 * 60 * 1000),
            (TimeRange::Last12Hours, 12 * 60 * 60 * 1000),
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
