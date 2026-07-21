//! Audit event filters.
//!
//! Provides filter state management for the audit event viewer.
//! Time-range types live in `dbflux_components::common::time_range` and are
//! re-exported here for backward compatibility.

use dbflux_core::observability::{
    AuditQuerySource, EventActorType, EventCategory, EventOutcome, EventSeverity, EventSourceId,
};

pub use dbflux_components::common::time_range::{
    TimeRange, TimestampDisplayMode, format_timestamp_ms, timestamp_from_date_time,
    validate_custom_range_parts,
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
