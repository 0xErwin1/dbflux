//! Query types for the global audit system.
//!
//! These types are used to filter and paginate audit event queries.

use serde::{Deserialize, Serialize};

use super::types::{EventActorType, EventCategory, EventOutcome, EventSeverity, EventSourceId};

// ============================================================================
// Event Query
// ============================================================================

/// Filter for querying audit events.
///
/// All fields are optional. When a field is `None`, it is not used as a filter.
/// When a field has a value, events must match that value to be returned.
///
/// ## Usage
///
/// ```ignore
/// let filter = EventQuery {
///     level: Some(EventSeverity::Error),
///     category: Some(EventCategory::Query),
///     from_ts_ms: Some(1700000000000),
///     to_ts_ms: Some(1700100000000),
///     ..Default::default()
/// };
/// let events = audit_service.query_events(&filter)?;
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventQuery {
    /// Filter by event ID.
    pub id: Option<i64>,
    /// Filter by minimum timestamp (inclusive).
    pub from_ts_ms: Option<i64>,
    /// Filter by maximum timestamp (inclusive).
    pub to_ts_ms: Option<i64>,
    /// Filter by severity level.
    pub level: Option<EventSeverity>,
    /// Filter by event category.
    pub category: Option<EventCategory>,
    /// Filter by action (exact match).
    pub action: Option<String>,
    /// Filter by outcome.
    pub outcome: Option<EventOutcome>,
    /// Filter by actor type.
    pub actor_type: Option<EventActorType>,
    /// Filter by actor ID (exact match).
    pub actor_id: Option<String>,
    /// Filter by source ID.
    pub source_id: Option<EventSourceId>,
    /// Filter by connection ID.
    pub connection_id: Option<String>,
    /// Filter by driver ID.
    pub driver_id: Option<String>,
    /// Filter by object type.
    pub object_type: Option<String>,
    /// Free-text search across summary and details_json.
    pub free_text: Option<String>,
    /// Maximum number of events to return.
    pub limit: Option<usize>,
    /// Number of events to skip (for pagination).
    pub offset: Option<usize>,
}

impl EventQuery {
    /// Creates a new query with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the time range filter.
    pub fn with_time_range(mut self, from_ts_ms: i64, to_ts_ms: i64) -> Self {
        self.from_ts_ms = Some(from_ts_ms);
        self.to_ts_ms = Some(to_ts_ms);
        self
    }

    /// Sets the severity filter.
    pub fn with_level(mut self, level: EventSeverity) -> Self {
        self.level = Some(level);
        self
    }

    /// Sets the category filter.
    pub fn with_category(mut self, category: EventCategory) -> Self {
        self.category = Some(category);
        self
    }

    /// Sets the outcome filter.
    pub fn with_outcome(mut self, outcome: EventOutcome) -> Self {
        self.outcome = Some(outcome);
        self
    }

    /// Sets the limit.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the offset for pagination.
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Adds a free-text search filter.
    pub fn with_free_text(mut self, text: impl Into<String>) -> Self {
        self.free_text = Some(text.into());
        self
    }

    /// Returns true if this query has any filters set.
    pub fn has_filters(&self) -> bool {
        self.id.is_some()
            || self.from_ts_ms.is_some()
            || self.to_ts_ms.is_some()
            || self.level.is_some()
            || self.category.is_some()
            || self.action.is_some()
            || self.outcome.is_some()
            || self.actor_type.is_some()
            || self.actor_id.is_some()
            || self.source_id.is_some()
            || self.connection_id.is_some()
            || self.driver_id.is_some()
            || self.object_type.is_some()
            || self.free_text.is_some()
    }
}

// ============================================================================
// Event Page
// ============================================================================

/// A page of audit events with pagination information.
///
/// Returned when querying events with pagination support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPage {
    /// The events in this page.
    pub events: Vec<super::types::EventRecord>,
    /// Total number of events matching the query (if known).
    pub total: Option<usize>,
    /// Whether there are more events after this page.
    pub has_more: bool,
    /// The offset of the first event in this page.
    pub offset: usize,
    /// The limit used for this query.
    pub limit: usize,
}

impl EventPage {
    /// Creates a new page of events.
    pub fn new(
        events: Vec<super::types::EventRecord>,
        total: Option<usize>,
        has_more: bool,
        offset: usize,
        limit: usize,
    ) -> Self {
        Self {
            events,
            total,
            has_more,
            offset,
            limit,
        }
    }

    /// Returns the number of events in this page.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns true if this page has no events.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

// ============================================================================
// Event Detail
// ============================================================================

/// Full event detail with formatted/parsed fields.
///
/// This extends `EventRecord` with additional computed or formatted fields
/// that are useful for display but not for storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDetail {
    /// The raw event record.
    #[serde(flatten)]
    pub record: super::types::EventRecord,
    /// Parsed details object (if details_json is valid JSON).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parsed_details: Option<serde_json::Value>,
    /// Formatted timestamp string for display.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formatted_ts: Option<String>,
    /// Formatted duration string for display (e.g., "42ms", "1.5s").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formatted_duration: Option<String>,
}

impl EventDetail {
    /// Creates a new event detail from a record.
    pub fn from_record(record: super::types::EventRecord) -> Self {
        let parsed_details = record
            .details_json
            .as_ref()
            .and_then(|json| serde_json::from_str(json).ok());

        let formatted_ts = Some(format_timestamp(record.ts_ms));

        let formatted_duration = record.duration_ms.map(format_duration);

        Self {
            record,
            parsed_details,
            formatted_ts,
            formatted_duration,
        }
    }

    /// Returns the event summary with fallback.
    pub fn summary(&self) -> &str {
        if self.record.summary.is_empty() {
            &self.record.action
        } else {
            &self.record.summary
        }
    }
}

/// Format a timestamp in milliseconds to a human-readable string.
fn format_timestamp(ts_ms: i64) -> String {
    let secs = ts_ms / 1000;
    let naive = chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.naive_utc())
        .unwrap_or_else(|| chrono::Utc::now().naive_utc());
    naive.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

/// Format a duration in milliseconds to a human-readable string.
fn format_duration(ms: i64) -> String {
    if ms < 1000 {
        format!("{}ms", ms)
    } else if ms < 60000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{:.1}m", ms as f64 / 60000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::EventRecord;
    use super::*;

    #[test]
    fn test_event_query_has_filters() {
        let empty = EventQuery::default();
        assert!(!empty.has_filters());

        let with_level = EventQuery::new().with_level(EventSeverity::Error);
        assert!(with_level.has_filters());

        let with_time = EventQuery::new().with_time_range(0, 1000);
        assert!(with_time.has_filters());
    }

    #[test]
    fn test_event_page() {
        let page = EventPage::new(vec![], Some(100), true, 0, 50);
        assert!(page.has_more);
        assert_eq!(page.len(), 0);

        let page_full = EventPage::new(vec![], Some(50), false, 0, 50);
        assert!(!page_full.has_more);
    }

    #[test]
    fn test_event_detail_from_record() {
        let record = EventRecord::new(
            1700000000000,
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Success,
        )
        .with_action("test")
        .with_summary("Test summary")
        .with_duration_ms(1500);

        let detail = EventDetail::from_record(record);

        assert_eq!(detail.summary(), "Test summary");
        assert_eq!(detail.formatted_duration, Some("1.5s".to_string()));
        assert!(detail.parsed_details.is_none());
    }

    #[test]
    fn test_event_detail_with_json_details() {
        let record = EventRecord::new(
            1700000000000,
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Success,
        )
        .with_action("test")
        .with_details_json(r#"{"key": "value"}"#);

        let detail = EventDetail::from_record(record);

        assert!(detail.parsed_details.is_some());
        let parsed = detail.parsed_details.unwrap();
        assert_eq!(parsed.get("key").unwrap(), "value");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(42), "42ms");
        assert_eq!(format_duration(1500), "1.5s");
        assert_eq!(format_duration(90000), "1.5m");
    }
}
