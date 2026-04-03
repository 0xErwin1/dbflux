//! Audit event filters.
//!
//! Provides filter state management for the audit event viewer.

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
    /// Filter by event category.
    pub category: Option<EventCategory>,
    /// Filter by multiple event categories (OR'd together). Takes precedence over `category`.
    pub categories: Option<Vec<EventCategory>>,
    /// Filter by event source ID (where event originated: ui, mcp, hook, etc.).
    pub source: Option<EventSourceId>,
    /// Filter by outcome.
    pub outcome: Option<EventOutcome>,
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
    Last15min,
    LastHour,
    #[default]
    Last24h,
    Last7Days,
    Custom,
}

impl TimeRange {
    /// Returns (start_ms, end_ms) tuple for this time range.
    pub fn to_filter_values(self) -> (Option<i64>, Option<i64>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match self {
            TimeRange::Last15min => (Some(now - 15 * 60 * 1000), None),
            TimeRange::LastHour => (Some(now - 60 * 60 * 1000), None),
            TimeRange::Last24h => (Some(now - 24 * 60 * 60 * 1000), None),
            TimeRange::Last7Days => (Some(now - 7 * 24 * 60 * 60 * 1000), None),
            TimeRange::Custom => (None, None),
        }
    }
}
