//! Observability types for the global audit system.
//!
//! This module defines the core event types used across all DBFlux components
//! for unified audit logging. Events flow from service layers to storage via
//! the `EventSink` trait implemented by `AuditService`.
//!
//! ## Event Flow
//!
//! ```text
//! [Service/Executor layers] --emit--> [EventSink trait] --record()--> [AuditService] --append--> [SQLite]
//! ```

use serde::{Deserialize, Serialize};

// ============================================================================
// Event Severity
// ============================================================================

/// Severity level for audit events.
///
/// Used to filter events in the audit view and determine display styling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum EventSeverity {
    /// Detailed tracing information for deep debugging.
    Trace,
    /// Debug-level information for development.
    Debug,
    /// Informational events that represent normal operation.
    #[default]
    Info,
    /// Warning events indicating potential issues.
    Warn,
    /// Error events representing failures.
    Error,
    /// Fatal events representing critical failures.
    Fatal,
}

impl EventSeverity {
    /// Returns the string representation used in storage and APIs.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventSeverity::Trace => "trace",
            EventSeverity::Debug => "debug",
            EventSeverity::Info => "info",
            EventSeverity::Warn => "warn",
            EventSeverity::Error => "error",
            EventSeverity::Fatal => "fatal",
        }
    }

    /// Parse from a string representation.
    pub fn from_str_repr(s: &str) -> Option<Self> {
        match s {
            "trace" => Some(EventSeverity::Trace),
            "debug" => Some(EventSeverity::Debug),
            "info" => Some(EventSeverity::Info),
            "warn" => Some(EventSeverity::Warn),
            "error" => Some(EventSeverity::Error),
            "fatal" => Some(EventSeverity::Fatal),
            _ => None,
        }
    }
}

// ============================================================================
// Event Category
// ============================================================================

/// Category of an audit event.
///
/// Describes the general domain of the event for filtering and organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum EventCategory {
    /// Configuration changes (profiles, auth, hooks, etc.).
    Config,
    /// Connection lifecycle events.
    Connection,
    /// Query execution events.
    Query,
    /// Hook execution events.
    Hook,
    /// Script execution events.
    Script,
    /// System-level events.
    #[default]
    System,
    /// MCP (Model Context Protocol) governance events.
    Mcp,
    /// Governance policy evaluation events.
    Governance,
}

impl EventCategory {
    /// Returns the string representation used in storage and APIs.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventCategory::Config => "config",
            EventCategory::Connection => "connection",
            EventCategory::Query => "query",
            EventCategory::Hook => "hook",
            EventCategory::Script => "script",
            EventCategory::System => "system",
            EventCategory::Mcp => "mcp",
            EventCategory::Governance => "governance",
        }
    }

    /// Parse from a string representation.
    pub fn from_str_repr(s: &str) -> Option<Self> {
        match s {
            "config" => Some(EventCategory::Config),
            "connection" => Some(EventCategory::Connection),
            "query" => Some(EventCategory::Query),
            "hook" => Some(EventCategory::Hook),
            "script" => Some(EventCategory::Script),
            "system" => Some(EventCategory::System),
            "mcp" => Some(EventCategory::Mcp),
            "governance" => Some(EventCategory::Governance),
            _ => None,
        }
    }
}

// ============================================================================
// Event Outcome
// ============================================================================

/// Outcome of an audited action.
///
/// Describes whether the action succeeded, failed, was cancelled, or is pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum EventOutcome {
    /// Action completed successfully.
    #[default]
    Success,
    /// Action failed.
    Failure,
    /// Action was cancelled before completion.
    Cancelled,
    /// Action is still in progress.
    Pending,
}

impl EventOutcome {
    /// Returns the string representation used in storage and APIs.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventOutcome::Success => "success",
            EventOutcome::Failure => "failure",
            EventOutcome::Cancelled => "cancelled",
            EventOutcome::Pending => "pending",
        }
    }

    /// Parse from a string representation.
    pub fn from_str_repr(s: &str) -> Option<Self> {
        match s {
            "success" => Some(EventOutcome::Success),
            "failure" => Some(EventOutcome::Failure),
            "cancelled" => Some(EventOutcome::Cancelled),
            "pending" => Some(EventOutcome::Pending),
            _ => None,
        }
    }
}

// ============================================================================
// Event Actor Type
// ============================================================================

/// Type of actor that initiated the audited action.
///
/// Describes what kind of entity triggered the event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum EventActorType {
    /// Human user acting through the UI.
    User,
    /// System-level operation (background tasks, timers, etc.).
    #[default]
    System,
    /// The application itself acting autonomously.
    App,
    /// MCP client/agent acting on behalf of a user.
    McpClient,
    /// A hook script or process.
    Hook,
    /// A user script (Lua, Python, Bash, etc.).
    Script,
}

impl EventActorType {
    /// Returns the string representation used in storage and APIs.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventActorType::User => "user",
            EventActorType::System => "system",
            EventActorType::App => "app",
            EventActorType::McpClient => "mcp_client",
            EventActorType::Hook => "hook",
            EventActorType::Script => "script",
        }
    }

    /// Parse from a string representation.
    pub fn from_str_repr(s: &str) -> Option<Self> {
        match s {
            "user" => Some(EventActorType::User),
            "system" => Some(EventActorType::System),
            "app" => Some(EventActorType::App),
            "mcp_client" => Some(EventActorType::McpClient),
            "hook" => Some(EventActorType::Hook),
            "script" => Some(EventActorType::Script),
            _ => None,
        }
    }
}

// ============================================================================
// Event Source ID
// ============================================================================

/// Source identifier for an audit event.
///
/// Indicates where the event originated from in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum EventSourceId {
    /// Local UI-initiated action.
    Local,
    /// MCP runtime (AI client).
    Mcp,
    /// Hook execution.
    Hook,
    /// Script execution.
    Script,
    /// System-level event.
    #[default]
    System,
}

impl EventSourceId {
    /// Returns the string representation used in storage and APIs.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventSourceId::Local => "local",
            EventSourceId::Mcp => "mcp",
            EventSourceId::Hook => "hook",
            EventSourceId::Script => "script",
            EventSourceId::System => "system",
        }
    }

    /// Parse from a string representation.
    pub fn from_str_repr(s: &str) -> Option<Self> {
        match s {
            "local" => Some(EventSourceId::Local),
            "mcp" => Some(EventSourceId::Mcp),
            "hook" => Some(EventSourceId::Hook),
            "script" => Some(EventSourceId::Script),
            "system" => Some(EventSourceId::System),
            _ => None,
        }
    }
}

// ============================================================================
// Event Object Reference
// ============================================================================

/// Reference to an object affected by or related to an audit event.
///
/// Used to track what database objects (tables, collections, etc.) were
/// involved in the audited action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventObjectRef {
    /// Type of the object (e.g., "table", "collection", "index").
    pub object_type: String,
    /// Identifier of the specific object.
    pub object_id: String,
}

impl EventObjectRef {
    /// Creates a new object reference.
    pub fn new(object_type: impl Into<String>, object_id: impl Into<String>) -> Self {
        Self {
            object_type: object_type.into(),
            object_id: object_id.into(),
        }
    }
}

// ============================================================================
// Event Record
// ============================================================================

/// Complete audit event record.
///
/// This is the full event structure stored in and retrieved from the audit log.
/// All fields are optional unless noted otherwise.
///
/// ## Fields
///
/// - `id`: Unique identifier (assigned by storage on insert)
/// - `ts_ms`: Timestamp in milliseconds since epoch
/// - `level`: Severity level (required)
/// - `category`: Event category (required)
/// - `action`: Action identifier (e.g., "query_execute", "hook_post_connect")
/// - `outcome`: Outcome of the action (required)
/// - `actor_type`: Type of actor that initiated the action (required)
/// - `actor_id`: Identifier of the actor (optional - may be null for system events)
/// - `source_id`: Source system that generated the event (required)
/// - `connection_id`: Connection profile ID if applicable
/// - `database_name`: Target database name if applicable
/// - `driver_id`: Driver identifier (e.g., "postgres", "mongodb")
/// - `object_type`: Type of object affected
/// - `object_id`: ID of object affected
/// - `summary`: Human-readable summary of the event
/// - `details_json`: Additional structured details as JSON
/// - `error_code`: Error code if the outcome was failure
/// - `error_message`: Error message if the outcome was failure
/// - `duration_ms`: Duration of the action in milliseconds
/// - `session_id`: Session ID for correlation
/// - `correlation_id`: Correlation ID for tracing related events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    /// Unique identifier (assigned by storage on insert, `None` for new events).
    pub id: Option<i64>,
    /// Timestamp in milliseconds since Unix epoch.
    pub ts_ms: i64,
    /// Severity level.
    pub level: EventSeverity,
    /// Event category.
    pub category: EventCategory,
    /// Action identifier (e.g., "query_execute", "hook_post_connect").
    pub action: String,
    /// Outcome of the action.
    pub outcome: EventOutcome,
    /// Type of actor that initiated the action.
    pub actor_type: EventActorType,
    /// Identifier of the actor (optional for system events).
    pub actor_id: Option<String>,
    /// Source system that generated the event.
    pub source_id: EventSourceId,
    /// Connection profile ID if applicable.
    pub connection_id: Option<String>,
    /// Target database name if applicable.
    pub database_name: Option<String>,
    /// Driver identifier (e.g., "postgres", "mongodb").
    pub driver_id: Option<String>,
    /// Type of object affected (e.g., "table", "collection").
    pub object_type: Option<String>,
    /// Identifier of the specific object affected.
    pub object_id: Option<String>,
    /// Human-readable summary of the event.
    pub summary: String,
    /// Additional structured details as JSON string.
    pub details_json: Option<String>,
    /// Error code if the outcome was failure.
    pub error_code: Option<String>,
    /// Error message if the outcome was failure.
    pub error_message: Option<String>,
    /// Duration of the action in milliseconds.
    pub duration_ms: Option<i64>,
    /// Session ID for correlating events within a session.
    pub session_id: Option<String>,
    /// Correlation ID for tracing related events across components.
    pub correlation_id: Option<String>,
}

impl EventRecord {
    /// Creates a new event record with required fields set to defaults.
    ///
    /// Required fields that must be set afterwards:
    /// - `action` (required, use `with_action()`)
    /// - `summary` (required, use `with_summary()`)
    pub fn new(
        ts_ms: i64,
        level: EventSeverity,
        category: EventCategory,
        outcome: EventOutcome,
    ) -> Self {
        Self {
            id: None,
            ts_ms,
            level,
            category,
            action: String::new(),
            outcome,
            actor_type: EventActorType::System,
            actor_id: None,
            source_id: EventSourceId::System,
            connection_id: None,
            database_name: None,
            driver_id: None,
            object_type: None,
            object_id: None,
            summary: String::new(),
            details_json: None,
            error_code: None,
            error_message: None,
            duration_ms: None,
            session_id: None,
            correlation_id: None,
        }
    }

    /// Sets the action field.
    pub fn with_action(mut self, action: impl Into<String>) -> Self {
        self.action = action.into();
        self
    }

    /// Sets the summary field.
    pub fn with_summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = summary.into();
        self
    }

    /// Sets the actor_id field.
    pub fn with_actor_id(mut self, actor_id: impl Into<String>) -> Self {
        self.actor_id = Some(actor_id.into());
        self
    }

    /// Sets the connection context fields.
    pub fn with_connection_context(
        mut self,
        connection_id: impl Into<String>,
        database_name: impl Into<String>,
        driver_id: impl Into<String>,
    ) -> Self {
        self.connection_id = Some(connection_id.into());
        self.database_name = Some(database_name.into());
        self.driver_id = Some(driver_id.into());
        self
    }

    /// Sets the object reference fields.
    pub fn with_object_ref(
        mut self,
        object_type: impl Into<String>,
        object_id: impl Into<String>,
    ) -> Self {
        self.object_type = Some(object_type.into());
        self.object_id = Some(object_id.into());
        self
    }

    /// Sets the details JSON.
    pub fn with_details_json(mut self, details: impl Into<String>) -> Self {
        self.details_json = Some(details.into());
        self
    }

    /// Sets the error fields.
    pub fn with_error(mut self, code: impl Into<String>, message: impl Into<String>) -> Self {
        self.error_code = Some(code.into());
        self.error_message = Some(message.into());
        self
    }

    /// Sets the duration in milliseconds.
    pub fn with_duration_ms(mut self, duration_ms: i64) -> Self {
        self.duration_ms = Some(duration_ms);
        self
    }

    /// Sets the session_id field.
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// Sets the correlation_id field.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Sets the action field from a typed [`AuditAction`] constant.
    ///
    /// This is preferred over [`with_action`](Self::with_action) when
    /// a canonical action constant is available.
    pub fn with_typed_action(mut self, action: crate::observability::AuditAction) -> Self {
        self.action = action.as_str().to_string();
        self
    }

    /// Sets `actor_type` and `source_id` from an [`EventOrigin`].
    ///
    /// This applies the origin's actor and source mapping in a single call.
    pub fn with_origin(mut self, origin: crate::observability::EventOrigin) -> Self {
        self.actor_type = origin.actor_type;
        self.source_id = origin.source_id;
        self
    }
}

// ============================================================================
// Event Source Location (for querying)
// ============================================================================

/// Source location for querying audit events.
///
/// This determines WHERE events are queried from, supporting multiple backends.
/// The `EventSourceId` enum identifies WHERE an event ORIGINATED,
/// while `AuditQuerySource` identifies WHERE to QUERY events FROM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum AuditQuerySource {
    /// Events from the local SQLite database (default, always available).
    #[default]
    Internal,
    /// Events from AWS CloudWatch Logs (future, not yet implemented).
    CloudWatch,
    /// Events from Grafana Loki (future, not yet implemented).
    Loki,
}

impl AuditQuerySource {
    /// Returns the string representation used in storage and APIs.
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditQuerySource::Internal => "internal",
            AuditQuerySource::CloudWatch => "cloudwatch",
            AuditQuerySource::Loki => "loki",
        }
    }

    /// Parse from a string representation.
    pub fn from_str_repr(s: &str) -> Option<Self> {
        match s {
            "internal" => Some(AuditQuerySource::Internal),
            "cloudwatch" => Some(AuditQuerySource::CloudWatch),
            "loki" => Some(AuditQuerySource::Loki),
            _ => None,
        }
    }

    /// Returns `true` if this source is currently implemented and queryable.
    pub fn is_available(&self) -> bool {
        matches!(self, AuditQuerySource::Internal)
    }

    /// Returns a human-readable description of this source.
    pub fn description(&self) -> &'static str {
        match self {
            AuditQuerySource::Internal => "Local SQLite",
            AuditQuerySource::CloudWatch => "CloudWatch (future)",
            AuditQuerySource::Loki => "Loki (future)",
        }
    }
}

// ============================================================================
// Event Retention Policy
// ============================================================================

/// Policy for how long audit events are retained.
///
/// Controls the retention period and purge behavior for audit events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRetentionPolicy {
    /// Number of days to retain audit events.
    pub retention_days: u32,
    /// Whether to purge old events on application startup.
    pub purge_on_startup: bool,
    /// Interval in seconds between background purge runs.
    pub purge_interval_secs: u64,
}

impl Default for EventRetentionPolicy {
    fn default() -> Self {
        Self {
            retention_days: 90,
            purge_on_startup: false,
            purge_interval_secs: 21600, // 6 hours
        }
    }
}

/// Validation for retention policy values.
impl EventRetentionPolicy {
    /// Validates the retention policy settings.
    ///
    /// Returns `Ok(())` if valid, or an error message if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.retention_days == 0 {
            return Err("retention_days must be at least 1".to_string());
        }
        if self.retention_days > 3650 {
            return Err("retention_days must not exceed 3650 (10 years)".to_string());
        }
        if self.purge_interval_secs < 300 && self.purge_interval_secs != 0 {
            return Err("purge_interval_secs must be at least 300 (5 minutes)".to_string());
        }
        Ok(())
    }
}

// ============================================================================
// Event Capture Policy
// ============================================================================

/// Policy for what types of events are captured in the audit log.
///
/// Controls which categories and sources of events are recorded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventCapturePolicy {
    /// Whether to capture user-initiated actions.
    pub capture_user_actions: bool,
    /// Whether to capture system-level events.
    pub capture_system_events: bool,
    /// Whether to capture full query text in events.
    pub capture_query_text: bool,
    /// Whether to capture hook output metadata.
    pub capture_hook_output_metadata: bool,
    /// Whether to redact sensitive values in details_json.
    pub redact_sensitive: bool,
    /// Maximum size in bytes for details_json field.
    pub max_detail_bytes: usize,
}

impl Default for EventCapturePolicy {
    fn default() -> Self {
        Self {
            capture_user_actions: true,
            capture_system_events: true,
            capture_query_text: false,
            capture_hook_output_metadata: true,
            redact_sensitive: true,
            max_detail_bytes: 65536, // 64KB
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_severity_conversion() {
        assert_eq!(EventSeverity::Info.as_str(), "info");
        assert_eq!(
            EventSeverity::from_str_repr("error"),
            Some(EventSeverity::Error)
        );
        assert_eq!(EventSeverity::from_str_repr("unknown"), None);
    }

    #[test]
    fn test_event_category_conversion() {
        assert_eq!(EventCategory::Query.as_str(), "query");
        assert_eq!(
            EventCategory::from_str_repr("hook"),
            Some(EventCategory::Hook)
        );
        assert_eq!(EventCategory::from_str_repr("unknown"), None);
    }

    #[test]
    fn test_event_outcome_conversion() {
        assert_eq!(EventOutcome::Success.as_str(), "success");
        assert_eq!(
            EventOutcome::from_str_repr("failure"),
            Some(EventOutcome::Failure)
        );
    }

    #[test]
    fn test_event_actor_type_conversion() {
        assert_eq!(EventActorType::User.as_str(), "user");
        assert_eq!(
            EventActorType::from_str_repr("mcp_client"),
            Some(EventActorType::McpClient)
        );
    }

    #[test]
    fn test_event_source_id_conversion() {
        assert_eq!(EventSourceId::Local.as_str(), "local");
        assert_eq!(
            EventSourceId::from_str_repr("hook"),
            Some(EventSourceId::Hook)
        );
    }

    #[test]
    fn test_event_record_builder() {
        let event = EventRecord::new(
            1700000000000,
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Success,
        )
        .with_action("select_data")
        .with_summary("Executed SELECT query")
        .with_actor_id("user123")
        .with_connection_context("conn1", "mydb", "postgres")
        .with_duration_ms(42);

        assert_eq!(event.action, "select_data");
        assert_eq!(event.summary, "Executed SELECT query");
        assert_eq!(event.actor_id, Some("user123".to_string()));
        assert_eq!(event.connection_id, Some("conn1".to_string()));
        assert_eq!(event.database_name, Some("mydb".to_string()));
        assert_eq!(event.driver_id, Some("postgres".to_string()));
        assert_eq!(event.duration_ms, Some(42));
    }

    #[test]
    fn test_event_object_ref() {
        let obj = EventObjectRef::new("table", "users");
        assert_eq!(obj.object_type, "table");
        assert_eq!(obj.object_id, "users");
    }

    #[test]
    fn test_retention_policy_validation() {
        let valid = EventRetentionPolicy::default();
        assert!(valid.validate().is_ok());

        let invalid_days = EventRetentionPolicy {
            retention_days: 0,
            ..Default::default()
        };
        assert!(invalid_days.validate().is_err());

        let invalid_interval = EventRetentionPolicy {
            purge_interval_secs: 100,
            ..Default::default()
        };
        assert!(invalid_interval.validate().is_err());
    }

    #[test]
    fn test_event_record_serialization() {
        let event = EventRecord::new(
            1700000000000,
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Success,
        )
        .with_action("test")
        .with_summary("Test event");

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: EventRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.action, event.action);
        assert_eq!(deserialized.level, event.level);
    }
}
