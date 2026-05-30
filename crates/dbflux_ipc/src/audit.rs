//! Audit-emission types for external RPC services.
//!
//! External RPC drivers (v1.2+) and auth providers (v1.3+) may emit audit events
//! as intermediate response frames. The host owns identity, correlation, and rate
//! limiting; the external service supplies only the event content.
//!
//! ## Design constraints
//!
//! - `AuditEventEmitDto` carries no identity fields (`actor_type`, `actor_id`,
//!   `connection_id`, etc.). Those are host-supplied at sanitization time, making
//!   it compile-time impossible for an external service to forge its own identity.
//! - `ExternalAuditEmitter` is defined here so `dbflux_driver_ipc` and
//!   `dbflux_ipc::auth_provider_client` can hold an `Arc<dyn ExternalAuditEmitter>`
//!   without depending on `dbflux_audit`.
//! - The concrete sanitizing implementation lives in `dbflux_app::rpc_services::external_audit`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// DTO mirror enums
// ============================================================================

/// Mirror of `dbflux_core::observability::EventSeverity` for IPC transport.
///
/// Defined separately so `dbflux_ipc` does not need to import
/// `dbflux_core::observability` internals. Conversion via `From` is implemented
/// in `dbflux_app::rpc_services::external_audit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventSeverityDto {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
    Fatal,
}

/// Mirror of `dbflux_core::observability::EventCategory` for IPC transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventCategoryDto {
    Config,
    Connection,
    Query,
    Hook,
    Script,
    #[default]
    System,
    Mcp,
    Governance,
}

/// Mirror of `dbflux_core::observability::EventOutcome` for IPC transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventOutcomeDto {
    #[default]
    Success,
    Failure,
    Cancelled,
    Pending,
}

// ============================================================================
// Audit emit DTO
// ============================================================================

/// Payload sent by an external RPC service to request audit-event recording.
///
/// Fields intentionally absent: `actor_type`, `source_id`, `actor_id`,
/// `connection_id`, `database_name`, `driver_id`, `correlation_id`, `session_id`.
/// All identity fields are unconditionally supplied by the host at sanitization
/// time, so there is no runtime validation surface to exploit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEventEmitDto {
    /// Service-side timestamp in milliseconds since Unix epoch.
    ///
    /// The host clamps this value when the drift exceeds five minutes.
    pub ts_ms: i64,
    /// Severity level of the event.
    pub level: EventSeverityDto,
    /// Functional category.  Only a whitelist of categories is accepted per source
    /// kind; others are dropped silently by the sanitizer.
    pub category: EventCategoryDto,
    /// Short action identifier (e.g. `"query.execute"`).  Required, non-empty.
    pub action: String,
    /// Outcome of the action.
    pub outcome: EventOutcomeDto,
    /// Human-readable summary.  Required, non-empty.
    pub summary: String,
    /// Type of object involved (e.g. `"table"`, `"collection"`).
    pub object_type: Option<String>,
    /// Identifier of the specific object involved.
    pub object_id: Option<String>,
    /// Duration of the action in milliseconds.
    pub duration_ms: Option<i64>,
    /// Short error code string when `outcome == Failure`.
    pub error_code: Option<String>,
    /// Human-readable error message when `outcome == Failure`.
    pub error_message: Option<String>,
    /// Additional structured detail as a JSON object string.
    /// Capped to 64 KiB by `AuditService::preprocess_event_for_storage`.
    pub details_json: Option<String>,
}

// ============================================================================
// Source context
// ============================================================================

/// Source context supplied by the transport layer to the audit sanitizer.
///
/// The sanitizer reads these host-controlled values to build the final
/// `EventRecord`. `correlation_id` is always host-generated and never DTO-supplied.
#[derive(Debug, Clone)]
pub enum ExternalAuditSource {
    /// Frame came from an RPC driver connection.
    Driver {
        socket_id: String,
        session_id: Option<Uuid>,
        correlation_id: String,
    },
    /// Frame came from an RPC auth-provider request cycle.
    AuthProvider {
        socket_id: String,
        provider_id: String,
        correlation_id: String,
    },
}

impl ExternalAuditSource {
    /// Returns the `socket_id` regardless of source kind, used as the rate-limiter key.
    pub fn socket_id(&self) -> &str {
        match self {
            ExternalAuditSource::Driver { socket_id, .. } => socket_id,
            ExternalAuditSource::AuthProvider { socket_id, .. } => socket_id,
        }
    }

    /// Returns a stable label for the source kind, used in log output.
    pub fn kind_label(&self) -> &'static str {
        match self {
            ExternalAuditSource::Driver { .. } => "driver",
            ExternalAuditSource::AuthProvider { .. } => "auth_provider",
        }
    }
}

// ============================================================================
// Emitter trait
// ============================================================================

/// Abstraction over the audit sanitizer, kept in `dbflux_ipc` so transport-layer
/// crates can hold an `Arc<dyn ExternalAuditEmitter>` without depending on
/// `dbflux_audit`.
///
/// The concrete implementation (`ExternalAuditSink`) lives in
/// `dbflux_app::rpc_services::external_audit`.
pub trait ExternalAuditEmitter: Send + Sync {
    /// Called by the transport loop for every `EmitAuditEvent` frame intercepted.
    ///
    /// Implementations must be cheap and non-blocking: they should acquire at most
    /// one short-lived mutex, build an `EventRecord`, and dispatch it to the audit
    /// store without waiting for a response.  Any failure is logged at `debug!` and
    /// discarded; IPC sessions must never stall due to audit failures.
    fn emit(&self, source: ExternalAuditSource, dto: AuditEventEmitDto);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_emit_dto_serde_round_trip() {
        let dto = AuditEventEmitDto {
            ts_ms: 1700000000000,
            level: EventSeverityDto::Warn,
            category: EventCategoryDto::Query,
            action: "query.execute".to_string(),
            outcome: EventOutcomeDto::Success,
            summary: "executed a SELECT".to_string(),
            object_type: Some("table".to_string()),
            object_id: Some("users".to_string()),
            duration_ms: Some(42),
            error_code: None,
            error_message: None,
            details_json: Some(r#"{"rows":10}"#.to_string()),
        };

        let json = serde_json::to_string(&dto).expect("serialize");
        let restored: AuditEventEmitDto = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(restored.ts_ms, 1700000000000);
        assert_eq!(restored.action, "query.execute");
        assert_eq!(restored.summary, "executed a SELECT");
        assert_eq!(restored.object_type.as_deref(), Some("table"));
        assert_eq!(restored.object_id.as_deref(), Some("users"));
        assert_eq!(restored.duration_ms, Some(42));
        assert!(restored.error_code.is_none());
        assert!(restored.error_message.is_none());
        assert_eq!(
            restored.details_json.as_deref(),
            Some(r#"{"rows":10}"#)
        );

        // Identity fields must not be present on the type.
        // Asserting at compile time: there is no .actor_type, .actor_id, etc.
        // Runtime check: JSON must not contain those keys.
        assert!(
            !json.contains("actor_type"),
            "DTO must not carry actor_type"
        );
        assert!(!json.contains("actor_id"), "DTO must not carry actor_id");
        assert!(
            !json.contains("connection_id"),
            "DTO must not carry connection_id"
        );
        assert!(
            !json.contains("driver_id"),
            "DTO must not carry driver_id"
        );
        assert!(
            !json.contains("correlation_id"),
            "DTO must not carry correlation_id"
        );
        assert!(!json.contains("source_id"), "DTO must not carry source_id");
    }

    #[test]
    fn external_audit_source_socket_id_and_kind_label() {
        let driver_source = ExternalAuditSource::Driver {
            socket_id: "my-driver".to_string(),
            session_id: None,
            correlation_id: "corr-1".to_string(),
        };
        assert_eq!(driver_source.socket_id(), "my-driver");
        assert_eq!(driver_source.kind_label(), "driver");

        let provider_source = ExternalAuditSource::AuthProvider {
            socket_id: "my-provider".to_string(),
            provider_id: "aws-sso".to_string(),
            correlation_id: "corr-2".to_string(),
        };
        assert_eq!(provider_source.socket_id(), "my-provider");
        assert_eq!(provider_source.kind_label(), "auth_provider");
    }
}
