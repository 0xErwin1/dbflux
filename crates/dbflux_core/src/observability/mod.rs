//! Observability module for global audit system.
//!
//! This module provides types and traits for unified audit event recording
//! and querying across all DBFlux components.
//!
//! ## Key Types
//!
//! - [`EventRecord`] - The canonical audit event structure
//! - [`EventSeverity`] - Severity levels (trace, debug, info, warn, error, fatal)
//! - [`EventCategory`] - Event categories (config, connection, query, etc.)
//! - [`EventOutcome`] - Action outcomes (success, failure, cancelled, pending)
//! - [`EventActorType`] - Actor types (user, system, mcp_client, etc.)
//! - [`EventSourceId`] - Event sources (local, mcp, hook, script, system)
//!
//! ## Key Traits
//!
//! - [`EventSink`] - For emitting audit events from service layers
//! - [`EventSource`] - For querying and reading audit events
//!
//! ## Query Types
//!
//! - [`EventQuery`] - Filter for querying events
//! - [`EventPage`] - Paginated results
//! - [`EventDetail`] - Full event with formatted fields
//!
//! ## Policies
//!
//! - [`EventRetentionPolicy`] - Controls event retention and purge behavior
//! - [`EventCapturePolicy`] - Controls what events are captured

pub mod actions;
pub mod context;
pub mod query;
pub mod source;
pub mod types;

#[cfg(feature = "tracing-bridge")]
pub mod tracing_bridge;

// Re-export commonly used types
pub use actions::AuditAction;
pub use context::{AuditContext, EventOrigin, new_correlation_id};
pub use query::{EventDetail, EventPage, EventQuery};
pub use source::{EventSink, EventSinkError, EventSource, EventSourceError};
pub use types::{
    AuditQuerySource, EventActorType, EventCapturePolicy, EventCategory, EventObjectRef,
    EventOutcome, EventRecord, EventRetentionPolicy, EventSeverity, EventSourceId,
};

/// Emits a structured tracing event for a user-facing error.
///
/// This helper centralises the field-name contract for user-error events so
/// that `AuditFieldVisitor` and call sites stay in sync without duplication.
/// The `correlation_id` field is captured by `AuditFieldVisitor` and written
/// directly to `EventRecord.correlation_id` (not `details_json`).
///
/// Call sites MUST use this function — never inline the `tracing::error!`
/// call — to guarantee the field set remains consistent with the bridge.
#[cfg(feature = "tracing-bridge")]
pub fn emit_user_error_event(level: tracing::Level, summary: &str, correlation_id: &str, kind: &str) {
    match level {
        tracing::Level::WARN => tracing::warn!(
            target: "dbflux_ui::user_error",
            correlation_id = %correlation_id,
            kind            = %kind,
            outcome         = "failure",
            action          = "user_error",
            "{summary}",
        ),
        _ => tracing::error!(
            target: "dbflux_ui::user_error",
            correlation_id = %correlation_id,
            kind            = %kind,
            outcome         = "failure",
            action          = "user_error",
            "{summary}",
        ),
    }
}
