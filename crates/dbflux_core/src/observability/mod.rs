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

// Re-export commonly used types
pub use actions::AuditAction;
pub use context::{AuditContext, EventOrigin, new_correlation_id};
pub use query::{EventDetail, EventPage, EventQuery};
pub use source::{EventSink, EventSinkError, EventSource, EventSourceError};
pub use types::{
    AuditQuerySource, EventActorType, EventCapturePolicy, EventCategory, EventObjectRef,
    EventOutcome, EventRecord, EventRetentionPolicy, EventSeverity, EventSourceId,
};
