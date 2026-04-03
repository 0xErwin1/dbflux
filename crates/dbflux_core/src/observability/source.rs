//! Event source and sink traits for the global audit system.
//!
//! These traits define the interfaces for emitting and querying audit events.
//! The `EventSink` trait is implemented by `AuditService` and is the primary
//! interface for emitting events from service layers.
//!
//! ## Design Notes
//!
//! - `EventSink::record()` is the primary method for emitting events
//! - `EventSource` is defined for future extensibility (e.g., external event ingestion)
//! - Both traits use `EventRecord` as the canonical event type

use super::types::EventRecord;
use crate::observability::query::{EventDetail, EventPage, EventQuery};

/// Error type for event sink operations.
#[derive(Debug, Clone)]
pub enum EventSinkError {
    /// Event is missing required fields.
    MissingRequiredField(&'static str),
    /// Serialization error for details_json.
    Serialization(String),
    /// Storage error.
    Storage(String),
    /// Internal error.
    Internal(String),
}

impl std::fmt::Display for EventSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventSinkError::MissingRequiredField(field) => {
                write!(f, "missing required field: {}", field)
            }
            EventSinkError::Serialization(msg) => write!(f, "serialization error: {}", msg),
            EventSinkError::Storage(msg) => write!(f, "storage error: {}", msg),
            EventSinkError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for EventSinkError {}

// Note: EventSinkError does not implement conversion to crate::DbError to avoid
// creating a dependency from observability to specific error handling patterns.
// Consumers should handle EventSinkError directly.

// ============================================================================
// Event Sink
// ============================================================================

/// Trait for emitting audit events.
///
/// This is the primary interface for service layers to emit audit events.
/// Implementations handle validation, storage, and potentially async processing.
///
/// ## Implementing EventSink
///
/// ```ignore
/// impl EventSink for AuditService {
///     fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
///         // Validate required fields
///         if event.action.is_empty() {
///             return Err(EventSinkError::MissingRequiredField("action"));
///         }
///
///         // Store the event and return with assigned ID
///         let stored = self.store.record(event)?;
///         Ok(stored)
///     }
/// }
/// ```
pub trait EventSink: Send + Sync {
    /// Records an audit event.
    ///
    /// The event should be validated and stored. On success, returns the
    /// event with its assigned ID and timestamp.
    ///
    /// # Errors
    ///
    /// Returns `EventSinkError` if:
    /// - A required field is missing
    /// - Serialization of details_json fails
    /// - Storage operation fails
    fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError>;

    /// Records an audit event asynchronously.
    ///
    /// Implementations may choose to spawn a background task for storage,
    /// allowing the caller to proceed without waiting for the write to complete.
    ///
    /// The default implementation calls `record()` synchronously.
    fn record_async(&self, event: EventRecord) -> Result<(), EventSinkError>
    where
        Self: Sized,
    {
        let _ = self.record(event)?;
        Ok(())
    }
}

// ============================================================================
// Event Source
// ============================================================================

/// Error type for event source operations.
#[derive(Debug, Clone)]
pub enum EventSourceError {
    /// Query error.
    Query(String),
    /// Not found error.
    NotFound(i64),
    /// Export error.
    Export(String),
    /// Internal error.
    Internal(String),
}

impl std::fmt::Display for EventSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventSourceError::Query(msg) => write!(f, "query error: {}", msg),
            EventSourceError::NotFound(id) => write!(f, "event {} not found", id),
            EventSourceError::Export(msg) => write!(f, "export error: {}", msg),
            EventSourceError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for EventSourceError {}

/// Trait for reading and querying audit events.
///
/// This trait supports the audit view UI and any external consumers
/// that need to query historical events. It is intentionally separate
/// from `EventSink` to allow independent scaling of read and write paths.
///
/// ## Default Implementations
///
/// Default implementations are provided for `query()` (returns empty),
/// `read_detail()` (returns NotFound), and `export_events()` (returns error).
pub trait EventSource: Send + Sync {
    /// Queries audit events with the given filter.
    ///
    /// Returns a page of events sorted by timestamp descending (newest first).
    ///
    /// # Errors
    ///
    /// Returns `EventSourceError::Query` if the query fails.
    fn query(&self, filter: &EventQuery) -> Result<EventPage, EventSourceError> {
        let _ = filter;
        Ok(EventPage::new(vec![], Some(0), false, 0, 0))
    }

    /// Reads a single event by ID with full details.
    ///
    /// Returns `EventDetail` with parsed and formatted fields.
    ///
    /// # Errors
    ///
    /// Returns `EventSourceError::NotFound` if the event doesn't exist.
    fn read_detail(&self, id: i64) -> Result<EventDetail, EventSourceError> {
        let _ = id;
        Err(EventSourceError::NotFound(id))
    }

    /// Exports events matching the filter to the specified format.
    ///
    /// Returns the exported data as bytes. The format should be indicated
    /// by the `format` parameter (e.g., "json", "csv").
    ///
    /// # Errors
    ///
    /// Returns `EventSourceError::Export` if export fails.
    fn export_events(
        &self,
        filter: &EventQuery,
        format: &str,
    ) -> Result<Vec<u8>, EventSourceError> {
        let _ = (filter, format);
        Err(EventSourceError::Export(
            "export not implemented".to_string(),
        ))
    }
}

// ============================================================================
// Combinators for composable event handling
// ============================================================================

/// A combinator that wraps an EventSink to add preprocessing.
///
/// This is useful for adding common fields, validation, or transformation
/// to all events before they are recorded.
pub struct EventSinkWrapper<S> {
    inner: S,
}

impl<S> EventSinkWrapper<S> {
    /// Creates a new wrapper around an EventSink.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: EventSink> EventSink for EventSinkWrapper<S> {
    fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
        self.inner.record(event)
    }

    fn record_async(&self, event: EventRecord) -> Result<(), EventSinkError>
    where
        Self: Sized,
    {
        self.inner.record_async(event)
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::EventRecord;
    use super::*;

    struct TestSink;

    impl EventSink for TestSink {
        fn record(&self, mut event: EventRecord) -> Result<EventRecord, EventSinkError> {
            if event.action.is_empty() {
                return Err(EventSinkError::MissingRequiredField("action"));
            }
            // Simulate assigning an ID
            event.id = Some(1);
            Ok(event)
        }
    }

    #[test]
    fn test_event_sink_record() {
        let sink = TestSink;
        let event = EventRecord::new(
            1700000000000,
            super::super::types::EventSeverity::Info,
            super::super::types::EventCategory::Query,
            super::super::types::EventOutcome::Success,
        )
        .with_action("test_action")
        .with_summary("Test summary");

        let result = sink.record(event);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().id, Some(1));
    }

    #[test]
    fn test_event_sink_missing_action() {
        let sink = TestSink;
        let event = EventRecord::new(
            1700000000000,
            super::super::types::EventSeverity::Info,
            super::super::types::EventCategory::Query,
            super::super::types::EventOutcome::Success,
        );

        let result = sink.record(event);
        assert!(matches!(
            result,
            Err(EventSinkError::MissingRequiredField("action"))
        ));
    }

    #[test]
    fn test_sink_wrapper() {
        let wrapper = EventSinkWrapper::new(TestSink);
        let event = EventRecord::new(
            1700000000000,
            super::super::types::EventSeverity::Info,
            super::super::types::EventCategory::Query,
            super::super::types::EventOutcome::Success,
        )
        .with_action("wrapped_action")
        .with_summary("Wrapped summary");

        let result = wrapper.record(event);
        assert!(result.is_ok());
    }
}
