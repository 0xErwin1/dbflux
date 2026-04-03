pub mod export;
pub mod purge;
pub mod query;
pub mod redaction;
pub mod store;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use dbflux_core::observability::{EventRecord, EventSink as CoreEventSink, EventSinkError};
use dbflux_storage::error::RepositoryError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::export::{AuditExportFormat, export_entries};
use crate::purge::{PurgeStats, purge_old_events};
use crate::query::AuditQueryFilter;
use crate::redaction::{redact_error_message, redact_json};
use crate::store::sqlite::SqliteAuditStore;

pub use dbflux_storage::repositories::audit::AuditEventDto;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: i64,
    pub actor_id: String,
    pub tool_id: String,
    pub decision: String,
    pub reason: Option<String>,
    pub created_at_epoch_ms: i64,
}

#[derive(Debug, Error)]
pub enum AuditError {
    #[error("audit database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("audit serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("audit io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("home config directory not found")]
    ConfigDirUnavailable,
    #[error("event sink error: {0}")]
    EventSink(#[from] EventSinkError),
    #[error("entity not found: {0}")]
    NotFound(String),
}

impl From<AuditError> for EventSinkError {
    fn from(err: AuditError) -> Self {
        match err {
            AuditError::Sqlite(_) => EventSinkError::Storage(err.to_string()),
            AuditError::Serialization(_) => EventSinkError::Serialization(err.to_string()),
            AuditError::Io(_) => EventSinkError::Storage(err.to_string()),
            AuditError::ConfigDirUnavailable => EventSinkError::Internal(err.to_string()),
            AuditError::EventSink(e) => e,
            AuditError::NotFound(_) => EventSinkError::Storage(err.to_string()),
        }
    }
}

impl From<RepositoryError> for AuditError {
    fn from(err: RepositoryError) -> Self {
        match err {
            RepositoryError::Sqlite { source } => AuditError::Sqlite(source),
            RepositoryError::NotFound(msg) => AuditError::NotFound(msg),
            RepositoryError::Serialization { source } => AuditError::Serialization(source),
        }
    }
}

/// Audit service for recording and querying audit events.
///
/// This is the central event bus for DBFlux's global audit system.
/// It provides methods for recording events, querying events, and purging old events.
#[derive(Clone)]
pub struct AuditService {
    store: SqliteAuditStore,
    /// Whether to redact sensitive values in details_json and error_message.
    redact_sensitive: Arc<AtomicBool>,
    /// Whether audit is enabled.
    enabled: Arc<AtomicBool>,
    /// Whether to capture full query text in details_json.
    /// When false, query text is replaced with a fingerprint (SHA256 hash).
    capture_query_text: Arc<AtomicBool>,
    /// Maximum allowed size for the stored details_json payload.
    max_detail_bytes: Arc<AtomicUsize>,
}

const DEFAULT_MAX_DETAIL_BYTES: usize = 65_536;
const UNKNOWN_ACTOR_ID: &str = "unknown";

impl AuditService {
    pub fn new(store: SqliteAuditStore) -> Self {
        Self {
            store,
            redact_sensitive: Arc::new(AtomicBool::new(true)),
            enabled: Arc::new(AtomicBool::new(true)),
            capture_query_text: Arc::new(AtomicBool::new(false)),
            max_detail_bytes: Arc::new(AtomicUsize::new(DEFAULT_MAX_DETAIL_BYTES)),
        }
    }

    pub fn new_sqlite_default() -> Result<Self, AuditError> {
        let data_dir = dirs::data_dir().ok_or(AuditError::ConfigDirUnavailable)?;
        let db_dir = data_dir.join("dbflux");
        std::fs::create_dir_all(&db_dir)?;

        let store = SqliteAuditStore::new(db_dir.join("dbflux.db"))?;
        Ok(Self::new(store))
    }

    pub fn new_sqlite(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        Ok(Self::new(SqliteAuditStore::new(path)?))
    }

    /// Sets whether sensitive values should be redacted.
    pub fn set_redact_sensitive(&self, redact: bool) {
        self.redact_sensitive.store(redact, Ordering::SeqCst);
    }

    /// Returns whether sensitive value redaction is enabled.
    pub fn redact_sensitive(&self) -> bool {
        self.redact_sensitive.load(Ordering::SeqCst)
    }

    /// Sets whether audit is enabled.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    /// Returns whether audit is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Sets whether full query text should be captured in details_json.
    ///
    /// When false (default), query text is replaced with a SHA256 fingerprint.
    pub fn set_capture_query_text(&self, capture: bool) {
        self.capture_query_text.store(capture, Ordering::SeqCst);
    }

    /// Returns whether full query text capture is enabled.
    pub fn capture_query_text(&self) -> bool {
        self.capture_query_text.load(Ordering::SeqCst)
    }

    /// Sets the maximum size in bytes for the stored details_json payload.
    pub fn set_max_detail_bytes(&self, max_bytes: usize) {
        self.max_detail_bytes.store(max_bytes, Ordering::SeqCst);
    }

    /// Returns the maximum size in bytes for the stored details_json payload.
    pub fn max_detail_bytes(&self) -> usize {
        self.max_detail_bytes.load(Ordering::SeqCst)
    }

    pub fn sqlite_path(&self) -> &Path {
        self.store.path()
    }

    pub fn query(&self, filter: &AuditQueryFilter) -> Result<Vec<AuditEvent>, AuditError> {
        self.store.query(filter)
    }

    pub fn get(&self, id: i64) -> Result<Option<AuditEvent>, AuditError> {
        self.store.get(id)
    }

    pub fn get_extended(&self, id: i64) -> Result<Option<AuditEventDto>, AuditError> {
        self.store.get_extended(id)
    }

    pub fn query_extended(
        &self,
        filter: &AuditQueryFilter,
    ) -> Result<Vec<AuditEventDto>, AuditError> {
        self.store.query_extended(filter)
    }

    pub fn export(
        &self,
        filter: &AuditQueryFilter,
        format: AuditExportFormat,
    ) -> Result<String, AuditError> {
        let events = self.query(filter)?;
        export_entries(&events, format).map_err(AuditError::from)
    }

    pub fn export_extended(
        &self,
        filter: &AuditQueryFilter,
        format: AuditExportFormat,
    ) -> Result<String, AuditError> {
        let events = self.query_extended(filter)?;
        export::export_extended(&events, format).map_err(AuditError::from)
    }

    /// Records an audit event using the extended schema.
    ///
    /// This is the primary method for recording events from service layers.
    /// It validates the event, optionally redacts sensitive values, and stores it
    /// with the full RF-050/RF-051 schema.
    ///
    /// # Validation
    ///
    /// The following fields are validated based on category:
    /// - **All**: `action`, `summary`, `ts_ms`
    /// - **Query**: `connection_id`, `driver_id`, `duration_ms` for execution events
    /// - **Connection**: `connection_id`
    /// - **Hook**: `object_type`, `object_id` (hook name), `connection_id`
    /// - **Script**: `object_type`, `object_id` (script name/path)
    /// - **Mcp**: `actor_id`, `object_id` (tool name)
    /// - **Config**: `object_type`, `object_id`
    ///
    /// # Errors
    ///
    /// Returns `AuditError` if:
    /// - The event has an empty action field
    /// - Category-specific required fields are missing
    /// - Storage operation fails
    pub fn record(&self, event: EventRecord) -> Result<EventRecord, AuditError> {
        // Check if audit is enabled
        if !self.is_enabled() {
            return Ok(event);
        }

        let event = Self::normalize_details_json(event)?;

        // Canonical validation — all validation happens here, before fingerprinting/redaction
        Self::validate_event(&event)?;

        self.store.record(self.preprocess_event_for_storage(event)?)
    }

    /// Validates an event's required fields based on its category.
    ///
    /// This is the canonical validation point called by `record()`. It enforces
    /// category-specific field requirements before storage.
    ///
    /// # Errors
    ///
    /// Returns `EventSinkError::MissingRequiredField` if a required field is absent.
    pub fn validate_event(event: &EventRecord) -> Result<(), AuditError> {
        use dbflux_core::observability::types::EventCategory;

        // Universal required fields
        if !Self::has_required_text(Some(event.action.as_str())) {
            return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                "action",
            )));
        }
        if !Self::has_required_text(Some(event.summary.as_str())) {
            return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                "summary",
            )));
        }

        // Category-specific required fields
        match event.category {
            EventCategory::Query => {
                if !Self::has_required_text(event.connection_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "connection_id",
                    )));
                }
                if !Self::has_required_text(event.driver_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "driver_id",
                    )));
                }
                if Self::query_action_requires_duration(event.action.as_str())
                    && event.duration_ms.is_none()
                {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "duration_ms",
                    )));
                }
            }
            EventCategory::Connection => {
                if !Self::has_required_text(event.connection_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "connection_id",
                    )));
                }
            }
            EventCategory::Hook => {
                if !Self::has_required_text(event.object_type.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "object_type",
                    )));
                }
                if !Self::has_required_text(event.object_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "object_id",
                    )));
                }
                if !Self::has_required_text(event.connection_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "connection_id",
                    )));
                }
            }
            EventCategory::Script => {
                if !Self::has_required_text(event.object_type.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "object_type",
                    )));
                }
                if !Self::has_required_text(event.object_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "object_id",
                    )));
                }
            }
            EventCategory::Mcp => {
                if !Self::has_required_text(event.actor_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "actor_id",
                    )));
                }
                if !Self::has_required_text(event.object_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "object_id",
                    )));
                }
            }
            EventCategory::Config => {
                if !Self::has_required_text(event.object_type.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "object_type",
                    )));
                }
                if !Self::has_required_text(event.object_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "object_id",
                    )));
                }
            }
            EventCategory::Governance | EventCategory::System => {
                // No additional required fields beyond universal
            }
        }

        Ok(())
    }

    fn has_required_text(value: Option<&str>) -> bool {
        value.is_some_and(|text| !text.trim().is_empty())
    }

    fn query_action_requires_duration(action: &str) -> bool {
        matches!(action, "query_execute" | "query_execute_failed")
    }

    fn normalize_details_json(mut event: EventRecord) -> Result<EventRecord, AuditError> {
        let Some(details) = event.details_json.take() else {
            return Ok(event);
        };

        let value: serde_json::Value = serde_json::from_str(&details).map_err(|err| {
            AuditError::EventSink(EventSinkError::Serialization(format!(
                "details_json must be valid JSON: {}",
                err
            )))
        })?;

        let serde_json::Value::Object(_) = value else {
            return Err(AuditError::EventSink(EventSinkError::Serialization(
                "details_json must be a JSON object".to_string(),
            )));
        };

        event.details_json = Some(serde_json::to_string(&value)?);

        Ok(event)
    }

    fn preprocess_event_for_storage(
        &self,
        mut event: EventRecord,
    ) -> Result<EventRecord, AuditError> {
        if event.actor_id.is_none() {
            event.actor_id = Some(UNKNOWN_ACTOR_ID.to_string());
        }

        if !self.capture_query_text() {
            Self::apply_query_fingerprint_static(&mut event);
        }

        if self.redact_sensitive() {
            event = self.apply_redaction(event);
        }

        self.enforce_max_detail_bytes(&event)?;

        Ok(event)
    }

    fn enforce_max_detail_bytes(&self, event: &EventRecord) -> Result<(), AuditError> {
        let Some(details) = event.details_json.as_ref() else {
            return Ok(());
        };

        let detail_len = details.len();
        let max_detail_bytes = self.max_detail_bytes();

        if detail_len > max_detail_bytes {
            return Err(AuditError::EventSink(EventSinkError::Serialization(
                format!(
                    "details_json exceeds max_detail_bytes ({} > {})",
                    detail_len, max_detail_bytes
                ),
            )));
        }

        Ok(())
    }

    /// Applies redaction for sensitive values in details_json and error_message.
    fn apply_redaction(&self, mut event: EventRecord) -> EventRecord {
        // Redact sensitive values in details_json
        if let Some(ref details) = event.details_json {
            let result = redact_json(details, true);
            if result.redaction_count > 0 {
                event.details_json = Some(result.redacted);
            }
        }

        // Redact error_message
        if let Some(ref error_msg) = event.error_message {
            let result = redact_error_message(error_msg, true);
            if result.redaction_count > 0 {
                event.error_message = Some(result.redacted);
            }
        }

        event
    }

    /// Replaces query text in details_json with a SHA256 fingerprint when
    /// capture_query_text is disabled.
    fn apply_query_fingerprint_static(event: &mut EventRecord) {
        if let Some(ref details) = event.details_json
            && let Ok(serde_json::Value::Object(mut map)) =
                serde_json::from_str::<serde_json::Value>(details)
            && let Some(query_val) = map.get("query")
            && let serde_json::Value::String(query) = query_val
        {
            let query_clone = query.clone();
            let query_len = query_clone.len();
            let fingerprint = Self::sha256_fingerprint(&query_clone);
            map.insert(
                "query".to_string(),
                serde_json::Value::String(format!("[FINGERPRINT:{}]", &fingerprint[..16])),
            );
            map.insert(
                "query_length".to_string(),
                serde_json::Value::Number(query_len.into()),
            );
            if let Ok(new_details) = serde_json::to_string(&map) {
                event.details_json = Some(new_details);
            }
        }
    }

    /// Computes a SHA256 fingerprint of the given text.
    fn sha256_fingerprint(text: &str) -> String {
        use sha2::Digest;
        let normalized = text.trim().to_lowercase();
        let bytes = normalized.as_bytes();
        let mut hash = sha2::Sha256::new();
        hash.update(bytes);
        let result = hash.finalize();
        hex::encode(result)
    }

    /// Purges old audit events based on retention policy.
    ///
    /// ## Arguments
    ///
    /// * `retention_days` - Number of days to retain events
    /// * `batch_size` - Number of events to delete per batch (default 500)
    ///
    /// ## Returns
    ///
    /// Statistics about the purge operation.
    pub fn purge_old_events(
        &self,
        retention_days: u32,
        batch_size: usize,
    ) -> Result<PurgeStats, AuditError> {
        purge_old_events(&self.store, retention_days, batch_size)
    }

    /// Records a panic event without blocking.
    ///
    /// This is the public entry point for the global panic hook.
    /// It creates a `system_panic` event from the provided panic info string
    /// and attempts a non-blocking write through the store layer.
    ///
    /// If audit is disabled, returns `Ok(None)` silently.
    /// If the store mutex is held by another thread, logs to stderr and returns `Ok(None)`.
    /// If an actual storage error occurs, logs to stderr and returns `Ok(None)`.
    ///
    /// This function is designed to be called from a panic hook without risking
    /// deadlock or double-panic.
    ///
    /// # Arguments
    ///
    /// * `panic_info` — A string describing the panic (message + location).
    ///
    /// # Returns
    ///
    /// `Ok(Some(record))` if the panic was recorded.
    /// `Ok(None)` if recording failed or was not possible (no error returned to caller).
    pub fn record_panic_best_effort(&self, panic_info: &str) -> Option<EventRecord> {
        use dbflux_core::observability::types::EventSeverity;

        if !self.is_enabled() {
            return None;
        }

        // Use current time from std::time if chrono is not available as a direct dep
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let panic_event = EventRecord::new(
            ts_ms,
            EventSeverity::Fatal,
            dbflux_core::observability::types::EventCategory::System,
            dbflux_core::observability::types::EventOutcome::Failure,
        )
        .with_typed_action(dbflux_core::observability::actions::SYSTEM_PANIC)
        .with_summary("Application panic captured")
        .with_error("panic", panic_info);

        let sanitized_panic_info = if self.redact_sensitive() {
            redact_error_message(panic_info, true).redacted
        } else {
            panic_info.to_string()
        };

        // Build panic details JSON using the sanitized version
        let details = serde_json::json!({
            "panic_info": sanitized_panic_info,
        });
        let panic_event =
            match Self::normalize_details_json(panic_event.with_details_json(details.to_string()))
                .and_then(|event| self.preprocess_event_for_storage(event))
            {
                Ok(event) => event,
                Err(e) => {
                    eprintln!("[dbflux_audit] panic preprocessing failed: {:?}", e);
                    return None;
                }
            };

        // Delegates to store's non-blocking path
        match self
            .store
            .record_panic_best_effort(panic_event, &sanitized_panic_info)
        {
            Ok(Some(record)) => Some(record),
            Ok(None) => {
                // Fallback already logged in store
                None
            }
            Err(e) => {
                eprintln!("[dbflux_audit] panic best-effort failed: {:?}", e);
                None
            }
        }
    }
}

/// Implement `EventSink` for `AuditService`.
///
/// This allows services to emit audit events through the `EventSink` trait
/// interface, which is the primary way service layers emit events.
impl CoreEventSink for AuditService {
    fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
        AuditService::record(self, event).map_err(|e| e.into())
    }
}

pub fn temp_sqlite_path(file_name: &str) -> PathBuf {
    std::env::temp_dir().join(file_name)
}
