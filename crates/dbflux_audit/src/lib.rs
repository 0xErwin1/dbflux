pub mod export;
pub mod purge;
pub mod query;
pub mod redaction;
pub mod store;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dbflux_core::observability::{EventRecord, EventSink as CoreEventSink, EventSinkError};
use dbflux_storage::error::RepositoryError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::export::{AuditExportFormat, export_entries};
use crate::purge::{PurgeStats, purge_old_events};
use crate::query::AuditQueryFilter;
use crate::redaction::{redact_error_message, redact_json};
use crate::store::sqlite::SqliteAuditStore;

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
}

impl From<AuditError> for EventSinkError {
    fn from(err: AuditError) -> Self {
        match err {
            AuditError::Sqlite(_) => EventSinkError::Storage(err.to_string()),
            AuditError::Serialization(_) => EventSinkError::Serialization(err.to_string()),
            AuditError::Io(_) => EventSinkError::Storage(err.to_string()),
            AuditError::ConfigDirUnavailable => EventSinkError::Internal(err.to_string()),
            AuditError::EventSink(e) => e,
        }
    }
}

impl From<RepositoryError> for AuditError {
    fn from(err: RepositoryError) -> Self {
        match err {
            RepositoryError::Sqlite { source } => AuditError::Sqlite(source),
            RepositoryError::NotFound(_msg) => AuditError::Sqlite(rusqlite::Error::InvalidQuery),
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
}

impl AuditService {
    pub fn new(store: SqliteAuditStore) -> Self {
        Self {
            store,
            redact_sensitive: Arc::new(AtomicBool::new(true)),
            enabled: Arc::new(AtomicBool::new(true)),
            capture_query_text: Arc::new(AtomicBool::new(false)),
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

    pub fn sqlite_path(&self) -> &Path {
        self.store.path()
    }

    pub fn append(
        &self,
        actor_id: &str,
        tool_id: &str,
        decision: &str,
        reason: Option<&str>,
        created_at_epoch_ms: i64,
    ) -> Result<AuditEvent, AuditError> {
        self.store
            .append(actor_id, tool_id, decision, reason, created_at_epoch_ms)
    }

    pub fn query(&self, filter: &AuditQueryFilter) -> Result<Vec<AuditEvent>, AuditError> {
        self.store.query(filter)
    }

    pub fn get(&self, id: i64) -> Result<Option<AuditEvent>, AuditError> {
        self.store.get(id)
    }

    pub fn export(
        &self,
        filter: &AuditQueryFilter,
        format: AuditExportFormat,
    ) -> Result<String, AuditError> {
        let events = self.query(filter)?;
        export_entries(&events, format).map_err(AuditError::from)
    }

    /// Records an audit event using the extended schema.
    ///
    /// This is the primary method for recording events from service layers.
    /// It validates the event, optionally redacts sensitive values, and stores it
    /// with the full RF-050/RF-051 schema.
    ///
    /// # Errors
    ///
    /// Returns `AuditError` if:
    /// - The event has an empty action field
    /// - Storage operation fails
    pub fn record(&self, event: EventRecord) -> Result<EventRecord, AuditError> {
        // Check if audit is enabled
        if !self.is_enabled() {
            return Ok(event);
        }

        // Validate required fields
        if event.action.is_empty() {
            return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                "action",
            )));
        }

        let mut event = event;

        // Apply query text fingerprinting if capture_query_text is disabled.
        // This is independent of redaction: we always fingerprint when capture is disabled,
        // regardless of whether redaction is enabled.
        if !self.capture_query_text() {
            Self::apply_query_fingerprint_static(&mut event);
        }

        // Apply redaction if enabled (sensitive value redaction)
        if self.redact_sensitive() {
            event = self.apply_redaction(event);
        }

        self.store.record(event)
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
