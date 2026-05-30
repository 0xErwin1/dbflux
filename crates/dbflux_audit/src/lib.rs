pub mod export;
pub mod purge;
pub mod query;
pub mod redaction;
pub mod store;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};

use dbflux_core::observability::{EventRecord, EventSink as CoreEventSink, EventSinkError};
use dbflux_storage::error::RepositoryError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::export::{AuditExportFormat, export_entries};
use crate::purge::{PurgeStats, purge_old_events};
use crate::query::AuditQueryFilter;
use crate::redaction::{redact_error_message, redact_json};
use crate::store::sqlite::SqliteAuditStore;

pub use crate::query::{AuditAggregateParams, AuditGroupColumn};
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
    #[error("invalid parameter: {0}")]
    Validation(String),
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
            AuditError::Validation(_) => EventSinkError::Internal(err.to_string()),
        }
    }
}

impl From<RepositoryError> for AuditError {
    fn from(err: RepositoryError) -> Self {
        match err {
            RepositoryError::Sqlite { source } => AuditError::Sqlite(source),
            RepositoryError::NotFound(msg) => AuditError::NotFound(msg),
            RepositoryError::Validation(msg) => AuditError::Validation(msg),
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
    /// Shared with the tracing bridge's `AuditLayer` so runtime level changes
    /// take effect without reinitializing the subscriber.
    bridge_min_level: Option<Arc<AtomicU8>>,
    /// Shared with the tracing bridge to expose drop count via `AuditService`.
    bridge_drop_counter: Option<Arc<AtomicU64>>,
    /// Counts frames dropped by the external-audit sanitizer (rate-limit, category
    /// filter, validation). Shared with `ExternalAuditSink` so it can increment
    /// without holding a reference to `AuditService`.
    external_audit_dropped: Arc<AtomicU64>,
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
            bridge_min_level: None,
            bridge_drop_counter: None,
            external_audit_dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns a clone of the shared drop counter for the external-audit sink.
    ///
    /// The returned `Arc` can be incremented by `ExternalAuditSink` without
    /// holding a reference to `AuditService`.
    pub fn external_audit_drop_counter(&self) -> Arc<AtomicU64> {
        self.external_audit_dropped.clone()
    }

    /// Returns the cumulative count of frames dropped by the external-audit sanitizer.
    pub fn external_audit_dropped_count(&self) -> u64 {
        self.external_audit_dropped.load(Ordering::Relaxed)
    }

    /// Attaches the tracing bridge's shared atomics so level changes and drop counts
    /// are visible through `AuditService::set_log_capture_min_level` and
    /// `AuditService::dropped_log_event_count`.
    pub fn attach_bridge(&mut self, min_level: Arc<AtomicU8>, drop_counter: Arc<AtomicU64>) {
        self.bridge_min_level = Some(min_level);
        self.bridge_drop_counter = Some(drop_counter);
    }

    /// Updates the tracing bridge capture threshold at runtime and persists it.
    ///
    /// Updates the shared `AtomicU8` immediately (no subscriber reinit) and
    /// writes the new value to `cfg_audit_settings` so it survives restart.
    pub fn set_log_capture_min_level(
        &self,
        level: dbflux_core::observability::EventSeverity,
    ) -> Result<(), AuditError> {
        if let Some(min_level_arc) = &self.bridge_min_level {
            let code = severity_to_level_code(level);
            min_level_arc.store(code, Ordering::Relaxed);
        }

        self.store.update_log_capture_min_level(level.as_str())?;

        Ok(())
    }

    /// Returns the count of events dropped by the tracing bridge since startup.
    ///
    /// This includes events dropped due to queue overflow and events dropped
    /// before the audit sink was installed (pre-init window).
    pub fn dropped_log_event_count(&self) -> u64 {
        self.bridge_drop_counter
            .as_ref()
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
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

    /// Aggregates audit events into time buckets and returns a wide-format `QueryResult`.
    ///
    /// The result is pivoted so each distinct group value becomes its own column:
    /// - `bucket_ms` (`ColumnKind::Timestamp`): start of the time bucket in ms since epoch.
    /// - One `ColumnKind::Integer` column per distinct group value, named after the value
    ///   (e.g. "error", "warn"). Column order is ascending alphabetical for stable
    ///   series colors across calls.
    ///
    /// Rows contain one entry per distinct bucket (ascending). Missing (bucket, group)
    /// combinations are filled with `Value::Int(0)` so line charts stay continuous.
    ///
    /// Empty input returns a `QueryResult` with only the `bucket_ms` column and no rows.
    pub fn aggregate(
        &self,
        params: &AuditAggregateParams,
    ) -> Result<dbflux_core::QueryResult, AuditError> {
        use std::time::Instant;

        let started = Instant::now();
        let raw = self.store.aggregate(params)?;
        let elapsed = started.elapsed();

        Ok(pivot_long_to_wide(raw, elapsed))
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
        use dbflux_core::observability::types::{EventActorType, EventCategory};

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
                // ExternalDriver events may have no connection context when the
                // host cannot resolve the session to a connection profile.
                let is_external = matches!(
                    event.actor_type,
                    EventActorType::ExternalDriver | EventActorType::ExternalAuthProvider
                );

                if !is_external && !Self::has_required_text(event.connection_id.as_deref()) {
                    return Err(AuditError::EventSink(EventSinkError::MissingRequiredField(
                        "connection_id",
                    )));
                }
                if !is_external && !Self::has_required_text(event.driver_id.as_deref()) {
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
                // ExternalAuthProvider events have no connection_id by design.
                let is_external_auth =
                    matches!(event.actor_type, EventActorType::ExternalAuthProvider);

                if !is_external_auth && !Self::has_required_text(event.connection_id.as_deref()) {
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

/// Pivots a long-format aggregate result `(bucket_ms, group_label, count)` into
/// a wide `QueryResult` where each distinct group value becomes its own column.
///
/// Column layout:
/// - col 0: `bucket_ms` (`ColumnKind::Timestamp`)
/// - col 1..N: one `ColumnKind::Integer` column per distinct group value, sorted
///   ascending alphabetically for stable series ordering and colors.
///
/// Rows are ordered by `bucket_ms` ascending. Missing (bucket, group) combinations
/// are filled with `Value::Int(0)` so line charts stay continuous.
///
/// Empty input returns a result with only the `bucket_ms` column and no rows.
pub fn pivot_long_to_wide(
    raw: Vec<(i64, String, i64)>,
    elapsed: std::time::Duration,
) -> dbflux_core::QueryResult {
    use std::collections::BTreeMap;

    use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, Value};

    if raw.is_empty() {
        let columns = vec![ColumnMeta {
            name: "bucket_ms".to_string(),
            type_name: "INTEGER".to_string(),
            kind: ColumnKind::Timestamp,
            nullable: false,
            is_primary_key: false,
        }];
        return QueryResult::table(columns, vec![], None, elapsed);
    }

    // Collect distinct group labels in sorted order, and accumulate counts
    // keyed by (bucket_ms, group_label).
    let mut group_set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut counts: BTreeMap<i64, BTreeMap<String, i64>> = BTreeMap::new();

    for (bucket_ms, label, count) in raw {
        group_set.insert(label.clone());
        counts.entry(bucket_ms).or_default().insert(label, count);
    }

    let group_labels: Vec<String> = group_set.into_iter().collect();

    // Build column metadata: bucket_ms + one Integer column per group.
    let mut columns = vec![ColumnMeta {
        name: "bucket_ms".to_string(),
        type_name: "INTEGER".to_string(),
        kind: ColumnKind::Timestamp,
        nullable: false,
        is_primary_key: false,
    }];
    for label in &group_labels {
        columns.push(ColumnMeta {
            name: label.clone(),
            type_name: "INTEGER".to_string(),
            kind: ColumnKind::Integer,
            nullable: false,
            is_primary_key: false,
        });
    }

    // Build rows: one per distinct bucket, 0-fill missing groups.
    let rows: Vec<Vec<Value>> = counts
        .into_iter()
        .map(|(bucket_ms, group_counts)| {
            let mut row = vec![Value::Int(bucket_ms)];
            for label in &group_labels {
                let count = group_counts.get(label).copied().unwrap_or(0);
                row.push(Value::Int(count));
            }
            row
        })
        .collect();

    QueryResult::table(columns, rows, None, elapsed)
}

/// Implement `EventSink` for `AuditService`.
///
/// This allows services to emit audit events through the `EventSink` trait
/// interface, which is the primary way service layers emit events.
/// Maps `EventSeverity` to the level code ordinal used by the tracing bridge's
/// `AtomicU8` gate. Mirrors `LevelCode` without requiring the `tracing-bridge`
/// feature flag on this crate.
fn severity_to_level_code(level: dbflux_core::observability::EventSeverity) -> u8 {
    use dbflux_core::observability::EventSeverity;
    match level {
        EventSeverity::Trace => 0,
        EventSeverity::Debug => 1,
        EventSeverity::Info => 2,
        EventSeverity::Warn => 3,
        EventSeverity::Error | EventSeverity::Fatal => 4,
    }
}

impl CoreEventSink for AuditService {
    fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
        AuditService::record(self, event).map_err(|e| e.into())
    }
}

pub fn temp_sqlite_path(file_name: &str) -> PathBuf {
    std::env::temp_dir().join(file_name)
}

#[cfg(test)]
mod tests {
    use dbflux_core::{ColumnKind, Value};
    use dbflux_storage::AuditQueryFilter as StorageFilter;

    use super::*;

    fn make_service(name: &str) -> AuditService {
        let path = std::env::temp_dir().join(format!(
            "dbflux_service_test_{}_{}.db",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        AuditService::new_sqlite(&path).expect("should create service")
    }

    /// Seeds a System-category event (no required fields beyond action/summary).
    fn seed_event(service: &AuditService, ts_ms: i64, outcome: &str) {
        use dbflux_core::observability::{
            EventActorType, EventCategory, EventOutcome, EventRecord, EventSeverity, EventSourceId,
        };

        let event = EventRecord {
            id: None,
            ts_ms,
            level: EventSeverity::Info,
            category: EventCategory::System,
            action: "test_action".to_string(),
            outcome: match outcome {
                "failure" => EventOutcome::Failure,
                _ => EventOutcome::Success,
            },
            actor_type: EventActorType::User,
            actor_id: Some("test".to_string()),
            source_id: EventSourceId::Local,
            summary: "test".to_string(),
            connection_id: None,
            database_name: None,
            driver_id: None,
            object_type: None,
            object_id: None,
            details_json: None,
            error_code: None,
            error_message: None,
            duration_ms: None,
            session_id: None,
            correlation_id: None,
        };

        service.record(event).expect("record should succeed");
    }

    // T-SVC-01: wide schema — bucket_ms column + one Integer column per distinct group value.
    #[test]
    fn service_aggregate_wide_schema() {
        let service = make_service("wide_schema");

        seed_event(&service, 0, "success");
        seed_event(&service, 1_000, "failure");

        let params = AuditAggregateParams {
            bucket_ms: 60_000,
            group_by: AuditGroupColumn::Outcome,
            filter: StorageFilter::default(),
        };

        let result = service
            .aggregate(&params)
            .expect("aggregate should succeed");

        // Wide format: bucket_ms + one column per distinct group value.
        // "failure" and "success" both fall in bucket 0 — two group columns.
        assert!(
            result.columns.len() >= 2,
            "expected at least 2 columns (bucket_ms + groups)"
        );
        assert_eq!(result.columns[0].name, "bucket_ms");
        assert_eq!(result.columns[0].kind, ColumnKind::Timestamp);

        // All non-bucket columns must be ColumnKind::Integer.
        for col in result.columns.iter().skip(1) {
            assert_eq!(
                col.kind,
                ColumnKind::Integer,
                "group column '{}' must be Integer",
                col.name
            );
        }
    }

    // T-SVC-02: distinct values become columns in sorted (ascending) order.
    #[test]
    fn service_aggregate_columns_sorted_ascending() {
        let service = make_service("sorted_cols");

        seed_event(&service, 0, "success");
        seed_event(&service, 1_000, "failure");

        let params = AuditAggregateParams {
            bucket_ms: 60_000,
            group_by: AuditGroupColumn::Outcome,
            filter: StorageFilter::default(),
        };

        let result = service
            .aggregate(&params)
            .expect("aggregate should succeed");

        // Group columns after bucket_ms must be in sorted ascending order.
        let group_names: Vec<&str> = result
            .columns
            .iter()
            .skip(1)
            .map(|c| c.name.as_str())
            .collect();
        let mut sorted = group_names.clone();
        sorted.sort_unstable();
        assert_eq!(
            group_names, sorted,
            "group columns must be in sorted ascending order"
        );
    }

    // T-SVC-03: 0-fill for missing (bucket, group) combinations.
    #[test]
    fn service_aggregate_zero_fill_for_missing_combos() {
        let service = make_service("zero_fill");

        // Bucket 0: only "success". Bucket 60_001: only "failure".
        // Wide result should have both group columns in both rows, with 0 where absent.
        seed_event(&service, 0, "success");
        seed_event(&service, 60_001, "failure");

        let params = AuditAggregateParams {
            bucket_ms: 60_000,
            group_by: AuditGroupColumn::Outcome,
            filter: StorageFilter::default(),
        };

        let result = service
            .aggregate(&params)
            .expect("aggregate should succeed");

        // Must have exactly 2 group columns (failure + success, sorted) and 2 rows.
        assert_eq!(
            result.columns.len(),
            3,
            "expected bucket_ms + 2 group columns"
        );
        assert_eq!(result.rows.len(), 2, "expected 2 bucket rows");

        // Find which column index is "failure" and which is "success".
        let failure_col = result
            .columns
            .iter()
            .position(|c| c.name == "failure")
            .expect("failure column");
        let success_col = result
            .columns
            .iter()
            .position(|c| c.name == "success")
            .expect("success column");

        // Row 0 is bucket 0 (only "success") — "failure" cell must be 0.
        assert!(
            matches!(result.rows[0][failure_col], Value::Int(0)),
            "failure count in bucket-0 row must be 0, got {:?}",
            result.rows[0][failure_col]
        );
        assert!(
            matches!(result.rows[0][success_col], Value::Int(1)),
            "success count in bucket-0 row must be 1, got {:?}",
            result.rows[0][success_col]
        );

        // Row 1 is the next bucket (only "failure") — "success" cell must be 0.
        assert!(
            matches!(result.rows[1][success_col], Value::Int(0)),
            "success count in bucket-1 row must be 0, got {:?}",
            result.rows[1][success_col]
        );
        assert!(
            matches!(result.rows[1][failure_col], Value::Int(1)),
            "failure count in bucket-1 row must be 1, got {:?}",
            result.rows[1][failure_col]
        );
    }

    // T-SVC-04: counts land in the correct (bucket, group) cell.
    #[test]
    fn service_aggregate_counts_in_correct_cells() {
        let service = make_service("counts_cells");

        // All events in the same bucket to simplify assertions.
        seed_event(&service, 0, "success");
        seed_event(&service, 1_000, "success");
        seed_event(&service, 2_000, "failure");

        let params = AuditAggregateParams {
            bucket_ms: 60_000,
            group_by: AuditGroupColumn::Outcome,
            filter: StorageFilter::default(),
        };

        let result = service
            .aggregate(&params)
            .expect("aggregate should succeed");

        // Single bucket row; failure=1, success=2.
        assert_eq!(result.rows.len(), 1, "all events fit in one bucket");

        let failure_col = result
            .columns
            .iter()
            .position(|c| c.name == "failure")
            .expect("failure col");
        let success_col = result
            .columns
            .iter()
            .position(|c| c.name == "success")
            .expect("success col");

        assert!(
            matches!(result.rows[0][failure_col], Value::Int(1)),
            "failure count must be 1, got {:?}",
            result.rows[0][failure_col]
        );
        assert!(
            matches!(result.rows[0][success_col], Value::Int(2)),
            "success count must be 2, got {:?}",
            result.rows[0][success_col]
        );
    }

    // T-SVC-05: single-group result has one Integer column beside bucket_ms.
    #[test]
    fn service_aggregate_single_group_case() {
        let service = make_service("single_group");

        seed_event(&service, 0, "success");
        seed_event(&service, 1_000, "success");

        let params = AuditAggregateParams {
            bucket_ms: 60_000,
            group_by: AuditGroupColumn::Outcome,
            filter: StorageFilter::default(),
        };

        let result = service
            .aggregate(&params)
            .expect("aggregate should succeed");

        // Only "success" group → exactly 2 columns.
        assert_eq!(
            result.columns.len(),
            2,
            "expected bucket_ms + 1 group column"
        );
        assert_eq!(result.columns[1].name, "success");
        assert_eq!(result.columns[1].kind, ColumnKind::Integer);
    }

    // T-SVC-06: empty raw input returns only the bucket_ms column, no rows, no panic.
    #[test]
    fn service_aggregate_empty_input_no_panic() {
        let service = make_service("empty_input");

        // No events — aggregate over an empty store.
        let params = AuditAggregateParams {
            bucket_ms: 60_000,
            group_by: AuditGroupColumn::Outcome,
            filter: StorageFilter::default(),
        };

        let result = service
            .aggregate(&params)
            .expect("aggregate on empty store should succeed");

        assert_eq!(
            result.columns.len(),
            1,
            "empty input must yield only bucket_ms column"
        );
        assert_eq!(result.columns[0].name, "bucket_ms");
        assert!(result.rows.is_empty(), "empty input must yield no rows");
    }

    #[test]
    fn test_drop_counter_starts_at_zero() {
        let service = make_service("drop_counter_zero");
        assert_eq!(service.external_audit_dropped_count(), 0);
    }

    #[test]
    fn test_drop_counter_shared_arc_increments_visible_via_service() {
        let service = make_service("drop_counter_arc");
        let counter = service.external_audit_drop_counter();
        counter.fetch_add(3, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(service.external_audit_dropped_count(), 3);
    }

    #[test]
    fn test_validate_event_accepts_external_driver() {
        use dbflux_core::observability::{
            EventActorType, EventCategory, EventOutcome, EventRecord, EventSeverity, EventSourceId,
        };

        let event = EventRecord {
            id: None,
            ts_ms: 1700000000000,
            level: EventSeverity::Info,
            category: EventCategory::System,
            action: "driver_action".to_string(),
            outcome: EventOutcome::Success,
            actor_type: EventActorType::ExternalDriver,
            actor_id: Some("rpc:test-driver".to_string()),
            source_id: EventSourceId::ExternalDriver,
            summary: "driver did something".to_string(),
            connection_id: None,
            database_name: None,
            driver_id: Some("rpc:test-driver".to_string()),
            object_type: None,
            object_id: None,
            details_json: None,
            error_code: None,
            error_message: None,
            duration_ms: None,
            session_id: None,
            correlation_id: None,
        };

        AuditService::validate_event(&event).expect("ExternalDriver events must pass validation");
    }

    #[test]
    fn test_validate_event_accepts_external_auth_provider() {
        use dbflux_core::observability::{
            EventActorType, EventCategory, EventOutcome, EventRecord, EventSeverity, EventSourceId,
        };

        let event = EventRecord {
            id: None,
            ts_ms: 1700000000000,
            level: EventSeverity::Info,
            category: EventCategory::Connection,
            action: "auth.login".to_string(),
            outcome: EventOutcome::Success,
            actor_type: EventActorType::ExternalAuthProvider,
            actor_id: Some("my-provider".to_string()),
            source_id: EventSourceId::ExternalAuthProvider,
            summary: "provider login completed".to_string(),
            connection_id: None,
            database_name: None,
            driver_id: None,
            object_type: None,
            object_id: None,
            details_json: None,
            error_code: None,
            error_message: None,
            duration_ms: None,
            session_id: None,
            correlation_id: None,
        };

        AuditService::validate_event(&event)
            .expect("ExternalAuthProvider Connection events must pass validation");
    }
}
