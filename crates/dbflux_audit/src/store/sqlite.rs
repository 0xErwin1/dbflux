//! SQLite-backed audit store implementation.
//!
//! Delegates to `dbflux_storage::AuditRepository` for actual storage.
//! The `aud_audit_events` table is created by the unified schema migration
//! in `dbflux_storage::migrations::mod_001_initial`.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use dbflux_core::observability::EventRecord;
use dbflux_storage::migrations::aud_schema;
use dbflux_storage::repositories::audit::AuditEventDto;
use dbflux_storage::{
    AppendAuditEventExtended, AuditAggregateParams, AuditQueryFilter as StorageAuditQueryFilter,
    AuditRepository, error::RepositoryError,
};
use rusqlite::Connection;

use crate::query::AuditQueryFilter;
use crate::{AuditError, AuditEvent};

fn to_audit_error(e: RepositoryError) -> AuditError {
    match e {
        RepositoryError::Sqlite { source } => AuditError::Sqlite(source),
        RepositoryError::NotFound(msg) => AuditError::NotFound(msg),
        RepositoryError::Validation(msg) => AuditError::Validation(msg),
        RepositoryError::Serialization { source: _ } => {
            AuditError::Sqlite(rusqlite::Error::InvalidQuery)
        }
    }
}

/// SQLite-backed audit store.
///
/// Wraps `dbflux_storage::AuditRepository` to provide the same interface
/// as before while delegating storage to the unified database.
#[derive(Clone)]
pub struct SqliteAuditStore {
    repo: AuditRepository,
    path: PathBuf,
}

impl SqliteAuditStore {
    fn legacy_tool_id(dto: &AuditEventDto) -> String {
        dto.legacy_tool_id()
    }

    fn legacy_decision(dto: &AuditEventDto) -> String {
        dto.legacy_decision()
    }

    fn projected_legacy_tool_id(event: &EventRecord) -> String {
        AuditEventDto::project_legacy_tool_id(Some(event.action.as_str()), None)
    }

    fn projected_legacy_decision(event: &EventRecord) -> String {
        AuditEventDto::project_legacy_decision(
            Some(event.action.as_str()),
            Some(event.outcome.as_str()),
            None,
        )
    }

    fn to_storage_filter(filter: &AuditQueryFilter) -> StorageAuditQueryFilter {
        StorageAuditQueryFilter {
            id: None,
            actor_id: filter.actor_id.clone(),
            tool_id: filter.tool_id.clone(),
            decision: filter.decision.clone(),
            profile_id: None,
            classification: None,
            start_epoch_ms: filter.start_epoch_ms,
            end_epoch_ms: filter.end_epoch_ms,
            limit: filter.limit,
            offset: None,
            level: filter.level.clone(),
            levels: None,
            category: filter.category.clone(),
            action: filter.action.clone(),
            categories: None,
            source_id: filter.source_id.clone(),
            outcome: filter.outcome.clone(),
            outcomes: None,
            connection_id: None,
            driver_id: None,
            actor_type: None,
            object_type: filter.object_type.clone(),
            free_text: filter.free_text.clone(),
            correlation_id: filter.correlation_id.clone(),
            session_id: None,
        }
    }

    /// Creates a new store backed by the database at the given path.
    ///
    /// The `aud_audit_events` table must already exist (created by dbflux_storage migrations).
    pub fn new(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        let path = path.as_ref().to_path_buf();

        // Open the database and run migrations if needed
        let conn = Connection::open(&path)?;

        // Wait on a contended database instead of failing immediately with
        // SQLITE_BUSY. The audit database can be opened concurrently (it shares
        // the WAL file with StorageRuntime, and tests may race on a shared temp
        // path), so a busy timeout serializes these openers rather than
        // surfacing a spurious "database is locked" error.
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(AuditError::Sqlite)?;

        // Enable WAL mode to be compatible with StorageRuntime's database configuration.
        // This must be done before any other operations to ensure proper isolation.
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(AuditError::Sqlite)?;

        aud_schema::create_aud_audit_events(&conn, false)?;

        dbflux_storage::paths::secure_file_permissions(&path)
            .map_err(|e| AuditError::Io(std::io::Error::other(e.to_string())))?;

        // Wrap in Arc<Mutex<Connection>> for AuditRepository
        let conn = Arc::new(Mutex::new(conn));
        let repo = AuditRepository::new(conn);

        Ok(Self { repo, path })
    }

    /// Returns the database path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Gets an audit event by ID.
    pub fn get(&self, id: i64) -> Result<Option<AuditEvent>, AuditError> {
        let dto = self.repo.find_by_id(id).map_err(to_audit_error)?;
        Ok(dto.map(|d| {
            let tool_id = Self::legacy_tool_id(&d);
            let decision = Self::legacy_decision(&d);

            AuditEvent {
                id: d.id,
                actor_id: d.actor_id,
                tool_id,
                decision,
                reason: d.reason,
                created_at_epoch_ms: d.created_at_epoch_ms,
            }
        }))
    }

    /// Gets an audit event by ID with the full canonical DTO shape.
    pub fn get_extended(&self, id: i64) -> Result<Option<AuditEventDto>, AuditError> {
        match self.repo.find_by_id(id) {
            Ok(Some(dto)) => Ok(Some(dto)),
            Ok(None) => Ok(None),
            Err(RepositoryError::NotFound(_)) => Ok(None),
            Err(e) => Err(to_audit_error(e)),
        }
    }

    /// Queries audit events with the given filter.
    pub fn query(&self, filter: &AuditQueryFilter) -> Result<Vec<AuditEvent>, AuditError> {
        let mut dtos = self
            .repo
            .query(&Self::to_storage_filter(filter))
            .map_err(to_audit_error)?;

        dtos.reverse();

        Ok(dtos
            .into_iter()
            .map(|d| {
                let tool_id = Self::legacy_tool_id(&d);
                let decision = Self::legacy_decision(&d);

                AuditEvent {
                    id: d.id,
                    actor_id: d.actor_id,
                    tool_id,
                    decision,
                    reason: d.reason,
                    created_at_epoch_ms: d.created_at_epoch_ms,
                }
            })
            .collect())
    }

    /// Queries audit events with the full canonical DTO shape.
    pub fn query_extended(
        &self,
        filter: &AuditQueryFilter,
    ) -> Result<Vec<AuditEventDto>, AuditError> {
        let mut dtos = self
            .repo
            .query(&Self::to_storage_filter(filter))
            .map_err(to_audit_error)?;

        dtos.reverse();

        Ok(dtos)
    }

    /// Records an audit event using the extended schema.
    ///
    /// This is the primary method for recording events from service layers.
    /// The event is validated and stored with the full RF-050/RF-051 schema.
    pub fn record(&self, event: EventRecord) -> Result<EventRecord, AuditError> {
        let legacy_tool_id = Self::projected_legacy_tool_id(&event);
        let legacy_decision = Self::projected_legacy_decision(&event);

        // Build the extended event for storage
        let extended_event = AppendAuditEventExtended {
            actor_id: event.actor_id.as_deref().unwrap_or("system"),
            tool_id: &legacy_tool_id,
            decision: &legacy_decision,
            reason: event
                .error_message
                .as_deref()
                .filter(|_| event.action == "mcp_reject_execution"),
            profile_id: None,
            classification: None,
            duration_ms: event.duration_ms,
            created_at_epoch_ms: event.ts_ms,
            level: Some(event.level.as_str()),
            category: Some(event.category.as_str()),
            action: Some(&event.action),
            outcome: Some(event.outcome.as_str()),
            actor_type: Some(event.actor_type.as_str()),
            source_id: Some(event.source_id.as_str()),
            summary: Some(&event.summary),
            connection_id: event.connection_id.as_deref(),
            database_name: event.database_name.as_deref(),
            driver_id: event.driver_id.as_deref(),
            object_type: event.object_type.as_deref(),
            object_id: event.object_id.as_deref(),
            details_json: event.details_json.as_deref(),
            error_code: event.error_code.as_deref(),
            error_message: event.error_message.as_deref(),
            session_id: event.session_id.as_deref(),
            correlation_id: event.correlation_id.as_deref(),
        };

        let dto = self
            .repo
            .append_extended(extended_event)
            .map_err(to_audit_error)?;

        // Return the event with the assigned ID
        let mut result = event;
        result.id = Some(dto.id);
        Ok(result)
    }

    /// Records a panic event without blocking.
    ///
    /// This is the store-level implementation for panic-hook integration.
    /// If the lock cannot be acquired, logs to stderr and returns `Ok(None)`.
    pub fn record_panic_best_effort(
        &self,
        event: EventRecord,
        panic_info: &str,
    ) -> Result<Option<EventRecord>, AuditError> {
        let legacy_tool_id = Self::projected_legacy_tool_id(&event);
        let legacy_decision = Self::projected_legacy_decision(&event);

        let extended_event = AppendAuditEventExtended {
            actor_id: event.actor_id.as_deref().unwrap_or("system"),
            tool_id: &legacy_tool_id,
            decision: &legacy_decision,
            reason: None,
            profile_id: None,
            classification: None,
            duration_ms: event.duration_ms,
            created_at_epoch_ms: event.ts_ms,
            level: Some(event.level.as_str()),
            category: Some(event.category.as_str()),
            action: Some(&event.action),
            outcome: Some(event.outcome.as_str()),
            actor_type: Some(event.actor_type.as_str()),
            source_id: Some(event.source_id.as_str()),
            summary: Some(&event.summary),
            connection_id: event.connection_id.as_deref(),
            database_name: event.database_name.as_deref(),
            driver_id: event.driver_id.as_deref(),
            object_type: event.object_type.as_deref(),
            object_id: event.object_id.as_deref(),
            details_json: event.details_json.as_deref(),
            error_code: event.error_code.as_deref(),
            error_message: event.error_message.as_deref(),
            session_id: event.session_id.as_deref(),
            correlation_id: event.correlation_id.as_deref(),
        };

        match self.repo.try_record_panic(extended_event, panic_info) {
            Ok(Some(_id)) => {
                let mut result = event;
                result.id = Some(_id);
                Ok(Some(result))
            }
            Ok(None) => {
                // Lock not available — fallback already logged in repo
                Ok(None)
            }
            Err(e) => {
                eprintln!("[dbflux_audit] panic recording failed (non-fatal): {:?}", e);
                Ok(None)
            }
        }
    }

    /// Aggregates audit events into time buckets grouped by a chosen column.
    ///
    /// Delegates to `AuditRepository::aggregate`. The `params.filter` uses
    /// the storage `AuditQueryFilter` directly (same type), so no conversion
    /// is needed here.
    ///
    /// Returns `(bucket_ms, group_label, count)` tuples ordered by bucket
    /// ascending.
    pub fn aggregate(
        &self,
        params: &AuditAggregateParams,
    ) -> Result<Vec<(i64, String, i64)>, AuditError> {
        self.repo.aggregate(params).map_err(to_audit_error)
    }

    /// Deletes audit events older than the given cutoff timestamp.
    ///
    /// ## Arguments
    ///
    /// * `cutoff_ms` - Unix timestamp in milliseconds
    /// * `limit` - Maximum number of events to delete
    ///
    /// ## Returns
    ///
    /// The number of events deleted.
    pub fn delete_older_than(&self, cutoff_ms: i64, limit: usize) -> Result<i64, AuditError> {
        self.repo
            .delete_older_than(cutoff_ms, limit)
            .map_err(to_audit_error)
    }

    /// Persists the tracing bridge capture threshold to `cfg_audit_settings`.
    ///
    /// This table lives in the same `dbflux.db` file as the audit events, so
    /// the write goes through the same connection pool without extra dependencies.
    pub fn update_log_capture_min_level(&self, level: &str) -> Result<(), AuditError> {
        self.repo
            .update_log_capture_min_level(level)
            .map_err(to_audit_error)
    }
}

#[cfg(test)]
mod tests {
    use dbflux_storage::AuditGroupColumn;

    use super::*;

    fn temp_store(name: &str) -> SqliteAuditStore {
        let path = std::env::temp_dir().join(format!(
            "dbflux_audit_store_{}_{}.db",
            name,
            std::process::id()
        ));

        let _ = std::fs::remove_file(&path);

        SqliteAuditStore::new(&path).expect("should open store")
    }

    fn insert_event_in_store(
        store: &SqliteAuditStore,
        ts_ms: i64,
        category: Option<&str>,
        outcome: Option<&str>,
        _level: Option<&str>,
    ) {
        use dbflux_core::observability::{
            EventActorType, EventCategory, EventOutcome, EventRecord, EventSeverity, EventSourceId,
        };

        let event = EventRecord {
            id: None,
            ts_ms,
            level: EventSeverity::Info,
            category: match category {
                Some("query") => EventCategory::Query,
                Some("connection") => EventCategory::Connection,
                Some("config") => EventCategory::Config,
                _ => EventCategory::System,
            },
            action: "test_action".to_string(),
            outcome: match outcome {
                Some("failure") => EventOutcome::Failure,
                _ => EventOutcome::Success,
            },
            actor_type: EventActorType::User,
            actor_id: Some("test".to_string()),
            source_id: EventSourceId::Local,
            summary: "test event".to_string(),
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

        store.record(event).expect("record should succeed");
    }

    #[test]
    fn sqlite_store_aggregate_delegates() {
        let store = temp_store("delegate");

        // Insert 3 events — all fall in the same bucket (bucket_ms=60_000)
        insert_event_in_store(&store, 1_000, Some("query"), Some("success"), Some("info"));
        insert_event_in_store(&store, 2_000, Some("query"), Some("success"), Some("info"));
        insert_event_in_store(
            &store,
            3_000,
            Some("connection"),
            Some("success"),
            Some("info"),
        );

        let params = AuditAggregateParams {
            bucket_ms: 60_000,
            group_by: AuditGroupColumn::Category,
            filter: StorageAuditQueryFilter::default(),
        };

        let results = store.aggregate(&params).expect("aggregate should succeed");

        // 2 distinct category values in bucket 0
        assert_eq!(results.len(), 2);

        let query_row = results.iter().find(|(_, l, _)| l == "query");
        assert!(query_row.is_some());
        assert_eq!(query_row.unwrap().2, 2);

        let connection_row = results.iter().find(|(_, l, _)| l == "connection");
        assert!(connection_row.is_some());
        assert_eq!(connection_row.unwrap().2, 1);
    }
}
