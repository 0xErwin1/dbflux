//! SQLite-backed audit store implementation.
//!
//! Delegates to `dbflux_storage::AuditRepository` for actual storage.
//! The `aud_audit_events` table is created by the unified schema migration
//! in `dbflux_storage::migrations::mod_001_initial`.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use dbflux_storage::{
    AppendAuditEvent, AuditQueryFilter as StorageAuditQueryFilter, AuditRepository,
    error::RepositoryError,
};
use rusqlite::Connection;

use crate::query::AuditQueryFilter;
use crate::{AuditError, AuditEvent};

/// Converts a RepositoryError to an AuditError.
fn to_audit_error(e: RepositoryError) -> AuditError {
    match e {
        RepositoryError::Sqlite { source } => AuditError::Sqlite(source),
        RepositoryError::NotFound(_msg) => AuditError::Sqlite(rusqlite::Error::InvalidQuery),
        RepositoryError::Serialization { source: _ } => {
            AuditError::Sqlite(rusqlite::Error::InvalidQuery)
        }
    }
}

/// SQLite-backed audit store.
///
/// Wraps `dbflux_storage::AuditRepository` to provide the same interface
/// as before while delegating storage to the unified database.
pub struct SqliteAuditStore {
    repo: AuditRepository,
    path: PathBuf,
}

impl SqliteAuditStore {
    /// Creates a new store backed by the database at the given path.
    ///
    /// The `aud_audit_events` table must already exist (created by dbflux_storage migrations).
    pub fn new(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        let path = path.as_ref().to_path_buf();

        // Open the database and run migrations if needed
        let conn = Connection::open(&path)?;

        // Apply migrations if the table doesn't exist
        let table_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='aud_audit_events'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .unwrap_or(false);

        if !table_exists {
            // Create the table - note: no FK constraint since cfg_connection_profiles
            // may not exist when used standalone (outside of StorageRuntime migrations)
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS aud_audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor_id TEXT NOT NULL,
                    tool_id TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    reason TEXT,
                    profile_id TEXT,
                    classification TEXT,
                    duration_ms INTEGER,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    created_at_epoch_ms INTEGER NOT NULL
                )",
            )?;
        }

        // Wrap in Arc<Mutex<Connection>> for AuditRepository
        let conn = Arc::new(Mutex::new(conn));
        let repo = AuditRepository::new(conn);

        Ok(Self { repo, path })
    }

    /// Returns the database path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends a new audit event.
    pub fn append(
        &self,
        actor_id: &str,
        tool_id: &str,
        decision: &str,
        reason: Option<&str>,
        created_at_epoch_ms: i64,
    ) -> Result<AuditEvent, AuditError> {
        // Note: profile_id, classification, duration_ms are not exposed in the legacy API
        let event = AppendAuditEvent {
            actor_id,
            tool_id,
            decision,
            reason,
            profile_id: None,
            classification: None,
            duration_ms: None,
            created_at_epoch_ms,
        };
        let dto = self.repo.append(event).map_err(to_audit_error)?;

        // Convert to legacy AuditEvent (only the fields that existed before)
        Ok(AuditEvent {
            id: dto.id,
            actor_id: dto.actor_id,
            tool_id: dto.tool_id,
            decision: dto.decision,
            reason: dto.reason,
            created_at_epoch_ms: dto.created_at_epoch_ms,
        })
    }

    /// Gets an audit event by ID.
    pub fn get(&self, id: i64) -> Result<Option<AuditEvent>, AuditError> {
        let dto = self.repo.find_by_id(id).map_err(to_audit_error)?;
        Ok(dto.map(|d| AuditEvent {
            id: d.id,
            actor_id: d.actor_id,
            tool_id: d.tool_id,
            decision: d.decision,
            reason: d.reason,
            created_at_epoch_ms: d.created_at_epoch_ms,
        }))
    }

    /// Queries audit events with the given filter.
    pub fn query(&self, filter: &AuditQueryFilter) -> Result<Vec<AuditEvent>, AuditError> {
        let storage_filter = StorageAuditQueryFilter {
            id: None,
            actor_id: filter.actor_id.clone(),
            tool_id: filter.tool_id.clone(),
            decision: filter.decision.clone(),
            profile_id: None,
            classification: None,
            start_epoch_ms: filter.start_epoch_ms,
            end_epoch_ms: filter.end_epoch_ms,
            limit: filter.limit,
        };

        let dtos = self.repo.query(&storage_filter).map_err(to_audit_error)?;

        Ok(dtos
            .into_iter()
            .map(|d| AuditEvent {
                id: d.id,
                actor_id: d.actor_id,
                tool_id: d.tool_id,
                decision: d.decision,
                reason: d.reason,
                created_at_epoch_ms: d.created_at_epoch_ms,
            })
            .collect())
    }
}
