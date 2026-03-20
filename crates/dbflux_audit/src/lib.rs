pub mod export;
pub mod query;
pub mod store;

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::export::{AuditExportFormat, export_entries};
use crate::query::AuditQueryFilter;
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
}

pub struct AuditService {
    store: SqliteAuditStore,
}

impl AuditService {
    pub fn new(store: SqliteAuditStore) -> Self {
        Self { store }
    }

    pub fn new_sqlite_default() -> Result<Self, AuditError> {
        let config_dir = dirs::config_dir().ok_or(AuditError::ConfigDirUnavailable)?;
        let db_dir = config_dir.join("dbflux");
        std::fs::create_dir_all(&db_dir)?;

        let store = SqliteAuditStore::new(db_dir.join("audit.sqlite"))?;
        Ok(Self::new(store))
    }

    pub fn new_sqlite(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        Ok(Self::new(SqliteAuditStore::new(path)?))
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
}

pub fn temp_sqlite_path(file_name: &str) -> PathBuf {
    std::env::temp_dir().join(file_name)
}
