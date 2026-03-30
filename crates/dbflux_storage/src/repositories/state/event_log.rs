//! Repository for event_log table in state.db.
//!
//! Provides typed access to event_log entries, with native columns for
//! common query fields (actor_id, tool_id, decision, duration_ms)
//! extracted from the details_json blob.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Event log entry DTO — reflects the native columns in the event_log table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLogDto {
    pub id: String,
    pub event_kind: String,
    pub description: String,
    pub target_kind: Option<String>,
    pub target_id: Option<String>,
    pub actor_id: Option<String>,
    pub tool_id: Option<String>,
    pub decision: Option<String>,
    pub duration_ms: Option<i64>,
    pub details_json: Option<String>,
    pub created_at: String,
}

/// Repository for event_log entries.
pub struct EventLogRepository {
    conn: OwnedConnection,
}

impl EventLogRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Returns all event log entries ordered by created_at descending.
    pub fn all(&self) -> Result<Vec<EventLogDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, event_kind, description, target_kind, target_id,
                        actor_id, tool_id, decision, duration_ms, details_json, created_at
                 FROM event_log ORDER BY created_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(EventLogDto {
                    id: row.get(0)?,
                    event_kind: row.get(1)?,
                    description: row.get(2)?,
                    target_kind: row.get(3)?,
                    target_id: row.get(4)?,
                    actor_id: row.get(5)?,
                    tool_id: row.get(6)?,
                    decision: row.get(7)?,
                    duration_ms: row.get(8)?,
                    details_json: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        for row in rows {
            match row {
                Ok(r) => result.push(r),
                Err(e) => {
                    return Err(StorageError::Sqlite {
                        path: "state.db".into(),
                        source: e,
                    });
                }
            }
        }
        Ok(result)
    }

    /// Returns event log entries filtered by actor_id.
    pub fn by_actor(&self, actor_id: &str) -> Result<Vec<EventLogDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, event_kind, description, target_kind, target_id,
                        actor_id, tool_id, decision, duration_ms, details_json, created_at
                 FROM event_log WHERE actor_id = ?1 ORDER BY created_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([actor_id], |row| {
                Ok(EventLogDto {
                    id: row.get(0)?,
                    event_kind: row.get(1)?,
                    description: row.get(2)?,
                    target_kind: row.get(3)?,
                    target_id: row.get(4)?,
                    actor_id: row.get(5)?,
                    tool_id: row.get(6)?,
                    decision: row.get(7)?,
                    duration_ms: row.get(8)?,
                    details_json: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        for row in rows {
            match row {
                Ok(r) => result.push(r),
                Err(e) => {
                    return Err(StorageError::Sqlite {
                        path: "state.db".into(),
                        source: e,
                    });
                }
            }
        }
        Ok(result)
    }

    /// Returns event log entries filtered by tool_id.
    pub fn by_tool(&self, tool_id: &str) -> Result<Vec<EventLogDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, event_kind, description, target_kind, target_id,
                        actor_id, tool_id, decision, duration_ms, details_json, created_at
                 FROM event_log WHERE tool_id = ?1 ORDER BY created_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([tool_id], |row| {
                Ok(EventLogDto {
                    id: row.get(0)?,
                    event_kind: row.get(1)?,
                    description: row.get(2)?,
                    target_kind: row.get(3)?,
                    target_id: row.get(4)?,
                    actor_id: row.get(5)?,
                    tool_id: row.get(6)?,
                    decision: row.get(7)?,
                    duration_ms: row.get(8)?,
                    details_json: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        for row in rows {
            match row {
                Ok(r) => result.push(r),
                Err(e) => {
                    return Err(StorageError::Sqlite {
                        path: "state.db".into(),
                        source: e,
                    });
                }
            }
        }
        Ok(result)
    }

    /// Inserts a new event log entry.
    pub fn insert(&self, dto: &EventLogDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO event_log (id, event_kind, description, target_kind, target_id,
                                       actor_id, tool_id, decision, duration_ms, details_json,
                                       created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, datetime('now'))
                "#,
                params![
                    dto.id,
                    dto.event_kind,
                    dto.description,
                    dto.target_kind,
                    dto.target_id,
                    dto.actor_id,
                    dto.tool_id,
                    dto.decision,
                    dto.duration_ms,
                    dto.details_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        info!("Inserted event log entry: {} ({})", dto.event_kind, dto.id);
        Ok(())
    }

    /// Returns the count of event log entries.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM event_log", [], |row| row.get(0))
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(count)
    }

    /// Clears all event log entries.
    pub fn clear(&self) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM event_log", [])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::state::run_state_migrations;
    use crate::sqlite::open_database;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn temp_db(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_event_log_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn insert_and_list_events() {
        let path = temp_db("insert");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = EventLogRepository::new(Arc::new(conn));

        let dto = EventLogDto {
            id: uuid::Uuid::new_v4().to_string(),
            event_kind: "governance.decision".to_string(),
            description: "Tool execution allowed".to_string(),
            target_kind: Some("tool".to_string()),
            target_id: Some("select_data".to_string()),
            actor_id: Some("mcp-client-1".to_string()),
            tool_id: Some("select_data".to_string()),
            decision: Some("allow".to_string()),
            duration_ms: Some(42),
            details_json: Some(r#"{"extra":"data"}"#.to_string()),
            created_at: String::new(),
        };

        repo.insert(&dto).expect("should insert");
        let all = repo.all().expect("should list");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].event_kind, "governance.decision");
        assert_eq!(all[0].actor_id.as_deref(), Some("mcp-client-1"));
        assert_eq!(all[0].decision.as_deref(), Some("allow"));
    }

    #[test]
    fn by_actor_filters_correctly() {
        let path = temp_db("by_actor");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = EventLogRepository::new(Arc::new(conn));

        let dto1 = EventLogDto {
            id: uuid::Uuid::new_v4().to_string(),
            event_kind: "test".to_string(),
            description: "Event 1".to_string(),
            target_kind: None,
            target_id: None,
            actor_id: Some("actor-a".to_string()),
            tool_id: None,
            decision: None,
            duration_ms: None,
            details_json: None,
            created_at: String::new(),
        };
        let dto2 = EventLogDto {
            id: uuid::Uuid::new_v4().to_string(),
            event_kind: "test".to_string(),
            description: "Event 2".to_string(),
            target_kind: None,
            target_id: None,
            actor_id: Some("actor-b".to_string()),
            tool_id: None,
            decision: None,
            duration_ms: None,
            details_json: None,
            created_at: String::new(),
        };

        repo.insert(&dto1).expect("insert 1");
        repo.insert(&dto2).expect("insert 2");

        let by_a = repo.by_actor("actor-a").expect("query");
        assert_eq!(by_a.len(), 1);
        assert_eq!(by_a[0].description, "Event 1");
    }

    #[test]
    fn count_and_clear() {
        let path = temp_db("count");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = EventLogRepository::new(Arc::new(conn));

        assert_eq!(repo.count().expect("count"), 0);

        let dto = EventLogDto {
            id: uuid::Uuid::new_v4().to_string(),
            event_kind: "test".to_string(),
            description: "Test".to_string(),
            target_kind: None,
            target_id: None,
            actor_id: None,
            tool_id: None,
            decision: None,
            duration_ms: None,
            details_json: None,
            created_at: String::new(),
        };
        repo.insert(&dto).expect("insert");
        assert_eq!(repo.count().expect("count"), 1);

        repo.clear().expect("clear");
        assert_eq!(repo.count().expect("count"), 0);
    }
}
