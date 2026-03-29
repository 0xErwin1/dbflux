//! Repository for recent items in state.db.
//!
//! Tracks recently opened files and connections with access timestamps.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

const MAX_RECENT_ITEMS: usize = 30;

/// Repository for recent items.
pub struct RecentItemsRepository {
    conn: OwnedConnection,
}

impl RecentItemsRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Records a recent file/connection access.
    /// Moves existing entry to front if already present; trims to MAX_RECENT_ITEMS.
    pub fn record_access(&self, dto: &RecentItemDto) -> Result<(), StorageError> {
        // Remove any existing entry with same id
        self.conn()
            .execute("DELETE FROM recent_items WHERE id = ?1", [dto.id.clone()])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        // Insert at front
        self.conn()
            .execute(
                r#"
                INSERT INTO recent_items (id, kind, profile_id, path, title, accessed_at)
                VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))
                "#,
                params![dto.id, dto.kind, dto.profile_id, dto.path, dto.title],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        // Trim to max entries (keep most recently accessed)
        self.conn()
            .execute(
                r#"
                DELETE FROM recent_items
                WHERE id NOT IN (
                    SELECT id FROM recent_items ORDER BY accessed_at DESC LIMIT ?1
                )
                "#,
                [MAX_RECENT_ITEMS as i64],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        info!("Recorded recent access: {} ({})", dto.title, dto.kind);
        Ok(())
    }

    /// Returns all recent items ordered by most recent first.
    pub fn all(&self) -> Result<Vec<RecentItemDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, kind, profile_id, path, title, accessed_at FROM recent_items ORDER BY accessed_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(RecentItemDto {
                    id: row.get(0)?,
                    kind: row.get(1)?,
                    profile_id: row.get(2)?,
                    path: row.get(3)?,
                    title: row.get(4)?,
                    accessed_at: row.get(5)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for row in rows {
            match row {
                Ok(r) => result.push(r),
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "state.db".into(),
                source: e,
            });
        }

        Ok(result)
    }

    /// Removes a recent item by ID.
    pub fn remove(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM recent_items WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    /// Clears all recent items.
    pub fn clear(&self) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM recent_items", [])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }
}

/// DTO for recent items.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentItemDto {
    pub id: String,
    pub kind: String, // e.g., "file", "connection"
    pub profile_id: Option<String>,
    pub path: Option<String>,
    pub title: String,
    pub accessed_at: String,
}

impl RecentItemDto {
    pub fn file(id: Uuid, path: String, title: String) -> Self {
        Self {
            id: id.to_string(),
            kind: "file".to_string(),
            profile_id: None,
            path: Some(path),
            title,
            accessed_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn connection(id: Uuid, profile_id: Uuid, title: String) -> Self {
        Self {
            id: id.to_string(),
            kind: "connection".to_string(),
            profile_id: Some(profile_id.to_string()),
            path: None,
            title,
            accessed_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::state::run_state_migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_recent_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn record_and_list() {
        let path = temp_db("record");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = RecentItemsRepository::new(Arc::new(conn));

        let dto = RecentItemDto::file(
            Uuid::new_v4(),
            "/tmp/test.sql".to_string(),
            "test.sql".to_string(),
        );
        repo.record_access(&dto).expect("should record");

        let all = repo.all().expect("should list");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].title, "test.sql");
    }

    #[test]
    fn remove_and_clear() {
        let path = temp_db("remove");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = RecentItemsRepository::new(Arc::new(conn));

        let id = Uuid::new_v4();
        let dto = RecentItemDto::file(id, "/tmp/test.sql".to_string(), "test.sql".to_string());
        repo.record_access(&dto).expect("should record");

        repo.remove(&id.to_string()).expect("should remove");
        assert_eq!(repo.all().expect("should list").len(), 0);

        repo.record_access(&dto).expect("record again");
        repo.clear().expect("should clear");
        assert_eq!(repo.all().expect("should list").len(), 0);
    }
}
