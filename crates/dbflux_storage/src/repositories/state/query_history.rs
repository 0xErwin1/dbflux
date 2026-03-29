//! Repository for query history in state.db.
//!
//! Stores individual query executions with timing, results, and favorites.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for query history entries.
pub struct QueryHistoryRepository {
    conn: OwnedConnection,
    max_entries: usize,
}

impl QueryHistoryRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self {
            conn,
            max_entries: 1000,
        }
    }

    pub fn with_max_entries(conn: OwnedConnection, max_entries: usize) -> Self {
        Self { conn, max_entries }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Adds a new history entry (inserts at front).
    pub fn add(&self, dto: &QueryHistoryDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO query_history (
                    id, connection_profile_id, driver_id, database_name,
                    query_text, query_kind, executed_at, duration_ms,
                    succeeded, error_summary, row_count, is_favorite
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                "#,
                params![
                    dto.id,
                    dto.connection_profile_id,
                    dto.driver_id,
                    dto.database_name,
                    dto.query_text,
                    dto.query_kind,
                    dto.executed_at,
                    dto.duration_ms,
                    dto.succeeded as i32,
                    dto.error_summary,
                    dto.row_count,
                    dto.is_favorite as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        // Trim to max entries, preserving favorites
        self.trim_to_max().map_err(|source| StorageError::Sqlite {
            path: "state.db".into(),
            source,
        })?;

        info!("Added query history entry: {}", dto.id);
        Ok(())
    }

    /// Returns recent history entries.
    pub fn recent(&self, limit: usize) -> Result<Vec<QueryHistoryDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, connection_profile_id, driver_id, database_name, query_text,
                        query_kind, executed_at, duration_ms, succeeded, error_summary,
                        row_count, is_favorite
                 FROM query_history ORDER BY executed_at DESC LIMIT ?1",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([limit as i64], |row| {
                Ok(QueryHistoryDto {
                    id: row.get(0)?,
                    connection_profile_id: row.get(1)?,
                    driver_id: row.get(2)?,
                    database_name: row.get(3)?,
                    query_text: row.get(4)?,
                    query_kind: row.get(5)?,
                    executed_at: row.get(6)?,
                    duration_ms: row.get(7)?,
                    succeeded: row.get::<_, i32>(8)? != 0,
                    error_summary: row.get(9)?,
                    row_count: row.get(10)?,
                    is_favorite: row.get::<_, i32>(11)? != 0,
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

    /// Returns all entries (for full list).
    pub fn all(&self) -> Result<Vec<QueryHistoryDto>, StorageError> {
        self.recent(10000)
    }

    /// Toggles the favorite flag on an entry.
    pub fn toggle_favorite(&self, id: &str) -> Result<bool, StorageError> {
        let rows = self
            .conn()
            .execute(
                "UPDATE query_history SET is_favorite = NOT is_favorite WHERE id = ?1",
                [id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        // Return new favorite state
        if rows > 0 {
            let mut stmt = self
                .conn()
                .prepare("SELECT is_favorite FROM query_history WHERE id = ?1")
                .map_err(|source| StorageError::Sqlite {
                    path: "state.db".into(),
                    source,
                })?;

            let fav: i32 =
                stmt.query_row([id], |row| row.get(0))
                    .map_err(|source| StorageError::Sqlite {
                        path: "state.db".into(),
                        source,
                    })?;

            Ok(fav != 0)
        } else {
            Ok(false)
        }
    }

    /// Removes a history entry by ID.
    pub fn remove(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM query_history WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    /// Clears all non-favorite entries.
    pub fn clear_non_favorites(&self) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM query_history WHERE is_favorite = 0", [])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    /// Clears all history entries.
    pub fn clear(&self) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM query_history", [])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    /// Searches entries by query text.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<QueryHistoryDto>, StorageError> {
        let pattern = format!("%{}%", query.to_lowercase());
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, connection_profile_id, driver_id, database_name, query_text,
                        query_kind, executed_at, duration_ms, succeeded, error_summary,
                        row_count, is_favorite
                 FROM query_history
                 WHERE LOWER(query_text) LIKE ?1
                 ORDER BY executed_at DESC LIMIT ?2",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map(params![pattern, limit as i64], |row| {
                Ok(QueryHistoryDto {
                    id: row.get(0)?,
                    connection_profile_id: row.get(1)?,
                    driver_id: row.get(2)?,
                    database_name: row.get(3)?,
                    query_text: row.get(4)?,
                    query_kind: row.get(5)?,
                    executed_at: row.get(6)?,
                    duration_ms: row.get(7)?,
                    succeeded: row.get::<_, i32>(8)? != 0,
                    error_summary: row.get(9)?,
                    row_count: row.get(10)?,
                    is_favorite: row.get::<_, i32>(11)? != 0,
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

    /// Returns only favorite entries.
    pub fn favorites(&self) -> Result<Vec<QueryHistoryDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, connection_profile_id, driver_id, database_name, query_text,
                        query_kind, executed_at, duration_ms, succeeded, error_summary,
                        row_count, is_favorite
                 FROM query_history WHERE is_favorite = 1 ORDER BY executed_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(QueryHistoryDto {
                    id: row.get(0)?,
                    connection_profile_id: row.get(1)?,
                    driver_id: row.get(2)?,
                    database_name: row.get(3)?,
                    query_text: row.get(4)?,
                    query_kind: row.get(5)?,
                    executed_at: row.get(6)?,
                    duration_ms: row.get(7)?,
                    succeeded: row.get::<_, i32>(8)? != 0,
                    error_summary: row.get(9)?,
                    row_count: row.get(10)?,
                    is_favorite: row.get::<_, i32>(11)? != 0,
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

    fn trim_to_max(&self) -> Result<(), rusqlite::Error> {
        // Count total entries
        let total: i64 =
            self.conn()
                .query_row("SELECT COUNT(*) FROM query_history", [], |row| row.get(0))?;

        if total as usize <= self.max_entries {
            return Ok(());
        }

        // Keep all favorites plus enough non-favorites to reach max_entries
        let non_fav_keep = self
            .max_entries
            .saturating_sub(self.conn().query_row::<i64, _, _>(
                "SELECT COUNT(*) FROM query_history WHERE is_favorite = 1",
                [],
                |row| row.get(0),
            )? as usize);

        // Delete old non-favorites beyond the keep limit
        self.conn().execute(
            r#"
            DELETE FROM query_history
            WHERE is_favorite = 0
              AND id NOT IN (
                  SELECT id FROM query_history
                  WHERE is_favorite = 0
                  ORDER BY executed_at DESC
                  LIMIT ?1
              )
            "#,
            [non_fav_keep as i64],
        )?;
        Ok(())
    }
}

/// DTO for query history entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryHistoryDto {
    pub id: String,
    pub connection_profile_id: Option<String>,
    pub driver_id: Option<String>,
    pub database_name: Option<String>,
    pub query_text: String,
    pub query_kind: String,
    pub executed_at: String,
    pub duration_ms: Option<i64>,
    pub succeeded: bool,
    pub error_summary: Option<String>,
    pub row_count: Option<i64>,
    pub is_favorite: bool,
}

impl QueryHistoryDto {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_text: String,
        connection_profile_id: Option<String>,
        driver_id: Option<String>,
        database_name: Option<String>,
        query_kind: String,
        duration_ms: Option<i64>,
        succeeded: bool,
        error_summary: Option<String>,
        row_count: Option<i64>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            connection_profile_id,
            driver_id,
            database_name,
            query_text,
            query_kind,
            executed_at: String::new(),
            duration_ms,
            succeeded,
            error_summary,
            row_count,
            is_favorite: false,
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
            "dbflux_repo_history_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn add_and_list() {
        let path = temp_db("add");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = QueryHistoryRepository::new(Arc::new(conn));

        let dto = QueryHistoryDto::new(
            "SELECT * FROM users".to_string(),
            None,
            Some("postgres".to_string()),
            Some("mydb".to_string()),
            "select".to_string(),
            Some(42),
            true,
            None,
            Some(100),
        );
        repo.add(&dto).expect("should add");

        let all = repo.all().expect("should list");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].query_text, "SELECT * FROM users");
    }

    #[test]
    fn toggle_favorite_and_remove() {
        let path = temp_db("fav");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = QueryHistoryRepository::new(Arc::new(conn));

        let dto = QueryHistoryDto::new(
            "SELECT 1".to_string(),
            None,
            None,
            None,
            "select".to_string(),
            None,
            true,
            None,
            None,
        );
        repo.add(&dto).expect("add");

        let id = &dto.id;
        let fav = repo.toggle_favorite(id).expect("should toggle");
        assert!(fav);

        repo.remove(id).expect("should remove");
        assert_eq!(repo.all().expect("should list").len(), 0);
    }

    #[test]
    fn search_finds_entries() {
        let path = temp_db("search");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = QueryHistoryRepository::new(Arc::new(conn));

        repo.add(&QueryHistoryDto::new(
            "SELECT * FROM orders".to_string(),
            None,
            None,
            None,
            "select".to_string(),
            None,
            true,
            None,
            None,
        ))
        .expect("add");
        repo.add(&QueryHistoryDto::new(
            "UPDATE users SET name = 'x'".to_string(),
            None,
            None,
            None,
            "update".to_string(),
            None,
            true,
            None,
            None,
        ))
        .expect("add");

        let results = repo.search("orders", 10).expect("should search");
        assert_eq!(results.len(), 1);
        assert!(results[0].query_text.contains("orders"));
    }

    #[test]
    fn clear_preserves_favorites() {
        let path = temp_db("clear");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = QueryHistoryRepository::new(Arc::new(conn));

        let dto = QueryHistoryDto::new(
            "SELECT 1".to_string(),
            None,
            None,
            None,
            "select".to_string(),
            None,
            true,
            None,
            None,
        );
        repo.add(&dto).expect("add");
        repo.toggle_favorite(&dto.id).expect("fav");

        repo.clear_non_favorites().expect("clear non-fav");
        assert_eq!(repo.all().expect("should list").len(), 1);
    }
}
