//! Repository for saved queries and folders in state.db.
//!
//! Stores named query definitions with folder organization.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for saved queries and their folder organization.
pub struct SavedQueriesRepository {
    conn: OwnedConnection,
}

impl SavedQueriesRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    // --- Folders ---

    /// Creates a new folder.
    pub fn create_folder(&self, dto: &SavedQueryFolderDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO saved_query_folders (id, parent_id, name, position)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![dto.id, dto.parent_id, dto.name, dto.position as i64],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        info!("Created saved query folder: {}", dto.name);
        Ok(())
    }

    /// Returns all folders.
    pub fn folders(&self) -> Result<Vec<SavedQueryFolderDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare("SELECT id, parent_id, name, position, created_at, updated_at FROM saved_query_folders ORDER BY position")
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(SavedQueryFolderDto {
                    id: row.get(0)?,
                    parent_id: row.get(1)?,
                    name: row.get(2)?,
                    position: row.get::<_, i64>(3)? as usize,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
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

    /// Deletes a folder (and orphan children via ON DELETE CASCADE).
    pub fn delete_folder(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM saved_query_folders WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    // --- Queries ---

    /// Inserts a new saved query.
    pub fn insert(&self, dto: &SavedQueryDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO saved_queries (id, folder_id, name, sql, is_favorite, connection_id)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                params![
                    dto.id,
                    dto.folder_id,
                    dto.name,
                    dto.sql,
                    dto.is_favorite as i32,
                    dto.connection_id,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        info!("Inserted saved query: {}", dto.name);
        Ok(())
    }

    /// Updates an existing saved query.
    pub fn update(&self, dto: &SavedQueryDto) -> Result<(), StorageError> {
        let rows = self
            .conn()
            .execute(
                r#"
                UPDATE saved_queries SET
                    folder_id = ?2, name = ?3, sql = ?4, is_favorite = ?5,
                    connection_id = ?6, last_used_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    dto.id,
                    dto.folder_id,
                    dto.name,
                    dto.sql,
                    dto.is_favorite as i32,
                    dto.connection_id,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        if rows == 0 {
            return Err(StorageError::Sqlite {
                path: "state.db".into(),
                source: rusqlite::Error::QueryReturnedNoRows,
            });
        }
        Ok(())
    }

    /// Returns all saved queries ordered by last_used descending.
    pub fn all(&self) -> Result<Vec<SavedQueryDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, folder_id, name, sql, is_favorite, connection_id, created_at, last_used_at
                 FROM saved_queries ORDER BY last_used_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(SavedQueryDto {
                    id: row.get(0)?,
                    folder_id: row.get(1)?,
                    name: row.get(2)?,
                    sql: row.get(3)?,
                    is_favorite: row.get::<_, i32>(4)? != 0,
                    connection_id: row.get(5)?,
                    created_at: row.get(6)?,
                    last_used_at: row.get(7)?,
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

    /// Returns a single saved query by ID.
    pub fn get(&self, id: &str) -> Result<Option<SavedQueryDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, folder_id, name, sql, is_favorite, connection_id, created_at, last_used_at
                 FROM saved_queries WHERE id = ?1",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        match stmt.query_row([id], |row| {
            Ok(SavedQueryDto {
                id: row.get(0)?,
                folder_id: row.get(1)?,
                name: row.get(2)?,
                sql: row.get(3)?,
                is_favorite: row.get::<_, i32>(4)? != 0,
                connection_id: row.get(5)?,
                created_at: row.get(6)?,
                last_used_at: row.get(7)?,
            })
        }) {
            Ok(dto) => Ok(Some(dto)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "state.db".into(),
                source: e,
            }),
        }
    }

    /// Deletes a saved query by ID.
    pub fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM saved_queries WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    /// Toggles favorite flag.
    pub fn toggle_favorite(&self, id: &str) -> Result<bool, StorageError> {
        self.conn()
            .execute(
                "UPDATE saved_queries SET is_favorite = NOT is_favorite WHERE id = ?1",
                [id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let mut stmt = self
            .conn()
            .prepare("SELECT is_favorite FROM saved_queries WHERE id = ?1")
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
    }

    /// Updates the last_used_at timestamp.
    pub fn touch(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "UPDATE saved_queries SET last_used_at = datetime('now') WHERE id = ?1",
                [id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }

    /// Searches by name or SQL text.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SavedQueryDto>, StorageError> {
        let pattern = format!("%{}%", query.to_lowercase());
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, folder_id, name, sql, is_favorite, connection_id, created_at, last_used_at
                 FROM saved_queries
                 WHERE LOWER(name) LIKE ?1 OR LOWER(sql) LIKE ?1
                 ORDER BY last_used_at DESC LIMIT ?2",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map(params![pattern, limit as i64], |row| {
                Ok(SavedQueryDto {
                    id: row.get(0)?,
                    folder_id: row.get(1)?,
                    name: row.get(2)?,
                    sql: row.get(3)?,
                    is_favorite: row.get::<_, i32>(4)? != 0,
                    connection_id: row.get(5)?,
                    created_at: row.get(6)?,
                    last_used_at: row.get(7)?,
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

    /// Returns favorite queries.
    pub fn favorites(&self) -> Result<Vec<SavedQueryDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, folder_id, name, sql, is_favorite, connection_id, created_at, last_used_at
                 FROM saved_queries WHERE is_favorite = 1 ORDER BY last_used_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(SavedQueryDto {
                    id: row.get(0)?,
                    folder_id: row.get(1)?,
                    name: row.get(2)?,
                    sql: row.get(3)?,
                    is_favorite: row.get::<_, i32>(4)? != 0,
                    connection_id: row.get(5)?,
                    created_at: row.get(6)?,
                    last_used_at: row.get(7)?,
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

    /// Clears all saved queries (for reset).
    pub fn clear(&self) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM saved_queries", [])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        self.conn()
            .execute("DELETE FROM saved_query_folders", [])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;
        Ok(())
    }
}

/// DTO for saved query folders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQueryFolderDto {
    pub id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub position: usize,
    pub created_at: String,
    pub updated_at: String,
}

/// DTO for saved queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQueryDto {
    pub id: String,
    pub folder_id: Option<String>,
    pub name: String,
    pub sql: String,
    pub is_favorite: bool,
    pub connection_id: Option<String>,
    pub created_at: String,
    pub last_used_at: String,
}

impl SavedQueryDto {
    pub fn new(name: String, sql: String, connection_id: Option<Uuid>) -> Self {
        let now = chrono::Utc::now().to_rfc3339();
        Self {
            id: Uuid::new_v4().to_string(),
            folder_id: None,
            name,
            sql,
            is_favorite: false,
            connection_id: connection_id.map(|u| u.to_string()),
            created_at: now.clone(),
            last_used_at: now,
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
        let path =
            std::env::temp_dir().join(format!("dbflux_repo_sq_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn insert_update_delete() {
        let path = temp_db("sq_crud");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SavedQueriesRepository::new(Arc::new(conn));

        let dto = SavedQueryDto::new("Test Query".to_string(), "SELECT 1".to_string(), None);
        repo.insert(&dto).expect("should insert");

        let mut updated = dto.clone();
        updated.name = "Updated".to_string();
        repo.update(&updated).expect("should update");

        let fetched = repo
            .get(&dto.id)
            .expect("should get")
            .expect("should exist");
        assert_eq!(fetched.name, "Updated");

        repo.delete(&dto.id).expect("should delete");
        assert!(repo.get(&dto.id).expect("should get").is_none());
    }

    #[test]
    fn folders_crud() {
        let path = temp_db("sq_folders");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SavedQueriesRepository::new(Arc::new(conn));

        let folder = SavedQueryFolderDto {
            id: Uuid::new_v4().to_string(),
            parent_id: None,
            name: "My Folder".to_string(),
            position: 0,
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        repo.create_folder(&folder).expect("create folder");

        let folders = repo.folders().expect("should list");
        assert_eq!(folders.len(), 1);
        assert_eq!(folders[0].name, "My Folder");

        repo.delete_folder(&folder.id).expect("delete");
        assert_eq!(repo.folders().expect("should list").len(), 0);
    }

    #[test]
    fn search_and_favorites() {
        let path = temp_db("sq_search");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = SavedQueriesRepository::new(Arc::new(conn));

        repo.insert(&SavedQueryDto::new(
            "Users Query".to_string(),
            "SELECT * FROM users".to_string(),
            None,
        ))
        .expect("add");
        repo.insert(&SavedQueryDto::new(
            "Orders Query".to_string(),
            "SELECT * FROM orders".to_string(),
            None,
        ))
        .expect("add");

        let results = repo.search("users", 10).expect("should search");
        assert_eq!(results.len(), 1);

        let fav = repo.toggle_favorite(&results[0].id).expect("toggle");
        assert!(fav);

        let favs = repo.favorites().expect("should list favs");
        assert_eq!(favs.len(), 1);
    }
}
