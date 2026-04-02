//! Repository for aud_saved_filters table.
//!
//! Stores user-defined audit filter presets.

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use crate::error::StorageError;

/// Repository for managing saved audit filters.
#[derive(Clone)]
pub struct SavedFiltersRepository {
    conn: Arc<Mutex<Connection>>,
}

impl SavedFiltersRepository {
    /// Creates a new repository instance.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Lists all saved filters ordered by name.
    pub fn list(&self) -> Result<Vec<SavedFilterDto>, StorageError> {
        let conn = self.conn.lock().map_err(|e| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        let mut stmt = conn
            .prepare(
                r#"
            SELECT id, name, description, filter_json, created_at, updated_at
            FROM aud_saved_filters
            ORDER BY name ASC
            "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let filters = stmt
            .query_map([], |row| {
                Ok(SavedFilterDto {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    filter_json: row.get(3)?,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(filters)
    }

    /// Gets a saved filter by ID.
    pub fn get_by_id(&self, id: i64) -> Result<Option<SavedFilterDto>, StorageError> {
        let conn = self.conn.lock().map_err(|e| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        let mut stmt = conn
            .prepare(
                r#"
            SELECT id, name, description, filter_json, created_at, updated_at
            FROM aud_saved_filters
            WHERE id = ?1
            "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let result = stmt.query_row([id], |row| {
            Ok(SavedFilterDto {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                filter_json: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        });

        match result {
            Ok(dto) => Ok(Some(dto)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            }),
        }
    }

    /// Saves (insert or update) a filter.
    pub fn upsert(&self, filter: &SavedFilterDto) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(|e| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        if let Some(id) = filter.id {
            // Update existing
            conn.execute(
                r#"
                UPDATE aud_saved_filters SET
                    name = ?1,
                    description = ?2,
                    filter_json = ?3,
                    updated_at = datetime('now')
                WHERE id = ?4
                "#,
                rusqlite::params![filter.name, filter.description, filter.filter_json, id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        } else {
            // Insert new
            conn.execute(
                r#"
                INSERT INTO aud_saved_filters (name, description, filter_json)
                VALUES (?1, ?2, ?3)
                "#,
                rusqlite::params![filter.name, filter.description, filter.filter_json],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        }
        Ok(())
    }

    /// Deletes a saved filter by ID.
    pub fn delete(&self, id: i64) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(|e| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

        conn.execute("DELETE FROM aud_saved_filters WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        Ok(())
    }
}

/// DTO for aud_saved_filters table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedFilterDto {
    /// ID (None for new filters).
    pub id: Option<i64>,
    /// Unique name for the filter.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// JSON representation of the filter configuration.
    pub filter_json: String,
    /// Creation timestamp.
    pub created_at: String,
    /// Last update timestamp.
    pub updated_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_saved_filters_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn upsert_and_list_saved_filters() {
        let path = temp_db("upsert_list");
        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        let conn_mutex = Arc::new(Mutex::new(conn));
        let repo = SavedFiltersRepository::new(conn_mutex);

        // Insert a new filter
        let filter = SavedFilterDto {
            id: None,
            name: "Errors Only".to_string(),
            description: Some("Show only error events".to_string()),
            filter_json: r#"{"level":"error"}"#.to_string(),
            created_at: String::new(),
            updated_at: String::new(),
        };
        repo.upsert(&filter).expect("should upsert");

        // List filters
        let filters = repo.list().expect("should list");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].name, "Errors Only");

        // Delete
        if let Some(id) = filters[0].id {
            repo.delete(id).expect("should delete");
        }

        let filters_after = repo.list().expect("should list after delete");
        assert!(filters_after.is_empty());

        let _ = std::fs::remove_file(&path);
    }
}
