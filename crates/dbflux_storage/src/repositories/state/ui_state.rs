//! Repository for UI runtime state in state.db.
//!
//! Stores persisted UI layout preferences (collapse state, scroll positions).

use log::info;
use rusqlite::{params, Connection};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for app runtime state (UI layout, collapse preferences).
pub struct UiStateRepository {
    conn: OwnedConnection,
}

impl UiStateRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Retrieves a state value by key.
    pub fn get(&self, key: &str) -> Result<Option<String>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare("SELECT value_json FROM app_runtime_state WHERE key = ?1")
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        match stmt.query_row([key], |row| row.get::<_, String>(0)) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "state.db".into(),
                source: e,
            }),
        }
    }

    /// Sets or updates a state value.
    pub fn set(&self, key: &str, value_json: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO app_runtime_state (key, value_json, updated_at)
                VALUES (?1, ?2, datetime('now'))
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = datetime('now')
                "#,
                params![key, value_json],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        info!("Set runtime state: {}", key);
        Ok(())
    }

    /// Deletes a state key.
    pub fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM app_runtime_state WHERE key = ?1", [key])
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        info!("Deleted runtime state: {}", key);
        Ok(())
    }

    /// Returns all state keys and values.
    pub fn all(&self) -> Result<Vec<(String, String)>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare("SELECT key, value_json FROM app_runtime_state ORDER BY key")
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
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

    /// Clears all runtime state (for reset).
    pub fn clear(&self) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM app_runtime_state", [])
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
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_uistate_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn set_and_get() {
        let path = temp_db("set_get");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = UiStateRepository::new(Arc::new(conn));

        repo.set("ui_layout", r#"{"sidebar_collapsed":false}"#)
            .expect("should set");

        let got = repo
            .get("ui_layout")
            .expect("should get")
            .expect("should exist");
        assert!(got.contains("sidebar_collapsed"));

        repo.delete("ui_layout").expect("should delete");
        assert!(repo.get("ui_layout").expect("should get").is_none());
    }

    #[test]
    fn all_and_clear() {
        let path = temp_db("all_clear");
        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");
        let repo = UiStateRepository::new(Arc::new(conn));

        repo.set("key1", r#"{"a":1}"#).expect("set");
        repo.set("key2", r#"{"b":2}"#).expect("set");

        let all = repo.all().expect("should get all");
        assert_eq!(all.len(), 2);

        repo.clear().expect("should clear");
        assert_eq!(repo.all().expect("should get").len(), 0);
    }
}
