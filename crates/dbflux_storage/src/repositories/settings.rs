//! Repository for general app settings in config.db.
//!
//! App settings store key-value pairs for global configuration.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing app settings.
pub struct SettingsRepository {
    conn: OwnedConnection,
}

impl SettingsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Gets a setting value by key.
    pub fn get(&self, key: &str) -> Result<Option<String>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare("SELECT value_json FROM app_settings WHERE key = ?1")
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([key], |row| row.get::<_, String>(0));

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Sets a setting value.
    pub fn set(&self, key: &str, value_json: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO app_settings (key, value_json, updated_at)
                VALUES (?1, ?2, datetime('now'))
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = datetime('now')
                "#,
                params![key, value_json],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Set setting: {}", key);
        Ok(())
    }

    /// Deletes a setting by key.
    pub fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM app_settings WHERE key = ?1", [key])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted setting: {}", key);
        Ok(())
    }

    /// Returns the count of settings.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM app_settings", [], |row| row.get(0))
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }

    /// Returns all settings as key-value pairs.
    pub fn all(&self) -> Result<Vec<SettingDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare("SELECT key, value_json, updated_at FROM app_settings ORDER BY key")
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(SettingDto {
                    key: row.get(0)?,
                    value_json: row.get(1)?,
                    updated_at: row.get(2)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
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
                path: "config.db".into(),
                source: e,
            });
        }

        Ok(result)
    }
}

/// DTO for app settings storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingDto {
    pub key: String,
    pub value_json: String,
    pub updated_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_settings_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn settings_set_and_get() {
        let path = temp_db("settings_set");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = SettingsRepository::new(Arc::new(conn));

        // Set a value
        repo.set("test_key", r#"{"value":"test"}"#)
            .expect("should set");

        // Get it back
        let got = repo
            .get("test_key")
            .expect("should get")
            .expect("should exist");
        assert!(got.contains("test"));

        // Delete it
        repo.delete("test_key").expect("should delete");
        let after_delete = repo.get("test_key").expect("should get");
        assert!(after_delete.is_none());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
