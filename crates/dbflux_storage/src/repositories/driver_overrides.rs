//! Repository for driver_overrides and driver_setting_values tables in config.db.
//!
//! These tables store the normalized driver settings as native columns,
//! replacing the JSON blobs previously stored in driver_settings.

use log::info;
use rusqlite::{params, Connection};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing driver overrides.
pub struct DriverOverridesRepository {
    conn: OwnedConnection,
}

impl DriverOverridesRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Gets all driver overrides.
    pub fn all(&self) -> Result<Vec<DriverOverridesDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT driver_key, refresh_policy, refresh_interval_secs,
                       confirm_dangerous, requires_where, requires_preview, updated_at
                FROM driver_overrides
                ORDER BY driver_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([], |row| {
                Ok(DriverOverridesDto {
                    driver_key: row.get(0)?,
                    refresh_policy: row.get(1)?,
                    refresh_interval_secs: row.get(2)?,
                    confirm_dangerous: row.get(3)?,
                    requires_where: row.get(4)?,
                    requires_preview: row.get(5)?,
                    updated_at: row.get(6)?,
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

    /// Gets overrides for a specific driver.
    pub fn get(&self, driver_key: &str) -> Result<Option<DriverOverridesDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT driver_key, refresh_policy, refresh_interval_secs,
                       confirm_dangerous, requires_where, requires_preview, updated_at
                FROM driver_overrides
                WHERE driver_key = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([driver_key], |row| {
            Ok(DriverOverridesDto {
                driver_key: row.get(0)?,
                refresh_policy: row.get(1)?,
                refresh_interval_secs: row.get(2)?,
                confirm_dangerous: row.get(3)?,
                requires_where: row.get(4)?,
                requires_preview: row.get(5)?,
                updated_at: row.get(6)?,
            })
        });

        match result {
            Ok(dto) => Ok(Some(dto)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Upserts driver overrides.
    pub fn upsert(&self, overrides: &DriverOverridesDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO driver_overrides (
                    driver_key, refresh_policy, refresh_interval_secs,
                    confirm_dangerous, requires_where, requires_preview, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'))
                ON CONFLICT(driver_key) DO UPDATE SET
                    refresh_policy = excluded.refresh_policy,
                    refresh_interval_secs = excluded.refresh_interval_secs,
                    confirm_dangerous = excluded.confirm_dangerous,
                    requires_where = excluded.requires_where,
                    requires_preview = excluded.requires_preview,
                    updated_at = datetime('now')
                "#,
                params![
                    overrides.driver_key,
                    overrides.refresh_policy,
                    overrides.refresh_interval_secs,
                    overrides.confirm_dangerous,
                    overrides.requires_where,
                    overrides.requires_preview,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Upserted driver overrides for: {}", overrides.driver_key);
        Ok(())
    }

    /// Deletes driver overrides for a specific driver.
    pub fn delete(&self, driver_key: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM driver_overrides WHERE driver_key = ?1",
                [driver_key],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted driver overrides for: {}", driver_key);
        Ok(())
    }

    /// Returns the count of driver overrides entries.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM driver_overrides", [], |row| {
                row.get(0)
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for driver_overrides table.
#[derive(Debug, Clone)]
pub struct DriverOverridesDto {
    pub driver_key: String,
    pub refresh_policy: Option<String>,
    pub refresh_interval_secs: Option<i32>,
    pub confirm_dangerous: Option<i32>,
    pub requires_where: Option<i32>,
    pub requires_preview: Option<i32>,
    pub updated_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_driver_overrides_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn upsert_and_get() {
        let path = temp_db("upsert_get");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = DriverOverridesRepository::new(Arc::new(conn));

        let dto = DriverOverridesDto {
            driver_key: "builtin:postgres".to_string(),
            refresh_policy: Some("interval".to_string()),
            refresh_interval_secs: Some(30),
            confirm_dangerous: Some(0),
            requires_where: None,
            requires_preview: Some(1),
            updated_at: String::new(),
        };

        repo.upsert(&dto).expect("should upsert");

        let fetched = repo
            .get("builtin:postgres")
            .expect("should get")
            .expect("should exist");
        assert_eq!(fetched.refresh_policy, Some("interval".to_string()));
        assert_eq!(fetched.refresh_interval_secs, Some(30));

        let _ = std::fs::remove_file(&path);
    }
}
