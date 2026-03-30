//! Repository for driver_setting_values table in config.db.
//!
//! This table stores driver-specific settings as key-value pairs (EAV pattern),
//! replacing the JSON blob previously stored in driver_settings.settings_json.

use log::info;
use rusqlite::{params, Connection};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing driver setting values.
pub struct DriverSettingValuesRepository {
    conn: OwnedConnection,
}

impl DriverSettingValuesRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Gets all setting values for a specific driver.
    pub fn get_for_driver(
        &self,
        driver_key: &str,
    ) -> Result<Vec<DriverSettingValueDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, driver_key, setting_key, setting_value
                FROM driver_setting_values
                WHERE driver_key = ?1
                ORDER BY setting_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let rows = stmt
            .query_map([driver_key], |row| {
                Ok(DriverSettingValueDto {
                    id: row.get(0)?,
                    driver_key: row.get(1)?,
                    setting_key: row.get(2)?,
                    setting_value: row.get(3)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// Upserts a single setting value.
    pub fn upsert(&self, value: &DriverSettingValueDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO driver_setting_values (id, driver_key, setting_key, setting_value)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(driver_key, setting_key) DO UPDATE SET
                    setting_value = excluded.setting_value
                "#,
                params![
                    value.id,
                    value.driver_key,
                    value.setting_key,
                    value.setting_value,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!(
            "Upserted driver setting: {} for {}",
            value.setting_key, value.driver_key
        );
        Ok(())
    }

    /// Replaces all setting values for a driver (deletes old, inserts new).
    pub fn replace_for_driver(
        &self,
        driver_key: &str,
        values: &[DriverSettingValueDto],
    ) -> Result<(), StorageError> {
        // Only start a transaction if we're not already in one
        let in_transaction = !self.conn().is_autocommit();

        if in_transaction {
            // Already in a transaction, just execute directly
            self.conn()
                .execute(
                    "DELETE FROM driver_setting_values WHERE driver_key = ?1",
                    [driver_key],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "config.db".into(),
                    source,
                })?;

            for value in values {
                self.conn()
                    .execute(
                        r#"
                        INSERT INTO driver_setting_values (id, driver_key, setting_key, setting_value)
                        VALUES (?1, ?2, ?3, ?4)
                        "#,
                        params![
                            value.id,
                            value.driver_key,
                            value.setting_key,
                            value.setting_value,
                        ],
                    )
                    .map_err(|source| StorageError::Sqlite {
                        path: "config.db".into(),
                        source,
                    })?;
            }
        } else {
            // Start a new transaction
            let tx =
                self.conn()
                    .unchecked_transaction()
                    .map_err(|source| StorageError::Sqlite {
                        path: "config.db".into(),
                        source,
                    })?;

            tx.execute(
                "DELETE FROM driver_setting_values WHERE driver_key = ?1",
                [driver_key],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

            for value in values {
                tx.execute(
                    r#"
                    INSERT INTO driver_setting_values (id, driver_key, setting_key, setting_value)
                    VALUES (?1, ?2, ?3, ?4)
                    "#,
                    params![
                        value.id,
                        value.driver_key,
                        value.setting_key,
                        value.setting_value,
                    ],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "config.db".into(),
                    source,
                })?;
            }

            tx.commit().map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;
        }

        info!(
            "Replaced {} setting values for driver: {}",
            values.len(),
            driver_key
        );
        Ok(())
    }

    /// Deletes all setting values for a driver.
    pub fn delete_for_driver(&self, driver_key: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM driver_setting_values WHERE driver_key = ?1",
                [driver_key],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted all setting values for driver: {}", driver_key);
        Ok(())
    }

    /// Returns the count of driver setting values entries.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM driver_setting_values", [], |row| {
                row.get(0)
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for driver_setting_values table.
#[derive(Debug, Clone)]
pub struct DriverSettingValueDto {
    pub id: String,
    pub driver_key: String,
    pub setting_key: String,
    pub setting_value: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_driver_setting_values_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn replace_for_driver() {
        use crate::repositories::driver_overrides::DriverOverridesDto;

        let path = temp_db("replace");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let conn_arc = Arc::new(conn);
        let values_repo = DriverSettingValuesRepository::new(conn_arc.clone());
        let overrides_repo =
            crate::repositories::driver_overrides::DriverOverridesRepository::new(conn_arc.clone());

        // First create a driver_overrides entry (required for FK constraint)
        let overrides = DriverOverridesDto {
            driver_key: "builtin:postgres".to_string(),
            refresh_policy: None,
            refresh_interval_secs: None,
            confirm_dangerous: None,
            requires_where: None,
            requires_preview: None,
            updated_at: String::new(),
        };
        overrides_repo
            .upsert(&overrides)
            .expect("should create overrides");

        let values = vec![
            DriverSettingValueDto {
                id: uuid::Uuid::new_v4().to_string(),
                driver_key: "builtin:postgres".to_string(),
                setting_key: "scan_batch_size".to_string(),
                setting_value: Some("1000".to_string()),
            },
            DriverSettingValueDto {
                id: uuid::Uuid::new_v4().to_string(),
                driver_key: "builtin:postgres".to_string(),
                setting_key: "max_connections".to_string(),
                setting_value: Some("10".to_string()),
            },
        ];

        values_repo
            .replace_for_driver("builtin:postgres", &values)
            .expect("should replace");

        let fetched = values_repo
            .get_for_driver("builtin:postgres")
            .expect("should get");
        assert_eq!(fetched.len(), 2);

        let _ = std::fs::remove_file(&path);
    }
}
