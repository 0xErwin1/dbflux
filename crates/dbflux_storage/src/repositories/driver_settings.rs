//! Repository for driver-level settings in config.db.
//!
//! # Deprecation Notice
//!
//! This repository is DEPRECATED after migration v16 normalized all data to native typed tables.
//!
//! - `driver_settings` table has been DROPped
//! - Use `DriverOverridesRepository` for driver overrides
//! - Use `DriverSettingValuesRepository` for driver setting values
//!
//! This module is kept for backward compatibility during migration but will be removed
//! once all callers migrate to the new repositories.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing driver settings.
pub struct DriverSettingsRepository {
    conn: OwnedConnection,
}

impl DriverSettingsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all driver settings.
    pub fn all(&self) -> Result<Vec<DriverSettingsDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT driver_key, overrides_json, settings_json, updated_at
                FROM driver_settings
                ORDER BY driver_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let settings = stmt
            .query_map([], |row| {
                Ok(DriverSettingsDto {
                    driver_key: row.get(0)?,
                    overrides_json: row.get(1)?,
                    settings_json: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for setting in settings {
            match setting {
                Ok(s) => result.push(s),
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

    /// Fetches settings for a specific driver.
    pub fn get(&self, driver_key: &str) -> Result<Option<DriverSettingsDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT driver_key, overrides_json, settings_json, updated_at
                FROM driver_settings
                WHERE driver_key = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([driver_key], |row| {
            Ok(DriverSettingsDto {
                driver_key: row.get(0)?,
                overrides_json: row.get(1)?,
                settings_json: row.get(2)?,
                updated_at: row.get(3)?,
            })
        });

        match result {
            Ok(setting) => Ok(Some(setting)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts or updates driver settings.
    pub fn upsert(&self, setting: &DriverSettingsDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO driver_settings (driver_key, overrides_json, settings_json, updated_at)
                VALUES (?1, ?2, ?3, datetime('now'))
                ON CONFLICT(driver_key) DO UPDATE SET
                    overrides_json = excluded.overrides_json,
                    settings_json = excluded.settings_json,
                    updated_at = datetime('now')
                "#,
                params![
                    setting.driver_key,
                    setting.overrides_json,
                    setting.settings_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Upserted driver settings for: {}", setting.driver_key);
        Ok(())
    }

    /// Deletes driver settings for a specific driver.
    pub fn delete(&self, driver_key: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM driver_settings WHERE driver_key = ?1",
                [driver_key],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted driver settings for: {}", driver_key);
        Ok(())
    }

    /// Returns the count of driver settings entries.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM driver_settings", [], |row| row.get(0))
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for driver settings storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverSettingsDto {
    pub driver_key: String,
    pub overrides_json: Option<String>,
    pub settings_json: Option<String>,
    pub updated_at: String,
}

impl DriverSettingsDto {
    /// Creates a new DTO.
    pub fn new(driver_key: String) -> Self {
        Self {
            driver_key,
            overrides_json: None,
            settings_json: None,
            updated_at: String::new(),
        }
    }
}
