//! Repository for driver-level settings in dbflux.db.
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
//!
//! NOTE: This file was previously querying a non-existent `driver_settings` table.
//! It has been corrected to query `cfg_driver_overrides` with the correct column names.

use log::info;
use rusqlite::{Connection, params};
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
                SELECT driver_key, refresh_policy, refresh_interval_secs,
                       confirm_dangerous, requires_where, requires_preview, updated_at
                FROM cfg_driver_overrides
                ORDER BY driver_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let settings = stmt
            .query_map([], |row| {
                Ok(DriverSettingsDto {
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
                path: "dbflux.db".into(),
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
                path: "dbflux.db".into(),
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
                SELECT driver_key, refresh_policy, refresh_interval_secs,
                       confirm_dangerous, requires_where, requires_preview, updated_at
                FROM cfg_driver_overrides
                WHERE driver_key = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let result = stmt.query_row([driver_key], |row| {
            Ok(DriverSettingsDto {
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
            Ok(setting) => Ok(Some(setting)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts or updates driver settings.
    pub fn upsert(&self, setting: &DriverSettingsDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_driver_overrides (
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
                    setting.driver_key,
                    setting.refresh_policy,
                    setting.refresh_interval_secs,
                    setting.confirm_dangerous,
                    setting.requires_where,
                    setting.requires_preview,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        info!("Upserted driver settings for: {}", setting.driver_key);
        Ok(())
    }

    /// Deletes driver settings for a specific driver.
    pub fn delete(&self, driver_key: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM cfg_driver_overrides WHERE driver_key = ?1",
                [driver_key],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        info!("Deleted driver settings for: {}", driver_key);
        Ok(())
    }

    /// Returns the count of driver settings entries.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM cfg_driver_overrides", [], |row| {
                row.get(0)
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for driver settings storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverSettingsDto {
    pub driver_key: String,
    pub refresh_policy: Option<String>,
    pub refresh_interval_secs: Option<i32>,
    pub confirm_dangerous: Option<i32>,
    pub requires_where: Option<i32>,
    pub requires_preview: Option<i32>,
    pub updated_at: String,
}

impl DriverSettingsDto {
    /// Creates a new DTO.
    pub fn new(driver_key: String) -> Self {
        Self {
            driver_key,
            refresh_policy: None,
            refresh_interval_secs: None,
            confirm_dangerous: None,
            requires_where: None,
            requires_preview: None,
            updated_at: String::new(),
        }
    }
}
