//! Repository for connection profile settings overrides in config.db.
//!
//! This module provides CRUD operations for the connection_profile_settings child table,
//! which stores settings overrides for connection profiles.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing connection profile settings overrides.
/// This is always used behind a ConnectionProfileRepository.
pub struct ConnectionProfileSettingsRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileSettingsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all settings for a connection profile.
    pub fn get_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<ConnectionProfileSettingDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, profile_id, setting_key, setting_value
                FROM connection_profile_settings
                WHERE profile_id = ?1
                ORDER BY setting_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let settings = stmt
            .query_map([profile_id], |row| {
                Ok(ConnectionProfileSettingDto {
                    id: row.get(0)?,
                    profile_id: row.get(1)?,
                    setting_key: row.get(2)?,
                    setting_value: row.get(3)?,
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

    /// Inserts a single setting.
    pub fn insert(&self, setting: &ConnectionProfileSettingDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_settings (
                    id, profile_id, setting_key, setting_value
                ) VALUES (?1, ?2, ?3, ?4)
                "#,
                params![
                    setting.id,
                    setting.profile_id,
                    setting.setting_key,
                    setting.setting_value,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Upserts a setting (insert or update by profile_id + setting_key).
    pub fn upsert(&self, setting: &ConnectionProfileSettingDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_settings (
                    id, profile_id, setting_key, setting_value
                ) VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(profile_id, setting_key) DO UPDATE SET
                    setting_value = excluded.setting_value
                "#,
                params![
                    setting.id,
                    setting.profile_id,
                    setting.setting_key,
                    setting.setting_value,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!(
            "Upserted connection profile setting: {} for profile: {}",
            setting.setting_key, setting.profile_id
        );
        Ok(())
    }

    /// Deletes all settings for a connection profile.
    pub fn delete_for_profile(&self, profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM connection_profile_settings WHERE profile_id = ?1",
                [profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Deletes all settings for a connection profile whose key starts with the given prefix.
    pub fn delete_by_key_prefix(&self, profile_id: &str, prefix: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM connection_profile_settings WHERE profile_id = ?1 AND setting_key LIKE ?2 ESCAPE '\\'",
                rusqlite::params![profile_id, format!("{}%", prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"))],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Replaces all settings for a profile (delete old, insert new).
    pub fn replace_for_profile(
        &self,
        profile_id: &str,
        settings: &[ConnectionProfileSettingDto],
    ) -> Result<(), StorageError> {
        self.delete_for_profile(profile_id)?;
        for setting in settings {
            self.insert(setting)?;
        }
        Ok(())
    }
}

/// DTO for connection profile settings overrides (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileSettingDto {
    pub id: String,
    pub profile_id: String,
    pub setting_key: String,
    pub setting_value: Option<String>,
}

impl ConnectionProfileSettingDto {
    /// Creates a new setting.
    pub fn new(profile_id: String, setting_key: String, setting_value: Option<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            setting_key,
            setting_value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::repositories::connection_profiles::{
        ConnectionProfileDto, ConnectionProfileRepository,
    };
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_profile_settings_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn connection_profile_settings_insert_and_fetch() {
        let path = temp_db("settings_insert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let profile = ConnectionProfileDto::new(uuid::Uuid::new_v4(), "Test Profile".to_string());
        let conn_arc = Arc::new(conn);
        let profile_repo = ConnectionProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile)
            .expect("should insert profile");

        let repo = ConnectionProfileSettingsRepository::new(conn_arc);

        repo.insert(&ConnectionProfileSettingDto::new(
            profile.id.clone(),
            "refresh_policy".to_string(),
            Some("auto".to_string()),
        ))
        .expect("should insert");

        let fetched = repo.get_for_profile(&profile.id).expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].setting_key, "refresh_policy");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn connection_profile_settings_replace() {
        let path = temp_db("settings_replace");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let profile = ConnectionProfileDto::new(uuid::Uuid::new_v4(), "Test Profile".to_string());
        let conn_arc = Arc::new(conn);
        let profile_repo = ConnectionProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile)
            .expect("should insert profile");

        let repo = ConnectionProfileSettingsRepository::new(conn_arc);

        // Replace with multiple settings
        let settings = vec![
            ConnectionProfileSettingDto::new(
                profile.id.clone(),
                "key1".to_string(),
                Some("val1".to_string()),
            ),
            ConnectionProfileSettingDto::new(
                profile.id.clone(),
                "key2".to_string(),
                Some("val2".to_string()),
            ),
        ];
        repo.replace_for_profile(&profile.id, &settings)
            .expect("should replace");

        let fetched = repo.get_for_profile(&profile.id).expect("should fetch");
        assert_eq!(fetched.len(), 2);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
