//! Repository for connection profile config values in config.db.
//!
//! This module provides CRUD operations for the connection_profile_configs child table,
//! which stores typed EAV (Entity-Attribute-Value) config values for connection profiles.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Represents the kind of value stored in a connection profile config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigValueKind {
    Text,
    Number,
    Bool,
    Secret,
}

impl ConfigValueKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigValueKind::Text => "text",
            ConfigValueKind::Number => "number",
            ConfigValueKind::Bool => "bool",
            ConfigValueKind::Secret => "secret",
        }
    }

    pub fn try_parse(s: &str) -> Option<Self> {
        match s {
            "text" => Some(ConfigValueKind::Text),
            "number" => Some(ConfigValueKind::Number),
            "bool" => Some(ConfigValueKind::Bool),
            "secret" => Some(ConfigValueKind::Secret),
            _ => None,
        }
    }
}

/// Repository for managing connection profile config values.
/// This is always used behind a ConnectionProfileRepository.
pub struct ConnectionProfileConfigsRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileConfigsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all configs for a connection profile.
    pub fn get_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<ConnectionProfileConfigDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, profile_id, config_key, config_value, config_value_kind
                FROM connection_profile_configs
                WHERE profile_id = ?1
                ORDER BY config_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let configs = stmt
            .query_map([profile_id], |row| {
                Ok(ConnectionProfileConfigDto {
                    id: row.get(0)?,
                    profile_id: row.get(1)?,
                    config_key: row.get(2)?,
                    config_value: row.get(3)?,
                    config_value_kind: row.get(4)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for config in configs {
            match config {
                Ok(c) => result.push(c),
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

    /// Inserts a single config value.
    pub fn insert(&self, config: &ConnectionProfileConfigDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_configs (
                    id, profile_id, config_key, config_value, config_value_kind
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![
                    config.id,
                    config.profile_id,
                    config.config_key,
                    config.config_value,
                    config.config_value_kind,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Upserts a config value (insert or update by profile_id + config_key).
    pub fn upsert(&self, config: &ConnectionProfileConfigDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_configs (
                    id, profile_id, config_key, config_value, config_value_kind
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(profile_id, config_key) DO UPDATE SET
                    config_value = excluded.config_value,
                    config_value_kind = excluded.config_value_kind
                "#,
                params![
                    config.id,
                    config.profile_id,
                    config.config_key,
                    config.config_value,
                    config.config_value_kind,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!(
            "Upserted connection profile config: {} for profile: {}",
            config.config_key, config.profile_id
        );
        Ok(())
    }

    /// Deletes all configs for a connection profile.
    pub fn delete_for_profile(&self, profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM connection_profile_configs WHERE profile_id = ?1",
                [profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Replaces all configs for a profile (delete old, insert new).
    /// This is used when the entire config set needs to be updated.
    pub fn replace_for_profile(
        &self,
        profile_id: &str,
        configs: &[ConnectionProfileConfigDto],
    ) -> Result<(), StorageError> {
        // Delete existing configs
        self.delete_for_profile(profile_id)?;

        // Insert new configs
        for config in configs {
            self.insert(config)?;
        }

        Ok(())
    }
}

/// DTO for connection profile config values (child table with EAV pattern).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileConfigDto {
    pub id: String,
    pub profile_id: String,
    pub config_key: String,
    pub config_value: Option<String>,
    pub config_value_kind: String,
}

impl ConnectionProfileConfigDto {
    /// Creates a new text config.
    pub fn new_text(profile_id: String, config_key: String, value: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            config_key,
            config_value: Some(value),
            config_value_kind: "text".to_string(),
        }
    }

    /// Creates a new number config.
    pub fn new_number(profile_id: String, config_key: String, value: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            config_key,
            config_value: Some(value),
            config_value_kind: "number".to_string(),
        }
    }

    /// Creates a new bool config.
    pub fn new_bool(profile_id: String, config_key: String, value: bool) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            config_key,
            config_value: Some(if value { "true" } else { "false" }.to_string()),
            config_value_kind: "bool".to_string(),
        }
    }

    /// Creates a new secret config.
    pub fn new_secret(profile_id: String, config_key: String, secret_ref: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            config_key,
            config_value: Some(secret_ref),
            config_value_kind: "secret".to_string(),
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
            "dbflux_repo_profile_configs_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn connection_profile_configs_insert_and_fetch() {
        let path = temp_db("configs_insert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // Create parent profile first
        let profile = ConnectionProfileDto::new(uuid::Uuid::new_v4(), "Test Profile".to_string());
        let conn_arc = Arc::new(conn);
        let profile_repo = ConnectionProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile)
            .expect("should insert profile");

        let repo = ConnectionProfileConfigsRepository::new(conn_arc);

        // Insert config
        repo.insert(&ConnectionProfileConfigDto::new_text(
            profile.id.clone(),
            "host".to_string(),
            "localhost".to_string(),
        ))
        .expect("should insert config");

        let fetched = repo.get_for_profile(&profile.id).expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].config_key, "host");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn connection_profile_configs_replace() {
        let path = temp_db("configs_replace");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let profile = ConnectionProfileDto::new(uuid::Uuid::new_v4(), "Test Profile".to_string());
        let conn_arc = Arc::new(conn);
        let profile_repo = ConnectionProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile)
            .expect("should insert profile");

        let repo = ConnectionProfileConfigsRepository::new(conn_arc);

        // Insert initial configs
        let configs = vec![
            ConnectionProfileConfigDto::new_text(
                profile.id.clone(),
                "key1".to_string(),
                "value1".to_string(),
            ),
            ConnectionProfileConfigDto::new_text(
                profile.id.clone(),
                "key2".to_string(),
                "value2".to_string(),
            ),
        ];
        repo.replace_for_profile(&profile.id, &configs)
            .expect("should replace");

        // Replace with new configs
        let new_configs = vec![ConnectionProfileConfigDto::new_text(
            profile.id.clone(),
            "key3".to_string(),
            "value3".to_string(),
        )];
        repo.replace_for_profile(&profile.id, &new_configs)
            .expect("should replace again");

        let fetched = repo.get_for_profile(&profile.id).expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].config_key, "key3");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
