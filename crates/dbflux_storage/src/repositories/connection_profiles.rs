//! Repository for connection profiles in config.db.
//!
//! Connection profiles store database connection configurations that users
//! create to connect to various databases.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing connection profiles.
pub struct ConnectionProfileRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all connection profiles.
    pub fn all(&self) -> Result<Vec<ConnectionProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, driver_id, description, favorite, color, icon,
                       config_json, auth_profile_id, proxy_profile_id,
                       ssh_tunnel_profile_id, access_profile_id,
                       settings_overrides_json, connection_settings_json,
                       hooks_json, hook_bindings_json, value_refs_json,
                       mcp_governance_json, created_at, updated_at
                FROM connection_profiles
                ORDER BY name ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let profiles = stmt
            .query_map([], |row| {
                Ok(ConnectionProfileDto {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    driver_id: row.get(2)?,
                    description: row.get(3)?,
                    favorite: row.get::<_, i32>(4)? != 0,
                    color: row.get(5)?,
                    icon: row.get(6)?,
                    config_json: row.get(7)?,
                    auth_profile_id: row.get(8)?,
                    proxy_profile_id: row.get(9)?,
                    ssh_tunnel_profile_id: row.get(10)?,
                    access_profile_id: row.get(11)?,
                    settings_overrides_json: row.get(12)?,
                    connection_settings_json: row.get(13)?,
                    hooks_json: row.get(14)?,
                    hook_bindings_json: row.get(15)?,
                    value_refs_json: row.get(16)?,
                    mcp_governance_json: row.get(17)?,
                    created_at: row.get(18)?,
                    updated_at: row.get(19)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for profile in profiles {
            match profile {
                Ok(p) => result.push(p),
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

    /// Fetches a single connection profile by ID.
    pub fn get(&self, id: &str) -> Result<Option<ConnectionProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, driver_id, description, favorite, color, icon,
                       config_json, auth_profile_id, proxy_profile_id,
                       ssh_tunnel_profile_id, access_profile_id,
                       settings_overrides_json, connection_settings_json,
                       hooks_json, hook_bindings_json, value_refs_json,
                       mcp_governance_json, created_at, updated_at
                FROM connection_profiles
                WHERE id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([id], |row| {
            Ok(ConnectionProfileDto {
                id: row.get(0)?,
                name: row.get(1)?,
                driver_id: row.get(2)?,
                description: row.get(3)?,
                favorite: row.get::<_, i32>(4)? != 0,
                color: row.get(5)?,
                icon: row.get(6)?,
                config_json: row.get(7)?,
                auth_profile_id: row.get(8)?,
                proxy_profile_id: row.get(9)?,
                ssh_tunnel_profile_id: row.get(10)?,
                access_profile_id: row.get(11)?,
                settings_overrides_json: row.get(12)?,
                connection_settings_json: row.get(13)?,
                hooks_json: row.get(14)?,
                hook_bindings_json: row.get(15)?,
                value_refs_json: row.get(16)?,
                mcp_governance_json: row.get(17)?,
                created_at: row.get(18)?,
                updated_at: row.get(19)?,
            })
        });

        match result {
            Ok(profile) => Ok(Some(profile)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts a new connection profile.
    pub fn insert(&self, profile: &ConnectionProfileDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profiles (
                    id, name, driver_id, description, favorite, color, icon,
                    config_json, auth_profile_id, proxy_profile_id,
                    ssh_tunnel_profile_id, access_profile_id,
                    settings_overrides_json, connection_settings_json,
                    hooks_json, hook_bindings_json, value_refs_json,
                    mcp_governance_json, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                    ?13, ?14, ?15, ?16, ?17, ?18, datetime('now'), datetime('now')
                )
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.driver_id,
                    profile.description,
                    profile.favorite as i32,
                    profile.color,
                    profile.icon,
                    profile.config_json,
                    profile.auth_profile_id,
                    profile.proxy_profile_id,
                    profile.ssh_tunnel_profile_id,
                    profile.access_profile_id,
                    profile.settings_overrides_json,
                    profile.connection_settings_json,
                    profile.hooks_json,
                    profile.hook_bindings_json,
                    profile.value_refs_json,
                    profile.mcp_governance_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Inserted connection profile: {}", profile.name);
        Ok(())
    }

    /// Updates an existing connection profile.
    pub fn update(&self, profile: &ConnectionProfileDto) -> Result<(), StorageError> {
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE connection_profiles SET
                    name = ?2,
                    driver_id = ?3,
                    description = ?4,
                    favorite = ?5,
                    color = ?6,
                    icon = ?7,
                    config_json = ?8,
                    auth_profile_id = ?9,
                    proxy_profile_id = ?10,
                    ssh_tunnel_profile_id = ?11,
                    access_profile_id = ?12,
                    settings_overrides_json = ?13,
                    connection_settings_json = ?14,
                    hooks_json = ?15,
                    hook_bindings_json = ?16,
                    value_refs_json = ?17,
                    mcp_governance_json = ?18,
                    updated_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.driver_id,
                    profile.description,
                    profile.favorite as i32,
                    profile.color,
                    profile.icon,
                    profile.config_json,
                    profile.auth_profile_id,
                    profile.proxy_profile_id,
                    profile.ssh_tunnel_profile_id,
                    profile.access_profile_id,
                    profile.settings_overrides_json,
                    profile.connection_settings_json,
                    profile.hooks_json,
                    profile.hook_bindings_json,
                    profile.value_refs_json,
                    profile.mcp_governance_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            info!("No profile found to update: {}", profile.id);
        } else {
            info!("Updated connection profile: {}", profile.name);
        }

        Ok(())
    }

    /// Upserts a connection profile (insert or update).
    pub fn upsert(&self, profile: &ConnectionProfileDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profiles (
                    id, name, driver_id, description, favorite, color, icon,
                    config_json, auth_profile_id, proxy_profile_id,
                    ssh_tunnel_profile_id, access_profile_id,
                    settings_overrides_json, connection_settings_json,
                    hooks_json, hook_bindings_json, value_refs_json,
                    mcp_governance_json, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                    ?13, ?14, ?15, ?16, ?17, ?18, datetime('now'), datetime('now')
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    driver_id = excluded.driver_id,
                    description = excluded.description,
                    favorite = excluded.favorite,
                    color = excluded.color,
                    icon = excluded.icon,
                    config_json = excluded.config_json,
                    auth_profile_id = excluded.auth_profile_id,
                    proxy_profile_id = excluded.proxy_profile_id,
                    ssh_tunnel_profile_id = excluded.ssh_tunnel_profile_id,
                    access_profile_id = excluded.access_profile_id,
                    settings_overrides_json = excluded.settings_overrides_json,
                    connection_settings_json = excluded.connection_settings_json,
                    hooks_json = excluded.hooks_json,
                    hook_bindings_json = excluded.hook_bindings_json,
                    value_refs_json = excluded.value_refs_json,
                    mcp_governance_json = excluded.mcp_governance_json,
                    updated_at = datetime('now')
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.driver_id,
                    profile.description,
                    profile.favorite as i32,
                    profile.color,
                    profile.icon,
                    profile.config_json,
                    profile.auth_profile_id,
                    profile.proxy_profile_id,
                    profile.ssh_tunnel_profile_id,
                    profile.access_profile_id,
                    profile.settings_overrides_json,
                    profile.connection_settings_json,
                    profile.hooks_json,
                    profile.hook_bindings_json,
                    profile.value_refs_json,
                    profile.mcp_governance_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Upserted connection profile: {}", profile.name);
        Ok(())
    }

    /// Deletes a connection profile by ID.
    pub fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM connection_profiles WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted connection profile: {}", id);
        Ok(())
    }

    /// Returns the count of profiles.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM connection_profiles", [], |row| {
                row.get(0)
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for connection profile storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileDto {
    pub id: String,
    pub name: String,
    pub driver_id: Option<String>,
    pub description: Option<String>,
    pub favorite: bool,
    pub color: Option<String>,
    pub icon: Option<String>,
    pub config_json: String,
    pub auth_profile_id: Option<String>,
    pub proxy_profile_id: Option<String>,
    pub ssh_tunnel_profile_id: Option<String>,
    pub access_profile_id: Option<String>,
    pub settings_overrides_json: Option<String>,
    pub connection_settings_json: Option<String>,
    pub hooks_json: Option<String>,
    pub hook_bindings_json: Option<String>,
    pub value_refs_json: Option<String>,
    pub mcp_governance_json: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl ConnectionProfileDto {
    /// Creates a new DTO with the given ID and name.
    pub fn new(id: Uuid, name: String, config_json: String) -> Self {
        Self {
            id: id.to_string(),
            name,
            driver_id: None,
            description: None,
            favorite: false,
            color: None,
            icon: None,
            config_json,
            auth_profile_id: None,
            proxy_profile_id: None,
            ssh_tunnel_profile_id: None,
            access_profile_id: None,
            settings_overrides_json: None,
            connection_settings_json: None,
            hooks_json: None,
            hook_bindings_json: None,
            value_refs_json: None,
            mcp_governance_json: None,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_profiles_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn insert_and_fetch_profile() {
        let path = temp_db("insert_fetch");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // Insert a profile
        let dto = ConnectionProfileDto::new(
            Uuid::new_v4(),
            "Test Profile".to_string(),
            r#"{"Postgres":{"host":"localhost","port":5432,"user":"test","database":"testdb"}}"#
                .to_string(),
        );

        let repo = ConnectionProfileRepository::new(Arc::new(conn));
        repo.insert(&dto).expect("should insert");

        // Fetch and verify
        let fetched = repo.all().expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].name, "Test Profile");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn update_and_delete_profile() {
        let path = temp_db("update_delete");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let id = Uuid::new_v4();
        let dto =
            ConnectionProfileDto::new(id, "Original".to_string(), r#"{"Postgres":{}}"#.to_string());

        let repo = ConnectionProfileRepository::new(Arc::new(conn));
        repo.insert(&dto).expect("should insert");

        // Update
        let mut updated = dto.clone();
        updated.name = "Updated".to_string();
        repo.update(&updated).expect("should update");

        // Verify update
        let fetched = repo
            .get(&id.to_string())
            .expect("should fetch")
            .expect("should exist");
        assert_eq!(fetched.name, "Updated");

        // Delete
        repo.delete(&id.to_string()).expect("should delete");

        // Verify deletion
        let after_delete = repo.get(&id.to_string()).expect("should fetch");
        assert!(after_delete.is_none());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn repository_count() {
        let path = temp_db("count");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = ConnectionProfileRepository::new(Arc::new(conn));

        // Empty initially
        assert_eq!(repo.count().expect("count should work"), 0);

        // After insert
        let dto = ConnectionProfileDto::new(Uuid::new_v4(), "Test".to_string(), "{}".to_string());
        repo.insert(&dto).expect("should insert");
        assert_eq!(repo.count().expect("count should work"), 1);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
