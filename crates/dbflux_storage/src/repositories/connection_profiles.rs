//! Repository for connection profiles in config.db.
//!
//! Connection profiles store database connection configurations that users
//! create to connect to various databases.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

pub use super::connection_driver_configs::ConnectionDriverConfigsRepository;
pub use super::connection_profile_access_params::ConnectionProfileAccessParamsRepository;
pub use super::connection_profile_configs::ConnectionProfileConfigsRepository;
pub use super::connection_profile_governance::ConnectionProfileGovernanceRepository;
pub use super::connection_profile_governance_binding_policies::ConnectionProfileGovernanceBindingPoliciesRepository;
pub use super::connection_profile_governance_binding_roles::ConnectionProfileGovernanceBindingRolesRepository;
pub use super::connection_profile_governance_bindings::ConnectionProfileGovernanceBindingsRepository;
pub use super::connection_profile_hook_args::ConnectionProfileHookArgsRepository;
pub use super::connection_profile_hook_bindings::ConnectionProfileHookBindingsRepository;
pub use super::connection_profile_hook_envs::ConnectionProfileHookEnvsRepository;
pub use super::connection_profile_hooks::ConnectionProfileHooksRepository;
pub use super::connection_profile_settings::ConnectionProfileSettingsRepository;
pub use super::connection_profile_value_refs::ConnectionProfileValueRefsRepository;

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

    /// Returns a configs repository for this profile.
    pub fn configs(&self) -> ConnectionProfileConfigsRepository {
        ConnectionProfileConfigsRepository::new(self.conn.clone())
    }

    /// Returns a driver configs repository for this profile (native columns for DbConfig).
    pub fn driver_configs(&self) -> ConnectionDriverConfigsRepository {
        ConnectionDriverConfigsRepository::new(self.conn.clone())
    }

    /// Returns a settings repository for this profile.
    pub fn settings(&self) -> ConnectionProfileSettingsRepository {
        ConnectionProfileSettingsRepository::new(self.conn.clone())
    }

    /// Returns a value refs repository for this profile.
    pub fn value_refs(&self) -> ConnectionProfileValueRefsRepository {
        ConnectionProfileValueRefsRepository::new(self.conn.clone())
    }

    /// Returns a hooks repository for this profile.
    pub fn hooks(&self) -> ConnectionProfileHooksRepository {
        ConnectionProfileHooksRepository::new(self.conn.clone())
    }

    /// Returns a hook bindings repository for this profile.
    pub fn hook_bindings(&self) -> ConnectionProfileHookBindingsRepository {
        ConnectionProfileHookBindingsRepository::new(self.conn.clone())
    }

    /// Returns a governance repository for this profile.
    pub fn governance(&self) -> ConnectionProfileGovernanceRepository {
        ConnectionProfileGovernanceRepository::new(self.conn.clone())
    }

    /// Returns a hook args repository for this profile.
    pub fn hook_args(&self) -> ConnectionProfileHookArgsRepository {
        ConnectionProfileHookArgsRepository::new(self.conn.clone())
    }

    /// Returns a hook envs repository for this profile.
    pub fn hook_envs(&self) -> ConnectionProfileHookEnvsRepository {
        ConnectionProfileHookEnvsRepository::new(self.conn.clone())
    }

    /// Returns an access params repository for this profile.
    pub fn access_params(&self) -> ConnectionProfileAccessParamsRepository {
        ConnectionProfileAccessParamsRepository::new(self.conn.clone())
    }

    /// Returns a governance bindings repository for this profile.
    pub fn governance_bindings(&self) -> ConnectionProfileGovernanceBindingsRepository {
        ConnectionProfileGovernanceBindingsRepository::new(self.conn.clone())
    }

    /// Returns a governance binding roles repository.
    pub fn governance_binding_roles(&self) -> ConnectionProfileGovernanceBindingRolesRepository {
        ConnectionProfileGovernanceBindingRolesRepository::new(self.conn.clone())
    }

    /// Returns a governance binding policies repository.
    pub fn governance_binding_policies(
        &self,
    ) -> ConnectionProfileGovernanceBindingPoliciesRepository {
        ConnectionProfileGovernanceBindingPoliciesRepository::new(self.conn.clone())
    }

    /// Fetches all connection profiles.
    pub fn all(&self) -> Result<Vec<ConnectionProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, driver_id, description, favorite, color, icon,
                       save_password, kind, access_kind, access_provider,
                       auth_profile_id, proxy_profile_id,
                       ssh_tunnel_profile_id,
                       created_at, updated_at
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
                    save_password: row.get::<_, i32>(7)? != 0,
                    kind: row.get(8)?,
                    access_kind: row.get(9)?,
                    access_provider: row.get(10)?,
                    auth_profile_id: row.get(11)?,
                    proxy_profile_id: row.get(12)?,
                    ssh_tunnel_profile_id: row.get(13)?,
                    created_at: row.get(14)?,
                    updated_at: row.get(15)?,
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
                       save_password, kind, access_kind, access_provider,
                       auth_profile_id, proxy_profile_id,
                       ssh_tunnel_profile_id,
                       created_at, updated_at
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
                save_password: row.get::<_, i32>(7)? != 0,
                kind: row.get(8)?,
                access_kind: row.get(9)?,
                access_provider: row.get(10)?,
                auth_profile_id: row.get(11)?,
                proxy_profile_id: row.get(12)?,
                ssh_tunnel_profile_id: row.get(13)?,
                created_at: row.get(14)?,
                updated_at: row.get(15)?,
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
                    save_password, kind, access_kind, access_provider,
                    auth_profile_id, proxy_profile_id,
                    ssh_tunnel_profile_id,
                    created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                    datetime('now'), datetime('now')
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
                    profile.save_password as i32,
                    profile.kind,
                    profile.access_kind,
                    profile.access_provider,
                    profile.auth_profile_id,
                    profile.proxy_profile_id,
                    profile.ssh_tunnel_profile_id,
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
                    save_password = ?8,
                    kind = ?9,
                    access_kind = ?10,
                    access_provider = ?11,
                    auth_profile_id = ?12,
                    proxy_profile_id = ?13,
                    ssh_tunnel_profile_id = ?14,
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
                    profile.save_password as i32,
                    profile.kind,
                    profile.access_kind,
                    profile.access_provider,
                    profile.auth_profile_id,
                    profile.proxy_profile_id,
                    profile.ssh_tunnel_profile_id,
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
                    save_password, kind, access_kind, access_provider,
                    auth_profile_id, proxy_profile_id,
                    ssh_tunnel_profile_id,
                    created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                    datetime('now'), datetime('now')
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    driver_id = excluded.driver_id,
                    description = excluded.description,
                    favorite = excluded.favorite,
                    color = excluded.color,
                    icon = excluded.icon,
                    save_password = excluded.save_password,
                    kind = excluded.kind,
                    access_kind = excluded.access_kind,
                    access_provider = excluded.access_provider,
                    auth_profile_id = excluded.auth_profile_id,
                    proxy_profile_id = excluded.proxy_profile_id,
                    ssh_tunnel_profile_id = excluded.ssh_tunnel_profile_id,
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
                    profile.save_password as i32,
                    profile.kind,
                    profile.access_kind,
                    profile.access_provider,
                    profile.auth_profile_id,
                    profile.proxy_profile_id,
                    profile.ssh_tunnel_profile_id,
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
    pub save_password: bool,
    pub kind: Option<String>,
    pub access_kind: Option<String>,
    pub access_provider: Option<String>,
    pub auth_profile_id: Option<String>,
    pub proxy_profile_id: Option<String>,
    pub ssh_tunnel_profile_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl ConnectionProfileDto {
    /// Creates a new DTO with the given ID and name.
    pub fn new(id: Uuid, name: String) -> Self {
        Self {
            id: id.to_string(),
            name,
            driver_id: None,
            description: None,
            favorite: false,
            color: None,
            icon: None,
            save_password: false,
            kind: None,
            access_kind: None,
            access_provider: None,
            auth_profile_id: None,
            proxy_profile_id: None,
            ssh_tunnel_profile_id: None,
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
        let dto = ConnectionProfileDto::new(Uuid::new_v4(), "Test Profile".to_string());

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
        let dto = ConnectionProfileDto::new(id, "Original".to_string());

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
        let dto = ConnectionProfileDto::new(Uuid::new_v4(), "Test".to_string());
        repo.insert(&dto).expect("should insert");
        assert_eq!(repo.count().expect("count should work"), 1);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
