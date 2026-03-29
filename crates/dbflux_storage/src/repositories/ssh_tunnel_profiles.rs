//! Repository for SSH tunnel profiles in config.db.
//!
//! SSH tunnel profiles store SSH tunnel configurations for secure database access.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing SSH tunnel profiles.
pub struct SshTunnelProfileRepository {
    conn: OwnedConnection,
}

impl SshTunnelProfileRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all SSH tunnel profiles.
    pub fn all(&self) -> Result<Vec<SshTunnelProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, config_json, save_secret, created_at, updated_at
                FROM ssh_tunnel_profiles
                ORDER BY name ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let profiles = stmt
            .query_map([], |row| {
                Ok(SshTunnelProfileDto {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    config_json: row.get(2)?,
                    save_secret: row.get::<_, i32>(3)? != 0,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
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

    /// Fetches a single SSH tunnel profile by ID.
    pub fn get(&self, id: &str) -> Result<Option<SshTunnelProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, config_json, save_secret, created_at, updated_at
                FROM ssh_tunnel_profiles
                WHERE id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([id], |row| {
            Ok(SshTunnelProfileDto {
                id: row.get(0)?,
                name: row.get(1)?,
                config_json: row.get(2)?,
                save_secret: row.get::<_, i32>(3)? != 0,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
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

    /// Inserts a new SSH tunnel profile.
    pub fn insert(&self, profile: &SshTunnelProfileDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO ssh_tunnel_profiles (
                    id, name, config_json, save_secret, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, datetime('now'), datetime('now')
                )
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.config_json,
                    profile.save_secret as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Inserted SSH tunnel profile: {}", profile.name);
        Ok(())
    }

    /// Updates an existing SSH tunnel profile.
    pub fn update(&self, profile: &SshTunnelProfileDto) -> Result<(), StorageError> {
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE ssh_tunnel_profiles SET
                    name = ?2,
                    config_json = ?3,
                    save_secret = ?4,
                    updated_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.config_json,
                    profile.save_secret as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            info!("No SSH tunnel profile found to update: {}", profile.id);
        } else {
            info!("Updated SSH tunnel profile: {}", profile.name);
        }

        Ok(())
    }

    /// Upserts an SSH tunnel profile (insert or update).
    pub fn upsert(&self, profile: &SshTunnelProfileDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO ssh_tunnel_profiles (
                    id, name, config_json, save_secret, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, datetime('now'), datetime('now')
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    config_json = excluded.config_json,
                    save_secret = excluded.save_secret,
                    updated_at = datetime('now')
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.config_json,
                    profile.save_secret as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Upserted SSH tunnel profile: {}", profile.name);
        Ok(())
    }

    /// Deletes an SSH tunnel profile by ID.
    pub fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM ssh_tunnel_profiles WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted SSH tunnel profile: {}", id);
        Ok(())
    }

    /// Returns the count of profiles.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM ssh_tunnel_profiles", [], |row| {
                row.get(0)
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for SSH tunnel profile storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelProfileDto {
    pub id: String,
    pub name: String,
    pub config_json: String,
    pub save_secret: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl SshTunnelProfileDto {
    /// Creates a new DTO.
    pub fn new(id: Uuid, name: String, config_json: String, save_secret: bool) -> Self {
        Self {
            id: id.to_string(),
            name,
            config_json,
            save_secret,
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
        std::env::temp_dir().join(format!("dbflux_repo_ssh_{}_{}", name, std::process::id()))
    }

    #[test]
    fn ssh_insert_and_fetch() {
        let path = temp_db("ssh_insert_fetch");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = SshTunnelProfileDto::new(
            Uuid::new_v4(),
            "Jump Host".to_string(),
            r#"{"host":"jump.example.com","port":22,"user":"admin"}"#.to_string(),
            true,
        );

        let repo = SshTunnelProfileRepository::new(Arc::new(conn));
        repo.insert(&dto).expect("should insert");

        let fetched = repo.all().expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].name, "Jump Host");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
