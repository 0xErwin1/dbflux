//! Repository for SSH tunnel profiles in config.db.
//!
//! SSH tunnel profiles store SSH tunnel configurations for secure database access.
//!
//! This repository uses native columns (host, port, user, auth_method, password_secret_ref)
//! and a ssh_tunnel_auth child table for key_path and passphrase. The config_json column
//! was dropped in migration v10. Column names were fixed in migration v13.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

use super::ssh_tunnel_auth::{SshTunnelAuthDto, SshTunnelAuthRepository};

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

    /// Returns a SshTunnelAuthRepository for managing auth credentials.
    pub fn auth_repo(&self) -> SshTunnelAuthRepository {
        SshTunnelAuthRepository::new(self.conn.clone())
    }

    /// Fetches all SSH tunnel profiles.
    /// Reads from native columns.
    pub fn all(&self) -> Result<Vec<SshTunnelProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, host, port, user, auth_method, key_path, passphrase_secret_ref,
                       password_secret_ref, save_secret, created_at, updated_at
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
                    host: row.get(2)?,
                    port: row.get::<_, i32>(3)?,
                    user: row.get(4)?,
                    auth_method: row.get(5)?,
                    key_path: row.get(6)?,
                    passphrase_secret_ref: row.get(7)?,
                    password_secret_ref: row.get(8)?,
                    save_secret: row.get::<_, i32>(9)? != 0,
                    created_at: row.get(10)?,
                    updated_at: row.get(11)?,
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
                SELECT id, name, host, port, user, auth_method, key_path, passphrase_secret_ref,
                       password_secret_ref, save_secret, created_at, updated_at
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
                host: row.get(2)?,
                port: row.get::<_, i32>(3)?,
                user: row.get(4)?,
                auth_method: row.get(5)?,
                key_path: row.get(6)?,
                passphrase_secret_ref: row.get(7)?,
                password_secret_ref: row.get(8)?,
                save_secret: row.get::<_, i32>(9)? != 0,
                created_at: row.get(10)?,
                updated_at: row.get(11)?,
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

    /// Gets auth credentials for an SSH tunnel profile (from child table).
    pub fn get_auth(
        &self,
        ssh_tunnel_profile_id: &str,
    ) -> Result<Option<SshTunnelAuthDto>, StorageError> {
        self.auth_repo().get(ssh_tunnel_profile_id)
    }

    /// Inserts a new SSH tunnel profile with auth credentials.
    /// Writes to ssh_tunnel_profiles (native columns) and ssh_tunnel_auth tables.
    pub fn insert(
        &self,
        profile: &SshTunnelProfileDto,
        auth: Option<&SshTunnelAuthDto>,
    ) -> Result<(), StorageError> {
        // Start transaction for atomic write
        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        tx.execute(
            r#"
            INSERT INTO ssh_tunnel_profiles (
                id, name, host, port, user, auth_method, key_path, passphrase_secret_ref,
                password_secret_ref, save_secret, created_at, updated_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, datetime('now'), datetime('now')
            )
            "#,
            params![
                profile.id,
                profile.name,
                profile.host,
                profile.port,
                profile.user,
                profile.auth_method,
                profile.key_path,
                profile.passphrase_secret_ref,
                profile.password_secret_ref,
                profile.save_secret as i32,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        // Insert auth credentials if provided (into child table)
        if let Some(auth_data) = auth {
            let mut auth_dto = auth_data.clone();
            auth_dto.ssh_tunnel_profile_id = profile.id.clone();
            tx.execute(
                r#"
                INSERT INTO ssh_tunnel_auth (
                    ssh_tunnel_profile_id, key_path, password_secret_ref, passphrase_secret_ref
                ) VALUES (
                    ?1, ?2, ?3, ?4
                )
                "#,
                params![
                    auth_dto.ssh_tunnel_profile_id,
                    auth_dto.key_path,
                    auth_dto.password_secret_ref,
                    auth_dto.passphrase_secret_ref,
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

        info!("Inserted SSH tunnel profile: {}", profile.name);
        Ok(())
    }

    /// Updates an existing SSH tunnel profile and its auth credentials.
    pub fn update(
        &self,
        profile: &SshTunnelProfileDto,
        auth: Option<&SshTunnelAuthDto>,
    ) -> Result<(), StorageError> {
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE ssh_tunnel_profiles SET
                    name = ?2,
                    host = ?3,
                    port = ?4,
                    user = ?5,
                    auth_method = ?6,
                    key_path = ?7,
                    passphrase_secret_ref = ?8,
                    password_secret_ref = ?9,
                    save_secret = ?10,
                    updated_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.host,
                    profile.port,
                    profile.user,
                    profile.auth_method,
                    profile.key_path,
                    profile.passphrase_secret_ref,
                    profile.password_secret_ref,
                    profile.save_secret as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            info!("No SSH tunnel profile found to update: {}", profile.id);
            return Ok(());
        }

        // Update or delete auth credentials
        match auth {
            Some(auth_data) => {
                let mut auth_dto = auth_data.clone();
                auth_dto.ssh_tunnel_profile_id = profile.id.clone();
                self.auth_repo().upsert(&auth_dto)?;
            }
            None => {
                self.auth_repo().delete(&profile.id)?;
            }
        }

        info!("Updated SSH tunnel profile: {}", profile.name);
        Ok(())
    }

    /// Upserts an SSH tunnel profile (insert or update) with auth credentials.
    pub fn upsert(
        &self,
        profile: &SshTunnelProfileDto,
        auth: Option<&SshTunnelAuthDto>,
    ) -> Result<(), StorageError> {
        // Start transaction for atomic write
        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        tx.execute(
            r#"
            INSERT INTO ssh_tunnel_profiles (
                id, name, host, port, user, auth_method, key_path, passphrase_secret_ref,
                password_secret_ref, save_secret, created_at, updated_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, datetime('now'), datetime('now')
            )
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                host = excluded.host,
                port = excluded.port,
                user = excluded.user,
                auth_method = excluded.auth_method,
                key_path = excluded.key_path,
                passphrase_secret_ref = excluded.passphrase_secret_ref,
                password_secret_ref = excluded.password_secret_ref,
                save_secret = excluded.save_secret,
                updated_at = datetime('now')
            "#,
            params![
                profile.id,
                profile.name,
                profile.host,
                profile.port,
                profile.user,
                profile.auth_method,
                profile.key_path,
                profile.passphrase_secret_ref,
                profile.password_secret_ref,
                profile.save_secret as i32,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        // Upsert auth credentials if provided
        if let Some(auth_data) = auth {
            let mut auth_dto = auth_data.clone();
            auth_dto.ssh_tunnel_profile_id = profile.id.clone();
            tx.execute(
                r#"
                INSERT INTO ssh_tunnel_auth (
                    ssh_tunnel_profile_id, key_path, password_secret_ref, passphrase_secret_ref
                ) VALUES (
                    ?1, ?2, ?3, ?4
                )
                ON CONFLICT(ssh_tunnel_profile_id) DO UPDATE SET
                    key_path = excluded.key_path,
                    password_secret_ref = excluded.password_secret_ref,
                    passphrase_secret_ref = excluded.passphrase_secret_ref
                "#,
                params![
                    auth_dto.ssh_tunnel_profile_id,
                    auth_dto.key_path,
                    auth_dto.password_secret_ref,
                    auth_dto.passphrase_secret_ref,
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

        info!("Upserted SSH tunnel profile: {}", profile.name);
        Ok(())
    }

    /// Deletes an SSH tunnel profile by ID (cascade deletes ssh_tunnel_auth).
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
/// Uses native columns instead of config_json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelProfileDto {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: i32,
    pub user: String,
    /// Auth method: 'password' or 'key'
    pub auth_method: String,
    pub key_path: Option<String>,
    pub passphrase_secret_ref: Option<String>,
    pub password_secret_ref: Option<String>,
    pub save_secret: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl SshTunnelProfileDto {
    /// Creates a new DTO with default auth_method of 'password'.
    pub fn new(id: Uuid, name: String, host: String, port: i32, user: String) -> Self {
        Self {
            id: id.to_string(),
            name,
            host,
            port,
            user,
            auth_method: "password".to_string(),
            key_path: None,
            passphrase_secret_ref: None,
            password_secret_ref: None,
            save_secret: true,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    /// Creates a new DTO with explicit auth_method.
    pub fn with_auth_method(
        id: Uuid,
        name: String,
        host: String,
        port: i32,
        user: String,
        auth_method: String,
    ) -> Self {
        Self {
            id: id.to_string(),
            name,
            host,
            port,
            user,
            auth_method,
            key_path: None,
            passphrase_secret_ref: None,
            password_secret_ref: None,
            save_secret: true,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    /// Returns true if this profile uses key-based auth.
    pub fn is_key_auth(&self) -> bool {
        self.auth_method.eq_ignore_ascii_case("key")
    }

    /// Returns true if this profile uses password auth.
    pub fn is_password_auth(&self) -> bool {
        self.auth_method.eq_ignore_ascii_case("password")
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

        let dto = SshTunnelProfileDto::with_auth_method(
            Uuid::new_v4(),
            "Jump Host".to_string(),
            "jump.example.com".to_string(),
            22,
            "admin".to_string(),
            "password".to_string(),
        );

        let repo = SshTunnelProfileRepository::new(Arc::new(conn));
        repo.insert(&dto, None).expect("should insert");

        let fetched = repo.all().expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].name, "Jump Host");
        assert_eq!(fetched[0].auth_method, "password");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn ssh_insert_with_key_auth() {
        let path = temp_db("ssh_insert_key");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = SshTunnelProfileDto::with_auth_method(
            Uuid::new_v4(),
            "Jump Host Key".to_string(),
            "jump.example.com".to_string(),
            22,
            "admin".to_string(),
            "key".to_string(),
        );

        let auth_dto = SshTunnelAuthDto {
            ssh_tunnel_profile_id: dto.id.clone(),
            key_path: Some("/home/user/.ssh/id_rsa".to_string()),
            password_secret_ref: None,
            passphrase_secret_ref: Some("dbflux:secret:ssh:passphrase:test".to_string()),
        };

        let repo = SshTunnelProfileRepository::new(Arc::new(conn));
        repo.insert(&dto, Some(&auth_dto)).expect("should insert");

        let fetched = repo.get(&dto.id).expect("should fetch");
        assert!(fetched.is_some());
        assert_eq!(fetched.as_ref().unwrap().auth_method, "key");

        let auth_fetched = repo.get_auth(&dto.id).expect("should fetch auth");
        assert!(auth_fetched.is_some());
        assert_eq!(
            auth_fetched.as_ref().unwrap().key_path.as_deref(),
            Some("/home/user/.ssh/id_rsa")
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn ssh_upsert_updates_auth() {
        let path = temp_db("ssh_upsert_auth");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = SshTunnelProfileDto::with_auth_method(
            Uuid::new_v4(),
            "Jump Host".to_string(),
            "jump.example.com".to_string(),
            22,
            "admin".to_string(),
            "key".to_string(),
        );

        let auth_dto = SshTunnelAuthDto {
            ssh_tunnel_profile_id: dto.id.clone(),
            key_path: Some("/home/user/.ssh/id_rsa".to_string()),
            password_secret_ref: None,
            passphrase_secret_ref: None,
        };

        let repo = SshTunnelProfileRepository::new(Arc::new(conn));
        repo.insert(&dto, Some(&auth_dto)).expect("should insert");

        // Upsert with password auth instead
        let updated_auth = SshTunnelAuthDto {
            ssh_tunnel_profile_id: dto.id.clone(),
            key_path: None,
            password_secret_ref: Some("dbflux:secret:ssh:password:test".to_string()),
            passphrase_secret_ref: None,
        };

        let mut updated_profile = dto.clone();
        updated_profile.auth_method = "password".to_string();
        repo.upsert(&updated_profile, Some(&updated_auth))
            .expect("should upsert");

        let auth_fetched = repo.get_auth(&dto.id).expect("should fetch auth");
        assert!(auth_fetched.is_some());
        assert!(auth_fetched.as_ref().unwrap().key_path.is_none());
        assert!(auth_fetched.as_ref().unwrap().password_secret_ref.is_some());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
