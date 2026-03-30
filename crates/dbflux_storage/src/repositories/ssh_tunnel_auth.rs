//! Repository for SSH tunnel auth credentials in config.db.
//!
//! This module provides CRUD operations for the ssh_tunnel_auth child table,
//! which stores normalized authentication credentials for SSH tunnel profiles.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing SSH tunnel auth credentials.
/// This is always used behind a SshTunnelProfileRepository.
pub struct SshTunnelAuthRepository {
    conn: OwnedConnection,
}

impl SshTunnelAuthRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches auth credentials for an SSH tunnel profile.
    pub fn get(
        &self,
        ssh_tunnel_profile_id: &str,
    ) -> Result<Option<SshTunnelAuthDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT ssh_tunnel_profile_id, key_path, password_secret_ref, passphrase_secret_ref
                FROM ssh_tunnel_auth
                WHERE ssh_tunnel_profile_id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([ssh_tunnel_profile_id], |row| {
            Ok(SshTunnelAuthDto {
                ssh_tunnel_profile_id: row.get(0)?,
                key_path: row.get(1)?,
                password_secret_ref: row.get(2)?,
                passphrase_secret_ref: row.get(3)?,
            })
        });

        match result {
            Ok(auth) => Ok(Some(auth)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts auth credentials for an SSH tunnel profile.
    pub fn insert(&self, auth: &SshTunnelAuthDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO ssh_tunnel_auth (
                    ssh_tunnel_profile_id, key_path, password_secret_ref, passphrase_secret_ref
                ) VALUES (
                    ?1, ?2, ?3, ?4
                )
                "#,
                params![
                    auth.ssh_tunnel_profile_id,
                    auth.key_path,
                    auth.password_secret_ref,
                    auth.passphrase_secret_ref,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!(
            "Inserted ssh_tunnel auth for profile: {}",
            auth.ssh_tunnel_profile_id
        );
        Ok(())
    }

    /// Updates auth credentials for an SSH tunnel profile.
    pub fn update(&self, auth: &SshTunnelAuthDto) -> Result<(), StorageError> {
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE ssh_tunnel_auth SET
                    key_path = ?2,
                    password_secret_ref = ?3,
                    passphrase_secret_ref = ?4
                WHERE ssh_tunnel_profile_id = ?1
                "#,
                params![
                    auth.ssh_tunnel_profile_id,
                    auth.key_path,
                    auth.password_secret_ref,
                    auth.passphrase_secret_ref,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            info!(
                "No ssh_tunnel auth found to update: {}",
                auth.ssh_tunnel_profile_id
            );
        } else {
            info!(
                "Updated ssh_tunnel auth for profile: {}",
                auth.ssh_tunnel_profile_id
            );
        }

        Ok(())
    }

    /// Upserts auth credentials (insert or update).
    pub fn upsert(&self, auth: &SshTunnelAuthDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
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
                    auth.ssh_tunnel_profile_id,
                    auth.key_path,
                    auth.password_secret_ref,
                    auth.passphrase_secret_ref,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!(
            "Upserted ssh_tunnel auth for profile: {}",
            auth.ssh_tunnel_profile_id
        );
        Ok(())
    }

    /// Deletes auth credentials for an SSH tunnel profile.
    pub fn delete(&self, ssh_tunnel_profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM ssh_tunnel_auth WHERE ssh_tunnel_profile_id = ?1",
                [ssh_tunnel_profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!(
            "Deleted ssh_tunnel auth for profile: {}",
            ssh_tunnel_profile_id
        );
        Ok(())
    }
}

/// DTO for SSH tunnel auth credentials (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelAuthDto {
    pub ssh_tunnel_profile_id: String,
    pub key_path: Option<String>,
    pub password_secret_ref: Option<String>,
    pub passphrase_secret_ref: Option<String>,
}

impl SshTunnelAuthDto {
    /// Creates a new DTO.
    pub fn new(ssh_tunnel_profile_id: String) -> Self {
        Self {
            ssh_tunnel_profile_id,
            key_path: None,
            password_secret_ref: None,
            passphrase_secret_ref: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::repositories::ssh_tunnel_profiles::{
        SshTunnelProfileDto, SshTunnelProfileRepository,
    };
    use crate::sqlite::open_database;
    use std::sync::Arc;
    use uuid::Uuid;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_ssh_tunnel_auth_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn ssh_tunnel_auth_insert_and_fetch() {
        let path = temp_db("ssh_auth_insert_fetch");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // First create the parent profile so FK constraint passes
        let profile = SshTunnelProfileDto::with_auth_method(
            Uuid::new_v4(),
            "Jump Host".to_string(),
            "jump.example.com".to_string(),
            22,
            "admin".to_string(),
            "key".to_string(),
        );

        let conn_arc = Arc::new(conn);
        let profile_repo = SshTunnelProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile, None)
            .expect("should insert profile");

        let auth = SshTunnelAuthDto {
            ssh_tunnel_profile_id: profile.id.clone(),
            key_path: Some("/home/user/.ssh/id_rsa".to_string()),
            password_secret_ref: None,
            passphrase_secret_ref: Some("dbflux:secret:ssh:passphrase:test".to_string()),
        };

        let repo = SshTunnelAuthRepository::new(conn_arc);
        repo.insert(&auth).expect("should insert");

        let fetched = repo.get(&profile.id).expect("should fetch");
        assert!(fetched.is_some());
        assert_eq!(
            fetched.as_ref().unwrap().key_path.as_deref(),
            Some("/home/user/.ssh/id_rsa")
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn ssh_tunnel_auth_upsert() {
        let path = temp_db("ssh_auth_upsert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // First create the parent profile
        let profile = SshTunnelProfileDto::with_auth_method(
            Uuid::new_v4(),
            "Jump Host".to_string(),
            "jump.example.com".to_string(),
            22,
            "admin".to_string(),
            "key".to_string(),
        );

        let conn_arc = Arc::new(conn);
        let profile_repo = SshTunnelProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile, None)
            .expect("should insert profile");

        let auth = SshTunnelAuthDto {
            ssh_tunnel_profile_id: profile.id.clone(),
            key_path: Some("/home/user/.ssh/id_rsa".to_string()),
            password_secret_ref: None,
            passphrase_secret_ref: None,
        };

        let repo = SshTunnelAuthRepository::new(conn_arc.clone());
        repo.insert(&auth).expect("should insert first");

        // Upsert with password instead of key
        let auth_updated = SshTunnelAuthDto {
            ssh_tunnel_profile_id: profile.id.clone(),
            key_path: None,
            password_secret_ref: Some("dbflux:secret:ssh:password:test".to_string()),
            passphrase_secret_ref: None,
        };
        repo.upsert(&auth_updated).expect("should upsert");

        let fetched = repo.get(&profile.id).expect("should fetch");
        assert!(fetched.is_some());
        assert!(fetched.as_ref().unwrap().key_path.is_none());
        assert!(fetched.as_ref().unwrap().password_secret_ref.is_some());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn ssh_tunnel_auth_delete() {
        let path = temp_db("ssh_auth_delete");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // First create the parent profile
        let profile = SshTunnelProfileDto::with_auth_method(
            Uuid::new_v4(),
            "Jump Host".to_string(),
            "jump.example.com".to_string(),
            22,
            "admin".to_string(),
            "key".to_string(),
        );

        let conn_arc = Arc::new(conn);
        let profile_repo = SshTunnelProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile, None)
            .expect("should insert profile");

        let auth = SshTunnelAuthDto {
            ssh_tunnel_profile_id: profile.id.clone(),
            key_path: Some("/home/user/.ssh/id_rsa".to_string()),
            password_secret_ref: None,
            passphrase_secret_ref: None,
        };

        let repo = SshTunnelAuthRepository::new(conn_arc);
        repo.insert(&auth).expect("should insert");
        repo.delete(&profile.id).expect("should delete");

        let fetched = repo.get(&profile.id).expect("should fetch");
        assert!(fetched.is_none());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
