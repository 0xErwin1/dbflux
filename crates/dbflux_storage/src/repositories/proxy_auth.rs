//! Repository for proxy auth credentials in config.db.
//!
//! This module provides CRUD operations for the proxy_auth child table,
//! which stores normalized authentication credentials for proxy profiles.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing proxy auth credentials.
/// This is always used behind a ProxyProfileRepository.
pub struct ProxyAuthRepository {
    conn: OwnedConnection,
}

impl ProxyAuthRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches auth credentials for a proxy profile.
    pub fn get(&self, proxy_profile_id: &str) -> Result<Option<ProxyAuthDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT proxy_profile_id, username, domain, password_secret_ref
                FROM proxy_auth
                WHERE proxy_profile_id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([proxy_profile_id], |row| {
            Ok(ProxyAuthDto {
                proxy_profile_id: row.get(0)?,
                username: row.get(1)?,
                domain: row.get(2)?,
                password_secret_ref: row.get(3)?,
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

    /// Inserts auth credentials for a proxy profile.
    pub fn insert(&self, auth: &ProxyAuthDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO proxy_auth (
                    proxy_profile_id, username, domain, password_secret_ref
                ) VALUES (
                    ?1, ?2, ?3, ?4
                )
                "#,
                params![
                    auth.proxy_profile_id,
                    auth.username,
                    auth.domain,
                    auth.password_secret_ref,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Inserted proxy auth for profile: {}", auth.proxy_profile_id);
        Ok(())
    }

    /// Updates auth credentials for a proxy profile.
    pub fn update(&self, auth: &ProxyAuthDto) -> Result<(), StorageError> {
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE proxy_auth SET
                    username = ?2,
                    domain = ?3,
                    password_secret_ref = ?4
                WHERE proxy_profile_id = ?1
                "#,
                params![
                    auth.proxy_profile_id,
                    auth.username,
                    auth.domain,
                    auth.password_secret_ref,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            info!("No proxy auth found to update: {}", auth.proxy_profile_id);
        } else {
            info!("Updated proxy auth for profile: {}", auth.proxy_profile_id);
        }

        Ok(())
    }

    /// Upserts auth credentials (insert or update).
    pub fn upsert(&self, auth: &ProxyAuthDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO proxy_auth (
                    proxy_profile_id, username, domain, password_secret_ref
                ) VALUES (
                    ?1, ?2, ?3, ?4
                )
                ON CONFLICT(proxy_profile_id) DO UPDATE SET
                    username = excluded.username,
                    domain = excluded.domain,
                    password_secret_ref = excluded.password_secret_ref
                "#,
                params![
                    auth.proxy_profile_id,
                    auth.username,
                    auth.domain,
                    auth.password_secret_ref,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Upserted proxy auth for profile: {}", auth.proxy_profile_id);
        Ok(())
    }

    /// Deletes auth credentials for a proxy profile.
    pub fn delete(&self, proxy_profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM proxy_auth WHERE proxy_profile_id = ?1",
                [proxy_profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted proxy auth for profile: {}", proxy_profile_id);
        Ok(())
    }
}

/// DTO for proxy auth credentials (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyAuthDto {
    pub proxy_profile_id: String,
    pub username: Option<String>,
    pub domain: Option<String>,
    pub password_secret_ref: Option<String>,
}

impl ProxyAuthDto {
    /// Creates a new DTO.
    pub fn new(proxy_profile_id: String) -> Self {
        Self {
            proxy_profile_id,
            username: None,
            domain: None,
            password_secret_ref: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::repositories::proxy_profiles::{ProxyProfileDto, ProxyProfileRepository};
    use crate::sqlite::open_database;
    use std::sync::Arc;
    use uuid::Uuid;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_proxy_auth_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn proxy_auth_insert_and_fetch() {
        let path = temp_db("proxy_auth_insert_fetch");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // First create the parent profile so FK constraint passes
        let profile = ProxyProfileDto::new(
            Uuid::new_v4(),
            "Test Proxy".to_string(),
            "Http".to_string(),
            "proxy.example.com".to_string(),
            8080,
        );

        let conn_arc = Arc::new(conn);
        let profile_repo = ProxyProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile, None)
            .expect("should insert profile");

        let auth = ProxyAuthDto {
            proxy_profile_id: profile.id.clone(),
            username: Some("testuser".to_string()),
            domain: None,
            password_secret_ref: Some("dbflux:secret:proxy:test".to_string()),
        };

        let repo = ProxyAuthRepository::new(conn_arc);
        repo.insert(&auth).expect("should insert");

        let fetched = repo.get(&profile.id).expect("should fetch");
        assert!(fetched.is_some());
        assert_eq!(
            fetched.as_ref().unwrap().username.as_deref(),
            Some("testuser")
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn proxy_auth_upsert() {
        let path = temp_db("proxy_auth_upsert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // First create the parent profile
        let profile = ProxyProfileDto::new(
            Uuid::new_v4(),
            "Test Proxy".to_string(),
            "Http".to_string(),
            "proxy.example.com".to_string(),
            8080,
        );

        let conn_arc = Arc::new(conn);
        let profile_repo = ProxyProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile, None)
            .expect("should insert profile");

        let auth = ProxyAuthDto {
            proxy_profile_id: profile.id.clone(),
            username: Some("original".to_string()),
            domain: None,
            password_secret_ref: None,
        };

        let repo = ProxyAuthRepository::new(conn_arc);
        repo.insert(&auth).expect("should insert first");

        // Upsert with updated username
        let auth_updated = ProxyAuthDto {
            proxy_profile_id: profile.id.clone(),
            username: Some("updated".to_string()),
            domain: None,
            password_secret_ref: None,
        };
        repo.upsert(&auth_updated).expect("should upsert");

        let fetched = repo.get(&profile.id).expect("should fetch");
        assert!(fetched.is_some());
        assert_eq!(
            fetched.as_ref().unwrap().username.as_deref(),
            Some("updated")
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn proxy_auth_delete() {
        let path = temp_db("proxy_auth_delete");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // First create the parent profile
        let profile = ProxyProfileDto::new(
            Uuid::new_v4(),
            "Test Proxy".to_string(),
            "Http".to_string(),
            "proxy.example.com".to_string(),
            8080,
        );

        let conn_arc = Arc::new(conn);
        let profile_repo = ProxyProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile, None)
            .expect("should insert profile");

        let auth = ProxyAuthDto {
            proxy_profile_id: profile.id.clone(),
            username: Some("testuser".to_string()),
            domain: None,
            password_secret_ref: None,
        };

        let repo = ProxyAuthRepository::new(conn_arc);
        repo.insert(&auth).expect("should insert");
        repo.delete(&profile.id).expect("should delete");

        let fetched = repo.get(&profile.id).expect("should fetch");
        assert!(fetched.is_none());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
