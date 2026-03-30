//! Repository for proxy profiles in config.db.
//!
//! Proxy profiles store SOCKS5/HTTP proxy configurations.
//!
//! This repository uses native auth_kind column and proxy_auth child table.
//! The auth_json column was dropped in migration v10.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

use super::proxy_auth::{ProxyAuthDto, ProxyAuthRepository};

/// Repository for managing proxy profiles.
pub struct ProxyProfileRepository {
    conn: OwnedConnection,
}

impl ProxyProfileRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Returns a ProxyAuthRepository for managing auth credentials.
    pub fn auth_repo(&self) -> ProxyAuthRepository {
        ProxyAuthRepository::new(self.conn.clone())
    }

    /// Fetches all proxy profiles.
    /// Reads from native auth_kind column when available, falls back to auth_json.
    pub fn all(&self) -> Result<Vec<ProxyProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, kind, host, port, auth_kind, no_proxy, enabled, save_secret, created_at, updated_at
                FROM proxy_profiles
                ORDER BY name ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let profiles = stmt
            .query_map([], |row| {
                Ok(ProxyProfileDto {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    kind: row.get(2)?,
                    host: row.get(3)?,
                    port: row.get(4)?,
                    auth_kind: row.get(5)?,
                    no_proxy: row.get(6)?,
                    enabled: row.get::<_, i32>(7)? != 0,
                    save_secret: row.get::<_, i32>(8)? != 0,
                    created_at: row.get(9)?,
                    updated_at: row.get(10)?,
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

    /// Fetches a single proxy profile by ID.
    pub fn get(&self, id: &str) -> Result<Option<ProxyProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, kind, host, port, auth_kind, no_proxy, enabled, save_secret, created_at, updated_at
                FROM proxy_profiles
                WHERE id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([id], |row| {
            Ok(ProxyProfileDto {
                id: row.get(0)?,
                name: row.get(1)?,
                kind: row.get(2)?,
                host: row.get(3)?,
                port: row.get(4)?,
                auth_kind: row.get(5)?,
                no_proxy: row.get(6)?,
                enabled: row.get::<_, i32>(7)? != 0,
                save_secret: row.get::<_, i32>(8)? != 0,
                created_at: row.get(9)?,
                updated_at: row.get(10)?,
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

    /// Gets auth credentials for a proxy profile.
    pub fn get_auth(&self, proxy_profile_id: &str) -> Result<Option<ProxyAuthDto>, StorageError> {
        self.auth_repo().get(proxy_profile_id)
    }

    /// Inserts a new proxy profile with auth credentials.
    /// Writes to proxy_profiles (with auth_kind) and proxy_auth tables.
    /// Note: auth_json column dropped in migration v10.
    pub fn insert(
        &self,
        profile: &ProxyProfileDto,
        auth: Option<&ProxyAuthDto>,
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
            INSERT INTO proxy_profiles (
                id, name, kind, host, port, auth_kind, no_proxy, enabled, save_secret, created_at, updated_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, datetime('now'), datetime('now')
            )
            "#,
            params![
                profile.id,
                profile.name,
                profile.kind,
                profile.host,
                profile.port,
                profile.auth_kind,
                profile.no_proxy,
                profile.enabled as i32,
                profile.save_secret as i32,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        // Insert auth credentials if provided
        if let Some(auth_data) = auth {
            let mut auth_dto = auth_data.clone();
            auth_dto.proxy_profile_id = profile.id.clone();
            tx.execute(
                r#"
                INSERT INTO proxy_auth (
                    proxy_profile_id, username, domain, password_secret_ref
                ) VALUES (
                    ?1, ?2, ?3, ?4
                )
                "#,
                params![
                    auth_dto.proxy_profile_id,
                    auth_dto.username,
                    auth_dto.domain,
                    auth_dto.password_secret_ref,
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

        info!("Inserted proxy profile: {}", profile.name);
        Ok(())
    }

    /// Updates an existing proxy profile and its auth credentials.
    /// Note: auth_json column dropped in migration v10.
    pub fn update(
        &self,
        profile: &ProxyProfileDto,
        auth: Option<&ProxyAuthDto>,
    ) -> Result<(), StorageError> {
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE proxy_profiles SET
                    name = ?2,
                    kind = ?3,
                    host = ?4,
                    port = ?5,
                    auth_kind = ?6,
                    no_proxy = ?7,
                    enabled = ?8,
                    save_secret = ?9,
                    updated_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.kind,
                    profile.host,
                    profile.port,
                    profile.auth_kind,
                    profile.no_proxy,
                    profile.enabled as i32,
                    profile.save_secret as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            info!("No proxy profile found to update: {}", profile.id);
            return Ok(());
        }

        // Update or delete auth credentials
        match auth {
            Some(auth_data) => {
                let mut auth_dto = auth_data.clone();
                auth_dto.proxy_profile_id = profile.id.clone();
                self.auth_repo().upsert(&auth_dto)?;
            }
            None => {
                self.auth_repo().delete(&profile.id)?;
            }
        }

        info!("Updated proxy profile: {}", profile.name);
        Ok(())
    }

    /// Upserts a proxy profile (insert or update) with auth credentials.
    /// Note: auth_json column dropped in migration v10.
    pub fn upsert(
        &self,
        profile: &ProxyProfileDto,
        auth: Option<&ProxyAuthDto>,
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
            INSERT INTO proxy_profiles (
                id, name, kind, host, port, auth_kind, no_proxy, enabled, save_secret, created_at, updated_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, datetime('now'), datetime('now')
            )
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                kind = excluded.kind,
                host = excluded.host,
                port = excluded.port,
                auth_kind = excluded.auth_kind,
                no_proxy = excluded.no_proxy,
                enabled = excluded.enabled,
                save_secret = excluded.save_secret,
                updated_at = datetime('now')
            "#,
            params![
                profile.id,
                profile.name,
                profile.kind,
                profile.host,
                profile.port,
                profile.auth_kind,
                profile.no_proxy,
                profile.enabled as i32,
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
            auth_dto.proxy_profile_id = profile.id.clone();
            tx.execute(
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
                    auth_dto.proxy_profile_id,
                    auth_dto.username,
                    auth_dto.domain,
                    auth_dto.password_secret_ref,
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

        info!("Upserted proxy profile: {}", profile.name);
        Ok(())
    }

    /// Deletes a proxy profile by ID (cascade deletes proxy_auth).
    pub fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM proxy_profiles WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted proxy profile: {}", id);
        Ok(())
    }

    /// Returns the count of profiles.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM proxy_profiles", [], |row| row.get(0))
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for proxy profile storage.
/// Uses native auth_kind column instead of auth_json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyProfileDto {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub host: String,
    pub port: i32,
    /// Auth kind: 'none', 'basic', 'ntlm'
    pub auth_kind: String,
    pub no_proxy: Option<String>,
    pub enabled: bool,
    pub save_secret: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl ProxyProfileDto {
    /// Creates a new DTO with default auth_kind of 'none'.
    pub fn new(id: Uuid, name: String, kind: String, host: String, port: i32) -> Self {
        Self {
            id: id.to_string(),
            name,
            kind,
            host,
            port,
            auth_kind: "none".to_string(),
            no_proxy: None,
            enabled: true,
            save_secret: false,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    /// Creates a new DTO with auth_kind explicitly set.
    pub fn with_auth(
        id: Uuid,
        name: String,
        kind: String,
        host: String,
        port: i32,
        auth_kind: String,
    ) -> Self {
        Self {
            id: id.to_string(),
            name,
            kind,
            host,
            port,
            auth_kind,
            no_proxy: None,
            enabled: true,
            save_secret: false,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    /// Returns true if this profile uses Basic auth.
    pub fn is_basic_auth(&self) -> bool {
        self.auth_kind.eq_ignore_ascii_case("basic")
    }

    /// Returns true if this profile uses NTLM auth.
    pub fn is_ntlm_auth(&self) -> bool {
        self.auth_kind.eq_ignore_ascii_case("ntlm")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("dbflux_repo_proxy_{}_{}", name, std::process::id()))
    }

    #[test]
    fn proxy_insert_and_fetch() {
        let path = temp_db("proxy_insert_fetch");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = ProxyProfileDto::with_auth(
            Uuid::new_v4(),
            "HTTP Proxy".to_string(),
            "Http".to_string(),
            "proxy.example.com".to_string(),
            8080,
            "none".to_string(),
        );

        let repo = ProxyProfileRepository::new(Arc::new(conn));
        repo.insert(&dto, None).expect("should insert");

        let fetched = repo.all().expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].name, "HTTP Proxy");
        assert_eq!(fetched[0].auth_kind, "none");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn proxy_insert_with_auth() {
        let path = temp_db("proxy_insert_with_auth");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = ProxyProfileDto::with_auth(
            Uuid::new_v4(),
            "HTTP Proxy Basic".to_string(),
            "Http".to_string(),
            "proxy.example.com".to_string(),
            8080,
            "basic".to_string(),
        );

        let auth_dto = ProxyAuthDto {
            proxy_profile_id: dto.id.clone(),
            username: Some("testuser".to_string()),
            domain: None,
            password_secret_ref: Some("dbflux:secret:proxy:test".to_string()),
        };

        let repo = ProxyProfileRepository::new(Arc::new(conn));
        repo.insert(&dto, Some(&auth_dto)).expect("should insert");

        let fetched = repo.get(&dto.id).expect("should fetch");
        assert!(fetched.is_some());
        assert_eq!(fetched.as_ref().unwrap().auth_kind, "basic");

        let auth_fetched = repo.get_auth(&dto.id).expect("should fetch auth");
        assert!(auth_fetched.is_some());
        assert_eq!(
            auth_fetched.as_ref().unwrap().username.as_deref(),
            Some("testuser")
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn proxy_upsert_updates_auth() {
        let path = temp_db("proxy_upsert_auth");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = ProxyProfileDto::with_auth(
            Uuid::new_v4(),
            "HTTP Proxy".to_string(),
            "Http".to_string(),
            "proxy.example.com".to_string(),
            8080,
            "basic".to_string(),
        );

        let auth_dto = ProxyAuthDto {
            proxy_profile_id: dto.id.clone(),
            username: Some("original".to_string()),
            domain: None,
            password_secret_ref: None,
        };

        let repo = ProxyProfileRepository::new(Arc::new(conn));
        repo.insert(&dto, Some(&auth_dto)).expect("should insert");

        // Upsert with updated auth
        let updated_auth = ProxyAuthDto {
            proxy_profile_id: dto.id.clone(),
            username: Some("updated".to_string()),
            domain: None,
            password_secret_ref: Some("dbflux:secret:proxy:test".to_string()),
        };
        repo.upsert(&dto, Some(&updated_auth))
            .expect("should upsert");

        let auth_fetched = repo.get_auth(&dto.id).expect("should fetch auth");
        assert!(auth_fetched.is_some());
        assert_eq!(
            auth_fetched.as_ref().unwrap().username.as_deref(),
            Some("updated")
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
