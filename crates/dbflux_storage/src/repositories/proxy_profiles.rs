//! Repository for proxy profiles in config.db.
//!
//! Proxy profiles store SOCKS5/HTTP proxy configurations.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

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

    /// Fetches all proxy profiles.
    pub fn all(&self) -> Result<Vec<ProxyProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, kind, host, port, auth_json, no_proxy, enabled, save_secret, created_at, updated_at
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
                    auth_json: row.get(5)?,
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
                SELECT id, name, kind, host, port, auth_json, no_proxy, enabled, save_secret, created_at, updated_at
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
                auth_json: row.get(5)?,
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

    /// Inserts a new proxy profile.
    pub fn insert(&self, profile: &ProxyProfileDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO proxy_profiles (
                    id, name, kind, host, port, auth_json, no_proxy, enabled, save_secret, created_at, updated_at
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
                    profile.auth_json,
                    profile.no_proxy,
                    profile.enabled as i32,
                    profile.save_secret as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Inserted proxy profile: {}", profile.name);
        Ok(())
    }

    /// Updates an existing proxy profile.
    pub fn update(&self, profile: &ProxyProfileDto) -> Result<(), StorageError> {
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE proxy_profiles SET
                    name = ?2,
                    kind = ?3,
                    host = ?4,
                    port = ?5,
                    auth_json = ?6,
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
                    profile.auth_json,
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
        } else {
            info!("Updated proxy profile: {}", profile.name);
        }

        Ok(())
    }

    /// Upserts a proxy profile (insert or update).
    pub fn upsert(&self, profile: &ProxyProfileDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO proxy_profiles (
                    id, name, kind, host, port, auth_json, no_proxy, enabled, save_secret, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, datetime('now'), datetime('now')
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    kind = excluded.kind,
                    host = excluded.host,
                    port = excluded.port,
                    auth_json = excluded.auth_json,
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
                    profile.auth_json,
                    profile.no_proxy,
                    profile.enabled as i32,
                    profile.save_secret as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Upserted proxy profile: {}", profile.name);
        Ok(())
    }

    /// Deletes a proxy profile by ID.
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyProfileDto {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub host: String,
    pub port: i32,
    pub auth_json: String,
    pub no_proxy: Option<String>,
    pub enabled: bool,
    pub save_secret: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl ProxyProfileDto {
    /// Creates a new DTO.
    pub fn new(
        id: Uuid,
        name: String,
        kind: String,
        host: String,
        port: i32,
        auth_json: String,
    ) -> Self {
        Self {
            id: id.to_string(),
            name,
            kind,
            host,
            port,
            auth_json,
            no_proxy: None,
            enabled: true,
            save_secret: false,
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
        std::env::temp_dir().join(format!("dbflux_repo_proxy_{}_{}", name, std::process::id()))
    }

    #[test]
    fn proxy_insert_and_fetch() {
        let path = temp_db("proxy_insert_fetch");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = ProxyProfileDto::new(
            Uuid::new_v4(),
            "HTTP Proxy".to_string(),
            "Http".to_string(),
            "proxy.example.com".to_string(),
            8080,
            r#"{"None":{}}"#.to_string(),
        );

        let repo = ProxyProfileRepository::new(Arc::new(conn));
        repo.insert(&dto).expect("should insert");

        let fetched = repo.all().expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].name, "HTTP Proxy");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
