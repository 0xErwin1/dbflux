//! Repository for service/RPC definitions in config.db.
//!
//! Services store external RPC driver configurations (e.g., socket IDs, commands,
//! environment variables) for launching managed driver hosts.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing service/RPC definitions.
pub struct ServiceRepository {
    conn: OwnedConnection,
}

impl ServiceRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all services.
    pub fn all(&self) -> Result<Vec<ServiceDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT socket_id, enabled, command, args_json, env_json, startup_timeout_ms, created_at, updated_at
                FROM services
                ORDER BY socket_id ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let services = stmt
            .query_map([], |row| {
                Ok(ServiceDto {
                    socket_id: row.get(0)?,
                    enabled: row.get::<_, i32>(1)? != 0,
                    command: row.get(2)?,
                    args_json: row.get(3)?,
                    env_json: row.get(4)?,
                    startup_timeout_ms: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for service in services {
            match service {
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

    /// Fetches a single service by socket ID.
    pub fn get(&self, socket_id: &str) -> Result<Option<ServiceDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT socket_id, enabled, command, args_json, env_json, startup_timeout_ms, created_at, updated_at
                FROM services
                WHERE socket_id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([socket_id], |row| {
            Ok(ServiceDto {
                socket_id: row.get(0)?,
                enabled: row.get::<_, i32>(1)? != 0,
                command: row.get(2)?,
                args_json: row.get(3)?,
                env_json: row.get(4)?,
                startup_timeout_ms: row.get(5)?,
                created_at: row.get(6)?,
                updated_at: row.get(7)?,
            })
        });

        match result {
            Ok(service) => Ok(Some(service)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts a new service.
    pub fn insert(&self, service: &ServiceDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO services (
                    socket_id, enabled, command, args_json, env_json, startup_timeout_ms, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, datetime('now'), datetime('now')
                )
                "#,
                params![
                    service.socket_id,
                    service.enabled as i32,
                    service.command,
                    service.args_json,
                    service.env_json,
                    service.startup_timeout_ms,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Inserted service: {}", service.socket_id);
        Ok(())
    }

    /// Updates an existing service.
    pub fn update(&self, service: &ServiceDto) -> Result<(), StorageError> {
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE services SET
                    enabled = ?2,
                    command = ?3,
                    args_json = ?4,
                    env_json = ?5,
                    startup_timeout_ms = ?6,
                    updated_at = datetime('now')
                WHERE socket_id = ?1
                "#,
                params![
                    service.socket_id,
                    service.enabled as i32,
                    service.command,
                    service.args_json,
                    service.env_json,
                    service.startup_timeout_ms,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            info!("No service found to update: {}", service.socket_id);
        } else {
            info!("Updated service: {}", service.socket_id);
        }

        Ok(())
    }

    /// Upserts a service (insert or update).
    pub fn upsert(&self, service: &ServiceDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO services (
                    socket_id, enabled, command, args_json, env_json, startup_timeout_ms, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'), datetime('now'))
                ON CONFLICT(socket_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    command = excluded.command,
                    args_json = excluded.args_json,
                    env_json = excluded.env_json,
                    startup_timeout_ms = excluded.startup_timeout_ms,
                    updated_at = datetime('now')
                "#,
                params![
                    service.socket_id,
                    service.enabled as i32,
                    service.command,
                    service.args_json,
                    service.env_json,
                    service.startup_timeout_ms,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;
        info!("Upserted service: {}", service.socket_id);
        Ok(())
    }

    /// Deletes a service by socket ID.
    pub fn delete(&self, socket_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM services WHERE socket_id = ?1", [socket_id])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted service: {}", socket_id);
        Ok(())
    }

    /// Returns the count of services.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM services", [], |row| row.get(0))
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for service storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDto {
    pub socket_id: String,
    pub enabled: bool,
    pub command: Option<String>,
    pub args_json: Option<String>,
    pub env_json: Option<String>,
    pub startup_timeout_ms: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
}

impl ServiceDto {
    /// Creates a new DTO.
    pub fn new(socket_id: String) -> Self {
        Self {
            socket_id,
            enabled: true,
            command: None,
            args_json: None,
            env_json: None,
            startup_timeout_ms: None,
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
            "dbflux_repo_service_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn service_insert_and_fetch() {
        let path = temp_db("service_insert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = ServiceDto::new("test-socket".to_string());

        let repo = ServiceRepository::new(Arc::new(conn));
        repo.insert(&dto).expect("should insert");

        let fetched = repo.all().expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].socket_id, "test-socket");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
