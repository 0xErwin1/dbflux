//! Repository for service/RPC definitions in config.db.
//!
//! Services store external RPC driver configurations (e.g., socket IDs, commands,
//! environment variables) for launching managed driver hosts.
//!
//! This repository supports both legacy args_json/env_json columns and the normalized
//! service_args and service_env child tables for the transition period.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

use super::service_args::ServiceArgsRepository;
use super::service_env::ServiceEnvRepository;

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

    /// Returns a ServiceArgsRepository for managing command arguments.
    pub fn args_repo(&self) -> ServiceArgsRepository {
        ServiceArgsRepository::new(self.conn.clone())
    }

    /// Returns a ServiceEnvRepository for managing environment variables.
    pub fn env_repo(&self) -> ServiceEnvRepository {
        ServiceEnvRepository::new(self.conn.clone())
    }

    /// Gets the command arguments for a service as a Vec<String>.
    /// Reads from native service_args table (args_json column dropped in v10).
    pub fn get_args(&self, socket_id: &str) -> Result<Vec<String>, StorageError> {
        let native_args = self.args_repo().get_for_service(socket_id)?;
        Ok(native_args.into_iter().map(|a| a.value).collect())
    }

    /// Gets the environment variables for a service as a HashMap.
    /// Reads from native service_env table (env_json column dropped in v10).
    pub fn get_env(&self, socket_id: &str) -> Result<HashMap<String, String>, StorageError> {
        let native_env = self.env_repo().get_map_for_service(socket_id)?;
        Ok(native_env)
    }

    /// Sets the command arguments for a service.
    /// Writes to native service_args table only (args_json column dropped in v10).
    pub fn set_args(&self, socket_id: &str, args: &[String]) -> Result<(), StorageError> {
        // Write to native child table
        self.args_repo().insert_many(socket_id, args)?;
        Ok(())
    }

    /// Sets the environment variables for a service.
    /// Writes to native service_env table only (env_json column dropped in v10).
    pub fn set_env(
        &self,
        socket_id: &str,
        env_vars: &HashMap<String, String>,
    ) -> Result<(), StorageError> {
        // Write to native child table
        self.env_repo().insert_many(socket_id, env_vars)?;
        Ok(())
    }

    /// Fetches all services.
    pub fn all(&self) -> Result<Vec<ServiceDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT socket_id, enabled, command, startup_timeout_ms, created_at, updated_at
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
                    startup_timeout_ms: row.get(3)?,
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
                SELECT socket_id, enabled, command, startup_timeout_ms, created_at, updated_at
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
                startup_timeout_ms: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
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
                INSERT INTO services (
                    socket_id, enabled, command, startup_timeout_ms, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, datetime('now'), datetime('now')
                )
                "#,
            params![
                service.socket_id,
                service.enabled as i32,
                service.command,
                service.startup_timeout_ms,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        info!("Inserted service: {}", service.socket_id);
        Ok(())
    }

    /// Updates an existing service.
    pub fn update(&self, service: &ServiceDto) -> Result<(), StorageError> {
        // Start transaction for atomic write
        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let rows_affected = tx
            .execute(
                r#"
                UPDATE services SET
                    enabled = ?2,
                    command = ?3,
                    startup_timeout_ms = ?4,
                    updated_at = datetime('now')
                WHERE socket_id = ?1
                "#,
                params![
                    service.socket_id,
                    service.enabled as i32,
                    service.command,
                    service.startup_timeout_ms,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            tx.rollback().ok();
            info!("No service found to update: {}", service.socket_id);
            return Ok(());
        }

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        info!("Updated service: {}", service.socket_id);
        Ok(())
    }

    /// Upserts a service (insert or update).
    pub fn upsert(&self, service: &ServiceDto) -> Result<(), StorageError> {
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
                INSERT INTO services (
                    socket_id, enabled, command, startup_timeout_ms, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, datetime('now'), datetime('now'))
                ON CONFLICT(socket_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    command = excluded.command,
                    startup_timeout_ms = excluded.startup_timeout_ms,
                    updated_at = datetime('now')
                "#,
            params![
                service.socket_id,
                service.enabled as i32,
                service.command,
                service.startup_timeout_ms,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        tx.commit().map_err(|source| StorageError::Sqlite {
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
/// Note: args and env are stored in child tables (service_args, service_env).
/// The args_json and env_json columns were dropped in migration v10.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDto {
    pub socket_id: String,
    pub enabled: bool,
    pub command: Option<String>,
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
