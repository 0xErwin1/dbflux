//! Repository for service environment variables in config.db.
//!
//! This module provides CRUD operations for the service_env child table,
//! which stores environment variables for service/RPC definitions.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing service environment variables.
/// This is always used behind a ServiceRepository.
pub struct ServiceEnvRepository {
    conn: OwnedConnection,
}

impl ServiceEnvRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all environment variables for a service.
    pub fn get_for_service(&self, service_id: &str) -> Result<Vec<ServiceEnvDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, service_id, key, value
                FROM service_env
                WHERE service_id = ?1
                ORDER BY key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let env_vars = stmt
            .query_map([service_id], |row| {
                Ok(ServiceEnvDto {
                    id: row.get(0)?,
                    service_id: row.get(1)?,
                    key: row.get(2)?,
                    value: row.get(3)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for env in env_vars {
            match env {
                Ok(e) => result.push(e),
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

    /// Fetches environment variables as a HashMap.
    pub fn get_map_for_service(
        &self,
        service_id: &str,
    ) -> Result<std::collections::HashMap<String, String>, StorageError> {
        let vars = self.get_for_service(service_id)?;
        Ok(vars.into_iter().map(|e| (e.key, e.value)).collect())
    }

    /// Inserts a single environment variable.
    pub fn insert(&self, env: &ServiceEnvDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO service_env (id, service_id, key, value)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![env.id, env.service_id, env.key, env.value,],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Inserts multiple environment variables from a HashMap (transactional).
    pub fn insert_many(
        &self,
        service_id: &str,
        env_vars: &std::collections::HashMap<String, String>,
    ) -> Result<(), StorageError> {
        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        // Delete existing env vars first
        tx.execute(
            "DELETE FROM service_env WHERE service_id = ?1",
            [service_id],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        // Insert new env vars
        for (key, value) in env_vars.iter() {
            let id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                r#"
                INSERT INTO service_env (id, service_id, key, value)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![id, service_id, key, value],
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

        info!(
            "Inserted {} env vars for service: {}",
            env_vars.len(),
            service_id
        );
        Ok(())
    }

    /// Deletes all environment variables for a service.
    pub fn delete_for_service(&self, service_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM service_env WHERE service_id = ?1",
                [service_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }
}

/// DTO for service environment variables (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEnvDto {
    pub id: String,
    pub service_id: String,
    pub key: String,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::repositories::services::{ServiceDto, ServiceRepository};
    use crate::sqlite::open_database;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_service_env_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn service_env_insert_and_fetch() {
        let path = temp_db("service_env_insert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let service = ServiceDto::new("test-socket".to_string());
        let conn_arc = Arc::new(conn);
        let service_repo = ServiceRepository::new(conn_arc.clone());
        service_repo
            .insert(&service)
            .expect("should insert service");

        let mut env_vars = HashMap::new();
        env_vars.insert("RUST_LOG".to_string(), "info".to_string());
        env_vars.insert("RUST_BACKTRACE".to_string(), "1".to_string());

        let repo = ServiceEnvRepository::new(conn_arc);
        repo.insert_many(&service.socket_id, &env_vars)
            .expect("should insert env vars");

        let fetched = repo
            .get_for_service(&service.socket_id)
            .expect("should fetch");
        assert_eq!(fetched.len(), 2);

        let map = repo
            .get_map_for_service(&service.socket_id)
            .expect("should get map");
        assert_eq!(map.get("RUST_LOG"), Some(&"info".to_string()));
        assert_eq!(map.get("RUST_BACKTRACE"), Some(&"1".to_string()));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn service_env_replace_existing() {
        let path = temp_db("service_env_replace");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let service = ServiceDto::new("test-socket".to_string());
        let conn_arc = Arc::new(conn);
        let service_repo = ServiceRepository::new(conn_arc.clone());
        service_repo
            .insert(&service)
            .expect("should insert service");

        let repo = ServiceEnvRepository::new(conn_arc);

        // Insert initial env vars
        let mut initial = HashMap::new();
        initial.insert("OLD_VAR".to_string(), "old_value".to_string());
        repo.insert_many(&service.socket_id, &initial)
            .expect("should insert");

        // Replace with new env vars
        let mut replacement = HashMap::new();
        replacement.insert("NEW_VAR".to_string(), "new_value".to_string());
        repo.insert_many(&service.socket_id, &replacement)
            .expect("should replace");

        let fetched = repo
            .get_for_service(&service.socket_id)
            .expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].key, "NEW_VAR");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
