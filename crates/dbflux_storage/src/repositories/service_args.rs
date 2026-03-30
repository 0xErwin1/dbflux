//! Repository for service command arguments in config.db.
//!
//! This module provides CRUD operations for the service_args child table,
//! which stores ordered command arguments for service/RPC definitions.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing service command arguments.
/// This is always used behind a ServiceRepository.
pub struct ServiceArgsRepository {
    conn: OwnedConnection,
}

impl ServiceArgsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all arguments for a service, ordered by position.
    pub fn get_for_service(&self, service_id: &str) -> Result<Vec<ServiceArgDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, service_id, position, value
                FROM service_args
                WHERE service_id = ?1
                ORDER BY position ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let args = stmt
            .query_map([service_id], |row| {
                Ok(ServiceArgDto {
                    id: row.get(0)?,
                    service_id: row.get(1)?,
                    position: row.get(2)?,
                    value: row.get(3)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for arg in args {
            match arg {
                Ok(a) => result.push(a),
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

    /// Inserts a single argument for a service.
    pub fn insert(&self, arg: &ServiceArgDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO service_args (id, service_id, position, value)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![arg.id, arg.service_id, arg.position, arg.value,],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Inserts multiple arguments for a service (transactional).
    pub fn insert_many(&self, service_id: &str, args: &[String]) -> Result<(), StorageError> {
        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        // Delete existing args first
        tx.execute(
            "DELETE FROM service_args WHERE service_id = ?1",
            [service_id],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        // Insert new args
        for (position, value) in args.iter().enumerate() {
            let id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                r#"
                INSERT INTO service_args (id, service_id, position, value)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![id, service_id, position as i64, value],
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

        info!("Inserted {} args for service: {}", args.len(), service_id);
        Ok(())
    }

    /// Deletes all arguments for a service.
    pub fn delete_for_service(&self, service_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM service_args WHERE service_id = ?1",
                [service_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }
}

/// DTO for service command arguments (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceArgDto {
    pub id: String,
    pub service_id: String,
    pub position: i64,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::repositories::services::{ServiceDto, ServiceRepository};
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_service_args_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn service_args_insert_and_fetch() {
        let path = temp_db("service_args_insert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // First create the parent service so FK constraint passes
        let service = ServiceDto::new("test-socket".to_string());
        let conn_arc = Arc::new(conn);
        let service_repo = ServiceRepository::new(conn_arc.clone());
        service_repo
            .insert(&service)
            .expect("should insert service");

        let args = vec!["arg1".to_string(), "arg2".to_string(), "arg3".to_string()];

        let repo = ServiceArgsRepository::new(conn_arc);
        repo.insert_many(&service.socket_id, &args)
            .expect("should insert args");

        let fetched = repo
            .get_for_service(&service.socket_id)
            .expect("should fetch");
        assert_eq!(fetched.len(), 3);
        assert_eq!(fetched[0].value, "arg1");
        assert_eq!(fetched[1].value, "arg2");
        assert_eq!(fetched[2].value, "arg3");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn service_args_replace_existing() {
        let path = temp_db("service_args_replace");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let service = ServiceDto::new("test-socket".to_string());
        let conn_arc = Arc::new(conn);
        let service_repo = ServiceRepository::new(conn_arc.clone());
        service_repo
            .insert(&service)
            .expect("should insert service");

        let repo = ServiceArgsRepository::new(conn_arc);

        // Insert initial args
        repo.insert_many(
            &service.socket_id,
            &["old1".to_string(), "old2".to_string()],
        )
        .expect("should insert");

        // Replace with new args
        repo.insert_many(&service.socket_id, &["new1".to_string()])
            .expect("should replace");

        let fetched = repo
            .get_for_service(&service.socket_id)
            .expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].value, "new1");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
