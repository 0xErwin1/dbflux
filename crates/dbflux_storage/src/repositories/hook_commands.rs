//! Repository for hook command definitions in config.db.
//!
//! This module provides CRUD operations for the hook_commands child table,
//! which stores command execution details for hook definitions.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing hook command definitions.
/// This is always used behind a HookDefinitionRepository.
pub struct HookCommandsRepository {
    conn: OwnedConnection,
}

impl HookCommandsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches the command for a hook, if any.
    pub fn get_for_hook(&self, hook_id: &str) -> Result<Option<HookCommandDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, hook_id, command, working_directory, timeout_ms, ready_signal
                FROM hook_commands
                WHERE hook_id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([hook_id], |row| {
            Ok(HookCommandDto {
                id: row.get(0)?,
                hook_id: row.get(1)?,
                command: row.get(2)?,
                working_directory: row.get(3)?,
                timeout_ms: row.get(4)?,
                ready_signal: row.get(5)?,
            })
        });

        match result {
            Ok(cmd) => Ok(Some(cmd)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts a command for a hook.
    pub fn insert(&self, cmd: &HookCommandDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO hook_commands (
                    id, hook_id, command, working_directory, timeout_ms, ready_signal
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                params![
                    cmd.id,
                    cmd.hook_id,
                    cmd.command,
                    cmd.working_directory,
                    cmd.timeout_ms,
                    cmd.ready_signal,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Inserted hook command for hook: {}", cmd.hook_id);
        Ok(())
    }

    /// Upserts a command for a hook.
    pub fn upsert(&self, cmd: &HookCommandDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO hook_commands (
                    id, hook_id, command, working_directory, timeout_ms, ready_signal
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(hook_id) DO UPDATE SET
                    command = excluded.command,
                    working_directory = excluded.working_directory,
                    timeout_ms = excluded.timeout_ms,
                    ready_signal = excluded.ready_signal
                "#,
                params![
                    cmd.id,
                    cmd.hook_id,
                    cmd.command,
                    cmd.working_directory,
                    cmd.timeout_ms,
                    cmd.ready_signal,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Upserted hook command for hook: {}", cmd.hook_id);
        Ok(())
    }

    /// Deletes the command for a hook.
    pub fn delete_for_hook(&self, hook_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM hook_commands WHERE hook_id = ?1", [hook_id])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }
}

/// DTO for hook command definitions (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookCommandDto {
    pub id: String,
    pub hook_id: String,
    pub command: String,
    pub working_directory: Option<String>,
    pub timeout_ms: Option<i64>,
    pub ready_signal: Option<String>,
}

impl HookCommandDto {
    /// Creates a new DTO.
    pub fn new(hook_id: String, command: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            hook_id,
            command,
            working_directory: None,
            timeout_ms: None,
            ready_signal: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::repositories::hook_definitions::{HookDefinitionDto, HookDefinitionRepository};
    use crate::sqlite::open_database;
    use std::sync::Arc;
    use uuid::Uuid;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_hook_commands_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn hook_commands_insert_and_fetch() {
        let path = temp_db("hook_commands_insert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let hook = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Test Hook".to_string(),
            "Blocking".to_string(),
        );
        let conn_arc = Arc::new(conn);
        let hook_repo = HookDefinitionRepository::new(conn_arc.clone());
        hook_repo.insert(&hook).expect("should insert hook");

        let cmd = HookCommandDto::new(hook.id.clone(), "echo hello".to_string());

        let repo = HookCommandsRepository::new(conn_arc);
        repo.insert(&cmd).expect("should insert");

        let fetched = repo.get_for_hook(&hook.id).expect("should fetch");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().command, "echo hello");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn hook_commands_upsert() {
        let path = temp_db("hook_commands_upsert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let hook = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Test Hook".to_string(),
            "Blocking".to_string(),
        );
        let conn_arc = Arc::new(conn);
        let hook_repo = HookDefinitionRepository::new(conn_arc.clone());
        hook_repo.insert(&hook).expect("should insert hook");

        let repo = HookCommandsRepository::new(conn_arc);

        // Insert initial
        let cmd = HookCommandDto::new(hook.id.clone(), "echo old".to_string());
        repo.insert(&cmd).expect("should insert");

        // Upsert with new command
        let cmd_updated = HookCommandDto::new(hook.id.clone(), "echo new".to_string());
        repo.upsert(&cmd_updated).expect("should upsert");

        let fetched = repo.get_for_hook(&hook.id).expect("should fetch");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().command, "echo new");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
