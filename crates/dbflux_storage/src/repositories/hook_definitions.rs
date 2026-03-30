//! Repository for hook definitions in config.db.
//!
//! Hook definitions store reusable command/script hooks that can be bound
//! to connection profiles.
//!
//! This repository supports both legacy command_json and env_json columns and the
//! normalized hook_commands and hook_environment child tables for the transition period.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

use super::hook_commands::{HookCommandDto, HookCommandsRepository};
use super::hook_environment::HookEnvRepository;

/// Repository for managing hook definitions.
pub struct HookDefinitionRepository {
    conn: OwnedConnection,
}

impl HookDefinitionRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Returns a HookCommandsRepository for managing hook commands.
    pub fn commands_repo(&self) -> HookCommandsRepository {
        HookCommandsRepository::new(self.conn.clone())
    }

    /// Returns a HookEnvRepository for managing hook environment variables.
    pub fn env_repo(&self) -> HookEnvRepository {
        HookEnvRepository::new(self.conn.clone())
    }

    /// Gets the command for a hook.
    /// Reads from native hook_commands table (command_json column dropped in v10).
    pub fn get_command(&self, id: &str) -> Result<Option<HookCommandDto>, StorageError> {
        self.commands_repo().get_for_hook(id)
    }

    /// Gets the environment variables for a hook as a HashMap.
    /// Reads from native hook_environment table (env_json column dropped in v10).
    pub fn get_env(&self, id: &str) -> Result<HashMap<String, String>, StorageError> {
        let native_env = self.env_repo().get_map_for_hook(id)?;
        Ok(native_env)
    }

    /// Sets the command for a hook.
    /// Writes to native hook_commands table only (command_json column dropped in v10).
    pub fn set_command(&self, _id: &str, cmd: &HookCommandDto) -> Result<(), StorageError> {
        // Write to native child table
        self.commands_repo().upsert(cmd)?;
        Ok(())
    }

    /// Sets the environment variables for a hook.
    /// Writes to native hook_environment table only (env_json column dropped in v10).
    pub fn set_env(
        &self,
        id: &str,
        env_vars: &HashMap<String, String>,
    ) -> Result<(), StorageError> {
        // Write to native child table
        self.env_repo().insert_many(id, env_vars)?;
        Ok(())
    }

    /// Fetches all hook definitions.
    pub fn all(&self) -> Result<Vec<HookDefinitionDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, execution_mode, script_ref, cwd,
                       inherit_env, timeout_ms, ready_signal, on_failure,
                       enabled, created_at, updated_at
                FROM hook_definitions
                ORDER BY name ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let hooks = stmt
            .query_map([], |row| {
                Ok(HookDefinitionDto {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    execution_mode: row.get(2)?,
                    script_ref: row.get(3)?,
                    cwd: row.get(4)?,
                    inherit_env: row.get::<_, i32>(5)? != 0,
                    timeout_ms: row.get(6)?,
                    ready_signal: row.get(7)?,
                    on_failure: row.get(8)?,
                    enabled: row.get::<_, i32>(9)? != 0,
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
        for hook in hooks {
            match hook {
                Ok(h) => result.push(h),
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

    /// Fetches a single hook definition by ID.
    pub fn get(&self, id: &str) -> Result<Option<HookDefinitionDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, execution_mode, script_ref, cwd,
                       inherit_env, timeout_ms, ready_signal, on_failure,
                       enabled, created_at, updated_at
                FROM hook_definitions
                WHERE id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([id], |row| {
            Ok(HookDefinitionDto {
                id: row.get(0)?,
                name: row.get(1)?,
                execution_mode: row.get(2)?,
                script_ref: row.get(3)?,
                cwd: row.get(4)?,
                inherit_env: row.get::<_, i32>(5)? != 0,
                timeout_ms: row.get(6)?,
                ready_signal: row.get(7)?,
                on_failure: row.get(8)?,
                enabled: row.get::<_, i32>(9)? != 0,
                created_at: row.get(10)?,
                updated_at: row.get(11)?,
            })
        });

        match result {
            Ok(hook) => Ok(Some(hook)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts a new hook definition.
    pub fn insert(&self, hook: &HookDefinitionDto) -> Result<(), StorageError> {
        // Note: We don't use a transaction wrapper here because:
        // 1. The main hook_definitions insert is atomic
        // 2. Child table operations (hook_commands, hook_environment) are denormalized
        //    and can be rebuilt on next upsert if interrupted
        // 3. This avoids "cannot start a transaction within a transaction" errors
        //    when called from legacy import contexts

        self.conn()
            .execute(
                r#"
                INSERT INTO hook_definitions (
                    id, name, execution_mode, script_ref, cwd,
                    inherit_env, timeout_ms, ready_signal, on_failure,
                    enabled, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                    datetime('now'), datetime('now')
                )
                "#,
                params![
                    hook.id,
                    hook.name,
                    hook.execution_mode,
                    hook.script_ref,
                    hook.cwd,
                    hook.inherit_env as i32,
                    hook.timeout_ms,
                    hook.ready_signal,
                    hook.on_failure,
                    hook.enabled as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Inserted hook definition: {}", hook.name);
        Ok(())
    }

    /// Upserts a hook definition (insert or update by ID).
    pub fn upsert(&self, hook: &HookDefinitionDto) -> Result<(), StorageError> {
        // Note: We don't use a transaction wrapper here because:
        // 1. The main hook_definitions upsert is atomic
        // 2. Child table operations (hook_commands, hook_environment) are denormalized
        //    and can be rebuilt on next upsert if interrupted
        // 3. This avoids "cannot start a transaction within a transaction" errors
        //    when called from legacy import contexts

        self.conn()
            .execute(
                r#"
                INSERT INTO hook_definitions (
                    id, name, execution_mode, script_ref, cwd,
                    inherit_env, timeout_ms, ready_signal, on_failure,
                    enabled, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                    datetime('now'), datetime('now')
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    execution_mode = excluded.execution_mode,
                    script_ref = excluded.script_ref,
                    cwd = excluded.cwd,
                    inherit_env = excluded.inherit_env,
                    timeout_ms = excluded.timeout_ms,
                    ready_signal = excluded.ready_signal,
                    on_failure = excluded.on_failure,
                    enabled = excluded.enabled,
                    updated_at = datetime('now')
                "#,
                params![
                    hook.id,
                    hook.name,
                    hook.execution_mode,
                    hook.script_ref,
                    hook.cwd,
                    hook.inherit_env as i32,
                    hook.timeout_ms,
                    hook.ready_signal,
                    hook.on_failure,
                    hook.enabled as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Upserted hook definition: {}", hook.name);
        Ok(())
    }

    /// Updates an existing hook definition.
    pub fn update(&self, hook: &HookDefinitionDto) -> Result<(), StorageError> {
        // Note: We don't use a transaction wrapper here because:
        // 1. The main hook_definitions update is atomic
        // 2. Child table operations (hook_commands, hook_environment) are denormalized
        //    and can be rebuilt on next upsert if interrupted
        // 3. This avoids "cannot start a transaction within a transaction" errors
        //    when called from legacy import contexts

        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE hook_definitions SET
                    name = ?2,
                    execution_mode = ?3,
                    script_ref = ?4,
                    cwd = ?5,
                    inherit_env = ?6,
                    timeout_ms = ?7,
                    ready_signal = ?8,
                    on_failure = ?9,
                    enabled = ?10,
                    updated_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    hook.id,
                    hook.name,
                    hook.execution_mode,
                    hook.script_ref,
                    hook.cwd,
                    hook.inherit_env as i32,
                    hook.timeout_ms,
                    hook.ready_signal,
                    hook.on_failure,
                    hook.enabled as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            info!("No hook definition found to update: {}", hook.id);
            return Ok(());
        }

        info!("Updated hook definition: {}", hook.name);
        Ok(())
    }

    /// Deletes a hook definition by ID.
    pub fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM hook_definitions WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted hook definition: {}", id);
        Ok(())
    }

    /// Returns the count of hooks.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM hook_definitions", [], |row| {
                row.get(0)
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for hook definition storage.
/// Note: kind is stored in child tables (hook_definitions already has execution_mode).
/// command is stored in hook_commands child table.
/// env is stored in hook_environment child table.
/// The kind_json, command_json, env_json columns were dropped in migration v10.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookDefinitionDto {
    pub id: String,
    pub name: String,
    pub execution_mode: String,
    pub script_ref: Option<String>,
    pub cwd: Option<String>,
    pub inherit_env: bool,
    pub timeout_ms: Option<i64>,
    pub ready_signal: Option<String>,
    pub on_failure: String,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl HookDefinitionDto {
    /// Creates a new DTO.
    pub fn new(id: Uuid, name: String, execution_mode: String) -> Self {
        Self {
            id: id.to_string(),
            name,
            execution_mode,
            script_ref: None,
            cwd: None,
            inherit_env: true,
            timeout_ms: None,
            ready_signal: None,
            on_failure: "Warn".to_string(),
            enabled: true,
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
        std::env::temp_dir().join(format!("dbflux_repo_hook_{}_{}", name, std::process::id()))
    }

    #[test]
    fn hook_insert_and_fetch() {
        let path = temp_db("hook_insert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = HookDefinitionDto::new(
            Uuid::new_v4(),
            "PreConnect Test".to_string(),
            "Command".to_string(),
        );

        let repo = HookDefinitionRepository::new(Arc::new(conn));
        repo.insert(&dto).expect("should insert");

        let fetched = repo.all().expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].name, "PreConnect Test");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
