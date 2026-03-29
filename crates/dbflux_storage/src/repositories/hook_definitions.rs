//! Repository for hook definitions in config.db.
//!
//! Hook definitions store reusable command/script hooks that can be bound
//! to connection profiles.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

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

    /// Fetches all hook definitions.
    pub fn all(&self) -> Result<Vec<HookDefinitionDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, kind_json, execution_mode, script_ref, command_json,
                       cwd, env_json, inherit_env, timeout_ms, ready_signal, on_failure,
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
                    kind_json: row.get(2)?,
                    execution_mode: row.get(3)?,
                    script_ref: row.get(4)?,
                    command_json: row.get(5)?,
                    cwd: row.get(6)?,
                    env_json: row.get(7)?,
                    inherit_env: row.get::<_, i32>(8)? != 0,
                    timeout_ms: row.get(9)?,
                    ready_signal: row.get(10)?,
                    on_failure: row.get(11)?,
                    enabled: row.get::<_, i32>(12)? != 0,
                    created_at: row.get(13)?,
                    updated_at: row.get(14)?,
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
                SELECT id, name, kind_json, execution_mode, script_ref, command_json,
                       cwd, env_json, inherit_env, timeout_ms, ready_signal, on_failure,
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
                kind_json: row.get(2)?,
                execution_mode: row.get(3)?,
                script_ref: row.get(4)?,
                command_json: row.get(5)?,
                cwd: row.get(6)?,
                env_json: row.get(7)?,
                inherit_env: row.get::<_, i32>(8)? != 0,
                timeout_ms: row.get(9)?,
                ready_signal: row.get(10)?,
                on_failure: row.get(11)?,
                enabled: row.get::<_, i32>(12)? != 0,
                created_at: row.get(13)?,
                updated_at: row.get(14)?,
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
        self.conn()
            .execute(
                r#"
                INSERT INTO hook_definitions (
                    id, name, kind_json, execution_mode, script_ref, command_json,
                    cwd, env_json, inherit_env, timeout_ms, ready_signal, on_failure,
                    enabled, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                    datetime('now'), datetime('now')
                )
                "#,
                params![
                    hook.id,
                    hook.name,
                    hook.kind_json,
                    hook.execution_mode,
                    hook.script_ref,
                    hook.command_json,
                    hook.cwd,
                    hook.env_json,
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
        self.conn()
            .execute(
                r#"
                INSERT INTO hook_definitions (
                    id, name, kind_json, execution_mode, script_ref, command_json,
                    cwd, env_json, inherit_env, timeout_ms, ready_signal, on_failure,
                    enabled, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                    datetime('now'), datetime('now')
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    kind_json = excluded.kind_json,
                    execution_mode = excluded.execution_mode,
                    script_ref = excluded.script_ref,
                    command_json = excluded.command_json,
                    cwd = excluded.cwd,
                    env_json = excluded.env_json,
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
                    hook.kind_json,
                    hook.execution_mode,
                    hook.script_ref,
                    hook.command_json,
                    hook.cwd,
                    hook.env_json,
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
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE hook_definitions SET
                    name = ?2,
                    kind_json = ?3,
                    execution_mode = ?4,
                    script_ref = ?5,
                    command_json = ?6,
                    cwd = ?7,
                    env_json = ?8,
                    inherit_env = ?9,
                    timeout_ms = ?10,
                    ready_signal = ?11,
                    on_failure = ?12,
                    enabled = ?13,
                    updated_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    hook.id,
                    hook.name,
                    hook.kind_json,
                    hook.execution_mode,
                    hook.script_ref,
                    hook.command_json,
                    hook.cwd,
                    hook.env_json,
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
        } else {
            info!("Updated hook definition: {}", hook.name);
        }

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookDefinitionDto {
    pub id: String,
    pub name: String,
    pub kind_json: String,
    pub execution_mode: String,
    pub script_ref: Option<String>,
    pub command_json: Option<String>,
    pub cwd: Option<String>,
    pub env_json: Option<String>,
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
    pub fn new(id: Uuid, name: String, kind_json: String, execution_mode: String) -> Self {
        Self {
            id: id.to_string(),
            name,
            kind_json,
            execution_mode,
            script_ref: None,
            command_json: None,
            cwd: None,
            env_json: None,
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
            r#"{"Command":{"command":"echo","args":["hello"]}}"#.to_string(),
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
