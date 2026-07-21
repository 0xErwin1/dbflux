//! Repository for hook definitions in dbflux.db.
//!
//! Hook definitions store reusable command/script hooks that can be bound
//! to connection profiles.
//!
//! This repository supports both legacy command_json and env_json columns and the
//! normalized hook_commands and hook_environment child tables for the transition period.

use log::info;
use rusqlite::{Connection, Transaction, params};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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
                       enabled, created_at, updated_at,
                       COALESCE(env_denylist_json, '[]'), kind_json
                FROM cfg_hook_definitions
                ORDER BY name ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let hooks = stmt
            .query_map([], |row| {
                let env_denylist_json: String = row.get(12)?;
                let env_denylist: Vec<String> =
                    serde_json::from_str(&env_denylist_json).unwrap_or_default();

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
                    env_denylist,
                    kind_json: row.get(13)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
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
                path: "dbflux.db".into(),
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
                       enabled, created_at, updated_at,
                       COALESCE(env_denylist_json, '[]'), kind_json
                FROM cfg_hook_definitions
                WHERE id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let result = stmt.query_row([id], |row| {
            let env_denylist_json: String = row.get(12)?;
            let env_denylist: Vec<String> =
                serde_json::from_str(&env_denylist_json).unwrap_or_default();

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
                env_denylist,
                kind_json: row.get(13)?,
            })
        });

        match result {
            Ok(hook) => Ok(Some(hook)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts a new hook definition.
    pub fn insert(&self, hook: &HookDefinitionDto) -> Result<(), StorageError> {
        // Note: We don't use a transaction wrapper here because:
        // 1. The main cfg_hook_definitions insert is atomic
        // 2. Child table operations (hook_commands, hook_environment) are denormalized
        //    and can be rebuilt on next upsert if interrupted
        // 3. This avoids "cannot start a transaction within a transaction" errors
        //    when called from legacy import contexts

        let env_denylist_json =
            serde_json::to_string(&hook.env_denylist).unwrap_or_else(|_| "[]".to_string());

        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_hook_definitions (
                    id, name, execution_mode, script_ref, cwd,
                    inherit_env, timeout_ms, ready_signal, on_failure,
                    enabled, created_at, updated_at, env_denylist_json, kind_json
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                    datetime('now'), datetime('now'), ?11, ?12
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
                    env_denylist_json,
                    hook.kind_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        info!("Inserted hook definition: {}", hook.name);
        Ok(())
    }

    /// Upserts a hook definition (insert or update by ID).
    pub fn upsert(&self, hook: &HookDefinitionDto) -> Result<(), StorageError> {
        // Note: We don't use a transaction wrapper here because:
        // 1. The main cfg_hook_definitions upsert is atomic
        // 2. Child table operations (hook_commands, hook_environment) are denormalized
        //    and can be rebuilt on next upsert if interrupted
        // 3. This avoids "cannot start a transaction within a transaction" errors
        //    when called from legacy import contexts

        let env_denylist_json =
            serde_json::to_string(&hook.env_denylist).unwrap_or_else(|_| "[]".to_string());

        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_hook_definitions (
                    id, name, execution_mode, script_ref, cwd,
                    inherit_env, timeout_ms, ready_signal, on_failure,
                    enabled, created_at, updated_at, env_denylist_json, kind_json
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                    datetime('now'), datetime('now'), ?11, ?12
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
                    env_denylist_json = excluded.env_denylist_json,
                    kind_json = COALESCE(excluded.kind_json, cfg_hook_definitions.kind_json),
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
                    env_denylist_json,
                    hook.kind_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        info!("Upserted hook definition: {}", hook.name);
        Ok(())
    }

    /// Updates an existing hook definition.
    pub fn update(&self, hook: &HookDefinitionDto) -> Result<(), StorageError> {
        // Note: We don't use a transaction wrapper here because:
        // 1. The main cfg_hook_definitions update is atomic
        // 2. Child table operations (hook_commands, hook_environment) are denormalized
        //    and can be rebuilt on next upsert if interrupted
        // 3. This avoids "cannot start a transaction within a transaction" errors
        //    when called from legacy import contexts

        let env_denylist_json =
            serde_json::to_string(&hook.env_denylist).unwrap_or_else(|_| "[]".to_string());

        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE cfg_hook_definitions SET
                    name = ?2,
                    execution_mode = ?3,
                    script_ref = ?4,
                    cwd = ?5,
                    inherit_env = ?6,
                    timeout_ms = ?7,
                    ready_signal = ?8,
                    on_failure = ?9,
                    enabled = ?10,
                    env_denylist_json = ?11,
                    kind_json = COALESCE(?12, kind_json),
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
                    env_denylist_json,
                    hook.kind_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
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
            .execute("DELETE FROM cfg_hook_definitions WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        info!("Deleted hook definition: {}", id);
        Ok(())
    }

    pub fn read_command_in_transaction(
        tx: &Transaction<'_>,
        hook_id: &str,
    ) -> Result<Option<HookCommandDto>, StorageError> {
        let result = tx.query_row(
            "SELECT id, hook_id, command, working_directory, timeout_ms, ready_signal
             FROM cfg_hook_commands WHERE hook_id = ?1",
            [hook_id],
            |row| {
                Ok(HookCommandDto {
                    id: row.get(0)?,
                    hook_id: row.get(1)?,
                    command: row.get(2)?,
                    working_directory: row.get(3)?,
                    timeout_ms: row.get(4)?,
                    ready_signal: row.get(5)?,
                })
            },
        );

        match result {
            Ok(command) => Ok(Some(command)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(source) => Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            }),
        }
    }

    pub fn write_command_in_transaction(
        tx: &Transaction<'_>,
        command: &HookCommandDto,
    ) -> Result<(), StorageError> {
        tx.execute(
            "INSERT INTO cfg_hook_commands (
                id, hook_id, command, working_directory, timeout_ms, ready_signal
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(hook_id) DO UPDATE SET
                command = excluded.command,
                working_directory = excluded.working_directory,
                timeout_ms = excluded.timeout_ms,
                ready_signal = excluded.ready_signal",
            params![
                command.id,
                command.hook_id,
                command.command,
                command.working_directory,
                command.timeout_ms,
                command.ready_signal,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;

        Ok(())
    }

    pub fn delete_command_in_transaction(
        tx: &Transaction<'_>,
        hook_id: &str,
    ) -> Result<(), StorageError> {
        tx.execute(
            "DELETE FROM cfg_hook_commands WHERE hook_id = ?1",
            [hook_id],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;

        Ok(())
    }

    pub fn read_environment_in_transaction(
        tx: &Transaction<'_>,
        hook_id: &str,
    ) -> Result<HashMap<String, String>, StorageError> {
        let mut statement = tx
            .prepare(
                "SELECT key, value FROM cfg_hook_environment WHERE hook_id = ?1 ORDER BY key ASC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        let rows = statement
            .query_map([hook_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        rows.collect::<Result<HashMap<_, _>, _>>()
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })
    }

    pub fn write_environment_in_transaction(
        tx: &Transaction<'_>,
        hook_id: &str,
        environment: &HashMap<String, String>,
    ) -> Result<(), StorageError> {
        Self::delete_environment_in_transaction(tx, hook_id)?;

        for (key, value) in environment {
            tx.execute(
                "INSERT INTO cfg_hook_environment (id, hook_id, key, value)
                 VALUES (?1, ?2, ?3, ?4)",
                params![Uuid::new_v4().to_string(), hook_id, key, value],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        }

        Ok(())
    }

    pub fn delete_environment_in_transaction(
        tx: &Transaction<'_>,
        hook_id: &str,
    ) -> Result<(), StorageError> {
        tx.execute(
            "DELETE FROM cfg_hook_environment WHERE hook_id = ?1",
            [hook_id],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;

        Ok(())
    }

    /// Replaces readable hook definitions and their normalized children in one transaction.
    pub fn replace_all_atomic(
        &self,
        desired: &[HookDefinitionReplacement],
        protected_ids: &HashSet<String>,
    ) -> Result<Vec<HookDefinitionDto>, StorageError> {
        let existing = self.all()?;
        let existing_by_id: HashMap<_, _> = existing.iter().map(|hook| (&hook.id, hook)).collect();
        let existing_by_name: HashMap<_, _> =
            existing.iter().map(|hook| (&hook.name, hook)).collect();
        let mut desired_ids = HashSet::new();
        let mut desired_names = HashSet::new();

        for replacement in desired {
            if !desired_names.insert(&replacement.definition.name) {
                return Err(StorageError::Data(format!(
                    "duplicate hook name: {}",
                    replacement.definition.name
                )));
            }

            if let Some(id) = &replacement.id {
                if replacement.definition.id != *id {
                    return Err(StorageError::Data(format!("hook ID mismatch: {id}")));
                }
                if !desired_ids.insert(id.clone()) {
                    return Err(StorageError::Data(format!("duplicate hook ID: {id}")));
                }
                if protected_ids.contains(id) {
                    return Err(StorageError::Data(format!("protected hook ID: {id}")));
                }
                if !existing_by_id.contains_key(id) {
                    return Err(StorageError::Data(format!("unknown hook ID: {id}")));
                }
            }

            if let Some(existing) = existing_by_name.get(&replacement.definition.name)
                && replacement.id.as_deref() != Some(existing.id.as_str())
            {
                return Err(StorageError::Data(format!(
                    "hook name already exists: {}",
                    replacement.definition.name
                )));
            }
        }

        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;
        let mut saved = Vec::with_capacity(desired.len());
        let mut retained_ids = desired_ids;

        for replacement in desired {
            let mut definition = replacement.definition.clone();
            definition.id = replacement
                .id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            retained_ids.insert(definition.id.clone());
            Self::upsert_in_transaction(&tx, &definition)?;

            match &replacement.command {
                Some(command) => {
                    let mut command = command.clone();
                    command.hook_id = definition.id.clone();
                    Self::write_command_in_transaction(&tx, &command)?;
                }
                None => Self::delete_command_in_transaction(&tx, &definition.id)?,
            }
            Self::write_environment_in_transaction(&tx, &definition.id, &replacement.environment)?;
            saved.push(definition);
        }

        for existing in existing {
            if !protected_ids.contains(&existing.id) && !retained_ids.contains(&existing.id) {
                tx.execute(
                    "DELETE FROM cfg_hook_definitions WHERE id = ?1",
                    [&existing.id],
                )
                .map_err(|source| StorageError::Sqlite {
                    path: "dbflux.db".into(),
                    source,
                })?;
            }
        }

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;
        Ok(saved)
    }

    fn upsert_in_transaction(
        tx: &Transaction<'_>,
        hook: &HookDefinitionDto,
    ) -> Result<(), StorageError> {
        let env_denylist_json =
            serde_json::to_string(&hook.env_denylist).unwrap_or_else(|_| "[]".to_string());

        tx.execute(
            r#"
            INSERT INTO cfg_hook_definitions (
                id, name, execution_mode, script_ref, cwd,
                inherit_env, timeout_ms, ready_signal, on_failure,
                enabled, created_at, updated_at, env_denylist_json, kind_json
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                datetime('now'), datetime('now'), ?11, ?12
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
                env_denylist_json = excluded.env_denylist_json,
                kind_json = COALESCE(excluded.kind_json, cfg_hook_definitions.kind_json),
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
                env_denylist_json,
                hook.kind_json,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        })?;

        Ok(())
    }

    /// Returns the count of hooks.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM cfg_hook_definitions", [], |row| {
                row.get(0)
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// A requested hook replacement including its normalized child data.
#[derive(Debug, Clone)]
pub struct HookDefinitionReplacement {
    pub id: Option<String>,
    pub definition: HookDefinitionDto,
    pub command: Option<HookCommandDto>,
    pub environment: HashMap<String, String>,
}

/// DTO for hook definition storage.
/// Canonical hook-kind data is stored in `kind_json`; command and environment
/// values remain in their normalized child tables for compatibility.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind_json: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_denylist: Vec<String>,
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
            kind_json: None,
            env_denylist: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("dbflux_repo_hook_{}_{}", name, std::process::id()))
    }

    #[test]
    fn replace_all_atomic_replaces_rows_and_children_after_preflight() {
        let path = temp_db("replace_all_atomic");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        #[allow(clippy::arc_with_non_send_sync)]
        let repo = HookDefinitionRepository::new(Arc::new(conn));
        let existing = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Existing".to_string(),
            "Command".to_string(),
        );
        let removed =
            HookDefinitionDto::new(Uuid::new_v4(), "Removed".to_string(), "Command".to_string());
        let protected = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Protected".to_string(),
            "Command".to_string(),
        );
        repo.insert(&existing).expect("should insert existing row");
        repo.insert(&removed).expect("should insert removed row");
        repo.insert(&protected)
            .expect("should insert protected row");
        repo.set_command(
            &removed.id,
            &HookCommandDto::new(removed.id.clone(), "echo removed".to_string()),
        )
        .expect("should seed removed command");
        repo.set_env(
            &removed.id,
            &HashMap::from([("REMOVED".to_string(), "value".to_string())]),
        )
        .expect("should seed removed environment");
        repo.conn()
            .execute(
                "UPDATE cfg_hook_definitions SET kind_json = ?1 WHERE id = ?2",
                params![r#"{"opaque":"protected"}"#, protected.id],
            )
            .expect("should seed protected bytes");

        let replacement = HookDefinitionReplacement {
            id: Some(existing.id.clone()),
            definition: HookDefinitionDto {
                name: "Renamed".to_string(),
                kind_json: Some(r#"{\"kind\":\"command\"}"#.to_string()),
                ..existing.clone()
            },
            command: Some(HookCommandDto::new(
                existing.id.clone(),
                "echo updated".to_string(),
            )),
            environment: HashMap::from([("KEY".to_string(), "VALUE".to_string())]),
        };
        let created = HookDefinitionReplacement {
            id: None,
            definition: HookDefinitionDto::new(
                Uuid::new_v4(),
                "Created".to_string(),
                "Script".to_string(),
            ),
            command: None,
            environment: HashMap::new(),
        };

        let requested_created_id = created.definition.id.clone();
        let saved = repo
            .replace_all_atomic(
                &[replacement, created],
                &HashSet::from([protected.id.clone()]),
            )
            .expect("should replace rows atomically");

        assert_eq!(saved.len(), 2);
        let saved_existing = saved
            .iter()
            .find(|definition| definition.name == "Renamed")
            .expect("existing replacement should be returned");
        let saved_created = saved
            .iter()
            .find(|definition| definition.name == "Created")
            .expect("created replacement should be returned");
        assert_eq!(saved_existing.id, existing.id);
        assert_ne!(saved_created.id, requested_created_id);
        assert!(Uuid::parse_str(&saved_created.id).is_ok());
        let persisted_created = repo
            .get(&saved_created.id)
            .expect("should fetch created row")
            .expect("created row should persist");
        assert_eq!(persisted_created.id, saved_created.id);
        assert_eq!(persisted_created.name, saved_created.name);
        assert!(
            repo.get(&removed.id)
                .expect("should fetch removed row")
                .is_none()
        );
        assert_eq!(
            repo.get(&existing.id)
                .expect("should fetch renamed row")
                .expect("renamed row should exist")
                .name,
            "Renamed"
        );
        assert_eq!(
            repo.get_command(&existing.id)
                .expect("should fetch command")
                .expect("command should exist")
                .command,
            "echo updated"
        );
        assert_eq!(
            repo.get_env(&existing.id)
                .expect("should fetch environment"),
            HashMap::from([("KEY".to_string(), "VALUE".to_string())])
        );
        assert_eq!(
            repo.get(&protected.id)
                .expect("should fetch protected row")
                .expect("protected row should exist")
                .name,
            "Protected"
        );
        assert_eq!(
            repo.conn()
                .query_row(
                    "SELECT kind_json FROM cfg_hook_definitions WHERE id = ?1",
                    [&protected.id],
                    |row| row.get::<_, String>(0),
                )
                .expect("should fetch protected bytes"),
            r#"{"opaque":"protected"}"#
        );
        assert!(
            repo.get_command(&removed.id)
                .expect("should fetch removed command")
                .is_none()
        );
        assert_eq!(
            repo.get_env(&removed.id)
                .expect("should fetch removed environment"),
            HashMap::new()
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn replace_all_atomic_rejects_invalid_desired_rows_without_mutation() {
        let path = temp_db("replace_all_preflight");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        #[allow(clippy::arc_with_non_send_sync)]
        let repo = HookDefinitionRepository::new(Arc::new(conn));
        let existing = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Existing".to_string(),
            "Command".to_string(),
        );
        repo.insert(&existing).expect("should insert existing row");

        let unknown_id = Uuid::new_v4().to_string();
        let invalid = HookDefinitionReplacement {
            id: Some(unknown_id.clone()),
            definition: HookDefinitionDto {
                id: unknown_id,
                name: "Changed".to_string(),
                ..existing.clone()
            },
            command: None,
            environment: HashMap::new(),
        };
        let error = repo
            .replace_all_atomic(&[invalid], &HashSet::new())
            .expect_err("unknown IDs must fail preflight");

        assert!(error.to_string().contains("unknown hook ID"));
        let persisted = repo
            .get(&existing.id)
            .expect("should fetch existing row")
            .expect("existing row should remain");
        assert_eq!(persisted.name, "Existing");
        assert_eq!(repo.count().expect("should count rows"), 1);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn replace_all_atomic_rejects_duplicate_and_protected_identities() {
        let path = temp_db("replace_all_identity_preflight");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        #[allow(clippy::arc_with_non_send_sync)]
        let repo = HookDefinitionRepository::new(Arc::new(conn));
        let existing = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Existing".to_string(),
            "Command".to_string(),
        );
        repo.insert(&existing).expect("should insert existing row");

        let replacement = |name: &str| HookDefinitionReplacement {
            id: Some(existing.id.clone()),
            definition: HookDefinitionDto {
                name: name.to_string(),
                ..existing.clone()
            },
            command: None,
            environment: HashMap::new(),
        };
        let duplicate_id = repo
            .replace_all_atomic(
                &[replacement("First"), replacement("Second")],
                &HashSet::new(),
            )
            .expect_err("duplicate IDs must fail preflight");
        assert!(duplicate_id.to_string().contains("duplicate hook ID"));

        let protected_id = repo
            .replace_all_atomic(
                &[replacement("Changed")],
                &HashSet::from([existing.id.clone()]),
            )
            .expect_err("protected IDs must fail preflight");
        assert!(protected_id.to_string().contains("protected hook ID"));
        assert_eq!(
            repo.get(&existing.id)
                .expect("should fetch existing row")
                .expect("existing row should remain")
                .name,
            "Existing"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn replace_all_atomic_rejects_all_name_and_identity_preflight_failures() {
        let path = temp_db("replace_all_complete_preflight");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        #[allow(clippy::arc_with_non_send_sync)]
        let repo = HookDefinitionRepository::new(Arc::new(conn));
        let readable = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Readable".to_string(),
            "Command".to_string(),
        );
        let protected = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Protected".to_string(),
            "Command".to_string(),
        );
        repo.insert(&readable).expect("should insert readable row");
        repo.insert(&protected)
            .expect("should insert protected row");
        repo.conn()
            .execute(
                "UPDATE cfg_hook_definitions SET kind_json = ?1 WHERE id = ?2",
                params![r#"{"opaque":"preserved"}"#, protected.id],
            )
            .expect("should seed protected bytes");

        let replacement =
            |id: Option<String>, definition_id: String, name: &str| HookDefinitionReplacement {
                id,
                definition: HookDefinitionDto {
                    id: definition_id,
                    name: name.to_string(),
                    ..readable.clone()
                },
                command: None,
                environment: HashMap::new(),
            };
        let protected_ids = HashSet::from([protected.id.clone()]);
        let invalid_cases = vec![
            (
                "duplicate hook name",
                vec![
                    replacement(Some(readable.id.clone()), readable.id.clone(), "Same"),
                    replacement(None, Uuid::new_v4().to_string(), "Same"),
                ],
            ),
            (
                "hook ID mismatch",
                vec![replacement(
                    Some(readable.id.clone()),
                    Uuid::new_v4().to_string(),
                    "Changed",
                )],
            ),
            (
                "hook name already exists",
                vec![replacement(None, Uuid::new_v4().to_string(), "Readable")],
            ),
            (
                "hook name already exists",
                vec![replacement(None, Uuid::new_v4().to_string(), "Protected")],
            ),
        ];

        for (expected_error, desired) in invalid_cases {
            let error = repo
                .replace_all_atomic(&desired, &protected_ids)
                .expect_err("invalid replacements must fail before mutation");
            assert!(error.to_string().contains(expected_error));
            assert_eq!(
                repo.get(&readable.id)
                    .expect("should fetch readable row")
                    .expect("readable row should remain")
                    .name,
                "Readable"
            );
            assert_eq!(
                repo.conn()
                    .query_row(
                        "SELECT kind_json FROM cfg_hook_definitions WHERE id = ?1",
                        [&protected.id],
                        |row| row.get::<_, String>(0),
                    )
                    .expect("should read protected bytes"),
                r#"{"opaque":"preserved"}"#
            );
        }

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn replace_all_atomic_rolls_back_parent_payload_children_and_deletions() {
        let path = temp_db("replace_all_rollback");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        #[allow(clippy::arc_with_non_send_sync)]
        let repo = HookDefinitionRepository::new(Arc::new(conn));
        let retained = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Retained".to_string(),
            "Command".to_string(),
        );
        let deleted =
            HookDefinitionDto::new(Uuid::new_v4(), "Deleted".to_string(), "Command".to_string());
        repo.insert(&retained).expect("should insert retained row");
        repo.insert(&deleted).expect("should insert deleted row");
        repo.set_command(
            &retained.id,
            &HookCommandDto::new(retained.id.clone(), "echo original".to_string()),
        )
        .expect("should seed retained command");
        repo.set_command(
            &deleted.id,
            &HookCommandDto::new(deleted.id.clone(), "echo deleted".to_string()),
        )
        .expect("should seed deleted command");
        repo.set_env(
            &retained.id,
            &HashMap::from([("ORIGINAL".to_string(), "value".to_string())]),
        )
        .expect("should seed retained environment");
        repo.conn()
            .execute(
                "UPDATE cfg_hook_definitions SET kind_json = ?1 WHERE id = ?2",
                params![r#"{"kind":"original"}"#, retained.id],
            )
            .expect("should seed original payload");
        repo.conn()
            .execute_batch(&format!(
                "CREATE TRIGGER abort_hook_deletion BEFORE DELETE ON cfg_hook_definitions
                 WHEN OLD.id = '{}' BEGIN SELECT RAISE(ABORT, 'forced rollback'); END",
                deleted.id
            ))
            .expect("should create rollback trigger");

        let replacement = HookDefinitionReplacement {
            id: Some(retained.id.clone()),
            definition: HookDefinitionDto {
                name: "Changed".to_string(),
                kind_json: Some(r#"{"kind":"changed"}"#.to_string()),
                ..retained.clone()
            },
            command: Some(HookCommandDto::new(
                retained.id.clone(),
                "echo changed".to_string(),
            )),
            environment: HashMap::from([("CHANGED".to_string(), "value".to_string())]),
        };
        let error = repo
            .replace_all_atomic(&[replacement], &HashSet::new())
            .expect_err("a late delete failure must roll back the replacement");
        assert!(error.to_string().contains("forced rollback"));

        assert_eq!(
            repo.get(&retained.id)
                .expect("should fetch retained row")
                .expect("retained row should survive")
                .name,
            "Retained"
        );
        assert_eq!(
            repo.get(&retained.id)
                .expect("should fetch retained payload")
                .expect("retained row should survive")
                .kind_json
                .as_deref(),
            Some(r#"{"kind":"original"}"#)
        );
        assert_eq!(
            repo.get_command(&retained.id)
                .expect("should fetch retained command")
                .expect("retained command should survive")
                .command,
            "echo original"
        );
        assert_eq!(
            repo.get_env(&retained.id)
                .expect("should fetch retained environment"),
            HashMap::from([("ORIGINAL".to_string(), "value".to_string())])
        );
        assert_eq!(
            repo.get(&deleted.id)
                .expect("should fetch deleted row")
                .expect("omitted row deletion should roll back")
                .name,
            "Deleted"
        );
        assert_eq!(
            repo.get_command(&deleted.id)
                .expect("should fetch omitted row command")
                .expect("omitted row command should roll back")
                .command,
            "echo deleted"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn hook_insert_and_fetch() {
        let path = temp_db("hook_insert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        let mut dto = HookDefinitionDto::new(
            Uuid::new_v4(),
            "PreConnect Test".to_string(),
            "Command".to_string(),
        );
        dto.kind_json =
            Some(r#"{"kind":"command","command":"echo hello","args":["world"]}"#.to_string());

        #[allow(clippy::arc_with_non_send_sync)]
        let repo = HookDefinitionRepository::new(Arc::new(conn));
        repo.insert(&dto).expect("should insert");

        let fetched = repo.all().expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].name, "PreConnect Test");
        assert_eq!(fetched[0].kind_json, dto.kind_json);

        dto.kind_json = Some(r#"{"kind":"script","language":"bash"}"#.to_string());
        repo.update(&dto).expect("should update canonical kind");

        let updated = repo
            .get(&dto.id)
            .expect("should fetch updated hook")
            .expect("updated hook should exist");
        assert_eq!(updated.kind_json, dto.kind_json);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn transaction_local_child_helpers_replace_command_and_environment() {
        let path = temp_db("transaction_children");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        let hook = HookDefinitionDto::new(
            Uuid::new_v4(),
            "PreConnect Test".to_string(),
            "Command".to_string(),
        );
        #[allow(clippy::arc_with_non_send_sync)]
        let mut conn = Arc::new(conn);
        let repo = HookDefinitionRepository::new(conn.clone());
        repo.insert(&hook).expect("should insert hook");
        drop(repo);

        let tx = Arc::get_mut(&mut conn)
            .expect("repository references should be released")
            .unchecked_transaction()
            .expect("should start transaction");
        let command = HookCommandDto::new(hook.id.clone(), "echo replacement".to_string());
        let environment = HashMap::from([
            ("FIRST".to_string(), "one".to_string()),
            ("SECOND".to_string(), "two".to_string()),
        ]);

        HookDefinitionRepository::write_command_in_transaction(&tx, &command)
            .expect("should write command");
        HookDefinitionRepository::write_environment_in_transaction(&tx, &hook.id, &environment)
            .expect("should write environment");

        assert_eq!(
            HookDefinitionRepository::read_command_in_transaction(&tx, &hook.id)
                .expect("should read command")
                .expect("command should exist")
                .command,
            "echo replacement"
        );
        assert_eq!(
            HookDefinitionRepository::read_environment_in_transaction(&tx, &hook.id)
                .expect("should read environment"),
            environment
        );

        HookDefinitionRepository::delete_command_in_transaction(&tx, &hook.id)
            .expect("should delete command");
        HookDefinitionRepository::delete_environment_in_transaction(&tx, &hook.id)
            .expect("should delete environment");

        assert!(
            HookDefinitionRepository::read_command_in_transaction(&tx, &hook.id)
                .expect("should read deleted command")
                .is_none()
        );
        assert_eq!(
            HookDefinitionRepository::read_environment_in_transaction(&tx, &hook.id)
                .expect("should read deleted environment"),
            HashMap::new()
        );

        tx.commit().expect("should commit transaction");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn update_retains_legacy_kind_json_when_no_canonical_payload_is_provided() {
        let path = temp_db("legacy_kind_json");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        let dto = HookDefinitionDto::new(
            Uuid::new_v4(),
            "Legacy Hook".to_string(),
            "Command".to_string(),
        );
        #[allow(clippy::arc_with_non_send_sync)]
        let repo = HookDefinitionRepository::new(Arc::new(conn));
        repo.insert(&dto).expect("should insert legacy hook");
        repo.conn()
            .execute(
                "UPDATE cfg_hook_definitions SET kind_json = ?1 WHERE id = ?2",
                params![r#"{"legacy":"payload"}"#, dto.id],
            )
            .expect("should seed legacy bytes");

        let mut updated = repo
            .get(&dto.id)
            .expect("should fetch legacy hook")
            .expect("legacy hook should exist");
        updated.name = "Renamed Legacy Hook".to_string();
        updated.kind_json = None;
        repo.update(&updated).expect("should update legacy hook");
        updated.name = "Upserted Legacy Hook".to_string();
        repo.upsert(&updated).expect("should upsert legacy hook");

        let fetched = repo
            .get(&dto.id)
            .expect("should fetch updated legacy hook")
            .expect("updated legacy hook should exist");
        assert_eq!(fetched.name, "Upserted Legacy Hook");
        assert_eq!(
            fetched.kind_json.as_deref(),
            Some(r#"{"legacy":"payload"}"#)
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
