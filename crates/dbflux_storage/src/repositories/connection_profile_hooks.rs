//! Repository for connection profile inline hooks in config.db.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

pub struct ConnectionProfileHooksRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileHooksRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn get_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<ConnectionProfileHookDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, profile_id, phase, order_index, enabled, hook_kind,
                       command, script_language, script_source_type, script_content, script_path,
                       lua_source_type, lua_content, lua_path,
                       lua_log, lua_env_read, lua_conn_metadata, lua_process_run,
                       cwd, inherit_env, timeout_ms, execution_mode, ready_signal, on_failure
                FROM connection_profile_hooks
                WHERE profile_id = ?1
                ORDER BY phase ASC, order_index ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let hooks = stmt
            .query_map([profile_id], |row| {
                Ok(ConnectionProfileHookDto {
                    id: row.get(0)?,
                    profile_id: row.get(1)?,
                    phase: row.get(2)?,
                    order_index: row.get(3)?,
                    enabled: row.get::<_, i32>(4)? != 0,
                    hook_kind: row.get(5)?,
                    command: row.get(6)?,
                    script_language: row.get(7)?,
                    script_source_type: row.get(8)?,
                    script_content: row.get(9)?,
                    script_path: row.get(10)?,
                    lua_source_type: row.get(11)?,
                    lua_content: row.get(12)?,
                    lua_path: row.get(13)?,
                    lua_log: row.get::<_, i32>(14)? != 0,
                    lua_env_read: row.get::<_, i32>(15)? != 0,
                    lua_conn_metadata: row.get::<_, i32>(16)? != 0,
                    lua_process_run: row.get::<_, i32>(17)? != 0,
                    cwd: row.get(18)?,
                    inherit_env: row.get::<_, i32>(19)? != 0,
                    timeout_ms: row.get(20)?,
                    execution_mode: row.get(21)?,
                    ready_signal: row.get(22)?,
                    on_failure: row.get(23)?,
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

    pub fn insert(&self, hook: &ConnectionProfileHookDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_hooks (
                    id, profile_id, phase, order_index, enabled, hook_kind,
                    command, script_language, script_source_type, script_content, script_path,
                    lua_source_type, lua_content, lua_path,
                    lua_log, lua_env_read, lua_conn_metadata, lua_process_run,
                    cwd, inherit_env, timeout_ms, execution_mode, ready_signal, on_failure
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                    ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24
                )
                "#,
                params![
                    hook.id,
                    hook.profile_id,
                    hook.phase,
                    hook.order_index,
                    hook.enabled as i32,
                    hook.hook_kind,
                    hook.command,
                    hook.script_language,
                    hook.script_source_type,
                    hook.script_content,
                    hook.script_path,
                    hook.lua_source_type,
                    hook.lua_content,
                    hook.lua_path,
                    hook.lua_log as i32,
                    hook.lua_env_read as i32,
                    hook.lua_conn_metadata as i32,
                    hook.lua_process_run as i32,
                    hook.cwd,
                    hook.inherit_env as i32,
                    hook.timeout_ms,
                    hook.execution_mode,
                    hook.ready_signal,
                    hook.on_failure,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    pub fn delete_for_profile(&self, profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM connection_profile_hooks WHERE profile_id = ?1",
                [profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM connection_profile_hooks",
                [],
                |row| row.get(0),
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileHookDto {
    pub id: String,
    pub profile_id: String,
    pub phase: String,
    pub order_index: i32,
    pub enabled: bool,
    pub hook_kind: String,
    pub command: Option<String>,
    pub script_language: Option<String>,
    pub script_source_type: Option<String>,
    pub script_content: Option<String>,
    pub script_path: Option<String>,
    pub lua_source_type: Option<String>,
    pub lua_content: Option<String>,
    pub lua_path: Option<String>,
    pub lua_log: bool,
    pub lua_env_read: bool,
    pub lua_conn_metadata: bool,
    pub lua_process_run: bool,
    pub cwd: Option<String>,
    pub inherit_env: bool,
    pub timeout_ms: Option<i64>,
    pub execution_mode: String,
    pub ready_signal: Option<String>,
    pub on_failure: String,
}

impl ConnectionProfileHookDto {
    pub fn new_command(
        profile_id: String,
        phase: String,
        order_index: i32,
        command: String,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            phase,
            order_index,
            enabled: true,
            hook_kind: "command".to_string(),
            command: Some(command),
            script_language: None,
            script_source_type: None,
            script_content: None,
            script_path: None,
            lua_source_type: None,
            lua_content: None,
            lua_path: None,
            lua_log: true,
            lua_env_read: true,
            lua_conn_metadata: true,
            lua_process_run: false,
            cwd: None,
            inherit_env: true,
            timeout_ms: None,
            execution_mode: "blocking".to_string(),
            ready_signal: None,
            on_failure: "disconnect".to_string(),
        }
    }
}
