//! Repository for connection profile hook environment variables in config.db.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

pub struct ConnectionProfileHookEnvsRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileHookEnvsRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn get_for_hook(
        &self,
        hook_id: &str,
    ) -> Result<Vec<ConnectionProfileHookEnvDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, hook_id, key, value
                FROM connection_profile_hook_envs
                WHERE hook_id = ?1
                ORDER BY key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let envs = stmt
            .query_map([hook_id], |row| {
                Ok(ConnectionProfileHookEnvDto {
                    id: row.get(0)?,
                    hook_id: row.get(1)?,
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
        for env in envs {
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

    pub fn insert(&self, env: &ConnectionProfileHookEnvDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_hook_envs (id, hook_id, key, value)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(hook_id, key) DO UPDATE SET value = excluded.value
                "#,
                params![env.id, env.hook_id, env.key, env.value],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    pub fn delete_for_hook(&self, hook_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM connection_profile_hook_envs WHERE hook_id = ?1",
                [hook_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    pub fn insert_batch(
        &self,
        hook_id: &str,
        env: &HashMap<String, String>,
    ) -> Result<(), StorageError> {
        for (key, value) in env {
            let dto = ConnectionProfileHookEnvDto {
                id: uuid::Uuid::new_v4().to_string(),
                hook_id: hook_id.to_string(),
                key: key.clone(),
                value: value.clone(),
            };
            self.insert(&dto)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileHookEnvDto {
    pub id: String,
    pub hook_id: String,
    pub key: String,
    pub value: String,
}
