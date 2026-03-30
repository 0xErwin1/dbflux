//! Repository for hook environment variables in config.db.
//!
//! This module provides CRUD operations for the hook_environment child table,
//! which stores environment variables for hook definitions.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing hook environment variables.
/// This is always used behind a HookDefinitionRepository.
pub struct HookEnvRepository {
    conn: OwnedConnection,
}

impl HookEnvRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all environment variables for a hook.
    pub fn get_for_hook(&self, hook_id: &str) -> Result<Vec<HookEnvDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, hook_id, key, value
                FROM hook_environment
                WHERE hook_id = ?1
                ORDER BY key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let env_vars = stmt
            .query_map([hook_id], |row| {
                Ok(HookEnvDto {
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
    pub fn get_map_for_hook(
        &self,
        hook_id: &str,
    ) -> Result<std::collections::HashMap<String, String>, StorageError> {
        let vars = self.get_for_hook(hook_id)?;
        Ok(vars.into_iter().map(|e| (e.key, e.value)).collect())
    }

    /// Inserts a single environment variable.
    pub fn insert(&self, env: &HookEnvDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO hook_environment (id, hook_id, key, value)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![env.id, env.hook_id, env.key, env.value,],
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
        hook_id: &str,
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
        tx.execute("DELETE FROM hook_environment WHERE hook_id = ?1", [hook_id])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        // Insert new env vars
        for (key, value) in env_vars.iter() {
            let id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                r#"
                INSERT INTO hook_environment (id, hook_id, key, value)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![id, hook_id, key, value],
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

        info!("Inserted {} env vars for hook: {}", env_vars.len(), hook_id);
        Ok(())
    }

    /// Deletes all environment variables for a hook.
    pub fn delete_for_hook(&self, hook_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM hook_environment WHERE hook_id = ?1", [hook_id])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }
}

/// DTO for hook environment variables (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookEnvDto {
    pub id: String,
    pub hook_id: String,
    pub key: String,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::repositories::hook_definitions::{HookDefinitionDto, HookDefinitionRepository};
    use crate::sqlite::open_database;
    use std::collections::HashMap;
    use std::sync::Arc;
    use uuid::Uuid;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_hook_env_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn hook_env_insert_and_fetch() {
        let path = temp_db("hook_env_insert");
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

        let mut env_vars = HashMap::new();
        env_vars.insert("HOOK_VAR".to_string(), "hook_value".to_string());
        env_vars.insert("ANOTHER_VAR".to_string(), "another_value".to_string());

        let repo = HookEnvRepository::new(conn_arc);
        repo.insert_many(&hook.id, &env_vars)
            .expect("should insert env vars");

        let fetched = repo.get_for_hook(&hook.id).expect("should fetch");
        assert_eq!(fetched.len(), 2);

        let map = repo.get_map_for_hook(&hook.id).expect("should get map");
        assert_eq!(map.get("HOOK_VAR"), Some(&"hook_value".to_string()));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn hook_env_replace_existing() {
        let path = temp_db("hook_env_replace");
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

        let repo = HookEnvRepository::new(conn_arc);

        // Insert initial
        let mut initial = HashMap::new();
        initial.insert("OLD_VAR".to_string(), "old_value".to_string());
        repo.insert_many(&hook.id, &initial).expect("should insert");

        // Replace with new
        let mut replacement = HashMap::new();
        replacement.insert("NEW_VAR".to_string(), "new_value".to_string());
        repo.insert_many(&hook.id, &replacement)
            .expect("should replace");

        let fetched = repo.get_for_hook(&hook.id).expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].key, "NEW_VAR");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
