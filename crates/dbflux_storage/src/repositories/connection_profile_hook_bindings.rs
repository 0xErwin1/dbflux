//! Repository for connection profile hook bindings in config.db.
//!
//! This module provides CRUD operations for the connection_profile_hook_bindings child table,
//! which stores bindings of global hooks to connection profiles by phase.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing connection profile hook bindings.
/// This is always used behind a ConnectionProfileRepository.
pub struct ConnectionProfileHookBindingsRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileHookBindingsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all hook bindings for a connection profile.
    pub fn get_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<ConnectionProfileHookBindingDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, profile_id, hook_id, phase, order_index
                FROM connection_profile_hook_bindings
                WHERE profile_id = ?1
                ORDER BY phase ASC, order_index ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let bindings = stmt
            .query_map([profile_id], |row| {
                Ok(ConnectionProfileHookBindingDto {
                    id: row.get(0)?,
                    profile_id: row.get(1)?,
                    hook_id: row.get(2)?,
                    phase: row.get(3)?,
                    order_index: row.get(4)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for binding in bindings {
            match binding {
                Ok(b) => result.push(b),
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

    /// Inserts a single hook binding.
    pub fn insert(&self, binding: &ConnectionProfileHookBindingDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_hook_bindings (
                    id, profile_id, hook_id, phase, order_index
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![
                    binding.id,
                    binding.profile_id,
                    binding.hook_id,
                    binding.phase,
                    binding.order_index,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Upserts a hook binding (insert or update by profile_id + hook_id + phase).
    pub fn upsert(&self, binding: &ConnectionProfileHookBindingDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_hook_bindings (
                    id, profile_id, hook_id, phase, order_index
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(profile_id, hook_id, phase) DO UPDATE SET
                    order_index = excluded.order_index
                "#,
                params![
                    binding.id,
                    binding.profile_id,
                    binding.hook_id,
                    binding.phase,
                    binding.order_index,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!(
            "Upserted connection profile hook binding: {} -> {} for profile: {}",
            binding.hook_id, binding.phase, binding.profile_id
        );
        Ok(())
    }

    /// Deletes all hook bindings for a connection profile.
    pub fn delete_for_profile(&self, profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM connection_profile_hook_bindings WHERE profile_id = ?1",
                [profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Replaces all hook bindings for a profile (delete old, insert new).
    pub fn replace_for_profile(
        &self,
        profile_id: &str,
        bindings: &[ConnectionProfileHookBindingDto],
    ) -> Result<(), StorageError> {
        self.delete_for_profile(profile_id)?;
        for binding in bindings {
            self.insert(binding)?;
        }
        Ok(())
    }
}

/// DTO for connection profile hook bindings (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileHookBindingDto {
    pub id: String,
    pub profile_id: String,
    pub hook_id: String,
    pub phase: String,
    pub order_index: i32,
}

impl ConnectionProfileHookBindingDto {
    /// Creates a new hook binding DTO.
    pub fn new(profile_id: String, hook_id: String, phase: String, order_index: i32) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            hook_id,
            phase,
            order_index,
        }
    }
}
