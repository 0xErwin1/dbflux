//! Repository for connection profile governance bindings in config.db.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

pub struct ConnectionProfileGovernanceBindingsRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileGovernanceBindingsRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn get_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<ConnectionProfileGovernanceBindingDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, profile_id, actor_id, order_index
                FROM connection_profile_governance_bindings
                WHERE profile_id = ?1
                ORDER BY order_index ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let bindings = stmt
            .query_map([profile_id], |row| {
                Ok(ConnectionProfileGovernanceBindingDto {
                    id: row.get(0)?,
                    profile_id: row.get(1)?,
                    actor_id: row.get(2)?,
                    order_index: row.get(3)?,
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

    pub fn insert(
        &self,
        binding: &ConnectionProfileGovernanceBindingDto,
    ) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_governance_bindings
                    (id, profile_id, actor_id, order_index)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![
                    binding.id,
                    binding.profile_id,
                    binding.actor_id,
                    binding.order_index,
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
                "DELETE FROM connection_profile_governance_bindings WHERE profile_id = ?1",
                [profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileGovernanceBindingDto {
    pub id: String,
    pub profile_id: String,
    pub actor_id: String,
    pub order_index: i32,
}

impl ConnectionProfileGovernanceBindingDto {
    pub fn new(profile_id: String, actor_id: String, order_index: i32) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            actor_id,
            order_index,
        }
    }
}
