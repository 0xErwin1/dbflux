//! Repository for connection profile governance binding roles in config.db.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

pub struct ConnectionProfileGovernanceBindingRolesRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileGovernanceBindingRolesRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn get_for_binding(
        &self,
        binding_id: &str,
    ) -> Result<Vec<ConnectionProfileGovernanceBindingRoleDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, binding_id, role_id
                FROM connection_profile_governance_binding_roles
                WHERE binding_id = ?1
                ORDER BY role_id ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let roles = stmt
            .query_map([binding_id], |row| {
                Ok(ConnectionProfileGovernanceBindingRoleDto {
                    id: row.get(0)?,
                    binding_id: row.get(1)?,
                    role_id: row.get(2)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for role in roles {
            match role {
                Ok(r) => result.push(r),
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
        role: &ConnectionProfileGovernanceBindingRoleDto,
    ) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_governance_binding_roles (id, binding_id, role_id)
                VALUES (?1, ?2, ?3)
                "#,
                params![role.id, role.binding_id, role.role_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    pub fn delete_for_binding(&self, binding_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM connection_profile_governance_binding_roles WHERE binding_id = ?1",
                [binding_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileGovernanceBindingRoleDto {
    pub id: String,
    pub binding_id: String,
    pub role_id: String,
}

impl ConnectionProfileGovernanceBindingRoleDto {
    pub fn new(binding_id: String, role_id: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            binding_id,
            role_id,
        }
    }
}
