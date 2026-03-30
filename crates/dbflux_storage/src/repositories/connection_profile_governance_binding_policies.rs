//! Repository for connection profile governance binding policies in config.db.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

pub struct ConnectionProfileGovernanceBindingPoliciesRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileGovernanceBindingPoliciesRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn get_for_binding(
        &self,
        binding_id: &str,
    ) -> Result<Vec<ConnectionProfileGovernanceBindingPolicyDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, binding_id, policy_id
                FROM connection_profile_governance_binding_policies
                WHERE binding_id = ?1
                ORDER BY policy_id ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let policies = stmt
            .query_map([binding_id], |row| {
                Ok(ConnectionProfileGovernanceBindingPolicyDto {
                    id: row.get(0)?,
                    binding_id: row.get(1)?,
                    policy_id: row.get(2)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for policy in policies {
            match policy {
                Ok(p) => result.push(p),
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
        policy: &ConnectionProfileGovernanceBindingPolicyDto,
    ) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_governance_binding_policies
                    (id, binding_id, policy_id)
                VALUES (?1, ?2, ?3)
                "#,
                params![policy.id, policy.binding_id, policy.policy_id],
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
                "DELETE FROM connection_profile_governance_binding_policies WHERE binding_id = ?1",
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
pub struct ConnectionProfileGovernanceBindingPolicyDto {
    pub id: String,
    pub binding_id: String,
    pub policy_id: String,
}

impl ConnectionProfileGovernanceBindingPolicyDto {
    pub fn new(binding_id: String, policy_id: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            binding_id,
            policy_id,
        }
    }
}
