//! Repository for connection profile governance settings in config.db.
//!
//! This module provides CRUD operations for the connection_profile_governance child table,
//! which stores MCP governance settings for connection profiles.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing connection profile governance settings.
/// This is always used behind a ConnectionProfileRepository.
pub struct ConnectionProfileGovernanceRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileGovernanceRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all governance settings for a connection profile.
    pub fn get_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<ConnectionProfileGovernanceDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, profile_id, governance_key, governance_value
                FROM connection_profile_governance
                WHERE profile_id = ?1
                ORDER BY governance_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let settings = stmt
            .query_map([profile_id], |row| {
                Ok(ConnectionProfileGovernanceDto {
                    id: row.get(0)?,
                    profile_id: row.get(1)?,
                    governance_key: row.get(2)?,
                    governance_value: row.get(3)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for setting in settings {
            match setting {
                Ok(s) => result.push(s),
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

    /// Inserts a single governance setting.
    pub fn insert(&self, governance: &ConnectionProfileGovernanceDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_governance (
                    id, profile_id, governance_key, governance_value
                ) VALUES (?1, ?2, ?3, ?4)
                "#,
                params![
                    governance.id,
                    governance.profile_id,
                    governance.governance_key,
                    governance.governance_value,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Upserts a governance setting (insert or update by profile_id and governance_key).
    pub fn upsert(&self, governance: &ConnectionProfileGovernanceDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_governance (
                    id, profile_id, governance_key, governance_value
                ) VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(profile_id, governance_key) DO UPDATE SET
                    governance_value = excluded.governance_value
                "#,
                params![
                    governance.id,
                    governance.profile_id,
                    governance.governance_key,
                    governance.governance_value,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!(
            "Upserted connection profile governance for profile: {}",
            governance.profile_id
        );
        Ok(())
    }

    /// Deletes governance settings for a connection profile.
    pub fn delete_for_profile(&self, profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM connection_profile_governance WHERE profile_id = ?1",
                [profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }
}

/// DTO for connection profile governance settings (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileGovernanceDto {
    pub id: String,
    pub profile_id: String,
    pub governance_key: String,
    pub governance_value: Option<String>,
}

impl ConnectionProfileGovernanceDto {
    /// Creates a new governance DTO.
    pub fn new(
        profile_id: String,
        governance_key: String,
        governance_value: Option<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            governance_key,
            governance_value,
        }
    }
}
