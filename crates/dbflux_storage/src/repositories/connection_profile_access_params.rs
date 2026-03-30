//! Repository for connection profile access params in config.db.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

pub struct ConnectionProfileAccessParamsRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileAccessParamsRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn get_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<ConnectionProfileAccessParamDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, profile_id, param_key, param_value
                FROM connection_profile_access_params
                WHERE profile_id = ?1
                ORDER BY param_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let params_iter = stmt
            .query_map([profile_id], |row| {
                Ok(ConnectionProfileAccessParamDto {
                    id: row.get(0)?,
                    profile_id: row.get(1)?,
                    param_key: row.get(2)?,
                    param_value: row.get(3)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for p in params_iter {
            match p {
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

    pub fn insert(&self, param: &ConnectionProfileAccessParamDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_access_params (id, profile_id, param_key, param_value)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(profile_id, param_key) DO UPDATE SET param_value = excluded.param_value
                "#,
                params![param.id, param.profile_id, param.param_key, param.param_value],
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
                "DELETE FROM connection_profile_access_params WHERE profile_id = ?1",
                [profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    pub fn upsert_batch(
        &self,
        profile_id: &str,
        params_map: &HashMap<String, String>,
    ) -> Result<(), StorageError> {
        for (key, value) in params_map {
            let dto = ConnectionProfileAccessParamDto {
                id: uuid::Uuid::new_v4().to_string(),
                profile_id: profile_id.to_string(),
                param_key: key.clone(),
                param_value: value.clone(),
            };
            self.insert(&dto)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileAccessParamDto {
    pub id: String,
    pub profile_id: String,
    pub param_key: String,
    pub param_value: String,
}
