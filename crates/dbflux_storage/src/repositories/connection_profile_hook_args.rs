//! Repository for connection profile hook args in config.db.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

pub struct ConnectionProfileHookArgsRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileHookArgsRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn get_for_hook(
        &self,
        hook_id: &str,
    ) -> Result<Vec<ConnectionProfileHookArgDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, hook_id, position, value
                FROM connection_profile_hook_args
                WHERE hook_id = ?1
                ORDER BY position ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let args = stmt
            .query_map([hook_id], |row| {
                Ok(ConnectionProfileHookArgDto {
                    id: row.get(0)?,
                    hook_id: row.get(1)?,
                    position: row.get(2)?,
                    value: row.get(3)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for arg in args {
            match arg {
                Ok(a) => result.push(a),
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

    pub fn insert(&self, arg: &ConnectionProfileHookArgDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO connection_profile_hook_args (id, hook_id, position, value)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![arg.id, arg.hook_id, arg.position, arg.value],
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
                "DELETE FROM connection_profile_hook_args WHERE hook_id = ?1",
                [hook_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    pub fn insert_batch(&self, hook_id: &str, args: &[String]) -> Result<(), StorageError> {
        for (i, value) in args.iter().enumerate() {
            let dto = ConnectionProfileHookArgDto {
                id: uuid::Uuid::new_v4().to_string(),
                hook_id: hook_id.to_string(),
                position: i as i32,
                value: value.clone(),
            };
            self.insert(&dto)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileHookArgDto {
    pub id: String,
    pub hook_id: String,
    pub position: i32,
    pub value: String,
}
