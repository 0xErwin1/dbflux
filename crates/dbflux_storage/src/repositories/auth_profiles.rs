//! Repository for auth profiles in config.db.
//!
//! Auth profiles store authentication configurations for connecting to
//! cloud-hosted databases (e.g., AWS SSO, Azure AD).

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing auth profiles.
pub struct AuthProfileRepository {
    conn: OwnedConnection,
}

impl AuthProfileRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all auth profiles.
    pub fn all(&self) -> Result<Vec<AuthProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, provider_id, fields_json, enabled, created_at, updated_at
                FROM auth_profiles
                ORDER BY name ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let profiles = stmt
            .query_map([], |row| {
                Ok(AuthProfileDto {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    provider_id: row.get(2)?,
                    fields_json: row.get(3)?,
                    enabled: row.get::<_, i32>(4)? != 0,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for profile in profiles {
            match profile {
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

    /// Fetches a single auth profile by ID.
    pub fn get(&self, id: &str) -> Result<Option<AuthProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, provider_id, fields_json, enabled, created_at, updated_at
                FROM auth_profiles
                WHERE id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([id], |row| {
            Ok(AuthProfileDto {
                id: row.get(0)?,
                name: row.get(1)?,
                provider_id: row.get(2)?,
                fields_json: row.get(3)?,
                enabled: row.get::<_, i32>(4)? != 0,
                created_at: row.get(5)?,
                updated_at: row.get(6)?,
            })
        });

        match result {
            Ok(profile) => Ok(Some(profile)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts a new auth profile.
    pub fn insert(&self, profile: &AuthProfileDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO auth_profiles (
                    id, name, provider_id, fields_json, enabled, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, datetime('now'), datetime('now')
                )
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.provider_id,
                    profile.fields_json,
                    profile.enabled as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Inserted auth profile: {}", profile.name);
        Ok(())
    }

    /// Updates an existing auth profile.
    pub fn update(&self, profile: &AuthProfileDto) -> Result<(), StorageError> {
        let rows_affected = self
            .conn()
            .execute(
                r#"
                UPDATE auth_profiles SET
                    name = ?2,
                    provider_id = ?3,
                    fields_json = ?4,
                    enabled = ?5,
                    updated_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.provider_id,
                    profile.fields_json,
                    profile.enabled as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            info!("No auth profile found to update: {}", profile.id);
        } else {
            info!("Updated auth profile: {}", profile.name);
        }

        Ok(())
    }

    /// Inserts a new auth profile from the core AuthProfile type.
    pub fn insert_auth_profile(
        &self,
        profile: &dbflux_core::AuthProfile,
    ) -> Result<(), StorageError> {
        let fields_json =
            serde_json::to_string(&profile.fields).map_err(|e| StorageError::Sqlite {
                path: "config.db".into(),
                source: rusqlite::Error::InvalidParameterName(e.to_string()),
            })?;

        let dto = AuthProfileDto {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            provider_id: profile.provider_id.clone(),
            fields_json,
            enabled: profile.enabled,
            created_at: String::new(),
            updated_at: String::new(),
        };

        self.insert(&dto)
    }

    /// Deletes an auth profile by ID.
    pub fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute("DELETE FROM auth_profiles WHERE id = ?1", [id])
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Deleted auth profile: {}", id);
        Ok(())
    }

    /// Returns the count of profiles.
    pub fn count(&self) -> Result<i64, StorageError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT COUNT(*) FROM auth_profiles", [], |row| row.get(0))
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(count)
    }
}

/// DTO for auth profile storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProfileDto {
    pub id: String,
    pub name: String,
    pub provider_id: String,
    pub fields_json: String,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl AuthProfileDto {
    /// Creates a new DTO.
    pub fn new(id: Uuid, name: String, provider_id: String, fields_json: String) -> Self {
        Self {
            id: id.to_string(),
            name,
            provider_id,
            fields_json,
            enabled: true,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("dbflux_repo_auth_{}_{}", name, std::process::id()))
    }

    #[test]
    fn insert_and_fetch_auth_profile() {
        let path = temp_db("auth_insert_fetch");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let dto = AuthProfileDto::new(
            Uuid::new_v4(),
            "AWS SSO".to_string(),
            "aws-sso".to_string(),
            r#"{"sso_start_url":"https://example.awsapps.com"}"#.to_string(),
        );

        let repo = AuthProfileRepository::new(Arc::new(conn));
        repo.insert(&dto).expect("should insert");

        let fetched = repo.all().expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].name, "AWS SSO");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
