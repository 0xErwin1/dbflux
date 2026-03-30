//! Repository for auth profiles in config.db.
//!
//! Auth profiles store authentication configurations for connecting to
//! cloud-hosted databases (e.g., AWS SSO, Azure AD).
//!
//! This repository supports both legacy fields_json column and the normalized
//! auth_profile_fields child table with EAV pattern for the transition period.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

use super::auth_profile_fields::{AuthProfileFieldDto, AuthProfileFieldsRepository};

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

    /// Returns an AuthProfileFieldsRepository for managing EAV field values.
    pub fn fields_repo(&self) -> AuthProfileFieldsRepository {
        AuthProfileFieldsRepository::new(self.conn.clone())
    }

    /// Gets the fields for a profile as a HashMap<String, String> (text values only).
    /// Reads from native auth_profile_fields table (fields_json column dropped in v10).
    pub fn get_fields(&self, id: &str) -> Result<HashMap<String, String>, StorageError> {
        let native_fields = self.fields_repo().get_for_profile(id)?;
        let mut result = HashMap::new();
        for field in native_fields {
            if field.value_kind == "text"
                && let Some(text) = field.value_text
            {
                result.insert(field.field_key, text);
            }
        }
        Ok(result)
    }

    /// Sets the fields for a profile from a HashMap.
    /// Writes to native auth_profile_fields table only (fields_json column dropped in v10).
    pub fn set_fields(
        &self,
        id: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), StorageError> {
        // Write to native child table - all values as text for simplicity
        let repo = self.fields_repo();
        repo.delete_for_profile(id)?;

        for (key, value) in fields.iter() {
            repo.insert(&AuthProfileFieldDto::new_text(
                id.to_string(),
                key.clone(),
                value.clone(),
            ))?;
        }

        Ok(())
    }

    /// Fetches all auth profiles.
    pub fn all(&self) -> Result<Vec<AuthProfileDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, name, provider_id, enabled, created_at, updated_at
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
                    enabled: row.get::<_, i32>(3)? != 0,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
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
                SELECT id, name, provider_id, enabled, created_at, updated_at
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
                enabled: row.get::<_, i32>(3)? != 0,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
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
        // Start transaction for atomic write
        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        tx.execute(
            r#"
                INSERT INTO auth_profiles (
                    id, name, provider_id, enabled, created_at, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, datetime('now'), datetime('now')
                )
                "#,
            params![
                profile.id,
                profile.name,
                profile.provider_id,
                profile.enabled as i32,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        info!("Inserted auth profile: {}", profile.name);
        Ok(())
    }

    /// Updates an existing auth profile.
    pub fn update(&self, profile: &AuthProfileDto) -> Result<(), StorageError> {
        // Start transaction for atomic write
        let tx = self
            .conn()
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let rows_affected = tx
            .execute(
                r#"
                UPDATE auth_profiles SET
                    name = ?2,
                    provider_id = ?3,
                    enabled = ?4,
                    updated_at = datetime('now')
                WHERE id = ?1
                "#,
                params![
                    profile.id,
                    profile.name,
                    profile.provider_id,
                    profile.enabled as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            tx.rollback().ok();
            info!("No auth profile found to update: {}", profile.id);
            return Ok(());
        }

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        info!("Updated auth profile: {}", profile.name);
        Ok(())
    }

    /// Inserts a new auth profile from the core AuthProfile type.
    pub fn insert_auth_profile(
        &self,
        profile: &dbflux_core::AuthProfile,
    ) -> Result<(), StorageError> {
        let dto = AuthProfileDto {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            provider_id: profile.provider_id.clone(),
            enabled: profile.enabled,
            created_at: String::new(),
            updated_at: String::new(),
        };

        // Insert the profile
        self.insert(&dto)?;

        // Then write the fields to the child table
        let repo = self.fields_repo();
        for (key, value) in profile.fields.iter() {
            repo.insert(&AuthProfileFieldDto::new_text(
                profile.id.to_string(),
                key.clone(),
                value.clone(),
            ))?;
        }

        Ok(())
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
/// Note: fields are stored in auth_profile_fields child table.
/// The fields_json column was dropped in migration v10.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProfileDto {
    pub id: String,
    pub name: String,
    pub provider_id: String,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl AuthProfileDto {
    /// Creates a new DTO.
    pub fn new(id: Uuid, name: String, provider_id: String) -> Self {
        Self {
            id: id.to_string(),
            name,
            provider_id,
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

        let dto = AuthProfileDto::new(Uuid::new_v4(), "AWS SSO".to_string(), "aws-sso".to_string());

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
