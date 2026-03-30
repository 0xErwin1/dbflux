//! Repository for auth profile field values in config.db.
//!
//! This module provides CRUD operations for the auth_profile_fields child table,
//! which stores typed EAV (Entity-Attribute-Value) field values for auth profiles.

use log::info;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Represents the kind of value stored in an auth profile field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldValueKind {
    Text,
    Bool,
    Number,
    Secret,
}

impl FieldValueKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            FieldValueKind::Text => "text",
            FieldValueKind::Bool => "bool",
            FieldValueKind::Number => "number",
            FieldValueKind::Secret => "secret",
        }
    }

    pub fn try_parse(s: &str) -> Option<Self> {
        match s {
            "text" => Some(FieldValueKind::Text),
            "bool" => Some(FieldValueKind::Bool),
            "number" => Some(FieldValueKind::Number),
            "secret" => Some(FieldValueKind::Secret),
            _ => None,
        }
    }
}

/// Repository for managing auth profile field values.
/// This is always used behind an AuthProfileRepository.
pub struct AuthProfileFieldsRepository {
    conn: OwnedConnection,
}

impl AuthProfileFieldsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all fields for an auth profile.
    pub fn get_for_profile(
        &self,
        auth_profile_id: &str,
    ) -> Result<Vec<AuthProfileFieldDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, auth_profile_id, field_key, value_text, value_bool,
                       value_number, value_secret_ref, value_kind
                FROM auth_profile_fields
                WHERE auth_profile_id = ?1
                ORDER BY field_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let fields = stmt
            .query_map([auth_profile_id], |row| {
                Ok(AuthProfileFieldDto {
                    id: row.get(0)?,
                    auth_profile_id: row.get(1)?,
                    field_key: row.get(2)?,
                    value_text: row.get(3)?,
                    value_bool: row.get(4)?,
                    value_number: row.get(5)?,
                    value_secret_ref: row.get(6)?,
                    value_kind: row.get(7)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for field in fields {
            match field {
                Ok(f) => result.push(f),
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

    /// Inserts a single field value.
    pub fn insert(&self, field: &AuthProfileFieldDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO auth_profile_fields (
                    id, auth_profile_id, field_key, value_text,
                    value_bool, value_number, value_secret_ref, value_kind
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    field.id,
                    field.auth_profile_id,
                    field.field_key,
                    field.value_text,
                    field.value_bool,
                    field.value_number,
                    field.value_secret_ref,
                    field.value_kind,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Upserts a field value (insert or update by profile_id + field_key).
    pub fn upsert(&self, field: &AuthProfileFieldDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO auth_profile_fields (
                    id, auth_profile_id, field_key, value_text,
                    value_bool, value_number, value_secret_ref, value_kind
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ON CONFLICT(auth_profile_id, field_key) DO UPDATE SET
                    value_text = excluded.value_text,
                    value_bool = excluded.value_bool,
                    value_number = excluded.value_number,
                    value_secret_ref = excluded.value_secret_ref,
                    value_kind = excluded.value_kind
                "#,
                params![
                    field.id,
                    field.auth_profile_id,
                    field.field_key,
                    field.value_text,
                    field.value_bool,
                    field.value_number,
                    field.value_secret_ref,
                    field.value_kind,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!(
            "Upserted auth profile field: {} for profile: {}",
            field.field_key, field.auth_profile_id
        );
        Ok(())
    }

    /// Deletes all fields for an auth profile.
    pub fn delete_for_profile(&self, auth_profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM auth_profile_fields WHERE auth_profile_id = ?1",
                [auth_profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }
}

/// DTO for auth profile field values (child table with EAV pattern).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProfileFieldDto {
    pub id: String,
    pub auth_profile_id: String,
    pub field_key: String,
    pub value_text: Option<String>,
    pub value_bool: Option<i32>,
    pub value_number: Option<f64>,
    pub value_secret_ref: Option<String>,
    pub value_kind: String,
}

impl AuthProfileFieldDto {
    /// Creates a new text field.
    pub fn new_text(auth_profile_id: String, field_key: String, value: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            auth_profile_id,
            field_key,
            value_text: Some(value),
            value_bool: None,
            value_number: None,
            value_secret_ref: None,
            value_kind: "text".to_string(),
        }
    }

    /// Creates a new boolean field.
    pub fn new_bool(auth_profile_id: String, field_key: String, value: bool) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            auth_profile_id,
            field_key,
            value_text: None,
            value_bool: Some(if value { 1 } else { 0 }),
            value_number: None,
            value_secret_ref: None,
            value_kind: "bool".to_string(),
        }
    }

    /// Creates a new number field.
    pub fn new_number(auth_profile_id: String, field_key: String, value: f64) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            auth_profile_id,
            field_key,
            value_text: None,
            value_bool: None,
            value_number: Some(value),
            value_secret_ref: None,
            value_kind: "number".to_string(),
        }
    }

    /// Creates a new secret field.
    pub fn new_secret(auth_profile_id: String, field_key: String, secret_ref: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            auth_profile_id,
            field_key,
            value_text: None,
            value_bool: None,
            value_number: None,
            value_secret_ref: Some(secret_ref),
            value_kind: "secret".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::repositories::auth_profiles::{AuthProfileDto, AuthProfileRepository};
    use crate::sqlite::open_database;
    use std::sync::Arc;
    use uuid::Uuid;

    fn temp_db(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_repo_auth_fields_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn auth_profile_fields_insert_and_fetch() {
        let path = temp_db("auth_fields_insert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        // First create the parent profile
        let _profile_id = Uuid::new_v4().to_string();
        let profile = AuthProfileDto::new(
            Uuid::new_v4(),
            "Test Profile".to_string(),
            "aws-sso".to_string(),
        );
        let conn_arc = Arc::new(conn);
        let profile_repo = AuthProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile)
            .expect("should insert profile");

        let repo = AuthProfileFieldsRepository::new(conn_arc);

        // Insert various field types
        repo.insert(&AuthProfileFieldDto::new_text(
            profile.id.clone(),
            "sso_start_url".to_string(),
            "https://example.awsapps.com".to_string(),
        ))
        .expect("should insert text field");

        repo.insert(&AuthProfileFieldDto::new_bool(
            profile.id.clone(),
            "sso_auto_refresh".to_string(),
            true,
        ))
        .expect("should insert bool field");

        let fetched = repo.get_for_profile(&profile.id).expect("should fetch");
        assert_eq!(fetched.len(), 2);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn auth_profile_fields_upsert() {
        let path = temp_db("auth_fields_upsert");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let profile = AuthProfileDto::new(
            Uuid::new_v4(),
            "Test Profile".to_string(),
            "aws-sso".to_string(),
        );
        let conn_arc = Arc::new(conn);
        let profile_repo = AuthProfileRepository::new(conn_arc.clone());
        profile_repo
            .insert(&profile)
            .expect("should insert profile");

        let repo = AuthProfileFieldsRepository::new(conn_arc);

        // Insert initial value
        repo.insert(&AuthProfileFieldDto::new_text(
            profile.id.clone(),
            "sso_start_url".to_string(),
            "https://old.example.com".to_string(),
        ))
        .expect("should insert");

        // Upsert with new value
        repo.upsert(&AuthProfileFieldDto::new_text(
            profile.id.clone(),
            "sso_start_url".to_string(),
            "https://new.example.com".to_string(),
        ))
        .expect("should upsert");

        let fetched = repo.get_for_profile(&profile.id).expect("should fetch");
        assert_eq!(fetched.len(), 1);
        assert_eq!(
            fetched[0].value_text.as_deref(),
            Some("https://new.example.com")
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
