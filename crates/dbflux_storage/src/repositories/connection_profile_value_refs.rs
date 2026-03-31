//! Repository for connection profile value references in dbflux.db.
//!
//! This module provides CRUD operations for the cfg_connection_profile_value_refs child table,
//! which stores value references (secrets, params, auth) for connection profiles.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Represents the kind of value reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefKind {
    Secret,
    Param,
    Auth,
}

impl RefKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            RefKind::Secret => "secret",
            RefKind::Param => "param",
            RefKind::Auth => "auth",
        }
    }

    pub fn try_parse(s: &str) -> Option<Self> {
        match s {
            "secret" => Some(RefKind::Secret),
            "param" => Some(RefKind::Param),
            "auth" => Some(RefKind::Auth),
            _ => None,
        }
    }
}

/// Repository for managing connection profile value references.
/// This is always used behind a ConnectionProfileRepository.
pub struct ConnectionProfileValueRefsRepository {
    conn: OwnedConnection,
}

impl ConnectionProfileValueRefsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Fetches all value refs for a connection profile.
    pub fn get_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<ConnectionProfileValueRefDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, profile_id, ref_key, ref_kind, ref_value, ref_provider, ref_json_key,
                       literal_value, env_key, secret_locator, param_name, auth_field
                FROM cfg_connection_profile_value_refs
                WHERE profile_id = ?1
                ORDER BY ref_key ASC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let refs = stmt
            .query_map([profile_id], |row| {
                Ok(ConnectionProfileValueRefDto {
                    id: row.get(0)?,
                    profile_id: row.get(1)?,
                    ref_key: row.get(2)?,
                    ref_kind: row.get(3)?,
                    ref_value: row.get(4)?,
                    ref_provider: row.get(5)?,
                    ref_json_key: row.get(6)?,
                    literal_value: row.get(7)?,
                    env_key: row.get(8)?,
                    secret_locator: row.get(9)?,
                    param_name: row.get(10)?,
                    auth_field: row.get(11)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for r in refs {
            match r {
                Ok(r) => result.push(r),
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            });
        }

        Ok(result)
    }

    /// Inserts a single value ref.
    pub fn insert(&self, value_ref: &ConnectionProfileValueRefDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_profile_value_refs (
                    id, profile_id, ref_key, ref_kind, ref_value, ref_provider, ref_json_key,
                    literal_value, env_key, secret_locator, param_name, auth_field
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                "#,
                params![
                    value_ref.id,
                    value_ref.profile_id,
                    value_ref.ref_key,
                    value_ref.ref_kind,
                    value_ref.ref_value,
                    value_ref.ref_provider,
                    value_ref.ref_json_key,
                    value_ref.literal_value,
                    value_ref.env_key,
                    value_ref.secret_locator,
                    value_ref.param_name,
                    value_ref.auth_field,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Upserts a value ref (insert or update by profile_id + ref_key).
    pub fn upsert(&self, value_ref: &ConnectionProfileValueRefDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_profile_value_refs (
                    id, profile_id, ref_key, ref_kind, ref_value, ref_provider, ref_json_key,
                    literal_value, env_key, secret_locator, param_name, auth_field
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                ON CONFLICT(profile_id, ref_key) DO UPDATE SET
                    ref_kind = excluded.ref_kind,
                    ref_value = excluded.ref_value,
                    ref_provider = excluded.ref_provider,
                    ref_json_key = excluded.ref_json_key,
                    literal_value = excluded.literal_value,
                    env_key = excluded.env_key,
                    secret_locator = excluded.secret_locator,
                    param_name = excluded.param_name,
                    auth_field = excluded.auth_field
                "#,
                params![
                    value_ref.id,
                    value_ref.profile_id,
                    value_ref.ref_key,
                    value_ref.ref_kind,
                    value_ref.ref_value,
                    value_ref.ref_provider,
                    value_ref.ref_json_key,
                    value_ref.literal_value,
                    value_ref.env_key,
                    value_ref.secret_locator,
                    value_ref.param_name,
                    value_ref.auth_field,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        info!(
            "Upserted connection profile value ref: {} for profile: {}",
            value_ref.ref_key, value_ref.profile_id
        );
        Ok(())
    }

    /// Deletes all value refs for a connection profile.
    pub fn delete_for_profile(&self, profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM cfg_connection_profile_value_refs WHERE profile_id = ?1",
                [profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Replaces all value refs for a profile (delete old, insert new).
    pub fn replace_for_profile(
        &self,
        profile_id: &str,
        refs: &[ConnectionProfileValueRefDto],
    ) -> Result<(), StorageError> {
        self.delete_for_profile(profile_id)?;
        for r in refs {
            self.insert(r)?;
        }
        Ok(())
    }
}

/// DTO for connection profile value references (child table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileValueRefDto {
    pub id: String,
    pub profile_id: String,
    pub ref_key: String,
    pub ref_kind: String,
    /// Legacy scalar value (use native variant columns instead).
    /// Native columns: literal_value, env_key, secret_locator, param_name, auth_field.
    pub ref_value: String,
    /// Provider ID for Secret and Parameter variants.
    pub ref_provider: Option<String>,
    /// Optional JSON key for Secret and Parameter variants.
    pub ref_json_key: Option<String>,
    /// Native columns for ValueRef variants.
    pub literal_value: Option<String>,
    pub env_key: Option<String>,
    pub secret_locator: Option<String>,
    pub param_name: Option<String>,
    pub auth_field: Option<String>,
}

impl ConnectionProfileValueRefDto {
    pub fn new_literal(profile_id: String, ref_key: String, value: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            ref_key,
            ref_kind: "literal".to_string(),
            ref_value: value.clone(),
            ref_provider: None,
            ref_json_key: None,
            literal_value: Some(value),
            env_key: None,
            secret_locator: None,
            param_name: None,
            auth_field: None,
        }
    }

    pub fn new_env(profile_id: String, ref_key: String, env_key: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            ref_key,
            ref_kind: "env".to_string(),
            ref_value: env_key.clone(),
            ref_provider: None,
            ref_json_key: None,
            literal_value: None,
            env_key: Some(env_key),
            secret_locator: None,
            param_name: None,
            auth_field: None,
        }
    }

    pub fn new_secret(
        profile_id: String,
        ref_key: String,
        provider: String,
        locator: String,
        json_key: Option<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            ref_key,
            ref_kind: "secret".to_string(),
            ref_value: locator.clone(),
            ref_provider: Some(provider.clone()),
            ref_json_key: json_key.clone(),
            literal_value: None,
            env_key: None,
            secret_locator: Some(locator),
            param_name: None,
            auth_field: None,
        }
    }

    pub fn new_param(
        profile_id: String,
        ref_key: String,
        provider: String,
        name: String,
        json_key: Option<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            ref_key,
            ref_kind: "param".to_string(),
            ref_value: name.clone(),
            ref_provider: Some(provider.clone()),
            ref_json_key: json_key.clone(),
            literal_value: None,
            env_key: None,
            secret_locator: None,
            param_name: Some(name),
            auth_field: None,
        }
    }

    pub fn new_auth(profile_id: String, ref_key: String, field: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            ref_key,
            ref_kind: "auth".to_string(),
            ref_value: field.clone(),
            ref_provider: None,
            ref_json_key: None,
            literal_value: None,
            env_key: None,
            secret_locator: None,
            param_name: None,
            auth_field: Some(field),
        }
    }
}
