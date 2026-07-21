//! Repository for the `sys_app_meta` key-value table.
//!
//! `sys_app_meta` stores one-time migration flags and other app-level metadata
//! that does not belong in user-facing config tables. Keys are plain strings;
//! values are stored as `TEXT`.
//!
//! The AWS config reflection migration uses the key
//! `aws_config_reflect_migrated` with value `"1"` as its idempotency guard.

use rusqlite::Connection;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

const META_PATH: &str = "dbflux.db";

/// Repository for `sys_app_meta` key-value metadata.
pub struct AppMetaRepository {
    conn: OwnedConnection,
}

impl AppMetaRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Returns the value associated with `key`, or `None` if the key is absent.
    pub fn get(&self, key: &str) -> Result<Option<String>, StorageError> {
        let result = self.conn().query_row(
            "SELECT value FROM sys_app_meta WHERE key = ?1",
            [key],
            |row| row.get::<_, String>(0),
        );

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(source) => Err(StorageError::Sqlite {
                path: META_PATH.into(),
                source,
            }),
        }
    }

    /// Returns `true` when `key` is present with the value `"1"`.
    ///
    /// This is the canonical check for boolean one-time migration flags.
    pub fn is_flag_set(&self, key: &str) -> Result<bool, StorageError> {
        Ok(self.get(key)?.as_deref() == Some("1"))
    }

    /// Inserts or updates `key` to `value`.
    pub fn set(&self, key: &str, value: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "INSERT INTO sys_app_meta (key, value, updated_at) \
                 VALUES (?1, ?2, datetime('now')) \
                 ON CONFLICT(key) DO UPDATE SET \
                     value = excluded.value, \
                     updated_at = excluded.updated_at",
                [key, value],
            )
            .map_err(|source| StorageError::Sqlite {
                path: META_PATH.into(),
                source,
            })?;

        Ok(())
    }

    /// Sets `key` to `"1"`, recording a boolean flag as present.
    pub fn set_flag(&self, key: &str) -> Result<(), StorageError> {
        self.set(key, "1")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("dbflux_meta_{}_{}", label, std::process::id()))
    }

    fn open_repo(label: &str) -> AppMetaRepository {
        let path = temp_path(label);
        let _ = std::fs::remove_file(&path);
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrations");
        #[allow(clippy::arc_with_non_send_sync)]
        AppMetaRepository::new(Arc::new(conn))
    }

    // T-4.1: absent marker → migration should run; present marker → skip.
    #[test]
    fn flag_absent_initially() {
        let repo = open_repo("flag_absent");
        assert!(
            !repo
                .is_flag_set("aws_config_reflect_migrated")
                .expect("query"),
            "flag must be absent on a fresh database"
        );
    }

    #[test]
    fn set_flag_makes_it_present() {
        let repo = open_repo("flag_set");
        repo.set_flag("aws_config_reflect_migrated").expect("set");
        assert!(
            repo.is_flag_set("aws_config_reflect_migrated")
                .expect("query"),
            "flag must be present after set_flag"
        );
    }

    #[test]
    fn set_flag_is_idempotent() {
        let repo = open_repo("flag_idempotent");
        repo.set_flag("aws_config_reflect_migrated")
            .expect("first set");
        repo.set_flag("aws_config_reflect_migrated")
            .expect("second set (idempotent)");
        assert!(
            repo.is_flag_set("aws_config_reflect_migrated")
                .expect("query"),
            "flag must still be present after double set"
        );
    }

    #[test]
    fn get_returns_none_for_unknown_key() {
        let repo = open_repo("flag_unknown");
        assert_eq!(repo.get("nonexistent_key").expect("query"), None);
    }

    #[test]
    fn set_and_get_arbitrary_value() {
        let repo = open_repo("flag_arbitrary");
        repo.set("some_key", "hello").expect("set");
        assert_eq!(
            repo.get("some_key").expect("query"),
            Some("hello".to_string())
        );
    }
}
