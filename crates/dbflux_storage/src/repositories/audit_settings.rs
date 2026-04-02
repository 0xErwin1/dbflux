//! Repository for cfg_audit_settings table in dbflux.db.
//!
//! This table stores the audit system configuration including
//! retention policy, capture settings, and purge configuration.

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing audit settings.
pub struct AuditSettingsRepository {
    conn: OwnedConnection,
}

impl AuditSettingsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Gets the audit settings row.
    pub fn get(&self) -> Result<Option<AuditSettingsDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, enabled, retention_days, capture_user_actions,
                       capture_system_events, capture_query_text, capture_hook_output_metadata,
                       redact_sensitive_values, max_detail_bytes, purge_on_startup,
                       background_purge_interval_minutes, updated_at
                FROM cfg_audit_settings WHERE id = 1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let result = stmt.query_row([], |row| {
            Ok(AuditSettingsDto {
                id: row.get(0)?,
                enabled: row.get::<_, i32>(1)? != 0,
                retention_days: row.get::<_, i32>(2)? as u32,
                capture_user_actions: row.get::<_, i32>(3)? != 0,
                capture_system_events: row.get::<_, i32>(4)? != 0,
                capture_query_text: row.get::<_, i32>(5)? != 0,
                capture_hook_output_metadata: row.get::<_, i32>(6)? != 0,
                redact_sensitive_values: row.get::<_, i32>(7)? != 0,
                max_detail_bytes: row.get::<_, i32>(8)? as usize,
                purge_on_startup: row.get::<_, i32>(9)? != 0,
                background_purge_interval_minutes: row.get::<_, i32>(10)? as u32,
                updated_at: row.get(11)?,
            })
        });

        match result {
            Ok(dto) => Ok(Some(dto)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            }),
        }
    }

    /// Upserts the audit settings.
    pub fn upsert(&self, settings: &AuditSettingsDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_audit_settings (
                    id, enabled, retention_days, capture_user_actions,
                    capture_system_events, capture_query_text, capture_hook_output_metadata,
                    redact_sensitive_values, max_detail_bytes, purge_on_startup,
                    background_purge_interval_minutes, updated_at
                ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                    enabled = excluded.enabled,
                    retention_days = excluded.retention_days,
                    capture_user_actions = excluded.capture_user_actions,
                    capture_system_events = excluded.capture_system_events,
                    capture_query_text = excluded.capture_query_text,
                    capture_hook_output_metadata = excluded.capture_hook_output_metadata,
                    redact_sensitive_values = excluded.redact_sensitive_values,
                    max_detail_bytes = excluded.max_detail_bytes,
                    purge_on_startup = excluded.purge_on_startup,
                    background_purge_interval_minutes = excluded.background_purge_interval_minutes,
                    updated_at = datetime('now')
                "#,
                rusqlite::params![
                    settings.enabled as i32,
                    settings.retention_days as i32,
                    settings.capture_user_actions as i32,
                    settings.capture_system_events as i32,
                    settings.capture_query_text as i32,
                    settings.capture_hook_output_metadata as i32,
                    settings.redact_sensitive_values as i32,
                    settings.max_detail_bytes as i32,
                    settings.purge_on_startup as i32,
                    settings.background_purge_interval_minutes as i32,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }
}

/// DTO for cfg_audit_settings table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditSettingsDto {
    pub id: i64,
    pub enabled: bool,
    pub retention_days: u32,
    pub capture_user_actions: bool,
    pub capture_system_events: bool,
    pub capture_query_text: bool,
    pub capture_hook_output_metadata: bool,
    pub redact_sensitive_values: bool,
    pub max_detail_bytes: usize,
    pub purge_on_startup: bool,
    pub background_purge_interval_minutes: u32,
    pub updated_at: String,
}

impl Default for AuditSettingsDto {
    fn default() -> Self {
        Self {
            id: 1,
            enabled: true,
            retention_days: 30,
            capture_user_actions: true,
            capture_system_events: true,
            capture_query_text: false,
            capture_hook_output_metadata: true,
            redact_sensitive_values: true,
            max_detail_bytes: 65536,
            purge_on_startup: false,
            background_purge_interval_minutes: 360,
            updated_at: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_audit_settings_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn upsert_and_get_audit_settings() {
        let path = temp_db("upsert_get");
        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        let repo = AuditSettingsRepository::new(Arc::new(conn));

        let dto = AuditSettingsDto::default();
        repo.upsert(&dto).expect("should upsert audit settings");

        let fetched = repo.get().expect("should get").expect("should exist");
        assert_eq!(fetched.enabled, true);
        assert_eq!(fetched.retention_days, 30);
        assert_eq!(fetched.capture_query_text, false);
        assert_eq!(fetched.redact_sensitive_values, true);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn update_audit_settings() {
        let path = temp_db("update");
        let conn = open_database(&path).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migration should run");

        let repo = AuditSettingsRepository::new(Arc::new(conn));

        let mut dto = AuditSettingsDto::default();
        dto.retention_days = 60;
        dto.capture_query_text = true;
        dto.background_purge_interval_minutes = 720;

        repo.upsert(&dto).expect("should upsert");

        let fetched = repo.get().expect("should get").expect("should exist");
        assert_eq!(fetched.retention_days, 60);
        assert_eq!(fetched.capture_query_text, true);
        assert_eq!(fetched.background_purge_interval_minutes, 720);

        let _ = std::fs::remove_file(&path);
    }
}
