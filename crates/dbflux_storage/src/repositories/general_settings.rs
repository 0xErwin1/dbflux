//! Repository for general_settings table in config.db.
//!
//! This table stores the normalized general settings as native columns,
//! replacing the JSON blob previously stored in app_settings.

use log::info;
use rusqlite::{params, Connection};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing general settings.
pub struct GeneralSettingsRepository {
    conn: OwnedConnection,
}

impl GeneralSettingsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Gets the general settings row.
    pub fn get(&self) -> Result<Option<GeneralSettingsDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT id, theme, restore_session_on_startup, reopen_last_connections,
                       default_focus_on_startup, max_history_entries, auto_save_interval_ms,
                       default_refresh_policy, default_refresh_interval_secs,
                       max_concurrent_background_tasks, auto_refresh_pause_on_error,
                       auto_refresh_only_if_visible, confirm_dangerous_queries,
                       dangerous_requires_where, dangerous_requires_preview, updated_at
                FROM general_settings WHERE id = 1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let result = stmt.query_row([], |row| {
            Ok(GeneralSettingsDto {
                id: row.get(0)?,
                theme: row.get(1)?,
                restore_session_on_startup: row.get(2)?,
                reopen_last_connections: row.get(3)?,
                default_focus_on_startup: row.get(4)?,
                max_history_entries: row.get(5)?,
                auto_save_interval_ms: row.get(6)?,
                default_refresh_policy: row.get(7)?,
                default_refresh_interval_secs: row.get(8)?,
                max_concurrent_background_tasks: row.get(9)?,
                auto_refresh_pause_on_error: row.get(10)?,
                auto_refresh_only_if_visible: row.get(11)?,
                confirm_dangerous_queries: row.get(12)?,
                dangerous_requires_where: row.get(13)?,
                dangerous_requires_preview: row.get(14)?,
                updated_at: row.get(15)?,
            })
        });

        match result {
            Ok(dto) => Ok(Some(dto)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Upserts the general settings.
    pub fn upsert(&self, settings: &GeneralSettingsDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO general_settings (
                    id, theme, restore_session_on_startup, reopen_last_connections,
                    default_focus_on_startup, max_history_entries, auto_save_interval_ms,
                    default_refresh_policy, default_refresh_interval_secs,
                    max_concurrent_background_tasks, auto_refresh_pause_on_error,
                    auto_refresh_only_if_visible, confirm_dangerous_queries,
                    dangerous_requires_where, dangerous_requires_preview, updated_at
                ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                    theme = excluded.theme,
                    restore_session_on_startup = excluded.restore_session_on_startup,
                    reopen_last_connections = excluded.reopen_last_connections,
                    default_focus_on_startup = excluded.default_focus_on_startup,
                    max_history_entries = excluded.max_history_entries,
                    auto_save_interval_ms = excluded.auto_save_interval_ms,
                    default_refresh_policy = excluded.default_refresh_policy,
                    default_refresh_interval_secs = excluded.default_refresh_interval_secs,
                    max_concurrent_background_tasks = excluded.max_concurrent_background_tasks,
                    auto_refresh_pause_on_error = excluded.auto_refresh_pause_on_error,
                    auto_refresh_only_if_visible = excluded.auto_refresh_only_if_visible,
                    confirm_dangerous_queries = excluded.confirm_dangerous_queries,
                    dangerous_requires_where = excluded.dangerous_requires_where,
                    dangerous_requires_preview = excluded.dangerous_requires_preview,
                    updated_at = datetime('now')
                "#,
                params![
                    settings.theme,
                    settings.restore_session_on_startup,
                    settings.reopen_last_connections,
                    settings.default_focus_on_startup,
                    settings.max_history_entries,
                    settings.auto_save_interval_ms,
                    settings.default_refresh_policy,
                    settings.default_refresh_interval_secs,
                    settings.max_concurrent_background_tasks,
                    settings.auto_refresh_pause_on_error,
                    settings.auto_refresh_only_if_visible,
                    settings.confirm_dangerous_queries,
                    settings.dangerous_requires_where,
                    settings.dangerous_requires_preview,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        info!("Upserted general settings");
        Ok(())
    }
}

/// DTO for general_settings table.
#[derive(Debug, Clone)]
pub struct GeneralSettingsDto {
    pub id: i64,
    pub theme: String,
    pub restore_session_on_startup: i32,
    pub reopen_last_connections: i32,
    pub default_focus_on_startup: String,
    pub max_history_entries: i64,
    pub auto_save_interval_ms: i64,
    pub default_refresh_policy: String,
    pub default_refresh_interval_secs: i32,
    pub max_concurrent_background_tasks: i64,
    pub auto_refresh_pause_on_error: i32,
    pub auto_refresh_only_if_visible: i32,
    pub confirm_dangerous_queries: i32,
    pub dangerous_requires_where: i32,
    pub dangerous_requires_preview: i32,
    pub updated_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_repo_general_settings_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn upsert_and_get() {
        let path = temp_db("upsert_get");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = GeneralSettingsRepository::new(Arc::new(conn));

        let dto = GeneralSettingsDto {
            id: 1,
            theme: "light".to_string(),
            restore_session_on_startup: 0,
            reopen_last_connections: 1,
            default_focus_on_startup: "last_tab".to_string(),
            max_history_entries: 500,
            auto_save_interval_ms: 3000,
            default_refresh_policy: "interval".to_string(),
            default_refresh_interval_secs: 10,
            max_concurrent_background_tasks: 4,
            auto_refresh_pause_on_error: 0,
            auto_refresh_only_if_visible: 1,
            confirm_dangerous_queries: 0,
            dangerous_requires_where: 0,
            dangerous_requires_preview: 1,
            updated_at: String::new(),
        };

        repo.upsert(&dto).expect("should upsert");

        let fetched = repo.get().expect("should get").expect("should exist");
        assert_eq!(fetched.theme, "light");
        assert_eq!(fetched.restore_session_on_startup, 0);
        assert_eq!(fetched.max_history_entries, 500);

        let _ = std::fs::remove_file(&path);
    }
}
