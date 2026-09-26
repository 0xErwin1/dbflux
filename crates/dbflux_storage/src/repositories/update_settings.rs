//! Repository for the `cfg_update_settings` singleton row in dbflux.db.
//!
//! Holds the update-check and changelog preferences together with the
//! first-run bookkeeping (last run version, skipped release, welcome shown).

use rusqlite::{Connection, params};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing update settings.
pub struct UpdateSettingsRepository {
    conn: OwnedConnection,
}

impl UpdateSettingsRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    fn sqlite_error(source: rusqlite::Error) -> StorageError {
        StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        }
    }

    /// Reads the singleton row, or `None` when it has never been written.
    pub fn get(&self) -> Result<Option<UpdateSettingsDto>, StorageError> {
        let result = self.conn().query_row(
            r#"
            SELECT check_for_updates_on_startup, show_whats_new_after_update,
                   last_run_version, skipped_update_version, welcome_shown
            FROM cfg_update_settings WHERE id = 1
            "#,
            [],
            |row| {
                Ok(UpdateSettingsDto {
                    check_for_updates_on_startup: row.get(0)?,
                    show_whats_new_after_update: row.get(1)?,
                    last_run_version: row.get(2)?,
                    skipped_update_version: row.get(3)?,
                    welcome_shown: row.get(4)?,
                })
            },
        );

        match result {
            Ok(dto) => Ok(Some(dto)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(source) => Err(Self::sqlite_error(source)),
        }
    }

    /// Inserts or replaces the singleton row.
    pub fn upsert(&self, settings: &UpdateSettingsDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_update_settings (
                    id, check_for_updates_on_startup, show_whats_new_after_update,
                    last_run_version, skipped_update_version, welcome_shown, updated_at
                ) VALUES (1, ?1, ?2, ?3, ?4, ?5, datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                    check_for_updates_on_startup = excluded.check_for_updates_on_startup,
                    show_whats_new_after_update = excluded.show_whats_new_after_update,
                    last_run_version = excluded.last_run_version,
                    skipped_update_version = excluded.skipped_update_version,
                    welcome_shown = excluded.welcome_shown,
                    updated_at = datetime('now')
                "#,
                params![
                    settings.check_for_updates_on_startup,
                    settings.show_whats_new_after_update,
                    settings.last_run_version,
                    settings.skipped_update_version,
                    settings.welcome_shown,
                ],
            )
            .map_err(Self::sqlite_error)?;

        Ok(())
    }
}

/// Row shape of `cfg_update_settings`. Booleans are stored as 0/1 integers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateSettingsDto {
    pub check_for_updates_on_startup: i32,
    pub show_whats_new_after_update: i32,
    pub last_run_version: Option<String>,
    pub skipped_update_version: Option<String>,
    pub welcome_shown: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn migrated_repository(directory: &tempfile::TempDir) -> UpdateSettingsRepository {
        let conn = open_database(&directory.path().join("dbflux.db")).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migrations should run");

        #[allow(clippy::arc_with_non_send_sync)]
        UpdateSettingsRepository::new(Arc::new(conn))
    }

    #[test]
    fn migration_seeds_the_defaults() {
        let directory = tempfile::tempdir().expect("temp dir");
        let repository = migrated_repository(&directory);

        let stored = repository
            .get()
            .expect("should read")
            .expect("seeded row exists");

        assert_eq!(
            stored,
            UpdateSettingsDto {
                check_for_updates_on_startup: 1,
                show_whats_new_after_update: 1,
                last_run_version: None,
                skipped_update_version: None,
                welcome_shown: 0,
            }
        );
    }

    #[test]
    fn upsert_round_trips_every_column() {
        let directory = tempfile::tempdir().expect("temp dir");
        let repository = migrated_repository(&directory);

        let written = UpdateSettingsDto {
            check_for_updates_on_startup: 0,
            show_whats_new_after_update: 0,
            last_run_version: Some("0.8.0".to_string()),
            skipped_update_version: Some("0.8.1".to_string()),
            welcome_shown: 1,
        };
        repository.upsert(&written).expect("should upsert");

        assert_eq!(repository.get().expect("should read"), Some(written));

        let cleared = UpdateSettingsDto {
            check_for_updates_on_startup: 1,
            show_whats_new_after_update: 1,
            last_run_version: Some("0.8.1".to_string()),
            skipped_update_version: None,
            welcome_shown: 1,
        };
        repository.upsert(&cleared).expect("should upsert again");

        assert_eq!(repository.get().expect("should read"), Some(cleared));
    }
}
