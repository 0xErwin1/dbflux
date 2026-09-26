use rusqlite::Transaction;

use super::{Migration, MigrationError, is_preexisting_database};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "032_cfg_update_settings"
    }

    /// Creates the update settings row. An existing install has already been
    /// used, so it starts with the welcome dialog marked as shown; a fresh
    /// install gets the welcome dialog on its first start.
    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let map_error = |source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<032_cfg_update_settings>"),
            source,
        };

        tx.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cfg_update_settings (
                id                           INTEGER PRIMARY KEY CHECK (id = 1),
                check_for_updates_on_startup INTEGER NOT NULL DEFAULT 1,
                show_whats_new_after_update  INTEGER NOT NULL DEFAULT 1,
                last_run_version             TEXT,
                skipped_update_version       TEXT,
                welcome_shown                INTEGER NOT NULL DEFAULT 0,
                updated_at                   TEXT    NOT NULL DEFAULT (datetime('now'))
            );
            ",
        )
        .map_err(map_error)?;

        let welcome_shown = is_preexisting_database(tx).map_err(map_error)?;

        tx.execute(
            "INSERT OR IGNORE INTO cfg_update_settings (id, welcome_shown) VALUES (1, ?1)",
            rusqlite::params![i32::from(welcome_shown)],
        )
        .map_err(map_error)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;

    fn welcome_shown(conn: &rusqlite::Connection) -> i64 {
        conn.query_row(
            "SELECT welcome_shown FROM cfg_update_settings WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .expect("update settings row")
    }

    #[test]
    fn fresh_install_keeps_the_welcome_dialog() {
        let directory = tempfile::tempdir().expect("temp dir");
        let conn = open_database(&directory.path().join("dbflux.db")).expect("open");

        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migrations run");

        assert_eq!(welcome_shown(&conn), 0);
    }

    #[test]
    fn existing_install_skips_the_welcome_dialog() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("dbflux.db");

        {
            let conn = open_database(&path).expect("open");
            let mut previous_release = MigrationRegistry::new();
            previous_release
                .migrations
                .retain(|migration| migration.name() != "032_cfg_update_settings");
            previous_release
                .run_all(&conn)
                .expect("previous migrations run");
        }

        let conn = open_database(&path).expect("reopen");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("upgrade migrations run");

        assert_eq!(welcome_shown(&conn), 1);
    }
}
