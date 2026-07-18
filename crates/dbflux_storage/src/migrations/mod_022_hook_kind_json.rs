use rusqlite::Transaction;

use super::{Migration, MigrationError};

pub struct MigrationImpl;

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{Migration, MigrationImpl};

    #[test]
    fn upgrade_adds_nullable_kind_json_without_rewriting_legacy_rows() {
        let mut conn = Connection::open_in_memory().expect("open in-memory database");
        conn.execute_batch(
            "CREATE TABLE cfg_hook_definitions (id TEXT PRIMARY KEY, name TEXT NOT NULL);
             INSERT INTO cfg_hook_definitions (id, name) VALUES ('legacy-id', 'legacy-name');",
        )
        .expect("create legacy hook table");

        let tx = conn.transaction().expect("start migration transaction");
        MigrationImpl.run(&tx).expect("apply hook-kind migration");
        tx.commit().expect("commit migration");

        let kind_json: Option<String> = conn
            .query_row(
                "SELECT kind_json FROM cfg_hook_definitions WHERE id = 'legacy-id'",
                [],
                |row| row.get(0),
            )
            .expect("read legacy row");

        assert_eq!(kind_json, None, "migration must not rewrite legacy rows");
    }
}

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "022_hook_kind_json"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'cfg_hook_definitions'",
            [],
            |row| row.get::<_, i64>(0),
        )? > 0;

        if !table_exists {
            return Ok(());
        }

        let column_exists: bool = tx.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('cfg_hook_definitions') WHERE name = 'kind_json'",
            [],
            |row| row.get::<_, i64>(0),
        )? > 0;

        if !column_exists {
            tx.execute_batch("ALTER TABLE cfg_hook_definitions ADD COLUMN kind_json TEXT NULL")?;
        }

        Ok(())
    }
}
