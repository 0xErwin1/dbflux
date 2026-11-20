//! Keep schema history identical with and without the optional telemetry runtime.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub(crate) struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "050_cfg_telemetry"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        // Identity generation belongs to transactional initialization, not DDL.
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS cfg_telemetry (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                revision INTEGER NOT NULL CHECK (revision >= 1),
                state_json TEXT NOT NULL
            );",
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::migrations::MigrationRegistry;

    #[test]
    fn telemetry_schema_is_unconditional_and_does_not_create_identity() {
        let connection = rusqlite::Connection::open_in_memory().expect("scratch database");
        let registry = MigrationRegistry::new();
        registry.run_all(&connection).expect("migrate");
        registry.run_all(&connection).expect("idempotent migration");
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM cfg_telemetry", [], |row| row.get(0))
            .expect("table exists");
        assert_eq!(count, 0);
        let applied: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sys_migrations WHERE name = '050_cfg_telemetry'",
                [],
                |row| row.get(0),
            )
            .expect("migration history");
        assert_eq!(applied, 1);
    }
}
