//! Migration 020: `sch_*` tables for persisted schema snapshots.
//!
//! Creates two tables that persist `SchemaSnapshotRecord` from `dbflux_core`:
//! - `sch_schema_snapshots` — root row with native scalar columns (identity,
//!   fingerprint, depth) used for list/prune queries.
//! - `sch_snapshot_tables` — one row per captured `TableInfo`, keyed by
//!   `(schema, name)` for identity with `detail_json` holding the full
//!   per-driver structure (genuinely dynamic, bounded by `serde` derives).
//!
//! Table prefix `sch_` follows the project domain-prefix convention.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "020_sch_schema_snapshots"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(SCHEMA).map_err(sqlite_err)?;
        Ok(())
    }
}

fn sqlite_err(source: rusqlite::Error) -> MigrationError {
    MigrationError::Sqlite {
        path: std::path::PathBuf::from("<unknown>"),
        source,
    }
}

const SCHEMA: &str = r#"
CREATE TABLE sch_schema_snapshots (
    id           TEXT    NOT NULL PRIMARY KEY,
    profile_id   TEXT    NOT NULL,
    database     TEXT,
    captured_at  INTEGER NOT NULL,
    fingerprint  TEXT    NOT NULL,
    depth        TEXT    NOT NULL
);

CREATE INDEX idx_sch_schema_snapshots_profile_db
    ON sch_schema_snapshots (profile_id, database, captured_at DESC);

CREATE TABLE sch_snapshot_tables (
    snapshot_id  TEXT NOT NULL,
    schema_name  TEXT,
    name         TEXT NOT NULL,
    detail_json  TEXT NOT NULL,
    FOREIGN KEY (snapshot_id) REFERENCES sch_schema_snapshots(id) ON DELETE CASCADE
);

CREATE INDEX idx_sch_snapshot_tables_snapshot
    ON sch_snapshot_tables (snapshot_id);
"#;
