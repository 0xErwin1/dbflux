//! Migration 037: `qry_saved_document_queries` for the document query builder.
//!
//! A saved document query is scoped to one collection: its connection
//! profile, database and collection name. The `DocumentQuerySpec` is stored
//! whole as JSON, tagged with `format_version`, so a later spec field reads
//! back from an older row through its serde default instead of a migration.
//! The SQL builder's `qry_saved_queries` tables are left untouched.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "037_qry_saved_document_queries"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(SCHEMA).map_err(sqlite_err)?;
        Ok(())
    }
}

fn sqlite_err(source: rusqlite::Error) -> MigrationError {
    MigrationError::Sqlite {
        path: std::path::PathBuf::from("<037_qry_saved_document_queries>"),
        source,
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS qry_saved_document_queries (
    id              TEXT    NOT NULL PRIMARY KEY,
    profile_id      TEXT    NOT NULL,
    database_name   TEXT    NOT NULL,
    collection_name TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    mode            TEXT    NOT NULL,
    format_version  INTEGER NOT NULL,
    spec_json       TEXT    NOT NULL,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    UNIQUE (profile_id, database_name, collection_name, name)
);

CREATE INDEX IF NOT EXISTS idx_qry_saved_document_queries_scope
    ON qry_saved_document_queries (profile_id, database_name, collection_name);
"#;
