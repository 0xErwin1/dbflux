//! Migration 017: `qry_*` tables for the visual query builder.
//!
//! Creates four tables that persist `VisualQuerySpec` as a mix of native
//! columns (stable, indexed, queryable) and a single JSON column for the
//! recursive filter tree (genuinely dynamic, bounded by `serde` derives).
//!
//! Table prefix `qry_` follows the project domain-prefix convention.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "017_qry_saved_queries"
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
CREATE TABLE qry_saved_queries (
    id              TEXT    NOT NULL PRIMARY KEY,
    profile_id      TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    schema_name     TEXT,
    table_name      TEXT    NOT NULL,
    source_alias    TEXT    NOT NULL,
    projection_mode TEXT    NOT NULL,
    limit_value     INTEGER,
    offset_value    INTEGER NOT NULL DEFAULT 0,
    filter_json     TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    UNIQUE (profile_id, name)
);

CREATE INDEX idx_qry_saved_queries_profile ON qry_saved_queries (profile_id);

CREATE TABLE qry_saved_query_columns (
    saved_query_id TEXT    NOT NULL,
    position       INTEGER NOT NULL,
    source_alias   TEXT    NOT NULL,
    column_name    TEXT    NOT NULL,
    alias          TEXT,
    PRIMARY KEY (saved_query_id, position),
    FOREIGN KEY (saved_query_id) REFERENCES qry_saved_queries(id) ON DELETE CASCADE
);

CREATE TABLE qry_saved_query_sorts (
    saved_query_id TEXT    NOT NULL,
    position       INTEGER NOT NULL,
    source_alias   TEXT    NOT NULL,
    column_name    TEXT    NOT NULL,
    direction      TEXT    NOT NULL,
    PRIMARY KEY (saved_query_id, position),
    FOREIGN KEY (saved_query_id) REFERENCES qry_saved_queries(id) ON DELETE CASCADE
);

CREATE TABLE qry_saved_query_joins (
    saved_query_id TEXT    NOT NULL,
    position       INTEGER NOT NULL,
    join_kind      TEXT    NOT NULL,
    from_alias     TEXT    NOT NULL,
    to_schema      TEXT,
    to_table       TEXT    NOT NULL,
    to_alias       TEXT    NOT NULL,
    on_mode        TEXT    NOT NULL,
    from_column    TEXT,
    to_column      TEXT,
    raw_expression TEXT,
    PRIMARY KEY (saved_query_id, position),
    FOREIGN KEY (saved_query_id) REFERENCES qry_saved_queries(id) ON DELETE CASCADE
);
"#;
