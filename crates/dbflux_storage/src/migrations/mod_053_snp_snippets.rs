//! Migration 053: `snp_snippets` for user code snippets.
//!
//! A snippet belongs to one query language, stored as a stable lowercase id
//! (`"sql"` first) rather than the `QueryLanguage` enum, and its prefix is
//! unique within that language. The migration also seeds the built-in SQL
//! snippets. They are inserted exactly once here, so a built-in the user
//! deletes is never recreated on a later start.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub(crate) struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "053_snp_snippets"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(SCHEMA).map_err(sqlite_err)?;

        for built_in in BUILT_INS {
            tx.execute(
                "INSERT INTO snp_snippets
                     (id, name, prefix, language, body, builtin_key, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 'sql', ?4, ?5,
                         CAST(strftime('%s', 'now') AS INTEGER) * 1000,
                         CAST(strftime('%s', 'now') AS INTEGER) * 1000)",
                rusqlite::params![
                    built_in.id,
                    built_in.name,
                    built_in.prefix,
                    built_in.body,
                    built_in.builtin_key
                ],
            )
            .map_err(sqlite_err)?;
        }

        Ok(())
    }
}

fn sqlite_err(source: rusqlite::Error) -> MigrationError {
    MigrationError::Sqlite {
        path: std::path::PathBuf::from("<053_snp_snippets>"),
        source,
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS snp_snippets (
    id          TEXT    NOT NULL PRIMARY KEY,
    name        TEXT    NOT NULL,
    prefix      TEXT    NOT NULL,
    language    TEXT    NOT NULL,
    body        TEXT    NOT NULL,
    builtin_key TEXT,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    UNIQUE (language, prefix)
);

CREATE INDEX IF NOT EXISTS idx_snp_snippets_language
    ON snp_snippets (language);
"#;

struct BuiltIn {
    id: &'static str,
    builtin_key: &'static str,
    prefix: &'static str,
    name: &'static str,
    body: &'static str,
}

const BUILT_INS: [BuiltIn; 6] = [
    BuiltIn {
        id: "0196a000-0000-7000-8000-000000000001",
        builtin_key: "sql.select",
        prefix: "sel",
        name: "Select rows",
        body: "SELECT ${2:*}\nFROM ${1:table}\nWHERE ${3:condition};$0",
    },
    BuiltIn {
        id: "0196a000-0000-7000-8000-000000000002",
        builtin_key: "sql.insert",
        prefix: "ins",
        name: "Insert a row",
        body: "INSERT INTO ${1:table} (${2:columns})\nVALUES (${3:values});$0",
    },
    BuiltIn {
        id: "0196a000-0000-7000-8000-000000000003",
        builtin_key: "sql.update",
        prefix: "upd",
        name: "Update rows",
        body: "UPDATE ${1:table}\nSET ${2:column} = ${3:value}\nWHERE ${4:condition};$0",
    },
    BuiltIn {
        id: "0196a000-0000-7000-8000-000000000004",
        builtin_key: "sql.delete",
        prefix: "del",
        name: "Delete rows",
        body: "DELETE FROM ${1:table}\nWHERE ${2:condition};$0",
    },
    BuiltIn {
        id: "0196a000-0000-7000-8000-000000000005",
        builtin_key: "sql.cte",
        prefix: "cte",
        name: "Common table expression",
        body: "WITH ${1:name} AS (\n  ${2:SELECT 1}\n)\nSELECT *\nFROM $1;$0",
    },
    BuiltIn {
        id: "0196a000-0000-7000-8000-000000000006",
        builtin_key: "sql.count",
        prefix: "cnt",
        name: "Count rows",
        body: "SELECT COUNT(*)\nFROM ${1:table};$0",
    },
];
