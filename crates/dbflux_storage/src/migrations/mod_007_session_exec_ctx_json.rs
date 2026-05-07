//! Migration 007: add JSON execution context to session tabs.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "007_session_exec_ctx_json"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        if !has_table(tx, "st_session_tabs")? {
            return Ok(());
        }

        if !has_column(tx, "exec_ctx_json")? {
            tx.execute(
                "ALTER TABLE st_session_tabs ADD COLUMN exec_ctx_json TEXT",
                [],
            )
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;
        }

        backfill_exec_ctx_json(tx)?;

        Ok(())
    }
}

fn has_table(tx: &Transaction, table_name: &str) -> Result<bool, MigrationError> {
    let mut stmt = tx
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1")
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    let mut rows = stmt
        .query([table_name])
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    Ok(rows
        .next()
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?
        .is_some())
}

fn has_column(tx: &Transaction, column_name: &str) -> Result<bool, MigrationError> {
    let mut stmt = tx
        .prepare("PRAGMA table_info(st_session_tabs)")
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    for column in columns {
        let column = column.map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        if column == column_name {
            return Ok(true);
        }
    }

    Ok(false)
}

fn backfill_exec_ctx_json(tx: &Transaction) -> Result<(), MigrationError> {
    let mut stmt = tx
        .prepare(
            "SELECT id, exec_ctx_connection_id, exec_ctx_database, exec_ctx_schema, exec_ctx_container, exec_ctx_json
             FROM st_session_tabs",
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    for row in rows {
        let (tab_id, connection_id, database, schema, container, existing_json) =
            row.map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        if existing_json
            .as_deref()
            .is_some_and(|json| serde_json::from_str::<dbflux_core::ExecutionContext>(json).is_ok())
        {
            continue;
        }

        let exec_ctx = dbflux_core::ExecutionContext {
            connection_id: connection_id.and_then(|value| uuid::Uuid::parse_str(&value).ok()),
            database,
            schema,
            container,
            source: None,
        };

        let exec_ctx_json =
            serde_json::to_string(&exec_ctx).map_err(|error| MigrationError::Failed {
                name: "007_session_exec_ctx_json".to_string(),
                details: format!("failed to serialize execution context for tab {tab_id}: {error}"),
            })?;

        tx.execute(
            "UPDATE st_session_tabs SET exec_ctx_json = ?2 WHERE id = ?1",
            rusqlite::params![tab_id, exec_ctx_json],
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;
    }

    Ok(())
}
