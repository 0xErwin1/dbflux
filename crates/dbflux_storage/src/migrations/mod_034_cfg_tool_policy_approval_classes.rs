//! Migration 034: Add `cfg_tool_policy_approval_classes` and move mutating
//! classes of existing policies into it.
//!
//! A tool policy now decides each execution class as Allow, Ask or Deny.
//! Allow stays in `cfg_tool_policy_allowed_classes`, Ask is stored in this new
//! child table, and a class in neither table is Deny.
//!
//! Existing policies are rewritten with the new defaults. Before this
//! migration a policy could only allow a class, so allowing a mutating class
//! was never an explicit choice to run it without approval:
//!
//! - an allowed mutating class (`write`, `destructive`, `admin_safe`, `admin`,
//!   `admin_destructive`) becomes Ask,
//! - an allowed reading class (`metadata`, `read`) stays Allow,
//! - a class that was not allowed stays Deny.
//!
//! A binary built before this migration ignores the new table, so a class
//! moved to Ask reads as Deny there instead of widening access.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "034_cfg_tool_policy_approval_classes"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cfg_tool_policy_approval_classes (
                id TEXT PRIMARY KEY,
                tool_policy_id TEXT NOT NULL,
                class_name TEXT NOT NULL,
                FOREIGN KEY (tool_policy_id) REFERENCES cfg_tool_policies(id) ON DELETE CASCADE,
                UNIQUE(tool_policy_id, class_name)
            );

            CREATE INDEX IF NOT EXISTS idx_cfg_tool_policy_approval_classes_policy
                ON cfg_tool_policy_approval_classes(tool_policy_id);
            ",
        )
        .map_err(sqlite_err)?;

        let allowed_classes_exist: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'cfg_tool_policy_allowed_classes'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(sqlite_err)?;

        if !allowed_classes_exist {
            return Ok(());
        }

        tx.execute_batch(
            "
            INSERT OR IGNORE INTO cfg_tool_policy_approval_classes (id, tool_policy_id, class_name)
                SELECT id, tool_policy_id, class_name
                FROM cfg_tool_policy_allowed_classes
                WHERE lower(class_name) IN
                    ('write', 'destructive', 'admin_safe', 'admin', 'admin_destructive');

            DELETE FROM cfg_tool_policy_allowed_classes
                WHERE lower(class_name) IN
                    ('write', 'destructive', 'admin_safe', 'admin', 'admin_destructive');
            ",
        )
        .map_err(sqlite_err)?;

        Ok(())
    }
}

fn sqlite_err(source: rusqlite::Error) -> MigrationError {
    MigrationError::Sqlite {
        path: std::path::PathBuf::from("<034_cfg_tool_policy_approval_classes>"),
        source,
    }
}
