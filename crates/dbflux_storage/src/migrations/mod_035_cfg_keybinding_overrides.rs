use rusqlite::Transaction;

use super::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "035_cfg_keybinding_overrides"
    }

    /// Creates the table of user keybinding overrides.
    ///
    /// A row replaces one default binding, identified by its context, its
    /// command and the keys the default keymap gives it. Only overrides are
    /// stored: a binding without a row keeps its default.
    ///
    /// `context` is free text: today it holds a keymap context id
    /// (`editor`), and it can hold a context predicate expression later
    /// without a schema change. `default_keys` and `keys` are key sequences:
    /// chords separated by single spaces (`ctrl+k ctrl+s`), so multi-key
    /// bindings need no schema change either. `keys` is NULL when the user
    /// removed the shortcut.
    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cfg_keybinding_overrides (
                context      TEXT NOT NULL,
                command_id   TEXT NOT NULL,
                default_keys TEXT NOT NULL,
                keys         TEXT,
                updated_at   TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (context, command_id, default_keys)
            );
            ",
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<035_cfg_keybinding_overrides>"),
            source,
        })?;

        Ok(())
    }
}
