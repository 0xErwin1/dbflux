//! Repository for the `cfg_keybinding_overrides` table in dbflux.db.
//!
//! Each row replaces one default keybinding. The keymap layer decides what
//! the context, identifiers and key strings mean; this repository only
//! stores them.

use rusqlite::{Connection, params};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing user keybinding overrides.
pub struct KeybindingOverridesRepository {
    conn: OwnedConnection,
}

impl KeybindingOverridesRepository {
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    fn conn(&self) -> &Connection {
        &self.conn
    }

    fn sqlite_error(source: rusqlite::Error) -> StorageError {
        StorageError::Sqlite {
            path: "dbflux.db".into(),
            source,
        }
    }

    /// Returns every stored override, ordered by context, command and
    /// default keys.
    pub fn list(&self) -> Result<Vec<KeybindingOverrideDto>, StorageError> {
        let mut statement = self
            .conn()
            .prepare(
                r#"
                SELECT context, command_id, default_keys, keys
                FROM cfg_keybinding_overrides
                ORDER BY context, command_id, default_keys
                "#,
            )
            .map_err(Self::sqlite_error)?;

        let rows = statement
            .query_map([], |row| {
                Ok(KeybindingOverrideDto {
                    context: row.get(0)?,
                    command_id: row.get(1)?,
                    default_keys: row.get(2)?,
                    keys: row.get(3)?,
                })
            })
            .map_err(Self::sqlite_error)?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(Self::sqlite_error)
    }

    /// Replaces the whole set of overrides in one transaction, so a failed
    /// write leaves the previous set in place.
    pub fn replace_all(&self, overrides: &[KeybindingOverrideDto]) -> Result<(), StorageError> {
        let transaction = self
            .conn()
            .unchecked_transaction()
            .map_err(Self::sqlite_error)?;

        transaction
            .execute("DELETE FROM cfg_keybinding_overrides", [])
            .map_err(Self::sqlite_error)?;

        for keybinding_override in overrides {
            transaction
                .execute(
                    r#"
                    INSERT INTO cfg_keybinding_overrides (
                        context, command_id, default_keys, keys, updated_at
                    ) VALUES (?1, ?2, ?3, ?4, datetime('now'))
                    "#,
                    params![
                        keybinding_override.context,
                        keybinding_override.command_id,
                        keybinding_override.default_keys,
                        keybinding_override.keys,
                    ],
                )
                .map_err(Self::sqlite_error)?;
        }

        transaction.commit().map_err(Self::sqlite_error)
    }
}

/// Row shape of `cfg_keybinding_overrides`.
///
/// `context` is free text (a context id today, a predicate expression
/// later). `default_keys` and `keys` are key sequences in the keymap's
/// storage form, chords separated by single spaces. `keys` is `None` when
/// the user removed the shortcut of the binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeybindingOverrideDto {
    pub context: String,
    pub command_id: String,
    pub default_keys: String,
    pub keys: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn migrated_repository(directory: &tempfile::TempDir) -> KeybindingOverridesRepository {
        let conn = open_database(&directory.path().join("dbflux.db")).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migrations should run");

        #[allow(clippy::arc_with_non_send_sync)]
        KeybindingOverridesRepository::new(Arc::new(conn))
    }

    fn dto(context: &str, default_keys: &str, keys: Option<&str>) -> KeybindingOverrideDto {
        KeybindingOverrideDto {
            context: context.to_string(),
            command_id: "run_query".to_string(),
            default_keys: default_keys.to_string(),
            keys: keys.map(str::to_string),
        }
    }

    #[test]
    fn fresh_database_has_no_overrides() {
        let directory = tempfile::tempdir().expect("temp dir");
        let repository = migrated_repository(&directory);

        assert!(repository.list().expect("should list").is_empty());
    }

    #[test]
    fn replace_all_round_trips_rebound_and_unbound_rows() {
        let directory = tempfile::tempdir().expect("temp dir");
        let repository = migrated_repository(&directory);

        let written = vec![
            dto("Editor && !Modal", "g g", Some("ctrl+k ctrl+s")),
            dto("editor", "ctrl+enter", None),
            dto("global", "ctrl+enter", Some("ctrl+shift+r")),
        ];
        repository.replace_all(&written).expect("should write");

        assert_eq!(repository.list().expect("should list"), written);
    }

    #[test]
    fn replace_all_drops_rows_missing_from_the_new_set() {
        let directory = tempfile::tempdir().expect("temp dir");
        let repository = migrated_repository(&directory);

        repository
            .replace_all(&[
                dto("editor", "ctrl+enter", None),
                dto("global", "ctrl+enter", Some("ctrl+r")),
            ])
            .expect("should write");
        repository
            .replace_all(&[dto("global", "ctrl+enter", Some("ctrl+t"))])
            .expect("should rewrite");

        assert_eq!(
            repository.list().expect("should list"),
            vec![dto("global", "ctrl+enter", Some("ctrl+t"))]
        );

        repository.replace_all(&[]).expect("should clear");
        assert!(repository.list().expect("should list").is_empty());
    }
}
