//! Repository for the `cfg_script_roots` table in dbflux.db.
//!
//! Each row registers an external folder of scripts. The scripts layer decides
//! what a valid folder is; this repository only stores the registrations.

use rusqlite::{Connection, params};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing external script folders.
pub struct ScriptRootsRepository {
    conn: OwnedConnection,
}

impl ScriptRootsRepository {
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

    /// Returns every registered folder in the order it was added.
    pub fn list(&self) -> Result<Vec<ScriptRootDto>, StorageError> {
        let mut statement = self
            .conn()
            .prepare(
                r#"
                SELECT id, path, label
                FROM cfg_script_roots
                ORDER BY position, created_at
                "#,
            )
            .map_err(Self::sqlite_error)?;

        let rows = statement
            .query_map([], |row| {
                Ok(ScriptRootDto {
                    id: row.get(0)?,
                    path: row.get(1)?,
                    label: row.get(2)?,
                })
            })
            .map_err(Self::sqlite_error)?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(Self::sqlite_error)
    }

    /// Appends a folder after the ones already registered.
    ///
    /// Fails when the id or the path is already registered.
    pub fn insert(&self, root: &ScriptRootDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_script_roots (id, path, label, position)
                VALUES (
                    ?1, ?2, ?3,
                    (SELECT COALESCE(MAX(position), -1) + 1 FROM cfg_script_roots)
                )
                "#,
                params![root.id, root.path, root.label],
            )
            .map_err(Self::sqlite_error)?;

        Ok(())
    }

    /// Unregisters a folder. Returns whether a row was removed.
    pub fn delete(&self, id: &str) -> Result<bool, StorageError> {
        let removed = self
            .conn()
            .execute("DELETE FROM cfg_script_roots WHERE id = ?1", params![id])
            .map_err(Self::sqlite_error)?;

        Ok(removed > 0)
    }
}

/// Row shape of `cfg_script_roots`.
///
/// `id` is a UUID in its hyphenated text form and `path` the canonical folder
/// path as text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptRootDto {
    pub id: String,
    pub path: String,
    pub label: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn migrated_repository(directory: &tempfile::TempDir) -> ScriptRootsRepository {
        let conn = open_database(&directory.path().join("dbflux.db")).expect("should open");
        MigrationRegistry::new()
            .run_all(&conn)
            .expect("migrations should run");

        #[allow(clippy::arc_with_non_send_sync)]
        ScriptRootsRepository::new(Arc::new(conn))
    }

    fn dto(id: &str, path: &str) -> ScriptRootDto {
        ScriptRootDto {
            id: id.to_string(),
            path: path.to_string(),
            label: path.rsplit('/').next().unwrap_or(path).to_string(),
        }
    }

    #[test]
    fn fresh_database_has_no_script_roots() {
        let directory = tempfile::tempdir().expect("temp dir");
        let repository = migrated_repository(&directory);

        assert!(repository.list().expect("should list").is_empty());
    }

    #[test]
    fn roots_are_listed_in_the_order_they_were_added() {
        let directory = tempfile::tempdir().expect("temp dir");
        let repository = migrated_repository(&directory);

        repository.insert(&dto("b", "/srv/zeta")).expect("insert");
        repository.insert(&dto("a", "/srv/alpha")).expect("insert");

        let listed = repository.list().expect("should list");

        assert_eq!(listed, vec![dto("b", "/srv/zeta"), dto("a", "/srv/alpha")]);
    }

    #[test]
    fn the_same_path_cannot_be_registered_twice() {
        let directory = tempfile::tempdir().expect("temp dir");
        let repository = migrated_repository(&directory);

        repository.insert(&dto("a", "/srv/sql")).expect("insert");

        assert!(repository.insert(&dto("b", "/srv/sql")).is_err());
        assert_eq!(repository.list().expect("should list").len(), 1);
    }

    #[test]
    fn deleting_removes_only_that_root() {
        let directory = tempfile::tempdir().expect("temp dir");
        let repository = migrated_repository(&directory);

        repository.insert(&dto("a", "/srv/one")).expect("insert");
        repository.insert(&dto("b", "/srv/two")).expect("insert");

        assert!(repository.delete("a").expect("delete"));
        assert!(!repository.delete("a").expect("delete again"));
        assert_eq!(
            repository.list().expect("should list"),
            vec![dto("b", "/srv/two")]
        );
    }
}
