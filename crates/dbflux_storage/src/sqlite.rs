use std::path::Path;

use rusqlite::Connection;

use crate::error::StorageError;

/// Opens (or creates) a SQLite database at `path` and applies the standard
/// PRAGMA set that every internal DBFlux database should use.
pub fn open_database(path: &Path) -> Result<Connection, StorageError> {
    let conn = Connection::open(path).map_err(|source| StorageError::Sqlite {
        path: path.to_path_buf(),
        source,
    })?;

    apply_default_pragmas(&conn, path)?;

    Ok(conn)
}

/// Applies the recommended PRAGMA set for internal databases.
///
/// - WAL journal mode for concurrent readers.
/// - NORMAL synchronous for a good durability/performance balance.
/// - Foreign keys ON so schema constraints are enforced.
/// - busy_timeout of 5000ms so concurrent writers retry rather than returning SQLITE_BUSY.
fn apply_default_pragmas(conn: &Connection, path: &Path) -> Result<(), StorageError> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|source| StorageError::Sqlite {
            path: path.to_path_buf(),
            source,
        })?;

    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(|source| StorageError::Sqlite {
            path: path.to_path_buf(),
            source,
        })?;

    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(|source| StorageError::Sqlite {
            path: path.to_path_buf(),
            source,
        })?;

    conn.pragma_update(None, "busy_timeout", 5000)
        .map_err(|source| StorageError::Sqlite {
            path: path.to_path_buf(),
            source,
        })?;

    Ok(())
}

/// Toggles foreign-key enforcement on `conn`.
///
/// Must be called OUTSIDE any open transaction — `PRAGMA foreign_keys` is a
/// no-op while a transaction is active. Migrations that rebuild a table (drop +
/// recreate to change constraints) require enforcement OFF, otherwise dropping
/// a referenced parent triggers cascading deletes on its child rows.
pub fn set_foreign_keys(conn: &Connection, enabled: bool) -> Result<(), StorageError> {
    conn.pragma_update(None, "foreign_keys", if enabled { "ON" } else { "OFF" })
        .map_err(|source| StorageError::Sqlite {
            path: std::path::PathBuf::from("<dbflux>"),
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dbflux_storage_test_{}.sqlite", name))
    }

    #[test]
    fn open_creates_file_and_applies_pragmas() {
        let path = temp_db("open");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");
        assert!(path.exists());

        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");

        let sync: i64 = conn
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        assert_eq!(sync, 1); // NORMAL = 1

        let fk: i64 = conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();
        assert_eq!(fk, 1);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
