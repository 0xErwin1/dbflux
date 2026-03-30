//! Repository for tracking legacy import provenance in config.db.
//!
//! The `legacy_imports` table records each JSON source file that has been migrated
//! to the native SQLite schema, enabling one-shot semantics (skip if already imported
//! and hash matches) and restart-safe retry on failure.

use rusqlite::params;
use uuid::Uuid;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// Repository for managing legacy import provenance records.
#[derive(Clone)]
pub struct LegacyImportsRepository {
    conn: OwnedConnection,
}

impl LegacyImportsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Fetches a legacy import record by source path.
    pub fn get_by_source_path(&self, path: &str) -> Result<Option<LegacyImport>, StorageError> {
        let result = self.conn.query_row(
            r#"
            SELECT id, source_path, source_hash, imported_at, record_count, domain, status, error_message
            FROM legacy_imports
            WHERE source_path = ?1
            "#,
            [path],
            |row| {
                Ok(LegacyImport {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    source_hash: row.get(2)?,
                    imported_at: row.get(3)?,
                    record_count: row.get(4)?,
                    domain: row.get(5)?,
                    status: row.get(6)?,
                    error_message: row.get(7)?,
                })
            },
        );

        match result {
            Ok(import) => Ok(Some(import)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Finds a legacy import record by source hash.
    pub fn find_by_hash(&self, source_hash: &str) -> Result<Option<LegacyImport>, StorageError> {
        let result = self.conn.query_row(
            r#"
            SELECT id, source_path, source_hash, imported_at, record_count, domain, status, error_message
            FROM legacy_imports
            WHERE source_hash = ?1
            "#,
            [source_hash],
            |row| {
                Ok(LegacyImport {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    source_hash: row.get(2)?,
                    imported_at: row.get(3)?,
                    record_count: row.get(4)?,
                    domain: row.get(5)?,
                    status: row.get(6)?,
                    error_message: row.get(7)?,
                })
            },
        );

        match result {
            Ok(import) => Ok(Some(import)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Returns all legacy imports for a given domain.
    pub fn all_for_domain(&self, domain: &str) -> Result<Vec<LegacyImport>, StorageError> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT id, source_path, source_hash, imported_at, record_count, domain, status, error_message
                FROM legacy_imports
                WHERE domain = ?1
                ORDER BY imported_at DESC
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let imports = stmt
            .query_map([domain], |row| {
                Ok(LegacyImport {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    source_hash: row.get(2)?,
                    imported_at: row.get(3)?,
                    record_count: row.get(4)?,
                    domain: row.get(5)?,
                    status: row.get(6)?,
                    error_message: row.get(7)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let mut result = Vec::new();
        let mut last_err = None;
        for import in imports {
            match import {
                Ok(i) => result.push(i),
                Err(e) => last_err = Some(e),
            }
        }

        if let Some(e) = last_err {
            return Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            });
        }

        Ok(result)
    }

    /// Returns the import status for a source path and hash pair.
    ///
    /// Returns `Completed` if the source has been imported (hash match or mismatch — data is
    /// already in SQLite either way), `Failed` if it was previously attempted but failed, or
    /// `None` if no record exists for the given path.
    pub fn get_status(&self, path: &str, hash: &str) -> Result<Option<ImportStatus>, StorageError> {
        let result = self.conn.query_row(
            r#"
            SELECT source_hash, status
            FROM legacy_imports
            WHERE source_path = ?1
            "#,
            [path],
            |row| {
                let stored_hash: String = row.get(0)?;
                let status: String = row.get(1)?;
                Ok((stored_hash, status))
            },
        );

        match result {
            Ok((stored_hash, status)) => {
                let import_status = if stored_hash == hash {
                    match status.as_str() {
                        "completed" => Some(ImportStatus::Completed),
                        "failed" => Some(ImportStatus::Failed),
                        _ => Some(ImportStatus::Failed),
                    }
                } else {
                    // Hash mismatch: file was edited post-migration.
                    // Data is already in SQLite — treat as completed to prevent re-import.
                    Some(ImportStatus::Completed)
                };
                Ok(import_status)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "config.db".into(),
                source: e,
            }),
        }
    }

    /// Records a new legacy import with the given parameters.
    pub fn record_import(&self, import: &LegacyImport) -> Result<(), StorageError> {
        self.conn
            .execute(
                r#"
                INSERT OR REPLACE INTO legacy_imports (
                    id, source_path, source_hash, imported_at, record_count, domain, status, error_message
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    import.id,
                    import.source_path,
                    import.source_hash,
                    import.imported_at,
                    import.record_count,
                    import.domain,
                    import.status,
                    import.error_message,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Records a failed import attempt with error message.
    pub fn record_failure(
        &self,
        source_path: &str,
        domain: &str,
        error: &str,
    ) -> Result<(), StorageError> {
        // Check if record exists
        let existing = self.get_by_source_path(source_path)?;

        if let Some(mut import) = existing {
            // Update existing record to failed status
            import.status = "failed".to_string();
            import.error_message = Some(error.to_string());
            self.record_import(&import)?;
        } else {
            // Create new failed record
            let import = LegacyImport::new(
                source_path.to_string(),
                domain.to_string(),
                String::new(), // hash unknown for failed imports
            );
            let mut import = import;
            import.status = "failed".to_string();
            import.error_message = Some(error.to_string());
            self.record_import(&import)?;
        }

        Ok(())
    }

    /// Updates the status of a legacy import by ID.
    pub fn update_status(&self, id: &str, status: &str) -> Result<(), StorageError> {
        let rows_affected = self
            .conn
            .execute(
                "UPDATE legacy_imports SET status = ?2 WHERE id = ?1",
                params![id, status],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        if rows_affected == 0 {
            log::warn!("No legacy_import record found to update: {}", id);
        }

        Ok(())
    }
}

/// Represents a legacy import provenance record.
#[derive(Debug, Clone)]
pub struct LegacyImport {
    pub id: String,
    pub source_path: String,
    pub source_hash: String,
    pub imported_at: String,
    pub record_count: i32,
    pub domain: String,
    pub status: String,
    pub error_message: Option<String>,
}

impl LegacyImport {
    /// Creates a new legacy import record with a generated UUID and current timestamp.
    pub fn new(source_path: String, domain: String, source_hash: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            source_path,
            domain,
            source_hash,
            imported_at: String::new(),
            record_count: 0,
            status: "completed".to_string(),
            error_message: None,
        }
    }

    /// Returns true if this import completed successfully.
    pub fn is_completed(&self) -> bool {
        self.status == "completed"
    }

    /// Returns true if this import failed.
    pub fn is_failed(&self) -> bool {
        self.status == "failed"
    }
}

/// Import status returned by `LegacyImportsRepository::get_status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportStatus {
    Completed,
    Failed,
    NotFound,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_config_migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_legacy_imports_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        path
    }

    #[test]
    fn record_and_fetch_import() {
        let path = temp_db("record_fetch");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = LegacyImportsRepository::new(Arc::new(conn));

        let import = LegacyImport::new(
            "profiles.json".to_string(),
            "connection_profiles".to_string(),
            "abc123def456".to_string(),
        );

        repo.record_import(&import).expect("should record");

        let found = repo
            .get_by_source_path("profiles.json")
            .expect("should fetch")
            .expect("should be found");
        assert_eq!(found.source_path, "profiles.json");
        assert_eq!(found.domain, "connection_profiles");
        assert_eq!(found.source_hash, "abc123def456");
        assert_eq!(found.status, "completed");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn get_status_completed() {
        let path = temp_db("status_completed");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = LegacyImportsRepository::new(Arc::new(conn));

        let import = LegacyImport::new(
            "test.json".to_string(),
            "test_kind".to_string(),
            "hash123".to_string(),
        );
        repo.record_import(&import).expect("should record");

        let status = repo
            .get_status("test.json", "hash123")
            .expect("should get status");
        assert_eq!(status, Some(ImportStatus::Completed));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn get_status_not_found() {
        let path = temp_db("status_not_found");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = LegacyImportsRepository::new(Arc::new(conn));

        let status = repo
            .get_status("nonexistent.json", "anyhash")
            .expect("should get status");
        assert_eq!(status, None);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn get_status_hash_mismatch() {
        let path = temp_db("status_mismatch");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = LegacyImportsRepository::new(Arc::new(conn));

        let import = LegacyImport::new(
            "edited.json".to_string(),
            "test_kind".to_string(),
            "original_hash".to_string(),
        );
        repo.record_import(&import).expect("should record");

        // Same path but different hash (file was edited post-migration)
        let status = repo
            .get_status("edited.json", "edited_hash")
            .expect("should get status");
        assert_eq!(status, Some(ImportStatus::Completed));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn update_status() {
        let path = temp_db("update_status");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = LegacyImportsRepository::new(Arc::new(conn));

        let import = LegacyImport::new(
            "fail_test.json".to_string(),
            "test_kind".to_string(),
            "hash".to_string(),
        );
        let id = import.id.clone();
        repo.record_import(&import).expect("should record");

        repo.update_status(&id, "failed").expect("should update");

        let found = repo
            .get_by_source_path("fail_test.json")
            .expect("should fetch")
            .expect("should be found");
        assert_eq!(found.status, "failed");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn find_by_hash() {
        let path = temp_db("find_by_hash");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = LegacyImportsRepository::new(Arc::new(conn));

        let import = LegacyImport::new(
            "test.json".to_string(),
            "connections".to_string(),
            "hash_abc123".to_string(),
        );
        repo.record_import(&import).expect("should record");

        let found = repo
            .find_by_hash("hash_abc123")
            .expect("should find")
            .expect("should exist");
        assert_eq!(found.source_hash, "hash_abc123");
        assert_eq!(found.domain, "connections");

        let not_found = repo
            .find_by_hash("nonexistent_hash")
            .expect("should not error");
        assert!(not_found.is_none());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn all_for_domain() {
        let path = temp_db("all_for_domain");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = LegacyImportsRepository::new(Arc::new(conn));

        // Create imports for different domains
        let import1 = LegacyImport::new(
            "profiles.json".to_string(),
            "connections".to_string(),
            "hash1".to_string(),
        );
        let import2 = LegacyImport::new(
            "auth.json".to_string(),
            "auth_profiles".to_string(),
            "hash2".to_string(),
        );
        let import3 = LegacyImport::new(
            "hooks.json".to_string(),
            "connections".to_string(),
            "hash3".to_string(),
        );

        repo.record_import(&import1).expect("should record");
        repo.record_import(&import2).expect("should record");
        repo.record_import(&import3).expect("should record");

        let connections = repo.all_for_domain("connections").expect("should work");
        assert_eq!(connections.len(), 2);

        let auth_profiles = repo.all_for_domain("auth_profiles").expect("should work");
        assert_eq!(auth_profiles.len(), 1);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn record_failure() {
        let path = temp_db("record_failure");
        let conn = open_database(&path).expect("should open");
        run_config_migrations(&conn).expect("migration should run");

        let repo = LegacyImportsRepository::new(Arc::new(conn));

        // Record a failure for a new file
        repo.record_failure(
            "failed.json",
            "test_domain",
            "Parse error: unexpected token",
        )
        .expect("should record failure");

        let found = repo
            .get_by_source_path("failed.json")
            .expect("should fetch")
            .expect("should exist");
        assert_eq!(found.status, "failed");
        assert!(found.error_message.is_some());
        assert!(found.error_message.unwrap().contains("Parse error"));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
