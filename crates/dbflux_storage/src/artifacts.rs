//! Artifact store — filesystem boundary for scratch, shadow, and session files.
//!
//! Filesystem artifact content (scratch files, shadow edits, scripts, exports,
//! crash dumps) lives on disk in `~/.local/share/dbflux/` subdirectories.
//! This module owns path construction, orphan cleanup, and basic I/O —
//! it never touches the content of those files, only their paths.
//!
//! `ArtifactStore` is part of `StorageRuntime` and is the single authoritative
//! source for scratch/shadow path ownership. Callers should never hardcode
//! `sessions/` paths directly.

use dbflux_core::LogErr;
use log::info;
use std::path::{Path, PathBuf};

use crate::error::StorageError;
use crate::paths;

/// The session artifact subdirectory relative to the data directory.
pub const SESSIONS_SUBDIR: &str = "sessions";

/// Manages filesystem artifact paths for DBFlux session content.
///
/// Scratch files hold content for untitled tabs. Shadow files hold unsaved
/// edits for file-backed tabs. Orphaned files (not referenced by any session
/// tab) are cleaned up on restore.
pub struct ArtifactStore {
    /// Root directory for session artifacts (e.g. `~/.local/share/dbflux/sessions/`).
    root: PathBuf,
}

impl ArtifactStore {
    /// Creates an `ArtifactStore` for the default DBFlux data directory.
    ///
    /// Calls `paths::data_dir()` which resolves `~/.local/share/dbflux/`.
    /// Use `for_root()` for test/alternate data roots.
    pub fn new() -> Result<Self, StorageError> {
        let data_dir = paths::data_dir()?;
        let root = data_dir.join(SESSIONS_SUBDIR);
        paths::ensure_private_dir(&root)?;
        Ok(Self { root })
    }

    /// Creates an `ArtifactStore` pointing at a specific root directory.
    ///
    /// Useful for tests or alternate data roots.
    pub fn for_root(root: PathBuf) -> Result<Self, StorageError> {
        paths::ensure_private_dir(&root)?;
        Ok(Self { root })
    }

    /// Returns the sessions root directory path.
    pub fn root_path(&self) -> &Path {
        &self.root
    }

    /// Returns the path for a scratch file with the given id and extension.
    ///
    /// E.g. `scratch_path("tab-abc", "sql")` → `sessions/tab-abc.sql`
    pub fn scratch_path(&self, id: &str, extension: &str) -> PathBuf {
        self.root.join(format!("{}.{}", id, extension))
    }

    /// Returns the path for a shadow file with the given id.
    ///
    /// E.g. `shadow_path("tab-abc")` → `sessions/tab-abc.shadow`
    pub fn shadow_path(&self, id: &str) -> PathBuf {
        self.root.join(format!("{}.shadow", id))
    }

    /// Checks whether a file exists at the given path.
    pub fn file_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    /// Reads the full content of a file, or returns an empty string on error.
    pub fn read_content(&self, path: &Path) -> String {
        std::fs::read_to_string(path).unwrap_or_default()
    }

    /// Writes content to a file, creating parent directories if needed.
    ///
    /// On Unix, the file is narrowed to `0o600` after the write succeeds.
    pub fn write_content(&self, path: &Path, content: &str) -> Result<(), StorageError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| StorageError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        std::fs::write(path, content).map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        paths::secure_file_permissions(path)?;
        Ok(())
    }

    /// Deletes a file at the given path, silently ignoring absence.
    pub fn delete_file(&self, path: &Path) {
        if let Err(error) = std::fs::remove_file(path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            log::error!("failed to delete file {}: {error}", path.display());
        }
    }

    /// Returns the modification time of a file, if readable.
    pub fn file_modified_time(&self, path: &Path) -> Option<std::time::SystemTime> {
        std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
    }

    /// Removes scratch/shadow files that are not in the given set of referenced paths.
    ///
    /// This is the orphan cleanup mechanism called during session restore.
    /// Files are matched by absolute path; `session.json` is always excluded.
    pub fn cleanup_orphans(&self, referenced_paths: &[PathBuf]) {
        let referenced: std::collections::HashSet<PathBuf> =
            referenced_paths.iter().cloned().collect();

        let entries = match std::fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(_) => return,
        };

        for entry in entries.flatten() {
            let path = entry.path();

            // Always skip session.json (the old manifest — may linger after migration)
            if path
                .file_name()
                .map(|n| n == "session.json")
                .unwrap_or(false)
            {
                continue;
            }

            if !referenced.contains(&path) && std::fs::remove_file(&path).log_err().is_some() {
                info!("Cleaned up orphan artifact: {}", path.display());
            }
        }
    }

    /// Lists all files in the sessions root directory, excluding hidden files.
    pub fn list_artifacts(&self) -> Vec<PathBuf> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(_) => return Vec::new(),
        };

        entries
            .flatten()
            .filter_map(|e| {
                let name = e.file_name();
                if name.to_string_lossy().starts_with('.') {
                    None
                } else {
                    Some(e.path())
                }
            })
            .collect()
    }
}

impl Default for ArtifactStore {
    fn default() -> Self {
        Self::new().expect("ArtifactStore::default() requires a valid data directory")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, ArtifactStore) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("sessions");
        let store = ArtifactStore::for_root(root).expect("temp store");
        (dir, store)
    }

    #[test]
    fn scratch_and_shadow_paths() {
        let (_dir, store) = temp_store();
        let scratch = store.scratch_path("abc-123", "sql");
        assert!(scratch.to_string_lossy().contains("abc-123.sql"));

        let shadow = store.shadow_path("abc-123");
        assert!(shadow.to_string_lossy().contains("abc-123.shadow"));
    }

    #[test]
    fn write_read_delete_content() {
        let (_dir, store) = temp_store();
        let path = store.scratch_path("test-id", "sql");

        store.write_content(&path, "SELECT 1;").expect("write");
        assert_eq!(store.read_content(&path), "SELECT 1;");

        store.delete_file(&path);
        assert!(!path.exists());
    }

    #[test]
    fn cleanup_removes_orphans() {
        let (_dir, store) = temp_store();

        let kept = store.scratch_path("kept", "sql");
        let orphan = store.scratch_path("orphan", "sql");
        store.write_content(&kept, "keep").expect("write");
        store.write_content(&orphan, "remove").expect("write");

        store.cleanup_orphans(std::slice::from_ref(&kept));

        assert!(kept.exists());
        assert!(!orphan.exists());
    }

    #[test]
    fn cleanup_ignores_session_json() {
        let (_dir, store) = temp_store();
        let session_json = store.root_path().join("session.json");
        store.write_content(&session_json, "{}").expect("write");

        // Cleanup with empty reference set — session.json should survive
        store.cleanup_orphans(&[]);

        assert!(
            session_json.exists(),
            "session.json should be preserved by cleanup"
        );
    }

    #[test]
    fn list_artifacts() {
        let (_dir, store) = temp_store();
        store
            .write_content(&store.scratch_path("a", "sql"), "")
            .expect("write");
        store
            .write_content(&store.scratch_path("b", "lua"), "")
            .expect("write");

        let artifacts = store.list_artifacts();
        assert_eq!(artifacts.len(), 2);
    }
}
