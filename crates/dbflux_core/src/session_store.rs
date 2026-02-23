use crate::{DbError, ExecutionContext, QueryLanguage};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

const MANIFEST_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "session.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionManifest {
    pub version: u32,
    pub active_index: Option<usize>,
    pub tabs: Vec<SessionTab>,
}

impl Default for SessionManifest {
    fn default() -> Self {
        Self {
            version: MANIFEST_VERSION,
            active_index: None,
            tabs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTab {
    pub id: String,
    pub kind: SessionTabKind,
    /// Stored as string ("sql", "mongo", "redis") to avoid deserializing `&'static str`.
    pub language: String,
    pub exec_ctx: ExecutionContext,
}

impl SessionTab {
    pub fn query_language(&self) -> QueryLanguage {
        match self.language.as_str() {
            "sql" => QueryLanguage::Sql,
            "mongo" => QueryLanguage::MongoQuery,
            "redis" => QueryLanguage::RedisCommands,
            "cypher" => QueryLanguage::Cypher,
            "influx" => QueryLanguage::InfluxQuery,
            "cql" => QueryLanguage::Cql,
            _ => QueryLanguage::Sql,
        }
    }

    pub fn language_key(language: QueryLanguage) -> String {
        match language {
            QueryLanguage::Sql => "sql".into(),
            QueryLanguage::MongoQuery => "mongo".into(),
            QueryLanguage::RedisCommands => "redis".into(),
            QueryLanguage::Cypher => "cypher".into(),
            QueryLanguage::InfluxQuery => "influx".into(),
            QueryLanguage::Cql => "cql".into(),
            QueryLanguage::Custom(_) => "sql".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SessionTabKind {
    Scratch {
        scratch_path: PathBuf,
        title: String,
    },
    FileBacked {
        file_path: PathBuf,
        shadow_path: Option<PathBuf>,
    },
}

/// Manages the `~/.local/share/dbflux/sessions/` directory for auto-persist.
///
/// Scratch files hold content for untitled tabs. Shadow files hold unsaved
/// edits for file-backed tabs. The manifest tracks which tabs to restore.
pub struct SessionStore {
    root: PathBuf,
}

impl SessionStore {
    pub fn new() -> Result<Self, DbError> {
        let data_dir = dirs::data_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find data directory"))
        })?;

        let root = data_dir.join("dbflux").join("sessions");
        fs::create_dir_all(&root).map_err(DbError::IoError)?;

        Ok(Self { root })
    }

    pub fn root_path(&self) -> &Path {
        &self.root
    }

    pub fn scratch_path(&self, id: &str, extension: &str) -> PathBuf {
        self.root.join(format!("{}.{}", id, extension))
    }

    pub fn shadow_path(&self, id: &str) -> PathBuf {
        self.root.join(format!("{}.shadow", id))
    }

    pub fn write_content(&self, path: &Path, content: &str) -> Result<(), DbError> {
        fs::write(path, content).map_err(DbError::IoError)
    }

    pub fn read_content(&self, path: &Path) -> Result<String, DbError> {
        fs::read_to_string(path).map_err(DbError::IoError)
    }

    pub fn delete(&self, path: &Path) {
        let _ = fs::remove_file(path);
    }

    pub fn file_modified_time(&self, path: &Path) -> Option<std::time::SystemTime> {
        fs::metadata(path).ok().and_then(|m| m.modified().ok())
    }

    pub fn load_manifest(&self) -> Option<SessionManifest> {
        let path = self.root.join(MANIFEST_FILE);
        let content = fs::read_to_string(&path).ok()?;
        let manifest: SessionManifest = serde_json::from_str(&content).ok()?;

        if manifest.version != MANIFEST_VERSION {
            return None;
        }

        Some(manifest)
    }

    pub fn save_manifest(&self, manifest: &SessionManifest) -> Result<(), DbError> {
        let path = self.root.join(MANIFEST_FILE);
        let content = serde_json::to_string_pretty(manifest)
            .map_err(|e| DbError::IoError(std::io::Error::other(e.to_string())))?;
        fs::write(&path, content).map_err(DbError::IoError)
    }

    /// Remove scratch/shadow files that are not referenced by the manifest.
    pub fn cleanup_orphans(&self, manifest: &SessionManifest) {
        let referenced: std::collections::HashSet<PathBuf> = manifest
            .tabs
            .iter()
            .flat_map(|tab| {
                let mut paths = Vec::new();
                match &tab.kind {
                    SessionTabKind::Scratch { scratch_path, .. } => {
                        paths.push(scratch_path.clone());
                    }
                    SessionTabKind::FileBacked { shadow_path, .. } => {
                        if let Some(p) = shadow_path {
                            paths.push(p.clone());
                        }
                    }
                }
                paths
            })
            .collect();

        let entries = match fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(_) => return,
        };

        for entry in entries.flatten() {
            let path = entry.path();

            if path
                .file_name()
                .map(|n| n == MANIFEST_FILE)
                .unwrap_or(false)
            {
                continue;
            }

            if !referenced.contains(&path) {
                let _ = fs::remove_file(&path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, SessionStore) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("sessions");
        fs::create_dir_all(&root).unwrap();
        (dir, SessionStore { root })
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

        store.write_content(&path, "SELECT 1;").unwrap();
        assert_eq!(store.read_content(&path).unwrap(), "SELECT 1;");

        store.delete(&path);
        assert!(store.read_content(&path).is_err());
    }

    #[test]
    fn manifest_roundtrip() {
        let (_dir, store) = temp_store();

        let manifest = SessionManifest {
            version: MANIFEST_VERSION,
            active_index: Some(1),
            tabs: vec![
                SessionTab {
                    id: "tab-1".into(),
                    kind: SessionTabKind::Scratch {
                        scratch_path: store.scratch_path("tab-1", "sql"),
                        title: "Query 1".into(),
                    },
                    language: "sql".into(),
                    exec_ctx: ExecutionContext::default(),
                },
                SessionTab {
                    id: "tab-2".into(),
                    kind: SessionTabKind::FileBacked {
                        file_path: PathBuf::from("/home/user/report.sql"),
                        shadow_path: Some(store.shadow_path("tab-2")),
                    },
                    language: "sql".into(),
                    exec_ctx: ExecutionContext {
                        connection_id: Some(uuid::Uuid::new_v4()),
                        database: Some("mydb".into()),
                        schema: Some("public".into()),
                        container: None,
                    },
                },
            ],
        };

        store.save_manifest(&manifest).unwrap();

        let loaded = store.load_manifest().unwrap();
        assert_eq!(loaded.active_index, Some(1));
        assert_eq!(loaded.tabs.len(), 2);
        assert_eq!(loaded.tabs[0].id, "tab-1");
    }

    #[test]
    fn cleanup_removes_orphans() {
        let (_dir, store) = temp_store();

        let kept = store.scratch_path("kept", "sql");
        let orphan = store.scratch_path("orphan", "sql");
        store.write_content(&kept, "keep").unwrap();
        store.write_content(&orphan, "remove").unwrap();

        let manifest = SessionManifest {
            version: MANIFEST_VERSION,
            active_index: None,
            tabs: vec![SessionTab {
                id: "kept".into(),
                kind: SessionTabKind::Scratch {
                    scratch_path: kept.clone(),
                    title: "Q".into(),
                },
                language: "sql".into(),
                exec_ctx: ExecutionContext::default(),
            }],
        };

        store.save_manifest(&manifest).unwrap();
        store.cleanup_orphans(&manifest);

        assert!(kept.exists());
        assert!(!orphan.exists());
    }

    #[test]
    fn load_missing_manifest_returns_none() {
        let (_dir, store) = temp_store();
        assert!(store.load_manifest().is_none());
    }
}
