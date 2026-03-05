use crate::DbError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

const MAX_RECENT_FILES: usize = 30;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecentFile {
    pub path: PathBuf,
    pub last_opened: i64,
}

pub struct RecentFilesStore {
    storage_path: PathBuf,
    entries: Vec<RecentFile>,
}

impl RecentFilesStore {
    pub fn new() -> Result<Self, DbError> {
        let config_dir = dirs::config_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find config directory"))
        })?;

        let app_dir = config_dir.join("dbflux");
        fs::create_dir_all(&app_dir).map_err(DbError::IoError)?;

        let storage_path = app_dir.join("recent_files.json");
        let entries = Self::load_from_path(&storage_path);

        Ok(Self {
            storage_path,
            entries,
        })
    }

    fn load_from_path(path: &PathBuf) -> Vec<RecentFile> {
        if !path.exists() {
            return Vec::new();
        }

        fs::read_to_string(path)
            .ok()
            .and_then(|content| serde_json::from_str(&content).ok())
            .unwrap_or_default()
    }

    pub fn save(&self) {
        if let Ok(content) = serde_json::to_string_pretty(&self.entries) {
            let _ = fs::write(&self.storage_path, content);
        }
    }

    /// Record that a file was opened. Moves existing entries to the top.
    pub fn record_open(&mut self, path: PathBuf) {
        self.entries.retain(|e| e.path != path);

        self.entries.insert(
            0,
            RecentFile {
                path,
                last_opened: chrono::Utc::now().timestamp(),
            },
        );

        if self.entries.len() > MAX_RECENT_FILES {
            self.entries.truncate(MAX_RECENT_FILES);
        }

        self.save();
    }

    pub fn entries(&self) -> &[RecentFile] {
        &self.entries
    }

    pub fn remove(&mut self, path: &PathBuf) {
        self.entries.retain(|e| &e.path != path);
        self.save();
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.save();
    }
}
