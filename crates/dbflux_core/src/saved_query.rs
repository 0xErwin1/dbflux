use crate::DbError;
use chrono::{DateTime, Local, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQuery {
    pub id: Uuid,
    pub name: String,
    pub sql: String,
    pub is_favorite: bool,
    pub connection_id: Option<Uuid>,
    pub created_at: i64,
    pub last_used_at: i64,
}

impl SavedQuery {
    pub fn new(name: String, sql: String, connection_id: Option<Uuid>) -> Self {
        let now = Utc::now().timestamp();

        Self {
            id: Uuid::new_v4(),
            name,
            sql,
            is_favorite: false,
            connection_id,
            created_at: now,
            last_used_at: now,
        }
    }

    pub fn formatted_created_at(&self) -> String {
        Self::format_timestamp(self.created_at)
    }

    pub fn formatted_last_used_at(&self) -> String {
        Self::format_timestamp(self.last_used_at)
    }

    pub fn sql_preview(&self, max_len: usize) -> String {
        let trimmed = self.sql.trim();
        let single_line = trimmed.replace('\n', " ").replace("  ", " ");
        crate::truncate_string_safe(&single_line, max_len)
    }

    fn format_timestamp(timestamp: i64) -> String {
        let utc_dt = Utc.timestamp_opt(timestamp, 0).single();
        match utc_dt {
            Some(dt) => {
                let local: DateTime<Local> = dt.into();
                local.format("%Y-%m-%d %H:%M:%S").to_string()
            }
            None => "Unknown".to_string(),
        }
    }
}

pub struct SavedQueryStore {
    path: PathBuf,
    entries: Vec<SavedQuery>,
    load_warning: Option<String>,
}

impl SavedQueryStore {
    pub fn new() -> Result<Self, DbError> {
        let config_dir = dirs::config_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find config directory"))
        })?;

        let app_dir = config_dir.join("dbflux");
        fs::create_dir_all(&app_dir).map_err(DbError::IoError)?;

        let path = app_dir.join("saved_queries.json");
        let (entries, load_warning) = Self::load_from_path(&path)?;

        Ok(Self {
            path,
            entries,
            load_warning,
        })
    }

    pub fn from_path(path: PathBuf) -> Result<Self, DbError> {
        let (entries, load_warning) = Self::load_from_path(&path)?;

        Ok(Self {
            path,
            entries,
            load_warning,
        })
    }

    fn load_from_path(path: &PathBuf) -> Result<(Vec<SavedQuery>, Option<String>), DbError> {
        if !path.exists() {
            return Ok((Vec::new(), None));
        }

        let content = fs::read_to_string(path).map_err(DbError::IoError)?;
        let parsed: Result<Vec<SavedQuery>, _> = serde_json::from_str(&content);

        match parsed {
            Ok(entries) => Ok((entries, None)),
            Err(err) => {
                let warning = "Saved queries file was corrupted and has been reset.".to_string();
                let backup_path =
                    path.with_extension(format!("corrupt-{}", Utc::now().format("%Y%m%d%H%M%S")));

                if let Err(rename_err) = fs::rename(path, &backup_path) {
                    log::warn!(
                        "Failed to backup corrupted saved queries file: {} (original parse error: {})",
                        rename_err,
                        err
                    );
                } else {
                    log::warn!(
                        "Saved queries file was corrupted. Backup created at {:?}: {}",
                        backup_path,
                        err
                    );
                }

                Ok((Vec::new(), Some(warning)))
            }
        }
    }

    pub fn save(&self) -> Result<(), DbError> {
        let content = serde_json::to_string_pretty(&self.entries)
            .map_err(|e| DbError::InvalidProfile(e.to_string()))?;

        fs::write(&self.path, content).map_err(DbError::IoError)?;
        Ok(())
    }

    pub fn take_load_warning(&mut self) -> Option<String> {
        self.load_warning.take()
    }

    pub fn add(&mut self, query: SavedQuery) {
        self.entries.push(query);
        self.sort_entries();
    }

    pub fn update(&mut self, id: Uuid, name: String, sql: String) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.id == id) {
            entry.name = name;
            entry.sql = sql;
            return true;
        }
        false
    }

    pub fn update_sql(&mut self, id: Uuid, sql: &str) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.id == id) {
            entry.sql = sql.to_string();
            entry.last_used_at = chrono::Utc::now().timestamp();
            return true;
        }
        false
    }

    pub fn update_name(&mut self, id: Uuid, name: &str) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.id == id) {
            entry.name = name.to_string();
            return true;
        }
        false
    }

    pub fn get(&self, id: Uuid) -> Option<&SavedQuery> {
        self.entries.iter().find(|e| e.id == id)
    }

    pub fn update_last_used(&mut self, id: Uuid) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.id == id) {
            entry.last_used_at = Utc::now().timestamp();
            self.sort_entries();
            return true;
        }
        false
    }

    pub fn remove(&mut self, id: Uuid) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.id != id);
        before != self.entries.len()
    }

    pub fn toggle_favorite(&mut self, id: Uuid) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.id == id) {
            entry.is_favorite = !entry.is_favorite;
            return entry.is_favorite;
        }
        false
    }

    pub fn get_all(&self) -> &[SavedQuery] {
        &self.entries
    }

    pub fn search(&self, query: &str) -> Vec<&SavedQuery> {
        let query_lower = query.to_lowercase();
        self.entries
            .iter()
            .filter(|e| {
                e.name.to_lowercase().contains(&query_lower)
                    || e.sql.to_lowercase().contains(&query_lower)
            })
            .collect()
    }

    pub fn favorites(&self) -> Vec<&SavedQuery> {
        self.entries.iter().filter(|e| e.is_favorite).collect()
    }

    fn sort_entries(&mut self) {
        self.entries
            .sort_by(|a, b| b.last_used_at.cmp(&a.last_used_at));
    }
}
