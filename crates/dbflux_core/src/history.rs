use crate::DbError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

/// A single query history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub id: Uuid,
    pub sql: String,
    pub timestamp: i64,
    pub database: Option<String>,
    pub connection_name: Option<String>,
    pub execution_time_ms: u64,
    pub row_count: Option<usize>,
    pub is_favorite: bool,
}

impl HistoryEntry {
    pub fn new(
        sql: String,
        database: Option<String>,
        connection_name: Option<String>,
        execution_time: Duration,
        row_count: Option<usize>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            sql,
            timestamp: chrono::Utc::now().timestamp(),
            database,
            connection_name,
            execution_time_ms: execution_time.as_millis() as u64,
            row_count,
            is_favorite: false,
        }
    }

    pub fn formatted_timestamp(&self) -> String {
        use chrono::{DateTime, Local, TimeZone, Utc};

        let utc_dt = Utc.timestamp_opt(self.timestamp, 0).single();
        match utc_dt {
            Some(dt) => {
                let local: DateTime<Local> = dt.into();
                local.format("%Y-%m-%d %H:%M:%S").to_string()
            }
            None => "Unknown".to_string(),
        }
    }

    pub fn sql_preview(&self, max_len: usize) -> String {
        let trimmed = self.sql.trim();
        let single_line = trimmed.replace('\n', " ").replace("  ", " ");
        crate::truncate_string_safe(&single_line, max_len)
    }
}

/// Persistent store for query history.
pub struct HistoryStore {
    path: PathBuf,
    entries: Vec<HistoryEntry>,
    max_entries: usize,
}

impl HistoryStore {
    const DEFAULT_MAX_ENTRIES: usize = 1000;

    pub fn new() -> Result<Self, DbError> {
        let config_dir = dirs::config_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find config directory"))
        })?;

        let app_dir = config_dir.join("dbflux");
        fs::create_dir_all(&app_dir).map_err(DbError::IoError)?;

        let path = app_dir.join("history.json");
        let entries = Self::load_from_path(&path)?;

        Ok(Self {
            path,
            entries,
            max_entries: Self::DEFAULT_MAX_ENTRIES,
        })
    }

    fn load_from_path(path: &PathBuf) -> Result<Vec<HistoryEntry>, DbError> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(path).map_err(DbError::IoError)?;
        let entries: Vec<HistoryEntry> =
            serde_json::from_str(&content).map_err(|e| DbError::InvalidProfile(e.to_string()))?;

        Ok(entries)
    }

    pub fn save(&self) -> Result<(), DbError> {
        let content = serde_json::to_string_pretty(&self.entries)
            .map_err(|e| DbError::InvalidProfile(e.to_string()))?;

        fs::write(&self.path, content).map_err(DbError::IoError)?;
        Ok(())
    }

    pub fn add(&mut self, entry: HistoryEntry) {
        self.entries.insert(0, entry);

        if self.entries.len() > self.max_entries {
            // Keep favorites, remove oldest non-favorites
            let favorites: Vec<_> = self
                .entries
                .iter()
                .filter(|e| e.is_favorite)
                .cloned()
                .collect();

            let non_favorites: Vec<_> = self
                .entries
                .iter()
                .filter(|e| !e.is_favorite)
                .take(self.max_entries.saturating_sub(favorites.len()))
                .cloned()
                .collect();

            self.entries = favorites;
            self.entries.extend(non_favorites);
            self.entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        }
    }

    pub fn set_max_entries(&mut self, max: usize) {
        self.max_entries = max.max(10);

        if self.entries.len() > self.max_entries {
            let favorites: Vec<_> = self
                .entries
                .iter()
                .filter(|e| e.is_favorite)
                .cloned()
                .collect();

            let non_favorites: Vec<_> = self
                .entries
                .iter()
                .filter(|e| !e.is_favorite)
                .take(self.max_entries.saturating_sub(favorites.len()))
                .cloned()
                .collect();

            self.entries = favorites;
            self.entries.extend(non_favorites);
            self.entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        }
    }

    pub fn entries(&self) -> &[HistoryEntry] {
        &self.entries
    }

    pub fn toggle_favorite(&mut self, id: Uuid) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.id == id) {
            entry.is_favorite = !entry.is_favorite;
            return entry.is_favorite;
        }
        false
    }

    pub fn remove(&mut self, id: Uuid) {
        self.entries.retain(|e| e.id != id);
    }

    pub fn clear_non_favorites(&mut self) {
        self.entries.retain(|e| e.is_favorite);
    }

    pub fn search(&self, query: &str) -> Vec<&HistoryEntry> {
        let query_lower = query.to_lowercase();
        self.entries
            .iter()
            .filter(|e| e.sql.to_lowercase().contains(&query_lower))
            .collect()
    }

    pub fn favorites(&self) -> Vec<&HistoryEntry> {
        self.entries.iter().filter(|e| e.is_favorite).collect()
    }
}

impl Default for HistoryStore {
    fn default() -> Self {
        Self::new().expect("Failed to create history store")
    }
}
