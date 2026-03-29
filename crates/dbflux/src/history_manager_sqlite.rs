//! Repository-backed history manager using `state.db`.
//!
//! This replaces the JSON-based `HistoryStore` with SQLite-backed storage
//! while preserving the same external interface.

use dbflux_core::chrono::Utc;
use dbflux_core::{HistoryEntry, SavedQuery};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::repositories::state::query_history::QueryHistoryRepository;
use dbflux_storage::repositories::state::recent_items::RecentItemDto;
use dbflux_storage::repositories::state::recent_items::RecentItemsRepository;
use dbflux_storage::repositories::state::saved_queries::SavedQueriesRepository;
use log::{error, info};
use std::time::Duration;
use uuid::Uuid;

/// Re-export for backwards compatibility.
pub type RecentFile = dbflux_core::RecentFile;

/// Repository-backed history manager.
pub struct HistoryManager {
    history_repo: QueryHistoryRepository,
    saved_queries_repo: SavedQueriesRepository,
    recent_items_repo: RecentItemsRepository,
    // Cached in-memory entries for fast reads
    history_entries: Vec<HistoryEntry>,
    saved_queries: Vec<SavedQuery>,
    recent_files: Vec<RecentFile>,
    max_history_entries: usize,
}

impl HistoryManager {
    /// Creates a manager backed by `state.db` repositories.
    ///
    /// If initialization fails (e.g., fresh DB), falls back to empty state.
    pub fn new(runtime: &StorageRuntime) -> Self {
        let history_repo = runtime.query_history();
        let saved_queries_repo = runtime.saved_queries();
        let recent_items_repo = runtime.recent_items();

        let history_entries = Self::load_history(&history_repo);
        let saved_queries = Self::load_saved_queries(&saved_queries_repo);
        let recent_files = Self::load_recent_files(&recent_items_repo);

        info!(
            "Loaded {} history entries, {} saved queries, {} recent files from state.db",
            history_entries.len(),
            saved_queries.len(),
            recent_files.len()
        );

        Self {
            history_repo,
            saved_queries_repo,
            recent_items_repo,
            history_entries,
            saved_queries,
            recent_files,
            max_history_entries: 1000,
        }
    }

    pub fn set_max_entries(&mut self, max: usize) {
        self.max_history_entries = max.max(10);
    }

    pub fn entries(&self) -> &[HistoryEntry] {
        &self.history_entries
    }

    pub fn add(&mut self, entry: HistoryEntry) {
        let dto = query_history_dto_from_entry(&entry);
        self.history_entries.insert(0, entry);

        const MAX_IN_MEMORY: usize = 500;
        if self.history_entries.len() > MAX_IN_MEMORY {
            self.history_entries.truncate(MAX_IN_MEMORY);
        }

        if let Err(e) = self.history_repo.add(&dto) {
            error!("Failed to save history entry: {:?}", e);
        }
    }

    pub fn toggle_favorite(&mut self, id: Uuid) -> bool {
        if let Some(entry) = self.history_entries.iter_mut().find(|e| e.id == id) {
            entry.is_favorite = !entry.is_favorite;
        }

        if let Ok(new_state) = self.history_repo.toggle_favorite(&id.to_string()) {
            if let Some(entry) = self.history_entries.iter_mut().find(|e| e.id == id) {
                entry.is_favorite = new_state;
            }
            return new_state;
        }
        false
    }

    pub fn remove(&mut self, id: Uuid) {
        self.history_entries.retain(|e| e.id != id);
        if let Err(e) = self.history_repo.remove(&id.to_string()) {
            error!("Failed to remove history entry: {:?}", e);
        }
    }

    // --- Saved Queries ---

    pub fn add_saved_query(&mut self, query: SavedQuery) {
        self.saved_queries.insert(0, query.clone());

        const MAX_IN_MEMORY: usize = 500;
        if self.saved_queries.len() > MAX_IN_MEMORY {
            self.saved_queries.truncate(MAX_IN_MEMORY);
        }

        let dto = saved_query_dto_from_query(&query);
        if let Err(e) = self.saved_queries_repo.insert(&dto) {
            error!("Failed to save query: {:?}", e);
        }
    }

    pub fn update_saved_query(&mut self, id: Uuid, name: String, sql: String) -> bool {
        if let Some(entry) = self.saved_queries.iter_mut().find(|e| e.id == id) {
            entry.name = name.clone();
            entry.sql = sql.clone();

            let dto = saved_query_dto_from_query(entry);
            if let Err(e) = self.saved_queries_repo.update(&dto) {
                error!("Failed to update saved query: {:?}", e);
                return false;
            }
            return true;
        }
        false
    }

    pub fn remove_saved_query(&mut self, id: Uuid) -> bool {
        let before = self.saved_queries.len();
        self.saved_queries.retain(|e| e.id != id);
        let removed = self.saved_queries.len() != before;

        if removed {
            if let Err(e) = self.saved_queries_repo.delete(&id.to_string()) {
                error!("Failed to remove saved query: {:?}", e);
            }
        }
        removed
    }

    pub fn toggle_saved_query_favorite(&mut self, id: Uuid) -> bool {
        if let Some(entry) = self.saved_queries.iter_mut().find(|e| e.id == id) {
            entry.is_favorite = !entry.is_favorite;
        }

        if let Ok(new_state) = self.saved_queries_repo.toggle_favorite(&id.to_string()) {
            if let Some(entry) = self.saved_queries.iter_mut().find(|e| e.id == id) {
                entry.is_favorite = new_state;
            }
            return new_state;
        }
        false
    }

    pub fn update_saved_query_last_used(&mut self, id: Uuid) -> bool {
        if let Some(entry) = self.saved_queries.iter_mut().find(|e| e.id == id) {
            entry.last_used_at = Utc::now().timestamp();
        }

        if let Err(e) = self.saved_queries_repo.touch(&id.to_string()) {
            error!("Failed to touch saved query: {:?}", e);
            return false;
        }
        true
    }

    pub fn update_saved_query_sql(&mut self, id: Uuid, sql: &str) -> bool {
        if let Some(entry) = self.saved_queries.iter_mut().find(|e| e.id == id) {
            entry.sql = sql.to_string();
            entry.last_used_at = Utc::now().timestamp();

            let dto = saved_query_dto_from_query(entry);
            if let Err(e) = self.saved_queries_repo.update(&dto) {
                error!("Failed to update saved query SQL: {:?}", e);
                return false;
            }
            return true;
        }
        false
    }

    pub fn update_saved_query_name(&mut self, id: Uuid, name: &str) -> bool {
        if let Some(entry) = self.saved_queries.iter_mut().find(|e| e.id == id) {
            entry.name = name.to_string();

            let dto = saved_query_dto_from_query(entry);
            if let Err(e) = self.saved_queries_repo.update(&dto) {
                error!("Failed to update saved query name: {:?}", e);
                return false;
            }
            return true;
        }
        false
    }

    pub fn get_saved_query(&self, id: Uuid) -> Option<&SavedQuery> {
        self.saved_queries.iter().find(|e| e.id == id)
    }

    pub fn saved_queries_list(&self) -> &[SavedQuery] {
        &self.saved_queries
    }

    // --- Recent Files (delegated) ---

    pub fn recent_files_entries(&self) -> &[RecentFile] {
        &self.recent_files
    }

    pub fn record_recent_file(&mut self, path: std::path::PathBuf) {
        let path_str = path.to_string_lossy().to_string();
        let stable_id = Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, path_str.as_bytes());
        let title = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        self.recent_files.retain(|e| &e.path != &path);

        self.recent_files.insert(
            0,
            RecentFile {
                path: path.clone(),
                last_opened: Utc::now().timestamp(),
            },
        );

        const MAX_RECENT: usize = 30;
        if self.recent_files.len() > MAX_RECENT {
            self.recent_files.truncate(MAX_RECENT);
        }

        let dto = RecentItemDto::file(stable_id, path_str, title);
        if let Err(e) = self.recent_items_repo.record_access(&dto) {
            error!("Failed to record recent file: {:?}", e);
        }
    }

    pub fn remove_recent_file(&mut self, path: &std::path::PathBuf) {
        let path_str = path.to_string_lossy().to_string();
        let stable_id = Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, path_str.as_bytes());
        self.recent_files.retain(|e| &e.path != path);
        if let Err(e) = self.recent_items_repo.remove(&stable_id.to_string()) {
            error!("Failed to remove recent file from repo: {:?}", e);
        }
    }

    // --- Helpers ---

    fn load_history(repo: &QueryHistoryRepository) -> Vec<HistoryEntry> {
        match repo.recent(500) {
            Ok(entries) => entries
                .into_iter()
                .map(|dto| {
                    let id = Uuid::parse_str(&dto.id).unwrap_or_else(|_| Uuid::new_v4());
                    let duration = dto.duration_ms.map(|ms| Duration::from_millis(ms as u64));
                    let timestamp = parse_rfc3339(&dto.executed_at);
                    HistoryEntry {
                        id,
                        sql: dto.query_text,
                        timestamp,
                        database: dto.database_name,
                        connection_name: dto.connection_profile_id,
                        execution_time_ms: duration.map(|d| d.as_millis() as u64).unwrap_or(0),
                        row_count: dto.row_count.map(|n| n as usize),
                        is_favorite: dto.is_favorite,
                    }
                })
                .collect(),
            Err(e) => {
                log::warn!("Failed to load history from state.db: {}", e);
                Vec::new()
            }
        }
    }

    fn load_saved_queries(repo: &SavedQueriesRepository) -> Vec<SavedQuery> {
        match repo.all() {
            Ok(entries) => entries
                .into_iter()
                .map(|dto| {
                    let id = Uuid::parse_str(&dto.id).unwrap_or_else(|_| Uuid::new_v4());
                    let connection_id = dto
                        .connection_id
                        .as_ref()
                        .and_then(|s| Uuid::parse_str(s).ok());
                    let created_at = parse_rfc3339(&dto.created_at);
                    let last_used_at = parse_rfc3339(&dto.last_used_at);
                    dbflux_core::SavedQuery {
                        id,
                        name: dto.name,
                        sql: dto.sql,
                        is_favorite: dto.is_favorite,
                        connection_id,
                        created_at,
                        last_used_at,
                    }
                })
                .collect(),
            Err(e) => {
                log::warn!("Failed to load saved queries from state.db: {}", e);
                Vec::new()
            }
        }
    }

    fn load_recent_files(repo: &RecentItemsRepository) -> Vec<RecentFile> {
        match repo.all() {
            Ok(entries) => entries
                .into_iter()
                .filter_map(|dto| {
                    let path = dto.path.as_ref().map(std::path::PathBuf::from)?;
                    if path.as_os_str().is_empty() {
                        return None;
                    }
                    Some(RecentFile {
                        path,
                        last_opened: parse_rfc3339(&dto.accessed_at),
                    })
                })
                .collect(),
            Err(e) => {
                log::warn!("Failed to load recent files from state.db: {}", e);
                Vec::new()
            }
        }
    }
}

/// Parses an RFC3339 timestamp string to a Unix timestamp (i64).
/// Falls back to current time if parsing fails.
fn parse_rfc3339(rfc3339: &str) -> i64 {
    if rfc3339.is_empty() {
        return Utc::now().timestamp();
    }
    dbflux_core::chrono::DateTime::parse_from_rfc3339(rfc3339)
        .map(|dt| dt.with_timezone(&Utc).timestamp())
        .unwrap_or_else(|_| Utc::now().timestamp())
}

/// Converts a Unix timestamp (i64) to an RFC3339 string.
fn chrono_utc_to_rfc3339(timestamp: i64) -> String {
    use std::time::UNIX_EPOCH;
    let duration = UNIX_EPOCH + std::time::Duration::from_secs(timestamp.max(0) as u64);
    dbflux_core::chrono::DateTime::<Utc>::from(duration).to_rfc3339()
}

fn query_history_dto_from_entry(
    entry: &HistoryEntry,
) -> dbflux_storage::repositories::state::query_history::QueryHistoryDto {
    dbflux_storage::repositories::state::query_history::QueryHistoryDto {
        id: entry.id.to_string(),
        connection_profile_id: None,
        driver_id: None,
        database_name: entry.database.clone(),
        query_text: entry.sql.clone(),
        query_kind: "select".to_string(),
        executed_at: Utc::now().to_rfc3339(),
        duration_ms: Some(entry.execution_time_ms as i64),
        succeeded: true,
        error_summary: None,
        row_count: entry.row_count.map(|n| n as i64),
        is_favorite: entry.is_favorite,
    }
}

fn saved_query_dto_from_query(
    query: &SavedQuery,
) -> dbflux_storage::repositories::state::saved_queries::SavedQueryDto {
    dbflux_storage::repositories::state::saved_queries::SavedQueryDto {
        id: query.id.to_string(),
        folder_id: None,
        name: query.name.clone(),
        sql: query.sql.clone(),
        is_favorite: query.is_favorite,
        connection_id: query.connection_id.map(|u| u.to_string()),
        created_at: chrono_utc_to_rfc3339(query.created_at),
        last_used_at: chrono_utc_to_rfc3339(query.last_used_at),
    }
}
