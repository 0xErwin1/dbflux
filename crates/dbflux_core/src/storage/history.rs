use serde::{Deserialize, Serialize};
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
