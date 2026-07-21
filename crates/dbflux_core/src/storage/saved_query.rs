use chrono::{DateTime, Local, TimeZone, Utc};
use serde::{Deserialize, Serialize};
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
