use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecentFile {
    pub path: PathBuf,
    pub last_opened: i64,
}
