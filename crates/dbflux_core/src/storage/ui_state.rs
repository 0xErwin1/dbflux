use crate::DbError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

const STATE_FILE: &str = "state.json";

/// Persisted UI state that lives in `~/.local/share/dbflux/state.json`.
///
/// This is for ephemeral layout preferences (collapse state, scroll positions,
/// window geometry) — not user configuration (which belongs in config.json).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UiState {
    #[serde(default)]
    pub settings_collapsed_security: bool,

    #[serde(default)]
    pub settings_collapsed_network: bool,

    #[serde(default)]
    pub settings_collapsed_connection: bool,
}

/// Reads and writes `state.json` from the XDG data directory.
pub struct UiStateStore {
    path: PathBuf,
}

impl UiStateStore {
    pub fn new() -> Result<Self, DbError> {
        let data_dir = dirs::data_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find data directory"))
        })?;

        let root = data_dir.join("dbflux");
        fs::create_dir_all(&root).map_err(DbError::IoError)?;

        Ok(Self {
            path: root.join(STATE_FILE),
        })
    }

    #[cfg(test)]
    pub fn from_path(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<UiState, DbError> {
        if !self.path.exists() {
            return Ok(UiState::default());
        }

        let content = fs::read_to_string(&self.path).map_err(DbError::IoError)?;

        serde_json::from_str(&content)
            .map_err(|e| DbError::IoError(std::io::Error::other(e.to_string())))
    }

    pub fn save(&self, state: &UiState) -> Result<(), DbError> {
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| DbError::IoError(std::io::Error::other(e.to_string())))?;

        fs::write(&self.path, json).map_err(DbError::IoError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn load_missing_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        let store = UiStateStore::from_path(dir.path().join("state.json"));

        let state = store.load().unwrap();
        assert!(!state.settings_collapsed_security);
        assert!(!state.settings_collapsed_network);
        assert!(!state.settings_collapsed_connection);
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = UiStateStore::from_path(dir.path().join("state.json"));

        let state = UiState {
            settings_collapsed_security: true,
            settings_collapsed_network: true,
            settings_collapsed_connection: false,
        };

        store.save(&state).unwrap();
        let loaded = store.load().unwrap();

        assert!(loaded.settings_collapsed_security);
        assert!(loaded.settings_collapsed_network);
        assert!(!loaded.settings_collapsed_connection);
    }

    #[test]
    fn load_ignores_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        fs::write(
            &path,
            r#"{"settings_collapsed_network": true, "future_field": 42}"#,
        )
        .unwrap();

        let store = UiStateStore::from_path(path);
        let state = store.load().unwrap();

        assert!(!state.settings_collapsed_security);
        assert!(state.settings_collapsed_network);
        assert!(!state.settings_collapsed_connection);
    }
}
