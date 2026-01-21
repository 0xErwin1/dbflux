use crate::{ConnectionProfile, DbError};
use std::fs;
use std::path::PathBuf;

pub struct ProfileStore {
    path: PathBuf,
}

impl ProfileStore {
    pub fn new() -> Result<Self, DbError> {
        let config_dir = dirs::config_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find config directory"))
        })?;

        let app_dir = config_dir.join("dbflux");
        fs::create_dir_all(&app_dir).map_err(DbError::IoError)?;

        Ok(Self {
            path: app_dir.join("profiles.json"),
        })
    }

    pub fn load(&self) -> Result<Vec<ConnectionProfile>, DbError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&self.path).map_err(DbError::IoError)?;
        let profiles: Vec<ConnectionProfile> =
            serde_json::from_str(&content).map_err(|e| DbError::InvalidProfile(e.to_string()))?;

        Ok(profiles)
    }

    pub fn save(&self, profiles: &[ConnectionProfile]) -> Result<(), DbError> {
        let content = serde_json::to_string_pretty(profiles)
            .map_err(|e| DbError::InvalidProfile(e.to_string()))?;

        fs::write(&self.path, content).map_err(DbError::IoError)?;

        Ok(())
    }
}

impl Default for ProfileStore {
    fn default() -> Self {
        Self::new().expect("Failed to create profile store")
    }
}
