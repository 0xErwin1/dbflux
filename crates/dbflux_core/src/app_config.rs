use crate::DbError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub rpc_services: Vec<RpcServiceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcServiceConfig {
    pub socket_id: String,

    #[serde(default)]
    pub command: Option<String>,

    #[serde(default)]
    pub args: Vec<String>,

    #[serde(default)]
    pub env: HashMap<String, String>,

    #[serde(default)]
    pub startup_timeout_ms: Option<u64>,
}

pub struct AppConfigStore {
    path: PathBuf,
}

impl AppConfigStore {
    pub fn new() -> Result<Self, DbError> {
        let config_dir = dirs::config_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find config directory"))
        })?;

        let app_dir = config_dir.join("dbflux");
        fs::create_dir_all(&app_dir).map_err(DbError::IoError)?;

        Ok(Self {
            path: app_dir.join("config.json"),
        })
    }

    pub fn load(&self) -> Result<AppConfig, DbError> {
        if !self.path.exists() {
            return Ok(AppConfig::default());
        }

        let content = fs::read_to_string(&self.path).map_err(DbError::IoError)?;
        let config: AppConfig =
            serde_json::from_str(&content).map_err(|e| DbError::InvalidProfile(e.to_string()))?;

        Ok(config)
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}
