use crate::DbError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub services: Vec<ServiceConfig>,

    #[serde(default)]
    pub general: GeneralSettings,
}

// ---------------------------------------------------------------------------
// GeneralSettings
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeneralSettings {
    // -- Appearance --
    #[serde(default)]
    pub theme: ThemeSetting,

    // -- Startup & Session --
    #[serde(default = "default_true")]
    pub restore_session_on_startup: bool,

    #[serde(default)]
    pub reopen_last_connections: bool,

    #[serde(default = "default_startup_focus")]
    pub default_focus_on_startup: StartupFocus,

    #[serde(default = "default_max_history_entries")]
    pub max_history_entries: usize,

    #[serde(default = "default_auto_save_interval_ms")]
    pub auto_save_interval_ms: u64,

    // -- Refresh & Background --
    #[serde(default = "default_refresh_policy_setting")]
    pub default_refresh_policy: RefreshPolicySetting,

    #[serde(default = "default_refresh_interval_secs")]
    pub default_refresh_interval_secs: u32,

    #[serde(default = "default_max_concurrent_background_tasks")]
    pub max_concurrent_background_tasks: usize,

    #[serde(default = "default_true")]
    pub auto_refresh_pause_on_error: bool,

    #[serde(default)]
    pub auto_refresh_only_if_visible: bool,

    // -- Execution Safety --
    #[serde(default = "default_true")]
    pub confirm_dangerous_queries: bool,

    #[serde(default = "default_true")]
    pub dangerous_requires_where: bool,

    #[serde(default)]
    pub dangerous_requires_preview: bool,

    #[serde(default)]
    pub allow_redis_flush: bool,
}

impl Default for GeneralSettings {
    fn default() -> Self {
        Self {
            theme: ThemeSetting::Dark,
            restore_session_on_startup: true,
            reopen_last_connections: false,
            default_focus_on_startup: StartupFocus::Sidebar,
            max_history_entries: 1000,
            auto_save_interval_ms: 2000,

            default_refresh_policy: RefreshPolicySetting::Manual,
            default_refresh_interval_secs: 5,
            max_concurrent_background_tasks: 8,
            auto_refresh_pause_on_error: true,
            auto_refresh_only_if_visible: false,

            confirm_dangerous_queries: true,
            dangerous_requires_where: true,
            dangerous_requires_preview: false,
            allow_redis_flush: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupFocus {
    Sidebar,
    LastTab,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshPolicySetting {
    Manual,
    Interval,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemeSetting {
    Dark,
    Light,
}

impl Default for ThemeSetting {
    fn default() -> Self {
        ThemeSetting::Dark
    }
}

fn default_true() -> bool {
    true
}

fn default_startup_focus() -> StartupFocus {
    StartupFocus::Sidebar
}

fn default_max_history_entries() -> usize {
    1000
}

fn default_auto_save_interval_ms() -> u64 {
    2000
}

fn default_refresh_policy_setting() -> RefreshPolicySetting {
    RefreshPolicySetting::Manual
}

fn default_refresh_interval_secs() -> u32 {
    5
}

fn default_max_concurrent_background_tasks() -> usize {
    8
}

impl GeneralSettings {
    pub fn resolve_refresh_policy(&self) -> crate::RefreshPolicy {
        match self.default_refresh_policy {
            RefreshPolicySetting::Manual => crate::RefreshPolicy::Manual,
            RefreshPolicySetting::Interval => crate::RefreshPolicy::Interval {
                every_secs: self.default_refresh_interval_secs,
            },
        }
    }

    /// Evaluate a detected dangerous query kind against the safety settings.
    ///
    /// Returns:
    /// - `DangerousAction::Allow` — execute without confirmation
    /// - `DangerousAction::Confirm(kind)` — show the confirmation modal
    /// - `DangerousAction::Block(msg)` — block execution with a hard error
    pub fn evaluate_dangerous(
        &self,
        kind: crate::DangerousQueryKind,
        is_suppressed: bool,
    ) -> DangerousAction {
        use crate::DangerousQueryKind::*;

        if !self.allow_redis_flush && matches!(kind, RedisFlushAll | RedisFlushDb) {
            return DangerousAction::Block(
                "FLUSHALL / FLUSHDB is disabled in settings".to_string(),
            );
        }

        if !self.confirm_dangerous_queries {
            return DangerousAction::Allow;
        }

        // No-where queries aren't dangerous when WHERE isn't required
        if !self.dangerous_requires_where && matches!(kind, DeleteNoWhere | UpdateNoWhere) {
            return DangerousAction::Allow;
        }

        // If preview is forced, ignore suppressions
        if self.dangerous_requires_preview {
            return DangerousAction::Confirm(kind);
        }

        // Otherwise, respect suppressions
        if is_suppressed {
            return DangerousAction::Allow;
        }

        DangerousAction::Confirm(kind)
    }
}

#[derive(Debug, Clone)]
pub enum DangerousAction {
    Allow,
    Confirm(crate::DangerousQueryKind),
    Block(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub socket_id: String,

    #[serde(default = "default_enabled")]
    pub enabled: bool,

    #[serde(default)]
    pub command: Option<String>,

    #[serde(default)]
    pub args: Vec<String>,

    #[serde(default)]
    pub env: HashMap<String, String>,

    #[serde(default)]
    pub startup_timeout_ms: Option<u64>,
}

fn default_enabled() -> bool {
    true
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

    pub fn save(&self, config: &AppConfig) -> Result<(), DbError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(DbError::IoError)?;
        }

        let content = serde_json::to_string_pretty(config)
            .map_err(|e| DbError::InvalidProfile(e.to_string()))?;
        fs::write(&self.path, content).map_err(DbError::IoError)?;

        Ok(())
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}
