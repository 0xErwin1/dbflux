use crate::DbError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Stable identifier for a registered driver.
///
/// Built-in drivers use `"builtin:<name>"` (e.g. `"builtin:redis"`).
/// External RPC drivers use `"rpc:<socket_id>"`.
pub type DriverKey = String;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_config_version")]
    pub version: u32,

    #[serde(default)]
    pub services: Vec<ServiceConfig>,

    #[serde(default)]
    pub general: GeneralSettings,

    /// Per-driver overrides for global settings (refresh policy, safety, etc.).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub driver_overrides: HashMap<DriverKey, GlobalOverrides>,

    /// Per-driver settings from driver-owned schemas (scan batch size, etc.).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub driver_settings: HashMap<DriverKey, crate::FormValues>,
}

fn default_config_version() -> u32 {
    1
}

// ---------------------------------------------------------------------------
// GlobalOverrides
// ---------------------------------------------------------------------------

/// Subset of global settings that can be overridden per driver.
///
/// Each field is `Option`: `None` means "use the global default",
/// `Some(value)` means "override with this value for this driver".
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GlobalOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_policy: Option<RefreshPolicySetting>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval_secs: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm_dangerous: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires_where: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires_preview: Option<bool>,
}

impl GlobalOverrides {
    pub fn is_empty(&self) -> bool {
        self.refresh_policy.is_none()
            && self.refresh_interval_secs.is_none()
            && self.confirm_dangerous.is_none()
            && self.requires_where.is_none()
            && self.requires_preview.is_none()
    }
}

// ---------------------------------------------------------------------------
// EffectiveSettings
// ---------------------------------------------------------------------------

/// Resolved settings snapshot: global defaults merged with per-driver overrides
/// and driver-owned settings from the schema.
#[derive(Debug, Clone)]
pub struct EffectiveSettings {
    pub refresh_policy: RefreshPolicySetting,
    pub refresh_interval_secs: u32,
    pub confirm_dangerous: bool,
    pub requires_where: bool,
    pub requires_preview: bool,

    /// Driver-owned settings from its settings schema.
    pub driver_values: crate::FormValues,
}

impl EffectiveSettings {
    pub fn resolve(
        global: &GeneralSettings,
        overrides: Option<&GlobalOverrides>,
        driver_values: &crate::FormValues,
    ) -> Self {
        let (
            refresh_policy,
            refresh_interval_secs,
            confirm_dangerous,
            requires_where,
            requires_preview,
        ) = match overrides {
            Some(ov) => (
                ov.refresh_policy.unwrap_or(global.default_refresh_policy),
                ov.refresh_interval_secs
                    .unwrap_or(global.default_refresh_interval_secs),
                ov.confirm_dangerous
                    .unwrap_or(global.confirm_dangerous_queries),
                ov.requires_where.unwrap_or(global.dangerous_requires_where),
                ov.requires_preview
                    .unwrap_or(global.dangerous_requires_preview),
            ),
            None => (
                global.default_refresh_policy,
                global.default_refresh_interval_secs,
                global.confirm_dangerous_queries,
                global.dangerous_requires_where,
                global.dangerous_requires_preview,
            ),
        };

        Self {
            refresh_policy,
            refresh_interval_secs,
            confirm_dangerous,
            requires_where,
            requires_preview,
            driver_values: driver_values.clone(),
        }
    }

    pub fn resolve_refresh_policy(&self) -> crate::RefreshPolicy {
        match self.refresh_policy {
            RefreshPolicySetting::Manual => crate::RefreshPolicy::Manual,
            RefreshPolicySetting::Interval => crate::RefreshPolicy::Interval {
                every_secs: self.refresh_interval_secs,
            },
        }
    }
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemeSetting {
    #[default]
    Dark,
    Light,
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

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // GlobalOverrides
    // =========================================================================

    #[test]
    fn global_overrides_default_is_empty() {
        let ov = GlobalOverrides::default();
        assert!(ov.is_empty());
    }

    #[test]
    fn global_overrides_is_not_empty_when_any_field_set() {
        let cases: Vec<GlobalOverrides> = vec![
            GlobalOverrides {
                refresh_policy: Some(RefreshPolicySetting::Interval),
                ..Default::default()
            },
            GlobalOverrides {
                refresh_interval_secs: Some(10),
                ..Default::default()
            },
            GlobalOverrides {
                confirm_dangerous: Some(false),
                ..Default::default()
            },
            GlobalOverrides {
                requires_where: Some(true),
                ..Default::default()
            },
            GlobalOverrides {
                requires_preview: Some(true),
                ..Default::default()
            },
        ];

        for (i, ov) in cases.iter().enumerate() {
            assert!(!ov.is_empty(), "case {} should not be empty", i);
        }
    }

    #[test]
    fn global_overrides_serde_roundtrip() {
        let ov = GlobalOverrides {
            refresh_policy: Some(RefreshPolicySetting::Interval),
            refresh_interval_secs: Some(30),
            confirm_dangerous: None,
            requires_where: Some(false),
            requires_preview: None,
        };

        let json = serde_json::to_string(&ov).unwrap();
        let deserialized: GlobalOverrides = serde_json::from_str(&json).unwrap();

        assert_eq!(ov, deserialized);
    }

    #[test]
    fn global_overrides_skips_none_fields_in_json() {
        let ov = GlobalOverrides {
            refresh_policy: Some(RefreshPolicySetting::Manual),
            ..Default::default()
        };

        let json = serde_json::to_string(&ov).unwrap();

        assert!(json.contains("refresh_policy"));
        assert!(!json.contains("refresh_interval_secs"));
        assert!(!json.contains("confirm_dangerous"));
        assert!(!json.contains("requires_where"));
        assert!(!json.contains("requires_preview"));
    }

    #[test]
    fn global_overrides_deserializes_from_empty_object() {
        let ov: GlobalOverrides = serde_json::from_str("{}").unwrap();
        assert!(ov.is_empty());
    }

    // =========================================================================
    // EffectiveSettings::resolve
    // =========================================================================

    fn test_global() -> GeneralSettings {
        GeneralSettings {
            default_refresh_policy: RefreshPolicySetting::Manual,
            default_refresh_interval_secs: 5,
            confirm_dangerous_queries: true,
            dangerous_requires_where: true,
            dangerous_requires_preview: false,
            ..Default::default()
        }
    }

    #[test]
    fn effective_settings_uses_global_defaults_when_no_overrides() {
        let global = test_global();
        let values = HashMap::new();

        let effective = EffectiveSettings::resolve(&global, None, &values);

        assert_eq!(effective.refresh_policy, RefreshPolicySetting::Manual);
        assert_eq!(effective.refresh_interval_secs, 5);
        assert!(effective.confirm_dangerous);
        assert!(effective.requires_where);
        assert!(!effective.requires_preview);
        assert!(effective.driver_values.is_empty());
    }

    #[test]
    fn effective_settings_uses_global_when_overrides_are_all_none() {
        let global = test_global();
        let overrides = GlobalOverrides::default();
        let values = HashMap::new();

        let effective = EffectiveSettings::resolve(&global, Some(&overrides), &values);

        assert_eq!(effective.refresh_policy, RefreshPolicySetting::Manual);
        assert_eq!(effective.refresh_interval_secs, 5);
        assert!(effective.confirm_dangerous);
        assert!(effective.requires_where);
        assert!(!effective.requires_preview);
    }

    #[test]
    fn effective_settings_applies_partial_overrides() {
        let global = test_global();
        let overrides = GlobalOverrides {
            refresh_policy: Some(RefreshPolicySetting::Interval),
            refresh_interval_secs: Some(30),
            confirm_dangerous: None,
            requires_where: None,
            requires_preview: Some(true),
        };
        let values = HashMap::new();

        let effective = EffectiveSettings::resolve(&global, Some(&overrides), &values);

        assert_eq!(effective.refresh_policy, RefreshPolicySetting::Interval);
        assert_eq!(effective.refresh_interval_secs, 30);
        // Not overridden — use global defaults
        assert!(effective.confirm_dangerous);
        assert!(effective.requires_where);
        // Overridden
        assert!(effective.requires_preview);
    }

    #[test]
    fn effective_settings_applies_all_overrides() {
        let global = test_global();
        let overrides = GlobalOverrides {
            refresh_policy: Some(RefreshPolicySetting::Interval),
            refresh_interval_secs: Some(60),
            confirm_dangerous: Some(false),
            requires_where: Some(false),
            requires_preview: Some(true),
        };
        let values = HashMap::new();

        let effective = EffectiveSettings::resolve(&global, Some(&overrides), &values);

        assert_eq!(effective.refresh_policy, RefreshPolicySetting::Interval);
        assert_eq!(effective.refresh_interval_secs, 60);
        assert!(!effective.confirm_dangerous);
        assert!(!effective.requires_where);
        assert!(effective.requires_preview);
    }

    #[test]
    fn effective_settings_includes_driver_values() {
        let global = test_global();
        let mut values = HashMap::new();
        values.insert("scan_batch_size".to_string(), "200".to_string());
        values.insert("allow_flush".to_string(), "true".to_string());

        let effective = EffectiveSettings::resolve(&global, None, &values);

        assert_eq!(effective.driver_values.len(), 2);
        assert_eq!(effective.driver_values["scan_batch_size"], "200");
        assert_eq!(effective.driver_values["allow_flush"], "true");
    }

    // =========================================================================
    // EffectiveSettings::resolve_refresh_policy
    // =========================================================================

    #[test]
    fn resolve_refresh_policy_manual() {
        let effective = EffectiveSettings::resolve(&test_global(), None, &HashMap::new());
        assert!(matches!(
            effective.resolve_refresh_policy(),
            crate::RefreshPolicy::Manual
        ));
    }

    #[test]
    fn resolve_refresh_policy_interval() {
        let overrides = GlobalOverrides {
            refresh_policy: Some(RefreshPolicySetting::Interval),
            refresh_interval_secs: Some(15),
            ..Default::default()
        };

        let effective =
            EffectiveSettings::resolve(&test_global(), Some(&overrides), &HashMap::new());

        match effective.resolve_refresh_policy() {
            crate::RefreshPolicy::Interval { every_secs } => assert_eq!(every_secs, 15),
            other => panic!("expected Interval, got {:?}", other),
        }
    }

    // =========================================================================
    // AppConfig serialization / backward compatibility
    // =========================================================================

    #[test]
    fn app_config_deserializes_legacy_json_without_new_fields() {
        let legacy_json = r#"{
            "services": [],
            "general": {
                "confirm_dangerous_queries": true
            }
        }"#;

        let config: AppConfig = serde_json::from_str(legacy_json).unwrap();

        assert_eq!(config.version, 1);
        assert!(config.driver_overrides.is_empty());
        assert!(config.driver_settings.is_empty());
        assert!(config.general.confirm_dangerous_queries);
    }

    #[test]
    fn app_config_roundtrip_with_driver_overrides_and_settings() {
        let mut config = AppConfig::default();
        config.version = 2;
        config.driver_overrides.insert(
            "builtin:redis".to_string(),
            GlobalOverrides {
                confirm_dangerous: Some(false),
                ..Default::default()
            },
        );

        let mut redis_settings = HashMap::new();
        redis_settings.insert("scan_batch_size".to_string(), "500".to_string());
        config
            .driver_settings
            .insert("builtin:redis".to_string(), redis_settings);

        let json = serde_json::to_string_pretty(&config).unwrap();
        let restored: AppConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.version, 2);
        assert_eq!(restored.driver_overrides.len(), 1);
        assert_eq!(
            restored.driver_overrides["builtin:redis"].confirm_dangerous,
            Some(false)
        );
        assert_eq!(
            restored.driver_settings["builtin:redis"]["scan_batch_size"],
            "500"
        );
    }

    #[test]
    fn app_config_omits_empty_driver_maps_in_json() {
        let config = AppConfig::default();
        let json = serde_json::to_string(&config).unwrap();

        assert!(!json.contains("driver_overrides"));
        assert!(!json.contains("driver_settings"));
    }

    #[test]
    fn app_config_default_version_is_one() {
        let config = AppConfig::default();
        assert_eq!(config.version, 0); // Default::default() gives u32 default (0)

        // But deserialization uses the serde default function
        let config: AppConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.version, 1);
    }
}
