//! Persisted update preferences and first-run bookkeeping.

use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::error::StorageError;
use dbflux_storage::repositories::update_settings::UpdateSettingsDto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateSettings {
    /// Ask GitHub for a newer release when the app starts.
    pub check_for_updates_on_startup: bool,
    /// Open the "What is new" dialog on the first start of a newer version.
    pub show_whats_new_after_update: bool,
    /// Version of the previous start, `None` before the first one.
    pub last_run_version: Option<String>,
    /// Release label the user chose to skip; its notice stays hidden.
    pub skipped_update_version: Option<String>,
    /// Whether the first-run welcome dialog was already shown.
    pub welcome_shown: bool,
}

impl Default for UpdateSettings {
    fn default() -> Self {
        Self {
            check_for_updates_on_startup: true,
            show_whats_new_after_update: true,
            last_run_version: None,
            skipped_update_version: None,
            welcome_shown: false,
        }
    }
}

impl UpdateSettings {
    /// The two user preferences restored to their defaults, keeping the
    /// first-run bookkeeping untouched.
    pub fn with_default_preferences(&self) -> Self {
        let defaults = Self::default();

        Self {
            check_for_updates_on_startup: defaults.check_for_updates_on_startup,
            show_whats_new_after_update: defaults.show_whats_new_after_update,
            ..self.clone()
        }
    }

    fn from_dto(dto: UpdateSettingsDto) -> Self {
        Self {
            check_for_updates_on_startup: dto.check_for_updates_on_startup != 0,
            show_whats_new_after_update: dto.show_whats_new_after_update != 0,
            last_run_version: dto.last_run_version.filter(|value| !value.is_empty()),
            skipped_update_version: dto.skipped_update_version.filter(|value| !value.is_empty()),
            welcome_shown: dto.welcome_shown != 0,
        }
    }

    fn to_dto(&self) -> UpdateSettingsDto {
        UpdateSettingsDto {
            check_for_updates_on_startup: i32::from(self.check_for_updates_on_startup),
            show_whats_new_after_update: i32::from(self.show_whats_new_after_update),
            last_run_version: self.last_run_version.clone(),
            skipped_update_version: self.skipped_update_version.clone(),
            welcome_shown: i32::from(self.welcome_shown),
        }
    }
}

/// Loads the update settings, falling back to the defaults when the row is
/// missing or unreadable.
pub fn load_update_settings(runtime: &StorageRuntime) -> UpdateSettings {
    match runtime.update_settings().get() {
        Ok(Some(dto)) => UpdateSettings::from_dto(dto),
        Ok(None) => UpdateSettings::default(),
        Err(error) => {
            log::warn!("Failed to load update settings, using defaults: {error}");
            UpdateSettings::default()
        }
    }
}

pub fn save_update_settings(
    runtime: &StorageRuntime,
    settings: &UpdateSettings,
) -> Result<(), StorageError> {
    runtime.update_settings().upsert(&settings.to_dto())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_database_loads_the_defaults() {
        let runtime = StorageRuntime::in_memory().expect("in-memory storage");

        assert_eq!(load_update_settings(&runtime), UpdateSettings::default());
    }

    #[test]
    fn settings_round_trip_through_storage() {
        let runtime = StorageRuntime::in_memory().expect("in-memory storage");
        let written = UpdateSettings {
            check_for_updates_on_startup: false,
            show_whats_new_after_update: false,
            last_run_version: Some("0.8.0".to_string()),
            skipped_update_version: Some("nightly abc1234".to_string()),
            welcome_shown: true,
        };

        save_update_settings(&runtime, &written).expect("save");

        assert_eq!(load_update_settings(&runtime), written);
    }

    #[test]
    fn default_preferences_keep_the_bookkeeping() {
        let customized = UpdateSettings {
            check_for_updates_on_startup: false,
            show_whats_new_after_update: false,
            last_run_version: Some("0.8.0".to_string()),
            skipped_update_version: Some("0.8.1".to_string()),
            welcome_shown: true,
        };

        let reset = customized.with_default_preferences();

        assert!(reset.check_for_updates_on_startup);
        assert!(reset.show_whats_new_after_update);
        assert_eq!(reset.last_run_version.as_deref(), Some("0.8.0"));
        assert_eq!(reset.skipped_update_version.as_deref(), Some("0.8.1"));
        assert!(reset.welcome_shown);
    }
}
