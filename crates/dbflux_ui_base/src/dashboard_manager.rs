//! `DashboardManager` — SQLite-backed manager for `Dashboard` and
//! `DashboardPanel` records.
//!
//! Wraps `DashboardsRepository` and `DashboardPanelsRepository` from
//! `dbflux_storage`. Keeps in-memory caches for synchronous reads.
//! All writes go to the repository first; caches are updated only on success.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use dbflux_components::{SavedChartRefreshPolicy, TimeRangePreset};
use dbflux_storage::{
    error::StorageError,
    repositories::viz_dashboard_panels::{DashboardPanelDto, DashboardPanelsRepository},
    repositories::viz_dashboards::{DashboardDto, DashboardsRepository},
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// In-memory domain record for a dashboard.
#[derive(Debug, Clone)]
pub struct Dashboard {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub profile_id: Option<Uuid>,
    pub shared_time_range_preset: Option<TimeRangePreset>,
    pub shared_refresh_policy: SavedChartRefreshPolicy,
    pub grid_columns: u32,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
}

/// In-memory domain record for one panel slot in a dashboard.
#[derive(Debug, Clone)]
pub struct DashboardPanel {
    pub dashboard_id: Uuid,
    pub panel_index: u32,
    pub saved_chart_id: Uuid,
    pub title_override: Option<String>,
    pub grid_row: u32,
    pub grid_column: u32,
    pub grid_width: u32,
    pub grid_height: u32,
}

// ---------------------------------------------------------------------------
// DTO ↔ domain conversions
// ---------------------------------------------------------------------------

fn dto_to_dashboard(dto: DashboardDto) -> Result<Dashboard, StorageError> {
    let id = Uuid::parse_str(&dto.id)
        .map_err(|e| StorageError::Data(format!("invalid dashboard id '{}': {e}", dto.id)))?;

    let profile_id = dto
        .profile_id
        .as_deref()
        .map(|s| {
            Uuid::parse_str(s)
                .map_err(|e| StorageError::Data(format!("invalid profile_id '{s}': {e}")))
        })
        .transpose()?;

    let created_at = Utc
        .timestamp_millis_opt(dto.created_at)
        .single()
        .ok_or_else(|| StorageError::Data(format!("invalid created_at: {}", dto.created_at)))?;

    let updated_at = Utc
        .timestamp_millis_opt(dto.updated_at)
        .single()
        .ok_or_else(|| StorageError::Data(format!("invalid updated_at: {}", dto.updated_at)))?;

    let shared_time_range_preset = dto
        .shared_time_range_preset
        .as_deref()
        .map(parse_time_range_preset)
        .transpose()?;

    let shared_refresh_policy = parse_refresh_policy(
        &dto.shared_refresh_policy_kind,
        dto.shared_refresh_policy_interval_secs,
    )?;

    Ok(Dashboard {
        id,
        name: dto.name,
        description: dto.description,
        profile_id,
        shared_time_range_preset,
        shared_refresh_policy,
        grid_columns: dto.grid_columns as u32,
        created_at,
        updated_at,
    })
}

fn dashboard_to_dto(dashboard: &Dashboard) -> DashboardDto {
    DashboardDto {
        id: dashboard.id.to_string(),
        name: dashboard.name.clone(),
        description: dashboard.description.clone(),
        profile_id: dashboard.profile_id.map(|u| u.to_string()),
        shared_time_range_preset: dashboard
            .shared_time_range_preset
            .map(time_range_preset_to_str),
        shared_refresh_policy_kind: refresh_policy_kind_to_str(dashboard.shared_refresh_policy),
        shared_refresh_policy_interval_secs: match dashboard.shared_refresh_policy {
            SavedChartRefreshPolicy::Interval { every_secs } => Some(every_secs as i64),
            _ => None,
        },
        grid_columns: dashboard.grid_columns as i64,
        created_at: dashboard.created_at.timestamp_millis(),
        updated_at: dashboard.updated_at.timestamp_millis(),
    }
}

fn dto_to_panel(dto: DashboardPanelDto) -> Result<DashboardPanel, StorageError> {
    let dashboard_id = Uuid::parse_str(&dto.dashboard_id).map_err(|e| {
        StorageError::Data(format!(
            "invalid panel dashboard_id '{}': {e}",
            dto.dashboard_id
        ))
    })?;

    let saved_chart_id = Uuid::parse_str(&dto.saved_chart_id).map_err(|e| {
        StorageError::Data(format!(
            "invalid panel saved_chart_id '{}': {e}",
            dto.saved_chart_id
        ))
    })?;

    Ok(DashboardPanel {
        dashboard_id,
        panel_index: dto.panel_index as u32,
        saved_chart_id,
        title_override: dto.title_override,
        grid_row: dto.grid_row as u32,
        grid_column: dto.grid_column as u32,
        grid_width: dto.grid_width as u32,
        grid_height: dto.grid_height as u32,
    })
}

fn panel_to_dto(panel: &DashboardPanel) -> DashboardPanelDto {
    DashboardPanelDto {
        dashboard_id: panel.dashboard_id.to_string(),
        panel_index: panel.panel_index as i64,
        saved_chart_id: panel.saved_chart_id.to_string(),
        title_override: panel.title_override.clone(),
        grid_row: panel.grid_row as i64,
        grid_column: panel.grid_column as i64,
        grid_width: panel.grid_width as i64,
        grid_height: panel.grid_height as i64,
    }
}

// ---------------------------------------------------------------------------
// Enum string serializers/parsers (shared subset with saved_chart_manager)
// ---------------------------------------------------------------------------

fn parse_time_range_preset(s: &str) -> Result<TimeRangePreset, StorageError> {
    match s {
        "last_15_min" => Ok(TimeRangePreset::Last15min),
        "last_hour" => Ok(TimeRangePreset::LastHour),
        "last_6_hours" => Ok(TimeRangePreset::Last6Hours),
        "last_24_hours" => Ok(TimeRangePreset::Last24Hours),
        "last_7_days" => Ok(TimeRangePreset::Last7Days),
        other => Err(StorageError::Data(format!(
            "unknown time_range_preset: '{other}'"
        ))),
    }
}

fn time_range_preset_to_str(p: TimeRangePreset) -> String {
    match p {
        TimeRangePreset::Last15min => "last_15_min",
        TimeRangePreset::LastHour => "last_hour",
        TimeRangePreset::Last6Hours => "last_6_hours",
        TimeRangePreset::Last24Hours => "last_24_hours",
        TimeRangePreset::Last7Days => "last_7_days",
    }
    .to_string()
}

fn parse_refresh_policy(
    kind: &str,
    interval_secs: Option<i64>,
) -> Result<SavedChartRefreshPolicy, StorageError> {
    match kind {
        "off" => Ok(SavedChartRefreshPolicy::Off),
        "interval" => {
            let secs = interval_secs.ok_or_else(|| {
                StorageError::Data(
                    "refresh_policy_kind = 'interval' but interval_secs is NULL".to_string(),
                )
            })?;
            Ok(SavedChartRefreshPolicy::Interval {
                every_secs: secs as u32,
            })
        }
        "on_open" => Ok(SavedChartRefreshPolicy::OnOpen),
        other => Err(StorageError::Data(format!(
            "unknown refresh_policy_kind: '{other}'"
        ))),
    }
}

fn refresh_policy_kind_to_str(p: SavedChartRefreshPolicy) -> String {
    match p {
        SavedChartRefreshPolicy::Off => "off",
        SavedChartRefreshPolicy::Interval { .. } => "interval",
        SavedChartRefreshPolicy::OnOpen => "on_open",
    }
    .to_string()
}

// ---------------------------------------------------------------------------
// DashboardManager
// ---------------------------------------------------------------------------

/// In-memory manager for `Dashboard` and `DashboardPanel` records.
///
/// Dashboards and their panels are loaded eagerly on construction. Writes go
/// through the repositories first; caches are updated only on success.
pub struct DashboardManager {
    dashboards: Vec<Dashboard>,
    panels: HashMap<Uuid, Vec<DashboardPanel>>,
    dashboards_repo: Arc<DashboardsRepository>,
    panels_repo: Arc<DashboardPanelsRepository>,
}

impl DashboardManager {
    /// Load all dashboards and their panels from the repositories.
    pub fn new(
        dashboards_repo: Arc<DashboardsRepository>,
        panels_repo: Arc<DashboardPanelsRepository>,
    ) -> Self {
        let dashboards = match dashboards_repo.list() {
            Ok(dtos) => dtos
                .into_iter()
                .filter_map(|dto| match dto_to_dashboard(dto) {
                    Ok(d) => Some(d),
                    Err(e) => {
                        log::warn!("DashboardManager: skipping dashboard: {e}");
                        None
                    }
                })
                .collect::<Vec<_>>(),
            Err(e) => {
                log::warn!("DashboardManager: failed to load dashboards: {e}; starting empty");
                Vec::new()
            }
        };

        let mut panels: HashMap<Uuid, Vec<DashboardPanel>> = HashMap::new();

        for dashboard in &dashboards {
            match panels_repo.list_for_dashboard(dashboard.id) {
                Ok(dtos) => {
                    let domain_panels: Vec<DashboardPanel> = dtos
                        .into_iter()
                        .filter_map(|dto| match dto_to_panel(dto) {
                            Ok(p) => Some(p),
                            Err(e) => {
                                log::warn!(
                                    "DashboardManager: skipping panel for {}: {e}",
                                    dashboard.id
                                );
                                None
                            }
                        })
                        .collect();
                    panels.insert(dashboard.id, domain_panels);
                }
                Err(e) => {
                    log::warn!(
                        "DashboardManager: failed to load panels for {}: {e}",
                        dashboard.id
                    );
                    panels.insert(dashboard.id, Vec::new());
                }
            }
        }

        Self {
            dashboards,
            panels,
            dashboards_repo,
            panels_repo,
        }
    }

    /// Insert or replace a dashboard by `id`.
    ///
    /// Returns `true` when an existing record was replaced, `false` when a
    /// new record was inserted. Cache updated only on success.
    pub fn upsert_dashboard(&mut self, dashboard: Dashboard) -> bool {
        let dto = dashboard_to_dto(&dashboard);
        let is_update = self.dashboards.iter().any(|d| d.id == dashboard.id);

        match self.dashboards_repo.upsert(&dto) {
            Ok(()) => {
                if let Some(existing) = self.dashboards.iter_mut().find(|d| d.id == dashboard.id) {
                    *existing = dashboard;
                } else {
                    self.dashboards.push(dashboard);
                }
                is_update
            }
            Err(e) => {
                log::error!("DashboardManager: upsert_dashboard failed: {e}");
                is_update
            }
        }
    }

    /// Replace all panels for a dashboard atomically.
    ///
    /// The repository write is attempted first; the in-memory cache is updated
    /// only on success. Returns `Err` when the repository write fails.
    pub fn replace_panels(
        &mut self,
        dashboard_id: Uuid,
        panels: Vec<DashboardPanel>,
    ) -> Result<(), StorageError> {
        let dtos: Vec<DashboardPanelDto> = panels.iter().map(panel_to_dto).collect();

        self.panels_repo
            .replace_panels_for_dashboard(dashboard_id, &dtos)?;

        self.panels.insert(dashboard_id, panels);
        Ok(())
    }

    /// Look up a dashboard by its id.
    pub fn dashboard_by_id(&self, id: Uuid) -> Option<&Dashboard> {
        self.dashboards.iter().find(|d| d.id == id)
    }

    /// All dashboards whose `profile_id` matches the given id.
    pub fn dashboards_for_profile(&self, profile_id: Uuid) -> Vec<&Dashboard> {
        self.dashboards
            .iter()
            .filter(|d| d.profile_id == Some(profile_id))
            .collect()
    }

    /// Panels for the given dashboard, or an empty slice if none loaded.
    pub fn panels_for_dashboard(&self, dashboard_id: Uuid) -> &[DashboardPanel] {
        self.panels
            .get(&dashboard_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Remove a dashboard by id. Returns `true` if a record was removed.
    ///
    /// Cache (dashboards + panels) updated only on success.
    pub fn remove_dashboard(&mut self, id: Uuid) -> bool {
        let was_present = self.dashboards.iter().any(|d| d.id == id);
        if !was_present {
            return false;
        }

        match self.dashboards_repo.delete(id) {
            Ok(()) => {
                self.dashboards.retain(|d| d.id != id);
                self.panels.remove(&id);
                true
            }
            Err(e) => {
                log::error!("DashboardManager: remove_dashboard failed: {e}");
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_storage::{
        bootstrap::StorageRuntime, repositories::viz_dashboard_panels::DashboardPanelsRepository,
        repositories::viz_dashboards::DashboardsRepository,
    };

    fn sample_dashboard(name: &str) -> Dashboard {
        let now = Utc::now();
        Dashboard {
            id: Uuid::new_v4(),
            name: name.to_string(),
            description: None,
            profile_id: None,
            shared_time_range_preset: None,
            shared_refresh_policy: SavedChartRefreshPolicy::Off,
            grid_columns: 12,
            created_at: now,
            updated_at: now,
        }
    }

    fn sample_panel(dashboard_id: Uuid, saved_chart_id: Uuid, index: u32) -> DashboardPanel {
        DashboardPanel {
            dashboard_id,
            panel_index: index,
            saved_chart_id,
            title_override: None,
            grid_row: 0,
            grid_column: index * 4,
            grid_width: 4,
            grid_height: 3,
        }
    }

    /// Design test #31: replace_panels is atomic; cache is updated only on
    /// success.
    ///
    /// We trigger a FK violation by using a dashboard_id that does not match
    /// the panel's dashboard_id in the DTO. However, SQLite FK enforcement
    /// on `viz_dashboard_panels` only checks the `dashboard_id → viz_dashboards`
    /// relationship. A simpler way to force failure is to use `grid_width = 0`
    /// which violates the `CHECK (grid_width >= 1)` constraint.
    #[test]
    fn test_replace_panels_atomic_cache_update() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt.viz_connection();
        let dashboards_repo = Arc::new(DashboardsRepository::new(Arc::clone(&conn)));
        let panels_repo = Arc::new(DashboardPanelsRepository::new(Arc::clone(&conn)));
        let mut manager =
            DashboardManager::new(Arc::clone(&dashboards_repo), Arc::clone(&panels_repo));

        let dashboard = sample_dashboard("test");
        let dashboard_id = dashboard.id;
        manager.upsert_dashboard(dashboard);

        let chart_id1 = Uuid::new_v4();
        let chart_id2 = Uuid::new_v4();

        // Insert 2 valid panels.
        let initial_panels = vec![
            sample_panel(dashboard_id, chart_id1, 0),
            sample_panel(dashboard_id, chart_id2, 1),
        ];
        manager
            .replace_panels(dashboard_id, initial_panels)
            .unwrap();
        assert_eq!(manager.panels_for_dashboard(dashboard_id).len(), 2);

        // Attempt replace with an invalid panel (grid_width = 0 → CHECK violation).
        let bad_panel = DashboardPanel {
            dashboard_id,
            panel_index: 0,
            saved_chart_id: Uuid::new_v4(),
            title_override: None,
            grid_row: 0,
            grid_column: 0,
            grid_width: 0, // violates CHECK (grid_width >= 1)
            grid_height: 3,
        };
        let result = manager.replace_panels(dashboard_id, vec![bad_panel]);

        // The repo write must have failed.
        assert!(result.is_err(), "bad panel must return Err");

        // The in-memory cache must still have the original 2 panels.
        assert_eq!(
            manager.panels_for_dashboard(dashboard_id).len(),
            2,
            "cache must not be updated on failure"
        );
    }
}
