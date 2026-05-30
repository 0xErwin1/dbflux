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

/// Minimal payload for appending a new panel to a dashboard.
///
/// `append_panels` assigns `panel_index`, places the panel on a new row past
/// every existing panel at `grid_column = 0`, sets `grid_width = 12,
/// grid_height = 2` (full-width on the canonical 12-column grid), and sets
/// `title_override = None`.
#[derive(Debug, Clone)]
pub enum DashboardPanelDraft {
    /// A chart slot referencing an existing `SavedChart`.
    Chart { saved_chart_id: Uuid },
    /// An inspector slot driven by an `InstanceInspectorQuery`.
    Inspector { metric_id: String },
}

/// What a `DashboardPanel` displays.
///
/// Dashboards mix two kinds of slots: chart slots that reference a stored
/// `SavedChart`, and divider slots that render a markdown header strip with
/// no chart, no toolbar, no resize affordance. Storage carries the
/// discriminator in `panel_kind` plus optional `divider_markdown`; this enum
/// is the in-memory equivalent.
#[derive(Debug, Clone, PartialEq)]
pub enum DashboardPanelKind {
    /// References a SavedChart via `saved_chart_id`.
    Chart { saved_chart_id: Uuid },
    /// Inline markdown divider rendered as a header strip.
    Divider { markdown: String },
    /// Live-inspector slot driven by `InstanceInspectorQuery`. No chart
    /// reference — the inspector is identified by `metric_id` alone.
    Inspector { metric_id: String },
}

impl DashboardPanelKind {
    /// Returns the saved-chart id when this panel is a chart slot; `None`
    /// for dividers and inspectors. Callers that iterate dashboards looking
    /// for chart references should use this instead of pattern-matching.
    pub fn saved_chart_id(&self) -> Option<Uuid> {
        match self {
            Self::Chart { saved_chart_id } => Some(*saved_chart_id),
            Self::Divider { .. } | Self::Inspector { .. } => None,
        }
    }

    /// `true` when this panel is a divider; helps render code dispatch with
    /// `matches!` instead of full pattern-matching when only the variant
    /// matters.
    pub fn is_divider(&self) -> bool {
        matches!(self, Self::Divider { .. })
    }
}

/// In-memory domain record for one panel slot in a dashboard.
#[derive(Debug, Clone)]
pub struct DashboardPanel {
    pub dashboard_id: Uuid,
    pub panel_index: u32,
    /// Discriminator: chart slot vs markdown divider. Replaces the previous
    /// always-a-chart contract.
    pub kind: DashboardPanelKind,
    pub title_override: Option<String>,
    pub grid_row: u32,
    pub grid_column: u32,
    pub grid_width: u32,
    pub grid_height: u32,
}

impl DashboardPanel {
    /// Returns the saved-chart id for chart panels; `None` for dividers.
    /// Convenience shim that keeps call sites that only want the id one line
    /// short.
    pub fn saved_chart_id(&self) -> Option<Uuid> {
        self.kind.saved_chart_id()
    }
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

    let kind = match dto.panel_kind.as_str() {
        "chart" => {
            let saved_chart_id = Uuid::parse_str(&dto.saved_chart_id).map_err(|e| {
                StorageError::Data(format!(
                    "invalid panel saved_chart_id '{}': {e}",
                    dto.saved_chart_id
                ))
            })?;
            DashboardPanelKind::Chart { saved_chart_id }
        }
        "divider" => DashboardPanelKind::Divider {
            markdown: dto.divider_markdown.unwrap_or_default(),
        },
        "inspector" => {
            let metric_id = dto.inspector_metric_id.ok_or_else(|| {
                StorageError::Data("inspector panel has no inspector_metric_id".to_string())
            })?;
            DashboardPanelKind::Inspector { metric_id }
        }
        other => {
            return Err(StorageError::Data(format!(
                "unknown dashboard panel_kind: '{other}'"
            )));
        }
    };

    Ok(DashboardPanel {
        dashboard_id,
        panel_index: dto.panel_index as u32,
        kind,
        title_override: dto.title_override,
        grid_row: dto.grid_row as u32,
        grid_column: dto.grid_column as u32,
        grid_width: dto.grid_width as u32,
        grid_height: dto.grid_height as u32,
    })
}

fn panel_to_dto(panel: &DashboardPanel) -> DashboardPanelDto {
    let (panel_kind, saved_chart_id, divider_markdown, inspector_metric_id) = match &panel.kind {
        DashboardPanelKind::Chart { saved_chart_id } => {
            ("chart".to_string(), saved_chart_id.to_string(), None, None)
        }
        DashboardPanelKind::Divider { markdown } => (
            "divider".to_string(),
            String::new(),
            Some(markdown.clone()),
            None,
        ),
        DashboardPanelKind::Inspector { metric_id } => (
            "inspector".to_string(),
            String::new(),
            None,
            Some(metric_id.clone()),
        ),
    };

    DashboardPanelDto {
        dashboard_id: panel.dashboard_id.to_string(),
        panel_index: panel.panel_index as i64,
        panel_kind,
        saved_chart_id,
        divider_markdown,
        inspector_metric_id,
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
// Grid helpers
// ---------------------------------------------------------------------------

/// Converts a dense linear `panel_index` to `(grid_row, grid_column)`.
///
/// `grid_columns` is clamped to a minimum of 1 to avoid division by zero.
fn panel_index_to_grid(panel_index: u32, grid_columns: u32) -> (u32, u32) {
    let cols = grid_columns.max(1);
    (panel_index / cols, panel_index % cols)
}

/// Default grid width for a panel appended via `append_panels`.
///
/// The dashboard grid is now fixed at 12 columns. New panels land full-width on
/// a new row, matching Grafana's default add-panel behaviour.
const DEFAULT_NEW_PANEL_WIDTH: u32 = 12;

/// Default grid height for a panel appended via `append_panels`.
const DEFAULT_NEW_PANEL_HEIGHT: u32 = 2;

/// Returns the first grid row that lies past every panel in `panels`.
///
/// Equivalent to `max(grid_row + grid_height)` over the slice, or `0` when the
/// slice is empty. Used by `append_panels` to find the row a new full-width
/// panel should land on.
fn next_free_row(panels: &[DashboardPanel]) -> u32 {
    panels
        .iter()
        .map(|p| p.grid_row.saturating_add(p.grid_height))
        .max()
        .unwrap_or(0)
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
    /// Returns `Ok(true)` when an existing record was replaced, `Ok(false)`
    /// when a new record was inserted. Cache updated only on success; on
    /// failure the error propagates so the caller can surface a toast and
    /// emit an audit event.
    pub fn upsert_dashboard(&mut self, dashboard: Dashboard) -> Result<bool, StorageError> {
        let dto = dashboard_to_dto(&dashboard);
        let is_update = self.dashboards.iter().any(|d| d.id == dashboard.id);

        self.dashboards_repo.upsert(&dto)?;

        if let Some(existing) = self.dashboards.iter_mut().find(|d| d.id == dashboard.id) {
            *existing = dashboard;
        } else {
            self.dashboards.push(dashboard);
        }
        Ok(is_update)
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

    /// Creates a new dashboard, persists it, and updates the cache.
    /// Returns the new dashboard's UUID on success.
    ///
    /// The new dashboard starts with zero panels.
    pub fn create_dashboard(
        &mut self,
        name: String,
        description: Option<String>,
        profile_id: Uuid,
        shared_time_range_preset: Option<TimeRangePreset>,
        shared_refresh_policy: SavedChartRefreshPolicy,
    ) -> Result<Uuid, StorageError> {
        let now = Utc::now();
        let id = Uuid::new_v4();

        let dashboard = Dashboard {
            id,
            name,
            description,
            profile_id: Some(profile_id),
            shared_time_range_preset,
            shared_refresh_policy,
            grid_columns: 2,
            created_at: now,
            updated_at: now,
        };

        let dto = dashboard_to_dto(&dashboard);
        self.dashboards_repo.upsert(&dto)?;

        self.dashboards.push(dashboard);
        self.panels.insert(id, Vec::new());

        Ok(id)
    }

    /// Renames a dashboard, bumps `updated_at`, and updates the cache.
    pub fn rename_dashboard(
        &mut self,
        dashboard_id: Uuid,
        new_name: String,
    ) -> Result<(), StorageError> {
        let idx = self
            .dashboards
            .iter()
            .position(|d| d.id == dashboard_id)
            .ok_or_else(|| StorageError::Data(format!("dashboard not found: {dashboard_id}")))?;

        let mut updated = self.dashboards[idx].clone();
        updated.name = new_name;
        updated.updated_at = Utc::now();

        let dto = dashboard_to_dto(&updated);
        self.dashboards_repo.upsert(&dto)?;

        self.dashboards[idx] = updated;
        Ok(())
    }

    /// Deletes a dashboard row. SQLite CASCADE on `viz_dashboard_panels` handles
    /// panel cleanup. Cache (dashboard + panels) is evicted only on success.
    pub fn delete_dashboard(&mut self, dashboard_id: Uuid) -> Result<(), StorageError> {
        self.dashboards_repo.delete(dashboard_id)?;
        self.dashboards.retain(|d| d.id != dashboard_id);
        self.panels.remove(&dashboard_id);
        Ok(())
    }

    /// Deep-copies a dashboard: new UUID, name prefixed with "Copy of ", same
    /// shared time range / refresh policy, panels copied with the same
    /// `saved_chart_id` references and dense `panel_index` values.
    ///
    /// Returns the new dashboard's UUID.
    pub fn duplicate_dashboard(&mut self, dashboard_id: Uuid) -> Result<Uuid, StorageError> {
        let src = self
            .dashboards
            .iter()
            .find(|d| d.id == dashboard_id)
            .ok_or_else(|| StorageError::Data(format!("dashboard not found: {dashboard_id}")))?
            .clone();

        let src_panels = self.panels.get(&dashboard_id).cloned().unwrap_or_default();

        let new_id = Uuid::new_v4();
        let now = Utc::now();

        let new_dashboard = Dashboard {
            id: new_id,
            name: format!("Copy of {}", src.name),
            description: src.description.clone(),
            profile_id: src.profile_id,
            shared_time_range_preset: src.shared_time_range_preset,
            shared_refresh_policy: src.shared_refresh_policy,
            grid_columns: src.grid_columns,
            created_at: now,
            updated_at: now,
        };

        let new_panels: Vec<DashboardPanel> = src_panels
            .iter()
            .enumerate()
            .map(|(i, p)| DashboardPanel {
                dashboard_id: new_id,
                panel_index: i as u32,
                kind: p.kind.clone(),
                title_override: p.title_override.clone(),
                grid_row: p.grid_row,
                grid_column: p.grid_column,
                grid_width: p.grid_width,
                grid_height: p.grid_height,
            })
            .collect();

        let dto = dashboard_to_dto(&new_dashboard);
        self.dashboards_repo.upsert(&dto)?;

        let panel_dtos: Vec<DashboardPanelDto> = new_panels.iter().map(panel_to_dto).collect();
        self.panels_repo
            .replace_panels_for_dashboard(new_id, &panel_dtos)?;

        self.dashboards.push(new_dashboard);
        self.panels.insert(new_id, new_panels);

        Ok(new_id)
    }

    /// Updates the shared time-range preset and bumps `updated_at`.
    pub fn update_shared_time_range(
        &mut self,
        dashboard_id: Uuid,
        preset: Option<TimeRangePreset>,
    ) -> Result<(), StorageError> {
        let idx = self
            .dashboards
            .iter()
            .position(|d| d.id == dashboard_id)
            .ok_or_else(|| StorageError::Data(format!("dashboard not found: {dashboard_id}")))?;

        let mut updated = self.dashboards[idx].clone();
        updated.shared_time_range_preset = preset;
        updated.updated_at = Utc::now();

        let dto = dashboard_to_dto(&updated);
        self.dashboards_repo.upsert(&dto)?;

        self.dashboards[idx] = updated;
        Ok(())
    }

    /// Updates the shared refresh policy and bumps `updated_at`.
    pub fn update_shared_refresh_policy(
        &mut self,
        dashboard_id: Uuid,
        policy: SavedChartRefreshPolicy,
    ) -> Result<(), StorageError> {
        let idx = self
            .dashboards
            .iter()
            .position(|d| d.id == dashboard_id)
            .ok_or_else(|| StorageError::Data(format!("dashboard not found: {dashboard_id}")))?;

        let mut updated = self.dashboards[idx].clone();
        updated.shared_refresh_policy = policy;
        updated.updated_at = Utc::now();

        let dto = dashboard_to_dto(&updated);
        self.dashboards_repo.upsert(&dto)?;

        self.dashboards[idx] = updated;
        Ok(())
    }

    /// Appends `drafts.len()` panels to the dashboard.
    ///
    /// Each new panel lands full-width on a new row past every existing panel
    /// (`grid_column = 0, grid_width = DEFAULT_NEW_PANEL_WIDTH,
    /// grid_height = DEFAULT_NEW_PANEL_HEIGHT, title_override = None`).
    /// Subsequent drafts in the same call stack down by their height so they
    /// do not overlap each other. `panel_index` is assigned dense starting
    /// from the current count.
    pub fn append_panels(
        &mut self,
        dashboard_id: Uuid,
        drafts: Vec<DashboardPanelDraft>,
    ) -> Result<(), StorageError> {
        // Verify the dashboard exists. `grid_columns` is intentionally ignored
        // here — the dashboard grid is always 12 wide at the UI level.
        if !self.dashboards.iter().any(|d| d.id == dashboard_id) {
            return Err(StorageError::Data(format!(
                "dashboard not found: {dashboard_id}"
            )));
        }

        let current_panels = self.panels.get(&dashboard_id).cloned().unwrap_or_default();
        let base_index = current_panels.len() as u32;
        let mut next_row = next_free_row(&current_panels);

        let mut new_panels = current_panels.clone();

        for (i, draft) in drafts.into_iter().enumerate() {
            let panel_index = base_index + i as u32;

            let kind = match draft {
                DashboardPanelDraft::Chart { saved_chart_id } => {
                    DashboardPanelKind::Chart { saved_chart_id }
                }
                DashboardPanelDraft::Inspector { metric_id } => {
                    DashboardPanelKind::Inspector { metric_id }
                }
            };

            new_panels.push(DashboardPanel {
                dashboard_id,
                panel_index,
                kind,
                title_override: None,
                grid_row: next_row,
                grid_column: 0,
                grid_width: DEFAULT_NEW_PANEL_WIDTH,
                grid_height: DEFAULT_NEW_PANEL_HEIGHT,
            });

            next_row = next_row.saturating_add(DEFAULT_NEW_PANEL_HEIGHT);
        }

        let dtos: Vec<DashboardPanelDto> = new_panels.iter().map(panel_to_dto).collect();
        self.panels_repo
            .replace_panels_for_dashboard(dashboard_id, &dtos)?;

        self.panels.insert(dashboard_id, new_panels);
        Ok(())
    }

    /// Replace the grid position and size of a single panel.
    ///
    /// Clamps `grid_column` to `[0, 11]`, `grid_width` to `[1, 12]`, and
    /// `grid_height` to `[1, 12]`. `grid_row` is stored unchanged. Persists
    /// the entire panel set via `replace_panels_for_dashboard` so the row's
    /// other fields stay intact.
    pub fn update_panel_position(
        &mut self,
        dashboard_id: Uuid,
        panel_index: u32,
        grid_column: u32,
        grid_row: u32,
        grid_width: u32,
        grid_height: u32,
    ) -> Result<(), StorageError> {
        let mut panels = self.panels.get(&dashboard_id).cloned().unwrap_or_default();
        let panel = panels
            .iter_mut()
            .find(|p| p.panel_index == panel_index)
            .ok_or_else(|| {
                StorageError::Data(format!(
                    "panel_index {panel_index} not found in dashboard {dashboard_id}"
                ))
            })?;

        panel.grid_column = grid_column.min(11);
        panel.grid_row = grid_row;
        panel.grid_width = grid_width.clamp(1, 12);
        panel.grid_height = grid_height.clamp(1, 12);

        let dtos: Vec<DashboardPanelDto> = panels.iter().map(panel_to_dto).collect();
        self.panels_repo
            .replace_panels_for_dashboard(dashboard_id, &dtos)?;

        self.panels.insert(dashboard_id, panels);
        Ok(())
    }

    /// Removes the panel at `panel_index`, re-indexes remaining panels dense
    /// (0..N-1), and persists via `replace_panels_for_dashboard`.
    pub fn remove_panel(
        &mut self,
        dashboard_id: Uuid,
        panel_index: u32,
    ) -> Result<(), StorageError> {
        let grid_columns = self
            .dashboards
            .iter()
            .find(|d| d.id == dashboard_id)
            .map(|d| d.grid_columns)
            .ok_or_else(|| StorageError::Data(format!("dashboard not found: {dashboard_id}")))?;

        let mut panels = self.panels.get(&dashboard_id).cloned().unwrap_or_default();

        // Sort by panel_index to ensure deterministic removal.
        panels.sort_by_key(|p| p.panel_index);
        panels.retain(|p| p.panel_index != panel_index);

        // Re-index dense with recomputed grid positions.
        let updated_panels: Vec<DashboardPanel> = panels
            .into_iter()
            .enumerate()
            .map(|(i, mut p)| {
                p.panel_index = i as u32;
                let (row, col) = panel_index_to_grid(i as u32, grid_columns);
                p.grid_row = row;
                p.grid_column = col;
                p
            })
            .collect();

        let dtos: Vec<DashboardPanelDto> = updated_panels.iter().map(panel_to_dto).collect();
        self.panels_repo
            .replace_panels_for_dashboard(dashboard_id, &dtos)?;

        self.panels.insert(dashboard_id, updated_panels);
        Ok(())
    }

    /// Reorders panels using insert-at-position semantics: the panel at
    /// `from_index` is removed and inserted at `to_index`; all panels between
    /// shift by one slot. Indices are rewritten dense (0..N-1).
    pub fn reorder_panels(
        &mut self,
        dashboard_id: Uuid,
        from_index: u32,
        to_index: u32,
    ) -> Result<(), StorageError> {
        let grid_columns = self
            .dashboards
            .iter()
            .find(|d| d.id == dashboard_id)
            .map(|d| d.grid_columns)
            .ok_or_else(|| StorageError::Data(format!("dashboard not found: {dashboard_id}")))?;

        let mut panels = self.panels.get(&dashboard_id).cloned().unwrap_or_default();
        panels.sort_by_key(|p| p.panel_index);

        if from_index as usize >= panels.len() || to_index as usize > panels.len() {
            return Err(StorageError::Data(format!(
                "reorder_panels: from={from_index} to={to_index} out of range (len={})",
                panels.len()
            )));
        }

        let panel = panels.remove(from_index as usize);

        // Clamp to_index after removal (length is now N-1).
        let insert_at = (to_index as usize).min(panels.len());
        panels.insert(insert_at, panel);

        // Re-index dense with recomputed grid positions.
        let updated_panels: Vec<DashboardPanel> = panels
            .into_iter()
            .enumerate()
            .map(|(i, mut p)| {
                p.panel_index = i as u32;
                let (row, col) = panel_index_to_grid(i as u32, grid_columns);
                p.grid_row = row;
                p.grid_column = col;
                p
            })
            .collect();

        let dtos: Vec<DashboardPanelDto> = updated_panels.iter().map(panel_to_dto).collect();
        self.panels_repo
            .replace_panels_for_dashboard(dashboard_id, &dtos)?;

        self.panels.insert(dashboard_id, updated_panels);
        Ok(())
    }

    /// Resizes the panel at `panel_index`. Clamps `new_width` to
    /// `1..=grid_columns` and `new_height` to `1..=4`.
    pub fn resize_panel(
        &mut self,
        dashboard_id: Uuid,
        panel_index: u32,
        new_width: u32,
        new_height: u32,
    ) -> Result<(), StorageError> {
        let grid_columns = self
            .dashboards
            .iter()
            .find(|d| d.id == dashboard_id)
            .map(|d| d.grid_columns)
            .ok_or_else(|| StorageError::Data(format!("dashboard not found: {dashboard_id}")))?;

        let clamped_width = new_width.clamp(1, grid_columns);
        let clamped_height = new_height.clamp(1, 4);

        let mut panels = self.panels.get(&dashboard_id).cloned().unwrap_or_default();
        let panel = panels
            .iter_mut()
            .find(|p| p.panel_index == panel_index)
            .ok_or_else(|| {
                StorageError::Data(format!(
                    "panel_index {panel_index} not found in dashboard {dashboard_id}"
                ))
            })?;

        panel.grid_width = clamped_width;
        panel.grid_height = clamped_height;

        let dtos: Vec<DashboardPanelDto> = panels.iter().map(panel_to_dto).collect();
        self.panels_repo
            .replace_panels_for_dashboard(dashboard_id, &dtos)?;

        self.panels.insert(dashboard_id, panels);
        Ok(())
    }

    /// Sets or clears the per-panel title override. An empty string is treated
    /// as `None` (reverts to source chart name).
    pub fn update_panel_title_override(
        &mut self,
        dashboard_id: Uuid,
        panel_index: u32,
        override_text: Option<String>,
    ) -> Result<(), StorageError> {
        let mut panels = self.panels.get(&dashboard_id).cloned().unwrap_or_default();
        let panel = panels
            .iter_mut()
            .find(|p| p.panel_index == panel_index)
            .ok_or_else(|| {
                StorageError::Data(format!(
                    "panel_index {panel_index} not found in dashboard {dashboard_id}"
                ))
            })?;

        // Normalize: empty string → None.
        panel.title_override =
            override_text.and_then(|s| if s.is_empty() { None } else { Some(s) });

        let dtos: Vec<DashboardPanelDto> = panels.iter().map(panel_to_dto).collect();
        self.panels_repo
            .replace_panels_for_dashboard(dashboard_id, &dtos)?;

        self.panels.insert(dashboard_id, panels);
        Ok(())
    }

    /// Remove a dashboard by id. Returns `Ok(true)` if a record was removed,
    /// `Ok(false)` if no dashboard with that id was present.
    ///
    /// Cache (dashboards + panels) updated only on success; storage failures
    /// propagate so the caller can surface a toast and emit an audit event.
    pub fn remove_dashboard(&mut self, id: Uuid) -> Result<bool, StorageError> {
        let was_present = self.dashboards.iter().any(|d| d.id == id);
        if !was_present {
            return Ok(false);
        }

        self.dashboards_repo.delete(id)?;
        self.dashboards.retain(|d| d.id != id);
        self.panels.remove(&id);
        Ok(true)
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
            kind: DashboardPanelKind::Chart { saved_chart_id },
            title_override: None,
            grid_row: 0,
            grid_column: index * 4,
            grid_width: 4,
            grid_height: 3,
        }
    }

    fn make_manager_with_rt() -> (DashboardManager, StorageRuntime) {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt.viz_connection();
        let dashboards_repo = Arc::new(DashboardsRepository::new(Arc::clone(&conn)));
        let panels_repo = Arc::new(DashboardPanelsRepository::new(Arc::clone(&conn)));
        let mgr = DashboardManager::new(dashboards_repo, panels_repo);
        (mgr, rt)
    }

    /// Inserts a minimal profile row and returns its UUID.
    fn insert_profile(rt: &StorageRuntime) -> Uuid {
        let id = Uuid::new_v4();
        let conn_guard = rt.viz_connection();
        let conn = conn_guard.lock().unwrap();
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, ?2)",
            rusqlite::params![id.to_string(), "test-profile"],
        )
        .unwrap();
        id
    }

    // ---- create_dashboard ---------------------------------------------------

    #[test]
    fn test_create_dashboard_persists_and_returns_id() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let id = mgr
            .create_dashboard(
                "Alpha".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        let found = mgr.dashboard_by_id(id).expect("must be in cache");
        assert_eq!(found.name, "Alpha");
        assert_eq!(found.profile_id, Some(profile_id));
        assert_eq!(mgr.panels_for_dashboard(id).len(), 0);
    }

    #[test]
    fn test_create_dashboard_appears_in_dashboards_for_profile() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let id = mgr
            .create_dashboard(
                "Beta".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        let list = mgr.dashboards_for_profile(profile_id);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, id);
    }

    // ---- rename_dashboard ---------------------------------------------------

    #[test]
    fn test_rename_dashboard_updates_name_in_cache() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let id = mgr
            .create_dashboard(
                "Old".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        mgr.rename_dashboard(id, "New".to_string()).unwrap();

        assert_eq!(mgr.dashboard_by_id(id).unwrap().name, "New");
    }

    #[test]
    fn test_rename_dashboard_not_found_returns_err() {
        let (mut mgr, _rt) = make_manager_with_rt();
        let result = mgr.rename_dashboard(Uuid::new_v4(), "X".to_string());
        assert!(result.is_err());
    }

    // ---- delete_dashboard ---------------------------------------------------

    #[test]
    fn test_delete_dashboard_removes_from_cache() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let id = mgr
            .create_dashboard(
                "D".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        mgr.delete_dashboard(id).unwrap();

        assert!(mgr.dashboard_by_id(id).is_none());
        assert!(mgr.dashboards_for_profile(profile_id).is_empty());
    }

    // ---- duplicate_dashboard ------------------------------------------------

    #[test]
    fn test_duplicate_dashboard_creates_copy_with_copy_of_prefix() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let orig_id = mgr
            .create_dashboard(
                "Orig".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        let dup_id = mgr.duplicate_dashboard(orig_id).unwrap();

        assert_ne!(orig_id, dup_id);
        let dup = mgr.dashboard_by_id(dup_id).unwrap();
        assert_eq!(dup.name, "Copy of Orig");
        assert_eq!(dup.profile_id, Some(profile_id));
    }

    // ---- append / remove / reorder / resize / title_override ---------------

    #[test]
    fn test_append_panels_assigns_dense_indices() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let dash_id = mgr
            .create_dashboard(
                "D".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        let drafts = vec![
            DashboardPanelDraft::Chart {
                saved_chart_id: Uuid::new_v4(),
            },
            DashboardPanelDraft::Chart {
                saved_chart_id: Uuid::new_v4(),
            },
        ];
        mgr.append_panels(dash_id, drafts).unwrap();

        let panels = mgr.panels_for_dashboard(dash_id);
        assert_eq!(panels.len(), 2);
        assert_eq!(panels[0].panel_index, 0);
        assert_eq!(panels[1].panel_index, 1);
    }

    #[test]
    fn test_remove_panel_reindexes_remaining() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let dash_id = mgr
            .create_dashboard(
                "D".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        let chart_a = Uuid::new_v4();
        let chart_b = Uuid::new_v4();
        let chart_c = Uuid::new_v4();
        mgr.append_panels(
            dash_id,
            vec![
                DashboardPanelDraft::Chart {
                    saved_chart_id: chart_a,
                },
                DashboardPanelDraft::Chart {
                    saved_chart_id: chart_b,
                },
                DashboardPanelDraft::Chart {
                    saved_chart_id: chart_c,
                },
            ],
        )
        .unwrap();

        // Remove middle panel (index 1 = chart_b).
        mgr.remove_panel(dash_id, 1).unwrap();

        let panels = mgr.panels_for_dashboard(dash_id);
        assert_eq!(panels.len(), 2);
        assert_eq!(panels[0].panel_index, 0);
        assert_eq!(panels[0].saved_chart_id(), Some(chart_a));
        assert_eq!(panels[1].panel_index, 1);
        assert_eq!(panels[1].saved_chart_id(), Some(chart_c));
    }

    #[test]
    fn test_reorder_panels_insert_at_position_semantics() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let dash_id = mgr
            .create_dashboard(
                "D".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        let chart_a = Uuid::new_v4();
        let chart_b = Uuid::new_v4();
        let chart_c = Uuid::new_v4();
        mgr.append_panels(
            dash_id,
            vec![
                DashboardPanelDraft::Chart {
                    saved_chart_id: chart_a,
                },
                DashboardPanelDraft::Chart {
                    saved_chart_id: chart_b,
                },
                DashboardPanelDraft::Chart {
                    saved_chart_id: chart_c,
                },
            ],
        )
        .unwrap();

        // Move panel 0 (chart_a) to position 2 → [B, C, A].
        mgr.reorder_panels(dash_id, 0, 2).unwrap();

        let panels = mgr.panels_for_dashboard(dash_id);
        assert_eq!(panels[0].saved_chart_id(), Some(chart_b));
        assert_eq!(panels[1].saved_chart_id(), Some(chart_c));
        assert_eq!(panels[2].saved_chart_id(), Some(chart_a));
    }

    #[test]
    fn test_resize_panel_clamps_to_grid_bounds() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let dash_id = mgr
            .create_dashboard(
                "D".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        mgr.append_panels(
            dash_id,
            vec![DashboardPanelDraft::Chart {
                saved_chart_id: Uuid::new_v4(),
            }],
        )
        .unwrap();

        // grid_columns defaults to 2; requesting width=99, height=99 should be clamped.
        mgr.resize_panel(dash_id, 0, 99, 99).unwrap();

        let panel = &mgr.panels_for_dashboard(dash_id)[0];
        assert_eq!(panel.grid_width, 2); // clamped to grid_columns
        assert_eq!(panel.grid_height, 4); // clamped to max 4
    }

    #[test]
    fn test_update_panel_title_override_empty_string_becomes_none() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let dash_id = mgr
            .create_dashboard(
                "D".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        mgr.append_panels(
            dash_id,
            vec![DashboardPanelDraft::Chart {
                saved_chart_id: Uuid::new_v4(),
            }],
        )
        .unwrap();

        // Set a title override.
        mgr.update_panel_title_override(dash_id, 0, Some("Custom".to_string()))
            .unwrap();
        assert_eq!(
            mgr.panels_for_dashboard(dash_id)[0]
                .title_override
                .as_deref(),
            Some("Custom")
        );

        // Clear with empty string.
        mgr.update_panel_title_override(dash_id, 0, Some(String::new()))
            .unwrap();
        assert!(
            mgr.panels_for_dashboard(dash_id)[0]
                .title_override
                .is_none()
        );
    }

    #[test]
    fn test_append_panels_defaults_to_full_width_on_new_row() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let dash_id = mgr
            .create_dashboard(
                "D".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        mgr.append_panels(
            dash_id,
            vec![
                DashboardPanelDraft::Chart {
                    saved_chart_id: Uuid::new_v4(),
                },
                DashboardPanelDraft::Chart {
                    saved_chart_id: Uuid::new_v4(),
                },
            ],
        )
        .unwrap();

        let panels = mgr.panels_for_dashboard(dash_id);
        assert_eq!(panels.len(), 2);
        // Both panels span the full 12-column row, stacked vertically.
        assert_eq!(panels[0].grid_column, 0);
        assert_eq!(panels[0].grid_row, 0);
        assert_eq!(panels[0].grid_width, 12);
        assert_eq!(panels[0].grid_height, 2);
        assert_eq!(panels[1].grid_column, 0);
        assert_eq!(panels[1].grid_row, 2);
        assert_eq!(panels[1].grid_width, 12);
        assert_eq!(panels[1].grid_height, 2);
    }

    #[test]
    fn test_update_panel_position_clamps_and_persists() {
        let (mut mgr, rt) = make_manager_with_rt();
        let profile_id = insert_profile(&rt);
        let dash_id = mgr
            .create_dashboard(
                "D".to_string(),
                None,
                profile_id,
                None,
                SavedChartRefreshPolicy::Off,
            )
            .unwrap();

        mgr.append_panels(
            dash_id,
            vec![DashboardPanelDraft::Chart {
                saved_chart_id: Uuid::new_v4(),
            }],
        )
        .unwrap();

        // Request out-of-range values; expect clamping.
        mgr.update_panel_position(dash_id, 0, 99, 5, 0, 999)
            .unwrap();
        let panel = &mgr.panels_for_dashboard(dash_id)[0];
        assert_eq!(panel.grid_column, 11);
        assert_eq!(panel.grid_row, 5);
        assert_eq!(panel.grid_width, 1);
        assert_eq!(panel.grid_height, 12);
    }

    // ---- panel_index_to_grid ------------------------------------------------

    #[test]
    fn test_panel_index_to_grid_two_columns() {
        assert_eq!(panel_index_to_grid(0, 2), (0, 0));
        assert_eq!(panel_index_to_grid(1, 2), (0, 1));
        assert_eq!(panel_index_to_grid(2, 2), (1, 0));
        assert_eq!(panel_index_to_grid(3, 2), (1, 1));
    }

    #[test]
    fn test_panel_index_to_grid_zero_columns_does_not_panic() {
        // grid_columns = 0 should be clamped to 1.
        assert_eq!(panel_index_to_grid(0, 0), (0, 0));
        assert_eq!(panel_index_to_grid(3, 0), (3, 0));
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
            kind: DashboardPanelKind::Chart {
                saved_chart_id: Uuid::new_v4(),
            },
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
