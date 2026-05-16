//! `SavedChart` — persisted record for a named chart with its query and spec.
//!
//! Stored in `~/.config/dbflux/saved_charts.json` via [`SavedChartStore`] /
//! [`SavedChartManager`].
//!
//! # Crate placement
//!
//! `SavedChart` lives in `dbflux_components` rather than `dbflux_core` because
//! it embeds `ChartSpec` and `BindingSpec`, which are owned by this crate.
//! The JSON store and item manager infrastructure is imported from `dbflux_core`.

use crate::chart::{BindingSpec, ChartSpec};
use chrono::{DateTime, Utc};
use dbflux_core::{Identifiable, JsonStore};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// Quick-select time-range presets stored alongside a chart.
///
/// Mirrors the variants in `dbflux_ui::ui::common::time_range::TimeRange` but
/// lives here so `SavedChart` can be (de)serialized without a GPUI dependency.
/// Phase D will bridge between the two types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TimeRangePreset {
    Last15min,
    LastHour,
    Last6Hours,
    #[default]
    Last24Hours,
    Last7Days,
}

/// Refresh behaviour for a saved chart when it is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SavedChartRefreshPolicy {
    /// No automatic refresh; user must trigger re-execution manually.
    #[default]
    Off,
    /// Re-execute the query every `every_secs` seconds.
    Interval { every_secs: u32 },
    /// Re-execute once automatically when the chart is opened.
    OnOpen,
}

// ---------------------------------------------------------------------------
// SavedChart
// ---------------------------------------------------------------------------

/// A persisted chart record.
///
/// Only the query string is persisted — raw result data is never stored.
/// `chart_spec` and `bindings` carry the full rendering configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedChart {
    /// Stable identity for upsert and deduplication.
    pub id: Uuid,
    /// User-supplied display name.
    pub name: String,
    /// The connection profile this chart was created under.
    pub profile_id: Uuid,
    /// Query string executed to produce data for the chart.
    pub query: String,
    /// Serialized chart spec. Uses `#[serde(default)]` fields so old JSON
    /// without newer fields is still loadable.
    pub chart_spec: ChartSpec,
    /// Column bindings for the AxisBar.
    pub bindings: BindingSpec,
    /// Optional time-range preset applied when the chart is opened.
    #[serde(default)]
    pub time_range_preset: Option<TimeRangePreset>,
    /// Refresh policy applied while the chart is open.
    #[serde(default)]
    pub refresh_policy: SavedChartRefreshPolicy,
    /// Creation timestamp (UTC).
    pub created_at: DateTime<Utc>,
    /// Last-modified timestamp (UTC); updated on every upsert.
    pub updated_at: DateTime<Utc>,
}

impl SavedChart {
    /// Create a new `SavedChart` with a fresh `Uuid` and timestamps set to now.
    pub fn new(
        name: String,
        profile_id: Uuid,
        query: String,
        chart_spec: ChartSpec,
        bindings: BindingSpec,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            profile_id,
            query,
            chart_spec,
            bindings,
            time_range_preset: None,
            refresh_policy: SavedChartRefreshPolicy::Off,
            created_at: now,
            updated_at: now,
        }
    }
}

impl Identifiable for SavedChart {
    fn id(&self) -> Uuid {
        self.id
    }
}

// ---------------------------------------------------------------------------
// Store and manager
// ---------------------------------------------------------------------------

/// JSON-file store backed by `~/.config/dbflux/saved_charts.json`.
pub type SavedChartStore = JsonStore<SavedChart>;

/// Open (or create) the saved-charts store.
///
/// If the file does not exist yet, `load()` on the returned store will return
/// an empty `Vec` without error.
#[allow(clippy::result_large_err)]
pub fn open_saved_charts_store() -> Result<SavedChartStore, dbflux_core::DbError> {
    JsonStore::new("saved_charts.json")
}

/// Manager for `SavedChart` records with profile-scoped queries and upsert.
pub struct SavedChartManager {
    items: Vec<SavedChart>,
    store: Option<SavedChartStore>,
}

impl SavedChartManager {
    /// Load from the default `~/.config/dbflux/saved_charts.json`.
    ///
    /// If the file is absent or the store cannot be opened, the manager starts
    /// empty without panicking.
    pub fn load() -> Self {
        match open_saved_charts_store() {
            Ok(store) => {
                let items = store.load().unwrap_or_else(|e| {
                    log::warn!("Failed to load saved charts: {e}; starting empty");
                    Vec::new()
                });
                Self {
                    items,
                    store: Some(store),
                }
            }
            Err(e) => {
                log::warn!("Cannot open saved-charts store: {e}; starting with empty manager");
                Self::empty()
            }
        }
    }

    /// Create a manager backed by `store` (useful in tests with a temp path).
    #[allow(clippy::result_large_err)]
    pub fn from_store(store: SavedChartStore) -> Result<Self, dbflux_core::DbError> {
        let items = store.load()?;
        Ok(Self {
            items,
            store: Some(store),
        })
    }

    /// Create an in-memory manager with no backing store.
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            store: None,
        }
    }

    /// Persist the current item list to disk.
    fn save(&self) {
        let Some(ref store) = self.store else {
            log::warn!("SavedChartManager: cannot save — no backing store");
            return;
        };
        if let Err(e) = store.save(&self.items) {
            log::error!("SavedChartManager: failed to save: {e}");
        }
    }

    /// Insert or replace a chart by `id`.
    ///
    /// Returns `true` when an existing record was replaced, `false` when a new
    /// record was inserted.
    pub fn upsert(&mut self, mut chart: SavedChart) -> bool {
        chart.updated_at = Utc::now();
        if let Some(existing) = self.items.iter_mut().find(|c| c.id == chart.id) {
            *existing = chart;
            self.save();
            true
        } else {
            self.items.push(chart);
            self.save();
            false
        }
    }

    /// All charts whose `profile_id` matches the given id.
    pub fn charts_for_profile(&self, profile_id: Uuid) -> Vec<&SavedChart> {
        self.items
            .iter()
            .filter(|c| c.profile_id == profile_id)
            .collect()
    }

    /// All charts, regardless of profile.
    pub fn all_charts(&self) -> &[SavedChart] {
        &self.items
    }

    /// Remove a chart by id. Returns `true` if a record was removed.
    pub fn remove(&mut self, id: Uuid) -> bool {
        let before = self.items.len();
        self.items.retain(|c| c.id != id);
        let removed = before != self.items.len();
        if removed {
            self.save();
        }
        removed
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::{AggKind, AxisKind, AxisSpec, ChartKind, SeriesSpec};

    fn sample_spec() -> ChartSpec {
        ChartSpec {
            kind: ChartKind::Line,
            x_axis: AxisSpec {
                column_index: 0,
                label: "time".to_string(),
                kind: AxisKind::Time,
                unit: None,
            },
            series: vec![SeriesSpec {
                column_index: 1,
                label: "value".to_string(),
                color_slot: 0,
            }],
            legend_visible: false,
            decimation_threshold: 10_000,
            binding: BindingSpec::default(),
        }
    }

    fn sample_chart(name: &str, profile_id: Uuid) -> SavedChart {
        SavedChart::new(
            name.to_string(),
            profile_id,
            "SELECT * FROM test".to_string(),
            sample_spec(),
            BindingSpec {
                x: 0,
                y: vec![1],
                group_by: None,
                filter: None,
                aggregation: AggKind::None,
            },
        )
    }

    /// T-CE-C05: Empty file → empty manager.
    #[test]
    fn empty_store_returns_empty_manager() {
        let dir = tempfile::tempdir().unwrap();
        let store = SavedChartStore::from_path(dir.path().join("saved_charts.json"));
        let manager = SavedChartManager::from_store(store).unwrap();
        assert!(manager.all_charts().is_empty());
    }

    /// T-CE-C05: Upsert dedup — upserting the same id twice yields one record.
    #[test]
    fn upsert_same_id_replaces_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = SavedChartStore::from_path(dir.path().join("saved_charts.json"));
        let mut manager = SavedChartManager::from_store(store).unwrap();

        let profile_id = Uuid::new_v4();
        let mut chart = sample_chart("First name", profile_id);
        let id = chart.id;

        manager.upsert(chart.clone());
        assert_eq!(manager.all_charts().len(), 1);

        chart.name = "Updated name".to_string();
        manager.upsert(chart);

        assert_eq!(manager.all_charts().len(), 1, "one record after upsert");
        assert_eq!(manager.all_charts()[0].name, "Updated name");
        assert_eq!(manager.all_charts()[0].id, id);
    }

    /// T-CE-C05: charts_for_profile filter.
    #[test]
    fn charts_for_profile_filters_by_profile_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = SavedChartStore::from_path(dir.path().join("saved_charts.json"));
        let mut manager = SavedChartManager::from_store(store).unwrap();

        let p1 = Uuid::new_v4();
        let p2 = Uuid::new_v4();

        manager.upsert(sample_chart("chart-a", p1));
        manager.upsert(sample_chart("chart-b", p1));
        manager.upsert(sample_chart("chart-c", p2));

        let p1_charts = manager.charts_for_profile(p1);
        assert_eq!(p1_charts.len(), 2);

        let p2_charts = manager.charts_for_profile(p2);
        assert_eq!(p2_charts.len(), 1);
        assert_eq!(p2_charts[0].name, "chart-c");

        let unknown = manager.charts_for_profile(Uuid::new_v4());
        assert!(unknown.is_empty());
    }

    /// T-CE-C05: Save then reload — chart appears in charts_for_profile.
    #[test]
    fn save_reload_chart_appears_in_profile() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("saved_charts.json");

        let profile_id = Uuid::new_v4();
        let chart_id;

        {
            let store = SavedChartStore::from_path(path.clone());
            let mut manager = SavedChartManager::from_store(store).unwrap();
            let chart = sample_chart("My chart", profile_id);
            chart_id = chart.id;
            manager.upsert(chart);
        }

        {
            let store = SavedChartStore::from_path(path.clone());
            let manager = SavedChartManager::from_store(store).unwrap();
            let charts = manager.charts_for_profile(profile_id);
            assert_eq!(charts.len(), 1);
            assert_eq!(charts[0].id, chart_id);
            assert_eq!(charts[0].name, "My chart");
        }
    }

    /// T-CE-C05: File is valid JSON after save.
    #[test]
    fn saved_file_is_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("saved_charts.json");

        let store = SavedChartStore::from_path(path.clone());
        let mut manager = SavedChartManager::from_store(store).unwrap();
        manager.upsert(sample_chart("test", Uuid::new_v4()));

        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(&content);
        assert!(parsed.is_ok(), "saved_charts.json must be valid JSON");
    }

    /// T-CE-C05: Chart with unknown profile loads without panic.
    #[test]
    fn load_chart_with_unknown_profile_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("saved_charts.json");

        let orphan_profile_id = Uuid::new_v4();

        {
            let store = SavedChartStore::from_path(path.clone());
            let mut manager = SavedChartManager::from_store(store).unwrap();
            manager.upsert(sample_chart("orphan", orphan_profile_id));
        }

        let store = SavedChartStore::from_path(path.clone());
        let manager = SavedChartManager::from_store(store).unwrap();

        let active_profile = Uuid::new_v4();
        let charts = manager.charts_for_profile(active_profile);
        assert!(charts.is_empty());

        // Orphan still present — no silent deletion.
        assert_eq!(manager.all_charts().len(), 1);
    }
}
