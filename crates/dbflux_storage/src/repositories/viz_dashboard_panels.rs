//! Repository for `viz_dashboard_panels` — panel slot rows for a dashboard.
//!
//! The `saved_chart_id` column is a soft reference: deleting a `SavedChart`
//! does NOT cascade here, so panels become orphans. The `count_orphans()`
//! method surfaces those orphans for diagnostics and testing.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use uuid::Uuid;

use crate::error::StorageError;

const DB_PATH: &str = "dbflux.db";

/// Data transfer object mirroring one row of `viz_dashboard_panels`.
///
/// `panel_kind = "chart"` rows reference a `SavedChart` via `saved_chart_id`
/// and ignore `divider_markdown` and `inspector_metric_id`.
/// `panel_kind = "divider"` rows carry the divider's markdown in
/// `divider_markdown` and ignore the other two kind-specific columns.
/// `panel_kind = "inspector"` rows carry the target metric id in
/// `inspector_metric_id` and ignore the other two kind-specific columns.
#[derive(Debug, Clone, PartialEq)]
pub struct DashboardPanelDto {
    pub dashboard_id: String,
    pub panel_index: i64,
    pub panel_kind: String,
    pub saved_chart_id: String,
    pub divider_markdown: Option<String>,
    pub inspector_metric_id: Option<String>,
    pub title_override: Option<String>,
    pub grid_row: i64,
    pub grid_column: i64,
    pub grid_width: i64,
    pub grid_height: i64,
}

/// Repository for `viz_dashboard_panels`.
#[derive(Clone)]
pub struct DashboardPanelsRepository {
    conn: Arc<Mutex<Connection>>,
}

impl DashboardPanelsRepository {
    /// Creates a new repository wrapping the given shared connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Lists all panels for a dashboard, ordered by `panel_index ASC`.
    pub fn list_for_dashboard(
        &self,
        dashboard_id: Uuid,
    ) -> Result<Vec<DashboardPanelDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT dashboard_id, panel_index, panel_kind, saved_chart_id,
                        divider_markdown, inspector_metric_id, title_override,
                        grid_row, grid_column, grid_width, grid_height
                 FROM viz_dashboard_panels
                 WHERE dashboard_id = ?1
                 ORDER BY panel_index ASC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let rows = stmt
            .query_map([dashboard_id.to_string()], |row| {
                Ok(DashboardPanelDto {
                    dashboard_id: row.get(0)?,
                    panel_index: row.get(1)?,
                    panel_kind: row.get(2)?,
                    saved_chart_id: row.get(3)?,
                    divider_markdown: row.get(4)?,
                    inspector_metric_id: row.get(5)?,
                    title_override: row.get(6)?,
                    grid_row: row.get(7)?,
                    grid_column: row.get(8)?,
                    grid_width: row.get(9)?,
                    grid_height: row.get(10)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Lists all panels that reference a given `saved_chart_id` (reverse lookup).
    pub fn list_by_saved_chart(
        &self,
        saved_chart_id: Uuid,
    ) -> Result<Vec<DashboardPanelDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT dashboard_id, panel_index, panel_kind, saved_chart_id,
                        divider_markdown, inspector_metric_id, title_override,
                        grid_row, grid_column, grid_width, grid_height
                 FROM viz_dashboard_panels
                 WHERE saved_chart_id = ?1
                 ORDER BY dashboard_id, panel_index ASC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let rows = stmt
            .query_map([saved_chart_id.to_string()], |row| {
                Ok(DashboardPanelDto {
                    dashboard_id: row.get(0)?,
                    panel_index: row.get(1)?,
                    panel_kind: row.get(2)?,
                    saved_chart_id: row.get(3)?,
                    divider_markdown: row.get(4)?,
                    inspector_metric_id: row.get(5)?,
                    title_override: row.get(6)?,
                    grid_row: row.get(7)?,
                    grid_column: row.get(8)?,
                    grid_width: row.get(9)?,
                    grid_height: row.get(10)?,
                })
            })
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Atomically replaces all panels for a dashboard.
    ///
    /// Deletes every existing panel row for `dashboard_id` and reinserts the
    /// provided slice in a single transaction. On any failure the original
    /// panels are preserved.
    pub fn replace_panels_for_dashboard(
        &self,
        dashboard_id: Uuid,
        panels: &[DashboardPanelDto],
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let tx = conn
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        tx.execute(
            "DELETE FROM viz_dashboard_panels WHERE dashboard_id = ?1",
            [dashboard_id.to_string()],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        for panel in panels {
            tx.execute(
                "INSERT INTO viz_dashboard_panels
                     (dashboard_id, panel_index, panel_kind, saved_chart_id,
                      divider_markdown, inspector_metric_id, title_override,
                      grid_row, grid_column, grid_width, grid_height)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    panel.dashboard_id,
                    panel.panel_index,
                    panel.panel_kind,
                    panel.saved_chart_id,
                    panel.divider_markdown,
                    panel.inspector_metric_id,
                    panel.title_override,
                    panel.grid_row,
                    panel.grid_column,
                    panel.grid_width,
                    panel.grid_height,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;
        }

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        Ok(())
    }

    /// Returns the count of panel rows whose `saved_chart_id` does not exist
    /// in `viz_saved_charts` (orphaned panels).
    pub fn count_orphans(&self) -> Result<u64, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM viz_dashboard_panels p
                 LEFT JOIN viz_saved_charts c ON p.saved_chart_id = c.id
                 WHERE p.panel_kind = 'chart' AND c.id IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        Ok(count as u64)
    }
}

fn lock_err<T>(e: std::sync::PoisonError<T>) -> StorageError {
    StorageError::Sqlite {
        path: DB_PATH.into(),
        source: rusqlite::Error::InvalidParameterName(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(suffix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_panels_{}_{}.db",
            suffix,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn insert_profile(conn: &Connection) -> Uuid {
        let id = Uuid::new_v4();
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, ?2)",
            rusqlite::params![id.to_string(), "test-profile"],
        )
        .unwrap();
        id
    }

    fn insert_chart(conn: &Connection, profile_id: Uuid) -> Uuid {
        let id = Uuid::new_v4();
        conn.execute(
            "INSERT INTO viz_saved_charts
             (id, name, profile_id, created_at, updated_at,
              chart_kind, legend_visible, decimation_threshold, track_source_indices,
              y_scale, x_axis_column_index, x_axis_label, x_axis_kind,
              binding_x, binding_aggregation, source_kind, source_query,
              refresh_policy_kind)
             VALUES (?1, 'Chart', ?2, 0, 0,
                     'line', 0, 10000, 0,
                     'linear', 0, 'X', 'time',
                     0, 'none', 'query', 'SELECT 1',
                     'off')",
            rusqlite::params![id.to_string(), profile_id.to_string()],
        )
        .unwrap();
        id
    }

    fn insert_dashboard(conn: &Connection, profile_id: Option<Uuid>) -> Uuid {
        let id = Uuid::new_v4();
        conn.execute(
            "INSERT INTO viz_dashboards
             (id, name, profile_id, shared_refresh_policy_kind, grid_columns, created_at, updated_at)
             VALUES (?1, 'Dash', ?2, 'off', 2, 0, 0)",
            rusqlite::params![id.to_string(), profile_id.map(|p| p.to_string())],
        )
        .unwrap();
        id
    }

    fn panel(dashboard_id: Uuid, panel_index: i64, chart_id: Uuid) -> DashboardPanelDto {
        DashboardPanelDto {
            dashboard_id: dashboard_id.to_string(),
            panel_index,
            panel_kind: "chart".to_string(),
            saved_chart_id: chart_id.to_string(),
            divider_markdown: None,
            inspector_metric_id: None,
            title_override: None,
            grid_row: 0,
            grid_column: 0,
            grid_width: 1,
            grid_height: 1,
        }
    }

    #[test]
    fn test_panels_list_order() {
        let path = temp_db("list_order");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let chart_id = insert_chart(&conn, profile_id);
        let dashboard_id = insert_dashboard(&conn, Some(profile_id));

        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardPanelsRepository::new(Arc::clone(&conn));

        let panels = vec![
            panel(dashboard_id, 2, chart_id),
            panel(dashboard_id, 0, chart_id),
            panel(dashboard_id, 1, chart_id),
        ];
        repo.replace_panels_for_dashboard(dashboard_id, &panels)
            .expect("replace");

        let result = repo.list_for_dashboard(dashboard_id).expect("list");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].panel_index, 0);
        assert_eq!(result[1].panel_index, 1);
        assert_eq!(result[2].panel_index, 2);
    }

    #[test]
    fn test_panels_replace_atomicity() {
        let path = temp_db("replace_atomicity");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let chart_id = insert_chart(&conn, profile_id);
        let dashboard_id = insert_dashboard(&conn, Some(profile_id));

        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardPanelsRepository::new(Arc::clone(&conn));

        let initial = vec![
            panel(dashboard_id, 0, chart_id),
            panel(dashboard_id, 1, chart_id),
        ];
        repo.replace_panels_for_dashboard(dashboard_id, &initial)
            .expect("initial");

        // grid_width = 0 violates CHECK (grid_width >= 1).
        let bad = vec![DashboardPanelDto {
            dashboard_id: dashboard_id.to_string(),
            panel_index: 0,
            panel_kind: "chart".to_string(),
            saved_chart_id: chart_id.to_string(),
            divider_markdown: None,
            inspector_metric_id: None,
            title_override: None,
            grid_row: 0,
            grid_column: 0,
            grid_width: 0,
            grid_height: 1,
        }];
        let result = repo.replace_panels_for_dashboard(dashboard_id, &bad);
        assert!(result.is_err(), "should fail due to CHECK constraint");

        let after = repo.list_for_dashboard(dashboard_id).expect("list after");
        assert_eq!(
            after.len(),
            2,
            "original 2 panels must survive the failed replace"
        );
    }

    #[test]
    fn test_count_orphans() {
        let path = temp_db("count_orphans");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let real_chart_id = insert_chart(&conn, profile_id);
        let ghost_chart_id = Uuid::new_v4(); // never inserted
        let dashboard_id = insert_dashboard(&conn, Some(profile_id));

        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardPanelsRepository::new(Arc::clone(&conn));

        let panels = vec![
            panel(dashboard_id, 0, real_chart_id),
            panel(dashboard_id, 1, ghost_chart_id),
        ];
        repo.replace_panels_for_dashboard(dashboard_id, &panels)
            .expect("replace");

        let orphans = repo.count_orphans().expect("count");
        assert_eq!(orphans, 1, "exactly 1 panel should be an orphan");
    }

    #[test]
    fn test_inspector_panel_roundtrip_via_repo() {
        let path = temp_db("inspector_roundtrip");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let dashboard_id = insert_dashboard(&conn, Some(profile_id));

        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardPanelsRepository::new(Arc::clone(&conn));

        let inspector_panel = DashboardPanelDto {
            dashboard_id: dashboard_id.to_string(),
            panel_index: 0,
            panel_kind: "inspector".to_string(),
            saved_chart_id: String::new(),
            divider_markdown: None,
            inspector_metric_id: Some("pg.activity".to_string()),
            title_override: None,
            grid_row: 0,
            grid_column: 0,
            grid_width: 6,
            grid_height: 4,
        };

        repo.replace_panels_for_dashboard(dashboard_id, std::slice::from_ref(&inspector_panel))
            .expect("replace with inspector panel");

        let loaded = repo.list_for_dashboard(dashboard_id).expect("list");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].panel_kind, "inspector");
        assert_eq!(
            loaded[0].inspector_metric_id.as_deref(),
            Some("pg.activity")
        );
    }

    #[test]
    fn test_count_orphans_excludes_inspector_and_divider_panels() {
        let path = temp_db("count_orphans_kinds");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let dashboard_id = insert_dashboard(&conn, Some(profile_id));
        let ghost_chart_id = Uuid::new_v4();

        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardPanelsRepository::new(Arc::clone(&conn));

        let inspector_panel = DashboardPanelDto {
            dashboard_id: dashboard_id.to_string(),
            panel_index: 0,
            panel_kind: "inspector".to_string(),
            saved_chart_id: String::new(),
            divider_markdown: None,
            inspector_metric_id: Some("mysql.processlist".to_string()),
            title_override: None,
            grid_row: 0,
            grid_column: 0,
            grid_width: 12,
            grid_height: 4,
        };

        let divider_panel = DashboardPanelDto {
            dashboard_id: dashboard_id.to_string(),
            panel_index: 1,
            panel_kind: "divider".to_string(),
            saved_chart_id: String::new(),
            divider_markdown: Some("## Section".to_string()),
            inspector_metric_id: None,
            title_override: None,
            grid_row: 1,
            grid_column: 0,
            grid_width: 12,
            grid_height: 1,
        };

        let orphan_chart_panel = DashboardPanelDto {
            dashboard_id: dashboard_id.to_string(),
            panel_index: 2,
            panel_kind: "chart".to_string(),
            saved_chart_id: ghost_chart_id.to_string(),
            divider_markdown: None,
            inspector_metric_id: None,
            title_override: None,
            grid_row: 2,
            grid_column: 0,
            grid_width: 6,
            grid_height: 3,
        };

        repo.replace_panels_for_dashboard(
            dashboard_id,
            &[inspector_panel, divider_panel, orphan_chart_panel],
        )
        .expect("replace");

        let orphans = repo.count_orphans().expect("count");
        assert_eq!(
            orphans, 1,
            "only the chart panel with missing saved_chart_id must be counted as orphan"
        );
    }

    #[test]
    fn test_soft_ref_no_cascade() {
        // Deleting a viz_saved_charts row must NOT cascade to viz_dashboard_panels.
        let path = temp_db("soft_ref");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let chart_id = insert_chart(&conn, profile_id);
        let dashboard_id = insert_dashboard(&conn, Some(profile_id));

        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardPanelsRepository::new(Arc::clone(&conn));

        repo.replace_panels_for_dashboard(dashboard_id, &[panel(dashboard_id, 0, chart_id)])
            .expect("replace");

        // Delete the chart.
        {
            let locked = conn.lock().unwrap();
            locked
                .execute(
                    "DELETE FROM viz_saved_charts WHERE id = ?1",
                    [chart_id.to_string()],
                )
                .expect("delete chart");
        }

        // Panel row must still be present.
        let panels = repo.list_for_dashboard(dashboard_id).expect("list");
        assert_eq!(
            panels.len(),
            1,
            "panel must survive chart deletion (soft ref)"
        );

        let orphans = repo.count_orphans().expect("count");
        assert_eq!(orphans, 1, "panel is now an orphan");
    }
}
