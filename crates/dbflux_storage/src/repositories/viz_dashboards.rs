//! Repository for `viz_dashboards` — one row per dashboard.
//!
//! `profile_id` uses ON DELETE SET NULL so dashboards survive the deletion
//! of their associated connection profile and become unbound artifacts.
//! The panels FK (`viz_dashboard_panels.dashboard_id`) uses ON DELETE CASCADE.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use uuid::Uuid;

use crate::error::StorageError;

const DB_PATH: &str = "dbflux.db";

/// Data transfer object mirroring one row of `viz_dashboards`.
#[derive(Debug, Clone, PartialEq)]
pub struct DashboardDto {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub profile_id: Option<String>,
    pub shared_time_range_preset: Option<String>,
    pub shared_refresh_policy_kind: String,
    pub shared_refresh_policy_interval_secs: Option<i64>,
    pub grid_columns: i64,
    pub created_at: i64,
    pub updated_at: i64,
    /// Origin discriminator: `"local"` (default) or `"cloudwatch"` (imported).
    pub source_kind: String,
    /// AWS account id when imported from CloudWatch; `None` for local or
    /// detached dashboards whose STS lookup failed.
    pub source_account_id: Option<String>,
    /// Home region recorded at import time.
    pub source_home_region: Option<String>,
    /// Dashboard name in the upstream system (may differ from `name`).
    pub source_dashboard_name: Option<String>,
    /// Canonicalized SHA256 of the upstream JSON body with the `"v1:"` prefix.
    pub source_content_hash: Option<String>,
    /// ISO8601 `lastModified` returned by upstream `ListDashboards`.
    pub source_last_modified: Option<String>,
    /// ISO8601 timestamp of the last successful drift check or apply.
    pub source_last_synced_at: Option<String>,
}

/// Repository for `viz_dashboards`.
#[derive(Clone)]
pub struct DashboardsRepository {
    conn: Arc<Mutex<Connection>>,
}

impl DashboardsRepository {
    /// Creates a new repository wrapping the given shared connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Lists all dashboards ordered by `updated_at DESC`.
    pub fn list(&self) -> Result<Vec<DashboardDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        Self::query_rows(&conn, "ORDER BY updated_at DESC", [])
    }

    /// Lists all dashboards for a specific profile, ordered by `updated_at DESC`.
    pub fn list_by_profile(&self, profile_id: Uuid) -> Result<Vec<DashboardDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        let mut stmt = conn
            .prepare(
                "SELECT id, name, description, profile_id,
                        shared_time_range_preset,
                        shared_refresh_policy_kind,
                        shared_refresh_policy_interval_secs,
                        grid_columns, created_at, updated_at,
                        source_kind, source_account_id, source_home_region,
                        source_dashboard_name, source_content_hash,
                        source_last_modified, source_last_synced_at
                 FROM viz_dashboards
                 WHERE profile_id = ?1
                 ORDER BY updated_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let rows = stmt
            .query_map([profile_id.to_string()], map_row)
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Returns a single dashboard by its UUID, or `None` if not found.
    pub fn get_by_id(&self, id: Uuid) -> Result<Option<DashboardDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT id, name, description, profile_id,
                        shared_time_range_preset,
                        shared_refresh_policy_kind,
                        shared_refresh_policy_interval_secs,
                        grid_columns, created_at, updated_at,
                        source_kind, source_account_id, source_home_region,
                        source_dashboard_name, source_content_hash,
                        source_last_modified, source_last_synced_at
                 FROM viz_dashboards
                 WHERE id = ?1",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let mut rows: Vec<DashboardDto> = stmt
            .query_map([id.to_string()], map_row)
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows.pop())
    }

    /// Inserts or replaces a dashboard row, bumping `updated_at` to now.
    pub fn upsert(&self, dashboard: &DashboardDto) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let now_ms = now_millis();

        conn.execute(
            "INSERT OR REPLACE INTO viz_dashboards
                 (id, name, description, profile_id,
                  shared_time_range_preset,
                  shared_refresh_policy_kind,
                  shared_refresh_policy_interval_secs,
                  grid_columns, created_at, updated_at,
                  source_kind, source_account_id, source_home_region,
                  source_dashboard_name, source_content_hash,
                  source_last_modified, source_last_synced_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                     ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            rusqlite::params![
                dashboard.id,
                dashboard.name,
                dashboard.description,
                dashboard.profile_id,
                dashboard.shared_time_range_preset,
                dashboard.shared_refresh_policy_kind,
                dashboard.shared_refresh_policy_interval_secs,
                dashboard.grid_columns,
                dashboard.created_at,
                now_ms,
                dashboard.source_kind,
                dashboard.source_account_id,
                dashboard.source_home_region,
                dashboard.source_dashboard_name,
                dashboard.source_content_hash,
                dashboard.source_last_modified,
                dashboard.source_last_synced_at,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        Ok(())
    }

    /// Deletes a dashboard by UUID. The panels FK cascades, so all associated
    /// panel rows are removed automatically.
    pub fn delete(&self, id: Uuid) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        conn.execute("DELETE FROM viz_dashboards WHERE id = ?1", [id.to_string()])
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        Ok(())
    }

    /// Returns the distinct dashboard IDs that have at least one panel referencing
    /// `chart_id` via `viz_dashboard_panels.saved_chart_id`. Used by the
    /// delete-saved-chart confirmation modal to show the orphan-impact count.
    ///
    /// SQL: `SELECT DISTINCT dashboard_id FROM viz_dashboard_panels
    ///        WHERE saved_chart_id = ?1 ORDER BY dashboard_id`
    pub fn find_dashboards_referencing_chart(
        &self,
        chart_id: Uuid,
    ) -> Result<Vec<Uuid>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT dashboard_id
                 FROM viz_dashboard_panels
                 WHERE saved_chart_id = ?1
                 ORDER BY dashboard_id",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let ids: Vec<Uuid> = stmt
            .query_map([chart_id.to_string()], |row| row.get::<_, String>(0))
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .filter_map(|s| {
                Uuid::parse_str(&s)
                    .map_err(|e| {
                        log::warn!(
                            "find_dashboards_referencing_chart: invalid uuid '{}': {e}",
                            s
                        );
                    })
                    .ok()
            })
            .collect();

        Ok(ids)
    }

    /// Looks up a CloudWatch-imported dashboard by `(account_id, dashboard_name)`.
    ///
    /// Returns `None` when no `source_kind = 'cloudwatch'` row matches. Used
    /// during import to decide whether to update an existing row in place or
    /// insert a new one (R1.5 idempotent re-import).
    pub fn find_by_cw_identity(
        &self,
        account_id: &str,
        dashboard_name: &str,
    ) -> Result<Option<DashboardDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT id, name, description, profile_id,
                        shared_time_range_preset,
                        shared_refresh_policy_kind,
                        shared_refresh_policy_interval_secs,
                        grid_columns, created_at, updated_at,
                        source_kind, source_account_id, source_home_region,
                        source_dashboard_name, source_content_hash,
                        source_last_modified, source_last_synced_at
                 FROM viz_dashboards
                 WHERE source_kind = 'cloudwatch'
                   AND source_account_id = ?1
                   AND source_dashboard_name = ?2
                 LIMIT 1",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let mut rows: Vec<DashboardDto> = stmt
            .query_map([account_id, dashboard_name], map_row)
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows.pop())
    }

    /// Updates the sync-state columns on `viz_dashboards` for a single dashboard.
    ///
    /// Touches only `source_content_hash`, `source_last_modified`,
    /// `source_last_synced_at`, and bumps `updated_at`. All other identity
    /// columns are preserved.
    pub fn update_sync_state(
        &self,
        dashboard_id: Uuid,
        content_hash: &str,
        last_modified: Option<&str>,
        last_synced_at: &str,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        let now_ms = now_millis();

        conn.execute(
            "UPDATE viz_dashboards
                SET source_content_hash   = ?1,
                    source_last_modified  = ?2,
                    source_last_synced_at = ?3,
                    updated_at            = ?4
              WHERE id = ?5",
            rusqlite::params![
                content_hash,
                last_modified,
                last_synced_at,
                now_ms,
                dashboard_id.to_string(),
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        Ok(())
    }

    // Internal helper: runs a full SELECT with a caller-supplied ORDER BY clause.
    fn query_rows<P: rusqlite::Params>(
        conn: &Connection,
        order: &str,
        params: P,
    ) -> Result<Vec<DashboardDto>, StorageError> {
        let sql = format!(
            "SELECT id, name, description, profile_id,
                    shared_time_range_preset,
                    shared_refresh_policy_kind,
                    shared_refresh_policy_interval_secs,
                    grid_columns, created_at, updated_at,
                    source_kind, source_account_id, source_home_region,
                    source_dashboard_name, source_content_hash,
                    source_last_modified, source_last_synced_at
             FROM viz_dashboards
             {order}"
        );

        let mut stmt = conn.prepare(&sql).map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        let rows = stmt
            .query_map(params, map_row)
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }
}

fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DashboardDto> {
    Ok(DashboardDto {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        profile_id: row.get(3)?,
        shared_time_range_preset: row.get(4)?,
        shared_refresh_policy_kind: row.get(5)?,
        shared_refresh_policy_interval_secs: row.get(6)?,
        grid_columns: row.get(7)?,
        created_at: row.get(8)?,
        updated_at: row.get(9)?,
        source_kind: row.get(10)?,
        source_account_id: row.get(11)?,
        source_home_region: row.get(12)?,
        source_dashboard_name: row.get(13)?,
        source_content_hash: row.get(14)?,
        source_last_modified: row.get(15)?,
        source_last_synced_at: row.get(16)?,
    })
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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
    use crate::repositories::viz_dashboard_panels::{DashboardPanelDto, DashboardPanelsRepository};
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(suffix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_dashboards_{}_{}.db",
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

    fn make_dashboard(id: Uuid, profile_id: Option<Uuid>) -> DashboardDto {
        DashboardDto {
            id: id.to_string(),
            name: "My Dashboard".to_string(),
            description: Some("desc".to_string()),
            profile_id: profile_id.map(|p| p.to_string()),
            shared_time_range_preset: Some("last_hour".to_string()),
            shared_refresh_policy_kind: "interval".to_string(),
            shared_refresh_policy_interval_secs: Some(60),
            grid_columns: 2,
            created_at: 1_000_000,
            updated_at: 1_000_000,
            source_kind: "local".to_string(),
            source_account_id: None,
            source_home_region: None,
            source_dashboard_name: None,
            source_content_hash: None,
            source_last_modified: None,
            source_last_synced_at: None,
        }
    }

    #[test]
    fn test_dashboard_upsert_roundtrip() {
        let path = temp_db("upsert_roundtrip");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardsRepository::new(Arc::clone(&conn));

        let id = Uuid::new_v4();
        let dto = make_dashboard(id, Some(profile_id));
        repo.upsert(&dto).expect("upsert");

        let loaded = repo.get_by_id(id).expect("get").expect("should exist");
        assert_eq!(loaded.id, dto.id);
        assert_eq!(loaded.name, dto.name);
        assert_eq!(loaded.description, dto.description);
        assert_eq!(loaded.profile_id, dto.profile_id);
        assert_eq!(
            loaded.shared_time_range_preset,
            dto.shared_time_range_preset
        );
        assert_eq!(
            loaded.shared_refresh_policy_kind,
            dto.shared_refresh_policy_kind
        );
        assert_eq!(
            loaded.shared_refresh_policy_interval_secs,
            dto.shared_refresh_policy_interval_secs
        );
        assert_eq!(loaded.grid_columns, dto.grid_columns);
    }

    #[test]
    fn test_dashboard_delete_cascades_panels() {
        let path = temp_db("delete_cascades");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let chart_id = insert_chart(&conn, profile_id);
        let conn = Arc::new(Mutex::new(conn));

        let dashboard_repo = DashboardsRepository::new(Arc::clone(&conn));
        let panels_repo = DashboardPanelsRepository::new(Arc::clone(&conn));

        let id = Uuid::new_v4();
        dashboard_repo
            .upsert(&make_dashboard(id, Some(profile_id)))
            .expect("upsert");
        panels_repo
            .replace_panels_for_dashboard(
                id,
                &[DashboardPanelDto {
                    dashboard_id: id.to_string(),
                    panel_index: 0,
                    panel_kind: "chart".to_string(),
                    divider_markdown: None,
                    saved_chart_id: chart_id.to_string(),
                    title_override: None,
                    grid_row: 0,
                    grid_column: 0,
                    grid_width: 1,
                    grid_height: 1,
                    source_widget_index: None,
                    source_widget_hash: None,
                }],
            )
            .expect("insert panel");

        dashboard_repo.delete(id).expect("delete");

        let panels = panels_repo.list_for_dashboard(id).expect("list");
        assert!(
            panels.is_empty(),
            "panels must be cascaded on dashboard delete"
        );
    }

    #[test]
    fn test_dashboard_profile_id_set_null_on_profile_delete() {
        let path = temp_db("profile_set_null");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardsRepository::new(Arc::clone(&conn));

        let id = Uuid::new_v4();
        repo.upsert(&make_dashboard(id, Some(profile_id)))
            .expect("upsert");

        // Delete the profile.
        {
            let locked = conn.lock().unwrap();
            locked
                .execute(
                    "DELETE FROM cfg_connection_profiles WHERE id = ?1",
                    [profile_id.to_string()],
                )
                .expect("delete profile");
        }

        let loaded = repo
            .get_by_id(id)
            .expect("get")
            .expect("dashboard must still exist");
        assert!(
            loaded.profile_id.is_none(),
            "profile_id must be NULL after profile deletion (SET NULL FK)"
        );
    }

    #[test]
    fn test_dashboard_check_refresh_policy_interval_secs_required() {
        let path = temp_db("check_interval");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);

        // interval kind without interval_secs must violate CHECK.
        let result = conn.execute(
            "INSERT INTO viz_dashboards
             (id, name, profile_id, shared_refresh_policy_kind,
              shared_refresh_policy_interval_secs, grid_columns, created_at, updated_at)
             VALUES (?1, 'D', ?2, 'interval', NULL, 2, 0, 0)",
            rusqlite::params![Uuid::new_v4().to_string(), profile_id.to_string()],
        );

        assert!(
            result.is_err(),
            "should fail: interval kind requires interval_secs"
        );
    }

    // --- K.1 tests: find_dashboards_referencing_chart ---

    fn setup_panels_db(
        suffix: &str,
    ) -> (
        Arc<Mutex<Connection>>,
        DashboardsRepository,
        DashboardPanelsRepository,
        Uuid,
    ) {
        let path = temp_db(suffix);
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");
        let profile_id = insert_profile(&conn);
        let conn = Arc::new(Mutex::new(conn));
        let dashboards_repo = DashboardsRepository::new(Arc::clone(&conn));
        let panels_repo = DashboardPanelsRepository::new(Arc::clone(&conn));
        (conn, dashboards_repo, panels_repo, profile_id)
    }

    fn insert_dashboard(repo: &DashboardsRepository, profile_id: Uuid) -> Uuid {
        let id = Uuid::new_v4();
        repo.upsert(&make_dashboard(id, Some(profile_id)))
            .expect("upsert dashboard");
        id
    }

    fn insert_panel(
        panels_repo: &DashboardPanelsRepository,
        dashboard_id: Uuid,
        chart_id: Uuid,
        index: i64,
    ) {
        panels_repo
            .replace_panels_for_dashboard(
                dashboard_id,
                &[DashboardPanelDto {
                    dashboard_id: dashboard_id.to_string(),
                    panel_index: index,
                    panel_kind: "chart".to_string(),
                    saved_chart_id: chart_id.to_string(),
                    divider_markdown: None,
                    title_override: None,
                    grid_row: 0,
                    grid_column: 0,
                    grid_width: 1,
                    grid_height: 1,
                    source_widget_index: None,
                    source_widget_hash: None,
                }],
            )
            .expect("insert panel");
    }

    #[test]
    fn test_find_dashboards_referencing_chart_returns_empty_when_none() {
        let (_conn, repo, _panels_repo, _profile_id) = setup_panels_db("ref_empty");
        let chart_id = Uuid::new_v4();
        let result = repo
            .find_dashboards_referencing_chart(chart_id)
            .expect("query");
        assert!(
            result.is_empty(),
            "no panels reference the chart; should return empty"
        );
    }

    #[test]
    fn test_find_dashboards_referencing_chart_returns_single_dashboard() {
        let (conn, repo, panels_repo, profile_id) = setup_panels_db("ref_single");
        let chart_id = {
            let locked = conn.lock().unwrap();
            insert_chart(&locked, profile_id)
        };
        let dashboard_id = insert_dashboard(&repo, profile_id);
        insert_panel(&panels_repo, dashboard_id, chart_id, 0);

        let result = repo
            .find_dashboards_referencing_chart(chart_id)
            .expect("query");
        assert_eq!(result, vec![dashboard_id]);
    }

    #[test]
    fn test_find_dashboards_referencing_chart_returns_distinct_dashboard_ids() {
        let (conn, repo, panels_repo, profile_id) = setup_panels_db("ref_distinct");
        let chart_x = {
            let locked = conn.lock().unwrap();
            insert_chart(&locked, profile_id)
        };
        let other_chart = {
            let locked = conn.lock().unwrap();
            insert_chart(&locked, profile_id)
        };

        let dashboard_a = insert_dashboard(&repo, profile_id);
        let dashboard_b = insert_dashboard(&repo, profile_id);

        // dashboard_a: one panel referencing chart_x
        insert_panel(&panels_repo, dashboard_a, chart_x, 0);
        // dashboard_b: two panels referencing chart_x (via replace_panels, can only use unique panel_index)
        // We simulate "twice in B" by inserting a second chart also in B alongside chart_x
        panels_repo
            .replace_panels_for_dashboard(
                dashboard_b,
                &[
                    DashboardPanelDto {
                        dashboard_id: dashboard_b.to_string(),
                        panel_index: 0,
                        panel_kind: "chart".to_string(),
                        saved_chart_id: chart_x.to_string(),
                        divider_markdown: None,
                        title_override: None,
                        grid_row: 0,
                        grid_column: 0,
                        grid_width: 1,
                        grid_height: 1,
                        source_widget_index: None,
                        source_widget_hash: None,
                    },
                    DashboardPanelDto {
                        dashboard_id: dashboard_b.to_string(),
                        panel_index: 1,
                        panel_kind: "chart".to_string(),
                        saved_chart_id: other_chart.to_string(),
                        divider_markdown: None,
                        title_override: None,
                        grid_row: 0,
                        grid_column: 1,
                        grid_width: 1,
                        grid_height: 1,
                        source_widget_index: None,
                        source_widget_hash: None,
                    },
                ],
            )
            .expect("insert panels b");

        let mut result = repo
            .find_dashboards_referencing_chart(chart_x)
            .expect("query");
        result.sort();
        let mut expected = vec![dashboard_a, dashboard_b];
        expected.sort();
        assert_eq!(
            result, expected,
            "should return exactly [A, B] without duplicates"
        );
    }

    #[test]
    fn test_find_dashboards_referencing_chart_returns_all_three() {
        let (conn, repo, panels_repo, profile_id) = setup_panels_db("ref_three");
        let chart_x = {
            let locked = conn.lock().unwrap();
            insert_chart(&locked, profile_id)
        };

        let d1 = insert_dashboard(&repo, profile_id);
        let d2 = insert_dashboard(&repo, profile_id);
        let d3 = insert_dashboard(&repo, profile_id);

        insert_panel(&panels_repo, d1, chart_x, 0);
        insert_panel(&panels_repo, d2, chart_x, 0);
        insert_panel(&panels_repo, d3, chart_x, 0);

        let result = repo
            .find_dashboards_referencing_chart(chart_x)
            .expect("query");
        assert_eq!(
            result.len(),
            3,
            "should return all three referencing dashboards"
        );
    }

    // --- Sync identity tests (B2) ---

    #[test]
    fn legacy_row_reads_back_as_local_with_nulls() {
        let path = temp_db("sync_legacy");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);

        // Insert a row WITHOUT the new identity columns -- defaults kick in.
        conn.execute(
            "INSERT INTO viz_dashboards
                 (id, name, profile_id, shared_refresh_policy_kind, grid_columns,
                  created_at, updated_at)
             VALUES (?1, 'Legacy', ?2, 'off', 12, 0, 0)",
            rusqlite::params![Uuid::new_v4().to_string(), profile_id.to_string()],
        )
        .unwrap();

        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardsRepository::new(Arc::clone(&conn));
        let rows = repo.list().expect("list");
        let row = rows
            .iter()
            .find(|r| r.name == "Legacy")
            .expect("legacy row");
        assert_eq!(row.source_kind, "local");
        assert!(row.source_account_id.is_none());
        assert!(row.source_content_hash.is_none());
        assert!(row.source_last_synced_at.is_none());
    }

    #[test]
    fn upsert_round_trip_with_full_identity_tuple() {
        let path = temp_db("sync_roundtrip");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardsRepository::new(Arc::clone(&conn));

        let id = Uuid::new_v4();
        let dto = DashboardDto {
            source_kind: "cloudwatch".into(),
            source_account_id: Some("123456789012".into()),
            source_home_region: Some("us-east-1".into()),
            source_dashboard_name: Some("prod-overview".into()),
            source_content_hash: Some("v1:abc123".into()),
            source_last_modified: Some("2026-05-27T10:00:00Z".into()),
            source_last_synced_at: Some("2026-05-27T10:05:00Z".into()),
            ..make_dashboard(id, Some(profile_id))
        };

        repo.upsert(&dto).expect("upsert");
        let loaded = repo.get_by_id(id).expect("get").expect("exists");
        assert_eq!(loaded.source_kind, "cloudwatch");
        assert_eq!(loaded.source_account_id.as_deref(), Some("123456789012"));
        assert_eq!(loaded.source_home_region.as_deref(), Some("us-east-1"));
        assert_eq!(
            loaded.source_dashboard_name.as_deref(),
            Some("prod-overview")
        );
        assert_eq!(loaded.source_content_hash.as_deref(), Some("v1:abc123"));
        assert_eq!(
            loaded.source_last_modified.as_deref(),
            Some("2026-05-27T10:00:00Z")
        );
        assert_eq!(
            loaded.source_last_synced_at.as_deref(),
            Some("2026-05-27T10:05:00Z")
        );
    }

    #[test]
    fn find_by_cw_identity_returns_existing_row() {
        let path = temp_db("sync_find_identity");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardsRepository::new(Arc::clone(&conn));

        let id = Uuid::new_v4();
        let dto = DashboardDto {
            source_kind: "cloudwatch".into(),
            source_account_id: Some("999".into()),
            source_dashboard_name: Some("my-dash".into()),
            ..make_dashboard(id, Some(profile_id))
        };
        repo.upsert(&dto).expect("upsert");

        let found = repo
            .find_by_cw_identity("999", "my-dash")
            .expect("query")
            .expect("must find");
        assert_eq!(found.id, id.to_string());

        let none = repo
            .find_by_cw_identity("999", "other-dash")
            .expect("query");
        assert!(none.is_none());
    }

    #[test]
    fn update_sync_state_touches_only_sync_columns() {
        let path = temp_db("sync_update");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = insert_profile(&conn);
        let conn = Arc::new(Mutex::new(conn));
        let repo = DashboardsRepository::new(Arc::clone(&conn));

        let id = Uuid::new_v4();
        let dto = DashboardDto {
            source_kind: "cloudwatch".into(),
            source_account_id: Some("a1".into()),
            source_home_region: Some("us-east-1".into()),
            source_dashboard_name: Some("d1".into()),
            source_content_hash: Some("v1:old".into()),
            source_last_modified: Some("2026-01-01T00:00:00Z".into()),
            source_last_synced_at: Some("2026-01-01T00:00:00Z".into()),
            ..make_dashboard(id, Some(profile_id))
        };
        repo.upsert(&dto).expect("upsert");

        repo.update_sync_state(
            id,
            "v1:new",
            Some("2026-05-01T00:00:00Z"),
            "2026-05-27T12:00:00Z",
        )
        .expect("update");

        let loaded = repo.get_by_id(id).expect("get").expect("exists");
        assert_eq!(loaded.source_content_hash.as_deref(), Some("v1:new"));
        assert_eq!(
            loaded.source_last_modified.as_deref(),
            Some("2026-05-01T00:00:00Z")
        );
        assert_eq!(
            loaded.source_last_synced_at.as_deref(),
            Some("2026-05-27T12:00:00Z")
        );
        // Other identity columns must be unchanged.
        assert_eq!(loaded.source_account_id.as_deref(), Some("a1"));
        assert_eq!(loaded.source_home_region.as_deref(), Some("us-east-1"));
        assert_eq!(loaded.source_dashboard_name.as_deref(), Some("d1"));
        assert_eq!(loaded.source_kind, "cloudwatch");
    }
}
