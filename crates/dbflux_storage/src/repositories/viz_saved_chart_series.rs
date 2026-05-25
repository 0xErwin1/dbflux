//! Repository for `viz_saved_chart_series` — child rows for `ChartSpec.series`.
//!
//! Each row represents one `SeriesSpec` entry in a saved chart's series list.
//! The composite PK `(chart_id, series_index)` preserves ordering.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use uuid::Uuid;

use crate::error::StorageError;

const DB_PATH: &str = "dbflux.db";

/// Data transfer object mirroring one row of `viz_saved_chart_series`.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesDto {
    pub chart_id: String,
    pub series_index: i64,
    pub column_index: i64,
    pub label: String,
    pub color_slot: i64,
}

/// Repository for `viz_saved_chart_series`.
#[derive(Clone)]
pub struct SavedChartSeriesRepository {
    conn: Arc<Mutex<Connection>>,
}

impl SavedChartSeriesRepository {
    /// Creates a new repository wrapping the given shared connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Lists all series for a chart, ordered by `series_index ASC`.
    pub fn list_for_chart(&self, chart_id: Uuid) -> Result<Vec<SeriesDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT chart_id, series_index, column_index, label, color_slot
                 FROM viz_saved_chart_series
                 WHERE chart_id = ?1
                 ORDER BY series_index ASC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let rows = stmt
            .query_map([chart_id.to_string()], |row| {
                Ok(SeriesDto {
                    chart_id: row.get(0)?,
                    series_index: row.get(1)?,
                    column_index: row.get(2)?,
                    label: row.get(3)?,
                    color_slot: row.get(4)?,
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

    /// Atomically replaces all series for a chart.
    ///
    /// Deletes every existing series row for `chart_id` and reinserts the
    /// provided slice in a single transaction. If any insert fails the
    /// transaction is rolled back and the original rows are preserved.
    pub fn replace_series_for_chart(
        &self,
        chart_id: Uuid,
        series: &[SeriesDto],
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let tx = conn
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        tx.execute(
            "DELETE FROM viz_saved_chart_series WHERE chart_id = ?1",
            [chart_id.to_string()],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        for row in series {
            tx.execute(
                "INSERT INTO viz_saved_chart_series
                     (chart_id, series_index, column_index, label, color_slot)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    row.chart_id,
                    row.series_index,
                    row.column_index,
                    row.label,
                    row.color_slot,
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
            "dbflux_series_{}_{}.db",
            suffix,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn setup(suffix: &str) -> (Arc<Mutex<Connection>>, SavedChartSeriesRepository, Uuid) {
        let path = temp_db(suffix);
        let conn = open_database(&path).expect("open db");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        // Insert a parent chart row so FK constraints are satisfied.
        let chart_id = Uuid::new_v4();
        let profile_id = Uuid::new_v4();

        // First insert a profile row to satisfy the FK on viz_saved_charts.
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, ?2)",
            rusqlite::params![profile_id.to_string(), "test-profile"],
        )
        .expect("insert profile");

        conn.execute(
            "INSERT INTO viz_saved_charts
             (id, name, profile_id, created_at, updated_at,
              chart_kind, legend_visible, decimation_threshold, track_source_indices,
              y_scale, x_axis_column_index, x_axis_label, x_axis_kind,
              binding_x, binding_aggregation, source_kind, source_query,
              refresh_policy_kind)
             VALUES (?1, 'Test', ?2, 0, 0,
                     'line', 0, 10000, 0,
                     'linear', 0, 'X', 'time',
                     0, 'none', 'query', 'SELECT 1',
                     'off')",
            rusqlite::params![chart_id.to_string(), profile_id.to_string()],
        )
        .expect("insert chart");

        let conn = Arc::new(Mutex::new(conn));
        let repo = SavedChartSeriesRepository::new(Arc::clone(&conn));

        (conn, repo, chart_id)
    }

    #[test]
    fn test_series_list_order() {
        let (_conn, repo, chart_id) = setup("list_order");

        let series = vec![
            SeriesDto {
                chart_id: chart_id.to_string(),
                series_index: 2,
                column_index: 2,
                label: "C".to_string(),
                color_slot: 2,
            },
            SeriesDto {
                chart_id: chart_id.to_string(),
                series_index: 0,
                column_index: 0,
                label: "A".to_string(),
                color_slot: 0,
            },
            SeriesDto {
                chart_id: chart_id.to_string(),
                series_index: 1,
                column_index: 1,
                label: "B".to_string(),
                color_slot: 1,
            },
        ];

        repo.replace_series_for_chart(chart_id, &series).expect("replace");

        let result = repo.list_for_chart(chart_id).expect("list");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].series_index, 0, "first row should have series_index 0");
        assert_eq!(result[1].series_index, 1, "second row should have series_index 1");
        assert_eq!(result[2].series_index, 2, "third row should have series_index 2");
    }

    #[test]
    fn test_series_replace_atomicity() {
        let (_conn, repo, chart_id) = setup("replace_atomicity");

        // Insert 2 valid series.
        let initial = vec![
            SeriesDto {
                chart_id: chart_id.to_string(),
                series_index: 0,
                column_index: 0,
                label: "A".to_string(),
                color_slot: 0,
            },
            SeriesDto {
                chart_id: chart_id.to_string(),
                series_index: 1,
                column_index: 1,
                label: "B".to_string(),
                color_slot: 1,
            },
        ];
        repo.replace_series_for_chart(chart_id, &initial).expect("initial replace");

        // Attempt to replace with a series that violates CHECK (color_slot BETWEEN 0 AND 255).
        let bad = vec![SeriesDto {
            chart_id: chart_id.to_string(),
            series_index: 0,
            column_index: 0,
            label: "Bad".to_string(),
            color_slot: 300, // violates CHECK
        }];
        let result = repo.replace_series_for_chart(chart_id, &bad);
        assert!(result.is_err(), "should fail due to CHECK constraint");

        // Original 2 rows must still be present.
        let after = repo.list_for_chart(chart_id).expect("list after failed replace");
        assert_eq!(after.len(), 2, "original 2 rows must survive the failed replace");
    }
}
