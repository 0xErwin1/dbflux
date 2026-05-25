//! Repository for `viz_saved_chart_binding_y` — child rows for `BindingSpec.y`.
//!
//! Each row stores one entry in the `y: Vec<usize>` field of `BindingSpec`.
//! The composite PK `(chart_id, slot_index)` preserves list ordering.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use uuid::Uuid;

use crate::error::StorageError;

const DB_PATH: &str = "dbflux.db";

/// Data transfer object mirroring one row of `viz_saved_chart_binding_y`.
#[derive(Debug, Clone, PartialEq)]
pub struct BindingYDto {
    pub chart_id: String,
    pub slot_index: i64,
    pub column_index: i64,
}

/// Repository for `viz_saved_chart_binding_y`.
#[derive(Clone)]
pub struct SavedChartBindingYRepository {
    conn: Arc<Mutex<Connection>>,
}

impl SavedChartBindingYRepository {
    /// Creates a new repository wrapping the given shared connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Lists all binding_y slots for a chart, ordered by `slot_index ASC`.
    pub fn list_for_chart(&self, chart_id: Uuid) -> Result<Vec<BindingYDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT chart_id, slot_index, column_index
                 FROM viz_saved_chart_binding_y
                 WHERE chart_id = ?1
                 ORDER BY slot_index ASC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let rows = stmt
            .query_map([chart_id.to_string()], |row| {
                Ok(BindingYDto {
                    chart_id: row.get(0)?,
                    slot_index: row.get(1)?,
                    column_index: row.get(2)?,
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

    /// Atomically replaces all binding_y slots for a chart.
    ///
    /// Deletes every existing slot row for `chart_id` and reinserts the
    /// provided slice in a single transaction. On failure the original rows
    /// are preserved.
    pub fn replace_binding_y_for_chart(
        &self,
        chart_id: Uuid,
        slots: &[BindingYDto],
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let tx = conn
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        tx.execute(
            "DELETE FROM viz_saved_chart_binding_y WHERE chart_id = ?1",
            [chart_id.to_string()],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        for slot in slots {
            tx.execute(
                "INSERT INTO viz_saved_chart_binding_y
                     (chart_id, slot_index, column_index)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![slot.chart_id, slot.slot_index, slot.column_index],
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
            "dbflux_binding_y_{}_{}.db",
            suffix,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn setup(suffix: &str) -> (Arc<Mutex<Connection>>, SavedChartBindingYRepository, Uuid) {
        let path = temp_db(suffix);
        let conn = open_database(&path).expect("open db");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let chart_id = Uuid::new_v4();
        let profile_id = Uuid::new_v4();

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
        let repo = SavedChartBindingYRepository::new(Arc::clone(&conn));

        (conn, repo, chart_id)
    }

    #[test]
    fn test_binding_y_list_order() {
        let (_conn, repo, chart_id) = setup("list_order");

        let slots = vec![
            BindingYDto {
                chart_id: chart_id.to_string(),
                slot_index: 1,
                column_index: 10,
            },
            BindingYDto {
                chart_id: chart_id.to_string(),
                slot_index: 0,
                column_index: 5,
            },
        ];

        repo.replace_binding_y_for_chart(chart_id, &slots).expect("replace");

        let result = repo.list_for_chart(chart_id).expect("list");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].slot_index, 0, "first row should have slot_index 0");
        assert_eq!(result[1].slot_index, 1, "second row should have slot_index 1");
    }

    #[test]
    fn test_binding_y_replace_atomicity() {
        let (_conn, repo, chart_id) = setup("replace_atomicity");

        // Insert 2 valid slots.
        let initial = vec![
            BindingYDto {
                chart_id: chart_id.to_string(),
                slot_index: 0,
                column_index: 0,
            },
            BindingYDto {
                chart_id: chart_id.to_string(),
                slot_index: 1,
                column_index: 1,
            },
        ];
        repo.replace_binding_y_for_chart(chart_id, &initial).expect("initial replace");

        // column_index = -1 violates CHECK (column_index >= 0).
        let bad = vec![BindingYDto {
            chart_id: chart_id.to_string(),
            slot_index: 0,
            column_index: -1,
        }];
        let result = repo.replace_binding_y_for_chart(chart_id, &bad);
        assert!(result.is_err(), "should fail due to CHECK constraint");

        let after = repo.list_for_chart(chart_id).expect("list after failed replace");
        assert_eq!(after.len(), 2, "original 2 rows must survive the failed replace");
    }
}
