//! Repository for `qry_saved_queries` and its normalized child tables.
//!
//! Persists `VisualQuerySpec` from `dbflux_core` across four SQLite tables:
//! - `qry_saved_queries` — root row with stable scalar columns + one JSON column
//!   for the recursive `FilterNode` tree.
//! - `qry_saved_query_columns` — projected column list (explicit projection only).
//! - `qry_saved_query_sorts` — sort entries in user-defined order.
//! - `qry_saved_query_joins` — join steps in user-defined order.
//!
//! All mutating methods wrap parent + child writes in a single transaction:
//! delete existing child rows, then re-insert from the spec. This keeps the
//! operation atomic and avoids partial-update states.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use uuid::Uuid;

use dbflux_core::{
    FilterNode, JoinKind, JoinOn, JoinStep, ProjectedColumn, Projection, SortEntry, SourceTable,
    VisualQuerySpec, VisualSortDirection,
};

use crate::error::StorageError;

const DB_PATH: &str = "dbflux.db";

/// A lightweight summary row returned by list operations — avoids
/// deserializing the full spec when only the id and name are needed.
#[derive(Debug, Clone, PartialEq)]
pub struct SavedQuerySummary {
    pub id: String,
    pub profile_id: String,
    pub name: String,
    pub table_name: String,
    pub updated_at: i64,
}

/// Repository for the `qry_saved_queries` table family.
#[derive(Clone)]
pub struct SavedQueryRepo {
    conn: Arc<Mutex<Connection>>,
}

impl SavedQueryRepo {
    /// Creates a new repository wrapping the given shared connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Lists summary rows for all saved queries belonging to `profile_id`,
    /// ordered by `updated_at DESC`.
    pub fn list_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Vec<SavedQuerySummary>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT id, profile_id, name, table_name, updated_at
                 FROM qry_saved_queries
                 WHERE profile_id = ?1
                 ORDER BY updated_at DESC",
            )
            .map_err(sqlite_err)?;

        let rows = stmt
            .query_map([profile_id], map_summary_row)
            .map_err(sqlite_err)?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Lists summary rows for all saved queries across all profiles, ordered
    /// by `updated_at DESC`.
    pub fn list_all(&self) -> Result<Vec<SavedQuerySummary>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT id, profile_id, name, table_name, updated_at
                 FROM qry_saved_queries
                 ORDER BY updated_at DESC",
            )
            .map_err(sqlite_err)?;

        let rows = stmt
            .query_map([], map_summary_row)
            .map_err(sqlite_err)?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Returns the full spec for a saved query by its UUID string, or `None`
    /// if the id is not found.
    pub fn get(&self, id: &str) -> Result<Option<VisualQuerySpec>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        load_spec(&conn, id)
    }

    /// Inserts or replaces the saved query identified by `id`. If a row with
    /// the given id already exists it is replaced in full; child rows are
    /// deleted and re-inserted from the spec.
    ///
    /// Returns the summary of the written row.
    pub fn upsert_by_id(
        &self,
        id: &str,
        profile_id: &str,
        name: &str,
        spec: &VisualQuerySpec,
    ) -> Result<SavedQuerySummary, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        let now_ms = now_millis();

        let tx = conn.unchecked_transaction().map_err(sqlite_err)?;

        write_row(&tx, id, profile_id, name, spec, now_ms)?;
        write_children_from_spec(&tx, id, spec)?;

        tx.commit().map_err(sqlite_err)?;

        Ok(SavedQuerySummary {
            id: id.to_string(),
            profile_id: profile_id.to_string(),
            name: name.to_string(),
            table_name: spec.source.table.clone(),
            updated_at: now_ms,
        })
    }

    /// Inserts or updates by `(profile_id, name)`. If a row with the same
    /// profile and name already exists, it is updated in place (same `id`).
    /// If no such row exists, a new UUID v7 is minted.
    ///
    /// Returns the summary (with the stable `id` of the written row).
    pub fn upsert_by_name(
        &self,
        profile_id: &str,
        name: &str,
        spec: &VisualQuerySpec,
    ) -> Result<SavedQuerySummary, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        let now_ms = now_millis();

        let existing_id: Option<String> = conn
            .query_row(
                "SELECT id FROM qry_saved_queries WHERE profile_id = ?1 AND name = ?2",
                rusqlite::params![profile_id, name],
                |row| row.get(0),
            )
            .ok();

        let id = match existing_id {
            Some(id) => id,
            None => Uuid::now_v7().to_string(),
        };

        let tx = conn.unchecked_transaction().map_err(sqlite_err)?;

        write_row(&tx, &id, profile_id, name, spec, now_ms)?;
        write_children_from_spec(&tx, &id, spec)?;

        tx.commit().map_err(sqlite_err)?;

        Ok(SavedQuerySummary {
            id,
            profile_id: profile_id.to_string(),
            name: name.to_string(),
            table_name: spec.source.table.clone(),
            updated_at: now_ms,
        })
    }

    /// Deletes a saved query by id. Child rows are removed by CASCADE.
    pub fn delete(&self, id: &str) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        conn.execute("DELETE FROM qry_saved_queries WHERE id = ?1", [id])
            .map_err(sqlite_err)?;

        Ok(())
    }

    /// Renames a saved query. Returns `Err` when `new_name` conflicts with
    /// another row in the same profile (UNIQUE constraint violation).
    pub fn rename(&self, id: &str, new_name: &str) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        let now_ms = now_millis();

        conn.execute(
            "UPDATE qry_saved_queries SET name = ?1, updated_at = ?2 WHERE id = ?3",
            rusqlite::params![new_name, now_ms, id],
        )
        .map_err(sqlite_err)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Private helpers — write path
// ---------------------------------------------------------------------------

/// Writes (INSERT OR REPLACE) the parent row.
fn write_row(
    tx: &rusqlite::Transaction,
    id: &str,
    profile_id: &str,
    name: &str,
    spec: &VisualQuerySpec,
    now_ms: i64,
) -> Result<(), StorageError> {
    let filter_json = spec
        .filter
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| StorageError::Sqlite {
            path: DB_PATH.into(),
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

    let projection_mode = match &spec.projection {
        Projection::All => "all",
        Projection::Explicit(_) => "explicit",
    };

    tx.execute(
        "INSERT OR REPLACE INTO qry_saved_queries
             (id, profile_id, name, schema_name, table_name, source_alias,
              projection_mode, limit_value, offset_value, filter_json,
              created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                 COALESCE(
                     (SELECT created_at FROM qry_saved_queries WHERE id = ?1),
                     ?11
                 ),
                 ?11)",
        rusqlite::params![
            id,
            profile_id,
            name,
            spec.source.schema,
            spec.source.table,
            spec.source.alias,
            projection_mode,
            spec.limit
                .and_then(|l| if l == 0 { None } else { Some(l as i64) }),
            spec.offset as i64,
            filter_json,
            now_ms,
        ],
    )
    .map_err(sqlite_err)?;

    Ok(())
}

/// Deletes and re-inserts all child rows for the given saved-query id.
fn write_children(tx: &rusqlite::Transaction, id: &str) -> Result<(), StorageError> {
    tx.execute(
        "DELETE FROM qry_saved_query_columns WHERE saved_query_id = ?1",
        [id],
    )
    .map_err(sqlite_err)?;

    tx.execute(
        "DELETE FROM qry_saved_query_sorts WHERE saved_query_id = ?1",
        [id],
    )
    .map_err(sqlite_err)?;

    tx.execute(
        "DELETE FROM qry_saved_query_joins WHERE saved_query_id = ?1",
        [id],
    )
    .map_err(sqlite_err)?;

    Ok(())
}

/// Deletes and re-inserts all child rows from the spec.
fn write_children_from_spec(
    tx: &rusqlite::Transaction,
    id: &str,
    spec: &VisualQuerySpec,
) -> Result<(), StorageError> {
    write_children(tx, id)?;

    if let Projection::Explicit(columns) = &spec.projection {
        for (position, col) in columns.iter().enumerate() {
            tx.execute(
                "INSERT INTO qry_saved_query_columns
                     (saved_query_id, position, source_alias, column_name, alias)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![id, position as i64, col.source_alias, col.column, col.alias],
            )
            .map_err(sqlite_err)?;
        }
    }

    for (position, sort) in spec.sort.iter().enumerate() {
        let direction = match sort.direction {
            VisualSortDirection::Asc => "asc",
            VisualSortDirection::Desc => "desc",
        };
        tx.execute(
            "INSERT INTO qry_saved_query_sorts
                 (saved_query_id, position, source_alias, column_name, direction)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                id,
                position as i64,
                sort.source_alias,
                sort.column,
                direction
            ],
        )
        .map_err(sqlite_err)?;
    }

    for (position, join) in spec.joins.iter().enumerate() {
        let join_kind = match join.kind {
            JoinKind::Inner => "inner",
            JoinKind::Left => "left",
            JoinKind::Right => "right",
            JoinKind::Full => "full",
        };
        let (on_mode, from_column, to_column, raw_expression) = match &join.on {
            JoinOn::FkPath {
                from_column,
                to_column,
            } => (
                "fk_path",
                Some(from_column.as_str()),
                Some(to_column.as_str()),
                None,
            ),
            JoinOn::RawExpression(expr) => ("raw", None, None, Some(expr.as_str())),
        };
        tx.execute(
            "INSERT INTO qry_saved_query_joins
                 (saved_query_id, position, join_kind, from_alias, to_schema, to_table,
                  to_alias, on_mode, from_column, to_column, raw_expression)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                id,
                position as i64,
                join_kind,
                join.from_alias,
                join.to_schema,
                join.to_table,
                join.to_alias,
                on_mode,
                from_column,
                to_column,
                raw_expression,
            ],
        )
        .map_err(sqlite_err)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Private helpers — read path
// ---------------------------------------------------------------------------

struct RootRow {
    schema_name: Option<String>,
    table_name: String,
    source_alias: String,
    projection_mode: String,
    filter_json: Option<String>,
    limit_value: Option<i64>,
    offset_value: i64,
}

/// Loads the full spec for `id` from the database.
fn load_spec(conn: &Connection, id: &str) -> Result<Option<VisualQuerySpec>, StorageError> {
    let row: Option<RootRow> = conn
        .query_row(
            "SELECT schema_name, table_name, source_alias, projection_mode,
                    filter_json, limit_value, offset_value
             FROM qry_saved_queries WHERE id = ?1",
            [id],
            |row| {
                Ok(RootRow {
                    schema_name: row.get(0)?,
                    table_name: row.get(1)?,
                    source_alias: row.get(2)?,
                    projection_mode: row.get(3)?,
                    filter_json: row.get(4)?,
                    limit_value: row.get(5)?,
                    offset_value: row.get(6)?,
                })
            },
        )
        .ok();

    let root = match row {
        Some(r) => r,
        None => return Ok(None),
    };

    let filter = root
        .filter_json
        .as_deref()
        .map(serde_json::from_str::<FilterNode>)
        .transpose()
        .map_err(|e| StorageError::Sqlite {
            path: DB_PATH.into(),
            source: rusqlite::Error::InvalidParameterName(e.to_string()),
        })?;

    let projection = if root.projection_mode == "explicit" {
        let columns = load_columns(conn, id)?;
        Projection::Explicit(columns)
    } else {
        Projection::All
    };

    let sort = load_sorts(conn, id)?;
    let joins = load_joins(conn, id)?;

    Ok(Some(VisualQuerySpec {
        source: SourceTable {
            schema: root.schema_name,
            table: root.table_name,
            alias: root.source_alias,
        },
        projection,
        joins,
        filter,
        sort,
        limit: root.limit_value.map(|l| l as u64),
        offset: root.offset_value as u64,
    }))
}

fn load_columns(conn: &Connection, id: &str) -> Result<Vec<ProjectedColumn>, StorageError> {
    let mut stmt = conn
        .prepare(
            "SELECT source_alias, column_name, alias
             FROM qry_saved_query_columns
             WHERE saved_query_id = ?1
             ORDER BY position",
        )
        .map_err(sqlite_err)?;

    let columns = stmt
        .query_map([id], |row| {
            Ok(ProjectedColumn {
                source_alias: row.get(0)?,
                column: row.get(1)?,
                alias: row.get(2)?,
            })
        })
        .map_err(sqlite_err)?
        .filter_map(|r| r.ok())
        .collect();

    Ok(columns)
}

fn load_sorts(conn: &Connection, id: &str) -> Result<Vec<SortEntry>, StorageError> {
    let mut stmt = conn
        .prepare(
            "SELECT source_alias, column_name, direction
             FROM qry_saved_query_sorts
             WHERE saved_query_id = ?1
             ORDER BY position",
        )
        .map_err(sqlite_err)?;

    let sorts = stmt
        .query_map([id], |row| {
            let direction_str: String = row.get(2)?;
            let direction = if direction_str == "desc" {
                VisualSortDirection::Desc
            } else {
                VisualSortDirection::Asc
            };
            Ok(SortEntry {
                source_alias: row.get(0)?,
                column: row.get(1)?,
                direction,
            })
        })
        .map_err(sqlite_err)?
        .filter_map(|r| r.ok())
        .collect();

    Ok(sorts)
}

fn load_joins(conn: &Connection, id: &str) -> Result<Vec<JoinStep>, StorageError> {
    let mut stmt = conn
        .prepare(
            "SELECT join_kind, from_alias, to_schema, to_table, to_alias,
                    on_mode, from_column, to_column, raw_expression
             FROM qry_saved_query_joins
             WHERE saved_query_id = ?1
             ORDER BY position",
        )
        .map_err(sqlite_err)?;

    let joins = stmt
        .query_map([id], |row| {
            let kind_str: String = row.get(0)?;
            let kind = match kind_str.as_str() {
                "left" => JoinKind::Left,
                "right" => JoinKind::Right,
                "full" => JoinKind::Full,
                _ => JoinKind::Inner,
            };

            let on_mode: String = row.get(5)?;
            let on = if on_mode == "fk_path" {
                JoinOn::FkPath {
                    from_column: row.get::<_, String>(6)?,
                    to_column: row.get::<_, String>(7)?,
                }
            } else {
                JoinOn::RawExpression(row.get::<_, String>(8)?)
            };

            Ok(JoinStep {
                kind,
                from_alias: row.get(1)?,
                to_schema: row.get(2)?,
                to_table: row.get(3)?,
                to_alias: row.get(4)?,
                on,
            })
        })
        .map_err(sqlite_err)?
        .filter_map(|r| r.ok())
        .collect();

    Ok(joins)
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

fn map_summary_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SavedQuerySummary> {
    Ok(SavedQuerySummary {
        id: row.get(0)?,
        profile_id: row.get(1)?,
        name: row.get(2)?,
        table_name: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn sqlite_err(source: rusqlite::Error) -> StorageError {
    StorageError::Sqlite {
        path: DB_PATH.into(),
        source,
    }
}

fn lock_err<T>(e: std::sync::PoisonError<T>) -> StorageError {
    StorageError::Sqlite {
        path: DB_PATH.into(),
        source: rusqlite::Error::InvalidParameterName(e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use dbflux_core::{
        BoolOp, Comparator, FilterNode, JoinKind, JoinOn, JoinStep, LiteralValue, Predicate,
        PredicateValue, ProjectedColumn, Projection, SortEntry, SourceTable, VisualQuerySpec,
        VisualSortDirection,
    };
    use std::sync::Arc;

    fn temp_db(suffix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_qry_repo_{}_{}.db",
            suffix,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn setup(suffix: &str) -> (Arc<Mutex<Connection>>, SavedQueryRepo, String) {
        let path = temp_db(suffix);
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'P')",
            [&profile_id],
        )
        .unwrap();

        let conn = Arc::new(Mutex::new(conn));
        let repo = SavedQueryRepo::new(Arc::clone(&conn));
        (conn, repo, profile_id)
    }

    fn base_spec() -> VisualQuerySpec {
        VisualQuerySpec {
            source: SourceTable {
                schema: Some("public".to_string()),
                table: "users".to_string(),
                alias: "users".to_string(),
            },
            projection: Projection::All,
            joins: vec![],
            filter: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        }
    }

    // --- upsert_by_id round-trip ---

    #[test]
    fn upsert_by_id_roundtrip() {
        let (_, repo, profile_id) = setup("upsert_by_id");

        let id = Uuid::now_v7().to_string();
        let spec = base_spec();

        repo.upsert_by_id(&id, &profile_id, "My Query", &spec)
            .expect("upsert");

        let loaded = repo.get(&id).expect("get").expect("should exist");

        assert_eq!(loaded.source.table, "users");
        assert_eq!(loaded.source.schema, Some("public".to_string()));
        assert_eq!(loaded.source.alias, "users");
        assert_eq!(loaded.limit, Some(100));
        assert_eq!(loaded.offset, 0);
        assert!(matches!(loaded.projection, Projection::All));
        assert!(loaded.filter.is_none());
        assert!(loaded.sort.is_empty());
        assert!(loaded.joins.is_empty());
    }

    // --- upsert_by_name overwrites within same profile ---

    #[test]
    fn upsert_by_name_same_profile_overwrites() {
        let (_, repo, profile_id) = setup("upsert_by_name_overwrite");

        let summary1 = repo
            .upsert_by_name(&profile_id, "My Query", &base_spec())
            .expect("first upsert");

        let mut spec2 = base_spec();
        spec2.source.table = "orders".to_string();
        spec2.source.alias = "orders".to_string();

        let summary2 = repo
            .upsert_by_name(&profile_id, "My Query", &spec2)
            .expect("second upsert");

        assert_eq!(
            summary1.id, summary2.id,
            "id must be stable across name-based upsert"
        );

        let loaded = repo.get(&summary1.id).expect("get").expect("exists");
        assert_eq!(loaded.source.table, "orders");
    }

    // --- same name in different profiles is allowed ---

    #[test]
    fn upsert_by_name_different_profiles_creates_separate_rows() {
        let (conn, repo, profile_a) = setup("upsert_cross_profile");

        let profile_b = uuid::Uuid::new_v4().to_string();
        {
            let locked = conn.lock().unwrap();
            locked
                .execute(
                    "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'B')",
                    [&profile_b],
                )
                .unwrap();
        }

        let s1 = repo
            .upsert_by_name(&profile_a, "My Query", &base_spec())
            .expect("insert A");
        let s2 = repo
            .upsert_by_name(&profile_b, "My Query", &base_spec())
            .expect("insert B");

        assert_ne!(
            s1.id, s2.id,
            "different profiles must produce different ids"
        );
    }

    // --- delete cascades child rows ---

    #[test]
    fn delete_cascades_child_rows() {
        let (conn, repo, profile_id) = setup("delete_cascade");

        let mut spec = base_spec();
        spec.projection = Projection::Explicit(vec![ProjectedColumn {
            source_alias: "users".to_string(),
            column: "id".to_string(),
            alias: None,
        }]);
        spec.sort = vec![SortEntry {
            source_alias: "users".to_string(),
            column: "id".to_string(),
            direction: VisualSortDirection::Asc,
        }];

        let id = Uuid::now_v7().to_string();
        repo.upsert_by_id(&id, &profile_id, "With Children", &spec)
            .expect("upsert");

        repo.delete(&id).expect("delete");

        let locked = conn.lock().unwrap();

        let col_count: i64 = locked
            .query_row(
                "SELECT COUNT(*) FROM qry_saved_query_columns WHERE saved_query_id = ?1",
                [&id],
                |row| row.get(0),
            )
            .unwrap();

        let sort_count: i64 = locked
            .query_row(
                "SELECT COUNT(*) FROM qry_saved_query_sorts WHERE saved_query_id = ?1",
                [&id],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(col_count, 0, "columns must be cascaded on delete");
        assert_eq!(sort_count, 0, "sorts must be cascaded on delete");
    }

    // --- filter_json round-trip ---

    #[test]
    fn filter_json_roundtrip() {
        let (_, repo, profile_id) = setup("filter_json");

        let filter = FilterNode::Group {
            op: BoolOp::And,
            children: vec![
                FilterNode::Predicate(Predicate {
                    source_alias: "users".to_string(),
                    column: "status".to_string(),
                    comparator: Comparator::Eq,
                    value: PredicateValue::Single(LiteralValue::Text("active".to_string())),
                }),
                FilterNode::Group {
                    op: BoolOp::Or,
                    children: vec![
                        FilterNode::Predicate(Predicate {
                            source_alias: "users".to_string(),
                            column: "age".to_string(),
                            comparator: Comparator::Gt,
                            value: PredicateValue::Single(LiteralValue::Integer(18)),
                        }),
                        FilterNode::Predicate(Predicate {
                            source_alias: "users".to_string(),
                            column: "vip".to_string(),
                            comparator: Comparator::IsNotNull,
                            value: PredicateValue::None,
                        }),
                    ],
                },
            ],
        };

        let mut spec = base_spec();
        spec.filter = Some(filter.clone());

        let id = Uuid::now_v7().to_string();
        repo.upsert_by_id(&id, &profile_id, "Filtered", &spec)
            .expect("upsert");

        let loaded = repo.get(&id).expect("get").expect("exists");
        assert_eq!(loaded.filter, Some(filter), "filter tree must round-trip");
    }

    // --- list_for_profile filters correctly ---

    #[test]
    fn list_for_profile_returns_only_own_profile() {
        let (conn, repo, profile_a) = setup("list_for_profile");

        let profile_b = uuid::Uuid::new_v4().to_string();
        {
            let locked = conn.lock().unwrap();
            locked
                .execute(
                    "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'B')",
                    [&profile_b],
                )
                .unwrap();
        }

        repo.upsert_by_name(&profile_a, "Q1", &base_spec()).unwrap();
        repo.upsert_by_name(&profile_a, "Q2", &base_spec()).unwrap();
        repo.upsert_by_name(&profile_b, "Q3", &base_spec()).unwrap();

        let result = repo.list_for_profile(&profile_a).expect("list");

        assert_eq!(result.len(), 2, "must return only rows for profile_a");
        assert!(result.iter().all(|s| s.profile_id == profile_a));
    }

    // --- rename ---

    #[test]
    fn rename_updates_name_keeps_id() {
        let (_, repo, profile_id) = setup("rename");

        let id = Uuid::now_v7().to_string();
        repo.upsert_by_id(&id, &profile_id, "Original", &base_spec())
            .expect("upsert");

        repo.rename(&id, "Renamed").expect("rename");

        let list = repo.list_for_profile(&profile_id).expect("list");
        let entry = list.iter().find(|s| s.id == id).expect("should exist");
        assert_eq!(entry.name, "Renamed");
    }

    #[test]
    fn rename_conflicts_returns_err() {
        let (_, repo, profile_id) = setup("rename_conflict");

        let id1 = Uuid::now_v7().to_string();
        let id2 = Uuid::now_v7().to_string();

        repo.upsert_by_id(&id1, &profile_id, "Alpha", &base_spec())
            .unwrap();
        repo.upsert_by_id(&id2, &profile_id, "Beta", &base_spec())
            .unwrap();

        let result = repo.rename(&id2, "Alpha");
        assert!(
            result.is_err(),
            "renaming to an existing name in the same profile must fail"
        );
    }

    // --- explicit projection round-trip ---

    #[test]
    fn explicit_projection_roundtrip() {
        let (_, repo, profile_id) = setup("explicit_proj");

        let columns = vec![
            ProjectedColumn {
                source_alias: "users".to_string(),
                column: "id".to_string(),
                alias: None,
            },
            ProjectedColumn {
                source_alias: "users".to_string(),
                column: "email".to_string(),
                alias: Some("user_email".to_string()),
            },
        ];

        let mut spec = base_spec();
        spec.projection = Projection::Explicit(columns.clone());

        let id = Uuid::now_v7().to_string();
        repo.upsert_by_id(&id, &profile_id, "Explicit Cols", &spec)
            .expect("upsert");

        let loaded = repo.get(&id).expect("get").expect("exists");
        let loaded_cols = match loaded.projection {
            Projection::Explicit(cols) => cols,
            _ => panic!("expected explicit projection"),
        };

        assert_eq!(loaded_cols, columns);
    }

    // --- joins round-trip ---

    #[test]
    fn joins_roundtrip() {
        let (_, repo, profile_id) = setup("joins_rt");

        let joins = vec![
            JoinStep {
                kind: JoinKind::Inner,
                from_alias: "users".to_string(),
                to_schema: Some("public".to_string()),
                to_table: "orders".to_string(),
                to_alias: "orders".to_string(),
                on: JoinOn::FkPath {
                    from_column: "id".to_string(),
                    to_column: "user_id".to_string(),
                },
            },
            JoinStep {
                kind: JoinKind::Left,
                from_alias: "orders".to_string(),
                to_schema: None,
                to_table: "shipments".to_string(),
                to_alias: "shipments".to_string(),
                on: JoinOn::RawExpression("orders.id = shipments.order_id".to_string()),
            },
        ];

        let mut spec = base_spec();
        spec.joins = joins.clone();

        let id = Uuid::now_v7().to_string();
        repo.upsert_by_id(&id, &profile_id, "Joins", &spec)
            .expect("upsert");

        let loaded = repo.get(&id).expect("get").expect("exists");
        assert_eq!(loaded.joins, joins);
    }
}
