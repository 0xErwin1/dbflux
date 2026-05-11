use std::sync::Arc;

use crate::Connection;
use crate::TableInfo;
use crate::schema::{
    SchemaDiff, SchemaDriftDetected, SchemaFingerprint, diff_table_info,
    query_parser::QueryTableRef,
};

/// Key used to address an entry in `ConnectedProfile::table_details`.
pub type TableKey = (String, String);

/// Outcome returned by [`check_schema_drift`].
pub enum DriftOutcome {
    /// Driver returned `None` from `referenced_tables` — drift detection does
    /// not apply (NoSQL, key-value, or other drivers that do not parse SQL).
    Skip,

    /// No drift detected. The `Vec` contains `(key, fresh TableInfo)` entries
    /// that the caller should write back into `ConnectedProfile::table_details`
    /// so the cache stays current.
    Refresh(Vec<(TableKey, TableInfo)>),

    /// At least one table has drifted. `diffs` has one entry per changed table;
    /// `refreshes` has the unchanged tables that should be refreshed transparently
    /// once the user acknowledges the modal.
    Drift(SchemaDriftDetected),
}

/// Pure synchronous helper: compare `cached` against `fresh` and return the
/// per-table outcome. Called from inside the blocking background task so its
/// I/O-free path is always unit-testable.
///
/// Returns `None` when the two snapshots are fingerprint-identical (no drift).
/// Returns `Some(changes)` when they differ.
pub fn check_drift_sync(
    cached: &TableInfo,
    fresh: &TableInfo,
) -> Option<Vec<crate::schema::SchemaChange>> {
    let before_fp = SchemaFingerprint::from_table_info(cached);
    let after_fp = SchemaFingerprint::from_table_info(fresh);

    if before_fp == after_fp {
        return None;
    }

    Some(diff_table_info(cached, fresh))
}

/// Check whether any tables referenced by `query` have drifted since they were
/// last cached in `table_details`.
///
/// **Trigger rules:**
/// - If the driver's `referenced_tables` returns `None`, return `Skip`.
/// - For each referenced table:
///   - If no cache entry exists, this is a first encounter — add to `refreshes`
///     and proceed silently (no diff possible).
///   - If the entry exists and fingerprints match, add to `refreshes` for a
///     transparent cache update.
///   - If the entry exists and fingerprints differ, add to `diffs`.
/// - If `diffs` is empty, return `Refresh(refreshes)`.
/// - Otherwise, return `Drift` with both `diffs` and `refreshes`.
///
/// The caller is responsible for:
/// - Running this in a background thread (it issues blocking I/O).
/// - On `Refresh`: writing all entries back into `ConnectedProfile::table_details`.
/// - On `Drift`: opening the modal. After "Refresh & re-run", writing both
///   the `refreshes` entries AND each `diff.fresh` into the cache. After
///   "Continue with stale", NOT updating the cache.
pub fn check_schema_drift(
    connection: &Arc<dyn Connection>,
    table_details: &std::collections::HashMap<(String, String), TableInfo>,
    query: &str,
    database: &str,
) -> DriftOutcome {
    let table_refs = match connection.referenced_tables(query) {
        None => return DriftOutcome::Skip,
        Some(refs) => refs,
    };

    let mut diffs: Vec<SchemaDiff> = Vec::new();
    let mut refreshes: Vec<(TableKey, TableInfo)> = Vec::new();

    for table_ref in &table_refs {
        let schema = table_ref.schema.as_deref().unwrap_or("public");
        let table_name = &table_ref.table;
        let effective_db = table_ref.database.as_deref().unwrap_or(database);

        let cache_key: TableKey = (effective_db.to_string(), table_name.clone());

        let fresh = match connection.table_details(effective_db, Some(schema), table_name) {
            Ok(info) => info,
            Err(_) => continue,
        };

        match table_details.get(&cache_key) {
            None => {
                // First encounter — populate cache silently, no drift to report.
                refreshes.push((cache_key, fresh));
            }

            Some(cached) => match check_drift_sync(cached, &fresh) {
                None => {
                    // No drift — schedule transparent cache refresh.
                    refreshes.push((cache_key, fresh));
                }

                Some(changes) => {
                    diffs.push(SchemaDiff {
                        table: QueryTableRef {
                            database: table_ref.database.clone(),
                            schema: table_ref.schema.clone(),
                            table: table_name.clone(),
                        },
                        changes,
                        fresh,
                    });
                }
            },
        }
    }

    if diffs.is_empty() {
        DriftOutcome::Refresh(refreshes)
    } else {
        DriftOutcome::Drift(SchemaDriftDetected { diffs, refreshes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnInfo, TableInfo};

    fn col(name: &str, type_name: &str, nullable: bool, is_pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable,
            is_primary_key: is_pk,
            default_value: None,
            enum_values: None,
        }
    }

    fn make_table(name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: Some("public".to_string()),
            columns: Some(columns),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
        }
    }

    // --- check_drift_sync covers all 4 outcomes via pure logic ---
    //
    // The four logical outcomes for a given (cached, fresh) pair are:
    //   1. Identical tables → None (no drift; caller emits Refresh)
    //   2. Changed tables   → Some(changes) (drift; caller emits Drift)
    //
    // The other two outcomes of check_schema_drift (Skip and first-encounter)
    // are determined by the Connection trait and cache key presence, not by
    // the comparison logic itself, so they are covered in integration tests
    // that use a real or fake driver.

    #[test]
    fn outcome_no_drift_identical_tables() {
        let table = make_table("users", vec![col("id", "integer", false, true)]);
        assert!(
            check_drift_sync(&table, &table).is_none(),
            "identical tables must produce no drift"
        );
    }

    #[test]
    fn outcome_drift_column_added() {
        let cached = make_table("users", vec![col("id", "integer", false, true)]);
        let fresh = make_table(
            "users",
            vec![
                col("id", "integer", false, true),
                col("email", "text", false, false),
            ],
        );
        let changes = check_drift_sync(&cached, &fresh).expect("column added must produce drift");
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, crate::schema::SchemaChange::ColumnAdded(_)))
        );
    }

    #[test]
    fn outcome_drift_column_type_changed() {
        let cached = make_table("users", vec![col("id", "integer", false, true)]);
        let fresh = make_table("users", vec![col("id", "bigint", false, true)]);
        let changes = check_drift_sync(&cached, &fresh).expect("type change must produce drift");
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, crate::schema::SchemaChange::ColumnTypeChanged { .. }))
        );
    }

    #[test]
    fn outcome_drift_column_removed() {
        let cached = make_table(
            "users",
            vec![
                col("id", "integer", false, true),
                col("email", "text", false, false),
            ],
        );
        let fresh = make_table("users", vec![col("id", "integer", false, true)]);
        let changes = check_drift_sync(&cached, &fresh).expect("removed column must produce drift");
        assert!(changes.iter().any(
            |c| matches!(c, crate::schema::SchemaChange::ColumnRemoved(s) if s.name == "email")
        ));
    }
}
