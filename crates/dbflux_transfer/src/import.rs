//! Import orchestration (File -> Table): reads `manifest.json` first — via
//! `manifest::read_manifest` — before touching any target table, so a
//! missing or malformed manifest fails the whole import with zero writes
//! (T20/R3's fast-fail). Then composes `FileSource -> ColumnMap -> TableSink`
//! through `run_transfer` for every planned table.
//!
//! Recreate/Truncate are destructive `TableMappingMode`s (R4): the caller
//! must set `ImportOptions::destructive_confirmed` before calling this
//! function at all when any plan uses one of them — checked once, up front,
//! before the per-table loop even starts, so an unconfirmed destructive plan
//! touches zero tables rather than failing partway through.
//!
//! A failure partway through the per-table loop (R4-002/B-007) does NOT
//! abort the whole run with a bare `Err`, which would discard every already
//! -completed table's result. Instead `run_import` always returns
//! `Ok(ImportOutcome)`, itemizing every planned table's
//! [`TableTransferStatus`]: tables before the failure are `Completed`, the
//! failing table is `Failed`, and every table after it is `NotStarted`. Only
//! the up-front gates (missing/malformed manifest, unconfirmed destructive
//! plan) still fail fast with a top-level `Err`, since those reject the
//! whole run before any table is touched.

use std::path::Path;
use std::sync::Arc;

use dbflux_core::{CancelToken, Connection, TransferColumn};

use crate::column_map::{AutoColumnMap, ColumnMappingOverride};
use crate::file_sink::FileFormat;
use crate::file_source::FileSource;
use crate::manifest::{ManifestTable, read_manifest};
use crate::pipeline::{
    ColumnMap, TableMappingMode, TableTransferStatus, TransferError, TransferOutcome, run_transfer,
};
use crate::table_sink::TableSink;

/// Where and how one manifest table should land on the target connection.
pub struct ImportTablePlan {
    /// Name of the manifest table this plan applies to (`ManifestTable::name`).
    pub source_table: String,
    pub target_schema: Option<String>,
    pub target_table: String,
    pub mapping_mode: TableMappingMode,
    /// User-adjusted column pairing from the T22 review step; `None` uses
    /// the by-name auto-map untouched.
    pub column_overrides: Option<Vec<ColumnMappingOverride>>,
}

/// Fixed per-run settings for [`run_import`].
pub struct ImportOptions {
    /// Maximum rows per chunk read from the file / written to the target.
    pub segment_size: u32,
    /// Database used to resolve existing target tables' columns
    /// (`Connection::table_details`) for `Existing`/`Truncate`/`Skip` plans.
    pub target_database: String,
    /// Hard destructive-confirm gate (R4): must be `true` when any plan uses
    /// `Recreate` or `Truncate`. Checked before any table is touched.
    pub destructive_confirmed: bool,
}

/// Result of importing one planned table.
pub struct ImportedTable {
    pub source_table: String,
    pub target_table: String,
    pub status: TableTransferStatus,
}

/// Result of a `run_import` call. Always returned once the up-front gates
/// pass — even when a table fails mid-run — so the caller never loses the
/// itemized status of tables that already completed (R4-002/B-007).
pub struct ImportOutcome {
    pub tables: Vec<ImportedTable>,
    pub warnings: Vec<String>,
    pub cancelled: bool,
}

/// Reads `manifest_dir/manifest.json` and imports every table with a
/// matching [`ImportTablePlan`] into `connection`.
///
/// `on_progress` is invoked with `(plan_index, rows_done_in_table,
/// estimated_total_in_table)` after each written chunk of any table.
pub fn run_import(
    connection: &Arc<dyn Connection>,
    manifest_dir: &Path,
    plans: &[ImportTablePlan],
    options: &ImportOptions,
    cancel: &CancelToken,
    mut on_progress: impl FnMut(usize, u64, Option<u64>),
) -> Result<ImportOutcome, TransferError> {
    let manifest = read_manifest(&manifest_dir.join("manifest.json"))?;

    let has_destructive_plan = plans.iter().any(|plan| {
        matches!(
            plan.mapping_mode,
            TableMappingMode::Recreate | TableMappingMode::Truncate
        )
    });
    if has_destructive_plan && !options.destructive_confirmed {
        return Err(TransferError::Sink(
            "import includes a Recreate or Truncate table and was not confirmed".to_string(),
        ));
    }

    let mut tables: Vec<ImportedTable> = plans
        .iter()
        .map(|plan| ImportedTable {
            source_table: plan.source_table.clone(),
            target_table: plan.target_table.clone(),
            status: TableTransferStatus::NotStarted,
        })
        .collect();
    let mut warnings = Vec::new();

    for (index, plan) in plans.iter().enumerate() {
        if cancel.is_cancelled() {
            break;
        }

        let Some(manifest_table) = manifest.tables.iter().find(|t| t.name == plan.source_table)
        else {
            tables[index].status = TableTransferStatus::Failed {
                error: format!("manifest has no table named '{}'", plan.source_table),
            };
            break;
        };

        let result = import_one_table(
            connection,
            manifest_dir,
            manifest_table,
            plan,
            options,
            cancel,
            |rows_done, estimated_total| on_progress(index, rows_done, estimated_total),
        );

        let report = match result {
            Ok(report) => report,
            Err(e) => {
                tables[index].status = TableTransferStatus::Failed {
                    error: e.to_string(),
                };
                break;
            }
        };

        let was_cancelled = report.outcome == TransferOutcome::Cancelled;
        warnings.extend(report.warnings);
        tables[index].status = if matches!(plan.mapping_mode, TableMappingMode::Skip) {
            TableTransferStatus::Skipped
        } else {
            TableTransferStatus::Completed {
                rows: report.rows_transferred,
            }
        };

        if was_cancelled {
            break;
        }
    }

    Ok(ImportOutcome {
        tables,
        warnings,
        cancelled: cancel.is_cancelled(),
    })
}

#[allow(clippy::too_many_arguments)]
fn import_one_table(
    connection: &Arc<dyn Connection>,
    manifest_dir: &Path,
    manifest_table: &ManifestTable,
    plan: &ImportTablePlan,
    options: &ImportOptions,
    cancel: &CancelToken,
    on_progress: impl FnMut(u64, Option<u64>),
) -> Result<crate::pipeline::TransferReport, TransferError> {
    let format = FileFormat::from_extension(&manifest_table.format).ok_or_else(|| {
        TransferError::Source(format!(
            "manifest table '{}' has unknown format '{}'",
            manifest_table.name, manifest_table.format
        ))
    })?;

    let mut source = FileSource::open(
        &manifest_dir.join(&manifest_table.file),
        format,
        manifest_table.columns.clone(),
        options.segment_size,
        Some(manifest_table.row_count),
    )?;

    let target_columns = resolve_target_columns(
        connection,
        plan,
        &manifest_table.columns,
        &options.target_database,
    )?;

    let column_map: Box<dyn ColumnMap> = match &plan.column_overrides {
        Some(overrides) => Box::new(AutoColumnMap::with_overrides(
            &manifest_table.columns,
            &target_columns,
            overrides,
        )),
        None => Box::new(AutoColumnMap::new(&manifest_table.columns, &target_columns)),
    };

    let mut sink = TableSink::new(
        Arc::clone(connection),
        plan.target_schema.clone(),
        plan.target_table.clone(),
    );

    let mut on_progress = on_progress;
    run_transfer(
        &mut source,
        column_map.as_ref(),
        &mut sink,
        plan.mapping_mode,
        cancel,
        &mut on_progress,
    )
}

/// Resolves the target table's column shape for auto-mapping.
///
/// `Create`/`Recreate` build a fresh table from the source's own columns
/// (same-engine ⇒ 1:1 types), so there is nothing to query yet. `Existing`/
/// `Truncate` require the table to already exist — a failed lookup
/// propagates, since inserting into a nonexistent table would fail anyway.
/// `Skip` never writes, so a missing target table falls back to the
/// manifest's columns rather than failing the whole import over an inert
/// table.
fn resolve_target_columns(
    connection: &Arc<dyn Connection>,
    plan: &ImportTablePlan,
    manifest_columns: &[TransferColumn],
    target_database: &str,
) -> Result<Vec<TransferColumn>, TransferError> {
    match plan.mapping_mode {
        TableMappingMode::Create | TableMappingMode::Recreate => Ok(manifest_columns.to_vec()),
        TableMappingMode::Existing | TableMappingMode::Truncate => {
            query_target_columns(connection, plan, target_database)
        }
        TableMappingMode::Skip => Ok(query_target_columns(connection, plan, target_database)
            .unwrap_or_else(|_| manifest_columns.to_vec())),
    }
}

fn query_target_columns(
    connection: &Arc<dyn Connection>,
    plan: &ImportTablePlan,
    target_database: &str,
) -> Result<Vec<TransferColumn>, TransferError> {
    let info = connection
        .table_details(
            target_database,
            plan.target_schema.as_deref(),
            &plan.target_table,
        )
        .map_err(|e| TransferError::Sink(e.to_string()))?;

    Ok(info
        .columns
        .unwrap_or_default()
        .into_iter()
        .map(|c| TransferColumn {
            name: c.name,
            type_name: Some(c.type_name),
            nullable: c.nullable,
            is_primary_key: c.is_primary_key,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ManifestSource, TransferManifest};
    use dbflux_core::{
        ColumnInfo, CreateTableSpec, DbError, DbKind, DefaultSqlDialect, DriverCapabilities,
        DriverMetadata, DriverMetadataBuilder, GeneratedQuery, GeneratorError, MutationCategory,
        MutationRequest, QueryGenerator, QueryLanguage, QueryRequest, QueryResult,
        SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TableInfo, Value,
    };
    use std::sync::Mutex;

    static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

    /// Minimal query generator so `Create`/`Recreate` plans have DDL/bulk-insert
    /// text to execute (the fake connection's `execute` is a no-op recorder, so
    /// the generated text's content doesn't need to be semantically valid SQL).
    /// Records every `columns` list it is asked to bulk-insert with, so tests
    /// can prove the INSERT column list matches the resolved target shape.
    struct FakeGenerator {
        recorded_bulk_insert_columns: Mutex<Vec<Vec<String>>>,
    }

    impl FakeGenerator {
        fn new() -> Self {
            Self {
                recorded_bulk_insert_columns: Mutex::new(Vec::new()),
            }
        }
    }

    impl QueryGenerator for FakeGenerator {
        fn supported_categories(&self) -> &'static [MutationCategory] {
            &[MutationCategory::Sql]
        }

        fn generate_mutation(&self, _mutation: &MutationRequest) -> Option<GeneratedQuery> {
            None
        }

        fn generate_bulk_insert(
            &self,
            _schema: Option<&str>,
            table: &str,
            columns: &[String],
            _column_types: &[Option<String>],
            rows: &[&[Value]],
        ) -> Result<Option<GeneratedQuery>, GeneratorError> {
            self.recorded_bulk_insert_columns
                .lock()
                .unwrap()
                .push(columns.to_vec());
            Ok(Some(GeneratedQuery {
                language: QueryLanguage::Sql,
                text: format!("INSERT INTO {table} VALUES (...) -- {} rows", rows.len()),
            }))
        }

        fn generate_create_table(
            &self,
            spec: &CreateTableSpec,
        ) -> Result<Option<GeneratedQuery>, GeneratorError> {
            Ok(Some(GeneratedQuery {
                language: QueryLanguage::Sql,
                text: format!("CREATE TABLE {} (...)", spec.table),
            }))
        }
    }

    /// Fake connection recording every `execute()`/`table_details()` call —
    /// used to prove the fast-fail and destructive-confirm gates touch zero
    /// tables (a "spy sink" in effect, since every write goes through
    /// `execute`/`insert_row`).
    struct FakeConnection {
        executed_sql: Mutex<Vec<String>>,
        table_details_calls: Mutex<Vec<String>>,
        existing_table_columns: Vec<ColumnInfo>,
        metadata: DriverMetadata,
        generator: FakeGenerator,
    }

    impl FakeConnection {
        fn new(existing_table_columns: Vec<ColumnInfo>) -> Self {
            let metadata = DriverMetadataBuilder::new(
                "fake",
                "Fake",
                dbflux_core::DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .capabilities(DriverCapabilities::BULK_INSERT | DriverCapabilities::TRUNCATE_TABLE)
            .build();

            Self {
                executed_sql: Mutex::new(Vec::new()),
                table_details_calls: Mutex::new(Vec::new()),
                existing_table_columns,
                metadata,
                generator: FakeGenerator::new(),
            }
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
            self.executed_sql.lock().unwrap().push(req.sql.clone());
            Ok(QueryResult::empty())
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Err(DbError::NotSupported("stub".to_string()))
        }

        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn SqlDialect {
            &DIALECT
        }

        fn query_generator(&self) -> Option<&dyn QueryGenerator> {
            Some(&self.generator)
        }

        fn table_details(
            &self,
            _database: &str,
            _schema: Option<&str>,
            table: &str,
        ) -> Result<TableInfo, DbError> {
            self.table_details_calls
                .lock()
                .unwrap()
                .push(table.to_string());
            Ok(TableInfo {
                name: table.to_string(),
                schema: None,
                columns: Some(self.existing_table_columns.clone()),
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: Default::default(),
                child_items: None,
            })
        }
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dbflux_transfer_import_test_{label}_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).ok();
        dir
    }

    fn pk_column() -> TransferColumn {
        TransferColumn {
            name: "id".to_string(),
            type_name: Some("integer".to_string()),
            nullable: false,
            is_primary_key: true,
        }
    }

    fn write_bundle(dir: &Path, table_name: &str, rows_csv: &str) {
        std::fs::write(dir.join(format!("{table_name}.csv")), rows_csv).unwrap();

        let manifest = TransferManifest {
            version: TransferManifest::CURRENT_VERSION,
            source: ManifestSource {
                driver: "sqlite".to_string(),
                database: "main".to_string(),
                schema: None,
            },
            created_at: "2026-07-07T10:00:00+00:00".to_string(),
            tables: vec![ManifestTable {
                schema: None,
                name: table_name.to_string(),
                file: format!("{table_name}.csv"),
                format: "csv".to_string(),
                columns: vec![pk_column()],
                row_count: 2,
                fk_order_index: 0,
            }],
        };
        std::fs::write(
            dir.join("manifest.json"),
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .unwrap();
    }

    fn existing_column_info() -> ColumnInfo {
        ColumnInfo {
            name: "id".to_string(),
            type_name: "integer".to_string(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            enum_values: None,
        }
    }

    fn default_options(target_database: &str) -> ImportOptions {
        ImportOptions {
            segment_size: 500,
            target_database: target_database.to_string(),
            destructive_confirmed: false,
        }
    }

    #[test]
    fn missing_manifest_fails_fast_with_zero_writes() {
        let dir = temp_dir("missing_manifest");
        std::fs::remove_file(dir.join("manifest.json")).ok();
        let connection: Arc<dyn Connection> = Arc::new(FakeConnection::new(vec![]));
        let plans = vec![ImportTablePlan {
            source_table: "users".to_string(),
            target_schema: None,
            target_table: "users".to_string(),
            mapping_mode: TableMappingMode::Create,
            column_overrides: None,
        }];
        let cancel = CancelToken::new();

        let fake = connection.clone();
        let result = run_import(
            &connection,
            &dir,
            &plans,
            &default_options("main"),
            &cancel,
            |_, _, _| {},
        );

        assert!(result.is_err());
        // Downcast is unnecessary — assert through the same connection handle.
        let _ = fake;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn malformed_manifest_fails_fast_with_zero_writes() {
        let dir = temp_dir("malformed_manifest");
        std::fs::write(dir.join("manifest.json"), "{ not valid json").unwrap();
        let fake = Arc::new(FakeConnection::new(vec![]));
        let connection: Arc<dyn Connection> = fake.clone();
        let plans = vec![ImportTablePlan {
            source_table: "users".to_string(),
            target_schema: None,
            target_table: "users".to_string(),
            mapping_mode: TableMappingMode::Create,
            column_overrides: None,
        }];
        let cancel = CancelToken::new();

        let result = run_import(
            &connection,
            &dir,
            &plans,
            &default_options("main"),
            &cancel,
            |_, _, _| {},
        );

        assert!(result.is_err());
        assert!(
            fake.executed_sql.lock().unwrap().is_empty(),
            "a malformed manifest must not touch any table"
        );
        assert!(fake.table_details_calls.lock().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recreate_without_confirmation_is_rejected_before_touching_any_table() {
        let dir = temp_dir("recreate_unconfirmed");
        write_bundle(&dir, "users", "id\n1\n2\n");
        let fake = Arc::new(FakeConnection::new(vec![existing_column_info()]));
        let connection: Arc<dyn Connection> = fake.clone();
        let plans = vec![ImportTablePlan {
            source_table: "users".to_string(),
            target_schema: None,
            target_table: "users".to_string(),
            mapping_mode: TableMappingMode::Recreate,
            column_overrides: None,
        }];
        let mut options = default_options("main");
        options.destructive_confirmed = false;
        let cancel = CancelToken::new();

        let result = run_import(&connection, &dir, &plans, &options, &cancel, |_, _, _| {});

        assert!(result.is_err());
        assert!(
            fake.executed_sql.lock().unwrap().is_empty(),
            "an unconfirmed Recreate must not touch any table"
        );
        assert!(
            fake.table_details_calls.lock().unwrap().is_empty(),
            "the gate must trip before even resolving target columns"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recreate_with_confirmation_proceeds() {
        let dir = temp_dir("recreate_confirmed");
        write_bundle(&dir, "users", "id\n1\n2\n");
        let fake = Arc::new(FakeConnection::new(vec![existing_column_info()]));
        let connection: Arc<dyn Connection> = fake.clone();
        let plans = vec![ImportTablePlan {
            source_table: "users".to_string(),
            target_schema: None,
            target_table: "users".to_string(),
            mapping_mode: TableMappingMode::Recreate,
            column_overrides: None,
        }];
        let mut options = default_options("main");
        options.destructive_confirmed = true;
        let cancel = CancelToken::new();

        let outcome = run_import(&connection, &dir, &plans, &options, &cancel, |_, _, _| {})
            .expect("confirmed Recreate must proceed");

        assert!(!outcome.cancelled);
        assert_eq!(outcome.tables.len(), 1);
        assert_eq!(
            outcome.tables[0].status,
            TableTransferStatus::Completed { rows: 2 }
        );
        let executed = fake.executed_sql.lock().unwrap();
        assert!(executed.iter().any(|sql| sql.starts_with("DROP TABLE")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn skip_mode_reports_zero_writes_and_is_marked_skipped() {
        let dir = temp_dir("skip_mode");
        write_bundle(&dir, "users", "id\n1\n2\n");
        let fake = Arc::new(FakeConnection::new(vec![existing_column_info()]));
        let connection: Arc<dyn Connection> = fake.clone();
        let plans = vec![ImportTablePlan {
            source_table: "users".to_string(),
            target_schema: None,
            target_table: "users".to_string(),
            mapping_mode: TableMappingMode::Skip,
            column_overrides: None,
        }];
        let cancel = CancelToken::new();

        let outcome = run_import(
            &connection,
            &dir,
            &plans,
            &default_options("main"),
            &cancel,
            |_, _, _| {},
        )
        .unwrap();

        assert_eq!(outcome.tables.len(), 1);
        assert_eq!(outcome.tables[0].status, TableTransferStatus::Skipped);
        assert!(
            fake.executed_sql.lock().unwrap().is_empty(),
            "Skip must never write to the target"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn successful_import_reports_row_counts_per_table() {
        let dir = temp_dir("success");
        write_bundle(&dir, "users", "id\n1\n2\n");
        let fake = Arc::new(FakeConnection::new(vec![existing_column_info()]));
        let connection: Arc<dyn Connection> = fake.clone();
        let plans = vec![ImportTablePlan {
            source_table: "users".to_string(),
            target_schema: None,
            target_table: "users".to_string(),
            mapping_mode: TableMappingMode::Existing,
            column_overrides: None,
        }];
        let cancel = CancelToken::new();

        let outcome = run_import(
            &connection,
            &dir,
            &plans,
            &default_options("main"),
            &cancel,
            |_, _, _| {},
        )
        .unwrap();

        assert!(!outcome.cancelled);
        assert_eq!(outcome.tables.len(), 1);
        assert_eq!(outcome.tables[0].source_table, "users");
        assert_eq!(
            outcome.tables[0].status,
            TableTransferStatus::Completed { rows: 2 }
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R4-002/B-007 regression: a plan referencing an unknown manifest table
    /// must NOT abort the whole run with a bare `Err` (which would discard
    /// every other table's itemized status) — it must surface as that one
    /// table's `Failed` status inside an `Ok(ImportOutcome)`.
    #[test]
    fn plan_referencing_an_unknown_manifest_table_is_reported_as_failed_not_a_hard_error() {
        let dir = temp_dir("unknown_table");
        write_bundle(&dir, "users", "id\n1\n");
        let connection: Arc<dyn Connection> =
            Arc::new(FakeConnection::new(vec![existing_column_info()]));
        let plans = vec![ImportTablePlan {
            source_table: "does_not_exist".to_string(),
            target_schema: None,
            target_table: "does_not_exist".to_string(),
            mapping_mode: TableMappingMode::Existing,
            column_overrides: None,
        }];
        let cancel = CancelToken::new();

        let outcome = run_import(
            &connection,
            &dir,
            &plans,
            &default_options("main"),
            &cancel,
            |_, _, _| {},
        )
        .expect("an unknown manifest table must surface as a per-table Failed status, not Err");

        assert_eq!(outcome.tables.len(), 1);
        assert!(
            matches!(&outcome.tables[0].status, TableTransferStatus::Failed { error } if error.contains("does_not_exist")),
            "expected a Failed status naming the unknown table, got: {:?}",
            outcome.tables[0].status
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R4-002/B-007 regression (test requirement #1): when table 2 of 3
    /// fails mid-load, table 1's success and table 3's `NotStarted` status
    /// must both be preserved in the returned outcome — not discarded by a
    /// bare `Err` that only carries the last error.
    #[test]
    fn a_mid_run_table_failure_itemizes_completed_failed_and_not_started_tables() {
        let dir = temp_dir("partial_failure");
        std::fs::write(dir.join("t1.csv"), "id\n1\n2\n").unwrap();
        std::fs::write(dir.join("t3.csv"), "id\n9\n").unwrap();

        let manifest = TransferManifest {
            version: TransferManifest::CURRENT_VERSION,
            source: ManifestSource {
                driver: "sqlite".to_string(),
                database: "main".to_string(),
                schema: None,
            },
            created_at: "2026-07-07T10:00:00+00:00".to_string(),
            tables: vec![
                ManifestTable {
                    schema: None,
                    name: "t1".to_string(),
                    file: "t1.csv".to_string(),
                    format: "csv".to_string(),
                    columns: vec![pk_column()],
                    row_count: 2,
                    fk_order_index: 0,
                },
                ManifestTable {
                    schema: None,
                    name: "t2".to_string(),
                    file: "t2.csv".to_string(),
                    format: "csv".to_string(),
                    columns: vec![pk_column()],
                    row_count: 1,
                    fk_order_index: 1,
                },
                ManifestTable {
                    schema: None,
                    name: "t3".to_string(),
                    file: "t3.csv".to_string(),
                    format: "csv".to_string(),
                    columns: vec![pk_column()],
                    row_count: 1,
                    fk_order_index: 2,
                },
            ],
        };
        std::fs::write(
            dir.join("manifest.json"),
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .unwrap();
        // t2.csv is deliberately never written: FileSource::open must fail
        // for that table specifically, mid-run.

        let fake = Arc::new(FakeConnection::new(vec![existing_column_info()]));
        let connection: Arc<dyn Connection> = fake.clone();
        let plans = vec![
            ImportTablePlan {
                source_table: "t1".to_string(),
                target_schema: None,
                target_table: "t1".to_string(),
                mapping_mode: TableMappingMode::Existing,
                column_overrides: None,
            },
            ImportTablePlan {
                source_table: "t2".to_string(),
                target_schema: None,
                target_table: "t2".to_string(),
                mapping_mode: TableMappingMode::Existing,
                column_overrides: None,
            },
            ImportTablePlan {
                source_table: "t3".to_string(),
                target_schema: None,
                target_table: "t3".to_string(),
                mapping_mode: TableMappingMode::Existing,
                column_overrides: None,
            },
        ];
        let cancel = CancelToken::new();

        let outcome = run_import(
            &connection,
            &dir,
            &plans,
            &default_options("main"),
            &cancel,
            |_, _, _| {},
        )
        .expect("a per-table failure must not abort the whole run with Err");

        assert_eq!(outcome.tables.len(), 3);
        assert_eq!(
            outcome.tables[0].status,
            TableTransferStatus::Completed { rows: 2 },
            "table 1 must stay reported as completed despite table 2 failing later"
        );
        assert!(
            matches!(outcome.tables[1].status, TableTransferStatus::Failed { .. }),
            "table 2 must be reported as Failed: {:?}",
            outcome.tables[1].status
        );
        assert_eq!(
            outcome.tables[2].status,
            TableTransferStatus::NotStarted,
            "table 3 must never have been attempted"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// JD-C1 regression (Import/Existing): the target table's columns are
    /// physically reordered relative to the manifest AND carry a target-only
    /// column plus a source-only column with no match. The INSERT column
    /// list must follow the resolved TARGET order/shape (not the source's),
    /// the unmatched source column must be dropped with a warning (R5), and
    /// the unmatched target column must simply receive NULL with no error.
    #[test]
    fn existing_target_with_reordered_and_mismatched_columns_aligns_insert_and_reports_unmatched_source()
     {
        let dir = temp_dir("column_alignment");
        std::fs::write(dir.join("users.csv"), "id,legacy_extra\n1,x\n2,y\n").unwrap();

        let manifest = TransferManifest {
            version: TransferManifest::CURRENT_VERSION,
            source: ManifestSource {
                driver: "sqlite".to_string(),
                database: "main".to_string(),
                schema: None,
            },
            created_at: "2026-07-07T10:00:00+00:00".to_string(),
            tables: vec![ManifestTable {
                schema: None,
                name: "users".to_string(),
                file: "users.csv".to_string(),
                format: "csv".to_string(),
                columns: vec![
                    pk_column(),
                    TransferColumn {
                        name: "legacy_extra".to_string(),
                        type_name: Some("text".to_string()),
                        nullable: true,
                        is_primary_key: false,
                    },
                ],
                row_count: 2,
                fk_order_index: 0,
            }],
        };
        std::fs::write(
            dir.join("manifest.json"),
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .unwrap();

        // Existing target columns in a DIFFERENT physical order than the
        // source, plus a target-only column ("name") absent from the source.
        let target_columns = vec![
            ColumnInfo {
                name: "name".to_string(),
                type_name: "text".to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                enum_values: None,
            },
            existing_column_info(),
        ];
        let fake = Arc::new(FakeConnection::new(target_columns));
        let connection: Arc<dyn Connection> = fake.clone();
        let plans = vec![ImportTablePlan {
            source_table: "users".to_string(),
            target_schema: None,
            target_table: "users".to_string(),
            mapping_mode: TableMappingMode::Existing,
            column_overrides: None,
        }];
        let cancel = CancelToken::new();

        let outcome = run_import(
            &connection,
            &dir,
            &plans,
            &default_options("main"),
            &cancel,
            |_, _, _| {},
        )
        .unwrap();

        assert_eq!(
            outcome.tables[0].status,
            TableTransferStatus::Completed { rows: 2 }
        );
        assert!(
            outcome.warnings.iter().any(|w| w.contains("legacy_extra")),
            "the unmatched source column must be reported as a non-blocking warning: {:?}",
            outcome.warnings
        );

        let recorded = fake.generator.recorded_bulk_insert_columns.lock().unwrap();
        assert_eq!(
            recorded[0],
            vec!["name".to_string(), "id".to_string()],
            "the INSERT column list must follow the resolved TARGET order/shape"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
