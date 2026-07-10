//! Export orchestration (Table -> File): composes `TableSource`, an identity
//! `AutoColumnMap`, and `FileSink` through `run_transfer` for every selected
//! table, then writes one `manifest.json` for the whole folder.
//!
//! A table failure partway through the run (R4-002/B-007) does NOT abort the
//! whole run with a bare `Err`, which would discard every already-exported
//! table's result. `run_export` always returns `Ok(ExportOutcome)`, itemizing
//! every planned table's [`TableTransferStatus`]. Like a cancelled run, a run
//! with any `Failed` table skips writing `manifest.json` — a manifest missing
//! an entire table would misrepresent the bundle it points to.
//!
//! Deliberately GPUI-free (this crate has no GPUI dependency) — the caller
//! (an app/UI layer with a `TaskManager`) supplies `on_progress` and is
//! responsible for wiring it to real task-progress reporting, and for
//! dispatching this synchronous, blocking work onto a background thread.

use std::path::Path;
use std::sync::Arc;

use dbflux_core::{CancelToken, Connection, TransferColumn};

use crate::column_map::AutoColumnMap;
use crate::file_sink::{FileFormat, FileSink};
use crate::manifest::{ManifestSource, ManifestTable, TransferManifest};
use crate::pipeline::{
    TableMappingMode, TableTransferStatus, TransferError, TransferOutcome, run_transfer,
};
use crate::table_source::TableSource;

/// One table selected for export, with its column shape already resolved by
/// the caller (e.g. from `Connection::table_details`).
pub struct ExportTable {
    pub schema: Option<String>,
    pub name: String,
    pub columns: Vec<TransferColumn>,
    /// Best-effort row count, when cheaply known, for progress reporting.
    pub estimated_total: Option<u64>,
}

/// Fixed per-run settings for [`run_export`], grouped to keep the function's
/// argument count reasonable.
pub struct ExportOptions<'a> {
    pub driver_id: &'a str,
    pub database: &'a str,
    pub format: FileFormat,
    /// Maximum rows per chunk read from the source / written to the sink.
    pub segment_size: u32,
}

/// Result of exporting one planned table.
pub struct ExportedTable {
    pub schema: Option<String>,
    pub name: String,
    pub status: TableTransferStatus,
}

/// Result of a `run_export` call. Always returned once `output_dir` is
/// created — even when a table fails mid-run — so the caller never loses the
/// itemized status of tables that already exported (R4-002/B-007).
pub struct ExportOutcome {
    /// `None` when the run was cancelled OR any table's status is `Failed` —
    /// a manifest describing a partially-written bundle (truncated by
    /// cancellation, or missing a whole failed table) would misrepresent the
    /// files it points to, so no `manifest.json` is written in either case.
    pub manifest: Option<TransferManifest>,
    pub warnings: Vec<String>,
    pub tables: Vec<ExportedTable>,
    pub cancelled: bool,
}

/// Exports every table in `tables` from `connection` into `output_dir`,
/// writing one `schema.table.<ext>` file per table plus a `manifest.json`
/// (unless cancelled or a table failed), and returns the outcome.
///
/// `on_progress` is invoked with `(table_index, rows_done_in_table,
/// estimated_total_in_table)` after each written chunk of any table.
pub fn run_export(
    connection: &Arc<dyn Connection>,
    tables: &[ExportTable],
    output_dir: &Path,
    options: &ExportOptions<'_>,
    cancel: &CancelToken,
    mut on_progress: impl FnMut(usize, u64, Option<u64>),
) -> Result<ExportOutcome, TransferError> {
    std::fs::create_dir_all(output_dir)
        .map_err(|e| TransferError::Sink(format!("{}: {e}", output_dir.display())))?;

    let mut manifest_tables = Vec::with_capacity(tables.len());
    let mut warnings = Vec::new();
    let mut table_statuses: Vec<ExportedTable> = tables
        .iter()
        .map(|t| ExportedTable {
            schema: t.schema.clone(),
            name: t.name.clone(),
            status: TableTransferStatus::NotStarted,
        })
        .collect();

    for (index, table) in tables.iter().enumerate() {
        if cancel.is_cancelled() {
            break;
        }

        let mut source = TableSource::new(
            Arc::clone(connection),
            table.schema.clone(),
            table.name.clone(),
            table.columns.clone(),
            options.segment_size,
            table.estimated_total,
        );

        // Export never remaps columns — the file mirrors the source table
        // exactly — so source and target column lists are identical.
        let column_map = AutoColumnMap::new(&table.columns, &table.columns);
        let mut sink = FileSink::new(
            output_dir,
            table.schema.clone(),
            table.name.clone(),
            options.format,
        );

        let result = run_transfer(
            &mut source,
            &column_map,
            &mut sink,
            TableMappingMode::Existing,
            cancel,
            &mut |rows_done, estimated_total| on_progress(index, rows_done, estimated_total),
        );

        let report = match result {
            Ok(report) => report,
            Err(e) => {
                table_statuses[index].status = TableTransferStatus::Failed {
                    error: e.to_string(),
                };
                break;
            }
        };

        let was_cancelled = report.outcome == TransferOutcome::Cancelled;
        warnings.extend(report.warnings);
        table_statuses[index].status = TableTransferStatus::Completed {
            rows: report.rows_transferred,
        };

        manifest_tables.push(ManifestTable {
            schema: table.schema.clone(),
            name: table.name.clone(),
            file: sink.file_name(),
            format: options.format.extension().to_string(),
            columns: table.columns.clone(),
            row_count: report.rows_transferred,
            fk_order_index: index,
        });

        if was_cancelled {
            break;
        }
    }

    let cancelled = cancel.is_cancelled();
    let any_failed = table_statuses
        .iter()
        .any(|t| matches!(t.status, TableTransferStatus::Failed { .. }));

    let manifest = if cancelled || any_failed {
        None
    } else {
        let manifest = TransferManifest {
            version: TransferManifest::CURRENT_VERSION,
            source: ManifestSource {
                driver: options.driver_id.to_string(),
                database: options.database.to_string(),
                schema: tables.first().and_then(|t| t.schema.clone()),
            },
            created_at: dbflux_core::chrono::Utc::now().to_rfc3339(),
            tables: manifest_tables,
        };

        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| TransferError::Sink(format!("failed to serialize manifest: {e}")))?;

        std::fs::write(output_dir.join("manifest.json"), manifest_json).map_err(|e| {
            TransferError::Sink(format!(
                "{}: {e}",
                output_dir.join("manifest.json").display()
            ))
        })?;

        Some(manifest)
    };

    Ok(ExportOutcome {
        manifest,
        warnings,
        tables: table_statuses,
        cancelled,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        DbError, DbKind, DefaultSqlDialect, DriverMetadata, QueryRequest, QueryResult,
        SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, Value,
    };
    use std::collections::VecDeque;
    use std::sync::Mutex;

    static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

    /// Fake connection returning one canned page of rows per table (in the
    /// order tables are exported), then an empty page to signal exhaustion.
    struct FakeConnection {
        pages_by_table: Mutex<std::collections::HashMap<String, VecDeque<Vec<Vec<Value>>>>>,
        /// When set, every query naming this table fails — a stand-in for a
        /// mid-run table failure, used to prove `run_export` itemizes the
        /// tables around the failure instead of discarding their status.
        fail_for_table: Option<String>,
    }

    impl FakeConnection {
        fn new(pages_by_table: std::collections::HashMap<String, Vec<Vec<Vec<Value>>>>) -> Self {
            Self {
                pages_by_table: Mutex::new(
                    pages_by_table
                        .into_iter()
                        .map(|(k, v)| (k, v.into()))
                        .collect(),
                ),
                fail_for_table: None,
            }
        }

        fn failing_for_table(mut self, table: &str) -> Self {
            self.fail_for_table = Some(table.to_string());
            self
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &DriverMetadata {
            unimplemented!("not needed for export tests")
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
            if let Some(name) = &self.fail_for_table
                && req.sql.contains(name.as_str())
            {
                return Err(DbError::NotSupported("forced query failure".to_string()));
            }

            // TableSource issues a COUNT(*) probe before its first paginated
            // SELECT (W1's real-progress-total fix); answer it with a single
            // dummy row rather than letting it fall through to the
            // table-name dispatch below and steal a page meant for an actual
            // chunk.
            if req.sql.contains("COUNT(*)") {
                return Ok(QueryResult::table(
                    Vec::new(),
                    vec![vec![Value::Int(0)]],
                    None,
                    std::time::Duration::ZERO,
                ));
            }

            // Each table's SQL contains its (quoted) table name; use that to
            // dispatch to the right page queue without parsing SQL properly.
            let mut pages = self.pages_by_table.lock().unwrap();
            let table_name = pages
                .keys()
                .find(|name| req.sql.contains(name.as_str()))
                .cloned();

            let rows = match table_name {
                Some(name) => pages
                    .get_mut(&name)
                    .and_then(|q| q.pop_front())
                    .unwrap_or_default(),
                None => Vec::new(),
            };

            Ok(QueryResult::table(
                Vec::new(),
                rows,
                None,
                std::time::Duration::ZERO,
            ))
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
    }

    fn pk_column(name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("integer".to_string()),
            nullable: false,
            is_primary_key: true,
        }
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dbflux_transfer_export_test_{label}_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn exports_two_tables_and_writes_a_manifest() {
        let mut pages = std::collections::HashMap::new();
        pages.insert(
            "users".to_string(),
            vec![vec![vec![Value::Int(1)], vec![Value::Int(2)]]],
        );
        pages.insert("orders".to_string(), vec![vec![vec![Value::Int(10)]]]);

        let connection: Arc<dyn Connection> = Arc::new(FakeConnection::new(pages));
        let tables = vec![
            ExportTable {
                schema: Some("public".to_string()),
                name: "users".to_string(),
                columns: vec![pk_column("id")],
                estimated_total: None,
            },
            ExportTable {
                schema: Some("public".to_string()),
                name: "orders".to_string(),
                columns: vec![pk_column("id")],
                estimated_total: None,
            },
        ];
        let dir = temp_dir("two_tables");
        let cancel = CancelToken::new();

        let options = ExportOptions {
            driver_id: "postgres",
            database: "app",
            format: FileFormat::Csv,
            segment_size: 10,
        };
        let outcome =
            run_export(&connection, &tables, &dir, &options, &cancel, |_, _, _| {}).unwrap();

        assert!(!outcome.cancelled);
        assert_eq!(outcome.tables.len(), 2);
        assert_eq!(
            outcome.tables[0].status,
            TableTransferStatus::Completed { rows: 2 }
        );
        assert_eq!(
            outcome.tables[1].status,
            TableTransferStatus::Completed { rows: 1 }
        );

        let manifest = outcome.manifest.expect("manifest must be written");
        assert_eq!(manifest.tables.len(), 2);
        assert_eq!(manifest.tables[0].name, "users");
        assert_eq!(manifest.tables[0].row_count, 2);
        assert_eq!(manifest.tables[1].name, "orders");
        assert_eq!(manifest.tables[1].row_count, 1);

        assert!(dir.join("public.users.csv").exists());
        assert!(dir.join("public.orders.csv").exists());

        let manifest_on_disk =
            std::fs::read_to_string(dir.join("manifest.json")).expect("manifest.json exists");
        let parsed: TransferManifest =
            serde_json::from_str(&manifest_on_disk).expect("manifest.json is valid JSON");
        assert_eq!(parsed, manifest);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cancellation_stops_further_tables_and_skips_the_manifest() {
        let mut pages = std::collections::HashMap::new();
        pages.insert(
            "users".to_string(),
            vec![vec![vec![Value::Int(1)]], vec![vec![Value::Int(2)]]],
        );
        pages.insert("orders".to_string(), vec![vec![vec![Value::Int(10)]]]);

        let connection: Arc<dyn Connection> = Arc::new(FakeConnection::new(pages));
        let tables = vec![
            ExportTable {
                schema: None,
                name: "users".to_string(),
                columns: vec![pk_column("id")],
                estimated_total: None,
            },
            ExportTable {
                schema: None,
                name: "orders".to_string(),
                columns: vec![pk_column("id")],
                estimated_total: None,
            },
        ];
        let dir = temp_dir("cancelled");
        let cancel = CancelToken::new();

        let options = ExportOptions {
            driver_id: "postgres",
            database: "app",
            format: FileFormat::Csv,
            segment_size: 1,
        };
        let outcome = run_export(
            &connection,
            &tables,
            &dir,
            &options,
            &cancel,
            |_, rows_done, _| {
                if rows_done == 1 {
                    cancel.cancel();
                }
            },
        )
        .unwrap();

        assert!(outcome.cancelled);
        assert!(
            outcome.manifest.is_none(),
            "a cancelled export must not write a manifest"
        );
        assert!(!dir.join("manifest.json").exists());
        assert!(
            !dir.join("orders.csv").exists(),
            "orders must never have started"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// R4-002/B-007 regression: a table failure partway through the run must
    /// not abort the whole export with a bare `Err` — the already-exported
    /// table's status must survive in an itemized `Ok(ExportOutcome)`, and
    /// (like cancellation) the manifest must not be written since it would
    /// misrepresent a bundle missing an entire table.
    #[test]
    fn a_mid_run_table_failure_itemizes_tables_and_skips_the_manifest() {
        let mut pages = std::collections::HashMap::new();
        pages.insert(
            "users".to_string(),
            vec![vec![vec![Value::Int(1)], vec![Value::Int(2)]]],
        );

        let connection: Arc<dyn Connection> =
            Arc::new(FakeConnection::new(pages).failing_for_table("orders"));
        let tables = vec![
            ExportTable {
                schema: None,
                name: "users".to_string(),
                columns: vec![pk_column("id")],
                estimated_total: None,
            },
            ExportTable {
                schema: None,
                name: "orders".to_string(),
                columns: vec![pk_column("id")],
                estimated_total: None,
            },
        ];
        let dir = temp_dir("mid_run_failure");
        let cancel = CancelToken::new();

        let options = ExportOptions {
            driver_id: "postgres",
            database: "app",
            format: FileFormat::Csv,
            segment_size: 10,
        };
        let outcome = run_export(&connection, &tables, &dir, &options, &cancel, |_, _, _| {})
            .expect("a per-table failure must not abort the whole run with Err");

        assert!(!outcome.cancelled);
        assert_eq!(outcome.tables.len(), 2);
        assert_eq!(
            outcome.tables[0].status,
            TableTransferStatus::Completed { rows: 2 },
            "users must stay reported as completed despite orders failing later"
        );
        assert!(
            matches!(outcome.tables[1].status, TableTransferStatus::Failed { .. }),
            "orders must be reported as Failed: {:?}",
            outcome.tables[1].status
        );
        assert!(
            outcome.manifest.is_none(),
            "a run with a failed table must not write a manifest"
        );
        assert!(!dir.join("manifest.json").exists());
        assert!(dir.join("users.csv").exists());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_table_list_still_creates_the_output_dir() {
        let connection: Arc<dyn Connection> =
            Arc::new(FakeConnection::new(std::collections::HashMap::new()));
        let dir = temp_dir("no_tables");
        std::fs::remove_dir_all(&dir).ok();
        let cancel = CancelToken::new();

        let options = ExportOptions {
            driver_id: "postgres",
            database: "app",
            format: FileFormat::Csv,
            segment_size: 10,
        };
        let outcome = run_export(&connection, &[], &dir, &options, &cancel, |_, _, _| {}).unwrap();

        assert!(dir.exists());
        assert_eq!(outcome.tables.len(), 0);
        assert!(outcome.manifest.is_some());

        std::fs::remove_dir_all(&dir).ok();
    }
}
