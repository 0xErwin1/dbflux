//! Migration orchestration (Table -> Table, same-engine): resolves the FK
//! load order via `dependency_order::topological_order` (T11) before
//! dispatching any transfer (T25/R6), then composes
//! `TableSource -> ColumnMap -> TableSink` per planned table through
//! `run_transfer`, same as Import. Also toggles referential integrity on the
//! target around the whole run when requested and supported (T25/R6/R7).
//!
//! On a foreign-key cycle, `run_migration` touches zero tables and returns
//! [`MigrationOutcome::CyclicOrderRequired`] with the cyclic subset instead of
//! guessing an order — the caller (wizard) is expected to show a manual
//! sortable-list step and retry with `MigrationOptions::manual_order` set.

use std::sync::{Arc, Mutex};

use dbflux_core::{
    CancelToken, Connection, DriverCapabilities, LogErr, OrderResult, SchemaForeignKeyInfo,
    TableRef, TransferColumn, topological_order,
};

use crate::column_map::{AutoColumnMap, ColumnMappingOverride};
use crate::pipeline::{
    ColumnMap, TableMappingMode, TableTransferStatus, TransferError, TransferOutcome, run_transfer,
};
use crate::table_sink::TableSink;
use crate::table_source::TableSource;

/// One table planned for migration, with source/target identity already
/// resolved by the caller (wizard).
pub struct MigrationTablePlan {
    pub source_table: TableRef,
    pub source_columns: Vec<TransferColumn>,
    pub target_schema: Option<String>,
    pub target_table: String,
    pub mapping_mode: TableMappingMode,
    /// User-adjusted column pairing from the T22-style review step; `None`
    /// uses the by-name auto-map untouched.
    pub column_overrides: Option<Vec<ColumnMappingOverride>>,
    /// Best-effort row count, when cheaply known, for progress reporting.
    pub estimated_total: Option<u64>,
}

/// Fixed per-run settings for [`run_migration`].
pub struct MigrationOptions {
    pub segment_size: u32,
    pub source_database: String,
    pub target_database: String,
    /// Hard destructive-confirm gate (R4): must be `true` when any plan uses
    /// `Recreate` or `Truncate`. Checked before any table is touched.
    pub destructive_confirmed: bool,
    /// Requested by the user in the wizard; only honored when the target
    /// advertises `DriverCapabilities::DISABLE_FK_CHECKS` (R6/R7) — otherwise
    /// the toggle is simply not attempted, not a runtime error.
    pub disable_referential_integrity: bool,
    /// A user-chosen load order (from the wizard's manual-reorder step after
    /// a `CyclicOrderRequired` response), consumed verbatim instead of the
    /// automatic FK topological sort. `None` runs the automatic sort.
    pub manual_order: Option<Vec<TableRef>>,
}

/// One migrated table's outcome.
pub struct MigratedTable {
    pub source_table: String,
    pub target_table: String,
    pub status: TableTransferStatus,
}

/// Result of a migration run that actually executed (completed or
/// cancelled). A per-table failure mid-run (R4-002/B-007) does NOT abort the
/// whole run — `run_migration` still returns `Ok(MigrationOutcome::Completed)`,
/// itemizing every planned table's [`TableTransferStatus`]: tables migrated
/// before the failure are `Completed`, the failing table is `Failed`, and
/// every table after it in load order is `NotStarted`.
pub struct MigrationRunOutcome {
    pub tables: Vec<MigratedTable>,
    pub warnings: Vec<String>,
    pub cancelled: bool,
}

/// Overall result of [`run_migration`]: either the transfer ran (or was
/// cancelled), or the source's FK graph has a cycle and needs a manual order
/// before anything can run.
pub enum MigrationOutcome {
    Completed(MigrationRunOutcome),
    /// No table was touched. The caller should present a manual
    /// sortable-list step seeded with `ordered_prefix` + `cycle`, then retry
    /// with `MigrationOptions::manual_order` set to the user's chosen order.
    CyclicOrderRequired {
        ordered_prefix: Vec<TableRef>,
        cycle: Vec<TableRef>,
    },
}

/// Resolves `source_connection`'s foreign keys, orders `plans` by
/// [`topological_order`] (parents before children), and migrates each table
/// in that order into `target_connection` via `TableSource -> ColumnMap ->
/// TableSink`.
///
/// `on_progress` is invoked with `(plan_index, rows_done_in_table,
/// estimated_total_in_table)` after each written chunk of any table, where
/// `plan_index` refers to the resolved (post-ordering) sequence, not the
/// caller's original `plans` order.
pub fn run_migration(
    source_connection: &Arc<dyn Connection>,
    target_connection: &Arc<dyn Connection>,
    plans: &[MigrationTablePlan],
    options: &MigrationOptions,
    cancel: &CancelToken,
    mut on_progress: impl FnMut(usize, u64, Option<u64>),
) -> Result<MigrationOutcome, TransferError> {
    let has_destructive_plan = plans.iter().any(|plan| {
        matches!(
            plan.mapping_mode,
            TableMappingMode::Recreate | TableMappingMode::Truncate
        )
    });
    if has_destructive_plan && !options.destructive_confirmed {
        return Err(TransferError::Sink(
            "migration includes a Recreate or Truncate table and was not confirmed".to_string(),
        ));
    }

    let ordered_plans = match resolve_plan_order(source_connection, plans, options)? {
        ResolvedOrder::Ordered(ordered) => ordered,
        ResolvedOrder::Cyclic {
            ordered_prefix,
            cycle,
        } => {
            return Ok(MigrationOutcome::CyclicOrderRequired {
                ordered_prefix,
                cycle,
            });
        }
    };

    let mut migrated: Vec<MigratedTable> = ordered_plans
        .iter()
        .map(|plan| MigratedTable {
            source_table: plan.source_table.name.clone(),
            target_table: plan.target_table.clone(),
            status: TableTransferStatus::NotStarted,
        })
        .collect();
    let mut warnings = Vec::new();
    let restore_failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    {
        // The guard's `Drop` restores referential integrity on every exit
        // path (success, a mid-loop table failure, or cancellation) without
        // needing matching cleanup code at each return point. It is scoped
        // to this block so it drops — and attempts the restore — before
        // `restore_failure` is read below.
        let _ri_guard = ReferentialIntegrityGuard::enable(
            target_connection,
            options.disable_referential_integrity,
            Arc::clone(&restore_failure),
        )?;

        for (index, plan) in ordered_plans.iter().enumerate() {
            if cancel.is_cancelled() {
                break;
            }

            let result = migrate_one_table(
                source_connection,
                target_connection,
                plan,
                options,
                cancel,
                |rows_done, estimated_total| on_progress(index, rows_done, estimated_total),
            );

            // A table failure (R4-002/B-007) stops the run here rather than
            // propagating via `?`, which would discard every earlier table's
            // itemized status along with the tables after it.
            let report = match result {
                Ok(report) => report,
                Err(e) => {
                    migrated[index].status = TableTransferStatus::Failed {
                        error: e.to_string(),
                    };
                    break;
                }
            };

            let was_cancelled = report.outcome == TransferOutcome::Cancelled;
            warnings.extend(report.warnings);
            migrated[index].status = if matches!(plan.mapping_mode, TableMappingMode::Skip) {
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
    }

    if let Some(failure) = restore_failure
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
    {
        warnings.push(format!(
            "referential integrity could not be restored on the target after migration: {failure}"
        ));
    }

    Ok(MigrationOutcome::Completed(MigrationRunOutcome {
        tables: migrated,
        warnings,
        cancelled: cancel.is_cancelled(),
    }))
}

enum ResolvedOrder<'a> {
    Ordered(Vec<&'a MigrationTablePlan>),
    Cyclic {
        ordered_prefix: Vec<TableRef>,
        cycle: Vec<TableRef>,
    },
}

/// Decides the load order for `plans`: verbatim from
/// `MigrationOptions::manual_order` when present, otherwise a fresh FK
/// topological sort fetched from `source_connection`.
fn resolve_plan_order<'a>(
    source_connection: &Arc<dyn Connection>,
    plans: &'a [MigrationTablePlan],
    options: &MigrationOptions,
) -> Result<ResolvedOrder<'a>, TransferError> {
    let order = match &options.manual_order {
        Some(manual) => manual.clone(),
        None => {
            let table_refs: Vec<TableRef> =
                plans.iter().map(|plan| plan.source_table.clone()).collect();
            let fks = fetch_all_foreign_keys(source_connection, &options.source_database, plans)?;

            match topological_order(&table_refs, &fks) {
                OrderResult::Ordered(order) => order,
                OrderResult::Cyclic {
                    ordered_prefix,
                    cycle,
                } => {
                    return Ok(ResolvedOrder::Cyclic {
                        ordered_prefix,
                        cycle,
                    });
                }
            }
        }
    };

    if order.len() != plans.len() {
        return Err(TransferError::Source(
            "resolved table order does not match every planned table".to_string(),
        ));
    }

    let ordered_plans: Vec<&MigrationTablePlan> = order
        .iter()
        .map(|table_ref| {
            plans
                .iter()
                .find(|plan| &plan.source_table == table_ref)
                .ok_or_else(|| {
                    TransferError::Source(format!(
                        "manual order references unplanned table '{}'",
                        table_ref.qualified_name()
                    ))
                })
        })
        .collect::<Result<_, _>>()?;

    Ok(ResolvedOrder::Ordered(ordered_plans))
}

/// Fetches every foreign key relevant to `plans`, scoped per distinct source
/// schema (`schema_foreign_keys` is called once per schema, since it takes at
/// most one). `topological_order` matches edges by table name only, so the
/// merged result is sufficient regardless of how many schemas are involved.
fn fetch_all_foreign_keys(
    connection: &Arc<dyn Connection>,
    database: &str,
    plans: &[MigrationTablePlan],
) -> Result<Vec<SchemaForeignKeyInfo>, TransferError> {
    let mut schemas: Vec<Option<String>> = plans
        .iter()
        .map(|plan| plan.source_table.schema.clone())
        .collect();
    schemas.sort();
    schemas.dedup();

    let mut fks = Vec::new();
    for schema in schemas {
        let batch = connection
            .schema_foreign_keys(database, schema.as_deref())
            .map_err(|e| TransferError::Source(e.to_string()))?;
        fks.extend(batch);
    }

    Ok(fks)
}

#[allow(clippy::too_many_arguments)]
fn migrate_one_table(
    source_connection: &Arc<dyn Connection>,
    target_connection: &Arc<dyn Connection>,
    plan: &MigrationTablePlan,
    options: &MigrationOptions,
    cancel: &CancelToken,
    on_progress: impl FnMut(u64, Option<u64>),
) -> Result<crate::pipeline::TransferReport, TransferError> {
    let mut source = TableSource::new(
        Arc::clone(source_connection),
        plan.source_table.schema.clone(),
        plan.source_table.name.clone(),
        plan.source_columns.clone(),
        options.segment_size,
        plan.estimated_total,
    );

    let target_columns = resolve_target_columns(target_connection, plan, &options.target_database)?;

    let column_map: Box<dyn ColumnMap> = match &plan.column_overrides {
        Some(overrides) => Box::new(AutoColumnMap::with_overrides(
            &plan.source_columns,
            &target_columns,
            overrides,
        )),
        None => Box::new(AutoColumnMap::new(&plan.source_columns, &target_columns)),
    };

    let mut sink = TableSink::new(
        Arc::clone(target_connection),
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

/// Resolves the target table's column shape for auto-mapping — identical
/// policy to Import's `resolve_target_columns`: `Create`/`Recreate` build
/// from the source's own columns (same-engine ⇒ 1:1 types); `Existing`/
/// `Truncate` require the table to already exist; `Skip` never writes, so a
/// missing target falls back to the source columns rather than failing the
/// whole migration over an inert table.
fn resolve_target_columns(
    connection: &Arc<dyn Connection>,
    plan: &MigrationTablePlan,
    target_database: &str,
) -> Result<Vec<TransferColumn>, TransferError> {
    match plan.mapping_mode {
        TableMappingMode::Create | TableMappingMode::Recreate => Ok(plan.source_columns.clone()),
        TableMappingMode::Existing | TableMappingMode::Truncate => {
            query_target_columns(connection, plan, target_database)
        }
        TableMappingMode::Skip => Ok(query_target_columns(connection, plan, target_database)
            .unwrap_or_else(|_| plan.source_columns.clone())),
    }
}

fn query_target_columns(
    connection: &Arc<dyn Connection>,
    plan: &MigrationTablePlan,
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

/// Temporarily disables referential integrity on `connection` for the
/// duration of a migration run, when requested and supported, and restores it
/// on drop regardless of how the run ends (success, a mid-transfer error via
/// `?`, or cancellation).
struct ReferentialIntegrityGuard<'a> {
    connection: &'a Arc<dyn Connection>,
    active: bool,
    /// Written by `Drop` when the restore call fails, so the caller can fold
    /// it into `MigrationRunOutcome.warnings` after the guard drops — `Drop`
    /// itself cannot return a `Result`, so logging alone would leave the user
    /// with no signal that RI is still disabled on the target.
    restore_failure: Arc<Mutex<Option<String>>>,
}

impl<'a> ReferentialIntegrityGuard<'a> {
    /// When the target lacks `DriverCapabilities::DISABLE_FK_CHECKS`, the
    /// toggle is simply not attempted (R7: unavailable, not a runtime error)
    /// — the guard is created inert rather than returning an error.
    fn enable(
        connection: &'a Arc<dyn Connection>,
        requested: bool,
        restore_failure: Arc<Mutex<Option<String>>>,
    ) -> Result<Self, TransferError> {
        if !requested || !connection.supports(DriverCapabilities::DISABLE_FK_CHECKS) {
            return Ok(Self {
                connection,
                active: false,
                restore_failure,
            });
        }

        connection
            .set_referential_integrity(false)
            .map_err(|e| TransferError::Sink(e.to_string()))?;

        Ok(Self {
            connection,
            active: true,
            restore_failure,
        })
    }
}

impl Drop for ReferentialIntegrityGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            let result = self.connection.set_referential_integrity(true);

            if let Err(e) = &result
                && let Ok(mut slot) = self.restore_failure.lock()
            {
                *slot = Some(e.to_string());
            }

            // A dropped restore error would leave the target with RI checks
            // disabled and no path to propagate a `Result` from `Drop`; log
            // it in addition to recording it above, rather than silently
            // discarding (project rule) — this is a GPUI-free engine crate,
            // so `log` is the right level, not a user-facing toast.
            result.log_err();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        ColumnInfo, CreateTableSpec, DbError, DbKind, DefaultSqlDialect, DriverCapabilities,
        DriverMetadata, DriverMetadataBuilder, GeneratedQuery, GeneratorError, MutationCategory,
        MutationRequest, QueryGenerator, QueryLanguage, QueryRequest, QueryResult,
        SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TableInfo, Value,
    };
    use std::collections::VecDeque;
    use std::sync::Mutex;

    static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

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

    /// Fake source connection: answers a `SELECT` for each planned table from
    /// a per-table page queue (source rows), plus `schema_foreign_keys` and a
    /// `COUNT(*)` probe.
    struct FakeSourceConnection {
        pages_by_table: Mutex<std::collections::HashMap<String, VecDeque<Vec<Vec<Value>>>>>,
        foreign_keys: Vec<SchemaForeignKeyInfo>,
        metadata: DriverMetadata,
    }

    impl FakeSourceConnection {
        fn new(
            pages_by_table: std::collections::HashMap<String, Vec<Vec<Vec<Value>>>>,
            foreign_keys: Vec<SchemaForeignKeyInfo>,
        ) -> Self {
            let metadata = DriverMetadataBuilder::new(
                "fake-source",
                "Fake Source",
                dbflux_core::DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .build();

            Self {
                pages_by_table: Mutex::new(
                    pages_by_table
                        .into_iter()
                        .map(|(k, v)| (k, v.into()))
                        .collect(),
                ),
                foreign_keys,
                metadata,
            }
        }
    }

    impl Connection for FakeSourceConnection {
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
            if req.sql.contains("COUNT(*)") {
                return Ok(QueryResult::table(
                    Vec::new(),
                    vec![vec![Value::Int(0)]],
                    None,
                    std::time::Duration::ZERO,
                ));
            }

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

        fn schema_foreign_keys(
            &self,
            _database: &str,
            _schema: Option<&str>,
        ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
            Ok(self.foreign_keys.clone())
        }
    }

    /// Fake target connection recording every `execute()`/`table_details()`
    /// call and every referential-integrity toggle — a spy sink proving the
    /// destructive-confirm gate and the RI guard's restore-on-every-path.
    struct FakeTargetConnection {
        executed_sql: Mutex<Vec<String>>,
        table_details_calls: Mutex<Vec<String>>,
        ri_calls: Mutex<Vec<bool>>,
        existing_table_columns: Vec<ColumnInfo>,
        metadata: DriverMetadata,
        ri_toggle_fails: bool,
        has_generator: bool,
        generator: FakeGenerator,
    }

    impl FakeTargetConnection {
        fn new(existing_table_columns: Vec<ColumnInfo>, capabilities: DriverCapabilities) -> Self {
            let metadata = DriverMetadataBuilder::new(
                "fake-target",
                "Fake Target",
                dbflux_core::DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .capabilities(capabilities)
            .build();

            Self {
                executed_sql: Mutex::new(Vec::new()),
                table_details_calls: Mutex::new(Vec::new()),
                ri_calls: Mutex::new(Vec::new()),
                existing_table_columns,
                metadata,
                ri_toggle_fails: false,
                has_generator: true,
                generator: FakeGenerator::new(),
            }
        }

        fn with_ri_toggle_failing(mut self) -> Self {
            self.ri_toggle_fails = true;
            self
        }

        fn without_generator(mut self) -> Self {
            self.has_generator = false;
            self
        }
    }

    impl Connection for FakeTargetConnection {
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
            self.has_generator
                .then_some(&self.generator as &dyn QueryGenerator)
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

        fn set_referential_integrity(&self, enabled: bool) -> Result<(), DbError> {
            self.ri_calls.lock().unwrap().push(enabled);
            if self.ri_toggle_fails && enabled {
                return Err(DbError::NotSupported("restore failed".to_string()));
            }
            Ok(())
        }
    }

    fn pk_column() -> TransferColumn {
        TransferColumn {
            name: "id".to_string(),
            type_name: Some("integer".to_string()),
            nullable: false,
            is_primary_key: true,
        }
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

    fn plan(name: &str, mode: TableMappingMode) -> MigrationTablePlan {
        MigrationTablePlan {
            source_table: TableRef::new(name),
            source_columns: vec![pk_column()],
            target_schema: None,
            target_table: name.to_string(),
            mapping_mode: mode,
            column_overrides: None,
            estimated_total: None,
        }
    }

    fn default_options() -> MigrationOptions {
        MigrationOptions {
            segment_size: 500,
            source_database: "main".to_string(),
            target_database: "main".to_string(),
            destructive_confirmed: false,
            disable_referential_integrity: false,
            manual_order: None,
        }
    }

    fn fk(table_name: &str, referenced_table: &str) -> SchemaForeignKeyInfo {
        SchemaForeignKeyInfo {
            name: format!("fk_{table_name}_{referenced_table}"),
            table_name: table_name.to_string(),
            columns: vec!["id".to_string()],
            referenced_schema: None,
            referenced_table: referenced_table.to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        }
    }

    #[test]
    fn migrates_tables_in_fk_parent_before_child_order() {
        let mut pages = std::collections::HashMap::new();
        pages.insert(
            "parent".to_string(),
            vec![vec![vec![Value::Int(1)], vec![Value::Int(2)]]],
        );
        pages.insert("child".to_string(), vec![vec![vec![Value::Int(10)]]]);

        // "child" references "parent": parent must load first even though it
        // is listed second in `plans`.
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(
            pages,
            vec![fk("child", "parent")],
        ));
        let target = Arc::new(FakeTargetConnection::new(
            vec![existing_column_info()],
            DriverCapabilities::BULK_INSERT,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![
            plan("child", TableMappingMode::Existing),
            plan("parent", TableMappingMode::Existing),
        ];
        let cancel = CancelToken::new();
        let mut order_seen = Vec::new();

        let outcome = run_migration(
            &source,
            &target_conn,
            &plans,
            &default_options(),
            &cancel,
            |index, _rows, _total| order_seen.push(index),
        )
        .unwrap();

        let MigrationOutcome::Completed(run) = outcome else {
            panic!("expected Completed, no cycle in this fixture");
        };
        assert!(!run.cancelled);
        assert_eq!(run.tables[0].source_table, "parent");
        assert_eq!(
            run.tables[0].status,
            TableTransferStatus::Completed { rows: 2 }
        );
        assert_eq!(run.tables[1].source_table, "child");
        assert_eq!(
            run.tables[1].status,
            TableTransferStatus::Completed { rows: 1 }
        );
    }

    #[test]
    fn cyclic_fk_graph_touches_no_table_and_surfaces_the_cycle() {
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(
            std::collections::HashMap::new(),
            vec![fk("a", "b"), fk("b", "a")],
        ));
        let target = Arc::new(FakeTargetConnection::new(
            vec![existing_column_info()],
            DriverCapabilities::BULK_INSERT,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![
            plan("a", TableMappingMode::Existing),
            plan("b", TableMappingMode::Existing),
        ];
        let cancel = CancelToken::new();

        let outcome = run_migration(
            &source,
            &target_conn,
            &plans,
            &default_options(),
            &cancel,
            |_, _, _| {},
        )
        .unwrap();

        match outcome {
            MigrationOutcome::CyclicOrderRequired {
                ordered_prefix,
                cycle,
            } => {
                assert!(ordered_prefix.is_empty());
                let mut names: Vec<&str> = cycle.iter().map(|t| t.name.as_str()).collect();
                names.sort_unstable();
                assert_eq!(names, vec!["a", "b"]);
            }
            MigrationOutcome::Completed(_) => panic!("expected a cyclic result"),
        }

        assert!(
            target.executed_sql.lock().unwrap().is_empty(),
            "a cyclic FK graph must not touch any table"
        );
        assert!(target.table_details_calls.lock().unwrap().is_empty());
    }

    #[test]
    fn manual_order_bypasses_the_automatic_topological_sort() {
        let mut pages = std::collections::HashMap::new();
        pages.insert("a".to_string(), vec![vec![vec![Value::Int(1)]]]);
        pages.insert("b".to_string(), vec![vec![vec![Value::Int(2)]]]);

        // Cyclic FK graph — automatic sort would report `CyclicOrderRequired`
        // — but a manual order lets the caller proceed anyway (engine
        // consumes any user order verbatim).
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(
            pages,
            vec![fk("a", "b"), fk("b", "a")],
        ));
        let target = Arc::new(FakeTargetConnection::new(
            vec![existing_column_info()],
            DriverCapabilities::BULK_INSERT,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![
            plan("a", TableMappingMode::Existing),
            plan("b", TableMappingMode::Existing),
        ];
        let mut options = default_options();
        options.manual_order = Some(vec![TableRef::new("b"), TableRef::new("a")]);
        let cancel = CancelToken::new();
        let mut order_seen = Vec::new();

        let outcome = run_migration(
            &source,
            &target_conn,
            &plans,
            &options,
            &cancel,
            |index, _rows, _total| order_seen.push(index),
        )
        .unwrap();

        let MigrationOutcome::Completed(run) = outcome else {
            panic!("manual order must bypass the cyclic check");
        };
        assert_eq!(run.tables[0].source_table, "b");
        assert_eq!(run.tables[1].source_table, "a");
    }

    #[test]
    fn recreate_without_confirmation_is_rejected_before_touching_any_table() {
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(
            std::collections::HashMap::new(),
            vec![],
        ));
        let target = Arc::new(FakeTargetConnection::new(
            vec![existing_column_info()],
            DriverCapabilities::BULK_INSERT,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![plan("users", TableMappingMode::Recreate)];
        let mut options = default_options();
        options.destructive_confirmed = false;
        let cancel = CancelToken::new();

        let result = run_migration(
            &source,
            &target_conn,
            &plans,
            &options,
            &cancel,
            |_, _, _| {},
        );

        assert!(result.is_err());
        assert!(target.executed_sql.lock().unwrap().is_empty());
        assert!(
            target.table_details_calls.lock().unwrap().is_empty(),
            "the gate must trip before even resolving target columns"
        );
    }

    #[test]
    fn recreate_with_confirmation_proceeds() {
        let mut pages = std::collections::HashMap::new();
        pages.insert("users".to_string(), vec![vec![vec![Value::Int(1)]]]);
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(pages, vec![]));
        let target = Arc::new(FakeTargetConnection::new(
            vec![existing_column_info()],
            DriverCapabilities::BULK_INSERT,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![plan("users", TableMappingMode::Recreate)];
        let mut options = default_options();
        options.destructive_confirmed = true;
        let cancel = CancelToken::new();

        let outcome = run_migration(
            &source,
            &target_conn,
            &plans,
            &options,
            &cancel,
            |_, _, _| {},
        )
        .unwrap();

        let MigrationOutcome::Completed(run) = outcome else {
            panic!("expected Completed");
        };
        assert_eq!(
            run.tables[0].status,
            TableTransferStatus::Completed { rows: 1 }
        );
        let executed = target.executed_sql.lock().unwrap();
        assert!(executed.iter().any(|sql| sql.starts_with("DROP TABLE")));
    }

    #[test]
    fn ri_disabled_before_transfer_and_restored_after_on_success() {
        let mut pages = std::collections::HashMap::new();
        pages.insert("users".to_string(), vec![vec![vec![Value::Int(1)]]]);
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(pages, vec![]));
        let target = Arc::new(FakeTargetConnection::new(
            vec![existing_column_info()],
            DriverCapabilities::BULK_INSERT | DriverCapabilities::DISABLE_FK_CHECKS,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![plan("users", TableMappingMode::Existing)];
        let mut options = default_options();
        options.disable_referential_integrity = true;
        let cancel = CancelToken::new();

        run_migration(
            &source,
            &target_conn,
            &plans,
            &options,
            &cancel,
            |_, _, _| {},
        )
        .unwrap();

        assert_eq!(*target.ri_calls.lock().unwrap(), vec![false, true]);
    }

    #[test]
    fn ri_restored_even_when_a_table_fails_mid_migration() {
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(
            std::collections::HashMap::new(),
            vec![],
        ));
        // No query generator -> `Create` mode's `create_table` errors out of
        // `migrate_one_table`, captured as this table's `Failed` status while
        // `_ri_guard` is still in scope.
        let target = Arc::new(
            FakeTargetConnection::new(vec![], DriverCapabilities::DISABLE_FK_CHECKS)
                .without_generator(),
        );
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![plan("users", TableMappingMode::Create)];
        let mut options = default_options();
        options.disable_referential_integrity = true;
        let cancel = CancelToken::new();

        let outcome = run_migration(
            &source,
            &target_conn,
            &plans,
            &options,
            &cancel,
            |_, _, _| {},
        )
        .expect("a per-table failure must not abort the whole run with Err");

        let MigrationOutcome::Completed(run) = outcome else {
            panic!("expected Completed, no cycle in this fixture");
        };
        assert!(
            matches!(run.tables[0].status, TableTransferStatus::Failed { .. }),
            "expected a Failed status, got: {:?}",
            run.tables[0].status
        );
        assert_eq!(
            *target.ri_calls.lock().unwrap(),
            vec![false, true],
            "RI must be restored even when a table fails mid-migration"
        );
    }

    #[test]
    fn ri_disable_is_skipped_without_error_when_capability_absent() {
        let mut pages = std::collections::HashMap::new();
        pages.insert("users".to_string(), vec![vec![vec![Value::Int(1)]]]);
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(pages, vec![]));
        let target = Arc::new(FakeTargetConnection::new(
            vec![existing_column_info()],
            DriverCapabilities::BULK_INSERT,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![plan("users", TableMappingMode::Existing)];
        let mut options = default_options();
        options.disable_referential_integrity = true;
        let cancel = CancelToken::new();

        run_migration(
            &source,
            &target_conn,
            &plans,
            &options,
            &cancel,
            |_, _, _| {},
        )
        .unwrap();

        assert!(
            target.ri_calls.lock().unwrap().is_empty(),
            "no set_referential_integrity call must be attempted without the capability"
        );
    }

    /// R4-001 regression: a restore-on-drop failure must not just be logged
    /// — it must surface as a warning on the completed outcome so the wizard
    /// (which renders `result_warnings`) tells the user RI is still disabled
    /// on the target.
    #[test]
    fn ri_restore_failure_surfaces_as_a_completion_warning() {
        let mut pages = std::collections::HashMap::new();
        pages.insert("users".to_string(), vec![vec![vec![Value::Int(1)]]]);
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(pages, vec![]));
        let target = Arc::new(
            FakeTargetConnection::new(
                vec![existing_column_info()],
                DriverCapabilities::BULK_INSERT | DriverCapabilities::DISABLE_FK_CHECKS,
            )
            .with_ri_toggle_failing(),
        );
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![plan("users", TableMappingMode::Existing)];
        let mut options = default_options();
        options.disable_referential_integrity = true;
        let cancel = CancelToken::new();

        // `set_referential_integrity(false)` (the disable call) succeeds via
        // the fake — only `enabled=true` (the restore-on-drop call) is
        // configured to fail — so the migration itself must still succeed
        // even though the restore errors internally.
        let outcome = run_migration(
            &source,
            &target_conn,
            &plans,
            &options,
            &cancel,
            |_, _, _| {},
        )
        .unwrap();

        let MigrationOutcome::Completed(run) = outcome else {
            panic!("expected Completed, no cycle in this fixture");
        };
        assert!(
            run.warnings
                .iter()
                .any(|w| w.contains("referential integrity") && w.contains("restored")),
            "a failed RI restore must surface as a completion warning, not only a log line: {:?}",
            run.warnings
        );
    }

    #[test]
    fn cancellation_mid_migration_stops_before_the_next_table() {
        let mut pages = std::collections::HashMap::new();
        pages.insert("a".to_string(), vec![vec![vec![Value::Int(1)]]]);
        pages.insert("b".to_string(), vec![vec![vec![Value::Int(2)]]]);
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(pages, vec![]));
        let target = Arc::new(FakeTargetConnection::new(
            vec![existing_column_info()],
            DriverCapabilities::BULK_INSERT,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![
            plan("a", TableMappingMode::Existing),
            plan("b", TableMappingMode::Existing),
        ];
        let cancel = CancelToken::new();

        let outcome = run_migration(
            &source,
            &target_conn,
            &plans,
            &default_options(),
            &cancel,
            |_index, rows_done, _total| {
                if rows_done == 1 {
                    cancel.cancel();
                }
            },
        )
        .unwrap();

        let MigrationOutcome::Completed(run) = outcome else {
            panic!("expected Completed(cancelled)");
        };
        assert!(run.cancelled);
        assert_eq!(run.tables.len(), 2, "every planned table must be itemized");
        assert_eq!(
            run.tables[0].status,
            TableTransferStatus::Completed { rows: 1 }
        );
        assert_eq!(
            run.tables[1].status,
            TableTransferStatus::NotStarted,
            "the second table must never start"
        );
    }

    /// R4-002/B-007 regression (test requirement #1, Migration): when the
    /// middle table of three fails mid-load, the first table's success and
    /// the third table's `NotStarted` status must both survive in the
    /// returned outcome — not be discarded by a bare `Err`.
    #[test]
    fn a_mid_run_table_failure_itemizes_completed_failed_and_not_started_tables() {
        let mut pages = std::collections::HashMap::new();
        pages.insert("t1".to_string(), vec![vec![vec![Value::Int(1)]]]);
        pages.insert("t3".to_string(), vec![vec![vec![Value::Int(3)]]]);
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(pages, vec![]));

        // No `TRUNCATE_TABLE` capability: only t2's `Truncate` mode fails, in
        // `begin()`'s DDL phase, before any row is written. t1 and t3 use
        // `Existing`, which never touches `TRUNCATE_TABLE`.
        let target = Arc::new(FakeTargetConnection::new(
            vec![existing_column_info()],
            DriverCapabilities::BULK_INSERT,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let plans = vec![
            plan("t1", TableMappingMode::Existing),
            plan("t2", TableMappingMode::Truncate),
            plan("t3", TableMappingMode::Existing),
        ];
        let mut options = default_options();
        options.destructive_confirmed = true;
        let cancel = CancelToken::new();

        let outcome = run_migration(
            &source,
            &target_conn,
            &plans,
            &options,
            &cancel,
            |_, _, _| {},
        )
        .expect("a per-table failure must not abort the whole run with Err");

        let MigrationOutcome::Completed(run) = outcome else {
            panic!("expected Completed, no cycle in this fixture");
        };
        assert_eq!(run.tables.len(), 3);
        assert_eq!(
            run.tables[0].status,
            TableTransferStatus::Completed { rows: 1 },
            "table 1 must stay reported as completed despite table 2 failing later"
        );
        assert!(
            matches!(run.tables[1].status, TableTransferStatus::Failed { .. }),
            "table 2 must be reported as Failed: {:?}",
            run.tables[1].status
        );
        assert_eq!(
            run.tables[2].status,
            TableTransferStatus::NotStarted,
            "table 3 must never have been attempted"
        );
    }

    /// JD-C1 regression (Migration): the target table's columns are
    /// physically reordered relative to the source AND carry a target-only
    /// column plus a source-only column with no match. The INSERT column
    /// list must follow the resolved TARGET order/shape (not the source's),
    /// the unmatched source column must be dropped with a warning (R5), and
    /// the unmatched target column must simply receive NULL with no error.
    #[test]
    fn migration_aligns_insert_columns_with_reordered_and_mismatched_target_shape() {
        let mut pages = std::collections::HashMap::new();
        pages.insert(
            "users".to_string(),
            vec![vec![
                vec![Value::Int(1), Value::Text("x".to_string())],
                vec![Value::Int(2), Value::Text("y".to_string())],
            ]],
        );
        let source: Arc<dyn Connection> = Arc::new(FakeSourceConnection::new(pages, vec![]));

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
        let target = Arc::new(FakeTargetConnection::new(
            target_columns,
            DriverCapabilities::BULK_INSERT,
        ));
        let target_conn: Arc<dyn Connection> = target.clone();

        let mut migration_plan = plan("users", TableMappingMode::Existing);
        migration_plan.source_columns = vec![
            pk_column(),
            TransferColumn {
                name: "legacy_extra".to_string(),
                type_name: Some("text".to_string()),
                nullable: true,
                is_primary_key: false,
            },
        ];
        let plans = vec![migration_plan];
        let cancel = CancelToken::new();

        let outcome = run_migration(
            &source,
            &target_conn,
            &plans,
            &default_options(),
            &cancel,
            |_, _, _| {},
        )
        .unwrap();

        let MigrationOutcome::Completed(run) = outcome else {
            panic!("expected Completed, no cycle in this fixture");
        };
        assert_eq!(
            run.tables[0].status,
            TableTransferStatus::Completed { rows: 2 }
        );
        assert!(
            run.warnings.iter().any(|w| w.contains("legacy_extra")),
            "the unmatched source column must be reported as a non-blocking warning: {:?}",
            run.warnings
        );

        let recorded = target
            .generator
            .recorded_bulk_insert_columns
            .lock()
            .unwrap();
        assert_eq!(
            recorded[0],
            vec!["name".to_string(), "id".to_string()],
            "the INSERT column list must follow the resolved TARGET order/shape"
        );
    }
}
