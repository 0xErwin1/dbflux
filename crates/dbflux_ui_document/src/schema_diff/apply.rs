use std::sync::Arc;

use dbflux_core::{
    AddColumnRequest, AlterColumnRequest, CodeGenerator, Connection, DdlRejection, DefaultSpec,
    DropColumnRequest, EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
    MutationPolicy, QueryRequest, ReadOnlyReason, RiskedChange, SchemaChange,
    TableCreationMetadata, TableInfo, TableRef, TransactionVocab,
};

use super::diff_source::{SelectedTableAlterRoute, select_table_alter_route};

/// Text explaining why DDL apply is refused for a read-only connection,
/// differentiated by why it is read-only. Shared with the `read_only` toast
/// in `view.rs::run_apply` so the two refusal paths never drift.
///
/// `None` falls back to the generic message: `ConnectedProfile` only sets
/// `read_only_reason` when `mutation_policy` is `ReadOnly`, but a `None`
/// reason on a `ReadOnly` policy is still handled instead of panicking.
pub(crate) fn read_only_message(reason: Option<ReadOnlyReason>) -> String {
    match reason {
        Some(ReadOnlyReason::ProfileSetting) => {
            dbflux_i18n::t!("document.schema_diff.apply.read_only_profile")
        }
        Some(ReadOnlyReason::ServerEnforced) => {
            dbflux_i18n::t!("document.schema_diff.apply.read_only_server")
        }
        None => dbflux_i18n::t!("document.schema_diff.apply.read_only"),
    }
}

/// Outcome of a completed (or short-circuited) `DdlApplyExecutor::apply` run.
#[derive(Debug, Clone, PartialEq)]
pub enum DdlApplyOutcome {
    /// All statements executed. `atomic` distinguishes a committed transaction
    /// from a non-atomic run where every statement happened to succeed.
    Success {
        statements_executed: usize,
        atomic: bool,
    },
    /// Only meaningful for the non-atomic path: some statements already
    /// committed (autocommit) before one failed, and nothing was rolled back.
    PartialFailure {
        statements_executed: usize,
        failed_at: usize,
        error: String,
    },
    /// `MutationPolicy::ApprovalRequired` — apply deferred without touching
    /// the connection. The caller is responsible for enqueueing the request
    /// through the approval flow, as done for row mutations.
    Deferred,
    /// `MutationPolicy::ReadOnly` — apply refused without touching the connection.
    Blocked { reason: String },
}

/// Error type for `DdlApplyExecutor::apply`.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorError {
    Generation(String),
    Policy(String),
    Execution(String),
    Transaction(String),
    /// A statement failed AND the subsequent ROLLBACK also failed, so the
    /// transaction was NOT cleanly rolled back and the database is left in an
    /// uncertain state. Distinct from `Transaction` (a clean abort) so the
    /// outcome never claims a rollback that did not happen.
    RollbackFailed {
        context: String,
        error: String,
        rollback_error: String,
    },
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Generation(msg) => write!(f, "DDL generation failed: {}", msg),
            Self::Policy(msg) => write!(f, "DDL preparation refused: {}", msg),
            Self::Execution(msg) => write!(f, "DDL execution failed: {}", msg),
            Self::Transaction(msg) => write!(f, "transaction error: {}", msg),
            Self::RollbackFailed {
                context,
                error,
                rollback_error,
            } => write!(
                f,
                "DDL apply failed {context} ({error}) and ROLLBACK also failed ({rollback_error}); \
                 the transaction was not rolled back and the schema state is uncertain"
            ),
        }
    }
}

impl std::error::Error for ExecutorError {}

/// Maps a single schema change onto driver-owned DDL via the `CodeGenerator`
/// column and index seams. Constraint changes (`PrimaryKeyChanged`,
/// `ForeignKeyChanged`) are not yet expressible through this seam and are
/// rejected rather than silently skipped — callers are expected to filter
/// these out of the changes passed to the executor and surface them
/// separately. Index changes go through `generate_create_index` /
/// `generate_drop_index`; a driver returning `None` (meaning it cannot
/// express the operation) is likewise surfaced as a named rejection rather
/// than silently skipped.
pub(crate) fn build_statements_for_change(
    table: &TableRef,
    change: &SchemaChange,
    code_generator: &dyn CodeGenerator,
) -> Result<Vec<String>, DdlRejection> {
    match change {
        SchemaChange::ColumnAdded(column) => {
            code_generator.generate_add_column(&AddColumnRequest {
                table_name: &table.name,
                schema_name: table.schema.as_deref(),
                column_name: &column.name,
                type_name: &column.type_name,
                nullable: column.nullable,
                default: column.default_value.as_deref(),
            })
        }
        SchemaChange::ColumnRemoved(column) => {
            code_generator.generate_drop_column(&DropColumnRequest {
                table_name: &table.name,
                schema_name: table.schema.as_deref(),
                column_name: &column.name,
            })
        }
        SchemaChange::ColumnTypeChanged { before, after } => {
            code_generator.generate_alter_column(&AlterColumnRequest {
                table_name: &table.name,
                schema_name: table.schema.as_deref(),
                column_name: &before.name,
                new_type: Some(after.type_name.as_str()),
                nullable: None,
                default: None,
            })
        }
        SchemaChange::NullabilityChanged { column, after, .. } => code_generator
            .generate_alter_column(&AlterColumnRequest {
                table_name: &table.name,
                schema_name: table.schema.as_deref(),
                column_name: column,
                new_type: None,
                nullable: Some(*after),
                default: None,
            }),
        SchemaChange::DefaultChanged { column, after, .. } => {
            let default = Some(match after {
                Some(value) => DefaultSpec::Set(value.as_str()),
                None => DefaultSpec::Drop,
            });
            code_generator.generate_alter_column(&AlterColumnRequest {
                table_name: &table.name,
                schema_name: table.schema.as_deref(),
                column_name: column,
                new_type: None,
                nullable: None,
                default,
            })
        }
        SchemaChange::IndexAdded(index) => code_generator
            .generate_create_index(&dbflux_core::CreateIndexRequest {
                index_name: &index.name,
                table_name: &table.name,
                schema_name: table.schema.as_deref(),
                columns: &index.columns,
                unique: index.is_unique,
            })
            .map(|stmt| vec![stmt])
            .ok_or_else(|| DdlRejection {
                reason: format!("driver cannot generate CREATE INDEX for {}", index.name),
                followup: None,
            }),
        SchemaChange::IndexRemoved(index) => code_generator
            .generate_drop_index(&dbflux_core::DropIndexRequest {
                index_name: &index.name,
                table_name: Some(&table.name),
                schema_name: table.schema.as_deref(),
            })
            .map(|stmt| vec![stmt])
            .ok_or_else(|| DdlRejection {
                reason: format!("driver cannot generate DROP INDEX for {}", index.name),
                followup: None,
            }),
        SchemaChange::PrimaryKeyChanged { .. } | SchemaChange::ForeignKeyChanged => {
            Err(DdlRejection {
                reason: "constraint changes are not yet supported by automatic DDL apply"
                    .to_string(),
                followup: None,
            })
        }
    }
}

/// A whole-table `CREATE` or `DROP`. A `Create` is generated through
/// `Connection::generate_code_with_creation_metadata("create_table", …)`,
/// where driver-specific type mapping for `CREATE TABLE` (auto-increment
/// PKs, dialect-specific column types, ...) plus the reference-side creation
/// metadata live; a `Drop` uses the legacy generic
/// `Connection::generate_code("drop_table")` seam, which remains the right
/// fit because a dropped table needs no reference metadata. A `Create`
/// carries the reference side's creation metadata (identity seed/
/// increment, primary-key order, blockers) so the target driver can generate
/// faithfully; `None` means the reference supplied no metadata (driver
/// without introspection, or a pre-metadata snapshot) and metadata-aware
/// drivers must refuse rather than silently flatten the table.
#[derive(Debug, Clone)]
pub enum TableLevelAction {
    Create(Box<TableInfo>, Option<TableCreationMetadata>),
    Drop(TableRef),
}

/// Builds a minimal `TableInfo` (name + schema only) for the legacy
/// `generate_code("drop_table")` seam, which only reads those two fields from it.
fn table_info_from_ref(table: &TableRef) -> TableInfo {
    TableInfo {
        name: table.name.clone(),
        schema: table.schema.clone(),
        columns: None,
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: Default::default(),
        child_items: None,
        storage_hints: None,
    }
}

/// Maps a whole-table add/remove onto the driver-owned generation seams: a
/// `Create` goes through `Connection::generate_code_with_creation_metadata`
/// with the reference metadata, a `Drop` through the legacy generic
/// `generate_code("drop_table")`. A driver that does not implement the
/// requested generator id returns `DbError::NotSupported`, surfaced here as a
/// named `DdlRejection` exactly like the column/index seam does, rather than
/// a silent skip.
pub(crate) fn build_statements_for_table_action(
    connection: &dyn Connection,
    action: &TableLevelAction,
) -> Result<Vec<String>, DdlRejection> {
    // The two actions deliberately use different seams: only the metadata-
    // aware seam can carry reference creation metadata for a faithful
    // CREATE TABLE, while a drop needs no metadata and must go through the
    // legacy generic `generate_code` — a driver override may implement the
    // metadata-aware seam only for `create_table` and handle other ids
    // differently, so routing a drop through it would be a compatibility
    // hazard. Both map `DbError::NotSupported` to a named `DdlRejection`
    // exactly like the column/index seam does, rather than a silent skip.
    let result = match action {
        TableLevelAction::Create(table, metadata) => connection
            .generate_code_with_creation_metadata("create_table", table, metadata.as_ref()),
        TableLevelAction::Drop(table_ref) => {
            connection.generate_code("drop_table", &table_info_from_ref(table_ref))
        }
    };

    result.map(|sql| vec![sql]).map_err(|e| DdlRejection {
        reason: e.to_string(),
        followup: None,
    })
}

/// Builds the full ordered statement list for `changes` (and, if present, a
/// whole-table `table_action` run first), failing fast on the first change
/// the driver cannot express. Mirrors the MCP `run_alter_transactional`
/// template of building all SQL before any statement executes, so a
/// rejection never leaves a partially-applied change.
fn build_all_statements(
    table: &TableRef,
    changes: &[RiskedChange],
    table_action: Option<&TableLevelAction>,
    connection: &dyn Connection,
) -> Result<Vec<String>, ExecutorError> {
    let mut statements = Vec::new();

    if let Some(action) = table_action {
        let stmts = build_statements_for_table_action(connection, action).map_err(|rejection| {
            ExecutorError::Generation(format!("table action rejected: {}", rejection.reason))
        })?;
        statements.extend(stmts);
    }

    let code_generator = connection.code_generator();
    for (index, risked) in changes.iter().enumerate() {
        let stmts = build_statements_for_change(table, &risked.change, code_generator).map_err(
            |rejection| {
                ExecutorError::Generation(format!(
                    "change {} rejected: {}",
                    index, rejection.reason
                ))
            },
        )?;
        statements.extend(stmts);
    }

    Ok(statements)
}

/// Dependencies injected into `DdlApplyExecutor`.
#[derive(Clone)]
pub struct DdlApplyDeps {
    pub connection: Arc<dyn Connection>,
    pub event_sink: Option<Arc<dyn EventSink>>,
    pub connection_id: String,
    pub driver_id: String,
    pub policy: MutationPolicy,
    /// Set when `policy` is `MutationPolicy::ReadOnly`, explaining whether the
    /// profile itself or the server enforced it. Mirrors
    /// `ConnectedProfile::read_only_reason` — drives the reason-aware refusal
    /// message in `apply()`.
    pub read_only_reason: Option<ReadOnlyReason>,
}

/// A prepared, consumable table operation whose SQL or driver plan is frozen.
///
/// Construct all selected tables with [`DdlApplyExecutor::prepare_all`] before
/// executing any of them. Legacy statements are generated once at preparation;
/// driver-managed plans are single-use objects owned by the connection.
pub struct FrozenDdlApply {
    table: TableRef,
    deps: DdlApplyDeps,
    execution: FrozenTableExecution,
}

enum FrozenTableExecution {
    Managed(Box<dyn dbflux_core::PreparedTableAlter>),
    Legacy(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenApplyRunOutcome {
    pub statements_applied: usize,
    pub tables_applied: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FrozenApplyFailure {
    pub failed_table: TableRef,
    pub tables_applied: usize,
    pub statements_applied: usize,
    pub error: Box<ExecutorError>,
}

impl FrozenDdlApply {
    pub fn table(&self) -> &TableRef {
        &self.table
    }

    pub fn preview_warnings(&self) -> Vec<String> {
        match &self.execution {
            FrozenTableExecution::Managed(plan) => plan.preview().warnings.clone(),
            FrozenTableExecution::Legacy(_) => Vec::new(),
        }
    }

    pub fn preview_statements(&self) -> Vec<String> {
        match &self.execution {
            FrozenTableExecution::Managed(plan) => plan.preview().statements.clone(),
            FrozenTableExecution::Legacy(statements) => statements.clone(),
        }
    }

    /// Executes the already-prepared table work exactly once.
    pub fn execute(self) -> Result<DdlApplyOutcome, ExecutorError> {
        match self.deps.policy {
            MutationPolicy::ApprovalRequired => return Ok(DdlApplyOutcome::Deferred),
            MutationPolicy::ReadOnly => {
                return Ok(DdlApplyOutcome::Blocked {
                    reason: read_only_message(self.deps.read_only_reason),
                });
            }
            MutationPolicy::Allowed => {}
        }

        let executor = DdlApplyExecutor {
            table: self.table,
            changes: Vec::new(),
            table_action: None,
            deps: self.deps,
        };
        match self.execution {
            FrozenTableExecution::Managed(plan) => executor.apply_managed(plan),
            FrozenTableExecution::Legacy(statements) => executor.apply_frozen_legacy(&statements),
        }
    }

    /// Executes frozen tables in order, retaining only work known to have
    /// committed when a later table fails. A transactional or driver-managed
    /// failure contributes no uncertain statement count.
    pub fn execute_all(
        tables: Vec<FrozenDdlApply>,
    ) -> Result<FrozenApplyRunOutcome, FrozenApplyFailure> {
        let mut statements_applied = 0;
        let mut tables_applied = 0;

        for table in tables {
            let table_ref = table.table.clone();
            match table.execute() {
                Ok(DdlApplyOutcome::Success {
                    statements_executed,
                    ..
                }) => {
                    statements_applied += statements_executed;
                    tables_applied += 1;
                }
                Ok(DdlApplyOutcome::PartialFailure {
                    statements_executed,
                    error,
                    ..
                }) => {
                    statements_applied += statements_executed;
                    return Err(FrozenApplyFailure {
                        failed_table: table_ref,
                        tables_applied,
                        statements_applied,
                        error: Box::new(ExecutorError::Execution(error)),
                    });
                }
                Ok(DdlApplyOutcome::Deferred) | Ok(DdlApplyOutcome::Blocked { .. }) => {
                    return Err(FrozenApplyFailure {
                        failed_table: table_ref,
                        tables_applied,
                        statements_applied,
                        error: Box::new(ExecutorError::Execution(
                            "frozen table execution was not authorized".to_string(),
                        )),
                    });
                }
                Err(error) => {
                    return Err(FrozenApplyFailure {
                        failed_table: table_ref,
                        tables_applied,
                        statements_applied,
                        error: Box::new(error),
                    });
                }
            }
        }

        Ok(FrozenApplyRunOutcome {
            statements_applied,
            tables_applied,
        })
    }
}

/// Plain (non-GPUI) struct that applies the DDL for a reviewed schema diff.
///
/// Parallels `data_grid_panel::mutation_executor::MutationExecutor` rather than
/// reusing it: that executor is `VisualMutationSpec`-based, while this one
/// works from a driver-agnostic statement list built through the
/// `CodeGenerator` column seam. Each execution method is synchronous and
/// intended to run on a background thread.
pub struct DdlApplyExecutor {
    table: TableRef,
    changes: Vec<RiskedChange>,
    table_action: Option<TableLevelAction>,
    deps: DdlApplyDeps,
}

impl DdlApplyExecutor {
    pub fn new(table: TableRef, changes: Vec<RiskedChange>, deps: DdlApplyDeps) -> Self {
        Self {
            table,
            changes,
            table_action: None,
            deps,
        }
    }

    /// Adds a whole-table `CREATE`/`DROP` to run alongside `changes`, for
    /// `TableChange::TableAdded`/`TableRemoved`, which carry no `RiskedChange`
    /// list of their own.
    pub fn with_table_action(mut self, action: TableLevelAction) -> Self {
        self.table_action = Some(action);
        self
    }

    /// Prepares a table's selected work into a consumable managed plan or a
    /// captured legacy statement list. Preparation is read-only, and callers
    /// must prepare every selected table before executing the first one.
    pub fn prepare(&self) -> Result<FrozenDdlApply, ExecutorError> {
        self.ensure_preparation_allowed()?;
        let route = select_table_alter_route(
            self.table.clone(),
            &self.changes,
            self.deps.connection.table_alter_planner().is_some(),
        )
        .map_err(ExecutorError::Generation)?;

        let execution = match route {
            SelectedTableAlterRoute::Managed(request) => {
                if self.table_action.is_some() {
                    return Err(ExecutorError::Generation(
                        "table-level actions cannot be mixed with a managed table alteration"
                            .to_string(),
                    ));
                }
                let planner = self.deps.connection.table_alter_planner().ok_or_else(|| {
                    ExecutorError::Generation(
                        "managed table alteration planner disappeared during preparation"
                            .to_string(),
                    )
                })?;
                let prepared = planner.prepare(&request).map_err(|error| {
                    ExecutorError::Generation(format!("managed table alteration rejected: {error}"))
                })?;
                FrozenTableExecution::Managed(prepared)
            }
            SelectedTableAlterRoute::Legacy => FrozenTableExecution::Legacy(build_all_statements(
                &self.table,
                &self.changes,
                self.table_action.as_ref(),
                self.deps.connection.as_ref(),
            )?),
        };

        Ok(FrozenDdlApply {
            table: self.table.clone(),
            deps: self.deps.clone(),
            execution,
        })
    }

    /// Prepares all selected tables before any execution may begin.
    pub fn prepare_all(
        executors: Vec<DdlApplyExecutor>,
    ) -> Result<Vec<FrozenDdlApply>, ExecutorError> {
        for executor in &executors {
            executor.ensure_preparation_allowed()?;
        }
        executors.iter().map(Self::prepare).collect()
    }

    /// Builds the frozen preview for this table without executing it.
    fn ensure_preparation_allowed(&self) -> Result<(), ExecutorError> {
        match self.deps.policy {
            MutationPolicy::Allowed => Ok(()),
            MutationPolicy::ReadOnly => Err(ExecutorError::Policy(read_only_message(
                self.deps.read_only_reason,
            ))),
            MutationPolicy::ApprovalRequired => Err(ExecutorError::Policy(
                "schema changes require approval before preparation".to_string(),
            )),
        }
    }

    pub fn preview_statements(&self) -> Result<Vec<String>, ExecutorError> {
        Ok(self.prepare()?.preview_statements())
    }

    /// Applies the executor's changes, gated by governance and dispatched to
    /// the transactional or non-atomic path depending on driver support.
    ///
    /// `MutationPolicy::ApprovalRequired` defers without touching the
    /// connection at all — the caller enqueues the request through the same
    /// approval/pending-execution path used for row mutations, exactly as
    /// `DataGridPanel::on_mutation_run_requested` does before ever
    /// constructing a `MutationExecutor`. `MutationPolicy::ReadOnly` refuses
    /// outright for the same reason.
    pub fn apply(&self) -> Result<DdlApplyOutcome, ExecutorError> {
        match self.deps.policy {
            MutationPolicy::ApprovalRequired => return Ok(DdlApplyOutcome::Deferred),
            MutationPolicy::ReadOnly => {
                return Ok(DdlApplyOutcome::Blocked {
                    reason: read_only_message(self.deps.read_only_reason),
                });
            }
            MutationPolicy::Allowed => {}
        }

        self.prepare()?.execute()
    }

    /// Executes captured legacy statements with the existing transaction and
    /// audit semantics. The input is never regenerated after confirmation.
    fn apply_frozen_legacy(&self, statements: &[String]) -> Result<DdlApplyOutcome, ExecutorError> {
        if statements.is_empty() {
            return Ok(DdlApplyOutcome::Success {
                statements_executed: 0,
                atomic: true,
            });
        }

        let run_id = uuid::Uuid::new_v4().to_string();
        let pending_event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Pending,
        )
        .with_action("schema_diff.apply")
        .with_summary(format!(
            "apply {} DDL statement(s) to {}",
            statements.len(),
            self.table.name
        ))
        .with_correlation_id(run_id.clone());
        self.emit_event(pending_event);

        if self.deps.connection.supports_transactional_ddl() {
            self.apply_transactional(statements, &run_id)
        } else {
            self.apply_non_atomic(statements, &run_id)
        }
    }

    /// Executes a single-use driver-owned plan directly. The driver owns its
    /// lifecycle, so this deliberately adds no outer SQL transaction.
    fn apply_managed(
        &self,
        plan: Box<dyn dbflux_core::PreparedTableAlter>,
    ) -> Result<DdlApplyOutcome, ExecutorError> {
        let run_id = uuid::Uuid::new_v4().to_string();
        let pending_event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Pending,
        )
        .with_action("schema_diff.apply")
        .with_summary(format!(
            "apply driver-managed table alteration to {}",
            self.table.name
        ))
        .with_correlation_id(run_id.clone());
        self.emit_event(pending_event);

        match plan.execute() {
            Ok(outcome) => {
                self.emit_success_event(&run_id, outcome.statement_count, outcome.table_atomic);
                Ok(DdlApplyOutcome::Success {
                    statements_executed: outcome.statement_count,
                    atomic: outcome.table_atomic,
                })
            }
            Err(error) => {
                let error = error.to_string();
                self.emit_failure_event(&run_id, &error);
                Err(ExecutorError::Execution(error))
            }
        }
    }

    /// Executes `statements` as BEGIN / DDL... / COMMIT, rolling back on the
    /// first failure. Mirrors MCP `run_alter_transactional`.
    fn apply_transactional(
        &self,
        statements: &[String],
        run_id: &str,
    ) -> Result<DdlApplyOutcome, ExecutorError> {
        let vocab = TransactionVocab::for_kind(self.deps.connection.kind()).ok_or_else(|| {
            ExecutorError::Transaction("driver does not support SQL transactions".to_string())
        })?;

        let begin_req = QueryRequest::new(vocab.begin);
        if let Err(e) = self.deps.connection.execute(&begin_req) {
            let err_msg = e.to_string();
            self.emit_failure_event(run_id, &err_msg);
            return Err(ExecutorError::Transaction(err_msg));
        }

        for (index, statement) in statements.iter().enumerate() {
            let request = QueryRequest::new(statement.clone());
            if let Err(e) = self.deps.connection.execute(&request) {
                let err_msg = e.to_string();
                return Err(self.rollback_and_classify(
                    &vocab,
                    run_id,
                    &format!("at statement {index}"),
                    &err_msg,
                ));
            }
        }

        let commit_req = QueryRequest::new(vocab.commit);
        if let Err(e) = self.deps.connection.execute(&commit_req) {
            let err_msg = e.to_string();
            return Err(self.rollback_and_classify(&vocab, run_id, "during COMMIT", &err_msg));
        }

        self.emit_success_event(run_id, statements.len(), true);
        Ok(DdlApplyOutcome::Success {
            statements_executed: statements.len(),
            atomic: true,
        })
    }

    /// Executes `statements` one at a time without a transaction wrapper,
    /// stopping at the first failure. Because there is no transaction,
    /// statements applied before the failure are already committed —
    /// `DdlApplyOutcome::PartialFailure` surfaces that non-atomicity to the
    /// caller instead of reporting a plain error. Mirrors MCP
    /// `run_alter_non_atomic`.
    fn apply_non_atomic(
        &self,
        statements: &[String],
        run_id: &str,
    ) -> Result<DdlApplyOutcome, ExecutorError> {
        let mut executed = 0usize;

        for statement in statements {
            let request = QueryRequest::new(statement.clone());
            if let Err(e) = self.deps.connection.execute(&request) {
                let err_msg = e.to_string();
                self.emit_partial_failure_event(run_id, executed, &err_msg);
                return Ok(DdlApplyOutcome::PartialFailure {
                    statements_executed: executed,
                    failed_at: executed,
                    error: err_msg,
                });
            }

            executed += 1;
        }

        self.emit_success_event(run_id, executed, false);
        Ok(DdlApplyOutcome::Success {
            statements_executed: executed,
            atomic: false,
        })
    }

    /// Attempts a ROLLBACK, returning the driver error string when it fails so
    /// the caller can treat a failed rollback as a distinct, observable outcome
    /// rather than silently assuming the transaction was undone.
    fn attempt_rollback(&self, vocab: &TransactionVocab) -> Result<(), String> {
        let rollback_req = QueryRequest::new(vocab.rollback);
        self.deps
            .connection
            .execute(&rollback_req)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// Rolls back after a mid-transaction failure and classifies the result: a
    /// clean rollback keeps the honest "aborted and rolled back" error, while a
    /// failed rollback surfaces `RollbackFailed` and its own audit event so the
    /// outcome never claims a rollback that did not happen.
    fn rollback_and_classify(
        &self,
        vocab: &TransactionVocab,
        run_id: &str,
        context: &str,
        original_error: &str,
    ) -> ExecutorError {
        match self.attempt_rollback(vocab) {
            Ok(()) => {
                self.emit_failure_event(run_id, original_error);
                ExecutorError::Transaction(format!(
                    "DDL apply aborted and rolled back {context}: {original_error}"
                ))
            }
            Err(rollback_error) => {
                self.emit_rollback_failed_event(run_id, original_error, &rollback_error);
                ExecutorError::RollbackFailed {
                    context: context.to_string(),
                    error: original_error.to_string(),
                    rollback_error,
                }
            }
        }
    }

    fn emit_event(&self, mut event: EventRecord) {
        event.connection_id = Some(self.deps.connection_id.clone());
        event.driver_id = Some(self.deps.driver_id.clone());
        if let Some(sink) = &self.deps.event_sink
            && let Err(e) = sink.record(event)
        {
            log::warn!("schema diff apply audit event failed: {e}");
        }
    }

    fn emit_success_event(&self, run_id: &str, statements_executed: usize, atomic: bool) {
        let mode = if atomic { "atomic" } else { "non-atomic" };
        let event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Success,
        )
        .with_action("schema_diff.apply")
        .with_summary(format!(
            "applied {} DDL statement(s) to {} ({})",
            statements_executed, self.table.name, mode
        ))
        .with_correlation_id(run_id.to_string());
        self.emit_event(event);
    }

    fn emit_partial_failure_event(&self, run_id: &str, statements_executed: usize, error: &str) {
        let event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Error,
            EventCategory::Query,
            EventOutcome::Failure,
        )
        .with_action("schema_diff.apply")
        .with_summary(format!(
            "DDL apply to {} stopped after {} statement(s) (non-atomic, not rolled back): {}",
            self.table.name, statements_executed, error
        ))
        .with_error("ddl_apply_partial_failure", error)
        .with_correlation_id(run_id.to_string());
        self.emit_event(event);
    }

    fn emit_failure_event(&self, run_id: &str, error: &str) {
        let event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Error,
            EventCategory::Query,
            EventOutcome::Failure,
        )
        .with_action("schema_diff.apply")
        .with_summary(format!(
            "DDL apply to {} failed: {}",
            self.table.name, error
        ))
        .with_error("ddl_apply_failed", error)
        .with_correlation_id(run_id.to_string());
        self.emit_event(event);
    }

    fn emit_rollback_failed_event(&self, run_id: &str, error: &str, rollback_error: &str) {
        let event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Error,
            EventCategory::Query,
            EventOutcome::Failure,
        )
        .with_action("schema_diff.apply")
        .with_summary(format!(
            "DDL apply to {} failed ({}) and ROLLBACK also failed ({}); \
             transaction not rolled back, schema state uncertain",
            self.table.name, error, rollback_error
        ))
        .with_error(
            "ddl_apply_rollback_failed",
            format!("{error}; rollback failed: {rollback_error}"),
        )
        .with_correlation_id(run_id.to_string());
        self.emit_event(event);
    }

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        ColumnSnapshot, DatabaseCategory, DbError, DbKind, DefaultSqlDialect, DriverCapabilities,
        DriverMetadataBuilder, EventRecord, EventSink, EventSinkError, ExecutionClassification,
        FormattedError, IndexSnapshot, QueryLanguage, QueryResult, SchemaLoadingStrategy,
        SchemaSnapshot,
    };
    use std::sync::Mutex;

    fn column(
        name: &str,
        type_name: &str,
        nullable: bool,
        default: Option<&str>,
    ) -> ColumnSnapshot {
        ColumnSnapshot {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable,
            is_primary_key: false,
            default_value: default.map(str::to_string),
        }
    }

    fn risked(change: SchemaChange, risk: ExecutionClassification) -> RiskedChange {
        RiskedChange { change, risk }
    }

    // -----------------------------------------------------------------
    // build_statements_for_change — pure-function mapping tests
    // -----------------------------------------------------------------

    mod statement_mapping_tests {
        use super::*;

        struct StubCodeGenerator;

        impl CodeGenerator for StubCodeGenerator {
            fn capabilities(&self) -> dbflux_core::CodeGenCapabilities {
                dbflux_core::CodeGenCapabilities::ADD_COLUMN
                    | dbflux_core::CodeGenCapabilities::DROP_COLUMN
                    | dbflux_core::CodeGenCapabilities::ALTER_COLUMN
                    | dbflux_core::CodeGenCapabilities::CREATE_INDEX
                    | dbflux_core::CodeGenCapabilities::DROP_INDEX
            }

            fn generate_create_index(
                &self,
                request: &dbflux_core::CreateIndexRequest,
            ) -> Option<String> {
                let unique = if request.unique { "UNIQUE " } else { "" };
                Some(format!(
                    "CREATE {}INDEX {} ON {} ({})",
                    unique,
                    request.index_name,
                    request.table_name,
                    request.columns.join(", ")
                ))
            }

            fn generate_drop_index(
                &self,
                request: &dbflux_core::DropIndexRequest,
            ) -> Option<String> {
                let table = request.table_name.unwrap_or_default();
                Some(format!("DROP INDEX {} ON {}", request.index_name, table))
            }

            fn generate_add_column(
                &self,
                request: &AddColumnRequest,
            ) -> Result<Vec<String>, DdlRejection> {
                Ok(vec![format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    request.table_name, request.column_name, request.type_name
                )])
            }

            fn generate_drop_column(
                &self,
                request: &DropColumnRequest,
            ) -> Result<Vec<String>, DdlRejection> {
                Ok(vec![format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    request.table_name, request.column_name
                )])
            }

            fn generate_alter_column(
                &self,
                request: &AlterColumnRequest,
            ) -> Result<Vec<String>, DdlRejection> {
                Ok(vec![format!(
                    "ALTER TABLE {} ALTER COLUMN {} new_type={:?} nullable={:?} default={:?}",
                    request.table_name,
                    request.column_name,
                    request.new_type,
                    request.nullable,
                    request.default
                )])
            }
        }

        fn table() -> TableRef {
            TableRef {
                schema: Some("public".to_string()),
                name: "users".to_string(),
            }
        }

        #[test]
        fn column_added_maps_to_generate_add_column() {
            let change = SchemaChange::ColumnAdded(column("email", "text", true, None));
            let stmts = build_statements_for_change(&table(), &change, &StubCodeGenerator).unwrap();
            assert_eq!(stmts, vec!["ALTER TABLE users ADD COLUMN email text"]);
        }

        #[test]
        fn column_removed_maps_to_generate_drop_column() {
            let change = SchemaChange::ColumnRemoved(column("email", "text", true, None));
            let stmts = build_statements_for_change(&table(), &change, &StubCodeGenerator).unwrap();
            assert_eq!(stmts, vec!["ALTER TABLE users DROP COLUMN email"]);
        }

        #[test]
        fn type_changed_maps_to_alter_column_with_new_type() {
            let change = SchemaChange::ColumnTypeChanged {
                before: column("id", "integer", false, None),
                after: column("id", "bigint", false, None),
            };
            let stmts = build_statements_for_change(&table(), &change, &StubCodeGenerator).unwrap();
            assert!(stmts[0].contains("new_type=Some(\"bigint\")"));
            assert!(stmts[0].contains("nullable=None"));
        }

        #[test]
        fn nullability_changed_maps_to_alter_column_with_nullable() {
            let change = SchemaChange::NullabilityChanged {
                column: "email".to_string(),
                before: true,
                after: false,
            };
            let stmts = build_statements_for_change(&table(), &change, &StubCodeGenerator).unwrap();
            assert!(stmts[0].contains("nullable=Some(false)"));
            assert!(stmts[0].contains("new_type=None"));
        }

        #[test]
        fn default_changed_to_value_maps_to_default_set() {
            let change = SchemaChange::DefaultChanged {
                column: "status".to_string(),
                before: None,
                after: Some("'active'".to_string()),
            };
            let stmts = build_statements_for_change(&table(), &change, &StubCodeGenerator).unwrap();
            assert!(stmts[0].contains("default=Some(Set(\"'active'\"))"));
        }

        #[test]
        fn default_changed_to_none_maps_to_default_drop() {
            let change = SchemaChange::DefaultChanged {
                column: "status".to_string(),
                before: Some("'active'".to_string()),
                after: None,
            };
            let stmts = build_statements_for_change(&table(), &change, &StubCodeGenerator).unwrap();
            assert!(stmts[0].contains("default=Some(Drop)"));
        }

        #[test]
        fn constraint_changes_are_rejected() {
            let changes = vec![
                SchemaChange::PrimaryKeyChanged {
                    before: vec!["id".to_string()],
                    after: vec!["uuid".to_string()],
                },
                SchemaChange::ForeignKeyChanged,
            ];

            for change in changes {
                let result = build_statements_for_change(&table(), &change, &StubCodeGenerator);
                assert!(
                    result.is_err(),
                    "expected {:?} to be rejected, got {:?}",
                    change,
                    result
                );
            }
        }

        #[test]
        fn index_added_maps_to_generate_create_index() {
            let change = SchemaChange::IndexAdded(IndexSnapshot {
                name: "idx_email".to_string(),
                columns: vec!["email".to_string()],
                is_unique: true,
            });
            let stmts = build_statements_for_change(&table(), &change, &StubCodeGenerator).unwrap();
            assert_eq!(
                stmts,
                vec!["CREATE UNIQUE INDEX idx_email ON users (email)"]
            );
        }

        #[test]
        fn index_removed_maps_to_generate_drop_index() {
            let change = SchemaChange::IndexRemoved(IndexSnapshot {
                name: "idx_email".to_string(),
                columns: vec!["email".to_string()],
                is_unique: false,
            });
            let stmts = build_statements_for_change(&table(), &change, &StubCodeGenerator).unwrap();
            assert_eq!(stmts, vec!["DROP INDEX idx_email ON users"]);
        }

        #[test]
        fn index_generator_returning_none_surfaces_as_rejection() {
            struct NoneCodeGenerator;

            impl CodeGenerator for NoneCodeGenerator {
                fn capabilities(&self) -> dbflux_core::CodeGenCapabilities {
                    dbflux_core::CodeGenCapabilities::empty()
                }
            }

            let added = SchemaChange::IndexAdded(IndexSnapshot {
                name: "idx_email".to_string(),
                columns: vec!["email".to_string()],
                is_unique: true,
            });
            let removed = SchemaChange::IndexRemoved(IndexSnapshot {
                name: "idx_email".to_string(),
                columns: vec!["email".to_string()],
                is_unique: true,
            });

            let added_result = build_statements_for_change(&table(), &added, &NoneCodeGenerator);
            let removed_result =
                build_statements_for_change(&table(), &removed, &NoneCodeGenerator);

            assert!(
                added_result.is_err(),
                "expected None from generate_create_index to surface as a rejection, got {:?}",
                added_result
            );
            assert!(
                removed_result.is_err(),
                "expected None from generate_drop_index to surface as a rejection, got {:?}",
                removed_result
            );
        }
    }

    // -----------------------------------------------------------------
    // build_statements_for_table_action — whole-table CREATE/DROP mapping
    // -----------------------------------------------------------------

    mod table_action_mapping_tests {
        use super::*;

        fn table_info() -> TableInfo {
            TableInfo {
                name: "orders".to_string(),
                schema: Some("public".to_string()),
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: Default::default(),
                child_items: None,
                storage_hints: None,
            }
        }

        #[test]
        fn table_added_maps_to_generate_code_create_table() {
            let conn = FakeConnection::new(DbKind::Postgres, true);
            let action = TableLevelAction::Create(Box::new(table_info()), None);

            let stmts = build_statements_for_table_action(conn.as_ref(), &action).unwrap();

            assert_eq!(stmts, vec!["CREATE TABLE orders (id INT)"]);
        }

        #[test]
        fn table_removed_maps_to_generate_code_drop_table() {
            let conn = FakeConnection::new(DbKind::Postgres, true);
            let action = TableLevelAction::Drop(TableRef {
                schema: Some("public".to_string()),
                name: "orders".to_string(),
            });

            let stmts = build_statements_for_table_action(conn.as_ref(), &action).unwrap();

            assert_eq!(stmts, vec!["DROP TABLE orders"]);
        }

        #[test]
        fn table_removed_routes_through_legacy_seam_not_metadata_aware() {
            // A driver override may implement the metadata-aware seam only for
            // the generators that carry reference metadata (create_table) and
            // behave differently there; a drop must therefore reach the legacy
            // generic `generate_code` seam and never the metadata-aware one.
            let conn = FakeConnection::new(DbKind::Postgres, true);
            let action = TableLevelAction::Drop(TableRef {
                schema: Some("public".to_string()),
                name: "orders".to_string(),
            });

            let stmts = build_statements_for_table_action(conn.as_ref(), &action).unwrap();

            assert_eq!(stmts, vec!["DROP TABLE orders"]);
            assert!(
                conn.creation_metadata_arg.lock().unwrap().is_empty(),
                "a drop must not route through the metadata-aware seam"
            );
            assert_eq!(
                *conn.generate_code_calls.lock().unwrap(),
                vec!["drop_table".to_string()],
                "a drop must go through the legacy generic generate_code seam"
            );
        }

        #[test]
        fn table_create_action_forwards_reference_metadata_to_the_driver_seam() {
            let conn = FakeConnection::new(DbKind::SqlServer, true);
            let metadata = dbflux_core::TableCreationMetadata {
                schema: Some("public".to_string()),
                table: "orders".to_string(),
                completeness: dbflux_core::MetadataCompleteness::Complete,
                identity: Some(dbflux_core::IdentitySpec {
                    column: "id".to_string(),
                    seed: "1".to_string(),
                    increment: "1".to_string(),
                }),
                primary_key: None,
                blockers: Vec::new(),
            };
            let action = TableLevelAction::Create(Box::new(table_info()), Some(metadata.clone()));

            let stmts = build_statements_for_table_action(conn.as_ref(), &action).unwrap();

            assert!(!stmts.is_empty());
            let recorded = conn.creation_metadata_arg.lock().unwrap().clone();
            assert_eq!(
                recorded,
                vec![Some(metadata)],
                "reference creation metadata must reach the target driver's metadata-aware seam"
            );
        }

        #[test]
        fn table_create_action_without_reference_metadata_forwards_none() {
            let conn = FakeConnection::new(DbKind::SqlServer, true);
            let action = TableLevelAction::Create(Box::new(table_info()), None);

            build_statements_for_table_action(conn.as_ref(), &action).unwrap();

            let recorded = conn.creation_metadata_arg.lock().unwrap().clone();
            assert_eq!(
                recorded,
                vec![None],
                "an absent reference metadata must arrive as None so metadata-aware drivers refuse"
            );
        }

        #[test]
        fn driver_without_table_ddl_support_surfaces_as_rejection() {
            let conn = FakeConnection::without_table_ddl_support(DbKind::SqlServer, true);

            let create_result = build_statements_for_table_action(
                conn.as_ref(),
                &TableLevelAction::Create(Box::new(table_info()), None),
            );
            let drop_result = build_statements_for_table_action(
                conn.as_ref(),
                &TableLevelAction::Drop(TableRef {
                    schema: Some("public".to_string()),
                    name: "orders".to_string(),
                }),
            );

            assert!(
                create_result.is_err(),
                "expected NotSupported from generate_code to surface as a rejection, got {:?}",
                create_result
            );
            assert!(
                drop_result.is_err(),
                "expected NotSupported from generate_code to surface as a rejection, got {:?}",
                drop_result
            );
        }
    }

    // -----------------------------------------------------------------
    // DdlApplyExecutor — end-to-end apply() tests
    // -----------------------------------------------------------------

    struct RecordingCodeGenerator {
        generate_add_calls: Arc<Mutex<usize>>,
    }

    impl CodeGenerator for RecordingCodeGenerator {
        fn capabilities(&self) -> dbflux_core::CodeGenCapabilities {
            dbflux_core::CodeGenCapabilities::ADD_COLUMN
        }

        fn generate_add_column(
            &self,
            request: &AddColumnRequest,
        ) -> Result<Vec<String>, DdlRejection> {
            *self.generate_add_calls.lock().unwrap() += 1;
            Ok(vec![format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                request.table_name, request.column_name, request.type_name
            )])
        }
    }

    struct FakeConnection {
        db_kind: DbKind,
        meta: dbflux_core::DriverMetadata,
        transactional_ddl: bool,
        code_generator: RecordingCodeGenerator,
        generation_calls: Arc<Mutex<usize>>,
        calls: Arc<Mutex<Vec<String>>>,
        managed_planner: Option<FakeTableAlterPlanner>,
        fail_on_sql_containing: Option<&'static str>,
        fail_rollback: bool,
        supports_table_ddl: bool,
        creation_metadata_arg: Arc<Mutex<Vec<Option<dbflux_core::TableCreationMetadata>>>>,
        generate_code_calls: Arc<Mutex<Vec<String>>>,
    }

    struct FakeTableAlterPlanner {
        calls: Arc<Mutex<Vec<String>>>,
    }

    struct FakePreparedTableAlter {
        calls: Arc<Mutex<Vec<String>>>,
        preview: dbflux_core::TableAlterPreview,
    }

    impl dbflux_core::TableAlterPlanner for FakeTableAlterPlanner {
        fn prepare(
            &self,
            request: &dbflux_core::TableAlterRequest,
        ) -> Result<Box<dyn dbflux_core::PreparedTableAlter>, DbError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("PREPARE {}", request.table.name));
            Ok(Box::new(FakePreparedTableAlter {
                calls: Arc::clone(&self.calls),
                preview: dbflux_core::TableAlterPreview {
                    route: dbflux_core::TableAlterRoute::Rebuild,
                    statements: vec![format!("managed {}", request.table.name)],
                    warnings: Vec::new(),
                    table_atomic: true,
                    driver_managed: true,
                },
            }))
        }
    }

    impl dbflux_core::PreparedTableAlter for FakePreparedTableAlter {
        fn preview(&self) -> &dbflux_core::TableAlterPreview {
            &self.preview
        }

        fn execute(self: Box<Self>) -> Result<dbflux_core::TableAlterOutcome, DbError> {
            self.calls
                .lock()
                .unwrap()
                .push("MANAGED EXECUTE".to_string());
            Ok(dbflux_core::TableAlterOutcome {
                statement_count: 1,
                table_atomic: true,
            })
        }
    }

    impl FakeConnection {
        fn new(kind: DbKind, transactional_ddl: bool) -> Arc<Self> {
            Self::build(kind, transactional_ddl, None, false, true, false)
        }

        fn with_managed_planner(kind: DbKind, transactional_ddl: bool) -> Arc<Self> {
            Self::build(kind, transactional_ddl, None, false, true, true)
        }

        fn with_failure(kind: DbKind, transactional_ddl: bool, fail_on: &'static str) -> Arc<Self> {
            Self::build(kind, transactional_ddl, Some(fail_on), false, true, false)
        }

        /// A connection whose `fail_on` statement fails AND whose subsequent
        /// ROLLBACK also fails, to exercise the `RollbackFailed` path.
        fn with_failing_rollback(
            kind: DbKind,
            transactional_ddl: bool,
            fail_on: &'static str,
        ) -> Arc<Self> {
            Self::build(kind, transactional_ddl, Some(fail_on), true, true, false)
        }

        fn without_table_ddl_support(kind: DbKind, transactional_ddl: bool) -> Arc<Self> {
            Self::build(kind, transactional_ddl, None, false, false, false)
        }

        fn build(
            kind: DbKind,
            transactional_ddl: bool,
            fail_on_sql_containing: Option<&'static str>,
            fail_rollback: bool,
            supports_table_ddl: bool,
            has_managed_planner: bool,
        ) -> Arc<Self> {
            let meta = DriverMetadataBuilder::new(
                "test",
                "Test",
                DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .capabilities(DriverCapabilities::TRANSACTIONS)
            .build();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let generation_calls = Arc::new(Mutex::new(0));
            Arc::new(Self {
                db_kind: kind,
                meta,
                transactional_ddl,
                code_generator: RecordingCodeGenerator {
                    generate_add_calls: Arc::clone(&generation_calls),
                },
                generation_calls,
                calls: Arc::clone(&calls),
                managed_planner: has_managed_planner.then(|| FakeTableAlterPlanner { calls }),
                fail_on_sql_containing,
                fail_rollback,
                supports_table_ddl,
                creation_metadata_arg: Arc::new(Mutex::new(Vec::new())),
                generate_code_calls: Arc::new(Mutex::new(Vec::new())),
            })
        }

        fn recorded_calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        fn generated_add_count(&self) -> usize {
            *self.generation_calls.lock().unwrap()
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.meta
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn table_alter_planner(&self) -> Option<&dyn dbflux_core::TableAlterPlanner> {
            self.managed_planner
                .as_ref()
                .map(|planner| planner as &dyn dbflux_core::TableAlterPlanner)
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
            self.calls.lock().unwrap().push(req.sql.clone());

            if self.fail_rollback && req.sql.contains("ROLLBACK") {
                return Err(DbError::QueryFailed(FormattedError::new(
                    "simulated ROLLBACK failure",
                )));
            }

            if let Some(needle) = self.fail_on_sql_containing
                && req.sql.contains(needle)
            {
                return Err(DbError::QueryFailed(FormattedError::new(
                    "simulated failure",
                )));
            }

            Ok(QueryResult::empty())
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Err(DbError::NotSupported("stub".to_string()))
        }

        fn kind(&self) -> DbKind {
            self.db_kind
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &DefaultSqlDialect
        }

        fn code_generator(&self) -> &dyn CodeGenerator {
            &self.code_generator
        }

        fn supports_transactional_ddl(&self) -> bool {
            self.transactional_ddl
        }

        fn generate_code_with_creation_metadata(
            &self,
            generator_id: &str,
            table: &dbflux_core::TableInfo,
            creation_metadata: Option<&dbflux_core::TableCreationMetadata>,
        ) -> Result<String, DbError> {
            self.creation_metadata_arg
                .lock()
                .unwrap()
                .push(creation_metadata.cloned());
            self.generate_code(generator_id, table)
        }

        fn generate_code(
            &self,
            generator_id: &str,
            table: &dbflux_core::TableInfo,
        ) -> Result<String, DbError> {
            self.generate_code_calls
                .lock()
                .unwrap()
                .push(generator_id.to_string());
            if !self.supports_table_ddl {
                return Err(DbError::NotSupported(format!(
                    "Code generator '{}' not supported",
                    generator_id
                )));
            }
            match generator_id {
                "create_table" => Ok(format!("CREATE TABLE {} (id INT)", table.name)),
                "drop_table" => Ok(format!("DROP TABLE {}", table.name)),
                _ => Err(DbError::NotSupported(format!(
                    "Code generator '{}' not supported",
                    generator_id
                ))),
            }
        }
    }

    struct FakeEventSink {
        records: Mutex<Vec<EventRecord>>,
    }

    impl FakeEventSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                records: Mutex::new(Vec::new()),
            })
        }

        fn recorded(&self) -> Vec<EventRecord> {
            self.records.lock().unwrap().clone()
        }
    }

    impl EventSink for FakeEventSink {
        fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
            self.records.lock().unwrap().push(event.clone());
            Ok(event)
        }
    }

    fn users_table() -> TableRef {
        TableRef {
            schema: Some("public".to_string()),
            name: "users".to_string(),
        }
    }

    fn add_column_change(name: &str) -> RiskedChange {
        risked(
            SchemaChange::ColumnAdded(column(name, "text", true, None)),
            ExecutionClassification::AdminSafe,
        )
    }

    fn deps(connection: Arc<FakeConnection>, sink: Option<Arc<FakeEventSink>>) -> DdlApplyDeps {
        let driver_id = connection.metadata().id.clone();
        DdlApplyDeps {
            connection: connection as Arc<dyn Connection>,
            event_sink: sink.map(|s| s as Arc<dyn EventSink>),
            connection_id: "profile-test".to_string(),
            driver_id,
            policy: MutationPolicy::Allowed,
            read_only_reason: None,
        }
    }

    #[test]
    fn managed_planner_runs_an_alter_the_legacy_generator_rejects() {
        let conn = FakeConnection::with_managed_planner(DbKind::SQLite, true);
        let conn_ref = Arc::clone(&conn);
        let changes = vec![risked(
            SchemaChange::ColumnTypeChanged {
                before: column("id", "integer", false, None),
                after: column("id", "bigint", false, None),
            },
            ExecutionClassification::Admin,
        )];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, None));

        let outcome = executor
            .apply()
            .expect("managed planner must handle the alter");

        assert_eq!(
            outcome,
            DdlApplyOutcome::Success {
                statements_executed: 1,
                atomic: true,
            }
        );
        assert_eq!(
            conn_ref.recorded_calls(),
            vec!["PREPARE users", "MANAGED EXECUTE"],
            "managed execution must not add an outer SQL transaction"
        );
    }

    #[test]
    fn managed_and_legacy_selection_rejects_before_prepare_or_execute() {
        let conn = FakeConnection::with_managed_planner(DbKind::SQLite, true);
        let conn_ref = Arc::clone(&conn);
        let changes = vec![
            risked(
                SchemaChange::ColumnTypeChanged {
                    before: column("id", "integer", false, None),
                    after: column("id", "bigint", false, None),
                },
                ExecutionClassification::Admin,
            ),
            add_column_change("email"),
        ];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, None));

        match executor.prepare() {
            Err(ExecutorError::Generation(error)) => assert!(error.contains("adding column")),
            _ => panic!("expected whole-table mixed-selection rejection"),
        }
        assert!(conn_ref.recorded_calls().is_empty());
    }

    #[test]
    fn planner_none_retains_legacy_rejection_for_an_alter() {
        let conn = FakeConnection::new(DbKind::SQLite, true);
        let conn_ref = Arc::clone(&conn);
        let changes = vec![risked(
            SchemaChange::ColumnTypeChanged {
                before: column("id", "integer", false, None),
                after: column("id", "bigint", false, None),
            },
            ExecutionClassification::Admin,
        )];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, None));

        assert!(matches!(
            executor.prepare(),
            Err(ExecutorError::Generation(_))
        ));
        assert!(conn_ref.recorded_calls().is_empty());
    }

    #[test]
    fn pure_legacy_work_uses_frozen_sql_even_when_a_planner_is_available() {
        let conn = FakeConnection::with_managed_planner(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let executor = DdlApplyExecutor::new(
            users_table(),
            vec![add_column_change("email")],
            deps(conn, None),
        );

        let frozen = executor.prepare().unwrap();
        assert_eq!(
            frozen.preview_statements(),
            vec!["ALTER TABLE users ADD COLUMN email text"]
        );
        assert!(conn_ref.recorded_calls().is_empty());
        assert_eq!(conn_ref.generated_add_count(), 1);

        let outcome = frozen.execute().unwrap();
        assert_eq!(
            conn_ref.generated_add_count(),
            1,
            "execution must use the confirmed frozen SQL rather than regenerate it"
        );
        assert_eq!(
            outcome,
            DdlApplyOutcome::Success {
                statements_executed: 1,
                atomic: true,
            }
        );
        assert_eq!(
            conn_ref.recorded_calls(),
            vec!["BEGIN", "ALTER TABLE users ADD COLUMN email text", "COMMIT"]
        );
    }

    #[test]
    fn all_tables_prepare_before_the_first_managed_execute() {
        let conn = FakeConnection::with_managed_planner(DbKind::SQLite, true);
        let conn_ref = Arc::clone(&conn);
        let changes = |_name: &str| {
            vec![risked(
                SchemaChange::ColumnTypeChanged {
                    before: column("id", "integer", false, None),
                    after: column("id", "bigint", false, None),
                },
                ExecutionClassification::Admin,
            )]
        };
        let orders = TableRef {
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        };
        let frozen = DdlApplyExecutor::prepare_all(vec![
            DdlApplyExecutor::new(
                users_table(),
                changes("users"),
                deps(Arc::clone(&conn), None),
            ),
            DdlApplyExecutor::new(orders, changes("orders"), deps(conn, None)),
        ])
        .unwrap();

        assert_eq!(
            conn_ref.recorded_calls(),
            vec!["PREPARE users", "PREPARE orders"]
        );
        let outcome = FrozenDdlApply::execute_all(frozen).unwrap();
        assert_eq!(
            outcome,
            FrozenApplyRunOutcome {
                statements_applied: 2,
                tables_applied: 2,
            }
        );
        assert_eq!(
            conn_ref.recorded_calls(),
            vec![
                "PREPARE users",
                "PREPARE orders",
                "MANAGED EXECUTE",
                "MANAGED EXECUTE",
            ]
        );
    }

    #[test]
    fn later_transactional_failure_keeps_prior_committed_counts_only() {
        let conn = FakeConnection::with_failure(DbKind::Postgres, true, "age");
        let conn_ref = Arc::clone(&conn);
        let frozen = DdlApplyExecutor::prepare_all(vec![
            DdlApplyExecutor::new(
                users_table(),
                vec![add_column_change("email")],
                deps(Arc::clone(&conn), None),
            ),
            DdlApplyExecutor::new(
                TableRef {
                    schema: Some("public".to_string()),
                    name: "orders".to_string(),
                },
                vec![add_column_change("age")],
                deps(conn, None),
            ),
        ])
        .unwrap();

        let failure = FrozenDdlApply::execute_all(frozen).unwrap_err();
        assert_eq!(failure.tables_applied, 1);
        assert_eq!(failure.statements_applied, 1);
        assert_eq!(failure.failed_table.name, "orders");
        assert!(matches!(*failure.error, ExecutorError::Transaction(_)));
        assert_eq!(
            conn_ref.recorded_calls(),
            vec![
                "BEGIN",
                "ALTER TABLE users ADD COLUMN email text",
                "COMMIT",
                "BEGIN",
                "ALTER TABLE orders ADD COLUMN age text",
                "ROLLBACK",
            ]
        );
    }

    #[test]
    fn later_non_atomic_failure_counts_only_known_autocommits() {
        let conn = FakeConnection::with_failure(DbKind::MySQL, false, "age");
        let frozen = DdlApplyExecutor::prepare_all(vec![
            DdlApplyExecutor::new(
                users_table(),
                vec![add_column_change("email")],
                deps(Arc::clone(&conn), None),
            ),
            DdlApplyExecutor::new(
                TableRef {
                    schema: Some("public".to_string()),
                    name: "orders".to_string(),
                },
                vec![add_column_change("email"), add_column_change("age")],
                deps(conn, None),
            ),
        ])
        .unwrap();

        let failure = FrozenDdlApply::execute_all(frozen).unwrap_err();
        assert_eq!(failure.tables_applied, 1);
        assert_eq!(failure.statements_applied, 2);
        assert_eq!(failure.failed_table.name, "orders");
        assert!(matches!(*failure.error, ExecutorError::Execution(_)));
    }

    // 3.6 — atomic success (BEGIN / DDL / DDL / COMMIT)
    #[test]
    fn atomic_apply_success_commits_all_statements() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let changes = vec![add_column_change("email"), add_column_change("age")];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, None));

        let outcome = executor.apply().unwrap();
        assert_eq!(
            outcome,
            DdlApplyOutcome::Success {
                statements_executed: 2,
                atomic: true,
            }
        );

        let calls = conn_ref.recorded_calls();
        assert_eq!(calls[0], "BEGIN");
        assert!(calls[1].contains("ADD COLUMN email"));
        assert!(calls[2].contains("ADD COLUMN age"));
        assert_eq!(calls[3], "COMMIT");
    }

    // 3.6 — rollback on mid-batch failure
    #[test]
    fn atomic_apply_rolls_back_on_mid_statement_failure() {
        let conn = FakeConnection::with_failure(DbKind::Postgres, true, "age");
        let conn_ref = Arc::clone(&conn);
        let changes = vec![add_column_change("email"), add_column_change("age")];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, None));

        let result = executor.apply();
        assert!(matches!(result, Err(ExecutorError::Transaction(_))));

        let calls = conn_ref.recorded_calls();
        assert_eq!(calls[0], "BEGIN");
        assert!(calls[1].contains("ADD COLUMN email"));
        assert!(calls[2].contains("ADD COLUMN age"));
        assert_eq!(
            calls[3], "ROLLBACK",
            "expected a ROLLBACK after the failing statement, got: {:?}",
            calls
        );
        assert!(
            !calls.contains(&"COMMIT".to_string()),
            "COMMIT must not run after a mid-batch failure: {:?}",
            calls
        );
    }

    // 3.6 — non-atomic fallback: driver without transactional DDL support
    #[test]
    fn non_atomic_apply_reports_partial_failure_without_rollback() {
        let conn = FakeConnection::with_failure(DbKind::MySQL, false, "age");
        let conn_ref = Arc::clone(&conn);
        let changes = vec![add_column_change("email"), add_column_change("age")];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, None));

        let outcome = executor.apply().unwrap();
        assert_eq!(
            outcome,
            DdlApplyOutcome::PartialFailure {
                statements_executed: 1,
                failed_at: 1,
                error: "simulated failure".to_string(),
            }
        );

        let calls = conn_ref.recorded_calls();
        assert!(calls[0].contains("ADD COLUMN email"));
        assert!(calls[1].contains("ADD COLUMN age"));
        assert!(
            !calls.iter().any(|c| c == "BEGIN" || c == "ROLLBACK"),
            "non-atomic mode must never wrap statements in a transaction: {:?}",
            calls
        );
    }

    #[test]
    fn non_atomic_apply_success_reports_atomic_false() {
        let conn = FakeConnection::new(DbKind::MySQL, false);
        let changes = vec![add_column_change("email")];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, None));

        let outcome = executor.apply().unwrap();
        assert_eq!(
            outcome,
            DdlApplyOutcome::Success {
                statements_executed: 1,
                atomic: false,
            }
        );
    }

    // 3.6 — approval-required defers instead of executing
    #[test]
    fn approval_required_defers_without_touching_connection() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let changes = vec![add_column_change("email")];
        let mut d = deps(conn, None);
        d.policy = MutationPolicy::ApprovalRequired;
        let executor = DdlApplyExecutor::new(users_table(), changes, d);

        let outcome = executor.apply().unwrap();
        assert_eq!(outcome, DdlApplyOutcome::Deferred);
        assert!(
            conn_ref.recorded_calls().is_empty(),
            "ApprovalRequired must not execute any SQL: {:?}",
            conn_ref.recorded_calls()
        );
    }

    #[test]
    fn restricted_policy_rejects_preparation_before_generator_or_planner() {
        for policy in [MutationPolicy::ReadOnly, MutationPolicy::ApprovalRequired] {
            let connection = FakeConnection::new(DbKind::Postgres, true);
            let mut restricted_deps = deps(Arc::clone(&connection), None);
            restricted_deps.policy = policy;
            let executor = DdlApplyExecutor::new(
                users_table(),
                vec![add_column_change("email")],
                restricted_deps,
            );

            assert!(executor.prepare().is_err());
            assert_eq!(connection.generated_add_count(), 0);
            assert!(connection.recorded_calls().is_empty());
        }
    }

    #[test]
    fn read_only_blocks_without_touching_connection() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let changes = vec![add_column_change("email")];
        let mut d = deps(conn, None);
        d.policy = MutationPolicy::ReadOnly;
        let executor = DdlApplyExecutor::new(users_table(), changes, d);

        let outcome = executor.apply().unwrap();
        assert!(matches!(outcome, DdlApplyOutcome::Blocked { .. }));
        assert!(conn_ref.recorded_calls().is_empty());
    }

    #[test]
    fn read_only_blocked_message_selects_profile_key_for_profile_setting() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let changes = vec![add_column_change("email")];
        let mut d = deps(conn, None);
        d.policy = MutationPolicy::ReadOnly;
        d.read_only_reason = Some(ReadOnlyReason::ProfileSetting);
        let executor = DdlApplyExecutor::new(users_table(), changes, d);

        let outcome = executor.apply().unwrap();
        assert_eq!(
            outcome,
            DdlApplyOutcome::Blocked {
                reason: dbflux_i18n::t!("document.schema_diff.apply.read_only_profile"),
            }
        );
    }

    #[test]
    fn read_only_blocked_message_selects_server_key_for_server_enforced() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let changes = vec![add_column_change("email")];
        let mut d = deps(conn, None);
        d.policy = MutationPolicy::ReadOnly;
        d.read_only_reason = Some(ReadOnlyReason::ServerEnforced);
        let executor = DdlApplyExecutor::new(users_table(), changes, d);

        let outcome = executor.apply().unwrap();
        assert_eq!(
            outcome,
            DdlApplyOutcome::Blocked {
                reason: dbflux_i18n::t!("document.schema_diff.apply.read_only_server"),
            }
        );
    }

    #[test]
    fn read_only_blocked_message_falls_back_to_generic_key_when_reason_missing() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let changes = vec![add_column_change("email")];
        let mut d = deps(conn, None);
        d.policy = MutationPolicy::ReadOnly;
        let executor = DdlApplyExecutor::new(users_table(), changes, d);

        let outcome = executor.apply().unwrap();
        assert_eq!(
            outcome,
            DdlApplyOutcome::Blocked {
                reason: dbflux_i18n::t!("document.schema_diff.apply.read_only"),
            }
        );
    }

    // 3.5 — audit: Pending -> Success with a shared correlation id
    #[test]
    fn executor_persists_query_audit_events_with_connection_and_driver_context() {
        let audit_path = dbflux_audit::temp_sqlite_path(&format!(
            "dbflux-schema-diff-apply-audit-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let audit_service = dbflux_audit::AuditService::new_sqlite(&audit_path)
            .expect("test-owned audit database should initialize");
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let mut executor_deps = deps(conn, None);
        executor_deps.event_sink = Some(Arc::new(audit_service.clone()));
        let executor = DdlApplyExecutor::new(
            users_table(),
            vec![add_column_change("email")],
            executor_deps,
        );

        executor.apply().expect("DDL apply should succeed");

        let events = audit_service
            .query_extended(&Default::default())
            .expect("executor audit events should be queryable");
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .any(|event| event.outcome.as_deref() == Some("pending"))
        );
        assert!(
            events
                .iter()
                .any(|event| event.outcome.as_deref() == Some("success"))
        );
        assert!(events.iter().all(|event| {
            event.connection_id.as_deref() == Some("profile-test")
                && event.driver_id.as_deref() == Some("test")
        }));
    }

    #[test]
    fn executor_persists_failure_context_and_error_details() {
        let audit_path = dbflux_audit::temp_sqlite_path(&format!(
            "dbflux-schema-diff-apply-audit-failure-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let audit_service = dbflux_audit::AuditService::new_sqlite(&audit_path)
            .expect("test-owned audit database should initialize");
        let conn = FakeConnection::with_failure(DbKind::Postgres, true, "email");
        let mut executor_deps = deps(conn, None);
        executor_deps.event_sink = Some(Arc::new(audit_service.clone()));
        let executor = DdlApplyExecutor::new(
            users_table(),
            vec![add_column_change("email")],
            executor_deps,
        );

        assert!(matches!(
            executor.apply(),
            Err(ExecutorError::Transaction(_))
        ));

        let events = audit_service
            .query_extended(&Default::default())
            .expect("executor failure audit events should be queryable");
        let failure = events
            .iter()
            .find(|event| event.outcome.as_deref() == Some("failure"))
            .expect("a failed execution should be persisted");
        assert_eq!(failure.connection_id.as_deref(), Some("profile-test"));
        assert_eq!(failure.driver_id.as_deref(), Some("test"));
        assert_eq!(failure.error_code.as_deref(), Some("ddl_apply_failed"));
        assert_eq!(failure.error_message.as_deref(), Some("simulated failure"));
    }

    #[test]
    fn blocked_or_deferred_execution_does_not_persist_a_query_event() {
        for policy in [MutationPolicy::ReadOnly, MutationPolicy::ApprovalRequired] {
            let audit_path = dbflux_audit::temp_sqlite_path(&format!(
                "dbflux-schema-diff-apply-audit-policy-{}.sqlite",
                uuid::Uuid::new_v4()
            ));
            let audit_service = dbflux_audit::AuditService::new_sqlite(&audit_path)
                .expect("test-owned audit database should initialize");
            let conn = FakeConnection::new(DbKind::Postgres, true);
            let mut executor_deps = deps(conn, None);
            executor_deps.policy = policy;
            executor_deps.event_sink = Some(Arc::new(audit_service.clone()));
            let executor = DdlApplyExecutor::new(
                users_table(),
                vec![add_column_change("email")],
                executor_deps,
            );

            assert!(matches!(
                executor.apply(),
                Ok(DdlApplyOutcome::Blocked { .. }) | Ok(DdlApplyOutcome::Deferred)
            ));
            assert!(
                audit_service
                    .query_extended(&Default::default())
                    .expect("policy outcome audit events should be queryable")
                    .is_empty(),
                "{policy:?} must not create an execution event"
            );
        }
    }

    // 3.5 — audit: Pending -> Success with a shared correlation id
    #[test]
    fn success_run_emits_pending_then_success_with_matching_correlation_id() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let sink = FakeEventSink::new();
        let sink_ref = Arc::clone(&sink);
        let changes = vec![add_column_change("email")];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, Some(sink)));

        executor.apply().unwrap();

        let events = sink_ref.recorded();
        let pending = events
            .iter()
            .find(|e| e.outcome == EventOutcome::Pending)
            .expect("expected a Pending event");
        let success = events
            .iter()
            .find(|e| e.outcome == EventOutcome::Success)
            .expect("expected a Success event");

        assert_eq!(pending.category, EventCategory::Query);
        assert_eq!(pending.correlation_id, success.correlation_id);
        assert!(pending.correlation_id.is_some());
    }

    // 3.5 — audit: Pending -> Failure on rollback
    #[test]
    fn atomic_failure_emits_failure_event() {
        let conn = FakeConnection::with_failure(DbKind::Postgres, true, "email");
        let sink = FakeEventSink::new();
        let sink_ref = Arc::clone(&sink);
        let changes = vec![add_column_change("email")];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, Some(sink)));

        let result = executor.apply();
        assert!(result.is_err());

        let events = sink_ref.recorded();
        assert!(
            events.iter().any(|e| e.outcome == EventOutcome::Failure),
            "expected a Failure event; got: {:?}",
            events.iter().map(|e| &e.outcome).collect::<Vec<_>>()
        );
    }

    // FIX-5 — a failed ROLLBACK is a distinct, observable outcome and must not
    // be reported as a clean "rolled back" abort.
    #[test]
    fn failed_rollback_yields_distinct_outcome_and_audit() {
        let conn = FakeConnection::with_failing_rollback(DbKind::Postgres, true, "age");
        let conn_ref = Arc::clone(&conn);
        let sink = FakeEventSink::new();
        let sink_ref = Arc::clone(&sink);
        let changes = vec![add_column_change("email"), add_column_change("age")];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, Some(sink)));

        let result = executor.apply();

        match result {
            Err(ExecutorError::RollbackFailed {
                error,
                rollback_error,
                ..
            }) => {
                assert!(
                    error.contains("simulated failure"),
                    "original error: {error}"
                );
                assert!(
                    rollback_error.contains("ROLLBACK"),
                    "rollback error: {rollback_error}"
                );
            }
            other => panic!("expected RollbackFailed, got {other:?}"),
        }

        // The ROLLBACK was attempted (and failed) — not silently skipped.
        assert!(
            conn_ref.recorded_calls().iter().any(|c| c == "ROLLBACK"),
            "expected a ROLLBACK attempt: {:?}",
            conn_ref.recorded_calls()
        );

        // The failure is captured in the audit trail.
        let events = sink_ref.recorded();
        let failure = events
            .iter()
            .find(|e| e.outcome == EventOutcome::Failure)
            .expect("expected a Failure audit event for the rollback failure");
        assert!(
            failure.summary.contains("ROLLBACK"),
            "audit summary should name the rollback failure: {:?}",
            failure.summary
        );
    }

    // 3.5 — Deferred/Blocked runs must not emit any audit event: the approval
    // flow (or the caller's read-only refusal) owns its own tracking.
    #[test]
    fn deferred_run_emits_no_audit_events() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let sink = FakeEventSink::new();
        let sink_ref = Arc::clone(&sink);
        let changes = vec![add_column_change("email")];
        let mut d = deps(conn, Some(sink));
        d.policy = MutationPolicy::ApprovalRequired;
        let executor = DdlApplyExecutor::new(users_table(), changes, d);

        executor.apply().unwrap();
        assert!(sink_ref.recorded().is_empty());
    }

    // Empty change list is a no-op success and must not touch the connection.
    #[test]
    fn empty_changes_is_a_no_op_success() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let executor = DdlApplyExecutor::new(users_table(), vec![], deps(conn, None));

        let outcome = executor.apply().unwrap();
        assert_eq!(
            outcome,
            DdlApplyOutcome::Success {
                statements_executed: 0,
                atomic: true,
            }
        );
        assert!(conn_ref.recorded_calls().is_empty());
    }

    // A rejected change fails generation before any statement executes.
    #[test]
    fn unsupported_change_fails_generation_before_touching_connection() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let changes = vec![risked(
            SchemaChange::ForeignKeyChanged,
            ExecutionClassification::Admin,
        )];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, None));

        let result = executor.apply();
        assert!(matches!(result, Err(ExecutorError::Generation(_))));
        assert!(conn_ref.recorded_calls().is_empty());
    }

    // -----------------------------------------------------------------
    // DdlApplyExecutor::with_table_action — whole-table CREATE/DROP apply
    // -----------------------------------------------------------------

    #[test]
    fn with_table_action_create_executes_create_table_transactionally() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let table_info = TableInfo {
            name: "orders".to_string(),
            schema: Some("public".to_string()),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: None,
        };
        let table = TableRef {
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        };
        let executor = DdlApplyExecutor::new(table, vec![], deps(conn, None))
            .with_table_action(TableLevelAction::Create(Box::new(table_info), None));

        let outcome = executor.apply().unwrap();
        assert_eq!(
            outcome,
            DdlApplyOutcome::Success {
                statements_executed: 1,
                atomic: true,
            }
        );

        let calls = conn_ref.recorded_calls();
        assert_eq!(calls[0], "BEGIN");
        assert!(calls[1].contains("CREATE TABLE orders"));
        assert_eq!(calls[2], "COMMIT");
    }

    #[test]
    fn with_table_action_drop_executes_drop_table_transactionally() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let table = TableRef {
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        };
        let executor = DdlApplyExecutor::new(table.clone(), vec![], deps(conn, None))
            .with_table_action(TableLevelAction::Drop(table));

        let outcome = executor.apply().unwrap();
        assert_eq!(
            outcome,
            DdlApplyOutcome::Success {
                statements_executed: 1,
                atomic: true,
            }
        );

        let calls = conn_ref.recorded_calls();
        assert!(calls[1].contains("DROP TABLE orders"));
    }

    #[test]
    fn table_action_unsupported_by_driver_fails_generation_before_touching_connection() {
        let conn = FakeConnection::without_table_ddl_support(DbKind::SqlServer, true);
        let conn_ref = Arc::clone(&conn);
        let table_info = TableInfo {
            name: "orders".to_string(),
            schema: None,
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: None,
        };
        let table = TableRef {
            schema: None,
            name: "orders".to_string(),
        };
        let executor = DdlApplyExecutor::new(table, vec![], deps(conn, None))
            .with_table_action(TableLevelAction::Create(Box::new(table_info), None));

        let result = executor.apply();
        assert!(matches!(result, Err(ExecutorError::Generation(_))));
        assert!(conn_ref.recorded_calls().is_empty());
    }
}
