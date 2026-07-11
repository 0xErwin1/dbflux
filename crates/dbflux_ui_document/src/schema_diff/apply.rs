use std::sync::Arc;

use dbflux_core::{
    AddColumnRequest, AlterColumnRequest, CodeGenerator, Connection, DdlRejection, DefaultSpec,
    DropColumnRequest, EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
    MutationPolicy, QueryRequest, RiskedChange, SchemaChange, TableRef, TransactionVocab,
};

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
    /// The run was cancelled before all statements executed. On the
    /// transactional path, any partial work was rolled back.
    Cancelled { statements_executed: usize },
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
    Transaction(String),
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Generation(msg) => write!(f, "DDL generation failed: {}", msg),
            Self::Transaction(msg) => write!(f, "transaction error: {}", msg),
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

/// Builds the full ordered statement list for `changes`, failing fast on the
/// first change the driver cannot express. Mirrors the MCP
/// `run_alter_transactional` template of building all SQL before any
/// statement executes, so a rejection never leaves a partially-applied change.
fn build_all_statements(
    table: &TableRef,
    changes: &[RiskedChange],
    code_generator: &dyn CodeGenerator,
) -> Result<Vec<String>, ExecutorError> {
    let mut statements = Vec::new();

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
pub struct DdlApplyDeps {
    pub connection: Arc<dyn Connection>,
    pub event_sink: Option<Arc<dyn EventSink>>,
    pub policy: MutationPolicy,
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
    deps: DdlApplyDeps,
}

impl DdlApplyExecutor {
    pub fn new(table: TableRef, changes: Vec<RiskedChange>, deps: DdlApplyDeps) -> Self {
        Self {
            table,
            changes,
            deps,
        }
    }

    /// Builds the full ordered DDL statement list without executing anything.
    ///
    /// This is the read-only feed for the preview surface: it runs the same
    /// `CodeGenerator` mapping the apply path uses, so the preview shows the
    /// exact statements that would run, but it never touches the connection and
    /// never mutates any database. A change the driver cannot express fails
    /// generation here exactly as it would on apply.
    pub fn preview_statements(&self) -> Result<Vec<String>, ExecutorError> {
        build_all_statements(
            &self.table,
            &self.changes,
            self.deps.connection.code_generator(),
        )
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
    pub fn apply(
        &self,
        cancel: &crate::task_runner::MutationCancelHandle,
    ) -> Result<DdlApplyOutcome, ExecutorError> {
        match self.deps.policy {
            MutationPolicy::ApprovalRequired => return Ok(DdlApplyOutcome::Deferred),
            MutationPolicy::ReadOnly => {
                return Ok(DdlApplyOutcome::Blocked {
                    reason: "This connection is read-only. Schema changes are not allowed."
                        .to_string(),
                });
            }
            MutationPolicy::Allowed => {}
        }

        let statements = build_all_statements(
            &self.table,
            &self.changes,
            self.deps.connection.code_generator(),
        )?;

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
            self.apply_transactional(&statements, &run_id, cancel)
        } else {
            self.apply_non_atomic(&statements, &run_id, cancel)
        }
    }

    /// Executes `statements` as BEGIN / DDL... / COMMIT, rolling back on the
    /// first failure. Mirrors MCP `run_alter_transactional`.
    fn apply_transactional(
        &self,
        statements: &[String],
        run_id: &str,
        cancel: &crate::task_runner::MutationCancelHandle,
    ) -> Result<DdlApplyOutcome, ExecutorError> {
        let vocab = TransactionVocab::for_kind(self.deps.connection.kind()).ok_or_else(|| {
            ExecutorError::Transaction("driver does not support SQL transactions".to_string())
        })?;

        if cancel.is_cancelled() {
            self.emit_cancelled_event(run_id, 0);
            return Ok(DdlApplyOutcome::Cancelled {
                statements_executed: 0,
            });
        }

        let begin_req = QueryRequest::new(vocab.begin);
        if let Err(e) = self.deps.connection.execute(&begin_req) {
            let err_msg = e.to_string();
            self.emit_failure_event(run_id, &err_msg);
            return Err(ExecutorError::Transaction(err_msg));
        }

        for (index, statement) in statements.iter().enumerate() {
            if cancel.is_cancelled() {
                self.rollback_best_effort(&vocab);
                self.emit_cancelled_event(run_id, index as u64);
                return Ok(DdlApplyOutcome::Cancelled {
                    statements_executed: index,
                });
            }

            let request = QueryRequest::new(statement.clone());
            if let Err(e) = self.deps.connection.execute(&request) {
                let err_msg = e.to_string();
                self.rollback_best_effort(&vocab);
                self.emit_failure_event(run_id, &err_msg);
                return Err(ExecutorError::Transaction(format!(
                    "DDL apply aborted and rolled back at statement {}: {}",
                    index, err_msg
                )));
            }
        }

        let commit_req = QueryRequest::new(vocab.commit);
        if let Err(e) = self.deps.connection.execute(&commit_req) {
            let err_msg = e.to_string();
            self.rollback_best_effort(&vocab);
            self.emit_failure_event(run_id, &err_msg);
            return Err(ExecutorError::Transaction(err_msg));
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
        cancel: &crate::task_runner::MutationCancelHandle,
    ) -> Result<DdlApplyOutcome, ExecutorError> {
        let mut executed = 0usize;

        for statement in statements {
            if cancel.is_cancelled() {
                self.emit_cancelled_event(run_id, executed as u64);
                return Ok(DdlApplyOutcome::Cancelled {
                    statements_executed: executed,
                });
            }

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

    fn rollback_best_effort(&self, vocab: &TransactionVocab) {
        let rollback_req = QueryRequest::new(vocab.rollback);
        if let Err(e) = self.deps.connection.execute(&rollback_req) {
            log::warn!("ROLLBACK failed during DDL apply error recovery: {}", e);
        }
    }

    fn emit_event(&self, event: EventRecord) {
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
        .with_correlation_id(run_id.to_string());
        self.emit_event(event);
    }

    fn emit_cancelled_event(&self, run_id: &str, statements_executed: u64) {
        let event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Cancelled,
        )
        .with_action("schema_diff.apply")
        .with_summary(format!(
            "DDL apply to {} cancelled after {} statement(s)",
            self.table.name, statements_executed
        ))
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

    fn no_cancel() -> crate::task_runner::MutationCancelHandle {
        crate::task_runner::MutationCancelHandle::new()
    }

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
    // DdlApplyExecutor — end-to-end apply() tests
    // -----------------------------------------------------------------

    struct RecordingCodeGenerator;

    impl CodeGenerator for RecordingCodeGenerator {
        fn capabilities(&self) -> dbflux_core::CodeGenCapabilities {
            dbflux_core::CodeGenCapabilities::ADD_COLUMN
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
    }

    struct FakeConnection {
        db_kind: DbKind,
        meta: dbflux_core::DriverMetadata,
        transactional_ddl: bool,
        code_generator: RecordingCodeGenerator,
        calls: Mutex<Vec<String>>,
        fail_on_sql_containing: Option<&'static str>,
    }

    impl FakeConnection {
        fn new(kind: DbKind, transactional_ddl: bool) -> Arc<Self> {
            Self::build(kind, transactional_ddl, None)
        }

        fn with_failure(kind: DbKind, transactional_ddl: bool, fail_on: &'static str) -> Arc<Self> {
            Self::build(kind, transactional_ddl, Some(fail_on))
        }

        fn build(
            kind: DbKind,
            transactional_ddl: bool,
            fail_on_sql_containing: Option<&'static str>,
        ) -> Arc<Self> {
            let meta = DriverMetadataBuilder::new(
                "test",
                "Test",
                DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .capabilities(DriverCapabilities::TRANSACTIONS)
            .build();
            Arc::new(Self {
                db_kind: kind,
                meta,
                transactional_ddl,
                code_generator: RecordingCodeGenerator,
                calls: Mutex::new(Vec::new()),
                fail_on_sql_containing,
            })
        }

        fn recorded_calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl Connection for FakeConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.meta
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
            self.calls.lock().unwrap().push(req.sql.clone());

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
        DdlApplyDeps {
            connection: connection as Arc<dyn Connection>,
            event_sink: sink.map(|s| s as Arc<dyn EventSink>),
            policy: MutationPolicy::Allowed,
        }
    }

    // 3.6 — atomic success (BEGIN / DDL / DDL / COMMIT)
    #[test]
    fn atomic_apply_success_commits_all_statements() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let changes = vec![add_column_change("email"), add_column_change("age")];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, None));

        let outcome = executor.apply(&no_cancel()).unwrap();
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

        let result = executor.apply(&no_cancel());
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

        let outcome = executor.apply(&no_cancel()).unwrap();
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

        let outcome = executor.apply(&no_cancel()).unwrap();
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

        let outcome = executor.apply(&no_cancel()).unwrap();
        assert_eq!(outcome, DdlApplyOutcome::Deferred);
        assert!(
            conn_ref.recorded_calls().is_empty(),
            "ApprovalRequired must not execute any SQL: {:?}",
            conn_ref.recorded_calls()
        );
    }

    #[test]
    fn read_only_blocks_without_touching_connection() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let changes = vec![add_column_change("email")];
        let mut d = deps(conn, None);
        d.policy = MutationPolicy::ReadOnly;
        let executor = DdlApplyExecutor::new(users_table(), changes, d);

        let outcome = executor.apply(&no_cancel()).unwrap();
        assert!(matches!(outcome, DdlApplyOutcome::Blocked { .. }));
        assert!(conn_ref.recorded_calls().is_empty());
    }

    // 3.5 — audit: Pending -> Success with a shared correlation id
    #[test]
    fn success_run_emits_pending_then_success_with_matching_correlation_id() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let sink = FakeEventSink::new();
        let sink_ref = Arc::clone(&sink);
        let changes = vec![add_column_change("email")];
        let executor = DdlApplyExecutor::new(users_table(), changes, deps(conn, Some(sink)));

        executor.apply(&no_cancel()).unwrap();

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

        let result = executor.apply(&no_cancel());
        assert!(result.is_err());

        let events = sink_ref.recorded();
        assert!(
            events.iter().any(|e| e.outcome == EventOutcome::Failure),
            "expected a Failure event; got: {:?}",
            events.iter().map(|e| &e.outcome).collect::<Vec<_>>()
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

        executor.apply(&no_cancel()).unwrap();
        assert!(sink_ref.recorded().is_empty());
    }

    // Empty change list is a no-op success and must not touch the connection.
    #[test]
    fn empty_changes_is_a_no_op_success() {
        let conn = FakeConnection::new(DbKind::Postgres, true);
        let conn_ref = Arc::clone(&conn);
        let executor = DdlApplyExecutor::new(users_table(), vec![], deps(conn, None));

        let outcome = executor.apply(&no_cancel()).unwrap();
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

        let result = executor.apply(&no_cancel());
        assert!(matches!(result, Err(ExecutorError::Generation(_))));
        assert!(conn_ref.recorded_calls().is_empty());
    }
}
