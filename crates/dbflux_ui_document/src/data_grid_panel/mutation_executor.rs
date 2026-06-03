use std::sync::Arc;
use std::time::Duration;

use dbflux_core::{
    Connection, DbError, DriverCapabilities, EventCategory, EventOutcome, EventRecord,
    EventSeverity, EventSink, MutationKind, MutationPolicy, PlaceholderStyle, QueryRequest, Row,
    TransactionVocab, Value, VisualMutationSpec,
};

/// Execution modes for visual bulk mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Wrap the entire mutation in a single BEGIN/COMMIT. Safe for small row counts.
    SingleTransaction,
    /// Break the mutation into PK-keyset chunks, each with its own BEGIN/COMMIT.
    /// Requires at least one PK column.
    ChunkedTransaction,
    /// Execute without any transaction wrapper (autocommit). Used when the driver
    /// does not support transactions.
    DirectAutocommit,
}

/// Result of the execution-mode auto-selector.
///
/// Carries the suggested mode plus a human-readable reason string for the UI label.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct SuggestedMode {
    pub mode: ExecutionMode,
    pub reason: &'static str,
}

/// The estimated row count at the time mode selection runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum RowEstimate {
    /// The count query returned a definite result.
    Known(u64),
    /// The count could not be obtained (timeout or error); treat as worst-case.
    Unknown,
}

/// Pure function: given driver capabilities, PK availability, and estimated rows,
/// return the suggested execution mode.
///
/// The design (§13) threshold for chunked selection is 50,000 rows.
/// Selection order:
/// 1. No TRANSACTIONS capability → DirectAutocommit.
/// 2. count Unknown AND PK available → ChunkedTransaction.
/// 3. count Unknown AND no PK → SingleTransaction.
/// 4. count > 50,000 AND PK available → ChunkedTransaction.
/// 5. count ≤ 50,000 AND TRANSACTIONS → SingleTransaction.
/// 6. Fallback → DirectAutocommit.
#[allow(dead_code)]
pub fn auto_suggest_mode(
    capabilities: DriverCapabilities,
    has_pk: bool,
    estimate: RowEstimate,
) -> SuggestedMode {
    const CHUNK_THRESHOLD: u64 = 50_000;

    if !capabilities.contains(DriverCapabilities::TRANSACTIONS) {
        return SuggestedMode {
            mode: ExecutionMode::DirectAutocommit,
            reason: "Driver does not support transactions",
        };
    }

    match estimate {
        RowEstimate::Unknown => {
            if has_pk {
                SuggestedMode {
                    mode: ExecutionMode::ChunkedTransaction,
                    reason: "Row count unknown — chunked mode chosen conservatively",
                }
            } else {
                SuggestedMode {
                    mode: ExecutionMode::SingleTransaction,
                    reason: "Row count unknown — single transaction (no PK for chunking)",
                }
            }
        }
        RowEstimate::Known(n) => {
            if n > CHUNK_THRESHOLD && has_pk {
                SuggestedMode {
                    mode: ExecutionMode::ChunkedTransaction,
                    reason: "Large row count — chunked mode recommended",
                }
            } else {
                SuggestedMode {
                    mode: ExecutionMode::SingleTransaction,
                    reason: "Row count within single-transaction threshold",
                }
            }
        }
    }
}

/// Reason why a count result is unknown.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum CountUnknownReason {
    TimedOut,
    Failed(String),
}

/// The result of the pre-execution count query.
#[derive(Debug, Clone, PartialEq)]
pub enum CountState {
    /// Still in progress.
    Counting,
    /// Completed with a definite count.
    Done(u64),
    /// Could not determine the count.
    Unknown { reason: CountUnknownReason },
}

/// Runs a count SQL query on the given connection with a maximum wait time.
///
/// Returns `CountState::Done(n)` if the query completes within `deadline`,
/// `CountState::Unknown { reason: TimedOut }` if it exceeds the deadline,
/// or `CountState::Unknown { reason: Failed(..) }` on a connection error.
///
/// The query is run on a detached thread so the deadline is enforced via
/// `std::sync::mpsc::Receiver::recv_timeout`.
#[allow(dead_code)]
pub fn count_with_deadline(
    connection: Arc<dyn Connection>,
    sql: String,
    params: Vec<Value>,
    deadline: Duration,
) -> CountState {
    let (tx, rx) = std::sync::mpsc::channel::<Result<u64, String>>();

    std::thread::spawn(move || {
        let mut request = QueryRequest::new(sql);
        request.params = params;

        let result = connection
            .execute(&request)
            .map(|qr| {
                qr.rows
                    .first()
                    .and_then(|row| row.first())
                    .and_then(|val| match val {
                        Value::Int(n) => Some(*n as u64),
                        Value::Float(f) => Some(*f as u64),
                        _ => None,
                    })
                    .unwrap_or(0)
            })
            .map_err(|e| e.to_string());

        let _ = tx.send(result);
    });

    match rx.recv_timeout(deadline) {
        Ok(Ok(count)) => CountState::Done(count),
        Ok(Err(msg)) => CountState::Unknown {
            reason: CountUnknownReason::Failed(msg),
        },
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => CountState::Unknown {
            reason: CountUnknownReason::TimedOut,
        },
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => CountState::Unknown {
            reason: CountUnknownReason::Failed("Count thread disconnected".to_string()),
        },
    }
}

/// Configuration for a mutation execution run.
///
/// `chunk_size` is clamped to [1000, 10_000] per spec DR-10.2.
/// `count_deadline_ms` is clamped to [500, 30_000] per design §10.
#[derive(Debug, Clone)]
pub struct MutationExecOptions {
    pub mode: ExecutionMode,
    pub chunk_size: u32,
    pub lock_timeout_ms: Option<u64>,
    pub count_deadline_ms: u64,
}

impl MutationExecOptions {
    const CHUNK_MIN: u32 = 1_000;
    const CHUNK_MAX: u32 = 10_000;
    const DEADLINE_MIN_MS: u64 = 500;
    const DEADLINE_MAX_MS: u64 = 30_000;
    const DEFAULT_COUNT_DEADLINE_MS: u64 = 3_000;

    pub fn new(
        mode: ExecutionMode,
        chunk_size: u32,
        lock_timeout_ms: Option<u64>,
        count_deadline_ms: u64,
    ) -> Self {
        Self {
            mode,
            chunk_size: chunk_size.clamp(Self::CHUNK_MIN, Self::CHUNK_MAX),
            lock_timeout_ms,
            count_deadline_ms: count_deadline_ms
                .clamp(Self::DEADLINE_MIN_MS, Self::DEADLINE_MAX_MS),
        }
    }

    pub fn single_transaction() -> Self {
        Self::new(
            ExecutionMode::SingleTransaction,
            5_000,
            None,
            Self::DEFAULT_COUNT_DEADLINE_MS,
        )
    }

    pub fn chunked(chunk_size: u32) -> Self {
        Self::new(
            ExecutionMode::ChunkedTransaction,
            chunk_size,
            None,
            Self::DEFAULT_COUNT_DEADLINE_MS,
        )
    }
}

// =============================================================================
// MutationExecutor
// =============================================================================

/// Outcome of a completed mutation execution.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum MutationOutcome {
    Success { rows_affected: u64 },
    Failed { error: String },
    Cancelled { rows_affected: u64 },
}

/// Error type for `MutationExecutor::run_single_tx`.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorError {
    Generation(String),
    Transaction(String),
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Generation(msg) => write!(f, "SQL generation failed: {}", msg),
            Self::Transaction(msg) => write!(f, "transaction error: {}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {}

/// Dependencies injected into `MutationExecutor`.
///
/// All fields are `Arc`-wrapped so the executor can be sent to a background thread.
/// The `QueryGenerator` is derived from `connection.query_generator()` at execution time;
/// no separate generator field is needed because the connection already owns one.
pub struct MutationDeps {
    pub connection: Arc<dyn Connection>,
    pub event_sink: Option<Arc<dyn EventSink>>,
    #[allow(dead_code)]
    pub policy: MutationPolicy,
}

/// Plain (non-GPUI) struct that executes a single visual mutation.
///
/// Constructed per run by `DataGridPanel::on_mutation_run_requested`.
/// Each execution method is synchronous and intended to run on a background thread.
pub struct MutationExecutor {
    spec: VisualMutationSpec,
    opts: MutationExecOptions,
    deps: MutationDeps,
}

impl MutationExecutor {
    pub fn new(spec: VisualMutationSpec, opts: MutationExecOptions, deps: MutationDeps) -> Self {
        Self { spec, opts, deps }
    }

    /// Execute the mutation as a single BEGIN / DML / COMMIT sequence.
    ///
    /// Emits a parent audit event with `Pending` at start, then finalizes with
    /// `Success`, `Failed`, or `Cancelled` depending on outcome.
    ///
    /// Returns the outcome after the transaction is committed or rolled back.
    pub fn run_single_tx(&self) -> Result<MutationOutcome, ExecutorError> {
        let generator = self.deps.connection.query_generator().ok_or_else(|| {
            ExecutorError::Generation("driver does not support SQL generation".to_string())
        })?;

        let kind = &self.spec.kind;

        let generated = match kind {
            dbflux_core::MutationKind::Update { .. } => generator
                .generate_update_from_spec(&self.spec)
                .map_err(|e| ExecutorError::Generation(e.to_string()))?,
            dbflux_core::MutationKind::Delete => generator
                .generate_delete_from_spec(&self.spec)
                .map_err(|e| ExecutorError::Generation(e.to_string()))?,
        };

        let vocab = TransactionVocab::for_kind(self.deps.connection.kind());

        let run_id = uuid::Uuid::new_v4().to_string();
        let table_name = self.spec.from.name.clone();
        let op_kind = match &self.spec.kind {
            dbflux_core::MutationKind::Update { .. } => "update",
            dbflux_core::MutationKind::Delete => "delete",
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let pending_event = EventRecord::new(
            now_ms,
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Pending,
        )
        .with_action("mutation.run")
        .with_summary(format!("{} {} (single transaction)", op_kind, table_name))
        .with_correlation_id(run_id.clone());

        self.emit_event(pending_event);

        let begin_req = QueryRequest::new(vocab.begin);
        if let Err(e) = self.deps.connection.execute(&begin_req) {
            let err_msg = e.to_string();
            self.emit_failure_event(now_ms, &run_id, op_kind, &table_name, &err_msg);
            return Err(ExecutorError::Transaction(err_msg));
        }

        let mut dml_req = QueryRequest::new(generated.sql.clone());
        dml_req.params = generated.params.clone();
        let dml_result = self.deps.connection.execute(&dml_req);

        match dml_result {
            Err(e) => {
                let err_msg = e.to_string();
                let rollback_req = QueryRequest::new(vocab.rollback);
                if let Err(rb_err) = self.deps.connection.execute(&rollback_req) {
                    log::warn!("ROLLBACK failed during error recovery: {}", rb_err);
                }
                self.emit_failure_event(now_ms, &run_id, op_kind, &table_name, &err_msg);
                Err(ExecutorError::Transaction(err_msg))
            }
            Ok(result) => {
                let rows_affected = result.affected_rows.unwrap_or(0);

                let commit_req = QueryRequest::new(vocab.commit);
                if let Err(e) = self.deps.connection.execute(&commit_req) {
                    let err_msg = e.to_string();
                    self.emit_failure_event(now_ms, &run_id, op_kind, &table_name, &err_msg);
                    return Err(ExecutorError::Transaction(err_msg));
                }

                let success_event = EventRecord::new(
                    now_ms,
                    EventSeverity::Info,
                    EventCategory::Query,
                    EventOutcome::Success,
                )
                .with_action("mutation.run")
                .with_summary(format!(
                    "{} {} completed ({} rows affected)",
                    op_kind, table_name, rows_affected
                ))
                .with_correlation_id(run_id);

                self.emit_event(success_event);

                Ok(MutationOutcome::Success { rows_affected })
            }
        }
    }

    /// Execute the mutation as a series of keyset-paginated chunks.
    ///
    /// Each chunk is executed as its own BEGIN / DML WHERE pk IN (batch) / COMMIT.
    /// Cancellation is checked between chunks; the current chunk always runs to completion.
    ///
    /// `pk_cols` are the primary key column names of the target table.
    /// `cancel` is checked between chunks — flip it to abort after the current chunk.
    pub fn run_chunked_tx(
        &self,
        pk_cols: &[&str],
        cancel: &crate::task_runner::MutationCancelHandle,
    ) -> Result<MutationOutcome, ExecutorError> {
        use dbflux_core::{DefaultSqlDialect, lower_keyset_predicate};

        let generator = self.deps.connection.query_generator().ok_or_else(|| {
            ExecutorError::Generation("driver does not support SQL generation".to_string())
        })?;

        let vocab = TransactionVocab::for_kind(self.deps.connection.kind());
        let table_name = self.spec.from.name.clone();
        let op_kind = match &self.spec.kind {
            MutationKind::Update { .. } => "update",
            MutationKind::Delete => "delete",
        };
        let chunk_size = self.opts.chunk_size as usize;

        let run_id = uuid::Uuid::new_v4().to_string();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let pending_event = EventRecord::new(
            now_ms,
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Pending,
        )
        .with_action("mutation.run")
        .with_summary(format!("{} {} (chunked transaction)", op_kind, table_name))
        .with_correlation_id(run_id.clone());

        self.emit_event(pending_event);

        let mut last_pk_values: Option<Vec<Value>> = None;
        let mut rows_affected_total: u64 = 0;
        let mut chunks_committed: u32 = 0;

        let dialect = self.deps.connection.dialect();

        loop {
            if cancel.is_cancelled() {
                let cancelled_event = EventRecord::new(
                    now_ms,
                    EventSeverity::Info,
                    EventCategory::Query,
                    EventOutcome::Cancelled,
                )
                .with_action("mutation.run")
                .with_summary(format!(
                    "{} {} cancelled after {} chunks ({} rows)",
                    op_kind, table_name, chunks_committed, rows_affected_total
                ))
                .with_correlation_id(run_id.clone());
                self.emit_event(cancelled_event);

                return Ok(MutationOutcome::Cancelled {
                    rows_affected: rows_affected_total,
                });
            }

            // Step 1: SELECT pk_cols WHERE filter AND keyset_pred ORDER BY pk LIMIT chunk_size
            let pk_col_refs: Vec<String> = pk_cols
                .iter()
                .map(|c| dialect.quote_identifier(c))
                .collect();
            let pk_select = pk_col_refs.join(", ");

            let mut select_params: Vec<Value> = Vec::new();
            let mut param_idx: usize = 1;

            let keyset_clause = last_pk_values.as_ref().map(|last| {
                let pk_strs: Vec<&str> = pk_cols.to_vec();
                lower_keyset_predicate(
                    &pk_strs,
                    last,
                    dialect,
                    &table_name,
                    &mut select_params,
                    &mut param_idx,
                )
            });

            let qualified_table =
                dialect.qualified_table(self.spec.from.schema.as_deref(), &table_name);

            let select_sql = match keyset_clause {
                None => format!(
                    "SELECT {} FROM {} ORDER BY {} LIMIT {}",
                    pk_select,
                    qualified_table,
                    pk_col_refs.join(", "),
                    chunk_size,
                ),
                Some(ks) => format!(
                    "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {}",
                    pk_select,
                    qualified_table,
                    ks,
                    pk_col_refs.join(", "),
                    chunk_size,
                ),
            };

            let mut select_req = QueryRequest::new(select_sql);
            select_req.params = select_params;

            let pk_rows = match self.deps.connection.execute(&select_req) {
                Ok(r) => r.rows,
                Err(e) => {
                    let err_msg = e.to_string();
                    self.emit_failure_event(now_ms, &run_id, op_kind, &table_name, &err_msg);
                    return Err(ExecutorError::Transaction(err_msg));
                }
            };

            if pk_rows.is_empty() {
                // No more rows — done.
                break;
            }

            // Track last PK for next iteration
            if let Some(last_row) = pk_rows.last() {
                last_pk_values = Some(last_row.clone());
            }

            // Step 2: Build IN clause from fetched PKs (single-column PK only for now)
            let pk_placeholders: Vec<String> = (0..pk_rows.len())
                .map(|i| {
                    let ph_idx = param_idx + i;
                    match dialect.placeholder_style() {
                        PlaceholderStyle::DollarNumber => {
                            format!("${}", ph_idx)
                        }
                        PlaceholderStyle::AtSign => {
                            format!("@p{}", ph_idx)
                        }
                        _ => "?".to_string(),
                    }
                })
                .collect();

            let pk_values: Vec<Value> = pk_rows.iter().map(|r| r[0].clone()).collect();

            // Build the DML for this chunk
            let generated = match &self.spec.kind {
                MutationKind::Update { .. } => generator
                    .generate_update_from_spec(&self.spec)
                    .map_err(|e| ExecutorError::Generation(e.to_string()))?,
                MutationKind::Delete => generator
                    .generate_delete_from_spec(&self.spec)
                    .map_err(|e| ExecutorError::Generation(e.to_string()))?,
            };

            // Append WHERE pk IN (...) to the generated DML
            let pk_quoted = dialect.quote_identifier(pk_cols[0]);
            let chunk_sql = format!(
                "{} WHERE {} IN ({})",
                generated.sql,
                pk_quoted,
                pk_placeholders.join(", ")
            );

            let mut chunk_params = generated.params.clone();
            chunk_params.extend(pk_values);

            // Step 3: Execute BEGIN / DML / COMMIT for this chunk
            let begin_req = QueryRequest::new(vocab.begin);
            if let Err(e) = self.deps.connection.execute(&begin_req) {
                let err_msg = e.to_string();
                self.emit_failure_event(now_ms, &run_id, op_kind, &table_name, &err_msg);
                return Err(ExecutorError::Transaction(err_msg));
            }

            let mut dml_req = QueryRequest::new(chunk_sql);
            dml_req.params = chunk_params;
            let dml_result = self.deps.connection.execute(&dml_req);

            match dml_result {
                Err(e) => {
                    let err_msg = e.to_string();
                    let rollback_req = QueryRequest::new(vocab.rollback);
                    if let Err(rb_err) = self.deps.connection.execute(&rollback_req) {
                        log::warn!("ROLLBACK failed during chunk error recovery: {}", rb_err);
                    }

                    let chunk_event = EventRecord::new(
                        now_ms,
                        EventSeverity::Error,
                        EventCategory::Query,
                        EventOutcome::Failure,
                    )
                    .with_action("mutation.chunk")
                    .with_summary(format!(
                        "chunk {} failed: {}",
                        chunks_committed + 1,
                        err_msg
                    ))
                    .with_correlation_id(run_id.clone());
                    self.emit_event(chunk_event);

                    self.emit_failure_event(now_ms, &run_id, op_kind, &table_name, &err_msg);
                    return Err(ExecutorError::Transaction(err_msg));
                }
                Ok(result) => {
                    let chunk_rows = result.affected_rows.unwrap_or(pk_rows.len() as u64);

                    let commit_req = QueryRequest::new(vocab.commit);
                    if let Err(e) = self.deps.connection.execute(&commit_req) {
                        let err_msg = e.to_string();
                        self.emit_failure_event(now_ms, &run_id, op_kind, &table_name, &err_msg);
                        return Err(ExecutorError::Transaction(err_msg));
                    }

                    chunks_committed += 1;
                    rows_affected_total += chunk_rows;

                    let chunk_event = EventRecord::new(
                        now_ms,
                        EventSeverity::Info,
                        EventCategory::Query,
                        EventOutcome::Success,
                    )
                    .with_action("mutation.chunk")
                    .with_summary(format!(
                        "chunk {} completed ({} rows)",
                        chunks_committed, chunk_rows
                    ))
                    .with_correlation_id(run_id.clone());
                    self.emit_event(chunk_event);
                }
            }

            // Loop terminates when fewer than chunk_size rows returned (last page)
            if pk_rows.len() < chunk_size {
                break;
            }
        }

        let success_event = EventRecord::new(
            now_ms,
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Success,
        )
        .with_action("mutation.run")
        .with_summary(format!(
            "{} {} completed ({} rows in {} chunks)",
            op_kind, table_name, rows_affected_total, chunks_committed
        ))
        .with_correlation_id(run_id);
        self.emit_event(success_event);

        Ok(MutationOutcome::Success {
            rows_affected: rows_affected_total,
        })
    }

    fn emit_event(&self, event: EventRecord) {
        if let Some(sink) = &self.deps.event_sink
            && let Err(e) = sink.record(event)
        {
            log::warn!("mutation audit event failed: {e}");
        }
    }

    fn emit_failure_event(
        &self,
        now_ms: i64,
        run_id: &str,
        op_kind: &str,
        table_name: &str,
        error: &str,
    ) {
        let event = EventRecord::new(
            now_ms,
            EventSeverity::Error,
            EventCategory::Query,
            EventOutcome::Failure,
        )
        .with_action("mutation.run")
        .with_summary(format!("{} {} failed: {}", op_kind, table_name, error))
        .with_correlation_id(run_id.to_string());

        self.emit_event(event);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // T-27 — [RED] Tests for MutationExecutor single-tx happy path (G-1, G-2, G-6, DR-11.1–11.4)

    mod executor_tests {
        use super::*;
        use dbflux_core::{
            DatabaseCategory, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadataBuilder,
            EventRecord, EventSink, EventSinkError, GeneratedMutation, GeneratedQuery,
            MutationCategory, MutationKind, MutationPolicy, MutationRequest, QueryGenerator,
            QueryLanguage, QueryResult, SchemaLoadingStrategy, SchemaSnapshot, TableRef,
            VisualMutationSpec,
        };
        use std::sync::{Arc, Mutex};

        // -----------------------------------------------------------------
        // RecordingConnection — records every execute() call and its SQL
        // -----------------------------------------------------------------

        pub(super) struct RecordingConnection {
            meta: dbflux_core::DriverMetadata,
            calls: Mutex<Vec<String>>,
            dml_affected_rows: u64,
        }

        impl RecordingConnection {
            pub(super) fn new(kind: DbKind, dml_affected_rows: u64) -> Arc<Self> {
                let meta = DriverMetadataBuilder::new(
                    "test",
                    "Test",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .capabilities(DriverCapabilities::TRANSACTIONS)
                .build();
                Arc::new(Self {
                    meta,
                    calls: Mutex::new(Vec::new()),
                    dml_affected_rows,
                })
            }

            fn recorded_calls(&self) -> Vec<String> {
                self.calls.lock().unwrap().clone()
            }
        }

        impl dbflux_core::Connection for RecordingConnection {
            fn metadata(&self) -> &dbflux_core::DriverMetadata {
                &self.meta
            }

            fn ping(&self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn execute(
                &self,
                req: &dbflux_core::QueryRequest,
            ) -> Result<QueryResult, dbflux_core::DbError> {
                self.calls.lock().unwrap().push(req.sql.clone());
                let mut result = QueryResult::empty();
                // DML statements (not BEGIN/COMMIT/ROLLBACK) get affected_rows
                let sql_upper = req.sql.to_ascii_uppercase();
                if sql_upper.starts_with("UPDATE")
                    || sql_upper.starts_with("DELETE")
                    || sql_upper.starts_with("INSERT")
                {
                    result.affected_rows = Some(self.dml_affected_rows);
                }
                Ok(result)
            }

            fn cancel(
                &self,
                _handle: &dbflux_core::QueryHandle,
            ) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                Err(dbflux_core::DbError::NotSupported("stub".to_string()))
            }

            fn kind(&self) -> DbKind {
                DbKind::Postgres
            }

            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }

            fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                &DefaultSqlDialect
            }

            fn query_generator(&self) -> Option<&dyn QueryGenerator> {
                static GENERATOR: SimpleDeleteGenerator = SimpleDeleteGenerator;
                Some(&GENERATOR)
            }
        }

        // -----------------------------------------------------------------
        // FakeEventSink — records all EventRecords in order
        // -----------------------------------------------------------------

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
                let mut records = self.records.lock().unwrap();
                records.push(event.clone());
                Ok(event)
            }
        }

        // -----------------------------------------------------------------
        // SimpleDeleteGenerator — generates a fixed DELETE SQL
        // -----------------------------------------------------------------

        pub(super) struct SimpleDeleteGenerator;

        impl QueryGenerator for SimpleDeleteGenerator {
            fn supported_categories(&self) -> &'static [MutationCategory] {
                &[]
            }

            fn generate_mutation(&self, _: &MutationRequest) -> Option<GeneratedQuery> {
                None
            }

            fn generate_delete_from_spec(
                &self,
                spec: &VisualMutationSpec,
            ) -> Result<GeneratedMutation, dbflux_core::GeneratorError> {
                Ok(GeneratedMutation {
                    sql: format!("DELETE FROM {}", spec.from.name),
                    params: vec![],
                    used_raw_expression: false,
                })
            }

            fn generate_update_from_spec(
                &self,
                spec: &VisualMutationSpec,
            ) -> Result<GeneratedMutation, dbflux_core::GeneratorError> {
                Ok(GeneratedMutation {
                    sql: format!("UPDATE {} SET col = $1", spec.from.name),
                    params: vec![dbflux_core::Value::Int(1)],
                    used_raw_expression: false,
                })
            }
        }

        pub(super) fn make_delete_spec(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: TableRef {
                    schema: None,
                    name: table.to_string(),
                },
                filter: None,
                kind: MutationKind::Delete,
            }
        }

        fn make_update_spec(table: &str) -> VisualMutationSpec {
            use dbflux_core::{Assignment, AssignmentValue, ScalarLiteral};
            VisualMutationSpec {
                from: TableRef {
                    schema: None,
                    name: table.to_string(),
                },
                filter: None,
                kind: MutationKind::Update {
                    assignments: vec![Assignment {
                        column: "col".to_string(),
                        value: AssignmentValue::Literal(ScalarLiteral::Integer(1)),
                    }],
                },
            }
        }

        fn make_deps(
            conn: Arc<RecordingConnection>,
            sink: Option<Arc<FakeEventSink>>,
        ) -> MutationDeps {
            let event_sink: Option<Arc<dyn EventSink>> = sink.map(|s| s as Arc<dyn EventSink>);
            MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink,
                policy: MutationPolicy::Allowed,
            }
        }

        // G-1: parent event emitted at run start with outcome Pending
        #[test]
        fn g1_parent_event_emitted_with_pending_at_start() {
            let conn = RecordingConnection::new(DbKind::Postgres, 5);
            let sink = FakeEventSink::new();
            let sink_ref = Arc::clone(&sink);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::single_transaction();
            let deps = make_deps(conn, Some(sink));
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx();
            assert!(result.is_ok(), "expected success, got: {:?}", result);

            let events = sink_ref.recorded();
            assert!(
                !events.is_empty(),
                "expected at least one event to be emitted"
            );

            let pending = events.iter().find(|e| {
                e.outcome == dbflux_core::EventOutcome::Pending && e.action == "mutation.run"
            });
            assert!(
                pending.is_some(),
                "expected a Pending mutation.run event; got actions: {:?}",
                events
                    .iter()
                    .map(|e| (&e.action, &e.outcome))
                    .collect::<Vec<_>>()
            );
        }

        // G-2: parent event finalized with Success after completion
        #[test]
        fn g2_parent_event_finalized_with_success() {
            let conn = RecordingConnection::new(DbKind::Postgres, 3);
            let sink = FakeEventSink::new();
            let sink_ref = Arc::clone(&sink);
            let spec = make_delete_spec("users");
            let opts = MutationExecOptions::single_transaction();
            let deps = make_deps(conn, Some(sink));
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx();
            assert!(matches!(
                result,
                Ok(MutationOutcome::Success { rows_affected: 3 })
            ));

            let events = sink_ref.recorded();
            let success_event = events.iter().find(|e| {
                e.outcome == dbflux_core::EventOutcome::Success && e.action == "mutation.run"
            });
            assert!(
                success_event.is_some(),
                "expected a Success mutation.run event; recorded events: {:?}",
                events
                    .iter()
                    .map(|e| (&e.action, &e.outcome))
                    .collect::<Vec<_>>()
            );
        }

        // G-6: audit events routed through FakeEventSink (not directly to SQLite)
        #[test]
        fn g6_events_routed_through_event_sink() {
            let conn = RecordingConnection::new(DbKind::Postgres, 1);
            let sink = FakeEventSink::new();
            let sink_ref = Arc::clone(&sink);
            let spec = make_delete_spec("accounts");
            let opts = MutationExecOptions::single_transaction();
            let deps = make_deps(conn, Some(sink));
            let executor = MutationExecutor::new(spec, opts, deps);

            let _ = executor.run_single_tx();

            // All events must have been received by the fake sink — not empty.
            assert!(
                !sink_ref.recorded().is_empty(),
                "FakeEventSink must receive all events; got none"
            );

            // All received events should have the mutation.run action
            for event in sink_ref.recorded() {
                assert_eq!(
                    event.action, "mutation.run",
                    "unexpected event action: {}",
                    event.action
                );
            }
        }

        // DR-11.1: BEGIN + DML + COMMIT sequence is correct
        #[test]
        fn dr11_1_single_tx_sequence_is_begin_dml_commit() {
            let conn = RecordingConnection::new(DbKind::Postgres, 7);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("products");
            let opts = MutationExecOptions::single_transaction();
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            };
            let executor = MutationExecutor::new(spec, opts, deps);

            let _ = executor.run_single_tx();

            let calls = conn_ref.recorded_calls();
            assert_eq!(
                calls.len(),
                3,
                "expected 3 calls: BEGIN + DML + COMMIT; got {:?}",
                calls
            );
            assert_eq!(calls[0], "BEGIN", "first call must be BEGIN");
            assert!(
                calls[1].starts_with("DELETE FROM"),
                "second call must be DML, got: {}",
                calls[1]
            );
            assert_eq!(calls[2], "COMMIT", "third call must be COMMIT");
        }

        // DR-11.2: rows_affected reported in outcome
        #[test]
        fn dr11_2_rows_affected_reported_in_outcome() {
            let conn = RecordingConnection::new(DbKind::Postgres, 42);
            let spec = make_delete_spec("logs");
            let opts = MutationExecOptions::single_transaction();
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx().expect("expected success");
            assert_eq!(result, MutationOutcome::Success { rows_affected: 42 });
        }
    }

    // T-29 — [RED] Tests for chunked-tx execution loop (F-1, F-2, F-4, F-5, F-6, DR-10.1–10.9)

    mod chunked_executor_tests {
        use super::*;
        use dbflux_core::{
            DatabaseCategory, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadataBuilder,
            EventOutcome, EventRecord, EventSink, EventSinkError, GeneratedMutation,
            GeneratedQuery, MutationCategory, MutationKind, MutationPolicy, MutationRequest,
            QueryGenerator, QueryLanguage, QueryResult, SchemaLoadingStrategy, SchemaSnapshot,
            TableRef, Value, VisualMutationSpec,
        };
        use std::sync::{Arc, Mutex};

        /// A connection that serves pre-programmed responses.
        ///
        /// SELECT calls consume from `select_responses`; everything else returns empty.
        struct ProgrammedConnection {
            meta: dbflux_core::DriverMetadata,
            calls: Mutex<Vec<String>>,
            /// Responses returned for consecutive SELECT calls (consumed in order).
            select_responses: Mutex<Vec<Vec<Vec<Value>>>>,
            dml_affected_rows: u64,
        }

        impl ProgrammedConnection {
            fn new(select_responses: Vec<Vec<Vec<Value>>>, dml_affected_rows: u64) -> Arc<Self> {
                let meta = DriverMetadataBuilder::new(
                    "test",
                    "Test",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .capabilities(DriverCapabilities::TRANSACTIONS)
                .build();
                Arc::new(Self {
                    meta,
                    calls: Mutex::new(Vec::new()),
                    select_responses: Mutex::new(select_responses),
                    dml_affected_rows,
                })
            }

            fn recorded_calls(&self) -> Vec<String> {
                self.calls.lock().unwrap().clone()
            }
        }

        impl dbflux_core::Connection for ProgrammedConnection {
            fn metadata(&self) -> &dbflux_core::DriverMetadata {
                &self.meta
            }

            fn ping(&self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn execute(
                &self,
                req: &dbflux_core::QueryRequest,
            ) -> Result<QueryResult, dbflux_core::DbError> {
                self.calls.lock().unwrap().push(req.sql.clone());
                let mut result = QueryResult::empty();
                let sql_upper = req.sql.to_ascii_uppercase();

                if sql_upper.starts_with("SELECT") {
                    let mut responses = self.select_responses.lock().unwrap();
                    if !responses.is_empty() {
                        result.rows = responses.remove(0);
                    }
                } else if sql_upper.starts_with("UPDATE")
                    || sql_upper.starts_with("DELETE")
                    || sql_upper.starts_with("INSERT")
                {
                    result.affected_rows = Some(self.dml_affected_rows);
                }
                Ok(result)
            }

            fn cancel(
                &self,
                _handle: &dbflux_core::QueryHandle,
            ) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                Err(dbflux_core::DbError::NotSupported("stub".to_string()))
            }

            fn kind(&self) -> DbKind {
                DbKind::Postgres
            }

            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }

            fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                &DefaultSqlDialect
            }

            fn query_generator(&self) -> Option<&dyn QueryGenerator> {
                static GENERATOR: SimpleDeleteGenerator = SimpleDeleteGenerator;
                Some(&GENERATOR)
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

        struct SimpleDeleteGenerator;

        impl QueryGenerator for SimpleDeleteGenerator {
            fn supported_categories(&self) -> &'static [MutationCategory] {
                &[]
            }

            fn generate_mutation(&self, _: &MutationRequest) -> Option<GeneratedQuery> {
                None
            }

            fn generate_delete_from_spec(
                &self,
                spec: &VisualMutationSpec,
            ) -> Result<GeneratedMutation, dbflux_core::GeneratorError> {
                Ok(GeneratedMutation {
                    sql: format!("DELETE FROM {}", spec.from.name),
                    params: vec![],
                    used_raw_expression: false,
                })
            }
        }

        fn pk_row(id: i64) -> Vec<Value> {
            vec![Value::Int(id)]
        }

        /// Generate a batch of `count` pk rows starting from `start`.
        fn pk_batch(start: i64, count: usize) -> Vec<Vec<Value>> {
            (start..start + count as i64).map(pk_row).collect()
        }

        fn make_delete_spec(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: TableRef {
                    schema: None,
                    name: table.to_string(),
                },
                filter: None,
                kind: MutationKind::Delete,
            }
        }

        fn make_deps(
            conn: Arc<ProgrammedConnection>,
            sink: Option<Arc<FakeEventSink>>,
        ) -> MutationDeps {
            let event_sink: Option<Arc<dyn EventSink>> = sink.map(|s| s as Arc<dyn EventSink>);
            MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink,
                policy: MutationPolicy::Allowed,
            }
        }

        fn no_cancel() -> crate::task_runner::MutationCancelHandle {
            crate::task_runner::MutationCancelHandle::new()
        }

        // F-1: 3 chunks → 3 successful chunk events
        // Uses chunk_size = 1000 (minimum allowed per spec DR-10.2); 3 full batches + empty terminator.
        #[test]
        fn f1_three_chunks_emit_three_chunk_events() {
            let select_responses = vec![
                pk_batch(1, 1_000),
                pk_batch(1_001, 1_000),
                pk_batch(2_001, 1_000),
                vec![], // terminator
            ];
            let conn = ProgrammedConnection::new(select_responses, 1_000);
            let sink = FakeEventSink::new();
            let sink_ref = Arc::clone(&sink);
            let spec = make_delete_spec("orders");
            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, Some(sink));
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_chunked_tx(&["id"], &no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let events = sink_ref.recorded();
            let chunk_events: Vec<_> = events
                .iter()
                .filter(|e| e.action == "mutation.chunk")
                .collect();
            assert_eq!(
                chunk_events.len(),
                3,
                "expected 3 chunk events; got {} events total: {:?}",
                chunk_events.len(),
                events.iter().map(|e| &e.action).collect::<Vec<_>>()
            );
            for e in &chunk_events {
                assert_eq!(e.outcome, EventOutcome::Success);
            }
        }

        // F-2: cancellation between chunks — cancel before any chunk runs
        #[test]
        fn f2_cancel_between_chunks_stops_execution() {
            let select_responses = vec![pk_batch(1, 1_000), pk_batch(1_001, 1_000), vec![]];
            let conn = ProgrammedConnection::new(select_responses, 1_000);
            let cancel = crate::task_runner::MutationCancelHandle::new();
            // Cancel before loop starts — executor should return Cancelled immediately
            cancel.cancel();

            let spec = make_delete_spec("items");
            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_chunked_tx(&["id"], &cancel);
            assert!(
                matches!(result, Ok(MutationOutcome::Cancelled { .. })),
                "expected Cancelled, got: {:?}",
                result
            );
        }

        // F-4: chunk failure → ROLLBACK, halt
        #[test]
        fn f4_chunk_dml_failure_triggers_rollback_and_halts() {
            // SELECT returns one batch, but DML will fail
            // We need a connection where DML fails — use FailingDMLConnection
            struct FailingDMLConn {
                meta: dbflux_core::DriverMetadata,
                calls: Mutex<Vec<String>>,
            }

            impl FailingDMLConn {
                fn new() -> Arc<Self> {
                    let meta = DriverMetadataBuilder::new(
                        "test",
                        "Test",
                        DatabaseCategory::Relational,
                        QueryLanguage::Sql,
                    )
                    .capabilities(DriverCapabilities::TRANSACTIONS)
                    .build();
                    Arc::new(Self {
                        meta,
                        calls: Mutex::new(Vec::new()),
                    })
                }
            }

            impl dbflux_core::Connection for FailingDMLConn {
                fn metadata(&self) -> &dbflux_core::DriverMetadata {
                    &self.meta
                }
                fn ping(&self) -> Result<(), dbflux_core::DbError> {
                    Ok(())
                }
                fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                    Ok(())
                }
                fn execute(
                    &self,
                    req: &dbflux_core::QueryRequest,
                ) -> Result<QueryResult, dbflux_core::DbError> {
                    self.calls.lock().unwrap().push(req.sql.clone());
                    let sql_upper = req.sql.to_ascii_uppercase();
                    if sql_upper.starts_with("DELETE") || sql_upper.starts_with("UPDATE") {
                        return Err(dbflux_core::DbError::query_failed("simulated DML error"));
                    }
                    if sql_upper.starts_with("SELECT") {
                        let mut result = QueryResult::empty();
                        result.rows = vec![pk_row(1)];
                        return Ok(result);
                    }
                    Ok(QueryResult::empty())
                }
                fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
                    Ok(())
                }
                fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                    Err(dbflux_core::DbError::NotSupported("stub".to_string()))
                }
                fn kind(&self) -> DbKind {
                    DbKind::Postgres
                }
                fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                    SchemaLoadingStrategy::SingleDatabase
                }
                fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                    &DefaultSqlDialect
                }
                fn query_generator(&self) -> Option<&dyn QueryGenerator> {
                    static GENERATOR: SimpleDeleteGenerator = SimpleDeleteGenerator;
                    Some(&GENERATOR)
                }
            }

            let conn = FailingDMLConn::new();
            let conn_ref = Arc::clone(&conn);
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            };
            let spec = make_delete_spec("orders");
            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_chunked_tx(&["id"], &no_cancel());
            assert!(
                matches!(result, Err(ExecutorError::Transaction(_))),
                "expected Transaction error, got: {:?}",
                result
            );

            // ROLLBACK must have been issued
            let calls = conn_ref.calls.lock().unwrap().clone();
            assert!(
                calls.iter().any(|c| c.to_ascii_uppercase() == "ROLLBACK"),
                "expected ROLLBACK in calls: {:?}",
                calls
            );
        }

        // F-6: SELECT uses ORDER BY pk cols
        #[test]
        fn f6_select_chunk_uses_order_by_pk() {
            // Single partial batch (< chunk_size) → terminates after 1 select
            let select_responses = vec![
                pk_batch(1, 50), // 50 < 1_000 → loop terminates
            ];
            let conn = ProgrammedConnection::new(select_responses, 50);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_chunked_tx(&["id"], &no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();
            let first_select = calls
                .iter()
                .find(|c| c.to_ascii_uppercase().starts_with("SELECT"));
            assert!(
                first_select.is_some(),
                "expected at least one SELECT call; calls: {:?}",
                calls
            );
            assert!(
                first_select
                    .unwrap()
                    .to_ascii_uppercase()
                    .contains("ORDER BY"),
                "SELECT must contain ORDER BY; got: {}",
                first_select.unwrap()
            );
        }

        // T-31 / G-3: cancel → parent event finalized with Cancelled + cumulative rows
        #[test]
        fn g3_cancel_emits_cancelled_parent_event() {
            let select_responses = vec![pk_batch(1, 1_000), pk_batch(1_001, 1_000), vec![]];
            let conn = ProgrammedConnection::new(select_responses, 1_000);
            let sink = FakeEventSink::new();
            let sink_ref = Arc::clone(&sink);
            let cancel = crate::task_runner::MutationCancelHandle::new();
            cancel.cancel(); // Cancel immediately (before first chunk)

            let spec = make_delete_spec("accounts");
            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, Some(sink));
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_chunked_tx(&["id"], &cancel);
            assert!(
                matches!(result, Ok(MutationOutcome::Cancelled { .. })),
                "expected Cancelled, got: {:?}",
                result
            );

            let events = sink_ref.recorded();
            let cancelled_event = events
                .iter()
                .find(|e| e.outcome == EventOutcome::Cancelled && e.action == "mutation.run");
            assert!(
                cancelled_event.is_some(),
                "expected a Cancelled mutation.run event; events: {:?}",
                events
                    .iter()
                    .map(|e| (&e.action, &e.outcome))
                    .collect::<Vec<_>>()
            );
        }

        // T-31 / DR-11.3: each chunk emits a mutation.chunk event
        #[test]
        fn dr11_3_each_chunk_emits_chunk_event() {
            let select_responses = vec![
                pk_batch(1, 1_000),
                pk_batch(1_001, 50), // partial last page
            ];
            let conn = ProgrammedConnection::new(select_responses, 1_000);
            let sink = FakeEventSink::new();
            let sink_ref = Arc::clone(&sink);
            let spec = make_delete_spec("events");
            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, Some(sink));
            let executor = MutationExecutor::new(spec, opts, deps);

            let _ = executor.run_chunked_tx(&["id"], &no_cancel());

            let chunk_events: Vec<_> = sink_ref
                .recorded()
                .into_iter()
                .filter(|e| e.action == "mutation.chunk")
                .collect();
            assert_eq!(
                chunk_events.len(),
                2,
                "expected 2 mutation.chunk events for 2 chunks"
            );
        }

        // G-4: 2 chunks → 2 chunk events with Success outcome (T-31 preview)
        #[test]
        fn g4_two_chunks_emit_two_success_chunk_events() {
            let select_responses = vec![
                pk_batch(1, 1_000),  // full first chunk
                pk_batch(1_001, 50), // partial last page → termination
            ];
            let conn = ProgrammedConnection::new(select_responses, 1_000);
            let sink = FakeEventSink::new();
            let sink_ref = Arc::clone(&sink);
            let spec = make_delete_spec("logs");
            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, Some(sink));
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_chunked_tx(&["id"], &no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let chunk_events: Vec<_> = sink_ref
                .recorded()
                .into_iter()
                .filter(|e| e.action == "mutation.chunk")
                .collect();
            assert_eq!(chunk_events.len(), 2, "expected 2 chunk events");
            assert!(
                chunk_events
                    .iter()
                    .all(|e| e.outcome == EventOutcome::Success)
            );
        }
    }

    // T-23 — [RED] Tests for auto_suggest_mode (spec D-1 through D-6, DR-8.1–DR-8.6)

    fn caps_with_transactions() -> DriverCapabilities {
        DriverCapabilities::TRANSACTIONS
    }

    fn caps_no_transactions() -> DriverCapabilities {
        DriverCapabilities::empty()
    }

    // D-1: No TRANSACTIONS capability → DirectAutocommit
    #[test]
    fn d1_no_transactions_suggests_direct() {
        let result = auto_suggest_mode(caps_no_transactions(), true, RowEstimate::Known(500));
        assert_eq!(result.mode, ExecutionMode::DirectAutocommit);
    }

    // D-2: count unknown + PK available → ChunkedTransaction
    #[test]
    fn d2_count_unknown_with_pk_suggests_chunked() {
        let result = auto_suggest_mode(caps_with_transactions(), true, RowEstimate::Unknown);
        assert_eq!(result.mode, ExecutionMode::ChunkedTransaction);
    }

    // D-3: count unknown, no PK → SingleTransaction
    #[test]
    fn d3_count_unknown_no_pk_suggests_single() {
        let result = auto_suggest_mode(caps_with_transactions(), false, RowEstimate::Unknown);
        assert_eq!(result.mode, ExecutionMode::SingleTransaction);
    }

    // D-4: count > 50k (design §13 threshold), PK available → ChunkedTransaction
    #[test]
    fn d4_large_count_with_pk_suggests_chunked() {
        let result = auto_suggest_mode(caps_with_transactions(), true, RowEstimate::Known(50_001));
        assert_eq!(result.mode, ExecutionMode::ChunkedTransaction);
    }

    // D-5: count ≤ 50k, TRANSACTIONS present → SingleTransaction
    #[test]
    fn d5_small_count_suggests_single() {
        let result = auto_suggest_mode(caps_with_transactions(), true, RowEstimate::Known(200));
        assert_eq!(result.mode, ExecutionMode::SingleTransaction);
    }

    // D-6: Large count without PK → SingleTransaction (ChunkedTx not eligible)
    #[test]
    fn d6_large_count_no_pk_suggests_single_not_chunked() {
        let result =
            auto_suggest_mode(caps_with_transactions(), false, RowEstimate::Known(100_000));
        assert_ne!(
            result.mode,
            ExecutionMode::ChunkedTransaction,
            "ChunkedTransaction not eligible without PK"
        );
        assert_eq!(result.mode, ExecutionMode::SingleTransaction);
    }

    // Exactly at threshold boundary (50,000) → SingleTransaction
    #[test]
    fn at_threshold_boundary_suggests_single() {
        let result = auto_suggest_mode(caps_with_transactions(), true, RowEstimate::Known(50_000));
        assert_eq!(result.mode, ExecutionMode::SingleTransaction);
    }

    // SuggestedMode has a non-empty reason string
    #[test]
    fn suggested_mode_has_non_empty_reason() {
        let result = auto_suggest_mode(caps_with_transactions(), true, RowEstimate::Known(100));
        assert!(!result.reason.is_empty());
    }

    // T-25 — [RED] Tests for count_with_deadline (spec DR-6.1–DR-6.5)

    /// Minimal Connection stub that returns a fixed count after sleeping for a given duration.
    mod count_tests {
        use super::*;
        use dbflux_core::{
            DatabaseCategory, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadataBuilder,
            QueryHandle, QueryLanguage, QueryResult, SchemaLoadingStrategy, SchemaSnapshot,
        };
        use std::time::Duration;

        struct CountReturningConnection {
            sleep_ms: u64,
            result: Result<u64, String>,
        }

        impl CountReturningConnection {
            fn succeeds_fast(count: u64) -> Arc<Self> {
                Arc::new(Self {
                    sleep_ms: 0,
                    result: Ok(count),
                })
            }

            fn slow(sleep_ms: u64, count: u64) -> Arc<Self> {
                Arc::new(Self {
                    sleep_ms,
                    result: Ok(count),
                })
            }

            fn fails_fast(msg: impl Into<String>) -> Arc<Self> {
                Arc::new(Self {
                    sleep_ms: 0,
                    result: Err(msg.into()),
                })
            }
        }

        static FAKE_META: std::sync::OnceLock<dbflux_core::DriverMetadata> =
            std::sync::OnceLock::new();

        fn fake_meta() -> &'static dbflux_core::DriverMetadata {
            FAKE_META.get_or_init(|| {
                DriverMetadataBuilder::new(
                    "fake",
                    "Fake",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .build()
            })
        }

        impl dbflux_core::Connection for CountReturningConnection {
            fn metadata(&self) -> &dbflux_core::DriverMetadata {
                fake_meta()
            }

            fn ping(&self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn execute(
                &self,
                _req: &dbflux_core::QueryRequest,
            ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
                if self.sleep_ms > 0 {
                    std::thread::sleep(Duration::from_millis(self.sleep_ms));
                }
                match &self.result {
                    Ok(count) => {
                        use dbflux_core::Value;
                        let row: Vec<Value> = vec![Value::Int(*count as i64)];
                        let mut result = QueryResult::empty();
                        result.rows = vec![row];
                        Ok(result)
                    }
                    Err(msg) => Err(dbflux_core::DbError::query_failed(msg.clone())),
                }
            }

            fn cancel(&self, _handle: &QueryHandle) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                Err(dbflux_core::DbError::NotSupported("stub".to_string()))
            }

            fn kind(&self) -> DbKind {
                DbKind::SQLite
            }

            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }

            fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                &DefaultSqlDialect
            }
        }

        // DR-6.1: Returns Done(n) when the connection returns within deadline.
        #[test]
        fn dr6_1_count_returns_done_when_fast() {
            let conn = CountReturningConnection::succeeds_fast(42);
            let result = count_with_deadline(
                conn,
                "SELECT COUNT(*) FROM t".to_string(),
                vec![],
                Duration::from_millis(1_000),
            );
            assert_eq!(result, CountState::Done(42));
        }

        // DR-6.2: Returns Unknown { TimedOut } when connection exceeds the deadline.
        #[test]
        fn dr6_2_count_returns_timed_out_when_slow() {
            // sleeps 200ms, deadline 50ms → timeout
            let conn = CountReturningConnection::slow(200, 999);
            let result = count_with_deadline(
                conn,
                "SELECT COUNT(*) FROM t".to_string(),
                vec![],
                Duration::from_millis(50),
            );
            assert_eq!(
                result,
                CountState::Unknown {
                    reason: CountUnknownReason::TimedOut
                }
            );
        }

        // DR-6.3: Returns Unknown { Failed } when the connection returns an error.
        #[test]
        fn dr6_3_count_returns_failed_on_error() {
            let conn = CountReturningConnection::fails_fast("access denied");
            let result = count_with_deadline(
                conn,
                "SELECT COUNT(*) FROM t".to_string(),
                vec![],
                Duration::from_millis(1_000),
            );
            assert!(
                matches!(
                    result,
                    CountState::Unknown {
                        reason: CountUnknownReason::Failed(_)
                    }
                ),
                "expected Failed variant, got: {:?}",
                result
            );
        }

        // DR-6.4: A zero count is reported as Done(0).
        #[test]
        fn dr6_4_count_returns_done_zero() {
            let conn = CountReturningConnection::succeeds_fast(0);
            let result = count_with_deadline(
                conn,
                "SELECT COUNT(*) FROM t".to_string(),
                vec![],
                Duration::from_millis(1_000),
            );
            assert_eq!(result, CountState::Done(0));
        }

        // DR-6.5: Deadline is tight (just barely passes).
        #[test]
        fn dr6_5_count_within_generous_deadline_succeeds() {
            let conn = CountReturningConnection::slow(10, 7);
            let result = count_with_deadline(
                conn,
                "SELECT COUNT(*) FROM t".to_string(),
                vec![],
                Duration::from_millis(500),
            );
            assert_eq!(result, CountState::Done(7));
        }
    }

    // T-37 — [RED] Tests for MutationExecOptions bounds validation (F-5, DR-10.2)
    // Spec DR-10.2 mandates [1,000 – 10,000]. Delivery decision #6188 confirms spec wins.

    #[test]
    fn chunk_size_below_min_clamped_to_1k() {
        let opts = MutationExecOptions::new(ExecutionMode::SingleTransaction, 0, None, 3000);
        assert_eq!(opts.chunk_size, 1_000);
    }

    #[test]
    fn chunk_size_above_max_clamped_to_10k() {
        let opts = MutationExecOptions::new(ExecutionMode::SingleTransaction, 200_000, None, 3000);
        assert_eq!(opts.chunk_size, 10_000);
    }

    #[test]
    fn count_deadline_below_min_clamped_to_500ms() {
        let opts = MutationExecOptions::new(ExecutionMode::SingleTransaction, 5_000, None, 100);
        assert_eq!(opts.count_deadline_ms, 500);
    }

    #[test]
    fn count_deadline_above_max_clamped_to_30s() {
        let opts = MutationExecOptions::new(ExecutionMode::SingleTransaction, 5_000, None, 999_999);
        assert_eq!(opts.count_deadline_ms, 30_000);
    }

    // K-1/K-3 — Background mutation error and success paths (spec DR-16, Group K)

    mod k_tests {
        use super::executor_tests::{SimpleDeleteGenerator, make_delete_spec};
        use super::*;
        use dbflux_core::{
            DatabaseCategory, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadataBuilder,
            FormattedError, GeneratedMutation, GeneratedQuery, MutationCategory, MutationPolicy,
            MutationRequest, QueryGenerator, QueryLanguage, SchemaLoadingStrategy, SchemaSnapshot,
            TableRef, VisualMutationSpec,
        };
        use std::sync::Arc;

        /// Connection whose DML execute always returns an error.
        struct FailingConnection {
            meta: dbflux_core::DriverMetadata,
        }

        impl FailingConnection {
            fn new() -> Arc<Self> {
                let meta = DriverMetadataBuilder::new(
                    "fail",
                    "Failing",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .capabilities(DriverCapabilities::TRANSACTIONS)
                .build();
                Arc::new(Self { meta })
            }
        }

        impl dbflux_core::Connection for FailingConnection {
            fn metadata(&self) -> &dbflux_core::DriverMetadata {
                &self.meta
            }

            fn ping(&self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn execute(
                &self,
                req: &dbflux_core::QueryRequest,
            ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
                let sql = req.sql.to_ascii_uppercase();
                if sql.starts_with("DELETE") || sql.starts_with("UPDATE") {
                    Err(dbflux_core::DbError::QueryFailed(
                        dbflux_core::FormattedError::new("simulated driver error"),
                    ))
                } else {
                    Ok(dbflux_core::QueryResult::empty())
                }
            }

            fn cancel(
                &self,
                _handle: &dbflux_core::QueryHandle,
            ) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }

            fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                Err(dbflux_core::DbError::NotSupported("stub".to_string()))
            }

            fn kind(&self) -> DbKind {
                DbKind::Postgres
            }

            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }

            fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                &DefaultSqlDialect
            }

            fn query_generator(&self) -> Option<&dyn QueryGenerator> {
                static GENERATOR: SimpleDeleteGenerator = SimpleDeleteGenerator;
                Some(&GENERATOR)
            }
        }

        /// K-1: run_single_tx on a failing connection returns ExecutorError::Transaction
        /// whose Display includes the table name (the caller wraps it before reporting_error_async).
        #[test]
        fn k1_run_single_tx_failure_returns_executor_error() {
            let conn = FailingConnection::new();
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::single_transaction();
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            };
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx();

            assert!(
                result.is_err(),
                "expected error from failing connection, got: {:?}",
                result
            );

            let err = result.unwrap_err();
            assert!(
                matches!(err, ExecutorError::Transaction(_)),
                "expected Transaction error variant, got: {:?}",
                err
            );

            let display = err.to_string();
            assert!(
                display.contains("simulated driver error"),
                "error message must contain the driver error text; got: {}",
                display
            );
        }

        /// K-3: run_single_tx on a successful connection returns MutationOutcome::Success
        /// with the rows_affected count from the connection.
        #[test]
        fn k3_run_single_tx_success_returns_rows_affected() {
            use super::executor_tests::RecordingConnection;

            let conn = RecordingConnection::new(DbKind::Postgres, 42);
            let spec = make_delete_spec("users");
            let opts = MutationExecOptions::single_transaction();
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            };
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx();

            assert!(
                matches!(result, Ok(MutationOutcome::Success { rows_affected: 42 })),
                "expected Success with 42 rows_affected; got: {:?}",
                result
            );
        }
    }
}
