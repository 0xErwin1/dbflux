use std::sync::Arc;
use std::time::Duration;

use dbflux_core::{
    Connection, DriverCapabilities, EventCategory, EventOutcome, EventRecord, EventSeverity,
    EventSink, MutationKind, MutationPolicy, QueryRequest, TransactionVocab, Value,
    VisualMutationSpec, render_filter_node_sql,
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

        // The receiver may have already timed out and been dropped; drop the send error.
        let _drop_send = tx.send(result);
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

/// Computes the effective chunk size, clamping `requested` to the driver's
/// `max_query_parameters` limit.
///
/// Returns `(effective, reduced_from)` where `reduced_from` is `Some(requested)`
/// when clamping occurred, `None` when no clamping was needed.
///
/// When the driver imposes a low parameter limit that forces the effective chunk
/// size below the spec floor of 1000, the floor is relaxed automatically. A
/// `Toast::warning` (not info) is emitted by the caller when this occurs.
pub fn compute_effective_chunk_size(
    requested: u32,
    max_params: u32,
    filter_param_count: u32,
    assignment_param_count: u32,
    pk_col_count: u32,
) -> (u32, Option<u32>) {
    if max_params == 0 {
        return (requested, None);
    }

    let overhead = filter_param_count + assignment_param_count;
    let per_row = pk_col_count.max(1);
    let max_safe = max_params.saturating_sub(overhead) / per_row;
    let max_safe = max_safe.max(1);

    if max_safe < requested {
        (max_safe, Some(requested))
    } else {
        (requested, None)
    }
}

/// Counts the number of parameters a set of assignments will bind.
///
/// `AssignmentValue::Null`, `Default`, and `Expression` produce no bound
/// parameters — only `Literal` and `Param` variants bind placeholder slots.
pub fn count_assignment_params(assignments: &[dbflux_core::Assignment]) -> u32 {
    use dbflux_core::AssignmentValue;
    assignments
        .iter()
        .filter(|a| {
            !matches!(
                a.value,
                AssignmentValue::Null | AssignmentValue::Default | AssignmentValue::Expression(_)
            )
        })
        .count() as u32
}

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
    /// Cancellation is checked at four points: (a) before lock_timeout SET, (b) after
    /// before-BEGIN lock_timeout SET but before BEGIN, (c) after BEGIN before DML,
    /// and (d) after DML success before COMMIT. DML itself is never interrupted mid-query.
    /// Sites (a) and (b) return without starting a transaction; sites (c) and (d) ROLLBACK.
    ///
    /// Emits a parent audit event with `Pending` at start, then finalizes with
    /// `Success`, `Failed`, or `Cancelled` depending on outcome.
    ///
    /// Returns the outcome after the transaction is committed or rolled back.
    pub fn run_single_tx(
        &self,
        cancel: &crate::task_runner::MutationCancelHandle,
    ) -> Result<MutationOutcome, ExecutorError> {
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

        let vocab = TransactionVocab::for_kind(self.deps.connection.kind()).ok_or_else(|| {
            ExecutorError::Transaction("driver does not support SQL transactions".to_string())
        })?;

        let run_id = uuid::Uuid::new_v4().to_string();
        let table_name = self.spec.from.name.clone();
        let op_kind = match &self.spec.kind {
            dbflux_core::MutationKind::Update { .. } => "update",
            dbflux_core::MutationKind::Delete => "delete",
        };

        let pending_event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Pending,
        )
        .with_action("mutation.run")
        .with_summary(format!("{} {} (single transaction)", op_kind, table_name))
        .with_correlation_id(run_id.clone());

        self.emit_event(pending_event);

        if cancel.is_cancelled() {
            self.emit_cancelled_event(&run_id, op_kind, &table_name, 0);
            return Ok(MutationOutcome::Cancelled { rows_affected: 0 });
        }

        if let Some(ms) = self.opts.lock_timeout_ms
            && vocab.lock_timeout_before_begin
            && let Some(lock_sql) = vocab.lock_timeout_sql(ms)
        {
            let lock_req = QueryRequest::new(lock_sql);
            if let Err(e) = self.deps.connection.execute(&lock_req) {
                let err_msg = e.to_string();
                self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                self.reset_lock_timeout_if_needed(&vocab);
                return Err(ExecutorError::Transaction(err_msg));
            }
        }

        // Site (b): cancel check after before-BEGIN lock_timeout SET, before BEGIN.
        // The SET ran but no transaction was opened, so no ROLLBACK is needed.
        if cancel.is_cancelled() {
            self.emit_cancelled_event(&run_id, op_kind, &table_name, 0);
            self.reset_lock_timeout_if_needed(&vocab);
            return Ok(MutationOutcome::Cancelled { rows_affected: 0 });
        }

        let begin_req = QueryRequest::new(vocab.begin);
        if let Err(e) = self.deps.connection.execute(&begin_req) {
            let err_msg = e.to_string();
            self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
            self.reset_lock_timeout_if_needed(&vocab);
            return Err(ExecutorError::Transaction(err_msg));
        }

        // Site (c): cancel check after BEGIN, before DML (existing).
        if cancel.is_cancelled() {
            let rollback_req = QueryRequest::new(vocab.rollback);
            if let Err(rb_err) = self.deps.connection.execute(&rollback_req) {
                log::warn!(
                    "ROLLBACK failed during cancellation after BEGIN: {}",
                    rb_err
                );
            }
            self.emit_cancelled_event(&run_id, op_kind, &table_name, 0);
            self.reset_lock_timeout_if_needed(&vocab);
            return Ok(MutationOutcome::Cancelled { rows_affected: 0 });
        }

        if let Some(ms) = self.opts.lock_timeout_ms
            && !vocab.lock_timeout_before_begin
            && let Some(lock_sql) = vocab.lock_timeout_sql(ms)
        {
            let lock_req = QueryRequest::new(lock_sql);
            if let Err(e) = self.deps.connection.execute(&lock_req) {
                let err_msg = e.to_string();
                let rollback_req = QueryRequest::new(vocab.rollback);
                if let Err(rb_err) = self.deps.connection.execute(&rollback_req) {
                    log::warn!("ROLLBACK failed after lock_timeout error: {}", rb_err);
                }
                self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                self.reset_lock_timeout_if_needed(&vocab);
                return Err(ExecutorError::Transaction(err_msg));
            }
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
                self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                self.reset_lock_timeout_if_needed(&vocab);
                Err(ExecutorError::Transaction(err_msg))
            }
            Ok(result) => {
                let rows_affected = result.affected_rows.unwrap_or(0);

                // Site (d): cancel check after DML success, before COMMIT.
                // DML has mutated rows but we haven't committed. ROLLBACK discards the changes.
                if cancel.is_cancelled() {
                    let rollback_req = QueryRequest::new(vocab.rollback);
                    if let Err(rb_err) = self.deps.connection.execute(&rollback_req) {
                        log::warn!("ROLLBACK failed during cancellation after DML: {}", rb_err);
                    }
                    self.emit_cancelled_event(&run_id, op_kind, &table_name, 0);
                    self.reset_lock_timeout_if_needed(&vocab);
                    return Ok(MutationOutcome::Cancelled { rows_affected: 0 });
                }

                let commit_req = QueryRequest::new(vocab.commit);
                if let Err(e) = self.deps.connection.execute(&commit_req) {
                    let err_msg = e.to_string();
                    self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                    self.reset_lock_timeout_if_needed(&vocab);
                    return Err(ExecutorError::Transaction(err_msg));
                }

                let success_event = EventRecord::new(
                    Self::now_ms(),
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
                self.reset_lock_timeout_if_needed(&vocab);
                Ok(MutationOutcome::Success { rows_affected })
            }
        }
    }

    /// Execute the mutation without a transaction wrapper (autocommit mode).
    ///
    /// Cancellation is checked once before execute. If cancelled, returns
    /// `Cancelled { rows_affected: 0 }` without executing any SQL.
    ///
    /// Used when the driver does not support transactions (`DirectAutocommit` mode).
    /// Emits the same audit events as `run_single_tx` but without BEGIN/COMMIT.
    pub fn run_direct(
        &self,
        cancel: &crate::task_runner::MutationCancelHandle,
    ) -> Result<MutationOutcome, ExecutorError> {
        let generator = self.deps.connection.query_generator().ok_or_else(|| {
            ExecutorError::Generation("driver does not support SQL generation".to_string())
        })?;

        let kind = &self.spec.kind;

        let generated = match kind {
            MutationKind::Update { .. } => generator
                .generate_update_from_spec(&self.spec)
                .map_err(|e| ExecutorError::Generation(e.to_string()))?,
            MutationKind::Delete => generator
                .generate_delete_from_spec(&self.spec)
                .map_err(|e| ExecutorError::Generation(e.to_string()))?,
        };

        let table_name = self.spec.from.name.clone();
        let op_kind = match kind {
            MutationKind::Update { .. } => "update",
            MutationKind::Delete => "delete",
        };

        let run_id = uuid::Uuid::new_v4().to_string();

        let pending_event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Pending,
        )
        .with_action("mutation.run")
        .with_summary(format!("{} {} (direct autocommit)", op_kind, table_name))
        .with_correlation_id(run_id.clone());

        self.emit_event(pending_event);

        if cancel.is_cancelled() {
            self.emit_cancelled_event(&run_id, op_kind, &table_name, 0);
            return Ok(MutationOutcome::Cancelled { rows_affected: 0 });
        }

        let vocab = TransactionVocab::for_kind(self.deps.connection.kind());

        // Use the autocommit-specific lock_timeout variant. For Postgres, `SET LOCAL` is
        // transaction-scoped and silently does nothing outside a transaction; the autocommit
        // template uses session-scoped `SET lock_timeout` instead. MySQL and MSSQL reuse the
        // same session/connection-scoped statement in both modes.
        let lock_timeout_set = if let Some(v) = &vocab
            && let Some(ms) = self.opts.lock_timeout_ms
            && let Some(lock_sql) = v.autocommit_lock_timeout_sql(ms)
        {
            let lock_req = QueryRequest::new(lock_sql);
            if let Err(e) = self.deps.connection.execute(&lock_req) {
                let err_msg = e.to_string();
                self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                self.reset_autocommit_lock_timeout_if_needed(v);
                return Err(ExecutorError::Transaction(err_msg));
            }
            true
        } else {
            false
        };

        let mut dml_req = QueryRequest::new(generated.sql.clone());
        dml_req.params = generated.params.clone();

        match self.deps.connection.execute(&dml_req) {
            Err(e) => {
                let err_msg = e.to_string();
                self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                if lock_timeout_set && let Some(v) = &vocab {
                    self.reset_autocommit_lock_timeout_if_needed(v);
                }
                Err(ExecutorError::Transaction(err_msg))
            }
            Ok(result) => {
                let rows_affected = result.affected_rows.unwrap_or(0);

                let success_event = EventRecord::new(
                    Self::now_ms(),
                    EventSeverity::Info,
                    EventCategory::Query,
                    EventOutcome::Success,
                )
                .with_action("mutation.run")
                .with_summary(format!(
                    "{} {} completed ({} rows affected, autocommit)",
                    op_kind, table_name, rows_affected
                ))
                .with_correlation_id(run_id);

                self.emit_event(success_event);
                if lock_timeout_set && let Some(v) = &vocab {
                    self.reset_autocommit_lock_timeout_if_needed(v);
                }
                Ok(MutationOutcome::Success { rows_affected })
            }
        }
    }

    /// Execute the mutation as a series of keyset-paginated chunks.
    ///
    /// Each chunk executes as its own BEGIN / DML WHERE (user_filter) AND pk IN (...) / COMMIT.
    /// The user filter and PK keyset are merged into a single WHERE clause by the generator —
    /// the executor never post-concatenates SQL.
    ///
    /// Cancellation is checked at five sites: at the top of each loop iteration (between
    /// chunks), between the before-BEGIN lock_timeout SET and BEGIN within a chunk, after
    /// BEGIN before DML (top of loop), between DML success and COMMIT within a chunk.
    /// Each chunk's DML always runs to natural completion — DML is never interrupted mid-query.
    ///
    /// # PK SELECT consistency note
    ///
    /// The keyset SELECT runs outside any chunk transaction. This is an accepted trade-off:
    /// the SELECT is read-only and used only to compute the next batch of PKs; it does not
    /// need to see the effects of earlier chunks. Running it outside the transaction avoids
    /// holding read locks across the SELECT + DML window, which would increase contention
    /// on busy tables. The approach is safe because the DML itself re-filters by PK IN (...)
    /// combined with the original user filter, so rows that disappear between the SELECT and
    /// the DML are simply missed (no phantom deletes, no stale updates).
    ///
    /// `pk_cols` are the primary key column names of the target table.
    /// `cancel` is checked between chunks — flip it to abort after the current chunk.
    pub fn run_chunked_tx(
        &self,
        pk_cols: &[&str],
        cancel: &crate::task_runner::MutationCancelHandle,
    ) -> Result<MutationOutcome, ExecutorError> {
        use dbflux_core::lower_keyset_predicate;

        let generator = self.deps.connection.query_generator().ok_or_else(|| {
            ExecutorError::Generation("driver does not support SQL generation".to_string())
        })?;

        let vocab = TransactionVocab::for_kind(self.deps.connection.kind()).ok_or_else(|| {
            ExecutorError::Transaction("driver does not support SQL transactions".to_string())
        })?;

        let table_name = self.spec.from.name.clone();
        let op_kind = match &self.spec.kind {
            MutationKind::Update { .. } => "update",
            MutationKind::Delete => "delete",
        };

        let run_id = uuid::Uuid::new_v4().to_string();

        let pending_event = EventRecord::new(
            Self::now_ms(),
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

        // Clamp chunk_size to stay within the driver's max_query_parameters limit.
        // A composite PK chunk binds `chunk_size * pk_cols.len()` params for PK IN,
        // plus filter params and SET params. If the total would exceed the driver
        // limit, reduce chunk_size to the largest safe value.
        let effective_chunk_size = {
            let max_params = self
                .deps
                .connection
                .metadata()
                .query
                .as_ref()
                .map(|q| q.max_query_parameters)
                .unwrap_or(0);

            let mut dummy_filter_params: Vec<Value> = Vec::new();
            let mut dummy_idx: usize = 1;
            render_filter_node_sql(
                self.spec.filter.as_ref(),
                dialect,
                &mut dummy_filter_params,
                &mut dummy_idx,
            );
            let filter_param_count = dummy_filter_params.len() as u32;

            let assignment_param_count = match &self.spec.kind {
                MutationKind::Update { assignments } => count_assignment_params(assignments),
                MutationKind::Delete => 0,
            };

            let (effective, _) = compute_effective_chunk_size(
                self.opts.chunk_size,
                max_params,
                filter_param_count,
                assignment_param_count,
                pk_cols.len() as u32,
            );

            effective
        };

        loop {
            if cancel.is_cancelled() {
                let cancelled_event = EventRecord::new(
                    Self::now_ms(),
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
                self.reset_lock_timeout_if_needed(&vocab);
                return Ok(MutationOutcome::Cancelled {
                    rows_affected: rows_affected_total,
                });
            }

            // Step 1: SELECT pk_cols WHERE (user_filter AND) keyset_pred ORDER BY pk {limit clause}
            //
            // The user filter is included so we only page through rows that match the mutation
            // predicate — without it, the loop would scan the entire table's PK space.
            let pk_col_refs: Vec<String> = pk_cols
                .iter()
                .map(|c| dialect.quote_identifier(c))
                .collect();
            let pk_select = pk_col_refs.join(", ");

            let mut select_params: Vec<Value> = Vec::new();
            let mut param_idx: usize = 1;

            // Build user filter clause first, then the keyset continuation predicate.
            let user_filter_clause = render_filter_node_sql(
                self.spec.filter.as_ref(),
                dialect,
                &mut select_params,
                &mut param_idx,
            );

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

            let where_parts: Vec<String> = [user_filter_clause, keyset_clause]
                .into_iter()
                .flatten()
                .filter(|s| !s.is_empty())
                .collect();

            let limit = dialect.limit_clause(effective_chunk_size);
            let select_sql = if where_parts.is_empty() {
                format!(
                    "SELECT {} FROM {} ORDER BY {} {}",
                    pk_select,
                    qualified_table,
                    pk_col_refs.join(", "),
                    limit,
                )
            } else {
                format!(
                    "SELECT {} FROM {} WHERE {} ORDER BY {} {}",
                    pk_select,
                    qualified_table,
                    where_parts.join(" AND "),
                    pk_col_refs.join(", "),
                    limit,
                )
            };

            let mut select_req = QueryRequest::new(select_sql);
            select_req.params = select_params;

            let pk_rows = match self.deps.connection.execute(&select_req) {
                Ok(r) => r.rows,
                Err(e) => {
                    let err_msg = e.to_string();
                    self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                    self.reset_lock_timeout_if_needed(&vocab);
                    return Err(ExecutorError::Transaction(err_msg));
                }
            };

            if pk_rows.is_empty() {
                break;
            }

            // Track last PK for next iteration's keyset predicate.
            if let Some(last_row) = pk_rows.last() {
                last_pk_values = Some(last_row.clone());
            }

            // Step 2: Generate the chunk DML via the generator which merges user_filter + pk IN.
            // The generator emits a single WHERE clause — the executor never post-concatenates.
            let generated = match &self.spec.kind {
                MutationKind::Update { .. } => generator
                    .generate_update_chunk_from_spec(&self.spec, pk_cols, &pk_rows)
                    .map_err(|e| ExecutorError::Generation(e.to_string()))?,
                MutationKind::Delete => generator
                    .generate_delete_chunk_from_spec(&self.spec, pk_cols, &pk_rows)
                    .map_err(|e| ExecutorError::Generation(e.to_string()))?,
            };

            // Step 3: Execute [lock_timeout if before_begin] / BEGIN / [lock_timeout if in-tx]
            //         / DML / COMMIT for this chunk.
            if let Some(ms) = self.opts.lock_timeout_ms
                && vocab.lock_timeout_before_begin
                && let Some(lock_sql) = vocab.lock_timeout_sql(ms)
            {
                let lock_req = QueryRequest::new(lock_sql);
                if let Err(e) = self.deps.connection.execute(&lock_req) {
                    let err_msg = e.to_string();
                    self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                    self.reset_lock_timeout_if_needed(&vocab);
                    return Err(ExecutorError::Transaction(err_msg));
                }
            }

            // Cancel site (b): between before-BEGIN lock_timeout SET and BEGIN.
            // No transaction is open yet, so no ROLLBACK is needed.
            if cancel.is_cancelled() {
                self.emit_event(
                    EventRecord::new(
                        Self::now_ms(),
                        EventSeverity::Info,
                        EventCategory::Query,
                        EventOutcome::Cancelled,
                    )
                    .with_action("mutation.run")
                    .with_summary(format!(
                        "{} {} cancelled after {} chunks ({} rows)",
                        op_kind, table_name, chunks_committed, rows_affected_total
                    ))
                    .with_correlation_id(run_id.clone()),
                );
                self.reset_lock_timeout_if_needed(&vocab);
                return Ok(MutationOutcome::Cancelled {
                    rows_affected: rows_affected_total,
                });
            }

            let begin_req = QueryRequest::new(vocab.begin);
            if let Err(e) = self.deps.connection.execute(&begin_req) {
                let err_msg = e.to_string();
                self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                self.reset_lock_timeout_if_needed(&vocab);
                return Err(ExecutorError::Transaction(err_msg));
            }

            if let Some(ms) = self.opts.lock_timeout_ms
                && !vocab.lock_timeout_before_begin
                && let Some(lock_sql) = vocab.lock_timeout_sql(ms)
            {
                let lock_req = QueryRequest::new(lock_sql);
                if let Err(e) = self.deps.connection.execute(&lock_req) {
                    let err_msg = e.to_string();
                    let rollback_req = QueryRequest::new(vocab.rollback);
                    if let Err(rb_err) = self.deps.connection.execute(&rollback_req) {
                        log::warn!("ROLLBACK failed after lock_timeout error: {}", rb_err);
                    }
                    self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                    self.reset_lock_timeout_if_needed(&vocab);
                    return Err(ExecutorError::Transaction(err_msg));
                }
            }

            let mut dml_req = QueryRequest::new(generated.sql);
            dml_req.params = generated.params;
            let dml_result = self.deps.connection.execute(&dml_req);

            match dml_result {
                Err(e) => {
                    let err_msg = e.to_string();
                    let rollback_req = QueryRequest::new(vocab.rollback);
                    if let Err(rb_err) = self.deps.connection.execute(&rollback_req) {
                        log::warn!("ROLLBACK failed during chunk error recovery: {}", rb_err);
                    }

                    let chunk_event = EventRecord::new(
                        Self::now_ms(),
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

                    self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                    self.reset_lock_timeout_if_needed(&vocab);
                    return Err(ExecutorError::Transaction(err_msg));
                }
                Ok(result) => {
                    let chunk_rows = result.affected_rows.unwrap_or(pk_rows.len() as u64);

                    // Cancel site (d): between DML success and COMMIT.
                    // DML mutated rows but they're uncommitted — ROLLBACK discards them.
                    if cancel.is_cancelled() {
                        let rollback_req = QueryRequest::new(vocab.rollback);
                        if let Err(rb_err) = self.deps.connection.execute(&rollback_req) {
                            log::warn!(
                                "ROLLBACK failed during cancellation after chunk DML: {}",
                                rb_err
                            );
                        }
                        self.emit_event(
                            EventRecord::new(
                                Self::now_ms(),
                                EventSeverity::Info,
                                EventCategory::Query,
                                EventOutcome::Cancelled,
                            )
                            .with_action("mutation.run")
                            .with_summary(format!(
                                "{} {} cancelled after {} chunks ({} rows, last chunk rolled back)",
                                op_kind, table_name, chunks_committed, rows_affected_total
                            ))
                            .with_correlation_id(run_id.clone()),
                        );
                        self.reset_lock_timeout_if_needed(&vocab);
                        return Ok(MutationOutcome::Cancelled {
                            rows_affected: rows_affected_total,
                        });
                    }

                    let commit_req = QueryRequest::new(vocab.commit);
                    if let Err(e) = self.deps.connection.execute(&commit_req) {
                        let err_msg = e.to_string();
                        self.emit_failure_event(&run_id, op_kind, &table_name, &err_msg);
                        self.reset_lock_timeout_if_needed(&vocab);
                        return Err(ExecutorError::Transaction(err_msg));
                    }

                    chunks_committed += 1;
                    rows_affected_total += chunk_rows;

                    let chunk_event = EventRecord::new(
                        Self::now_ms(),
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

            if pk_rows.len() < effective_chunk_size as usize {
                break;
            }
        }

        let success_event = EventRecord::new(
            Self::now_ms(),
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
        self.reset_lock_timeout_if_needed(&vocab);

        Ok(MutationOutcome::Success {
            rows_affected: rows_affected_total,
        })
    }

    /// Emit the driver's lock timeout reset SQL if one was set for this run.
    ///
    /// MySQL's `SET SESSION innodb_lock_wait_timeout` and SQL Server's `SET LOCK_TIMEOUT`
    /// are connection-scoped: they persist for the lifetime of the pooled connection.
    /// This method resets them to the driver default so subsequent mutations on the same
    /// connection do not silently inherit the previous timeout.
    ///
    /// Failure to reset is a `log::warn!` only — the primary mutation has already completed.
    fn reset_lock_timeout_if_needed(&self, vocab: &TransactionVocab) {
        if self.opts.lock_timeout_ms.is_some()
            && let Some(reset_sql) = vocab.lock_timeout_reset_sql
        {
            let reset_req = dbflux_core::QueryRequest::new(reset_sql);
            if let Err(e) = self.deps.connection.execute(&reset_req) {
                log::warn!(
                    "lock_timeout reset failed (connection may retain previous timeout): {}",
                    e
                );
            }
        }
    }

    /// Emit the autocommit lock timeout reset SQL after a `run_direct` call.
    ///
    /// `run_direct` uses the autocommit-specific SET variant (e.g. session-scoped `SET
    /// lock_timeout` for Postgres) which persists beyond the statement. This cleans up
    /// the session state so subsequent autocommit operations don't inherit the timeout.
    fn reset_autocommit_lock_timeout_if_needed(&self, vocab: &TransactionVocab) {
        if self.opts.lock_timeout_ms.is_some()
            && let Some(reset_sql) = vocab.autocommit_lock_timeout_reset_sql
        {
            let reset_req = dbflux_core::QueryRequest::new(reset_sql);
            if let Err(e) = self.deps.connection.execute(&reset_req) {
                log::warn!(
                    "autocommit lock_timeout reset failed (connection may retain previous timeout): {}",
                    e
                );
            }
        }
    }

    fn emit_event(&self, event: EventRecord) {
        if let Some(sink) = &self.deps.event_sink
            && let Err(e) = sink.record(event)
        {
            log::warn!("mutation audit event failed: {e}");
        }
    }

    fn emit_failure_event(&self, run_id: &str, op_kind: &str, table_name: &str, error: &str) {
        let event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Error,
            EventCategory::Query,
            EventOutcome::Failure,
        )
        .with_action("mutation.run")
        .with_summary(format!("{} {} failed: {}", op_kind, table_name, error))
        .with_correlation_id(run_id.to_string());

        self.emit_event(event);
    }

    fn emit_cancelled_event(
        &self,
        run_id: &str,
        op_kind: &str,
        table_name: &str,
        rows_affected: u64,
    ) {
        let event = EventRecord::new(
            Self::now_ms(),
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Cancelled,
        )
        .with_action("mutation.run")
        .with_summary(format!(
            "{} {} cancelled ({} rows affected)",
            op_kind, table_name, rows_affected
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

    fn no_cancel() -> crate::task_runner::MutationCancelHandle {
        crate::task_runner::MutationCancelHandle::new()
    }

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
            db_kind: DbKind,
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
                    db_kind: kind,
                    meta,
                    calls: Mutex::new(Vec::new()),
                    dml_affected_rows,
                })
            }

            pub(super) fn recorded_calls(&self) -> Vec<String> {
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
                self.db_kind
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

            let result = executor.run_single_tx(&no_cancel());
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

            let result = executor.run_single_tx(&no_cancel());
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

            let _outcome = executor.run_single_tx(&no_cancel());

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

            let _outcome = executor.run_single_tx(&no_cancel());

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

            let result = executor
                .run_single_tx(&no_cancel())
                .expect("expected success");
            assert_eq!(result, MutationOutcome::Success { rows_affected: 42 });
        }

        // F-R3-7: MySQL lock_timeout reset is emitted after COMMIT (success path).
        //
        // MySQL's `SET SESSION innodb_lock_wait_timeout` is connection-scoped.
        // After a successful mutation, the executor must emit the reset SQL so the
        // pooled connection does not carry the timeout into the next mutation.
        #[test]
        fn mysql_lock_timeout_reset_emitted_after_commit() {
            let conn = RecordingConnection::new(DbKind::MySQL, 5);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::SingleTransaction,
                5_000,
                Some(5_000), // enable lock_timeout
                3_000,
            );
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx(&no_cancel());
            assert!(result.is_ok(), "expected success; got: {:?}", result);

            let calls = conn_ref.recorded_calls();
            let last = calls.last().expect("must have at least one call");
            assert!(
                last.contains("DEFAULT"),
                "last call must be the lock_timeout reset (contains DEFAULT); calls: {:?}",
                calls
            );
        }

        // F-R3-7: MySQL lock_timeout reset is emitted even when the mutation fails.
        //
        // The executor must reset the session-scoped timeout regardless of outcome.
        #[test]
        fn mysql_lock_timeout_reset_emitted_after_rollback() {
            // Use a connection where DML fails so the mutation rolls back.
            struct FailingDMLOnMySQL {
                calls: Mutex<Vec<String>>,
                meta: dbflux_core::DriverMetadata,
            }

            impl FailingDMLOnMySQL {
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
                        calls: Mutex::new(Vec::new()),
                        meta,
                    })
                }
            }

            impl dbflux_core::Connection for FailingDMLOnMySQL {
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
                    Ok(QueryResult::empty())
                }
                fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
                    Ok(())
                }
                fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                    Err(dbflux_core::DbError::NotSupported("stub".to_string()))
                }
                fn kind(&self) -> DbKind {
                    DbKind::MySQL
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

            let conn = FailingDMLOnMySQL::new();
            let conn_ref = Arc::clone(&conn);
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            };
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::SingleTransaction,
                5_000,
                Some(5_000),
                3_000,
            );
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx(&no_cancel());
            assert!(result.is_err(), "expected failure; got: {:?}", result);

            let calls = conn_ref.calls.lock().unwrap().clone();
            let last = calls.last().expect("must have at least one call");
            assert!(
                last.contains("DEFAULT"),
                "last call must be the lock_timeout reset even on failure; calls: {:?}",
                calls
            );
        }

        // F-R3-7: Postgres does NOT emit lock_timeout reset (SET LOCAL is transaction-local,
        // resets automatically on COMMIT/ROLLBACK — no explicit reset needed).
        #[test]
        fn postgres_lock_timeout_no_reset_emitted() {
            let conn = RecordingConnection::new(DbKind::Postgres, 5);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::SingleTransaction,
                5_000,
                Some(2_000), // enable lock_timeout for Postgres (SET LOCAL inside tx)
                3_000,
            );
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let _outcome = executor.run_single_tx(&no_cancel());

            let calls = conn_ref.recorded_calls();
            let has_reset = calls.iter().any(|c| {
                let upper = c.to_ascii_uppercase();
                upper.contains("DEFAULT") || upper.contains("LOCK_TIMEOUT -1")
            });
            assert!(
                !has_reset,
                "Postgres must NOT emit a lock_timeout reset call; calls: {:?}",
                calls
            );
        }

        // F-R4-4: single-tx cancel before BEGIN — no transaction started, outcome Cancelled.
        #[test]
        fn single_tx_cancel_before_begin_returns_cancelled() {
            let conn = RecordingConnection::new(DbKind::Postgres, 5);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::single_transaction();
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let cancel = crate::task_runner::MutationCancelHandle::new();
            cancel.cancel();

            let result = executor.run_single_tx(&cancel);
            assert!(
                matches!(result, Ok(MutationOutcome::Cancelled { rows_affected: 0 })),
                "expected Cancelled{{0}}, got: {:?}",
                result
            );

            let calls = conn_ref.recorded_calls();
            assert!(
                !calls.iter().any(|c| c.eq_ignore_ascii_case("BEGIN")),
                "no BEGIN must be issued when cancelled before BEGIN; calls: {:?}",
                calls
            );
        }

        // F-R4-4: single-tx cancel between BEGIN and DML — ROLLBACK must be issued.
        #[test]
        fn single_tx_cancel_between_begin_and_dml_rolls_back() {
            // We need a connection that triggers cancel after BEGIN is recorded.
            // Use a connection that sets the cancel flag on the first execute (BEGIN).
            struct CancelOnBeginConnection {
                meta: dbflux_core::DriverMetadata,
                calls: Mutex<Vec<String>>,
                cancel: crate::task_runner::MutationCancelHandle,
            }

            impl CancelOnBeginConnection {
                fn new(cancel: crate::task_runner::MutationCancelHandle) -> Arc<Self> {
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
                        cancel,
                    })
                }
            }

            impl dbflux_core::Connection for CancelOnBeginConnection {
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
                    let sql_upper = req.sql.to_ascii_uppercase();
                    self.calls.lock().unwrap().push(req.sql.clone());
                    if sql_upper.eq("BEGIN") {
                        self.cancel.cancel();
                    }
                    Ok(QueryResult::empty())
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

            let cancel = crate::task_runner::MutationCancelHandle::new();
            let conn = CancelOnBeginConnection::new(cancel.clone());
            let conn_calls = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::single_transaction();
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            };
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx(&cancel);
            assert!(
                matches!(result, Ok(MutationOutcome::Cancelled { rows_affected: 0 })),
                "expected Cancelled{{0}}, got: {:?}",
                result
            );

            let calls = conn_calls.calls.lock().unwrap().clone();
            assert!(
                calls.iter().any(|c| c.eq_ignore_ascii_case("ROLLBACK")),
                "ROLLBACK must be issued when cancelled after BEGIN; calls: {:?}",
                calls
            );
        }

        // F-R5-1: single-tx cancel after lock_timeout SET but before BEGIN (MySQL path).
        //
        // MySQL emits `SET SESSION innodb_lock_wait_timeout` BEFORE `START TRANSACTION`.
        // If the user cancels between the SET and the BEGIN, no transaction was opened so no
        // ROLLBACK is needed. The session timeout must still be reset.
        #[test]
        fn single_tx_cancel_after_lock_timeout_set_returns_cancelled_without_begin() {
            struct CancelOnSetConnection {
                meta: dbflux_core::DriverMetadata,
                calls: Mutex<Vec<String>>,
                cancel: crate::task_runner::MutationCancelHandle,
            }

            impl CancelOnSetConnection {
                fn new(cancel: crate::task_runner::MutationCancelHandle) -> Arc<Self> {
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
                        cancel,
                    })
                }
            }

            impl dbflux_core::Connection for CancelOnSetConnection {
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
                    let sql_upper = req.sql.to_ascii_uppercase();
                    self.calls.lock().unwrap().push(req.sql.clone());
                    // Trigger cancel when the lock_timeout SET is executed.
                    if sql_upper.contains("INNODB_LOCK_WAIT_TIMEOUT")
                        && !sql_upper.contains("DEFAULT")
                    {
                        self.cancel.cancel();
                    }
                    Ok(QueryResult::empty())
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
                    DbKind::MySQL
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

            let cancel = crate::task_runner::MutationCancelHandle::new();
            let conn = CancelOnSetConnection::new(cancel.clone());
            let conn_calls = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::SingleTransaction,
                5_000,
                Some(2_000),
                3_000,
            );
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            };
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx(&cancel);
            assert!(
                matches!(result, Ok(MutationOutcome::Cancelled { rows_affected: 0 })),
                "expected Cancelled{{0}}, got: {:?}",
                result
            );

            let calls = conn_calls.calls.lock().unwrap().clone();

            // SET must have fired but no BEGIN.
            assert!(
                calls
                    .iter()
                    .any(|c| c.to_ascii_uppercase().contains("INNODB_LOCK_WAIT_TIMEOUT")),
                "SET SESSION lock_timeout must be in call log; calls: {:?}",
                calls
            );
            assert!(
                !calls.iter().any(|c| {
                    let u = c.to_ascii_uppercase();
                    u == "START TRANSACTION" || u == "BEGIN"
                }),
                "no BEGIN must be issued when cancelled after SET but before BEGIN; calls: {:?}",
                calls
            );
            // The lock_timeout reset must still fire (SESSION scope persists).
            assert!(
                calls
                    .iter()
                    .any(|c| c.to_ascii_uppercase().contains("DEFAULT")),
                "lock_timeout reset (DEFAULT) must be emitted even when cancelled; calls: {:?}",
                calls
            );
        }

        // F-R5-1: single-tx cancel after DML success but before COMMIT — ROLLBACK must be issued.
        //
        // The DML may have mutated rows. If the user cancels in the window between DML completion
        // and COMMIT, we must ROLLBACK to discard those changes and return Cancelled.
        #[test]
        fn single_tx_cancel_after_dml_rolls_back() {
            struct CancelOnDmlConnection {
                meta: dbflux_core::DriverMetadata,
                calls: Mutex<Vec<String>>,
                cancel: crate::task_runner::MutationCancelHandle,
            }

            impl CancelOnDmlConnection {
                fn new(cancel: crate::task_runner::MutationCancelHandle) -> Arc<Self> {
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
                        cancel,
                    })
                }
            }

            impl dbflux_core::Connection for CancelOnDmlConnection {
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
                    let sql_upper = req.sql.to_ascii_uppercase();
                    self.calls.lock().unwrap().push(req.sql.clone());
                    // Trigger cancel when DML executes (after BEGIN).
                    if sql_upper.starts_with("DELETE") || sql_upper.starts_with("UPDATE") {
                        self.cancel.cancel();
                    }
                    Ok(QueryResult::empty())
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

            let cancel = crate::task_runner::MutationCancelHandle::new();
            let conn = CancelOnDmlConnection::new(cancel.clone());
            let conn_calls = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::single_transaction();
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            };
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx(&cancel);
            assert!(
                matches!(result, Ok(MutationOutcome::Cancelled { rows_affected: 0 })),
                "expected Cancelled{{0}}, got: {:?}",
                result
            );

            let calls = conn_calls.calls.lock().unwrap().clone();

            // DML must have fired.
            assert!(
                calls.iter().any(|c| {
                    let u = c.to_ascii_uppercase();
                    u.starts_with("DELETE") || u.starts_with("UPDATE")
                }),
                "DML must be in call log; calls: {:?}",
                calls
            );
            // ROLLBACK must follow the DML.
            let dml_pos = calls.iter().position(|c| {
                let u = c.to_ascii_uppercase();
                u.starts_with("DELETE") || u.starts_with("UPDATE")
            });
            let rollback_pos = calls
                .iter()
                .position(|c| c.to_ascii_uppercase() == "ROLLBACK");
            assert!(
                rollback_pos.is_some(),
                "ROLLBACK must be issued after DML cancel; calls: {:?}",
                calls
            );
            assert!(
                dml_pos.unwrap() < rollback_pos.unwrap(),
                "ROLLBACK must come AFTER DML; calls: {:?}",
                calls
            );
            // COMMIT must NOT appear.
            assert!(
                !calls.iter().any(|c| c.to_ascii_uppercase() == "COMMIT"),
                "COMMIT must NOT be issued when cancelled after DML; calls: {:?}",
                calls
            );
        }

        // F-R4-4: direct mode cancel before execute — no SQL executed, outcome Cancelled.
        #[test]
        fn direct_cancel_before_execute_returns_cancelled() {
            let conn = RecordingConnection::new(DbKind::Postgres, 5);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts =
                MutationExecOptions::new(ExecutionMode::DirectAutocommit, 1_000, None, 3_000);
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let cancel = crate::task_runner::MutationCancelHandle::new();
            cancel.cancel();

            let result = executor.run_direct(&cancel);
            assert!(
                matches!(result, Ok(MutationOutcome::Cancelled { rows_affected: 0 })),
                "expected Cancelled{{0}}, got: {:?}",
                result
            );

            let calls = conn_ref.recorded_calls();
            assert!(
                calls.is_empty(),
                "no SQL must be executed when cancelled before execute; calls: {:?}",
                calls
            );
        }
    }

    // T-29 — [RED] Tests for chunked-tx execution loop (F-1, F-2, F-4, F-5, F-6, DR-10.1–10.9)

    mod chunked_executor_tests {
        use super::*;
        use dbflux_core::{
            Comparator, DatabaseCategory, DbKind, DefaultSqlDialect, DriverCapabilities,
            DriverMetadataBuilder, EventOutcome, EventRecord, EventSink, EventSinkError,
            FilterNode, GeneratedMutation, GeneratedQuery, LiteralValue, MutationCategory,
            MutationKind, MutationPolicy, MutationRequest, Predicate, PredicateValue,
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
                Self::new_with_max_params(select_responses, dml_affected_rows, 0)
            }

            fn new_with_max_params(
                select_responses: Vec<Vec<Vec<Value>>>,
                dml_affected_rows: u64,
                max_query_parameters: u32,
            ) -> Arc<Self> {
                use dbflux_core::QueryCapabilities;
                let mut builder = DriverMetadataBuilder::new(
                    "test",
                    "Test",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .capabilities(DriverCapabilities::TRANSACTIONS);
                if max_query_parameters > 0 {
                    builder = builder.query(QueryCapabilities {
                        max_query_parameters,
                        ..QueryCapabilities::default()
                    });
                }
                let meta = builder.build();
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

            fn generate_update_from_spec(
                &self,
                spec: &VisualMutationSpec,
            ) -> Result<GeneratedMutation, dbflux_core::GeneratorError> {
                Ok(GeneratedMutation {
                    sql: format!("UPDATE {} SET x = $1", spec.from.name),
                    params: vec![Value::Int(1)],
                    used_raw_expression: false,
                })
            }

            fn generate_delete_chunk_from_spec(
                &self,
                spec: &VisualMutationSpec,
                pk_cols: &[&str],
                pk_values: &[Vec<Value>],
            ) -> Result<GeneratedMutation, dbflux_core::GeneratorError> {
                let pk_col = pk_cols.first().copied().unwrap_or("id");
                let in_list: Vec<String> = pk_values
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("${}", i + 1))
                    .collect();
                let sql = format!(
                    "DELETE FROM {} WHERE {} IN ({})",
                    spec.from.name,
                    pk_col,
                    in_list.join(", ")
                );
                let params: Vec<Value> = pk_values
                    .iter()
                    .map(|row| row.first().cloned().unwrap_or(Value::Null))
                    .collect();
                Ok(GeneratedMutation {
                    sql,
                    params,
                    used_raw_expression: false,
                })
            }

            fn generate_update_chunk_from_spec(
                &self,
                spec: &VisualMutationSpec,
                pk_cols: &[&str],
                pk_values: &[Vec<Value>],
            ) -> Result<GeneratedMutation, dbflux_core::GeneratorError> {
                let pk_col = pk_cols.first().copied().unwrap_or("id");
                let in_list: Vec<String> = pk_values
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("${}", i + 1))
                    .collect();
                let sql = format!(
                    "UPDATE {} SET x = 1 WHERE {} IN ({})",
                    spec.from.name,
                    pk_col,
                    in_list.join(", ")
                );
                let params: Vec<Value> = pk_values
                    .iter()
                    .map(|row| row.first().cloned().unwrap_or(Value::Null))
                    .collect();
                Ok(GeneratedMutation {
                    sql,
                    params,
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

            let _outcome = executor.run_chunked_tx(&["id"], &no_cancel());

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

        // F-2: PK SELECT must include spec.filter in its WHERE clause.
        //
        // When the spec has a filter (e.g. status = 'active'), the PK SELECT
        // must contain a WHERE clause so only matching rows are paginated.
        // Without this fix, run_chunked_tx would page through the entire table
        // PK space, not just the filtered subset.
        #[test]
        fn chunked_pk_select_applies_user_filter() {
            let select_responses = vec![
                vec![vec![Value::Int(1)]],
                vec![], // terminator
            ];
            let conn = ProgrammedConnection::new(select_responses, 1);
            let conn_ref = Arc::clone(&conn);

            let spec = VisualMutationSpec {
                from: TableRef {
                    schema: None,
                    name: "orders".to_string(),
                },
                filter: Some(FilterNode::Predicate(Predicate {
                    source_alias: "t".to_string(),
                    column: "status".to_string(),
                    comparator: Comparator::Eq,
                    value: PredicateValue::Single(LiteralValue::Text("active".to_string())),
                    node_id: 0,
                })),
                kind: MutationKind::Delete,
            };

            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let _outcome = executor.run_chunked_tx(&["id"], &no_cancel());

            let calls = conn_ref.recorded_calls();
            let select_sql = calls
                .iter()
                .find(|c| c.to_ascii_uppercase().starts_with("SELECT"))
                .expect("run_chunked_tx must issue at least one SELECT for PK pagination");

            assert!(
                select_sql.to_ascii_uppercase().contains("WHERE"),
                "PK SELECT must include a WHERE clause reflecting spec.filter; SQL: {}",
                select_sql
            );
        }

        // F-R3-2: With a 2-column PK and driver max_params=100, chunk_size=1000 must be
        // clamped to ≤ 50 (100 / 2). No filter or SET params in this case.
        #[test]
        fn chunked_chunk_size_clamped_to_driver_max_params() {
            // One partial batch of 30 rows → loop runs once and terminates.
            let select_responses = vec![pk_batch(1, 30)];
            let conn = ProgrammedConnection::new_with_max_params(select_responses, 30, 100);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            // chunk_size = 1000, but driver only allows 100 params; 2-col PK → max 50/chunk.
            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let _outcome = executor.run_chunked_tx(&["id", "tenant"], &no_cancel());

            let calls = conn_ref.recorded_calls();
            let select_sql = calls
                .iter()
                .find(|c| c.to_ascii_uppercase().starts_with("SELECT"))
                .expect("must issue at least one SELECT");

            // The LIMIT (or equivalent) must be ≤ 50.
            // Extract the number after LIMIT or FETCH NEXT.
            let limit_n = if let Some(pos) = select_sql.to_ascii_uppercase().find("LIMIT ") {
                select_sql[pos + 6..]
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u32>().ok())
            } else if let Some(pos) = select_sql.to_ascii_uppercase().find("FETCH NEXT ") {
                select_sql[pos + 11..]
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u32>().ok())
            } else {
                None
            };

            assert!(
                limit_n.is_some(),
                "SELECT must include a row limit clause; SQL: {}",
                select_sql
            );
            assert!(
                limit_n.unwrap() <= 50,
                "effective chunk_size must be ≤ 50 for 2-col PK with max_params=100; \
                 got limit_n={:?}, SQL: {}",
                limit_n,
                select_sql
            );
        }

        // F-R3-2: With overhead from filter+set params, effective chunk_size is further reduced.
        // driver max_params=100, 5 SET params, 3 filter params → overhead=8, single-col PK.
        // max_safe = (100 - 8) / 1 = 92, so chunk_size=1000 must be clamped to ≤ 92.
        #[test]
        fn chunked_chunk_size_accounts_for_filter_and_set_params() {
            use dbflux_core::{
                Assignment, AssignmentValue, BoolOp, Comparator, FilterNode, LiteralValue,
                Predicate, PredicateValue, ScalarLiteral, TableRef, VisualMutationSpec,
            };
            let select_responses = vec![pk_batch(1, 10)];
            let conn = ProgrammedConnection::new_with_max_params(select_responses, 10, 100);
            let conn_ref = Arc::clone(&conn);

            // 3 filter predicates (3 params overhead) + 5 UPDATE SET params (5 overhead) = 8 total.
            let filter = Some(FilterNode::Group {
                op: BoolOp::And,
                children: vec![
                    FilterNode::Predicate(Predicate {
                        source_alias: "t".to_string(),
                        column: "col1".to_string(),
                        comparator: Comparator::Eq,
                        value: PredicateValue::Single(LiteralValue::Integer(1)),
                        node_id: 1,
                    }),
                    FilterNode::Predicate(Predicate {
                        source_alias: "t".to_string(),
                        column: "col2".to_string(),
                        comparator: Comparator::Eq,
                        value: PredicateValue::Single(LiteralValue::Integer(2)),
                        node_id: 2,
                    }),
                    FilterNode::Predicate(Predicate {
                        source_alias: "t".to_string(),
                        column: "col3".to_string(),
                        comparator: Comparator::Eq,
                        value: PredicateValue::Single(LiteralValue::Integer(3)),
                        node_id: 3,
                    }),
                ],
            });
            let assignments: Vec<Assignment> = (1u8..=5)
                .map(|i| Assignment {
                    column: format!("c{}", i),
                    value: AssignmentValue::Literal(ScalarLiteral::Integer(i as i64)),
                })
                .collect();
            let spec = VisualMutationSpec {
                from: TableRef {
                    schema: None,
                    name: "orders".to_string(),
                },
                filter,
                kind: dbflux_core::MutationKind::Update { assignments },
            };

            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let _outcome = executor.run_chunked_tx(&["id"], &no_cancel());

            let calls = conn_ref.recorded_calls();
            let select_sql = calls
                .iter()
                .find(|c| c.to_ascii_uppercase().starts_with("SELECT"))
                .expect("must issue at least one SELECT");

            let limit_n = if let Some(pos) = select_sql.to_ascii_uppercase().find("LIMIT ") {
                select_sql[pos + 6..]
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u32>().ok())
            } else if let Some(pos) = select_sql.to_ascii_uppercase().find("FETCH NEXT ") {
                select_sql[pos + 11..]
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u32>().ok())
            } else {
                None
            };

            assert!(
                limit_n.is_some(),
                "SELECT must include a row limit clause; SQL: {}",
                select_sql
            );
            assert!(
                limit_n.unwrap() <= 92,
                "effective chunk_size must be ≤ 92 with overhead=8 and max_params=100; \
                 got limit_n={:?}, SQL: {}",
                limit_n,
                select_sql
            );
        }

        // F-R3-2: When max_params is 0 (unlimited), chunk_size passes through unchanged.
        #[test]
        fn chunked_chunk_size_unchanged_when_under_driver_limit() {
            // max_params=0 → unlimited; chunk_size=1000 must pass through.
            let select_responses = vec![pk_batch(1, 50)];
            let conn = ProgrammedConnection::new(select_responses, 50); // max_params=0
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts =
                MutationExecOptions::new(ExecutionMode::ChunkedTransaction, 1_000, None, 3_000);
            let deps = make_deps(conn, None);
            let executor = MutationExecutor::new(spec, opts, deps);

            let _outcome = executor.run_chunked_tx(&["id"], &no_cancel());

            let calls = conn_ref.recorded_calls();
            let select_sql = calls
                .iter()
                .find(|c| c.to_ascii_uppercase().starts_with("SELECT"))
                .expect("must issue at least one SELECT");

            let limit_n = if let Some(pos) = select_sql.to_ascii_uppercase().find("LIMIT ") {
                select_sql[pos + 6..]
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u32>().ok())
            } else {
                None
            };

            assert_eq!(
                limit_n,
                Some(1000),
                "with unlimited max_params, chunk_size must stay at 1000; SQL: {}",
                select_sql
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

            let result = executor.run_single_tx(&no_cancel());

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

            let result = executor.run_single_tx(&no_cancel());

            assert!(
                matches!(result, Ok(MutationOutcome::Success { rows_affected: 42 })),
                "expected Success with 42 rows_affected; got: {:?}",
                result
            );
        }
    }

    // -----------------------------------------------------------------------
    // F-3: lock_timeout SQL must be emitted between BEGIN and DML
    // F-5: run_direct must not emit BEGIN or COMMIT
    // F-7: Cancelled and Failed outcomes propagated (test at executor level)
    // -----------------------------------------------------------------------

    mod fix_tests {
        use super::executor_tests::{RecordingConnection, SimpleDeleteGenerator, make_delete_spec};
        use super::*;
        use dbflux_core::{
            DatabaseCategory, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadataBuilder,
            GeneratedMutation, GeneratedQuery, MutationCategory, MutationPolicy, MutationRequest,
            QueryGenerator, QueryLanguage, QueryResult, SchemaLoadingStrategy, SchemaSnapshot,
            VisualMutationSpec,
        };
        use std::sync::Arc;

        fn make_deps_no_sink(conn: Arc<RecordingConnection>) -> MutationDeps {
            MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            }
        }

        // F-3: lock_timeout_sql_emitted_before_dml_postgres
        // Sequence must be: BEGIN, SET LOCAL lock_timeout = '500ms', DELETE ..., COMMIT
        #[test]
        fn lock_timeout_sql_emitted_before_dml_postgres() {
            let conn = RecordingConnection::new(DbKind::Postgres, 1);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts =
                MutationExecOptions::new(ExecutionMode::SingleTransaction, 5_000, Some(500), 3_000);
            let deps = make_deps_no_sink(conn);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx(&no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();
            assert!(
                calls.len() >= 3,
                "expected at least BEGIN + lock_timeout + DML: {:?}",
                calls
            );
            assert_eq!(calls[0], "BEGIN", "first call must be BEGIN");
            assert!(
                calls[1].contains("lock_timeout") || calls[1].contains("500"),
                "second call must be lock_timeout SQL; got: {}",
                calls[1]
            );
            let dml_idx = calls.iter().position(|c| {
                c.to_ascii_uppercase().starts_with("DELETE")
                    || c.to_ascii_uppercase().starts_with("UPDATE")
            });
            assert!(dml_idx.is_some(), "must have a DML call");
            assert!(
                dml_idx.unwrap() > 1,
                "DML must come after lock_timeout; calls: {:?}",
                calls
            );
            let commit_pos = calls.iter().position(|c| c == "COMMIT");
            let dml_pos = dml_idx.unwrap();
            assert!(
                commit_pos.map(|p| p > dml_pos).unwrap_or(false),
                "COMMIT must come after DML; calls: {:?}",
                calls
            );
        }

        // F-5: run_direct_emits_no_begin_or_commit
        #[test]
        fn run_direct_emits_no_begin_or_commit() {
            let conn = RecordingConnection::new(DbKind::Postgres, 7);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("events");
            let opts =
                MutationExecOptions::new(ExecutionMode::DirectAutocommit, 5_000, None, 3_000);
            let deps = make_deps_no_sink(conn);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_direct(&no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();
            assert!(
                !calls.iter().any(|c| c == "BEGIN"),
                "run_direct must NOT emit BEGIN; calls: {:?}",
                calls
            );
            assert!(
                !calls.iter().any(|c| c == "COMMIT"),
                "run_direct must NOT emit COMMIT; calls: {:?}",
                calls
            );
            let has_dml = calls.iter().any(|c| {
                let u = c.to_ascii_uppercase();
                u.starts_with("DELETE") || u.starts_with("UPDATE")
            });
            assert!(
                has_dml,
                "run_direct must execute the DML; calls: {:?}",
                calls
            );
        }

        // F-5: run_direct success returns MutationOutcome::Success
        #[test]
        fn run_direct_success_returns_rows_affected() {
            let conn = RecordingConnection::new(DbKind::Postgres, 13);
            let spec = make_delete_spec("logs");
            let opts =
                MutationExecOptions::new(ExecutionMode::DirectAutocommit, 5_000, None, 3_000);
            let deps = make_deps_no_sink(conn);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_direct(&no_cancel());
            assert!(
                matches!(result, Ok(MutationOutcome::Success { rows_affected: 13 })),
                "expected Success(13); got: {:?}",
                result
            );
        }

        // F-R2-5: MySQL lock_timeout must be emitted BEFORE START TRANSACTION.
        // Sequence must be: SET SESSION innodb_lock_wait_timeout = N, START TRANSACTION, DELETE ..., COMMIT
        #[test]
        fn mysql_lock_timeout_emitted_before_begin() {
            use dbflux_core::{
                DatabaseCategory, DefaultSqlDialect, DriverCapabilities, DriverMetadataBuilder,
                GeneratedMutation, GeneratedQuery, MutationCategory, MutationPolicy,
                MutationRequest, QueryGenerator, QueryResult, SchemaLoadingStrategy,
                SchemaSnapshot, VisualMutationSpec,
            };
            use std::sync::{Arc, Mutex};

            struct MysqlRecordingConn {
                meta: dbflux_core::DriverMetadata,
                calls: Mutex<Vec<String>>,
            }

            impl MysqlRecordingConn {
                fn new() -> Arc<Self> {
                    let meta = DriverMetadataBuilder::new(
                        "mysql", // guardrail-allow: test stub; not production branching logic
                        "MySQL",
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

                fn recorded_calls(&self) -> Vec<String> {
                    self.calls.lock().unwrap().clone()
                }
            }

            impl dbflux_core::Connection for MysqlRecordingConn {
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
                    if sql_upper.starts_with("UPDATE")
                        || sql_upper.starts_with("DELETE")
                        || sql_upper.starts_with("INSERT")
                    {
                        result.affected_rows = Some(1);
                    }
                    Ok(result)
                }
                fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
                    Ok(())
                }
                fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                    Err(dbflux_core::DbError::NotSupported("stub".to_string()))
                }
                fn kind(&self) -> DbKind {
                    DbKind::MySQL
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

            let conn = MysqlRecordingConn::new();
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            // lock_timeout_ms = Some(5000) so the SET SESSION statement is emitted
            let opts = MutationExecOptions::new(
                ExecutionMode::SingleTransaction,
                5_000,
                Some(5_000),
                3_000,
            );
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: None,
                policy: MutationPolicy::Allowed,
            };
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_single_tx(&no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();
            assert!(
                calls.len() >= 3,
                "expected at least SET SESSION + START TRANSACTION + DML; got: {:?}",
                calls
            );

            let set_pos = calls
                .iter()
                .position(|c| c.to_ascii_uppercase().contains("SESSION"));
            let begin_pos = calls.iter().position(|c| {
                c.to_ascii_uppercase().contains("START TRANSACTION")
                    || c.to_ascii_uppercase() == "BEGIN"
                    || c.to_ascii_uppercase() == "BEGIN TRANSACTION"
            });
            let dml_pos = calls.iter().position(|c| {
                let u = c.to_ascii_uppercase();
                u.starts_with("DELETE") || u.starts_with("UPDATE")
            });

            assert!(
                set_pos.is_some(),
                "must have SET SESSION call; calls: {:?}",
                calls
            );
            assert!(
                begin_pos.is_some(),
                "must have BEGIN/START TRANSACTION; calls: {:?}",
                calls
            );
            assert!(dml_pos.is_some(), "must have DML call; calls: {:?}", calls);

            assert!(
                set_pos.unwrap() < begin_pos.unwrap(),
                "SET SESSION lock_timeout must come BEFORE START TRANSACTION; calls: {:?}",
                calls
            );
            assert!(
                begin_pos.unwrap() < dml_pos.unwrap(),
                "START TRANSACTION must come BEFORE DML; calls: {:?}",
                calls
            );
        }

        // F-R2-6: audit events must be timestamped at emission time, not at run-start.
        //
        // This test cannot use sleep (too flaky), so it uses a simpler invariant:
        // every emitted event must have a timestamp >= the test start time. Before this
        // fix, `now_ms` was captured once before any events were emitted and all events
        // reused that stale value. After the fix, `Self::now_ms()` is called per event,
        // so timestamps are guaranteed to be fresh.
        #[test]
        fn chunked_run_events_have_monotonic_timestamps() {
            use dbflux_core::{EventRecord, EventSink, EventSinkError, MutationPolicy};
            use std::sync::{Arc, Mutex};

            struct TimestampCollector {
                timestamps: Mutex<Vec<i64>>,
            }

            impl TimestampCollector {
                fn new() -> Arc<Self> {
                    Arc::new(Self {
                        timestamps: Mutex::new(Vec::new()),
                    })
                }

                fn collected(&self) -> Vec<i64> {
                    self.timestamps.lock().unwrap().clone()
                }
            }

            impl EventSink for TimestampCollector {
                fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
                    self.timestamps.lock().unwrap().push(event.ts_ms);
                    Ok(event)
                }
            }

            let test_start_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let conn = RecordingConnection::new(DbKind::Postgres, 5);
            let sink = TimestampCollector::new();
            let sink_ref = Arc::clone(&sink);
            let spec = make_delete_spec("events");
            let opts = MutationExecOptions::single_transaction();
            let deps = MutationDeps {
                connection: conn as Arc<dyn dbflux_core::Connection>,
                event_sink: Some(sink as Arc<dyn dbflux_core::EventSink>),
                policy: MutationPolicy::Allowed,
            };
            let executor = MutationExecutor::new(spec, opts, deps);
            let _outcome = executor.run_single_tx(&no_cancel());

            let timestamps = sink_ref.collected();
            assert!(
                !timestamps.is_empty(),
                "expected at least one event to be emitted"
            );

            for ts in &timestamps {
                assert!(
                    *ts >= test_start_ms,
                    "event timestamp {} must be >= test start {}; all timestamps: {:?}",
                    ts,
                    test_start_ms,
                    timestamps
                );
            }
        }

        // F-R4-5: run_direct with lock_timeout must emit SET before DML and reset after DML.
        // Sequence: SET SESSION innodb_lock_wait_timeout = N, DELETE ..., SET SESSION ... DEFAULT
        #[test]
        fn direct_emits_lock_timeout_and_reset() {
            use super::executor_tests::RecordingConnection;

            let conn = RecordingConnection::new(DbKind::MySQL, 3);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::DirectAutocommit,
                1_000,
                Some(5_000),
                3_000,
            );
            let deps = make_deps_no_sink(conn);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_direct(&no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();
            assert!(
                calls.len() >= 3,
                "expected at least SET + DML + RESET; calls: {:?}",
                calls
            );

            let set_pos = calls
                .iter()
                .position(|c| c.to_ascii_uppercase().contains("INNODB_LOCK_WAIT_TIMEOUT"));
            let dml_pos = calls.iter().position(|c| {
                let u = c.to_ascii_uppercase();
                u.starts_with("DELETE") || u.starts_with("UPDATE")
            });
            let reset_pos = calls
                .iter()
                .rposition(|c| c.to_ascii_uppercase().contains("DEFAULT"));

            assert!(
                set_pos.is_some(),
                "SET SESSION lock_timeout must be emitted; calls: {:?}",
                calls
            );
            assert!(dml_pos.is_some(), "DML must be emitted; calls: {:?}", calls);
            assert!(
                reset_pos.is_some(),
                "lock_timeout RESET must be emitted; calls: {:?}",
                calls
            );
            assert!(
                set_pos.unwrap() < dml_pos.unwrap(),
                "SET must come before DML; calls: {:?}",
                calls
            );
            assert!(
                dml_pos.unwrap() < reset_pos.unwrap(),
                "RESET must come after DML; calls: {:?}",
                calls
            );
        }

        // F-R4-5: run_direct without lock_timeout must not emit any SET or RESET.
        #[test]
        fn direct_no_lock_timeout_skips_emit_and_reset() {
            use super::executor_tests::RecordingConnection;

            let conn = RecordingConnection::new(DbKind::MySQL, 3);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::DirectAutocommit,
                1_000,
                None, // no lock_timeout
                3_000,
            );
            let deps = make_deps_no_sink(conn);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_direct(&no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();
            let has_lock_timeout = calls.iter().any(|c| {
                let u = c.to_ascii_uppercase();
                u.contains("INNODB_LOCK_WAIT_TIMEOUT") || u.contains("LOCK_TIMEOUT")
            });
            assert!(
                !has_lock_timeout,
                "no lock_timeout SQL expected when lock_timeout_ms is None; calls: {:?}",
                calls
            );
        }

        // F-R5-2: Postgres run_direct must emit session-scoped SET (not SET LOCAL) and reset.
        //
        // `SET LOCAL lock_timeout` is transaction-scoped and has no effect outside a transaction.
        // In DirectAutocommit mode, `SET lock_timeout = '...ms'` (session-scoped) must be used
        // instead, and `SET lock_timeout = DEFAULT` must follow to clean up the session state.
        #[test]
        fn direct_postgres_emits_session_lock_timeout_and_reset() {
            use super::executor_tests::RecordingConnection;

            let conn = RecordingConnection::new(DbKind::Postgres, 3);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::DirectAutocommit,
                1_000,
                Some(5_000),
                3_000,
            );
            let deps = make_deps_no_sink(conn);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_direct(&no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();
            assert!(
                calls.len() >= 3,
                "expected SET + DML + RESET; calls: {:?}",
                calls
            );

            // Must contain a session-scoped SET (not SET LOCAL).
            let set_call = calls.iter().find(|c| {
                let u = c.to_ascii_uppercase();
                u.contains("LOCK_TIMEOUT") && !u.contains("DEFAULT")
            });
            assert!(
                set_call.is_some(),
                "SET lock_timeout must be emitted; calls: {:?}",
                calls
            );
            let set_sql = set_call.unwrap();
            assert!(
                !set_sql.to_ascii_uppercase().contains("LOCAL"),
                "Postgres autocommit must use session-scoped SET (no LOCAL); got: {}",
                set_sql
            );
            assert!(
                set_sql.contains("5000"),
                "SET must encode the requested ms value; got: {}",
                set_sql
            );

            // Must contain a reset.
            let reset_call = calls
                .iter()
                .rfind(|c| c.to_ascii_uppercase().contains("DEFAULT"));
            assert!(
                reset_call.is_some(),
                "SET lock_timeout = DEFAULT reset must be emitted; calls: {:?}",
                calls
            );
        }

        // F-R5-2: MySQL run_direct emits session lock_timeout + DEFAULT reset (unchanged behaviour).
        #[test]
        fn direct_mysql_emits_session_lock_timeout_and_reset() {
            use super::executor_tests::RecordingConnection;

            let conn = RecordingConnection::new(DbKind::MySQL, 3);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::DirectAutocommit,
                1_000,
                Some(5_000),
                3_000,
            );
            let deps = make_deps_no_sink(conn);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_direct(&no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();

            let set_pos = calls.iter().position(|c| {
                c.to_ascii_uppercase().contains("INNODB_LOCK_WAIT_TIMEOUT")
                    && !c.to_ascii_uppercase().contains("DEFAULT")
            });
            let reset_pos = calls
                .iter()
                .rposition(|c| c.to_ascii_uppercase().contains("DEFAULT"));
            assert!(
                set_pos.is_some(),
                "MySQL SET must be emitted; calls: {:?}",
                calls
            );
            assert!(
                reset_pos.is_some(),
                "MySQL DEFAULT reset must be emitted; calls: {:?}",
                calls
            );
            assert!(
                set_pos.unwrap() < reset_pos.unwrap(),
                "SET must come before DEFAULT reset; calls: {:?}",
                calls
            );
        }

        // F-R5-2: MSSQL run_direct emits connection-scoped SET LOCK_TIMEOUT + reset.
        #[test]
        fn direct_mssql_emits_lock_timeout_and_reset() {
            use super::executor_tests::RecordingConnection;

            let conn = RecordingConnection::new(DbKind::SqlServer, 3);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::DirectAutocommit,
                1_000,
                Some(5_000),
                3_000,
            );
            let deps = make_deps_no_sink(conn);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_direct(&no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();

            let set_pos = calls.iter().position(|c| {
                let u = c.to_ascii_uppercase();
                u.contains("LOCK_TIMEOUT") && !u.contains("-1")
            });
            let reset_pos = calls
                .iter()
                .rposition(|c| c.to_ascii_uppercase().contains("LOCK_TIMEOUT -1"));
            assert!(
                set_pos.is_some(),
                "MSSQL SET LOCK_TIMEOUT must be emitted; calls: {:?}",
                calls
            );
            assert!(
                reset_pos.is_some(),
                "MSSQL SET LOCK_TIMEOUT -1 reset must be emitted; calls: {:?}",
                calls
            );
            assert!(
                set_pos.unwrap() < reset_pos.unwrap(),
                "SET must come before reset; calls: {:?}",
                calls
            );
        }

        // F-R5-2: SQLite run_direct skips lock_timeout entirely (no template).
        #[test]
        fn direct_sqlite_skips_lock_timeout() {
            use super::executor_tests::RecordingConnection;

            let conn = RecordingConnection::new(DbKind::SQLite, 3);
            let conn_ref = Arc::clone(&conn);
            let spec = make_delete_spec("orders");
            let opts = MutationExecOptions::new(
                ExecutionMode::DirectAutocommit,
                1_000,
                Some(5_000),
                3_000,
            );
            let deps = make_deps_no_sink(conn);
            let executor = MutationExecutor::new(spec, opts, deps);

            let result = executor.run_direct(&no_cancel());
            assert!(result.is_ok(), "expected ok: {:?}", result);

            let calls = conn_ref.recorded_calls();
            let has_lock_timeout = calls.iter().any(|c| {
                let u = c.to_ascii_uppercase();
                u.contains("LOCK_TIMEOUT") || u.contains("INNODB")
            });
            assert!(
                !has_lock_timeout,
                "SQLite has no lock_timeout; no SET must be emitted; calls: {:?}",
                calls
            );
        }
    }

    // F-R4-2/F-R4-3: Tests for compute_effective_chunk_size and count_assignment_params.

    mod effective_chunk_size_tests {
        use super::super::{compute_effective_chunk_size, count_assignment_params};
        use dbflux_core::{Assignment, AssignmentValue, ScalarLiteral};

        // F-R4-2: When max_params forces a reduction within [1000, 10000], returns
        // (effective, Some(requested)).
        #[test]
        fn chunk_size_reduced_when_max_params_forces_it() {
            // max_params=2100 (MSSQL), 1 PK col, 1 filter param, 1 SET param
            // => overhead=2, per_row=1, max_safe = (2100-2)/1 = 2098
            // requested=5000 > 2098 → reduction
            let (effective, reduced_from) = compute_effective_chunk_size(5_000, 2_100, 1, 1, 1);
            assert_eq!(effective, 2_098, "effective must be max_safe");
            assert_eq!(
                reduced_from,
                Some(5_000),
                "reduced_from must carry the original request"
            );
        }

        // F-R4-2: When effective == requested (no reduction needed), reduced_from is None.
        #[test]
        fn chunk_size_unchanged_when_under_driver_limit() {
            // max_params=10000, requested=1000, overhead=0, per_row=1 → max_safe=10000
            let (effective, reduced_from) = compute_effective_chunk_size(1_000, 10_000, 0, 0, 1);
            assert_eq!(effective, 1_000);
            assert_eq!(
                reduced_from, None,
                "no reduction expected when requested <= max_safe"
            );
        }

        // F-R4-3: When max_params is very low (e.g. wide PK + many SET cols),
        // effective can fall below the spec floor of 1000.
        // The function returns the raw effective value (floor relaxation is allowed),
        // and reduced_from is Some(requested).
        #[test]
        fn chunk_size_can_fall_below_floor_when_driver_forces_it() {
            // max_params=100, 4 PK cols, 50 SET params, 10 filter params
            // => overhead=60, per_row=4, max_safe=(100-60)/4 = 10
            let (effective, reduced_from) = compute_effective_chunk_size(1_000, 100, 10, 50, 4);
            assert_eq!(
                effective, 10,
                "effective must follow driver limit below floor"
            );
            assert_eq!(
                reduced_from,
                Some(1_000),
                "reduced_from must carry original request"
            );
        }

        // F-R4-2: max_params=0 means unlimited — no reduction.
        #[test]
        fn chunk_size_unchanged_when_max_params_is_zero() {
            let (effective, reduced_from) = compute_effective_chunk_size(5_000, 0, 100, 100, 4);
            assert_eq!(effective, 5_000, "zero max_params means unlimited");
            assert_eq!(reduced_from, None);
        }

        // F-R4-6: count_assignment_params excludes Null, Default, and Expression.
        #[test]
        fn assignment_param_count_excludes_non_param_values() {
            let assignments = vec![
                Assignment {
                    column: "a".to_string(),
                    value: AssignmentValue::Literal(ScalarLiteral::Integer(1)),
                },
                Assignment {
                    column: "b".to_string(),
                    value: AssignmentValue::Null,
                },
                Assignment {
                    column: "c".to_string(),
                    value: AssignmentValue::Default,
                },
                Assignment {
                    column: "d".to_string(),
                    value: AssignmentValue::Expression("price * 1.1".to_string()),
                },
                Assignment {
                    column: "e".to_string(),
                    value: AssignmentValue::Literal(ScalarLiteral::Text("x".to_string())),
                },
            ];
            let count = count_assignment_params(&assignments);
            assert_eq!(
                count, 2,
                "only Literal values bind params; Null/Default/Expression do not"
            );
        }

        // F-R4-6: all Literal → count equals assignments.len()
        #[test]
        fn assignment_param_count_all_literals() {
            let assignments = vec![
                Assignment {
                    column: "x".to_string(),
                    value: AssignmentValue::Literal(ScalarLiteral::Integer(1)),
                },
                Assignment {
                    column: "y".to_string(),
                    value: AssignmentValue::Literal(ScalarLiteral::Integer(2)),
                },
            ];
            assert_eq!(count_assignment_params(&assignments), 2);
        }

        // F-R4-6: no Literal → count is 0
        #[test]
        fn assignment_param_count_no_literals() {
            let assignments = vec![
                Assignment {
                    column: "a".to_string(),
                    value: AssignmentValue::Null,
                },
                Assignment {
                    column: "b".to_string(),
                    value: AssignmentValue::Expression("NOW()".to_string()),
                },
            ];
            assert_eq!(count_assignment_params(&assignments), 0);
        }
    }
}
