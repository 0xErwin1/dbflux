//! `ScriptEngine::run` — orchestrates one script run: sandbox setup,
//! `db`/`print` binding installation, dispatch-boundary classification
//! enforcement, and ledger/result collection.
//!
//! See the module docs on [`crate::binding`] and [`crate::limits`] for the
//! two collaborators this brings together.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::time::Instant;

use dbflux_core::{
    CancelToken, ExecutionClassification, ScriptMethod, ScriptOperation, ScriptOperationCounts,
    ScriptOperationHost, ScriptOperationOutcome, ScriptTarget,
};
use rquickjs::{Context, Ctx, Exception, Function, Runtime, Value};

use crate::binding;
use crate::limits;
use crate::scan::{self, StaticScanOutcome};

/// Input to one script run.
pub struct ScriptRunConfig {
    pub source: String,
    /// The classification ceiling confirmed for this run (via the
    /// pre-execution static scan). Any dispatched operation whose
    /// classification exceeds this aborts the script at that statement.
    pub ceiling: ExecutionClassification,
    pub cancel_token: CancelToken,
}

/// Whether one dispatched operation succeeded or failed.
#[derive(Debug, Clone, PartialEq)]
pub enum ScriptLedgerOutcome {
    Success,
    Failed { message: String },
}

/// One row of the dispatch ledger — one entry per operation the engine
/// attempted to dispatch, in dispatch order, successes and the failure that
/// stopped the run alike.
#[derive(Debug, Clone)]
pub struct ScriptLedgerEntry {
    pub index: usize,
    pub target: ScriptTarget,
    pub method: String,
    /// `None` only when the method name itself did not resolve to a known
    /// [`ScriptMethod`] — there is nothing to classify.
    pub classification: Option<ExecutionClassification>,
    pub outcome: ScriptLedgerOutcome,
    pub counts: ScriptOperationCounts,
}

/// The result of one successfully dispatched operation, attributable to its
/// dispatch order.
#[derive(Debug, Clone)]
pub struct ScriptStatementResult {
    pub index: usize,
    pub documents: Vec<serde_json::Value>,
    pub counts: ScriptOperationCounts,
}

/// Describes the operation that stopped the run, when one did.
#[derive(Debug, Clone)]
pub struct ScriptFailure {
    pub index: usize,
    pub message: String,
}

/// The full result of a script run. Deliberately always `Ok` from `run` for
/// a run that reached the interpreter — a mid-script driver failure is
/// recorded in `failure`, not returned as `Err`, so the ledger for the
/// statements that already ran is never discarded.
#[derive(Debug, Clone, Default)]
pub struct ScriptRunOutcome {
    pub ledger: Vec<ScriptLedgerEntry>,
    pub statements: Vec<ScriptStatementResult>,
    pub print_output: String,
    pub failure: Option<ScriptFailure>,
}

/// Failure that prevents a script from ever reaching the interpreter, or an
/// unrecoverable interpreter-level error (e.g. allocation failure creating
/// the runtime). Distinct from [`ScriptRunOutcome::failure`], which covers a
/// script that *did* start running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptEngineError {
    /// The static scan found a construct this engine does not support.
    /// Nothing from the script is dispatched.
    RejectedConstruct { construct: &'static str },
    /// The interpreter itself could not be set up.
    Interpreter { message: String },
}

impl fmt::Display for ScriptEngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RejectedConstruct { construct } => {
                write!(f, "script uses unsupported construct: {construct}")
            }
            Self::Interpreter { message } => write!(f, "script interpreter error: {message}"),
        }
    }
}

impl std::error::Error for ScriptEngineError {}

#[derive(Default)]
struct EngineState {
    ledger: Vec<ScriptLedgerEntry>,
    statements: Vec<ScriptStatementResult>,
    print_buffer: String,
    next_index: usize,
}

/// Runs `config.source` against `host`, returning the full outcome.
///
/// Returns `Err` only when the script never reaches the interpreter (a
/// rejected construct) or the interpreter itself cannot be created. Every
/// other failure — including a mid-script abort — is folded into the `Ok`
/// outcome's `failure` field so the ledger for statements that already ran
/// is preserved.
pub fn run(
    config: ScriptRunConfig,
    host: &dyn ScriptOperationHost,
) -> Result<ScriptRunOutcome, ScriptEngineError> {
    if let StaticScanOutcome::Rejected { construct } = scan::static_scan(&config.source) {
        return Err(ScriptEngineError::RejectedConstruct { construct });
    }

    let runtime = Runtime::new().map_err(|e| ScriptEngineError::Interpreter {
        message: e.to_string(),
    })?;
    runtime.set_memory_limit(limits::MEMORY_LIMIT_BYTES);
    runtime.set_max_stack_size(limits::MAX_STACK_SIZE_BYTES);

    let deadline = Instant::now() + limits::WALL_CLOCK_DEADLINE;
    let cancel_for_interrupt = config.cancel_token.clone();
    runtime.set_interrupt_handler(Some(Box::new(move || {
        Instant::now() >= deadline || cancel_for_interrupt.is_cancelled()
    })));

    let context = Context::full(&runtime).map_err(|e| ScriptEngineError::Interpreter {
        message: e.to_string(),
    })?;

    let state = Rc::new(RefCell::new(EngineState::default()));
    let ceiling = config.ceiling;
    let cancel_token = config.cancel_token.clone();
    let source = config.source.clone();

    // SAFETY: `Context::with` requires its closure to be valid for an
    // arbitrary (HRTB) `'js`, which forces any captured reference to be
    // `'static` from the type system's point of view. `host` genuinely
    // outlives this call — it is borrowed for the whole body of `run`, and
    // `with` invokes its closure exactly once, synchronously, before
    // returning here. The erased `'static` reference is used only inside
    // that single synchronous call and never stored or leaked beyond it.
    let host_static: &'static dyn ScriptOperationHost = unsafe {
        std::mem::transmute::<&dyn ScriptOperationHost, &'static dyn ScriptOperationHost>(host)
    };

    let eval_result: rquickjs::Result<()> = context.with(|ctx| {
        install_bindings(&ctx, Rc::clone(&state), host_static, ceiling, cancel_token)?;
        ctx.eval::<(), _>(binding::BOOTSTRAP_SOURCE)?;
        ctx.eval::<(), _>(source.as_str())
    });

    let mut outcome = {
        let collected = state.borrow();
        ScriptRunOutcome {
            ledger: collected.ledger.clone(),
            statements: collected.statements.clone(),
            print_output: collected.print_buffer.clone(),
            failure: None,
        }
    };

    if let Err(err) = eval_result {
        let message = match err {
            rquickjs::Error::Exception => context.with(|ctx| exception_message(&ctx, &ctx.catch())),
            other => other.to_string(),
        };
        let index = outcome.ledger.last().map(|entry| entry.index).unwrap_or(0);
        outcome.failure = Some(ScriptFailure { index, message });
    }

    Ok(outcome)
}

fn install_bindings<'js>(
    ctx: &Ctx<'js>,
    state: Rc<RefCell<EngineState>>,
    host: &'static dyn ScriptOperationHost,
    ceiling: ExecutionClassification,
    cancel_token: CancelToken,
) -> rquickjs::Result<()> {
    let globals = ctx.globals();

    let dispatch_state = Rc::clone(&state);
    let dispatch_fn = move |ctx: Ctx<'js>,
                            target_kind: String,
                            target_name: String,
                            method: String,
                            args_json: String|
          -> rquickjs::Result<String> {
        if cancel_token.is_cancelled() {
            let message = "script execution was cancelled".to_string();
            record_failure(
                &dispatch_state,
                &target_kind,
                &target_name,
                &method,
                None,
                message.clone(),
            );
            return Err(Exception::throw_message(&ctx, &message));
        }

        let Some(script_method) = ScriptMethod::from_js_name(&method) else {
            let message = format!("unsupported script method: .{method}()");
            record_failure(
                &dispatch_state,
                &target_kind,
                &target_name,
                &method,
                None,
                message.clone(),
            );
            return Err(Exception::throw_message(&ctx, &message));
        };

        let target = if target_kind == "database" {
            ScriptTarget::Database
        } else {
            ScriptTarget::Container(target_name.clone())
        };

        let arguments: Vec<serde_json::Value> = serde_json::from_str(&args_json).map_err(|e| {
            Exception::throw_message(&ctx, &format!("invalid script arguments: {e}"))
        })?;

        let operation = ScriptOperation::new(target.clone(), script_method, arguments);
        let classification = operation.classification();

        if ceiling.max(classification) != ceiling {
            let message = format!(
                ".{method}() is classified {classification:?}, which exceeds the confirmed {ceiling:?} ceiling for this run"
            );
            record_failure(
                &dispatch_state,
                &target_kind,
                &target_name,
                &method,
                Some(classification),
                message.clone(),
            );
            return Err(Exception::throw_message(&ctx, &message));
        }

        match host.dispatch(&operation) {
            Ok(outcome) => {
                let response = serde_json::json!({
                    "documents": outcome.documents,
                    "counts": counts_to_json(&outcome.counts),
                });
                record_success(
                    &dispatch_state,
                    target,
                    method.clone(),
                    classification,
                    outcome,
                );
                serde_json::to_string(&response).map_err(|e| {
                    Exception::throw_message(&ctx, &format!("failed to encode script result: {e}"))
                })
            }
            Err(db_error) => {
                let message = db_error.to_string();
                record_failure(
                    &dispatch_state,
                    &target_kind,
                    &target_name,
                    &method,
                    Some(classification),
                    message.clone(),
                );
                Err(Exception::throw_message(&ctx, &message))
            }
        }
    };
    globals.set("__dispatch", Function::new(ctx.clone(), dispatch_fn)?)?;

    let print_state = Rc::clone(&state);
    let print_fn = move |text: String| -> rquickjs::Result<()> {
        let mut collected = print_state.borrow_mut();
        if !collected.print_buffer.is_empty() {
            collected.print_buffer.push('\n');
        }
        collected.print_buffer.push_str(&text);
        Ok(())
    };
    globals.set("__print", Function::new(ctx.clone(), print_fn)?)?;

    Ok(())
}

fn record_success(
    state: &Rc<RefCell<EngineState>>,
    target: ScriptTarget,
    method: String,
    classification: ExecutionClassification,
    outcome: ScriptOperationOutcome,
) -> usize {
    let mut collected = state.borrow_mut();
    let index = collected.next_index;
    collected.next_index += 1;
    collected.statements.push(ScriptStatementResult {
        index,
        documents: outcome.documents,
        counts: outcome.counts.clone(),
    });
    collected.ledger.push(ScriptLedgerEntry {
        index,
        target,
        method,
        classification: Some(classification),
        outcome: ScriptLedgerOutcome::Success,
        counts: outcome.counts,
    });
    index
}

fn record_failure(
    state: &Rc<RefCell<EngineState>>,
    target_kind: &str,
    target_name: &str,
    method: &str,
    classification: Option<ExecutionClassification>,
    message: String,
) -> usize {
    let mut collected = state.borrow_mut();
    let index = collected.next_index;
    collected.next_index += 1;
    let target = if target_kind == "database" {
        ScriptTarget::Database
    } else {
        ScriptTarget::Container(target_name.to_string())
    };
    collected.ledger.push(ScriptLedgerEntry {
        index,
        target,
        method: method.to_string(),
        classification,
        outcome: ScriptLedgerOutcome::Failed { message },
        counts: ScriptOperationCounts::default(),
    });
    index
}

fn counts_to_json(counts: &ScriptOperationCounts) -> serde_json::Value {
    serde_json::json!({
        "matched": counts.matched,
        "modified": counts.modified,
        "inserted": counts.inserted,
        "deleted": counts.deleted,
        "upserted": counts.upserted,
    })
}

fn exception_message<'js>(_ctx: &Ctx<'js>, value: &Value<'js>) -> String {
    if let Some(obj) = value.as_object()
        && let Some(exception) = Exception::from_object(obj.clone())
        && let Some(message) = exception.message()
    {
        return message;
    }
    format!("{value:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use dbflux_core::DbError;

    #[derive(Default)]
    struct FakeHost {
        responses: Mutex<VecDeque<Result<ScriptOperationOutcome, DbError>>>,
        calls: Mutex<Vec<ScriptOperation>>,
    }

    impl FakeHost {
        fn with_responses(responses: Vec<Result<ScriptOperationOutcome, DbError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    impl ScriptOperationHost for FakeHost {
        fn dispatch(&self, op: &ScriptOperation) -> Result<ScriptOperationOutcome, DbError> {
            self.calls.lock().unwrap().push(op.clone());
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(ScriptOperationOutcome::default()))
        }
    }

    fn config(source: &str, ceiling: ExecutionClassification) -> ScriptRunConfig {
        ScriptRunConfig {
            source: source.to_string(),
            ceiling,
            cancel_token: CancelToken::new(),
        }
    }

    #[test]
    fn three_independent_statements_execute_in_order_and_produce_three_results() {
        let host = FakeHost::with_responses(vec![
            Ok(ScriptOperationOutcome::default()),
            Ok(ScriptOperationOutcome::default()),
            Ok(ScriptOperationOutcome::default()),
        ]);
        let outcome = run(
            config(
                "db.users.insertOne({a: 1}); db.users.insertOne({b: 2}); db.users.find({});",
                ExecutionClassification::Write,
            ),
            &host,
        )
        .expect("run should not error");

        assert!(outcome.failure.is_none());
        assert_eq!(outcome.statements.len(), 3);
        assert_eq!(outcome.ledger.len(), 3);
        assert_eq!(host.call_count(), 3);
        for (i, entry) in outcome.ledger.iter().enumerate() {
            assert_eq!(entry.index, i);
            assert_eq!(entry.outcome, ScriptLedgerOutcome::Success);
        }
    }

    #[test]
    fn computed_method_name_is_classified_destructive_and_aborts_under_read_ceiling() {
        let host = FakeHost::with_responses(vec![]);
        let outcome = run(
            config(
                "const m = 'deleteMany'; db.users[m]({});",
                ExecutionClassification::Read,
            ),
            &host,
        )
        .expect("run should not error");

        assert!(outcome.failure.is_some());
        assert_eq!(host.call_count(), 0, "host must never be reached");
        assert_eq!(outcome.ledger.len(), 1);
        assert_eq!(
            outcome.ledger[0].classification,
            Some(ExecutionClassification::Destructive)
        );
    }

    #[test]
    fn operation_behind_a_conditional_is_classified_and_dispatches_when_allowed() {
        let host = FakeHost::with_responses(vec![Ok(ScriptOperationOutcome::default())]);
        let outcome = run(
            config(
                "if (true) { db.users.deleteMany({}); }",
                ExecutionClassification::Destructive,
            ),
            &host,
        )
        .expect("run should not error");

        assert!(outcome.failure.is_none());
        assert_eq!(host.call_count(), 1);
        assert_eq!(
            outcome.ledger[0].classification,
            Some(ExecutionClassification::Destructive)
        );
        assert_eq!(outcome.ledger[0].outcome, ScriptLedgerOutcome::Success);
    }

    #[test]
    fn exceeding_ceiling_aborts_before_dispatch_and_later_statements_do_not_run() {
        let host = FakeHost::with_responses(vec![Ok(ScriptOperationOutcome::default())]);
        let outcome = run(
            config(
                "db.users.deleteMany({}); db.users.insertOne({x: 1});",
                ExecutionClassification::Read,
            ),
            &host,
        )
        .expect("run should not error");

        assert_eq!(host.call_count(), 0);
        assert_eq!(outcome.ledger.len(), 1);
        assert_eq!(outcome.statements.len(), 0);
        assert!(outcome.failure.is_some());
    }

    #[test]
    fn mid_script_driver_failure_aborts_and_run_still_returns_ok_with_partial_ledger() {
        let host = FakeHost::with_responses(vec![
            Ok(ScriptOperationOutcome::default()),
            Err(DbError::query_failed("boom".to_string())),
            Ok(ScriptOperationOutcome::default()),
        ]);
        let outcome = run(
            config(
                "db.users.insertOne({a: 1}); db.users.insertOne({b: 2}); db.users.insertOne({c: 3});",
                ExecutionClassification::Write,
            ),
            &host,
        )
        .expect("run should stay Ok even when a statement fails");

        assert_eq!(
            host.call_count(),
            2,
            "the third statement must not dispatch"
        );
        assert_eq!(outcome.ledger.len(), 2);
        assert_eq!(outcome.ledger[0].outcome, ScriptLedgerOutcome::Success);
        assert!(matches!(
            &outcome.ledger[1].outcome,
            ScriptLedgerOutcome::Failed { message } if message.contains("boom")
        ));
        let failure = outcome.failure.expect("failure must be recorded");
        assert_eq!(failure.index, 1);
        assert!(failure.message.contains("boom"));
    }

    #[test]
    fn print_output_is_captured() {
        let host = FakeHost::with_responses(vec![Ok(ScriptOperationOutcome::default())]);
        let outcome = run(
            config(
                "print('checkpoint'); db.users.find({});",
                ExecutionClassification::Read,
            ),
            &host,
        )
        .expect("run should not error");

        assert_eq!(outcome.print_output, "checkpoint");
    }

    #[test]
    fn find_result_supports_foreach_and_for_of_natively() {
        let outcome_docs = ScriptOperationOutcome {
            documents: vec![serde_json::json!({"_id": 1}), serde_json::json!({"_id": 2})],
            ..Default::default()
        };
        let host = FakeHost::with_responses(vec![Ok(outcome_docs)]);
        let outcome = run(
            config(
                "db.users.find({}).forEach(doc => print('seen ' + doc._id)); \
                 for (const doc of db.users.find({})) { print('iter ' + doc._id); }",
                ExecutionClassification::Read,
            ),
            &host,
        );

        // Two find() calls dispatch (one per statement); the fake host has
        // only one queued response, so the second reuses the default empty
        // outcome. What matters here is that .forEach and for...of both
        // execute without the script engine rejecting them.
        let outcome = outcome.expect("run should not error");
        assert!(outcome.failure.is_none());
        assert!(outcome.print_output.contains("seen 1"));
        assert!(outcome.print_output.contains("seen 2"));
    }

    #[test]
    fn cursor_only_methods_throw_naming_the_method_after_the_preceding_find_dispatches() {
        let host = FakeHost::with_responses(vec![Ok(ScriptOperationOutcome::default())]);
        let outcome = run(
            config("db.users.find({}).limit(5);", ExecutionClassification::Read),
            &host,
        )
        .expect("run should not error");

        assert_eq!(
            host.call_count(),
            1,
            "the preceding find must still dispatch"
        );
        assert_eq!(outcome.ledger.len(), 1);
        assert_eq!(outcome.ledger[0].outcome, ScriptLedgerOutcome::Success);
        let failure = outcome.failure.expect("limit() must throw");
        assert!(failure.message.contains("limit"));
    }

    #[test]
    fn to_array_returns_the_same_bounded_array_without_throwing() {
        let host = FakeHost::with_responses(vec![Ok(ScriptOperationOutcome::default())]);
        let outcome = run(
            config(
                "print(db.users.find({}).toArray().length);",
                ExecutionClassification::Read,
            ),
            &host,
        )
        .expect("run should not error");

        assert!(outcome.failure.is_none());
        assert_eq!(outcome.print_output, "0");
    }

    #[test]
    fn top_level_await_is_rejected_before_the_interpreter_runs() {
        let host = FakeHost::with_responses(vec![]);
        let error = run(
            config("await db.users.find({});", ExecutionClassification::Read),
            &host,
        )
        .expect_err("await must be rejected");

        assert_eq!(host.call_count(), 0);
        assert_eq!(
            error,
            ScriptEngineError::RejectedConstruct { construct: "await" }
        );
    }

    // ==================== Sandbox limits (T5b) ====================

    #[test]
    fn infinite_loop_is_terminated_via_a_pre_cancelled_token() {
        // A pre-cancelled token makes the interrupt handler fire on its
        // first poll, so this proves the cutoff mechanism without waiting
        // out the full 30s wall-clock deadline.
        let host = FakeHost::with_responses(vec![]);
        let cancel_token = CancelToken::new();
        cancel_token.cancel();
        let outcome = run(
            ScriptRunConfig {
                source: "while (true) {}".to_string(),
                ceiling: ExecutionClassification::Read,
                cancel_token,
            },
            &host,
        )
        .expect("run must still return Ok — the interrupt is reported as a script failure");

        assert!(outcome.failure.is_some());
        assert_eq!(host.call_count(), 0);
    }

    #[test]
    fn allocation_bomb_is_cut_off_by_the_memory_limit() {
        let host = FakeHost::with_responses(vec![]);
        let outcome = run(
            config(
                "let s = 'x'; while (true) { s = s + s; }",
                ExecutionClassification::Read,
            ),
            &host,
        )
        .expect("run must still return Ok — allocation failure is a script failure");

        assert!(outcome.failure.is_some());
    }

    #[test]
    fn deep_recursion_is_cut_off_by_the_stack_size_limit() {
        let host = FakeHost::with_responses(vec![]);
        let outcome = run(
            config(
                "function recurse(n) { return recurse(n + 1); } recurse(0);",
                ExecutionClassification::Read,
            ),
            &host,
        )
        .expect("run must still return Ok — stack overflow is a script failure");

        assert!(outcome.failure.is_some());
    }

    #[test]
    fn no_filesystem_network_or_process_globals_are_reachable() {
        let host = FakeHost::with_responses(vec![Ok(ScriptOperationOutcome::default())]);
        let outcome = run(
            config(
                "print(typeof Deno === 'undefined' && typeof process === 'undefined'); \
                 db.users.find({});",
                ExecutionClassification::Read,
            ),
            &host,
        )
        .expect("run should not error");

        assert!(outcome.failure.is_none());
        assert_eq!(outcome.print_output, "true");
    }
}
