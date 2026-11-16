use std::time::{Duration, Instant};

use dbflux_core::{
    CancelToken, Connection, DbError, DriverCapabilities, QueryLanguage, QueryRequest,
    TransactionVocab,
};

use super::control::TransactionTracker;
use super::summary::{changes_data, classify_all, ddl_flags};
use super::{ScriptError, SqlScript};

/// What a failed statement does to the rest of the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnError {
    /// The run ends at the first statement that fails (the default).
    #[default]
    Stop,
    /// Every statement runs; the failures are listed in the report.
    Continue,
}

/// How a script is run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunOptions {
    pub on_error: OnError,
    /// Run every statement in one transaction, begun and ended with the
    /// connection's own [`TransactionVocab`]. Needs
    /// `DriverCapabilities::TRANSACTIONS` and cannot be combined with
    /// [`OnError::Continue`]: a failed statement aborts the transaction on
    /// some engines, so the statements after it could not run.
    pub transaction: bool,
    /// The connection's profile is read-only: a script with any statement
    /// that is not a plain read is refused before it runs.
    pub read_only_profile: bool,
}

/// Reported before each statement runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptProgress {
    /// 1-based position of the statement about to run.
    pub index: usize,
    pub total: usize,
    pub snippet: String,
}

/// How the run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEnd {
    /// Every statement was attempted.
    Finished,
    /// [`OnError::Stop`] and a statement failed, or the transaction could not
    /// be committed.
    StoppedOnError,
    /// Cancelled: the statement in progress finished, the rest did not run.
    Cancelled,
}

/// What happened to the changes the statements made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// The transaction was committed.
    Committed,
    /// The transaction was rolled back: nothing was saved (except schema
    /// statements the connection commits on its own, see
    /// [`ScriptReport::ddl_not_transactional`]).
    RolledBack,
    /// No transaction: every statement that ran was saved as it ran.
    Kept,
    /// A transaction was rolled back, but the script had already made an
    /// implicit commit (schema statements on a connection that commits them
    /// on its own, see [`ScriptReport::first_implicit_commit`]). Everything up
    /// to that statement was saved, and statements after it ran outside the
    /// transaction, so they may be saved too. Never report this as a full
    /// rollback.
    PartiallyKept,
    /// The rollback itself failed, so the transaction may still be open; see
    /// [`ScriptReport::finish_error`].
    Unresolved,
}

/// What became of a transaction the script opened itself (`BEGIN`,
/// `START TRANSACTION`) in a run that did not use [`RunOptions::transaction`].
///
/// The connection is shared with the rest of the application, so the engine
/// never leaves such a transaction open: later work would run inside it and
/// hold its locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptTransaction {
    /// The script left no transaction open.
    None,
    /// The run was stopped by an error or cancelled inside the script's own
    /// transaction, and the engine rolled it back: nothing it had changed
    /// there was saved.
    RolledBack,
    /// Every statement ran, but the script ended inside its own transaction
    /// without `COMMIT`, so the engine rolled it back: nothing it had changed
    /// there was saved. The UI says the script ended without COMMIT.
    EndedWithoutCommit,
    /// The rollback failed, so the transaction may still be open; see
    /// [`ScriptReport::finish_error`].
    LeftOpen,
}

/// A statement that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementFailure {
    /// 1-based position in the script.
    pub index: usize,
    /// 1-based start line in the file.
    pub line: usize,
    pub snippet: String,
    /// The driver's own formatted error (detail, hint and location included).
    pub error: String,
}

/// The result of [`run_script`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptReport {
    pub end: RunEnd,
    pub disposition: Disposition,
    pub total: usize,
    /// Statements that ran without error. In a rolled-back transaction they
    /// ran and were then undone.
    pub ran: usize,
    /// Failed statements in script order: at most one with [`OnError::Stop`].
    pub failures: Vec<StatementFailure>,
    /// 1-based position of the first statement that did not complete: the one
    /// that failed, or the next one that did not run after a cancel.
    pub stopped_at: Option<usize>,
    pub transaction_used: bool,
    /// A transaction was used, the script has schema statements, and the
    /// connection commits those on its own, so a rollback cannot undo them.
    pub ddl_not_transactional: bool,
    /// 1-based position of the first statement that committed on its own while
    /// the engine's transaction was open. `Some` only when
    /// `ddl_not_transactional` and such a statement ran.
    pub first_implicit_commit: Option<usize>,
    /// What became of a transaction opened by the script itself.
    pub script_transaction: ScriptTransaction,
    /// The error from the commit or rollback that ended the transaction.
    pub finish_error: Option<String>,
    pub elapsed: Duration,
}

/// Runs `script` on `connection` one statement at a time.
///
/// The run needs the connection to itself: `BEGIN`, the statements and
/// `COMMIT` or `ROLLBACK` go through the one `&dyn Connection`, so a query
/// from anywhere else in between would run inside the open transaction and
/// be undone by its rollback. Pass a connection nothing else uses for the
/// duration of the run.
///
/// A script may open a transaction of its own (`BEGIN`) when
/// [`RunOptions::transaction`] is off. If the run is stopped or cancelled
/// inside it, or the script ends inside it without `COMMIT`, the engine rolls
/// it back, and says which in [`ScriptReport::script_transaction`]. It is left
/// open only when that rollback fails. Only `BEGIN`, `START TRANSACTION`,
/// `COMMIT`, `END`, `ABORT` and `ROLLBACK` are tracked; `SET autocommit` is
/// not.
///
/// `on_progress` is called before each statement. `cancel` is checked between
/// statements, so a cancel stops the run after the statement in progress; a
/// statement the driver interrupts with `DbError::Cancelled` also counts as
/// a cancel, not a failure. A transaction is rolled back on a stop, a cancel
/// or a failed commit.
///
/// Errors are returned only when nothing ran: the connection is not SQL, the
/// options are contradictory or unsupported, a read-only profile refuses the
/// script, or the transaction could not start.
pub fn run_script(
    connection: &dyn Connection,
    script: &SqlScript,
    options: &RunOptions,
    mut on_progress: impl FnMut(&ScriptProgress),
    cancel: &CancelToken,
) -> Result<ScriptReport, ScriptError> {
    let started = Instant::now();
    let plan = preflight(connection, script, options)?;

    if let Some(vocab) = &plan.vocab {
        connection
            .execute(&QueryRequest::new(vocab.begin))
            .map_err(|error| ScriptError::TransactionStart {
                message: error.to_string(),
            })?;
    }

    let outcome = execute_statements(
        connection,
        script,
        options.on_error,
        &plan.implicit_commit,
        &mut on_progress,
        cancel,
    );

    Ok(finish(connection, script, plan, outcome, started.elapsed()))
}

struct Plan {
    vocab: Option<TransactionVocab>,
    ddl_not_transactional: bool,
    /// Per statement: it commits on its own while the engine's transaction is
    /// open.
    implicit_commit: Vec<bool>,
}

fn preflight(
    connection: &dyn Connection,
    script: &SqlScript,
    options: &RunOptions,
) -> Result<Plan, ScriptError> {
    let language = &connection.metadata().query_language;
    if !matches!(language, QueryLanguage::Sql) {
        return Err(ScriptError::NotSql {
            language: format!("{language:?}"),
        });
    }

    if options.transaction && options.on_error == OnError::Continue {
        return Err(ScriptError::TransactionWithContinue);
    }

    if options.transaction
        && let Some(control) = script.transaction_control()
    {
        return Err(ScriptError::SelfManagedTransaction {
            keyword: control.keyword.to_string(),
            line: control.line,
        });
    }

    let classes = classify_all(script, connection);

    if options.read_only_profile {
        let changing = classes.iter().filter(|class| changes_data(**class)).count();
        if changing > 0 {
            return Err(ScriptError::ReadOnlyProfile { changing });
        }
    }

    let vocab = if options.transaction {
        if !connection.supports(DriverCapabilities::TRANSACTIONS) {
            return Err(ScriptError::TransactionUnavailable);
        }
        Some(
            TransactionVocab::for_kind(connection.kind())
                .ok_or(ScriptError::TransactionUnavailable)?,
        )
    } else {
        None
    };

    let commits_ddl = vocab.is_some() && !connection.supports_transactional_ddl();
    let implicit_commit: Vec<bool> = ddl_flags(script, &classes)
        .into_iter()
        .map(|ddl| ddl && commits_ddl)
        .collect();

    Ok(Plan {
        ddl_not_transactional: implicit_commit.iter().any(|commits| *commits),
        implicit_commit,
        vocab,
    })
}

struct LoopOutcome {
    end: RunEnd,
    ran: usize,
    failures: Vec<StatementFailure>,
    stopped_at: Option<usize>,
    first_implicit_commit: Option<usize>,
    /// The script opened a transaction of its own and has not closed it.
    script_transaction_open: bool,
}

fn execute_statements(
    connection: &dyn Connection,
    script: &SqlScript,
    on_error: OnError,
    implicit_commit: &[bool],
    on_progress: &mut impl FnMut(&ScriptProgress),
    cancel: &CancelToken,
) -> LoopOutcome {
    let total = script.statement_count();
    let mut outcome = LoopOutcome {
        end: RunEnd::Finished,
        ran: 0,
        failures: Vec::new(),
        stopped_at: None,
        first_implicit_commit: None,
        script_transaction_open: false,
    };

    let mut tracker = TransactionTracker::default();

    for (position, statement) in script.statements().iter().enumerate() {
        let index = position + 1;

        if cancel.is_cancelled() {
            outcome.end = RunEnd::Cancelled;
            outcome.stopped_at = Some(index);
            break;
        }

        on_progress(&ScriptProgress {
            index,
            total,
            snippet: statement.snippet(),
        });

        match connection.execute(&QueryRequest::new(statement.text.as_str())) {
            Ok(_) => {
                outcome.ran += 1;

                if implicit_commit.get(position) == Some(&true) {
                    outcome.first_implicit_commit.get_or_insert(index);
                }
                tracker.apply(&statement.text);
                outcome.script_transaction_open = tracker.is_open();
            }
            Err(DbError::Cancelled) => {
                outcome.end = RunEnd::Cancelled;
                outcome.stopped_at = Some(index);
                break;
            }
            Err(error) => {
                outcome.failures.push(StatementFailure {
                    index,
                    line: statement.line,
                    snippet: statement.snippet(),
                    error: error.to_string(),
                });

                if on_error == OnError::Stop {
                    outcome.end = RunEnd::StoppedOnError;
                    outcome.stopped_at = Some(index);
                    break;
                }
            }
        }
    }

    outcome
}

/// Ends the transaction (commit when the run finished, rollback otherwise),
/// resolves a transaction the script opened itself, and builds the report.
fn finish(
    connection: &dyn Connection,
    script: &SqlScript,
    plan: Plan,
    outcome: LoopOutcome,
    elapsed: Duration,
) -> ScriptReport {
    let mut end = outcome.end;
    let mut finish_error = None;
    let mut script_transaction = ScriptTransaction::None;

    let disposition = match &plan.vocab {
        None => {
            if outcome.script_transaction_open {
                (script_transaction, finish_error) =
                    resolve_script_transaction(connection, end != RunEnd::Finished);
            }
            Disposition::Kept
        }
        Some(vocab) => {
            let commit_error = (end == RunEnd::Finished)
                .then(|| connection.execute(&QueryRequest::new(vocab.commit)).err())
                .flatten();

            match (end == RunEnd::Finished, commit_error) {
                (true, None) => Disposition::Committed,
                (_, commit_error) => {
                    if let Some(error) = commit_error {
                        end = RunEnd::StoppedOnError;
                        finish_error = Some(format!("Commit failed: {error}"));
                    }

                    match connection.execute(&QueryRequest::new(vocab.rollback)) {
                        Ok(_) if outcome.first_implicit_commit.is_some() => {
                            Disposition::PartiallyKept
                        }
                        Ok(_) => Disposition::RolledBack,
                        Err(error) => {
                            finish_error = Some(match finish_error {
                                Some(commit) => format!("{commit}. Rollback failed: {error}"),
                                None => format!("Rollback failed: {error}"),
                            });
                            Disposition::Unresolved
                        }
                    }
                }
            }
        }
    };

    ScriptReport {
        end,
        disposition,
        total: script.statement_count(),
        ran: outcome.ran,
        failures: outcome.failures,
        stopped_at: outcome.stopped_at,
        transaction_used: plan.vocab.is_some(),
        ddl_not_transactional: plan.ddl_not_transactional,
        first_implicit_commit: outcome.first_implicit_commit,
        script_transaction,
        finish_error,
        elapsed,
    }
}

/// The script opened a transaction and the run ended inside it, whether it
/// was interrupted (the rest of the script, including its `COMMIT`, never
/// ran) or it simply ended without one. Either way the connection is rolled
/// back: it is shared with the rest of the application, so a transaction left
/// open would silently capture later work and hold its locks.
fn resolve_script_transaction(
    connection: &dyn Connection,
    interrupted: bool,
) -> (ScriptTransaction, Option<String>) {
    let Some(vocab) = TransactionVocab::for_kind(connection.kind()) else {
        return (
            ScriptTransaction::LeftOpen,
            Some("Rollback failed: this connection has no transaction statements.".to_string()),
        );
    };

    match connection.execute(&QueryRequest::new(vocab.rollback)) {
        Ok(_) if interrupted => (ScriptTransaction::RolledBack, None),
        Ok(_) => (ScriptTransaction::EndedWithoutCommit, None),
        Err(error) => (
            ScriptTransaction::LeftOpen,
            Some(format!("Rollback failed: {error}")),
        ),
    }
}
