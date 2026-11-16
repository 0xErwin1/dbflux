use std::time::{Duration, Instant};

use dbflux_core::{
    CancelToken, Connection, DbError, DriverCapabilities, QueryLanguage, QueryRequest,
    TransactionVocab,
};

use super::summary::{changes_data, classify_all, is_ddl_class};
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
    /// The rollback itself failed, so the transaction may still be open; see
    /// [`ScriptReport::finish_error`].
    Unresolved,
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
    /// The error from the commit or rollback that ended the transaction.
    pub finish_error: Option<String>,
    pub elapsed: Duration,
}

/// Runs `script` on `connection` one statement at a time.
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
        &mut on_progress,
        cancel,
    );

    Ok(finish(connection, script, plan, outcome, started.elapsed()))
}

struct Plan {
    vocab: Option<TransactionVocab>,
    ddl_not_transactional: bool,
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

    let has_ddl = classes.iter().any(|class| is_ddl_class(*class));

    Ok(Plan {
        ddl_not_transactional: vocab.is_some()
            && has_ddl
            && !connection.supports_transactional_ddl(),
        vocab,
    })
}

struct LoopOutcome {
    end: RunEnd,
    ran: usize,
    failures: Vec<StatementFailure>,
    stopped_at: Option<usize>,
}

fn execute_statements(
    connection: &dyn Connection,
    script: &SqlScript,
    on_error: OnError,
    on_progress: &mut impl FnMut(&ScriptProgress),
    cancel: &CancelToken,
) -> LoopOutcome {
    let total = script.statement_count();
    let mut outcome = LoopOutcome {
        end: RunEnd::Finished,
        ran: 0,
        failures: Vec::new(),
        stopped_at: None,
    };

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
            Ok(_) => outcome.ran += 1,
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

/// Ends the transaction (commit when the run finished, rollback otherwise)
/// and builds the report.
fn finish(
    connection: &dyn Connection,
    script: &SqlScript,
    plan: Plan,
    outcome: LoopOutcome,
    elapsed: Duration,
) -> ScriptReport {
    let mut end = outcome.end;
    let mut finish_error = None;

    let disposition = match &plan.vocab {
        None => Disposition::Kept,
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
        finish_error,
        elapsed,
    }
}
