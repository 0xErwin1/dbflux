//! What the "Run SQL script" flow tells the user: the Pick Source errors and
//! the Done step of a run, each with what happened to the statements and what
//! to do next. Pure text and data, no GPUI.

use dbflux_transfer::sql_script::{
    Disposition, RunEnd, ScriptError, ScriptReport, ScriptTransaction, StatementFailure,
};

use super::super::RunTaskAction;
use super::super::errors::ErrorBlock;
use super::options::{counted, elapsed_label, grouped, size_label};

/// How the Done step is toned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::import_wizard) enum DoneTone {
    Success,
    Warning,
    Danger,
}

/// A statement that failed, as the Done step lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::import_wizard) struct FailureRow {
    /// `#212 · line 214`.
    pub(in crate::import_wizard) label: String,
    pub(in crate::import_wizard) snippet: String,
    pub(in crate::import_wizard) error: String,
}

/// The Done step of a script run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::import_wizard) struct DoneView {
    pub(in crate::import_wizard) headline: String,
    pub(in crate::import_wizard) tone: DoneTone,
    /// A danger block for an outcome the user must act on: nothing ran, or
    /// the transaction is unresolved.
    pub(in crate::import_wizard) block: Option<ErrorBlock>,
    /// The statement a stopped run failed on.
    pub(in crate::import_wizard) failed_statement: Option<FailureRow>,
    /// The statements a run that continued after errors failed on.
    pub(in crate::import_wizard) failures: Vec<FailureRow>,
    /// The database's error for a stopped run, or the commit's.
    pub(in crate::import_wizard) error_text: Option<String>,
    /// What happened to the changes of the statements that ran.
    pub(in crate::import_wizard) outcome: Option<String>,
    /// What to do next.
    pub(in crate::import_wizard) next_step: Option<String>,
}

/// The block of a script that could not be loaded: what went wrong and what
/// to do next. `ScriptError` variants this flow does not know yet fall back
/// to the engine's own message.
pub(in crate::import_wizard) fn load_error_block(error: &ScriptError) -> ErrorBlock {
    let (message, next_step) = match error {
        ScriptError::TooLarge { size, limit } => (
            {
                let (size, limit) = size_and_limit_labels(*size, *limit);
                dbflux_i18n::t!(
                    "document.import_wizard.script.error.too_large",
                    size = size,
                    limit = limit
                )
            },
            dbflux_i18n::t!("document.import_wizard.script.error.too_large_next"),
        ),
        ScriptError::NotUtf8 { position } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.not_utf8",
                position = grouped(*position)
            ),
            dbflux_i18n::t!("document.import_wizard.script.error.not_utf8_next"),
        ),
        ScriptError::Unreadable { path, message } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.unreadable",
                path = path,
                message = message
            ),
            dbflux_i18n::t!("document.import_wizard.script.error.unreadable_next"),
        ),
        ScriptError::Empty => (
            dbflux_i18n::t!("document.import_wizard.script.error.empty"),
            dbflux_i18n::t!("document.import_wizard.script.error.empty_next"),
        ),
        ScriptError::NotSql { language } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.not_sql",
                language = language
            ),
            dbflux_i18n::t!("document.import_wizard.script.error.not_sql_next"),
        ),
        ScriptError::ClientDirective { directive, line } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.client_directive",
                directive = directive,
                line = grouped(*line)
            ),
            dbflux_i18n::t!(
                "document.import_wizard.script.error.client_directive_next",
                directive = directive
            ),
        ),
        ScriptError::SplitRoutineBody { line } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.split_routine",
                line = grouped(*line)
            ),
            dbflux_i18n::t!("document.import_wizard.script.error.split_routine_next"),
        ),
        ScriptError::CopyFromStdin { line } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.copy_stdin",
                line = grouped(*line)
            ),
            dbflux_i18n::t!("document.import_wizard.script.error.copy_stdin_next"),
        ),
        ScriptError::PsqlMetaCommand { command, line } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.psql_meta",
                command = command,
                line = grouped(*line)
            ),
            dbflux_i18n::t!("document.import_wizard.script.error.psql_meta_next"),
        ),
        ScriptError::SessionCommitMode {
            change,
            statement,
            line,
        } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.session_commit",
                change = session_change_label(change),
                statement = statement,
                line = grouped(*line)
            ),
            dbflux_i18n::t!("document.import_wizard.script.error.session_commit_next"),
        ),
        ScriptError::ConditionalTransactionControl { line } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.conditional_transaction",
                line = grouped(*line)
            ),
            dbflux_i18n::t!("document.import_wizard.script.error.conditional_transaction_next"),
        ),
        ScriptError::SelfManagedTransaction { keyword, line } => (
            dbflux_i18n::t!(
                "document.import_wizard.script.error.self_managed",
                keyword = keyword,
                line = grouped(*line)
            ),
            dbflux_i18n::t!("document.import_wizard.script.error.self_managed_next"),
        ),
        other => (
            other.to_string(),
            dbflux_i18n::t!("document.import_wizard.script.error.other_next"),
        ),
    };

    ErrorBlock {
        message,
        next_step,
        footer_reason: None,
    }
}

/// The size and the limit as labels. A size that rounds to the same label as
/// the limit (a file one byte over it) is shown in bytes, so the user can see
/// why it was refused.
fn size_and_limit_labels(size: u64, limit: u64) -> (String, String) {
    let (rounded_size, rounded_limit) = (size_label(size), size_label(limit));

    if rounded_size == rounded_limit {
        let bytes = |value: u64| format!("{} B", grouped(value as usize));
        return (bytes(size), bytes(limit));
    }

    (rounded_size, rounded_limit)
}

/// What a statement that switches the session's commit mode does, in words.
/// A change this flow does not know is shown as the engine names it.
fn session_change_label(change: &str) -> String {
    match change {
        "turns off autocommit" => {
            dbflux_i18n::t!("document.import_wizard.script.error.session_change_autocommit")
        }
        "turns on implicit transactions" => {
            dbflux_i18n::t!("document.import_wizard.script.error.session_change_implicit")
        }
        other => other.to_string(),
    }
}

/// Whether the run ended with a transaction that may still be open: the
/// engine's own, or one the script opened.
pub(in crate::import_wizard) fn is_unresolved(report: &ScriptReport) -> bool {
    report.disposition == Disposition::Unresolved
        || report.script_transaction == ScriptTransaction::LeftOpen
}

/// The Done step of a finished run. `transaction_offered` says whether the
/// connection could have run it in one transaction, which decides whether
/// the next step suggests it.
#[cfg(test)]
pub(in crate::import_wizard) fn done_view(
    report: &ScriptReport,
    transaction_offered: bool,
) -> DoneView {
    done_view_at(report, transaction_offered, None)
}

/// [`done_view`] with the file line of the statement that committed on its
/// own, which the report only carries as a position.
pub(in crate::import_wizard) fn done_view_at(
    report: &ScriptReport,
    transaction_offered: bool,
    implicit_commit_line: Option<usize>,
) -> DoneView {
    if is_unresolved(report) {
        return unresolved_view(report);
    }

    let mut view = match report.end {
        RunEnd::Finished if report.failures.is_empty() => {
            finished_view(report, transaction_offered)
        }
        RunEnd::Finished => continued_view(report),
        RunEnd::Cancelled => cancelled_view(report),
        RunEnd::StoppedOnError => stopped_view(report, transaction_offered),
    };

    if report.disposition == Disposition::PartiallyKept {
        view.outcome = Some(partially_kept_text(report, implicit_commit_line));
        view.next_step = Some(dbflux_i18n::t!(
            "document.import_wizard.script.done.partially_kept_next"
        ));
    } else if let Some(neutral) = own_transaction_rollback(report) {
        view.outcome = Some(neutral.0);
        view.next_step = Some(neutral.1);
    } else if let Some(key) = script_transaction_note(report.script_transaction) {
        let note = dbflux_i18n::t!(key);
        view.outcome = Some(match view.outcome.take() {
            Some(outcome) => format!("{outcome} {note}"),
            None => note,
        });
    }

    view
}

/// The line about a transaction the engine rolled back for the script, if
/// the run ended that way.
fn script_transaction_note(transaction: ScriptTransaction) -> Option<&'static str> {
    match transaction {
        ScriptTransaction::PossiblyOpenRolledBack => {
            Some("document.import_wizard.script.done.possibly_open_rolled_back")
        }
        _ => None,
    }
}

/// The outcome and next step of a stop or cancel inside the script's own
/// transaction. Only the statements before the script's `BEGIN` were saved,
/// and the report does not say where that was, so neither text counts them.
fn own_transaction_rollback(report: &ScriptReport) -> Option<(String, String)> {
    if report.script_transaction != ScriptTransaction::RolledBack {
        return None;
    }
    let outcome = dbflux_i18n::t!("document.import_wizard.script.done.script_rolled_back");

    match (report.end, report.failures.first()) {
        (RunEnd::StoppedOnError, Some(failure)) => Some((
            outcome,
            dbflux_i18n::t!(
                "document.import_wizard.script.done.stopped_own_transaction_next",
                index = grouped(failure.index)
            ),
        )),
        (RunEnd::Cancelled, _) => Some((
            outcome,
            dbflux_i18n::t!("document.import_wizard.script.done.cancelled_own_transaction_next"),
        )),
        _ => None,
    }
}

/// Everything up to the statement that committed on its own was saved, and
/// the ones after it may have been: never "nothing was saved".
fn partially_kept_text(report: &ScriptReport, line: Option<usize>) -> String {
    let index = grouped(report.first_implicit_commit.unwrap_or_default());

    match line {
        Some(line) => dbflux_i18n::t!(
            "document.import_wizard.script.done.partially_kept",
            index = index,
            line = grouped(line)
        ),
        None => dbflux_i18n::t!(
            "document.import_wizard.script.done.partially_kept_no_line",
            index = index
        ),
    }
}

fn plain_view(headline: String, tone: DoneTone) -> DoneView {
    DoneView {
        headline,
        tone,
        block: None,
        failed_statement: None,
        failures: Vec::new(),
        error_text: None,
        outcome: None,
        next_step: None,
    }
}

fn finished_view(report: &ScriptReport, transaction_offered: bool) -> DoneView {
    if report.script_transaction == ScriptTransaction::EndedWithoutCommit {
        return ended_without_commit_view(report, transaction_offered);
    }

    let mut view = plain_view(
        counted(
            "document.import_wizard.script.done.finished",
            report.ran,
            &[("elapsed", elapsed_label(report.elapsed))],
        ),
        DoneTone::Success,
    );
    view.outcome = Some(match report.disposition {
        Disposition::Committed => {
            dbflux_i18n::t!("document.import_wizard.script.done.committed")
        }
        _ => dbflux_i18n::t!("document.import_wizard.script.done.saved_as_run"),
    });

    view
}

/// The script opened a transaction and never ended it: the engine rolled it
/// back, so what it changed there was not saved.
fn ended_without_commit_view(report: &ScriptReport, transaction_offered: bool) -> DoneView {
    let mut view = plain_view(
        counted(
            "document.import_wizard.script.done.finished",
            report.ran,
            &[("elapsed", elapsed_label(report.elapsed))],
        ),
        DoneTone::Warning,
    );
    view.outcome = Some(dbflux_i18n::t!(
        "document.import_wizard.script.done.ended_without_commit"
    ));
    view.next_step = Some(if transaction_offered {
        dbflux_i18n::t!("document.import_wizard.script.done.ended_without_commit_next_or")
    } else {
        dbflux_i18n::t!("document.import_wizard.script.done.ended_without_commit_next")
    });

    view
}

fn continued_view(report: &ScriptReport) -> DoneView {
    let mut view = plain_view(
        dbflux_i18n::t!(
            "document.import_wizard.script.done.with_errors",
            ran = grouped(report.ran),
            total = grouped(report.total),
            failed = grouped(report.failures.len())
        ),
        DoneTone::Danger,
    );
    view.failures = report.failures.iter().map(failure_row).collect();
    view.next_step = Some(dbflux_i18n::t!(
        "document.import_wizard.script.done.with_errors_next",
        failed = grouped(report.failures.len())
    ));

    view
}

fn cancelled_view(report: &ScriptReport) -> DoneView {
    let mut view = plain_view(
        dbflux_i18n::t!(
            "document.import_wizard.script.done.cancelled",
            ran = grouped(report.ran),
            total = grouped(report.total)
        ),
        DoneTone::Warning,
    );
    view.failures = report.failures.iter().map(failure_row).collect();

    match report.disposition {
        Disposition::RolledBack => {
            view.outcome = Some(rolled_back_text(
                dbflux_i18n::t!("document.import_wizard.script.done.rolled_back_cancelled"),
                report,
            ));
            view.next_step = Some(dbflux_i18n::t!(
                "document.import_wizard.script.done.cancelled_rolled_back_next"
            ));
        }
        _ if report.ran == 0 => {
            view.outcome = Some(dbflux_i18n::t!(
                "document.import_wizard.script.done.nothing_ran"
            ));
        }
        _ => {
            view.outcome = Some(counted(
                "document.import_wizard.script.done.cancelled_kept",
                report.ran,
                &[],
            ));
            view.next_step = Some(dbflux_i18n::t!(
                "document.import_wizard.script.done.cancelled_kept_next"
            ));
        }
    }

    view
}

fn stopped_view(report: &ScriptReport, transaction_offered: bool) -> DoneView {
    let Some(failure) = report.failures.first() else {
        return commit_failed_view(report);
    };

    let mut view = plain_view(
        dbflux_i18n::t!(
            "document.import_wizard.script.done.stopped",
            index = grouped(failure.index),
            total = grouped(report.total)
        ),
        DoneTone::Danger,
    );
    let before = failure.index.saturating_sub(1);

    view.failed_statement = Some(FailureRow {
        label: dbflux_i18n::t!(
            "document.import_wizard.script.done.failed_statement",
            line = grouped(failure.line)
        ),
        ..failure_row(failure)
    });
    view.error_text = Some(database_error(failure));

    if report.disposition == Disposition::RolledBack {
        let text = if before == 0 {
            dbflux_i18n::t!("document.import_wizard.script.done.rolled_back_first")
        } else {
            counted(
                "document.import_wizard.script.done.rolled_back_before",
                before,
                &[],
            )
        };
        view.outcome = Some(rolled_back_text(text, report));
        view.next_step = Some(dbflux_i18n::t!(
            "document.import_wizard.script.done.stopped_rolled_back_next"
        ));
    } else {
        let (outcome, next_step) = kept_after_stop(report, failure, before, transaction_offered);
        view.outcome = Some(outcome);
        view.next_step = Some(next_step);
    }

    view
}

/// What a stopped run without a transaction says: what ran and stayed, what
/// did not run, and how to run the rest.
fn kept_after_stop(
    report: &ScriptReport,
    failure: &StatementFailure,
    before: usize,
    transaction_offered: bool,
) -> (String, String) {
    let not_run = if failure.index >= report.total {
        dbflux_i18n::t!(
            "document.import_wizard.script.done.not_run_single",
            index = grouped(failure.index)
        )
    } else {
        dbflux_i18n::t!(
            "document.import_wizard.script.done.not_run_range",
            from = grouped(failure.index),
            total = grouped(report.total)
        )
    };

    if before == 0 {
        return (
            format!(
                "{} {not_run}",
                dbflux_i18n::t!("document.import_wizard.script.done.nothing_before_it")
            ),
            dbflux_i18n::t!(
                "document.import_wizard.script.done.fix_first_next",
                index = grouped(failure.index)
            ),
        );
    }

    let kept = counted(
        "document.import_wizard.script.done.kept_before",
        before,
        &[],
    );
    let mut next_step = dbflux_i18n::t!(
        "document.import_wizard.script.done.stopped_kept_next",
        index = grouped(failure.index),
        before = grouped(before)
    );
    if transaction_offered {
        next_step.push(' ');
        next_step.push_str(&dbflux_i18n::t!(
            "document.import_wizard.script.done.use_transaction_next"
        ));
    }

    (format!("{kept} {not_run}"), next_step)
}

fn commit_failed_view(report: &ScriptReport) -> DoneView {
    let mut view = plain_view(
        dbflux_i18n::t!("document.import_wizard.script.done.commit_failed"),
        DoneTone::Danger,
    );
    view.error_text = report.finish_error.clone();
    view.outcome = Some(rolled_back_text(
        dbflux_i18n::t!("document.import_wizard.script.done.rolled_back_nothing_saved"),
        report,
    ));
    view.next_step = Some(dbflux_i18n::t!(
        "document.import_wizard.script.done.commit_failed_next"
    ));

    view
}

fn unresolved_view(report: &ScriptReport) -> DoneView {
    let error = report.finish_error.clone().unwrap_or_default();

    if report.script_transaction == ScriptTransaction::LeftOpen
        && report.disposition != Disposition::Unresolved
    {
        let mut view = plain_view(
            dbflux_i18n::t!("document.import_wizard.script.done.left_open"),
            DoneTone::Danger,
        );
        view.block = Some(ErrorBlock {
            message: dbflux_i18n::t!(
                "document.import_wizard.script.done.left_open_message",
                error = error
            ),
            next_step: dbflux_i18n::t!("document.import_wizard.script.done.left_open_next"),
            footer_reason: None,
        });

        return view;
    }

    let mut view = plain_view(
        dbflux_i18n::t!("document.import_wizard.script.done.unresolved"),
        DoneTone::Danger,
    );
    view.block = Some(ErrorBlock {
        message: dbflux_i18n::t!(
            "document.import_wizard.script.done.unresolved_message",
            error = error
        ),
        next_step: dbflux_i18n::t!("document.import_wizard.script.done.unresolved_next"),
        footer_reason: None,
    });

    view
}

/// `text`, then the note that schema statements the connection commits on
/// its own were not undone, when the report says so.
fn rolled_back_text(text: String, report: &ScriptReport) -> String {
    if report.ddl_not_transactional {
        format!(
            "{text} {}",
            dbflux_i18n::t!("document.import_wizard.script.done.ddl_not_undone")
        )
    } else {
        text
    }
}

fn failure_row(failure: &StatementFailure) -> FailureRow {
    FailureRow {
        label: dbflux_i18n::t!(
            "document.import_wizard.script.done.failure_label",
            index = grouped(failure.index),
            line = grouped(failure.line)
        ),
        snippet: failure.snippet.clone(),
        error: failure.error.clone(),
    }
}

fn database_error(failure: &StatementFailure) -> String {
    dbflux_i18n::t!(
        "document.import_wizard.script.done.database_error",
        error = failure.error
    )
}

/// A run's outcome resolved once for the task registry, the toast and the
/// Done step, so they agree even if the wizard is closed before it ends.
pub(in crate::import_wizard) struct ScriptResolution {
    pub(in crate::import_wizard) view: DoneView,
    pub(in crate::import_wizard) task_action: RunTaskAction,
    /// The failure to report to the user, once.
    pub(in crate::import_wizard) report: Option<String>,
    pub(in crate::import_wizard) toast_success: bool,
}

#[cfg(test)]
pub(in crate::import_wizard) fn resolve_script_outcome(
    result: Result<ScriptReport, ScriptError>,
    transaction_offered: bool,
) -> ScriptResolution {
    resolve_script_outcome_at(result, transaction_offered, None)
}

/// [`resolve_script_outcome`] with the line of the statement that committed
/// on its own.
pub(in crate::import_wizard) fn resolve_script_outcome_at(
    result: Result<ScriptReport, ScriptError>,
    transaction_offered: bool,
    implicit_commit_line: Option<usize>,
) -> ScriptResolution {
    let report = match result {
        Ok(report) => report,
        Err(error) => return not_run_resolution(&error),
    };
    let view = done_view_at(&report, transaction_offered, implicit_commit_line);
    let succeeded =
        report.end == RunEnd::Finished && report.failures.is_empty() && !is_unresolved(&report);
    let saved = succeeded && report.script_transaction != ScriptTransaction::EndedWithoutCommit;

    if succeeded && !saved {
        return ScriptResolution {
            view,
            task_action: RunTaskAction::Complete,
            report: None,
            toast_success: false,
        };
    }

    if succeeded {
        return ScriptResolution {
            view,
            task_action: RunTaskAction::Complete,
            report: None,
            toast_success: true,
        };
    }
    if report.end == RunEnd::Cancelled && !is_unresolved(&report) {
        return ScriptResolution {
            view,
            task_action: RunTaskAction::Cancel,
            report: None,
            toast_success: false,
        };
    }

    let cause = failure_cause(&report);
    let message = format!("{}: {cause}", view.headline);

    ScriptResolution {
        view,
        task_action: RunTaskAction::Fail(message.clone()),
        report: Some(message),
        toast_success: false,
    }
}

/// The error a failed run is reported with: the first failed statement's,
/// else the commit's or rollback's.
fn failure_cause(report: &ScriptReport) -> String {
    report
        .failures
        .first()
        .map(|failure| failure.error.clone())
        .or_else(|| report.finish_error.clone())
        .unwrap_or_default()
}

fn not_run_resolution(error: &ScriptError) -> ScriptResolution {
    let message = load_error_block(error).message;
    let mut view = plain_view(
        dbflux_i18n::t!("document.import_wizard.script.done.not_run"),
        DoneTone::Danger,
    );
    view.block = Some(ErrorBlock {
        message: message.clone(),
        next_step: dbflux_i18n::t!("document.import_wizard.script.done.not_run_next"),
        footer_reason: None,
    });

    ScriptResolution {
        view,
        task_action: RunTaskAction::Fail(message.clone()),
        report: Some(message),
        toast_success: false,
    }
}
