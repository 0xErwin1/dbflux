//! What the "Run SQL script" flow tells the user: the Pick Source errors and
//! the Done step of a run, each with what happened to the statements and what
//! to do next. Pure text and data, no GPUI.

use dbflux_transfer::sql_script::{
    Disposition, RunEnd, ScriptError, ScriptReport, StatementFailure,
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
            dbflux_i18n::t!(
                "document.import_wizard.script.error.too_large",
                size = size_label(*size),
                limit = size_label(*limit)
            ),
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
        other => (
            other.to_string(),
            dbflux_i18n::t!("document.import_wizard.script.error.other_next"),
        ),
    };

    ErrorBlock { message, next_step }
}

/// The Done step of a finished run. `transaction_offered` says whether the
/// connection could have run it in one transaction, which decides whether
/// the next step suggests it.
pub(in crate::import_wizard) fn done_view(
    report: &ScriptReport,
    transaction_offered: bool,
) -> DoneView {
    if report.disposition == Disposition::Unresolved {
        return unresolved_view(report);
    }

    match report.end {
        RunEnd::Finished if report.failures.is_empty() => finished_view(report),
        RunEnd::Finished => continued_view(report),
        RunEnd::Cancelled => cancelled_view(report),
        RunEnd::StoppedOnError => stopped_view(report, transaction_offered),
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

fn finished_view(report: &ScriptReport) -> DoneView {
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

pub(in crate::import_wizard) fn resolve_script_outcome(
    result: Result<ScriptReport, ScriptError>,
    transaction_offered: bool,
) -> ScriptResolution {
    let report = match result {
        Ok(report) => report,
        Err(error) => return not_run_resolution(&error),
    };
    let view = done_view(&report, transaction_offered);
    let succeeded = report.end == RunEnd::Finished
        && report.failures.is_empty()
        && report.disposition != Disposition::Unresolved;

    if succeeded {
        return ScriptResolution {
            view,
            task_action: RunTaskAction::Complete,
            report: None,
            toast_success: true,
        };
    }
    if report.end == RunEnd::Cancelled && report.disposition != Disposition::Unresolved {
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
    let message = error.to_string();
    let mut view = plain_view(
        dbflux_i18n::t!("document.import_wizard.script.done.not_run"),
        DoneTone::Danger,
    );
    view.block = Some(ErrorBlock {
        message: message.clone(),
        next_step: dbflux_i18n::t!("document.import_wizard.script.done.not_run_next"),
    });

    ScriptResolution {
        view,
        task_action: RunTaskAction::Fail(message.clone()),
        report: Some(message),
        toast_success: false,
    }
}
