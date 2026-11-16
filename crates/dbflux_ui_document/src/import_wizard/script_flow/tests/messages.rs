use std::time::Duration;

use dbflux_transfer::sql_script::{
    Disposition, RunEnd, ScriptError, ScriptReport, ScriptTransaction, StatementFailure,
};

use super::super::messages::{
    DoneTone, done_view, done_view_at, load_error_block, resolve_script_outcome,
};
use crate::import_wizard::RunTaskAction;

fn report(end: RunEnd, disposition: Disposition) -> ScriptReport {
    ScriptReport {
        end,
        disposition,
        total: 12_481,
        ran: 12_481,
        failures: Vec::new(),
        stopped_at: None,
        transaction_used: disposition != Disposition::Kept,
        ddl_not_transactional: false,
        first_implicit_commit: None,
        script_transaction: ScriptTransaction::None,
        finish_error: None,
        elapsed: Duration::from_millis(8_400),
    }
}

fn failure(index: usize, line: usize, snippet: &str, error: &str) -> StatementFailure {
    StatementFailure {
        index,
        line,
        snippet: snippet.to_string(),
        error: error.to_string(),
    }
}

fn stopped(disposition: Disposition) -> ScriptReport {
    ScriptReport {
        ran: 4_120,
        failures: vec![failure(
            4_121,
            4_133,
            "INSERT INTO orders VALUES (4120)",
            "duplicate key",
        )],
        stopped_at: Some(4_121),
        ..report(RunEnd::StoppedOnError, disposition)
    }
}

#[test]
fn a_committed_run_says_the_changes_are_saved() {
    let view = done_view(&report(RunEnd::Finished, Disposition::Committed), true);

    assert_eq!(
        view.headline,
        "Script finished: 12,481 statements ran in 8.4 s"
    );
    assert_eq!(view.tone, DoneTone::Success);
    assert_eq!(
        view.outcome.as_deref(),
        Some("Committed: the changes are saved.")
    );
    assert_eq!(view.next_step, None);
    assert_eq!(view.block, None);
}

#[test]
fn a_run_without_a_transaction_says_each_statement_was_saved() {
    let view = done_view(&report(RunEnd::Finished, Disposition::Kept), false);

    assert_eq!(view.tone, DoneTone::Success);
    assert_eq!(
        view.outcome.as_deref(),
        Some("Every statement was saved as it ran.")
    );
}

#[test]
fn a_cancelled_transaction_is_rolled_back_with_the_sequence_caveat() {
    let cancelled = ScriptReport {
        ran: 4_120,
        stopped_at: Some(4_121),
        ..report(RunEnd::Cancelled, Disposition::RolledBack)
    };
    let view = done_view(&cancelled, true);

    assert_eq!(
        view.headline,
        "Script cancelled after 4,120 of 12,481 statements"
    );
    assert_eq!(view.tone, DoneTone::Warning);
    assert_eq!(
        view.outcome.as_deref(),
        Some(
            "Rolled back: the transactional changes were undone, so nothing was saved. Sequence changes (setval, nextval) are not undone by a rollback."
        )
    );
    assert_eq!(
        view.next_step.as_deref(),
        Some("Choose the file and run it again when you are ready.")
    );
}

#[test]
fn a_cancelled_run_without_a_transaction_says_how_many_were_kept() {
    let cancelled = ScriptReport {
        ran: 4_120,
        stopped_at: Some(4_121),
        ..report(RunEnd::Cancelled, Disposition::Kept)
    };
    let view = done_view(&cancelled, false);

    assert_eq!(
        view.outcome.as_deref(),
        Some("4,120 statements ran and were kept: each was saved as it ran.")
    );
    assert_eq!(
        view.next_step.as_deref(),
        Some(
            "Delete them from the file before you run it again, or they run twice. Keep or restore the session settings the script needs (for example SET search_path or SET NAMES) for the rest."
        )
    );

    let nothing = ScriptReport {
        ran: 0,
        ..cancelled
    };
    assert_eq!(
        done_view(&nothing, false).outcome.as_deref(),
        Some("No statement ran, so nothing changed.")
    );
}

#[test]
fn a_stopped_transaction_names_the_statement_and_says_it_was_rolled_back() {
    let view = done_view(&stopped(Disposition::RolledBack), true);

    assert_eq!(view.headline, "Script stopped at statement 4,121 of 12,481");
    assert_eq!(view.tone, DoneTone::Danger);

    let row = view.failed_statement.expect("failed statement");
    assert_eq!(row.label, "Failed statement · line 4,133");
    assert_eq!(row.snippet, "INSERT INTO orders VALUES (4120)");
    assert_eq!(
        view.error_text.as_deref(),
        Some("Database error: duplicate key")
    );
    assert_eq!(
        view.outcome.as_deref(),
        Some(
            "Rolled back: the transactional changes of the 4,120 statements before it were undone. Sequence changes (setval, nextval) are not undone by a rollback."
        )
    );
    assert_eq!(
        view.next_step.as_deref(),
        Some("Fix that statement in the file, then choose the file and run it again.")
    );
}

#[test]
fn a_stopped_run_without_a_transaction_says_what_ran_and_what_did_not() {
    let view = done_view(&stopped(Disposition::Kept), true);

    assert_eq!(
        view.outcome.as_deref(),
        Some(
            "The 4,120 statements before it ran and were kept. Statements 4,121 to 12,481 did not run."
        )
    );
    assert_eq!(
        view.next_step.as_deref(),
        Some(
            "Fix statement 4,121, then reconcile the changes the first 4,120 statements already made. Before running the rest, keep or restore the session settings the script needs (for example SET search_path or SET NAMES), then run only the remaining statements, or they run twice. Turn on “Run in one transaction” next time to avoid a partial run."
        )
    );

    let no_transactions = done_view(&stopped(Disposition::Kept), false);
    assert!(
        !no_transactions
            .next_step
            .as_deref()
            .unwrap_or_default()
            .contains("Run in one transaction"),
        "a connection without transactions is not told to use one"
    );
}

#[test]
fn a_first_statement_failure_has_nothing_before_it() {
    let first = ScriptReport {
        ran: 0,
        failures: vec![failure(1, 1, "SELECT nope", "no such column")],
        stopped_at: Some(1),
        ..report(RunEnd::StoppedOnError, Disposition::Kept)
    };
    let view = done_view(&first, false);

    assert_eq!(
        view.outcome.as_deref(),
        Some("No statement ran before it. Statements 1 to 12,481 did not run.")
    );
    assert_eq!(
        view.next_step.as_deref(),
        Some("Fix statement 1, then run the script again.")
    );
}

#[test]
fn continue_after_errors_lists_every_failed_statement() {
    let continued = ScriptReport {
        ran: 12_478,
        failures: vec![
            failure(
                212,
                214,
                "INSERT INTO customers VALUES (7)",
                "duplicate key",
            ),
            failure(
                3_030,
                3_041,
                "ALTER TABLE orders DROP COLUMN x",
                "no such column",
            ),
            failure(9_877, 9_890, "UPDATE stock SET qty = 0", "check violated"),
        ],
        ..report(RunEnd::Finished, Disposition::Kept)
    };
    let view = done_view(&continued, false);

    assert_eq!(
        view.headline,
        "Script finished with errors: 12,478 of 12,481 statements ran, 3 failed"
    );
    assert_eq!(view.tone, DoneTone::Danger);
    assert_eq!(view.failures.len(), 3);
    assert_eq!(view.failures[1].label, "#3,030 · line 3,041");
    assert_eq!(view.failures[1].error, "no such column");
    assert_eq!(
        view.next_step.as_deref(),
        Some(
            "The statements that ran were kept; the 3 that failed were skipped. Review the failed statements and the ones after them that depend on them, and reconcile the partial run, and keep or restore the session settings the script needs (for example SET search_path or SET NAMES), before running the fixed statements again: a later statement may have run against data that a failed one should have changed."
        )
    );
}

#[test]
fn an_unresolved_transaction_is_a_danger_block_naming_the_error() {
    let unresolved = ScriptReport {
        finish_error: Some("Rollback failed: connection reset".to_string()),
        stopped_at: Some(10),
        ..report(RunEnd::StoppedOnError, Disposition::Unresolved)
    };
    let view = done_view(&unresolved, true);
    let block = view.block.expect("danger block");

    assert_eq!(view.tone, DoneTone::Danger);
    assert!(block.message.contains("Rollback failed: connection reset"));
    assert!(
        block.next_step.contains("database"),
        "it tells the user to check the database state: {}",
        block.next_step
    );
}

#[test]
fn a_failed_commit_says_the_transaction_could_not_be_committed() {
    let commit_failed = ScriptReport {
        finish_error: Some("Commit failed: disk full".to_string()),
        ..report(RunEnd::StoppedOnError, Disposition::RolledBack)
    };
    let view = done_view(&commit_failed, true);

    assert_eq!(
        view.headline,
        "Script stopped: the transaction could not be committed"
    );
    assert_eq!(view.error_text.as_deref(), Some("Commit failed: disk full"));
    assert!(
        view.outcome
            .as_deref()
            .unwrap_or_default()
            .starts_with("Rolled back")
    );
}

#[test]
fn schema_statements_that_commit_on_their_own_are_named_after_a_rollback() {
    let rolled_back = ScriptReport {
        ddl_not_transactional: true,
        ran: 10,
        stopped_at: Some(11),
        ..report(RunEnd::Cancelled, Disposition::RolledBack)
    };

    assert!(
        done_view(&rolled_back, true)
            .outcome
            .as_deref()
            .unwrap_or_default()
            .ends_with("CREATE, ALTER and DROP statements are saved as they run on this connection, so a rollback did not undo them.")
    );
}

#[test]
fn each_load_error_explains_and_gives_a_next_step() {
    let cases = [
        (
            ScriptError::TooLarge {
                size: 240 * 1024 * 1024,
                limit: 100 * 1024 * 1024,
            },
            "This script is 240 MB, over the 100 MB limit for running a script here.",
            "Run it with the database's own command-line client instead. Splitting it into smaller files also works, but each file runs on its own: a later file that fails cannot undo an earlier one.",
        ),
        (
            ScriptError::NotUtf8 { position: 1_204 },
            "Cannot read the script: it is not valid UTF-8 (invalid byte at position 1,204). SQL scripts are read as UTF-8 and have no encoding option.",
            "Save the file as UTF-8 in your editor and choose it again.",
        ),
        (
            ScriptError::Unreadable {
                path: "/dumps/shop.sql".to_string(),
                message: "Permission denied (os error 13)".to_string(),
            },
            "Cannot read the file: /dumps/shop.sql: Permission denied (os error 13)",
            "Check that you can read the file, or choose another one.",
        ),
        (
            ScriptError::Empty,
            "The script has no statements: the file is empty or holds only comments.",
            "Choose a file that has at least one statement.",
        ),
        (
            ScriptError::NotSql {
                language: "MongoDB".to_string(),
            },
            "Cannot run a SQL script on this connection: it uses the MongoDB query language, not SQL.",
            "Connect to a SQL connection and open the import there, or choose a CSV, JSON or JSON Lines file to import data into this connection.",
        ),
    ];

    for (error, message, next_step) in cases {
        let block = load_error_block(&error);

        assert_eq!(block.message, message, "{error:?}");
        assert_eq!(block.next_step, next_step, "{error:?}");
    }
}

#[test]
fn an_error_the_flow_does_not_know_keeps_the_engines_message() {
    let block = load_error_block(&ScriptError::TransactionUnavailable);

    assert!(block.message.contains("One transaction is not available"));
    assert!(!block.next_step.is_empty());
}

#[test]
fn a_finished_run_completes_the_task_and_toasts() {
    let resolution =
        resolve_script_outcome(Ok(report(RunEnd::Finished, Disposition::Committed)), true);

    assert!(matches!(resolution.task_action, RunTaskAction::Complete));
    assert!(resolution.toast_success);
    assert_eq!(resolution.report, None);
}

#[test]
fn a_cancelled_run_cancels_the_task_without_an_error() {
    let cancelled = ScriptReport {
        stopped_at: Some(2),
        ..report(RunEnd::Cancelled, Disposition::RolledBack)
    };
    let resolution = resolve_script_outcome(Ok(cancelled), true);

    assert!(matches!(resolution.task_action, RunTaskAction::Cancel));
    assert!(!resolution.toast_success);
    assert_eq!(resolution.report, None);
}

#[test]
fn a_stopped_run_fails_the_task_and_reports_once() {
    let resolution = resolve_script_outcome(Ok(stopped(Disposition::RolledBack)), true);

    assert!(matches!(resolution.task_action, RunTaskAction::Fail(_)));
    assert_eq!(
        resolution.report.as_deref(),
        Some("Script stopped at statement 4,121 of 12,481: duplicate key")
    );
    assert!(!resolution.toast_success);
}

#[test]
fn a_run_that_could_not_start_shows_the_reason_and_that_nothing_ran() {
    let resolution = resolve_script_outcome(
        Err(ScriptError::TransactionStart {
            message: "locked".to_string(),
        }),
        true,
    );
    let block = resolution.view.block.expect("danger block");

    assert!(matches!(resolution.task_action, RunTaskAction::Fail(_)));
    assert!(block.message.contains("Could not start the transaction"));
    assert_eq!(
        block.next_step,
        "Nothing ran. Fix the cause above and run the script again."
    );
    assert!(resolution.report.is_some());
}

#[test]
fn each_refused_script_content_has_localized_copy_and_a_next_step() {
    let cases = [
        (
            ScriptError::ClientDirective {
                directive: "DELIMITER".to_string(),
                line: 12,
            },
            "This script uses the client command DELIMITER (line 12), which only a database command-line client understands.",
            "Run the script with the database's own command-line client, or remove the DELIMITER lines and end each statement with a semicolon.",
        ),
        (
            ScriptError::SplitRoutineBody { line: 30 },
            "This script has a routine body starting on line 30 whose statements end with \";\", so it cannot be split into statements here.",
            "Run the script with the database's own command-line client.",
        ),
        (
            ScriptError::CopyFromStdin { line: 48 },
            "This script has a COPY … FROM stdin data block (line 48), which only the psql command-line client can load.",
            "Run it with psql instead.",
        ),
        (
            ScriptError::PsqlMetaCommand {
                command: "\\connect".to_string(),
                line: 3,
            },
            "This script uses the psql command \\connect (line 3), which only the psql command-line client understands.",
            "Run the script with psql instead: removing it would run the remaining statements on this connection, which may not be the database it selects.",
        ),
        (
            ScriptError::SelfManagedTransaction {
                keyword: "COMMIT".to_string(),
                line: 214,
            },
            "This script manages its own transactions (COMMIT on line 214), so it cannot run in one transaction.",
            "Turn off “Run in one transaction”; the script's own statements then decide what is saved.",
        ),
    ];

    for (error, message, next_step) in cases {
        let block = load_error_block(&error);

        assert_eq!(block.message, message, "{error:?}");
        assert_eq!(block.next_step, next_step, "{error:?}");
    }
}

#[test]
fn a_stop_without_a_transaction_tells_the_user_to_reconcile_and_keep_session_settings() {
    let view = done_view(&stopped(Disposition::Kept), false);

    assert_eq!(
        view.next_step.as_deref(),
        Some(
            "Fix statement 4,121, then reconcile the changes the first 4,120 statements already made. Before running the rest, keep or restore the session settings the script needs (for example SET search_path or SET NAMES), then run only the remaining statements, or they run twice."
        )
    );
}

#[test]
fn continue_with_failures_says_to_reconcile_before_running_the_fixed_statements() {
    let continued = ScriptReport {
        ran: 9,
        failures: vec![failure(2, 2, "INSERT INTO t VALUES (1)", "duplicate key")],
        ..report(RunEnd::Finished, Disposition::Kept)
    };

    assert_eq!(
        done_view(&continued, false).next_step.as_deref(),
        Some(
            "The statements that ran were kept; the 1 that failed were skipped. Review the failed statements and the ones after them that depend on them, and reconcile the partial run, and keep or restore the session settings the script needs (for example SET search_path or SET NAMES), before running the fixed statements again: a later statement may have run against data that a failed one should have changed."
        )
    );
}

const PARTLY: &str = "Partly saved: everything up to statement 7 (line 9) was saved, because it commits on its own on this connection, and later statements may have been saved too.";
const PARTLY_NEXT: &str = "Check the database and reconcile what was saved before you run the script again, or the statements that were saved run twice.";

fn partially_kept(end: RunEnd) -> ScriptReport {
    ScriptReport {
        ddl_not_transactional: true,
        first_implicit_commit: Some(7),
        ran: 7,
        stopped_at: Some(8),
        ..report(end, Disposition::PartiallyKept)
    }
}

#[test]
fn a_partial_commit_never_says_nothing_was_saved() {
    for end in [RunEnd::Cancelled, RunEnd::StoppedOnError] {
        let mut report = partially_kept(end);
        if end == RunEnd::StoppedOnError {
            report.failures = vec![failure(8, 10, "INSERT INTO t VALUES (1)", "boom")];
        }
        let view = done_view_at(&report, true, Some(9));
        let outcome = view.outcome.clone().unwrap_or_default();

        assert_eq!(outcome, PARTLY, "{end:?}");
        assert_eq!(view.next_step.as_deref(), Some(PARTLY_NEXT), "{end:?}");
        assert!(!outcome.contains("nothing was saved"));
    }
}

#[test]
fn a_partial_commit_without_a_known_line_omits_it() {
    let view = done_view(&partially_kept(RunEnd::Cancelled), true);

    assert_eq!(
        view.outcome.as_deref(),
        Some(
            "Partly saved: everything up to statement 7 was saved, because it commits on its own on this connection, and later statements may have been saved too."
        )
    );
}

#[test]
fn a_script_that_ends_without_commit_says_it_was_rolled_back() {
    let ended = ScriptReport {
        script_transaction: ScriptTransaction::EndedWithoutCommit,
        ..report(RunEnd::Finished, Disposition::Kept)
    };
    let view = done_view(&ended, false);

    assert_eq!(view.tone, DoneTone::Warning);
    assert_eq!(
        view.outcome.as_deref(),
        Some("The script ended without COMMIT, so its open transaction was rolled back.")
    );
    assert_eq!(
        view.next_step.as_deref(),
        Some("Add a COMMIT at the end of the script to save its changes.")
    );
    assert_eq!(
        done_view(&ended, true).next_step.as_deref(),
        Some(
            "Add a COMMIT at the end of the script to save its changes, or turn on “Run in one transaction”."
        )
    );

    let resolution = resolve_script_outcome(Ok(ended), false);
    assert!(
        !resolution.toast_success,
        "nothing was saved: no success toast"
    );
}

const OWN_TX_OUTCOME: &str = "Statements before the script's own BEGIN were saved as they ran. The script's open transaction was rolled back, so what it had changed there was not saved.";

#[test]
fn a_stop_inside_the_scripts_own_transaction_does_not_claim_everything_before_it_was_kept() {
    let rolled = ScriptReport {
        script_transaction: ScriptTransaction::RolledBack,
        ..stopped(Disposition::Kept)
    };
    let view = done_view(&rolled, false);
    let outcome = view.outcome.clone().unwrap_or_default();

    assert_eq!(outcome, OWN_TX_OUTCOME);
    assert!(!outcome.contains("were kept"));
    assert_eq!(
        view.next_step.as_deref(),
        Some(
            "Fix statement 4,121, then check which statements before it were saved (only those before the script's own BEGIN were) and reconcile the changes. Keep or restore the session settings the script needs (for example SET search_path or SET NAMES), then run only what is missing, or the saved statements run twice."
        )
    );
}

#[test]
fn a_cancel_inside_the_scripts_own_transaction_uses_the_same_neutral_wording() {
    let rolled = ScriptReport {
        script_transaction: ScriptTransaction::RolledBack,
        ran: 5,
        stopped_at: Some(6),
        ..report(RunEnd::Cancelled, Disposition::Kept)
    };
    let view = done_view(&rolled, false);

    assert_eq!(view.outcome.as_deref(), Some(OWN_TX_OUTCOME));
    assert_eq!(
        view.next_step.as_deref(),
        Some(
            "Check which statements were saved (only those before the script's own BEGIN were) and reconcile the changes before you run the script again, or the saved statements run twice."
        )
    );
}

#[test]
fn a_cancelled_run_still_lists_the_statements_that_failed_before_it() {
    let cancelled = ScriptReport {
        failures: vec![failure(2, 3, "INSERT INTO t VALUES (1)", "duplicate key")],
        stopped_at: Some(5),
        ran: 3,
        ..report(RunEnd::Cancelled, Disposition::Kept)
    };
    let view = done_view(&cancelled, false);

    assert_eq!(
        view.headline,
        "Script cancelled after 3 of 12,481 statements"
    );
    assert_eq!(view.failures.len(), 1);
    assert_eq!(view.failures[0].label, "#2 · line 3");
    assert_eq!(view.failures[0].error, "duplicate key");
}

#[test]
fn a_size_that_rounds_to_the_limit_is_shown_in_bytes() {
    let block = load_error_block(&ScriptError::TooLarge {
        size: 100 * 1024 * 1024 + 1,
        limit: 100 * 1024 * 1024,
    });

    assert_eq!(
        block.message,
        "This script is 104,857,601 B, over the 104,857,600 B limit for running a script here."
    );
}

#[test]
fn a_script_transaction_that_could_not_be_rolled_back_is_a_danger_block() {
    let open = ScriptReport {
        script_transaction: ScriptTransaction::LeftOpen,
        finish_error: Some("Rollback failed: connection reset".to_string()),
        ..report(RunEnd::Finished, Disposition::Kept)
    };
    let view = done_view(&open, false);
    let block = view.block.expect("danger block");

    assert_eq!(view.tone, DoneTone::Danger);
    assert_eq!(view.headline, "Script ended with a transaction left open");
    assert_eq!(
        block.message,
        "The script's transaction could not be rolled back: Rollback failed: connection reset"
    );
    assert_eq!(
        block.next_step,
        "A transaction may still be open on this connection. Disconnect and reconnect to end it, and check the state of the database before you run anything else."
    );

    let resolution = resolve_script_outcome(Ok(open), false);
    assert!(matches!(resolution.task_action, RunTaskAction::Fail(_)));
    assert!(resolution.report.is_some());
}

#[test]
fn a_script_that_switches_off_autocommit_is_refused_with_localized_copy() {
    let block = load_error_block(&ScriptError::SessionCommitMode {
        change: "turns off autocommit",
        statement: "SET autocommit = 0".to_string(),
        line: 4,
    });

    assert_eq!(
        block.message,
        "This script turns off autocommit on line 4 (SET autocommit = 0), which would leave this connection in manual-commit mode after the run."
    );
    assert_eq!(
        block.next_step,
        "Remove that statement, or run the script with the database's own command-line client."
    );

    let implicit = load_error_block(&ScriptError::SessionCommitMode {
        change: "turns on implicit transactions",
        statement: "SET IMPLICIT_TRANSACTIONS ON".to_string(),
        line: 1,
    });
    assert!(
        implicit
            .message
            .starts_with("This script turns on implicit transactions on line 1")
    );
}

#[test]
fn a_savepoint_outside_a_transaction_explains_the_precautionary_rollback() {
    let precaution = ScriptReport {
        script_transaction: ScriptTransaction::PossiblyOpenRolledBack,
        ..report(RunEnd::Finished, Disposition::Kept)
    };
    let outcome = done_view(&precaution, false).outcome.unwrap_or_default();

    assert!(
        outcome.ends_with(
            "The script used SAVEPOINT outside a transaction, so DBFlux issued a ROLLBACK in case one was left open. Changes the database had already committed stay."
        ),
        "{outcome}"
    );
    assert!(outcome.starts_with("Every statement was saved as it ran."));
}

#[test]
fn a_version_conditional_transaction_comment_is_refused_with_localized_copy() {
    let block = load_error_block(&ScriptError::ConditionalTransactionControl { line: 7 });

    assert_eq!(
        block.message,
        "This script has a version-conditional comment (line 7) that controls transactions, and whether it runs depends on the server."
    );
    assert_eq!(
        block.next_step,
        "Run the script with the database's own command-line client."
    );
}
