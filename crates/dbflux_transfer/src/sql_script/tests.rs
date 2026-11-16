use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use dbflux_core::{
    CancelToken, Connection, ConnectionProfile, DangerousQueryKind, DbConfig, DbDriver, DbError,
    DbKind, DriverCapabilities, DriverMetadata, ExecutionClassification, QueryHandle,
    QueryLanguage, QueryRequest, QueryResult, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
    Value, classify_query_for_language,
};
use dbflux_driver_sqlite::SqliteDriver;

use super::control::TransactionTracker;
use super::load::{ReadError, read_limited};
use super::routine::changes_a_sequence;
use super::*;

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "dbflux_transfer_sql_script_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        Self(dir)
    }

    fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, bytes).expect("write scratch file");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

/// SQLite does not declare `DriverCapabilities::TRANSACTIONS`; this wrapper
/// declares it so the transaction paths can be exercised without a server.
struct WithTransactions(Box<dyn Connection>, bool);

impl Connection for WithTransactions {
    fn metadata(&self) -> &DriverMetadata {
        self.0.metadata()
    }

    fn capabilities(&self) -> DriverCapabilities {
        self.0.capabilities() | DriverCapabilities::TRANSACTIONS
    }

    fn ping(&self) -> Result<(), DbError> {
        self.0.ping()
    }

    fn close(&mut self) -> Result<(), DbError> {
        self.0.close()
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.0.execute(req)
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        self.0.cancel(handle)
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        self.0.schema()
    }

    fn kind(&self) -> DbKind {
        self.0.kind()
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        self.0.schema_loading_strategy()
    }

    fn dialect(&self) -> &dyn SqlDialect {
        self.0.dialect()
    }

    fn supports_transactional_ddl(&self) -> bool {
        self.1
    }
}

fn sqlite(scratch: &Scratch) -> Box<dyn Connection> {
    Box::new(WithTransactions(plain_sqlite(scratch), true))
}

fn plain_sqlite(scratch: &Scratch) -> Box<dyn Connection> {
    let profile = ConnectionProfile::new(
        "sql-script-test",
        DbConfig::SQLite {
            path: scratch.0.join("target.db"),
            connection_id: None,
        },
    );
    SqliteDriver::new().connect(&profile).expect("connect")
}

fn exec(connection: &dyn Connection, sql: &str) {
    connection.execute(&QueryRequest::new(sql)).expect(sql);
}

fn row_count(connection: &dyn Connection, table: &str) -> i64 {
    let result = connection
        .execute(&QueryRequest::new(format!("SELECT COUNT(*) FROM {table}")))
        .expect("count");

    match result.rows.first().and_then(|row| row.first()) {
        Some(Value::Int(count)) => *count,
        other => panic!("unexpected count value: {other:?}"),
    }
}

fn script(text: &str) -> SqlScript {
    SqlScript::from_text(text, &QueryLanguage::Sql).expect("script")
}

fn run(
    connection: &dyn Connection,
    script: &SqlScript,
    options: RunOptions,
) -> Result<ScriptReport, ScriptError> {
    run_script(connection, script, &options, |_| {}, &CancelToken::new())
}

const THREE_INSERTS: &str =
    "INSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2);\nINSERT INTO t VALUES (3);\n";

// ---------------------------------------------------------------- loading

#[test]
fn a_script_over_the_limit_is_refused_with_its_size() {
    let scratch = Scratch::new();
    let path = scratch.0.join("big.sql");
    let file = std::fs::File::create(&path).expect("create");
    file.set_len(MAX_SCRIPT_BYTES + 1).expect("sparse file");

    let error = load_script(&path, &QueryLanguage::Sql).expect_err("too large");

    assert_eq!(
        error,
        ScriptError::TooLarge {
            size: MAX_SCRIPT_BYTES + 1,
            limit: MAX_SCRIPT_BYTES
        }
    );
    assert!(
        error
            .to_string()
            .contains(&(MAX_SCRIPT_BYTES + 1).to_string())
    );
}

#[test]
fn a_script_that_is_not_utf8_names_the_byte_position() {
    let scratch = Scratch::new();
    let path = scratch.write("bad.sql", b"SELECT 1;\n\xFF\xFE");

    let error = load_script(&path, &QueryLanguage::Sql).expect_err("not utf-8");

    assert_eq!(error, ScriptError::NotUtf8 { position: 11 });
    assert!(error.to_string().contains("position 11"), "{error}");
}

#[test]
fn an_unreadable_file_names_the_path() {
    let scratch = Scratch::new();
    let path = scratch.0.join("absent.sql");

    let error = load_script(&path, &QueryLanguage::Sql).expect_err("unreadable");

    assert!(matches!(error, ScriptError::Unreadable { .. }), "{error:?}");
    assert!(error.to_string().contains("absent.sql"), "{error}");
}

#[test]
fn an_empty_or_comments_only_script_has_no_statements() {
    for text in [
        "",
        "  \n\t\n",
        ";;\n;",
        "-- only a comment\n",
        "/* block; */ -- and a line\n;\n",
    ] {
        let error = SqlScript::from_text(text, &QueryLanguage::Sql).expect_err(text);
        assert_eq!(error, ScriptError::Empty, "{text:?}");
    }
}

#[test]
fn a_non_sql_language_is_refused() {
    let error =
        SqlScript::from_text("db.users.find({})", &QueryLanguage::MongoQuery).expect_err("not sql");

    assert!(matches!(error, ScriptError::NotSql { .. }), "{error:?}");
}

#[test]
fn statements_split_around_quotes_comments_and_dollar_quotes_with_start_lines() {
    let text = "CREATE TABLE t (id INTEGER, name TEXT);\n\
                INSERT INTO t VALUES (1, 'a;b');\n\
                \n\
                -- note; with semicolon\n\
                INSERT INTO t VALUES (2, \"q;x\");\n\
                /* c; */ UPDATE t SET name = 'z'\n\
                  WHERE id = 1;\n\
                CREATE FUNCTION f() RETURNS int AS $body$ BEGIN; RETURN 1; END; $body$ LANGUAGE plpgsql;\n\
                -- trailing\n";

    let script = script(text);
    let found: Vec<(usize, &str)> = script
        .statements()
        .iter()
        .map(|statement| (statement.line, statement.text.as_str()))
        .collect();

    assert_eq!(
        found,
        vec![
            (1, "CREATE TABLE t (id INTEGER, name TEXT)"),
            (2, "INSERT INTO t VALUES (1, 'a;b')"),
            (5, "INSERT INTO t VALUES (2, \"q;x\")"),
            (6, "UPDATE t SET name = 'z'\nWHERE id = 1"),
            (
                8,
                "CREATE FUNCTION f() RETURNS int AS $body$ BEGIN; RETURN 1; END; $body$ LANGUAGE plpgsql"
            ),
        ]
    );
    assert_eq!(script.bytes(), text.len() as u64);
}

#[test]
fn a_utf8_byte_order_mark_is_not_part_of_the_first_statement() {
    let scratch = Scratch::new();
    let path = scratch.write("bom.sql", b"\xEF\xBB\xBFSELECT 1;\n");

    let script = load_script(&path, &QueryLanguage::Sql).expect("load");

    assert_eq!(script.statements()[0].text, "SELECT 1");
    assert_eq!(script.statements()[0].line, 1);
}

#[test]
fn a_conditional_comment_is_an_executable_statement() {
    let script = script("/*!40101 SET NAMES utf8 */;\nSELECT 1;");

    assert_eq!(script.statement_count(), 2);
    assert_eq!(script.statements()[0].text, "/*!40101 SET NAMES utf8 */");
}

#[test]
fn a_snippet_is_the_first_line_cut_short() {
    let script = script("UPDATE t SET name = 'z'\n  WHERE id = 1;");

    assert_eq!(script.statements()[0].snippet(), "UPDATE t SET name = 'z'…");
}

// ---------------------------------------------------------------- summary

#[test]
fn the_summary_classifies_each_statement_not_the_whole_script() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    let script = script(
        "CREATE TABLE t (id INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         SELECT * FROM t;\n\
         UPDATE t SET id = 2;\n\
         DELETE FROM t;\n\
         DROP TABLE t;\n\
         TRUNCATE TABLE t;\n",
    );

    let summary = script.summarize(connection.as_ref(), 2);

    assert_eq!(summary.statement_count, 7);
    assert_eq!(summary.bytes, script.bytes());

    let total: usize = summary.by_classification.iter().map(|(_, n)| n).sum();
    assert_eq!(total, 7);
    for (classification, count) in &summary.by_classification {
        let expected = script
            .statements()
            .iter()
            .filter(|s| {
                classify_query_for_language(&QueryLanguage::Sql, &s.text) == *classification
            })
            .count();
        assert_eq!(*count, expected, "{classification:?}");
    }

    let danger = |kind: DangerousQueryKind| {
        summary
            .by_danger
            .iter()
            .find(|(found, _)| *found == kind)
            .map(|(_, count)| *count)
    };
    assert_eq!(danger(DangerousQueryKind::UpdateNoWhere), Some(1));
    assert_eq!(danger(DangerousQueryKind::DeleteNoWhere), Some(1));
    assert_eq!(danger(DangerousQueryKind::Drop), Some(1));
    assert_eq!(danger(DangerousQueryKind::Truncate), Some(1));
    assert_eq!(danger(DangerousQueryKind::Script), None);

    assert_eq!(summary.destructive_count, 4);
    let first: Vec<usize> = summary.first_destructive.iter().map(|s| s.index).collect();
    assert_eq!(first, vec![4, 5], "limited to 2, in script order");
    assert_eq!(summary.preview.len(), 2);
    assert_eq!(summary.preview[0].index, 1);
    assert_eq!(summary.preview[1].snippet, "INSERT INTO t VALUES (1)");
    assert_eq!(summary.changing_count, 6);
    assert!(summary.ddl_count >= 1);
    assert!(
        summary
            .by_classification
            .iter()
            .any(|(c, _)| *c == ExecutionClassification::Read)
    );
}

// ---------------------------------------------------------------- running

#[test]
fn a_successful_run_in_one_transaction_commits() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");

    let report = run(
        connection.as_ref(),
        &script(THREE_INSERTS),
        RunOptions {
            transaction: true,
            ..RunOptions::default()
        },
    )
    .expect("run");

    assert_eq!(report.end, RunEnd::Finished);
    assert_eq!(report.disposition, Disposition::Committed);
    assert_eq!((report.total, report.ran), (3, 3));
    assert!(report.failures.is_empty());
    assert!(report.transaction_used);
    assert_eq!(row_count(connection.as_ref(), "t"), 3);
}

#[test]
fn a_run_without_a_transaction_keeps_what_it_ran() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");

    let report = run(
        connection.as_ref(),
        &script(THREE_INSERTS),
        RunOptions::default(),
    )
    .expect("run");

    assert_eq!(report.end, RunEnd::Finished);
    assert_eq!(report.disposition, Disposition::Kept);
    assert!(!report.transaction_used);
    assert_eq!(row_count(connection.as_ref(), "t"), 3);
}

#[test]
fn stopping_on_an_error_in_a_transaction_rolls_everything_back() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script(
        "INSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2);\n\nINSERT INTO missing VALUES (3);\nINSERT INTO t VALUES (4);\n",
    );

    let report = run(
        connection.as_ref(),
        &script,
        RunOptions {
            transaction: true,
            ..RunOptions::default()
        },
    )
    .expect("run");

    assert_eq!(report.end, RunEnd::StoppedOnError);
    assert_eq!(report.disposition, Disposition::RolledBack);
    assert_eq!(report.ran, 2);
    assert_eq!(report.stopped_at, Some(3));
    assert_eq!(report.failures.len(), 1);
    let failure = &report.failures[0];
    assert_eq!((failure.index, failure.line), (3, 4));
    assert_eq!(failure.snippet, "INSERT INTO missing VALUES (3)");
    assert!(failure.error.contains("missing"), "{}", failure.error);
    assert_eq!(row_count(connection.as_ref(), "t"), 0);
}

#[test]
fn stopping_on_an_error_without_a_transaction_keeps_the_earlier_statements() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script(
        "INSERT INTO t VALUES (1);\nINSERT INTO missing VALUES (2);\nINSERT INTO t VALUES (3);\n",
    );

    let report = run(connection.as_ref(), &script, RunOptions::default()).expect("run");

    assert_eq!(report.end, RunEnd::StoppedOnError);
    assert_eq!(report.disposition, Disposition::Kept);
    assert_eq!((report.ran, report.stopped_at), (1, Some(2)));
    assert_eq!(row_count(connection.as_ref(), "t"), 1);
}

#[test]
fn continuing_after_errors_collects_every_failure_and_keeps_the_rest() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script(
        "INSERT INTO t VALUES (1);\nINSERT INTO missing VALUES (2);\nINSERT INTO t VALUES (3);\nBOGUS;\n",
    );

    let report = run(
        connection.as_ref(),
        &script,
        RunOptions {
            on_error: OnError::Continue,
            ..RunOptions::default()
        },
    )
    .expect("run");

    assert_eq!(report.end, RunEnd::Finished);
    assert_eq!(report.disposition, Disposition::Kept);
    assert_eq!((report.total, report.ran), (4, 2));
    let failed: Vec<usize> = report.failures.iter().map(|f| f.index).collect();
    assert_eq!(failed, vec![2, 4]);
    assert_eq!(row_count(connection.as_ref(), "t"), 2);
}

#[test]
fn cancelling_stops_after_the_current_statement_and_rolls_back_a_transaction() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let cancel = CancelToken::new();

    let report = run_script(
        connection.as_ref(),
        &script(THREE_INSERTS),
        &RunOptions {
            transaction: true,
            ..RunOptions::default()
        },
        |progress| {
            if progress.index == 2 {
                cancel.cancel();
            }
        },
        &cancel,
    )
    .expect("run");

    assert_eq!(report.end, RunEnd::Cancelled);
    assert_eq!(report.disposition, Disposition::RolledBack);
    assert_eq!((report.ran, report.stopped_at), (2, Some(3)));
    assert!(report.failures.is_empty());
    assert_eq!(row_count(connection.as_ref(), "t"), 0);
}

#[test]
fn cancelling_without_a_transaction_keeps_the_statements_that_ran() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let cancel = CancelToken::new();

    let report = run_script(
        connection.as_ref(),
        &script(THREE_INSERTS),
        &RunOptions::default(),
        |progress| {
            if progress.index == 2 {
                cancel.cancel();
            }
        },
        &cancel,
    )
    .expect("run");

    assert_eq!(report.end, RunEnd::Cancelled);
    assert_eq!(report.disposition, Disposition::Kept);
    assert_eq!(row_count(connection.as_ref(), "t"), 2);
}

#[test]
fn progress_reports_each_statement_before_it_runs() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let mut seen = Vec::new();

    run_script(
        connection.as_ref(),
        &script(THREE_INSERTS),
        &RunOptions::default(),
        |progress| seen.push(progress.clone()),
        &CancelToken::new(),
    )
    .expect("run");

    let positions: Vec<(usize, usize)> = seen.iter().map(|p| (p.index, p.total)).collect();
    assert_eq!(positions, vec![(1, 3), (2, 3), (3, 3)]);
    assert_eq!(seen[1].snippet, "INSERT INTO t VALUES (2)");
}

#[test]
fn a_transaction_with_continue_after_errors_is_rejected_before_anything_runs() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");

    let error = run(
        connection.as_ref(),
        &script(THREE_INSERTS),
        RunOptions {
            on_error: OnError::Continue,
            transaction: true,
            ..RunOptions::default()
        },
    )
    .expect_err("rejected");

    assert_eq!(error, ScriptError::TransactionWithContinue);
    assert!(error.to_string().contains("Stop at the first error"));
    assert_eq!(row_count(connection.as_ref(), "t"), 0);
}

#[test]
fn a_transaction_on_a_connection_without_transactions_is_refused_with_the_reason() {
    let scratch = Scratch::new();
    let connection = plain_sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");

    let error = run(
        connection.as_ref(),
        &script(THREE_INSERTS),
        RunOptions {
            transaction: true,
            ..RunOptions::default()
        },
    )
    .expect_err("refused");

    assert_eq!(error, ScriptError::TransactionUnavailable);
    assert!(error.to_string().contains("does not support transactions"));
    assert_eq!(row_count(connection.as_ref(), "t"), 0);
}

#[test]
fn a_read_only_profile_refuses_a_script_that_changes_data() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let options = RunOptions {
        read_only_profile: true,
        ..RunOptions::default()
    };

    let error = run(connection.as_ref(), &script(THREE_INSERTS), options).expect_err("refused");
    assert_eq!(error, ScriptError::ReadOnlyProfile { changing: 3 });
    assert_eq!(row_count(connection.as_ref(), "t"), 0);

    let report = run(connection.as_ref(), &script("SELECT * FROM t;"), options)
        .expect("a reading script runs");
    assert_eq!(report.ran, 1);
}

// ------------------------------------------------- client directives

fn refused(text: &str) -> (String, usize) {
    match SqlScript::from_text(text, &QueryLanguage::Sql) {
        Err(ScriptError::ClientDirective { directive, line }) => (directive, line),
        other => panic!("expected a client directive error for {text:?}, got {other:?}"),
    }
}

#[test]
fn a_delimiter_directive_is_refused_naming_it_and_its_line() {
    let text =
        "SELECT 1;\n  delimiter //\nCREATE PROCEDURE p() BEGIN SELECT 1; END//\nDELIMITER ;\n";

    assert_eq!(refused(text), ("DELIMITER".to_string(), 2));
}

#[test]
fn a_go_batch_separator_is_refused_with_or_without_a_count() {
    assert_eq!(refused("SELECT 1\nGO\nSELECT 2\n"), ("GO".to_string(), 2));
    assert_eq!(refused("SELECT 1;\n\n go 5 \n"), ("GO".to_string(), 3));
    assert_eq!(refused("SELECT 1\r\ngo\r\n"), ("GO".to_string(), 2));
}

#[test]
fn directive_words_inside_text_are_not_directives() {
    let text = "SELECT 'a\nGO\nb';\n\
                -- GO\n\
                /* DELIMITER //\nGO\n*/\n\
                SELECT $$\nGO\n$$;\n\
                SELECT go, delimiter FROM t WHERE go = 1;\n\
                SELECT\n  gopher\nFROM t;\n";

    let script = SqlScript::from_text(text, &QueryLanguage::Sql).expect("no directive");

    assert_eq!(script.statement_count(), 4);
}

#[test]
fn the_directive_error_message_says_what_to_do() {
    let error = ScriptError::ClientDirective {
        directive: "DELIMITER".to_string(),
        line: 7,
    };

    assert_eq!(
        error.to_string(),
        "The script uses the DELIMITER directive on line 7. It is a command of a command-line client, not SQL, so it cannot be run here. Remove it and end each statement with \";\", or run the script with the database's own command-line client."
    );
}

// ------------------------------------------ self-managed transactions

fn control_of(text: &str) -> Option<(usize, usize, &'static str)> {
    script(text)
        .transaction_control()
        .map(|control| (control.index, control.line, control.keyword))
}

#[test]
fn transaction_control_statements_are_detected_case_insensitively_after_comments() {
    let cases = [
        ("BEGIN;", "BEGIN"),
        ("begin transaction;", "BEGIN"),
        ("BEGIN IMMEDIATE;", "BEGIN"),
        ("-- go\n/* x */ Start   Transaction;", "START TRANSACTION"),
        ("commit;", "COMMIT"),
        ("END;", "END"),
        ("end transaction;", "END"),
        ("ROLLBACK TO sp;", "ROLLBACK"),
        ("SAVEPOINT sp;", "SAVEPOINT"),
        ("release savepoint sp;", "RELEASE"),
        ("set @@session.autocommit=1;", "SET autocommit"),
        ("SET SESSION autocommit = 1;", "SET autocommit"),
        (
            "SET IMPLICIT_TRANSACTIONS OFF;",
            "SET IMPLICIT_TRANSACTIONS",
        ),
    ];

    for (sql, keyword) in cases {
        let text = format!("SELECT 1;\n{sql}\n");
        let found = control_of(&text);
        assert_eq!(found.map(|(_, _, k)| k), Some(keyword), "{sql}");
    }
}

#[test]
fn the_first_transaction_control_statement_reports_its_index_and_line() {
    let found = control_of("SELECT 1;\n\nINSERT INTO t VALUES (1);\n-- c\nCOMMIT;\nROLLBACK;\n");

    assert_eq!(found, Some((3, 5, "COMMIT")));
}

#[test]
fn compound_bodies_and_ordinary_statements_are_not_transaction_control() {
    for text in [
        "BEGIN\n  INSERT INTO t VALUES (1)\nEND;",
        "SET search_path = public;",
        "SET NAMES utf8;",
        "SELECT 'commit';",
        "INSERT INTO commit_log VALUES (1);",
        "SELECT 1 AS begin_value;",
    ] {
        assert_eq!(control_of(text), None, "{text}");
    }
}

#[test]
fn the_summary_exposes_the_first_transaction_control_statement() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);

    let with = script("SELECT 1;\nBEGIN;\nCOMMIT;").summarize(connection.as_ref(), 3);
    let without = script("SELECT 1;").summarize(connection.as_ref(), 3);

    let control = with.transaction_control.expect("detected");
    assert_eq!(
        (control.index, control.line, control.keyword),
        (2, 2, "BEGIN")
    );
    assert_eq!(without.transaction_control, None);
}

#[test]
fn a_script_that_manages_its_own_transactions_is_refused_in_one_transaction() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script("INSERT INTO t VALUES (1);\nBEGIN;\nINSERT INTO t VALUES (2);\nCOMMIT;\n");

    let error = run(
        connection.as_ref(),
        &script,
        RunOptions {
            transaction: true,
            ..RunOptions::default()
        },
    )
    .expect_err("refused");

    assert_eq!(
        error,
        ScriptError::SelfManagedTransaction {
            keyword: "BEGIN".to_string(),
            line: 2
        }
    );
    assert_eq!(
        error.to_string(),
        "The script manages its own transactions (BEGIN on line 2), so it cannot run in one transaction. Turn off \"Run in one transaction\"; the script's own statements then decide what is saved."
    );
    assert_eq!(row_count(connection.as_ref(), "t"), 0, "nothing ran");
}

#[test]
fn a_script_that_manages_its_own_transactions_runs_as_written_without_one() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script("BEGIN;\nINSERT INTO t VALUES (1);\nCOMMIT;\n");

    let report = run(connection.as_ref(), &script, RunOptions::default()).expect("run");

    assert_eq!(report.end, RunEnd::Finished);
    assert_eq!(report.disposition, Disposition::Kept);
    assert_eq!(row_count(connection.as_ref(), "t"), 1);
}

// ------------------------------------------------ split routine bodies

fn split_body_line(text: &str) -> Option<usize> {
    match SqlScript::from_text(text, &QueryLanguage::Sql) {
        Err(ScriptError::SplitRoutineBody { line }) => Some(line),
        Ok(_) => None,
        Err(other) => panic!("unexpected error for {text:?}: {other:?}"),
    }
}

#[test]
fn a_procedure_body_cut_at_its_semicolons_is_refused_with_its_line() {
    let text = "SELECT 1;\n\nCREATE PROCEDURE p()\nBEGIN\n  SELECT 1;\n  SELECT 2;\nEND;\n";

    assert_eq!(split_body_line(text), Some(4));
    assert_eq!(
        ScriptError::SplitRoutineBody { line: 12 }.to_string(),
        "The script has a routine body starting on line 12 whose statements end with \";\", so it cannot be split into statements here. Run the script with the database's own command-line client."
    );
}

#[test]
fn trigger_bodies_are_refused_too() {
    let text = "CREATE TRIGGER x AFTER INSERT ON t BEGIN INSERT INTO log VALUES (1); END;";

    assert_eq!(split_body_line(text), Some(1));
}

#[test]
fn a_dollar_quoted_function_is_accepted() {
    let text = "CREATE FUNCTION f() RETURNS int AS $$ BEGIN PERFORM 1; RETURN 1; END; $$ LANGUAGE plpgsql;";

    assert_eq!(split_body_line(text), None);
}

#[test]
fn transaction_scripts_and_lookalikes_are_not_split_bodies() {
    for text in [
        "BEGIN TRANSACTION;\nINSERT INTO t VALUES (1);\nCOMMIT;",
        "BEGIN;\nINSERT INTO t VALUES (1);\nEND;",
        "BEGIN IMMEDIATE;\nSELECT 1;\nCOMMIT;",
        "SELECT CASE WHEN a THEN 1 ELSE 2 END FROM t;\nSELECT CASE a WHEN 1 THEN 'begin' END;",
        "SELECT 'BEGIN';\n-- BEGIN\n/* BEGIN */ SELECT \"begin\";",
        "CREATE PROCEDURE p() BEGIN SELECT 1 END",
    ] {
        assert_eq!(split_body_line(text), None, "{text}");
    }
}

#[test]
fn a_case_expression_inside_a_body_does_not_hide_the_split() {
    let text = "CREATE PROCEDURE p() BEGIN\n  SELECT CASE WHEN a THEN 1 END;\n  IF x THEN SELECT 1; END IF;\nEND;";

    assert_eq!(split_body_line(text), Some(1));
}

// ------------------------------------------------- sequence changes

#[test]
fn a_sequence_function_call_counts_as_a_write_in_the_summary() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    let script = script(
        "SELECT setval('s', 5);\nSELECT NEXTVAL ('s');\nSELECT 'setval(';\nSELECT 1; -- setval(\nALTER SEQUENCE s RESTART;\nSELECT setval_count FROM t;",
    );

    let summary = script.summarize(connection.as_ref(), 10);

    let classes: Vec<_> = summary.preview.iter().map(|s| s.classification).collect();
    assert_eq!(classes[0], ExecutionClassification::Write);
    assert_eq!(classes[1], ExecutionClassification::Write);
    assert_eq!(classes[2], ExecutionClassification::Read);
    assert_eq!(classes[3], ExecutionClassification::Read);
    assert_ne!(classes[4], ExecutionClassification::Read);
    assert_eq!(classes[5], ExecutionClassification::Read);
    assert_eq!(summary.changing_count, 3);
}

#[test]
fn a_read_only_profile_refuses_a_sequence_change_but_not_a_lookalike() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    let options = RunOptions {
        read_only_profile: true,
        ..RunOptions::default()
    };

    let error = run(
        connection.as_ref(),
        &script("SELECT setval('s', 5);"),
        options,
    )
    .expect_err("refused");
    assert_eq!(error, ScriptError::ReadOnlyProfile { changing: 1 });

    let report = run(connection.as_ref(), &script("SELECT 'setval(';"), options)
        .expect("a string is not a call");
    assert_eq!(report.ran, 1);
}

// ------------------------------------------------------ psql-only content

fn psql_error(text: &str) -> Option<ScriptError> {
    SqlScript::from_text(text, &QueryLanguage::Sql).err()
}

#[test]
fn copy_from_stdin_is_refused_with_its_line() {
    for sql in [
        "COPY t (a, b) FROM stdin;\n1\t2\n\\.\n",
        "copy t from STDIN WITH (FORMAT csv);\n1,2\n\\.\n",
    ] {
        let text = format!("SELECT 1;\n\n{sql}");
        assert_eq!(
            psql_error(&text),
            Some(ScriptError::CopyFromStdin { line: 3 }),
            "{sql}"
        );
    }

    assert_eq!(
        ScriptError::CopyFromStdin { line: 48 }.to_string(),
        "The script has a COPY … FROM stdin data block on line 48, which only the psql command-line client can load. Run it with psql instead."
    );
}

#[test]
fn psql_meta_commands_are_refused_with_the_command_and_line() {
    for (text, command, line) in [
        ("SELECT 1;\n\\connect shop\n", "\\connect", 2),
        ("  \\c db\nSELECT 1;", "\\c", 1),
        ("SELECT 1;\n\\set x 1\n", "\\set", 2),
        ("SELECT 1;\n\t\\i other.sql\n", "\\i", 2),
    ] {
        assert_eq!(
            psql_error(text),
            Some(ScriptError::PsqlMetaCommand {
                command: command.to_string(),
                line
            }),
            "{text:?}"
        );
    }

    assert_eq!(
        ScriptError::PsqlMetaCommand {
            command: "\\connect".to_string(),
            line: 3
        }
        .to_string(),
        "The script uses the psql command \\connect on line 3, which only the psql command-line client understands. Run the script with psql instead: removing it would run the remaining statements on this connection, which may not be the database it selects."
    );
}

#[test]
fn server_side_copy_and_backslashes_in_text_are_not_psql_only() {
    let text = "COPY t FROM '/data/file.csv';\n\
                SELECT 'a\n\\connect x\nb';\n\
                -- \\connect\n\
                /* line\n\\set */\n\
                SELECT $$\n\\i x\n$$;\n\
                SELECT 'C:\\dir';\n";

    assert_eq!(psql_error(text), None);
}

fn control_keyword(text: &str) -> Option<&'static str> {
    control_of(text).map(|(_, _, keyword)| keyword)
}

#[test]
fn comments_are_whitespace_for_transaction_keywords() {
    assert_eq!(control_keyword("COMMIT/*note*/;"), Some("COMMIT"));
    assert_eq!(control_keyword("/*a*/BEGIN/*b*/;"), Some("BEGIN"));
    assert_eq!(control_keyword("ROLLBACK--x\n;"), Some("ROLLBACK"));
}

#[test]
fn a_begin_in_a_comment_or_string_does_not_hide_a_later_end() {
    assert_eq!(
        control_of("SELECT 1 -- begin\n;\nEND;"),
        Some((2, 3, "END"))
    );
    assert_eq!(control_keyword("SELECT 'begin';\nEND;"), Some("END"));
}

#[test]
fn abort_is_transaction_control() {
    assert_eq!(control_keyword("ABORT;"), Some("ABORT"));
    assert_eq!(control_keyword("abort work;"), Some("ABORT"));
}

#[test]
fn begin_not_atomic_is_a_compound_body_not_a_transaction() {
    assert_eq!(control_keyword("BEGIN NOT ATOMIC SELECT 1 END"), None);
    assert_eq!(
        split_body_line("BEGIN NOT ATOMIC INSERT INTO t VALUES (1); DELETE FROM t; END;"),
        Some(1)
    );
    assert_eq!(control_keyword("BEGIN NOT DEFERRABLE;"), Some("BEGIN"));
}

#[test]
fn quoted_sequence_function_names_count_as_calls() {
    for sql in [
        "SELECT pg_catalog.\"setval\"('s', 5);",
        "SELECT \"nextval\" ('s');",
        "SELECT public.NEXTVAL('s');",
    ] {
        assert!(changes_a_sequence(sql), "{sql}");
    }
    for sql in [
        "SELECT \"setval(\" FROM t;",
        "SELECT 'x', \"my setval\" FROM t;",
    ] {
        assert!(!changes_a_sequence(sql), "{sql}");
    }
}

#[test]
fn the_read_is_bounded_even_when_the_file_outgrows_its_metadata() {
    let source = std::io::Cursor::new(vec![b'x'; 11]);

    match read_limited(source, 10) {
        Err(ReadError::TooLarge(size)) => assert_eq!(size, 11),
        other => panic!(
            "expected TooLarge, got {:?}",
            other.map(|bytes| bytes.len())
        ),
    }
    assert_eq!(
        read_limited(std::io::Cursor::new(vec![b'x'; 10]), 10)
            .map(|bytes| bytes.len())
            .ok(),
        Some(10)
    );
}

#[test]
fn a_script_transaction_left_open_after_a_failure_is_rolled_back_and_reported() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script("BEGIN;\nINSERT INTO t VALUES (1);\nINSERT INTO missing VALUES (2);\n");

    let report = run(connection.as_ref(), &script, RunOptions::default()).expect("run");

    assert_eq!(report.end, RunEnd::StoppedOnError);
    assert_eq!(report.script_transaction, ScriptTransaction::RolledBack);
    assert_eq!(row_count(connection.as_ref(), "t"), 0);
}

#[test]
fn a_script_that_ends_without_commit_has_its_transaction_rolled_back() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script("BEGIN;\nINSERT INTO t VALUES (1);\n");

    let report = run(connection.as_ref(), &script, RunOptions::default()).expect("run");

    assert_eq!(report.end, RunEnd::Finished);
    assert_eq!(
        report.script_transaction,
        ScriptTransaction::EndedWithoutCommit
    );
    assert_eq!(report.finish_error, None);
    assert_eq!(row_count(connection.as_ref(), "t"), 0);
    exec(connection.as_ref(), "BEGIN");
    exec(connection.as_ref(), "COMMIT");
}

#[test]
fn a_script_that_closes_its_own_transaction_reports_none() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script("BEGIN;\nINSERT INTO t VALUES (1);\nCOMMIT;");

    let report = run(connection.as_ref(), &script, RunOptions::default()).expect("run");

    assert_eq!(report.script_transaction, ScriptTransaction::None);
}

#[test]
fn an_implicit_commit_before_a_failure_is_never_a_full_rollback() {
    let scratch = Scratch::new();
    let connection: Box<dyn Connection> = Box::new(WithTransactions(plain_sqlite(&scratch), false));
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script(
        "INSERT INTO t VALUES (1);\nCREATE TABLE marker (id INTEGER);\nINSERT INTO missing VALUES (2);\n",
    );

    let report = run(
        connection.as_ref(),
        &script,
        RunOptions {
            transaction: true,
            ..RunOptions::default()
        },
    )
    .expect("run");

    assert_eq!(report.end, RunEnd::StoppedOnError);
    assert_eq!(report.disposition, Disposition::PartiallyKept);
    assert_eq!(report.first_implicit_commit, Some(2));
    assert!(report.ddl_not_transactional);
}

#[test]
fn a_failure_before_any_implicit_commit_is_still_a_full_rollback() {
    let scratch = Scratch::new();
    let connection: Box<dyn Connection> = Box::new(WithTransactions(plain_sqlite(&scratch), false));
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script(
        "INSERT INTO t VALUES (1);\nINSERT INTO missing VALUES (2);\nCREATE TABLE marker (id INTEGER);\n",
    );

    let report = run(
        connection.as_ref(),
        &script,
        RunOptions {
            transaction: true,
            ..RunOptions::default()
        },
    )
    .expect("run");

    assert_eq!(report.disposition, Disposition::RolledBack);
    assert_eq!(report.first_implicit_commit, None);
}

#[test]
fn destructive_ddl_counts_as_schema_statements_in_the_summary() {
    let scratch = Scratch::new();
    let connection: Box<dyn Connection> = Box::new(WithTransactions(plain_sqlite(&scratch), false));

    let summary = script(
        "ALTER TABLE t ADD COLUMN x INTEGER;\nDROP TABLE t;\nTRUNCATE TABLE t;\nDELETE FROM t;",
    )
    .summarize(connection.as_ref(), 4);

    assert_eq!(summary.ddl_count, 3);
    assert!(summary.ddl_commits_on_its_own);
}

// ------------------------------------------------ session commit mode

fn commit_mode(text: &str) -> Option<(&'static str, usize)> {
    match SqlScript::from_text(text, &QueryLanguage::Sql) {
        Err(ScriptError::SessionCommitMode { change, line, .. }) => Some((change, line)),
        Ok(_) => None,
        Err(other) => panic!("unexpected error for {text:?}: {other:?}"),
    }
}

#[test]
fn switching_autocommit_off_is_refused_in_every_spelling() {
    for sql in [
        "SET autocommit = 0",
        "set autocommit=0",
        "SET AUTOCOMMIT = OFF",
        "SET autocommit = FALSE",
        "SET @@autocommit = 0",
        "SET SESSION autocommit = 0",
        "SET @@session.autocommit = 0",
        "SET LOCAL autocommit TO off",
        "/* c */ SET autocommit = 0",
    ] {
        let text = format!("SELECT 1;\n\n{sql};\n");
        assert_eq!(
            commit_mode(&text),
            Some(("turns off autocommit", 3)),
            "{sql}"
        );
    }
}

#[test]
fn implicit_transactions_on_is_refused() {
    assert_eq!(
        commit_mode("SET IMPLICIT_TRANSACTIONS ON;"),
        Some(("turns on implicit transactions", 1))
    );
}

#[test]
fn switching_autocommit_on_or_unrelated_sets_are_allowed() {
    for sql in [
        "SET autocommit = 1",
        "SET autocommit = ON",
        "SET autocommit = TRUE",
        "SET IMPLICIT_TRANSACTIONS OFF",
        "SET search_path = public",
        "SET NAMES utf8",
        "SELECT 'SET autocommit = 0'",
        "-- SET autocommit = 0\nSELECT 1",
        "SELECT autocommit FROM settings",
    ] {
        assert_eq!(commit_mode(&format!("{sql};")), None, "{sql}");
    }
}

#[test]
fn the_commit_mode_error_names_the_statement_and_says_what_to_do() {
    let error = SqlScript::from_text(
        "SELECT 1;\nSELECT 2;\nSELECT 3;\nSELECT 4;\nSET autocommit = 0;",
        &QueryLanguage::Sql,
    )
    .expect_err("refused");

    assert_eq!(
        error.to_string(),
        "The script turns off autocommit on line 5 (SET autocommit = 0). That would leave this connection in manual-commit mode after the run. Remove that statement, or run the script with the database's own command-line client."
    );
}

// ------------------------------------------------ lexing edge cases

#[test]
fn a_dollar_inside_an_identifier_does_not_open_a_quote() {
    assert!(changes_a_sequence("SELECT a$b$c, nextval('s') FROM t"));
}

#[test]
fn a_limit_of_u64_max_still_reads_the_source() {
    let read = read_limited(std::io::Cursor::new(vec![b'x'; 3]), u64::MAX);

    assert_eq!(read.map(|bytes| bytes.len()).ok(), Some(3));
}

#[test]
fn autocommit_off_is_found_in_any_assignment_of_a_set_list_and_in_quoted_values() {
    for sql in [
        "SET sql_mode = 'ANSI', autocommit = 0",
        "SET autocommit = 'OFF'",
        "SET autocommit = '0'",
        "/*!50003 SET autocommit = 0 */",
        "/*!50003 SET @@session.autocommit=0 */",
    ] {
        assert_eq!(
            commit_mode(&format!("{sql};")),
            Some(("turns off autocommit", 1)),
            "{sql}"
        );
    }
    for sql in [
        "SET sql_mode = 'autocommit 0'",
        "SET autocommit = 'ON', sql_mode = ''",
        "SET search_path TO autocommit",
    ] {
        assert_eq!(commit_mode(&format!("{sql};")), None, "{sql}");
    }
    assert_eq!(
        control_keyword("SET sql_mode = '', autocommit = 1;"),
        Some("SET autocommit")
    );
}

#[test]
fn a_chained_commit_keeps_the_script_transaction_open() {
    for (statements, open) in [
        (vec!["BEGIN", "COMMIT AND CHAIN"], true),
        (vec!["BEGIN", "ROLLBACK AND CHAIN"], true),
        (vec!["BEGIN", "COMMIT AND NO CHAIN"], false),
        (vec!["BEGIN", "COMMIT"], false),
        (vec!["BEGIN", "SAVEPOINT a", "ROLLBACK TO a"], true),
    ] {
        let mut tracker = TransactionTracker::default();
        for statement in &statements {
            tracker.apply(statement);
        }
        assert_eq!(tracker.is_open(), open, "{statements:?}");
    }
}

#[test]
fn a_savepoint_that_opens_a_transaction_is_tracked_until_its_outermost_release() {
    let mut tracker = TransactionTracker::default();
    tracker.apply("SAVEPOINT outer_one");
    tracker.apply("SAVEPOINT inner_one");
    assert!(tracker.is_open());
    tracker.apply("RELEASE inner_one");
    assert!(tracker.is_open());
    tracker.apply("RELEASE SAVEPOINT outer_one");
    assert!(!tracker.is_open());

    let mut inside_begin = TransactionTracker::default();
    inside_begin.apply("BEGIN");
    inside_begin.apply("SAVEPOINT a");
    inside_begin.apply("RELEASE a");
    assert!(inside_begin.is_open(), "the BEGIN is still open");
}

#[test]
fn a_savepoint_left_open_at_the_end_is_rolled_back() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    exec(connection.as_ref(), "CREATE TABLE t (id INTEGER)");
    let script = script("SAVEPOINT imported;\nINSERT INTO t VALUES (1);\n");

    let report = run(connection.as_ref(), &script, RunOptions::default()).expect("run");

    assert_eq!(
        report.script_transaction,
        ScriptTransaction::EndedWithoutCommit
    );
    assert_eq!(row_count(connection.as_ref(), "t"), 0);

    let released = self::script("SAVEPOINT s;\nINSERT INTO t VALUES (1);\nRELEASE s;\n");
    let report = run(connection.as_ref(), &released, RunOptions::default()).expect("run");
    assert_eq!(report.script_transaction, ScriptTransaction::None);
    assert_eq!(row_count(connection.as_ref(), "t"), 1);
}
