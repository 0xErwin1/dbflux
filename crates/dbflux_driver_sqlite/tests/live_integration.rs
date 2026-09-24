#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err,
    clippy::unwrap_in_result
)]

use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, DescribeRequest, ExplainRequest, OrderByColumn,
    Pagination, QueryRequest, RecordIdentity, RowDelete, RowInsert, RowPatch,
    SchemaLoadingStrategy, TableBrowseRequest, TableCountRequest, TableRef, TransactionStateNote,
    Value,
};
use dbflux_driver_sqlite::SqliteDriver;

fn connect_sqlite() -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.sqlite");

    let driver = SqliteDriver::new();
    let profile = ConnectionProfile::new(
        "live-sqlite",
        DbConfig::SQLite {
            path: db_path,
            connection_id: None,
        },
    );

    let connection = driver.connect(&profile)?;
    connection.ping()?;

    // Leak the tempdir so it doesn't get cleaned up while connection is alive.
    // The OS will clean it up when the process exits.
    std::mem::forget(temp_dir);

    Ok(connection)
}

// ---------------------------------------------------------------------------
// Basic connectivity
// ---------------------------------------------------------------------------

#[test]
fn sqlite_file_connect_ping_query_and_schema() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO users (name) VALUES ('alice')",
    ))?;

    let result = connection.execute(&QueryRequest::new("SELECT id, name FROM users"))?;
    assert_eq!(result.rows.len(), 1);

    assert_eq!(
        connection.schema_loading_strategy(),
        SchemaLoadingStrategy::SingleDatabase
    );

    let databases = connection.list_databases()?;
    assert!(databases.is_empty());

    let schema = connection.schema()?;
    assert!(schema.is_relational());
    let _ = schema.databases();

    Ok(())
}

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

#[test]
fn sqlite_schema_introspection() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE test_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER DEFAULT 0
        )",
    ))?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE test_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES test_users(id),
            amount REAL NOT NULL
        )",
    ))?;

    connection.execute(&QueryRequest::new(
        "CREATE INDEX idx_orders_user_id ON test_orders(user_id)",
    ))?;

    connection.execute(&QueryRequest::new(
        "CREATE VIEW test_user_view AS SELECT id, name FROM test_users",
    ))?;

    let schema = connection.schema()?;
    assert!(schema.is_relational());

    let table = connection.table_details("main", None, "test_users")?;
    assert_eq!(table.name, "test_users");

    let columns = table.columns.as_ref().expect("columns should be loaded");
    assert!(columns.len() >= 4);

    let id_col = columns.iter().find(|c| c.name == "id").expect("id column");
    assert!(id_col.is_primary_key);

    let name_col = columns
        .iter()
        .find(|c| c.name == "name")
        .expect("name column");
    assert!(!name_col.nullable);

    let indexes = table.indexes.as_ref().expect("indexes should be loaded");
    let idx_data = match indexes {
        dbflux_core::IndexData::Relational(v) => v,
        _ => panic!("expected relational index data"),
    };
    assert!(!idx_data.is_empty());

    let relational = schema.as_relational().expect("should be relational schema");
    let has_view = relational
        .schemas
        .iter()
        .flat_map(|s| s.views.iter())
        .chain(relational.views.iter())
        .any(|v| v.name == "test_user_view");
    assert!(has_view, "view should appear in schema");

    Ok(())
}

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

#[test]
fn sqlite_crud_operations() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE crud_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER DEFAULT 0
        )",
    ))?;

    let insert_result = connection.insert_row(&RowInsert::new(
        "crud_test".to_string(),
        None,
        vec!["name".to_string(), "value".to_string()],
        vec![Value::Text("alice".to_string()), Value::Int(42)],
    ))?;
    assert_eq!(insert_result.affected_rows, 1);

    let rows = connection
        .execute(&QueryRequest::new(
            "SELECT * FROM crud_test WHERE name = 'alice'",
        ))?
        .rows;
    assert_eq!(rows.len(), 1);

    let update_result = connection.update_row(&RowPatch::new(
        RecordIdentity::composite(
            vec!["name".to_string()],
            vec![Value::Text("alice".to_string())],
        ),
        "crud_test".to_string(),
        None,
        vec![("value".to_string(), Value::Int(99))],
    ))?;
    assert_eq!(update_result.affected_rows, 1);

    let rows = connection
        .execute(&QueryRequest::new(
            "SELECT value FROM crud_test WHERE name = 'alice'",
        ))?
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(99));

    let delete_result = connection.delete_row(&RowDelete::new(
        RecordIdentity::composite(
            vec!["name".to_string()],
            vec![Value::Text("alice".to_string())],
        ),
        "crud_test".to_string(),
        None,
    ))?;
    assert_eq!(delete_result.affected_rows, 1);

    let rows = connection
        .execute(&QueryRequest::new("SELECT * FROM crud_test"))?
        .rows;
    assert!(rows.is_empty());

    Ok(())
}

// ---------------------------------------------------------------------------
// Browse and count
// ---------------------------------------------------------------------------

#[test]
fn sqlite_browse_and_count() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE browse_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )",
    ))?;

    for i in 1..=25 {
        connection.execute(&QueryRequest::new(format!(
            "INSERT INTO browse_test (name) VALUES ('item_{}')",
            i
        )))?;
    }

    let table_ref = TableRef::new("browse_test");

    let count = connection.count_table(&TableCountRequest::new(table_ref.clone()))?;
    assert_eq!(count, 25);

    let filtered_count = connection.count_table(
        &TableCountRequest::new(table_ref.clone()).with_filter("name LIKE 'item_1%'"),
    )?;
    assert!(filtered_count > 0);
    assert!(filtered_count < 25);

    let page1 = connection.browse_table(
        &TableBrowseRequest::new(table_ref.clone())
            .with_pagination(Pagination::Offset {
                limit: 10,
                offset: 0,
            })
            .with_order_by(vec![OrderByColumn::asc("id")]),
    )?;
    assert_eq!(page1.rows.len(), 10);

    let page2 = connection.browse_table(
        &TableBrowseRequest::new(table_ref.clone())
            .with_pagination(Pagination::Offset {
                limit: 10,
                offset: 10,
            })
            .with_order_by(vec![OrderByColumn::asc("id")]),
    )?;
    assert_eq!(page2.rows.len(), 10);
    assert_ne!(page1.rows[0], page2.rows[0]);

    let filtered = connection.browse_table(
        &TableBrowseRequest::new(table_ref)
            .with_filter("name = 'item_5'")
            .with_pagination(Pagination::Offset {
                limit: 100,
                offset: 0,
            }),
    )?;
    assert_eq!(filtered.rows.len(), 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Explain and describe
// ---------------------------------------------------------------------------

#[test]
fn sqlite_explain() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE explain_test (id INTEGER PRIMARY KEY, name TEXT)",
    ))?;

    let table_ref = TableRef::new("explain_test");
    let result = connection.explain(&ExplainRequest::new(table_ref))?;
    assert!(!result.rows.is_empty() || result.text_body.is_some());

    Ok(())
}

#[test]
fn sqlite_describe_table() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE describe_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )",
    ))?;

    let table_ref = TableRef::new("describe_test");
    let result = connection.describe_table(&DescribeRequest::new(table_ref))?;
    assert!(result.rows.len() >= 3);

    Ok(())
}

// ---------------------------------------------------------------------------
// Query cancellation
// ---------------------------------------------------------------------------

#[test]
fn sqlite_cancel_active() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    let result = connection.cancel_active();
    assert!(result.is_ok());

    Ok(())
}

// ---------------------------------------------------------------------------
// Code generators
// ---------------------------------------------------------------------------

#[test]
fn sqlite_code_generators() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE codegen_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )",
    ))?;

    let generators = connection.code_generators();
    assert!(!generators.is_empty());

    let table = connection.table_details("main", None, "codegen_test")?;

    for generator in generators {
        let code = connection.generate_code(&generator.id, &table)?;
        assert!(
            !code.is_empty(),
            "generator '{}' returned empty code",
            generator.id
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Referential integrity toggle (data-transfer engine)
// ---------------------------------------------------------------------------

#[test]
fn sqlite_set_referential_integrity_disables_and_restores_fk_checks() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE parent_ri (id INTEGER PRIMARY KEY)",
    ))?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE child_ri (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent_ri(id))",
    ))?;
    // SQLite disables FK enforcement per-connection by default.
    connection.set_referential_integrity(true)?;

    let violates = connection.execute(&QueryRequest::new("INSERT INTO child_ri VALUES (1, 999)"));
    assert!(violates.is_err(), "FK violation must fail with RI enabled");

    connection.set_referential_integrity(false)?;
    connection.execute(&QueryRequest::new("INSERT INTO child_ri VALUES (1, 999)"))?;

    connection.set_referential_integrity(true)?;
    let still_violates =
        connection.execute(&QueryRequest::new("INSERT INTO child_ri VALUES (2, 998)"));
    assert!(
        still_violates.is_err(),
        "FK violation must fail again after RI is restored"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction state after a failed execution
// ---------------------------------------------------------------------------

fn connect_sqlite_at(path: &std::path::Path) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let profile = ConnectionProfile::new(
        "live-sqlite",
        DbConfig::SQLite {
            path: path.to_path_buf(),
            connection_id: None,
        },
    );

    SqliteDriver::new().connect(&profile)
}

fn error_hint(error: &DbError) -> Option<&str> {
    error
        .formatted()
        .and_then(|formatted| formatted.hint.as_deref())
}

fn row_count(connection: &dyn dbflux_core::Connection, table: &str) -> Result<usize, DbError> {
    let result = connection.execute(&QueryRequest::new(format!("SELECT * FROM {table}")))?;
    Ok(result.rows.len())
}

#[test]
fn sqlite_failed_script_rolls_back_the_transaction_it_opened() -> Result<(), DbError> {
    let connection = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE tx_script (id INTEGER PRIMARY KEY)",
    ))?;

    let error = connection
        .execute(&QueryRequest::new(
            "BEGIN; INSERT INTO tx_script VALUES (1); INSERT INTO tx_script VALUES (1); COMMIT;",
        ))
        .expect_err("duplicate primary key must fail the script");

    assert!(
        error_hint(&error)
            .is_some_and(|hint| hint.contains(TransactionStateNote::RolledBack.message())),
        "hint must report the rollback, got {error:?}"
    );
    assert_eq!(row_count(connection.as_ref(), "tx_script")?, 0);

    connection.execute(&QueryRequest::new(
        "BEGIN; INSERT INTO tx_script VALUES (2); COMMIT;",
    ))?;
    assert_eq!(row_count(connection.as_ref(), "tx_script")?, 1);

    Ok(())
}

#[test]
fn sqlite_failed_statement_leaves_an_earlier_transaction_open() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("still-open.sqlite");

    let connection = connect_sqlite_at(&db_path)?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE tx_open (id INTEGER PRIMARY KEY)",
    ))?;

    connection.execute(&QueryRequest::new("BEGIN"))?;
    connection.execute(&QueryRequest::new("INSERT INTO tx_open VALUES (1)"))?;

    let error = connection
        .execute(&QueryRequest::new("INSERT INTO tx_open VALUES (1)"))
        .expect_err("duplicate primary key must fail");

    assert!(
        error_hint(&error)
            .is_some_and(|hint| hint.contains(TransactionStateNote::StillOpen.message())),
        "hint must report the open transaction, got {error:?}"
    );

    let observer = connect_sqlite_at(&db_path)?;
    assert_eq!(row_count(observer.as_ref(), "tx_open")?, 0);

    connection.execute(&QueryRequest::new("COMMIT"))?;
    assert_eq!(row_count(observer.as_ref(), "tx_open")?, 1);

    Ok(())
}

#[test]
fn sqlite_failed_statement_outside_a_transaction_adds_no_note() -> Result<(), DbError> {
    let connection = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE tx_none (id INTEGER PRIMARY KEY)",
    ))?;
    connection.execute(&QueryRequest::new("INSERT INTO tx_none VALUES (1)"))?;

    let error = connection
        .execute(&QueryRequest::new("INSERT INTO tx_none VALUES (1)"))
        .expect_err("duplicate primary key must fail");

    let hint = error_hint(&error).unwrap_or_default();
    assert!(!hint.contains(TransactionStateNote::RolledBack.message()));
    assert!(!hint.contains(TransactionStateNote::StillOpen.message()));

    Ok(())
}

// ---------------------------------------------------------------------------
// Query safety (bounded execution contracts)
// ---------------------------------------------------------------------------

fn assert_no_rows_truncated(result: &dbflux_core::QueryResult, expected_rows: usize) {
    assert_eq!(result.rows.len(), expected_rows);
    assert!(
        !result.rows_truncated(),
        "expected no truncation flag with {expected_rows} retained rows"
    );
}

#[test]
fn sqlite_query_safety_limit_below_exact_and_over_retains_and_flags() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_limit.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE safety_limit (id INTEGER PRIMARY KEY)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO safety_limit VALUES (1), (2), (3), (4), (5)",
    ))?;

    // Below the row count: everything is retained, no truncation flag.
    let below =
        connection.execute(&QueryRequest::new("SELECT id FROM safety_limit").with_limit(8))?;
    assert_no_rows_truncated(&below, 5);

    // Exactly at the row count: the last row is retained and no extra row is
    // observed afterwards, so there is still no truncation flag.
    let exact =
        connection.execute(&QueryRequest::new("SELECT id FROM safety_limit").with_limit(5))?;
    assert_no_rows_truncated(&exact, 5);

    // Over the row count: only the requested rows are retained, in order, and
    // the truncation flag is set.
    let over =
        connection.execute(&QueryRequest::new("SELECT id FROM safety_limit").with_limit(3))?;
    assert_eq!(over.rows.len(), 3);
    assert_eq!(over.rows[0][0], Value::Int(1));
    assert_eq!(over.rows[2][0], Value::Int(3));
    assert!(over.rows_truncated());

    Ok(())
}

#[test]
fn sqlite_query_safety_zero_limit_retains_nothing_only_when_rows_exist() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_zero.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE safety_zero (id INTEGER PRIMARY KEY)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO safety_zero VALUES (1), (2), (3), (4)",
    ))?;

    // Some(0) means retain zero rows — not unlimited — and the flag is set
    // because rows were actually omitted.
    let nonempty =
        connection.execute(&QueryRequest::new("SELECT id FROM safety_zero").with_limit(0))?;
    assert!(nonempty.rows.is_empty());
    assert!(nonempty.rows_truncated());

    // A result with no rows at all omits nothing, so the flag stays off.
    let empty = connection
        .execute(&QueryRequest::new("SELECT id FROM safety_zero WHERE id > 100").with_limit(0))?;
    assert!(empty.rows.is_empty());
    assert!(!empty.rows_truncated());

    // A capped request never caps a mutation's effects or its affected count.
    let insert = connection
        .execute(&QueryRequest::new("INSERT INTO safety_zero VALUES (5)").with_limit(0))?;
    assert_eq!(insert.affected_rows, Some(1));
    let delete = connection.execute(&QueryRequest::new("DELETE FROM safety_zero").with_limit(0))?;
    assert_eq!(delete.affected_rows, Some(5));

    let count = connection
        .execute(&QueryRequest::new("SELECT COUNT(*) FROM safety_zero"))?
        .rows;
    assert_eq!(
        count[0][0],
        Value::Int(0),
        "the capped DML must fully apply"
    );

    Ok(())
}

#[test]
fn sqlite_query_safety_explicit_none_limit_keeps_uncapped_behavior() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_uncapped.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE safety_uncapped (id INTEGER PRIMARY KEY)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO safety_uncapped VALUES (1), (2), (3)",
    ))?;

    let mut request = QueryRequest::new("SELECT id FROM safety_uncapped");
    request.limit = None;
    let result = connection.execute(&request)?;
    assert_no_rows_truncated(&result, 3);

    Ok(())
}

#[test]
fn sqlite_query_safety_uncapped_batch_still_executes_later_statements() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_legacy_batch.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    // Legacy behavior: without a limit, the whole batch runs statement by
    // statement and later statements execute.
    let result = connection.execute(&QueryRequest::new(
        "CREATE TABLE safety_legacy_batch (id INTEGER PRIMARY KEY); \
         INSERT INTO safety_legacy_batch VALUES (1), (2); \
         SELECT id FROM safety_legacy_batch ORDER BY id",
    ))?;
    assert!(result.rows.is_empty());
    assert_eq!(result.additional_results.len(), 2);
    let select_set = &result.additional_results[1];
    assert_eq!(select_set.rows.len(), 2);

    let count = connection
        .execute(&QueryRequest::new(
            "SELECT COUNT(*) FROM safety_legacy_batch",
        ))?
        .rows;
    assert_eq!(count[0][0], Value::Int(2));

    Ok(())
}

#[test]
fn sqlite_query_safety_bounded_batch_rejected_without_effects_and_reusable() -> Result<(), DbError>
{
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_bounded_batch.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE safety_bounded_batch (n INTEGER PRIMARY KEY)",
    ))?;

    // A bounded request over a multi-statement batch must be rejected before
    // any statement is prepared or executed — including the first INSERT and
    // the later RETURNING statement, which would otherwise take effect.
    let rejected = connection.execute(
        &QueryRequest::new(
            "INSERT INTO safety_bounded_batch (n) VALUES (1); \
             INSERT INTO safety_bounded_batch (n) VALUES (2) RETURNING n;",
        )
        .with_limit(5),
    );
    match rejected {
        Err(DbError::NotSupported(reason)) => {
            assert!(
                reason.to_lowercase().contains("batch"),
                "rejection should name the batch limitation, got: {reason}"
            );
        }
        Err(other) => panic!("unexpected error kind: {other:?}"),
        Ok(result) => panic!(
            "bounded batch must be rejected, got {:?}",
            result.rows.len()
        ),
    }

    // No statement of the batch may have run: same connection and an
    // independent connection against the same database file both see zero
    // rows.
    let count = connection
        .execute(&QueryRequest::new(
            "SELECT COUNT(*) FROM safety_bounded_batch",
        ))?
        .rows;
    assert_eq!(count[0][0], Value::Int(0));
    let witness = connect_sqlite_at(&db_path)?;
    let count = witness
        .execute(&QueryRequest::new(
            "SELECT COUNT(*) FROM safety_bounded_batch",
        ))?
        .rows;
    assert_eq!(count[0][0], Value::Int(0));

    // DDL-dependent batch: if any earlier statement had executed, the table
    // created by the first statement would exist afterwards.
    let rejected = connection.execute(
        &QueryRequest::new(
            "CREATE TABLE safety_ddl_batch (id INTEGER PRIMARY KEY); \
             INSERT INTO safety_ddl_batch VALUES (1);",
        )
        .with_limit(1),
    );
    assert!(
        matches!(rejected, Err(DbError::NotSupported(_))),
        "bounded DDL-dependent batch must be rejected, got: {rejected:?}"
    );
    let exists = connection
        .execute(&QueryRequest::new(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'safety_ddl_batch'",
        ))?
        .rows;
    assert_eq!(
        exists[0][0],
        Value::Int(0),
        "no statement of the DDL-dependent batch may run"
    );

    // The connection stays reusable for a bounded single statement.
    let reused = connection.execute(&QueryRequest::new("SELECT 1").with_limit(1))?;
    assert_eq!(reused.rows.len(), 1);

    Ok(())
}

#[test]
fn sqlite_query_safety_bounded_commented_pragma_runs_as_requested() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    // A bounded PRAGMA behind a leading comment is the requested statement,
    // not a refusal: it applies its setting and returns its row under the cap.
    for (sql, expected) in [
        ("-- c\nPRAGMA busy_timeout = 1234", 1234),
        ("/* c */ PRAGMA busy_timeout = 2345", 2345),
    ] {
        let result = connection.execute(&QueryRequest::new(sql).with_limit(1))?;
        assert_no_rows_truncated(&result, 1);
        assert_eq!(result.rows[0][0], Value::Int(expected), "result of '{sql}'");

        let current = connection
            .execute(&QueryRequest::new("PRAGMA busy_timeout"))?
            .rows[0][0]
            .clone();
        assert_eq!(current, Value::Int(expected), "setting after '{sql}'");
    }

    Ok(())
}

#[test]
fn sqlite_query_safety_bounded_bom_prefixed_pragma_runs_as_requested() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    let result = connection
        .execute(&QueryRequest::new("\u{feff}PRAGMA busy_timeout = 1234").with_limit(1))?;
    assert_no_rows_truncated(&result, 1);
    assert_eq!(result.rows[0][0], Value::Int(1234));

    let current = connection
        .execute(&QueryRequest::new("PRAGMA busy_timeout"))?
        .rows[0][0]
        .clone();
    assert_eq!(current, Value::Int(1234));

    Ok(())
}

#[test]
fn sqlite_query_safety_backslash_literal_batch_refused_before_insert() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("backslash_batch.sqlite");
    let connection = connect_sqlite_at(&db_path)?;
    connection.execute(&QueryRequest::new("CREATE TABLE t (v TEXT)"))?;

    let outcome = connection.execute(
        &QueryRequest::new(r"INSERT INTO t VALUES ('\'); INSERT INTO t VALUES ('x');")
            .with_limit(1),
    );
    let witness = connect_sqlite_at(&db_path)?;
    let persisted_count = witness
        .execute(&QueryRequest::new("SELECT COUNT(*) FROM t"))?
        .rows[0][0]
        .clone();

    assert!(
        matches!(outcome, Err(DbError::NotSupported(_))) && persisted_count == Value::Int(0),
        "bounded batch must be refused before execution without persisting its first INSERT: outcome={outcome:?}, persisted_count={persisted_count:?}"
    );
    Ok(())
}

#[test]
fn sqlite_query_safety_bounded_returning_persists_all_rows_and_caps_output() -> Result<(), DbError>
{
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_returning.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE safety_returning (id INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0)",
    ))?;

    // The cap limits the returned rows, never the mutation: every inserted row
    // persists, only the first two RETURNING rows are retained, and the
    // omission is flagged.
    let result = connection.execute(
        &QueryRequest::new(
            "INSERT INTO safety_returning (id) VALUES (1), (2), (3), (4), (5) RETURNING id",
        )
        .with_limit(2),
    )?;
    assert_eq!(result.rows.len(), 2);
    assert!(result.rows_truncated());

    // Committed-state proof through an independent connection.
    let witness = connect_sqlite_at(&db_path)?;
    let count = witness
        .execute(&QueryRequest::new("SELECT COUNT(*) FROM safety_returning"))?
        .rows;
    assert_eq!(
        count[0][0],
        Value::Int(5),
        "the capped INSERT ... RETURNING must persist every row"
    );

    // Output at or under the cap is retained whole and not flagged.
    let result = connection.execute(
        &QueryRequest::new("UPDATE safety_returning SET v = 1 WHERE id = 1 RETURNING id, v")
            .with_limit(1),
    )?;
    assert_no_rows_truncated(&result, 1);
    assert_eq!(result.rows[0][1], Value::Int(1));

    Ok(())
}

#[test]
fn sqlite_query_safety_bounded_with_and_values_are_capped() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_row_producing.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    for sql in [
        "WITH generated(n) AS (VALUES (1), (2), (3)) SELECT n FROM generated",
        "VALUES (1), (2), (3)",
    ] {
        let result = connection.execute(&QueryRequest::new(sql).with_limit(2))?;
        assert_eq!(result.rows.len(), 2, "retained rows for '{sql}'");
        assert_eq!(result.rows[0][0], Value::Int(1), "first row for '{sql}'");
        assert!(result.rows_truncated(), "truncation flag for '{sql}'");

        let whole = connection.execute(&QueryRequest::new(sql).with_limit(3))?;
        assert_no_rows_truncated(&whole, 3);
    }

    Ok(())
}

#[test]
fn sqlite_query_safety_late_row_error_propagates_after_budget_consumption() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_late_error.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE safety_late (id INTEGER PRIMARY KEY)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO safety_late VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
    ))?;

    // abs() raises 'integer overflow' only for the exact INT64 minimum, so
    // rows 1..7 evaluate cleanly and row 8 fails during iteration. The scan
    // follows the INTEGER PRIMARY KEY, so no sorter evaluates the expression
    // ahead of the rows. Reaching the cap must not hide that late error.
    let result = connection.execute(
        &QueryRequest::new(
            "SELECT id, abs(CASE WHEN id = 8 \
             THEN CAST('-9223372036854775808' AS INTEGER) ELSE -id END) AS v \
             FROM safety_late ORDER BY id",
        )
        .with_limit(5),
    );
    match result {
        Err(DbError::QueryFailed(formatted)) => {
            assert!(
                formatted
                    .to_display_string()
                    .to_lowercase()
                    .contains("overflow"),
                "expected the engine's integer overflow error, got: {}",
                formatted.to_display_string()
            );
        }
        Err(other) => panic!("unexpected error kind: {other:?}"),
        Ok(result) => panic!(
            "cap must not hide the late row error, got {:?}",
            result.rows
        ),
    }

    // The connection stays usable afterwards.
    let follow_up = connection.execute(&QueryRequest::new("SELECT 1").with_limit(1))?;
    assert_eq!(follow_up.rows.len(), 1);

    Ok(())
}

#[test]
fn sqlite_query_safety_statement_timeout_rejected_before_execution() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_timeout.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE safety_timeout (n INTEGER PRIMARY KEY)",
    ))?;

    // A requested statement deadline cannot be honored safely by this driver
    // yet, so the request is rejected at entry — before the INSERT could take
    // effect.
    let mut request = QueryRequest::new("INSERT INTO safety_timeout (n) VALUES (1)");
    request.statement_timeout = Some(std::time::Duration::from_secs(5));
    match connection.execute(&request) {
        Err(DbError::NotSupported(reason)) => {
            assert!(
                reason.to_lowercase().contains("timeout"),
                "rejection should name the unsupported deadline, got: {reason}"
            );
        }
        Err(other) => panic!("unexpected error kind: {other:?}"),
        Ok(result) => panic!("requested deadline must be rejected, got {result:?}"),
    }

    let count = connection
        .execute(&QueryRequest::new("SELECT COUNT(*) FROM safety_timeout"))?
        .rows;
    assert_eq!(
        count[0][0],
        Value::Int(0),
        "no effect may precede the rejection"
    );

    // Uncapped execution is untouched.
    let ping = connection.execute(&QueryRequest::new("SELECT 1"))?;
    assert_eq!(ping.rows[0][0], Value::Int(1));

    Ok(())
}

#[test]
fn sqlite_query_safety_bounded_contexts_rejected_and_uncapped_control_runs() -> Result<(), DbError>
{
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_context.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    // A bounded request must not bypass its cap through the instance-metric or
    // instance-inspector dispatch contexts, which the public request type can
    // carry even though this driver advertises no instance catalog.
    for limit in [0u32, 1] {
        let mut request = QueryRequest::new("SELECT 1");
        request.limit = Some(limit);
        request.execution_context = Some(dbflux_core::ExecutionContext {
            source: Some(dbflux_core::ExecutionSourceContext::InstanceMetricQuery {
                metric_id: "sqlite.safety".to_string(),
                start_ms: 0,
                end_ms: 1,
            }),
            ..Default::default()
        });
        match connection.execute(&request) {
            Err(DbError::NotSupported(reason)) => {
                let lowered = reason.to_lowercase();
                assert!(
                    lowered.contains("instance metric") && lowered.contains("row limit"),
                    "expected the bounded metric-context rejection, got: {reason}"
                );
                assert!(
                    !lowered.contains("batch"),
                    "bounded-context rejection must not be the batch refusal: {reason}"
                );
            }
            other => panic!(
                "bounded metric context must be rejected before dispatch, got {:?}",
                other.map(|r| r.rows.len())
            ),
        }

        let mut request = QueryRequest::new("SELECT 1");
        request.limit = Some(limit);
        request.execution_context = Some(dbflux_core::ExecutionContext {
            source: Some(
                dbflux_core::ExecutionSourceContext::InstanceInspectorQuery {
                    metric_id: "sqlite.safety".to_string(),
                },
            ),
            ..Default::default()
        });
        match connection.execute(&request) {
            Err(DbError::NotSupported(reason)) => {
                let lowered = reason.to_lowercase();
                assert!(
                    lowered.contains("instance inspector") && lowered.contains("row limit"),
                    "expected the bounded inspector-context rejection, got: {reason}"
                );
            }
            other => panic!(
                "bounded inspector context must be rejected before dispatch, got {:?}",
                other.map(|r| r.rows.len())
            ),
        }
    }

    // Uncapped control: the same contexts without a limit keep the existing
    // execution behavior.
    let mut request = QueryRequest::new("SELECT 1");
    request.execution_context = Some(dbflux_core::ExecutionContext {
        source: Some(dbflux_core::ExecutionSourceContext::InstanceMetricQuery {
            metric_id: "sqlite.safety".to_string(),
            start_ms: 0,
            end_ms: 1,
        }),
        ..Default::default()
    });
    let result = connection.execute(&request)?;
    assert_eq!(result.rows.len(), 1);
    assert!(!result.rows_truncated());

    Ok(())
}

#[test]
fn sqlite_query_safety_capped_single_statement_with_trailing_comments_executes()
-> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("safety_comments.sqlite");
    let connection = connect_sqlite_at(&db_path)?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE safety_comments (id INTEGER PRIMARY KEY)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO safety_comments VALUES (1), (2), (3)",
    ))?;

    // Trailing line comment: the splitter sees two non-empty segments, but
    // this is one statement and a capped request must execute it.
    let line = connection.execute(
        &QueryRequest::new("SELECT id FROM safety_comments; -- trailing line comment")
            .with_limit(2),
    )?;
    assert_eq!(line.rows.len(), 2);
    assert!(line.rows_truncated());

    // Trailing block comment.
    let block = connection.execute(
        &QueryRequest::new("SELECT id FROM safety_comments; /* trailing block comment */")
            .with_limit(3),
    )?;
    assert_no_rows_truncated(&block, 3);

    // Trailing semicolon alone: a single statement, not a batch.
    let semicolon =
        connection.execute(&QueryRequest::new("SELECT id FROM safety_comments;").with_limit(2))?;
    assert_eq!(semicolon.rows.len(), 2);
    assert!(semicolon.rows_truncated());

    // Semicolons inside string literals are not statement separators.
    let quoted = connection.execute(&QueryRequest::new("SELECT ';' AS semi").with_limit(1))?;
    assert_no_rows_truncated(&quoted, 1);
    assert_eq!(quoted.rows[0][0], Value::Text(";".to_string()));

    Ok(())
}
