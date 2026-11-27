use super::*;

#[test]
fn a_single_statement_is_wrapped_in_explain_query_plan() {
    assert_eq!(
        explain_statement("  DELETE FROM t WHERE id = 1; ").unwrap(),
        "EXPLAIN QUERY PLAN DELETE FROM t WHERE id = 1"
    );
}

#[test]
fn several_statements_are_refused() {
    let error = explain_statement("SELECT 1; DELETE FROM t").unwrap_err();

    assert_eq!(
        error.to_string(),
        "A query plan covers exactly one statement; the query has 2. Select a single statement."
    );
}

#[test]
fn an_empty_query_is_refused() {
    let error = explain_statement(" ; ").unwrap_err();

    assert_eq!(
        error.to_string(),
        "A query plan needs a statement; the query is empty."
    );
}

#[test]
fn a_pragma_is_refused_before_anything_is_sent() {
    let error = explain_statement("PRAGMA foreign_keys = OFF").unwrap_err();

    assert!(
        error
            .to_string()
            .starts_with("A query plan covers SELECT, INSERT, UPDATE"),
        "{error}"
    );
}

#[test]
fn an_analyze_request_is_refused_before_anything_runs() {
    use dbflux_core::{
        ConnectionProfile, DbConfig, DbDriver, DbError, QueryPlanRequest, QueryRequest, Value,
    };

    let connection = crate::SqliteDriver::new()
        .connect(&ConnectionProfile::new(
            "memory",
            DbConfig::SQLite {
                path: ":memory:".into(),
                connection_id: None,
            },
        ))
        .unwrap();
    connection
        .execute(&QueryRequest::new("CREATE TABLE t (id INTEGER)"))
        .unwrap();
    connection
        .execute(&QueryRequest::new("INSERT INTO t VALUES (1)"))
        .unwrap();

    for statement in ["SELECT * FROM t", "DELETE FROM t"] {
        match connection.query_plan(&QueryPlanRequest::analyzed(statement)) {
            Err(DbError::NotSupported(message)) => {
                assert_eq!(message, "Analyzed plans are not supported by this driver");
            }
            other => panic!("{statement}: expected NotSupported, got {other:?}"),
        }
    }

    let rows = connection
        .execute(&QueryRequest::new("SELECT count(*) FROM t"))
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Int(1));
    assert!(
        !connection
            .metadata()
            .query
            .as_ref()
            .unwrap()
            .supports_explain_analyze
    );
}
