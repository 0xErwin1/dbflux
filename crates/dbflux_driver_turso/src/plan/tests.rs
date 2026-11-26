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
fn an_analyze_request_is_refused_before_anything_is_sent() {
    use dbflux_core::{Connection, DbError, QueryPlanRequest};

    let builder = turso_serverless::Builder::new_remote("wss://127.0.0.1:1");
    let connection = crate::connection::TursoConnection::open_root(builder)
        .expect("open_root performs no I/O against a closed local URL");

    match connection.query_plan(&QueryPlanRequest::analyzed("SELECT 1")) {
        Err(DbError::NotSupported(message)) => {
            assert_eq!(message, "Analyzed plans are not supported by this driver");
        }
        other => panic!("expected NotSupported, got {other:?}"),
    }
    assert!(
        !connection
            .metadata()
            .query
            .as_ref()
            .unwrap()
            .supports_explain_analyze
    );
}
