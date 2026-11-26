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
