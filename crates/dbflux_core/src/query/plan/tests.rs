use super::*;
use crate::{QueryResult, Value};

#[test]
fn single_statement_is_returned_trimmed_without_the_terminator() {
    assert_eq!(single_plan_statement("  SELECT 1;  ").unwrap(), "SELECT 1");
    assert_eq!(
        single_plan_statement("DELETE FROM t WHERE id = 1").unwrap(),
        "DELETE FROM t WHERE id = 1"
    );
}

#[test]
fn semicolons_inside_literals_do_not_split() {
    assert_eq!(
        single_plan_statement("SELECT 'a;b'").unwrap(),
        "SELECT 'a;b'"
    );
}

#[test]
fn multiple_statements_are_refused() {
    let error = single_plan_statement("SELECT 1; DELETE FROM t").unwrap_err();

    assert_eq!(
        error.to_string(),
        "A query plan covers exactly one statement; the query has 2. Select a single statement."
    );
}

#[test]
fn empty_queries_are_refused() {
    for query in ["", "   \n\t", " ; "] {
        let error = single_plan_statement(query).unwrap_err();
        assert_eq!(
            error.to_string(),
            "A query plan needs a statement; the query is empty."
        );
    }
}

fn eqp_result(rows: &[(i64, i64, &str)]) -> QueryResult {
    let mut result = QueryResult::empty();
    result.rows = rows
        .iter()
        .map(|(id, parent, detail)| {
            vec![
                Value::Int(*id),
                Value::Int(*parent),
                Value::Int(0),
                Value::Text((*detail).to_string()),
            ]
        })
        .collect();
    result
}

fn detail<'a>(node: &'a PlanNode, key: &str) -> Option<&'a str> {
    node.details
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

#[test]
fn search_using_an_index_maps_relation_index_and_condition() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(3, 0, "SEARCH t USING INDEX idx_a (a=?)")]))
        .unwrap();

    assert_eq!(plan.raw_format, PlanFormat::Text);
    assert_eq!(plan.roots.len(), 1);
    let node = &plan.roots[0];
    assert_eq!(node.operation, "SEARCH");
    assert_eq!(node.relation.as_deref(), Some("t"));
    assert_eq!(node.index.as_deref(), Some("idx_a"));
    assert_eq!(detail(node, "Condition"), Some("(a=?)"));
    assert_eq!(node.estimated_total_cost, None);
    assert_eq!(node.estimated_rows, None);
}

#[test]
fn covering_index_and_alias_and_legacy_table_keyword() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (2, 0, "SCAN TABLE users AS u USING COVERING INDEX idx_name"),
        (3, 0, "SCAN orders"),
    ]))
    .unwrap();

    let first = &plan.roots[0];
    assert_eq!(first.operation, "SCAN");
    assert_eq!(first.relation.as_deref(), Some("users"));
    assert_eq!(first.index.as_deref(), Some("idx_name"));
    assert_eq!(detail(first, "Alias"), Some("u"));
    assert_eq!(detail(first, "Covering"), Some("true"));

    let second = &plan.roots[1];
    assert_eq!(second.relation.as_deref(), Some("orders"));
    assert_eq!(second.index, None);
}

#[test]
fn primary_key_and_automatic_index_have_no_index_name() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (2, 0, "SEARCH t USING INTEGER PRIMARY KEY (rowid=?)"),
        (3, 0, "SEARCH u USING AUTOMATIC COVERING INDEX (a=?)"),
    ]))
    .unwrap();

    assert_eq!(plan.roots[0].index, None);
    assert_eq!(detail(&plan.roots[0], "Key"), Some("INTEGER PRIMARY KEY"));
    assert_eq!(detail(&plan.roots[0], "Condition"), Some("(rowid=?)"));
    assert_eq!(plan.roots[1].index, None);
    assert_eq!(detail(&plan.roots[1], "Automatic"), Some("true"));
}

#[test]
fn non_scan_details_become_the_operation() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (2, 0, "SCAN CONSTANT ROW"),
        (3, 0, "SCAN SUBQUERY 1"),
        (4, 0, "USE TEMP B-TREE FOR ORDER BY"),
    ]))
    .unwrap();

    assert_eq!(plan.roots[0].operation, "SCAN CONSTANT ROW");
    assert_eq!(plan.roots[0].relation, None);
    assert_eq!(plan.roots[1].operation, "SCAN SUBQUERY 1");
    assert_eq!(plan.roots[1].relation, None);
    assert_eq!(plan.roots[2].operation, "USE TEMP B-TREE FOR ORDER BY");
}

#[test]
fn tree_is_built_from_id_and_parent() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (2, 0, "MATERIALIZE x"),
        (3, 2, "SCAN t"),
        (9, 0, "SCAN x"),
        (10, 9, "SEARCH y USING INDEX i (k=?)"),
        (11, 3, "USE TEMP B-TREE FOR DISTINCT"),
    ]))
    .unwrap();

    assert_eq!(plan.roots.len(), 2);
    assert_eq!(plan.roots[0].operation, "MATERIALIZE x");
    assert_eq!(plan.roots[0].children.len(), 1);
    assert_eq!(plan.roots[0].children[0].relation.as_deref(), Some("t"));
    assert_eq!(plan.roots[0].children[0].children.len(), 1);
    assert_eq!(plan.roots[1].children[0].index.as_deref(), Some("i"));
}

#[test]
fn an_orphan_parent_becomes_a_root() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(5, 77, "SCAN t")])).unwrap();

    assert_eq!(plan.roots.len(), 1);
}

#[test]
fn raw_text_keeps_every_row() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(2, 0, "SCAN t"), (3, 2, "SCAN u")])).unwrap();

    assert_eq!(plan.raw, "2\t0\tSCAN t\n3\t2\tSCAN u");
}

#[test]
fn a_result_without_rows_is_an_empty_plan_with_a_warning() {
    let plan = parse_sqlite_query_plan(&QueryResult::empty()).unwrap();

    assert!(plan.roots.is_empty());
    assert_eq!(
        plan.warnings,
        ["The engine reported no plan steps for this statement."]
    );
}

#[test]
fn a_malformed_row_is_an_error() {
    let mut malformed = QueryResult::empty();
    malformed.rows = vec![vec![Value::Text("x".into())]];
    assert!(parse_sqlite_query_plan(&malformed).is_err());
}

#[test]
fn plan_text_is_read_from_the_first_cell_or_the_text_body() {
    let mut result = QueryResult::empty();
    result.rows = vec![vec![Value::Json("{\"a\":1}".to_string())]];
    assert_eq!(plan_text_from_result(&result).unwrap(), "{\"a\":1}");

    let mut text = QueryResult::empty();
    text.text_body = Some("plan".to_string());
    assert_eq!(plan_text_from_result(&text).unwrap(), "plan");

    assert!(plan_text_from_result(&QueryResult::empty()).is_err());
}

fn costed(operation: &str, total: f64, children: Vec<PlanNode>) -> PlanNode {
    PlanNode {
        estimated_total_cost: Some(total),
        children,
        ..PlanNode::new(operation)
    }
}

#[test]
fn self_cost_subtracts_the_direct_children() {
    let node = costed(
        "Nested Loop",
        100.0,
        vec![
            costed("Seq Scan", 60.0, vec![]),
            costed("Index Scan", 15.0, vec![]),
        ],
    );

    assert_eq!(node.estimated_self_cost(), Some(25.0));
}

#[test]
fn self_cost_never_goes_below_zero() {
    let node = costed("Limit", 10.0, vec![costed("Sort", 12.0, vec![])]);

    assert_eq!(node.estimated_self_cost(), Some(0.0));
}

#[test]
fn self_cost_is_unknown_when_a_child_has_no_cost() {
    let node = costed("Nested Loop", 100.0, vec![PlanNode::new("SCAN t")]);

    assert_eq!(node.estimated_self_cost(), None);
    assert_eq!(PlanNode::new("SCAN t").estimated_self_cost(), None);
}

#[test]
fn search_gets_a_description_and_its_condition_as_the_key_condition() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(3, 0, "SEARCH t USING INDEX idx_a (a=?)")]))
        .unwrap();
    let node = &plan.roots[0];

    assert_eq!(
        node.description.as_deref(),
        Some("Looks rows up through an index instead of scanning the whole table.")
    );
    let condition = node.key_condition.as_ref().expect("a key condition");
    assert_eq!(condition.label, "Condition");
    assert_eq!(condition.value, "(a=?)");
}

#[test]
fn a_scan_has_a_description_and_no_key_condition() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(3, 0, "SCAN t")])).unwrap();

    assert!(plan.roots[0].description.is_some());
    assert_eq!(plan.roots[0].key_condition, None);
}

#[test]
fn engine_steps_get_their_glossary_text() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[
        (1, 0, "USE TEMP B-TREE FOR ORDER BY"),
        (2, 0, "MATERIALIZE x"),
        (3, 0, "CO-ROUTINE y"),
        (4, 0, "COMPOUND QUERY"),
        (5, 0, "SCAN CONSTANT ROW"),
        (6, 0, "SCAN SUBQUERY 1"),
    ]))
    .unwrap();

    assert_eq!(
        plan.roots[0].description.as_deref(),
        Some("Builds a temporary index to group or order the rows.")
    );
    for node in &plan.roots {
        assert!(
            node.description.is_some(),
            "{} has no description",
            node.operation
        );
    }
}

#[test]
fn an_unknown_step_has_no_description() {
    let plan = parse_sqlite_query_plan(&eqp_result(&[(2, 0, "TELEPORT t")])).unwrap();

    assert_eq!(plan.roots[0].description, None);
    assert_eq!(plan.roots[0].key_condition, None);
}
