use super::*;
use dbflux_core::PlanFormat;

const NESTED_JOIN: &str = r#"[{
  "Plan": {
    "Node Type": "Nested Loop",
    "Join Type": "Inner",
    "Startup Cost": 0.29,
    "Total Cost": 16.34,
    "Plan Rows": 5,
    "Plan Width": 72,
    "Inner Unique": false,
    "Plans": [
      {
        "Node Type": "Seq Scan",
        "Parent Relationship": "Outer",
        "Relation Name": "orders",
        "Alias": "o",
        "Startup Cost": 0.00,
        "Total Cost": 1.05,
        "Plan Rows": 5,
        "Filter": "(status = 'open'::text)"
      },
      {
        "Node Type": "Index Scan",
        "Parent Relationship": "Inner",
        "Scan Direction": "Forward",
        "Index Name": "customers_pkey",
        "Relation Name": "customers",
        "Alias": "c",
        "Startup Cost": 0.29,
        "Total Cost": 3.05,
        "Plan Rows": 1,
        "Index Cond": "(id = o.customer_id)"
      }
    ]
  }
}]"#;

fn detail<'a>(node: &'a dbflux_core::PlanNode, key: &str) -> Option<&'a str> {
    node.details
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

#[test]
fn nested_join_maps_the_tree_costs_relations_and_index() {
    let plan = parse_explain_json(NESTED_JOIN).unwrap();

    assert_eq!(plan.raw_format, PlanFormat::Json);
    assert_eq!(plan.raw, NESTED_JOIN);
    assert!(plan.warnings.is_empty());
    assert_eq!(plan.roots.len(), 1);

    let root = &plan.roots[0];
    assert_eq!(root.operation, "Nested Loop");
    assert_eq!(root.estimated_startup_cost, Some(0.29));
    assert_eq!(root.estimated_total_cost, Some(16.34));
    assert_eq!(root.estimated_rows, Some(5.0));
    assert_eq!(detail(root, "Join Type"), Some("Inner"));
    assert_eq!(root.children.len(), 2);

    let seq = &root.children[0];
    assert_eq!(seq.operation, "Seq Scan");
    assert_eq!(seq.relation.as_deref(), Some("orders"));
    assert_eq!(seq.index, None);
    assert_eq!(detail(seq, "Filter"), Some("(status = 'open'::text)"));
    assert_eq!(detail(seq, "Alias"), Some("o"));
    assert!(seq.children.is_empty());

    let index = &root.children[1];
    assert_eq!(index.operation, "Index Scan");
    assert_eq!(index.relation.as_deref(), Some("customers"));
    assert_eq!(index.index.as_deref(), Some("customers_pkey"));
    assert_eq!(detail(index, "Index Cond"), Some("(id = o.customer_id)"));
}

#[test]
fn keys_already_mapped_to_fields_are_not_repeated_as_details() {
    let plan = parse_explain_json(NESTED_JOIN).unwrap();
    let index = &plan.roots[0].children[1];

    for key in [
        "Node Type",
        "Relation Name",
        "Index Name",
        "Startup Cost",
        "Total Cost",
        "Plan Rows",
        "Plans",
    ] {
        assert_eq!(detail(index, key), None, "{key} must not be a detail");
    }
}

#[test]
fn seq_scan_with_only_a_node_type_leaves_optional_fields_empty() {
    let raw = r#"[{"Plan": {"Node Type": "Result"}}]"#;
    let plan = parse_explain_json(raw).unwrap();

    let node = &plan.roots[0];
    assert_eq!(node.operation, "Result");
    assert_eq!(node.relation, None);
    assert_eq!(node.index, None);
    assert_eq!(node.estimated_startup_cost, None);
    assert_eq!(node.estimated_total_cost, None);
    assert_eq!(node.estimated_rows, None);
    assert!(node.details.is_empty());
    assert!(node.children.is_empty());
}

#[test]
fn non_string_attributes_are_kept_as_text_details() {
    let raw = r#"[{"Plan": {"Node Type": "Sort", "Sort Key": ["a", "b DESC"], "Parallel Aware": false}}]"#;
    let plan = parse_explain_json(raw).unwrap();
    let node = &plan.roots[0];

    assert_eq!(detail(node, "Sort Key"), Some("a, b DESC"));
    assert_eq!(detail(node, "Parallel Aware"), Some("false"));
}

#[test]
fn malformed_json_is_an_error() {
    assert!(parse_explain_json("not json").is_err());
}

#[test]
fn json_without_a_plan_is_an_error() {
    assert!(parse_explain_json("[]").is_err());
    assert!(parse_explain_json(r#"[{"Other": 1}]"#).is_err());
    assert!(parse_explain_json(r#"{"Plan": {"Node Type": "Result"}}"#).is_err());
}

#[test]
fn a_plan_node_without_a_node_type_is_an_error() {
    assert!(parse_explain_json(r#"[{"Plan": {"Total Cost": 1.0}}]"#).is_err());
}

#[test]
fn explain_statement_wraps_a_single_statement_without_analyze() {
    assert_eq!(
        explain_statement("  SELECT 1;  ").unwrap(),
        "EXPLAIN (FORMAT JSON) SELECT 1"
    );
    assert_eq!(
        explain_statement("DELETE FROM t WHERE id = 1").unwrap(),
        "EXPLAIN (FORMAT JSON) DELETE FROM t WHERE id = 1"
    );
}

#[test]
fn explain_statement_ignores_semicolons_inside_literals() {
    assert_eq!(
        explain_statement("SELECT 'a;b'").unwrap(),
        "EXPLAIN (FORMAT JSON) SELECT 'a;b'"
    );
}

#[test]
fn multiple_statements_are_refused_before_anything_is_sent() {
    let error = explain_statement("SELECT 1; DELETE FROM t").unwrap_err();

    assert_eq!(
        error.to_string(),
        "A query plan covers exactly one statement; the query has 2. Select a single statement."
    );
}

#[test]
fn empty_queries_are_refused() {
    for query in ["", "   \n\t", " ; "] {
        let error = explain_statement(query).unwrap_err();
        assert_eq!(
            error.to_string(),
            "A query plan needs a statement; the query is empty."
        );
    }
}

#[test]
fn raw_plan_is_read_from_the_json_cell() {
    let mut result = QueryResult::empty();
    result.rows = vec![vec![dbflux_core::Value::Json(
        "[{\"Plan\":{}}]".to_string(),
    )]];

    assert_eq!(raw_plan_from_result(&result).unwrap(), "[{\"Plan\":{}}]");
}

#[test]
fn raw_plan_from_an_empty_result_is_an_error() {
    assert!(raw_plan_from_result(&QueryResult::empty()).is_err());
}
