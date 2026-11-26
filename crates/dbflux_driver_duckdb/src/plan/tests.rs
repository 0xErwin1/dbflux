use super::*;
use dbflux_core::{
    Connection, ConnectionProfile, DbConfig, DbDriver, PlanFormat, QueryPlanRequest, QueryRequest,
    QueryResult, Value,
};

const JOIN_PLAN: &str = r#"[
    {
        "name": "ORDER_BY",
        "children": [
            {
                "name": "HASH_JOIN",
                "children": [
                    {
                        "name": "SEQ_SCAN",
                        "children": [],
                        "extra_info": {
                            "Table": "memory.main.t",
                            "Type": "Sequential Scan",
                            "Projections": ["id", "a"],
                            "Filters": "(id = 5)",
                            "Estimated Cardinality": "2"
                        }
                    },
                    {
                        "name": "EMPTY_RESULT",
                        "children": [],
                        "extra_info": {}
                    }
                ],
                "extra_info": {
                    "Join Type": "INNER",
                    "Conditions": "id = id",
                    "Estimated Cardinality": "~1"
                }
            }
        ],
        "extra_info": {"Order By": "a ASC"}
    }
]"#;

fn detail<'a>(node: &'a dbflux_core::PlanNode, key: &str) -> Option<&'a str> {
    node.details
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

fn connect() -> Box<dyn Connection> {
    crate::DuckDbDriver::new()
        .connect(&ConnectionProfile::new("duck", DbConfig::default_duckdb()))
        .unwrap()
}

#[test]
fn the_json_tree_maps_names_relations_rows_and_details() {
    let plan = parse_explain_json(JOIN_PLAN).unwrap();

    assert_eq!(plan.raw_format, PlanFormat::Json);
    assert_eq!(plan.raw, JOIN_PLAN);
    assert_eq!(plan.roots.len(), 1);

    let order = &plan.roots[0];
    assert_eq!(order.operation, "ORDER_BY");
    assert_eq!(detail(order, "Order By"), Some("a ASC"));
    assert_eq!(order.estimated_rows, None);

    let join = &order.children[0];
    assert_eq!(join.operation, "HASH_JOIN");
    assert_eq!(join.estimated_rows, Some(1.0));
    assert_eq!(detail(join, "Join Type"), Some("INNER"));
    assert_eq!(detail(join, "Estimated Cardinality"), None);
    assert_eq!(join.estimated_total_cost, None);
    assert_eq!(join.children.len(), 2);

    let scan = &join.children[0];
    assert_eq!(scan.operation, "SEQ_SCAN");
    assert_eq!(scan.relation.as_deref(), Some("memory.main.t"));
    assert_eq!(scan.estimated_rows, Some(2.0));
    assert_eq!(detail(scan, "Type"), Some("Sequential Scan"));
    assert_eq!(detail(scan, "Projections"), Some("id, a"));
    assert_eq!(detail(scan, "Filters"), Some("(id = 5)"));
    assert_eq!(detail(scan, "Table"), None);
}

#[test]
fn a_textual_extra_info_is_kept_as_a_detail() {
    let plan =
        parse_explain_json(r#"[{"name":"SEQ_SCAN","children":[],"extra_info":"Table: t\nEC: 5"}]"#)
            .unwrap();

    assert_eq!(
        detail(&plan.roots[0], "extra_info"),
        Some("Table: t\nEC: 5")
    );
}

#[test]
fn invalid_or_unexpected_json_is_an_error() {
    assert!(parse_explain_json("not json").is_err());
    assert!(parse_explain_json("{}").is_err());
    assert!(parse_explain_json("[]").is_err());
    assert!(parse_explain_json(r#"[{"children":[]}]"#).is_err());
}

#[test]
fn the_plan_text_is_read_from_the_physical_plan_row() {
    let mut result = QueryResult::empty();
    result.rows = vec![
        vec![Value::Text("logical_plan".into()), Value::Text("[]".into())],
        vec![
            Value::Text("physical_plan".into()),
            Value::Text("[{\"name\":\"X\"}]".into()),
        ],
    ];

    assert_eq!(
        plan_text_from_result(&result).unwrap(),
        "[{\"name\":\"X\"}]"
    );
    assert!(plan_text_from_result(&QueryResult::empty()).is_err());
}

#[test]
fn explain_statement_wraps_one_statement_without_analyze() {
    assert_eq!(
        explain_statement(" DELETE FROM t WHERE id = 1; ").unwrap(),
        "EXPLAIN (FORMAT JSON) DELETE FROM t WHERE id = 1"
    );
}

#[test]
fn several_statements_and_empty_queries_are_refused() {
    assert_eq!(
        explain_statement("SELECT 1; SELECT 2")
            .unwrap_err()
            .to_string(),
        "A query plan covers exactly one statement; the query has 2. Select a single statement."
    );
    assert_eq!(
        explain_statement(" ").unwrap_err().to_string(),
        "A query plan needs a statement; the query is empty."
    );
}

#[test]
fn the_connection_returns_a_structured_plan_and_never_runs_the_statement() {
    let connection = connect();
    connection
        .execute(&QueryRequest::new(
            "CREATE TABLE plan_test (id INTEGER PRIMARY KEY, name VARCHAR);
             INSERT INTO plan_test VALUES (1, 'kept')",
        ))
        .unwrap();

    let plan = connection
        .query_plan(&QueryPlanRequest::new(
            "SELECT * FROM plan_test WHERE id = 1",
        ))
        .unwrap();
    assert_eq!(plan.raw_format, PlanFormat::Json);
    assert_eq!(plan.roots.len(), 1);

    fn find_relation(node: &dbflux_core::PlanNode) -> Option<String> {
        node.relation
            .clone()
            .or_else(|| node.children.iter().find_map(find_relation))
    }
    let relation = find_relation(&plan.roots[0]).expect("a scan names its table");
    assert!(relation.ends_with("plan_test"), "{relation}");

    connection
        .query_plan(&QueryPlanRequest::new("DELETE FROM plan_test"))
        .unwrap();
    let remaining = connection
        .execute(&QueryRequest::new("SELECT count(*) FROM plan_test"))
        .unwrap();
    assert_eq!(remaining.rows[0][0], Value::Int(1));

    assert!(
        connection
            .query_plan(&QueryPlanRequest::new("SELECT 1; SELECT 2"))
            .is_err()
    );
}
