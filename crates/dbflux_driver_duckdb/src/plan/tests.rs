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

fn condition(node: &dbflux_core::PlanNode) -> Option<(&str, &str)> {
    node.key_condition
        .as_ref()
        .map(|condition| (condition.label.as_str(), condition.value.as_str()))
}

#[test]
fn known_operators_get_a_description_and_their_extra_info_condition() {
    let plan = parse_explain_json(JOIN_PLAN).unwrap();
    let order = &plan.roots[0];
    let join = &order.children[0];
    let scan = &join.children[0];

    assert_eq!(
        order.description.as_deref(),
        Some("Orders the rows by the sort key.")
    );
    assert_eq!(condition(order), Some(("Order By", "a ASC")));
    assert!(join.description.is_some());
    assert_eq!(condition(join), Some(("Conditions", "id = id")));
    assert!(scan.description.is_some());
    assert_eq!(condition(scan), Some(("Filters", "(id = 5)")));
}

#[test]
fn conditions_win_over_filters() {
    let plan = parse_explain_json(
        r#"[{"name":"HASH_JOIN","children":[],"extra_info":{"Filters":"x > 1","Conditions":"a = b"}}]"#,
    )
    .unwrap();

    assert_eq!(condition(&plan.roots[0]), Some(("Conditions", "a = b")));
}

#[test]
fn an_unknown_operator_has_no_description_and_a_textual_extra_info_no_condition() {
    let plan =
        parse_explain_json(r#"[{"name":"TELEPORT","children":[],"extra_info":"Filters: x"}]"#)
            .unwrap();

    assert_eq!(plan.roots[0].description, None);
    assert_eq!(plan.roots[0].key_condition, None);
}

#[test]
fn every_listed_operator_has_a_description() {
    for name in [
        "SEQ_SCAN",
        "TABLE_SCAN",
        "FILTER",
        "PROJECTION",
        "HASH_JOIN",
        "HASH_GROUP_BY",
        "ORDER_BY",
        "TOP_N",
    ] {
        let raw = serde_json::json!([{ "name": name, "children": [] }]).to_string();
        let plan = parse_explain_json(&raw).unwrap();

        assert!(
            plan.roots[0].description.is_some(),
            "{name} has no description"
        );
    }
}

#[test]
fn a_standalone_filter_uses_its_expression_as_the_key_condition() {
    let plan = parse_explain_json(
        r#"[{"name":"FILTER","children":[],"extra_info":{"Expression":"(r > 0.5)"}}]"#,
    )
    .unwrap();

    assert_eq!(condition(&plan.roots[0]), Some(("Expression", "(r > 0.5)")));

    let join = parse_explain_json(
        r#"[{"name":"HASH_JOIN","children":[],"extra_info":{"Expression":"x","Conditions":"a = b"}}]"#,
    )
    .unwrap();
    assert_eq!(condition(&join.roots[0]), Some(("Conditions", "a = b")));

    let other = parse_explain_json(
        r#"[{"name":"PROJECTION","children":[],"extra_info":{"Expression":"x"}}]"#,
    )
    .unwrap();
    assert_eq!(condition(&other.roots[0]), None);
}

#[test]
fn a_standalone_filter_from_the_engine_gets_a_key_condition() {
    let plan = connect()
        .query_plan(&QueryPlanRequest::new(
            "SELECT * FROM (SELECT range AS a, random() AS r FROM range(10)) WHERE r > 0.5",
        ))
        .unwrap();

    assert_eq!(plan.roots[0].operation, "FILTER");
    assert_eq!(condition(&plan.roots[0]), Some(("Expression", "(r > 0.5)")));
}

#[test]
fn a_logical_plan_is_never_returned_as_the_physical_plan() {
    let mut result = QueryResult::empty();
    result.rows = vec![vec![
        Value::Text("logical_opt".into()),
        Value::Text("[{\"name\":\"X\"}]".into()),
    ]];
    assert!(plan_text_from_result(&result).is_err());

    let connection = connect();
    connection
        .execute(&QueryRequest::new("SET explain_output = 'optimized_only'"))
        .unwrap();
    let error = connection
        .query_plan(&QueryPlanRequest::new("SELECT 1"))
        .unwrap_err();
    assert!(error.to_string().contains("physical plan"), "{error}");
}

const ANALYZED_PLAN: &str = r#"{
    "operator": [
        {
            "extra_info": {},
            "intermediate_rows": 0,
            "timing": 0.0,
            "type": "RESULT_COLLECTOR",
            "children": [
                {
                    "extra_info": {},
                    "intermediate_rows": 0,
                    "timing": 0.0,
                    "type": "EXECUTE",
                    "children": [
                        {
                            "extra_info": {},
                            "intermediate_rows": 0,
                            "timing": 4.53e-7,
                            "type": "EXPLAIN_ANALYZE",
                            "children": [
                                {
                                    "extra_info": {
                                        "Order By": "count_star() ASC",
                                        "Estimated Cardinality": "6"
                                    },
                                    "intermediate_rows": 7,
                                    "timing": 0.0002,
                                    "type": "ORDER_BY",
                                    "children": [
                                        {
                                            "extra_info": {
                                                "Table": "memory.main.p",
                                                "Type": "Sequential Scan",
                                                "Filters": "(id > 10)",
                                                "Estimated Cardinality": "200"
                                            },
                                            "intermediate_rows": 989,
                                            "row_groups_scanned": 1,
                                            "rows_scanned": 1000,
                                            "timing": 0.0001,
                                            "type": "TABLE_SCAN"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ],
    "planner": {"total_time": 0.003},
    "optimizer": {"total_time": 0.002},
    "physical_planner": {"total_time": 0.001},
    "query": {"total_time": 0.008}
}"#;

#[test]
fn an_analyzed_plan_skips_the_explain_wrappers() {
    let plan = parse_analyzed_json(ANALYZED_PLAN).unwrap();

    assert_eq!(plan.raw_format, PlanFormat::Json);
    assert_eq!(plan.roots.len(), 1);
    assert_eq!(plan.roots[0].operation, "ORDER_BY");
    assert_eq!(plan.roots[0].children[0].operation, "TABLE_SCAN");
}

#[test]
fn an_analyzed_plan_maps_cardinality_and_cumulative_timing() {
    let plan = parse_analyzed_json(ANALYZED_PLAN).unwrap();

    let sort = &plan.roots[0];
    let scan = &sort.children[0];

    assert_eq!(scan.estimated_rows, Some(200.0));
    assert_eq!(scan.actual_rows, Some(989.0));
    assert_eq!(scan.actual_loops, Some(1));
    // Operator timing is seconds spent in the operator alone; the model keeps
    // an inclusive total in milliseconds, so self time comes back out.
    assert!((scan.actual_total_time.unwrap() - 0.1).abs() < 1e-9);
    assert!((scan.actual_self_time().unwrap() - 0.1).abs() < 1e-9);
    assert!((sort.actual_total_time.unwrap() - 0.3).abs() < 1e-9);
    assert!((sort.actual_self_time().unwrap() - 0.2).abs() < 1e-9);
    assert_eq!(sort.actual_rows, Some(7.0));
}

#[test]
fn an_analyzed_plan_keeps_scan_counters_as_details() {
    let plan = parse_analyzed_json(ANALYZED_PLAN).unwrap();
    let scan = &plan.roots[0].children[0];

    assert_eq!(detail(scan, "rows_scanned"), Some("1000"));
    assert_eq!(detail(scan, "row_groups_scanned"), Some("1"));
    assert_eq!(detail(scan, "intermediate_rows"), None);
    assert_eq!(detail(scan, "timing"), None);
    assert_eq!(scan.relation.as_deref(), Some("memory.main.p"));
}

#[test]
fn an_analyzed_plan_reports_planning_and_execution_time_in_milliseconds() {
    let plan = parse_analyzed_json(ANALYZED_PLAN).unwrap();

    assert!((plan.planning_time.unwrap() - 6.0).abs() < 1e-9);
    assert!((plan.execution_time.unwrap() - 2.0).abs() < 1e-9);
}

#[test]
fn the_analyzed_gap_comes_out_of_the_parsed_plan() {
    let plan = parse_analyzed_json(ANALYZED_PLAN).unwrap();

    // 989 actual rows against 200 estimated: 4.945x, under the threshold.
    assert!(plan.largest_estimate_gap().is_none());
}

#[test]
fn an_analyzed_document_without_operators_is_an_error() {
    assert!(parse_analyzed_json("{}").is_err());
    assert!(parse_analyzed_json("[]").is_err());
    assert!(parse_analyzed_json("nope").is_err());
}

#[test]
fn the_analyzed_plan_row_is_found_by_its_key() {
    let mut result = QueryResult::empty();
    result.rows = vec![vec![
        Value::Text("analyzed_plan".to_string()),
        Value::Text("{}".to_string()),
    ]];

    assert_eq!(plan_text_from_result(&result).unwrap(), "{}");
}

#[test]
fn the_analyze_statement_runs_the_one_read_statement_with_analyze() {
    assert_eq!(
        analyze_explain_statement("SELECT 1;").unwrap(),
        "EXPLAIN (ANALYZE, FORMAT JSON) SELECT 1"
    );
}

#[test]
fn the_analyze_statement_refuses_writes_and_batches() {
    for query in ["UPDATE t SET a = 1", "DELETE FROM t", "DROP TABLE t"] {
        assert_eq!(
            analyze_explain_statement(query).unwrap_err().to_string(),
            "Analyze is only available for read statements; this statement is not a plain read, so only the estimate can run.",
            "{query}"
        );
    }

    assert!(analyze_explain_statement("SELECT 1; SELECT 2").is_err());
}

#[test]
fn analyzing_a_real_select_reports_actuals_and_changes_nothing() {
    let connection = connect();
    connection
        .execute(&QueryRequest::new(
            "CREATE TABLE p AS SELECT range AS id, range % 7 AS g FROM range(1000)",
        ))
        .unwrap();

    let plan = connection
        .query_plan(&QueryPlanRequest::analyzed(
            "SELECT g, count(*) FROM p WHERE id > 10 GROUP BY g ORDER BY 2",
        ))
        .unwrap();

    assert!(plan.execution_time.is_some());
    assert!(plan.planning_time.is_some());
    let mut stack: Vec<&dbflux_core::PlanNode> = plan.roots.iter().collect();
    let mut scanned = None;
    while let Some(node) = stack.pop() {
        if node.relation.as_deref() == Some("memory.main.p") {
            scanned = Some(node);
        }
        stack.extend(&node.children);
    }
    let scan = scanned.expect("a node scans p");
    assert_eq!(scan.actual_rows, Some(989.0));
    assert!(scan.actual_total_time.is_some());

    let count = connection
        .execute(&QueryRequest::new("SELECT count(*) FROM p"))
        .unwrap();
    assert_eq!(count.rows[0][0], Value::Int(1000));
}

#[test]
fn analyzing_a_write_is_refused_and_the_rows_stay() {
    let connection = connect();
    connection
        .execute(&QueryRequest::new("CREATE TABLE guard AS SELECT 1 AS id"))
        .unwrap();

    for statement in [
        "DELETE FROM guard",
        "UPDATE guard SET id = 2",
        "INSERT INTO guard VALUES (3)",
    ] {
        let error = connection
            .query_plan(&QueryPlanRequest::analyzed(statement))
            .unwrap_err();
        assert!(
            error.to_string().starts_with("Analyze is only available"),
            "{statement}: {error}"
        );
    }

    let rows = connection
        .execute(&QueryRequest::new("SELECT id FROM guard"))
        .unwrap();
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn the_estimate_request_still_never_analyzes() {
    let connection = connect();
    connection
        .execute(&QueryRequest::new("CREATE TABLE est AS SELECT 1 AS id"))
        .unwrap();

    let plan = connection
        .query_plan(&QueryPlanRequest::new("DELETE FROM est"))
        .unwrap();
    assert!(plan.execution_time.is_none());

    let rows = connection
        .execute(&QueryRequest::new("SELECT count(*) FROM est"))
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Int(1));
}
