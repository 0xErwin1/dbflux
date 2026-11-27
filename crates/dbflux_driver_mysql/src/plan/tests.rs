use super::*;
use dbflux_core::PlanFormat;

const MYSQL_8_JOIN: &str = r#"{
  "query_block": {
    "select_id": 1,
    "cost_info": {"query_cost": "2.35"},
    "nested_loop": [
      {
        "table": {
          "table_name": "o",
          "access_type": "ALL",
          "possible_keys": ["customer_id"],
          "rows_examined_per_scan": 5,
          "rows_produced_per_join": 5,
          "filtered": "100.00",
          "cost_info": {
            "read_cost": "0.75",
            "eval_cost": "0.50",
            "prefix_cost": "1.25",
            "data_read_per_join": "160"
          },
          "used_columns": ["id", "customer_id"]
        }
      },
      {
        "table": {
          "table_name": "c",
          "access_type": "eq_ref",
          "possible_keys": ["PRIMARY"],
          "key": "PRIMARY",
          "used_key_parts": ["id"],
          "key_length": "4",
          "ref": ["shop.o.customer_id"],
          "rows_examined_per_scan": 1,
          "rows_produced_per_join": 5,
          "filtered": "100.00",
          "cost_info": {
            "read_cost": "0.60",
            "eval_cost": "0.50",
            "prefix_cost": "2.35",
            "data_read_per_join": "1K"
          },
          "used_columns": ["id", "name"]
        }
      }
    ]
  }
}"#;

const MYSQL_8_SORT: &str = r#"{
  "query_block": {
    "select_id": 1,
    "cost_info": {"query_cost": "0.35"},
    "ordering_operation": {
      "using_filesort": true,
      "table": {
        "table_name": "t",
        "access_type": "ref",
        "key": "idx_a",
        "rows_examined_per_scan": 3,
        "cost_info": {"read_cost": "0.25", "eval_cost": "0.10", "prefix_cost": "0.35"}
      }
    }
  }
}"#;

const MYSQL_8_DELETE: &str = r#"{
  "query_block": {
    "select_id": 1,
    "table": {
      "delete": true,
      "table_name": "t",
      "access_type": "range",
      "key": "PRIMARY",
      "rows_examined_per_scan": 1
    }
  }
}"#;

const MARIADB_JOIN: &str = r#"{
  "query_block": {
    "select_id": 1,
    "cost": 0.0125,
    "nested_loop": [
      {
        "table": {
          "table_name": "o",
          "access_type": "ALL",
          "loops": 1,
          "rows": 5,
          "cost": 0.0043,
          "filtered": 100,
          "attached_condition": "o.status = 'open'"
        }
      },
      {
        "table": {
          "table_name": "c",
          "access_type": "eq_ref",
          "possible_keys": ["PRIMARY"],
          "key": "PRIMARY",
          "key_length": "4",
          "used_key_parts": ["id"],
          "ref": ["shop.o.customer_id"],
          "loops": 5,
          "rows": 1,
          "cost": 0.0082,
          "filtered": 100
        }
      }
    ]
  }
}"#;

const MARIADB_SORT: &str = r#"{
  "query_block": {
    "select_id": 1,
    "read_sorted_file": {
      "filesort": {
        "sort_key": "t.b",
        "table": {
          "table_name": "t",
          "access_type": "ALL",
          "rows": 10,
          "filtered": 100
        }
      }
    }
  }
}"#;

fn detail<'a>(node: &'a dbflux_core::PlanNode, key: &str) -> Option<&'a str> {
    node.details
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

#[test]
fn mysql_8_nested_loop_maps_tables_access_keys_rows_and_costs() {
    let plan = parse_explain_json(MYSQL_8_JOIN).unwrap();

    assert_eq!(plan.raw_format, PlanFormat::Json);
    assert_eq!(plan.raw, MYSQL_8_JOIN);
    assert_eq!(plan.roots.len(), 1);

    let block = &plan.roots[0];
    assert_eq!(block.operation, "Query block");
    assert_eq!(block.estimated_total_cost, Some(2.35));
    assert_eq!(detail(block, "select_id"), Some("1"));

    let loop_node = &block.children[0];
    assert_eq!(loop_node.operation, "Nested loop");
    assert_eq!(loop_node.children.len(), 2);

    let first = &loop_node.children[0];
    assert_eq!(first.operation, "ALL");
    assert_eq!(first.relation.as_deref(), Some("o"));
    assert_eq!(first.index, None);
    assert_eq!(first.estimated_rows, Some(5.0));
    assert_eq!(first.estimated_total_cost, Some(1.25));
    assert_eq!(detail(first, "read_cost"), Some("0.75"));
    assert_eq!(detail(first, "possible_keys"), Some("customer_id"));
    assert_eq!(detail(first, "access_type"), None);
    assert_eq!(detail(first, "table_name"), None);

    let second = &loop_node.children[1];
    assert_eq!(second.operation, "eq_ref");
    assert_eq!(second.relation.as_deref(), Some("c"));
    assert_eq!(second.index.as_deref(), Some("PRIMARY"));
    assert_eq!(second.estimated_total_cost, Some(1.1));
    assert_eq!(detail(second, "prefix_cost"), Some("2.35"));
    assert_eq!(detail(first, "prefix_cost"), Some("1.25"));
    assert_eq!(detail(second, "used_key_parts"), Some("id"));
}

#[test]
fn mysql_8_operations_wrap_their_table() {
    let plan = parse_explain_json(MYSQL_8_SORT).unwrap();

    let ordering = &plan.roots[0].children[0];
    assert_eq!(ordering.operation, "Ordering operation");
    assert_eq!(detail(ordering, "using_filesort"), Some("true"));
    assert_eq!(ordering.children[0].index.as_deref(), Some("idx_a"));
    assert_eq!(ordering.children[0].estimated_rows, Some(3.0));
}

#[test]
fn mysql_8_delete_plans_a_table_without_a_nested_loop() {
    let plan = parse_explain_json(MYSQL_8_DELETE).unwrap();

    let table = &plan.roots[0].children[0];
    assert_eq!(table.operation, "range");
    assert_eq!(table.relation.as_deref(), Some("t"));
    assert_eq!(table.index.as_deref(), Some("PRIMARY"));
    assert_eq!(detail(table, "delete"), Some("true"));
}

#[test]
fn a_single_table_nested_loop_has_no_wrapper() {
    let plan = parse_explain_json(
        r#"{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"t","access_type":"ALL"}}]}}"#,
    )
    .unwrap();

    assert_eq!(plan.roots[0].children.len(), 1);
    assert_eq!(plan.roots[0].children[0].relation.as_deref(), Some("t"));
}

#[test]
fn mariadb_maps_rows_and_numeric_costs() {
    let plan = parse_explain_json(MARIADB_JOIN).unwrap();

    let block = &plan.roots[0];
    assert_eq!(block.estimated_total_cost, Some(0.0125));

    let loop_node = &block.children[0];
    let first = &loop_node.children[0];
    assert_eq!(first.operation, "ALL");
    assert_eq!(first.relation.as_deref(), Some("o"));
    assert_eq!(first.estimated_rows, Some(5.0));
    assert_eq!(first.estimated_total_cost, Some(0.0043));
    assert_eq!(
        detail(first, "attached_condition"),
        Some("o.status = 'open'")
    );
    assert_eq!(detail(first, "loops"), Some("1"));

    let second = &loop_node.children[1];
    assert_eq!(second.index.as_deref(), Some("PRIMARY"));
    assert_eq!(second.estimated_rows, Some(1.0));
}

#[test]
fn mariadb_filesort_wraps_its_table() {
    let plan = parse_explain_json(MARIADB_SORT).unwrap();

    let sorted = &plan.roots[0].children[0];
    assert_eq!(sorted.operation, "Read sorted file");
    let filesort = &sorted.children[0];
    assert_eq!(filesort.operation, "Filesort");
    assert_eq!(detail(filesort, "sort_key"), Some("t.b"));
    assert_eq!(filesort.children[0].relation.as_deref(), Some("t"));
}

#[test]
fn a_query_without_tables_keeps_its_message() {
    let plan = parse_explain_json(r#"{"query_block":{"select_id":1,"message":"No tables used"}}"#)
        .unwrap();

    assert!(plan.roots[0].children.is_empty());
    assert_eq!(detail(&plan.roots[0], "message"), Some("No tables used"));
}

#[test]
fn unions_keep_every_query_specification() {
    let plan = parse_explain_json(
        r#"{"query_block":{"union_result":{"using_temporary_table":true,"query_specifications":[
            {"dependent":false,"query_block":{"select_id":1,"table":{"table_name":"a","access_type":"ALL"}}},
            {"dependent":false,"query_block":{"select_id":2,"table":{"table_name":"b","access_type":"ALL"}}}
        ]}}}"#,
    )
    .unwrap();

    let union = &plan.roots[0].children[0];
    assert_eq!(union.operation, "Union result");
    assert_eq!(union.children.len(), 2);
    assert_eq!(
        union.children[1].children[0].children[0]
            .relation
            .as_deref(),
        Some("b")
    );
}

#[test]
fn invalid_or_unexpected_json_is_an_error() {
    assert!(parse_explain_json("not json").is_err());
    assert!(parse_explain_json("[]").is_err());
    assert!(parse_explain_json(r#"{"other":{}}"#).is_err());
}

#[test]
fn explain_statement_wraps_one_statement_without_analyze() {
    assert_eq!(
        explain_statement(" DELETE FROM t WHERE id = 1; ").unwrap(),
        "EXPLAIN FORMAT=JSON DELETE FROM t WHERE id = 1"
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

fn condition(node: &dbflux_core::PlanNode) -> Option<(&str, &str)> {
    node.key_condition
        .as_ref()
        .map(|condition| (condition.label.as_str(), condition.value.as_str()))
}

#[test]
fn access_types_get_a_description() {
    let plan = parse_explain_json(MARIADB_JOIN).unwrap();
    let nested = &plan.roots[0].children[0];

    assert_eq!(
        nested.children[0].description.as_deref(),
        Some("Reads every row of the table.")
    );
    assert!(nested.children[1].description.is_some());
    assert!(plan.roots[0].description.is_some());
}

#[test]
fn attached_condition_is_the_key_condition() {
    let plan = parse_explain_json(MARIADB_JOIN).unwrap();
    let scan = &plan.roots[0].children[0].children[0];

    assert_eq!(
        condition(scan),
        Some(("Attached condition", "o.status = 'open'"))
    );
}

#[test]
fn used_key_parts_are_the_key_condition_without_an_attached_condition() {
    let plan = parse_explain_json(MARIADB_JOIN).unwrap();
    let lookup = &plan.roots[0].children[0].children[1];

    assert_eq!(condition(lookup), Some(("Used key parts", "id")));
}

#[test]
fn an_attached_condition_wins_over_used_key_parts() {
    let plan = parse_explain_json(
        r#"{"query_block":{"table":{"table_name":"t","access_type":"ref","key":"k","used_key_parts":["a"],"attached_condition":"t.b > 1"}}}"#,
    )
    .unwrap();

    assert_eq!(
        condition(&plan.roots[0].children[0]),
        Some(("Attached condition", "t.b > 1"))
    );
}

#[test]
fn operation_containers_get_a_description() {
    let plan = parse_explain_json(MYSQL_8_SORT).unwrap();
    let ordering = &plan.roots[0].children[0];

    assert_eq!(ordering.operation, "Ordering operation");
    assert!(ordering.description.is_some());
    assert!(ordering.children[0].description.is_some());
}

#[test]
fn unknown_containers_and_access_types_have_no_description() {
    let plan = parse_explain_json(
        r#"{"query_block":{"teleport_operation":{"table":{"table_name":"t","access_type":"warp"}}}}"#,
    )
    .unwrap();
    let teleport = &plan.roots[0].children[0];

    assert_eq!(teleport.description, None);
    assert_eq!(teleport.children[0].description, None);
    assert_eq!(teleport.children[0].key_condition, None);
}

#[test]
fn every_listed_access_type_has_a_description() {
    for access in [
        "ALL",
        "index",
        "range",
        "ref",
        "eq_ref",
        "const",
        "system",
        "fulltext",
        "index_merge",
        "ref_or_null",
        "unique_subquery",
        "index_subquery",
    ] {
        let raw = serde_json::json!({"query_block": {"table": {"table_name": "t", "access_type": access}}})
            .to_string();
        let plan = parse_explain_json(&raw).unwrap();

        assert!(
            plan.roots[0].children[0].description.is_some(),
            "{access} has no description"
        );
    }
}

const MYSQL_ANALYZE_TREE: &str = "\
-> Sort: `count(*)` DESC  (actual time=0.142..0.142 rows=1 loops=1)
    -> Table scan on <temporary>  (actual time=0.136..0.136 rows=1 loops=1)
        -> Aggregate using temporary table  (actual time=0.135..0.135 rows=1 loops=1)
            -> Nested loop inner join  (cost=45.2 rows=5) (actual time=0.0283..0.124 rows=2 loops=1)
                -> Filter: (b.a_id is not null)  (cost=10.2 rows=100) (actual time=0.0183..0.0302 rows=100 loops=1)
                    -> Covering index scan on b using a_id  (cost=10.2 rows=100) (actual time=0.0177..0.0259 rows=100 loops=1)
                -> Filter: (a.k = 3)  (cost=0.25 rows=0.05) (actual time=878e-6..879e-6 rows=0.02 loops=100)
                    -> Single-row index lookup on a using PRIMARY (id=b.a_id)  (cost=0.25 rows=1) (actual time=767e-6..780e-6 rows=1 loops=100)
";

#[test]
fn the_analyze_tree_is_nested_by_indentation() {
    let plan = parse_analyze_tree(MYSQL_ANALYZE_TREE).unwrap();

    assert_eq!(plan.raw_format, PlanFormat::Text);
    assert_eq!(plan.raw, MYSQL_ANALYZE_TREE);
    assert_eq!(plan.roots.len(), 1);

    let sort = &plan.roots[0];
    assert_eq!(sort.operation, "Sort");
    let scan = &sort.children[0];
    assert_eq!(scan.operation, "Table scan");
    assert_eq!(scan.relation.as_deref(), Some("<temporary>"));
    let aggregate = &scan.children[0];
    assert_eq!(aggregate.operation, "Aggregate using temporary table");
    let join = &aggregate.children[0];
    assert_eq!(join.operation, "Nested loop inner join");
    assert_eq!(join.children.len(), 2);
    assert_eq!(join.children[0].operation, "Filter");
    assert_eq!(
        join.children[0].children[0].operation,
        "Covering index scan"
    );
}

#[test]
fn the_analyze_tree_maps_estimates_and_actuals_per_loop() {
    let plan = parse_analyze_tree(MYSQL_ANALYZE_TREE).unwrap();
    let join = &plan.roots[0].children[0].children[0].children[0];

    assert_eq!(join.estimated_total_cost, Some(45.2));
    assert_eq!(join.estimated_rows, Some(5.0));
    assert_eq!(join.actual_startup_time, Some(0.0283));
    assert_eq!(join.actual_total_time, Some(0.124));
    assert_eq!(join.actual_rows, Some(2.0));
    assert_eq!(join.actual_loops, Some(1));

    let lookup = &join.children[1].children[0];
    assert_eq!(lookup.actual_loops, Some(100));
    assert_eq!(lookup.actual_total_time, Some(780e-6));
    assert_eq!(lookup.actual_rows, Some(1.0));

    let sort = &plan.roots[0];
    assert_eq!(sort.estimated_total_cost, None);
    assert_eq!(sort.actual_total_time, Some(0.142));
}

#[test]
fn the_analyze_tree_reads_relation_index_and_conditions() {
    let plan = parse_analyze_tree(MYSQL_ANALYZE_TREE).unwrap();
    let join = &plan.roots[0].children[0].children[0].children[0];

    let covering = &join.children[0].children[0];
    assert_eq!(covering.relation.as_deref(), Some("b"));
    assert_eq!(covering.index.as_deref(), Some("a_id"));

    let filter = &join.children[0];
    let condition = filter.key_condition.as_ref().unwrap();
    assert_eq!(condition.label, "Filter");
    assert_eq!(condition.value, "(b.a_id is not null)");

    let lookup = &join.children[1].children[0];
    assert_eq!(lookup.operation, "Single-row index lookup");
    assert_eq!(lookup.relation.as_deref(), Some("a"));
    assert_eq!(lookup.index.as_deref(), Some("PRIMARY"));
    assert_eq!(lookup.key_condition.as_ref().unwrap().value, "(id=b.a_id)");

    let sort = &plan.roots[0];
    let sort_key = sort.key_condition.as_ref().unwrap();
    assert_eq!(sort_key.label, "Sort Key");
    assert_eq!(sort_key.value, "`count(*)` DESC");
}

#[test]
fn a_range_scan_keeps_its_range_and_index_condition() {
    let tree = "-> Limit: 3 row(s)  (cost=32.7 rows=3) (actual time=0.06..0.0606 rows=3 loops=1)\n    -> Index range scan on a using k over (40 < k), with index condition: (a.k > 40)  (cost=32.7 rows=72) (actual time=0.0597..0.0601 rows=3 loops=1)\n";
    let plan = parse_analyze_tree(tree).unwrap();

    let limit = &plan.roots[0];
    assert_eq!(limit.operation, "Limit");
    assert_eq!(limit.estimated_rows, Some(3.0));

    let scan = &limit.children[0];
    assert_eq!(scan.operation, "Index range scan");
    assert_eq!(scan.relation.as_deref(), Some("a"));
    assert_eq!(scan.index.as_deref(), Some("k"));
    let condition = scan.key_condition.as_ref().unwrap();
    assert_eq!(condition.label, "Index Cond");
    assert_eq!(condition.value, "(a.k > 40)");
    assert!(
        scan.details
            .iter()
            .any(|(name, value)| name == "Range" && value == "over (40 < k)"),
        "{:?}",
        scan.details
    );
}

#[test]
fn a_join_condition_is_the_key_condition() {
    let tree = "-> Inner hash join (b.a_id = a.id)  (cost=10 rows=5) (actual time=0.1..0.2 rows=5 loops=1)\n    -> Table scan on a  (cost=1 rows=5) (actual time=0.01..0.02 rows=5 loops=1)\n    -> Hash\n        -> Table scan on b  (cost=1 rows=5) (actual time=0.01..0.02 rows=5 loops=1)\n";
    let plan = parse_analyze_tree(tree).unwrap();

    let join = &plan.roots[0];
    assert_eq!(join.operation, "Inner hash join");
    let condition = join.key_condition.as_ref().unwrap();
    assert_eq!(condition.label, "Join Cond");
    assert_eq!(condition.value, "(b.a_id = a.id)");
    assert_eq!(join.children.len(), 2);
    assert_eq!(join.children[1].operation, "Hash");
    assert_eq!(join.children[1].children[0].relation.as_deref(), Some("b"));
}

#[test]
fn a_node_that_never_ran_has_zero_loops_and_no_gap() {
    let tree = "-> Nested loop left join  (cost=2 rows=1) (actual time=0.1..0.1 rows=1 loops=1)\n    -> Table scan on a  (cost=1 rows=1) (actual time=0.01..0.01 rows=1 loops=1)\n    -> Index lookup on b using a_id (a_id=a.id)  (cost=1 rows=900) (never executed)\n";
    let plan = parse_analyze_tree(tree).unwrap();

    let skipped = &plan.roots[0].children[1];
    assert_eq!(skipped.actual_loops, Some(0));
    assert_eq!(skipped.actual_total_time, Some(0.0));
    assert!(plan.largest_estimate_gap().is_none());
}

#[test]
fn the_tree_execution_time_is_the_roots_total_time() {
    let plan = parse_analyze_tree(MYSQL_ANALYZE_TREE).unwrap();

    assert_eq!(plan.execution_time, Some(0.142));
    assert_eq!(plan.planning_time, None);
}

#[test]
fn the_tree_self_time_and_gap_come_out_of_the_parsed_plan() {
    let plan = parse_analyze_tree(MYSQL_ANALYZE_TREE).unwrap();

    // Filter a.k = 3: estimated 0.05 rows per loop, 0.02 actual: no gap.
    // Index scan on b: 5 estimated, 2 actual: no gap. No node reaches 10x.
    assert!(plan.largest_estimate_gap().is_none());

    let join = &plan.roots[0].children[0].children[0].children[0];
    // 0.124 - (0.0302 + 0.000879 * 100) = 0.0059
    let self_time = join.actual_self_time().unwrap();
    assert!((self_time - 0.0059).abs() < 1e-9, "{self_time}");
}

#[test]
fn text_that_is_not_an_analyze_tree_is_an_error() {
    assert!(parse_analyze_tree("").is_err());
    assert!(parse_analyze_tree("just some words").is_err());
}

#[test]
fn the_analyze_statements_use_each_flavours_syntax_for_one_read_statement() {
    assert_eq!(
        analyze_statement(
            dbflux_core::DbKind::MySQL,
            Some("8.0.20"),
            "SELECT * FROM t;"
        )
        .unwrap(),
        "EXPLAIN ANALYZE SELECT * FROM t"
    );
    assert_eq!(
        analyze_statement(dbflux_core::DbKind::MySQL, None, "SELECT * FROM t;").unwrap(),
        "EXPLAIN ANALYZE SELECT * FROM t"
    );
    assert_eq!(
        analyze_statement(
            dbflux_core::DbKind::MySQL,
            Some("8.0.36"),
            "SELECT * FROM t;"
        )
        .unwrap(),
        "EXPLAIN ANALYZE FORMAT=TREE SELECT * FROM t"
    );
    assert_eq!(
        analyze_statement(
            dbflux_core::DbKind::MariaDB,
            Some("11.4.2-MariaDB"),
            "SELECT * FROM t;"
        )
        .unwrap(),
        "ANALYZE FORMAT=JSON SELECT * FROM t"
    );
}

#[test]
fn the_analyze_statements_refuse_writes_and_batches() {
    for kind in [dbflux_core::DbKind::MySQL, dbflux_core::DbKind::MariaDB] {
        for query in [
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "INSERT INTO t VALUES (1)",
            "TRUNCATE t",
            "SELECT * INTO OUTFILE '/tmp/x' FROM t",
        ] {
            let error = analyze_statement(kind, None, query).unwrap_err();
            assert_eq!(
                error.to_string(),
                "Analyze is only available for read statements; this statement is not a plain read, so only the estimate can run.",
                "{kind:?} {query}"
            );
        }

        assert!(analyze_statement(kind, None, "SELECT 1; SELECT 2").is_err());
    }
}

const MARIADB_ANALYZE: &str = r#"{
  "query_optimization": {"r_total_time_ms": 0.107},
  "query_block": {
    "select_id": 1,
    "cost": 0.019,
    "r_loops": 1,
    "r_total_time_ms": 0.062,
    "filesort": {
      "sort_key": "count(0) desc",
      "r_loops": 1,
      "r_total_time_ms": 0.005,
      "r_output_rows": 1,
      "temporary_table": {
        "nested_loop": [
          {
            "table": {
              "table_name": "a",
              "access_type": "ref",
              "key": "k",
              "used_key_parts": ["k"],
              "loops": 1,
              "r_loops": 1,
              "rows": 8,
              "r_rows": 80,
              "cost": 0.01,
              "r_table_time_ms": 0.012,
              "r_other_time_ms": 0.004,
              "r_engine_stats": {"pages_accessed": 10},
              "filtered": 100,
              "attached_condition": "a.k <=> 3"
            }
          },
          {
            "table": {
              "table_name": "b",
              "access_type": "ref",
              "key": "a_id",
              "loops": 8,
              "r_loops": 8,
              "rows": 1,
              "r_rows": 0.25,
              "r_table_time_ms": 0.016,
              "r_other_time_ms": 0.024
            }
          }
        ]
      }
    }
  }
}"#;

#[test]
fn a_mariadb_analyze_maps_actual_rows_loops_and_per_loop_times() {
    let plan = parse_explain_json(MARIADB_ANALYZE).unwrap();

    assert_eq!(plan.planning_time, Some(0.107));
    assert_eq!(plan.execution_time, Some(0.062));

    let block = &plan.roots[0];
    assert_eq!(block.actual_loops, Some(1));
    assert_eq!(block.actual_total_time, Some(0.062));

    let sort = &block.children[0];
    assert_eq!(sort.actual_rows, Some(1.0));

    let nested = &sort.children[0].children[0];
    let outer = &nested.children[0];
    assert_eq!(outer.relation.as_deref(), Some("a"));
    assert_eq!(outer.estimated_rows, Some(8.0));
    assert_eq!(outer.actual_rows, Some(80.0));
    assert_eq!(outer.actual_loops, Some(1));
    assert!((outer.actual_total_time.unwrap() - 0.016).abs() < 1e-12);

    // MariaDB reports a total over all loops; the model keeps it per loop.
    let inner = &nested.children[1];
    assert_eq!(inner.actual_loops, Some(8));
    assert!((inner.actual_total_time.unwrap() - 0.005).abs() < 1e-12);
    assert!((inner.actual_total_time.unwrap() * 8.0 - 0.040).abs() < 1e-12);
}

#[test]
fn mariadb_actual_keys_are_fields_and_engine_stats_stay_details() {
    let plan = parse_explain_json(MARIADB_ANALYZE).unwrap();
    let outer = &plan.roots[0].children[0].children[0].children[0].children[0];

    for key in [
        "r_rows",
        "r_loops",
        "r_table_time_ms",
        "r_other_time_ms",
        "r_total_time_ms",
    ] {
        assert!(
            !outer.details.iter().any(|(name, _)| name == key),
            "{key} is a field"
        );
    }
    assert!(outer.children.is_empty(), "engine stats are not a node");
    assert!(
        outer
            .details
            .iter()
            .any(|(name, value)| name == "r_engine_stats" && value.contains("pages_accessed")),
        "{:?}",
        outer.details
    );
}

#[test]
fn a_mariadb_analyze_finds_the_estimate_gap() {
    let plan = parse_explain_json(MARIADB_ANALYZE).unwrap();
    let gap = plan.largest_estimate_gap().unwrap();

    assert_eq!(gap.node.relation.as_deref(), Some("a"));
    assert_eq!(gap.ratio, 10.0);
}

#[test]
fn an_estimate_json_still_has_no_actuals() {
    let plan = parse_explain_json(MYSQL_8_JOIN).unwrap();

    assert_eq!(plan.planning_time, None);
    assert_eq!(plan.execution_time, None);
    assert!(plan.roots[0].actual_total_time.is_none());
}

#[test]
fn analyze_needs_mysql_8_0_18_or_later() {
    for version in ["8.0.18", "8.0.36", "8.4.8", "9.1.0", "10.0.0"] {
        assert!(
            analyze_version_gate(DbKind::MySQL, version).is_ok(),
            "{version}"
        );
    }

    for version in ["5.7.44", "8.0.17", "8.0.0", "5.6.51"] {
        match analyze_version_gate(DbKind::MySQL, version) {
            Err(DbError::NotSupported(message)) => {
                assert_eq!(
                    message, "Analyzed plans need MySQL 8.0.18 or later",
                    "{version}"
                );
            }
            other => panic!("{version}: expected NotSupported, got {other:?}"),
        }
    }
}

#[test]
fn analyze_needs_mariadb_10_1_or_later() {
    for version in [
        "10.1.0-MariaDB",
        "11.8.9-MariaDB-ubu2404",
        "5.5.5-10.11.6-MariaDB",
    ] {
        assert!(
            analyze_version_gate(DbKind::MariaDB, version).is_ok(),
            "{version}"
        );
    }

    for version in ["10.0.38-MariaDB", "5.5.68-MariaDB", "5.5.5-10.0.1-MariaDB"] {
        match analyze_version_gate(DbKind::MariaDB, version) {
            Err(DbError::NotSupported(message)) => {
                assert_eq!(
                    message, "Analyzed plans need MariaDB 10.1 or later",
                    "{version}"
                );
            }
            other => panic!("{version}: expected NotSupported, got {other:?}"),
        }
    }
}

#[test]
fn an_unreadable_version_does_not_block_analyze() {
    assert!(analyze_version_gate(DbKind::MySQL, "").is_ok());
    assert!(analyze_version_gate(DbKind::MySQL, "unknown").is_ok());
    assert!(analyze_version_gate(DbKind::MariaDB, "x").is_ok());
}

#[test]
fn a_mariadb_scan_reports_the_rows_that_pass_its_attached_condition() {
    let plan = parse_explain_json(
        r#"{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"x","access_type":"ALL","r_loops":1,"rows":1000,"r_rows":1000,"filtered":50,"r_filtered":1,"attached_condition":"x.v = 1"}}]}}"#,
    )
    .unwrap();

    let scan = &plan.roots[0].children[0];
    assert_eq!(scan.actual_rows, Some(10.0));
    assert_eq!(scan.estimated_rows, Some(500.0));
}

#[test]
fn a_mariadb_scan_that_filters_nothing_keeps_its_rows() {
    let plan = parse_explain_json(
        r#"{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"x","access_type":"ALL","r_loops":1,"rows":8,"r_rows":80,"filtered":100,"r_filtered":100}}]}}"#,
    )
    .unwrap();

    let scan = &plan.roots[0].children[0];
    assert_eq!(scan.actual_rows, Some(80.0));
    assert_eq!(scan.estimated_rows, Some(8.0));
}

#[test]
fn a_cost_range_maps_startup_and_total_and_a_scalar_cost_stays_total() {
    let tree =
        "-> Table scan on t  (cost=0.05..4.12 rows=10) (actual time=0.01..0.02 rows=10 loops=1)\n";
    let node = &parse_analyze_tree(tree).unwrap().roots[0];
    assert_eq!(node.estimated_startup_cost, Some(0.05));
    assert_eq!(node.estimated_total_cost, Some(4.12));

    let tree = "-> Table scan on t  (cost=4.12 rows=10) (actual time=0.01..0.02 rows=10 loops=1)\n";
    let node = &parse_analyze_tree(tree).unwrap().roots[0];
    assert_eq!(node.estimated_startup_cost, None);
    assert_eq!(node.estimated_total_cost, Some(4.12));
}

#[test]
fn a_loop_count_in_engineering_notation_is_read() {
    let tree = "-> Table scan on t  (cost=1 rows=1) (actual time=0.001..0.002 rows=1 loops=1e+6)\n";
    let node = &parse_analyze_tree(tree).unwrap().roots[0];
    assert_eq!(node.actual_loops, Some(1_000_000));

    for bad in ["1.5", "-2", "inf", "NaN", "1e30"] {
        let tree = format!(
            "-> Table scan on t  (cost=1 rows=1) (actual time=0.001..0.002 rows=1 loops={bad})\n"
        );
        let node = &parse_analyze_tree(&tree).unwrap().roots[0];
        assert_eq!(node.actual_loops, None, "{bad}");
    }
}
